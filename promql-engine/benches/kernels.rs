//! Numerical-kernel microbenchmarks and isolated preplanned series evaluation.
//!
//! `SeriesEval*` includes DataFusion stream creation, the real executor, Arrow
//! output construction/draining, and execution-state reset, while excluding
//! source selection and planning. Other groups drive public kernels and
//! accumulators directly. `AggregateSingletonSpans` isolates legal descending
//! span growth. Fixtures are deterministic and constructed lazily outside the
//! timed `keep_running` window so filtered runs do not pay setup costs for
//! non-matching cases.
//!
//! Recommended CLI invocation:
//! ```sh
//! cargo bench -p promql-engine --bench kernels -- -bench '^Benchmark' -benchtime 1s -count 6 -cpu 2 -benchmem | tee ${bench}.txt
//! ```

use std::hint::black_box;
use std::num::NonZeroU64;
use std::sync::{Arc, OnceLock};

use benchfmt::{CountingAlloc, Suite};
use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, BooleanBufferBuilder, ListArray,
};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Schema};
use datafusion::common::ScalarValue;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::function::AccumulatorArgs;
use datafusion::logical_expr::{EmitTo, GroupsAccumulator};
use datafusion::physical_expr::expressions::{Column, Literal};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::execution_plan::reset_plan_states;
use datafusion::physical_plan::{collect, execute_stream, ExecutionPlan};
use datafusion::prelude::SessionContext;
use futures::StreamExt;
use promql_engine::absent;
use promql_engine::aggregate::{Grouped, Op};
use promql_engine::binary::{self, Op as BinOp};
use promql_engine::elementwise;
use promql_engine::math::{self, FlagLane, Welford};
use promql_engine::range::Func;
use promql_engine::selector::STALE_NAN_BITS;
use promql_engine::series::{self, Block, Series};
use promql_engine::{
    CompiledMatcher, Engine, MemorySeriesSource, Params, RangeQuery, SelectHints, SeriesSource,
};

#[global_allocator]
static ALLOC: CountingAlloc = CountingAlloc::new();

/// Scrape interval of every synthetic series.
const SCRAPE_MS: i64 = 15_000;
/// Step of every synthetic query grid.
const STEP_MS: i64 = 30_000;
const FIVE_MINUTES_MS: i64 = 5 * 60_000;
/// Series lengths the per-series kernels are measured at.
const LENGTHS: [usize; 2] = [1_000, 16_000];
/// The one block every bench fixture sits in: the kernels are measured
/// over whole series, the block edge is the planner's concern.
const WHOLE: Block = Block {
    start_ms: i64::MIN,
    end_ms: i64::MAX,
};

/// Bytes per `(timestamp_ms: i64, value: f64)` sample pair for `MB/s` throughput.
const SAMPLE_BYTES: u64 = 16;

/// A counter scraped every `SCRAPE_MS`, rising by a value that varies
/// per sample and resetting every 1000 samples, like a restarting pod.
fn counter(n: usize) -> (Vec<i64>, Vec<f64>) {
    let mut ts = Vec::with_capacity(n);
    let mut vs = Vec::with_capacity(n);
    let mut v = 0.0;
    for i in 0..n {
        if i % 1000 == 0 {
            v = 0.0;
        }
        v += 1.0 + (i % 7) as f64;
        ts.push(i as i64 * SCRAPE_MS);
        vs.push(v);
    }
    (ts, vs)
}

/// The same values with every `every`-th one replaced by Prometheus's
/// staleness marker.
fn with_stale(vs: &[f64], every: usize) -> Vec<f64> {
    vs.iter()
        .enumerate()
        .map(|(i, v)| {
            if i % every == every - 1 {
                f64::from_bits(STALE_NAN_BITS)
            } else {
                *v
            }
        })
        .collect()
}

/// The grid a range query over the whole series evaluates on. Selector
/// and range kernels share one `Params`, so `window_ms` reads as the
/// lookback delta here and as the `[5m]` in [`range_params`].
fn selector_params(n: usize) -> Params {
    Params {
        start_ms: 0,
        end_ms: (n as i64 - 1) * SCRAPE_MS,
        step_ms: STEP_MS,
        window_ms: FIVE_MINUTES_MS,
        offset_ms: 0,
        at_ms: None,
    }
}

fn range_params(n: usize) -> Params {
    selector_params(n)
}

/// `k` series with a sample on every one of `steps` grid points, the
/// shape the aggregate accumulator sees after the vector selector.
fn aligned(k: usize, steps: usize) -> Vec<(Vec<i64>, Vec<f64>)> {
    (0..k)
        .map(|i| {
            let ts = (0..steps).map(|j| j as i64 * STEP_MS).collect();
            let vs = (0..steps).map(|j| (i * 31 + j) as f64 * 0.25).collect();
            (ts, vs)
        })
        .collect()
}

/// The `samples` column of those series, which is what the aggregate's
/// `update_batch` is handed. Routed through the canonical encoder so the
/// list offsets and child layout match what the plan really produces.
fn samples_of(all: &[(Vec<i64>, Vec<f64>)]) -> ArrayRef {
    let series: Vec<Series> = all
        .iter()
        .enumerate()
        .map(|(i, (ts, vs))| {
            let id = format!("s-{i}");
            let labels = [("__name__", "aligned"), ("id", id.as_str())];
            Series::new(&labels, ts.clone(), vs.clone()).unwrap()
        })
        .collect();
    let batch = series::encode(&series::label_names_of(&series), &series, WHOLE).unwrap();
    Arc::clone(batch.column_by_name(series::SAMPLES).unwrap())
}

/// `k` series of `n` samples with five labels, four of them varying.
fn labelled(k: usize, n: usize) -> Vec<Series> {
    (0..k)
        .map(|i| {
            let pod = format!("nginx-{i}");
            let route = format!("/{}", i % 8);
            let zone = format!("eu-west-{}", i % 3);
            let code = if i % 16 == 0 { "500" } else { "200" };
            let labels = [
                ("__name__", "http_requests_total"),
                ("code", code),
                ("pod", pod.as_str()),
                ("route", route.as_str()),
                ("zone", zone.as_str()),
            ];
            let (ts, vs) = counter(n);
            Series::new(&labels, ts, vs).unwrap()
        })
        .collect()
}

fn runtime() -> &'static tokio::runtime::Runtime {
    static RT: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
    })
}

fn task_ctx() -> &'static Arc<TaskContext> {
    static CTX: OnceLock<Arc<TaskContext>> = OnceLock::new();
    CTX.get_or_init(|| Arc::new(TaskContext::default()))
}

/// The sole evaluator is reached through a preplanned public engine query;
/// benchmark-only wrappers must not keep the removed native UDAFs alive.
fn selected_plan(
    rt: &tokio::runtime::Runtime,
    func: Option<BenchRangeFunc>,
    p: &Params,
    rows: Vec<Series>,
) -> Arc<dyn ExecutionPlan> {
    assert_eq!(p.offset_ms, 0);
    let modifier = p
        .at_ms
        .map(|at| format!(" @ {}", at / 1000))
        .unwrap_or_default();
    let query = match func {
        Some(BenchRangeFunc::Func(func)) => format!(
            "{}(http_requests_total[{}ms]{modifier})",
            func.as_str(),
            p.window_ms
        ),
        Some(BenchRangeFunc::PredictLinear) => format!(
            "predict_linear(http_requests_total[{}ms]{modifier}, 3600)",
            p.window_ms
        ),
        None => format!("http_requests_total{modifier}"),
    };
    let source = MemorySeriesSource::try_new(rows).unwrap();
    let plan = rt
        .block_on(Engine::new().prepare_async(
            &source,
            &query,
            &RangeQuery::new(p.start_ms, p.end_ms, p.step_ms),
        ))
        .unwrap()
        .physical_plan()
        .clone();
    assert_answers(rt, &plan, task_ctx());
    plan
}

#[derive(Clone, Copy)]
enum BenchRangeFunc {
    Func(Func),
    PredictLinear,
}

fn drain_plan(rt: &tokio::runtime::Runtime, plan: &Arc<dyn ExecutionPlan>, ctx: &Arc<TaskContext>) {
    rt.block_on(async {
        let plan = reset_plan_states(plan.clone()).unwrap();
        let mut stream = execute_stream(plan, ctx.clone()).unwrap();
        while let Some(batch) = stream.next().await {
            black_box(batch.unwrap());
        }
    });
}

/// A benchmark must not silently time an empty grid or empty output.
fn assert_answers(
    rt: &tokio::runtime::Runtime,
    plan: &Arc<dyn ExecutionPlan>,
    ctx: &Arc<TaskContext>,
) {
    let plan = reset_plan_states(plan.clone()).unwrap();
    let batches = rt.block_on(collect(plan, ctx.clone())).unwrap();
    assert!(batches.iter().any(|batch| batch.num_rows() > 0));
    for batch in batches {
        let out = batch
            .column_by_name(series::SAMPLES)
            .unwrap()
            .as_list::<i32>();
        assert!((0..out.len()).all(|row| !out.value(row).is_empty()));
    }
}

fn single_counter(ts: Vec<i64>, vs: Vec<f64>) -> Vec<Series> {
    vec![Series::new(&[("__name__", "http_requests_total")], ts, vs).unwrap()]
}

/// The elementwise kernels behind `Steps::add_run`: one slice of values
/// folded into as many accumulator lanes. 240 lanes is an hour at 15s,
/// 1440 a day at 60s.
fn register_math_add_each<'a>(mut suite: Suite<'a>) -> Suite<'a> {
    for lanes in [240usize, 1440] {
        let bytes = (lanes * size_of::<f64>()) as u64;

        suite = suite
            .bench(format!("MathAddEach/op=kahan/lanes={lanes}"), move |b| {
                let values: Vec<f64> = (0..lanes).map(|i| 1.0 + (i % 7) as f64).collect();
                let (mut sums, mut comps) = (vec![0.0; lanes], vec![0.0; lanes]);
                b.set_bytes(bytes);
                b.report_allocs();
                while b.keep_running() {
                    math::kahan_add_each(&mut sums, &mut comps, black_box(&values));
                    black_box(&sums);
                }
            })
            .bench(format!("MathAddEach/op=mean/lanes={lanes}"), move |b| {
                let values: Vec<f64> = (0..lanes).map(|i| 1.0 + (i % 7) as f64).collect();
                let (mut acc, mut comps, mut counts) =
                    (vec![0.0; lanes], vec![0.0; lanes], vec![0.0; lanes]);
                let mut incremental = BooleanBufferBuilder::new(lanes);
                incremental.append_n(lanes, false);
                b.set_bytes(bytes);
                b.report_allocs();
                while b.keep_running() {
                    math::mean_add_each(
                        &mut acc,
                        &mut comps,
                        &mut counts,
                        FlagLane::new(&mut incremental, 0, lanes),
                        black_box(&values),
                    );
                    black_box(&acc);
                }
            })
            .bench(format!("MathAddEach/op=max/lanes={lanes}"), move |b| {
                let values: Vec<f64> = (0..lanes).map(|i| 1.0 + (i % 7) as f64).collect();
                let mut cur = vec![f64::NAN; lanes];
                b.set_bytes(bytes);
                b.report_allocs();
                while b.keep_running() {
                    math::max_add_each(&mut cur, black_box(&values));
                    black_box(&cur);
                }
            })
            .bench(format!("MathAddEach/op=welford/lanes={lanes}"), move |b| {
                let values: Vec<f64> = (0..lanes).map(|i| 1.0 + (i % 7) as f64).collect();
                let (mut means, mut m2s, mut counts) =
                    (vec![0.0; lanes], vec![0.0; lanes], vec![0.0; lanes]);
                b.set_bytes(bytes);
                b.report_allocs();
                while b.keep_running() {
                    math::welford_add_each(&mut means, &mut m2s, &mut counts, black_box(&values));
                    black_box(&means);
                }
            });
    }
    suite
}

/// The sequential reductions behind the `*_over_time` functions.
fn register_math_reductions<'a>(mut suite: Suite<'a>) -> Suite<'a> {
    for n in LENGTHS {
        let bytes = (n * size_of::<f64>()) as u64;
        suite = suite
            .bench(
                format!("MathReductions/op=kahan_sum/samples={n}"),
                move |b| {
                    let mut vs_opt = None;
                    let vs = vs_opt.get_or_insert_with(|| counter(n).1);
                    b.set_bytes(bytes);
                    b.report_allocs();
                    while b.keep_running() {
                        black_box(math::kahan_sum(black_box(vs)).value());
                    }
                },
            )
            .bench(format!("MathReductions/op=mean_of/samples={n}"), move |b| {
                let mut vs_opt = None;
                let vs = vs_opt.get_or_insert_with(|| counter(n).1);
                b.set_bytes(bytes);
                b.report_allocs();
                while b.keep_running() {
                    black_box(math::mean_of(black_box(vs)));
                }
            })
            .bench(
                format!("MathReductions/op=variance_of/samples={n}"),
                move |b| {
                    let mut vs_opt = None;
                    let vs = vs_opt.get_or_insert_with(|| counter(n).1);
                    b.set_bytes(bytes);
                    b.report_allocs();
                    while b.keep_running() {
                        black_box(math::variance_of(black_box(vs)));
                    }
                },
            )
            // No slice-at-a-time welford reduction exists; the `stddev` and
            // `stdvar` aggregations fold the accumulator a sample at a time, so
            // the bench folds it too.
            .bench(
                format!("MathReductions/op=welford_of/samples={n}"),
                move |b| {
                    let mut vs_opt = None;
                    let vs = vs_opt.get_or_insert_with(|| counter(n).1);
                    b.set_bytes(bytes);
                    b.report_allocs();
                    while b.keep_running() {
                        let mut w = Welford::default();
                        for v in black_box(vs.as_slice()) {
                            w.add(*v);
                        }
                        black_box(w.variance());
                    }
                },
            );
    }
    suite
}

/// One series walked across the grid by the real `SeriesEvalExec`.
fn register_selector_eval_series<'a>(mut suite: Suite<'a>) -> Suite<'a> {
    #[derive(Clone, Copy)]
    enum SelectorCase {
        Plain,
        StaleEvery100,
        AtPinned,
    }

    for n in LENGTHS {
        let bytes = n as u64 * SAMPLE_BYTES;
        for (name, case) in [
            ("plain", SelectorCase::Plain),
            ("stale_every_100", SelectorCase::StaleEvery100),
            ("at_pinned", SelectorCase::AtPinned),
        ] {
            suite = suite.bench(
                format!("SeriesEvalSelector/case={name}/samples={n}"),
                move |b| {
                    let rt = runtime();
                    let ctx = task_ctx();
                    let mut plan_opt = None;
                    let plan = plan_opt.get_or_insert_with(|| {
                        let (ts, vs) = counter(n);
                        let p = selector_params(n);
                        match case {
                            SelectorCase::Plain => {
                                selected_plan(rt, None, &p, single_counter(ts, vs))
                            }
                            SelectorCase::StaleEvery100 => {
                                let stale = with_stale(&vs, 100);
                                selected_plan(rt, None, &p, single_counter(ts, stale))
                            }
                            SelectorCase::AtPinned => {
                                let pinned = Params {
                                    at_ms: Some(p.end_ms),
                                    ..p
                                };
                                selected_plan(rt, None, &pinned, single_counter(ts, vs))
                            }
                        }
                    });
                    b.set_bytes(bytes);
                    b.report_allocs();
                    while b.keep_running() {
                        drain_plan(rt, black_box(plan), ctx);
                    }
                },
            );
        }
    }
    suite
}

/// A thousand identities; output construction is timed, selection is not.
fn register_selector_apply<'a>(suite: Suite<'a>) -> Suite<'a> {
    let (k, n) = (1_000usize, 1_000usize);
    let bytes = (k * n) as u64 * SAMPLE_BYTES;
    suite.bench(format!("SeriesEvalSelectorApply/shape={k}x{n}"), move |b| {
        let rt = runtime();
        let ctx = task_ctx();
        let mut plan_opt = None;
        let plan = plan_opt
            .get_or_insert_with(|| selected_plan(rt, None, &selector_params(n), labelled(k, n)));
        b.set_bytes(bytes);
        b.report_allocs();
        while b.keep_running() {
            drain_plan(rt, black_box(plan), ctx);
        }
    })
}

/// Buffer and sweep internals stay private; this times their production driver.
fn register_range_function<'a>(mut suite: Suite<'a>) -> Suite<'a> {
    for n in LENGTHS {
        let bytes = n as u64 * SAMPLE_BYTES;
        for (name, func, stale) in [
            ("rate", Func::Rate, false),
            ("irate", Func::Irate, false),
            ("increase", Func::Increase, false),
            ("delta", Func::Delta, false),
            ("count_over_time", Func::CountOverTime, false),
            ("sum_over_time", Func::SumOverTime, false),
            ("avg_over_time", Func::AvgOverTime, false),
            ("max_over_time", Func::MaxOverTime, false),
            ("last_over_time", Func::LastOverTime, false),
            ("present_over_time", Func::PresentOverTime, false),
            ("stddev_over_time", Func::StddevOverTime, false),
            ("changes", Func::Changes, false),
            ("resets", Func::Resets, false),
            ("deriv", Func::Deriv, false),
            ("rate_stale_every_100", Func::Rate, true),
        ] {
            suite = suite.bench(
                format!("SeriesEvalRange/func={name}/samples={n}"),
                move |b| {
                    let rt = runtime();
                    let ctx = task_ctx();
                    let mut plan_opt = None;
                    let plan = plan_opt.get_or_insert_with(|| {
                        let (ts, vs) = counter(n);
                        let values = if stale { with_stale(&vs, 100) } else { vs };
                        selected_plan(
                            rt,
                            Some(BenchRangeFunc::Func(func)),
                            &range_params(n),
                            single_counter(ts, values),
                        )
                    });
                    b.set_bytes(bytes);
                    b.report_allocs();
                    while b.keep_running() {
                        drain_plan(rt, black_box(plan), ctx);
                    }
                },
            );
        }
    }
    suite
}

/// The pinned window is reused, while `predict_linear` refits at every
/// evaluation timestamp because Prometheus anchors the intercept there.
fn register_predict_linear_series<'a>(mut suite: Suite<'a>) -> Suite<'a> {
    for n in LENGTHS {
        let bytes = n as u64 * SAMPLE_BYTES;
        for (name, pin) in [("moving", false), ("at_pinned", true)] {
            suite = suite.bench(
                format!("SeriesEvalPredictLinear/mode={name}/samples={n}"),
                move |b| {
                    let rt = runtime();
                    let ctx = task_ctx();
                    let mut plan_opt = None;
                    let plan = plan_opt.get_or_insert_with(|| {
                        let (ts, vs) = counter(n);
                        let mut p = range_params(n);
                        if pin {
                            p.at_ms = Some(p.end_ms);
                        }
                        selected_plan(
                            rt,
                            Some(BenchRangeFunc::PredictLinear),
                            &p,
                            single_counter(ts, vs),
                        )
                    });
                    b.set_bytes(bytes);
                    b.report_allocs();
                    while b.keep_running() {
                        drain_plan(rt, black_box(plan), ctx);
                    }
                },
            );
        }
    }
    suite
}

/// `rate` over a thousand identities through the sole executor.
fn register_range_apply<'a>(suite: Suite<'a>) -> Suite<'a> {
    let (k, n) = (1_000usize, 1_000usize);
    let bytes = (k * n) as u64 * SAMPLE_BYTES;
    suite.bench(
        format!("SeriesEvalRangeApply/func=rate/shape={k}x{n}"),
        move |b| {
            let rt = runtime();
            let ctx = task_ctx();
            let mut plan_opt = None;
            let plan = plan_opt.get_or_insert_with(|| {
                selected_plan(
                    rt,
                    Some(BenchRangeFunc::Func(Func::Rate)),
                    &range_params(n),
                    labelled(k, n),
                )
            });
            b.set_bytes(bytes);
            b.report_allocs();
            while b.keep_running() {
                drain_plan(rt, black_box(plan), ctx);
            }
        },
    )
}

/// One accumulator filled with a whole `samples` column, all of it in
/// `groups` groups assigned round-robin.
fn filled(op: Op, column: &ArrayRef, groups: &[usize], end_ms: i64, total: usize) -> Grouped {
    let mut acc = Grouped::new(op, 0, end_ms, STEP_MS).unwrap();
    acc.update_batch(std::slice::from_ref(column), groups, None, total)
        .unwrap();
    acc
}

/// Folding `k` aligned series into one accumulator, the partial phase of
/// a cross-series aggregation. The accumulator's allocation is included.
fn register_aggregate_add_series<'a>(mut suite: Suite<'a>) -> Suite<'a> {
    let steps = 1440usize;
    let end_ms = (steps as i64 - 1) * STEP_MS;
    for k in [100usize, 1_000] {
        let bytes = (k * steps) as u64 * SAMPLE_BYTES;
        for (name, op) in [
            ("sum", Op::Sum),
            ("avg", Op::Avg),
            ("count", Op::Count),
            ("min", Op::Min),
            ("max", Op::Max),
            ("group", Op::Group),
            ("stddev", Op::Stddev),
            ("stdvar", Op::Stdvar),
        ] {
            suite = suite.bench(
                format!("AggregateAddSeries/op={name}/series={k}"),
                move |b| {
                    let mut fix_opt = None;
                    let (column, one) = fix_opt
                        .get_or_insert_with(|| (samples_of(&aligned(k, steps)), vec![0usize; k]));
                    b.set_bytes(bytes);
                    b.report_allocs();
                    while b.keep_running() {
                        black_box(filled(op, black_box(column), one, end_ms, 1));
                    }
                },
            );
        }
    }
    suite
}

/// Folding 1,000 aligned series round-robin across 64 distinct group IDs so
/// per-group span growth and group-indexed lane lookup are measured alongside
/// the single-group case.
fn register_aggregate_multi_group<'a>(mut suite: Suite<'a>) -> Suite<'a> {
    let (k, groups_count, steps) = (1_000usize, 64usize, 1_440usize);
    let end_ms = (steps as i64 - 1) * STEP_MS;
    let bytes = (k * steps) as u64 * SAMPLE_BYTES;
    for (name, op) in [("sum", Op::Sum), ("avg", Op::Avg), ("stddev", Op::Stddev)] {
        suite = suite.bench(
            format!("AggregateMultiGroup/op={name}/groups={groups_count}/series={k}"),
            move |b| {
                let mut fix_opt = None;
                let (column, groups) = fix_opt.get_or_insert_with(|| {
                    let groups = (0..k).map(|i| i % groups_count).collect::<Vec<_>>();
                    (samples_of(&aligned(k, steps)), groups)
                });
                b.set_bytes(bytes);
                b.report_allocs();
                while b.keep_running() {
                    black_box(filled(op, black_box(column), groups, end_ms, groups_count));
                }
            },
        );
    }
    suite
}

/// Singleton contributions in legal between-ID order. This isolates lane
/// front growth in Prometheus `aggregation` (83962c35) from source/planner
/// work. Ascending is the control; descending must not repeatedly move the
/// entire accumulated span. Values and update order are never pre-reduced.
fn register_aggregate_singleton_spans<'a>(mut suite: Suite<'a>) -> Suite<'a> {
    for steps in [1024usize, 8192] {
        let end_ms = (steps as i64 - 1) * STEP_MS;
        let bytes = steps as u64 * SAMPLE_BYTES;
        for descending in [false, true] {
            let name = if descending {
                "descending"
            } else {
                "ascending"
            };
            suite = suite.bench(
                format!("AggregateSingletonSpans/order={name}/steps={steps}"),
                move |b| {
                    let mut fix_opt = None;
                    let (column, groups) = fix_opt.get_or_insert_with(|| {
                        let groups = vec![0; steps];
                        let rows: Vec<_> = (0..steps)
                            .map(|i| {
                                let step = if descending { steps - 1 - i } else { i };
                                (vec![step as i64 * STEP_MS], vec![1.0])
                            })
                            .collect();
                        let column = samples_of(&rows);
                        let mut probe = filled(Op::Sum, &column, &groups, end_ms, 1);
                        let result = probe.evaluate(EmitTo::All).unwrap();
                        let list = result.as_list::<i32>();
                        assert_eq!(list.len(), 1);
                        assert_eq!(list.value(0).len(), steps);
                        (column, groups)
                    });
                    b.set_bytes(bytes);
                    b.report_allocs();
                    while b.keep_running() {
                        black_box(filled(Op::Sum, black_box(column), groups, end_ms, 1));
                    }
                },
            );
        }
    }
    suite
}

/// The shuffle between the two aggregation phases: emitting partial state
/// from a filled accumulator (`phase=state`, comparable against
/// `AggregateAddSeries/op=.../series=100` because `state()` consumes the
/// accumulator's lanes) and merging that state into a final accumulator
/// (`phase=merge_batch`).
fn register_aggregate_state_merge<'a>(mut suite: Suite<'a>) -> Suite<'a> {
    let steps = 1440usize;
    let end_ms = (steps as i64 - 1) * STEP_MS;
    let bytes = steps as u64 * SAMPLE_BYTES;
    for (name, op) in [("sum", Op::Sum), ("avg", Op::Avg), ("stddev", Op::Stddev)] {
        suite = suite
            .bench(
                format!("AggregateStateMerge/op={name}/phase=state/steps={steps}"),
                move |b| {
                    let mut fix_opt = None;
                    let (column, one) = fix_opt.get_or_insert_with(|| {
                        (samples_of(&aligned(100, steps)), vec![0usize; 100])
                    });
                    b.set_bytes(bytes);
                    b.report_allocs();
                    while b.keep_running() {
                        let mut acc = filled(op, black_box(column), one, end_ms, 1);
                        black_box(acc.state(EmitTo::All).unwrap());
                    }
                },
            )
            .bench(
                format!("AggregateStateMerge/op={name}/phase=merge_batch/steps={steps}"),
                move |b| {
                    let mut state_opt = None;
                    let state = state_opt.get_or_insert_with(|| {
                        let column = samples_of(&aligned(100, steps));
                        let one = vec![0usize; 100];
                        filled(op, &column, &one, end_ms, 1)
                            .state(EmitTo::All)
                            .unwrap()
                            .remove(0)
                    });
                    b.set_bytes(bytes);
                    b.report_allocs();
                    while b.keep_running() {
                        let mut acc = Grouped::new(op, 0, end_ms, STEP_MS).unwrap();
                        acc.merge_batch(std::slice::from_ref(black_box(state)), &[0], 1)
                            .unwrap();
                        black_box(acc.evaluate(EmitTo::All).unwrap());
                    }
                },
            );
    }
    suite
}

struct BinaryFixture {
    pairs: Vec<[ArrayRef; 3]>,
    schema: Schema,
    exprs: Vec<Arc<dyn PhysicalExpr>>,
    fields: Vec<FieldRef>,
    return_field: FieldRef,
    udaf: datafusion::logical_expr::AggregateUDF,
}

impl BinaryFixture {
    fn new(groups: usize, steps: usize, step_ms: i64, op: BinOp, return_bool: bool) -> Self {
        let end_ms = (steps as i64 - 1) * step_ms;
        // Left and right of group `i` are rows `2i` and `2i + 1`. Left
        // ranges over 1..=100 and right sits at 50.5, so a `>` keeps about
        // half the steps and never ties.
        let rows: Vec<Series> = (0..groups * 2)
            .map(|row| {
                let (i, right) = (row / 2, row % 2 == 1);
                let id = format!("g-{i}");
                let name = if right { "rhs" } else { "lhs" };
                let labels = [("__name__", name), ("group", id.as_str())];
                let ts = (0..steps).map(|j| j as i64 * step_ms).collect();
                let vs = (0..steps)
                    .map(|j| {
                        if right {
                            50.5
                        } else {
                            ((i + j) % 100 + 1) as f64
                        }
                    })
                    .collect();
                Series::new(&labels, ts, vs).unwrap()
            })
            .collect();
        let names = series::label_names_of(&rows);
        let batch = series::encode(&names, &rows, WHOLE).unwrap();
        let samples = batch.column_by_name(series::SAMPLES).unwrap();
        let labels = batch.column_by_name(series::LABELS).unwrap();
        let sides: ArrayRef = Arc::new(BooleanArray::from(vec![false, true]));
        let pairs: Vec<[ArrayRef; 3]> = (0..groups)
            .map(|i| {
                [
                    samples.slice(2 * i, 2),
                    Arc::clone(&sides),
                    labels.slice(2 * i, 2),
                ]
            })
            .collect();
        let schema = Schema::new(vec![
            Field::new(series::SAMPLES, series::samples_type(), false),
            Field::new("side", DataType::Boolean, false),
            Field::new(series::LABELS, labels.data_type().clone(), false),
        ]);
        let mut exprs: Vec<Arc<dyn PhysicalExpr>> = vec![
            Arc::new(Column::new(series::SAMPLES, 0)),
            Arc::new(Column::new("side", 1)),
            Arc::new(Column::new(series::LABELS, 2)),
        ];
        exprs.extend(
            [
                ScalarValue::from(binary::literal(op, return_bool)),
                ScalarValue::from(""),
                ScalarValue::Int64(Some(0)),
                ScalarValue::Int64(Some(end_ms)),
                ScalarValue::Int64(Some(step_ms)),
            ]
            .into_iter()
            .map(|v| Arc::new(Literal::new(v)) as Arc<dyn PhysicalExpr>),
        );
        let fields: Vec<FieldRef> = exprs
            .iter()
            .map(|e| e.return_field(&schema).unwrap())
            .collect();
        let udaf = binary::udaf();
        let return_field = Arc::new(Field::new("out", binary::output_type(&names), false));

        // Verify the grid literals produce non-empty output before timing.
        let args = AccumulatorArgs {
            return_field: Arc::clone(&return_field),
            schema: &schema,
            ignore_nulls: false,
            order_bys: &[],
            is_reversed: false,
            name: udaf.name(),
            is_distinct: false,
            exprs: &exprs,
            expr_fields: &fields,
        };
        let mut probe = udaf.accumulator(args).unwrap();
        probe.update_batch(&pairs[0]).unwrap();
        let ScalarValue::List(out) = probe.evaluate().unwrap() else {
            panic!("a list of output series")
        };
        assert!(!out.value(0).is_empty());

        Self {
            pairs,
            schema,
            exprs,
            fields,
            return_field,
            udaf,
        }
    }

    fn run(&self) {
        let args = AccumulatorArgs {
            return_field: Arc::clone(&self.return_field),
            schema: &self.schema,
            ignore_nulls: false,
            order_bys: &[],
            is_reversed: false,
            name: self.udaf.name(),
            is_distinct: false,
            exprs: &self.exprs,
            expr_fields: &self.fields,
        };
        for pair in &self.pairs {
            let mut acc = self.udaf.accumulator(args.clone()).unwrap();
            acc.update_batch(black_box(pair)).unwrap();
            black_box(acc.evaluate().unwrap());
        }
    }
}

/// The plan's `promql_binary` aggregate over match groups: a fresh accumulator,
/// one left and one right row, then `evaluate`. Accumulator construction is
/// inside the timed closure because the plan builds one per match group too.
fn register_binary_pairing<'a>(mut suite: Suite<'a>) -> Suite<'a> {
    let steps = 1440usize;
    let step_ms = 60_000i64;
    for groups in [1usize, 1_000] {
        let bytes = (groups * steps) as u64 * SAMPLE_BYTES;
        for (name, op, return_bool) in [
            ("add", BinOp::Add, false),
            ("div", BinOp::Div, false),
            ("gtr", BinOp::Gtr, false),
            ("gtr_bool", BinOp::Gtr, true),
        ] {
            suite = suite.bench(
                format!("BinaryPairing/op={name}/shape={groups}x{steps}"),
                move |b| {
                    let mut fix_opt = None;
                    let fix = fix_opt.get_or_insert_with(|| {
                        BinaryFixture::new(groups, steps, step_ms, op, return_bool)
                    });
                    b.set_bytes(bytes);
                    b.report_allocs();
                    while b.keep_running() {
                        fix.run();
                    }
                },
            );
        }
    }
    suite
}

/// `elementwise::apply` under a binary operator's bound, the vector-scalar
/// path. The sawtooth values straddle 100, so `> 100` keeps about half and
/// the filtered path rebuilds row boundaries for a realistic fraction,
/// where `* 2` leaves them alone.
fn register_elementwise_binary<'a>(mut suite: Suite<'a>) -> Suite<'a> {
    let (rows, samples) = (1_000usize, 1_440usize);
    let bytes = (rows * samples) as u64 * SAMPLE_BYTES;
    for (name, spelling, scalar) in [
        ("mul", "*", 2.0),
        ("gtr", ">", 100.0),
        ("gtr_bool", "> bool", 100.0),
    ] {
        suite = suite.bench(
            format!("ElementwiseBinary/op={name}/shape={rows}x{samples}"),
            move |b| {
                let mut col_opt = None;
                let column = col_opt.get_or_insert_with(|| {
                    let all: Vec<(Vec<i64>, Vec<f64>)> = (0..rows)
                        .map(|i| {
                            let ts = (0..samples).map(|j| j as i64 * 60_000).collect();
                            let vs = (0..samples).map(|j| ((i * 7 + j) % 200) as f64).collect();
                            (ts, vs)
                        })
                        .collect();
                    samples_of(&all)
                });
                let list: &ListArray = column.as_list::<i32>();
                let bound = elementwise::Func::parse(spelling)
                    .unwrap()
                    .bind(Some(scalar), None);
                b.set_bytes(bytes);
                b.report_allocs();
                while b.keep_running() {
                    black_box(elementwise::apply(black_box(list), bound).unwrap());
                }
            },
        );
    }
    suite
}

/// `elementwise::apply` under a date extractor, the per-sample map path.
/// The values are Unix timestamps a minute apart from early 2024, what
/// `hour(timestamp(x))` hands the extractor, so the calendar arithmetic
/// runs on the dates a real query reaches rather than on small integers.
fn register_elementwise_date<'a>(mut suite: Suite<'a>) -> Suite<'a> {
    let (rows, samples) = (1_000usize, 1_440usize);
    let bytes = (rows * samples) as u64 * SAMPLE_BYTES;
    for name in [
        "day_of_month",
        "day_of_week",
        "day_of_year",
        "days_in_month",
        "hour",
        "minute",
        "month",
        "year",
    ] {
        suite = suite.bench(
            format!("ElementwiseDate/func={name}/shape={rows}x{samples}"),
            move |b| {
                let mut col_opt = None;
                let column = col_opt.get_or_insert_with(|| {
                    let all: Vec<(Vec<i64>, Vec<f64>)> = (0..rows)
                        .map(|i| {
                            let ts = (0..samples).map(|j| j as i64 * 60_000).collect();
                            let vs = (0..samples)
                                .map(|j| 1_704_067_200.0 + (i * 86_400 + j * 60) as f64)
                                .collect();
                            (ts, vs)
                        })
                        .collect();
                    samples_of(&all)
                });
                let list: &ListArray = column.as_list::<i32>();
                let bound = elementwise::Func::parse(name).unwrap().bind(None, None);
                b.set_bytes(bytes);
                b.report_allocs();
                while b.keep_running() {
                    black_box(elementwise::apply(black_box(list), bound).unwrap());
                }
            },
        );
    }
    suite
}

/// `elementwise::apply` under unary math, rounding, sign, and clamping
/// functions. Inputs stay strictly positive (`1.0..=200.0`) so `ln` and `sqrt`
/// measure finite fast paths rather than NaN short-circuits.
fn register_elementwise_math<'a>(mut suite: Suite<'a>) -> Suite<'a> {
    let (rows, samples) = (1_000usize, 1_440usize);
    let bytes = (rows * samples) as u64 * SAMPLE_BYTES;
    for (name, arg_a, arg_b) in [
        ("abs", None, None),
        ("round", Some(0.5), None),
        ("exp", None, None),
        ("ln", None, None),
        ("sqrt", None, None),
        ("sin", None, None),
        ("sgn", None, None),
        ("clamp", Some(25.0), Some(150.0)),
    ] {
        suite = suite.bench(
            format!("ElementwiseMath/func={name}/shape={rows}x{samples}"),
            move |b| {
                let mut col_opt = None;
                let column = col_opt.get_or_insert_with(|| {
                    let all: Vec<(Vec<i64>, Vec<f64>)> = (0..rows)
                        .map(|i| {
                            let ts = (0..samples).map(|j| j as i64 * 60_000).collect();
                            let vs = (0..samples)
                                .map(|j| 1.0 + ((i * 7 + j) % 200) as f64)
                                .collect();
                            (ts, vs)
                        })
                        .collect();
                    samples_of(&all)
                });
                let list: &ListArray = column.as_list::<i32>();
                let bound = elementwise::Func::parse(name).unwrap().bind(arg_a, arg_b);
                b.set_bytes(bytes);
                b.report_allocs();
                while b.keep_running() {
                    black_box(elementwise::apply(black_box(list), bound).unwrap());
                }
            },
        );
    }
    suite
}

/// Reference label-matcher compilation (`CompiledMatcher::compile`) and
/// per-value evaluation (`CompiledMatcher::matches`).
fn register_matcher<'a>(mut suite: Suite<'a>) -> Suite<'a> {
    for (name, selector) in [
        ("equal", r#"{pod="nginx-42"}"#),
        (
            "regex_alternation",
            r#"{pod=~"nginx-(1|2|3|42|99)-[a-z0-9]+"}"#,
        ),
    ] {
        suite = suite.bench(format!("MatcherCompile/op={name}"), move |b| {
            let mut m_opt = None;
            let matcher = m_opt.get_or_insert_with(|| {
                let mut ms = promql_parser::parse_metric_selector(selector).unwrap();
                ms.pop().unwrap()
            });
            b.report_allocs();
            while b.keep_running() {
                black_box(CompiledMatcher::compile(black_box(matcher)).unwrap());
            }
        });
    }

    let rows = 1_000usize;
    for (name, selector) in [
        ("equal", r#"{pod="nginx-42-prod"}"#),
        ("not_equal", r#"{pod!="nginx-42-prod"}"#),
        ("regex", r#"{pod=~"nginx-[0-9]+-(prod|staging)"}"#),
        ("not_regex", r#"{pod!~"nginx-9[0-9]+-.*"}"#),
    ] {
        suite = suite.bench(format!("MatcherEval/op={name}/rows={rows}"), move |b| {
            let mut fix_opt = None;
            let (compiled, values) = fix_opt.get_or_insert_with(|| {
                let ms = promql_parser::parse_metric_selector(selector).unwrap();
                let compiled = CompiledMatcher::compile(&ms[0]).unwrap();
                let values: Vec<String> = (0..rows)
                    .map(|i| {
                        let env = if i % 4 == 0 { "staging" } else { "prod" };
                        format!("nginx-{i}-{env}")
                    })
                    .collect();
                (compiled, values)
            });
            b.report_allocs();
            while b.keep_running() {
                let mut hits = 0usize;
                for v in black_box(values.as_slice()) {
                    hits += usize::from(compiled.matches(v));
                }
                black_box(hits);
            }
        });
    }
    suite
}

/// Reference store construction (`MemorySeriesSource::try_new` and
/// `from_descriptions`) and `SeriesSource::select` across matcher shapes and
/// chunked multi-block layouts.
fn register_memory_source<'a>(mut suite: Suite<'a>) -> Suite<'a> {
    let (build_k, build_n) = (1_000usize, 100usize);
    let build_bytes = (build_k * build_n) as u64 * SAMPLE_BYTES;
    suite = suite
        .bench(
            format!("SourceBuild/mode=try_new/shape={build_k}x{build_n}"),
            move |b| {
                let mut rows_opt = None;
                let rows = rows_opt.get_or_insert_with(|| labelled(build_k, build_n));
                b.set_bytes(build_bytes);
                b.report_allocs();
                while b.keep_running() {
                    let cloned = black_box(rows.as_slice()).to_vec();
                    black_box(MemorySeriesSource::try_new(cloned).unwrap());
                }
            },
        )
        .bench(
            format!("SourceBuild/mode=from_descriptions/shape={build_k}x{build_n}"),
            move |b| {
                let mut descs_opt = None;
                let descs = descs_opt.get_or_insert_with(|| {
                    (0..build_k)
                        .map(|i| {
                            let line = format!(
                                r#"http_requests_total{{pod="nginx-{i}", route="/{}", zone="eu-west-{}"}} 0+{}x{}"#,
                                i % 8,
                                i % 3,
                                1 + i % 7,
                                build_n - 1
                            );
                            promql_parser::parse_series_desc(&line).unwrap()
                        })
                        .collect::<Vec<_>>()
                });
                b.set_bytes(build_bytes);
                b.report_allocs();
                while b.keep_running() {
                    black_box(MemorySeriesSource::from_descriptions(
                        black_box(descs.as_slice()),
                        15.0,
                    ));
                }
            },
        );

    let (sel_k, sel_n) = (1_000usize, 300usize);
    let end_ms = (sel_n as i64 - 1) * SCRAPE_MS;
    for (case, selector, chunked_blocks) in [
        ("equal", r#"http_requests_total{code="200"}"#, false),
        (
            "regex",
            r#"http_requests_total{pod=~"nginx-1[0-9]+"}"#,
            false,
        ),
        (
            "narrow_then_regex",
            r#"http_requests_total{code="500", pod=~"nginx-[0-9]+"}"#,
            false,
        ),
        ("chunked_blocks", r#"http_requests_total{code="200"}"#, true),
    ] {
        suite = suite.bench(
            format!("SourceSelect/case={case}/shape={sel_k}x{sel_n}"),
            move |b| {
                let rt = runtime();
                let mut fix_opt = None;
                let (source, state, matchers, hints) = fix_opt.get_or_insert_with(|| {
                    let mut source = MemorySeriesSource::try_new(labelled(sel_k, sel_n)).unwrap();
                    if chunked_blocks {
                        source = source
                            .chunked(60_000)
                            .blocks(NonZeroU64::new(300_000).unwrap());
                    }
                    let state = SessionContext::new().state();
                    let matchers = promql_parser::parse_metric_selector(selector).unwrap();
                    let hints = SelectHints::range(0, end_ms).with_window_ms(FIVE_MINUTES_MS);
                    (source, state, matchers, hints)
                });
                b.report_allocs();
                while b.keep_running() {
                    black_box(
                        rt.block_on(source.select(
                            state,
                            black_box(matchers.as_slice()),
                            hints.clone(),
                        ))
                        .unwrap(),
                    );
                }
            },
        );
    }
    suite
}

/// `absent::labels_for` label extraction and the `promql_absent` UDAF
/// accumulator on both empty and partially populated grids.
fn register_absent<'a>(mut suite: Suite<'a>) -> Suite<'a> {
    suite = suite.bench("AbsentLabelsFor/case=mixed_matchers", move |b| {
        let mut expr_opt = None;
        let expr = expr_opt.get_or_insert_with(|| {
            promql_parser::parse_expr(
                r#"http_requests_total{job="api", instance=~"10\\..*", env="prod", region="eu", region="us"}"#,
            )
            .unwrap()
        });
        b.report_allocs();
        while b.keep_running() {
            black_box(absent::labels_for(black_box(expr)));
        }
    });

    let steps = 1_440usize;
    let end_ms = (steps as i64 - 1) * STEP_MS;
    for (case, populate) in [("empty", false), ("half_present", true)] {
        suite = suite.bench(
            format!("AbsentAccumulator/case={case}/steps={steps}"),
            move |b| {
                let mut fix_opt = None;
                let (udaf, schema, exprs, fields, return_field, column) = fix_opt
                    .get_or_insert_with(|| {
                        let udaf = absent::udaf();
                        let schema = Schema::new(vec![Field::new(
                            series::SAMPLES,
                            series::samples_type(),
                            false,
                        )]);
                        let exprs: Vec<Arc<dyn PhysicalExpr>> = vec![
                            Arc::new(Column::new(series::SAMPLES, 0)),
                            Arc::new(Literal::new(ScalarValue::Int64(Some(0)))),
                            Arc::new(Literal::new(ScalarValue::Int64(Some(end_ms)))),
                            Arc::new(Literal::new(ScalarValue::Int64(Some(STEP_MS)))),
                        ];
                        let fields: Vec<FieldRef> = exprs
                            .iter()
                            .map(|e| e.return_field(&schema).unwrap())
                            .collect();
                        let return_field =
                            Arc::new(Field::new("out", series::samples_type(), false));
                        let rows: Vec<(Vec<i64>, Vec<f64>)> = (0..10)
                            .map(|_| {
                                let ts =
                                    (0..steps).step_by(2).map(|j| j as i64 * STEP_MS).collect();
                                let vs = vec![1.0; steps / 2];
                                (ts, vs)
                            })
                            .collect();
                        let column = samples_of(&rows);
                        (udaf, schema, exprs, fields, return_field, column)
                    });
                b.report_allocs();
                while b.keep_running() {
                    let args = AccumulatorArgs {
                        return_field: Arc::clone(return_field),
                        schema,
                        ignore_nulls: false,
                        order_bys: &[],
                        is_reversed: false,
                        name: udaf.name(),
                        is_distinct: false,
                        exprs,
                        expr_fields: fields,
                    };
                    let mut acc = udaf.accumulator(args).unwrap();
                    if populate {
                        acc.update_batch(std::slice::from_ref(black_box(column)))
                            .unwrap();
                    }
                    black_box(acc.evaluate().unwrap());
                }
            },
        );
    }
    suite
}

/// Canonical Arrow batch operations across `series`: encode, decode, clip,
/// schema validation, empty-row pruning, unused-label pruning, and multi-span
/// coalescing.
fn register_series_encode_decode<'a>(suite: Suite<'a>) -> Suite<'a> {
    let (k, n) = (1_000usize, 1_000usize);
    let bytes = (k * n) as u64 * SAMPLE_BYTES;
    let (lo, hi) = (n as i64 * SCRAPE_MS / 4, n as i64 * SCRAPE_MS / 2);
    suite
        .bench(format!("Series/op=encode/shape={k}x{n}"), move |b| {
            let mut fix_opt = None;
            let (all, names) = fix_opt.get_or_insert_with(|| {
                let all = labelled(k, n);
                let names = series::label_names_of(&all);
                (all, names)
            });
            b.set_bytes(bytes);
            b.report_allocs();
            while b.keep_running() {
                black_box(series::encode(names, black_box(all), WHOLE).unwrap());
            }
        })
        .bench(format!("Series/op=decode/shape={k}x{n}"), move |b| {
            let mut batch_opt = None;
            let batch = batch_opt.get_or_insert_with(|| {
                let all = labelled(k, n);
                let names = series::label_names_of(&all);
                series::encode(&names, &all, WHOLE).unwrap()
            });
            b.set_bytes(bytes);
            b.report_allocs();
            while b.keep_running() {
                black_box(series::decode(std::slice::from_ref(black_box(batch))).unwrap());
            }
        })
        .bench(format!("Series/op=clip/shape={k}x{n}"), move |b| {
            let mut batch_opt = None;
            let batch = batch_opt.get_or_insert_with(|| {
                let all = labelled(k, n);
                let names = series::label_names_of(&all);
                series::encode(&names, &all, WHOLE).unwrap()
            });
            b.set_bytes(bytes);
            b.report_allocs();
            while b.keep_running() {
                black_box(series::clip(black_box(batch), lo, hi).unwrap());
            }
        })
        .bench(format!("Series/op=validate/shape={k}x{n}"), move |b| {
            let mut schema_opt = None;
            let schema = schema_opt.get_or_insert_with(|| {
                let all = labelled(k, n);
                let names = series::label_names_of(&all);
                series::encode(&names, &all, WHOLE).unwrap().schema()
            });
            b.report_allocs();
            while b.keep_running() {
                series::validate(black_box(schema.as_ref())).unwrap();
            }
        })
        .bench(format!("Series/op=drop_empty/shape={k}x{n}"), move |b| {
            let mut batch_opt = None;
            let batch = batch_opt.get_or_insert_with(|| {
                let all: Vec<Series> = (0..k)
                    .map(|i| {
                        let pod = format!("nginx-{i}");
                        let labels = [("__name__", "http_requests_total"), ("pod", pod.as_str())];
                        let (ts, vs) = if i % 2 == 0 {
                            counter(n)
                        } else {
                            (Vec::new(), Vec::new())
                        };
                        Series::new(&labels, ts, vs).unwrap()
                    })
                    .collect();
                let names = series::label_names_of(&all);
                series::encode(&names, &all, WHOLE).unwrap()
            });
            b.report_allocs();
            while b.keep_running() {
                black_box(series::drop_empty(black_box(batch)));
            }
        })
        .bench(
            format!("Series/op=drop_unused_labels/shape={k}x{n}"),
            move |b| {
                let mut batch_opt = None;
                let batch = batch_opt.get_or_insert_with(|| {
                    let all = labelled(k, n);
                    let mut names = series::label_names_of(&all);
                    names.extend(
                        ["unused_a", "unused_b", "unused_c"]
                            .into_iter()
                            .map(str::to_owned),
                    );
                    series::encode(&names, &all, WHOLE).unwrap()
                });
                b.report_allocs();
                while b.keep_running() {
                    black_box(series::drop_unused_labels(black_box(batch)).unwrap());
                }
            },
        )
        .bench(format!("Series/op=coalesce/shape={k}x{n}"), move |b| {
            let mut spans_opt = None;
            let spans = spans_opt.get_or_insert_with(|| {
                let half = n / 2;
                let mut out = Vec::with_capacity(k * 2);
                for i in 0..k {
                    let pod = format!("nginx-{i}");
                    let labels = [("__name__", "http_requests_total"), ("pod", pod.as_str())];
                    let (ts, vs) = counter(n);
                    out.push(
                        Series::new(&labels, ts[..half].to_vec(), vs[..half].to_vec()).unwrap(),
                    );
                    out.push(
                        Series::new(&labels, ts[half..].to_vec(), vs[half..].to_vec()).unwrap(),
                    );
                }
                out
            });
            b.set_bytes(bytes);
            b.report_allocs();
            while b.keep_running() {
                let cloned = black_box(spans.as_slice()).to_vec();
                black_box(series::coalesce(cloned).unwrap());
            }
        })
}

fn main() {
    let mut suite = Suite::from_env("promql_engine/kernels").with_alloc(&ALLOC);
    suite = register_math_add_each(suite);
    suite = register_math_reductions(suite);
    suite = register_selector_eval_series(suite);
    suite = register_selector_apply(suite);
    suite = register_range_function(suite);
    suite = register_predict_linear_series(suite);
    suite = register_range_apply(suite);
    suite = register_aggregate_add_series(suite);
    suite = register_aggregate_multi_group(suite);
    suite = register_aggregate_singleton_spans(suite);
    suite = register_aggregate_state_merge(suite);
    suite = register_binary_pairing(suite);
    suite = register_elementwise_binary(suite);
    suite = register_elementwise_math(suite);
    suite = register_elementwise_date(suite);
    suite = register_matcher(suite);
    suite = register_memory_source(suite);
    suite = register_absent(suite);
    suite = register_series_encode_decode(suite);
    suite.run();
}
