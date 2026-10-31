//! Numerical-kernel microbenchmarks and isolated preplanned series evaluation.
//!
//! `series_eval/*` includes DataFusion stream creation, the real executor and
//! Arrow output construction/draining and execution-state reset, but excludes
//! source selection/planning. Other groups drive public kernels/accumulators
//! directly. `aggregate/singleton_spans` isolates legal descending span growth;
//! benches/README.md gives the bounded serial comparison and profile commands.
//!
//! Every group is named `area/function` and parameterized by input size,
//! so a tracker keys a stable history on ids like
//! `series_eval/selector/plain/16000`. Names are chosen once: renaming a
//! benchmark resets its history. Fixtures are deterministic and built
//! outside the timed closure. To compare two states of the code:
//!
//! ```sh
//! cargo bench -p promql-engine --bench kernels -- --save-baseline before
//! # change code
//! cargo bench -p promql-engine --bench kernels -- --baseline before
//! ```

use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;

use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput};
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
use futures::StreamExt;
use promql_engine::aggregate::{Grouped, Op};
use promql_engine::binary::{self, Op as BinOp};
use promql_engine::elementwise;
use promql_engine::math::{self, FlagLane, Welford};
use promql_engine::range::Func;
use promql_engine::selector::STALE_NAN_BITS;
use promql_engine::series::{self, Block, Series};
use promql_engine::{Engine, MemorySeriesSource, Params, RangeQuery};

#[path = "support/pprof.rs"]
mod profiler;

use profiler::PProfProfiler;

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

/// The elementwise kernels behind `Steps::add_run`: one slice of values
/// folded into as many accumulator lanes. 240 lanes is an hour at 15s,
/// 1440 a day at 60s.
fn math_add_each(c: &mut Criterion) {
    let mut g = c.benchmark_group("math/add_each");
    for lanes in [240usize, 1440] {
        let values: Vec<f64> = (0..lanes).map(|i| 1.0 + (i % 7) as f64).collect();
        g.throughput(Throughput::Elements(lanes as u64));

        let (mut sums, mut comps) = (vec![0.0; lanes], vec![0.0; lanes]);
        g.bench_function(BenchmarkId::new("kahan", lanes), |b| {
            b.iter(|| {
                math::kahan_add_each(&mut sums, &mut comps, black_box(&values));
                black_box(&sums);
            })
        });

        let (mut acc, mut comps, mut counts) =
            (vec![0.0; lanes], vec![0.0; lanes], vec![0.0; lanes]);
        let mut incremental = BooleanBufferBuilder::new(lanes);
        incremental.append_n(lanes, false);
        g.bench_function(BenchmarkId::new("mean", lanes), |b| {
            b.iter(|| {
                math::mean_add_each(
                    &mut acc,
                    &mut comps,
                    &mut counts,
                    FlagLane::new(&mut incremental, 0, lanes),
                    black_box(&values),
                );
                black_box(&acc);
            })
        });

        let mut cur = vec![f64::NAN; lanes];
        g.bench_function(BenchmarkId::new("max", lanes), |b| {
            b.iter(|| {
                math::max_add_each(&mut cur, black_box(&values));
                black_box(&cur);
            })
        });

        let (mut means, mut m2s, mut counts) =
            (vec![0.0; lanes], vec![0.0; lanes], vec![0.0; lanes]);
        g.bench_function(BenchmarkId::new("welford", lanes), |b| {
            b.iter(|| {
                math::welford_add_each(&mut means, &mut m2s, &mut counts, black_box(&values));
                black_box(&means);
            })
        });
    }
    g.finish();
}

/// The sequential reductions behind the `*_over_time` functions.
fn math_reductions(c: &mut Criterion) {
    let mut g = c.benchmark_group("math/reductions");
    for n in LENGTHS {
        let (_, vs) = counter(n);
        g.throughput(Throughput::Elements(n as u64));
        g.bench_function(BenchmarkId::new("kahan_sum", n), |b| {
            b.iter(|| math::kahan_sum(black_box(&vs)).value())
        });
        g.bench_function(BenchmarkId::new("mean_of", n), |b| {
            b.iter(|| math::mean_of(black_box(&vs)))
        });
        g.bench_function(BenchmarkId::new("variance_of", n), |b| {
            b.iter(|| math::variance_of(black_box(&vs)))
        });
        // No slice-at-a-time welford reduction exists; the `stddev` and
        // `stdvar` aggregations fold the accumulator a sample at a time, so
        // the bench folds it too.
        g.bench_function(BenchmarkId::new("welford_of", n), |b| {
            b.iter(|| {
                let mut w = Welford::default();
                for v in black_box(&vs) {
                    w.add(*v);
                }
                w.variance()
            })
        });
    }
    g.finish();
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
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
    rt.block_on(Engine::new().prepare_async(
        &source,
        &query,
        &RangeQuery::new(p.start_ms, p.end_ms, p.step_ms),
    ))
    .unwrap()
    .physical_plan()
    .clone()
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

/// One series walked across the grid by the real `SeriesEvalExec`.
fn selector_eval_series(c: &mut Criterion) {
    let rt = runtime();
    let ctx = Arc::new(TaskContext::default());
    let mut g = c.benchmark_group("series_eval/selector");
    for n in LENGTHS {
        let (ts, vs) = counter(n);
        let stale = with_stale(&vs, 100);
        let p = selector_params(n);
        let pinned = Params {
            at_ms: Some(p.end_ms),
            ..p
        };
        g.throughput(Throughput::Elements(n as u64));
        for (name, rows, p) in [
            ("plain", single_counter(ts.clone(), vs.clone()), &p),
            ("stale_every_100", single_counter(ts.clone(), stale), &p),
            ("at_pinned", single_counter(ts, vs), &pinned),
        ] {
            let plan = selected_plan(&rt, None, p, rows);
            assert_answers(&rt, &plan, &ctx);
            g.bench_function(BenchmarkId::new(name, n), |b| {
                b.iter(|| drain_plan(&rt, black_box(&plan), &ctx))
            });
        }
    }
    g.finish();
}

/// A thousand identities; output construction is timed, selection is not.
fn selector_apply(c: &mut Criterion) {
    let (k, n) = (1_000usize, 1_000usize);
    let rt = runtime();
    let ctx = Arc::new(TaskContext::default());
    let plan = selected_plan(&rt, None, &selector_params(n), labelled(k, n));
    assert_answers(&rt, &plan, &ctx);
    let mut g = c.benchmark_group("series_eval/selector_apply");
    g.throughput(Throughput::Elements((k * n) as u64));
    g.bench_function(BenchmarkId::from_parameter(format!("{k}x{n}")), |b| {
        b.iter(|| drain_plan(&rt, black_box(&plan), &ctx))
    });
    g.finish();
}

/// Buffer and sweep internals stay private; this times their production driver.
fn range_function(c: &mut Criterion) {
    let rt = runtime();
    let ctx = Arc::new(TaskContext::default());
    let mut g = c.benchmark_group("series_eval/range");
    for n in LENGTHS {
        let (ts, vs) = counter(n);
        let stale = with_stale(&vs, 100);
        let p = range_params(n);
        g.throughput(Throughput::Elements(n as u64));
        for (name, func, values) in [
            ("rate", Func::Rate, &vs),
            ("increase", Func::Increase, &vs),
            ("count_over_time", Func::CountOverTime, &vs),
            ("max_over_time", Func::MaxOverTime, &vs),
            ("stddev_over_time", Func::StddevOverTime, &vs),
            ("deriv", Func::Deriv, &vs),
            ("rate_stale_every_100", Func::Rate, &stale),
        ] {
            let plan = selected_plan(
                &rt,
                Some(BenchRangeFunc::Func(func)),
                &p,
                single_counter(ts.clone(), values.clone()),
            );
            assert_answers(&rt, &plan, &ctx);
            g.bench_function(BenchmarkId::new(name, n), |b| {
                b.iter(|| drain_plan(&rt, black_box(&plan), &ctx))
            });
        }
    }
    g.finish();
}

/// The pinned window is reused, while `predict_linear` refits at every
/// evaluation timestamp because Prometheus anchors the intercept there.
fn predict_linear_series(c: &mut Criterion) {
    let rt = runtime();
    let ctx = Arc::new(TaskContext::default());
    let mut g = c.benchmark_group("series_eval/predict_linear");
    for n in LENGTHS {
        let (ts, vs) = counter(n);
        let p = range_params(n);
        let pinned = Params {
            at_ms: Some(p.end_ms),
            ..p
        };
        g.throughput(Throughput::Elements(n as u64));
        for (name, params) in [("moving", &p), ("at_pinned", &pinned)] {
            let plan = selected_plan(
                &rt,
                Some(BenchRangeFunc::PredictLinear),
                params,
                single_counter(ts.clone(), vs.clone()),
            );
            assert_answers(&rt, &plan, &ctx);
            g.bench_function(BenchmarkId::new(name, n), |b| {
                b.iter(|| drain_plan(&rt, black_box(&plan), &ctx))
            });
        }
    }
    g.finish();
}

/// `rate` over a thousand identities through the sole executor.
fn range_apply(c: &mut Criterion) {
    let (k, n) = (1_000usize, 1_000usize);
    let rt = runtime();
    let ctx = Arc::new(TaskContext::default());
    let plan = selected_plan(
        &rt,
        Some(BenchRangeFunc::Func(Func::Rate)),
        &range_params(n),
        labelled(k, n),
    );
    assert_answers(&rt, &plan, &ctx);
    let mut g = c.benchmark_group("series_eval/range_apply");
    g.throughput(Throughput::Elements((k * n) as u64));
    g.bench_function(BenchmarkId::new("rate", format!("{k}x{n}")), |b| {
        b.iter(|| drain_plan(&rt, black_box(&plan), &ctx))
    });
    g.finish();
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
fn aggregate_add_series(c: &mut Criterion) {
    let steps = 1440usize;
    let end_ms = (steps as i64 - 1) * STEP_MS;
    let mut g = c.benchmark_group("aggregate/add_series");
    for k in [100usize, 1_000] {
        let column = samples_of(&aligned(k, steps));
        let one = vec![0usize; k];
        g.throughput(Throughput::Elements((k * steps) as u64));
        for (name, op) in [
            ("sum", Op::Sum),
            ("avg", Op::Avg),
            ("stddev", Op::Stddev),
            ("max", Op::Max),
        ] {
            g.bench_function(BenchmarkId::new(name, k), |b| {
                b.iter(|| filled(op, black_box(&column), &one, end_ms, 1))
            });
        }
    }
    g.finish();
}

/// Singleton contributions in legal between-ID order. This isolates lane
/// front growth in Prometheus `aggregation` (83962c35) from source/planner
/// work. Ascending is the control; descending must not repeatedly move the
/// entire accumulated span. Values and update order are never pre-reduced.
fn aggregate_singleton_spans(c: &mut Criterion) {
    let mut g = c.benchmark_group("aggregate/singleton_spans");
    for steps in [1024usize, 8192] {
        let end_ms = (steps as i64 - 1) * STEP_MS;
        let groups = vec![0; steps];
        g.throughput(Throughput::Elements(steps as u64));
        for descending in [false, true] {
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
            let name = if descending {
                "descending"
            } else {
                "ascending"
            };
            g.bench_function(BenchmarkId::new(name, steps), |b| {
                b.iter(|| filled(Op::Sum, black_box(&column), &groups, end_ms, 1))
            });
        }
    }
    g.finish();
}

/// The shuffle between the two aggregation phases: serializing one
/// accumulator's state and merging it into another. Emitting the state
/// consumes the accumulator's lanes, so each sample refills one outside
/// the timed closure.
fn aggregate_state_merge(c: &mut Criterion) {
    let steps = 1440usize;
    let end_ms = (steps as i64 - 1) * STEP_MS;
    let column = samples_of(&aligned(100, steps));
    let one = vec![0usize; 100];
    let mut g = c.benchmark_group("aggregate/state_merge");
    g.throughput(Throughput::Elements(steps as u64));
    for (name, op) in [("sum", Op::Sum), ("avg", Op::Avg), ("stddev", Op::Stddev)] {
        g.bench_function(BenchmarkId::new(format!("{name}/state"), steps), |b| {
            b.iter_batched(
                || filled(op, &column, &one, end_ms, 1),
                |mut acc| acc.state(EmitTo::All).unwrap(),
                BatchSize::SmallInput,
            )
        });
        let state: ArrayRef = filled(op, &column, &one, end_ms, 1)
            .state(EmitTo::All)
            .unwrap()
            .remove(0);
        g.bench_function(
            BenchmarkId::new(format!("{name}/merge_batch"), steps),
            |b| {
                b.iter(|| {
                    let mut acc = Grouped::new(op, 0, end_ms, STEP_MS).unwrap();
                    acc.merge_batch(std::slice::from_ref(black_box(&state)), &[0], 1)
                        .unwrap();
                    acc.evaluate(EmitTo::All).unwrap()
                })
            },
        );
    }
    g.finish();
}

/// The plan's `promql_binary` aggregate over match groups: a fresh accumulator, one left and one right row, then
/// `evaluate`. Accumulator construction is inside the timed closure because
/// the plan builds one per match group too, and at 1 group against 1000 the
/// pair separates the per-step lane work from that fixed cost. Fixtures are
/// sliced out of one encoded batch beforehand, so only the kernel is timed.
/// Ids are `binary/pairing/<op>/<groups>x<steps>`.
fn binary_pairing(c: &mut Criterion) {
    let steps = 1440usize;
    let step_ms = 60_000i64;
    let end_ms = (steps as i64 - 1) * step_ms;
    let mut g = c.benchmark_group("binary/pairing");
    for groups in [1usize, 1_000] {
        // Left and right of group `i` are rows `2i` and `2i + 1`. Left
        // ranges over 1..=100 and right sits at 50.5, so a `>` keeps about
        // half the steps and never ties.
        let rows: Vec<Series> = (0..groups * 2)
            .map(|row| {
                let (i, right) = (row / 2, row % 2 == 1);
                let id = format!("g-{i}");
                // The two sides of a batch cannot share a label set, so
                // they differ by name like `a / b` does.
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
        g.throughput(Throughput::Elements((groups * steps) as u64));
        for (name, op, return_bool) in [
            ("add", BinOp::Add, false),
            ("div", BinOp::Div, false),
            ("gtr", BinOp::Gtr, false),
            ("gtr_bool", BinOp::Gtr, true),
        ] {
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
            let args = AccumulatorArgs {
                return_field: Arc::new(Field::new("out", binary::output_type(&names), false)),
                schema: &schema,
                ignore_nulls: false,
                order_bys: &[],
                is_reversed: false,
                name: udaf.name(),
                is_distinct: false,
                exprs: &exprs,
                expr_fields: &fields,
            };
            // A grid the literals got wrong pairs nothing, and the bench
            // would time an empty walk.
            let mut probe = udaf.accumulator(args.clone()).unwrap();
            probe.update_batch(&pairs[0]).unwrap();
            let ScalarValue::List(out) = probe.evaluate().unwrap() else {
                panic!("a list of output series")
            };
            assert!(!out.value(0).is_empty());
            g.bench_function(BenchmarkId::new(name, format!("{groups}x{steps}")), |b| {
                b.iter(|| {
                    for pair in &pairs {
                        let mut acc = udaf.accumulator(args.clone()).unwrap();
                        acc.update_batch(black_box(pair)).unwrap();
                        black_box(acc.evaluate().unwrap());
                    }
                })
            });
        }
    }
    g.finish();
}

/// `elementwise::apply` under a binary operator's bound, the vector-scalar
/// path. The sawtooth values straddle 100, so `> 100` keeps about half and
/// the filtered path rebuilds row boundaries for a realistic fraction,
/// where `* 2` leaves them alone. Ids are
/// `elementwise/binary/<op>/<rows>x<samples>`.
fn elementwise_binary(c: &mut Criterion) {
    let (rows, samples) = (1_000usize, 1_440usize);
    let all: Vec<(Vec<i64>, Vec<f64>)> = (0..rows)
        .map(|i| {
            let ts = (0..samples).map(|j| j as i64 * 60_000).collect();
            let vs = (0..samples).map(|j| ((i * 7 + j) % 200) as f64).collect();
            (ts, vs)
        })
        .collect();
    let column = samples_of(&all);
    let list: &ListArray = column.as_list::<i32>();
    let mut g = c.benchmark_group("elementwise/binary");
    g.throughput(Throughput::Elements((rows * samples) as u64));
    for (name, spelling, scalar) in [
        ("mul", "*", 2.0),
        ("gtr", ">", 100.0),
        ("gtr_bool", "> bool", 100.0),
    ] {
        let bound = elementwise::Func::parse(spelling)
            .unwrap()
            .bind(Some(scalar), None);
        g.bench_function(BenchmarkId::new(name, format!("{rows}x{samples}")), |b| {
            b.iter(|| elementwise::apply(black_box(list), bound))
        });
    }
    g.finish();
}

/// `elementwise::apply` under a date extractor, the per-sample map path.
/// The values are Unix timestamps a minute apart from early 2024, what
/// `hour(timestamp(x))` hands the extractor, so the calendar arithmetic
/// runs on the dates a real query reaches rather than on small integers.
/// Ids are `elementwise/date/<function>/<rows>x<samples>`.
fn elementwise_date(c: &mut Criterion) {
    let (rows, samples) = (1_000usize, 1_440usize);
    let all: Vec<(Vec<i64>, Vec<f64>)> = (0..rows)
        .map(|i| {
            let ts = (0..samples).map(|j| j as i64 * 60_000).collect();
            let vs = (0..samples)
                .map(|j| 1_704_067_200.0 + (i * 86_400 + j * 60) as f64)
                .collect();
            (ts, vs)
        })
        .collect();
    let column = samples_of(&all);
    let list: &ListArray = column.as_list::<i32>();
    let mut g = c.benchmark_group("elementwise/date");
    g.throughput(Throughput::Elements((rows * samples) as u64));
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
        let bound = elementwise::Func::parse(name).unwrap().bind(None, None);
        g.bench_function(BenchmarkId::new(name, format!("{rows}x{samples}")), |b| {
            b.iter(|| elementwise::apply(black_box(list), bound))
        });
    }
    g.finish();
}

/// The two edges of the seam: rows into a batch and back, plus clipping
/// one series to a range.
fn series_encode_decode(c: &mut Criterion) {
    let (k, n) = (1_000usize, 1_000usize);
    let all = labelled(k, n);
    let names = series::label_names_of(&all);
    let batch = series::encode(&names, &all, WHOLE).unwrap();
    let mut g = c.benchmark_group("series");
    g.throughput(Throughput::Elements((k * n) as u64));
    g.bench_function(BenchmarkId::new("encode", format!("{k}x{n}")), |b| {
        b.iter(|| series::encode(&names, black_box(&all), WHOLE).unwrap())
    });
    g.bench_function(BenchmarkId::new("decode", format!("{k}x{n}")), |b| {
        b.iter(|| series::decode(std::slice::from_ref(black_box(&batch))).unwrap())
    });
    let (lo, hi) = (n as i64 * SCRAPE_MS / 4, n as i64 * SCRAPE_MS / 2);
    g.bench_function(BenchmarkId::new("clip", format!("{k}x{n}")), |b| {
        b.iter(|| series::clip(black_box(&batch), lo, hi).unwrap())
    });
    g.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .with_profiler(PProfProfiler::new(100))
        .warm_up_time(Duration::from_millis(500))
        .measurement_time(Duration::from_secs(2))
        .sample_size(50);
    targets =
        math_add_each,
        math_reductions,
        selector_eval_series,
        selector_apply,
        range_function,
        predict_linear_series,
        range_apply,
        aggregate_add_series,
        aggregate_singleton_spans,
        aggregate_state_merge,
        binary_pairing,
        elementwise_binary,
        elementwise_date,
        series_encode_decode
}
criterion_main!(benches);
