//! End-to-end benchmarks: parse, plan, execute and collect a query over
//! the in-memory source, DataFusion included.
//!
//! One engine, with its Tokio runtime and UDF registrations, and one
//! source per shape are built outside the timed closure, so an iteration
//! is what a caller of `Engine::range_query` pays. `engine/plan` isolates
//! parsing and planning, which includes the store's `select`, from
//! execution. `engine/execute_preplanned` instead resets physical templates
//! over preconstructed selected batches and drains DataFusion outputs;
//! setup, planning, source generation and API collection are excluded.
//! `engine/focused_*` covers adverse scaling shapes (see benches/README.md):
//! prepared API collection is isolated from raw drain, and only the streaming
//! probes generate input inside timing. All raw executions reset plan state.
//! Ids are `engine/query/<query>/<range>/<series>x<samples>`
//! and are chosen once: renaming a benchmark resets its history.
//!
//! ```sh
//! cargo bench -p promql-engine --bench engine -- --save-baseline before
//! # change code
//! cargo bench -p promql-engine --bench engine -- --baseline before
//! ```

use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::execution_plan::reset_plan_states;
use datafusion::physical_plan::{execute_stream, ExecutionPlan};
use futures::StreamExt;
use promql_engine::{
    Engine, EngineOptions, MemorySeriesSource, RangeQuery, Series, SeriesIdEncoding, SeriesSource,
};
use promql_parser::ParserOptions;
use std::num::NonZeroU64;

#[path = "support/focused.rs"]
mod focused;
#[path = "support/generated.rs"]
mod generated;
#[path = "support/pprof.rs"]
mod profiler;

use profiler::PProfProfiler;

const SCRAPE_MS: i64 = 15_000;
const STEP_MS: i64 = 30_000;
const HOUR_MS: i64 = 60 * 60_000;

/// Cardinality and length of a synthetic source.
struct Shape {
    series: usize,
    samples: usize,
}

const SHAPES: [Shape; 3] = [
    Shape {
        series: 100,
        samples: 1_000,
    },
    Shape {
        series: 1_000,
        samples: 1_000,
    },
    Shape {
        series: 10_000,
        samples: 300,
    },
];

impl Shape {
    fn id(&self) -> String {
        format!("{}x{}", self.series, self.samples)
    }

    fn end_ms(&self) -> i64 {
        (self.samples as i64 - 1) * SCRAPE_MS
    }

    fn elements(&self) -> u64 {
        (self.series * self.samples) as u64
    }
}

/// Counters named `http_requests_total` with `pod`, `route` and `code`
/// labels, scraped every 15s, resetting every 1000 samples.
fn synthetic_memory(shape: &Shape) -> MemorySeriesSource {
    let all = (0..shape.series)
        .map(|i| {
            let pod = format!("nginx-{i}");
            let route = format!("/{}", i % 8);
            let code = if i % 16 == 0 { "500" } else { "200" };
            let labels = [
                ("__name__", "http_requests_total"),
                ("code", code),
                ("pod", pod.as_str()),
                ("route", route.as_str()),
            ];
            let mut v = 0.0;
            let (ts, vs): (Vec<i64>, Vec<f64>) = (0..shape.samples)
                .map(|j| {
                    if j % 1000 == 0 {
                        v = 0.0;
                    }
                    v += 1.0 + ((i + j) % 7) as f64;
                    (j as i64 * SCRAPE_MS, v)
                })
                .unzip();
            Series::new(&labels, ts, vs).unwrap()
        })
        .collect();
    let source = MemorySeriesSource::try_new(all).unwrap();
    // PROMQL_BENCH_PARTS=n spreads the series over n store partitions.
    match std::env::var("PROMQL_BENCH_PARTS") {
        Ok(n) => source.partitions(n.parse().expect("PROMQL_BENCH_PARTS is a count")),
        Err(_) => source,
    }
}

fn synthetic(shape: &Shape) -> Arc<dyn SeriesSource> {
    Arc::new(synthetic_memory(shape))
}

/// An ID-encoding comparison, not an isolated executor speedup: source
/// selection and final API collection are timed as in engine/query.
fn contiguous(c: &mut Criterion) {
    let engine = Engine::blocking().unwrap();
    let shape = &SHAPES[2];
    let end = shape.end_ms();
    let range = RangeQuery::new(end - HOUR_MS, end, STEP_MS);
    let mut g = c.benchmark_group("engine/contiguous");
    g.throughput(Throughput::Elements(shape.elements()));
    for (encoding, id) in [
        ("uint64", SeriesIdEncoding::UInt64),
        ("binary16", SeriesIdEncoding::FixedSizeBinary),
    ] {
        let source = synthetic_memory(shape)
            .chunked(150_000)
            .blocks(NonZeroU64::new(900_000).unwrap())
            .with_ids(id);
        for (qname, query) in [QUERIES[0], QUERIES[3], QUERIES[4]] {
            g.bench_function(
                BenchmarkId::new(format!("{encoding}/{qname}/range_1h"), shape.id()),
                |b| {
                    b.iter(|| {
                        engine
                            .range_query(&source, black_box(query), &range)
                            .unwrap()
                    })
                },
            );
        }
    }
    g.finish();
}

/// Isolate real plan execution, including stream creation and Arrow output
/// construction but not source selection/repacking or API result collection.
/// Unlike the end-to-end contiguous family, each selection is a single block.
fn execute_preplanned(c: &mut Criterion) {
    let engine = Engine::new();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let ctx = Arc::new(TaskContext::default());
    let shape = &SHAPES[2];
    // No sample reaches this step; lookup must finish at each ID transition
    // (or EOF), not wait for the block's remaining identities.
    let at = shape.end_ms() + SCRAPE_MS;
    let instant = RangeQuery::new(at, at, STEP_MS);
    let range = RangeQuery::new(at - 30 * STEP_MS, at, STEP_MS);
    let mut g = c.benchmark_group("engine/execute_preplanned");
    g.throughput(Throughput::Elements(shape.series as u64));
    for (encoding, id) in [
        ("uint64", SeriesIdEncoding::UInt64),
        ("binary16", SeriesIdEncoding::FixedSizeBinary),
    ] {
        let source = synthetic_memory(shape).chunked(45_000).with_ids(id);
        for (qname, query, rname, range) in [
            ("selector", "http_requests_total", "one_step_tail", &instant),
            (
                "timestamp",
                "timestamp(http_requests_total)",
                "one_step_tail",
                &instant,
            ),
            (
                "rate_5m",
                "rate(http_requests_total[5m])",
                "31_steps_tail",
                &range,
            ),
            (
                "min_5m",
                "min_over_time(http_requests_total[5m])",
                "31_steps_tail",
                &range,
            ),
        ] {
            // Selection builds ID-contiguous chunks outside timing. The
            // reusable plan owns those resident batches.
            let plan = rt
                .block_on(engine.prepare_async(&source, query, range))
                .unwrap()
                .physical_plan()
                .clone();
            g.bench_function(
                BenchmarkId::new(format!("{encoding}/{qname}/{rname}"), shape.id()),
                |b| {
                    b.iter(|| {
                        rt.block_on(async {
                            let plan = reset_plan_states(Arc::clone(&plan)).unwrap();
                            let mut stream = execute_stream(plan, Arc::clone(&ctx)).unwrap();
                            while let Some(batch) = stream.next().await {
                                // Retain at most this output batch, not a result Vec.
                                black_box(batch.unwrap());
                            }
                        })
                    })
                },
            );
        }
    }
    g.finish();
}

/// The queries, from a bare selector to the SLO recording-rule shape.
///
/// The two `sort` shapes only do work in the `instant` half of the
/// matrix below: a range result is never reordered, so their `range_1h`
/// numbers are the plain selector's plus the planner's.
const QUERIES: [(&str, &str); 7] = [
    ("selector", "http_requests_total"),
    ("sum", "sum(http_requests_total)"),
    ("sum_by_route", "sum by (route) (http_requests_total)"),
    ("rate_5m", "rate(http_requests_total[5m])"),
    (
        "sum_by_route_increase_5m",
        "sum by (route) (increase(http_requests_total[5m]))",
    ),
    ("sort", "sort(http_requests_total)"),
    (
        "sort_by_label_pod",
        r#"sort_by_label(http_requests_total, "pod")"#,
    ),
];

/// Every query at every shape, over the last hour of data at 30s and as
/// an instant query at the last sample.
fn query(c: &mut Criterion) {
    // `sort_by_label` is an experimental function.
    let engine = Engine::blocking_with_options(EngineOptions {
        parser: ParserOptions {
            enable_experimental_functions: true,
            ..Default::default()
        },
    })
    .unwrap();
    let mut g = c.benchmark_group("engine/query");
    for shape in &SHAPES {
        let source = synthetic(shape);
        let end = shape.end_ms();
        let ranges = [
            ("range_1h", RangeQuery::new(end - HOUR_MS, end, STEP_MS)),
            ("instant", RangeQuery::new(end, end, STEP_MS)),
        ];
        g.throughput(Throughput::Elements(shape.elements()));
        for (qname, q) in QUERIES {
            for (rname, range) in &ranges {
                g.bench_function(
                    BenchmarkId::new(format!("{qname}/{rname}"), shape.id()),
                    |b| {
                        b.iter(|| {
                            engine
                                .range_query(source.as_ref(), black_box(q), range)
                                .unwrap()
                        })
                    },
                );
            }
        }
    }
    g.finish();
}

/// The pinned five-minute window must be refit at every step because
/// Prometheus anchors the regression intercept on evaluation time.
fn linear_regression_queries(c: &mut Criterion) {
    let engine = Engine::blocking().unwrap();
    let shape = &SHAPES[1];
    let source = synthetic(shape);
    let end = shape.end_ms();
    let range = RangeQuery::new(end - HOUR_MS, end, STEP_MS);
    let pinned = format!(
        "predict_linear(http_requests_total[5m] @ {}, 3600)",
        end / 1000
    );
    let queries = [
        ("deriv_5m", "deriv(http_requests_total[5m])"),
        (
            "predict_linear_5m",
            "predict_linear(http_requests_total[5m], 3600)",
        ),
        ("predict_linear_5m_at_pinned", pinned.as_str()),
    ];
    let mut g = c.benchmark_group("engine/linear_regression");
    g.throughput(Throughput::Elements(shape.elements()));
    for (name, query) in queries {
        g.bench_function(BenchmarkId::new(name, shape.id()), |b| {
            b.iter(|| {
                engine
                    .range_query(source.as_ref(), black_box(query), &range)
                    .unwrap()
            })
        });
    }
    g.finish();
}

/// The elementwise operator's own cost, over the last hour at 30s.
///
/// Each case is a function wrapped round a query `engine/query` already
/// measures, so the difference is one more projection and one pass over
/// the values: `abs` is the cheapest of them, and `clamp` carries two
/// scalar arguments through the same pass. Ids are
/// `engine/elementwise/<query>/<series>x<samples>`.
fn elementwise(c: &mut Criterion) {
    let engine = Engine::blocking().unwrap();
    let shape = &SHAPES[2];
    let source = synthetic(shape);
    let end = shape.end_ms();
    let range = RangeQuery::new(end - HOUR_MS, end, STEP_MS);
    let mut g = c.benchmark_group("engine/elementwise");
    g.throughput(Throughput::Elements(shape.elements()));
    for (qname, q) in [
        ("abs_selector", "abs(http_requests_total)"),
        ("clamp_selector", "clamp(http_requests_total, 0, 1000)"),
        ("abs_rate_5m", "abs(rate(http_requests_total[5m]))"),
    ] {
        g.bench_function(BenchmarkId::new(qname, shape.id()), |b| {
            b.iter(|| {
                engine
                    .range_query(source.as_ref(), black_box(q), &range)
                    .unwrap()
            })
        });
    }
    g.finish();
}

/// The two shapes a binary operator takes, over the same source.
///
/// A scalar operand is folded while planning, so `vector_scalar` is one
/// more pass over the values and nothing else — the same price as
/// `engine/elementwise`. `vector_vector` is what the pairing costs: both
/// sides widened to one schema, unioned, and grouped by the match
/// signature, which over this source is one group per series. Ids are
/// `engine/binary/<shape>/<series>x<samples>`.
fn binary(c: &mut Criterion) {
    // `fill` is experimental syntax.
    let engine = Engine::blocking_with_options(EngineOptions {
        parser: ParserOptions {
            enable_binop_fill_modifiers: true,
            ..Default::default()
        },
    })
    .unwrap();
    let shape = &SHAPES[2];
    let source = synthetic(shape);
    let end = shape.end_ms();
    let range = RangeQuery::new(end - HOUR_MS, end, STEP_MS);
    let mut g = c.benchmark_group("engine/binary");
    g.throughput(Throughput::Elements(shape.elements()));
    for (qname, q) in [
        ("vector_scalar", "http_requests_total * 2"),
        // A comparison is the one that rebuilds the row boundaries
        // rather than only the values, and over this source it keeps
        // most of them, which is the expensive half of that path.
        ("vector_scalar_compare", "http_requests_total > 100"),
        ("vector_vector", "http_requests_total + http_requests_total"),
        // The fan-out: every series of a route matches the one sum of
        // it, so a match group here is a lane per series rather than
        // the single pair the cases above pay for.
        (
            "group_left",
            "http_requests_total / on(route) group_left() sum by (route) (http_requests_total)",
        ),
        // A set operator holds both sides of every match group whole,
        // and `or` is the one that then emits both — the widest a group
        // gets for the least arithmetic.
        (
            "or",
            "http_requests_total or on(route) sum by (route) (http_requests_total)",
        ),
        // A fill turns the sides that did not match from something to
        // skip into something to answer for, which is the per-step
        // bookkeeping at its widest.
        (
            "fill",
            "http_requests_total / on(route) group_left() fill(0) sum by (route) (http_requests_total)",
        ),
    ] {
        g.bench_function(BenchmarkId::new(qname, shape.id()), |b| {
            b.iter(|| {
                engine
                    .range_query(source.as_ref(), black_box(q), &range)
                    .unwrap()
            })
        });
    }
    g.finish();

    // A day of a 15s scrape at a 60s step: 1440 steps per series instead of
    // 121, which is the size at which the per-step lanes of the pairing and
    // the per-sample loop of the elementwise path dominate the fixed costs
    // the hour above is mostly made of. Ids are
    // `engine/binary/<shape>_24h/<series>x<samples>`.
    let day = Shape {
        series: 1_000,
        samples: 5_760,
    };
    let source = synthetic(&day);
    let range = RangeQuery::new(0, day.end_ms(), 60_000);
    let mut g = c.benchmark_group("engine/binary");
    g.throughput(Throughput::Elements(day.elements()));
    for (qname, q) in [
        (
            "vector_vector_24h",
            "http_requests_total / http_requests_total",
        ),
        ("vector_scalar_24h", "http_requests_total / 2"),
        ("vector_scalar_compare_24h", "http_requests_total > 100"),
    ] {
        g.bench_function(BenchmarkId::new(qname, day.id()), |b| {
            b.iter(|| {
                engine
                    .range_query(source.as_ref(), black_box(q), &range)
                    .unwrap()
            })
        });
    }
    g.finish();
}

/// Parsing and logical planning alone, including the store's `select`.
/// The gap to `engine/query` also includes physical lowering and API
/// collection/postprocessing; use execute_preplanned to isolate execution.
fn plan(c: &mut Criterion) {
    let engine = Engine::new();
    // Planning is only exposed as `plan_async`, and the runtime an
    // `Engine::blocking()` owns is private, so the bench drives its own.
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let (qname, q) = QUERIES[4];
    let mut g = c.benchmark_group("engine/plan");
    for shape in &SHAPES {
        let source = synthetic(shape);
        let end = shape.end_ms();
        let range = RangeQuery::new(end - HOUR_MS, end, STEP_MS);
        g.throughput(Throughput::Elements(shape.series as u64));
        g.bench_function(
            BenchmarkId::new(format!("{qname}/range_1h"), shape.id()),
            |b| {
                b.iter(|| {
                    rt.block_on(engine.plan_async(source.as_ref(), black_box(q), &range))
                        .unwrap()
                })
            },
        );
    }
    g.finish();
}

/// Raw execution always gets a reset tree, including the first iteration;
/// an Arc clone alone does not reset DataFusion exchanges.
fn drain(rt: &tokio::runtime::Runtime, plan: &Arc<dyn ExecutionPlan>, ctx: &Arc<TaskContext>) {
    rt.block_on(async {
        let plan = reset_plan_states(plan.clone()).unwrap();
        let mut stream = execute_stream(plan, ctx.clone()).unwrap();
        while let Some(batch) = stream.next().await {
            black_box(batch.unwrap());
        }
    });
}

fn points(batches: &[datafusion::arrow::array::RecordBatch]) -> usize {
    use datafusion::arrow::array::AsArray;
    batches
        .iter()
        .map(|batch| {
            let samples = batch
                .column_by_name(promql_engine::series::SAMPLES)
                .unwrap()
                .as_list::<i32>();
            (samples.offsets()[batch.num_rows()] - samples.offsets()[0]) as usize
        })
        .sum()
}

/// Scaling comparisons. API collection and raw drain are deliberately
/// separate boundaries; the former retains the answer, the latter does not.
fn focused(c: &mut Criterion) {
    use generated::{ChunkProbe, Generated, Shape};
    let engine = Engine::new();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let ctx = Arc::new(TaskContext::default());
    let mut g = c.benchmark_group("engine/focused_collect");
    for count in [1024usize, 16384] {
        g.throughput(Throughput::Elements(count as u64));
        for shuffled in [false, true] {
            let source = focused::wide_source(count, shuffled, false);
            let prepared = rt
                .block_on(engine.prepare_async(
                    source.as_ref(),
                    focused::SELECTOR,
                    &focused::instant(),
                ))
                .unwrap();
            let out = rt.block_on(engine.execute_async(&prepared)).unwrap();
            assert_eq!(points(&out), count);
            drop(out);
            let name = if shuffled {
                "shuffled_wide"
            } else {
                "ordered_wide"
            };
            g.bench_function(BenchmarkId::new(name, count), |b| {
                b.iter(|| {
                    rt.block_on(engine.execute_async(black_box(&prepared)))
                        .unwrap()
                })
            });
        }
    }
    let count = 16384;
    let source = focused::wide_source(count, true, true);
    let empty = rt
        .block_on(engine.prepare_async(source.as_ref(), focused::SELECTOR, &focused::instant()))
        .unwrap();
    assert_eq!(
        points(&rt.block_on(engine.execute_async(&empty)).unwrap()),
        0
    );
    g.throughput(Throughput::Elements(count as u64));
    g.bench_function(BenchmarkId::new("empty_stale_wide", count), |b| {
        b.iter(|| {
            rt.block_on(engine.execute_async(black_box(&empty)))
                .unwrap()
        })
    });
    g.finish();

    let source = focused::wide_source(count, true, false);
    let prepared = rt
        .block_on(engine.prepare_async(source.as_ref(), focused::SELECTOR, &focused::instant()))
        .unwrap();
    let plan = prepared.physical_plan().clone();
    let mut g = c.benchmark_group("engine/focused_drain");
    g.throughput(Throughput::Elements(count as u64));
    g.bench_function(BenchmarkId::new("shuffled_wide", count), |b| {
        b.iter(|| drain(&rt, black_box(&plan), &ctx))
    });
    g.finish();

    let mut g = c.benchmark_group("engine/focused_sparse");
    for (groups, blocks) in [(512usize, 8usize), (4096, 24)] {
        let source = focused::sparse_source(groups, blocks);
        let prepared = rt
            .block_on(engine.prepare_async(
                source.as_ref(),
                focused::SPARSE_SUM,
                &focused::sparse_range(blocks),
            ))
            .unwrap();
        let out = rt.block_on(engine.execute_async(&prepared)).unwrap();
        assert_eq!(points(&out), groups * blocks);
        drop(out);
        let plan = prepared.physical_plan().clone();
        g.throughput(Throughput::Elements((groups * blocks * 2) as u64));
        g.bench_function(
            BenchmarkId::new("sum", format!("{groups}groups_{blocks}blocks")),
            |b| b.iter(|| drain(&rt, black_box(&plan), &ctx)),
        );
    }
    g.finish();

    let mut g = c.benchmark_group("engine/focused_streaming");
    for (name, probe) in [
        ("whole", ChunkProbe::Whole),
        ("second", ChunkProbe::Second),
        ("stale", ChunkProbe::Stale),
    ] {
        let source = Generated::streamed_with_ids(
            Shape {
                series: 4,
                samples: 131072,
                scrape_ms: 1000,
                chunk: None,
                rows_per_batch: 1,
            },
            SeriesIdEncoding::UInt64,
        )
        .chunk_probe(probe);
        let range = RangeQuery::new(0, 60_000, 1000);
        let prepared = rt
            .block_on(engine.prepare_async(&source, focused::RATE, &range))
            .unwrap();
        assert_eq!(
            points(&rt.block_on(engine.execute_async(&prepared)).unwrap()),
            4 * 60
        );
        let plan = prepared.physical_plan().clone();
        g.throughput(Throughput::Elements((4 * 131072) as u64));
        g.bench_function(BenchmarkId::new(name, "4x131072"), |b| {
            b.iter(|| drain(&rt, black_box(&plan), &ctx))
        });
    }
    g.finish();

    let mut g = c.benchmark_group("engine/focused_churn");
    for count in [1024usize, 8192] {
        let source = Generated::resident(Shape {
            series: count,
            samples: 32,
            scrape_ms: 1000,
            chunk: Some(4),
            rows_per_batch: 1024,
        });
        let range = RangeQuery::new(0, 31_000, 1000);
        let prepared = rt
            .block_on(engine.prepare_async(&source, focused::RATE, &range))
            .unwrap();
        assert_eq!(
            points(&rt.block_on(engine.execute_async(&prepared)).unwrap()),
            count * 31
        );
        let plan = prepared.physical_plan().clone();
        g.throughput(Throughput::Elements(count as u64));
        g.bench_function(BenchmarkId::new("rate_32steps", count), |b| {
            b.iter(|| drain(&rt, black_box(&plan), &ctx))
        });
    }
    g.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .with_profiler(PProfProfiler::new(100))
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(3))
        .sample_size(20);
    targets = query, linear_regression_queries, elementwise, binary, plan, contiguous, execute_preplanned, focused
}
criterion_main!(benches);
