//! End-to-end benchmarks: parse, plan, execute, and collect a query over
//! the in-memory source, DataFusion included.
//!
//! Engines and per-shape sources are initialized lazily outside the timed
//! `keep_running` window so filtered runs do not construct unused datasets.
//! `EnginePlan` isolates parsing and logical planning (including the store's
//! `select`) from execution. `EngineExecutePreplanned` resets physical
//! templates over preconstructed selected batches and drains DataFusion
//! outputs without planning or API result collection. `EngineFocused*` covers
//! adverse scaling shapes (see `benches/README.md`). All raw executions reset
//! plan state before each run.
//!
//! Recommended CLI invocation:
//! ```sh
//! cargo bench -p promql-engine --bench engine -- -bench '^BenchmarkEngine' -benchtime 1s -count 6 -cpu 2 -benchmem | tee ${bench}.txt
//! ```

use std::hint::black_box;
use std::num::NonZeroU64;
use std::sync::{Arc, OnceLock};

use benchfmt::{ProcessAlloc, Suite};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::execution_plan::reset_plan_states;
use datafusion::physical_plan::{execute_stream, ExecutionPlan};
use futures::StreamExt;
use promql_engine::{
    explain_physical_plan, explain_plan, Engine, EngineOptions, InstantResult, MemorySeriesSource,
    RangeQuery, Series, SeriesIdEncoding, SeriesSource,
};
use promql_parser::ParserOptions;

#[path = "support/focused.rs"]
mod focused;
#[path = "support/generated.rs"]
mod generated;

#[global_allocator]
static ALLOC: ProcessAlloc = ProcessAlloc::new();

const SCRAPE_MS: i64 = 15_000;
const STEP_MS: i64 = 30_000;
const HOUR_MS: i64 = 60 * 60_000;
const SAMPLE_BYTES: u64 = 16;

/// Cardinality and length of a synthetic source.
#[derive(Clone, Copy)]
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

    fn bytes(&self) -> u64 {
        (self.series * self.samples) as u64 * SAMPLE_BYTES
    }
}

/// Counters named `http_requests_total` with `pod`, `route`, and `code`
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

fn shared_source(index: usize) -> &'static Arc<dyn SeriesSource> {
    static SOURCES: [OnceLock<Arc<dyn SeriesSource>>; 3] =
        [OnceLock::new(), OnceLock::new(), OnceLock::new()];
    SOURCES[index].get_or_init(|| Arc::new(synthetic_memory(&SHAPES[index])))
}

fn blocking_engine() -> &'static Engine {
    static ENGINE: OnceLock<Engine> = OnceLock::new();
    ENGINE.get_or_init(|| {
        Engine::blocking_with_options(EngineOptions {
            parser: ParserOptions::all(),
        })
        .unwrap()
    })
}

fn async_engine() -> &'static Engine {
    static ENGINE: OnceLock<Engine> = OnceLock::new();
    ENGINE.get_or_init(|| {
        Engine::with_options(EngineOptions {
            parser: ParserOptions::all(),
        })
    })
}

fn current_thread_rt() -> &'static tokio::runtime::Runtime {
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
fn register_query<'a>(mut suite: Suite<'a>) -> Suite<'a> {
    for (idx, shape) in SHAPES.iter().copied().enumerate() {
        let end = shape.end_ms();
        let ranges = [
            ("range_1h", RangeQuery::new(end - HOUR_MS, end, STEP_MS)),
            ("instant", RangeQuery::new(end, end, STEP_MS)),
        ];
        for (qname, q) in QUERIES {
            for (rname, range) in ranges {
                suite = suite.bench(
                    format!(
                        "EngineQuery/query={qname}/range={rname}/shape={}",
                        shape.id()
                    ),
                    move |b| {
                        let engine = blocking_engine();
                        let source = shared_source(idx);
                        b.set_bytes(shape.bytes());
                        b.report_allocs();
                        while b.keep_running() {
                            black_box(
                                engine
                                    .range_query(source.as_ref(), black_box(q), &range)
                                    .unwrap(),
                            );
                        }
                    },
                );
            }
        }
    }
    suite
}

/// The pinned five-minute window must be refit at every step because
/// Prometheus anchors the regression intercept on evaluation time.
fn register_linear_regression<'a>(mut suite: Suite<'a>) -> Suite<'a> {
    let idx = 1usize;
    let shape = SHAPES[idx];
    let end = shape.end_ms();
    let range = RangeQuery::new(end - HOUR_MS, end, STEP_MS);
    let pinned = format!(
        "predict_linear(http_requests_total[5m] @ {}, 3600)",
        end / 1000
    );
    for (name, query) in [
        ("deriv_5m", "deriv(http_requests_total[5m])".to_owned()),
        (
            "predict_linear_5m",
            "predict_linear(http_requests_total[5m], 3600)".to_owned(),
        ),
        ("predict_linear_5m_at_pinned", pinned),
    ] {
        suite = suite.bench(
            format!("EngineLinearRegression/query={name}/shape={}", shape.id()),
            move |b| {
                let engine = blocking_engine();
                let source = shared_source(idx);
                b.set_bytes(shape.bytes());
                b.report_allocs();
                while b.keep_running() {
                    black_box(
                        engine
                            .range_query(source.as_ref(), black_box(query.as_str()), &range)
                            .unwrap(),
                    );
                }
            },
        );
    }
    suite
}

/// The elementwise operator's own cost, over the last hour at 30s.
fn register_elementwise<'a>(mut suite: Suite<'a>) -> Suite<'a> {
    let idx = 2usize;
    let shape = SHAPES[idx];
    let end = shape.end_ms();
    let range = RangeQuery::new(end - HOUR_MS, end, STEP_MS);
    for (qname, q) in [
        ("abs_selector", "abs(http_requests_total)"),
        ("clamp_selector", "clamp(http_requests_total, 0, 1000)"),
        ("abs_rate_5m", "abs(rate(http_requests_total[5m]))"),
    ] {
        suite = suite.bench(
            format!("EngineElementwise/query={qname}/shape={}", shape.id()),
            move |b| {
                let engine = blocking_engine();
                let source = shared_source(idx);
                b.set_bytes(shape.bytes());
                b.report_allocs();
                while b.keep_running() {
                    black_box(
                        engine
                            .range_query(source.as_ref(), black_box(q), &range)
                            .unwrap(),
                    );
                }
            },
        );
    }
    suite
}

/// The two shapes a binary operator takes, over the same source.
fn register_binary<'a>(mut suite: Suite<'a>) -> Suite<'a> {
    let idx = 2usize;
    let shape = SHAPES[idx];
    let end = shape.end_ms();
    let range = RangeQuery::new(end - HOUR_MS, end, STEP_MS);
    for (qname, q) in [
        ("vector_scalar", "http_requests_total * 2"),
        ("vector_scalar_compare", "http_requests_total > 100"),
        ("vector_vector", "http_requests_total + http_requests_total"),
        (
            "group_left",
            "http_requests_total / on(route) group_left() sum by (route) (http_requests_total)",
        ),
        (
            "or",
            "http_requests_total or on(route) sum by (route) (http_requests_total)",
        ),
        (
            "fill",
            "http_requests_total / on(route) group_left() fill(0) sum by (route) (http_requests_total)",
        ),
    ] {
        suite = suite.bench(
            format!("EngineBinary/query={qname}/shape={}", shape.id()),
            move |b| {
                let engine = blocking_engine();
                let source = shared_source(idx);
                b.set_bytes(shape.bytes());
                b.report_allocs();
                while b.keep_running() {
                    black_box(
                        engine
                            .range_query(source.as_ref(), black_box(q), &range)
                            .unwrap(),
                    );
                }
            },
        );
    }

    // A day of a 15s scrape at a 60s step: 1440 steps per series instead of
    // 121, where per-step pairing lanes and per-sample elementwise loops
    // dominate fixed planning costs.
    static DAY_SOURCE: OnceLock<Arc<dyn SeriesSource>> = OnceLock::new();
    let day = Shape {
        series: 1_000,
        samples: 5_760,
    };
    let day_range = RangeQuery::new(0, day.end_ms(), 60_000);
    for (qname, q) in [
        (
            "vector_vector_24h",
            "http_requests_total / http_requests_total",
        ),
        ("vector_scalar_24h", "http_requests_total / 2"),
        ("vector_scalar_compare_24h", "http_requests_total > 100"),
    ] {
        suite = suite.bench(
            format!("EngineBinary/query={qname}/shape={}", day.id()),
            move |b| {
                let engine = blocking_engine();
                let source = DAY_SOURCE.get_or_init(|| Arc::new(synthetic_memory(&day)));
                b.set_bytes(day.bytes());
                b.report_allocs();
                while b.keep_running() {
                    black_box(
                        engine
                            .range_query(source.as_ref(), black_box(q), &day_range)
                            .unwrap(),
                    );
                }
            },
        );
    }
    suite
}

/// Parsing and logical planning alone, including the store's `select`.
fn register_plan<'a>(mut suite: Suite<'a>) -> Suite<'a> {
    let (qname, q) = QUERIES[4];
    for (idx, shape) in SHAPES.iter().copied().enumerate() {
        let end = shape.end_ms();
        let range = RangeQuery::new(end - HOUR_MS, end, STEP_MS);
        suite = suite.bench(
            format!(
                "EnginePlan/query={qname}/range=range_1h/shape={}",
                shape.id()
            ),
            move |b| {
                let engine = async_engine();
                let rt = current_thread_rt();
                let source = shared_source(idx);
                b.report_allocs();
                while b.keep_running() {
                    black_box(
                        rt.block_on(engine.plan_async(source.as_ref(), black_box(q), &range))
                            .unwrap(),
                    );
                }
            },
        );
    }
    suite
}

/// An ID-encoding comparison, not an isolated executor speedup: source
/// selection and final API collection are timed as in `EngineQuery`.
fn register_contiguous<'a>(mut suite: Suite<'a>) -> Suite<'a> {
    static CONTIGUOUS_SOURCES: [OnceLock<MemorySeriesSource>; 2] =
        [OnceLock::new(), OnceLock::new()];
    let shape = SHAPES[2];
    let end = shape.end_ms();
    let range = RangeQuery::new(end - HOUR_MS, end, STEP_MS);
    for (eidx, (encoding, id)) in [
        ("uint64", SeriesIdEncoding::UInt64),
        ("binary16", SeriesIdEncoding::FixedSizeBinary),
    ]
    .into_iter()
    .enumerate()
    {
        for (qname, query) in [QUERIES[0], QUERIES[3], QUERIES[4]] {
            suite = suite.bench(
                format!(
                    "EngineContiguous/id={encoding}/query={qname}/range=range_1h/shape={}",
                    shape.id()
                ),
                move |b| {
                    let engine = blocking_engine();
                    let source = CONTIGUOUS_SOURCES[eidx].get_or_init(|| {
                        synthetic_memory(&shape)
                            .chunked(150_000)
                            .blocks(NonZeroU64::new(900_000).unwrap())
                            .with_ids(id)
                    });
                    b.set_bytes(shape.bytes());
                    b.report_allocs();
                    while b.keep_running() {
                        black_box(
                            engine
                                .range_query(source, black_box(query), &range)
                                .unwrap(),
                        );
                    }
                },
            );
        }
    }
    suite
}

/// Isolate real plan execution, including stream creation and Arrow output
/// construction but not source selection/repacking or API result collection.
fn register_execute_preplanned<'a>(mut suite: Suite<'a>) -> Suite<'a> {
    static PREPLANNED_SOURCES: [OnceLock<MemorySeriesSource>; 2] =
        [OnceLock::new(), OnceLock::new()];
    let shape = SHAPES[2];
    let at = shape.end_ms() + SCRAPE_MS;
    let instant = RangeQuery::new(at, at, STEP_MS);
    let range = RangeQuery::new(at - 30 * STEP_MS, at, STEP_MS);
    for (eidx, (encoding, id)) in [
        ("uint64", SeriesIdEncoding::UInt64),
        ("binary16", SeriesIdEncoding::FixedSizeBinary),
    ]
    .into_iter()
    .enumerate()
    {
        for (qname, query, rname, rq) in [
            ("selector", "http_requests_total", "one_step_tail", instant),
            (
                "timestamp",
                "timestamp(http_requests_total)",
                "one_step_tail",
                instant,
            ),
            (
                "rate_5m",
                "rate(http_requests_total[5m])",
                "31_steps_tail",
                range,
            ),
            (
                "min_5m",
                "min_over_time(http_requests_total[5m])",
                "31_steps_tail",
                range,
            ),
        ] {
            suite = suite.bench(
                format!(
                    "EngineExecutePreplanned/id={encoding}/query={qname}/range={rname}/shape={}",
                    shape.id()
                ),
                move |b| {
                    let engine = async_engine();
                    let rt = current_thread_rt();
                    let ctx = task_ctx();
                    let mut plan_opt = None;
                    let plan = plan_opt.get_or_insert_with(|| {
                        let source = PREPLANNED_SOURCES[eidx]
                            .get_or_init(|| synthetic_memory(&shape).chunked(45_000).with_ids(id));
                        rt.block_on(engine.prepare_async(source, query, &rq))
                            .unwrap()
                            .physical_plan()
                            .clone()
                    });
                    b.report_allocs();
                    while b.keep_running() {
                        drain(rt, black_box(plan), ctx);
                    }
                },
            );
        }
    }
    suite
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
fn register_focused<'a>(mut suite: Suite<'a>) -> Suite<'a> {
    use generated::{ChunkProbe, Generated, Shape as GenShape};

    for count in [1024usize, 16384] {
        for shuffled in [false, true] {
            let name = if shuffled {
                "shuffled_wide"
            } else {
                "ordered_wide"
            };
            suite = suite.bench(
                format!("EngineFocusedCollect/case={name}/series={count}"),
                move |b| {
                    let engine = async_engine();
                    let rt = current_thread_rt();
                    let mut prep_opt = None;
                    let prepared = prep_opt.get_or_insert_with(|| {
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
                        prepared
                    });
                    b.report_allocs();
                    while b.keep_running() {
                        black_box(
                            rt.block_on(engine.execute_async(black_box(prepared)))
                                .unwrap(),
                        );
                    }
                },
            );
        }
    }

    let wide_count = 16384usize;
    suite = suite
        .bench(
            format!("EngineFocusedCollect/case=empty_stale_wide/series={wide_count}"),
            move |b| {
                let engine = async_engine();
                let rt = current_thread_rt();
                let mut prep_opt = None;
                let empty = prep_opt.get_or_insert_with(|| {
                    let source = focused::wide_source(wide_count, true, true);
                    let empty = rt
                        .block_on(engine.prepare_async(
                            source.as_ref(),
                            focused::SELECTOR,
                            &focused::instant(),
                        ))
                        .unwrap();
                    assert_eq!(
                        points(&rt.block_on(engine.execute_async(&empty)).unwrap()),
                        0
                    );
                    empty
                });
                b.report_allocs();
                while b.keep_running() {
                    black_box(rt.block_on(engine.execute_async(black_box(empty))).unwrap());
                }
            },
        )
        .bench(
            format!("EngineFocusedDrain/case=shuffled_wide/series={wide_count}"),
            move |b| {
                let engine = async_engine();
                let rt = current_thread_rt();
                let ctx = task_ctx();
                let mut plan_opt = None;
                let plan = plan_opt.get_or_insert_with(|| {
                    let source = focused::wide_source(wide_count, true, false);
                    rt.block_on(engine.prepare_async(
                        source.as_ref(),
                        focused::SELECTOR,
                        &focused::instant(),
                    ))
                    .unwrap()
                    .physical_plan()
                    .clone()
                });
                b.report_allocs();
                while b.keep_running() {
                    drain(rt, black_box(plan), ctx);
                }
            },
        );

    for (groups, blocks) in [(512usize, 8usize), (4096, 24)] {
        suite = suite.bench(
            format!("EngineFocusedSparse/query=sum/shape={groups}groups_{blocks}blocks"),
            move |b| {
                let engine = async_engine();
                let rt = current_thread_rt();
                let ctx = task_ctx();
                let mut plan_opt = None;
                let plan = plan_opt.get_or_insert_with(|| {
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
                    prepared.physical_plan().clone()
                });
                b.report_allocs();
                while b.keep_running() {
                    drain(rt, black_box(plan), ctx);
                }
            },
        );
    }

    for (name, probe) in [
        ("whole", ChunkProbe::Whole),
        ("second", ChunkProbe::Second),
        ("stale", ChunkProbe::Stale),
    ] {
        suite = suite.bench(
            format!("EngineFocusedStreaming/probe={name}/shape=4x131072"),
            move |b| {
                let engine = async_engine();
                let rt = current_thread_rt();
                let ctx = task_ctx();
                let mut plan_opt = None;
                let plan = plan_opt.get_or_insert_with(|| {
                    let source = Generated::streamed_with_ids(
                        GenShape {
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
                    prepared.physical_plan().clone()
                });
                b.set_bytes((4 * 131072) as u64 * SAMPLE_BYTES);
                b.report_allocs();
                while b.keep_running() {
                    drain(rt, black_box(plan), ctx);
                }
            },
        );
    }

    for count in [1024usize, 8192] {
        suite = suite.bench(
            format!("EngineFocusedChurn/query=rate_32steps/series={count}"),
            move |b| {
                let engine = async_engine();
                let rt = current_thread_rt();
                let ctx = task_ctx();
                let mut plan_opt = None;
                let plan = plan_opt.get_or_insert_with(|| {
                    let source = Generated::resident(GenShape {
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
                    prepared.physical_plan().clone()
                });
                b.report_allocs();
                while b.keep_running() {
                    drain(rt, black_box(plan), ctx);
                }
            },
        );
    }
    suite
}

/// End-to-end `instant_query` coverage across all four `InstantResult` arms
/// that `range_query` does not exercise directly: vector, raw matrix selector,
/// scalar extraction, string literal, `absent` (hit and miss), and root
/// instant sorts.
fn register_instant_query<'a>(mut suite: Suite<'a>) -> Suite<'a> {
    let idx = 1usize;
    let shape = SHAPES[idx];
    let at_ms = shape.end_ms();
    for (kind, query) in [
        ("vector_selector", "http_requests_total"),
        ("matrix_5m", "http_requests_total[5m]"),
        ("scalar_sum", "scalar(sum(http_requests_total))"),
        ("string_literal", r#""promql-rs""#),
        ("absent_hit", r#"absent(nonexistent_metric{job="api"})"#),
        ("absent_miss", r#"absent(http_requests_total{code="200"})"#),
        ("sort_desc", "sort_desc(http_requests_total)"),
        (
            "sort_by_label_desc",
            r#"sort_by_label_desc(http_requests_total, "route", "pod")"#,
        ),
    ] {
        let mut checked = false;
        suite = suite.bench(
            format!("EngineInstantQuery/kind={kind}/shape={}", shape.id()),
            move |b| {
                let engine = blocking_engine();
                let source = shared_source(idx);
                if !checked {
                    match (
                        kind,
                        engine.instant_query(source.as_ref(), query, at_ms).unwrap(),
                    ) {
                        (
                            "vector_selector" | "sort_desc" | "sort_by_label_desc",
                            InstantResult::Vector(batches),
                        ) => assert_eq!(points(&batches), shape.series),
                        ("matrix_5m", InstantResult::Matrix(batches)) => {
                            assert!(points(&batches) > shape.series);
                        }
                        ("scalar_sum", InstantResult::Scalar(v)) => {
                            assert!(v.is_finite() && v > 0.0);
                        }
                        ("string_literal", InstantResult::String(s)) => {
                            assert_eq!(s, "promql-rs");
                        }
                        ("absent_hit", InstantResult::Vector(batches)) => {
                            assert_eq!(points(&batches), 1);
                        }
                        ("absent_miss", InstantResult::Vector(batches)) => {
                            assert_eq!(points(&batches), 0);
                        }
                        (_, other) => panic!("unexpected result for {kind}: {other:?}"),
                    }
                    checked = true;
                }
                b.report_allocs();
                while b.keep_running() {
                    let res: InstantResult = engine
                        .instant_query(source.as_ref(), black_box(query), at_ms)
                        .unwrap();
                    black_box(res);
                }
            },
        );
    }
    suite
}

/// Logical and physical plan explain rendering (`explain_plan` and
/// `explain_physical_plan`) on prebuilt plans so formatting and expression
/// folding cost is isolated from planning.
fn register_explain<'a>(mut suite: Suite<'a>) -> Suite<'a> {
    let idx = 1usize;
    let shape = SHAPES[idx];
    let end = shape.end_ms();
    let range = RangeQuery::new(end - HOUR_MS, end, STEP_MS);
    for (qname, query) in [
        ("rate_sum", "sum by (route) (rate(http_requests_total[5m]))"),
        (
            "group_left",
            "http_requests_total / on(route) group_left() sum by (route) (http_requests_total)",
        ),
    ] {
        let mut plan_opt = None;
        let mut exec_opt = None;
        suite = suite
            .bench(
                format!(
                    "EngineExplain/level=logical/query={qname}/shape={}",
                    shape.id()
                ),
                move |b| {
                    let engine = async_engine();
                    let rt = current_thread_rt();
                    let plan = plan_opt.get_or_insert_with(|| {
                        let source = shared_source(idx);
                        let plan = rt
                            .block_on(engine.plan_async(source.as_ref(), query, &range))
                            .unwrap();
                        assert!(explain_plan(&plan).contains("SeriesEval:"));
                        plan
                    });
                    b.report_allocs();
                    while b.keep_running() {
                        black_box(explain_plan(black_box(plan)));
                    }
                },
            )
            .bench(
                format!(
                    "EngineExplain/level=physical/query={qname}/shape={}",
                    shape.id()
                ),
                move |b| {
                    let engine = async_engine();
                    let rt = current_thread_rt();
                    let exec = exec_opt.get_or_insert_with(|| {
                        let source = shared_source(idx);
                        let exec = rt
                            .block_on(engine.prepare_async(source.as_ref(), query, &range))
                            .unwrap()
                            .physical_plan()
                            .clone();
                        let rendered = explain_physical_plan(exec.as_ref());
                        assert!(
                            rendered.contains("SeriesEvalExec:")
                                && rendered.contains("SeriesSetExec:")
                        );
                        exec
                    });
                    b.report_allocs();
                    while b.keep_running() {
                        black_box(explain_physical_plan(black_box(exec.as_ref())));
                    }
                },
            );
    }
    suite
}

fn main() {
    let mut suite = Suite::from_env("promql_engine/engine").with_alloc(&ALLOC);
    suite = register_query(suite);
    suite = register_instant_query(suite);
    suite = register_linear_regression(suite);
    suite = register_elementwise(suite);
    suite = register_binary(suite);
    suite = register_plan(suite);
    suite = register_explain(suite);
    suite = register_contiguous(suite);
    suite = register_execute_preplanned(suite);
    suite = register_focused(suite);
    suite.run();
}
