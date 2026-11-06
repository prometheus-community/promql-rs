//! Direct `benchstat` counterpart to `BenchmarkSingleQuery`,
//! `BenchmarkRangeQuery`, `BenchmarkInstantQuery`, and
//! `BenchmarkMergeSelectorsOptimizer` in `thanos-io/promql-engine`
//! (`engine/bench_test.go` at `89d6517ed91a74f857fc4493e257ecc2fe0974f6`).
//!
//! Fixture shapes, label sets, sample intervals, values, `pkg` header
//! (`github.com/thanos-io/promql-engine/engine`), and `/new_engine` /
//! `/withOptimizers` sub-benchmark names match `engine/bench_test.go` so
//! `benchstat thanos.txt rust.txt` pairs `thanos-io/promql-engine` and
//! `promql_engine::Engine` directly without renaming. Upstream's `/old_engine`
//! sub-benchmarks (`prometheus/prometheus/promql.Engine`) on the same fixtures
//! can also be compared by rewriting `/old_engine` to `/new_engine`.
//!
//! Two boundaries are exposed for every query:
//! - `BenchmarkSingleQuery`, `BenchmarkRangeQuery/<name>/new_engine`,
//!   `BenchmarkInstantQuery/<name>/new_engine`, and
//!   `BenchmarkMergeSelectorsOptimizer/withOptimizers` time whole-query
//!   `Engine::range_query` or `Engine::instant_query` (parse +
//!   `MemorySeriesSource::select` + DataFusion logical/physical planning +
//!   execution + result finalization), matching Go's `NewRangeQuery` /
//!   `NewInstantQuery` + `Exec` loop.
//! - `BenchmarkPreparedSingleQuery`, `BenchmarkPreparedRange/<name>/new_engine`,
//!   `BenchmarkPreparedInstant/<name>/new_engine`, and
//!   `BenchmarkPreparedMergeSelectors/withOptimizers` time
//!   `Engine::execute_async` on pre-prepared queries to isolate vectorized
//!   DataFusion execution and PromQL finalization from `MemorySeriesSource`'s
//!   per-query batch copy and planning overhead.
//!
//! `BenchmarkChunkDecoding` (which benchmarks raw TSDB `chunkenc.Iterator`
//! seeks without PromQL), `BenchmarkNativeHistograms`, and individual
//! expressions that exercise features listed in
//! `promql-conformance/testdata/prometheus/UNSUPPORTED.md` (`topk`, `bottomk`,
//! `limitk`, `limit_ratio`, `quantile`, `count_values`, `histogram_quantile`,
//! `double_exponential_smoothing`, and subqueries) are omitted until
//! implemented.
//!
//! Recommended CLI invocation:
//! ```sh
//! cargo bench -p promql-engine --bench thanos_compare -- -bench '^BenchmarkRangeQuery/' -benchtime 200ms -count 6 -cpu 2 -benchmem | tee ${bench}.txt
//! ```

use std::hint::black_box;
use std::sync::OnceLock;

use benchfmt::Suite;
use promql_engine::{Engine, EngineOptions, MemorySeriesSource, PreparedQuery, RangeQuery, Series};
use promql_parser::ParserOptions;

#[path = "support/process_alloc.rs"]
mod process_alloc;

use process_alloc::ProcessAlloc;

#[global_allocator]
static ALLOC: ProcessAlloc = ProcessAlloc::new();

const LOAD_STEP_MS: i64 = 30_000;
const SIX_HOUR_STEPS: usize = 6 * 60 * 2; // 720 intervals -> 721 samples over [0, 6h]
const TWO_HOURS_MS: i64 = 2 * 3_600_000;
const SIX_HOURS_MS: i64 = 6 * 3_600_000;
const FIVE_MINUTES_MS: i64 = 5 * 60_000;
const INSTANT_SHORT_AT_MS: i64 = 50_000;

/// Build the exact series dataset of `setupStorage` / `synthesizeLoad` in
/// `engine/bench_test.go:845-894`.
///
/// In Prometheus's `promqltest` grammar (`generated_parser.y`), both `a+bxN` and
/// `axN` emit `N + 1` samples at `0, 30s, ..., N*30s`:
/// - `http_requests_total{container="c0".."c{C-1}", pod="p0".."p{P-1}"}`: `i + j*s`
/// - `http_responses_total{pod="p0".."p{P-1}"}`: `i`
/// - `http_response_seconds_bucket{le="0".."9", pod="p0".."p{P-1}"}`: `i + j*s`
/// - `http_response_seconds_bucket{le="+Inf", pod="p0".."p{P-1}"}`: `i + i*s`
fn build_synthesized_load_source(
    num_pods: usize,
    num_containers: usize,
    num_steps: usize,
) -> MemorySeriesSource {
    let sample_len = num_steps + 1;
    let timestamps: Vec<i64> = (0..sample_len).map(|s| s as i64 * LOAD_STEP_MS).collect();
    let containers: Vec<String> = (0..num_containers).map(|j| format!("c{j}")).collect();
    let buckets: Vec<String> = (0..10).map(|j| j.to_string()).collect();

    let series_per_pod = num_containers + 1 + 11;
    let mut all_series = Vec::with_capacity(num_pods * series_per_pod);

    let make_ramp = |start: f64, inc: f64| -> Vec<f64> {
        let mut vals = Vec::with_capacity(sample_len);
        let mut cur = start;
        for _ in 0..sample_len {
            vals.push(cur);
            cur += inc;
        }
        vals
    };

    for i in 0..num_pods {
        let pod = format!("p{i}");
        let base = i as f64;
        for (j, container) in containers.iter().enumerate() {
            let labels = [
                ("__name__", "http_requests_total"),
                ("container", container.as_str()),
                ("pod", pod.as_str()),
            ];
            all_series
                .push(Series::new(&labels, timestamps.clone(), make_ramp(base, j as f64)).unwrap());
        }
        let resp_labels = [("__name__", "http_responses_total"), ("pod", pod.as_str())];
        all_series
            .push(Series::new(&resp_labels, timestamps.clone(), vec![base; sample_len]).unwrap());
    }

    for i in 0..num_pods {
        let pod = format!("p{i}");
        let base = i as f64;
        for (j, le) in buckets.iter().enumerate() {
            let labels = [
                ("__name__", "http_response_seconds_bucket"),
                ("le", le.as_str()),
                ("pod", pod.as_str()),
            ];
            all_series
                .push(Series::new(&labels, timestamps.clone(), make_ramp(base, j as f64)).unwrap());
        }
        let inf_labels = [
            ("__name__", "http_response_seconds_bucket"),
            ("le", "+Inf"),
            ("pod", pod.as_str()),
        ];
        all_series
            .push(Series::new(&inf_labels, timestamps.clone(), make_ramp(base, base)).unwrap());
    }

    MemorySeriesSource::try_new(all_series).unwrap()
}

/// Build the exact series dataset of `createRequestsMetricBlock(10000, 9900)` in
/// `engine/bench_test.go:850-874`.
///
/// NOTE: Upstream passes `t` in `[0, 720)` stepped by `30` directly to
/// `appender.Append` (millisecond timestamps `0, 30, ..., 690`), with `code="200"`
/// for `i <= 9900` and `code="500"` for `i > 9900`.
fn build_merge_selectors_source() -> MemorySeriesSource {
    const NUM_REQUESTS: usize = 10_000;
    const NUM_SUCCESS: usize = 9_900;

    let timestamps: Vec<i64> = (0..720).step_by(30).collect();
    let ones = vec![1.0; timestamps.len()];
    let mut all_series = Vec::with_capacity(NUM_REQUESTS);

    for i in 0..NUM_REQUESTS {
        let code = if NUM_SUCCESS < i { "500" } else { "200" };
        let pod = i.to_string();
        let labels = [
            ("__name__", "http_requests_total"),
            ("code", code),
            ("pod", pod.as_str()),
        ];
        all_series.push(Series::new(&labels, timestamps.clone(), ones.clone()).unwrap());
    }

    MemorySeriesSource::try_new(all_series).unwrap()
}

fn six_hour_source() -> &'static MemorySeriesSource {
    static SRC: OnceLock<MemorySeriesSource> = OnceLock::new();
    SRC.get_or_init(|| build_synthesized_load_source(1_000, 3, SIX_HOUR_STEPS))
}

fn single_query_source() -> &'static MemorySeriesSource {
    static SRC: OnceLock<MemorySeriesSource> = OnceLock::new();
    SRC.get_or_init(|| build_synthesized_load_source(5_000, 3, SIX_HOUR_STEPS))
}

fn merge_selectors_source() -> &'static MemorySeriesSource {
    static SRC: OnceLock<MemorySeriesSource> = OnceLock::new();
    SRC.get_or_init(build_merge_selectors_source)
}

fn blocking_engine() -> &'static Engine {
    static ENGINE: OnceLock<Engine> = OnceLock::new();
    ENGINE.get_or_init(|| {
        Engine::blocking_with_options(EngineOptions {
            parser: ParserOptions {
                enable_extended_range_selectors: true,
                enable_experimental_functions: true,
                ..Default::default()
            },
        })
        .unwrap()
    })
}

fn async_engine() -> &'static Engine {
    static ENGINE: OnceLock<Engine> = OnceLock::new();
    ENGINE.get_or_init(|| {
        Engine::with_options(EngineOptions {
            parser: ParserOptions {
                enable_extended_range_selectors: true,
                enable_experimental_functions: true,
                ..Default::default()
            },
        })
    })
}

fn multi_thread_rt() -> &'static tokio::runtime::Runtime {
    static RT: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap()
    })
}

/// Supported subset of `BenchmarkRangeQuery` (`engine/bench_test.go:132-423`) in
/// exact upstream order, `(name, query, step_ms)`.
const RANGE_CASES: &[(&str, &str, i64)] = &[
    ("vector selector", "http_requests_total", LOAD_STEP_MS),
    ("sum", "sum(http_requests_total)", LOAD_STEP_MS),
    (
        "sum by pod",
        "sum by (pod) (http_requests_total)",
        LOAD_STEP_MS,
    ),
    ("rate", "rate(http_requests_total[1m])", LOAD_STEP_MS),
    (
        "rate with longer window",
        "rate(http_requests_total[10m])",
        FIVE_MINUTES_MS,
    ),
    (
        "sum rate",
        "sum(rate(http_requests_total[1m]))",
        LOAD_STEP_MS,
    ),
    (
        "sum by rate",
        "sum by (pod) (rate(http_requests_total[1m]))",
        LOAD_STEP_MS,
    ),
    (
        "binary operation with one to one",
        r#"http_requests_total{container="c1"} / ignoring (container) http_responses_total"#,
        LOAD_STEP_MS,
    ),
    (
        "binary operation with many to one",
        "http_requests_total / on (pod) group_left () http_responses_total",
        LOAD_STEP_MS,
    ),
    (
        "binary operation with vector and scalar",
        "http_requests_total * 10",
        LOAD_STEP_MS,
    ),
    ("unary negation", "-http_requests_total", LOAD_STEP_MS),
    (
        "vector and scalar comparison",
        "http_requests_total > 10",
        LOAD_STEP_MS,
    ),
    (
        "positive offset vector",
        "http_requests_total offset 5m",
        LOAD_STEP_MS,
    ),
    // Trailing space matches `name: "at modifier "` in `engine/bench_test.go:252`.
    (
        "at modifier ",
        "http_requests_total @ 600.000",
        LOAD_STEP_MS,
    ),
    (
        "at modifier with positive offset vector",
        "http_requests_total @ 600.000 offset 5m",
        LOAD_STEP_MS,
    ),
    ("clamp", "clamp(http_requests_total, 5, 10)", LOAD_STEP_MS),
    (
        "clamp_min",
        "clamp_min(http_requests_total, 10)",
        LOAD_STEP_MS,
    ),
    (
        "complex func query",
        "clamp(1 - http_requests_total, 10 - 5, 10)",
        LOAD_STEP_MS,
    ),
    (
        "func within func query",
        "clamp(irate(http_requests_total[30s]), 10 - 5, 10)",
        LOAD_STEP_MS,
    ),
    (
        "aggr within func query",
        "clamp(rate(http_requests_total[30s]), 10 - 5, 10)",
        LOAD_STEP_MS,
    ),
    ("sort", "sort(http_requests_total)", LOAD_STEP_MS),
    ("sort_desc", "sort_desc(http_requests_total)", LOAD_STEP_MS),
    (
        "absent and exists",
        "absent(http_requests_total)",
        LOAD_STEP_MS,
    ),
    (
        "absent and doesnt exist",
        "absent(nonexistent)",
        LOAD_STEP_MS,
    ),
    (
        "count_over_time_5m",
        "count_over_time(http_requests_total[5m])",
        LOAD_STEP_MS,
    ),
    (
        "count_over_time_1h",
        "count_over_time(http_requests_total[1h])",
        LOAD_STEP_MS,
    ),
    (
        "count_over_time_6h",
        "count_over_time(http_requests_total[6h])",
        LOAD_STEP_MS,
    ),
    (
        "sum_over_time_5m",
        "sum_over_time(http_requests_total[5m])",
        LOAD_STEP_MS,
    ),
    (
        "sum_over_time_1h",
        "sum_over_time(http_requests_total[1h])",
        LOAD_STEP_MS,
    ),
    (
        "sum_over_time_6h",
        "sum_over_time(http_requests_total[6h])",
        LOAD_STEP_MS,
    ),
    (
        "avg_over_time_5m",
        "avg_over_time(http_requests_total[5m])",
        LOAD_STEP_MS,
    ),
    (
        "avg_over_time_1h",
        "avg_over_time(http_requests_total[1h])",
        LOAD_STEP_MS,
    ),
    (
        "avg_over_time_6h",
        "avg_over_time(http_requests_total[6h])",
        LOAD_STEP_MS,
    ),
    (
        "min_over_time_5m",
        "min_over_time(http_requests_total[5m])",
        LOAD_STEP_MS,
    ),
    (
        "min_over_time_1h",
        "min_over_time(http_requests_total[1h])",
        LOAD_STEP_MS,
    ),
    (
        "min_over_time_6h",
        "min_over_time(http_requests_total[6h])",
        LOAD_STEP_MS,
    ),
    (
        "max_over_time_5m",
        "max_over_time(http_requests_total[5m])",
        LOAD_STEP_MS,
    ),
    (
        "max_over_time_1h",
        "max_over_time(http_requests_total[1h])",
        LOAD_STEP_MS,
    ),
    (
        "max_over_time_6h",
        "max_over_time(http_requests_total[6h])",
        LOAD_STEP_MS,
    ),
    (
        "stddev_over_time_5m",
        "stddev_over_time(http_requests_total[5m])",
        LOAD_STEP_MS,
    ),
    (
        "stddev_over_time_1h",
        "stddev_over_time(http_requests_total[1h])",
        LOAD_STEP_MS,
    ),
    (
        "stddev_over_time_6h",
        "stddev_over_time(http_requests_total[6h])",
        LOAD_STEP_MS,
    ),
    (
        "stdvar_over_time",
        "stdvar_over_time(http_requests_total[5m])",
        LOAD_STEP_MS,
    ),
    (
        "last_over_time",
        "last_over_time(http_requests_total[5m])",
        LOAD_STEP_MS,
    ),
    (
        "present_over_time",
        "present_over_time(http_requests_total[5m])",
        LOAD_STEP_MS,
    ),
];

/// Supported subset of `BenchmarkInstantQuery` (`engine/bench_test.go:684-785`)
/// in exact upstream registration order (`longRangeCases` at `t = 6h`, followed
/// by `cases` at `t = 50s`).
const INSTANT_CASES: &[(&str, &str, i64)] = &[
    (
        "count_over_time 6h",
        "count_over_time(http_requests_total[6h])",
        SIX_HOURS_MS,
    ),
    (
        "sum_over_time 6h",
        "sum_over_time(http_requests_total[6h])",
        SIX_HOURS_MS,
    ),
    (
        "avg_over_time 6h",
        "avg_over_time(http_requests_total[6h])",
        SIX_HOURS_MS,
    ),
    (
        "min_over_time 6h",
        "min_over_time(http_requests_total[6h])",
        SIX_HOURS_MS,
    ),
    (
        "max_over_time 6h",
        "max_over_time(http_requests_total[6h])",
        SIX_HOURS_MS,
    ),
    (
        "stddev_over_time 6h",
        "stddev_over_time(http_requests_total[6h])",
        SIX_HOURS_MS,
    ),
    (
        "stdvar_over_time 6h",
        "stdvar_over_time(http_requests_total[6h])",
        SIX_HOURS_MS,
    ),
    (
        "present_over_time 6h",
        "present_over_time(http_requests_total[6h])",
        SIX_HOURS_MS,
    ),
    (
        "last_over_time 6h",
        "last_over_time(http_requests_total[6h])",
        SIX_HOURS_MS,
    ),
    (
        "vector selector",
        "http_requests_total",
        INSTANT_SHORT_AT_MS,
    ),
    ("count", "count(http_requests_total)", INSTANT_SHORT_AT_MS),
    ("round", "round(http_requests_total)", INSTANT_SHORT_AT_MS),
    (
        "round with argument",
        "round(http_requests_total, 0.5)",
        INSTANT_SHORT_AT_MS,
    ),
    ("avg", "avg(http_requests_total)", INSTANT_SHORT_AT_MS),
    ("sum", "sum(http_requests_total)", INSTANT_SHORT_AT_MS),
    (
        "sum by pod",
        "sum by (pod) (http_requests_total)",
        INSTANT_SHORT_AT_MS,
    ),
    ("rate", "rate(http_requests_total[1m])", INSTANT_SHORT_AT_MS),
    (
        "rate with long window",
        "rate(http_requests_total[1h])",
        INSTANT_SHORT_AT_MS,
    ),
    (
        "sum rate",
        "sum(rate(http_requests_total[1m]))",
        INSTANT_SHORT_AT_MS,
    ),
    (
        "sum by rate",
        "sum by (pod) (rate(http_requests_total[1m]))",
        INSTANT_SHORT_AT_MS,
    ),
    (
        "binary operation with many to one",
        "http_requests_total / on (pod) group_left () http_responses_total",
        INSTANT_SHORT_AT_MS,
    ),
    (
        "unary negation",
        "-http_requests_total",
        INSTANT_SHORT_AT_MS,
    ),
    (
        "vector and scalar comparison",
        "http_requests_total > 10",
        INSTANT_SHORT_AT_MS,
    ),
    ("sort", "sort(http_requests_total)", INSTANT_SHORT_AT_MS),
    (
        "sort_desc",
        "sort_desc(http_requests_total)",
        INSTANT_SHORT_AT_MS,
    ),
];

fn register_single_query<'a>(suite: Suite<'a>) -> Suite<'a> {
    const QUERY: &str = "sum(rate(http_requests_total[2m]))";
    let rq = RangeQuery::new(0, SIX_HOURS_MS, LOAD_STEP_MS);
    let mut warmed = false;

    suite
        .bench("SingleQuery", move |b| {
            let engine = blocking_engine();
            let source = single_query_source();
            if !warmed {
                black_box(engine.range_query(source, QUERY, &rq).unwrap());
                warmed = true;
            }
            b.report_allocs();
            while b.keep_running() {
                black_box(engine.range_query(source, black_box(QUERY), &rq).unwrap());
            }
        })
        .bench("PreparedSingleQuery", move |b| {
            let engine = async_engine();
            let rt = multi_thread_rt();
            let mut prep_opt: Option<PreparedQuery> = None;
            let prepared = prep_opt.get_or_insert_with(|| {
                let source = single_query_source();
                let p = rt
                    .block_on(engine.prepare_async(source, QUERY, &rq))
                    .unwrap();
                black_box(rt.block_on(engine.execute_async(&p)).unwrap());
                p
            });
            b.report_allocs();
            while b.keep_running() {
                black_box(
                    rt.block_on(engine.execute_async(black_box(prepared)))
                        .unwrap(),
                );
            }
        })
}

fn register_range_queries<'a>(mut suite: Suite<'a>) -> Suite<'a> {
    for &(name, query, step_ms) in RANGE_CASES {
        let rq = RangeQuery::new(0, TWO_HOURS_MS, step_ms);
        suite = suite
            .bench(format!("RangeQuery/{name}/new_engine"), move |b| {
                let engine = blocking_engine();
                let source = six_hour_source();
                b.report_allocs();
                while b.keep_running() {
                    black_box(engine.range_query(source, black_box(query), &rq).unwrap());
                }
            })
            .bench(format!("PreparedRange/{name}/new_engine"), move |b| {
                let engine = async_engine();
                let rt = multi_thread_rt();
                let mut prep_opt: Option<PreparedQuery> = None;
                let prepared = prep_opt.get_or_insert_with(|| {
                    let source = six_hour_source();
                    let p = rt
                        .block_on(engine.prepare_async(source, query, &rq))
                        .unwrap();
                    black_box(rt.block_on(engine.execute_async(&p)).unwrap());
                    p
                });
                b.report_allocs();
                while b.keep_running() {
                    black_box(
                        rt.block_on(engine.execute_async(black_box(prepared)))
                            .unwrap(),
                    );
                }
            });
    }
    suite
}

fn register_instant_queries<'a>(mut suite: Suite<'a>) -> Suite<'a> {
    for &(name, query, at_ms) in INSTANT_CASES {
        // Every expression in `INSTANT_CASES` evaluates to an instant vector;
        // `plan_instant` lowers it onto the one-step grid `RangeQuery::new(at_ms, at_ms, 1)`
        // while preserving explicit root `sort` / `sort_desc` ordering.
        let rq = RangeQuery::new(at_ms, at_ms, 1);
        suite = suite
            .bench(format!("InstantQuery/{name}/new_engine"), move |b| {
                let engine = blocking_engine();
                let source = six_hour_source();
                b.report_allocs();
                while b.keep_running() {
                    black_box(
                        engine
                            .instant_query(source, black_box(query), at_ms)
                            .unwrap(),
                    );
                }
            })
            .bench(format!("PreparedInstant/{name}/new_engine"), move |b| {
                let engine = async_engine();
                let rt = multi_thread_rt();
                let mut prep_opt: Option<PreparedQuery> = None;
                let prepared = prep_opt.get_or_insert_with(|| {
                    let source = six_hour_source();
                    let p = rt
                        .block_on(engine.prepare_async(source, query, &rq))
                        .unwrap();
                    black_box(rt.block_on(engine.execute_async(&p)).unwrap());
                    p
                });
                b.report_allocs();
                while b.keep_running() {
                    black_box(
                        rt.block_on(engine.execute_async(black_box(prepared)))
                            .unwrap(),
                    );
                }
            });
    }
    suite
}

fn register_merge_selectors<'a>(suite: Suite<'a>) -> Suite<'a> {
    const QUERY: &str = r#"sum(http_requests_total{code="200"}) / sum(http_requests_total)"#;
    let rq = RangeQuery::new(0, SIX_HOURS_MS, LOAD_STEP_MS);

    suite
        .bench("MergeSelectorsOptimizer/withOptimizers", move |b| {
            let engine = blocking_engine();
            let source = merge_selectors_source();
            b.report_allocs();
            while b.keep_running() {
                black_box(engine.range_query(source, black_box(QUERY), &rq).unwrap());
            }
        })
        .bench("PreparedMergeSelectors/withOptimizers", move |b| {
            let engine = async_engine();
            let rt = multi_thread_rt();
            let mut prep_opt: Option<PreparedQuery> = None;
            let prepared = prep_opt.get_or_insert_with(|| {
                let source = merge_selectors_source();
                let p = rt
                    .block_on(engine.prepare_async(source, QUERY, &rq))
                    .unwrap();
                black_box(rt.block_on(engine.execute_async(&p)).unwrap());
                p
            });
            b.report_allocs();
            while b.keep_running() {
                black_box(
                    rt.block_on(engine.execute_async(black_box(prepared)))
                        .unwrap(),
                );
            }
        })
}

fn main() {
    let mut suite = Suite::from_env("github.com/thanos-io/promql-engine/engine").with_alloc(&ALLOC);
    suite = register_single_query(suite);
    suite = register_range_queries(suite);
    suite = register_instant_queries(suite);
    suite = register_merge_selectors(suite);
    suite.run();
}
