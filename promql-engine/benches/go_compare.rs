//! Direct `benchstat` counterpart to `BenchmarkRangeQuery`,
//! `BenchmarkJoinQuery`, and `BenchmarkInfoFunction`'s `group_left` cases in
//! upstream Prometheus (`promql/bench_test.go` at `83962c35a4ab0c9988bc469aa8165014fc065d34`).
//!
//! Fixture shapes, label sets, sample intervals, values, and `benchfmt` names
//! match `promql/bench_test.go` so `benchstat go.txt rust.txt` compares Go's
//! `promql.Engine` (backed by in-memory `teststorage.TestStorage` with head
//! compaction) against `promql_engine::Engine` (backed by `MemorySeriesSource`).
//!
//! Two boundaries are exposed for `RangeQuery`, `HighCardQuery`, and `JoinQuery`:
//! - `BenchmarkRangeQuery/...`, `BenchmarkHighCardQuery/...`, and
//!   `BenchmarkJoinQuery/...` time whole-query `Engine::range_query` (parse +
//!   `MemorySeriesSource::select` + DataFusion logical/physical planning +
//!   execution + result finalization), matching Go's `NewRangeQuery` + `Exec`
//!   loop.
//! - `BenchmarkPreparedRange/...`, `BenchmarkPreparedHighCard/...`, and
//!   `BenchmarkPreparedJoin/...` time `Engine::execute_async` on pre-prepared
//!   queries to isolate vectorized DataFusion execution and PromQL finalization
//!   from `MemorySeriesSource`'s per-query batch copy and planning overhead.
//! - `BenchmarkInfoFunction/...` times `Engine::execute_async` on pre-prepared
//!   queries, matching Go's `BenchmarkInfoFunction` which wraps `NewRangeQuery`
//!   in `b.StopTimer()` / `b.StartTimer()` and times `qry.Exec` alone.
//!
//! Upstream `rangeQueryCases()` expressions that exercise features listed in
//! `promql-conformance/testdata/prometheus/UNSUPPORTED.md` (`smoothed`,
//! `double_exponential_smoothing`, `label_replace`, `label_join`, `count_values`,
//! `topk`, `limitk`, `limit_ratio`, and `histogram_quantile`) are omitted until
//! implemented.
//!
//! Recommended CLI invocation:
//! ```sh
//! cargo bench -p promql-engine --bench go_compare -- -bench '^BenchmarkJoinQuery/' -benchtime 1s -count 6 -cpu 2 -benchmem | tee ${bench}.txt
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

const RANGE_INTERVAL_MS: i64 = 10_000;
const RANGE_NUM_INTERVALS: usize = 8_640 + 10_000;

const JOIN_INTERVAL_MS: i64 = 10_000;
const JOIN_STEPS: usize = 5_000;
const JOIN_NUM_INTERVALS: usize = 8_640 + JOIN_STEPS;
const JOIN_NUM_INSTANCES: usize = 1_000;

const INFO_SERIES_NUM: usize = 100;
const INFO_INTERVAL_MS: i64 = 2_000;
const INFO_NUM_INTERVALS: usize = 3_600;

const HIGH_CARD_INTERVAL_MS: i64 = 10_000;
const HIGH_CARD_NUM_INTERVALS: usize = 65;

/// Build the exact series dataset of `setupRangeQueryTestData` in
/// `promql/bench_test.go:41-100`:
/// - `a_one`, `b_one`, `h_one` (`le="0".."9"`, `"+Inf"`) -> 13 series
/// - `a_ten`, `b_ten`, `h_ten` (`l="0".."9"`) -> 130 series
/// - `a_hundred`, `b_hundred`, `h_hundred` (`l="0".."99"`) -> 1,300 series
/// - `sparse` (`l = strconv.Itoa(s / (numIntervals / 50))`) -> 51 disjoint series
fn build_range_query_source() -> MemorySeriesSource {
    let mut dense_specs: Vec<Vec<(String, String)>> = Vec::with_capacity(1_443);

    dense_specs.push(vec![("__name__".into(), "a_one".into())]);
    dense_specs.push(vec![("__name__".into(), "b_one".into())]);
    for j in 0..10 {
        dense_specs.push(vec![
            ("__name__".into(), "h_one".into()),
            ("le".into(), j.to_string()),
        ]);
    }
    dense_specs.push(vec![
        ("__name__".into(), "h_one".into()),
        ("le".into(), "+Inf".into()),
    ]);

    for (suffix, count) in [("ten", 10usize), ("hundred", 100usize)] {
        for i in 0..count {
            let l_val = i.to_string();
            dense_specs.push(vec![
                ("__name__".into(), format!("a_{suffix}")),
                ("l".into(), l_val.clone()),
            ]);
            dense_specs.push(vec![
                ("__name__".into(), format!("b_{suffix}")),
                ("l".into(), l_val.clone()),
            ]);
            for j in 0..10 {
                dense_specs.push(vec![
                    ("__name__".into(), format!("h_{suffix}")),
                    ("l".into(), l_val.clone()),
                    ("le".into(), j.to_string()),
                ]);
            }
            dense_specs.push(vec![
                ("__name__".into(), format!("h_{suffix}")),
                ("l".into(), l_val),
                ("le".into(), "+Inf".into()),
            ]);
        }
    }

    let metrics_len = dense_specs.len();
    assert_eq!(metrics_len, 1_443);
    let denom = metrics_len as f64;
    let timestamps: Vec<i64> = (0..RANGE_NUM_INTERVALS)
        .map(|s| s as i64 * RANGE_INTERVAL_MS)
        .collect();

    let mut all_series = Vec::with_capacity(metrics_len + 51);
    for (i, spec) in dense_specs.iter().enumerate() {
        let offset = (i as f64) / denom;
        let values: Vec<f64> = (0..RANGE_NUM_INTERVALS)
            .map(|s| s as f64 + offset)
            .collect();
        let pairs: Vec<(&str, &str)> = spec.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
        all_series.push(Series::new(&pairs, timestamps.clone(), values).unwrap());
    }

    let points_per_sparse = RANGE_NUM_INTERVALS / 50;
    let mut sparse_start = 0usize;
    while sparse_start < RANGE_NUM_INTERVALS {
        let bucket = sparse_start / points_per_sparse;
        let sparse_end = ((bucket + 1) * points_per_sparse).min(RANGE_NUM_INTERVALS);
        let l_val = bucket.to_string();
        let ts: Vec<i64> = (sparse_start..sparse_end)
            .map(|s| s as i64 * RANGE_INTERVAL_MS)
            .collect();
        let vs: Vec<f64> = (sparse_start..sparse_end)
            .map(|s| (s as f64) / denom)
            .collect();
        all_series
            .push(Series::new(&[("__name__", "sparse"), ("l", l_val.as_str())], ts, vs).unwrap());
        sparse_start = sparse_end;
    }

    MemorySeriesSource::try_new(all_series).unwrap()
}

/// Deterministic `splitmix64` PRNG for generating reproducible RFC 4122 v4 UUID
/// strings matching `setupJoinQueryTestData` (`0.*` selects ~1/16 of instances).
struct SplitMix64(u64);

impl SplitMix64 {
    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    fn next_uuid_v4(&mut self) -> String {
        let hi = self.next_u64();
        let lo = self.next_u64();
        let mut bytes = [0u8; 16];
        bytes[..8].copy_from_slice(&hi.to_le_bytes());
        bytes[8..].copy_from_slice(&lo.to_le_bytes());
        bytes[6] = (bytes[6] & 0x0f) | 0x40;
        bytes[8] = (bytes[8] & 0x3f) | 0x80;
        format!(
            "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
            bytes[0],
            bytes[1],
            bytes[2],
            bytes[3],
            bytes[4],
            bytes[5],
            bytes[6],
            bytes[7],
            bytes[8],
            bytes[9],
            bytes[10],
            bytes[11],
            bytes[12],
            bytes[13],
            bytes[14],
            bytes[15],
        )
    }
}

/// Build the exact series dataset of `setupJoinQueryTestData` in
/// `promql/bench_test.go:104-151`.
fn build_join_query_source() -> MemorySeriesSource {
    let mut rng = SplitMix64(0);
    let total_metrics = JOIN_NUM_INSTANCES * 2;
    let denom = total_metrics as f64;
    let timestamps: Vec<i64> = (0..JOIN_NUM_INTERVALS)
        .map(|s| s as i64 * JOIN_INTERVAL_MS)
        .collect();

    let mut zero_prefix = 0usize;
    let mut all_series = Vec::with_capacity(total_metrics);
    for inst_idx in 0..JOIN_NUM_INSTANCES {
        let instance = rng.next_uuid_v4();
        if instance.starts_with('0') {
            zero_prefix += 1;
        }
        for (pair_offset, metric_name) in [
            (0usize, "rpc_request_success_total"),
            (1usize, "rpc_request_error_total"),
        ] {
            let metric_idx = inst_idx * 2 + pair_offset;
            let val_offset = (metric_idx as f64) / denom;
            let values: Vec<f64> = (0..JOIN_NUM_INTERVALS)
                .map(|s| s as f64 + val_offset)
                .collect();
            let labels = [
                ("__name__", metric_name),
                ("cluster", "test-kubernetes-cluster"),
                ("domain", "test-domain"),
                ("environment", "staging"),
                ("instance", instance.as_str()),
                ("job", "worker"),
                ("namespace", "test-kubernetes-namespace"),
                ("rpc_method", "fetch-my-data-from-this-service"),
            ];
            all_series.push(Series::new(&labels, timestamps.clone(), values).unwrap());
        }
    }
    // Sanity-check that `instance=~"0.*"` matches ~1/16 of the 1,000 UUIDs.
    assert!(
        (45..=80).contains(&zero_prefix),
        "expected ~62/1000 UUIDs starting with '0', got {zero_prefix}"
    );
    MemorySeriesSource::try_new(all_series).unwrap()
}

/// Build the exact series dataset of `generateInfoFunctionTestSeries` in
/// `promql/bench_test.go:667-720`.
fn build_info_query_source() -> MemorySeriesSource {
    let timestamps: Vec<i64> = (0..INFO_NUM_INTERVALS)
        .map(|i| i as i64 * INFO_INTERVAL_MS)
        .collect();
    let ones = vec![1.0; INFO_NUM_INTERVALS];
    let ramp: Vec<f64> = (0..INFO_NUM_INTERVALS).map(|i| i as f64).collect();

    let mut all_series = Vec::with_capacity(INFO_SERIES_NUM + 3);
    for i in 0..INFO_SERIES_NUM {
        let cluster = if i >= INFO_SERIES_NUM / 2 {
            "eu-south"
        } else {
            "us-east"
        };
        let instance = format!("instance{i}");
        let job = format!("job{i}");
        let labels = [
            ("__name__", "target_info"),
            ("instance", instance.as_str()),
            ("job", job.as_str()),
            ("k8s_cluster_name", cluster),
        ];
        all_series.push(Series::new(&labels, timestamps.clone(), ones.clone()).unwrap());
    }
    for code in ["200", "400", "500"] {
        let labels = [
            ("__name__", "http_server_request_duration_seconds_count"),
            ("http_status_code", code),
            ("instance", "instance0"),
            ("job", "job0"),
        ];
        all_series.push(Series::new(&labels, timestamps.clone(), ramp.clone()).unwrap());
    }
    MemorySeriesSource::try_new(all_series).unwrap()
}

fn range_source() -> &'static MemorySeriesSource {
    static SRC: OnceLock<MemorySeriesSource> = OnceLock::new();
    SRC.get_or_init(build_range_query_source)
}

fn join_source() -> &'static MemorySeriesSource {
    static SRC: OnceLock<MemorySeriesSource> = OnceLock::new();
    SRC.get_or_init(build_join_query_source)
}

fn info_source() -> &'static MemorySeriesSource {
    static SRC: OnceLock<MemorySeriesSource> = OnceLock::new();
    SRC.get_or_init(build_info_query_source)
}

/// Build high-cardinality series scaling across `1k`, `10k`, and `100k` series
/// per metric (`a_1k`/`b_1k`, `a_10k`/`b_10k`, `a_100k`/`b_100k`) with `l="0"..`
/// and `k="0".."99"` (100 aggregation groups) over 65 ten-second intervals so
/// `[5m]` rate and 30-step range queries fit comfortably in memory.
fn build_high_card_query_source() -> MemorySeriesSource {
    let tiers = [
        ("1k", 1_000usize),
        ("10k", 10_000usize),
        ("100k", 100_000usize),
    ];
    let total_metrics: usize = tiers.iter().map(|&(_, n)| n * 2).sum();
    assert_eq!(total_metrics, 222_000);
    let denom = total_metrics as f64;
    let timestamps: Vec<i64> = (0..HIGH_CARD_NUM_INTERVALS)
        .map(|s| s as i64 * HIGH_CARD_INTERVAL_MS)
        .collect();

    let mut all_series = Vec::with_capacity(total_metrics);
    let mut metric_idx = 0usize;
    for (suffix, count) in tiers {
        let a_name = format!("a_{suffix}");
        let b_name = format!("b_{suffix}");
        for i in 0..count {
            let l_val = i.to_string();
            let k_val = (i % 100).to_string();
            for metric_name in [a_name.as_str(), b_name.as_str()] {
                let offset = (metric_idx as f64) / denom;
                metric_idx += 1;
                let values: Vec<f64> = (0..HIGH_CARD_NUM_INTERVALS)
                    .map(|s| s as f64 + offset)
                    .collect();
                let labels = [
                    ("__name__", metric_name),
                    ("k", k_val.as_str()),
                    ("l", l_val.as_str()),
                ];
                all_series.push(Series::new(&labels, timestamps.clone(), values).unwrap());
            }
        }
    }
    MemorySeriesSource::try_new(all_series).unwrap()
}

fn high_card_source() -> &'static MemorySeriesSource {
    static SRC: OnceLock<MemorySeriesSource> = OnceLock::new();
    SRC.get_or_init(build_high_card_query_source)
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

#[derive(Clone)]
struct BenchCase {
    expr: String,
    steps: usize,
}

/// Supported subset of `rangeQueryCases()` (`promql/bench_test.go:158-334`),
/// expanded across `X` -> `{"one", "hundred"}` and `steps == 0` -> `{1, 1000}`
/// in the exact upstream order.
fn supported_range_query_cases() -> Vec<BenchCase> {
    let raw: &[(&str, usize)] = &[
        ("a_X", 0),
        ("rate(a_X[1m])", 0),
        ("rate(a_X[1m])", 10_000),
        ("rate(sparse[1m])", 10_000),
        ("changes(a_X[1d])", 0),
        ("rate(a_X[1d])", 0),
        ("absent_over_time(a_X[1d])", 0),
        ("-a_X", 0),
        ("a_X - b_X", 0),
        ("a_X - b_X", 10_000),
        ("a_X and b_X{l=~'.*[0-4]$'}", 0),
        ("a_X or b_X{l=~'.*[0-4]$'}", 0),
        ("a_X unless b_X{l=~'.*[0-4]$'}", 0),
        ("a_X and b_X{l='notfound'}", 0),
        ("abs(a_X)", 0),
        ("sum(a_X)", 0),
        ("avg(a_X)", 0),
        ("sum without (l)(h_X)", 0),
        ("sum without (le)(h_X)", 0),
        ("sum by (l)(h_X)", 0),
        ("sum by (le)(h_X)", 0),
        ("rate(a_X[1m]) + rate(b_X[1m])", 0),
        ("sum without (l)(rate(a_X[1m]))", 0),
        (
            "sum without (l)(rate(a_X[1m])) / sum without (l)(rate(b_X[1m]))",
            0,
        ),
        ("a_X + on(l) group_right a_one", 0),
        (r#"count({__name__!=""})"#, 1),
        (r#"count({__name__!="",l=""})"#, 1),
        ("timestamp(a_X)", 0),
    ];

    let mut by_size = Vec::new();
    for &(expr, steps) in raw {
        if !expr.contains('X') {
            by_size.push((expr.to_owned(), steps));
        } else {
            by_size.push((expr.replace('X', "one"), steps));
            by_size.push((expr.replace('X', "hundred"), steps));
        }
    }

    let mut expanded = Vec::new();
    for (expr, steps) in by_size {
        if steps != 0 {
            expanded.push(BenchCase { expr, steps });
        } else {
            expanded.push(BenchCase {
                expr: expr.clone(),
                steps: 1,
            });
            expanded.push(BenchCase { expr, steps: 1_000 });
        }
    }
    expanded
}

const JOIN_CASES: [&str; 5] = [
    "rpc_request_success_total + rpc_request_error_total",
    "rpc_request_success_total + ON (job, instance) GROUP_LEFT rpc_request_error_total",
    r#"rpc_request_success_total AND rpc_request_error_total{instance=~"0.*"}"#,
    r#"rpc_request_success_total OR rpc_request_error_total{instance=~"0.*"}"#,
    r#"rpc_request_success_total UNLESS rpc_request_error_total{instance=~"0.*"}"#,
];

const INFO_GROUP_LEFT_CASES: [(&str, &str); 2] = [
    (
        "Joining info metrics with other metrics with group_left example 1",
        r#"rate(http_server_request_duration_seconds_count[2m]) * on (job, instance) group_left (k8s_cluster_name) target_info{k8s_cluster_name="us-east"}"#,
    ),
    (
        "Joining info metrics with other metrics with group_left example 2",
        "sum by (k8s_cluster_name, http_status_code) (rate(http_server_request_duration_seconds_count[2m]) * on (job, instance) group_left (k8s_cluster_name) target_info)",
    ),
];

fn register_range_queries<'a>(mut suite: Suite<'a>) -> Suite<'a> {
    for case in supported_range_query_cases() {
        let start_ms = ((RANGE_NUM_INTERVALS - case.steps) as i64) * RANGE_INTERVAL_MS;
        let end_ms = (RANGE_NUM_INTERVALS as i64) * RANGE_INTERVAL_MS;
        let rq = RangeQuery::new(start_ms, end_ms, RANGE_INTERVAL_MS);
        let expr_whole = case.expr.clone();
        let expr_prep = case.expr.clone();

        suite = suite
            .bench(
                format!("RangeQuery/expr={},steps={}", case.expr, case.steps),
                move |b| {
                    let engine = blocking_engine();
                    let source = range_source();
                    b.report_allocs();
                    while b.keep_running() {
                        black_box(
                            engine
                                .range_query(source, black_box(expr_whole.as_str()), &rq)
                                .unwrap(),
                        );
                    }
                },
            )
            .bench(
                format!("PreparedRange/expr={},steps={}", case.expr, case.steps),
                move |b| {
                    let engine = async_engine();
                    let rt = multi_thread_rt();
                    let mut prep_opt: Option<PreparedQuery> = None;
                    let prepared = prep_opt.get_or_insert_with(|| {
                        let source = range_source();
                        let p = rt
                            .block_on(engine.prepare_async(source, expr_prep.as_str(), &rq))
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
                },
            );
    }
    suite
}

fn register_join_queries<'a>(mut suite: Suite<'a>) -> Suite<'a> {
    let start_ms = ((JOIN_NUM_INTERVALS - JOIN_STEPS) as i64) * JOIN_INTERVAL_MS;
    let end_ms = (JOIN_NUM_INTERVALS as i64) * JOIN_INTERVAL_MS;
    let rq = RangeQuery::new(start_ms, end_ms, JOIN_INTERVAL_MS);

    for expr in JOIN_CASES {
        let mut warmed = false;
        suite = suite
            .bench(
                format!("JoinQuery/expr={expr}/steps={JOIN_STEPS}"),
                move |b| {
                    let engine = blocking_engine();
                    let source = join_source();
                    if !warmed {
                        black_box(engine.range_query(source, expr, &rq).unwrap());
                        warmed = true;
                    }
                    b.report_allocs();
                    while b.keep_running() {
                        black_box(engine.range_query(source, black_box(expr), &rq).unwrap());
                    }
                },
            )
            .bench(
                format!("PreparedJoin/expr={expr}/steps={JOIN_STEPS}"),
                move |b| {
                    let engine = async_engine();
                    let rt = multi_thread_rt();
                    let mut prep_opt: Option<PreparedQuery> = None;
                    let prepared = prep_opt.get_or_insert_with(|| {
                        let source = join_source();
                        let p = rt
                            .block_on(engine.prepare_async(source, expr, &rq))
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
                },
            );
    }
    suite
}

fn register_info_group_left<'a>(mut suite: Suite<'a>) -> Suite<'a> {
    let rq = RangeQuery::new(0, 2 * 3_600_000, 30_000);
    for (name, query) in INFO_GROUP_LEFT_CASES {
        suite = suite.bench(format!("InfoFunction/{name}"), move |b| {
            let engine = async_engine();
            let rt = multi_thread_rt();
            let mut prep_opt: Option<PreparedQuery> = None;
            let prepared = prep_opt.get_or_insert_with(|| {
                let source = info_source();
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

fn high_card_query_cases() -> Vec<BenchCase> {
    let raw: &[(&str, usize)] = &[
        ("a_X", 1),
        ("a_X", 30),
        ("sum(a_X)", 1),
        ("sum(a_X)", 30),
        ("sum by (k)(a_X)", 30),
        ("rate(a_X[5m])", 1),
        ("rate(a_X[5m])", 30),
        ("sum by (k)(rate(a_X[5m]))", 30),
        ("a_X - b_X", 1),
        ("a_X - b_X", 30),
    ];
    let mut out = Vec::with_capacity(raw.len() * 3);
    for &(expr, steps) in raw {
        for sz in ["1k", "10k", "100k"] {
            out.push(BenchCase {
                expr: expr.replace('X', sz),
                steps,
            });
        }
    }
    out
}

fn register_high_card_queries<'a>(mut suite: Suite<'a>) -> Suite<'a> {
    for case in high_card_query_cases() {
        let start_ms = ((HIGH_CARD_NUM_INTERVALS - case.steps) as i64) * HIGH_CARD_INTERVAL_MS;
        let end_ms = (HIGH_CARD_NUM_INTERVALS as i64) * HIGH_CARD_INTERVAL_MS;
        let rq = RangeQuery::new(start_ms, end_ms, HIGH_CARD_INTERVAL_MS);
        let expr_whole = case.expr.clone();
        let expr_prep = case.expr.clone();

        suite = suite
            .bench(
                format!("HighCardQuery/expr={},steps={}", case.expr, case.steps),
                move |b| {
                    let engine = blocking_engine();
                    let source = high_card_source();
                    b.report_allocs();
                    while b.keep_running() {
                        black_box(
                            engine
                                .range_query(source, black_box(expr_whole.as_str()), &rq)
                                .unwrap(),
                        );
                    }
                },
            )
            .bench(
                format!("PreparedHighCard/expr={},steps={}", case.expr, case.steps),
                move |b| {
                    let engine = async_engine();
                    let rt = multi_thread_rt();
                    let mut prep_opt: Option<PreparedQuery> = None;
                    let prepared = prep_opt.get_or_insert_with(|| {
                        let source = high_card_source();
                        let p = rt
                            .block_on(engine.prepare_async(source, expr_prep.as_str(), &rq))
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
                },
            );
    }
    suite
}

fn main() {
    let mut suite = Suite::from_env("github.com/prometheus/prometheus/promql").with_alloc(&ALLOC);
    suite = register_range_queries(suite);
    suite = register_high_card_queries(suite);
    suite = register_join_queries(suite);
    suite = register_info_group_left(suite);
    suite.run();
}
