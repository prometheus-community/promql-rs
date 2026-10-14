//! Peak heap of one query, which no timing bench or CPU profile shows.
//!
//! Every case runs once under a counting global allocator and reports:
//! what was live before the query (process baseline, including input),
//! the high-water mark during it minus that (the query's incremental
//! heap, including source generation and any held result), what stays
//! live once it returns, and the
//! logical result payload (sample pairs plus visible label bytes) and unique
//! retained Arrow backing capacities, so shared slices are not charged as
//! separate allocations. Headers/schema/Arc overhead is in allocator readings,
//! not backing capacity. Unknown external capacities are counted separately.
//! Allocation-call/requested-byte traffic distinguishes repeated small scratch
//! allocations from peak retention; realloc traffic is not bytes copied.
//!
//! `resident/…` cases collect the result as `Engine::range_query` does.
//! `streamed/…` cases drop each output batch once counted. Most also generate
//! input as polled, so neither input nor result is resident; the explicitly
//! documented `streamed/focused/churn_*` controls use resident input to remove
//! generator traffic. The measured interval includes planning, source-driven
//! batch generation where applicable, execution and one live output batch,
//! not just the evaluator. Source construction is outside it. These bytes are
//! not DataFusion reservations or RSS; reservation checkpoints estimate a
//! different set of owners.
//!
//! ```sh
//! cargo bench -p promql-engine --bench memory              # every case
//! cargo bench -p promql-engine --bench memory -- streamed  # names containing "streamed"
//! ```
//!
//! With `--features heap-profile` jemalloc sits under the counter with
//! profiling on, and `MEMORY_HEAP_DIR=dir` writes a pprof heap profile of
//! each case to `dir/<case>.pb`: a resident case's right after the query,
//! result held; a streamed case's at the output batch where the most was
//! live, found by running it once more. Run one case per process: the
//! first dump's symbolization leaves tens of MiB of debug info cached,
//! which every later case would count as its own.

use std::sync::Arc;

use datafusion::execution::TaskContext;
use datafusion::physical_plan::execution_plan::reset_plan_states;
use datafusion::physical_plan::{execute_stream, ExecutionPlan};
use futures::StreamExt;
use promql_engine::{
    Engine, MemorySeriesSource, RangeQuery, Series, SeriesIdEncoding, SeriesSource,
};

#[path = "support/counting.rs"]
mod counting;
#[path = "support/focused.rs"]
mod focused;
#[path = "support/generated.rs"]
mod generated;
#[path = "support/result_memory.rs"]
mod result_memory;

use counting::Counting;
use generated::{ChunkProbe, Generated, Shape};
use result_memory::logical_bytes;

#[cfg(not(feature = "heap-profile"))]
#[global_allocator]
static ALLOC: Counting<std::alloc::System> = Counting(std::alloc::System);

#[cfg(feature = "heap-profile")]
#[global_allocator]
static ALLOC: Counting<tikv_jemallocator::Jemalloc> = Counting(tikv_jemallocator::Jemalloc);

/// jemalloc reads its options from this symbol before the first
/// allocation, too early for code in `main` to set the environment. One
/// sample per 512 KiB allocated on average.
#[cfg(feature = "heap-profile")]
#[export_name = "_rjem_malloc_conf"]
pub static MALLOC_CONF: &[u8] = b"prof:true,prof_active:true,lg_prof_sample:19\0";

const MINUTE_MS: i64 = 60_000;
const DAY_MS: i64 = 24 * 60 * MINUTE_MS;

struct Case {
    name: &'static str,
    query: &'static str,
    source: fn() -> Arc<dyn SeriesSource>,
    range: RangeQuery,
    streamed: bool,
}

/// `days` of samples scraped every 60s, all of them queried at `step_ms`.
fn days(days: i64, step_ms: i64) -> RangeQuery {
    let end = days * DAY_MS - MINUTE_MS;
    RangeQuery::new(end - days * DAY_MS + MINUTE_MS, end, step_ms)
}

/// 30 days scraped and stepped every 60s: 43,200 samples and steps.
fn thirty_days() -> RangeQuery {
    days(30, MINUTE_MS)
}

const LONG_SERIES: usize = 1_000;
const LONG_SAMPLES: usize = (30 * DAY_MS / MINUTE_MS) as usize;

fn long(chunk: Option<usize>, rows_per_batch: usize) -> Shape {
    long_days(30, chunk, rows_per_batch)
}

fn long_days(days: i64, chunk: Option<usize>, rows_per_batch: usize) -> Shape {
    Shape {
        series: LONG_SERIES,
        samples: (days * DAY_MS / MINUTE_MS) as usize,
        scrape_ms: MINUTE_MS,
        chunk,
        rows_per_batch,
    }
}

/// The engine bench's widest shape: 10,000 series of 300 samples at 15s,
/// the last hour at 30s.
fn wide_range() -> RangeQuery {
    let end = 299 * 15_000;
    RangeQuery::new(end - 60 * MINUTE_MS, end, 30_000)
}

/// Whole series, one per label set, as `MemorySeriesSource` stores them.
fn memory(series: usize, samples: usize, scrape_ms: i64) -> Arc<dyn SeriesSource> {
    let all = (0..series)
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
            let (ts, vs): (Vec<i64>, Vec<f64>) = (0..samples)
                .map(|j| {
                    if j % 1000 == 0 {
                        v = 0.0;
                    }
                    v += 1.0 + ((i + j) % 7) as f64;
                    (j as i64 * scrape_ms, v)
                })
                .unzip();
            Series::new(&labels, ts, vs).unwrap()
        })
        .collect();
    Arc::new(MemorySeriesSource::try_new(all).unwrap())
}

const SELECTOR: &str = "http_requests_total";
const RATE: &str = "rate(http_requests_total[5m])";
const TIMESTAMP: &str = "timestamp(http_requests_total)";
const MIN: &str = "min_over_time(http_requests_total[5m])";

fn contiguous_shape(series: usize, samples: usize) -> Shape {
    Shape {
        series,
        samples,
        scrape_ms: 15_000,
        chunk: Some(4),
        rows_per_batch: 1024,
    }
}

/// The latest sample lies in the lookback, but none reaches the evaluation
/// time: the current ID keeps an unfinished lookup until the next ID or EOF.
fn one_step_tail() -> RangeQuery {
    let at = 20 * 15_000;
    RangeQuery::new(at, at, 30_000)
}

/// 31 steps with a last window ending 15s after the last input sample,
/// retaining the current ID's range history/sweep and output until it closes.
fn range_tail() -> RangeQuery {
    RangeQuery::new(MINUTE_MS, 64 * 15_000, 30_000)
}

fn cases() -> Vec<Case> {
    let mut cases = vec![
        Case {
            name: "resident/30d_1000/one_row/selector",
            query: SELECTOR,
            source: || memory(LONG_SERIES, LONG_SAMPLES, MINUTE_MS),
            range: thirty_days(),
            streamed: false,
        },
        Case {
            name: "resident/30d_1000/one_row/rate_5m",
            query: RATE,
            source: || memory(LONG_SERIES, LONG_SAMPLES, MINUTE_MS),
            range: thirty_days(),
            streamed: false,
        },
        Case {
            name: "resident/1h_10000/one_row/selector",
            query: SELECTOR,
            source: || memory(10_000, 300, 15_000),
            range: wide_range(),
            streamed: false,
        },
        Case {
            name: "resident/30d_1000/chunk120_x8192/selector",
            query: SELECTOR,
            source: || Arc::new(Generated::resident(long(Some(120), 8192))),
            range: thirty_days(),
            streamed: false,
        },
        Case {
            name: "resident/30d_1000/chunk120_x8192/rate_5m",
            query: RATE,
            source: || Arc::new(Generated::resident(long(Some(120), 8192))),
            range: thirty_days(),
            streamed: false,
        },
        Case {
            name: "streamed/30d_1000/one_row_x1/selector",
            query: SELECTOR,
            source: || Arc::new(Generated::streamed(long(None, 1))),
            range: thirty_days(),
            streamed: true,
        },
        Case {
            name: "streamed/30d_1000/one_row_x1/rate_5m",
            query: RATE,
            source: || Arc::new(Generated::streamed(long(None, 1))),
            range: thirty_days(),
            streamed: true,
        },
        Case {
            name: "streamed/30d_1000/chunk120_x360/selector",
            query: SELECTOR,
            source: || Arc::new(Generated::streamed(long(Some(120), 360))),
            range: thirty_days(),
            streamed: true,
        },
        Case {
            name: "streamed/30d_1000/chunk120_x360/rate_5m",
            query: RATE,
            source: || Arc::new(Generated::streamed(long(Some(120), 360))),
            range: thirty_days(),
            streamed: true,
        },
        Case {
            name: "streamed/30d_1000/chunk120_x8192/selector",
            query: SELECTOR,
            source: || Arc::new(Generated::streamed(long(Some(120), 8192))),
            range: thirty_days(),
            streamed: true,
        },
        Case {
            name: "streamed/30d_1000/chunk120_x8192/rate_5m",
            query: RATE,
            source: || Arc::new(Generated::streamed(long(Some(120), 8192))),
            range: thirty_days(),
            streamed: true,
        },
        // Hourly steps keep the result small, so what is left is evaluation
        // state: flat in series length if the window bounds it.
        Case {
            name: "streamed/30d_1000/one_row_x1/rate_5m_step_1h",
            query: RATE,
            source: || Arc::new(Generated::streamed(long(None, 1))),
            range: days(30, 60 * MINUTE_MS),
            streamed: true,
        },
        Case {
            name: "streamed/30d_1000/chunk120_x1/rate_5m_step_1h",
            query: RATE,
            source: || Arc::new(Generated::streamed(long(Some(120), 1))),
            range: days(30, 60 * MINUTE_MS),
            streamed: true,
        },
        Case {
            name: "streamed/90d_1000/one_row_x1/rate_5m_step_1h",
            query: RATE,
            source: || Arc::new(Generated::streamed(long_days(90, None, 1))),
            range: days(90, 60 * MINUTE_MS),
            streamed: true,
        },
        Case {
            name: "streamed/90d_1000/chunk120_x1/rate_5m_step_1h",
            query: RATE,
            source: || Arc::new(Generated::streamed(long_days(90, Some(120), 1))),
            range: days(90, 60 * MINUTE_MS),
            streamed: true,
        },
        Case {
            name: "streamed/contiguous/uint64/100k/chunk4_x1024/selector_one_step_tail",
            query: SELECTOR,
            source: || {
                Arc::new(Generated::streamed_with_ids(
                    contiguous_shape(100_000, 20),
                    SeriesIdEncoding::UInt64,
                ))
            },
            range: one_step_tail(),
            streamed: true,
        },
        Case {
            name: "streamed/contiguous/binary16/100k/chunk4_x1024/selector_one_step_tail",
            query: SELECTOR,
            source: || {
                Arc::new(Generated::streamed_with_ids(
                    contiguous_shape(100_000, 20),
                    SeriesIdEncoding::FixedSizeBinary,
                ))
            },
            range: one_step_tail(),
            streamed: true,
        },
        Case {
            name: "streamed/contiguous/uint64/100k/chunk4_x1024/timestamp_one_step_tail",
            query: TIMESTAMP,
            source: || {
                Arc::new(Generated::streamed_with_ids(
                    contiguous_shape(100_000, 20),
                    SeriesIdEncoding::UInt64,
                ))
            },
            range: one_step_tail(),
            streamed: true,
        },
        Case {
            name: "streamed/contiguous/binary16/100k/chunk4_x1024/timestamp_one_step_tail",
            query: TIMESTAMP,
            source: || {
                Arc::new(Generated::streamed_with_ids(
                    contiguous_shape(100_000, 20),
                    SeriesIdEncoding::FixedSizeBinary,
                ))
            },
            range: one_step_tail(),
            streamed: true,
        },
        Case {
            name: "streamed/contiguous/uint64/10k/chunk4_x1024/rate_5m_31_steps_tail",
            query: RATE,
            source: || {
                Arc::new(Generated::streamed_with_ids(
                    contiguous_shape(10_000, 64),
                    SeriesIdEncoding::UInt64,
                ))
            },
            range: range_tail(),
            streamed: true,
        },
        Case {
            name: "streamed/contiguous/binary16/10k/chunk4_x1024/rate_5m_31_steps_tail",
            query: RATE,
            source: || {
                Arc::new(Generated::streamed_with_ids(
                    contiguous_shape(10_000, 64),
                    SeriesIdEncoding::FixedSizeBinary,
                ))
            },
            range: range_tail(),
            streamed: true,
        },
        Case {
            name: "streamed/contiguous/uint64/10k/chunk4_x1024/min_5m_31_steps_tail",
            query: MIN,
            source: || {
                Arc::new(Generated::streamed_with_ids(
                    contiguous_shape(10_000, 64),
                    SeriesIdEncoding::UInt64,
                ))
            },
            range: range_tail(),
            streamed: true,
        },
        Case {
            name: "streamed/contiguous/binary16/10k/chunk4_x1024/min_5m_31_steps_tail",
            query: MIN,
            source: || {
                Arc::new(Generated::streamed_with_ids(
                    contiguous_shape(10_000, 64),
                    SeriesIdEncoding::FixedSizeBinary,
                ))
            },
            range: range_tail(),
            streamed: true,
        },
    ];
    cases.extend(focused_cases());
    cases
}

fn probe(probe: ChunkProbe) -> Arc<dyn SeriesSource> {
    Arc::new(
        Generated::streamed(Shape {
            series: 4,
            samples: 131072,
            scrape_ms: 1000,
            chunk: None,
            rows_per_batch: 1,
        })
        .chunk_probe(probe),
    )
}

fn churn() -> Arc<dyn SeriesSource> {
    Arc::new(Generated::resident(Shape {
        series: 8192,
        samples: 32,
        scrape_ms: 1000,
        chunk: Some(4),
        rows_per_batch: 1024,
    }))
}

fn focused_cases() -> Vec<Case> {
    vec![
        Case {
            name: "resident/focused/ordered_wide/16384",
            query: focused::SELECTOR,
            source: || focused::wide_source(16384, false, false),
            range: focused::instant(),
            streamed: false,
        },
        Case {
            name: "resident/focused/shuffled_wide/16384",
            query: focused::SELECTOR,
            source: || focused::wide_source(16384, true, false),
            range: focused::instant(),
            streamed: false,
        },
        Case {
            name: "resident/focused/empty_stale_wide/16384",
            query: focused::SELECTOR,
            source: || focused::wide_source(16384, true, true),
            range: focused::instant(),
            streamed: false,
        },
        Case {
            name: "resident/focused/sparse_sum/4096groups_24blocks",
            query: focused::SPARSE_SUM,
            source: || focused::sparse_source(4096, 24),
            range: focused::sparse_range(24),
            streamed: false,
        },
        Case {
            name: "streamed/focused/whole/4x131072",
            query: focused::RATE,
            source: || probe(ChunkProbe::Whole),
            range: RangeQuery::new(0, 60_000, 1000),
            streamed: true,
        },
        Case {
            name: "streamed/focused/second/4x131072",
            query: focused::RATE,
            source: || probe(ChunkProbe::Second),
            range: RangeQuery::new(0, 60_000, 1000),
            streamed: true,
        },
        Case {
            name: "streamed/focused/stale/4x131072",
            query: focused::RATE,
            source: || probe(ChunkProbe::Stale),
            range: RangeQuery::new(0, 60_000, 1000),
            streamed: true,
        },
        Case {
            name: "streamed/focused/churn_rate_32steps/8192",
            query: focused::RATE,
            source: churn,
            range: RangeQuery::new(0, 31_000, 1000),
            streamed: true,
        },
        Case {
            name: "streamed/focused/churn_rate_one_step/8192",
            query: focused::RATE,
            source: churn,
            range: RangeQuery::new(31_000, 31_000, 1000),
            streamed: true,
        },
    ]
}

/// The plan `Engine::range_query_async` runs, to stream instead of collect.
async fn physical(
    engine: &Engine,
    source: &dyn SeriesSource,
    case: &Case,
) -> Arc<dyn ExecutionPlan> {
    engine
        .prepare_async(source, case.query, &case.range)
        .await
        .unwrap()
        .physical_plan()
        .clone()
}

#[derive(Default)]
struct Streamed {
    rows: usize,
    batches: usize,
    result: usize,
    largest_batch: usize,
    /// The output batch at which the most was live, and how much.
    fullest: (usize, usize),
}

/// Drain the plan one batch at a time; `at` runs with each batch in hand.
async fn stream(plan: Arc<dyn ExecutionPlan>, mut at: impl FnMut(usize)) -> Streamed {
    let mut out = Streamed::default();
    let plan = reset_plan_states(plan).unwrap();
    let mut s = execute_stream(plan, Arc::new(TaskContext::default())).unwrap();
    while let Some(b) = s.next().await {
        let b = b.unwrap();
        let bytes = logical_bytes(&b);
        out.rows += b.num_rows();
        out.result += bytes;
        out.largest_batch = out.largest_batch.max(bytes);
        if counting::live() > out.fullest.1 {
            out.fullest = (out.batches, counting::live());
        }
        at(out.batches);
        out.batches += 1;
    }
    out
}

fn mib(n: usize) -> String {
    format!("{:.1}", n as f64 / (1 << 20) as f64)
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    // The bench workflow runs every bench for the tracker's timings, which
    // this one has none of, and its "bencher" would read as a filter.
    if args.iter().any(|a| a == "--output-format") {
        return;
    }
    let filters: Vec<&String> = args.iter().filter(|a| !a.starts_with("--")).collect();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    let engine = Engine::new();
    // Runtime workers, DataFusion's lazy statics and the session's first
    // plan allocate once; a throwaway query keeps them out of the first
    // case's numbers.
    let warm = memory(1, 10, 15_000);
    rt.block_on(engine.range_query_async(warm.as_ref(), SELECTOR, &RangeQuery::new(0, 0, 1)))
        .unwrap();
    drop(warm);

    println!("| case | process baseline MiB | incremental peak MiB | held after MiB | logical payload MiB | unique retained Arrow capacity MiB | external owners | output rows | output batches | largest logical batch MiB | allocation calls | requested traffic MiB |");
    println!("|---|---|---|---|---|---|---|---|---|---|---|---|");
    for case in cases() {
        if !filters.is_empty() && !filters.iter().any(|f| case.name.contains(f.as_str())) {
            continue;
        }
        let source = (case.source)();
        let before = counting::live();
        let traffic_before = counting::traffic();
        counting::reset_peak();
        let mut fullest = None;
        let mut resident = None;
        let (rows, batches, result, largest) = if case.streamed {
            let plan = rt.block_on(physical(&engine, source.as_ref(), &case));
            let s = rt.block_on(stream(plan, |_| {}));
            fullest = Some(s.fullest.0);
            (s.rows, s.batches, s.result, s.largest_batch)
        } else {
            let out = rt
                .block_on(engine.range_query_async(source.as_ref(), case.query, &case.range))
                .unwrap();
            let rows = out.iter().map(|b| b.num_rows()).sum();
            let result = out.iter().map(logical_bytes).sum();
            let largest = out.iter().map(logical_bytes).max().unwrap_or(0);
            let batches = out.len();
            resident = Some(out);
            (rows, batches, result, largest)
        };
        let working = counting::peak().saturating_sub(before);
        let held = counting::live().saturating_sub(before);
        let traffic_after = counting::traffic();
        let calls = traffic_after.0 - traffic_before.0;
        let requested = traffic_after.1 - traffic_before.1;
        // This allocating owner walk is intentionally outside peak/traffic
        // measurement. Streamed results have no retained consumer owners.
        let retained = resident
            .as_deref()
            .map(result_memory::retained)
            .unwrap_or_default();
        if let Some(at) = fullest {
            heap_profile(&rt, &engine, source.as_ref(), &case, at);
        }
        // Symbolizing allocates, so the resident dump waits for the peak
        // to be read, with the result still held.
        if let Some(out) = resident.take() {
            dump(&case);
            drop(out);
        }
        println!(
            "| {} | {} | {} | {} | {} | {} | {} | {rows} | {batches} | {} | {calls} | {} |",
            case.name,
            mib(before),
            mib(working),
            mib(held),
            mib(result),
            mib(retained.bytes),
            retained.external_owners,
            mib(largest),
            mib(requested),
        );
    }
}

#[cfg(not(feature = "heap-profile"))]
fn dump(_: &Case) {}

#[cfg(not(feature = "heap-profile"))]
fn heap_profile(_: &tokio::runtime::Runtime, _: &Engine, _: &dyn SeriesSource, _: &Case, _: usize) {
}

/// Write jemalloc's heap profile of what is live now to
/// `MEMORY_HEAP_DIR/<case>.pb`, if that is set.
#[cfg(feature = "heap-profile")]
fn dump(case: &Case) {
    let Ok(dir) = std::env::var("MEMORY_HEAP_DIR") else {
        return;
    };
    let ctl = jemalloc_pprof::PROF_CTL
        .as_ref()
        .expect("jemalloc profiling is on via _rjem_malloc_conf");
    let path = std::path::Path::new(&dir).join(format!("{}.pb", case.name.replace('/', "_")));
    // `try_lock`: this may run inside the runtime, where tokio forbids
    // `blocking_lock`, and nothing else holds the lock.
    let pprof = ctl
        .try_lock()
        .expect("only this bench dumps")
        .dump_pprof()
        .expect("dump heap profile");
    std::fs::write(&path, pprof).expect("write heap profile");
    eprintln!("wrote {}", path.display());
}

/// Run `case` again and dump jemalloc's heap profile at output batch
/// `fullest`, which the first run found to have the most live.
#[cfg(feature = "heap-profile")]
fn heap_profile(
    rt: &tokio::runtime::Runtime,
    engine: &Engine,
    source: &dyn SeriesSource,
    case: &Case,
    fullest: usize,
) {
    if std::env::var_os("MEMORY_HEAP_DIR").is_none() {
        return;
    }
    let plan = rt.block_on(physical(engine, source, case));
    rt.block_on(stream(plan, |i| {
        if i == fullest {
            dump(case);
        }
    }));
}
