//! Golden parity corpus: a fixed set of range-query results, recorded once
//! from the one-row-per-series engine this branch stacks on — main at
//! `4dea702` plus the memory source's chunk knobs — replayed against
//! whatever `SeriesSource` shape a later step introduces.
//!
//! `tests/chunked.rs` recomputes the unchunked answer on every run and
//! diffs it live; that only catches a regression the moment it lands. This
//! corpus instead pins the *actual* one-row output as data, so a later
//! step's chunked or partitioned source is checked against a value that
//! cannot silently drift with the kernels it is supposed to match.
//!
//! `tests/testdata/parity.json` holds the corpus: the series (as
//! `promqltest` load lines) and, per case, the query, its range, and the
//! recorded output. `regenerate_corpus` below is the only thing that
//! writes it, and only when asked, with `PROMQL_REGEN_PARITY=1` — see its
//! doc for why.

use std::collections::BTreeMap;
use std::sync::Arc;

use promql_engine::{Engine, MemorySeriesSource, RangeQuery, Series};
use serde::{Deserialize, Serialize};
use std::num::{NonZeroU64, NonZeroUsize};

fn nz(n: usize) -> NonZeroUsize {
    NonZeroUsize::new(n).expect("a count of at least one")
}

fn nz64(n: u64) -> NonZeroU64 {
    NonZeroU64::new(n).expect("a span of at least a millisecond")
}

const CORPUS_PATH: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/testdata/parity.json");

#[derive(Serialize, Deserialize)]
struct Corpus {
    interval_secs: f64,
    /// `promqltest` `load` lines. Deliberately repeats some label sets
    /// (duplicate descriptions), which `MemorySeriesSource::from_descriptions`
    /// merges by taking the later line's value at any timestamp both define.
    series: Vec<String>,
    cases: Vec<Case>,
}

#[derive(Serialize, Deserialize)]
struct Case {
    name: String,
    query: String,
    start_ms: i64,
    end_ms: i64,
    step_ms: i64,
    expected: Vec<ExpectedSeries>,
}

#[derive(Serialize, Deserialize)]
struct ExpectedSeries {
    labels: BTreeMap<String, String>,
    timestamps: Vec<i64>,
    values: Vec<f64>,
}

fn key(labels: &BTreeMap<String, String>) -> String {
    labels
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join(",")
}

fn series_key(s: &Series) -> String {
    key(&s
        .labels()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect())
}

fn load_corpus() -> Corpus {
    let text = std::fs::read_to_string(CORPUS_PATH).unwrap_or_else(|e| {
        panic!(
            "{CORPUS_PATH} is missing or unreadable ({e}); run `PROMQL_REGEN_PARITY=1 cargo test \
             -p promql-engine --test parity regenerate_corpus -- --ignored` to record it"
        )
    });
    serde_json::from_str(&text).expect("parity.json parses")
}

fn descriptions(series: &[String]) -> Vec<promql_parser::SeriesDescription> {
    series
        .iter()
        .map(|l| promql_parser::parse_series_desc(l).unwrap_or_else(|e| panic!("{l}: {e:?}")))
        .collect()
}

fn run(source: &MemorySeriesSource, case: &Case) -> Vec<Series> {
    let range = RangeQuery::new(case.start_ms, case.end_ms, case.step_ms);
    let batches = Engine::blocking()
        .unwrap()
        .range_query(source, &case.query, &range)
        .unwrap_or_else(|e| panic!("{}: {}: {e}", case.name, case.query));
    // The corpus is one series per label set; a blocked source answers
    // in several rows per label set, joined here.
    promql_engine::series::coalesce(promql_engine::series::decode(&batches).unwrap()).unwrap()
}

/// `expected` and `actual` must agree on the set of series, and per
/// series on timestamps exactly and values within `1e-9`.
fn assert_matches_recorded(case: &Case, mut actual: Vec<Series>) {
    let mut expected: Vec<&ExpectedSeries> = case.expected.iter().collect();
    expected.sort_by_key(|e| key(&e.labels));
    actual.sort_by_key(series_key);

    let expected_keys: Vec<String> = expected.iter().map(|e| key(&e.labels)).collect();
    let actual_keys: Vec<String> = actual.iter().map(series_key).collect();
    assert_eq!(
        expected_keys, actual_keys,
        "{} ({}): different set of series than recorded",
        case.name, case.query
    );

    for (e, a) in expected.iter().zip(&actual) {
        assert_eq!(
            e.timestamps,
            a.timestamps(),
            "{} ({}) {}: timestamps differ from the recorded corpus",
            case.name,
            case.query,
            key(&e.labels)
        );
        assert_eq!(
            e.values.len(),
            a.values().len(),
            "{} ({}) {}: value count differs from the recorded corpus",
            case.name,
            case.query,
            key(&e.labels)
        );
        for (ev, av) in e.values.iter().zip(a.values()) {
            assert!(
                (ev - av).abs() < 1e-9 || (ev.is_nan() && av.is_nan()),
                "{} ({}) {}: expected {:?}, got {:?}",
                case.name,
                case.query,
                key(&e.labels),
                e.values,
                a.values()
            );
        }
    }
}

/// The gate for this step: every recorded case, replayed against the
/// same one-row `MemorySeriesSource` shape it was recorded from. This is
/// the corpus checking itself in — it must equal what is on disk, not
/// what a later chunked or partitioned source produces.
#[test]
fn replay_against_unchunked() {
    let corpus = load_corpus();
    let source = Arc::new(MemorySeriesSource::from_descriptions(
        &descriptions(&corpus.series),
        corpus.interval_secs,
    ));
    for case in &corpus.cases {
        let actual = run(&source, case);
        assert_matches_recorded(case, actual);
    }
}

/// `chunked(k)` for `k` in `{0, one interval, 150s, 1h}`, against the
/// same recorded corpus. `0` is one sample per chunk, so every window
/// crosses a chunk boundary.
#[test]
fn replay_against_chunked() {
    let corpus = load_corpus();
    let interval_ms = (corpus.interval_secs * 1000.0).round() as i64;
    for chunk_ms in [0, interval_ms, 150_000, 3_600_000] {
        let source = Arc::new(
            MemorySeriesSource::from_descriptions(
                &descriptions(&corpus.series),
                corpus.interval_secs,
            )
            .chunked(chunk_ms),
        );
        for case in &corpus.cases {
            let actual = run(&source, case);
            assert_matches_recorded(case, actual);
        }
    }
}

/// One chunk per batch, on top of `chunked(0)`'s one sample per chunk: every
/// step lands in its own batch. Packing to `ROWS_PER_BATCH` (8192) leaves
/// the whole corpus in one batch at 150s and 1h chunks, so without this
/// the per-batch emit and the re-reserve of the still-open series would
/// only be exercised by `tests/chunked.rs`, not by the recorded corpus.
#[test]
fn replay_against_chunked_one_row_per_batch() {
    let corpus = load_corpus();
    let source = Arc::new(
        MemorySeriesSource::from_descriptions(&descriptions(&corpus.series), corpus.interval_secs)
            .chunked(0)
            .rows_per_batch(nz(1)),
    );
    for case in &corpus.cases {
        let actual = run(&source, case);
        assert_matches_recorded(case, actual);
    }
}

/// `partitions(4)`: a series never straddles partitions, so each one's
/// answer must not depend on which partition it came from or what else
/// shared it.
#[test]
fn replay_against_partitions() {
    let corpus = load_corpus();
    let source = Arc::new(
        MemorySeriesSource::from_descriptions(&descriptions(&corpus.series), corpus.interval_secs)
            .partitions(nz(4)),
    );
    for case in &corpus.cases {
        let actual = run(&source, case);
        assert_matches_recorded(case, actual);
    }
}

/// `blocks(k)` for `k` in `{one interval, 150s, 1h}`: at one interval
/// every step is the first of its block, so every window is answered from
/// reach-back alone; at 1h most cases sit in one block with an edge or
/// none. Then blocks over one-sample chunks in four partitions, the shape
/// a sharded chunked store with fixed block ranges hands over.
#[test]
fn replay_against_blocks() {
    let corpus = load_corpus();
    let interval_ms = (corpus.interval_secs * 1000.0).round() as u64;
    let stored = || {
        MemorySeriesSource::from_descriptions(&descriptions(&corpus.series), corpus.interval_secs)
    };
    for block_ms in [interval_ms, 150_000, 3_600_000] {
        let source = stored().blocks(nz64(block_ms));
        for case in &corpus.cases {
            assert_matches_recorded(case, run(&source, case));
        }
    }
    let source = stored()
        .blocks(nz64(150_000))
        .chunked(0)
        .rows_per_batch(nz(3))
        .partitions(nz(4));
    for case in &corpus.cases {
        assert_matches_recorded(case, run(&source, case));
    }
}

/// Records `tests/testdata/parity.json` from *today's* engine. Not a
/// check: a generator, gated behind an env var rather than `#[ignore]`
/// alone so a plain `cargo test --test parity -- --ignored` (which a
/// later step might run to sweep up every ignored test at once) cannot
/// accidentally overwrite the golden file it is also being checked
/// against. Run explicitly:
///
/// ```text
/// PROMQL_REGEN_PARITY=1 cargo test -p promql-engine --test parity \
///     regenerate_corpus -- --ignored --exact
/// ```
#[test]
#[ignore = "writes tests/testdata/parity.json; run with PROMQL_REGEN_PARITY=1"]
fn regenerate_corpus() {
    if std::env::var("PROMQL_REGEN_PARITY").is_err() {
        panic!("set PROMQL_REGEN_PARITY=1 to regenerate tests/testdata/parity.json");
    }
    let corpus = generate::corpus();
    let json = serde_json::to_string_pretty(&corpus).unwrap();
    std::fs::write(CORPUS_PATH, json).unwrap();
}

/// Deterministic corpus generation, isolated so `regenerate_corpus` is
/// the only caller: nothing here needs to be `pub` outside this file, and
/// the replay tests above depend only on the recorded JSON, never on this
/// module, so the corpus can't silently regenerate itself differently
/// between recording and replay.
mod generate {
    use std::sync::Arc;

    use promql_engine::{Engine, MemorySeriesSource, RangeQuery};
    use promql_parser::SeriesDescription;

    use super::{Case, Corpus, ExpectedSeries};

    const INTERVAL_SECS: f64 = 15.0;
    const SAMPLES: usize = 40;
    /// Half of `COUNTERS`/`GAUGES` get a straight run of samples; the
    /// other half are split into two same-labelled lines that each hold
    /// half the timeline, to exercise `from_descriptions`'s "duplicate
    /// descriptions merge, later line wins any timestamp both define"
    /// rule (here they don't overlap, so the merge is a plain union).
    const COUNTERS: usize = 60;
    const GAUGES: usize = 60;
    const RANGE: RangeQuery_ = RangeQuery_ {
        start_ms: 0,
        end_ms: 600_000,
        step_ms: 30_000,
    };

    /// [`RangeQuery`] has no `Copy`/const constructor since its lookback
    /// comes from a `Duration`; this is just the three fields the
    /// generator needs at compile time.
    struct RangeQuery_ {
        start_ms: i64,
        end_ms: i64,
        step_ms: i64,
    }

    /// xorshift64*, seeded fixed: deterministic and dependency-free. Only
    /// used to shape synthetic samples, never for anything security- or
    /// statistics-sensitive.
    struct Rng(u64);
    impl Rng {
        fn new(seed: u64) -> Self {
            Self(seed | 1)
        }
        fn next_u64(&mut self) -> u64 {
            let mut x = self.0;
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            self.0 = x;
            x.wrapping_mul(0x2545F4914F6CDD1D)
        }
        /// `[0, 1)`.
        fn next_f64(&mut self) -> f64 {
            (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
        }
        fn chance(&mut self, p: f64) -> bool {
            self.next_f64() < p
        }
        fn range(&mut self, lo: f64, hi: f64) -> f64 {
            lo + self.next_f64() * (hi - lo)
        }
    }

    #[derive(Clone, Copy)]
    enum Token {
        Value(f64),
        Gap,
        Stale,
    }

    fn render(tokens: &[Token]) -> String {
        tokens
            .iter()
            .map(|t| match t {
                Token::Value(v) => format!("{v:.3}"),
                Token::Gap => "_".to_string(),
                Token::Stale => "stale".to_string(),
            })
            .collect::<Vec<_>>()
            .join(" ")
    }

    /// A monotonically increasing sequence with occasional counter
    /// resets, gaps and stale markers, so `Rate`/`Increase`/`Resets` and
    /// friends all see the cases they exist for.
    fn counter_tokens(rng: &mut Rng, start: f64) -> Vec<Token> {
        let mut cur = start;
        let mut out = Vec::with_capacity(SAMPLES);
        for _ in 0..SAMPLES {
            if rng.chance(0.06) {
                out.push(Token::Gap);
                continue;
            }
            if rng.chance(0.06) {
                out.push(Token::Stale);
                continue;
            }
            if rng.chance(0.05) {
                cur = rng.range(0.0, 2.0); // counter reset
            } else {
                cur += rng.range(0.0, 5.0);
            }
            out.push(Token::Value(cur));
        }
        out
    }

    /// A random walk, up and down, so `MinOverTime`/`MaxOverTime`/
    /// `Changes`/`Delta` see real direction changes rather than a
    /// monotone line.
    fn gauge_tokens(rng: &mut Rng, start: f64) -> Vec<Token> {
        let mut cur = start;
        let mut out = Vec::with_capacity(SAMPLES);
        for _ in 0..SAMPLES {
            if rng.chance(0.06) {
                out.push(Token::Gap);
                continue;
            }
            if rng.chance(0.06) {
                out.push(Token::Stale);
                continue;
            }
            cur += rng.range(-5.0, 5.0);
            out.push(Token::Value(cur));
        }
        out
    }

    /// Either one `metric{labels} <tokens...>` line, or (for the
    /// "duplicate descriptions" case, when `split` is true) two lines
    /// with identical labels that between them cover the same timeline,
    /// first half in one line and second half in the other.
    fn lines(metric: &str, labels: &str, tokens: &[Token], split: bool) -> Vec<String> {
        if !split {
            return vec![format!("{metric}{{{labels}}} {}", render(tokens))];
        }
        let mid = tokens.len() / 2;
        let mut first: Vec<Token> = tokens[..mid].to_vec();
        first.extend(std::iter::repeat_n(Token::Gap, tokens.len() - mid));
        let mut second: Vec<Token> = std::iter::repeat_n(Token::Gap, mid).collect();
        second.extend(tokens[mid..].to_vec());
        vec![
            format!("{metric}{{{labels}}} {}", render(&first)),
            format!("{metric}{{{labels}}} {}", render(&second)),
        ]
    }

    fn build_series(rng: &mut Rng) -> Vec<String> {
        let mut out = Vec::new();
        for i in 0..COUNTERS {
            let job = if i % 2 == 0 { "a" } else { "b" };
            let labels = format!(r#"job="{job}",pod="{i}""#);
            let tokens = counter_tokens(rng, 10.0 + i as f64);
            out.extend(lines("counter_metric", &labels, &tokens, i % 2 == 0));
        }
        for i in 0..GAUGES {
            let job = if i % 2 == 0 { "a" } else { "b" };
            let labels = format!(r#"job="{job}",pod="{i}""#);
            let tokens = gauge_tokens(rng, 50.0 + i as f64);
            out.extend(lines("gauge_metric", &labels, &tokens, i % 2 == 0));
        }
        out
    }

    const RANGE_FUNCS: &[&str] = &[
        "rate",
        "increase",
        "delta",
        "irate",
        "idelta",
        "sum_over_time",
        "avg_over_time",
        "min_over_time",
        "max_over_time",
        "count_over_time",
        "last_over_time",
        "present_over_time",
        "changes",
        "resets",
    ];

    fn case_queries() -> Vec<(String, String)> {
        let mut out = Vec::new();
        for metric in ["counter_metric", "gauge_metric"] {
            out.push((format!("plain_selector_{metric}"), metric.to_string()));
            for func in RANGE_FUNCS {
                out.push((format!("{func}_{metric}"), format!("{func}({metric}[5m])")));
            }
        }
        out.push((
            "offset".to_string(),
            r#"counter_metric{job="a"} offset 1m"#.to_string(),
        ));
        out.push((
            "at_modifier".to_string(),
            r#"counter_metric{job="a"} @ 300"#.to_string(),
        ));
        out.push((
            "sum_by_job".to_string(),
            "sum by (job) (gauge_metric)".to_string(),
        ));
        out.push((
            "count_by_job".to_string(),
            "count by (job) (counter_metric)".to_string(),
        ));
        out
    }

    /// Builds the corpus by actually running every case against today's
    /// one-row `MemorySeriesSource`: the recorded `expected` is this
    /// engine's real output, not a hand-derived one, because the corpus
    /// exists to catch a later source shape disagreeing with this one —
    /// not to re-verify PromQL semantics `tests/range.rs` already covers.
    pub(super) fn corpus() -> Corpus {
        let mut rng = Rng::new(0x5EED_C0FF_EE42_1234);
        let series = build_series(&mut rng);
        let source = Arc::new(MemorySeriesSource::from_descriptions(
            &descriptions(&series),
            INTERVAL_SECS,
        ));
        let engine = Engine::blocking().unwrap();
        let range = RangeQuery::new(RANGE.start_ms, RANGE.end_ms, RANGE.step_ms);

        let cases = case_queries()
            .into_iter()
            .map(|(name, query)| {
                let batches = engine
                    .range_query(source.as_ref(), &query, &range)
                    .unwrap_or_else(|e| panic!("{name} ({query}): {e}"));
                let expected = promql_engine::series::decode(&batches)
                    .unwrap()
                    .iter()
                    .map(|s| ExpectedSeries {
                        labels: s
                            .labels()
                            .map(|(k, v)| (k.to_string(), v.to_string()))
                            .collect(),
                        timestamps: s.timestamps().to_vec(),
                        values: s.values().to_vec(),
                    })
                    .collect();
                Case {
                    name,
                    query,
                    start_ms: RANGE.start_ms,
                    end_ms: RANGE.end_ms,
                    step_ms: RANGE.step_ms,
                    expected,
                }
            })
            .collect();

        Corpus {
            interval_secs: INTERVAL_SECS,
            series,
            cases,
        }
    }

    fn descriptions(series: &[String]) -> Vec<SeriesDescription> {
        series
            .iter()
            .map(|l| promql_parser::parse_series_desc(l).unwrap_or_else(|e| panic!("{l}: {e:?}")))
            .collect()
    }
}
