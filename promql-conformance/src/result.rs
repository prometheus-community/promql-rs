//! The shape both implementations produce, and the seam the Rust engine
//! will plug into.
//!
//! Both sides of a differential test have to speak one vocabulary before
//! anything can be compared, so [`QueryResult`] is deliberately the
//! oracle's wire format decoded into Rust terms rather than anything
//! engine-specific.

use std::collections::BTreeMap;

use promql_parser::SeriesDescription;

/// A label set. `BTreeMap` because comparison must be order-insensitive
/// and upstream's `labels.New` sorts by name anyway, so this matches Go
/// without a separate sorting step.
pub type Labels = BTreeMap<String, String>;

/// One float sample.
#[derive(Debug, Clone, PartialEq)]
pub struct Point {
    pub t: i64,
    pub v: f64,
}

/// One matrix row.
#[derive(Debug, Clone, PartialEq)]
pub struct Series {
    pub labels: Labels,
    pub floats: Vec<Point>,
    /// Native-histogram points, which the protocol does not encode yet.
    /// Non-zero means the case is out of reach rather than mismatched.
    pub histograms: usize,
}

/// One vector element.
#[derive(Debug, Clone, PartialEq)]
pub struct Sample {
    pub labels: Labels,
    pub t: i64,
    pub v: f64,
    pub histogram: bool,
}

/// The result of a range query.
///
/// A range query always yields a matrix in practice; the other variants
/// exist because the comparer must handle whatever the oracle reports
/// rather than assume.
#[derive(Debug, Clone, PartialEq)]
pub enum QueryResult {
    Matrix(Vec<Series>),
    Vector(Vec<Sample>),
    Scalar {
        v: f64,
        t: i64,
    },
    Str {
        v: String,
        t: i64,
    },
    /// The query failed. Comparison counts two errors as a match
    /// regardless of message, matching Go.
    Error(String),
}

impl QueryResult {
    /// Whether any part of this result is a native histogram, which the
    /// protocol cannot carry yet.
    pub fn has_histograms(&self) -> bool {
        match self {
            QueryResult::Matrix(series) => series.iter().any(|s| s.histograms > 0),
            QueryResult::Vector(samples) => samples.iter().any(|s| s.histogram),
            _ => false,
        }
    }

    pub fn kind(&self) -> &'static str {
        match self {
            QueryResult::Matrix(_) => "matrix",
            QueryResult::Vector(_) => "vector",
            QueryResult::Scalar { .. } => "scalar",
            QueryResult::Str { .. } => "string",
            QueryResult::Error(_) => "error",
        }
    }
}

/// A PromQL range-query engine.
///
/// The oracle receives the raw `load` block so that Go performs its own
/// parsing; an implementation of this trait receives the series already
/// parsed, which is what a real engine gets. Turning a
/// [`SeriesDescription`] into timestamped samples is the storage layer's
/// job, not the harness's.
///
/// # Contract
///
/// The oracle configures Prometheus a particular way, and an
/// implementation has to match it or diverge for reasons that have
/// nothing to do with being wrong. From `scripts/promql-oracle`:
///
/// - **Sample timestamps.** Value `i` of a series sits at
///   `i * interval_secs * 1000` ms, counting from 0 — upstream's
///   `testStartTime` is the Unix epoch. An omitted value (`_`) emits no
///   sample at all, and `stale` emits `StaleNaN`
///   (`0x7ff0000000000002`). Note `1+1x40` is 41 points, not 40; see
///   `promql-parser/tests/series_desc.rs`.
/// - **Lookback delta 5m**, Prometheus's default.
/// - **`@` modifier and negative offsets enabled.** 32 and 8 corpus
///   cases respectively need them.
/// - **All four parser feature flags enabled**, matching upstream's
///   `TestParserOpts` (`promql/promqltest/test.go`):
///   `promql-experimental-functions`, `promql-duration-expr`,
///   `promql-extended-range-selectors` and `promql-binop-fill-modifiers`.
///   The corpus needs them; a stock engine rejects those queries.
///   Evals that need one are still run, but counted nowhere: see
///   `Verdict::Gated`.
/// - **`MaxSamples` effectively unlimited** (1e10), so no case should
///   fail on a sample limit.
/// - **Default subquery step 1m**, but unobservable: the corpus contains
///   no stepless subquery, so this cannot be exercised.
/// - **A range query always yields a matrix.** The other
///   [`QueryResult`] variants exist for completeness, not because a
///   range query produces them.
/// - **Error text is not compared**, only the fact of erroring; 11
///   corpus cases legitimately error.
///
/// # Async
///
/// This trait is synchronous because the harness is: `libtest-mimic`
/// trials are plain closures. A DataFusion-backed engine is async, so
/// the implementation should own a runtime and `block_on` inside
/// `range_query` rather than the trait becoming async and forcing every
/// caller to acquire one.
pub trait Engine {
    fn range_query(
        &self,
        load: &[LoadedSeries<'_>],
        query: &str,
        start_ms: i64,
        end_ms: i64,
        step_ms: i64,
    ) -> Result<QueryResult, EngineError>;

    /// An instant query, answered with the type its expression has:
    /// a vector, a matrix for a range selector, a scalar or a string.
    ///
    /// The default is the one-step range query this seam offered before
    /// it had an instant method, whose answer is always a matrix; the
    /// runner reshapes that by what the script expects. An engine that
    /// knows the expression's type overrides this, because a range query
    /// cannot carry a string or a range vector, and Prometheus refuses
    /// both in one.
    fn instant_query(
        &self,
        load: &[LoadedSeries<'_>],
        query: &str,
        at_ms: i64,
    ) -> Result<QueryResult, EngineError> {
        self.range_query(load, query, at_ms, at_ms, 1)
    }
}

/// One `load <interval>` block's worth of data.
///
/// A slice of these rather than a single `(series, interval)` pair
/// because a promqltest script accumulates: `histograms.test` issues 37
/// loads against 4 clears, and one file mixes intervals freely —
/// `functions.test` uses 10s, 1ms, 4m and six others. Each block keeps
/// its own interval, so sample *i* of a block lands at
/// `i * interval_ms(block)` and blocks merge by label set. Collapsing
/// them to one interval would silently move half that file's samples.
///
/// The YAML corpus passes exactly one block, which is the degenerate
/// case rather than a separate code path.
#[derive(Debug, Clone, Copy)]
pub struct LoadedSeries<'a> {
    pub series: &'a [SeriesDescription],
    /// The block's sample interval, in seconds.
    pub interval_secs: f64,
}

#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    /// The expected state until an execution layer exists. Every
    /// differential test fails with this, and that failure is the
    /// engine's specification.
    #[error("not implemented: no Rust execution engine exists yet")]
    NotImplemented,
    /// The engine exists but the query uses a feature it lacks, named so
    /// the suite can count cases per missing feature. Distinct from
    /// [`Self::Other`] so that "not built yet" is never mistaken for
    /// "built wrong".
    #[error("{0} is not supported yet")]
    Unsupported(String),
    #[error("{0}")]
    Other(String),
}

/// Stand-in engine that fails every query.
///
/// Replace with the real engine once PR #4's `TableProvider` and an
/// execution layer land; the differential suite then starts going green
/// case by case.
pub struct Unimplemented;

impl Engine for Unimplemented {
    fn range_query(
        &self,
        _load: &[LoadedSeries<'_>],
        _query: &str,
        _start_ms: i64,
        _end_ms: i64,
        _step_ms: i64,
    ) -> Result<QueryResult, EngineError> {
        Err(EngineError::NotImplemented)
    }
}
