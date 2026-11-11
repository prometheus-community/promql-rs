//! The DataFusion engine behind the harness's [`Engine`] seam.
//!
//! Each case seeds a fresh [`MemorySeriesSource`] from its `load` blocks
//! — the same series descriptions the oracle parses from the raw text —
//! and runs the query through `promql_engine`. Error mapping is the whole
//! of the logic here, and it matters: a query the engine rejects as
//! invalid is an *answer* (Prometheus rejects queries too, and both
//! erroring is a match), an unsupported feature is "not built yet", and
//! anything else is a bug to be reported as one.

use std::collections::BTreeMap;
use std::num::{NonZeroU64, NonZeroUsize};

use promql_engine::series::{coalesce, decode};
use promql_engine::{InstantResult, MemorySeriesSource, RangeQuery, SeriesIdEncoding};
use promql_histogram::{CounterResetHint, FloatHistogram};

use crate::result::{
    Engine, EngineError, HPoint, LoadedSeries, Point, QueryResult, Sample, Series,
};

pub struct DataFusionEngine {
    inner: promql_engine::Engine,
    source: SourceMode,
}

/// How the store hands each case's series to the engine. The answers must
/// not depend on it: the chunked and partitioned modes replay the whole
/// corpus through the shapes a real store produces, which the corpus's
/// own series, one row each, never reach.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceMode {
    /// One series per label set, one partition.
    Plain,
    /// [`MemorySeriesSource::chunked`].
    Chunked(i64),
    /// [`MemorySeriesSource::blocks`].
    Blocks(NonZeroU64),
    /// [`MemorySeriesSource::partitions`].
    Partitions(NonZeroUsize),
    /// ID-contiguous one-sample chunks, tiny batches, blocks and source partitions.
    Contiguous(SeriesIdEncoding),
}

const DEFAULT_BLOCK_MS: NonZeroU64 = NonZeroU64::new(60_000).unwrap();
const DEFAULT_PARTITIONS: NonZeroUsize = NonZeroUsize::new(4).unwrap();

impl SourceMode {
    /// The variable [`DataFusionEngine::new`] reads the mode from.
    pub const VAR: &'static str = "PROMQL_CONFORMANCE_SOURCE";

    /// `plain`, `chunked[=<ms>]`, `blocks[=<ms>]`, `partitions[=<n>]`,
    /// `contiguous-u64` or `contiguous-binary`.
    /// Bare `chunked` is one sample per chunk, so every window of the
    /// corpus crosses chunks; bare `blocks` is one minute, the step most
    /// of the corpus evaluates at, so most steps open a block; bare
    /// `partitions` is four.
    pub fn parse(s: &str) -> Result<Self, String> {
        let (mode, arg) = match s.split_once('=') {
            Some((mode, arg)) => (mode, Some(arg)),
            None => (s, None),
        };
        let bad = || {
            format!(
                "{}={s:?}: expected plain, chunked[=<ms>], blocks[=<ms>], partitions[=<n>], contiguous-u64 or contiguous-binary",
                Self::VAR
            )
        };
        match (mode, arg) {
            ("plain", None) => Ok(Self::Plain),
            ("contiguous-u64", None) => Ok(Self::Contiguous(SeriesIdEncoding::UInt64)),
            ("contiguous-binary", None) => Ok(Self::Contiguous(SeriesIdEncoding::FixedSizeBinary)),
            ("chunked", None) => Ok(Self::Chunked(0)),
            ("chunked", Some(ms)) => ms
                .parse()
                .ok()
                .filter(|ms| *ms >= 0)
                .map(Self::Chunked)
                .ok_or_else(bad),
            ("blocks", None) => Ok(Self::Blocks(DEFAULT_BLOCK_MS)),
            ("blocks", Some(ms)) => ms.parse().ok().map(Self::Blocks).ok_or_else(bad),
            ("partitions", None) => Ok(Self::Partitions(DEFAULT_PARTITIONS)),
            ("partitions", Some(n)) => n.parse().ok().map(Self::Partitions).ok_or_else(bad),
            _ => Err(bad()),
        }
    }

    fn apply(self, source: MemorySeriesSource) -> MemorySeriesSource {
        match self {
            Self::Plain => source,
            Self::Chunked(ms) => source.chunked(ms),
            Self::Blocks(ms) => source.blocks(ms),
            Self::Partitions(n) => source.partitions(n),
            Self::Contiguous(id) => source
                .with_ids(id)
                .chunked(0)
                .blocks(DEFAULT_BLOCK_MS)
                .partitions(DEFAULT_PARTITIONS)
                .rows_per_batch(NonZeroUsize::new(7).unwrap()),
        }
    }
}

impl DataFusionEngine {
    /// The source mode comes from [`SourceMode::VAR`], so the promqltest
    /// binary can replay the corpus in every mode without a flag of its
    /// own; unset is [`SourceMode::Plain`].
    pub fn new() -> Result<Self, EngineError> {
        let source = match std::env::var(SourceMode::VAR) {
            Ok(v) => SourceMode::parse(&v).map_err(EngineError::Other)?,
            Err(std::env::VarError::NotPresent) => SourceMode::Plain,
            Err(e) => return Err(EngineError::Other(format!("{}: {e}", SourceMode::VAR))),
        };
        Self::with_source(source)
    }

    pub fn with_source(source: SourceMode) -> Result<Self, EngineError> {
        Ok(Self {
            inner: promql_engine::Engine::blocking_with_options(engine_options())
                .map_err(|e| EngineError::Other(e.to_string()))?,
            source,
        })
    }
}

/// What the harness parses with: every gate on, as upstream's promqltest
/// does (`TestParserOpts` in `promql/promqltest/test.go`). The corpus
/// uses `mad_over_time`, `fill`, `anchored` and the rest, which stock
/// Prometheus refuses; which flag each needs is in `UNSUPPORTED.md`.
fn engine_options() -> promql_engine::EngineOptions {
    promql_engine::EngineOptions {
        parser: promql_parser::ParserOptions::all(),
    }
}

/// Flatten several `load` blocks into the rows one store holds.
///
/// `MemorySeriesSource::from_descriptions` takes a single interval, so
/// the cross-block merge happens here rather than in `promql-engine`:
/// this is test-fixture shaping, and the engine has no business knowing
/// that a script can load twice at different resolutions.
///
/// Sample *i* of a block is at `i * interval_ms(block)`, matching what
/// `from_descriptions` does within one block. Where two blocks write the
/// same label set at the same timestamp the later one wins, which is the
/// order an appender would have applied them in.
///
/// Returns the rows rather than the store so the tests below can read
/// them back: the store's own accessor is test-only *within the engine*
/// and invisible from here.
fn merge(load: &[LoadedSeries<'_>]) -> Vec<promql_engine::Series> {
    let mut merged: BTreeMap<Vec<(String, String)>, BTreeMap<i64, Value>> = BTreeMap::new();

    for block in load {
        let interval_ms = (block.interval_secs * 1000.0).round() as i64;
        let mut classic: BTreeMap<Vec<(String, String)>, BTreeMap<i64, Classic>> = BTreeMap::new();
        for sd in block.series {
            let labels = label_set(sd);
            let part = block.with_nhcb.then(|| classic_part(&labels)).flatten();
            let samples = merged.entry(labels).or_default();
            for (i, v) in sd.values.iter().enumerate().filter(|(_, v)| !v.omitted) {
                let t = i as i64 * interval_ms;
                match &v.histogram {
                    Some(h) => {
                        samples.insert(t, Value::Histogram(h.clone()));
                    }
                    None => {
                        samples.insert(t, Value::Float(v.value));
                        if let Some((base, part)) = &part {
                            classic
                                .entry(base.clone())
                                .or_default()
                                .entry(t)
                                .or_default()
                                .set(*part, v.value);
                        }
                    }
                }
            }
        }
        // `appendCustomHistogram`: after the block's own series, the native
        // histograms its classic ones convert to.
        for (labels, by_time) in classic {
            let samples = merged.entry(labels).or_default();
            for (t, classic) in by_time {
                if let Some(h) = classic.convert() {
                    samples.insert(t, Value::Histogram(h));
                }
            }
        }
    }

    merged
        .into_iter()
        .map(|(labels, samples)| {
            let labels: Vec<(&str, &str)> = labels
                .iter()
                .map(|(n, v)| (n.as_str(), v.as_str()))
                .collect();
            let mut floats = (Vec::new(), Vec::new());
            let mut histograms = Vec::new();
            for (t, value) in samples {
                match value {
                    Value::Float(v) => {
                        floats.0.push(t);
                        floats.1.push(v);
                    }
                    Value::Histogram(h) => histograms.push((t, h)),
                }
            }
            stored_hints(&floats.0, &mut histograms);
            promql_engine::Series::new(&labels, floats.0, floats.1)
                .and_then(|series| series.with_histograms(histograms))
                .expect("a sorted map yields ascending, distinct timestamps and unique names")
        })
        .collect()
}

enum Value {
    Float(f64),
    Histogram(FloatHistogram),
}

/// A description's label set as the store keys it.
///
/// The parser keeps labels as written, possibly with a name repeated;
/// sorted by name, the last value wins. `""` values drop out because
/// `Series::new` drops them: keeping them here would key `x{env=""}`
/// apart from `x` and then have the two collide once built, which the
/// store rejects.
fn label_set(sd: &promql_parser::SeriesDescription) -> Vec<(String, String)> {
    let mut pairs: Vec<(&str, &str)> = sd
        .labels
        .iter()
        .map(|l| (l.name.as_str(), l.value.as_str()))
        .filter(|(_, v)| !v.is_empty())
        .collect();
    pairs.sort_by(|a, b| a.0.cmp(b.0));
    let mut labels: Vec<(String, String)> = Vec::with_capacity(pairs.len());
    for (name, value) in pairs {
        match labels.last_mut() {
            Some(last) if last.0 == name => last.1 = value.to_string(),
            _ => labels.push((name.to_string(), value.to_string())),
        }
    }
    labels
}

/// Which part of a classic histogram a float series is, and the label set
/// of the native histogram it converts into: upstream's
/// `GetHistogramMetricBaseName` and `GetHistogramMetricBase`.
#[derive(Clone, Copy)]
enum Part {
    Bucket(f64),
    Count,
    Sum,
}

fn classic_part(labels: &[(String, String)]) -> Option<(Vec<(String, String)>, Part)> {
    let get = |name: &str| {
        labels
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, v)| v.as_str())
    };
    let name = get("__name__")?;
    // The first suffix that cuts wins, in upstream's order.
    let (base, part) = if let Some(base) = name.strip_suffix("_bucket") {
        // `strconv.ParseFloat`; a bucket without a readable `le` is not one.
        let le = get("le")?.parse::<f64>().ok().filter(|le| !le.is_nan())?;
        (base, Part::Bucket(le))
    } else if let Some(base) = name.strip_suffix("_sum") {
        (base, Part::Sum)
    } else if let Some(base) = name.strip_suffix("_count") {
        (base, Part::Count)
    } else {
        return None;
    };
    let mut labels: Vec<(String, String)> = labels
        .iter()
        .filter(|(n, _)| n != "le" && n != "__name__")
        .cloned()
        .collect();
    labels.push(("__name__".to_string(), base.to_string()));
    labels.sort();
    Some((labels, part))
}

/// One classic histogram at one timestamp, collected from its float
/// series: upstream's `convertnhcb.TempHistogram`.
#[derive(Default)]
struct Classic {
    /// `(upper bound, cumulative count)`, in the order given.
    buckets: Vec<(f64, f64)>,
    count: Option<f64>,
    sum: f64,
}

impl Classic {
    fn set(&mut self, part: Part, v: f64) {
        match part {
            // A bound given twice keeps its first count, as upstream does.
            Part::Bucket(le) => {
                if !self.buckets.iter().any(|(bound, _)| *bound == le) {
                    self.buckets.push((le, v));
                }
            }
            Part::Count => self.count = Some(v),
            Part::Sum => self.sum = v,
        }
    }

    /// `TempHistogram.Convert`: the cumulative buckets as absolute counts
    /// under custom bounds, the last bucket `+Inf`. `None` where upstream
    /// returns an error: a negative or non-cumulative count, or a count
    /// that disagrees with the `+Inf` bucket.
    fn convert(self) -> Option<FloatHistogram> {
        let mut buckets = self.buckets;
        buckets.sort_by(|a, b| a.0.total_cmp(&b.0));
        if buckets.iter().any(|(_, count)| *count < 0.0)
            || buckets.windows(2).any(|w| w[1].1 < w[0].1)
            || self.count.is_some_and(|count| count < 0.0)
        {
            return None;
        }
        let count = self
            .count
            .or_else(|| buckets.last().map(|(_, count)| *count))
            .unwrap_or(0.0);
        if buckets.last().is_none_or(|(le, _)| *le != f64::INFINITY) {
            buckets.push((f64::INFINITY, count));
        }
        if buckets.last().is_some_and(|(_, last)| *last != count) {
            return None;
        }
        let mut previous = 0.0;
        let absolute: Vec<f64> = buckets
            .iter()
            .map(|(_, cumulative)| {
                let own = cumulative - previous;
                previous = *cumulative;
                own
            })
            .collect();
        let mut h = FloatHistogram {
            schema: promql_histogram::CUSTOM_SCHEMA,
            count,
            sum: self.sum,
            positive_spans: vec![promql_histogram::Span {
                offset: 0,
                length: absolute.len() as u32,
            }],
            positive_buckets: absolute,
            // The `+Inf` bound is implied, not stored.
            custom_values: buckets[..buckets.len() - 1]
                .iter()
                .map(|(le, _)| *le)
                .collect(),
            ..FloatHistogram::default()
        };
        h.compact();
        Some(h)
    }
}

/// Rewrite each histogram's counter reset hint to the one a TSDB read
/// returns for it.
///
/// Upstream's `load` appends to a real head, and what a query reads back
/// is the chunk's view of resets, not the hint the sample was written
/// with: a gauge histogram stays a gauge, the first sample of a chunk
/// reads as `unknown` whatever opened the chunk, and every later one as
/// `not_reset` (`counterResetHint` in `tsdb/chunkenc/histogram_meta.go`
/// at 83962c35). The corpus expects those hints, so the fixture has to
/// hold them; an engine reading a real store is handed them by it.
///
/// A sample opens a chunk where `FloatHistogramAppender.appendable`
/// (`tsdb/chunkenc/float_histogram.go`) refuses it the open one: after a
/// float or a gauge, on an explicit or a detected counter reset, and on a
/// change of schema, zero threshold or custom bounds. Chunks cut for
/// their size are not modelled.
fn stored_hints(float_times: &[i64], histograms: &mut [(i64, FloatHistogram)]) {
    let stale = |h: &FloatHistogram| h.sum.to_bits() == STALE_NAN_BITS;
    // The last histogram of the open chunk, `None` where the next sample
    // opens one.
    let mut open: Option<FloatHistogram> = None;
    let mut last_t = i64::MIN;
    for (t, h) in histograms.iter_mut() {
        // A float between two histograms is a chunk of another encoding.
        let float_between = float_times
            .binary_search(&last_t)
            .map_or_else(|i| i, |i| i + 1);
        if float_times.get(float_between).is_some_and(|ft| ft < t) {
            open = None;
        }
        last_t = *t;
        if h.counter_reset_hint == CounterResetHint::Gauge {
            // Gauge chunks hold only gauges, so the next counter opens one.
            open = None;
            continue;
        }
        let joins = match &open {
            None => false,
            Some(_) if h.counter_reset_hint == CounterResetHint::Reset => false,
            // A stale sample joins the chunk; nothing joins after one.
            Some(_) if stale(h) => true,
            Some(prev) if stale(prev) => false,
            Some(prev) => {
                // What remains of `appendable` once the layouts agree is
                // the zero bucket and each bucket against its predecessor,
                // which `DetectReset` does when no hint decides.
                let mut unhinted = h.clone();
                unhinted.counter_reset_hint = CounterResetHint::Unknown;
                h.count >= prev.count
                    && h.schema == prev.schema
                    && h.zero_threshold == prev.zero_threshold
                    && (!h.uses_custom_buckets() || h.custom_values == prev.custom_values)
                    && !unhinted.detect_reset(prev)
            }
        };
        h.counter_reset_hint = match joins {
            true => CounterResetHint::NotReset,
            false => CounterResetHint::Unknown,
        };
        open = Some(h.clone());
    }
}

const STALE_NAN_BITS: u64 = 0x7ff0_0000_0000_0002;

impl DataFusionEngine {
    /// A store that cannot be built is a broken fixture, not a verdict
    /// on the query, so it stays an Other rather than an answer.
    fn store(&self, load: &[LoadedSeries<'_>]) -> Result<MemorySeriesSource, EngineError> {
        let source = MemorySeriesSource::try_new(merge(load))
            .map_err(|e| EngineError::Other(format!("seeding the store: {e}")))?;
        Ok(self.source.apply(source))
    }
}

/// How an engine failure reads to the harness: an invalid query is an
/// answer, an unsupported feature is "not built yet", anything else is a
/// bug.
fn failure(error: promql_engine::EngineError) -> Result<QueryResult, EngineError> {
    match error {
        promql_engine::EngineError::Unsupported(feature) => Err(EngineError::Unsupported(feature)),
        // The text is what the corpus's expected errors are matched on.
        e if e.is_user_error() => Ok(QueryResult::Error(e.to_string())),
        other => Err(EngineError::Other(other.to_string())),
    }
}

/// Upstream's matrix has one series per label set; the engine's rows have
/// one per label set and block.
fn series_of(
    decoded: Result<Vec<promql_engine::Series>, promql_engine::SeriesError>,
) -> Result<Vec<Series>, EngineError> {
    let decoded = decoded
        .and_then(coalesce)
        .map_err(|e| EngineError::Other(format!("decoding: {e}")))?;
    Ok(decoded
        .iter()
        .map(|s| Series {
            labels: s
                .labels()
                .map(|(k, v)| (k.to_owned(), v.to_owned()))
                .collect(),
            floats: s
                .timestamps()
                .iter()
                .zip(s.values())
                .map(|(&t, &v)| Point { t, v })
                .collect(),
            histograms: s
                .histograms()
                .into_iter()
                .map(|(t, h)| HPoint { t, h })
                .collect(),
        })
        .collect())
}

impl Engine for DataFusionEngine {
    fn instant_query(
        &self,
        load: &[LoadedSeries<'_>],
        query: &str,
        at_ms: i64,
    ) -> Result<QueryResult, EngineError> {
        let source = self.store(load)?;
        match self.inner.instant_query(&source, query, at_ms) {
            Ok(InstantResult::Vector(batches)) => {
                // `execEvalStmt` stamps every sample with the evaluation
                // time, whatever time the point it came from carried.
                let samples = series_of(decode(&batches))?
                    .into_iter()
                    .map(|s| match (s.floats.as_slice(), s.histograms.as_slice()) {
                        ([p], []) => Ok(Sample {
                            labels: s.labels,
                            t: at_ms,
                            v: p.v,
                            histogram: None,
                        }),
                        ([], [p]) => Ok(Sample {
                            labels: s.labels,
                            t: at_ms,
                            v: 0.0,
                            histogram: Some(p.h.clone()),
                        }),
                        (floats, histograms) => Err(EngineError::Other(format!(
                            "an instant vector series carried {} points",
                            floats.len() + histograms.len()
                        ))),
                    })
                    .collect::<Result<_, _>>()?;
                Ok(QueryResult::Vector(samples))
            }
            Ok(InstantResult::Matrix(batches)) => {
                Ok(QueryResult::Matrix(series_of(decode(&batches))?))
            }
            Ok(InstantResult::Scalar(v)) => Ok(QueryResult::Scalar { v, t: at_ms }),
            Ok(InstantResult::String(v)) => Ok(QueryResult::Str { v, t: at_ms }),
            Err(e) => failure(e),
        }
    }

    fn range_query(
        &self,
        load: &[LoadedSeries<'_>],
        query: &str,
        start_ms: i64,
        end_ms: i64,
        step_ms: i64,
    ) -> Result<QueryResult, EngineError> {
        let source = self.store(load)?;
        let range = RangeQuery::new(start_ms, end_ms, step_ms);
        match self.inner.range_query(&source, query, &range) {
            Ok(batches) => Ok(QueryResult::Matrix(series_of(decode(&batches))?)),
            Err(e) => failure(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use promql_parser::SeriesDescription;

    fn desc(line: &str) -> SeriesDescription {
        promql_parser::parse_series_desc(line).expect("parses")
    }

    fn find<'a>(stored: &'a [promql_engine::Series], name: &str) -> &'a promql_engine::Series {
        stored
            .iter()
            .find(|s| s.labels().any(|(k, v)| k == "__name__" && v == name))
            .unwrap_or_else(|| panic!("no series named {name}"))
    }

    /// The reason the trait takes a slice rather than one block: a
    /// script can load at several resolutions, and each block's samples
    /// are spaced by its own interval.
    #[test]
    fn each_block_keeps_its_own_interval() {
        let slow = vec![desc("slow 1 2 3")];
        let fast = vec![desc("fast 1 2 3")];
        let stored = merge(&[
            LoadedSeries {
                series: &slow,
                interval_secs: 60.0,
                with_nhcb: false,
            },
            LoadedSeries {
                series: &fast,
                interval_secs: 10.0,
                with_nhcb: false,
            },
        ]);

        assert_eq!(find(&stored, "slow").timestamps(), [0, 60_000, 120_000]);
        assert_eq!(find(&stored, "fast").timestamps(), [0, 10_000, 20_000]);
    }

    #[test]
    fn blocks_accumulate_rather_than_replace() {
        let first = vec![desc("a 1")];
        let second = vec![desc("b 2")];
        let stored = merge(&[
            LoadedSeries {
                series: &first,
                interval_secs: 60.0,
                with_nhcb: false,
            },
            LoadedSeries {
                series: &second,
                interval_secs: 60.0,
                with_nhcb: false,
            },
        ]);
        assert_eq!(stored.len(), 2);
    }

    /// Two blocks writing the same series at the same timestamp: the
    /// later one wins, as it would through an appender.
    #[test]
    fn a_later_block_overwrites_the_same_timestamp() {
        let first = vec![desc("a 1 1 1")];
        let second = vec![desc("a 9")];
        let stored = merge(&[
            LoadedSeries {
                series: &first,
                interval_secs: 60.0,
                with_nhcb: false,
            },
            LoadedSeries {
                series: &second,
                interval_secs: 60.0,
                with_nhcb: false,
            },
        ]);
        assert_eq!(stored.len(), 1);
        assert_eq!(find(&stored, "a").values(), [9.0, 1.0, 1.0]);
    }

    #[test]
    fn no_blocks_is_an_empty_store() {
        assert!(merge(&[]).is_empty());
    }

    fn nz(n: usize) -> NonZeroUsize {
        NonZeroUsize::new(n).expect("a count of at least one")
    }

    fn nz64(n: u64) -> NonZeroU64 {
        NonZeroU64::new(n).expect("a span of at least a millisecond")
    }

    #[test]
    fn source_modes_parse() {
        assert_eq!(SourceMode::parse("plain"), Ok(SourceMode::Plain));
        assert_eq!(
            SourceMode::parse("contiguous-u64"),
            Ok(SourceMode::Contiguous(SeriesIdEncoding::UInt64))
        );
        assert_eq!(
            SourceMode::parse("contiguous-binary"),
            Ok(SourceMode::Contiguous(SeriesIdEncoding::FixedSizeBinary))
        );
        assert_eq!(SourceMode::parse("chunked"), Ok(SourceMode::Chunked(0)));
        assert_eq!(
            SourceMode::parse("chunked=150000"),
            Ok(SourceMode::Chunked(150_000))
        );
        assert_eq!(
            SourceMode::parse("blocks"),
            Ok(SourceMode::Blocks(nz64(60_000)))
        );
        assert_eq!(
            SourceMode::parse("blocks=150000"),
            Ok(SourceMode::Blocks(nz64(150_000)))
        );
        assert_eq!(
            SourceMode::parse("partitions"),
            Ok(SourceMode::Partitions(nz(4)))
        );
        assert_eq!(
            SourceMode::parse("partitions=7"),
            Ok(SourceMode::Partitions(nz(7)))
        );
        for bad in [
            "",
            "chunky",
            "plain=1",
            "chunked=-1",
            "partitions=0",
            "partitions=x",
            "contiguous-u64=2",
            "contiguous-binary=0",
            "keyed-u64",
            "keyed-binary",
        ] {
            assert!(SourceMode::parse(bad).is_err(), "{bad:?}");
        }
    }

    /// The corpus is written for upstream's `TestParserOpts`: with a gate
    /// shut, its experimental evals fail at parse time and read as engine
    /// gaps, so none may be.
    #[test]
    fn the_harness_opens_all_four_parser_gates() {
        assert_eq!(engine_options().parser, promql_parser::ParserOptions::all());
        let engine = DataFusionEngine::with_source(SourceMode::Plain).unwrap();
        let load = [LoadedSeries {
            series: &[desc("x 1 2 3"), desc(r#"y 4 5 6"#)],
            interval_secs: 30.0,
            with_nhcb: false,
        }];
        // Parses under fill; the result itself is not the question.
        engine
            .range_query(&load, "x + fill(0) y", 0, 60_000, 30_000)
            .expect("fill parses");
    }

    /// Every mode answers a query over several series and blocks the way
    /// the plain store does.
    #[test]
    fn every_source_mode_answers_alike() {
        let a = vec![desc(r#"x{pod="a"} 1+1x19"#), desc(r#"x{pod="b"} 2+3x19"#)];
        let b = vec![desc(r#"x{pod="c"} 0+5x9"#)];
        let load = [
            LoadedSeries {
                series: &a,
                interval_secs: 30.0,
                with_nhcb: false,
            },
            LoadedSeries {
                series: &b,
                interval_secs: 60.0,
                with_nhcb: false,
            },
        ];
        let run = |mode| {
            DataFusionEngine::with_source(mode)
                .unwrap()
                .range_query(&load, "rate(x[5m])", 0, 600_000, 30_000)
                .unwrap()
        };
        let plain = run(SourceMode::Plain);
        for mode in [
            SourceMode::Chunked(0),
            SourceMode::Chunked(150_000),
            SourceMode::Blocks(nz64(30_000)),
            SourceMode::Blocks(nz64(200_000)),
            SourceMode::Partitions(nz(4)),
            SourceMode::Contiguous(SeriesIdEncoding::UInt64),
            SourceMode::Contiguous(SeriesIdEncoding::FixedSizeBinary),
        ] {
            assert_eq!(run(mode), plain, "{mode:?}");
        }
    }
}
