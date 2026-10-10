//! Executing a parsed `.test` script against an engine.
//!
//! A script is a sequence, not a set: `load` blocks accumulate into
//! storage, an `eval` reads whatever is there, `clear` wipes it. So this
//! walks the commands in order and hands each eval exactly the blocks
//! above it since the last clear — not every block in the file, which
//! would feed cases data that does not exist yet at that point.
//!
//! # Comparison
//!
//! Deliberately *not* [`crate::compare`]. That implements the
//! differential suite's rules, which are Go's
//! `cmp.EquateApprox(1e-10, 1e-6)`. promqltest uses `util/almost.Equal`:
//! a single relative epsilon of `1e-6`, and — the part that matters —
//! Prometheus's stale marker compares equal only to another stale
//! marker, never to an ordinary NaN. Two comparers because there are
//! genuinely two specifications; sharing one would mean quietly
//! applying the wrong tolerances to one of the suites.
//!
//! # Instant queries
//!
//! 1,801 of the 2,098 evals are instant, and the engine only offers a
//! range query. Issuing one with `start == end` yields exactly one point
//! per series, which is then reshaped into the vector or scalar the
//! script expects. The *script* says which — our own output cannot,
//! since a scalar comes back as a matrix with one empty-label series,
//! indistinguishable from what `sum(x)` returns. Anything other than
//! exactly one point means the emulation itself is wrong, and is
//! reported as a harness failure rather than passed to the comparer.

use std::collections::BTreeMap;

use promql_parser::SeriesDescription;

use super::script::{Command, Eval, Expected, LoadBlock, Range, Script, Timing, Unsupported};
use crate::result::{
    Engine, EngineError, Labels, LoadedSeries, Point, QueryResult, Sample, Series,
};

/// Relative tolerance, from upstream's `defaultEpsilon`.
pub const EPSILON: f64 = 0.000001;

/// Prometheus's stale marker: a NaN with a specific payload.
pub fn is_stale_nan(v: f64) -> bool {
    v.to_bits() == 0x7ff0_0000_0000_0002
}

/// Upstream's `util/almost.Equal`.
///
/// Note the two NaN rules, which are not the same rule: a stale marker
/// equals only another stale marker, while any two ordinary NaNs are
/// equal to each other. Collapsing them would let a genuine NaN satisfy
/// an assertion about staleness.
pub fn almost_equal(a: f64, b: f64, epsilon: f64) -> bool {
    if is_stale_nan(a) || is_stale_nan(b) {
        return is_stale_nan(a) && is_stale_nan(b);
    }
    if a.is_nan() && b.is_nan() {
        return true;
    }
    if a == b {
        return true;
    }
    let abs_sum = a.abs() + b.abs();
    let diff = (a - b).abs();
    // Near zero, or when both are denormal, the relative form is
    // meaningless; fall back to an absolute comparison scaled the same
    // way Go scales it.
    if a == 0.0 || b == 0.0 || abs_sum < f64::MIN_POSITIVE {
        return diff < epsilon * f64::MIN_POSITIVE;
    }
    diff / abs_sum.min(f64::MAX) < epsilon
}

/// What happened to one eval.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    Pass,
    /// The engine and the script disagree. This is the interesting one.
    Fail(String),
    /// The engine has not implemented something the query needs. Named
    /// so the suite can count cases per missing feature.
    Unsupported(String),
    /// Nothing to hold the engine to — an unreadable load block or
    /// expected row, which today means native histograms.
    Skipped(String),
    /// The query is experimental syntax: stock Prometheus refuses it unless
    /// the named `--enable-feature` values are on. Not part of the core
    /// language yet, so it is left out of the totals, the supported list and
    /// the trials. It still runs, with every gate open, and `verdict` is how
    /// that went, so the report can say how far each flag's syntax is along
    /// without a passing eval ever reaching the allowlist.
    Gated {
        flags: String,
        verdict: Box<Verdict>,
    },
}

impl Verdict {
    pub fn is_pass(&self) -> bool {
        matches!(self, Verdict::Pass)
    }
}

/// One eval and how it went.
#[derive(Debug, Clone)]
pub struct Outcome {
    /// The `.test` file's stem, e.g. `operators`.
    pub file: String,
    /// Identifies the eval within its file, and stays stable across an
    /// upstream re-pin: the query and the evaluation time, plus an
    /// ordinal only when a file repeats the identical query at the
    /// identical time. Line numbers deliberately play no part — comments
    /// reflow, and a baseline keyed on them would invalidate wholesale
    /// on every bump.
    pub id: String,
    /// 1-based line of the `eval`, for humans reading a failure.
    pub line: usize,
    pub query: String,
    pub verdict: Verdict,
    /// How many rows the script expects. Zero means the assertion is
    /// "this returns nothing", which passes against an engine that
    /// returns nothing for any reason — worth counting separately so the
    /// pass total cannot be read as more coverage than it is.
    pub expected_rows: usize,
    /// Whether the eval carried `expect warn`/`info` assertions that
    /// were parsed but not checked.
    pub unchecked_annotations: bool,
    /// The eval is about native histograms: an expected row is one, or a
    /// selector in the query matches a series an active load block loads
    /// as one. Matched by label matchers, not by whether a histogram sits
    /// anywhere in the block: `operators.test` seeds one histogram series
    /// next to the floats 165 evals read, and filing those under
    /// histograms hid real float gaps.
    pub histograms: bool,
    /// The query contains a subquery. Counted for passing evals too, so
    /// the report's denominator does not move when one starts passing.
    pub subquery: bool,
}

/// One `load` block's data, as an eval below it sees it.
struct Block {
    series: Vec<SeriesDescription>,
    interval_secs: f64,
    /// Metric names of series lines in this block that did not parse —
    /// native-histogram descriptors, today. `None` for a line whose name
    /// could not be recovered at all.
    ///
    /// An eval reading one of these is being asked a different question
    /// than the script wrote, because the data it needs is missing. But
    /// most are not: `operators.test` seeds one histogram series in a
    /// block that 165 float evals also read from, and skipping all of
    /// them would throw away most of the file.
    missing: Vec<Option<String>>,
    /// Label sets of the series this block loads with native histogram
    /// samples. `None` for a line whose labels could not be read, which
    /// any selector is taken to match.
    histograms: Vec<Option<Labels>>,
}

/// Whether a query could be reading one of the series we failed to load.
///
/// A deliberate over-approximation, and a textual one: it answers "might
/// this touch it?", never "does it?". Two ways to say yes — the query
/// names the metric, or it uses a selector with no metric name at all
/// (`max({job="api-server"})`), which can match anything. A name we
/// could not recover also counts, since there is nothing to compare.
///
/// Over-approximating is the safe direction. A false yes costs one
/// skipped eval; a false no reports a missing-data failure as though the
/// engine were wrong, and step 7 would then bake that into the baseline.
fn might_read(query: &str, missing: &[Option<String>]) -> bool {
    if missing.is_empty() {
        return false;
    }
    if missing.iter().any(Option::is_none) {
        return true;
    }
    if missing
        .iter()
        .flatten()
        .any(|name| query.contains(name.as_str()))
    {
        return true;
    }
    has_nameless_selector(query)
}

fn has_nameless_selector(query: &str) -> bool {
    let bytes = query.as_bytes();
    bytes.iter().enumerate().any(|(i, &b)| {
        b == b'{'
            && !bytes[..i]
                .iter()
                .rev()
                .find(|c| !c.is_ascii_whitespace())
                .is_some_and(|c| c.is_ascii_alphanumeric() || *c == b'_' || *c == b':')
    })
}

/// The metric name a series line declares, from either spelling:
/// `foo{bar="baz"} …` or `{__name__="foo", bar="baz"} …`.
fn metric_name(line: &str) -> Option<String> {
    let line = line.trim();
    if let Some(rest) = line.strip_prefix('{') {
        let at = rest.find("__name__")?;
        let eq = rest[at..].find('=')? + at;
        let open = rest[eq..].find('"')? + eq + 1;
        let close = rest[open..].find('"')? + open;
        return Some(rest[open..close].to_string());
    }
    let name: String = line
        .chars()
        .take_while(|c| c.is_ascii_alphanumeric() || *c == '_' || *c == ':')
        .collect();
    (!name.is_empty()).then_some(name)
}

/// Run every eval in a script, in source order.
pub fn run_script(engine: &dyn Engine, script: &Script) -> Vec<Outcome> {
    let mut outcomes = Vec::new();
    // Blocks visible to the next eval: those above it, since the last
    // `clear`. Not every block in the file.
    let mut active: Vec<Block> = Vec::new();
    // Disambiguates a file that asks the identical question twice.
    let mut seen: BTreeMap<String, usize> = BTreeMap::new();

    for command in &script.commands {
        match command {
            Command::Clear => active.clear(),
            Command::Load(load) => active.push(Block {
                series: load.block.parsed().cloned().collect(),
                interval_secs: load.block.interval_secs,
                missing: load
                    .block
                    .unsupported()
                    .map(|u| metric_name(&u.text))
                    .collect(),
                histograms: histogram_series(&load.block),
            }),
            Command::Eval(eval) => {
                let key = case_key(eval);
                let n = seen.entry(key.clone()).or_insert(0);
                *n += 1;
                let id = if *n > 1 { format!("{key} #{n}") } else { key };
                outcomes.push(Outcome {
                    file: script.name.clone(),
                    id,
                    line: eval.line,
                    query: eval.query.clone(),
                    verdict: run_eval(engine, &script.name, eval, &active),
                    expected_rows: match &eval.expected {
                        Expected::Series(rows) => rows.len(),
                        Expected::Scalar(_) | Expected::Str(_) => 1,
                        Expected::Nothing => 0,
                    },
                    unchecked_annotations: eval.expect.has_annotation_assertions(),
                    histograms: has_histograms(eval, &active),
                    subquery: has_subquery(&eval.query),
                });
            }
        }
    }

    outcomes
}

fn has_histograms(eval: &Eval, active: &[Block]) -> bool {
    eval.unsupported
        .iter()
        .any(|u| u.reason == Unsupported::NativeHistogram)
        || selects_histogram(&eval.query, active)
}

/// The series a block holds as native histograms, as upstream's `loadCmd`
/// would store them: every `{{…}}` line, and under `load_with_nhcb` the
/// series `appendCustomHistogram` derives from classic `_bucket`, `_sum`
/// and `_count` lines. The derived ones are floats in the text, so missing
/// them would file `histogram_quantile(0.5, foo)` as a core failure.
fn histogram_series(block: &LoadBlock) -> Vec<Option<Labels>> {
    let mut out: Vec<Option<Labels>> = block
        .unsupported()
        .filter(|u| u.reason == Unsupported::NativeHistogram)
        .map(|u| series_labels(&u.text))
        .collect();
    if block.with_nhcb {
        for desc in block.parsed() {
            let mut labels = labels_of(desc);
            let Some(name) = labels.get(METRIC_NAME) else {
                continue;
            };
            // `GetHistogramMetricBaseName`: first suffix that cuts wins.
            let Some(base) = ["_bucket", "_sum", "_count"]
                .iter()
                .find_map(|s| name.strip_suffix(s))
            else {
                continue;
            };
            let base = base.to_string();
            labels.insert(METRIC_NAME.to_string(), base);
            labels.remove("le");
            if !out.contains(&Some(labels.clone())) {
                out.push(Some(labels));
            }
        }
    }
    out
}

const METRIC_NAME: &str = "__name__";

/// The labels of a series line the series grammar rejects for its values.
/// The label part alone, with a stand-in value, is a line it accepts, so
/// quoting and `{__name__="…"}` spellings are read by the real grammar.
fn series_labels(line: &str) -> Option<Labels> {
    let line = line.trim();
    let mut end = line
        .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_' || c == ':'))
        .unwrap_or(line.len());
    if line[end..].starts_with('{') && !line[end..].starts_with("{{") {
        let mut quoted = false;
        let mut escaped = false;
        let close = line[end..].char_indices().find(|&(_, c)| {
            match (quoted, escaped, c) {
                (true, true, _) => escaped = false,
                (true, false, '\\') => escaped = true,
                (_, false, '"') => quoted = !quoted,
                (false, _, '}') => return true,
                _ => {}
            }
            false
        })?;
        end += close.0 + 1;
    }
    let desc = promql_parser::parse_series_desc(&format!("{} 1", &line[..end])).ok()?;
    Some(labels_of(&desc))
}

/// Whether a selector in `query` matches a series an active block loads as
/// a native histogram, by Prometheus's matcher rules: a label the series
/// lacks compares as `""`, and a regex matches the whole value.
fn selects_histogram(query: &str, active: &[Block]) -> bool {
    use promql_engine::CompiledMatcher;
    use promql_parser::ast::{LabelMatcher, MatchOp, VectorSelector};
    use promql_parser::{Expr, ParserOptions};

    let series: Vec<&Option<Labels>> = active.iter().flat_map(|b| &b.histograms).collect();
    if series.is_empty() {
        return false;
    }
    fn selectors<'a>(e: &'a Expr, out: &mut Vec<&'a VectorSelector>) {
        match e {
            Expr::VectorSelector(vs) => out.push(vs),
            Expr::Binary(b) => {
                selectors(&b.lhs, out);
                selectors(&b.rhs, out);
            }
            Expr::Aggregate(a) => {
                if let Some(p) = a.param.as_deref() {
                    selectors(p, out);
                }
                selectors(&a.expr, out);
            }
            Expr::Call(c) => c.args.iter().for_each(|a| selectors(a, out)),
            Expr::MatrixSelector(m) => selectors(&m.vector_selector, out),
            Expr::Subquery(s) => selectors(&s.expr, out),
            Expr::Paren(p) => selectors(&p.expr, out),
            Expr::Unary(u) => selectors(&u.expr, out),
            Expr::StepInvariant(e) => selectors(e, out),
            Expr::NumberLiteral(_) | Expr::StringLiteral(_) | Expr::Duration(_) => {}
        }
    }
    let Ok(expr) = promql_parser::Parser::new(ParserOptions::all()).parse_expr(query) else {
        // No selectors to match, so fall back to the names it mentions.
        // The histogram trim operators `</` and `>/` do not parse yet, and
        // without this their evals would read as core failures.
        let names: Vec<&str> = series
            .iter()
            .filter_map(|l| l.as_ref()?.get(METRIC_NAME).map(String::as_str))
            .collect();
        return series.iter().any(|l| l.is_none())
            || query
                .split(|c: char| !(c.is_ascii_alphanumeric() || c == '_' || c == ':'))
                .any(|word| names.contains(&word));
    };
    let mut found = Vec::new();
    selectors(&expr, &mut found);
    found.iter().any(|vs| {
        // The parser keeps a bare name out of the matchers; upstream folds
        // it into a `__name__` one, and so must the match.
        let name = (!vs.name.is_empty()).then(|| LabelMatcher {
            name: METRIC_NAME.to_string(),
            op: MatchOp::Equal,
            value: vs.name.clone(),
            pos_range: Default::default(),
        });
        let Ok(matchers) = vs
            .label_matchers
            .iter()
            .chain(&name)
            .map(|m| CompiledMatcher::compile(m).map(|c| (m.name.as_str(), c)))
            .collect::<Result<Vec<_>, _>>()
        else {
            return false;
        };
        series.iter().any(|labels| {
            let Some(labels) = labels else { return true };
            matchers
                .iter()
                .all(|(name, m)| m.matches(labels.get(*name).map_or("", String::as_str)))
        })
    })
}

/// Parsed with every gate open, so an experimental-syntax query is still
/// asked the question; a query nobody parses has no subquery to report.
fn has_subquery(query: &str) -> bool {
    use promql_parser::{Expr, ParserOptions};
    fn walk(e: &Expr) -> bool {
        match e {
            Expr::Subquery(_) => true,
            Expr::Binary(b) => walk(&b.lhs) || walk(&b.rhs),
            Expr::Aggregate(a) => a.param.as_deref().is_some_and(walk) || walk(&a.expr),
            Expr::Call(c) => c.args.iter().any(walk),
            Expr::MatrixSelector(m) => walk(&m.vector_selector),
            Expr::Paren(p) => walk(&p.expr),
            Expr::Unary(u) => walk(&u.expr),
            Expr::StepInvariant(e) => walk(e),
            Expr::VectorSelector(_)
            | Expr::NumberLiteral(_)
            | Expr::StringLiteral(_)
            | Expr::Duration(_) => false,
        }
    }
    promql_parser::Parser::new(ParserOptions::all())
        .parse_expr(query)
        .is_ok_and(|e| walk(&e))
}

fn case_key(eval: &Eval) -> String {
    let query = eval.query.split_whitespace().collect::<Vec<_>>().join(" ");
    match eval.timing {
        Timing::Instant { at_ms } => format!("{query} @ {at_ms}"),
        Timing::Range(r) => format!("{query} @ {}..{}/{}", r.start_ms, r.end_ms, r.step_ms),
    }
}

/// The `--enable-feature` values `query` needs, comma-separated, or `None`
/// when stock Prometheus parses it (or nobody does, which is the engine's
/// or the script's business, not a gate's).
///
/// Read off the stock parse's errors, each of which names the gate that
/// raised it. The parse with every gate open is no witness: `step()` and
/// its siblings are gated but not ported, so they fail there too, and
/// asking it first filed them as core failures.
fn gating_flags(file: &str, query: &str) -> Option<String> {
    use promql_parser::FEATURE_FLAGS;
    let Err(stock) = promql_parser::parse_expr(query) else {
        return None;
    };
    let needed: Vec<&str> = FEATURE_FLAGS
        .iter()
        .map(|f| f.flag)
        .filter(|&flag| stock.iter().any(|e| e.gate == Some(flag)))
        .collect();
    if !needed.is_empty() {
        return Some(needed.join(", "));
    }
    let parses = promql_parser::Parser::new(promql_parser::ParserOptions::all())
        .parse_expr(query)
        .is_ok();
    GATED_BY_FILE
        .iter()
        .find(|(f, _)| !parses && *f == file)
        .map(|(_, flag)| flag.to_string())
}

/// Files whose experimental syntax has no productions yet, with the flag
/// upstream needs to parse it. Without a production nothing reaches the
/// gate, so the stock parse fails with a bare syntax error and only the
/// file says which flag it is about. Applied to a query no option set
/// parses, so the stable queries such a file compares against stay core.
/// An entry goes when its productions are ported and the gate names
/// itself.
const GATED_BY_FILE: &[(&str, &str)] = &[("duration_expression", "promql-duration-expr")];

fn run_eval(engine: &dyn Engine, file: &str, eval: &Eval, active: &[Block]) -> Verdict {
    let verdict = evaluate(engine, eval, active);
    match gating_flags(file, &eval.query) {
        Some(flags) => Verdict::Gated {
            flags,
            verdict: Box::new(verdict),
        },
        None => verdict,
    }
}

fn evaluate(engine: &dyn Engine, eval: &Eval, active: &[Block]) -> Verdict {
    if !eval.is_supported() {
        return Verdict::Skipped(format!(
            "{} expected row(s) we cannot read: {}",
            eval.unsupported.len(),
            eval.unsupported[0].text
        ));
    }
    if active.iter().any(|b| might_read(&eval.query, &b.missing)) {
        return Verdict::Skipped(
            "a series it may read did not load (native histogram)".to_string(),
        );
    }

    let load: Vec<LoadedSeries<'_>> = active
        .iter()
        .filter(|b| !b.series.is_empty())
        .map(|b| LoadedSeries {
            series: &b.series,
            interval_secs: b.interval_secs,
        })
        .collect();

    // An instant query is a range of one step. `Grid::new` rejects a
    // non-positive step, so the step is nominal rather than zero.
    let (start_ms, end_ms, step_ms) = match eval.timing {
        Timing::Instant { at_ms } => (at_ms, at_ms, 1),
        Timing::Range(r) => (r.start_ms, r.end_ms, r.step_ms),
    };

    if matches!(eval.timing, Timing::Instant { .. }) {
        if let Ok(Some(v)) = engine.instant_string(&eval.query) {
            return match verify(eval, QueryResult::Str { v, t: start_ms }) {
                Ok(()) => Verdict::Pass,
                Err(detail) => Verdict::Fail(detail),
            };
        }
    }
    let actual = match engine.range_query(&load, &eval.query, start_ms, end_ms, step_ms) {
        Ok(r) => r,
        Err(EngineError::Unsupported(feature)) => return Verdict::Unsupported(feature),
        Err(e) => return Verdict::Fail(e.to_string()),
    };

    match verify(eval, actual) {
        Ok(()) => Verdict::Pass,
        Err(detail) => Verdict::Fail(detail),
    }
}

/// The shape the script says to expect. Drives normalisation, because
/// our own output cannot distinguish a scalar from a one-series vector.
enum Shape {
    Vector,
    Matrix(Range),
    Scalar,
    Str,
}

fn shape(eval: &Eval) -> Shape {
    match (&eval.expected, eval.timing, eval.range_vector) {
        (Expected::Str(_), _, _) => Shape::Str,
        (Expected::Scalar(_), _, _) => Shape::Scalar,
        (_, _, Some(r)) => Shape::Matrix(r),
        (_, Timing::Range(r), _) => Shape::Matrix(r),
        (_, Timing::Instant { .. }, None) => Shape::Vector,
    }
}

fn verify(eval: &Eval, actual: QueryResult) -> Result<(), String> {
    // `expect fail` is checked before anything else: a failing query has
    // no result to compare.
    if eval.expect.expects_failure() {
        return match actual {
            QueryResult::Error(_) => Ok(()),
            other => Err(format!(
                "expected the query to fail, got {}",
                describe(&other)
            )),
        };
    }
    if let QueryResult::Error(msg) = &actual {
        return Err(format!("query failed: {msg}"));
    }

    let at_ms = match eval.timing {
        Timing::Instant { at_ms } => at_ms,
        Timing::Range(r) => r.start_ms,
    };

    match shape(eval) {
        Shape::Str => {
            let Expected::Str(want) = &eval.expected else {
                unreachable!("shape() returns Str only for Expected::Str")
            };
            match actual {
                QueryResult::Str { v, .. } if &v == want => Ok(()),
                other => Err(format!(
                    "expected string {want:?}, got {}",
                    describe(&other)
                )),
            }
        }
        Shape::Scalar => {
            let Expected::Scalar(want) = eval.expected else {
                unreachable!("shape() returns Scalar only for Expected::Scalar")
            };
            match normalise_scalar(actual) {
                Ok(got) if almost_equal(want, got, EPSILON) => Ok(()),
                Ok(got) => Err(format!("expected scalar {want}, got {got}")),
                Err(e) => Err(e),
            }
        }
        Shape::Vector => {
            let want = expected_rows(eval, at_ms, 0);
            let got = normalise_vector(actual)?;
            compare_vector(&want, &got, eval.expect.ordered)
        }
        Shape::Matrix(r) => {
            let want = expected_rows(eval, r.start_ms, r.step_ms);
            match actual {
                QueryResult::Matrix(series) => compare_matrix(&want, &series),
                other => Err(format!("expected a matrix, got {}", describe(&other))),
            }
        }
    }
}

/// One expected series, already resolved to timestamps.
struct Expected1 {
    labels: Labels,
    points: Vec<Point>,
    pos: usize,
}

fn expected_rows(eval: &Eval, start_ms: i64, step_ms: i64) -> Vec<Expected1> {
    let Expected::Series(rows) = &eval.expected else {
        return Vec::new();
    };
    rows.iter()
        .map(|row| Expected1 {
            labels: labels_of(&row.desc),
            points: row
                .desc
                .values
                .iter()
                .enumerate()
                .filter(|(_, v)| !v.omitted)
                .map(|(i, v)| Point {
                    t: start_ms + i as i64 * step_ms,
                    v: v.value,
                })
                .collect(),
            pos: row.pos,
        })
        .collect()
}

fn labels_of(desc: &SeriesDescription) -> Labels {
    desc.labels
        .iter()
        .map(|l| (l.name.clone(), l.value.clone()))
        .collect()
}

/// A range query always answers with a matrix. Reshape it into the
/// vector an instant eval expects, refusing to guess when the shape is
/// not what a single-step query should have produced.
fn normalise_vector(actual: QueryResult) -> Result<Vec<Sample>, String> {
    let series = match actual {
        QueryResult::Matrix(series) => series,
        other => {
            return Err(format!(
                "expected a matrix to reshape, got {}",
                describe(&other)
            ))
        }
    };
    let mut out = Vec::with_capacity(series.len());
    for s in series {
        match s.floats.len() {
            // A series with no point at the eval time simply is not in
            // the instant result.
            0 => continue,
            1 => out.push(Sample {
                labels: s.labels,
                t: s.floats[0].t,
                v: s.floats[0].v,
                histogram: false,
            }),
            n => {
                return Err(format!(
                    "HARNESS FAILURE (not an engine failure): a single-step query \
                     returned {n} points for one series"
                ))
            }
        }
    }
    Ok(out)
}

fn normalise_scalar(actual: QueryResult) -> Result<f64, String> {
    match actual {
        QueryResult::Scalar { v, .. } => Ok(v),
        QueryResult::Matrix(series) => match series.as_slice() {
            [s] if s.floats.len() == 1 => Ok(s.floats[0].v),
            [] => Err("expected a scalar, got an empty result".to_string()),
            _ => Err(format!("expected a scalar, got {} series", series.len())),
        },
        other => Err(format!("expected a scalar, got {}", describe(&other))),
    }
}

fn compare_vector(want: &[Expected1], got: &[Sample], ordered: bool) -> Result<(), String> {
    if want.len() != got.len() {
        return Err(format!(
            "expected {} sample(s), got {}{}",
            want.len(),
            got.len(),
            unexpected_labels(
                want.iter().map(|e| &e.labels),
                got.iter().map(|s| &s.labels)
            ),
        ));
    }

    if ordered {
        // `ordered` is the one case where output order is the
        // assertion, so it cannot go through the label-keyed path
        // below: sorting first would make the check vacuous.
        let mut want: Vec<&Expected1> = want.iter().collect();
        want.sort_by_key(|e| e.pos);
        for (i, (w, g)) in want.iter().zip(got).enumerate() {
            if w.labels != g.labels {
                return Err(format!(
                    "position {}: expected {:?}, got {:?}",
                    i + 1,
                    w.labels,
                    g.labels
                ));
            }
            let wv = w.points.first().map(|p| p.v).unwrap_or(f64::NAN);
            if !almost_equal(wv, g.v, EPSILON) {
                return Err(format!("position {}: expected {wv}, got {}", i + 1, g.v));
            }
        }
        return Ok(());
    }

    let by_labels: BTreeMap<&Labels, &Sample> = got.iter().map(|s| (&s.labels, s)).collect();
    for w in want {
        let Some(g) = by_labels.get(&w.labels) else {
            return Err(format!("expected metric {:?} not in the result", w.labels));
        };
        let wv = w.points.first().map(|p| p.v).unwrap_or(f64::NAN);
        if !almost_equal(wv, g.v, EPSILON) {
            return Err(format!("{:?}: expected {wv}, got {}", w.labels, g.v));
        }
    }
    Ok(())
}

fn compare_matrix(want: &[Expected1], got: &[Series]) -> Result<(), String> {
    if want.len() != got.len() {
        return Err(format!(
            "expected {} series, got {}{}",
            want.len(),
            got.len(),
            unexpected_labels(
                want.iter().map(|e| &e.labels),
                got.iter().map(|s| &s.labels)
            ),
        ));
    }

    let by_labels: BTreeMap<&Labels, &Series> = got.iter().map(|s| (&s.labels, s)).collect();
    for w in want {
        let Some(g) = by_labels.get(&w.labels) else {
            return Err(format!("expected metric {:?} not in the result", w.labels));
        };
        if w.points.len() != g.floats.len() {
            return Err(format!(
                "{:?}: expected {} point(s), got {}",
                w.labels,
                w.points.len(),
                g.floats.len()
            ));
        }
        for (wp, gp) in w.points.iter().zip(&g.floats) {
            if wp.t != gp.t {
                return Err(format!(
                    "{:?}: expected a point at {}, got one at {}",
                    w.labels, wp.t, gp.t
                ));
            }
            if !almost_equal(wp.v, gp.v, EPSILON) {
                return Err(format!(
                    "{:?} @{}: expected {}, got {}",
                    w.labels, wp.t, wp.v, gp.v
                ));
            }
        }
    }
    Ok(())
}

/// Name the label sets that differ, so a count mismatch says which
/// series is missing rather than only how many.
fn unexpected_labels<'a>(
    want: impl Iterator<Item = &'a Labels>,
    got: impl Iterator<Item = &'a Labels>,
) -> String {
    let want: Vec<&Labels> = want.collect();
    let got: Vec<&Labels> = got.collect();
    let missing: Vec<_> = want.iter().filter(|l| !got.contains(l)).collect();
    let extra: Vec<_> = got.iter().filter(|l| !want.contains(l)).collect();
    let mut out = String::new();
    if !missing.is_empty() {
        out.push_str(&format!("\n  missing: {missing:?}"));
    }
    if !extra.is_empty() {
        out.push_str(&format!("\n  unexpected: {extra:?}"));
    }
    out
}

fn describe(result: &QueryResult) -> String {
    match result {
        QueryResult::Matrix(series) => {
            let samples: usize = series.iter().map(|s| s.floats.len()).sum();
            format!("a matrix of {} series, {samples} samples", series.len())
        }
        QueryResult::Vector(samples) => format!("a vector of {} samples", samples.len()),
        QueryResult::Scalar { v, .. } => format!("scalar {v}"),
        QueryResult::Str { v, .. } => format!("string {v:?}"),
        QueryResult::Error(e) => format!("error: {e}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stale_and_ordinary_nan_are_different_things() {
        let stale = f64::from_bits(0x7ff0_0000_0000_0002);
        assert!(almost_equal(stale, stale, EPSILON));
        assert!(almost_equal(f64::NAN, f64::NAN, EPSILON));
        assert!(!almost_equal(stale, f64::NAN, EPSILON));
        assert!(!almost_equal(f64::NAN, stale, EPSILON));
    }

    /// Note the denominator is the *sum* of the magnitudes, not either
    /// one, so the tolerance on the difference is ~2ε rather than ε.
    /// That is upstream's formula, quirk included.
    #[test]
    fn tolerance_is_relative() {
        assert!(almost_equal(1e9, 1e9 + 1.0, EPSILON));
        assert!(almost_equal(1.0, 1.0000000001, EPSILON));
        assert!(almost_equal(1.0, 1.000001, EPSILON));
        assert!(!almost_equal(1.0, 1.00001, EPSILON));
    }

    #[test]
    fn infinities_compare_by_identity() {
        assert!(almost_equal(f64::INFINITY, f64::INFINITY, EPSILON));
        assert!(!almost_equal(f64::INFINITY, f64::NEG_INFINITY, EPSILON));
        assert!(!almost_equal(f64::INFINITY, 1e308, EPSILON));
    }

    #[test]
    fn zero_uses_the_absolute_fallback() {
        assert!(almost_equal(0.0, 0.0, EPSILON));
        assert!(!almost_equal(0.0, 1e-30, EPSILON));
    }

    #[test]
    fn a_metric_name_is_read_from_either_spelling() {
        assert_eq!(metric_name("foo{a=\"b\"} 1 2 3").as_deref(), Some("foo"));
        assert_eq!(metric_name("  foo 1 2 3").as_deref(), Some("foo"));
        assert_eq!(
            metric_name("{__name__=\"testhistogram\", id=\"1\"} {{schema:-53}}").as_deref(),
            Some("testhistogram")
        );
        assert_eq!(metric_name("{a=\"b\"} 1").as_deref(), None);
    }

    /// The case that motivates the whole heuristic: `operators.test`
    /// loads one histogram series into a block that 165 float evals also
    /// read. Poisoning the block wholesale would skip all of them.
    #[test]
    fn a_query_naming_other_metrics_is_not_poisoned() {
        let missing = vec![Some("http_requests_histogram".to_string())];
        assert!(!might_read("sum(http_requests) by (job)", &missing));
        assert!(might_read("sum(http_requests_histogram)", &missing));
    }

    #[test]
    fn a_selector_without_a_metric_name_matches_anything() {
        let missing = vec![Some("http_requests_histogram".to_string())];
        assert!(might_read("max({job=\"api-server\"})", &missing));
        assert!(might_read("count({__name__=~\".+\"})", &missing));
    }

    #[test]
    fn a_named_selector_is_not_mistaken_for_a_nameless_one() {
        assert!(!has_nameless_selector("http_requests{job=\"x\"}"));
        assert!(!has_nameless_selector("rate(foo{a=\"b\"}[5m])"));
        // A space between name and braces is still a named selector.
        assert!(!has_nameless_selector("http_requests {job=\"x\"}"));
        assert!(!has_nameless_selector("sum by (job) (foo)"));
        assert!(!has_nameless_selector("foo[5m:1m]"));
        assert!(has_nameless_selector("{job=\"x\"}"));
        assert!(has_nameless_selector("sum({job=\"x\"})"));
    }

    #[test]
    fn a_block_that_loaded_cleanly_poisons_nothing() {
        assert!(!might_read("{job=\"x\"}", &[]));
    }

    /// A name we could not recover means we cannot reason about it, so
    /// everything downstream is suspect.
    #[test]
    fn an_unrecoverable_name_poisons_conservatively() {
        assert!(might_read("sum(unrelated)", &[None]));
    }

    // ---------------- the runner ----------------
    //
    // These go through `run_script` on hand-written snippets rather than
    // calling the comparison helpers directly, because the parts most
    // likely to be wrong are the ones joining them up: which load blocks
    // an eval sees, and which comparison a script's shape selects.
    //
    // They also cover paths the corpus cannot reach yet. All 24 `expect
    // ordered` cases are `topk`/`sort`, which the engine does not
    // implement, so the ordered comparison would otherwise ship with no
    // test at all and start gating results the day sorting lands.

    use std::cell::RefCell;

    use crate::result::EngineError;

    use super::super::script::parse;

    /// An engine that answers from a closure, so a test can state the
    /// engine's reply and check only what the runner does with it.
    struct Stub<F>(F);

    impl<F: Fn(&str) -> Result<QueryResult, EngineError>> Engine for Stub<F> {
        fn range_query(
            &self,
            _load: &[LoadedSeries<'_>],
            query: &str,
            _start_ms: i64,
            _end_ms: i64,
            _step_ms: i64,
        ) -> Result<QueryResult, EngineError> {
            (self.0)(query)
        }
    }

    /// A label set and its points, as a test writes them inline.
    type Row<'a> = (&'a [(&'a str, &'a str)], &'a [(i64, f64)]);

    fn matrix(rows: &[Row<'_>]) -> QueryResult {
        QueryResult::Matrix(
            rows.iter()
                .map(|(labels, points)| Series {
                    labels: labels
                        .iter()
                        .map(|(k, v)| (k.to_string(), v.to_string()))
                        .collect(),
                    floats: points.iter().map(|&(t, v)| Point { t, v }).collect(),
                    histograms: 0,
                })
                .collect(),
        )
    }

    fn verdicts(input: &str, engine: &dyn Engine) -> Vec<Verdict> {
        let script = parse("t", input).expect("the snippet parses");
        run_script(engine, &script)
            .into_iter()
            .map(|o| o.verdict)
            .collect()
    }

    /// The rule that would poison many cases at once if wrong: an eval
    /// sees the loads above it since the last `clear`, and no others.
    #[test]
    fn an_eval_sees_only_the_loads_above_it_since_the_last_clear() {
        let counts = RefCell::new(Vec::new());
        let script = parse(
            "t",
            "load 1m\n  a 1\n\neval instant at 0 first\n\nload 1m\n  b 2\n\n\
             eval instant at 0 second\n\nclear\n\nload 1m\n  c 3\n\n\
             eval instant at 0 third\n",
        )
        .expect("the snippet parses");

        let _ = run_script(&CountingStub(&counts), &script);
        assert_eq!(counts.borrow().as_slice(), &[1, 2, 1]);
    }

    /// Records how many series each eval was seeded with.
    struct CountingStub<'a>(&'a RefCell<Vec<usize>>);

    impl Engine for CountingStub<'_> {
        fn range_query(
            &self,
            load: &[LoadedSeries<'_>],
            _query: &str,
            _start_ms: i64,
            _end_ms: i64,
            _step_ms: i64,
        ) -> Result<QueryResult, EngineError> {
            self.0
                .borrow_mut()
                .push(load.iter().map(|b| b.series.len()).sum());
            Ok(matrix(&[]))
        }
    }

    #[test]
    fn an_instant_eval_compares_the_single_point_as_a_vector() {
        let engine = Stub(|_: &str| Ok(matrix(&[(&[("job", "a")], &[(0, 7.0)])])));
        assert_eq!(
            verdicts("eval instant at 0 q\n  {job=\"a\"} 7\n", &engine),
            [Verdict::Pass]
        );
        assert!(!verdicts("eval instant at 0 q\n  {job=\"a\"} 8\n", &engine)[0].is_pass());
    }

    /// Without this the instant emulation could silently compare against
    /// whichever point happened to come first.
    #[test]
    fn more_than_one_point_for_an_instant_eval_is_a_harness_failure() {
        let engine = Stub(|_: &str| Ok(matrix(&[(&[("job", "a")], &[(0, 7.0), (60_000, 8.0)])])));
        let Verdict::Fail(detail) = &verdicts("eval instant at 0 q\n  {job=\"a\"} 7\n", &engine)[0]
        else {
            panic!("expected a failure")
        };
        assert!(detail.contains("HARNESS FAILURE"), "{detail}");
    }

    #[test]
    fn an_unordered_eval_ignores_the_order_the_engine_returned() {
        let engine = Stub(|_: &str| {
            Ok(matrix(&[
                (&[("job", "b")], &[(0, 2.0)]),
                (&[("job", "a")], &[(0, 1.0)]),
            ]))
        });
        let script = "eval instant at 0 q\n  {job=\"a\"} 1\n  {job=\"b\"} 2\n";
        assert_eq!(verdicts(script, &engine), [Verdict::Pass]);
    }

    /// The same reply and the same expectation as above, plus `expect
    /// ordered` — which must now reject it.
    #[test]
    fn an_ordered_eval_holds_the_engine_to_the_order() {
        let engine = Stub(|_: &str| {
            Ok(matrix(&[
                (&[("job", "b")], &[(0, 2.0)]),
                (&[("job", "a")], &[(0, 1.0)]),
            ]))
        });
        let script = "eval instant at 0 q\n  expect ordered\n  {job=\"a\"} 1\n  {job=\"b\"} 2\n";
        let Verdict::Fail(detail) = &verdicts(script, &engine)[0] else {
            panic!("expected the wrong order to be caught")
        };
        assert!(detail.contains("position 1"), "{detail}");

        let script = "eval instant at 0 q\n  expect ordered\n  {job=\"b\"} 2\n  {job=\"a\"} 1\n";
        assert_eq!(verdicts(script, &engine), [Verdict::Pass]);
    }

    #[test]
    fn a_range_eval_compares_every_point_and_its_timestamp() {
        let engine = Stub(|_: &str| Ok(matrix(&[(&[("job", "a")], &[(0, 1.0), (60_000, 2.0)])])));
        let script = "eval range from 0 to 1m step 1m q\n  {job=\"a\"} 1 2\n";
        assert_eq!(verdicts(script, &engine), [Verdict::Pass]);

        let script = "eval range from 0 to 1m step 1m q\n  {job=\"a\"} 1 3\n";
        assert!(!verdicts(script, &engine)[0].is_pass());
    }

    #[test]
    fn expect_fail_wants_an_error_and_nothing_else() {
        let erroring = Stub(|_: &str| Ok(QueryResult::Error("bad".into())));
        let answering = Stub(|_: &str| Ok(matrix(&[(&[], &[(0, 1.0)])])));
        let script = "eval instant at 0 q\n  expect fail\n";
        assert_eq!(verdicts(script, &erroring), [Verdict::Pass]);
        assert!(!verdicts(script, &answering)[0].is_pass());
    }

    #[test]
    fn a_bare_number_is_compared_as_a_scalar() {
        let engine = Stub(|_: &str| Ok(matrix(&[(&[], &[(0, 42.0)])])));
        assert_eq!(
            verdicts("eval instant at 0 q\n  42\n", &engine),
            [Verdict::Pass]
        );
        assert!(!verdicts("eval instant at 0 q\n  43\n", &engine)[0].is_pass());
    }

    #[test]
    fn an_unsupported_feature_is_reported_as_such_not_as_a_failure() {
        let engine = Stub(|_: &str| Err(EngineError::Unsupported("the frob function".into())));
        assert_eq!(
            verdicts("eval instant at 0 frob(x)\n  1\n", &engine),
            [Verdict::Unsupported("the frob function".into())]
        );
    }

    #[test]
    fn a_query_is_gated_by_exactly_the_flags_it_needs() {
        for (query, flags) in [
            ("rate(x[5m])", None),
            ("x +", None),
            (
                "mad_over_time(x[5m])",
                Some("promql-experimental-functions"),
            ),
            ("limitk(1, x)", Some("promql-experimental-functions")),
            // Gated, then refused with the gate open too: not ported.
            ("vector(step())", Some("promql-experimental-functions")),
            ("end() - start()", Some("promql-experimental-functions")),
            ("x + fill(0) y", Some("promql-binop-fill-modifiers")),
            (
                "rate(x[5m] anchored)",
                Some("promql-extended-range-selectors"),
            ),
            (
                "mad_over_time(x[5m] smoothed) + fill(0) y",
                Some(
                    "promql-experimental-functions, promql-extended-range-selectors, \
                     promql-binop-fill-modifiers",
                ),
            ),
        ] {
            assert_eq!(gating_flags("t", query).as_deref(), flags, "{query}");
        }
    }

    /// The duration productions are not ported, so a duration expression
    /// fails with a bare syntax error whatever the options; its file is
    /// all that says which flag it waits on.
    #[test]
    fn an_unparsed_query_in_a_gated_file_is_filed_under_its_flag() {
        let file = "duration_expression";
        assert_eq!(
            gating_flags(file, "changes(x[26m+4m])").as_deref(),
            Some("promql-duration-expr")
        );
        assert_eq!(gating_flags(file, "changes(x[30m])"), None);
        assert_eq!(gating_flags("functions", "changes(x[26m+4m])"), None);
    }

    fn categories(input: &str) -> Vec<super::super::report::Category> {
        let script = parse("t", input).expect("the snippet parses");
        let engine = Stub(|_: &str| Err(EngineError::Unsupported("x".into())));
        run_script(&engine, &script)
            .iter()
            .map(super::super::report::category)
            .collect()
    }

    const HIST: &str = "{{schema:0 sum:1 count:1}}";

    #[test]
    fn each_category_is_read_off_the_eval() {
        use super::super::report::Category::*;
        let script = format!(
            "load 1m\n  a 1 2\n  h {HIST}\n\n\
             eval instant at 0m mad_over_time(h[5m])\n  {{}} 1\n\n\
             eval instant at 0m h\n  h 1\n\n\
             eval instant at 0m sum(max_over_time(a[2m:1m]))\n  {{}} 1\n\n\
             eval instant at 0m a\n  a 1\n\n\
             eval instant at 0m a\n  a {HIST}\n"
        );
        assert_eq!(
            categories(&script),
            [
                FlagGated,
                NativeHistograms,
                Subquery,
                Core,
                NativeHistograms
            ]
        );
    }

    /// The coarse rule this replaces filed 165 of `operators.test`'s evals
    /// under histograms for one histogram series seeded beside them.
    #[test]
    fn a_float_eval_sharing_a_block_with_a_histogram_stays_core() {
        use super::super::report::Category::*;
        let script = format!(
            "load 1m\n  a{{job=\"x\"}} 1 2\n  h{{job=\"y\"}} {HIST}\n\n\
             eval instant at 0m a\n  a{{job=\"x\"}} 1\n\n\
             eval instant at 0m sum({{job=\"x\"}})\n  {{}} 1\n\n\
             eval instant at 0m a + ignoring(job) h\n  {{}} 1\n"
        );
        assert_eq!(categories(&script), [Core, Core, NativeHistograms]);
    }

    #[test]
    fn a_selector_matching_a_histogram_by_its_matchers_is_native_histogram() {
        use super::super::report::Category::*;
        let script = format!(
            "load 1m\n  a{{job=\"x\"}} 1 2\n  {{__name__=\"h\", job=\"y\"}} {HIST}\n\n\
             eval instant at 0m sum({{job=\"y\"}})\n  {{}} 1\n\n\
             eval instant at 0m count({{__name__=~\"a|h\", job!=\"x\"}})\n  {{}} 1\n\n\
             eval instant at 0m count({{__name__=~\"a|h\", job=\"x\"}})\n  {{}} 1\n\n\
             eval instant at 0m h{{job=\"x\"}}\n\n\
             clear\n\n\
             eval instant at 0m sum({{job=\"y\"}})\n"
        );
        assert_eq!(
            categories(&script),
            [NativeHistograms, NativeHistograms, Core, Core, Core]
        );
    }

    /// `load_with_nhcb` stores a native histogram per classic one under the
    /// name without `_bucket`; the classic floats stay readable as floats.
    #[test]
    fn load_with_nhcb_loads_the_derived_series_as_native_histograms() {
        use super::super::report::Category::*;
        let script = "load_with_nhcb 1m\n  \
             foo_bucket{le=\"1\", job=\"x\"} 1\n  foo_bucket{le=\"+Inf\", job=\"x\"} 2\n\n\
             eval instant at 0m histogram_quantile(0.5, foo{job=\"x\"})\n  {job=\"x\"} 1\n\n\
             eval instant at 0m histogram_quantile(0.5, foo_bucket)\n  {job=\"x\"} 1\n\n\
             eval instant at 0m foo{le=\"1\"}\n";
        assert_eq!(categories(script), [NativeHistograms, Core, Core]);
    }

    /// A query that does not parse has no selectors; the series it names
    /// are all there is to go on.
    #[test]
    fn an_unparsed_query_is_native_histogram_if_it_names_a_histogram() {
        use super::super::report::Category::*;
        let script = format!(
            "load 1m\n  a 1 2\n  h_test {HIST}\n\n\
             eval instant at 0m histogram_count(h_test </ 2)\n  {{}} 1\n\n\
             eval instant at 0m a </ 2\n  {{}} 1\n\n\
             eval instant at 0m h </ 2\n  {{}} 1\n"
        );
        assert_eq!(categories(&script), [NativeHistograms, Core, Core]);
    }

    #[test]
    fn a_histogram_line_gives_up_its_labels_in_either_spelling() {
        let labels = |pairs: &[(&str, &str)]| -> Option<Labels> {
            Some(
                pairs
                    .iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect(),
            )
        };
        assert_eq!(
            series_labels(&format!("h {HIST}x3")),
            labels(&[("__name__", "h")])
        );
        assert_eq!(
            series_labels(&format!("h{{a=\"}} \\\" {{\"}} 1 {HIST}")),
            labels(&[("__name__", "h"), ("a", "} \" {")])
        );
        assert_eq!(
            series_labels(&format!("{{__name__=\"h\", b=\"c\"}} {HIST}")),
            labels(&[("__name__", "h"), ("b", "c")])
        );
    }

    #[test]
    fn a_gated_eval_still_runs_with_every_gate_open() {
        let engine = Stub(|_: &str| Ok(matrix(&[(&[], &[(0, 1.0)])])));
        assert_eq!(
            verdicts("eval instant at 0 mad_over_time(x[5m])\n  {} 1\n", &engine),
            [Verdict::Gated {
                flags: "promql-experimental-functions".into(),
                verdict: Box::new(Verdict::Pass),
            }]
        );
    }

    #[test]
    fn a_subquery_anywhere_in_the_query_is_found() {
        assert!(has_subquery("a + rate(b[5m:1m])"));
        assert!(!has_subquery("rate(b[5m])"));
    }
}
