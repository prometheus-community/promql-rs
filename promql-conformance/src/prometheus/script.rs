//! The promqltest `.test` script format.
//!
//! Upstream has no grammar file for this. `promql/promqltest/test.go`
//! splits the input into lines, trims each one, blanks out whole-line
//! `#` comments, dispatches on the first token, and matches six regexes
//! — reproduced verbatim below. Everything *inside* a block is then
//! handed to the real PromQL grammar via `parser.ParseSeriesDesc`
//! (`test.go:513`), which is the half we already have as
//! [`promql_parser::parse_series_desc`].
//!
//! So this module is the line scanner and nothing more. It deliberately
//! does not evaluate anything, which is what lets it be tested against
//! all 20 vendored files before an engine is involved.
//!
//! # What it does not do
//!
//! A `.test` file is a *sequence*: loads accumulate into storage, evals
//! read it, `clear` wipes it. This module preserves that order and
//! leaves the execution to [`super`]. It also parses assertions it
//! cannot check — the `expect warn`/`info` family needs an annotation
//! channel the engine does not have — because recording them keeps the
//! size of that gap visible instead of silently dropping 617 lines.

use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use promql_parser::SeriesDescription;
use regex::Regex;

/// Where the vendored corpus lives, relative to this crate's root.
/// Resolved against `CARGO_MANIFEST_DIR` so the suite does not care
/// what directory it was invoked from.
pub const CORPUS_SUBDIR: &str = "testdata/prometheus";

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("read {path}: {source}")]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("{file}:{line}: not a command: {text:?}")]
    UnknownCommand {
        file: String,
        line: usize,
        text: String,
    },
    #[error("{file}:{line}: malformed eval directive: {text:?}")]
    MalformedEval {
        file: String,
        line: usize,
        text: String,
    },
    #[error("{file}:{line}: malformed expect: {text:?}")]
    MalformedExpect {
        file: String,
        line: usize,
        text: String,
    },
    #[error("{file}:{line}: cannot parse duration {raw:?}")]
    BadDuration {
        file: String,
        line: usize,
        raw: String,
    },
    #[error("{file}:{line}: range ends before it starts")]
    BackwardsRange { file: String, line: usize },
    #[error("{file}:{line}: an instant eval takes one value per series, got {got}")]
    MultipleInstantValues {
        file: String,
        line: usize,
        got: usize,
    },
    #[error("{file}:{line}: load block does not start with a `load <interval>` directive")]
    MissingLoadDirective { file: String, line: usize },
    #[error("{file}:{line}: invalid load interval {interval:?}")]
    InvalidInterval {
        file: String,
        line: usize,
        interval: String,
    },
}

// ---------------- the six regexes ----------------
//
// Transcribed from promqltest/test.go:51-57. Kept in that order and
// with upstream's names so a future divergence is a one-line diff
// against a file we vendor.

macro_rules! pattern {
    ($name:ident, $re:expr) => {
        fn $name() -> &'static Regex {
            static RE: OnceLock<Regex> = OnceLock::new();
            RE.get_or_init(|| Regex::new($re).expect("valid regex"))
        }
    };
}

pattern!(
    pat_eval_instant,
    r"^eval(?:_(fail|warn|ordered|info))?\s+instant\s+(?:at\s+(.+?))?\s+(.+)$"
);
pattern!(
    pat_eval_range,
    r"^eval(?:_(fail|warn|info))?\s+range\s+from\s+(.+)\s+to\s+(.+)\s+step\s+(.+?)\s+(.+)$"
);
pattern!(
    pat_expect,
    r"^expect\s+(ordered|fail|warn|no_warn|info|no_info)(?:\s+(regex|msg):(.+))?$"
);
pattern!(
    pat_expect_range,
    r"^expect range vector\s+from\s+(.+)\s+to\s+(.+)\s+step\s+(.+)$"
);

const EXPECT_RANGE_PREFIX: &str = "expect range vector";
const EXPECT_STRING_PREFIX: &str = "expect string";

// ---------------- the shapes ----------------

/// One `.test` file.
#[derive(Debug)]
pub struct Script {
    /// The file stem, e.g. `operators`. Used to name trials.
    pub name: String,
    /// Commands in source order. Order is the semantics: an eval sees
    /// exactly the loads above it since the last [`Command::Clear`].
    pub commands: Vec<Command>,
}

impl Script {
    pub fn evals(&self) -> impl Iterator<Item = &Eval> {
        self.commands.iter().filter_map(|c| match c {
            Command::Eval(e) => Some(&**e),
            _ => None,
        })
    }

    pub fn loads(&self) -> impl Iterator<Item = &Load> {
        self.commands.iter().filter_map(|c| match c {
            Command::Load(l) => Some(l),
            _ => None,
        })
    }
}

#[derive(Debug)]
pub enum Command {
    Load(Load),
    /// Boxed: an `Eval` is an order of magnitude larger than the other
    /// variants and there are thousands of them.
    Eval(Box<Eval>),
    Clear,
}

#[derive(Debug)]
pub struct Load {
    /// 1-based line of the directive, for diagnostics.
    pub line: usize,
    pub block: LoadBlock,
}

#[derive(Debug)]
pub struct Eval {
    /// 1-based line of the `eval` directive.
    pub line: usize,
    /// The PromQL expression, verbatim.
    pub query: String,
    pub timing: Timing,
    /// Present when an instant eval carries `expect range vector`,
    /// which switches its expected rows to a full value sequence.
    pub range_vector: Option<Range>,
    pub expect: Expect,
    pub expected: Expected,
    /// Expected rows the series grammar cannot read. None in the
    /// vendored corpus today; an upstream re-pin can bring one.
    ///
    /// Recorded rather than raised, and recorded rather than dropped:
    /// an eval whose expectation we can only half-read must be skipped,
    /// not compared against the half we understood.
    pub unsupported: Vec<UnsupportedLine>,
}

impl Eval {
    /// Whether every expected row parsed. Says nothing about the load
    /// blocks feeding it — see [`LoadBlock::is_fully_supported`].
    pub fn is_supported(&self) -> bool {
        self.unsupported.is_empty()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Timing {
    /// All times are offsets from the Unix epoch: upstream's
    /// `testStartTime` is `time.Unix(0, 0)`.
    Instant {
        at_ms: i64,
    },
    Range(Range),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Range {
    pub start_ms: i64,
    pub end_ms: i64,
    pub step_ms: i64,
}

/// The `expect …` assertions attached to an eval.
///
/// `eval_fail` / `eval_ordered` / `eval_warn` / `eval_info` are the
/// older prefix spelling of the same thing and land in these fields
/// too; upstream ORs the two forms together (`test.go:1069`).
#[derive(Debug, Default)]
pub struct Expect {
    /// `Some` means the query must fail. The [`Match`] says how hard
    /// the message is checked.
    pub fail: Option<Match>,
    pub ordered: bool,
    pub warn: Vec<Match>,
    pub no_warn: bool,
    pub info: Vec<Match>,
    pub no_info: bool,
}

impl Expect {
    pub fn expects_failure(&self) -> bool {
        self.fail.is_some()
    }

    /// Whether anything here needs an annotation channel we do not
    /// have. Counted rather than asserted, so the gap stays visible.
    pub fn has_annotation_assertions(&self) -> bool {
        self.no_warn || self.no_info || !self.warn.is_empty() || !self.info.is_empty()
    }
}

/// How closely an expected message is matched. `Any` is upstream's
/// `patMatchAny` — assert the annotation exists, not what it says.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Match {
    Any,
    Msg(String),
    Regex(String),
}

#[derive(Debug)]
pub enum Expected {
    /// Indented label-set rows.
    Series(Vec<ExpectedSeries>),
    /// A bare numeric line: the whole result is one scalar.
    Scalar(f64),
    /// `expect string "…"`.
    Str(String),
    /// No rows at all, which is what `expect fail` looks like.
    Nothing,
}

#[derive(Debug)]
pub struct ExpectedSeries {
    /// Labels and the value sequence, parsed by the same grammar
    /// upstream uses for these rows.
    pub desc: SeriesDescription,
    /// 1-based order of appearance among value rows, ignoring
    /// interleaved `expect` lines. This is what `ordered` compares
    /// against.
    pub pos: usize,
}

// ---------------- loading ----------------

/// The vendored corpus directory.
pub fn corpus_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join(CORPUS_SUBDIR)
}

/// Every vendored `.test` file, parsed, in filename order.
pub fn load_corpus() -> Result<Vec<Script>, Error> {
    let dir = corpus_dir();
    let mut paths: Vec<PathBuf> = std::fs::read_dir(&dir)
        .map_err(|source| Error::Read {
            path: dir.clone(),
            source,
        })?
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|e| e == "test"))
        .collect();
    paths.sort();

    paths.iter().map(|p| load_file(p)).collect()
}

pub fn load_file(path: &Path) -> Result<Script, Error> {
    let body = std::fs::read_to_string(path).map_err(|source| Error::Read {
        path: path.to_path_buf(),
        source,
    })?;
    let name = path
        .file_stem()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_default();
    parse(&name, &body)
}

// ---------------- the scanner ----------------

/// Upstream's `getLines` (`test.go:804`): trim every line, blank out
/// whole-line comments. Indentation carries no meaning after this —
/// only emptiness does.
fn clean(input: &str) -> Vec<&str> {
    input
        .lines()
        .map(|l| {
            let t = l.trim();
            if t.starts_with('#') {
                ""
            } else {
                t
            }
        })
        .collect()
}

pub fn parse(name: &str, input: &str) -> Result<Script, Error> {
    let lines = clean(input);
    let mut commands = Vec::new();
    let mut i = 0;

    while i < lines.len() {
        if lines[i].is_empty() {
            i += 1;
            continue;
        }
        // Upstream lowercases only the first token, and only to
        // dispatch; the directive regexes below then match the
        // original text, so a capitalised `Eval` would dispatch here
        // and fail there. Mirroring that rather than "fixing" it.
        let head = lines[i]
            .split_whitespace()
            .next()
            .unwrap_or_default()
            .to_ascii_lowercase();

        if head == "clear" {
            commands.push(Command::Clear);
            i += 1;
        } else if head.starts_with("load") {
            let (next, load) = parse_load(name, &lines, i)?;
            commands.push(Command::Load(load));
            i = next;
        } else if head.starts_with("eval") {
            let (next, eval) = parse_eval(name, &lines, i)?;
            commands.push(Command::Eval(Box::new(eval)));
            i = next;
        } else {
            return Err(Error::UnknownCommand {
                file: name.to_string(),
                line: i + 1,
                text: lines[i].to_string(),
            });
        }
    }

    Ok(Script {
        name: name.to_string(),
        commands,
    })
}

/// A command block runs to the first blank line.
fn block_end(lines: &[&str], start: usize) -> usize {
    let mut end = start + 1;
    while end < lines.len() && !lines[end].is_empty() {
        end += 1;
    }
    end
}

fn parse_load(file: &str, lines: &[&str], i: usize) -> Result<(usize, Load), Error> {
    let end = block_end(lines, i);
    let raw = lines[i..end].join("\n");
    let block = parse_load_block(file, i + 1, &raw)?;
    Ok((end, Load { line: i + 1, block }))
}

fn parse_eval(file: &str, lines: &[&str], i: usize) -> Result<(usize, Eval), Error> {
    let directive = lines[i];
    let line = i + 1;

    let (modifier, timing, query) = if let Some(c) = pat_eval_instant().captures(directive) {
        let at = c.get(2).map(|m| m.as_str()).unwrap_or("0");
        let at_ms = duration_ms(at).ok_or_else(|| Error::BadDuration {
            file: file.to_string(),
            line,
            raw: at.to_string(),
        })?;
        (
            c.get(1).map(|m| m.as_str().to_string()),
            Timing::Instant { at_ms },
            c[3].to_string(),
        )
    } else if let Some(c) = pat_eval_range().captures(directive) {
        let range = parse_range(file, line, &c[2], &c[3], &c[4])?;
        (
            c.get(1).map(|m| m.as_str().to_string()),
            Timing::Range(range),
            c[5].to_string(),
        )
    } else {
        return Err(Error::MalformedEval {
            file: file.to_string(),
            line,
            text: directive.to_string(),
        });
    };

    let mut eval = Eval {
        line,
        query,
        timing,
        range_vector: None,
        expect: Expect::default(),
        expected: Expected::Nothing,
        unsupported: Vec::new(),
    };

    // The legacy `eval_<modifier>` prefix sets the same fields the
    // `expect` lines do.
    match modifier.as_deref() {
        Some("fail") => eval.expect.fail = Some(Match::Any),
        Some("ordered") => eval.expect.ordered = true,
        Some("warn") => eval.expect.warn.push(Match::Any),
        Some("info") => eval.expect.info.push(Match::Any),
        _ => {}
    }

    let end = block_end(lines, i);
    let mut rows: Vec<ExpectedSeries> = Vec::new();
    let mut pos = 0usize;

    for (offset, row) in lines[i + 1..end].iter().enumerate() {
        let row_line = i + offset + 2;

        // The two `expect` forms with their own shape come first: the
        // general `patExpect` would not match them anyway, but
        // upstream checks them in this order and so do we.
        if let Some(rest) = row.strip_prefix(EXPECT_RANGE_PREFIX) {
            let _ = rest;
            let c = pat_expect_range()
                .captures(row)
                .ok_or_else(|| Error::MalformedExpect {
                    file: file.to_string(),
                    line: row_line,
                    text: row.to_string(),
                })?;
            eval.range_vector = Some(parse_range(file, row_line, &c[1], &c[2], &c[3])?);
            continue;
        }

        if let Some(rest) = row.strip_prefix(EXPECT_STRING_PREFIX) {
            eval.expected =
                Expected::Str(unquote(rest.trim()).ok_or_else(|| Error::MalformedExpect {
                    file: file.to_string(),
                    line: row_line,
                    text: row.to_string(),
                })?);
            continue;
        }

        if row.split_whitespace().next() == Some("expect") {
            apply_expect(&mut eval.expect, file, row_line, row)?;
            continue;
        }

        // A bare number is the whole result and ends the block.
        if let Some(v) = parse_number(row) {
            eval.expected = Expected::Scalar(v);
            break;
        }

        // Position advances for every value row, readable or not, so
        // that `ordered` positions stay faithful to the source text.
        pos += 1;

        let Ok(desc) = promql_parser::parse_series_desc(row) else {
            // A native-histogram descriptor is a known gap, not a
            // broken corpus. Anything else is a parser bug worth
            // seeing, so the two are kept apart.
            eval.unsupported.push(UnsupportedLine {
                text: row.to_string(),
                reason: if row.contains("{{") {
                    Unsupported::NativeHistogram
                } else {
                    Unsupported::ParseError
                },
            });
            continue;
        };

        // An instant eval takes one value per series unless it declared
        // `expect range vector`.
        if matches!(eval.timing, Timing::Instant { .. })
            && eval.range_vector.is_none()
            && desc.values.len() > 1
        {
            return Err(Error::MultipleInstantValues {
                file: file.to_string(),
                line: row_line,
                got: desc.values.len(),
            });
        }
        rows.push(ExpectedSeries { desc, pos });
    }

    if !rows.is_empty() {
        eval.expected = Expected::Series(rows);
    }

    Ok((end, eval))
}

fn apply_expect(expect: &mut Expect, file: &str, line: usize, row: &str) -> Result<(), Error> {
    let c = pat_expect()
        .captures(row)
        .ok_or_else(|| Error::MalformedExpect {
            file: file.to_string(),
            line,
            text: row.to_string(),
        })?;

    let matcher = match (c.get(2).map(|m| m.as_str()), c.get(3)) {
        (Some("msg"), Some(v)) => Match::Msg(v.as_str().trim().to_string()),
        (Some("regex"), Some(v)) => Match::Regex(v.as_str().trim().to_string()),
        _ => Match::Any,
    };

    match &c[1] {
        "fail" => expect.fail = Some(matcher),
        "ordered" => expect.ordered = true,
        "warn" => expect.warn.push(matcher),
        "no_warn" => expect.no_warn = true,
        "info" => expect.info.push(matcher),
        "no_info" => expect.no_info = true,
        other => unreachable!("patExpect matched an unlisted keyword {other:?}"),
    }
    Ok(())
}

fn parse_range(file: &str, line: usize, from: &str, to: &str, step: &str) -> Result<Range, Error> {
    let bad = |raw: &str| Error::BadDuration {
        file: file.to_string(),
        line,
        raw: raw.to_string(),
    };
    let start_ms = duration_ms(from).ok_or_else(|| bad(from))?;
    let end_ms = duration_ms(to).ok_or_else(|| bad(to))?;
    let step_ms = duration_ms(step).ok_or_else(|| bad(step))?;
    if end_ms < start_ms {
        return Err(Error::BackwardsRange {
            file: file.to_string(),
            line,
        });
    }
    Ok(Range {
        start_ms,
        end_ms,
        step_ms,
    })
}

/// A promqltest time offset, in milliseconds from the epoch.
///
/// Upstream uses `model.ParseDuration`, and `promql_parser::parse_duration_seconds`
/// is the same grammar via `promql_common::model::Duration`, so no local special
/// cases: a bare `0` (`eval instant at 0`) parses because Go accepts it, and `1.5h`
/// or `30m1h` fail because Go rejects them.
fn duration_ms(raw: &str) -> Option<i64> {
    let raw = raw.trim();
    promql_parser::parse_duration_seconds(raw)
        .ok()
        .map(|secs| (secs * 1000.0).round() as i64)
}

/// Upstream's `parseNumber` (`test.go:1883`): integer first, then
/// float. The integer attempt matters for values too large to survive a
/// float round-trip.
fn parse_number(s: &str) -> Option<f64> {
    if s.is_empty() {
        return None;
    }
    if let Ok(i) = s.parse::<i64>() {
        return Some(i as f64);
    }
    // Reject anything with whitespace: `{a="b"} 1` must reach the
    // series parser, not be mistaken for a scalar.
    if s.split_whitespace().count() != 1 {
        return None;
    }
    s.parse::<f64>().ok()
}

/// A Go string literal, as `expect string …` carries. Upstream passes
/// these through `strconv.Unquote`, which takes both the interpreted
/// `"…"` form and the raw backquoted form — and the corpus uses both,
/// deliberately, since the point of those cases is that a PromQL string
/// literal survives either spelling.
fn unquote(s: &str) -> Option<String> {
    if let Some(inner) = s.strip_prefix('`').and_then(|r| r.strip_suffix('`')) {
        // A raw literal: no escape processing at all.
        return Some(inner.to_string());
    }

    let inner = s.strip_prefix('"')?.strip_suffix('"')?;
    let mut out = String::with_capacity(inner.len());
    let mut chars = inner.chars();
    while let Some(c) = chars.next() {
        if c == '\\' {
            match chars.next()? {
                'n' => out.push('\n'),
                't' => out.push('\t'),
                other => out.push(other),
            }
        } else {
            out.push(c);
        }
    }
    Some(out)
}

// ---------------- load blocks ----------------

pattern!(pat_load, r"^load(?:_(with_nhcb))?\s+(.+?)\s*$");

/// A parsed `load <interval>` block.
#[derive(Debug, Clone, PartialEq)]
pub struct LoadBlock {
    /// The sample interval from the directive line, in seconds.
    pub interval_secs: f64,
    /// Whether the directive was `load_with_nhcb` rather than `load`.
    /// Upstream then also stores the block's classic histograms as
    /// native ones with custom buckets (`appendCustomHistogram`).
    pub with_nhcb: bool,
    /// One entry per series line, in source order.
    pub series: Vec<SeriesLine>,
}

impl LoadBlock {
    /// The series that parsed, skipping anything unsupported.
    pub fn parsed(&self) -> impl Iterator<Item = &SeriesDescription> {
        self.series.iter().filter_map(|s| match s {
            SeriesLine::Parsed(sd) => Some(sd),
            SeriesLine::Unsupported { .. } => None,
        })
    }

    /// The series lines this parser cannot handle yet.
    pub fn unsupported(&self) -> impl Iterator<Item = &UnsupportedLine> {
        self.series.iter().filter_map(|s| match s {
            SeriesLine::Unsupported(u) => Some(u),
            SeriesLine::Parsed(_) => None,
        })
    }

    pub fn is_fully_supported(&self) -> bool {
        self.unsupported().next().is_none()
    }
}

/// One line of a load block.
#[derive(Debug, Clone, PartialEq)]
pub enum SeriesLine {
    Parsed(SeriesDescription),
    /// Kept rather than dropped so callers can assert on how much of
    /// the corpus is out of reach, and watch that number shrink.
    Unsupported(UnsupportedLine),
}

#[derive(Debug, Clone, PartialEq)]
pub struct UnsupportedLine {
    /// The line as it appeared, trimmed.
    pub text: String,
    pub reason: Unsupported,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Unsupported {
    /// A line holding a `{{schema:1 ...}}` native-histogram descriptor
    /// that the series grammar still refused. It marks the eval as a
    /// native-histogram one, so it is categorised as such, not as core.
    NativeHistogram,
    /// Anything else the parser rejected. Treated as a real failure by
    /// the corpus tests rather than an accepted gap.
    ParseError,
}

/// Split a load block into its directive and its series lines.
///
/// A line that fails to parse is recorded rather than raised: a native
/// histogram is a known gap, not a broken corpus. Anything else is
/// flagged as [`Unsupported::ParseError`] so the tests can tell the two
/// apart.
fn parse_load_block(file: &str, line: usize, raw: &str) -> Result<LoadBlock, Error> {
    let mut lines = raw.lines().map(str::trim).filter(|l| !l.is_empty());

    let directive = lines.next().unwrap_or_default();
    let caps = pat_load()
        .captures(directive)
        .ok_or_else(|| Error::MissingLoadDirective {
            file: file.to_string(),
            line,
        })?;
    let with_nhcb = caps.get(1).is_some();
    let interval = caps.get(2).expect("group 2 always matches").as_str();
    let interval_secs =
        promql_parser::parse_duration_seconds(interval).map_err(|()| Error::InvalidInterval {
            file: file.to_string(),
            line,
            interval: interval.to_string(),
        })?;

    let series = lines
        .map(|l| match promql_parser::parse_series_desc(l) {
            Ok(sd) => SeriesLine::Parsed(sd),
            Err(_) => SeriesLine::Unsupported(UnsupportedLine {
                text: l.to_string(),
                reason: if l.contains("{{") {
                    Unsupported::NativeHistogram
                } else {
                    Unsupported::ParseError
                },
            }),
        })
        .collect();

    Ok(LoadBlock {
        interval_secs,
        with_nhcb,
        series,
    })
}
