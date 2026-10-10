//! The evals the engine is expected to pass, and must never stop passing.
//!
//! Most of the corpus is out of reach today, so a green CI run cannot
//! mean "everything passes". It means "everything we have *declared*
//! supported still passes" — and this is that declaration.
//!
//! It is deliberately an allowlist rather than a record of what fails.
//! Both catch a regression, but only one answers the question a reader
//! actually has, which is *what works right now*. A denylist answers it
//! by omission: the supported set is the complement of a file listing
//! everything broken, written down nowhere. It also makes deleting
//! coverage invisible — a case that quietly stops being tested looks
//! like any other line. Here it is a deletion in the diff.
//!
//! # Three checks, all failing closed
//!
//! | | |
//! |---|---|
//! | listed, and passes | fine |
//! | listed, and does not pass | **fail** — the regression this exists to catch |
//! | passes, not listed | **fail** — something was implemented; declare it |
//! | listed, no such eval ran | **fail** — an upstream re-pin moved it |
//!
//! The third is the ratchet. Implementing a feature *is* the act of
//! declaring it supported, so the list cannot drift behind reality and
//! nobody has to remember to update it. It costs a drive-by contributor
//! one re-bless, and their diff shows the win.
//!
//! # Form
//!
//! Grouped by `.test` file. A file is either the string `"all"` or a
//! list of case ids:
//!
//! ```toml
//! staleness = "all"
//! aggregators = [ 'sum by (group) (http_requests) @ 3000000' ]
//! ```
//!
//! `"all"` is stronger than spelling out the file's current cases: it
//! also binds evals a future re-pin *adds* to that file. That is the
//! point of it — an area that is fully green should stay fully green,
//! including for tests that do not exist yet.
//!
//! Case ids are the query text and the evaluation time, never the line
//! number. Upstream reflows comments constantly, and a line-keyed file
//! would invalidate wholesale on every bump and teach everyone to bless
//! without reading.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use super::run::{Outcome, Verdict};
use super::script::corpus_dir;

/// The generated file, next to the corpus it describes.
pub const FILE_NAME: &str = "SUPPORTED.toml";

/// Set to any non-empty value other than `0` to regenerate the file.
pub const BLESS_ENV: &str = "PROMQL_PROMQLTEST_BLESS";

/// The only keyword a batch may be spelled with.
const ALL: &str = "all";

pub fn path() -> PathBuf {
    corpus_dir().join(FILE_NAME)
}

pub fn bless_requested() -> bool {
    std::env::var_os(BLESS_ENV).is_some_and(|v| !v.is_empty() && v != "0")
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{path}: {source}\n\nRegenerate it with {BLESS_ENV}=1 cargo test -p promql-conformance --test promqltest")]
    Read {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("{path}: {source}")]
    Parse {
        path: PathBuf,
        source: toml::de::Error,
    },
    #[error("{path}: {file} = {found:?} is not a batch. Write {ALL:?} or a list of case ids.")]
    BadKeyword {
        path: PathBuf,
        file: String,
        found: String,
    },
    #[error("{path}: {source}")]
    Write {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("serialising the supported list: {0}")]
    Serialise(#[from] toml::ser::Error),
}

impl Error {
    /// Attach the file a string-parsed error came from.
    fn with_path(self, path: &Path) -> Self {
        match self {
            Error::Parse { source, .. } => Error::Parse {
                path: path.to_path_buf(),
                source,
            },
            Error::BadKeyword { file, found, .. } => Error::BadKeyword {
                path: path.to_path_buf(),
                file,
                found,
            },
            other => other,
        }
    }
}

/// What a `.test` file contributes to the supported set.
///
/// Untagged so the file can say either form naturally. The keyword is
/// kept as a `String` rather than modelled as a unit variant because
/// that lets [`Supported::load`] reject a typo with a message naming the
/// file, which an untagged enum's own error cannot do.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Batch {
    Keyword(String),
    Cases(BTreeSet<String>),
}

impl Batch {
    pub fn all() -> Self {
        Batch::Keyword(ALL.to_string())
    }

    pub fn is_all(&self) -> bool {
        matches!(self, Batch::Keyword(k) if k == ALL)
    }

    /// Whether this batch declares the given case supported.
    pub fn covers(&self, id: &str) -> bool {
        match self {
            Batch::Keyword(_) => true,
            Batch::Cases(ids) => ids.contains(id),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Batch::Keyword(_) => 0,
            Batch::Cases(ids) => ids.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        matches!(self, Batch::Cases(ids) if ids.is_empty())
    }
}

/// File stem → the cases in it we expect to pass.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Supported(BTreeMap<String, Batch>);

impl Supported {
    /// Whether an eval is declared supported.
    pub fn covers(&self, file: &str, id: &str) -> bool {
        self.0.get(file).is_some_and(|b| b.covers(id))
    }

    pub fn batch(&self, file: &str) -> Option<&Batch> {
        self.0.get(file)
    }

    /// Files declared `"all"`.
    pub fn all_files(&self) -> impl Iterator<Item = &str> {
        self.0
            .iter()
            .filter(|(_, b)| b.is_all())
            .map(|(f, _)| f.as_str())
    }

    /// Cases named explicitly. A `"all"` file contributes none, since
    /// what it covers is only knowable against a run.
    pub fn len(&self) -> usize {
        self.0.values().map(Batch::len).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Rebuild from a run, **preserving the form a human chose**.
    ///
    /// A file written as `"all"` stays `"all"`, so a hand-edit is not
    /// silently reverted by the next bless. The exception is a `"all"`
    /// file that no longer passes in full: keeping the keyword there
    /// would write a state that immediately fails, so it is downgraded
    /// to the cases that do pass. That is a loud change in the diff,
    /// which is what it should be — an area that was fully green no
    /// longer is.
    pub fn from_outcomes(outcomes: &[Outcome], previous: &Supported) -> (Self, Vec<String>) {
        let mut passing: BTreeMap<&str, BTreeSet<String>> = BTreeMap::new();
        let mut totals: BTreeMap<&str, (usize, usize)> = BTreeMap::new();

        for o in outcomes {
            let entry = totals.entry(&o.file).or_default();
            entry.1 += 1;
            if o.verdict.is_pass() {
                entry.0 += 1;
                passing.entry(&o.file).or_default().insert(o.id.clone());
            }
        }

        let mut files = BTreeMap::new();
        let mut downgraded = Vec::new();

        for (file, (passed, total)) in totals {
            let keep_all = previous.batch(file).is_some_and(Batch::is_all);
            if keep_all && passed == total {
                files.insert(file.to_string(), Batch::all());
                continue;
            }
            if keep_all {
                downgraded.push(format!(
                    "{file} was \"all\" but only {passed}/{total} pass now — \
                     downgraded to an explicit list"
                ));
            }
            if let Some(ids) = passing.remove(file) {
                files.insert(file.to_string(), Batch::Cases(ids));
            }
        }

        (Self(files), downgraded)
    }

    pub fn load(path: &Path) -> Result<Self, Error> {
        let body = std::fs::read_to_string(path).map_err(|source| Error::Read {
            path: path.to_path_buf(),
            source,
        })?;
        Self::load_from_str(&body).map_err(|e| e.with_path(path))
    }

    /// The parsing half of [`Self::load`], so callers and tests can work
    /// from a string. Errors carry no path until `load` attaches one.
    pub fn load_from_str(body: &str) -> Result<Self, Error> {
        let files: BTreeMap<String, Batch> =
            toml::from_str(body).map_err(|source| Error::Parse {
                path: PathBuf::new(),
                source,
            })?;
        match bad_keyword(&files) {
            Some((file, found)) => Err(Error::BadKeyword {
                path: PathBuf::new(),
                file,
                found,
            }),
            None => Ok(Self(files)),
        }
    }

    pub fn save(&self, path: &Path) -> Result<(), Error> {
        let mut out = String::new();
        out.push_str(
            "# The promqltest evals this engine is expected to pass.\n\
             #\n\
             # Everything named here MUST keep passing. Removing an entry deletes\n\
             # coverage — that is a code-review question, not a way to make CI green.\n\
             #\n\
             # A file is either \"all\" -- every eval in it, including ones a future\n\
             # upstream bump adds, which an explicit list cannot bind -- or a list of\n\
             # case ids. Regenerate with:\n\
             #\n\
             #   PROMQL_PROMQLTEST_BLESS=1 cargo test -p promql-conformance --test promqltest\n\
             #\n\
             # Blessing keeps a file's \"all\" form, because that is a deliberate human\n\
             # choice, and only downgrades it if the file stops passing in full. It\n\
             # does NOT keep comments you add below: this is written through serde, so\n\
             # put any rationale in the commit message instead.\n\
             #\n\
             # Everything else in the corpus still runs — it just is not gated. See\n\
             # UNSUPPORTED.md for what is missing, or run with PROMQL_PROMQLTEST_ALL=1.\n\n",
        );
        out.push_str(&toml::to_string_pretty(&self.0)?);
        std::fs::write(path, out).map_err(|source| Error::Write {
            path: path.to_path_buf(),
            source,
        })
    }
}

/// Find a batch spelled with anything but [`ALL`].
///
/// Worth its own check because [`Batch::covers`] treats any keyword as
/// "covers everything": a typo would otherwise silently promote a whole
/// file to the strictest contract there is.
fn bad_keyword(files: &BTreeMap<String, Batch>) -> Option<(String, String)> {
    files.iter().find_map(|(file, batch)| match batch {
        Batch::Keyword(k) if k != ALL => Some((file.clone(), k.clone())),
        _ => None,
    })
}

/// A disagreement between the supported list and this run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Violation {
    /// Declared supported, and it no longer passes. The regression this
    /// whole mechanism exists to catch.
    Regressed {
        file: String,
        id: String,
        why: String,
        at: String,
    },
    /// Passes, but nobody declared it. Someone implemented something.
    Unlisted { file: String, id: String },
    /// Declared, but no such eval ran.
    Orphaned { file: String, id: String },
    /// Declared, and the whole file is gone. Collapsed from what would
    /// otherwise be one orphan per case.
    MissingFile { file: String, cases: usize },
}

impl Violation {
    /// The trial name this reports under.
    pub fn trial_name(&self) -> String {
        match self {
            Violation::Regressed { file, id, .. }
            | Violation::Unlisted { file, id }
            | Violation::Orphaned { file, id } => format!("SUPPORTED {file}/{id}"),
            Violation::MissingFile { file, .. } => format!("SUPPORTED {file}/*"),
        }
    }
}

impl fmt::Display for Violation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Violation::Regressed { file, id, why, at } => write!(
                f,
                "REGRESSION. {file} declares this supported and it no longer passes.\n  \
                 {id}\n  at:  {at}\n  {why}\n\n  \
                 Fix the engine. Removing the entry from {FILE_NAME} would delete \
                 coverage rather than restore it."
            ),
            Violation::Unlisted { file, id } => write!(
                f,
                "{file}: this PASSES but is not declared supported.\n  \
                 {id}\n\n  \
                 Something got implemented. Declare it, so it can never silently \
                 break again:\n    \
                 {BLESS_ENV}=1 cargo test -p promql-conformance --test promqltest"
            ),
            Violation::Orphaned { file, id } => write!(
                f,
                "{file}: {FILE_NAME} declares an eval that no longer exists.\n  \
                 {id}\n\n  \
                 An upstream re-pin moved or deleted it. Check what it covered — and \
                 whether the case that replaced it is now unlisted — before re-blessing."
            ),
            Violation::MissingFile { file, cases } => write!(
                f,
                "{file}: {FILE_NAME} declares this file ({cases} case(s)) but no evals \
                 from it ran.\n\n  \
                 Either an upstream re-pin removed the file or the corpus failed to \
                 load. Do not re-bless until you know which."
            ),
        }
    }
}

/// Compare a run against the supported list. Empty means CI is green.
pub fn check(supported: &Supported, outcomes: &[Outcome]) -> Vec<Violation> {
    let mut violations = Vec::new();
    let mut live: BTreeMap<&str, BTreeSet<&str>> = BTreeMap::new();

    for o in outcomes {
        live.entry(&o.file).or_default().insert(&o.id);

        match (o.verdict.is_pass(), supported.covers(&o.file, &o.id)) {
            (true, true) | (false, false) => {}
            (true, false) => violations.push(Violation::Unlisted {
                file: o.file.clone(),
                id: o.id.clone(),
            }),
            (false, true) => violations.push(Violation::Regressed {
                file: o.file.clone(),
                id: o.id.clone(),
                why: why(&o.verdict),
                at: format!("{}.test:{}", o.file, o.line),
            }),
        }
    }

    for (file, batch) in &supported.0 {
        let Some(ran) = live.get(file.as_str()) else {
            violations.push(Violation::MissingFile {
                file: file.clone(),
                cases: batch.len(),
            });
            continue;
        };
        // An "all" batch names no cases, so it has nothing to orphan —
        // its contract is checked case by case above, against whatever
        // the file happens to contain.
        if let Batch::Cases(ids) = batch {
            for id in ids {
                if !ran.contains(id.as_str()) {
                    violations.push(Violation::Orphaned {
                        file: file.clone(),
                        id: id.clone(),
                    });
                }
            }
        }
    }

    violations
}

fn why(verdict: &Verdict) -> String {
    match verdict {
        Verdict::Pass => String::new(),
        Verdict::Fail(detail) => detail.clone(),
        Verdict::Skipped(reason) => format!("skipped: {reason}"),
        Verdict::Gated { flags, .. } => format!("gated behind {flags}"),
        Verdict::Unsupported(feature) => format!("{feature} is not supported yet"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn outcome(file: &str, id: &str, verdict: Verdict) -> Outcome {
        Outcome {
            file: file.to_string(),
            id: id.to_string(),
            line: 1,
            query: id.to_string(),
            verdict,
            expected_rows: 1,
            unchecked_annotations: false,
            histograms: false,
            subquery: false,
        }
    }

    fn listed(entries: &[(&str, &[&str])]) -> Supported {
        Supported(
            entries
                .iter()
                .map(|(file, ids)| {
                    (
                        file.to_string(),
                        Batch::Cases(ids.iter().map(|s| s.to_string()).collect()),
                    )
                })
                .collect(),
        )
    }

    #[test]
    fn a_listed_case_that_still_passes_is_green() {
        let s = listed(&[("ops", &["q @ 0"])]);
        let run = [outcome("ops", "q @ 0", Verdict::Pass)];
        assert_eq!(check(&s, &run), []);
    }

    /// The whole point of the file.
    #[test]
    fn a_listed_case_that_stops_passing_is_a_regression() {
        let s = listed(&[("ops", &["q @ 0"])]);
        let run = [outcome("ops", "q @ 0", Verdict::Fail("broke".into()))];
        let v = check(&s, &run);
        assert!(
            matches!(&v[..], [Violation::Regressed { why, .. }] if why == "broke"),
            "{v:?}"
        );
    }

    /// A previously-passing case that becomes unaskable is still a loss
    /// of coverage, so "not a pass" is the test rather than "a failure".
    #[test]
    fn a_listed_case_that_becomes_skipped_or_unsupported_also_regresses() {
        let s = listed(&[("ops", &["a @ 0", "b @ 0"])]);
        let run = [
            outcome("ops", "a @ 0", Verdict::Skipped("histogram".into())),
            outcome(
                "ops",
                "b @ 0",
                Verdict::Unsupported("the frob function".into()),
            ),
        ];
        let v = check(&s, &run);
        assert_eq!(v.len(), 2, "{v:?}");
        assert!(v.iter().all(|x| matches!(x, Violation::Regressed { .. })));
    }

    /// The ratchet: implementing something forces declaring it.
    #[test]
    fn a_newly_passing_case_must_be_declared() {
        let run = [outcome("ops", "q @ 0", Verdict::Pass)];
        assert_eq!(
            check(&Supported::default(), &run),
            [Violation::Unlisted {
                file: "ops".into(),
                id: "q @ 0".into(),
            }]
        );
    }

    #[test]
    fn a_case_that_never_passed_and_is_not_listed_is_ignored() {
        let run = [outcome(
            "ops",
            "q @ 0",
            Verdict::Fail("still broken".into()),
        )];
        assert_eq!(check(&Supported::default(), &run), []);
    }

    /// What makes an upstream re-pin visible instead of silent.
    #[test]
    fn a_listed_case_with_no_matching_eval_is_orphaned() {
        let s = listed(&[("ops", &["deleted upstream @ 0"])]);
        let run = [outcome(
            "ops",
            "something else @ 0",
            Verdict::Fail("x".into()),
        )];
        let v = check(&s, &run);
        assert!(
            matches!(&v[..], [Violation::Orphaned { id, .. }] if id == "deleted upstream @ 0"),
            "{v:?}"
        );
    }

    /// One violation for a vanished file, not one per case in it.
    #[test]
    fn a_vanished_file_reports_once() {
        let s = listed(&[("ops", &["a @ 0", "b @ 0", "c @ 0"])]);
        assert_eq!(
            check(&s, &[]),
            [Violation::MissingFile {
                file: "ops".into(),
                cases: 3,
            }]
        );
    }

    #[test]
    fn the_same_id_in_two_files_is_two_cases() {
        let s = listed(&[("ops", &["q @ 0"])]);
        let run = [
            outcome("ops", "q @ 0", Verdict::Pass),
            outcome("funcs", "q @ 0", Verdict::Pass),
        ];
        let v = check(&s, &run);
        assert!(
            matches!(&v[..], [Violation::Unlisted { file, .. }] if file == "funcs"),
            "{v:?}"
        );
    }

    // ---------------- the "all" batch ----------------

    #[test]
    fn an_all_batch_covers_every_case_in_its_file() {
        let s = Supported(BTreeMap::from([("staleness".to_string(), Batch::all())]));
        let run = [
            outcome("staleness", "a @ 0", Verdict::Pass),
            outcome("staleness", "b @ 0", Verdict::Pass),
        ];
        assert_eq!(check(&s, &run), []);
    }

    /// The reason `"all"` is worth having: it binds cases that did not
    /// exist when it was written, which an explicit list cannot.
    #[test]
    fn an_all_batch_binds_cases_added_later() {
        let s = Supported(BTreeMap::from([("staleness".to_string(), Batch::all())]));
        let run = [
            outcome("staleness", "a @ 0", Verdict::Pass),
            outcome(
                "staleness",
                "added by a re-pin @ 0",
                Verdict::Fail("nope".into()),
            ),
        ];
        let v = check(&s, &run);
        assert!(
            matches!(&v[..], [Violation::Regressed { id, .. }] if id == "added by a re-pin @ 0"),
            "{v:?}"
        );
    }

    #[test]
    fn an_all_batch_has_no_cases_to_orphan() {
        let s = Supported(BTreeMap::from([("staleness".to_string(), Batch::all())]));
        let run = [outcome("staleness", "whatever @ 0", Verdict::Pass)];
        assert_eq!(check(&s, &run), []);
    }

    // ---------------- blessing ----------------

    #[test]
    fn blessing_records_only_what_passes() {
        let run = [
            outcome("ops", "a @ 0", Verdict::Pass),
            outcome("ops", "b @ 0", Verdict::Fail("x".into())),
            outcome(
                "ops",
                "c @ 0",
                Verdict::Unsupported("the frob function".into()),
            ),
        ];
        let (s, warnings) = Supported::from_outcomes(&run, &Supported::default());
        assert!(s.covers("ops", "a @ 0"));
        assert!(!s.covers("ops", "b @ 0"));
        assert!(!s.covers("ops", "c @ 0"));
        assert_eq!(s.len(), 1);
        assert!(warnings.is_empty());
    }

    /// Otherwise a hand-edit to `"all"` would be undone by the next
    /// bless, and nobody could use the stronger form.
    #[test]
    fn blessing_preserves_an_all_batch() {
        let previous = Supported(BTreeMap::from([("staleness".to_string(), Batch::all())]));
        let run = [
            outcome("staleness", "a @ 0", Verdict::Pass),
            outcome("staleness", "b @ 0", Verdict::Pass),
        ];
        let (s, warnings) = Supported::from_outcomes(&run, &previous);
        assert_eq!(s.batch("staleness"), Some(&Batch::all()));
        assert!(warnings.is_empty());
    }

    /// Keeping the keyword would write a state that immediately fails,
    /// so bless would stop being idempotent. Downgrade, and say so.
    #[test]
    fn blessing_downgrades_an_all_batch_that_no_longer_passes_in_full() {
        let previous = Supported(BTreeMap::from([("staleness".to_string(), Batch::all())]));
        let run = [
            outcome("staleness", "a @ 0", Verdict::Pass),
            outcome("staleness", "b @ 0", Verdict::Fail("x".into())),
        ];
        let (s, warnings) = Supported::from_outcomes(&run, &previous);
        assert_eq!(
            s.batch("staleness"),
            Some(&Batch::Cases(BTreeSet::from(["a @ 0".to_string()])))
        );
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].contains("1/2"), "{:?}", warnings[0]);
        // And the result it wrote is green.
        assert_eq!(check(&s, &run), []);
    }

    #[test]
    fn blessing_omits_a_file_with_nothing_passing() {
        let run = [outcome("ops", "a @ 0", Verdict::Fail("x".into()))];
        let (s, _) = Supported::from_outcomes(&run, &Supported::default());
        assert!(s.is_empty());
    }

    // ---------------- the file ----------------

    /// Query text is full of quotes and backslashes, and both forms have
    /// to survive a round trip.
    #[test]
    fn both_forms_round_trip_through_toml() {
        let written = Supported(BTreeMap::from([
            ("staleness".to_string(), Batch::all()),
            (
                "ops".to_string(),
                Batch::Cases(BTreeSet::from([
                    r#"label_replace(x, "d", "$1", "src", "(\\d+)") @ 0"#.to_string(),
                    "sum(x) by (job) @ 0..60000/30000".to_string(),
                ])),
            ),
        ]));
        let body = toml::to_string_pretty(&written.0).expect("serialises");
        let read: BTreeMap<String, Batch> = toml::from_str(&body).expect("parses back");
        assert_eq!(Supported(read), written);
    }

    /// A typo must not quietly promote a file to the strictest contract
    /// there is, which is what `covers` would otherwise do with it.
    #[test]
    fn a_misspelled_keyword_is_rejected_by_name() {
        let files: BTreeMap<String, Batch> =
            toml::from_str("staleness = \"al\"\n").expect("parses as toml");
        assert!(files["staleness"].covers("anything @ 0"), "the trap");
        assert_eq!(
            bad_keyword(&files),
            Some(("staleness".to_string(), "al".to_string()))
        );
    }

    #[test]
    fn the_real_keyword_and_case_lists_pass_validation() {
        let files: BTreeMap<String, Batch> =
            toml::from_str("staleness = \"all\"\nops = [\"q @ 0\"]\n").expect("parses");
        assert_eq!(bad_keyword(&files), None);
    }
}
