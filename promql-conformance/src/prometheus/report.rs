//! Where the engine stands against the corpus.
//!
//! Separate from [`super::supported`] because these answer different
//! questions and only one of them is a gate. The supported list says
//! what must not break; this says how much of PromQL is left, which is
//! the number you want when deciding what to build next rather than
//! whether to merge.
//!
//! Both views are printed on every run, gated or not. The whole corpus
//! evaluates in well under a second with no oracle and no network, so
//! there is nothing to save by not measuring, and a green tick that
//! hides "12% of PromQL works" would be its own kind of lie.

use std::collections::BTreeMap;
use std::fmt::Write as _;

use super::run::{Outcome, Verdict};
use super::supported::Supported;

/// One `.test` file's standing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileStats {
    pub file: String,
    pub evals: usize,
    pub passed: usize,
    /// Evals we cannot even ask — a native histogram in the data. Worth
    /// separating from failures: a file at 0/521 because the parser
    /// cannot read histograms is not the same finding as one at 0/213
    /// because the engine gets everything wrong.
    pub skipped: usize,
    /// Evals of experimental syntax, left out of `evals` and everything
    /// derived from it. See [`Verdict::Gated`].
    pub gated: usize,
    /// Evals with native histograms in play, passing or not. The three
    /// of `histograms`, `subqueries` and the rest partition `evals`, with
    /// histograms winning an eval that is both, so the columns add up.
    pub histograms: usize,
    pub subqueries: usize,
    /// Passes among the evals that are neither.
    pub core_passed: usize,
}

impl FileStats {
    pub fn core_evals(&self) -> usize {
        self.evals - self.histograms - self.subqueries
    }

    pub fn core_percent(&self) -> f64 {
        percent(self.core_passed, self.core_evals())
    }

    pub fn percent(&self) -> f64 {
        percent(self.passed, self.evals)
    }

    pub fn is_fully_green(&self) -> bool {
        self.evals > 0 && self.passed == self.evals
    }
}

fn percent(part: usize, whole: usize) -> f64 {
    if whole == 0 {
        0.0
    } else {
        100.0 * part as f64 / whole as f64
    }
}

/// Why a core eval fails, one per eval.
///
/// Ordered by who can act: native histograms and subqueries are named
/// feature areas that are parked, so an eval in either is not a finding
/// about anything else. Only what is left over says how far the engine is
/// from the whole of the rest of the language.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reason {
    NativeHistograms,
    Subquery,
    MissingFeature,
    Unattributed,
}

/// First match wins. `None` for a pass and for a gated eval, which are not
/// failures of the core language.
pub fn reason(o: &Outcome) -> Option<Reason> {
    match o.verdict {
        Verdict::Pass | Verdict::Gated(_) => None,
        _ if o.histograms => Some(Reason::NativeHistograms),
        _ if o.subquery => Some(Reason::Subquery),
        Verdict::Unsupported(_) => Some(Reason::MissingFeature),
        _ => Some(Reason::Unattributed),
    }
}

pub fn per_file(outcomes: &[Outcome]) -> Vec<FileStats> {
    let mut by_file: BTreeMap<&str, FileStats> = BTreeMap::new();
    for o in outcomes {
        let stats = by_file.entry(&o.file).or_insert_with(|| FileStats {
            file: o.file.clone(),
            evals: 0,
            passed: 0,
            skipped: 0,
            gated: 0,
            histograms: 0,
            subqueries: 0,
            core_passed: 0,
        });
        if matches!(o.verdict, Verdict::Gated(_)) {
            stats.gated += 1;
            continue;
        }
        stats.evals += 1;
        match o.verdict {
            Verdict::Pass => stats.passed += 1,
            Verdict::Skipped(_) => stats.skipped += 1,
            _ => {}
        }
        if o.histograms {
            stats.histograms += 1;
        } else if o.subquery {
            stats.subqueries += 1;
        } else if o.verdict.is_pass() {
            stats.core_passed += 1;
        }
    }
    let mut stats: Vec<FileStats> = by_file.into_values().collect();
    // Best first: the question is "what is closest to done".
    stats.sort_by(|a, b| {
        b.percent()
            .total_cmp(&a.percent())
            .then_with(|| b.evals.cmp(&a.evals))
    });
    stats
}

/// Missing engine features, and how many evals each blocks. Only evals
/// whose [`Reason`] is the feature: one that also has native histograms or
/// a subquery is counted under that, so the rows sum to the reasons table.
pub fn missing_features(outcomes: &[Outcome]) -> BTreeMap<&str, usize> {
    let mut features: BTreeMap<&str, usize> = BTreeMap::new();
    for o in outcomes {
        if let (Verdict::Unsupported(feature), Some(Reason::MissingFeature)) =
            (&o.verdict, reason(o))
        {
            *features.entry(feature.as_str()).or_default() += 1;
        }
    }
    features
}

/// Failing evals per reason, in the order the reasons are tried.
pub fn reason_counts(outcomes: &[Outcome]) -> [(Reason, usize); 4] {
    let count = |r| outcomes.iter().filter(|o| reason(o) == Some(r)).count();
    [
        Reason::NativeHistograms,
        Reason::Subquery,
        Reason::MissingFeature,
        Reason::Unattributed,
    ]
    .map(|r| (r, count(r)))
}

/// How many gated evals each `--enable-feature` combination holds out.
pub fn gated_by_flags(outcomes: &[Outcome]) -> BTreeMap<&str, usize> {
    let mut gated: BTreeMap<&str, usize> = BTreeMap::new();
    for o in outcomes {
        if let Verdict::Gated(flags) = &o.verdict {
            *gated.entry(flags.as_str()).or_default() += 1;
        }
    }
    gated
}

/// The per-file table printed on every run.
///
/// Nudges toward `"all"` where a file is fully green, but never acts on
/// it: promoting a file to the stronger form binds evals that do not
/// exist yet, and that should be somebody's decision.
pub fn scoreboard(outcomes: &[Outcome], supported: &Supported) -> String {
    let stats = per_file(outcomes);
    let passed: usize = stats.iter().map(|s| s.passed).sum();
    let total: usize = stats.iter().map(|s| s.evals).sum();

    let mut out = String::new();
    let _ = writeln!(
        out,
        "\npromqltest: {} files, {total} evals — {passed} pass ({:.1}%)\n",
        stats.len(),
        if total == 0 {
            0.0
        } else {
            100.0 * passed as f64 / total as f64
        },
    );
    let gated: usize = stats.iter().map(|s| s.gated).sum();
    if gated > 0 {
        let _ = writeln!(
            out,
            "  not counted: {gated} more evals use experimental syntax behind a parser flag\n"
        );
    }

    let width = stats.iter().map(|s| s.file.len()).max().unwrap_or(0);
    for s in &stats {
        let ratio = format!("{}/{}", s.passed, s.evals);
        let _ = write!(
            out,
            "  {:<width$}  {ratio:>9}  {:>5.1}%",
            s.file,
            s.percent()
        );
        if s.is_fully_green() && !supported.batch(&s.file).is_some_and(|b| b.is_all()) {
            let _ = write!(out, "   ← fully green; consider {} = \"all\"", s.file);
        } else if s.skipped == s.evals && s.evals > 0 {
            let _ = write!(out, "   (all skipped)");
        } else if s.skipped > 0 {
            let _ = write!(out, "   ({} skipped)", s.skipped);
        }
        if s.gated > 0 {
            let _ = write!(out, "   (+{} gated)", s.gated);
        }
        out.push('\n');
    }

    out
}

/// `UNSUPPORTED.md` — the roadmap, regenerated on bless.
///
/// Committed but never gating. It exists so "what should I implement
/// next" is answerable by reading the repo, and so a feature landing
/// shows up as a diff someone can be pleased about, rather than only as
/// a number in a CI log nobody scrolls back to.
pub fn inventory_markdown(outcomes: &[Outcome]) -> String {
    let features = missing_features(outcomes);
    let blocked: usize = features.values().sum();
    let stats = per_file(outcomes);
    let passed: usize = stats.iter().map(|s| s.passed).sum();
    let total: usize = stats.iter().map(|s| s.evals).sum();

    let core_total: usize = stats.iter().map(|s| s.core_evals()).sum();
    let core_passed: usize = stats.iter().map(|s| s.core_passed).sum();
    let counts = reason_counts(outcomes);
    let count_of = |r| counts.iter().find(|(x, _)| *x == r).map_or(0, |c| c.1);

    let mut out = String::new();
    out.push_str(
        "# What the engine is missing\n\n\
         Generated — do not edit by hand. Regenerated alongside `SUPPORTED.toml` by:\n\n\
         ```sh\n\
         PROMQL_PROMQLTEST_BLESS=1 cargo test -p promql-conformance --test promqltest\n\
         ```\n\n\
         Nothing here gates CI. This file is the roadmap, and the second line below is \
         the target: when every eval that is neither native-histogram nor subquery \
         passes, it reads 100%. Every failing eval is filed under exactly one reason, \
         tried in the order of the table, so what is left under *unattributed* is the \
         work nobody has named yet.\n\n",
    );

    let _ = writeln!(
        out,
        "Against the vendored corpus: **{passed} of {total} evals pass** ({:.1}%), \
         and **{blocked}** are blocked on the features below.\n",
        percent(passed, total),
    );
    let _ = writeln!(
        out,
        "Excluding native histograms and subqueries: **{core_passed} of {core_total}** \
         pass ({:.1}%). An eval is excluded if any series in its load blocks or its \
         expected rows is a native histogram, or its query contains a subquery, \
         whether it passes or not, so the denominator holds still.\n",
        percent(core_passed, core_total),
    );

    // The failing core evals are exactly the two reasons with work in
    // them; the other two are parked areas.
    let _ = write!(
        out,
        "## Core evals\n\n\
         | | evals |\n|---|---:|\n\
         | total | {core_total} |\n\
         | passing | {core_passed} |\n\
         | not passing | {} |\n\n\
         The not-passing ones are the missing features and the unattributed failures \
         below. Fixing those is what takes the core to 100%.\n\n",
        core_total - core_passed,
    );

    let failing: usize = counts.iter().map(|c| c.1).sum();
    let _ = write!(
        out,
        "## Why evals fail\n\n\
         | failing evals | reason |\n|---:|---|\n\
         | {} | native histograms |\n\
         | {} | subquery |\n\
         | {} | missing features |\n\
         | {} | unattributed |\n\
         | {failing} | **total** |\n\n",
        count_of(Reason::NativeHistograms),
        count_of(Reason::Subquery),
        count_of(Reason::MissingFeature),
        count_of(Reason::Unattributed),
    );

    out.push_str("## Missing features\n\n| evals blocked | feature |\n|---:|---|\n");
    let mut rows: Vec<(&str, usize)> = features.into_iter().collect();
    rows.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(b.0)));
    for (feature, count) in rows {
        let _ = writeln!(out, "| {count} | {feature} |");
    }

    let mut unattributed: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
    for o in outcomes
        .iter()
        .filter(|o| reason(o) == Some(Reason::Unattributed))
    {
        unattributed.entry(&o.file).or_default().push(&o.id);
    }
    if !unattributed.is_empty() {
        out.push_str(
            "\n## Unattributed failures\n\n\
             Not native histograms, not a subquery, and the engine does not name a missing \
             feature: wrong results, panics, errors nobody has classified. Ids are spelled \
             as in `SUPPORTED.toml`.\n",
        );
        for (file, ids) in &unattributed {
            let _ = write!(out, "\n### {file} ({})\n\n", ids.len());
            for id in ids {
                let _ = writeln!(out, "- {}", code_span(id));
            }
        }
    }

    let gated = gated_by_flags(outcomes);
    if !gated.is_empty() {
        let total: usize = gated.values().sum();
        let _ = write!(
            out,
            "\n## Left out: experimental syntax\n\n\
             **{total}** more evals use syntax that stock Prometheus refuses unless an \
             `--enable-feature` value is on. They are not part of the core language yet, \
             so they are in no total above, in no row of the tables, and not in \
             `SUPPORTED.toml`. The harness still evaluates them with every gate open; \
             `docs/feature-flags.md` lists the flags.\n\n\
             | evals | needs |\n|---:|---|\n"
        );
        let mut rows: Vec<(&str, usize)> = gated.into_iter().collect();
        rows.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(b.0)));
        for (flags, count) in rows {
            let _ = writeln!(out, "| {count} | `{flags}` |");
        }
    }

    out.push_str(
        "\n## By file\n\n\
         | file | evals | native histograms | subquery | passing | passing core | |\n\
         |---|---:|---:|---:|---:|---:|---|\n",
    );
    for s in &stats {
        let note = if s.evals == 0 && s.gated > 0 {
            "all gated — experimental syntax"
        } else if s.is_fully_green() {
            "fully green"
        } else if s.skipped == s.evals {
            "all skipped — native histograms"
        } else {
            ""
        };
        let _ = writeln!(
            out,
            "| {} | {} | {} | {} | {} ({:.0}%) | {} of {} ({:.0}%) | {note} |",
            s.file,
            s.evals,
            s.histograms,
            s.subqueries,
            s.passed,
            s.percent(),
            s.core_passed,
            s.core_evals(),
            s.core_percent(),
        );
    }

    out.push_str(
        "\n---\n\n\
         Some failures are one cause wearing many hats — a single lexer gap can account \
         for hundreds of rows above. Read a handful before picking, with:\n\n\
         ```sh\n\
         PROMQL_PROMQLTEST_ALL=1 cargo test -p promql-conformance --test promqltest -- <file>/\n\
         ```\n",
    );

    out
}

/// A code span that survives backticks inside the text.
fn code_span(text: &str) -> String {
    let longest = text
        .split(|c| c != '`')
        .map(str::len)
        .max()
        .unwrap_or_default();
    let fence = "`".repeat(longest + 1);
    let pad = if text.starts_with('`') || text.ends_with('`') {
        " "
    } else {
        ""
    };
    format!("{fence}{pad}{text}{pad}{fence}")
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

    fn sample() -> Vec<Outcome> {
        vec![
            outcome("green", "a @ 0", Verdict::Pass),
            outcome("green", "b @ 0", Verdict::Pass),
            outcome("half", "a @ 0", Verdict::Pass),
            outcome("half", "b @ 0", Verdict::Fail("wrong".into())),
            outcome(
                "none",
                "a @ 0",
                Verdict::Unsupported("the frob function".into()),
            ),
            outcome(
                "none",
                "b @ 0",
                Verdict::Unsupported("the frob function".into()),
            ),
            outcome(
                "none",
                "c @ 0",
                Verdict::Unsupported("the baz function".into()),
            ),
            outcome("hist", "a @ 0", Verdict::Skipped("histogram".into())),
        ]
    }

    #[test]
    fn per_file_counts_and_orders_by_how_close_to_done() {
        let stats = per_file(&sample());
        let names: Vec<&str> = stats.iter().map(|s| s.file.as_str()).collect();
        assert_eq!(names, ["green", "half", "none", "hist"]);
        assert_eq!(stats[0].passed, 2);
        assert_eq!(stats[0].evals, 2);
        assert!(stats[0].is_fully_green());
        assert_eq!(stats[1].percent(), 50.0);
        assert_eq!(stats[3].skipped, 1);
    }

    #[test]
    fn features_are_counted_by_how_many_evals_they_block() {
        let outcomes = sample();
        let features = missing_features(&outcomes);
        assert_eq!(features["the frob function"], 2);
        assert_eq!(features["the baz function"], 1);
        assert_eq!(features.len(), 2);
    }

    #[test]
    fn gated_evals_are_in_no_total_and_are_listed_by_flag() {
        let mut outcomes = sample();
        outcomes.push(outcome(
            "fill",
            "1",
            Verdict::Gated("promql-binop-fill-modifiers".into()),
        ));
        outcomes.push(outcome(
            "half",
            "9",
            Verdict::Gated("promql-extended-range-selectors".into()),
        ));
        let stats = per_file(&outcomes);
        let fill = stats.iter().find(|s| s.file == "fill").unwrap();
        assert_eq!((fill.evals, fill.passed, fill.gated), (0, 0, 1));
        let half = stats.iter().find(|s| s.file == "half").unwrap();
        assert_eq!((half.evals, half.gated), (2, 1));

        let out = scoreboard(&outcomes, &Supported::default());
        assert!(out.contains("8 evals — 3 pass (37.5%)"), "{out}");
        assert!(out.contains("2 more evals"), "{out}");

        let md = inventory_markdown(&outcomes);
        assert!(md.contains("**3 of 8 evals pass**"), "{md}");
        assert!(md.contains("| 1 | `promql-binop-fill-modifiers` |"), "{md}");
        assert!(md.contains("all gated"), "{md}");
        assert!(!inventory_markdown(&sample()).contains("Left out"));
    }

    #[test]
    fn a_fully_green_file_is_nudged_toward_all_until_it_says_all() {
        let out = scoreboard(&sample(), &Supported::default());
        assert!(out.contains("green = \"all\""), "{out}");

        // Once declared, the nudge goes away rather than repeating.
        let declared = Supported::load_from_str("green = \"all\"\n").expect("parses");
        let out = scoreboard(&sample(), &declared);
        assert!(!out.contains("consider"), "{out}");
    }

    #[test]
    fn the_scoreboard_totals_match_the_outcomes() {
        let out = scoreboard(&sample(), &Supported::default());
        assert!(out.contains("8 evals — 3 pass (37.5%)"), "{out}");
    }

    #[test]
    fn the_inventory_leads_with_the_most_blocking_feature() {
        let md = inventory_markdown(&sample());
        let frob = md.find("the frob function").expect("listed");
        let baz = md.find("the baz function").expect("listed");
        assert!(frob < baz, "the bigger win should come first");
        assert!(md.contains("**3 of 8 evals pass**"), "{md}");
        assert!(md.contains("all skipped"), "{md}");
    }

    fn flagged(id: &str, verdict: Verdict, histograms: bool, subquery: bool) -> Outcome {
        Outcome {
            histograms,
            subquery,
            ..outcome("f", id, verdict)
        }
    }

    /// One eval per reason, plus the two ways precedence can go wrong.
    fn reasoned() -> Vec<Outcome> {
        vec![
            flagged("hist", Verdict::Fail("x".into()), true, false),
            flagged("sub", Verdict::Fail("x".into()), false, true),
            flagged(
                "feat",
                Verdict::Unsupported("the frob function".into()),
                false,
                false,
            ),
            flagged("rest", Verdict::Fail("wrong".into()), false, false),
            // A histogram eval the engine would also call a missing
            // feature, and one that is both histogram and subquery:
            // each is filed once, under the first reason.
            flagged(
                "both",
                Verdict::Unsupported("a subquery".into()),
                true,
                true,
            ),
            flagged(
                "sub-feat",
                Verdict::Unsupported("a subquery".into()),
                false,
                true,
            ),
            flagged("pass", Verdict::Pass, false, false),
            flagged("pass-sub", Verdict::Pass, false, true),
            flagged("gated", Verdict::Gated("flag".into()), false, false),
        ]
    }

    #[test]
    fn every_failing_eval_gets_one_reason_and_first_match_wins() {
        let by_id: Vec<_> = reasoned().iter().map(reason).collect();
        assert_eq!(
            by_id,
            [
                Some(Reason::NativeHistograms),
                Some(Reason::Subquery),
                Some(Reason::MissingFeature),
                Some(Reason::Unattributed),
                Some(Reason::NativeHistograms),
                Some(Reason::Subquery),
                None,
                None,
                None,
            ]
        );
        assert_eq!(missing_features(&reasoned()).len(), 1);
    }

    #[test]
    fn the_core_denominator_ignores_whether_an_excluded_eval_passes() {
        let stats = per_file(&reasoned());
        let f = &stats[0];
        // gated is outside every count; the passing subquery eval is
        // excluded from the core but stays in the headline passes.
        assert_eq!((f.evals, f.passed), (8, 2));
        assert_eq!((f.histograms, f.subqueries), (2, 3));
        assert_eq!((f.core_evals(), f.core_passed), (3, 1));

        let md = inventory_markdown(&reasoned());
        assert!(md.contains("**1 of 3** pass (33.3%)"), "{md}");
        assert!(md.contains("| not passing | 2 |"), "{md}");
        assert!(md.contains("| 2 | native histograms |"), "{md}");
        assert!(md.contains("| 2 | subquery |"), "{md}");
        assert!(md.contains("| 1 | missing features |"), "{md}");
        assert!(md.contains("| 1 | unattributed |"), "{md}");
        assert!(md.contains("### f (1)\n\n- `rest`"), "{md}");
    }

    #[test]
    fn a_code_span_outgrows_backticks_in_the_text() {
        assert_eq!(code_span("a"), "`a`");
        assert_eq!(code_span("a`b"), "``a`b``");
        assert_eq!(code_span("`a"), "`` `a ``");
    }
}
