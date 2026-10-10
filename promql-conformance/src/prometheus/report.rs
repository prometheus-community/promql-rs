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
    /// With `subqueries` and the core, partitions `evals` by [`Category`],
    /// so the columns add up.
    pub histograms: usize,
    pub subqueries: usize,
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

/// The part of PromQL an eval exercises, exactly one per eval.
///
/// Taken from the eval and never from its verdict, so an eval that starts
/// passing moves a numerator and no denominator. The core is the target;
/// the other three are feature areas parked on purpose, and an eval in one
/// of them says nothing about the rest of the language.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Category {
    FlagGated,
    NativeHistograms,
    Subquery,
    Core,
}

/// First match wins, in the order of [`Category`]: a gated eval is held
/// out of the core totals and `SUPPORTED.toml` whatever else it touches.
pub fn category(o: &Outcome) -> Category {
    if matches!(o.verdict, Verdict::Gated { .. }) {
        Category::FlagGated
    } else if o.histograms {
        Category::NativeHistograms
    } else if o.subquery {
        Category::Subquery
    } else {
        Category::Core
    }
}

/// Whether the eval passed, looking through a gate.
fn passed(o: &Outcome) -> bool {
    match &o.verdict {
        Verdict::Gated { verdict, .. } => verdict.is_pass(),
        v => v.is_pass(),
    }
}

/// Why a core eval fails. Only the core gets a reason: the other
/// categories already say what an eval is waiting on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reason {
    MissingFeature,
    Unattributed,
}

/// `None` for a pass and for every eval outside the core.
pub fn reason(o: &Outcome) -> Option<Reason> {
    if category(o) != Category::Core {
        return None;
    }
    match o.verdict {
        Verdict::Pass => None,
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
        match category(o) {
            Category::FlagGated => {
                stats.gated += 1;
                continue;
            }
            Category::NativeHistograms => stats.histograms += 1,
            Category::Subquery => stats.subqueries += 1,
            Category::Core if o.verdict.is_pass() => stats.core_passed += 1,
            Category::Core => {}
        }
        stats.evals += 1;
        match o.verdict {
            Verdict::Pass => stats.passed += 1,
            Verdict::Skipped(_) => stats.skipped += 1,
            _ => {}
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

/// Passing and total evals in one category.
pub fn category_counts(outcomes: &[Outcome], c: Category) -> (usize, usize) {
    let evals = outcomes.iter().filter(|o| category(o) == c);
    (evals.clone().filter(|o| passed(o)).count(), evals.count())
}

/// Passing and total gated evals per flag the parser knows, in its order.
/// An eval needing two flags counts under both, so the rows can sum to
/// more than the gated total.
pub fn gated_by_flag(outcomes: &[Outcome]) -> Vec<(&'static str, usize, usize)> {
    promql_parser::FEATURE_FLAGS
        .iter()
        .map(|f| {
            let evals = outcomes.iter().filter(|o| match &o.verdict {
                Verdict::Gated { flags, .. } => flags.split(", ").any(|x| x == f.flag),
                _ => false,
            });
            (
                f.flag,
                evals.clone().filter(|o| passed(o)).count(),
                evals.count(),
            )
        })
        .collect()
}

/// Missing engine features, and how many core evals each blocks, so the
/// rows sum to the reasons table.
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
    let stats = per_file(outcomes);
    let passed: usize = stats.iter().map(|s| s.passed).sum();
    let total: usize = stats.iter().map(|s| s.evals).sum();
    let (core_passed, core_total) = category_counts(outcomes, Category::Core);
    let (hist_passed, hist_total) = category_counts(outcomes, Category::NativeHistograms);
    let (sub_passed, sub_total) = category_counts(outcomes, Category::Subquery);
    let (gated_passed, gated_total) = category_counts(outcomes, Category::FlagGated);

    let mut out = String::new();
    out.push_str(
        "# What the engine is missing\n\n\
         Generated — do not edit by hand. Regenerated alongside `SUPPORTED.toml` by:\n\n\
         ```sh\n\
         PROMQL_PROMQLTEST_BLESS=1 cargo test -p promql-conformance --test promqltest\n\
         ```\n\n\
         Nothing here gates CI. This file is the roadmap, and the core line below is \
         the target: it reads 100% when the engine passes every eval that is neither \
         native histograms, subquery nor experimental syntax. \
         An eval is native-histogram if an expected row is a native histogram sample \
         or a selector in its query matches, by its label matchers, a series the active \
         load blocks load with native histogram samples (`{{…}}`, or derived by \
         `load_with_nhcb`). Each eval is in exactly one category, first match wins: \
         flag-gated, native histograms, subquery, core. The category comes from the \
         eval, never from whether it passes, so the denominators hold still.\n\n",
    );

    // `scripts/progress` reads the first bold `N of M evals pass`, and only
    // the total line may match it: the series it rebuilds started there.
    let _ = write!(
        out,
        "**Core: {core_passed} of {core_total} evals pass ({:.1}%).**\n\n\
         - Native histograms: {hist_passed} of {hist_total} pass.\n\
         - Subqueries: {sub_passed} of {sub_total} pass.\n\
         - Experimental (flag-gated): {gated_passed} of {gated_total} pass, run with \
         every gate open and never in `SUPPORTED.toml`; `docs/feature-flags.md` lists \
         the flags.\n",
        percent(core_passed, core_total),
    );
    let by_flag = gated_by_flag(outcomes);
    for (flag, p, t) in &by_flag {
        let _ = writeln!(out, "  - `{flag}`: {p} of {t}");
    }
    if by_flag.iter().map(|f| f.2).sum::<usize>() > gated_total {
        out.push_str("  - an eval needing two flags counts under both\n");
    }
    let _ = writeln!(
        out,
        "\nAll but the flag-gated: **{passed} of {total} evals pass** ({:.1}%).\n",
        percent(passed, total),
    );

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

    let count_of = |r| outcomes.iter().filter(|o| reason(o) == Some(r)).count();
    let missing = count_of(Reason::MissingFeature);
    let unattributed = count_of(Reason::Unattributed);
    let _ = write!(
        out,
        "## Why core evals fail\n\n\
         Each failing core eval is filed under a missing feature the engine names, or \
         left unattributed: what is left there is the work nobody has named yet.\n\n\
         | failing evals | reason |\n|---:|---|\n\
         | {missing} | missing features |\n\
         | {unattributed} | unattributed |\n\
         | {} | **total** |\n\n",
        missing + unattributed,
    );

    out.push_str("## Missing features\n\n| evals blocked | feature |\n|---:|---|\n");
    let mut rows: Vec<(&str, usize)> = missing_features(outcomes).into_iter().collect();
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
             Core evals the engine does not name a missing feature for: wrong results, \
             panics, errors nobody has classified. Ids are spelled as in \
             `SUPPORTED.toml`.\n",
        );
        for (file, ids) in &unattributed {
            let _ = write!(out, "\n### {file} ({})\n\n", ids.len());
            for id in ids {
                let _ = writeln!(out, "- {}", code_span(id));
            }
        }
    }

    out.push_str(
        "\n## By file\n\n\
         | file | evals | native histograms | subquery | flag-gated | passing | passing core | |\n\
         |---|---:|---:|---:|---:|---:|---:|---|\n",
    );
    let mut stats = stats;
    // Closest to a green core first; a file with no core evals has
    // nothing to say about it and goes last.
    stats.sort_by(|a, b| {
        (b.core_evals() > 0)
            .cmp(&(a.core_evals() > 0))
            .then_with(|| b.core_percent().total_cmp(&a.core_percent()))
            .then_with(|| b.core_evals().cmp(&a.core_evals()))
            .then_with(|| a.file.cmp(&b.file))
    });
    for s in &stats {
        let note = if s.core_evals() == 0 {
            "no core evals"
        } else if s.core_passed == s.core_evals() {
            "core green"
        } else {
            ""
        };
        let _ = writeln!(
            out,
            "| {} | {} | {} | {} | {} | {} ({:.0}%) | {} of {} ({:.0}%) | {note} |",
            s.file,
            s.evals,
            s.histograms,
            s.subqueries,
            s.gated,
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

    fn gated(flags: &str, verdict: Verdict) -> Verdict {
        Verdict::Gated {
            flags: flags.into(),
            verdict: Box::new(verdict),
        }
    }

    #[test]
    fn gated_evals_are_in_no_total_and_are_counted_per_flag() {
        let mut outcomes = sample();
        outcomes.push(outcome(
            "fill",
            "1",
            gated("promql-binop-fill-modifiers", Verdict::Pass),
        ));
        outcomes.push(outcome(
            "half",
            "9",
            gated(
                "promql-binop-fill-modifiers, promql-extended-range-selectors",
                Verdict::Fail("x".into()),
            ),
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
        assert!(
            md.contains("Experimental (flag-gated): 1 of 2 pass"),
            "{md}"
        );
        assert!(md.contains("`promql-binop-fill-modifiers`: 1 of 2"), "{md}");
        assert!(
            md.contains("`promql-extended-range-selectors`: 0 of 1"),
            "{md}"
        );
        assert!(md.contains("`promql-duration-expr`: 0 of 0"), "{md}");
        assert!(md.contains("counts under both"), "{md}");
        assert!(
            md.contains("| fill | 0 | 0 | 0 | 1 | 0 (0%) | 0 of 0 (0%) | no core evals |"),
            "{md}"
        );
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
    }

    /// The headline's total is the only text `scripts/progress` may read
    /// as the pass count; a category line it matched first would rewrite
    /// the series it rebuilds from git history.
    #[test]
    fn only_the_total_line_reads_as_the_progress_count() {
        let md = inventory_markdown(&categorised());
        let re = regex::Regex::new(r"\*\*([0-9]+) of ([0-9]+) evals pass\*\*").unwrap();
        let found: Vec<_> = re.find_iter(&md).map(|m| m.as_str()).collect();
        assert_eq!(found, ["**2 of 7 evals pass**"], "{md}");
    }

    fn flagged(id: &str, verdict: Verdict, histograms: bool, subquery: bool) -> Outcome {
        Outcome {
            histograms,
            subquery,
            ..outcome("f", id, verdict)
        }
    }

    /// One eval per category, plus the ways precedence can go wrong: a
    /// gated eval with histograms and a subquery, and an eval with both.
    fn categorised() -> Vec<Outcome> {
        vec![
            flagged("gated", gated("x", Verdict::Pass), true, true),
            flagged("hist", Verdict::Fail("x".into()), true, false),
            flagged("both", Verdict::Pass, true, true),
            flagged("sub", Verdict::Fail("x".into()), false, true),
            flagged(
                "feat",
                Verdict::Unsupported("the frob function".into()),
                false,
                false,
            ),
            flagged("rest", Verdict::Skipped("x".into()), false, false),
            flagged("pass", Verdict::Pass, false, false),
            // Excluded from the core, so its missing feature is not one
            // of the core's.
            flagged(
                "sub-feat",
                Verdict::Unsupported("a subquery".into()),
                false,
                true,
            ),
        ]
    }

    #[test]
    fn every_eval_gets_one_category_and_first_match_wins() {
        let got: Vec<_> = categorised().iter().map(category).collect();
        use Category::*;
        assert_eq!(
            got,
            [
                FlagGated,
                NativeHistograms,
                NativeHistograms,
                Subquery,
                Core,
                Core,
                Core,
                Subquery
            ]
        );
        let reasons: Vec<_> = categorised().iter().map(reason).collect();
        assert_eq!(
            reasons,
            [
                None,
                None,
                None,
                None,
                Some(Reason::MissingFeature),
                Some(Reason::Unattributed),
                None,
                None,
            ]
        );
        assert_eq!(missing_features(&categorised()).len(), 1);
    }

    #[test]
    fn each_category_counts_its_passes_against_its_own_evals() {
        let outcomes = categorised();
        let counts = |c| category_counts(&outcomes, c);
        assert_eq!(counts(Category::FlagGated), (1, 1));
        assert_eq!(counts(Category::NativeHistograms), (1, 2));
        assert_eq!(counts(Category::Subquery), (0, 2));
        assert_eq!(counts(Category::Core), (1, 3));

        let f = &per_file(&outcomes)[0];
        assert_eq!((f.evals, f.passed, f.gated), (7, 2, 1));
        assert_eq!((f.histograms, f.subqueries), (2, 2));
        assert_eq!((f.core_evals(), f.core_passed), (3, 1));

        let md = inventory_markdown(&outcomes);
        assert!(md.contains("**Core: 1 of 3 evals pass (33.3%).**"), "{md}");
        assert!(md.contains("- Native histograms: 1 of 2 pass."), "{md}");
        assert!(md.contains("- Subqueries: 0 of 2 pass."), "{md}");
        assert!(
            md.contains("Experimental (flag-gated): 1 of 1 pass"),
            "{md}"
        );
        assert!(md.contains("| not passing | 2 |"), "{md}");
        assert!(md.contains("| 1 | missing features |"), "{md}");
        assert!(md.contains("| 1 | unattributed |"), "{md}");
        assert!(md.contains("### f (1)\n\n- `rest`"), "{md}");
        assert!(
            md.contains("| f | 7 | 2 | 2 | 1 | 2 (29%) | 1 of 3 (33%) |  |"),
            "{md}"
        );
    }

    #[test]
    fn a_code_span_outgrows_backticks_in_the_text() {
        assert_eq!(code_span("a"), "`a`");
        assert_eq!(code_span("a`b"), "``a`b``");
        assert_eq!(code_span("`a"), "`` `a ``");
    }
}
