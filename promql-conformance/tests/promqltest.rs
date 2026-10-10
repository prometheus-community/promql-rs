//! Prometheus's promqltest corpus, replayed against the Rust engine.
//!
//! 2,098 evals across 20 vendored `.test` files, each carrying its own
//! expected values. Unlike [the differential suite](differential.rs)
//! there is nothing to ask at runtime — no oracle, no Go, no network —
//! so this is the conformance suite that actually runs in CI.
//!
//! # What green means
//!
//! Everything named in `testdata/prometheus/SUPPORTED.toml` still
//! passes. That file is the list of what this engine is expected to do
//! *right now*, and nothing on it may ever stop working.
//!
//! It is an allowlist, so the rest of the corpus runs without gating
//! anything. Three things turn CI red: a listed eval stops passing, an
//! unlisted eval starts passing (declare it), or a listed eval stops
//! existing after an upstream re-pin.
//!
//! # Seeing where we stand
//!
//! Just run it. The per-file scoreboard and the missing-feature table
//! print on every run, gated or not — a green tick never hides how much
//! of PromQL is left.
//!
//! ```sh
//! cargo test -p promql-conformance --test promqltest
//!
//! # every eval as its own trial, failing as it really is
//! PROMQL_PROMQLTEST_ALL=1 cargo test -p promql-conformance --test promqltest
//!
//! # ...narrowed to one file, or one feature you are implementing
//! PROMQL_PROMQLTEST_ALL=1 cargo test -p promql-conformance --test promqltest -- operators/
//! PROMQL_PROMQLTEST_ALL=1 cargo test -p promql-conformance --test promqltest -- topk
//! ```
//!
//! `PROMQL_PROMQLTEST_BLESS=1` rewrites `SUPPORTED.toml` and
//! `UNSUPPORTED.md` from what actually passes.
//!
//! # What is not asserted
//!
//! `expect warn` / `info` / `no_warn` / `no_info` — 555 evals — need an
//! annotation channel the engine does not have. They are parsed, counted
//! and reported, but not checked; the count is printed so the gap stays
//! visible rather than silently passing.

use std::io::ErrorKind;
use std::process::ExitCode;
use std::sync::OnceLock;

use libtest_mimic::{Arguments, Failed, Trial};
use promql_conformance::prometheus::supported::{self, Supported};
use promql_conformance::prometheus::{
    inventory_markdown, load_corpus, run_script, scoreboard, Outcome, Verdict,
};
use promql_conformance::DataFusionEngine;

fn main() -> ExitCode {
    let args = Arguments::from_args();

    let corpus = match load_corpus() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("cannot load the promqltest corpus: {e}");
            return ExitCode::FAILURE;
        }
    };

    let outcomes: Vec<Outcome> = corpus
        .iter()
        .flat_map(|script| run_script(engine(), script))
        .collect();

    // Read it even when blessing: the "all" batches in it are human
    // choices that a bless has to preserve rather than overwrite.
    let declared = match Supported::load(&supported::path()) {
        Ok(s) => s,
        // A bless can create the file, but must not paper over a
        // corrupt one: losing the "all" batches in it would silently
        // weaken the gate.
        Err(supported::Error::Read { source, .. })
            if supported::bless_requested() && source.kind() == ErrorKind::NotFound =>
        {
            eprintln!("no {} yet — creating it", supported::FILE_NAME);
            Supported::default()
        }
        Err(e) => {
            eprintln!("cannot read the supported list: {e}");
            return ExitCode::FAILURE;
        }
    };

    eprint!("{}", scoreboard(&outcomes, &declared));
    annotations_note(&outcomes);

    if supported::bless_requested() {
        return bless(&outcomes, &declared);
    }

    let trials = if all_requested() {
        every_eval(&outcomes)
    } else {
        gated(&declared, &outcomes)
    };

    libtest_mimic::run(&args, trials).exit_code()
}

/// Rewrite both generated files from this run.
fn bless(outcomes: &[Outcome], previous: &Supported) -> ExitCode {
    let (next, downgrades) = Supported::from_outcomes(outcomes, previous);

    for warning in &downgrades {
        eprintln!("  warning: {warning}");
    }

    if let Err(e) = next.save(&supported::path()) {
        eprintln!("cannot write the supported list: {e}");
        return ExitCode::FAILURE;
    }

    let inventory = supported::path().with_file_name("UNSUPPORTED.md");
    if let Err(e) = std::fs::write(&inventory, inventory_markdown(outcomes)) {
        eprintln!("cannot write {}: {e}", inventory.display());
        return ExitCode::FAILURE;
    }

    eprintln!(
        "\nwrote {} declared case(s) to {}\nwrote {}",
        next.len(),
        supported::path().display(),
        inventory.display(),
    );
    ExitCode::SUCCESS
}

/// One trial per declared eval, plus one per violation.
///
/// Undeclared evals get no trial. Registering several hundred
/// permanently-red ones would bury the few that mean something, and they
/// are already accounted for in the scoreboard above.
fn gated(declared: &Supported, outcomes: &[Outcome]) -> Vec<Trial> {
    let violations = supported::check(declared, outcomes);

    // A violation and a passing trial can collide on one eval — an
    // unlisted case that passes is both. The violation is the one to
    // report, and two trials cannot share a name.
    let flagged: std::collections::BTreeSet<String> =
        violations.iter().map(|v| v.trial_name()).collect();

    let mut trials: Vec<Trial> = outcomes
        .iter()
        .filter(|o| o.verdict.is_pass() && declared.covers(&o.file, &o.id))
        .map(|o| format!("{}/{}", o.file, o.id))
        .filter(|name| !flagged.contains(&format!("SUPPORTED {name}")))
        .map(|name| Trial::test(name, || Ok(())))
        .collect();

    trials.extend(violations.into_iter().map(|v| {
        let name = v.trial_name();
        Trial::test(name, move || Err(Failed::from(v.to_string())))
    }));

    trials
}

/// Every eval on its own, allowlist ignored: the view for working on the
/// engine rather than for guarding it.
fn every_eval(outcomes: &[Outcome]) -> Vec<Trial> {
    outcomes
        .iter()
        .filter(|o| !matches!(o.verdict, Verdict::Skipped(_) | Verdict::Gated { .. }))
        .map(|o| {
            let name = format!("{}/{}", o.file, o.id);
            let at = format!("{}.test:{}", o.file, o.line);
            let query = o.query.clone();
            let verdict = o.verdict.clone();
            Trial::test(name, move || match verdict {
                Verdict::Pass => Ok(()),
                Verdict::Fail(detail) => Err(Failed::from(format!(
                    "{detail}\n  query: {query}\n  at:    {at}"
                ))),
                Verdict::Unsupported(feature) => Err(Failed::from(format!(
                    "{feature} is not supported yet\n  query: {query}\n  at:    {at}"
                ))),
                Verdict::Skipped(_) | Verdict::Gated { .. } => Ok(()),
            })
        })
        .collect()
}

/// Say how many assertions we parse but do not check, so the gap stays
/// visible instead of reading as coverage.
fn annotations_note(outcomes: &[Outcome]) {
    let unchecked = outcomes.iter().filter(|o| o.unchecked_annotations).count();
    if unchecked > 0 {
        eprintln!(
            "\n{unchecked} evals carry warn/info assertions that are parsed but not \
             checked — the engine has no annotation channel yet."
        );
    }
}

/// Ignore the allowlist and give every eval its own trial.
const ALL_ENV: &str = "PROMQL_PROMQLTEST_ALL";

fn all_requested() -> bool {
    std::env::var_os(ALL_ENV).is_some_and(|v| !v.is_empty() && v != "0")
}

fn engine() -> &'static DataFusionEngine {
    static ENGINE: OnceLock<DataFusionEngine> = OnceLock::new();
    ENGINE.get_or_init(|| DataFusionEngine::new().expect("the engine constructs"))
}
