//! Prometheus's own promqltest corpus, replayed against our engine.
//!
//! The 23 `.test` files under `testdata/prometheus/` are the largest
//! specification of PromQL behaviour that exists: 2,552 assertions, each
//! carrying its expected values inline. That last part is what makes
//! this suite different from [`crate::thanos`] — there is nothing to
//! ask, so it needs no oracle, no Go toolchain and no network, and it
//! therefore runs everywhere, including CI.
//!
//! Upstream parses these files in two layers (`promql/promqltest/
//! test.go`): a hand-written line scanner for the directives, and the
//! real PromQL grammar for everything inside a block. There is no
//! grammar file for the script format itself. We mirror that split —
//! [`script`] is the line scanner, and the series lines and expected
//! rows go through `promql_parser::parse_series_desc`, which is already
//! cross-checked against this very corpus.

//! [`run`] walks a parsed script against an [`crate::result::Engine`]
//! and reports one [`run::Outcome`] per eval. [`supported`] declares
//! which of those must pass — the gate — and [`report`] says how much of
//! the corpus is left, which is a different question and not a gate.

pub mod report;
pub mod run;
pub mod script;
pub mod supported;

pub use report::{inventory_markdown, scoreboard, FileStats};
pub use run::{almost_equal, run_script, Outcome, Verdict};
pub use script::{load_corpus, Command, Eval, Expect, Expected, Script, Timing};
pub use supported::{Batch, Supported, Violation};
