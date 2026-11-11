//! Conformance harness: runs every case from upstream's parse_test.go
//! corpus (materialised to `tests/fixtures/parse_test_corpus.json` by
//! `scripts/gen-conformance`) through our parser and verifies that
//! parse success / failure matches upstream.
//!
//! The pass-rate floor below (`MIN_PASS_RATE`) reflects current grammar
//! coverage, not a target ceiling. The gap is expected in areas the
//! grammar hasn't yet ported — vector-matching modifiers, series
//! descriptions, histogram descriptors, experimental productions. This
//! harness gives maintainers one number to track as those land.

use std::collections::HashSet;
use std::path::PathBuf;

use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Corpus {
    #[serde(default)]
    upstream_sha: String,
    cases: Vec<Case>,
}

#[derive(Debug, Deserialize)]
struct Case {
    input: String,
    #[serde(default)]
    fail: bool,
    #[serde(default)]
    err_msg: String,
}

/// Conformance pass-rate floor.
///
/// Set below the eventual target to reflect honest first-pass coverage
/// on the full corpus (no numeric/histogram split in the generator yet
/// — follow-up). The gap comes from three categories of grammar not
/// yet ported:
///   - Vector-matching modifiers (bool, on, ignoring, group_left,
///     group_right).
///   - Series descriptions, in the corpus generator rather than the
///     parser: they parse, native-histogram descriptors included, but
///     this fixture holds expressions.
///   - Experimental productions (fill/trim_upper/_lower/anchored/
///     smoothed/duration-expr arithmetic) — tracked against upstream.
///
/// Tighten as each lands. Long-term target is ≥95% on the full corpus.
const MIN_PASS_RATE: f64 = 0.55;

/// How many example mismatches to surface on stderr when the gate
/// fails, so a red CI run is immediately diagnosable.
const EXAMPLES_ON_FAIL: usize = 5;

fn corpus_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/parse_test_corpus.json")
}

fn load_corpus() -> Corpus {
    let p = corpus_path();
    if !p.exists() {
        panic!(
            "corpus fixture missing: {}\n\
             Regenerate with:\n\
               curl -sSL -o /tmp/parse_test.go \\\n\
                 https://raw.githubusercontent.com/prometheus/prometheus/<sha>/promql/parser/parse_test.go\n\
               go run ./scripts/gen-conformance -in /tmp/parse_test.go \\\n\
                 -out promql-parser/tests/fixtures/parse_test_corpus.json \\\n\
                 -sha <sha>",
            p.display()
        );
    }
    let data = std::fs::read_to_string(&p).expect("read corpus fixture");
    serde_json::from_str(&data).expect("parse corpus fixture")
}

#[derive(Default, Debug)]
struct Stats {
    total: usize,
    matched: usize,
    false_success: Vec<String>,
    false_failure: Vec<String>,
}

impl Stats {
    fn pass_rate(&self) -> f64 {
        if self.total == 0 {
            0.0
        } else {
            self.matched as f64 / self.total as f64
        }
    }
}

fn run() -> Stats {
    let corpus = load_corpus();
    let mut stats = Stats::default();
    let mut seen_inputs: HashSet<String> = HashSet::new();
    // Upstream's `TestParseExpressions` parses `testExpr` with exactly these
    // two gates on and the other two off; the corpus is that table.
    let parser = promql_parser::Parser::new(promql_parser::ParserOptions {
        enable_experimental_functions: true,
        experimental_duration_expr: true,
        ..Default::default()
    });
    for case in corpus.cases {
        if !seen_inputs.insert(case.input.clone()) {
            // Dedup exact-duplicate cases (upstream has a few).
            continue;
        }
        stats.total += 1;
        let got = parser.parse_expr(&case.input);
        let ours_ok = got.is_ok();
        let expect_ok = !case.fail;
        if ours_ok == expect_ok {
            stats.matched += 1;
        } else if ours_ok && case.fail {
            stats.false_success.push(case.input);
        } else if !ours_ok && !case.fail {
            let _ = case.err_msg;
            stats.false_failure.push(case.input);
        }
    }
    eprintln!(
        "conformance: upstream={sha} total={total} matched={matched} rate={rate:.1}%  \
         false_success={fs} false_failure={ff}",
        sha = corpus.upstream_sha,
        total = stats.total,
        matched = stats.matched,
        rate = stats.pass_rate() * 100.0,
        fs = stats.false_success.len(),
        ff = stats.false_failure.len(),
    );
    stats
}

#[test]
fn meets_conformance_floor() {
    let stats = run();
    if stats.pass_rate() < MIN_PASS_RATE {
        eprintln!("Sample false successes (our parser accepts, upstream rejects):");
        for input in stats.false_success.iter().take(EXAMPLES_ON_FAIL) {
            eprintln!("  {input:?}");
        }
        eprintln!("Sample false failures (our parser rejects, upstream accepts):");
        for input in stats.false_failure.iter().take(EXAMPLES_ON_FAIL) {
            eprintln!("  {input:?}");
        }
        panic!(
            "conformance floor not met: {:.1}% < {:.0}%  ({} matched of {} total; \
             {} false successes, {} false failures)",
            stats.pass_rate() * 100.0,
            MIN_PASS_RATE * 100.0,
            stats.matched,
            stats.total,
            stats.false_success.len(),
            stats.false_failure.len(),
        );
    }
}
