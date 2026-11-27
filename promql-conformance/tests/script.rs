//! The `.test` script parser, against the corpus it has to read.
//!
//! Before an engine is involved at all, this asserts we understand the
//! format: every vendored file parses, and the directive counts match
//! what upstream's own regexes find. A parser that silently skipped a
//! third of the corpus would otherwise look exactly like an engine that
//! passes a third of it.
//!
//! The numbers below were measured against
//! prometheus/prometheus@ccdfcea8 with `grep`. They are expected to
//! change when the pin moves — that is the point of pinning.

use std::collections::BTreeMap;
use std::sync::OnceLock;

use promql_conformance::prometheus::script::{self, Command, Expected, Match, Script, Timing};

/// Parsed once and shared: every test below reads the whole corpus, and
/// re-parsing 428 KB thirteen times is most of this suite's runtime.
fn corpus() -> &'static [Script] {
    static CORPUS: OnceLock<Vec<Script>> = OnceLock::new();
    CORPUS.get_or_init(|| script::load_corpus().expect("the vendored corpus parses"))
}

#[test]
fn every_vendored_file_parses() {
    let scripts = corpus();
    assert_eq!(scripts.len(), 23, "vendored file count");

    // Nothing silently empty: every file has at least one eval.
    for s in scripts {
        assert!(
            s.evals().next().is_some(),
            "{} parsed to zero evals",
            s.name
        );
    }
}

#[test]
fn eval_counts_match_upstream() {
    let scripts = corpus();
    let mut per_file: BTreeMap<&str, usize> = BTreeMap::new();
    for s in scripts {
        per_file.insert(s.name.as_str(), s.evals().count());
    }

    // Counted with `grep -cE '^\s*eval'` over the vendored files.
    let expected: &[(&str, usize)] = &[
        ("aggregators", 167),
        ("at_modifier", 78),
        ("bitwise", 87),
        ("collision", 2),
        ("duration_expression", 65),
        ("extended_vectors", 174),
        ("fill-modifier", 50),
        ("functions", 541),
        ("histograms", 195),
        ("info", 70),
        ("limit", 37),
        ("literals", 25),
        ("name_label_dropping", 30),
        ("native_histograms", 557),
        ("operators", 214),
        ("range_queries", 18),
        ("selectors", 31),
        ("staleness", 17),
        ("start_timestamps", 40),
        ("subquery", 41),
        ("trig_functions", 21),
        ("type_and_unit", 65),
        ("unquoted_utf8_names", 27),
    ];

    for (name, want) in expected {
        assert_eq!(per_file.get(name), Some(want), "{name}.test eval count");
    }

    let total: usize = per_file.values().sum();
    assert_eq!(total, 2552, "total evals across the corpus");
}

#[test]
fn instant_and_range_split_matches_upstream() {
    let scripts = corpus();
    let mut instant = 0;
    let mut range = 0;
    for e in scripts.iter().flat_map(Script::evals) {
        match e.timing {
            Timing::Instant { .. } => instant += 1,
            Timing::Range(_) => range += 1,
        }
    }
    // 2,166 `eval instant` + 5 `eval_fail instant` + 1 `eval_warn instant`,
    // and 380 `eval range`.
    assert_eq!(instant, 2172, "instant evals");
    assert_eq!(range, 380, "range evals");
}

#[test]
fn load_blocks_and_clears_are_in_source_order() {
    let scripts = corpus();
    let loads: usize = scripts.iter().map(|s| s.loads().count()).sum();
    let clears: usize = scripts
        .iter()
        .flat_map(|s| &s.commands)
        .filter(|c| matches!(c, Command::Clear))
        .count();
    assert_eq!(loads, 346, "load blocks");
    assert_eq!(clears, 263, "clear directives");

    // The accumulate-then-clear shape is the thing the runner depends
    // on, so assert a file that genuinely exercises it: histograms.test
    // loads far more often than it clears.
    let histograms = scripts
        .iter()
        .find(|s| s.name == "histograms")
        .expect("histograms.test");
    assert!(
        histograms.loads().count() > 30
            && histograms
                .commands
                .iter()
                .filter(|c| matches!(c, Command::Clear))
                .count()
                < 10,
        "histograms.test should stack many loads against few clears"
    );
}

#[test]
fn expect_assertions_are_recorded() {
    let scripts = corpus();
    let mut fail = 0;
    let mut ordered = 0;
    let mut annotations = 0;
    let mut with_message = 0;

    for e in scripts.iter().flat_map(Script::evals) {
        if e.expect.expects_failure() {
            fail += 1;
        }
        if e.expect.ordered {
            ordered += 1;
        }
        if e.expect.has_annotation_assertions() {
            annotations += 1;
        }
        let messaged = e
            .expect
            .warn
            .iter()
            .chain(e.expect.info.iter())
            .chain(e.expect.fail.iter())
            .filter(|m| !matches!(m, Match::Any))
            .count();
        with_message += messaged;
    }

    // 32 `expect fail` plus the 5 legacy `eval_fail` prefixes.
    assert_eq!(fail, 37, "evals expecting failure");
    assert_eq!(ordered, 24, "evals expecting ordered results");
    assert!(
        annotations > 400,
        "expected hundreds of annotation assertions, got {annotations}"
    );
    assert!(
        with_message > 50,
        "expected msg:/regex: qualifiers to survive parsing, got {with_message}"
    );
}

/// Every row the series parser rejects is a known gap, and each gap
/// stays in the files that need it. Native histograms turn up on both
/// sides, in `load` lines and in expected results, so both are counted.
/// A [`script::Unsupported::ParseError`] anywhere is a parser bug.
#[test]
fn every_unreadable_row_is_a_known_gap() {
    use script::Unsupported::{NativeHistogram, ParseError, StartTimestamp, UnquotedUtf8Name};

    let scripts = corpus();
    let mut histograms_in_loads = 0;
    let mut histograms_in_expectations = 0;
    let mut misplaced = Vec::new();

    for s in scripts {
        let allowed = |reason| match reason {
            NativeHistogram => true,
            StartTimestamp => s.name == "start_timestamps",
            UnquotedUtf8Name => s.name == "unquoted_utf8_names",
            ParseError => false,
        };
        for u in s.loads().flat_map(|l| l.block.unsupported()) {
            histograms_in_loads += usize::from(u.reason == NativeHistogram);
            if !allowed(u.reason) {
                misplaced.push((s.name.clone(), u.reason, u.text.clone()));
            }
        }
        for u in s.evals().flat_map(|e| &e.unsupported) {
            histograms_in_expectations += usize::from(u.reason == NativeHistogram);
            if !allowed(u.reason) {
                misplaced.push((s.name.clone(), u.reason, u.text.clone()));
            }
        }
    }

    assert!(
        histograms_in_loads > 0,
        "native-histogram load lines should be recorded"
    );
    assert!(
        histograms_in_expectations > 0,
        "native-histogram expected rows should be recorded"
    );
    assert!(
        misplaced.is_empty(),
        "{} row(s) rejected outside a known gap: {misplaced:#?}",
        misplaced.len()
    );
}

#[test]
fn scalar_string_and_range_vector_expectations_are_parsed() {
    let scripts = corpus();
    let mut scalars = 0;
    let mut strings = 0;
    let mut range_vectors = 0;
    for e in scripts.iter().flat_map(Script::evals) {
        match e.expected {
            Expected::Scalar(_) => scalars += 1,
            Expected::Str(_) => strings += 1,
            _ => {}
        }
        if e.range_vector.is_some() {
            range_vectors += 1;
        }
    }

    assert_eq!(scalars, 105, "bare scalar results");
    assert_eq!(strings, 5, "`expect string` rows");
    // Twelve, not the thirteen a grep for the phrase reports: range_queries.test:79
    // is a comment that names the directive in prose. Comments are blanked
    // before dispatch, which is why the two numbers differ.
    assert_eq!(range_vectors, 12, "`expect range vector` rows");

    // literals.test is small enough to account for completely, which is
    // what makes the totals above more than magic numbers: 25 evals,
    // every one of them a scalar or a string and nothing else.
    let literals = scripts
        .iter()
        .find(|s| s.name == "literals")
        .expect("literals.test");
    let mut lit_scalars = 0;
    let mut lit_strings = 0;
    for e in literals.evals() {
        match e.expected {
            Expected::Scalar(_) => lit_scalars += 1,
            Expected::Str(_) => lit_strings += 1,
            ref other => panic!("literals.test carries an unexpected shape: {other:?}"),
        }
    }
    assert_eq!((lit_scalars, lit_strings), (20, 5));
    assert_eq!(lit_scalars + lit_strings, literals.evals().count());
}

/// Every spelling of a non-finite literal in the corpus survives the
/// round trip. Go writes `+Inf`/`-Inf`/`NaN`; a parser that quietly
/// turned one of those into `None` would drop assertions rather than
/// fail them.
#[test]
fn non_finite_scalars_parse() {
    let literals = corpus()
        .iter()
        .find(|s| s.name == "literals")
        .expect("literals.test");

    let mut pos_inf = 0;
    let mut neg_inf = 0;
    let mut nan = 0;
    for e in literals.evals() {
        if let Expected::Scalar(v) = e.expected {
            if v.is_nan() {
                nan += 1;
            } else if v == f64::INFINITY {
                pos_inf += 1;
            } else if v == f64::NEG_INFINITY {
                neg_inf += 1;
            }
        }
    }
    assert_eq!((pos_inf, neg_inf, nan), (4, 2, 4));
}

// ---------------- the format, on inputs we control ----------------

#[test]
fn an_eval_sees_only_the_loads_above_it() {
    let s = script::parse(
        "synthetic",
        "\
load 1m
  a 1

eval instant at 1m a
  a 1

load 1m
  b 2

clear

load 1m
  c 3

eval instant at 1m c
  c 3
",
    )
    .expect("parses");

    // Four commands before the second eval, in this order.
    assert!(matches!(s.commands[0], Command::Load(_)));
    assert!(matches!(s.commands[1], Command::Eval(_)));
    assert!(matches!(s.commands[2], Command::Load(_)));
    assert!(matches!(s.commands[3], Command::Clear));
    assert!(matches!(s.commands[4], Command::Load(_)));
    assert!(matches!(s.commands[5], Command::Eval(_)));
    assert_eq!(s.commands.len(), 6);
}

#[test]
fn comments_and_indentation_carry_no_meaning() {
    let s = script::parse(
        "synthetic",
        "\
# a leading comment
\t\tload 1m
\t\t\ta 1

  # an indented comment
  eval instant at 1m a
\t{__name__=\"a\"} 1
",
    )
    .expect("parses");
    assert_eq!(s.loads().count(), 1);
    assert_eq!(s.evals().count(), 1);
}

#[test]
fn bare_zero_is_a_valid_offset() {
    let s = script::parse("synthetic", "eval instant at 0 vector(1)\n  {} 1\n").expect("parses");
    let e = s.evals().next().unwrap();
    assert_eq!(e.timing, Timing::Instant { at_ms: 0 });

    let s = script::parse(
        "synthetic",
        "eval range from 0 to 1m step 30s vector(1)\n  {} 1 1 1\n",
    )
    .expect("parses");
    let e = s.evals().next().unwrap();
    assert_eq!(
        e.timing,
        Timing::Range(script::Range {
            start_ms: 0,
            end_ms: 60_000,
            step_ms: 30_000,
        })
    );
}

#[test]
fn an_unknown_directive_is_an_error() {
    let err = script::parse("synthetic", "frobnicate 1m\n").unwrap_err();
    assert!(
        err.to_string().contains("not a command"),
        "unexpected error: {err}"
    );
}

#[test]
fn multiple_values_under_an_instant_eval_are_rejected() {
    let err = script::parse("synthetic", "eval instant at 1m a\n  a 1 2 3\n").unwrap_err();
    assert!(
        err.to_string().contains("one value per series"),
        "unexpected error: {err}"
    );
}
