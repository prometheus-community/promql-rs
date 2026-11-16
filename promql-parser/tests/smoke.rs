//! End-to-end smoke tests. Exercise a handful of representative PromQL
//! expressions through the full parser pipeline (lrlex lexer + grmtools
//! parser + action helpers) and assert on the resulting AST.

use promql_parser::ast::{Expr, MatchOp};
use promql_parser::{parse_expr, parse_metric_selector, Parser, ParserOptions};

fn must_parse(q: &str) -> Expr {
    parse_expr(q).unwrap_or_else(|e| panic!("failed to parse {q:?}: {e}"))
}

/// The anchored and smoothed modifiers are experimental syntax, off in
/// [`parse_expr`] as in stock Prometheus; the tests that exercise them
/// switch on the one gate that guards them.
fn extended_range_selectors() -> Parser {
    Parser::new(ParserOptions {
        enable_extended_range_selectors: true,
        ..Default::default()
    })
}

fn must_parse_extended(q: &str) -> Expr {
    extended_range_selectors()
        .parse_expr(q)
        .unwrap_or_else(|e| panic!("failed to parse {q:?}: {e}"))
}

#[test]
fn bare_vector_selector() {
    match must_parse("up") {
        Expr::VectorSelector(vs) => {
            assert_eq!(vs.name, "up");
            assert!(vs.label_matchers.is_empty());
        }
        other => panic!("expected VectorSelector, got {other:?}"),
    }
}

#[test]
fn vector_selector_with_matchers() {
    match must_parse(r#"http_requests_total{job="api", status!~"5.."}"#) {
        Expr::VectorSelector(vs) => {
            assert_eq!(vs.name, "http_requests_total");
            assert_eq!(vs.label_matchers.len(), 2);
            assert_eq!(vs.label_matchers[0].name, "job");
            assert_eq!(vs.label_matchers[0].op, MatchOp::Equal);
            assert_eq!(vs.label_matchers[0].value, "api");
            assert_eq!(vs.label_matchers[1].name, "status");
            assert_eq!(vs.label_matchers[1].op, MatchOp::RegexNotEqual);
            assert_eq!(vs.label_matchers[1].value, "5..");
        }
        other => panic!("expected VectorSelector, got {other:?}"),
    }
}

#[test]
fn arithmetic_precedence() {
    // 2 + 3 * 4 parses as 2 + (3 * 4)
    match must_parse("2 + 3 * 4") {
        Expr::Binary(b) => {
            assert_eq!(b.op, promql_parser::token::ItemType::Add);
            match *b.rhs {
                Expr::Binary(ref inner) => {
                    assert_eq!(inner.op, promql_parser::token::ItemType::Mul);
                }
                other => panic!("expected nested Mul, got {other:?}"),
            }
        }
        other => panic!("expected Binary, got {other:?}"),
    }
}

/// Upstream gives the unary rule `MUL`'s precedence, so a sign binds to
/// the operand and not to the expression around it.
#[test]
fn unary_binds_tighter_than_a_binary_operator() {
    for q in ["-foo + bar", "-foo or bar", "-foo == bar"] {
        match must_parse(q) {
            Expr::Binary(b) => assert!(
                matches!(*b.lhs, Expr::Unary(_)),
                "{q}: the sign belongs to the left operand, got {:?}",
                b.lhs
            ),
            other => panic!("{q}: expected Binary, got {other:?}"),
        }
    }
}

#[test]
fn right_associative_pow() {
    // 2 ^ 3 ^ 4 parses as 2 ^ (3 ^ 4)
    match must_parse("2 ^ 3 ^ 4") {
        Expr::Binary(b) => {
            assert_eq!(b.op, promql_parser::token::ItemType::Pow);
            match *b.rhs {
                Expr::Binary(ref inner) => {
                    assert_eq!(inner.op, promql_parser::token::ItemType::Pow);
                }
                other => panic!("expected nested Pow, got {other:?}"),
            }
        }
        other => panic!("expected Binary, got {other:?}"),
    }
}

#[test]
fn rate_over_matrix() {
    match must_parse("rate(http_requests_total[5m])") {
        Expr::Call(c) => {
            assert_eq!(c.func.name, "rate");
            assert_eq!(c.args.len(), 1);
            match &c.args[0] {
                Expr::MatrixSelector(ms) => {
                    assert_eq!(ms.range_secs, 300.0);
                    assert!(matches!(&*ms.vector_selector, Expr::VectorSelector(_)));
                }
                other => panic!("expected MatrixSelector, got {other:?}"),
            }
        }
        other => panic!("expected Call, got {other:?}"),
    }
}

#[test]
fn aggregate_by() {
    match must_parse(r#"sum by (service) (rate(http_requests_total[5m]))"#) {
        Expr::Aggregate(a) => {
            assert_eq!(a.op, promql_parser::token::ItemType::Sum);
            assert_eq!(a.grouping, vec!["service"]);
            assert!(!a.without);
        }
        other => panic!("expected Aggregate, got {other:?}"),
    }
}

#[test]
fn aggregate_without() {
    match must_parse(r#"max without (instance) (cpu_usage)"#) {
        Expr::Aggregate(a) => {
            assert_eq!(a.op, promql_parser::token::ItemType::Max);
            assert_eq!(a.grouping, vec!["instance"]);
            assert!(a.without);
        }
        other => panic!("expected Aggregate, got {other:?}"),
    }
}

#[test]
fn offset_modifier() {
    match must_parse(r#"foo offset 5m"#) {
        Expr::VectorSelector(vs) => {
            assert_eq!(vs.name, "foo");
            assert_eq!(vs.original_offset_secs, 300.0);
        }
        other => panic!("expected VectorSelector with offset, got {other:?}"),
    }
}

#[test]
fn offset_negative() {
    match must_parse(r#"foo offset -1h"#) {
        Expr::VectorSelector(vs) => {
            assert_eq!(vs.original_offset_secs, -3600.0);
        }
        other => panic!("expected VectorSelector, got {other:?}"),
    }
}

#[test]
fn at_modifier_timestamp() {
    match must_parse(r#"foo @ 1700000000"#) {
        Expr::VectorSelector(vs) => {
            assert_eq!(vs.timestamp, Some(1_700_000_000_000));
        }
        other => panic!("expected VectorSelector with @ modifier, got {other:?}"),
    }
}

#[test]
fn subquery_with_step() {
    match must_parse(r#"rate(foo[5m])[30m:1m]"#) {
        Expr::Subquery(sq) => {
            assert_eq!(sq.range_secs, 1800.0);
            assert_eq!(sq.step_secs, 60.0);
        }
        other => panic!("expected Subquery, got {other:?}"),
    }
}

#[test]
fn paren_preserves_structure() {
    match must_parse("(1 + 2) * 3") {
        Expr::Binary(b) => {
            assert_eq!(b.op, promql_parser::token::ItemType::Mul);
            match *b.lhs {
                Expr::Paren(_) => {}
                other => panic!("expected Paren on LHS, got {other:?}"),
            }
        }
        other => panic!("expected Binary, got {other:?}"),
    }
}

#[test]
fn unary_negation_of_literal_collapses() {
    match must_parse("-2.75") {
        Expr::NumberLiteral(n) => assert!((n.val - -2.75).abs() < 1e-12),
        other => panic!("expected NumberLiteral (collapsed), got {other:?}"),
    }
}

#[test]
fn metric_selector_api() {
    let matchers = parse_metric_selector(r#"up{job="api"}"#).expect("parse");
    // Expect 2: job="api" and the synthesised __name__="up".
    assert_eq!(matchers.len(), 2);
    assert!(matchers
        .iter()
        .any(|m| m.name == "__name__" && m.value == "up"));
    assert!(matchers.iter().any(|m| m.name == "job" && m.value == "api"));
}

// -------- error positions --------

#[test]
fn parse_error_points_at_the_offending_token() {
    // Every error used to be reported as `0..0`, so a consumer could not
    // tell where the problem was without re-parsing the message text.
    let input = "sum(foo) by";
    let errs = parse_expr(input).expect_err("rejects");
    let range = errs.iter().next().expect("at least one error").range;
    assert!(
        range.start > 0,
        "expected a position past the start of the input, got {range:?}"
    );
    assert!(
        range.end as usize <= input.len(),
        "range {range:?} runs past the input"
    );
}

#[test]
fn parse_error_position_tracks_the_input() {
    // The same malformed tail, pushed further along, has to report a
    // correspondingly later position — otherwise the range is constant
    // rather than meaningful.
    let near = parse_expr("foo +").expect_err("rejects");
    let far = parse_expr("foo + bar + baz +").expect_err("rejects");
    let near_start = near.iter().next().expect("an error").range.start;
    let far_start = far.iter().next().expect("an error").range.start;
    assert!(
        far_start > near_start,
        "expected the later error to report a later offset, got {far_start} vs {near_start}"
    );
}

#[test]
fn empty_input_error_covers_the_input() {
    // No lexeme to blame, so the range spans what was given rather than
    // claiming a misleading `0..0`.
    let errs = parse_expr("").expect_err("rejects");
    let range = errs.iter().next().expect("an error").range;
    assert_eq!(range.start, 0);
}

#[test]
fn keywords_lex_case_insensitively() {
    match must_parse(r#"SUM BY (group) (http_requests) AND up"#) {
        Expr::Binary(b) => {
            assert_eq!(b.op, promql_parser::token::ItemType::Land);
            match *b.lhs {
                Expr::Aggregate(a) => {
                    assert_eq!(a.op, promql_parser::token::ItemType::Sum);
                    assert_eq!(a.grouping, vec!["group".to_string()]);
                }
                other => panic!("expected Aggregate on LHS, got {other:?}"),
            }
        }
        other => panic!("expected Binary, got {other:?}"),
    }
    match must_parse("up OFFSET 5m") {
        Expr::VectorSelector(vs) => assert_eq!(vs.original_offset_secs, 300.0),
        other => panic!("expected VectorSelector with offset, got {other:?}"),
    }
}

/// Both modifiers sit on the VectorSelector, reached through the
/// matrix selector: upstream's `setAnchored`/`setSmoothed` walk into it
/// rather than giving the MatrixSelector its own field.
#[test]
fn anchored_and_smoothed_land_on_the_vector_selector() {
    for (q, anchored, smoothed) in [
        ("rate(foo[5m] anchored)", true, false),
        ("rate(foo[5m] smoothed)", false, true),
    ] {
        match must_parse_extended(q) {
            Expr::Call(c) => match &c.args[0] {
                Expr::MatrixSelector(ms) => match &*ms.vector_selector {
                    Expr::VectorSelector(vs) => {
                        assert_eq!(vs.anchored, anchored, "{q}");
                        assert_eq!(vs.smoothed, smoothed, "{q}");
                    }
                    other => panic!("{q}: expected VectorSelector, got {other:?}"),
                },
                other => panic!("{q}: expected MatrixSelector, got {other:?}"),
            },
            other => panic!("{q}: expected Call, got {other:?}"),
        }
    }
    // A bare instant selector takes them too.
    match must_parse_extended("foo anchored") {
        Expr::VectorSelector(vs) => assert!(vs.anchored && !vs.smoothed),
        other => panic!("expected VectorSelector, got {other:?}"),
    }
}

#[test]
fn case_folding_does_not_invent_keywords() {
    assert!(parse_expr("SUM BY").is_err());
}

#[test]
fn float_literals_may_end_in_a_dot() {
    match must_parse("1./6.") {
        Expr::Binary(b) => {
            assert_eq!(b.op, promql_parser::token::ItemType::Div);
            match (*b.lhs, *b.rhs) {
                (Expr::NumberLiteral(l), Expr::NumberLiteral(r)) => {
                    assert_eq!(l.val, 1.0);
                    assert_eq!(r.val, 6.0);
                }
                other => panic!("expected two NumberLiterals, got {other:?}"),
            }
        }
        other => panic!("expected Binary, got {other:?}"),
    }
}

#[test]
fn a_second_dot_is_still_an_error() {
    assert!(parse_expr("1..6").is_err());
}

#[test]
fn parse_errors_can_be_taken_by_value() {
    let errs = parse_expr("sum(foo) by").expect_err("rejects");
    let n = errs.len();
    let owned: Vec<promql_parser::ParseError> = errs.clone().into_iter().collect();
    assert_eq!(owned.len(), n);
    assert_eq!(errs.into_vec().len(), n);
}

#[test]
fn anchored_and_smoothed_are_mutually_exclusive() {
    assert!(extended_range_selectors()
        .parse_expr("rate(foo[5m] anchored smoothed)")
        .is_err());
    assert!(extended_range_selectors()
        .parse_expr("rate(foo[5m] smoothed anchored)")
        .is_err());
}

/// Upstream rejects the modifier on a subquery and on anything that is
/// not a selector at all.
#[test]
fn a_misplaced_range_modifier_is_an_error() {
    assert!(extended_range_selectors()
        .parse_expr("rate(foo[5m])[10m:1m] anchored")
        .is_err());
    assert!(extended_range_selectors()
        .parse_expr("(foo + bar) anchored")
        .is_err());
    assert!(extended_range_selectors()
        .parse_expr("sum(foo) smoothed")
        .is_err());
}

/// `anchored_expr`, `offset_expr` and `at_expr` are all `expr` suffixes
/// upstream, so the modifier composes with the other two in either
/// order and every one of them reaches the same VectorSelector.
#[test]
fn a_range_modifier_composes_with_offset_and_at() {
    for (q, offset_secs, timestamp) in [
        ("rate(foo[5m] anchored offset 1m)", 60.0, None),
        ("rate(foo[5m] offset 1m anchored)", 60.0, None),
        ("rate(foo[5m] @ 100 anchored)", 0.0, Some(100_000)),
    ] {
        match must_parse_extended(q) {
            Expr::Call(c) => match &c.args[0] {
                Expr::MatrixSelector(ms) => match &*ms.vector_selector {
                    Expr::VectorSelector(vs) => {
                        assert!(vs.anchored, "{q}");
                        assert_eq!(vs.original_offset_secs, offset_secs, "{q}");
                        assert_eq!(vs.timestamp, timestamp, "{q}");
                    }
                    other => panic!("{q}: expected VectorSelector, got {other:?}"),
                },
                other => panic!("{q}: expected MatrixSelector, got {other:?}"),
            },
            other => panic!("{q}: expected Call, got {other:?}"),
        }
    }
}

/// Upstream lists ANCHORED and SMOOTHED in `metric_identifier` and
/// `maybe_label`, so adding the keywords must not cost anyone a metric,
/// label or grouping name that parsed before. A keyword as a *label*
/// name needs the lexer's BRACES start condition, which this branch
/// does not have; `keywords_are_label_names_inside_braces` covers it
/// there.
#[test]
fn the_new_keywords_are_still_names() {
    for (q, name) in [("anchored", "anchored"), ("smoothed", "smoothed")] {
        match must_parse(q) {
            Expr::VectorSelector(vs) => assert_eq!(vs.name, name, "{q}"),
            other => panic!("{q}: expected VectorSelector, got {other:?}"),
        }
    }
    match must_parse("sum by (anchored) (x)") {
        Expr::Aggregate(a) => assert_eq!(a.grouping, ["anchored"]),
        other => panic!("expected Aggregate, got {other:?}"),
    }
}

#[test]
fn hex_literals_lex_as_one_number() {
    for (q, want) in [
        ("0x1F", 31.0),
        ("0X1f", 31.0),
        ("0x_1F", 31.0),
        ("0x1_F", 31.0),
    ] {
        match must_parse(q) {
            Expr::NumberLiteral(n) => assert_eq!(n.val, want, "{q}"),
            other => panic!("expected NumberLiteral for {q}, got {other:?}"),
        }
    }
    // `scanNumber` rejects an underscore with nothing after it.
    assert!(parse_expr("0x1F_").is_err());
}

/// Upstream only makes `fill` a keyword when the next non-space rune is
/// `(`, so queries that already used it as a metric name keep working.
#[test]
fn fill_is_a_metric_name_unless_it_opens_a_call() {
    match must_parse("fill + fill") {
        Expr::Binary(b) => match (*b.lhs, *b.rhs) {
            (Expr::VectorSelector(l), Expr::VectorSelector(r)) => {
                assert_eq!(l.name, "fill");
                assert_eq!(r.name, "fill");
            }
            other => panic!("expected two VectorSelectors, got {other:?}"),
        },
        other => panic!("expected Binary, got {other:?}"),
    }
    match must_parse("fill_left") {
        Expr::VectorSelector(vs) => assert_eq!(vs.name, "fill_left"),
        other => panic!("expected VectorSelector, got {other:?}"),
    }
}

#[test]
fn keywords_are_label_names_inside_braces() {
    let matchers = parse_metric_selector(
        r#"x{by="a", on="b", group="c", offset="d", anchored="e", smoothed="f"}"#,
    )
    .expect("keywords inside braces parse as label names");
    for (name, value) in [
        ("by", "a"),
        ("on", "b"),
        ("group", "c"),
        ("offset", "d"),
        ("anchored", "e"),
        ("smoothed", "f"),
    ] {
        assert!(
            matchers.iter().any(|m| m.name == name && m.value == value),
            "missing matcher {name}={value:?} in {matchers:?}"
        );
    }
}

#[test]
fn keywords_stay_keywords_after_the_closing_brace() {
    // The start condition has to end at `}`, or this `offset` would lex
    // as a label name and the modifier would silently vanish.
    match must_parse(r#"up{group="canary"} offset 5m"#) {
        Expr::VectorSelector(vs) => assert_eq!(vs.original_offset_secs, 300.0),
        other => panic!("expected VectorSelector with offset, got {other:?}"),
    }
}

#[test]
fn keyword_as_label_name_still_needs_a_matcher() {
    assert!(parse_expr(r#"up{by}"#).is_err());
}

#[test]
fn a_label_name_may_not_contain_a_colon() {
    // Only a metric name may; upstream's lexInsideBraces reads an
    // identifier as alphanumerics alone.
    assert!(parse_expr(r#"up{a:b="c"}"#).is_err());
}

fn messages(q: &str) -> Vec<String> {
    parse_expr(q)
        .expect_err("upstream rejects this query")
        .into_iter()
        .map(|e| e.message)
        .collect()
}

/// Upstream `addOffset`: an offset on anything but a selector or a
/// subquery is an error. Dropping it would answer `sum(foo) offset 5m`
/// as `sum(foo)`.
#[test]
fn offset_must_follow_a_selector_or_subquery() {
    const MSG: &str = "offset modifier must be preceded by an instant vector selector \
                       or range vector selector or a subquery";
    for q in [
        "sum(foo) offset 5m",
        "rate(foo[5m]) offset 5m",
        "(foo) offset 5m",
        "1 offset 5m",
    ] {
        assert_eq!(messages(q), [MSG], "{q}");
    }
    assert_eq!(
        messages("foo offset 5m offset 10m"),
        ["offset may not be set multiple times"]
    );
    for q in [
        "foo offset 5m",
        "foo[5m] offset 5m",
        "foo offset 0s offset 5m",
    ] {
        must_parse(q);
    }
}

/// Upstream `getAtModifierVars` and `setTimestamp`.
#[test]
fn at_must_follow_a_selector_or_subquery_once() {
    const MSG: &str = "@ modifier must be preceded by an instant vector selector \
                       or range vector selector or a subquery";
    for q in ["sum(foo) @ 100", "rate(foo[5m]) @ start()", "(foo) @ end()"] {
        assert_eq!(messages(q), [MSG], "{q}");
    }
    for q in ["foo @ 1 @ 2", "foo @ 1 @ start()", "foo @ end() @ 1"] {
        assert_eq!(
            messages(q),
            ["@ <timestamp> may not be set multiple times"],
            "{q}"
        );
    }
    assert_eq!(
        messages("foo @ NaN"),
        ["timestamp out of bounds for @ modifier: NaN"]
    );
    // Both of `setTimestamp`'s checks report.
    assert_eq!(
        messages("sum(foo) @ Inf"),
        [
            "timestamp out of bounds for @ modifier: +Inf".to_string(),
            MSG.to_string()
        ]
    );
    for q in ["foo @ 100", "foo[5m] @ start()", "foo @ 100 offset 5m"] {
        must_parse(q);
    }
}

/// Upstream `newAggregateExpr` counts the arguments instead of making
/// up a node from what is there.
#[test]
fn aggregate_argument_count_is_checked() {
    let at = |q: &str| {
        parse_expr(q)
            .expect_err("upstream rejects this query")
            .into_iter()
            .map(|e| (e.message, e.range.start, e.range.end))
            .collect::<Vec<_>>()
    };
    assert_eq!(
        at("sum()"),
        [(
            "no arguments for aggregate expression provided".to_string(),
            0,
            5
        )]
    );
    for (q, want) in [
        ("sum(a, b)", "expected 1, got 2"),
        ("topk(foo)", "expected 2, got 1"),
        ("topk(1, 2, foo)", "expected 2, got 3"),
    ] {
        assert_eq!(
            messages(q),
            [format!(
                "wrong number of arguments for aggregate expression provided, {want}"
            )],
            "{q}"
        );
    }
    // The missing arguments are reported ahead of the experimental gate.
    assert_eq!(
        messages("limitk()"),
        ["no arguments for aggregate expression provided"]
    );
}
