//! The four parser gates (`ParserOptions`, upstream `parser.Options`).
//!
//! Each gate is tested the same way: with defaults the construct is an
//! error carrying upstream's message at upstream's position, with its flag
//! alone it parses, and with every *other* flag it is still an error, so a
//! gate cannot be satisfied by the wrong switch. The message and position
//! are the ones in `upstream/parse.go` and `upstream/generated_parser.y`.

use promql_parser::posrange::PositionRange;
use promql_parser::{parse_expr, ParseErrors, Parser, ParserOptions, FEATURE_FLAGS};

const FUNCTIONS: ParserOptions = ParserOptions {
    enable_experimental_functions: true,
    ..OFF
};
const DURATION: ParserOptions = ParserOptions {
    experimental_duration_expr: true,
    ..OFF
};
const SELECTORS: ParserOptions = ParserOptions {
    enable_extended_range_selectors: true,
    ..OFF
};
const FILL: ParserOptions = ParserOptions {
    enable_binop_fill_modifiers: true,
    ..OFF
};
const OFF: ParserOptions = ParserOptions {
    enable_experimental_functions: false,
    experimental_duration_expr: false,
    enable_extended_range_selectors: false,
    enable_binop_fill_modifiers: false,
};

fn errors(result: Result<impl std::fmt::Debug, ParseErrors>) -> Vec<(String, PositionRange)> {
    result
        .expect_err("the construct should be rejected")
        .into_iter()
        .map(|e| (e.message, e.range))
        .collect()
}

/// The flag each error names, which a caller sorting queries by the flag
/// they need reads instead of the message.
fn gates(result: Result<impl std::fmt::Debug, ParseErrors>) -> Vec<Option<&'static str>> {
    result
        .expect_err("the construct should be rejected")
        .into_iter()
        .map(|e| e.gate)
        .collect()
}

fn flag_of(enabling: ParserOptions) -> &'static str {
    FEATURE_FLAGS
        .iter()
        .find(|f| OFF.with_flag(f.flag, true) == Some(enabling))
        .expect("one flag")
        .flag
}

fn range(start: u32, end: u32) -> PositionRange {
    PositionRange { start, end }
}

/// Assert `query` is rejected with exactly `message` at `at` under every
/// option set but `enabling`, and parses under `enabling`.
fn gate(query: &str, enabling: ParserOptions, message: &str, at: PositionRange) {
    let want = vec![(message.to_string(), at)];
    assert_eq!(errors(parse_expr(query)), want, "{query}: defaults");
    assert_eq!(
        gates(parse_expr(query)),
        [Some(flag_of(enabling))],
        "{query}: defaults"
    );
    for (name, other) in [
        ("functions", FUNCTIONS),
        ("duration", DURATION),
        ("selectors", SELECTORS),
        ("fill", FILL),
    ] {
        if other == enabling {
            continue;
        }
        assert_eq!(
            errors(Parser::new(other).parse_expr(query)),
            want,
            "{query}: only {name}"
        );
    }
    Parser::new(enabling)
        .parse_expr(query)
        .unwrap_or_else(|e| panic!("{query}: with its flag: {e}"));
    Parser::new(ParserOptions::all())
        .parse_expr(query)
        .unwrap_or_else(|e| panic!("{query}: with every flag: {e}"));
}

/// Upstream's `function_call` identifier arm: `$1.PositionRange()`, the
/// name token.
#[test]
fn an_experimental_function_is_gated_at_its_name() {
    gate(
        "mad_over_time(x[5m])",
        FUNCTIONS,
        r#"function "mad_over_time" is not enabled"#,
        range(0, 13),
    );
    gate(
        "sum(ts_of_last_over_time(x[5m]))",
        FUNCTIONS,
        r#"function "ts_of_last_over_time" is not enabled"#,
        range(4, 24),
    );
}

#[test]
fn a_stable_function_needs_no_flag() {
    parse_expr("rate(x[5m])").expect("rate is not experimental");
}

#[test]
fn every_experimental_call_in_a_query_is_reported() {
    let got = errors(parse_expr("mad_over_time(x[5m]) + first_over_time(y[5m])"));
    assert_eq!(
        got,
        vec![
            (
                r#"function "mad_over_time" is not enabled"#.to_string(),
                range(0, 13)
            ),
            (
                r#"function "first_over_time" is not enabled"#.to_string(),
                range(23, 38)
            ),
        ]
    );
}

/// The other three `function_call` arms (`start()`, `end()`, `step()`,
/// `range()`) carry the same check upstream. They are not ported past
/// it, so with the flag on they still fail, just not with the gate's
/// message.
#[test]
fn the_keyword_function_arms_are_gated_too() {
    for (query, name, at) in [
        ("start()", "start", range(0, 5)),
        ("end()", "end", range(0, 3)),
        ("step()", "step", range(0, 4)),
        ("range()", "range", range(0, 5)),
    ] {
        let message = format!("function {name:?} is not enabled");
        assert_eq!(
            errors(parse_expr(query)),
            vec![(message.clone(), at)],
            "{query}"
        );
        assert_eq!(
            gates(parse_expr(query)),
            [Some("promql-experimental-functions")],
            "{query}"
        );
        let enabled = errors(Parser::new(FUNCTIONS).parse_expr(query));
        assert!(
            enabled.iter().all(|(m, _)| *m != message),
            "{query} with its flag: {enabled:?}"
        );
    }
}

/// Upstream `newAggregateExpr`: `ret.PositionRange()`, the aggregation.
#[test]
fn limitk_and_limit_ratio_follow_the_experimental_functions_flag() {
    gate(
        "limitk(1, x)",
        FUNCTIONS,
        "limitk() is experimental and must be enabled with --enable-feature=promql-experimental-functions",
        range(0, 12),
    );
    gate(
        "limit_ratio(0.5, x)",
        FUNCTIONS,
        "limit_ratio() is experimental and must be enabled with --enable-feature=promql-experimental-functions",
        range(0, 19),
    );
    parse_expr("topk(1, x)").expect("topk is not experimental");
}

/// Upstream `newBinaryExpression`: `ret.PositionRange()`, the whole
/// binary expression.
#[test]
fn binop_fill_modifiers_are_gated_at_the_binary_expression() {
    for query in [
        "a + fill(0) b",
        "a + on(x) fill_left(1) b",
        "a + ignoring(x) fill_right(2) b",
        "a + fill_left(1) fill_right(2) b",
    ] {
        gate(
            query,
            FILL,
            "binop fill modifiers are experimental and not enabled",
            range(0, query.len() as u32),
        );
    }
    parse_expr("a + on(x) b").expect("matching without fill is stable");
}

/// Upstream `setAnchored` / `setSmoothed`: `e.PositionRange()`, the
/// expression the modifier follows.
#[test]
fn anchored_and_smoothed_are_gated_at_the_expression_they_follow() {
    gate(
        "foo anchored",
        SELECTORS,
        "anchored modifier is experimental and not enabled",
        range(0, 3),
    );
    gate(
        "rate(foo[5m] smoothed)",
        SELECTORS,
        "smoothed modifier is experimental and not enabled",
        range(5, 12),
    );
}

/// With the gate open the modifiers keep upstream's other rejections, and
/// now say so.
#[test]
fn a_misplaced_modifier_says_why() {
    let p = Parser::new(SELECTORS);
    for (query, message, at) in [
        (
            "foo[5m:1m] anchored",
            "anchored modifier is not supported for subqueries",
            range(0, 10),
        ),
        (
            "(foo + bar) smoothed",
            "smoothed modifier not implemented",
            range(0, 11),
        ),
        (
            "foo anchored smoothed",
            "anchored and smoothed modifiers cannot be used together",
            range(0, 3),
        ),
    ] {
        assert_eq!(
            errors(p.parse_expr(query)),
            vec![(message.to_string(), at)],
            "{query}"
        );
    }
}

/// Everything experimental still parses with every gate on, which is how
/// upstream's promqltest runs (`TestParserOpts`).
#[test]
fn every_gate_on_parses_all_of_it_together() {
    Parser::new(ParserOptions::all())
        .parse_expr("limitk(1, mad_over_time(x[5m] anchored)) + on() fill(0) rate(y[5m] smoothed)")
        .expect("parses");
}

/// `series` and `metric` descriptions take no experimental syntax, so the
/// options reach them without changing the answer.
#[test]
fn options_do_not_change_series_descriptions() {
    let p = Parser::new(ParserOptions::all());
    assert_eq!(
        p.parse_series_desc("up{job=\"a\"} 1 2 3").unwrap(),
        promql_parser::parse_series_desc("up{job=\"a\"} 1 2 3").unwrap()
    );
}
