//! The engine parses with the `ParserOptions` it was built with, on every
//! path a query string takes in. Which construct each gate guards, and
//! with what message, is `promql-parser/tests/feature_flags.rs`; this file
//! only pins that the engine hands its options to the parser and does not
//! keep a second set of defaults.

use promql_engine::{
    series, Engine, EngineError, EngineOptions, InstantResult, MemorySeriesSource, RangeQuery,
};
use promql_parser::ParserOptions;

const FUNCTIONS: ParserOptions = ParserOptions {
    enable_experimental_functions: true,
    experimental_duration_expr: false,
    enable_extended_range_selectors: false,
    enable_binop_fill_modifiers: false,
};

fn source() -> MemorySeriesSource {
    let descriptions: Vec<_> = ["x 1 2 3", "y 4 5 6"]
        .map(|l| promql_parser::parse_series_desc(l).expect("series line parses"))
        .into();
    MemorySeriesSource::from_descriptions(&descriptions, 30.0)
}

fn range() -> RangeQuery {
    RangeQuery::new(0, 60_000, 30_000)
}

/// The refusal a stock engine gives, as the parser words it.
fn is_the_gate(err: &EngineError) -> bool {
    matches!(err, EngineError::Parse(e)
        if e.iter().any(|e| e.message == r#"function "mad_over_time" is not enabled"#))
}

const QUERY: &str = "mad_over_time(x[1m])";

#[test]
fn a_stock_engine_refuses_experimental_syntax_on_every_path() {
    let blocking = Engine::blocking().unwrap();
    let err = blocking
        .range_query(&source(), QUERY, &range())
        .unwrap_err();
    assert!(is_the_gate(&err), "range_query: {err}");
    let err = blocking
        .instant_query(&source(), QUERY, 60_000)
        .unwrap_err();
    assert!(is_the_gate(&err), "instant_query: {err}");

    let engine = Engine::new();
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let err = rt
        .block_on(engine.range_query_async(&source(), QUERY, &range()))
        .unwrap_err();
    assert!(is_the_gate(&err), "range_query_async: {err}");
    let err = rt
        .block_on(engine.plan_async(&source(), QUERY, &range()))
        .unwrap_err();
    assert!(is_the_gate(&err), "plan_async: {err}");
    let err = rt
        .block_on(engine.prepare_async(&source(), QUERY, &range()))
        .unwrap_err();
    assert!(is_the_gate(&err), "prepare_async: {err}");
    let err = rt
        .block_on(engine.plan_instant_async(&source(), QUERY, 60_000))
        .unwrap_err();
    assert!(is_the_gate(&err), "plan_instant_async: {err}");
    let err = rt
        .block_on(engine.instant_query_async(&source(), QUERY, 60_000))
        .unwrap_err();
    assert!(is_the_gate(&err), "instant_query_async: {err}");

    let default = Engine::with_options(EngineOptions::default());
    let err = rt
        .block_on(default.plan_async(&source(), QUERY, &range()))
        .unwrap_err();
    assert!(is_the_gate(&err), "with_options(default): {err}");
}

/// With the gate open the query gets past the parser. This engine has no
/// `mad_over_time`, so it stops there, with its own error rather than the
/// parser's.
#[test]
fn an_engine_built_with_the_flag_parses_it_on_every_path() {
    let options = EngineOptions { parser: FUNCTIONS };
    let blocking = Engine::blocking_with_options(options.clone()).unwrap();
    let err = blocking
        .range_query(&source(), QUERY, &range())
        .unwrap_err();
    assert!(matches!(err, EngineError::Unsupported(_)), "{err}");
    let err = blocking
        .instant_query(&source(), QUERY, 60_000)
        .unwrap_err();
    assert!(matches!(err, EngineError::Unsupported(_)), "{err}");

    let engine = Engine::with_options(options);
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    for (path, result) in [
        (
            "range_query_async",
            rt.block_on(engine.range_query_async(&source(), QUERY, &range()))
                .map(|_| ()),
        ),
        (
            "plan_async",
            rt.block_on(engine.plan_async(&source(), QUERY, &range()))
                .map(|_| ()),
        ),
        (
            "prepare_async",
            rt.block_on(engine.prepare_async(&source(), QUERY, &range()))
                .map(|_| ()),
        ),
        (
            "plan_instant_async",
            rt.block_on(engine.plan_instant_async(&source(), QUERY, 60_000))
                .map(|_| ()),
        ),
        (
            "instant_query_async",
            rt.block_on(engine.instant_query_async(&source(), QUERY, 60_000))
                .map(|_| ()),
        ),
    ] {
        let err = result.unwrap_err();
        assert!(matches!(err, EngineError::Unsupported(_)), "{path}: {err}");
    }
}

/// A supported gated function must reach execution through both preparation
/// and the typed instant path, not just get past parsing to an unsupported node.
#[tokio::test]
async fn enabled_sort_executes_through_prepared_and_typed_instant_paths() {
    let engine = Engine::with_options(EngineOptions { parser: FUNCTIONS });
    let source = source();
    let query = r#"sort_by_label_desc({__name__=~"x|y"}, "__name__")"#;
    let prepared = engine
        .prepare_async(&source, query, &RangeQuery::new(60_000, 60_000, 1))
        .await
        .unwrap();
    let planned = engine.execute_async(&prepared).await.unwrap();
    let InstantResult::Vector(instant) = engine
        .instant_query_async(&source, query, 60_000)
        .await
        .unwrap()
    else {
        panic!("sort_by_label_desc must produce a vector");
    };
    for batches in [planned, instant] {
        let rows = series::decode(&batches).unwrap();
        let names: Vec<_> = rows.iter().map(|row| row.label("__name__")).collect();
        assert_eq!(names, ["y", "x"]);
    }
}

/// A flag does not leak into the others: this engine admits experimental
/// functions and still refuses fill.
#[test]
fn each_flag_opens_only_its_own_gate() {
    let engine = Engine::with_options(EngineOptions { parser: FUNCTIONS });
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let err = rt
        .block_on(engine.plan_async(&source(), "x + fill(0) y", &range()))
        .unwrap_err();
    assert!(
        matches!(&err, EngineError::Parse(e)
            if e.iter().any(|e| e.message == "binop fill modifiers are experimental and not enabled")),
        "{err}"
    );
}
