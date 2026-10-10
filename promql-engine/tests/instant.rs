//! Instant queries answer with the type their expression has.
//!
//! Upstream's `execEvalStmt` runs an instant query as a range of one step
//! and reads the matrix back by `Expr.Type()`; `NewRangeQuery` refuses the
//! types that only an instant query can answer.

use std::sync::Arc;

use promql_engine::{series, Engine, EngineError, InstantResult, MemorySeriesSource, RangeQuery};
use promql_parser::SeriesDescription;

fn load(lines: &[&str]) -> Vec<SeriesDescription> {
    lines
        .iter()
        .map(|l| promql_parser::parse_series_desc(l).expect("series line parses"))
        .collect()
}

/// Two series 30s apart: at 60s oslo is 3 and lima is 12.
fn source() -> Arc<MemorySeriesSource> {
    Arc::new(MemorySeriesSource::from_descriptions(
        &load(&[
            r#"temperature{city="oslo"} 1+1x4"#,
            r#"temperature{city="lima"} 10+1x4"#,
        ]),
        30.0,
    ))
}

fn instant(query: &str, at_ms: i64) -> Result<InstantResult, EngineError> {
    Engine::blocking()
        .unwrap()
        .instant_query(source().as_ref(), query, at_ms)
}

/// A string is not a series; it is the literal without its quotes, through
/// any parentheses.
#[test]
fn a_string_is_the_literal_without_its_quotes() {
    for (query, want) in [(r#"(" Foo ")"#, " Foo "), (r#""""#, "")] {
        match instant(query, 0).unwrap() {
            InstantResult::String(s) => assert_eq!(s, want, "{query}"),
            other => panic!("{query}: {other:?}"),
        }
    }
}

/// A vector keeps the engine's batches, one sample per series stamped
/// with the evaluation time.
#[test]
fn an_instant_vector_is_a_sample_per_series_at_the_evaluation_time() {
    let InstantResult::Vector(batches) = instant("temperature", 60_000).unwrap() else {
        panic!("not a vector")
    };
    let decoded = series::decode(&batches).unwrap();
    let got: Vec<(String, Vec<i64>, Vec<f64>)> = decoded
        .iter()
        .map(|s| {
            (
                s.label("city").to_string(),
                s.timestamps().to_vec(),
                s.values().to_vec(),
            )
        })
        .collect();
    assert_eq!(
        got,
        [
            ("lima".to_string(), vec![60_000], vec![12.0]),
            ("oslo".to_string(), vec![60_000], vec![3.0]),
        ]
    );
}

/// A scalar is read off the one-series plan it shares with `vector(x)`, so
/// only the expression's type tells it from `sum(x)`.
#[test]
fn a_scalar_expression_answers_a_number_and_a_vector_of_one_does_not() {
    assert!(matches!(
        instant("1 + 1", 0).unwrap(),
        InstantResult::Scalar(v) if v == 2.0
    ));
    assert!(matches!(
        instant("time()", 90_000).unwrap(),
        InstantResult::Scalar(v) if v == 90.0
    ));
    assert!(matches!(
        instant("vector(2)", 0).unwrap(),
        InstantResult::Vector(_)
    ));
    assert!(matches!(
        instant("sum(temperature)", 60_000).unwrap(),
        InstantResult::Vector(_)
    ));
}

/// `NewRangeQuery` takes a scalar or an instant vector and nothing else;
/// the words are upstream's.
#[test]
fn a_range_query_refuses_what_only_an_instant_query_answers() {
    let engine = Engine::blocking().unwrap();
    let range = RangeQuery::new(0, 120_000, 60_000);
    for (query, typ) in [
        (r#"("Foo")"#, "string"),
        ("temperature[1m]", "range vector"),
        ("temperature[1m:30s]", "range vector"),
    ] {
        let err = engine
            .range_query(source().as_ref(), query, &range)
            .unwrap_err();
        assert!(
            matches!(&err, EngineError::Query(m) if m == &format!(
                "invalid expression type \"{typ}\" for range query, must be Scalar or instant Vector"
            )),
            "{query}: {err}"
        );
    }
}
