//! Instant queries answer with the type their expression has.
//!
//! Upstream's `execEvalStmt` runs an instant query as a range of one step
//! and reads the matrix back by `Expr.Type()`; `NewRangeQuery` refuses the
//! types that only an instant query can answer.

use std::num::{NonZeroU64, NonZeroUsize};
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

/// One series of a matrix: its `city` and its samples.
type Row = (String, Vec<(i64, f64)>);

fn matrix_over(source: &MemorySeriesSource, query: &str, at_ms: i64) -> Vec<Row> {
    let InstantResult::Matrix(batches) = Engine::blocking()
        .unwrap()
        .instant_query(source, query, at_ms)
        .unwrap_or_else(|e| panic!("{query}: {e}"))
    else {
        panic!("{query}: not a matrix")
    };
    series::coalesce(series::decode(&batches).unwrap())
        .unwrap()
        .iter()
        .map(|s| {
            (
                s.label("city").to_string(),
                s.timestamps()
                    .iter()
                    .copied()
                    .zip(s.values().iter().copied())
                    .collect(),
            )
        })
        .collect()
}

fn matrix(query: &str, at_ms: i64) -> Vec<Row> {
    matrix_over(source().as_ref(), query, at_ms)
}

fn row(city: &str, points: &[(i64, f64)]) -> Row {
    (city.to_string(), points.to_vec())
}

/// `matrixSelector`: the samples under their own timestamps, not the
/// step's, in a window that is open at its start and closed at its end.
#[test]
fn a_range_selector_is_the_samples_of_its_window_open_at_the_start() {
    // oslo is 1, 2, 3 at 0, 30s, 60s: the sample at exactly 0 is mint.
    assert_eq!(
        matrix(r#"temperature{city="oslo"}[1m]"#, 60_000),
        [row("oslo", &[(30_000, 2.0), (60_000, 3.0)])]
    );
    assert_eq!(
        matrix("temperature[1m]", 60_000),
        [
            row("lima", &[(30_000, 11.0), (60_000, 12.0)]),
            row("oslo", &[(30_000, 2.0), (60_000, 3.0)]),
        ]
    );
}

/// A series with no sample in the window is not in the result, and a
/// selector that matches nothing is an empty matrix, not an error.
#[test]
fn a_series_with_no_sample_in_the_window_is_not_in_the_matrix() {
    assert_eq!(matrix("temperature[10s]", 45_000), []);
    assert_eq!(matrix("nonexistent[1m]", 60_000), []);
    // The last sample is at 120s; ten minutes on, nothing is in reach.
    assert_eq!(matrix("temperature[1m]", 600_000), []);
}

/// `offset` moves the window and `@` pins it; the samples keep their own
/// timestamps in both, which is why upstream does not duplicate a range
/// selector's result per step.
#[test]
fn offset_and_at_move_the_window_and_not_the_timestamps() {
    assert_eq!(
        matrix(r#"temperature{city="oslo"}[1m] offset 30s"#, 60_000),
        [row("oslo", &[(0, 1.0), (30_000, 2.0)])]
    );
    assert_eq!(
        matrix(r#"temperature{city="oslo"}[1m] @ 120"#, 0),
        [row("oslo", &[(90_000, 4.0), (120_000, 5.0)])]
    );
}

/// A stale marker ends a series' run and is no sample of a range
/// selector, as `matrixIterSlice` drops it.
#[test]
fn a_stale_marker_is_not_a_sample_of_the_window() {
    let source = MemorySeriesSource::from_descriptions(
        &load(&[r#"temperature{city="oslo"} 1 stale 3"#]),
        60.0,
    );
    assert_eq!(
        matrix_over(&source, "temperature[3m]", 180_000),
        [row("oslo", &[(120_000, 3.0)])]
    );
    assert_eq!(
        matrix_over(&source, "temperature[3m]", 120_000),
        [row("oslo", &[(0, 1.0), (120_000, 3.0)])]
    );
}

/// Whatever shape the store cuts its answer into, a label set's chunks
/// are one series once joined, with every sample of the window once.
#[test]
fn a_range_selector_is_the_same_across_store_shapes() {
    let plain = matrix("temperature[2m]", 120_000);
    assert_eq!(plain.len(), 2);
    let stores = [
        Arc::try_unwrap(source()).unwrap().chunked(0),
        Arc::try_unwrap(source()).unwrap().chunked(45_000),
        Arc::try_unwrap(source())
            .unwrap()
            .blocks(NonZeroU64::new(60_000).unwrap()),
        Arc::try_unwrap(source())
            .unwrap()
            .partitions(NonZeroUsize::new(3).unwrap()),
    ];
    for store in &stores {
        assert_eq!(matrix_over(store, "temperature[2m]", 120_000), plain);
    }
}

/// A range selector in parentheses is the same call argument upstream's
/// `unwrapParenExpr` makes it.
#[test]
fn parentheses_around_a_range_selector_argument_change_nothing() {
    let engine = Engine::blocking().unwrap();
    let range = RangeQuery::new(60_000, 120_000, 30_000);
    let run = |query: &str| {
        let batches = engine
            .range_query(source().as_ref(), query, &range)
            .unwrap_or_else(|e| panic!("{query}: {e}"));
        series::decode(&batches)
            .unwrap()
            .iter()
            .map(|s| (s.label("city").to_string(), s.values().to_vec()))
            .collect::<Vec<_>>()
    };
    for func in ["rate", "changes", "sum_over_time"] {
        let plain = run(&format!("{func}(temperature[1m])"));
        assert!(!plain.is_empty(), "{func}");
        assert_eq!(run(&format!("{func}((temperature[1m]))")), plain, "{func}");
        assert_eq!(
            run(&format!("{func}(((temperature[1m])))")),
            plain,
            "{func}"
        );
    }
    assert!(matches!(
        run("absent_over_time((nonexistent[1m]))").as_slice(),
        [(_, v)] if v == &[1.0, 1.0, 1.0]
    ));
}
