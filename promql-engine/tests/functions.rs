//! End to end: the instant-vector functions over the in-memory source.
//!
//! What is under test is the operator's seam — one row in, one row out,
//! `__name__` gone, the scalar arguments folded — rather than the
//! arithmetic, which the unit tests in `elementwise.rs` hold to Go's
//! `math` value by value.
//!
//! A one-step range at 150s stands in for an instant query, which this
//! engine does not have its own entry point for yet: with `start == end`
//! every series carries exactly one value, so a row reads as a vector
//! element.

use std::sync::Arc;

use promql_engine::{Engine, EngineError, MemorySeriesSource, RangeQuery};
use promql_parser::SeriesDescription;

fn load(lines: &[&str]) -> Vec<SeriesDescription> {
    lines
        .iter()
        .map(|l| promql_parser::parse_series_desc(l).expect("series line parses"))
        .collect()
}

/// Two series 30s apart, one climbing and one falling through zero.
/// The values are fractional so that rounding has something to do:
/// at 150s they are oslo 9.25 and lima -9.75.
fn source() -> Arc<MemorySeriesSource> {
    Arc::new(MemorySeriesSource::from_descriptions(
        &load(&[
            r#"temperature{city="oslo"} 0.5+1.75x10"#,
            r#"temperature{city="lima"} 1.5-2.25x10"#,
        ]),
        30.0,
    ))
}

/// The one step every assertion below is read at.
fn at_150s() -> RangeQuery {
    RangeQuery::new(150_000, 150_000, 30_000)
}

/// One vector element: its labels, then its value.
type Row = (Vec<(String, String)>, f64);

/// The result of `query` at 150s, sorted by label set — a vector
/// result has no promised order.
fn vector(query: &str) -> Vec<Row> {
    let batches = Engine::blocking()
        .unwrap()
        .range_query(source().as_ref(), query, &at_150s())
        .unwrap_or_else(|e| panic!("{query}: {e}"));
    let mut rows: Vec<Row> = promql_engine::series::decode(&batches)
        .expect("the canonical shape decodes")
        .iter()
        .map(|s| {
            assert_eq!(s.values().len(), 1, "{query}: one step, one value");
            (
                s.labels()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect(),
                s.values()[0],
            )
        })
        .collect();
    rows.sort_by(|a, b| a.0.cmp(&b.0));
    rows
}

fn error(query: &str) -> EngineError {
    Engine::blocking()
        .unwrap()
        .range_query(source().as_ref(), query, &at_150s())
        .expect_err(query)
}

#[test]
fn the_store_is_what_the_assertions_below_assume() {
    assert_eq!(
        vector("temperature"),
        vec![
            (
                vec![
                    ("__name__".into(), "temperature".into()),
                    ("city".into(), "lima".into())
                ],
                -9.75
            ),
            (
                vec![
                    ("__name__".into(), "temperature".into()),
                    ("city".into(), "oslo".into())
                ],
                9.25
            ),
        ]
    );
}

/// The label set comes back without `__name__`: every one of these
/// functions emits its sample with `DropName: true`.
#[test]
fn an_elementwise_function_keeps_the_series_and_drops_the_metric_name() {
    let got = vector("abs(temperature)");
    assert_eq!(got.len(), 2);
    assert_eq!(got[0].0, vec![("city".to_string(), "lima".to_string())]);
    assert_eq!(got[0].1, 9.75);
    assert_eq!(got[1].1, 9.25);
}

#[test]
fn each_function_answers_for_each_series() {
    for (query, lima, oslo) in [
        ("abs(temperature)", 9.75, 9.25),
        ("ceil(temperature)", -9.0, 10.0),
        ("floor(temperature)", -10.0, 9.0),
        ("sgn(temperature)", -1.0, 1.0),
        ("clamp_min(temperature, 0)", 0.0, 9.25),
        ("clamp_max(temperature, 0)", -9.75, 0.0),
        ("clamp(temperature, -1, 1)", -1.0, 1.0),
        // Ties round up, so -9.75 goes to -10 and 9.25 to 9.
        ("round(temperature)", -10.0, 9.0),
        ("round(temperature, 4)", -8.0, 8.0),
        ("exp(sgn(temperature))", (-1f64).exp(), 1f64.exp()),
        ("deg(rad(temperature))", -9.75, 9.25),
        ("sqrt(abs(temperature))", 9.75f64.sqrt(), 9.25f64.sqrt()),
    ] {
        let got = vector(query);
        assert_eq!(got.len(), 2, "{query}");
        assert!((got[0].1 - lima).abs() < 1e-12, "{query} for lima: {got:?}");
        assert!((got[1].1 - oslo).abs() < 1e-12, "{query} for oslo: {got:?}");
    }
}

/// `dateWrapper` reads each value as Unix seconds, truncated toward
/// zero: lima's -9.75 is nine seconds before the epoch, the last minute
/// of 1969, and oslo's 9.25 is the epoch's first.
#[test]
fn the_date_functions_read_the_value_as_unix_seconds() {
    for (query, lima, oslo) in [
        ("year(temperature)", 1969.0, 1970.0),
        ("month(temperature)", 12.0, 1.0),
        ("day_of_month(temperature)", 31.0, 1.0),
        ("day_of_year(temperature)", 365.0, 1.0),
        // A Wednesday and a Thursday, Sunday being 0.
        ("day_of_week(temperature)", 3.0, 4.0),
        ("days_in_month(temperature)", 31.0, 31.0),
        ("hour(temperature)", 23.0, 0.0),
        ("minute(temperature)", 59.0, 0.0),
    ] {
        let got = vector(query);
        assert_eq!(got.len(), 2, "{query}");
        assert_eq!(got[0].0, vec![("city".to_string(), "lima".to_string())]);
        assert_eq!(got[0].1, lima, "{query} for lima");
        assert_eq!(got[1].1, oslo, "{query} for oslo");
    }
}

/// A value past chrono's calendar is still a date upstream, and here:
/// ten trillion seconds is the year 318857, and as many before the
/// epoch the year -314918.
#[test]
fn a_date_past_chronos_calendar_is_still_answered() {
    for (query, want) in [
        ("year(vector(10000000000000))", 318_857.0),
        ("month(vector(10000000000000))", 5.0),
        ("hour(vector(10000000000000))", 17.0),
        ("year(vector(-10000000000000))", -314_918.0),
    ] {
        let got = vector(query);
        assert_eq!(got.len(), 1, "{query}");
        assert_eq!(got[0].1, want, "{query}");
    }
}

/// Without a vector the date functions read the evaluation time, here
/// 150s into the epoch, as one unlabelled sample: `minute()` is
/// `minute(vector(time()))`, and must agree with it on every step.
#[test]
fn a_date_function_without_a_vector_reads_the_evaluation_time() {
    for (name, want) in [
        ("year", 1970.0),
        ("month", 1.0),
        ("day_of_month", 1.0),
        ("day_of_year", 1.0),
        ("day_of_week", 4.0),
        ("days_in_month", 31.0),
        ("hour", 0.0),
        ("minute", 2.0),
    ] {
        let got = vector(&format!("{name}()"));
        assert_eq!(got.len(), 1, "{name}()");
        assert!(got[0].0.is_empty(), "{name}(): {:?}", got[0].0);
        assert_eq!(got[0].1, want, "{name}()");
        assert_eq!(vector(&format!("{name}(vector(time()))")), got, "{name}");
    }
}

/// Over a bare selector `timestamp` is the picked sample's own time: at
/// 150s with a 10s offset the sample at 120s is picked, and 120 is the
/// answer. Through another function the sample is the step's, so the
/// same selector under `abs` answers 150, as upstream does.
#[test]
fn timestamp_is_the_samples_time_over_a_selector_and_the_steps_elsewhere() {
    for (query, want) in [
        ("timestamp(temperature)", 150.0),
        ("timestamp(temperature offset 10s)", 120.0),
        ("timestamp((temperature offset 10s))", 120.0),
        ("timestamp(abs(temperature offset 10s))", 150.0),
        ("timestamp(timestamp(temperature offset 10s))", 150.0),
    ] {
        let got = vector(query);
        assert_eq!(got.len(), 2, "{query}");
        assert_eq!(got[0].0, vec![("city".to_string(), "lima".to_string())]);
        assert_eq!(got[0].1, want, "{query}");
        assert_eq!(got[1].1, want, "{query}");
    }
    let got = vector("timestamp(vector(1))");
    assert_eq!(got.len(), 1);
    assert!(got[0].0.is_empty());
    assert_eq!(got[0].1, 150.0);
}

/// A value a function has nothing to say about is still a sample: the
/// series stays in the result carrying a NaN, as upstream's
/// `simpleFloatFunc` leaves it.
#[test]
fn a_nan_is_a_value_and_not_a_missing_sample() {
    let got = vector("ln(temperature)");
    assert_eq!(got.len(), 2);
    assert!(got[0].1.is_nan(), "ln of a negative value is NaN");
    assert!((got[1].1 - 9.25f64.ln()).abs() < 1e-12, "{got:?}");
}

/// `clamp` with the bounds the wrong way round answers with nothing at
/// all — not with the series unchanged, and not with an error.
#[test]
fn a_clamp_whose_bounds_cross_is_the_empty_vector() {
    assert!(vector("clamp(temperature, 1, 0)").is_empty());
    // clamp_min and clamp_max each have one bound at infinity, so
    // neither can ever cross.
    assert_eq!(vector("clamp_min(temperature, 100)").len(), 2);
}

/// A function over a function: the inner one has already dropped the
/// metric name, and the outer runs on the same rows.
#[test]
fn the_operator_stacks_on_a_range_function_and_an_aggregation() {
    // Held against the same rate without the `abs`, so the assertion
    // is about the operator and not about the extrapolation.
    let rate = vector("rate(temperature[2m])");
    let absolute = vector("abs(rate(temperature[2m]))");
    assert_eq!(rate.len(), 2);
    assert!(rate[0].1 < 0.0 && rate[1].1 > 0.0, "{rate:?}");
    for (with, without) in absolute.iter().zip(&rate) {
        assert_eq!(with.0, without.0);
        assert_eq!(with.1, without.1.abs());
    }

    let got = vector("floor(sum(temperature))");
    assert_eq!(got.len(), 1);
    assert!(got[0].0.is_empty());
    assert_eq!(got[0].1, -1.0);

    let got = vector("sum(abs(temperature))");
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].1, 19.0);
}

/// `vector(s)` is the scalar's value as one series with no labels, and
/// so it needs no store at all.
#[test]
fn vector_of_a_scalar_is_one_unlabelled_series() {
    let got = vector("vector(1)");
    assert_eq!(got.len(), 1);
    assert!(got[0].0.is_empty());
    assert_eq!(got[0].1, 1.0);

    // At 150s, `time()` is 150.
    assert_eq!(vector("vector(time())")[0].1, 150.0);
    // And it stays a vector when a function is wrapped round it.
    assert_eq!(vector("abs(vector(-3))")[0].1, 3.0);
}

/// A scalar at the top of the query comes back as the one unlabelled
/// series `vector()` would make of it, which is how upstream's
/// evaluator carries one too; the expression's type, not the result,
/// says it is a scalar.
#[test]
fn a_scalar_returning_call_is_one_unlabelled_series() {
    let got = vector("time()");
    assert_eq!(got.len(), 1);
    assert!(got[0].0.is_empty());
    assert_eq!(got[0].1, 150.0);
    assert_eq!(vector("pi()")[0].1, std::f64::consts::PI);
}

/// Upstream evaluates a scalar argument per step; this engine folds it
/// while planning, so one that moves is named rather than guessed at.
/// A one-step query has nothing that can move — this is the range path.
#[test]
fn a_scalar_argument_that_moves_over_the_range_is_unsupported_by_name() {
    let engine = Engine::blocking().unwrap();
    let range = RangeQuery::new(0, 120_000, 60_000);
    let err = engine
        .range_query(source().as_ref(), "clamp_max(temperature, time())", &range)
        .unwrap_err();
    assert!(
        matches!(&err, EngineError::Unsupported(f)
            if f == "the clamp_max function with a scalar argument that changes between steps"),
        "{err}"
    );

    // One that does not move is fine over the same range.
    assert!(engine
        .range_query(source().as_ref(), "clamp_max(temperature, 2 * 3)", &range)
        .is_ok());
    // And over one step, even `time()` does not move.
    assert_eq!(vector("clamp_max(temperature, time())")[1].1, 9.25);
}

/// The arity and the argument types are the parser's job upstream, so
/// they are query errors here, in upstream's words.
#[test]
fn a_call_prometheus_would_not_parse_is_a_query_error() {
    for (query, message) in [
        (
            "abs(temperature, 1)",
            "expected 1 argument(s) in call to \"abs\", got 2",
        ),
        (
            "abs(temperature[5m])",
            "expected type instant vector in call to function \"abs\", got range vector",
        ),
        (
            "clamp(temperature, 1)",
            "expected 3 argument(s) in call to \"clamp\", got 2",
        ),
        (
            "vector(temperature)",
            "expected type scalar in call to function \"vector\", got instant vector",
        ),
        ("nope(temperature)", "unknown function with name \"nope\""),
    ] {
        let err = error(query);
        assert_eq!(err.to_string(), message, "{query}");
        assert!(matches!(err, EngineError::Query(_)), "{query}: {err}");
    }
}

/// What the elementwise operator cannot express is still named as the
/// feature it is, not as a bad query.
#[test]
fn the_functions_that_are_not_elementwise_are_still_unsupported() {
    let err = error("scalar(temperature)");
    assert!(
        matches!(&err, EngineError::Unsupported(f) if f == "the scalar function"),
        "{err}"
    );
}

/// `absent` answers nothing while the series is there, whatever shape
/// the argument takes.
#[test]
fn absent_is_empty_when_the_series_is_there() {
    for query in [
        "absent(temperature)",
        r#"absent(temperature{city="oslo"})"#,
        "absent(sum(temperature))",
        "absent(temperature > -100)",
        "absent(abs(temperature))",
        r#"absent_over_time(temperature{city="oslo"}[1m])"#,
        "absent_over_time(temperature[5m])",
    ] {
        assert_eq!(vector(query), vec![], "{query}");
    }
}

/// With nothing there, the one series carries the labels upstream reads
/// off the selector, not off any data: equality matchers only, a name
/// matched twice dropped, nothing at all above a function or an
/// aggregation.
#[test]
fn absent_carries_the_labels_the_selector_asked_for() {
    let labelled = |pairs: &[(&str, &str)]| {
        vec![(
            pairs
                .iter()
                .map(|(n, v)| (n.to_string(), v.to_string()))
                .collect::<Vec<_>>(),
            1.0,
        )]
    };
    assert_eq!(vector("absent(nonexistent)"), labelled(&[]));
    assert_eq!(
        vector(r#"absent(nonexistent{city="rome", region=~"eu.*"})"#),
        labelled(&[("city", "rome")])
    );
    assert_eq!(
        vector(r#"absent((nonexistent{city="rome"}))"#),
        labelled(&[("city", "rome")])
    );
    assert_eq!(
        vector(r#"absent(temperature{city="bergen", city="tromso", region="north"})"#),
        labelled(&[("region", "north")])
    );
    assert_eq!(
        vector(r#"absent(sum(nonexistent{city="rome"}))"#),
        labelled(&[])
    );
    assert_eq!(
        vector(r#"absent_over_time(nonexistent{city="rome", region!="eu"}[5m])"#),
        labelled(&[("city", "rome")])
    );
    assert_eq!(
        vector(r#"absent(temperature{city="oslo"} offset 1h)"#),
        labelled(&[("city", "oslo")])
    );
}

/// A subquery under `absent_over_time` is the gap it is everywhere
/// else, named as such rather than answered with the wrong labels.
#[test]
fn absent_over_time_of_a_subquery_is_still_unsupported() {
    let err = error("absent_over_time(rate(temperature[1m])[5m:30s])");
    assert!(
        matches!(&err, EngineError::Unsupported(f) if f == "a subquery"),
        "{err}"
    );
}

/// Upstream's `UnaryExpr` arm: `-` negates each sample and drops the
/// name, `+` is the operand as it was, name kept, and a scalar operand
/// is still a scalar.
#[test]
fn unary_minus_negates_and_drops_the_name_and_plus_changes_nothing() {
    let labelled = |city: &str| vec![("city".to_string(), city.to_string())];
    assert_eq!(
        vector("-temperature"),
        vec![(labelled("lima"), 9.75), (labelled("oslo"), -9.25)]
    );
    assert_eq!(vector("- -temperature")[1], (labelled("oslo"), 9.25));
    assert_eq!(vector("-(temperature)"), vector("-temperature"));
    assert_eq!(vector("+temperature"), vector("temperature"));
    assert_eq!(vector("-sum(temperature)")[0].1, 0.5);
    // A sign over a scalar-typed expression folds; `vector` makes it
    // visible as a series.
    assert_eq!(vector("vector(-time())")[0].1, -150.0);
    assert_eq!(vector("-vector(1)")[0].1, -1.0);
}

/// A number written out is a scalar, one unlabelled series like
/// `time()`; the signs `-1` and `-Inf` carry are folded into it, and
/// `NaN` stays NaN.
#[test]
fn a_bare_number_literal_is_one_unlabelled_series() {
    for (query, want) in [
        ("1", 1.0),
        ("-0x1", -1.0),
        ("1e3", 1000.0),
        ("(2.)", 2.0),
        ("+.5", 0.5),
        ("-Inf", f64::NEG_INFINITY),
    ] {
        let got = vector(query);
        assert_eq!(got.len(), 1, "{query}");
        assert!(got[0].0.is_empty(), "{query}");
        assert_eq!(got[0].1, want, "{query}");
    }
    assert!(vector("NaN")[0].1.is_nan());
}

/// A literal answers at every step of a range query, and a string,
/// which upstream gives only to an instant query, is refused in one.
#[test]
fn a_literal_covers_the_range_and_a_string_is_refused_in_one() {
    let engine = Engine::blocking().unwrap();
    let range = RangeQuery::new(0, 120_000, 60_000);
    let batches = engine.range_query(source().as_ref(), "7", &range).unwrap();
    let decoded = promql_engine::series::decode(&batches).unwrap();
    assert_eq!(decoded[0].values(), [7.0, 7.0, 7.0]);

    let err = engine
        .range_query(source().as_ref(), r#"("Foo")"#, &range)
        .unwrap_err();
    assert!(
        matches!(&err, EngineError::Query(m) if m.contains("for range query")),
        "{err}"
    );
}
