//! End to end: range functions over the in-memory source, expectations
//! worked by hand from the semantics.

use std::sync::Arc;

use promql_engine::{Engine, EngineError, MemorySeriesSource, RangeQuery, Series};
use promql_parser::SeriesDescription;

fn load(lines: &[&str]) -> Vec<SeriesDescription> {
    lines
        .iter()
        .map(|l| promql_parser::parse_series_desc(l).expect("series line parses"))
        .collect()
}

/// Two counters at 30s: +1 and +2 per step, 16 and 19 samples long.
fn source() -> Arc<MemorySeriesSource> {
    Arc::new(MemorySeriesSource::from_descriptions(
        &load(&[
            r#"http_requests_total{pod="envoy-1"} 1+1x15"#,
            r#"http_requests_total{pod="envoy-2"} 1+2x18"#,
        ]),
        30.0,
    ))
}

fn query(q: &str, range: RangeQuery) -> Vec<Series> {
    let batches = Engine::blocking()
        .unwrap()
        .range_query(source().as_ref(), q, &range)
        .unwrap();
    promql_engine::series::decode(&batches).unwrap()
}

fn close(a: f64, b: f64) -> bool {
    (a - b).abs() < 1e-9
}

#[test]
fn rate_drops_the_metric_name_and_gives_the_slope() {
    // At 300s the (0, 300s] window holds ten samples 30s apart; the
    // first is one interval from the boundary so extrapolation reaches
    // it: envoy-1 rises 1/30 per second, envoy-2 2/30.
    let out = query(
        "rate(http_requests_total[5m])",
        RangeQuery::new(300_000, 300_000, 30_000),
    );
    assert_eq!(out.len(), 2);
    assert_eq!(
        out[0].labels().collect::<Vec<_>>(),
        vec![("pod", "envoy-1")]
    );
    assert!(close(out[0].values()[0], 1.0 / 30.0), "{out:?}");
    assert!(close(out[1].values()[0], 2.0 / 30.0), "{out:?}");
}

#[test]
fn increase_and_the_over_time_family_at_one_step() {
    let at = RangeQuery::new(300_000, 300_000, 30_000);
    let one = |q: &str| query(q, at)[0].values()[0];
    assert!(close(one("increase(http_requests_total[5m])"), 10.0));
    assert_eq!(one("sum_over_time(http_requests_total[5m])"), 65.0);
    assert_eq!(one("avg_over_time(http_requests_total[5m])"), 6.5);
    assert_eq!(one("min_over_time(http_requests_total[5m])"), 2.0);
    assert_eq!(one("max_over_time(http_requests_total[5m])"), 11.0);
    assert_eq!(one("count_over_time(http_requests_total[5m])"), 10.0);
    assert_eq!(one("last_over_time(http_requests_total[5m])"), 11.0);
    assert_eq!(one("present_over_time(http_requests_total[5m])"), 1.0);
    assert_eq!(one("changes(http_requests_total[5m])"), 9.0);
    assert_eq!(one("resets(http_requests_total[5m])"), 0.0);
    assert!(close(one("irate(http_requests_total[5m])"), 1.0 / 30.0));
    assert_eq!(one("idelta(http_requests_total[5m])"), 1.0);
}

#[test]
fn last_over_time_keeps_the_metric_name() {
    let out = query(
        "last_over_time(http_requests_total[5m])",
        RangeQuery::new(0, 0, 30_000),
    );
    assert_eq!(
        out[0].labels().collect::<Vec<_>>(),
        vec![("__name__", "http_requests_total"), ("pod", "envoy-1")]
    );
    assert_eq!(out[0].timestamps(), [0]);
    assert_eq!(out[0].values(), [1.0]);
}

#[test]
fn a_series_ends_when_its_window_empties() {
    // envoy-1's last sample is at 450s. A (t-1m, t] window is empty once
    // t - 60s >= 450s, i.e. from t = 510s on; at 480s it still holds 450s.
    let out = query(
        "count_over_time(http_requests_total[1m])",
        RangeQuery::new(0, 900_000, 30_000),
    );
    let envoy1 = &out[0];
    assert_eq!(envoy1.timestamps().last(), Some(&480_000));
    assert_eq!(envoy1.values().last(), Some(&1.0));
    assert_eq!(envoy1.timestamps()[..2], [0, 30_000]);
    assert_eq!(envoy1.values()[..2], [1.0, 2.0]);
}

#[test]
fn offset_and_at_shift_the_window() {
    let out = query(
        "count_over_time(http_requests_total[5m] offset 1m)",
        RangeQuery::new(360_000, 360_000, 30_000),
    );
    assert_eq!(out[0].timestamps(), [360_000]);
    assert_eq!(out[0].values(), [10.0]);

    let out = query(
        "count_over_time(http_requests_total[5m] @ 300)",
        RangeQuery::new(0, 60_000, 30_000),
    );
    assert_eq!(out[0].timestamps(), [0, 30_000, 60_000]);
    assert_eq!(out[0].values(), [10.0, 10.0, 10.0]);

    let out = query(
        "count_over_time(http_requests_total[5m] @ end())",
        RangeQuery::new(0, 60_000, 30_000),
    );
    // (−4m, 60s]: samples at 0, 30s, 60s.
    assert_eq!(out[0].timestamps(), [0, 30_000, 60_000]);
    assert_eq!(out[0].values(), [3.0, 3.0, 3.0]);
}

#[test]
fn aggregations_compose_over_range_functions() {
    let out = query(
        "sum(rate(http_requests_total[5m]))",
        RangeQuery::new(300_000, 300_000, 30_000),
    );
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].labels().count(), 0);
    assert!(close(out[0].values()[0], 3.0 / 30.0), "{out:?}");

    let out = query(
        "avg by (pod) (increase(http_requests_total[5m]))",
        RangeQuery::new(300_000, 300_000, 30_000),
    );
    assert_eq!(out.len(), 2);
    let envoy1 = out
        .iter()
        .find(|s| s.label("pod") == "envoy-1")
        .expect("envoy-1 group");
    assert_eq!(envoy1.labels().count(), 1);
    assert!(close(envoy1.values()[0], 10.0), "{out:?}");
}

#[test]
fn what_is_still_unsupported_is_named() {
    let engine = Engine::blocking().unwrap();
    for (q, what) in [
        ("rate(http_requests_total[5m:1m])", "a subquery"),
        (
            "quantile_over_time(0.5, http_requests_total[5m])",
            "the quantile_over_time function",
        ),
    ] {
        match engine.range_query(source().as_ref(), q, &RangeQuery::new(0, 0, 30_000)) {
            Err(EngineError::Unsupported(f)) => assert_eq!(f, what, "{q}"),
            other => panic!("{q}: {other:?}"),
        }
    }
}

#[tokio::test]
async fn the_plan_keeps_the_range_kernel_and_modifiers_in_typed_evaluation() {
    let engine = Engine::new();
    let source = source();
    let plan = engine
        .plan_async(
            source.as_ref(),
            "rate(http_requests_total[5m] offset 1m)",
            &RangeQuery::new(0, 600_000, 30_000),
        )
        .await
        .unwrap();
    let rendered = promql_engine::explain_plan(&plan);
    assert!(
        rendered.contains(
            "SeriesEval: rate(samples[5m] offset 1m, 0..600000 step 30s) per series per block"
        ),
        "{rendered}"
    );
    assert!(!rendered.contains("promql_range_function"), "{rendered}");
    // Metric-name removal remains above full-identity evaluation.
    assert_eq!(
        promql_engine::series::label_names(plan.schema().as_arrow()),
        ["pod"]
    );
    let mut stack = vec![&plan];
    let mut evaluations = 0;
    while let Some(node) = stack.pop() {
        if let datafusion::logical_expr::LogicalPlan::Extension(extension) = node {
            assert_eq!(extension.node.name(), "SeriesEval");
            evaluations += 1;
            assert_eq!(
                promql_engine::series::label_names(extension.node.inputs()[0].schema().as_arrow()),
                ["__name__", "pod"]
            );
        }
        stack.extend(node.inputs());
    }
    assert_eq!(evaluations, 1);
}

/// Two metrics with one label set but the name, plus a third pod so the
/// queries that are allowed still have rows.
fn two_metrics() -> Arc<MemorySeriesSource> {
    Arc::new(MemorySeriesSource::from_descriptions(
        &load(&[
            r#"http_requests_total{pod="envoy-1"} 1+1x15"#,
            r#"http_errors_total{pod="envoy-1"} 1+1x15"#,
            r#"http_requests_total{pod="envoy-2"} 1+2x18"#,
        ]),
        30.0,
    ))
}

#[test]
fn dropping_the_metric_name_may_not_leave_one_label_set_twice() {
    let engine = Engine::blocking().unwrap();
    let source = two_metrics();
    let at = RangeQuery::new(300_000, 300_000, 30_000);
    let run = |q: &str| engine.range_query(source.as_ref(), q, &at);

    // Both series lose `__name__` and become `{pod="envoy-1"}`.
    let err = run(r#"rate({pod="envoy-1"}[1m])"#).unwrap_err();
    assert_eq!(
        err.to_string(),
        "vector cannot contain metrics with the same labelset"
    );

    // An aggregation over the same rows is not a duplicate: upstream
    // removes the name after evaluating, so the check only ever sees the
    // one series the sum produced.
    for q in [
        r#"sum(rate({pod="envoy-1"}[1m]))"#,
        r#"sum by (pod)(rate({pod="envoy-1"}[1m]))"#,
    ] {
        let out = run(q).unwrap_or_else(|e| panic!("{q}: {e}"));
        let series = promql_engine::series::decode(&out).unwrap();
        assert_eq!(series.len(), 1, "{q}: {series:?}");
    }

    // A pinned name leaves one row, and a function that keeps the name
    // leaves two rows that are still distinct.
    assert!(run(r#"rate(http_requests_total{pod="envoy-1"}[1m])"#).is_ok());
    assert!(run(r#"last_over_time({pod="envoy-1"}[1m])"#).is_ok());
}

#[test]
fn dropping_the_metric_name_rejects_duplicate_label_sets() {
    let source = MemorySeriesSource::from_descriptions(&load(&["foo 1", "bar 2"]), 30.0);
    let engine = Engine::blocking().unwrap();

    // Both series contribute at t=0. Dropping their only distinguishing
    // label would produce two samples with the same empty label set.
    let err = engine
        .range_query(
            &source,
            r#"count_over_time({__name__=~"foo|bar"}[1m])"#,
            &RangeQuery::new(0, 0, 30_000),
        )
        .expect_err("overlapping series with the same output labels must be rejected");
    assert!(err.to_string().contains("same labelset"), "{err}");
}

#[test]
fn a_series_with_no_output_points_cannot_collide() {
    // Both series share `pod` and lose `__name__` under rate(). foo has
    // enough samples for rate to produce a slope at t=300s; bar has only
    // one sample ever, so rate over it never has two points to compare
    // and the row it contributes has no output points at any step.
    let source = MemorySeriesSource::from_descriptions(
        &load(&[r#"foo{pod="envoy-1"} 1+1x15"#, r#"bar{pod="envoy-1"} 5"#]),
        30.0,
    );
    let engine = Engine::blocking().unwrap();
    let out = engine
        .range_query(
            &source,
            r#"rate({pod="envoy-1"}[1m])"#,
            &RangeQuery::new(300_000, 300_000, 30_000),
        )
        .expect("bar's empty row must not be counted as a collision with foo's");
    let series = promql_engine::series::decode(&out).unwrap();
    assert_eq!(series.len(), 1, "{series:?}");
}

fn instant() -> RangeQuery {
    RangeQuery::new(300_000, 300_000, 30_000)
}

fn assert_refuses_inside_runtime(
    result: Result<Vec<datafusion::arrow::array::RecordBatch>, EngineError>,
) {
    match result {
        Err(EngineError::Runtime(msg)) => assert!(msg.contains("range_query_async"), "{msg}"),
        other => panic!("expected EngineError::Runtime, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn blocking_query_inside_a_multi_thread_runtime_is_an_error() {
    let engine = Engine::blocking().unwrap();
    let result = engine.range_query(source().as_ref(), "http_requests_total", &instant());
    assert_refuses_inside_runtime(result);
    // Dropping here also exercises the Drop path from inside a runtime.
}

#[tokio::test(flavor = "current_thread")]
async fn blocking_query_inside_a_current_thread_runtime_is_an_error() {
    let engine = Engine::blocking().unwrap();
    let result = engine.range_query(source().as_ref(), "http_requests_total", &instant());
    assert_refuses_inside_runtime(result);
}

#[test]
fn dropping_a_blocking_engine_outside_a_runtime_works() {
    let engine = Engine::blocking().unwrap();
    engine
        .range_query(source().as_ref(), "http_requests_total", &instant())
        .unwrap();
}

#[tokio::test]
async fn dropping_a_blocking_engine_inside_a_task_does_not_panic() {
    let engine = Engine::blocking().unwrap();
    tokio::spawn(async move { drop(engine) }).await.unwrap();
}
