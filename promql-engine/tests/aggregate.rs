//! End to end: aggregations over the in-memory source, expectations worked
//! by hand from the semantics.

use std::collections::BTreeMap;
use std::sync::Arc;

use promql_engine::{Engine, EngineError, MemorySeriesSource, RangeQuery, Series};
use promql_parser::SeriesDescription;

fn load(lines: &[&str]) -> Vec<SeriesDescription> {
    lines
        .iter()
        .map(|l| promql_parser::parse_series_desc(l).expect("series line parses"))
        .collect()
}

/// Three series at 30s: two pods on route "/", one on "/api"; the "/api"
/// one is short (4 samples).
fn source() -> Arc<MemorySeriesSource> {
    Arc::new(MemorySeriesSource::from_descriptions(
        &load(&[
            r#"http_requests_total{pod="envoy-1", route="/"} 1+1x10"#,
            r#"http_requests_total{pod="envoy-2", route="/"} 1+2x10"#,
            r#"http_requests_total{pod="envoy-3", route="/api"} 100+0x3"#,
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

#[test]
fn sum_over_everything_yields_one_unlabelled_series() {
    let out = query(
        "sum(http_requests_total)",
        RangeQuery::new(0, 60_000, 30_000),
    );
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].labels().count(), 0);
    // t=0: 1+1+100, t=30s: 2+3+100, t=60s: 3+5+100.
    assert_eq!(out[0].timestamps(), [0, 30_000, 60_000]);
    assert_eq!(out[0].values(), [102.0, 105.0, 108.0]);
}

#[test]
fn by_keeps_only_the_listed_labels() {
    let out = query(
        "sum by (route) (http_requests_total)",
        RangeQuery::new(0, 30_000, 30_000),
    );
    assert_eq!(out.len(), 2);
    let by_route: BTreeMap<&str, &Series> = out.iter().map(|s| (s.label("route"), s)).collect();
    assert_eq!(by_route["/"].timestamps(), [0, 30_000]);
    assert_eq!(by_route["/"].values(), [2.0, 5.0]);
    assert_eq!(by_route["/api"].timestamps(), [0, 30_000]);
    assert_eq!(by_route["/api"].values(), [100.0, 100.0]);
    assert!(out.iter().all(|s| s.labels().count() == 1), "{out:?}");
}

#[test]
fn without_drops_the_listed_labels_and_the_metric_name() {
    let out = query(
        "max without (pod) (http_requests_total)",
        RangeQuery::new(0, 30_000, 30_000),
    );
    assert_eq!(out.len(), 2);
    let slash = out.iter().find(|s| s.label("route") == "/").unwrap();
    assert_eq!(slash.labels().collect::<Vec<_>>(), vec![("route", "/")]);
    assert_eq!(slash.timestamps(), [0, 30_000]);
    assert_eq!(slash.values(), [1.0, 3.0]);
}

#[test]
fn a_group_lives_exactly_as_long_as_its_series() {
    // envoy-3 has samples at 0, 30, 60, 90s and is visible through the 5m
    // lookback until t < 90s + 5m = 390s. Its group ends where it ends;
    // the "/" group runs to the end of the query.
    let out = query(
        "count by (route) (http_requests_total)",
        RangeQuery::new(0, 600_000, 30_000),
    );
    let api = out.iter().find(|s| s.label("route") == "/api").unwrap();
    assert_eq!(api.timestamps().len(), 13);
    assert_eq!(api.timestamps().last(), Some(&360_000));
    assert_eq!(api.values().last(), Some(&1.0));
    // The "/" series end at 300s and expire at 600s: 20 steps, 0..=570s.
    let slash = out.iter().find(|s| s.label("route") == "/").unwrap();
    assert_eq!(slash.timestamps().len(), 20);
    assert_eq!(slash.timestamps().last(), Some(&570_000));
    assert!(slash.values().iter().all(|v| *v == 2.0));
}

#[test]
fn avg_and_the_spread_statistics() {
    let out = query(
        "avg by (route) (http_requests_total)",
        RangeQuery::new(30_000, 30_000, 30_000),
    );
    let slash = out.iter().find(|s| s.label("route") == "/").unwrap();
    assert_eq!(slash.timestamps(), [30_000]);
    assert_eq!(slash.values(), [2.5]);

    // Values at 30s on "/": 2 and 3. Population variance 0.25, stddev 0.5.
    let out = query(
        "stdvar by (route) (http_requests_total)",
        RangeQuery::new(30_000, 30_000, 30_000),
    );
    let slash = out.iter().find(|s| s.label("route") == "/").unwrap();
    assert_eq!(slash.values(), [0.25]);
    let out = query(
        "stddev by (route) (http_requests_total)",
        RangeQuery::new(30_000, 30_000, 30_000),
    );
    let slash = out.iter().find(|s| s.label("route") == "/").unwrap();
    assert_eq!(slash.values(), [0.5]);
}

#[test]
fn by_can_keep_the_metric_name() {
    let out = query(
        r#"count by (__name__) ({__name__=~".+"})"#,
        RangeQuery::new(0, 0, 30_000),
    );
    assert_eq!(out.len(), 1);
    assert_eq!(
        out[0].labels().collect::<Vec<_>>(),
        vec![("__name__", "http_requests_total")]
    );
    assert_eq!(out[0].timestamps(), [0]);
    assert_eq!(out[0].values(), [3.0]);
}

/// `avg`'s overflow switch must not trip on a group's very first sample:
/// a lone `+Inf` averaged with a plain value must stay `+Inf`, not fold
/// to NaN. Own fixture, since [`source`] has no infinite series.
#[test]
fn avg_of_a_group_with_an_infinite_series_stays_infinite() {
    let source = Arc::new(MemorySeriesSource::from_descriptions(
        &load(&[
            r#"pos_inf{group="a", pod="p1"} +Inf +Inf +Inf"#,
            r#"pos_inf{group="a", pod="p2"} 2+0x2"#,
        ]),
        30.0,
    ));
    let batches = Engine::blocking()
        .unwrap()
        .range_query(
            source.as_ref(),
            "avg by (group) (pos_inf)",
            &RangeQuery::new(0, 60_000, 30_000),
        )
        .unwrap();
    let out = promql_engine::series::decode(&batches).unwrap();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].timestamps(), [0, 30_000, 60_000]);
    assert!(
        out[0].values().iter().all(|v| *v == f64::INFINITY),
        "{:?}",
        out[0].values()
    );
}

#[test]
fn an_aggregation_over_nothing_is_nothing() {
    let out = query("sum(no_such_metric)", RangeQuery::new(0, 60_000, 30_000));
    assert!(out.is_empty(), "{out:?}");
    let out = query(
        "sum by (pod) (no_such_metric)",
        RangeQuery::new(0, 60_000, 30_000),
    );
    assert!(out.is_empty(), "{out:?}");
}

#[test]
fn aggregations_nest() {
    let out = query(
        "max(sum by (route) (http_requests_total))",
        RangeQuery::new(0, 30_000, 30_000),
    );
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].timestamps(), [0, 30_000]);
    assert_eq!(out[0].values(), [100.0, 100.0]);
}

#[test]
fn parameterized_aggregations_are_named_as_unsupported() {
    let engine = Engine::blocking().unwrap();
    for (q, what) in [
        ("topk(2, http_requests_total)", "the topk aggregation"),
        (
            "quantile(0.5, http_requests_total)",
            "the quantile aggregation",
        ),
        (
            "count_values(\"v\", http_requests_total)",
            "the count_values aggregation",
        ),
    ] {
        match engine.range_query(source().as_ref(), q, &RangeQuery::new(0, 0, 30_000)) {
            Err(EngineError::Unsupported(f)) => assert_eq!(f, what, "{q}"),
            other => panic!("{q}: {other:?}"),
        }
    }
}

#[tokio::test]
async fn the_plan_is_an_aggregate_over_the_label_fields() {
    let engine = Engine::new();
    let source = source();
    let plan = engine
        .plan_async(
            source.as_ref(),
            "sum by (route) (http_requests_total)",
            &RangeQuery::new(0, 60_000, 30_000),
        )
        .await
        .unwrap();
    let rendered = promql_engine::explain_plan(&plan);
    assert!(
        rendered.contains("Aggregate: sum by (route) (samples, 0..60000 step 30s) per block"),
        "{rendered}"
    );
    assert!(rendered.contains("SeriesEval: vector_selector(samples, 0..60000 step 30s, lookback 5m) per series per block"), "{rendered}");
    assert_eq!(
        promql_engine::series::label_names(plan.schema().as_arrow()),
        ["route"]
    );
}
