//! Plan pins for the topic of `promql-conformance/testdata/prometheus/at_modifier.test`.

use insta::assert_snapshot;

use super::{Bounds, Case};

const RANGE: Bounds = Bounds {
    start_ms: 600_000,
    end_ms: 1_200_000,
    step_ms: 30_000,
};

/// @ takes seconds in PromQL but the plan carries milliseconds, so the pin
/// shows the raw resolved timestamp the plan holds rather than the query's
/// own units.
#[test]
fn an_at_modifier_on_a_range_function() {
    let case = Case::new(
        RANGE,
        &[
            r#"http_requests_total{job="api", pod="api-1"} 0"#,
            r#"http_requests_total{job="web", pod="web-1"} 0"#,
        ],
    );
    let query = "rate(http_requests_total[5m] @ 900)";
    assert_snapshot!(
        "an_at_modifier_on_a_range_function",
        case.plan(query),
        query
    );
}

/// @ pins the sample window on the SeriesEval while the scalar horizon
/// stays beside it. The runtime still reanchors the regression on each
/// evaluation step; the plan only records the pinned window.
#[test]
fn an_at_modifier_on_predict_linear() {
    let case = Case::new(
        RANGE,
        &[
            r#"http_requests_total{job="api", pod="api-1"} 0"#,
            r#"http_requests_total{job="web", pod="web-1"} 0"#,
        ],
    );
    let query = "predict_linear(http_requests_total[5m] @ 900, 3600)";
    assert_snapshot!("an_at_modifier_on_predict_linear", case.plan(query), query);
}
