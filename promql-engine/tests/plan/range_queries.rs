//! Plan pins for the topic of `promql-conformance/testdata/prometheus/range_queries.test`.

use insta::assert_snapshot;

use super::{Bounds, Case};

const RANGE: Bounds = Bounds {
    start_ms: 60_000,
    end_ms: 60_000,
    step_ms: 1000,
};

/// No aggregate and no grid: matrixSelector keeps the samples of one window
/// under their own timestamps, so the scan's rows are filtered where they
/// lie. The window is the evaluation time less the range, open at its
/// start.
#[test]
fn a_range_selector_answered_by_an_instant_query() {
    let case = Case::new(RANGE, &[r#"some_metric{env="a"} 0"#]).instant();
    let query = "some_metric[1m]";
    assert_snapshot!(
        "a_range_selector_answered_by_an_instant_query",
        case.plan(query),
        query
    );
}
