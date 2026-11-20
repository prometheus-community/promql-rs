//! Plan pins for the topic of `promql-conformance/testdata/prometheus/name_label_dropping.test`.

use insta::assert_snapshot;

use super::{Bounds, Case};

const RANGE: Bounds = Bounds {
    start_ms: 600_000,
    end_ms: 1_200_000,
    step_ms: 30_000,
};

/// `label_replace` reads the name `rate` marked, so `rate` keeps it, and
/// only the `promql_drop_name` projection over the root drops it.
#[test]
fn a_rewrite_reads_the_name_a_range_function_marked() {
    let case = Case::new(RANGE, &[r#"http_requests_total{job="api"} 0"#]);
    let query =
        r#"label_replace(rate(http_requests_total[5m]), "metric", "$1", "__name__", "(.+)")"#;
    assert_snapshot!(
        "a_rewrite_reads_the_name_a_range_function_marked",
        case.plan(query),
        query
    );
}

/// The sides of `or` disagree, so the mark becomes the per-row
/// `__promql_drop_name__` label, and `sum by (__name__)` carries it on as
/// a group's `max` over the members that answered.
#[test]
fn grouping_by_name_over_sides_that_disagree() {
    let case = Case::new(RANGE, &[r#"http_requests_total{job="api"} 0"#]);
    let query = r#"sum by (__name__) (http_requests_total or rate(http_requests_total[5m]))"#;
    assert_snapshot!(
        "grouping_by_name_over_sides_that_disagree",
        case.plan(query),
        query
    );
}
