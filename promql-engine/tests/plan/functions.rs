//! Plan pins for the topic of `promql-conformance/testdata/prometheus/functions.test`.

use insta::assert_snapshot;

use super::{Bounds, Case};

const RANGE: Bounds = Bounds {
    start_ms: 600_000,
    end_ms: 1_200_000,
    step_ms: 30_000,
};

/// The range function is an aggregate grouped by the whole labels struct,
/// like the selector. rate drops __name__, so the labels are rebuilt field
/// by field above it: dropped below, two series differing only in their
/// name would fold into one group. The fields named here are this case's
/// own, which is what shows that the projection follows the data rather
/// than a fixed schema.
#[test]
fn range_function_over_a_different_label_set() {
    let case = Case::new(
        RANGE,
        &[
            r#"node_cpu_seconds_total{cpu="0", instance="a", mode="idle"} 0"#,
            r#"node_cpu_seconds_total{cpu="1", instance="a", mode="user"} 0"#,
        ],
    );
    let query = "rate(node_cpu_seconds_total[5m])";
    assert_snapshot!(
        "range_function_over_a_different_label_set",
        case.plan(query),
        query
    );
}

/// One projection per function, each over the last: the operator changes
/// only the value, so the shape it runs on is the shape it hands back. The
/// subquery alias is what keeps the inner `labels` and the rebuilt one
/// apart when the two projections are merged.
#[test]
fn an_elementwise_function_over_a_selector() {
    let case = Case::new(RANGE, &[r#"temperature{city="oslo"} 0"#]);
    let query = "abs(temperature)";
    assert_snapshot!(
        "an_elementwise_function_over_a_selector",
        case.plan(query),
        query
    );
}

/// vector(s) asks no store: it is the scalar's value under an empty label
/// set, which is the table a scalar plan already is.
#[test]
fn vector_of_a_scalar() {
    let case = Case::new(RANGE, &[r#"temperature{city="oslo"} 0"#]);
    let query = "vector(time())";
    assert_snapshot!("vector_of_a_scalar", case.plan(query), query);
}

/// A scalar-returning call is a value per step and nothing else, so it is
/// the scalar table itself, with no function node over it; the caller reads
/// the one unlabelled series as a scalar by the expression's type.
#[test]
fn time_at_the_top_of_the_query() {
    let case = Case::new(RANGE, &[r#"temperature{city="oslo"} 0"#]);
    let query = "time()";
    assert_snapshot!("time_at_the_top_of_the_query", case.plan(query), query);
}

/// dateWrapper is simpleFloatFunc's shape with a calendar for the map, so
/// it takes the elementwise projection abs takes, __name__ dropped above
/// the selector.
#[test]
fn a_date_function_over_a_selector() {
    let case = Case::new(RANGE, &[r#"temperature{city="oslo"} 0"#]);
    let query = "day_of_week(temperature)";
    assert_snapshot!("a_date_function_over_a_selector", case.plan(query), query);
}

/// With no vector, dateWrapper reads the evaluation time, which the planner
/// hands the same kernel as the vector(time()) table: no store is asked,
/// and the plan says which function ran rather than folding the calendar
/// into the table.
#[test]
fn a_date_function_without_a_vector() {
    let case = Case::new(RANGE, &[r#"temperature{city="oslo"} 0"#]);
    let query = "year()";
    assert_snapshot!("a_date_function_without_a_vector", case.plan(query), query);
}

/// The selector itself picks each sample's time, so there is no function
/// node: the pick is the selector's trailing argument, and __name__ is
/// dropped above the grouping as a range function drops it.
#[test]
fn timestamp_over_a_selector() {
    let case = Case::new(RANGE, &[r#"temperature{city="oslo"} 0"#]);
    let query = "timestamp(temperature)";
    assert_snapshot!("timestamp_over_a_selector", case.plan(query), query);
}

/// Under abs the samples already carry the step, so timestamp is the
/// elementwise projection reading the timestamp column, and the selector
/// below it picks values as usual.
#[test]
fn timestamp_over_another_function() {
    let case = Case::new(RANGE, &[r#"temperature{city="oslo"} 0"#]);
    let query = "timestamp(abs(temperature))";
    assert_snapshot!("timestamp_over_another_function", case.plan(query), query);
}

/// A counting function takes the same aggregate as rate; only the function-
/// name literal differs, so one registration serves the whole family.
#[test]
fn count_over_time() {
    let case = Case::new(
        RANGE,
        &[
            r#"http_requests_total{job="api", pod="api-1"} 0"#,
            r#"http_requests_total{job="web", pod="web-1"} 0"#,
        ],
    );
    let query = "count_over_time(http_requests_total[10m])";
    assert_snapshot!("count_over_time", case.plan(query), query);
}

/// predict_linear is a range SeriesEval that carries its scalar horizon
/// beside the window. It drops __name__ like other range kernels, so
/// the labels are rebuilt field by field above it.
#[test]
fn predict_linear() {
    let case = Case::new(
        RANGE,
        &[
            r#"http_requests_total{job="api", pod="api-1"} 0"#,
            r#"http_requests_total{job="web", pod="web-1"} 0"#,
        ],
    );
    let query = "predict_linear(http_requests_total[5m], 3600)";
    assert_snapshot!("predict_linear", case.plan(query), query);
}

/// deriv is an ordinary one-arg range SeriesEval: same boundary as rate,
/// slope from linear_regression with intercept at the first sample.
#[test]
fn deriv() {
    let case = Case::new(
        RANGE,
        &[
            r#"http_requests_total{job="api", pod="api-1"} 0"#,
            r#"http_requests_total{job="web", pod="web-1"} 0"#,
        ],
    );
    let query = "deriv(http_requests_total[5m])";
    assert_snapshot!("deriv", case.plan(query), query);
}

/// Four store partitions, each series whole in one of them and chunked
/// within it. Each partition's Partial folds its own series Sorted and
/// emits them finished; a merge on labels hands them in order to one Final,
/// which stays Sorted too.
#[test]
fn range_function_over_chunks_in_four_partitions() {
    let case = Case::new(RANGE, &[r#"x{pod="a"} 1+1x19"#, r#"x{pod="b"} 2+3x19"#])
        .chunked(150_000)
        .partitions(4);
    let query = "rate(x[5m])";
    assert_snapshot!(
        "range_function_over_chunks_in_four_partitions",
        case.plan(query),
        query
    );
    assert_snapshot!(
        "range_function_over_chunks_in_four_partitions_physical",
        case.physical(query),
        query
    );
}

/// sort is a Sort node over the series rows and nothing else: the
/// projection below it is the bare selector's. Two keys, because Prometheus
/// puts NaN last whichever way the values go. Equal bounds because sort
/// only orders an instant result.
#[test]
fn sort_over_an_instant_selector() {
    let case = Case::new(
        Bounds {
            start_ms: 1_200_000,
            end_ms: 1_200_000,
            step_ms: 30_000,
        },
        &[
            r#"http_requests_total{job="api", pod="api-1"} 0"#,
            r#"http_requests_total{job="api", pod="api-2"} 0"#,
        ],
    );
    let query = "sort(http_requests_total)";
    assert_snapshot!("sort_over_an_instant_selector", case.plan(query), query);
}

/// The one aggregate with no group key at all, the block included: absent
/// has to answer when no row ever arrived, and a block no series reached is
/// never sent. The labels are literals the planner read off the selector's
/// equality matchers, not fields of any input, and the row is stamped with
/// the whole range as a scalar's is.
#[test]
fn absent_of_a_selector() {
    let case = Case::new(RANGE, &[r#"temperature{city="oslo"} 0"#]);
    let query = r#"absent(temperature{city="oslo", region=~"eu.*"})"#;
    assert_snapshot!("absent_of_a_selector", case.plan(query), query);
}

/// Anything but a selector has no labels to read, so the struct is empty,
/// and the argument plans as it would anywhere else.
#[test]
fn absent_of_an_aggregation() {
    let case = Case::new(RANGE, &[r#"temperature{city="oslo"} 0"#]);
    let query = "absent(sum(temperature))";
    assert_snapshot!("absent_of_an_aggregation", case.plan(query), query);
}

/// present_over_time below the same complement: upstream's
/// funcAbsentOverTime is 1 per non-empty window, and the engine then
/// complements the steps. The store is told absent_over_time, the name
/// whose samples it may skip.
#[test]
fn absent_over_time_of_a_range_selector() {
    let case = Case::new(RANGE, &[r#"temperature{city="oslo"} 0"#]);
    let query = r#"absent_over_time(temperature{city="oslo"}[5m])"#;
    assert_snapshot!(
        "absent_over_time_of_a_range_selector",
        case.plan(query),
        query
    );
}

/// The Sort sits on the row absent made, whose labels are literals and
/// whose samples are the kernel's: the value key reads the same column it
/// reads on a selected row.
#[test]
fn sort_over_absent() {
    let case = Case::new(
        Bounds {
            start_ms: 1_200_000,
            end_ms: 1_200_000,
            step_ms: 30_000,
        },
        &[r#"temperature{city="oslo"} 0"#],
    );
    let query = r#"sort(absent(temperature{city="oslo"}))"#;
    assert_snapshot!("sort_over_absent", case.plan(query), query);
}

/// funcScalar answers at every step, so like absent it is an aggregate with
/// no group key, which finalizes a row even when no input arrived, and the
/// row is stamped with the whole range as a scalar's is. The labels are the
/// empty struct.
#[test]
fn scalar_of_a_vector() {
    let case = Case::new(RANGE, &[r#"temperature{city="oslo"} 0"#]);
    let query = r#"scalar(temperature{city="oslo"})"#;
    assert_snapshot!("scalar_of_a_vector", case.plan(query), query);
}
