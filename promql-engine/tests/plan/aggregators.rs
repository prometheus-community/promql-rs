//! Plan pins for the topic of `promql-conformance/testdata/prometheus/aggregators.test`.

use insta::assert_snapshot;

use super::{Bounds, Case};

const RANGE: Bounds = Bounds {
    start_ms: 600_000,
    end_ms: 1_200_000,
    step_ms: 30_000,
};

/// An ungrouped sum stacks on the selector aggregate: two aggregates, the
/// lower grouped by series, the upper by nothing.
#[test]
fn aggregation_over_an_instant_selector() {
    let case = Case::new(
        RANGE,
        &[
            r#"http_requests_total{job="api", pod="api-1"} 0"#,
            r#"http_requests_total{job="web", pod="web-1"} 0"#,
        ],
    );
    let query = "sum(http_requests_total)";
    assert_snapshot!(
        "aggregation_over_an_instant_selector",
        case.plan(query),
        query
    );
}

/// by (job) becomes an ordinary DataFusion Aggregate, so grouping and the
/// two-phase split are the engine's rather than hand-rolled. Only the
/// grouped label survives into the top projection.
#[test]
fn grouped_aggregation_over_a_range_function() {
    let case = Case::new(
        RANGE,
        &[
            r#"http_requests_total{job="api", pod="api-1"} 0"#,
            r#"http_requests_total{job="web", pod="web-1"} 0"#,
        ],
    );
    let query = "sum by (job) (rate(http_requests_total[5m]))";
    assert_snapshot!(
        "grouped_aggregation_over_a_range_function",
        case.plan(query),
        query
    );
}

/// The rate aggregate runs SinglePartitioned per store partition. The
/// projection above it computes the group key as a new expression, so
/// Hash(labels) does not carry through it and the upper aggregate splits
/// into a Partial per partition, a hash shuffle on the block columns and
/// the group key, and a FinalPartitioned. The shuffle keeps block order, so
/// the Final emits a block once every partition has passed it instead of
/// holding every block to the end. Its width is the session's
/// target_partitions, which the harness prints as a name.
#[test]
fn grouped_aggregation_over_chunks_in_four_partitions() {
    let case = Case::new(RANGE, &[r#"x{pod="a"} 1+1x19"#, r#"x{pod="b"} 2+3x19"#])
        .chunked(150_000)
        .partitions(4);
    let query = "sum by (pod) (rate(x[5m]))";
    assert_snapshot!(
        "grouped_aggregation_over_chunks_in_four_partitions",
        case.plan(query),
        query
    );
    assert_snapshot!(
        "grouped_aggregation_over_chunks_in_four_partitions_physical",
        case.physical(query),
        query
    );
}

/// Sixteen series, eight per pod, spread so that both groups occur in every
/// store partition: the shape is the same as with one series per pod, which
/// is the point. Each Partial emits a partial sum per group it saw, and the
/// shuffle brings a group's partials together.
#[test]
fn grouped_aggregation_with_every_group_in_every_partition() {
    let case = Case::new(
        RANGE,
        &[
            r#"x{pod="a", i="0"} 1+1x19"#,
            r#"x{pod="a", i="1"} 1+1x19"#,
            r#"x{pod="a", i="2"} 1+1x19"#,
            r#"x{pod="a", i="3"} 1+1x19"#,
            r#"x{pod="a", i="4"} 1+1x19"#,
            r#"x{pod="a", i="5"} 1+1x19"#,
            r#"x{pod="a", i="6"} 1+1x19"#,
            r#"x{pod="a", i="7"} 1+1x19"#,
            r#"x{pod="b", i="0"} 2+3x19"#,
            r#"x{pod="b", i="1"} 2+3x19"#,
            r#"x{pod="b", i="2"} 2+3x19"#,
            r#"x{pod="b", i="3"} 2+3x19"#,
            r#"x{pod="b", i="4"} 2+3x19"#,
            r#"x{pod="b", i="5"} 2+3x19"#,
            r#"x{pod="b", i="6"} 2+3x19"#,
            r#"x{pod="b", i="7"} 2+3x19"#,
        ],
    )
    .chunked(150_000)
    .partitions(4);
    let query = "sum by (pod) (rate(x[5m]))";
    assert_snapshot!(
        "grouped_aggregation_with_every_group_in_every_partition",
        case.plan(query),
        query
    );
    assert_snapshot!(
        "grouped_aggregation_with_every_group_in_every_partition_physical",
        case.physical(query),
        query
    );
}

/// Twelve labels per series, grouped by three, the shape a wide store
/// produces at scale: the range function rebuilds every label but __name__,
/// and DataFusion extracts each group key through that rebuilt struct. The
/// logical struct folds once its run of keys reaches eight, and each
/// extracted key reads as the one field it is rather than the whole struct
/// spelled out per key.
#[test]
fn grouped_aggregation_over_a_wide_label_set() {
    let case = Case::new(
        RANGE,
        &[
            r#"x{cloud="aws", cluster="c1", container="app", host="h1", job="api", namespace="prod", region="eu", service="quote", team="t1", os_name="linux", os_type="unix", os_version="6"} 1"#,
            r#"x{cloud="aws", cluster="c1", container="app", host="h1", job="api", namespace="prod", region="eu", service="quote", team="t1", os_name="darwin", os_type="unix", os_version="24"} 2"#,
        ],
    );
    let query = "sum by (os_name, os_type, os_version) (sum_over_time(x[5m]))";
    assert_snapshot!(
        "grouped_aggregation_over_a_wide_label_set",
        case.plan(query),
        query
    );
    assert_snapshot!(
        "grouped_aggregation_over_a_wide_label_set_physical",
        case.physical(query),
        query
    );
}

/// With no group key the block columns are still a key: a Partial per
/// partition and a shuffle on the block, order kept, so each block's sum is
/// final once every partition has passed it.
#[test]
fn ungrouped_aggregation_over_chunks_in_four_partitions() {
    let case = Case::new(RANGE, &[r#"x{pod="a"} 1+1x19"#, r#"x{pod="b"} 2+3x19"#])
        .chunked(150_000)
        .partitions(4);
    let query = "sum(rate(x[5m]))";
    assert_snapshot!(
        "ungrouped_aggregation_over_chunks_in_four_partitions",
        case.plan(query),
        query
    );
    assert_snapshot!(
        "ungrouped_aggregation_over_chunks_in_four_partitions_physical",
        case.physical(query),
        query
    );
}

/// The planner turns without (pod) into by over whatever labels are left,
/// so the plan only ever holds a by clause: this pins that sum by (job) is
/// what without (pod) actually plans to, not a without-shaped node the
/// renderer would need to reconstruct.
#[test]
fn without_resolves_to_the_remaining_keys_at_plan_time() {
    let case = Case::new(
        RANGE,
        &[
            r#"http_requests_total{job="api", pod="api-1"} 0"#,
            r#"http_requests_total{job="web", pod="web-1"} 0"#,
        ],
    );
    let query = "sum without (pod) (rate(http_requests_total[5m]))";
    assert_snapshot!(
        "without_resolves_to_the_remaining_keys_at_plan_time",
        case.plan(query),
        query
    );
}
