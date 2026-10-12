//! Plan pins for the topic of `promql-conformance/testdata/prometheus/operators.test`.

use insta::assert_snapshot;

use super::{Bounds, Case};

const RANGE: Bounds = Bounds {
    start_ms: 600_000,
    end_ms: 1_200_000,
    step_ms: 30_000,
};

/// The scalar is folded while planning, so the operator is the elementwise
/// kernel with one operand already a number and no second input at all.
/// __name__ is rebuilt away, as it is for any arithmetic.
#[test]
fn a_vector_against_a_scalar() {
    let case = Case::new(RANGE, &[r#"http_requests_total{job="api", pod="api-1"} 0"#]);
    let query = "http_requests_total * 2";
    assert_snapshot!("a_vector_against_a_scalar", case.plan(query), query);
}

/// Which side the scalar was written on survives as which argument slot it
/// lands in: upstream's swap, and the only thing that separates this plan
/// from the one above.
#[test]
fn a_scalar_on_the_left_of_a_vector() {
    let case = Case::new(RANGE, &[r#"http_requests_total{job="api", pod="api-1"} 0"#]);
    let query = "600 / http_requests_total";
    assert_snapshot!("a_scalar_on_the_left_of_a_vector", case.plan(query), query);
}

/// A comparison without bool answers with the left sample as it stood,
/// __name__ and all, and __name__ is the one label the match signature
/// leaves out. So the aggregation answers with a series per name instead of
/// one per match group, and the two Unnest nodes open that list into a row
/// each before the top projection puts the name back among the group keys.
#[test]
fn a_comparison_between_two_vectors() {
    let case = Case::new(
        RANGE,
        &[
            r#"requests{job="api", pod="api-1"} 0"#,
            r#"errors{job="api", pod="api-1"} 0"#,
        ],
    );
    let query = "requests > errors";
    assert_snapshot!("a_comparison_between_two_vectors", case.plan(query), query);
}

/// Both sides widened to one label schema, unioned, and grouped by the
/// match signature, which for default matching is the result's own label
/// set. A label only one side carries is read as the empty string on the
/// other, which is how the shape spells absent.
#[test]
fn two_vectors_matched_one_to_one() {
    let case = Case::new(
        RANGE,
        &[
            r#"requests{job="api", pod="api-1"} 0"#,
            r#"errors{job="api", zone="eu"} 0"#,
        ],
    );
    let query = "requests / errors";
    assert_snapshot!("two_vectors_matched_one_to_one", case.plan(query), query);
}

/// on(job) makes the signature one column rather than every label the two
/// sides share, and group_left(zone) says the left may be many. Neither
/// shows in the plan's shape: the modifiers ride into the aggregation as
/// their own PromQL spelling, because which series pair and which labels
/// survive is one decision and it is made where the rows meet.
#[test]
fn a_fan_out_matched_on_one_label() {
    let case = Case::new(
        RANGE,
        &[
            r#"requests{job="api", pod="api-1"} 0"#,
            r#"limit{job="api", zone="eu"} 0"#,
        ],
    );
    let query = "requests / on(job) group_left(zone) limit";
    assert_snapshot!("a_fan_out_matched_on_one_label", case.plan(query), query);
}

/// or is the same union and the same grouping as the arithmetic operators:
/// which samples come out of a match group is the only thing that differs,
/// and that is the accumulator's business, not the plan's. What does change
/// is the projection above the union, which keeps __name__ on both sides --
/// a set operator hands a sample back as it found it.
#[test]
fn a_set_operator_matched_on_one_label() {
    let case = Case::new(
        RANGE,
        &[
            r#"requests{job="api", pod="api-1"} 0"#,
            r#"limit{job="api", zone="eu"} 0"#,
        ],
    );
    let query = "requests or on(job) limit";
    assert_snapshot!(
        "a_set_operator_matched_on_one_label",
        case.plan(query),
        query
    );
}

/// The inner comparison keeps every label, __name__ included, and still
/// rebuilds the struct rather than passing the column through. A passed-
/// through column keeps the qualifier of the stage below it, so this branch
/// of the union would answer with binary_0.labels where the other answers
/// with labels, and the union would be ambiguous. The promql_labels call
/// that copies every field across is what makes the name plain again.
#[test]
fn a_filtering_comparison_as_an_operand() {
    let case = Case::new(
        RANGE,
        &[
            r#"requests{job="api", pod="api-1"} 0"#,
            r#"errors{job="api", pod="api-1"} 0"#,
        ],
    );
    let query = "(requests > 100) + errors";
    assert_snapshot!(
        "a_filtering_comparison_as_an_operand",
        case.plan(query),
        query
    );
}

/// The fill values ride into the aggregation with the rest of the
/// modifiers, spelled as the query spelled them -- fill(0) is both sides at
/// once, so it reads back as the pair it stands for. The plan is the
/// pairing's, unchanged: filling in a missing side is a decision about a
/// match group and there is nothing above the grouping that could make it.
#[test]
fn a_fill_value_for_a_side_with_no_series() {
    let case = Case::new(
        RANGE,
        &[
            r#"requests{job="api", pod="api-1"} 0"#,
            r#"limit{job="api", zone="eu"} 0"#,
        ],
    );
    let query = "requests / on(job) fill(0) limit";
    assert_snapshot!(
        "a_fill_value_for_a_side_with_no_series",
        case.plan(query),
        query
    );
}
