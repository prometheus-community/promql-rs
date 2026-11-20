//! Delayed `__name__` removal end to end: each operator either marks a
//! series or keeps its mark, and only the result drops the metadata labels.

use std::sync::Arc;

use promql_engine::{Engine, InstantResult, MemorySeriesSource, RangeQuery};

const AT_MS: i64 = 600_000;

fn source() -> Arc<MemorySeriesSource> {
    let lines = [
        r#"x{__type__="counter", a="1"} 0+10x10"#,
        r#"x{__type__="counter", a="2"} 0+30x10"#,
        r#"y{a="3"} 0+20x10"#,
        // One sample in the window: `rate` answers nothing for it.
        r#"z{a="1"} _x9 5"#,
        r#"z{a="2"} 0+60x10"#,
    ];
    let descriptions: Vec<_> = lines
        .iter()
        .map(|line| promql_parser::parse_series_desc(line).expect("series line parses"))
        .collect();
    Arc::new(MemorySeriesSource::from_descriptions(&descriptions, 60.0))
}

type Labels = Vec<(String, String)>;

fn labels(pairs: &[(&str, &str)]) -> Labels {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

fn vector(query: &str) -> Vec<(Labels, f64)> {
    let batches = Engine::blocking()
        .unwrap()
        .range_query(
            source().as_ref(),
            query,
            &RangeQuery::new(AT_MS, AT_MS, 60_000),
        )
        .unwrap_or_else(|e| panic!("{query}: {e}"));
    let mut rows: Vec<(Labels, f64)> = promql_engine::series::decode(&batches)
        .expect("the canonical shape decodes")
        .iter()
        .map(|s| {
            let labels: Labels = s
                .labels()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect();
            (labels, s.values()[0])
        })
        .collect();
    rows.sort_by(|a, b| a.0.cmp(&b.0));
    rows
}

fn label_sets(query: &str) -> Vec<Labels> {
    vector(query)
        .into_iter()
        .map(|(labels, _)| labels)
        .collect()
}

/// `label_replace` copying the name into `n` shows whether it was still
/// there to read.
fn name_read_above(inner: &str) -> Vec<Labels> {
    label_sets(&format!(
        r#"label_replace({inner}, "n", "$1", "__name__", "(.+)")"#
    ))
}

#[test]
fn every_name_dropping_operator_keeps_the_name_readable_above_it() {
    let read = vec![labels(&[("a", "1"), ("n", "x")])];
    for inner in [
        r#"rate(x{a="1"}[5m])"#,
        r#"abs(x{a="1"})"#,
        r#"-x{a="1"}"#,
        r#"x{a="1"} * 2"#,
        r#"timestamp(x{a="1"})"#,
        r#"x{a="1"} == bool x{a="1"}"#,
    ] {
        assert_eq!(name_read_above(inner), read, "{inner}");
    }
}

#[test]
fn the_result_drops_the_type_and_unit_with_the_name() {
    assert_eq!(label_sets(r#"-x{a="1"}"#), [labels(&[("a", "1")])]);
}

#[test]
fn vector_arithmetic_drops_the_metadata_at_once() {
    assert_eq!(
        name_read_above(r#"x{a="1"} + x{a="1"}"#),
        [labels(&[("a", "1")])]
    );
}

/// `VectorBinop` answers `DropName: returnBool`, so a filter keeps even a
/// name its operand had marked.
#[test]
fn a_comparison_between_vectors_clears_the_mark() {
    assert_eq!(
        label_sets(r#"rate(x{a="1"}[5m]) >= rate(x{a="1"}[5m])"#),
        [labels(&[
            ("__name__", "x"),
            ("__type__", "counter"),
            ("a", "1")
        ])]
    );
    assert_eq!(
        label_sets(r#"x{a="1"} == bool x{a="1"}"#),
        [labels(&[("a", "1")])]
    );
}

#[test]
fn the_mark_is_not_part_of_the_match_signature() {
    assert_eq!(
        vector(r#"rate(x{a="1"}[5m]) * x{a="1"}"#),
        [(labels(&[("a", "1")]), 10.0 / 60.0 * 100.0)]
    );
    // `or` needs the mark per row, as its sides differ; the right-hand
    // `x{a="1"}` matches the left and is dropped.
    assert_eq!(
        label_sets(r#"rate(x{a="1"}[5m]) or x{a="1"}"#),
        [labels(&[("a", "1")])]
    );
    assert_eq!(
        label_sets(r#"rate(x{a="1"}[5m]) or y"#),
        [
            labels(&[("__name__", "y"), ("a", "3")]),
            labels(&[("a", "1")])
        ]
    );
}

#[test]
fn an_aggregation_by_name_drops_a_marked_group_name() {
    assert_eq!(label_sets(r#"sum by (__name__) (rate(x[5m]))"#), [vec![]]);
}

/// A `by` without metadata lets a marking call drop the name at once, but
/// only if nothing between them reads it.
#[test]
fn a_grouping_without_the_name_still_lets_a_rewrite_read_it() {
    assert_eq!(
        label_sets(r#"sum by (a) (label_replace(rate(x[5m]), "a", "$1", "__name__", "(.+)"))"#),
        [labels(&[("a", "x")])]
    );
    assert_eq!(
        label_sets(r#"sum by (a) (abs(rate(x[5m])))"#),
        [labels(&[("a", "1")]), labels(&[("a", "2")])]
    );
}

#[test]
fn an_aggregation_by_name_drops_the_name_if_any_member_is_marked() {
    assert_eq!(
        label_sets(r#"sum by (__name__) (x{a="1"} or rate(x{a="2"}[5m]))"#),
        [vec![]]
    );
}

/// `z{a="1"}` has no rate and so is not in the group at the step; only
/// the renamed, unmarked `z{a="2"}` is.
#[test]
fn a_member_with_no_sample_does_not_mark_its_group() {
    assert_eq!(
        label_sets(r#"sum by (__name__) (label_replace(rate(z[5m]), "__name__", "z", "a", "2"))"#),
        [labels(&[("__name__", "z")])]
    );
}

#[test]
fn an_aggregation_without_drops_a_marked_type() {
    assert_eq!(label_sets(r#"sum without (a) (rate(x[5m]))"#), [vec![]]);
    assert_eq!(
        label_sets(r#"sum without (a) (rate(x[5m]) or x{a="9"})"#),
        [vec![]]
    );
}

#[test]
fn writing_the_name_keeps_it_where_the_regex_matched() {
    assert_eq!(
        label_sets(r#"label_replace(rate(x{a="1"}[5m]), "__name__", "$1", "__name__", "(.+)")"#),
        [labels(&[
            ("__name__", "x"),
            ("__type__", "counter"),
            ("a", "1")
        ])]
    );
    assert_eq!(
        label_sets(r#"label_replace(rate(x{a="1"}[5m]), "__name__", "w", "a", "nomatch")"#),
        [labels(&[("a", "1")])]
    );
    assert_eq!(
        label_sets(r#"label_join(rate(x{a="1"}[5m]), "__name__", "_", "__name__", "a")"#),
        [labels(&[
            ("__name__", "x_1"),
            ("__type__", "counter"),
            ("a", "1")
        ])]
    );
}

#[test]
fn a_root_sort_still_orders_the_result() {
    let result = Engine::blocking()
        .unwrap()
        .instant_query(
            source().as_ref(),
            r#"sort_desc(rate({__name__=~"x|y"}[5m]))"#,
            AT_MS,
        )
        .unwrap();
    let InstantResult::Vector(batches) = result else {
        panic!("expected a vector");
    };
    let order: Vec<String> = promql_engine::series::decode(&batches)
        .unwrap()
        .iter()
        .map(|s| s.label("a").to_string())
        .collect();
    assert_eq!(order, ["2", "3", "1"]);
}

/// A nested sort orders nothing, and a label added below it and read above
/// it used to fail DataFusion's `push_down_leaf_projections`.
#[test]
fn a_label_added_under_a_nested_sort_is_readable_above_it() {
    assert_eq!(
        label_sets(r#"sort_desc(rate(x{a="1"}[5m])) > 0"#),
        [labels(&[("a", "1")])]
    );
    assert_eq!(
        label_sets(
            r#"label_replace(sort_desc(label_replace(y, "n", "v", "a", ".*")), "m", "$1", "n", "(.+)")"#
        ),
        [labels(&[
            ("__name__", "y"),
            ("a", "3"),
            ("m", "v"),
            ("n", "v")
        ])]
    );
}

/// Upstream keeps a group per name and drops the names last, so two names
/// collide; dropping them first would quietly sum both into one series.
#[test]
fn an_aggregation_by_name_keeps_a_group_per_name_until_the_result() {
    let error = Engine::blocking()
        .unwrap()
        .instant_query(
            source().as_ref(),
            r#"sum by (__name__) (rate({__name__=~"x|z"}[5m]))"#,
            AT_MS,
        )
        .expect_err("two groups collide once their names go");
    assert!(error.to_string().contains("same labelset"), "{error}");
}

/// Arithmetic drops its operands' metadata itself, so an operand keeps its
/// mark only for the labels the match reads: `on`, `group_left`, and the
/// `__type__` an `ignoring` signature still holds.
#[test]
fn arithmetic_keeps_the_mark_for_what_it_matches_on() {
    assert_eq!(
        label_sets(r#"rate(x{a="1"}[5m]) + on(__name__, a) x{a="1"}"#),
        [labels(&[("a", "1")])]
    );
    assert_eq!(
        label_sets(r#"x{a="1"} * on(a) group_left(__name__) rate(x{a="1"}[5m])"#),
        [labels(&[("__name__", "x"), ("a", "1")])]
    );
}

/// `without` never groups on the name but does on `__type__`, so `x` and
/// `z` stay two groups and collide once the result drops their metadata.
#[test]
fn an_aggregation_without_keeps_a_group_per_type_until_the_result() {
    let error = Engine::blocking()
        .unwrap()
        .instant_query(
            source().as_ref(),
            r#"sum without (a) (rate({__name__=~"x|z"}[5m]))"#,
            AT_MS,
        )
        .expect_err("two groups collide once their metadata goes");
    assert!(error.to_string().contains("same labelset"), "{error}");
}
