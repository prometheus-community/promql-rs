//! Plan pins for the topic of `promql-conformance/testdata/prometheus/selectors.test`.

use insta::assert_snapshot;

use super::{Bounds, Case};

const RANGE: Bounds = Bounds {
    start_ms: 600_000,
    end_ms: 1_200_000,
    step_ms: 30_000,
};

/// The selector is an aggregate grouped by the whole labels struct, so a
/// label set split over several chunks is one group. The labels pass
/// through untouched: an instant selector keeps every label it was given,
/// __name__ included.
#[test]
fn instant_selector() {
    let case = Case::new(
        RANGE,
        &[
            r#"http_requests_total{job="api", pod="api-1"} 0"#,
            r#"http_requests_total{job="api", pod="api-2"} 0"#,
        ],
    );
    let query = r#"http_requests_total{job="api"}"#;
    assert_snapshot!("instant_selector", case.plan(query), query);
    assert_snapshot!("instant_selector_physical", case.physical(query), query);
}

/// The store hands each label set over as several chunks, each its own
/// batch, which only the physical plan shows as the number of batches under
/// SeriesSetExec. The logical plan is the whole-series case's: a label set
/// split over chunks is still one group of the Sorted aggregate.
#[test]
fn instant_selector_over_chunks() {
    let case = Case::new(RANGE, &[r#"x{pod="a"} 1+1x19"#, r#"x{pod="b"} 2+3x19"#]).chunked(150_000);
    let query = "x";
    assert_snapshot!("instant_selector_over_chunks", case.plan(query), query);
    assert_snapshot!(
        "instant_selector_over_chunks_physical",
        case.physical(query),
        query
    );
}

/// The store cuts the select into 200s blocks on top of 150s chunks, so a
/// label set arrives in several blocks and each block in several chunks:
/// the data ends at 570s, so the first block, reaching back from 600s by
/// the lookback, holds two chunks per series, the second one, and the two
/// after it nothing. The plan is the one-block case's: the block is a group
/// key the store fills, and the number of batches is all that grows.
#[test]
fn instant_selector_over_blocks_and_chunks() {
    let case = Case::new(RANGE, &[r#"x{pod="a"} 1+1x19"#, r#"x{pod="b"} 2+3x19"#])
        .chunked(150_000)
        .blocks(200_000);
    let query = "x";
    assert_snapshot!(
        "instant_selector_over_blocks_and_chunks",
        case.plan(query),
        query
    );
    assert_snapshot!(
        "instant_selector_over_blocks_and_chunks_physical",
        case.physical(query),
        query
    );
}

/// The offset is a literal argument of the aggregate, in the slot an @
/// fills too, so the shape does not change with either modifier.
#[test]
fn range_function_with_an_offset() {
    let case = Case::new(
        RANGE,
        &[
            r#"http_requests_total{job="api", pod="api-1"} 0"#,
            r#"http_requests_total{job="web", pod="web-1"} 0"#,
        ],
    );
    let query = "rate(http_requests_total[5m] offset 5m)";
    assert_snapshot!("range_function_with_an_offset", case.plan(query), query);
}
