//! Evaluated-result materialization after `cleanupMetricLabels` and
//! `mergeSeriesWithSameLabelset` (Prometheus `promql/engine.go` at 83962c35).
//! Source chunk overlap is deliberately not involved: distinct raw identities
//! can become one output label set only after a name-dropping operation.

use std::num::{NonZeroU64, NonZeroUsize};

use promql_engine::series;
use promql_engine::{
    Engine, EngineError, MemorySeriesSource, RangeQuery, Series, SeriesIdEncoding,
};

const QUERY: &str = "min_over_time({__name__=~\"alpha|beta\"}[1ms])";
const SAME_LABELSET: &str = "vector cannot contain metrics with the same labelset";

fn special_values() -> [f64; 8] {
    [
        -0.0,
        0.0,
        f64::INFINITY,
        f64::NEG_INFINITY,
        f64::from_bits(0x7ff8_0000_0000_1234),
        f64::from_bits(0xfff8_0000_0000_5678),
        42.0,
        -17.0,
    ]
}

fn points(steps: usize, shift: usize) -> Vec<(i64, u64)> {
    (0..steps)
        .map(|i| (i as i64 * 1000, special_values()[(i + shift) % 8].to_bits()))
        .collect()
}

fn data(steps: usize, interleaved: bool) -> Vec<Series> {
    // Sparse labels reverse Arrow struct order: labels.Compare puts {a="z"}
    // before {z="a"}, even though its value "z" sorts after "a".
    [("z", "a", 3), ("a", "z", 0)]
        .into_iter()
        .flat_map(|(label, value, shift)| {
            ["alpha", "beta"]
                .into_iter()
                .enumerate()
                .map(move |(metric, name)| {
                    let mut ts = Vec::new();
                    let mut vs = Vec::new();
                    for (i, (t, bits)) in points(steps, shift).into_iter().enumerate() {
                        let owner = if interleaved {
                            i % 2
                        } else {
                            usize::from(i >= steps / 2)
                        };
                        if owner == metric {
                            ts.extend([t, t + 500]);
                            vs.extend([
                                f64::from_bits(bits),
                                f64::from_bits(promql_engine::selector::STALE_NAN_BITS),
                            ]);
                        }
                    }
                    Series::new(&[("__name__", name), (label, value)], ts, vs).unwrap()
                })
        })
        .collect()
}

fn assert_matrix(matrix: &[Series], steps: usize) {
    assert_eq!(matrix.len(), 2);
    for (row, labels, shift) in [(&matrix[0], ("a", "z"), 0), (&matrix[1], ("z", "a"), 3)] {
        assert_eq!(row.labels().collect::<Vec<_>>(), [labels]);
        assert!(row.timestamps().windows(2).all(|pair| pair[0] < pair[1]));
        let actual: Vec<_> = row
            .timestamps()
            .iter()
            .zip(row.values())
            .map(|(&t, &v)| (t, v.to_bits()))
            .collect();
        assert_eq!(actual, points(steps, shift));
    }
}

/// Unary `eval` and `mergeSeriesWithSameLabelset` (Prometheus 83962c35)
/// must compose: raw metric order is not time order after negation drops
/// the name. This is the operators.test range case.
#[tokio::test]
async fn unary_name_removal_coalesces_disjoint_metrics_in_time_order() {
    let engine = Engine::new();
    for encoding in [SeriesIdEncoding::UInt64, SeriesIdEncoding::FixedSizeBinary] {
        let source = MemorySeriesSource::try_new(vec![
            Series::new(
                &[("__name__", "http_errors"), ("job", "api")],
                vec![1_200_000],
                vec![4.0],
            )
            .unwrap(),
            Series::new(
                &[("__name__", "http_requests"), ("job", "api")],
                vec![0],
                vec![2.0],
            )
            .unwrap(),
        ])
        .unwrap()
        .with_ids(encoding);
        let batches = engine
            .range_query_async(
                &source,
                "-{job=\"api\"}",
                &RangeQuery::new(0, 1_200_000, 1_200_000),
            )
            .await
            .unwrap();
        let rows = series::coalesce(series::decode(&batches).unwrap()).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].labels().collect::<Vec<_>>(), [("job", "api")]);
        assert_eq!(rows[0].timestamps(), [0, 1_200_000]);
        assert_eq!(rows[0].values(), [-2.0, -4.0]);
    }
}

#[tokio::test]
async fn name_removal_merges_interleaved_steps_across_batches_and_blocks() {
    let engine = Engine::new();
    let range = RangeQuery::new(0, 15_000, 1000);
    for id in [SeriesIdEncoding::UInt64, SeriesIdEncoding::FixedSizeBinary] {
        for block_ms in [None, Some(8000)] {
            for partitions in [1, 3] {
                let mut source = MemorySeriesSource::try_new(data(16, true))
                    .unwrap()
                    .with_ids(id)
                    .chunked(0)
                    .rows_per_batch(NonZeroUsize::new(1).unwrap())
                    .partitions(NonZeroUsize::new(partitions).unwrap());
                if let Some(ms) = block_ms {
                    source = source.blocks(NonZeroU64::new(ms).unwrap());
                }
                // The 1ms window sees exactly one real sample, so min_over_time
                // drops the name without arithmetic changing any float bits.
                let batches = engine
                    .range_query_async(&source, QUERY, &range)
                    .await
                    .unwrap();
                assert!(
                    batches.len() > 1,
                    "fixture must cover multiple finalized batches"
                );
                for batch in &batches {
                    series::validate(&batch.schema()).unwrap();
                    assert_eq!(batch.num_columns(), 4, "raw identity escaped evaluation");
                }
                let decoded = series::decode(&batches).unwrap();
                assert_eq!(decoded.len(), if block_ms.is_some() { 8 } else { 4 });
                assert!(
                    decoded.windows(2).any(|rows| {
                        rows[0].labels().eq(rows[1].labels())
                            && rows[0].timestamps().last() > rows[1].timestamps().first()
                    }),
                    "fixture must expose interleaved same-label output spans"
                );
                assert_matrix(&series::coalesce(decoded).unwrap(), 16);
            }
        }
    }
}

#[tokio::test]
async fn chronological_disjoint_blocks_and_single_rows_still_materialize() {
    let engine = Engine::new();
    let source = MemorySeriesSource::try_new(data(8, false))
        .unwrap()
        .chunked(0)
        .blocks(NonZeroU64::new(4000).unwrap())
        .rows_per_batch(NonZeroUsize::new(1).unwrap());
    for end in [0, 7000] {
        let batches = engine
            .range_query_async(&source, QUERY, &RangeQuery::new(0, end, 1000))
            .await
            .unwrap();
        let decoded = series::decode(&batches).unwrap();
        assert_eq!(decoded.len(), if end == 0 { 2 } else { 4 });
        for rows in decoded.windows(2) {
            if rows[0].labels().eq(rows[1].labels()) {
                assert!(rows[0].timestamps().last() < rows[1].timestamps().first());
            }
        }
        assert_matrix(
            &series::coalesce(decoded).unwrap(),
            if end == 0 { 1 } else { 8 },
        );
    }
}

#[tokio::test]
async fn duplicate_name_dropped_timestamps_remain_query_errors_before_coalescing() {
    let source = MemorySeriesSource::try_new(
        ["alpha", "beta"]
            .into_iter()
            .map(|name| {
                Series::new(
                    &[("__name__", name), ("pod", "p")],
                    vec![0, 2000],
                    vec![1.0, 2.0],
                )
                .unwrap()
            })
            .collect(),
    )
    .unwrap()
    .chunked(0)
    .rows_per_batch(NonZeroUsize::new(1).unwrap());
    let error = Engine::new()
        .range_query_async(&source, QUERY, &RangeQuery::new(0, 2000, 1000))
        .await
        .unwrap_err();
    assert!(
        matches!(&error, EngineError::Query(message) if message == SAME_LABELSET),
        "{error:?}"
    );
}
