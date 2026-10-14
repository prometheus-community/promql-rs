//! The benchmark's lazy source must obey the same chunk contract as real stores.
use datafusion::arrow::array::{FixedSizeBinaryArray, UInt64Array};
use datafusion::physical_plan::collect;
use datafusion::prelude::SessionContext;
use promql_engine::series::{self, SERIES_ID};
use promql_engine::{Engine, RangeQuery, SelectHints, SeriesIdEncoding, SeriesSource};

#[path = "../benches/support/generated.rs"]
mod generated;
use generated::{Generated, Shape};

fn shape() -> Shape {
    Shape {
        series: 3,
        samples: 9,
        scrape_ms: 1000,
        chunk: Some(2),
        rows_per_batch: 2,
    }
}

#[tokio::test]
async fn streamed_and_resident_benchmark_sources_keep_ids_and_chronology_across_batches() {
    let ctx = SessionContext::new();
    let sources: Vec<(Generated, SeriesIdEncoding)> = vec![
        (Generated::streamed(shape()), SeriesIdEncoding::UInt64),
        (Generated::resident(shape()), SeriesIdEncoding::UInt64),
        (
            Generated::streamed_with_ids(shape(), SeriesIdEncoding::FixedSizeBinary),
            SeriesIdEncoding::FixedSizeBinary,
        ),
    ];
    for (source, encoding) in sources {
        let plan = source
            .select(&ctx.state(), &[], SelectHints::range(0, 8000))
            .await
            .unwrap();
        let batches = collect(plan, ctx.task_ctx()).await.unwrap();
        assert_eq!(batches.len(), 8);
        let mut row_count = 0;
        for batch in batches {
            assert!(batch.num_rows() <= 2);
            let ids = batch.column_by_name(SERIES_ID).unwrap();
            assert_eq!(ids.null_count(), 0);
            for row in 0..batch.num_rows() {
                let identity = (row_count / 5) as u64;
                let expected = u64::MAX - identity;
                match encoding {
                    SeriesIdEncoding::UInt64 => assert_eq!(
                        ids.as_any()
                            .downcast_ref::<UInt64Array>()
                            .unwrap()
                            .value(row),
                        expected
                    ),
                    SeriesIdEncoding::FixedSizeBinary => {
                        let mut bytes = [0xA5; 16];
                        bytes[8..].copy_from_slice(&expected.to_be_bytes());
                        assert_eq!(
                            ids.as_any()
                                .downcast_ref::<FixedSizeBinaryArray>()
                                .unwrap()
                                .value(row),
                            bytes
                        );
                    }
                }
                let decoded = series::decode(&[batch.slice(row, 1)]).unwrap();
                let series = &decoded[0];
                let chunk = row_count % 5;
                let expected_timestamps = ((chunk * 2)..((chunk * 2 + 2).min(9)))
                    .map(|sample| sample as i64 * 1000)
                    .collect::<Vec<_>>();
                assert_eq!(series.timestamps(), expected_timestamps);
                assert!(series.labels().any(|(name, value)| {
                    name == "pod" && value == format!("nginx-{identity}")
                }));
                row_count += 1;
            }
        }
        assert_eq!(row_count, 15);

        // Numeric answers are literal, not parity against the same generator.
        let output = Engine::new()
            .range_query_async(
                &source,
                "http_requests_total",
                &RangeQuery::new(8000, 8000, 1),
            )
            .await
            .unwrap();
        let decoded = series::decode(&output).unwrap();
        assert_eq!(decoded.len(), 3);
        for (pod, value) in [("nginx-0", 31.), ("nginx-1", 33.), ("nginx-2", 35.)] {
            let row = decoded
                .iter()
                .find(|row| row.labels().any(|(name, v)| name == "pod" && v == pod))
                .unwrap();
            assert_eq!(row.timestamps(), [8000]);
            assert_eq!(row.values(), [value]);
        }
    }
}
