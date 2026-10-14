//! Small invariants for benchmark accounting and adversarial source fixtures.
//! These are not performance assertions and do not depend on Criterion.

use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, AsArray, Int64Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::physical_plan::collect;
use datafusion::prelude::SessionContext;
use promql_engine::selector::STALE_NAN_BITS;
use promql_engine::series::{self, Block, SAMPLES};
use promql_engine::{Engine, SelectHints, Series, SeriesSource};

#[allow(dead_code)] // Each bench binary uses a different subset of the helpers.
#[path = "../benches/support/focused.rs"]
mod focused;
#[allow(dead_code)]
#[path = "../benches/support/generated.rs"]
mod generated;
#[path = "../benches/support/result_memory.rs"]
mod result_memory;

fn sample_batch() -> RecordBatch {
    let rows = vec![
        Series::new(
            &[("pod", "a-long-shared-label-value")],
            vec![0, 1],
            vec![1., 2.],
        )
        .unwrap(),
        Series::new(
            &[("pod", "a-different-long-label-value")],
            vec![0],
            vec![3.],
        )
        .unwrap(),
    ];
    series::encode(
        &series::label_names_of(&rows),
        &rows,
        Block {
            start_ms: 0,
            end_ms: 2,
        },
    )
    .unwrap()
}

#[test]
fn sliced_results_count_logical_values_but_backing_owners_only_once() {
    let batch = sample_batch();
    assert_eq!(result_memory::logical_bytes(&batch.slice(0, 0)), 0);
    let slices = vec![batch.slice(1, 1), batch.slice(0, 1)];
    let whole = result_memory::retained(std::slice::from_ref(&batch));
    assert!(whole.bytes > 0);
    assert_eq!(whole.external_owners, 0);
    assert_eq!(result_memory::retained(&slices), whole);
    // One visible row still pins hidden samples and long-label backing.
    assert_eq!(result_memory::retained(&slices[..1]), whole);
    assert_eq!(
        slices
            .iter()
            .map(result_memory::logical_bytes)
            .sum::<usize>(),
        3 * 16 + "a-long-shared-label-value".len() + "a-different-long-label-value".len(),
    );
    assert_eq!(
        result_memory::logical_bytes(&slices[0]),
        16 + "a-different-long-label-value".len(),
    );
    let duplicated = vec![batch.clone(), batch.clone()];
    assert_eq!(result_memory::retained(&duplicated), whole);
    assert_eq!(
        duplicated
            .iter()
            .map(result_memory::logical_bytes)
            .sum::<usize>(),
        2 * result_memory::logical_bytes(&batch),
    );
    let independent = sample_batch();
    let independent_bytes = result_memory::retained(std::slice::from_ref(&independent)).bytes;
    assert_eq!(
        result_memory::retained(&[batch, independent]).bytes,
        whole.bytes + independent_bytes,
    );
}

#[test]
fn shared_nullable_buffers_are_deduplicated_across_columns_and_slices() {
    let values = Arc::new(Int64Array::from(vec![Some(1), None, Some(3)])) as ArrayRef;
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, true),
        Field::new("b", DataType::Int64, true),
    ]));
    let data = values.to_data();
    let expected = data.buffers()[0].capacity() + data.nulls().unwrap().buffer().capacity();
    let batch = RecordBatch::try_new(schema, vec![values.clone(), values]).unwrap();
    assert_eq!(
        result_memory::retained(&[batch.slice(0, 2), batch.slice(1, 2)]).bytes,
        expected
    );
}

#[tokio::test]
async fn focused_wide_and_sparse_fixtures_produce_the_intended_answers() {
    let engine = Engine::new();
    let mut outputs = Vec::new();
    for shuffled in [false, true] {
        let source = focused::wide_source(16, shuffled, false);
        let out = engine
            .range_query_async(source.as_ref(), focused::SELECTOR, &focused::instant())
            .await
            .unwrap();
        let rows = series::decode(&out).unwrap();
        assert_eq!(rows.len(), 16);
        assert_eq!(rows[0].label("a_identity"), "pod-00000000");
        outputs.push(rows.iter().map(|row| row.values()[0]).collect::<Vec<_>>());
    }
    assert_eq!(outputs[0], (0..16).map(|id| id as f64).collect::<Vec<_>>());
    assert_eq!(outputs[0], outputs[1]);
    let empty = focused::wide_source(16, true, true);
    let out = engine
        .range_query_async(empty.as_ref(), focused::SELECTOR, &focused::instant())
        .await
        .unwrap();
    assert_eq!(out.iter().map(RecordBatch::num_rows).sum::<usize>(), 0);

    let sparse = focused::sparse_source(3, 2);
    let out = engine
        .range_query_async(
            sparse.as_ref(),
            focused::SPARSE_SUM,
            &focused::sparse_range(2),
        )
        .await
        .unwrap();
    let rows = series::decode(&out).unwrap();
    assert_eq!(
        rows.iter().map(|row| row.timestamps().len()).sum::<usize>(),
        6
    );
    for row in rows {
        assert!(row
            .timestamps()
            .iter()
            .all(|&ts| ts == 0 || ts == focused::BLOCK_MS));
        assert!(row.values().iter().all(|&value| value == 3.0));
    }
}

#[tokio::test]
async fn chunk_probes_preserve_large_future_tails_and_stale_bits() {
    use generated::{ChunkProbe, Generated, Shape};
    let ctx = SessionContext::new();
    for probe in [ChunkProbe::Whole, ChunkProbe::Second, ChunkProbe::Stale] {
        let source = Generated::streamed(Shape {
            series: 2,
            samples: 128,
            scrape_ms: 1000,
            chunk: None,
            rows_per_batch: 1,
        })
        .chunk_probe(probe);
        let plan = source
            .select(&ctx.state(), &[], SelectHints::range(-300_000, 60_000))
            .await
            .unwrap();
        let batches = collect(plan, ctx.task_ctx()).await.unwrap();
        assert_eq!(
            batches.len(),
            if matches!(probe, ChunkProbe::Second) {
                4
            } else {
                2
            }
        );
        let decoded = series::decode(&batches).unwrap();
        assert_eq!(
            decoded
                .iter()
                .map(|row| row.timestamps().len())
                .sum::<usize>(),
            256
        );
        assert!(decoded
            .iter()
            .any(|row| row.timestamps().last() == Some(&127_000)));
        for batch in &batches {
            let samples = batch.column_by_name(SAMPLES).unwrap().as_list::<i32>();
            assert!(!samples.value(0).is_empty());
        }
        let stale_count = decoded
            .iter()
            .flat_map(|row| row.values())
            .filter(|v| v.to_bits() == STALE_NAN_BITS)
            .count();
        assert_eq!(
            stale_count,
            if matches!(probe, ChunkProbe::Stale) {
                2
            } else {
                0
            }
        );
    }
}
