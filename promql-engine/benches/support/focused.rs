//! Scaling fixtures, not a general store: only unfiltered `http_requests_total`
//! selections on the documented grids are supported. Raw identity and chunk
//! rows survive all the way to evaluation; between-ID order is intentional.

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{ArrayRef, RecordBatch, UInt64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::catalog::Session;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::error::Result;
use datafusion::physical_plan::ExecutionPlan;
use promql_engine::selector::STALE_NAN_BITS;
use promql_engine::series::{self, Block, SERIES_ID};
use promql_engine::{RangeQuery, SelectHints, Series, SeriesSource};
use promql_parser::ast::LabelMatcher;

pub const SELECTOR: &str = "http_requests_total";
pub const SPARSE_SUM: &str = "sum by (pod) (http_requests_total)";
pub const RATE: &str = "rate(http_requests_total[5m])";
pub const BLOCK_MS: i64 = 900_000;
pub const STEP_MS: i64 = 30_000;

#[derive(Debug)]
struct Selected {
    batches: Vec<RecordBatch>,
}

#[async_trait]
impl SeriesSource for Selected {
    async fn select(
        &self,
        _state: &dyn Session,
        _matchers: &[LabelMatcher],
        _hints: SelectHints,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(MemorySourceConfig::try_new_exec(
            std::slice::from_ref(&self.batches),
            self.batches[0].schema(),
            None,
        )?)
    }
}

fn raw(rows: &[Series], ids: Vec<u64>, block: Block) -> RecordBatch {
    let batch = series::encode(&series::label_names_of(rows), rows, block).unwrap();
    let mut fields = batch.schema().fields().to_vec();
    fields.push(Arc::new(Field::new(SERIES_ID, DataType::UInt64, false)));
    let schema = Arc::new(Schema::new_with_metadata(
        fields,
        batch.schema().metadata().clone(),
    ));
    let mut columns = batch.columns().to_vec();
    columns.push(Arc::new(UInt64Array::from(ids)) as ArrayRef);
    RecordBatch::try_new(schema, columns).unwrap()
}

/// Twelve long label values expose both per-row Arrow headers and retained
/// Utf8View backing. Power-of-two sizes admit a reproducible odd-stride
/// permutation without sorting or RNG work in the timed interval.
pub fn wide_source(count: usize, shuffled: bool, stale: bool) -> Arc<dyn SeriesSource> {
    assert!(count.is_power_of_two());
    let order: Vec<usize> = (0..count)
        .map(|i| if shuffled { (i * 4051) % count } else { i })
        .collect();
    let rows: Vec<Series> = order
        .iter()
        .map(|&id| {
            let mut labels = vec![
                ("__name__".to_owned(), SELECTOR.to_owned()),
                ("a_identity".to_owned(), format!("pod-{id:08}")),
            ];
            for column in 0..12 {
                labels.push((
                    format!("label_{column:02}"),
                    format!(
                        "tenant-{:04}-field-{column:02}-{}",
                        id % 128,
                        "x".repeat(48)
                    ),
                ));
            }
            let borrowed: Vec<(&str, &str)> = labels
                .iter()
                .map(|(name, value)| (name.as_str(), value.as_str()))
                .collect();
            let value = if stale {
                f64::from_bits(STALE_NAN_BITS)
            } else {
                id as f64
            };
            Series::new(&borrowed, vec![0], vec![value]).unwrap()
        })
        .collect();
    Arc::new(Selected {
        batches: vec![raw(
            &rows,
            order.into_iter().map(|id| id as u64).collect(),
            Block {
                start_ms: 0,
                end_ms: 1,
            },
        )],
    })
}

pub fn instant() -> RangeQuery {
    RangeQuery::new(0, 0, STEP_MS)
}

pub fn sparse_range(blocks: usize) -> RangeQuery {
    RangeQuery::new(0, blocks as i64 * BLOCK_MS - STEP_MS, STEP_MS)
}

/// Two distinct raw identities per group and block. A stale marker between
/// the first two steps makes each evaluated span a singleton, despite a
/// 30-step block and a query grid hundreds of steps wide. IDs stay stable
/// across blocks. Reverse group order is legal, not an ASC label promise.
/// This probes the lanes of Prometheus `aggregation` (83962c35), not a
/// store-side reduction that would change counter/identity semantics.
pub fn sparse_source(groups: usize, blocks: usize) -> Arc<dyn SeriesSource> {
    assert!(groups > 0 && blocks > 0);
    let mut batches = Vec::new();
    for block in 0..blocks {
        let start = block as i64 * BLOCK_MS;
        let rows: Vec<Series> = (0..groups * 2)
            .rev()
            .map(|id| {
                let pod = format!("pod-{:08}", id / 2);
                let replica = format!("{}", id % 2);
                Series::new(
                    &[("__name__", SELECTOR), ("pod", &pod), ("replica", &replica)],
                    vec![start, start + STEP_MS / 2],
                    vec![1.0 + (id % 2) as f64, f64::from_bits(STALE_NAN_BITS)],
                )
                .unwrap()
            })
            .collect();
        let batch = raw(
            &rows,
            (0..groups as u64 * 2).rev().collect(),
            Block {
                start_ms: start,
                end_ms: start + BLOCK_MS,
            },
        );
        for from in (0..batch.num_rows()).step_by(1024) {
            batches.push(batch.slice(from, (batch.num_rows() - from).min(1024)));
        }
    }
    Arc::new(Selected { batches })
}
