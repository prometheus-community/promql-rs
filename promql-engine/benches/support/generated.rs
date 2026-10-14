//! A [`SeriesSource`] that computes its series from a seed-free
//! description instead of storing them, for the memory bench.
//!
//! `MemorySeriesSource` keeps every input batch resident, so a query over
//! it can never show what the engine saves by not holding a series: the
//! input is already all there. Streamed, each batch is built when the
//! plan polls for it and freed once the engine lets go, which is what a
//! store reading chunks off disk or a socket does. Resident, the same
//! batches are built up front and served from memory, the equivalent of
//! `MemorySeriesSource` with its rows packed into full batches.
//! All constructors emit ID-contiguous chunks. `streamed_with_ids` selects
//! UInt64 or binary16 encoding without changing row order.
//!
//! The data is the long bench's: counters `http_requests_total` with
//! `code`, `pod` and `route`, resetting every 1000 samples.

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{
    ArrayRef, FixedSizeBinaryArray, Float64Array, ListArray, RecordBatch, StringViewBuilder,
    StructArray, TimestampMillisecondArray, UInt64Array,
};
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::Session;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::error::Result;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::{PartitionStream, StreamingTableExec};
use datafusion::physical_plan::ExecutionPlan;
use promql_engine::series::{labels_type, sample_fields, sample_item, schema, Block, SERIES_ID};
use promql_engine::{SelectHints, SeriesIdEncoding, SeriesSource};
use promql_parser::ast::LabelMatcher;

const NAMES: [&str; 4] = ["__name__", "code", "pod", "route"];

#[derive(Debug, Clone)]
pub struct Shape {
    pub series: usize,
    pub samples: usize,
    pub scrape_ms: i64,
    /// Samples per chunk; `None` is one chunk per label set.
    pub chunk: Option<usize>,
    pub rows_per_batch: usize,
}

/// Deliberately untrimmed physical chunks. They intersect the requested
/// interval but retain future tails, as a chunk-metadata-pruning store may.
#[allow(dead_code)] // Also imported by the ordinary generated-source tests.
#[derive(Debug, Clone, Copy)]
pub enum ChunkProbe {
    Whole,
    /// Four samples leave nonempty history before one large second chunk.
    Second,
    /// A stale marker forces the range path to filter within a large chunk.
    Stale,
}

#[derive(Debug)]
pub struct Generated {
    shape: Shape,
    schema: SchemaRef,
    id: SeriesIdEncoding,
    probe: Option<ChunkProbe>,
    /// Built up front when resident; `None` streams.
    resident: Option<Vec<RecordBatch>>,
}

impl Generated {
    pub fn streamed(shape: Shape) -> Self {
        Self::new(shape, SeriesIdEncoding::UInt64)
    }

    pub fn streamed_with_ids(shape: Shape, id: SeriesIdEncoding) -> Self {
        Self::new(shape, id)
    }

    fn new(shape: Shape, id: SeriesIdEncoding) -> Self {
        let names: Vec<String> = NAMES.iter().map(|n| n.to_string()).collect();
        let schema = schema(&names);
        let ty = match id {
            SeriesIdEncoding::UInt64 => DataType::UInt64,
            SeriesIdEncoding::FixedSizeBinary => DataType::FixedSizeBinary(16),
        };
        let mut fields = schema.fields().to_vec();
        fields.push(Arc::new(Field::new(SERIES_ID, ty, false)));
        let schema = Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()));
        Self {
            shape,
            schema,
            id,
            probe: None,
            resident: None,
        }
    }

    /// Keep ordinary fixtures clipped; only tail probes opt into whole
    /// physical chunks. No overlap/duplicate precedence is changed here.
    #[allow(dead_code)] // Used only by the chunk-tail workloads.
    pub fn chunk_probe(mut self, probe: ChunkProbe) -> Self {
        assert!(self.resident.is_none());
        self.probe = Some(probe);
        self
    }

    pub fn resident(shape: Shape) -> Self {
        let mut s = Self::streamed(shape);
        let all = s.partition(0, i64::MAX, 0).batches().collect::<Result<_>>();
        s.resident = Some(all.expect("generated batches are canonical"));
        s
    }

    /// One block over the whole select, the way `MemorySeriesSource` cuts.
    fn partition(&self, start_ms: i64, end_ms: i64, window_ms: i64) -> Partition {
        Partition {
            shape: self.shape.clone(),
            schema: self.schema.clone(),
            id: self.id,
            probe: self.probe,
            start_ms,
            end_ms,
            block: Block {
                start_ms: start_ms.saturating_add(window_ms),
                end_ms: end_ms.saturating_add(1),
            },
        }
    }
}

#[async_trait]
impl SeriesSource for Generated {
    /// Every series matches: the bench only asks for `http_requests_total`.
    async fn select(
        &self,
        _state: &dyn Session,
        _matchers: &[LabelMatcher],
        hints: SelectHints,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if let Some(batches) = &self.resident {
            let last = (self.shape.samples as i64 - 1) * self.shape.scrape_ms;
            assert!(
                hints.start_ms <= 0 && hints.end_ms >= last,
                "a resident source is not clipped; query its whole range"
            );
            return Ok(MemorySourceConfig::try_new_exec(
                std::slice::from_ref(batches),
                self.schema.clone(),
                None,
            )?);
        }
        let part: Arc<dyn PartitionStream> =
            Arc::new(self.partition(hints.start_ms, hints.end_ms, hints.window_ms));
        let plan = Arc::new(StreamingTableExec::try_new(
            self.schema.clone(),
            vec![part],
            None,
            [],
            false,
            None,
        )?);
        Ok(plan)
    }
}

#[derive(Debug)]
struct Partition {
    shape: Shape,
    schema: SchemaRef,
    id: SeriesIdEncoding,
    probe: Option<ChunkProbe>,
    start_ms: i64,
    end_ms: i64,
    block: Block,
}

impl Partition {
    /// Only the current batch's row descriptors and arrays are materialized;
    /// an identity may span any number of batches without reopening later.
    fn batches(&self) -> impl Iterator<Item = Result<RecordBatch>> + Send + 'static {
        let s = &self.shape;
        let lo = (self.start_ms.max(0) + s.scrape_ms - 1) / s.scrape_ms;
        let hi = ((self.end_ms / s.scrape_ms) + 1).min(s.samples as i64);
        let (lo, hi) = if self.probe.is_some() {
            // Probe chunks all intersect the 0..60s query. Preserve their
            // future tails rather than silently benchmarking clipped input.
            assert!(s.samples > 8 && self.start_ms <= 0 && self.end_ms >= 4 * s.scrape_ms);
            (0, s.samples)
        } else {
            (lo as usize, (hi.max(lo)) as usize)
        };
        let chunk = s.chunk.unwrap_or(usize::MAX).max(1);
        let probe = self.probe;
        let series = s.series;
        let id = self.id;
        let mut k = 0;
        let mut from = lo;
        let mut rows = std::iter::from_fn(move || {
            if k == series || from >= hi {
                return None;
            }
            let to = match probe {
                Some(ChunkProbe::Second) if from == 0 => 4,
                Some(_) => hi,
                None => from.saturating_add(chunk).min(hi),
            };
            // k is immutable full identity, not a selected-row ordinal.
            let row = (k, from, to);
            from = to;
            if from == hi {
                k += 1;
                from = lo;
            }
            Some(row)
        });
        let schema = self.schema.clone();
        let scrape_ms = s.scrape_ms;
        let per_batch = s.rows_per_batch;
        let block = self.block;
        std::iter::from_fn(move || {
            let rows: Vec<(usize, usize, usize)> = rows.by_ref().take(per_batch).collect();
            (!rows.is_empty()).then(|| {
                build(
                    &schema,
                    &rows,
                    scrape_ms,
                    block,
                    id,
                    matches!(probe, Some(ChunkProbe::Stale)).then_some(8),
                )
            })
        })
    }
}

impl PartitionStream for Partition {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            futures::stream::iter(self.batches()),
        ))
    }
}

fn code(i: usize) -> &'static str {
    if i.is_multiple_of(16) {
        "500"
    } else {
        "200"
    }
}

fn pod(i: usize) -> String {
    format!("nginx-{i}")
}

/// The value of series `i` at sample `j`: a counter that resets every
/// 1000 samples, computed in closed form so a chunk needs no history.
fn value(i: usize, j: usize) -> f64 {
    let base = j - j % 1000;
    (base..=j).map(|k| 1.0 + ((i + k) % 7) as f64).sum()
}

fn build(
    schema: &SchemaRef,
    rows: &[(usize, usize, usize)],
    scrape_ms: i64,
    block: Block,
    id: SeriesIdEncoding,
    stale_at: Option<usize>,
) -> Result<RecordBatch> {
    let mut labels: Vec<StringViewBuilder> =
        NAMES.iter().map(|_| StringViewBuilder::new()).collect();
    let n: usize = rows.iter().map(|(_, from, to)| to - from).sum();
    let mut ts: Vec<i64> = Vec::with_capacity(n);
    let mut vs: Vec<f64> = Vec::with_capacity(n);
    let mut offsets: Vec<i32> = Vec::with_capacity(rows.len() + 1);
    offsets.push(0);
    for &(i, from, to) in rows {
        labels[0].append_value("http_requests_total");
        labels[1].append_value(code(i));
        labels[2].append_value(pod(i));
        labels[3].append_value(format!("/{}", i % 8));
        // One running sum per row rather than `value` per sample.
        let mut v = value(i, from);
        for j in from..to {
            if j > from {
                v = if j % 1000 == 0 { 0.0 } else { v } + 1.0 + ((i + j) % 7) as f64;
            }
            ts.push(j as i64 * scrape_ms);
            vs.push(v);
        }
        if let Some(stale_at) = stale_at.filter(|&j| (from..to).contains(&j)) {
            let index = vs.len() - (to - stale_at);
            vs[index] = f64::from_bits(promql_engine::selector::STALE_NAN_BITS);
        }
        offsets.push(ts.len() as i32);
    }
    let DataType::Struct(label_fields) = labels_type(&NAMES.map(String::from)) else {
        unreachable!("labels are a struct")
    };
    let labels = StructArray::new(
        label_fields,
        labels
            .iter_mut()
            .map(|b| Arc::new(b.finish()) as ArrayRef)
            .collect(),
        None,
    );
    let entries = StructArray::new(
        sample_fields(),
        vec![
            Arc::new(TimestampMillisecondArray::from(ts)),
            Arc::new(Float64Array::from(vs)),
        ],
        None,
    );
    let samples = ListArray::new(
        sample_item(),
        OffsetBuffer::new(offsets.into()),
        Arc::new(entries),
        None,
    );
    let [block_start, block_end] = block.columns(rows.len());
    let mut columns: Vec<ArrayRef> =
        vec![Arc::new(labels), Arc::new(samples), block_start, block_end];
    // IDs encode full identity, never a filtered row or surviving grouping label.
    let ids = rows.iter().map(|&(i, _, _)| u64::MAX - i as u64);
    columns.push(match id {
        SeriesIdEncoding::UInt64 => Arc::new(UInt64Array::from(ids.collect::<Vec<_>>())),
        SeriesIdEncoding::FixedSizeBinary => {
            let bytes = ids.map(|id| {
                let mut bytes = [0xA5; 16];
                bytes[8..].copy_from_slice(&id.to_be_bytes());
                Some(bytes)
            });
            Arc::new(FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                bytes, 16,
            )?)
        }
    });
    Ok(RecordBatch::try_new(schema.clone(), columns)?)
}
