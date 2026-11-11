//! Native histogram samples in Arrow: the `histograms` column.
//!
//! ```text
//! histograms  List<Struct<
//!   timestamp: Timestamp(ms),
//!   counter_reset_hint: UInt8,        0 unknown, 1 reset, 2 not reset, 3 gauge
//!   schema: Int32,
//!   count, sum, zero_threshold, zero_count: Float64,
//!   negative_spans: List<Struct<offset: Int32, length: UInt32>>,
//!   negative_buckets: List<Float64>,
//!   positive_spans: List<Struct<offset: Int32, length: UInt32>>,
//!   positive_buckets: List<Float64>,
//!   custom_values: List<Float64>,
//! >>
//! ```
//!
//! A sibling of `floats`, not a child of it and not a union with it. A
//! float sample then costs what it did before histograms existed, a
//! consumer that reads one sample type never touches the other's
//! buffers, and DataFusion prunes a top-level column where it would not
//! prune a struct child. Prometheus's `promql.Series` holds `Floats` and
//! `Histograms` apart the same way (`promql/value.go` at 83962c35).
//!
//! The entry is Prometheus's `histogram.FloatHistogram` field for field,
//! spans and absolute bucket counts as they are, with the timestamp
//! first: every helper that only moves samples in time ([`clip`],
//! chunking, emptiness) reads one child and never decodes a bucket.
//! Nothing is nullable. Staleness is `sum` holding StaleNaN's bits, as
//! `value.IsStaleNaN(h.Sum)` is upstream.
//!
//! Within one series a timestamp is in `floats` or in `histograms`, never
//! both; evaluation merges the two lists by time and nowhere else.
//!
//! [`clip`]: crate::series::clip

use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, Float64Array, Int32Array, ListArray, StructArray,
    TimestampMillisecondArray, UInt32Array, UInt8Array,
};
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::arrow::datatypes::{
    DataType, Field, FieldRef, Fields, Float64Type, Int32Type, TimestampMillisecondType,
    UInt32Type, UInt8Type,
};
use promql_histogram::{CounterResetHint, FloatHistogram, HistogramError, KahanSum, Span};

use crate::series::{timestamp_type, LIST_ITEM, TIMESTAMP};

pub const HISTOGRAMS: &str = "histograms";

/// A sample of either type, owned: never a slice of a source batch, which
/// would pin every other series' buckets with it.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Float(f64),
    Histogram(Arc<FloatHistogram>),
}

fn list_of(item: DataType) -> DataType {
    DataType::List(Arc::new(Field::new(LIST_ITEM, item, false)))
}

fn span_fields() -> Fields {
    Fields::from(vec![
        Field::new("offset", DataType::Int32, false),
        Field::new("length", DataType::UInt32, false),
    ])
}

/// The fields of one histogram sample, timestamp first.
pub fn histogram_fields() -> Fields {
    let spans = list_of(DataType::Struct(span_fields()));
    let buckets = list_of(DataType::Float64);
    Fields::from(vec![
        Field::new(TIMESTAMP, timestamp_type(), false),
        Field::new("counter_reset_hint", DataType::UInt8, false),
        Field::new("schema", DataType::Int32, false),
        Field::new("count", DataType::Float64, false),
        Field::new("sum", DataType::Float64, false),
        Field::new("zero_threshold", DataType::Float64, false),
        Field::new("zero_count", DataType::Float64, false),
        Field::new("negative_spans", spans.clone(), false),
        Field::new("negative_buckets", buckets.clone(), false),
        Field::new("positive_spans", spans, false),
        Field::new("positive_buckets", buckets.clone(), false),
        Field::new("custom_values", buckets, false),
    ])
}

pub fn histogram_item() -> FieldRef {
    Arc::new(Field::new(
        LIST_ITEM,
        DataType::Struct(histogram_fields()),
        false,
    ))
}

pub fn histograms_type() -> DataType {
    DataType::List(histogram_item())
}

pub fn histograms_field() -> Field {
    Field::new(HISTOGRAMS, histograms_type(), false)
}

/// Whether `data_type` is the histograms column's, names included.
/// Arrow's own comparison ignores nested field names, and every reader
/// here binds children by position after this check.
pub(crate) fn is_histograms_type(data_type: &DataType) -> bool {
    fn same(a: &DataType, b: &DataType) -> bool {
        match (a, b) {
            (DataType::List(a), DataType::List(b)) => {
                a.name() == b.name() && same(a.data_type(), b.data_type())
            }
            (DataType::Struct(a), DataType::Struct(b)) => {
                a.len() == b.len()
                    && a.iter()
                        .zip(b)
                        .all(|(a, b)| a.name() == b.name() && same(a.data_type(), b.data_type()))
            }
            _ => a == b,
        }
    }
    same(data_type, &histograms_type())
}

fn hint_of(code: u8) -> CounterResetHint {
    match code {
        1 => CounterResetHint::Reset,
        2 => CounterResetHint::NotReset,
        3 => CounterResetHint::Gauge,
        // A code this engine does not know says nothing about resets,
        // which is what `unknown` means.
        _ => CounterResetHint::Unknown,
    }
}

struct Floats<'a> {
    offsets: &'a [i32],
    values: &'a [f64],
}

impl Floats<'_> {
    fn at(&self, row: usize) -> &[f64] {
        &self.values[self.offsets[row] as usize..self.offsets[row + 1] as usize]
    }
}

struct Spans<'a> {
    offsets: &'a [i32],
    offset: &'a [i32],
    length: &'a [u32],
}

impl Spans<'_> {
    fn at(&self, row: usize) -> Vec<Span> {
        (self.offsets[row] as usize..self.offsets[row + 1] as usize)
            .map(|i| Span {
                offset: self.offset[i],
                length: self.length[i],
            })
            .collect()
    }
}

/// The typed children of a histograms list's entries, bound once per
/// batch so that reading a sample is slice indexing.
///
/// `sum` and `count` are plain slices: `histogram_count` and the
/// staleness check read them without building a [`FloatHistogram`].
pub(crate) struct HistogramColumns<'a> {
    pub(crate) timestamps: &'a [i64],
    hint: &'a [u8],
    schema: &'a [i32],
    pub(crate) count: &'a [f64],
    pub(crate) sum: &'a [f64],
    zero_threshold: &'a [f64],
    zero_count: &'a [f64],
    negative_spans: Spans<'a>,
    negative_buckets: Floats<'a>,
    positive_spans: Spans<'a>,
    positive_buckets: Floats<'a>,
    custom_values: Floats<'a>,
}

impl<'a> HistogramColumns<'a> {
    /// `entries` is the struct child of a column [`is_histograms_type`]
    /// accepted, or a slice of it.
    pub(crate) fn new(entries: &'a StructArray) -> Self {
        let f64s =
            |i: usize| -> &'a [f64] { entries.column(i).as_primitive::<Float64Type>().values() };
        let floats = |i: usize| {
            let list = entries.column(i).as_list::<i32>();
            Floats {
                offsets: list.value_offsets(),
                values: list.values().as_primitive::<Float64Type>().values(),
            }
        };
        let spans = |i: usize| {
            let list = entries.column(i).as_list::<i32>();
            let values = list.values().as_struct();
            Spans {
                offsets: list.value_offsets(),
                offset: values.column(0).as_primitive::<Int32Type>().values(),
                length: values.column(1).as_primitive::<UInt32Type>().values(),
            }
        };
        Self {
            timestamps: entries
                .column(0)
                .as_primitive::<TimestampMillisecondType>()
                .values(),
            hint: entries.column(1).as_primitive::<UInt8Type>().values(),
            schema: entries.column(2).as_primitive::<Int32Type>().values(),
            count: f64s(3),
            sum: f64s(4),
            zero_threshold: f64s(5),
            zero_count: f64s(6),
            negative_spans: spans(7),
            negative_buckets: floats(8),
            positive_spans: spans(9),
            positive_buckets: floats(10),
            custom_values: floats(11),
        }
    }

    /// Sample `row` as an owned histogram: the one place buckets are copied
    /// out of a batch.
    pub(crate) fn decode(&self, row: usize) -> FloatHistogram {
        FloatHistogram {
            counter_reset_hint: hint_of(self.hint[row]),
            schema: self.schema[row],
            count: self.count[row],
            sum: self.sum[row],
            zero_threshold: self.zero_threshold[row],
            zero_count: self.zero_count[row],
            negative_spans: self.negative_spans.at(row),
            negative_buckets: self.negative_buckets.at(row).to_vec(),
            positive_spans: self.positive_spans.at(row),
            positive_buckets: self.positive_buckets.at(row).to_vec(),
            custom_values: self.custom_values.at(row).to_vec(),
        }
    }
}

/// The mean of histograms as upstream takes it, in `funcAvgOverTime` and
/// in `aggregation`'s `AVG` arm (`promql/functions.go` and `engine.go` at
/// 83962c35): a Kahan sum divided by the count at the end, for as long
/// as the sum stays finite. Once adding a histogram would overflow it,
/// the sum becomes a running mean and each further histogram is folded
/// in by its weight. The switch changes the rounding of everything after
/// it, so it has to happen at the same sample upstream makes it.
#[derive(Debug, Clone)]
pub(crate) struct Mean {
    /// The sum, or the mean once `incremental`.
    acc: KahanSum,
    count: f64,
    incremental: bool,
}

impl Mean {
    pub(crate) fn new(first: FloatHistogram) -> Self {
        Self {
            acc: KahanSum::new(first),
            count: 1.0,
            incremental: false,
        }
    }

    pub(crate) fn from_parts(acc: KahanSum, count: f64, incremental: bool) -> Self {
        Self {
            acc,
            count,
            incremental,
        }
    }

    pub(crate) fn into_parts(self) -> (KahanSum, f64, bool) {
        (self.acc, self.count, self.incremental)
    }

    pub(crate) fn heap_size(&self) -> usize {
        self.acc.heap_size()
    }

    /// Fold one more histogram in. An error is two histograms that do not
    /// add, an exponential one and one with custom buckets.
    pub(crate) fn add(&mut self, h: &FloatHistogram) -> Result<(), HistogramError> {
        self.count += 1.0;
        if !self.incremental {
            let mut next = self.acc.clone();
            next.add(h)?;
            if !next.has_overflow() {
                self.acc = next;
                return Ok(());
            }
            self.incremental = true;
            self.acc.div(self.count - 1.0);
        }
        // `mean*q + h/count`, the compensation scaled with the mean.
        self.acc.mul((self.count - 1.0) / self.count);
        let mut share = h.clone();
        share.div(self.count);
        self.acc.add(&share).map(|_| ())
    }

    /// Fold another mean in, for the two halves of a two-stage
    /// aggregation. Upstream has one stage and so no rule for this; two
    /// sums add as sums, and otherwise each mean counts by its share,
    /// which is what folding its histograms in one by one amounts to.
    pub(crate) fn merge(&mut self, mut other: Mean) -> Result<(), HistogramError> {
        let total = self.count + other.count;
        if !self.incremental && !other.incremental {
            let mut next = self.acc.clone();
            let (sum, compensation) = other.acc.clone().into_parts();
            next.add_parts(&sum, &compensation)?;
            if !next.has_overflow() {
                self.acc = next;
                self.count = total;
                return Ok(());
            }
        }
        for mean in [&mut *self, &mut other] {
            if !mean.incremental {
                mean.acc.div(mean.count);
                mean.incremental = true;
            }
            mean.acc.mul(mean.count / total);
        }
        let (sum, compensation) = other.acc.into_parts();
        self.acc.add_parts(&sum, &compensation)?;
        self.count = total;
        Ok(())
    }

    pub(crate) fn finish(self) -> Result<FloatHistogram, HistogramError> {
        let (mut mean, mut compensation) = self.acc.into_parts();
        if !self.incremental {
            mean.div(self.count);
            compensation.div(self.count);
        }
        mean.add(&compensation)?;
        mean.compact();
        Ok(mean)
    }
}

/// The heap a decoded histogram owns, with its `Arc` header: what a window
/// buffer or a result vector is charged for holding one.
pub(crate) fn heap_size(h: &FloatHistogram) -> usize {
    std::mem::size_of::<FloatHistogram>()
        + 2 * std::mem::size_of::<usize>()
        + (h.negative_spans.capacity() + h.positive_spans.capacity()) * std::mem::size_of::<Span>()
        + (h.negative_buckets.capacity()
            + h.positive_buckets.capacity()
            + h.custom_values.capacity())
            * std::mem::size_of::<f64>()
}

#[derive(Debug)]
struct FloatLists {
    offsets: Vec<i32>,
    values: Vec<f64>,
}

impl Default for FloatLists {
    fn default() -> Self {
        Self {
            offsets: vec![0],
            values: Vec::new(),
        }
    }
}

impl FloatLists {
    fn push(&mut self, values: &[f64]) {
        self.values.extend_from_slice(values);
        self.offsets.push(offset(self.values.len()));
    }

    fn size(&self) -> usize {
        self.offsets.capacity() * 4 + self.values.capacity() * 8
    }

    fn finish(&mut self) -> ArrayRef {
        let lists = std::mem::take(self);
        Arc::new(ListArray::new(
            Arc::new(Field::new(LIST_ITEM, DataType::Float64, false)),
            OffsetBuffer::new(lists.offsets.into()),
            Arc::new(Float64Array::from(lists.values)),
            None,
        ))
    }
}

#[derive(Debug)]
struct SpanLists {
    offsets: Vec<i32>,
    offset: Vec<i32>,
    length: Vec<u32>,
}

impl Default for SpanLists {
    fn default() -> Self {
        Self {
            offsets: vec![0],
            offset: Vec::new(),
            length: Vec::new(),
        }
    }
}

impl SpanLists {
    fn push(&mut self, spans: &[Span]) {
        self.offset.extend(spans.iter().map(|s| s.offset));
        self.length.extend(spans.iter().map(|s| s.length));
        self.offsets.push(offset(self.offset.len()));
    }

    fn size(&self) -> usize {
        (self.offsets.capacity() + self.offset.capacity() + self.length.capacity()) * 4
    }

    fn finish(&mut self) -> ArrayRef {
        let lists = std::mem::take(self);
        Arc::new(ListArray::new(
            Arc::new(Field::new(
                LIST_ITEM,
                DataType::Struct(span_fields()),
                false,
            )),
            OffsetBuffer::new(lists.offsets.into()),
            Arc::new(StructArray::new(
                span_fields(),
                vec![
                    Arc::new(Int32Array::from(lists.offset)),
                    Arc::new(UInt32Array::from(lists.length)),
                ],
                None,
            )),
            None,
        ))
    }
}

/// Past `i32::MAX` entries a list offset cannot say where a row ends.
fn offset(len: usize) -> i32 {
    i32::try_from(len).expect("more than i32::MAX entries in one histograms column")
}

/// Rows of a histograms column under construction, the histogram
/// counterpart of [`crate::series::SamplesBuilder`]: one vector per leaf,
/// handed to Arrow without a copy. Pushing copies the histogram's spans
/// and buckets in, so the caller's histogram can be dropped afterwards.
#[derive(Debug)]
pub(crate) struct HistogramsBuilder {
    timestamps: Vec<i64>,
    hint: Vec<u8>,
    schema: Vec<i32>,
    count: Vec<f64>,
    sum: Vec<f64>,
    zero_threshold: Vec<f64>,
    zero_count: Vec<f64>,
    negative_spans: SpanLists,
    negative_buckets: FloatLists,
    positive_spans: SpanLists,
    positive_buckets: FloatLists,
    custom_values: FloatLists,
    rows: Vec<i32>,
}

impl Default for HistogramsBuilder {
    fn default() -> Self {
        Self {
            timestamps: Vec::new(),
            hint: Vec::new(),
            schema: Vec::new(),
            count: Vec::new(),
            sum: Vec::new(),
            zero_threshold: Vec::new(),
            zero_count: Vec::new(),
            negative_spans: SpanLists::default(),
            negative_buckets: FloatLists::default(),
            positive_spans: SpanLists::default(),
            positive_buckets: FloatLists::default(),
            custom_values: FloatLists::default(),
            rows: vec![0],
        }
    }
}

impl HistogramsBuilder {
    pub(crate) fn push(&mut self, t: i64, h: &FloatHistogram) {
        self.timestamps.push(t);
        self.hint.push(h.counter_reset_hint as u8);
        self.schema.push(h.schema);
        self.count.push(h.count);
        self.sum.push(h.sum);
        self.zero_threshold.push(h.zero_threshold);
        self.zero_count.push(h.zero_count);
        self.negative_spans.push(&h.negative_spans);
        self.negative_buckets.push(&h.negative_buckets);
        self.positive_spans.push(&h.positive_spans);
        self.positive_buckets.push(&h.positive_buckets);
        self.custom_values.push(&h.custom_values);
    }

    pub(crate) fn finish_row(&mut self) {
        self.rows.push(offset(self.timestamps.len()));
    }

    /// Rows finished and not yet taken.
    #[cfg(test)]
    pub(crate) fn rows(&self) -> usize {
        self.rows.len() - 1
    }

    /// Samples pushed and not yet taken, in finished rows or the open one.
    pub(crate) fn len(&self) -> usize {
        self.timestamps.len()
    }

    /// Every finished row as the histograms column. Samples of an open row
    /// must not be pending: callers finish the row first.
    pub(crate) fn take_all(&mut self) -> ListArray {
        debug_assert_eq!(*self.rows.last().expect("offsets") as usize, self.len());
        let rows = std::mem::replace(&mut self.rows, vec![0]);
        let columns: Vec<ArrayRef> = vec![
            Arc::new(TimestampMillisecondArray::from(std::mem::take(
                &mut self.timestamps,
            ))),
            Arc::new(UInt8Array::from(std::mem::take(&mut self.hint))),
            Arc::new(Int32Array::from(std::mem::take(&mut self.schema))),
            Arc::new(Float64Array::from(std::mem::take(&mut self.count))),
            Arc::new(Float64Array::from(std::mem::take(&mut self.sum))),
            Arc::new(Float64Array::from(std::mem::take(&mut self.zero_threshold))),
            Arc::new(Float64Array::from(std::mem::take(&mut self.zero_count))),
            self.negative_spans.finish(),
            self.negative_buckets.finish(),
            self.positive_spans.finish(),
            self.positive_buckets.finish(),
            self.custom_values.finish(),
        ];
        ListArray::new(
            histogram_item(),
            OffsetBuffer::new(rows.into()),
            Arc::new(StructArray::new(histogram_fields(), columns, None)),
            None,
        )
    }

    /// Capacity of every leaf: buckets and spans, not only the per-sample
    /// scalars, since a histogram's weight is in its buckets.
    pub(crate) fn size(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.timestamps.capacity() * 8
            + self.hint.capacity()
            + self.schema.capacity() * 4
            + (self.count.capacity()
                + self.sum.capacity()
                + self.zero_threshold.capacity()
                + self.zero_count.capacity())
                * 8
            + self.negative_spans.size()
            + self.positive_spans.size()
            + self.negative_buckets.size()
            + self.positive_buckets.size()
            + self.custom_values.size()
            + self.rows.capacity() * 4
    }
}

/// One row of a histograms column, for a caller that holds whole
/// histograms: a store filling a batch, or a test.
pub fn encode_row(samples: &[(i64, FloatHistogram)]) -> StructArray {
    let mut builder = HistogramsBuilder::default();
    for (t, h) in samples {
        builder.push(*t, h);
    }
    builder.finish_row();
    builder.take_all().values().as_struct().clone()
}

/// The samples of one row's entries, decoded.
pub fn decode_row(entries: &StructArray) -> Vec<(i64, FloatHistogram)> {
    let columns = HistogramColumns::new(entries);
    (0..entries.len())
        .map(|i| (columns.timestamps[i], columns.decode(i)))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn histogram() -> FloatHistogram {
        FloatHistogram {
            counter_reset_hint: CounterResetHint::Gauge,
            schema: 1,
            count: 12.0,
            sum: -3.5,
            zero_threshold: 0.001,
            zero_count: 2.0,
            negative_spans: vec![Span {
                offset: -1,
                length: 2,
            }],
            negative_buckets: vec![1.0, 2.0],
            positive_spans: vec![
                Span {
                    offset: 0,
                    length: 1,
                },
                Span {
                    offset: 3,
                    length: 2,
                },
            ],
            positive_buckets: vec![3.0, 4.0, 0.0],
            custom_values: Vec::new(),
        }
    }

    #[test]
    fn a_histogram_survives_arrow_field_for_field() {
        let stale = FloatHistogram {
            sum: f64::from_bits(crate::selector::STALE_NAN_BITS),
            ..FloatHistogram::default()
        };
        let custom = FloatHistogram {
            schema: promql_histogram::CUSTOM_SCHEMA,
            custom_values: vec![1.0, 5.0],
            positive_spans: vec![Span {
                offset: 0,
                length: 3,
            }],
            positive_buckets: vec![1.0, 0.0, 2.0],
            count: 3.0,
            sum: 9.0,
            ..FloatHistogram::default()
        };
        let samples = vec![(10, histogram()), (20, stale), (30, custom)];
        let entries = encode_row(&samples);
        assert_eq!(entries.data_type(), &DataType::Struct(histogram_fields()));
        let decoded = decode_row(&entries);
        assert_eq!(decoded.len(), 3);
        for ((t, h), (want_t, want)) in decoded.iter().zip(&samples) {
            assert_eq!(t, want_t);
            // `equals` ignores the hint, and the hint must survive too.
            assert!(h.equals(want), "{h:?} != {want:?}");
            assert_eq!(h.counter_reset_hint, want.counter_reset_hint);
        }
        // StaleNaN is a payload, not just a NaN.
        assert_eq!(decoded[1].1.sum.to_bits(), crate::selector::STALE_NAN_BITS);
    }

    #[test]
    fn a_sliced_row_decodes_its_own_samples() {
        let mut builder = HistogramsBuilder::default();
        builder.push(1, &FloatHistogram::default());
        builder.finish_row();
        builder.push(2, &histogram());
        builder.push(3, &FloatHistogram::default());
        builder.finish_row();
        let list = builder.take_all();
        assert_eq!(builder.rows(), 0);
        let row = list.value(1);
        let decoded = decode_row(row.as_struct());
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0].0, 2);
        assert!(decoded[0].1.equals(&histogram()));
        assert!(decoded[1].1.equals(&FloatHistogram::default()));
    }

    #[test]
    fn the_type_check_reads_nested_names() {
        assert!(is_histograms_type(&histograms_type()));
        assert!(!is_histograms_type(&crate::series::floats_type()));
        let mut fields = histogram_fields().to_vec();
        fields[4] = Arc::new(Field::new("total", DataType::Float64, false));
        let renamed = list_of(DataType::Struct(fields.into()));
        // Arrow calls these equal; a reader binding `sum` by position would
        // then read a column that means something else.
        assert!(renamed.equals_datatype(&histograms_type()));
        assert!(!is_histograms_type(&renamed));
    }

    #[test]
    fn a_mean_is_the_sum_over_the_count_until_the_sum_overflows() {
        let counted = |count: f64| FloatHistogram {
            count,
            ..FloatHistogram::default()
        };
        let mut mean = Mean::new(counted(1.0));
        mean.add(&counted(2.0)).unwrap();
        mean.add(&counted(6.0)).unwrap();
        assert_eq!(mean.finish().unwrap().count, 3.0);

        // Two of these overflow a sum; their mean is still the value.
        let huge = f64::MAX / 1.5;
        let mut mean = Mean::new(counted(huge));
        mean.add(&counted(huge)).unwrap();
        mean.add(&counted(huge)).unwrap();
        let (_, _, incremental) = mean.clone().into_parts();
        assert!(incremental);
        let got = mean.finish().unwrap().count;
        assert!(got.is_finite());
        assert!((got - huge).abs() / huge < 1e-12, "{got} != {huge}");
    }

    #[test]
    fn merged_means_weigh_each_half_by_its_count() {
        let counted = |count: f64| FloatHistogram {
            count,
            ..FloatHistogram::default()
        };
        let mut left = Mean::new(counted(1.0));
        left.add(&counted(3.0)).unwrap();
        left.add(&counted(5.0)).unwrap();
        left.merge(Mean::new(counted(11.0))).unwrap();
        assert_eq!(left.finish().unwrap().count, 5.0);

        let huge = f64::MAX / 1.5;
        let mut left = Mean::new(counted(huge));
        left.add(&counted(huge)).unwrap();
        let mut right = Mean::new(counted(huge));
        right.add(&counted(huge)).unwrap();
        right.add(&counted(huge)).unwrap();
        left.merge(right).unwrap();
        let got = left.finish().unwrap().count;
        assert!((got - huge).abs() / huge < 1e-12, "{got} != {huge}");
    }

    #[test]
    fn the_builder_charges_for_buckets() {
        let mut builder = HistogramsBuilder::default();
        let empty = builder.size();
        let wide = FloatHistogram {
            positive_buckets: vec![1.0; 1000],
            ..FloatHistogram::default()
        };
        builder.push(0, &wide);
        assert!(builder.size() >= empty + 8000);
    }
}
