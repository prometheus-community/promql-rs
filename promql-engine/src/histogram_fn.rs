//! The DataFusion functions that read the `histograms` column.
//!
//! Above evaluation a series row carries `floats` and, where its
//! expression can produce them, `histograms`. Operators that only know
//! floats keep reading `floats` and are untouched. These four functions
//! are what the planner composes for the ones that do know histograms:
//!
//! - [`to_floats`]: one float per histogram, `histogram_count` and its
//!   family (`simpleHistogramFunc`, `funcHistogramQuantile` and
//!   `funcHistogramFraction` in `promql/functions.go` at 83962c35).
//! - [`map`]: one histogram per histogram, a histogram times or over a
//!   scalar or trimmed at one (`vectorElemBinop` in `promql/engine.go`).
//! - [`merge`]: two float lists of one row as one, by timestamp. A step
//!   holds a float or a histogram, so floats computed from the histograms
//!   interleave with the row's own floats and never collide.
//! - [`without`]: the samples of one list at timestamps the other list
//!   has none, which is how an aggregation that saw both types at a step
//!   answers nothing there.
//!
//! All four are row-local scalar functions: a row is still a series, in
//! the block it was in.

use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, BooleanBufferBuilder, ListArray, StructArray,
};
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::arrow::compute::filter;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, TimestampMillisecondType};
use datafusion::common::{plan_err, ScalarValue};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{
    lit, ColumnarValue, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl,
    Signature, Volatility,
};
use promql_histogram::{statistics, FloatHistogram, TrimDirection};

use crate::histogram::{self, HistogramColumns, HistogramsBuilder};
use crate::series::{self, SamplesBuilder};

/// What [`to_floats`] reads off each histogram.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ToFloat {
    Count,
    Sum,
    Avg,
    Stddev,
    Stdvar,
    /// `histogram_quantile(q, h)`.
    Quantile(f64),
    /// `histogram_fraction(lower, upper, h)`.
    Fraction(f64, f64),
    /// `timestamp(h)`: the sample's time in seconds.
    Timestamp,
    /// The same number for every histogram: `1` to count it as present,
    /// NaN where `scalar()` reads a vector element that is not a float.
    Constant(f64),
}

impl ToFloat {
    /// The histogram functions of one vector argument, by PromQL name.
    pub fn parse(name: &str) -> Option<Self> {
        Some(match name {
            "histogram_count" => Self::Count,
            "histogram_sum" => Self::Sum,
            "histogram_avg" => Self::Avg,
            "histogram_stddev" => Self::Stddev,
            "histogram_stdvar" => Self::Stdvar,
            _ => return None,
        })
    }

    fn name(self) -> &'static str {
        match self {
            Self::Count => "count",
            Self::Sum => "sum",
            Self::Avg => "avg",
            Self::Stddev => "stddev",
            Self::Stdvar => "stdvar",
            Self::Quantile(_) => "quantile",
            Self::Fraction(..) => "fraction",
            Self::Timestamp => "timestamp",
            Self::Constant(_) => "constant",
        }
    }

    fn args(self) -> (Option<f64>, Option<f64>) {
        match self {
            Self::Quantile(q) => (Some(q), None),
            Self::Fraction(lower, upper) => (Some(lower), Some(upper)),
            Self::Constant(v) => (Some(v), None),
            _ => (None, None),
        }
    }

    fn bind(name: &str, a: Option<f64>, b: Option<f64>) -> Option<Self> {
        let nan = f64::NAN;
        Some(match name {
            "count" => Self::Count,
            "sum" => Self::Sum,
            "avg" => Self::Avg,
            "stddev" => Self::Stddev,
            "stdvar" => Self::Stdvar,
            "quantile" => Self::Quantile(a.unwrap_or(nan)),
            "fraction" => Self::Fraction(a.unwrap_or(nan), b.unwrap_or(nan)),
            "timestamp" => Self::Timestamp,
            "constant" => Self::Constant(a.unwrap_or(nan)),
            _ => return None,
        })
    }
}

/// `promql_histogram_to_floats(histograms, '<what>', a, b)`.
pub fn to_floats(histograms: Expr, what: ToFloat) -> Expr {
    let (a, b) = what.args();
    ScalarUDF::new_from_impl(ToFloats::default()).call(vec![
        histograms,
        lit(what.name()),
        lit(ScalarValue::Float64(a)),
        lit(ScalarValue::Float64(b)),
    ])
}

/// A histogram against a scalar: `vectorElemBinop`'s arms with a
/// histogram on the left and a float on the right, and `*` the other way
/// round too. `Mul(-1)` is unary minus.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Map {
    Mul(f64),
    Div(f64),
    /// `h </ x`: the observations at or below `x`.
    TrimUpper(f64),
    /// `h >/ x`: the observations above `x`.
    TrimLower(f64),
}

impl Map {
    fn parts(self) -> (&'static str, f64) {
        match self {
            Map::Mul(f) => ("mul", f),
            Map::Div(f) => ("div", f),
            Map::TrimUpper(x) => ("trim_upper", x),
            Map::TrimLower(x) => ("trim_lower", x),
        }
    }

    fn bind(name: &str, x: f64) -> Option<Self> {
        Some(match name {
            "mul" => Map::Mul(x),
            "div" => Map::Div(x),
            "trim_upper" => Map::TrimUpper(x),
            "trim_lower" => Map::TrimLower(x),
            _ => return None,
        })
    }

    /// The histogram this maps `h` to.
    pub fn apply(self, mut h: FloatHistogram) -> FloatHistogram {
        match self {
            // `hlhs.Copy().Mul(rhs).Compact(0)` and its `Div`.
            Map::Mul(factor) => {
                h.mul(factor).compact();
                h
            }
            Map::Div(divisor) => {
                h.div(divisor).compact();
                h
            }
            Map::TrimUpper(x) => h.trim_buckets(x, TrimDirection::Upper),
            Map::TrimLower(x) => h.trim_buckets(x, TrimDirection::Lower),
        }
    }
}

/// `promql_histogram_map(histograms, '<operation>', x)`.
pub fn map(histograms: Expr, by: Map) -> Expr {
    let (name, x) = by.parts();
    ScalarUDF::new_from_impl(MapHistograms::default()).call(vec![histograms, lit(name), lit(x)])
}

/// `promql_merge_floats(a, b)`: both rows' floats in timestamp order.
pub fn merge(a: Expr, b: Expr) -> Expr {
    ScalarUDF::new_from_impl(MergeFloats::default()).call(vec![a, b])
}

/// `promql_samples_without(samples, other)`: `samples` less the entries
/// whose timestamp `other` also has. Either list may be floats or
/// histograms; only their timestamps are compared.
pub fn without(samples: Expr, other: Expr) -> Expr {
    ScalarUDF::new_from_impl(SamplesWithout::default()).call(vec![samples, other])
}

/// An empty histograms list on every row, for the branch of a plan that
/// has no `histograms` column where a sibling does, or where a function
/// takes the column either way.
pub fn no_histograms() -> Expr {
    lit(ScalarValue::List(Arc::new(ListArray::new(
        histogram::histogram_item(),
        OffsetBuffer::new(vec![0, 0].into()),
        Arc::new(StructArray::new_null(histogram::histogram_fields(), 0)),
        None,
    ))))
}

/// Every function here, for the session to register.
pub fn udfs() -> Vec<ScalarUDF> {
    vec![
        ScalarUDF::new_from_impl(ToFloats::default()),
        ScalarUDF::new_from_impl(MapHistograms::default()),
        ScalarUDF::new_from_impl(MergeFloats::default()),
        ScalarUDF::new_from_impl(SamplesWithout::default()),
    ]
}

fn array(arg: &ColumnarValue, rows: usize) -> Result<ArrayRef> {
    match arg {
        ColumnarValue::Array(a) => Ok(Arc::clone(a)),
        ColumnarValue::Scalar(s) => s.to_array_of_size(rows),
    }
}

fn string_arg(name: &str, arg: &ColumnarValue) -> Result<String> {
    match arg {
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => Ok(s.clone()),
        other => Err(DataFusionError::Execution(format!(
            "{name}: expected a string literal, got {:?}",
            other.data_type()
        ))),
    }
}

fn float_arg(name: &str, arg: &ColumnarValue) -> Result<Option<f64>> {
    match arg {
        ColumnarValue::Scalar(ScalarValue::Float64(v)) => Ok(*v),
        ColumnarValue::Scalar(ScalarValue::Null) => Ok(None),
        other => Err(DataFusionError::Execution(format!(
            "{name}: expected a Float64 literal, got {:?}",
            other.data_type()
        ))),
    }
}

fn expect_histograms(name: &str, data_type: Option<&DataType>) -> Result<()> {
    if data_type.is_some_and(histogram::is_histograms_type) {
        return Ok(());
    }
    plan_err!(
        "{name}: expected {}, got {data_type:?}",
        histogram::histograms_type()
    )
}

fn expect_floats(name: &str, data_type: Option<&DataType>) -> Result<()> {
    if data_type.is_some_and(series::is_floats_type) {
        return Ok(());
    }
    plan_err!(
        "{name}: expected {}, got {data_type:?}",
        series::floats_type()
    )
}

/// The timestamps of a samples list's entries, of either sample type.
fn timestamps(list: &ListArray) -> &[i64] {
    list.values()
        .as_struct()
        .column(0)
        .as_primitive::<TimestampMillisecondType>()
        .values()
}

fn row(list: &ListArray, row: usize) -> std::ops::Range<usize> {
    let offsets = list.value_offsets();
    offsets[row] as usize..offsets[row + 1] as usize
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct ToFloats {
    signature: Signature,
}

impl Default for ToFloats {
    fn default() -> Self {
        Self {
            signature: Signature::any(4, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for ToFloats {
    fn name(&self) -> &str {
        "promql_histogram_to_floats"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        expect_histograms(self.name(), arg_types.first())?;
        Ok(series::floats_type())
    }

    /// A row is a series and a series has a list, possibly empty: never
    /// NULL, which the canonical shape has no place for.
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let types: Vec<DataType> = args
            .arg_fields
            .iter()
            .map(|f| f.data_type().clone())
            .collect();
        Ok(Arc::new(Field::new(
            self.name(),
            self.return_type(&types)?,
            false,
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let histograms = array(&args.args[0], args.number_rows)?;
        let name = string_arg(self.name(), &args.args[1])?;
        let what = ToFloat::bind(
            &name,
            float_arg(self.name(), &args.args[2])?,
            float_arg(self.name(), &args.args[3])?,
        )
        .ok_or_else(|| {
            DataFusionError::Execution(format!("{}: unknown function {name}", self.name()))
        })?;
        Ok(ColumnarValue::Array(Arc::new(apply_to_floats(
            histograms.as_list::<i32>(),
            what,
        ))))
    }
}

/// One float per histogram of every row, at the histogram's timestamp.
pub fn apply_to_floats(histograms: &ListArray, what: ToFloat) -> ListArray {
    let columns = HistogramColumns::new(histograms.values().as_struct());
    let mut out = SamplesBuilder::default();
    for r in 0..histograms.len() {
        for i in row(histograms, r) {
            let t = columns.timestamps[i];
            // Count, sum and the time are scalars of the sample: only the
            // functions that walk buckets copy them out of the batch.
            let v = match what {
                ToFloat::Count => columns.count[i],
                ToFloat::Sum => columns.sum[i],
                ToFloat::Avg => columns.sum[i] / columns.count[i],
                ToFloat::Timestamp => t as f64 / 1000.0,
                ToFloat::Constant(v) => v,
                ToFloat::Stddev => statistics::stddev(&columns.decode(i)),
                ToFloat::Stdvar => statistics::variance(&columns.decode(i)),
                ToFloat::Quantile(q) => statistics::quantile(q, &columns.decode(i)).value,
                ToFloat::Fraction(lower, upper) => {
                    statistics::fraction(lower, upper, &columns.decode(i)).value
                }
            };
            out.push(t, v);
        }
        out.finish_row();
    }
    out.take_all()
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct MapHistograms {
    signature: Signature,
}

impl Default for MapHistograms {
    fn default() -> Self {
        Self {
            signature: Signature::any(3, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for MapHistograms {
    fn name(&self) -> &str {
        "promql_histogram_map"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        expect_histograms(self.name(), arg_types.first())?;
        Ok(histogram::histograms_type())
    }

    /// A row is a series and a series has a list, possibly empty: never
    /// NULL, which the canonical shape has no place for.
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let types: Vec<DataType> = args
            .arg_fields
            .iter()
            .map(|f| f.data_type().clone())
            .collect();
        Ok(Arc::new(Field::new(
            self.name(),
            self.return_type(&types)?,
            false,
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let histograms = array(&args.args[0], args.number_rows)?;
        let x = float_arg(self.name(), &args.args[2])?.unwrap_or(f64::NAN);
        let operation = string_arg(self.name(), &args.args[1])?;
        let by = Map::bind(&operation, x).ok_or_else(|| {
            DataFusionError::Execution(format!("{}: unknown operation {operation}", self.name()))
        })?;
        Ok(ColumnarValue::Array(Arc::new(apply_map(
            histograms.as_list::<i32>(),
            by,
        ))))
    }
}

/// Every histogram of every row through `by`, at its own timestamp.
pub fn apply_map(histograms: &ListArray, by: Map) -> ListArray {
    let columns = HistogramColumns::new(histograms.values().as_struct());
    let mut out = HistogramsBuilder::default();
    for r in 0..histograms.len() {
        for i in row(histograms, r) {
            out.push(columns.timestamps[i], &by.apply(columns.decode(i)));
        }
        out.finish_row();
    }
    out.take_all()
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct MergeFloats {
    signature: Signature,
}

impl Default for MergeFloats {
    fn default() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for MergeFloats {
    fn name(&self) -> &str {
        "promql_merge_floats"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        expect_floats(self.name(), arg_types.first())?;
        expect_floats(self.name(), arg_types.get(1))?;
        Ok(series::floats_type())
    }

    /// A row is a series and a series has a list, possibly empty: never
    /// NULL, which the canonical shape has no place for.
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let types: Vec<DataType> = args
            .arg_fields
            .iter()
            .map(|f| f.data_type().clone())
            .collect();
        Ok(Arc::new(Field::new(
            self.name(),
            self.return_type(&types)?,
            false,
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let a = array(&args.args[0], args.number_rows)?;
        let b = array(&args.args[1], args.number_rows)?;
        Ok(ColumnarValue::Array(Arc::new(apply_merge(
            a.as_list::<i32>(),
            b.as_list::<i32>(),
        ))))
    }
}

/// Row by row, the samples of `a` and `b` in timestamp order. Where both
/// hold a timestamp `a`'s sample is kept, which the planner's inputs
/// never do: one is computed from histograms, the other is the floats.
pub fn apply_merge(a: &ListArray, b: &ListArray) -> ListArray {
    let (ta, va) = series::sample_slices(a.values().as_struct());
    let (tb, vb) = series::sample_slices(b.values().as_struct());
    let mut out = SamplesBuilder::default();
    for r in 0..a.len() {
        let (mut i, mut j) = (row(a, r), row(b, r));
        loop {
            match (i.start < i.end, j.start < j.end) {
                (false, false) => break,
                (true, true) if ta[i.start] == tb[j.start] => {
                    out.push(ta[i.start], va[i.start]);
                    i.start += 1;
                    j.start += 1;
                }
                (true, false) => {
                    out.append_samples(&ta[i.clone()], &va[i.clone()]);
                    break;
                }
                (false, true) => {
                    out.append_samples(&tb[j.clone()], &vb[j.clone()]);
                    break;
                }
                (true, true) if ta[i.start] < tb[j.start] => {
                    out.push(ta[i.start], va[i.start]);
                    i.start += 1;
                }
                (true, true) => {
                    out.push(tb[j.start], vb[j.start]);
                    j.start += 1;
                }
            }
        }
        out.finish_row();
    }
    out.take_all()
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct SamplesWithout {
    signature: Signature,
}

impl Default for SamplesWithout {
    fn default() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SamplesWithout {
    fn name(&self) -> &str {
        "promql_samples_without"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        for t in arg_types.iter().take(2) {
            if !series::is_floats_type(t) && !histogram::is_histograms_type(t) {
                return plan_err!("{}: expected a samples list, got {t}", self.name());
            }
        }
        Ok(arg_types[0].clone())
    }

    /// A row is a series and a series has a list, possibly empty: never
    /// NULL, which the canonical shape has no place for.
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let types: Vec<DataType> = args
            .arg_fields
            .iter()
            .map(|f| f.data_type().clone())
            .collect();
        Ok(Arc::new(Field::new(
            self.name(),
            self.return_type(&types)?,
            false,
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let samples = array(&args.args[0], args.number_rows)?;
        let other = array(&args.args[1], args.number_rows)?;
        Ok(ColumnarValue::Array(Arc::new(apply_without(
            samples.as_list::<i32>(),
            other.as_list::<i32>(),
        )?)))
    }
}

/// Row by row, the entries of `samples` at timestamps `other` lacks.
pub fn apply_without(samples: &ListArray, other: &ListArray) -> Result<ListArray> {
    // Nothing to give way to: the usual case for a selection that could
    // hold both types and holds one, and then no sample is copied.
    if other.value_offsets()[0] == other.value_offsets()[other.len()] {
        return Ok(samples.clone());
    }
    let (ts, to) = (timestamps(samples), timestamps(other));
    let first = samples.value_offsets()[0] as usize;
    let last = samples.value_offsets()[samples.len()] as usize;
    let mut keep = BooleanBufferBuilder::new(last - first);
    let mut lengths = Vec::with_capacity(samples.len());
    for r in 0..samples.len() {
        let mut j = row(other, r);
        let mut kept = 0usize;
        for i in row(samples, r) {
            while j.start < j.end && to[j.start] < ts[i] {
                j.start += 1;
            }
            let held = j.start < j.end && to[j.start] == ts[i];
            keep.append(!held);
            kept += usize::from(!held);
        }
        lengths.push(kept);
    }
    let entries = samples.values().slice(first, last - first);
    let entries = filter(&entries, &BooleanArray::new(keep.finish(), None))?;
    let DataType::List(item) = samples.data_type() else {
        unreachable!("a samples list")
    };
    Ok(ListArray::new(
        Arc::clone(item),
        OffsetBuffer::from_lengths(lengths),
        entries,
        None,
    ))
}

#[cfg(test)]
mod tests {
    use promql_histogram::Span;

    use super::*;

    fn h(count: f64) -> FloatHistogram {
        FloatHistogram {
            count,
            sum: count * 10.0,
            positive_spans: vec![Span {
                offset: 0,
                length: 2,
            }],
            positive_buckets: vec![count / 2.0, count / 2.0],
            ..FloatHistogram::default()
        }
    }

    fn histograms(rows: &[&[(i64, FloatHistogram)]]) -> ListArray {
        let mut builder = HistogramsBuilder::default();
        for row in rows {
            for (t, h) in *row {
                builder.push(*t, h);
            }
            builder.finish_row();
        }
        builder.take_all()
    }

    fn floats(rows: &[&[(i64, f64)]]) -> ListArray {
        let mut builder = SamplesBuilder::default();
        for row in rows {
            for (t, v) in *row {
                builder.push(*t, *v);
            }
            builder.finish_row();
        }
        builder.take_all()
    }

    fn rows(list: &ListArray) -> Vec<Vec<(i64, f64)>> {
        (0..list.len())
            .map(|r| {
                let row = list.value(r);
                let (ts, vs) = series::sample_slices(row.as_struct());
                ts.iter().copied().zip(vs.iter().copied()).collect()
            })
            .collect()
    }

    #[test]
    fn count_sum_and_time_are_read_off_each_histogram() {
        let input = histograms(&[&[(1000, h(4.0)), (2000, h(6.0))], &[], &[(3000, h(2.0))]]);
        assert_eq!(
            rows(&apply_to_floats(&input, ToFloat::Count)),
            vec![vec![(1000, 4.0), (2000, 6.0)], vec![], vec![(3000, 2.0)]]
        );
        assert_eq!(
            rows(&apply_to_floats(&input, ToFloat::Sum))[0],
            vec![(1000, 40.0), (2000, 60.0)]
        );
        assert_eq!(
            rows(&apply_to_floats(&input, ToFloat::Avg))[2],
            vec![(3000, 10.0)]
        );
        assert_eq!(
            rows(&apply_to_floats(&input, ToFloat::Timestamp))[0],
            vec![(1000, 1.0), (2000, 2.0)]
        );
        assert_eq!(
            rows(&apply_to_floats(&input, ToFloat::Constant(1.0)))[2],
            vec![(3000, 1.0)]
        );
        // A slice reads its own rows, not the backing array's first ones.
        let tail = input.slice(2, 1);
        assert_eq!(
            rows(&apply_to_floats(&tail, ToFloat::Count)),
            vec![vec![(3000, 2.0)]]
        );
    }

    #[test]
    fn quantile_and_fraction_walk_the_buckets() {
        let input = histograms(&[&[(0, h(4.0))]]);
        let want_q = statistics::quantile(0.5, &h(4.0)).value;
        let want_f = statistics::fraction(0.0, 1.0, &h(4.0)).value;
        assert_eq!(
            rows(&apply_to_floats(&input, ToFloat::Quantile(0.5))),
            vec![vec![(0, want_q)]]
        );
        assert_eq!(
            rows(&apply_to_floats(&input, ToFloat::Fraction(0.0, 1.0))),
            vec![vec![(0, want_f)]]
        );
    }

    #[test]
    fn scaling_multiplies_every_count() {
        let input = histograms(&[&[(5, h(4.0))]]);
        let doubled = apply_map(&input, Map::Mul(2.0));
        let decoded = histogram::decode_row(doubled.value(0).as_struct());
        assert_eq!(decoded[0].0, 5);
        assert!(decoded[0].1.equals(&h(8.0)));
        let halved = apply_map(&input, Map::Div(2.0));
        assert!(histogram::decode_row(halved.value(0).as_struct())[0]
            .1
            .equals(&h(2.0)));
    }

    #[test]
    fn merging_interleaves_by_timestamp() {
        let a = floats(&[&[(1, 1.0), (4, 4.0)], &[], &[(7, 7.0)]]);
        let b = floats(&[&[(2, 2.0), (3, 3.0), (5, 5.0)], &[(6, 6.0)], &[]]);
        assert_eq!(
            rows(&apply_merge(&a, &b)),
            vec![
                vec![(1, 1.0), (2, 2.0), (3, 3.0), (4, 4.0), (5, 5.0)],
                vec![(6, 6.0)],
                vec![(7, 7.0)]
            ]
        );
    }

    #[test]
    fn without_drops_the_shared_timestamps_of_either_list_type() {
        let f = floats(&[&[(1, 1.0), (2, 2.0), (3, 3.0)], &[(4, 4.0)]]);
        let hs = histograms(&[&[(2, h(2.0))], &[(4, h(4.0)), (5, h(5.0))]]);
        assert_eq!(
            rows(&apply_without(&f, &hs).unwrap()),
            vec![vec![(1, 1.0), (3, 3.0)], vec![]]
        );
        let kept = apply_without(&hs, &f).unwrap();
        assert_eq!(kept.value_length(0), 0);
        // With nothing on the other side the rows come back as they were.
        let nothing = floats(&[&[], &[]]);
        assert_eq!(rows(&apply_without(&f, &nothing).unwrap()), rows(&f));
        let second = histogram::decode_row(kept.value(1).as_struct());
        assert_eq!(second.len(), 1);
        assert_eq!(second[0].0, 5);
    }
}
