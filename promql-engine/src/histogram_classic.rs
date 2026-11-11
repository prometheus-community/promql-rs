//! `histogram_quantile` and `histogram_fraction` over a vector that can
//! hold classic histograms: `promql_classic_histogram(floats, histograms,
//! le, 'quantile', 0.9) … GROUP BY <every label but le>`.
//!
//! A classic histogram is float series told apart by `le`, one per
//! cumulative bucket. Upstream collects them per step into
//! `signatureToMetricWithBuckets`, keyed by the label set without `le`,
//! and reads each group's buckets with `BucketQuantile` or
//! `BucketFraction` (`resetHistograms`, `funcHistogramQuantile` and
//! `funcHistogramFraction` in `promql/engine.go` and `functions.go` at
//! 83962c35). That key is this aggregation's grouping, and the fold is
//! [`promql_histogram::statistics`].
//!
//! A series whose `le` is not a float is not a bucket and is passed
//! over; upstream says so in an annotation. A group answers at every
//! step it has a bucket at, NaN included: fewer than two buckets or no
//! `+Inf` bucket is NaN, not absence.
//!
//! A native histogram of the vector lands in the group its label set
//! names, since it has no `le` to set it apart, and is answered from
//! here too: its own quantile where the group has no bucket at that
//! step, and nothing at all where it has, for either of them. That is
//! upstream's rule for a label set that is a native and a classic
//! histogram at once, and seeing both in one group is the only way to
//! apply it. A vector that cannot hold a classic bucket never comes
//! here; the planner reads its histograms row by row instead.

use std::any::Any;
use std::collections::BTreeMap;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, ListArray};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{plan_err, ScalarValue};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::utils::format_state_name;
use datafusion::logical_expr::{
    lit, Accumulator, AggregateUDF, AggregateUDFImpl, Expr, Signature, Volatility,
};
use datafusion::physical_expr::expressions::Literal;
use promql_histogram::statistics::{self, ClassicBucket};
use promql_histogram::FloatHistogram;

use crate::histogram::{self, HistogramColumns, HistogramsBuilder};
use crate::series::{self, SamplesBuilder};

pub const NAME: &str = "promql_classic_histogram";

/// The label that tells a classic histogram's buckets apart, upstream's
/// `model.BucketLabel`.
pub const BUCKET_LABEL: &str = "le";

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Func {
    Quantile(f64),
    Fraction(f64, f64),
}

impl Func {
    fn parts(self) -> (&'static str, f64, Option<f64>) {
        match self {
            Func::Quantile(q) => ("quantile", q, None),
            Func::Fraction(lower, upper) => ("fraction", lower, Some(upper)),
        }
    }

    fn classic(self, buckets: &mut [ClassicBucket]) -> f64 {
        match self {
            Func::Quantile(q) => statistics::classic_quantile(q, buckets).value,
            Func::Fraction(lower, upper) => statistics::classic_fraction(lower, upper, buckets),
        }
    }

    fn native(self, h: &FloatHistogram) -> f64 {
        match self {
            Func::Quantile(q) => statistics::quantile(q, h).value,
            Func::Fraction(lower, upper) => statistics::fraction(lower, upper, h).value,
        }
    }
}

pub fn udaf() -> AggregateUDF {
    AggregateUDF::new_from_impl(Classic::default())
}

/// `promql_classic_histogram(floats, histograms, le, '<func>', a, b)`.
pub fn call(floats: Expr, histograms: Expr, le: Expr, func: Func) -> Expr {
    let (name, a, b) = func.parts();
    udaf().call(vec![
        floats,
        histograms,
        le,
        lit(name),
        lit(a),
        lit(ScalarValue::Float64(b)),
    ])
}

/// What one group holds at one step.
#[derive(Debug, Default)]
struct Step {
    /// In arrival order. The fold sorts them, so the order series arrive
    /// in does not matter.
    buckets: Vec<ClassicBucket>,
    /// The first native histogram of the group here. A second would be
    /// two series with one label set but for `le`, which the answer has
    /// no room for; it is passed over.
    native: Option<FloatHistogram>,
}

/// One group: every series with one label set but for `le`.
#[derive(Debug)]
struct Buckets {
    func: Func,
    steps: BTreeMap<i64, Step>,
}

impl Buckets {
    fn add(&mut self, t: i64, upper_bound: f64, count: f64) {
        self.steps
            .entry(t)
            .or_default()
            .buckets
            .push(ClassicBucket { upper_bound, count });
    }

    fn add_native(&mut self, t: i64, h: FloatHistogram) {
        self.steps.entry(t).or_default().native.get_or_insert(h);
    }
}

fn list<'a>(values: &'a [ArrayRef], index: usize, what: &str) -> Result<&'a ListArray> {
    values
        .get(index)
        .and_then(|v| v.as_list_opt::<i32>())
        .ok_or_else(|| DataFusionError::Internal(format!("{NAME}: {what} is not a samples list")))
}

impl Accumulator for Buckets {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let floats = list(values, 0, "the first argument")?;
        let histograms = list(values, 1, "the second argument")?;
        let columns = HistogramColumns::new(histograms.values().as_struct());
        let offsets = histograms.value_offsets();
        for row in 0..histograms.len() {
            for i in offsets[row] as usize..offsets[row + 1] as usize {
                self.add_native(columns.timestamps[i], columns.decode(i));
            }
        }
        let le = values.get(2).ok_or_else(|| {
            DataFusionError::Internal(format!("{NAME}: no bucket label argument"))
        })?;
        let bound = |row: usize| -> Option<f64> {
            let text = match le.data_type() {
                DataType::Utf8View => le.as_string_view().value(row),
                DataType::Utf8 => le.as_string::<i32>().value(row),
                _ => return None,
            };
            // Go's `strconv.ParseFloat`, which Rust's parser agrees with on
            // decimals, exponents and the spellings of infinity.
            text.parse::<f64>().ok()
        };
        let (ts, vs) = series::sample_slices(floats.values().as_struct());
        let offsets = floats.value_offsets();
        for row in 0..floats.len() {
            let Some(upper_bound) = bound(row) else {
                continue;
            };
            for i in offsets[row] as usize..offsets[row + 1] as usize {
                self.add(ts[i], upper_bound, vs[i]);
            }
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let mut out = SamplesBuilder::default();
        for (t, mut step) in std::mem::take(&mut self.steps) {
            match (&step.native, step.buckets.is_empty()) {
                (None, _) => out.push(t, self.func.classic(&mut step.buckets)),
                (Some(h), true) => out.push(t, self.func.native(h)),
                // "At this data point, we have classic histogram buckets
                // and a native histogram with the same name and labels.
                // Do not evaluate anything."
                (Some(_), false) => {}
            }
        }
        out.finish_row();
        Ok(ScalarValue::List(Arc::new(out.take_all())))
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
            + self
                .steps
                .values()
                .map(|step| {
                    48 + std::mem::size_of::<Step>()
                        + step.buckets.capacity() * std::mem::size_of::<ClassicBucket>()
                        + step.native.as_ref().map_or(0, histogram::heap_size)
                })
                .sum::<usize>()
    }

    /// The buckets as two parallel sample lists, `(t, upper bound)` and
    /// `(t, count)`, and the native histograms as they are: nothing is
    /// folded until the group is whole.
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let mut bounds = SamplesBuilder::default();
        let mut counts = SamplesBuilder::default();
        let mut natives = HistogramsBuilder::default();
        for (t, step) in &self.steps {
            for bucket in &step.buckets {
                bounds.push(*t, bucket.upper_bound);
                counts.push(*t, bucket.count);
            }
            if let Some(h) = &step.native {
                natives.push(*t, h);
            }
        }
        bounds.finish_row();
        counts.finish_row();
        natives.finish_row();
        Ok(vec![
            ScalarValue::List(Arc::new(bounds.take_all())),
            ScalarValue::List(Arc::new(counts.take_all())),
            ScalarValue::List(Arc::new(natives.take_all())),
        ])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let bounds = list(states, 0, "the bounds state")?;
        let counts = list(states, 1, "the counts state")?;
        let (ts, upper_bounds) = series::sample_slices(bounds.values().as_struct());
        let (_, cumulative) = series::sample_slices(counts.values().as_struct());
        if bounds.value_offsets() != counts.value_offsets() {
            return Err(DataFusionError::Internal(format!(
                "{NAME}: partial state bounds and counts differ in shape"
            )));
        }
        let (first, last) = (
            bounds.value_offsets()[0] as usize,
            bounds.value_offsets()[bounds.len()] as usize,
        );
        for i in first..last {
            self.add(ts[i], upper_bounds[i], cumulative[i]);
        }
        let natives = list(states, 2, "the natives state")?;
        let columns = HistogramColumns::new(natives.values().as_struct());
        let (first, last) = (
            natives.value_offsets()[0] as usize,
            natives.value_offsets()[natives.len()] as usize,
        );
        for i in first..last {
            self.add_native(columns.timestamps[i], columns.decode(i));
        }
        Ok(())
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct Classic {
    signature: Signature,
}

impl Default for Classic {
    fn default() -> Self {
        Self {
            signature: Signature::any(6, Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for Classic {
    fn name(&self) -> &str {
        NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if !arg_types.first().is_some_and(series::is_floats_type) {
            return plan_err!(
                "{NAME}: first argument must be {}, got {:?}",
                series::floats_type(),
                arg_types.first()
            );
        }
        Ok(series::floats_type())
    }

    fn is_nullable(&self) -> bool {
        false
    }

    fn accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        if args.is_distinct {
            return plan_err!("{NAME}: DISTINCT is not supported");
        }
        let literal = |i: usize| {
            args.exprs
                .get(i)
                .and_then(|e| (e.as_ref() as &dyn Any).downcast_ref::<Literal>())
                .map(Literal::value)
        };
        let number = |i: usize| match literal(i) {
            Some(ScalarValue::Float64(v)) => *v,
            _ => None,
        };
        let func = match (literal(3), number(4), number(5)) {
            (Some(ScalarValue::Utf8(Some(name))), Some(a), b) => match (name.as_str(), b) {
                ("quantile", _) => Some(Func::Quantile(a)),
                ("fraction", Some(b)) => Some(Func::Fraction(a, b)),
                _ => None,
            },
            _ => None,
        }
        .ok_or_else(|| {
            DataFusionError::Plan(format!(
                "{NAME}: expected 'quantile' with q, or 'fraction' with its bounds, as literals"
            ))
        })?;
        Ok(Box::new(Buckets {
            func,
            steps: BTreeMap::new(),
        }))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        let field = |name: &str| -> FieldRef {
            Arc::new(Field::new(
                format_state_name(args.name, name),
                series::floats_type(),
                false,
            ))
        };
        Ok(vec![
            field("bounds"),
            field("counts"),
            Arc::new(Field::new(
                format_state_name(args.name, "natives"),
                histogram::histograms_type(),
                false,
            )),
        ])
    }

    fn default_value(&self, _data_type: &DataType) -> Result<ScalarValue> {
        let mut out = SamplesBuilder::default();
        out.finish_row();
        Ok(ScalarValue::List(Arc::new(out.take_all())))
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::StringViewArray;

    use super::*;

    fn column(rows: &[&[(i64, f64)]]) -> ArrayRef {
        let mut builder = SamplesBuilder::default();
        for row in rows {
            for (t, v) in *row {
                builder.push(*t, *v);
            }
            builder.finish_row();
        }
        Arc::new(builder.take_all())
    }

    fn le(values: &[&str]) -> ArrayRef {
        Arc::new(StringViewArray::from(values.to_vec()))
    }

    fn natives(rows: &[&[(i64, FloatHistogram)]]) -> ArrayRef {
        let mut builder = HistogramsBuilder::default();
        for row in rows {
            for (t, h) in *row {
                builder.push(*t, h);
            }
            builder.finish_row();
        }
        Arc::new(builder.take_all())
    }

    /// As many rows without a native histogram.
    fn none(rows: usize) -> ArrayRef {
        natives(&vec![&[][..]; rows])
    }

    fn native(count: f64) -> FloatHistogram {
        FloatHistogram {
            count,
            sum: count,
            positive_spans: vec![promql_histogram::Span {
                offset: 0,
                length: 1,
            }],
            positive_buckets: vec![count],
            ..FloatHistogram::default()
        }
    }

    fn samples(value: ScalarValue) -> Vec<(i64, f64)> {
        let ScalarValue::List(list) = value else {
            panic!("expected a list");
        };
        let row = list.value(0);
        let (ts, vs) = series::sample_slices(row.as_struct());
        ts.iter().copied().zip(vs.iter().copied()).collect()
    }

    fn buckets(func: Func) -> Buckets {
        Buckets {
            func,
            steps: BTreeMap::new(),
        }
    }

    #[test]
    fn a_quantile_is_read_off_the_buckets_at_each_step() {
        let mut acc = buckets(Func::Quantile(0.5));
        // Arrival order is not bucket order; `bogus` is no bucket at all.
        acc.update_batch(&[
            column(&[
                &[(0, 10.0), (60, 20.0)],
                &[(0, 5.0), (60, 10.0)],
                &[(0, 99.0)],
                &[(0, 10.0), (60, 20.0)],
            ]),
            none(4),
            le(&["2", "1", "bogus", "+Inf"]),
        ])
        .unwrap();
        // Half of ten observations is the fifth, the top of the first bucket.
        assert_eq!(samples(acc.evaluate().unwrap()), vec![(0, 1.0), (60, 1.0)]);
    }

    #[test]
    fn a_group_without_an_infinite_bucket_is_nan_not_absent() {
        let mut acc = buckets(Func::Quantile(0.5));
        acc.update_batch(&[
            column(&[&[(0, 5.0)], &[(0, 10.0)]]),
            none(2),
            le(&["1", "2"]),
        ])
        .unwrap();
        let out = samples(acc.evaluate().unwrap());
        assert_eq!(out.len(), 1);
        assert!(out[0].1.is_nan());
    }

    #[test]
    fn a_fraction_is_read_off_the_buckets() {
        let mut acc = buckets(Func::Fraction(0.0, 1.0));
        acc.update_batch(&[
            column(&[&[(0, 5.0)], &[(0, 10.0)], &[(0, 10.0)]]),
            none(3),
            le(&["1", "2", "+Inf"]),
        ])
        .unwrap();
        assert_eq!(samples(acc.evaluate().unwrap()), vec![(0, 0.5)]);
    }

    #[test]
    fn a_native_histogram_answers_alone_and_not_beside_classic_buckets() {
        let mut acc = buckets(Func::Quantile(0.5));
        // The group's native series has no `le`. It shares step 0 with the
        // buckets and has step 60 to itself.
        acc.update_batch(&[
            column(&[&[(0, 5.0)], &[(0, 10.0)], &[]]),
            natives(&[&[], &[], &[(0, native(4.0)), (60, native(4.0))]]),
            le(&["1", "+Inf", ""]),
        ])
        .unwrap();
        let want = statistics::quantile(0.5, &native(4.0)).value;
        assert_eq!(samples(acc.evaluate().unwrap()), vec![(60, want)]);
    }

    #[test]
    fn partial_states_merge_to_the_same_answer() {
        let rows: [&[(i64, f64)]; 3] = [
            &[(0, 5.0), (60, 6.0)],
            &[(0, 10.0)],
            &[(0, 10.0), (60, 6.0)],
        ];
        let bounds = ["1", "2", "+Inf"];
        let mut whole = buckets(Func::Quantile(0.9));
        let histograms: [&[(i64, FloatHistogram)]; 3] = [&[(120, native(2.0))], &[], &[]];
        whole
            .update_batch(&[column(&rows), natives(&histograms), le(&bounds)])
            .unwrap();
        let want = samples(whole.evaluate().unwrap());

        let mut merged = buckets(Func::Quantile(0.9));
        for i in 0..rows.len() {
            let mut partial = buckets(Func::Quantile(0.9));
            partial
                .update_batch(&[
                    column(&rows[i..=i]),
                    natives(&histograms[i..=i]),
                    le(&bounds[i..=i]),
                ])
                .unwrap();
            let state: Vec<ArrayRef> = partial
                .state()
                .unwrap()
                .into_iter()
                .map(|s| s.to_array().unwrap())
                .collect();
            merged.merge_batch(&state).unwrap();
        }
        assert_eq!(want.len(), 3);
        assert_eq!(samples(merged.evaluate().unwrap()), want);
    }
}
