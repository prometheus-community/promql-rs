//! `sum` and `avg` over native histograms:
//! `promql_histogram_aggregate(histograms, 'sum') … GROUP BY <labels>`.
//!
//! Ports the histogram half of `aggregation` (`promql/engine.go` at
//! 83962c35): a Kahan sum of histograms per group and step, divided by
//! the count for `avg`. It runs beside [`crate::aggregate`]'s float sum
//! in one grouping, each over its own column. Where a group holds both
//! types at a step upstream answers nothing, which the planner states
//! above the grouping with [`crate::histogram_fn::without`]; this
//! function never sees a float.
//!
//! A group whose histograms cannot be added, an exponential one and one
//! with custom buckets, answers nothing at that step
//! (`incompatibleHistograms`).
//!
//! Unlike the float aggregation this keeps a map per group rather than
//! lanes over the step grid. A histogram is hundreds of bytes of buckets
//! behind a pointer either way, so dense lanes would buy little, and a
//! map needs no grid to be told.

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
use promql_histogram::KahanSum;

use crate::histogram::{self, HistogramColumns, HistogramsBuilder, Mean};
use crate::series::{self, SamplesBuilder};

pub const NAME: &str = "promql_histogram_aggregate";

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum Op {
    #[default]
    Sum,
    Avg,
}

impl Op {
    pub fn as_str(self) -> &'static str {
        match self {
            Op::Sum => "sum",
            Op::Avg => "avg",
        }
    }

    fn parse(s: &str) -> Option<Self> {
        match s {
            "sum" => Some(Op::Sum),
            "avg" => Some(Op::Avg),
            _ => None,
        }
    }
}

pub fn udaf() -> AggregateUDF {
    AggregateUDF::new_from_impl(Aggregate::default())
}

/// `promql_histogram_aggregate(histograms, '<op>')`.
pub fn call(histograms: Expr, op: Op) -> Expr {
    udaf().call(vec![histograms, lit(op.as_str())])
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct Aggregate {
    signature: Signature,
}

impl Default for Aggregate {
    fn default() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for Aggregate {
    fn name(&self) -> &str {
        NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if !arg_types.first().is_some_and(histogram::is_histograms_type) {
            return plan_err!(
                "{NAME}: first argument must be {}, got {:?}",
                histogram::histograms_type(),
                arg_types.first()
            );
        }
        Ok(histogram::histograms_type())
    }

    /// Every group yields a list, possibly empty; never NULL.
    fn is_nullable(&self) -> bool {
        false
    }

    fn accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        if args.is_distinct {
            return plan_err!("{NAME}: DISTINCT is not supported");
        }
        let op = args
            .exprs
            .get(1)
            .and_then(|e| (e.as_ref() as &dyn Any).downcast_ref::<Literal>())
            .and_then(|l| match l.value() {
                ScalarValue::Utf8(Some(s)) => Op::parse(s.as_str()),
                _ => None,
            })
            .ok_or_else(|| {
                DataFusionError::Plan(format!("{NAME}: the operator must be 'sum' or 'avg'"))
            })?;
        Ok(Box::new(Steps {
            op,
            steps: BTreeMap::new(),
            bytes: 0,
        }))
    }

    /// The unfinished Kahan sums cross to the final stage as they are, a
    /// sum and a compensation histogram per step, with the count and
    /// whether `avg` has already turned the sum into a running mean:
    /// adding rounded partial sums would not be the sum.
    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        let field = |name: &str, data_type: DataType| -> FieldRef {
            Arc::new(Field::new(
                format_state_name(args.name, name),
                data_type,
                false,
            ))
        };
        Ok(vec![
            field("sums", histogram::histograms_type()),
            field("compensations", histogram::histograms_type()),
            field("counts", series::floats_type()),
            field("incremental", series::floats_type()),
        ])
    }

    fn default_value(&self, _data_type: &DataType) -> Result<ScalarValue> {
        Ok(ScalarValue::List(Arc::new(row_of(
            &mut HistogramsBuilder::default(),
        ))))
    }
}

/// One step of one group: a sum, or for `avg` the same fold as
/// [`Mean`], which is a sum until it would overflow. `None` once two of
/// its histograms could not be added: the step still answers nothing.
#[derive(Debug)]
struct Step(Option<Mean>);

impl Step {
    fn heap_size(&self) -> usize {
        self.0.as_ref().map_or(0, Mean::heap_size)
    }
}

#[derive(Debug, Default)]
struct Steps {
    op: Op,
    steps: BTreeMap<i64, Step>,
    /// Heap of the sums in `steps`, refreshed where one changes.
    bytes: usize,
}

impl Steps {
    /// Fold `part` into step `t`. A failure at a step is final.
    fn add(&mut self, t: i64, part: Option<Mean>) {
        let step = self.steps.remove(&t);
        let before = step.as_ref().map_or(0, Step::heap_size);
        let next = match (step, part) {
            (None, part) => Step(part),
            (Some(Step(None)), _) | (Some(_), None) => Step(None),
            (Some(Step(Some(mut held))), Some(part)) => {
                let folded = match self.op {
                    // A sum is a mean that never divides: its parts add,
                    // however large they grow.
                    Op::Sum => {
                        let (mut sum, count, _) = held.into_parts();
                        let (part, part_count, _) = part.into_parts();
                        let (part, compensation) = part.into_parts();
                        sum.add_parts(&part, &compensation)
                            .map(|_| Mean::from_parts(sum, count + part_count, false))
                    }
                    Op::Avg => held.merge(part).map(|_| held),
                };
                Step(folded.ok())
            }
        };
        self.bytes = self.bytes - before + next.heap_size();
        self.steps.insert(t, next);
    }
}

/// One finished row of a builder as the single-row list an accumulator
/// answers with.
fn row_of(builder: &mut HistogramsBuilder) -> ListArray {
    builder.finish_row();
    builder.take_all()
}

fn lists<'a>(name: &str, array: &'a ArrayRef) -> Result<&'a ListArray> {
    array.as_list_opt::<i32>().ok_or_else(|| {
        DataFusionError::Internal(format!(
            "{NAME}: {name} is not a list, got {}",
            array.data_type()
        ))
    })
}

impl Accumulator for Steps {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let histograms = lists("the histograms argument", &values[0])?;
        let columns = HistogramColumns::new(histograms.values().as_struct());
        let offsets = histograms.value_offsets();
        for row in 0..histograms.len() {
            for i in offsets[row] as usize..offsets[row + 1] as usize {
                self.add(columns.timestamps[i], Some(Mean::new(columns.decode(i))));
            }
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let mut out = HistogramsBuilder::default();
        for (t, step) in std::mem::take(&mut self.steps) {
            let Step(Some(mean)) = step else {
                continue;
            };
            let result = match self.op {
                Op::Sum => mean.into_parts().0.finish(),
                Op::Avg => mean.finish(),
            };
            // Upstream notes this last `Add` cannot fail once every
            // `KahanAdd` before it succeeded; a failure answers nothing
            // regardless.
            if let Ok(h) = result {
                out.push(t, &h);
            }
        }
        self.bytes = 0;
        Ok(ScalarValue::List(Arc::new(row_of(&mut out))))
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.steps.len() * (std::mem::size_of::<(i64, Step)>() + 16)
            + self.bytes
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let mut sums = HistogramsBuilder::default();
        let mut compensations = HistogramsBuilder::default();
        let mut counts = SamplesBuilder::default();
        let mut incremental = SamplesBuilder::default();
        for (t, step) in std::mem::take(&mut self.steps) {
            // A failed step travels as a zero count and no sum: the final
            // stage finds none for it and keeps it failed.
            let Step(Some(mean)) = step else {
                counts.push(t, 0.0);
                incremental.push(t, 0.0);
                continue;
            };
            let (acc, count, is_mean) = mean.into_parts();
            let (sum, compensation) = acc.into_parts();
            sums.push(t, &sum);
            compensations.push(t, &compensation);
            counts.push(t, count);
            incremental.push(t, f64::from(u8::from(is_mean)));
        }
        self.bytes = 0;
        counts.finish_row();
        incremental.finish_row();
        Ok(vec![
            ScalarValue::List(Arc::new(row_of(&mut sums))),
            ScalarValue::List(Arc::new(row_of(&mut compensations))),
            ScalarValue::List(Arc::new(counts.take_all())),
            ScalarValue::List(Arc::new(incremental.take_all())),
        ])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let sums = lists("the sums state", &states[0])?;
        let compensations = lists("the compensations state", &states[1])?;
        let counts = lists("the counts state", &states[2])?;
        let incremental = lists("the incremental state", &states[3])?;
        let sum_columns = HistogramColumns::new(sums.values().as_struct());
        let compensation_columns = HistogramColumns::new(compensations.values().as_struct());
        let (count_ts, count_vs) = series::sample_slices(counts.values().as_struct());
        let (_, incremental_vs) = series::sample_slices(incremental.values().as_struct());
        for row in 0..sums.len() {
            let mut next = sums.value_offsets()[row] as usize;
            let end = sums.value_offsets()[row + 1] as usize;
            // The compensation of a sum sits at the same place in its row,
            // and so does a step's flag beside its count.
            let shift = compensations.value_offsets()[row] as isize - next as isize;
            let flags =
                incremental.value_offsets()[row] as isize - counts.value_offsets()[row] as isize;
            let range =
                counts.value_offsets()[row] as usize..counts.value_offsets()[row + 1] as usize;
            for i in range {
                let t = count_ts[i];
                let part = (next < end && sum_columns.timestamps[next] == t).then(|| {
                    let acc = KahanSum::from_parts(
                        sum_columns.decode(next),
                        compensation_columns.decode((next as isize + shift) as usize),
                    );
                    next += 1;
                    let is_mean = incremental_vs[(i as isize + flags) as usize] != 0.0;
                    Mean::from_parts(acc, count_vs[i], is_mean)
                });
                self.add(t, part);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use promql_histogram::{FloatHistogram, Span, CUSTOM_SCHEMA};

    use super::*;

    fn h(count: f64) -> FloatHistogram {
        FloatHistogram {
            count,
            sum: count * 10.0,
            positive_spans: vec![Span {
                offset: 0,
                length: 1,
            }],
            positive_buckets: vec![count],
            ..FloatHistogram::default()
        }
    }

    fn custom(count: f64) -> FloatHistogram {
        FloatHistogram {
            schema: CUSTOM_SCHEMA,
            custom_values: vec![1.0],
            ..h(count)
        }
    }

    fn column(rows: &[&[(i64, FloatHistogram)]]) -> ArrayRef {
        let mut builder = HistogramsBuilder::default();
        for row in rows {
            for (t, h) in *row {
                builder.push(*t, h);
            }
            builder.finish_row();
        }
        Arc::new(builder.take_all())
    }

    fn steps(op: Op) -> Steps {
        Steps {
            op,
            ..Steps::default()
        }
    }

    fn counts(value: ScalarValue) -> Vec<(i64, f64)> {
        let ScalarValue::List(list) = value else {
            panic!("expected a list");
        };
        histogram::decode_row(list.value(0).as_struct())
            .into_iter()
            .map(|(t, h)| (t, h.count))
            .collect()
    }

    #[test]
    fn each_step_sums_the_histograms_present_at_it() {
        let mut acc = steps(Op::Sum);
        acc.update_batch(&[column(&[
            &[(0, h(1.0)), (60, h(2.0))],
            &[(60, h(5.0)), (120, h(7.0))],
        ])])
        .unwrap();
        assert!(acc.size() > std::mem::size_of::<Steps>());
        assert_eq!(
            counts(acc.evaluate().unwrap()),
            vec![(0, 1.0), (60, 7.0), (120, 7.0)]
        );
    }

    #[test]
    fn avg_divides_by_the_histograms_at_the_step() {
        let mut acc = steps(Op::Avg);
        acc.update_batch(&[column(&[&[(0, h(2.0))], &[(0, h(4.0))], &[(0, h(9.0))]])])
            .unwrap();
        assert_eq!(counts(acc.evaluate().unwrap()), vec![(0, 5.0)]);
    }

    #[test]
    fn incompatible_histograms_answer_nothing_at_their_step() {
        let mut acc = steps(Op::Sum);
        acc.update_batch(&[column(&[
            &[(0, h(1.0)), (60, h(1.0))],
            &[(0, custom(1.0))],
            // A later compatible histogram does not revive the step.
            &[(0, h(1.0)), (60, h(2.0))],
        ])])
        .unwrap();
        assert_eq!(counts(acc.evaluate().unwrap()), vec![(60, 3.0)]);
    }

    #[test]
    fn partial_states_merge_into_the_same_answer() {
        for op in [Op::Sum, Op::Avg] {
            let rows: [&[(i64, FloatHistogram)]; 4] = [
                &[(0, h(1.0)), (60, h(2.0))],
                &[(0, h(3.0))],
                &[(60, h(4.0)), (120, custom(1.0))],
                &[(120, h(1.0))],
            ];
            let mut whole = steps(op);
            whole.update_batch(&[column(&rows)]).unwrap();
            let want = counts(whole.evaluate().unwrap());

            let mut merged = steps(op);
            for part in rows.chunks(2) {
                let mut partial = steps(op);
                partial.update_batch(&[column(part)]).unwrap();
                let state: Vec<ArrayRef> = partial
                    .state()
                    .unwrap()
                    .into_iter()
                    .map(|s| s.to_array().unwrap())
                    .collect();
                merged.merge_batch(&state).unwrap();
            }
            assert_eq!(counts(merged.evaluate().unwrap()), want, "{op:?}");
            // 120 mixed a custom histogram in and stays unanswered.
            assert!(want.iter().all(|(t, _)| *t != 120), "{op:?}");
        }
    }
}
