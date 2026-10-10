//! `scalar(v)`: the value of the one series an instant vector holds, per
//! step, and NaN at every step where it holds none or several.
//!
//! Ports `funcScalar` (`promql/functions.go` at 83962c35). Upstream
//! returns `Sample{F: NaN}` for a vector of any other size than one, an
//! empty one included, so unlike every other operator here the answer
//! has a sample at every step of the grid.
//!
//! An aggregate planned with no group key, as [`crate::absent`] is and
//! for the same reason: the answer must exist when no row ever arrived,
//! and a block no series reached is never sent. It folds into a row per
//! step of which series reached it and what they said, nothing per
//! series.
//!
//! A step reached by one series holds that series' value; reached by a
//! second it holds NaN, which no later series changes. The partial state
//! is that row read as a samples list: a step that is absent was reached
//! by none, and a NaN may be one series' own NaN or the sign of several,
//! which `funcScalar` answers alike. So merging two partitions' states is
//! the same walk as folding input, and the state needs no count.

use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, Float64Array, ListArray, StructArray, TimestampMillisecondArray,
};
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{plan_err, ScalarValue};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::utils::format_state_name;
use datafusion::logical_expr::{
    lit, Accumulator, AggregateUDF, AggregateUDFImpl, Signature, Volatility,
};
use datafusion::physical_expr::expressions::Literal;

use crate::aggregate::Grid;
use crate::series;

pub const NAME: &str = "promql_scalar";

/// What the series that reached each step said, `None` for a step none
/// reached.
#[derive(Debug)]
struct Steps {
    grid: Grid,
    said: Vec<Option<f64>>,
}

impl Steps {
    fn new(grid: Grid) -> Self {
        Self {
            said: vec![None; grid.len()],
            grid,
        }
    }

    /// Folds in every sample of every row of `list`, a series or a
    /// partition's state alike.
    fn fold(&mut self, list: &ListArray) -> Result<()> {
        let (ts, vs) = series::sample_slices(list.values().as_struct());
        let offsets = list.offsets();
        let grid = self.grid;
        for row in 0..list.len() {
            if list.is_null(row) {
                continue;
            }
            let (lo, hi) = (offsets[row] as usize, offsets[row + 1] as usize);
            grid.runs(&ts[lo..hi], |index, from, len| {
                for k in 0..len {
                    let slot = &mut self.said[index + k];
                    // A second series at a step: more than one float.
                    *slot = Some(match slot {
                        None => vs[lo + from + k],
                        Some(_) => f64::NAN,
                    });
                }
            })?;
        }
        Ok(())
    }

    /// One samples list holding the steps `keep` says yes to, each with
    /// `value` of what was said there.
    fn list(&self, value: impl Fn(Option<f64>) -> Option<f64>) -> ScalarValue {
        let (ts, vs): (Vec<i64>, Vec<f64>) = self
            .said
            .iter()
            .enumerate()
            .filter_map(|(i, said)| value(*said).map(|v| (self.grid.timestamp(i), v)))
            .unzip();
        let n = ts.len();
        let entries = StructArray::new(
            series::sample_fields(),
            vec![
                Arc::new(TimestampMillisecondArray::from(ts)),
                Arc::new(Float64Array::from(vs)),
            ],
            None,
        );
        ScalarValue::List(Arc::new(ListArray::new(
            series::sample_item(),
            OffsetBuffer::new(vec![0, n as i32].into()),
            Arc::new(entries),
            None,
        )))
    }
}

impl Accumulator for Steps {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.fold(values[0].as_list::<i32>())
    }

    /// `funcScalar`'s NaN for every step the vector did not hold exactly
    /// one float at.
    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(self.list(|said| Some(said.unwrap_or(f64::NAN))))
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>() + self.said.capacity() * std::mem::size_of::<Option<f64>>()
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.list(|said| said)])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.fold(states[0].as_list::<i32>())
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct Scalar {
    signature: Signature,
}

impl Default for Scalar {
    fn default() -> Self {
        Self {
            signature: Signature::exact(
                vec![
                    series::samples_type(),
                    DataType::Int64,
                    DataType::Int64,
                    DataType::Int64,
                ],
                Volatility::Immutable,
            ),
        }
    }
}

pub fn udaf() -> AggregateUDF {
    AggregateUDF::new_from_impl(Scalar::default())
}

/// `promql_scalar(samples, start, end, step)`. The grid is an argument
/// for the reason [`crate::absent::call`] gives.
pub fn call(
    samples: datafusion::logical_expr::Expr,
    start_ms: i64,
    end_ms: i64,
    step_ms: i64,
) -> datafusion::logical_expr::Expr {
    udaf().call(vec![samples, lit(start_ms), lit(end_ms), lit(step_ms)])
}

fn grid_of(args: &AccumulatorArgs) -> Result<Grid> {
    let grid = |i: usize, what: &str| {
        args.exprs
            .get(i)
            .and_then(|e| (e.as_ref() as &dyn Any).downcast_ref::<Literal>())
            .and_then(|l| match l.value() {
                ScalarValue::Int64(Some(n)) => Some(*n),
                _ => None,
            })
            .ok_or_else(|| {
                DataFusionError::Plan(format!("{NAME}: {what} must be an Int64 literal"))
            })
    };
    Grid::new(NAME, grid(1, "start")?, grid(2, "end")?, grid(3, "step")?)
}

impl AggregateUDFImpl for Scalar {
    fn name(&self) -> &str {
        NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.first() != Some(&series::samples_type()) {
            return plan_err!(
                "{NAME}: first argument must be {}, got {:?}",
                series::samples_type(),
                arg_types.first()
            );
        }
        Ok(series::samples_type())
    }

    fn is_nullable(&self) -> bool {
        false
    }

    fn accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(Steps::new(grid_of(&args)?)))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![Arc::new(Field::new(
            format_state_name(args.name, "said"),
            series::samples_type(),
            false,
        ))])
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::RecordBatch;
    use datafusion::arrow::datatypes::Schema;
    use datafusion::datasource::MemTable;
    use datafusion::logical_expr::col;
    use datafusion::prelude::{SessionConfig, SessionContext};

    use super::*;
    use crate::series::SAMPLES;

    const GRID: (i64, i64, i64) = (0, 60_000, 30_000);

    fn steps() -> Steps {
        Steps::new(Grid::new(NAME, GRID.0, GRID.1, GRID.2).unwrap())
    }

    fn samples(rows: &[&[(i64, f64)]]) -> ListArray {
        let mut ts = Vec::new();
        let mut vs = Vec::new();
        let mut lengths = Vec::new();
        for row in rows {
            lengths.push(row.len());
            for (t, v) in *row {
                ts.push(*t);
                vs.push(*v);
            }
        }
        let entries = StructArray::new(
            series::sample_fields(),
            vec![
                Arc::new(TimestampMillisecondArray::from(ts)),
                Arc::new(Float64Array::from(vs)),
            ],
            None,
        );
        ListArray::new(
            series::sample_item(),
            OffsetBuffer::from_lengths(lengths),
            Arc::new(entries),
            None,
        )
    }

    fn read(value: &ScalarValue) -> Vec<(i64, f64)> {
        let ScalarValue::List(list) = value else {
            panic!("not a list")
        };
        let (ts, vs) = series::sample_slices(list.values().as_struct());
        ts.iter().copied().zip(vs.iter().copied()).collect()
    }

    /// `funcScalar` with no float, one, and more than one.
    #[test]
    fn a_step_is_the_one_value_or_nan() {
        let mut acc = steps();
        // Step 0: one series. Step 1: two. Step 2: none.
        acc.fold(&samples(&[&[(0, 5.0), (30_000, 1.0)], &[(30_000, 2.0)]]))
            .unwrap();
        let got = read(&acc.evaluate().unwrap());
        assert_eq!(got.len(), 3);
        assert_eq!(got[0], (0, 5.0));
        assert!(got[1].1.is_nan() && got[1].0 == 30_000);
        assert!(got[2].1.is_nan() && got[2].0 == 60_000);
    }

    /// Two partitions each saw one series at a step: together that is two.
    #[test]
    fn partial_states_merge_to_the_count_of_both() {
        let (mut a, mut b, mut whole) = (steps(), steps(), steps());
        a.fold(&samples(&[&[(0, 5.0), (30_000, 1.0)]])).unwrap();
        b.fold(&samples(&[&[(30_000, 2.0)]])).unwrap();
        whole
            .fold(&samples(&[&[(0, 5.0), (30_000, 1.0)], &[(30_000, 2.0)]]))
            .unwrap();

        let mut merged = steps();
        for part in [&mut a, &mut b] {
            let ScalarValue::List(state) = part.state().unwrap().remove(0) else {
                panic!("not a list")
            };
            merged.merge_batch(&[state as ArrayRef]).unwrap();
        }
        let (got, want) = (
            read(&merged.evaluate().unwrap()),
            read(&whole.evaluate().unwrap()),
        );
        assert_eq!(got.len(), want.len());
        for (g, w) in got.iter().zip(&want) {
            assert_eq!(g.0, w.0);
            assert!(g.1 == w.1 || (g.1.is_nan() && w.1.is_nan()), "{g:?} {w:?}");
        }
    }

    /// Without a single input row the answer is still the whole grid of
    /// NaN, which is what a no-grouping `AggregateExec` is for.
    #[tokio::test]
    async fn no_rows_at_all_is_nan_at_every_step() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            SAMPLES,
            series::samples_type(),
            false,
        )]));
        let table =
            MemTable::try_new(schema.clone(), vec![vec![RecordBatch::new_empty(schema)]]).unwrap();
        let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
        ctx.register_udaf(udaf());
        ctx.register_table("t", Arc::new(table)).unwrap();
        let df = ctx
            .table("t")
            .await
            .unwrap()
            .aggregate(
                Vec::<datafusion::logical_expr::Expr>::new(),
                vec![call(col(SAMPLES), GRID.0, GRID.1, GRID.2).alias(SAMPLES)],
            )
            .unwrap();
        let batches = df.collect().await.unwrap();
        assert_eq!(batches.len(), 1);
        let list = batches[0].column(0).as_list::<i32>();
        let row = list.value(0);
        let (ts, vs) = series::sample_slices(row.as_struct());
        assert_eq!(ts, [0, 30_000, 60_000]);
        assert!(vs.iter().all(|v| v.is_nan()));
    }
}
