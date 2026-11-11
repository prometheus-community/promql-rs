//! `absent` and `absent_over_time`: one series, valued 1, over the steps
//! no input series reached.
//!
//! Ports `funcAbsent` and `funcAbsentOverTime` (`promql/functions.go` at
//! 83962c35) together with the `absent_over_time` post-processing in
//! `evalCall` (`promql/engine.go`, "The absent_over_time function returns
//! 0 or 1 series"). The two share one kernel because upstream's
//! `funcAbsentOverTime` returns 1 for every non-empty window, which is
//! `present_over_time`, and the engine then takes per step the decision
//! `funcAbsent` takes: 1 where nothing answered, nothing otherwise.
//!
//! Every other operator here makes output rows from input rows, and this
//! one has to make its row precisely when there are none. It is therefore
//! an aggregate planned with no group key at all, not by block: a block no
//! series reached is never sent, so a block-grouped aggregate would never
//! hear of it, whereas DataFusion's no-grouping `AggregateExec` finalizes
//! exactly one row at end of input whether or not a row ever arrived
//! (`aggregates/no_grouping.rs` in DataFusion 54). The row leaves only
//! when the input ends, which is what absent means: a step is known to be
//! empty once every block has passed. The state is one bit per grid step
//! and nothing per series, and a partial state is itself a samples list,
//! so merging partitions is the same walk as folding input.
//!
//! The output's labels come from the planner ([`labels_for`]), read off
//! the argument expression rather than off any series, because there is
//! no series to read them from.

use std::any::Any;
use std::collections::{BTreeMap, HashSet};
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
use promql_parser::ast::{Expr, MatchOp};

use crate::aggregate::Grid;
use crate::matcher::METRIC_NAME;
use crate::series;

pub const NAME: &str = "promql_absent";

/// Port of `createLabelsForAbsentFunction` (`promql/functions.go` at
/// 83962c35): the output series' labels, from the argument expression
/// alone, sorted by name.
///
/// Only a selector's equality matchers survive, `__name__` excluded, and
/// a name any later matcher touches again is dropped. Upstream's `has`
/// map records the names an equality matcher has set, never a deletion,
/// so a second `job="…"` falls to the delete branch exactly as a regex
/// would, and order decides: `job=~"a",job="a"` keeps `job`. Upstream
/// calls this historic behaviour and keeps it on purpose.
///
/// Parentheses are looked through because upstream's `preprocessExpr`
/// strips them off every call argument (`unwrapParenExpr`) before the
/// function reads it.
pub fn labels_for(arg: &Expr) -> Vec<(String, String)> {
    let mut arg = arg;
    while let Expr::Paren(p) = arg {
        arg = &p.expr;
    }
    let vs = match arg {
        Expr::VectorSelector(vs) => vs,
        Expr::MatrixSelector(ms) => match ms.vector_selector.as_ref() {
            Expr::VectorSelector(vs) => vs,
            _ => return Vec::new(),
        },
        _ => return Vec::new(),
    };
    let mut out = BTreeMap::new();
    let mut has = HashSet::new();
    for m in &vs.label_matchers {
        if m.name == METRIC_NAME {
            continue;
        }
        if m.op == MatchOp::Equal && has.insert(m.name.as_str()) {
            out.insert(m.name.clone(), m.value.clone());
        } else {
            out.remove(&m.name);
        }
    }
    out.into_iter().collect()
}

/// Which steps of the grid some input series answered.
#[derive(Debug)]
struct Steps {
    grid: Grid,
    present: Vec<bool>,
}

impl Steps {
    fn new(grid: Grid) -> Self {
        Self {
            present: vec![false; grid.len()],
            grid,
        }
    }

    /// Marks the step of every sample in every row of `list`.
    fn mark(&mut self, list: &ListArray) -> Result<()> {
        let (ts, _) = series::sample_slices(list.values().as_struct());
        let offsets = list.offsets();
        let grid = self.grid;
        for row in 0..list.len() {
            if list.is_null(row) {
                continue;
            }
            let (lo, hi) = (offsets[row] as usize, offsets[row + 1] as usize);
            grid.runs(&ts[lo..hi], |index, _, len| {
                self.present[index..index + len].fill(true);
            })?;
        }
        Ok(())
    }

    /// The steps whose presence equals `present`, as one samples list
    /// valued 1: the answer when `false`, the partial state when `true`.
    fn list(&self, present: bool) -> ScalarValue {
        let ts: Vec<i64> = (0..self.grid.len())
            .filter(|&i| self.present[i] == present)
            .map(|i| self.grid.timestamp(i))
            .collect();
        let n = ts.len();
        let entries = StructArray::new(
            series::sample_fields(),
            vec![
                Arc::new(TimestampMillisecondArray::from(ts)),
                Arc::new(Float64Array::from(vec![1.0; n])),
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
        self.mark(values[0].as_list::<i32>())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(self.list(false))
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>() + self.present.capacity()
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.list(true)])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.mark(states[0].as_list::<i32>())
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct Absent {
    signature: Signature,
}

impl Default for Absent {
    fn default() -> Self {
        Self {
            signature: Signature::exact(
                vec![
                    series::floats_type(),
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
    AggregateUDF::new_from_impl(Absent::default())
}

/// `promql_absent(floats, start, end, step)`. The grid is an argument
/// for the same reason it is one of `promql_aggregate`: it is what turns
/// a timestamp into a step, and here also what the answer is made of.
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

impl AggregateUDFImpl for Absent {
    fn name(&self) -> &str {
        NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.first() != Some(&series::floats_type()) {
            return plan_err!(
                "{NAME}: first argument must be {}, got {:?}",
                series::floats_type(),
                arg_types.first()
            );
        }
        Ok(series::floats_type())
    }

    /// A list, empty when every step was answered; never NULL.
    fn is_nullable(&self) -> bool {
        false
    }

    fn accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(Steps::new(grid_of(&args)?)))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![Arc::new(Field::new(
            format_state_name(args.name, "present"),
            series::floats_type(),
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
    use crate::series::FLOATS;

    fn labels(query: &str) -> Vec<(String, String)> {
        let Expr::Call(call) = promql_parser::parse_expr(query).expect("parses") else {
            panic!("{query}: not a call");
        };
        labels_for(&call.args[0])
    }

    fn pairs(v: &[(&str, &str)]) -> Vec<(String, String)> {
        v.iter()
            .map(|(n, val)| (n.to_string(), val.to_string()))
            .collect()
    }

    #[test]
    fn only_equality_matchers_name_the_output() {
        assert_eq!(labels("absent(nonexistent)"), pairs(&[]));
        assert_eq!(
            labels(r#"absent(nonexistent{job="testjob", instance="testinstance", method=~".x"})"#),
            pairs(&[("instance", "testinstance"), ("job", "testjob")])
        );
        assert_eq!(
            labels(r#"absent(nonexistent{job="a", instance!="b", method!~"c"})"#),
            pairs(&[("job", "a")])
        );
        // `__name__` never comes out, written explicitly or not.
        assert_eq!(
            labels(r#"absent({__name__="up", job="a"})"#),
            pairs(&[("job", "a")])
        );
    }

    /// Upstream's `has` map: a name an equality matcher already set is
    /// deleted by any later matcher on it, an equality one included, and
    /// only the written order decides.
    #[test]
    fn a_repeated_name_is_dropped_in_upstreams_order() {
        assert_eq!(
            labels(r#"absent(nonexistent{job="testjob",job="testjob2",foo="bar"})"#),
            pairs(&[("foo", "bar")])
        );
        assert_eq!(
            labels(r#"absent(nonexistent{job="testjob",job="testjob2",job="three",foo="bar"})"#),
            pairs(&[("foo", "bar")])
        );
        assert_eq!(
            labels(r#"absent(nonexistent{job="testjob",job=~"testjob2",foo="bar"})"#),
            pairs(&[("foo", "bar")])
        );
        assert_eq!(
            labels(r#"absent(nonexistent{job=~"testjob2",job="testjob",foo="bar"})"#),
            pairs(&[("foo", "bar"), ("job", "testjob")])
        );
        assert_eq!(labels(r#"absent(x{job="a",job="a"})"#), pairs(&[]));
    }

    #[test]
    fn a_range_selector_and_parentheses_are_looked_through() {
        assert_eq!(
            labels(r#"absent_over_time(nonexistent{handler="/foo", instance="x"}[5m])"#),
            pairs(&[("handler", "/foo"), ("instance", "x")])
        );
        assert_eq!(
            labels(r#"absent(((nonexistent{job="a"})))"#),
            pairs(&[("job", "a")])
        );
    }

    #[test]
    fn anything_but_a_selector_has_no_labels() {
        for query in [
            r#"absent(sum(nonexistent{job="a"}))"#,
            r#"absent(nonexistent{job="a"} > 1)"#,
            r#"absent(abs(nonexistent{job="a"}))"#,
            r#"absent(rate(nonexistent{job="a"}[5m]))"#,
            "absent(vector(1))",
        ] {
            assert_eq!(labels(query), pairs(&[]), "{query}");
        }
    }

    /// 0..120s every 30s: five steps.
    fn grid() -> Grid {
        Grid::new(NAME, 0, 120_000, 30_000).unwrap()
    }

    /// One samples list per row, every value 1.
    fn samples(rows: &[&[i64]]) -> ArrayRef {
        let mut offsets = vec![0i32];
        let mut ts = Vec::new();
        for row in rows {
            ts.extend_from_slice(row);
            offsets.push(ts.len() as i32);
        }
        let n = ts.len();
        let entries = StructArray::new(
            series::sample_fields(),
            vec![
                Arc::new(TimestampMillisecondArray::from(ts)),
                Arc::new(Float64Array::from(vec![1.0; n])),
            ],
            None,
        );
        Arc::new(ListArray::new(
            series::sample_item(),
            OffsetBuffer::new(offsets.into()),
            Arc::new(entries),
            None,
        ))
    }

    fn timestamps(v: &ScalarValue) -> Vec<i64> {
        let ScalarValue::List(list) = v else {
            panic!("not a list")
        };
        let (ts, vs) = series::sample_slices(list.values().as_struct());
        assert!(vs.iter().all(|v| *v == 1.0), "every value is 1");
        ts.to_vec()
    }

    #[test]
    fn no_input_at_all_is_the_whole_grid() {
        let mut acc = Steps::new(grid());
        assert_eq!(
            timestamps(&acc.evaluate().unwrap()),
            [0, 30_000, 60_000, 90_000, 120_000]
        );
        assert_eq!(timestamps(&acc.state().unwrap()[0]), [] as [i64; 0]);
    }

    #[test]
    fn the_answer_is_the_complement_of_every_row_together() {
        let mut acc = Steps::new(grid());
        acc.update_batch(&[samples(&[&[0, 30_000], &[], &[30_000, 90_000]])])
            .unwrap();
        assert_eq!(timestamps(&acc.evaluate().unwrap()), [60_000, 120_000]);
        assert_eq!(timestamps(&acc.state().unwrap()[0]), [0, 30_000, 90_000]);

        acc.update_batch(&[samples(&[&[60_000]])]).unwrap();
        assert_eq!(timestamps(&acc.evaluate().unwrap()), [120_000]);
        acc.update_batch(&[samples(&[&[120_000]])]).unwrap();
        assert_eq!(timestamps(&acc.evaluate().unwrap()), [] as [i64; 0]);
    }

    #[test]
    fn merging_partial_states_is_folding_their_steps() {
        let mut left = Steps::new(grid());
        left.update_batch(&[samples(&[&[0]])]).unwrap();
        let mut right = Steps::new(grid());
        right.update_batch(&[samples(&[&[90_000]])]).unwrap();
        let empty = Steps::new(grid()).state().unwrap().remove(0);

        let mut merged = Steps::new(grid());
        let states: Vec<ScalarValue> = vec![
            left.state().unwrap().remove(0),
            right.state().unwrap().remove(0),
            empty,
        ];
        merged
            .merge_batch(&[ScalarValue::iter_to_array(states).unwrap()])
            .unwrap();
        assert_eq!(
            timestamps(&merged.evaluate().unwrap()),
            [30_000, 60_000, 120_000]
        );
    }

    #[test]
    fn a_sample_off_the_grid_is_an_error() {
        let mut acc = Steps::new(grid());
        let err = acc.update_batch(&[samples(&[&[15_000]])]).unwrap_err();
        assert!(err.to_string().contains("not on the step grid"), "{err}");
        let err = acc.update_batch(&[samples(&[&[150_000]])]).unwrap_err();
        assert!(err.to_string().contains("not on the step grid"), "{err}");
    }

    /// The property the operator rests on, checked against DataFusion
    /// itself rather than the accumulator: an aggregate with no group key
    /// answers one row over a table with no rows, through a Partial and a
    /// Final when the table has several partitions, some of them empty.
    #[tokio::test]
    async fn one_row_comes_out_of_no_rows_across_partitions() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            FLOATS,
            series::floats_type(),
            false,
        )]));
        let row = |ts: &[i64]| RecordBatch::try_new(schema.clone(), vec![samples(&[ts])]).unwrap();
        for partitions in [
            vec![vec![]],
            vec![vec![], vec![], vec![]],
            vec![vec![row(&[0])], vec![], vec![row(&[90_000]), row(&[])]],
        ] {
            let fed = partitions.iter().flatten().count() > 0;
            let table = MemTable::try_new(schema.clone(), partitions).unwrap();
            let ctx =
                SessionContext::new_with_config(SessionConfig::new().with_target_partitions(3));
            let batches = ctx
                .read_table(Arc::new(table))
                .unwrap()
                .aggregate(
                    vec![],
                    vec![call(col(FLOATS), 0, 120_000, 30_000).alias(FLOATS)],
                )
                .unwrap()
                .collect()
                .await
                .unwrap();
            let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert_eq!(rows, 1);
            let got = ScalarValue::try_from_array(
                batches
                    .iter()
                    .find(|b| b.num_rows() == 1)
                    .unwrap()
                    .column(0),
                0,
            )
            .unwrap();
            let expected: &[i64] = if fed {
                &[30_000, 60_000, 120_000]
            } else {
                &[0, 30_000, 60_000, 90_000, 120_000]
            };
            assert_eq!(timestamps(&got), expected);
        }
    }
}
