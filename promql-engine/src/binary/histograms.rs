//! The binary operators between two vectors when either may hold native
//! histograms: `promql_histogram_binary(floats, histograms, __rhs, labels,
//! '+', 'on(job)')`.
//!
//! The same grouping as [`super`], by the match signature, and the same
//! rules for what pairs and what is an error, read off the same
//! [`Pairing`]. What differs is the arithmetic: upstream's
//! `vectorElemBinop` (`promql/engine.go:3257` at 83962c35) in full, all
//! four combinations of a float and a histogram on either side.
//!
//! [`super`]'s lanes are a float per step, bit for bit Arrow's kernels,
//! and stay that. This accumulator holds each series of its match group
//! as it arrived, both sample lists in time order, and pairs them step
//! by step when the group is complete. A histogram is hundreds of bytes
//! behind a pointer, so lanes over the grid would be lanes of pointers,
//! and the kernel would be a loop over them either way. The planner
//! sends an operator here only when an operand has a `histograms`
//! column; a query without native histograms never sees this code.

use std::any::Any;
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, Int64Array, ListArray, StringViewArray, StructArray,
};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Fields, Int64Type};
use datafusion::common::{plan_err, ScalarValue};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::utils::format_state_name;
use datafusion::logical_expr::{
    lit, Accumulator, AggregateUDF, AggregateUDFImpl, Expr, Signature, Volatility,
};
use datafusion::physical_expr::expressions::Literal;
use promql_histogram::{CounterResetHint, FloatHistogram};

use super::{
    names_of, one_row, parse_literal, read_labels, rectangle, text_lane, texts, Card, Matching, Op,
    Pairing, GROUPING_NOT_UNIQUE, LHS, MULTIPLE_MATCHES, PAIR_FLOATS, PAIR_LABELS, RHS,
};
use crate::aggregate::Grid;
use crate::histogram::{self, HistogramColumns, HistogramsBuilder, Value, HISTOGRAMS};
use crate::histogram_fn::Map;
use crate::series::{self, SamplesBuilder};

pub const NAME: &str = "promql_histogram_binary";

/// The histograms of an output series, beside [`PAIR_LABELS`] and
/// [`PAIR_FLOATS`].
pub const PAIR_HISTOGRAMS: &str = HISTOGRAMS;

fn pair_fields(names: &[String]) -> Fields {
    Fields::from(vec![
        Field::new(PAIR_LABELS, series::labels_type(names), false),
        Field::new(PAIR_FLOATS, series::floats_type(), false),
        Field::new(PAIR_HISTOGRAMS, histogram::histograms_type(), false),
    ])
}

fn pair_item(names: &[String]) -> FieldRef {
    Arc::new(Field::new(
        "item",
        DataType::Struct(pair_fields(names)),
        false,
    ))
}

/// What this pairing computes: one of [`Op`]'s operators, with its
/// `bool`, or a histogram trim. The trims exist only where a histogram
/// can be an operand, so they are operators here and not in [`Op`], whose
/// every member has a float kernel.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Operator {
    Op(Op, bool),
    /// `h </ x`.
    TrimUpper,
    /// `h >/ x`.
    TrimLower,
}

impl Operator {
    fn literal(self) -> &'static str {
        match self {
            Operator::Op(op, return_bool) => super::literal(op, return_bool),
            Operator::TrimUpper => "</",
            Operator::TrimLower => ">/",
        }
    }

    fn parse(s: &str) -> Option<Self> {
        match s {
            "</" => Some(Operator::TrimUpper),
            ">/" => Some(Operator::TrimLower),
            _ => parse_literal(s).map(|(op, return_bool)| Operator::Op(op, return_bool)),
        }
    }

    /// The operator [`Pairing`]'s label rules are asked about. A trim is
    /// not in `changesMetricSchema` and is no set operator, which is all
    /// those rules read, and what a filtering comparison answers too.
    fn rules(self) -> (Op, bool) {
        match self {
            Operator::Op(op, return_bool) => (op, return_bool),
            Operator::TrimUpper | Operator::TrimLower => (Op::Eql, false),
        }
    }
}

pub fn udaf() -> AggregateUDF {
    AggregateUDF::new_from_impl(Binary::default())
}

/// `promql_histogram_binary(floats, histograms, <side>, labels, '<op>',
/// '<matching>')`. No grid: a step is a timestamp some series has a
/// sample at.
pub fn call(
    floats: Expr,
    histograms: Expr,
    side: Expr,
    labels: Expr,
    operator: Operator,
    matching: &Matching,
) -> Expr {
    udaf().call(vec![
        floats,
        histograms,
        side,
        labels,
        lit(operator.literal()),
        lit(matching.literal()),
    ])
}

/// `vectorElemBinop` for one pair, without the `returnBool` wrapper:
/// the value and whether a comparison holds. `None` is an element
/// upstream leaves out, with an annotation this engine cannot attach
/// yet: two types the operator does not combine, histograms that cannot
/// be added, or an addition whose operands disagree about a counter
/// reset, which upstream's `VectorBinop` returns on like an error.
fn elem(operator: Operator, lhs: &Value, rhs: &Value) -> Option<(Value, bool)> {
    let histogram = |h: FloatHistogram| Some((Value::Histogram(Arc::new(h)), true));
    let op = match operator {
        Operator::Op(op, _) => op,
        // A histogram on the left and a float on the right, or nothing.
        Operator::TrimUpper | Operator::TrimLower => {
            let (Value::Histogram(h), Value::Float(x)) = (lhs, rhs) else {
                return None;
            };
            let by = match operator {
                Operator::TrimUpper => Map::TrimUpper(*x),
                _ => Map::TrimLower(*x),
            };
            return histogram(by.apply(FloatHistogram::clone(h)));
        }
    };
    match (lhs, rhs) {
        (Value::Float(l), Value::Float(r)) => match op.is_comparison() {
            true => Some((Value::Float(*l), op.compare(*l, *r))),
            false => Some((Value::Float(op.value(*l, *r, false)?), true)),
        },
        (Value::Float(l), Value::Histogram(h)) => match op {
            Op::Mul => histogram(Map::Mul(*l).apply(FloatHistogram::clone(h))),
            _ => None,
        },
        (Value::Histogram(h), Value::Float(r)) => match op {
            Op::Mul => histogram(Map::Mul(*r).apply(FloatHistogram::clone(h))),
            Op::Div => histogram(Map::Div(*r).apply(FloatHistogram::clone(h))),
            _ => None,
        },
        (Value::Histogram(l), Value::Histogram(r)) => match op {
            Op::Add | Op::Sub => {
                let mut result = FloatHistogram::clone(l);
                let outcome = match op {
                    Op::Add => result.add(r),
                    _ => result.sub(r),
                }
                .ok()?;
                if outcome.counter_reset_collision {
                    return None;
                }
                if op == Op::Sub {
                    // "The result must be marked as gauge."
                    result.counter_reset_hint = CounterResetHint::Gauge;
                }
                result.compact();
                histogram(result)
            }
            Op::Eql => Some((Value::Histogram(Arc::clone(l)), l.equals(r))),
            Op::Neq => Some((Value::Histogram(Arc::clone(l)), !l.equals(r))),
            _ => None,
        },
    }
}

/// One operand series of a match group: both sample lists as one, in
/// time order.
#[derive(Debug)]
struct Series {
    side: usize,
    labels: Vec<String>,
    samples: Vec<(i64, Value)>,
}

impl Series {
    fn at(&self, t: i64) -> Option<&Value> {
        self.samples
            .binary_search_by_key(&t, |(t, _)| *t)
            .ok()
            .map(|i| &self.samples[i].1)
    }
}

/// One output series under construction.
#[derive(Debug, Default)]
struct Bucket {
    labels: Vec<String>,
    floats: Vec<(i64, f64)>,
    histograms: Vec<(i64, Arc<FloatHistogram>)>,
    /// The step this last paired at, as in [`super::Bucket`].
    claimed: Option<i64>,
}

impl Bucket {
    fn push(&mut self, t: i64, value: Value) {
        match value {
            Value::Float(v) => self.floats.push((t, v)),
            Value::Histogram(h) => self.histograms.push((t, h)),
        }
    }
}

#[derive(Debug)]
struct Group {
    operator: Operator,
    /// The matching and the label schema, for the label rules and error
    /// texts [`Pairing`] owns. Its lanes are unused.
    rules: Pairing,
    series: Vec<Series>,
}

impl Group {
    fn steps(&self) -> BTreeSet<i64> {
        self.series
            .iter()
            .flat_map(|s| s.samples.iter().map(|(t, _)| *t))
            .collect()
    }

    /// [`Pairing::sets`], a sample of either type kept as it is.
    fn sets(&self) -> Vec<Bucket> {
        let mut buckets: Vec<Bucket> = Vec::new();
        let mut by_labels: HashMap<&Vec<String>, usize> = HashMap::new();
        for t in self.steps() {
            let present = |side: usize| {
                self.series
                    .iter()
                    .any(|s| s.side == side && s.at(t).is_some())
            };
            let (left, right) = (present(LHS), present(RHS));
            for series in &self.series {
                let Some(value) = series.at(t) else {
                    continue;
                };
                let keep = match self.rules.op {
                    Op::And => series.side == LHS && right,
                    Op::Unless => series.side == LHS && !right,
                    Op::Or => series.side == LHS || !left,
                    _ => unreachable!("only a set operator walks this grid"),
                };
                if !keep {
                    continue;
                }
                let bucket = *by_labels.entry(&series.labels).or_insert_with(|| {
                    buckets.push(Bucket {
                        labels: series.labels.clone(),
                        ..Bucket::default()
                    });
                    buckets.len() - 1
                });
                buckets[bucket].push(t, value.clone());
            }
        }
        buckets
    }

    /// [`Pairing::pairs`] step for step, with [`elem`] applied as each
    /// pair is found. The errors and their order are that function's.
    fn pairs(&self) -> Result<Vec<Bucket>> {
        let rules = &self.rules;
        if rules.op.is_set() {
            return Ok(self.sets());
        }
        let one_side = rules.matching.one_side();
        let mut buckets: Vec<Bucket> = Vec::new();
        let mut by_labels: HashMap<Vec<String>, usize> = HashMap::new();
        let one_to_one = rules.matching.card == Card::OneToOne;
        let fill = |v: Option<f64>| v.map(Value::Float);
        let (fill_one, fill_many) = (
            fill(rules.matching.fill_one()),
            fill(rules.matching.fill_many()),
        );
        let nothing: Vec<String> = vec![String::new(); rules.schema.len()];

        for t in self.steps() {
            let here = |one: bool| {
                self.series
                    .iter()
                    .filter(move |s| (s.side == one_side) == one)
                    .filter_map(move |s| Some((s, s.at(t)?)))
            };
            let mut ones = here(true);
            let one = ones.next();
            if here(false).next().is_none() && fill_one.is_none() && fill_many.is_none() {
                continue;
            }
            if ones.next().is_some() {
                return Err(rules.duplicate());
            }

            // The "many" series, or `None` where a fill stands in.
            let mut todo: Vec<(Option<&Series>, Value, Value)> = Vec::new();
            if let Some(one_value) = one.map(|(_, v)| v.clone()).or(fill_one.clone()) {
                for (many, value) in here(false) {
                    todo.push((Some(many), value.clone(), one_value.clone()));
                }
            }
            if let (true, Some((_, one_value)), Some(fill)) = (todo.is_empty(), one, &fill_many) {
                todo.push((None, fill.clone(), one_value.clone()));
            }
            let one_labels = one.map_or(&nothing, |(s, _)| &s.labels);
            // Upstream's `matchedSigsPresent` for this group at this step.
            let mut matched = false;
            for (many, many_value, one_value) in todo {
                let (lhs, rhs) = match one_side {
                    LHS => (one_value, many_value),
                    _ => (many_value, one_value),
                };
                // `vectorElemBinop` returns before the match bookkeeping on
                // an error and after it on a comparison that does not hold,
                // so a pair the operator declines claims nothing while a
                // sample that does not survive still claims. `elem` answers
                // `None` for exactly the pairings upstream errors on.
                let Some((value, keep)) = elem(self.operator, &lhs, &rhs) else {
                    continue;
                };

                let base = match many {
                    Some(many) => many.labels.clone(),
                    None => rules.signature_of(one_labels),
                };
                let one_part = match many {
                    Some(_) if one.is_none() => rules.signature_of(&base),
                    _ => one_labels.clone(),
                };
                let labels = rules.result_labels(&base, &one_part);

                if one_to_one {
                    if matched {
                        return Err(DataFusionError::Execution(MULTIPLE_MATCHES.into()));
                    }
                    matched = true;
                }
                let bucket = *by_labels.entry(labels.clone()).or_insert_with(|| {
                    buckets.push(Bucket {
                        labels,
                        ..Bucket::default()
                    });
                    buckets.len() - 1
                });
                let bucket = &mut buckets[bucket];
                if bucket.claimed == Some(t) {
                    return Err(DataFusionError::Execution(GROUPING_NOT_UNIQUE.into()));
                }
                bucket.claimed = Some(t);

                match (rules.return_bool, keep) {
                    (true, keep) => bucket.push(t, Value::Float(f64::from(u8::from(keep)))),
                    (false, true) => bucket.push(t, value),
                    (false, false) => {}
                }
            }
        }
        Ok(buckets)
    }

    fn absorb(&mut self, side: usize, labels: Vec<String>, samples: Vec<(i64, Value)>) {
        if samples.is_empty() {
            return;
        }
        if !self.rules.op.is_set() && side == self.rules.matching.one_side() {
            self.rules.remember(&labels);
        }
        self.series.push(Series {
            side,
            labels,
            samples,
        });
    }
}

/// The samples of row `row` of both lists as one, in time order.
fn samples_of(
    floats: &ListArray,
    histograms: &ListArray,
    columns: &HistogramColumns<'_>,
    row: usize,
) -> Vec<(i64, Value)> {
    let (ts, vs) = series::sample_slices(floats.values().as_struct());
    let range = |list: &ListArray| {
        let offsets = list.value_offsets();
        offsets[row] as usize..offsets[row + 1] as usize
    };
    let mut samples: Vec<(i64, Value)> = range(floats)
        .map(|i| (ts[i], Value::Float(vs[i])))
        .chain(range(histograms).map(|i| {
            (
                columns.timestamps[i],
                Value::Histogram(Arc::new(columns.decode(i))),
            )
        }))
        .collect();
    samples.sort_by_key(|(t, _)| *t);
    samples
}

fn list<'a>(values: &'a [ArrayRef], index: usize, what: &str) -> Result<&'a ListArray> {
    values
        .get(index)
        .and_then(|v| v.as_list_opt::<i32>())
        .ok_or_else(|| DataFusionError::Internal(format!("{NAME}: {what} is not a list")))
}

const STATE_LABELS: &str = "labels";
const STATE_SIDES: &str = "sides";
const STATE_FLOATS: &str = "floats";
const STATE_HISTOGRAMS: &str = "histograms";

fn state_item(name: &str, of: DataType) -> FieldRef {
    Arc::new(Field::new(name, of, false))
}

impl Accumulator for Group {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let floats = list(values, 0, "the floats argument")?;
        let histograms = list(values, 1, "the histograms argument")?;
        let side = values
            .get(2)
            .and_then(|v| v.as_boolean_opt())
            .ok_or_else(|| {
                DataFusionError::Internal(format!("{NAME}: third argument is not a boolean"))
            })?;
        let labels = values
            .get(3)
            .and_then(|v| v.as_struct_opt())
            .ok_or_else(|| {
                DataFusionError::Internal(format!("{NAME}: fourth argument is not a label struct"))
            })?;
        let columns = HistogramColumns::new(histograms.values().as_struct());
        for row in 0..floats.len() {
            let which = if side.is_valid(row) && side.value(row) {
                RHS
            } else {
                LHS
            };
            self.absorb(
                which,
                read_labels(&self.rules.schema, labels, row),
                samples_of(floats, histograms, &columns, row),
            );
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let mut buckets = self.pairs()?;
        buckets.retain(|b| !b.floats.is_empty() || !b.histograms.is_empty());
        // Sorted, because DataFusion is free to hand the rows of a group
        // over in any order and the answer must not be.
        buckets.sort_by(|a, b| a.labels.cmp(&b.labels));

        let mut floats = SamplesBuilder::default();
        let mut histograms = HistogramsBuilder::default();
        for bucket in &buckets {
            for (t, v) in &bucket.floats {
                floats.push(*t, *v);
            }
            floats.finish_row();
            for (t, h) in &bucket.histograms {
                histograms.push(*t, h);
            }
            histograms.finish_row();
        }

        let schema = &self.rules.schema;
        let DataType::Struct(fields) = series::labels_type(schema) else {
            unreachable!("a labels type is a struct")
        };
        let columns: Vec<ArrayRef> = fields
            .iter()
            .map(|field| {
                let index = schema
                    .iter()
                    .position(|name| name == field.name())
                    .expect("every field of the labels type is a column of the schema");
                let values: Vec<&str> = buckets.iter().map(|b| b.labels[index].as_str()).collect();
                Arc::new(StringViewArray::from(values)) as ArrayRef
            })
            .collect();
        let labels = match fields.is_empty() {
            true => StructArray::new_empty_fields(buckets.len(), None),
            false => StructArray::new(fields, columns, None),
        };
        let pairs = StructArray::new(
            pair_fields(schema),
            vec![
                Arc::new(labels),
                Arc::new(floats.take_all()),
                Arc::new(histograms.take_all()),
            ],
            None,
        );
        Ok(one_row(pair_item(schema), Arc::new(pairs)))
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
            + self
                .series
                .iter()
                .map(|s| {
                    std::mem::size_of::<Series>()
                        + s.labels.iter().map(String::capacity).sum::<usize>()
                        + s.samples.capacity() * std::mem::size_of::<(i64, Value)>()
                        + s.samples
                            .iter()
                            .map(|(_, v)| match v {
                                Value::Float(_) => 0,
                                Value::Histogram(h) => histogram::heap_size(h),
                            })
                            .sum::<usize>()
                })
                .sum::<usize>()
    }

    /// The series as they arrived: nothing is paired until the group is
    /// whole, so a partial is the rows it saw.
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let mut floats = SamplesBuilder::default();
        let mut histograms = HistogramsBuilder::default();
        for series in &self.series {
            for (t, value) in &series.samples {
                match value {
                    Value::Float(v) => floats.push(*t, *v),
                    Value::Histogram(h) => histograms.push(*t, h),
                }
            }
            floats.finish_row();
            histograms.finish_row();
        }
        Ok(vec![
            text_lane(
                STATE_LABELS,
                self.series
                    .iter()
                    .flat_map(|s| s.labels.iter().map(String::as_str))
                    .collect(),
            ),
            one_row(
                state_item(STATE_SIDES, DataType::Int64),
                Arc::new(Int64Array::from_iter_values(
                    self.series.iter().map(|s| s.side as i64),
                )),
            ),
            one_row(
                state_item(STATE_FLOATS, series::floats_type()),
                Arc::new(floats.take_all()),
            ),
            one_row(
                state_item(STATE_HISTOGRAMS, histogram::histograms_type()),
                Arc::new(histograms.take_all()),
            ),
        ])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let labels = texts(states.first(), STATE_LABELS)?;
        let sides = list(states, 1, "the sides state")?;
        let floats = list(states, 2, "the floats state")?;
        let histograms = list(states, 3, "the histograms state")?;
        let width = self.rules.schema.len();
        for group in 0..sides.len() {
            let sides = sides.value(group);
            let sides = sides.as_primitive_opt::<Int64Type>().ok_or_else(|| {
                DataFusionError::Internal(format!("{NAME}: the sides state is not Int64"))
            })?;
            let label_sets = rectangle(&labels.value(group), width, sides.len())?;
            let (floats, histograms) = (floats.value(group), histograms.value(group));
            let (floats, histograms) =
                (floats.as_list_opt::<i32>(), histograms.as_list_opt::<i32>());
            let (Some(floats), Some(histograms)) = (floats, histograms) else {
                return Err(DataFusionError::Internal(format!(
                    "{NAME}: partial state samples are not lists"
                )));
            };
            if label_sets.len() != sides.len()
                || floats.len() != sides.len()
                || histograms.len() != sides.len()
            {
                return Err(DataFusionError::Internal(format!(
                    "{NAME}: partial state holds {} sides, {} label sets, {} and {} sample rows",
                    sides.len(),
                    label_sets.len(),
                    floats.len(),
                    histograms.len()
                )));
            }
            let columns = HistogramColumns::new(histograms.values().as_struct());
            for (row, labels) in label_sets.into_iter().enumerate() {
                let side = if sides.value(row) == RHS as i64 {
                    RHS
                } else {
                    LHS
                };
                self.absorb(side, labels, samples_of(floats, histograms, &columns, row));
            }
        }
        Ok(())
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct Binary {
    signature: Signature,
}

impl Default for Binary {
    fn default() -> Self {
        Self {
            signature: Signature::any(6, Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for Binary {
    fn name(&self) -> &str {
        NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if !arg_types.first().is_some_and(series::is_floats_type) {
            return plan_err!("{NAME}: first argument must be {}", series::floats_type());
        }
        if !arg_types.get(1).is_some_and(histogram::is_histograms_type) {
            return plan_err!(
                "{NAME}: second argument must be {}",
                histogram::histograms_type()
            );
        }
        if !matches!(arg_types.get(2), Some(DataType::Boolean)) {
            return plan_err!("{NAME}: third argument must say which side a row is on");
        }
        let Some(names) = arg_types.get(3).and_then(names_of) else {
            return plan_err!("{NAME}: fourth argument must be a label struct");
        };
        Ok(DataType::List(pair_item(&names)))
    }

    fn is_nullable(&self) -> bool {
        false
    }

    fn accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        if args.is_distinct {
            return plan_err!("{NAME}: DISTINCT is not supported");
        }
        let text = |i: usize| {
            let literal = args
                .exprs
                .get(i)
                .and_then(|e| (e.as_ref() as &dyn Any).downcast_ref::<Literal>())?;
            match literal.value() {
                ScalarValue::Utf8(Some(s)) => Some(s.as_str()),
                _ => None,
            }
        };
        let operator = text(4).and_then(Operator::parse).ok_or_else(|| {
            DataFusionError::Plan(format!(
                "{NAME}: fifth argument must be a binary operator as a string literal"
            ))
        })?;
        let (op, return_bool) = operator.rules();
        let matching = text(5).and_then(Matching::parse).ok_or_else(|| {
            DataFusionError::Plan(format!(
                "{NAME}: sixth argument must be the matching modifiers as a string literal"
            ))
        })?;
        let schema = args
            .exprs
            .get(3)
            .and_then(|labels| labels.data_type(args.schema).ok())
            .as_ref()
            .and_then(names_of)
            .ok_or_else(|| {
                DataFusionError::Plan(format!("{NAME}: fourth argument must be a label struct"))
            })?;
        // One step: the rules read no lane, and a grid costs three.
        let grid = Grid::new(NAME, 0, 0, 1)?;
        Ok(Box::new(Group {
            operator,
            rules: Pairing::new(op, return_bool, matching, grid, schema),
            series: Vec::new(),
        }))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        let field = |name: &str, of: DataType| -> FieldRef {
            Arc::new(Field::new(
                format_state_name(args.name, name),
                DataType::List(state_item(name, of)),
                false,
            ))
        };
        Ok(vec![
            field(STATE_LABELS, DataType::Utf8),
            field(STATE_SIDES, DataType::Int64),
            field(STATE_FLOATS, series::floats_type()),
            field(STATE_HISTOGRAMS, histogram::histograms_type()),
        ])
    }
}

#[cfg(test)]
mod tests {
    use promql_histogram::Span;

    use super::*;

    fn h(count: f64) -> Value {
        Value::Histogram(Arc::new(FloatHistogram {
            count,
            sum: count,
            positive_spans: vec![Span {
                offset: 0,
                length: 1,
            }],
            positive_buckets: vec![count],
            ..FloatHistogram::default()
        }))
    }

    fn count(v: &Value) -> f64 {
        match v {
            Value::Histogram(h) => h.count,
            Value::Float(_) => panic!("expected a histogram"),
        }
    }

    fn group(op: Op, return_bool: bool, matching: Matching) -> Group {
        let schema = vec!["__name__".to_string(), "job".to_string()];
        Group {
            operator: Operator::Op(op, return_bool),
            rules: Pairing::new(
                op,
                return_bool,
                matching,
                Grid::new(NAME, 0, 0, 1).unwrap(),
                schema,
            ),
            series: Vec::new(),
        }
    }

    fn labels(name: &str, job: &str) -> Vec<String> {
        vec![name.to_string(), job.to_string()]
    }

    #[test]
    fn the_arithmetic_is_upstreams_vector_elem_binop() {
        let elem = |op: Op, lhs: &Value, rhs: &Value| elem(Operator::Op(op, false), lhs, rhs);
        let float = Value::Float(3.0);
        // histogram ∘ histogram
        assert_eq!(count(&elem(Op::Add, &h(2.0), &h(5.0)).unwrap().0), 7.0);
        let (difference, _) = elem(Op::Sub, &h(5.0), &h(2.0)).unwrap();
        assert_eq!(count(&difference), 3.0);
        let Value::Histogram(difference) = difference else {
            unreachable!()
        };
        assert_eq!(difference.counter_reset_hint, CounterResetHint::Gauge);
        assert!(elem(Op::Eql, &h(2.0), &h(2.0)).unwrap().1);
        assert!(!elem(Op::Eql, &h(2.0), &h(3.0)).unwrap().1);
        assert!(elem(Op::Neq, &h(2.0), &h(3.0)).unwrap().1);
        for op in [
            Op::Mul,
            Op::Div,
            Op::Pow,
            Op::Mod,
            Op::Gtr,
            Op::Lte,
            Op::Atan2,
        ] {
            assert!(elem(op, &h(2.0), &h(3.0)).is_none(), "{op:?}");
        }
        // histogram ∘ float and float ∘ histogram
        assert_eq!(count(&elem(Op::Mul, &h(2.0), &float).unwrap().0), 6.0);
        assert_eq!(count(&elem(Op::Mul, &float, &h(2.0)).unwrap().0), 6.0);
        assert_eq!(count(&elem(Op::Div, &h(6.0), &float).unwrap().0), 2.0);
        assert!(elem(Op::Div, &float, &h(6.0)).is_none());
        for op in [Op::Add, Op::Sub, Op::Eql, Op::Gtr] {
            assert!(elem(op, &h(2.0), &float).is_none(), "{op:?}");
            assert!(elem(op, &float, &h(2.0)).is_none(), "{op:?}");
        }
        // float ∘ float is what it always was.
        assert_eq!(
            elem(Op::Add, &float, &float),
            Some((Value::Float(6.0), true))
        );
        assert_eq!(
            elem(Op::Gtr, &float, &Value::Float(5.0)),
            Some((float, false))
        );
    }

    #[test]
    fn a_trim_takes_a_histogram_and_a_float_in_that_order() {
        let float = Value::Float(0.5);
        for trim in [Operator::TrimUpper, Operator::TrimLower] {
            assert!(elem(trim, &h(2.0), &float).is_some(), "{trim:?}");
            assert!(elem(trim, &float, &h(2.0)).is_none(), "{trim:?}");
            assert!(elem(trim, &h(2.0), &h(2.0)).is_none(), "{trim:?}");
            assert!(elem(trim, &float, &float).is_none(), "{trim:?}");
            assert_eq!(Operator::parse(trim.literal()), Some(trim));
        }
    }

    #[test]
    fn an_addition_across_a_counter_reset_collision_is_left_out() {
        let hinted = |hint| {
            let Value::Histogram(base) = h(1.0) else {
                unreachable!()
            };
            Value::Histogram(Arc::new(FloatHistogram {
                counter_reset_hint: hint,
                ..FloatHistogram::clone(&base)
            }))
        };
        let (reset, not_reset) = (
            hinted(CounterResetHint::Reset),
            hinted(CounterResetHint::NotReset),
        );
        let add = Operator::Op(Op::Add, false);
        assert!(elem(add, &reset, &not_reset).is_none());
        assert!(elem(add, &not_reset, &not_reset).is_some());
    }

    #[test]
    fn histograms_pair_by_signature_and_step() {
        let mut g = group(Op::Add, false, Matching::default());
        g.absorb(
            LHS,
            labels("a", "x"),
            vec![(0, h(1.0)), (60, h(2.0)), (120, Value::Float(1.0))],
        );
        g.absorb(
            RHS,
            labels("b", "x"),
            vec![(60, h(10.0)), (120, h(1.0)), (180, h(1.0))],
        );
        let buckets = g.pairs().unwrap();
        assert_eq!(buckets.len(), 1);
        // 0 and 180 have one side only; 120 adds a float to a histogram.
        assert_eq!(buckets[0].labels, labels("", "x"));
        assert!(buckets[0].floats.is_empty());
        assert_eq!(buckets[0].histograms.len(), 1);
        assert_eq!(buckets[0].histograms[0].0, 60);
        assert_eq!(buckets[0].histograms[0].1.count, 12.0);
    }

    #[test]
    fn a_bool_comparison_of_histograms_scores_as_floats() {
        let mut g = group(Op::Eql, true, Matching::default());
        g.absorb(LHS, labels("a", "x"), vec![(0, h(1.0)), (60, h(2.0))]);
        g.absorb(RHS, labels("b", "x"), vec![(0, h(1.0)), (60, h(3.0))]);
        let buckets = g.pairs().unwrap();
        assert_eq!(buckets[0].floats, vec![(0, 1.0), (60, 0.0)]);
        assert!(buckets[0].histograms.is_empty());
    }

    #[test]
    fn set_operators_keep_a_histogram_as_it_is() {
        let mut g = group(Op::And, false, Matching::default());
        g.absorb(LHS, labels("a", "x"), vec![(0, h(1.0)), (60, h(2.0))]);
        g.absorb(RHS, labels("b", "x"), vec![(60, Value::Float(9.0))]);
        let buckets = g.sets();
        assert_eq!(buckets.len(), 1);
        assert_eq!(buckets[0].labels, labels("a", "x"));
        assert_eq!(buckets[0].histograms.len(), 1);
        assert_eq!(buckets[0].histograms[0].1.count, 2.0);
    }

    #[test]
    fn the_matching_errors_are_the_float_pairings() {
        let mut g = group(Op::Add, false, Matching::default());
        g.absorb(LHS, labels("a", "x"), vec![(0, h(1.0))]);
        g.absorb(RHS, labels("b", "x"), vec![(0, h(1.0))]);
        g.absorb(RHS, labels("c", "x"), vec![(0, h(1.0))]);
        let err = g.pairs().unwrap_err().to_string();
        assert!(err.contains(super::super::DUPLICATE_SERIES), "{err}");
    }

    #[test]
    fn a_partial_state_merges_to_the_same_pairs() {
        let mut whole = group(Op::Add, false, Matching::default());
        let mut left = group(Op::Add, false, Matching::default());
        let mut right = group(Op::Add, false, Matching::default());
        for g in [&mut whole, &mut left] {
            g.absorb(
                LHS,
                labels("a", "x"),
                vec![(0, h(1.0)), (60, Value::Float(2.0))],
            );
        }
        for g in [&mut whole, &mut right] {
            g.absorb(
                RHS,
                labels("b", "x"),
                vec![(0, h(4.0)), (60, Value::Float(5.0))],
            );
        }
        let mut merged = group(Op::Add, false, Matching::default());
        for part in [&mut left, &mut right] {
            let state: Vec<ArrayRef> = part
                .state()
                .unwrap()
                .into_iter()
                .map(|s| s.to_array().unwrap())
                .collect();
            merged.merge_batch(&state).unwrap();
        }
        let (want, got) = (whole.pairs().unwrap(), merged.pairs().unwrap());
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].floats, want[0].floats);
        assert_eq!(got[0].floats, vec![(60, 7.0)]);
        assert_eq!(got[0].histograms[0].1.count, 5.0);
    }
}
