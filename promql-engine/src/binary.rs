//! The binary operators between two vectors, and the matching that
//! decides which series pair, as an aggregate function:
//! `promql_binary(samples, __rhs, labels, '+', 'on(job) group_left()', start, end, step)`.
//!
//! [`Op`] is the arithmetic itself, upstream's `vectorElemBinop`
//! (`promql/engine.go:3257` at 83962c35) restricted to the float/float
//! arms. It is shared by all three shapes the operators take: a scalar
//! on both sides folds through [`crate::scalar`], a scalar on one side
//! rides the elementwise kernel, and two vectors come here.
//!
//! # Why an aggregation and not a join
//!
//! Matching pairs a left series with a right one when their labels agree
//! on the match signature — the `on` labels, or everything but
//! `__name__` and the `ignoring` ones. Grouping both sides by that
//! signature gives the pairing, the duplicate detection and the
//! `group_left` fan-out in a single node, where a join would give the
//! pairing and still need an aggregation to find the duplicates, which
//! upstream reports per step rather than per series.
//!
//! # What a group holds
//!
//! One side of a match is the "one": at most one of its series may be
//! in a group at any step, and [`Pairing`] keeps it as lanes over the
//! step grid, a count and a value per step. The other side is the
//! "many" — for `group_left` it really is many — so its series are kept
//! one lane pair each. That is the cost of this shape: a group as wide
//! as a `group_left` fan-out holds a lane per series of it.
//!
//! # Why the result is a list of series
//!
//! A match group is not one output series. `group_left` answers with a
//! series per left-hand one, and even plain one-to-one splits when the
//! operator keeps `__name__`, which is exactly the label the signature
//! forgets. So the aggregation builds the result label set upstream's
//! `resultMetric` would, groups the samples under it, and hands back a
//! `(labels, samples)` pair per distinct one; the planner unnests that
//! into a row each. Two series of the "many" side reaching the same
//! result labels at one step is upstream's matching error, in its own
//! words — which words depends on the cardinality.

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, Datum, Float64Array, Int32Array, ListArray,
    StringArray, StringBuilder, StringViewArray, StringViewBuilder, StructArray,
    TimestampMillisecondArray, UInt8Array,
};
use datafusion::arrow::buffer::{BooleanBuffer, OffsetBuffer};
use datafusion::arrow::compute::kernels::numeric;
use datafusion::arrow::compute::{binary, FilterBuilder};
use datafusion::arrow::datatypes::{
    DataType, Field, FieldRef, Fields, Float64Type, Int32Type, UInt8Type,
};
use datafusion::arrow::error::ArrowError;
use datafusion::common::{plan_err, ScalarValue};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::utils::format_state_name;
use datafusion::logical_expr::{
    lit, Accumulator, AggregateUDF, AggregateUDFImpl, EmitTo, Expr, GroupsAccumulator, Signature,
    Volatility,
};
use datafusion::physical_expr::expressions::Literal;
use promql_parser::token::ItemType;

use crate::aggregate::Grid;
use crate::matcher::METRIC_NAME;
use crate::series;

pub const NAME: &str = "promql_binary";

/// The column telling the aggregation which operand a row came from.
///
/// Both sides are unioned into one table so that a single grouping
/// finds the match groups; this is the only thing that survives of
/// which side a series was on.
pub const SIDE: &str = "__rhs";

/// The two sides, as the [`SIDE`] flag spells them.
const LHS: usize = 0;
const RHS: usize = 1;

/// Every binary operator between two vectors.
///
/// The set operators sit here with the rest even though they never read
/// a value: they match on the same signature, so they are the same
/// grouping with a different rule for which samples come out of it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Op {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Pow,
    Atan2,
    Eql,
    Neq,
    Gtr,
    Lss,
    Gte,
    Lte,
    And,
    Or,
    Unless,
}

impl Op {
    /// Every operator, for the round trips that have to name them all.
    pub const ALL: [Op; 16] = [
        Op::Add,
        Op::Sub,
        Op::Mul,
        Op::Div,
        Op::Mod,
        Op::Pow,
        Op::Atan2,
        Op::Eql,
        Op::Neq,
        Op::Gtr,
        Op::Lss,
        Op::Gte,
        Op::Lte,
        Op::And,
        Op::Or,
        Op::Unless,
    ];

    pub fn from_token(op: ItemType) -> Option<Op> {
        Some(match op {
            ItemType::Add => Op::Add,
            ItemType::Sub => Op::Sub,
            ItemType::Mul => Op::Mul,
            ItemType::Div => Op::Div,
            ItemType::Mod => Op::Mod,
            ItemType::Pow => Op::Pow,
            ItemType::Atan2 => Op::Atan2,
            ItemType::EqlC => Op::Eql,
            ItemType::Neq => Op::Neq,
            ItemType::Gtr => Op::Gtr,
            ItemType::Lss => Op::Lss,
            ItemType::Gte => Op::Gte,
            ItemType::Lte => Op::Lte,
            ItemType::Land => Op::And,
            ItemType::Lor => Op::Or,
            ItemType::Lunless => Op::Unless,
            _ => return None,
        })
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Op::Add => "+",
            Op::Sub => "-",
            Op::Mul => "*",
            Op::Div => "/",
            Op::Mod => "%",
            Op::Pow => "^",
            Op::Atan2 => "atan2",
            Op::Eql => "==",
            Op::Neq => "!=",
            Op::Gtr => ">",
            Op::Lss => "<",
            Op::Gte => ">=",
            Op::Lte => "<=",
            Op::And => "and",
            Op::Or => "or",
            Op::Unless => "unless",
        }
    }

    /// The operators upstream's `IsSetOperator` answers for. They read
    /// no value at all: which samples come out is decided by which
    /// signatures the other side has at that step.
    pub fn is_set(&self) -> bool {
        matches!(self, Op::And | Op::Or | Op::Unless)
    }

    /// The operators upstream's `IsComparisonOperator` answers for.
    /// They filter rather than compute: the sample keeps its own value
    /// and is dropped where the comparison does not hold.
    pub fn is_comparison(&self) -> bool {
        matches!(
            self,
            Op::Eql | Op::Neq | Op::Gtr | Op::Lss | Op::Gte | Op::Lte
        )
    }

    /// Whether the result stops being the metric it was computed from.
    ///
    /// Upstream's `changesMetricSchema` (`promql/engine.go:4208` at
    /// 83962c35) names the arithmetic operators and nothing else, so a
    /// comparison keeps `__name__` — it answers with a sample that was
    /// already there. `bool` replaces the value with a 1 or a 0, which
    /// is no longer that metric either, and drops it too.
    pub fn drops_metric_name(&self, return_bool: bool) -> bool {
        match self {
            // A set operator hands a sample back untouched, so there is
            // nothing for `changesMetricSchema` to be true of.
            op if op.is_set() => false,
            op if op.is_comparison() => return_bool,
            _ => true,
        }
    }

    /// One pair of floats, upstream's `vectorElemBinop` with the
    /// `returnBool` wrapper its callers apply: `None` is the sample
    /// upstream leaves out of the result.
    ///
    /// Rust's `%` is Go's `math.Mod` — the remainder takes the sign of
    /// the dividend — and `powf` is `math.Pow`; the division by zero
    /// that yields an infinity or a NaN is IEEE in both languages, so
    /// none of these needs a Go-shaped wrapper the way `clamp`'s
    /// `math.Min` did. A NaN compares false to everything, in Go and in
    /// Rust alike, so it is simply filtered away.
    pub fn value(&self, lhs: f64, rhs: f64, return_bool: bool) -> Option<f64> {
        debug_assert!(!self.is_set(), "a set operator never combines two values");
        if !self.is_comparison() {
            return Some(match self {
                Op::Add => lhs + rhs,
                Op::Sub => lhs - rhs,
                Op::Mul => lhs * rhs,
                Op::Div => lhs / rhs,
                Op::Mod => lhs % rhs,
                Op::Pow => lhs.powf(rhs),
                Op::Atan2 => lhs.atan2(rhs),
                _ => unreachable!("every comparison is handled below, every set operator above"),
            });
        }
        let keep = self.compare(lhs, rhs);
        match (return_bool, keep) {
            (true, keep) => Some(if keep { 1.0 } else { 0.0 }),
            (false, true) => Some(lhs),
            (false, false) => None,
        }
    }

    /// Whether the comparison holds. Meaningless for the arithmetic
    /// operators, which never ask.
    pub fn compare(&self, lhs: f64, rhs: f64) -> bool {
        match self {
            Op::Eql => lhs == rhs,
            Op::Neq => lhs != rhs,
            Op::Gtr => lhs > rhs,
            Op::Lss => lhs < rhs,
            Op::Gte => lhs >= rhs,
            Op::Lte => lhs <= rhs,
            _ => unreachable!("only a comparison compares"),
        }
    }

    /// [`Op::value`] over whole lanes, for the two shapes with a vector
    /// in them. It must stay bit for bit what `value` answers, pair by
    /// pair; the tests hold it to that.
    ///
    /// The arithmetic is Arrow's `numeric` kernels, which for floats run
    /// the `_wrapping` ops (`float_op`, arrow-arith 58.4.0), and for
    /// `f64` those are the bare IEEE `+ - * / %`. The `_checked` ones
    /// would refuse the division by zero that Go answers with an
    /// infinity or a NaN.
    ///
    /// A comparison answers with where it holds rather than with values:
    /// the value a filtering comparison keeps is the caller's to name.
    pub fn kernel(&self, lhs: Operand, rhs: Operand, return_bool: bool) -> Result<Computed> {
        if let (Operand::Lane(l), Operand::Lane(r)) = (lhs, rhs) {
            if l.len() != r.len() {
                return Err(DataFusionError::Internal(format!(
                    "{NAME}: lanes of {} and {} values",
                    l.len(),
                    r.len()
                )));
            }
        }
        let holds = match self {
            Op::Add => return arithmetic(numeric::add, lhs, rhs),
            Op::Sub => return arithmetic(numeric::sub, lhs, rhs),
            Op::Mul => return arithmetic(numeric::mul, lhs, rhs),
            Op::Div => return arithmetic(numeric::div, lhs, rhs),
            Op::Mod => return arithmetic(numeric::rem, lhs, rhs),
            Op::Pow => return native(lhs, rhs, f64::powf),
            Op::Atan2 => return native(lhs, rhs, f64::atan2),
            // Not `arrow::compute::kernels::cmp`: it orders floats by
            // `total_cmp`, so there NaN == NaN and -0 < +0. Go's `==`
            // and `<`, like Rust's, have a NaN compare false to
            // everything and the two zeros equal, and the corpus
            // depends on both.
            Op::Eql => holds(lhs, rhs, |l, r| l == r),
            Op::Neq => holds(lhs, rhs, |l, r| l != r),
            Op::Gtr => holds(lhs, rhs, |l, r| l > r),
            Op::Lss => holds(lhs, rhs, |l, r| l < r),
            Op::Gte => holds(lhs, rhs, |l, r| l >= r),
            Op::Lte => holds(lhs, rhs, |l, r| l <= r),
            Op::And | Op::Or | Op::Unless => {
                return Err(DataFusionError::Internal(format!(
                    "{NAME}: {} keeps samples and computes no value",
                    self.as_str()
                )))
            }
        };
        Ok(match return_bool {
            true => Computed::Values(Float64Array::from_iter_values(holds.iter().map(f64::from))),
            false => Computed::Holds(holds),
        })
    }
}

/// One side of [`Op::kernel`]: a lane of values, or the scalar a
/// `vector op scalar` folded while planning, standing at every position.
/// Two scalars answer with a lane of one.
#[derive(Debug, Clone, Copy)]
pub enum Operand<'a> {
    Lane(&'a Float64Array),
    Scalar(f64),
}

/// What [`Op::kernel`] answers with, position by position.
#[derive(Debug)]
pub enum Computed {
    Values(Float64Array),
    /// Where a filtering comparison holds. Between vectors the sample
    /// kept is the left one; against a scalar it is the vector's,
    /// whichever side the scalar was written on.
    Holds(BooleanBuffer),
}

type NumericKernel = fn(&dyn Datum, &dyn Datum) -> std::result::Result<ArrayRef, ArrowError>;

fn arithmetic(kernel: NumericKernel, lhs: Operand, rhs: Operand) -> Result<Computed> {
    let scalar = Float64Array::new_scalar;
    let out = match (lhs, rhs) {
        (Operand::Lane(l), Operand::Lane(r)) => kernel(l, r),
        (Operand::Lane(l), Operand::Scalar(r)) => kernel(l, &scalar(r)),
        (Operand::Scalar(l), Operand::Lane(r)) => kernel(&scalar(l), r),
        (Operand::Scalar(l), Operand::Scalar(r)) => kernel(&scalar(l), &scalar(r)),
    }?;
    Ok(Computed::Values(out.as_primitive::<Float64Type>().clone()))
}

/// The operators Arrow has no kernel for, through its arity kernels.
/// `f` is a function item, not a pointer, so each operator gets a loop
/// of its own with the call inlined.
fn native(lhs: Operand, rhs: Operand, f: impl Fn(f64, f64) -> f64) -> Result<Computed> {
    Ok(Computed::Values(match (lhs, rhs) {
        (Operand::Lane(l), Operand::Lane(r)) => binary::<_, _, _, Float64Type>(l, r, f)?,
        (Operand::Lane(l), Operand::Scalar(r)) => l.unary::<_, Float64Type>(|l| f(l, r)),
        (Operand::Scalar(l), Operand::Lane(r)) => r.unary::<_, Float64Type>(|r| f(l, r)),
        (Operand::Scalar(l), Operand::Scalar(r)) => Float64Array::from_value(f(l, r), 1),
    }))
}

/// Where `f` holds, 64 positions to a word, which is the shape LLVM
/// vectorises. The sample value column carries no nulls, so there is no
/// validity to fold in.
fn holds(lhs: Operand, rhs: Operand, f: impl Fn(f64, f64) -> bool) -> BooleanBuffer {
    match (lhs, rhs) {
        (Operand::Lane(l), Operand::Lane(r)) => {
            let (l, r) = (l.values(), r.values());
            BooleanBuffer::collect_bool(l.len(), |i| f(l[i], r[i]))
        }
        (Operand::Lane(l), Operand::Scalar(r)) => {
            let l = l.values();
            BooleanBuffer::collect_bool(l.len(), |i| f(l[i], r))
        }
        (Operand::Scalar(l), Operand::Lane(r)) => {
            let r = r.values();
            BooleanBuffer::collect_bool(r.len(), |i| f(l, r[i]))
        }
        (Operand::Scalar(l), Operand::Scalar(r)) => BooleanBuffer::collect_bool(1, |_| f(l, r)),
    }
}

/// How many series on each side one match may pair, upstream's
/// `VectorMatchCardinality` without the many-to-many the set operators
/// alone are.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Card {
    #[default]
    OneToOne,
    /// `group_left`: many on the left, one on the right.
    ManyToOne,
    /// `group_right`: one on the left, many on the right.
    OneToMany,
}

/// The `on`/`ignoring` and `group_left`/`group_right` modifiers, which
/// together decide the match signature and the result's labels.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Matching {
    pub card: Card,
    /// `on(…)` rather than `ignoring(…)`.
    pub on: bool,
    /// The labels named by whichever of the two was written.
    pub labels: Vec<String>,
    /// The `group_left(…)` labels, copied from the "one" side.
    pub include: Vec<String>,
    /// `fill_left(v)` and `fill_right(v)`: the value to stand in for a
    /// side that has no series under this signature at a step, instead
    /// of dropping the sample. `fill(v)` writes both.
    ///
    /// Left and right here are the operands as the query wrote them and
    /// *not* the many/one sides: upstream swaps the operands for
    /// `group_right` and leaves `FillValues` alone
    /// (`promql/engine.go:2976-2979` at 83962c35), so under
    /// `group_right` it is `fill_right` that stands in for the left
    /// operand. Mirrored, quirk and all.
    pub fill_lhs: Option<f64>,
    pub fill_rhs: Option<f64>,
}

impl Matching {
    /// The modifiers as the query spelled them, so that a plan reads
    /// back as PromQL. Default matching writes nothing at all.
    pub fn literal(&self) -> String {
        let mut out = String::new();
        if self.on || !self.labels.is_empty() {
            out.push_str(if self.on { "on(" } else { "ignoring(" });
            out.push_str(&self.labels.join(", "));
            out.push(')');
        }
        let group = match self.card {
            Card::OneToOne => None,
            Card::ManyToOne => Some("group_left("),
            Card::OneToMany => Some("group_right("),
        };
        if let Some(group) = group {
            if !out.is_empty() {
                out.push(' ');
            }
            out.push_str(group);
            out.push_str(&self.include.join(", "));
            out.push(')');
        }
        for (word, fill) in [
            ("fill_left(", self.fill_lhs),
            ("fill_right(", self.fill_rhs),
        ] {
            let Some(fill) = fill else { continue };
            if !out.is_empty() {
                out.push(' ');
            }
            out.push_str(word);
            // `{:?}` and not `{}`: the debug shape of an `f64` is the
            // shortest text that reads back as the same bits.
            out.push_str(&format!("{fill:?}"));
            out.push(')');
        }
        out
    }

    /// [`Matching::literal`] read back. Deliberately strict: anything
    /// this cannot spell is a plan it did not write.
    pub fn parse(s: &str) -> Option<Matching> {
        let mut out = Matching::default();
        let mut rest = s.trim();
        for (prefix, on) in [("on(", true), ("ignoring(", false)] {
            if let Some(tail) = rest.strip_prefix(prefix) {
                let (names, tail) = tail.split_once(')')?;
                out.on = on;
                out.labels = split_names(names);
                rest = tail.trim_start();
                break;
            }
        }
        for (prefix, card) in [
            ("group_left(", Card::ManyToOne),
            ("group_right(", Card::OneToMany),
        ] {
            if let Some(tail) = rest.strip_prefix(prefix) {
                let (names, tail) = tail.split_once(')')?;
                out.card = card;
                out.include = split_names(names);
                rest = tail.trim_start();
                break;
            }
        }
        for (prefix, side) in [("fill_left(", LHS), ("fill_right(", RHS)] {
            if let Some(tail) = rest.strip_prefix(prefix) {
                let (value, tail) = tail.split_once(')')?;
                let value = value.parse().ok()?;
                match side {
                    LHS => out.fill_lhs = Some(value),
                    _ => out.fill_rhs = Some(value),
                }
                rest = tail.trim_start();
            }
        }
        rest.is_empty().then_some(out)
    }

    /// The value to stand in for the side that holds at most one series
    /// per match group, and for the side that may hold many. See
    /// [`Matching::fill_lhs`] for why neither follows the swap.
    fn fill_one(&self) -> Option<f64> {
        self.fill_rhs
    }

    fn fill_many(&self) -> Option<f64> {
        self.fill_lhs
    }

    /// Which side of the union holds at most one series per group.
    fn one_side(&self) -> usize {
        match self.card {
            Card::OneToMany => LHS,
            _ => RHS,
        }
    }

    /// The word upstream puts in the duplicate message for that side.
    fn one_side_word(&self) -> &'static str {
        match self.card {
            Card::OneToMany => "left",
            _ => "right",
        }
    }
}

fn split_names(names: &str) -> Vec<String> {
    names
        .split(',')
        .map(str::trim)
        .filter(|n| !n.is_empty())
        .map(str::to_string)
        .collect()
}

/// The three ways a match fails, in upstream's words.
///
/// Exported because a UDAF can only fail with a `DataFusionError`, so
/// the engine has to recognise these on the way out to report them as
/// what they are: a query Prometheus also refuses, not a bug in here.
/// [`DUPLICATE_SERIES`] is only the fixed head of its message — the
/// rest names the group and the two series that collided.
pub const DUPLICATE_SERIES: &str = "found duplicate series for the match group";
pub const MULTIPLE_MATCHES: &str =
    "multiple matches for labels: many-to-one matching must be explicit (group_left/group_right)";
pub const GROUPING_NOT_UNIQUE: &str =
    "multiple matches for labels: grouping labels must ensure unique matches";

/// Whether a failure out of [`udaf`] is one of them.
pub fn is_matching_error(message: &str) -> bool {
    message.starts_with(DUPLICATE_SERIES)
        || message == MULTIPLE_MATCHES
        || message == GROUPING_NOT_UNIQUE
}

/// The result label set and the samples under it, the shape [`udaf`]
/// answers with.
pub const PAIR_LABELS: &str = "labels";
pub const PAIR_SAMPLES: &str = "samples";

fn pair_fields(names: &[String]) -> Fields {
    Fields::from(vec![
        Field::new(PAIR_LABELS, series::labels_type(names), false),
        Field::new(PAIR_SAMPLES, series::samples_type(), false),
    ])
}

fn pair_item(names: &[String]) -> FieldRef {
    Arc::new(Field::new(
        "item",
        DataType::Struct(pair_fields(names)),
        false,
    ))
}

/// What one match group answers with: the series it splits into.
pub fn output_type(names: &[String]) -> DataType {
    DataType::List(pair_item(names))
}

/// The operator as a plan carries it: its PromQL spelling, with the
/// modifier written in where the query had one. One literal rather than
/// two arguments, so a plan text reads back as the query it came from.
pub fn literal(op: Op, return_bool: bool) -> &'static str {
    if !return_bool {
        return op.as_str();
    }
    match op {
        Op::Eql => "== bool",
        Op::Neq => "!= bool",
        Op::Gtr => "> bool",
        Op::Lss => "< bool",
        Op::Gte => ">= bool",
        Op::Lte => "<= bool",
        // `bool` is only ever written on a comparison; upstream's
        // parser refuses it anywhere else.
        other => other.as_str(),
    }
}

/// [`literal`] read back.
pub fn parse_literal(s: &str) -> Option<(Op, bool)> {
    let (spelling, return_bool) = match s.strip_suffix(" bool") {
        Some(spelling) => (spelling, true),
        None => (s, false),
    };
    let op = Op::ALL.into_iter().find(|op| op.as_str() == spelling)?;
    // `1 + bool 2` is not a query upstream's parser accepts, so it is
    // not a plan this reads back either.
    if return_bool && !op.is_comparison() {
        return None;
    }
    Some((op, return_bool))
}

/// One series held whole: its labels, and what it put at each step of
/// the grid.
///
/// For the operators that combine two values this is the "many" side
/// and `side` is always the one the modifier allows to be many. A set
/// operator has no "one" side at all, so both operands land here and
/// `side` is what tells them apart.
#[derive(Debug)]
struct Many {
    side: usize,
    labels: Vec<String>,
    /// Saturating sample count per step. PromQL only distinguishes zero,
    /// one, and more than one, so one byte per step suffices.
    counts: Vec<u8>,
    values: Vec<f64>,
}

/// Lanes for the "one" side of a match group, allocated lazily only when a
/// series actually lands on that side. Set operators and partial groups that
/// saw only the "many" side never allocate or serialize these lanes.
#[derive(Debug)]
struct OneLanes {
    counts: Vec<u8>,
    values: Vec<f64>,
    /// Which of [`Pairing::one_pool`] the "one" side's sample at each
    /// step came from, `-1` where it put none. Kept `None` while at most
    /// one series has contributed to the "one" side, in which case every
    /// populated step belongs to `one_pool[0]`.
    rows: Option<Vec<i32>>,
}

/// One output series under construction: the operands it paired at
/// each step, for [`Op::kernel`] to compute once over every bucket. A
/// set operator computes nothing, so its buckets carry the kept sample
/// in `lhs` and leave `rhs` empty.
#[derive(Debug)]
struct Bucket {
    labels: Vec<String>,
    timestamps: Vec<i64>,
    lhs: Vec<f64>,
    rhs: Vec<f64>,
    /// The step this last took a sample at, which is how two series of
    /// the "many" side reaching it at once are caught.
    claimed: Option<usize>,
}

/// One match group: the "one" side as lanes, the "many" side as a
/// series each.
#[derive(Debug)]
struct Pairing {
    op: Op,
    return_bool: bool,
    matching: Arc<Matching>,
    grid: Grid,
    /// The label columns, in the order the input struct carries them,
    /// which is the order every label vector here is in.
    schema: Arc<[String]>,
    one: Option<OneLanes>,
    one_pool: Vec<Vec<String>>,
    many: Vec<Many>,
    /// The match group's own labels, for the message a duplicate has to
    /// name. Every row of the group renders the same text, so the first
    /// one to arrive settles it.
    group: Option<String>,
    /// Up to two label sets from the "one" side, again only for that
    /// message: upstream prints the pair that collided and there is
    /// nothing to say about a third.
    one_metrics: Vec<String>,
}

impl Pairing {
    fn new(op: Op, return_bool: bool, matching: Matching, grid: Grid, schema: Vec<String>) -> Self {
        Self::new_shared(op, return_bool, Arc::new(matching), grid, schema.into())
    }

    fn new_shared(
        op: Op,
        return_bool: bool,
        matching: Arc<Matching>,
        grid: Grid,
        schema: Arc<[String]>,
    ) -> Self {
        Self {
            op,
            return_bool,
            matching,
            grid,
            schema,
            one: None,
            one_pool: Vec::new(),
            many: Vec::new(),
            group: None,
            one_metrics: Vec::new(),
        }
    }

    fn intern_one(&mut self, labels: &[String]) -> i32 {
        if let Some(index) = self.one_pool.iter().position(|held| held == labels) {
            return index as i32;
        }
        self.one_pool.push(labels.to_vec());
        (self.one_pool.len() - 1) as i32
    }

    fn intern_many(&mut self, side: usize, labels: &[String]) -> usize {
        if let Some(index) = self
            .many
            .iter()
            .position(|held| held.side == side && held.labels == labels)
        {
            return index;
        }
        self.many.push(Many {
            side,
            labels: labels.to_vec(),
            counts: vec![0; self.grid.len()],
            values: vec![f64::NAN; self.grid.len()],
        });
        self.many.len() - 1
    }

    /// One input series folded into the group's lanes.
    fn absorb(
        &mut self,
        side: usize,
        labels: &[String],
        timestamps: &[i64],
        values: &[f64],
    ) -> Result<()> {
        // A filtering comparison upstream of a binary operator can leave a
        // series row with zero samples; ignore it so it does not allocate dead
        // lanes or appear in duplicate-series diagnostics.
        if timestamps.is_empty() {
            return Ok(());
        }
        let grid = self.grid;
        if !self.op.is_set() && side == self.matching.one_side() {
            self.remember(labels);
            let row = self.intern_one(labels);
            let steps = grid.len();
            let one = self.one.get_or_insert_with(|| OneLanes {
                counts: vec![0; steps],
                values: vec![f64::NAN; steps],
                rows: None,
            });
            if row != 0 && one.rows.is_none() {
                one.rows = Some(
                    one.counts
                        .iter()
                        .map(|&c| if c > 0 { 0 } else { -1 })
                        .collect(),
                );
            }
            return grid.runs(timestamps, |index, from, len| {
                for count in &mut one.counts[index..index + len] {
                    *count = count.saturating_add(1);
                }
                one.values[index..index + len].copy_from_slice(&values[from..from + len]);
                if let Some(rows) = &mut one.rows {
                    rows[index..index + len].fill(row);
                }
            });
        }
        let many = self.intern_many(side, labels);
        let series = &mut self.many[many];
        let (counts, lanes) = (&mut series.counts, &mut series.values);
        grid.runs(timestamps, |index, from, len| {
            for count in &mut counts[index..index + len] {
                *count = count.saturating_add(1);
            }
            lanes[index..index + len].copy_from_slice(&values[from..from + len]);
        })
    }

    /// Keep what a failure would have to quote.
    fn remember(&mut self, labels: &[String]) {
        if self.group.is_none() {
            self.group = Some(self.signature_text(labels));
        }
        if self.one_metrics.len() < 2 {
            let metric = print(&self.schema, labels, |_| true);
            if self.one_metrics.first() != Some(&metric) {
                self.one_metrics.push(metric);
            }
        }
    }

    /// Whether a label is part of the match signature, upstream's
    /// `MatchLabels`: the `on` labels, or everything but `__name__` and
    /// the `ignoring` ones.
    fn in_signature(&self, name: &str) -> bool {
        let m = &self.matching;
        if m.on {
            m.labels.iter().any(|l| l == name)
        } else {
            name != METRIC_NAME && !m.labels.iter().any(|l| l == name)
        }
    }

    /// The match group as upstream's `MatchLabels` renders it.
    fn signature_text(&self, labels: &[String]) -> String {
        print(&self.schema, labels, |name| self.in_signature(name))
    }

    /// A label set with only the signature left of it -- the whole of
    /// what upstream gives a filled-in sample (`MatchLabels` at
    /// `promql/engine.go:3100` and `:3119` at 83962c35), since there is
    /// no series it could have come from.
    fn signature_of(&self, labels: &[String]) -> Vec<String> {
        labels
            .iter()
            .zip(self.schema.iter())
            .map(|(value, name)| match self.in_signature(name) {
                true => value.clone(),
                false => String::new(),
            })
            .collect()
    }

    /// Upstream's `resultMetric` (`promql/engine.go:3132` at 83962c35),
    /// in its order: the metadata labels go first where the operator
    /// changed what the metric means, then one-to-one narrows to the
    /// match labels, then the `group_x` labels are copied over from the
    /// "one" side — or deleted where it has none.
    fn result_labels(&self, base: &[String], one: &[String]) -> Vec<String> {
        let mut out = base.to_vec();
        // A set operator answers with the sample it was handed, so its
        // labels are the ones it arrived with -- `resultMetric` is not
        // on that path at all (`promql/engine.go:2888-2961` at
        // 83962c35).
        if self.op.is_set() {
            return out;
        }
        let m = &self.matching;
        for (index, name) in self.schema.iter().enumerate() {
            // `on` keeps only what it names and `ignoring` drops only
            // what it names, which is one test: the label goes where
            // being named is not what the modifier wanted.
            let drop = (name == METRIC_NAME && self.op.drops_metric_name(self.return_bool))
                || (m.card == Card::OneToOne && m.on != m.labels.iter().any(|l| l == name));
            if drop {
                out[index].clear();
            }
        }
        for name in &m.include {
            if let Some(index) = self.schema.iter().position(|held| held == name) {
                out[index] = one[index].clone();
            }
        }
        out
    }

    /// Upstream's message for two series on the "one" side of a match
    /// (`promql/engine.go:2999` at 83962c35).
    ///
    /// The pair inside the brackets is the first two label sets this
    /// group saw on that side, which is arrival order and not
    /// upstream's pair: upstream quotes the series that collided and
    /// the one it collided with at that step, and once three or more
    /// share a match group those need not be these two. It is the
    /// message text and nothing else, so the tests assert the sentence
    /// around the brackets rather than what is in them — DataFusion is
    /// free to hand the rows over in any order, and an assertion on the
    /// pair would be a flake waiting to happen.
    fn duplicate(&self) -> DataFusionError {
        let group = self.group.as_deref().unwrap_or("{}");
        let side = self.matching.one_side_word();
        // Upstream names the series it collided on first and the one
        // already held second; a side with fewer than two is out of
        // reach here, and an empty string is all there would be to say.
        let second = self.one_metrics.get(1).map(String::as_str).unwrap_or("");
        let first = self.one_metrics.first().map(String::as_str).unwrap_or("");
        DataFusionError::Execution(format!(
            "{DUPLICATE_SERIES} {group} on the {side} hand-side of the \
             operation: [{second}, {first}];many-to-many matching not allowed: matching labels \
             must be unique on one side"
        ))
    }

    /// Copy every populated sample from `target_side` directly into sorted
    /// output buckets when the opposing side of `unless` or `or` is absent
    /// across the whole match group.
    fn copy_side_buckets(&self, target_side: usize) -> Vec<Bucket> {
        let steps = self.grid.len();
        let mut buckets: Vec<Bucket> = Vec::with_capacity(self.many.len());
        for series in &self.many {
            if series.side != target_side {
                continue;
            }
            let kept = series.counts.iter().filter(|&&c| c > 0).count();
            if kept == 0 {
                continue;
            }
            let mut timestamps = Vec::with_capacity(kept);
            let mut lhs = Vec::with_capacity(kept);
            for step in 0..steps {
                if series.counts[step] > 0 {
                    timestamps.push(self.grid.timestamp(step));
                    lhs.push(series.values[step]);
                }
            }
            buckets.push(Bucket {
                labels: series.labels.clone(),
                timestamps,
                lhs,
                rhs: Vec::new(),
                claimed: None,
            });
        }
        buckets.sort_by(|a, b| a.labels.cmp(&b.labels));
        buckets
    }

    /// `and`, `or` and `unless` over the same grid: upstream's
    /// `VectorAnd` / `VectorOr` / `VectorUnless` (`promql/engine.go:2888`
    /// at 83962c35), which ask nothing of a sample but whether the other
    /// side has this signature at this step.
    ///
    /// Every sample that survives keeps its own labels and its own
    /// value, so two of them can only ever land in one output series if
    /// their label sets are equal -- and equal labels are the same
    /// signature, which for `or` means the right-hand one was dropped.
    /// That is why upstream's "vector cannot contain metrics with the
    /// same labelset" has no way to happen here, and why merging by
    /// label set is safe.
    fn sets(&self) -> Vec<Bucket> {
        let mut lhs_count = 0usize;
        let mut rhs_count = 0usize;
        for series in &self.many {
            if series.side == LHS {
                lhs_count += 1;
            } else {
                rhs_count += 1;
            }
        }
        // Skip the per-step scan when an entire side of the match group is empty.
        match self.op {
            Op::And if lhs_count == 0 || rhs_count == 0 => return Vec::new(),
            Op::Unless if lhs_count == 0 => return Vec::new(),
            Op::Unless if rhs_count == 0 => return self.copy_side_buckets(LHS),
            Op::Or if lhs_count == 0 && rhs_count == 0 => return Vec::new(),
            Op::Or if rhs_count == 0 => return self.copy_side_buckets(LHS),
            Op::Or if lhs_count == 0 => return self.copy_side_buckets(RHS),
            _ => {}
        }

        // Resolve each series to its output bucket once up front so the step
        // loop indexes into `buckets` without hashing label vectors per step.
        let bucket_cap = if self.op == Op::Or {
            self.many.len()
        } else {
            lhs_count
        };
        let mut buckets: Vec<Bucket> = Vec::with_capacity(bucket_cap);
        let mut by_labels: HashMap<&[String], usize> = HashMap::with_capacity(bucket_cap);
        let mut series_bucket: Vec<usize> = Vec::with_capacity(self.many.len());
        for series in &self.many {
            if self.op != Op::Or && series.side == RHS {
                series_bucket.push(usize::MAX);
                continue;
            }
            let cap = series.counts.iter().filter(|&&c| c > 0).count();
            let idx = *by_labels
                .entry(series.labels.as_slice())
                .or_insert_with(|| {
                    buckets.push(Bucket {
                        labels: series.labels.clone(),
                        timestamps: Vec::with_capacity(cap),
                        lhs: Vec::with_capacity(cap),
                        rhs: Vec::new(),
                        claimed: None,
                    });
                    buckets.len() - 1
                });
            series_bucket.push(idx);
        }

        for step in 0..self.grid.len() {
            let present = |side: usize| {
                self.many
                    .iter()
                    .any(|series| series.side == side && series.counts[step] > 0)
            };
            let (left, right) = (present(LHS), present(RHS));
            for (idx, series) in self.many.iter().enumerate() {
                if series.counts[step] == 0 {
                    continue;
                }
                let keep = match self.op {
                    Op::And => series.side == LHS && right,
                    Op::Unless => series.side == LHS && !right,
                    // Everything on the left, and the right only where
                    // the left put nothing under this signature.
                    Op::Or => series.side == LHS || !left,
                    _ => unreachable!("only a set operator walks this grid"),
                };
                if !keep {
                    continue;
                }
                let b = &mut buckets[series_bucket[idx]];
                b.timestamps.push(self.grid.timestamp(step));
                b.lhs.push(series.values[step]);
            }
        }

        buckets.retain(|bucket| !bucket.timestamps.is_empty());
        buckets.sort_by(|a, b| a.labels.cmp(&b.labels));
        buckets
    }

    /// The step grid walked once, in upstream's order: a duplicate on
    /// the "one" side is reported before a "many" side that matched
    /// twice, and both before any value is emitted.
    ///
    /// A step whose "many" side is empty and has no fill to stand in
    /// for it is skipped rather than checked, which is where this parts
    /// company with upstream: there the short-circuit is on the whole
    /// vector (`promql/engine.go:2968-2971` at 83962c35), so a
    /// duplicate in *this* match group still fails the query as long as
    /// some other group had a sample on the "many" side at that step.
    /// Seeing that would take a second pass over every group. With a
    /// fill the step is not empty -- the fill is the missing side --
    /// so it is walked like any other.
    fn pairs(&self) -> Result<Vec<Bucket>> {
        if self.op.is_set() {
            return Ok(self.sets());
        }
        let (fill_one, fill_many) = (self.matching.fill_one(), self.matching.fill_many());
        // Short-circuit match groups where a required side never arrived.
        if self.many.is_empty() && fill_one.is_none() && fill_many.is_none() {
            return Ok(Vec::new());
        }
        if self.one.is_none() && fill_one.is_none() {
            return Ok(Vec::new());
        }

        // Fast path for the common case of one series on each side and no fill
        // modifiers, avoiding per-step worklists and label hash lookups.
        if fill_one.is_none() && fill_many.is_none() && self.many.len() == 1 {
            if let Some(one) = &self.one {
                if one.rows.is_none() {
                    if let Some(one_labels) = self.one_pool.first() {
                        let many = &self.many[0];
                        let steps = self.grid.len();
                        let mut paired = 0usize;
                        for step in 0..steps {
                            if many.counts[step] > 0 {
                                let oc = one.counts[step];
                                if oc > 1 {
                                    return Err(self.duplicate());
                                }
                                if oc == 1 {
                                    paired += 1;
                                }
                            }
                        }
                        if paired == 0 {
                            return Ok(Vec::new());
                        }
                        let mut timestamps = Vec::with_capacity(paired);
                        let mut lhs = Vec::with_capacity(paired);
                        let mut rhs = Vec::with_capacity(paired);
                        let one_is_lhs = self.matching.one_side() == LHS;
                        for step in 0..steps {
                            if many.counts[step] > 0 && one.counts[step] == 1 {
                                timestamps.push(self.grid.timestamp(step));
                                let (mv, ov) = (many.values[step], one.values[step]);
                                let (l, r) = if one_is_lhs { (ov, mv) } else { (mv, ov) };
                                lhs.push(l);
                                rhs.push(r);
                            }
                        }
                        return Ok(vec![Bucket {
                            labels: self.result_labels(&many.labels, one_labels),
                            timestamps,
                            lhs,
                            rhs,
                            claimed: None,
                        }]);
                    }
                }
            }
        }

        let mut buckets: Vec<Bucket> = Vec::new();
        let mut by_labels: HashMap<Vec<String>, usize> = HashMap::new();
        let mut by_pair: HashMap<(Option<usize>, i32), usize> = HashMap::new();
        // Direct bucket lookup table for `one_row == 0` indexed by `many`.
        let mut row0_buckets: Vec<usize> = vec![usize::MAX; self.many.len()];
        let one_to_one = self.matching.card == Card::OneToOne;
        let nothing: Vec<String> = vec![String::new(); self.schema.len()];
        // Reuse `todo` across steps instead of allocating a new `Vec` per step.
        let mut todo: Vec<(Option<usize>, f64, f64)> = Vec::with_capacity(self.many.len().max(1));

        for step in 0..self.grid.len() {
            let any_many = self.many.iter().any(|series| series.counts[step] > 0);
            let one_count = self.one.as_ref().map_or(0, |o| o.counts[step]);
            let one_here = one_count > 0;
            if !any_many && fill_one.is_none() && fill_many.is_none() {
                continue;
            }
            if one_count > 1 {
                return Err(self.duplicate());
            }

            // What pairs at this step: the "many" row, or `None` where
            // the fill value stands in for a side with no series here.
            todo.clear();
            let one_step_val = self
                .one
                .as_ref()
                .map(|o| o.values[step])
                .unwrap_or(f64::NAN);
            if let Some(one_value) = one_here.then_some(one_step_val).or(fill_one) {
                for many in 0..self.many.len() {
                    if self.many[many].counts[step] > 0 {
                        todo.push((Some(many), self.many[many].values[step], one_value));
                    }
                }
            }
            // Upstream's second pass: a "one" side nothing matched, and
            // a fill to stand in for the side that would have matched
            // it (`promql/engine.go:3106-3124` at 83962c35).
            if todo.is_empty() && one_here {
                if let Some(fill) = fill_many {
                    todo.push((None, fill, one_step_val));
                }
            }
            if todo.is_empty() {
                continue;
            }
            // One to one is the cardinality that says there is nothing
            // to fan out over, so a second series here is the query
            // asking for a fan-out without saying so.
            if one_to_one && todo.len() > 1 {
                return Err(DataFusionError::Execution(MULTIPLE_MATCHES.into()));
            }

            let one_row = if one_here {
                self.one
                    .as_ref()
                    .and_then(|o| o.rows.as_ref().map(|r| r[step]))
                    .unwrap_or(0)
            } else {
                -1
            };
            let one_labels = match one_row {
                -1 => &nothing,
                row => &self.one_pool[row as usize],
            };
            for &(many, many_value, one_value) in &todo {
                let cached = match (many, one_row) {
                    (Some(m), 0) if row0_buckets[m] != usize::MAX => Some(row0_buckets[m]),
                    _ => by_pair.get(&(many, one_row)).copied(),
                };
                let bucket = match cached {
                    Some(bucket) => bucket,
                    None => {
                        // A filled-in side carries the signature and
                        // nothing else, on whichever side it stands in.
                        let base = match many {
                            Some(m) => self.many[m].labels.clone(),
                            None => self.signature_of(one_labels),
                        };
                        let one = match many {
                            Some(_) if !one_here => self.signature_of(&base),
                            _ => one_labels.clone(),
                        };
                        let labels = self.result_labels(&base, &one);
                        let cap = match many {
                            Some(m) if fill_one.is_none() => {
                                self.many[m].counts.iter().filter(|&&c| c > 0).count()
                            }
                            _ => self.grid.len(),
                        };
                        let bucket = *by_labels.entry(labels.clone()).or_insert_with(|| {
                            buckets.push(Bucket {
                                labels,
                                timestamps: Vec::with_capacity(cap),
                                lhs: Vec::with_capacity(cap),
                                rhs: Vec::with_capacity(cap),
                                claimed: None,
                            });
                            buckets.len() - 1
                        });
                        if let (Some(m), 0) = (many, one_row) {
                            row0_buckets[m] = bucket;
                        } else {
                            by_pair.insert((many, one_row), bucket);
                        }
                        bucket
                    }
                };
                // The claim happens before the comparison is applied,
                // because upstream checks the match before it decides
                // whether the sample survives the operator.
                if buckets[bucket].claimed == Some(step) {
                    return Err(DataFusionError::Execution(GROUPING_NOT_UNIQUE.into()));
                }
                buckets[bucket].claimed = Some(step);

                let (lhs, rhs) = match self.matching.one_side() {
                    LHS => (one_value, many_value),
                    _ => (many_value, one_value),
                };
                let bucket = &mut buckets[bucket];
                bucket.timestamps.push(self.grid.timestamp(step));
                bucket.lhs.push(lhs);
                bucket.rhs.push(rhs);
            }
        }

        buckets.retain(|bucket| !bucket.timestamps.is_empty());
        // Sorted, because DataFusion is free to hand the rows of a
        // group over in any order and the answer must not be.
        buckets.sort_by(|a, b| a.labels.cmp(&b.labels));
        Ok(buckets)
    }
}

fn corrupt(what: &str) -> DataFusionError {
    DataFusionError::Internal(format!("{NAME}: partial state holds {what}"))
}

impl Pairing {
    /// Approximate heap bytes owned uniquely by this group, excluding the
    /// `Arc`-shared `matching` and `schema` allocations.
    fn heap_size(&self) -> usize {
        let one_bytes = match &self.one {
            Some(one) => {
                one.counts.capacity() * std::mem::size_of::<u8>()
                    + one.values.capacity() * std::mem::size_of::<f64>()
                    + one
                        .rows
                        .as_ref()
                        .map_or(0, |r| r.capacity() * std::mem::size_of::<i32>())
            }
            None => 0,
        };
        let many_lane = self.grid.len() * (std::mem::size_of::<u8>() + std::mem::size_of::<f64>());
        one_bytes
            + self.many.capacity() * std::mem::size_of::<Many>()
            + self.many.len() * many_lane
            + (self.many.len() + self.one_pool.capacity())
                * self.schema.len()
                * std::mem::size_of::<String>()
            + self.one_metrics.capacity() * std::mem::size_of::<String>()
            + self.group.as_ref().map_or(0, String::capacity)
    }

    /// Merge row `row` of `parsed` partial-state columns into this group.
    fn merge_row(&mut self, parsed: &ParsedStates<'_>, row: usize) -> Result<()> {
        let steps = self.grid.len();
        let width = self.schema.len();

        let (oc_lo, oc_hi) = parsed.one_counts.range(row);
        let (ov_lo, ov_hi) = parsed.one_values.range(row);
        let (or_lo, or_hi) = parsed.one_rows.range(row);
        let counts = &parsed.one_counts.values[oc_lo..oc_hi];
        let values = &parsed.one_values.values[ov_lo..ov_hi];
        let rows = &parsed.one_rows.values[or_lo..or_hi];

        if !counts.is_empty() || !values.is_empty() || !rows.is_empty() {
            if counts.len() != steps
                || values.len() != steps
                || (!rows.is_empty() && rows.len() != steps)
            {
                return Err(DataFusionError::Internal(format!(
                    "{NAME}: partial state is {} steps, not the {steps} of this grid",
                    counts.len()
                )));
            }
            let (op_lo, op_hi) = parsed.one_pool.range(row);
            let pool = rectangle_slice(parsed.one_pool.values, op_lo, op_hi, width, NO_ROWS)?;
            let mut seen: Vec<i32> = Vec::with_capacity(pool.len());
            for labels in &pool {
                seen.push(self.intern_one(labels));
            }
            let one = self.one.get_or_insert_with(|| OneLanes {
                counts: vec![0; steps],
                values: vec![f64::NAN; steps],
                rows: None,
            });
            let need_rows = !rows.is_empty() || seen.first().copied().unwrap_or(0) != 0;
            if need_rows && one.rows.is_none() {
                one.rows = Some(
                    one.counts
                        .iter()
                        .map(|&c| if c > 0 { 0 } else { -1 })
                        .collect(),
                );
            }
            if rows.is_empty() {
                let mapped = seen.first().copied();
                for step in 0..steps {
                    let c = counts[step];
                    if c > 0 {
                        let dst = mapped
                            .ok_or_else(|| corrupt("a one-side sample with an empty pool"))?;
                        one.counts[step] = one.counts[step].saturating_add(c);
                        one.values[step] = values[step];
                        if let Some(dst_rows) = &mut one.rows {
                            dst_rows[step] = dst;
                        }
                    }
                }
            } else {
                let dst_rows = one.rows.as_mut().expect("materialized above");
                for step in 0..steps {
                    let c = counts[step];
                    if c > 0 {
                        let dst = usize::try_from(rows[step])
                            .ok()
                            .and_then(|i| seen.get(i).copied())
                            .ok_or_else(|| corrupt("a one-side row out of its pool"))?;
                        one.counts[step] = one.counts[step].saturating_add(c);
                        one.values[step] = values[step];
                        dst_rows[step] = dst;
                    }
                }
            }
        }

        let (mc_lo, mc_hi) = parsed.many_counts.range(row);
        let (mv_lo, mv_hi) = parsed.many_values.range(row);
        let (ms_lo, ms_hi) = parsed.many_sides.range(row);
        let m_counts = &parsed.many_counts.values[mc_lo..mc_hi];
        let m_values = &parsed.many_values.values[mv_lo..mv_hi];
        let m_sides = &parsed.many_sides.values[ms_lo..ms_hi];
        let series_rows = m_counts.len() / steps.max(1);
        if m_counts.len() != series_rows * steps || m_values.len() != m_counts.len() {
            return Err(corrupt("a many-side rectangle that is not rows by steps"));
        }
        if m_sides.len() != series_rows {
            return Err(corrupt("a side per series that is not one per series"));
        }
        if series_rows > 0 {
            let (ml_lo, ml_hi) = parsed.many_labels.range(row);
            let labels =
                rectangle_slice(parsed.many_labels.values, ml_lo, ml_hi, width, series_rows)?;
            for (index, labels) in labels.iter().enumerate() {
                let many = self.intern_many(m_sides[index] as usize, labels);
                let dst = &mut self.many[many];
                let base = index * steps;
                for step in 0..steps {
                    let c = m_counts[base + step];
                    if c > 0 {
                        dst.counts[step] = dst.counts[step].saturating_add(c);
                        dst.values[step] = m_values[base + step];
                    }
                }
            }
        }

        if let Some(metrics) = &parsed.one_metrics {
            let (lo, hi) = metrics.range(row);
            for i in lo..hi {
                let val = metrics.values.value(i);
                if self.one_metrics.len() < 2
                    && self.one_metrics.first().map(String::as_str) != Some(val)
                {
                    self.one_metrics.push(val.to_string());
                }
            }
        }
        if let Some(group) = parsed.group {
            if self.group.is_none() && group.is_valid(row) {
                self.group = Some(group.value(row).to_string());
            }
        }
        Ok(())
    }
}

impl Accumulator for Pairing {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let inputs = BatchInputs::parse(&self.schema, values)?;
        let mut row_labels: Vec<String> = Vec::with_capacity(self.schema.len());
        for row in 0..inputs.list.len() {
            if inputs.list.is_null(row) {
                continue;
            }
            let which = inputs.side_at(row);
            inputs.labels.read_into(row, &mut row_labels);
            let (lo, hi) = inputs.sample_range(row);
            self.absorb(
                which,
                &row_labels,
                &inputs.timestamps[lo..hi],
                &inputs.samples[lo..hi],
            )?;
        }
        Ok(())
    }

    /// The operands [`Pairing::pairs`] gathered, computed by one kernel
    /// call over every bucket end to end rather than once per step. A
    /// filtering comparison keeps the left operand where it holds, which
    /// may leave a bucket empty, and an empty bucket is no series.
    fn evaluate(&mut self) -> Result<ScalarValue> {
        let out = evaluate_groups(
            self.op,
            self.return_bool,
            &self.schema,
            std::slice::from_ref(self),
        )?;
        Ok(ScalarValue::List(Arc::new(out)))
    }

    /// Flat lists throughout: the "many" side is a rectangle of rows by
    /// steps, and the label sets a rectangle of rows by columns, so a
    /// partial travels as the same handful of primitive lanes whatever
    /// the query's label schema is.
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let arrays = emit_groups_state(
            std::slice::from_ref(self),
            self.grid.len(),
            self.schema.len(),
        )?;
        let mut scalars = Vec::with_capacity(arrays.len());
        for array in &arrays[..STATE_LANES.len()] {
            scalars.push(ScalarValue::List(Arc::new(array.as_list::<i32>().clone())));
        }
        scalars.push(ScalarValue::Utf8(self.group.clone()));
        Ok(scalars)
    }

    /// Lanes added position by position, as in [`crate::aggregate`]. A
    /// value is only read where the total count is one, and then exactly
    /// one partial saw that sample, so any partial that saw something
    /// carries the value.
    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let parsed = ParsedStates::parse(states)?;
        for row in 0..parsed.num_rows {
            self.merge_row(&parsed, row)?;
        }
        Ok(())
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>() + self.heap_size()
    }
}

/// Batched multi-group accumulator for [`Binary`] so DataFusion processes
/// input batches, partial state serialization, shuffles, and final evaluation
/// directly across all match groups without `ScalarValue` round-trips.
#[derive(Debug)]
struct GroupedBinary {
    op: Op,
    return_bool: bool,
    matching: Arc<Matching>,
    grid: Grid,
    schema: Arc<[String]>,
    groups: Vec<Pairing>,
    heap_size: usize,
}

impl GroupedBinary {
    fn from_pairing(template: Pairing) -> Self {
        Self {
            op: template.op,
            return_bool: template.return_bool,
            matching: template.matching,
            grid: template.grid,
            schema: template.schema,
            groups: Vec::new(),
            heap_size: 0,
        }
    }

    fn grow(&mut self, total_num_groups: usize) {
        if self.groups.len() < total_num_groups {
            let op = self.op;
            let return_bool = self.return_bool;
            let matching = &self.matching;
            let grid = self.grid;
            let schema = &self.schema;
            self.groups.resize_with(total_num_groups, || {
                Pairing::new_shared(
                    op,
                    return_bool,
                    Arc::clone(matching),
                    grid,
                    Arc::clone(schema),
                )
            });
        }
    }

    fn emitted(&self, emit_to: EmitTo) -> usize {
        match emit_to {
            EmitTo::All => self.groups.len(),
            EmitTo::First(n) => n.min(self.groups.len()),
        }
    }

    fn release(&mut self, emit_to: EmitTo) {
        match emit_to {
            EmitTo::All => {
                self.groups.clear();
                self.heap_size = 0;
            }
            EmitTo::First(n) => {
                let drained = n.min(self.groups.len());
                for group in self.groups.drain(..drained) {
                    self.heap_size -= group.heap_size();
                }
            }
        }
    }
}

impl GroupsAccumulator for GroupedBinary {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        self.grow(total_num_groups);
        let inputs = BatchInputs::parse(&self.schema, values)?;
        let mut row_labels: Vec<String> = Vec::with_capacity(self.schema.len());

        for (row, &group_idx) in group_indices.iter().enumerate() {
            if inputs.list.is_null(row)
                || opt_filter.is_some_and(|f| f.is_null(row) || !f.value(row))
            {
                continue;
            }
            let which = inputs.side_at(row);
            inputs.labels.read_into(row, &mut row_labels);
            let (lo, hi) = inputs.sample_range(row);
            let pairing = &mut self.groups[group_idx];
            let before = pairing.heap_size();
            let res = pairing.absorb(
                which,
                &row_labels,
                &inputs.timestamps[lo..hi],
                &inputs.samples[lo..hi],
            );
            self.heap_size = self.heap_size - before + pairing.heap_size();
            res?;
        }
        Ok(())
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let n = self.emitted(emit_to);
        let out = evaluate_groups(self.op, self.return_bool, &self.schema, &self.groups[..n])?;
        self.release(emit_to);
        Ok(Arc::new(out))
    }

    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        let n = self.emitted(emit_to);
        let out = emit_groups_state(&self.groups[..n], self.grid.len(), self.schema.len())?;
        self.release(emit_to);
        Ok(out)
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        total_num_groups: usize,
    ) -> Result<()> {
        self.grow(total_num_groups);
        let parsed = ParsedStates::parse(values)?;
        for (row, &group_idx) in group_indices.iter().enumerate() {
            let pairing = &mut self.groups[group_idx];
            let before = pairing.heap_size();
            let res = pairing.merge_row(&parsed, row);
            self.heap_size = self.heap_size - before + pairing.heap_size();
            res?;
        }
        Ok(())
    }

    /// Convert each input row directly into a single-row partial state when
    /// DataFusion bypasses high-cardinality partial aggregation.
    fn convert_to_state(
        &self,
        values: &[ArrayRef],
        opt_filter: Option<&BooleanArray>,
    ) -> Result<Vec<ArrayRef>> {
        let num_rows = values.first().map_or(0, Array::len);
        let mut temp = GroupedBinary {
            op: self.op,
            return_bool: self.return_bool,
            matching: Arc::clone(&self.matching),
            grid: self.grid,
            schema: Arc::clone(&self.schema),
            groups: Vec::with_capacity(num_rows),
            heap_size: 0,
        };
        let indices: Vec<usize> = (0..num_rows).collect();
        temp.update_batch(values, &indices, opt_filter, num_rows)?;
        temp.state(EmitTo::All)
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.groups.capacity() * std::mem::size_of::<Pairing>()
            + self.heap_size
    }
}

fn checked_i32_offset(offset: usize) -> Result<i32> {
    i32::try_from(offset).map_err(|_| {
        DataFusionError::Execution(format!("{NAME}: list offset {offset} exceeds i32::MAX"))
    })
}

/// Evaluate `groups` in one pass, running [`Op::kernel`] once across all
/// paired sample lanes and assembling one outer `ListArray` row per group.
fn evaluate_groups(
    op: Op,
    return_bool: bool,
    schema: &[String],
    groups: &[Pairing],
) -> Result<ListArray> {
    let mut group_buckets: Vec<Vec<Bucket>> = Vec::with_capacity(groups.len());
    let mut total_buckets = 0usize;
    let mut total_samples = 0usize;
    for group in groups {
        let buckets = group.pairs()?;
        for b in &buckets {
            total_samples += b.timestamps.len();
        }
        total_buckets += buckets.len();
        group_buckets.push(buckets);
    }

    let mut ts_vec: Vec<i64> = Vec::with_capacity(total_samples);
    let mut lhs_vec: Vec<f64> = Vec::with_capacity(total_samples);
    for buckets in &group_buckets {
        for b in buckets {
            ts_vec.extend_from_slice(&b.timestamps);
            lhs_vec.extend_from_slice(&b.lhs);
        }
    }
    let timestamps = TimestampMillisecondArray::from(ts_vec);
    let lhs = Float64Array::from(lhs_vec);

    let computed = if op.is_set() {
        Computed::Values(lhs.clone())
    } else {
        let mut rhs_vec: Vec<f64> = Vec::with_capacity(total_samples);
        for buckets in &group_buckets {
            for b in buckets {
                rhs_vec.extend_from_slice(&b.rhs);
            }
        }
        let rhs = Float64Array::from(rhs_vec);
        op.kernel(Operand::Lane(&lhs), Operand::Lane(&rhs), return_bool)?
    };

    let mut kept_buckets: Vec<&Bucket> = Vec::with_capacity(total_buckets);
    let mut sample_offsets: Vec<i32> = Vec::with_capacity(total_buckets + 1);
    sample_offsets.push(0);
    let mut group_offsets: Vec<i32> = Vec::with_capacity(groups.len() + 1);
    group_offsets.push(0);

    let (timestamps_arr, values_arr): (ArrayRef, ArrayRef) = match computed {
        Computed::Values(values) => {
            let mut sample_pos = 0usize;
            for buckets in &group_buckets {
                for b in buckets {
                    let len = b.timestamps.len();
                    if len > 0 {
                        kept_buckets.push(b);
                        sample_pos += len;
                        sample_offsets.push(checked_i32_offset(sample_pos)?);
                    }
                }
                group_offsets.push(checked_i32_offset(kept_buckets.len())?);
            }
            (Arc::new(timestamps), Arc::new(values))
        }
        Computed::Holds(holds) => {
            let mut from = 0usize;
            let mut sample_pos = 0usize;
            for buckets in &group_buckets {
                for b in buckets {
                    let len = b.timestamps.len();
                    let kept = holds.slice(from, len).count_set_bits();
                    from += len;
                    if kept > 0 {
                        kept_buckets.push(b);
                        sample_pos += kept;
                        sample_offsets.push(checked_i32_offset(sample_pos)?);
                    }
                }
                group_offsets.push(checked_i32_offset(kept_buckets.len())?);
            }
            let timestamps_ref: ArrayRef = Arc::new(timestamps);
            let lhs_ref: ArrayRef = Arc::new(lhs);
            let predicate = FilterBuilder::new(&BooleanArray::new(holds, None))
                .optimize()
                .build();
            (
                predicate.filter(&timestamps_ref)?,
                predicate.filter(&lhs_ref)?,
            )
        }
    };

    let entries = StructArray::new(
        series::sample_fields(),
        vec![timestamps_arr, values_arr],
        None,
    );
    let samples = ListArray::new(
        series::sample_item(),
        OffsetBuffer::new(sample_offsets.into()),
        Arc::new(entries),
        None,
    );

    // Rebuilt through `series::labels_type` rather than from the
    // input's own fields, so that the struct this hands back is the
    // one `promql_labels` would have built for the same names.
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
            let mut builder = StringViewBuilder::with_capacity(kept_buckets.len());
            for bucket in &kept_buckets {
                builder.append_value(bucket.labels[index].as_str());
            }
            Arc::new(builder.finish()) as ArrayRef
        })
        .collect();
    let labels = match fields.is_empty() {
        true => StructArray::new_empty_fields(kept_buckets.len(), None),
        false => StructArray::new(fields, columns, None),
    };

    let pairs = StructArray::new(
        pair_fields(schema),
        vec![Arc::new(labels), Arc::new(samples)],
        None,
    );
    Ok(ListArray::new(
        pair_item(schema),
        OffsetBuffer::new(group_offsets.into()),
        Arc::new(pairs),
        None,
    ))
}

/// Serialize `groups` into the ten partial-state columns declared by
/// [`Binary::state_fields`] without intermediate `ScalarValue` wrappers.
fn emit_groups_state(groups: &[Pairing], steps: usize, width: usize) -> Result<Vec<ArrayRef>> {
    let n = groups.len();
    let mut cap_one_steps = 0usize;
    let mut cap_one_rows = 0usize;
    let mut cap_one_pool = 0usize;
    let mut cap_one_metrics = 0usize;
    let mut cap_many_series = 0usize;
    for g in groups {
        if let Some(one) = &g.one {
            cap_one_steps += steps;
            if one.rows.is_some() {
                cap_one_rows += steps;
            }
        }
        cap_one_pool += g.one_pool.len() * width;
        cap_one_metrics += g.one_metrics.len();
        cap_many_series += g.many.len();
    }
    let cap_many_steps = cap_many_series * steps;
    let cap_many_labels = cap_many_series * width;

    let mut one_counts_vec: Vec<u8> = Vec::with_capacity(cap_one_steps);
    let mut one_values_vec: Vec<f64> = Vec::with_capacity(cap_one_steps);
    let mut one_steps_offsets: Vec<i32> = Vec::with_capacity(n + 1);
    one_steps_offsets.push(0);

    let mut one_rows_vec: Vec<i32> = Vec::with_capacity(cap_one_rows);
    let mut one_rows_offsets: Vec<i32> = Vec::with_capacity(n + 1);
    one_rows_offsets.push(0);

    let mut one_pool_builder = StringBuilder::with_capacity(cap_one_pool, cap_one_pool * 8);
    let mut one_pool_offsets: Vec<i32> = Vec::with_capacity(n + 1);
    one_pool_offsets.push(0);
    let mut one_pool_pos = 0usize;

    let mut one_metrics_builder =
        StringBuilder::with_capacity(cap_one_metrics, cap_one_metrics * 24);
    let mut one_metrics_offsets: Vec<i32> = Vec::with_capacity(n + 1);
    one_metrics_offsets.push(0);
    let mut one_metrics_pos = 0usize;

    let mut many_labels_builder =
        StringBuilder::with_capacity(cap_many_labels, cap_many_labels * 8);
    let mut many_labels_offsets: Vec<i32> = Vec::with_capacity(n + 1);
    many_labels_offsets.push(0);
    let mut many_labels_pos = 0usize;

    let mut many_counts_vec: Vec<u8> = Vec::with_capacity(cap_many_steps);
    let mut many_values_vec: Vec<f64> = Vec::with_capacity(cap_many_steps);
    let mut many_steps_offsets: Vec<i32> = Vec::with_capacity(n + 1);
    many_steps_offsets.push(0);

    let mut many_sides_vec: Vec<u8> = Vec::with_capacity(cap_many_series);
    let mut many_sides_offsets: Vec<i32> = Vec::with_capacity(n + 1);
    many_sides_offsets.push(0);

    for g in groups {
        if let Some(one) = &g.one {
            one_counts_vec.extend_from_slice(&one.counts);
            one_values_vec.extend_from_slice(&one.values);
            if let Some(rows) = &one.rows {
                one_rows_vec.extend_from_slice(rows);
            }
        }
        one_steps_offsets.push(checked_i32_offset(one_counts_vec.len())?);
        one_rows_offsets.push(checked_i32_offset(one_rows_vec.len())?);

        for labels in &g.one_pool {
            for label in labels {
                one_pool_builder.append_value(label.as_str());
            }
            one_pool_pos += labels.len();
        }
        one_pool_offsets.push(checked_i32_offset(one_pool_pos)?);

        for metric in &g.one_metrics {
            one_metrics_builder.append_value(metric.as_str());
        }
        one_metrics_pos += g.one_metrics.len();
        one_metrics_offsets.push(checked_i32_offset(one_metrics_pos)?);

        for series in &g.many {
            for label in &series.labels {
                many_labels_builder.append_value(label.as_str());
            }
            many_labels_pos += series.labels.len();
            many_counts_vec.extend_from_slice(&series.counts);
            many_values_vec.extend_from_slice(&series.values);
            many_sides_vec.push(series.side as u8);
        }
        many_labels_offsets.push(checked_i32_offset(many_labels_pos)?);
        many_steps_offsets.push(checked_i32_offset(many_counts_vec.len())?);
        many_sides_offsets.push(checked_i32_offset(many_sides_vec.len())?);
    }

    let one_steps_buf = OffsetBuffer::new(one_steps_offsets.into());
    let many_steps_buf = OffsetBuffer::new(many_steps_offsets.into());
    let group_array = StringArray::from_iter(groups.iter().map(|g| g.group.as_deref()));

    let list =
        |name: &str, dt: DataType, offsets: OffsetBuffer<i32>, values: ArrayRef| -> ArrayRef {
            Arc::new(ListArray::new(state_item(name, dt), offsets, values, None))
        };

    Ok(vec![
        list(
            STATE_ONE_COUNTS,
            DataType::UInt8,
            one_steps_buf.clone(),
            Arc::new(UInt8Array::from(one_counts_vec)),
        ),
        list(
            STATE_ONE_VALUES,
            DataType::Float64,
            one_steps_buf,
            Arc::new(Float64Array::from(one_values_vec)),
        ),
        list(
            STATE_ONE_ROWS,
            DataType::Int32,
            OffsetBuffer::new(one_rows_offsets.into()),
            Arc::new(Int32Array::from(one_rows_vec)),
        ),
        list(
            STATE_ONE_POOL,
            DataType::Utf8,
            OffsetBuffer::new(one_pool_offsets.into()),
            Arc::new(one_pool_builder.finish()),
        ),
        list(
            STATE_ONE_METRICS,
            DataType::Utf8,
            OffsetBuffer::new(one_metrics_offsets.into()),
            Arc::new(one_metrics_builder.finish()),
        ),
        list(
            STATE_MANY_LABELS,
            DataType::Utf8,
            OffsetBuffer::new(many_labels_offsets.into()),
            Arc::new(many_labels_builder.finish()),
        ),
        list(
            STATE_MANY_COUNTS,
            DataType::UInt8,
            many_steps_buf.clone(),
            Arc::new(UInt8Array::from(many_counts_vec)),
        ),
        list(
            STATE_MANY_VALUES,
            DataType::Float64,
            many_steps_buf,
            Arc::new(Float64Array::from(many_values_vec)),
        ),
        list(
            STATE_MANY_SIDES,
            DataType::UInt8,
            OffsetBuffer::new(many_sides_offsets.into()),
            Arc::new(UInt8Array::from(many_sides_vec)),
        ),
        Arc::new(group_array),
    ])
}

/// Decoded input columns for one `update_batch` call, bound once per batch.
struct BatchInputs<'a> {
    list: &'a ListArray,
    side: &'a BooleanArray,
    labels: LabelColumns<'a>,
    timestamps: &'a [i64],
    samples: &'a [f64],
}

impl<'a> BatchInputs<'a> {
    fn parse(schema: &[String], values: &'a [ArrayRef]) -> Result<Self> {
        let list = values
            .first()
            .and_then(|v| v.as_list_opt::<i32>())
            .ok_or_else(|| {
                DataFusionError::Internal(format!("{NAME}: first argument is not a samples list"))
            })?;
        let side = values
            .get(1)
            .and_then(|v| v.as_boolean_opt())
            .ok_or_else(|| {
                DataFusionError::Internal(format!("{NAME}: second argument is not a boolean"))
            })?;
        let labels_struct = values
            .get(2)
            .and_then(|v| v.as_struct_opt())
            .ok_or_else(|| {
                DataFusionError::Internal(format!("{NAME}: third argument is not a label struct"))
            })?;
        let entries = list.values().as_struct();
        let timestamps = child::<TimestampMillisecondArray>(entries, series::TIMESTAMP)?.values();
        let samples = child::<Float64Array>(entries, series::VALUE)?.values();
        Ok(Self {
            list,
            side,
            labels: LabelColumns::bind(schema, labels_struct),
            timestamps,
            samples,
        })
    }

    fn side_at(&self, row: usize) -> usize {
        if self.side.is_valid(row) && self.side.value(row) {
            RHS
        } else {
            LHS
        }
    }

    fn sample_range(&self, row: usize) -> (usize, usize) {
        let offsets = self.list.offsets();
        (offsets[row] as usize, offsets[row + 1] as usize)
    }
}

enum LabelCol<'a> {
    View(&'a StringViewArray),
    Utf8(&'a StringArray),
    Empty,
}

/// Pre-bound label struct columns in schema order so row loops avoid linear
/// field-name lookups on every row.
struct LabelColumns<'a> {
    cols: Vec<LabelCol<'a>>,
}

impl<'a> LabelColumns<'a> {
    fn bind(schema: &[String], labels: &'a StructArray) -> Self {
        let cols = schema
            .iter()
            .map(|name| match labels.column_by_name(name) {
                Some(column) => match column.data_type() {
                    DataType::Utf8View => LabelCol::View(column.as_string_view()),
                    DataType::Utf8 => LabelCol::Utf8(column.as_string::<i32>()),
                    _ => LabelCol::Empty,
                },
                None => LabelCol::Empty,
            })
            .collect();
        Self { cols }
    }

    fn read_into(&self, row: usize, out: &mut Vec<String>) {
        if out.len() < self.cols.len() {
            out.resize_with(self.cols.len(), String::new);
        } else {
            out.truncate(self.cols.len());
        }
        for (dst, col) in out.iter_mut().zip(&self.cols) {
            dst.clear();
            let val = match col {
                LabelCol::View(a) if a.is_valid(row) => a.value(row),
                LabelCol::Utf8(a) if a.is_valid(row) => a.value(row),
                _ => "",
            };
            if !val.is_empty() {
                dst.push_str(val);
            }
        }
    }
}

struct PrimList<'a, T> {
    offsets: &'a OffsetBuffer<i32>,
    values: &'a [T],
    len: usize,
}

impl<T> PrimList<'_, T> {
    fn range(&self, row: usize) -> (usize, usize) {
        (self.offsets[row] as usize, self.offsets[row + 1] as usize)
    }
}

struct TextList<'a> {
    offsets: &'a OffsetBuffer<i32>,
    values: &'a StringArray,
    len: usize,
}

impl TextList<'_> {
    fn range(&self, row: usize) -> (usize, usize) {
        (self.offsets[row] as usize, self.offsets[row + 1] as usize)
    }
}

/// Partial-state columns bound once per `merge_batch` call as borrowed slices
/// so per-row merging never clones sliced `ArrayRef` wrappers.
struct ParsedStates<'a> {
    num_rows: usize,
    one_counts: PrimList<'a, u8>,
    one_values: PrimList<'a, f64>,
    one_rows: PrimList<'a, i32>,
    one_pool: TextList<'a>,
    one_metrics: Option<TextList<'a>>,
    many_labels: TextList<'a>,
    many_counts: PrimList<'a, u8>,
    many_values: PrimList<'a, f64>,
    many_sides: PrimList<'a, u8>,
    group: Option<&'a StringArray>,
}

impl<'a> ParsedStates<'a> {
    fn parse(states: &'a [ArrayRef]) -> Result<Self> {
        let one_counts = prim_list::<UInt8Type>(states.first(), STATE_ONE_COUNTS)?;
        let one_values = prim_list::<Float64Type>(states.get(1), STATE_ONE_VALUES)?;
        let one_rows = prim_list::<Int32Type>(states.get(2), STATE_ONE_ROWS)?;
        let one_pool = text_list(states.get(3), STATE_ONE_POOL)?;
        let one_metrics = states
            .get(4)
            .and_then(|s| s.as_list_opt::<i32>())
            .and_then(|list| {
                list.values().as_string_opt::<i32>().map(|values| TextList {
                    offsets: list.offsets(),
                    values,
                    len: list.len(),
                })
            });
        let many_labels = text_list(states.get(5), STATE_MANY_LABELS)?;
        let many_counts = prim_list::<UInt8Type>(states.get(6), STATE_MANY_COUNTS)?;
        let many_values = prim_list::<Float64Type>(states.get(7), STATE_MANY_VALUES)?;
        let many_sides = prim_list::<UInt8Type>(states.get(8), STATE_MANY_SIDES)?;
        let group = states.get(9).and_then(|s| s.as_string_opt::<i32>());

        let num_rows = one_counts.len;
        if one_values.len != num_rows
            || one_rows.len != num_rows
            || one_pool.len != num_rows
            || many_labels.len != num_rows
            || many_counts.len != num_rows
            || many_values.len != num_rows
            || many_sides.len != num_rows
            || one_metrics.as_ref().is_some_and(|m| m.len != num_rows)
            || group.is_some_and(|g| g.len() != num_rows)
        {
            return Err(DataFusionError::Internal(format!(
                "{NAME}: partial state columns have mismatched row counts"
            )));
        }

        Ok(Self {
            num_rows,
            one_counts,
            one_values,
            one_rows,
            one_pool,
            one_metrics,
            many_labels,
            many_counts,
            many_values,
            many_sides,
            group,
        })
    }
}

/// Row count of a label rectangle is derived from its length where the
/// caller does not already know it, which a zero-width schema makes
/// impossible — so the caller says which case it is.
const NO_ROWS: usize = usize::MAX;

/// A flat slice of `[lo..hi]` label values read back as one label set per row.
fn rectangle_slice(
    flat: &StringArray,
    lo: usize,
    hi: usize,
    width: usize,
    rows: usize,
) -> Result<Vec<Vec<String>>> {
    let len = hi - lo;
    if width == 0 {
        // Every label set is the empty one, so a pool of them holds
        // exactly one entry and a rectangle holds as many as the
        // caller counted.
        return Ok(vec![Vec::new(); if rows == NO_ROWS { 1 } else { rows }]);
    }
    if !len.is_multiple_of(width) {
        return Err(DataFusionError::Internal(format!(
            "{NAME}: {len} label values are not a multiple of the {width} columns"
        )));
    }
    Ok((0..len / width)
        .map(|row| {
            (0..width)
                .map(|column| flat.value(lo + row * width + column).to_string())
                .collect()
        })
        .collect())
}

/// A label set as Prometheus prints one, keeping the names `wanted`
/// answers for. An empty value is how the canonical shape spells a
/// label that is not there, so it prints as nothing at all.
fn print(schema: &[String], labels: &[String], mut wanted: impl FnMut(&str) -> bool) -> String {
    let mut out = String::from("{");
    for (name, value) in schema.iter().zip(labels) {
        if value.is_empty() || !wanted(name) {
            continue;
        }
        if out.len() > 1 {
            out.push_str(", ");
        }
        out.push_str(name);
        out.push_str("=\"");
        out.push_str(value);
        out.push('"');
    }
    out.push('}');
    out
}

const STATE_ONE_COUNTS: &str = "one_counts";
const STATE_ONE_VALUES: &str = "one_values";
const STATE_ONE_ROWS: &str = "one_rows";
const STATE_ONE_POOL: &str = "one_pool";
const STATE_ONE_METRICS: &str = "one_metrics";
const STATE_MANY_LABELS: &str = "many_labels";
const STATE_MANY_COUNTS: &str = "many_counts";
const STATE_MANY_VALUES: &str = "many_values";
const STATE_MANY_SIDES: &str = "many_sides";
const STATE_GROUP: &str = "group";

/// Named once so that a rename fails to compile at both ends rather
/// than mismatching across a partial/final plan boundary.
const STATE_LANES: [(&str, DataType); 9] = [
    (STATE_ONE_COUNTS, DataType::UInt8),
    (STATE_ONE_VALUES, DataType::Float64),
    (STATE_ONE_ROWS, DataType::Int32),
    (STATE_ONE_POOL, DataType::Utf8),
    (STATE_ONE_METRICS, DataType::Utf8),
    (STATE_MANY_LABELS, DataType::Utf8),
    (STATE_MANY_COUNTS, DataType::UInt8),
    (STATE_MANY_VALUES, DataType::Float64),
    (STATE_MANY_SIDES, DataType::UInt8),
];

fn state_item(name: &str, of: DataType) -> FieldRef {
    Arc::new(Field::new(name, of, false))
}

fn state_type(name: &str, of: DataType) -> DataType {
    DataType::List(state_item(name, of))
}

fn child<'a, T: 'static>(entries: &'a StructArray, name: &str) -> Result<&'a T> {
    entries
        .column_by_name(name)
        .and_then(|c| c.as_any().downcast_ref::<T>())
        .ok_or_else(|| {
            DataFusionError::Internal(format!("{NAME}: samples have no {name} column of its type"))
        })
}

fn prim_list<'a, T: datafusion::arrow::datatypes::ArrowPrimitiveType>(
    state: Option<&'a ArrayRef>,
    name: &str,
) -> Result<PrimList<'a, T::Native>> {
    let list = state.and_then(|s| s.as_list_opt::<i32>()).ok_or_else(|| {
        DataFusionError::Internal(format!("{NAME}: partial state column {name} is not a list"))
    })?;
    let prim = list.values().as_primitive_opt::<T>().ok_or_else(|| {
        DataFusionError::Internal(format!(
            "{NAME}: partial state column {name} holds {}",
            list.values().data_type()
        ))
    })?;
    Ok(PrimList {
        offsets: list.offsets(),
        values: prim.values(),
        len: list.len(),
    })
}

fn text_list<'a>(state: Option<&'a ArrayRef>, name: &str) -> Result<TextList<'a>> {
    let list = state.and_then(|s| s.as_list_opt::<i32>()).ok_or_else(|| {
        DataFusionError::Internal(format!("{NAME}: partial state column {name} is not a list"))
    })?;
    let values = list.values().as_string_opt::<i32>().ok_or_else(|| {
        DataFusionError::Internal(format!(
            "{NAME}: partial state column {name} holds {}",
            list.values().data_type()
        ))
    })?;
    Ok(TextList {
        offsets: list.offsets(),
        values,
        len: list.len(),
    })
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct Binary {
    signature: Signature,
}

impl Default for Binary {
    fn default() -> Self {
        // Not `exact`: the label struct's type is the query's own label
        // set, so only the count of arguments is fixed.
        Self {
            signature: Signature::any(8, Volatility::Immutable),
        }
    }
}

pub fn udaf() -> AggregateUDF {
    AggregateUDF::new_from_impl(Binary::default())
}

/// `promql_binary(samples, <side>, labels, '<op>', '<matching>', …grid)`.
///
/// The grid is an argument for the same reason [`crate::aggregate`]
/// takes one: it turns a timestamp into a lane index. `labels` is the
/// *input's* label set, `__name__` included, not the grouping's: the
/// result's labels are built from it, and so is the label set a failure
/// has to quote.
#[allow(clippy::too_many_arguments)]
pub fn call(
    samples: Expr,
    side: Expr,
    labels: Expr,
    op: Op,
    return_bool: bool,
    matching: &Matching,
    start_ms: i64,
    end_ms: i64,
    step_ms: i64,
) -> Expr {
    udaf().call(vec![
        samples,
        side,
        labels,
        lit(literal(op, return_bool)),
        lit(matching.literal()),
        lit(start_ms),
        lit(end_ms),
        lit(step_ms),
    ])
}

/// The label column names of a labels struct, in schema order.
fn names_of(labels: &DataType) -> Option<Vec<String>> {
    match labels {
        DataType::Struct(fields) => Some(fields.iter().map(|f| f.name().clone()).collect()),
        _ => None,
    }
}

/// The operator, the matching and the grid, read back off the planned
/// call.
fn from_args(args: &AccumulatorArgs) -> Result<Pairing> {
    let literal = |i: usize| {
        args.exprs
            .get(i)
            .and_then(|e| (e.as_ref() as &dyn Any).downcast_ref::<Literal>())
            .map(Literal::value)
    };
    let text = |i: usize| match literal(i) {
        Some(ScalarValue::Utf8(Some(s))) => Some(s.as_str()),
        _ => None,
    };
    let (op, return_bool) = text(3).and_then(parse_literal).ok_or_else(|| {
        DataFusionError::Plan(format!(
            "{NAME}: fourth argument must be a binary operator as a string literal"
        ))
    })?;
    let matching = text(4).and_then(Matching::parse).ok_or_else(|| {
        DataFusionError::Plan(format!(
            "{NAME}: fifth argument must be the matching modifiers as a string literal"
        ))
    })?;
    let number = |i: usize, what: &str| {
        literal(i)
            .and_then(|v| match v {
                ScalarValue::Int64(Some(n)) => Some(*n),
                _ => None,
            })
            .ok_or_else(|| {
                DataFusionError::Plan(format!("{NAME}: {what} must be an Int64 literal"))
            })
    };
    let grid = Grid::new(
        NAME,
        number(5, "start")?,
        number(6, "end")?,
        number(7, "step")?,
    )?;
    let schema = args
        .exprs
        .get(2)
        .ok_or_else(|| DataFusionError::Plan(format!("{NAME}: no labels argument")))?
        .data_type(args.schema)
        .ok()
        .as_ref()
        .and_then(names_of)
        .ok_or_else(|| {
            DataFusionError::Plan(format!("{NAME}: third argument must be a label struct"))
        })?;
    Ok(Pairing::new(op, return_bool, matching, grid, schema))
}

impl AggregateUDFImpl for Binary {
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
        if !matches!(arg_types.get(1), Some(DataType::Boolean)) {
            return plan_err!("{NAME}: second argument must say which side a row is on");
        }
        let Some(names) = arg_types.get(2).and_then(names_of) else {
            return plan_err!("{NAME}: third argument must be a label struct");
        };
        Ok(output_type(&names))
    }

    /// A match group with nothing to pair answers with an empty list,
    /// which the planner's `Unnest` drops on its own.
    fn is_nullable(&self) -> bool {
        false
    }

    fn accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        if args.is_distinct {
            return plan_err!("{NAME}: DISTINCT is not supported");
        }
        Ok(Box::new(from_args(&args)?))
    }

    fn groups_accumulator_supported(&self, args: AccumulatorArgs) -> bool {
        !args.is_distinct
    }

    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        if args.is_distinct {
            return plan_err!("{NAME}: DISTINCT is not supported");
        }
        Ok(Box::new(GroupedBinary::from_pairing(from_args(&args)?)))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        let mut fields: Vec<FieldRef> = STATE_LANES
            .iter()
            .map(|(name, of)| {
                Arc::new(Field::new(
                    format_state_name(args.name, name),
                    state_type(name, of.clone()),
                    false,
                )) as FieldRef
            })
            .collect();
        fields.push(Arc::new(Field::new(
            format_state_name(args.name, STATE_GROUP),
            DataType::Utf8,
            true,
        )));
        Ok(fields)
    }
}

/// The floats where Go and a plausible Rust port part ways, for the
/// tests of both kernels to pair up every way round: both zeros, NaN
/// with either sign bit, the infinities, the largest float and the
/// smallest subnormal.
#[cfg(test)]
pub(crate) const EDGES: [f64; 12] = [
    0.0,
    -0.0,
    1.0,
    -1.0,
    2.5,
    -3.0,
    f64::NAN,
    -f64::NAN,
    f64::INFINITY,
    f64::NEG_INFINITY,
    f64::MAX,
    5e-324,
];

#[cfg(test)]
mod tests {
    use super::*;

    /// The label schema every pairing below works in, sorted as the
    /// canonical struct sorts it, so an expected label vector reads in
    /// the order the output carries.
    const SCHEMA: [&str; 4] = [METRIC_NAME, "path", "pod", "zone"];

    fn names(of: &[&str]) -> Vec<String> {
        of.iter().map(|n| n.to_string()).collect()
    }

    /// Bits to compare results by: every NaN is one value. Which NaN (sign,
    /// payload) an IEEE op yields from NaN inputs is unspecified and
    /// codegen picks it (Arrow's vectorised add gives the negative NaN in
    /// release where `Op::value` gives the positive one); PromQL output
    /// cannot see it. Stale markers, the one NaN with meaning, are dropped
    /// by the selector and range kernels before any operator. Everything
    /// else stays exact, so -0 against +0 still fails.
    fn bits(v: f64) -> u64 {
        if v.is_nan() {
            f64::NAN.to_bits()
        } else {
            v.to_bits()
        }
    }

    /// One label set in [`SCHEMA`]'s order; `""` is a label the series
    /// does not carry.
    fn lset(name: &str, path: &str, pod: &str, zone: &str) -> Vec<String> {
        names(&[name, path, pod, zone])
    }

    fn grid() -> Grid {
        Grid::new(NAME, 0, 30_000, 10_000).unwrap()
    }

    fn pairing(op: Op, return_bool: bool, matching: Matching) -> Pairing {
        Pairing::new(op, return_bool, matching, grid(), names(&SCHEMA))
    }

    fn plain(op: Op) -> Pairing {
        pairing(op, false, Matching::default())
    }

    fn on(labels: &[&str]) -> Matching {
        Matching {
            on: true,
            labels: names(labels),
            ..Matching::default()
        }
    }

    /// One output series as a test reads it: its labels in [`SCHEMA`]'s
    /// order, then its samples.
    type Out = (Vec<String>, Vec<(i64, f64)>);

    /// Every output series of one match group.
    fn pairs_of(value: ScalarValue) -> Vec<Out> {
        let ScalarValue::List(list) = value else {
            panic!("a list of output series")
        };
        let pairs = list.value(0);
        let pairs = pairs.as_struct();
        let labels = pairs.column_by_name(PAIR_LABELS).unwrap().as_struct();
        let samples = pairs.column_by_name(PAIR_SAMPLES).unwrap().as_list::<i32>();
        (0..pairs.len())
            .map(|row| {
                let entries = samples.value(row);
                let entries = entries.as_struct();
                let timestamps = entries
                    .column_by_name(series::TIMESTAMP)
                    .unwrap()
                    .as_primitive::<datafusion::arrow::datatypes::TimestampMillisecondType>(
                );
                let values = entries
                    .column_by_name(series::VALUE)
                    .unwrap()
                    .as_primitive::<Float64Type>();
                let samples = (0..entries.len())
                    .map(|i| (timestamps.value(i), values.value(i)))
                    .collect();
                let labels = (0..labels.num_columns())
                    .map(|c| labels.column(c).as_string_view().value(row).to_string())
                    .collect();
                (labels, samples)
            })
            .collect()
    }

    /// The samples of a group that answers with one series.
    fn samples_of(value: ScalarValue) -> Vec<(i64, f64)> {
        match pairs_of(value).as_slice() {
            [] => Vec::new(),
            [(_, samples)] => samples.clone(),
            many => panic!("{} output series, not one", many.len()),
        }
    }

    /// Upstream's `vectorElemBinop`, including the two Go functions the
    /// operators are: `math.Mod` takes the dividend's sign, and division
    /// by zero is an infinity rather than an error.
    #[test]
    fn the_arithmetic_is_upstreams_vector_elem_binop() {
        let v = |op: Op, l, r| op.value(l, r, false).expect("arithmetic always keeps");
        assert_eq!(v(Op::Add, 1.0, 2.0), 3.0);
        assert_eq!(v(Op::Sub, 1.0, 2.0), -1.0);
        assert_eq!(v(Op::Mul, 3.0, 4.0), 12.0);
        assert_eq!(v(Op::Div, 1.0, 4.0), 0.25);
        assert_eq!(v(Op::Pow, 2.0, 10.0), 1024.0);
        assert_eq!(v(Op::Atan2, 1.0, 1.0), std::f64::consts::FRAC_PI_4);

        assert_eq!(v(Op::Mod, 7.0, 3.0), 1.0);
        assert_eq!(v(Op::Mod, -7.0, 3.0), -1.0);
        assert_eq!(v(Op::Mod, 7.0, -3.0), 1.0);
        assert!(v(Op::Mod, 1.0, 0.0).is_nan());

        assert_eq!(v(Op::Div, 1.0, 0.0), f64::INFINITY);
        assert_eq!(v(Op::Div, -1.0, 0.0), f64::NEG_INFINITY);
        assert!(v(Op::Div, 0.0, 0.0).is_nan());
    }

    /// A comparison answers with the sample it was given or with
    /// nothing at all; `bool` turns that into a 1 or a 0 and always
    /// answers.
    #[test]
    fn a_comparison_filters_and_bool_scores() {
        assert_eq!(Op::Gtr.value(3.0, 2.0, false), Some(3.0));
        assert_eq!(Op::Gtr.value(2.0, 3.0, false), None);
        assert_eq!(Op::Gtr.value(3.0, 2.0, true), Some(1.0));
        assert_eq!(Op::Gtr.value(2.0, 3.0, true), Some(0.0));

        assert_eq!(Op::Eql.value(2.0, 2.0, false), Some(2.0));
        assert_eq!(Op::Neq.value(2.0, 2.0, false), None);
        assert_eq!(Op::Gte.value(2.0, 2.0, false), Some(2.0));
        assert_eq!(Op::Lte.value(2.0, 2.0, false), Some(2.0));
        assert_eq!(Op::Lss.value(2.0, 2.0, false), None);

        // A NaN compares false to everything, `!=` included by
        // negation, so it survives only that one.
        for op in [Op::Eql, Op::Gtr, Op::Lss, Op::Gte, Op::Lte] {
            assert_eq!(op.value(f64::NAN, 1.0, false), None, "{}", op.as_str());
            assert_eq!(op.value(f64::NAN, 1.0, true), Some(0.0), "{}", op.as_str());
        }
        assert!(Op::Neq.value(f64::NAN, 1.0, false).unwrap().is_nan());
    }

    /// Only a comparison without `bool` answers with the metric it was
    /// given, so only that one keeps its name.
    #[test]
    fn what_drops_the_metric_name_is_changes_metric_schema_plus_bool() {
        assert!(Op::Add.drops_metric_name(false));
        assert!(Op::Atan2.drops_metric_name(false));
        assert!(!Op::Gtr.drops_metric_name(false));
        assert!(Op::Gtr.drops_metric_name(true));
        // A set operator hands the sample back as it came, name and all.
        for op in [Op::And, Op::Or, Op::Unless] {
            assert!(!op.drops_metric_name(false), "{}", op.as_str());
        }
    }

    #[test]
    fn every_operator_round_trips_through_its_promql_spelling() {
        for op in Op::ALL {
            assert_eq!(parse_literal(literal(op, false)), Some((op, false)));
            if op.is_comparison() {
                assert_eq!(parse_literal(literal(op, true)), Some((op, true)));
                assert_eq!(literal(op, true), format!("{} bool", op.as_str()));
            }
        }
        assert_eq!(Op::from_token(ItemType::Add), Some(Op::Add));
        assert_eq!(Op::from_token(ItemType::Gtr), Some(Op::Gtr));
        assert_eq!(Op::from_token(ItemType::Land), Some(Op::And));
        assert_eq!(Op::from_token(ItemType::Lunless), Some(Op::Unless));
        assert_eq!(parse_literal("and"), Some((Op::And, false)));
        assert_eq!(parse_literal("and bool"), None);
        // `bool` belongs to a comparison and to nothing else, so a plan
        // that spells it anywhere else is not one this wrote.
        assert_eq!(parse_literal("+ bool"), None);
        assert_eq!(parse_literal("atan2 bool"), None);
    }

    /// The modifiers go into a plan as the query wrote them and come
    /// back the same.
    #[test]
    fn the_matching_modifiers_round_trip_through_promql() {
        let cases = [
            (Matching::default(), ""),
            (on(&["job", "pod"]), "on(job, pod)"),
            (
                Matching {
                    labels: names(&["zone"]),
                    ..Matching::default()
                },
                "ignoring(zone)",
            ),
            (
                Matching {
                    card: Card::ManyToOne,
                    on: true,
                    labels: names(&["job"]),
                    include: names(&["tier"]),
                    ..Matching::default()
                },
                "on(job) group_left(tier)",
            ),
            (
                Matching {
                    card: Card::OneToMany,
                    on: true,
                    labels: names(&["job"]),
                    include: Vec::new(),
                    ..Matching::default()
                },
                "on(job) group_right()",
            ),
            (
                Matching {
                    card: Card::ManyToOne,
                    ..Matching::default()
                },
                "group_left()",
            ),
            (
                Matching {
                    fill_lhs: Some(0.0),
                    fill_rhs: Some(0.0),
                    ..Matching::default()
                },
                "fill_left(0.0) fill_right(0.0)",
            ),
            (
                Matching {
                    card: Card::OneToMany,
                    on: true,
                    labels: names(&["pod"]),
                    fill_rhs: Some(-1.5),
                    ..Matching::default()
                },
                "on(pod) group_right() fill_right(-1.5)",
            ),
        ];
        for (matching, spelling) in cases {
            assert_eq!(matching.literal(), spelling);
            assert_eq!(Matching::parse(spelling), Some(matching), "{spelling}");
        }
        assert_eq!(Matching::parse("on(job"), None);
        assert_eq!(Matching::parse("group_sideways()"), None);
    }

    /// A step only one side reached is not in the result: upstream's
    /// loop over the left side skips a sample with no match, and a right
    /// sample nothing matched is never looked at.
    #[test]
    fn only_the_steps_both_sides_reached_are_paired() {
        let mut pairing = plain(Op::Add);
        let left = lset("requests", "", "a", "");
        let right = lset("errors", "", "a", "");
        pairing
            .absorb(LHS, &left, &[0, 10_000, 20_000], &[1.0, 2.0, 3.0])
            .unwrap();
        pairing
            .absorb(RHS, &right, &[10_000, 30_000], &[10.0, 30.0])
            .unwrap();
        assert_eq!(samples_of(pairing.evaluate().unwrap()), [(10_000, 12.0)]);
    }

    /// A comparison between two vectors answers with the left sample
    /// where it holds and leaves the step out where it does not — the
    /// filter upstream's `keep` is, one step at a time.
    #[test]
    fn a_comparison_between_vectors_filters_step_by_step() {
        let left = lset("requests", "", "a", "");
        let right = lset("errors", "", "a", "");

        let mut filtered = pairing(Op::Gtr, false, Matching::default());
        filtered
            .absorb(LHS, &left, &[0, 10_000], &[5.0, 1.0])
            .unwrap();
        filtered
            .absorb(RHS, &right, &[0, 10_000], &[2.0, 9.0])
            .unwrap();
        assert_eq!(samples_of(filtered.evaluate().unwrap()), [(0, 5.0)]);

        let mut scored = pairing(Op::Gtr, true, Matching::default());
        scored
            .absorb(LHS, &left, &[0, 10_000], &[5.0, 1.0])
            .unwrap();
        scored
            .absorb(RHS, &right, &[0, 10_000], &[2.0, 9.0])
            .unwrap();
        assert_eq!(
            samples_of(scored.evaluate().unwrap()),
            [(0, 1.0), (10_000, 0.0)]
        );
    }

    /// Two series on the same side of one match group are the two
    /// matching errors, and only where their samples meet at a step.
    #[test]
    fn two_series_in_a_match_group_fail_where_they_overlap() {
        let a = lset("requests", "", "a", "");
        let b = lset("retries", "", "a", "");
        let c = lset("errors", "", "a", "");

        let mut both = plain(Op::Add);
        both.absorb(LHS, &a, &[0], &[1.0]).unwrap();
        both.absorb(LHS, &b, &[0], &[2.0]).unwrap();
        both.absorb(RHS, &c, &[0], &[3.0]).unwrap();
        let err = both.evaluate().unwrap_err().to_string();
        assert!(
            err.contains("many-to-one matching must be explicit"),
            "{err}"
        );

        let mut right = plain(Op::Add);
        right.absorb(LHS, &c, &[0], &[1.0]).unwrap();
        right.absorb(RHS, &a, &[0], &[2.0]).unwrap();
        right.absorb(RHS, &b, &[0], &[3.0]).unwrap();
        let err = right.evaluate().unwrap_err().to_string();
        assert!(
            err.contains("found duplicate series for the match group {pod=\"a\"} on the right"),
            "{err}"
        );
        assert!(
            err.contains(
                ";many-to-many matching not allowed: matching labels must be unique on one side"
            ),
            "{err}"
        );

        // Apart in time is not a duplicate at all: upstream checks the
        // vector at one step, not the series over the range.
        let mut apart = plain(Op::Add);
        apart.absorb(LHS, &a, &[0], &[1.0]).unwrap();
        apart.absorb(LHS, &b, &[10_000], &[2.0]).unwrap();
        apart.absorb(RHS, &c, &[0, 10_000], &[3.0, 4.0]).unwrap();
        assert_eq!(
            samples_of(apart.evaluate().unwrap()),
            [(0, 4.0), (10_000, 6.0)]
        );
    }

    /// Two left series that differ only in `__name__` share a match
    /// group, because the signature drops the name — but a comparison
    /// gives that name back, so the group has to answer with both.
    #[test]
    fn a_kept_name_splits_the_match_group_back_up() {
        let a = lset("a", "", "one", "");
        let b = lset("b", "", "one", "");
        let c = lset("c", "", "one", "");

        let mut filtered = pairing(Op::Gtr, false, Matching::default());
        filtered.absorb(LHS, &b, &[0], &[5.0]).unwrap();
        filtered.absorb(LHS, &a, &[10_000], &[7.0]).unwrap();
        filtered.absorb(RHS, &c, &[0, 10_000], &[1.0, 1.0]).unwrap();
        assert_eq!(
            pairs_of(filtered.evaluate().unwrap()),
            [
                (lset("a", "", "one", ""), vec![(10_000, 7.0)]),
                (lset("b", "", "one", ""), vec![(0, 5.0)]),
            ]
        );

        // `bool` takes the name away again, so the same two series are
        // one result — which is what upstream's `resultMetric` does
        // once `changesMetricSchema` holds.
        let mut scored = pairing(Op::Gtr, true, Matching::default());
        scored.absorb(LHS, &b, &[0], &[5.0]).unwrap();
        scored.absorb(LHS, &a, &[10_000], &[7.0]).unwrap();
        scored.absorb(RHS, &c, &[0, 10_000], &[1.0, 1.0]).unwrap();
        assert_eq!(
            pairs_of(scored.evaluate().unwrap()),
            [(lset("", "", "one", ""), vec![(0, 1.0), (10_000, 1.0)])]
        );
    }

    /// `on(…)` narrows the result to the labels it names, `ignoring(…)`
    /// takes only those away — upstream's `Keep` and `Del`.
    #[test]
    fn one_to_one_keeps_what_the_modifier_says() {
        let left = lset("requests", "", "a", "eu");
        let right = lset("errors", "", "a", "us");

        let mut kept = pairing(Op::Add, false, on(&["pod"]));
        kept.absorb(LHS, &left, &[0], &[6.0]).unwrap();
        kept.absorb(RHS, &right, &[0], &[2.0]).unwrap();
        assert_eq!(
            pairs_of(kept.evaluate().unwrap()),
            [(lset("", "", "a", ""), vec![(0, 8.0)])]
        );

        let mut ignoring = pairing(
            Op::Add,
            false,
            Matching {
                labels: names(&["zone"]),
                ..Matching::default()
            },
        );
        ignoring.absorb(LHS, &left, &[0], &[6.0]).unwrap();
        ignoring.absorb(RHS, &right, &[0], &[2.0]).unwrap();
        assert_eq!(
            pairs_of(ignoring.evaluate().unwrap()),
            [(lset("", "", "a", ""), vec![(0, 8.0)])]
        );
    }

    /// `group_left` keeps every label of the many side — no `Keep` and
    /// no `Del` — and copies the named ones over from the one side, so
    /// a match group answers with a series per left-hand one.
    #[test]
    fn group_left_fans_out_and_carries_the_included_labels() {
        let mut pairing = pairing(
            Op::Div,
            false,
            Matching {
                card: Card::ManyToOne,
                on: true,
                labels: names(&["pod"]),
                include: names(&["zone"]),
                ..Matching::default()
            },
        );
        pairing
            .absorb(LHS, &lset("requests", "/a", "a", ""), &[0], &[10.0])
            .unwrap();
        pairing
            .absorb(LHS, &lset("requests", "/b", "a", ""), &[0], &[20.0])
            .unwrap();
        pairing
            .absorb(RHS, &lset("errors", "", "a", "eu"), &[0], &[2.0])
            .unwrap();
        assert_eq!(
            pairs_of(pairing.evaluate().unwrap()),
            [
                (lset("", "/a", "a", "eu"), vec![(0, 5.0)]),
                (lset("", "/b", "a", "eu"), vec![(0, 10.0)]),
            ]
        );
    }

    /// `group_right` is the same match with the sides swapped: the
    /// right is the many, and the operands keep their written order.
    #[test]
    fn group_right_swaps_which_side_may_be_many() {
        let mut pairing = pairing(
            Op::Sub,
            false,
            Matching {
                card: Card::OneToMany,
                on: true,
                labels: names(&["pod"]),
                include: Vec::new(),
                ..Matching::default()
            },
        );
        pairing
            .absorb(LHS, &lset("total", "", "a", ""), &[0], &[10.0])
            .unwrap();
        pairing
            .absorb(RHS, &lset("errors", "/a", "a", ""), &[0], &[2.0])
            .unwrap();
        pairing
            .absorb(RHS, &lset("errors", "/b", "a", ""), &[0], &[3.0])
            .unwrap();
        assert_eq!(
            pairs_of(pairing.evaluate().unwrap()),
            [
                (lset("", "/a", "a", ""), vec![(0, 8.0)]),
                (lset("", "/b", "a", ""), vec![(0, 7.0)]),
            ]
        );
    }

    /// A duplicate on the one side of a `group_right` is quoted as the
    /// left-hand side, because that is where the one now sits.
    #[test]
    fn group_right_names_the_left_hand_side_in_a_duplicate() {
        let mut pairing = pairing(
            Op::Add,
            false,
            Matching {
                card: Card::OneToMany,
                on: true,
                labels: names(&["pod"]),
                include: Vec::new(),
                ..Matching::default()
            },
        );
        pairing
            .absorb(LHS, &lset("total", "", "a", ""), &[0], &[1.0])
            .unwrap();
        pairing
            .absorb(LHS, &lset("count", "", "a", ""), &[0], &[2.0])
            .unwrap();
        pairing
            .absorb(RHS, &lset("errors", "", "a", ""), &[0], &[3.0])
            .unwrap();
        let err = pairing.evaluate().unwrap_err().to_string();
        assert!(
            err.contains("found duplicate series for the match group {pod=\"a\"} on the left"),
            "{err}"
        );
    }

    /// The grouping labels have to leave the result unique: two series
    /// of the many side reaching the same labels is upstream's other
    /// matching error.
    #[test]
    fn a_fan_out_that_collides_names_the_grouping_labels() {
        let mut pairing = pairing(
            Op::Add,
            false,
            Matching {
                card: Card::ManyToOne,
                on: true,
                labels: names(&["pod"]),
                include: Vec::new(),
                ..Matching::default()
            },
        );
        // Both left series lose `__name__` to the arithmetic and agree
        // on everything else, so they are one result label set.
        pairing
            .absorb(LHS, &lset("requests", "", "a", "eu"), &[0], &[1.0])
            .unwrap();
        pairing
            .absorb(LHS, &lset("retries", "", "a", "eu"), &[0], &[2.0])
            .unwrap();
        pairing
            .absorb(RHS, &lset("errors", "", "a", ""), &[0], &[3.0])
            .unwrap();
        let err = pairing.evaluate().unwrap_err().to_string();
        assert!(
            err.contains("multiple matches for labels: grouping labels must ensure unique matches"),
            "{err}"
        );
    }

    /// An `on` that names `__name__` matches on it and keeps it, save
    /// where the operator has taken it away first.
    #[test]
    fn on_the_metric_name_matches_on_it() {
        let mut pairing = pairing(Op::Gtr, false, on(&[METRIC_NAME]));
        pairing
            .absorb(LHS, &lset("up", "", "a", ""), &[0], &[5.0])
            .unwrap();
        pairing
            .absorb(RHS, &lset("up", "", "b", ""), &[0], &[2.0])
            .unwrap();
        assert_eq!(
            pairs_of(pairing.evaluate().unwrap()),
            [(lset("up", "", "", ""), vec![(0, 5.0)])]
        );
    }

    /// Two partitions of one match group reach the same answer as one,
    /// down to the many side's labels and the one side's `group_x`
    /// values — whose pool indices mean nothing across a partial.
    #[test]
    fn partial_states_merge_to_the_same_pairing() {
        let matching = Matching {
            card: Card::ManyToOne,
            on: true,
            labels: names(&["pod"]),
            include: names(&["zone"]),
            ..Matching::default()
        };
        let of = || pairing(Op::Mul, false, matching.clone());
        let many_a = lset("requests", "/a", "a", "");
        let many_b = lset("requests", "/b", "a", "");
        let one = lset("errors", "", "a", "eu");

        let mut whole = of();
        whole
            .absorb(LHS, &many_a, &[0, 10_000], &[2.0, 3.0])
            .unwrap();
        whole.absorb(LHS, &many_b, &[10_000], &[4.0]).unwrap();
        whole.absorb(RHS, &one, &[0, 10_000], &[5.0, 7.0]).unwrap();

        let mut left = of();
        left.absorb(LHS, &many_a, &[0, 10_000], &[2.0, 3.0])
            .unwrap();
        let mut right = of();
        right.absorb(LHS, &many_b, &[10_000], &[4.0]).unwrap();
        right.absorb(RHS, &one, &[0, 10_000], &[5.0, 7.0]).unwrap();

        let mut merged = of();
        for mut partial in [left, right] {
            let state = partial.state().unwrap();
            let arrays: Vec<ArrayRef> = state.iter().map(|s| s.to_array().unwrap()).collect();
            merged.merge_batch(&arrays).unwrap();
        }
        assert_eq!(merged.evaluate().unwrap(), whole.evaluate().unwrap());
        assert_eq!(
            pairs_of(merged.evaluate().unwrap()),
            [
                (lset("", "/a", "a", "eu"), vec![(0, 10.0), (10_000, 21.0)]),
                (lset("", "/b", "a", "eu"), vec![(10_000, 28.0)]),
            ]
        );
    }

    /// Every operator shape the kernel computes: `bool` only on a
    /// comparison, and no set operator, which keeps samples instead.
    fn shapes() -> impl Iterator<Item = (Op, bool)> {
        Op::ALL
            .into_iter()
            .filter(|op| !op.is_set())
            .flat_map(|op| {
                [(op, false), (op, true)]
                    .into_iter()
                    .filter(|(op, return_bool)| !return_bool || op.is_comparison())
            })
    }

    /// Every edge value against every other, itself included.
    fn edge_pairs() -> Vec<(f64, f64)> {
        EDGES
            .iter()
            .flat_map(|l| EDGES.iter().map(move |r| (*l, *r)))
            .collect()
    }

    /// The kernel's answer at each position against [`Op::value`] on the
    /// pair that stood there, compared as [`bits`] so that a zero's sign
    /// counts.
    fn assert_kernel_is_value(
        op: Op,
        return_bool: bool,
        lhs: Operand,
        rhs: Operand,
        pairs: &[(f64, f64)],
    ) {
        let got = op.kernel(lhs, rhs, return_bool).unwrap();
        let len = match &got {
            Computed::Values(values) => values.len(),
            Computed::Holds(holds) => holds.len(),
        };
        assert_eq!(len, pairs.len(), "{}", literal(op, return_bool));
        for (i, &(l, r)) in pairs.iter().enumerate() {
            let want = op.value(l, r, return_bool);
            let what = format!("{l:?} {} {r:?}", literal(op, return_bool));
            match &got {
                Computed::Values(values) => {
                    let want = want.unwrap_or_else(|| panic!("{what}: value drops it"));
                    assert_eq!(bits(values.value(i)), bits(want), "{what}");
                }
                Computed::Holds(holds) => {
                    assert!(op.is_comparison() && !return_bool, "{what}");
                    assert_eq!(holds.value(i), want.is_some(), "{what}");
                }
            }
        }
    }

    /// The lane kernel answers what `vectorElemBinop` does one pair at a
    /// time, for every operator, both lanes against each other and each
    /// against a scalar on either side.
    #[test]
    fn the_lane_kernel_is_vector_elem_binop_bit_for_bit() {
        let pairs = edge_pairs();
        let lhs = Float64Array::from_iter_values(pairs.iter().map(|p| p.0));
        let rhs = Float64Array::from_iter_values(pairs.iter().map(|p| p.1));
        let lane = Float64Array::from(EDGES.to_vec());
        for (op, return_bool) in shapes() {
            assert_kernel_is_value(
                op,
                return_bool,
                Operand::Lane(&lhs),
                Operand::Lane(&rhs),
                &pairs,
            );
            for s in EDGES {
                assert_kernel_is_value(
                    op,
                    return_bool,
                    Operand::Lane(&lane),
                    Operand::Scalar(s),
                    &EDGES.map(|v| (v, s)),
                );
                assert_kernel_is_value(
                    op,
                    return_bool,
                    Operand::Scalar(s),
                    Operand::Lane(&lane),
                    &EDGES.map(|v| (s, v)),
                );
            }
        }
    }

    fn bits_of(value: ScalarValue) -> Vec<(i64, u64)> {
        samples_of(value)
            .into_iter()
            .map(|(t, v)| (t, bits(v)))
            .collect()
    }

    /// A one-to-one pairing over a grid of `steps` with one millisecond
    /// to the step, so a test's timestamps are its step numbers.
    fn over(op: Op, return_bool: bool, steps: i64) -> Pairing {
        let grid = Grid::new(NAME, 0, steps - 1, 1).unwrap();
        Pairing::new(op, return_bool, Matching::default(), grid, names(&SCHEMA))
    }

    /// The whole of `evaluate` over the edge pairs, one per step, with a
    /// step only the left reached and one only the right reached: what
    /// comes out is `Op::value` at the paired steps and nothing else.
    /// Evaluated twice, because a window frame evaluates again and the
    /// state must still be there.
    #[test]
    fn evaluate_is_vector_elem_binop_at_every_paired_step() {
        let pairs = edge_pairs();
        let steps = pairs.len() as i64 + 2;
        let paired: Vec<i64> = (0..pairs.len() as i64).collect();
        let (lhs, rhs) = (lset("requests", "", "a", ""), lset("errors", "", "a", ""));
        for (op, return_bool) in shapes() {
            let mut pairing = over(op, return_bool, steps);
            let left: Vec<f64> = pairs.iter().map(|p| p.0).chain([1.0]).collect();
            let left_steps: Vec<i64> = paired.iter().copied().chain([steps - 2]).collect();
            pairing.absorb(LHS, &lhs, &left_steps, &left).unwrap();
            let right: Vec<f64> = pairs.iter().map(|p| p.1).chain([1.0]).collect();
            let right_steps: Vec<i64> = paired.iter().copied().chain([steps - 1]).collect();
            pairing.absorb(RHS, &rhs, &right_steps, &right).unwrap();

            let want: Vec<(i64, u64)> = pairs
                .iter()
                .enumerate()
                .filter_map(|(step, &(l, r))| {
                    op.value(l, r, return_bool).map(|v| (step as i64, bits(v)))
                })
                .collect();
            let what = literal(op, return_bool);
            assert_eq!(bits_of(pairing.evaluate().unwrap()), want, "{what}");
            assert_eq!(bits_of(pairing.evaluate().unwrap()), want, "{what} again");
        }
    }

    /// Go's `==` and `<` on floats, which Rust's native operators share
    /// and Arrow's comparison kernels do not: those order by `total_cmp`,
    /// where a NaN equals itself and -0 sorts below +0.
    #[test]
    fn a_nan_is_equal_to_nothing_and_the_zeros_are_equal() {
        let run = |op: Op, return_bool: bool| {
            let mut pairing = over(op, return_bool, 2);
            let left = lset("requests", "", "a", "");
            let right = lset("errors", "", "a", "");
            pairing
                .absorb(LHS, &left, &[0, 1], &[f64::NAN, -0.0])
                .unwrap();
            pairing
                .absorb(RHS, &right, &[0, 1], &[f64::NAN, 0.0])
                .unwrap();
            bits_of(pairing.evaluate().unwrap())
        };
        let bits_all = |samples: Vec<(i64, f64)>| -> Vec<(i64, u64)> {
            samples.into_iter().map(|(t, v)| (t, bits(v))).collect()
        };
        assert_eq!(run(Op::Eql, false), bits_all(vec![(1, -0.0)]));
        assert_eq!(run(Op::Eql, true), bits_all(vec![(0, 0.0), (1, 1.0)]));
        assert_eq!(run(Op::Neq, false), bits_all(vec![(0, f64::NAN)]));
        assert_eq!(run(Op::Lss, false), Vec::new());
        assert_eq!(run(Op::Lte, false), bits_all(vec![(1, -0.0)]));
        assert_eq!(run(Op::Gte, true), bits_all(vec![(0, 0.0), (1, 1.0)]));
    }

    /// Two buckets side by side, one the comparison empties and one it
    /// keeps: the kernel runs over both at once, and the empty one must
    /// not turn into an output series with nothing in it.
    #[test]
    fn a_bucket_the_comparison_empties_is_no_series() {
        let mut pairing = pairing(
            Op::Gtr,
            false,
            Matching {
                card: Card::ManyToOne,
                ..on(&["pod"])
            },
        );
        let steps = [0, 10_000];
        pairing
            .absorb(LHS, &lset("requests", "/a", "a", ""), &steps, &[1.0, 1.0])
            .unwrap();
        pairing
            .absorb(LHS, &lset("requests", "/b", "a", ""), &steps, &[7.0, 8.0])
            .unwrap();
        pairing
            .absorb(RHS, &lset("limit", "", "a", ""), &steps, &[5.0, 5.0])
            .unwrap();
        let out = pairs_of(pairing.evaluate().unwrap());
        assert_eq!(
            out,
            [(
                lset("requests", "/b", "a", ""),
                vec![(0, 7.0), (10_000, 8.0)]
            )]
        );
    }

    /// Two lanes that do not line up are a broken caller, not a
    /// panic inside the comparison loop.
    #[test]
    fn lanes_of_different_lengths_are_refused() {
        let (short, long) = (
            Float64Array::from(vec![1.0]),
            Float64Array::from(vec![1.0, 2.0]),
        );
        for op in [Op::Add, Op::Pow, Op::Gtr] {
            assert!(op
                .kernel(Operand::Lane(&short), Operand::Lane(&long), false)
                .is_err());
        }
    }

    /// The three set operators over one match group, step by step: the
    /// right side is present at the first two steps and gone at the
    /// third, so each operator changes its mind exactly there.
    #[test]
    fn a_set_operator_asks_only_whether_the_other_side_is_there() {
        let left = lset("requests", "", "a", "");
        let right = lset("errors", "", "a", "");
        let of = |op: Op| {
            let mut pairing = plain(op);
            pairing
                .absorb(LHS, &left, &[0, 10_000, 20_000], &[1.0, 2.0, 3.0])
                .unwrap();
            pairing
                .absorb(RHS, &right, &[0, 10_000], &[9.0, 9.0])
                .unwrap();
            pairs_of(pairing.evaluate().unwrap())
        };

        // `and` keeps the left sample, its value and its name.
        assert_eq!(of(Op::And), [(left.clone(), vec![(0, 1.0), (10_000, 2.0)])]);
        assert_eq!(of(Op::Unless), [(left.clone(), vec![(20_000, 3.0)])]);
        // `or` is every left sample plus the right ones the left did
        // not already answer for, each under its own labels.
        assert_eq!(
            of(Op::Or),
            [(left.clone(), vec![(0, 1.0), (10_000, 2.0), (20_000, 3.0)])]
        );
    }

    /// `or` answers with the right-hand series, name and all, at the
    /// steps the left side left empty — and with nothing of it where
    /// the left side was there.
    #[test]
    fn or_falls_back_to_the_right_hand_series() {
        let left = lset("requests", "", "a", "");
        let right = lset("errors", "", "a", "");
        let mut pairing = plain(Op::Or);
        pairing.absorb(LHS, &left, &[10_000], &[1.0]).unwrap();
        pairing
            .absorb(RHS, &right, &[0, 10_000, 20_000], &[7.0, 8.0, 9.0])
            .unwrap();
        assert_eq!(
            pairs_of(pairing.evaluate().unwrap()),
            [
                (right, vec![(0, 7.0), (20_000, 9.0)]),
                (left, vec![(10_000, 1.0)]),
            ]
        );
    }

    /// Two left series in one group are a fan-out error for an
    /// arithmetic operator and nothing at all for a set one: every
    /// signature may hold as many series as it likes on both sides.
    #[test]
    fn a_set_operator_lets_both_sides_be_many() {
        let mut pairing = pairing(Op::And, false, on(&["pod"]));
        let a = lset("requests", "/a", "a", "");
        let b = lset("requests", "/b", "a", "");
        pairing.absorb(LHS, &a, &[0], &[1.0]).unwrap();
        pairing.absorb(LHS, &b, &[0], &[2.0]).unwrap();
        pairing
            .absorb(RHS, &lset("errors", "", "a", "eu"), &[0], &[9.0])
            .unwrap();
        pairing
            .absorb(RHS, &lset("errors", "", "a", "us"), &[0], &[9.0])
            .unwrap();
        assert_eq!(
            pairs_of(pairing.evaluate().unwrap()),
            [(a, vec![(0, 1.0)]), (b, vec![(0, 2.0)])]
        );
    }

    /// A set operator's partial state carries which side each series was
    /// on, which is the only thing the merged group cannot work out for
    /// itself.
    #[test]
    fn partial_states_keep_the_side_a_series_was_on() {
        let left = lset("requests", "", "a", "");
        let right = lset("errors", "", "a", "");
        let mut whole = plain(Op::Unless);
        whole.absorb(LHS, &left, &[0, 10_000], &[1.0, 2.0]).unwrap();
        whole.absorb(RHS, &right, &[10_000], &[9.0]).unwrap();

        let mut a = plain(Op::Unless);
        a.absorb(LHS, &left, &[0, 10_000], &[1.0, 2.0]).unwrap();
        let mut b = plain(Op::Unless);
        b.absorb(RHS, &right, &[10_000], &[9.0]).unwrap();

        let mut merged = plain(Op::Unless);
        for mut partial in [a, b] {
            let state = partial.state().unwrap();
            let arrays: Vec<ArrayRef> = state.iter().map(|s| s.to_array().unwrap()).collect();
            merged.merge_batch(&arrays).unwrap();
        }
        assert_eq!(merged.evaluate().unwrap(), whole.evaluate().unwrap());
        assert_eq!(
            pairs_of(merged.evaluate().unwrap()),
            [(left, vec![(0, 1.0)])]
        );
    }

    /// A fill value stands in for a side with no series under this
    /// signature, instead of the step being dropped.
    #[test]
    fn a_fill_value_stands_in_for_a_side_that_is_not_there() {
        let left = lset("requests", "", "a", "");
        let right = lset("errors", "", "a", "");
        let of = |matching: Matching, left_steps: &[i64], right_steps: &[i64]| {
            let mut pairing = pairing(Op::Add, false, matching);
            let ones = vec![1.0; left_steps.len()];
            let tens = vec![10.0; right_steps.len()];
            pairing.absorb(LHS, &left, left_steps, &ones).unwrap();
            pairing.absorb(RHS, &right, right_steps, &tens).unwrap();
            pairs_of(pairing.evaluate().unwrap())
        };
        let fill = |lhs: Option<f64>, rhs: Option<f64>| Matching {
            fill_lhs: lhs,
            fill_rhs: rhs,
            ..Matching::default()
        };

        // Without a fill, only the step both sides reached is answered.
        assert_eq!(
            of(Matching::default(), &[0, 10_000], &[10_000, 20_000]),
            [(lset("", "", "a", ""), vec![(10_000, 11.0)])]
        );
        // `fill_right(0)` stands in for the right-hand side at 0s, and
        // says nothing about the right-hand sample at 20s.
        assert_eq!(
            of(fill(None, Some(0.0)), &[0, 10_000], &[10_000, 20_000]),
            [(lset("", "", "a", ""), vec![(0, 1.0), (10_000, 11.0)])]
        );
        // `fill_left(100)` is the other way round.
        assert_eq!(
            of(fill(Some(100.0), None), &[0, 10_000], &[10_000, 20_000]),
            [(lset("", "", "a", ""), vec![(10_000, 11.0), (20_000, 110.0)])]
        );
        // `fill(0)` writes both, so every step either side reached is
        // answered.
        assert_eq!(
            of(fill(Some(0.0), Some(0.0)), &[0, 10_000], &[10_000, 20_000]),
            [(
                lset("", "", "a", ""),
                vec![(0, 1.0), (10_000, 11.0), (20_000, 10.0)]
            )]
        );
    }

    /// A filled-in side has no series behind it, so what it brings to
    /// the result labels is the match signature and nothing more --
    /// which is what the `group_x` labels are copied from.
    #[test]
    fn a_filled_in_side_carries_only_the_signature() {
        let matching = Matching {
            card: Card::ManyToOne,
            on: true,
            labels: names(&["pod"]),
            include: names(&["zone"]),
            fill_lhs: Some(7.0),
            fill_rhs: Some(0.0),
        };
        let mut pairing = pairing(Op::Add, false, matching);
        // The many side at 0s only, the one side at 10s only, so each
        // step has exactly one real sample and one filled-in one.
        pairing
            .absorb(LHS, &lset("requests", "/x", "a", ""), &[0], &[1.0])
            .unwrap();
        pairing
            .absorb(RHS, &lset("errors", "", "a", "eu"), &[10_000], &[2.0])
            .unwrap();
        assert_eq!(
            pairs_of(pairing.evaluate().unwrap()),
            [
                // At 10s the "many" side is filled: the base is the
                // signature alone, plus `zone` off the real one side.
                (lset("", "", "a", "eu"), vec![(10_000, 9.0)]),
                // At 0s the "one" side is filled, so `zone` has nothing
                // to be copied from and the path label survives.
                (lset("", "/x", "a", ""), vec![(0, 1.0)]),
            ]
        );
    }

    /// A match group the "many" side never reached answers with
    /// nothing at all, however many series the "one" side put in it --
    /// the duplicate is upstream's error only once there is a match to
    /// be made. A fill is that match, so with one the group is walked
    /// and the duplicate is found.
    #[test]
    fn an_unfilled_group_with_no_many_side_is_not_a_duplicate() {
        let of = |matching: Matching| {
            let mut pairing = pairing(Op::Add, false, matching);
            pairing
                .absorb(RHS, &lset("errors", "/x", "a", ""), &[0], &[1.0])
                .unwrap();
            pairing
                .absorb(RHS, &lset("errors", "/y", "a", ""), &[0], &[2.0])
                .unwrap();
            pairing.evaluate()
        };

        assert!(pairs_of(of(on(&["pod"])).unwrap()).is_empty());
        let err = of(Matching {
            fill_lhs: Some(0.0),
            ..on(&["pod"])
        })
        .expect_err("the fill gives the group something to match");
        assert!(err.to_string().contains(DUPLICATE_SERIES), "{err}");
    }

    /// A fill does not buy a fan-out its way out of the grouping rule:
    /// two "many" series whose result labels collide are the same
    /// error whether the "one" side was there or filled in.
    #[test]
    fn a_filled_fan_out_still_has_to_land_on_distinct_labels() {
        let matching = Matching {
            card: Card::ManyToOne,
            on: true,
            labels: names(&["pod"]),
            fill_rhs: Some(0.0),
            ..Matching::default()
        };
        let mut pairing = pairing(Op::Add, false, matching);
        // Two left series differing only in `__name__`, which `+` takes
        // away, and no right side at all -- so the fill is what they
        // both pair with.
        pairing
            .absorb(LHS, &lset("requests", "", "a", ""), &[0], &[1.0])
            .unwrap();
        pairing
            .absorb(LHS, &lset("retries", "", "a", ""), &[0], &[2.0])
            .unwrap();
        let err = pairing.evaluate().expect_err("the two collide");
        assert!(err.to_string().contains(GROUPING_NOT_UNIQUE), "{err}");
    }

    /// The label set as Prometheus prints it, with the canonical
    /// shape's empty string read as a label that is not there.
    #[test]
    fn a_label_set_prints_as_prometheus_prints_it() {
        let schema = names(&[METRIC_NAME, "job", "pod"]);
        let labels = names(&["up", "api", ""]);
        assert_eq!(
            print(&schema, &labels, |_| true),
            r#"{__name__="up", job="api"}"#
        );
        assert_eq!(
            print(&schema, &labels, |n| n != METRIC_NAME),
            r#"{job="api"}"#
        );
    }

    fn pairs_at(array: &ArrayRef, row: usize) -> Vec<Out> {
        let list = array.as_list::<i32>();
        let single = ListArray::new(
            pair_item(&names(&SCHEMA)),
            OffsetBuffer::new(vec![0, list.value(row).len() as i32].into()),
            list.value(row),
            None,
        );
        pairs_of(ScalarValue::List(Arc::new(single)))
    }

    #[test]
    fn grouped_accumulator_merges_prefixes_filters_rows_and_tracks_size() {
        use crate::Series;
        type RowSpec<'a> = (&'a [(&'a str, &'a str)], &'a [i64], &'a [f64], bool);

        let make_batch = |rows: &[RowSpec<'_>]| -> [ArrayRef; 3] {
            let series_vec: Vec<Series> = rows
                .iter()
                .map(|(lbls, ts, vs, _)| Series::new(lbls, ts.to_vec(), vs.to_vec()).unwrap())
                .collect();
            let batch = series::encode(
                &names(&SCHEMA),
                &series_vec,
                series::Block {
                    start_ms: 0,
                    end_ms: 30_001,
                },
            )
            .unwrap();
            let sides = BooleanArray::from(rows.iter().map(|r| r.3).collect::<Vec<_>>());
            [
                Arc::clone(batch.column_by_name(series::SAMPLES).unwrap()),
                Arc::new(sides),
                Arc::clone(batch.column_by_name(series::LABELS).unwrap()),
            ]
        };

        // Group 0: plain 1:1 match. Group 1: LHS-only unmatched group. Group 2:
        // two non-overlapping RHS series exercising `one.rows` materialization,
        // plus one filtered-out duplicate row that `opt_filter` drops.
        let part1 = make_batch(&[
            (
                &[(METRIC_NAME, "req"), ("pod", "a")],
                &[0, 10_000],
                &[2.0, 3.0],
                false,
            ),
            (
                &[(METRIC_NAME, "req"), ("pod", "lonely")],
                &[0],
                &[9.0],
                false,
            ),
            (&[(METRIC_NAME, "err_a"), ("pod", "c")], &[0], &[10.0], true),
            (
                &[(METRIC_NAME, "filtered_dup"), ("pod", "a")],
                &[0],
                &[999.0],
                true,
            ),
        ]);
        let filter1 = BooleanArray::from(vec![true, true, true, false]);

        let part2 = make_batch(&[
            (
                &[(METRIC_NAME, "err"), ("pod", "a")],
                &[0, 10_000],
                &[5.0, 7.0],
                true,
            ),
            (
                &[(METRIC_NAME, "req"), ("pod", "c")],
                &[0, 10_000],
                &[1.0, 2.0],
                false,
            ),
            (
                &[(METRIC_NAME, "err_b"), ("pod", "c")],
                &[10_000],
                &[20.0],
                true,
            ),
        ]);

        let mut p1 = GroupedBinary::from_pairing(plain(Op::Add));
        let base_size = p1.size();
        p1.update_batch(&part1, &[0, 1, 2, 0], Some(&filter1), 3)
            .unwrap();
        assert!(p1.size() > base_size);
        let s1_first = p1.state(EmitTo::First(1)).unwrap();
        let s1_rest = p1.state(EmitTo::All).unwrap();
        assert_eq!(p1.heap_size, 0);

        let mut p2 = GroupedBinary::from_pairing(plain(Op::Add));
        p2.update_batch(&part2, &[0, 1, 1], None, 2).unwrap();
        let s2_all = p2.state(EmitTo::All).unwrap();

        let mut final_acc = GroupedBinary::from_pairing(plain(Op::Add));
        final_acc.merge_batch(&s1_first, &[0], 3).unwrap();
        final_acc.merge_batch(&s1_rest, &[1, 2], 3).unwrap();
        final_acc.merge_batch(&s2_all, &[0, 2], 3).unwrap();

        let first_out = final_acc.evaluate(EmitTo::First(1)).unwrap();
        assert_eq!(
            pairs_at(&first_out, 0),
            [(lset("", "", "a", ""), vec![(0, 7.0), (10_000, 10.0)])]
        );
        let rest_out = final_acc.evaluate(EmitTo::All).unwrap();
        assert!(pairs_at(&rest_out, 0).is_empty());
        assert_eq!(
            pairs_at(&rest_out, 1),
            [(lset("", "", "c", ""), vec![(0, 11.0), (10_000, 22.0)])]
        );
        assert_eq!(final_acc.heap_size, 0);

        // Also verify DataFusion's high-cardinality partial bypass via
        // `convert_to_state` reaches the exact same merged answer.
        let converter = GroupedBinary::from_pairing(plain(Op::Add));
        let bypass1 = converter.convert_to_state(&part1, Some(&filter1)).unwrap();
        let bypass2 = converter.convert_to_state(&part2, None).unwrap();
        let mut from_bypass = GroupedBinary::from_pairing(plain(Op::Add));
        from_bypass.merge_batch(&bypass1, &[0, 1, 2, 0], 3).unwrap();
        from_bypass.merge_batch(&bypass2, &[0, 2, 2], 3).unwrap();
        let bypass_out = from_bypass.evaluate(EmitTo::All).unwrap();
        assert_eq!(
            pairs_at(&bypass_out, 0),
            [(lset("", "", "a", ""), vec![(0, 7.0), (10_000, 10.0)])]
        );
        assert!(pairs_at(&bypass_out, 1).is_empty());
        assert_eq!(
            pairs_at(&bypass_out, 2),
            [(lset("", "", "c", ""), vec![(0, 11.0), (10_000, 22.0)])]
        );
    }

    #[test]
    fn grouped_set_operators_handle_single_sided_and_matched_groups() {
        use crate::Series;

        let rows = [
            Series::new(&[(METRIC_NAME, "l"), ("pod", "only_l")], vec![0], vec![1.0]).unwrap(),
            Series::new(&[(METRIC_NAME, "r"), ("pod", "only_r")], vec![0], vec![2.0]).unwrap(),
            Series::new(
                &[(METRIC_NAME, "l"), ("pod", "both")],
                vec![0, 10_000],
                vec![3.0, 4.0],
            )
            .unwrap(),
            Series::new(&[(METRIC_NAME, "r"), ("pod", "both")], vec![0], vec![9.0]).unwrap(),
        ];
        let batch = series::encode(
            &names(&SCHEMA),
            &rows,
            series::Block {
                start_ms: 0,
                end_ms: 30_001,
            },
        )
        .unwrap();
        let cols: [ArrayRef; 3] = [
            Arc::clone(batch.column_by_name(series::SAMPLES).unwrap()),
            Arc::new(BooleanArray::from(vec![false, true, false, true])),
            Arc::clone(batch.column_by_name(series::LABELS).unwrap()),
        ];

        for (op, want_g0, want_g1, want_g2) in [
            (
                Op::And,
                vec![],
                vec![],
                vec![(lset("l", "", "both", ""), vec![(0, 3.0)])],
            ),
            (
                Op::Unless,
                vec![(lset("l", "", "only_l", ""), vec![(0, 1.0)])],
                vec![],
                vec![(lset("l", "", "both", ""), vec![(10_000, 4.0)])],
            ),
            (
                Op::Or,
                vec![(lset("l", "", "only_l", ""), vec![(0, 1.0)])],
                vec![(lset("r", "", "only_r", ""), vec![(0, 2.0)])],
                vec![(lset("l", "", "both", ""), vec![(0, 3.0), (10_000, 4.0)])],
            ),
        ] {
            let mut acc = GroupedBinary::from_pairing(plain(op));
            acc.update_batch(&cols, &[0, 1, 2, 2], None, 3).unwrap();
            let out = acc.evaluate(EmitTo::All).unwrap();
            assert_eq!(pairs_at(&out, 0), want_g0, "{}", op.as_str());
            assert_eq!(pairs_at(&out, 1), want_g1, "{}", op.as_str());
            assert_eq!(pairs_at(&out, 2), want_g2, "{}", op.as_str());
        }
    }

    #[test]
    fn empty_sample_rows_do_not_allocate_lanes_or_pollute_duplicates() {
        let mut p = plain(Op::Add);
        p.absorb(LHS, &lset("filtered_lhs", "", "a", ""), &[], &[])
            .unwrap();
        p.absorb(RHS, &lset("filtered_rhs", "", "a", ""), &[], &[])
            .unwrap();
        assert!(p.one.is_none());
        assert!(p.one_pool.is_empty());
        assert!(p.one_metrics.is_empty());
        assert!(p.many.is_empty());

        p.absorb(LHS, &lset("req", "", "a", ""), &[0], &[1.0])
            .unwrap();
        p.absorb(RHS, &lset("err_first", "", "a", ""), &[0], &[2.0])
            .unwrap();
        p.absorb(RHS, &lset("err_second", "", "a", ""), &[0], &[3.0])
            .unwrap();
        let err = p.evaluate().unwrap_err().to_string();
        assert!(err.contains("err_first"), "{err}");
        assert!(err.contains("err_second"), "{err}");
        assert!(!err.contains("filtered_rhs"), "{err}");
    }
}
