//! Instant-vector functions as one DataFusion scalar function:
//! `promql_elementwise(samples, 'abs', a, b)`.
//!
//! These are the functions that look at one sample at a time and change
//! nothing else: same series, same timestamps, a new value. Upstream
//! writes them through `simpleFloatFunc`, `clamp`, `funcRound`,
//! `dateWrapper` and `funcTimestamp` (`promql/functions.go` at 83962c35),
//! each mapping one `Sample` to one `Sample` with `DropName: true`.
//!
//! So the kernel here rebuilds only the value array and hands back the
//! timestamps and validity it was given, rebasing sliced list offsets as
//! needed. Only the referenced child span is mapped: a list slice retains
//! unrelated backing samples that must not add work or output storage.
//!
//! The scalar arguments — `round`'s `to_nearest`, `clamp`'s bounds —
//! arrive already folded to numbers. Upstream evaluates them per step,
//! which this cannot do while planning, so a scalar argument that is not
//! the same at every step is refused by name in [`crate::plan`] rather
//! than quietly taken at its first value.

use std::sync::Arc;

use chrono::{DateTime, Datelike, Timelike, Utc};
use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, Float64Array, ListArray, StructArray,
    TimestampMillisecondArray,
};
use datafusion::arrow::buffer::{BooleanBuffer, OffsetBuffer};
use datafusion::arrow::compute::FilterBuilder;
use datafusion::arrow::datatypes::{DataType, FieldRef, Float64Type, TimestampMillisecondType};
use datafusion::common::{plan_err, ScalarValue};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{
    lit, ColumnarValue, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl,
    Signature, Volatility,
};

use crate::binary::{Computed, Operand};
use crate::series;

pub const NAME: &str = "promql_elementwise";

/// The instant-vector functions that map a value to a value.
///
/// A binary operator with a scalar on one side is one of these too:
/// upstream's `VectorscalarBinop` (`promql/engine.go:3177` at 83962c35)
/// walks the vector and writes the value back into the same sample,
/// which is this kernel exactly. The scalar is folded while planning,
/// so the operator arrives here with one operand already a number.
/// A comparison is the one of these that drops samples rather than
/// rewriting them, which is why [`apply`] has a second path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Func {
    Binary {
        op: crate::binary::Op,
        /// The `bool` modifier, carried here because it decides both
        /// the value and whether `__name__` survives.
        return_bool: bool,
    },
    Abs,
    /// Unary minus on a vector: `-x`. Upstream evaluates it inline in
    /// `rangeEval`'s `UnaryExpr` arm (`promql/engine.go`) rather than as a
    /// function, and drops the name there too. Never a function name, so
    /// no call can reach it; the planner builds it from the `-` operator.
    Neg,
    Ceil,
    Floor,
    Round,
    Exp,
    Ln,
    Log2,
    Log10,
    Sqrt,
    Sgn,
    Clamp,
    ClampMin,
    ClampMax,
    Sin,
    Cos,
    Tan,
    Asin,
    Acos,
    Atan,
    Sinh,
    Cosh,
    Tanh,
    Asinh,
    Acosh,
    Atanh,
    Deg,
    Rad,
    DayOfMonth,
    DayOfWeek,
    DayOfYear,
    DaysInMonth,
    Hour,
    Minute,
    Month,
    Year,
    /// `funcTimestamp` over a vector that is not a bare selector: the
    /// samples carry the step they were evaluated at, and that is the
    /// answer. Over a bare selector the sample's own time is wanted
    /// instead, which only the selector kernel still knows
    /// ([`crate::selector::Pick`]); the planner sends that shape there.
    Timestamp,
}

impl Func {
    pub fn parse(s: &str) -> Option<Func> {
        Some(match s {
            "abs" => Func::Abs,
            "neg" => Func::Neg,
            "ceil" => Func::Ceil,
            "floor" => Func::Floor,
            "round" => Func::Round,
            "exp" => Func::Exp,
            "ln" => Func::Ln,
            "log2" => Func::Log2,
            "log10" => Func::Log10,
            "sqrt" => Func::Sqrt,
            "sgn" => Func::Sgn,
            "clamp" => Func::Clamp,
            "clamp_min" => Func::ClampMin,
            "clamp_max" => Func::ClampMax,
            "sin" => Func::Sin,
            "cos" => Func::Cos,
            "tan" => Func::Tan,
            "asin" => Func::Asin,
            "acos" => Func::Acos,
            "atan" => Func::Atan,
            "sinh" => Func::Sinh,
            "cosh" => Func::Cosh,
            "tanh" => Func::Tanh,
            "asinh" => Func::Asinh,
            "acosh" => Func::Acosh,
            "atanh" => Func::Atanh,
            "deg" => Func::Deg,
            "rad" => Func::Rad,
            "day_of_month" => Func::DayOfMonth,
            "day_of_week" => Func::DayOfWeek,
            "day_of_year" => Func::DayOfYear,
            "days_in_month" => Func::DaysInMonth,
            "hour" => Func::Hour,
            "minute" => Func::Minute,
            "month" => Func::Month,
            "year" => Func::Year,
            "timestamp" => Func::Timestamp,
            // Last, and reachable only from a plan's own literal: an
            // operator's spelling is never a function name, so no call
            // can land here.
            other => {
                let (op, return_bool) = crate::binary::parse_literal(other)?;
                return Some(Func::Binary { op, return_bool });
            }
        })
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Func::Binary { op, return_bool } => crate::binary::literal(*op, *return_bool),
            Func::Abs => "abs",
            Func::Neg => "neg",
            Func::Ceil => "ceil",
            Func::Floor => "floor",
            Func::Round => "round",
            Func::Exp => "exp",
            Func::Ln => "ln",
            Func::Log2 => "log2",
            Func::Log10 => "log10",
            Func::Sqrt => "sqrt",
            Func::Sgn => "sgn",
            Func::Clamp => "clamp",
            Func::ClampMin => "clamp_min",
            Func::ClampMax => "clamp_max",
            Func::Sin => "sin",
            Func::Cos => "cos",
            Func::Tan => "tan",
            Func::Asin => "asin",
            Func::Acos => "acos",
            Func::Atan => "atan",
            Func::Sinh => "sinh",
            Func::Cosh => "cosh",
            Func::Tanh => "tanh",
            Func::Asinh => "asinh",
            Func::Acosh => "acosh",
            Func::Atanh => "atanh",
            Func::Deg => "deg",
            Func::Rad => "rad",
            Func::DayOfMonth => "day_of_month",
            Func::DayOfWeek => "day_of_week",
            Func::DayOfYear => "day_of_year",
            Func::DaysInMonth => "days_in_month",
            Func::Hour => "hour",
            Func::Minute => "minute",
            Func::Month => "month",
            Func::Year => "year",
            Func::Timestamp => "timestamp",
        }
    }

    /// Every function here sets `DropName: true` on the sample it
    /// emits. A binary operator answers for itself: a comparison
    /// without `bool` hands back a sample that was already there, and
    /// it is still that metric.
    pub fn drops_metric_name(&self) -> bool {
        match self {
            Func::Binary { op, return_bool } => op.drops_metric_name(*return_bool),
            _ => true,
        }
    }

    /// The function with its scalar arguments folded in, ready to run
    /// over values. `a` and `b` are the call's arguments after the
    /// vector, absent where the call did not give them.
    ///
    /// `None` is the empty result: `clamp` with `max < min` drops every
    /// sample (`clamp`, `promql/functions.go:705-707` at 83962c35), however
    /// many series came in. It is not a `Bound` variant because such a
    /// variant has no value to compute, and `Bound::value` would have to
    /// panic on it.
    ///
    /// A binary operator has no second argument to spend, so the two
    /// slots say which side its scalar was written on: `a` for
    /// `vector op scalar`, `b` for `scalar op vector`. Upstream's
    /// `VectorscalarBinop` carries the same fact as its `swap` flag.
    pub fn bind(self, a: Option<f64>, b: Option<f64>) -> Option<Bound> {
        Some(match self {
            Func::Binary { op, return_bool } => match (a, b) {
                (None, Some(scalar)) => Bound::Binary {
                    op,
                    return_bool,
                    scalar,
                    swap: true,
                },
                // A call with neither operand cannot come from the
                // planner; reachable from SQL, where a missing number
                // is a NaN rather than a panic.
                (a, _) => Bound::Binary {
                    op,
                    return_bool,
                    scalar: a.unwrap_or(f64::NAN),
                    swap: false,
                },
            },
            // `round(v)` is `round(v, 1)`: the default is in upstream's
            // signature, not in a branch.
            Func::Round => Bound::Round(a.unwrap_or(1.0)),
            Func::Clamp => Bound::clamp(a.unwrap_or(f64::NAN), b.unwrap_or(f64::NAN))?,
            Func::ClampMin => Bound::clamp(a.unwrap_or(f64::NAN), f64::INFINITY)?,
            Func::ClampMax => Bound::clamp(f64::NEG_INFINITY, a.unwrap_or(f64::NAN))?,
            Func::Timestamp => Bound::Timestamp,
            other => Bound::Map(other.map_fn()),
        })
    }

    /// The `func(float64) float64` upstream hands `simpleFloatFunc`.
    fn map_fn(self) -> fn(f64) -> f64 {
        match self {
            Func::Abs => f64::abs,
            Func::Neg => |v| -v,
            Func::Ceil => f64::ceil,
            Func::Floor => f64::floor,
            Func::Exp => f64::exp,
            Func::Ln => f64::ln,
            Func::Log2 => f64::log2,
            Func::Log10 => f64::log10,
            Func::Sqrt => f64::sqrt,
            Func::Sgn => sgn,
            Func::Sin => f64::sin,
            Func::Cos => f64::cos,
            Func::Tan => f64::tan,
            Func::Asin => f64::asin,
            Func::Acos => f64::acos,
            Func::Atan => f64::atan,
            Func::Sinh => f64::sinh,
            Func::Cosh => f64::cosh,
            Func::Tanh => f64::tanh,
            Func::Asinh => f64::asinh,
            Func::Acosh => f64::acosh,
            Func::Atanh => f64::atanh,
            Func::Deg => |v| v * 180.0 / std::f64::consts::PI,
            Func::Rad => |v| v * std::f64::consts::PI / 180.0,
            Func::DayOfMonth => |v| civil(v, |t| t.day()),
            // Sunday is 0, as `time.Weekday` counts.
            Func::DayOfWeek => |v| civil(v, |t| t.weekday().num_days_from_sunday()),
            Func::DayOfYear => |v| civil(v, |t| t.ordinal()),
            // `funcDaysInMonth`'s `32 - time.Date(y, m, 32, …).Day()`: the
            // month's length, which chrono answers without the overflow trick.
            Func::DaysInMonth => |v| civil(v, |t| t.num_days_in_month()),
            Func::Hour => |v| civil(v, |t| t.hour()),
            Func::Minute => |v| civil(v, |t| t.minute()),
            Func::Month => |v| civil(v, |t| t.month()),
            Func::Year => year,
            Func::Binary { .. }
            | Func::Round
            | Func::Clamp
            | Func::ClampMin
            | Func::ClampMax
            | Func::Timestamp => {
                unreachable!("{} takes its arguments through bind", self.as_str())
            }
        }
    }
}

/// `dateWrapper`'s `time.Unix(int64(v), 0).UTC()` with one of its eight
/// callers' field reads applied: proleptic Gregorian in UTC, as Go's
/// `time` is and as chrono's `DateTime<Utc>` is. The year is the one
/// field that does not go through here, see [`year`].
///
/// Go's `int64(v)` truncates toward zero, as `as i64` does, and leaves a
/// NaN, an infinity or a value past `int64` to the CPU. `as` saturates
/// those to the nearer end, which is Go's own answer on arm64, and on
/// amd64 for everything but NaN: there the CPU answers `math.MinInt64`
/// for all of them, which reads as the same date as `MaxInt64` because
/// Go's wrap puts the first second of `int64` one second after its last
/// (see [`beyond_chrono`]), where `as` makes a NaN zero and so the
/// epoch. No query in the corpus observes the difference, and a check
/// for it on every sample is not worth one CPU's answer to a NaN date.
///
/// The match is written out here and in [`year`] rather than behind one
/// shared function so that the hot arm stays the two calls it was: with
/// the fallback's arithmetic in the same body the compiler stopped
/// fusing chrono's field read with `from_timestamp`, and every extractor
/// paid a tenth more per sample for a branch it never takes.
fn civil<T: Into<f64>>(v: f64, read: impl FnOnce(DateTime<Utc>) -> T) -> f64 {
    let secs = v as i64;
    match DateTime::from_timestamp(secs, 0) {
        Some(t) => read(t).into(),
        None => read(beyond_chrono(secs).0).into(),
    }
}

/// `funcYear`: the one field the 400-year shift in [`beyond_chrono`]
/// moves, so it alone restores the shift.
fn year(v: f64) -> f64 {
    let secs = v as i64;
    match DateTime::from_timestamp(secs, 0) {
        Some(t) => f64::from(t.year()),
        None => {
            let (t, cycles) = beyond_chrono(secs);
            (i64::from(t.year()) + 400 * cycles) as f64
        }
    }
}

/// Seconds in Gregorian 400-year cycle: 146,097 days, which is also a
/// whole number of weeks, so a date shifted by whole cycles keeps its
/// month, day, weekday, day of the year, leap-year status and time of
/// day, and moves only in its year.
const CYCLE_SECS: i128 = 146_097 * 86_400;

/// 2000-01-01 UTC, the first second of a cycle that chrono's calendar
/// holds whole.
const CYCLE_ANCHOR_SECS: i128 = 946_684_800;

/// Go's `unixToAbsolute` (`time/time.go`, Go 1.25 as Prometheus pins):
/// the seconds from its absolute zero, March 1 of the year
/// -292277022400, to the Unix epoch.
const UNIX_TO_ABSOLUTE: i128 = 9_223_372_028_741_760_000;

/// `time.Unix(secs, 0).UTC()` for a second chrono's calendar does not
/// hold, as a chrono time in the years 2000 to 2399 plus the number of
/// 400-year cycles it was moved by to get there.
///
/// chrono's calendar ends at the years ±262143, some 8.2 trillion
/// seconds from the epoch, where Go's keeps counting to the ends of
/// `int64`. Moved by whole cycles a date keeps every field but its year,
/// so the fields are read where chrono can read them and only [`year`]
/// has anything to restore.
///
/// Go's `time` counts seconds from its absolute zero in a `uint64`, and
/// its `int64` addition on the way wraps rather than fails, so the last
/// 257 years of `int64` before the zero come out as the years after its
/// end instead (`Time.absSec`); the modulus here is that wrap, and it
/// is what makes an infinity, saturated to either end, read as one date.
#[cold]
#[inline(never)]
fn beyond_chrono(secs: i64) -> (DateTime<Utc>, i64) {
    let unix = (i128::from(secs) + UNIX_TO_ABSOLUTE).rem_euclid(1 << 64) - UNIX_TO_ABSOLUTE;
    let cycles = (unix - CYCLE_ANCHOR_SECS).div_euclid(CYCLE_SECS);
    let shifted = (unix - cycles * CYCLE_SECS) as i64;
    let t = DateTime::from_timestamp(shifted, 0)
        .expect("a second in the years 2000 to 2399 is a chrono date");
    (t, cycles as i64)
}

/// `funcSgn`: zero and NaN come back as themselves, so the sign of `-0`
/// survives and a NaN does not become a 1.
fn sgn(v: f64) -> f64 {
    if v < 0.0 {
        -1.0
    } else if v > 0.0 {
        1.0
    } else {
        v
    }
}

/// A function with its arguments applied, one value in, one out.
///
/// Not `PartialEq`: a variant holds a function pointer, and comparing
/// those compares addresses the linker is free to merge or duplicate.
#[derive(Debug, Clone, Copy)]
pub enum Bound {
    Map(fn(f64) -> f64),
    /// A binary operator with the scalar operand folded in. `swap` is
    /// upstream's: it says the scalar was written on the left, and the
    /// operands go back in that order for the ones that care.
    Binary {
        op: crate::binary::Op,
        return_bool: bool,
        scalar: f64,
        swap: bool,
    },
    Round(f64),
    Clamp {
        min: f64,
        max: f64,
    },
    /// Reads the timestamp column instead of the value column; see
    /// [`Func::Timestamp`].
    Timestamp,
}

impl Bound {
    fn clamp(min: f64, max: f64) -> Option<Bound> {
        // Not `max >= min`: a NaN bound is not inverted, it clamps to NaN.
        if max < min {
            None
        } else {
            Some(Bound::Clamp { min, max })
        }
    }

    /// One sample's new value. A binary operator runs over the whole
    /// value array through [`crate::binary::Op::kernel`] instead, in
    /// [`apply`].
    pub fn value(&self, v: f64) -> f64 {
        match self {
            Bound::Map(f) => f(v),
            // Inverted as upstream inverts it: dividing by `to_nearest`
            // and multiplying back is a different float from multiplying
            // by its reciprocal, and upstream took the reciprocal.
            Bound::Round(to_nearest) => {
                let inverse = 1.0 / to_nearest;
                (v * inverse + 0.5).floor() / inverse
            }
            Bound::Clamp { min, max } => go_max(*min, go_min(*max, v)),
            Bound::Binary { .. } => unreachable!("a binary operator runs over whole lanes"),
            Bound::Timestamp => unreachable!("timestamp reads the timestamp column, not a value"),
        }
    }
}

/// Go's `math.Min` and `math.Max`, which answer NaN when either operand
/// is one. Rust's `f64::min`/`f64::max` return the *other* operand
/// instead, which would make `clamp` turn a NaN sample into a bound.
fn go_min(x: f64, y: f64) -> f64 {
    if x.is_nan() || y.is_nan() {
        f64::NAN
    } else {
        x.min(y)
    }
}

fn go_max(x: f64, y: f64) -> f64 {
    if x.is_nan() || y.is_nan() {
        f64::NAN
    } else {
        x.max(y)
    }
}

/// The DataFusion function. Stateless: the operator and its arguments
/// are literals, as in [`crate::selector`].
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct Elementwise {
    signature: Signature,
}

impl Default for Elementwise {
    fn default() -> Self {
        Self {
            signature: Signature::any(4, Volatility::Immutable),
        }
    }
}

pub fn udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(Elementwise::default())
}

/// `promql_elementwise(samples, 'abs', a, b)`. The two scalar arguments
/// are always present in the plan, NULL where the call had none, so one
/// signature serves every function.
pub fn call(samples: Expr, func: Func, a: Option<f64>, b: Option<f64>) -> Expr {
    udf().call(vec![
        samples,
        lit(func.as_str()),
        lit(ScalarValue::Float64(a)),
        lit(ScalarValue::Float64(b)),
    ])
}

impl ScalarUDFImpl for Elementwise {
    fn name(&self) -> &str {
        NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if !arg_types.first().is_some_and(series::is_samples_type) {
            return plan_err!(
                "{NAME}: first argument must be {}, got {:?}",
                series::samples_type(),
                arg_types.first()
            );
        }
        if !matches!(arg_types.get(1), Some(DataType::Utf8)) {
            return plan_err!("{NAME}: the function name must be a string");
        }
        for (i, t) in arg_types.iter().enumerate().skip(2) {
            if !matches!(t, DataType::Float64 | DataType::Null) {
                return plan_err!("{NAME}: argument {i} must be Float64, got {t}");
            }
        }
        Ok(arg_types[0].clone())
    }

    /// As in [`crate::selector`]: a null row maps to a null row, and
    /// anything else would leave the canonical shape.
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let types: Vec<DataType> = args
            .arg_fields
            .iter()
            .map(|f| f.data_type().clone())
            .collect();
        let data_type = self.return_type(&types)?;
        Ok(Arc::new(
            args.arg_fields[0]
                .as_ref()
                .clone()
                .with_name(NAME)
                .with_data_type(data_type),
        ))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let samples: ArrayRef = match &args.args[0] {
            ColumnarValue::Array(a) => Arc::clone(a),
            ColumnarValue::Scalar(s) => s.to_array_of_size(args.number_rows)?,
        };
        let name = match &args.args[1] {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => s.clone(),
            other => {
                return Err(DataFusionError::Execution(format!(
                    "{NAME}: the function name must be a string literal, got {:?}",
                    other.data_type()
                )))
            }
        };
        let func = Func::parse(&name).ok_or_else(|| {
            DataFusionError::Execution(format!("{NAME}: unknown function {name}"))
        })?;
        let bound = func.bind(float_arg(&args, 2)?, float_arg(&args, 3)?);
        Ok(ColumnarValue::Array(Arc::new(apply(
            samples.as_list::<i32>(),
            bound,
        )?)))
    }
}

/// Read literal argument `i` as a `Float64`, `None` for SQL NULL.
fn float_arg(args: &ScalarFunctionArgs, i: usize) -> Result<Option<f64>> {
    match &args.args[i] {
        ColumnarValue::Scalar(ScalarValue::Float64(v)) => Ok(*v),
        ColumnarValue::Scalar(ScalarValue::Null) => Ok(None),
        other => Err(DataFusionError::Execution(format!(
            "{NAME}: argument {i} must be a Float64 literal, got {:?}",
            other.data_type()
        ))),
    }
}

/// Run `bound` over every value of a samples column.
///
/// A function moves no sample between series, so a row is still a
/// series and the timestamps keep their values. What the row boundaries
/// do depends on the bound: a function rewrites every sample and the
/// offsets are rebased if sliced, while a comparison answers for some of
/// them and nothing for the rest, so the rows are rebuilt around what
/// survived — upstream's `keep`, which leaves a sample out of the
/// Vector entirely.
///
/// `None` is [`Func::bind`]'s empty result: every series comes back empty.
pub fn apply(samples: &ListArray, bound: Option<Bound>) -> Result<ListArray> {
    let Some(bound) = bound else {
        return Ok(empty_rows(samples));
    };

    let offsets = samples.offsets();
    let start = offsets[0] as usize;
    let end = offsets[samples.len()] as usize;
    let backing = samples.values().as_struct();
    let entries = if start == 0 && end == backing.len() {
        backing.clone()
    } else {
        backing.slice(start, end - start)
    };
    let timestamps = entries
        .column_by_name(series::TIMESTAMP)
        .expect("validated by return_type");
    let values = entries
        .column_by_name(series::VALUE)
        .expect("validated by return_type")
        .as_primitive::<Float64Type>();

    let mapped = match bound {
        Bound::Binary {
            op,
            return_bool,
            scalar,
            swap,
        } => {
            let (vector, scalar) = (Operand::Lane(values), Operand::Scalar(scalar));
            let (lhs, rhs) = if swap {
                (scalar, vector)
            } else {
                (vector, scalar)
            };
            match op.kernel(lhs, rhs, return_bool)? {
                Computed::Values(values) => values,
                // Upstream keeps the *vector* element's value whichever
                // side the scalar was written on, which only shows when
                // a comparison is swapped: `1 < x` answers with x.
                Computed::Holds(holds) => return filtered(samples, timestamps, values, &holds),
            }
        }
        // `float64(el.T) / 1000`: the sample's own time in seconds.
        Bound::Timestamp => timestamps
            .as_primitive::<TimestampMillisecondType>()
            .values()
            .iter()
            .map(|t| *t as f64 / 1000.0)
            .collect(),
        _ => values.values().iter().map(|v| bound.value(*v)).collect(),
    };
    let offsets = if start == 0 {
        offsets.clone()
    } else {
        OffsetBuffer::new(
            offsets
                .iter()
                .map(|o| o - start as i32)
                .collect::<Vec<_>>()
                .into(),
        )
    };
    let entries = StructArray::new(
        entries.fields().clone(),
        vec![Arc::clone(timestamps), Arc::new(mapped)],
        None,
    );
    let DataType::List(item) = samples.data_type() else {
        unreachable!("a samples list")
    };
    Ok(ListArray::new(
        Arc::clone(item),
        offsets,
        Arc::new(entries),
        samples.nulls().cloned(),
    ))
}

/// [`apply`] for a comparison that drops samples: the rows are rebuilt
/// from the ones `holds` kept, so the offsets move with them.
///
/// The kernel's `timestamps`, `values` and `holds` cover only the referenced
/// child span, not the whole backing array. Original row offsets still
/// address that backing array, so predicate counts subtract its first offset.
/// Matrix selection shares this contract with filtering comparisons.
pub(crate) fn filtered(
    samples: &ListArray,
    timestamps: &ArrayRef,
    values: &Float64Array,
    holds: &BooleanBuffer,
) -> Result<ListArray> {
    let offsets = samples.offsets();
    let first = offsets[0] as usize;
    let predicate = FilterBuilder::new(&BooleanArray::new(holds.clone(), None))
        .optimize()
        .build();
    let entries = StructArray::new(
        samples.values().as_struct().fields().clone(),
        vec![
            predicate.filter(timestamps.as_ref())?,
            predicate.filter(values)?,
        ],
        None,
    );
    let kept = offsets.windows(2).map(|row| {
        let (lo, hi) = (row[0] as usize - first, row[1] as usize - first);
        holds.slice(lo, hi - lo).count_set_bits()
    });
    let DataType::List(item) = samples.data_type() else {
        unreachable!("a samples list")
    };
    Ok(ListArray::new(
        Arc::clone(item),
        OffsetBuffer::from_lengths(kept),
        Arc::new(entries),
        samples.nulls().cloned(),
    ))
}

/// `len` rows, every one of them an empty series.
fn empty_rows(samples: &ListArray) -> ListArray {
    let entries = StructArray::new(
        samples.values().as_struct().fields().clone(),
        vec![
            Arc::new(TimestampMillisecondArray::from(Vec::<i64>::new())),
            Arc::new(Float64Array::from(Vec::<f64>::new())),
        ],
        None,
    );
    let DataType::List(item) = samples.data_type() else {
        unreachable!("a samples list")
    };
    ListArray::new(
        Arc::clone(item),
        OffsetBuffer::new(vec![0i32; samples.len() + 1].into()),
        Arc::new(entries),
        samples.nulls().cloned(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn value(name: &str, v: f64) -> f64 {
        Func::parse(name)
            .expect(name)
            .bind(None, None)
            .expect(name)
            .value(v)
    }

    #[test]
    fn the_simple_functions_are_gos_math() {
        assert_eq!(value("abs", -1.5), 1.5);
        assert_eq!(value("ceil", 1.2), 2.0);
        assert_eq!(value("floor", 1.8), 1.0);
        assert_eq!(value("exp", 0.0), 1.0);
        assert_eq!(value("ln", 1.0), 0.0);
        assert_eq!(value("log2", 8.0), 3.0);
        assert_eq!(value("log10", 1000.0), 3.0);
        assert_eq!(value("sqrt", 9.0), 3.0);
        assert_eq!(value("deg", std::f64::consts::PI), 180.0);
        assert_eq!(value("rad", 180.0), std::f64::consts::PI);
        assert_eq!(value("sin", 0.0), 0.0);
        assert_eq!(value("cos", 0.0), 1.0);

        // The edges Go's math is specified at, and Rust's agrees.
        assert_eq!(value("ln", 0.0), f64::NEG_INFINITY);
        assert!(value("ln", -1.0).is_nan());
        assert!(value("sqrt", -1.0).is_nan());
        assert!(value("asin", 2.0).is_nan());
        assert_eq!(value("exp", f64::INFINITY), f64::INFINITY);
        assert_eq!(value("exp", f64::NEG_INFINITY), 0.0);
    }

    /// `funcSgn` returns the value itself for zero and NaN, so `-0`
    /// stays negative and a NaN never becomes a number.
    #[test]
    fn sgn_keeps_the_values_that_have_no_sign() {
        assert_eq!(value("sgn", -7.0), -1.0);
        assert_eq!(value("sgn", 7.0), 1.0);
        assert_eq!(value("sgn", 0.0).to_bits(), 0.0f64.to_bits());
        assert_eq!(value("sgn", -0.0).to_bits(), (-0.0f64).to_bits());
        assert!(value("sgn", f64::NAN).is_nan());
    }

    /// A NaN in, a NaN out, for every function that has nothing else to
    /// say about one.
    #[test]
    fn a_nan_propagates() {
        for name in [
            "abs", "ceil", "floor", "exp", "ln", "log2", "log10", "sqrt", "sin", "cos", "tan",
            "asin", "acos", "atan", "sinh", "cosh", "tanh", "asinh", "acosh", "atanh", "deg",
            "rad",
        ] {
            assert!(value(name, f64::NAN).is_nan(), "{name}");
        }
    }

    /// `funcRound` ties round up — away from zero for a positive value
    /// and towards zero for a negative one, which is what
    /// `floor(v + 0.5)` does and what `math.Round` does not.
    #[test]
    fn round_settles_ties_upwards() {
        let round = |v: f64, to: Option<f64>| Func::Round.bind(to, None).unwrap().value(v);
        assert_eq!(round(2.5, None), 3.0);
        assert_eq!(round(-2.5, None), -2.0);
        assert_eq!(round(1.4, None), 1.0);
        assert_eq!(round(-1.4, None), -1.0);
        // to_nearest is a step, not a digit count.
        assert_eq!(round(2.6, Some(5.0)), 5.0);
        assert_eq!(round(2.4, Some(5.0)), 0.0);
        assert_eq!(round(1.234, Some(0.1)), 1.2);
        // A to_nearest of zero is an infinite inverse: upstream answers
        // NaN rather than refusing the call.
        assert!(round(1.0, Some(0.0)).is_nan());
    }

    /// The corpus's `functions.test` dates, each a Go `time` edge: the
    /// leap second's two sides, a February 29th and the March 1st after
    /// it, the 366th day, and a Monday against `time.Weekday`'s Sunday.
    #[test]
    fn the_date_functions_read_gos_utc_calendar() {
        let at = |name: &str, secs: f64| value(name, secs);
        // 2006-01-02 22:04:05 UTC, a Monday.
        assert_eq!(at("year", 1_136_239_445.0), 2006.0);
        assert_eq!(at("month", 1_136_239_445.0), 1.0);
        assert_eq!(at("day_of_month", 1_136_239_445.0), 2.0);
        assert_eq!(at("day_of_year", 1_136_239_445.0), 2.0);
        assert_eq!(at("day_of_week", 1_136_239_445.0), 1.0);
        assert_eq!(at("hour", 1_136_239_445.0), 22.0);
        assert_eq!(at("minute", 1_136_239_445.0), 4.0);
        // The epoch, a Thursday.
        assert_eq!(at("year", 0.0), 1970.0);
        assert_eq!(at("day_of_week", 0.0), 4.0);
        assert_eq!(at("day_of_year", 0.0), 1.0);
        // 2008-12-31 23:59:59 and 2009-01-01 00:00:00.
        assert_eq!(at("year", 1_230_767_999.0), 2008.0);
        assert_eq!(at("year", 1_230_768_000.0), 2009.0);
        // 2016-02-29 23:59:59 and 2016-03-01 00:00:00.
        assert_eq!(at("month", 1_456_790_399.0), 2.0);
        assert_eq!(at("day_of_month", 1_456_790_399.0), 29.0);
        assert_eq!(at("month", 1_456_790_400.0), 3.0);
        assert_eq!(at("day_of_month", 1_456_790_400.0), 1.0);
        // The last day of a leap year and of a common one.
        assert_eq!(at("day_of_year", 1_483_191_420.0), 366.0);
        assert_eq!(at("day_of_year", 1_672_493_820.0), 365.0);
        // February 2016 and February 2017.
        assert_eq!(at("days_in_month", 1_454_284_800.0), 29.0);
        assert_eq!(at("days_in_month", 1_485_907_200.0), 28.0);
        // A 30-day month, and a century year that is not a leap year.
        assert_eq!(at("days_in_month", 1_491_004_800.0), 30.0);
        assert_eq!(at("days_in_month", 4_105_123_200.0), 28.0);
    }

    /// Every field of one date, in the order the extractors are named.
    fn fields(secs: f64) -> [f64; 8] {
        [
            "day_of_month",
            "day_of_week",
            "day_of_year",
            "days_in_month",
            "hour",
            "minute",
            "month",
            "year",
        ]
        .map(|name| value(name, secs))
    }

    /// chrono's calendar stops at the years ±262143 and Go's does not.
    /// The expectations here and below are Go 1.25's `time` run by hand
    /// (`absSeconds.days().split()`, `time/time.go`), the calendar
    /// Prometheus at 83962c35 answers with.
    #[test]
    fn the_calendar_continues_past_chronos_edges() {
        // The last second chrono holds on each side, and the first it
        // does not: 262142-12-31 23:59:59, then 262143-01-01, and
        // -262143-01-01 00:00:00, then the leap day's year before it.
        assert_eq!(
            fields(8_210_266_876_799.0),
            [31.0, 1.0, 365.0, 31.0, 23.0, 59.0, 12.0, 262_142.0]
        );
        assert_eq!(
            fields(8_210_266_876_800.0),
            [1.0, 2.0, 1.0, 31.0, 0.0, 0.0, 1.0, 262_143.0]
        );
        assert_eq!(
            fields(-8_334_601_228_800.0),
            [1.0, 4.0, 1.0, 31.0, 0.0, 0.0, 1.0, -262_143.0]
        );
        assert_eq!(
            fields(-8_334_601_228_801.0),
            [31.0, 3.0, 366.0, 31.0, 23.0, 59.0, 12.0, -262_144.0]
        );
    }

    /// Ten trillion seconds either side of the epoch: 318857-05-20
    /// 17:46:40, a Sunday, and -314918-08-13 06:13:20, also a Sunday.
    #[test]
    fn a_far_date_keeps_every_field_but_the_year_from_its_cycle() {
        assert_eq!(
            fields(10_000_000_000_000.0),
            [20.0, 0.0, 140.0, 31.0, 17.0, 46.0, 5.0, 318_857.0]
        );
        assert_eq!(
            fields(-10_000_000_000_000.0),
            [13.0, 0.0, 225.0, 31.0, 6.0, 13.0, 8.0, -314_918.0]
        );
    }

    /// `int64`'s last second is 292277026596-12-04 15:30:07, Go's
    /// documented end of time. Its first second is one second later by
    /// Go's wrap, so an infinity or a float past `int64` lands on the
    /// same date whichever end the conversion saturates it to. A NaN is
    /// the one input where Go's answer is the CPU's: zero on arm64 and
    /// here, the epoch; `MinInt64` on amd64, which is this same end.
    #[test]
    fn the_ends_of_int64_meet_where_go_wraps() {
        let end = [4.0, 0.0, 339.0, 31.0, 15.0, 30.0, 12.0, 292_277_026_596.0];
        assert_eq!(fields(9_223_372_036_854_775_807.0), end);
        assert_eq!(fields(-9_223_372_036_854_775_808.0), end);
        for v in [f64::INFINITY, f64::NEG_INFINITY, 1e19, -1e19] {
            assert_eq!(fields(v), end, "{v}");
        }
        assert_eq!(fields(f64::NAN), fields(0.0));
        // One second before the wrap is Go's absolute zero, March 1 of
        // the year -292277022400, a Wednesday.
        assert_eq!(
            fields(-9_223_372_028_741_760_000.0),
            [1.0, 3.0, 61.0, 31.0, 0.0, 0.0, 3.0, -292_277_022_400.0]
        );
    }

    /// `int64(v)` truncates toward zero: `-9.75` is `-9`, nine seconds
    /// before the epoch, which is the last day of 1969 and not, as a
    /// floor would make it, ten seconds before.
    #[test]
    fn a_fractional_timestamp_is_truncated_toward_zero() {
        assert_eq!(value("year", -9.75), 1969.0);
        assert_eq!(value("month", -9.75), 12.0);
        assert_eq!(value("day_of_month", -9.75), 31.0);
        assert_eq!(value("day_of_year", -9.75), 365.0);
        assert_eq!(value("day_of_week", -9.75), 3.0);
        assert_eq!(value("hour", -9.75), 23.0);
        assert_eq!(value("minute", -9.75), 59.0);
        assert_eq!(value("minute", 119.9), 1.0);
        // Half a second before the epoch is still the epoch's second.
        assert_eq!(value("year", -0.5), 1970.0);
        assert_eq!(value("year", -1.0), 1969.0);
    }

    /// `funcTimestamp` answers with the sample's time, in seconds, and
    /// never looks at its value.
    #[test]
    fn timestamp_reads_the_timestamp_column() {
        let out = apply(&samples(), Func::Timestamp.bind(None, None)).unwrap();
        assert_eq!(out.offsets(), samples().offsets());
        assert_eq!(values_of(&out), vec![0.0, 0.001, 0.007]);
    }

    #[test]
    fn clamp_is_max_of_min_and_bounds_that_cross_give_nothing() {
        let clamp = |v: f64, min, max| Func::Clamp.bind(Some(min), Some(max)).unwrap().value(v);
        assert_eq!(clamp(5.0, 0.0, 1.0), 1.0);
        assert_eq!(clamp(-5.0, 0.0, 1.0), 0.0);
        assert_eq!(clamp(0.5, 0.0, 1.0), 0.5);
        assert!(Func::Clamp.bind(Some(1.0), Some(0.0)).is_none());
        // Equal bounds still clamp; only max < min is empty.
        assert_eq!(clamp(5.0, 1.0, 1.0), 1.0);
        // Go's math.Min and math.Max propagate a NaN; Rust's return the
        // other operand, which would clamp a NaN to a bound.
        assert!(clamp(f64::NAN, 0.0, 1.0).is_nan());

        let clamp_min = |v: f64| Func::ClampMin.bind(Some(3.0), None).unwrap().value(v);
        let clamp_max = |v: f64| Func::ClampMax.bind(Some(3.0), None).unwrap().value(v);
        assert_eq!(clamp_min(1.0), 3.0);
        assert_eq!(clamp_min(9.0), 9.0);
        assert_eq!(clamp_max(9.0), 3.0);
        assert_eq!(clamp_max(1.0), 1.0);
        // `min.max(max.min(v))` with a NaN bound answers as Go's
        // math.Max(min, math.Min(max, v)) does.
        assert!(Func::ClampMin
            .bind(Some(f64::NAN), None)
            .unwrap()
            .value(1.0)
            .is_nan());
    }

    #[test]
    fn every_name_round_trips() {
        for func in [
            Func::Abs,
            Func::Ceil,
            Func::Floor,
            Func::Round,
            Func::Exp,
            Func::Ln,
            Func::Log2,
            Func::Log10,
            Func::Sqrt,
            Func::Sgn,
            Func::Clamp,
            Func::ClampMin,
            Func::ClampMax,
            Func::Sin,
            Func::Cos,
            Func::Tan,
            Func::Asin,
            Func::Acos,
            Func::Atan,
            Func::Sinh,
            Func::Cosh,
            Func::Tanh,
            Func::Asinh,
            Func::Acosh,
            Func::Atanh,
            Func::Deg,
            Func::Rad,
        ] {
            assert_eq!(Func::parse(func.as_str()), Some(func));
            assert!(crate::function::signature(func.as_str()).is_some());
        }
        // The operator's own spelling: it round-trips, but names no
        // PromQL function, so it has no signature.
        assert_eq!(Func::parse(Func::Neg.as_str()), Some(Func::Neg));
        assert!(crate::function::signature(Func::Neg.as_str()).is_none());
        assert_eq!(Func::parse("rate"), None);
    }

    fn samples() -> ListArray {
        let batch = series::encode(
            &["s".to_string()],
            &[
                series::Series::new(&[("s", "a")], vec![0, 1], vec![-1.0, 2.5]).unwrap(),
                series::Series::new(&[("s", "b")], vec![], vec![]).unwrap(),
                series::Series::new(&[("s", "c")], vec![7], vec![-9.0]).unwrap(),
            ],
            series::ONE_BLOCK,
        )
        .unwrap();
        batch
            .column_by_name(series::SAMPLES)
            .unwrap()
            .as_list::<i32>()
            .clone()
    }

    fn values_of(list: &ListArray) -> Vec<f64> {
        list.values()
            .as_struct()
            .column_by_name(series::VALUE)
            .unwrap()
            .as_primitive::<Float64Type>()
            .values()
            .to_vec()
    }

    #[test]
    fn neg_flips_every_sign_and_drops_the_name() {
        let input = samples();
        let out = apply(&input, Func::Neg.bind(None, None)).unwrap();
        let expect: Vec<f64> = values_of(&input).iter().map(|v| -v).collect();
        assert_eq!(values_of(&out), expect);
        assert!(Func::Neg.drops_metric_name());
    }

    #[test]
    fn apply_keeps_the_rows_and_the_timestamps() {
        let input = samples();
        let out = apply(&input, Func::Abs.bind(None, None)).unwrap();
        assert_eq!(out.len(), 3);
        assert_eq!(out.offsets(), input.offsets());
        assert_eq!(values_of(&out), [1.0, 2.5, 9.0]);
        let ts = out
            .values()
            .as_struct()
            .column_by_name(series::TIMESTAMP)
            .unwrap();
        assert_eq!(
            ts.as_ref(),
            input
                .values()
                .as_struct()
                .column_by_name(series::TIMESTAMP)
                .unwrap()
                .as_ref()
        );
    }

    #[test]
    fn apply_maps_only_the_referenced_child_span_and_shares_timestamps() {
        fn selected_only(v: f64) -> f64 {
            assert_eq!(v, -9.0, "unreferenced backing samples must not be mapped");
            v.abs()
        }
        let backing = samples();
        let input = backing.slice(1, 2);
        assert_eq!(input.offsets().as_ref(), &[2, 2, 3]);
        let out = apply(&input, Some(Bound::Map(selected_only))).unwrap();
        assert_eq!(out.offsets().as_ref(), &[0, 0, 1]);
        assert_eq!(values_of(&out), [9.0]);
        assert_eq!(out.values().len(), 1);
        let original_ts = backing
            .values()
            .as_struct()
            .column_by_name(series::TIMESTAMP)
            .unwrap()
            .as_primitive::<TimestampMillisecondType>();
        let mapped_ts = out
            .values()
            .as_struct()
            .column_by_name(series::TIMESTAMP)
            .unwrap()
            .as_primitive::<TimestampMillisecondType>();
        assert_eq!(mapped_ts.values().as_ref(), &[7]);
        assert_eq!(
            mapped_ts.values().as_ptr(),
            original_ts.values()[2..].as_ptr()
        );

        let prefix = apply(
            &backing.slice(0, 2),
            Some(Bound::Map(|v| {
                assert_ne!(v, -9.0, "unreferenced suffix must not be mapped");
                v.abs()
            })),
        )
        .unwrap();
        assert_eq!(prefix.offsets().as_ref(), &[0, 2, 2]);
        assert_eq!(values_of(&prefix), [1.0, 2.5]);
        assert_eq!(prefix.values().len(), 2);

        fn never_called(_: f64) -> f64 {
            panic!("an empty referenced span must not map backing samples")
        }
        for input in [backing.slice(1, 1), backing.slice(2, 0)] {
            let out = apply(&input, Some(Bound::Map(never_called))).unwrap();
            assert_eq!(out.len(), input.len());
            assert_eq!(out.offsets().to_vec(), vec![0; input.len() + 1]);
            assert_eq!(out.values().len(), 0);
        }
    }

    #[test]
    fn mapping_filtering_and_empty_results_preserve_nested_sample_metadata() {
        use datafusion::arrow::datatypes::{Field, Fields};

        let backing = samples();
        let entries = backing.values().as_struct();
        let fields: Fields = entries
            .fields()
            .iter()
            .map(|f| {
                Arc::new(
                    f.as_ref()
                        .clone()
                        .with_metadata([("child".into(), f.name().clone())].into_iter().collect()),
                )
            })
            .collect();
        let entries = StructArray::new(fields.clone(), entries.columns().to_vec(), None);
        let item = Arc::new(
            Field::new(series::LIST_ITEM, DataType::Struct(fields), false)
                .with_metadata([("item".into(), "preserved".into())].into_iter().collect()),
        );
        let input =
            ListArray::new(item, backing.offsets().clone(), Arc::new(entries), None).slice(1, 2);
        assert_eq!(
            Elementwise::default()
                .return_type(&[
                    input.data_type().clone(),
                    DataType::Utf8,
                    DataType::Null,
                    DataType::Null,
                ])
                .unwrap(),
            *input.data_type()
        );
        for bound in [
            Func::Abs.bind(None, None),
            Func::Neg.bind(None, None),
            Func::Timestamp.bind(None, None),
            Func::Binary {
                op: crate::binary::Op::Lss,
                return_bool: false,
            }
            .bind(Some(0.0), None),
            None,
        ] {
            let out = apply(&input, bound).unwrap();
            assert_eq!(out.data_type(), input.data_type());
            assert_eq!(out.offsets()[0], 0);
            assert_eq!(out.values().len(), usize::from(bound.is_some()));
        }
    }

    #[test]
    fn sliced_null_and_empty_rows_keep_their_validity() {
        let backing = samples();
        let nullable = ListArray::new(
            series::sample_item(),
            backing.offsets().clone(),
            Arc::clone(backing.values()),
            Some(vec![true, true, false].into()),
        );
        let input = nullable.slice(1, 2);
        for bound in [
            Func::Abs.bind(None, None),
            Func::Timestamp.bind(None, None),
            Func::Binary {
                op: crate::binary::Op::Lss,
                return_bool: false,
            }
            .bind(Some(0.0), None),
            None,
        ] {
            let out = apply(&input, bound).unwrap();
            assert_eq!(out.nulls(), input.nulls());
            assert_eq!(out.value_length(0), 0);
            assert!(out.is_null(1));
            assert_eq!(out.values().len(), usize::from(bound.is_some()));
        }
    }

    #[test]
    fn sliced_mapping_preserves_every_bound_functions_float_semantics() {
        let vs = vec![
            f64::from_bits(crate::selector::STALE_NAN_BITS),
            -0.0,
            0.0,
            f64::INFINITY,
            f64::NEG_INFINITY,
            -2.5,
            2.5,
            f64::NAN,
        ];
        let batch = series::encode(
            &["s".to_string()],
            &[
                series::Series::new(&[("s", "before")], vec![0], vec![99.0]).unwrap(),
                series::Series::new(
                    &[("s", "selected")],
                    (0..vs.len() as i64).collect(),
                    vs.clone(),
                )
                .unwrap(),
                series::Series::new(&[("s", "after")], vec![0], vec![99.0]).unwrap(),
            ],
            series::ONE_BLOCK,
        )
        .unwrap();
        let input = batch
            .column_by_name(series::SAMPLES)
            .unwrap()
            .as_list::<i32>()
            .slice(1, 1);
        for name in [
            "abs",
            "ceil",
            "floor",
            "round",
            "exp",
            "ln",
            "log2",
            "log10",
            "sqrt",
            "sgn",
            "clamp",
            "clamp_min",
            "clamp_max",
            "sin",
            "cos",
            "tan",
            "asin",
            "acos",
            "atan",
            "sinh",
            "cosh",
            "tanh",
            "asinh",
            "acosh",
            "atanh",
            "deg",
            "rad",
            "day_of_month",
            "day_of_week",
            "day_of_year",
            "days_in_month",
            "hour",
            "minute",
            "month",
            "year",
        ] {
            let bound = Func::parse(name)
                .unwrap()
                .bind(Some(0.5), Some(2.0))
                .unwrap();
            let expected: Vec<_> = vs.iter().map(|&v| bound.value(v).to_bits()).collect();
            let out = apply(&input, Some(bound)).unwrap();
            assert_eq!(out.offsets().as_ref(), &[0, vs.len() as i32]);
            assert_eq!(out.values().len(), vs.len());
            assert_eq!(
                values_of(&out)
                    .into_iter()
                    .map(f64::to_bits)
                    .collect::<Vec<_>>(),
                expected,
                "{name}"
            );
        }
        let out = apply(&input, Func::Timestamp.bind(None, None)).unwrap();
        assert_eq!(out.offsets().as_ref(), &[0, vs.len() as i32]);
        assert_eq!(
            values_of(&out),
            (0..vs.len()).map(|t| t as f64 / 1000.0).collect::<Vec<_>>()
        );
    }

    /// The edge values as three rows, the middle one empty, timestamps
    /// counting up from zero across all of them.
    fn edge_samples() -> ListArray {
        use crate::binary::EDGES;
        let (a, c) = EDGES.split_at(5);
        let timestamps = |from: i64, n: usize| (from..from + n as i64).collect::<Vec<_>>();
        let batch = series::encode(
            &["s".to_string()],
            &[
                series::Series::new(&[("s", "a")], timestamps(0, a.len()), a.to_vec()).unwrap(),
                series::Series::new(&[("s", "b")], vec![], vec![]).unwrap(),
                series::Series::new(&[("s", "c")], timestamps(5, c.len()), c.to_vec()).unwrap(),
            ],
            series::ONE_BLOCK,
        )
        .unwrap();
        batch
            .column_by_name(series::SAMPLES)
            .unwrap()
            .as_list::<i32>()
            .clone()
    }

    /// Bits to compare results by, with every NaN one value: its sign and
    /// payload are codegen's pick and PromQL cannot see them. A stale
    /// marker never reaches here, the selector and range kernels drop it.
    /// See `binary::tests::bits`.
    fn bits(v: f64) -> u64 {
        if v.is_nan() {
            f64::NAN.to_bits()
        } else {
            v.to_bits()
        }
    }

    /// Each row's samples, values as [`bits`] so that a zero's sign
    /// counts.
    fn rows_of(list: &ListArray) -> Vec<Vec<(i64, u64)>> {
        (0..list.len())
            .map(|row| {
                let entries = list.value(row);
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
                (0..entries.len())
                    .map(|i| (timestamps.value(i), bits(values.value(i))))
                    .collect()
            })
            .collect()
    }

    /// What upstream's `VectorscalarBinop` makes of each row: `value`
    /// on the pair in written order, except that a filtering comparison
    /// keeps the vector's own sample whichever side it was on.
    fn scalar_binop_by_hand(input: &ListArray, bound: Bound) -> Vec<Vec<(i64, u64)>> {
        let Bound::Binary {
            op,
            return_bool,
            scalar,
            swap,
        } = bound
        else {
            panic!("a binary operator")
        };
        rows_of(input)
            .into_iter()
            .map(|row| {
                row.into_iter()
                    .filter_map(|(t, raw)| {
                        let v = f64::from_bits(raw);
                        let (l, r) = if swap { (scalar, v) } else { (v, scalar) };
                        let value = match op.is_comparison() && !return_bool {
                            true => op.compare(l, r).then_some(v),
                            false => op.value(l, r, return_bool),
                        };
                        value.map(|value| (t, bits(value)))
                    })
                    .collect()
            })
            .collect()
    }

    /// Every operator against every edge scalar on either side, over
    /// every edge value: the kernel path answers bit for bit what the
    /// per-sample reference does, and where nothing can drop it hands
    /// the offsets and timestamps back as they came.
    #[test]
    fn a_scalar_operator_is_vector_elem_binop_bit_for_bit() {
        let input = edge_samples();
        let input_timestamps = input
            .values()
            .as_struct()
            .column_by_name(series::TIMESTAMP)
            .unwrap()
            .clone();
        // A set operator between a vector and a scalar is not a query.
        for op in crate::binary::Op::ALL.into_iter().filter(|op| !op.is_set()) {
            for return_bool in [false, true] {
                if return_bool && !op.is_comparison() {
                    continue;
                }
                let func = Func::Binary { op, return_bool };
                for scalar in crate::binary::EDGES {
                    for bound in [func.bind(Some(scalar), None), func.bind(None, Some(scalar))] {
                        let out = apply(&input, bound).unwrap();
                        let what = format!("{bound:?}");
                        assert_eq!(
                            rows_of(&out),
                            scalar_binop_by_hand(&input, bound.unwrap()),
                            "{what}"
                        );
                        if op.is_comparison() && !return_bool {
                            continue;
                        }
                        assert_eq!(out.offsets(), input.offsets(), "{what}");
                        let timestamps = out
                            .values()
                            .as_struct()
                            .column_by_name(series::TIMESTAMP)
                            .unwrap()
                            .clone();
                        assert!(Arc::ptr_eq(&timestamps, &input_timestamps), "{what}");
                    }
                }
            }
        }
    }

    #[test]
    fn sliced_scalar_operators_preserve_bits_and_only_rebuild_referenced_samples() {
        let backing = edge_samples();
        for input in [
            backing.slice(0, 2),
            backing.slice(1, 2),
            backing.slice(1, 1),
            backing.slice(2, 0),
        ] {
            let covered = input.offsets()[input.len()] - input.offsets()[0];
            for op in crate::binary::Op::ALL.into_iter().filter(|op| !op.is_set()) {
                for return_bool in [false, true] {
                    if return_bool && !op.is_comparison() {
                        continue;
                    }
                    let func = Func::Binary { op, return_bool };
                    for scalar in crate::binary::EDGES {
                        for bound in [func.bind(Some(scalar), None), func.bind(None, Some(scalar))]
                        {
                            let out = apply(&input, bound).unwrap();
                            assert_eq!(
                                rows_of(&out),
                                scalar_binop_by_hand(&input, bound.unwrap()),
                                "{bound:?}"
                            );
                            assert_eq!(out.offsets()[0], 0);
                            let expected_len = if op.is_comparison() && !return_bool {
                                rows_of(&out).concat().len()
                            } else {
                                covered as usize
                            };
                            assert_eq!(out.values().len(), expected_len, "{bound:?}");
                        }
                    }
                }
            }
        }
    }

    /// Go's `==` and `<` on floats, against a scalar on either side:
    /// a NaN equals nothing, itself included, and the two zeros are
    /// equal. Arrow's comparison kernels order by `total_cmp` and would
    /// answer the other way on both.
    #[test]
    fn a_nan_is_equal_to_nothing_and_the_zeros_are_equal() {
        let input = series::encode(
            &["s".to_string()],
            &[series::Series::new(&[("s", "a")], vec![0, 1], vec![f64::NAN, -0.0]).unwrap()],
            series::ONE_BLOCK,
        )
        .unwrap();
        let input = input
            .column_by_name(series::SAMPLES)
            .unwrap()
            .as_list::<i32>()
            .clone();
        let run = |op, return_bool, a, b| {
            let bound = Func::Binary { op, return_bool }.bind(a, b);
            rows_of(&apply(&input, bound).unwrap())
        };
        use crate::binary::Op;
        let (nan, zero) = (f64::NAN, 0.0f64);
        for (a, b) in [(Some(nan), None), (None, Some(nan))] {
            assert_eq!(run(Op::Eql, false, a, b), [vec![]]);
            assert_eq!(
                run(Op::Neq, false, a, b),
                [vec![(0, bits(nan)), (1, bits(-0.0))]]
            );
        }
        for (a, b) in [(Some(zero), None), (None, Some(zero))] {
            assert_eq!(run(Op::Eql, false, a, b), [vec![(1, bits(-0.0))]]);
            assert_eq!(
                run(Op::Eql, true, a, b),
                [vec![(0, bits(0.0)), (1, bits(1.0))]]
            );
            assert_eq!(run(Op::Lss, false, a, b), [vec![]]);
            assert_eq!(run(Op::Gte, false, a, b), [vec![(1, bits(-0.0))]]);
        }
    }

    /// A list sliced out of a longer one has child samples outside its
    /// rows, and a filtering comparison must not pull them in.
    #[test]
    fn a_filtering_comparison_keeps_to_a_sliced_lists_rows() {
        let input = edge_samples().slice(1, 2);
        let bound = Func::Binary {
            op: crate::binary::Op::Gtr,
            return_bool: false,
        }
        .bind(Some(0.0), None);
        let out = apply(&input, bound).unwrap();
        assert_eq!(out.len(), 2);
        assert_eq!(rows_of(&out), scalar_binop_by_hand(&input, bound.unwrap()));
        let kept = rows_of(&out).concat().len();
        assert_eq!(out.values().len(), kept);
    }

    #[test]
    fn a_clamp_that_cannot_be_satisfied_empties_every_row() {
        let out = apply(&samples(), Func::Clamp.bind(Some(1.0), Some(0.0))).unwrap();
        assert_eq!(out.len(), 3);
        assert_eq!(out.offsets().to_vec(), vec![0, 0, 0, 0]);
        assert!(values_of(&out).is_empty());
    }
}
