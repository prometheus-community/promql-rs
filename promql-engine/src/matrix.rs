//! A range selector answered as an instant query's result:
//! `promql_matrix_selector(samples, mint, maxt)`.
//!
//! Ports `matrixSelector` and `matrixIterSlice` (`promql/engine.go` at
//! 83962c35) for the one step an instant query has. The result is the
//! samples of each series with `mint < t <= maxt` under their own
//! timestamps, StaleNaN left out, and a series with none is not in it.
//!
//! A scalar function rather than an aggregate, unlike the vector selector
//! and the range functions. Those fold a series' chunks in order because
//! each step reads a window that may span chunks; here every sample is
//! judged by its own time against two constants, so a chunk is filtered
//! where it lies and the engine's rows come out as the store's chunks
//! with fewer samples. Joining a label set's chunks into one series is
//! the caller's, as it is for a range query's result.

use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, AsArray, BooleanBufferBuilder, ListArray};
use datafusion::arrow::buffer::BooleanBuffer;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{plan_err, ScalarValue};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{
    lit, ColumnarValue, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl,
    Signature, Volatility,
};

use crate::elementwise::filtered;
use crate::selector::is_stale;
use crate::series;

pub const NAME: &str = "promql_matrix_selector";

/// The window of one instant: `mint` exclusive, `maxt` inclusive, the
/// bounds `matrixSelector` builds from the evaluation time, the offset
/// and the range.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Window {
    pub mint: i64,
    pub maxt: i64,
}

impl Window {
    fn holds(&self, t: i64, v: f64) -> bool {
        // Left-open since Prometheus 3.0, which is what puts the sample
        // at exactly `mint` in the previous window and not this one.
        self.mint < t && t <= self.maxt && !is_stale(v)
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct MatrixSelector {
    signature: Signature,
}

impl Default for MatrixSelector {
    fn default() -> Self {
        Self {
            signature: Signature::exact(
                vec![series::samples_type(), DataType::Int64, DataType::Int64],
                Volatility::Immutable,
            ),
        }
    }
}

pub fn udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(MatrixSelector::default())
}

/// `promql_matrix_selector(samples, mint, maxt)`.
pub fn call(samples: Expr, window: Window) -> Expr {
    udf().call(vec![samples, lit(window.mint), lit(window.maxt)])
}

impl ScalarUDFImpl for MatrixSelector {
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

    /// A null row maps to a null row, as in [`crate::elementwise`].
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(
            NAME,
            series::samples_type(),
            args.arg_fields[0].is_nullable(),
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let samples: ArrayRef = match &args.args[0] {
            ColumnarValue::Array(a) => Arc::clone(a),
            ColumnarValue::Scalar(s) => s.to_array_of_size(args.number_rows)?,
        };
        let bound = |i: usize, what: &str| match &args.args[i] {
            ColumnarValue::Scalar(ScalarValue::Int64(Some(n))) => Ok(*n),
            other => Err(DataFusionError::Execution(format!(
                "{NAME}: {what} must be an Int64 literal, got {:?}",
                other.data_type()
            ))),
        };
        let window = Window {
            mint: bound(1, "mint")?,
            maxt: bound(2, "maxt")?,
        };
        Ok(ColumnarValue::Array(Arc::new(apply(
            samples.as_list::<i32>(),
            window,
        )?)))
    }
}

/// Keep, in every row of `samples`, the samples `window` holds.
pub fn apply(samples: &ListArray, window: Window) -> Result<ListArray> {
    let entries = samples.values().as_struct();
    let (ts, vs) = series::sample_slices(entries);
    let mut holds = BooleanBufferBuilder::new(ts.len());
    for (t, v) in ts.iter().zip(vs) {
        holds.append(window.holds(*t, *v));
    }
    let holds: BooleanBuffer = holds.finish();
    let timestamps = entries
        .column_by_name(series::TIMESTAMP)
        .expect("a canonical samples entry");
    let values = entries
        .column_by_name(series::VALUE)
        .expect("a canonical samples entry")
        .as_primitive();
    filtered(samples, timestamps, values, &holds)
}
