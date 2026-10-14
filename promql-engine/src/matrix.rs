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

use datafusion::arrow::array::{Array, ArrayRef, AsArray, BooleanBufferBuilder, ListArray};
use datafusion::arrow::buffer::BooleanBuffer;
use datafusion::arrow::datatypes::{DataType, FieldRef};
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
            // Exact would coerce away nested source-field metadata before
            // filtering. Validate the numerical shape in `return_type`.
            signature: Signature::any(3, Volatility::Immutable),
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
        if !arg_types.first().is_some_and(series::is_samples_type) {
            return plan_err!(
                "{NAME}: first argument must be {}, got {:?}",
                series::samples_type(),
                arg_types.first()
            );
        }
        for (i, t) in arg_types.iter().enumerate().skip(1) {
            if t != &DataType::Int64 {
                return plan_err!("{NAME}: argument {i} must be Int64, got {t}");
            }
        }
        Ok(arg_types[0].clone())
    }

    /// Filtering does not change the sample fields or their metadata.
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let types: Vec<_> = args
            .arg_fields
            .iter()
            .map(|f| f.data_type().clone())
            .collect();
        self.return_type(&types)?;
        Ok(Arc::new(
            args.arg_fields[0].as_ref().clone().with_name(NAME),
        ))
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
    // A list slice retains the entire backing child. `filtered` counts
    // predicates relative to the first referenced offset, as elementwise
    // comparisons do; passing a backing-wide predicate misassigns rows.
    let offsets = samples.offsets();
    let start = offsets[0] as usize;
    let end = offsets[samples.len()] as usize;
    let entries = samples.values().as_struct().slice(start, end - start);
    let (ts, vs) = series::sample_slices(&entries);
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

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{Float64Array, StructArray, TimestampMillisecondArray};
    use datafusion::arrow::buffer::OffsetBuffer;
    use datafusion::arrow::datatypes::{Field, Fields};

    use super::*;

    fn samples() -> ListArray {
        let stale = f64::from_bits(crate::selector::STALE_NAN_BITS);
        let rows: &[&[(i64, f64)]] = &[
            &[(12, 99.0)], // Unreferenced prefix would pass the window.
            &[
                (10, 1.0),
                (11, stale),
                (12, f64::NAN),
                (13, -0.0),
                (14, 4.0),
                (15, 5.0),
            ],
            &[],
            &[(9, 9.0), (12, 22.0), (14, 24.0), (16, 26.0)],
            &[(12, 99.0)], // So would the unreferenced suffix.
        ];
        let (ts, vs): (Vec<_>, Vec<_>) = rows.iter().flat_map(|row| row.iter().copied()).unzip();
        let fields: Fields = series::sample_fields()
            .iter()
            .map(|f| {
                Arc::new(
                    f.as_ref().clone().with_metadata(
                        [("child".to_string(), f.name().clone())]
                            .into_iter()
                            .collect(),
                    ),
                )
            })
            .collect();
        let entries = StructArray::new(
            fields.clone(),
            vec![
                Arc::new(TimestampMillisecondArray::from(ts)),
                Arc::new(Float64Array::from(vs)),
            ],
            None,
        );
        let item = Arc::new(
            Field::new(series::LIST_ITEM, DataType::Struct(fields), false)
                .with_metadata([("item".into(), "preserved".into())].into_iter().collect()),
        );
        ListArray::new(
            item,
            OffsetBuffer::from_lengths(rows.iter().map(|row| row.len())),
            Arc::new(entries),
            None,
        )
    }

    fn rows(list: &ListArray) -> Vec<Vec<(i64, u64)>> {
        (0..list.len())
            .map(|i| {
                let entries = list.value(i);
                let (ts, vs) = series::sample_slices(entries.as_struct());
                ts.iter().zip(vs).map(|(&t, &v)| (t, v.to_bits())).collect()
            })
            .collect()
    }

    const WINDOW: Window = Window { mint: 10, maxt: 14 };

    /// `matrixIterSlice`'s bounds and stale test must use the same rebased
    /// predicate as filtering comparisons, not offsets into the backing child.
    #[test]
    fn sliced_matrix_filters_only_its_referenced_span() {
        let backing = samples();
        let input = backing.slice(1, 3);
        assert_eq!(input.offsets().as_ref(), &[1, 7, 7, 11]);
        let out = apply(&input, WINDOW).unwrap();
        assert_eq!(out.offsets().as_ref(), &[0, 3, 3, 5]);
        assert_eq!(out.values().len(), 5);
        assert_eq!(
            out.data_type(),
            input.data_type(),
            "nested metadata survives"
        );
        assert_eq!(
            rows(&out),
            [
                vec![
                    (12, f64::NAN.to_bits()),
                    (13, (-0.0f64).to_bits()),
                    (14, 4.0f64.to_bits())
                ],
                vec![],
                vec![(12, 22.0f64.to_bits()), (14, 24.0f64.to_bits())],
            ]
        );

        // A zero first offset does not imply the backing suffix is referenced.
        let prefix = apply(&backing.slice(0, 3), WINDOW).unwrap();
        assert_eq!(prefix.offsets().as_ref(), &[0, 1, 4, 4]);
        assert_eq!(prefix.values().len(), 4);
    }

    #[test]
    fn empty_referenced_spans_and_empty_windows_do_not_pull_in_backing_samples() {
        let backing = samples();
        for input in [backing.slice(2, 1), backing.slice(2, 0)] {
            assert_eq!(input.offsets()[0], 7);
            let out = apply(&input, WINDOW).unwrap();
            assert_eq!(out.len(), input.len());
            assert_eq!(out.offsets().to_vec(), vec![0; input.len() + 1]);
            assert_eq!(out.values().len(), 0);
            assert_eq!(out.data_type(), input.data_type());
        }
        let input = backing.slice(1, 3);
        let out = apply(&input, Window { mint: 14, maxt: 14 }).unwrap();
        assert_eq!(out.offsets().as_ref(), &[0, 0, 0, 0]);
        assert_eq!(out.values().len(), 0);
        assert_eq!(out.data_type(), input.data_type());
    }

    #[test]
    fn matrix_return_field_keeps_source_metadata_and_nullability() {
        let input = Arc::new(
            Field::new(series::SAMPLES, samples().data_type().clone(), true)
                .with_metadata([("unit".into(), "samples".into())].into_iter().collect()),
        );
        let args = [
            input.clone(),
            Arc::new(Field::new("mint", DataType::Int64, false)),
            Arc::new(Field::new("maxt", DataType::Int64, false)),
        ];
        let out = MatrixSelector::default()
            .return_field_from_args(ReturnFieldArgs {
                arg_fields: &args,
                scalar_arguments: &[None, None, None],
            })
            .unwrap();
        assert_eq!(out.as_ref(), &input.as_ref().clone().with_name(NAME));
    }
}
