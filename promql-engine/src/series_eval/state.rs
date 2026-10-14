//! Per-series state and Arrow buffer ownership for contiguous evaluation.
//!
//! Each fold uses the shared buffered iterator for Prometheus's `evalSeries`
//! / `matrixIterSlice` (83962c35). The current series copies schema-ordered label
//! values separately from the sample window; consumed source arrays can die.
//! Owned label capacities are measured at capture. Output builders copy those
//! values again while the current series still lives. Finalized samples coexist
//! with input window capacity until the executor's release checkpoint.
//! Empty/singleton fold results stay inline. The batch owner alone creates Arrow
//! row boundaries, including rows with no evaluated samples.

#[cfg(test)]
use std::sync::Arc;

use datafusion::arrow::array::{AsArray, StructArray};
#[cfg(test)]
use datafusion::arrow::{
    array::{ArrayRef, StringViewBuilder},
    datatypes::Fields,
};
use datafusion::common::{DataFusionError, Result};

use crate::buffer::{BufferedSeriesIterator, EvalContext, PushCursor, WorkBudget};
use crate::series::SamplesBuilder;

#[cfg(test)]
mod tests;

/// One series' window and evaluated samples; labels belong to its caller.
pub(crate) struct SeriesFold {
    // Close retains capacities until the owner has charged output growth.
    iterator: Option<BufferedSeriesIterator>,
    pub(crate) out: SingleSeriesSamples,
}

/// Arrow row boundaries belong to the batch owner, not a numerical fold.
pub(crate) enum SingleSeriesSamples {
    Inline(Option<(i64, f64)>),
    Heap {
        timestamps: Vec<i64>,
        values: Vec<f64>,
    },
}
impl SingleSeriesSamples {
    fn push(&mut self, timestamp: i64, value: f64) {
        match self {
            Self::Inline(None) => *self = Self::Inline(Some((timestamp, value))),
            Self::Inline(Some((first_t, first_v))) => {
                *self = Self::Heap {
                    timestamps: vec![*first_t, timestamp],
                    values: vec![*first_v, value],
                };
            }
            Self::Heap { timestamps, values } => {
                timestamps.push(timestamp);
                values.push(value);
            }
        }
    }

    pub(crate) fn len(&self) -> usize {
        match self {
            Self::Inline(point) => usize::from(point.is_some()),
            Self::Heap { timestamps, .. } => timestamps.len(),
        }
    }

    fn heap_size(&self) -> usize {
        match self {
            Self::Inline(_) => 0,
            Self::Heap { timestamps, values } => {
                timestamps.capacity() * std::mem::size_of::<i64>()
                    + values.capacity() * std::mem::size_of::<f64>()
            }
        }
    }

    pub(crate) fn append_to(&self, output: &mut SamplesBuilder) {
        match self {
            Self::Inline(None) => {}
            Self::Inline(Some((timestamp, value))) => output.push(*timestamp, *value),
            Self::Heap { timestamps, values } => output.append_samples(timestamps, values),
        }
    }
}

pub(crate) fn copy_label_values(labels: &StructArray, row: usize) -> Vec<String> {
    labels
        .columns()
        .iter()
        .map(|column| column.as_string_view().value(row).to_owned())
        .collect()
}

pub(crate) fn label_values_heap_size(values: &Vec<String>) -> Result<usize> {
    checked_label_capacity(values.capacity(), values.iter().map(String::capacity))
}

// Separate capacity arithmetic so overflow can be tested without huge allocations.
fn checked_label_capacity(
    vector_capacity: usize,
    string_capacities: impl IntoIterator<Item = usize>,
) -> Result<usize> {
    let overflow =
        || DataFusionError::ResourcesExhausted("series evaluation label charge overflow".into());
    let slots = vector_capacity
        .checked_mul(std::mem::size_of::<String>())
        .ok_or_else(overflow)?;
    string_capacities
        .into_iter()
        .try_fold(slots, |bytes, capacity| {
            bytes.checked_add(capacity).ok_or_else(overflow)
        })
}

#[cfg(test)]
pub(crate) fn emit_label_rows<'a>(
    fields: Fields,
    rows: impl ExactSizeIterator<Item = &'a Vec<String>>,
) -> Result<ArrayRef> {
    let len = rows.len();
    // An empty schema still needs
    // its real row count; there is no child array from which Arrow can infer it.
    debug_assert_ne!(len, 0);
    if fields.is_empty() {
        return Ok(Arc::new(StructArray::new_empty_fields(len, None)));
    }
    let mut builders: Vec<_> = fields
        .iter()
        .map(|_| StringViewBuilder::with_capacity(len))
        .collect();
    for values in rows {
        for (builder, value) in builders.iter_mut().zip(values) {
            builder.append_value(value);
        }
    }
    let columns = builders
        .iter_mut()
        .map(|builder| Arc::new(builder.finish()) as ArrayRef)
        .collect();
    // Fields, including nested metadata, belong to the shared schema; only
    // values are copied per series and then into builder-owned output strings.
    Ok(Arc::new(StructArray::try_new(fields, columns, None)?))
}

impl SeriesFold {
    pub(crate) fn new(context: &EvalContext) -> Self {
        Self {
            iterator: Some(BufferedSeriesIterator::new(context)),
            out: SingleSeriesSamples::Inline(None),
        }
    }

    pub(crate) fn heap_size(&self) -> usize {
        self.iterator.as_ref().map_or(0, |it| {
            it.size() - std::mem::size_of::<BufferedSeriesIterator>()
        }) + self.out.heap_size()
    }

    #[cfg(test)]
    pub(crate) fn push(&mut self, context: &EvalContext, ts: &[i64], vs: &[f64]) {
        if !ts.is_empty() {
            let output = &mut self.out;
            self.iterator
                .as_mut()
                .expect("open fold")
                .push(context, ts, vs, |t, v| output.push(t, v));
        }
    }

    // Closing is numerically idempotent. Input capacity stays owned until
    // the caller's growth checkpoint.
    #[cfg(test)]
    pub(crate) fn finish_output(&mut self, context: &EvalContext) {
        if let Some(iterator) = self.iterator.as_mut() {
            let output = &mut self.out;
            iterator.close(context, |t, v| output.push(t, v));
        }
    }

    pub(crate) fn push_budgeted(
        &mut self,
        context: &EvalContext,
        ts: &[i64],
        vs: &[f64],
        cursor: &mut PushCursor,
        budget: &mut WorkBudget,
    ) -> bool {
        let output = &mut self.out;
        self.iterator.as_mut().expect("open fold").push_budgeted(
            context,
            ts,
            vs,
            cursor,
            budget,
            |t, v| output.push(t, v),
        )
    }

    pub(crate) fn finish_output_budgeted(
        &mut self,
        context: &EvalContext,
        budget: &mut WorkBudget,
    ) -> bool {
        let output = &mut self.out;
        self.iterator.as_mut().is_none_or(|iterator| {
            iterator.close_budgeted(context, budget, |t, v| output.push(t, v))
        })
    }

    pub(crate) fn release_input(&mut self) {
        self.iterator = None;
    }
}

pub(crate) fn checked_sample_count(counts: impl IntoIterator<Item = usize>) -> Result<usize> {
    counts.into_iter().try_fold(0usize, |total, count| {
        total
            .checked_add(count)
            .filter(|&n| n <= i32::MAX as usize)
            .ok_or_else(|| {
                DataFusionError::Execution(
                    "series evaluation: more than i32::MAX samples in one output column".into(),
                )
            })
    })
}
