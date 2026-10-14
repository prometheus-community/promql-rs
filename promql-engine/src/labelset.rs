//! `cleanupMetricLabels` on the query's result: two series that ended up
//! with one label set.
//!
//! Prometheus runs with delayed `__name__` removal (`promql/engine.go`,
//! `evaluator.cleanupMetricLabels`, and `EnableDelayedNameRemoval: true`
//! in `promql/promqltest`), so dropping the name never errors where it
//! happens. The name stays on the series for the whole evaluation and is
//! removed once, on the way out; only then does upstream look for a label
//! set twice. Every check inside the evaluation — above a call, above a
//! binary operator, in `rangeEval` — is behind `!enableDelayedNameRemoval`
//! and dead. Checking earlier would answer with an error where upstream
//! answers with a number: `sum(rate({env="1"}[10m])) by (env)` adds two
//! rows that lost their names into one series and is not a duplicate at
//! all.
//!
//! Upstream errors only when two such series share a timestamp, and
//! merges them into one series when they do not. Two metrics alike in
//! every label but the name, sampled over windows that do not overlap,
//! are exactly that under a name-dropping call. Finalization orders row
//! indices and checks each label run once. Disjoint sample bounds need
//! no timestamp walk; connected overlaps use borrowed heads, not copies
//! of all timestamps. Arrow output retains separate buffer-sharing rows;
//! matrix consumers must merge interleaved spans within a label run.

use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;
use std::sync::Arc;

use datafusion::arrow::array::{AsArray, RecordBatch, StringViewArray};
use datafusion::arrow::datatypes::TimestampMillisecondType;

use crate::error::EngineError;
use crate::series::{self, BLOCK_START, LABELS, SAMPLES, TIMESTAMP};

/// Prometheus's own sentence, word for word: the conformance suite
/// compares error text, not error kinds.
pub const SAME_LABELSET: &str = "vector cannot contain metrics with the same labelset";

/// The collector's single result organization pass, after Prometheus's
/// `cleanupMetricLabels` and `sort.Sort(mat)` (`promql/engine.go` at 83962c35).
/// Validation precedes downcasts; empty rows cannot collide. An explicit
/// instant sort still uses label-ordered indices for checking, but its output
/// order must not be replaced by matrix label ordering.
pub(crate) fn finalize(
    batches: &[RecordBatch],
    ordered: bool,
) -> Result<Vec<RecordBatch>, EngineError> {
    let mut validated = None;
    let batches = batches
        .iter()
        .map(|batch| {
            let schema = batch.schema();
            if !validated.as_ref().is_some_and(|s| Arc::ptr_eq(s, &schema)) {
                series::validate(&schema)?;
            }
            validated = Some(schema);
            Ok(series::drop_empty(batch))
        })
        .collect::<Result<Vec<_>, EngineError>>()?;
    let rows = OrderedRows::new(&batches)?;
    rows.reject_same_labelset()?;
    if ordered {
        drop(rows);
        Ok(batches)
    } else {
        Ok(rows.into_batches(&batches))
    }
}

/// `labels.Compare`, then block start, with stable ties. Struct ordering
/// with empty values is not Prometheus's sparse-label ordering. One row
/// index ordering serves both duplicate checking and zero-copy output.
struct OrderedRows<'a> {
    columns: Vec<Vec<&'a StringViewArray>>,
    timestamps: Vec<&'a [i64]>,
    offsets: Vec<&'a [i32]>,
    order: Vec<(usize, usize)>,
    already_sorted: bool,
}

impl<'a> OrderedRows<'a> {
    /// Input schemas have already been validated by the collector.
    fn new(batches: &'a [RecordBatch]) -> Result<Self, EngineError> {
        let mut columns = Vec::with_capacity(batches.len());
        let mut starts = Vec::with_capacity(batches.len());
        let mut timestamps = Vec::with_capacity(batches.len());
        let mut offsets = Vec::with_capacity(batches.len());
        for batch in batches {
            // `compare` pairs fields by position across batches.
            if batch.schema_ref() != batches[0].schema_ref() {
                return Err(EngineError::Schema(
                    "result batches differ in schema".into(),
                ));
            }
            let labels = batch.column_by_name(LABELS).expect("validated").as_struct();
            columns.push(
                labels
                    .columns()
                    .iter()
                    .map(|c| c.as_string_view())
                    .collect(),
            );
            starts.push(
                batch
                    .column_by_name(BLOCK_START)
                    .expect("validated")
                    .as_primitive::<TimestampMillisecondType>(),
            );
            let samples = batch
                .column_by_name(SAMPLES)
                .expect("validated")
                .as_list::<i32>();
            offsets.push(samples.value_offsets());
            timestamps.push(
                samples
                    .values()
                    .as_struct()
                    .column_by_name(TIMESTAMP)
                    .expect("validated")
                    .as_primitive::<TimestampMillisecondType>()
                    .values()
                    .as_ref(),
            );
        }
        let mut order: Vec<(usize, usize)> = batches
            .iter()
            .enumerate()
            .flat_map(|(b, batch)| (0..batch.num_rows()).map(move |row| (b, row)))
            .collect();
        let cmp = |x: &(usize, usize), y: &(usize, usize)| {
            compare(&columns, *x, *y)
                .then_with(|| starts[x.0].value(x.1).cmp(&starts[y.0].value(y.1)))
        };
        let already_sorted = order.is_sorted_by(|x, y| cmp(x, y) != Ordering::Greater);
        if !already_sorted {
            order.sort_by(cmp);
        }
        Ok(Self {
            columns,
            timestamps,
            offsets,
            order,
            already_sorted,
        })
    }

    fn timestamps_at(&self, (batch, row): (usize, usize)) -> &'a [i64] {
        let offsets = self.offsets[batch];
        &self.timestamps[batch][offsets[row] as usize..offsets[row + 1] as usize]
    }

    fn reject_same_labelset(&self) -> Result<(), EngineError> {
        let mut from = 0;
        while from < self.order.len() {
            let mut to = from + 1;
            while to < self.order.len()
                && compare(&self.columns, self.order[from], self.order[to]) == Ordering::Equal
            {
                to += 1;
            }
            if to - from > 1 {
                let mut last = None;
                let disjoint = self.order[from..to]
                    .iter()
                    .map(|&at| self.timestamps_at(at))
                    .filter(|s| !s.is_empty())
                    .all(|span| {
                        let separated = last.is_none_or(|last| last < span[0]);
                        last = span.last().copied();
                        separated
                    });
                // Ordinary block-local results need only first/last
                // timestamp bounds, not a copy or a sample walk.
                if !disjoint {
                    let mut spans: Vec<&[i64]> = self.order[from..to]
                        .iter()
                        .map(|&at| self.timestamps_at(at))
                        .filter(|s| !s.is_empty())
                        .collect();
                    if !spans.is_sorted_by_key(|s| s[0]) {
                        spans.sort_by_key(|s| s[0]);
                    }
                    let mut start = 0;
                    while start < spans.len() {
                        let mut end = start + 1;
                        let mut last = *spans[start].last().expect("nonempty");
                        while end < spans.len() && spans[end][0] <= last {
                            last = last.max(*spans[end].last().expect("nonempty"));
                            end += 1;
                        }
                        if end - start > 1 {
                            reject_overlapping_timestamps(&spans[start..end])?;
                        }
                        start = end;
                    }
                }
            }
            from = to;
        }
        Ok(())
    }

    fn into_batches(self, batches: &[RecordBatch]) -> Vec<RecordBatch> {
        if self.already_sorted {
            return batches.to_vec();
        }
        // Reordering nested samples would copy almost the entire answer.
        // Adjacent row runs instead retain their input's Arrow buffers.
        let mut runs = Vec::new();
        let mut run: Option<(usize, usize, usize)> = None;
        for (b, row) in self.order {
            run = match run {
                Some((rb, start, len)) if rb == b && start + len == row => {
                    Some((rb, start, len + 1))
                }
                Some((rb, start, len)) => {
                    runs.push(batches[rb].slice(start, len));
                    Some((b, row, 1))
                }
                None => Some((b, row, 1)),
            };
        }
        runs.extend(run.map(|(b, start, len)| batches[b].slice(start, len)));
        runs
    }
}

/// Only connected overlapping bounds reach this walk. Evaluated timestamps
/// ascend within each row, so one borrowed head per span detects exact
/// duplicates without an owned timestamp vector for the whole label set.
fn reject_overlapping_timestamps(spans: &[&[i64]]) -> Result<(), EngineError> {
    let mut heads: BinaryHeap<_> = spans
        .iter()
        .enumerate()
        .map(|(row, span)| Reverse((span[0], row, 0usize)))
        .collect();
    let mut previous = None;
    while let Some(Reverse((time, row, point))) = heads.pop() {
        if previous == Some(time) {
            return Err(EngineError::Query(SAME_LABELSET.into()));
        }
        previous = Some(time);
        if let Some(&time) = spans[row].get(point + 1) {
            heads.push(Reverse((time, row, point + 1)));
        }
    }
    Ok(())
}

/// `labels.Compare` of two rows, read off the columns without collecting
/// either label set. Where the rows first differ and one has the label and
/// the other not, the one without it continues with a later name, or ends.
fn compare(columns: &[Vec<&StringViewArray>], x: (usize, usize), y: (usize, usize)) -> Ordering {
    let (a, b) = (&columns[x.0], &columns[y.0]);
    let has_more = |c: &[&StringViewArray], row: usize, from: usize| {
        c[from..].iter().any(|v| !v.value(row).is_empty())
    };
    for f in 0..a.len() {
        let (va, vb) = (a[f].value(x.1), b[f].value(y.1));
        if va == vb {
            continue;
        }
        return match (va.is_empty(), vb.is_empty()) {
            (false, true) if has_more(b, y.1, f + 1) => Ordering::Less,
            (false, true) => Ordering::Greater,
            (true, false) if has_more(a, x.1, f + 1) => Ordering::Greater,
            (true, false) => Ordering::Less,
            _ => va.cmp(vb),
        };
    }
    Ordering::Equal
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::series::{label_names_of, Series, ONE_BLOCK};

    fn encode(names: &[String], series: &[Series]) -> RecordBatch {
        crate::series::encode(names, series, ONE_BLOCK).unwrap()
    }

    fn row(pod: &str, name: &str, timestamps: Vec<i64>) -> Series {
        let values = vec![1.0; timestamps.len()];
        Series::new(&[("__name__", name), ("pod", pod)], timestamps, values).unwrap()
    }

    /// Encode `batches`, one batch per inner vec, and check them.
    ///
    /// Rows are encoded one at a time and concatenated: [`encode`]
    /// refuses a duplicate label set, which is the very input under test
    /// here — a plan that emits one is what this catches.
    fn check(batches: Vec<Vec<Series>>) -> Result<(), EngineError> {
        let names = label_names_of(&batches.concat());
        let one = |row| encode(&names, std::slice::from_ref(row));
        let schema = one(&batches[0][0]).schema();
        let batches: Vec<RecordBatch> = batches
            .iter()
            .map(|rows| {
                datafusion::arrow::compute::concat_batches(
                    &schema,
                    rows.iter().map(one).collect::<Vec<_>>().iter(),
                )
                .unwrap()
            })
            .collect();
        finalize(&batches, false).map(|_| ())
    }

    #[test]
    fn a_label_set_repeated_across_batches_is_an_error() {
        let err = check(vec![
            vec![row("envoy-1", "a", vec![0]), row("envoy-2", "a", vec![0])],
            vec![row("envoy-3", "a", vec![0]), row("envoy-1", "a", vec![0])],
        ])
        .unwrap_err();
        assert!(err.to_string().contains(SAME_LABELSET), "{err}");
    }

    #[test]
    fn a_label_set_repeated_within_one_batch_is_an_error() {
        let err = check(vec![vec![
            row("envoy-1", "a", vec![0]),
            row("envoy-1", "a", vec![0]),
        ]])
        .unwrap_err();
        assert!(err.to_string().contains(SAME_LABELSET), "{err}");
    }

    #[test]
    fn distinct_label_sets_are_fine() {
        check(vec![
            vec![row("envoy-1", "a", vec![0]), row("envoy-2", "a", vec![0])],
            vec![row("envoy-3", "a", vec![0])],
        ])
        .unwrap();
    }

    /// Upstream merges these rather than erroring; the error is what is
    /// mirrored, so at least it must not fire.
    #[test]
    fn one_label_set_on_rows_that_share_no_timestamp_is_not_an_error() {
        check(vec![vec![
            row("envoy-1", "a", vec![0, 30_000]),
            row("envoy-1", "a", vec![60_000]),
        ]])
        .unwrap();
    }

    #[test]
    fn interleaved_same_block_rows_are_checked_by_exact_timestamps() {
        check(vec![vec![
            row("p", "x", vec![0, 4]),
            row("p", "x", vec![1, 3]),
            row("p", "x", vec![2, 6]),
        ]])
        .unwrap();

        // Interior duplicate in a connected overlap, not equal endpoints
        // or adjacent span starts.
        let error = check(vec![vec![
            row("p", "x", vec![0, 10]),
            row("p", "x", vec![2, 8]),
            row("p", "x", vec![5, 8, 12]),
        ]])
        .unwrap_err();
        assert_eq!(error.to_string(), SAME_LABELSET);
    }

    #[test]
    fn finalization_orders_sliced_blocks_without_copying_samples() {
        let source_rows = vec![row("ignored", "x", vec![99]), row("p", "x", vec![0, 2])];
        let names = label_names_of(&source_rows);
        let early = encode(&names, &source_rows).slice(1, 1);
        let late = series::with_block(
            &encode(&names, &[row("p", "x", vec![4, 6])]),
            series::Block {
                start_ms: 4,
                end_ms: 8,
            },
        )
        .unwrap();
        let early = series::with_block(
            &early,
            series::Block {
                start_ms: 0,
                end_ms: 4,
            },
        )
        .unwrap();
        let batches = [late, early];
        let buffers: Vec<_> = batches.iter().map(values_buffer).collect();
        let finalized = finalize(&batches, false).unwrap();
        let decoded = series::decode(&finalized).unwrap();
        assert_eq!(
            decoded.iter().map(|s| s.timestamps()).collect::<Vec<_>>(),
            [&[0, 2][..], &[4, 6][..]]
        );
        for batch in &finalized {
            assert!(
                buffers.contains(&values_buffer(batch)),
                "samples were copied"
            );
        }

        let boundary = series::with_block(
            &encode(&names, &[row("p", "x", vec![2, 6])]),
            series::Block {
                start_ms: 4,
                end_ms: 8,
            },
        )
        .unwrap();
        // Actual sample bounds, not declared blocks, decide duplicates.
        let error = finalize(&[batches[1].clone(), boundary], false).unwrap_err();
        assert_eq!(error.to_string(), SAME_LABELSET);
    }

    #[test]
    fn finalization_validates_schemas_and_drops_empty_colliding_rows() {
        let names = vec!["__name__".to_owned(), "pod".to_owned()];
        let populated = encode(&names, &[row("p", "x", vec![0])]);
        let empty = encode(&names, &[row("p", "x", vec![])]);
        for ordered in [false, true] {
            let result = finalize(&[empty.clone(), populated.clone()], ordered).unwrap();
            assert_eq!(series::decode(&result).unwrap().len(), 1);
            assert!(finalize(&[], ordered).unwrap().is_empty());

            let missing_column = populated.project(&[0, 1, 2]).unwrap();
            assert!(matches!(
                finalize(&[missing_column], ordered),
                Err(EngineError::Schema(_))
            ));
            let different_labels = encode(
                &["pod".to_owned()],
                &[Series::new(&[("pod", "p")], vec![1], vec![1.0]).unwrap()],
            );
            assert!(matches!(
                finalize(&[populated.clone(), different_labels], ordered),
                Err(EngineError::Schema(_))
            ));
        }
    }

    #[test]
    fn explicit_order_is_preserved_while_nonadjacent_labels_are_checked() {
        let names = vec!["__name__".to_owned(), "pod".to_owned()];
        let batches = [
            encode(&names, &[row("z", "x", vec![0])]),
            encode(&names, &[row("a", "x", vec![0])]),
            encode(&names, &[row("z", "x", vec![2])]),
        ];
        let finalized = finalize(&batches, true).unwrap();
        let decoded = series::decode(&finalized).unwrap();
        assert_eq!(
            decoded.iter().map(|s| s.label("pod")).collect::<Vec<_>>(),
            ["z", "a", "z"]
        );
        for (input, output) in batches.iter().zip(&finalized) {
            assert!(Arc::ptr_eq(input.column(1), output.column(1)));
        }
        let collision = encode(&names, &[row("z", "x", vec![0, 2])]);
        let error =
            finalize(&[batches[0].clone(), batches[1].clone(), collision], true).unwrap_err();
        assert_eq!(error.to_string(), SAME_LABELSET);
    }

    #[test]
    fn ordinary_disjoint_blocks_borrow_timestamp_spans() {
        let names = vec!["__name__".to_owned(), "pod".to_owned()];
        let batches: Vec<_> = (0..4)
            .map(|block| {
                let start = block * 10_000;
                series::with_block(
                    &encode(&names, &[row("p", "x", (start..start + 10_000).collect())]),
                    series::Block {
                        start_ms: start,
                        end_ms: start + 10_000,
                    },
                )
                .unwrap()
            })
            .collect();
        let rows = OrderedRows::new(&batches).unwrap();
        assert_eq!(rows.order.len(), 4);
        for (batch, input) in batches.iter().enumerate() {
            let samples = input.column_by_name(SAMPLES).unwrap().as_list::<i32>();
            let timestamps = samples
                .values()
                .as_struct()
                .column_by_name(TIMESTAMP)
                .unwrap()
                .as_primitive::<TimestampMillisecondType>();
            assert_eq!(
                rows.timestamps_at((batch, 0)).as_ptr(),
                timestamps.values().as_ptr()
            );
        }
        rows.reject_same_labelset().unwrap();
    }

    /// Every ordering of label sets that differ in which names they carry,
    /// split over two batches, against `labels.Compare` written the obvious
    /// way: pair lists compared lexicographically.
    #[test]
    fn sort_by_labelset_is_labels_compare() {
        let sets: Vec<Vec<(&str, &str)>> = vec![
            vec![("a", "1")],
            vec![("a", "1"), ("b", "1")],
            vec![("a", "1"), ("c", "1")],
            vec![("a", "2")],
            vec![("b", "1")],
            vec![("b", "2"), ("c", "1")],
            vec![("c", "1")],
            vec![],
        ];
        let series: Vec<Series> = sets
            .iter()
            .rev()
            .map(|l| Series::new(l, vec![0], vec![1.0]).unwrap())
            .collect();
        let names = label_names_of(&series);
        let (front, back) = series.split_at(4);
        let batches = [encode(&names, front), encode(&names, back)];
        let sorted = crate::series::decode(&finalize(&batches, false).unwrap()).unwrap();
        let got: Vec<Vec<(&str, &str)>> = sorted.iter().map(|s| s.labels().collect()).collect();
        let mut want = sets.clone();
        want.sort();
        assert_eq!(got, want);
    }

    /// The value buffer behind a batch's samples: shared with the input
    /// when the output is a slice of it, fresh when the rows were copied.
    fn values_buffer(batch: &RecordBatch) -> *const u8 {
        batch
            .column_by_name(SAMPLES)
            .unwrap()
            .as_list::<i32>()
            .values()
            .as_struct()
            .column_by_name(crate::series::VALUE)
            .unwrap()
            .to_data()
            .buffers()[0]
            .as_ptr()
    }

    /// Two partitions whose rows alternate in label-set order: the output
    /// is one slice per row, and every slice still points at its input's
    /// samples.
    #[test]
    fn sort_by_labelset_slices_without_copying_samples() {
        let series: Vec<Series> = (0..6)
            .map(|i| row(&format!("p{i}"), "x", vec![0, 1, 2]))
            .collect();
        let names = label_names_of(&series);
        let even: Vec<Series> = series.iter().step_by(2).cloned().collect();
        let odd: Vec<Series> = series.iter().skip(1).step_by(2).cloned().collect();
        let batches = [encode(&names, &odd), encode(&names, &even)];
        let inputs: Vec<*const u8> = batches.iter().map(values_buffer).collect();

        let sorted = finalize(&batches, false).unwrap();
        assert_eq!(sorted.len(), 6);
        for batch in &sorted {
            assert!(
                inputs.contains(&values_buffer(batch)),
                "samples were copied"
            );
        }
        let pods: Vec<String> = crate::series::decode(&sorted)
            .unwrap()
            .iter()
            .map(|s| s.label("pod").to_string())
            .collect();
        assert_eq!(pods, ["p0", "p1", "p2", "p3", "p4", "p5"]);
    }
}
