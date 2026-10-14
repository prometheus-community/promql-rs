//! Ordering the result: `sort`, `sort_desc`, `sort_by_label` and
//! `sort_by_label_desc`.
//!
//! These four are the only PromQL functions whose whole effect is the
//! order of the rows, so they are a DataFusion `Sort` over the series
//! rows rather than a kernel over the samples. What they need is sort
//! *keys* — ordinary columns Arrow can compare bytewise — and the two
//! comparators Prometheus uses are not Arrow's:
//!
//! * `funcSort`/`funcSortDesc` put NaN **last** in both directions:
//!   `vectorByValueHeap` and `vectorByReverseValueHeap` both answer
//!   "NaN is smallest", and both are handed to `sort.Reverse`.
//! * `funcSortByLabel` compares label values with `natsort.Compare`, a
//!   natural order in which `cpu="100"` follows `cpu="20"`, and breaks a
//!   tie with `labels.Compare` over the full label set.
//!
//! The value needs no key function: `isnan` as a leading key restores
//! NaN-last where Arrow's total order puts NaN above every number. The
//! labels do: [`Natural`] and [`Set`] turn a label value and a whole
//! label set into byte strings whose bytewise order *is* the Go
//! comparator's. Mirrors Prometheus at 83962c35
//! (`promql/functions.go`); floats only, so the `filterFloats` that drops
//! native histograms before sorting has nothing to do here.

use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, BinaryBuilder, StructArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::plan_err;
use datafusion::error::{DataFusionError, Result};
use datafusion::functions::core::expr_fn::get_field;
use datafusion::functions::math::expr_fn::isnan;
use datafusion::functions_nested::expr_fn::array_element;
use datafusion::logical_expr::{
    col, lit, ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, SortExpr,
    Volatility,
};

use crate::series::{self, LABELS, SAMPLES, VALUE};

/// Which of the four this is. Named for the `promql/functions.go`
/// functions they mirror.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Func {
    Sort,
    SortDesc,
    SortByLabel,
    SortByLabelDesc,
}

impl Func {
    pub fn parse(s: &str) -> Option<Func> {
        Some(match s {
            "sort" => Func::Sort,
            "sort_desc" => Func::SortDesc,
            "sort_by_label" => Func::SortByLabel,
            "sort_by_label_desc" => Func::SortByLabelDesc,
            _ => return None,
        })
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Func::Sort => "sort",
            Func::SortDesc => "sort_desc",
            Func::SortByLabel => "sort_by_label",
            Func::SortByLabelDesc => "sort_by_label_desc",
        }
    }

    /// Whether the keys ascend. The two `_desc` variants are their
    /// sibling's comparator negated, tiebreak included.
    fn ascending(&self) -> bool {
        matches!(self, Func::Sort | Func::SortByLabel)
    }

    /// Whether the label arguments are this function's, rather than the
    /// sample value.
    pub fn by_label(&self) -> bool {
        matches!(self, Func::SortByLabel | Func::SortByLabelDesc)
    }
}

/// The sort keys for one call, over an input carrying `label_names`.
///
/// `labels` are the label arguments of `sort_by_label*`, empty otherwise.
/// A label the input does not carry is `""`, as everywhere in PromQL, so
/// it becomes a literal rather than a read of a field that is not there.
/// `""` then sorts first, and it sorts there every time: `natsort.Compare`
/// answers false for the empty string in *both* directions, so upstream's
/// comparator is not a strict weak ordering there and the order it lands
/// on falls out of the input's. Pinning one is a refinement of behaviour
/// Prometheus leaves undefined, not a disagreement with a defined one.
pub fn exprs(func: Func, labels: &[String], label_names: &[String]) -> Vec<SortExpr> {
    let asc = func.ascending();
    if !func.by_label() {
        // An instant query narrows a row to at most one sample, so the
        // last is the step's. A row with none is null, which sorts after
        // every NaN, and `drop_empty` removes it before anyone sees it.
        let value = get_field(array_element(col(SAMPLES), lit(-1_i64)), VALUE);
        return vec![
            // NaN last whichever way the values go: `funcSort` and
            // `funcSortDesc` both reverse a heap that sorts NaN first.
            SortExpr::new(isnan(value.clone()), true, false),
            SortExpr::new(value, asc, false),
        ];
    }
    labels
        .iter()
        .map(|l| {
            let value = if label_names.contains(l) {
                get_field(col(LABELS), l.as_str())
            } else {
                lit("")
            };
            SortExpr::new(Natural::udf().call(vec![value]), asc, false)
        })
        // "If all labels provided as arguments were equal, sort by the
        // full label set. This ensures a consistent ordering."
        .chain(std::iter::once(SortExpr::new(
            Set::udf().call(vec![col(LABELS)]),
            asc,
            false,
        )))
        .collect()
}

/// `promql_natural_key(value)`: one label value as a byte string ordered
/// the way `natsort.Compare` orders the value itself.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct Natural {
    signature: Signature,
}

pub const NATURAL_NAME: &str = "promql_natural_key";

impl Default for Natural {
    fn default() -> Self {
        Self {
            signature: Signature::uniform(
                1,
                vec![series::label_type(), DataType::Utf8],
                Volatility::Immutable,
            ),
        }
    }
}

impl Natural {
    pub fn udf() -> ScalarUDF {
        ScalarUDF::new_from_impl(Natural::default())
    }
}

impl ScalarUDFImpl for Natural {
    fn name(&self) -> &str {
        NATURAL_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Binary)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let values = match &args.args[0] {
            ColumnarValue::Array(a) => Arc::clone(a),
            ColumnarValue::Scalar(s) => s.to_array_of_size(args.number_rows)?,
        };
        let mut out = BinaryBuilder::with_capacity(values.len(), values.len() * 8);
        let mut key = Vec::new();
        for row in 0..values.len() {
            key.clear();
            natural_key(read_string(&values, row)?, &mut key);
            out.append_value(&key);
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

/// `promql_labels_key(labels)`: a whole label set as a byte string
/// ordered the way `labels.Compare` orders the set.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct Set {
    signature: Signature,
}

pub const SET_NAME: &str = "promql_labels_key";

impl Default for Set {
    fn default() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl Set {
    pub fn udf() -> ScalarUDF {
        ScalarUDF::new_from_impl(Set::default())
    }
}

impl ScalarUDFImpl for Set {
    fn name(&self) -> &str {
        SET_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match &arg_types[0] {
            DataType::Struct(_) => Ok(DataType::Binary),
            other => plan_err!("{SET_NAME}: expects a labels struct, got {other}"),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let labels = match &args.args[0] {
            ColumnarValue::Array(a) => Arc::clone(a),
            ColumnarValue::Scalar(s) => s.to_array_of_size(args.number_rows)?,
        };
        let labels: &StructArray = labels.as_struct();
        let mut out = BinaryBuilder::with_capacity(labels.len(), labels.len() * 32);
        let mut key = Vec::new();
        for row in 0..labels.len() {
            key.clear();
            // Fields are sorted by name and `""` means the series does
            // not carry the label, which is exactly `labels.Labels`.
            for (field, column) in labels.fields().iter().zip(labels.columns()) {
                let value = read_string(column, row)?;
                if value.is_empty() {
                    continue;
                }
                escape(field.name(), &mut key);
                key.extend_from_slice(&SEPARATOR);
                escape(value, &mut key);
                key.extend_from_slice(&SEPARATOR);
            }
            out.append_value(&key);
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

/// One string out of a `Utf8View` or `Utf8` column, NULL as `""`.
fn read_string(values: &ArrayRef, row: usize) -> Result<&str> {
    match values.data_type() {
        DataType::Utf8View => {
            let views = values.as_string_view();
            Ok(if views.is_null(row) {
                ""
            } else {
                views.value(row)
            })
        }
        DataType::Utf8 => {
            let strings = values.as_string::<i32>();
            Ok(if strings.is_null(row) {
                ""
            } else {
                strings.value(row)
            })
        }
        other => Err(DataFusionError::Execution(format!(
            "expected a string column, got {other}"
        ))),
    }
}

/// Opens an encoded digit run, and the byte a literal `0x00` is escaped
/// with so that nothing else can.
const DIGITS: u8 = 0x00;
const ESCAPE: u8 = 0xFF;
/// Ends a name or a value in a label-set key. Lower than every encoded
/// content byte, so a shorter label set sorts before one that extends it.
const SEPARATOR: [u8; 2] = [0x00, 0x00];

/// `s` with every `0x00` doubled out of the way, so a `0x00` in the key
/// can only be one this module put there.
fn escape(s: &str, out: &mut Vec<u8>) {
    for &b in s.as_bytes() {
        out.push(b);
        if b == DIGITS {
            out.push(ESCAPE);
        }
    }
}

/// `s` as a byte string whose bytewise order is `natsort.Compare`'s.
///
/// `natsort` cuts a string into maximal runs of digits and non-digits and
/// compares run by run: two digit runs as integers, anything else
/// bytewise. Runs can only meet a run of the other kind when the two
/// strings differ in kind at their very first byte — the runs alternate
/// in lockstep otherwise — and then the comparison is decided by that
/// first byte alone. Hence the leading class byte: `b'0'` stands for
/// "starts with a digit", and every possible non-digit first byte orders
/// against `b'0'` exactly as it orders against any digit.
///
/// A digit run becomes `DIGITS`, its length as four big-endian bytes, and
/// its digits with leading zeros stripped: without leading zeros, integer
/// order is length then lexicographic order. Go's `strconv.Atoi` gives up
/// past `int64` and `natsort` falls back to bytewise there; this keeps
/// counting, which is the order a reader of a 20-digit label value would
/// expect anyway.
fn natural_key(s: &str, out: &mut Vec<u8>) {
    let bytes = s.as_bytes();
    let Some(&first) = bytes.first() else {
        return;
    };
    out.push(if first.is_ascii_digit() { b'0' } else { first });

    let mut i = 0;
    while i < bytes.len() {
        let start = i;
        let digits = bytes[i].is_ascii_digit();
        while i < bytes.len() && bytes[i].is_ascii_digit() == digits {
            i += 1;
        }
        let run = &bytes[start..i];
        if !digits {
            escape(&s[start..i], out);
            continue;
        }
        let significant = &run[run.iter().take_while(|d| **d == b'0').count()..];
        out.push(DIGITS);
        out.extend_from_slice(&(significant.len() as u32).to_be_bytes());
        out.extend_from_slice(significant);
    }
}

#[cfg(test)]
mod tests {
    use std::cmp::Ordering;

    use super::*;
    use crate::RangeQuery;

    const INSTANT: RangeQuery = RangeQuery {
        start_ms: 0,
        end_ms: 0,
        step_ms: 30_000,
        lookback_ms: 300_000,
    };

    fn key(s: &str) -> Vec<u8> {
        let mut out = Vec::new();
        natural_key(s, &mut out);
        out
    }

    fn cmp(a: &str, b: &str) -> Ordering {
        key(a).cmp(&key(b))
    }

    /// The order `sort_by_label(cpu_time_total, "cpu")` and
    /// `sort_by_label(node_uname_info, …)` assert in `functions.test`.
    #[test]
    fn digit_runs_compare_as_numbers() {
        let mut cpus = ["100", "0", "21", "1", "10", "2", "20", "3", "11", "12"];
        cpus.sort_by(|a, b| cmp(a, b));
        assert_eq!(
            cpus,
            ["0", "1", "2", "3", "10", "11", "12", "20", "21", "100"]
        );

        let mut instances = ["4m600", "4m5", "4m1000"];
        instances.sort_by(|a, b| cmp(a, b));
        assert_eq!(instances, ["4m5", "4m600", "4m1000"]);

        let mut releases = ["1.11.3", "1.2.3", "1.111.3"];
        releases.sort_by(|a, b| cmp(a, b));
        assert_eq!(releases, ["1.2.3", "1.11.3", "1.111.3"]);
    }

    #[test]
    fn a_digit_run_and_a_letter_run_compare_by_their_first_byte() {
        assert_eq!(cmp("12", "a"), Ordering::Less);
        assert_eq!(cmp("12", "!"), Ordering::Greater);
        assert_eq!(cmp("9", "a"), Ordering::Less);
        // Among themselves the digit runs are numbers again, not bytes.
        assert_eq!(cmp("9", "12"), Ordering::Less);
    }

    #[test]
    fn a_prefix_sorts_first_whatever_follows_it() {
        assert_eq!(cmp("ab", "abc"), Ordering::Less);
        assert_eq!(cmp("ab", "ab1"), Ordering::Less);
        assert_eq!(cmp("ab1", "ab1cd"), Ordering::Less);
        assert_eq!(cmp("", "a"), Ordering::Less);
        assert_eq!(cmp("", ""), Ordering::Equal);
        // A literal NUL cannot be read as the opening of a digit run.
        assert_eq!(cmp("ab9", "ab\u{0}"), Ordering::Less);
        assert_eq!(cmp("ab\u{0}", "ab\u{0}c"), Ordering::Less);
    }

    #[test]
    fn leading_zeros_do_not_change_a_number() {
        assert_eq!(cmp("007", "7"), Ordering::Equal);
        assert_eq!(cmp("000", "0"), Ordering::Equal);
        assert_eq!(cmp("08", "7"), Ordering::Greater);
    }

    fn set_key(labels: &[(&str, &str)]) -> Vec<u8> {
        let mut out = Vec::new();
        for (name, value) in labels {
            if value.is_empty() {
                continue;
            }
            escape(name, &mut out);
            out.extend_from_slice(&SEPARATOR);
            escape(value, &mut out);
            out.extend_from_slice(&SEPARATOR);
        }
        out
    }

    /// `labels.Compare`: name then value, pair by pair, the shorter set
    /// first where one extends the other. A `""` value is a label the
    /// series does not have, so it is not part of the set at all.
    #[test]
    fn a_label_set_key_orders_like_labels_compare() {
        assert!(set_key(&[("a", "1")]) < set_key(&[("a", "2")]));
        assert!(set_key(&[("a", "1")]) < set_key(&[("b", "0")]));
        assert!(set_key(&[("a", "1")]) < set_key(&[("a", "1"), ("b", "0")]));
        assert_eq!(
            set_key(&[("a", "1"), ("b", "")]),
            set_key(&[("a", "1"), ("c", "")])
        );
        // A separator inside a value cannot be forged out of content.
        assert!(set_key(&[("a", "1")]) < set_key(&[("a", "1\u{0}")]));
    }

    /// A store that hands its series over one partition per series.
    ///
    /// `SeriesSource` says nothing about how many partitions a store's plan
    /// has, and a real one — a file per block, a shard per store node —
    /// will have several. `MemorySeriesSource` has exactly one, which is
    /// the case that cannot catch [the property below].
    ///
    /// [the property below]: the_root_merges_the_parallel_sorts_into_one_partition
    #[derive(Debug)]
    struct Split(Vec<crate::Series>);

    #[async_trait::async_trait]
    impl crate::SeriesSource for Split {
        async fn select(
            &self,
            _state: &dyn datafusion::catalog::Session,
            _matchers: &[promql_parser::ast::LabelMatcher],
            _hints: crate::SelectHints,
        ) -> datafusion::error::Result<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
            let names = crate::series::label_names_of(&self.0);
            let mut fields = crate::series::schema(&names).fields().to_vec();
            fields.push(Arc::new(datafusion::arrow::datatypes::Field::new(
                crate::series::SERIES_ID,
                DataType::UInt64,
                false,
            )));
            let schema = Arc::new(datafusion::arrow::datatypes::Schema::new(fields));
            let partitions: Vec<Vec<_>> = self
                .0
                .iter()
                .enumerate()
                .map(|(id, s)| {
                    let batch = crate::series::encode(
                        &names,
                        std::slice::from_ref(s),
                        // Whole series: one block answers every step.
                        crate::series::Block {
                            start_ms: i64::MIN,
                            end_ms: i64::MAX,
                        },
                    )
                    .expect("one series is one batch");
                    let mut columns = batch.columns().to_vec();
                    columns.push(Arc::new(datafusion::arrow::array::UInt64Array::from(vec![
                        id as u64,
                    ])));
                    vec![
                        datafusion::arrow::array::RecordBatch::try_new(schema.clone(), columns)
                            .unwrap(),
                    ]
                })
                .collect();
            Ok(
                datafusion::datasource::memory::MemorySourceConfig::try_new_exec(
                    &partitions,
                    schema,
                    None,
                )?,
            )
        }
    }

    /// The load-bearing property behind every assertion in `tests/sort.rs`:
    /// the plan's root must hand back **one** output partition.
    ///
    /// `Engine` configures nothing, so DataFusion is free to sort each input
    /// partition on its own. What makes the result ordered is that the
    /// physical planner then caps the root with a
    /// `SortPreservingMergeExec` — everything downstream (`collect`,
    /// `drop_empty`, `decode`, the conformance harness) only preserves the
    /// order it is given, it never establishes one. Two partitions reaching
    /// the caller and the rows would interleave by whichever finished
    /// first, silently and only on some machines and some stores.
    ///
    /// Five input partitions from [`Split`] and eight target partitions,
    /// because a single-partition store or a one-core machine would satisfy
    /// this for the wrong reason.
    #[test]
    fn the_root_merges_the_parallel_sorts_into_one_partition() {
        use datafusion::physical_plan::{collect, ExecutionPlanProperties};
        use datafusion::prelude::{SessionConfig, SessionContext};

        let state = datafusion::execution::session_state::SessionStateBuilder::new()
            .with_config(SessionConfig::new().with_target_partitions(8))
            .with_default_features()
            .with_query_planner(Arc::new(crate::series_eval::SeriesEvalQueryPlanner))
            .build();
        let ctx = SessionContext::new_with_state(state);
        // `tests/sort.rs`'s `VALUES`, one series per partition and out of
        // value order.
        let source = Split(
            [
                ("a", 3.0),
                ("b", 1.0),
                ("c", f64::NAN),
                ("d", 2.0),
                ("e", 1.0),
            ]
            .into_iter()
            .map(|(i, v)| {
                crate::Series::new(&[("__name__", "x"), ("i", i)], vec![0], vec![v]).unwrap()
            })
            .collect(),
        );
        let expr = promql_parser::parse_expr("sort(x)").expect("the query parses");
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();

        // Through `plan::plan` rather than `Engine`, which owns its session
        // and so cannot be told to use more than one partition. Register the
        // same evaluation extension planner while keeping this test's parallelism.
        let (partitions, rendered, values) = rt.block_on(async {
            let plan = crate::plan::plan(&ctx.state(), &source, &expr, &INSTANT)
                .await
                .expect("the query plans");
            let physical = ctx
                .state()
                .create_physical_plan(&plan)
                .await
                .expect("the plan lowers");
            let partitions = physical.output_partitioning().partition_count();
            let rendered = datafusion::physical_plan::displayable(physical.as_ref())
                .indent(false)
                .to_string();
            let batches = collect(physical, ctx.task_ctx()).await.expect("it runs");
            let values: Vec<f64> = crate::series::decode(&batches)
                .expect("the result is canonical")
                .iter()
                .map(|s| s.values()[0])
                .collect();
            (partitions, rendered, values)
        });

        assert_eq!(partitions, 1, "the root must not fan out to the caller");
        // Without this the count above would hold for the boring reason
        // that nothing was parallelised in the first place.
        assert!(
            rendered.contains("SortPreservingMergeExec"),
            "the sort ran on one partition, so the merge was never exercised:\n{rendered}"
        );
        assert_eq!(values[0..4], [1.0, 1.0, 2.0, 3.0]);
        assert!(values[4].is_nan(), "{values:?}");
    }
}
