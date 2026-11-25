//! Upstream's `DropName` and `cleanupMetricLabels` (`promql/engine.go` at
//! 83962c35). A plan node marks all its rows with a flag; only where rows can
//! differ (`or`, `label_replace`) does the mark become the [`DROP_NAME`]
//! label, which rides every operator that copies labels.

use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, AsArray, BooleanArray, StringViewArray, StructArray};
use datafusion::arrow::compute::kernels::cmp::eq;
use datafusion::arrow::compute::kernels::zip::zip;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Fields};
use datafusion::common::{internal_err, plan_err, ScalarValue};
use datafusion::error::Result;
use datafusion::functions::core::expr_fn::get_field;
use datafusion::functions_aggregate::expr_fn::max;
use datafusion::functions_nested::expr_fn::cardinality;
use datafusion::logical_expr::{
    col, lit, ColumnarValue, Expr, ExprFunctionExt, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF,
    ScalarUDFImpl, Signature, Volatility,
};

use crate::labels;
use crate::matcher::METRIC_NAME;
use crate::series::{self, LABELS, SAMPLES};

/// The reserved label carrying upstream's `DropName` per row.
pub const DROP_NAME: &str = "__promql_drop_name__";

/// The value [`DROP_NAME`] holds on a row whose name is to be dropped.
const DROP: &str = "1";

/// The labels upstream's `schema.IsMetadataLabel` is true of, which
/// `DropReserved` removes together.
pub const METADATA: [&str; 3] = [METRIC_NAME, "__type__", "__unit__"];

pub fn is_metadata(name: &str) -> bool {
    METADATA.contains(&name)
}

/// Whether `names` carries anything a mark would drop.
pub fn has_metadata(names: &[String]) -> bool {
    names.iter().any(|name| is_metadata(name))
}

pub fn has_mark(names: &[String]) -> bool {
    names.iter().any(|name| name == DROP_NAME)
}

/// `names` with [`DROP_NAME`] added, sorted.
pub fn with_mark(names: &[String]) -> Vec<String> {
    let mut out = names.to_vec();
    out.push(DROP_NAME.to_string());
    out.sort();
    out.dedup();
    out
}

/// Which [`METADATA`] labels the operators above a plan node can still
/// read. Unknown (`None` where it is passed) means all of them.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Reads([bool; 3]);

impl Reads {
    /// The metadata labels among `names`.
    pub fn named(names: &[String]) -> Reads {
        Reads(METADATA.map(|label| names.iter().any(|name| name == label)))
    }

    /// What a grouping or a match signature that lists `names` out keeps:
    /// every metadata label but the name, which upstream always leaves out.
    pub fn all_but_name_and(names: &[String]) -> Reads {
        let listed = Reads::named(names);
        Reads([false, !listed.0[1], !listed.0[2]])
    }

    /// What a `by`/`on` (`only`) or a `without`/`ignoring` list keeps.
    pub fn kept_by(names: &[String], only: bool) -> Reads {
        match only {
            true => Reads::named(names),
            false => Reads::all_but_name_and(names),
        }
    }

    pub fn or(self, other: Reads) -> Reads {
        Reads([0, 1, 2].map(|i| self.0[i] || other.0[i]))
    }

    fn any_in(self, names: &[String]) -> bool {
        METADATA
            .iter()
            .zip(self.0)
            .any(|(label, read)| read && names.iter().any(|name| name == label))
    }
}

/// The `labels` struct of an operator that sets `DropName: true` on every
/// row: the name kept and the flag set, or, where nothing above can read
/// any metadata label these rows carry, the metadata dropped at once.
/// Returns the flag.
pub fn mark(input: &[String], reads: Option<Reads>) -> (Expr, Vec<String>, bool) {
    if reads.is_some_and(|reads| !reads.any_in(input)) {
        let (expr, names) = labels::keep(input, |n| !is_metadata(n) && n != DROP_NAME);
        return (expr, names, false);
    }
    let (expr, names) = labels::keep(input, |n| n != DROP_NAME);
    let marked = has_metadata(&names);
    (expr, names, marked)
}

/// [`mark`] where the operator drops the name, else the input's labels
/// and flag carried through.
pub fn mark_if(
    drops: bool,
    input: &[String],
    marked: bool,
    reads: Option<Reads>,
) -> (Expr, Vec<String>, bool) {
    if drops {
        return mark(input, reads);
    }
    let (expr, names) = labels::keep(input, |_| true);
    (expr, names, marked)
}

/// The [`DROP_NAME`] value of a row from a plan node whose flag is `marked`.
pub fn value(input: &[String], marked: bool) -> Expr {
    match marked {
        true => lit(DROP),
        false => labels::field(input, DROP_NAME),
    }
}

/// A group's mark, upstream's `groupedAggregation.dropName`: set if any
/// member that answered was marked. Upstream decides it per step; one mark
/// per output row cannot, so a range query takes it over the whole row.
pub fn any_marked() -> Expr {
    max(get_field(col(LABELS), DROP_NAME))
        .filter(cardinality(col(SAMPLES)).gt(lit(0u64)))
        .build()
        .expect("a filtered max is a well formed aggregate")
}

/// `cleanupMetricLabels`: `promql_drop_name(labels, all)`, the metadata
/// labels emptied on every row if `all`, else where the mark is set, and the
/// mark gone. One call over the whole struct: a per-field `get_field` above a
/// root sort trips DataFusion's `push_down_leaf_projections`.
pub fn cleanup(all: bool) -> Expr {
    ScalarUDF::new_from_impl(Cleanup::default()).call(vec![col(LABELS), lit(all)])
}

const NAME: &str = "promql_drop_name";

#[derive(Debug, PartialEq, Eq, Hash)]
struct Cleanup {
    signature: Signature,
}

impl Default for Cleanup {
    fn default() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for Cleanup {
    fn name(&self) -> &str {
        NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!("{NAME}: return_field_from_args should be called instead")
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let DataType::Struct(fields) = args.arg_fields[0].data_type() else {
            return plan_err!("{NAME}: expects the labels struct");
        };
        let names: Vec<String> = fields
            .iter()
            .map(|field| field.name().clone())
            .filter(|name| name != DROP_NAME)
            .collect();
        Ok(Arc::new(Field::new(
            NAME,
            series::labels_type(&names),
            false,
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let labels = args.args[0].to_array(args.number_rows)?;
        let labels = labels.as_struct();
        let all = matches!(
            &args.args[1],
            ColumnarValue::Scalar(ScalarValue::Boolean(Some(true)))
        );
        let empty = StringViewArray::new_scalar("");
        // `None`: no row keeps its metadata.
        let kept: Option<BooleanArray> = match (all, labels.column_by_name(DROP_NAME)) {
            (true, _) => None,
            (false, Some(mark)) => Some(eq(mark, &empty)?),
            (false, None) => return Ok(ColumnarValue::Array(Arc::new(labels.clone()))),
        };
        // One column of `""` shared by every metadata label.
        let blank: Option<ArrayRef> = kept.is_none().then(|| {
            Arc::new(StringViewArray::from_iter_values(std::iter::repeat_n(
                "",
                args.number_rows,
            ))) as ArrayRef
        });
        let mut fields = Vec::with_capacity(labels.num_columns());
        let mut columns = Vec::with_capacity(labels.num_columns());
        for (field, column) in labels.fields().iter().zip(labels.columns()) {
            match field.name().as_str() {
                DROP_NAME => continue,
                name if is_metadata(name) => columns.push(match &kept {
                    None => Arc::clone(blank.as_ref().expect("built where nothing is kept")),
                    Some(kept) if kept.true_count() == kept.len() => Arc::clone(column),
                    Some(kept) => zip(kept, column, &empty)?,
                }),
                _ => columns.push(Arc::clone(column)),
            }
            fields.push(Arc::clone(field));
        }
        let out = match fields.is_empty() {
            true => StructArray::new_empty_fields(args.number_rows, None),
            false => StructArray::try_new(Fields::from(fields), columns, None)?,
        };
        Ok(ColumnarValue::Array(Arc::new(out)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(v: &[&str]) -> Vec<String> {
        v.iter().map(|x| x.to_string()).collect()
    }

    #[test]
    fn marking_keeps_the_name_and_sets_the_flag() {
        let (_, names, marked) = mark(&s(&["__name__", DROP_NAME, "pod"]), None);
        assert_eq!(names, s(&["__name__", "pod"]));
        assert!(marked);
    }

    #[test]
    fn a_label_set_without_metadata_is_not_marked() {
        let (_, names, marked) = mark(&s(&["pod"]), None);
        assert_eq!(names, s(&["pod"]));
        assert!(!marked);
    }

    #[test]
    fn marking_drops_the_metadata_at_once_where_none_is_read() {
        let input = s(&["__name__", "__type__", "pod"]);
        let (_, names, marked) = mark(&input, Some(Reads::default()));
        assert_eq!(names, s(&["pod"]));
        assert!(!marked);
        let (_, names, marked) = mark(&input, Some(Reads::all_but_name_and(&[])));
        assert_eq!(names, input);
        assert!(marked);
    }

    #[test]
    fn listing_a_label_out_stops_it_being_read() {
        let reads = Reads::all_but_name_and(&s(&["__type__"]));
        assert!(!reads.any_in(&s(&["__name__", "__type__"])));
        assert!(reads.any_in(&s(&["__unit__"])));
        assert!(Reads::named(&s(&["__name__"])).any_in(&s(&["__name__"])));
    }

    #[test]
    fn with_mark_adds_the_mark_once_in_order() {
        let names = with_mark(&with_mark(&s(&["__type__", "pod"])));
        assert_eq!(names, s(&[DROP_NAME, "__type__", "pod"]));
    }
}
