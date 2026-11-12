//! `label_replace`'s destination value: upstream's `evalLabelReplace`
//! (`promql/functions.go` at 83962c35) applied per row of the `labels`
//! struct. `label_join` needs no kernel: `concat_ws` is `strings.Join`.

use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{AsArray, StringViewBuilder};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::plan_err;
use datafusion::error::Result;
use datafusion::logical_expr::{
    lit, ColumnarValue, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl,
    Signature, Volatility,
};
use regex::Regex;

use crate::error::EngineError;
use crate::{labels, series};

pub const NAME: &str = "promql_label_replace";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Func {
    Replace,
    Join,
}

impl Func {
    pub fn parse(name: &str) -> Option<Self> {
        match name {
            "label_replace" => Some(Self::Replace),
            "label_join" => Some(Self::Join),
            _ => None,
        }
    }
}

/// Compiled at plan time, so an invalid pattern is refused before the
/// store is asked for anything, as upstream refuses it before `ev.eval`.
#[derive(Debug)]
pub struct LabelReplace {
    signature: Signature,
    regex: Regex,
    replacement: String,
}

// `Regex` has no equality of its own; its source text is the identity.
impl PartialEq for LabelReplace {
    fn eq(&self, other: &Self) -> bool {
        self.regex.as_str() == other.regex.as_str() && self.replacement == other.replacement
    }
}

impl Eq for LabelReplace {}

impl Hash for LabelReplace {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.regex.as_str().hash(state);
        self.replacement.hash(state);
    }
}

/// Upstream wraps the pattern as `^(?s:` + regex + `)$`: a full-string
/// match where `.` also crosses newlines. The message is upstream's.
pub fn compile(regex: &str) -> Result<Regex, EngineError> {
    Regex::new(&format!("^(?s:{regex})$")).map_err(|_| {
        EngineError::Query(format!(
            "invalid regular expression in label_replace(): {regex}"
        ))
    })
}

/// `promql_label_replace(src, dst, '<regex>', '<replacement>')`. The two
/// literals only make the plan text say what runs; the compiled `regex`
/// is what does.
pub fn call(src: Expr, dst: Expr, regex: Regex, replacement: &str) -> Expr {
    let pattern = regex.as_str().to_string();
    let udf = ScalarUDF::new_from_impl(LabelReplace {
        signature: Signature::any(4, Volatility::Immutable),
        regex,
        replacement: replacement.to_string(),
    });
    udf.call(vec![src, dst, lit(pattern), lit(replacement)])
}

impl ScalarUDFImpl for LabelReplace {
    fn name(&self) -> &str {
        NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(series::label_type())
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        for (position, field) in args.arg_fields.iter().take(2).enumerate() {
            if !matches!(field.data_type(), DataType::Utf8View | DataType::Utf8) {
                return plan_err!(
                    "{NAME}: argument {position} must be a label value, got {}",
                    field.data_type()
                );
            }
        }
        Ok(Arc::new(Field::new(NAME, series::label_type(), false)))
    }

    /// Upstream's loop body: `FindStringSubmatchIndex` on the source,
    /// `ExpandString` into the destination on a match, the destination
    /// untouched otherwise. An empty result is how the shape spells a
    /// deleted label, which is what `lb.Set(dst, "")` does upstream.
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let rows = args.number_rows;
        let src = labels::canonical(&args.args[0].to_array(rows)?)?;
        let dst = labels::canonical(&args.args[1].to_array(rows)?)?;
        let (src, dst) = (src.as_string_view(), dst.as_string_view());
        let mut out = StringViewBuilder::with_capacity(rows);
        let mut expanded = String::new();
        for row in 0..rows {
            match self.regex.captures(src.value(row)) {
                Some(captures) => {
                    expanded.clear();
                    // `Captures::expand` reads `$1`, `${1}`, `$name` and
                    // `$$` as Go's `Regexp.ExpandString` does, the
                    // longest name winning (`$1x` is group `1x`).
                    captures.expand(&self.replacement, &mut expanded);
                    out.append_value(&expanded);
                }
                None => out.append_value(dst.value(row)),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::StringViewArray;
    use datafusion::common::config::ConfigOptions;

    use super::*;

    fn replace(regex: &str, replacement: &str, src: &[&str], dst: &[&str]) -> Vec<String> {
        let Expr::ScalarFunction(f) = call(lit(""), lit(""), compile(regex).unwrap(), replacement)
        else {
            unreachable!()
        };
        let column = |v: &[&str]| -> ColumnarValue {
            ColumnarValue::Array(Arc::new(StringViewArray::from_iter_values(v)))
        };
        let field = Arc::new(Field::new("x", series::label_type(), false));
        let out = f
            .func
            .invoke_with_args(ScalarFunctionArgs {
                args: vec![
                    column(src),
                    column(dst),
                    lit_value(regex),
                    lit_value(replacement),
                ],
                arg_fields: vec![Arc::clone(&field); 4],
                number_rows: src.len(),
                return_field: field,
                config_options: Arc::new(ConfigOptions::default()),
            })
            .unwrap()
            .to_array(src.len())
            .unwrap();
        out.as_string_view()
            .iter()
            .map(|v| v.unwrap().to_string())
            .collect()
    }

    fn lit_value(s: &str) -> ColumnarValue {
        ColumnarValue::Scalar(s.into())
    }

    #[test]
    fn the_match_is_the_whole_value() {
        assert_eq!(
            replace("value-(.*)", "x-$1", &["source-value-10"], &["orig"]),
            ["orig"]
        );
        assert_eq!(
            replace("source-value-(.*)", "x-$1", &["source-value-10"], &["orig"]),
            ["x-10"]
        );
    }

    #[test]
    fn no_match_keeps_the_destination() {
        assert_eq!(replace("a", "b", &["z", "a"], &["d1", "d2"]), ["d1", "b"]);
    }

    #[test]
    fn dot_crosses_newlines() {
        assert_eq!(replace("a.b", "y", &["a\nb"], &[""]), ["y"]);
    }

    /// Go's `ExpandString`: braced and named groups, `$$`, and the
    /// longest name, so `$1x` is a group that does not exist.
    #[test]
    fn the_template_is_gos() {
        assert_eq!(
            replace("(?P<n>a)(b)", "${1}-$2-$n-$$", &["ab"], &[""]),
            ["a-b-a-$"]
        );
        assert_eq!(replace("(a)", "$1x", &["a"], &[""]), [""]);
    }

    #[test]
    fn an_invalid_pattern_is_upstreams_error() {
        assert_eq!(
            compile("(.*").unwrap_err().to_string(),
            "invalid regular expression in label_replace(): (.*"
        );
    }
}
