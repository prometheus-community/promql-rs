//! PromQL AST node types. Structural mirror of `upstream/ast.go`.
//!
//! Upstream uses a `Node` interface with `Expr` / `Statement`
//! sub-interfaces and one struct per node kind. In Rust we collapse
//! expression types into a single [`Expr`] enum for exhaustive matching;
//! statements are a separate enum. Field names are preserved, case-shifted
//! to Rust's `snake_case`. A non-trivial field-name change would cost us
//! diff review against future upstream.

use std::fmt;

use crate::posrange::{Pos, PositionRange};
use crate::token::ItemType;
use promql_histogram::FloatHistogram;

/// Value classification, mirroring upstream `ValueType`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ValueType {
    None,
    Scalar,
    Vector,
    Matrix,
    String,
}

/// Label-matcher operator. Mirrors `labels.MatchType` upstream.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MatchOp {
    Equal,
    NotEqual,
    RegexEqual,
    RegexNotEqual,
}

impl MatchOp {
    pub fn from_item_type(it: ItemType) -> Option<Self> {
        Some(match it {
            ItemType::Eql => MatchOp::Equal,
            ItemType::Neq => MatchOp::NotEqual,
            ItemType::EqlRegex => MatchOp::RegexEqual,
            ItemType::NeqRegex => MatchOp::RegexNotEqual,
            _ => return None,
        })
    }
}

/// Port of `labels.MatchType.String()`.
impl fmt::Display for MatchOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            MatchOp::Equal => "=",
            MatchOp::NotEqual => "!=",
            MatchOp::RegexEqual => "=~",
            MatchOp::RegexNotEqual => "!~",
        })
    }
}

/// Single label matcher from a `{…}` selector.
#[derive(Debug, Clone, PartialEq)]
pub struct LabelMatcher {
    pub name: String,
    pub op: MatchOp,
    pub value: String,
    pub pos_range: PositionRange,
}

/// Port of `labels.Matcher.String()`: `name<op>"value"`, `name` quoted
/// with Go's `%q` when it isn't a bare identifier (`shouldQuoteName`
/// upstream) — a name matcher's own name is never quoted in practice
/// (the parser only accepts identifiers there), but a defensive port
/// stays correct if that ever changes.
impl fmt::Display for LabelMatcher {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if should_quote_name(&self.name) {
            write!(f, "{}", go_quote(&self.name))?;
        } else {
            f.write_str(&self.name)?;
        }
        write!(f, "{}{}", self.op, go_quote(&self.value))
    }
}

/// Port of `Matcher.shouldQuoteName`: a bare name is `[A-Za-z_][A-Za-z0-9_]*`.
fn should_quote_name(name: &str) -> bool {
    let mut chars = name.char_indices();
    match chars.next() {
        Some((_, c)) if c == '_' || c.is_ascii_alphabetic() => {}
        _ => return true,
    }
    for (i, c) in chars {
        if c == '_' || c.is_ascii_alphabetic() || (i > 0 && c.is_ascii_digit()) {
            continue;
        }
        return true;
    }
    false
}

/// Port of Go's `strconv.AppendQuote`, the quoting `%q` and
/// `Matcher.String()` use: a `"`-delimited string, `appendEscapedRune`
/// (`strconv/quote.go`) run over every rune.
fn go_quote(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        append_escaped_rune(&mut out, c);
    }
    out.push('"');
    out
}

/// Port of `appendEscapedRune`, the double-quote case (`quote == '"'`,
/// `ASCIIonly` and `graphicOnly` both false — the only call
/// `Quote`/`AppendQuote` ever make). Go's own control-character names
/// (`\a`, `\b`, `\f`, `\v`) come before the generic `\xNN`/`\uNNNN`/
/// `\UNNNNNNNN` fallback, in the same order, since `\n`, `\r`, `\t` and
/// `\x07` (BEL) all have a named form that would otherwise be shadowed
/// by the generic one below.
fn append_escaped_rune(out: &mut String, r: char) {
    if r == '"' || r == '\\' {
        out.push('\\');
        out.push(r);
        return;
    }
    if is_go_print(r) {
        out.push(r);
        return;
    }
    match r {
        '\u{07}' => out.push_str("\\a"),
        '\u{08}' => out.push_str("\\b"),
        '\u{0c}' => out.push_str("\\f"),
        '\n' => out.push_str("\\n"),
        '\r' => out.push_str("\\r"),
        '\t' => out.push_str("\\t"),
        '\u{0b}' => out.push_str("\\v"),
        _ => {
            let cp = r as u32;
            if cp < 0x20 || r == '\u{7f}' {
                out.push_str(&format!("\\x{cp:02x}"));
            } else if cp < 0x10000 {
                out.push_str(&format!("\\u{cp:04x}"));
            } else {
                out.push_str(&format!("\\U{cp:08x}"));
            }
        }
    }
}

/// Port of `unicode.IsPrint` as `appendEscapedRune` uses it: ASCII space
/// through `~` prints raw; above ASCII, Go prints everything except the
/// control (Cc), format (Cf), private-use (Co), surrogate (Cs) and
/// unassigned (Cn) categories, and every separator (Zs/Zl/Zp) but the
/// plain ASCII space already handled above.
///
/// `char::is_control` covers Cc (which is what makes U+0085 escape, the
/// test below). `char::is_whitespace` is a proxy for the Zs/Zl/Zp
/// carve-out — it is the Unicode `White_Space` property, not exactly
/// "separator", but it is exactly the separators this engine's label
/// values are ever built from (space, NBSP, line/paragraph separator),
/// and it costs nothing else since none of the plain-space or control
/// runes it also matches reach this branch.
///
/// Cf/Co/Cn are not excluded: doing that needs full Unicode category
/// tables, which the standard library does not expose and this crate
/// pulls in no dependency for. Per AGENTS.md, Go wins on a float
/// disagreement; here the stated divergence is that a format, private-
/// use or unassigned code point in a label value prints raw where Go
/// would escape it, rather than silently claiming to match `IsPrint`.
fn is_go_print(r: char) -> bool {
    if r.is_ascii() {
        return (' '..='~').contains(&r);
    }
    !(r.is_control() || r.is_whitespace())
}

/// One point of a series description's value sequence. Mirrors
/// upstream `parse.go`'s `SequenceValue`.
///
/// A point is a float, a native histogram (`{{schema:1 ...}}`) or a gap.
/// `value` is meaningless when `histogram` is set, as upstream's `Value`
/// is when its `Histogram` is non-nil.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct SequenceValue {
    pub value: f64,
    pub omitted: bool,
    pub histogram: Option<FloatHistogram>,
    /// Upstream's `CounterResetHintSet`: the descriptor spelled a
    /// `counter_reset_hint`, so a comparison must match it. An unspelled
    /// hint is "don't care", not `unknown`.
    pub counter_reset_hint_set: bool,
}

impl SequenceValue {
    pub fn value(value: f64) -> Self {
        Self {
            value,
            ..Self::default()
        }
    }

    /// The `_` placeholder: a gap in the sequence.
    pub fn omitted() -> Self {
        Self {
            omitted: true,
            ..Self::default()
        }
    }

    pub fn histogram(histogram: FloatHistogram, counter_reset_hint_set: bool) -> Self {
        Self {
            histogram: Some(histogram),
            counter_reset_hint_set,
            ..Self::default()
        }
    }
}

/// A parsed series description — one `metric{...} <values>` line of a
/// promqltest load block. Mirrors upstream `parse.go`'s
/// `seriesDescription`.
///
/// `labels` are the metric's label set, carried as `Equal` matchers
/// (including `__name__`) so the shape matches
/// [`crate::parse_metric_selector`].
#[derive(Debug, Clone, PartialEq, Default)]
pub struct SeriesDescription {
    pub labels: Vec<LabelMatcher>,
    pub values: Vec<SequenceValue>,
}

/// Vector-matching cardinality. Mirrors upstream `VectorMatchCardinality`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum VectorMatchCardinality {
    #[default]
    OneToOne,
    ManyToOne,
    OneToMany,
    ManyToMany,
}

/// Fill values for vector matching with `fill` / `fill_left` /
/// `fill_right`. Upstream `VectorMatchFillValues`.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct VectorMatchFillValues {
    pub lhs: Option<f64>,
    pub rhs: Option<f64>,
}

/// Vector-matching shape for a binary expression. Upstream
/// `VectorMatching`.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct VectorMatching {
    pub card: VectorMatchCardinality,
    pub matching_labels: Vec<String>,
    pub on: bool,
    pub include: Vec<String>,
    pub fill_values: VectorMatchFillValues,
}

/// `@ start()` / `@ end()` preprocessor. Upstream represents this via
/// `ItemType`; we use a dedicated enum for type safety in Rust.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AtModifier {
    Start,
    End,
}

/// All expression node types. Upstream `Expr` interface flattened.
#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    Aggregate(AggregateExpr),
    Binary(BinaryExpr),
    Call(Call),
    MatrixSelector(MatrixSelector),
    NumberLiteral(NumberLiteral),
    Paren(ParenExpr),
    StringLiteral(StringLiteral),
    Subquery(SubqueryExpr),
    Unary(UnaryExpr),
    VectorSelector(VectorSelector),
    StepInvariant(Box<Expr>),
    Duration(DurationExpr),
}

impl Expr {
    pub fn value_type(&self) -> ValueType {
        match self {
            Expr::Aggregate(_) | Expr::VectorSelector(_) => ValueType::Vector,
            Expr::Binary(e) => {
                if matches!(e.lhs.value_type(), ValueType::Scalar)
                    && matches!(e.rhs.value_type(), ValueType::Scalar)
                {
                    ValueType::Scalar
                } else {
                    ValueType::Vector
                }
            }
            Expr::Call(c) => c.func.return_type,
            Expr::MatrixSelector(_) | Expr::Subquery(_) => ValueType::Matrix,
            Expr::NumberLiteral(_) | Expr::Duration(_) => ValueType::Scalar,
            Expr::Paren(p) => p.expr.value_type(),
            Expr::StringLiteral(_) => ValueType::String,
            Expr::Unary(u) => u.expr.value_type(),
            Expr::StepInvariant(e) => e.value_type(),
        }
    }

    pub fn position_range(&self) -> PositionRange {
        match self {
            Expr::Aggregate(e) => e.pos_range,
            Expr::Binary(e) => PositionRange::merge(e.lhs.position_range(), e.rhs.position_range()),
            Expr::Call(e) => e.pos_range,
            Expr::MatrixSelector(e) => PositionRange {
                start: e.vector_selector.position_range().start,
                end: e.end_pos,
            },
            Expr::NumberLiteral(e) => e.pos_range,
            Expr::Paren(e) => e.pos_range,
            Expr::StringLiteral(e) => e.pos_range,
            Expr::Subquery(e) => PositionRange {
                start: e.expr.position_range().start,
                end: e.end_pos,
            },
            Expr::Unary(e) => PositionRange {
                start: e.start_pos,
                end: e.expr.position_range().end,
            },
            Expr::VectorSelector(e) => e.pos_range,
            Expr::StepInvariant(e) => e.position_range(),
            Expr::Duration(e) => e.position_range(),
        }
    }
}

/// Upstream `AggregateExpr`.
#[derive(Debug, Clone, PartialEq)]
pub struct AggregateExpr {
    pub op: ItemType,
    pub expr: Box<Expr>,
    pub param: Option<Box<Expr>>,
    pub grouping: Vec<String>,
    pub without: bool,
    pub pos_range: PositionRange,
}

/// Upstream `BinaryExpr`.
#[derive(Debug, Clone, PartialEq)]
pub struct BinaryExpr {
    pub op: ItemType,
    pub lhs: Box<Expr>,
    pub rhs: Box<Expr>,
    pub vector_matching: Option<VectorMatching>,
    pub return_bool: bool,
}

/// Reference to a built-in function. Upstream `*Function` is a pointer
/// into a global registry; we carry the resolved metadata in the AST so
/// consumers don't need the registry at query time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FunctionRef {
    pub name: String,
    pub arg_types: Vec<ValueType>,
    pub return_type: ValueType,
    /// Upstream `Variadic`: index from which args are variadic, or 0/−1
    /// encoding per upstream convention.
    pub variadic: i32,
    pub experimental: bool,
}

/// Upstream `Call`.
#[derive(Debug, Clone, PartialEq)]
pub struct Call {
    pub func: FunctionRef,
    pub args: Vec<Expr>,
    pub pos_range: PositionRange,
}

/// Upstream `MatrixSelector`.
#[derive(Debug, Clone, PartialEq)]
pub struct MatrixSelector {
    pub vector_selector: Box<Expr>,
    /// Upstream `Range time.Duration` — stored as seconds. Zero when the
    /// range comes from a `DurationExpr` (step()/range() experimental).
    pub range_secs: f64,
    pub range_expr: Option<Box<DurationExpr>>,
    pub end_pos: Pos,
}

/// Upstream `SubqueryExpr`.
#[derive(Debug, Clone, PartialEq)]
pub struct SubqueryExpr {
    pub expr: Box<Expr>,
    pub range_secs: f64,
    pub range_expr: Option<Box<DurationExpr>>,
    pub original_offset_secs: f64,
    pub original_offset_expr: Option<Box<DurationExpr>>,
    pub offset_secs: f64,
    pub timestamp: Option<i64>,
    pub start_or_end: Option<AtModifier>,
    pub step_secs: f64,
    pub step_expr: Option<Box<DurationExpr>>,
    pub end_pos: Pos,
}

/// Upstream `NumberLiteral`.
#[derive(Debug, Clone, PartialEq)]
pub struct NumberLiteral {
    pub val: f64,
    pub duration: bool,
    pub pos_range: PositionRange,
}

/// Upstream `ParenExpr`.
#[derive(Debug, Clone, PartialEq)]
pub struct ParenExpr {
    pub expr: Box<Expr>,
    pub pos_range: PositionRange,
}

/// Upstream `StringLiteral`.
#[derive(Debug, Clone, PartialEq)]
pub struct StringLiteral {
    pub val: String,
    pub pos_range: PositionRange,
}

/// Upstream `UnaryExpr`.
#[derive(Debug, Clone, PartialEq)]
pub struct UnaryExpr {
    pub op: ItemType,
    pub expr: Box<Expr>,
    pub start_pos: Pos,
}

/// Upstream `VectorSelector`. Fields that upstream populates during query
/// execution (`UnexpandedSeriesSet`, `Series`) are intentionally omitted:
/// the parser crate has no storage dependency.
#[derive(Debug, Clone, PartialEq)]
pub struct VectorSelector {
    pub name: String,
    pub original_offset_secs: f64,
    pub original_offset_expr: Option<Box<DurationExpr>>,
    pub offset_secs: f64,
    pub timestamp: Option<i64>,
    pub skip_histogram_buckets: bool,
    pub start_or_end: Option<AtModifier>,
    pub label_matchers: Vec<LabelMatcher>,
    pub bypass_empty_matcher_check: bool,
    pub anchored: bool,
    pub smoothed: bool,
    pub pos_range: PositionRange,
}

/// Upstream `DurationExpr`. Used for experimental duration arithmetic
/// (`step()`, `range()`, `min()`, `max()`, and operator-joined
/// durations).
#[derive(Debug, Clone, PartialEq)]
pub struct DurationExpr {
    pub op: ItemType,
    pub lhs: Option<Box<Expr>>,
    pub rhs: Option<Box<Expr>>,
    pub wrapped: bool,
    pub start_pos: Pos,
    pub end_pos: Pos,
}

impl DurationExpr {
    pub fn position_range(&self) -> PositionRange {
        PositionRange {
            start: self.start_pos,
            end: self.end_pos,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Each case checked against `strconv.Quote` itself (`go run` on the
    /// pinned Go toolchain), not against a hand-derived expectation.
    #[test]
    fn go_quote_matches_go_strconv_quote() {
        let cases: &[(&str, &str)] = &[
            ("a\"b", "\"a\\\"b\""),
            ("a\\b", "\"a\\\\b\""),
            ("\n", "\"\\n\""),
            ("\u{07}", "\"\\a\""), // BEL, named \a, not \x07
            ("\u{7f}", "\"\\x7f\""),
            ("\u{85}", "\"\\u0085\""),   // NEL, a Cc control above ASCII
            ("\u{a0}", "\"\\u00a0\""),   // NBSP, Zs
            ("\u{2028}", "\"\\u2028\""), // LINE SEPARATOR, Zl
            ("ü", "\"ü\""),              // printable non-ASCII: stays raw
        ];
        for (input, want) in cases {
            assert_eq!(&go_quote(input), want, "go_quote({input:?})");
        }
    }

    #[test]
    fn label_matcher_display_matches_go_matcher_string() {
        let m = |name: &str, op, value: &str| LabelMatcher {
            name: name.to_string(),
            op,
            value: value.to_string(),
            pos_range: PositionRange::default(),
        };
        assert_eq!(m("job", MatchOp::Equal, "api").to_string(), r#"job="api""#);
        assert_eq!(
            m("job", MatchOp::RegexEqual, "a|b").to_string(),
            r#"job=~"a|b""#
        );
    }
}
