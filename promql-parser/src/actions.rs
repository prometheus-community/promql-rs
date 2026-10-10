//! Per-production Rust action helpers. Mirrors upstream `parse.go`
//! helpers (`newBinaryExpression`, `newAggregateExpr`, `unquoteString`,
//! `addOffset`, `setTimestamp`, etc.).
//!
//! The grammar file calls into these so `src/grammar.y` stays
//! one-line-per-action and structurally diffable against upstream.
//!
//! Lifetime convention: every action takes
//! `&'l dyn NonStreamingLexer<'i, DefaultLexerTypes<u32>>` with
//! `'i: 'l` (the input string outlives the lexer borrow). This matches
//! the `__gt_lexer` / `__gt_input` lifetimes in grmtools' generated
//! parser.
//!
//! Current scope: the subset invoked by the core expression and series
//! description grammars (see `src/grammar.y` for the matching rule
//! coverage). Helpers for histogram descriptors, fill modifiers, and
//! duration-expression arithmetic are not yet implemented.

// Every action returns `Result<T, ()>`: that is grmtools' convention for
// the grammar's `%actiontype`, where `Err(())` signals "this production
// failed, error recovery takes over". The error type is fixed by the
// generated parser, so there is nothing richer to return here.
#![allow(clippy::result_unit_err)]

use cfgrammar::Span;
use lrlex::{DefaultLexeme, DefaultLexerTypes};
use lrpar::{Lexeme, NonStreamingLexer};

use crate::ast::{
    AggregateExpr, AtModifier, BinaryExpr, Call, Expr, FunctionRef, LabelMatcher, MatchOp,
    MatrixSelector, NumberLiteral, ParenExpr, SequenceValue, SeriesDescription, StringLiteral,
    SubqueryExpr, UnaryExpr, ValueType, VectorMatchCardinality, VectorMatching, VectorSelector,
};
use crate::context::ParserCtx;
use crate::posrange::{Pos, PositionRange};
use crate::token::ItemType;

/// Two-lifetime trait-object alias for the lexer trait grmtools passes
/// into every action. `'l` is the borrow lifetime (the `&'l dyn …`),
/// `'i` is the source string lifetime.
type L<'l, 'i> = dyn NonStreamingLexer<'i, DefaultLexerTypes<u32>> + 'l;

/// Short alias for the lexeme type the grammar's terminal actions see.
type Lx = DefaultLexeme<u32>;

/// Helper: source substring for a span.
pub fn span_str<'l, 'i: 'l>(lexer: &'l L<'l, 'i>, span: Span) -> &'i str {
    lexer.span_str(span)
}

fn to_pos_range(span: Span) -> PositionRange {
    PositionRange::new(span.start() as Pos, span.end() as Pos)
}

// -------- binary modifiers --------

/// Intermediate struct for building up binary modifiers through the
/// grammar's left-recursive modifier rules. Mirrors upstream's approach
/// of threading a partial BinaryExpr through bool_modifier →
/// on_or_ignoring → group_modifiers → fill_modifiers.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct BinModifiers {
    pub vector_matching: VectorMatching,
    pub return_bool: bool,
}

// -------- literals --------

pub fn number_literal<'l, 'i: 'l>(lexer: &'l L<'l, 'i>, lx: Lx) -> Result<Expr, ()> {
    let span = lx.span();
    let raw = lexer.span_str(span);
    // Upstream's `number()` reports a literal `ParseFloat` rejects, `1e400`
    // included, as a parse error; a zero here would pass it on silently.
    let val = parse_number_literal(raw)?;
    Ok(Expr::NumberLiteral(NumberLiteral {
        val,
        duration: false,
        pos_range: to_pos_range(span),
    }))
}

/// `number_duration_literal : DURATION`.
///
/// An unparseable duration is an error, never a zero window: nothing
/// downstream validates it. grmtools fixes actions at `Result<T, ()>`,
/// so the span and message are lost until a side channel exists.
pub fn duration_literal<'l, 'i: 'l>(lexer: &'l L<'l, 'i>, lx: Lx) -> Result<Expr, ()> {
    let span = lx.span();
    let raw = lexer.span_str(span);
    let val = parse_duration_seconds(raw)?;
    Ok(Expr::NumberLiteral(NumberLiteral {
        val,
        duration: true,
        pos_range: to_pos_range(span),
    }))
}

pub fn string_literal<'l, 'i: 'l>(lexer: &'l L<'l, 'i>, lx: Lx) -> Result<Expr, ()> {
    let span = lx.span();
    let raw = lexer.span_str(span);
    let val = unquote_string(raw).unwrap_or_else(|_| raw.to_string());
    Ok(Expr::StringLiteral(StringLiteral {
        val,
        pos_range: to_pos_range(span),
    }))
}

/// Return just the string content of a STRING token (with outer
/// quotes stripped and escapes processed), useful when the grammar
/// wants the raw value rather than a full `StringLiteral` expression.
pub fn string_literal_value<'l, 'i: 'l>(lexer: &'l L<'l, 'i>, span: Span) -> Result<String, ()> {
    let raw = lexer.span_str(span);
    Ok(unquote_string(raw).unwrap_or_else(|_| raw.to_string()))
}

pub fn paren<'l, 'i: 'l>(
    _lexer: &'l L<'l, 'i>,
    span: Span,
    inner: Result<Expr, ()>,
) -> Result<Expr, ()> {
    Ok(Expr::Paren(ParenExpr {
        expr: Box::new(inner?),
        pos_range: to_pos_range(span),
    }))
}

// -------- unary / binary --------

/// Grammar-facing binary helper. The operator arrives as a captured
/// token (`$2` in the emitted grammar) rather than a constant, so a
/// single helper covers every operator alt.
///
/// Upstream `newBinaryExpression`, including its fill-modifier gate: the
/// error is reported at the whole expression's range and the node is
/// still built, as upstream returns `ret` after `addParseErrf`.
pub fn binary<'l, 'i: 'l>(
    lexer: &'l L<'l, 'i>,
    p: &ParserCtx,
    op_lx: Lx,
    modifiers: Result<Option<BinModifiers>, ()>,
    lhs: Result<Expr, ()>,
    rhs: Result<Expr, ()>,
) -> Result<Expr, ()> {
    let op = op_item_type_from_lex(lexer, op_lx)?;
    let (vector_matching, return_bool) = match modifiers? {
        Some(mods) => (Some(mods.vector_matching), mods.return_bool),
        None => (None, false),
    };
    let expr = Expr::Binary(BinaryExpr {
        op,
        lhs: Box::new(lhs?),
        rhs: Box::new(rhs?),
        vector_matching,
        return_bool,
    });
    if !p.options().enable_binop_fill_modifiers {
        if let Expr::Binary(BinaryExpr {
            vector_matching: Some(vm),
            ..
        }) = &expr
        {
            if vm.fill_values.lhs.is_some() || vm.fill_values.rhs.is_some() {
                p.add_gate_err(
                    crate::options::BINOP_FILL_MODIFIERS,
                    expr.position_range(),
                    "binop fill modifiers are experimental and not enabled",
                );
            }
        }
    }
    Ok(expr)
}

/// Grammar-facing unary helper. Same shape as [`binary`] — op comes in
/// as a captured token.
pub fn unary_from_token<'l, 'i: 'l>(
    lexer: &'l L<'l, 'i>,
    span: Span,
    op_ty: Result<ItemType, ()>,
    inner: Result<Expr, ()>,
) -> Result<Expr, ()> {
    let _ = lexer;
    let op = op_ty?;
    let e = inner?;
    let start_pos = span.start() as Pos;
    if let Expr::NumberLiteral(mut nl) = e {
        if op == ItemType::Sub {
            nl.val = -nl.val;
        }
        nl.pos_range.start = start_pos;
        return Ok(Expr::NumberLiteral(nl));
    }
    Ok(Expr::Unary(UnaryExpr {
        op,
        expr: Box::new(e),
        start_pos,
    }))
}

/// Map an operator token's source text back to its [`ItemType`]. Used
/// by [`binary`] to convert a captured `$2` token into the enum value.
/// Panics on an unknown operator — grmtools' grammar shape restricts
/// the input to the operators enumerated below.
fn op_item_type_from_lex<'l, 'i: 'l>(lexer: &'l L<'l, 'i>, lx: Lx) -> Result<ItemType, ()> {
    let raw = lexer.span_str(lx.span());
    let ty = match raw {
        "+" => ItemType::Add,
        "-" => ItemType::Sub,
        "*" => ItemType::Mul,
        "/" => ItemType::Div,
        "%" => ItemType::Mod,
        "^" => ItemType::Pow,
        "==" => ItemType::EqlC,
        "!=" => ItemType::Neq,
        "<" => ItemType::Lss,
        "<=" => ItemType::Lte,
        ">" => ItemType::Gtr,
        ">=" => ItemType::Gte,
        // Word operators are recovered from the source text, so they
        // have to fold case the way the lexer does.
        w if w.eq_ignore_ascii_case("and") => ItemType::Land,
        w if w.eq_ignore_ascii_case("or") => ItemType::Lor,
        w if w.eq_ignore_ascii_case("unless") => ItemType::Lunless,
        w if w.eq_ignore_ascii_case("atan2") => ItemType::Atan2,
        _ => return Err(()),
    };
    Ok(ty)
}

// -------- binary modifier helpers --------

/// Empty bool_modifier: no modifier, no allocation.
pub fn bool_modifier_empty() -> Result<Option<BinModifiers>, ()> {
    Ok(None)
}

/// Bool modifier: sets return_bool flag.
pub fn bool_modifier_bool() -> Result<Option<BinModifiers>, ()> {
    Ok(Some(BinModifiers {
        vector_matching: VectorMatching::default(),
        return_bool: true,
    }))
}

/// on_or_ignoring with IGNORING: takes bool_modifier and adds matching labels.
pub fn on_or_ignoring_ignoring(
    mods: Result<Option<BinModifiers>, ()>,
    labels: Result<Vec<String>, ()>,
) -> Result<Option<BinModifiers>, ()> {
    let mut m = mods?.unwrap_or_default();
    m.vector_matching.matching_labels = labels?;
    m.vector_matching.on = false;
    Ok(Some(m))
}

/// on_or_ignoring with ON: takes bool_modifier and adds matching labels + on flag.
pub fn on_or_ignoring_on(
    mods: Result<Option<BinModifiers>, ()>,
    labels: Result<Vec<String>, ()>,
) -> Result<Option<BinModifiers>, ()> {
    let mut m = mods?.unwrap_or_default();
    m.vector_matching.matching_labels = labels?;
    m.vector_matching.on = true;
    Ok(Some(m))
}

/// group_modifiers: pass through bool_modifier or on_or_ignoring unchanged.
pub fn group_modifiers_pass(
    mods: Result<Option<BinModifiers>, ()>,
) -> Result<Option<BinModifiers>, ()> {
    mods
}

/// group_modifiers with GROUP_LEFT: sets ManyToOne cardinality and include labels.
pub fn group_modifiers_left(
    mods: Result<Option<BinModifiers>, ()>,
    labels: Result<Vec<String>, ()>,
) -> Result<Option<BinModifiers>, ()> {
    let mut m = mods?.unwrap_or_default();
    m.vector_matching.card = VectorMatchCardinality::ManyToOne;
    m.vector_matching.include = labels?;
    Ok(Some(m))
}

/// group_modifiers with GROUP_RIGHT: sets OneToMany cardinality and include labels.
pub fn group_modifiers_right(
    mods: Result<Option<BinModifiers>, ()>,
    labels: Result<Vec<String>, ()>,
) -> Result<Option<BinModifiers>, ()> {
    let mut m = mods?.unwrap_or_default();
    m.vector_matching.card = VectorMatchCardinality::OneToMany;
    m.vector_matching.include = labels?;
    Ok(Some(m))
}

/// fill_modifiers: pass through group_modifiers unchanged.
pub fn fill_modifiers_pass(
    mods: Result<Option<BinModifiers>, ()>,
) -> Result<Option<BinModifiers>, ()> {
    mods
}

/// fill_modifiers with FILL: sets both LHS and RHS to the same fill value.
pub fn fill_modifiers_fill(
    mods: Result<Option<BinModifiers>, ()>,
    value: Result<f64, ()>,
) -> Result<Option<BinModifiers>, ()> {
    let mut m = mods?.unwrap_or_default();
    let v = value?;
    m.vector_matching.fill_values.lhs = Some(v);
    m.vector_matching.fill_values.rhs = Some(v);
    Ok(Some(m))
}

/// fill_modifiers with FILL_LEFT: sets only LHS fill value.
pub fn fill_modifiers_fill_left(
    mods: Result<Option<BinModifiers>, ()>,
    value: Result<f64, ()>,
) -> Result<Option<BinModifiers>, ()> {
    let mut m = mods?.unwrap_or_default();
    m.vector_matching.fill_values.lhs = Some(value?);
    Ok(Some(m))
}

/// fill_modifiers with FILL_RIGHT: sets only RHS fill value.
pub fn fill_modifiers_fill_right(
    mods: Result<Option<BinModifiers>, ()>,
    value: Result<f64, ()>,
) -> Result<Option<BinModifiers>, ()> {
    let mut m = mods?.unwrap_or_default();
    m.vector_matching.fill_values.rhs = Some(value?);
    Ok(Some(m))
}

/// fill_modifiers with FILL_LEFT then FILL_RIGHT: sets both fill values.
pub fn fill_modifiers_fill_left_right(
    mods: Result<Option<BinModifiers>, ()>,
    left_val: Result<f64, ()>,
    right_val: Result<f64, ()>,
) -> Result<Option<BinModifiers>, ()> {
    let mut m = mods?.unwrap_or_default();
    m.vector_matching.fill_values.lhs = Some(left_val?);
    m.vector_matching.fill_values.rhs = Some(right_val?);
    Ok(Some(m))
}

/// fill_modifiers with FILL_RIGHT then FILL_LEFT: sets both fill values.
pub fn fill_modifiers_fill_right_left(
    mods: Result<Option<BinModifiers>, ()>,
    right_val: Result<f64, ()>,
    left_val: Result<f64, ()>,
) -> Result<Option<BinModifiers>, ()> {
    let mut m = mods?.unwrap_or_default();
    m.vector_matching.fill_values.lhs = Some(left_val?);
    m.vector_matching.fill_values.rhs = Some(right_val?);
    Ok(Some(m))
}

/// Extract the numeric value from a fill_value production (number_duration_literal wrapped in parens).
pub fn fill_value_extract(expr: Result<Expr, ()>) -> Result<f64, ()> {
    match expr? {
        Expr::NumberLiteral(nl) => Ok(nl.val),
        _ => Err(()),
    }
}

/// Extract negated numeric value from a fill_value production with unary minus.
pub fn fill_value_unary(op: Result<ItemType, ()>, expr: Result<Expr, ()>) -> Result<f64, ()> {
    let op_ty = op?;
    match expr? {
        Expr::NumberLiteral(nl) => {
            if op_ty == ItemType::Sub {
                Ok(-nl.val)
            } else {
                Ok(nl.val)
            }
        }
        _ => Err(()),
    }
}

// -------- selectors --------

pub fn vector_selector<'l, 'i: 'l>(
    _lexer: &'l L<'l, 'i>,
    span: Span,
    metric_name: Option<String>,
    matchers: Option<Vec<LabelMatcher>>,
) -> Result<Expr, ()> {
    let name = metric_name.unwrap_or_default();
    let label_matchers = matchers.unwrap_or_default();
    Ok(Expr::VectorSelector(VectorSelector {
        name,
        original_offset_secs: 0.0,
        original_offset_expr: None,
        offset_secs: 0.0,
        timestamp: None,
        skip_histogram_buckets: false,
        start_or_end: None,
        label_matchers,
        bypass_empty_matcher_check: false,
        anchored: false,
        smoothed: false,
        pos_range: to_pos_range(span),
    }))
}

pub fn label_matcher<'l, 'i: 'l>(
    lexer: &'l L<'l, 'i>,
    span: Span,
    name_span: Span,
    op: MatchOp,
    value_lx: Lx,
) -> Result<LabelMatcher, ()> {
    let name = lexer.span_str(name_span).to_string();
    let raw_value = lexer.span_str(value_lx.span());
    let value = unquote_string(raw_value).unwrap_or_else(|_| raw_value.to_string());
    Ok(LabelMatcher {
        name,
        op,
        value,
        pos_range: to_pos_range(span),
    })
}

/// Variant of [`quoted_metric_name_matcher`] where the string has
/// already been unquoted (e.g. via `actions::string_literal_value`).
pub fn quoted_metric_name_matcher_from_ident<'l, 'i: 'l>(
    _lexer: &'l L<'l, 'i>,
    span: Span,
    value: String,
) -> Result<LabelMatcher, ()> {
    Ok(LabelMatcher {
        name: "__name__".to_string(),
        op: MatchOp::Equal,
        value,
        pos_range: to_pos_range(span),
    })
}

/// Matcher where the label *name* is a `string_identifier` (quoted
/// UTF-8 label) and the value is a `STRING` token. Upstream's
/// `label_matcher : string_identifier match_op STRING` form.
pub fn label_matcher_quoted_name<'l, 'i: 'l>(
    lexer: &'l L<'l, 'i>,
    span: Span,
    name: String,
    op: MatchOp,
    value_lx: Lx,
) -> Result<LabelMatcher, ()> {
    let raw = lexer.span_str(value_lx.span());
    let value = unquote_string(raw).unwrap_or_else(|_| raw.to_string());
    Ok(LabelMatcher {
        name,
        op,
        value,
        pos_range: to_pos_range(span),
    })
}

// -------- matrix / subquery / offset / @ --------

/// Variant of [`matrix_selector`] for the upstream-shaped grammar,
/// where the range arrives as a full Expr (produced by the
/// `positive_duration_expr → duration_expr → number_duration_literal`
/// chain) rather than a raw DURATION token. Currently only the literal
/// form is supported; its f64 value is extracted directly.
pub fn matrix_selector_from_expr<'l, 'i: 'l>(
    _lexer: &'l L<'l, 'i>,
    span: Span,
    selector: Result<Expr, ()>,
    range: Result<Expr, ()>,
) -> Result<Expr, ()> {
    let range_secs = duration_from_expr(&range?)?;
    Ok(Expr::MatrixSelector(MatrixSelector {
        vector_selector: Box::new(selector?),
        range_secs,
        range_expr: None,
        end_pos: span.end() as Pos,
    }))
}

/// Variant of [`subquery`] for the upstream-shaped grammar. Both the
/// range and the optional step are Exprs built by the duration
/// chain; each is currently expected to be a numeric literal.
pub fn subquery_from_exprs<'l, 'i: 'l>(
    _lexer: &'l L<'l, 'i>,
    span: Span,
    inner: Result<Expr, ()>,
    range: Result<Expr, ()>,
    step: Option<Result<Expr, ()>>,
) -> Result<Expr, ()> {
    let range_secs = duration_from_expr(&range?)?;
    let step_secs = match step {
        Some(r) => duration_from_expr(&r?)?,
        None => 0.0,
    };
    Ok(Expr::Subquery(SubqueryExpr {
        expr: Box::new(inner?),
        range_secs,
        range_expr: None,
        original_offset_secs: 0.0,
        original_offset_expr: None,
        offset_secs: 0.0,
        timestamp: None,
        start_or_end: None,
        step_secs,
        step_expr: None,
        end_pos: span.end() as Pos,
    }))
}

/// Variant of [`offset`] for the upstream-shaped grammar, where the
/// offset value flows through `offset_duration_expr → duration_expr`
/// rather than a raw DURATION token. Currently only the literal form
/// is supported; extracts the f64 and applies it to the subject.
pub fn offset_from_expr<'l, 'i: 'l>(
    _lexer: &'l L<'l, 'i>,
    inner: Result<Expr, ()>,
    offset: Result<Expr, ()>,
) -> Result<Expr, ()> {
    let offset_secs = duration_from_expr(&offset?)?;
    Ok(apply_offset(inner?, offset_secs))
}

/// Extract a seconds-valued f64 from an Expr whose shape is a
/// `NumberLiteral` (either a `DURATION` token or a numeric literal).
/// Other shapes — duration arithmetic, function calls — aren't
/// yet supported and produce `Err(())`.
fn duration_from_expr(e: &Expr) -> Result<f64, ()> {
    match e {
        Expr::NumberLiteral(nl) => Ok(nl.val),
        Expr::Paren(p) => duration_from_expr(&p.expr),
        _ => Err(()),
    }
}

fn apply_offset(mut e: Expr, offset_secs: f64) -> Expr {
    match &mut e {
        Expr::VectorSelector(vs) => {
            vs.original_offset_secs = offset_secs;
            vs.offset_secs = offset_secs;
        }
        Expr::MatrixSelector(ms) => {
            if let Expr::VectorSelector(vs) = ms.vector_selector.as_mut() {
                vs.original_offset_secs = offset_secs;
                vs.offset_secs = offset_secs;
            }
        }
        Expr::Subquery(sq) => {
            sq.original_offset_secs = offset_secs;
            sq.offset_secs = offset_secs;
        }
        _ => {}
    }
    e
}

/// NaN, ±Inf and out-of-range seconds are a parse error, not a clamped
/// timestamp (upstream's `setTimestamp` rejects them too).
fn apply_at_timestamp(mut e: Expr, secs: f64) -> Result<Expr, ()> {
    let ts_ms = promql_common::model::secs_to_millis(secs).ok_or(())?;
    match &mut e {
        Expr::VectorSelector(vs) => vs.timestamp = Some(ts_ms),
        Expr::MatrixSelector(ms) => {
            if let Expr::VectorSelector(vs) = ms.vector_selector.as_mut() {
                vs.timestamp = Some(ts_ms);
            }
        }
        Expr::Subquery(sq) => sq.timestamp = Some(ts_ms),
        _ => {}
    }
    Ok(e)
}

pub fn at_modifier<'l, 'i: 'l>(
    _lexer: &'l L<'l, 'i>,
    inner: Result<Expr, ()>,
    modifier: AtModifier,
) -> Result<Expr, ()> {
    let mut e = inner?;
    match &mut e {
        Expr::VectorSelector(vs) => vs.start_or_end = Some(modifier),
        Expr::MatrixSelector(ms) => {
            if let Expr::VectorSelector(vs) = ms.vector_selector.as_mut() {
                vs.start_or_end = Some(modifier);
            }
        }
        Expr::Subquery(sq) => sq.start_or_end = Some(modifier),
        _ => {}
    }
    Ok(e)
}

// -------- anchored / smoothed --------

/// `anchored_expr : expr ANCHORED` and `smoothed_expr : expr SMOOTHED`.
/// Both modifiers live on the `VectorSelector`, reached through the
/// matrix selector when there is one; upstream's `setAnchored` and
/// `setSmoothed` differ only in which field they set, and in the words
/// of their messages.
///
/// The gate comes first and leaves the expression untouched, as
/// upstream's early `return` does.
fn set_range_modifier(p: &ParserCtx, inner: Result<Expr, ()>, anchored: bool) -> Result<Expr, ()> {
    let mut e = inner?;
    let name = if anchored { "anchored" } else { "smoothed" };
    let range = e.position_range();
    if !p.options().enable_extended_range_selectors {
        p.add_gate_err(
            crate::options::EXTENDED_RANGE_SELECTORS,
            range,
            format!("{name} modifier is experimental and not enabled"),
        );
        return Ok(e);
    }
    let vs = match &mut e {
        Expr::VectorSelector(vs) => vs,
        Expr::MatrixSelector(ms) => match ms.vector_selector.as_mut() {
            Expr::VectorSelector(vs) => vs,
            _ => return Err(()),
        },
        Expr::Subquery(_) => {
            p.add_parse_err(
                range,
                format!("{name} modifier is not supported for subqueries"),
            );
            return Err(());
        }
        _ => {
            p.add_parse_err(range, format!("{name} modifier not implemented"));
            return Err(());
        }
    };
    if anchored {
        vs.anchored = true;
    } else {
        vs.smoothed = true;
    }
    // Upstream: "anchored and smoothed modifiers cannot be used
    // together". It sets the flag first and then complains, so the
    // check is on both being set, not on the one arriving second.
    if vs.anchored && vs.smoothed {
        p.add_parse_err(
            range,
            "anchored and smoothed modifiers cannot be used together",
        );
        return Err(());
    }
    Ok(e)
}

pub fn set_anchored(p: &ParserCtx, inner: Result<Expr, ()>) -> Result<Expr, ()> {
    set_range_modifier(p, inner, true)
}

pub fn set_smoothed(p: &ParserCtx, inner: Result<Expr, ()>) -> Result<Expr, ()> {
    set_range_modifier(p, inner, false)
}

/// Variant of [`at_timestamp`] for the upstream-shaped grammar, where
/// the `@ <number>` arrives as a pre-parsed f64 (from the
/// `signed_or_unsigned_number → number` rule chain).
pub fn at_timestamp_val<'l, 'i: 'l>(
    _lexer: &'l L<'l, 'i>,
    inner: Result<Expr, ()>,
    secs: f64,
) -> Result<Expr, ()> {
    apply_at_timestamp(inner?, secs)
}

/// Just the f64 value of a NUMBER token — used by upstream's `number`
/// rule. Different return shape from `number_literal` (which wraps in
/// an Expr).
pub fn number_value<'l, 'i: 'l>(lexer: &'l L<'l, 'i>, lx: Lx) -> Result<f64, ()> {
    let raw = lexer.span_str(lx.span());
    parse_number_literal(raw)
}

/// `unary_op number_duration_literal` — apply a +/- sign to a duration
/// Expr. Used by `offset_duration_expr` and `signed_number` chains.
pub fn signed_duration(op: Result<ItemType, ()>, inner: Result<Expr, ()>) -> Result<Expr, ()> {
    let e = inner?;
    let op = op?;
    if let Expr::NumberLiteral(mut nl) = e {
        if op == ItemType::Sub {
            nl.val = -nl.val;
        }
        return Ok(Expr::NumberLiteral(nl));
    }
    Ok(e)
}

// -------- function calls & aggregates --------

/// Upstream's `function_call` action, identifier arm.
pub fn function_call<'l, 'i: 'l>(
    lexer: &'l L<'l, 'i>,
    p: &ParserCtx,
    span: Span,
    name_lx: Lx,
    args: Vec<Expr>,
) -> Result<Expr, ()> {
    let name = lexer.span_str(name_lx.span()).to_string();
    let experimental = check_function_enabled(p, &name, to_pos_range(name_lx.span()));
    Ok(Expr::Call(Call {
        func: FunctionRef {
            name,
            arg_types: Vec::new(),
            return_type: ValueType::Vector,
            variadic: 0,
            experimental,
        },
        args,
        pos_range: to_pos_range(span),
    }))
}

/// The `fn.Experimental && !EnableExperimentalFunctions` check every
/// `function_call` arm repeats upstream. `range` is the function-name
/// token's, where upstream reports it. Returns whether `name` is
/// experimental.
fn check_function_enabled(p: &ParserCtx, name: &str, range: PositionRange) -> bool {
    let experimental = crate::functions::is_experimental(name);
    if experimental && !p.options().enable_experimental_functions {
        p.add_gate_err(
            crate::options::EXPERIMENTAL_FUNCTIONS,
            range,
            format!("function {name:?} is not enabled"),
        );
    }
    experimental
}

/// `function_call : at_modifier_preprocessors function_call_body`, i.e.
/// `start()` and `end()`. Gated; then rejected as before, since the arm
/// is not ported.
pub fn function_call_at_modifier(
    p: &ParserCtx,
    span: Span,
    modifier: Result<AtModifier, ()>,
) -> Result<Expr, ()> {
    let name = match modifier? {
        AtModifier::Start => "start",
        AtModifier::End => "end",
    };
    let start = span.start() as Pos;
    check_function_enabled(
        p,
        name,
        PositionRange::new(start, start + name.len() as Pos),
    );
    Err(())
}

/// `function_call : STEP function_call_body` and the `RANGE` twin. Gated;
/// then rejected as before, since the arms are not ported.
pub fn function_call_keyword<'l, 'i: 'l>(
    lexer: &'l L<'l, 'i>,
    p: &ParserCtx,
    lx: Lx,
) -> Result<Expr, ()> {
    // Upstream's `$1.Val`: the token as written.
    check_function_enabled(p, lexer.span_str(lx.span()), to_pos_range(lx.span()));
    Err(())
}

/// Upstream `newAggregateExpr`, including the experimental-aggregator
/// gate: `limitk` and `limit_ratio` follow `EnableExperimentalFunctions`.
/// Upstream reports it and returns the half-built node; this returns an
/// error, since nothing downstream can use that node.
pub fn aggregate<'l, 'i: 'l>(
    _lexer: &'l L<'l, 'i>,
    p: &ParserCtx,
    span: Span,
    op: ItemType,
    modifier: Option<(bool, Vec<String>)>,
    mut args: Vec<Expr>,
) -> Result<Expr, ()> {
    if !p.options().enable_experimental_functions
        && matches!(op, ItemType::Limitk | ItemType::LimitRatio)
    {
        p.add_gate_err(
            crate::options::EXPERIMENTAL_FUNCTIONS,
            to_pos_range(span),
            format!(
                "{op}() is experimental and must be enabled with \
                 --enable-feature=promql-experimental-functions"
            ),
        );
        return Err(());
    }
    let (without, grouping) = modifier.unwrap_or((false, Vec::new()));
    let (param, expr) = if aggregator_takes_param(op) && args.len() >= 2 {
        let p = args.remove(0);
        (Some(Box::new(p)), args.pop().expect("checked len"))
    } else {
        let e = args.pop().unwrap_or_else(|| {
            Expr::NumberLiteral(NumberLiteral {
                val: 0.0,
                duration: false,
                pos_range: to_pos_range(span),
            })
        });
        (None, e)
    };
    Ok(Expr::Aggregate(AggregateExpr {
        op,
        expr: Box::new(expr),
        param,
        grouping,
        without,
        pos_range: to_pos_range(span),
    }))
}

fn aggregator_takes_param(op: ItemType) -> bool {
    matches!(
        op,
        ItemType::Topk
            | ItemType::Bottomk
            | ItemType::Quantile
            | ItemType::CountValues
            | ItemType::Limitk
            | ItemType::LimitRatio
    )
}

// -------- start-symbol dispatch --------

/// Result of the `start` rule, one variant per parse mode. Upstream
/// stores this in `parser.generatedParserResult` as an `any` and type-
/// asserts per entry point (`parse.go`); grmtools needs a single
/// concrete return type, so the modes become an enum.
#[derive(Debug, Clone, PartialEq)]
pub enum ParseResult {
    /// `START_EXPRESSION` / `START_METRIC_SELECTOR`.
    Expr(Expr),
    /// `START_METRIC` — a bare label set.
    Metric(Vec<LabelMatcher>),
    /// `START_SERIES_DESCRIPTION`.
    SeriesDescription(SeriesDescription),
}

// -------- label sets (series descriptions & START_METRIC) --------

/// `metric : metric_identifier label_set`. Upstream sets `__name__` on
/// a `labels.Builder`; we prepend the equivalent `Equal` matcher.
///
/// Going through a Builder has an observable side effect that the bare
/// `metric : label_set` alternative does not share: `Builder.Reset`
/// treats an empty value as a deletion, so `foo{bar=""} 1` yields just
/// `__name__="foo"`. Dropping empties here — and only here — keeps us
/// byte-identical with upstream on the promqltest corpus.
pub fn metric_with_name(
    span: Span,
    name: String,
    mut labels: Vec<LabelMatcher>,
) -> Result<Vec<LabelMatcher>, ()> {
    labels.retain(|l| !l.value.is_empty());
    labels.insert(
        0,
        LabelMatcher {
            name: "__name__".to_string(),
            op: MatchOp::Equal,
            value: name,
            pos_range: to_pos_range(span),
        },
    );
    Ok(labels)
}

pub fn push_label(mut acc: Vec<LabelMatcher>, item: LabelMatcher) -> Result<Vec<LabelMatcher>, ()> {
    acc.push(item);
    Ok(acc)
}

/// `label_set_item : IDENTIFIER EQL STRING`. Unlike `label_matcher`,
/// a label set only ever has `=`.
pub fn label_from_ident<'l, 'i: 'l>(
    lexer: &'l L<'l, 'i>,
    span: Span,
    name: Result<Lx, Lx>,
    value: Result<Lx, Lx>,
) -> Result<LabelMatcher, ()> {
    let name = lexer.span_str(name.map_err(|_| ())?.span()).to_string();
    let raw = lexer.span_str(value.map_err(|_| ())?.span());
    Ok(LabelMatcher {
        name,
        op: MatchOp::Equal,
        value: unquote_string(raw)?,
        pos_range: to_pos_range(span),
    })
}

/// `label_set_item : string_identifier EQL STRING` — quoted UTF-8
/// label name.
pub fn label_from_string_ident<'l, 'i: 'l>(
    lexer: &'l L<'l, 'i>,
    span: Span,
    name: String,
    value: Result<Lx, Lx>,
) -> Result<LabelMatcher, ()> {
    let raw = lexer.span_str(value.map_err(|_| ())?.span());
    Ok(LabelMatcher {
        name,
        op: MatchOp::Equal,
        value: unquote_string(raw)?,
        pos_range: to_pos_range(span),
    })
}

/// `label_set_item : string_identifier` — a bare quoted string in a
/// label set is the metric name.
pub fn label_metric_name(span: Span, value: String) -> Result<LabelMatcher, ()> {
    Ok(LabelMatcher {
        name: "__name__".to_string(),
        op: MatchOp::Equal,
        value,
        pos_range: to_pos_range(span),
    })
}

// -------- series descriptions --------

/// Upstream's stale marker: `math.Float64frombits(value.StaleNaN)`,
/// a quiet NaN with a distinguishing low bit.
const STALE_NAN: u64 = 0x7ff0_0000_0000_0002;

pub fn series_description(
    labels: Vec<LabelMatcher>,
    values: Vec<SequenceValue>,
) -> Result<SeriesDescription, ()> {
    Ok(SeriesDescription { labels, values })
}

/// Upper bound on the points one series description may expand to.
///
/// [`MAX_REPEAT_COUNT`] bounds a *single* run, but `series_values` is
/// left-recursive and runs chain into one accumulator. This is exactly
/// what one maximal run expands to, so a single run is the whole budget,
/// still ~100x the largest line in upstream's promqltest corpus.
const MAX_DESCRIPTION_VALUES: usize = MAX_REPEAT_COUNT as usize + 1;

/// `series_values : series_values SPACE series_item`.
///
/// Left-recursive, so this is the one place that sees the whole
/// description and the accumulator's length *is* the running total, with
/// no extra state threaded through the grammar. Bounding here rather
/// than in [`series_description`] refuses before the allocation.
pub fn extend_values(
    mut acc: Vec<SequenceValue>,
    item: Vec<SequenceValue>,
) -> Result<Vec<SequenceValue>, ()> {
    if acc.len() + item.len() > MAX_DESCRIPTION_VALUES {
        return Err(());
    }
    acc.extend(item);
    Ok(acc)
}

/// `_ x<count>` — a run of gaps.
///
/// Note the asymmetry with [`series_repeat`]: upstream's `BLANK TIMES
/// uint` loop is `i < $3`, not `i <= $3`, so `_x5` yields exactly 5
/// omitted points while `1x5` yields 6.
///
/// `count` is bounded by [`MAX_REPEAT_COUNT`] in [`parse_uint`], the
/// only rule that produces it.
pub fn repeat_omitted(count: u64) -> Vec<SequenceValue> {
    vec![SequenceValue::omitted(); count as usize]
}

/// `<value>[<step>]x<count>` — an arithmetic run.
///
/// Upstream loops `for i := 0; i <= count; i++`, i.e. it emits
/// `count + 1` points: "Add an additional value for time 0, which we
/// ignore in tests". So `46.00+13.00x40` is 41 values, not 40.
///
/// `count` is bounded by [`MAX_REPEAT_COUNT`] in [`parse_uint`], the
/// only rule that produces it; that is what makes the `count + 1`
/// below safe from overflow.
pub fn series_repeat(start: f64, step: f64, count: u64) -> Vec<SequenceValue> {
    let mut out = Vec::with_capacity(count as usize + 1);
    let mut val = start;
    for _ in 0..=count {
        out.push(SequenceValue::value(val));
        val += step;
    }
    out
}

/// `series_value : IDENTIFIER` — the only identifier a value sequence
/// accepts is `stale`.
pub fn stale_value<'l, 'i: 'l>(lexer: &'l L<'l, 'i>, lx: Result<Lx, Lx>) -> Result<f64, ()> {
    let raw = lexer.span_str(lx.map_err(|_| ())?.span());
    if raw != "stale" {
        return Err(());
    }
    Ok(f64::from_bits(STALE_NAN))
}

/// Upper bound on an `x<count>` repetition.
///
/// Upstream has no explicit limit: Go would simply attempt the
/// allocation. Here the count is a `u64` taken straight from the input
/// text and [`series_repeat`] emits `count + 1` points, so an unbounded
/// value overflows `usize` on a 64-bit target (`metric 1x18446744073709551615`
/// panicked with "attempt to add with overflow" before this bound
/// existed) or asks for an allocation no test could want. promqltest's
/// own fixtures stay in the low thousands, so this is far above any
/// legitimate input while keeping a bad one a parse error.
///
/// One run only; chained runs are bounded by [`MAX_DESCRIPTION_VALUES`].
const MAX_REPEAT_COUNT: u64 = 1_000_000;

/// `uint : NUMBER` — the repetition count after `x`.
///
/// Rejects counts above [`MAX_REPEAT_COUNT`], which is what keeps
/// [`series_repeat`] and [`repeat_omitted`] from being handed a value
/// they cannot allocate for. Both reach their count through this rule,
/// so bounding it here covers both.
pub fn parse_uint<'l, 'i: 'l>(lexer: &'l L<'l, 'i>, lx: Result<Lx, Lx>) -> Result<u64, ()> {
    let count = lexer
        .span_str(lx.map_err(|_| ())?.span())
        .parse::<u64>()
        .map_err(|_| ())?;
    if count > MAX_REPEAT_COUNT {
        return Err(());
    }
    Ok(count)
}

// -------- shared parsers (number, duration, string) --------

fn parse_number_literal(raw: &str) -> Result<f64, ()> {
    if raw.eq_ignore_ascii_case("nan") {
        return Ok(f64::NAN);
    }
    if raw.eq_ignore_ascii_case("inf") || raw.eq_ignore_ascii_case("+inf") {
        return Ok(f64::INFINITY);
    }
    if raw.eq_ignore_ascii_case("-inf") {
        return Ok(f64::NEG_INFINITY);
    }
    let cleaned: String = raw.chars().filter(|&c| c != '_').collect();
    let s = cleaned.as_str();
    if let Some(hex) = s.strip_prefix("0x").or_else(|| s.strip_prefix("0X")) {
        return u64::from_str_radix(hex, 16)
            .map(|n| n as f64)
            .map_err(|_| ());
    }
    if let Some(oct) = s.strip_prefix("0o").or_else(|| s.strip_prefix("0O")) {
        return u64::from_str_radix(oct, 8)
            .map(|n| n as f64)
            .map_err(|_| ());
    }
    if let Some(bin) = s.strip_prefix("0b").or_else(|| s.strip_prefix("0B")) {
        return u64::from_str_radix(bin, 2)
            .map(|n| n as f64)
            .map_err(|_| ());
    }
    // Rust's parse saturates an overflowing literal to infinity where
    // Go's `ParseFloat` returns `ErrRange`; the spelled-out infinities
    // were handled above.
    s.parse::<f64>().ok().filter(|v| v.is_finite()).ok_or(())
}

/// Parse a PromQL duration literal (`30s`, `1h30m`, `500ms`) into
/// seconds. Delegates the grammar to `promql_common::model::Duration`,
/// the hand-port of upstream's `model.ParseDuration` — one duration
/// grammar for the whole workspace instead of a second, subtly different
/// one living here.
///
/// Public because promqltest scripts carry durations of their own, in
/// `load <interval>` directives and in eval ranges, and those are the
/// same literals. A second implementation would drift.
pub fn parse_duration_seconds(raw: &str) -> Result<f64, ()> {
    promql_common::model::Duration::parse(raw)
        .map(|d| d.as_secs_f64())
        .map_err(|_| ())
}

fn unquote_string(raw: &str) -> Result<String, ()> {
    if raw.len() < 2 {
        return Err(());
    }
    let bytes = raw.as_bytes();
    let open = bytes[0];
    let close = bytes[raw.len() - 1];
    if open != close {
        return Err(());
    }
    let inner = &raw[1..raw.len() - 1];
    if open == b'`' {
        return Ok(inner.to_string());
    }
    let mut out = String::with_capacity(inner.len());
    let mut it = inner.chars();
    while let Some(c) = it.next() {
        if c != '\\' {
            out.push(c);
            continue;
        }
        match it.next() {
            Some('n') => out.push('\n'),
            Some('t') => out.push('\t'),
            Some('r') => out.push('\r'),
            Some('\\') => out.push('\\'),
            Some('\'') => out.push('\''),
            Some('"') => out.push('"'),
            Some('`') => out.push('`'),
            Some('0') => out.push('\0'),
            Some('a') => out.push('\x07'),
            Some('b') => out.push('\x08'),
            Some('f') => out.push('\x0c'),
            Some('v') => out.push('\x0b'),
            Some(other) => {
                out.push('\\');
                out.push(other);
            }
            None => return Err(()),
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `parse_duration_seconds` delegates to `Duration::parse`, which
    /// stops at `ms` (see `promql_common::model::time`'s `unit_info_basic`
    /// doc comment) — the PromQL grammar has never had a sub-millisecond
    /// duration unit, so `rate(x[5us])` is rejected the same way upstream
    /// rejects it.
    #[test]
    fn duration_literal_rejects_sub_millisecond_unit() {
        assert!(parse_duration_seconds("5us").is_err());
    }
}
