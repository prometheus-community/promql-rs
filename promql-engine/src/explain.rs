//! A readable `Display` for a planned [`LogicalPlan`], and for the
//! `ExecutionPlan` it lowers to ([`render_physical`]), for pinning in
//! `promql-engine/tests/testdata/plans/`.
//!
//! `LogicalPlan::display_indent()` prints every node exactly, but this
//! engine's own scalar and aggregate functions carry their PromQL
//! parameters as a flat, untyped argument list —
//! `promql_range_function(samples, block_start, block_end, 'rate', 600000,
//! 1200000, 30000, 300000, 0, NULL)` — which is what DataFusion's UDFs
//! give it to work with, not what a reviewer diffing a plan pin wants to
//! read. This module
//! recognises the shapes this engine's own planner builds
//! ([`crate::selector`], [`crate::range`], [`crate::labels`],
//! [`crate::aggregate`], `get_field`, and a scan over a
//! [`SelectorTable`]) and prints them as `rate(samples[5m],
//! 600000..1200000 step 30s)` instead, reading like PromQL wherever
//! PromQL has syntax for it.
//!
//! # Fallthrough is what makes this safe to pin
//!
//! Nothing here changes what is planned or executed; it only chooses how
//! to print an already-built `LogicalPlan`. Recognising a shape is
//! therefore never load-bearing: any node kind this module does not
//! special-case renders as `LogicalPlan::display()` would, any expression
//! it does not recognise renders as `Expr`'s own `Display` would, and a
//! recognised call with the wrong argument count or a non-literal where a
//! literal is expected falls back the same way rather than guessing or
//! panicking. A planner change that adds a new function, a new argument
//! or a new node kind cannot make this module print something wrong; at
//! worst it prints the unrecognised shape verbatim, which is what
//! `display_indent()` always did.

use std::fmt;
use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{Column, ScalarValue};
use datafusion::datasource::source_as_provider;
use datafusion::logical_expr::expr::AggregateFunction;
use datafusion::logical_expr::{
    Aggregate, Expr, Filter, LogicalPlan, Projection, Sort, SortExpr, TableScan,
};
use datafusion::physical_expr::aggregate::AggregateFunctionExpr;
use datafusion::physical_expr::expressions::{Column as PhysicalColumn, Literal};
use datafusion::physical_expr::{PhysicalExpr, ScalarFunctionExpr};
use datafusion::physical_plan::aggregates::{
    aggregate_expressions, AggregateExec, AggregateInputMode, AggregateMode,
};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::{DisplayFormatType, ExecutionPlan, InputOrderMode};
use promql_common::model::Duration;
use promql_parser::ast::LabelMatcher;

use crate::matcher::METRIC_NAME;
use crate::source::{SelectHints, SelectorTable};
use crate::{absent, aggregate, labels, range, selector, series, to_scalar};

/// Renders `plan` the way `tests/testdata/plans/` pins it. See the module doc
/// comment for what "readable" means and why it is safe to pin.
pub fn render(plan: &LogicalPlan) -> String {
    let single_scan = count_table_scans(plan) == 1;
    let mut out = String::new();
    render_node(plan, 0, single_scan, &mut out);
    out
}

fn count_table_scans(plan: &LogicalPlan) -> usize {
    let here = usize::from(matches!(plan, LogicalPlan::TableScan(_)));
    here + plan
        .inputs()
        .iter()
        .map(|p| count_table_scans(p))
        .sum::<usize>()
}

fn render_node(plan: &LogicalPlan, indent: usize, single_scan: bool, out: &mut String) {
    if indent > 0 {
        out.push('\n');
    }
    out.push_str(&" ".repeat(indent * 2));
    out.push_str(&render_line(plan, single_scan));
    for input in plan.inputs() {
        render_node(input, indent + 1, single_scan, out);
    }
}

/// One node's own line, stock DataFusion's format string
/// (`Projection: a, b`, `Aggregate: groupBy=[[...]], aggr=[[...]]`,
/// `Filter: ...`, `Sort: ...`, `TableScan: name`) with only the
/// expressions inside re-rendered — except `TableScan` over a
/// [`SelectorTable`], whose line is its selector text, prefixed by the
/// table name only when the plan has several scans, so a qualified
/// column elsewhere has a name to point at.
fn render_line(plan: &LogicalPlan, single_scan: bool) -> String {
    match plan {
        LogicalPlan::Projection(p) => render_projection(p, single_scan),
        LogicalPlan::Filter(Filter { predicate, .. }) => {
            format!("Filter: {}", render_expr(predicate, single_scan))
        }
        LogicalPlan::Aggregate(a) => render_aggregate_node(a, single_scan).unwrap_or_else(|| {
            let group: Vec<_> = a
                .group_expr
                .iter()
                .map(|e| render_expr(e, single_scan))
                .collect();
            let aggr: Vec<_> = a
                .aggr_expr
                .iter()
                .map(|e| render_expr(e, single_scan))
                .collect();
            format!(
                "Aggregate: groupBy=[[{}]], aggr=[[{}]]",
                group.join(", "),
                aggr.join(", ")
            )
        }),
        LogicalPlan::Sort(Sort { expr, fetch, .. }) => {
            let mut s = String::from("Sort: ");
            for (i, e) in expr.iter().enumerate() {
                if i > 0 {
                    s.push_str(", ");
                }
                s.push_str(&render_sort_expr(e, single_scan));
            }
            if let Some(n) = fetch {
                s.push_str(&format!(", fetch={n}"));
            }
            s
        }
        LogicalPlan::TableScan(ts) => render_table_scan(ts, plan, single_scan),
        // Any other node kind (Join, Limit, Window, Distinct, …): stock
        // DataFusion text, unmodified. None of this engine's planner
        // output reaches them today, and recognising a new kind is opt-in
        // here, not required for this to stay correct.
        other => other.display().to_string(),
    }
}

/// Every series batch in this engine carries exactly four columns,
/// `labels, samples, block_start, block_end` in that order
/// (`series::LABELS`/`SAMPLES`/`BLOCK_START`/`BLOCK_END` are the schema's
/// own names for them, not this module's choice) — so a `Projection`
/// whose *output* schema is those four fields in that order is restating
/// an invariant the reader already knows, and its pass-through columns
/// and self-aliases are noise on top of it. This checks `plan.schema()`,
/// not the expression list itself: an expression can be aliased to
/// `labels` without being a bare `labels` column (a rebuilt struct, say),
/// and only the output name is what the guard cares about.
///
/// Anything else — a fifth column, the four out of order, missing one of
/// them, or a shape this schema check can't see — prints every
/// expression with its alias: that fallthrough is what keeps a
/// projection that breaks the four-column invariant visible in a pin
/// instead of silently going short-form.
fn render_projection(p: &Projection, single_scan: bool) -> String {
    if is_canonical_series_schema(&p.schema) {
        let parts: Vec<_> = p
            .expr
            .iter()
            .filter_map(|e| render_short_projection_expr(e, single_scan))
            .collect();
        // Every field was a pass-through: this is an identity projection,
        // and printing `Projection:` with nothing after it would hide
        // that from a reviewer reading the pin. Falling through to the
        // stock form below is what keeps it visible.
        if !parts.is_empty() {
            return format!("Projection: {}", parts.join(", "));
        }
    }
    let rendered: Vec<_> = p.expr.iter().map(|e| render_expr(e, single_scan)).collect();
    format!("Projection: {}", rendered.join(", "))
}

fn is_canonical_series_schema(schema: &datafusion::common::DFSchema) -> bool {
    let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    is_series_columns(&names)
}

/// The four columns of every series batch, in schema order.
const SERIES_COLUMNS: [&str; 4] = [
    series::LABELS,
    series::SAMPLES,
    series::BLOCK_START,
    series::BLOCK_END,
];

fn is_series_columns(names: &[&str]) -> bool {
    names == SERIES_COLUMNS
}

/// `None` for a pass-through reference to its own output name — a bare
/// `labels`/`samples`/`block_start`/`block_end` column, or one aliased
/// right back to the name it already has — since the schema already says
/// that's what it is. `Some` for anything else, rendered without the `AS
/// labels`/`AS samples`/… alias the four-column guard already established
/// as this expression's position in the schema.
fn render_short_projection_expr(e: &Expr, single_scan: bool) -> Option<String> {
    match e {
        Expr::Column(_) => None,
        Expr::Alias(a) => match a.expr.as_ref() {
            Expr::Column(c) if c.name == a.name => None,
            inner => Some(render_expr(inner, single_scan)),
        },
        other => Some(render_expr(other, single_scan)),
    }
}

fn render_sort_expr(e: &SortExpr, single_scan: bool) -> String {
    let mut s = render_expr(&e.expr, single_scan);
    s.push_str(if e.asc { " ASC" } else { " DESC" });
    s.push_str(if e.nulls_first {
        " NULLS FIRST"
    } else {
        " NULLS LAST"
    });
    s
}

/// A `TableScan` over a [`SelectorTable`] prints as its PromQL selector,
/// `http_requests_total{job="api"}`, in place of the table name; anything
/// else (a table this engine did not plan, or one carrying a pushed-down
/// projection/filter/fetch this module does not special-case) prints
/// exactly as `LogicalPlan::display()` would.
///
/// The scan's own name (`selector_0`) is only what a qualified column
/// elsewhere in the plan points back at — see [`render_column`] — so with
/// one scan in the whole plan it is dropped; with several it is kept as a
/// prefix so a reader can still match a `TableScan` to its qualifier.
fn render_table_scan(ts: &TableScan, plan: &LogicalPlan, single_scan: bool) -> String {
    if ts.projection.is_some() || !ts.filters.is_empty() || ts.fetch.is_some() {
        return plan.display().to_string();
    }
    let selector = source_as_provider(&ts.source).ok().and_then(|p| {
        // `TableProvider: Any`; trait-object upcasting (stable since
        // Rust 1.86) gets from `&dyn TableProvider` to `&dyn Any` without
        // a provider-side `as_any` accessor.
        let any: &dyn std::any::Any = p.as_ref();
        any.downcast_ref::<SelectorTable>()
            .map(|st| render_selector(st.name(), st.matchers()))
    });
    match selector {
        Some(selector) if single_scan => format!("TableScan: {selector}"),
        Some(selector) => format!("TableScan: {} {selector}", ts.table_name),
        None => format!("TableScan: {}", ts.table_name),
    }
}

/// Port of Go `VectorSelector.String()` (`promql/parser/printer.go`),
/// restricted to what a `TableScan` actually keeps: the name and the
/// matchers. Upstream's skip condition is on the matcher's own value
/// equalling `node.Name`, not on `__name__` equality alone — so
/// `{__name__="foo"}` (no bare metric name; `node.Name == ""`) keeps its
/// `__name__` matcher in braces instead of being folded into a bare
/// prefix, and two `__name__` matchers on the same selector both stay in
/// braces since at most one can equal the name. The offset/`@`/anchored/
/// smoothed suffixes upstream also prints have no matcher to hang off, so
/// they are left to whatever else pins a selector's modifiers.
fn render_selector(name: &str, matchers: &[LabelMatcher]) -> String {
    use promql_parser::ast::MatchOp;

    let mut rest = Vec::new();
    for m in matchers {
        if m.name == METRIC_NAME && m.op == MatchOp::Equal && m.value == name && !m.value.is_empty()
        {
            continue;
        }
        rest.push(m.to_string());
    }
    rest.sort();

    let mut out = name.to_string();
    if !rest.is_empty() {
        out.push('{');
        out.push_str(&rest.join(","));
        out.push('}');
    }
    out
}

/// What the call recognisers below read off an argument. The logical
/// `Expr` and the physical `PhysicalExpr` both implement it, so a call
/// prints the same in a logical pin and a physical one; a second copy of
/// the recognisers over `PhysicalExpr` would let the two drift apart.
trait Arg: Sized {
    /// The whole argument, re-rendered recursively. A physical column has
    /// no qualifier to drop, so `single_scan` only matters for `Expr`.
    fn render(&self, single_scan: bool) -> String;
    fn column_name(&self) -> Option<&str>;
    fn literal(&self) -> Option<&ScalarValue>;
    /// A scalar function call's name and arguments.
    fn call(&self) -> Option<(&str, &[Self])>;
}

impl Arg for Expr {
    fn render(&self, single_scan: bool) -> String {
        render_expr(self, single_scan)
    }

    fn column_name(&self) -> Option<&str> {
        match self {
            Expr::Column(c) => Some(&c.name),
            _ => None,
        }
    }

    fn literal(&self) -> Option<&ScalarValue> {
        match self {
            Expr::Literal(v, _) => Some(v),
            _ => None,
        }
    }

    fn call(&self) -> Option<(&str, &[Expr])> {
        match self {
            Expr::ScalarFunction(f) => Some((f.name(), &f.args)),
            _ => None,
        }
    }
}

/// What a selector asked its store for, as `SeriesSetExec` prints it: the
/// selector as its `TableScan` line spells it, then the query range and
/// every hint `select` was given, an advisory one only when present. The
/// store's own nodes below then read as its answer to exactly this.
pub(crate) fn render_selection(
    name: &str,
    matchers: &[LabelMatcher],
    hints: &SelectHints,
) -> String {
    let mut out = format!(
        "{} {}..{} window={}",
        render_selector(name, matchers),
        hints.start_ms,
        hints.end_ms,
        span(hints.window_ms)
    );
    if let Some(step) = hints.step_ms {
        out.push_str(&format!(" step={}", span(step)));
    }
    if let Some(range) = hints.range_ms {
        out.push_str(&format!(" range={}", span(range)));
    }
    if let Some(func) = &hints.func {
        out.push_str(&format!(" func={func}"));
    }
    if let Some(grouping) = &hints.grouping {
        let word = if grouping.by { "by" } else { "without" };
        out.push_str(&format!(" {word}=({})", grouping.labels.join(", ")));
    }
    if let Some(shard) = &hints.shard {
        out.push_str(&format!(" shard={}/{}", shard.index, shard.count));
    }
    out
}

/// A span in Go's `Duration` spelling, or in milliseconds where Go's
/// `Duration` cannot hold it.
fn span(ms: i64) -> String {
    Duration::from_millis(ms).map_or_else(|_| format!("{ms}ms"), |d| d.to_string())
}

/// One expression, recursively. Anything not recognised — a node this
/// engine never plans, or a recognised call whose shape does not match
/// (wrong argument count, a non-literal where a literal is required) —
/// falls back to `Expr`'s own `Display`, verbatim and un-recursed: that is
/// what makes an unrecognised shape safe rather than silently wrong.
fn render_expr(e: &Expr, single_scan: bool) -> String {
    match e {
        Expr::Alias(a) => format!("{} AS {}", render_expr(&a.expr, single_scan), a.name),
        Expr::Column(c) => render_column(c, single_scan),
        Expr::ScalarFunction(f) => {
            render_scalar_call(f.name(), &f.args, single_scan).unwrap_or_else(|| e.to_string())
        }
        Expr::AggregateFunction(f) => {
            render_aggregate_function(f, single_scan).unwrap_or_else(|| e.to_string())
        }
        _ => e.to_string(),
    }
}

/// Bare (`labels`) when the plan scans exactly one table, qualified
/// (`selector_0.labels`) otherwise — with two scans the qualifier is the
/// only thing telling the sides apart, so it cannot be dropped there.
fn render_column(c: &Column, single_scan: bool) -> String {
    if single_scan && c.relation.is_some() {
        c.name.clone()
    } else {
        c.to_string()
    }
}

fn render_scalar_call<A: Arg>(name: &str, args: &[A], single_scan: bool) -> Option<String> {
    match name {
        labels::NAME => render_labels(args, single_scan),
        "get_field" => render_get_field(args, single_scan),
        _ => None,
    }
}

fn render_aggregate_call<A: Arg>(name: &str, args: &[A], single_scan: bool) -> Option<String> {
    match name {
        selector::NAME => render_vector_selector(args, single_scan),
        range::NAME => render_range_function(args, single_scan),
        aggregate::NAME => render_aggregate_op(args, single_scan),
        absent::NAME => render_absent_call("absent", args, single_scan),
        to_scalar::NAME => render_absent_call("scalar", args, single_scan),
        _ => None,
    }
}

/// A bare column named `name`, any qualifier: what `block_start`/
/// `block_end` look like as a selector/range/`Aggregate`-node group-key
/// argument. `per block`/`per series per block` is what a reader is told
/// instead, so this is also the check that lets that phrase stand in for
/// the argument rather than silently dropping a different expression that
/// happens to sit in the same slot.
fn is_block_column<A: Arg>(e: &A, name: &str) -> bool {
    e.column_name() == Some(name)
}

/// `promql_vector_selector(samples, block_start, block_end, start, end,
/// step, lookback, offset, at, timestamp)` as `vector_selector(samples[
/// offset OFFSET][ @ N], START..END step STEP, lookback LOOKBACK[,
/// timestamp])` — PromQL has
/// no syntax for the evaluation range, so that stays a trailing argument
/// on every call, but `offset` and `@` are PromQL's own selector
/// modifiers and take PromQL's own order and spelling, attached to the
/// column they modify rather than tacked onto the end. The block
/// arguments never print: a caller only reaches this once it already
/// knows to say `per block` for them (see [`render_aggregate_node`]), and
/// requiring them to actually be the block columns is what keeps this
/// call from omitting some other expression that merely sits in that
/// slot.
///
/// The `@` value is `Params::at_ms`, the millisecond timestamp
/// `plan.rs`'s `resolve_at` already resolved it to, printed raw rather
/// than through `Duration`'s formatting: it names a point in time, not a
/// span.
fn render_vector_selector<A: Arg>(args: &[A], single_scan: bool) -> Option<String> {
    if args.len() != 10 {
        return None;
    }
    if !is_block_column(&args[1], series::BLOCK_START)
        || !is_block_column(&args[2], series::BLOCK_END)
    {
        return None;
    }
    let mut samples = args[0].render(single_scan);
    let start = require_i64(&args[3])?;
    let end = require_i64(&args[4])?;
    let step = require_i64(&args[5])?;
    let lookback = require_i64(&args[6])?;
    let offset = require_i64(&args[7])?;
    let at = optional_i64(&args[8])?;
    push_offset_and_at(&mut samples, offset, at)?;
    // Only the pick that is not the default prints: the value is what a
    // selector is for, and a plan that said so on every line would hide
    // the one that reads the time instead.
    let pick = match require_bool(&args[9])? {
        true => ", timestamp",
        false => "",
    };

    Some(format!(
        "vector_selector({samples}, {start}..{end} step {}, lookback {}{pick})",
        Duration::from_millis(step).ok()?,
        Duration::from_millis(lookback).ok()?
    ))
}

/// `promql_range_function(samples, block_start, block_end, '<func>',
/// start, end, step, range, offset, at)` as `<func>(samples[WINDOW][
/// offset OFFSET][ @ N], START..END step STEP)`. The window goes in
/// brackets after the column, like a PromQL range selector; see
/// [`render_vector_selector`] for why `offset`/`@` sit there too, why the
/// evaluation range stays a trailing argument, and why the block
/// arguments never print.
fn render_range_function<A: Arg>(args: &[A], single_scan: bool) -> Option<String> {
    if args.len() != 10 {
        return None;
    }
    if !is_block_column(&args[1], series::BLOCK_START)
        || !is_block_column(&args[2], series::BLOCK_END)
    {
        return None;
    }
    let samples = args[0].render(single_scan);
    let func = utf8_literal(&args[3])?;
    let start = require_i64(&args[4])?;
    let end = require_i64(&args[5])?;
    let step = require_i64(&args[6])?;
    let window = require_i64(&args[7])?;
    let offset = require_i64(&args[8])?;
    let at = optional_i64(&args[9])?;

    let mut column = format!("{samples}[{}]", Duration::from_millis(window).ok()?);
    push_offset_and_at(&mut column, offset, at)?;

    Some(format!(
        "{func}({column}, {start}..{end} step {})",
        Duration::from_millis(step).ok()?
    ))
}

/// Appends PromQL's own selector-modifier suffix — `offset` before `@`,
/// each only when present — directly onto the column text they modify.
/// `None` when `offset`'s millisecond count is out of Go's `Duration`
/// range, same fallthrough rule as every other recogniser here: the
/// caller falls back to DataFusion's own text for the whole expression
/// rather than rendering a partial line.
fn push_offset_and_at(s: &mut String, offset: i64, at: Option<i64>) -> Option<()> {
    if offset != 0 {
        s.push_str(&format!(" offset {}", Duration::from_millis(offset).ok()?));
    }
    if let Some(at) = at {
        s.push_str(&format!(" @ {at}"));
    }
    Some(())
}

/// `promql_labels('a', <a>, 'b', <b>, …)` as `labels{a, b}`, or `labels{a:
/// <expr>}` for a pair whose value is not `get_field(<labels column>,
/// 'a')` — the shape [`crate::labels::regroup`] produces for a group key
/// read back out of an `Aggregate`'s own output column instead of an
/// input struct.
///
/// The struct's own prefix word is not hardcoded to `"labels"`: a pair
/// eligible for the bare-key shorthand carries its `get_field` base's
/// own rendering (bare `labels`, or `selector_1.labels` with two scans),
/// and that becomes the prefix for the whole call. Two scans is exactly
/// where dropping the qualifier would make `job` from one scan
/// indistinguishable from `job` off another, so the shorthand only
/// applies once a base is established and every further shorthand-
/// eligible pair matches it; a pair whose base doesn't match — a
/// different scan, or not the labels column at all — falls back to
/// `name: <rendered value>` instead of being silently elided.
///
/// A long run of shorthand keys folds by [`fold_alike`]: a store with a
/// wide schema hands every series dozens of labels, and the struct that
/// rebuilds them would otherwise be one name per label.
fn render_labels<A: Arg>(args: &[A], single_scan: bool) -> Option<String> {
    if !args.len().is_multiple_of(2) {
        return None;
    }
    let mut prefix = None;
    let mut fields = Vec::with_capacity(args.len() / 2);
    for pair in args.chunks(2) {
        let name = utf8_literal(&pair[0])?;
        let text = render_label_field(name, &pair[1], single_scan, &mut prefix);
        // Every shorthand key has one shape; a `name: value` pair is only
        // ever alike with itself.
        let shape = if text == name {
            String::new()
        } else {
            text.clone()
        };
        fields.push(Entry {
            tail: text.clone(),
            text,
            shape,
        });
    }
    Some(format!(
        "{}{{{}}}",
        prefix.unwrap_or_else(|| series::LABELS.to_string()),
        fold_alike(fields, "labels").join(", ")
    ))
}

/// The base of a `get_field` call, rendered, but only when that base is
/// actually the canonical `labels` column — not just any struct — since
/// that is the one column the bare-key shorthand is entitled to elide.
fn labels_column_text<A: Arg>(base: &A, single_scan: bool) -> Option<String> {
    (base.column_name() == Some(series::LABELS)).then(|| base.render(single_scan))
}

fn render_label_field<A: Arg>(
    name: &str,
    value: &A,
    single_scan: bool,
    prefix: &mut Option<String>,
) -> String {
    if let Some(("get_field", [base, key])) = value.call() {
        if utf8_literal(key) == Some(name) {
            if let Some(base) = labels_column_text(base, single_scan) {
                match prefix {
                    Some(p) if *p == base => return name.to_string(),
                    None => {
                        *prefix = Some(base);
                        return name.to_string();
                    }
                    Some(_) => {} // a different base: not the same struct, don't elide
                }
            }
        }
    }
    format!("{name}: {}", value.render(single_scan))
}

/// `get_field(labels, 'job')` as `labels.job`.
fn render_get_field<A: Arg>(args: &[A], single_scan: bool) -> Option<String> {
    let [base, key] = args else { return None };
    let key = utf8_literal(key)?;
    if let Some(field) = field_of_rebuilt_labels(base, key, single_scan) {
        return Some(field);
    }
    Some(format!("{}.{key}", base.render(single_scan)))
}

/// `get_field(promql_labels(…, 'job', get_field(labels, 'job'), …), 'job')`
/// as `labels.job`. DataFusion builds this when it extracts a group key
/// through a projection that rebuilt the labels: every key then spells out
/// the whole struct, dozens of labels each on a wide store, to read one
/// field back.
///
/// Printing the field alone is exact only because the value is read
/// straight out of the canonical `labels` column: `promql_labels` then
/// has nothing to cast and no NULL to turn into `""`. Any other value, or
/// a key the struct names twice or not at all, prints in full.
fn field_of_rebuilt_labels<A: Arg>(base: &A, key: &str, single_scan: bool) -> Option<String> {
    let (labels::NAME, pairs) = base.call()? else {
        return None;
    };
    if !pairs.len().is_multiple_of(2) {
        return None;
    }
    let mut named = pairs
        .chunks(2)
        .filter(|pair| utf8_literal(&pair[0]) == Some(key));
    let ([_, value], None) = (named.next()?, named.next()) else {
        return None;
    };
    let Some(("get_field", [labels, inner_key])) = value.call() else {
        return None;
    };
    if utf8_literal(inner_key) != Some(key) {
        return None;
    }
    Some(format!(
        "{}.{key}",
        labels_column_text(labels, single_scan)?
    ))
}

/// The least number of consecutive alike entries that fold. A store that
/// gives each label its own column repeats one expression per label at
/// every node, 72 times over in a production plan; below this a list
/// stays whole, which keeps every pin's lists and the three pass-through
/// columns of a series projection, alike as they are, readable one by one.
const FOLD_MIN: usize = 8;

/// One entry of a printed list. `shape` is what two entries must share to
/// fold together, and `tail` is how the last entry of a folded run prints.
struct Entry {
    text: String,
    shape: String,
    tail: String,
}

/// Folds every run of at least [`FOLD_MIN`] consecutive entries with one
/// shape into its first entry, the last one's tail and the count:
/// `a@1 … z@71 (71 alike)`. The first entry prints whole, so a folded run
/// still shows what each entry computes; an entry of another
/// shape ends the run and prints on its own.
fn fold_alike(entries: Vec<Entry>, noun: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut rest = entries.as_slice();
    while let Some(first) = rest.first() {
        let run = rest.iter().take_while(|e| e.shape == first.shape).count();
        if run >= FOLD_MIN {
            out.push(format!(
                "{} … {} ({run} {noun})",
                first.text,
                rest[run - 1].tail
            ));
        } else {
            out.extend(rest[..run].iter().map(|e| e.text.clone()));
        }
        rest = &rest[run..];
    }
    out
}

/// Every aggregate function this engine plans — `promql_vector_selector`,
/// `promql_range_function`, `promql_aggregate` — takes only positional
/// literal/column arguments, so `DISTINCT`, a `FILTER` clause or an
/// `ORDER BY` are metadata a plan step of this engine's own never sets;
/// falling through when one is present, instead of ignoring it, is what
/// keeps a later change that starts setting them from being silently
/// misrendered.
fn render_aggregate_function(f: &AggregateFunction, single_scan: bool) -> Option<String> {
    let p = &f.params;
    if p.distinct || p.filter.is_some() || !p.order_by.is_empty() || p.null_treatment.is_some() {
        return None;
    }
    render_aggregate_call(f.func.name(), &p.args, single_scan)
}

/// `promql_absent(samples, start, end, step)` as
/// `absent(samples, START..END step STEP)`, and `promql_scalar`, which
/// takes the same arguments, as `scalar(...)`.
fn render_absent_call<A: Arg>(name: &str, args: &[A], single_scan: bool) -> Option<String> {
    if args.len() != 4 {
        return None;
    }
    let samples = args[0].render(single_scan);
    let start = require_i64(&args[1])?;
    let end = require_i64(&args[2])?;
    let step = require_i64(&args[3])?;
    Some(format!(
        "{name}({samples}, {start}..{end} step {})",
        Duration::from_millis(step).ok()?
    ))
}

/// `promql_aggregate(samples, '<op>', start, end, step)` as
/// `<op>(samples, START..END step STEP)`. No `topk`/`quantile` parameter:
/// `plan.rs`'s `aggregate` rejects an aggregation with a parameter as
/// unsupported before planning reaches here, so the call has exactly
/// these five positional arguments.
fn render_aggregate_op<A: Arg>(args: &[A], single_scan: bool) -> Option<String> {
    if args.len() != 5 {
        return None;
    }
    let samples = args[0].render(single_scan);
    let op = utf8_literal(&args[1])?;
    let start = require_i64(&args[2])?;
    let end = require_i64(&args[3])?;
    let step = require_i64(&args[4])?;
    Some(format!(
        "{op}({samples}, {start}..{end} step {})",
        Duration::from_millis(step).ok()?
    ))
}

/// An `Aggregate` node whose shape is exactly what `plan.rs` builds for a
/// selector, a range function or a PromQL aggregation over a block-and-
/// label-set grouping: one aggregate expression aliased `samples`, group
/// keys led by `block_start, block_end` (`plan.rs`'s `series_keys`/
/// `aggregate` both put the block first, so each group leaves when its
/// block does), then either the whole `labels` column over
/// `promql_vector_selector`/`promql_range_function` (`per series per
/// block`: each group is one series' chunks in one block) or zero or more
/// [`labels::group_exprs`] keys over `promql_aggregate` (`per block`: a
/// PromQL aggregate has no series left to speak of, only blocks),
/// PromQL's own `by`/`without` syntax (`without` never appears: `plan.rs`
/// already resolves it to the `by` keys that remain, per
/// [`labels::group_keys`]).
///
/// `None` on anything else — wrong key count or order, a key that isn't
/// the shape [`labels::group_alias`] or the bare `labels` column
/// establishes, a call whose own block arguments (see
/// [`render_vector_selector`]/[`render_range_function`]) aren't columns
/// named `block_start`/`block_end` — so the caller's stock
/// `groupBy=[[…]], aggr=[[…]]` line is what a shape this does not expect
/// falls back to, same as every other recogniser here. The planner always
/// passes the grouped block columns as these arguments, so a plain name
/// check is enough to tell a call built by this engine from one that
/// merely takes the same slots; there is nothing here to compare the
/// group's own qualifier against.
fn render_aggregate_node(agg: &Aggregate, single_scan: bool) -> Option<String> {
    if agg.aggr_expr.len() != 1 {
        return None;
    }
    let Expr::Alias(alias) = &agg.aggr_expr[0] else {
        return None;
    };
    if alias.name != series::SAMPLES {
        return None;
    }
    let Expr::AggregateFunction(f) = alias.expr.as_ref() else {
        return None;
    };
    let p = &f.params;
    if p.distinct || p.filter.is_some() || !p.order_by.is_empty() || p.null_treatment.is_some() {
        return None;
    }

    // `absent` and `scalar` are the aggregates with no key at all, the
    // block included: they have to answer when no row ever arrived, and a
    // block no series reached is never sent.
    if matches!(f.func.name(), absent::NAME | to_scalar::NAME) {
        if !agg.group_expr.is_empty() {
            return None;
        }
        let call = render_aggregate_call(f.func.name(), &p.args, single_scan)?;
        return Some(format!("Aggregate: {call} over the whole range"));
    }

    let [first, second, rest @ ..] = agg.group_expr.as_slice() else {
        return None;
    };
    if !is_block_column(first, series::BLOCK_START) || !is_block_column(second, series::BLOCK_END) {
        return None;
    }

    match f.func.name() {
        selector::NAME | range::NAME => {
            let [key] = rest else { return None };
            if !matches!(key, Expr::Column(c) if c.name == series::LABELS) {
                return None;
            }
            let call = if f.func.name() == selector::NAME {
                render_vector_selector(&p.args, single_scan)?
            } else {
                render_range_function(&p.args, single_scan)?
            };
            Some(format!("Aggregate: {call} per series per block"))
        }
        aggregate::NAME => {
            let args = &p.args;
            if args.len() != 5 {
                return None;
            }
            let samples = render_expr(&args[0], single_scan);
            let op = utf8_literal(&args[1])?;
            let start = require_i64(&args[2])?;
            let end = require_i64(&args[3])?;
            let step = require_i64(&args[4])?;

            let mut keys = Vec::with_capacity(rest.len());
            for e in rest {
                let Expr::Alias(a) = e else { return None };
                let Expr::ScalarFunction(gf) = a.expr.as_ref() else {
                    return None;
                };
                if gf.name() != "get_field" || gf.args.len() != 2 {
                    return None;
                }
                labels_column_text(&gf.args[0], single_scan)?;
                let key = utf8_literal(&gf.args[1])?;
                if a.name != labels::group_alias(key) {
                    return None;
                }
                keys.push(key);
            }

            let step_text = Duration::from_millis(step).ok()?;
            Some(if keys.is_empty() {
                format!("Aggregate: {op}({samples}, {start}..{end} step {step_text}) per block")
            } else {
                format!(
                    "Aggregate: {op} by ({}) ({samples}, {start}..{end} step {step_text}) per block",
                    keys.join(", ")
                )
            })
        }
        _ => None,
    }
}

/// A literal, non-`NULL` `Int64` — every timestamp/duration argument in
/// this engine's calls except the trailing `@` one, which is
/// `NULL`-shaped when absent (see [`optional_i64`]).
fn require_i64<A: Arg>(e: &A) -> Option<i64> {
    match e.literal()? {
        ScalarValue::Int64(Some(n)) => Some(*n),
        _ => None,
    }
}

/// A literal `Int64`, `NULL` meaning "absent" (the `@` modifier when the
/// query has none). `None` only when the argument is not even a literal
/// `Int64` at all, which is the actual fallthrough signal.
fn optional_i64<A: Arg>(e: &A) -> Option<Option<i64>> {
    match e.literal()? {
        ScalarValue::Int64(v) => Some(*v),
        _ => None,
    }
}

fn require_bool<A: Arg>(e: &A) -> Option<bool> {
    match e.literal()? {
        ScalarValue::Boolean(Some(b)) => Some(*b),
        _ => None,
    }
}

fn utf8_literal<A: Arg>(e: &A) -> Option<&str> {
    match e.literal()? {
        ScalarValue::Utf8(Some(s)) | ScalarValue::Utf8View(Some(s)) => Some(s.as_str()),
        _ => None,
    }
}

/// Renders an `ExecutionPlan` the way the `physical:` blocks in
/// `tests/testdata/plans/` pin it: one node per line, indented two spaces
/// per level, as `displayable(plan).indent(true)` lays it out.
///
/// Only the two node kinds that carry this engine's calls are re-rendered,
/// `ProjectionExec` and `AggregateExec`, wherever they sit, a store's own
/// included. Their calls read as in the logical pin, and every column
/// they print keeps its `@index`: in a physical plan the index is what is
/// evaluated and the name is only a label, so a name that drifted from
/// its index would otherwise be invisible. The one exception is a
/// selector or range call's block arguments, left out as in the logical
/// pin once their names say they are the block columns. Every other node
/// prints exactly as `indent(true)` prints it, by the same fallthrough
/// rule as the logical renderer.
pub fn render_physical(plan: &dyn ExecutionPlan) -> String {
    let mut out = String::new();
    render_exec_node(plan, 0, &mut out);
    out
}

fn render_exec_node(plan: &dyn ExecutionPlan, indent: usize, out: &mut String) {
    if indent > 0 {
        out.push('\n');
    }
    out.push_str(&" ".repeat(indent * 2));
    out.push_str(&render_exec_line(plan));
    for child in plan.children() {
        render_exec_node(child.as_ref(), indent + 1, out);
    }
}

fn render_exec_line(plan: &dyn ExecutionPlan) -> String {
    if let Some(p) = plan.downcast_ref::<ProjectionExec>() {
        return render_projection_exec(p);
    }
    if let Some(line) = plan
        .downcast_ref::<AggregateExec>()
        .and_then(render_aggregate_exec)
    {
        return line;
    }
    Stock(plan).to_string()
}

/// A node's own line exactly as `displayable(plan).indent(true)` prints
/// it, which is `fmt_as` with `Verbose`.
struct Stock<'a>(&'a dyn ExecutionPlan);

impl fmt::Display for Stock<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt_as(DisplayFormatType::Verbose, f)
    }
}

/// Like the logical [`render_projection`], the four series columns' own
/// aliases go only when the output is exactly those four in order. A
/// projection that adds, drops or reorders one keeps them, since nothing
/// else would then say which output a call such as `{}` fills.
fn render_projection_exec(p: &ProjectionExec) -> String {
    let names: Vec<&str> = p.expr().iter().map(|e| e.alias.as_str()).collect();
    let series_columns = is_series_columns(&names);
    let exprs = p
        .expr()
        .iter()
        .map(|e| output_entry(&e.expr, &e.alias, series_columns))
        .collect();
    format!(
        "ProjectionExec: expr=[{}]",
        fold_alike(exprs, "alike").join(", ")
    )
}

/// `expr as alias`, without the alias where it says nothing: the
/// expression is a column of that very name, or, with
/// `drop_series_aliases`, the alias is one of the four series columns,
/// which no query can rename.
///
/// A series alias on another column stays: `block_end@3 as block_start`
/// renames one series column to another, which position cannot tell.
fn output_entry(e: &Arc<dyn PhysicalExpr>, alias: &str, drop_series_aliases: bool) -> Entry {
    let text = e.render(false);
    let same_column = e.column_name() == Some(alias);
    let series_alias = drop_series_aliases && is_series_column(alias) && e.column_name().is_none();
    if same_column || series_alias {
        Entry {
            shape: shape_of(e, None),
            tail: text.clone(),
            text,
        }
    } else {
        Entry {
            text: format!("{text} as {alias}"),
            shape: shape_of(e, Some(alias)),
            tail: format!("as {alias}"),
        }
    }
}

/// `e` printed with each name it reads, a column or a struct field
/// `get_field` takes, and then its `alias`, replaced by a placeholder
/// numbered by first appearance: one string, one placeholder. Two
/// entries share a shape when they compute the same thing over different
/// names *and* use those names the same way, so `labels@0.l3 as l3` and
/// `labels@0.l4 as l4` fold together while `labels@0.l3 as l4` breaks
/// the run, and so does a `CASE` that reads its two columns swapped.
/// Masking every name to one blank would fold all three alike.
///
/// What it cannot see is a pairing between names that differ by design,
/// such as `__datafusion_extracted_1@3 as __group__a`; a run of those
/// folds even when one pair in its middle is wrong.
fn shape_of(e: &Arc<dyn PhysicalExpr>, alias: Option<&str>) -> String {
    let mut names: Vec<String> = Vec::new();
    let mut placeholder = |name: &str| {
        let n = names
            .iter()
            .position(|seen| seen == name)
            .unwrap_or_else(|| {
                names.push(name.to_string());
                names.len() - 1
            });
        format!("${n}")
    };
    let masked = Arc::clone(e).transform(|node| {
        if let Some(name) = node.column_name() {
            let renamed: Arc<dyn PhysicalExpr> =
                Arc::new(PhysicalColumn::new(&placeholder(name), 0));
            return Ok(Transformed::yes(renamed));
        }
        if let Some(("get_field", [base, key])) = node.call() {
            if let Some(key) = utf8_literal(key) {
                let renamed: Arc<dyn PhysicalExpr> =
                    Arc::new(Literal::new(ScalarValue::Utf8(Some(placeholder(key)))));
                let base = Arc::clone(base);
                return node
                    .with_new_children(vec![base, renamed])
                    .map(Transformed::yes);
            }
        }
        Ok(Transformed::no(node))
    });
    let mut shape = masked.map_or_else(|_| e.render(false), |masked| masked.data.render(false));
    if let Some(alias) = alias {
        shape.push_str(&format!(" as {}", placeholder(alias)));
    }
    shape
}

fn is_series_column(name: &str) -> bool {
    SERIES_COLUMNS.contains(&name)
}

/// The stock `AggregateExec` line with its expressions re-rendered. A call
/// aliased to a series column drops the alias with no order guard, unlike
/// a projection: `plan.rs` only aliases a call to the column it fills.
/// A store's own aggregates below `SeriesSetExec` render by the same
/// rules, so a store that aliases otherwise reads as if it did not.
///
/// The aggregate's arguments are the ones the node evaluates, from
/// DataFusion's own [`aggregate_expressions`]. Stock DataFusion prints
/// the aggregate's logical name instead, which shows neither index and,
/// in a final mode, names the raw samples where the node reads the
/// partial state.
///
/// `None`, and so the stock line, for what this does not print: grouping
/// sets and a limit.
fn render_aggregate_exec(a: &AggregateExec) -> Option<String> {
    let group = a.group_expr();
    if !group.is_single() || a.limit_options().is_some() {
        return None;
    }
    let reads = aggregate_expressions(a.aggr_expr(), a.mode(), group.expr().len()).ok()?;
    let gby = group
        .expr()
        .iter()
        .map(|(e, alias)| output_entry(e, alias, true))
        .collect();
    let gby = fold_alike(gby, "alike");
    let aggr: Vec<_> = a
        .aggr_expr()
        .iter()
        .zip(&reads)
        .map(|(agg, reads)| {
            render_aggregate_expr(agg, a.mode(), reads).unwrap_or_else(|| stock_aggregate_expr(agg))
        })
        .collect();
    let mut line = format!(
        "AggregateExec: mode={:?}, gby=[{}], aggr=[{}]",
        a.mode(),
        gby.join(", "),
        aggr.join(", ")
    );
    if *a.input_order_mode() != InputOrderMode::Linear {
        line.push_str(&format!(", ordering_mode={:?}", a.input_order_mode()));
    }
    Some(line)
}

/// One of this engine's aggregate calls over the columns `reads`.
///
/// In a mode whose input is partial state, the node reads the state
/// column where the raw call took `samples`, and that column goes in the
/// call's first argument. Only when the call's other arguments are all
/// literals, which they are for `promql_aggregate`: the selector and range
/// functions also take the block columns, and with those swapped for
/// state there is no call left to print truthfully.
fn render_aggregate_expr(
    agg: &AggregateFunctionExpr,
    mode: &AggregateMode,
    reads: &[Arc<dyn PhysicalExpr>],
) -> Option<String> {
    if agg.is_distinct() || agg.ignore_nulls() || !agg.order_bys().is_empty() {
        return None;
    }
    let args = match mode.input_mode() {
        AggregateInputMode::Raw => reads.to_vec(),
        AggregateInputMode::Partial => {
            let [state] = reads else { return None };
            let mut args = agg.expressions();
            let (first, params) = args.split_first_mut()?;
            if params.iter().any(|p| p.literal().is_none()) {
                return None;
            }
            *first = Arc::clone(state);
            args
        }
    };
    let call = render_aggregate_call(agg.fun().name(), &args, false)?;
    let name = agg.name();
    Some(if is_series_column(name) {
        call
    } else {
        format!("{call} as {name}")
    })
}

/// DataFusion's own text for an aggregate expression, which its private
/// `format_aggregate_exec_expr` builds.
fn stock_aggregate_expr(agg: &AggregateFunctionExpr) -> String {
    match (agg.human_display_alias(), agg.human_display()) {
        (Some(alias), Some(shown)) => format!("{shown} as {alias}"),
        _ => agg.name().to_string(),
    }
}

impl Arg for Arc<dyn PhysicalExpr> {
    fn render(&self, _single_scan: bool) -> String {
        self.call()
            .and_then(|(name, args)| render_scalar_call(name, args, false))
            .unwrap_or_else(|| self.to_string())
    }

    fn column_name(&self) -> Option<&str> {
        self.downcast_ref::<PhysicalColumn>().map(|c| c.name())
    }

    fn literal(&self) -> Option<&ScalarValue> {
        self.downcast_ref::<Literal>().map(|l| l.value())
    }

    fn call(&self) -> Option<(&str, &[Self])> {
        self.downcast_ref::<ScalarFunctionExpr>()
            .map(|f| (f.name(), f.args()))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::datasource::provider_as_source;
    use datafusion::logical_expr::{col, lit, LogicalPlanBuilder, LogicalTableSource};
    use promql_parser::ast::MatchOp;
    use promql_parser::posrange::PositionRange;

    use super::*;
    use crate::params::Params;
    use crate::series::SAMPLES;

    fn matcher(name: &str, op: MatchOp, value: &str) -> LabelMatcher {
        LabelMatcher {
            name: name.to_string(),
            op,
            value: value.to_string(),
            pos_range: PositionRange::default(),
        }
    }

    fn params(offset_ms: i64, at_ms: Option<i64>) -> Params {
        Params {
            start_ms: 600_000,
            end_ms: 1_200_000,
            step_ms: 30_000,
            window_ms: 300_000,
            offset_ms,
            at_ms,
        }
    }

    #[test]
    fn duration_renders_like_go_string() {
        assert_eq!(Duration::from_millis(300_000).unwrap().to_string(), "5m");
        assert_eq!(Duration::from_millis(90_000).unwrap().to_string(), "1m30s");
        assert_eq!(Duration::from_millis(0).unwrap().to_string(), "0s");
    }

    #[test]
    fn vector_selector_with_neither_offset_nor_at() {
        let call = selector::call(
            col("samples"),
            col(series::BLOCK_START),
            col(series::BLOCK_END),
            &params(0, None),
            selector::Pick::Value,
        );
        assert_eq!(
            render_expr(&call, true),
            "vector_selector(samples, 600000..1200000 step 30s, lookback 5m)"
        );
    }

    #[test]
    fn vector_selector_with_offset_and_at() {
        let call = selector::call(
            col("samples"),
            col(series::BLOCK_START),
            col(series::BLOCK_END),
            &params(3_600_000, Some(900_000)),
            selector::Pick::Value,
        );
        assert_eq!(
            render_expr(&call, true),
            "vector_selector(samples offset 1h @ 900000, 600000..1200000 step 30s, lookback 5m)"
        );
    }

    /// The pick prints only when it is not the value, so every existing
    /// selector line stays as it was.
    #[test]
    fn vector_selector_names_the_timestamp_pick() {
        let call = selector::call(
            col("samples"),
            col(series::BLOCK_START),
            col(series::BLOCK_END),
            &params(0, None),
            selector::Pick::Timestamp,
        );
        assert_eq!(
            render_expr(&call, true),
            "vector_selector(samples, 600000..1200000 step 30s, lookback 5m, timestamp)"
        );
    }

    #[test]
    fn range_function_renders_its_name_and_window() {
        let call = range::call(
            col("samples"),
            col(series::BLOCK_START),
            col(series::BLOCK_END),
            range::Func::Rate,
            &params(0, None),
        );
        assert_eq!(
            render_expr(&call, true),
            "rate(samples[5m], 600000..1200000 step 30s)"
        );
    }

    #[test]
    fn range_function_with_offset_and_at() {
        let call = range::call(
            col("samples"),
            col(series::BLOCK_START),
            col(series::BLOCK_END),
            range::Func::Rate,
            &params(3_600_000, Some(900_000)),
        );
        assert_eq!(
            render_expr(&call, true),
            "rate(samples[5m] offset 1h @ 900000, 600000..1200000 step 30s)"
        );
    }

    /// A step outside Go's ±(1<<63−1)-nanosecond range (`i64::MAX`
    /// milliseconds overflows `i64` nanoseconds) makes `Duration::from_millis`
    /// return `Err`; `render_range_function` propagates that as `None` via
    /// `?`, and `render_expr` falls through to DataFusion's own rendering
    /// of the call, same as any other shape this module doesn't recognize.
    #[test]
    fn range_function_with_unrenderable_step_falls_through_to_datafusion() {
        let mut p = params(0, None);
        p.step_ms = i64::MAX;
        let call = range::call(
            col("samples"),
            col(series::BLOCK_START),
            col(series::BLOCK_END),
            range::Func::Rate,
            &p,
        );
        assert_eq!(render_expr(&call, true), call.to_string());
    }

    #[test]
    fn aggregate_renders_its_op() {
        let call = aggregate::call(
            col("samples"),
            aggregate::Op::Sum,
            600_000,
            1_200_000,
            30_000,
        );
        assert_eq!(
            render_expr(&call, true),
            "sum(samples, 600000..1200000 step 30s)"
        );
    }

    /// An `Aggregate` node shaped exactly as `plan.rs`'s `aggregate`
    /// builds it, over a two-label source, for `keys` group columns led
    /// by the block.
    fn aggregate_node(keys: &[String]) -> LogicalPlan {
        let source = Arc::new(LogicalTableSource::new(crate::series::schema(&[
            "job".to_string(),
            "pod".to_string(),
        ])));
        let mut group = vec![col(series::BLOCK_START), col(series::BLOCK_END)];
        group.extend(labels::group_exprs(keys));
        LogicalPlanBuilder::scan("selector_0", source, None)
            .unwrap()
            .aggregate(
                group,
                vec![
                    aggregate::call(col(SAMPLES), aggregate::Op::Sum, 600_000, 1_200_000, 30_000)
                        .alias(SAMPLES),
                ],
            )
            .unwrap()
            .build()
            .unwrap()
    }

    #[test]
    fn aggregate_node_by_one_key_reads_like_promql() {
        let plan = aggregate_node(&["job".to_string()]);
        assert_eq!(
            render_line(&plan, true),
            "Aggregate: sum by (job) (samples, 600000..1200000 step 30s) per block"
        );
    }

    #[test]
    fn aggregate_node_with_no_group_keys_omits_the_by_clause() {
        let plan = aggregate_node(&[]);
        assert_eq!(
            render_line(&plan, true),
            "Aggregate: sum(samples, 600000..1200000 step 30s) per block"
        );
    }

    #[test]
    fn aggregate_node_falls_through_when_the_alias_is_not_samples() {
        let source = Arc::new(LogicalTableSource::new(crate::series::schema(&[
            "job".to_string()
        ])));
        let keys = ["job".to_string()];
        let group = [
            vec![col(series::BLOCK_START), col(series::BLOCK_END)],
            labels::group_exprs(&keys),
        ]
        .concat();
        let plan = LogicalPlanBuilder::scan("selector_0", source, None)
            .unwrap()
            .aggregate(
                group,
                vec![
                    aggregate::call(col(SAMPLES), aggregate::Op::Sum, 600_000, 1_200_000, 30_000)
                        .alias("other"),
                ],
            )
            .unwrap()
            .build()
            .unwrap();
        assert_eq!(
            render_line(&plan, true),
            "Aggregate: groupBy=[[block_start, block_end, labels.job AS __group__job]], \
             aggr=[[sum(samples, 600000..1200000 step 30s) AS other]]"
        );
    }

    #[test]
    fn aggregate_node_falls_through_when_a_key_alias_does_not_match_group_alias() {
        let source = Arc::new(LogicalTableSource::new(crate::series::schema(&[
            "job".to_string()
        ])));
        let mismatched_group =
            datafusion::functions::core::expr_fn::get_field(col(series::LABELS), "job")
                .alias("not_a_group_alias");
        let plan = LogicalPlanBuilder::scan("selector_0", source, None)
            .unwrap()
            .aggregate(
                vec![
                    col(series::BLOCK_START),
                    col(series::BLOCK_END),
                    mismatched_group,
                ],
                vec![
                    aggregate::call(col(SAMPLES), aggregate::Op::Sum, 600_000, 1_200_000, 30_000)
                        .alias(SAMPLES),
                ],
            )
            .unwrap()
            .build()
            .unwrap();
        assert_eq!(
            render_line(&plan, true),
            "Aggregate: groupBy=[[block_start, block_end, labels.job AS not_a_group_alias]], \
             aggr=[[sum(samples, 600000..1200000 step 30s) AS samples]]"
        );
    }

    #[test]
    fn aggregate_node_falls_through_when_the_first_two_keys_are_not_the_block_columns() {
        let source = Arc::new(LogicalTableSource::new(crate::series::schema(&[
            "job".to_string(),
            "pod".to_string(),
        ])));
        let keys = ["job".to_string(), "pod".to_string()];
        let plan = LogicalPlanBuilder::scan("selector_0", source, None)
            .unwrap()
            .aggregate(
                labels::group_exprs(&keys),
                vec![
                    aggregate::call(col(SAMPLES), aggregate::Op::Sum, 600_000, 1_200_000, 30_000)
                        .alias(SAMPLES),
                ],
            )
            .unwrap()
            .build()
            .unwrap();
        assert_eq!(
            render_line(&plan, true),
            "Aggregate: groupBy=[[labels.job AS __group__job, labels.pod AS __group__pod]], \
             aggr=[[sum(samples, 600000..1200000 step 30s) AS samples]]"
        );
    }

    /// A selector or range aggregate is shaped exactly as `plan.rs` builds
    /// it: grouped by the block and the whole `labels` column.
    fn selector_series_group() -> Vec<Expr> {
        vec![
            col(series::BLOCK_START),
            col(series::BLOCK_END),
            col(series::LABELS),
        ]
    }

    #[test]
    fn selector_aggregate_node_with_offset_and_at_reads_like_promql() {
        let source = Arc::new(LogicalTableSource::new(crate::series::schema(&[
            "job".to_string()
        ])));
        let plan = LogicalPlanBuilder::scan("selector_0", source, None)
            .unwrap()
            .aggregate(
                selector_series_group(),
                vec![selector::call(
                    col(SAMPLES),
                    col(series::BLOCK_START),
                    col(series::BLOCK_END),
                    &params(3_600_000, Some(900_000)),
                    selector::Pick::Value,
                )
                .alias(SAMPLES)],
            )
            .unwrap()
            .build()
            .unwrap();
        assert_eq!(
            render_line(&plan, true),
            "Aggregate: vector_selector(samples offset 1h @ 900000, 600000..1200000 step 30s, \
             lookback 5m) per series per block"
        );
    }

    #[test]
    fn range_aggregate_node_with_offset_and_at_reads_like_promql() {
        let source = Arc::new(LogicalTableSource::new(crate::series::schema(&[
            "job".to_string()
        ])));
        let plan = LogicalPlanBuilder::scan("selector_0", source, None)
            .unwrap()
            .aggregate(
                selector_series_group(),
                vec![range::call(
                    col(SAMPLES),
                    col(series::BLOCK_START),
                    col(series::BLOCK_END),
                    range::Func::Rate,
                    &params(3_600_000, Some(900_000)),
                )
                .alias(SAMPLES)],
            )
            .unwrap()
            .build()
            .unwrap();
        assert_eq!(
            render_line(&plan, true),
            "Aggregate: rate(samples[5m] offset 1h @ 900000, 600000..1200000 step 30s) \
             per series per block"
        );
    }

    #[test]
    fn selector_aggregate_node_falls_through_when_the_third_key_is_not_labels() {
        let source = Arc::new(LogicalTableSource::new(crate::series::schema(&[
            "job".to_string()
        ])));
        let group = vec![
            col(series::BLOCK_START),
            col(series::BLOCK_END),
            get_field_of("job").alias(labels::group_alias("job")),
        ];
        let plan = LogicalPlanBuilder::scan("selector_0", source, None)
            .unwrap()
            .aggregate(
                group,
                vec![selector::call(
                    col(SAMPLES),
                    col(series::BLOCK_START),
                    col(series::BLOCK_END),
                    &params(0, None),
                    selector::Pick::Value,
                )
                .alias(SAMPLES)],
            )
            .unwrap()
            .build()
            .unwrap();
        assert_eq!(
            render_line(&plan, true),
            "Aggregate: groupBy=[[block_start, block_end, labels.job AS __group__job]], \
             aggr=[[vector_selector(samples, 600000..1200000 step 30s, lookback 5m) AS samples]]"
        );
    }

    #[test]
    fn selector_aggregate_node_falls_through_when_the_calls_block_arguments_are_wrong() {
        let source = Arc::new(LogicalTableSource::new(crate::series::schema(&[
            "job".to_string()
        ])));
        // The call's own block arguments are the two block columns
        // swapped: the group key check alone cannot see this, so the
        // call itself has to reject it.
        let call = selector::udaf().call(vec![
            col(SAMPLES),
            col(series::BLOCK_END),
            col(series::BLOCK_START),
            lit(600_000i64),
            lit(1_200_000i64),
            lit(30_000i64),
            lit(300_000i64),
            lit(0i64),
            lit(ScalarValue::Int64(None)),
            lit(false),
        ]);
        let plan = LogicalPlanBuilder::scan("selector_0", source, None)
            .unwrap()
            .aggregate(selector_series_group(), vec![call.alias(SAMPLES)])
            .unwrap()
            .build()
            .unwrap();
        // The scan qualifies every column once the plan resolves it, so
        // the expected text is built off the plan's own aggregate
        // expression rather than the pre-resolution `call` above.
        let LogicalPlan::Aggregate(a) = &plan else {
            panic!("expected an Aggregate node")
        };
        let resolved = a.aggr_expr[0].to_string();
        assert_eq!(
            render_line(&plan, true),
            format!("Aggregate: groupBy=[[block_start, block_end, labels]], aggr=[[{resolved}]]")
        );
    }

    #[test]
    fn labels_keeps_matching_get_field_pairs() {
        let call = labels::call(vec![
            ("job".to_string(), get_field_of("job")),
            ("pod".to_string(), get_field_of("pod")),
        ]);
        assert_eq!(render_expr(&call, true), "labels{job, pod}");
    }

    #[test]
    fn labels_regroups_a_non_get_field_value() {
        let call = labels::call(vec![("job".to_string(), col("__group__job"))]);
        assert_eq!(render_expr(&call, true), "labels{job: __group__job}");
    }

    #[test]
    fn labels_keeps_the_qualifier_when_the_base_is_a_qualified_labels_column() {
        let qualified = Expr::Column(Column::new(Some("selector_1"), "labels"));
        let call = labels::call(vec![(
            "job".to_string(),
            datafusion::functions::core::expr_fn::get_field(qualified, "job"),
        )]);
        assert_eq!(render_expr(&call, false), "selector_1.labels{job}");
    }

    #[test]
    fn labels_does_not_elide_a_get_field_from_a_non_labels_struct() {
        let call = labels::call(vec![(
            "job".to_string(),
            datafusion::functions::core::expr_fn::get_field(col("other"), "job"),
        )]);
        assert_eq!(render_expr(&call, true), "labels{job: other.job}");
    }

    fn get_field_of(key: &str) -> Expr {
        datafusion::functions::core::expr_fn::get_field(col("labels"), key)
    }

    #[test]
    fn get_field_renders_as_dot_notation() {
        let call = get_field_of("job");
        assert_eq!(render_expr(&call, true), "labels.job");
    }

    /// Builds a `Projection` with an explicit output schema rather than
    /// one DataFusion derives from `exprs` against a real input: the
    /// shorthand guard reads `plan.schema()`, and this lets a test pick
    /// that schema directly without also having to fabricate an input
    /// plan whose columns the exprs would otherwise need to resolve
    /// against.
    fn projection_with_schema(exprs: Vec<Expr>, field_names: &[&str]) -> Projection {
        let source = Arc::new(LogicalTableSource::new(crate::series::schema(&[
            "job".to_string()
        ])));
        let input = Arc::new(
            LogicalPlanBuilder::scan("selector_0", source, None)
                .unwrap()
                .build()
                .unwrap(),
        );
        let fields: Vec<datafusion::arrow::datatypes::Field> = field_names
            .iter()
            .map(|n| {
                datafusion::arrow::datatypes::Field::new(
                    *n,
                    datafusion::arrow::datatypes::DataType::Utf8,
                    false,
                )
            })
            .collect();
        let schema = Arc::new(
            datafusion::common::DFSchema::try_from(datafusion::arrow::datatypes::Schema::new(
                fields,
            ))
            .unwrap(),
        );
        Projection::try_new_with_schema(exprs, input, schema).unwrap()
    }

    /// The two block columns, as they'd sit at the end of a canonical
    /// four-column projection: pass-throughs, so a test only cares about
    /// them when it means to break that invariant.
    fn block_pass_throughs() -> Vec<Expr> {
        vec![col(series::BLOCK_START), col(series::BLOCK_END)]
    }

    const CANONICAL_SCHEMA: [&str; 4] = [
        series::LABELS,
        series::SAMPLES,
        series::BLOCK_START,
        series::BLOCK_END,
    ];

    #[test]
    fn projection_short_form_drops_a_regrouped_labels_alias() {
        let mut exprs = vec![
            labels::call(vec![("job".to_string(), col("__group__job"))]).alias(series::LABELS),
            col(SAMPLES),
        ];
        exprs.extend(block_pass_throughs());
        let p = projection_with_schema(exprs, &CANONICAL_SCHEMA);
        assert_eq!(
            render_projection(&p, true),
            "Projection: labels{job: __group__job}"
        );
    }

    #[test]
    fn projection_short_form_drops_both_aliases_when_both_are_rebuilt() {
        let mut exprs = vec![
            labels::call(vec![
                ("job".to_string(), get_field_of("job")),
                ("pod".to_string(), get_field_of("pod")),
            ])
            .alias(series::LABELS),
            range::call(
                col(SAMPLES),
                col(series::BLOCK_START),
                col(series::BLOCK_END),
                range::Func::Rate,
                &params(0, None),
            )
            .alias(series::SAMPLES),
        ];
        exprs.extend(block_pass_throughs());
        let p = projection_with_schema(exprs, &CANONICAL_SCHEMA);
        assert_eq!(
            render_projection(&p, true),
            "Projection: labels{job, pod}, rate(samples[5m], 600000..1200000 step 30s)"
        );
    }

    #[test]
    fn projection_short_form_drops_a_pass_through_labels_column() {
        let mut exprs = vec![
            col(series::LABELS),
            selector::call(
                col(SAMPLES),
                col(series::BLOCK_START),
                col(series::BLOCK_END),
                &params(0, None),
                selector::Pick::Value,
            )
            .alias(series::SAMPLES),
        ];
        exprs.extend(block_pass_throughs());
        let p = projection_with_schema(exprs, &CANONICAL_SCHEMA);
        assert_eq!(
            render_projection(&p, true),
            "Projection: vector_selector(samples, 600000..1200000 step 30s, lookback 5m)"
        );
    }

    #[test]
    fn projection_keeps_aliases_with_a_fifth_column() {
        let mut exprs = vec![
            labels::call(vec![("job".to_string(), get_field_of("job"))]).alias(series::LABELS),
            range::call(
                col(SAMPLES),
                col(series::BLOCK_START),
                col(series::BLOCK_END),
                range::Func::Rate,
                &params(0, None),
            )
            .alias(series::SAMPLES),
        ];
        exprs.extend(block_pass_throughs());
        exprs.push(col("extra"));
        let mut field_names = CANONICAL_SCHEMA.to_vec();
        field_names.push("extra");
        let p = projection_with_schema(exprs, &field_names);
        assert_eq!(
            render_projection(&p, true),
            "Projection: labels{job} AS labels, rate(samples[5m], 600000..1200000 step 30s) AS samples, \
             block_start, block_end, extra"
        );
    }

    #[test]
    fn projection_keeps_aliases_with_the_columns_reordered() {
        let p = projection_with_schema(
            vec![
                col(SAMPLES),
                labels::call(vec![("job".to_string(), get_field_of("job"))]).alias(series::LABELS),
                col(series::BLOCK_START),
                col(series::BLOCK_END),
            ],
            &[
                series::SAMPLES,
                series::LABELS,
                series::BLOCK_START,
                series::BLOCK_END,
            ],
        );
        assert_eq!(
            render_projection(&p, true),
            "Projection: samples, labels{job} AS labels, block_start, block_end"
        );
    }

    #[test]
    fn projection_keeps_aliases_with_only_labels() {
        let p = projection_with_schema(
            vec![labels::call(vec![("job".to_string(), get_field_of("job"))]).alias(series::LABELS)],
            &[series::LABELS],
        );
        assert_eq!(
            render_projection(&p, true),
            "Projection: labels{job} AS labels"
        );
    }

    #[test]
    fn projection_identity_prints_the_stock_form() {
        let mut exprs = vec![col(series::LABELS), col(series::SAMPLES)];
        exprs.extend(block_pass_throughs());
        let p = projection_with_schema(exprs, &CANONICAL_SCHEMA);
        assert_eq!(
            render_projection(&p, true),
            "Projection: labels, samples, block_start, block_end"
        );
    }

    #[test]
    fn column_is_bare_with_one_scan_qualified_with_two() {
        let qualified = Expr::Column(Column::new(Some("selector_0"), "labels"));
        assert_eq!(render_expr(&qualified, true), "labels");
        assert_eq!(render_expr(&qualified, false), "selector_0.labels");
    }

    #[test]
    fn table_scan_over_a_selector_table_prints_its_matchers() {
        use crate::source::{SelectHints, SeriesSource};
        use datafusion::catalog::Session;
        use datafusion::error::Result as DfResult;
        use datafusion::physical_plan::{empty::EmptyExec, ExecutionPlan};

        #[derive(Debug)]
        struct Empty;

        #[async_trait::async_trait]
        impl SeriesSource for Empty {
            async fn select(
                &self,
                _state: &dyn Session,
                _matchers: &[LabelMatcher],
                _hints: SelectHints,
            ) -> DfResult<Arc<dyn ExecutionPlan>> {
                Ok(Arc::new(EmptyExec::new(crate::series::schema(&[
                    "job".to_string()
                ]))))
            }
        }

        let matchers = vec![
            matcher(METRIC_NAME, MatchOp::Equal, "http_requests_total"),
            matcher("job", MatchOp::Equal, "api"),
        ];
        let ctx = datafusion::execution::context::SessionContext::new();
        let table = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(SelectorTable::try_new(
                &ctx.state(),
                &Empty,
                "http_requests_total",
                &matchers,
                SelectHints::range(0, 1),
            ))
            .unwrap();

        let builder =
            LogicalPlanBuilder::scan("selector_0", provider_as_source(Arc::new(table)), None)
                .unwrap();
        let plan = builder.build().unwrap();
        // One scan in the whole plan: the name is dropped, since nothing
        // needs to point back at it.
        assert_eq!(render(&plan), "TableScan: http_requests_total{job=\"api\"}");
        assert_eq!(
            render_line(&plan, false),
            "TableScan: selector_0 http_requests_total{job=\"api\"}"
        );
    }

    #[test]
    fn fallthrough_on_an_unrecognised_scalar_function() {
        let call = datafusion::functions::expr_fn::upper(lit("x"));
        assert_eq!(render_expr(&call, true), call.to_string());
    }

    #[test]
    fn fallthrough_on_a_udf_with_the_wrong_argument_count() {
        // Well short of `promql_vector_selector`'s nine arguments.
        let call = selector::udaf().call(vec![col("samples"), lit(0i64)]);
        assert_eq!(render_expr(&call, true), call.to_string());
    }

    #[test]
    fn selector_folds_a_name_matcher_that_matches_the_selector_into_the_bare_prefix() {
        let matchers = vec![
            matcher(METRIC_NAME, MatchOp::Equal, "http_requests_total"),
            matcher("job", MatchOp::Equal, "api"),
        ];
        assert_eq!(
            render_selector("http_requests_total", &matchers),
            "http_requests_total{job=\"api\"}"
        );
    }

    #[test]
    fn selector_keeps_a_name_matcher_that_does_not_match_the_selector_in_braces() {
        // `{__name__="foo"}`: no bare metric name, so `node.Name` is `""`
        // and the matcher's value never equals it.
        let matchers = vec![matcher(METRIC_NAME, MatchOp::Equal, "foo")];
        assert_eq!(render_selector("", &matchers), "{__name__=\"foo\"}");
    }

    #[test]
    fn selector_keeps_a_second_name_matcher_in_braces() {
        // Only the matcher whose value equals the selector's own name is
        // folded; a second `__name__` matcher is never that, however it
        // reads, so it stays in braces rather than being dropped.
        let matchers = vec![
            matcher(METRIC_NAME, MatchOp::Equal, "a"),
            matcher(METRIC_NAME, MatchOp::Equal, "b"),
        ];
        assert_eq!(render_selector("a", &matchers), "a{__name__=\"b\"}");
    }

    fn label_names(names: &[&str]) -> Vec<String> {
        names.iter().map(|n| n.to_string()).collect()
    }

    /// `e` lowered against a series batch carrying `names` as its labels,
    /// so its columns get the indices a real plan would give them.
    fn physical(e: Expr, names: &[&str]) -> Arc<dyn PhysicalExpr> {
        let schema = series::schema(&label_names(names));
        let schema = datafusion::common::DFSchema::try_from(schema.as_ref().clone()).unwrap();
        datafusion::physical_expr::create_physical_expr(
            &e,
            &schema,
            &datafusion::execution::context::ExecutionProps::new(),
        )
        .unwrap()
    }

    fn field_of(names: &[&str], key: &str) -> Expr {
        let pairs = names
            .iter()
            .map(|n| (n.to_string(), get_field_of(n)))
            .collect();
        datafusion::functions::core::expr_fn::get_field(labels::call(pairs), key)
    }

    #[test]
    fn physical_columns_keep_their_index() {
        let e = physical(col(series::BLOCK_END), &["pod"]);
        assert_eq!(e.render(false), "block_end@3");
    }

    #[test]
    fn physical_get_field_of_rebuilt_labels_reads_the_field() {
        let e = physical(field_of(&["i", "pod"], "pod"), &["i", "pod"]);
        assert_eq!(e.render(false), "labels@0.pod");
    }

    #[test]
    fn logical_get_field_of_rebuilt_labels_reads_the_field() {
        assert_eq!(
            render_expr(&field_of(&["i", "pod"], "pod"), true),
            "labels.pod"
        );
    }

    #[test]
    fn get_field_of_rebuilt_labels_keeps_a_value_it_would_canonicalise() {
        let call = labels::call(vec![("pod".to_string(), lit("a"))]);
        let e = datafusion::functions::core::expr_fn::get_field(call, "pod");
        assert_eq!(render_expr(&e, true), "labels{pod: Utf8(\"a\")}.pod");
    }

    #[test]
    fn get_field_of_rebuilt_labels_keeps_a_key_named_twice() {
        let e = field_of(&["pod", "pod"], "pod");
        assert_eq!(render_expr(&e, true), "labels{pod, pod}.pod");
    }

    #[test]
    fn get_field_of_rebuilt_labels_keeps_a_key_it_does_not_name() {
        let e = field_of(&["i"], "pod");
        assert_eq!(render_expr(&e, true), "labels{i}.pod");
    }

    fn many(n: usize) -> Vec<String> {
        (0..n).map(|i| format!("l{i}")).collect()
    }

    #[test]
    fn labels_fold_a_long_run_of_keys() {
        let names = many(FOLD_MIN);
        let names: Vec<&str> = names.iter().map(String::as_str).collect();
        let pairs = names
            .iter()
            .map(|n| (n.to_string(), get_field_of(n)))
            .collect();
        assert_eq!(
            render_expr(&labels::call(pairs), true),
            format!("labels{{l0 … l{} ({FOLD_MIN} labels)}}", FOLD_MIN - 1)
        );
    }

    #[test]
    fn labels_keep_a_short_run_of_keys() {
        let names = many(FOLD_MIN - 1);
        let pairs = names
            .iter()
            .map(|n| (n.to_string(), get_field_of(n)))
            .collect();
        assert_eq!(
            render_expr(&labels::call(pairs), true),
            format!("labels{{{}}}", names.join(", "))
        );
    }

    fn entry(text: &str, shape: &str) -> Entry {
        Entry {
            text: text.to_string(),
            shape: shape.to_string(),
            tail: format!("tail of {text}"),
        }
    }

    #[test]
    fn fold_alike_breaks_a_run_at_another_shape() {
        let mut entries: Vec<_> = (0..=FOLD_MIN)
            .map(|i| entry(&format!("a{i}"), "a"))
            .collect();
        entries.insert(1, entry("odd", "b"));
        entries.push(entry("last", "c"));
        let folded = fold_alike(entries, "alike");
        assert_eq!(
            folded,
            [
                "a0".to_string(),
                "odd".to_string(),
                format!("a1 … tail of a{FOLD_MIN} ({FOLD_MIN} alike)"),
                "last".to_string(),
            ]
        );
    }

    #[test]
    fn fold_alike_folds_a_run_only_from_fold_min() {
        let entries = |n: usize| (0..n).map(|i| entry(&format!("a{i}"), "a")).collect();
        assert_eq!(
            fold_alike(entries(FOLD_MIN - 1), "alike").len(),
            FOLD_MIN - 1
        );
        assert_eq!(fold_alike(entries(FOLD_MIN), "alike").len(), 1);
    }

    #[test]
    fn shape_ignores_which_columns_an_expression_reads_but_not_what_it_does() {
        use datafusion::arrow::datatypes::DataType;
        let cast = |name: &str, to: DataType| {
            let e = Expr::Cast(datafusion::logical_expr::Cast::new(
                Box::new(get_field_of(name)),
                to,
            ));
            shape_of(&physical(e, &["a", "b"]), None)
        };
        assert_eq!(cast("a", DataType::Utf8), cast("b", DataType::Utf8));
        assert_ne!(
            shape_of(&physical(get_field_of("a"), &["a"]), None),
            shape_of(&physical(col(series::LABELS), &["a"]), None)
        );
        assert_ne!(cast("a", DataType::Utf8), cast("a", DataType::LargeUtf8));
        assert_ne!(
            shape_of(&physical(col(series::SAMPLES), &["a"]), None),
            shape_of(&physical(lit("a"), &["a"]), None)
        );
    }

    fn projection(exprs: Vec<(Expr, &str)>, names: &[&str]) -> String {
        use datafusion::physical_plan::empty::EmptyExec;
        let input = Arc::new(EmptyExec::new(series::schema(&label_names(names))));
        let exprs: Vec<(Arc<dyn PhysicalExpr>, String)> = exprs
            .into_iter()
            .map(|(e, alias)| (physical(e, names), alias.to_string()))
            .collect();
        render_physical(&ProjectionExec::try_new(exprs, input).unwrap())
            .lines()
            .next()
            .unwrap()
            .to_string()
    }

    #[test]
    fn physical_projection_drops_the_series_aliases_in_series_order() {
        let shown = projection(
            vec![
                (labels::call(vec![]), series::LABELS),
                (col(series::SAMPLES), series::SAMPLES),
                (col(series::BLOCK_START), series::BLOCK_START),
                (col(series::BLOCK_END), series::BLOCK_END),
            ],
            &[],
        );
        assert_eq!(
            shown,
            "ProjectionExec: expr=[labels{}, samples@1, block_start@2, block_end@3]"
        );
    }

    #[test]
    fn physical_projection_keeps_a_series_alias_out_of_series_order() {
        let shown = projection(
            vec![
                (col(series::SAMPLES), series::SAMPLES),
                (labels::call(vec![]), series::LABELS),
            ],
            &[],
        );
        assert_eq!(
            shown,
            "ProjectionExec: expr=[samples@1, labels{} as labels]"
        );
    }

    #[test]
    fn physical_projection_keeps_a_rename() {
        let shown = projection(vec![(get_field_of("pod"), "__group__pod")], &["pod"]);
        assert_eq!(shown, "ProjectionExec: expr=[labels@0.pod as __group__pod]");
    }

    #[test]
    fn physical_projection_folds_a_long_run_of_alike_outputs() {
        let names = many(FOLD_MIN);
        let names: Vec<&str> = names.iter().map(String::as_str).collect();
        let mut exprs: Vec<(Expr, &str)> = names.iter().map(|n| (get_field_of(n), *n)).collect();
        exprs.push((col(series::SAMPLES), series::SAMPLES));
        let last = names[FOLD_MIN - 1];
        assert_eq!(
            projection(exprs, &names),
            format!(
                "ProjectionExec: expr=[labels@0.l0 as l0 … as {last} ({FOLD_MIN} alike), samples@1]"
            )
        );
    }

    #[test]
    fn physical_nodes_this_does_not_know_print_as_datafusion_does() {
        use datafusion::physical_plan::empty::EmptyExec;
        use datafusion::physical_plan::{displayable, ExecutionPlan};
        let plan: Arc<dyn ExecutionPlan> =
            Arc::new(EmptyExec::new(series::schema(&label_names(&["pod"]))));
        assert_eq!(
            render_physical(plan.as_ref()),
            displayable(plan.as_ref())
                .indent(true)
                .to_string()
                .trim_end()
        );
    }

    fn up() -> Vec<LabelMatcher> {
        vec![
            matcher(METRIC_NAME, MatchOp::Equal, "up"),
            matcher("job", MatchOp::Equal, "api"),
        ]
    }

    #[test]
    fn selection_prints_only_the_hints_it_was_given() {
        let hints = SelectHints::range(300_000, 1_200_000).with_window_ms(300_000);
        assert_eq!(
            render_selection("up", &up(), &hints),
            r#"up{job="api"} 300000..1200000 window=5m"#
        );
    }

    #[test]
    fn selection_prints_every_hint_it_was_given() {
        use crate::source::{Grouping, Shard};
        let hints = SelectHints::range(300_000, 1_200_000)
            .with_window_ms(300_000)
            .with_step_ms(30_000)
            .with_range_ms(300_000)
            .with_func("rate")
            .with_grouping(Grouping::new(vec!["job".into(), "pod".into()], true))
            .with_shard(Shard::new(1, 4));
        assert_eq!(
            render_selection("up", &up(), &hints),
            r#"up{job="api"} 300000..1200000 window=5m step=30s range=5m func=rate by=(job, pod) shard=1/4"#
        );
    }

    #[test]
    fn selection_spells_without() {
        use crate::source::Grouping;
        let hints =
            SelectHints::range(0, 1).with_grouping(Grouping::new(vec!["pod".into()], false));
        assert_eq!(
            render_selection("up", &up(), &hints),
            r#"up{job="api"} 0..1 window=0s without=(pod)"#
        );
    }

    #[test]
    fn a_run_breaks_where_an_entry_uses_its_names_differently() {
        let names = many(FOLD_MIN + 1);
        let names: Vec<&str> = names.iter().map(String::as_str).collect();
        // The fourth output reads `l3` but calls it `l4`.
        let exprs: Vec<(Expr, &str)> = names
            .iter()
            .enumerate()
            .map(|(i, n)| (get_field_of(n), if i == 3 { names[4] } else { *n }))
            .collect();
        let shown = projection(exprs, &names);
        assert!(
            shown.contains("labels@0.l3 as l4"),
            "the odd entry must print whole: {shown}"
        );
    }

    #[test]
    fn shape_tells_swapped_columns_apart() {
        let case = |first: &str, second: &str| {
            let e = Expr::Case(datafusion::logical_expr::Case::new(
                None,
                vec![(
                    Box::new(get_field_of(first).is_null()),
                    Box::new(get_field_of(second)),
                )],
                Some(Box::new(get_field_of(first))),
            ));
            shape_of(&physical(e, &["a", "b", "c", "d"]), None)
        };
        assert_eq!(case("a", "b"), case("c", "d"));
        assert_ne!(case("a", "b"), case("a", "a"));
    }

    #[test]
    fn physical_aggregate_style_rename_between_series_columns_keeps_its_alias() {
        let entry = output_entry(
            &physical(col(series::BLOCK_END), &["pod"]),
            series::BLOCK_START,
            true,
        );
        assert_eq!(entry.text, "block_end@3 as block_start");
    }
}
