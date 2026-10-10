//! From a parsed expression to a DataFusion [`LogicalPlan`].
//!
//! The planner walks the expression tree once, bottom up. Every node
//! yields a plan in the canonical schema plus the label names that schema
//! carries — the next node up needs those to decide its own labels.
//! Whatever is not planned is [`EngineError::Unsupported`], **named**: the
//! differential suite counts those by feature, so a precise name is how
//! progress becomes a number rather than "red".
//!
//! Every node is stock DataFusion with one of this crate's functions in it:
//!
//! ```text
//! sum by (pod) (rate(http_requests_total[5m]))
//!
//! Projection: promql_labels('pod', __group__pod) AS labels, samples, block_start, block_end
//!   Aggregate: groupBy=[block_start, block_end, get_field(labels, 'pod') AS __group__pod],
//!              aggr=[promql_aggregate(samples, 'sum') AS samples]
//!     Projection: promql_labels(…without __name__) AS labels, samples, block_start, block_end
//!       Aggregate: groupBy=[block_start, block_end, labels],
//!                  aggr=[promql_range_function(samples, block_start, block_end, 'rate', …) AS samples]
//!         TableScan: selector_0        ← SelectorTable over SeriesSource::select
//! ```
//!
//! Every group key leads with the block. The store's plan is ordered by
//! block, then labels, so the selector and range aggregates run Sorted
//! over it and close a series at the block edge as well as at the next
//! label set; an aggregation above holds one partial per group and step
//! of the open block and emits when the block's last series has passed.
//! Each node projects back to the canonical column order, because an
//! Aggregate puts its keys first and everything above reads the shape by
//! position through [`series::validate`].
//!
//! The `__group__` prefix on a group key keeps a label named `labels`
//! or `samples` from colliding with the engine's own columns; see
//! [`labels::group_exprs`]. [`labels::regroup`] restores the bare name.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use datafusion::catalog::Session;
use datafusion::common::{Column, ScalarValue};
use datafusion::datasource::{provider_as_source, MemTable};
use datafusion::logical_expr::{col, lit, LogicalPlan, LogicalPlanBuilder};
use promql_parser::ast::{
    AggregateExpr, AtModifier, BinaryExpr, Call, Expr, UnaryExpr, VectorMatchCardinality,
    VectorSelector,
};
use promql_parser::token::ItemType;

use crate::absent;
use crate::aggregate::{self, Op};
use crate::binary;
use crate::elementwise;
use crate::error::EngineError;
use crate::labels;
use crate::matcher::{effective_matchers, METRIC_NAME};
use crate::params::{step_count, Params};
use crate::range::{self, Func};
use crate::selector;
use crate::series::{Block, Series, BLOCK_END, BLOCK_START, LABELS, SAMPLES};
use crate::sort;
use crate::source::{Grouping, SelectHints, SelectorTable, SeriesSource};
use crate::value_type::{value_type, ValueType};
use crate::{scalar, series};

/// The range a query is evaluated over.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RangeQuery {
    pub start_ms: i64,
    pub end_ms: i64,
    pub step_ms: i64,
    /// Prometheus's `--query.lookback-delta`, 5m by default.
    pub lookback_ms: i64,
}

impl RangeQuery {
    pub const DEFAULT_LOOKBACK: Duration = Duration::from_secs(5 * 60);

    pub fn new(start_ms: i64, end_ms: i64, step_ms: i64) -> Self {
        Self {
            start_ms,
            end_ms,
            step_ms,
            lookback_ms: millis(Self::DEFAULT_LOOKBACK),
        }
    }
}

/// Timestamps stay i64 milliseconds (Prometheus convention); this is the
/// one point where a `Duration` constant crosses into that world.
fn millis(d: Duration) -> i64 {
    i64::try_from(d.as_millis()).expect("duration exceeds i64 millis")
}

/// Checks the caller's range and returns it with the lookback resolved.
///
/// The bound on `start_ms`, `end_ms` and `lookback_ms` is what keeps the
/// kernels' `step - offset_ms - window_ms` (`selector.rs`, `buffer.rs`,
/// `range.rs`) and `select_range` inside an `i64`: a step lies in
/// `[start, end]` or is the `@` time, and each of the three terms is at most
/// `MAX_TIME_MS`, so the sum stays below `3 * MAX_TIME_MS < i64::MAX`. The
/// per-selector `@`, offset and range checks use the same bound, so the
/// terms compose. The kernels saturate or wrap rather than fail, which
/// would turn an out-of-range query into an empty result.
fn validate(query: &RangeQuery) -> Result<RangeQuery, EngineError> {
    if query.step_ms <= 0 {
        return Err(EngineError::Query(format!(
            "step must be positive, got {}ms",
            query.step_ms
        )));
    }
    // Without this the failure surfaces as a store ordering error, which
    // blames the store for the caller's mistake.
    if query.start_ms > query.end_ms {
        return Err(EngineError::Query(format!(
            "end {}ms is before start {}ms",
            query.end_ms, query.start_ms
        )));
    }
    check_time_bound("the start", query.start_ms)?;
    check_time_bound("the end", query.end_ms)?;
    let mut query = *query;
    // Ports `Engine.NewRangeQuery` (`engine.go`): `lookbackDelta <= 0`
    // takes the engine's value, which is 5m unless configured (`NewEngine`
    // replaces 0 with `defaultLookbackDelta`). Zero would otherwise
    // give every selector an empty window and no rows.
    if query.lookback_ms <= 0 {
        query.lookback_ms = millis(RangeQuery::DEFAULT_LOOKBACK);
    }
    check_time_bound("the lookback", query.lookback_ms)?;
    Ok(query)
}

/// Plan `expr` as `NewRangeQuery` (`promql/engine.go` at 83962c35)
/// would run it: a value per step, so only a scalar or an instant vector
/// has a plan.
pub async fn plan(
    state: &dyn Session,
    source: &dyn SeriesSource,
    expr: &Expr,
    query: &RangeQuery,
) -> Result<LogicalPlan, EngineError> {
    // Every query comes through here, so this covers a `RangeQuery` built
    // as a struct literal as well as through `new`.
    let query = &validate(query)?;
    check_expr(expr)?;
    // `NewRangeQuery` refuses every expression type but these two. A
    // string and a range vector are answered only by an instant query,
    // which [`crate::Engine::instant_query_async`] gives them without
    // the grid this plan is built over.
    let typ = value_type(expr);
    if !matches!(typ, ValueType::Scalar | ValueType::Vector) {
        return Err(EngineError::Query(format!(
            "invalid expression type \"{}\" for range query, must be Scalar or instant Vector",
            typ.as_str()
        )));
    }
    plan_grid(state, source, expr, query).await
}

/// Plan `expr` as `execEvalStmt` runs an instant query: a range of one
/// step whose answer is read as the expression's own type, so a range
/// selector is planned here and refused by [`plan`].
pub async fn plan_instant(
    state: &dyn Session,
    source: &dyn SeriesSource,
    expr: &Expr,
    at_ms: i64,
) -> Result<LogicalPlan, EngineError> {
    // The step is nominal: `Grid::new` wants it positive, and with equal
    // bounds nothing reads it but the hint the store hears.
    let query = &validate(&RangeQuery::new(at_ms, at_ms, 1))?;
    check_expr(expr)?;
    plan_grid(state, source, expr, query).await
}

async fn plan_grid(
    state: &dyn Session,
    source: &dyn SeriesSource,
    expr: &Expr,
    query: &RangeQuery,
) -> Result<LogicalPlan, EngineError> {
    // `aggregate::Grid` checks the same constant; this half exists so
    // the user gets a query error naming the limit.
    let steps = step_count(query.start_ms, query.end_ms, query.step_ms);
    if steps > aggregate::MAX_STEPS as i128 {
        return Err(EngineError::Query(format!(
            "{}..{} every {}ms is {steps} steps, more than the {} this engine allows",
            query.start_ms,
            query.end_ms,
            query.step_ms,
            aggregate::MAX_STEPS
        )));
    }
    let mut planner = Planner {
        state,
        source,
        query,
        selectors: 0,
        stages: 0,
    };
    Ok(planner.expr(expr, Above::default()).await?.plan)
}

/// The parse-time rules upstream holds an expression to before any of
/// it runs, for the ones our parser does not.
///
/// A whole-tree walk, as upstream's `checkAST` is, and for the same
/// reason: `1 > 1` is rejected wherever it sits, not only at the top.
/// These are `Query` errors — Prometheus refuses the query too, so the
/// engine is answering rather than admitting a gap — and they must come
/// before planning, or the scalar fold would quietly answer one of them.
fn check_expr(expr: &Expr) -> Result<(), EngineError> {
    match expr {
        Expr::Binary(b) => {
            // Down first: upstream's `checkAST` recurses before it
            // checks the node it is on, so `(1 > 1) and up` is refused
            // for the inner comparison and not for the outer operator.
            check_expr(&b.lhs)?;
            check_expr(&b.rhs)?;
            check_binary(b)
        }
        Expr::Aggregate(a) => {
            if let Some(param) = &a.param {
                check_expr(param)?;
            }
            check_expr(&a.expr)
        }
        Expr::Call(c) => c.args.iter().try_for_each(check_expr),
        Expr::MatrixSelector(ms) => check_expr(&ms.vector_selector),
        Expr::Subquery(s) => check_expr(&s.expr),
        Expr::Paren(p) => check_expr(&p.expr),
        Expr::Unary(u) => check_expr(&u.expr),
        Expr::StepInvariant(e) => check_expr(e),
        Expr::VectorSelector(_)
        | Expr::NumberLiteral(_)
        | Expr::StringLiteral(_)
        | Expr::Duration(_) => Ok(()),
    }
}

/// Upstream's `checkAST` arm for a binary expression
/// (`promql/parser/parse.go:760-828` at 83962c35), in its order and its
/// words.
///
/// The `bool` rule is here because the operators need it: a comparison
/// between two scalars folds to a 1 or a 0 either way, so without the
/// rule `1 > 1` would be answered rather than refused. The
/// `instant-query` branch adds the same rule for the same reason, and
/// the merge collapses the two into one.
fn check_binary(b: &BinaryExpr) -> Result<(), EngineError> {
    let (lhs, rhs) = (value_type(&b.lhs), value_type(&b.rhs));
    // In `parser/parse.go`'s own order (`:775` onwards at 83962c35), so
    // that a query breaking two of these is refused for the same one
    // Prometheus names.
    if b.return_bool && !is_comparison(b.op) {
        return Err(EngineError::Query(
            "bool modifier can only be used on comparison operators".into(),
        ));
    }
    // `1 > 1` is not a filter — there is nothing to filter — so upstream
    // makes the author write `bool` and say which of the two they meant.
    if is_comparison(b.op) && !b.return_bool && lhs == ValueType::Scalar && rhs == ValueType::Scalar
    {
        return Err(EngineError::Query(
            "comparisons between scalars must use BOOL modifier".into(),
        ));
    }
    if let Some(matching) = &b.vector_matching {
        // A label cannot both pick the match and be copied across it.
        if let Some(both) = matching
            .include
            .iter()
            .find(|name| matching.on && matching.matching_labels.contains(name))
        {
            return Err(EngineError::Query(format!(
                "label \"{both}\" must not occur in ON and GROUP clause at once"
            )));
        }
    }
    for side in [lhs, rhs] {
        if !matches!(side, ValueType::Scalar | ValueType::Vector) {
            return Err(EngineError::Query(
                "binary expression must contain only scalar and instant vector types".into(),
            ));
        }
    }
    // One switch upstream (`:805-824`), so the two arms are exclusive:
    // a modifier is only checked against the operator once both sides
    // are known to be vectors.
    let vectors = lhs == ValueType::Vector && rhs == ValueType::Vector;
    match &b.vector_matching {
        Some(matching) if !vectors => {
            // A scalar has no labels to match on. Only the named labels
            // are refused: upstream quietly throws the whole modifier
            // away otherwise, `group_left()` on a scalar included.
            if !matching.matching_labels.is_empty() {
                return Err(EngineError::Query(
                    "vector matching only allowed between instant vectors".into(),
                ));
            }
            if has_fill(matching) {
                return Err(EngineError::Query(
                    "filling in missing series only allowed between instant vectors".into(),
                ));
            }
        }
        // A set operator is many-to-many and nothing else: there is no
        // "one" side for `group_x` to copy a label off.
        Some(matching)
            if is_set(b.op)
                && matches!(
                    matching.card,
                    VectorMatchCardinality::ManyToOne | VectorMatchCardinality::OneToMany
                ) =>
        {
            return Err(EngineError::Query(format!(
                "no grouping allowed for \"{}\" operation",
                b.op
            )));
        }
        // Nothing to fill: a set operator never reads a value, so a
        // stand-in for a missing one has nothing to say.
        Some(matching) if is_set(b.op) && has_fill(matching) => {
            return Err(EngineError::Query(
                "filling in missing series not allowed for set operators".into(),
            ));
        }
        _ => {}
    }
    // A set operator is defined over signatures, and a scalar has no
    // labels to take one from.
    if is_set(b.op) && !vectors {
        return Err(EngineError::Query(format!(
            "set operator \"{}\" not allowed in binary scalar expression",
            b.op
        )));
    }
    Ok(())
}

/// The operators upstream's `IsComparisonOperator` answers for.
fn is_comparison(op: promql_parser::token::ItemType) -> bool {
    use promql_parser::token::ItemType::*;
    matches!(op, EqlC | Neq | Gtr | Lss | Gte | Lte)
}

fn has_fill(matching: &promql_parser::ast::VectorMatching) -> bool {
    matching.fill_values.lhs.is_some() || matching.fill_values.rhs.is_some()
}

/// The operators upstream's `IsSetOperator` answers for.
fn is_set(op: promql_parser::token::ItemType) -> bool {
    use promql_parser::token::ItemType::*;
    matches!(op, Land | Lor | Lunless)
}

/// The name both binary shapes give the node they add, so a plan reads
/// the same whichever side the scalar was on. Not the operator's own
/// spelling: `+_0` as a relation alias is a name nothing else in the
/// plan text would survive being asked to parse.
const BINARY_STAGE: &str = "binary";

/// The column a match group's output series arrive in, before `Unnest`
/// makes a row of each. Not a group alias and not a label name:
/// `labels::group_exprs` prefixes every key it makes, so nothing it
/// produces can be spelled this way.
const PAIRS: &str = "__pairs__";

/// What `Unnest` calls one field of an opened struct.
fn pair_column(field: &str) -> Column {
    Column::new_unqualified(format!("{PAIRS}.{field}"))
}

/// The operator a binary expression is, or the feature that stands in
/// the way of planning it.
///
/// Everything named here is a gap rather than a mistake — Prometheus
/// answers all of it — so each gets its own name: what the differential
/// suite counts is how many evaluations wait on which feature.
fn binary_op(b: &BinaryExpr) -> Result<(binary::Op, bool, binary::Matching), EngineError> {
    let op = binary::Op::from_token(b.op)
        .ok_or_else(|| EngineError::Unsupported(format!("the {} operator", b.op)))?;
    let mut out = binary::Matching::default();
    if let Some(matching) = &b.vector_matching {
        out.card = match matching.card {
            VectorMatchCardinality::OneToOne => binary::Card::OneToOne,
            VectorMatchCardinality::ManyToOne => binary::Card::ManyToOne,
            VectorMatchCardinality::OneToMany => binary::Card::OneToMany,
            // Upstream rewrites a set operator's `CardOneToOne` to
            // `CardManyToMany` while parsing (`parser/parse.go:783` at
            // 83962c35) and this crate's parser does not, so nothing
            // reaches here spelling it -- but if it ever does, it means
            // the same thing `and` already means: every series on both
            // sides, which is the card a set operator plans with.
            VectorMatchCardinality::ManyToMany => binary::Card::OneToOne,
        };
        out.fill_lhs = matching.fill_values.lhs;
        out.fill_rhs = matching.fill_values.rhs;
        out.on = matching.on;
        out.labels = sorted(&matching.matching_labels);
        out.include = sorted(&matching.include);
    }
    Ok((op, b.return_bool, out))
}

/// Label names as the engine holds them everywhere: sorted and unique,
/// so that two spellings of one modifier plan to one plan.
fn sorted(names: &[String]) -> Vec<String> {
    let mut out = names.to_vec();
    out.sort();
    out.dedup();
    out
}

/// The step grid alone, for the expressions that are a function of the
/// step and nothing else.
fn grid_of(query: &RangeQuery) -> Params {
    Params {
        start_ms: query.start_ms,
        end_ms: query.end_ms,
        step_ms: query.step_ms,
        window_ms: 0,
        offset_ms: 0,
        at_ms: None,
    }
}

/// A planned subexpression: the plan and the label names in its schema.
struct Planned {
    plan: LogicalPlan,
    label_names: Vec<String>,
}

struct Planner<'a> {
    state: &'a dyn Session,
    source: &'a dyn SeriesSource,
    query: &'a RangeQuery,
    /// Selectors seen so far; each becomes its own table `selector_N`.
    selectors: usize,
    /// Named nodes built so far — a folded table, an operator's alias.
    /// One counter for both, so no two names in one plan can collide.
    stages: usize,
}

type Planning<'f> = Pin<Box<dyn Future<Output = Result<Planned, EngineError>> + Send + 'f>>;

/// What stands directly above a selector, which is all the store gets
/// told about the query beyond its matchers and its range.
#[derive(Debug, Clone, Copy, Default)]
struct Above<'a> {
    /// The enclosing function or aggregation's PromQL name.
    func: Option<&'a str>,
    /// The nearest enclosing aggregation's labels, carried through the
    /// calls that map each series to one series and keep its labels.
    /// `func` stays the node nearest the selector, so under `rate` the
    /// store hears `func=rate` with the `by` of the `sum` above it.
    grouping: Option<&'a Grouping>,
}

impl Planner<'_> {
    /// A name nothing else in this plan has, for a node the planner
    /// invents rather than one the store named.
    fn stage(&mut self, what: &str) -> String {
        let n = self.stages;
        self.stages += 1;
        format!("{what}_{n}")
    }

    /// The one unlabelled series a scalar expression is, over the grid.
    ///
    /// Upstream's `funcVector` hands back `Sample{Metric:
    /// labels.Labels{}, F: …}` (`promql/functions.go:2028-2034` at
    /// 83962c35), the scalar's value with an empty label set.
    ///
    /// Nothing here reads a series, so the values are folded while
    /// planning (`value` is [`crate::scalar::fold`] over an expression,
    /// or a function of the step alone) and the plan is the table
    /// holding them: no store is asked, and no kernel runs. The table is
    /// numbered because a query can hold more than one of them —
    /// `vector(1) + vector(2)`, once binary operators land — and two
    /// scans of the same name in one plan are one scan.
    fn scalar_series(
        &mut self,
        value: impl Fn(i64) -> Result<f64, EngineError>,
    ) -> Result<LogicalPlan, EngineError> {
        let mut timestamps = Vec::new();
        let mut values = Vec::new();
        for ts in grid_of(self.query).steps() {
            values.push(value(ts)?);
            timestamps.push(ts);
        }

        let series = Series::new(&[], timestamps, values)?;
        // One block over the whole grid: the series is complete here, and
        // an aggregate above groups by block, so a split would only hand it
        // partials to fold back together.
        let block = Block {
            start_ms: i64::MIN,
            end_ms: i64::MAX,
        };
        let batch = series::encode(&[], &[series], block)?;
        let table = MemTable::try_new(batch.schema(), vec![vec![batch]])?;
        // Not `selector_N`: nothing was selected, and a plan text that
        // said so would be misleading.
        let name = self.stage("scalar");
        Ok(LogicalPlanBuilder::scan(name, provider_as_source(Arc::new(table)), None)?.build()?)
    }

    /// Plan one node. `above` is what the store will be told sits over
    /// the selector this subtree bottoms out in.
    fn expr<'f>(&'f mut self, expr: &'f Expr, above: Above<'f>) -> Planning<'f> {
        Box::pin(async move {
            match expr {
                Expr::VectorSelector(vs) => self.selector(vs, above, selector::Pick::Value).await,
                Expr::Paren(p) => self.expr(&p.expr, above).await,
                Expr::Aggregate(a) => self.aggregate(a).await,
                Expr::Call(c) => self.call(c, above).await,
                Expr::Binary(b) => self.binary(expr, b).await,
                Expr::Unary(u) => self.unary(expr, u, above).await,
                // A number is a scalar, and a scalar is one unlabelled
                // series over the grid, as `time()` is. A sign over a
                // number is a scalar-typed unary, which `unary` folds.
                Expr::NumberLiteral(_) => Ok(Planned {
                    plan: self.scalar_series(|ts| scalar::fold(expr, ts))?,
                    label_names: Vec::new(),
                }),
                other => Err(EngineError::Unsupported(describe(other))),
            }
        })
    }

    /// The store's hints for one selector: the scan range and the window
    /// it must honour, and whatever else may let it read less.
    ///
    /// `range_ms` is the `[5m]` of a range selector, absent for an
    /// instant one. `step_ms` is always the step the caller passed: this
    /// engine has one entry point, so a query whose bounds happen to be
    /// equal is a one-step range query, not an instant one, and guessing
    /// otherwise would withhold a grid the store can still align to.
    fn hints(&self, params: &Params, range_ms: Option<i64>, above: Above) -> SelectHints {
        let (start_ms, end_ms) = params.select_range();
        SelectHints {
            start_ms,
            end_ms,
            window_ms: params.window_ms,
            step_ms: Some(self.query.step_ms),
            range_ms,
            func: above.func.map(str::to_string),
            grouping: above.grouping.cloned(),
            shard: None,
        }
    }

    /// Ask the store for `vs` under `hints` and start a plan on the
    /// resulting table.
    async fn scan(
        &mut self,
        vs: &VectorSelector,
        hints: SelectHints,
    ) -> Result<(LogicalPlanBuilder, Vec<String>), EngineError> {
        reject_unsupported_modifiers(vs)?;
        let table = SelectorTable::try_new(
            self.state,
            self.source,
            &vs.name,
            &effective_matchers(vs),
            hints,
        )
        .await?;
        let label_names = table.label_names();
        let index = self.selectors;
        self.selectors += 1;
        let builder = LogicalPlanBuilder::scan(
            format!("selector_{index}"),
            provider_as_source(Arc::new(table)),
            None,
        )?;
        Ok((builder, label_names))
    }

    /// `pick` is the value, except under `timestamp(x)`, where the
    /// selector itself answers with each sample's time: see
    /// [`Planner::timestamp`].
    async fn selector(
        &mut self,
        vs: &VectorSelector,
        above: Above<'_>,
        pick: selector::Pick,
    ) -> Result<Planned, EngineError> {
        let params = Params {
            start_ms: self.query.start_ms,
            end_ms: self.query.end_ms,
            step_ms: self.query.step_ms,
            window_ms: self.query.lookback_ms,
            offset_ms: offset_ms(vs)?,
            at_ms: resolve_at(vs, self.query),
        };
        check_selector_bounds(params.at_ms, params.offset_ms)?;
        let hints = self.hints(&params, None, above);
        let (builder, label_names) = self.scan(vs, hints).await?;
        // Grouped by the block and the whole label set, so every chunk of
        // a label set in a block folds into one group.
        let builder = builder.aggregate(
            series_keys(),
            vec![selector::call(
                col(SAMPLES),
                col(BLOCK_START),
                col(BLOCK_END),
                &params,
                pick,
            )
            .alias(SAMPLES)],
        )?;
        let (labels_expr, label_names) = match pick {
            selector::Pick::Value => (col(LABELS), label_names),
            // `funcTimestamp` emits with `DropName`. Above the grouping,
            // as the range functions drop it: below, two series differing
            // only in their name would fold into one group.
            selector::Pick::Timestamp => {
                let (expr, names) = labels::keep(&label_names, |n| n != METRIC_NAME);
                (expr.alias(LABELS), names)
            }
        };
        let plan = builder.project(canonical(labels_expr))?.build()?;
        Ok(Planned { plan, label_names })
    }

    /// `timestamp(v)`, which upstream evaluates two ways (the `Call` arm
    /// of `eval`, `promql/engine.go` at 83962c35). Over a bare vector
    /// selector, parentheses and all,
    /// `rangeEvalTimestampFunctionOverVectorSelector` reads each picked
    /// sample's own time. Over anything else the vector's samples carry
    /// the step they were evaluated at, and `funcTimestamp` reads that;
    /// `timestamp(abs(x))` is the step, where `timestamp(x)` is not.
    async fn timestamp(&mut self, call: &Call, above: Above<'_>) -> Result<Planned, EngineError> {
        let mut arg = &call.args[0];
        loop {
            arg = match arg {
                Expr::Paren(p) => &p.expr,
                Expr::StepInvariant(e) => e,
                _ => break,
            };
        }
        match arg {
            Expr::VectorSelector(vs) => {
                self.selector(
                    vs,
                    Above {
                        func: Some("timestamp"),
                        grouping: above.grouping,
                    },
                    selector::Pick::Timestamp,
                )
                .await
            }
            _ => {
                self.elementwise(call, elementwise::Func::Timestamp, above)
                    .await
            }
        }
    }

    /// One function call, sent to the operator its shape calls for.
    ///
    /// The arity and argument types are settled first, in upstream's
    /// words: a call this engine cannot plan and a call Prometheus would
    /// not accept are different answers, and only the second is the
    /// user's mistake.
    async fn call(&mut self, call: &Call, above: Above<'_>) -> Result<Planned, EngineError> {
        crate::function::check_call(call)?;
        let name = call.func.name.as_str();
        if name == "vector" {
            return Ok(Planned {
                plan: self.scalar_series(|ts| scalar::fold(&call.args[0], ts))?,
                label_names: Vec::new(),
            });
        }
        if name == "absent" {
            // `absent` reads which series exist, not their labels, so no
            // `by` above it describes what the store should read.
            let above = Above {
                func: Some(name),
                grouping: None,
            };
            let input = self.expr(&call.args[0], above).await?;
            return self.absent(&call.args[0], input);
        }
        if name == "absent_over_time" {
            let input = self
                .range_over(call, Func::PresentOverTime, name, None)
                .await?;
            return self.absent(&call.args[0], input);
        }
        // `time()` and `pi()` are a value per step and nothing else, the
        // table `scalar_series` builds, which the result then reads as a
        // scalar. `scalar(v)` is typed the same and is not that: it reads
        // a vector, and the fold names it as the gap it is.
        let returns_scalar =
            crate::function::signature(name).is_some_and(|s| s.return_type == ValueType::Scalar);
        if returns_scalar {
            return Ok(Planned {
                plan: self.scalar_series(|ts| scalar::call(call, ts))?,
                label_names: Vec::new(),
            });
        }
        if name == "timestamp" {
            return self.timestamp(call, above).await;
        }
        if let Some(func) = elementwise::Func::parse(name) {
            return self.elementwise(call, func, above).await;
        }
        if let Some(func) = sort::Func::parse(name) {
            return self.sort(call, func).await;
        }
        // `label_replace` and `label_join` do not get here yet. When they
        // do they must stop `grouping`: they rewrite labels, so a `by`
        // can name a label the input lacks while its source goes unnamed.
        self.range_function(call, above).await
    }

    /// `absent(v)` and `absent_over_time(x[5m])`: the one series over the
    /// steps `input` reached none of, under the labels upstream reads off
    /// the argument expression rather than off any series.
    ///
    /// The aggregate has no group key, not even the block: that is what
    /// makes it answer when `input` has no rows at all, and the block it
    /// stamps is the whole grid, as a scalar's is. The store partitions
    /// meet in the Final it plans, which merges one presence set per
    /// partition. Where every step was answered the row leaves with no
    /// samples, and the output drops it as it drops any other empty
    /// series.
    fn absent(&mut self, arg: &Expr, input: Planned) -> Result<Planned, EngineError> {
        let pairs = absent::labels_for(arg);
        let label_names = pairs.iter().map(|(n, _)| n.clone()).collect();
        let labels_expr = labels::call(pairs.into_iter().map(|(n, v)| (n, lit(v))).collect());
        let plan = LogicalPlanBuilder::from(input.plan)
            .aggregate(
                Vec::<datafusion::logical_expr::Expr>::new(),
                vec![absent::call(
                    col(SAMPLES),
                    self.query.start_ms,
                    self.query.end_ms,
                    self.query.step_ms,
                )
                .alias(SAMPLES)],
            )?
            .project(whole_grid(labels_expr.alias(LABELS), col(SAMPLES)))?
            .build()?;
        Ok(Planned { plan, label_names })
    }

    /// A function applied to one sample at a time: `abs(x)` and its
    /// family. The vector is the first argument and the rest are
    /// scalars, already checked to be scalars by [`crate::function`].
    async fn elementwise(
        &mut self,
        call: &Call,
        func: elementwise::Func,
        above: Above<'_>,
    ) -> Result<Planned, EngineError> {
        let mut args = Vec::new();
        for arg in call.args.iter().skip(1) {
            args.push(self.constant(arg, &format!("the {} function", func.as_str()))?);
        }
        let input = match call.args.first() {
            // Upstream's `extractFuncFromPath` tells the store the
            // innermost call it sits under, which for `abs(x)` is this
            // one.
            Some(vector) => {
                self.expr(
                    vector,
                    Above {
                        func: Some(func.as_str()),
                        grouping: above.grouping,
                    },
                )
                .await?
            }
            // `dateWrapper` with no vector reads the evaluation time as
            // one unlabelled sample, which is what `vector(time())` is;
            // `time.Unix(enh.Ts/1000, 0)` and `int64(float64(Ts)/1000)`
            // both truncate toward zero, so the one kernel serves both
            // forms. Only that family reaches here without a vector:
            // `check_call` has held every other call to its arity.
            None => Planned {
                plan: self.scalar_series(|ts| Ok(scalar::time(ts)))?,
                label_names: Vec::new(),
            },
        };
        let (labels_expr, label_names) = labels::keep(&input.label_names, |n| {
            !func.drops_metric_name() || n != METRIC_NAME
        });
        // A name of its own for what this function reads. Without it the
        // projection below can be merged into the one under it, and the
        // merged node would hold both the input's qualified `labels` and
        // this one's plain `labels` — the same column under two names,
        // which DataFusion resolves as ambiguous.
        let stage = self.stage(func.as_str());
        let plan = LogicalPlanBuilder::from(input.plan)
            .alias(stage)?
            .project(vec![
                labels_expr.alias(LABELS),
                elementwise::call(
                    col(SAMPLES),
                    func,
                    args.first().copied(),
                    args.get(1).copied(),
                )
                .alias(SAMPLES),
                col(BLOCK_START),
                col(BLOCK_END),
            ])?
            .build()?;
        Ok(Planned { plan, label_names })
    }

    /// `-x` and `+x`: upstream's `UnaryExpr` arm of `eval`
    /// (`promql/engine.go`). Minus negates every sample and drops the
    /// metric name; plus returns its operand untouched, name included,
    /// because upstream's loop only runs for `SUB`. A scalar operand is a
    /// value per step and folds like any other scalar. `above` passes
    /// through: the sign changes no sample's selection, so the store is
    /// told what sits over the operator.
    async fn unary(
        &mut self,
        expr: &Expr,
        u: &UnaryExpr,
        above: Above<'_>,
    ) -> Result<Planned, EngineError> {
        if value_type(&u.expr) == ValueType::Scalar {
            return Ok(Planned {
                plan: self.scalar_series(|ts| scalar::fold(expr, ts))?,
                label_names: Vec::new(),
            });
        }
        match u.op {
            ItemType::Add => self.expr(&u.expr, above).await,
            ItemType::Sub => {
                let input = self.expr(&u.expr, above).await?;
                self.negate(input)
            }
            op => Err(EngineError::Unsupported(format!("the unary {op} operator"))),
        }
    }

    /// `elementwise`'s projection with [`elementwise::Func::Neg`] and no
    /// arguments, over an already planned vector.
    fn negate(&mut self, input: Planned) -> Result<Planned, EngineError> {
        let func = elementwise::Func::Neg;
        let (labels_expr, label_names) = labels::keep(&input.label_names, |n| n != METRIC_NAME);
        // Named for the reason `elementwise` names its stage.
        let stage = self.stage(func.as_str());
        let plan = LogicalPlanBuilder::from(input.plan)
            .alias(stage)?
            .project(vec![
                labels_expr.alias(LABELS),
                elementwise::call(col(SAMPLES), func, None, None).alias(SAMPLES),
                col(BLOCK_START),
                col(BLOCK_END),
            ])?
            .build()?;
        Ok(Planned { plan, label_names })
    }

    /// One binary operator, sent to the shape its operand types call
    /// for. `expr` is `b` again, for the scalar fold that needs the
    /// whole node.
    async fn binary(&mut self, expr: &Expr, b: &BinaryExpr) -> Result<Planned, EngineError> {
        let (op, return_bool, matching) = binary_op(b)?;
        match (value_type(&b.lhs), value_type(&b.rhs)) {
            (ValueType::Vector, ValueType::Scalar) => {
                self.vector_scalar(&b.lhs, &b.rhs, op, return_bool, false)
                    .await
            }
            (ValueType::Scalar, ValueType::Vector) => {
                self.vector_scalar(&b.rhs, &b.lhs, op, return_bool, true)
                    .await
            }
            (ValueType::Vector, ValueType::Vector) => {
                self.vector_vector(b, op, return_bool, matching).await
            }
            // Two scalars are a value per step and nothing else, which
            // is the table [`Planner::scalar_series`] already builds.
            _ => Ok(Planned {
                plan: self.scalar_series(|ts| scalar::fold(expr, ts))?,
                label_names: Vec::new(),
            }),
        }
    }

    /// A vector against a scalar: upstream's `VectorscalarBinop`, which
    /// writes the value back into the sample it read. That is
    /// [`Planner::elementwise`]'s shape with the scalar folded in, so it
    /// is that kernel that runs — a comparison included, which answers
    /// for some samples and drops the rest.
    async fn vector_scalar(
        &mut self,
        vector: &Expr,
        scalar: &Expr,
        op: binary::Op,
        return_bool: bool,
        swap: bool,
    ) -> Result<Planned, EngineError> {
        let what = format!("the {} operator", op.as_str());
        let value = self.constant(scalar, &what)?;
        let input = self.expr(vector, Above::default()).await?;
        // Rebuilt even when every label survives. Passing `col(LABELS)`
        // through keeps the input's own qualifier on the output field,
        // so the stage answers with `binary_0.labels` where every other
        // stage answers with `labels`; the next operator up then unions
        // two branches that disagree about the name and DataFusion
        // calls it ambiguous. A computed struct is unqualified.
        let (labels_expr, label_names) = labels::keep(&input.label_names, |n| {
            !op.drops_metric_name(return_bool) || n != METRIC_NAME
        });
        // The scalar goes in the slot that says which side it was on;
        // see `elementwise::Func::bind`.
        let (a, b) = if swap {
            (None, Some(value))
        } else {
            (Some(value), None)
        };
        // Named for the same reason `elementwise` names its stage.
        let stage = self.stage(BINARY_STAGE);
        let plan = LogicalPlanBuilder::from(input.plan)
            .alias(stage)?
            .project(vec![
                labels_expr.alias(LABELS),
                elementwise::call(
                    col(SAMPLES),
                    elementwise::Func::Binary { op, return_bool },
                    a,
                    b,
                )
                .alias(SAMPLES),
                col(BLOCK_START),
                col(BLOCK_END),
            ])?
            .build()?;
        Ok(Planned { plan, label_names })
    }

    /// Two vectors, matched on the signature the modifiers name.
    ///
    /// Both sides are widened to one label schema and unioned, so a
    /// single `Aggregate` over the match signature is the pairing;
    /// [`crate::binary`] says why a grouping is the right node and why
    /// it answers with a list of series rather than one.
    async fn vector_vector(
        &mut self,
        b: &BinaryExpr,
        op: binary::Op,
        return_bool: bool,
        matching: binary::Matching,
    ) -> Result<Planned, EngineError> {
        let lhs = self.expr(&b.lhs, Above::default()).await?;
        let rhs = self.expr(&b.rhs, Above::default()).await?;
        // The `on` labels join the schema even where neither side
        // carries them: a label both sides lack is the empty string on
        // both, which is a match, and the grouping needs a column to
        // say so.
        let mut names: Vec<String> = lhs
            .label_names
            .iter()
            .chain(rhs.label_names.iter())
            .chain(matching.labels.iter().filter(|_| matching.on))
            .cloned()
            .collect();
        names.sort();
        names.dedup();

        // Upstream's `signatureFunc`: `on` names the whole signature,
        // `ignoring` takes its labels and `__name__` out of it.
        let keys: Vec<String> = match matching.on {
            true => matching.labels.clone(),
            false => names
                .iter()
                .filter(|n| *n != METRIC_NAME && !matching.labels.contains(n))
                .cloned()
                .collect(),
        };

        let left = self.operand(lhs, &names, false)?;
        let right = self.operand(rhs, &names, true)?;
        let plan = LogicalPlanBuilder::from(left)
            .union(right)?
            .aggregate(
                labels::group_exprs(&keys),
                vec![binary::call(
                    col(SAMPLES),
                    col(binary::SIDE),
                    col(LABELS),
                    op,
                    return_bool,
                    &matching,
                    self.query.start_ms,
                    self.query.end_ms,
                    self.query.step_ms,
                )
                .alias(PAIRS)],
            )?
            // Twice: the first turns the list into one row per output
            // series, the second opens the struct that row holds into
            // its own columns. Reaching into it with `get_field`
            // instead lets a projection pushdown float the read back
            // under the `Unnest`, where the column is still a list.
            .unnest_column(Column::new_unqualified(PAIRS))?
            .unnest_column(Column::new_unqualified(PAIRS))?
            .project(whole_grid(
                col(pair_column(binary::PAIR_LABELS)).alias(LABELS),
                col(pair_column(binary::PAIR_SAMPLES)).alias(SAMPLES),
            ))?
            .build()?;
        // The aggregation builds the result labels itself, in the
        // schema both sides were widened to; which of them a given
        // query leaves empty everywhere is upstream's `resultMetric`
        // to decide, not the planner's.
        Ok(Planned {
            plan,
            label_names: names,
        })
    }

    /// One operand in the shape the union takes: the shared label
    /// schema, the samples, and which side it is.
    fn operand(
        &mut self,
        operand: Planned,
        names: &[String],
        is_rhs: bool,
    ) -> Result<LogicalPlan, EngineError> {
        let stage = self.stage(BINARY_STAGE);
        Ok(LogicalPlanBuilder::from(operand.plan)
            .alias(stage)?
            .project(vec![
                labels::widen(&operand.label_names, names).alias(LABELS),
                col(SAMPLES),
                lit(is_rhs).alias(binary::SIDE),
            ])?
            .build()?)
    }

    /// A scalar argument as the one number the whole plan will use.
    ///
    /// Upstream evaluates a function's scalar arguments at every step,
    /// so `clamp_max(x, time())` narrows as the query walks forward.
    /// Here they are folded while planning, which only holds for an
    /// argument that does not move; one that does is named as the gap it
    /// is rather than silently taken at its first value.
    fn constant(&self, expr: &Expr, what: &str) -> Result<f64, EngineError> {
        let mut folded: Option<f64> = None;
        for ts in grid_of(self.query).steps() {
            let v = scalar::fold(expr, ts)?;
            match folded {
                Some(first) if first.to_bits() != v.to_bits() => {
                    return Err(EngineError::Unsupported(format!(
                        "{what} with a scalar argument that changes between steps"
                    )))
                }
                _ => folded = Some(v),
            }
        }
        // An empty grid cannot happen: the step check above has already
        // refused a non-positive step, and start..=end always holds one
        // step.
        folded.ok_or_else(|| EngineError::Query("the query has no steps".into()))
    }

    /// One of the four ordering functions over an instant vector.
    ///
    /// The result of a range query is a matrix, which Prometheus sorts by
    /// label set regardless, so these are inert there — `funcSort` only
    /// ever sees the one-step vector. Equal bounds is how the one entry
    /// point this engine has spells an instant query.
    async fn sort(&mut self, call: &Call, func: sort::Func) -> Result<Planned, EngineError> {
        let name = func.as_str();
        let (vector, args) = call.args.split_first().ok_or_else(|| {
            EngineError::Query(format!("{name} expects a vector argument, got none"))
        })?;
        let mut labels = Vec::with_capacity(args.len());
        for arg in args {
            match arg {
                Expr::StringLiteral(s) => labels.push(s.val.clone()),
                other => {
                    return Err(EngineError::Query(format!(
                        "{name} expects label names as string literals, got {}",
                        describe(other)
                    )))
                }
            }
        }

        let input = self
            .expr(
                vector,
                Above {
                    func: Some(name),
                    grouping: None,
                },
            )
            .await?;
        if self.query.start_ms != self.query.end_ms {
            return Ok(input);
        }
        let keys = sort::exprs(func, &labels, &input.label_names);
        let plan = LogicalPlanBuilder::from(input.plan).sort(keys)?.build()?;
        Ok(Planned {
            plan,
            label_names: input.label_names,
        })
    }

    /// A function of one range selector: `rate(x[5m])` and its family.
    ///
    /// This call is what stands directly over the selector, so of `above`
    /// only the enclosing `by` reaches the store.
    async fn range_function(
        &mut self,
        call: &Call,
        above: Above<'_>,
    ) -> Result<Planned, EngineError> {
        // Before the function is resolved: `deriv(foo[3m] smoothed)` is
        // an invalid query whether or not this engine has `deriv`.
        check_range_modifiers(call)?;
        let name = call.func.name.as_str();
        let func = Func::parse(name)
            .ok_or_else(|| EngineError::Unsupported(format!("the {name} function")))?;
        self.range_over(call, func, name, above.grouping).await
    }

    /// `func` over the one range selector `call` takes, told to the store
    /// as `name`: the function the user wrote, which is not always the
    /// kernel that answers it. `absent_over_time` runs `present_over_time`
    /// below its complement, and the store should still hear the name
    /// whose samples it may skip.
    async fn range_over(
        &mut self,
        call: &Call,
        func: Func,
        name: &str,
        grouping: Option<&Grouping>,
    ) -> Result<Planned, EngineError> {
        let (ms, vs) = match call.args.as_slice() {
            [Expr::MatrixSelector(ms)] => match ms.vector_selector.as_ref() {
                Expr::VectorSelector(vs) => (ms, vs),
                other => {
                    return Err(EngineError::Unsupported(format!(
                        "a range selector over {}",
                        describe(other)
                    )))
                }
            },
            [Expr::Subquery(_)] => return Err(EngineError::Unsupported("a subquery".into())),
            [other] => {
                return Err(EngineError::Unsupported(format!(
                    "the {name} function over {}",
                    describe(other)
                )))
            }
            // Out of reach: every range function this engine knows takes
            // exactly one argument, and `check_call` has already said so.
            _ => {
                return Err(EngineError::Query(format!(
                    "{name} expects exactly one argument, got {}",
                    call.args.len()
                )))
            }
        };
        if ms.range_expr.is_some() {
            return Err(EngineError::Unsupported(
                "a range given as a duration expression".into(),
            ));
        }

        let params = Params {
            start_ms: self.query.start_ms,
            end_ms: self.query.end_ms,
            step_ms: self.query.step_ms,
            window_ms: duration_ms("the range", ms.range_secs)?,
            offset_ms: offset_ms(vs)?,
            at_ms: resolve_at(vs, self.query),
        };
        check_selector_bounds(params.at_ms, params.offset_ms)?;
        check_time_bound("the range", params.window_ms)?;
        // `func` is this function, not the aggregation over it: `rate` is
        // what decides which samples the store may skip. The aggregation's
        // `by` still travels as `grouping`, because `rate` keeps each
        // series' labels and so the labels it names are the ones read off
        // these series. It does not license merging them: two counters
        // merged below `rate` break reset detection.
        let hints = self.hints(
            &params,
            Some(params.window_ms),
            Above {
                func: Some(name),
                grouping,
            },
        );
        let (builder, input_names) = self.scan(vs, hints).await?;
        let builder = builder.aggregate(
            series_keys(),
            vec![range::call(
                col(SAMPLES),
                col(BLOCK_START),
                col(BLOCK_END),
                func,
                &params,
            )
            .alias(SAMPLES)],
        )?;
        // `__name__` is dropped above the grouping, not below it: below, two
        // series differing only in their name would fold into one group.
        if !func.drops_metric_name() {
            return Ok(Planned {
                plan: builder.project(canonical(col(LABELS)))?.build()?,
                label_names: input_names,
            });
        }
        let (labels_expr, label_names) = labels::keep(&input_names, |n| n != METRIC_NAME);
        let plan = builder
            .project(canonical(labels_expr.alias(LABELS)))?
            .build()?;
        Ok(Planned { plan, label_names })
    }

    async fn aggregate(&mut self, agg: &AggregateExpr) -> Result<Planned, EngineError> {
        let op = Op::from_token(agg.op)
            .ok_or_else(|| EngineError::Unsupported(format!("the {} aggregation", agg.op)))?;
        if agg.param.is_some() {
            return Err(EngineError::Unsupported(format!(
                "the {} aggregation with a parameter",
                agg.op
            )));
        }
        // `without` names what to drop, so it says nothing about which
        // labels a store may stop reading; only `by` does, and upstream's
        // `extractGroupsFromPath` hints on that alone.
        let grouping = (!agg.without).then(|| Grouping {
            labels: agg.grouping.clone(),
            by: true,
        });
        let input = self
            .expr(
                &agg.expr,
                Above {
                    func: Some(op.as_str()),
                    grouping: grouping.as_ref(),
                },
            )
            .await?;

        let keys = labels::group_keys(&input.label_names, &agg.grouping, agg.without);
        // The block leads the key, so each group is one block's partials
        // and leaves when the block does.
        let mut group = vec![col(BLOCK_START), col(BLOCK_END)];
        group.extend(labels::group_exprs(&keys));
        let plan = LogicalPlanBuilder::from(input.plan)
            .aggregate(
                group,
                vec![aggregate::call(
                    col(SAMPLES),
                    op,
                    self.query.start_ms,
                    self.query.end_ms,
                    self.query.step_ms,
                )
                .alias(SAMPLES)],
            )?
            .project(canonical(labels::regroup(&keys).alias(LABELS)))?
            .build()?;
        Ok(Planned {
            plan,
            label_names: keys,
        })
    }
}

/// The group key of a selector or range aggregate: one group per label
/// set and block.
fn series_keys() -> Vec<datafusion::logical_expr::Expr> {
    vec![col(BLOCK_START), col(BLOCK_END), col(LABELS)]
}

/// The canonical columns with the one block [`Planner::scalar_series`]
/// uses too. Two operands' edges need not agree, since a selector's offset
/// moves them, so the pairing groups by the match signature alone and
/// holds the whole grid; grouping by block as well would split a pair
/// whose sides fall in different blocks and drop it.
fn whole_grid(
    labels: datafusion::logical_expr::Expr,
    samples: datafusion::logical_expr::Expr,
) -> Vec<datafusion::logical_expr::Expr> {
    let edge = |ms: i64| lit(ScalarValue::TimestampMillisecond(Some(ms), None));
    vec![
        labels,
        samples,
        edge(i64::MIN).alias(BLOCK_START),
        edge(i64::MAX).alias(BLOCK_END),
    ]
}

/// The canonical columns in order, `labels` being that column as given:
/// a computed label set arrives aliased, the column itself bare, since
/// aliasing `labels` to its own name leaves the optimizer two columns
/// of that name to choose from.
fn canonical(labels: datafusion::logical_expr::Expr) -> Vec<datafusion::logical_expr::Expr> {
    vec![labels, col(SAMPLES), col(BLOCK_START), col(BLOCK_END)]
}

fn offset_ms(vs: &VectorSelector) -> Result<i64, EngineError> {
    duration_ms("the offset", vs.original_offset_secs)
}

/// A range or offset given as float seconds. Non-finite or `i64`-sized
/// input is a query error rather than the clamped value `as i64` gives.
fn duration_ms(what: &str, secs: f64) -> Result<i64, EngineError> {
    promql_common::model::secs_to_millis(secs)
        .ok_or_else(|| EngineError::Query(format!("{what} {secs}s is out of range")))
}

/// The widest `@` timestamp, offset or range this engine will plan.
///
/// Three of these terms meet in one `i64` where `select_range` subtracts
/// the window and the offset, so the bound is a quarter of the range.
/// Prometheus catches a non-finite or `int64`-sized `@` while parsing
/// (`setTimestamp` in `parser.go`, ported in `promql-parser`); this bound
/// is the narrower one this engine's arithmetic needs on top of that.
const MAX_TIME_MS: i64 = i64::MAX / 4;

/// One term of the scan-range arithmetic, named for the message.
fn check_time_bound(what: &str, ms: i64) -> Result<(), EngineError> {
    if (-MAX_TIME_MS..=MAX_TIME_MS).contains(&ms) {
        return Ok(());
    }
    Err(EngineError::Query(format!(
        "{what} {ms}ms is out of range, the limit is {MAX_TIME_MS}ms"
    )))
}

/// The `@` and the offset of one selector, rejected here rather than in
/// the kernel: `select_range` and the kernels below it saturate rather
/// than panic, which would answer an absurd query with an empty result.
fn check_selector_bounds(at_ms: Option<i64>, offset_ms: i64) -> Result<(), EngineError> {
    if let Some(at) = at_ms {
        check_time_bound("the @ modifier timestamp", at)?;
    }
    check_time_bound("the offset", offset_ms)
}

/// The `@` modifier's timestamp, with `start()`/`end()` resolved against
/// the query. Upstream does this in `preprocessExpr`; our parser leaves it
/// to the engine.
fn resolve_at(vs: &VectorSelector, query: &RangeQuery) -> Option<i64> {
    match (vs.timestamp, vs.start_or_end) {
        (Some(ts), _) => Some(ts),
        (None, Some(AtModifier::Start)) => Some(query.start_ms),
        (None, Some(AtModifier::End)) => Some(query.end_ms),
        (None, None) => None,
    }
}

/// Upstream's `AnchoredSafeFunctions` / `SmoothedSafeFunctions` gate
/// (engine.go). Both modifiers hand the function samples from outside
/// the window, so only the functions that reduce a window to a
/// difference between its ends may see them.
///
/// Sorted, because upstream sorts the map keys into the message and the
/// corpus asserts on the text.
fn check_range_modifiers(call: &Call) -> Result<(), EngineError> {
    const ANCHORED_SAFE: [&str; 5] = ["changes", "delta", "increase", "rate", "resets"];
    const SMOOTHED_SAFE: [&str; 3] = ["delta", "increase", "rate"];

    let Some(vs) = call.args.iter().find_map(|a| match a {
        Expr::MatrixSelector(ms) => match ms.vector_selector.as_ref() {
            Expr::VectorSelector(vs) => Some(vs),
            _ => None,
        },
        _ => None,
    }) else {
        return Ok(());
    };
    // Upstream's `switch` tries anchored first; the parser has already
    // rejected a selector carrying both.
    let (modifier, safe) = if vs.anchored {
        ("anchored", &ANCHORED_SAFE[..])
    } else if vs.smoothed {
        ("smoothed", &SMOOTHED_SAFE[..])
    } else {
        return Ok(());
    };
    let name = call.func.name.as_str();
    if safe.contains(&name) {
        return Ok(());
    }
    Err(EngineError::Query(format!(
        "{modifier} modifier can only be used with: {} - not with {name}",
        safe.join(", ")
    )))
}

fn reject_unsupported_modifiers(vs: &VectorSelector) -> Result<(), EngineError> {
    if vs.original_offset_expr.is_some() {
        return Err(EngineError::Unsupported(
            "an offset given as a duration expression".into(),
        ));
    }
    if vs.anchored || vs.smoothed {
        return Err(EngineError::Unsupported(
            "the anchored and smoothed modifiers".into(),
        ));
    }
    if vs.skip_histogram_buckets {
        return Err(EngineError::Unsupported(
            "the __ignore_histogram_buckets__ modifier".into(),
        ));
    }
    Ok(())
}

/// The text of a query that is nothing but a string literal, however
/// parenthesised. Upstream evaluates `("Foo")` to the string `Foo` as it
/// does `"Foo"`: `ParenExpr` is transparent in `eval`.
pub fn string_literal(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::StringLiteral(s) => Some(&s.val),
        Expr::Paren(p) => string_literal(&p.expr),
        _ => None,
    }
}

/// A name for what an expression is, for the "not supported yet" message.
/// Singular noun phrases, because the message is "{this} is not supported
/// yet". Aggregations and functions are named individually so the count
/// per feature is a roadmap.
pub fn describe(expr: &Expr) -> String {
    match expr {
        Expr::Aggregate(a) => format!("the {} aggregation", a.op),
        Expr::Call(c) => format!("the {} function", c.func.name),
        Expr::Binary(_) => "a binary operator".into(),
        Expr::MatrixSelector(_) => "a range selector".into(),
        Expr::Subquery(_) => "a subquery".into(),
        Expr::NumberLiteral(_) | Expr::Duration(_) => "a scalar literal".into(),
        Expr::StringLiteral(_) => "a string literal".into(),
        Expr::Paren(_) => "a parenthesized expression".into(),
        Expr::Unary(_) => "a unary operator".into(),
        Expr::StepInvariant(_) => "a step-invariant expression".into(),
        Expr::VectorSelector(_) => "a vector selector".into(),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use async_trait::async_trait;
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::prelude::SessionContext;
    use promql_parser::ast::{Expr, LabelMatcher};

    use super::*;
    use crate::memory::MemorySeriesSource;

    /// A store that keeps what it was asked, so a test can read the
    /// hints the planner built.
    #[derive(Debug, Default)]
    struct Recorder {
        inner: MemorySeriesSource,
        seen: Mutex<Vec<SelectHints>>,
    }

    #[async_trait]
    impl SeriesSource for Recorder {
        async fn select(
            &self,
            state: &dyn Session,
            matchers: &[LabelMatcher],
            hints: SelectHints,
        ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
            self.seen.lock().unwrap().push(hints.clone());
            self.inner.select(state, matchers, hints).await
        }
    }

    /// The hints one query's selectors reach the store with.
    async fn hints_of(query: &str, range: RangeQuery) -> Vec<SelectHints> {
        let ctx = SessionContext::new();
        let source = Recorder::default();
        let expr: Expr = promql_parser::parse_expr(query).expect("query parses");
        plan(&ctx.state(), &source, &expr, &range)
            .await
            .expect("query plans");
        source.seen.into_inner().unwrap()
    }

    #[tokio::test]
    async fn the_store_hears_the_step_the_window_and_what_is_above_the_selector() {
        let range = RangeQuery::new(0, 600_000, 30_000);

        let hints = hints_of("sum by (pod)(rate(x[5m]))", range).await;
        assert_eq!(hints.len(), 1);
        assert_eq!(hints[0].step_ms, Some(30_000));
        assert_eq!(hints[0].range_ms, Some(300_000));
        // `rate` stands between the aggregation and the selector, so it
        // is the function the store is told about; the grouping two
        // levels up still reaches it, as metadata.
        assert_eq!(hints[0].func.as_deref(), Some("rate"));
        assert_eq!(
            hints[0].grouping,
            Some(Grouping {
                labels: vec!["pod".to_string()],
                by: true,
            })
        );

        // A plain selector has no window and nothing above it.
        let hints = hints_of("x", range).await;
        assert_eq!(hints.len(), 1);
        assert_eq!(hints[0].step_ms, Some(30_000));
        assert_eq!(hints[0].range_ms, None);
        assert_eq!(hints[0].func, None);
        assert_eq!(hints[0].grouping, None);

        // Directly under a `by` aggregation, that is what it hears.
        let hints = hints_of("sum by (pod)(x)", range).await;
        assert_eq!(hints[0].func.as_deref(), Some("sum"));
        assert_eq!(
            hints[0].grouping,
            Some(Grouping {
                labels: vec!["pod".to_string()],
                by: true,
            })
        );

        // `without` names what to drop, which is no help to a store.
        let hints = hints_of("count without (pod)(x)", range).await;
        assert_eq!(hints[0].func.as_deref(), Some("count"));
        assert_eq!(hints[0].grouping, None);

        // Equal bounds are a one-step range query, not an instant one:
        // the step is still the grid the caller asked for.
        let one_step = RangeQuery::new(600_000, 600_000, 30_000);
        let hints = hints_of("x", one_step).await;
        assert_eq!(hints[0].step_ms, Some(30_000));
    }

    /// `grouping` reaches through calls that keep each series' labels and
    /// stops where the `by` would name labels the selector does not carry.
    /// `func` stays the node nearest the selector throughout.
    #[tokio::test]
    async fn the_store_hears_the_by_of_the_aggregation_over_label_keeping_calls() {
        let range = RangeQuery::new(0, 600_000, 30_000);
        let by = |labels: &[&str]| {
            Some(Grouping {
                labels: labels.iter().map(|l| l.to_string()).collect(),
                by: true,
            })
        };

        for query in [
            "sum by (pod)(rate(x[5m]))",
            "sum by (pod)(abs(rate(x[5m])))",
            // A sign neither adds a node to `func` nor ends the `by`.
            "sum by (pod)(-rate(x[5m]))",
            "sum by (pod)(+rate(x[5m]))",
        ] {
            let hints = hints_of(query, range).await;
            assert_eq!(hints[0].func.as_deref(), Some("rate"), "{query}");
            assert_eq!(hints[0].grouping, by(&["pod"]), "{query}");
        }

        let hints = hints_of("sum by (pod)(timestamp(x))", range).await;
        assert_eq!(hints[0].func.as_deref(), Some("timestamp"));
        assert_eq!(hints[0].grouping, by(&["pod"]));
        let hints = hints_of("sum by (pod)(timestamp(abs(x)))", range).await;
        assert_eq!(hints[0].func.as_deref(), Some("abs"));
        assert_eq!(hints[0].grouping, by(&["pod"]));

        // An ungrouped aggregation says so: it reads no label.
        let hints = hints_of("sum(rate(x[5m]))", range).await;
        assert_eq!(hints[0].grouping, by(&[]));

        // `without` names what to drop, not what to read.
        let hints = hints_of("count without (pod)(rate(x[5m]))", range).await;
        assert_eq!(hints[0].grouping, None);

        // A nested aggregation starts its own grouping.
        let hints = hints_of("sum by (a)(max by (a, b)(rate(x[5m])))", range).await;
        assert_eq!(hints[0].grouping, by(&["a", "b"]));

        // Vector matching reads labels the `by` does not name.
        let hints = hints_of("sum by (pod)(rate(x[5m]) * on() group_left y)", range).await;
        assert_eq!(hints.len(), 2);
        assert!(hints.iter().all(|h| h.grouping.is_none()));

        for query in [
            "sum by (pod)(absent(x))",
            "sum by (pod)(absent_over_time(x[5m]))",
            "sum by (pod)(sort(x))",
            "rate(x[5m])",
        ] {
            let hints = hints_of(query, range).await;
            assert_eq!(hints[0].grouping, None, "{query}");
        }
    }

    /// Plan `query` against an empty store: every guard here runs before
    /// the store is asked for anything, and each node carries its own
    /// functions, so none need registering.
    async fn plan_of(query: &str, range: RangeQuery) -> Result<LogicalPlan, EngineError> {
        let ctx = SessionContext::new();
        let source = MemorySeriesSource::default();
        // `limitk` and `limit_ratio` are experimental aggregators, and the
        // guards below are about planning them, not about whether they parse.
        let expr: Expr = promql_parser::Parser::new(promql_parser::ParserOptions {
            enable_experimental_functions: true,
            ..Default::default()
        })
        .parse_expr(query)
        .expect("query parses");
        plan(&ctx.state(), &source, &expr, &range).await
    }

    #[tokio::test]
    async fn a_query_with_more_steps_than_the_cap_is_a_query_error() {
        let range = RangeQuery::new(0, 1000 * 24 * 60 * 60 * 1000, 1);
        let err = plan_of("up", range).await.unwrap_err();
        assert!(matches!(err, EngineError::Query(_)), "{err}");
        let message = err.to_string();
        assert!(
            message.contains(&aggregate::MAX_STEPS.to_string()),
            "the message names the limit: {message}"
        );
    }

    #[tokio::test]
    async fn a_query_at_the_step_cap_is_planned() {
        let end = aggregate::MAX_STEPS as i64 - 1;
        assert!(plan_of("up", RangeQuery::new(0, end, 1)).await.is_ok());
        assert!(plan_of("up", RangeQuery::new(0, end + 1, 1)).await.is_err());
    }

    #[tokio::test]
    async fn an_at_modifier_out_of_range_is_a_query_error() {
        let range = RangeQuery::new(0, 60_000, 30_000);
        // Past `MAX_TIME_MS` but inside `i64` milliseconds, so the parser
        // accepts it; `1e30` never gets this far.
        let err = plan_of("up @ -4e15", range).await.unwrap_err();
        assert!(matches!(err, EngineError::Query(_)), "{err}");
        assert!(err.to_string().contains("@ modifier"), "{err}");

        let err = plan_of("rate(up[5m] @ 4e15)", range).await.unwrap_err();
        assert!(matches!(err, EngineError::Query(_)), "{err}");
        assert!(err.to_string().contains("@ modifier"), "{err}");
    }

    #[tokio::test]
    async fn an_at_modifier_at_a_real_timestamp_is_planned() {
        let range = RangeQuery::new(1_600_000_000_000, 1_600_000_060_000, 30_000);
        assert!(plan_of("up @ 1600000000", range).await.is_ok());
        assert!(plan_of("up @ start()", range).await.is_ok());
        assert!(plan_of("up offset 5m", range).await.is_ok());
    }

    #[test]
    fn a_term_the_scan_range_cannot_hold_is_rejected() {
        assert!(check_selector_bounds(None, 0).is_ok());
        assert!(check_selector_bounds(Some(0), 0).is_ok());
        assert!(check_selector_bounds(Some(MAX_TIME_MS), -MAX_TIME_MS).is_ok());

        assert!(check_selector_bounds(Some(i64::MIN), 0).is_err());
        assert!(check_selector_bounds(Some(i64::MAX), 0).is_err());
        assert!(check_selector_bounds(None, i64::MAX).is_err());
        assert!(check_time_bound("the range", i64::MAX).is_err());

        let message = check_selector_bounds(Some(i64::MIN), 0)
            .unwrap_err()
            .to_string();
        assert!(message.contains("@ modifier timestamp"), "{message}");
        assert!(message.contains(&MAX_TIME_MS.to_string()), "{message}");
    }

    /// `year()` has no series to read, so no store is asked: the
    /// evaluation time comes from the grid the planner already holds.
    /// `time()` at the top of a query is the scalar table and nothing
    /// else; `scalar(up)` is typed the same and is still the gap it was.
    #[tokio::test]
    async fn a_scalar_returning_call_is_the_scalar_table() {
        let range = RangeQuery::new(0, 60_000, 30_000);
        assert!(hints_of("time()", range).await.is_empty());
        assert!(hints_of("pi()", range).await.is_empty());
        let err = plan_of("scalar(up)", range).await.unwrap_err();
        assert!(
            matches!(&err, EngineError::Unsupported(f) if f == "the scalar function"),
            "{err}"
        );
    }

    /// The store hears `timestamp` only when the selector is what it
    /// reads; through another function it hears that function, since
    /// the time then comes from the step and not from the store.
    #[tokio::test]
    async fn timestamp_reaches_the_store_only_over_a_bare_selector() {
        let range = RangeQuery::new(0, 60_000, 30_000);
        for query in ["timestamp(x)", "timestamp(((x)))"] {
            let hints = hints_of(query, range).await;
            assert_eq!(hints.len(), 1, "{query}");
            assert_eq!(hints[0].func.as_deref(), Some("timestamp"), "{query}");
        }
        let hints = hints_of("timestamp(abs(x))", range).await;
        assert_eq!(hints[0].func.as_deref(), Some("abs"));
    }

    #[tokio::test]
    async fn a_date_function_without_a_vector_asks_no_store() {
        let range = RangeQuery::new(0, 60_000, 30_000);
        assert!(hints_of("year()", range).await.is_empty());
        let hints = hints_of("year(up)", range).await;
        assert_eq!(hints.len(), 1);
        assert_eq!(hints[0].func.as_deref(), Some("year"));
    }

    #[tokio::test]
    async fn every_supported_aggregation_plans() {
        let range = RangeQuery::new(0, 60_000, 30_000);
        for op in [
            "sum", "avg", "count", "min", "max", "group", "stddev", "stdvar",
        ] {
            let query = format!("{op} by (pod) (up)");
            assert!(plan_of(&query, range).await.is_ok(), "{query}");
        }
        for op in [
            "topk",
            "bottomk",
            "quantile",
            "count_values",
            "limitk",
            "limit_ratio",
        ] {
            let query = format!("{op}(1, up)");
            let err = plan_of(&query, range).await.unwrap_err();
            assert!(matches!(err, EngineError::Unsupported(_)), "{query}: {err}");
            assert!(err.to_string().contains(op), "{query}: {err}");
        }
    }

    /// Upstream's parser refuses `1 > 1` because it cannot mean the
    /// filter a comparison usually is. Ours accepts it, so the rule is
    /// held here, in upstream's words.
    #[tokio::test]
    async fn a_comparison_between_scalars_needs_the_bool_modifier() {
        let range = RangeQuery::new(0, 60_000, 30_000);
        for query in ["1 > 1", "(1 > 1)", "time() <= 100", "2 * (1 == 1)"] {
            let err = plan_of(query, range).await.unwrap_err();
            assert_eq!(
                err.to_string(),
                "comparisons between scalars must use BOOL modifier",
                "{query}"
            );
            assert!(matches!(err, EngineError::Query(_)), "{query}: {err}");
        }

        // With the modifier, or with a vector on either side, the
        // comparison is a query the engine answers.
        assert!(plan_of("1 > bool 1", range).await.is_ok());
        assert!(plan_of("up > 1", range).await.is_ok());
        assert!(plan_of("up and up", range).await.is_ok());
    }

    /// The rest of upstream's binary arm, in upstream's words, and in
    /// the order the walk reaches them.
    #[tokio::test]
    async fn the_parse_time_rules_are_upstreams_and_the_inner_one_wins() {
        let range = RangeQuery::new(0, 60_000, 30_000);
        let fails = |query: &'static str| async move {
            plan_of(query, range).await.unwrap_err().to_string()
        };

        assert_eq!(
            fails("up + bool 1").await,
            "bool modifier can only be used on comparison operators"
        );
        // A scalar operand has no labels to match on -- but only the
        // named ones are refused, as upstream refuses them.
        assert_eq!(
            fails("up + on(pod) 1").await,
            "vector matching only allowed between instant vectors"
        );
        assert!(plan_of("up + on() 1", range).await.is_ok());
        // Both operands are vectors, so this reaches the other arm.
        assert_eq!(
            fails("up and on(pod) group_left() up").await,
            r#"no grouping allowed for "and" operation"#
        );
        assert_eq!(
            fails("up and 1").await,
            r#"set operator "and" not allowed in binary scalar expression"#
        );
        // The walk goes down before it checks the node it is on, so the
        // comparison inside is what the query is refused for.
        assert_eq!(
            fails("(1 > 1) and up").await,
            "comparisons between scalars must use BOOL modifier"
        );
    }

    /// A string or a range vector as an operand is a query error, not a
    /// gap: no operator upstream has ever taken one.
    #[tokio::test]
    async fn a_binary_operand_must_be_a_scalar_or_an_instant_vector() {
        let range = RangeQuery::new(0, 60_000, 30_000);
        for query in [r#""foo" + 1"#, r#"1 + "foo""#, "foo[5m] + 1", "1 + foo[5m]"] {
            let err = plan_of(query, range).await.unwrap_err();
            assert_eq!(
                err.to_string(),
                "binary expression must contain only scalar and instant vector types",
                "{query}"
            );
            assert!(matches!(err, EngineError::Query(_)), "{query}: {err}");
        }
    }

    #[tokio::test]
    async fn a_step_that_is_not_positive_is_a_query_error() {
        let err = plan_of("up", RangeQuery::new(0, 60_000, 0))
            .await
            .unwrap_err();
        assert!(matches!(err, EngineError::Query(_)), "{err}");
    }
}
