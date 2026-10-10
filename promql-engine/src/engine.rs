//! The engine: parse, plan, execute.
//!
//! Owns a DataFusion [`SessionContext`] with the PromQL functions
//! registered. A DataFusion query is async end to end; callers that are
//! not — the conformance harness runs plain closures — construct the
//! engine with [`Engine::blocking`], which adds a Tokio runtime to block
//! on. An engine built with [`Engine::new`] has none and is safe to hold
//! and drop inside someone else's runtime.
//!
//! A range query hands back Arrow batches in the canonical schema
//! ([`crate::series`]), not decoded [`Series`](crate::Series): this is a
//! DataFusion engine, and Arrow in, Arrow out lets a caller stay on
//! `RecordBatch` end to end instead of paying to materialize Rust values
//! it may only re-encode. Applied before the batches come back are
//! [`series::drop_empty`] — see its doc for why that can't live in the
//! plan — and `labelset::reject_same_labelset` and
//! `labelset::sort_by_labelset`, Prometheus's own post-evaluation passes.
//!
//! Between planning and execution the physical plan goes through
//! [`check_selector_plans`]. The selector and range aggregates bound memory
//! only in DataFusion's Sorted mode, and DataFusion drops to Linear mode
//! without an error; the check turns that into a refused query.

use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion::physical_plan::coop::CooperativeExec;
use datafusion::physical_plan::joins::{
    HashJoinExec, PartitionMode, SortMergeJoinExec, StreamJoinPartitionMode, SymmetricHashJoinExec,
};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion::physical_plan::union::InterleaveExec;
use datafusion::physical_plan::{self, ExecutionPlan, InputOrderMode, Partitioning};
use datafusion::prelude::{SessionConfig, SessionContext};

use promql_parser::{Parser, ParserOptions};

use crate::error::EngineError;
use crate::labelset;
pub use crate::plan::RangeQuery;
use crate::series::{self, LABELS};
use crate::source::{SeriesSetExec, SeriesSource};
use crate::value_type::{value_type, ValueType};
use crate::{absent, aggregate, binary, elementwise, labels, matrix, range, selector, sort};

/// The knobs of Prometheus's `promql.EngineOpts` that this engine honours,
/// one field each, under a name that follows upstream's.
///
/// `Default` is a stock Prometheus: every experimental option off. A new
/// option goes here with its upstream default, so `Engine::new` keeps
/// behaving as it did and a caller opts in by name:
/// `EngineOptions { parser: ParserOptions::all(), ..Default::default() }`.
#[derive(Debug, Clone, Default)]
pub struct EngineOptions {
    /// What the engine's parser accepts. Every query path parses with
    /// these, so an experimental construct is refused with upstream's
    /// message unless its gate is on here.
    pub parser: ParserOptions,
}

/// What an instant query answers with: the type its expression has, as
/// Prometheus's `parser.Value`. `Vector` and `Matrix` stay Arrow batches in
/// the canonical schema ([`crate::series`]); a scalar and a string are not
/// series, so they come back as plain values.
///
/// A `Vector` row holds one sample, stamped with the evaluation time. A
/// `Matrix` row holds the samples of a range selector under their own
/// timestamps, one row per chunk, so a label set may span several rows
/// that [`series::coalesce`] joins.
#[derive(Debug, Clone)]
pub enum InstantResult {
    Vector(Vec<RecordBatch>),
    Matrix(Vec<RecordBatch>),
    Scalar(f64),
    String(String),
}

pub struct Engine {
    ctx: SessionContext,
    rt: Option<tokio::runtime::Runtime>,
    options: EngineOptions,
}

/// A failure out of DataFusion as the caller should read it.
///
/// A UDAF has no way to fail but a `DataFusionError`, so the matching
/// errors [`crate::binary`] raises arrive looking exactly like a bug in
/// this crate. They are neither: Prometheus refuses the same query with
/// the same words, so they are an answer. Named one by one rather than
/// lifting every execution error, which would turn the next real bug
/// into a query result. The `instant-label-functions` branch hangs its
/// own same-labelset error on this hook, so the merge adds a text
/// rather than a mechanism.
fn lift(error: DataFusionError) -> EngineError {
    match error.find_root() {
        DataFusionError::Execution(message) if binary::is_matching_error(message) => {
            EngineError::Query(message.clone())
        }
        _ => EngineError::from(error),
    }
}

impl Engine {
    /// An engine for async callers, with stock Prometheus's options. Use the
    /// `*_async` methods.
    pub fn new() -> Self {
        Self::with_options(EngineOptions::default())
    }

    /// [`Self::new`] under `options`.
    pub fn with_options(options: EngineOptions) -> Self {
        // A scan split by byte range cuts through a series. Round-robin is
        // off out of caution: with the selector SinglePartitioned over
        // Hash(labels) DataFusion only places it above the selector, over
        // finished series, but nothing but that requirement keeps it there.
        // Hash repartitioning for aggregations is what lets the selector run
        // SinglePartitioned per store partition. The plan then ends in
        // several partitions, and `range_query_async` restores the order.
        let mut config = SessionConfig::new()
            .with_round_robin_repartition(false)
            .with_repartition_file_scans(false)
            .with_repartition_aggregations(true);
        // Below this many input partitions EnforceDistribution re-hashes a
        // satisfied Hash(labels) input anyway, to reach target_partitions,
        // which splits the selector into Partial and FinalPartitioned.
        config.options_mut().optimizer.subset_repartition_threshold = 1;
        let ctx = SessionContext::new_with_config(config);
        ctx.register_udaf(selector::udaf());
        ctx.register_udf(labels::udf());
        ctx.register_udaf(aggregate::udaf());
        ctx.register_udaf(range::udaf());
        ctx.register_udaf(absent::udaf());
        ctx.register_udf(elementwise::udf());
        ctx.register_udf(matrix::udf());
        ctx.register_udaf(binary::udaf());
        ctx.register_udf(sort::Natural::udf());
        ctx.register_udf(sort::Set::udf());
        Self {
            ctx,
            rt: None,
            options,
        }
    }

    /// An engine with its own runtime, for synchronous callers. The blocking
    /// methods refuse to run from inside a Tokio runtime; async callers use
    /// the `*_async` methods.
    pub fn blocking() -> Result<Self, EngineError> {
        Self::blocking_with_options(EngineOptions::default())
    }

    /// [`Self::blocking`] under `options`.
    pub fn blocking_with_options(options: EngineOptions) -> Result<Self, EngineError> {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .map_err(EngineError::RuntimeBuild)?;
        // Not `..Self::new()`: struct update cannot move out of a Drop type.
        let mut engine = Self::with_options(options);
        engine.rt = Some(rt);
        Ok(engine)
    }

    /// Plan a query without running it, for inspection.
    ///
    /// Like every `*_async` method, this must run on a Tokio runtime:
    /// DataFusion spawns onto it once a plan has more than one partition, and
    /// panics ("no reactor running") under any other executor.
    pub async fn plan_async(
        &self,
        source: &dyn SeriesSource,
        query: &str,
        range: &RangeQuery,
    ) -> Result<LogicalPlan, EngineError> {
        // The one place a query string is parsed: `range_query`,
        // `range_query_async` and `physical_plan_async` all plan through here.
        let expr = self.parse(query)?;
        crate::plan::plan(&self.ctx.state(), source, &expr, range).await
    }

    /// Plan an instant query without running it, for inspection. Like
    /// every `*_async` method, this must run on a Tokio runtime.
    pub async fn plan_instant_async(
        &self,
        source: &dyn SeriesSource,
        query: &str,
        at_ms: i64,
    ) -> Result<LogicalPlan, EngineError> {
        let expr = self.parse(query)?;
        crate::plan::plan_instant(&self.ctx.state(), source, &expr, at_ms).await
    }

    fn parse(&self, query: &str) -> Result<promql_parser::ast::Expr, EngineError> {
        Parser::new(self.options.parser)
            .parse_expr(query)
            .map_err(EngineError::Parse)
    }

    /// The optimized `ExecutionPlan` a range query runs, for inspection or
    /// for a caller that streams it instead of paying for
    /// [`Self::range_query_async`]'s buffering, as the memory bench does.
    /// Must run on a Tokio runtime, see [`Self::plan_async`].
    /// It has passed [`check_selector_plans`], so it is exactly what
    /// [`Self::range_query_async`] executes.
    pub async fn physical_plan_async(
        &self,
        source: &dyn SeriesSource,
        query: &str,
        range: &RangeQuery,
    ) -> Result<Arc<dyn ExecutionPlan>, EngineError> {
        let plan = self.plan_async(source, query, range).await?;
        self.lower(plan).await
    }

    async fn lower(&self, plan: LogicalPlan) -> Result<Arc<dyn ExecutionPlan>, EngineError> {
        let exec = self
            .ctx
            .execute_logical_plan(plan)
            .await?
            .create_physical_plan()
            .await?;
        check_selector_plans(&exec)?;
        Ok(exec)
    }

    /// Evaluate a range query. Batches are in the canonical schema
    /// ([`crate::series`]) with empty series already dropped, series in
    /// Prometheus's label-set order unless an ordering function set it; call
    /// [`series::decode`] to get [`Series`](crate::Series) instead.
    ///
    /// Must run on a Tokio runtime, see [`Self::plan_async`]. A single-partition
    /// store hides the requirement; a multi-partition one panics without it.
    pub async fn range_query_async(
        &self,
        source: &dyn SeriesSource,
        query: &str,
        range: &RangeQuery,
    ) -> Result<Vec<RecordBatch>, EngineError> {
        let plan = self.plan_async(source, query, range).await?;
        self.run(plan).await
    }

    /// Evaluate a query at one instant and answer with the type the
    /// *expression* has: `execEvalStmt` (`promql/engine.go` at 83962c35)
    /// runs an instant query as a range evaluation of one step and then
    /// reads the matrix back by `Expr.Type()`. The result of the
    /// evaluation cannot say which it was, since `scalar(x)` and `sum(x)`
    /// both come back as one series with no labels, which is why the type
    /// comes from [`value_type`] before anything runs.
    ///
    /// A string is not a series, so it has no place in the engine's
    /// batches and is answered without a plan. Like the range query, this
    /// must run on a Tokio runtime, see [`Self::plan_async`].
    pub async fn instant_query_async(
        &self,
        source: &dyn SeriesSource,
        query: &str,
        at_ms: i64,
    ) -> Result<InstantResult, EngineError> {
        let expr = self.parse(query)?;
        if let Some(s) = crate::plan::string_literal(&expr) {
            return Ok(InstantResult::String(s.to_owned()));
        }
        let plan = crate::plan::plan_instant(&self.ctx.state(), source, &expr, at_ms).await?;
        let batches = self.run(plan).await?;
        match value_type(&expr) {
            ValueType::Vector => Ok(InstantResult::Vector(batches)),
            ValueType::Matrix => Ok(InstantResult::Matrix(batches)),
            // `Scalar{V: mat[0].Floats[0].F}`: a scalar is planned as one
            // unlabelled series with a sample at every step, so the one
            // step has one sample. Anything else is a planner bug and is
            // said so rather than answered as a number.
            ValueType::Scalar => {
                let decoded = series::decode(&batches)?;
                match decoded.as_slice() {
                    [only] if only.values().len() == 1 => {
                        Ok(InstantResult::Scalar(only.values()[0]))
                    }
                    other => Err(EngineError::Schema(format!(
                        "a scalar query produced {} series instead of one sample",
                        other.len()
                    ))),
                }
            }
            ValueType::String => Err(EngineError::Schema(
                "a string query that is not a literal reached the planner".into(),
            )),
        }
    }

    /// [`Self::instant_query_async`], blocking on the engine's own runtime,
    /// under the refusal [`Self::range_query`] makes.
    pub fn instant_query(
        &self,
        source: &dyn SeriesSource,
        query: &str,
        at_ms: i64,
    ) -> Result<InstantResult, EngineError> {
        self.outside_runtime("instant_query")?
            .block_on(self.instant_query_async(source, query, at_ms))
    }

    /// Execute a planned query and apply the passes Prometheus runs on
    /// every result.
    async fn run(&self, plan: LogicalPlan) -> Result<Vec<RecordBatch>, EngineError> {
        // Only sort, sort_desc and sort_by_label plan a Sort, and only at
        // the root of an instant query: their order is the result, and
        // Prometheus orders by label set only a matrix.
        let ordered = matches!(plan, LogicalPlan::Sort(_));
        let exec = self.lower(plan).await?;
        let batches = physical_plan::collect(exec, self.ctx.task_ctx())
            .await
            .map_err(lift)?;
        // One `collect()` shares a single output schema across all its
        // batches, so validating it once and comparing later batches by
        // pointer skips the redundant re-walk of the label fields.
        let mut validated = None;
        let batches: Vec<RecordBatch> = batches
            .iter()
            .map(|b| {
                let schema = b.schema();
                if !validated.as_ref().is_some_and(|s| Arc::ptr_eq(s, &schema)) {
                    series::validate(&schema)?;
                }
                validated = Some(schema);
                Ok(series::drop_empty(b))
            })
            .collect::<Result<_, EngineError>>()?;
        labelset::reject_same_labelset(&batches)?;
        if ordered {
            return Ok(batches);
        }
        labelset::sort_by_labelset(&batches)
    }

    /// [`Self::range_query_async`], blocking on the engine's own runtime.
    ///
    /// Refused with [`EngineError::Runtime`] on any thread that has a Tokio
    /// runtime context, `spawn_blocking` threads included, where blocking
    /// would be legal but Tokio gives no public way to tell them apart. Use
    /// `range_query_async` or a plain thread there.
    pub fn range_query(
        &self,
        source: &dyn SeriesSource,
        query: &str,
        range: &RangeQuery,
    ) -> Result<Vec<RecordBatch>, EngineError> {
        self.outside_runtime("range_query")?
            .block_on(self.range_query_async(source, query, range))
    }

    /// The runtime a blocking method runs on, unless this thread is
    /// already inside one.
    fn outside_runtime(&self, method: &str) -> Result<&tokio::runtime::Runtime, EngineError> {
        // `block_on` panics inside a runtime, on a worker thread of either
        // flavor; an error lets the caller find the `*_async` method instead.
        if tokio::runtime::Handle::try_current().is_ok() {
            return Err(EngineError::Runtime(format!(
                "{method} blocks and cannot run inside a Tokio runtime, spawn_blocking threads \
                 included; use {method}_async or a plain thread"
            )));
        }
        self.runtime()
    }

    fn runtime(&self) -> Result<&tokio::runtime::Runtime, EngineError> {
        self.rt.as_ref().ok_or_else(|| {
            EngineError::Runtime(
                "this engine has no runtime; build it with Engine::blocking() or use the *_async methods".into(),
            )
        })
    }
}

/// Refuse a plan in which a selector or range-function aggregate would not
/// hold exactly one open series per partition.
///
/// The session flags in [`Engine::new`] produce such plans today, but
/// nothing makes DataFusion keep doing so: an upgrade or a new optimizer
/// rule that loses `InputOrderMode::Sorted` does not fail, it buffers every
/// series of the scan and answers out of order. `EvalSeries`' contiguity
/// checks catch the rows that then arrive interleaved, but only for data
/// that happens to interleave, so the shape is checked here on every plan.
///
/// Accepted: a Partial, Single or SinglePartitioned aggregate, Sorted,
/// whose input reaches [`SeriesSetExec`] through nothing but projections
/// and cooperative yields; and a Final aggregate, Sorted, over such a Partial through an
/// order-preserving merge or hash repartition. Anything that deals rows
/// out anew below the Partial — round-robin, a hash split of chunks —
/// can put one series in two partitions, each of which folds half of it.
///
/// A sort on a `labels` column is refused wherever it sits: Arrow cannot
/// sort a struct, so the plan would otherwise fail inside the sort.
///
/// So is a partitioned join, or an `InterleaveExec`, with an input whose
/// partitioning is still the one [`SeriesSetExec`] declares. That
/// declaration names the right key but not DataFusion's hash, so partition
/// i of the store is not partition i of anything DataFusion hashed, nor of
/// another store's. Aggregates only need a key in one partition and are
/// fine; a join matches partitions by index and would silently miss series,
/// and DataFusion turns a `UnionExec` into an `InterleaveExec` whenever
/// every child declares the same partitioning
/// (`enforce_distribution.rs:1458-1485`, `union.rs:664-676`), which is true
/// of two selectors over the same store: it would then zip partition i of
/// one scan with partition i of the other and hand a downstream Sorted
/// consumer the same label set twice.
pub fn check_selector_plans(plan: &Arc<dyn ExecutionPlan>) -> Result<(), EngineError> {
    let refuse = |why: &str| {
        EngineError::Query(format!(
            "refusing a plan in which {why}:\n{}",
            crate::explain::render_physical(plan.as_ref())
        ))
    };
    let mut stack = vec![plan];
    while let Some(node) = stack.pop() {
        stack.extend(node.children());
        if joins_by_partition(node) && node.children().into_iter().any(trusts_store_partitioning) {
            return Err(refuse(
                "a partitioned join relies on the store's partitioning",
            ));
        }
        if node.is::<InterleaveExec>() && node.children().into_iter().any(trusts_store_partitioning)
        {
            return Err(refuse("an interleave relies on the store's partitioning"));
        }
        if let Some(sort) = node.downcast_ref::<SortExec>() {
            if sort.expr().iter().any(|e| is_labels(e.expr.as_ref())) {
                return Err(refuse("a sort orders by labels"));
            }
            continue;
        }
        let Some(agg) = selector_aggregate(node) else {
            continue;
        };
        if agg.input_order_mode() != &InputOrderMode::Sorted {
            return Err(refuse("a selector aggregate does not run Sorted"));
        }
        let fed = match agg.mode() {
            AggregateMode::Partial | AggregateMode::Single | AggregateMode::SinglePartitioned => {
                reaches(agg.input(), &|n| n.is::<SeriesSetExec>(), &|n| {
                    n.is::<ProjectionExec>() || n.is::<CooperativeExec>()
                })
            }
            AggregateMode::Final | AggregateMode::FinalPartitioned => reaches(
                agg.input(),
                &|n| selector_aggregate(n).is_some_and(|a| *a.mode() == AggregateMode::Partial),
                &|n| {
                    n.is::<ProjectionExec>()
                        || n.is::<CooperativeExec>()
                        || n.is::<SortPreservingMergeExec>()
                        || n.downcast_ref::<RepartitionExec>().is_some_and(|r| {
                            r.preserve_order() && matches!(r.partitioning(), Partitioning::Hash(..))
                        })
                },
            ),
            AggregateMode::PartialReduce => false,
        };
        if !fed {
            return Err(refuse(
                "a selector aggregate's input does not come straight from the store",
            ));
        }
    }
    Ok(())
}

fn selector_aggregate(node: &Arc<dyn ExecutionPlan>) -> Option<&AggregateExec> {
    node.downcast_ref::<AggregateExec>().filter(|agg| {
        agg.aggr_expr()
            .iter()
            .any(|e| [selector::NAME, range::NAME].contains(&e.fun().name()))
    })
}

fn joins_by_partition(node: &Arc<dyn ExecutionPlan>) -> bool {
    node.downcast_ref::<HashJoinExec>()
        .is_some_and(|j| *j.partition_mode() == PartitionMode::Partitioned)
        || node
            .downcast_ref::<SymmetricHashJoinExec>()
            .is_some_and(|j| j.partition_mode() == StreamJoinPartitionMode::Partitioned)
        || node.is::<SortMergeJoinExec>()
}

fn is_labels(expr: &dyn datafusion::physical_expr::PhysicalExpr) -> bool {
    expr.downcast_ref::<Column>()
        .is_some_and(|c| c.name() == LABELS)
}

/// Whether `node` still carries [`SeriesSetExec`]'s declared partitioning
/// through nothing but projections, cooperative yields, a selector
/// aggregate, or a single-child node DataFusion reports as leaving its
/// child's `output_partitioning()` unchanged — `FilterExec`,
/// `GlobalLimitExec`, `LocalLimitExec` and `CoalesceBatchesExec` among
/// them. Shared by the join guard and the interleave guard: one trusting
/// side is enough to refuse either, because DataFusion would hash only the
/// other side to match it.
fn trusts_store_partitioning(node: &Arc<dyn ExecutionPlan>) -> bool {
    reaches(node, &|n| n.is::<SeriesSetExec>(), &|n| {
        n.is::<ProjectionExec>()
            || n.is::<CooperativeExec>()
            || selector_aggregate(n).is_some()
            || passes_partitioning_through(n)
    })
}

/// Whether `node` has exactly one child and reports the same
/// `output_partitioning()` as that child, so a Hash(labels, n) declared
/// below it reaches above it unchanged. A positive rule on
/// `output_partitioning()` instead of naming node types: DataFusion
/// guarantees the property, not the list, and an allowlist only grows as
/// more pass-through operators turn up. Excludes `RepartitionExec`: its
/// whole purpose is to declare new partitioning, and `Partitioning::Hash`
/// equality does not distinguish a genuine re-hash of the same width from
/// a passthrough of the declaration this guard exists to catch.
fn passes_partitioning_through(node: &Arc<dyn ExecutionPlan>) -> bool {
    if node.is::<RepartitionExec>() {
        return false;
    }
    match node.children().as_slice() {
        [child] => node.properties().partitioning == child.properties().partitioning,
        _ => false,
    }
}

/// Whether `node`, or the single-child chain below it through `through`,
/// ends in a node that is `target`.
fn reaches(
    node: &Arc<dyn ExecutionPlan>,
    target: &dyn Fn(&Arc<dyn ExecutionPlan>) -> bool,
    through: &dyn Fn(&Arc<dyn ExecutionPlan>) -> bool,
) -> bool {
    let mut node = node;
    loop {
        if target(node) {
            return true;
        }
        match node.children().as_slice() {
            [child] if through(node) => node = child,
            _ => return false,
        }
    }
}

impl Drop for Engine {
    fn drop(&mut self) {
        // A plain drop of a `Runtime` waits for its blocking workers, which
        // Tokio forbids inside another runtime (an engine held in axum state
        // is dropped there at shutdown). Detaching avoids the panic but
        // abandons in-flight `spawn_blocking` tasks, so it is the fallback
        // for that case only: a sync caller keeps the waiting shutdown.
        if let Some(rt) = self.rt.take() {
            if tokio::runtime::Handle::try_current().is_ok() {
                rt.shutdown_background();
            } else {
                drop(rt);
            }
        }
    }
}

impl Default for Engine {
    fn default() -> Self {
        Self::new()
    }
}
