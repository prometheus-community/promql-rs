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
//! `labelset::finalize`: empty rows are dropped, duplicate labels checked
//! through borrowed samples and ordinary results organized by label set.
//! Explicit root instant sorts retain their result order.

use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::error::DataFusionError;
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::execution_plan::reset_plan_states;
use datafusion::physical_plan::{self, ExecutionPlan};
use datafusion::prelude::{SessionConfig, SessionContext};

use promql_parser::{Parser, ParserOptions};

use crate::error::EngineError;
use crate::labelset;
pub use crate::plan::RangeQuery;
use crate::series;
use crate::source::SeriesSource;
use crate::value_type::{value_type, ValueType};
use crate::{absent, aggregate, binary, elementwise, labels, matrix, sort, to_scalar};

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

/// A planned PromQL query, including the semantics needed to finalize its result.
///
/// Preparation selects from the source once per selector. Each
/// [`Engine::execute_async`] resets a separate execution tree, including protected
/// source-owned plans, so this value can be executed repeatedly or concurrently.
/// Source operators must implement DataFusion's execution-state reset contract.
#[derive(Debug)]
pub struct PreparedQuery {
    physical_plan: Arc<dyn ExecutionPlan>,
    result_order: ResultOrder,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ResultOrder {
    LabelSet,
    Preserve,
}

impl PreparedQuery {
    /// The optimized physical template, for diagnostics or raw streaming.
    ///
    /// Raw execution bypasses PromQL result finalization. Callers using DataFusion
    /// directly must reset execution state with `reset_plan_states` before each
    /// execution, including the first; cloning the Arc does not reset exchanges.
    /// [`Engine::execute_async`] does this automatically. Its execution metrics
    /// belong to the reset tree, not to this template.
    pub fn physical_plan(&self) -> &Arc<dyn ExecutionPlan> {
        &self.physical_plan
    }
}

/// A failure out of DataFusion as the caller should read it.
///
/// A UDAF has no way to fail but a `DataFusionError`, so the matching
/// errors [`crate::binary`] raises arrive looking exactly like a bug in
/// this crate. They are neither: Prometheus refuses the same query with
/// the same words, so they are an answer. Named one by one rather than
/// lifting every execution error, which would turn the next real bug
/// into a query result.
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
        // Raw chunks stay in the store's partitions; evaluation requests no
        // shuffle. Upper aggregations may hash their completed series rows.
        // Byte-range scan splitting could cut through a source identity.
        let mut config = SessionConfig::new()
            .with_round_robin_repartition(false)
            .with_repartition_file_scans(false)
            .with_repartition_aggregations(true);
        // Do not rehash an already satisfied upper-aggregation distribution
        // merely to reach the session's target partition count.
        config.options_mut().optimizer.subset_repartition_threshold = 1;
        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_config(config)
            .with_query_planner(Arc::new(crate::series_eval::SeriesEvalQueryPlanner))
            .build();
        let ctx = SessionContext::new_with_state(state);
        ctx.register_udf(labels::udf());
        ctx.register_udaf(aggregate::udaf());
        ctx.register_udaf(absent::udaf());
        ctx.register_udaf(to_scalar::udaf());
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

    /// Prepare a range query without executing it or losing its result-order policy.
    ///
    /// Selects once per selector; executing the prepared query does not select or
    /// plan again. Matrix label ordering follows Prometheus's `Engine.execEvalStmt`
    /// (83962c35); a one-step grid retains the direct logical-root sort policy.
    /// Must run on a Tokio runtime, see [`Self::plan_async`].
    pub async fn prepare_async(
        &self,
        source: &dyn SeriesSource,
        query: &str,
        range: &RangeQuery,
    ) -> Result<PreparedQuery, EngineError> {
        let plan = self.plan_async(source, query, range).await?;
        self.prepare_plan(plan).await
    }

    // Both public query paths capture ordering before optimization and execute
    // through the same reset/finalization boundary, including raw instant matrices.
    async fn prepare_plan(&self, plan: LogicalPlan) -> Result<PreparedQuery, EngineError> {
        // Prometheus Engine.execEvalStmt (83962c35) label-orders matrices,
        // while explicit instant sorts can order vectors. Preserve the existing
        // direct logical-root policy before optimization: a sort below a
        // projection or aggregate is internal work, not a result-order promise.
        let result_order = if matches!(&plan, LogicalPlan::Sort(_)) {
            ResultOrder::Preserve
        } else {
            ResultOrder::LabelSet
        };
        Ok(PreparedQuery {
            physical_plan: self.lower(plan).await?,
            result_order,
        })
    }

    async fn lower(&self, plan: LogicalPlan) -> Result<Arc<dyn ExecutionPlan>, EngineError> {
        let exec = self
            .ctx
            .execute_logical_plan(plan)
            .await?
            .create_physical_plan()
            .await?;
        Ok(exec)
    }

    /// Evaluate a range query. Batches are in the canonical schema
    /// ([`crate::series`]) with empty series already dropped, series in
    /// Prometheus's label-set order unless a root instant ordering function set it; call
    /// [`crate::series::decode`] to get [`Series`](crate::Series) instead.
    ///
    /// Must run on a Tokio runtime, see [`Self::plan_async`]. A single-partition
    /// store hides the requirement; a multi-partition one panics without it.
    pub async fn range_query_async(
        &self,
        source: &dyn SeriesSource,
        query: &str,
        range: &RangeQuery,
    ) -> Result<Vec<RecordBatch>, EngineError> {
        let prepared = self.prepare_async(source, query, range).await?;
        self.execute_async(&prepared).await
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
        let prepared = self.prepare_plan(plan).await?;
        let batches = self.execute_async(&prepared).await?;
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

    /// Execute a prepared query with [`Self::range_query_async`]'s finalization.
    ///
    /// Each call resets a separate execution tree before collecting, even on the
    /// first execution. Repeated or concurrent calls and a call after cancellation
    /// do not reuse consumed exchange channels, including source-owned exchanges.
    /// Source operators must honor DataFusion's `reset_state` contract.
    ///
    /// Explicit instant-query root sorts retain their order; other results are
    /// sorted by Prometheus label-set order. Must run on a Tokio runtime.
    pub async fn execute_async(
        &self,
        prepared: &PreparedQuery,
    ) -> Result<Vec<RecordBatch>, EngineError> {
        // Never execute the template: resetting only after collection would
        // miss cancellation and let concurrent executions share exchange state.
        // SeriesSetExec::reset_state also resets its protected source subtree.
        let exec = reset_plan_states(Arc::clone(prepared.physical_plan())).map_err(lift)?;
        let batches = physical_plan::collect(exec, self.ctx.task_ctx())
            .await
            .map_err(lift)?;
        labelset::finalize(&batches, prepared.result_order == ResultOrder::Preserve)
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

#[cfg(test)]
mod lifecycle;

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;
    use datafusion::catalog::Session;
    use promql_parser::ast::LabelMatcher;

    use super::*;
    use crate::{series, MemorySeriesSource, SelectHints, Series};

    #[derive(Debug)]
    struct CountedSource {
        inner: MemorySeriesSource,
        selections: AtomicUsize,
    }

    #[async_trait]
    impl SeriesSource for CountedSource {
        async fn select(
            &self,
            state: &dyn Session,
            matchers: &[LabelMatcher],
            hints: SelectHints,
        ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
            self.selections.fetch_add(1, Ordering::Relaxed);
            self.inner.select(state, matchers, hints).await
        }
    }

    #[tokio::test]
    async fn collector_preserves_explicit_instant_order_but_label_orders_matrices() {
        let source = MemorySeriesSource::try_new(
            [("cpu20", 1.0), ("cpu3", 2.0), ("cpu100", 3.0)]
                .into_iter()
                .map(|(pod, value)| {
                    Series::new(&[("__name__", "x"), ("pod", pod)], vec![0], vec![value]).unwrap()
                })
                .collect(),
        )
        .unwrap()
        .partitions(NonZeroUsize::new(3).unwrap())
        .rows_per_batch(NonZeroUsize::new(1).unwrap());
        let engine = Engine::with_options(EngineOptions {
            parser: ParserOptions::all(),
        });
        for (query, instant_pods, root_sort) in [
            ("x", ["cpu100", "cpu20", "cpu3"], false),
            ("sum by (pod) (x)", ["cpu100", "cpu20", "cpu3"], false),
            ("sort(x)", ["cpu20", "cpu3", "cpu100"], true),
            ("(sort(x))", ["cpu20", "cpu3", "cpu100"], true),
            ("sort_desc(x)", ["cpu100", "cpu3", "cpu20"], true),
            (
                "sort_by_label(x, \"pod\")",
                ["cpu3", "cpu20", "cpu100"],
                true,
            ),
            (
                "sort_by_label_desc(x, \"pod\")",
                ["cpu100", "cpu20", "cpu3"],
                true,
            ),
            ("abs(sort(x))", ["cpu100", "cpu20", "cpu3"], false),
            ("abs((sort_desc(x)))", ["cpu100", "cpu20", "cpu3"], false),
            ("sum by (pod) (sort(x))", ["cpu100", "cpu20", "cpu3"], false),
            (
                "sum by (pod) (abs(sort_desc(x)))",
                ["cpu100", "cpu20", "cpu3"],
                false,
            ),
        ] {
            let InstantResult::Vector(batches) =
                engine.instant_query_async(&source, query, 0).await.unwrap()
            else {
                panic!("{query} must answer with an instant vector");
            };
            let rows = series::decode(&batches).unwrap();
            let pods: Vec<_> = rows.iter().map(|row| row.label("pod")).collect();
            assert_eq!(pods, instant_pods, "typed instant: {query}");

            for range in [RangeQuery::new(0, 0, 1), RangeQuery::new(0, 2, 1)] {
                let prepared = engine.prepare_async(&source, query, &range).await.unwrap();
                assert_eq!(
                    prepared.result_order,
                    if root_sort && range.start_ms == range.end_ms {
                        ResultOrder::Preserve
                    } else {
                        ResultOrder::LabelSet
                    },
                    "{query}, {range:?}"
                );
                let planned = engine.execute_async(&prepared).await.unwrap();
                let direct = engine
                    .range_query_async(&source, query, &range)
                    .await
                    .unwrap();
                for result in [planned, direct] {
                    let rows = series::decode(&result).unwrap();
                    let pods: Vec<_> = rows.iter().map(|row| row.label("pod")).collect();
                    let expected = if range.start_ms == range.end_ms {
                        instant_pods
                    } else {
                        ["cpu100", "cpu20", "cpu3"]
                    };
                    assert_eq!(pods, expected, "{query}, {range:?}");
                }
            }
        }
    }

    #[tokio::test]
    async fn inspected_plan_execution_finalizes_results_without_selecting_again() {
        let source = CountedSource {
            inner: MemorySeriesSource::try_new(
                [("cpu20", 1.0), ("cpu3", 2.0), ("cpu100", 3.0)]
                    .into_iter()
                    .map(|(pod, value)| {
                        Series::new(&[("__name__", "x"), ("pod", pod)], vec![0], vec![value])
                            .unwrap()
                    })
                    .collect(),
            )
            .unwrap()
            .partitions(NonZeroUsize::new(3).unwrap())
            .rows_per_batch(NonZeroUsize::new(1).unwrap()),
            selections: AtomicUsize::new(0),
        };
        let engine = Engine::with_options(EngineOptions {
            parser: ParserOptions::all(),
        });
        for range in [RangeQuery::new(0, 0, 1), RangeQuery::new(0, 2, 1)] {
            for query in [
                "x",
                "sum by (pod) (x)",
                "sort(x)",
                "sort_desc(x)",
                "sort_by_label(x, \"pod\")",
                "sort_by_label_desc(x, \"pod\")",
            ] {
                let expected = engine
                    .range_query_async(&source, query, &range)
                    .await
                    .unwrap();
                let prepared = engine.prepare_async(&source, query, &range).await.unwrap();
                let before = source.selections.load(Ordering::Relaxed);
                let actual = engine.execute_async(&prepared).await.unwrap();
                assert_eq!(source.selections.load(Ordering::Relaxed), before, "{query}");
                let rows = |batches: &[RecordBatch]| {
                    series::decode(batches)
                        .unwrap()
                        .into_iter()
                        .map(|row| {
                            (
                                row.labels()
                                    .map(|(n, v)| (n.to_string(), v.to_string()))
                                    .collect::<Vec<_>>(),
                                row.timestamps().to_vec(),
                                row.values().iter().map(|v| v.to_bits()).collect::<Vec<_>>(),
                            )
                        })
                        .collect::<Vec<_>>()
                };
                assert_eq!(rows(&actual), rows(&expected), "{query}, {range:?}");
            }
        }
    }
}
