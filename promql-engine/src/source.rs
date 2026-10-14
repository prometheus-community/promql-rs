//! How a store hands series to the engine.
//!
//! [`SeriesSource`] mirrors Prometheus's `storage.Querier.Select`, which
//! every PromQL-on-something implementation has ended up writing. The
//! engine asks in PromQL terms — label matchers and a millisecond range —
//! and the store answers with a DataFusion [`ExecutionPlan`] carrying the
//! [series fields](crate::series) and mandatory opaque `series_id`.
//! Columns bind by name. What happens inside `select` is the
//! store's business: a parquet scan, a vortex scan, a gRPC call, a
//! `GROUP BY` that nests samples into lists. This crate never looks.
//!
//! Four obligations come with the answer, `docs/series-source.md` being
//! the contract:
//!
//! 1. **Filter.** Every series matches every matcher, and a chunk whose
//!    whole span misses the range is not sent.
//! 2. **Rows.** Each row holds one chunk of exactly one full raw label set,
//!    in one block, with at least one sample, samples ascending. A label set
//!    with several chunks in a block arrives once per chunk.
//! 3. **Order.** Over a partition's stream of chunks, never over a batch,
//!    which may be cut anywhere. All of a block's series come before the
//!    next block's; blocks ascend and do not overlap. Inside a block,
//!    the chunks of one ID are consecutive, never cross a partition, and
//!    ascend by first sample timestamp. Order between IDs is unspecified;
//!    neither labels nor IDs must ascend. IDs have a
//!    stable one-to-one mapping to full label sets; advisory grouping hints
//!    do not authorize dropping labels from that identity.
//! 4. **Blocks.** The store cuts time. Every series carries its block's
//!    `block_start` and `block_end`; a block answers the steps whose
//!    window end lies in `[block_start, block_end)`, and its series hold
//!    every sample those windows reach, which the store learns from
//!    `SelectHints::window_ms`. Blocks are contiguous over the select's
//!    window ends.
//!
//! [`SeriesSetExec`] exposes block ordering and unknown partitioning as a
//! physical source leaf. The source is trusted, not audited. The returned
//! plan is execution-ready and remains source-owned: outer optimization must
//! not remove exchanges that establish its co-location or chunk ordering.
//!
//! [`SelectorTable`] then makes a `select` result look like an ordinary
//! table to DataFusion, so a selector is a `TableScan` leaf and everything
//! above it — `EXPLAIN`, the optimizer, later operators — is stock.

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr, PhysicalSortExpr};
use datafusion::physical_plan::execution_plan::reset_plan_states;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::statistics::{StatisticsArgs, StatisticsContext};
use datafusion::physical_plan::{
    ChildrenPropertiesMode, DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning,
    PlanProperties, ReplaceChildrenOptions, Statistics,
};
use promql_parser::ast::LabelMatcher;

use crate::error::EngineError;
use crate::series::{self, BLOCK_START};

/// What the engine wants from a scan, beyond the matchers: Prometheus's
/// `storage.SelectHints`, typed, plus the window.
///
/// The range and the window are binding. Both bounds are inclusive
/// milliseconds and already account for lookback, `offset`, `@` and range
/// windows, so the store does not need to know any PromQL to honour them.
/// Everything else is advisory: a store may use it to read less or to lay
/// its output out better, and may ignore it without changing the result.
///
/// Prometheus's `Limit` and `DisableTrimming` are left out: the first
/// serves its label-values API, the second its own chunk trimming.
///
/// `#[non_exhaustive]`: the engine adds hints over time, and a struct
/// literal in a store's own code would stop compiling with each one. A
/// store reads the fields; tests that need a hint build it with
/// [`Self::range`] and the `with_` methods.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct SelectHints {
    pub start_ms: i64,
    pub end_ms: i64,
    /// How far back of each window end the query reads: the lookback
    /// delta for an instant selector, the `[5m]` for a range one. The
    /// range starts a window before the first window end, so the blocks
    /// run from `start_ms + window_ms` to `end_ms`, and each reaches back
    /// by this much so that it answers its steps alone.
    pub window_ms: i64,
    /// Step of the enclosing range query; `None` for an instant query.
    /// Read alignment must preserve exact window semantics.
    pub step_ms: Option<i64>,
    /// Window of a range selector such as `[5m]`; `None` for an instant
    /// selector. A store may prune chunks per window.
    pub range_ms: Option<i64>,
    /// The nearest function or aggregation above the selector, by its
    /// PromQL name (`rate`, `sum`); `None` when there is none. This names
    /// a reader, not a safe pre-aggregation or sample-pruning rule.
    pub func: Option<String>,
    /// `by` labels of the nearest aggregation above the selector, seen
    /// through the calls that map each series to one series and keep its
    /// labels (`rate`, `abs`, `timestamp`): `sum by (pod) (rate(x[5m]))`
    /// says `by (pod)` next to `func = rate`. `func` is the node nearest
    /// the selector, so it alone does not describe what lies between the
    /// selector and the aggregation.
    ///
    /// Advisory metadata about which labels the aggregation reads, never
    /// permission to prune raw labels or split or merge identities. The
    /// source must retain full raw label sets and their one-to-one ID mapping:
    /// merging below `count` changes cardinality, and below `rate` breaks
    /// reset detection. Absent under `without`, label rewrites, `absent`, sorts
    /// and binary operators; a nested aggregation starts its own grouping.
    /// An ungrouped aggregation says an empty `by`.
    pub grouping: Option<Grouping>,
    /// Return only shard `index` of `count` of the matching series, for
    /// scanning the series space in parallel; `None` for all of them.
    pub shard: Option<Shard>,
}

impl SelectHints {
    /// Just the range, with no window to reach back by; every advisory
    /// hint absent.
    pub fn range(start_ms: i64, end_ms: i64) -> Self {
        Self {
            start_ms,
            end_ms,
            window_ms: 0,
            step_ms: None,
            range_ms: None,
            func: None,
            grouping: None,
            shard: None,
        }
    }

    pub fn with_window_ms(mut self, window_ms: i64) -> Self {
        self.window_ms = window_ms;
        self
    }

    pub fn with_step_ms(mut self, step_ms: i64) -> Self {
        self.step_ms = Some(step_ms);
        self
    }

    pub fn with_range_ms(mut self, range_ms: i64) -> Self {
        self.range_ms = Some(range_ms);
        self
    }

    pub fn with_func(mut self, func: impl Into<String>) -> Self {
        self.func = Some(func.into());
        self
    }

    pub fn with_grouping(mut self, grouping: Grouping) -> Self {
        self.grouping = Some(grouping);
        self
    }

    pub fn with_shard(mut self, shard: Shard) -> Self {
        self.shard = Some(shard);
        self
    }
}

/// `by (labels…)` or `without (labels…)` of an enclosing aggregation.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct Grouping {
    pub labels: Vec<String>,
    /// `true` for `by`, `false` for `without`. The planner emits grouping
    /// hints only for `by`; `without` stops their propagation.
    pub by: bool,
}

impl Grouping {
    pub fn new(labels: Vec<String>, by: bool) -> Self {
        Self { labels, by }
    }
}

/// One of `count` equal parts of the series space, by series identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct Shard {
    pub index: u64,
    pub count: u64,
}

impl Shard {
    pub fn new(index: u64, count: u64) -> Self {
        Self { index, count }
    }
}

/// A store of series, as the engine sees it.
#[async_trait]
pub trait SeriesSource: fmt::Debug + Send + Sync {
    /// Every series matching all of `matchers`, carrying the samples
    /// required by `hints`, contiguous by ID within each block. See this module's
    /// row, ordering and block requirements.
    ///
    /// `matchers` arrive verbatim from the parser, including `__name__`
    /// (synthesized from a bare metric name). An absent label compares as
    /// `""`, and regexes are anchored to the whole value; see
    /// [`crate::CompiledMatcher`] for the reference semantics a store is held to.
    async fn select(
        &self,
        state: &dyn Session,
        matchers: &[LabelMatcher],
        hints: SelectHints,
    ) -> Result<Arc<dyn ExecutionPlan>>;
}

/// One selector's scan, as a DataFusion table.
///
/// Built eagerly: `select` is called once at plan time and its result
/// held, because a [`TableProvider`] must report its schema before any
/// scan and the schema — the label names — is only known once the store
/// has found the matching series. That is also how Prometheus works: it
/// expands every selector's series before evaluating anything. A
/// consequence worth knowing is that the store's own planning happens
/// while the engine is planning, not while it is executing.
///
/// Public so that a store which implements [`SeriesSource`] can register
/// itself as a SQL table for debugging: `SELECT labels, samples FROM …`.
#[derive(Debug)]
pub struct SelectorTable {
    plan: Arc<dyn ExecutionPlan>,
    schema: SchemaRef,
    /// The selector's bare metric name (`vs.name`), `""` when it had
    /// none. Kept alongside `matchers` because Go `VectorSelector.String()`
    /// (`promql/parser/printer.go`) only folds a `__name__` matcher into
    /// the bare-name prefix when its value equals this name — not for any
    /// `__name__` matcher — so rendering the scan needs the name on its
    /// own, not just recovered from the matcher list.
    name: String,
    /// The matchers `select` was called with. `select` only takes them,
    /// never returns them, and a `TableScan` node has no other way to say
    /// what it scans — kept here so `explain` can render `TableScan:
    /// selector_0 [http_requests_total{job="api"}]` instead of a bare
    /// table name.
    matchers: Vec<LabelMatcher>,
    /// What `select` was asked, for the [`SeriesSetExec`] above the
    /// store's plan to print; rendered here because `select` takes the
    /// hints by value.
    selection: String,
}

impl SelectorTable {
    /// Select once at plan time, when DataFusion needs the label schema.
    pub async fn try_new(
        state: &dyn Session,
        source: &dyn SeriesSource,
        name: &str,
        matchers: &[LabelMatcher],
        hints: SelectHints,
    ) -> std::result::Result<Self, EngineError> {
        let selection = crate::explain::render_selection(name, matchers, &hints);
        let plan = source.select(state, matchers, hints).await?;
        let schema = plan.schema();
        Ok(Self {
            plan,
            schema,
            name: name.to_string(),
            matchers: matchers.to_vec(),
            selection,
        })
    }

    /// The label names this selection carries.
    pub fn label_names(&self) -> Vec<String> {
        series::label_names(&self.schema)
    }

    /// The selector's bare metric name, for `explain`; see the field doc.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// What this table scans, for `explain` to render on the `TableScan` line.
    pub fn matchers(&self) -> &[LabelMatcher] {
        &self.matchers
    }
}

#[async_trait]
impl TableProvider for SelectorTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Temporary
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let plan: Arc<dyn ExecutionPlan> = Arc::new(
            SeriesSetExec::new(Arc::clone(&self.plan)).with_selection(self.selection.clone()),
        );
        let Some(cols) = projection else {
            return Ok(plan);
        };
        let exprs: Vec<(Arc<dyn PhysicalExpr>, String)> = cols
            .iter()
            .map(|&i| {
                let name = self.schema.field(i).name().clone();
                (
                    Arc::new(Column::new(&name, i)) as Arc<dyn PhysicalExpr>,
                    name,
                )
            })
            .collect();
        Ok(Arc::new(ProjectionExec::try_new(exprs, plan)?))
    }
}

/// Prometheus's `storage.SeriesSet`: exposes the selection's ordering and
/// partitioning as a physical source leaf. The execution-ready source plan
/// remains private so outer optimization cannot undo its assembly.
///
/// Co-location is a source obligation, not proof of DataFusion-compatible
/// hashing. Retain the input width as unknown partitioning and advertise
/// only block ordering: contiguity is not ascending ID or label order.
#[derive(Debug)]
pub struct SeriesSetExec {
    input: Arc<dyn ExecutionPlan>,
    properties: Arc<PlanProperties>,
    /// What the store was asked for, from [`SelectorTable`]; empty for a
    /// node built without one.
    selection: String,
}

impl SeriesSetExec {
    /// `input` must deliver ID-contiguous series within each block, with the
    /// columns described by this module's trusted source contract.
    pub fn new(input: Arc<dyn ExecutionPlan>) -> Self {
        let schema = input.schema();
        let column = |name: &str| -> Arc<dyn PhysicalExpr> {
            Arc::new(Column::new(
                name,
                schema.index_of(name).expect("a canonical schema"),
            ))
        };
        let ordering = vec![PhysicalSortExpr::new_default(column(BLOCK_START))];
        let eq = EquivalenceProperties::new_with_orderings(schema, [ordering]);
        let n = input.properties().partitioning.partition_count();
        let partitioning = Partitioning::UnknownPartitioning(n);
        let properties = PlanProperties::clone(input.properties())
            .with_eq_properties(eq)
            .with_partitioning(partitioning)
            .into();
        Self {
            input,
            properties,
            selection: String::new(),
        }
    }

    pub(crate) fn with_selection(mut self, selection: String) -> Self {
        self.selection = selection;
        self
    }

    /// The source-owned plan, for explain and native metric inspection.
    ///
    /// This is not an optimizer child: its internal exchanges may establish
    /// the selection's promised co-location and chunk ordering.
    pub fn source_plan(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }
}

impl DisplayAs for SeriesSetExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SeriesSetExec: ")?;
        if !self.selection.is_empty() {
            write!(f, "{}, ", self.selection)?;
        }
        write!(f, "partitioning={}", self.properties.partitioning)
    }
}

impl ExecutionPlan for SeriesSetExec {
    fn name(&self) -> &str {
        "SeriesSetExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        // The contract describes the completed selection, not its internal
        // partitions before the source assembles them.
        vec![]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        // Ordering describes the trusted stream; this leaf evaluates no
        // expressions and must not expose private source expression roots.
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.replace_children(
            children,
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )
    }

    fn replace_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
        _options: ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return Err(DataFusionError::Internal(
                "SeriesSetExec is a source leaf and takes no children".into(),
            ));
        }
        Ok(self)
    }

    fn reset_state(self: Arc<Self>) -> Result<Arc<dyn ExecutionPlan>> {
        // Ordinary reset traversal stops at this leaf too. Stateful source
        // operators still need fresh execution state when the plan is reused.
        let input = reset_plan_states(Arc::clone(&self.input))?;
        Ok(Arc::new(
            Self::new(input).with_selection(self.selection.clone()),
        ))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        self.input.execute(partition, context)
    }

    fn statistics_from_inputs(
        &self,
        _input_stats: &[Arc<Statistics>],
        args: &StatisticsArgs,
    ) -> Result<Arc<Statistics>> {
        // The protected source is not an optimizer child, so the outer
        // statistics walk supplies none. Walk its private tree separately.
        StatisticsContext::new().compute(self.input.as_ref(), args)
    }
}

#[cfg(test)]
pub(crate) fn source_error(msg: String) -> DataFusionError {
    DataFusionError::External(Box::new(EngineError::Source(msg)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{
        ArrayRef, Float64Array, ListArray, RecordBatch, StringViewArray, StructArray,
        TimestampMillisecondArray,
    };
    use datafusion::arrow::buffer::OffsetBuffer;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::physical_plan::collect;
    use datafusion::prelude::SessionContext;

    use crate::series::{sample_fields, sample_item, schema, Block};

    #[test]
    fn hints_built_by_the_builders_carry_what_was_set() {
        let hints = SelectHints::range(10, 20)
            .with_window_ms(5)
            .with_step_ms(2)
            .with_range_ms(5)
            .with_func("rate")
            .with_grouping(Grouping::new(vec!["route".into()], true))
            .with_shard(Shard::new(1, 4));
        assert_eq!(
            hints,
            SelectHints {
                start_ms: 10,
                end_ms: 20,
                window_ms: 5,
                step_ms: Some(2),
                range_ms: Some(5),
                func: Some("rate".into()),
                grouping: Some(Grouping {
                    labels: vec!["route".into()],
                    by: true
                }),
                shard: Some(Shard { index: 1, count: 4 }),
            }
        );
    }

    /// `(labels, timestamps)`; a label missing from a series is `""`, as
    /// in the canonical shape.
    type Row<'a> = (&'a [(&'a str, &'a str)], &'a [i64]);

    /// One block over everything for the forwarding fixtures.
    const ONE_BLOCK: Block = Block {
        start_ms: 0,
        end_ms: i64::MAX,
    };

    /// One batch of `rows` over the label `names`, all in one block.
    fn batch(names: &[&str], rows: &[Row]) -> RecordBatch {
        batch_in(ONE_BLOCK, names, rows)
    }

    /// One batch of `rows` over the label `names`, all in `block`.
    fn batch_in(block: Block, names: &[&str], rows: &[Row]) -> RecordBatch {
        let names: Vec<String> = names.iter().map(|n| n.to_string()).collect();
        let schema = schema(&names);
        let columns: Vec<ArrayRef> = names
            .iter()
            .map(|n| {
                let values = rows
                    .iter()
                    .map(|(labels, _)| labels.iter().find(|(k, _)| k == n).map_or("", |(_, v)| *v));
                Arc::new(StringViewArray::from_iter_values(values)) as ArrayRef
            })
            .collect();
        let labels = match schema.field(0).data_type() {
            datafusion::arrow::datatypes::DataType::Struct(f) if f.is_empty() => {
                StructArray::new_empty_fields(rows.len(), None)
            }
            datafusion::arrow::datatypes::DataType::Struct(f) => {
                StructArray::new(f.clone(), columns, None)
            }
            _ => unreachable!("canonical"),
        };
        let ts: Vec<i64> = rows.iter().flat_map(|(_, t)| t.iter().copied()).collect();
        let entries = StructArray::new(
            sample_fields(),
            vec![
                Arc::new(TimestampMillisecondArray::from(ts.clone())),
                Arc::new(Float64Array::from(vec![1.0; ts.len()])),
            ],
            None,
        );
        let offsets = OffsetBuffer::from_lengths(rows.iter().map(|(_, t)| t.len()));
        let samples = ListArray::new(sample_item(), offsets, Arc::new(entries), None);
        let [block_start, block_end] = block.columns(rows.len());
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(labels), Arc::new(samples), block_start, block_end],
        )
        .unwrap();
        // Fixture IDs reflect the full raw label set, not its order or the
        // batch's row ordinal, so an identity survives every chunk/block cut.
        use std::hash::{Hash, Hasher};
        let ids = rows
            .iter()
            .map(|(labels, _)| {
                let mut labels = labels.to_vec();
                labels.sort();
                let mut hash = std::collections::hash_map::DefaultHasher::new();
                labels.hash(&mut hash);
                hash.finish()
            })
            .collect();
        with_ids(&batch, ids)
    }

    fn block(start_ms: i64, end_ms: i64) -> Block {
        Block { start_ms, end_ms }
    }

    #[test]
    fn series_set_without_a_selection_prints_its_partitioning() {
        let batch = batch(&[], &[]);
        let plan = series_set(vec![vec![batch.clone()], vec![batch]]);
        assert_eq!(
            datafusion::physical_plan::displayable(plan.as_ref())
                .one_line()
                .to_string()
                .trim_end(),
            "SeriesSetExec: partitioning=UnknownPartitioning(2)"
        );
    }

    fn with_ids(batch: &RecordBatch, ids: Vec<u64>) -> RecordBatch {
        use datafusion::arrow::array::UInt64Array;
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        let mut fields = batch.schema().fields().to_vec();
        let mut columns = batch.columns().to_vec();
        if let Ok(index) = batch.schema().index_of(series::SERIES_ID) {
            columns[index] = Arc::new(UInt64Array::from(ids));
        } else {
            fields.push(Arc::new(Field::new(
                series::SERIES_ID,
                DataType::UInt64,
                false,
            )));
            columns.push(Arc::new(UInt64Array::from(ids)));
        }
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
    }

    #[tokio::test]
    async fn selector_table_preserves_the_selected_plan_and_block_order() {
        #[derive(Debug)]
        struct Source(Arc<dyn ExecutionPlan>);

        #[async_trait]
        impl SeriesSource for Source {
            async fn select(
                &self,
                _state: &dyn Session,
                _matchers: &[LabelMatcher],
                _hints: SelectHints,
            ) -> Result<Arc<dyn ExecutionPlan>> {
                Ok(Arc::clone(&self.0))
            }
        }

        // ID and label order carry no physical ASC guarantee.
        let batch = with_ids(&batch(&["pod"], &[(A, &[1]), (B, &[1])]), vec![7, 91]);
        let source = Source(
            MemorySourceConfig::try_new_exec(&[vec![batch.clone()]], batch.schema(), None).unwrap(),
        );
        let ctx = SessionContext::new();
        let state = ctx.state();
        let table = SelectorTable::try_new(&state, &source, "x", &[], SelectHints::range(0, 1))
            .await
            .unwrap();
        let scan = table.scan(&state, None, &[], None).await.unwrap();
        assert_eq!(
            scan.properties().output_ordering().unwrap().to_string(),
            "block_start@2 ASC"
        );
        assert!(matches!(
            scan.properties().output_partitioning(),
            Partitioning::UnknownPartitioning(1)
        ));
        assert_eq!(collect(scan, ctx.task_ctx()).await.unwrap(), [batch]);
    }

    #[tokio::test]
    async fn series_set_preserves_contiguity_arbitrary_id_order_and_partition_width() {
        let first = with_ids(
            &batch_in(block(0, 100), &["pod"], &[(B, &[1]), (B, &[2])]),
            vec![91, 91],
        );
        let second = with_ids(
            &batch_in(block(0, 100), &["pod"], &[(B, &[3]), (A, &[1])]),
            vec![91, 7],
        );
        let later = with_ids(
            &batch_in(block(100, 200), &["pod"], &[(A, &[101]), (B, &[101])]),
            vec![7, 91],
        );
        let exec = series_set(vec![
            vec![first.clone(), second.clone(), later.clone()],
            vec![first.slice(0, 0)],
        ]);
        assert_eq!(
            exec.properties().output_ordering().unwrap().to_string(),
            "block_start@2 ASC"
        );
        assert_eq!(
            exec.properties().output_partitioning().to_string(),
            "UnknownPartitioning(2)"
        );
        assert!(exec.children().is_empty());
        let ctx = SessionContext::new();
        let output =
            datafusion::physical_plan::common::collect(exec.execute(0, ctx.task_ctx()).unwrap())
                .await
                .unwrap();
        assert_eq!(output, [first, second, later]);
    }

    #[test]
    fn series_set_is_a_leaf_and_keeps_its_properties_on_rebuild_and_reset() {
        let batch = with_ids(&batch(&["pod"], &[(A, &[1])]), vec![7]);
        let input =
            MemorySourceConfig::try_new_exec(&[vec![batch.clone()]], batch.schema(), None).unwrap();
        let plan =
            Arc::new(SeriesSetExec::new(input.clone()).with_selection("x 0..1 window=0s".into()));
        assert!(plan.children().is_empty());
        assert!(plan.required_input_ordering().is_empty());
        assert!(plan
            .input_distribution_requirements()
            .per_child_distributions()
            .next()
            .is_none());
        assert!(plan.maintains_input_order().is_empty());
        assert!(plan.benefits_from_input_partitioning().is_empty());
        let mut expressions = 0;
        plan.apply_expressions(&mut |_| {
            expressions += 1;
            Ok(TreeNodeRecursion::Continue)
        })
        .unwrap();
        assert_eq!(expressions, 0);
        let options = ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute);
        assert!(plan.clone().replace_children(vec![input], options).is_err());

        for rebuilt in [
            plan.clone().replace_children(vec![], options).unwrap(),
            plan.clone().reset_state().unwrap(),
        ] {
            assert!(rebuilt.children().is_empty());
            assert!(rebuilt.is::<SeriesSetExec>());
            assert_eq!(rebuilt.schema(), batch.schema());
            assert_eq!(
                rebuilt.properties().output_ordering().unwrap().to_string(),
                plan.properties().output_ordering().unwrap().to_string()
            );
            assert_eq!(
                datafusion::physical_plan::displayable(rebuilt.as_ref())
                    .one_line()
                    .to_string()
                    .trim_end(),
                "SeriesSetExec: x 0..1 window=0s, partitioning=UnknownPartitioning(1)"
            );
        }
    }

    fn series_set(partitions: Vec<Vec<RecordBatch>>) -> Arc<SeriesSetExec> {
        let schema = partitions
            .iter()
            .flatten()
            .next()
            .expect("at least one batch")
            .schema();
        let child = MemorySourceConfig::try_new_exec(&partitions, schema, None).unwrap();
        Arc::new(SeriesSetExec::new(child))
    }

    async fn run(
        partitions: Vec<Vec<RecordBatch>>,
    ) -> std::result::Result<Vec<RecordBatch>, EngineError> {
        let ctx = SessionContext::new();
        Ok(collect(series_set(partitions), ctx.task_ctx()).await?)
    }

    const A: &[(&str, &str)] = &[("pod", "a")];
    const B: &[(&str, &str)] = &[("pod", "b")];
    const C: &[(&str, &str)] = &[("pod", "c")];

    #[tokio::test]
    async fn a_series_split_across_three_batches_passes_untouched() {
        let input = vec![
            batch(&["pod"], &[(A, &[1, 2])]),
            batch(&["pod"], &[(A, &[3])]),
            batch(&["pod"], &[(A, &[3, 4]), (B, &[1])]),
        ];
        let out = run(vec![input.clone()]).await.unwrap();
        assert_eq!(out, input);
    }

    /// Empty batches are forwarded just like non-empty batches.
    #[tokio::test]
    async fn an_empty_batch_between_series_changes_nothing() {
        run(vec![vec![
            batch(&["pod"], &[(A, &[1])]),
            batch(&["pod"], &[]),
            batch(&["pod"], &[(A, &[2]), (B, &[1])]),
        ]])
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn a_series_continues_after_another_series_in_the_previous_batch() {
        run(vec![vec![
            batch(&["pod"], &[(A, &[1]), (B, &[1])]),
            batch(&["pod"], &[(B, &[2]), (C, &[1])]),
        ]])
        .await
        .unwrap();
    }

    /// No label comparison order is required of the raw source.
    #[tokio::test]
    async fn sparse_label_sets_pass_in_arbitrary_order() {
        run(vec![vec![batch(
            &["a", "b"],
            &[
                (&[("a", "1"), ("b", "1")], &[1]),
                (&[("b", "1")], &[1]),
                (&[("a", "1")], &[1]),
            ],
        )]])
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn a_label_set_without_labels_is_one_series() {
        run(vec![vec![
            batch(&[], &[(&[], &[1, 2])]),
            batch(&[], &[(&[], &[3])]),
        ]])
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn arbitrary_partition_assignment_is_preserved() {
        let out = run(vec![
            vec![batch(&["pod"], &[(B, &[1])])],
            vec![batch(&["pod"], &[(A, &[1])])],
        ])
        .await
        .unwrap();
        assert_eq!(out.len(), 2);
    }

    #[test]
    fn declares_only_block_start_ascending_and_keeps_the_partitioning() {
        let exec = series_set(vec![
            vec![batch(&["pod"], &[(A, &[1])])],
            vec![batch(&["pod"], &[(B, &[1])])],
        ]);
        assert!(matches!(
            exec.properties().output_partitioning(),
            Partitioning::UnknownPartitioning(2)
        ));
        let ordering = exec.properties().output_ordering().expect("an ordering");
        assert_eq!(ordering.to_string(), "block_start@2 ASC");
        assert!(exec.children().is_empty());
    }

    /// Repeated IDs and reach-back across blocks pass through the source leaf.
    #[test]
    fn an_id_can_recur_with_reach_back_after_a_block_transition() {
        run(vec![vec![
            batch_in(block(0, 100), &["pod"], &[(A, &[1]), (B, &[1])]),
            batch_in(block(100, 200), &["pod"], &[(A, &[150]), (B, &[150])]),
        ]])
        .await_ok();
        run(vec![vec![batch_in(block(0, 100), &["pod"], &[(B, &[1])])]
            .into_iter()
            .chain([batch_in(block(100, 200), &["pod"], &[(A, &[1])])])
            .collect()])
        .await_ok();
        // The second block's reach-back may repeat earlier samples.
        run(vec![vec![
            batch_in(block(0, 100), &["pod"], &[(A, &[50, 90])]),
            batch_in(block(100, 200), &["pod"], &[(A, &[40])]),
        ]])
        .await_ok();
    }

    /// `run` for the tests above that only want it to succeed, on a
    /// runtime of their own so a `#[test]` can chain several.
    trait AwaitOk {
        fn await_ok(self);
    }

    impl<F: std::future::Future<Output = std::result::Result<Vec<RecordBatch>, EngineError>>>
        AwaitOk for F
    {
        fn await_ok(self) {
            tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap()
                .block_on(self)
                .unwrap();
        }
    }
}
