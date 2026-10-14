//! Series-contiguous port of Prometheus's `evalSeries` / `matrixIterSlice`
//! (`promql/engine.go`, 83962c35). Only one identity is open per partition.
//! An ID or block transition closes it before consuming the next row; source
//! batch boundaries do not close it. The source guarantees contiguity and
//! chronology, so no identity table or provenance checks belong here.
//!
//! Output builders own completed rows, bounded by row/sample budgets (except
//! one indivisible oversized row). Push and close share a sample/step budget
//! per poll; a pending raw row and the fold's edge/step cursors survive yields.
//! Range value folds, Arrow bulk append/finish and destruction are indivisible:
//! this is cooperative numerical progress, not wall-clock preemption.
//! Owned labels and finalized samples coexist
//! with their destination at reservation checkpoints. Native Arrow estimates
//! charge the pending raw batch and canonical output independently; these
//! estimates are not RSS or bounds on opaque allocation owners.

use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, ListArray, RecordBatch, RecordBatchOptions, StringViewBuilder,
    StructArray,
};
use datafusion::arrow::datatypes::{
    DataType, FieldRef, Fields, Schema, SchemaRef, TimestampMillisecondType,
};
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{internal_err, plan_err, Result};
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::equivalence::ProjectionMapping;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{
    EquivalenceProperties, LexOrdering, OrderingRequirements, PhysicalExpr, PhysicalSortExpr,
};
use datafusion::physical_plan::coop::cooperative;
use datafusion::physical_plan::execution_plan::{EmissionType, SchedulingType};
use datafusion::physical_plan::metrics::{
    BaselineMetrics, ExecutionPlanMetricsSet, Gauge, MetricBuilder, MetricsSet,
};
use datafusion::physical_plan::projection::{
    remove_unnecessary_projections, ProjectionExec, ProjectionExpr,
};
use datafusion::physical_plan::{
    ChildrenPropertiesMode, DisplayAs, DisplayFormatType, Distribution, ExecutionPlan,
    InputDistributionRequirements, Partitioning, PlanProperties, ReplaceChildrenOptions,
};
use futures::{Stream, StreamExt};

use super::state;
use crate::buffer::{EvalContext, Kernel, PushCursor, WorkBudget};
use crate::params::Params;
use crate::series::{
    self, Block, SamplesBuilder, BLOCK_END, BLOCK_START, LABELS, SAMPLES, SERIES_ID,
};

const BATCH_SAMPLES: usize = 1 << 20;
const INPUT_ROWS: usize = 8192;
// Shared by ingestion, sample-edge movement, sweep pops, and remaining steps.
// Row caps alone cannot make one large raw row or EOF close cooperative.
const WORK_UNITS: usize = 32_768;

/// Evaluates consecutive chunks independently in each source partition.
/// IDs are raw-input dependencies only. Evaluation never advertises them as
/// an output hash or claims group uniqueness from a source's opaque identity.
#[derive(Debug)]
pub(crate) struct SeriesEvalExec {
    input: Arc<dyn ExecutionPlan>,
    properties: Arc<PlanProperties>,
    grid: Params,
    kernel: Kernel,
    metrics: ExecutionPlanMetricsSet,
    // A canonical-order subset; native ProjectionExec owns presentation.
    projection: Vec<usize>,
}
impl SeriesEvalExec {
    pub(crate) fn try_new(
        input: Arc<dyn ExecutionPlan>,
        kernel: Kernel,
        grid: Params,
    ) -> Result<Self> {
        let input_schema = input.schema();
        let fields = [LABELS, SAMPLES, BLOCK_START, BLOCK_END]
            .into_iter()
            .map(|name| input_schema.field_with_name(name).cloned().map(Arc::new))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        match input_schema.field_with_name(SERIES_ID)?.data_type() {
            DataType::UInt64 => {}
            DataType::FixedSizeBinary(width) if *width > 0 => {}
            dtype => return plan_err!("series evaluation does not support series_id type {dtype}"),
        }
        let schema = Arc::new(Schema::new_with_metadata(
            fields,
            input_schema.metadata().clone(),
        ));
        let ordering = [PhysicalSortExpr::new_default(Arc::new(
            Column::new_with_schema(BLOCK_START, &schema).expect("canonical"),
        ))];
        // Co-location does not imply DataFusion hashing, nor any order among IDs.
        let properties = PlanProperties::new(
            EquivalenceProperties::new_with_orderings(schema, [ordering.to_vec()]),
            Partitioning::UnknownPartitioning(input.properties().partitioning.partition_count()),
            EmissionType::Both,
            input.properties().boundedness,
        )
        .with_scheduling_type(SchedulingType::Cooperative)
        .into();
        Ok(Self {
            input,
            properties,
            grid,
            kernel,
            metrics: ExecutionPlanMetricsSet::new(),
            projection: vec![0, 1, 2, 3],
        })
    }

    // Called on a fresh evaluator with indices into its canonical schema.
    fn with_projection(mut self, projection: Vec<usize>) -> Result<Self> {
        debug_assert!(projection.windows(2).all(|pair| pair[0] < pair[1]));
        let schema = self.schema();
        let mapping = ProjectionMapping::from_indices(&projection, &schema)?;
        let schema = Arc::new(schema.project(&projection)?);
        self.properties = PlanProperties::new(
            self.properties.eq_properties.project(&mapping, schema),
            self.properties.partitioning.clone(),
            EmissionType::Both,
            self.properties.boundedness,
        )
        .with_scheduling_type(SchedulingType::Cooperative)
        .into();
        self.projection = projection;
        Ok(self)
    }
}
impl DisplayAs for SeriesEvalExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "SeriesEvalExec: kernel={:?}, grid={:?}{}",
            self.kernel,
            self.grid,
            if !self.projection.contains(&0) {
                ", labels pruned"
            } else {
                ""
            }
        )
    }
}
impl ExecutionPlan for SeriesEvalExec {
    fn name(&self) -> &str {
        "SeriesEvalExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }
    fn input_distribution_requirements(&self) -> InputDistributionRequirements {
        // A hash requirement would shuffle raw chunks and destroy contiguity.
        InputDistributionRequirements::new(vec![Distribution::UnspecifiedDistribution])
    }
    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }
    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
        vec![Some(OrderingRequirements::from(
            LexOrdering::new([BLOCK_START].map(|name| {
                PhysicalSortExpr::new_default(Arc::new(
                    Column::new_with_schema(name, &self.input.schema()).expect("canonical"),
                ))
            }))
            .expect("block ordering"),
        ))]
    }
    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.replace_children(
            children,
            ReplaceChildrenOptions {
                children_properties: ChildrenPropertiesMode::Recompute,
            },
        )
    }
    fn replace_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
        _options: ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let [input]: [Arc<dyn ExecutionPlan>; 1] = children.try_into().map_err(|_| {
            datafusion::error::DataFusionError::Internal(
                "SeriesEvalExec takes exactly one child".into(),
            )
        })?;
        Ok(Arc::new(
            Self::try_new(input, self.kernel, self.grid)?
                .with_projection(self.projection.clone())?,
        ))
    }
    fn try_swapping_with_projection(
        &self,
        projection: &ProjectionExec,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let schema = self.schema();
        let mut indices = Vec::with_capacity(projection.expr().len());
        for expr in projection.expr() {
            let Some(column) = expr.expr.downcast_ref::<Column>() else {
                return Ok(None);
            };
            if schema
                .fields()
                .get(column.index())
                .is_none_or(|f| f.name() != column.name())
            {
                return Ok(None);
            }
            indices.push(column.index());
        }
        indices.sort_unstable();
        indices.dedup();
        // No pruning means no rewrite, even for aliases/reordering/duplicates.
        // This also makes residual native projections an optimizer fixpoint.
        if indices.len() == self.projection.len() {
            return Ok(None);
        }
        let pruned = Self::try_new(self.input.clone(), self.kernel, self.grid)?
            .with_projection(indices.iter().map(|&i| self.projection[i]).collect())?;
        let expressions = projection.expr().iter().map(|expr| {
            let column = expr.expr.downcast_ref::<Column>().expect("direct column");
            ProjectionExpr {
                expr: Arc::new(Column::new(
                    column.name(),
                    indices
                        .binary_search(&column.index())
                        .expect("retained column"),
                )),
                alias: expr.alias.clone(),
            }
        });
        let residual = ProjectionExec::try_new(expressions, Arc::new(pruned))?;
        Ok(Some(
            remove_unnecessary_projections(Arc::new(residual))?.data,
        ))
    }
    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if context.session_config().batch_size() == 0 {
            return internal_err!("series evaluation requires a positive session batch size");
        }
        let input = self.input.execute(partition, context.clone())?;
        let schema = input.schema();
        let DataType::Struct(fields) = schema.field_with_name(LABELS)?.data_type() else {
            return internal_err!("series evaluation needs canonical labels");
        };
        let DataType::List(sample_item) = schema.field_with_name(SAMPLES)?.data_type() else {
            unreachable!("trusted source samples shape");
        };
        Ok(Box::pin(cooperative(SeriesStream {
            input: Some(input),
            schema: self.schema(),
            fields: fields.clone(),
            sample_item: sample_item.clone(),
            projection: self.projection.clone(),
            grid: self.grid,
            kernel: self.kernel,
            batch_size: context.session_config().batch_size().min(i32::MAX as usize),
            pending: None,
            push_cursor: PushCursor::default(),
            input_bytes: 0,
            block: None,
            current: None,
            output: None,
            output_ready: false,
            reservation: MemoryConsumer::new(format!("SeriesEvalExec[{partition}]"))
                .register(&context.runtime_env().memory_pool),
            baseline: BaselineMetrics::new(&self.metrics, partition),
            memory: MetricBuilder::new(&self.metrics).mem_used(partition),
            peak_memory: MetricBuilder::new(&self.metrics).gauge("peak_mem_used", partition),
        })))
    }
}

// Only the open identity is owned. Binary IDs are opaque, including zero bytes.
enum SeriesId {
    U64(u64),
    Binary(Box<[u8]>),
}
impl SeriesId {
    fn copy(ids: &ArrayRef, row: usize) -> Self {
        match ids.data_type() {
            DataType::UInt64 => Self::U64(
                ids.as_primitive::<datafusion::arrow::datatypes::UInt64Type>()
                    .value(row),
            ),
            DataType::FixedSizeBinary(_) => {
                Self::Binary(ids.as_fixed_size_binary().value(row).into())
            }
            _ => unreachable!("bound ID type"),
        }
    }
    fn matches(&self, ids: &ArrayRef, row: usize) -> bool {
        match self {
            Self::U64(id) => {
                *id == ids
                    .as_primitive::<datafusion::arrow::datatypes::UInt64Type>()
                    .value(row)
            }
            Self::Binary(id) => id.as_ref() == ids.as_fixed_size_binary().value(row),
        }
    }
    fn heap_size(&self) -> usize {
        match self {
            Self::U64(_) => 0,
            Self::Binary(id) => id.len(),
        }
    }
}

struct CurrentSeries {
    id: SeriesId,
    fold: state::SeriesFold,
    labels: Option<Vec<String>>,
    label_bytes: usize,
    sealed: bool,
}
impl CurrentSeries {
    fn size(&self) -> Result<usize> {
        self.id
            .heap_size()
            .checked_add(self.label_bytes)
            .and_then(|bytes| bytes.checked_add(self.fold.heap_size()))
            .ok_or_else(charge_overflow)
    }
}

struct CurrentBlock {
    bounds: Block,
    context: EvalContext,
}

// These are ordinary Arrow builders, not a collection of finalized folds.
// One batch never straddles a block edge, including blocks with no query steps.
struct Output {
    bounds: Block,
    samples: SamplesBuilder,
    sample_count: usize,
    labels: Option<Vec<StringViewBuilder>>,
}
impl Output {
    fn new(bounds: Block, fields: &Fields, needs_labels: bool) -> Self {
        Self {
            bounds,
            samples: SamplesBuilder::default(),
            sample_count: 0,
            labels: needs_labels.then(|| {
                fields
                    .iter()
                    .map(|_| StringViewBuilder::with_capacity(0))
                    .collect()
            }),
        }
    }
    fn size(&self) -> Result<usize> {
        let labels = self.labels.as_ref().map_or(Ok(0), |builders| {
            let slots = builders
                .capacity()
                .checked_mul(std::mem::size_of::<StringViewBuilder>())
                .ok_or_else(charge_overflow)?;
            builders.iter().try_fold(slots, |bytes, builder| {
                bytes
                    .checked_add(builder.allocated_size())
                    .ok_or_else(charge_overflow)
            })
        })?;
        (self.samples.size() - std::mem::size_of::<SamplesBuilder>())
            .checked_add(labels)
            .ok_or_else(charge_overflow)
    }
    fn rows(&self) -> usize {
        self.samples.rows()
    }
}

fn charge_overflow() -> datafusion::error::DataFusionError {
    datafusion::error::DataFusionError::ResourcesExhausted(
        "series evaluation memory charge overflow".into(),
    )
}

// SamplesBuilder uses canonical fields. The boundary promises the source's
// nested metadata too, even if samples are projected away. Rewrap only Arrow
// headers: copying the finished payload would add a second output allocation.
fn samples_with_declared_fields(samples: ListArray, item: &FieldRef) -> Result<ListArray> {
    if samples.data_type() == &DataType::List(item.clone()) {
        return Ok(samples);
    }
    let DataType::Struct(fields) = item.data_type() else {
        unreachable!("trusted source sample item shape");
    };
    let (_, offsets, values, nulls) = samples.into_parts();
    let values = values.as_struct();
    let values = StructArray::try_new(
        fields.clone(),
        values.columns().to_vec(),
        values.nulls().cloned(),
    )?;
    Ok(ListArray::try_new(
        item.clone(),
        offsets,
        Arc::new(values),
        nulls,
    )?)
}

struct SeriesStream {
    input: Option<SendableRecordBatchStream>,
    schema: SchemaRef,
    fields: Fields,
    sample_item: FieldRef,
    projection: Vec<usize>,
    grid: Params,
    kernel: Kernel,
    batch_size: usize,
    // The complete arrays remain charged until the last raw row is consumed,
    // even while a finished output prefix waits for downstream demand.
    pending: Option<(RecordBatch, usize)>,
    push_cursor: PushCursor,
    input_bytes: usize,
    block: Option<CurrentBlock>,
    current: Option<CurrentSeries>,
    output: Option<Output>,
    output_ready: bool,
    reservation: MemoryReservation,
    baseline: BaselineMetrics,
    memory: Gauge,
    peak_memory: Gauge,
}
impl SeriesStream {
    fn size(&self) -> Result<usize> {
        let current = self.current.as_ref().map_or(Ok(0), CurrentSeries::size)?;
        let output = self.output.as_ref().map_or(Ok(0), Output::size)?;
        std::mem::size_of::<Self>()
            .checked_add(self.input_bytes)
            .and_then(|bytes| bytes.checked_add(current))
            .and_then(|bytes| bytes.checked_add(output))
            .ok_or_else(charge_overflow)
    }
    fn charge(&self, extra: usize) -> Result<()> {
        let bytes = self
            .size()?
            .checked_add(extra)
            .ok_or_else(charge_overflow)?;
        self.reservation.try_resize(bytes)?;
        self.memory.set(bytes);
        self.peak_memory.set_max(bytes);
        Ok(())
    }
    fn release(&mut self) {
        self.input = None;
        self.pending = None;
        self.push_cursor = PushCursor::default();
        self.input_bytes = 0;
        self.current = None;
        self.block = None;
        self.output = None;
        self.output_ready = false;
        self.reservation.free();
        self.memory.set(0);
    }
    fn has_output(&self) -> bool {
        self.output
            .as_ref()
            .is_some_and(|output| output.rows() != 0)
    }
    fn needs_labels(&self) -> bool {
        self.projection.contains(&0)
    }

    #[cfg(test)]
    fn finish_series(&mut self) -> Result<bool> {
        self.finish_series_budgeted(&mut WorkBudget::new(usize::MAX))
    }

    // False means close must resume, or its sealed row cannot join the current
    // output within its sample budget. Neither case opens the next identity.
    fn finish_series_budgeted(&mut self, budget: &mut WorkBudget) -> Result<bool> {
        let Some(current) = self.current.as_mut() else {
            return Ok(true);
        };
        if !current.sealed {
            let finished = current
                .fold
                .finish_output_budgeted(&self.block.as_ref().expect("open block").context, budget);
            current.sealed = finished;
            // Closing may grow finalized output while old sweep/window capacity
            // still lives. A failed checkpoint must not be rescued by freeing it.
            self.charge(0)?;
            if !finished {
                return Ok(false);
            }
            self.current
                .as_mut()
                .expect("sealed series")
                .fold
                .release_input();
            self.charge(0)?;
        }
        let current = self.current.as_ref().expect("sealed series");
        let count = current.fold.out.len();
        let existing = self.output.as_ref().map_or(0, |output| output.sample_count);
        if self.has_output() && existing.saturating_add(count) > BATCH_SAMPLES {
            self.output_ready = true;
            return Ok(false);
        }
        let total = state::checked_sample_count([existing, count])?;
        if self.output.is_none() {
            self.output = Some(Output::new(
                self.block.as_ref().expect("open block").bounds,
                &self.fields,
                self.needs_labels(),
            ));
        }
        let output = self.output.as_mut().expect("output builders");
        let current = self.current.as_ref().expect("sealed series");
        current.fold.out.append_to(&mut output.samples);
        output.samples.finish_row();
        output.sample_count = total;
        if let Some(builders) = &mut output.labels {
            for (builder, value) in builders
                .iter_mut()
                .zip(current.labels.as_ref().expect("owned labels"))
            {
                builder.append_value(value);
            }
        }
        // Both owned labels/results and their copied destination must fit.
        self.charge(0)?;
        self.current = None;
        let output = self.output.as_ref().expect("finished output row");
        self.output_ready = output.rows() >= self.batch_size || total >= BATCH_SAMPLES;
        self.charge(0)?;
        Ok(true)
    }

    #[cfg(test)]
    fn update(&mut self, batch: &RecordBatch, from: usize) -> Result<usize> {
        self.update_budgeted(batch, from, &mut WorkBudget::new(usize::MAX))
    }

    fn update_budgeted(
        &mut self,
        batch: &RecordBatch,
        from: usize,
        budget: &mut WorkBudget,
    ) -> Result<usize> {
        let samples = batch
            .column_by_name(SAMPLES)
            .expect("bound source column")
            .as_list::<i32>();
        let (ts, vs) = series::sample_slices(samples.values().as_struct());
        let offsets = samples.value_offsets();
        let starts = batch
            .column_by_name(BLOCK_START)
            .expect("bound source column")
            .as_primitive::<TimestampMillisecondType>();
        let ends = batch
            .column_by_name(BLOCK_END)
            .expect("bound source column")
            .as_primitive::<TimestampMillisecondType>();
        let ids = batch
            .column_by_name(SERIES_ID)
            .expect("bound source column");
        let until = batch
            .num_rows()
            .min(from.saturating_add(self.batch_size.min(INPUT_ROWS)));
        for row in from..until {
            if budget.exhausted() {
                self.charge(0)?;
                return Ok(row);
            }
            let bounds = Block {
                start_ms: starts.value(row),
                end_ms: ends.value(row),
            };
            let changed_block = self.block.as_ref().map(|block| block.bounds) != Some(bounds);
            let changed_id = self
                .current
                .as_ref()
                .is_some_and(|current| !current.id.matches(ids, row));
            if changed_block || changed_id {
                if !self.finish_series_budgeted(budget)? || self.output_ready {
                    return Ok(row);
                }
                if changed_block {
                    if self.has_output() {
                        self.output_ready = true;
                        return Ok(row);
                    }
                    self.block = Some(CurrentBlock {
                        bounds,
                        context: EvalContext::new(self.kernel, self.grid.for_block(bounds)),
                    });
                }
            }
            if self.current.is_none() {
                let labels = self.needs_labels().then(|| {
                    state::copy_label_values(
                        batch
                            .column_by_name(LABELS)
                            .expect("bound source column")
                            .as_struct(),
                        row,
                    )
                });
                let label_bytes = labels
                    .as_ref()
                    .map_or(Ok(0), state::label_values_heap_size)?;
                self.current = Some(CurrentSeries {
                    id: SeriesId::copy(ids, row),
                    fold: state::SeriesFold::new(&self.block.as_ref().expect("open block").context),
                    labels,
                    label_bytes,
                    sealed: false,
                });
            }
            let (a, b) = (offsets[row] as usize, offsets[row + 1] as usize);
            if a != b && !samples.is_null(row) {
                let finished = self
                    .current
                    .as_mut()
                    .expect("open series")
                    .fold
                    .push_budgeted(
                        &self.block.as_ref().expect("open block").context,
                        &ts[a..b],
                        &vs[a..b],
                        &mut self.push_cursor,
                        budget,
                    );
                if !finished {
                    self.charge(0)?;
                    return Ok(row);
                }
            }
            self.push_cursor = PushCursor::default();
        }
        self.charge(0)?;
        Ok(until)
    }

    fn emit(&mut self) -> Result<RecordBatch> {
        let output = self.output.as_mut().expect("completed output");
        let n = output.rows();
        debug_assert_ne!(n, 0);
        let samples: ArrayRef = Arc::new(samples_with_declared_fields(
            output.samples.take_all(),
            &self.sample_item,
        )?);
        let [start, end] = output.bounds.columns(n);
        let labels: Option<ArrayRef> = if let Some(builders) = &mut output.labels {
            Some(if builders.is_empty() {
                Arc::new(StructArray::new_empty_fields(n, None))
            } else {
                let columns = builders
                    .iter_mut()
                    .map(|builder| Arc::new(builder.finish()) as ArrayRef)
                    .collect();
                Arc::new(StructArray::try_new(self.fields.clone(), columns, None)?)
            })
        } else {
            None
        };
        // Canonical locals stay live even when projected away.
        let live = [
            samples.as_ref(),
            start.as_ref(),
            end.as_ref(),
            labels.as_deref().unwrap_or(samples.as_ref()),
        ];
        let live = &live[..if labels.is_some() { 4 } else { 3 }];
        let output_bytes = live.iter().try_fold(0usize, |bytes, array| {
            bytes
                .checked_add(array.get_array_memory_size())
                .ok_or_else(charge_overflow)
        })?;
        self.charge(output_bytes)?;
        let canonical = [labels.as_ref(), Some(&samples), Some(&start), Some(&end)];
        let columns = self
            .projection
            .iter()
            .map(|&index| canonical[index].expect("retained column").clone())
            .collect();
        let batch = RecordBatch::try_new_with_options(
            self.schema.clone(),
            columns,
            &RecordBatchOptions::new().with_row_count(Some(n)),
        )?;
        drop(labels);
        drop(samples);
        drop(start);
        drop(end);
        self.output = None;
        self.output_ready = false;
        if self.input.is_none() && self.current.is_none() && self.pending.is_none() {
            self.release();
        } else {
            self.charge(0)?;
        }
        Ok(batch)
    }

    fn poll_inner(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<RecordBatch>>> {
        let compute = self.baseline.elapsed_compute().clone();
        let mut budget = WorkBudget::new(WORK_UNITS);
        for _ in 0..16 {
            if self.output_ready {
                let timer = compute.timer();
                let batch = self.emit();
                timer.done();
                return Poll::Ready(Some(batch));
            }
            if let Some((batch, from)) = self.pending.take() {
                let timer = compute.timer();
                let updated = self.update_budgeted(&batch, from, &mut budget);
                timer.done();
                match updated {
                    Ok(row) if row < batch.num_rows() => self.pending = Some((batch, row)),
                    Ok(_) => {
                        drop(batch);
                        self.input_bytes = 0;
                    }
                    Err(e) => return Poll::Ready(Some(Err(e))),
                }
                if let Err(e) = self.charge(0) {
                    return Poll::Ready(Some(Err(e)));
                }
                if !self.output_ready {
                    cx.waker().wake_by_ref();
                    return Poll::Pending;
                }
                continue;
            }
            if self.input.is_none() {
                let timer = compute.timer();
                let finished = self.finish_series_budgeted(&mut budget);
                timer.done();
                match finished {
                    Err(e) => return Poll::Ready(Some(Err(e))),
                    Ok(false) if !self.output_ready => {
                        cx.waker().wake_by_ref();
                        return Poll::Pending;
                    }
                    _ => {}
                }
                if self.has_output() {
                    self.output_ready = true;
                    continue;
                }
                self.release();
                return Poll::Ready(None);
            }
            match self.input.as_mut().expect("live input").poll_next_unpin(cx) {
                Poll::Pending if self.has_output() => {
                    self.output_ready = true;
                }
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e))),
                Poll::Ready(Some(Ok(batch))) => {
                    self.input_bytes = batch.get_array_memory_size();
                    self.pending = Some((batch, 0));
                    if let Err(e) = self.charge(0) {
                        return Poll::Ready(Some(Err(e)));
                    }
                }
                Poll::Ready(None) => self.input = None,
            }
        }
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}
impl Stream for SeriesStream {
    type Item = Result<RecordBatch>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let poll = self.poll_inner(cx);
        if matches!(poll, Poll::Ready(Some(Err(_)))) {
            self.release();
        }
        self.baseline.record_poll(poll)
    }
}
impl RecordBatchStream for SeriesStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
impl Drop for SeriesStream {
    fn drop(&mut self) {
        self.memory.set(0);
    }
}

#[cfg(test)]
mod tests;
