//! Direct native Partial/Final lifetime and spill tests bypass optimizer
//! combination into PartiallySorted Single, which DF55 leaves on the fallback
//! stream. Pending-group observations are fixture-specific, not a one-block bound.

use super::*;
use datafusion::arrow::array::StringViewArray;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::datasource::{memory::MemorySourceConfig, source::DataSourceExec};
use datafusion::execution::{
    memory_pool::GreedyMemoryPool, runtime_env::RuntimeEnvBuilder, TaskContext,
};
use datafusion::physical_expr::{
    aggregate::AggregateExprBuilder,
    expressions::{col, lit},
    LexOrdering, PhysicalExpr, PhysicalSortExpr,
};
use datafusion::physical_plan::{
    aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy},
    stream::RecordBatchStreamAdapter,
    DisplayAs, DisplayFormatType, ExecutionPlan, InputOrderMode, PlanProperties,
    SendableRecordBatchStream,
};
use futures::StreamExt;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Mutex,
};
use std::time::Duration;
use tokio::sync::{oneshot, Notify};

#[derive(Debug, Default)]
struct Progress {
    batches: AtomicUsize,
    rows: AtomicUsize,
    emitted: AtomicUsize,
    peak_pending: AtomicUsize,
    spill_metrics: Mutex<Option<datafusion::physical_plan::metrics::MetricsSet>>,
    first_spill_after_rows: AtomicUsize,
    at_eof: Notify,
}

/// Observe only the aggregate's immediate input, never a shuffle task. Unique
/// keys make accepted minus emitted rows an exact pending-group count, including
/// fallback output queues. It is not a measurement of retained buffer capacities.
#[derive(Debug)]
struct ObservedInput {
    input: Arc<dyn ExecutionPlan>,
    progress: Arc<Progress>,
    eof_gate: Mutex<Option<oneshot::Receiver<()>>>,
}

impl DisplayAs for ObservedInput {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ObservedInput")
    }
}

impl ExecutionPlan for ObservedInput {
    fn name(&self) -> &str {
        "ObservedInput"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.input.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn apply_expressions(
        &self,
        _: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        datafusion::common::not_impl_err!("test input is executed directly, never replanned")
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // Open the memory source directly: DataSourceExec would split to the
        // context batch size and conceal the deliberately oversized input case.
        let input = self
            .input
            .downcast_ref::<DataSourceExec>()
            .unwrap()
            .data_source()
            .open(partition, context)?;
        let gate = self.eof_gate.lock().unwrap().take();
        let progress = self.progress.clone();
        let stream = futures::stream::unfold(
            (input, gate, progress),
            |(mut input, gate, progress)| async move {
                let batch = input.next().await;
                if progress
                    .spill_metrics
                    .lock()
                    .unwrap()
                    .as_ref()
                    .is_some_and(|m| m.spill_count().unwrap_or(0) > 0)
                    && progress.first_spill_after_rows.load(Ordering::SeqCst) == 0
                {
                    progress
                        .first_spill_after_rows
                        .store(progress.rows.load(Ordering::SeqCst), Ordering::SeqCst);
                }
                match batch {
                    Some(batch) => {
                        if let Ok(batch) = &batch {
                            progress.batches.fetch_add(1, Ordering::SeqCst);
                            let rows = progress.rows.fetch_add(batch.num_rows(), Ordering::SeqCst)
                                + batch.num_rows();
                            progress.peak_pending.fetch_max(
                                rows - progress.emitted.load(Ordering::SeqCst),
                                Ordering::SeqCst,
                            );
                        }
                        Some((batch, (input, gate, progress)))
                    }
                    None => {
                        progress.at_eof.notify_one();
                        if let Some(gate) = gate {
                            gate.await.expect("test must release input EOF");
                        }
                        None
                    }
                }
            },
        );
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }
}

fn raw_batches(blocks: usize, groups: usize, input_size: usize) -> Vec<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![
        Field::new(series::BLOCK_START, series::timestamp_type(), false),
        Field::new(series::BLOCK_END, series::timestamp_type(), false),
        Field::new("job", DataType::Utf8View, false),
        Field::new(series::SAMPLES, series::samples_type(), false),
    ]));
    let mut batches = Vec::new();
    for block in 0..blocks {
        for first in (0..groups).step_by(input_size) {
            let end = (first + input_size).min(groups);
            let len = end - first;
            // The same global grid and one-position spans isolate native
            // lifetime from query-length-dependent lane allocation.
            let start = (block * 2) as i64;
            batches.push(
                RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(TimestampMillisecondArray::from(vec![start; len])),
                        Arc::new(TimestampMillisecondArray::from(vec![start + 2; len])),
                        Arc::new(StringViewArray::from_iter_values(
                            (first..end).map(|g| format!("j{g:03}")),
                        )),
                        samples_column(
                            &(first..end)
                                .map(|g| vec![(start, g as f64 + 1.)])
                                .collect::<Vec<_>>(),
                        ),
                    ],
                )
                .unwrap(),
            );
        }
    }
    batches
}

fn observed(
    batches: Vec<RecordBatch>,
    progress: Arc<Progress>,
    gate: Option<oneshot::Receiver<()>>,
) -> Arc<dyn ExecutionPlan> {
    let schema = batches[0].schema();
    let source = MemorySourceConfig::try_new(&[batches], schema.clone(), None)
        .unwrap()
        .try_with_sort_information(vec![LexOrdering::new(vec![PhysicalSortExpr::new_default(
            col(series::BLOCK_START, &schema).unwrap(),
        )])
        .unwrap()])
        .unwrap();
    Arc::new(ObservedInput {
        input: Arc::new(DataSourceExec::new(Arc::new(source))),
        progress,
        eof_gate: Mutex::new(gate),
    })
}

fn partial(input: Arc<dyn ExecutionPlan>) -> AggregateExec {
    let schema = input.schema();
    let group = PhysicalGroupBy::new_single(
        [series::BLOCK_START, series::BLOCK_END, "job"]
            .into_iter()
            .map(|name| (col(name, &schema).unwrap(), name.to_string()))
            .collect(),
    );
    let expr = AggregateExprBuilder::new(
        Arc::new(udaf()),
        vec![
            col(series::SAMPLES, &schema).unwrap(),
            lit("sum"),
            lit(0i64),
            lit(63i64),
            lit(1i64),
        ],
    )
    .schema(schema.clone())
    .alias(series::SAMPLES)
    .build()
    .unwrap();
    let agg = AggregateExec::try_new(
        AggregateMode::Partial,
        group,
        vec![Arc::new(expr)],
        vec![None],
        input,
        schema,
    )
    .unwrap();
    assert_eq!(
        agg.input_order_mode(),
        &InputOrderMode::PartiallySorted(vec![0])
    );
    agg
}

fn final_agg(template: &AggregateExec, input: Arc<dyn ExecutionPlan>) -> AggregateExec {
    let agg = AggregateExec::try_new(
        AggregateMode::Final,
        template.group_expr().clone(),
        template.aggr_expr().to_vec(),
        template.filter_expr().to_vec(),
        input,
        template.input_schema(),
    )
    .unwrap();
    assert_eq!(
        agg.input_order_mode(),
        &InputOrderMode::PartiallySorted(vec![0])
    );
    agg
}

/// Timestamp-bearing lane states, not raw selector iterators. Neither Partial
/// nor Repartition can read ahead of Final in this controlled lifetime fixture.
fn state_batches(raw: &[RecordBatch], schema: Arc<Schema>) -> Vec<RecordBatch> {
    raw.iter()
        .map(|batch| {
            let mut groups = Grouped::new(Op::Sum, 0, 63, 1).unwrap();
            let indices: Vec<_> = (0..batch.num_rows()).collect();
            groups
                .update_batch(&[batch.column(3).clone()], &indices, None, indices.len())
                .unwrap();
            let state = groups.state(EmitTo::All).unwrap().remove(0);
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    batch.column(0).clone(),
                    batch.column(1).clone(),
                    batch.column(2).clone(),
                    state,
                ],
            )
            .unwrap()
        })
        .collect()
}

fn context(migration: bool, batch_size: usize, budget: Option<usize>) -> SessionContext {
    let mut config = SessionConfig::new()
        .with_target_partitions(1)
        .with_batch_size(batch_size);
    config.options_mut().execution.enable_migration_aggregate = migration;
    let mut runtime = RuntimeEnvBuilder::new();
    if let Some(budget) = budget {
        runtime = runtime.with_memory_pool(Arc::new(GreedyMemoryPool::new(budget)));
    }
    SessionContext::new_with_config_rt(config, runtime.build_arc().unwrap())
}

fn check_answers(output: &[RecordBatch], is_partial: bool, blocks: usize, groups: usize) {
    let mut keys = std::collections::BTreeSet::new();
    let mut previous = None;
    for batch in output {
        for row in 0..batch.num_rows() {
            let start = batch
                .column(0)
                .as_primitive::<TimestampMillisecondType>()
                .value(row);
            assert!(previous.is_none_or(|p| start >= p));
            previous = Some(start);
            assert_eq!(
                batch
                    .column(1)
                    .as_primitive::<TimestampMillisecondType>()
                    .value(row),
                start + 2
            );
            let job = batch.column(2).as_string_view().value(row);
            let group = job[1..].parse::<usize>().unwrap();
            assert!(keys.insert((start, group)), "duplicate group");
            let result = if is_partial {
                let mut accumulator = Grouped::new(Op::Sum, 0, 63, 1).unwrap();
                accumulator
                    .merge_batch(&[batch.column(3).slice(row, 1)], &[0], 1)
                    .unwrap();
                let values = accumulator.evaluate(EmitTo::All).unwrap();
                rows(values.as_list::<i32>(), 0)
            } else {
                rows(batch.column(3).as_list::<i32>(), row)
            };
            assert_eq!(result, vec![(start, group as f64 + 1.)]);
        }
    }
    assert_eq!(
        keys,
        (0..blocks)
            .flat_map(|b| (0..groups).map(move |g| ((b * 2) as i64, g)))
            .collect()
    );
}

async fn ordinary_lifetime(is_partial: bool) {
    for blocks in [8, 32] {
        for input_size in [1, 2] {
            for migration in [true, false] {
                let ctx = context(migration, 1, None);
                let progress = Arc::new(Progress::default());
                let raw = raw_batches(blocks, 2, input_size);
                let template = partial(observed(raw.clone(), Arc::new(Progress::default()), None));
                let agg = if is_partial {
                    partial(observed(raw, progress.clone(), None))
                } else {
                    let states = state_batches(&raw, template.schema());
                    final_agg(&template, observed(states, progress.clone(), None))
                };
                let mut stream = agg.execute(0, ctx.task_ctx()).unwrap();
                let mut output = Vec::new();
                let mut first_consumed = Vec::new();
                let mut retained_at_last_input = None;
                while let Some(batch) = stream.next().await {
                    let batch = batch.unwrap();
                    assert_eq!(batch.num_rows(), 1);
                    let emitted = progress.emitted.fetch_add(1, Ordering::SeqCst) + 1;
                    let consumed = progress.rows.load(Ordering::SeqCst);
                    if first_consumed.len() < 2 {
                        first_consumed.push(progress.batches.load(Ordering::SeqCst));
                    }
                    let pending = consumed - emitted;
                    if consumed == blocks * 2 && retained_at_last_input.is_none() {
                        retained_at_last_input = Some(pending);
                    }
                    if migration && input_size == 1 {
                        assert!(
                            pending <= 2,
                            "bounded input must not grow with query length"
                        );
                    }
                    output.push(batch);
                }
                // Preserve the DF55 observation, not a fabricated universal
                // bound: oversized native input can accumulate pending groups.
                if input_size == 2 {
                    assert_eq!(
                        first_consumed,
                        if migration { vec![2, 3] } else { vec![2, 2] }
                    );
                    assert_eq!(
                        retained_at_last_input,
                        Some(if migration { blocks + 1 } else { 3 })
                    );
                    if migration {
                        assert_eq!(progress.peak_pending.load(Ordering::SeqCst), blocks + 2);
                    }
                } else if migration {
                    assert_eq!(first_consumed, vec![3, 4]);
                    assert_eq!(progress.peak_pending.load(Ordering::SeqCst), 3);
                }
                assert_eq!(progress.rows.load(Ordering::SeqCst), blocks * 2);
                assert_eq!(
                    progress.batches.load(Ordering::SeqCst),
                    blocks * 2 / input_size
                );
                assert_eq!(progress.emitted.load(Ordering::SeqCst), blocks * 2);
                assert_eq!(agg.metrics().unwrap().spill_count().unwrap_or(0), 0);
                check_answers(&output, is_partial, blocks, 2);
            }
        }
    }
}

#[tokio::test]
async fn migrated_partial_lifetime_depends_on_input_batch_bound() {
    ordinary_lifetime(true).await;
}

#[tokio::test]
async fn migrated_final_lifetime_depends_on_input_batch_bound() {
    ordinary_lifetime(false).await;
}

#[tokio::test]
async fn migrated_final_spill_waits_for_input_eof_before_output() {
    let blocks = 8;
    let groups = 1024;
    let raw = raw_batches(blocks, groups, 32);
    let template = partial(observed(raw.clone(), Arc::new(Progress::default()), None));
    let states = state_batches(&raw, template.schema());
    let progress = Arc::new(Progress::default());
    let (release, gate) = oneshot::channel();
    let agg = final_agg(&template, observed(states, progress.clone(), Some(gate)));
    let ctx = context(true, 32, Some(200_000));
    assert!(ctx.runtime_env().disk_manager.tmp_files_enabled());
    let mut stream = agg.execute(0, ctx.task_ctx()).unwrap();
    *progress.spill_metrics.lock().unwrap() = agg.metrics();
    let mut next = Box::pin(stream.next());
    // Drive actual spill I/O to gated EOF. Timeout is only a deadlock guard.
    tokio::time::timeout(Duration::from_secs(60), async {
        tokio::select! {
            output = &mut next => panic!("spilled Final emitted before gated EOF: {output:?}"),
            () = progress.at_eof.notified() => {},
        }
    })
    .await
    .expect("Final must reach gated EOF");
    assert_eq!(progress.rows.load(Ordering::SeqCst), blocks * groups);
    assert!(
        agg.metrics().unwrap().spill_count().unwrap_or(0) > 0,
        "must actually spill"
    );
    let first_spill = progress.first_spill_after_rows.load(Ordering::SeqCst);
    assert!(
        first_spill > 0 && first_spill <= groups,
        "spill must finish before a second block closes the first: {first_spill}"
    );
    assert!(futures::poll!(next.as_mut()).is_pending());
    release.send(()).unwrap();
    let first = tokio::time::timeout(Duration::from_secs(60), next)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let mut output = vec![first];
    tokio::time::timeout(Duration::from_secs(60), async {
        while let Some(batch) = stream.next().await {
            output.push(batch.unwrap());
        }
    })
    .await
    .expect("spill replay must complete");
    assert!(output.iter().all(|b| b.num_rows() <= 32));
    check_answers(&output, false, blocks, groups);
}

/// Real multi-partition native Partial plus FinalPartitioned and an exchange
/// plan, then replay isolated upper states under pressure to avoid racing
/// unspillable shuffle queues against the aggregate's finite reservation.
#[tokio::test]
async fn migrated_ordered_upper_states_survive_memory_pressure_and_tiny_drains() {
    use datafusion::logical_expr::col as logical_col;

    let groups = 1024;
    let blocks = 8;
    let steps = 64;
    let schema = Arc::new(Schema::new(vec![
        Field::new("job", DataType::Utf8View, false),
        Field::new(series::SAMPLES, series::samples_type(), false),
        Field::new(series::BLOCK_START, series::timestamp_type(), false),
        Field::new(series::BLOCK_END, series::timestamp_type(), false),
    ]));
    let mut batches = Vec::new();
    for block in 0..blocks {
        let start = block * (steps / blocks);
        for first in (0..groups).step_by(32) {
            let values: Vec<Vec<(i64, f64)>> = (first..first + 32)
                .map(|g| {
                    (start..start + steps / blocks)
                        .map(|t| (t, g as f64))
                        .collect()
                })
                .collect();
            batches.push(
                RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(StringViewArray::from_iter_values(
                            (first..first + 32).map(|g| format!("j{g}")),
                        )),
                        samples_column(&values),
                        Arc::new(TimestampMillisecondArray::from(vec![start; 32])),
                        Arc::new(TimestampMillisecondArray::from(vec![
                            start + steps / blocks;
                            32
                        ])),
                    ],
                )
                .unwrap(),
            );
        }
    }
    let mut config = SessionConfig::new()
        .with_target_partitions(2)
        .with_batch_size(7);
    config.options_mut().execution.enable_migration_aggregate = true;
    let ctx = SessionContext::new_with_config(config.clone());
    ctx.register_udaf(udaf());
    let table = MemTable::try_new(schema.clone(), vec![batches.clone(), batches.clone()])
        .unwrap()
        .with_sort_order(vec![
            vec![logical_col(series::BLOCK_START).sort(true, true)],
        ]);
    ctx.register_table("t", Arc::new(table)).unwrap();
    let frame = ctx
        .sql(&format!(
            "SELECT block_start, block_end, job, {NAME}(samples, 'sum', 0, {}, 1) AS samples \
         FROM t GROUP BY block_start, block_end, job",
            steps - 1,
        ))
        .await
        .unwrap();
    let plan = frame.create_physical_plan().await.unwrap();
    let mut stack = vec![plan.clone()];
    let (mut partial, mut final_plan) = (None, None);
    let mut exchange = false;
    while let Some(node) = stack.pop() {
        stack.extend(node.children().into_iter().cloned());
        exchange |= node.name() == "RepartitionExec";
        if let Some(agg) = node.downcast_ref::<AggregateExec>() {
            assert!(matches!(
                agg.input_order_mode(),
                InputOrderMode::PartiallySorted(_)
            ));
            match agg.mode() {
                AggregateMode::Partial => partial = Some(node.clone()),
                AggregateMode::FinalPartitioned => final_plan = Some(node.clone()),
                mode => panic!("unexpected upper aggregate mode {mode:?}"),
            }
        }
    }
    assert!(
        exchange,
        "upper grouping must establish its own distribution"
    );
    let partial = partial.expect("actual native Partial");
    let final_plan = final_plan.expect("actual native FinalPartitioned");
    let template = final_plan.downcast_ref::<AggregateExec>().unwrap();
    let states = datafusion::physical_plan::collect_partitioned(partial, ctx.task_ctx())
        .await
        .unwrap()
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    assert!(states.iter().all(|batch| batch.num_rows() <= 7));
    let state_schema = states[0].schema();
    let states = datafusion::arrow::compute::concat_batches(&state_schema, &states).unwrap();
    let order = datafusion::arrow::compute::sort_to_indices(states.column(0), None, None).unwrap();
    let states = RecordBatch::try_new(
        state_schema.clone(),
        states
            .columns()
            .iter()
            .map(|column| datafusion::arrow::compute::take(column, &order, None).unwrap())
            .collect(),
    )
    .unwrap();
    let states = (0..states.num_rows())
        .step_by(32)
        .map(|first| states.slice(first, 32.min(states.num_rows() - first)))
        .collect::<Vec<_>>();
    let source = MemorySourceConfig::try_new(&[states], state_schema.clone(), None)
        .unwrap()
        .try_with_sort_information(vec![LexOrdering::new(vec![PhysicalSortExpr::new_default(
            col(series::BLOCK_START, &state_schema).unwrap(),
        )])
        .unwrap()])
        .unwrap();
    let input: Arc<dyn ExecutionPlan> = Arc::new(DataSourceExec::new(Arc::new(source)));
    let mut pressure = Grouped::new(Op::Sum, 0, steps - 1, 1).unwrap();
    for (i, batch) in batches[..groups / 32].iter().enumerate() {
        let indices = (i * 32..(i + 1) * 32).collect::<Vec<_>>();
        pressure
            .update_batch(&[batch.column(1).clone()], &indices, None, groups)
            .unwrap();
    }
    let budget = pressure.size() / 2;
    drop(pressure);
    for budget in [None, Some(budget)] {
        let mut runtime = RuntimeEnvBuilder::new();
        if let Some(budget) = budget {
            runtime = runtime.with_memory_pool(Arc::new(GreedyMemoryPool::new(budget)));
        }
        let runtime = runtime.build_arc().unwrap();
        assert!(runtime.disk_manager.tmp_files_enabled());
        let ctx =
            SessionContext::new_with_config_rt(config.clone().with_target_partitions(1), runtime);
        let agg = Arc::new(
            AggregateExec::try_new(
                AggregateMode::FinalPartitioned,
                template.group_expr().clone(),
                template.aggr_expr().to_vec(),
                template.filter_expr().to_vec(),
                input.clone(),
                template.input_schema(),
            )
            .unwrap(),
        );
        assert!(matches!(
            agg.input_order_mode(),
            InputOrderMode::PartiallySorted(_)
        ));
        let output = datafusion::physical_plan::collect_partitioned(agg.clone(), ctx.task_ctx())
            .await
            .unwrap();
        let mut keys = std::collections::BTreeSet::new();
        for partition in output {
            let mut previous = None;
            for batch in partition {
                assert!(batch.num_rows() <= 7);
                let starts = batch.column(0).as_primitive::<TimestampMillisecondType>();
                let ends = batch.column(1).as_primitive::<TimestampMillisecondType>();
                let jobs = batch.column(2).as_string_view();
                let samples = batch.column(3).as_list::<i32>();
                for row in 0..batch.num_rows() {
                    let start = starts.value(row);
                    assert!(previous.is_none_or(|p| start >= p));
                    previous = Some(start);
                    let group: usize = jobs.value(row)[1..].parse().unwrap();
                    assert!(keys.insert((start, group)), "duplicate group");
                    assert_eq!(ends.value(row), start + steps / blocks);
                    assert_eq!(
                        rows(samples, row),
                        (start..start + steps / blocks)
                            .map(|t| (t, 2. * group as f64))
                            .collect::<Vec<_>>()
                    );
                }
            }
        }
        assert_eq!(
            keys,
            (0..blocks)
                .flat_map(|block| (0..groups).map(move |g| (block * (steps / blocks), g)))
                .collect()
        );
        let spills = agg.metrics().unwrap().spill_count().unwrap_or(0);
        if budget.is_some() {
            assert!(spills > 0, "finite pool must exercise upper spilling");
        } else {
            assert_eq!(spills, 0);
        }
    }
}
