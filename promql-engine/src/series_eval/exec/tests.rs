use super::*;
use crate::{Engine, EngineError, RangeQuery, SelectHints, SeriesSource};
use async_trait::async_trait;
use datafusion::arrow::array::{
    FixedSizeBinaryArray, Float64Array, ListArray, StringViewArray, StructArray,
    TimestampMillisecondArray, UInt64Array,
};
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::arrow::datatypes::{Field, Schema};
use datafusion::catalog::Session;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::execution::memory_pool::GreedyMemoryPool;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::physical_optimizer::projection_pushdown::ProjectionPushdown;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::{collect, displayable};
use datafusion::prelude::{SessionConfig, SessionContext};
use promql_parser::ast::LabelMatcher;

#[derive(Debug)]
struct Raw(Vec<RecordBatch>);
#[async_trait]
impl SeriesSource for Raw {
    async fn select(
        &self,
        _: &dyn Session,
        _: &[LabelMatcher],
        _: SelectHints,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(MemorySourceConfig::try_new_exec(
            std::slice::from_ref(&self.0),
            self.0[0].schema(),
            None,
        )?)
    }
}

fn block(start_ms: i64, end_ms: i64) -> Block {
    Block { start_ms, end_ms }
}
const BLOCK: Block = Block {
    start_ms: 0,
    end_ms: 1_000,
};
type Row<'a> = (Block, u64, &'a str, &'a [(i64, f64)]);
fn batch(rows: &[Row<'_>], binary: bool) -> RecordBatch {
    let mut fields = series::schema(&["pod".into()]).fields().to_vec();
    fields.push(Arc::new(Field::new(
        SERIES_ID,
        if binary {
            DataType::FixedSizeBinary(16)
        } else {
            DataType::UInt64
        },
        false,
    )));
    let labels = StructArray::new(
        vec![Field::new("pod", DataType::Utf8View, false)].into(),
        vec![Arc::new(StringViewArray::from_iter_values(
            rows.iter().map(|r| r.2),
        ))],
        None,
    );
    let ts: Vec<_> = rows.iter().flat_map(|r| r.3.iter().map(|p| p.0)).collect();
    let vs: Vec<_> = rows.iter().flat_map(|r| r.3.iter().map(|p| p.1)).collect();
    let samples = ListArray::new(
        series::sample_item(),
        OffsetBuffer::from_lengths(rows.iter().map(|r| r.3.len())),
        Arc::new(StructArray::new(
            series::sample_fields(),
            vec![
                Arc::new(TimestampMillisecondArray::from(ts)),
                Arc::new(Float64Array::from(vs)),
            ],
            None,
        )),
        None,
    );
    let ids: ArrayRef = if binary && rows.is_empty() {
        Arc::new(FixedSizeBinaryArray::try_new(16, Vec::<u8>::new().into(), None).unwrap())
    } else if binary {
        Arc::new(
            FixedSizeBinaryArray::try_from_iter(rows.iter().map(|r| {
                let mut b = [0; 16];
                b[8..].copy_from_slice(&r.1.to_be_bytes());
                b
            }))
            .unwrap(),
        )
    } else {
        Arc::new(UInt64Array::from(
            rows.iter().map(|r| r.1).collect::<Vec<_>>(),
        ))
    };
    RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        vec![
            Arc::new(labels),
            Arc::new(samples),
            Arc::new(TimestampMillisecondArray::from(
                rows.iter().map(|r| r.0.start_ms).collect::<Vec<_>>(),
            )),
            Arc::new(TimestampMillisecondArray::from(
                rows.iter().map(|r| r.0.end_ms).collect::<Vec<_>>(),
            )),
            ids,
        ],
    )
    .unwrap()
}
async fn plan(source: &Raw, query: &str, range: &RangeQuery) -> Arc<dyn ExecutionPlan> {
    let plan = Engine::new()
        .prepare_async(source, query, range)
        .await
        .unwrap()
        .physical_plan()
        .clone();
    assert!(displayable(plan.as_ref())
        .indent(true)
        .to_string()
        .contains("SeriesEvalExec"));
    plan
}
fn lower(plan: &Arc<dyn ExecutionPlan>) -> &SeriesEvalExec {
    if let Some(exec) = plan.downcast_ref::<SeriesEvalExec>() {
        return exec;
    }
    lower(plan.children()[0])
}

fn project_columns(
    input: Arc<dyn ExecutionPlan>,
    columns: &[(&str, usize, &str)],
) -> Arc<dyn ExecutionPlan> {
    let projection = ProjectionExec::try_new(
        columns.iter().map(|&(name, index, alias)| ProjectionExpr {
            expr: Arc::new(Column::new(name, index)),
            alias: alias.into(),
        }),
        input.clone(),
    )
    .unwrap();
    input
        .try_swapping_with_projection(&projection)
        .unwrap()
        .unwrap_or_else(|| Arc::new(projection))
}

// A one-chunk numerical reference, not a second layout. The fixture alone
// materializes samples; production always folds raw chunks without concatenation.
// Duplicate timestamps within a chunk are retained; overlap between chunks is
// first-chunk-wins, including stale markers.
async fn reference(
    source: &Raw,
    query: &str,
    range: &RangeQuery,
    ctx: &SessionContext,
) -> Vec<RecordBatch> {
    type ReferenceSeries = (Block, u64, String, Vec<(i64, f64)>);
    let mut groups: Vec<ReferenceSeries> = Vec::new();
    let mut previous = None;
    for input in &source.0 {
        let samples = input.column_by_name(SAMPLES).unwrap().as_list::<i32>();
        let (ts, vs) = series::sample_slices(samples.values().as_struct());
        for row in 0..input.num_rows() {
            let bounds = series::block_of(input, row);
            let ids = input.column_by_name(SERIES_ID).unwrap();
            let id = match ids.data_type() {
                DataType::UInt64 => ids
                    .as_primitive::<datafusion::arrow::datatypes::UInt64Type>()
                    .value(row),
                DataType::FixedSizeBinary(_) => u64::from_be_bytes(
                    ids.as_fixed_size_binary().value(row)[8..]
                        .try_into()
                        .unwrap(),
                ),
                _ => unreachable!(),
            };
            if groups
                .last()
                .is_none_or(|group| (group.0, group.1) != (bounds, id))
            {
                groups.push((
                    bounds,
                    id,
                    input
                        .column_by_name(LABELS)
                        .unwrap()
                        .as_struct()
                        .column(0)
                        .as_string_view()
                        .value(row)
                        .to_owned(),
                    Vec::new(),
                ));
                previous = None;
            }
            let offsets = samples.value_offsets();
            let (a, b) = (offsets[row] as usize, offsets[row + 1] as usize);
            let group = groups.last_mut().unwrap();
            for (&t, &v) in ts[a..b].iter().zip(&vs[a..b]) {
                if previous.is_none_or(|last| t > last) {
                    group.3.push((t, v));
                }
            }
            previous = previous.max(ts[a..b].last().copied());
        }
    }
    let rows: Vec<_> = groups
        .iter()
        .map(|group| (group.0, group.1, group.2.as_str(), group.3.as_slice()))
        .collect();
    let source = Raw(vec![batch(&rows, false)]);
    collect(plan(&source, query, range).await, ctx.task_ctx())
        .await
        .unwrap()
}
fn answers(batches: &[RecordBatch]) -> Vec<(String, Vec<(i64, u64)>)> {
    let mut answer: Vec<_> = series::decode(batches)
        .unwrap()
        .into_iter()
        .map(|s| {
            (
                s.labels().next().map_or("", |(_, v)| v).to_owned(),
                s.timestamps()
                    .iter()
                    .zip(s.values())
                    .map(|(&t, &v)| (t, v.to_bits()))
                    .collect(),
            )
        })
        .collect();
    answer.sort();
    answer
}
fn private_stream(exec: &SeriesEvalExec, ctx: &SessionContext, batch_size: usize) -> SeriesStream {
    let input = exec.input.execute(0, ctx.task_ctx()).unwrap();
    let input_schema = exec.input.schema();
    let DataType::Struct(fields) = input_schema
        .field_with_name(LABELS)
        .unwrap()
        .data_type()
        .clone()
    else {
        unreachable!()
    };
    let DataType::List(sample_item) = input_schema
        .field_with_name(SAMPLES)
        .unwrap()
        .data_type()
        .clone()
    else {
        unreachable!()
    };
    SeriesStream {
        input: Some(input),
        schema: exec.schema(),
        fields,
        sample_item,
        projection: exec.projection.clone(),
        grid: exec.grid,
        kernel: exec.kernel,
        batch_size,
        pending: None,
        push_cursor: PushCursor::default(),
        input_bytes: 0,
        block: None,
        current: None,
        output: None,
        output_ready: false,
        reservation: MemoryConsumer::new("test").register(&ctx.runtime_env().memory_pool),
        baseline: BaselineMetrics::new(&exec.metrics, 0),
        memory: MetricBuilder::new(&exec.metrics).mem_used(0),
        peak_memory: MetricBuilder::new(&exec.metrics).gauge("peak_mem_used", 0),
    }
}
fn assert_charged(stream: &SeriesStream) {
    assert_eq!(stream.reservation.size(), stream.size().unwrap());
    if let Some(current) = &stream.current {
        let labels = current.labels.as_ref().map_or(0, |values| {
            values.capacity() * std::mem::size_of::<String>()
                + values.iter().map(String::capacity).sum::<usize>()
        });
        assert_eq!(current.label_bytes, labels);
        assert_eq!(
            current.size().unwrap(),
            labels + current.id.heap_size() + current.fold.heap_size()
        );
    }
}

#[tokio::test]
async fn many_contiguous_series_produce_independent_bounded_arrays_across_batch_cuts() {
    for binary in [false, true] {
        let labels: Vec<_> = (0..257)
            .map(|i| format!("series-{i:04}-long-value"))
            .collect();
        let first = [(350, 1.), (500, -0.)];
        let second = [(510, 3.), (590, 4.)];
        // Descending IDs/labels are legal: only chunks of one identity are consecutive.
        let rows: Vec<_> = labels
            .iter()
            .enumerate()
            .rev()
            .flat_map(|(i, l)| {
                [
                    (BLOCK, i as u64, l.as_str(), &first[..]),
                    (BLOCK, i as u64, l.as_str(), &second[..]),
                ]
            })
            .collect();
        let all = batch(&rows, binary);
        let source = Raw((0..all.num_rows())
            .step_by(19)
            .map(|i| all.slice(i, 19.min(all.num_rows() - i)))
            .collect());
        let physical = plan(&source, "{pod=~\".*\"}", &RangeQuery::new(600, 650, 100)).await;
        let ctx = SessionContext::new_with_config(SessionConfig::new().with_batch_size(3));
        let mut stream = lower(&physical).execute(0, ctx.task_ctx()).unwrap();
        let mut batches = Vec::new();
        let mut sample_backings = std::collections::HashSet::new();
        while let Some(batch) = stream.next().await {
            let batch = batch.unwrap();
            assert!(batch.num_rows() <= 3);
            let samples = batch.column_by_name(SAMPLES).unwrap().as_list::<i32>();
            assert_eq!(samples.offset(), 0);
            assert_eq!(samples.value_offsets()[0], 0);
            assert!(samples.values().len() <= 3);
            for child in samples.values().as_struct().columns() {
                let data = child.to_data();
                assert_eq!(data.offset(), 0);
                assert!(data.buffers()[0].capacity() <= samples.values().len() * 2 * 8);
                assert!(sample_backings.insert(data.buffers()[0].data_ptr().as_ptr() as usize));
            }
            let values = batch
                .column_by_name(LABELS)
                .unwrap()
                .as_struct()
                .column(0)
                .as_string_view();
            assert_eq!(values.views().len(), batch.num_rows());
            batches.push(batch);
        }
        assert_eq!(
            batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
            257
        );
        assert_eq!(ctx.runtime_env().memory_pool.reserved(), 0);
        assert!(answers(&batches)
            .iter()
            .all(|(_, p)| p == &vec![(600, 4f64.to_bits())]));
        assert_eq!(
            answers(&batches),
            answers(
                &reference(
                    &source,
                    "{pod=~\".*\"}",
                    &RangeQuery::new(600, 650, 100),
                    &ctx
                )
                .await
            )
        );
    }
}

#[tokio::test]
async fn literal_fixed_lookup_semantics_and_modifiers_keep_bits_and_overlap_precedence() {
    let stale = f64::from_bits(crate::selector::STALE_NAN_BITS);
    let nan = f64::from_bits(0x7ff8_0000_0000_0017);
    let source = Raw(vec![batch(
        &[
            (BLOCK, 1, "overlap", &[(350, 1.), (500, 2.), (650, 3.)]),
            (BLOCK, 1, "overlap", &[(400, 9.), (590, 7.), (700, 4.)]),
            (BLOCK, 2, "stale", &[(590, 1.), (600, stale)]),
            (BLOCK, 2, "stale", &[(600, 9.)]),
            (BLOCK, 3, "nan", &[(600, nan)]),
            (BLOCK, 4, "lower", &[(300, 8.)]),
            (BLOCK, 5, "after-lower", &[(301, -0.)]),
            (BLOCK, 6, "equals", &[(600, 1.), (600, -0.)]),
            (BLOCK, 7, "equals-stale", &[(600, 1.), (600, stale)]),
            (BLOCK, 8, "positive-infinity", &[(600, f64::INFINITY)]),
            (BLOCK, 9, "negative-infinity", &[(600, f64::NEG_INFINITY)]),
            (BLOCK, 10, "stale-equals", &[(600, stale), (600, 5.)]),
        ],
        false,
    )]);
    for (query, range) in [
        ("{pod=~\".*\"}", RangeQuery::new(600, 650, 100)),
        ("{pod=~\".*\"} offset 100ms", RangeQuery::new(700, 700, 100)),
        (
            "{pod=~\".*\"} offset -100ms",
            RangeQuery::new(500, 500, 100),
        ),
        (
            "{pod=~\".*\"} @ 0.7 offset 100ms",
            RangeQuery::new(600, 600, 100),
        ),
        ("{pod=~\".*\"} @ start()", RangeQuery::new(600, 600, 100)),
    ] {
        let range = RangeQuery {
            lookback_ms: 300,
            ..range
        };
        let physical = plan(&source, query, &range).await;
        let ctx = SessionContext::new_with_config(SessionConfig::new().with_batch_size(2));
        let trial = collect(physical, ctx.task_ctx()).await.unwrap();
        assert_eq!(
            answers(&trial),
            vec![
                (
                    "after-lower".into(),
                    vec![(range.start_ms, (-0f64).to_bits())]
                ),
                ("equals".into(), vec![(range.start_ms, (-0f64).to_bits())]),
                ("nan".into(), vec![(range.start_ms, nan.to_bits())]),
                (
                    "negative-infinity".into(),
                    vec![(range.start_ms, f64::NEG_INFINITY.to_bits())]
                ),
                ("overlap".into(), vec![(range.start_ms, 2f64.to_bits())]),
                (
                    "positive-infinity".into(),
                    vec![(range.start_ms, f64::INFINITY.to_bits())]
                ),
                (
                    "stale-equals".into(),
                    vec![(range.start_ms, 5f64.to_bits())]
                ),
            ],
            "{query}"
        );
        assert_eq!(
            answers(&trial),
            answers(&reference(&source, query, &range, &ctx).await)
        );
    }
    // @ end() resolves the external end (650), not the only grid step (600).
    let range = RangeQuery {
        lookback_ms: 300,
        ..RangeQuery::new(600, 650, 100)
    };
    let physical = plan(&source, "{pod=~\".*\"} @ end()", &range).await;
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_batch_size(2));
    let trial = collect(physical, ctx.task_ctx()).await.unwrap();
    let actual = answers(&trial);
    assert!(actual.contains(&("overlap".into(), vec![(600, 3f64.to_bits())])));
    assert!(!actual.iter().any(|(label, _)| label == "after-lower"));
    assert_eq!(
        actual,
        answers(&reference(&source, "{pod=~\".*\"} @ end()", &range, &ctx).await)
    );
}

#[tokio::test]
async fn same_id_across_blocks_closes_afresh_and_keeps_empty_rows() {
    let source = Raw(vec![batch(
        &[
            (block(0, 500), 7, "a", &[(400, 1.)]),
            (block(0, 500), 2, "b", &[(400, 4.)]),
            // Every answering block includes its available 200ms reach-back.
            (block(500, 700), 7, "a", &[(400, 1.), (590, 2.)]),
            (block(500, 700), 2, "b", &[(400, 4.)]),
            (block(700, 800), 7, "a", &[(590, 2.), (750, 3.)]),
        ],
        false,
    )]);
    let range = RangeQuery {
        lookback_ms: 200,
        ..RangeQuery::new(600, 750, 150)
    };
    let physical = plan(&source, "{pod=~\".*\"}", &range).await;
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_batch_size(1));
    let batches = collect(physical, ctx.task_ctx()).await.unwrap();
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 5);
    assert_eq!(
        batches
            .iter()
            .map(|batch| {
                batch
                    .column_by_name(SAMPLES)
                    .unwrap()
                    .as_list::<i32>()
                    .value_offsets()
                    .windows(2)
                    .filter(|offsets| offsets[0] == offsets[1])
                    .count()
            })
            .sum::<usize>(),
        3
    );
    assert_eq!(
        answers(&batches),
        vec![
            ("a".into(), vec![(600, 2f64.to_bits())]),
            ("a".into(), vec![(750, 3f64.to_bits())]),
        ]
    );
    assert_eq!(
        answers(&batches),
        answers(&reference(&source, "{pod=~\".*\"}", &range, &ctx).await)
    );
    assert_eq!(ctx.runtime_env().memory_pool.reserved(), 0);
}

#[tokio::test]
async fn sample_projection_does_not_retain_or_emit_label_columns_on_any_grid() {
    for binary in [false, true] {
        let source = Raw(vec![batch(
            &[
                (BLOCK, 1, "first-long-label-value", &[(500, 1.), (590, 2.)]),
                (BLOCK, 2, "second-long-label-value", &[(500, 3.), (590, 4.)]),
            ],
            binary,
        )]);
        for (query, range) in [
            ("count({pod=~\".*\"})", RangeQuery::new(600, 600, 1)),
            ("count({pod=~\".*\"})", RangeQuery::new(600, 700, 50)),
            (
                "sum(rate({pod=~\".*\"}[1s]))",
                RangeQuery::new(600, 700, 50),
            ),
        ] {
            let physical = plan(&source, query, &range).await;
            let exec = lower(&physical);
            assert!(!exec.projection.contains(&0));
            assert!(exec.schema().field_with_name(LABELS).is_err());
            let ctx = SessionContext::new();
            let mut owned = private_stream(exec, &ctx, 1);
            owned.update(&source.0[0], 0).unwrap();
            assert!(owned.current.as_ref().unwrap().labels.is_none());
            assert_eq!(owned.current.as_ref().unwrap().label_bytes, 0);
            drop(owned);
            let mut stream = exec.execute(0, ctx.task_ctx()).unwrap();
            let first = stream.next().await.unwrap().unwrap();
            assert_eq!(first.num_rows(), 2);
            assert!(first.column_by_name(LABELS).is_none());
            assert!(stream.next().await.is_none());
            assert_eq!(ctx.runtime_env().memory_pool.reserved(), 0);
        }
    }
}

#[tokio::test]
async fn reservation_errors_and_cancellation_release_actual_state() {
    let labels: Vec<_> = (0..80).map(|i| format!("long-label-{i}")).collect();
    let samples = [(590, 1.)];
    let rows: Vec<_> = labels
        .iter()
        .enumerate()
        .map(|(i, l)| (BLOCK, i as u64, l.as_str(), &samples[..]))
        .collect();
    let all = batch(&rows, false);
    let source = Raw(vec![all.clone(), all.slice(0, 0)]);
    let cancellation_limit = 2 * all.num_rows() * all.get_array_memory_size();
    for limit in [512, 4096, cancellation_limit] {
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_pool(Arc::new(GreedyMemoryPool::new(limit)))
            .build_arc()
            .unwrap();
        let ctx =
            SessionContext::new_with_config_rt(SessionConfig::new().with_batch_size(1), runtime);
        let physical = plan(&source, "{pod=~\".*\"}", &RangeQuery::new(600, 600, 1)).await;
        let mut stream = lower(&physical).execute(0, ctx.task_ctx()).unwrap();
        let first = stream.next().await.unwrap();
        if limit < cancellation_limit {
            assert!(first
                .unwrap_err()
                .to_string()
                .contains("Resources exhausted"));
            assert!(stream.next().await.is_none());
            assert_eq!(ctx.runtime_env().memory_pool.reserved(), 0);
        } else {
            assert_eq!(first.unwrap().num_rows(), 1);
            assert!(ctx.runtime_env().memory_pool.reserved() > 0);
            drop(stream);
            assert_eq!(ctx.runtime_env().memory_pool.reserved(), 0);
        }
    }
}

#[tokio::test]
async fn one_owned_id_and_label_capture_survive_only_the_current_series() {
    for binary in [false, true] {
        for query in [
            "{pod=~\".*\"}",
            "rate({pod=~\".*\"}[1s])",
            "min_over_time({pod=~\".*\"}[1s])",
            "changes({pod=~\".*\"}[1s])",
        ] {
            let source = Raw(vec![batch(
                &[(BLOCK, 9, "first-long-label-value", &[(590, 1.)])],
                binary,
            )]);
            let physical = plan(&source, query, &RangeQuery::new(600, 800, 50)).await;
            let ctx = SessionContext::new();
            let mut stream = private_stream(lower(&physical), &ctx, 1);
            stream.input_bytes = source.0[0].get_array_memory_size();
            stream.update(&source.0[0], 0).unwrap();
            assert_charged(&stream);
            let labels_ptr = stream.current.as_ref().unwrap().labels.as_ref().unwrap()[0].as_ptr();
            let id_bytes = stream.current.as_ref().unwrap().id.heap_size();
            assert_eq!(id_bytes, if binary { 16 } else { 0 });
            for step in 0..128 {
                let samples = [(591 + step, if step % 2 == 0 { 4. } else { 1. })];
                let input = batch(&[(BLOCK, 9, "first-long-label-value", &samples)], binary);
                stream.input_bytes = input.get_array_memory_size();
                stream.update(&input, 0).unwrap();
                assert_charged(&stream);
                assert_eq!(
                    stream.current.as_ref().unwrap().labels.as_ref().unwrap()[0].as_ptr(),
                    labels_ptr
                );
                assert_eq!(stream.current.as_ref().unwrap().id.heap_size(), id_bytes);
            }
            stream.input_bytes = 0;
            stream.charge(0).unwrap();
            assert!(stream.finish_series().unwrap());
            assert!(stream.current.is_none());
            assert_charged(&stream);
            let output = stream.emit().unwrap();
            stream.release();
            assert_eq!(ctx.runtime_env().memory_pool.reserved(), 0);
            assert_eq!(
                output
                    .column(0)
                    .as_struct()
                    .column(0)
                    .as_string_view()
                    .value(0),
                "first-long-label-value"
            );
        }
    }
}

#[tokio::test]
async fn native_resource_total_overflow_does_not_grow_the_reservation() {
    let source = Raw(vec![batch(&[(BLOCK, 1, "first", &[(590, 1.)])], true)]);
    let physical = plan(&source, "{pod=~\".*\"}", &RangeQuery::new(600, 600, 1)).await;
    let ctx = SessionContext::new();
    let mut stream = private_stream(lower(&physical), &ctx, 1);
    stream.input_bytes = usize::MAX;
    assert!(matches!(
        stream.size().unwrap_err(),
        datafusion::error::DataFusionError::ResourcesExhausted(message)
            if message == "series evaluation memory charge overflow"
    ));
    assert!(stream
        .charge(0)
        .unwrap_err()
        .to_string()
        .contains("overflow"));
    assert_eq!(ctx.runtime_env().memory_pool.reserved(), 0);
    stream.input_bytes = 0;
    assert!(stream
        .charge(usize::MAX)
        .unwrap_err()
        .to_string()
        .contains("overflow"));
    stream.input_bytes = source.0[0].get_array_memory_size();
    stream.update(&source.0[0], 0).unwrap();
    let admitted = stream.reservation.size();
    stream.current.as_mut().unwrap().label_bytes = usize::MAX;
    assert!(stream.current.as_ref().unwrap().size().is_err());
    assert!(stream
        .charge(0)
        .unwrap_err()
        .to_string()
        .contains("overflow"));
    assert_eq!(stream.reservation.size(), admitted);
    let later = batch(&[(BLOCK, 1, "first", &[(599, 2.)])], true);
    assert!(stream
        .update(&later, 0)
        .unwrap_err()
        .to_string()
        .contains("overflow"));
    assert_eq!(stream.reservation.size(), admitted);
    stream.release();
    assert_eq!(ctx.runtime_env().memory_pool.reserved(), 0);
}

const KERNEL_NAMES: [&str; 16] = [
    "selector",
    "timestamp",
    "rate",
    "increase",
    "delta",
    "irate",
    "idelta",
    "sum_over_time",
    "avg_over_time",
    "min_over_time",
    "max_over_time",
    "count_over_time",
    "last_over_time",
    "present_over_time",
    "changes",
    "resets",
];
// Translate sample timestamps with offsets, but keep evaluated timestamps on
// 650..950. Pinned windows have one binding block that answers all seven steps.
const MODIFIER_BLOCKS: [(&str, i64, Block); 5] = [
    (
        "",
        0,
        Block {
            start_ms: 650,
            end_ms: 951,
        },
    ),
    (
        " offset 1s",
        -1000,
        Block {
            start_ms: -350,
            end_ms: -49,
        },
    ),
    (
        " offset -1s",
        1000,
        Block {
            start_ms: 1650,
            end_ms: 1951,
        },
    ),
    (
        " @ 0.7",
        0,
        Block {
            start_ms: 700,
            end_ms: 701,
        },
    ),
    (
        " @ end() offset 1s",
        -1000,
        Block {
            start_ms: -50,
            end_ms: -49,
        },
    ),
];
fn kernel_query(name: &str, modifier: &str) -> String {
    match name {
        "selector" => format!("{{pod=~\".*\"}}{modifier}"),
        "timestamp" => format!("timestamp({{pod=~\".*\"}}{modifier})"),
        _ => format!("{name}({{pod=~\".*\"}}[1s]{modifier})"),
    }
}

#[tokio::test]
async fn all_kernels_and_multistep_modifiers_match_one_chunk_with_bounded_batches() {
    let stale = f64::from_bits(crate::selector::STALE_NAN_BITS);
    let nan = f64::from_bits(0x7ff8_0000_0000_1234);
    let names: Vec<_> = (0..17).map(|i| format!("pod-{i}")).collect();
    let range = RangeQuery {
        lookback_ms: 1000,
        ..RangeQuery::new(650, 950, 50)
    };
    for binary in [false, true] {
        for (modifier, shift, bounds) in MODIFIER_BLOCKS {
            let first = [(50 + shift, 1.), (200 + shift, -0.), (500 + shift, stale)];
            let second = [(200 + shift, 99.), (600 + shift, 3.), (900 + shift, nan)];
            let rows: Vec<_> = names
                .iter()
                .enumerate()
                .flat_map(|(i, name)| {
                    [
                        (bounds, i as u64, name.as_str(), &first[..]),
                        (bounds, i as u64, name.as_str(), &second[..]),
                    ]
                })
                .collect();
            let all = batch(&rows, binary);
            // Empty batches, unlike empty raw chunks, are legal mid-identity.
            let source = Raw(vec![
                all.slice(0, 1),
                all.slice(0, 0),
                all.slice(1, all.num_rows() - 1),
            ]);
            for name in KERNEL_NAMES {
                let query = kernel_query(name, modifier);
                let physical = plan(&source, &query, &range).await;
                let ctx = SessionContext::new_with_config(SessionConfig::new().with_batch_size(3));
                let expected = reference(&source, &query, &range, &ctx).await;
                let actual = collect(physical, ctx.task_ctx()).await.unwrap();
                assert!(actual.iter().all(|batch| batch.num_rows() <= 3));
                let actual = answers(&actual);
                assert_eq!(actual.len(), names.len(), "binary={binary} query={query}");
                assert!(
                    actual.iter().all(|(_, points)| points.len() == 7),
                    "{query}"
                );
                assert_eq!(actual, answers(&expected), "binary={binary} query={query}");
                assert_eq!(ctx.runtime_env().memory_pool.reserved(), 0);
            }
        }
    }
}

#[tokio::test]
async fn all_kernels_and_modifiers_have_independent_nonempty_literal_answers() {
    let range = RangeQuery {
        lookback_ms: 1000,
        ..RangeQuery::new(650, 950, 50)
    };
    for binary in [false, true] {
        for (modifier, shift, bounds) in MODIFIER_BLOCKS {
            // These three constant samples stay inside every 1s window. The
            // overlap's 99 must lose to the earlier chunk's 4. No reference
            // executor or numerical sizing helper supplies the expected values.
            let first = [(100 + shift, 4.), (300 + shift, 4.)];
            let second = [(300 + shift, 99.), (600 + shift, 4.)];
            let all = batch(
                &[(bounds, 9, "pod", &first), (bounds, 9, "pod", &second)],
                binary,
            );
            let source = Raw(vec![all.slice(0, 1), all.slice(0, 0), all.slice(1, 1)]);
            for name in KERNEL_NAMES {
                let query = kernel_query(name, modifier);
                let value: f64 = match name {
                    "selector" | "avg_over_time" | "min_over_time" | "max_over_time"
                    | "last_over_time" => 4.,
                    "timestamp" => (600 + shift) as f64 / 1000.,
                    "sum_over_time" => 12.,
                    "count_over_time" => 3.,
                    "present_over_time" => 1.,
                    "rate" | "increase" | "delta" | "irate" | "idelta" | "changes" | "resets" => 0.,
                    _ => unreachable!(),
                };
                let ctx = SessionContext::new_with_config(SessionConfig::new().with_batch_size(1));
                let actual = collect(plan(&source, &query, &range).await, ctx.task_ctx())
                    .await
                    .unwrap();
                assert_eq!(
                    answers(&actual),
                    vec![(
                        "pod".into(),
                        (650..=950)
                            .step_by(50)
                            .map(|t| (t, value.to_bits()))
                            .collect(),
                    )],
                    "binary={binary} query={query}"
                );
                assert_eq!(ctx.runtime_env().memory_pool.reserved(), 0);
            }
        }
    }
}

#[tokio::test]
async fn child_replacement_binds_columns_without_restricting_source_operators() {
    let source = Raw(vec![batch(&[(BLOCK, 1, "a", &[(600, 1.)])], false)]);
    let physical = plan(&source, "{pod=~\".*\"}", &RangeQuery::new(600, 600, 1)).await;
    let exec = lower(&physical);
    assert!(matches!(
        exec.input_distribution_requirements()
            .per_child_distributions()
            .next(),
        Some(Distribution::UnspecifiedDistribution)
    ));
    assert_eq!(
        exec.required_input_ordering()[0]
            .as_ref()
            .unwrap()
            .first()
            .len(),
        1
    );
    assert_eq!(exec.benefits_from_input_partitioning(), vec![false]);
    assert_eq!(exec.schema().fields().len(), 4);
    assert!(matches!(
        exec.properties().partitioning,
        Partitioning::UnknownPartitioning(1)
    ));
    let input = exec.input.clone();
    let instance =
        Arc::new(SeriesEvalExec::try_new(input.clone(), exec.kernel, exec.grid).unwrap());
    let options = ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute);
    assert!(instance.clone().replace_children(vec![], options).is_err());
    assert!(instance
        .clone()
        .replace_children(vec![input.clone(), input.clone()], options)
        .is_err());
    let reordered = ProjectionExec::try_new(
        [SERIES_ID, BLOCK_END, SAMPLES, LABELS, BLOCK_START]
            .into_iter()
            .map(|name| {
                (
                    Arc::new(Column::new_with_schema(name, &input.schema()).unwrap())
                        as Arc<dyn PhysicalExpr>,
                    name.to_owned(),
                )
            })
            .collect::<Vec<_>>(),
        input,
    )
    .unwrap();
    let rebuilt = instance
        .replace_children(vec![Arc::new(reordered)], options)
        .unwrap();
    let output = collect(rebuilt, SessionContext::new().task_ctx())
        .await
        .unwrap();
    assert_eq!(
        answers(&output),
        vec![("a".into(), vec![(600, 1f64.to_bits())])]
    );
}

#[tokio::test]
async fn instant_functions_grouping_and_final_duplicate_checks_are_unchanged() {
    use crate::{MemorySeriesSource, Series, SeriesIdEncoding};
    use std::num::NonZeroUsize;
    let rows = vec![
        Series::new(&[("__name__", "a"), ("pod", "p")], vec![590], vec![-0.]).unwrap(),
        Series::new(&[("__name__", "b"), ("pod", "p")], vec![590], vec![-2.]).unwrap(),
    ];
    for id in [SeriesIdEncoding::UInt64, SeriesIdEncoding::FixedSizeBinary] {
        for partitions in [1, 4] {
            let source = MemorySeriesSource::try_new(rows.clone())
                .unwrap()
                .with_ids(id)
                .partitions(NonZeroUsize::new(partitions).unwrap())
                .rows_per_batch(NonZeroUsize::new(1).unwrap());
            let engine = Engine::new();
            let range = RangeQuery::new(600, 650, 100);
            assert_eq!(
                answers(
                    &engine
                        .range_query_async(&source, "count({pod=\"p\"})", &range)
                        .await
                        .unwrap()
                ),
                vec![("".into(), vec![(600, 2f64.to_bits())])]
            );
            assert_eq!(
                answers(
                    &engine
                        .range_query_async(&source, "sum by(pod)({pod=\"p\"})", &range)
                        .await
                        .unwrap()
                ),
                vec![("p".into(), vec![(600, (-2f64).to_bits())])]
            );
            let error = engine
                .range_query_async(&source, "abs({pod=\"p\"})", &range)
                .await
                .unwrap_err();
            assert!(matches!(error, EngineError::Query(_)));
            assert_eq!(error.to_string(), crate::labelset::SAME_LABELSET);
            let result = engine
                .range_query_async(&source, "abs(a)", &range)
                .await
                .unwrap();
            assert_eq!(
                answers(&result),
                vec![("p".into(), vec![(600, 0f64.to_bits())])]
            );
            assert_eq!(series::label_names(&result[0].schema()), ["pod"]);
        }
    }
}

#[tokio::test]
async fn large_finalized_fold_output_exhausts_the_pool_without_emitting() {
    let source = Raw(vec![batch(
        &[(block(0, 200_000), 1, "pod", &[(0, 1.)])],
        false,
    )]);
    let physical = plan(&source, "{pod=~\".*\"}", &RangeQuery::new(0, 100_000, 1)).await;
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::new(GreedyMemoryPool::new(1_000_000)))
        .build_arc()
        .unwrap();
    let ctx = SessionContext::new_with_config_rt(SessionConfig::new(), runtime);
    let exec = lower(&physical);
    let mut stream = exec.execute(0, ctx.task_ctx()).unwrap();
    assert!(matches!(futures::poll!(stream.next()), Poll::Pending));
    assert!(ctx.runtime_env().memory_pool.reserved() < 10_000);
    let error = stream.next().await.unwrap().unwrap_err();
    assert!(error.to_string().contains("Resources exhausted"));
    assert_eq!(exec.metrics.clone_inner().output_rows().unwrap(), 0);
    assert!(stream.next().await.is_none());
    assert_eq!(ctx.runtime_env().memory_pool.reserved(), 0);
}

#[tokio::test]
async fn range_results_roll_by_samples_without_whole_block_backing() {
    let span = block(0, 800_000);
    let chunks = [(0, 1.), (699_999, 2.)];
    let source = Raw(vec![batch(
        &[
            (span, 1, "a", &chunks),
            (span, 2, "b", &chunks),
            (span, 3, "c", &chunks),
            (span, 4, "d", &chunks),
        ],
        false,
    )]);
    let physical = plan(&source, "{pod=~\".*\"}", &RangeQuery::new(0, 700_000, 1)).await;
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_batch_size(8192));
    let mut stream = lower(&physical).execute(0, ctx.task_ctx()).unwrap();
    let mut rows = 0;
    let mut batches = 0;
    while let Some(batch) = stream.next().await {
        let batch = batch.unwrap();
        let samples = batch.column(1).as_list::<i32>();
        assert!(samples.values().len() <= BATCH_SAMPLES);
        for column in samples.values().as_struct().columns() {
            // Ordinary Vec builders may retain at most their doubling slack,
            // never the four-series block or another emitted batch's backing.
            assert!(column.to_data().buffers()[0].capacity() <= BATCH_SAMPLES * 2 * 8);
        }
        for offsets in samples.value_offsets().windows(2) {
            assert_eq!(offsets[1] - offsets[0], 300_002);
        }
        rows += batch.num_rows();
        batches += 1;
    }
    assert_eq!(rows, 4);
    assert_eq!(batches, 2);
    assert_eq!(ctx.runtime_env().memory_pool.reserved(), 0);
}

#[tokio::test]
async fn canonical_column_pruning_fuses_and_preserves_source_dependencies() {
    let source = Raw(vec![batch(&[(BLOCK, 1, "pod", &[(590, -0.)])], false)]);
    let physical = plan(&source, "{pod=~\".*\"}", &RangeQuery::new(600, 600, 1)).await;
    let exec = lower(&physical);
    let base: Arc<dyn ExecutionPlan> =
        Arc::new(SeriesEvalExec::try_new(exec.input.clone(), exec.kernel, exec.grid).unwrap());
    for indices in [
        vec![0, 1],
        vec![1],
        vec![1, 2, 3],
        vec![0],
        vec![2, 3],
        vec![],
    ] {
        let schema = base.schema();
        let expressions = indices.iter().map(|&index| ProjectionExpr {
            expr: Arc::new(Column::new(schema.field(index).name(), index)),
            alias: schema.field(index).name().clone(),
        });
        let projection = ProjectionExec::try_new(expressions, base.clone()).unwrap();
        let fused = base
            .try_swapping_with_projection(&projection)
            .unwrap()
            .unwrap();
        assert!(fused.is::<SeriesEvalExec>());
        assert_eq!(lower(&fused).projection, indices);
        assert_eq!(fused.schema(), projection.schema());
        assert_eq!(fused.children()[0].schema(), base.children()[0].schema());
        assert_eq!(fused.children()[0].schema().fields().len(), 5);
        assert_eq!(
            fused.properties().output_ordering().is_some(),
            indices.contains(&2)
        );
        let ctx = SessionContext::new();
        let actual = collect(fused, ctx.task_ctx()).await.unwrap();
        let expected = collect(Arc::new(projection), ctx.task_ctx()).await.unwrap();
        assert_eq!(actual, expected);
        assert_eq!(actual[0].num_rows(), 1);
        assert_eq!(ctx.runtime_env().memory_pool.reserved(), 0);
    }
}

#[tokio::test]
async fn aliases_reordering_and_duplicates_remain_native_with_identical_results_and_metadata() {
    let input = batch(&[(BLOCK, 1, "long-label-value", &[(590, -0.)])], true);
    let metadata = std::collections::HashMap::from([("query".into(), "preserved".into())]);
    let fields: Vec<_> = input
        .schema()
        .fields()
        .iter()
        .map(|f| f.as_ref().clone().with_metadata(metadata.clone()))
        .collect();
    let input = RecordBatch::try_new(
        Arc::new(Schema::new_with_metadata(fields, metadata.clone())),
        input.columns().to_vec(),
    )
    .unwrap();
    let source = Raw(vec![input]);
    let physical = plan(&source, "{pod=~\".*\"}", &RangeQuery::new(600, 600, 1)).await;
    let exec = lower(&physical);
    let base: Arc<dyn ExecutionPlan> =
        Arc::new(SeriesEvalExec::try_new(exec.input.clone(), exec.kernel, exec.grid).unwrap());
    for columns in [
        vec![(SAMPLES, 1, "values")],
        vec![(SAMPLES, 1, SAMPLES), (LABELS, 0, LABELS)],
        vec![(SAMPLES, 1, SAMPLES), (SAMPLES, 1, SAMPLES)],
        vec![
            (BLOCK_END, 3, BLOCK_END),
            (SAMPLES, 1, SAMPLES),
            (BLOCK_START, 2, BLOCK_START),
            (LABELS, 0, LABELS),
        ],
        vec![
            (SAMPLES, 1, "values"),
            (BLOCK_START, 2, "start"),
            (SAMPLES, 1, "values_again"),
        ],
    ] {
        let projected = project_columns(base.clone(), &columns);
        assert!(projected.is::<ProjectionExec>());
        let pruned = lower(&projected);
        assert_eq!(
            pruned.projection.contains(&0),
            columns.iter().any(|&(name, _, _)| name == LABELS)
        );
        assert_eq!(pruned.input.schema(), exec.input.schema());
        let native = ProjectionExec::try_new(
            columns.iter().map(|&(name, index, alias)| ProjectionExpr {
                expr: Arc::new(Column::new(name, index)),
                alias: alias.into(),
            }),
            base.clone(),
        )
        .unwrap();
        assert_eq!(projected.schema(), native.schema());
        let ctx = SessionContext::new();
        let batches = collect(projected, ctx.task_ctx()).await.unwrap();
        assert_eq!(
            batches,
            collect(Arc::new(native), ctx.task_ctx()).await.unwrap()
        );
        assert_eq!(batches[0].schema().metadata(), &metadata);
        let sample_indices: Vec<_> = columns
            .iter()
            .enumerate()
            .filter_map(|(index, &(name, _, _))| (name == SAMPLES).then_some(index))
            .collect();
        if sample_indices.len() == 2 {
            assert!(Arc::ptr_eq(
                batches[0].column(sample_indices[0]),
                batches[0].column(sample_indices[1])
            ));
        }
        let (ts, vs) = series::sample_slices(
            batches[0]
                .column(sample_indices[0])
                .as_list::<i32>()
                .values()
                .as_struct(),
        );
        assert_eq!(ts, &[600]);
        assert_eq!(vs[0].to_bits(), (-0f64).to_bits());
        assert_eq!(ctx.runtime_env().memory_pool.reserved(), 0);
    }
}

#[tokio::test]
async fn arbitrary_projection_expressions_remain_native() {
    use datafusion::common::ScalarValue;
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::expressions::{BinaryExpr, Literal};

    let source = Raw(vec![batch(&[(BLOCK, 1, "pod", &[(590, 1.)])], false)]);
    let physical = plan(&source, "{pod=~\".*\"}", &RangeQuery::new(600, 600, 1)).await;
    let exec = lower(&physical);
    let base: Arc<dyn ExecutionPlan> =
        Arc::new(SeriesEvalExec::try_new(exec.input.clone(), exec.kernel, exec.grid).unwrap());
    let ctx = SessionContext::new();
    for expr in [
        Arc::new(Literal::new(ScalarValue::Int64(Some(1)))) as Arc<dyn PhysicalExpr>,
        Arc::new(BinaryExpr::new(
            Arc::new(Column::new(BLOCK_START, 2)),
            Operator::Eq,
            Arc::new(Column::new(BLOCK_END, 3)),
        )),
    ] {
        let projection = Arc::new(
            ProjectionExec::try_new(vec![(expr, "expression".into())], base.clone()).unwrap(),
        );
        assert!(base
            .try_swapping_with_projection(&projection)
            .unwrap()
            .is_none());
        let optimized = ProjectionPushdown::new()
            .optimize(
                projection.clone(),
                &datafusion::common::config::ConfigOptions::new(),
            )
            .unwrap();
        assert!(optimized.is::<ProjectionExec>());
        assert_eq!(lower(&optimized).projection, vec![0, 1, 2, 3]);
        assert_eq!(
            collect(optimized, ctx.task_ctx()).await.unwrap(),
            collect(projection, ctx.task_ctx()).await.unwrap()
        );
    }
}

#[tokio::test]
async fn nested_projections_preserve_name_binding_child_replacement_and_optimizer_fixpoint() {
    let source = Raw(vec![batch(&[(BLOCK, 1, "pod", &[(590, -0.)])], true)]);
    let physical = plan(&source, "{pod=~\".*\"}", &RangeQuery::new(600, 600, 1)).await;
    let exec = lower(&physical);
    let base: Arc<dyn ExecutionPlan> =
        Arc::new(SeriesEvalExec::try_new(exec.input.clone(), exec.kernel, exec.grid).unwrap());
    let pruned = project_columns(
        base,
        &[(SAMPLES, 1, SAMPLES), (BLOCK_START, 2, BLOCK_START)],
    );
    let presented = project_columns(
        pruned,
        &[
            (BLOCK_START, 1, "start"),
            (SAMPLES, 0, "values"),
            (SAMPLES, 0, "again"),
        ],
    );
    let nested: Arc<dyn ExecutionPlan> = Arc::new(
        ProjectionExec::try_new(
            vec![(
                Arc::new(Column::new("again", 2)) as Arc<dyn PhysicalExpr>,
                "result".into(),
            )],
            presented,
        )
        .unwrap(),
    );
    let expected_schema = nested.schema();
    let ctx = SessionContext::new();
    let expected = collect(nested.clone(), ctx.task_ctx()).await.unwrap();
    let optimizer = ProjectionPushdown::new();
    let config = datafusion::common::config::ConfigOptions::new();
    let mut optimized = optimizer.optimize(nested, &config).unwrap();
    let shape = displayable(optimized.as_ref()).indent(true).to_string();
    for _ in 0..4 {
        optimized = optimizer.optimize(optimized, &config).unwrap();
        assert_eq!(
            displayable(optimized.as_ref()).indent(true).to_string(),
            shape
        );
        assert!(optimized.is::<ProjectionExec>());
        assert_eq!(optimized.schema(), expected_schema);
        assert_eq!(lower(&optimized).projection, vec![1]);
        assert!(optimized.properties().output_ordering().is_none());
        assert_eq!(
            collect(optimized.clone(), ctx.task_ctx()).await.unwrap(),
            expected
        );
    }
    let pruned = optimized.children()[0].clone();
    assert!(pruned.is::<SeriesEvalExec>());
    let rebuilt = pruned
        .clone()
        .replace_children(
            vec![exec.input.clone()],
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )
        .unwrap();
    assert_eq!(rebuilt.schema(), pruned.schema());
    assert_eq!(lower(&rebuilt).projection, vec![1]);
    assert_eq!(ctx.runtime_env().memory_pool.reserved(), 0);
}

#[tokio::test]
async fn label_bearing_projection_keeps_aliases_copied_values_and_nested_metadata() {
    let input = batch(
        &[
            (BLOCK, 1, "first-long-label-value", &[(590, -0.)]),
            (BLOCK, 2, "second-long-label-value", &[(590, 2.)]),
        ],
        true,
    );
    let nested = std::collections::HashMap::from([("label".into(), "nested".into())]);
    let outer = std::collections::HashMap::from([("query".into(), "preserved".into())]);
    let labels = StructArray::new(
        vec![Field::new("pod", DataType::Utf8View, false).with_metadata(nested.clone())].into(),
        input.column(0).as_struct().columns().to_vec(),
        None,
    );
    let mut fields = input.schema().fields().to_vec();
    fields[0] = Arc::new(
        Field::new(LABELS, labels.data_type().clone(), false).with_metadata(outer.clone()),
    );
    let mut columns = input.columns().to_vec();
    columns[0] = Arc::new(labels);
    let input = RecordBatch::try_new(
        Arc::new(Schema::new_with_metadata(fields, outer.clone())),
        columns,
    )
    .unwrap();
    let payload = input
        .column(0)
        .as_struct()
        .column(0)
        .as_string_view()
        .data_buffers()[0]
        .data_ptr();
    let source = Raw(vec![input]);
    let physical = plan(&source, "{pod=~\".*\"}", &RangeQuery::new(600, 600, 1)).await;
    let exec = lower(&physical);
    let base =
        Arc::new(SeriesEvalExec::try_new(exec.input.clone(), exec.kernel, exec.grid).unwrap());
    let projected = project_columns(
        base,
        &[
            (LABELS, 0, "labels_one"),
            (LABELS, 0, "labels_two"),
            (SAMPLES, 1, "values"),
        ],
    );
    assert!(projected.is::<ProjectionExec>());
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_batch_size(1));
    let output = collect(projected, ctx.task_ctx()).await.unwrap();
    assert_eq!(ctx.runtime_env().memory_pool.reserved(), 0);
    for (row, batch) in output.iter().enumerate() {
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.schema().metadata(), &outer);
        assert_eq!(batch.schema().field(0).name(), "labels_one");
        assert_eq!(batch.schema().field(0).metadata(), &outer);
        assert!(Arc::ptr_eq(batch.column(0), batch.column(1)));
        let labels = batch.column(0).as_struct();
        assert_eq!(labels.fields()[0].metadata(), &nested);
        assert_ne!(
            labels.column(0).as_string_view().data_buffers()[0].data_ptr(),
            payload
        );
        assert_eq!(
            labels.column(0).as_string_view().value(0),
            ["first-long-label-value", "second-long-label-value"][row]
        );
    }
}

fn metadata_sample_item() -> FieldRef {
    let fields: Fields = series::sample_fields()
        .iter()
        .enumerate()
        .map(|(i, field)| {
            Arc::new(
                field
                    .as_ref()
                    .clone()
                    .with_metadata(std::collections::HashMap::from([(
                        "sample-child".into(),
                        ["timestamp-ms", "value-bits"][i].into(),
                    )])),
            )
        })
        .collect();
    Arc::new(
        Field::new("item", DataType::Struct(fields), false).with_metadata(
            std::collections::HashMap::from([("sample-item".into(), "preserved".into())]),
        ),
    )
}

#[test]
fn declared_sample_fields_rewrap_without_copying_offsets_or_sample_payloads() {
    let nan = f64::from_bits(0x7ff8_0000_0000_0017);
    let mut builder = SamplesBuilder::default();
    builder.push(600, -0.);
    builder.push(650, nan);
    builder.finish_row();
    builder.finish_row(); // Empty evaluated rows are legal.
    let canonical = builder.take_all();
    let item = metadata_sample_item();
    let output = samples_with_declared_fields(canonical.clone(), &item).unwrap();
    assert_eq!(output.data_type(), &DataType::List(item));
    assert_eq!(output.value_offsets(), &[0, 2, 2]);
    assert_eq!(
        output.value_offsets().as_ptr(),
        canonical.value_offsets().as_ptr()
    );
    for (actual, original) in output
        .values()
        .as_struct()
        .columns()
        .iter()
        .zip(canonical.values().as_struct().columns())
    {
        assert!(Arc::ptr_eq(actual, original));
        assert_eq!(
            actual.to_data().buffers()[0].data_ptr(),
            original.to_data().buffers()[0].data_ptr()
        );
    }
    let (ts, vs) = series::sample_slices(output.values().as_struct());
    assert_eq!(ts, &[600, 650]);
    assert_eq!(
        vs.iter().map(|v| v.to_bits()).collect::<Vec<_>>(),
        [(-0f64).to_bits(), nan.to_bits()]
    );
}

#[tokio::test]
async fn legal_nested_sample_metadata_matches_canonical_and_projected_output_types() {
    let nan = f64::from_bits(0x7ff8_0000_0000_0017);
    for binary in [false, true] {
        let input = batch(
            &[
                (BLOCK, 1, "first", &[(500, 1.)]),
                (BLOCK, 1, "first", &[(590, -0.)]),
                (BLOCK, 2, "second", &[(580, nan)]),
            ],
            binary,
        );
        let item = metadata_sample_item();
        let DataType::Struct(sample_fields) = item.data_type() else {
            unreachable!()
        };
        // Construct a genuinely legal raw array with the decorated nested
        // fields, not just a schema that lies about canonical array types.
        let raw_samples = input.column(1).as_list::<i32>();
        let samples = ListArray::new(
            item.clone(),
            raw_samples.offsets().clone(),
            Arc::new(StructArray::new(
                sample_fields.clone(),
                raw_samples.values().as_struct().columns().to_vec(),
                None,
            )),
            None,
        );
        let metadata = std::collections::HashMap::from([("source".into(), "preserved".into())]);
        let mut fields = input.schema().fields().to_vec();
        fields[1] = Arc::new(
            Field::new(SAMPLES, samples.data_type().clone(), false).with_metadata(metadata.clone()),
        );
        let mut columns = input.columns().to_vec();
        columns[1] = Arc::new(samples);
        let input = RecordBatch::try_new(
            Arc::new(Schema::new_with_metadata(fields, metadata.clone())),
            columns,
        )
        .unwrap();
        let declared_type = input.schema().field(1).data_type().clone();
        assert_eq!(input.column(1).data_type(), &declared_type);
        let raw_children = input
            .column(1)
            .as_list::<i32>()
            .values()
            .as_struct()
            .columns()
            .to_vec();
        let source = Raw(vec![
            input.slice(0, 1),
            input.slice(0, 0),
            input.slice(1, 2),
        ]);
        let physical = plan(&source, "{pod=~\".*\"}", &RangeQuery::new(600, 600, 1)).await;
        let base = lower(&physical);
        for mode in 0..4 {
            let mut exec: Arc<dyn ExecutionPlan> = Arc::new(
                SeriesEvalExec::try_new(base.input.clone(), base.kernel, base.grid).unwrap(),
            );
            if mode != 0 {
                let columns = match mode {
                    1 => vec![
                        (SAMPLES, 1, "values"),
                        (LABELS, 0, "identity"),
                        (SAMPLES, 1, "again"),
                        (BLOCK_START, 2, "start"),
                    ],
                    2 => vec![(SAMPLES, 1, "values"), (SAMPLES, 1, "again")],
                    3 => vec![(LABELS, 0, "identity")],
                    _ => unreachable!(),
                };
                exec = project_columns(exec, &columns);
                assert!(exec.is::<ProjectionExec>());
            }
            let expected_schema = exec.schema();
            let ctx = SessionContext::new_with_config(SessionConfig::new().with_batch_size(1));
            let output = collect(exec, ctx.task_ctx()).await.unwrap();
            assert_eq!(output.len(), 2);
            for (row, batch) in output.iter().enumerate() {
                assert_eq!(batch.num_rows(), 1);
                assert_eq!(batch.schema(), expected_schema);
                assert_eq!(batch.schema().metadata(), &metadata);
                for (field, array) in batch.schema().fields().iter().zip(batch.columns()) {
                    assert_eq!(array.data_type(), field.data_type());
                }
                if mode != 2 {
                    let labels = batch.column(if mode == 1 { 1 } else { 0 }).as_struct();
                    assert_eq!(
                        labels.column(0).as_string_view().value(0),
                        ["first", "second"][row]
                    );
                }
                if mode == 3 {
                    continue;
                }
                let column = if mode == 0 { 1 } else { 0 };
                assert_eq!(batch.schema().field(column).metadata(), &metadata);
                let samples = batch.column(column).as_list::<i32>();
                assert_eq!(samples.data_type(), &declared_type);
                assert_eq!(samples.values().as_struct().fields(), sample_fields);
                if mode == 1 {
                    assert!(Arc::ptr_eq(batch.column(0), batch.column(2)));
                }
                if mode == 2 {
                    assert!(Arc::ptr_eq(batch.column(0), batch.column(1)));
                }
                let (ts, vs) = series::sample_slices(samples.values().as_struct());
                assert_eq!(ts, &[600]);
                assert_eq!(vs[0].to_bits(), [(-0f64).to_bits(), nan.to_bits()][row]);
                for (actual, raw) in samples
                    .values()
                    .as_struct()
                    .columns()
                    .iter()
                    .zip(&raw_children)
                {
                    assert_ne!(
                        actual.to_data().buffers()[0].data_ptr(),
                        raw.to_data().buffers()[0].data_ptr()
                    );
                }
            }
            assert_eq!(ctx.runtime_env().memory_pool.reserved(), 0);
        }
        // The normal collector's explicit schema utility must accept the same
        // legal metadata-bearing output, not only the physical stream above.
        let output = Engine::new()
            .range_query_async(&source, "{pod=~\".*\"}", &RangeQuery::new(600, 600, 1))
            .await
            .unwrap();
        assert_eq!(
            answers(&output),
            vec![
                ("first".into(), vec![(600, (-0f64).to_bits())]),
                ("second".into(), vec![(600, nan.to_bits())]),
            ]
        );
        assert!(output
            .iter()
            .all(|batch| batch.schema().field(1).data_type() == &declared_type));
        for (lookback_ms, expected) in [
            (20, vec![("first".into(), vec![(600, (-0f64).to_bits())])]),
            (10, vec![]),
        ] {
            let source = Raw(vec![input.slice(1, if lookback_ms == 20 { 2 } else { 1 })]);
            let range = RangeQuery {
                lookback_ms,
                ..RangeQuery::new(600, 600, 1)
            };
            let output = Engine::new()
                .range_query_async(&source, "{pod=~\".*\"}", &range)
                .await
                .unwrap();
            assert_eq!(answers(&output), expected);
            assert!(output
                .iter()
                .all(|batch| batch.schema().field(1).data_type() == &declared_type));
        }
        for (query, value) in [("abs({pod=~\".*\"})", 0f64), ("{pod=~\".*\"} + 1", 1f64)] {
            let output = Engine::new()
                .range_query_async(&source, query, &RangeQuery::new(600, 600, 1))
                .await
                .unwrap();
            let actual = answers(&output);
            assert_eq!(actual.len(), 2);
            assert_eq!(actual[0], ("first".into(), vec![(600, value.to_bits())]));
            assert_eq!(actual[1].0, "second");
            assert_eq!(actual[1].1.len(), 1);
            assert_eq!(actual[1].1[0].0, 600);
            assert!(f64::from_bits(actual[1].1[0].1).is_nan());
        }
    }
}

#[tokio::test]
async fn empty_output_projection_preserves_row_counts_without_retaining_labels() {
    // b has a legal chunk at the excluded lookback boundary: an empty
    // evaluated row must still count in a zero-column projection.
    let source = Raw(vec![batch(
        &[(BLOCK, 1, "a", &[(590, 1.)]), (BLOCK, 2, "b", &[(300, 2.)])],
        false,
    )]);
    let range = RangeQuery {
        lookback_ms: 300,
        ..RangeQuery::new(600, 600, 1)
    };
    let physical = plan(&source, "{pod=~\".*\"}", &range).await;
    let exec = lower(&physical);
    let base: Arc<dyn ExecutionPlan> =
        Arc::new(SeriesEvalExec::try_new(exec.input.clone(), exec.kernel, exec.grid).unwrap());
    let projection = ProjectionExec::try_new(Vec::<ProjectionExpr>::new(), base).unwrap();
    let fused = exec
        .try_swapping_with_projection(&projection)
        .unwrap()
        .unwrap();
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_batch_size(1));
    let output = collect(fused, ctx.task_ctx()).await.unwrap();
    assert_eq!(output.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
    assert!(output.iter().all(|batch| batch.num_columns() == 0));
    assert_eq!(ctx.runtime_env().memory_pool.reserved(), 0);
}

#[tokio::test]
async fn fixed_binary_ids_are_opaque_at_every_positive_width() {
    for width in [1, 7, 16, 32] {
        let input = batch(
            &[
                (BLOCK, 0, "a", &[(500, 1.)]),
                (BLOCK, 0, "a", &[(590, 2.)]),
                (BLOCK, u64::MAX, "b", &[(500, 3.)]),
                (BLOCK, u64::MAX, "b", &[(590, 4.)]),
            ],
            false,
        );
        let ids = FixedSizeBinaryArray::try_from_iter(
            [0u8, 0, 255, 255].into_iter().map(|byte| vec![byte; width]),
        )
        .unwrap();
        let mut fields = input.schema().fields().to_vec();
        fields[4] = Arc::new(Field::new(
            SERIES_ID,
            DataType::FixedSizeBinary(width as i32),
            false,
        ));
        let mut columns = input.columns().to_vec();
        columns[4] = Arc::new(ids);
        let source = Raw(vec![RecordBatch::try_new(
            Arc::new(Schema::new(fields)),
            columns,
        )
        .unwrap()]);
        let physical = plan(&source, "{pod=~\".*\"}", &RangeQuery::new(600, 600, 1)).await;
        let ctx = SessionContext::new_with_config(SessionConfig::new().with_batch_size(1));
        let actual = collect(physical, ctx.task_ctx()).await.unwrap();
        assert_eq!(
            answers(&actual),
            vec![
                ("a".into(), vec![(600, 2f64.to_bits())]),
                ("b".into(), vec![(600, 4f64.to_bits())])
            ]
        );
        assert_eq!(ctx.runtime_env().memory_pool.reserved(), 0);
    }
}

mod resources;

#[tokio::test]
async fn expression_visitors_leave_pruning_indices_to_the_native_projection() {
    let source = Raw(vec![batch(&[(BLOCK, 1, "a", &[(590, 1.)])], false)]);
    let physical = plan(&source, "{pod=~\".*\"}", &RangeQuery::new(600, 600, 1)).await;
    let exec = lower(&physical);
    let mut count = 0;
    exec.apply_expressions(&mut |_| {
        count += 1;
        Ok(TreeNodeRecursion::Continue)
    })
    .unwrap();
    assert_eq!(count, 0);
    let exec = SeriesEvalExec::try_new(exec.input.clone(), exec.kernel, exec.grid)
        .unwrap()
        .with_projection(vec![1, 2])
        .unwrap();
    exec.apply_expressions(&mut |_| {
        count += 1;
        Ok(TreeNodeRecursion::Continue)
    })
    .unwrap();
    assert_eq!(count, 0);
    let projected = project_columns(
        Arc::new(exec),
        &[(SAMPLES, 0, "values"), (BLOCK_START, 1, "start")],
    );
    projected
        .apply_expressions(&mut |_| {
            count += 1;
            Ok(TreeNodeRecursion::Continue)
        })
        .unwrap();
    assert_eq!(count, 2);
    count = 0;
    assert_eq!(
        projected
            .apply_expressions(&mut |_| {
                count += 1;
                Ok(TreeNodeRecursion::Stop)
            })
            .unwrap(),
        TreeNodeRecursion::Stop
    );
    assert_eq!(count, 1);
}

#[test]
fn constructor_requires_a_supported_id_without_a_runtime_schema_audit() {
    use datafusion::physical_plan::empty::EmptyExec;
    let schema = batch(&[], false).schema();
    let grid = Params {
        start_ms: 600,
        end_ms: 600,
        step_ms: 1,
        window_ms: 300,
        offset_ms: 0,
        at_ms: None,
    };
    let kernel = Kernel::Selector(crate::selector::Pick::Value);
    let missing = Arc::new(Schema::new(schema.fields()[..4].to_vec()));
    assert!(SeriesEvalExec::try_new(Arc::new(EmptyExec::new(missing)), kernel, grid).is_err());
    for dtype in [
        DataType::Int64,
        DataType::Utf8View,
        DataType::FixedSizeBinary(0),
    ] {
        let mut fields = schema.fields().to_vec();
        fields[4] = Arc::new(Field::new(SERIES_ID, dtype, false));
        let input = Arc::new(EmptyExec::new(Arc::new(Schema::new(fields))));
        assert!(SeriesEvalExec::try_new(input, kernel, grid)
            .unwrap_err()
            .to_string()
            .contains("does not support series_id"));
    }
}

#[tokio::test]
async fn empty_label_schema_keeps_the_real_completed_row_count() {
    let input = batch(
        &[
            (BLOCK, 9, "unused", &[(500, 1.)]),
            (BLOCK, 9, "unused", &[(590, 2.)]),
        ],
        false,
    );
    let mut fields = input.schema().fields().to_vec();
    fields[0] = Arc::new(Field::new(LABELS, DataType::Struct(Fields::empty()), false));
    let mut columns = input.columns().to_vec();
    columns[0] = Arc::new(StructArray::new_empty_fields(2, None));
    let input = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
    let source = Raw(vec![input]);
    let physical = plan(&source, "{pod=~\".*\"}", &RangeQuery::new(600, 600, 1)).await;
    let ctx = SessionContext::new();
    let output = collect(physical, ctx.task_ctx()).await.unwrap();
    assert_eq!(output.len(), 1);
    assert_eq!(output[0].column(0).as_struct().len(), 1);
    assert!(output[0].column(0).as_struct().fields().is_empty());
    assert_eq!(
        answers(&output),
        vec![("".into(), vec![(600, 2f64.to_bits())])]
    );
    assert_eq!(ctx.runtime_env().memory_pool.reserved(), 0);
}

#[tokio::test]
async fn empty_input_has_no_output_and_all_grids_share_execution() {
    let source = Raw(vec![batch(&[], false)]);
    let ctx = SessionContext::new();
    for (query, range) in [
        ("{pod=~\".*\"}", RangeQuery::new(600, 600, 1)),
        ("{pod=~\".*\"}", RangeQuery::new(500, 600, 100)),
        ("rate({pod=~\".*\"}[1s])", RangeQuery::new(600, 600, 1)),
        ("{pod=~\".*\"} @ 0.6", RangeQuery::new(500, 600, 100)),
    ] {
        assert!(collect(plan(&source, query, &range).await, ctx.task_ctx())
            .await
            .unwrap()
            .is_empty());
    }
    assert_eq!(ctx.runtime_env().memory_pool.reserved(), 0);
}
