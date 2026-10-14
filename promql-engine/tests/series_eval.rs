//! ID-contiguous evaluation through the engine, with chunk/block parity and
//! independent Value/Timestamp selector expectations, including cross-batch
//! single-step candidates and old-context close across zero/one/many-step
//! blocks. Parity shares kernels; it is not a replacement for the unchanged
//! Prometheus conformance corpus.
use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, FixedSizeBinaryArray, RecordBatch, StringViewArray,
    StringViewBuilder, StructArray, UInt64Array,
};
use datafusion::arrow::buffer::Buffer;
use datafusion::arrow::compute::concat_batches;
use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema};
use datafusion::catalog::Session;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::execution::{memory_pool::GreedyMemoryPool, runtime_env::RuntimeEnvBuilder};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::{collect, displayable, ExecutionPlan};
use datafusion::prelude::SessionContext;
use futures::StreamExt;
use promql_engine::series::{self, Block, SERIES_ID};
use promql_engine::{
    Engine, MemorySeriesSource, RangeQuery, SelectHints, Series, SeriesIdEncoding, SeriesSource,
};
use promql_parser::ast::LabelMatcher;

fn nz(n: usize) -> NonZeroUsize {
    NonZeroUsize::new(n).unwrap()
}
fn nz64(n: u64) -> NonZeroU64 {
    NonZeroU64::new(n).unwrap()
}
fn data() -> Vec<Series> {
    (0..17)
        .map(|i| {
            let (ts, vs) = (0..35)
                .filter(|j| (i + j) % 7 != 0)
                .map(|j| {
                    (
                        j * 15_000,
                        if j == 18 {
                            f64::from_bits(promql_engine::selector::STALE_NAN_BITS)
                        } else {
                            ((j + i) % 11) as f64
                        },
                    )
                })
                .unzip();
            Series::new(
                &[
                    ("__name__", "x"),
                    ("pod", &format!("p{i}")),
                    ("route", &format!("r{}", i % 3)),
                ],
                ts,
                vs,
            )
            .unwrap()
        })
        .collect()
}
type Answer = (Vec<(String, String)>, Vec<(i64, u64)>);
type RawRow<'a> = (&'a str, &'a str, u64, &'a [(i64, f64)]);

fn answers(batches: &[RecordBatch]) -> Vec<Answer> {
    series::coalesce(series::decode(batches).unwrap())
        .unwrap()
        .into_iter()
        .map(|s| {
            (
                s.labels()
                    .map(|(k, v)| (k.to_owned(), v.to_owned()))
                    .collect(),
                s.timestamps()
                    .iter()
                    .zip(s.values())
                    .map(|(&t, &v)| (t, v.to_bits()))
                    .collect(),
            )
        })
        .collect()
}

fn literal_answers(metric: Option<&str>, rows: &[(&str, &[(i64, f64)])]) -> Vec<Answer> {
    rows.iter()
        .map(|(pod, samples)| {
            let mut labels = Vec::new();
            if let Some(metric) = metric {
                labels.push(("__name__".to_owned(), metric.to_owned()));
            }
            labels.push(("pod".to_owned(), (*pod).to_owned()));
            (
                labels,
                samples.iter().map(|&(t, v)| (t, v.to_bits())).collect(),
            )
        })
        .collect()
}

#[tokio::test]
async fn id_encodings_match_whole_chunks_across_blocks_partitions_and_batches() {
    let engine = Engine::with_options(promql_engine::EngineOptions {
        parser: promql_parser::ParserOptions {
            enable_experimental_functions: true,
            ..Default::default()
        },
    });
    let range = RangeQuery::new(150_000, 600_000, 30_000);
    let plain = MemorySeriesSource::try_new(data()).unwrap();
    for query in [
        "x",
        "x offset 45s",
        "x offset -45s",
        "x @ 300",
        "x @ start()",
        "rate(x[2m])",
        "rate(x[2m] offset 45s)",
        "increase(x[2m] @ end())",
        "sum(rate(x[2m]))",
        "sum by (route)(x)",
        "sum by (pod)(rate(x[2m]))",
        "sum without (pod)(rate(x[2m]))",
        "count(sum by (pod, route)(rate(x[2m])))",
        "count(sum without (pod)(x))",
        "sum by (pod)(abs(rate(x[2m])))",
        "max_over_time(x[2m])",
        "avg_over_time(x[2m])",
        "changes(x[2m])",
        "abs(rate(x[2m]))",
        "timestamp(x)",
        "x + x offset 45s",
        "x / on(route) group_left() sum by (route)(x)",
        "x >= bool x offset 45s",
        "absent(x)",
        "absent_over_time(x[1m])",
        "absent(missing)",
        "sort(x)",
        r#"sort_by_label(x, "pod")"#,
    ] {
        let expected = answers(
            &engine
                .range_query_async(&plain, query, &range)
                .await
                .unwrap(),
        );
        for id in [SeriesIdEncoding::UInt64, SeriesIdEncoding::FixedSizeBinary] {
            for parts in [1, 4] {
                for batch in [1, 7, 8192] {
                    let source = MemorySeriesSource::try_new(data())
                        .unwrap()
                        .with_ids(id)
                        .chunked(0)
                        .blocks(nz64(67_000))
                        .partitions(nz(parts))
                        .rows_per_batch(nz(batch));
                    let actual = engine
                        .range_query_async(&source, query, &range)
                        .await
                        .unwrap_or_else(|e| {
                            panic!("{query}, {id:?}, {parts} partitions, {batch} rows: {e}")
                        });
                    assert!(actual
                        .iter()
                        .all(|b| b.num_columns() == 4 && b.schema().index_of(SERIES_ID).is_err()));
                    assert_eq!(
                        answers(&actual),
                        expected,
                        "{query}, {id:?}, {parts}, {batch}"
                    );
                }
            }
        }
    }
}

#[tokio::test]
async fn contiguous_block_prefixes_preserve_answers_with_tiny_batches() {
    let engine = Engine::new();
    let range = RangeQuery::new(150_000, 600_000, 30_000);
    for query in ["x", "rate(x[2m])"] {
        let expected = answers(
            &engine
                .range_query_async(&MemorySeriesSource::try_new(data()).unwrap(), query, &range)
                .await
                .unwrap(),
        );
        for id in [SeriesIdEncoding::UInt64, SeriesIdEncoding::FixedSizeBinary] {
            let source = MemorySeriesSource::try_new(data())
                .unwrap()
                .chunked(0)
                .blocks(nz64(67_000))
                .partitions(nz(4))
                .rows_per_batch(nz(7))
                .with_ids(id);
            // Execute the directly planned node with one output row per batch;
            // chunks and evaluation results must not be confused with fragments.
            {
                // RepartitionExec owns execution channels; cloning an Arc
                // does not reset them for a second execution.
                let plan = engine
                    .prepare_async(&source, query, &range)
                    .await
                    .unwrap()
                    .physical_plan()
                    .clone();
                let config = datafusion::prelude::SessionConfig::new().with_batch_size(1);
                let ctx = SessionContext::new_with_config(config);
                let partitions =
                    datafusion::physical_plan::collect_partitioned(plan.clone(), ctx.task_ctx())
                        .await
                        .unwrap();
                for batches in &partitions {
                    let mut previous = None;
                    for batch in batches {
                        assert!(batch.num_rows() <= 1);
                        for row in 0..batch.num_rows() {
                            let start = series::block_of(batch, row).start_ms;
                            assert!(previous.is_none_or(|p| start >= p), "{query}, {id:?}");
                            previous = Some(start);
                        }
                    }
                }
                let actual: Vec<_> = partitions.into_iter().flatten().collect();
                // Raw plan partitions are not the API's final label sort.
                let mut decoded = series::decode(&actual).unwrap();
                decoded.sort_by(|a, b| {
                    a.labels()
                        .cmp(b.labels())
                        .then_with(|| a.timestamps()[0].cmp(&b.timestamps()[0]))
                });
                let actual: Vec<Answer> = series::coalesce(decoded)
                    .unwrap()
                    .into_iter()
                    .map(|s| {
                        (
                            s.labels()
                                .map(|(k, v)| (k.to_owned(), v.to_owned()))
                                .collect(),
                            s.timestamps()
                                .iter()
                                .zip(s.values())
                                .map(|(&t, &v)| (t, v.to_bits()))
                                .collect(),
                        )
                    })
                    .collect();
                assert_eq!(actual, expected, "{query}, {id:?}");
            }
        }
    }
}

#[tokio::test]
async fn id_encodings_preserve_empty_selections_and_later_empty_blocks() {
    let engine = Engine::new();
    let data = vec![Series::new(&[("__name__", "x"), ("pod", "a")], vec![0], vec![1.]).unwrap()];
    let range = RangeQuery::new(0, 900_000, 30_000);
    for query in [
        "missing",
        "x",
        "rate(x[2m])",
        "sum(x)",
        "absent(x)",
        "absent_over_time(x[1m])",
    ] {
        let plain = MemorySeriesSource::try_new(data.clone())
            .unwrap()
            .blocks(nz64(60_000));
        let expected = answers(
            &engine
                .range_query_async(&plain, query, &range)
                .await
                .unwrap(),
        );
        for id in [SeriesIdEncoding::UInt64, SeriesIdEncoding::FixedSizeBinary] {
            for blocked in [false, true] {
                let mut source = MemorySeriesSource::try_new(data.clone())
                    .unwrap()
                    .with_ids(id)
                    .chunked(0)
                    .partitions(nz(4))
                    .rows_per_batch(nz(1));
                if blocked {
                    source = source.blocks(nz64(60_000));
                }
                let actual = engine
                    .range_query_async(&source, query, &range)
                    .await
                    .unwrap();
                assert_eq!(
                    answers(&actual),
                    expected,
                    "{query}, {id:?}, blocked={blocked}"
                );
            }
        }
    }
}

#[tokio::test]
async fn empty_block_domains_preserve_the_id_schema() {
    let ctx = SessionContext::new();
    for id in [SeriesIdEncoding::UInt64, SeriesIdEncoding::FixedSizeBinary] {
        let source = MemorySeriesSource::try_new(vec![])
            .unwrap()
            .with_ids(id)
            .blocks(nz64(60_000));
        let plan = source
            .select(&ctx.state(), &[], SelectHints::range(1, 0))
            .await
            .unwrap();
        assert!(plan.schema().index_of(SERIES_ID).is_ok());
        assert!(collect(plan, ctx.task_ctx())
            .await
            .unwrap()
            .iter()
            .all(|b| b.num_rows() == 0));
        // An empty storage selection remains well typed; an inverted public
        // query range is still invalid.
        let error = Engine::new()
            .range_query_async(&source, "x", &RangeQuery::new(1, 0, 1))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("before start"), "{error}");
    }
}

#[tokio::test]
async fn reference_source_keeps_ids_contiguous_chronological_and_partition_colocated() {
    use std::collections::{BTreeMap, BTreeSet};
    let ctx = SessionContext::new();
    for id in [SeriesIdEncoding::UInt64, SeriesIdEncoding::FixedSizeBinary] {
        for parts in [1, 4] {
            let source = MemorySeriesSource::try_new(data())
                .unwrap()
                .with_ids(id)
                .chunked(0)
                .blocks(nz64(67_000))
                .rows_per_batch(nz(7))
                .partitions(nz(parts));
            let plan = source
                .select(
                    &ctx.state(),
                    &[],
                    SelectHints::range(0, 500_000).with_window_ms(120_000),
                )
                .await
                .unwrap();
            let partitions = datafusion::physical_plan::collect_partitioned(plan, ctx.task_ctx())
                .await
                .unwrap();
            let mut identities = BTreeMap::new();
            let mut by_labels = BTreeMap::new();
            for (p, batches) in partitions.iter().enumerate() {
                let mut closed = BTreeSet::new();
                let mut previous = None;
                let mut continued = false;
                let mut crossed_batch = false;
                let mut starts = BTreeMap::new();
                let mut previous_block = None;
                for batch in batches {
                    let previous_batch = previous.clone();
                    let labels = batch
                        .column(0)
                        .as_struct()
                        .column_by_name("pod")
                        .unwrap()
                        .as_string_view();
                    for row in 0..batch.num_rows() {
                        let bytes = match id {
                            SeriesIdEncoding::UInt64 => batch
                                .column(4)
                                .as_any()
                                .downcast_ref::<UInt64Array>()
                                .unwrap()
                                .value(row)
                                .to_be_bytes()
                                .to_vec(),
                            SeriesIdEncoding::FixedSizeBinary => batch
                                .column(4)
                                .as_any()
                                .downcast_ref::<FixedSizeBinaryArray>()
                                .unwrap()
                                .value(row)
                                .to_vec(),
                        };
                        let identity = (labels.value(row).to_owned(), p);
                        if let Some(existing) = identities.insert(bytes.clone(), identity.clone()) {
                            assert_eq!(existing, identity, "id changed labels or partition");
                        }
                        if let Some(existing) = by_labels.insert(identity.0, bytes.clone()) {
                            assert_eq!(existing, bytes, "one label identity received two IDs");
                        }
                        let block = series::block_of(batch, row).start_ms;
                        assert!(previous_block.is_none_or(|previous| block >= previous));
                        previous_block = Some(block);
                        let key = (block, bytes.clone());
                        if previous.as_ref() != Some(&key) {
                            if let Some(last) = previous.replace(key.clone()) {
                                closed.insert(last);
                            }
                            assert!(!closed.contains(&key), "id reopened within its block");
                        } else {
                            continued = true;
                            crossed_batch |= row == 0 && previous_batch.as_ref() == Some(&key);
                        }
                        let decoded = series::decode(&[batch.slice(row, 1)]).unwrap();
                        if let Some(series) = decoded.first() {
                            let first = series.timestamps()[0];
                            if let Some(last) = starts.insert(key, first) {
                                assert!(first >= last, "id chunks moved backwards");
                            }
                        }
                    }
                }
                assert!(
                    continued,
                    "{id:?}, partition {p}/{parts} had no chunk continuation"
                );
                assert!(
                    crossed_batch,
                    "{id:?}, partition {p}/{parts} had no cross-batch ID"
                );
            }
        }
    }
}

#[derive(Debug)]
struct Raw {
    batches: Vec<RecordBatch>,
}
impl Raw {
    fn rows_per_batch(mut self, rows: usize) -> Self {
        self.batches = self
            .batches
            .iter()
            .flat_map(|batch| {
                (0..batch.num_rows())
                    .step_by(rows)
                    .map(move |row| batch.slice(row, rows.min(batch.num_rows() - row)))
            })
            .collect();
        self
    }
}
#[async_trait]
impl SeriesSource for Raw {
    async fn select(
        &self,
        _: &dyn Session,
        _: &[LabelMatcher],
        _: SelectHints,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        Ok(MemorySourceConfig::try_new_exec(
            std::slice::from_ref(&self.batches),
            self.batches[0].schema(),
            None,
        )?)
    }
}
fn raw(rows: &[RawRow<'_>], id: SeriesIdEncoding) -> Raw {
    Raw {
        batches: vec![raw_block(
            rows,
            id,
            Block {
                start_ms: 0,
                end_ms: 400_000,
            },
        )],
    }
}
fn raw_block(rows: &[RawRow<'_>], id: SeriesIdEncoding, block: Block) -> RecordBatch {
    let names = vec!["__name__".into(), "pod".into()];
    let batches: Vec<_> = rows
        .iter()
        .map(|(name, pod, _, samples)| {
            let (ts, vs) = samples.iter().copied().unzip();
            let s = Series::new(&[("__name__", name), ("pod", pod)], ts, vs).unwrap();
            series::encode(&names, &[s], block).unwrap()
        })
        .collect();
    let batch = concat_batches(&batches[0].schema(), &batches).unwrap();
    let mut fields = batch.schema().fields().to_vec();
    let ty = match id {
        SeriesIdEncoding::UInt64 => DataType::UInt64,
        SeriesIdEncoding::FixedSizeBinary => DataType::FixedSizeBinary(16),
    };
    fields.push(Arc::new(Field::new(SERIES_ID, ty, false)));
    let ids: ArrayRef = match id {
        SeriesIdEncoding::UInt64 => Arc::new(UInt64Array::from(
            rows.iter().map(|r| r.2).collect::<Vec<_>>(),
        )),
        SeriesIdEncoding::FixedSizeBinary => Arc::new(
            FixedSizeBinaryArray::try_from_iter(rows.iter().map(|r| {
                let mut bytes = [0; 16];
                bytes[8..].copy_from_slice(&r.2.to_be_bytes());
                bytes
            }))
            .unwrap(),
        ),
    };
    let mut columns = batch.columns().to_vec();
    columns.push(ids);
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
}

#[tokio::test]
async fn first_completed_id_is_emitted_without_reading_the_rest_of_its_block() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use datafusion::arrow::datatypes::SchemaRef;
    use datafusion::execution::{SendableRecordBatchStream, TaskContext};
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use datafusion::physical_plan::streaming::{PartitionStream, StreamingTableExec};

    #[derive(Debug, Clone)]
    struct Probe {
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
        read: Arc<AtomicUsize>,
    }
    impl PartitionStream for Probe {
        fn schema(&self) -> &SchemaRef {
            &self.schema
        }
        fn execute(&self, _: Arc<TaskContext>) -> SendableRecordBatchStream {
            let read = self.read.clone();
            let batches = self.batches.clone().into_iter().map(move |batch| {
                read.fetch_add(1, Ordering::SeqCst);
                Ok(batch)
            });
            Box::pin(RecordBatchStreamAdapter::new(
                self.schema.clone(),
                futures::stream::iter(batches),
            ))
        }
    }
    #[async_trait]
    impl SeriesSource for Probe {
        async fn select(
            &self,
            _: &dyn Session,
            _: &[LabelMatcher],
            _: SelectHints,
        ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
            Ok(Arc::new(StreamingTableExec::try_new(
                self.schema.clone(),
                vec![Arc::new(self.clone())],
                None,
                [],
                false,
                None,
            )?))
        }
    }

    for id in [SeriesIdEncoding::UInt64, SeriesIdEncoding::FixedSizeBinary] {
        // Independent backing per ID makes release of consumed source arrays
        // observable while a later ID is still pending in the live stream.
        let raw = Raw {
            batches: [
                raw(
                    &[("x", "b", 200, &[(0, 1.)]), ("x", "b", 200, &[(1000, 3.)])],
                    id,
                ),
                raw(
                    &[("x", "a", 100, &[(0, 5.)]), ("x", "a", 100, &[(1000, 7.)])],
                    id,
                ),
                raw(
                    &[("x", "c", 300, &[(0, 9.)]), ("x", "c", 300, &[(1000, 11.)])],
                    id,
                ),
            ]
            .into_iter()
            .flat_map(|raw| raw.batches)
            .collect(),
        }
        .rows_per_batch(1);
        let consumed_labels = raw.batches[0]
            .column(0)
            .as_struct()
            .columns()
            .iter()
            .map(Arc::downgrade)
            .collect::<Vec<_>>();
        let consumed_samples = Arc::downgrade(raw.batches[0].column(1).as_list::<i32>().values());
        let read = Arc::new(AtomicUsize::new(0));
        let source = Probe {
            schema: raw.batches[0].schema(),
            batches: raw.batches,
            read: read.clone(),
        };
        let plan = Engine::new()
            .prepare_async(&source, "x", &RangeQuery::new(2000, 2000, 1))
            .await
            .unwrap()
            .physical_plan()
            .clone();
        let ctx = SessionContext::new_with_config(
            datafusion::prelude::SessionConfig::new().with_batch_size(1),
        );
        let mut stream = plan.execute(0, ctx.task_ctx()).unwrap();
        drop((source, plan));
        let first = stream.next().await.unwrap().unwrap();
        assert!(consumed_labels
            .iter()
            .all(|labels| labels.upgrade().is_none()));
        assert!(consumed_samples.upgrade().is_none());
        // Closing b may read a's first row, but must not ingest a's remaining
        // chunk or c before yielding b under one-row output backpressure.
        assert_eq!(read.load(Ordering::SeqCst), 3, "{id:?}");
        assert_eq!(
            answers(&[first]),
            literal_answers(Some("x"), &[("b", &[(2000, 3.)])])
        );
        drop(stream);
        assert_eq!(
            read.load(Ordering::SeqCst),
            3,
            "cancellation read extra input"
        );
        assert_eq!(ctx.runtime_env().memory_pool.reserved(), 0);
    }
}

#[tokio::test]
async fn contiguous_chunks_and_empty_batches_preserve_samples_for_both_id_types() {
    for id in [SeriesIdEncoding::UInt64, SeriesIdEncoding::FixedSizeBinary] {
        let source = raw(
            &[
                ("x", "a", 100, &[(0, 1.)]),
                ("x", "a", 100, &[(60_000, 2.)]),
                ("x", "b", 200, &[(0, 1.), (30_000, 2.)]),
            ],
            id,
        );
        let mut source = source.rows_per_batch(1);
        source.batches.insert(1, source.batches[0].slice(0, 0));
        let result = Engine::new()
            .range_query_async(
                &source,
                "count(x)",
                &RangeQuery::new(180_000, 300_000, 30_000),
            )
            .await
            .unwrap();
        let answer = answers(&result);
        assert_eq!(answer.len(), 1);
        assert!(answer[0].0.is_empty());
        assert_eq!(
            answer[0].1,
            (180_000..=300_000)
                .step_by(30_000)
                .map(|t| (t, 2f64.to_bits()))
                .collect::<Vec<_>>()
        );
    }
}

#[tokio::test]
async fn arbitrary_positive_binary_widths_preserve_literal_answers_and_chunk_precedence() {
    let rows: &[RawRow<'_>] = &[
        ("x", "b", 208, &[(0, 10.)]),
        ("x", "b", 208, &[(0, 100.), (60_000, 30.)]),
        ("x", "a", 160, &[(0, 1.)]),
        ("x", "a", 160, &[(0, 100.), (60_000, 3.)]),
    ];
    for width in [1, 3, 8, 16, 32] {
        let batch = raw_block(
            rows,
            SeriesIdEncoding::UInt64,
            Block {
                start_ms: 0,
                end_ms: 400_000,
            },
        );
        let mut fields = batch.schema().fields().to_vec();
        fields[4] = Arc::new(Field::new(
            SERIES_ID,
            DataType::FixedSizeBinary(width),
            false,
        ));
        let ids = FixedSizeBinaryArray::try_from_sparse_iter_with_size(
            rows.iter().map(|row| {
                let mut bytes = vec![0; width as usize];
                bytes[width as usize - 1] = row.2 as u8;
                Some(bytes)
            }),
            width,
        )
        .unwrap();
        let mut columns = batch.columns().to_vec();
        columns[4] = Arc::new(ids);
        let source = Raw {
            batches: vec![RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()],
        }
        .rows_per_batch(1);
        for (query, expected) in [
            (
                "x",
                literal_answers(
                    Some("x"),
                    &[("a", &[(90_000, 3.)]), ("b", &[(90_000, 30.)])],
                ),
            ),
            (
                "timestamp(x)",
                literal_answers(None, &[("a", &[(90_000, 60.)]), ("b", &[(90_000, 60.)])]),
            ),
            (
                "sum_over_time(x[2m])",
                literal_answers(None, &[("a", &[(90_000, 4.)]), ("b", &[(90_000, 40.)])]),
            ),
        ] {
            let output = Engine::new()
                .range_query_async(&source, query, &RangeQuery::new(90_000, 90_000, 1))
                .await
                .unwrap();
            assert_eq!(answers(&output), expected, "{query}, width={width}");
        }
    }
}

#[test]
fn schema_accepts_only_nonnullable_positive_fixed_width_ids() {
    let plain = series::schema(&["pod".into()]);
    for (ty, nullable, valid) in [
        (DataType::UInt64, false, true),
        (DataType::FixedSizeBinary(1), false, true),
        (DataType::FixedSizeBinary(16), false, true),
        (DataType::FixedSizeBinary(0), false, false),
        (DataType::FixedSizeBinary(-1), false, false),
        (DataType::UInt64, true, false),
        (DataType::Int64, false, false),
        (DataType::Binary, false, false),
        (DataType::Utf8View, false, false),
    ] {
        let mut fields = plain.fields().to_vec();
        fields.push(Arc::new(Field::new(SERIES_ID, ty.clone(), nullable)));
        assert_eq!(
            series::validate(&Schema::new(fields)).is_ok(),
            valid,
            "{ty}, nullable {nullable}"
        );
    }
}

#[tokio::test]
async fn contiguous_selector_has_literal_offset_and_staleness_expectations() {
    let stale = f64::from_bits(promql_engine::selector::STALE_NAN_BITS);
    let data = vec![
        Series::new(
            &[("__name__", "x"), ("pod", "a")],
            vec![0, 30_000, 60_000, 90_000],
            vec![1., 2., stale, 4.],
        )
        .unwrap(),
        Series::new(
            &[("__name__", "x"), ("pod", "b")],
            vec![0, 30_000],
            vec![10., 20.],
        )
        .unwrap(),
    ];
    for id in [SeriesIdEncoding::UInt64, SeriesIdEncoding::FixedSizeBinary] {
        let source = MemorySeriesSource::try_new(data.clone())
            .unwrap()
            .with_ids(id)
            .chunked(0)
            .blocks(nz64(45_000))
            .rows_per_batch(nz(1))
            .partitions(nz(4));
        let output = Engine::new()
            .range_query_async(
                &source,
                "x offset 30s",
                &RangeQuery::new(30_000, 360_000, 30_000),
            )
            .await
            .unwrap();
        let answer = answers(&output);
        let a = answer
            .iter()
            .find(|s| s.0.iter().any(|(n, v)| n == "pod" && v == "a"))
            .unwrap();
        let b = answer
            .iter()
            .find(|s| s.0.iter().any(|(n, v)| n == "pod" && v == "b"))
            .unwrap();
        assert_eq!(
            a.1,
            [(30_000, 1f64.to_bits()), (60_000, 2f64.to_bits())]
                .into_iter()
                .chain(
                    (120_000..=360_000)
                        .step_by(30_000)
                        .map(|t| (t, 4f64.to_bits()))
                )
                .collect::<Vec<_>>()
        );
        assert_eq!(
            b.1,
            [(30_000, 10f64.to_bits())]
                .into_iter()
                .chain(
                    (60_000..=330_000)
                        .step_by(30_000)
                        .map(|t| (t, 20f64.to_bits()))
                )
                .collect::<Vec<_>>()
        );
        assert!(a.0.iter().any(|(n, v)| n == "__name__" && v == "x"));
    }
}

#[tokio::test]
async fn contiguous_special_floats_keep_payloads_and_range_semantics() {
    let values = vec![
        -0.0,
        0.0,
        f64::INFINITY,
        f64::NEG_INFINITY,
        f64::from_bits(0x7ff8_0000_0000_1234),
        f64::from_bits(promql_engine::selector::STALE_NAN_BITS),
        7.0,
    ];
    let data = vec![
        Series::new(
            &[("__name__", "x"), ("pod", "a")],
            (0..values.len()).map(|i| i as i64 * 30_000).collect(),
            values,
        )
        .unwrap(),
        Series::new(
            &[("__name__", "x"), ("pod", "b")],
            vec![0, 60_000, 180_000],
            vec![1., 2., 3.],
        )
        .unwrap(),
    ];
    let engine = Engine::new();
    let range = RangeQuery::new(0, 240_000, 30_000);
    for query in [
        "x",
        "last_over_time(x[2m])",
        "sum_over_time(x[2m])",
        "min_over_time(x[2m])",
        "max_over_time(x[2m])",
        "count_over_time(x[2m])",
    ] {
        let reference = MemorySeriesSource::try_new(data.clone()).unwrap();
        let expected = answers(
            &engine
                .range_query_async(&reference, query, &range)
                .await
                .unwrap(),
        );
        for id in [SeriesIdEncoding::UInt64, SeriesIdEncoding::FixedSizeBinary] {
            let source = MemorySeriesSource::try_new(data.clone())
                .unwrap()
                .with_ids(id)
                .chunked(0)
                .blocks(nz64(67_000))
                .rows_per_batch(nz(1));
            let actual = answers(
                &engine
                    .range_query_async(&source, query, &range)
                    .await
                    .unwrap(),
            );
            assert_eq!(actual, expected, "{query}, {id:?}");
            if query == "x" {
                let a = &actual[0].1;
                assert_eq!(a[0], (0, (-0.0f64).to_bits()));
                assert_eq!(a[4], (120_000, 0x7ff8_0000_0000_1234));
                assert!(!a.iter().any(|(t, _)| *t == 150_000));
            }
        }
    }
}

#[tokio::test]
async fn single_step_selector_has_literal_contiguous_candidate_answers() {
    let stale = f64::from_bits(promql_engine::selector::STALE_NAN_BITS);
    let nan = f64::from_bits(0x7ff8_0000_0000_1234);
    // f's first chunk ends beyond the lookup. Tracking only the candidate's
    // timestamp would wrongly admit the overlapping 95s/100s samples. g's
    // stale marker must claim 80s even though it produces no output.
    let rows: &[RawRow<'_>] = &[
        ("x", "a", 100, &[(0, 1.)]),
        ("x", "a", 100, &[(40_000, stale)]),
        ("x", "a", 100, &[(90_000, 7.)]),
        ("x", "b", 200, &[(0, 10.)]),
        ("x", "b", 200, &[(40_000, 12.)]),
        ("x", "b", 200, &[(90_000, stale)]),
        ("x", "c", 300, &[(0, 2.)]),
        ("x", "c", 300, &[(90_000, nan), (120_000, 9.)]),
        ("x", "d", 400, &[(0, 3.)]),
        ("x", "d", 400, &[(90_000, -0.), (120_000, 10.)]),
        ("x", "e", 500, &[(0, -0.)]),
        ("x", "e", 500, &[(90_000, 0.)]),
        ("x", "f", 600, &[(0, 4.), (60_000, 6.), (110_000, 11.)]),
        (
            "x",
            "f",
            600,
            &[
                (60_000, 60.),
                (95_000, 95.),
                (100_000, 100.),
                (110_000, 110.),
                (130_000, 13.),
            ],
        ),
        ("x", "g", 700, &[(0, 5.), (80_000, stale)]),
        ("x", "g", 700, &[(80_000, 8.), (120_000, 12.)]),
    ];
    let cases = [
        (
            "x",
            literal_answers(
                Some("x"),
                &[
                    ("a", &[(100_000, 7.)]),
                    ("c", &[(100_000, f64::from_bits(0x7ff8_0000_0000_1234))]),
                    ("d", &[(100_000, -0.)]),
                    ("e", &[(100_000, 0.)]),
                    ("f", &[(100_000, 6.)]),
                ],
            ),
        ),
        (
            "timestamp(x)",
            literal_answers(
                None,
                &[
                    ("a", &[(100_000, 90.)]),
                    ("c", &[(100_000, 90.)]),
                    ("d", &[(100_000, 90.)]),
                    ("e", &[(100_000, 90.)]),
                    ("f", &[(100_000, 60.)]),
                ],
            ),
        ),
    ];
    let engine = Engine::new();
    for id in [SeriesIdEncoding::UInt64, SeriesIdEncoding::FixedSizeBinary] {
        for batch in [1, 5, 8192] {
            let source = raw(rows, id).rows_per_batch(batch);
            for (query, expected) in &cases {
                let output = engine
                    .range_query_async(&source, query, &RangeQuery::new(100_000, 100_000, 30_000))
                    .await
                    .unwrap();
                assert_eq!(
                    &answers(&output),
                    expected,
                    "{query}, {id:?}, batch={batch}"
                );
            }
        }
    }
}

#[tokio::test]
async fn single_step_selector_has_literal_lookback_and_modifier_boundaries() {
    let stale = f64::from_bits(promql_engine::selector::STALE_NAN_BITS);
    let rows: &[RawRow<'_>] = &[
        ("x", "a", 100, &[(0, 1.)]),
        ("x", "a", 100, &[(0, 100.), (300_001, 101.)]),
        ("x", "b", 200, &[(1, 2.)]),
        ("x", "b", 200, &[(1, 200.), (300_001, 201.)]),
        ("x", "c", 300, &[(299_999, 3.)]),
        ("x", "c", 300, &[(299_999, 300.), (300_000, 4.)]),
        ("x", "d", 400, &[(0, stale), (300_001, 5.)]),
        ("x", "e", 500, &[(-1, 6.), (300_001, 7.)]),
    ];
    // The lower lookback bound is strict, the lookup's upper bound is
    // inclusive. Literal timestamp answers also distinguish one millisecond
    // of offset from rounding to seconds or emitting the evaluation time.
    let at_300s = (
        literal_answers(
            Some("x"),
            &[("b", &[(300_000, 2.)]), ("c", &[(300_000, 4.)])],
        ),
        literal_answers(
            None,
            &[("b", &[(300_000, 0.001)]), ("c", &[(300_000, 300.)])],
        ),
    );
    let just_before = (
        literal_answers(
            Some("x"),
            &[
                ("a", &[(300_000, 1.)]),
                ("b", &[(300_000, 2.)]),
                ("c", &[(300_000, 3.)]),
            ],
        ),
        literal_answers(
            None,
            &[
                ("a", &[(300_000, 0.)]),
                ("b", &[(300_000, 0.001)]),
                ("c", &[(300_000, 299.999)]),
            ],
        ),
    );
    let just_after = (
        literal_answers(
            Some("x"),
            &[
                ("a", &[(300_000, 101.)]),
                ("b", &[(300_000, 201.)]),
                ("c", &[(300_000, 4.)]),
                ("d", &[(300_000, 5.)]),
                ("e", &[(300_000, 7.)]),
            ],
        ),
        literal_answers(
            None,
            &[
                ("a", &[(300_000, 300.001)]),
                ("b", &[(300_000, 300.001)]),
                ("c", &[(300_000, 300.)]),
                ("d", &[(300_000, 300.001)]),
                ("e", &[(300_000, 300.001)]),
            ],
        ),
    );
    let engine = Engine::new();
    for id in [SeriesIdEncoding::UInt64, SeriesIdEncoding::FixedSizeBinary] {
        let source = raw(rows, id).rows_per_batch(1);
        for (selector, (value, timestamp)) in [
            ("x", &at_300s),
            ("x offset 1ms", &just_before),
            ("x offset -1ms", &just_after),
            ("x @ 300 offset 1ms", &just_before),
        ] {
            for (query, expected) in [
                (selector.to_owned(), value),
                (format!("timestamp({selector})"), timestamp),
            ] {
                let output = engine
                    .range_query_async(&source, &query, &RangeQuery::new(300_000, 300_000, 30_000))
                    .await
                    .unwrap();
                assert_eq!(&answers(&output), expected, "{query}, {id:?}");
            }
        }
    }
}

#[tokio::test]
async fn selector_literals_survive_old_context_finalization_and_zero_step_blocks() {
    let stale = f64::from_bits(promql_engine::selector::STALE_NAN_BITS);
    // The 100s grid leaves each answering block's point after its last
    // sample: close must evaluate against the old context. The 50s grid
    // changes block-local counts through 1/0/3/0/1; zero-step blocks must
    // neither leak results nor reuse the previous candidate.
    let blocks: &[(Block, &[RawRow<'_>])] = &[
        (
            Block {
                start_ms: 0,
                end_ms: 100_000,
            },
            &[
                ("x", "a", 100, &[(0, 0.)]),
                ("x", "a", 100, &[(50_000, 1.)]),
                ("x", "b", 200, &[(10_000, 5.)]),
                ("x", "b", 200, &[(60_000, 10.)]),
            ],
        ),
        (
            Block {
                start_ms: 100_000,
                end_ms: 140_000,
            },
            &[
                ("x", "a", 100, &[(0, 0.), (50_000, 1.)]),
                ("x", "b", 200, &[(10_000, 5.), (60_000, 10.)]),
            ],
        ),
        (
            Block {
                start_ms: 140_000,
                end_ms: 250_000,
            },
            &[
                ("x", "a", 100, &[(0, 0.), (50_000, 1.)]),
                ("x", "a", 100, &[(50_000, 1.), (170_000, 2.)]),
                ("x", "b", 200, &[(10_000, 5.), (60_000, 10.)]),
                ("x", "b", 200, &[(60_000, 10.), (180_000, stale)]),
            ],
        ),
        (
            Block {
                start_ms: 250_000,
                end_ms: 260_000,
            },
            &[
                ("x", "a", 100, &[(0, 0.), (50_000, 1.), (170_000, 2.)]),
                (
                    "x",
                    "b",
                    200,
                    &[(10_000, 5.), (60_000, 10.), (180_000, stale)],
                ),
            ],
        ),
        (
            Block {
                start_ms: 260_000,
                end_ms: 310_000,
            },
            &[
                ("x", "a", 100, &[(0, 0.), (50_000, 1.), (170_000, 2.)]),
                ("x", "a", 100, &[(50_000, 1.), (170_000, 2.), (280_000, 3.)]),
                (
                    "x",
                    "b",
                    200,
                    &[(10_000, 5.), (60_000, 10.), (180_000, stale)],
                ),
                (
                    "x",
                    "b",
                    200,
                    &[(60_000, 10.), (180_000, stale), (285_000, -0.)],
                ),
            ],
        ),
    ];
    let unshifted = (
        literal_answers(
            Some("x"),
            &[
                ("a", &[(90_000, 1.), (190_000, 2.), (290_000, 3.)]),
                ("b", &[(90_000, 10.), (290_000, -0.)]),
            ],
        ),
        literal_answers(
            None,
            &[
                ("a", &[(90_000, 50.), (190_000, 170.), (290_000, 280.)]),
                ("b", &[(90_000, 60.), (290_000, 285.)]),
            ],
        ),
    );
    let unshifted_many = (
        literal_answers(
            Some("x"),
            &[
                (
                    "a",
                    &[
                        (90_000, 1.),
                        (140_000, 1.),
                        (190_000, 2.),
                        (240_000, 2.),
                        (290_000, 3.),
                    ],
                ),
                ("b", &[(90_000, 10.), (140_000, 10.), (290_000, -0.)]),
            ],
        ),
        literal_answers(
            None,
            &[
                (
                    "a",
                    &[
                        (90_000, 50.),
                        (140_000, 50.),
                        (190_000, 170.),
                        (240_000, 170.),
                        (290_000, 280.),
                    ],
                ),
                ("b", &[(90_000, 60.), (140_000, 60.), (290_000, 285.)]),
            ],
        ),
    );
    let past = (
        literal_answers(
            Some("x"),
            &[
                ("a", &[(90_000, 1.), (190_000, 2.), (290_000, 3.)]),
                ("b", &[(90_000, 10.)]),
            ],
        ),
        literal_answers(
            None,
            &[
                ("a", &[(90_000, 50.), (190_000, 170.), (290_000, 280.)]),
                ("b", &[(90_000, 60.)]),
            ],
        ),
    );
    let pinned_start = (
        literal_answers(
            Some("x"),
            &[
                ("a", &[(90_000, 1.), (190_000, 1.), (290_000, 1.)]),
                ("b", &[(90_000, 10.), (190_000, 10.), (290_000, 10.)]),
            ],
        ),
        literal_answers(
            None,
            &[
                ("a", &[(90_000, 50.), (190_000, 50.), (290_000, 50.)]),
                ("b", &[(90_000, 60.), (190_000, 60.), (290_000, 60.)]),
            ],
        ),
    );
    let pinned_end = (
        literal_answers(
            Some("x"),
            &[
                ("a", &[(90_000, 3.), (190_000, 3.), (290_000, 3.)]),
                ("b", &[(90_000, -0.), (190_000, -0.), (290_000, -0.)]),
            ],
        ),
        literal_answers(
            None,
            &[
                ("a", &[(90_000, 280.), (190_000, 280.), (290_000, 280.)]),
                ("b", &[(90_000, 285.), (190_000, 285.), (290_000, 285.)]),
            ],
        ),
    );
    let pinned_end_past = (
        literal_answers(
            Some("x"),
            &[("a", &[(90_000, 3.), (190_000, 3.), (290_000, 3.)])],
        ),
        literal_answers(
            None,
            &[("a", &[(90_000, 280.), (190_000, 280.), (290_000, 280.)])],
        ),
    );
    let engine = Engine::new();
    for id in [SeriesIdEncoding::UInt64, SeriesIdEncoding::FixedSizeBinary] {
        let batches: Vec<_> = blocks
            .iter()
            .map(|(block, rows)| raw_block(rows, id, *block))
            .collect();
        let batch = concat_batches(&batches[0].schema(), &batches).unwrap();
        for rows in [1, 5, 8192] {
            // The unsplit form keeps later IDs and blocks in one pending raw
            // cursor while earlier identities finalize against their context.
            let source = Raw {
                batches: vec![batch.clone()],
            }
            .rows_per_batch(rows);
            for (selector, step, (value, timestamp)) in [
                ("x", 100_000, &unshifted),
                ("x", 50_000, &unshifted_many),
                ("x offset 10s", 100_000, &past),
                ("x offset -10s", 100_000, &unshifted),
                ("x @ start()", 100_000, &pinned_start),
                ("x @ end()", 100_000, &pinned_end),
                ("x @ start() offset -10s", 100_000, &pinned_start),
                ("x @ end() offset 10s", 100_000, &pinned_end_past),
            ] {
                for (query, expected) in [
                    (selector.to_owned(), value),
                    (format!("timestamp({selector})"), timestamp),
                ] {
                    let output = engine
                        .range_query_async(&source, &query, &RangeQuery::new(90_000, 290_000, step))
                        .await
                        .unwrap();
                    assert_eq!(
                        &answers(&output),
                        expected,
                        "{query}, {id:?}, step={step}, batch={rows}"
                    );
                }
            }
        }
    }
}

#[tokio::test]
async fn memory_pressure_with_disk_enabled_fails_instead_of_spilling_series_state() {
    let engine = Engine::new();
    let source = MemorySeriesSource::try_new(data())
        .unwrap()
        .with_ids(SeriesIdEncoding::UInt64)
        .chunked(0)
        .rows_per_batch(nz(7));
    // A fine grid makes one current fold exceed the pool limit.
    let plan = engine
        .prepare_async(&source, "rate(x[2m])", &RangeQuery::new(0, 500_000, 10))
        .await
        .unwrap()
        .physical_plan()
        .clone();
    let shown = displayable(plan.as_ref()).indent(true).to_string();
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::new(GreedyMemoryPool::new(32_000)))
        .build_arc()
        .unwrap();
    assert!(runtime.disk_manager.tmp_files_enabled());
    let ctx =
        SessionContext::new_with_config_rt(datafusion::prelude::SessionConfig::new(), runtime);
    let err = collect(plan, ctx.task_ctx()).await.unwrap_err();
    assert!(
        err.to_string().contains("Resources exhausted"),
        "{err}\n{shown}"
    );
}

#[tokio::test]
async fn label_results_own_values_and_preserve_schema_after_source_and_stream_drop() {
    use std::collections::HashMap;

    for id in [SeriesIdEncoding::UInt64, SeriesIdEncoding::FixedSizeBinary] {
        let long_b = "/api/long-route/".repeat(40);
        let long_a = "東京/🦀/café/".repeat(40);
        let zone_values = [long_b.as_str(), long_a.as_str(), "cancelled-group-label"];
        let owner = Arc::new(
            zone_values
                .iter()
                .flat_map(|value| value.as_bytes().iter().copied())
                .collect::<Vec<_>>(),
        );
        let original_payload = Arc::downgrade(&owner);
        let pointer = std::ptr::NonNull::new(owner.as_ptr() as *mut u8).unwrap();
        // Arrow alone owns this immutable Vec after `owner` is dropped; Weak
        // observes payload lifetime without keeping the allocation alive.
        let buffer = unsafe { Buffer::from_custom_allocation(pointer, owner.len(), owner.clone()) };
        let mut zones = StringViewBuilder::new();
        let block = zones.append_block(buffer);
        let offsets = [0, long_b.len(), long_b.len() + long_a.len()];
        for index in [0, 0, 1, 1, 2, 2] {
            zones
                .try_append_view(
                    block,
                    u32::try_from(offsets[index]).unwrap(),
                    u32::try_from(zone_values[index].len()).unwrap(),
                )
                .unwrap();
        }
        let label_fields: Fields = ["__name__", "empty", "pod", "zone"]
            .into_iter()
            .map(|name| {
                Field::new(name, DataType::Utf8View, false)
                    .with_metadata(HashMap::from([("source-label".into(), name.into())]))
            })
            .collect();
        let labels = StructArray::new(
            label_fields.clone(),
            vec![
                Arc::new(StringViewArray::from_iter_values(["x"; 6])),
                Arc::new(StringViewArray::from_iter_values([""; 6])),
                Arc::new(StringViewArray::from_iter_values([
                    "β", "β", "", "", "cancel", "cancel",
                ])),
                Arc::new(zones.finish()),
            ],
            None,
        );
        let mut source = raw(
            &[
                ("x", "β", 200, &[(0, 10.)]),
                ("x", "β", 200, &[(60_000, 12.)]),
                ("x", "", 100, &[(0, 1.)]),
                ("x", "", 100, &[(60_000, -0.)]),
                ("x", "cancel", 300, &[(0, 3.)]),
                ("x", "cancel", 300, &[(60_000, 4.)]),
            ],
            id,
        );
        let input = source.batches.pop().unwrap();
        let mut fields = input.schema().fields().to_vec();
        fields[0] = Arc::new(Field::new("labels", labels.data_type().clone(), false));
        let fields = fields
            .into_iter()
            .map(|field| {
                field.as_ref().clone().with_metadata(HashMap::from([(
                    "source-column".into(),
                    field.name().clone(),
                )]))
            })
            .collect::<Vec<_>>();
        let schema_metadata = HashMap::from([("source-schema".into(), "owned-labels".into())]);
        let expected_schema = Arc::new(Schema::new_with_metadata(
            fields[..4].to_vec(),
            schema_metadata.clone(),
        ));
        let mut columns = input.columns().to_vec();
        columns[0] = Arc::new(labels);
        source.batches.push(
            RecordBatch::try_new(
                Arc::new(Schema::new_with_metadata(fields, schema_metadata)),
                columns,
            )
            .unwrap(),
        );
        source = source.rows_per_batch(1);
        let original_children = source
            .batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_struct()
                    .columns()
                    .iter()
                    .map(Arc::downgrade)
            })
            .collect::<Vec<_>>();
        drop((owner, input));
        assert!(original_payload.upgrade().is_some());
        assert_eq!(original_children.len(), 24);

        let engine = Engine::new();
        let plan = engine
            .prepare_async(&source, "x", &RangeQuery::new(90_000, 90_000, 1))
            .await
            .unwrap()
            .physical_plan()
            .clone();
        assert_eq!(plan.schema(), expected_schema);
        let ctx = SessionContext::new_with_config(
            datafusion::prelude::SessionConfig::new().with_batch_size(1),
        );
        let mut stream = plan.execute(0, ctx.task_ctx()).unwrap();
        drop((source, plan, engine));
        let first = stream.next().await.unwrap().unwrap();
        let second = stream.next().await.unwrap().unwrap();
        // A pending raw cursor may still own the source's shared label buffer.
        // Cancellation must release it without invalidating emitted rows.
        drop(stream);
        assert!(original_payload.upgrade().is_none(), "{id:?}");
        assert!(original_children
            .iter()
            .all(|child| child.upgrade().is_none()));
        assert_eq!(ctx.runtime_env().memory_pool.reserved(), 0);

        for (batch, (pod, zone, value)) in [first, second]
            .iter()
            .zip([("β", long_b.as_str(), 12f64), ("", long_a.as_str(), -0f64)])
        {
            assert_eq!(batch.schema(), expected_schema);
            assert_eq!(batch.num_rows(), 1);
            let labels = batch.column(0).as_struct();
            assert_eq!(labels.fields(), &label_fields);
            assert_eq!(labels.null_count(), 0);
            for (column, expected) in labels.columns().iter().zip(["x", "", pod, zone]) {
                let values = column.as_string_view();
                assert_eq!(values.len(), 1);
                assert_eq!(values.null_count(), 0);
                assert_eq!(values.value(0), expected, "{id:?}");
            }
            let decoded = series::decode(std::slice::from_ref(batch)).unwrap();
            assert_eq!(decoded.len(), 1);
            assert_eq!(decoded[0].timestamps(), &[90_000]);
            assert_eq!(decoded[0].values()[0].to_bits(), value.to_bits());
            assert_eq!(series::block_of(batch, 0).start_ms, 0);
            assert_eq!(series::block_of(batch, 0).end_ms, 400_000);
        }
    }
}

#[tokio::test]
async fn empty_label_schema_distinguishes_empty_input_from_label_only_output() {
    for id in [SeriesIdEncoding::UInt64, SeriesIdEncoding::FixedSizeBinary] {
        for nonempty in [false, true] {
            let rows = if nonempty {
                vec![
                    Series::new(&[], vec![0, 30_000, 60_000, 90_000], vec![1., 2., 3., 4.])
                        .unwrap(),
                ]
            } else {
                vec![]
            };
            let source = MemorySeriesSource::try_new(rows)
                .unwrap()
                .with_ids(id)
                .chunked(0)
                .blocks(nz64(45_000))
                .rows_per_batch(nz(1));
            let physical = Engine::new()
                .prepare_async(
                    &source,
                    r#"{pod=~".*"}"#,
                    &RangeQuery::new(0, 90_000, 30_000),
                )
                .await
                .unwrap()
                .physical_plan()
                .clone();
            let mut nodes = vec![physical.clone()];
            let eval = loop {
                let node = nodes.pop().expect("the directly planned series evaluator");
                if node.name() == "SeriesEvalExec" {
                    break node;
                }
                nodes.extend(node.children().into_iter().cloned());
            };
            let projection = ProjectionExec::try_new(
                vec![(
                    Arc::new(Column::new_with_schema("labels", &eval.schema()).unwrap())
                        as Arc<dyn PhysicalExpr>,
                    "label_result".to_owned(),
                )],
                eval.clone(),
            )
            .unwrap();
            let projected_schema = projection.schema();
            let projected = eval
                .try_swapping_with_projection(&projection)
                .unwrap()
                .expect("label-only output prunes unused evaluator columns");
            assert_eq!(projected.name(), "ProjectionExec");
            assert_eq!(projected.children()[0].name(), "SeriesEvalExec");
            assert_eq!(projected.children()[0].schema().fields().len(), 1);
            assert_eq!(projected.schema(), projected_schema);
            assert_eq!(projected_schema.field(0).name(), "label_result");
            assert_eq!(
                projected_schema.field(0).data_type(),
                &DataType::Struct(Fields::empty())
            );
            let ctx = SessionContext::new_with_config(
                datafusion::prelude::SessionConfig::new().with_batch_size(1),
            );
            let batches = collect(projected, ctx.task_ctx()).await.unwrap();
            if nonempty {
                // One valid identity (the empty label set) answers each of
                // three blocks; empty Fields must not erase its Struct rows.
                assert_eq!(batches.len(), 3, "{id:?}");
                for batch in batches {
                    assert_eq!(batch.schema(), projected_schema);
                    assert_eq!(batch.num_rows(), 1);
                    assert_eq!(batch.num_columns(), 1);
                    let labels = batch.column(0).as_struct();
                    assert!(labels.fields().is_empty());
                    assert!(labels.columns().is_empty());
                    assert_eq!(labels.len(), batch.num_rows());
                    assert_eq!(labels.null_count(), 0);
                }
            } else {
                assert!(batches.is_empty(), "{id:?}");
            }
            assert_eq!(ctx.runtime_env().memory_pool.reserved(), 0);
        }
    }
}
