//! Real native exchanges exercise reset, not a mock execution counter.

use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;
use datafusion::arrow::array::{Int64Array, UInt64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::catalog::Session;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{LexOrdering, PhysicalExpr, PhysicalSortExpr};
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, Partitioning, PlanProperties, SendableRecordBatchStream,
};
use futures::channel::oneshot;
use futures::future::Shared;
use futures::{FutureExt, StreamExt};
use promql_parser::ast::LabelMatcher;

use super::*;
use crate::series::{self, SERIES_ID};
use crate::source::SeriesSetExec;
use crate::{explain_physical_plan, SelectHints, Series};

type GateReceiver = Shared<oneshot::Receiver<()>>;

#[derive(Debug)]
struct Gate {
    ready: GateReceiver,
    started: Mutex<Option<oneshot::Sender<()>>>,
}

impl Gate {
    fn new() -> (Arc<Self>, oneshot::Receiver<()>, oneshot::Sender<()>) {
        let (release, ready) = oneshot::channel();
        let (signal, started) = oneshot::channel();
        (
            Arc::new(Self {
                ready: ready.shared(),
                started: Mutex::new(Some(signal)),
            }),
            started,
            release,
        )
    }

    fn signal_started(&self) {
        if let Some(signal) = self.started.lock().unwrap().take() {
            let _ = signal.send(());
        }
    }
}

/// Wait below the private exchange, so cancellation happens only after the
/// exchange producers have started. All reset trees share the test's gate,
/// but never an exchange's channels.
#[derive(Debug)]
struct GatedInput {
    input: Arc<dyn ExecutionPlan>,
    gate: Arc<Gate>,
}

impl DisplayAs for GatedInput {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "GatedInput")
    }
}

impl ExecutionPlan for GatedInput {
    fn name(&self) -> &str {
        "GatedInput"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.input.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn apply_expressions(
        &self,
        _: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> datafusion::error::Result<TreeNodeRecursion>,
    ) -> datafusion::error::Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        assert_eq!(children.len(), 1);
        Ok(Arc::new(Self {
            input: children.remove(0),
            gate: self.gate.clone(),
        }))
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::error::Result<SendableRecordBatchStream> {
        let input = self.input.execute(partition, context)?;
        let gate = self.gate.clone();
        let stream = futures::stream::once(async move {
            gate.signal_started();
            gate.ready
                .clone()
                .await
                .expect("test must release the gate");
            input
        })
        .flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }
}

#[derive(Debug, Default)]
struct ExchangeSource {
    selections: AtomicUsize,
    gate: Option<Arc<Gate>>,
}

#[async_trait]
impl SeriesSource for ExchangeSource {
    async fn select(
        &self,
        _: &dyn Session,
        _: &[LabelMatcher],
        hints: SelectHints,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        self.selections.fetch_add(1, Ordering::Relaxed);
        // A series is split across the two storage inputs. The source's hash
        // exchange and sort must assemble its chunks before evaluation sees it.
        let partitions = [1000, 0]
            .into_iter()
            .map(|timestamp| {
                let rows = [
                    ("cpu20", "a"),
                    ("cpu3", "a"),
                    ("cpu100", "a"),
                    ("cpu20", "b"),
                ]
                .into_iter()
                .enumerate()
                .map(|(i, (pod, replica))| {
                    Series::new(
                        &[("__name__", "x"), ("pod", pod), ("replica", replica)],
                        vec![timestamp],
                        vec![i as f64 + 1. + if timestamp == 1000 { 10. } else { 0. }],
                    )
                    .unwrap()
                })
                .collect::<Vec<_>>();
                let batch = series::encode(
                    &series::label_names_of(&rows),
                    &rows,
                    series::Block {
                        start_ms: hints.start_ms + hints.window_ms,
                        end_ms: hints.end_ms + 1,
                    },
                )
                .unwrap();
                let mut fields = batch.schema().fields().to_vec();
                fields.extend([
                    Arc::new(Field::new(SERIES_ID, DataType::UInt64, false)),
                    Arc::new(Field::new("chunk_first", DataType::Int64, false)),
                ]);
                let mut columns = batch.columns().to_vec();
                columns.push(Arc::new(UInt64Array::from(vec![7, 8, 9, 10])));
                columns.push(Arc::new(Int64Array::from(vec![timestamp; rows.len()])));
                Ok(vec![RecordBatch::try_new(
                    Arc::new(Schema::new(fields)),
                    columns,
                )?])
            })
            .collect::<datafusion::error::Result<Vec<_>>>()?;
        let schema = partitions[0][0].schema();
        let mut input: Arc<dyn ExecutionPlan> =
            MemorySourceConfig::try_new_exec(&partitions, schema.clone(), None)?;
        if let Some(gate) = &self.gate {
            input = Arc::new(GatedInput {
                input,
                gate: gate.clone(),
            });
        }
        let distributed = Arc::new(RepartitionExec::try_new(
            input,
            Partitioning::Hash(
                vec![Arc::new(Column::new_with_schema(SERIES_ID, &schema)?)],
                2,
            ),
        )?);
        let ordering = LexOrdering::new(["block_start", SERIES_ID, "chunk_first"].map(|name| {
            PhysicalSortExpr::new_default(Arc::new(Column::new_with_schema(name, &schema).unwrap()))
        }))
        .unwrap();
        Ok(Arc::new(
            SortExec::new(ordering, distributed).with_preserve_partitioning(true),
        ))
    }
}

/// Return the same source template for repeated identical selections. A newly
/// planned instant query must reset this too, not just its fresh upper operators.
#[derive(Debug, Default)]
struct CachedExchangeSource {
    inner: ExchangeSource,
    plan: Mutex<Option<Arc<dyn ExecutionPlan>>>,
}

#[async_trait]
impl SeriesSource for CachedExchangeSource {
    async fn select(
        &self,
        state: &dyn Session,
        matchers: &[LabelMatcher],
        hints: SelectHints,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        if let Some(plan) = self.plan.lock().unwrap().as_ref() {
            return Ok(Arc::clone(plan));
        }
        let plan = self.inner.select(state, matchers, hints).await?;
        *self.plan.lock().unwrap() = Some(Arc::clone(&plan));
        Ok(plan)
    }
}

fn engine() -> Engine {
    let engine = Engine::new();
    engine
        .ctx
        .state_ref()
        .write()
        .config_mut()
        .options_mut()
        .execution
        .target_partitions = 2;
    engine
}

fn assert_exchanges(prepared: &PreparedQuery) {
    let mut upper_exchanges = 0;
    let mut source_exchanges = 0;
    let mut stack = vec![prepared.physical_plan()];
    while let Some(node) = stack.pop() {
        upper_exchanges += usize::from(node.is::<RepartitionExec>());
        if let Some(source) = node.downcast_ref::<SeriesSetExec>() {
            assert!(node.children().is_empty(), "source must remain protected");
            let mut private = vec![source.source_plan()];
            while let Some(node) = private.pop() {
                source_exchanges += usize::from(node.is::<RepartitionExec>());
                private.extend(node.children());
            }
        }
        stack.extend(node.children());
    }
    assert!(upper_exchanges > 0, "fixture needs an upper exchange");
    assert!(
        source_exchanges > 0,
        "fixture needs a source-owned exchange"
    );
}

type Answer = (String, Vec<(i64, u64)>);

fn answers(batches: &[RecordBatch]) -> Vec<Answer> {
    series::coalesce(series::decode(batches).unwrap())
        .unwrap()
        .into_iter()
        .map(|row| {
            (
                row.label("pod").to_owned(),
                row.timestamps()
                    .iter()
                    .zip(row.values())
                    .map(|(&timestamp, &value)| (timestamp, value.to_bits()))
                    .collect(),
            )
        })
        .collect()
}

fn expected() -> Vec<Answer> {
    [
        ("cpu100", 3_f64, 13_f64),
        ("cpu20", 5., 25.),
        ("cpu3", 2., 12.),
    ]
    .into_iter()
    .map(|(pod, first, last)| {
        (
            pod.to_owned(),
            vec![(0, first.to_bits()), (1000, last.to_bits())],
        )
    })
    .collect()
}

#[tokio::test]
async fn prepared_exchanges_reset_before_first_repeated_and_concurrent_execution() {
    let engine = engine();
    let source = ExchangeSource::default();
    let prepared = engine
        .prepare_async(&source, "sum by (pod) (x)", &RangeQuery::new(0, 1000, 1000))
        .await
        .unwrap();
    assert_exchanges(&prepared);
    assert_eq!(source.selections.load(Ordering::Relaxed), 1);
    let explanation = explain_physical_plan(prepared.physical_plan().as_ref());

    // Deliberately consume the escaped template, not a reset copy. Even the
    // first public execution must not rely on its channels being unused.
    let raw = physical_plan::collect(prepared.physical_plan().clone(), engine.ctx.task_ctx())
        .await
        .unwrap();
    let mut raw_answers = answers(&raw);
    raw_answers.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(raw_answers, expected());
    for _ in 0..3 {
        let output = engine.execute_async(&prepared).await.unwrap();
        assert_eq!(answers(&output), expected());
        assert_eq!(source.selections.load(Ordering::Relaxed), 1);
    }
    let (left, right) = tokio::join!(
        engine.execute_async(&prepared),
        engine.execute_async(&prepared)
    );
    for output in [left, right] {
        assert_eq!(answers(&output.unwrap()), expected());
    }
    assert_eq!(source.selections.load(Ordering::Relaxed), 1);
    assert_eq!(
        explain_physical_plan(prepared.physical_plan().as_ref()),
        explanation,
        "execution must not rewrite the physical template"
    );
}

#[tokio::test]
async fn typed_instant_queries_reset_consumed_source_exchanges_for_every_result_type() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let engine = engine();
        let at = 1000;
        let points = |first: f64, last: f64| vec![(0, first.to_bits()), (at, last.to_bits())];
        for (query, kind, want) in [
            (
                "sum by (pod) (x)",
                ValueType::Vector,
                [("cpu100", 13_f64), ("cpu20", 25.), ("cpu3", 12.)]
                    .into_iter()
                    .map(|(pod, value)| (pod.to_owned(), vec![(at, value.to_bits())]))
                    .collect::<Vec<_>>(),
            ),
            ("scalar(sum(x))", ValueType::Scalar, vec![]),
            (
                "x[2s]",
                ValueType::Matrix,
                vec![
                    ("cpu100".to_owned(), points(3., 13.)),
                    ("cpu20".to_owned(), points(1., 11.)),
                    ("cpu20".to_owned(), points(4., 14.)),
                    ("cpu3".to_owned(), points(2., 12.)),
                ],
            ),
        ] {
            let source = CachedExchangeSource::default();
            let plan = engine.plan_instant_async(&source, query, at).await.unwrap();
            let prepared = engine.prepare_plan(plan).await.unwrap();
            if kind == ValueType::Vector {
                assert_exchanges(&prepared);
            }
            // Consume the source-owned exchange before the first public instant
            // execution. Replanning upper operators alone cannot repair it.
            let raw =
                physical_plan::collect(prepared.physical_plan().clone(), engine.ctx.task_ctx())
                    .await
                    .unwrap();
            assert!(!raw.is_empty(), "{query}");
            let check = |result: InstantResult| match (kind, result) {
                (ValueType::Vector, InstantResult::Vector(batches))
                | (ValueType::Matrix, InstantResult::Matrix(batches)) => {
                    assert_eq!(answers(&batches), want, "{query}");
                }
                (ValueType::Scalar, InstantResult::Scalar(value)) => assert_eq!(value, 50.),
                (_, other) => panic!("{query}: wrong result type: {other:?}"),
            };
            for _ in 0..2 {
                check(
                    engine
                        .instant_query_async(&source, query, at)
                        .await
                        .unwrap(),
                );
            }
            let (left, right) = tokio::join!(
                engine.instant_query_async(&source, query, at),
                engine.instant_query_async(&source, query, at)
            );
            check(left.unwrap());
            check(right.unwrap());
            assert_eq!(source.inner.selections.load(Ordering::Relaxed), 1);
        }
    })
    .await
    .expect("instant execution must not wait on consumed source channels");
}

#[tokio::test]
async fn cancelled_prepared_execution_does_not_poison_upper_or_private_exchange() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let engine = engine();
        let (gate, started, release) = Gate::new();
        let source = ExchangeSource {
            gate: Some(gate),
            ..Default::default()
        };
        let prepared = engine
            .prepare_async(&source, "sum by (pod) (x)", &RangeQuery::new(0, 1000, 1000))
            .await
            .unwrap();
        assert_exchanges(&prepared);
        let mut execution = Box::pin(engine.execute_async(&prepared));
        tokio::select! {
            result = &mut execution => panic!("execution finished before source release: {result:?}"),
            result = started => result.expect("source must start before cancellation"),
        }
        // On this current-thread runtime the producer cannot signal until the
        // exchange consumers have been polled. Dropping only the collect future
        // leaves native task cancellation/cleanup to DataFusion.
        drop(execution);
        assert_eq!(source.selections.load(Ordering::Relaxed), 1);

        let mut retry = Box::pin(engine.execute_async(&prepared));
        assert!(futures::poll!(retry.as_mut()).is_pending());
        release.send(()).unwrap();
        assert_eq!(answers(&retry.await.unwrap()), expected());
        assert_eq!(
            answers(&engine.execute_async(&prepared).await.unwrap()),
            expected()
        );
        assert_eq!(source.selections.load(Ordering::Relaxed), 1);
    })
    .await
    .expect("cancelled exchange must not prevent reexecution");
}
