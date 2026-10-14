//! Gated source EOF distinguishes block-prefix emission from complete input
//! buffering over real selectors and ranges, without relying on exchange timing.
//! Emission before EOF does not establish a one-block retained-state bound.

use super::*;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::{
    aggregates::{AggregateExec, AggregateMode},
    stream::RecordBatchStreamAdapter,
    DisplayAs, DisplayFormatType, ExecutionPlan, InputOrderMode, PlanProperties,
    SendableRecordBatchStream,
};
use futures::StreamExt;
use promql_parser::ast::LabelMatcher;
use std::num::{NonZeroU64, NonZeroUsize};
use std::time::Duration;
use tokio::sync::Semaphore;

use crate::{Engine, MemorySeriesSource, RangeQuery, SelectHints, Series, SeriesSource};

#[derive(Debug)]
struct GatedSource {
    inner: MemorySeriesSource,
    eof: Arc<Semaphore>,
}

#[async_trait]
impl SeriesSource for GatedSource {
    async fn select(
        &self,
        state: &dyn Session,
        matchers: &[LabelMatcher],
        hints: SelectHints,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(GatedInput {
            input: self.inner.select(state, matchers, hints).await?,
            eof: self.eof.clone(),
        }))
    }
}

#[derive(Debug)]
struct GatedInput {
    input: Arc<dyn ExecutionPlan>,
    eof: Arc<Semaphore>,
}

impl DisplayAs for GatedInput {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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
        _: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self {
            input: children[0].clone(),
            eof: self.eof.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input = self.input.execute(partition, context)?;
        let eof = self.eof.clone();
        let stream = futures::stream::unfold((input, eof), |(mut input, eof)| async move {
            match input.next().await {
                Some(batch) => Some((batch, (input, eof))),
                None => {
                    eof.acquire().await.expect("EOF gate stays open").forget();
                    None
                }
            }
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }
}

#[tokio::test]
async fn selectors_and_ranges_release_upper_block_prefixes_through_exchange_before_eof() {
    for (query, expected) in [
        ("sum by (job) (x)", 136.0),
        ("sum by (job) (x offset 1ms)", 136.0),
        ("sum by (job) (sum_over_time(x[2ms]))", 272.0),
    ] {
        let stored = (1..=16)
            .map(|g| {
                let pod = format!("p{g}");
                Series::new(
                    &[("__name__", "x"), ("job", "test"), ("pod", &pod)],
                    (-2..64).collect(),
                    vec![g as f64; 66],
                )
                .unwrap()
            })
            .collect();
        let eof = Arc::new(Semaphore::new(0));
        let source = GatedSource {
            inner: MemorySeriesSource::try_new(stored)
                .unwrap()
                .blocks(NonZeroU64::new(4).unwrap())
                .chunked(0)
                .partitions(NonZeroUsize::new(2).unwrap())
                .rows_per_batch(NonZeroUsize::new(1).unwrap()),
            eof: eof.clone(),
        };
        let engine = Engine::new();
        let mut range = RangeQuery::new(0, 63, 1);
        range.lookback_ms = 1;
        let prepared = engine.prepare_async(&source, query, &range).await.unwrap();
        let plan = prepared.physical_plan().clone();
        let mut stack = vec![plan.clone()];
        let (mut partial, mut final_partitioned, mut exchange) = (false, false, false);
        while let Some(node) = stack.pop() {
            stack.extend(node.children().into_iter().cloned());
            exchange |= node.name() == "RepartitionExec";
            if let Some(agg) = node.downcast_ref::<AggregateExec>() {
                assert!(
                    matches!(agg.input_order_mode(), InputOrderMode::PartiallySorted(_)),
                    "{query}"
                );
                partial |= *agg.mode() == AggregateMode::Partial;
                final_partitioned |= *agg.mode() == AggregateMode::FinalPartitioned;
            }
        }
        assert!(
            partial && final_partitioned && exchange,
            "native distributed upper pipeline: {query}"
        );
        let mut config = SessionConfig::new()
            .with_target_partitions(2)
            .with_batch_size(1);
        config.options_mut().execution.enable_migration_aggregate = true;
        let ctx = SessionContext::new_with_config(config);
        let mut stream = datafusion::physical_plan::execute_stream(plan, ctx.task_ctx()).unwrap();
        let first = tokio::time::timeout(Duration::from_secs(60), stream.next()).await;
        // Release on failure too, so a failed assertion cannot leave source
        // tasks waiting for a test-owned permit forever.
        eof.add_permits(2);
        let first = first
            .expect("upper aggregation must emit a closed block before source EOF")
            .expect("nonempty answer")
            .unwrap();
        assert!(first.num_rows() > 0);
        let ends = first
            .column_by_name(series::BLOCK_END)
            .unwrap()
            .as_primitive::<TimestampMillisecondType>();
        assert!(
            (0..ends.len()).all(|row| ends.value(row) < range.end_ms),
            "must emit an early block: {query}"
        );
        let mut output = vec![first];
        tokio::time::timeout(Duration::from_secs(60), async {
            while let Some(batch) = stream.next().await {
                output.push(batch.unwrap());
            }
        })
        .await
        .expect("released EOF must complete");
        let mut points = Vec::new();
        for batch in output {
            let lists = batch
                .column_by_name(series::SAMPLES)
                .unwrap()
                .as_list::<i32>();
            for row in 0..batch.num_rows() {
                points.extend(rows(lists, row));
            }
        }
        points.sort_by_key(|(t, _)| *t);
        assert_eq!(
            points,
            (0..64).map(|t| (t, expected)).collect::<Vec<_>>(),
            "{query}"
        );
    }
}
