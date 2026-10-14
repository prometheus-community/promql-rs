//! Per-selector ID encodings, partition assignments and source-plan composition.
use std::collections::HashMap;
use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{AsArray, Int64Array, RecordBatch, UInt64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::catalog::Session;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::physical_expr::expressions::{lit, Column};
use datafusion::physical_expr::{LexOrdering, PhysicalExpr, PhysicalSortExpr};
use datafusion::physical_plan::execution_plan::reset_plan_states;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion::physical_plan::{
    collect, collect_partitioned, ExecutionPlan, ExecutionPlanProperties, Partitioning,
};
use datafusion::prelude::SessionContext;
use promql_engine::series::{self, SERIES_ID};
use promql_engine::source::SeriesSetExec;
use promql_engine::{
    Engine, MemorySeriesSource, RangeQuery, SelectHints, Series, SeriesIdEncoding, SeriesSource,
};
use promql_parser::ast::LabelMatcher;

#[derive(Debug)]
struct PartitionedSource {
    rows: Vec<Series>,
    left: SeriesIdEncoding,
    right: SeriesIdEncoding,
    widths: (usize, usize),
}

impl PartitionedSource {
    fn new(left: SeriesIdEncoding, right: SeriesIdEncoding, widths: (usize, usize)) -> Self {
        let rows = ["x", "y"]
            .into_iter()
            .flat_map(|name| {
                (0..12).map(move |i| {
                    Series::new(
                        &[
                            ("__name__", name),
                            ("pod", &format!("p{i:02}")),
                            ("route", &format!("r{}", i % 3)),
                        ],
                        vec![0, 30_000, 60_000, 90_000],
                        vec![i as f64 + 1., i as f64 + 2., i as f64 + 3., i as f64 + 4.],
                    )
                    .unwrap()
                })
            })
            .collect();
        Self {
            rows,
            left,
            right,
            widths,
        }
    }
}

#[async_trait]
impl SeriesSource for PartitionedSource {
    async fn select(
        &self,
        state: &dyn Session,
        matchers: &[LabelMatcher],
        hints: SelectHints,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let right = matchers
            .iter()
            .any(|m| m.name == "__name__" && m.value == "y");
        let (width, id) = if right {
            (self.widths.1, self.right)
        } else {
            (self.widths.0, self.left)
        };
        let source = MemorySeriesSource::try_new(self.rows.clone())
            .unwrap()
            .chunked(0)
            .blocks(NonZeroU64::new(45_000).unwrap())
            .rows_per_batch(NonZeroUsize::new(3).unwrap())
            .partitions(NonZeroUsize::new(width).unwrap())
            .with_ids(id);
        let selected = source.select(state, matchers, hints).await?;
        let input_schema = selected.schema();
        let ctx = SessionContext::new();
        let mut partitions = collect_partitioned(selected, ctx.task_ctx()).await?;
        // Two selections may put the same identity in different partition
        // indices, even when they happen to have equal partition widths.
        if right {
            partitions.rotate_left(1);
        }
        let fields = input_schema
            .fields()
            .iter()
            .map(|field| {
                Arc::new(field.as_ref().clone().with_metadata(HashMap::from([(
                    "source-field".into(),
                    field.name().clone(),
                )])))
            })
            .collect::<Vec<_>>();
        let schema = Arc::new(Schema::new_with_metadata(
            fields,
            HashMap::from([("source-selection".into(), "partitioned".into())]),
        ));
        for batches in &mut partitions {
            for batch in batches {
                *batch = RecordBatch::try_new(schema.clone(), batch.columns().to_vec())?;
            }
        }
        let input: Arc<dyn ExecutionPlan> =
            MemorySourceConfig::try_new_exec(&partitions, schema, None)?;
        // Put columns in a different order beneath evaluation to exercise
        // name binding through ordinary DataFusion operators.
        let filtered: Arc<dyn ExecutionPlan> = Arc::new(FilterExec::try_new(lit(true), input)?);
        let names = ["block_end", "labels", "samples", "block_start", SERIES_ID];
        let expressions = names
            .into_iter()
            .map(|name| {
                Ok((
                    Arc::new(Column::new_with_schema(name, &filtered.schema())?)
                        as Arc<dyn PhysicalExpr>,
                    name.to_owned(),
                ))
            })
            .collect::<datafusion::error::Result<Vec<_>>>()?;
        let plan = Arc::new(ProjectionExec::try_new(expressions, filtered)?);
        Ok(plan)
    }
}

#[derive(Debug, Clone, Copy)]
enum Assembly {
    Merge,
    Hash,
}

#[derive(Debug)]
struct AssembledSource {
    assembly: Assembly,
}

#[async_trait]
impl SeriesSource for AssembledSource {
    async fn select(
        &self,
        _: &dyn Session,
        _: &[LabelMatcher],
        hints: SelectHints,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        // Neither internal partition alone is the selected series. The
        // returned plan establishes co-location and chunk chronology.
        let partitions = [(1000, 3.), (0, 1.)]
            .into_iter()
            .map(|(timestamp, value)| {
                let row = Series::new(&[("__name__", "x")], vec![timestamp], vec![value]).unwrap();
                let batch = series::encode(
                    &series::label_names_of(std::slice::from_ref(&row)),
                    &[row],
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
                columns.push(Arc::new(UInt64Array::from(vec![7])));
                columns.push(Arc::new(Int64Array::from(vec![timestamp])));
                Ok(vec![RecordBatch::try_new(
                    Arc::new(Schema::new(fields)),
                    columns,
                )?])
            })
            .collect::<datafusion::error::Result<Vec<_>>>()?;
        let schema = partitions[0][0].schema();
        let input = MemorySourceConfig::try_new_exec(&partitions, schema.clone(), None)?;
        let ordering = LexOrdering::new(["block_start", SERIES_ID, "chunk_first"].into_iter().map(
            |name| {
                PhysicalSortExpr::new_default(Arc::new(
                    Column::new_with_schema(name, &schema).unwrap(),
                ))
            },
        ))
        .unwrap();
        let plan: Arc<dyn ExecutionPlan> = match self.assembly {
            Assembly::Merge => {
                let sorted = Arc::new(
                    SortExec::new(ordering.clone(), input).with_preserve_partitioning(true),
                );
                Arc::new(SortPreservingMergeExec::new(ordering, sorted))
            }
            Assembly::Hash => {
                let distributed = Arc::new(RepartitionExec::try_new(
                    input,
                    Partitioning::Hash(
                        vec![Arc::new(Column::new_with_schema(SERIES_ID, &schema)?)],
                        2,
                    ),
                )?);
                Arc::new(SortExec::new(ordering, distributed).with_preserve_partitioning(true))
            }
        };
        Ok(plan)
    }
}

#[tokio::test]
async fn source_assembly_survives_engine_optimization() {
    let engine = Engine::new();
    let range = RangeQuery::new(1000, 1000, 1);
    for assembly in [Assembly::Merge, Assembly::Hash] {
        let source = AssembledSource { assembly };
        let ctx = SessionContext::new();
        let selected = source
            .select(&ctx.state(), &[], SelectHints::range(0, 1000))
            .await
            .unwrap();
        let partitions = collect_partitioned(selected, ctx.task_ctx()).await.unwrap();
        let nonempty: Vec<_> = partitions
            .iter()
            .map(|batches| {
                // The source deliberately carries chunk_first for its native
                // assembly. The explicit decode utility accepts canonical fields.
                let canonical = batches
                    .iter()
                    .map(|batch| {
                        let columns = [
                            series::LABELS,
                            series::SAMPLES,
                            series::BLOCK_START,
                            series::BLOCK_END,
                            series::SERIES_ID,
                        ]
                        .map(|name| batch.schema().index_of(name).unwrap());
                        batch.project(&columns).unwrap()
                    })
                    .collect::<Vec<_>>();
                series::decode(&canonical).unwrap()
            })
            .filter(|rows| !rows.is_empty())
            .collect();
        assert_eq!(nonempty.len(), 1, "assembly must co-locate the split ID");
        assert_eq!(
            nonempty[0]
                .iter()
                .flat_map(|row| row.timestamps().iter().copied())
                .collect::<Vec<_>>(),
            [0, 1000],
            "assembly must restore chronological chunk order"
        );
        for query in ["sum(x)", "x"] {
            let output = engine
                .range_query_async(&source, query, &range)
                .await
                .unwrap();
            let actual = answers(&output);
            assert_eq!(actual.len(), 1, "{source:?}, {query}");
            assert_eq!(actual[0].1, [(1000, 3f64.to_bits())], "{source:?}, {query}");
        }
    }
}

#[tokio::test]
async fn source_boundary_retains_diagnostics_and_resets_private_execution_state() {
    let engine = Engine::new();
    let ctx = SessionContext::new();
    let range = RangeQuery::new(1000, 1000, 1);
    for assembly in [Assembly::Merge, Assembly::Hash] {
        let source = AssembledSource { assembly };
        let mut plan = engine
            .prepare_async(&source, "x", &range)
            .await
            .unwrap()
            .physical_plan()
            .clone();
        let explanation = promql_engine::explain_physical_plan(plan.as_ref());
        assert!(explanation.contains("SeriesSetExec: x"));
        assert!(explanation.contains(match assembly {
            Assembly::Merge => "SortPreservingMergeExec:",
            Assembly::Hash => "RepartitionExec:",
        }));
        assert!(explanation.contains("chunk_first"));

        // Reusing the same private hash exchange without resetting it
        // leaves its output partitions consumed by the first execution.
        for _ in 0..2 {
            let mut stack = vec![&plan];
            let mut selected = None;
            while let Some(node) = stack.pop() {
                if let Some(boundary) = node.downcast_ref::<SeriesSetExec>() {
                    assert!(node.children().is_empty());
                    assert_eq!(
                        node.output_partitioning().partition_count(),
                        match assembly {
                            Assembly::Merge => 1,
                            Assembly::Hash => 2,
                        }
                    );
                    assert!(selected.replace(boundary.source_plan().clone()).is_none());
                }
                stack.extend(node.children());
            }
            let selected = selected.unwrap();
            let output = collect(plan.clone(), ctx.task_ctx()).await.unwrap();
            assert_eq!(answers(&output)[0].1, [(1000, 3f64.to_bits())]);
            // These are the executed native source operator's metrics,
            // not counters synthesized by the boundary.
            assert_eq!(selected.metrics().unwrap().output_rows(), Some(2));
            plan = reset_plan_states(plan).unwrap();
            assert_eq!(
                promql_engine::explain_physical_plan(plan.as_ref()),
                explanation
            );
        }
    }
}

type Answer = (Vec<(String, String)>, Vec<(i64, u64)>);
fn answers(batches: &[RecordBatch]) -> Vec<Answer> {
    series::coalesce(series::decode(batches).unwrap())
        .unwrap()
        .into_iter()
        .map(|row| {
            (
                row.labels()
                    .map(|(name, value)| (name.to_string(), value.to_string()))
                    .collect(),
                row.timestamps()
                    .iter()
                    .zip(row.values())
                    .map(|(&timestamp, &value)| (timestamp, value.to_bits()))
                    .collect(),
            )
        })
        .collect()
}

#[tokio::test]
async fn selectors_can_use_different_id_encodings_and_partition_assignments() {
    let engine = Engine::new();
    let range = RangeQuery::new(0, 90_000, 30_000);
    for widths in [(2, 4), (4, 4)] {
        for (left, right) in [
            (SeriesIdEncoding::UInt64, SeriesIdEncoding::UInt64),
            (
                SeriesIdEncoding::FixedSizeBinary,
                SeriesIdEncoding::FixedSizeBinary,
            ),
            (SeriesIdEncoding::FixedSizeBinary, SeriesIdEncoding::UInt64),
            (SeriesIdEncoding::UInt64, SeriesIdEncoding::FixedSizeBinary),
        ] {
            let source = PartitionedSource::new(left, right, widths);
            let reference = MemorySeriesSource::try_new(source.rows.clone()).unwrap();
            for query in [
                "x + y",
                "x or y",
                "x / on(route) group_left() sum by (route)(y)",
                "sum by (route)(rate(x[1m]) + rate(y[1m]))",
                "sum by (route)(x)",
                "count by (route)(y)",
            ] {
                let expected = answers(
                    &engine
                        .range_query_async(&reference, query, &range)
                        .await
                        .unwrap(),
                );
                let actual = answers(
                    &engine
                        .range_query_async(&source, query, &range)
                        .await
                        .unwrap_or_else(|e| panic!("{query}, {left:?}/{right:?}, {widths:?}: {e}")),
                );
                assert_eq!(actual, expected, "{query}, {left:?}/{right:?}, {widths:?}");
            }
        }
    }
}

#[tokio::test]
async fn reordered_source_columns_preserve_answers_and_metadata() {
    let engine = Engine::new();
    let range = RangeQuery::new(0, 90_000, 30_000);
    for encoding in [SeriesIdEncoding::UInt64, SeriesIdEncoding::FixedSizeBinary] {
        let source = PartitionedSource::new(encoding, encoding, (2, 4));
        let plan = engine
            .prepare_async(&source, "x", &range)
            .await
            .unwrap()
            .physical_plan()
            .clone();
        assert_eq!(
            plan.schema()
                .metadata()
                .get("source-selection")
                .map(String::as_str),
            Some("partitioned")
        );
        for name in ["labels", "samples", "block_start", "block_end"] {
            assert_eq!(
                plan.schema()
                    .field_with_name(name)
                    .unwrap()
                    .metadata()
                    .get("source-field")
                    .map(String::as_str),
                Some(name)
            );
        }
        let mut stack = vec![&plan];
        let mut seen = 0;
        while let Some(node) = stack.pop() {
            if node.name() == "SeriesSetExec" {
                seen += 1;
                assert!(matches!(
                    node.output_partitioning(),
                    Partitioning::UnknownPartitioning(2)
                ));
            }
            stack.extend(node.children());
        }
        assert_eq!(seen, 1);
        let aliased = Arc::new(
            ProjectionExec::try_new(
                ["samples", "labels"]
                    .into_iter()
                    .map(|name| {
                        (
                            Arc::new(Column::new_with_schema(name, &plan.schema()).unwrap())
                                as Arc<dyn PhysicalExpr>,
                            format!("out_{name}"),
                        )
                    })
                    .collect::<Vec<_>>(),
                plan,
            )
            .unwrap(),
        );
        let aliased_schema = aliased.schema();
        let ctx = SessionContext::new();
        let batches = datafusion::physical_plan::collect(aliased, ctx.task_ctx())
            .await
            .unwrap();
        assert!(batches.iter().any(|batch| batch.num_rows() > 0));
        for batch in batches {
            assert_eq!(batch.schema(), aliased_schema);
            assert_eq!(batch.schema().field(0).name(), "out_samples");
            assert_eq!(batch.schema().field(1).name(), "out_labels");
            assert_eq!(
                batch
                    .schema()
                    .field(0)
                    .metadata()
                    .get("source-field")
                    .map(String::as_str),
                Some("samples")
            );
            assert!((0..batch.num_rows()).all(|row| batch
                .column(0)
                .as_list::<i32>()
                .value_length(row)
                > 0));
        }
        let expected = MemorySeriesSource::try_new(source.rows.clone()).unwrap();
        assert_eq!(
            answers(
                &engine
                    .range_query_async(&source, "x", &range)
                    .await
                    .unwrap()
            ),
            answers(
                &engine
                    .range_query_async(&expected, "x", &range)
                    .await
                    .unwrap()
            )
        );
    }
}

#[tokio::test]
async fn source_ids_are_internal_to_the_single_evaluator() {
    #[derive(Debug)]
    struct WithId;
    #[async_trait]
    impl SeriesSource for WithId {
        async fn select(
            &self,
            _: &dyn Session,
            _: &[LabelMatcher],
            hints: SelectHints,
        ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
            let row = Series::new(&[("__name__", "x")], vec![0], vec![7.]).unwrap();
            let batch = series::encode(
                &series::label_names_of(std::slice::from_ref(&row)),
                &[row],
                series::Block {
                    start_ms: hints.start_ms + hints.window_ms,
                    end_ms: hints.end_ms + 1,
                },
            )
            .unwrap();
            let mut fields = batch.schema().fields().to_vec();
            fields.push(Arc::new(Field::new(SERIES_ID, DataType::UInt64, false)));
            let mut columns = batch.columns().to_vec();
            columns.push(Arc::new(UInt64Array::from(vec![42])));
            let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)?;
            Ok(MemorySourceConfig::try_new_exec(
                &[vec![batch.clone()]],
                batch.schema(),
                None,
            )?)
        }
    }
    let engine = Engine::new();
    let plan = engine
        .prepare_async(&WithId, "x", &RangeQuery::new(0, 0, 1))
        .await
        .unwrap()
        .physical_plan()
        .clone();
    let mut stack = vec![&plan];
    let mut evaluations = 0;
    while let Some(node) = stack.pop() {
        evaluations += usize::from(node.name() == "SeriesEvalExec");
        stack.extend(node.children());
    }
    assert_eq!(evaluations, 1);
    assert!(plan.schema().index_of(SERIES_ID).is_err());
    let ctx = SessionContext::new();
    let selected = WithId
        .select(&ctx.state(), &[], SelectHints::range(0, 0))
        .await
        .unwrap();
    assert!(selected.schema().index_of(SERIES_ID).is_ok());
    let output = engine
        .range_query_async(&WithId, "x", &RangeQuery::new(0, 0, 1))
        .await
        .unwrap();
    assert_eq!(answers(&output)[0].1, [(0, 7f64.to_bits())]);
}
