//! The engine's registered extension planner, not a test-only lowering.
use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::Arc;

use datafusion::common::config::ConfigOptions;
use datafusion::common::FunctionalDependencies;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{PhysicalExpr, PhysicalSortExpr};
use datafusion::physical_optimizer::projection_pushdown::ProjectionPushdown;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::aggregates::AggregateExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::{
    displayable, ChildrenPropertiesMode, Distribution, ExecutionPlan, ExecutionPlanProperties,
    Partitioning, ReplaceChildrenOptions,
};
use promql_engine::{Engine, MemorySeriesSource, RangeQuery, SeriesIdEncoding};

fn find<'a>(plan: &'a Arc<dyn ExecutionPlan>, name: &str) -> Option<&'a Arc<dyn ExecutionPlan>> {
    if plan.name() == name {
        return Some(plan);
    }
    plan.children()
        .into_iter()
        .find_map(|child| find(child, name))
}
fn replace_children(
    plan: &Arc<dyn ExecutionPlan>,
    children: Vec<Arc<dyn ExecutionPlan>>,
) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
    plan.clone().replace_children(
        children,
        ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
    )
}
fn series_eval(plan: &Arc<dyn ExecutionPlan>) -> &Arc<dyn ExecutionPlan> {
    find(plan, "SeriesEvalExec").expect("the series evaluator is directly planned")
}
fn source(n: usize, id: SeriesIdEncoding) -> MemorySeriesSource {
    let rows = ["x{pod=\"a\"} 1+1x19", "x{pod=\"b\"} 2+3x19"]
        .map(|s| promql_parser::parse_series_desc(s).unwrap());
    MemorySeriesSource::from_descriptions(&rows, 30.)
        .chunked(0)
        .blocks(NonZeroU64::new(67_000).unwrap())
        .partitions(NonZeroUsize::new(n).unwrap())
        .with_ids(id)
}
async fn plan(n: usize, id: SeriesIdEncoding, query: &str) -> Arc<dyn ExecutionPlan> {
    Engine::new()
        .prepare_async(
            &source(n, id),
            query,
            &RangeQuery::new(300_000, 600_000, 30_000),
        )
        .await
        .unwrap()
        .physical_plan()
        .clone()
}

#[tokio::test]
async fn series_evaluation_reports_partition_width_and_only_block_order() {
    for n in [1, 2, 4] {
        for id in [SeriesIdEncoding::UInt64, SeriesIdEncoding::FixedSizeBinary] {
            for query in [
                "x",
                "timestamp(x)",
                "rate(x[5m])",
                "sum by (pod)(rate(x[5m]))",
            ] {
                let plan = plan(n, id, query).await;
                let shown = displayable(plan.as_ref()).indent(true).to_string();
                let eval = series_eval(&plan);
                let input = eval.children()[0];
                assert_eq!(input.output_partitioning().partition_count(), n, "{shown}");
                assert!(
                    matches!(
                        input.output_partitioning(),
                        Partitioning::UnknownPartitioning(_)
                    ),
                    "{shown}"
                );
                assert_eq!(eval.output_partitioning().partition_count(), n, "{shown}");
                assert!(
                    matches!(
                        eval.output_partitioning(),
                        Partitioning::UnknownPartitioning(_)
                    ),
                    "{shown}"
                );
                assert_eq!(
                    input
                        .schema()
                        .fields()
                        .iter()
                        .map(|f| f.name().as_str())
                        .collect::<Vec<_>>(),
                    ["labels", "samples", "block_start", "block_end", "series_id"]
                );
                // Source co-location does not establish compatibility with
                // DataFusion's hash function.
                assert!(matches!(
                    eval.input_distribution_requirements()
                        .into_per_child()
                        .as_slice(),
                    [Distribution::UnspecifiedDistribution]
                ));
                assert_eq!(eval.benefits_from_input_partitioning(), [false]);
                assert!(eval.equivalence_properties().constraints().is_empty());
                assert_eq!(eval.required_input_ordering().len(), 1);
                assert!(eval.required_input_ordering()[0].is_some());
                let block_start =
                    Arc::new(Column::new_with_schema("block_start", &eval.schema()).unwrap());
                assert!(
                    eval.equivalence_properties()
                        .ordering_satisfy([PhysicalSortExpr::new_default(block_start),])
                        .unwrap(),
                    "{shown}"
                );
                for name in ["labels", "series_id"] {
                    let expr = Arc::new(Column::new_with_schema(name, &input.schema()).unwrap());
                    assert!(
                        !input
                            .equivalence_properties()
                            .ordering_satisfy([PhysicalSortExpr::new_default(expr)])
                            .unwrap(),
                        "source invented {name} ASC ordering: {shown}"
                    );
                }
                let mut stack = vec![&plan];
                while let Some(node) = stack.pop() {
                    if let Some(agg) = node.downcast_ref::<AggregateExec>() {
                        assert!(
                            agg.aggr_expr().iter().all(|e| ![
                                "promql_vector_selector",
                                "promql_range_function",
                            ]
                            .contains(&e.fun().name())),
                            "{shown}"
                        );
                    }
                    stack.extend(node.children());
                }
                assert_eq!(
                    plan.schema()
                        .fields()
                        .iter()
                        .map(|f| f.name().as_str())
                        .collect::<Vec<_>>(),
                    ["labels", "samples", "block_start", "block_end"]
                );
            }
        }
    }
}

#[tokio::test]
async fn child_replacement_rebinds_columns_and_partition_width() {
    for id in [SeriesIdEncoding::UInt64, SeriesIdEncoding::FixedSizeBinary] {
        let original = plan(1, id, "x").await;
        let eval = series_eval(&original);
        let input = eval.children()[0].clone();
        assert!(replace_children(eval, vec![]).is_err());
        assert!(replace_children(eval, vec![input.clone(), input.clone()]).is_err());
        let source = find(&input, "SeriesSetExec").unwrap();
        let selected = source
            .downcast_ref::<promql_engine::source::SeriesSetExec>()
            .unwrap()
            .source_plan();
        let unwrapped = replace_children(eval, vec![selected.clone()]).unwrap();
        assert_eq!(unwrapped.schema(), eval.schema());
        for indices in [vec![4, 0, 1, 2, 3], vec![0, 1, 3, 2, 4]] {
            let exprs = indices
                .into_iter()
                .map(|i| {
                    let name = input.schema().field(i).name().clone();
                    (
                        Arc::new(Column::new(&name, i)) as Arc<dyn PhysicalExpr>,
                        name,
                    )
                })
                .collect::<Vec<_>>();
            let projected = Arc::new(ProjectionExec::try_new(exprs, input.clone()).unwrap());
            let replaced = replace_children(eval, vec![projected]).unwrap();
            assert_eq!(replaced.schema(), eval.schema());
        }
        let expressions = input
            .schema()
            .fields()
            .iter()
            .enumerate()
            .map(|(i, field)| {
                (
                    Arc::new(Column::new(field.name(), i)) as Arc<dyn PhysicalExpr>,
                    field.name().clone(),
                )
            })
            .collect::<Vec<_>>();
        let identity =
            Arc::new(ProjectionExec::try_new(expressions.clone(), input.clone()).unwrap());
        let accepted = replace_children(eval, vec![identity]).unwrap();
        assert_eq!(accepted.schema(), eval.schema());
        let wider = plan(3, id, "x").await;
        let accepted =
            replace_children(eval, vec![series_eval(&wider).children()[0].clone()]).unwrap();
        assert_eq!(accepted.output_partitioning().partition_count(), 3);
        let mut aliases = expressions;
        aliases[4].1 = "another_id".into();
        let renamed = Arc::new(ProjectionExec::try_new(aliases, input).unwrap());
        assert!(replace_children(eval, vec![renamed]).is_err());
    }
}

#[tokio::test]
async fn both_id_encodings_retain_raw_dependencies_below_series_eval() {
    for encoding in [SeriesIdEncoding::UInt64, SeriesIdEncoding::FixedSizeBinary] {
        for query in ["x", "rate(x[5m])", "sum by (pod)(rate(x[5m]))"] {
            let logical = Engine::new()
                .plan_async(
                    &source(1, encoding),
                    query,
                    &RangeQuery::new(300_000, 600_000, 30_000),
                )
                .await
                .unwrap();
            let mut stack = vec![&logical];
            let mut evaluations = 0;
            while let Some(node) = stack.pop() {
                if let LogicalPlan::Extension(extension) = node {
                    if extension.node.name() == "SeriesEval" {
                        evaluations += 1;
                        assert_eq!(
                            extension
                                .node
                                .schema()
                                .fields()
                                .iter()
                                .map(|f| f.name().as_str())
                                .collect::<Vec<_>>(),
                            ["labels", "samples", "block_start", "block_end"]
                        );
                        assert_eq!(
                            extension.node.schema().functional_dependencies(),
                            &FunctionalDependencies::empty()
                        );
                        let inputs = extension.node.inputs();
                        assert_eq!(inputs.len(), 1);
                        assert_eq!(
                            extension.node.necessary_children_exprs(&[1]).unwrap(),
                            vec![(0..inputs[0].schema().fields().len()).collect::<Vec<_>>()]
                        );
                    }
                }
                stack.extend(node.inputs());
            }
            assert_eq!(evaluations, 1, "{query}, {encoding:?}");
        }
    }
}

#[tokio::test]
async fn ordinary_column_projection_preserves_aliases_schema_and_optimizer_fixpoint() {
    use datafusion::arrow::array::AsArray;
    use datafusion::prelude::SessionContext;
    for id in [SeriesIdEncoding::UInt64, SeriesIdEncoding::FixedSizeBinary] {
        let original = plan(1, id, "x").await;
        let eval = series_eval(&original);
        for names in [
            vec!["samples"],
            vec!["block_end", "samples", "block_start", "labels"],
            vec!["samples", "samples"],
        ] {
            let expressions = names
                .iter()
                .enumerate()
                .map(|(i, name)| {
                    (
                        Arc::new(Column::new_with_schema(name, &eval.schema()).unwrap())
                            as Arc<dyn PhysicalExpr>,
                        format!("out_{name}_{i}"),
                    )
                })
                .collect::<Vec<_>>();
            let projection = ProjectionExec::try_new(expressions, eval.clone()).unwrap();
            let projected_schema = projection.schema();
            let mut projected: Arc<dyn ExecutionPlan> = Arc::new(projection);
            let optimizer = ProjectionPushdown::new();
            for _ in 0..4 {
                projected = optimizer
                    .optimize(projected, &ConfigOptions::new())
                    .unwrap();
                // Aliases and presentation order stay native; only the
                // evaluator's unused columns are pruned below this boundary.
                assert_eq!(projected.name(), "ProjectionExec");
                assert_eq!(projected.schema(), projected_schema);
                let pruned = series_eval(&projected);
                assert_eq!(pruned.children()[0].schema(), eval.children()[0].schema());
                assert_eq!(pruned.children()[0].schema().fields().len(), 5);
                if names.iter().all(|name| *name == "samples") {
                    assert_eq!(pruned.schema().fields().len(), 1);
                    assert_eq!(pruned.schema().field(0).name(), "samples");
                }
            }
            let batches =
                datafusion::physical_plan::collect(projected, SessionContext::new().task_ctx())
                    .await
                    .unwrap();
            assert!(batches.iter().any(|b| b.num_rows() > 0));
            for batch in batches {
                assert_eq!(batch.schema(), projected_schema);
                let index = names.iter().position(|n| *n == "samples").unwrap();
                let samples = batch.column(index).as_list::<i32>();
                assert!((0..batch.num_rows()).all(|row| samples.value_length(row) > 0));
            }
        }
        let projection = ProjectionExec::try_new(
            vec![(
                datafusion::physical_expr::expressions::lit(1_i64),
                "constant".to_owned(),
            )],
            eval.clone(),
        )
        .unwrap();
        assert!(eval
            .try_swapping_with_projection(&projection)
            .unwrap()
            .is_none());
    }
}
