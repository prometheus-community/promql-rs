//! Typed selector/range evaluation on a step grid.
//!
//! The logical boundary consumes ID-contiguous raw chunks and produces
//! evaluated series rows. Identity stays below it: later projections may
//! give distinct raw identities the same output labels, so no group uniqueness
//! is declared. The sole executor ports `evalVectorSelector`,
//! `rangeEvalTimestampFunctionOverVectorSelector`, and `matrixIterSlice`
//! in Prometheus `promql/engine.go` at 83962c35.

use std::cmp::Ordering;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::{internal_err, DFSchema, DFSchemaRef, Result};
use datafusion::execution::context::QueryPlanner;
use datafusion::logical_expr::physical_planning_context::PhysicalPlanningContext;
use datafusion::logical_expr::{Expr, Extension, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{DefaultPhysicalPlanner, ExtensionPlanner, PhysicalPlanner};

use crate::buffer::Kernel;
use crate::params::Params;
use crate::series::{BLOCK_END, BLOCK_START, LABELS, SAMPLES, SERIES_ID};

pub(crate) mod exec;
mod state;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct SeriesEval {
    input: LogicalPlan,
    pub(crate) kernel: Kernel,
    pub(crate) params: Params,
    schema: DFSchemaRef,
}

impl PartialOrd for SeriesEval {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        // Like native logical nodes, compare semantic fields rather than the
        // output DFSchema, which is derived only from input and not ordered.
        (&self.input, self.kernel, self.params).partial_cmp(&(
            &other.input,
            other.kernel,
            other.params,
        ))
    }
}

impl SeriesEval {
    pub(crate) fn try_new(input: LogicalPlan, kernel: Kernel, params: Params) -> Result<Self> {
        // Bind by name, retaining field and schema metadata but not raw
        // identity uniqueness. Physical construction binds the ID type.
        input.schema().field_with_unqualified_name(SERIES_ID)?;
        let fields = [LABELS, SAMPLES, BLOCK_START, BLOCK_END]
            .into_iter()
            .map(|name| input.schema().field_with_unqualified_name(name).cloned())
            .collect::<Result<Vec<_>>>()?;
        let schema = Arc::new(DFSchema::from_unqualified_fields(
            fields.into(),
            input.schema().metadata().clone(),
        )?);
        Ok(Self {
            input,
            kernel,
            params,
            schema,
        })
    }

    pub(crate) fn into_plan(self) -> LogicalPlan {
        LogicalPlan::Extension(Extension {
            node: Arc::new(self),
        })
    }
}

impl UserDefinedLogicalNodeCore for SeriesEval {
    fn name(&self) -> &str {
        "SeriesEval"
    }
    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }
    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }
    fn expressions(&self) -> Vec<Expr> {
        Vec::new()
    }
    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "SeriesEval: kernel={:?}, grid={:?}",
            self.kernel, self.params
        )
    }
    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        if !exprs.is_empty() || inputs.len() != 1 {
            return internal_err!("SeriesEval takes no expressions and exactly one input");
        }
        Self::try_new(inputs.swap_remove(0), self.kernel, self.params)
    }
    fn necessary_children_exprs(&self, _output_columns: &[usize]) -> Option<Vec<Vec<usize>>> {
        // Samples-only output still needs full raw identity, samples and
        // blocks. Speculative extracted columns are not output aliases.
        let schema = self.input.schema();
        let columns = [LABELS, SAMPLES, BLOCK_START, BLOCK_END, SERIES_ID]
            .into_iter()
            .map(|name| schema.index_of_column_by_name(None, name))
            .collect::<Option<Vec<_>>>()?;
        Some(vec![columns])
    }
    // Default predicate/limit behavior prevents pushdown through evaluation:
    // a raw chunk is not an evaluated output row.
}

#[derive(Debug)]
pub(crate) struct SeriesEvalQueryPlanner;

#[async_trait]
impl QueryPlanner for SeriesEvalQueryPlanner {
    async fn create_physical_plan(
        &self,
        plan: &LogicalPlan,
        session: &dyn Session,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(SeriesEvalPlanner)])
            .create_physical_plan(plan, session)
            .await
    }
}

#[derive(Debug)]
struct SeriesEvalPlanner;

#[async_trait]
impl ExtensionPlanner for SeriesEvalPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn datafusion::logical_expr::UserDefinedLogicalNode,
        logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session: &dyn Session,
        _planning_ctx: &PhysicalPlanningContext,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let Some(eval) = node.as_any().downcast_ref::<SeriesEval>() else {
            return Ok(None);
        };
        if logical_inputs.len() != 1 || physical_inputs.len() != 1 {
            return internal_err!("SeriesEval requires one logical and physical input");
        }
        Ok(Some(Arc::new(exec::SeriesEvalExec::try_new(
            Arc::clone(&physical_inputs[0]),
            eval.kernel,
            eval.params,
        )?)))
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::{Dependency, FunctionalDependence, FunctionalDependencies};
    use datafusion::logical_expr::{col, EmptyRelation};

    use super::*;
    use crate::{range, selector, series};

    fn params() -> Params {
        Params {
            start_ms: 0,
            end_ms: 100,
            step_ms: 10,
            window_ms: 20,
            offset_ms: 0,
            at_ms: None,
        }
    }

    fn raw() -> LogicalPlan {
        let schema = series::schema(&["pod".to_string()]);
        let mut fields = schema.fields().to_vec();
        fields[1] = Arc::new(
            fields[1].as_ref().clone().with_metadata(
                [("unit".to_string(), "samples".to_string())]
                    .into_iter()
                    .collect(),
            ),
        );
        fields.push(Arc::new(Field::new(SERIES_ID, DataType::UInt64, false)));
        let n = fields.len();
        let arrow = Schema::new_with_metadata(
            fields,
            [("owner".to_string(), "source".to_string())]
                .into_iter()
                .collect(),
        );
        let schema = DFSchema::try_from(arrow)
            .unwrap()
            .with_functional_dependencies(FunctionalDependencies::new(vec![
                FunctionalDependence::new(vec![n - 1], (0..n).collect(), false)
                    .with_mode(Dependency::Single),
            ]))
            .unwrap();
        LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(schema),
        })
    }

    #[test]
    fn evaluated_rows_have_canonical_metadata_but_no_identity_uniqueness() {
        let eval =
            SeriesEval::try_new(raw(), Kernel::Selector(selector::Pick::Value), params()).unwrap();
        let names: Vec<_> = eval
            .schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        assert_eq!(names, [LABELS, SAMPLES, BLOCK_START, BLOCK_END]);
        assert_eq!(
            eval.schema.functional_dependencies(),
            &FunctionalDependencies::empty()
        );
        assert_eq!(
            eval.schema
                .as_arrow()
                .metadata()
                .get("owner")
                .map(String::as_str),
            Some("source")
        );
        assert_eq!(
            eval.schema
                .field(1)
                .metadata()
                .get("unit")
                .map(String::as_str),
            Some("samples")
        );
        assert_eq!(
            eval.necessary_children_exprs(&[1]),
            Some(vec![(0..5).collect()])
        );
        assert!(!eval.supports_limit_pushdown());
        assert_eq!(eval.prevent_predicate_push_down_columns().len(), 4);
    }

    #[tokio::test]
    async fn every_kernel_lowers_to_the_contiguous_executor_with_canonical_metadata() {
        use datafusion::physical_plan::empty::EmptyExec;
        use datafusion::physical_plan::{
            ChildrenPropertiesMode, Distribution, Partitioning, ReplaceChildrenOptions,
        };
        use datafusion::prelude::SessionContext;

        let state = SessionContext::new().state();
        let planner = DefaultPhysicalPlanner::default();
        for kernel in [
            Kernel::Selector(selector::Pick::Value),
            Kernel::Selector(selector::Pick::Timestamp),
            Kernel::Range(range::RangeOp::Func(range::Func::Rate)),
        ] {
            let input = raw();
            let physical: Arc<dyn ExecutionPlan> = Arc::new(crate::source::SeriesSetExec::new(
                Arc::new(EmptyExec::new(Arc::new(input.schema().as_arrow().clone()))),
            ));
            let eval = SeriesEval::try_new(input.clone(), kernel, params()).unwrap();
            let output = SeriesEvalPlanner
                .plan_extension(
                    &planner,
                    &eval,
                    &[&input],
                    &[physical],
                    &state,
                    &PhysicalPlanningContext::default(),
                )
                .await
                .unwrap()
                .unwrap();
            assert_eq!(output.schema().as_ref(), eval.schema.as_arrow());
            assert!(output.is::<exec::SeriesEvalExec>(), "{kernel:?}");
            assert!(output.children()[0].is::<crate::source::SeriesSetExec>());
            assert!(output
                .input_distribution_requirements()
                .per_child_distributions()
                .all(|dist| matches!(dist, Distribution::UnspecifiedDistribution)));
            assert_eq!(output.benefits_from_input_partitioning(), vec![false]);
            assert_eq!(
                output.required_input_ordering()[0]
                    .as_ref()
                    .unwrap()
                    .first()
                    .len(),
                1
            );
            assert!(matches!(
                output.properties().partitioning,
                Partitioning::UnknownPartitioning(1)
            ));
            assert!(output.metrics().is_some());
            let options = ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute);
            assert!(output.clone().replace_children(vec![], options).is_err());
            let rebuilt = output
                .clone()
                .replace_children(vec![output.children()[0].clone()], options)
                .unwrap();
            assert_eq!(rebuilt.schema(), output.schema());
            assert!(matches!(
                rebuilt.properties().partitioning,
                Partitioning::UnknownPartitioning(1)
            ));
        }
    }

    #[test]
    fn speculative_leaf_extractions_stay_below_the_output_boundary() {
        let input = raw();
        let eval = SeriesEval::try_new(
            input.clone(),
            Kernel::Selector(selector::Pick::Value),
            params(),
        )
        .unwrap();
        let mut exprs = vec![datafusion::logical_expr::lit(1).alias("__datafusion_extracted_1")];
        exprs.extend(input.schema().iter().map(Expr::from));
        let speculative = datafusion::logical_expr::LogicalPlanBuilder::from(input)
            .project(exprs)
            .unwrap()
            .build()
            .unwrap();
        let rebuilt = eval
            .with_exprs_and_inputs(vec![], vec![speculative])
            .unwrap();
        assert_eq!(rebuilt.input.schema().fields().len(), 6);
        assert_eq!(rebuilt.schema, eval.schema);
        assert!(!rebuilt
            .schema
            .has_column_with_unqualified_name("__datafusion_extracted_1"));
        assert_eq!(
            rebuilt.necessary_children_exprs(&[1]),
            Some(vec![vec![1, 2, 3, 4, 5]])
        );
    }

    #[test]
    fn timestamp_pick_is_a_distinct_typed_evaluation() {
        use std::hash::{Hash, Hasher};
        let value =
            SeriesEval::try_new(raw(), Kernel::Selector(selector::Pick::Value), params()).unwrap();
        let timestamp =
            SeriesEval::try_new(raw(), Kernel::Selector(selector::Pick::Timestamp), params())
                .unwrap();
        assert_ne!(value, timestamp);
        assert_ne!(value.partial_cmp(&timestamp), Some(Ordering::Equal));
        let hash = |node: &SeriesEval| {
            let mut state = std::collections::hash_map::DefaultHasher::new();
            node.hash(&mut state);
            state.finish()
        };
        assert_ne!(hash(&value), hash(&timestamp));
        assert_eq!(
            timestamp
                .with_exprs_and_inputs(vec![], vec![raw()])
                .unwrap(),
            timestamp
        );
    }

    #[test]
    fn logical_rebuild_retains_typed_parameters_and_name_bound_dependencies() {
        let input = raw();
        let eval = SeriesEval::try_new(
            input.clone(),
            Kernel::Range(range::RangeOp::Func(range::Func::Rate)),
            params(),
        )
        .unwrap();
        let rebuilt = eval.with_exprs_and_inputs(vec![], vec![input]).unwrap();
        assert_eq!(rebuilt, eval);
        assert_eq!(rebuilt.partial_cmp(&eval), Some(Ordering::Equal));
        assert!(eval
            .with_exprs_and_inputs(vec![col(SAMPLES)], vec![raw()])
            .is_err());
        assert!(eval.with_exprs_and_inputs(vec![], vec![]).is_err());
        let reordered = datafusion::logical_expr::LogicalPlanBuilder::from(raw())
            .project([SERIES_ID, BLOCK_END, SAMPLES, LABELS, BLOCK_START].map(col))
            .unwrap()
            .build()
            .unwrap();
        let rebuilt = eval.with_exprs_and_inputs(vec![], vec![reordered]).unwrap();
        assert_eq!(
            rebuilt.necessary_children_exprs(&[1]),
            Some(vec![vec![3, 2, 4, 1, 0]])
        );
        assert_eq!(rebuilt.params, eval.params);
        assert_eq!(rebuilt.schema, eval.schema);
    }

    #[test]
    fn raw_identity_is_mandatory_even_for_samples_only_output() {
        let missing_id = datafusion::logical_expr::LogicalPlanBuilder::from(raw())
            .project([LABELS, SAMPLES, BLOCK_START, BLOCK_END].map(col))
            .unwrap()
            .build()
            .unwrap();
        assert!(SeriesEval::try_new(
            missing_id,
            Kernel::Selector(selector::Pick::Value),
            params()
        )
        .is_err());
    }
}
