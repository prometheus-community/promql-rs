//! The planner's output shape, pinned as insta snapshots in `plan/snapshots/`.
//!
//! Every other suite here checks numbers, which a planner change can keep
//! right while quietly moving work between nodes: a projection that stops
//! pruning labels, an aggregation that loses its grouping expression, a
//! literal that stops being folded in. Rendering the `LogicalPlan` and
//! comparing it to an expected text makes that visible as a reviewable
//! diff instead. The rendering is `promql_engine::explain_plan`, not
//! `LogicalPlan::display_indent()`: see `src/explain.rs` for why a plan
//! pin needs its own reader-facing text and what makes that text safe to
//! trust.
//!
//! The snapshots are descriptive, not normative: a deliberate planner
//! change re-blesses them with `cargo insta review` (or `cargo insta
//! accept`, or `INSTA_UPDATE=always cargo test`), in a commit of its own
//! whose message says why the shape moved, and that diff is the review.
//! The corpus is meant to grow one query at a time as shapes are added.
//!
//! # One module per upstream topic
//!
//! Each module under `plan/` is named after the promqltest file in
//! `promql-conformance/testdata/prometheus/` whose topic it covers, so a
//! shape sits next to the evaluations that check its numbers. A module's
//! `RANGE` is the query range its cases plan with, so the literals in a
//! plan never differ within a module unless a case passes bounds of its
//! own, for a shape that only exists at other bounds.
//!
//! Modules are code, so none can be left out of a run the way a data file
//! could. What can go stale is the other direction, a `.snap` file whose
//! test was renamed or deleted: `cargo insta test --unreferenced=reject`
//! names those.
//!
//! # Adding a case
//!
//! Add a test to the module for its topic, or start a module named after
//! the upstream `.test` file, with a doc comment saying what shape it pins,
//! the series the query runs over, and an `assert_snapshot!` named after
//! the test whose third argument is the query, so the `.snap` header shows
//! it. Run `cargo insta test --review`: a case without a snapshot fails and
//! the review prints the rendered plan. Read it before accepting. The
//! point is to notice a shape you did not expect, not to record one.
//!
//! [`Case::physical`], asserted as a second snapshot named `<test>_physical`,
//! also pins the `ExecutionPlan`, rendered by
//! `promql_engine::explain_physical_plan`: the calls read as in the logical
//! plan, and every column printed keeps the `@index` the node evaluates.
//! Only small cases can carry one: partition counts and repartitioning make
//! larger physical plans too fragile to hold as text.
//!
//! [`Case::chunked`] and [`Case::partitions`] run a case over
//! [`MemorySeriesSource::chunked`] and [`MemorySeriesSource::partitions`],
//! the ID-contiguous store shapes the evaluator consumes.
//!
//! # Why the series are per case
//!
//! A projection over the source names the label fields one by one, and
//! those names come from the source's data. So the label set is part of
//! the shape being pinned, and a case that shares someone else's fixture
//! would pin someone else's schema. Only the label set reaches the
//! planner, so one sample per series is enough and a second adds
//! nothing; [`Case::source`] rejects any series carrying more, so the
//! corpus cannot drift into looking like a data-driven evaluation suite.
//! A chunked case is the exception: there the samples decide how many
//! batches the store hands over, which the physical plan shows.
//!
//! # Why no Filter appears
//!
//! Matchers are the store's to apply. `SeriesSource::select` takes them,
//! so they never become a plan node, and two queries differing only in
//! their matchers render the same text.

use std::sync::Arc;

use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{LexOrdering, OrderingRequirements, PhysicalSortExpr};
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion::physical_plan::{
    displayable, Distribution, ExecutionPlan, ExecutionPlanProperties, Partitioning,
};
use promql_engine::{Engine, EngineOptions, MemorySeriesSource, RangeQuery, SeriesSource};
use std::num::{NonZeroU64, NonZeroUsize};

// A test crate's root resolves `mod` against `tests/`, where each file is
// a test binary of its own and `functions.rs` already is one.
#[path = "plan/aggregators.rs"]
mod aggregators;
#[path = "plan/at_modifier.rs"]
mod at_modifier;
#[path = "plan/functions.rs"]
mod functions;
#[path = "plan/operators.rs"]
mod operators;
#[path = "plan/range_queries.rs"]
mod range_queries;
#[path = "plan/selectors.rs"]
mod selectors;

fn nz(n: usize) -> NonZeroUsize {
    NonZeroUsize::new(n).expect("a count of at least one")
}

fn nz64(n: u64) -> NonZeroU64 {
    NonZeroU64::new(n).expect("a span of at least a millisecond")
}

#[derive(Debug, Clone, Copy)]
struct Bounds {
    start_ms: i64,
    end_ms: i64,
    step_ms: i64,
}

/// The series a pinned query plans over, and the store shape that hands
/// them over.
#[derive(Debug)]
struct Case {
    range: Bounds,
    load: &'static [&'static str],
    chunked_ms: Option<i64>,
    blocks_ms: Option<u64>,
    partitions: Option<usize>,
    instant: bool,
}

impl Case {
    fn new(range: Bounds, load: &'static [&'static str]) -> Self {
        Case {
            range,
            load,
            chunked_ms: None,
            blocks_ms: None,
            partitions: None,
            instant: false,
        }
    }

    /// [`MemorySeriesSource::chunked`]: the store hands each series over
    /// as chunks of at most this span, which only the physical plan shows.
    fn chunked(mut self, ms: i64) -> Self {
        self.chunked_ms = Some(ms);
        self
    }

    /// [`MemorySeriesSource::blocks`].
    fn blocks(mut self, ms: u64) -> Self {
        self.blocks_ms = Some(ms);
        self
    }

    /// [`MemorySeriesSource::partitions`].
    fn partitions(mut self, n: usize) -> Self {
        self.partitions = Some(n);
        self
    }

    /// Plan as an instant query at the range's start, the only way to
    /// plan a range selector, which a range query refuses.
    fn instant(mut self) -> Self {
        self.instant = true;
        self
    }

    fn source(&self) -> MemorySeriesSource {
        let series: Vec<_> = self
            .load
            .iter()
            .map(|l| {
                let desc = promql_parser::parse_series_desc(l).expect("series line parses");
                // Chunking is the one thing that turns the sample count into
                // plan shape, as the number of batches the store hands over.
                assert!(
                    desc.values.len() == 1 || self.chunked_ms.is_some() || self.blocks_ms.is_some(),
                    "series {l:?} has {} samples; an unchunked plan case carries exactly one, \
                     because nothing but the label set reaches the planner",
                    desc.values.len(),
                );
                desc
            })
            .collect();
        // The interval only spaces samples out, and no sample is ever read.
        let mut source = MemorySeriesSource::from_descriptions(&series, 30.0);
        // One chunk per batch: the cases show a label set crossing batches
        // as the batch count under SeriesSetExec.
        if let Some(ms) = self.chunked_ms {
            source = source.chunked(ms).rows_per_batch(nz(1));
        }
        if let Some(ms) = self.blocks_ms {
            source = source.blocks(nz64(ms));
        }
        if let Some(n) = self.partitions {
            source = source.partitions(nz(n));
        }
        source
    }

    fn range_query(&self) -> RangeQuery {
        RangeQuery::new(self.range.start_ms, self.range.end_ms, self.range.step_ms)
    }

    /// The `LogicalPlan`, rendered by `promql_engine::explain_plan`.
    fn plan(&self, query: &str) -> String {
        let source = self.source();
        let engine = engine();
        let plan = runtime()
            .block_on(async {
                if self.instant {
                    engine
                        .plan_instant_async(&source, query, self.range.start_ms)
                        .await
                } else {
                    engine.plan_async(&source, query, &self.range_query()).await
                }
            })
            .expect("the query plans");
        promql_engine::explain_plan(&plan)
    }

    /// The `ExecutionPlan`, for shapes the logical plan cannot show: the
    /// store-order wrapper and the physical operators DataFusion picks.
    /// Pinned only where the text does not depend on the machine.
    fn physical(&self, query: &str) -> String {
        assert!(
            !self.instant,
            "the engine lowers only the plan a range query runs"
        );
        let source = self.source();
        // The engine's own physical plan, not one lowered here, so the text is
        // what the engine runs.
        let exec = runtime()
            .block_on(engine().prepare_async(&source, query, &self.range_query()))
            .expect("the plan lowers")
            .physical_plan()
            .clone();
        let rendered = promql_engine::explain_physical_plan(exec.as_ref());
        // A shuffle above the selector is as wide as the session's
        // target_partitions, DataFusion's default of one per core.
        let cores = std::thread::available_parallelism().map_or(1, |n| n.get());
        rendered.replace(
            &format!("], {cores}), input_partitions="),
            "], target_partitions), input_partitions=",
        )
    }
}

/// Every gate is open: a pin is about the plan a query gets, and the
/// corpus spans the experimental surface (fill in `operators`, and
/// whatever the next topic adds). Whether stock options refuse that
/// syntax is `tests/feature_flags.rs`'s question.
///
/// Not `Engine::blocking`: an engine that owns a runtime cannot be
/// dropped from inside one, and the cases drive [`runtime`]'s.
fn engine() -> Engine {
    Engine::with_options(EngineOptions {
        parser: promql_parser::ParserOptions::all(),
    })
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
}

// Source ID contiguity is not a native hash or label-ordering guarantee.
fn series_evaluations(plan: &Arc<dyn ExecutionPlan>) -> Vec<&Arc<dyn ExecutionPlan>> {
    let mut found = Vec::new();
    let mut stack = vec![plan];
    while let Some(node) = stack.pop() {
        if node.name() == "SeriesEvalExec" {
            found.push(node);
        }
        stack.extend(node.children());
    }
    found
}

fn two_counters() -> Vec<promql_parser::SeriesDescription> {
    [r#"x{pod="a"} 1+1x19"#, r#"x{pod="b"} 2+3x19"#]
        .iter()
        .map(|l| promql_parser::parse_series_desc(l).expect("series line parses"))
        .collect()
}

fn physical_plan(source: &dyn SeriesSource, query: &str) -> Arc<dyn ExecutionPlan> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    rt.block_on(Engine::new().prepare_async(
        source,
        query,
        &RangeQuery::new(300_000, 600_000, 30_000),
    ))
    .unwrap_or_else(|e| panic!("{query}: {e}"))
    .physical_plan()
    .clone()
}

#[test]
fn series_evaluation_preserves_store_width_without_a_hash_requirement() {
    for n in [1, 4] {
        let source = MemorySeriesSource::from_descriptions(&two_counters(), 30.0)
            .chunked(150_000)
            .partitions(nz(n));
        for query in ["x", "rate(x[5m])", "sum by (pod) (rate(x[5m]))"] {
            let plan = physical_plan(&source, query);
            let shown = displayable(plan.as_ref()).indent(true).to_string();
            let evaluations = series_evaluations(&plan);
            assert_eq!(evaluations.len(), 1, "{shown}");
            for eval in evaluations {
                assert!(
                    matches!(
                        eval.output_partitioning(),
                        Partitioning::UnknownPartitioning(width) if *width == n
                    ),
                    "{shown}"
                );
                assert!(matches!(
                    eval.input_distribution_requirements()
                        .into_per_child()
                        .as_slice(),
                    [Distribution::UnspecifiedDistribution]
                ));
                assert_eq!(eval.benefits_from_input_partitioning(), [false]);
                assert!(eval.equivalence_properties().constraints().is_empty());
                assert_eq!(eval.children().len(), 1);
                assert!(eval.children()[0].downcast_ref::<AggregateExec>().is_none());
                let input = eval.children()[0];
                assert!(
                    matches!(
                        input.output_partitioning(),
                        Partitioning::UnknownPartitioning(width) if *width == n
                    ),
                    "{shown}"
                );
                let ordering = ["block_start"].map(|name| {
                    PhysicalSortExpr::new_default(Arc::new(
                        Column::new_with_schema(name, &input.schema()).unwrap(),
                    ))
                });
                assert!(
                    input
                        .equivalence_properties()
                        .ordering_satisfy(ordering.clone())
                        .unwrap(),
                    "{shown}"
                );
                assert_eq!(
                    eval.required_input_ordering(),
                    [Some(OrderingRequirements::from(
                        LexOrdering::new(ordering).unwrap()
                    ))],
                    "{shown}"
                );
            }
        }
    }
}

/// An upper grouping must establish its own DataFusion distribution;
/// arbitrary store partitioning is not a grouping hash.
#[test]
fn upper_grouping_establishes_its_own_distribution() {
    let source = MemorySeriesSource::from_descriptions(&two_counters(), 30.0)
        .chunked(150_000)
        .partitions(nz(4));
    let plan = physical_plan(&source, "sum by (pod) (rate(x[5m]))");
    let shown = displayable(plan.as_ref()).indent(true).to_string();
    assert_eq!(series_evaluations(&plan).len(), 1, "{shown}");
    let mut stack = vec![&plan];
    let mut checked = false;
    while let Some(node) = stack.pop() {
        if let Some(aggregate) = node.downcast_ref::<AggregateExec>() {
            if matches!(
                aggregate.mode(),
                AggregateMode::Partial | AggregateMode::Single | AggregateMode::SinglePartitioned
            ) && aggregate
                .aggr_expr()
                .iter()
                .any(|expr| expr.fun().name() == promql_engine::aggregate::NAME)
            {
                assert!(
                    matches!(
                        aggregate.input().output_partitioning(),
                        Partitioning::UnknownPartitioning(_)
                    ),
                    "the source invented a hash before upper grouping:\n{shown}"
                );
                checked = true;
            }
        }
        stack.extend(node.children());
    }
    assert!(
        checked,
        "no first-stage upper aggregation was checked:\n{shown}"
    );
}
