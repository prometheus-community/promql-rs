//! The planner's output shape, pinned as text in `testdata/plans/*.yaml`.
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
//! The expectations are descriptive, not normative: a deliberate planner
//! change updates the YAML, and that diff is the review. The corpus is
//! meant to grow one query at a time as shapes are added.
//!
//! # One file per upstream topic
//!
//! Each file is named after the promqltest file in
//! `promql-conformance/testdata/prometheus/` whose topic it covers, so a
//! shape sits next to the evaluations that check its numbers. A file is
//! self-contained like a `.test` file: its `defaults` are the query range
//! its cases plan with, so the literals in a plan never differ within a
//! file unless a case sets a `range` of its own, for a shape that only
//! exists at other bounds. Each file is its own `#[test]`, named after
//! it, and a file missing from the list at the bottom fails the suite
//! rather than never running.
//!
//! # Adding a case
//!
//! Append an entry to the file for its topic, or start a file named after
//! the upstream `.test` file and add it to `plan_files!`, with a `name`, a
//! `description` saying what shape it pins, the `load` series the query
//! runs over, the `query: |`, and an empty `plan: |` block. Both of those
//! are block scalars even for a one-line query, so every entry has one
//! shape and a long query can be wrapped. Run the test:
//! the failure prints the rendered plan, and pasting it in is the whole
//! edit. Read it before pasting — the point is to notice a shape you did
//! not expect, not to record one.
//!
//! A `physical: |` block, added the same way, also pins the
//! `ExecutionPlan`, rendered by `promql_engine::explain_physical_plan`:
//! the calls read as in the logical plan, and every column printed keeps
//! the `@index` the node evaluates. Only small cases can carry one: partition
//! counts and repartitioning make larger physical plans too fragile to
//! hold as text.
//!
//! `chunked_ms` and `partitions` run a case over
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
//! nothing; the loader rejects any series carrying more, so the corpus
//! cannot drift into looking like a data-driven evaluation suite. A
//! chunked case is the exception: there the samples decide how many
//! batches the store hands over, which the physical plan shows.
//!
//! # Conventions borrowed
//!
//! The file layout — `defaults` holding the query range, which a case
//! may override with a `range` of its own, `tests` holding
//! `name`/`description`/`load`/`query` — is the shared engine test-case
//! format that `promql-testcases` binds (thanos-io/promql-engine's
//! `testcases/range_queries.yaml`), read with the same `serde_norway`,
//! with `plan` added and `load` as a list of series rather than a
//! promqltest block. It sits under `testdata/` and resolves against
//! `CARGO_MANIFEST_DIR`, like the conformance corpus, so the suite does
//! not care where it was invoked from.
//!
//! # Why no Filter appears
//!
//! Matchers are the store's to apply. `SeriesSource::select` takes them,
//! so they never become a plan node, and two queries differing only in
//! their matchers render the same text.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{LexOrdering, OrderingRequirements, PhysicalSortExpr};
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion::physical_plan::{
    displayable, Distribution, ExecutionPlan, ExecutionPlanProperties, Partitioning,
};
use promql_engine::{Engine, EngineOptions, MemorySeriesSource, RangeQuery, SeriesSource};
use serde::Deserialize;
use std::num::{NonZeroU64, NonZeroUsize};

fn nz(n: usize) -> NonZeroUsize {
    NonZeroUsize::new(n).expect("a count of at least one")
}

fn nz64(n: u64) -> NonZeroU64 {
    NonZeroU64::new(n).expect("a span of at least a millisecond")
}

#[derive(Debug, Deserialize)]
struct Suite {
    defaults: Bounds,
    tests: Vec<Case>,
}

#[derive(Debug, Clone, Copy, Deserialize)]
struct Bounds {
    start_ms: i64,
    end_ms: i64,
    step_ms: i64,
}

#[derive(Debug, Deserialize)]
struct Case {
    name: String,
    #[allow(dead_code)]
    description: Option<String>,
    load: Vec<String>,
    query: String,
    plan: String,
    /// The `ExecutionPlan`, for shapes the logical plan cannot show: the
    /// store-order wrapper and the physical operators DataFusion picks.
    /// Pinned only where the text does not depend on the machine.
    physical: Option<String>,
    /// [`MemorySeriesSource::chunked`]: the store hands each series over
    /// as chunks of at most this span, which only the physical plan shows.
    chunked_ms: Option<i64>,
    /// [`MemorySeriesSource::blocks`].
    blocks_ms: Option<u64>,
    /// [`MemorySeriesSource::partitions`].
    partitions: Option<usize>,
    /// The default bounds, unless the shape only exists at other ones:
    /// the ordering functions plan a `Sort` over an instant query and
    /// nothing at all over a range.
    range: Option<Bounds>,
    /// Plan as an instant query at the range's start, the only way to
    /// plan a range selector, which a range query refuses. No physical
    /// plan: the engine lowers only the plan a range query runs.
    #[serde(default)]
    instant: bool,
}

fn plans_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/testdata/plans")
}

/// The logical plan's text, and the physical plan's when `case` pins one.
fn plan_of(case: &Case, defaults: Bounds) -> (String, Option<String>) {
    let b = case.range.unwrap_or(defaults);
    let range = &RangeQuery::new(b.start_ms, b.end_ms, b.step_ms);
    let series: Vec<_> = case
        .load
        .iter()
        .map(|l| {
            let desc = promql_parser::parse_series_desc(l).expect("series line parses");
            // Chunking is the one thing that turns the sample count into
            // plan shape, as the number of batches the store hands over.
            assert!(
                desc.values.len() == 1 || case.chunked_ms.is_some() || case.blocks_ms.is_some(),
                "case {:?}: series {l:?} has {} samples; an unchunked plan case carries \
                 exactly one, because nothing but the label set reaches the planner",
                case.name,
                desc.values.len(),
            );
            desc
        })
        .collect();
    // The interval only spaces samples out, and no sample is ever read.
    let mut source = MemorySeriesSource::from_descriptions(&series, 30.0);
    // One chunk per batch: the cases show a label set crossing batches
    // as the batch count under SeriesSetExec.
    if let Some(ms) = case.chunked_ms {
        source = source.chunked(ms).rows_per_batch(nz(1));
    }
    if let Some(ms) = case.blocks_ms {
        source = source.blocks(nz64(ms));
    }
    if let Some(n) = case.partitions {
        source = source.partitions(nz(n));
    }

    // `Engine::new`, not `blocking`: an engine that owns a runtime cannot
    // be dropped from inside one, and the runtime here is this function's.
    //
    // Every gate is open: a pin is about the plan a query gets, and the
    // corpus spans the experimental surface (fill in `operators`, and
    // whatever the next topic adds). Whether stock options refuse that
    // syntax is `tests/feature_flags.rs`'s question.
    let engine = Engine::with_options(EngineOptions {
        parser: promql_parser::ParserOptions::all(),
    });
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    // Both `query` and `plan` are `|` block scalars, so both shed the
    // newline it appends; a query wrapped over several lines keeps its
    // own newlines, which PromQL treats as whitespace.
    let query = case.query.trim_end();
    let plan = rt
        .block_on(async {
            if case.instant {
                engine.plan_instant_async(&source, query, b.start_ms).await
            } else {
                engine.plan_async(&source, query, range).await
            }
        })
        .expect("the query plans");
    let logical = promql_engine::explain_plan(&plan);
    // The engine's own physical plan, not one lowered here, so the text is
    // what the engine runs.
    let physical = case.physical.as_ref().map(|_| {
        let exec = rt
            .block_on(engine.prepare_async(&source, query, range))
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
    });
    (logical, physical)
}

fn check_plan_file(topic: &str) {
    let path = plans_dir().join(format!("{topic}.yaml"));
    let body = std::fs::read_to_string(&path).expect("the plans file is readable");
    let suite: Suite = serde_norway::from_str(&body).expect("the plans file parses");
    assert!(!suite.tests.is_empty(), "{} has no cases", path.display());

    // Every case is reported, not just the first: one planner change
    // usually moves several shapes, and seeing all of them is what makes
    // the update a single reviewable edit.
    let mut failures = Vec::new();
    for case in &suite.tests {
        let (logical, physical) = plan_of(case, suite.defaults);
        let pairs = [
            ("plan", Some(case.plan.as_str()), Some(logical)),
            ("physical", case.physical.as_deref(), physical),
        ];
        for (block, expected, actual) in pairs {
            let (Some(expected), Some(actual)) = (expected, actual) else {
                continue;
            };
            let expected = expected.trim_end();
            let actual = actual.trim_end();
            if actual != expected {
                failures.push(format!(
                    "case {:?}, {block}:\n  query:    {}\n  expected: {}\n  actual:   {}",
                    case.name,
                    case.query.trim_end(),
                    expected.replace('\n', "\n            "),
                    actual.replace('\n', "\n            "),
                ));
            }
        }
    }
    assert!(
        failures.is_empty(),
        "{} of {} plans changed in {}\n\n{}",
        failures.len(),
        suite.tests.len(),
        path.display(),
        failures.join("\n\n"),
    );
}

macro_rules! plan_files {
    ($($topic:ident),* $(,)?) => {
        $(
            #[test]
            fn $topic() {
                check_plan_file(stringify!($topic));
            }
        )*

        #[test]
        fn every_plan_file_has_a_test() {
            let listed = [$(stringify!($topic)),*];
            let unlisted: Vec<_> = std::fs::read_dir(plans_dir())
                .expect("the plans directory is readable")
                .map(|e| e.expect("a directory entry").path())
                .filter(|p| p.extension().is_some_and(|e| e == "yaml"))
                .filter_map(|p| p.file_stem().map(|s| s.to_string_lossy().into_owned()))
                .filter(|stem| !listed.contains(&stem.as_str()))
                .collect();
            assert!(
                unlisted.is_empty(),
                "plan files never run, add them to plan_files!: {unlisted:?}"
            );
        }
    };
}

plan_files!(
    aggregators,
    at_modifier,
    functions,
    operators,
    range_queries,
    selectors
);

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
