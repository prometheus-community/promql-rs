# Migrating to prepared queries

This is a breaking change to the query-planning API relative to `1951274`.
A prepared query now carries both a physical **template** and the result-order
policy needed for PromQL finalization. An `Arc<dyn ExecutionPlan>` alone cannot
express that policy, and cloning it does not reset stateful exchange channels.

The workspace also moves to DataFusion 55 and Rust 1.94. Stores must satisfy the
[series-source contract](series-source.md), including mandatory raw IDs,
contiguous chunks and an execution-ready source plan. Changing the API calls
alone does not adapt a source that still implements the old ordering contract.

## Which calls change

| Previous API or usage | Replacement |
|---|---|
| `Engine::physical_plan_async(...)` | `Engine::prepare_async(...)` returns `PreparedQuery`, not an execution-plan Arc. |
| Inspect the returned physical plan | Inspect `prepared.physical_plan().as_ref()`. |
| Collect an inspected query with engine result semantics | `engine.execute_async(&prepared).await`. |
| Execute a physical plan directly through DataFusion | Reset a clone of the template first; see [raw streaming](#raw-streaming-and-metrics). |
| `check_selector_plans(...)` | Remove the call. It guarded the old selector/range UDAF plan shapes; the typed evaluator replaces those shapes. It is not replaced by runtime source validation. |

The ordinary query and configuration entrypoints retain their signatures:
`Engine::new`, `with_options`, `blocking`, `blocking_with_options`, `plan_async`,
`plan_instant_async`, `range_query_async`, `range_query`, `instant_query_async`
and `instant_query`. Both planning methods still return logical plans.

`prepare_async` prepares range queries, whose expressions must be scalars or
instant vectors, even when the range has equal bounds. Use `instant_query_async`
or `instant_query` for typed `InstantResult` values, including strings and raw
range selectors. Non-string instant queries share the prepared reset/finalization
path internally; strings are answered without an execution plan.

All query paths honor `EngineOptions::parser`. Stock constructors keep the
experimental [parser feature flags](feature-flags.md) off; preparing a query does
not bypass them.

## Inspect, then execute

Previously, physical inspection looked like this:

```rust,ignore
let plan = engine.physical_plan_async(source, query, &range).await?;
let explanation = explain_physical_plan(plan.as_ref());
```

Prepare once, inspect the template, and execute through the engine:

```rust,no_run
use promql_engine::{explain_physical_plan, Engine, EngineError, RangeQuery, SeriesSource};

# async fn example(source: &dyn SeriesSource) -> Result<(), EngineError> {
let engine = Engine::new();
let range = RangeQuery::new(0, 600_000, 30_000);
let prepared = engine.prepare_async(source, "rate(x[5m])", &range).await?;
let explanation = explain_physical_plan(prepared.physical_plan().as_ref());

let first = engine.execute_async(&prepared).await?;
let second = engine.execute_async(&prepared).await?;
# let _ = (explanation, first, second);
# Ok(())
# }
```

Use a Tokio runtime and prepare/execute with the same `Engine`. Preparation calls
`SeriesSource::select` once per selector. Execution does not select or plan again;
it resets a fresh tree, including the protected source-owned plan, before every
run. Repeated and concurrent executions, and execution after cancellation, do
not reuse consumed exchange channels. Custom source operators must honor
DataFusion's execution-state reset contract. Reusing a plan does not itself
promise a storage snapshot or identical data across executions.

`execute_async` performs the same schema validation, empty-row removal,
duplicate-label checking and result ordering as `range_query_async`. It returns
canonical Arrow batches, not one fully materialized series per label set.
Consumers wanting a matrix can use `series::decode` followed by
`series::coalesce`, which also merges disjoint interleaved result spans.

### Ordering is semantic metadata

The policy is captured from the logical root **before** physical optimization:

- Root instant `sort`, `sort_desc`, `sort_by_label` and `sort_by_label_desc`
  preserve their requested result order. The label-sort functions require
  `enable_experimental_functions` in the engine's parser options.
- Range results use Prometheus's label-set ordering.
- A nested sort under a projection or aggregation, such as `abs(sort(x))`, does
  not supply the root ordering policy. This retains `range_query_async`'s policy
  rather than guessing from a physical `SortExec` found inside the plan.

`PreparedQuery` has no public constructor or caller-supplied ordering flag. It
cannot be reconstructed from an arbitrary raw plan or used to substitute a
rewritten plan for the one the engine prepared.

## Raw streaming and metrics

Use this path only when deliberately bypassing engine collection/finalization,
for example to drain output incrementally or inspect per-execution metrics:

```rust,no_run
use std::sync::Arc;

use datafusion::physical_plan::{execute_stream, execution_plan::reset_plan_states};
use datafusion::prelude::SessionContext;
use futures::StreamExt;
use promql_engine::{Engine, RangeQuery, SeriesSource};

# async fn example(source: &dyn SeriesSource) -> Result<(), Box<dyn std::error::Error>> {
let engine = Engine::new();
let range = RangeQuery::new(0, 600_000, 30_000);
let prepared = engine.prepare_async(source, "rate(x[5m])", &range).await?;
let context = SessionContext::new();

for _ in 0..2 {
    let exec = reset_plan_states(Arc::clone(prepared.physical_plan()))?;
    let mut stream = execute_stream(Arc::clone(&exec), context.task_ctx())?;
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        // Consume this raw output batch; it has not been finalized by Engine.
        # let _ = batch;
    }
    // Inspect this execution tree and its descendants, not the template.
    let root_metrics = exec.metrics();
    # let _ = root_metrics;
}
# Ok(())
# }
```

Reset before **every** raw execution, including the first. The raw task context
must provide the runtime resources required by the source plan; the default
context above is only an example. Raw streaming does not apply the engine's
final result validation, empty-row removal, duplicate checks, ordering policy or
error lifting. Use `execute_async` when those API guarantees are required.

Metrics belong to the reset execution tree. The template exposed by
`PreparedQuery::physical_plan()` is not the tree run by `execute_async`, so its
metrics do not describe that execution. For source-internal metrics, inspect
`SeriesSetExec::source_plan()` on the executed tree: ordinary DataFusion visitors
stop at the protected source leaf. The convenience collection API does not
return its per-run execution tree.

## Users of development snapshots

Some pre-submission snapshots exposed `execute_plan_async(plan)`. That method
was **not** present at the upstream base `1951274`; it is removed rather than
retained as an alias.
Migrate the preparation and execution calls together to `prepare_async` and
`execute_async(&prepared)`. In particular, nested-sort handling now follows the
same logical-root policy as direct query execution, not the old physical-tree
walk.

Snapshots exposing `KeyedId` should use `SeriesIdEncoding` instead. This selects
the in-memory source's ID encoding, not an alternate evaluator. There is no
compatibility alias. No downstream source copies are updated automatically.
