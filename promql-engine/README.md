# promql-engine

Evaluates PromQL as DataFusion plans over a store you plug in. The crate owns
everything PromQL (lookback, staleness, the step grid, `offset`, `@`,
functions, aggregations, vector matching). The store owns selection: given
label matchers and a time range, it returns a DataFusion `ExecutionPlan` whose
rows are chunks of series.

The design notes behind this README:

- [`docs/series-source.md`](../docs/series-source.md): the store contract
  (the trait and the Arrow shape).
- [`docs/engine.md`](../docs/engine.md): evaluation, retained state, memory.
- [`docs/engine-blocks.md`](../docs/engine-blocks.md): one `rate` walked across
  chunks, batches and a block edge, with numbers.
- [`docs/query-api.md`](../docs/query-api.md): prepared queries, raw streaming,
  metrics.

## The layers

A query passes through six layers. Each one names the Prometheus function it
ports, so you can read the Go code side by side.

```text
 PromQL text
   │  1. Parse             promql_parser::Parser, gated by EngineOptions::parser
   ▼
 Expr (AST)
   │  2. Check and type    plan::check_expr, function.rs, value_type.rs
   ▼
   │  3. Plan              plan.rs ──► SeriesSource::select, once per selector,
   │                                   wrapped as SelectorTable (a TableScan)
   ▼
 LogicalPlan
   TableScan selector_N ─► SeriesEval ─► Projection / Aggregate with promql_* UDFs
   │  4. Lower             DataFusion's physical planner + SeriesEvalQueryPlanner
   ▼
 ExecutionPlan (held in a PreparedQuery)
   SeriesSetExec(store plan) ─► SeriesEvalExec ─► AggregateExec / ProjectionExec …
   │  5. Execute           reset_plan_states, then collect
   ▼
 RecordBatches
   │  6. Finalize          labelset::finalize: validate, drop empty rows,
   ▼                       reject duplicate label sets, order by label set
 Vec<RecordBatch> in the canonical schema, or an InstantResult
```

### Layer 0: the store seam (`source.rs`, `series.rs`)

This is the one layer you implement. `SeriesSource::select(state, matchers,
hints)` is Prometheus's `storage.Querier.Select`. It is called **at plan
time**, once per selector, and returns an execution-ready
`Arc<dyn ExecutionPlan>`. The engine never looks inside that plan.

`SelectHints` carries what the store must honour and what it may use:

| Field | Binding? | Meaning |
|---|---|---|
| `start_ms`, `end_ms` | yes | Inclusive range, already widened by the window and moved by `offset`/`@` (`getTimeRangesForSelector`). |
| `window_ms` | yes | How far back each block must reach: the lookback delta, or the `[5m]`. |
| `step_ms`, `range_ms` | no | Grid and range-selector window, for aligned or downsampled reads. |
| `func` | no | The function or aggregation directly above the selector (`rate`, `sum`). |
| `grouping` | no | The nearest `by (…)`, seen through label-keeping calls. It does not license merging series. |
| `shard` | no | Return shard `index` of `count` only. |

The rows the plan returns, the **raw series batch**:

```text
labels       Struct<{name}: Utf8View, …>    one field per label name, sorted, non-null; absent label = ""
samples      List<Struct<timestamp: Timestamp(ms), value: Float64>>
block_start  Timestamp(ms)                  the block answers window ends in [block_start, block_end)
block_end    Timestamp(ms)
series_id    UInt64 | FixedSizeBinary(n>0)  mandatory, opaque, 1:1 with the full label set
```

One row is one chunk of one series in one block. The obligations, which the
engine trusts and does not audit per row, are:

- Every row matches every matcher.
- All rows of a block come before the next block's rows.
- Within a block, the chunks of one `series_id` are consecutive and ascend by
  first sample timestamp.
- An identity never crosses a DataFusion partition.
- Order *between* IDs is unspecified.

`docs/series-source.md` has the full contract.

`SeriesSetExec` wraps the store's plan as a protected leaf. It declares only
`block_start ASC` and `UnknownPartitioning(n)`, so DataFusion's optimizer
cannot rewrite the store's internal exchanges or merges.

### Layer 1: parse (`engine.rs` → `promql-parser`)

`Engine::parse` builds a `promql_parser::Parser` with `EngineOptions::parser`.
Every query path parses here, so the experimental syntax gates apply
everywhere. A parse failure is `EngineError::Parse`, which keeps each
`ParseError`'s position.

### Layer 2: check and type (`plan.rs`, `function.rs`, `value_type.rs`)

These checks refuse an invalid query before anything runs:

- `check_expr` walks the whole tree before planning and applies upstream's
  `checkAST` rules: `bool` only on comparisons, no scalar-scalar comparison
  without `bool`, and the vector-matching rules.
- `function.rs` holds upstream's `parser.Functions` table. The parser fills a
  stub `FunctionRef`, so `function::check_call` checks arity and argument
  types as the planner reaches each call.
- `value_type` is `Expr.Type()`. An instant query needs it to know whether its
  one-step result is a vector, a matrix, a scalar or a string.

All of these failures are `EngineError::Query`: Prometheus refuses the same
query, so the error is an answer.

### Layer 3: plan (`plan.rs`)

`plan::plan` (range) and `plan::plan_instant` walk the `Expr` bottom up. Each
node yields a `Planned { plan, label_names }`. The label names travel with the
plan because the node above needs them, for example to turn `by (job)` into
group keys (`labels::group_keys`, upstream's `generateGroupingLabels`).

| PromQL | Logical plan |
|---|---|
| vector selector `x{…}` | `TableScan selector_N` (a `SelectorTable`) → `SeriesEval` with the selector kernel |
| range function `rate(x[5m])` | `TableScan` → `SeriesEval` with the range kernel |
| range selector `x[5m]` (instant only) | `TableScan` → `Projection` with `promql_matrix_selector` |
| aggregation `sum by (…)` | `Aggregate` grouped by `(block_start, block_end, __group__…)` with `promql_aggregate` |
| elementwise function `abs`, `round`, `clamp`, … | `Projection` with `promql_elementwise` |
| binary operator, vector/vector | `Aggregate` with `promql_binary` grouped by match signature, then `Unnest` (matching is grouping, not a join) |
| `absent`, `scalar` | ungrouped `Aggregate` (`promql_absent`, `promql_scalar`) |
| `sort*` | `Sort` over keys from `promql_natural_key` / `promql_labels_key` |
| number literal, `time()`, … | a `MemTable` folded at plan time (`scalar.rs`) |

Anything not yet planned is `EngineError::Unsupported("<feature>")`. The
conformance suite counts those names per feature.

To print a plan the way the pins in `tests/testdata/plans/` do, call
`explain_plan(&plan)`:

```text
Projection: labels{pod: __group__pod}
  Aggregate: sum by (pod) (samples, 0..600000 step 30s) per block
    Projection: labels{pod}
      SeriesEval: rate(samples[5m], 0..600000 step 30s) per series per block
        TableScan: x
```

### Layer 4: lower (`series_eval.rs`, `engine.rs`)

`Engine::prepare_async` hands the logical plan to DataFusion's physical
planner. The engine registers `SeriesEvalQueryPlanner`, which turns each
logical `SeriesEval` into one `SeriesEvalExec`. Everything else lowers as
stock DataFusion: `AggregateExec`, `ProjectionExec`, `RepartitionExec`, and so
on.

The result is a `PreparedQuery`: the physical template plus the result-order
policy, which is captured from the logical root before optimization. The
session config disables round-robin and file-scan repartitioning, so no
optimizer rule splits one identity across partitions.

### Layer 5: execute (`series_eval/`, `buffer.rs`, `selector.rs`, `range.rs`, the UDFs)

`Engine::execute_async` resets a fresh execution tree from the template, then
collects it. You can execute one `PreparedQuery` repeatedly or concurrently.

`SeriesEvalExec` is the port of `evalSeries` / `matrixIterSlice`. Per
partition it holds **one** open identity. It closes that identity when the
`series_id` or the block changes, never at a batch boundary. It feeds chunks
into a `BufferedSeriesIterator`-style window buffer (`buffer.rs`) and answers
each step as soon as its window can no longer change. The kernels:

- `selector.rs`: `vectorSelectorSingle`.
- `range.rs`: `extrapolatedRate`, `*_over_time`, and the rest.
- `math.rs`: Kahan summation, Welford, NaN rules.

Above evaluation, rows are ordinary four-column series, and stock DataFusion
aggregation combines them per block.

### Layer 6: finalize (`labelset.rs`)

`labelset::finalize` applies Prometheus's post-evaluation passes to the
collected batches:

1. `series::validate` on each output schema.
2. `series::drop_empty`.
3. Reject two output series with one label set (`cleanupMetricLabels`).
4. Order by label set, unless the root was an explicit instant `sort*`.

The output keeps the canonical four columns. `series_id` never leaves
evaluation.

## Where the schema comes from

There is no catalog or table schema. Each selector gets its own schema, per
query, at plan time, and the store decides it:

1. `Planner::scan` (`plan.rs`) calls `SelectorTable::try_new` (`source.rs`).
2. `SelectorTable::try_new` calls `source.select(state, matchers, hints)` and
   keeps `plan.schema()`.
3. `SelectorTable` is a DataFusion `TableProvider`, and a provider must report
   its schema before any scan. That is why `select` runs eagerly while the
   engine is still planning: the store's own planning happens during the
   engine's planning, as Prometheus expands a selector's series before it
   evaluates.
4. The fixed part of the schema (the column names and types) is defined once
   in `series.rs`: `series::schema(label_names)`, `labels_type`,
   `samples_type`, and `SERIES_ID`.
5. The variable part is the fields of the `labels` struct: the union of label
   names over the series the selection matched.
   `SelectorTable::label_names()` reads them back, and the planner carries
   them up the tree in `Planned::label_names`.
6. The engine binds the raw schema by name, not by position.
   `SeriesEval::try_new` requires `labels`, `samples`, `block_start`,
   `block_end` and `series_id`. `SeriesEvalExec::try_new` requires `series_id`
   to be `UInt64` or `FixedSizeBinary(n > 0)`. A store missing one of these
   fails at plan time with a DataFusion error. Extra source columns are
   allowed and dropped.

Two consequences matter when you implement a store:

- **Know the label-name union before the first batch.** The schema is fixed
  for the whole select, every block and partition included. A label name that
  first turns up during execution cannot be represented. A real store usually
  answers this from its series index (postings or label-name metadata for the
  matched series) inside `select`, not from a table catalog.
- **Narrow the schema to the selection.** `MemorySeriesSource` calls
  `series::drop_unused_labels` after filtering. Otherwise the schema would
  depend on what else the store holds, and two stores holding the same series
  would answer one query with different schemas. Two selectors in one query
  (for example `a / b`) can have different schemas.

## Public interface

Everything a caller needs is re-exported at the crate root.

| Item | Use it to |
|---|---|
| `Engine` | Run queries. `new()` / `with_options()` for async callers; `blocking()` / `blocking_with_options()` own a Tokio runtime for sync callers. |
| `Engine::range_query[_async]` | Evaluate over a `RangeQuery`. Returns `Vec<RecordBatch>`, canonical schema, label-set order. |
| `Engine::instant_query[_async]` | Evaluate at one instant. Returns an `InstantResult` (`Vector`, `Matrix`, `Scalar`, `String`). |
| `Engine::plan_async` / `plan_instant_async` | Get the `LogicalPlan` without running it. This still calls `select`. |
| `Engine::prepare_async` + `execute_async` | Plan once, execute many times. `PreparedQuery::physical_plan()` is the template for inspection or raw streaming. |
| `EngineOptions` | `parser: ParserOptions`, the experimental-syntax gates, off by default. |
| `RangeQuery` | `start_ms`, `end_ms`, `step_ms`, `lookback_ms` (default 5m). |
| `SeriesSource`, `SelectHints`, `Grouping`, `Shard` | The store seam (layer 0). |
| `SelectorTable` | One `select` result as a DataFusion table, for example to query a store with SQL while debugging. |
| `CompiledMatcher`, `METRIC_NAME` | Prometheus matcher semantics: absent label is `""`, regexes fully anchored. This is the reference a store is held to. |
| `series::{schema, encode, decode, coalesce, clip, drop_empty, drop_unused_labels, validate, Block, SERIES_ID, …}` | Build and read the series shape. |
| `Series` | One decoded row: `labels()`, `label(name)`, `timestamps()`, `values()`. |
| `MemorySeriesSource`, `SeriesIdEncoding` | The in-memory reference store. The test modes `chunked`, `blocks`, `partitions` and `rows_per_batch` reproduce a real store's shapes. |
| `explain_plan`, `explain_physical_plan` | Print plans in the form the plan pins use. |
| `value_type`, `ValueType` | `Expr.Type()`, decided before evaluation. |
| `EngineError`, `SeriesError` | See [Errors](#errors). |

The kernel modules (`aggregate`, `binary`, `elementwise`, `math`, `range`,
`selector`, `absent`) are `pub` for `benches/kernels.rs`. They are not a
stable API.

Every `*_async` method must run on a Tokio runtime: DataFusion spawns tasks
once a plan has more than one partition. The blocking methods refuse to run
inside a runtime and return `EngineError::Runtime`.

## Usage

### Query the in-memory store

```rust
use promql_engine::series::{self, Series};
use promql_engine::{Engine, InstantResult, MemorySeriesSource, RangeQuery};

let source = MemorySeriesSource::try_new(vec![
    Series::new(&[("__name__", "http_requests_total"), ("job", "api")],
                vec![0, 30_000, 60_000], vec![1.0, 2.0, 3.0])?,
    Series::new(&[("__name__", "http_requests_total"), ("job", "web")],
                vec![0, 30_000, 60_000], vec![10.0, 20.0, 30.0])?,
])?;

let engine = Engine::blocking()?;
let range = RangeQuery::new(0, 60_000, 30_000);
let batches = engine.range_query(&source, "sum by (job) (http_requests_total)", &range)?;
for s in series::decode(&batches)? {
    println!("{:?} {:?} {:?}", s.labels().collect::<Vec<_>>(), s.timestamps(), s.values());
}
// [("job", "api")] [0, 30000, 60000] [1.0, 2.0, 3.0]
// [("job", "web")] [0, 30000, 60000] [10.0, 20.0, 30.0]

if let InstantResult::Vector(b) = engine.instant_query(&source, "sum(http_requests_total)", 60_000)? {
    println!("{:?}", series::decode(&b)?[0].values()); // [33.0]
}
```

A label set that spans several blocks comes back as several rows. Call
`series::coalesce(series::decode(&batches)?)` to get one `Series` per label
set.

### Inspect the plan

```rust
let engine = Engine::new(); // inside a Tokio runtime
let plan = engine
    .plan_async(&source, "sum by (pod) (rate(x[5m]))", &RangeQuery::new(0, 600_000, 30_000))
    .await?;
println!("{}", promql_engine::explain_plan(&plan));

let prepared = engine.prepare_async(&source, "rate(x[5m])", &range).await?;
println!("{}", promql_engine::explain_physical_plan(prepared.physical_plan().as_ref()));
let batches = engine.execute_async(&prepared).await?;
```

### Implement a store

The smallest correct store holds each series whole and answers every select as
one block. It follows the same steps as `MemorySeriesSource::select`: filter,
build the schema from the label-name union, stamp the block, attach IDs, clip
to the range.

```rust
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{RecordBatch, UInt64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::catalog::Session;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::error::{DataFusionError, Result};
use datafusion::physical_plan::ExecutionPlan;
use promql_engine::series::{self, Block, Series};
use promql_engine::{CompiledMatcher, SelectHints, SeriesSource};
use promql_parser::ast::LabelMatcher;

/// Whole series held in memory; `ids[i]` is series `i`'s stable identity.
#[derive(Debug)]
struct MyStore {
    series: Vec<Series>,
    ids: Vec<u64>,
}

fn ext(e: impl std::error::Error + Send + Sync + 'static) -> DataFusionError {
    DataFusionError::External(Box::new(e))
}

#[async_trait]
impl SeriesSource for MyStore {
    async fn select(
        &self,
        _state: &dyn Session,
        matchers: &[LabelMatcher],
        hints: SelectHints,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // 1. Filter with Prometheus matcher semantics.
        let compiled: Vec<(&str, CompiledMatcher)> = matchers
            .iter()
            .map(|m| CompiledMatcher::compile(m).map(|c| (m.name.as_str(), c)))
            .collect::<std::result::Result<_, _>>()
            .map_err(ext)?;
        let (hits, ids): (Vec<Series>, Vec<u64>) = self
            .series
            .iter()
            .zip(&self.ids)
            .filter(|(s, _)| compiled.iter().all(|(name, m)| m.matches(s.label(name))))
            .map(|(s, id)| (s.clone(), *id))
            .unzip();

        // 2. The schema: the union of the hits' label names.
        let names = series::label_names_of(&hits);

        // 3. One block over the whole window-end domain, every series whole.
        let block = Block {
            start_ms: hints.start_ms + hints.window_ms,
            end_ms: hints.end_ms + 1,
        };
        let batch = series::encode(&names, &hits, block).map_err(ext)?;

        // 4. The mandatory identity column, last.
        let mut fields = batch.schema().fields().to_vec();
        fields.push(Arc::new(Field::new(series::SERIES_ID, DataType::UInt64, false)));
        let mut columns = batch.columns().to_vec();
        columns.push(Arc::new(UInt64Array::from(ids)));
        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)?;

        // 5. Keep only samples inside the binding range; drop rows left empty.
        let batch = series::clip(&batch, hints.start_ms, hints.end_ms).map_err(ext)?;
        let batch = series::drop_empty(&batch);
        Ok(MemorySourceConfig::try_new_exec(&[vec![batch.clone()]], batch.schema(), None)?)
    }
}
```

A store that sends several chunks per series, or cuts time into several
blocks, builds its Arrow batches directly rather than through `encode`.
`encode` rejects two rows with one label set. The "Implementing it" section of
`docs/series-source.md` covers columnar and remote stores.

## Errors

`EngineError` keeps failure kinds apart because callers treat them
differently. `is_user_error()` picks out the ones an HTTP front end answers
with a 4xx.

| Variant | Meaning |
|---|---|
| `Unsupported(feature)` | Parsed, but not implemented yet. |
| `Query`, `Parse`, `Regex` | The query is invalid. Prometheus refuses it too. |
| `Schema`, `Arrow` | A store or result shape is wrong. |
| `Source` | A store broke an order it promised. |
| `DataFusion` | DataFusion refused or failed the plan. |
| `Runtime`, `RuntimeBuild` | A blocking call inside a runtime, or with no runtime. |

## Module map

| Layer | Files |
|---|---|
| 0 Store seam | `source.rs` (`SeriesSource`, `SelectHints`, `SelectorTable`, `SeriesSetExec`), `series.rs` (the shape), `matcher.rs`, `memory.rs` |
| 1–2 Parse, check, type | `engine.rs` (`parse`), `function.rs`, `value_type.rs`, `plan.rs` (`check_expr`) |
| 3 Plan | `plan.rs`, `params.rs` (the step grid), `labels.rs` (`promql_labels`, group keys), `scalar.rs` (plan-time scalar folding) |
| 4 Lower | `series_eval.rs` (`SeriesEval` node and its extension planner), `engine.rs` (`prepare_async`) |
| 5 Execute | `series_eval/exec.rs` (`SeriesEvalExec`), `series_eval/state.rs`, `buffer.rs`, `selector.rs`, `range.rs`, `math.rs`, `aggregate.rs`, `binary.rs`, `elementwise.rs`, `matrix.rs`, `absent.rs`, `to_scalar.rs`, `sort.rs` |
| 6 Finalize | `labelset.rs` |
| Diagnostics | `explain.rs`, `error.rs` |

## Tests and benches

- `tests/*.rs` cover the engine through its public API. `tests/plan.rs` replays
  the plan pins in `tests/testdata/plans/*.yaml`; a changed pin is a design
  change (see `AGENTS.md`, gate 3).
- `benches/` holds the `kernels`, `engine` and `memory` targets.
  [`benches/README.md`](benches/README.md) covers taking a baseline and reading
  a profile.
- Corpus conformance lives in [`promql-conformance`](../promql-conformance).
