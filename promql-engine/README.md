# promql-engine

Evaluates PromQL as DataFusion plans. The engine handles lookback, staleness,
the step grid, `offset`, `@`, and the supported functions and operators.
The store implements `SeriesSource` and returns series as Arrow batches.
Unsupported expressions return `EngineError::Unsupported`.

The [series-source contract](../docs/series-source.md) defines the store's
schema and ordering requirements. The [engine design](../docs/engine.md)
explains evaluation and memory use. The
[query API guide](../docs/query-api.md) covers prepared queries and raw streaming.

## Query API

The crate root re-exports these query types and methods:

| Item | Contract |
|---|---|
| `Engine::new`, `Engine::with_options` | Engine for async callers. Execution requires a Tokio runtime. |
| `Engine::blocking`, `Engine::blocking_with_options` | Engine with an owned Tokio runtime for synchronous callers. |
| `Engine::range_query`, `Engine::range_query_async` | Range evaluation. The result is `Vec<RecordBatch>` in the canonical schema, ordered by label set. |
| `Engine::instant_query`, `Engine::instant_query_async` | Evaluation at one timestamp. The result is `InstantResult::Vector`, `Matrix`, `Scalar`, or `String`. |
| `Engine::plan_async`, `Engine::plan_instant_async` | Logical planning without execution. Planning calls the store's `select` method. |
| `Engine::prepare_async` | Range-query preparation. Returns a `PreparedQuery` containing the physical template and result-order policy. |
| `Engine::execute_async` | Prepared-query execution. Repeated and concurrent executions use fresh execution state. |
| `PreparedQuery::physical_plan` | Physical template for inspection or raw execution. Raw execution requires a state reset. |
| `EngineOptions` | Configuration, including `parser: ParserOptions`. Experimental parser flags are off by default. |
| `RangeQuery` | `start_ms`, `end_ms`, `step_ms`, and `lookback_ms`. The default lookback is five minutes. |
| `explain_plan`, `explain_physical_plan` | Text representations of logical and physical plans. |
| `value_type`, `ValueType` | Expression type, determined before evaluation. Correspond to upstream `Expr.Type()`. |

Blocking methods return `EngineError::Runtime` when called inside a Tokio
runtime or on an engine without an owned runtime.

Custom source operators must honor DataFusion's execution-state reset contract.
A prepared query does not guarantee a storage snapshot or identical data across
executions.

### In-memory query example

This range query returns one row per `job`, with three samples per row:

```rust
use promql_engine::series::{self, Series};
use promql_engine::{Engine, MemorySeriesSource, RangeQuery};

let source = MemorySeriesSource::try_new(vec![
	Series::new(
		&[("__name__", "http_requests_total"), ("job", "api")],
		vec![0, 30_000, 60_000],
		vec![1.0, 2.0, 3.0],
	)?,
	Series::new(
		&[("__name__", "http_requests_total"), ("job", "web")],
		vec![0, 30_000, 60_000],
		vec![10.0, 20.0, 30.0],
	)?,
])?;

let engine = Engine::blocking()?;
let range = RangeQuery::new(0, 60_000, 30_000);
let batches = engine.range_query(&source, "sum by (job) (http_requests_total)", &range)?;
for series in series::decode(&batches)? {
	println!("{:?} {:?}", series.label("job"), series.values());
}
// "api" [1.0, 2.0, 3.0]
// "web" [10.0, 20.0, 30.0]
```

A label set can occupy several rows when a query spans several blocks.
`series::coalesce(series::decode(&batches)?)` combines those rows into one
`Series` per label set.

## Store API

`SeriesSource::select(state, matchers, hints)` corresponds to Prometheus's
`storage.Querier.Select`. It runs once per selector, at plan time, and returns
an execution-ready `Arc<dyn ExecutionPlan>`.

| Item | Contract |
|---|---|
| `SeriesSource` | Store trait for source selection. |
| `SelectHints`, `Grouping`, `Shard` | Time bounds, required window coverage, and selection metadata. |
| `SelectorTable` | A selection exposed as a DataFusion `TableProvider`. |
| `CompiledMatcher`, `METRIC_NAME` | Prometheus matcher semantics. An absent label is `""`. Regexes match the whole value. |
| `MemorySeriesSource`, `SeriesIdEncoding` | In-memory store with chunk, block, partition, batch-size, and ID-encoding options. |

### Raw schema

The store's plan supplies these columns. Column order is not significant.
Extra source columns are allowed and dropped during evaluation.

| Column | Arrow type | Meaning |
|---|---|---|
| `labels` | `Struct<{name}: Utf8View, …>` | Sorted, non-null fields for the selection's label names. An absent label has value `""`. |
| `samples` | `List<Struct<timestamp: Timestamp(ms), value: Float64>>` | Samples in a chunk. |
| `block_start` | `Timestamp(ms)` | Inclusive start of the block's window-end range. |
| `block_end` | `Timestamp(ms)` | Exclusive end of the block's window-end range. |
| `series_id` | `UInt64` or `FixedSizeBinary(n > 0)` | Opaque ID corresponding one-to-one with the full label set. |

One row is one chunk of one series in one block. Every row matches every
selector matcher. Within a block, chunks of an ID are consecutive and ordered
by first sample timestamp. Blocks arrive in time order, and an ID never crosses
a partition. Order between IDs is unspecified. The engine trusts these source
requirements. It does not audit each source row.

The `labels` fields are the union of label names in the selection, not in the
whole store. The schema is fixed across all blocks and partitions of that
selection and must be known before execution. Two selectors in one query can
have different label schemas.

The [complete source contract](../docs/series-source.md) defines block coverage,
binding time bounds, advisory hints, and schema validation. Its
[implementation guide](../docs/series-source.md#implementing-it) covers
in-memory, columnar, and remote stores.

### Series helpers

The `series` module defines the raw and evaluated Arrow shapes:

| Item | Contract |
|---|---|
| `series::schema`, `labels_type`, `samples_type`, `Block`, `SERIES_ID` | Schema definitions, block bounds, and the raw ID column name. |
| `series::encode`, `series::decode` | Conversion between Arrow batches and decoded `Series` rows. `encode` rejects duplicate label sets. |
| `series::coalesce` | One decoded `Series` per label set across result rows. |
| `series::clip`, `drop_empty`, `drop_unused_labels`, `validate` | Sample clipping, empty-row removal, label-schema reduction, and schema checks. |
| `Series` | Decoded row with `labels()`, `label(name)`, `timestamps()`, and `values()`. |

Evaluated results keep `labels`, `samples`, `block_start`, and `block_end`.
`series_id` does not appear in results.

## Implementation references

The [engine design](../docs/engine.md) describes evaluation and memory
ownership. [Plan fixtures](tests/testdata/plans) record planner output for
representative expressions. The
[crate root](src/lib.rs) lists public exports and internal modules.

Source comments name the upstream functions they port. The specification is
the Prometheus revision in the
[corpus provenance](../promql-conformance/testdata/prometheus/UPSTREAM.md).

The numerical kernel modules are public for benchmarks, not stable APIs.

## Errors

`EngineError::is_user_error()` identifies `Query`, `Parse`, and `Regex` errors.
The error variants distinguish query failures from store and execution failures:

| Variant | Meaning |
|---|---|
| `Unsupported` | The expression parsed but uses an unimplemented feature. |
| `Query`, `Parse`, `Regex` | Invalid query. `Parse` retains error positions. `Regex` retains the invalid pattern and label name. |
| `Schema`, `Arrow` | A schema check or Arrow operation failed. These errors can occur during result processing, not just planning. |
| `Source` | A source or adapter reported a source-contract error. The engine does not audit source identity or ordering at runtime. |
| `DataFusion` | DataFusion rejected or failed the plan. |
| `Runtime` | A blocking call ran inside a runtime or used an engine without an owned runtime. |
| `RuntimeBuild` | The owned Tokio runtime could not be built. |

`SeriesError` reports `Arrow` and `Invalid` failures from series helpers.

## Tests and benchmarks

The integration tests in `tests/` exercise the public API.
`tests/plan.rs` compares planner output with `tests/testdata/plans/*.yaml`.
The [engine feature checks](../AGENTS.md) require a separate commit explaining
any changed plan pin.

The [benchmark README](benches/README.md) describes the `kernels`, `engine`,
and `memory` targets. [promql-conformance](../promql-conformance/README.md)
records results against the upstream corpus.
