# promql-rs

A PromQL query engine and parser in Rust. The engine compiles PromQL into
DataFusion plans over a store that implements `SeriesSource`. The parser ports
the grammar and AST from [Prometheus](https://github.com/prometheus/prometheus).

Neither the engine nor the parser is complete. The
[conformance report](promql-conformance/testdata/prometheus/UNSUPPORTED.md)
records the engine's passing tests and unsupported features.

## Crates

The workspace members are defined in [`Cargo.toml`](Cargo.toml).

| Crate | Responsibility |
|---|---|
| [promql-engine](promql-engine/README.md) | PromQL evaluation, the `SeriesSource` trait, and the Arrow schema for series. |
| [promql-parser](promql-parser/README.md) | Grammar, lexers, AST, and parse errors ported from upstream `promql/parser`. |
| [promql-common](promql-common/README.md) | `model::Duration` and duration parsing, shared by the parser and engine. |
| [promql-conformance](promql-conformance/README.md) | Prometheus's promqltest corpus, engine adapters, and the passing-test allowlist. |
| [promql-sync](promql-sync/README.md) | Commands to check vendored files, fetch upstream revisions, and generate the Rust grammar. |

## Engine

`SeriesSource::select` runs at plan time and returns an
`Arc<dyn ExecutionPlan>`. The store supplies series as Arrow rows, with opaque
IDs and chronological chunks grouped by ID within each time block.
All chunks of an ID stay in one partition. IDs need not be sorted.

`SeriesEvalExec` evaluates one ID at a time in each partition. A series can
span several batches. The evaluator closes the current ID when the ID or block
changes, or when the stream ends. DataFusion operators combine the evaluated
rows for functions, aggregations, and binary operations.

`Engine::prepare_async` returns a `PreparedQuery` containing a physical plan
template and the result-order policy. `Engine::execute_async` resets execution
state for each run without selecting or planning again. The query methods
return Arrow batches or a typed `InstantResult`.

The engine uses Arrow through `datafusion::arrow`, not a separate `arrow`
dependency. A consumer can therefore use a DataFusion fork without introducing
incompatible Arrow types at the store boundary.

The detailed guides cover separate parts of the engine:

- [Series-source contract](docs/series-source.md): the store trait, schema,
  ordering requirements, and store implementations.
- [Engine design](docs/engine.md): evaluation, retained state, and memory limits.
- [Block evaluation](docs/engine-blocks.md): a range function across chunks,
  batches, and a block boundary.
- [Prepared queries](docs/query-api.md): API migration, repeated execution,
  raw streaming, and metrics.
- [Parser feature flags](docs/feature-flags.md): experimental syntax options,
  disabled by default.
- [Benchmarks](promql-engine/benches/README.md): measurement boundaries,
  comparisons, and profiles.

## Parser and upstream files

The parser uses a generated grammar with Rust actions. `promql-sync` takes the
grammar structure from the vendored `generated_parser.y` and the Rust actions
from `grammar-actions.toml`. An alternative without a Rust action produces an
`Err(())` placeholder. The generator reports each missing action.

The vendored sets record their upstream revision and per-file SHA-256 hashes:

| Set | Provenance | Hashes |
|---|---|---|
| Parser sources | [Parser upstream revision](promql-parser/upstream/UPSTREAM.md) | `promql-parser/upstream/MANIFEST.toml` |
| Engine test corpus | [Corpus upstream revision](promql-conformance/testdata/prometheus/UPSTREAM.md) | `promql-conformance/testdata/prometheus/MANIFEST.toml` |

`promql-sync check` verifies the hashes without network access. The
[parser sync design](docs/parser-sync.md) describes the translation rules and
upgrade procedure.

## Conformance status

The [parser sync follow-ups](docs/parser-sync.md#open-follow-ups) describe the
parser's remaining work.

The engine's
[`SUPPORTED.toml`](promql-conformance/testdata/prometheus/SUPPORTED.toml)
lists the corpus evals that must pass. CI fails if a listed eval fails or
disappears, or if an unlisted eval starts passing.
[`UNSUPPORTED.md`](promql-conformance/testdata/prometheus/UNSUPPORTED.md)
records the pass counts, per-file results, and unsupported features.

This command regenerates both files from an engine run:

```sh
PROMQL_PROMQLTEST_BLESS=1 cargo test -p promql-conformance --test promqltest
```

## Build and test commands

[`Cargo.toml`](Cargo.toml) records the minimum Rust version. The standard
workspace build and test commands are:

```sh
cargo build --workspace
cargo test --workspace
```

The [CI workflow](.github/workflows/ci.yml) defines the full checks and their
order. The [benchmark workflow](.github/workflows/bench.yml) defines benchmark
runs.
[`AGENTS.md`](AGENTS.md) defines the unit, conformance, plan-pin, benchmark,
and profile checks required for an engine feature.

## Developer tools

The `scripts/` directory contains these tools:

- `scripts/gen-conformance` reads upstream parser tests with Go's `go/ast` and
  writes the JSON fixture used by `promql-parser/tests/conformance.rs`.
- `scripts/progress` reconstructs historical promqltest pass counts from Git
  commits that changed `UNSUPPORTED.md`.

## License

[Apache-2.0](LICENSE). The vendored Prometheus sources and test corpus also use
Apache-2.0 and retain their upstream copyright headers.
