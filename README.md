# promql-rs

A PromQL query engine and a PromQL parser in Rust. The engine compiles a parsed
query into a DataFusion logical plan over a store trait. The parser is a
structural hand-port of the Go parser in
[`prometheus/prometheus`](https://github.com/prometheus/prometheus). Both are
measured against Prometheus's own test corpora, vendored in this repository and
pinned to one upstream commit.

Neither is complete. See [Status](#status) for what passes today and where the
numbers come from.

## Layout

The workspace has five crates. Each README lists the crate's public interface
and how to use it.

| Crate | What it is |
|---|---|
| [`promql-engine/`](promql-engine/README.md) | Evaluates PromQL as DataFusion plans. Defines `SeriesSource`, the trait a store implements, and the Arrow schema its data comes back in. |
| [`promql-parser/`](promql-parser/README.md) | The parser. Grammar, lexer, AST, and error types ported from upstream `promql/parser`. |
| [`promql-common/`](promql-common/README.md) | The parts of `prometheus/common`'s `model` package the parser and engine share: `Duration` and its parsing. |
| [`promql-conformance/`](promql-conformance/README.md) | Replays Prometheus's promqltest corpus against the engine and records what passes. |
| [`promql-sync/`](promql-sync/README.md) | Developer-invoked CLI that checks, pulls, and regenerates the vendored upstream files. |

A query crosses them in this order. The engine's README walks each step:

```text
query text ─► promql-parser ─► Expr ─► promql-engine planner ─► DataFusion LogicalPlan
                                              │                        │
                         SeriesSource::select (your store, at plan time)
                                                                       ▼
                     RecordBatches ◄─ finalize ◄─ execute ◄─ ExecutionPlan (PreparedQuery)
```

Vendored upstream material sits next to the crate that consumes it.
`promql-parser/upstream/` holds the Go sources the port is derived from.
`promql-conformance/testdata/prometheus/` holds the `.test` corpus. Both record
their provenance in an `UPSTREAM.md` and a per-file SHA-256 in a
`MANIFEST.toml`.

The design notes are in `docs/`:

- `docs/engine.md`: evaluation, retained state and its current limits, with
  Prometheus and Thanos lineage.
- `docs/series-source.md`: the `SeriesSource` trait and the Arrow shape a store
  returns.
- [`docs/query-api.md`](docs/query-api.md): migrating query callers to prepared
  execution, including raw streaming and metrics.
- [`docs/feature-flags.md`](docs/feature-flags.md): the parser options carried by
  the engine, off by default.
- `docs/engine-blocks.md`: how a range function walks a block and crosses a
  block edge. `docs/engine-blocks.html` is the same walkthrough with diagrams.
- `docs/parser-sync.md`: the translation discipline for the parser port and the
  sync tooling.

Two tools live in `scripts/`. `scripts/gen-conformance` is a Go program that
reads upstream's parser tests with `go/ast` and writes the JSON fixture the
parser replays. `scripts/progress` reconstructs the promqltest pass count for
every commit that changed it, by walking git rather than by reading a metrics
database.

## How the engine runs a query

The engine turns an `Expr` into a DataFusion `LogicalPlan` that sits on top of
whatever `ExecutionPlan` the store returned from `SeriesSource::select`. A
series is a row from the store to the result. The store supplies mandatory
opaque IDs with each identity's chronological chunks contiguous within each
logical time block and colocated in one partition. Order between IDs is
unspecified. A typed `SeriesEval` boundary lowers to one streaming
`SeriesEvalExec`, holding one current identity/fold per source partition;
ordinary DataFusion aggregation combines its evaluated rows above it. A series
may span any number of batches, and its ID may recur in later blocks. The
engine folds chunks without concatenating a whole raw series. Numerical
history follows the window's sample high water plus a bounded ingestion
allowance. `docs/engine.md` describes this and the separate input/output
and native aggregation state.

The crate reaches Arrow only through `datafusion::arrow`. A direct `arrow`
dependency would pin a second Arrow version for any consumer that patches
DataFusion to a fork, and every `RecordBatch` a store hands over would then be a
type mismatch.

`SeriesSource::select` returns an execution-ready `Arc<dyn ExecutionPlan>`.
The protected `SeriesSetExec` leaf keeps source-internal merges/exchanges out
of outer optimization and declares only truthful block ordering, not labels
or IDs ASC. `Engine::prepare_async` returns a `PreparedQuery` with its result-order
policy; `Engine::execute_async` resets a fresh execution tree for each run without
selecting or planning again. `PreparedQuery::physical_plan` exposes the physical
template for inspection. `instant_query_async` returns a typed `InstantResult`,
including scalars, strings and raw range-selector matrices. The latter filter
chunks under their own sample timestamps rather than entering the step-grid
fold. Public `value_type` and `ValueType` classify expressions without inspecting
result rows. See the [query API migration guide](docs/query-api.md)
for call-site changes and execution-state ownership. There is no alternate source
layout or label-projection hint.

`MemorySeriesSource` is an in-memory `SeriesSource` for tests and benchmarks.
`promql-engine/benches` has three criterion targets, `kernels`, `engine`, and
`memory`. `promql-engine/benches/README.md` covers taking a baseline and reading
a profile.

## Why the parser port needs its own tooling

A hand-ported parser rots quietly. Upstream adds a function, a keyword, or an
AST field, nothing here fails to compile, and the port falls behind until
someone notices that a query behaves differently than it does in Prometheus.
The tooling makes that divergence visible and cheap to close.

**Upstream is vendored, not just referenced.** `promql-sync check` verifies
every vendored file against its recorded hash. It needs no network, so CI runs
it on every pull request. A hand-edited vendored file fails the check by name.

**The grammar is generated, not hand-maintained in place.**
`promql-sync generate-grammar` rebuilds `promql-parser/src/grammar.y` from the
vendored `upstream/generated_parser.y` for structure, and from two
hand-maintained sidecar files, `grammar-actions.toml` and `grammar-tokens.toml`,
for the Rust parts. A rule that changed on the Rust side shows up as a small
sidecar diff instead of a diff buried in a large generated file. An upstream
alternative with no sidecar entry gets an `Err(())` placeholder rather than
blocking the build, and the tool prints every unmapped alternative at the end.
`generate-grammar` prints how many placeholders `grammar.y` still holds.

**Upgrades are a worktree diff, not a pipeline.** Nothing here commits,
branches, or opens a pull request:

```sh
cargo run -p promql-sync -- pull --target <upstream-sha-or-tag>
cargo run -p promql-sync -- generate-grammar
# add a [[actions]] entry in grammar-actions.toml for each alternative
# the tool reported as unmapped, then regenerate
cargo run -p promql-sync -- generate-grammar
cargo test -p promql-parser
```

`pull` takes `--set parser` or `--set promqltest` and handles one vendored set
per run. Re-pinning the promqltest set moves test content, so
`promql-conformance/testdata/prometheus/UPSTREAM.md` describes the regeneration
step that has to land in the same change.

## Status

Both vendored sets are pinned to `prometheus/prometheus@83962c35`.

**Neither the engine nor the parser is complete, and most of the corpus does
not pass.** The engine plans a narrow slice of PromQL, so most evals fail on an
expression it cannot plan at all, and whole corpus files score zero. The parser
misses native-histogram descriptors, duration arithmetic, and the anchored and
smoothed selectors. `promql-conformance/testdata/prometheus/UNSUPPORTED.md`
records the pass count, the per-file scoreboard, and every missing feature with
the number of evals it blocks. `PROMQL_PROMQLTEST_BLESS=1` regenerates it from a
real run. `docs/parser-sync.md` draws the same line for the parser port.

CI gates the engine on an allowlist rather than on that count.
`promql-conformance/testdata/prometheus/SUPPORTED.toml` names the evals that
must keep passing. A listed eval that stops passing turns CI red, and so does an
unlisted eval that starts passing, which forces the new coverage to be declared.

## Building

```sh
cargo build --workspace
cargo test --workspace
```

CI runs those two, and then:

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo build --workspace --benches
cargo run -p promql-sync -- check
```

CI builds the benchmarks but does not run them, so a bench that stops compiling
fails the build instead of rotting unnoticed. The `bench.yml` workflow runs them
on demand.

After those, CI re-blesses the promqltest allowlist and fails if the tree is
dirty, which is what makes each commit's recorded pass count trustworthy.

To see where the engine stands on the corpus, run the conformance suite
directly. It prints a per-file scoreboard and the missing-feature table on every
run, gated or not:

```sh
cargo test -p promql-conformance --test promqltest

# every eval as its own trial, failing as it really is
PROMQL_PROMQLTEST_ALL=1 cargo test -p promql-conformance --test promqltest

# narrowed to one file, or to the feature you are implementing
PROMQL_PROMQLTEST_ALL=1 cargo test -p promql-conformance --test promqltest -- operators/
PROMQL_PROMQLTEST_ALL=1 cargo test -p promql-conformance --test promqltest -- topk
```

`AGENTS.md` describes the five gates an engine feature moves through before it
is done.

## License

Apache-2.0, see [LICENSE](LICENSE). The vendored files under
`promql-parser/upstream/` and
`promql-conformance/testdata/prometheus/` are copied verbatim from
[`prometheus/prometheus`](https://github.com/prometheus/prometheus), also
Apache-2.0. Their upstream copyright headers are retained.
