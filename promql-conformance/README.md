# promql-conformance

Replays Prometheus's promqltest corpus against `promql-engine`. The `.test`
files in `testdata/prometheus/` contain queries and expected values from the
[vendored Prometheus revision](testdata/prometheus/UPSTREAM.md).
The suite needs no reference server, Go toolchain, or network access.

## Test pipeline

The modules process each script in this order:

1. `prometheus::script` reads directives such as `load`, `eval`, and `clear`.
   It passes series lines to `promql_parser::parse_series_desc`.
2. `prometheus::run` evaluates the script through `result::Engine` and records
   one `Outcome` per eval.
3. `prometheus::supported` compares outcomes with `SUPPORTED.toml`.
4. `prometheus::report` generates the counts and unsupported-feature report
   in `UNSUPPORTED.md`.

`tests/script.rs` checks the script parser against every vendored file without
an engine. Script parse failures are distinct from evaluation failures.

## Public interface

The crate exposes an engine adapter interface and corpus utilities:

| Item | Contract |
|---|---|
| `result::Engine` | Synchronous `range_query` and `instant_query` methods returning `QueryResult`. Synchronous methods match the `libtest-mimic` trial closures. |
| `DataFusionEngine` | Adapter backed by `promql-engine`. Builds a `MemorySeriesSource` from each script's loaded data. |
| `QueryResult`, `Series`, `Sample`, `Point`, `Labels` | Engine-independent result types. |
| `EngineError` | Distinguishes `Unsupported` features from `Other` errors. |
| `prometheus::{load_corpus, Script, Command, Eval, Expect}` | Corpus loading and parsed script types. |
| `prometheus::{run_script, Outcome, Verdict, almost_equal}` | Script execution and upstream-compatible value comparison. `run_script` accepts any implementation of `result::Engine`. |
| `prometheus::{Supported, Batch, Violation}` | Loading and validation of `SUPPORTED.toml`. |
| `prometheus::{scoreboard, inventory_markdown, FileStats}` | Per-file results and generated report data. |

## Test commands

The default command checks the allowlist and prints per-file results and
unsupported features:

```sh
cargo test -p promql-conformance --test promqltest
```

`PROMQL_PROMQLTEST_ALL=1` exposes every eval as a trial, including unsupported
queries. A trial filter selects a file or feature:

```sh
PROMQL_PROMQLTEST_ALL=1 cargo test -p promql-conformance --test promqltest
PROMQL_PROMQLTEST_ALL=1 cargo test -p promql-conformance --test promqltest -- operators/
PROMQL_PROMQLTEST_ALL=1 cargo test -p promql-conformance --test promqltest -- topk
```

`PROMQL_CONFORMANCE_SOURCE` selects the input shape. Accepted values are
`plain`, `chunked[=ms]`, `blocks[=ms]`, and `partitions[=n]`. Results must not
depend on the source mode. For example, this command uses one sample per chunk:

```sh
PROMQL_CONFORMANCE_SOURCE=chunked cargo test -p promql-conformance --test promqltest
```

## Allowlist and report

[`SUPPORTED.toml`](testdata/prometheus/SUPPORTED.toml) determines whether CI
passes, not the pass count. The suite fails when any of these conditions holds:

- A listed eval fails.
- An unlisted eval passes.
- A listed eval no longer exists after an upstream revision change.

[`UNSUPPORTED.md`](testdata/prometheus/UNSUPPORTED.md) contains generated pass
counts by category, per-file results, and unsupported features with the number
of evals each one blocks. This command regenerates both files:

```sh
PROMQL_PROMQLTEST_BLESS=1 cargo test -p promql-conformance --test promqltest
```

The [conformance requirement](../AGENTS.md) requires both generated files in
the same change as the code that changes the results.

## Comparison limits

The [test documentation](tests/promqltest.rs) records the checks the suite
performs and the annotation expectations it does not assert. A passing eval
establishes only the behavior those checks cover.
