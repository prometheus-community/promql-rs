# promql-conformance

Replays Prometheus's own promqltest corpus against `promql-engine` and records
what passes. The corpus is the 20-odd `.test` files under
`testdata/prometheus/`, vendored from the commit pinned in `UPSTREAM.md`. Each
`eval` carries its expected values inline. No oracle, Go toolchain or network
is needed, so the suite runs in CI on every commit.

## How it fits

```text
testdata/prometheus/*.test
   │  prometheus::script        line scanner for load/eval/clear directives;
   │                            series lines go through promql_parser::parse_series_desc
   ▼
Script { commands: [Load, Eval, Clear, …] }
   │  prometheus::run           one Outcome per eval, against any result::Engine
   ▼
result::Engine  ◄── datafusion::DataFusionEngine
   │                  seeds a MemorySeriesSource from the load blocks,
   │                  runs promql_engine::Engine, maps errors
   ▼
Outcome / Verdict per eval
   │  prometheus::supported     the gate: SUPPORTED.toml (an allowlist)
   │  prometheus::report        the roadmap: UNSUPPORTED.md (scoreboard, missing features)
```

## Public interface

| Item | Use it to |
|---|---|
| `result::Engine` | The seam. `range_query(load, query, start, end, step)` and `instant_query(load, query, at)` return a `QueryResult`. It is synchronous because `libtest-mimic` trials are plain closures. |
| `DataFusionEngine` | The `Engine` backed by `promql-engine`. Its `SourceMode` (`PROMQL_CONFORMANCE_SOURCE=plain\|chunked[=ms]\|blocks[=ms]\|partitions[=n]`) replays the corpus through the store shapes a real store produces. The answers must not depend on the mode. |
| `QueryResult`, `Series`, `Sample`, `Point`, `Labels` | Engine-neutral results. |
| `EngineError` | `Unsupported` ("not built yet") is kept apart from `Other` ("built wrong"). |
| `prometheus::{load_corpus, Script, Command, Eval, Expect, …}` | The parsed corpus. |
| `prometheus::{run_script, Outcome, Verdict, almost_equal}` | Running a script and comparing values the way upstream does. |
| `prometheus::{Supported, Batch, Violation}` | Reading and checking `SUPPORTED.toml`. |
| `prometheus::{scoreboard, inventory_markdown, FileStats}` | Producing `UNSUPPORTED.md`. |

To run the corpus against another engine, implement `result::Engine` and pass
it to `run_script`.

## Running it

```sh
cargo test -p promql-conformance --test promqltest          # the CI gate + scoreboard
PROMQL_PROMQLTEST_ALL=1 cargo test -p promql-conformance --test promqltest            # every eval as a trial
PROMQL_PROMQLTEST_ALL=1 cargo test -p promql-conformance --test promqltest -- topk    # narrowed
PROMQL_CONFORMANCE_SOURCE=chunked cargo test -p promql-conformance --test promqltest  # one sample per chunk
PROMQL_PROMQLTEST_BLESS=1 cargo test -p promql-conformance --test promqltest          # rewrite SUPPORTED.toml + UNSUPPORTED.md
```

The gate is `SUPPORTED.toml`, not the pass count. CI turns red when:

- a listed eval stops passing;
- an unlisted eval starts passing (declare it by re-blessing);
- a listed eval no longer exists after a re-pin.

`UNSUPPORTED.md` is generated. It holds the pass counts by category (core,
native histograms, subqueries, flag-gated), the per-file scoreboard and the
missing features, each with the number of evals it blocks. Commit both files
with the code that moved them (see `AGENTS.md`, gate 2).

`tests/script.rs` checks that the line scanner reads every vendored file
before any engine is involved. A scanner bug then cannot pass for an engine
bug.
