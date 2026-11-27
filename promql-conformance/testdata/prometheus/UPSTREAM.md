# Upstream reference

The `.test` files in this directory are verbatim copies from the
Prometheus source tree. They are the specification our engine is held
to: each one carries its own expected values inline, so running them
needs no oracle, no Go toolchain and no network.

Nothing here is ours to edit. `promql-sync` is the only thing that
writes to this directory, and CI verifies every file against
`MANIFEST.toml` on each run.

## Provenance

- Repository: https://github.com/prometheus/prometheus
- Path: `promql/promqltest/testdata/`
- Commit: `ccdfcea89903f5966d3f6b73631f1b37f907dbeb`
- Vendored: 2026-11-27

The commit is deliberately the same one `promql-parser/sync-meta.toml`
pins. The two sets are independent — this corpus carries its own
expected values and never consults the parser's vendored Go — but there
is no reason to track two Prometheus revisions until one of them needs
to move.

## Files

23 files, 2,552 `eval` assertions.

| File | Evals |
|---|---:|
| `native_histograms.test` | 557 |
| `functions.test` | 541 |
| `operators.test` | 214 |
| `histograms.test` | 195 |
| `extended_vectors.test` | 174 |
| `aggregators.test` | 167 |
| `bitwise.test` | 87 |
| `at_modifier.test` | 78 |
| `info.test` | 70 |
| `duration_expression.test` | 65 |
| `type_and_unit.test` | 65 |
| `fill-modifier.test` | 50 |
| `subquery.test` | 41 |
| `start_timestamps.test` | 40 |
| `limit.test` | 37 |
| `selectors.test` | 31 |
| `name_label_dropping.test` | 30 |
| `unquoted_utf8_names.test` | 27 |
| `literals.test` | 25 |
| `trig_functions.test` | 21 |
| `range_queries.test` | 18 |
| `staleness.test` | 17 |
| `collision.test` | 2 |

`MANIFEST.toml` holds a sha256 per file and is maintained by
`promql-sync`. Not upstream.

## The format

Upstream parses these files in two layers, and we already own the
second one:

1. A hand-written line scanner in `promql/promqltest/test.go` — split on
   newlines, trim each line, drop whole-line `#` comments, dispatch on
   the first token (`load` / `eval` / `clear`), and six regexes
   (`test.go:51-57`) for the directives. There is no grammar file.
2. `parser.ParseSeriesDesc` (`test.go:513`) for every series line in a
   `load` block *and* every expected-value row under an `eval` — that is
   the real goyacc grammar, entered at `START_SERIES_DESCRIPTION`. Our
   equivalent is `promql_parser::parse_series_desc`.

All timestamps are offsets from the Unix epoch, parsed with
`model.ParseDuration` (non-negative, integer per unit, units in strictly
decreasing order, bare `0` accepted).

## Re-vendoring

```sh
cargo run -p promql-sync -- pull --set promqltest --target <sha>
```

This rewrites the `.test` files, `MANIFEST.toml` and the pinned SHA in
`promql-conformance/sync-meta.toml`. Re-pinning moves test content, so
`SUPPORTED.toml` and `UNSUPPORTED.md` must be regenerated in the same
change:

```sh
PROMQL_PROMQLTEST_BLESS=1 cargo test -p promql-conformance --test promqltest
```

**Read the `SUPPORTED.toml` diff before committing it.** Every removed
line is coverage that used to exist and no longer does. A re-pin that
renames or reflows a query moves its case id, so the eval it covered
silently stops being gated — the suite reports that as an orphan first,
and re-blessing is what makes it permanent. That review is the entire
reason the gate is an allowlist.

If upstream adds or removes a file, update the `promqltest` entry in
`VENDOR_SETS` (`promql-sync/src/main.rs`); the list is explicit on
purpose, so a new upstream file is a deliberate decision rather than a
silent addition.

## Licensing

Vendored files are licensed Apache-2.0, same as the Prometheus project.
