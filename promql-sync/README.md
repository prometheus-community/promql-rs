# promql-sync

A CLI that checks vendored Prometheus files, fetches upstream revisions, and
generates the Rust parser grammar. It changes files in the worktree but never
creates commits, branches, or pull requests.

## Vendored sets

Each set has a `MANIFEST.toml` with per-file SHA-256 hashes and an `UPSTREAM.md`
with provenance:

| `--set` | Directory | Contents |
|---|---|---|
| `parser` | `promql-parser/upstream/` | Go parser sources listed in the [parser provenance](../promql-parser/upstream/UPSTREAM.md). |
| `promqltest` | `promql-conformance/testdata/prometheus/` | The `.test` corpus. |

## Command reference

Commands run from the repository root by default. The global `--root <path>`
option selects another repository directory.

### `check`

```sh
cargo run -p promql-sync -- check [--set parser|promqltest]
```

Verifies each vendored file against its manifest hash. Without `--set`, it
checks all sets. It needs no network access and changes no files. A hash
mismatch reports the file name and fails the check. CI runs this command on
every pull request.

### `pull`

```sh
cargo run -p promql-sync -- pull --set parser --target <sha-or-tag> [--dry-run] [--report <path>]
```

Fetches upstream files at the target revision and compares them with the
vendored files. Each run selects one set. `--set` defaults to `parser`.
Without `--dry-run`, the command overwrites the vendored files,
`MANIFEST.toml`, and the set's `sync-meta.toml`.

The report path defaults to `target/promql-sync-report.md`. With `--dry-run`,
the command still writes the report but leaves the vendored files, manifest,
and revision metadata unchanged.

A `promqltest` revision change also requires regeneration of `SUPPORTED.toml`
and `UNSUPPORTED.md`. The
[corpus upgrade procedure](../promql-conformance/testdata/prometheus/UPSTREAM.md#re-vendoring)
describes the required checks.

### `generate-grammar`

```sh
cargo run -p promql-sync -- generate-grammar [--dry-run] [--out <path>]
```

Generates `promql-parser/src/grammar.y`. The vendored
`upstream/generated_parser.y` supplies rules, alternatives, and precedence.
`grammar-actions.toml` supplies return types and Rust actions.
`grammar-tokens.toml` supplies token mappings.

An alternative without an action produces an `Err(())` placeholder, so the
grammar still compiles. The command lists missing actions and their count.
`--dry-run` prints the grammar to stdout without writing it. `--out` changes
the output path.

The [parser sync design](../docs/parser-sync.md) describes upstream change
classification, hashing, and the procedure for an upgrade that fails checks.
