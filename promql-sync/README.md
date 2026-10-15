# promql-sync

A developer-invoked CLI that keeps the vendored upstream files honest and
regenerates the parser's grammar from them. It never commits, branches or
opens a pull request: an upgrade is a worktree diff that you review.

It manages two vendored sets, each with its own `MANIFEST.toml` (SHA-256 per
file) and `UPSTREAM.md`:

| `--set` | Directory | Contents |
|---|---|---|
| `parser` | `promql-parser/upstream/` | The Go sources the parser ports: `generated_parser.y`, `lex.go`, `ast.go`, `parse.go`, `functions.go`, `printer.go`. |
| `promqltest` | `promql-conformance/testdata/prometheus/` | The `.test` corpus. |

## Commands

```sh
cargo run -p promql-sync -- check [--set parser|promqltest]
```

Verifies every vendored file against its manifest hash. It needs no network
and changes nothing, so CI runs it on every pull request. A hand-edited
vendored file fails by name.

```sh
cargo run -p promql-sync -- pull --set parser --target <sha-or-tag> [--dry-run] [--report <path>]
```

Fetches upstream at the target and compares it with the vendored files.
`--set` defaults to `parser`. Without `--dry-run` it overwrites them, their `MANIFEST.toml` and the set's
`sync-meta.toml`. It pulls one set per run, because each set needs different
follow-up work: re-pinning `promqltest` moves test content, so the
regeneration step in that set's `UPSTREAM.md` lands in the same change. The
report defaults to `target/promql-sync-report.md`.

```sh
cargo run -p promql-sync -- generate-grammar [--dry-run] [--out <path>]
```

Rewrites `promql-parser/src/grammar.y`. The structure (rules, alternatives,
precedence) comes from `upstream/generated_parser.y`. The Rust parts come from
two sidecars: `grammar-actions.toml` (per-rule return types and per-alternative
actions) and `grammar-tokens.toml`. An alternative without an action is
emitted as an `Err(())` placeholder, so the grammar still compiles, and the
tool lists every one at the end. `--dry-run` prints the grammar to stdout
instead of writing it.

## Source layout

| File | Role |
|---|---|
| `src/main.rs` | CLI, vendored-set definitions, `check` and `pull` |
| `src/yacc.rs` | Reads upstream's goyacc grammar |
| `src/sidecar.rs` | Reads `grammar-actions.toml` and `grammar-tokens.toml` |
| `src/emitter.rs` | Writes the grmtools `grammar.y` |

[`docs/parser-sync.md`](../docs/parser-sync.md) is the design: classification
of upstream changes, hashing, and resuming a sync that went red.
