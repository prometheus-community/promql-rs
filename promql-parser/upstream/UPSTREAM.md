# Upstream reference

The files in this directory are verbatim copies from the Prometheus
source tree. They are the source of truth for our parser port; when
upstream changes, we re-vendor them and the `promql-sync` tool classifies
every delta as auto-apply-safe or human-required.

## Provenance

- Repository: https://github.com/prometheus/prometheus
- Path: `promql/parser/`
- Commit: `ccdfcea89903f5966d3f6b73631f1b37f907dbeb`
- Vendored: 2026-11-27

## Files

| File | Role |
|---|---|
| `generated_parser.y` | goyacc grammar, source for our `src/grammar.y` |
| `lex.go`             | Hand-rolled state-machine lexer, source for our `src/lexer.rs` (custom `impl lrpar::NonStreamingLexer`; the current `src/lexer.l` is a placeholder) |
| `ast.go`             | AST node types, source for `src/ast.rs` |
| `parse.go`           | Parser entry points and actions, source for `src/parser.rs` and `src/actions.rs` |
| `functions.go`       | Built-in function registry, source for a future `src/functions.rs` |
| `printer.go`         | AST prettifier, reference for round-trip tests |
| `go.mod`             | Stub module so these Go files are excluded from the parent module's build. Not upstream. |
| `MANIFEST.toml`      | sha256 per vendored file, used by the sync tool to detect accidental edits. Not upstream. |

## Re-vendoring

Developer-invoked, not automated:

```sh
cd promql-parser/upstream
SHA=...  # target upstream commit
for f in generated_parser.y lex.go ast.go parse.go functions.go printer.go; do
  curl -sSL -o "$f" "https://raw.githubusercontent.com/prometheus/prometheus/${SHA}/promql/parser/${f}"
done
```

Then run `cargo run -p promql-sync -- pull --target "$SHA"` to diff the
new files against the pinned SHA and write a report of what needs
human translation.

## Licensing

Vendored files are licensed Apache-2.0, same as the Prometheus project.
Upstream copyright headers are retained verbatim.
