# Parser and upstream sync

This document pins the PromQL parser implementation strategy and the
mechanism for staying current with upstream Prometheus.

> [!NOTE]
> This describes the full design, including parts that are still
> aspirational. `promql-sync` currently implements `check`, `pull`,
> and `generate-grammar` (a byte-level diff plus report). The
> structural Green/Yellow/Red classification and automatic apply
> pipeline described under "Sync mechanism" and "Classification" is
> the target design, not yet built — see the code comments in
> `promql-sync/src/main.rs` for the current behavior.

## Goal

Hand-port the PromQL parser from upstream (`prometheus/prometheus@<sha>`)
into a standalone Rust crate, and provide tooling that makes resyncing
against new upstream revisions cheap. The sync tool is a plain Unix
filter: it fetches upstream, classifies each change as auto-apply-safe
or human-required, applies the safe subset directly to the developer's
current worktree, and writes a report alongside. It does not commit,
branch, push, or open PRs. The developer reviews the resulting diff
with `git diff`, runs tests locally, and takes it from there.

## Non-goals

- Reusing a community Rust port. Evaluated and rejected in favour of a
  port that tracks upstream directly, with tooling that makes the
  tracking itself cheap rather than a one-off effort.
- FFI to upstream's Go parser. Evaluated and rejected on operational
  grounds.
- Automatic scheduled sync. Sync is manually triggered by a developer.

## Parser generator

grmtools (`cfgrammar` + `lrpar` + `lrlex`). Chosen because its `.y`
grammar syntax is the closest Rust analog to goyacc, so our grammar
file reads like upstream's with actions translated to Rust. LALRPOP was
evaluated first and rejected because its grammar syntax diverges too
far from yacc to keep maintenance ergonomic.

Lexer: custom `impl lrpar::Lexer` structurally ported from upstream
`lex.go`. PromQL's lexing is context-sensitive (brace-depth state,
duration-after-number detection, keyword-vs-identifier mode shifts)
in ways that regex-driven `lrlex` cannot express without contortion.

## Repository layout

```
promql-parser/
├── Cargo.toml
├── README.md
├── build.rs                             # grmtools CTParserBuilder
├── upstream/
│   ├── UPSTREAM.md                      # provenance, pinned SHA, last-sync date
│   ├── MANIFEST.toml                    # per-file SHA256 for drift detection
│   ├── generated_parser.y               # verbatim vendored
│   ├── lex.go
│   ├── ast.go
│   ├── parse.go
│   └── functions.go
├── src/
│   ├── lib.rs                           # public API, lrpar_mod! invocation
│   ├── grammar.y                        # our translated grammar
│   ├── ast.rs
│   ├── actions.rs                       # every non-trivial grammar action body
│   ├── lexer.rs                         # impl lrpar::Lexer, mirrors lex.go
│   ├── tok.rs                           # Item / ItemType
│   ├── functions.rs                     # which functions are experimental (signatures: promql-engine)
│   ├── posrange.rs
│   ├── error.rs
│   ├── options.rs                       # ParserOptions (experimental gates)
│   └── context.rs                       # ParserCtx (options + error accumulator)
├── tests/
│   ├── smoke.rs
│   ├── conformance.rs                   # runs the generated corpus
│   └── fixtures/
│       └── parse_test_corpus.json       # produced by scripts/gen-conformance
└── sync-meta.toml                       # sync-tool state (see below)

promql-sync/                              # developer-facing sync tool
├── Cargo.toml
└── src/
    ├── main.rs
    ├── fetch.rs                         # download upstream files at target SHA
    ├── diff.rs                          # structural diff vs pinned SHA
    ├── classify.rs                      # Green / Yellow / Red classification
    ├── apply/                           # mechanical appliers
    │   ├── tokens.rs                    # ItemType + grammar %token block
    │   ├── ast_fields.rs                # new primitive fields on existing structs
    │   ├── keywords.rs                  # lex.go `key` table entries
    │   └── functions.rs                 # functions.go registry entries
    ├── hashes.rs                        # action-body hashing, whitespace-normalized
    └── report.rs                        # Markdown + JSON report emitters

scripts/
└── gen-conformance/                     # one-time Go tool, run on demand
    ├── go.mod
    └── main.go                          # reads promql/parser/parse_test.go, emits JSON
```

## Translation discipline

**Grammar file stays structural-one-to-one with upstream.** Non-terminal
names, rule ordering, `%token` / `%left` / `%right` / `%nonassoc`
declarations are preserved verbatim. Only action bodies are rewritten.

**Actions are one-line calls into `actions.rs`.** Comparison, upstream:

```
aggregate_expr : aggregate_op aggregate_modifier function_call_body
                 { $$ = yylex.(*parser).newAggregateExpr($1, $2, $3, false) }
```

Our translation:

```
AggregateExpr -> Expr :
    AggregateOp AggregateModifier FunctionCallBody
        { actions::new_aggregate_expr($lexer, $1, $2, $3, false) }
    ;
```

Every action delegates to a named helper in `actions.rs`. Helper names
mirror upstream (`newAggregateExpr` → `new_aggregate_expr`), as do
parameter orders. The only systemic rename is `yylex.(*parser)` →
`$lexer` (grmtools' idiomatic parser-context accessor), which the
sync tool recognizes as structural noise when diffing.

**Error accumulation matches upstream.** Upstream's parser holds a slice
of errors and keeps parsing after each fault. Our `ParserCtx` does the
same; the parse result is `(Expr, Vec<ParseError>)`.

**Experimental-feature gating is in-action, not post-parse.** Upstream
checks `yylex.(*parser).options.EnableExperimentalFunctions` inside the
action and calls `addParseErrf` when disabled. We mirror this exactly so
our error messages and their positions match upstream — necessary for
conformance.

`ParserOptions` (`src/options.rs`) is `parser.Options` field for field, all
off by default; `Parser::new(opts)` is `NewParser`, and the free `parse_*`
functions use the zero options like upstream's package-level ones.
grmtools gives an action only its own symbols and the lexer, so what upstream
reaches as `yylex.(*parser)` travels as the grammar's `%parse-param`, named `p`:
a `ParserCtx` holding the options and the `addParseErrf` accumulator. The
sidecar's `parse_param` key makes `generate-grammar` emit the declaration, and
actions that gate take `p` as their second argument. `docs/feature-flags.md`
lists each gate against the upstream check it ports.

## Sync mechanism

Developer-invoked. No cron, no scheduled workflow, no branch or PR
automation. The tool behaves like a formatter or a code-generator: it
mutates files in the current worktree and exits. The developer reviews
the resulting diff with `git diff`, runs tests, and decides what to
commit.

```
$ cargo run -p promql-sync -- pull --target v3.2.0
```

Pipeline: fetch upstream at `--target`, diff against the pinned SHA
recorded in `sync-meta.toml`, classify every change as Green, Yellow,
or Red, apply Green+Yellow mutations to the current worktree, write
the report to `target/promql-sync-report.md` (+ JSON sidecar), exit.

### Classification

**Green — apply unconditionally.**

- New `%token` declaration in `generated_parser.y`. Applier adds a
  variant to `ItemType`, adds an entry to the grammar's extern token
  block, adds a display string.
- New entry in `lex.go`'s keyword table (`key` map). Applier adds to our
  keyword → `ItemType` map.
- New entry in `functions.go`'s function registry. Applier appends a
  record to our function table preserving name, arg types, return type,
  and variadic/experimental flags.
- New primitive field on an existing AST struct (`bool`, `int64`,
  `float64`, `string`, `*T` where `T` is already translated, or
  well-known container types). Applier adds a mirror field to the Rust
  struct, defaulted via `Default::default()`.
- Comment, whitespace, and copyright-header changes. Applier ignores.

**Yellow — apply with a follow-up marker, still safe.**

- New AST struct consisting only of primitive fields plus standard
  methods (`String`, `PositionRange`, `Type`, `PromQLExpr`). Applier
  generates a Rust type with matching fields, stubs the methods with
  `todo!()`. Build still passes because nothing references it yet. A
  Yellow report item tells the dev to flesh out the methods.
- New experimental keyword (recognised by upstream's `isExperimental`
  marker on function or feature). Applier adds the token and keyword
  but wires the grammar rule behind a `ParserOptions` flag that defaults
  off. User-visible surface unchanged until the flag is flipped.

**Red — refuse to apply, emit report.**

- New production rule in `generated_parser.y`. Action translation is
  human work.
- Modified action body on an existing production (detected via
  action-body hash mismatch). Semantic drift — the existing translation
  may no longer match upstream's behaviour.
- New `lex.go` state function, or materially changed state transition.
  Detected via structural diff of `lexStateFn`-named symbols.
- Precedence declaration change (new `%left` / `%right` / `%nonassoc`
  line, or reordering). Changes the grammar's ambiguity resolution.
- AST struct method with non-trivial body. Detected via simple
  heuristics (function body longer than N tokens, or containing a
  control-flow keyword).
- Anything the structural recognizers don't understand. Default is
  Red. False positives are fine — a false Red produces a noisier-than-
  necessary report, not a broken build.

### Apply and report

After classification the tool:

1. Applies all Green and Yellow changes directly to files in the
   current worktree (updating `upstream/*` vendored files,
   `promql-parser/src/tok.rs`, `ast.rs`, `functions.rs`, the
   `%token` block in `promql-parser/src/grammar.y`, etc.).
2. Updates `sync-meta.toml` with new `pinned_upstream_sha`, refreshed
   file hashes, and any new Green/Yellow entries.
3. Writes the report:
   - `target/promql-sync-report.md` — human-readable summary of what
     was applied and what the developer still needs to translate.
   - `target/promql-sync-report.json` — machine-readable equivalent
     (item list, classification, hashes, upstream line references).
4. Exits 0 if classification and apply completed (even when Red items
   exist), nonzero only on fetch/parse errors.

The tool runs no tests, produces no commits, creates no branches, and
opens no PRs. The developer inspects `git diff`, builds, runs
`cargo test -p promql-parser`, iterates on Red items if any, and
commits when satisfied. That workflow is the same whether the result
is all-Green (clean bump) or includes Red items (incremental work over
several developer sessions).

If a developer wants to abort a sync cleanly, `git restore .` and
`git clean -f` in the relevant directories return to the pre-sync
state — standard Unix-tool semantics.

### Resuming a red-blocked sync

Red items are resolved by the developer writing Rust translations in
`promql-parser/src/` and recording the new action-body hash in
`sync-meta.toml`. Re-running the sync tool recomputes the upstream
hash; if it matches the recorded translation hash, the item is
reclassified as resolved and the corresponding entry moves out of the
Red bucket on the next report. When all Red items are resolved, the
report simply shows a clean sync — no "propose" step exists because
there is no proposal model.

## `sync-meta.toml`

Machine-readable state that lives with the repo.

```toml
pinned_upstream_sha = "83962c35a4ab0c9988bc469aa8165014fc065d34"
last_sync_date = "2026-04-21"

[file_hashes]
"upstream/generated_parser.y" = "sha256:..."
"upstream/lex.go"             = "sha256:..."
"upstream/ast.go"             = "sha256:..."
"upstream/functions.go"       = "sha256:..."

[translations.productions]
"aggregate_expr.rule_0" = { action_body_hash = "sha256:..." }
"aggregate_expr.rule_1" = { action_body_hash = "sha256:..." }
"vector_selector.rule_0" = { action_body_hash = "sha256:..." }
# … one entry per translated grammar rule

[translations.ast_types]
"VectorSelector"  = { field_set_hash = "sha256:...", method_set_hash = "sha256:..." }
"BinaryExpr"      = { field_set_hash = "sha256:...", method_set_hash = "sha256:..." }

[translations.lexer_states]
"lexStatements"       = { body_hash = "sha256:..." }
"lexInsideBraces"     = { body_hash = "sha256:..." }
"lexNumberOrDuration" = { body_hash = "sha256:..." }
```

The sync tool reads this, compares against upstream-target hashes, and
emits one entry of classification per entry here. Drift is detected by
hash mismatch even when structural layout is unchanged.

## Hashing

Action-body hash is SHA-256 over the action body text with this
normalization:

- Strip leading/trailing whitespace.
- Collapse runs of whitespace to a single space.
- Strip line-end comments (`//` through end-of-line).
- Preserve token ordering and quote content verbatim.

This catches real semantic changes and ignores upstream formatting
tweaks (indentation changes, blank-line insertions, comment edits).

Struct field-set hash is SHA-256 over the sorted list of
`name: type` pairs from the Go struct. Method-set hash is similar.

## Conformance corpus

`scripts/gen-conformance/` is a small Go program that:

1. Parses upstream `promql/parser/parse_test.go` using `go/ast`.
2. Walks the `testCases` table literal.
3. Emits `promql-parser/tests/fixtures/parse_test_corpus.json`,
   one object per case with `input`, `fail`, and when success is
   expected, a structurally-serialized `expected_ast`.

Run on demand (`go run ./scripts/gen-conformance`) when the sync tool
reports upstream `parse_test.go` has changed. The tool itself is part
of what the sync workflow surfaces as a report item when relevant.

The Go tool is stable — it consumes upstream's test-case struct, which
upstream has kept structurally unchanged for years. Maintenance on the
Go tool is effectively zero.

## CI

No scheduled sync job. No CI involvement in the sync workflow itself —
the tool is developer-local and produces ordinary file changes that
travel through the normal PR/review pipeline.

Continuous CI on every PR and on main runs:

- `cargo test -p promql-parser`
- `cargo test -p promql-parser --test conformance`
- `cargo run -p promql-sync -- check` (read-only): validates
  `sync-meta.toml` integrity and that vendored-file hashes match
  `MANIFEST.toml`. Catches accidental manual edits to vendored files,
  which would otherwise silently invalidate classification on the next
  real sync. If this check fails, the PR fails, and a developer runs
  the sync tool (or manually fixes the discrepancy) before re-pushing.

The "don't break main" property comes from the same mechanism that
protects any other code change: tests in CI, human code review, and
branch protection on main. The sync tool is not special in the review
pipeline.

## Risks

**1. Upstream restructures files.** Our structural recognizers assume
upstream's existing conventions in `ast.go`, `lex.go`, `functions.go`.
If upstream reshapes these files materially, the recognizers bail and
everything becomes Red — one large human sync. Mitigation: we take the
hit manually that one time; recognizers get updated to match the new
conventions. Low-frequency risk.

**2. Action-body hash false positives.** A trivial upstream refactor
(renaming a local variable inside an action) triggers Red even though
semantics are unchanged. Mitigation: the developer inspects the diff,
confirms it's a rename, updates the recorded hash, moves on. Few minutes
of work per false positive.

**3. Action-body hash false negatives.** An upstream change that is
semantically significant but produces the same whitespace-normalized
body could slip through as Green. Extremely unlikely given how upstream
authors write Go — but if it happens, the conformance suite catches it
on the developer's local test run and, failing that, in CI on the
resulting PR. Defense in depth.

**4. grmtools-upstream behaviour divergence.** grmtools' LALR(1) table
may resolve shift/reduce conflicts differently than goyacc in edge
cases. We catch these by running the upstream conformance corpus on
every build. Mitigation is to annotate our grammar with `%prec` where
needed to force grmtools' behaviour to match.

**5. Maintainer bandwidth on red items.** A flurry of upstream changes
produces a large report. Mitigation: the tool applies Green and Yellow
work autonomously; Red items are the only human load, and they're
genuinely novel translation work that would exist under any maintenance
model.

**6. Dirty worktree when sync runs.** The tool mutates files in the
developer's current worktree. If the developer had uncommitted changes
in `promql-parser/` before running, the sync mutations are
interleaved with their own work. Mitigation: the tool refuses to run
when `git status` reports uncommitted changes in any path it intends
to mutate, unless `--force-dirty` is passed. Keeps the common case
safe without adding ceremony to the intentional-override case.

## Done criteria for the parser and sync tooling

- `cargo test -p promql-parser` green.
- Upstream `parse_test.go` corpus ≥95% pass.
- `cargo run -p promql-sync -- check` wired into CI.
- At least one end-to-end sync run by a developer against a tagged
  upstream release that isn't the pinned release — tool correctly
  categorizes changes, mutates the worktree with Green/Yellow applies,
  writes a report, the developer reviews `git diff` and
  `target/promql-sync-report.md`, runs tests, and commits.

## Parse modes

Upstream serves several entry points from one grammar. goyacc allows a
single `%start`, so `parse.go` selects among them by *injecting* a
`START_*` pseudo-token at the head of the token stream (`InjectItem` /
`parseGenerated`), and the `start` rule dispatches on it. We mirror
that: `%start start`, the same four alternatives, and an
`InjectingLexer` in `src/parser.rs` that prepends the lexeme. The
pseudo-tokens have no lexer rule, so `build.rs` sets
`allow_missing_terms_in_lexer`.

Keeping upstream's shape here is what makes `metric` and `label_set`
shared between expressions and series descriptions rather than
duplicated into a second grammar.

Upstream's `EOF` token has no counterpart on our side — grmtools
handles end-of-input implicitly — so the sidecar's `skip_tokens` list
drops the alternatives that reference it.

### Series descriptions

The `series_*` rules are generated from upstream like every other rule,
the native-histogram alternatives of `series_item` and the
`histogram_*`, `bucket_set*` and `counter_reset_hint` rules behind them
included. A descriptor's keys are collected into `actions::HistogramDesc`
where upstream uses a `map[string]any`, and
`lastHistogramCounterResetHintSet`, parser state upstream, travels with
the built histogram as `actions::DescribedHistogram`.

The lexer side is a known divergence. Upstream lexes series
descriptions with the *same* lexer under a `seriesDesc bool`
(`upstream/lex.go`), which makes SPACE significant, turns `x`/`_` into
TIMES/BLANK, and disables hex literals. lrlex always begins in start
state 0 and offers the caller no way to change it, so the mode cannot
be a flag and currently lives in a second lexer file, `src/series.l`,
whose start conditions mirror upstream's state functions:

| `series.l` | `upstream/lex.go`  |
|------------|--------------------|
| `INITIAL`  | `lexStatements`    |
| `BRACES`   | `lexInsideBraces`  |
| `SERIES`   | `lexValueSequence` |
| `HIST`     | `lexHistogram`, `lexHistogramDescriptor` |
| `BUCKETS`  | `lexBuckets`       |

Because lrlex requires rule names to be unique per file, tokens
produced from more than one start condition are spelled differently in
`series.l` and aliased back onto the real token ids in `build.rs`.

See the follow-up below: finishing `src/lexer.rs` as a custom
`lrpar::Lexer` collapses the two files, the aliases and the start-state
limitation back into upstream's single-lexer-with-a-flag shape.

## Open follow-ups

- Wire `src/lexer.rs` — the hand-written state machine that already
  mirrors `lex.go` — into grmtools as a custom `lrpar::Lexer`, replacing
  `src/lexer.l` and `src/series.l`. This is the layout this document
  already describes, and series descriptions are the first feature that
  forces the question: modal lexing is exactly what a regex/DFA lexer
  cannot express, and the workarounds it needs (a second file, token-id
  aliases) get more expensive every time upstream touches `lex.go`.
- Cross-check native-histogram descriptors (`{{schema:1 …}}`) against
  Go's `parser.ParseSeriesDesc`, as the 787 float lines of upstream's
  `promqltest/testdata` were. The 228 histogram lines parse and the
  conformance corpus evaluates over them, but their values are checked
  by hand-written tests, not against upstream's parser output.
- Extend structural recognizers with per-release "known-quirks" entries
  (e.g. upstream-v3.5 reshuffled duration expression rules; recognizer
  knows to treat that transition specially). Only needed if we see the
  recognizer producing persistent false positives on a specific version
  bump.
