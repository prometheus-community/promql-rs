# Feature flags

Prometheus gates its experimental PromQL syntax behind four parser options,
each set by one `--enable-feature` value (`cmd/prometheus/main.go`) and all
off by default, so a stock Prometheus rejects `mad_over_time(x[5m])` with
`function "mad_over_time" is not enabled`. promql-rs mirrors them. The spec is
`parser.Options` in `promql/parser/parse.go` at the commit pinned in
`promql-conformance/testdata/prometheus/UPSTREAM.md`.

| `--enable-feature` | upstream `parser.Options` | `promql_parser::ParserOptions` | default | gates |
|---|---|---|---|---|
| `promql-experimental-functions` | `EnableExperimentalFunctions` | `enable_experimental_functions` | off | Functions marked `Experimental: true` in `functions.go` (`mad_over_time`, `sort_by_label`, `info`, `first_over_time`, `ts_of_*_over_time`, `double_exponential_smoothing`, `histogram_quantiles`, `start`, `end`, `step`, `range`), and the `limitk` and `limit_ratio` aggregators |
| `promql-duration-expr` | `ExperimentalDurationExpr` | `experimental_duration_expr` | off | Arithmetic, `step()`, `range()` and `min`/`max` in durations. The option and its helper exist; the grammar does not use them until the duration productions are ported |
| `promql-extended-range-selectors` | `EnableExtendedRangeSelectors` | `enable_extended_range_selectors` | off | The `anchored` and `smoothed` selector modifiers |
| `promql-binop-fill-modifiers` | `EnableBinopFillModifiers` | `enable_binop_fill_modifiers` | off | `fill`, `fill_left` and `fill_right` on binary operators |

`ParserOptions::all()` turns every gate on, as upstream's promqltest does
(`TestParserOpts` in `promql/promqltest/test.go`). The conformance harness uses
it, which is why the corpus passes with the same count either way.

## Where each gate is checked

Gating is in the grammar action or its helper, where upstream checks it, so the
message and the position match. Every error is accumulated, not fatal, and the
node is still built, as with `addParseErrf`.

| gate | promql-rs (`promql-parser/src`) | upstream | message, at |
|---|---|---|---|
| functions | `actions::function_call`, `function_call_at_modifier`, `function_call_keyword` through `check_function_enabled` | the four `function_call` arms in `generated_parser.y` | `function "<name>" is not enabled`, at the name token |
| aggregators | `actions::aggregate` | `newAggregateExpr` | `<op>() is experimental and must be enabled with --enable-feature=promql-experimental-functions`, at the aggregation |
| fill | `actions::binary` | `newBinaryExpression` | `binop fill modifiers are experimental and not enabled`, at the binary expression |
| anchored, smoothed | `actions::set_anchored`, `set_smoothed` | `setAnchored`, `setSmoothed` | `<name> modifier is experimental and not enabled`, at the expression the modifier follows |
| duration | `ParserCtx::experimental_duration_expr` | `experimentalDurationExpr` | `experimental duration expression is not enabled`, at the expression |

The `start()`, `end()`, `step()` and `range()` arms are gated but still not
ported past the gate: with the flag on they fail as before.

Every gate error carries its flag in `ParseError::gate`, so a caller sorting
queries by the flag they need does not match on upstream's wording.

`FEATURE_FLAGS` in `promql-parser/src/options.rs` is the code's copy of the
first three columns.

## Conformance

Experimental syntax is not part of the core language yet, so the promqltest
harness leaves it out of the count. An eval whose query stock Prometheus rejects
but parses with every gate open is `Verdict::Gated`, naming the flags it needs
(a flag is needed when closing just that gate stops the query parsing). It is in
no total, no per-file ratio, no trial and not in `SUPPORTED.toml`;
`UNSUPPORTED.md` lists how many there are per flag. The harness still evaluates
the query with every gate open, upstream's `TestParserOpts`, so nothing else
changes. A feature leaves the exclusion by being removed from the gated set,
not by a flag flip: when upstream makes a feature stable it parses with defaults
and its evals count again.

## Using them

- `promql_parser::parse_expr` and the other free functions parse with every gate
  off, like upstream's package-level `ParseExpr`.
- `promql_parser::Parser::new(options)` is upstream's `NewParser(opts)`.
- `promql_engine::EngineOptions { parser, .. }` carries the `ParserOptions`;
  `Engine::with_options` and `Engine::blocking_with_options` build an engine
  under it, and every query path parses with it. `Engine::new` and
  `Engine::blocking` use `EngineOptions::default()`, a stock Prometheus.

## Not a parser flag

Delayed name removal (`--enable-feature=promql-delayed-name-removal`,
`EnableDelayedNameRemoval`) is an engine option in upstream's `EngineOpts`, not
a `parser.Options` field. It is not in this table; it is handled separately, as
a field of `EngineOptions`.
