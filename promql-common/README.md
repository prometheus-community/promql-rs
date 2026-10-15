# promql-common

A structural hand-port of the `model` package of
[`github.com/prometheus/common`](https://github.com/prometheus/common) v0.67.5,
the version the pinned Prometheus commit uses. Only what PromQL parsing needs
is ported, so this is a leaf crate shared by `promql-parser` and
`promql-engine`.

## Public interface

| Item | Upstream | Use it to |
|---|---|---|
| `model::Duration` | `model.Duration` | Hold a Prometheus duration. It wraps `chrono::TimeDelta`, but every constructor enforces Go's ±(2⁶³−1) ns bound, so `as_nanos_i64` is never lossy. |
| `Duration::from_str` / `parse` | `model.ParseDuration` | Parse `1h30m`, `5m`, `2y`. Units, ordering and overflow errors follow Go. `parse_allow_negative` and `parse_nanos` cover the other parsing variants. |
| `as_millis`, `as_secs_f64`, `as_nanos_i64`, `from_millis` | – | Convert to and from the engine's millisecond timestamps. |
| `Duration`'s `Display` | `Duration.String` | Print the way Prometheus does (`1h30m`, `0s`). |
| `model::ParseDurationError` | the `ParseDuration` errors | Report why a duration did not parse. |
| `model::secs_to_millis` | – | Convert float seconds from the AST to `i64` milliseconds; `None` when out of range. |

```rust
use promql_common::model::Duration;

let d: Duration = "1h30m".parse()?;
assert_eq!(d.to_string(), "1h30m");
```

Where Rust and Go disagree on an edge case, Go wins, with a comment at the
spot (`AGENTS.md`).
