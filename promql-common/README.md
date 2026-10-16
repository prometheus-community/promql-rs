# promql-common

A port of the duration types and functions from
[`github.com/prometheus/common/model`](https://github.com/prometheus/common).
The [port's module documentation](src/model/time.rs) records its upstream version.
`promql-parser` and `promql-engine` share this crate.

## Public interface

The `model` module exports these types and functions:

| Item | Upstream | Behavior |
|---|---|---|
| `model::Duration` | `model.Duration` | Wraps `chrono::TimeDelta`. Constructors enforce Go's ±(2⁶³−1) ns bound, so `as_nanos_i64` is lossless. |
| `Duration::from_str`, `Duration::parse` | `model.ParseDuration` | Parse durations such as `1h30m`, `5m`, and `2y`. Units, unit order, and overflow errors follow Go. |
| `Duration::parse_allow_negative` | `model.ParseDurationAllowNegative` | Accepts negative durations as well as the `parse` grammar. |
| `Duration::parse_nanos` | Extended parsing | Adds the sub-millisecond units `us`, `µs`, `μs`, and `ns` to the `parse` grammar. |
| `Duration::as_millis`, `Duration::as_secs_f64`, `Duration::as_nanos_i64` | No direct equivalent | Return the duration in milliseconds, float seconds, or nanoseconds. |
| `Duration::from_millis` | No direct equivalent | Constructs a duration from milliseconds. Returns an error outside Go's duration range. |
| `Duration`'s `Display` | `Duration.String` | Formats durations as Prometheus does, for example `1h30m` or `0s`. |
| `model::ParseDurationError` | `ParseDuration` errors | Describes invalid units, unit order, and overflow. |
| `model::secs_to_millis` | No direct equivalent | Converts float seconds to `i64` milliseconds. Returns `None` outside the supported range. |

For example, parsing and formatting preserve this duration:

```rust
use promql_common::model::Duration;

let d: Duration = "1h30m".parse()?;
assert_eq!(d.to_string(), "1h30m");
```

The port follows Go's behavior where Rust and Go differ. Comments at those
cases record the reason, as required by the [porting rules](../AGENTS.md).
