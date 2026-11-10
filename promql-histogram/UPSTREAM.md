# Upstream sources

Histogram behavior in this crate is derived from Prometheus at commit
`83962c35a4ab0c9988bc469aa8165014fc065d34`, the same specification as
[`promql-conformance`](../promql-conformance/testdata/prometheus/UPSTREAM.md):

- `model/histogram/generic.go`
- `model/histogram/histogram.go`
- `model/histogram/float_histogram.go`
- `promql/functions.go`
- `promql/quantile.go`
- `promql/quantile_test.go`
- `promql/promqltest/testdata/histograms.test`
- `promql/promqltest/testdata/native_histograms.test`
- `util/almost/almost.go`
- `util/kahansum/kahansum.go`
- `util/jsonutil/marshal.go`

## Port map

| Rust | Prometheus |
|---|---|
| `FloatHistogram::{equals, mul, div, add, sub, detect_reset, copy_to_schema}` | `FloatHistogram.{Equals, Mul, Div, Add, Sub, DetectReset, CopyToSchema}` |
| `KahanSum` | `FloatHistogram.KahanAdd` and `util/kahansum.Inc` |
| `FloatHistogram::compact` | `FloatHistogram.Compact(0)` |
| `FloatHistogram::trim_buckets` | `FloatHistogram.TrimBuckets` (returns a copy) |
| `all_buckets`, `all_buckets_rev` | `AllBucketIterator`, `AllReverseBucketIterator` |
| `statistics::{quantile, fraction}` | `HistogramQuantile`, `HistogramFraction` |
| `statistics::{classic_quantile, classic_fraction}` | `BucketQuantile`, `BucketFraction` |
| `statistics::{average, variance, stddev}` | `funcHistogramAvg`, `histogramVariance`, `funcHistogramStdDev` |
| `validate_ingress` | `FloatHistogram.Validate` plus ingress safety checks |

The initial port at `b8e9a0e` used `40ea54d0…`. Comparing the source revisions
found two changes relevant to this library: `TrimBuckets` copies at the pinned
revision, and `HistogramFraction` multiplies before dividing for linear
interpolation. Both are covered by `tests/upstream.rs`. The shared interpolation
helper is retained for trimming and exponential interpolation, where the
arithmetic order is unchanged. Rate/start-timestamp changes in `functions.go`
are outside this library; engine ports must use the engine's pinned revision.

Ingress validation additionally rejects NaN zero thresholds and unrepresentable
bucket indices, and accepts reserved exponential schemas for reduction to schema
8. Arithmetic may produce negative counts and is not ingress-validated. The
owned Rust type does not distinguish nil and empty slices.

## Tests

Files under `tests/` are Cargo integration-test targets, compiled only when those
targets are requested, never into the production library. They intentionally
exercise the public API; moving them to `#[cfg(test)]` modules would not reduce
production compilation. Timing measurements belong in `benches/histogram.rs`,
not ignored tests.

Prometheus is Copyright 2012-2026 The Prometheus Authors and is licensed under
the Apache License, Version 2.0.
