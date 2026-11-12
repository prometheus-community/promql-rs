# promql-histogram

Native-histogram values and numerical operations ported from Prometheus:
exponential and custom buckets, traversal, validation, arithmetic, reset
detection, quantiles, fractions, variance, and trimming. See
[`UPSTREAM.md`](UPSTREAM.md) for the pinned revision and function map.

This crate is independent of Arrow and DataFusion. `promql-engine` carries
histograms between operators in Arrow (its `histogram` module) and decodes a
sample into a `FloatHistogram` here wherever one is computed on; `promql-parser`
builds them from `{{…}}` series descriptors.

## Ownership and validation

`FloatHistogram` owns its span, count, and custom-bound vectors. Iterators borrow
those buffers without allocating. A `BucketCursor` used separately from its
histogram must continue against that same unchanged histogram.

Stores may call `normalize_ingress` once on untrusted data; this validates the
layout and reduces reserved exponential schemas to schema 8. Do not validate
arithmetic results as ingress: subtraction and scaling can legitimately produce
negative populations. Kernels assume structurally valid, normalized input.

`add`, `sub`, `mul`, and `div` modify the receiver, never another operand.
`add`/`sub` and `KahanSum::add` return `ArithmeticOutcome`: reset-hint collisions
and custom-bound reconciliation are non-fatal findings an evaluator must turn
into annotations. Incompatible exponential/custom schemas are errors.
`trim_buckets` returns an independent copy, matching the pinned Prometheus
`FloatHistogram.TrimBuckets`.

`add`, `sub`, `KahanSum::add` and `detect_reset` reconcile two bucket layouts
through ordered maps. Two histograms of one exponential layout, the same
schema, zero threshold and spans with no bucket inside the zero bucket, skip
that and are walked bucket by bucket: consecutive samples of a series nearly
always qualify, and so do the terms of a `sum` over series that populate the
same buckets. `tests/aligned.rs` holds the shortcut to the general path.

`heap_size` reports retained vector capacity, not just populated entries. It
excludes the inline value; an engine's reservation must account for both.

## Checks and benchmarks

Run from the workspace root:

```sh
mise run test -p promql-histogram
mise run lint
mise run build-benches
```

Public API tests live in `tests/`, which Cargo does not compile into production
libraries. Timing and allocation measurements use `benchfmt` in
[`benches/histogram.rs`](benches/histogram.rs); its header gives the recommended
invocation. Mutating benchmarks include the owned working copy so repeated
iterations cannot converge to empty or overflowing histograms.
