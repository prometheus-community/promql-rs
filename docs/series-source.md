# The `SeriesSource` trait

Every PromQL-on-something implementation needs Prometheus's
`storage.Querier.Select`: the series matching these matchers, in this time
range. The store owns selection and assembly; the engine owns PromQL,
lookback, staleness, the step grid, functions and aggregations.

There is one source contract: **series-contiguous chunks within logical time
blocks, with mandatory opaque IDs**. A batch boundary is not a series boundary.

## What the trait requires

```rust
#[async_trait]
pub trait SeriesSource: Debug + Send + Sync {
    async fn select(
        &self,
        state: &dyn Session,
        matchers: &[LabelMatcher],
        hints: SelectHints,
    ) -> DFResult<Arc<dyn ExecutionPlan>>;
}

/// Prometheus's storage.SelectHints, typed, plus the window.
pub struct SelectHints {
    pub start_ms: i64,              // inclusive; binding, widened by the window
    pub end_ms: i64,                // inclusive; binding
    pub window_ms: i64,             // lookback delta or [5m]; binding at every block
    pub step_ms: Option<i64>,       // advisory query grid
    pub range_ms: Option<i64>,      // advisory range-selector window
    pub func: Option<String>,      // nearest function or aggregation above
    pub grouping: Option<Grouping>,// nearest by labels through label-keeping calls
    pub shard: Option<Shard>,      // return shard index of count only
}

pub struct Grouping { pub labels: Vec<String>, pub by: bool }
pub struct Shard { pub index: u64, pub count: u64 }
```

The public hint structs are non-exhaustive. Build hints with
`SelectHints::range` and its `with_` methods; stores normally read them rather
than constructing them.

**One select per selector.** Selection happens at plan time, before the query
runs, as Prometheus expands a selector's series before evaluating. The schema,
including the union of the selection's label names, must be known for planning
and fixed across all blocks and partitions. `SelectorTable` makes the returned
physical plan an ordinary DataFusion table scan.

The matchers arrive as parsed, including `__name__` synthesized from a bare
metric name. An absent label compares as `""`, and regexes are anchored to the
whole value: `k!="v"` and `k=~".*"` match a series without `k`, while `k=~".+"`
does not. `matcher.rs` is the reference implementation.

The inclusive millisecond range is binding and already accounts for the window,
`offset` and `@`: from the first step's window end, `t − offset` or
`@ − offset`, widened back by the instant lookback or range window, to the last step's
window end. This follows `getTimeRangesForSelector` in Prometheus
`promql/engine.go` at 83962c35. `window_ms` tells the store how far back to reach
at **every** block, without interpreting PromQL.

The remaining hints are advisory and may be ignored without changing results:

- `step_ms` and `range_ms` may help choose aligned/downsampled data or prune
  chunks, but never license changing exact window semantics.
- `func` names the nearest function, not the entire expression. For example,
  `count_over_time` needs presence but `rate` needs values across its windows.
- `grouping` carries the nearest `by` through label-keeping calls:
  `sum by (pod) (abs(rate(x[5m])))` supplies `func=rate`, `by=(pod)`. A nested
  aggregation starts its own grouping. `without`, label rewrites, `absent`,
  sorts and binary operators stop it. An ungrouped aggregation has empty `by`.
  This is metadata about readers, not permission to merge identities or omit
  the source's full raw label set. Merging counters before `rate` breaks reset
  detection, and merging below `count` changes the answer.
- `shard` selects one part of the full series space by identity.

Prometheus's `Limit` and `DisableTrimming` are omitted: they serve its label
API and its own chunk trimming.

## Blocks

The store cuts time according to its own units. Blocks ascend by `block_start`,
do not overlap, and are contiguous over the selection's window-end domain,
`[start_ms + window_ms, end_ms]`. The last exclusive `block_end` lies past
`end_ms`. Every partition of a selection uses the same edges.

A block answers only the steps whose window end, `t − offset` or `@ − offset`, lies in
`[block_start, block_end)`. For each selected identity it contains all available
samples in `[block_start − window_ms, block_end)` within the select's range.
The series appears even if all its samples are before `block_start`: they may
still answer an instant lookback or a range window. Reach-back feeds windows,
not steps in the previous block. Each block can answer alone; no per-series
numerical state survives its edge.

Whole chunks may straddle either sample boundary. The store may prune by chunk
min/max metadata without decoding to trim. Evaluation skips unreachable prefixes
and future tails and ingests the remainder in bounded allowances (see
[memory and output ownership](engine.md#memory-and-output-ownership)). The pending
raw batch remains a separate retained owner. A store with whole series sends one
block and one chunk per series; it still supplies mandatory IDs.

## The series batch

```text
labels       Struct<{name}: Utf8View, …>    one field per label name, sorted
floats       List<Struct<timestamp: Timestamp(ms), value: Float64>>
histograms   List<Struct<timestamp: Timestamp(ms), …>>   native histograms
block_start  Timestamp(ms)                  inclusive window-end boundary
block_end    Timestamp(ms)                  exclusive; both constant within a block
series_id    UInt64 | FixedSizeBinary(n>0)  mandatory raw identity, internal to evaluation
```

One row is one chunk of one full label set in one block, with at least one
sample. A selection has `floats`, `histograms`, or both
([sample types](#sample-types)). Samples within a list ascend by timestamp.
Nothing is nullable.
An absent label is `""`, not NULL. Block columns are plain millisecond timestamp
arrays, not run-end encoded. `Float64` preserves StaleNaN's exact `0x7ff0_0000_0000_0002` bits;
nothing may cast it through an ordinary NaN.

Columns bind by name, so sources may reorder them and supply extra columns.
Evaluation binds `labels`, the sample columns the selection has, both edges and
`series_id`; it emits the canonical four fields in that order, then
`histograms` where it can answer with one, preserving field and schema
metadata.
IDs and other source-only columns never escape evaluation. `series.rs` defines
the shape; `series::validate` is an explicit utility, not a required
source-boundary audit. Canonical evaluated data has four columns; a canonical
raw batch also has its fixed-width ID. Chunk-splitting helpers preserve every
non-sample column, including IDs and source-specific columns.

### Sample types

A series' samples are floats, native histograms, or both over time: a metric
migrated from a classic to a native histogram is one series that changes type.
The two types are sibling list columns, not one list of a union:

```text
histograms  List<Struct<
  timestamp: Timestamp(ms),
  counter_reset_hint: UInt8,       0 unknown, 1 reset, 2 not reset, 3 gauge
  schema: Int32,                   -4..8 exponential, -53 custom buckets
  count, sum, zero_threshold, zero_count: Float64,
  negative_spans: List<Struct<offset: Int32, length: UInt32>>,
  negative_buckets: List<Float64>,
  positive_spans: List<Struct<offset: Int32, length: UInt32>>,
  positive_buckets: List<Float64>,
  custom_values: List<Float64>,
>>
```

The entry is Prometheus's `histogram.FloatHistogram` field for field: spans and
absolute bucket counts as they are, neither expanded to bounds nor serialized
to bytes. `histogram::histograms_type` builds the type and
`histogram::encode_row` one row of it.

- **Either column may be absent, not both.** The schema is fixed per selection
  and planned against, so a store without native histograms omits `histograms`
  and its queries plan exactly as they did before the column existed. A
  selection of only histograms may omit `floats`. A store that cannot know
  before reading supplies both; a row's list for the type it lacks is empty.
- **A timestamp of a series is in one list.** Evaluation merges the two lists
  of a chunk by time and treats the result as one series: the latest sample
  before a step is the latest of either type. Like contiguity this is trusted,
  not checked. Should both lists hold a timestamp, the float is the sample.
- **Staleness** is `sum` holding StaleNaN's exact bits, as upstream's
  `value.IsStaleNaN(h.Sum)`. A stale histogram's other fields are not read.
- **Counter reset hints are the store's to supply**, as its chunks know them:
  `gauge` for a gauge histogram, `not_reset` where the store knows the sample
  continues its predecessor, `unknown` otherwise. The engine detects resets
  itself where the hint does not decide, with `FloatHistogram.DetectReset`.
  Prometheus's TSDB answers `unknown` for the first histogram of every chunk
  and `not_reset` for the rest (`counterResetHint` in
  `tsdb/chunkenc/histogram_meta.go`).
- **Buckets are validated at ingress, not here.** The kernels assume
  structurally valid histograms (`promql_histogram::FloatHistogram::
  normalize_ingress`); evaluation copies a histogram out of a batch only when a
  window or a step needs it, and validating there would touch every bucket of
  every sample.

Why two columns rather than a union or a nullable child: a float sample then
costs sixteen bytes and no type tag, as it did; a consumer of one type never
touches the other's buffers; and DataFusion prunes a top-level column where it
would not prune a struct child, so `rate(float_metric[5m])` over a store with
both columns carries no bucket past the scan. Prometheus's `promql.Series`
holds `Floats` and `Histograms` apart for the first two reasons
(`promql/value.go`).

### Identity and order

The plan returned by `select` has these trusted obligations over **each
partition's entire stream**, never just a batch:

1. Every series matches every matcher; chunks wholly outside the select's
   range are pruned by their metadata.
2. All rows of one block precede the next block's rows.
3. All chunks of one ID within a block are contiguous and ascend by first
   sample timestamp. The first foreign ID closes the current series; that ID
   cannot recur until a later block. A series can close mid-batch or continue
   across any number of batches. Empty batches change nothing.
4. All chunks of an identity stay in one partition throughout the selection.
   Partition widths and assignments need not align between selections.
5. Within one source, one ID maps to one full raw label set and that label set
   maps to one ID, stable across chunks, blocks and selections. A hash of only
   surviving aggregation labels is not a series identity. A store whose
   ingestion ID is finer than PromQL identity must merge it to one ID per full
   label set before emitting. The existing UInt64 collision policy is the
   source's responsibility; this contract does not change it.

Order **between IDs is unspecified**. Neither ascending labels nor ascending
IDs may be advertised just because chunks are contiguous. The engine holds
one current ID/fold, not a cardinality-sized map or a seen-ID set. It does not
rehash labels, audit schemas/rows or validate assembly provenance on every
chunk. A source that violates contiguity can emit the same identity twice;
trusted ordering is an obligation, not a runtime certification.

**Overlap.** Adjacent chunks can overlap, as several files or compaction levels
may contain them. Within a block, first chunk wins: samples at or before the
last timestamp already seen are skipped, of either type and including stale
markers before range functions filter them out. Each block starts afresh because reach-back repeats
samples intentionally. This follows Thanos's `chunkSeriesIterator`
(`pkg/query/iter.go:277-281`); Prometheus's block merge instead unions samples
and drops duplicate timestamps. They agree on duplicates, not interleaved
nonduplicate timestamps. Stores must preserve their intended duplicate
precedence while assembling chronological chunks.

### Physical source boundary

The returned plan is execution-ready: the store completes its own planning
and optimization before returning it. `SeriesSetExec::new(input)` executes
that plan as a **protected physical source leaf**. Its internals are not outer
optimizer children: ordinary rewriting must not remove exchanges or merges
that establish co-location and chronology. Resetting execution state also
resets the private source plan, including stateful exchanges.

The adapter reports `UnknownPartitioning(n)` at the store's original width and
only `block_start ASC`. Co-location says nothing about DataFusion's hash
function or compatibility with another selection's partition assignment.
`SeriesEvalExec` requires that block prefix, unspecified distribution and no
extra input partitioning. Upper aggregation/vector matching establishes its
own distribution over evaluated rows, never raw chunks.

The engine's physical explanation renders source-owned nodes below
`SeriesSetExec` explicitly; generic visitors stop at the leaf.
`SeriesSetExec::source_plan()` permits explicit inspection and native metrics.
The printed request includes selector, range and hints. Source nodes implement
`DisplayAs` to explain their pushdowns; a generic data source cannot convey
store-specific work on its own.

## Implementing it

**In memory.** `MemorySeriesSource` assigns stable IDs before filtering and
emits contiguous chunks. It knows its data before it answers, so a selection
none of whose series has a histogram has no `histograms` column, and one with
no float has no `floats`; a chunk of a series with both holds what the series
had in the chunk's span, of either type. Whole-series, realistic-chunk and one-sample-chunk
fixtures share the same contract. Tests also cut blocks and partitions and
exercise both UInt64 and fixed binary identities, without an alternate
interleaving mode.

**Columnar storage.** Emit `(block, identity, chronological chunk)` order. The
identity key need not be label order. A store with samples as rows performs
one regrouping into chunks; one with chunks as rows sorts/merges chunk
metadata, never concatenates a whole series. Predicate and chunk min/max
pruning belong in its scan.

For example, if one logical block reads several files or storage partitions,
merge their chunk descriptors by identity and then first sample timestamp.
Emitting each file's rows independently could reopen an identity within that
block. Storage buckets do not replace window filtering: reach-back and
modifiers still determine which samples the block must contain.

**Remote protocols.** Request each block widened by the window. Group reply
chunk descriptors by identity, sort each identity's chunks by first timestamp
where the protocol does not promise chronology, and emit them consecutively.
A protocol's label order satisfies contiguity but is not required by this
engine. `encode` represents one complete chunk per series and rejects duplicate
label sets, so multi-chunk stores build Arrow batches directly rather than
concatenating every chunk to use that helper.

## Non-goals

Exemplars, metadata and dictionary labels remain deferred. Histograms arrive
as `FloatHistogram`s; a store holding integer histograms converts them as
Prometheus's `ToFloat` does.
A series crossing DataFusion partitions would require a distributed merge and
is outside the contract. Downsampling/pre-aggregation hints do not specify a
safe transformation. See [`engine.md`](engine.md) for evaluation, numerical
state, output ownership and memory accounting.
