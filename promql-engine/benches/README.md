# promql-engine benchmarks

The benchmark targets measure different parts of query evaluation:

- [`kernels.rs`](kernels.rs) times numerical kernels, accumulators, and
  preplanned series evaluation.
- [`engine.rs`](engine.rs) times whole queries and preplanned DataFusion execution.
- [`memory.rs`](memory.rs) measures heap use for individual queries, not
  execution time.

The source files define benchmark IDs, fixture sizes, query grids, and setup.
Their module comments describe the measurement boundaries.
[Compare benchmark revisions](compare.md) covers baselines and profiles.

## Measurement boundaries

Measurements include different costs depending on the execution path:

| Path | Included | Excluded |
|---|---|---|
| Whole query | Parsing, source selection, planning, execution, and API result processing | Runtime, engine, and stored-input construction |
| Logical planning | Parsing, source selection, and logical planning | Physical planning and execution |
| Raw preplanned execution | State reset, stream creation, execution, output construction, and output drain | Planning and API result processing |
| Prepared API execution | State reset, execution, validation, ordering, and collection | Planning and source selection |

API result processing includes schema validation, empty-row removal,
duplicate-label checks, and result ordering. Raw execution does not perform
those steps. Source generation can also be measured when a fixture generates
chunks as the consumer polls them. Each benchmark defines its setup boundary.

Resident memory cases keep the collected result during the peak and held-memory
readings. Streamed cases drain output and retain at most the current output
batch in the consumer. Streamed output does not imply that input is also
generated on demand.

## Heap accounting

The memory benchmark reports distinct quantities. None is RSS or a DataFusion
reservation.

### Logical payload

Logical payload counts visible sample pairs and UTF-8 label value bytes.
A label repeated on separate rows counts once per occurrence. Offsets, views,
block columns, spare capacity, and headers are excluded.

Streamed cases accumulate total payload processed, not simultaneously retained
results. `largest logical batch` uses the same payload definition.

### Unique retained Arrow capacity

This reading counts result buffer capacities, including offsets, views,
validity buffers, and hidden portions of slices. Buffers are deduplicated by
their base address across the held result. A retained buffer can also belong
to the source, so retained capacity is not exclusive ownership.

The buffer walk runs after allocator readings to exclude its accounting
allocations from query peaks. Schemas, headers, `Arc` control blocks, and `Vec`
overhead are excluded. External buffers without a reported capacity appear
in `external owners`, without an estimated byte count.

Streamed consumers retain no result buffers after the drain. They keep no
address set across batches because allocators can reuse addresses.

### Allocator readings

The allocator reports requested allocation sizes, not size-class overhead:

| Reading | Meaning |
|---|---|
| `process baseline` | All live allocations before the query, including the runtime, engine, resident input, and fixture metadata. |
| `incremental peak` | Maximum live bytes during the query minus the baseline. Includes planning and streamed source generation. |
| `held after` | Live-byte growth on return, with resident results held and before accounting or profile helpers run. |
| `allocation calls` | Successful `alloc`, `alloc_zeroed`, and `realloc` calls. |
| `requested traffic` | Full requested sizes for those calls. A reallocation counts its new size even when it stays in place. |

Allocation traffic is neither bytes copied nor unique retained bytes.
[`support/result_memory.rs`](support/result_memory.rs) defines payload and
buffer accounting. [`support/counting.rs`](support/counting.rs) defines the
allocator readings.

DataFusion reservations estimate accounted memory at executor checkpoints.
Reservation peaks can differ from allocator peaks. Reservations do not promise
preallocation or establish a hard heap bound.

## Measurement limits

A timing difference isolates one cost only when the compared fixtures differ
in that cost alone. Renamed IDs, changed input shapes, or different result
retention can invalidate a comparison. The fixture code is the source for
those details, not historical measurements.

The suite does not establish a general window-only memory bound, count bytes
copied, or prove bounds on scheduling delays or cancellation. Those claims need
capacity tests, allocation attribution, and scheduler integration tests.

## CPU and heap profiles

`--profile-time` produces a CPU profile instead of a timing estimate.
[`support/pprof.rs`](support/pprof.rs) defines the profiler configuration.

The memory benchmark's `heap-profile` feature adds jemalloc profiling, and
`MEMORY_HEAP_DIR` selects the output directory. Resident cases capture the heap
with the result held. Streamed cases capture the output-batch observation with
the most live bytes. That observation can miss an earlier peak during evaluation.

Heap symbolization caches debug information after the first dump. Later cases
in the same process include that memory in their readings. The
[comparison guide](compare.md) runs each heap-profile case in a separate process.
