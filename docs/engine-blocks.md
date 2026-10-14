# How `rate` walks a block and crosses its edge

promql-engine internals · companion to [engine.md](engine.md) and [series-source.md](series-source.md) · [illustrated version](engine-blocks.html) of this walkthrough, with an overview of blocks, a step-through of the block edge and the Arrow buffer inspector

A store cuts its answer into blocks along its own time units. It streams each block as series in `RecordBatch`es it may cut anywhere, and a series with several chunks arrives once per chunk. Inside a block, how are two chunks of one series aligned so that `rate` can reach the last value of one and the first value of the next?

`rate(x[5m])` · step 30 s · 420 s → 600 s · two batches, two series, three chunks

**They are not aligned, and need not be.** `x`'s two chunks are ranges in two different `RecordBatch`es, read in arrival order. When the engine reads the second, the **window buffer** of `x` already holds copies of the first chunk's samples that a later window reaches. Alignment is a question only if the operator needs all chunks of a label set at once. `SeriesEvalExec` instead folds one current ID at a time, then emits its evaluated row; an aggregate above sees only those evaluated rows.

## Before you read

- **Block.** The store's unit of time, such as a TSDB block, named on every row by `block_start` and `block_end`. Its samples include the reach-back needed to answer its steps independently. No raw-series fold crosses the edge; completed output and native aggregate state may outlive it.
- **Batch (`RecordBatch`).** The series the store sends together. The store may cut a block into batches anywhere, even between two chunks of one series. It is the unit of transport.
- **Series.** One series' full raw labels, opaque `series_id`, and one chunk of its samples, with the two block columns, stored as one Arrow row. A series with several chunks arrives once per chunk, consecutive by ID and chronological within each block and partition. Ordering between IDs is unspecified. How many chunks a label set becomes in a block is the store's call: a TSDB-backed store sends one per chunk it keeps, a remote store may merge what it receives into one. It is the unit the engine folds.
- **Chunk.** A run of one series' samples as the store keeps it, such as a compressed TSDB chunk. The store need not decode a chunk to trim it, so samples may reach past a block edge or overlap the next chunk of the same label set. It is the store's unit of storage.

One `select` yields blocks, a block arrives as batches, a batch holds series, and a series holds one chunk's samples.

## 1. The shape everything happens in

```
labels       Struct<__name__: Utf8View, ...>
samples      List<Struct<timestamp: Timestamp(Millisecond), value: Float64>>
block_start  Timestamp(Millisecond)
block_end    Timestamp(Millisecond)
series_id    UInt64 | FixedSizeBinary(n>0)   mandatory at the source
```

One series is one Arrow row: a label set and one chunk of its samples, in one block the store cut its answer into. `block_start` and `block_end` name that block on every series. The fixture is one block, so the two columns are the same on every series and stay out of view until section 4. `x` arrives twice and `y` once, in two batches. Batch A holds `x`'s first chunk. Batch B holds `x`'s second chunk, then the one chunk of `y`, which closes `x`. For illustration `x` has ID 41 and `y` ID 7: the order is neither numeric ID order nor a required label order. The counter resets inside B.0, from 16 to 3. Evaluation removes `series_id`, leaving the canonical four-column result.

```
batch A
row  labels.__name__  samples                  first / last sample timestamp, derived
 0   "x"              offsets[0]..offsets[1]   timestamp[0] = 300   timestamp[4] = 420

  offsets    [0                  5]
              |                  |
  timestamp  [300 330 360 390 420]
  value      [ 10  11  12  13  14]
              \______ A.0 _______/
```

```
batch B
row  labels.__name__  samples                  first / last sample timestamp, derived
 0   "x"              offsets[0]..offsets[1]   timestamp[0] = 450   timestamp[5] = 600
 1   "y"              offsets[1]..offsets[2]   timestamp[6] = 300   timestamp[8] = 360

  offsets    [0                       6            9]
              |                       |            |
  timestamp  [450 480 510 540 570 600 | 300 330 360]
  value      [ 15  16   3   4   5   6 |   7   8   9]
              \________ B.0 _________/  \__ B.1 ___/
```

Batch B's offsets and child buffers start again at 0. No index in batch B reaches batch A.

## 2. Batches carry no meaning

> **A store may split a series' chunks across any number of `RecordBatch`es.** A series may end mid-batch or span several, and `x` here does both. The engine never gathers a series' chunks for `rate`. It folds the chunks in arrival order into one window buffer. The current series closes on an ID change, block change or end of stream. Only one ID's fold and history are open per partition; numerical capacity follows the window's sample high water plus a bounded ingestion allowance. Completed output and a pending input cursor are separate, backpressured state. Output budgets allow one indivisible oversized row.

What crosses from A.0 to B.0 is a copy of the samples a later window reaches, never a reference into batch A's buffers. The fold does not notice the batch boundary. [engine.md](engine.md#memory-and-output-ownership) explains why the engine never joins a series' chunks into one.

B.1 closes `x` because ID 7 differs from the current ID 41. The store promises that ID 41 cannot resume later in this block, a contract in [series-source.md](series-source.md#identity-and-order); it may recur in a later block with that block's reach-back. No label comparison or ID lookup table is needed.

These are trusted source obligations, not per-row engine audits:

- An ID maps one-to-one to the full raw label set, stable across chunks, blocks and selections.
- All chunks of an ID are contiguous within a block, chronological by first sample timestamp, and co-located in one partition.
- `block_start` and `block_end` stay constant within a block; blocks ascend and do not overlap, with the same edges in every partition.

A source that violates these obligations can produce wrong results. The engine keeps no seen-ID map or provenance validator. It advertises only block ordering and unknown partitioning to DataFusion, never labels or IDs ascending. Window bounds determine which samples affect a step; block edges assign steps, not sample cutoffs.

## 3. The fold

`rate(x[5m])` runs seven steps, from t = 420 s to t = 600 s at step 30 s. Each step's answer is a function of its **lane**, the samples in its window `(t_step − 300 s, t_step]`. The lanes are the semantics, not `rate`'s data structure. `rate` keeps no lane per step. It copies each chunk into one window buffer per open series, after Prometheus's `BufferedSeriesIterator` in `storage/buffer.go`. It answers a step as soon as its window cannot change any more, and trims from the front what no later window reaches ([engine.md](engine.md#kernel-state)). A state per step would pay for every sample once per window it falls in: ten times here, twenty for `[5m]` at 15 s.

The aggregate above `rate` keeps partials per group and evaluated step. [Section 4](#4-an-aggregate-across-a-block-edge) shows how block ordering permits prefix emission, without promising exactly one block of native state.

What `extrapolatedRate` needs from a lane is small: its first and last sample, the count, and `reset_sum`, the values lost to counter resets. The engine reads the endpoints and the count off the lane's slice of the buffer. It carries the resets from step to step in `Sweep::Counter`, as ordinals into the buffer, so a step costs the samples that enter and leave the window. A running `reset_sum` would still store every reset, to subtract its value when the reset leaves the window. A float that is added to and subtracted from also drifts (the `Sweep::Counter` comment in `range.rs`). What the resets cost is bounded by the resets inside one window, not by its samples.

thanos promql-engine's [`ringbuffer/rate.go`](https://github.com/thanos-io/promql-engine/blob/main/ringbuffer/rate.go) makes the same choice. It keeps every reset in the window as the pair of values either side of the drop. promql-engine uses that buffer only when the range overlaps five or fewer steps (`ringbuffer/overtime.go`). `[5m]` at 15 s overlaps twenty steps, so promql-engine falls back to `generic.go`, which keeps every sample (section 7). The tables below show each lane's endpoints, count, and `reset_sum`.

`rate` pushes A.0 first. The last timestamp seen is then 420 s, so no later sample can land in `(120 s, 420 s]`. `rate` answers the lane at t = 420 s at once. The other six lanes stay open. All five samples stay in the buffer, because the next window, `(150 s, 450 s]`, still reaches 300 s. The lane at t = 600 s holds four of them. Its window is half-open, so the sample exactly on 300 s falls outside it.

| lane t | window | after A.0: first | last | count | reset_sum |
|---|---|---|---|---|---|
| 420 s | (120 s, 420 s] | (300 s, 10) | (420 s, 14) | 5 | 0 |
| 450 s | (150 s, 450 s] | (300 s, 10) | (420 s, 14) | 5 | 0 |
| 480 s | (180 s, 480 s] | (300 s, 10) | (420 s, 14) | 5 | 0 |
| 510 s | (210 s, 510 s] | (300 s, 10) | (420 s, 14) | 5 | 0 |
| 540 s | (240 s, 540 s] | (300 s, 10) | (420 s, 14) | 5 | 0 |
| 570 s | (270 s, 570 s] | (300 s, 10) | (420 s, 14) | 5 | 0 |
| 600 s | (300 s, 600 s] | (330 s, 11) | (420 s, 14) | 4 | 0 |

`rate` copies B.0 into the same buffer, after A.0's five samples, and references nothing of batch A. Only the buffer connects the chunk's first sample, the first thing read from batch B, to A.0. The chunk ends at 600 s, so `rate` answers all six open lanes during the push:

| lane t | after B.0: first | last | count | reset_sum |
|---|---|---|---|---|
| 420 s | (300 s, 10) | (420 s, 14) | 5 | 0 |
| 450 s | (300 s, 10) | (450 s, 15) | 6 | 0 |
| 480 s | (300 s, 10) | (480 s, 16) | 7 | 0 |
| 510 s | (300 s, 10) | (510 s, 3) | 8 | 16 |
| 540 s | (300 s, 10) | (540 s, 4) | 9 | 16 |
| 570 s | (300 s, 10) | (570 s, 5) | 10 | 16 |
| 600 s | (330 s, 11) | (600 s, 6) | 10 | 16 |

The lane at t = 600 s holds `first = 11` from A.0 in batch A and `last = 6` from B.0 in batch B. The fold connects the chunks through copied history, not a reference into batch A. This fixture copies eleven samples. Ingestion is limited to 256 samples between readiness and retirement checks, and unused future tails are not copied. Front trimming leaves vector capacity allocated, so the bound follows the window's sample high water plus that allowance ([engine.md](engine.md#memory-and-output-ownership)).

## 4. An aggregate across a block edge

`sum(rate(x[5m]))` runs over the same seven steps, now with two series and a block edge inside the range. `x{pod="a"}` is the series above. `x{pod="b"}` counts up by one every 30 s, from 10 at 150 s. To keep the tables short, the store cuts its blocks every 240 s; production block widths are source-defined. The steps from 420 s to 600 s fall in two blocks the store declares:

- Block 1, from `block_start` 240 s to `block_end` 480 s, answers 420 s and 450 s.
- Block 2, from `block_start` 480 s to `block_end` 720 s, answers 480 s to 600 s.

The engine selects once, from 120 s to 600 s with `window_ms` 300 s. The store reaches back by that window at each block. Block 1's series hold samples from 120 s to 479 s, and block 2's from 180 s to 600 s. All of block 1's series come before block 2's.

```
block  labels.pod  samples                     first / last sample timestamp
  1    "a"         300 330 … 420 450    (6)    300 / 450
  1    "b"         150 180 … 420 450   (11)    150 / 450
  2    "a"         300 330 … 570 600   (11)    300 / 600    reach-back 300–450
  2    "b"         180 210 … 570 600   (15)    180 / 600    reach-back 180–450
```

The fixture's chunks happen to end and begin exactly at these edges. `a`'s
block-1 chunk ends at 450 s, and `b`'s block-2 chunk begins at 180 s, its
reach-back start. The store need not trim chunks to block edges; whole-chunk
pruning avoids decoding solely to trim ([series-source.md](series-source.md#blocks)).

Suppose block 1's two series come in one batch. The batch spans 150 s to 450 s, and that does not matter. `sum` never sees samples, only the step values `rate` emits, each keyed by its step. `sum` has no current step, so a batch cannot arrive early or late. Evaluated timestamps determine which aggregate lanes receive values.

`rate` folds `a`'s block-1 chunk first. The chunk ends at 450 s, so `a`'s steps at 420 s and 450 s, the only ones block 1 answers, are final. When `b`'s different ID arrives, the evaluator closes `a` and drops its window buffer before reading any of `b`'s samples. The completed row carries the rates of section 5; when its output batch reaches `sum`, those values enter the partials for 420 s and 450 s.

`b`'s two steps are final at 450 s too, at 10 / 300 s = 0.0333333333 each. Each window holds ten samples, so nine increments. A first value of 10 or more keeps the zero clip from biting, so `rate` extrapolates by 10/9. Block 2's first series then arrives with `block_start` 480 s, closing block 1's last raw-series fold. Once the aggregate has consumed the completed block-1 rows and observed the next prefix, the 420 s and 450 s groups can emit. The tables describe this logical progression, not native buffering or exact emission timing.

| step | block | partial after `a` | after `b` | after block-1 prefix closes |
|---|---|---|---|---|
| 420 s | 1 | 0.015 | 0.0483333333 | eligible to emit |
| 450 s | 1 | 0.0183333333 | 0.0516666667 | eligible to emit |
| 480 s … 600 s | 2 | – | – | contributed by block 2 |

A store that hands over whole chunks could send `a`'s B.0 to block 1 unchanged, with samples up to 600 s. Block 1 answers no step past 450 s, so the samples from 480 s fall in no window it evaluates, and nothing is clipped.

Block 2 starts every series from an empty buffer. `a`'s samples from 300 s to 450 s are the block's reach-back. They fill the windows from 480 s onward and answer no step, not even 450 s, which block 1 answered. `a`'s chunk ends at 600 s, so all five steps of block 2 are final during the push:

| step | partial after `a` | after `b`, end of block 2 |
|---|---|---|
| 480 s | 0.0216666667 | **0.055** |
| 510 s | 0.0321428571 | **0.0654761905** |
| 540 s | 0.0354166667 | **0.06875** |
| 570 s | 0.0407407407 | **0.0740740741** |
| 600 s | 0.0407407407 | **0.0740740741** |

`a`'s rates match section 5's to the last digit, because the window at 480 s, `(180 s, 480 s]`, lies wholly inside what block 2's series hold. `rate` reads `b`'s sample at 180 s, but it falls in no window, because each window is open at its start.

No raw-series fold crosses the edge: `rate` holds only one identity's history per source partition. Evaluated rows remain in block order, and each aggregate group allocates lanes over its evaluated span. With several partitions, Partial aggregates feed a Final aggregate through an order-preserving repartition; the group key leads with `block_start` ([engine.md](engine.md#physical-properties-and-session-flags)). This permits completed block prefixes to emit, but native batches, output queues and spill can retain groups from several blocks. The illustrated one-block partials are a schematic of the arithmetic, not an upper-state memory bound.

## 5. Finalising

Each lane went through Prometheus's `extrapolatedRate` when `rate` answered it: t = 420 s during A.0's push, the rest during B.0's. B.1 belongs to series `y`, so for `x` it only closes the output series, which DataFusion sees from then on. For the lane at t = 600 s, spelled out once:

```
result             = last_v - first_v + reset_sum = 6 - 11 + 16 = 11
sampled_interval   = 600 - 330                                  = 270 s
average_interval   = 270 / (10 - 1)                             = 30 s
threshold          = 1.1 * 30                                   = 33 s
duration_to_start  = 330 - 300 = 30 s   (< 33, kept as measured)
duration_to_zero   = 270 * (11 / 11) = 270 s   (30 < 270, zero clip does not bite)
duration_to_end    = 600 - 600 = 0 s    (< 33, kept)
factor             = (270 + 30 + 0) / 270                       = 10/9
increase           = 11 * 10/9                                  = 12.2222222
rate               = 12.2222222 / 300                           = 0.0407407407
```

All seven, same procedure:

| lane t | result | sampled interval | to_start | factor | increase | rate |
|---|---|---|---|---|---|---|
| 420 s | 4 | 120 s | 15 s (clamped) | 135/120 | 4.5 | **0.015** |
| 450 s | 5 | 150 s | 15 s (clamped) | 165/150 | 5.5 | **0.0183333333** |
| 480 s | 6 | 180 s | 15 s (clamped) | 195/180 | 6.5 | **0.0216666667** |
| 510 s | 9 | 210 s | 15 s (clamped) | 225/210 | 9.6428571 | **0.0321428571** |
| 540 s | 10 | 240 s | 15 s (clamped) | 255/240 | 10.625 | **0.0354166667** |
| 570 s | 11 | 270 s | 30 s | 300/270 | 12.2222222 | **0.0407407407** |
| 600 s | 11 | 270 s | 30 s | 300/270 | 12.2222222 | **0.0407407407** |

The clamped rows are the five whose window starts at least the 33 s threshold before the first sample, so `duration_to_start` gets the half-interval guess. The reset shows as `result` climbing from 6 to 9 while the raw last value fell from 16 to 3.

## 6. Overlapping chunks

When a series' chunks arrive from several files or compaction levels, adjacent chunks of that series in one block may overlap in time. The engine remembers the last timestamp it saw for the current identity and skips any sample at or before it. The first chunk therefore wins, as in Thanos's `chunkSeriesIterator`. Prometheus's union can retain interleaved nonduplicate timestamps that this policy skips ([series-source.md](series-source.md#identity-and-order)).

A stale marker claims its timestamp too, before `rate` filters the marker out of the buffer. A sample at the same timestamp in the second chunk therefore does not replace a marker in the first. The skip matters even for an exact duplicate, which moves neither endpoint and fakes no reset but would raise `count`. A higher `count` shortens the average interval, which can push `duration_to_start` through a clamp it would otherwise miss.

## 7. Functions that need every sample in the window

`quantile_over_time` and functions like it need every sample in the window. Each step refolds its lane's chronological slice of the same history buffer, with function-specific scratch as needed ([engine.md](engine.md#kernel-state)). The slice follows the window; retained capacity follows the window's sample high water plus the bounded ingestion allowance. One value calculation remains indivisible, even though the surrounding sample and step walks can yield. thanos promql-engine's [`ringbuffer/generic.go`](https://github.com/thanos-io/promql-engine/blob/main/ringbuffer/generic.go) is the same structure as the window buffer. It keeps every sample in the window and hands the function the slice.

## Notes on the numbers

The fold and `extrapolatedRate` run in the interactive page. The page follows Prometheus's `promql/functions.go` term for term and runs the guards in that file's order: threshold clamp, then counter zero clip. The page reads nothing back from the engine. Instead, the engine checks itself against the page. `the_engine_blocks_fixture` in `buffer.rs` pins this fixture as a regression: A.0 must push before B.0, the lane at 420 s must answer before B.0 arrives, and all seven rates must match to 1e-10. Section 4 reuses those rates and adds `x{pod="b"}`'s, worked by hand through the same terms.
