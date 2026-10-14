# The engine

Companion to [`series-source.md`](series-source.md), which fixes what a store
hands us. Evaluation folds chunks for **one current identity per partition**,
rather than assembling every selected series. The source contract makes one
identity's chunks contiguous within each logical time block. Numerical history
follows the window's sample high water plus a bounded ingestion allowance,
independent of raw chunk length.

## Lineage

Prometheus never materialises a series. `storage.BufferedSeriesIterator` keeps
a ring buffer of the lookback delta (`storage/buffer.go:26-36`),
`matrixIterSlice` walks timestamps and a reused sample slice rather than a
series object (`promql/engine.go:2727-2886`), and
`populateWithDelSeriesIterator` makes chunk boundaries invisible to evaluation
(`tsdb/querier.go:759-791`). Memory depends on the window and identities in
flight, not the age of a series.

Thanos's proxy merges chunk descriptors, not samples
(`pkg/store/proxy_merge.go:145-188`). Its engine likewise uses a per-series ring
buffer, but opening one for every selected series retains cardinality-sized
state (`promql-engine/docs/architecture.md:198-213` there). Our source contract
moves assembly to the store that already knows chunk identities. DataFusion
provides streams of Arrow `RecordBatch`es; every operator consumes and emits
that shape, and a series remains a row from store to result.

The spec is Prometheus at 83962c35, recorded in
`promql-conformance/testdata/prometheus/UPSTREAM.md`, not a local checkout's
HEAD. Kernel comments name the functions they port.

## The evaluation boundary

Each selector issues one `SeriesSource::select` over the query range, widened
by its window and shifted by `offset` or `@`, as Prometheus's
`getTimeRangesForSelector` widens select hints. The source returns an
execution-ready `Arc<dyn ExecutionPlan>`. IDs are mandatory UInt64 or
positive-width FixedSizeBinary, one-to-one with full raw identity. Within each block, an ID's
chunks are consecutive, chronological and partition-colocated; that ID may
recur in later blocks. Order between IDs is unspecified.

`SelectorTable` presents the selection as a `TableScan`. `SeriesSetExec` is a
protected physical leaf over the completed source plan. Outer optimization
cannot remove source-internal merges/exchanges that establish co-location and
chronology. Resetting this leaf also resets the private source plan. The
physical explanation renders that private plan explicitly, while generic
DataFusion visitors stop at the leaf; native source metrics remain accessible
through `SeriesSetExec::source_plan()`.

Selectors and range functions share one typed `SeriesEval` logical boundary
(`series_eval.rs`). It carries a `buffer::Kernel` and `Params`: step grid,
window and modifiers. The extension planner always constructs
`series_eval::exec::SeriesEvalExec`. It ports `evalVectorSelector`,
`rangeEvalTimestampFunctionOverVectorSelector` and `matrixIterSlice` in
Prometheus `promql/engine.go`. Shared execution helpers live in
`series_eval/state.rs`. All supported ID encodings use the same executor;
selectors and range functions are not DataFusion aggregate UDFs.

A top-level range selector in an instant query is different: `matrix::Window`
filters each chunk's referenced sample span and retains the samples' own
timestamps, after Prometheus's `matrixSelector`. It does not evaluate a step grid
or pass through `SeriesEval`; `series::coalesce` joins its evaluated result rows.

Evaluation retains raw labels, samples, both block edges and ID even when a
consumer projects labels away. It binds fields by name and emits canonical
`labels, samples, block_start, block_end`, retaining metadata without raw IDs
or uniqueness constraints. Predicates and limits remain above evaluation: a
raw chunk is not a result row. Direct column projections can prune unused
evaluation outputs, including labels for samples-only output. The evaluator
retains a canonical-order subset; native `ProjectionExec` owns aliases,
reordering and repeated columns. Arbitrary expressions remain native projections.

A scalar UDF over `samples` cannot implement range evaluation here. It sees
only one chunk, not all chunks needed by a window. Concatenating a whole series
in the store to satisfy that signature merely moves the unbounded copy down
one layer. The typed evaluator folds chunks directly, retaining numerical
history separately from the source batch. Its current allocation limits are
described below.

## Blocks and transitions

The store cuts time and stamps every row with `block_start` and `block_end`.
All partitions of a selection share those edges. A block answers only steps
whose window end, `t − offset` or `@ − offset`, lies in `[block_start, block_end)`. Its
reach-back samples before `block_start` feed windows, not previous steps.
`SelectHints.window_ms` tells the store how much reach-back to include at each
edge. Each block answers independently; no per-series numerical state crosses
its edge.

The executor learns a block's bounds from its first row and holds one current
ID, owned labels and fold. It finishes that fold at an ID change, block change
or EOF. A pending input cursor retains the next unconsumed row while ordinary
completed output fills or waits for its consumer. The next ID is not opened
merely to drain the old one. Output batches have row/sample budgets, except
for one indivisible oversized result row, and downstream demand provides
backpressure. A 32,768-unit poll budget covers ingestion, window-edge movement,
sweep queue pops and evaluated steps. Input and close cursors survive a yield,
including an ID/block transition or EOF. An individual range value calculation
remains indivisible and is charged afterward; this is not wall-clock preemption. There is
no per-block identity map or seen-ID set: source ordering and identity are
trusted, not revalidated on every chunk.

**Batch boundaries mean nothing.** Chunks can span any number of batches and
a series can finish mid-batch. Empty batches do not close it. The current fold
consumes chunks in arrival order; ID/block transitions, not batch boundaries,
decide its lifetime. A whole-series source is the one-block, one-chunk case,
still with mandatory IDs.

An upper `sum by (…)` uses native `AggregateExec` (`aggregate.rs`). Its keys
lead with the block, and each group allocates lanes over its evaluated
span, not unconditionally over the query grid. Block-prefix ordering lets
completed groups emit once all source partitions have passed the block;
native batching and spill can still retain state for multiple blocks. Two
selectors may cut different edges, especially under offsets. Their vector
matching uses evaluated steps, not raw block/ID keys;
existing whole-grid binary matching is not changed by this evaluator work.
[`engine-blocks.md`](engine-blocks.md) walks numerical block edges.

## Kernel state

A kernel reduces one step's window to a value: `range::evaluate` for a range
function, or the instant selector's last eligible sample. The native lane
kernels of `aggregate.rs` combine evaluated per-step partials and are separate.
`buffer::BufferedSeriesIterator`, after Prometheus's
`storage.BufferedSeriesIterator`, owns mutable cursors/history and borrows an
immutable block-local `EvalContext` containing kernel, parameters and cached
step count. One context serves the block; only one fold is open per partition.

Range functions and multi-step selectors retain one window buffer for the
current identity. A one-step selector instead keeps the latest eligible
sample's timestamp and original value, including stale bits, without
sample-history allocation. `timestamp()` checks that original value for
staleness before converting its timestamp; output points still carry the
evaluation step's timestamp.

History owns copied samples, not Arrow slices that would pin the batch's
child buffers, including other series. Ingestion admits at most 256 samples
before checking ready steps and retiring history. Dead prefixes and samples
past the last window end are skipped. With empty history and no range stale
markers or `@`, that bounded slice can be evaluated while borrowed; only its
reachable suffix is copied if more work remains. Stale-containing and second
chunks use the same bounded allowance rather than whole-chunk appends.

A step evaluates as soon as its window cannot change, when its end is at or
before the readiness watermark. An allowance ending inside an equal-timestamp
run cannot finalize that timestamp prematurely. At close, remaining steps drain
under the same work budget. Without eager evaluation, a long one-block source
would accumulate an entire history before its first answer.

History drops samples no later window reaches once at least half its buffer
can retire, following Prometheus's `ReduceDelta` (`storage/buffer.go:67-70`).
`rate`, `increase`, `min_over_time` and `max_over_time` carry a `Sweep` across
steps: counter resets or extremum candidates use absolute sample ordinals, so
trimming the front does not rewrite them. Each step costs the samples entering
and leaving its window, rather than refolding every sample for every lane.
Other functions refold through `range::evaluate`; order-dependent functions
read already chronological history.

`offset` and `@` move window ends, which determine the answering block. Under
`@`, every step shares one pinned window, so one block answers the whole grid.
Its value (including an absent result) is computed once and repeated under the
step budget; samples outside that window are not copied. Overlap is another
reason chunks cannot be evaluated independently and their answers combined.

**Overlap.** Within a block, samples at or before the last timestamp already
seen for the identity are skipped: first chunk wins, following Thanos's
`chunkSeriesIterator` (`pkg/query/iter.go:277-281`). A stale marker claims its
timestamp before range evaluation filters it. The next block starts afresh
because reach-back repeats samples intentionally. Prometheus's block merge
instead unions samples and drops only duplicates (`storage/merge.go:653-656`);
they agree on duplicates, not interleaved nonduplicate timestamps.

## Memory and output ownership

Numerical iteration emits `(timestamp, value)` pairs; the executor owns Arrow
row boundaries. The current fold's empty/singleton results stay inline and
larger results use compact timestamp/value vectors without list offsets.
Ordinary output builders bulk-append them and finish one row per identity per
block. There is no per-fold Arrow result array.

Only one identity's numerical history is retained per source partition,
independent of selection cardinality. History and sweep capacity are
**O(H + 256)**, where H is the fold's live-window sample high water (including
stale samples for selectors). Deferred front retirement, adjacent-window
transitions and geometric vector growth add constant factors. Trimming does
not shrink capacity, so this is a high-water bound, not the current window's
length. Raw chunk length and unreachable future tails do not add a capacity
term.
Current evaluated results add **O(steps answered by the block)**; under `@`
this may be the whole query grid. Budgeted ordinary output (with an
oversized-row exception) and the pending input batch/cursor are separate
retained state. Above evaluation, aggregation allocates each group over its
evaluated timestamp span with geometric left headroom, or shared flat lanes
for a one-step grid. Headroom prevents descending inputs from repeatedly
shifting every existing lane; it remains charged in the capacity estimate.
For ordinary block-local input this is
**O(live groups × steps per block)** rather than each group over the whole query
grid. Native DataFusion batching and spill can retain groups from several blocks;
this is not a guarantee of exactly one block of upper state. Result collection
still holds the full answer. These are state bounds, not a claim that arbitrary
source batches, block grids or final answers have a universal byte limit.

Labels are copied once when an identity opens and an output consumer needs
them, in schema order. Later chunks use the owned values. Schema metadata and
field names stay shared; consumed input label arrays are not retained by the
fold. Output builds ordinary Arrow string arrays. Samples-only output skips
label capture. This avoids pinning unrelated labels but costs string
allocations/copies; it is not a label-demand API for the store, which retains
full raw identity.

A DataFusion reservation charges owned ID/label capacities, numerical state,
compact results and output construction. Raw input and canonical output use
Arrow's native size estimates. Finish checkpoints charge result/output storage
while numerical allocations and owned labels still coexist, before releasing
them. Sweep queue capacity counts even when cleared. Exhaustion is an error,
not a spill/resume path. Reservations are checkpoint estimates, not RSS or
preallocation. Cancellation can synchronously destroy remaining current/output
state. One numerical value fold, binary searches, allocation/compaction, Arrow
bulk append/finish and destruction remain indivisible; the work budget is not
a hard elapsed-time bound.

The engine does not use `arrow::compute::concat` to assemble a whole raw series. Arrow list offsets give
O(1) first/last timestamp reads for a chunk, allowing chunks that cannot reach
the first window to be skipped. Empty chunks are forbidden by the source
contract but safely skipped because they have nothing to fold.

## Physical properties and session flags

`SeriesSetExec` and evaluation report the original width as
`UnknownPartitioning(n)` and only `block_start ASC`. ID contiguity is not an
ASC declaration; no label or ID order is fabricated. Co-location does not
prove a DataFusion-compatible hash distribution, group uniqueness or alignment
with another selection. The evaluator requires only the truthful block prefix,
`UnspecifiedDistribution`, and `benefits_from_input_partitioning = false`.
Output properties are fresh, not inherited source hashes/identity constraints.

Upper aggregation/vector matching establishes its own distribution over
completed series rows. An upper aggregation's hash exchange leads its grouping
key with `block_start`, and order-preserving exchange lets the final aggregate
stream on the block prefix. DataFusion enforces these native requirements.
Raw chunks must not be shuffled just to satisfy session target parallelism.

The engine keeps `repartition_aggregations` on and
`subset_repartition_threshold = 1` for upper aggregation tuning. Round-robin
and file-scan repartitioning stay off: a raw-row or byte-range split can cut
through an identity. Parallelism below evaluation is the source's partition
count and `SelectHints.shard`, never `target_partitions`.

## Public results and execution APIs

The [query API migration guide](query-api.md) covers breaking call-site changes,
prepared-query reuse, raw streaming and per-execution metrics.

Results leave the evaluator block by block; partitions can finish in any
order. An identity seen in many blocks leaves as one Arrow row per block, each
holding ascending evaluated steps. Collection validates the result schema,
drops empty rows and rejects equal output labels at the same evaluation
timestamp, then sorts by Prometheus's `labels.Compare` and block order without
copying samples. DataFusion's struct label order is not that comparator:
missing labels are `""`, which can reverse sparse-label comparisons.

Explicit `sort`, `sort_desc`, `sort_by_label` and `sort_by_label_desc` set the
order of an instant result; range matrices use label-set order instead.
`Engine::prepare_async` returns a `PreparedQuery` carrying the optimized physical
template and the result-order policy from the logical root, before optimization.
`Engine::execute_async(&prepared)` resets a fresh execution tree for each run,
including protected source-owned exchanges, without selecting or planning again.
Repeated and concurrent executions do not share consumed exchange channels, and
cancellation leaves the template reusable. Sources must honor DataFusion's
execution-state reset contract. `range_query_async` uses this same path.
`instant_query_async` returns `InstantResult` according to the expression's type;
non-string results share reset and finalization, while strings need no plan.
All planning and query entrypoints honor `EngineOptions::parser`, with the
experimental [feature flags](feature-flags.md) off by default.

A root instant sort supplies the result order; a nested sort below a projection
or aggregation does not. This policy is not inferred from physical operator
types. `PreparedQuery::physical_plan()` exposes the template for inspection or raw
streaming. Raw callers must reset it before execution and handle finalization
themselves. Metrics belong to the reset execution tree, not the template.
Public `value_type` and `ValueType` classify the expression without inspecting an
ambiguous unlabelled result row. Async APIs require a Tokio runtime.

## Correctness and measurements

`labelset::finalize` rejects equal output labels at the same evaluation timestamp;
disjoint spans with equal labels remain valid. A shared borrowed row ordering
serves checks and ordinary output slicing. Disjoint time bounds avoid sample
walks, and only connected overlapping spans use a borrowed-head heap. Explicit
instant sorts use sorted indices for checking without reordering their output.
`series::coalesce` concatenates chronological disjoint evaluated spans and merges
interleaved spans by timestamp; it does not change raw-source overlap precedence.
Differential/promqltest coverage runs whole chunks,
realistic chunks and one sample per chunk, also cut into blocks. Literal
selector/range cases, staleness, overlap and modifiers remain numerical
oracles, not just parity through shared kernels. Both supported ID types and
cross-batch/mid-batch transitions need coverage. Arbitrary between-ID order
must not accidentally restore a label/ID ASC requirement.

The expected hot path is current-ID comparison, buffered chunk folding/shared
kernel evaluation and ordinary Arrow output construction, not hash probes,
cardinality-sized drains, raw-series concatenation or selector/range aggregate
state. Benchmark/profile gates need actual measurements; this shape alone
establishes neither speedup nor the five-percent noise-floor comparison.
`AGENTS.md` requires reporting unit, conformance, plan pins, benchmarks and
profile separately. Relative to the base implementation's selector/range
UDAFs under native `AggregateExec`, plan pins now show `SeriesEvalExec` for
every selection and only block ordering at the source/evaluation boundary.
Upper PromQL aggregates still use native `AggregateExec`. Unordered partial
aggregation may use DataFusion's adaptive bypass: each raw evaluated row then
serializes its per-step aggregate state, including compensation and variance
state, rather than forwarding final values. Tests force that path with low
probe thresholds. The evaluator boundary itself is a planner design change,
not a numerical expectation change.

## Non-goals

Native histograms remain unsupported: kernels branch on f64. Series crossing
partitions would require a distributed merge. This work adds neither
aggregation fusion, cardinality tables nor runtime source validators.
