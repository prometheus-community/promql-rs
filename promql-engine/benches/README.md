# promql-engine benchmarks

`kernels` times numerical kernels/accumulators directly and the sole series
evaluator through preplanned DataFusion execution. `engine` measures whole
queries and isolated preplanned execution through DataFusion, and
`memory` measures peak heap of single queries instead of time. The doc comment
at the top of each file says how to compare two states of the code. `PROMQL_BENCH_PARTS=<n>` spreads
`engine`'s synthetic series over n store partitions.

## Measurement boundaries

| Family | Before measurement | Inside measurement | Output retention |
|---|---|---|---|
| `engine/query`, `engine/contiguous` | Runtime, engine registrations, synthetic stored input | Parse, source selection/clipping/chunk repacking, logical and physical planning, DataFusion execution, API collection/validation/canonical ordering | Complete result returned by the API |
| `engine/plan` | Runtime, engine, stored input | Parse, source selection and logical planning; not physical lowering or execution | Logical plan |
| `kernels/series_eval/*` | Runtime, selected mandatory-ID source batches, physical plan, task context | Create execution stream, run selector/range executor, build/drain Arrow outputs | Current batch only; no API postprocessing |
| `engine/execute_preplanned` | Runtime, engine, stored input, selected ID-contiguous input batches, optimized physical plan, task context | Reset execution state, create a fresh execution stream, execute the real DataFusion plan, construct and drain Arrow outputs | Drop each output batch; no result collection or API postprocessing |
| `engine/focused_collect` | Runtime, engine, selected raw input and prepared query | Prepared API execution (including reset), validation/canonical ordering and collection | Full result; planning/source selection excluded |
| Other `engine/focused_*` | Runtime, engine, selected input/stream description and prepared physical template | Reset, stream creation, execution and output construction/drain; `focused_streaming` also generates raw chunks as polled | Current output batch only |
| `memory` resident cases | Runtime warm-up, engine, resident input | Plan, execute and collect, including API postprocessing | Complete result held through peak/held readings |
| `memory` streamed cases | Runtime warm-up, engine, source description/schema (resident input for `focused/churn_*` only) | Plan, lazily generate source batches as polled where applicable, execute and drain | At most the current output batch in the consumer; no result collection |

The preplanned execution family covers UInt64 and binary16 IDs over the
existing 10k-series synthetic fixture, with four-sample chunks and one block.
It measures a one-step selector and `timestamp(selector)`, plus 31-step
`rate` and `min_over_time`. The evaluation time ends 15s after the last sample,
so the one-step lookup cannot finish from a sample reaching its time. Throughput
here counts selected **series**, not stored samples. Its shapes differ from the
15-minute-block `engine/contiguous` family; do not subtract the two families'
numbers as though only planning differed. Input generation/repacking is excluded
from this family, but generating output arrays and dropping them is execution.

The streamed ID-contiguous memory cases live under `streamed/contiguous/{uint64,binary16}`:

- `100k/chunk4_x1024/{selector,timestamp}_one_step_tail`: 20 input samples at
  15s (last at 285s), one evaluation at 300s with lookback `(0s, 300s]`.
  Each identity finishes at the next ID or EOF; this exercises retained
  one-step state rather than a step already resolved by the last pushed sample.
- `10k/chunk4_x1024/{rate_5m,min_5m}_31_steps_tail`: 64 samples at 15s,
  evaluation from 60s through 960s at 30s, a 5-minute window and a last
  input sample at 945s. The current ID's range history/sweeps and its results
  remain live together until the ID closes.

Every `Generated` constructor appends a non-null `series_id`; default and
resident sources use UInt64, while `streamed_with_ids` also supports binary16.
IDs encode the immutable full synthetic identity before selection, not a filtered
row position or projected labels. It emits all chunks of one ID before the next,
chronologically, with only one batch's descriptors/Arrow arrays built at a time.
Neither labels nor IDs are sorted, and batch cuts may split an ID. This fixture
is not a general store: it assumes the benchmark's unfiltered metric selection, uses
one partition and one block. Ordinary constructors do not exercise arbitrary
binary widths, staleness, empty/sparse output, long labels, refolded ranges or
block transitions. The scaling probes below add only their stated shapes;
these are not claims of general store coverage.

### Heap versus reservations

`memory` separates three quantities; none is RSS or a DataFusion reservation:

- **Logical payload:** visible sample pairs at 16 bytes each plus UTF-8 label
  value bytes. Labels repeated on separate rows count per occurrence. Offsets,
  views, block columns, spare capacities and headers are not logical payload.
  Streaming cases accumulate payload **throughput**, not simultaneously retained
  results. `largest logical batch` uses the same definition.
- **Unique retained Arrow capacity:** all result columns' buffer capacities,
  including offsets, views, validity and hidden portions of slices, deduplicated
  by backing base address across the entire held answer. This is retained, not
  exclusive, ownership: a buffer can also belong to a source. The owner walk is
  after allocator readings so its maps/ArrayData do not pollute query peaks.
  Schemas, headers, Arc control blocks and Vec overhead are excluded; custom
  external buffers expose no capacity and appear in `external owners` instead
  of a made-up byte estimate. Streaming consumers retain zero result owners
  after drain; no address set survives across batches (addresses can be reused).
- **Allocator readings:** requested live allocation sizes, not size-class
  overhead. `process baseline` is the entire pre-query live process, including
  runtime/engine and resident input/fixture metadata, not isolated input size.
  `incremental peak` is high-water live bytes minus that baseline; planning and
  streamed source generation are included. `held after` is live growth on return,
  with resident results still held, before accounting/profile helpers run.
  `allocation calls` and `requested traffic` are successful alloc/alloc_zeroed/
  realloc operations and full requested sizes; a realloc counts its new size
  even in place. Traffic is neither bytes copied nor unique retained bytes.

This replaces the old `result MiB` estimate, which repeatedly charged shared
label backing for row slices. Compare revisions with the **same accounting
helper**, not with historical `result MiB` values. Tests in
`tests/benchmark_helpers.rs` cover reordered row slices, cloned batches,
independent allocations, nullable buffers and aliasing across columns.

Reservations instead estimate accounted owners at executor checkpoints;
reservation peaks can differ from allocator peaks, are not guaranteed
preallocation and do not establish a hard heap bound. The optional streamed
heap dump is taken at the fullest **output-batch** observation, which can miss
an earlier allocation peak while an ID is accumulating or finalizing.

## Comparing revisions

Use identical fixtures and benchmark IDs on both revisions, and a separate
absolute `CARGO_TARGET_DIR` for each worktree. Save and compare Criterion
baselines as the file doc comments describe. Compare memory cases individually
under the same allocator, features and environment. `engine/contiguous` and
`streamed/contiguous` are new families, not renamed baselines. Existing families
now also supply mandatory IDs; record this source-generation change when
comparing them.

The base implementation evaluates selectors and ranges with UDAFs under native
`AggregateExec`. In `kernels`, `selector/{eval_series,apply}` and `range/{range_function,apply}`
are replaced by `series_eval/{selector,selector_apply,range,range_apply}`.
Their plain/stale/pinned, per-series and 1k-series fixtures remain, but timing
now includes DataFusion stream overhead. Do not treat these as directly
comparable UDAF microbenchmarks or infer an executor speedup from renamed IDs.

## Scaling and allocation probes

Existing IDs and data fixtures stay intact. Raw preplanned execution now resets
DataFusion state before **every** execution, including the first. Use that same
reset in the pre-fix copy: cloning an Arc does not reset a stateful exchange.
Current benches prepare with `Engine::prepare_async`; raw execution uses
`prepared.physical_plan().clone()` plus `reset_plan_states`, while API collection
uses `Engine::execute_async(&prepared)`.

| Family | Small / realistic shape | What it discriminates |
|---|---|---|
| `aggregate/singleton_spans/{ascending,descending}` | 1,024 / 8,192 singleton contributions, one sum group | Legal descending endpoints versus ascending control; 8x size distinguishes amortized lane growth from repeated front movement. Only update/allocation/drop is timed, not source/planner or final evaluate. |
| `engine/focused_sparse/sum` | 512 groups x 8 blocks / 4,096 x 24, two raw identities per group | High-cardinality sparse lanes across many blocks: each 15-minute block has 30 steps but each group answers only its first. Query grids are 240/720 steps; raw IDs stay distinct and stable across blocks. Timed execution, not API collection. |
| `engine/focused_collect/{ordered_wide,shuffled_wide}` | 1,024 / 16,384 one-step identities, 12 long label values plus metric/identity | Collection sorting and one-row sliced-batch header churn. Same payload, deterministic odd-stride shuffle. `focused_drain/shuffled_wide/16384` is the raw-execution control, not an exact subtraction oracle. |
| `engine/focused_collect/empty_stale_wide` | 16,384 wide-label identities | Cost of empty folds/output with selected labels present. Latest input is exact StaleNaN, not a filtered-out source. |
| `engine/focused_streaming/{whole,second,stale}` | Four series x 131,072 samples at 1s; query only 0..60s, 61 steps | Large physical chunks with unreachable future tails. `second` sends four samples then a large second chunk; `stale` has the marker at sample 8. Source generation is timed and allocates too; these measure combined streaming cost/peak, not exclusive history capacity. |
| `engine/focused_churn/rate_32steps` | 1,024 / 8,192 identities, 32 samples, chunks of four | Fresh per-series scratch and compact-result/output-builder churn. Resident selected input excludes repacking. Memory probes compare one output point with 31 per identity at the same raw shape. |

`memory -- focused/` runs nine matching realistic probes. Wide/sparse/empty
cases collect; long-chunk cases generate lazily and drain. The two churn cases
use resident **input** and stream their **output**, avoiding generator traffic
in their allocation comparisons. Sources are one-partition fixtures; many-block
sparse aggregation does not force DF55's **unordered** adaptive partial bypass.
All nonempty timing fixtures check point counts before timing; empty cases check
zero points. Small fixture tests also pin labels, sum values, future-tail
retention and stale bits. These assertions are not performance measurements.

### Bounded serial comparison

After the correctness gates, use identical benchmark fixtures on both revisions.
If the base revision lacks a probe, port only its benchmark harness, adapting API
spelling without changing source identity, grouping, grids, numerical semantics
or the execution-reset boundary. Keep implementation changes out of the baseline.
Run `tests/benchmark_helpers.rs` to check fixture and accounting invariants.
Record both revisions and any harness adaptations alongside the measurements.
Use separate absolute target directories and the same toolchain and build flags:

```sh
BEFORE=/absolute/base-worktree
AFTER=/absolute/feature-worktree
BEFORE_TARGET=/absolute/base-worktree/target
AFTER_TARGET=/absolute/feature-worktree/target
export PATH="$HOME/.rustup/toolchains/1.94.0-x86_64-unknown-linux-gnu/bin:$PATH"
unset PROMQL_BENCH_PARTS

# Helpers/bench compilation are gate 1; no bless is needed.
(cd "$AFTER" && CARGO_TARGET_DIR="$AFTER_TARGET" cargo test -p promql-engine --test benchmark_helpers)
(cd "$AFTER" && CARGO_TARGET_DIR="$AFTER_TARGET" cargo build -p promql-engine --benches)

# Gate 4: 17 focused IDs, not the full existing query matrix.
(cd "$BEFORE" && CARGO_TARGET_DIR="$BEFORE_TARGET" cargo bench -p promql-engine --bench kernels -- \
  'aggregate/singleton_spans/' --warm-up-time 0.2 --measurement-time 1 --sample-size 10 --noplot --save-baseline audit-before)
(cd "$BEFORE" && CARGO_TARGET_DIR="$BEFORE_TARGET" cargo bench -p promql-engine --bench engine -- \
  'engine/focused_' --warm-up-time 0.2 --measurement-time 1 --sample-size 10 --noplot --save-baseline audit-before)
# Copy Criterion data only; NEVER share compiled target artifacts.
mkdir -p "$AFTER_TARGET/criterion"
cp -a "$BEFORE_TARGET/criterion/." "$AFTER_TARGET/criterion/"
(cd "$AFTER" && CARGO_TARGET_DIR="$AFTER_TARGET" cargo bench -p promql-engine --bench kernels -- \
  'aggregate/singleton_spans/' --warm-up-time 0.2 --measurement-time 1 --sample-size 10 --noplot --baseline audit-before)
(cd "$AFTER" && CARGO_TARGET_DIR="$AFTER_TARGET" cargo bench -p promql-engine --bench engine -- \
  'engine/focused_' --warm-up-time 0.2 --measurement-time 1 --sample-size 10 --noplot --baseline audit-before)

# Same allocator/features/fixture/accounting code, no Criterion baseline here.
(cd "$BEFORE" && CARGO_TARGET_DIR="$BEFORE_TARGET" cargo bench -p promql-engine --bench memory -- focused/) > /tmp/audit-memory-before.txt
(cd "$AFTER" && CARGO_TARGET_DIR="$AFTER_TARGET" cargo bench -p promql-engine --bench memory -- focused/) > /tmp/audit-memory-after.txt
```

This is a short screening run (17 x 1s requested measurement per revision,
plus warmup, Criterion analysis, fixture setup and compilation), **not** evidence
that all differences inside/outside 5% are settled. Repeat suspicious IDs at
3s/20 samples, in alternating revision order on an otherwise idle machine;
retain estimates/confidence intervals. Criterion still enters untimed setup in
other benchmark functions even with a filter. Do not extend the full matrix
until a finding warrants it.

### Focused profiles and limitations

Gate 5 targets, one exact ID at a time (same pinned toolchain/target as above):

```sh
CARGO_TARGET_DIR="$AFTER_TARGET" cargo bench -p promql-engine --bench kernels -- \
  'aggregate/singleton_spans/descending/8192$' --profile-time 15
CARGO_TARGET_DIR="$AFTER_TARGET" cargo bench -p promql-engine --bench engine -- \
  'engine/focused_collect/shuffled_wide/16384$' --profile-time 15
# Follow a streaming/churn finding with one of:
CARGO_TARGET_DIR="$AFTER_TARGET" cargo bench -p promql-engine --bench engine -- \
  'engine/focused_streaming/second/4x131072$' --profile-time 15
CARGO_TARGET_DIR="$AFTER_TARGET" cargo bench -p promql-engine --bench engine -- \
  'engine/focused_churn/rate_32steps/8192$' --profile-time 15
find "$AFTER_TARGET/criterion" -name profile.pb -print
# go tool pprof -top -cum -nodecount=40 <the matching profile.pb>
```

Inspect descending lane movement/reallocation; wide-result comparison, slice
headers and Arrow construction; long-chunk history copies versus source
construction; and per-identity allocation/output copying. For sparse or empty
findings profile respectively
`engine/focused_sparse/sum/4096groups_24blocks$` or
`engine/focused_collect/empty_stale_wide/16384$` with the same command.

This suite screens scratch reuse, result-copy, empty-fold and
shuffled-collection hypotheses without assuming they merit optimization. It
cannot attribute allocation traffic solely to history/results, establish a
window-only capacity bound, measure bytes copied, prove cooperative
stall/cancellation bounds, or validate adaptive partial bypass. Those need
internal capacity tests, allocation/profile attribution and scheduler/bypass
integration tests. RuntimeEnv/memory-pool injection, samples-only projection
fusion, alternate refolding kernels, partition scaling and binary matching
remain separate measurements, not inferred wins. No benchmark/profile gate is
green until actual comparable measurements and profiles have been read.

## Profiling

Every criterion group runs pprof-rs as its profiler, so `--profile-time`
profiles instead of measuring:

```sh
cargo bench -p promql-engine --bench kernels -- --profile-time 15 'series_eval/selector_apply'
```

The profile lands in
`target/criterion/<group>/<benchmark>/profile/profile.pb`, one directory
per benchmark id with `/` replaced by `_`. Open it with Go's pprof:

```sh
go tool pprof -http=: target/criterion/series_eval_selector_apply/1000x1000/profile/profile.pb
go tool pprof -top -nodecount=40 target/criterion/series_eval_selector_apply/1000x1000/profile/profile.pb
```

It samples at 100 Hz, so 15 s gives about 1500 samples; profile longer
for a bench whose hot path is under a few percent.

For ad-hoc profiling of a whole process on macOS, including threads the
bench does not own, `samply record` on the bench binary opens the result
in the Firefox Profiler.

## Heap profiles

`memory` counts live bytes under its own global allocator. With the
`heap-profile` feature that allocator wraps jemalloc with profiling on,
and `MEMORY_HEAP_DIR` makes it write a pprof heap profile per case:

```sh
MEMORY_HEAP_DIR=/tmp/heap cargo bench -p promql-engine --features heap-profile \
  --bench memory -- streamed/30d_1000/chunk120_x360/selector
go tool pprof -sample_index=inuse_space -top -cum /tmp/heap/streamed_30d_1000_chunk120_x360_selector.pb
```

Run one case per process; the doc comment in `memory.rs` says why.
