# Compare benchmark revisions

Use this guide to compare execution time and heap use between two worktrees.
Complete the unit, conformance, and plan-pin checks in
[AGENTS.md](../../AGENTS.md) before measuring performance.

## Prepare both worktrees

1. Use the same benchmark fixtures and IDs on both revisions. If the base lacks
   a probe, copy only the benchmark code and adapt API names as needed.
   Preserve source IDs, grouping, query grids, numerical behavior, and execution
   resets. Keep engine implementation changes out of the baseline.
2. Record both revisions and any benchmark adaptations with the results.
3. Select the same installed Rust toolchain for both worktrees. Check the minimum
   version in each revision's `Cargo.toml`. Use the same build flags, allocator,
   and features.
4. Set absolute paths for the worktrees and separate target directories:

```sh
BEFORE=/absolute/base-worktree
AFTER=/absolute/feature-worktree
BEFORE_TARGET="$BEFORE/target"
AFTER_TARGET="$AFTER/target"
```

## Select comparable cases

1. List the available timing benchmark IDs on each revision:

```sh
(cd "$BEFORE" && CARGO_TARGET_DIR="$BEFORE_TARGET" cargo bench -p promql-engine --bench engine -- --list)
(cd "$AFTER" && CARGO_TARGET_DIR="$AFTER_TARGET" cargo bench -p promql-engine --bench engine -- --list)
```

2. Choose an ID present on both revisions. Check the fixture definitions and
   [measurement boundaries](README.md#measurement-boundaries) before comparing.
   To measure kernels instead, use `--bench kernels` throughout this procedure.
3. Set `BENCH_ID` to the chosen ID, such as
   `EngineQuery/query=rate_5m/range=range_1h/shape=1000x1000`. Pass `--exact` so
   substring matches do not run or profile unrelated cases.
4. Check the fixture and accounting helpers on both revisions:

```sh
(cd "$BEFORE" && CARGO_TARGET_DIR="$BEFORE_TARGET" cargo test -p promql-engine --test benchmark_helpers)
(cd "$AFTER" && CARGO_TARGET_DIR="$AFTER_TARGET" cargo test -p promql-engine --test benchmark_helpers)
```

## Compare execution time and allocations

1. Run the selected benchmark on the base revision and save `benchfmt` output:

```sh
(cd "$BEFORE" && CARGO_TARGET_DIR="$BEFORE_TARGET" cargo bench -p promql-engine --bench engine -- \
	-bench "$BENCH_ID" --exact -benchtime 1s -count 6 -cpu 2 -benchmem) | tee /tmp/before.txt
```

2. Run the same benchmark on the feature revision:

```sh
(cd "$AFTER" && CARGO_TARGET_DIR="$AFTER_TARGET" cargo bench -p promql-engine --bench engine -- \
	-bench "$BENCH_ID" --exact -benchtime 1s -count 6 -cpu 2 -benchmem) | tee /tmp/after.txt
```

3. Compare both runs with [`benchstat`](https://pkg.go.dev/golang.org/x/perf/cmd/benchstat):

```sh
benchstat /tmp/before.txt /tmp/after.txt
```

4. Repeat suspicious results in alternating revision order on an otherwise idle
   machine. Paste raw `benchstat` output and exact `before` and `after` commit
   links when reporting results.

## Compare heap use

1. Choose a memory case from [`memory.rs`](memory.rs). Set `MEMORY_CASE` to its
   name and verify that both revisions use the same fixture.
2. Run the case on both revisions:

```sh
(cd "$BEFORE" && CARGO_TARGET_DIR="$BEFORE_TARGET" cargo bench -p promql-engine --bench memory -- "$MEMORY_CASE") > /tmp/memory-before.txt
(cd "$AFTER" && CARGO_TARGET_DIR="$AFTER_TARGET" cargo bench -p promql-engine --bench memory -- "$MEMORY_CASE") > /tmp/memory-after.txt
```

3. Compare each case with the same accounting code. Separate allocator traffic
   from retained capacity when reporting results. Neither measures bytes copied.
   The [heap accounting reference](README.md#heap-accounting) defines each reading.

## Inspect profiles

1. Collect CPU and sampled allocation profiles on the selected timing benchmark:

```sh
(cd "$AFTER" && CARGO_TARGET_DIR="$AFTER_TARGET" cargo bench -p promql-engine --bench engine -- \
	-bench "$BENCH_ID" --exact -benchtime 10s -count 1 -cpu 2 -benchmem \
	-cpuprofile /tmp/cpu.pb.gz -memprofile /tmp/mem.pb.gz)
```

2. Inspect the CPU and allocation profiles with Go's pprof:

```sh
go tool pprof -top -cum -nodecount=40 /tmp/cpu.pb.gz
go tool pprof -sample_index=alloc_space -top -cum -nodecount=40 /tmp/mem.pb.gz
```

3. Check which functions account for the measured change. Distinguish source
   construction from evaluation and result processing. If the relevant function
   has too few samples, increase `-benchtime` or lower `-memprofilerate`.
4. For a heap profile, run one memory case per process:

```sh
(cd "$AFTER" && CARGO_TARGET_DIR="$AFTER_TARGET" MEMORY_HEAP_DIR=/tmp/heap \
	cargo bench -p promql-engine --features heap-profile --bench memory -- "$MEMORY_CASE")
find /tmp/heap -name '*.pb' -print
```

5. Inspect live allocations in the matching heap profile:

```sh
go tool pprof -sample_index=inuse_space -top -cum <path-to-heap-profile.pb>
```

Heap symbolization caches debug information after the first dump, which affects
later heap readings in the same process. Report the observed hot path with the
measurements, subject to the [measurement limits](README.md#measurement-limits).
