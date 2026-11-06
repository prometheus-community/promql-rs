# Working on promql-rs

## Build tools

Run builds and checks through the root `mise.toml` tasks: `mise run build`,
`mise run test`, `mise run lint`, `mise run fmt-check`,
`mise run build-benches`, and `mise run check-upstream`. Compile tasks use
mbx's shared compiler cache; keep target directories separate as described below.
`rust-toolchain.toml` owns the compiler pin. See README.md for installation,
argument forwarding, and benchmark commands.

## The spec

Prometheus at the commit named in `promql-conformance/testdata/prometheus/UPSTREAM.md`
is the spec; a local checkout's HEAD is not. Port by naming the upstream function
(`extrapolatedRate`, `VectorBinop`) in the commit and the doc comment, so a reviewer
reads both side by side. Where Rust and Go disagree on floats, Go wins, with a comment
at the spot.

## Five gates per engine feature

A feature moves through the gates in order, and a gate is green before the next opens.
Each gate catches a different failure, so a skipped gate hides which layer broke. Report
per gate. A pull request that stops before gate 5 names the gate it reached and stays a
draft.

1. **Unit.** The steps of `.github/workflows/ci.yml` before the promqltest allowlist
   check, run locally. The workflow is the list: a copy of it here went stale and
   missed `cargo fmt --check`. Green: all pass, and every new kernel or planner branch
   has a test that fails without it.
2. **Conformance.** The promqltest corpus in `promql-conformance`; the doc comment in
   `tests/promqltest.rs` says what turns CI red. Re-bless with
   `PROMQL_PROMQLTEST_BLESS=1` and commit `SUPPORTED.toml` and `UNSUPPORTED.md` together
   with the code. Green: the gate passes without `BLESS`, and the pass count rose by the
   evals the feature targets. A lower count is a regression to fix.
3. **Plan pins.** `promql-engine/tests/plan.rs` pins the planner's output as insta
   snapshots in `promql-engine/tests/plan/snapshots/`. A changed pin is a design change:
   re-bless it with `cargo insta review` (`INSTA_UPDATE=always cargo test` without
   cargo-insta) in its own commit whose message says why the shape moved. Green: every
   changed pin is explained in that commit.
4. **Benchmarks.** Timing suites (`promql-common`, `promql-parser`, and `promql-engine`'s
   `kernels` and `engine`) plus peak-heap accounting (`promql-engine/benches/memory.rs`);
   see `promql-engine/benches/compare.md` for baseline comparison with `benchstat`. Green:
   no move beyond the five percent noise floor in the wrong direction, and the feature has
   a bench at realistic size.
5. **Profile.** Run the feature's bench with `-cpuprofile` and `-memprofile`
   (`promql-engine/benches/README.md`) and read the profile. Green: the hot path is the
   one `docs/engine.md` predicts. Anything else is a finding to fix or to write down.

## Performance and benchmarks

- Write all timing benchmarks with [`benchfmt.rs`](https://github.com/bwplotka/benchfmt.rs)
  using `benchfmt::Suite` and `benchfmt::CountingAlloc`, or `benchfmt::ProcessAlloc`
  when Tokio worker threads allocate, in `harness = false` `[[bench]]` targets with
  `bench = false` on `[lib]` and `[[bin]]` targets. Benchmarks emit Go
  [Benchmark Data Format](https://go.googlesource.com/proposal/+/master/design/14313-benchmark-format.md)
  lines with `ns/op`, `B/op`, and `allocs/op`, plus `MB/s` when `b.set_bytes` is set,
  for [`benchstat`](https://pkg.go.dev/golang.org/x/perf/cmd/benchstat). Do not use
  Criterion or custom timing output formats. Keep `promql-engine/benches/memory.rs` for
  single-pass peak-heap and retained-Arrow-buffer accounting.
- Name benchmarks with `/key=value` sub-benchmark dimensions such as
  `EngineQuery/query=rate_5m/range=range_1h/shape=1000x1000`, call `b.report_allocs()`
  and `while b.keep_running()`, and include a `Recommended CLI invocation:` doc comment
  at the top of each benchmark file.
- Performance improvements and hot-path changes require running benchmarks before and
  after the change, for example `cargo bench -p promql-engine --bench engine -- -bench '^BenchmarkEngine' -benchtime 1s -count 6 -cpu 2 -benchmem | tee ${bench}.txt`,
  and comparing with `benchstat before.txt after.txt`.
- Always output benchmark results in raw `benchstat` format in the PR body and review
  comments, and always include links to the exact `before` and `after` commits that the
  benchmark refers to. Rerun benchmarks and update the commit links whenever addressing
  review comments that touch benchmarked code or benchmarks.
- If a subset of benchmark results shows a regression, address it or explain in the PR
  body why that case is not important.
- Reuse allocations in hot paths where possible, such as `Vec::clear`, scratch buffers,
  and reusable Arrow builders. When reusing buffers exposed through APIs or traits,
  document lifetime and copy requirements on the trait or type definition.

## Engine design

- Arrow in, Arrow out: every operator takes `RecordBatch`es and emits `RecordBatch`es,
  and a series stays a row from the store to the result. `docs/series-source.md` is the
  store contract and `docs/engine.md` the engine's shape; read both before adding an
  operator.
- Chunk rows flow end to end: a series may arrive as several rows, and the engine folds
  them where it evaluates. The whole series never exists as one array.
- An operator carries the name of the Prometheus engine function it ports.

## Comments and docs

Write intention, constraints, and what a plausible alternative gets wrong. A sentence a
reader could derive from the adjacent code goes.

## Worktrees

- One absolute `CARGO_TARGET_DIR` per worktree, deleted with it. The conformance binary
  bakes its testdata path in at compile time, so a shared dir reports another
  worktree's results, and a dir reaches 10 to 20 GB.
- Park unfinished work in a WIP commit; the stash stack is shared by every worktree.
- One concern per branch, stacked on `main` or on the branch it needs. Rebase your own
  branch and push it with `--force-with-lease`.
