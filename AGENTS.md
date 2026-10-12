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
4. **Benchmarks.** `promql-engine/benches` (`kernels`, `engine`, `memory`); each file's
   doc comment says how to take a baseline on the base branch and compare. Green: no
   move beyond the five percent noise floor in the wrong direction, and the feature has
   a bench at realistic size.
5. **Profile.** Run the feature's bench with `--profile-time`
   (`promql-engine/benches/README.md`) and read the profile. Green: the hot path is the
   one `docs/engine.md` predicts. Anything else is a finding to fix or to write down.

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
