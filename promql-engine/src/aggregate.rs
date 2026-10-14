//! PromQL aggregation as a DataFusion aggregate function:
//! `promql_aggregate(samples, 'sum') … GROUP BY <label columns>`.
//!
//! Every input series is already on the query's step grid — evaluation
//! emits at `start + i * step` — so a timestamp *is* an array index, and
//! the running state can be dense lanes over each group's evaluated span,
//! not the entire query grid. Only output timestamps define those spans:
//! offsets shift them and a pinned `@` can cover the full grid. A one-step
//! query instead shares flat lanes across group indices, avoiding per-group
//! buffers. This ports Prometheus's `groupedAggregation` at 83962c35 using
//! the same numerical kernels, with an independent accumulator
//! per step and no loop-carried dependency; the kernels and their
//! bit-fidelity obligations are in [`crate::math`].
//!
//! The lanes are paired with a `seen` bitmap rather than a sentinel
//! value, because a step no series reached is absent from the output, as
//! in Prometheus, and that is not the same as a step that summed to zero.
//!
//! Geometric left headroom keeps descending span growth amortized linear,
//! without refolding or sorting contributions. Native block-prefix emission
//! frees completed spans; one-step prefix drains retain flat float capacity
//! and continue charging it. DataFusion determines how many groups are live:
//! its stream mode, input batch size and spill path
//! can change that lifetime, so span-local allocation alone is not an RSS bound.
//! Timestamp-bearing partial states and list offsets stay unchanged.
//!
//! The operator is a literal argument rather than eight registered
//! functions: one name to register and one plan to serialize, without
//! distinct function identities for the same lane machinery.

use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, BooleanBufferBuilder, Float64Array, ListArray,
    StructArray, TimestampMillisecondArray,
};
use datafusion::arrow::buffer::{BooleanBuffer, OffsetBuffer, ScalarBuffer};
use datafusion::arrow::datatypes::{
    DataType, Field, FieldRef, Fields, Float64Type, TimestampMillisecondType,
};
use datafusion::arrow::util::bit_chunk_iterator::BitChunks;
use datafusion::arrow::util::bit_iterator::BitIndexIterator;
use datafusion::common::{plan_err, ScalarValue};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::utils::format_state_name;
use datafusion::logical_expr::{
    lit, Accumulator, AggregateUDF, AggregateUDFImpl, EmitTo, Expr, GroupsAccumulator, Signature,
    Volatility,
};
use datafusion::physical_expr::expressions::Literal;
use promql_parser::token::ItemType;

use crate::math::{self, Mean};
use crate::params::step_count;
use crate::series;

pub const NAME: &str = "promql_aggregate";

/// The aggregation operators this function implements. PromQL's others
/// (`topk`, `quantile`, `count_values`, …) produce per-series or
/// per-value output and get their own treatment later.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Op {
    Sum,
    Avg,
    Count,
    Min,
    Max,
    Group,
    Stddev,
    Stdvar,
}

impl Op {
    /// Dispatch is on the variant, not the text, because `ItemType`'s
    /// `Display` is not injective: `SumDesc` also prints `sum`, which
    /// would let a series-description token pick a kernel.
    pub fn from_token(op: ItemType) -> Option<Op> {
        Some(match op {
            ItemType::Sum => Op::Sum,
            ItemType::Avg => Op::Avg,
            ItemType::Count => Op::Count,
            ItemType::Min => Op::Min,
            ItemType::Max => Op::Max,
            ItemType::Group => Op::Group,
            ItemType::Stddev => Op::Stddev,
            ItemType::Stdvar => Op::Stdvar,
            _ => return None,
        })
    }

    /// The inverse of [`Op::as_str`], for reading the operator back off
    /// a planned `promql_aggregate` call.
    pub fn parse(s: &str) -> Option<Op> {
        Some(match s {
            "sum" => Op::Sum,
            "avg" => Op::Avg,
            "count" => Op::Count,
            "min" => Op::Min,
            "max" => Op::Max,
            "group" => Op::Group,
            "stddev" => Op::Stddev,
            "stdvar" => Op::Stdvar,
            _ => return None,
        })
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Op::Sum => "sum",
            Op::Avg => "avg",
            Op::Count => "count",
            Op::Min => "min",
            Op::Max => "max",
            Op::Group => "group",
            Op::Stddev => "stddev",
            Op::Stdvar => "stdvar",
        }
    }
}

/// The most steps one query may evaluate.
///
/// Deliberately not Prometheus's 11,000-point cap (`web/api/v1/api.go`),
/// which bounds an HTTP response to a graph rather than an engine a rule
/// evaluator calls. This caps one dimension only, keeping a single
/// group's grid to about 25MB; group count is whatever the data has, so
/// the guard against a query outgrowing the machine is a bounded
/// [`MemoryPool`](datafusion::execution::memory_pool::MemoryPool) on the
/// session, fed by `Grouped::size`.
pub const MAX_STEPS: usize = 1_000_000;

/// The query's step grid, which is what makes a timestamp an index.
///
/// `name` is the operator the messages speak for: two functions walk the
/// same grid, and a complaint about a sample has to name the one the
/// user's query reached.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Grid {
    name: &'static str,
    start_ms: i64,
    step_ms: i64,
    len: usize,
}

impl Grid {
    pub(crate) fn new(
        name: &'static str,
        start_ms: i64,
        end_ms: i64,
        step_ms: i64,
    ) -> Result<Self> {
        if step_ms <= 0 {
            return Err(DataFusionError::Execution(format!(
                "{name}: step must be positive, got {step_ms}ms"
            )));
        }
        // The planner rejects an oversized grid too; again here because
        // this function is registered and reachable from SQL.
        let count = step_count(start_ms, end_ms, step_ms);
        if count > MAX_STEPS as i128 {
            return Err(DataFusionError::Execution(format!(
                "{name}: {start_ms}..{end_ms} every {step_ms}ms is {count} steps, more than the {MAX_STEPS} this engine allows"
            )));
        }
        Ok(Self {
            name,
            start_ms,
            step_ms,
            len: count as usize,
        })
    }

    pub(crate) fn len(&self) -> usize {
        self.len
    }

    pub(crate) fn timestamp(&self, index: usize) -> i64 {
        self.start_ms + index as i64 * self.step_ms
    }

    fn index(&self, ts: i64) -> Result<usize> {
        // Reachable from SQL with any timestamp, so a sample far enough
        // below the grid start to wrap `i64` is off the grid, not a panic.
        let off_grid = || {
            DataFusionError::Execution(format!(
                "{}: sample at {ts}ms is not on the step grid {}..{} every {}ms",
                self.name,
                self.start_ms,
                self.timestamp(self.len.saturating_sub(1)),
                self.step_ms
            ))
        };
        let offset = ts.checked_sub(self.start_ms).ok_or_else(off_grid)?;
        let index = offset / self.step_ms;
        if offset < 0 || offset % self.step_ms != 0 || index as usize >= self.len {
            return Err(off_grid());
        }
        Ok(index as usize)
    }

    /// Split ascending on-grid timestamps into maximal runs of
    /// consecutive grid positions, calling `run(grid index, offset into
    /// `timestamps`, len)` once per run.
    ///
    /// Stepping by `step_ms` finds the run edges without a division per
    /// sample; only the ends of a run go through [`Grid::index`], which is
    /// what proves the whole run is on the grid and inside it. A step past
    /// the end of time cannot be consecutive with anything, so it opens a
    /// run and `index` rejects it there.
    ///
    /// Ascending order is the store's obligation, but both callers are
    /// reachable from SQL over any list column, so a breach has to be an
    /// error rather than a run written at the wrong step.
    pub(crate) fn runs(
        &self,
        timestamps: &[i64],
        mut run: impl FnMut(usize, usize, usize),
    ) -> Result<()> {
        self.try_runs(timestamps, |index, from, len| {
            run(index, from, len);
            Ok(())
        })
    }

    /// Span-local writes can fail even when the global grid position is valid:
    /// a malformed SQL list may leave its endpoint-derived span before the
    /// timestamp walk encounters a later descending timestamp.
    fn try_runs(
        &self,
        timestamps: &[i64],
        mut run: impl FnMut(usize, usize, usize) -> Result<()>,
    ) -> Result<()> {
        if timestamps.is_empty() {
            return Ok(());
        }
        let mut start = 0;
        let mut index = self.index(timestamps[0])?;
        for k in 1..timestamps.len() {
            if Some(timestamps[k]) == timestamps[k - 1].checked_add(self.step_ms) {
                continue;
            }
            if timestamps[k] <= timestamps[k - 1] {
                return Err(DataFusionError::Execution(format!(
                    "{}: samples must be in ascending timestamp order, got {}ms after {}ms",
                    self.name,
                    timestamps[k],
                    timestamps[k - 1]
                )));
            }
            self.index(timestamps[k - 1])?;
            run(index, start, k - start)?;
            start = k;
            index = self.index(timestamps[k])?;
        }
        self.index(timestamps[timestamps.len() - 1])?;
        run(index, start, timestamps.len() - start)
    }
}

/// Flat `f64` lanes over one evaluated span, or over all one-step groups.
///
/// Only the lanes the operator uses are allocated. Growing, widening and
/// measuring them read `floats` without matching on the operator.
#[derive(Debug)]
struct Lanes {
    op: Op,
    /// The operator's float lanes, in the order its kernels below read
    /// them: `sum, c` for `sum`, `value, c, count` for `avg`, `mean, m2,
    /// count` for the variances, one lane for `count`/`min`/`max`, none
    /// for `group`.
    floats: Vec<Vec<f64>>,
    /// `avg`'s "already gone incremental" flag, bit-packed: one flag
    /// where its neighbours are eight bytes.
    incremental: Option<BooleanBufferBuilder>,
    /// What a position holds before anything reaches it, the test oracle's
    /// `State::new` lane-wise. Only `min` and `max` want anything but zero: they have
    /// no identity element, so a fresh position is the NaN that the
    /// first real value beats.
    fill: f64,
}

/// The float lanes over `r` as distinct mutable slices, in the order
/// [`Lanes::new`] laid them out. A free function on the field rather than
/// a method so that `avg` can borrow the bit lane at the same time.
fn runs<const N: usize>(floats: &mut [Vec<f64>], r: std::ops::Range<usize>) -> [&mut [f64]; N] {
    debug_assert_eq!(floats.len(), N);
    let mut lanes = floats.iter_mut().map(move |v| &mut v[r.clone()]);
    std::array::from_fn(|_| lanes.next().expect("lane count matches the operator"))
}

impl Lanes {
    fn new(op: Op) -> Self {
        let (count, fill) = match op {
            Op::Sum => (2, 0.0),
            Op::Avg | Op::Stddev | Op::Stdvar => (3, 0.0),
            Op::Count => (1, 0.0),
            Op::Min | Op::Max => (1, f64::NAN),
            Op::Group => (0, 0.0),
        };
        Self {
            op,
            floats: (0..count).map(|_| Vec::new()).collect(),
            incremental: (op == Op::Avg).then(|| BooleanBufferBuilder::new(0)),
            fill,
        }
    }

    /// The float lanes whole, in the order [`Lanes::new`] laid them out.
    fn read<const N: usize>(&self) -> [&[f64]; N] {
        debug_assert_eq!(self.floats.len(), N);
        let mut lanes = self.floats.iter().map(|v| v.as_slice());
        std::array::from_fn(|_| lanes.next().expect("lane count matches the operator"))
    }

    /// Room for `len` positions, the new ones holding the test oracle's
    /// `State::new` lane-wise.
    fn resize(&mut self, len: usize) {
        for lane in &mut self.floats {
            lane.resize(len, self.fill);
        }
        if let Some(incremental) = &mut self.incremental {
            incremental.resize(len);
        }
    }

    /// Widen towards earlier steps without refolding any state. Compensation,
    /// NaN payloads, signed zeros and incremental flags must survive verbatim.
    fn prepend(&mut self, count: usize, len: usize) {
        for lane in &mut self.floats {
            let old_len = lane.len();
            lane.resize(len, self.fill);
            lane.copy_within(..old_len, count);
            lane[..count].fill(self.fill);
        }
        if let Some(incremental) = &mut self.incremental {
            prepend_bits(incremental, count, len);
        }
    }

    fn drop_front(&mut self, cut: usize) {
        for lane in &mut self.floats {
            lane.drain(..cut);
        }
        if let Some(incremental) = &mut self.incremental {
            drop_bits_front(incremental, cut);
        }
    }

    /// Fold one series' values into the positions starting at `at`. The
    /// values must be consecutive steps of one group.
    fn add_run(&mut self, at: usize, values: &[f64]) {
        let r = at..at + values.len();
        match self.op {
            Op::Sum => {
                let [sum, c] = runs(&mut self.floats, r);
                math::kahan_add_each(sum, c, values)
            }
            Op::Avg => {
                let [value, c, count] = runs(&mut self.floats, r);
                math::mean_add_each(
                    value,
                    c,
                    count,
                    math::FlagLane::new(
                        self.incremental.as_mut().expect("avg has the bit lane"),
                        at,
                        values.len(),
                    ),
                    values,
                )
            }
            Op::Count => {
                let [n] = runs(&mut self.floats, r);
                math::count_add_each(n)
            }
            Op::Min => {
                let [cur] = runs(&mut self.floats, r);
                math::min_add_each(cur, values)
            }
            Op::Max => {
                let [cur] = runs(&mut self.floats, r);
                math::max_add_each(cur, values)
            }
            Op::Group => {}
            Op::Stddev | Op::Stdvar => {
                let [mean, m2, count] = runs(&mut self.floats, r);
                math::welford_add_each(mean, m2, count, values)
            }
        }
    }

    /// Fold `len` incoming partial states into positions that were
    /// already reached, starting at `at` here and at `from` in `rows`.
    fn merge_run(&mut self, at: usize, rows: &StateRows, from: usize, len: usize) {
        let d = at..at + len;
        let s = from..from + len;
        match self.op {
            Op::Sum => {
                let [sum, c] = runs(&mut self.floats, d);
                math::kahan_merge_each(sum, c, &rows.a[s.clone()], &rows.b[s])
            }
            Op::Avg => {
                let [value, c, count] = runs(&mut self.floats, d);
                math::mean_merge_each(
                    value,
                    c,
                    count,
                    math::FlagLane::new(
                        self.incremental.as_mut().expect("avg has the bit lane"),
                        at,
                        len,
                    ),
                    &rows.a[s.clone()],
                    &rows.b[s.clone()],
                    &rows.n[s],
                    math::Flags::new(rows.m, from, len),
                )
            }
            Op::Count => {
                let [n] = runs(&mut self.floats, d);
                math::count_merge_each(n, &rows.n[s])
            }
            // Merging another partition's min or max obeys the same rule
            // as taking one more value, so the add kernels serve both.
            Op::Min => {
                let [cur] = runs(&mut self.floats, d);
                math::min_add_each(cur, &rows.a[s])
            }
            Op::Max => {
                let [cur] = runs(&mut self.floats, d);
                math::max_add_each(cur, &rows.a[s])
            }
            Op::Group => {}
            Op::Stddev | Op::Stdvar => {
                let [mean, m2, count] = runs(&mut self.floats, d);
                math::welford_merge_each(
                    mean,
                    m2,
                    count,
                    &rows.a[s.clone()],
                    &rows.b[s.clone()],
                    &rows.n[s],
                )
            }
        }
    }

    /// The same run into positions nothing has reached yet, where the
    /// incoming values are taken verbatim: merging into a fresh Kahan or
    /// Welford state is not bit-identical to a copy.
    fn copy_run(&mut self, at: usize, rows: &StateRows, from: usize, len: usize) {
        let d = at..at + len;
        let s = from..from + len;
        match self.op {
            Op::Sum => {
                let [sum, c] = runs(&mut self.floats, d);
                sum.copy_from_slice(&rows.a[s.clone()]);
                c.copy_from_slice(&rows.b[s]);
            }
            Op::Avg => {
                let [value, c, count] = runs(&mut self.floats, d);
                value.copy_from_slice(&rows.a[s.clone()]);
                c.copy_from_slice(&rows.b[s.clone()]);
                count.copy_from_slice(&rows.n[s]);
                let incremental = self.incremental.as_mut().expect("avg has the bit lane");
                for k in 0..len {
                    incremental.set_bit(at + k, rows.m.value(from + k));
                }
            }
            Op::Count => {
                let [n] = runs(&mut self.floats, d);
                n.copy_from_slice(&rows.n[s]);
            }
            Op::Min | Op::Max => {
                let [cur] = runs(&mut self.floats, d);
                cur.copy_from_slice(&rows.a[s]);
            }
            Op::Group => {}
            Op::Stddev | Op::Stdvar => {
                let [mean, m2, count] = runs(&mut self.floats, d);
                mean.copy_from_slice(&rows.a[s.clone()]);
                m2.copy_from_slice(&rows.b[s.clone()]);
                count.copy_from_slice(&rows.n[s]);
            }
        }
    }

    /// Bytes of lane storage, for DataFusion's memory accounting.
    fn size(&self) -> usize {
        self.floats.capacity() * std::mem::size_of::<Vec<f64>>()
            + self
                .floats
                .iter()
                .map(|v| v.capacity() * std::mem::size_of::<f64>())
                .sum::<usize>()
            + self
                .incremental
                .as_ref()
                .map_or(0, |bits| bits.capacity() / 8)
    }
}

/// The partial states of one `merge_batch` on the way in: every row of
/// every list, flattened into four lanes, borrowed where Arrow put them.
struct StateRows<'a> {
    a: &'a [f64],
    b: &'a [f64],
    n: &'a [f64],
    m: &'a BooleanBuffer,
}

/// Set every bit in `range`.
///
/// The builder only exposes a single-bit setter, so this fills the whole
/// bytes in the middle at once and leaves the two partial ends to it.
fn set_run(bits: &mut BooleanBufferBuilder, range: std::ops::Range<usize>) {
    let (first, last) = (range.start.div_ceil(8), range.end / 8);
    if first >= last {
        for i in range {
            bits.set_bit(i, true);
        }
        return;
    }
    for i in range.start..first * 8 {
        bits.set_bit(i, true);
    }
    bits.as_slice_mut()[first..last].fill(u8::MAX);
    for i in last * 8..range.end {
        bits.set_bit(i, true);
    }
}

/// Where the stretch of positions starting at `at` that all equal `seen`
/// ends, looking no further than `at + len`.
///
/// Per bit, unlike its counterpart [`set_run`]: this is only reached from
/// a two-phase merge, where a stretch ends early far more often than a
/// whole byte of it runs on.
fn run_end(bits: &BooleanBufferBuilder, at: usize, len: usize, seen: bool) -> usize {
    let end = at + len;
    (at..end).find(|&i| bits.get_bit(i) != seen).unwrap_or(end)
}

/// Drop the first `cut` bits of a bit-packed lane, shifting the rest down
/// to index 0 — the bitmap equivalent of `Vec::drain(..cut)`.
fn drop_bits_front(bits: &mut BooleanBufferBuilder, cut: usize) {
    let all = bits.finish();
    bits.append_buffer(&all.slice(cut, all.len() - cut));
}

/// Rebase a bitmap onto a widened span, preserving its old bits.
fn prepend_bits(bits: &mut BooleanBufferBuilder, count: usize, len: usize) {
    let old = bits.finish();
    bits.append_n(count, false);
    bits.append_buffer(&old);
    bits.resize(len);
}

/// Separate spans never bridge unrelated blocks sharing an input batch.
/// Internal gaps remain dense, with a bitmap distinguishing absence from zero.
#[derive(Debug)]
struct Span {
    /// Grid index at `start`, not at the headroom's origin.
    first: usize,
    /// Active positions are `start..seen.len()`; the unused prefix buys
    /// amortized left growth without changing timestamp or accumulation order.
    start: usize,
    seen: BooleanBufferBuilder,
    lanes: Lanes,
}

impl Span {
    fn new(op: Op) -> Self {
        Self {
            first: 0,
            start: 0,
            seen: BooleanBufferBuilder::new(0),
            lanes: Lanes::new(op),
        }
    }

    fn len(&self) -> usize {
        self.seen.len() - self.start
    }

    fn include(&mut self, first: usize, last: usize) {
        if self.len() == 0 {
            self.first = first;
        }
        let end = (last + 1).max(self.first + self.len());
        let begin = first.min(self.first);
        let left = self.first - begin;
        if left > self.start {
            // Doubling the physical span bounds total movement by O(span),
            // even for descending singleton inputs. Stop at grid zero: there
            // can be no further left growth after that final, capped rebase.
            let origin = self.first - self.start;
            let prepend = (left - self.start).max(self.seen.len()).min(origin);
            let len = self.seen.len() + prepend;
            self.lanes.prepend(prepend, len);
            prepend_bits(&mut self.seen, prepend, len);
            self.start += prepend;
        }
        self.start -= left;
        self.first = begin;
        let needed = self.start + end - begin;
        if needed > self.seen.len() {
            self.lanes.resize(needed);
            self.seen.resize(needed);
        }
    }

    fn local_index(&self, index: usize, len: usize) -> Result<usize> {
        match index.checked_sub(self.first) {
            Some(at) if at + len <= self.len() => Ok(self.start + at),
            _ => Err(DataFusionError::Execution(format!(
                "{NAME}: samples must be in ascending timestamp order"
            ))),
        }
    }

    fn add_run(&mut self, at: usize, values: &[f64]) {
        set_run(&mut self.seen, at..at + values.len());
        self.lanes.add_run(at, values);
    }

    /// Fresh positions copy partial states; reached positions merge them.
    /// A merge into a fresh Kahan or Welford state is not bit-identical to a copy.
    fn merge_run(&mut self, at: usize, rows: &StateRows, from: usize, len: usize) {
        let mut k = 0;
        while k < len {
            let seen = self.seen.get_bit(at + k);
            let j = run_end(&self.seen, at + k, len - k, seen) - at;
            if seen {
                self.lanes.merge_run(at + k, rows, from + k, j - k);
            } else {
                self.lanes.copy_run(at + k, rows, from + k, j - k);
                set_run(&mut self.seen, at + k..at + j);
            }
            k = j;
        }
    }

    fn size(&self) -> usize {
        self.seen.capacity() / 8 + self.lanes.size()
    }
}

/// Chosen from the global grid, never from a group's current span.
#[derive(Debug)]
enum Storage {
    SingleStep(Span),
    Spans { spans: Vec<Span>, heap_size: usize },
}

struct GroupView<'a> {
    lanes: &'a Lanes,
    seen: &'a BooleanBufferBuilder,
    start: usize,
    len: usize,
    first: usize,
}

impl GroupView<'_> {
    fn count(&self) -> usize {
        if self.len == 1 {
            return usize::from(self.seen.get_bit(self.start));
        }
        BitChunks::new(self.seen.as_slice(), self.start, self.len)
            .iter_padded()
            .map(|word| word.count_ones() as usize)
            .sum()
    }
}

/// Validate Arrow List totals before allocating the emitted sample buffers.
fn checked_offset(rows: i32, count: usize) -> Result<i32> {
    i32::try_from(count)
        .ok()
        .and_then(|count| rows.checked_add(count))
        .ok_or_else(|| {
            DataFusionError::Execution(format!("{NAME}: output exceeds i32 list offsets"))
        })
}

/// Prometheus's `groupedAggregation` at 83962c35, driven by native group indices.
///
/// Each multi-step group owns its timestamp-derived dense span plus amortized
/// left headroom, at most another span's width. Single-step queries share flat lanes. Timestamp-bearing partial/spilled states reconstruct
/// the same spans without planner-specific offset or `@` handling. Nested heap
/// capacities are cached so DataFusion's frequent size checks stay O(1).
#[derive(Debug)]
pub struct Grouped {
    op: Op,
    grid: Grid,
    storage: Storage,
}

impl Grouped {
    pub fn new(op: Op, start_ms: i64, end_ms: i64, step_ms: i64) -> Result<Self> {
        let grid = Grid::new(NAME, start_ms, end_ms, step_ms)?;
        let storage = if grid.len == 1 {
            Storage::SingleStep(Span::new(op))
        } else {
            Storage::Spans {
                spans: Vec::new(),
                heap_size: 0,
            }
        };
        Ok(Self { op, grid, storage })
    }

    /// Filtered, null and empty rows can discover keys without allocating lanes.
    fn grow(&mut self, groups: usize) -> Result<()> {
        if groups > isize::MAX as usize / std::mem::size_of::<Span>() {
            return Err(DataFusionError::Execution(format!(
                "{NAME}: {groups} groups is more state than can be addressed"
            )));
        }
        match &mut self.storage {
            Storage::SingleStep(flat) => {
                if groups > flat.seen.len() {
                    flat.lanes.resize(groups);
                    flat.seen.resize(groups);
                }
            }
            Storage::Spans { spans, heap_size } => {
                spans.reserve(groups.saturating_sub(spans.len()));
                while spans.len() < groups {
                    let span = Span::new(self.op);
                    *heap_size += span.size();
                    spans.push(span);
                }
            }
        }
        Ok(())
    }

    /// Widen once per series, not per gapped run. Invalid endpoint bounds must
    /// be rejected before they can allocate a span; runs validates its interior.
    fn prepare(&mut self, group: usize, timestamps: &[i64]) -> Result<()> {
        let Storage::Spans { spans, heap_size } = &mut self.storage else {
            return Ok(());
        };
        let Some(&first_ts) = timestamps.first() else {
            return Ok(());
        };
        let last_ts = timestamps[timestamps.len() - 1];
        let first = self.grid.index(first_ts)?;
        let last = self.grid.index(last_ts)?;
        if last < first {
            return Err(DataFusionError::Execution(format!(
                "{NAME}: samples must be in ascending timestamp order, got {last_ts}ms after {first_ts}ms"
            )));
        }
        let span = &mut spans[group];
        let before = span.size();
        span.include(first, last);
        *heap_size = *heap_size - before + span.size();
        Ok(())
    }

    fn add_series(&mut self, group: usize, timestamps: &[i64], values: &[f64]) -> Result<()> {
        debug_assert_eq!(timestamps.len(), values.len());
        self.prepare(group, timestamps)?;
        match &mut self.storage {
            Storage::SingleStep(flat) => self.grid.try_runs(timestamps, |_, from, len| {
                debug_assert_eq!(len, 1);
                flat.add_run(group, &values[from..from + len]);
                Ok(())
            }),
            Storage::Spans { spans, .. } => {
                let span = &mut spans[group];
                self.grid.try_runs(timestamps, |index, from, len| {
                    let at = span.local_index(index, len)?;
                    span.add_run(at, &values[from..from + len]);
                    Ok(())
                })
            }
        }
    }

    fn emitted(&self, emit_to: EmitTo) -> usize {
        let groups = match &self.storage {
            Storage::SingleStep(flat) => flat.seen.len(),
            Storage::Spans { spans, .. } => spans.len(),
        };
        match emit_to {
            EmitTo::All => groups,
            EmitTo::First(n) => n.min(groups),
        }
    }

    fn view(&self, group: usize) -> GroupView<'_> {
        match &self.storage {
            Storage::SingleStep(flat) => GroupView {
                lanes: &flat.lanes,
                seen: &flat.seen,
                start: group,
                len: 1,
                first: 0,
            },
            Storage::Spans { spans, .. } => {
                let span = &spans[group];
                GroupView {
                    lanes: &span.lanes,
                    seen: &span.seen,
                    start: span.start,
                    len: span.len(),
                    first: span.first,
                }
            }
        }
    }

    fn kept(&self, groups: usize) -> Result<usize> {
        let rows = match &self.storage {
            Storage::SingleStep(flat) => {
                let count = BitChunks::new(flat.seen.as_slice(), 0, groups)
                    .iter_padded()
                    .map(|word| word.count_ones() as usize)
                    .sum();
                checked_offset(0, count)?
            }
            Storage::Spans { .. } => (0..groups).try_fold(0, |rows, group| {
                checked_offset(rows, self.view(group).count())
            })?,
        };
        Ok(rows as usize)
    }

    /// The first `groups` groups' series: one list row each, holding only
    /// the steps something reached.
    ///
    /// Gather directly through borrowed per-group bitmaps, without group Arrow
    /// arrays or sample-index vectors. Only the emitted prefix sizes the output.
    fn finish(&self, groups: usize) -> Result<ListArray> {
        let kept = self.kept(groups)?;
        let (mut ts, mut vs) = (Vec::with_capacity(kept), Vec::with_capacity(kept));
        let offsets = self.walk(groups, |span| match self.op {
            Op::Sum => {
                let [sum, c] = span.lanes.read();
                self.walk_span(span, |i, t| {
                    ts.push(t);
                    vs.push(sum[i] + c[i]);
                });
            }
            Op::Avg => {
                let [value, c, count] = span.lanes.read();
                let incremental = span
                    .lanes
                    .incremental
                    .as_ref()
                    .expect("avg has the bit lane");
                self.walk_span(span, |i, t| {
                    ts.push(t);
                    vs.push(
                        Mean {
                            value: value[i],
                            c: c[i],
                            count: count[i],
                            incremental: incremental.get_bit(i),
                        }
                        .result(),
                    );
                });
            }
            Op::Count | Op::Min | Op::Max => {
                let [lane] = span.lanes.read();
                self.walk_span(span, |i, t| {
                    ts.push(t);
                    vs.push(lane[i]);
                });
            }
            Op::Group => self.walk_span(span, |_, t| {
                ts.push(t);
                vs.push(1.0);
            }),
            Op::Stddev => {
                let [_, m2, count] = span.lanes.read();
                self.walk_span(span, |i, t| {
                    ts.push(t);
                    vs.push((m2[i] / count[i]).sqrt());
                });
            }
            Op::Stdvar => {
                let [_, m2, count] = span.lanes.read();
                self.walk_span(span, |i, t| {
                    ts.push(t);
                    vs.push(m2[i] / count[i]);
                });
            }
        });
        let entries = StructArray::new(
            series::sample_fields(),
            vec![
                Arc::new(TimestampMillisecondArray::from(ts)),
                Arc::new(Float64Array::from(vs)),
            ],
            None,
        );
        Ok(list_per_group(series::sample_item(), offsets, entries))
    }

    /// The same rows as [`Grouped::finish`], but each step's running
    /// state rather than its result: what another partition's accumulator
    /// merges in, the lane-wise restatement of the test oracle's `State::to_row`.
    ///
    /// Unused state columns get non-null zeros after the emitted sample count
    /// is known; their serialization format is unchanged by span storage.
    fn partial(&self, groups: usize) -> Result<ListArray> {
        let kept = self.kept(groups)?;
        let mut ts = Vec::with_capacity(kept);
        let lane = |used: bool| {
            if used {
                Vec::with_capacity(kept)
            } else {
                Vec::new()
            }
        };
        let mut a = lane(!matches!(self.op, Op::Count | Op::Group));
        let mut b = lane(matches!(
            self.op,
            Op::Sum | Op::Avg | Op::Stddev | Op::Stdvar
        ));
        let mut n = lane(matches!(
            self.op,
            Op::Avg | Op::Count | Op::Stddev | Op::Stdvar
        ));
        let mut m = BooleanBufferBuilder::new(if self.op == Op::Avg { kept } else { 0 });
        let offsets = self.walk(groups, |span| match self.op {
            Op::Sum => {
                let [sum, c] = span.lanes.read();
                self.walk_span(span, |i, t| {
                    ts.push(t);
                    a.push(sum[i]);
                    b.push(c[i]);
                });
            }
            Op::Avg => {
                let [value, c, count] = span.lanes.read();
                let incremental = span
                    .lanes
                    .incremental
                    .as_ref()
                    .expect("avg has the bit lane");
                self.walk_span(span, |i, t| {
                    ts.push(t);
                    a.push(value[i]);
                    b.push(c[i]);
                    n.push(count[i]);
                    m.append(incremental.get_bit(i));
                });
            }
            Op::Count => {
                let [counts] = span.lanes.read();
                self.walk_span(span, |i, t| {
                    ts.push(t);
                    n.push(counts[i]);
                });
            }
            Op::Min | Op::Max => {
                let [cur] = span.lanes.read();
                self.walk_span(span, |i, t| {
                    ts.push(t);
                    a.push(cur[i]);
                });
            }
            Op::Group => self.walk_span(span, |_, t| ts.push(t)),
            Op::Stddev | Op::Stdvar => {
                let [mean, m2, count] = span.lanes.read();
                self.walk_span(span, |i, t| {
                    ts.push(t);
                    a.push(mean[i]);
                    b.push(m2[i]);
                    n.push(count[i]);
                });
            }
        });
        let rows = ts.len();
        // The state schema declares all five children non-nullable, so a
        // lane the operator does not use is a buffer of zeros of the full
        // length rather than nulls.
        let zeros = || Float64Array::new(ScalarBuffer::from(vec![0.0; rows]), None);
        let no_flags = || BooleanArray::new(BooleanBuffer::new_unset(rows), None);
        let (a, b, n, m) = match self.op {
            Op::Sum => (
                Float64Array::from(a),
                Float64Array::from(b),
                zeros(),
                no_flags(),
            ),
            Op::Avg => (
                Float64Array::from(a),
                Float64Array::from(b),
                Float64Array::from(n),
                BooleanArray::new(m.finish(), None),
            ),
            Op::Count => (zeros(), zeros(), Float64Array::from(n), no_flags()),
            Op::Min | Op::Max => (Float64Array::from(a), zeros(), zeros(), no_flags()),
            Op::Group => (zeros(), zeros(), zeros(), no_flags()),
            Op::Stddev | Op::Stdvar => (
                Float64Array::from(a),
                Float64Array::from(b),
                Float64Array::from(n),
                no_flags(),
            ),
        };
        let entries = StructArray::new(
            state_fields(),
            vec![
                Arc::new(TimestampMillisecondArray::from(ts)),
                Arc::new(a),
                Arc::new(b),
                Arc::new(n),
                Arc::new(m),
            ],
            None,
        );
        Ok(list_per_group(state_item(), offsets, entries))
    }

    /// `kept` validated all offsets before output allocation. Borrowed views
    /// keep both storage paths on one emission path without group arrays.
    fn walk(&self, groups: usize, mut visit: impl FnMut(&GroupView<'_>)) -> Vec<i32> {
        let mut offsets = Vec::with_capacity(groups + 1);
        offsets.push(0i32);
        let mut rows = 0i32;
        for group in 0..groups {
            let view = self.view(group);
            visit(&view);
            rows = checked_offset(rows, view.count()).expect("kept validated all offsets");
            offsets.push(rows);
        }
        offsets
    }

    fn walk_span(&self, span: &GroupView<'_>, mut visit: impl FnMut(usize, i64)) {
        if span.len == 1 {
            if span.seen.get_bit(span.start) {
                visit(span.start, self.grid.timestamp(span.first));
            }
        } else {
            for step in BitIndexIterator::new(span.seen.as_slice(), span.start, span.len) {
                visit(span.start + step, self.grid.timestamp(span.first + step));
            }
        }
    }

    /// Rebase native group indices and retire emitted spans. Flat float lanes
    /// retain capacity after prefix drains, which remains charged by size().
    fn release(&mut self, emit_to: EmitTo) {
        let groups = self.emitted(emit_to);
        match &mut self.storage {
            Storage::SingleStep(flat) => {
                if matches!(emit_to, EmitTo::All) {
                    *flat = Span::new(self.op);
                } else if groups > 0 {
                    flat.lanes.drop_front(groups);
                    drop_bits_front(&mut flat.seen, groups);
                }
            }
            Storage::Spans { spans, heap_size } => {
                for span in spans.drain(..groups) {
                    *heap_size -= span.size();
                }
                if matches!(emit_to, EmitTo::All) {
                    *spans = Vec::new();
                }
            }
        }
    }

    /// The timestamps and values of a whole `samples` column, flat; the
    /// list offsets cut them into series.
    fn samples(list: &ListArray) -> Result<(&[i64], &[f64])> {
        let entries = list.values().as_struct();
        let ts = entries
            .column_by_name(series::TIMESTAMP)
            .ok_or_else(|| DataFusionError::Internal(format!("{NAME}: no timestamp column")))?
            .as_primitive::<TimestampMillisecondType>()
            .values();
        let vs = entries
            .column_by_name(series::VALUE)
            .ok_or_else(|| DataFusionError::Internal(format!("{NAME}: no value column")))?
            .as_primitive::<Float64Type>()
            .values();
        Ok((ts, vs))
    }

    #[cfg(test)]
    fn spans(&self) -> &[Span] {
        match &self.storage {
            Storage::Spans { spans, .. } => spans,
            Storage::SingleStep(_) => panic!("expected multi-step storage"),
        }
    }

    #[cfg(test)]
    fn heap_size(&self) -> usize {
        match &self.storage {
            Storage::Spans { heap_size, .. } => *heap_size,
            Storage::SingleStep(flat) => flat.size(),
        }
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
            + match &self.storage {
                Storage::SingleStep(flat) => flat.size(),
                Storage::Spans { spans, heap_size } => {
                    spans.capacity() * std::mem::size_of::<Span>() + heap_size
                }
            }
    }
}

impl GroupsAccumulator for Grouped {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        self.grow(total_num_groups)?;
        let list = values[0].as_list::<i32>();
        let (ts, vs) = Self::samples(list)?;
        let offsets = list.offsets();
        for (row, &group) in group_indices.iter().enumerate() {
            if skipped(list, opt_filter, row) {
                continue;
            }
            let (lo, hi) = (offsets[row] as usize, offsets[row + 1] as usize);
            self.add_series(group, &ts[lo..hi], &vs[lo..hi])?;
        }
        Ok(())
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let groups = self.emitted(emit_to);
        let out = self.finish(groups)?;
        self.release(emit_to);
        Ok(Arc::new(out))
    }

    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        let groups = self.emitted(emit_to);
        let out = self.partial(groups)?;
        self.release(emit_to);
        Ok(vec![Arc::new(out)])
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        total_num_groups: usize,
    ) -> Result<()> {
        self.grow(total_num_groups)?;
        let list = values[0].as_list::<i32>();
        let entries = list.values().as_struct();
        let ts = state_child(entries, series::TIMESTAMP)?
            .as_primitive_opt::<TimestampMillisecondType>()
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "{NAME}: partial state column {} is not a millisecond timestamp",
                    series::TIMESTAMP
                ))
            })?
            .values();
        let a = state_floats(entries, STATE_A)?.values();
        let b = state_floats(entries, STATE_B)?.values();
        let n = state_floats(entries, STATE_N)?.values();
        let m = state_child(entries, STATE_M)?
            .as_boolean_opt()
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "{NAME}: partial state column {STATE_M} is not boolean"
                ))
            })?;
        if m.null_count() > 0 {
            return Err(DataFusionError::Internal(format!(
                "{NAME}: partial state column {STATE_M} has nulls"
            )));
        }
        let rows = StateRows {
            a,
            b,
            n,
            m: m.values(),
        };
        let offsets = list.offsets();
        for (row, &group) in group_indices.iter().enumerate() {
            if list.is_null(row) {
                continue;
            }
            let (lo, hi) = (offsets[row] as usize, offsets[row + 1] as usize);
            if lo == hi {
                continue;
            }
            self.prepare(group, &ts[lo..hi])?;
            match &mut self.storage {
                Storage::SingleStep(flat) => self.grid.try_runs(&ts[lo..hi], |_, from, len| {
                    debug_assert_eq!(len, 1);
                    flat.merge_run(group, &rows, lo + from, len);
                    Ok(())
                })?,
                Storage::Spans { spans, .. } => {
                    let span = &mut spans[group];
                    self.grid.try_runs(&ts[lo..hi], |index, from, len| {
                        let at = span.local_index(index, len)?;
                        span.merge_run(at, &rows, lo + from, len);
                        Ok(())
                    })?;
                }
            }
        }
        Ok(())
    }

    /// DataFusion 55 can bypass unordered, high-cardinality Partial aggregation.
    /// Each input row must still serialize `aggregation`/`groupedAggregation`'s
    /// per-step state (Prometheus 83962c35), not its result or the raw value.
    /// In particular, a singleton Welford state containing infinity has a NaN m2, and
    /// adding -0 to a fresh Kahan lane need not preserve the input's zero sign.
    /// Reuse the update kernels rather than hand-seeding those corner cases.
    fn convert_to_state(
        &self,
        values: &[ArrayRef],
        filter: Option<&BooleanArray>,
    ) -> Result<Vec<ArrayRef>> {
        let list = values[0].as_list::<i32>();
        let (ts, vs) = Self::samples(list)?;
        let offsets = list.offsets();
        let mut out_offsets = Vec::with_capacity(list.len() + 1);
        out_offsets.push(0);
        let mut kept = 0;
        for row in 0..list.len() {
            if !skipped(list, filter, row) {
                kept = checked_offset(kept, (offsets[row + 1] - offsets[row]) as usize)?;
            }
            out_offsets.push(kept);
        }
        // Flat positions here are samples, not grid positions: a sparse row
        // must not allocate its entire timestamp span just to pass it through.
        let mut timestamps = Vec::with_capacity(kept as usize);
        let mut lanes = Lanes::new(self.op);
        lanes.resize(kept as usize);
        for row in 0..list.len() {
            if skipped(list, filter, row) {
                continue;
            }
            let (lo, hi) = (offsets[row] as usize, offsets[row + 1] as usize);
            self.grid.runs(&ts[lo..hi], |_, _, _| {})?;
            timestamps.extend_from_slice(&ts[lo..hi]);
            lanes.add_run(out_offsets[row] as usize, &vs[lo..hi]);
        }
        let rows = timestamps.len();
        let mut floats = lanes.floats.into_iter();
        let mut take =
            || -> ArrayRef { Arc::new(Float64Array::from(floats.next().expect("operator lane"))) };
        let zeros = || -> ArrayRef { Arc::new(Float64Array::from(vec![0.0; rows])) };
        let (a, b, n) = match self.op {
            Op::Sum => (take(), take(), zeros()),
            Op::Avg | Op::Stddev | Op::Stdvar => (take(), take(), take()),
            Op::Count => (zeros(), zeros(), take()),
            Op::Min | Op::Max => (take(), zeros(), zeros()),
            Op::Group => (zeros(), zeros(), zeros()),
        };
        let flags = lanes
            .incremental
            .map_or_else(|| BooleanBuffer::new_unset(rows), |mut bits| bits.finish());
        let entries = StructArray::new(
            state_fields(),
            vec![
                Arc::new(TimestampMillisecondArray::from(timestamps)),
                a,
                b,
                n,
                Arc::new(BooleanArray::new(flags, None)),
            ],
            None,
        );
        Ok(vec![Arc::new(list_per_group(
            state_item(),
            out_offsets,
            entries,
        ))])
    }

    fn size(&self) -> usize {
        Grouped::size(self)
    }
}

/// Rows a `FILTER` clause dropped, and rows with no series at all,
/// contribute nothing to their group.
fn skipped(list: &ListArray, filter: Option<&BooleanArray>, row: usize) -> bool {
    list.is_null(row) || filter.is_some_and(|f| f.is_null(row) || !f.value(row))
}

/// The same lanes driven as a single group.
///
/// A SQL call without grouping columns runs through [`Accumulator`] rather
/// than the grouped path (PromQL plans still group on blocks). Delegating
/// keeps the two paths from drifting into two arithmetics.
#[derive(Debug)]
pub struct OneGroup {
    groups: Grouped,
    /// Every row belongs to group zero.
    zeros: Vec<usize>,
}

impl OneGroup {
    fn new(mut groups: Grouped) -> Result<Self> {
        groups.grow(1)?;
        Ok(Self {
            groups,
            zeros: Vec::new(),
        })
    }
}

impl Accumulator for OneGroup {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.zeros.resize(values[0].len(), 0);
        self.groups.update_batch(values, &self.zeros, None, 1)
    }

    /// Borrowed reads retain state: a window frame may evaluate it again.
    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(ScalarValue::List(Arc::new(self.groups.finish(1)?)))
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.groups.size()
            + self.zeros.capacity() * std::mem::size_of::<usize>()
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::List(Arc::new(self.groups.partial(1)?))])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.zeros.resize(states[0].len(), 0);
        self.groups.merge_batch(states, &self.zeros, 1)
    }
}

/// Field names of the partial-state struct, shared by `state_fields()` on the
/// writing side and `merge_batch` on the reading side. Named once so a rename
/// fails to compile at both ends instead of mismatching at runtime across a
/// partial/final plan boundary. The test module borrows them for fixtures.
const STATE_A: &str = "a";
const STATE_B: &str = "b";
const STATE_N: &str = "n";
const STATE_M: &str = "m";

/// Fields of the partial state's list element.
fn state_fields() -> Fields {
    Fields::from(vec![
        Field::new(series::TIMESTAMP, series::timestamp_type(), false),
        Field::new(STATE_A, DataType::Float64, false),
        Field::new(STATE_B, DataType::Float64, false),
        Field::new(STATE_N, DataType::Float64, false),
        Field::new(STATE_M, DataType::Boolean, false),
    ])
}

fn state_item() -> FieldRef {
    Arc::new(Field::new(
        series::LIST_ITEM,
        DataType::Struct(state_fields()),
        false,
    ))
}

fn state_type() -> DataType {
    DataType::List(state_item())
}

/// By name and type-checked rather than downcast blind: the state crosses
/// a serialization boundary in a distributed plan, where a peer encoding
/// another version has to be a query error, not a worker panic.
fn state_child<'a>(entries: &'a StructArray, name: &str) -> Result<&'a ArrayRef> {
    entries.column_by_name(name).ok_or_else(|| {
        DataFusionError::Internal(format!("{NAME}: partial state has no {name} column"))
    })
}

fn state_floats<'a>(entries: &'a StructArray, name: &str) -> Result<&'a Float64Array> {
    state_child(entries, name)?
        .as_primitive_opt::<Float64Type>()
        .ok_or_else(|| {
            DataFusionError::Internal(format!(
                "{NAME}: partial state column {name} is not Float64"
            ))
        })
}

/// One list row per group, cut out of one flat run of entries.
fn list_per_group(item: FieldRef, offsets: Vec<i32>, entries: StructArray) -> ListArray {
    ListArray::new(
        item,
        OffsetBuffer::new(offsets.into()),
        Arc::new(entries),
        None,
    )
}

fn empty_samples() -> ScalarValue {
    ScalarValue::List(Arc::new(list_per_group(
        series::sample_item(),
        vec![0, 0],
        StructArray::new(
            series::sample_fields(),
            vec![
                Arc::new(TimestampMillisecondArray::from(Vec::<i64>::new())),
                Arc::new(Float64Array::from(Vec::<f64>::new())),
            ],
            None,
        ),
    )))
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct Aggregate {
    signature: Signature,
}

impl Default for Aggregate {
    fn default() -> Self {
        Self {
            signature: Signature::exact(
                vec![
                    series::samples_type(),
                    DataType::Utf8,
                    DataType::Int64,
                    DataType::Int64,
                    DataType::Int64,
                ],
                Volatility::Immutable,
            ),
        }
    }
}

pub fn udaf() -> AggregateUDF {
    AggregateUDF::new_from_impl(Aggregate::default())
}

/// `promql_aggregate(samples, '<op>', start, end, step)`. The step grid
/// is an argument because it is what turns a timestamp into an array
/// index; see `Grid`.
pub fn call(samples: Expr, op: Op, start_ms: i64, end_ms: i64, step_ms: i64) -> Expr {
    udaf().call(vec![
        samples,
        lit(op.as_str()),
        lit(start_ms),
        lit(end_ms),
        lit(step_ms),
    ])
}

/// The operator and the step grid, read back off the planned call.
fn from_args(args: &AccumulatorArgs) -> Result<Grouped> {
    let literal = |i: usize| {
        args.exprs
            .get(i)
            .and_then(|e| (e.as_ref() as &dyn Any).downcast_ref::<Literal>())
            .map(Literal::value)
    };
    let op = literal(1)
        .and_then(|v| match v {
            ScalarValue::Utf8(Some(s)) => Op::parse(s.as_str()),
            _ => None,
        })
        .ok_or_else(|| {
            DataFusionError::Plan(format!(
                "{NAME}: second argument must be one of sum, avg, count, min, max, group, stddev, stdvar as a string literal"
            ))
        })?;
    let grid = |i: usize, what: &str| {
        literal(i)
            .and_then(|v| match v {
                ScalarValue::Int64(Some(n)) => Some(*n),
                _ => None,
            })
            .ok_or_else(|| {
                DataFusionError::Plan(format!("{NAME}: {what} must be an Int64 literal"))
            })
    };
    Grouped::new(op, grid(2, "start")?, grid(3, "end")?, grid(4, "step")?)
}

/// `DISTINCT` would have to deduplicate whole series before folding them,
/// which this aggregation has no notion of. Answering the non-distinct
/// question instead would be silently wrong, so refuse to plan it.
fn distinct_unsupported(args: &AccumulatorArgs) -> Result<()> {
    if args.is_distinct {
        return plan_err!("{NAME}: DISTINCT is not supported");
    }
    Ok(())
}

impl AggregateUDFImpl for Aggregate {
    fn name(&self) -> &str {
        NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.first() != Some(&series::samples_type()) {
            return plan_err!(
                "{NAME}: first argument must be {}, got {:?}",
                series::samples_type(),
                arg_types.first()
            );
        }
        Ok(series::samples_type())
    }

    /// Every group yields a list, possibly empty; never NULL.
    fn is_nullable(&self) -> bool {
        false
    }

    fn accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        distinct_unsupported(&args)?;
        Ok(Box::new(OneGroup::new(from_args(&args)?)?))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![Arc::new(Field::new(
            format_state_name(args.name, "steps"),
            state_type(),
            false,
        ))])
    }

    fn groups_accumulator_supported(&self, args: AccumulatorArgs) -> bool {
        !args.is_distinct
    }

    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        distinct_unsupported(&args)?;
        Ok(Box::new(from_args(&args)?))
    }

    /// What an aggregation over no rows at all yields: no samples.
    fn default_value(&self, _data_type: &DataType) -> Result<ScalarValue> {
        Ok(empty_samples())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::math::{max_nan_loses, min_nan_loses, KahanSum, Welford};
    use datafusion::arrow::array::{Int64Array, RecordBatch, StringArray};
    use datafusion::arrow::datatypes::Schema;
    use datafusion::datasource::MemTable;
    use datafusion::physical_plan::displayable;
    use datafusion::prelude::{SessionConfig, SessionContext};

    mod bypass;
    mod left_growth;
    mod preservation;
    mod selector_prefix;
    mod stream_lifetime;

    // The one-sample-at-a-time reference the lane kernels are pinned
    // against: a slow restatement of each operator, in the value types
    // upstream's per-group `groupedAggregation` (`engine.go`) uses, kept
    // only so the differential tests below can check the lanes bit for bit.

    /// One group's running value at one step, mirroring `groupedAggregation`
    /// in upstream's `engine.go`.
    ///
    /// Nothing in the engine holds a `State`: the accumulator works on flat
    /// lanes and never materializes one. It exists as the bit-fidelity oracle
    /// the lane kernels are pinned against, stated in the value types
    /// upstream uses.
    #[derive(Debug, Clone, Copy, PartialEq)]
    enum State {
        Sum(KahanSum),
        Avg(Mean),
        Count(f64),
        Min(f64),
        Max(f64),
        Group,
        Var(Welford),
    }

    impl State {
        fn new(op: Op) -> State {
            match op {
                Op::Sum => State::Sum(KahanSum::default()),
                Op::Avg => State::Avg(Mean::default()),
                Op::Count => State::Count(0.0),
                Op::Min => State::Min(f64::NAN),
                Op::Max => State::Max(f64::NAN),
                Op::Group => State::Group,
                Op::Stddev | Op::Stdvar => State::Var(Welford::default()),
            }
        }

        /// One more series' value at this step.
        fn add(&mut self, f: f64) {
            match self {
                State::Sum(k) => k.add(f),
                State::Avg(m) => m.add(f),
                State::Count(n) => *n += 1.0,
                State::Min(v) => *v = min_nan_loses(*v, f),
                State::Max(v) => *v = max_nan_loses(*v, f),
                State::Group => {}
                State::Var(w) => w.add(f),
            }
        }

        /// Another partial state for the same step, from another partition.
        fn merge(&mut self, other: &State) {
            match (self, other) {
                (State::Sum(a), State::Sum(b)) => a.merge(b),
                (State::Avg(a), State::Avg(b)) => a.merge(b),
                (State::Count(a), State::Count(b)) => *a += b,
                (State::Min(a), State::Min(b)) => *a = min_nan_loses(*a, *b),
                (State::Max(a), State::Max(b)) => *a = max_nan_loses(*a, *b),
                (State::Group, State::Group) => {}
                (State::Var(a), State::Var(b)) => a.merge(b),
                _ => unreachable!("one accumulator, one operator"),
            }
        }

        fn result(&self, op: Op) -> f64 {
            match (self, op) {
                (State::Sum(k), _) => k.value(),
                (State::Avg(m), _) => m.result(),
                (State::Count(n), _) => *n,
                (State::Min(v), _) | (State::Max(v), _) => *v,
                (State::Group, _) => 1.0,
                (State::Var(w), Op::Stddev) => w.variance().sqrt(),
                (State::Var(w), _) => w.variance(),
            }
        }

        /// The four numbers that serialize any arm. [`Grouped::partial`] is
        /// the lane-wise restatement, and this is what pins it.
        fn to_row(self) -> (f64, f64, f64, bool) {
            match self {
                State::Sum(k) => (k.sum, k.c, 0.0, false),
                State::Avg(m) => (m.value, m.c, m.count, m.incremental),
                State::Count(n) => (0.0, 0.0, n, false),
                State::Min(v) | State::Max(v) => (v, 0.0, 0.0, false),
                State::Group => (0.0, 0.0, 0.0, false),
                State::Var(w) => (w.mean, w.m2, w.count, false),
            }
        }

        /// The inverse, which [`Lanes::copy_run`] restates lane-wise. Only
        /// the tests need it as a value: the accumulator copies the lanes.
        fn from_row(op: Op, a: f64, b: f64, n: f64, m: bool) -> State {
            match op {
                Op::Sum => State::Sum(KahanSum::new(a, b)),
                Op::Avg => State::Avg(Mean {
                    value: a,
                    c: b,
                    count: n,
                    incremental: m,
                }),
                Op::Count => State::Count(n),
                Op::Min => State::Min(a),
                Op::Max => State::Max(a),
                Op::Group => State::Group,
                Op::Stddev | Op::Stdvar => State::Var(Welford {
                    mean: a,
                    m2: b,
                    count: n,
                }),
            }
        }
    }

    /// The state one lane position holds, read back out of [`Lanes`] as a
    /// [`State`] value for comparison against the oracle.
    fn get(lanes: &Lanes, at: usize) -> State {
        match lanes.op {
            Op::Sum => {
                let [sum, c] = lanes.read();
                State::Sum(KahanSum::new(sum[at], c[at]))
            }
            Op::Avg => {
                let [value, c, count] = lanes.read();
                State::Avg(Mean {
                    value: value[at],
                    c: c[at],
                    count: count[at],
                    incremental: lanes
                        .incremental
                        .as_ref()
                        .expect("avg has the bit lane")
                        .get_bit(at),
                })
            }
            Op::Count => {
                let [n] = lanes.read();
                State::Count(n[at])
            }
            Op::Min => {
                let [cur] = lanes.read();
                State::Min(cur[at])
            }
            Op::Max => {
                let [cur] = lanes.read();
                State::Max(cur[at])
            }
            Op::Group => State::Group,
            Op::Stddev | Op::Stdvar => {
                let [mean, m2, count] = lanes.read();
                State::Var(Welford {
                    mean: mean[at],
                    m2: m2[at],
                    count: count[at],
                })
            }
        }
    }

    /// Fold `series` into one group through the slice path, on a grid
    /// wide enough for every timestamp used.
    fn accumulate(op: Op, series: &[&[(i64, f64)]], start: i64, end: i64, step: i64) -> Grouped {
        let mut acc = Grouped::new(op, start, end, step).unwrap();
        acc.grow(1).unwrap();
        for s in series {
            let (ts, vs): (Vec<i64>, Vec<f64>) = s.iter().copied().unzip();
            acc.add_series(0, &ts, &vs).unwrap();
        }
        acc
    }

    /// One group's series, as pairs.
    fn samples(acc: &Grouped, group: usize) -> Vec<(i64, f64)> {
        rows(&acc.finish(acc.emitted(EmitTo::All)).unwrap(), group)
    }

    /// Read global grid positions without assuming a span starts at zero.
    fn at(acc: &Grouped, group: usize, index: usize) -> Option<State> {
        let view = acc.view(group);
        let index = index.checked_sub(view.first)?;
        let at = view.start + index;
        (index < view.len && view.seen.get_bit(at)).then(|| get(view.lanes, at))
    }

    /// One list row of an emitted `samples` array.
    fn rows(list: &ListArray, group: usize) -> Vec<(i64, f64)> {
        let row = list.value(group);
        let entries = row.as_struct();
        let ts = entries.column(0).as_primitive::<TimestampMillisecondType>();
        let vs = entries.column(1).as_primitive::<Float64Type>();
        (0..entries.len())
            .map(|i| (ts.value(i), vs.value(i)))
            .collect()
    }

    /// Fold a partial state into every row's group zero.
    fn merge(acc: &mut Grouped, state: &ArrayRef) -> Result<()> {
        let groups = vec![0usize; state.len()];
        acc.merge_batch(std::slice::from_ref(state), &groups, 1)
    }

    fn run(op: Op, series: &[&[(i64, f64)]]) -> Vec<(i64, f64)> {
        samples(&accumulate(op, series, 0, 3, 1), 0)
    }

    const OPS: [Op; 8] = [
        Op::Sum,
        Op::Avg,
        Op::Count,
        Op::Min,
        Op::Max,
        Op::Group,
        Op::Stddev,
        Op::Stdvar,
    ];

    #[test]
    fn each_step_is_aggregated_over_the_series_present_at_it() {
        let a: &[(i64, f64)] = &[(0, 1.0), (1, 2.0), (2, 3.0)];
        let b: &[(i64, f64)] = &[(1, 10.0), (2, 20.0), (3, 30.0)];
        assert_eq!(
            run(Op::Sum, &[a, b]),
            vec![(0, 1.0), (1, 12.0), (2, 23.0), (3, 30.0)]
        );
        assert_eq!(
            run(Op::Count, &[a, b]),
            vec![(0, 1.0), (1, 2.0), (2, 2.0), (3, 1.0)]
        );
        assert_eq!(
            run(Op::Avg, &[a, b]),
            vec![(0, 1.0), (1, 6.0), (2, 11.5), (3, 30.0)]
        );
        assert_eq!(run(Op::Min, &[a, b])[1], (1, 2.0));
        assert_eq!(run(Op::Max, &[a, b])[1], (1, 10.0));
        assert_eq!(run(Op::Group, &[a, b])[3], (3, 1.0));
    }

    #[test]
    fn stddev_and_stdvar_are_population_statistics() {
        let series: Vec<&[(i64, f64)]> = vec![
            &[(0, 2.0)],
            &[(0, 4.0)],
            &[(0, 4.0)],
            &[(0, 4.0)],
            &[(0, 5.0)],
            &[(0, 5.0)],
            &[(0, 7.0)],
            &[(0, 9.0)],
        ];
        assert!((run(Op::Stdvar, &series)[0].1 - 4.0).abs() < 1e-12);
        assert!((run(Op::Stddev, &series)[0].1 - 2.0).abs() < 1e-12);
    }

    /// The lane kernels and the one-value-at-a-time [`State`] must agree
    /// bit for bit. [`State`] is the readable definition of each
    /// operator's arithmetic; the lanes are the fast restatement of it,
    /// and this is what pins them together. Every number of the state is
    /// compared, not only the result, because `avg` and the variances
    /// carry more than their result shows.
    #[test]
    fn the_slice_path_matches_the_sample_at_a_time_path_exactly() {
        let values: Vec<Vec<f64>> = vec![
            vec![1.0, 2.0, 3.0, 4.0],
            vec![1e16, 1.0, -1e16, 0.5],
            vec![f64::NAN, 7.0, -0.0, 1e308],
            vec![-3.5, f64::INFINITY, 2.0, 1e-320],
            vec![0.1, 0.2, 0.3, 0.4],
        ];
        for op in OPS {
            let series: Vec<Vec<(i64, f64)>> = values
                .iter()
                .map(|vs| {
                    vs.iter()
                        .copied()
                        .enumerate()
                        .map(|(i, v)| (i as i64, v))
                        .collect()
                })
                .collect();
            let refs: Vec<&[(i64, f64)]> = series.iter().map(|s| s.as_slice()).collect();
            let lanes = accumulate(op, &refs, 0, 3, 1);

            // The same numbers, one at a time, through `State`.
            let mut scalar: Vec<State> = (0..4).map(|_| State::new(op)).collect();
            for vs in &values {
                for (i, v) in vs.iter().enumerate() {
                    scalar[i].add(*v);
                }
            }
            for (i, want) in scalar.iter().enumerate() {
                assert_same(op, at(&lanes, 0, i).unwrap(), *want, i);
            }
        }
    }

    /// Two states hold the same four numbers, bit for bit.
    fn assert_same(op: Op, got: State, want: State, at: usize) {
        let (a, b, n, m) = got.to_row();
        let (x, y, z, f) = want.to_row();
        assert_eq!(a.to_bits(), x.to_bits(), "{op:?} a at {at}: {a} vs {x}");
        assert_eq!(b.to_bits(), y.to_bits(), "{op:?} b at {at}: {b} vs {y}");
        assert_eq!(n.to_bits(), z.to_bits(), "{op:?} n at {at}: {n} vs {z}");
        assert_eq!(m, f, "{op:?} m at {at}");
        assert_eq!(
            got.result(op).to_bits(),
            want.result(op).to_bits(),
            "{op:?} result at {at}"
        );
    }

    /// The partial state that arrives, one row per series over all four
    /// steps.
    ///
    /// The values are laid out for the steps the destination also
    /// reached, because those are the only ones that merge rather than
    /// copy, and a merge is where the two subtle errors live. Step 1
    /// cancels to a sum of zero and a compensation of one on both sides,
    /// so a merge that drops the incoming compensation changes the
    /// answer; step 3 overflows `avg`'s Kahan sum on both sides, so a
    /// merge that reads the incoming mean as a plain sum does too.
    /// Steps 0 and 2 are copied, and carry a NaN, a signed zero, a
    /// denormal and an infinity.
    const INCOMING: [[f64; 4]; 4] = [
        [1.0, 1e16, -0.0, 1e308],
        [2.0, 1.0, 7.0, 1e308],
        [f64::NAN, -1e16, 3.0, 1.0],
        [-3.5, 1e-320, f64::INFINITY, 0.5],
    ];

    /// What the destination already holds, at steps 1 and 3 only.
    const RESIDENT: [[f64; 2]; 3] = [[1e16, 1e308], [1.0, 1e308], [-1e16, 2.0]];

    /// The five numbers of each partial state row, in emitted order.
    fn state_rows(state: &ArrayRef) -> Vec<(i64, f64, f64, f64, bool)> {
        let entries = state.as_list::<i32>().values().as_struct();
        let column = |name: &str| entries.column_by_name(name).unwrap().clone();
        let ts = column(series::TIMESTAMP);
        let ts = ts.as_primitive::<TimestampMillisecondType>();
        let (a, b, n) = (column(STATE_A), column(STATE_B), column(STATE_N));
        let m = column(STATE_M);
        (0..entries.len())
            .map(|i| {
                (
                    ts.value(i),
                    a.as_primitive::<Float64Type>().value(i),
                    b.as_primitive::<Float64Type>().value(i),
                    n.as_primitive::<Float64Type>().value(i),
                    m.as_boolean().value(i),
                )
            })
            .collect()
    }

    /// The lane-wise partial state and merge must agree with [`State`]
    /// bit for bit, the same claim
    /// [`the_slice_path_matches_the_sample_at_a_time_path_exactly`] makes
    /// for the add path. The destination is reached at steps 1 and 3
    /// only, so one incoming run covers both kinds of position: merging
    /// into a fresh Kahan state is not a copy, and the two must not be
    /// confused.
    #[test]
    fn the_lane_merge_matches_the_state_at_a_time_merge_exactly() {
        for op in OPS {
            let source: Vec<Vec<(i64, f64)>> = INCOMING
                .iter()
                .map(|vs| (0..4).map(|i| (i as i64, vs[i])).collect())
                .collect();
            let target: Vec<Vec<(i64, f64)>> = RESIDENT
                .iter()
                .map(|vs| vec![(1, vs[0]), (3, vs[1])])
                .collect();
            fn as_refs(s: &[Vec<(i64, f64)>]) -> Vec<&[(i64, f64)]> {
                s.iter().map(|v| v.as_slice()).collect()
            }
            let mut src = accumulate(op, &as_refs(&source), 0, 3, 1);
            let mut dest = accumulate(op, &as_refs(&target), 0, 3, 1);

            let before: Vec<Option<State>> = (0..4).map(|i| at(&dest, 0, i)).collect();
            let sent: Vec<Option<State>> = (0..4).map(|i| at(&src, 0, i)).collect();

            let state = src.state(EmitTo::All).unwrap().remove(0);
            let rows = state_rows(&state);

            // What the lanes wrote is what `State::to_row` would have.
            let expect_rows: Vec<(i64, f64, f64, f64, bool)> = sent
                .iter()
                .enumerate()
                .filter_map(|(i, s)| s.map(|s| (i as i64, s)))
                .map(|(t, s)| {
                    let (a, b, n, m) = s.to_row();
                    (t, a, b, n, m)
                })
                .collect();
            assert_eq!(rows.len(), expect_rows.len(), "{op:?}");
            for (got, want) in rows.iter().zip(&expect_rows) {
                assert_eq!(got.0, want.0, "{op:?}");
                assert_eq!(got.1.to_bits(), want.1.to_bits(), "{op:?} a");
                assert_eq!(got.2.to_bits(), want.2.to_bits(), "{op:?} b");
                assert_eq!(got.3.to_bits(), want.3.to_bits(), "{op:?} n");
                assert_eq!(got.4, want.4, "{op:?} m");
            }

            // The same rows merged one step at a time, through `State`.
            let expect: Vec<Option<State>> = (0..4)
                .map(|i| {
                    let incoming = rows
                        .iter()
                        .find(|r| r.0 == i as i64)
                        .map(|r| State::from_row(op, r.1, r.2, r.3, r.4));
                    match (before[i], incoming) {
                        (Some(mut current), Some(s)) => {
                            current.merge(&s);
                            Some(current)
                        }
                        (None, Some(s)) => Some(s),
                        (current, None) => current,
                    }
                })
                .collect();

            merge(&mut dest, &state).unwrap();
            for (i, want) in expect.iter().enumerate() {
                assert_eq!(
                    at(&dest, 0, i).is_some(),
                    want.is_some(),
                    "{op:?} seen at {i}"
                );
                if let Some(want) = want {
                    assert_same(op, at(&dest, 0, i).unwrap(), *want, i);
                }
            }
        }
    }

    #[test]
    fn a_gap_in_a_series_splits_it_into_runs() {
        // Steps 0 and 2 present, 1 missing: two runs, and step 1 stays
        // unreached rather than being counted as a zero contribution.
        let a: &[(i64, f64)] = &[(0, 1.0), (2, 3.0)];
        let b: &[(i64, f64)] = &[(0, 10.0), (1, 20.0), (2, 30.0)];
        assert_eq!(
            samples(&accumulate(Op::Count, &[a], 0, 3, 1), 0),
            vec![(0, 1.0), (2, 1.0)]
        );
        assert_eq!(
            samples(&accumulate(Op::Sum, &[a, b], 0, 3, 1), 0),
            vec![(0, 11.0), (1, 20.0), (2, 33.0)]
        );
    }

    /// A span reaching the query's last step must not touch another group,
    /// including an empty group with no numerical lanes allocated.
    #[test]
    fn groups_do_not_bleed_into_each_other() {
        let mut acc = Grouped::new(Op::Sum, 0, 2, 1).unwrap();
        acc.grow(3).unwrap();
        acc.add_series(0, &[0, 1, 2], &[1.0, 1.0, 1.0]).unwrap();
        acc.add_series(2, &[2], &[5.0]).unwrap();
        acc.add_series(2, &[2], &[6.0]).unwrap();
        assert_eq!(
            samples(&acc, 0),
            vec![(0, 1.0), (1, 1.0), (2, 1.0)],
            "the first group"
        );
        assert!(samples(&acc, 1).is_empty(), "a group nothing reached");
        assert_eq!(samples(&acc, 2), vec![(2, 11.0)], "the last group");
    }

    /// The trait's bounded-memory emission: the first groups leave and
    /// the rest keep their state under indices shifted down by as many.
    #[test]
    fn emitting_the_first_groups_shifts_the_rest_down() {
        let mut acc = Grouped::new(Op::Sum, 0, 2, 1).unwrap();
        acc.grow(3).unwrap();
        acc.add_series(0, &[0], &[1.0]).unwrap();
        acc.add_series(1, &[1], &[2.0]).unwrap();
        acc.add_series(2, &[2], &[3.0]).unwrap();

        let out = acc.evaluate(EmitTo::First(1)).unwrap();
        let out = out.as_list::<i32>();
        assert_eq!(out.len(), 1);
        assert_eq!(rows(out, 0), vec![(0, 1.0)]);

        // What was group 1 is now group 0, and still accumulating.
        assert_eq!(acc.emitted(EmitTo::All), 2);
        acc.add_series(0, &[1], &[10.0]).unwrap();
        assert_eq!(samples(&acc, 0), vec![(1, 12.0)]);
        assert_eq!(samples(&acc, 1), vec![(2, 3.0)]);
    }

    #[test]
    fn a_sample_off_the_step_grid_is_an_error() {
        let mut acc = Grouped::new(Op::Sum, 0, 60_000, 30_000).unwrap();
        acc.grow(1).unwrap();
        assert!(acc.add_series(0, &[15_000], &[1.0]).is_err());
        assert!(acc.add_series(0, &[90_000], &[1.0]).is_err());
        assert!(acc.add_series(0, &[-30_000], &[1.0]).is_err());
        assert!(acc
            .add_series(0, &[0, 30_000, 60_000], &[1.0, 2.0, 3.0])
            .is_ok());
    }

    #[test]
    fn partial_states_round_trip_and_merge() {
        for op in OPS {
            let left_series: &[&[(i64, f64)]] = &[&[(0, 1.0), (1, 5.0)], &[(0, 3.0)]];
            let right_series: &[&[(i64, f64)]] = &[&[(0, 8.0), (1, 4.0), (2, 2.0)]];
            let all: Vec<&[(i64, f64)]> = left_series.iter().chain(right_series).copied().collect();
            let whole = accumulate(op, &all, 0, 2, 1);
            let mut left = accumulate(op, left_series, 0, 2, 1);
            let mut right = accumulate(op, right_series, 0, 2, 1);
            let mut merged = Grouped::new(op, 0, 2, 1).unwrap();
            let mut states: Vec<ArrayRef> = Vec::new();
            for acc in [&mut left, &mut right] {
                states.push(acc.state(EmitTo::All).unwrap().remove(0));
            }
            for s in &states {
                merge(&mut merged, s).unwrap();
            }
            let expect = samples(&whole, 0);
            let got = samples(&merged, 0);
            assert_eq!(got.len(), expect.len(), "{op:?}");
            for ((t1, a), (t2, b)) in got.iter().zip(&expect) {
                assert_eq!(t1, t2, "{op:?}");
                assert!((a - b).abs() < 1e-12, "{op:?}: {a} vs {b}");
            }
        }
    }

    /// `avg`'s overflow switch must follow its group when indices rebase.
    #[test]
    fn emitting_the_first_groups_shifts_the_packed_flag_too() {
        let flag = |acc: &Grouped, group: usize, index: usize| match at(acc, group, index) {
            Some(State::Avg(m)) => m.incremental,
            None => false,
            _ => unreachable!("one accumulator, one operator"),
        };
        let mut acc = Grouped::new(Op::Avg, 0, 2, 1).unwrap();
        acc.grow(2).unwrap();
        acc.add_series(0, &[0], &[1.0]).unwrap();
        // Twice the largest float at the last step of the second group:
        // the running sum overflows, which is where Prometheus switches
        // `avg` to an incremental mean.
        acc.add_series(1, &[2], &[f64::MAX]).unwrap();
        acc.add_series(1, &[2], &[f64::MAX]).unwrap();
        assert!(flag(&acc, 1, 2));

        acc.evaluate(EmitTo::First(1)).unwrap();
        assert_eq!(acc.emitted(EmitTo::All), 1);
        assert!(flag(&acc, 0, 2), "the flag moved down with its group");
        assert!(!flag(&acc, 0, 0));
    }

    /// A run wide enough to cover whole bytes of the seen grid, which is
    /// the case [`set_run`] fills byte by byte rather than bit by bit.
    #[test]
    fn a_run_across_whole_bytes_sets_exactly_the_steps_it_covers() {
        let mut acc = Grouped::new(Op::Count, 0, 199, 1).unwrap();
        acc.grow(2).unwrap();
        let timestamps: Vec<i64> = (3..197).collect();
        let values = vec![1.0; timestamps.len()];
        acc.add_series(1, &timestamps, &values).unwrap();
        for bit in 0..400 {
            assert_eq!(
                at(&acc, bit / 200, bit % 200).is_some(),
                (203..397).contains(&bit),
                "bit {bit}"
            );
        }
        assert!(samples(&acc, 0).is_empty());
        assert_eq!(samples(&acc, 1).len(), 194);
    }

    #[test]
    fn evaluate_is_a_canonical_samples_list() {
        let mut acc = Grouped::new(Op::Sum, 0, 60_000, 30_000).unwrap();
        acc.grow(2).unwrap();
        acc.add_series(0, &[30_000], &[1.0]).unwrap();
        acc.add_series(0, &[0], &[2.0]).unwrap();
        let arr = acc.evaluate(EmitTo::All).unwrap();
        assert_eq!(arr.data_type(), &series::samples_type());
        let list = arr.as_list::<i32>();
        assert_eq!(list.len(), 2, "one row per group, empty or not");
        assert_eq!(list.value(0).len(), 2);
        assert_eq!(list.value(1).len(), 0);
        assert_eq!(
            acc.emitted(EmitTo::All),
            0,
            "emitting everything resets the state"
        );
        assert_eq!(
            empty_samples().to_array().unwrap().data_type(),
            &series::samples_type()
        );
    }

    #[test]
    fn every_supported_aggregation_dispatches_on_its_token() {
        for (token, op) in [
            (ItemType::Sum, Op::Sum),
            (ItemType::Avg, Op::Avg),
            (ItemType::Count, Op::Count),
            (ItemType::Min, Op::Min),
            (ItemType::Max, Op::Max),
            (ItemType::Group, Op::Group),
            (ItemType::Stddev, Op::Stddev),
            (ItemType::Stdvar, Op::Stdvar),
        ] {
            assert_eq!(Op::from_token(token), Some(op), "{token}");
            // The token and the plan's string literal name one kernel.
            assert_eq!(op.as_str(), token.to_string(), "{token}");
            assert_eq!(Op::parse(op.as_str()), Some(op), "{token}");
        }
    }

    #[test]
    fn a_token_that_merely_prints_like_an_aggregation_is_not_one() {
        assert_eq!(ItemType::SumDesc.to_string(), ItemType::Sum.to_string());
        assert_eq!(ItemType::CountDesc.to_string(), ItemType::Count.to_string());
        assert_eq!(Op::from_token(ItemType::SumDesc), None);
        assert_eq!(Op::from_token(ItemType::CountDesc), None);
    }

    #[test]
    fn an_aggregation_this_engine_does_not_implement_has_no_operator() {
        for token in [
            ItemType::Topk,
            ItemType::Bottomk,
            ItemType::Quantile,
            ItemType::CountValues,
            ItemType::Limitk,
            ItemType::LimitRatio,
        ] {
            assert_eq!(Op::from_token(token), None, "{token}");
        }
    }

    #[test]
    fn a_grid_wider_than_the_cap_is_an_error() {
        let err = Grouped::new(Op::Sum, 0, 1000 * 24 * 60 * 60 * 1000, 1)
            .unwrap_err()
            .to_string();
        assert!(err.contains(&MAX_STEPS.to_string()), "{err}");
        assert!(Grouped::new(Op::Group, 0, MAX_STEPS as i64 - 1, 1).is_ok());
        assert!(Grouped::new(Op::Group, 0, MAX_STEPS as i64, 1).is_err());
    }

    #[test]
    fn a_series_whose_timestamps_are_not_ascending_is_an_error() {
        let mut acc = Grouped::new(Op::Sum, 0, 60_000, 30_000).unwrap();
        acc.grow(1).unwrap();
        let err = acc
            .add_series(0, &[60_000, 30_000, 0], &[1.0, 2.0, 3.0])
            .unwrap_err()
            .to_string();
        assert!(err.contains("ascending"), "{err}");
        assert!(acc
            .add_series(0, &[0, 30_000, 60_000], &[3.0, 2.0, 1.0])
            .is_ok());

        // A partial state arrives through the same walk, so the merge
        // side rejects the breach rather than writing it at the wrong
        // step: both sides are reachable from SQL over any list column.
        let mut acc = Grouped::new(Op::Sum, 0, 60_000, 30_000).unwrap();
        let err = merge(&mut acc, &partial_state_at(vec![60_000, 30_000, 0]))
            .unwrap_err()
            .to_string();
        assert!(err.contains("ascending"), "{err}");
    }

    /// A permutation spans as many grid positions as it has samples, so
    /// a length check alone would take it for one ascending run and fold
    /// every value into the wrong step.
    #[test]
    fn a_series_whose_timestamps_are_permuted_is_an_error() {
        let mut acc = Grouped::new(Op::Sum, 0, 3, 1).unwrap();
        acc.grow(1).unwrap();
        let err = acc
            .add_series(0, &[0, 2, 1, 3], &[1.0, 2.0, 3.0, 4.0])
            .unwrap_err()
            .to_string();
        assert!(err.contains("ascending"), "{err}");
    }

    #[test]
    fn a_series_with_a_gap_still_lands_on_the_right_steps() {
        let acc = accumulate(Op::Sum, &[&[(0, 1.0), (1, 2.0), (3, 4.0)]], 0, 3, 1);
        assert_eq!(samples(&acc, 0), vec![(0, 1.0), (1, 2.0), (3, 4.0)]);
    }

    /// The grid's own arithmetic is reachable from SQL with any
    /// timestamp, including one that wraps `i64` on the way to an index.
    #[test]
    fn a_sample_at_the_start_of_time_is_an_error_not_an_overflow() {
        let mut acc = Grouped::new(Op::Sum, 1, 1, 1).unwrap();
        acc.grow(1).unwrap();
        let err = acc
            .add_series(0, &[i64::MIN], &[1.0])
            .unwrap_err()
            .to_string();
        assert!(err.contains("not on the step grid"), "{err}");
    }

    /// A partial state whose struct is not the one [`state_fields`]
    /// describes, as a one-row list ready for `merge_batch`.
    fn partial_state(fields: Fields, columns: Vec<ArrayRef>) -> ArrayRef {
        let entries = StructArray::new(fields.clone(), columns, None);
        let item = Arc::new(Field::new(
            series::LIST_ITEM,
            DataType::Struct(fields),
            false,
        ));
        let n = entries.len() as i32;
        Arc::new(list_per_group(item, vec![0, n], entries))
    }

    /// One row of partial state at `timestamps`, all lanes zero.
    fn partial_state_at(timestamps: Vec<i64>) -> ArrayRef {
        let rows = timestamps.len();
        let zeros = || -> ArrayRef { Arc::new(Float64Array::from(vec![0.0; rows])) };
        let flags: ArrayRef = Arc::new(BooleanArray::from(vec![false; rows]));
        partial_state(
            state_fields(),
            vec![
                Arc::new(TimestampMillisecondArray::from(timestamps)),
                zeros(),
                zeros(),
                zeros(),
                flags,
            ],
        )
    }

    /// Run detection steps one grid position forward, which is where a
    /// timestamp at the end of time would wrap.
    #[test]
    fn a_partial_state_timestamp_at_the_end_of_time_is_an_error_not_an_overflow() {
        let mut acc = Grouped::new(Op::Sum, 0, 0, 1).unwrap();
        let err = merge(&mut acc, &partial_state_at(vec![0, i64::MAX]))
            .unwrap_err()
            .to_string();
        assert!(err.contains("not on the step grid"), "{err}");

        // And a grid that genuinely ends there still merges its one step.
        let mut acc = Grouped::new(Op::Sum, i64::MAX, i64::MAX, 1).unwrap();
        merge(&mut acc, &partial_state_at(vec![i64::MAX])).unwrap();
    }

    #[test]
    fn a_partial_state_missing_a_field_is_an_error() {
        let state = partial_state(
            Fields::from(vec![
                Field::new(series::TIMESTAMP, series::timestamp_type(), false),
                Field::new(STATE_A, DataType::Float64, false),
            ]),
            vec![
                Arc::new(TimestampMillisecondArray::from(vec![0i64])),
                Arc::new(Float64Array::from(vec![1.0])),
            ],
        );
        let mut acc = Grouped::new(Op::Sum, 0, 0, 1).unwrap();
        let err = merge(&mut acc, &state).unwrap_err().to_string();
        assert!(err.contains(&format!("no {STATE_B} column")), "{err}");
    }

    #[test]
    fn a_partial_state_with_the_wrong_types_is_an_error() {
        let state = partial_state(
            Fields::from(vec![
                Field::new(series::TIMESTAMP, series::timestamp_type(), false),
                Field::new(STATE_A, DataType::Float64, false),
                Field::new(STATE_B, DataType::Float64, false),
                // A peer that widened this lane, say.
                Field::new(STATE_N, DataType::Int64, false),
                Field::new(STATE_M, DataType::Boolean, false),
            ]),
            vec![
                Arc::new(TimestampMillisecondArray::from(vec![0i64])),
                Arc::new(Float64Array::from(vec![1.0])),
                Arc::new(Float64Array::from(vec![1.0])),
                Arc::new(Int64Array::from(vec![1i64])),
                Arc::new(BooleanArray::from(vec![false])),
            ],
        );
        let mut acc = Grouped::new(Op::Sum, 0, 0, 1).unwrap();
        let err = merge(&mut acc, &state).unwrap_err().to_string();
        assert!(err.contains(&format!("{STATE_N} is not Float64")), "{err}");
    }

    /// A `samples` column holding one list row per series.
    fn samples_column(series: &[Vec<(i64, f64)>]) -> ArrayRef {
        let mut offsets = vec![0i32];
        let (mut ts, mut vs) = (Vec::new(), Vec::new());
        for s in series {
            for (t, v) in s {
                ts.push(*t);
                vs.push(*v);
            }
            offsets.push(ts.len() as i32);
        }
        let entries = StructArray::new(
            series::sample_fields(),
            vec![
                Arc::new(TimestampMillisecondArray::from(ts)),
                Arc::new(Float64Array::from(vs)),
            ],
            None,
        );
        Arc::new(list_per_group(series::sample_item(), offsets, entries))
    }

    /// Eight groups of three series each, spread over six batches so
    /// that repartitioning has something to split and every group is
    /// reached from more than one of them.
    fn batches() -> (Arc<Schema>, Vec<RecordBatch>) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("job", DataType::Utf8, false),
            Field::new(series::SAMPLES, series::samples_type(), false),
        ]));
        let mut batches = Vec::new();
        for batch in 0..6 {
            let mut jobs = Vec::new();
            let mut series = Vec::new();
            for group in 0..8 {
                jobs.push(format!("job-{group}"));
                // A gap and an offset per batch, so the runs merged
                // across partitions overlap only partly.
                series.push(
                    (0..5)
                        .filter(|step| (step + batch) % 3 != 0)
                        .map(|step| (step as i64, (batch * 100 + group * 10 + step) as f64))
                        .collect(),
                );
            }
            batches.push(
                RecordBatch::try_new(
                    schema.clone(),
                    vec![Arc::new(StringArray::from(jobs)), samples_column(&series)],
                )
                .unwrap(),
            );
        }
        (schema, batches)
    }

    /// Every group's series, keyed by job and compared as bits.
    async fn grouped_sql(op: Op, partitions: usize) -> Vec<(String, Vec<(i64, u64)>)> {
        let (schema, batches) = batches();
        let ctx = SessionContext::new_with_config(
            SessionConfig::new().with_target_partitions(partitions),
        );
        ctx.register_udaf(udaf());
        // One partition per batch so DataFusion's Final phase actually
        // receives more than one partial state per group: a single
        // MemTable partition only ever exercises `Lanes::copy_run`. The
        // `partitions == 1` case stays one MemTable partition, since
        // that's the serial reference the many-partitions run is
        // compared against.
        let table_partitions = if partitions > 1 {
            batches.iter().cloned().map(|b| vec![b]).collect()
        } else {
            vec![batches]
        };
        ctx.register_table(
            "t",
            Arc::new(MemTable::try_new(schema, table_partitions).unwrap()),
        )
        .unwrap();
        let sql = format!(
            "SELECT job, {NAME}(samples, '{}', 0, 4, 1) AS out FROM t GROUP BY job ORDER BY job",
            op.as_str()
        );
        let frame = ctx.sql(&sql).await.unwrap();
        // The premise of the comparison: with more than one partition
        // DataFusion really does split this into a partial phase whose
        // states the final phase merges, rather than quietly running it
        // in one.
        let plan = format!(
            "{}",
            displayable(frame.clone().create_physical_plan().await.unwrap().as_ref()).indent(false)
        );
        assert_eq!(
            plan.contains("mode=Partial"),
            partitions > 1,
            "{partitions} partitions:\n{plan}"
        );
        let out = frame.collect().await.unwrap();
        let mut got = Vec::new();
        for batch in &out {
            let jobs = batch.column(0).as_string::<i32>();
            let lists = batch.column(1).as_list::<i32>();
            for row in 0..batch.num_rows() {
                got.push((
                    jobs.value(row).to_string(),
                    rows(lists, row)
                        .into_iter()
                        .map(|(t, v)| (t, v.to_bits()))
                        .collect(),
                ));
            }
        }
        got.sort();
        got
    }

    /// The partial/final split DataFusion runs across partitions must
    /// not change a single bit, for any operator: the partial states
    /// leaving one partition and the merge in the final one are the same
    /// arithmetic as one accumulator seeing every row. Stddev and stdvar
    /// are the exception: Chan's parallel Welford merge used by
    /// `Lanes::merge_run` is not bit-identical to the serial fold, so
    /// those two are compared with a relative tolerance instead (this
    /// fixture's values run into the hundreds, where the absolute 1e-12
    /// `partial_states_round_trip_and_merge` uses is too tight). The
    /// Kahan-based operators stay exact.
    #[tokio::test]
    async fn many_partitions_aggregate_to_the_same_bits_as_one() {
        for op in OPS {
            let one = grouped_sql(op, 1).await;
            let many = grouped_sql(op, 4).await;
            assert_eq!(one.len(), 8, "{op:?}");
            assert!(one.iter().all(|(_, s)| !s.is_empty()), "{op:?}");
            if matches!(op, Op::Stddev | Op::Stdvar) {
                assert_eq!(one.len(), many.len(), "{op:?}");
                for ((job1, s1), (job2, s2)) in one.iter().zip(&many) {
                    assert_eq!(job1, job2, "{op:?}");
                    assert_eq!(s1.len(), s2.len(), "{op:?}");
                    for ((t1, a), (t2, b)) in s1.iter().zip(s2) {
                        assert_eq!(t1, t2, "{op:?}");
                        let (a, b) = (f64::from_bits(*a), f64::from_bits(*b));
                        let tol = 1e-11 * a.abs().max(b.abs()).max(1.0);
                        assert!((a - b).abs() < tol, "{op:?}: {a} vs {b}");
                    }
                }
            } else {
                assert_eq!(one, many, "{op:?}");
            }
        }
    }

    /// SQL without grouping columns drives `Accumulator` rather than the
    /// grouped path, so the two must agree. One partition per batch exercises
    /// partial states and merging rather than aggregating in one go.
    #[tokio::test]
    async fn an_aggregation_without_grouping_columns_agrees_with_the_grouped_path() {
        let (schema, batches) = batches();
        let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(4));
        ctx.register_udaf(udaf());
        let partitions: Vec<Vec<RecordBatch>> = batches.iter().cloned().map(|b| vec![b]).collect();
        ctx.register_table(
            "t",
            Arc::new(MemTable::try_new(schema, partitions).unwrap()),
        )
        .unwrap();
        let frame = ctx
            .sql(&format!(
                "SELECT {NAME}(samples, 'sum', 0, 4, 1) AS out FROM t"
            ))
            .await
            .unwrap();
        let plan = format!(
            "{}",
            displayable(frame.clone().create_physical_plan().await.unwrap().as_ref()).indent(false)
        );
        assert!(plan.contains("mode=Partial"), "{plan}");
        let out = frame.collect().await.unwrap();
        let got = rows(out[0].column(0).as_list::<i32>(), 0);

        let mut acc = Grouped::new(Op::Sum, 0, 4, 1).unwrap();
        acc.grow(1).unwrap();
        for batch in &batches {
            let lists = batch.column(1).as_list::<i32>();
            for row in 0..batch.num_rows() {
                let (ts, vs): (Vec<i64>, Vec<f64>) = rows(lists, row).into_iter().unzip();
                acc.add_series(0, &ts, &vs).unwrap();
            }
        }
        let want = samples(&acc, 0);
        assert_eq!(got.len(), want.len());
        for ((t1, a), (t2, b)) in got.iter().zip(&want) {
            assert_eq!(t1, t2);
            assert_eq!(a.to_bits(), b.to_bits(), "at {t1}: {a} vs {b}");
        }
    }
}
