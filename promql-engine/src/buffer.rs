//! One series' samples as its chunks arrive: Prometheus's
//! `storage.BufferedSeriesIterator` (`storage/buffer.go`), fed chunk by
//! chunk instead of pulling from a chunk iterator.
//!
//! What a later step still reads of a pushed chunk is copied into one contiguous
//! buffer rather than kept as a slice of its batch. A retained slice would
//! pin the whole batch's child buffers, other series' samples included, and
//! every kernel indexes one contiguous slice; the copy replaces the
//! staleness filter the range kernel pays anyway. Ingestion is bounded, even
//! for a second chunk or one containing stale markers. Ready steps drain
//! before more samples enter, and unreachable future tails are never copied.
//! Sample/step cursors survive a work-budget boundary; a range kernel's value
//! calculation and amortised buffer compaction remain indivisible.
//!
//! Multi-step selectors and range kernels evaluate as soon as their window
//! can no longer change, not at series close. Otherwise the buffer would hold
//! the whole series, which is 172,800 samples for a store that sends 30 days
//! at 15 s as one block; evaluated eagerly it holds a window. Exactly one
//! selector step retains only its latest eligible original sample and emits
//! it on close, with no history allocation. Row visibility and boundaries
//! remain the caller's, not the numerical iterator's.
//!
//! Blocks are the caller's: `EvalContext` is the grid of the steps one block
//! answers, and `close` at the block edge leaves nothing of the series
//! behind, so the reach-back a block repeats is folded afresh.

use crate::params::{step_count, Params};
use crate::range::{advance_predict_linear, advance_range, RangeOp, Sweep};
use crate::selector::{advance_selector, is_stale, Pick};

/// Maximum new history between readiness/retirement checks, independent of
/// the raw row size. This is an ingestion allowance, not a window-size cap.
const INGEST_SAMPLES: usize = 256;

/// Counts ingestion, edge movement, sweep queue pops, and evaluated steps.
/// It bounds cursor progress, not elapsed time. A range value calculation is
/// charged after its indivisible fold (so may overrun by that one call).
/// Binary search, allocation and amortised compaction cannot be preempted.
pub(crate) struct WorkBudget {
    remaining: usize,
}
impl WorkBudget {
    pub(crate) fn new(units: usize) -> Self {
        Self { remaining: units }
    }

    pub(crate) fn take(&mut self) -> bool {
        if self.remaining == 0 {
            return false;
        }
        self.remaining -= 1;
        true
    }

    fn take_up_to(&mut self, count: usize) -> usize {
        let count = count.min(self.remaining);
        self.remaining -= count;
        count
    }

    pub(crate) fn exhausted(&self) -> bool {
        self.remaining == 0
    }

    pub(crate) fn account_kernel(&mut self, samples: usize) {
        self.remaining = self.remaining.saturating_sub(samples);
    }
}

/// Position in one raw row, retained until both ingestion and its ready steps
/// complete. A row's final timestamp is claimed only after its reachable
/// prefix is consumed (dead prefixes and future tails can be skipped).
#[derive(Default)]
pub(crate) struct PushCursor {
    position: usize,
    started: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Hash)]
pub(crate) enum Kernel {
    Selector(Pick),
    /// Window fold: ordinary range functions and `predict_linear`.
    Range(RangeOp),
}

/// Immutable configuration belongs to the block, not to each series. The
/// cached count changes only together with its grid.
#[derive(Debug)]
pub(crate) struct EvalContext {
    kernel: Kernel,
    params: Params,
    steps: i128,
}

impl EvalContext {
    pub(crate) fn new(kernel: Kernel, params: Params) -> Self {
        Self {
            kernel,
            params,
            steps: step_count(params.start_ms, params.end_ms, params.step_ms),
        }
    }

    pub(crate) fn kernel(&self) -> Kernel {
        self.kernel
    }

    pub(crate) fn params(&self) -> Params {
        self.params
    }

    pub(crate) fn steps(&self) -> i128 {
        self.steps
    }

    pub(crate) fn step_at(&self, i: i128) -> i64 {
        (i128::from(self.params.start_ms) + i * i128::from(self.params.step_ms)) as i64
    }

    #[cfg(test)]
    pub(crate) fn timestamps(&self) -> impl Iterator<Item = i64> + '_ {
        (0..self.steps).map(|i| self.step_at(i))
    }
}

pub(crate) struct BufferedSeriesIterator {
    state: SeriesState,
}

/// A one-step selector needs neither history nor a sweep. Keeping these
/// owners exclusive avoids adding a candidate to every range window.
enum SeriesState {
    Window(WindowBuffer),
    Candidate {
        sample: Option<(i64, f64)>,
        last_t: Option<i64>,
    },
}

pub(crate) struct WindowBuffer {
    pub(crate) sweep: Option<Sweep>,
    pub(crate) ts: Vec<i64>,
    pub(crate) vs: Vec<f64>,
    /// Absolute ordinal `i` lives at `[i - base]`.
    pub(crate) base: usize,
    /// Absolute ordinals: the first sample a later step can still read, and
    /// one past the last sample the current step has read.
    pub(crate) lo: usize,
    pub(crate) hi: usize,
    /// Index into the step grid of the first step not yet evaluated.
    pub(crate) next_step: i128,
    /// Readiness watermark of the consumed prefix; completed rows use this
    /// for overlap, including timestamps claimed by filtered stale markers.
    pub(crate) last_t: Option<i64>,
    /// `@` computes one value once, then emits it resumably over the grid.
    pub(crate) pinned_value: Option<Option<f64>>,
}

impl BufferedSeriesIterator {
    pub(crate) fn new(context: &EvalContext) -> Self {
        let state = if matches!(context.kernel(), Kernel::Selector(_)) && context.steps() == 1 {
            SeriesState::Candidate {
                sample: None,
                last_t: None,
            }
        } else {
            SeriesState::Window(WindowBuffer::new(context))
        };
        Self { state }
    }

    /// Chunks must ascend by first timestamp. The callback sees numerical
    /// samples only; the caller owns visibility and Arrow row boundaries.
    #[cfg(test)]
    pub(crate) fn push(
        &mut self,
        context: &EvalContext,
        ts: &[i64],
        vs: &[f64],
        emit: impl FnMut(i64, f64),
    ) {
        assert!(self.push_budgeted(
            context,
            ts,
            vs,
            &mut PushCursor::default(),
            &mut WorkBudget::new(usize::MAX),
            emit,
        ));
    }

    /// True only when this row and all currently ready steps are consumed.
    pub(crate) fn push_budgeted(
        &mut self,
        context: &EvalContext,
        ts: &[i64],
        vs: &[f64],
        cursor: &mut PushCursor,
        budget: &mut WorkBudget,
        mut emit: impl FnMut(i64, f64),
    ) -> bool {
        debug_assert_eq!(ts.len(), vs.len());
        match &mut self.state {
            SeriesState::Window(window) => {
                return window.push(context, ts, vs, cursor, budget, &mut emit);
            }
            SeriesState::Candidate { sample, last_t } => {
                if !budget.take() {
                    return false;
                }
                let Some(&last) = ts.last() else {
                    return true;
                };
                let previous = *last_t;
                // The full chunk claims overlap, even beyond the lookup or
                // at a stale marker. An ineligible chunk keeps an older pick.
                *last_t = previous.max(Some(last));
                let p = context.params();
                let first_start = p
                    .at_ms
                    .unwrap_or(p.start_ms)
                    .saturating_sub(p.offset_ms)
                    .saturating_sub(p.window_ms);
                if last <= first_start {
                    return true;
                }
                let end = p.at_ms.unwrap_or(p.start_ms) - p.offset_ms;
                // Once an earlier chunk claims the lookup end, no later
                // non-overlapping sample can change the candidate.
                if previous.is_some_and(|last| last >= end) {
                    return true;
                }
                let from = previous.map_or(0, |last| ts.partition_point(|t| *t <= last));
                let ts = &ts[from..];
                let vs = &vs[from..];
                let hi = ts.partition_point(|t| *t <= end);
                if hi > 0 && ts[hi - 1] > end - p.window_ms {
                    *sample = Some((ts[hi - 1], vs[hi - 1]));
                }
            }
        }
        true
    }

    /// Emits remaining samples and resets numerics, retaining window/sweep
    /// capacities. Repeated close without push emits nothing, not an empty row.
    #[cfg(test)]
    pub(crate) fn close(&mut self, context: &EvalContext, emit: impl FnMut(i64, f64)) {
        assert!(self.close_budgeted(context, &mut WorkBudget::new(usize::MAX), emit));
    }

    /// False leaves the close cursor and all allocations alive for resumption.
    pub(crate) fn close_budgeted(
        &mut self,
        context: &EvalContext,
        budget: &mut WorkBudget,
        mut emit: impl FnMut(i64, f64),
    ) -> bool {
        match &mut self.state {
            SeriesState::Window(window) => {
                return window.close(context, budget, &mut emit);
            }
            SeriesState::Candidate { sample, last_t } => {
                if !budget.take() {
                    return false;
                }
                if let Some((t, v)) = sample.take() {
                    // `vectorSelectorSingle`: stale bits belong to the
                    // original value, before timestamp's Pick conversion.
                    if !is_stale(v) {
                        let Kernel::Selector(pick) = context.kernel() else {
                            unreachable!("candidate belongs to a selector");
                        };
                        emit(context.params().start_ms, pick.of(t, v));
                    }
                }
                *last_t = None;
            }
        }
        true
    }

    pub(crate) fn size(&self) -> usize {
        std::mem::size_of::<Self>()
            + match &self.state {
                SeriesState::Window(window) => window.heap_size(),
                SeriesState::Candidate { .. } => 0,
            }
    }
}

impl WindowBuffer {
    pub(crate) fn new(context: &EvalContext) -> Self {
        let sweep = match context.kernel() {
            Kernel::Range(RangeOp::Func(func)) => Sweep::new(func),
            Kernel::Range(RangeOp::PredictLinear(_)) | Kernel::Selector(_) => None,
        };
        Self {
            sweep,
            ts: Vec::new(),
            vs: Vec::new(),
            base: 0,
            lo: 0,
            hi: 0,
            next_step: 0,
            last_t: None,
            pinned_value: None,
        }
    }

    fn push(
        &mut self,
        context: &EvalContext,
        ts: &[i64],
        vs: &[f64],
        cursor: &mut PushCursor,
        budget: &mut WorkBudget,
        emit: &mut impl FnMut(i64, f64),
    ) -> bool {
        let Some(&last) = ts.last() else {
            return true;
        };
        let p = context.params();
        // Prometheus `matrixIterSlice` / `vectorSelectorSingle` (83962c35):
        // no sample after the last lookup can affect this grid. It still
        // claims overlap when the row completes, including stale markers.
        let final_end = p.at_ms.unwrap_or_else(|| {
            if context.steps() > 0 {
                context.step_at(context.steps() - 1)
            } else {
                p.start_ms
            }
        }) - p.offset_ms;
        if !cursor.started {
            cursor.position = self
                .last_t
                .map_or(0, |last| ts.partition_point(|t| *t <= last));
            cursor.started = true;
        }
        loop {
            // Do not ingest another allowance while a ready step is suspended.
            if self.ready(context) {
                self.advance_buffered(context, budget, emit);
                self.reduce_delta();
                if self.next_step >= context.steps() {
                    self.retire_history();
                }
                if self.ready(context) {
                    return false;
                }
            }
            if cursor.position == ts.len() {
                return true;
            }
            if budget.exhausted() {
                return false;
            }
            if self.next_step >= context.steps()
                || (matches!(context.kernel(), Kernel::Range(_)) && p.window_ms <= 0)
            {
                self.next_step = context.steps();
                self.last_t = self.last_t.max(Some(last));
                cursor.position = ts.len();
                return true;
            }
            // Searches skip dead prefixes without scanning them. Overlap was
            // removed only at row entry: equal timestamps within this row must
            // not acquire a new precedence rule at an ingestion boundary.
            let start = p
                .at_ms
                .unwrap_or_else(|| context.step_at(self.next_step))
                .saturating_sub(p.offset_ms)
                .saturating_sub(p.window_ms);
            cursor.position += ts[cursor.position..].partition_point(|t| *t <= start);
            if cursor.position == ts.len() || ts[cursor.position] > final_end {
                self.last_t = self.last_t.max(Some(last));
                cursor.position = ts.len();
                continue;
            }
            let from = cursor.position;
            let count = budget.take_up_to(INGEST_SAMPLES.min(ts.len() - from));
            let until = from + count;
            // Even a slice that straddles the last lookup copies no future tail.
            let end = from + ts[from..until].partition_point(|t| *t <= final_end);
            // A row may contain equal timestamps. Do not finalise a step
            // halfway through such a run just because an allowance ended.
            self.last_t = if until < ts.len() && ts[until] == ts[until - 1] {
                ts[until - 1].checked_sub(1)
            } else {
                Some(ts[until - 1])
            };
            let (slice_ts, slice_vs) = (&ts[from..end], &vs[from..end]);
            if self.ts.is_empty()
                && p.at_ms.is_none()
                && (matches!(context.kernel(), Kernel::Selector(_))
                    || !slice_vs.iter().any(|v| is_stale(*v)))
            {
                // Keep the borrowed fast path for small ordinary rows, but
                // inspect/walk at most one allowance, never the whole row.
                // If a step suspends, copy only its reachable suffix before
                // returning: ordinals survive the change of backing storage.
                debug_assert_eq!(self.lo, self.base);
                debug_assert_eq!(self.hi, self.base);
                self.advance(context, slice_ts, slice_vs, budget, emit);
                if self.next_step >= context.steps() {
                    self.retire_history();
                } else {
                    let keep = self.lo - self.base;
                    self.ts.extend_from_slice(&slice_ts[keep..]);
                    self.vs.extend_from_slice(&slice_vs[keep..]);
                    self.base = self.lo;
                }
            } else {
                self.append(context, slice_ts, slice_vs);
            }
            cursor.position = until;
        }
    }

    fn ready(&self, context: &EvalContext) -> bool {
        if self.next_step >= context.steps() {
            return false;
        }
        let p = context.params();
        if matches!(context.kernel(), Kernel::Range(_)) && p.window_ms <= 0 {
            return false;
        }
        let end = p.at_ms.unwrap_or_else(|| context.step_at(self.next_step)) - p.offset_ms;
        self.last_t.is_some_and(|last| end <= last)
    }

    fn close(
        &mut self,
        context: &EvalContext,
        budget: &mut WorkBudget,
        emit: &mut impl FnMut(i64, f64),
    ) -> bool {
        // A pinned value may still need repetition after history is retired.
        if !self.ts.is_empty() || self.pinned_value.is_some() {
            self.last_t = Some(i64::MAX);
            self.advance_buffered(context, budget, emit);
            self.reduce_delta();
            if self.ready(context) {
                return false;
            }
        }
        self.ts.clear();
        self.vs.clear();
        self.base = 0;
        self.lo = 0;
        self.hi = 0;
        self.next_step = 0;
        self.last_t = None;
        self.pinned_value = None;
        if let Some(sweep) = self.sweep.as_mut() {
            sweep.reset();
        }
        true
    }

    fn heap_size(&self) -> usize {
        self.ts.capacity() * std::mem::size_of::<i64>()
            + self.vs.capacity() * std::mem::size_of::<f64>()
            + self.sweep.as_ref().map_or(0, Sweep::heap_size)
    }

    /// `ts`/`vs` are the series' samples from ordinal `base` on, the
    /// buffer's or a chunk's.
    fn advance(
        &mut self,
        context: &EvalContext,
        ts: &[i64],
        vs: &[f64],
        budget: &mut WorkBudget,
        emit: &mut impl FnMut(i64, f64),
    ) {
        match context.kernel() {
            Kernel::Selector(pick) => advance_selector(self, context, ts, vs, pick, budget, emit),
            Kernel::Range(RangeOp::Func(func)) => {
                advance_range(self, context, ts, vs, func, budget, emit)
            }
            Kernel::Range(RangeOp::PredictLinear(seconds)) => {
                advance_predict_linear(self, context, ts, vs, f64::from_bits(seconds), budget, emit)
            }
        }
    }

    /// Lend the owned history without moving samples. Kernels keep absolute
    /// ordinals, including when a budget suspends an edge walk mid-step.
    fn advance_buffered(
        &mut self,
        context: &EvalContext,
        budget: &mut WorkBudget,
        emit: &mut impl FnMut(i64, f64),
    ) {
        let (ts, vs) = (std::mem::take(&mut self.ts), std::mem::take(&mut self.vs));
        self.advance(context, &ts, &vs, budget, emit);
        (self.ts, self.vs) = (ts, vs);
    }

    /// Copies at most one ingestion allowance. `push` already removed dead
    /// prefixes/overlap and advanced the readiness watermark before filtering
    /// stale markers: filtering must not give a later chunk a second claim.
    fn append(&mut self, context: &EvalContext, ts: &[i64], vs: &[f64]) {
        let p = context.params();
        match context.kernel() {
            // Pinned, the selector reads only the latest sample of its window.
            Kernel::Selector(_) if p.at_ms.is_some() => {
                if let (Some(&t), Some(&v)) = (ts.last(), vs.last()) {
                    self.ts.clear();
                    self.vs.clear();
                    self.ts.push(t);
                    self.vs.push(v);
                }
            }
            Kernel::Selector(_) => {
                self.ts.extend_from_slice(ts);
                self.vs.extend_from_slice(vs);
            }
            Kernel::Range(_) => {
                for (&t, &v) in ts.iter().zip(vs) {
                    if !is_stale(v) {
                        self.ts.push(t);
                        self.vs.push(v);
                    }
                }
            }
        }
    }

    fn retire_history(&mut self) {
        self.ts.clear();
        self.vs.clear();
        self.base = self.hi.max(self.lo);
        self.lo = self.base;
        self.hi = self.base;
        if let Some(sweep) = &mut self.sweep {
            sweep.reset();
        }
    }

    /// Prometheus `ReduceDelta` (83962c35): retire at least half the buffer so
    /// movement is amortised. Capacity follows the live-window high water plus
    /// INGEST_SAMPLES, never the raw chunk length. Between ready steps, the old
    /// window and the next window's arriving prefix can coexist; geometric Vec
    /// growth and the deferred dead prefix add only constant factors. Capacity
    /// is deliberately retained through the finish reservation checkpoint.
    fn reduce_delta(&mut self) {
        let cut = self.lo - self.base;
        if cut > 0 && cut >= self.ts.len() / 2 {
            self.ts.drain(..cut);
            self.vs.drain(..cut);
            self.base = self.lo;
        }
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{Array, AsArray, ListArray};

    use super::*;
    use crate::range::Func;
    use crate::selector::STALE_NAN_BITS;
    use crate::series::{self, SamplesBuilder};

    const M: i64 = 60_000;

    fn params() -> Params {
        Params {
            start_ms: 0,
            end_ms: 4 * M,
            step_ms: M,
            window_ms: 5 * M,
            offset_ms: 0,
            at_ms: None,
        }
    }

    /// Every finished row of `list`, as `(timestamp, value)` pairs.
    fn rows(list: &ListArray) -> Vec<Vec<(i64, f64)>> {
        (0..list.len())
            .map(|r| {
                let row = list.value(r);
                let (ts, vs) = series::sample_slices(row.as_struct());
                ts.iter().copied().zip(vs.iter().copied()).collect()
            })
            .collect()
    }

    fn window(it: &BufferedSeriesIterator) -> &WindowBuffer {
        let SeriesState::Window(window) = &it.state else {
            panic!("expected a multi-step selector or range window");
        };
        window
    }

    /// One series pushed as `chunks`, then closed. The Arrow owner, not
    /// numerical close, creates its one (possibly empty) row.
    fn select(p: Params, chunks: &[(&[i64], &[f64])]) -> Vec<(i64, f64)> {
        let context = EvalContext::new(Kernel::Selector(Pick::Value), p);
        let mut it = BufferedSeriesIterator::new(&context);
        let mut out = SamplesBuilder::default();
        for (ts, vs) in chunks {
            it.push(&context, ts, vs, |t, v| out.push(t, v));
        }
        it.close(&context, |t, v| out.push(t, v));
        out.finish_row();
        let mut rows = rows(&out.take_all());
        assert_eq!(rows.len(), 1);
        rows.pop().unwrap()
    }

    #[test]
    fn a_window_split_across_chunks_selects_as_one() {
        let out = select(
            params(),
            &[
                (&[0, 30_000], &[1.0, 2.0]),
                (&[90_000, 150_000], &[3.0, 4.0]),
            ],
        );
        assert_eq!(
            out,
            vec![(0, 1.0), (M, 2.0), (2 * M, 3.0), (3 * M, 4.0), (4 * M, 4.0)]
        );
    }

    #[test]
    fn a_step_is_answered_before_the_series_closes() {
        let context = EvalContext::new(Kernel::Selector(Pick::Value), params());
        let mut it = BufferedSeriesIterator::new(&context);
        let mut out = SamplesBuilder::default();
        it.push(&context, &[0, 90_000], &[1.0, 2.0], |t, v| out.push(t, v));
        // 0 and 1m can no longer change once 90s is seen; 2m can.
        assert_eq!(window(&it).next_step, 2);
        it.push(&context, &[150_000], &[3.0], |t, v| out.push(t, v));
        assert_eq!(window(&it).next_step, 3);
        it.close(&context, |t, v| out.push(t, v));
        out.finish_row();
        assert_eq!(
            rows(&out.take_all()),
            vec![vec![
                (0, 1.0),
                (M, 1.0),
                (2 * M, 2.0),
                (3 * M, 3.0),
                (4 * M, 3.0)
            ]]
        );
    }

    #[test]
    fn an_overlapping_chunk_loses_to_the_first() {
        let out = select(
            params(),
            &[
                (&[0, M, 2 * M], &[1.0, 2.0, 3.0]),
                (&[M, 90_000, 3 * M], &[9.0, 9.0, 4.0]),
            ],
        );
        assert_eq!(
            out,
            vec![(0, 1.0), (M, 2.0), (2 * M, 3.0), (3 * M, 4.0), (4 * M, 4.0)]
        );
    }

    #[test]
    fn a_stale_marker_claims_its_timestamp() {
        let stale = f64::from_bits(STALE_NAN_BITS);
        let out = select(
            params(),
            &[(&[0, M], &[1.0, stale]), (&[M, 2 * M], &[5.0, 6.0])],
        );
        assert_eq!(
            out,
            vec![(0, 1.0), (2 * M, 6.0), (3 * M, 6.0), (4 * M, 6.0)]
        );
    }

    #[test]
    fn a_chunk_before_every_window_is_never_buffered() {
        let p = Params {
            start_ms: 10 * M,
            end_ms: 12 * M,
            ..params()
        };
        let context = EvalContext::new(Kernel::Selector(Pick::Value), p);
        let mut it = BufferedSeriesIterator::new(&context);
        let mut out = SamplesBuilder::default();
        it.push(&context, &[0, 5 * M], &[1.0, 2.0], |t, v| out.push(t, v));
        assert!(window(&it).ts.is_empty());
        assert_eq!(window(&it).last_t, Some(5 * M));
        // Still the overlap reference: 5m again is a duplicate.
        it.push(&context, &[5 * M, 6 * M], &[7.0, 3.0], |t, v| {
            out.push(t, v)
        });
        it.close(&context, |t, v| out.push(t, v));
        out.finish_row();
        assert_eq!(rows(&out.take_all()), vec![vec![(10 * M, 3.0)]],);
    }

    #[test]
    fn at_pins_the_lookup_across_chunks() {
        let p = Params {
            at_ms: Some(M + 1),
            ..params()
        };
        let out = select(p, &[(&[0], &[1.0]), (&[M], &[2.0]), (&[2 * M], &[3.0])]);
        assert_eq!(
            out,
            vec![(0, 2.0), (M, 2.0), (2 * M, 2.0), (3 * M, 2.0), (4 * M, 2.0)]
        );
    }

    #[test]
    fn close_starts_the_next_series_from_nothing() {
        let context = EvalContext::new(Kernel::Selector(Pick::Value), params());
        let mut it = BufferedSeriesIterator::new(&context);
        let mut out = SamplesBuilder::default();
        it.push(&context, &[0, 3 * M], &[1.0, 2.0], |t, v| out.push(t, v));
        it.close(&context, |t, v| out.push(t, v));
        out.finish_row();
        // Earlier than the first series' last sample: not an overlap.
        it.push(&context, &[2 * M], &[5.0], |t, v| out.push(t, v));
        it.close(&context, |t, v| out.push(t, v));
        out.finish_row();
        assert_eq!(out.rows(), 2);
        it.close(&context, |_, _| panic!("a repeated close emitted a sample"));
        assert_eq!(out.rows(), 2, "numeric close must not append an empty row");
        // An explicit empty row still belongs to this Arrow owner.
        out.finish_row();
        assert_eq!(
            rows(&out.take_all()),
            vec![
                vec![(0, 1.0), (M, 1.0), (2 * M, 1.0), (3 * M, 2.0), (4 * M, 2.0)],
                vec![(2 * M, 5.0), (3 * M, 5.0), (4 * M, 5.0)],
                vec![],
            ]
        );
    }

    #[test]
    fn the_buffer_holds_the_window_not_the_series() {
        let context = EvalContext::new(
            Kernel::Selector(Pick::Value),
            Params {
                end_ms: 1000 * M,
                ..params()
            },
        );
        let mut it = BufferedSeriesIterator::new(&context);
        let mut out = SamplesBuilder::default();
        for i in 0..1000 {
            it.push(&context, &[i * M], &[i as f64], |t, v| out.push(t, v));
            assert!(
                window(&it).ts.len() <= 4,
                "{} samples buffered at {i}",
                window(&it).ts.len()
            );
        }
        it.close(&context, |t, v| out.push(t, v));
        out.finish_row();
        assert_eq!(rows(&out.take_all())[0].len(), 1001);
    }

    /// The kernels step by addition, and the step after the last one lies
    /// past `i64::MAX` here.
    #[test]
    fn a_grid_ending_at_the_end_of_time_does_not_overflow() {
        let p = Params {
            start_ms: i64::MAX - 2 * M,
            end_ms: i64::MAX,
            ..params()
        };
        let (ts, vs) = ([i64::MAX - 2 * M, i64::MAX - M], [1.0, 2.0]);
        for func in kernels() {
            let got = eval(func, p, &[(&ts, &vs)]);
            assert!(!got.is_empty(), "{}", name(func));
            assert!(got.iter().all(|(t, _)| *t >= p.start_ms), "{got:?}");
        }
    }

    /// Even one whole-series row must obey the ingestion allowance. Unlike
    /// the former whole-row in-place special case, this bound also applies
    /// with retained history and stale markers.
    #[test]
    fn a_chunk_is_ingested_in_bounded_slices() {
        let p = Params {
            end_ms: 1000 * M,
            ..params()
        };
        let (ts, vs): (Vec<i64>, Vec<f64>) = (0..1000).map(|i| (i * M, i as f64)).unzip();
        for func in kernels() {
            let context = EvalContext::new(kernel(func), p);
            let mut it = BufferedSeriesIterator::new(&context);
            let mut out = SamplesBuilder::default();
            it.push(&context, &ts, &vs, |t, v| out.push(t, v));
            assert!(
                window(&it).ts.capacity() <= 4 * 5 + 2 * INGEST_SAMPLES,
                "{}: {} samples of capacity after one chunk",
                name(func),
                window(&it).ts.capacity()
            );
            // A second chunk with the tail still buffered is appended to it.
            it.push(&context, &[1000 * M], &[1000.0], |t, v| out.push(t, v));
            it.close(&context, |t, v| out.push(t, v));
            out.finish_row();
            let whole = eval(
                func,
                p,
                &[(
                    &[ts.as_slice(), &[1000 * M]].concat(),
                    &[vs.as_slice(), &[1000.0]].concat(),
                )],
            );
            assert_bits(&rows(&out.take_all())[0], &whole, &name(func));
        }
    }

    // Range kernels and the chunk size sweep: whatever the cut points, a series
    // must answer bit for bit what it answers as one chunk.

    const S: i64 = 1000;

    const FUNCS: [Func; 17] = [
        Func::Rate,
        Func::Increase,
        Func::Delta,
        Func::Irate,
        Func::Idelta,
        Func::SumOverTime,
        Func::AvgOverTime,
        Func::MinOverTime,
        Func::MaxOverTime,
        Func::CountOverTime,
        Func::LastOverTime,
        Func::PresentOverTime,
        Func::StddevOverTime,
        Func::StdvarOverTime,
        Func::Changes,
        Func::Resets,
        Func::Deriv,
    ];

    /// `None` is the value selector; the sweeps use the same function set.
    fn kernel(func: Option<Func>) -> Kernel {
        func.map_or(Kernel::Selector(Pick::Value), |f| {
            Kernel::Range(RangeOp::Func(f))
        })
    }

    fn name(func: Option<Func>) -> &'static str {
        func.map_or("selector", |f| f.as_str())
    }

    /// Every kernel: the selector, then each range function.
    fn kernels() -> impl Iterator<Item = Option<Func>> {
        std::iter::once(None).chain(FUNCS.map(Some))
    }

    /// One series pushed as `chunks`, then closed, through any kernel.
    fn eval(func: Option<Func>, p: Params, chunks: &[(&[i64], &[f64])]) -> Vec<(i64, f64)> {
        let context = EvalContext::new(kernel(func), p);
        let mut it = BufferedSeriesIterator::new(&context);
        let mut out = SamplesBuilder::default();
        for (ts, vs) in chunks {
            it.push(&context, ts, vs, |t, v| out.push(t, v));
        }
        it.close(&context, |t, v| out.push(t, v));
        out.finish_row();
        let mut rows = rows(&out.take_all());
        assert_eq!(rows.len(), 1);
        rows.pop().unwrap()
    }

    /// Compared by bits: a cut point that changes the order of a float sum
    /// changes the answer, and `1e-9` would not see it.
    fn assert_bits(got: &[(i64, f64)], want: &[(i64, f64)], ctx: &dyn std::fmt::Display) {
        let bits = |s: &[(i64, f64)]| s.iter().map(|(t, v)| (*t, v.to_bits())).collect::<Vec<_>>();
        assert_eq!(
            bits(got),
            bits(want),
            "{ctx}\n got: {got:?}\nwant: {want:?}"
        );
    }

    /// Counter resets, NaN runs, a staleness marker, signed zeros and a gap
    /// wider than the window, as range.rs's suite uses: every branch of
    /// every kernel as a 2m window slides over it.
    fn a_rough_series() -> (Vec<i64>, Vec<f64>) {
        let stale = f64::from_bits(STALE_NAN_BITS);
        [
            (0, 1.0),
            (30 * S, 2.0),
            (60 * S, 2.0),
            (90 * S, 1.0),
            (120 * S, f64::NAN),
            (150 * S, 5.0),
            (180 * S, stale),
            (210 * S, 4.0),
            (240 * S, -0.0),
            (270 * S, 0.0),
            (300 * S, 9.0),
            (330 * S, 9.0),
            (360 * S, 8.0),
            (900 * S, f64::NAN),
            (930 * S, f64::NAN),
            (960 * S, 3.0),
            (990 * S, 3.0),
            (1020 * S, 2.0),
        ]
        .into_iter()
        .unzip()
    }

    /// Grids that put window edges on samples, between them, pinned before
    /// the data ends and pinned after it.
    fn rough_grids() -> Vec<Params> {
        let plain = Params {
            start_ms: 0,
            end_ms: 20 * M,
            step_ms: 15 * S,
            window_ms: 2 * M,
            offset_ms: 45 * S,
            at_ms: None,
        };
        vec![
            plain,
            Params {
                offset_ms: 0,
                ..plain
            },
            Params {
                window_ms: 5 * M,
                step_ms: M,
                ..plain
            },
            Params {
                at_ms: Some(5 * M),
                ..plain
            },
            Params {
                at_ms: Some(17 * M),
                ..plain
            },
        ]
    }

    fn split<'a>(ts: &'a [i64], vs: &'a [f64], k: usize) -> Vec<(&'a [i64], &'a [f64])> {
        ts.chunks(k).zip(vs.chunks(k)).collect()
    }

    /// An independent fresh-window oracle: it does not share edge cursors,
    /// sweeps, ingestion or pinned-value replication with the streaming walk.
    fn fresh(kernel: Kernel, p: Params, ts: &[i64], vs: &[f64]) -> Vec<(i64, f64)> {
        let context = EvalContext::new(kernel, p);
        context
            .timestamps()
            .filter_map(|step| {
                let end = p.at_ms.unwrap_or(step) - p.offset_ms;
                let a = ts.partition_point(|t| *t <= end - p.window_ms);
                let b = ts.partition_point(|t| *t <= end);
                let value = match kernel {
                    Kernel::Selector(pick) => {
                        (a < b && !is_stale(vs[b - 1])).then(|| pick.of(ts[b - 1], vs[b - 1]))
                    }
                    Kernel::Range(RangeOp::Func(func)) => {
                        let (ts, vs): (Vec<_>, Vec<_>) = ts[a..b]
                            .iter()
                            .copied()
                            .zip(vs[a..b].iter().copied())
                            .filter(|(_, v)| !is_stale(*v))
                            .unzip();
                        crate::range::evaluate(
                            func,
                            &crate::range::Window {
                                ts: &ts,
                                vs: &vs,
                                range_start: end - p.window_ms,
                                range_end: end,
                                range_ms: p.window_ms,
                            },
                        )
                    }
                    Kernel::Range(RangeOp::PredictLinear(seconds)) => {
                        let (ts, vs): (Vec<_>, Vec<_>) = ts[a..b]
                            .iter()
                            .copied()
                            .zip(vs[a..b].iter().copied())
                            .filter(|(_, v)| !is_stale(*v))
                            .unzip();
                        crate::range::predict_linear(
                            &crate::range::Window {
                                ts: &ts,
                                vs: &vs,
                                range_start: end - p.window_ms,
                                range_end: end,
                                range_ms: p.window_ms,
                            },
                            f64::from_bits(seconds),
                            step,
                        )
                    }
                };
                value.map(|v| (step, v))
            })
            .collect()
    }

    fn budgeted(
        kernel: Kernel,
        p: Params,
        chunks: &[(&[i64], &[f64])],
        units: usize,
    ) -> Vec<(i64, f64)> {
        let context = EvalContext::new(kernel, p);
        let mut it = BufferedSeriesIterator::new(&context);
        let mut out = Vec::new();
        for &(ts, vs) in chunks {
            let mut cursor = PushCursor::default();
            loop {
                let before = out.len();
                let mut budget = WorkBudget::new(units);
                let done = it.push_budgeted(&context, ts, vs, &mut cursor, &mut budget, |t, v| {
                    out.push((t, v))
                });
                assert!(out.len() - before <= units);
                if done {
                    break;
                }
                assert!(budget.exhausted());
            }
        }
        loop {
            let before = out.len();
            let mut budget = WorkBudget::new(units);
            let done = it.close_budgeted(&context, &mut budget, |t, v| out.push((t, v)));
            assert!(out.len() - before <= units);
            if done {
                break;
            }
            assert!(budget.exhausted());
        }
        it.close(&context, |_, _| panic!("resumed close emitted twice"));
        out
    }

    #[test]
    fn sample_and_step_budgets_match_fresh_kernels_bitwise() {
        let (ts, vs) = a_rough_series();
        for p in rough_grids().into_iter().chain([at_10m()]) {
            for kernel in kernels()
                .map(kernel)
                .chain([Kernel::Selector(Pick::Timestamp)])
                .chain([Kernel::Range(RangeOp::predict_linear(3600.0))])
            {
                let expected = fresh(kernel, p, &ts, &vs);
                for units in [1, 2, 7, 31, 255, 256, 257, 8192] {
                    // Repeated heads include stale markers and conflicting
                    // values. They must still lose after a budget boundary.
                    let mut chunks = split(&ts, &vs, 3);
                    chunks.insert(1, (&ts[..3], &[91., 92., 93.]));
                    let got = budgeted(kernel, p, &chunks, units);
                    assert_bits(&got, &expected, &format!("{kernel:?} {p:?} units={units}"));
                }
            }
        }
    }

    #[test]
    fn a_small_unpinned_range_row_still_borrows_its_samples() {
        let ts: Vec<_> = (0..20).collect();
        let vs: Vec<_> = (0..20).map(|i| i as f64).collect();
        let p = Params {
            start_ms: 19,
            end_ms: 19,
            step_ms: 1,
            window_ms: 20,
            offset_ms: 0,
            at_ms: None,
        };
        for func in FUNCS {
            let kernel = Kernel::Range(RangeOp::Func(func));
            let context = EvalContext::new(kernel, p);
            let mut it = BufferedSeriesIterator::new(&context);
            let mut out = Vec::new();
            assert!(it.push_budgeted(
                &context,
                &ts,
                &vs,
                &mut PushCursor::default(),
                &mut WorkBudget::new(8192),
                |t, v| out.push((t, v))
            ));
            assert_eq!(window(&it).ts.capacity(), 0);
            assert_eq!(window(&it).vs.capacity(), 0);
            it.close(&context, |t, v| out.push((t, v)));
            assert_bits(&out, &fresh(kernel, p, &ts, &vs), &func.as_str());
        }
    }

    #[test]
    fn one_indivisible_range_fold_is_charged_before_another_step_runs() {
        let context = EvalContext::new(
            Kernel::Range(RangeOp::Func(Func::SumOverTime)),
            Params {
                start_ms: 10_000,
                end_ms: 10_009,
                step_ms: 1,
                window_ms: 20_000,
                offset_ms: 0,
                at_ms: None,
            },
        );
        let ts: Vec<_> = (0..10_000).collect();
        let vs = vec![1.; ts.len()];
        let mut it = BufferedSeriesIterator::new(&context);
        it.push(&context, &ts, &vs, |_, _| panic!("first step is not ready"));
        let mut out = Vec::new();
        assert!(
            !it.close_budgeted(&context, &mut WorkBudget::new(20_000), |t, v| out
                .push((t, v)))
        );
        assert_eq!(window(&it).next_step, 1);
        assert_eq!(out, vec![(10_000, 10_000.)]);
        assert!(!it.close_budgeted(&context, &mut WorkBudget::new(1), |t, v| out.push((t, v))));
        assert_eq!(window(&it).next_step, 2);
        it.close(&context, |t, v| out.push((t, v)));
        assert_eq!(
            out,
            (10_000..10_010).map(|t| (t, 10_000.)).collect::<Vec<_>>()
        );
    }

    #[test]
    fn suspended_sweep_pops_and_duplicate_runs_do_not_change_float_order() {
        // A descending run makes min's queue long for max; the last value
        // pops that queue. Equal timestamps cross many ingestion boundaries.
        let ts: Vec<_> = (0..600).chain(std::iter::repeat_n(600, 600)).collect();
        let vs: Vec<_> = (0..1200)
            .map(|i| if i == 1199 { 1000. } else { 600. - i as f64 })
            .collect();
        let p = Params {
            start_ms: 600,
            end_ms: 605,
            step_ms: 1,
            window_ms: 1000,
            offset_ms: 0,
            at_ms: None,
        };
        let kernels = FUNCS
            .map(|func| Kernel::Range(RangeOp::Func(func)))
            .into_iter()
            .chain([Kernel::Range(RangeOp::predict_linear(3600.0))]);
        for kernel in kernels {
            let expected = fresh(kernel, p, &ts, &vs);
            for units in [1, 2, 3, 257] {
                assert_bits(
                    &budgeted(kernel, p, &[(&ts, &vs)], units),
                    &expected,
                    &format!("{kernel:?} units={units}"),
                );
            }
        }
    }

    #[test]
    fn empty_predict_linear_windows_still_charge_one_unit_per_step() {
        // One sample at the first step; past the fifth step every window is
        // empty, so neither edge moves and the fold charges nothing. Only
        // the per-step charge can stop the loop there.
        let context = EvalContext::new(
            Kernel::Range(RangeOp::predict_linear(3600.0)),
            Params {
                start_ms: 1000 * S,
                end_ms: 1000 * S + 9_999 * S,
                step_ms: S,
                window_ms: 5 * S,
                offset_ms: 0,
                at_ms: None,
            },
        );
        for units in [1, 2, 3, 257] {
            let mut it = BufferedSeriesIterator::new(&context);
            it.push(&context, &[1000 * S], &[1.], |_, _| {});
            assert_eq!(window(&it).next_step, 1);
            let before = window(&it).next_step;
            assert!(
                !it.close_budgeted(&context, &mut WorkBudget::new(units), |_, _| {}),
                "units={units}: one budgeted close finished all 10000 steps"
            );
            let ran = window(&it).next_step - before;
            assert!(ran <= units as i128, "units={units}: ran {ran} steps");
        }
    }

    fn assert_history_capacity(it: &BufferedSeriesIterator, bound: usize) {
        let w = window(it);
        assert!(
            w.ts.capacity() <= bound,
            "timestamp capacity {} > {bound}",
            w.ts.capacity()
        );
        assert!(
            w.vs.capacity() <= bound,
            "value capacity {} > {bound}",
            w.vs.capacity()
        );
        assert!(w.sweep.as_ref().map_or(0, Sweep::heap_size) <= bound * 8);
    }

    #[test]
    fn huge_stale_and_second_chunks_bound_capacity_at_every_resume() {
        let ts: Vec<_> = (0..200_000).collect();
        let stale = f64::from_bits(STALE_NAN_BITS);
        for stale_mode in 0..3 {
            let vs: Vec<_> = ts
                .iter()
                .map(|t| {
                    if stale_mode == 2 || (stale_mode == 1 && t % 3 == 0) {
                        stale
                    } else {
                        (t % 17) as f64
                    }
                })
                .collect();
            let p = Params {
                start_ms: 0,
                end_ms: 200_000,
                step_ms: 16,
                window_ms: 32,
                offset_ms: 0,
                at_ms: None,
            };
            for kernel in [
                Kernel::Selector(Pick::Value),
                Kernel::Range(RangeOp::Func(Func::Rate)),
                Kernel::Range(RangeOp::Func(Func::MinOverTime)),
                Kernel::Range(RangeOp::Func(Func::SumOverTime)),
            ] {
                let context = EvalContext::new(kernel, p);
                let mut it = BufferedSeriesIterator::new(&context);
                // Nonempty history forces the old whole-append path, even
                // without stale markers in the second chunk.
                it.push(&context, &ts[..20], &vs[..20], |_, _| {});
                let mut cursor = PushCursor::default();
                loop {
                    let done = it.push_budgeted(
                        &context,
                        &ts[20..],
                        &vs[20..],
                        &mut cursor,
                        &mut WorkBudget::new(4096),
                        |_, _| {},
                    );
                    // Conservative constant factors cover deferred front
                    // retirement, both adjacent windows and geometric growth.
                    assert_history_capacity(&it, 8 * 32 + 4 * INGEST_SAMPLES + 8);
                    if done {
                        break;
                    }
                }
                it.close(&context, |_, _| {});
                assert_history_capacity(&it, 8 * 32 + 4 * INGEST_SAMPLES + 8);
            }
        }
    }

    #[test]
    fn huge_unreachable_future_tails_and_pinned_windows_never_determine_capacity() {
        let ts: Vec<_> = (0..1_000_000).collect();
        let vs = vec![1.; ts.len()];
        for pinned in [false, true] {
            let p = Params {
                start_ms: 0,
                end_ms: 17,
                step_ms: 16,
                window_ms: 32,
                offset_ms: 7,
                at_ms: pinned.then_some(1024),
            };
            for kernel in [
                Kernel::Selector(Pick::Value),
                Kernel::Range(RangeOp::Func(Func::Rate)),
                Kernel::Range(RangeOp::Func(Func::SumOverTime)),
            ] {
                let context = EvalContext::new(kernel, p);
                let mut it = BufferedSeriesIterator::new(&context);
                let mut cursor = PushCursor::default();
                let mut out = Vec::new();
                loop {
                    let done = it.push_budgeted(
                        &context,
                        &ts,
                        &vs,
                        &mut cursor,
                        &mut WorkBudget::new(7),
                        |t, v| out.push((t, v)),
                    );
                    // Only the actual final-grid/pinned window is copied, not
                    // even one allowance of the million-sample future tail.
                    assert_history_capacity(&it, 64);
                    if done {
                        break;
                    }
                }
                it.close(&context, |t, v| out.push((t, v)));
                assert_bits(
                    &out,
                    &fresh(kernel, p, &ts, &vs),
                    &format!("{kernel:?} pinned={pinned}"),
                );
                assert_history_capacity(&it, 64);
                assert!(window(&it).ts.is_empty());
            }
        }
    }

    #[test]
    fn chunk_size_sweep() {
        let (ts, vs) = a_rough_series();
        for p in rough_grids() {
            for func in kernels() {
                let whole = eval(func, p, &[(&ts, &vs)]);
                for k in 1..=ts.len() {
                    let got = eval(func, p, &split(&ts, &vs, k));
                    assert_bits(&got, &whole, &format!("{} k={k} {p:?}", name(func)));
                }
            }
        }
    }

    /// One step at 10m over a 5m window: `(5m, 10m]`.
    fn at_10m() -> Params {
        Params {
            start_ms: 10 * M,
            end_ms: 10 * M,
            step_ms: M,
            window_ms: 5 * M,
            offset_ms: 0,
            at_ms: None,
        }
    }

    #[test]
    fn cached_context_grid_handles_empty_and_extreme_domains() {
        let kernel = Kernel::Selector(Pick::Value);
        for p in [
            Params {
                end_ms: -1,
                ..params()
            },
            Params {
                step_ms: 0,
                ..params()
            },
        ] {
            let context = EvalContext::new(kernel, p);
            assert_eq!(context.kernel(), kernel);
            assert_eq!(context.params(), p);
            assert_eq!(context.steps(), 0);
            assert_eq!(context.timestamps().count(), 0);
        }
        let context = EvalContext::new(
            kernel,
            Params {
                start_ms: i64::MAX - 2,
                end_ms: i64::MAX,
                step_ms: 1,
                ..params()
            },
        );
        assert_eq!(context.steps(), 3);
        assert_eq!(
            context.timestamps().collect::<Vec<_>>(),
            [i64::MAX - 2, i64::MAX - 1, i64::MAX]
        );
        let context = EvalContext::new(
            kernel,
            Params {
                start_ms: i64::MIN,
                end_ms: i64::MAX,
                step_ms: 1,
                ..params()
            },
        );
        assert_eq!(context.steps(), i128::from(u64::MAX) + 1);
    }

    #[test]
    fn one_step_candidate_has_no_heap_and_takes_original_sample_on_close() {
        let stale = f64::from_bits(STALE_NAN_BITS);
        let nan = f64::from_bits(0x7ff8_0000_0000_1234);
        for pick in [Pick::Value, Pick::Timestamp] {
            let context = EvalContext::new(Kernel::Selector(pick), at_10m());
            let mut it = BufferedSeriesIterator::new(&context);
            let inline = std::mem::size_of::<BufferedSeriesIterator>();
            for value in [-0.0, nan, stale] {
                it.push(&context, &[9 * M], &[value], |_, _| {
                    panic!("candidate emitted before close")
                });
                // Beyond the lookup: still claims overlap, but does not
                // erase the original sample or convert a stale marker.
                it.push(&context, &[11 * M], &[stale], |_, _| {
                    panic!("candidate emitted before close")
                });
                it.push(
                    &context,
                    &[9 * M, 10 * M, 12 * M],
                    &[7.0, 8.0, 9.0],
                    |_, _| panic!("candidate emitted before close"),
                );
                assert_eq!(it.size(), inline);
                let SeriesState::Candidate { sample, last_t } = &it.state else {
                    panic!("one step allocated a window");
                };
                let (t, v) = sample.unwrap();
                assert_eq!(t, 9 * M);
                assert_eq!(v.to_bits(), value.to_bits());
                assert_eq!(*last_t, Some(12 * M));
                let mut got = Vec::new();
                it.close(&context, |t, v| got.push((t, v)));
                if is_stale(value) {
                    assert!(got.is_empty());
                } else {
                    let want = if pick == Pick::Value { value } else { 540.0 };
                    assert_bits(&got, &[(10 * M, want)], &"one step");
                }
                let SeriesState::Candidate { sample, last_t } = &it.state else {
                    unreachable!();
                };
                assert!(sample.is_none() && last_t.is_none());
                it.close(&context, |_, _| panic!("candidate repeated on close"));
                assert_eq!(it.size(), inline);
            }
        }
    }

    #[test]
    fn close_keeps_all_window_and_sweep_allocations_and_reuses_them() {
        for func in [
            Func::Rate,
            Func::MinOverTime,
            Func::MaxOverTime,
            Func::LastOverTime,
        ] {
            let context = EvalContext::new(Kernel::Range(RangeOp::Func(func)), params());
            let mut it = BufferedSeriesIterator::new(&context);
            it.push(
                &context,
                &[0, M, 2 * M, 3 * M],
                &[3.0, 2.0, -0.0, 0.0],
                |_, _| {},
            );
            let window = window(&it);
            let heap = window.ts.capacity() * std::mem::size_of::<i64>()
                + window.vs.capacity() * std::mem::size_of::<f64>()
                + window.sweep.as_ref().map_or(0, Sweep::heap_size);
            if matches!(func, Func::Rate | Func::MinOverTime | Func::MaxOverTime) {
                assert!(window.sweep.as_ref().unwrap().heap_size() > 0);
            }
            assert_eq!(
                it.size(),
                std::mem::size_of::<BufferedSeriesIterator>() + heap
            );
            let before = it.size();
            it.close(&context, |_, _| {});
            assert_eq!(it.size(), before);
            it.close(&context, |_, _| panic!("window repeated on close"));
            assert_eq!(it.size(), before);
            let mut got = Vec::new();
            it.push(&context, &[0, M], &[1.0, 2.0], |t, v| got.push((t, v)));
            it.close(&context, |t, v| got.push((t, v)));
            let want = eval(Some(func), params(), &[(&[0, M], &[1.0, 2.0])]);
            assert_bits(&got, &want, &func.as_str());
        }
    }

    #[test]
    fn boundary_on_range_start() {
        let context = EvalContext::new(Kernel::Range(RangeOp::Func(Func::CountOverTime)), at_10m());
        let mut it = BufferedSeriesIterator::new(&context);
        let mut out = SamplesBuilder::default();
        it.push(
            &context,
            &[3 * M, 4 * M, 5 * M],
            &[1.0, 2.0, 3.0],
            |t, v| out.push(t, v),
        );
        // The window is open at its start, so 5m is outside it.
        assert!(window(&it).ts.is_empty(), "buffered {:?}", window(&it).ts);
        it.push(&context, &[6 * M], &[4.0], |t, v| out.push(t, v));
        it.close(&context, |t, v| out.push(t, v));
        out.finish_row();
        assert_eq!(rows(&out.take_all()), vec![vec![(10 * M, 1.0)]]);
    }

    #[test]
    fn boundary_on_range_end() {
        let p = Params {
            end_ms: 11 * M,
            ..at_10m()
        };
        let context = EvalContext::new(Kernel::Range(RangeOp::Func(Func::CountOverTime)), p);
        let mut it = BufferedSeriesIterator::new(&context);
        let mut out = SamplesBuilder::default();
        it.push(&context, &[9 * M, 10 * M], &[1.0, 2.0], |t, v| {
            out.push(t, v)
        });
        // 10m's window is closed at its end: nothing later can enter it.
        assert_eq!(window(&it).next_step, 1);
        it.push(&context, &[10 * M + 30 * S], &[3.0], |t, v| out.push(t, v));
        assert_eq!(window(&it).next_step, 1);
        it.push(&context, &[11 * M], &[4.0], |t, v| out.push(t, v));
        assert_eq!(window(&it).next_step, 2);
        it.close(&context, |t, v| out.push(t, v));
        out.finish_row();
        assert_eq!(
            rows(&out.take_all()),
            vec![vec![(10 * M, 2.0), (11 * M, 4.0)]]
        );
    }

    #[test]
    fn boundary_on_step_with_duplicate_head() {
        let ts = [6 * M, 7 * M, 8 * M, 9 * M, 10 * M, 11 * M];
        let vs = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0];
        let p = Params {
            end_ms: 11 * M,
            ..at_10m()
        };
        for func in [Func::CountOverTime, Func::Rate] {
            let whole = eval(Some(func), p, &[(&ts, &vs)]);
            // The next chunk repeats 10m, the first step's range end, with a
            // value that would read as a counter reset.
            let got = eval(
                Some(func),
                p,
                &[(&ts[..5], &vs[..5]), (&[10 * M, 11 * M], &[0.0, 6.0])],
            );
            assert_bits(&got, &whole, &func.as_str());
        }
    }

    #[test]
    fn counter_reset_on_the_chunk_boundary() {
        let ts: Vec<i64> = (0..=10).map(|i| i * 30 * S).collect();
        let vs = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 1.0, 2.0, 3.0, 4.0, 5.0];
        let p = Params {
            start_ms: 5 * M,
            end_ms: 5 * M,
            step_ms: M,
            window_ms: 5 * M,
            offset_ms: 0,
            at_ms: None,
        };
        for func in [Func::Rate, Func::Increase, Func::Resets, Func::Irate] {
            let whole = eval(Some(func), p, &[(&ts, &vs)]);
            // The reset is the first sample of the second chunk, so the
            // value it compares against lives only in the first.
            let got = eval(Some(func), p, &[(&ts[..6], &vs[..6]), (&ts[6..], &vs[6..])]);
            assert_bits(&got, &whole, &func.as_str());
        }
        assert_eq!(
            eval(
                Some(Func::Resets),
                p,
                &[(&ts[..6], &vs[..6]), (&ts[6..], &vs[6..])]
            ),
            vec![(5 * M, 1.0)]
        );
        // Increase over (0, 5m]: 2 → 6, reset, 1 → 5, extrapolated.
        let increase = eval(
            Some(Func::Increase),
            p,
            &[(&ts[..6], &vs[..6]), (&ts[6..], &vs[6..])],
        );
        assert!((increase[0].1 - 10.0).abs() < 1e-9, "{increase:?}");
    }

    /// A.0 and B.0 of docs/engine-blocks.md §1, and the seven rates of §5.
    #[test]
    fn the_engine_blocks_fixture() {
        let p = Params {
            start_ms: 420 * S,
            end_ms: 600 * S,
            step_ms: 30 * S,
            window_ms: 300 * S,
            offset_ms: 0,
            at_ms: None,
        };
        let a0: (Vec<i64>, Vec<f64>) = [300, 330, 360, 390, 420]
            .into_iter()
            .map(|t| t * S)
            .zip([10.0, 11.0, 12.0, 13.0, 14.0])
            .unzip();
        let b0: (Vec<i64>, Vec<f64>) = [450, 480, 510, 540, 570, 600]
            .into_iter()
            .map(|t| t * S)
            .zip([15.0, 16.0, 3.0, 4.0, 5.0, 6.0])
            .unzip();

        let context = EvalContext::new(Kernel::Range(RangeOp::Func(Func::Rate)), p);
        let mut it = BufferedSeriesIterator::new(&context);
        let mut out = SamplesBuilder::default();
        it.push(&context, &a0.0, &a0.1, |t, v| out.push(t, v));
        // The lane at 420s closes before B.0 opens.
        assert_eq!(window(&it).next_step, 1);
        it.push(&context, &b0.0, &b0.1, |t, v| out.push(t, v));
        it.close(&context, |t, v| out.push(t, v));
        out.finish_row();
        let got = rows(&out.take_all()).pop().unwrap();

        let want = [
            (420, 0.015),
            (450, 0.0183333333333),
            (480, 0.0216666666667),
            (510, 0.0321428571429),
            (540, 0.0354166666667),
            (570, 0.0407407407407),
            (600, 0.0407407407407),
        ];
        assert_eq!(got.len(), want.len(), "{got:?}");
        for ((t, v), (wt, wv)) in got.iter().zip(want) {
            assert_eq!(*t, wt * S);
            assert!((v - wv).abs() < 1e-10, "at {wt}s: {v} vs {wv}");
        }
    }

    #[test]
    fn overlap_keeps_the_first_chunk() {
        let p = Params {
            start_ms: 3 * M,
            end_ms: 3 * M,
            step_ms: M,
            window_ms: 5 * M,
            offset_ms: 0,
            at_ms: None,
        };
        // Interleaved: the union would be six samples, the first chunk keeps
        // its three and only 150s of the second survives.
        let chunks: [(&[i64], &[f64]); 2] = [
            (&[0, 60 * S, 120 * S], &[1.0, 2.0, 3.0]),
            (&[30 * S, 90 * S, 150 * S], &[7.0, 8.0, 9.0]),
        ];
        assert_eq!(
            eval(Some(Func::CountOverTime), p, &chunks),
            vec![(3 * M, 4.0)]
        );
        assert_eq!(
            eval(Some(Func::SumOverTime), p, &chunks),
            vec![(3 * M, 15.0)]
        );

        // A stale marker is not a sample, but it still claims its timestamp:
        // the second chunk's 120s is a duplicate, not a value.
        let stale = f64::from_bits(STALE_NAN_BITS);
        let chunks: [(&[i64], &[f64]); 2] = [
            (&[0, 60 * S, 120 * S], &[1.0, 2.0, stale]),
            (&[120 * S, 150 * S], &[5.0, 6.0]),
        ];
        assert_eq!(
            eval(Some(Func::CountOverTime), p, &chunks),
            vec![(3 * M, 3.0)]
        );
        assert_eq!(
            eval(Some(Func::SumOverTime), p, &chunks),
            vec![(3 * M, 9.0)]
        );
    }

    /// range.rs's 400-case fuzz, split at random cut points with empty
    /// chunks and repeated heads thrown in, against the same series as one
    /// chunk. A repeated head is the previous chunk's last timestamp with a
    /// different value, so answering from it would show.
    #[test]
    fn fuzz_random_cuts_empty_chunks_and_duplicated_heads() {
        let mut seed: u64 = 0x2545F4914F6CDD1D;
        let mut next = move || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        for case in 0..400 {
            let n = (next() % 25) as usize;
            let mut ts: Vec<i64> = Vec::new();
            let mut vs: Vec<f64> = Vec::new();
            let mut t = 0i64;
            let mut v = 0.0f64;
            for _ in 0..n {
                t += ((next() % 6) as i64) * 30 * S;
                if ts.last() == Some(&t) {
                    continue;
                }
                ts.push(t);
                v = match next() % 10 {
                    0 => f64::NAN,
                    1 => f64::from_bits(STALE_NAN_BITS),
                    2 => -0.0,
                    3 => 0.0,
                    4 => v - (next() % 5) as f64,
                    _ => v + (next() % 5) as f64,
                };
                vs.push(v);
            }
            let p = Params {
                start_ms: 0,
                end_ms: (t + 5 * M).max(M),
                step_ms: 15 * S,
                window_ms: (1 + next() % 8) as i64 * 30 * S,
                offset_ms: (next() % 4) as i64 * 15 * S,
                at_ms: (next() % 5 == 0).then(|| (next() % 20) as i64 * 30 * S),
            };

            let mut chunks: Vec<(Vec<i64>, Vec<f64>)> = Vec::new();
            let mut from = 0;
            while from < ts.len() {
                let to = (from + (next() % 5) as usize).min(ts.len());
                let (mut cts, mut cvs) = (ts[from..to].to_vec(), vs[from..to].to_vec());
                if from > 0 && next() % 3 == 0 {
                    cts.insert(0, ts[from - 1]);
                    cvs.insert(0, vs[from - 1] + 100.0);
                }
                chunks.push((cts, cvs));
                from = to;
            }
            let chunks: Vec<(&[i64], &[f64])> = chunks
                .iter()
                .map(|(t, v)| (t.as_slice(), v.as_slice()))
                .collect();

            for func in kernels() {
                let whole = eval(func, p, &[(&ts, &vs)]);
                let got = eval(func, p, &chunks);
                assert_bits(
                    &got,
                    &whole,
                    &format!("case {case} {} ts={ts:?} vs={vs:?} p={p:?}", name(func)),
                );
            }
        }
    }

    #[test]
    fn steps_finalise_before_close() {
        let p = Params {
            start_ms: 0,
            end_ms: 1000 * M,
            step_ms: M,
            window_ms: 5 * M,
            offset_ms: 0,
            at_ms: None,
        };
        let (ts, vs): (Vec<i64>, Vec<f64>) = (0..1000).map(|i| (i * M, i as f64)).unzip();
        // A 5m window at 1m spacing holds five samples.
        let window_size = 5;
        for chunk in [1, 3, 10, 50] {
            let context = EvalContext::new(Kernel::Range(RangeOp::Func(Func::Rate)), p);
            let mut it = BufferedSeriesIterator::new(&context);
            let mut out = SamplesBuilder::default();
            let empty = out.size();
            for (i, (cts, cvs)) in split(&ts, &vs, chunk).into_iter().enumerate() {
                let steps = window(&it).next_step;
                it.push(&context, cts, cvs, |t, v| out.push(t, v));
                assert!(
                    window(&it).next_step > steps,
                    "chunk {chunk}: no step finalised at {i}"
                );
                // Every finalised step but the first, whose window holds one
                // sample, has a rate, and `size` counts at least the samples
                // held: they are in `out` before the row is finished.
                let held = (window(&it).next_step - 1) as usize * 16;
                assert!(
                    out.size() >= empty + held,
                    "chunk {chunk}: out holds fewer than {} steps after chunk {i}",
                    window(&it).next_step - 1
                );
                // What survives a push is the window of the first step not
                // yet answered, plus a dead prefix `reduce_delta` keeps while
                // it is under half the buffer: under two windows, whatever
                // the chunk size. "Window plus one chunk" is the peak inside
                // `push`, before the steps are walked.
                assert!(
                    window(&it).ts.len() < 2 * window_size,
                    "chunk {chunk}: {} samples buffered after chunk {i}",
                    window(&it).ts.len()
                );
            }
            it.close(&context, |t, v| out.push(t, v));
            out.finish_row();
            assert_eq!(rows(&out.take_all())[0].len(), 1000);
        }
    }
}
