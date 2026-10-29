//! Range-vector numerical kernels after `matrixIterSlice`, `extrapolatedRate`,
//! `instantValue` and the `*_over_time` functions in Prometheus's
//! `promql/functions.go` and `engine.go` at 83962c35. [`crate::series_eval`]
//! drives one ID's chunks; `advance_range` answers its block-local steps.
//! Floats only; native histograms are a later slice.

use std::collections::VecDeque;
use std::fmt;

use crate::buffer::{EvalContext, WindowBuffer, WorkBudget};
use crate::math;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Hash)]
pub enum Func {
    Rate,
    Increase,
    Delta,
    Irate,
    Idelta,
    SumOverTime,
    AvgOverTime,
    MinOverTime,
    MaxOverTime,
    CountOverTime,
    LastOverTime,
    PresentOverTime,
    StddevOverTime,
    StdvarOverTime,
    Changes,
    Resets,
    Deriv,
}

impl Func {
    pub fn parse(s: &str) -> Option<Func> {
        Some(match s {
            "rate" => Func::Rate,
            "increase" => Func::Increase,
            "delta" => Func::Delta,
            "irate" => Func::Irate,
            "idelta" => Func::Idelta,
            "sum_over_time" => Func::SumOverTime,
            "avg_over_time" => Func::AvgOverTime,
            "min_over_time" => Func::MinOverTime,
            "max_over_time" => Func::MaxOverTime,
            "count_over_time" => Func::CountOverTime,
            "last_over_time" => Func::LastOverTime,
            "present_over_time" => Func::PresentOverTime,
            "stddev_over_time" => Func::StddevOverTime,
            "stdvar_over_time" => Func::StdvarOverTime,
            "changes" => Func::Changes,
            "resets" => Func::Resets,
            "deriv" => Func::Deriv,
            _ => return None,
        })
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Func::Rate => "rate",
            Func::Increase => "increase",
            Func::Delta => "delta",
            Func::Irate => "irate",
            Func::Idelta => "idelta",
            Func::SumOverTime => "sum_over_time",
            Func::AvgOverTime => "avg_over_time",
            Func::MinOverTime => "min_over_time",
            Func::MaxOverTime => "max_over_time",
            Func::CountOverTime => "count_over_time",
            Func::LastOverTime => "last_over_time",
            Func::PresentOverTime => "present_over_time",
            Func::StddevOverTime => "stddev_over_time",
            Func::StdvarOverTime => "stdvar_over_time",
            Func::Changes => "changes",
            Func::Resets => "resets",
            Func::Deriv => "deriv",
        }
    }

    /// Work in an indivisible value fold; O(1) kernels are already charged
    /// by the step. This is sample work, not a timing estimate or preemption.
    fn value_work(self, samples: usize) -> usize {
        match self {
            Self::Rate
            | Self::Increase
            | Self::SumOverTime
            | Self::AvgOverTime
            | Self::MinOverTime
            | Self::MaxOverTime
            | Self::StddevOverTime
            | Self::StdvarOverTime
            | Self::Changes
            | Self::Resets
            | Self::Deriv => samples,
            _ => 0,
        }
    }

    /// Every range function drops `__name__` except `last_over_time`,
    /// which "acts like offset" (`dropName` in `evalCall`).
    pub fn drops_metric_name(&self) -> bool {
        !matches!(self, Func::LastOverTime)
    }
}

/// A window fold: an ordinary one-arg range function, or `predict_linear`
/// with its scalar horizon. Lives under [`crate::buffer::Kernel::Range`] so
/// the top-level kernel split stays selector vs window.
///
/// The horizon is `f64` bits so the logical node stays `Eq`/`Hash` for NaN.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Hash)]
pub enum RangeOp {
    Func(Func),
    PredictLinear(u64),
}

impl RangeOp {
    pub fn predict_linear(seconds: f64) -> Self {
        Self::PredictLinear(seconds.to_bits())
    }

    pub fn drops_metric_name(&self) -> bool {
        match self {
            RangeOp::Func(func) => func.drops_metric_name(),
            RangeOp::PredictLinear(_) => true,
        }
    }
}

/// Flatten `Func(Rate)` to `Rate` so physical-plan pins stay `Range(Rate)`.
impl fmt::Debug for RangeOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RangeOp::Func(func) => write!(f, "{func:?}"),
            RangeOp::PredictLinear(bits) => write!(f, "PredictLinear({bits})"),
        }
    }
}

/// One step's window: the samples with `range_start < t <= range_end`,
/// StaleNaN already removed, plus the bounds the extrapolation needs.
pub struct Window<'a> {
    pub ts: &'a [i64],
    pub vs: &'a [f64],
    pub range_start: i64,
    pub range_end: i64,
    pub range_ms: i64,
}

/// Apply `func` to one window. `None` is "no sample at this step".
pub fn evaluate(func: Func, w: &Window) -> Option<f64> {
    if w.ts.is_empty() {
        return None;
    }
    match func {
        Func::Rate => extrapolated_rate(w, raw_increase(w.vs, true), true, true),
        Func::Increase => extrapolated_rate(w, raw_increase(w.vs, true), true, false),
        Func::Delta => extrapolated_rate(w, raw_increase(w.vs, false), false, false),
        Func::Irate => instant_value(w, true),
        Func::Idelta => instant_value(w, false),
        Func::SumOverTime => {
            let k = math::kahan_sum(w.vs);
            Some(if k.sum.is_infinite() {
                k.sum
            } else {
                k.value()
            })
        }
        Func::AvgOverTime => Some(math::mean_of(w.vs)),
        Func::MinOverTime => math::min_of(w.vs),
        Func::MaxOverTime => math::max_of(w.vs),
        Func::CountOverTime => Some(w.vs.len() as f64),
        Func::LastOverTime => Some(w.vs[w.vs.len() - 1]),
        Func::PresentOverTime => Some(1.0),
        Func::StddevOverTime => Some(math::variance_of(w.vs).sqrt()),
        Func::StdvarOverTime => Some(math::variance_of(w.vs)),
        Func::Changes => Some(
            w.vs.windows(2)
                .filter(|p| p[1] != p[0] && !(p[1].is_nan() && p[0].is_nan()))
                .count() as f64,
        ),
        Func::Resets => Some(w.vs.windows(2).filter(|p| p[1] < p[0]).count() as f64),
        // Prometheus's `funcDeriv`: slope only, intercept at the first
        // sample so the fit stays near the data (`#2674`).
        Func::Deriv => linear_regression(w.ts, w.vs, w.ts[0]).map(|(slope, _)| slope),
    }
}

/// Prometheus's `linearRegression` (`promql/functions.go` at 83962c35):
/// least squares with timestamps relative to `intercept_ms`, Kahan sums
/// folded in before the covariance. `deriv` uses the first sample's time;
/// `predict_linear` uses the evaluation timestamp.
///
/// The constant-series path is intentional. Computing the covariance for a
/// horizontal line can turn an exactly constant input into a rounded nearby
/// output, while upstream returns slope 0 and the first value verbatim. An
/// infinite constant is NaN, matching `linearRegression`'s `IsInf` guard.
pub(crate) fn linear_regression(ts: &[i64], vs: &[f64], intercept_ms: i64) -> Option<(f64, f64)> {
    if ts.len() < 2 {
        return None;
    }
    // Locals, not `KahanSum` methods: same shape as `math::kahan_sum`, so
    // the four compensations stay in registers for the whole window.
    let (mut sum_x, mut c_x) = (0.0, 0.0);
    let (mut sum_y, mut c_y) = (0.0, 0.0);
    let (mut sum_xy, mut c_xy) = (0.0, 0.0);
    let (mut sum_x2, mut c_x2) = (0.0, 0.0);
    let first = vs[0];
    let mut constant = true;
    for (&t, &y) in ts.iter().zip(vs) {
        constant &= y == first;
        let x = (t - intercept_ms) as f64 / 1e3;
        (sum_x, c_x) = math::kahan_inc(x, sum_x, c_x);
        (sum_y, c_y) = math::kahan_inc(y, sum_y, c_y);
        (sum_xy, c_xy) = math::kahan_inc(x * y, sum_xy, c_xy);
        (sum_x2, c_x2) = math::kahan_inc(x * x, sum_x2, c_x2);
    }
    if constant {
        return Some(if first.is_infinite() {
            (f64::NAN, f64::NAN)
        } else {
            (0.0, first)
        });
    }
    // Upstream folds each compensation into its sum before cov/var.
    sum_x += c_x;
    sum_y += c_y;
    sum_xy += c_xy;
    sum_x2 += c_x2;
    let n = ts.len() as f64;
    let cov_xy = sum_xy - sum_x * sum_y / n;
    let var_x = sum_x2 - sum_x * sum_x / n;
    let slope = cov_xy / var_x;
    let intercept = sum_y / n - slope * sum_x / n;
    Some((slope, intercept))
}

/// Prometheus's `funcPredictLinear`: project `linearRegression` from the
/// evaluation timestamp (`enh.Ts`) by `seconds`. The window that supplies
/// the samples may end at `@` or at `t − offset`; that end is not the
/// regression origin.
pub fn predict_linear(w: &Window, seconds: f64, intercept_ms: i64) -> Option<f64> {
    let (slope, intercept) = linear_regression(w.ts, w.vs, intercept_ms)?;
    Some(slope * seconds + intercept)
}

/// `result` in `extrapolatedRate`: the span of the window plus, for a
/// counter, the value lost at every reset.
///
/// The sweep reproduces this sum from the reset positions it tracks
/// rather than calling this, so the additions must stay in sample order
/// and keep starting from the span: a differently associated sum is a
/// different float, and the differential suite holds 1e-10 relative.
fn raw_increase(vs: &[f64], is_counter: bool) -> f64 {
    let mut result = vs[vs.len() - 1] - vs[0];
    if is_counter {
        for p in vs.windows(2) {
            if p[1] < p[0] {
                result += p[0];
            }
        }
    }
    result
}

/// `extrapolatedRate` for floats without start timestamps, from
/// `result` and the window's endpoints alone.
fn extrapolated_rate(w: &Window, result: f64, is_counter: bool, is_rate: bool) -> Option<f64> {
    let n = w.ts.len();
    let (first_t, last_t) = (w.ts[0], w.ts[n - 1]);

    let mut duration_to_start = (first_t - w.range_start) as f64 / 1000.0;
    let mut duration_to_end = (w.range_end - last_t) as f64 / 1000.0;
    let sampled_interval = (last_t - first_t) as f64 / 1000.0;
    let average_between_samples = if n > 1 {
        sampled_interval / (n - 1) as f64
    } else {
        0.0
    };
    let extrapolation_threshold = average_between_samples * 1.1;

    if n == 1 {
        // A single sample and no start timestamp to anchor it: nothing.
        return None;
    }
    // Samples close to the boundary extrapolate all the way to it;
    // further away, only half an average interval, our guess for where
    // the series really starts or ends.
    if duration_to_start >= extrapolation_threshold {
        duration_to_start = average_between_samples / 2.0;
    }
    if is_counter {
        // Never extrapolate a counter below zero.
        let mut duration_to_zero = duration_to_start;
        if result > 0.0 && w.vs[0] >= 0.0 {
            duration_to_zero = sampled_interval * (w.vs[0] / result);
        }
        if duration_to_zero < duration_to_start {
            duration_to_start = duration_to_zero;
        }
    }
    if duration_to_end >= extrapolation_threshold {
        duration_to_end = average_between_samples / 2.0;
    }

    let mut factor = if sampled_interval != 0.0 {
        (sampled_interval + duration_to_start + duration_to_end) / sampled_interval
    } else {
        1.0
    };
    if is_rate {
        factor /= w.range_ms as f64 / 1000.0;
    }
    Some(result * factor)
}

/// `instantValue`: the last two samples.
fn instant_value(w: &Window, is_rate: bool) -> Option<f64> {
    let n = w.ts.len();
    if n < 2 {
        return None;
    }
    let (t0, v0, t1, v1) = (w.ts[n - 2], w.vs[n - 2], w.ts[n - 1], w.vs[n - 1]);
    let sampled_interval = t1 - t0;
    if sampled_interval == 0 {
        return None;
    }
    // A counter reset leaves the result at the newer value.
    let reset = is_rate && v1 < v0;
    let mut result = if reset { v1 } else { v1 - v0 };
    if is_rate {
        result /= sampled_interval as f64 / 1000.0;
    }
    Some(result)
}

/// What a step costs once the window's edges have moved.
///
/// A 5m window on a 15s step overlaps its predecessor twenty to one, so a
/// kernel that refolds the window pays for the same samples twenty times.
/// These variants carry just enough across a step that the work is the
/// samples that entered and left it instead.
///
/// Only the functions where that trade measured out are here: removing
/// the sweep cost these 60-78%, while carrying a sliding pair count for
/// `changes`/`resets` was indistinguishable from refolding, and a no-op
/// variant for the rest cost the whole family 16-19% in `enter`, `leave`
/// and the dispatch in `value`. Everything else refolds through
/// [`evaluate`], with no `Sweep` at all.
pub(crate) enum Sweep {
    /// Where the counter reset, oldest first, so `raw_increase`'s sum
    /// can be replayed in sample order instead of carried as a running
    /// float that drifts as pairs leave.
    Counter {
        resets: VecDeque<usize>,
        /// `rate` divides by the window; `increase` does not.
        is_rate: bool,
    },
    /// Candidates for `min_over_time`/`max_over_time`, extreme at the
    /// front. NaNs never enter, since any number beats one; a window
    /// with only NaNs is recognised by the queue being empty.
    Extremum {
        candidates: VecDeque<usize>,
        max: bool,
    },
}

impl Sweep {
    /// Owned queue allocations; the enum itself is inline in its iterator.
    pub(crate) fn heap_size(&self) -> usize {
        let capacity = match self {
            Sweep::Counter { resets, .. } => resets.capacity(),
            Sweep::Extremum { candidates, .. } => candidates.capacity(),
        };
        capacity * std::mem::size_of::<usize>()
    }

    /// Between series, so that one `Sweep` can be hoisted above a row
    /// loop and keep the `VecDeque`s' capacity.
    pub(crate) fn reset(&mut self) {
        match self {
            Sweep::Counter { resets, .. } => resets.clear(),
            Sweep::Extremum { candidates, .. } => candidates.clear(),
        }
    }

    pub(crate) fn new(func: Func) -> Option<Sweep> {
        Some(match func {
            Func::Rate | Func::Increase => Sweep::Counter {
                resets: VecDeque::new(),
                is_rate: func == Func::Rate,
            },
            Func::MinOverTime => Sweep::Extremum {
                candidates: VecDeque::new(),
                max: false,
            },
            Func::MaxOverTime => Sweep::Extremum {
                candidates: VecDeque::new(),
                max: true,
            },
            _ => return None,
        })
    }

    /// Sample `i` joins the window's end; `lo` is its start, so a pair
    /// arrives with `i` only once its left half is inside. `vs` holds
    /// ordinals from `base` on: the tracked indices stay absolute so that
    /// trimming the buffer's front never has to rewrite them.
    fn enter(
        &mut self,
        i: usize,
        lo: usize,
        base: usize,
        vs: &[f64],
        budget: &mut WorkBudget,
    ) -> bool {
        let at = |j: usize| vs[j - base];
        match self {
            Sweep::Counter { resets, .. } => {
                if !budget.take() {
                    return false;
                }
                if i > lo && at(i) < at(i - 1) {
                    resets.push_back(i - 1);
                }
            }
            Sweep::Extremum { candidates, max } => {
                if at(i).is_nan() {
                    return budget.take();
                }
                // A strict comparison keeps the earliest of equal
                // values, which is the one `min_of`'s fold settles on
                // and the only way `-0.0` against `0.0` agrees.
                while let Some(&back) = candidates.back() {
                    let beaten = if *max {
                        at(back) < at(i)
                    } else {
                        at(back) > at(i)
                    };
                    if !beaten {
                        break;
                    }
                    if !budget.take() {
                        // This sample has not entered yet. Resuming at i is
                        // safe: already removed candidates stay beaten.
                        return false;
                    }
                    candidates.pop_back();
                }
                if !budget.take() {
                    return false;
                }
                candidates.push_back(i);
            }
        }
        true
    }

    fn leave(&mut self, i: usize) {
        match self {
            Sweep::Counter { resets, .. } => {
                if resets.front() == Some(&i) {
                    resets.pop_front();
                }
            }
            Sweep::Extremum { candidates, .. } => {
                if candidates.front() == Some(&i) {
                    candidates.pop_front();
                }
            }
        }
    }

    fn value_work(&self) -> usize {
        match self {
            Sweep::Counter { resets, .. } => resets.len(),
            Sweep::Extremum { .. } => 0,
        }
    }

    /// The step's value, `None` where `evaluate` gives `None`. `lo`
    /// rebases the tracked indices onto the window's slices.
    fn value(&self, w: &Window, lo: usize) -> Option<f64> {
        if w.ts.is_empty() {
            return None;
        }
        match self {
            Sweep::Counter { resets, is_rate } => {
                let mut result = w.vs[w.vs.len() - 1] - w.vs[0];
                for i in resets {
                    result += w.vs[i - lo];
                }
                extrapolated_rate(w, result, true, *is_rate)
            }
            Sweep::Extremum { candidates, .. } => Some(match candidates.front() {
                Some(i) => w.vs[i - lo],
                // Every sample is NaN, and `min_of` folds those to the
                // last one rather than the first.
                None => w.vs[w.vs.len() - 1],
            }),
        }
    }
}

/// Evaluate the steps of `it`'s open series that are ready.
///
/// A step is ready once its window's end is at or before `last_t`: the
/// samples still to come cannot land in it. Close sets `last_t` to MAX.
/// Edge and step cursors survive budget exhaustion. Computing one value
/// (`evaluate` or `Sweep::value`) remains indivisible, including its ordered
/// float fold; this is not wall-clock preemption.
/// Evaluating as the chunks arrive, rather than at close,
/// is what lets the buffer drop everything below `lo` between chunks.
///
/// `ts`/`vs` are the series' samples from ordinal `it.base` on and must
/// already be free of StaleNaN entries, and a `Sweep`, if any, must have
/// seen only this series. `emit` receives `(step_timestamp, value)` in
/// step order, each step at most once.
pub(crate) fn advance_range(
    it: &mut WindowBuffer,
    context: &EvalContext,
    ts: &[i64],
    vs: &[f64],
    func: Func,
    budget: &mut WorkBudget,
    mut emit: impl FnMut(i64, f64),
) {
    let WindowBuffer {
        sweep,
        base,
        lo,
        hi,
        next_step,
        last_t,
        pinned_value,
        ..
    } = it;
    debug_assert_eq!(ts.len(), vs.len());
    let p = context.params();
    let steps = context.steps();
    if p.window_ms <= 0 || *next_step >= steps {
        return;
    }
    // A window ending after this may still gain a sample.
    let Some(ready) = *last_t else {
        return;
    };
    let base = *base;
    // Indices relative to the slices.
    let window = |l: usize, h: usize, range_end: i64| Window {
        ts: &ts[l..h],
        vs: &vs[l..h],
        range_start: range_end - p.window_ms,
        range_end,
        range_ms: p.window_ms,
    };

    // `@` pins the window: it is evaluated once, when it is ready, and
    // repeated across the whole grid.
    if let Some(at) = p.at_ms {
        let range_end = at - p.offset_ms;
        if range_end > ready {
            return;
        }
        if budget.exhausted() {
            return;
        }
        let value = *pinned_value.get_or_insert_with(|| {
            let l = ts.partition_point(|t| *t <= range_end - p.window_ms);
            let h = ts.partition_point(|t| *t <= range_end);
            let value = evaluate(func, &window(l, h, range_end));
            budget.account_kernel(func.value_work(h - l));
            value
        });
        while *next_step < steps && budget.take() {
            if let Some(v) = value {
                emit(context.step_at(*next_step), v);
            }
            *next_step += 1;
        }
        return;
    }

    // Both window edges only move forward with the step, so a sweep sees
    // every sample enter once and leave once, across chunks too. The two
    // loops are one `match` above the step loop rather than a branch
    // inside it, so the refolding path pays neither the calls nor the
    // dispatch in them. As in `advance_selector`, the cursor lives in
    // locals relative to the slice and the step moves by addition, which
    // may wrap past the last step, unread; a sweep's indices stay absolute.
    let (mut l, mut h, mut next) = (*lo - base, *hi - base, *next_step);
    let mut step = (i128::from(p.start_ms) + next * i128::from(p.step_ms)) as i64;
    match sweep {
        None => {
            'steps: while next < steps {
                let range_end = step - p.offset_ms;
                if range_end > ready {
                    break;
                }
                let range_start = range_end - p.window_ms;
                while l < ts.len() && ts[l] <= range_start {
                    if !budget.take() {
                        break 'steps;
                    }
                    l += 1;
                }
                h = h.max(l);
                while h < ts.len() && ts[h] <= range_end {
                    if !budget.take() {
                        break 'steps;
                    }
                    h += 1;
                }
                if !budget.take() {
                    break;
                }
                if let Some(v) = evaluate(func, &window(l, h, range_end)) {
                    emit(step, v);
                }
                budget.account_kernel(func.value_work(h - l));
                next += 1;
                step = step.wrapping_add(p.step_ms);
            }
        }
        Some(sweep) => {
            'steps: while next < steps {
                let range_end = step - p.offset_ms;
                if range_end > ready {
                    break;
                }
                let range_start = range_end - p.window_ms;
                while l < ts.len() && ts[l] <= range_start {
                    if !budget.take() {
                        break 'steps;
                    }
                    if l < h {
                        sweep.leave(base + l);
                    }
                    l += 1;
                }
                // A gap wider than the window leaves `h` behind `l`; the
                // samples it skips never entered, and the state is empty.
                h = h.max(l);
                while h < ts.len() && ts[h] <= range_end {
                    if !sweep.enter(base + h, base + l, base, vs, budget) {
                        break 'steps;
                    }
                    h += 1;
                }
                if !budget.take() {
                    break;
                }
                if let Some(v) = sweep.value(&window(l, h, range_end), base + l) {
                    emit(step, v);
                }
                budget.account_kernel(sweep.value_work());
                next += 1;
                step = step.wrapping_add(p.step_ms);
            }
        }
    }
    // A suspended left-edge walk can pass the previous right edge before
    // reaching h.max(l) above. No skipped sample entered the sweep.
    (*lo, *hi, *next_step) = (base + l, base + h.max(l), next);
}

/// `predict_linear` walks the same sample window as an ordinary range
/// function, but folds with the step timestamp as the regression
/// intercept (`enh.Ts`). Under `@` the samples stay pinned while each
/// step still gets its own intercept, so the value is not replayed.
pub(crate) fn advance_predict_linear(
    it: &mut WindowBuffer,
    context: &EvalContext,
    ts: &[i64],
    vs: &[f64],
    seconds: f64,
    budget: &mut WorkBudget,
    mut emit: impl FnMut(i64, f64),
) {
    let WindowBuffer {
        base,
        lo,
        hi,
        next_step,
        last_t,
        ..
    } = it;
    let p = context.params();
    let steps = context.steps();
    if p.window_ms <= 0 || *next_step >= steps {
        return;
    }
    let Some(ready) = *last_t else {
        return;
    };
    let window = |l: usize, h: usize, range_end: i64| Window {
        ts: &ts[l..h],
        vs: &vs[l..h],
        range_start: range_end - p.window_ms,
        range_end,
        range_ms: p.window_ms,
    };
    if let Some(at) = p.at_ms {
        let range_end = at - p.offset_ms;
        if range_end > ready {
            return;
        }
        let l = ts.partition_point(|t| *t <= range_end - p.window_ms);
        let h = ts.partition_point(|t| *t <= range_end);
        let w = window(l, h, range_end);
        while *next_step < steps {
            if budget.exhausted() {
                return;
            }
            let step = context.step_at(*next_step);
            if !budget.take() {
                return;
            }
            if let Some(value) = predict_linear(&w, seconds, step) {
                emit(step, value);
            }
            budget.account_kernel(h - l);
            *next_step += 1;
        }
        return;
    }
    let base = *base;
    let (mut l, mut h, mut next) = (*lo - base, *hi - base, *next_step);
    let mut step = (i128::from(p.start_ms) + next * i128::from(p.step_ms)) as i64;
    while next < steps {
        let range_end = step - p.offset_ms;
        if range_end > ready {
            break;
        }
        let range_start = range_end - p.window_ms;
        while l < ts.len() && ts[l] <= range_start {
            if !budget.take() {
                break;
            }
            l += 1;
        }
        if budget.exhausted() {
            break;
        }
        h = h.max(l);
        while h < ts.len() && ts[h] <= range_end {
            if !budget.take() {
                break;
            }
            h += 1;
        }
        if !budget.take() {
            break;
        }
        // Intercept is the evaluation step, not `range_end`: with an
        // offset those differ, and upstream still passes `enh.Ts`.
        if let Some(value) = predict_linear(&window(l, h, range_end), seconds, step) {
            emit(step, value);
        }
        budget.account_kernel(h - l);
        next += 1;
        step = step.wrapping_add(p.step_ms);
    }
    (*lo, *hi, *next_step) = (base + l, base + h.max(l), next);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::{BufferedSeriesIterator, Kernel};
    use crate::params::Params;
    use crate::selector::{is_stale, STALE_NAN_BITS};
    use crate::series::{self, SamplesBuilder};
    use datafusion::arrow::array::AsArray;

    const S: i64 = 1000;
    const M: i64 = 60 * S;

    /// One series in one chunk, pushed and closed.
    fn run(func: Func, ts: &[i64], vs: &[f64], p: Params) -> Vec<(i64, f64)> {
        let context = EvalContext::new(Kernel::Range(RangeOp::Func(func)), p);
        let mut it = BufferedSeriesIterator::new(&context);
        let mut out = SamplesBuilder::default();
        it.push(&context, ts, vs, |t, v| out.push(t, v));
        it.close(&context, |t, v| out.push(t, v));
        out.finish_row();
        let row = out.take_all().value(0);
        let (ts, vs) = series::sample_slices(row.as_struct());
        ts.iter().copied().zip(vs.iter().copied()).collect()
    }

    /// The same streamed evaluator used by planning, rather than a direct
    /// call to `predict_linear`, so this holds the scalar horizon and window
    /// boundary behaviour together.
    fn run_predict_linear(seconds: f64, ts: &[i64], vs: &[f64], p: Params) -> Vec<(i64, f64)> {
        let context = EvalContext::new(Kernel::Range(RangeOp::predict_linear(seconds)), p);
        let mut it = BufferedSeriesIterator::new(&context);
        let mut out = SamplesBuilder::default();
        it.push(&context, ts, vs, |t, v| out.push(t, v));
        it.close(&context, |t, v| out.push(t, v));
        out.finish_row();
        let row = out.take_all().value(0);
        let (ts, vs) = series::sample_slices(row.as_struct());
        ts.iter().copied().zip(vs.iter().copied()).collect()
    }

    /// One step at 5m over a 5m window.
    fn at_5m() -> Params {
        Params {
            start_ms: 5 * M,
            end_ms: 5 * M,
            step_ms: 30 * S,
            window_ms: 5 * M,
            offset_ms: 0,
            at_ms: None,
        }
    }

    /// A counter incrementing by one every 30s: samples 0..=10 at 0..=300s.
    fn counter() -> (Vec<i64>, Vec<f64>) {
        (0..=10).map(|i| (i * 30 * S, i as f64 + 1.0)).unzip()
    }

    #[test]
    fn rate_of_a_clean_counter_is_its_slope() {
        // Window (0, 300s]: ten samples 2..=11 at 30s..=300s. The first
        // is 30s from the range start, within 1.1× the 30s spacing, so
        // extrapolation reaches the boundary: increase 9 × 300/270 = 10.
        let (ts, vs) = counter();
        let out = run(Func::Rate, &ts, &vs, at_5m());
        assert_eq!(out.len(), 1);
        assert!((out[0].1 - 1.0 / 30.0).abs() < 1e-12, "{out:?}");
        let out = run(Func::Increase, &ts, &vs, at_5m());
        assert!((out[0].1 - 10.0).abs() < 1e-12, "{out:?}");
        let out = run(Func::Delta, &ts, &vs, at_5m());
        assert!((out[0].1 - 10.0).abs() < 1e-12, "{out:?}");
    }

    #[test]
    fn deriv_is_the_slope_anchored_at_the_first_sample() {
        // `functions.test`: same series as predict_linear's fit, slope
        // 0.010606... with intercept at the first in-window sample.
        let ts: Vec<_> = (0..=10).map(|i| i * 5 * M).collect();
        let vs = [0., 10., 20., 30., 40., 0., 10., 20., 30., 40., 50.];
        let out = run(
            Func::Deriv,
            &ts,
            &vs,
            Params {
                start_ms: 50 * M,
                end_ms: 50 * M,
                window_ms: 100 * M,
                ..at_5m()
            },
        );
        assert_eq!(out.len(), 1, "{out:?}");
        assert!((out[0].1 - 0.010606060606060607).abs() < 1e-12, "{out:?}");
    }

    #[test]
    fn predict_linear_projects_from_the_eval_timestamp() {
        // `functions.test`: a reset in the middle makes the fitted line's
        // value at 3000s 38.636..., and one hour beyond it exactly 70.
        let ts: Vec<_> = (0..=10).map(|i| i * 5 * M).collect();
        let vs = [0., 10., 20., 30., 40., 0., 10., 20., 30., 40., 50.];
        let w = Window {
            ts: &ts,
            vs: &vs,
            range_start: 0,
            range_end: 50 * M,
            range_ms: 50 * M,
        };
        // PromQL's window is open at its left edge, so the sample at 0 is
        // excluded: this is the `functions.test` result at 50m.
        let w = Window {
            ts: &ts[1..],
            vs: &vs[1..],
            ..w
        };
        assert_eq!(predict_linear(&w, 3600.0, 50 * M), Some(70.0));
    }

    #[test]
    fn predict_linear_keeps_constant_values_exact() {
        let w = Window {
            ts: &[10_000, 20_000, 30_000],
            vs: &[1.5990505637277868; 3],
            range_start: 0,
            range_end: 30_000,
            range_ms: 30_000,
        };
        assert_eq!(
            predict_linear(&w, 3600.0, 30_000).unwrap().to_bits(),
            w.vs[0].to_bits()
        );
    }

    #[test]
    fn predict_linear_turns_a_constant_infinity_into_nan() {
        let w = Window {
            ts: &[10_000, 20_000, 30_000],
            vs: &[f64::INFINITY; 3],
            range_start: 0,
            range_end: 30_000,
            range_ms: 30_000,
        };
        assert!(predict_linear(&w, 3600.0, 30_000).unwrap().is_nan());
    }

    #[test]
    fn predict_linear_streams_the_same_window_as_other_range_kernels() {
        let ts: Vec<_> = (0..=10).map(|i| i * 5 * M).collect();
        let vs = [0., 10., 20., 30., 40., 0., 10., 20., 30., 40., 50.];
        let out = run_predict_linear(
            3600.0,
            &ts,
            &vs,
            Params {
                start_ms: 50 * M,
                end_ms: 50 * M,
                window_ms: 50 * M,
                ..at_5m()
            },
        );
        assert_eq!(out, vec![(50 * M, 70.0)]);
    }

    #[test]
    fn predict_linear_under_at_reanchors_each_eval_timestamp() {
        // `functions.test`: samples through 50m, window ends at `@ 3000`
        // (50m), eval at 10m. Samples stay on the pinned window; the fit
        // uses the 10m evaluation timestamp.
        let ts: Vec<_> = (0..=10).map(|i| i * 5 * M).collect();
        let vs = [0., 10., 20., 30., 40., 0., 10., 20., 30., 40., 50.];
        let out = run_predict_linear(
            3600.0,
            &ts,
            &vs,
            Params {
                start_ms: 10 * M,
                end_ms: 10 * M,
                window_ms: 55 * M,
                at_ms: Some(50 * M),
                ..at_5m()
            },
        );
        assert_eq!(out.len(), 1, "{out:?}");
        assert!((out[0].1 - 51.36363636363637).abs() < 1e-9, "{out:?}");
    }

    #[test]
    fn a_counter_reset_adds_the_value_before_it() {
        // 2, 3, 1, 2 at 30s..120s in a (0, 120s] window: raw 0, plus the
        // 3 lost at the reset. Extrapolated to the boundary: 3 × 120/90.
        let ts = [30 * S, 60 * S, 90 * S, 120 * S];
        let vs = [2.0, 3.0, 1.0, 2.0];
        let p = Params {
            start_ms: 120 * S,
            end_ms: 120 * S,
            window_ms: 120 * S,
            ..at_5m()
        };
        let out = run(Func::Increase, &ts, &vs, p);
        assert!((out[0].1 - 4.0).abs() < 1e-12, "{out:?}");
        assert_eq!(run(Func::Resets, &ts, &vs, p), vec![(120 * S, 1.0)]);
        // A gauge does not see resets.
        assert!((run(Func::Delta, &ts, &vs, p)[0].1 - 0.0).abs() < 1e-12);
    }

    #[test]
    fn far_from_the_boundary_extrapolation_is_half_an_interval() {
        // Samples at 0, 30s, 60s in a (0, 300s] window; the one at 0 is
        // excluded, leaving 30s and 60s. The start is one interval away
        // (30s < 1.1 × 30s) so extrapolation reaches it; the end is 240s
        // away, so only half an interval, 15s, is added there. Increase =
        // 1 × (30 + 30 + 15)/30 = 2.5. The counter's zero clip does not
        // bite: 30s × (2/1) = 60s > 30s.
        let ts = [0, 30 * S, 60 * S];
        let vs = [1.0, 2.0, 3.0];
        let out = run(Func::Increase, &ts, &vs, at_5m());
        assert!((out[0].1 - 2.5).abs() < 1e-12, "{out:?}");
    }

    #[test]
    fn a_single_sample_yields_nothing_for_rates_but_counts() {
        let ts = [60 * S];
        let vs = [5.0];
        assert!(run(Func::Rate, &ts, &vs, at_5m()).is_empty());
        assert!(run(Func::Irate, &ts, &vs, at_5m()).is_empty());
        assert_eq!(
            run(Func::CountOverTime, &ts, &vs, at_5m()),
            vec![(5 * M, 1.0)]
        );
        assert_eq!(
            run(Func::PresentOverTime, &ts, &vs, at_5m()),
            vec![(5 * M, 1.0)]
        );
    }

    #[test]
    fn the_window_is_open_at_the_start_and_closed_at_the_end() {
        // Samples at exactly range_start and range_end: only the latter.
        let ts = [0, 5 * M];
        let vs = [1.0, 2.0];
        assert_eq!(
            run(Func::CountOverTime, &ts, &vs, at_5m()),
            vec![(5 * M, 1.0)]
        );
        // A step with an empty window emits nothing.
        let p = Params {
            start_ms: 20 * M,
            end_ms: 20 * M,
            ..at_5m()
        };
        assert!(run(Func::CountOverTime, &ts, &vs, p).is_empty());
    }

    #[test]
    fn the_over_time_family() {
        let (ts, vs) = counter();
        let one = |f: Func| run(f, &ts, &vs, at_5m())[0].1;
        assert_eq!(one(Func::SumOverTime), 65.0);
        assert_eq!(one(Func::AvgOverTime), 6.5);
        assert_eq!(one(Func::MinOverTime), 2.0);
        assert_eq!(one(Func::MaxOverTime), 11.0);
        assert_eq!(one(Func::CountOverTime), 10.0);
        assert_eq!(one(Func::LastOverTime), 11.0);
        assert_eq!(one(Func::Changes), 9.0);
        assert_eq!(one(Func::Resets), 0.0);
        assert!((one(Func::Irate) - 1.0 / 30.0).abs() < 1e-12);
        assert_eq!(one(Func::Idelta), 1.0);
        // 2..=11 in steps of one: the variance of ten consecutive integers.
        assert_eq!(one(Func::StdvarOverTime), 8.25);
        assert_eq!(one(Func::StddevOverTime), 8.25f64.sqrt());
    }

    /// The window's edges decide what the variance sees: a single sample is
    /// exactly 0, not "no sample", and a step with an empty window answers
    /// nothing rather than NaN.
    #[test]
    fn stddev_and_stdvar_over_time_at_the_window_edges() {
        let ts = [5 * M];
        let vs = [7.0];
        for f in [Func::StddevOverTime, Func::StdvarOverTime] {
            assert_eq!(run(f, &ts, &vs, at_5m()), vec![(5 * M, 0.0)]);
            let p = Params {
                start_ms: 20 * M,
                end_ms: 20 * M,
                ..at_5m()
            };
            assert!(run(f, &ts, &vs, p).is_empty());
        }
    }

    #[test]
    fn irate_on_a_reset_keeps_the_new_value() {
        let ts = [0, 30 * S];
        let vs = [10.0, 2.0];
        let p = Params {
            start_ms: 30 * S,
            end_ms: 30 * S,
            window_ms: M,
            ..at_5m()
        };
        assert!((run(Func::Irate, &ts, &vs, p)[0].1 - 2.0 / 30.0).abs() < 1e-12);
        assert_eq!(run(Func::Idelta, &ts, &vs, p)[0].1, -8.0);
    }

    #[test]
    fn nan_rules_in_min_max_and_changes() {
        let ts = [30 * S, 60 * S, 90 * S];
        let vs = [f64::NAN, 3.0, f64::NAN];
        let p = Params {
            start_ms: 90 * S,
            end_ms: 90 * S,
            window_ms: 2 * M,
            ..at_5m()
        };
        assert_eq!(run(Func::MinOverTime, &ts, &vs, p)[0].1, 3.0);
        assert_eq!(run(Func::MaxOverTime, &ts, &vs, p)[0].1, 3.0);
        assert_eq!(run(Func::Changes, &ts, &vs, p)[0].1, 2.0);
        let vs = [f64::NAN, f64::NAN, 1.0];
        assert_eq!(run(Func::Changes, &ts, &vs, p)[0].1, 1.0);
    }

    #[test]
    fn stale_markers_are_not_samples() {
        let (ts, mut vs) = counter();
        vs[5] = f64::from_bits(STALE_NAN_BITS);
        assert_eq!(run(Func::CountOverTime, &ts, &vs, at_5m())[0].1, 9.0);
        assert_eq!(run(Func::SumOverTime, &ts, &vs, at_5m())[0].1, 65.0 - 6.0);
    }

    #[test]
    fn offset_and_at_move_the_window_not_the_emission() {
        let (ts, vs) = counter();
        let p = Params {
            start_ms: 6 * M,
            end_ms: 6 * M,
            offset_ms: M,
            ..at_5m()
        };
        let out = run(Func::CountOverTime, &ts, &vs, p);
        assert_eq!(out, vec![(6 * M, 10.0)]);

        let p = Params {
            start_ms: 0,
            end_ms: M,
            step_ms: 30 * S,
            at_ms: Some(5 * M),
            ..at_5m()
        };
        let out = run(Func::CountOverTime, &ts, &vs, p);
        assert_eq!(out, vec![(0, 10.0), (30 * S, 10.0), (M, 10.0)]);
    }

    /// Every function, because both branches of `advance_range` are
    /// sliding walks that carry `lo`/`hi` across steps: the swept one
    /// carries kernel state too, the other only the bounds. `fresh`
    /// recomputes both bounds per step with `partition_point`, so it
    /// pins each walk against an independent oracle rather than
    /// against itself.
    const ALL: [Func; 17] = [
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

    /// The executor's oracles resolve a query through `parse` too, so a name
    /// bound to the wrong kernel answers wrongly on both sides and agrees.
    #[test]
    fn every_name_parses_back_to_its_function() {
        for func in ALL {
            assert_eq!(Func::parse(func.as_str()), Some(func));
        }
    }

    /// Counter resets, NaN runs, a staleness marker, signed zeros and a
    /// gap wider than the window: one series that reaches every branch
    /// of every kernel as a 2m window slides over it.
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
            // Nothing for nine minutes, then two samples that are only
            // ever seen together with nothing else: an all-NaN window.
            (900 * S, f64::NAN),
            (930 * S, f64::NAN),
            (960 * S, 3.0),
            (990 * S, 3.0),
            (1020 * S, 2.0),
        ]
        .into_iter()
        .unzip()
    }

    /// The oracle: one window built from scratch per step, handed to
    /// `evaluate`, with the sweep's staleness filter in front of it.
    fn fresh(func: Func, ts: &[i64], vs: &[f64], p: Params) -> Vec<(i64, f64)> {
        let (ts, vs): (Vec<i64>, Vec<f64>) = ts
            .iter()
            .zip(vs)
            .filter(|(_, v)| !is_stale(**v))
            .map(|(t, v)| (*t, *v))
            .unzip();
        let mut out = Vec::new();
        for step in p.steps() {
            let range_end = step - p.offset_ms;
            let range_start = range_end - p.window_ms;
            let lo = ts.partition_point(|t| *t <= range_start);
            let hi = ts.partition_point(|t| *t <= range_end);
            let w = Window {
                ts: &ts[lo..hi],
                vs: &vs[lo..hi],
                range_start,
                range_end,
                range_ms: p.window_ms,
            };
            if let Some(v) = evaluate(func, &w) {
                out.push((step, v));
            }
        }
        out
    }

    #[test]
    fn the_sliding_walks_match_a_fresh_window_per_step() {
        let (ts, vs) = a_rough_series();
        let p = Params {
            start_ms: 0,
            end_ms: 20 * M,
            step_ms: 15 * S,
            window_ms: 2 * M,
            offset_ms: 45 * S,
            at_ms: None,
        };
        for func in ALL {
            let name = func.as_str();
            let swept = run(func, &ts, &vs, p);
            let expected = fresh(func, &ts, &vs, p);
            assert_eq!(swept.len(), expected.len(), "{name}");
            for (a, b) in swept.iter().zip(&expected) {
                assert_eq!(a.0, b.0, "{name}");
                assert_eq!(
                    a.1.to_bits(),
                    b.1.to_bits(),
                    "{name} at {}: {} vs {}",
                    a.0,
                    a.1,
                    b.1
                );
            }
        }
    }

    #[test]
    fn fuzz_the_sliding_walks_against_fresh() {
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
                at_ms: None,
            };
            for func in ALL {
                let name = func.as_str();
                let swept = run(func, &ts, &vs, p);
                let expected = fresh(func, &ts, &vs, p);
                assert_eq!(swept.len(), expected.len(), "case {case} {name}");
                for (a, b) in swept.iter().zip(&expected) {
                    assert_eq!(a.0, b.0, "case {case} {name}");
                    assert_eq!(
                        a.1.to_bits(),
                        b.1.to_bits(),
                        "case {case} {name} at {}: {} vs {} | ts={ts:?} vs={vs:?} p={p:?}",
                        a.0,
                        a.1,
                        b.1
                    );
                }
            }
        }
    }

    /// Feeds `ts`/`vs` through the cursor `k` samples at a time, as if
    /// `base` samples had already been trimmed off the front, and trims
    /// below `lo` after every chunk the way `BufferedSeriesIterator::push`
    /// does, so that every ordinal the sweep holds is shifted off its
    /// buffer index.
    fn cursor(
        func: Func,
        ts: &[i64],
        vs: &[f64],
        p: Params,
        base: usize,
        k: usize,
    ) -> Vec<(i64, f64)> {
        let context = EvalContext::new(Kernel::Range(RangeOp::Func(func)), p);
        let mut it = WindowBuffer::new(&context);
        (it.base, it.lo, it.hi) = (base, base, base);
        let mut out = Vec::new();
        for (cts, cvs) in ts.chunks(k).zip(vs.chunks(k)) {
            for (t, v) in cts.iter().zip(cvs) {
                if !is_stale(*v) {
                    it.ts.push(*t);
                    it.vs.push(*v);
                }
            }
            it.last_t = cts.last().copied();
            let (bts, bvs) = (it.ts.clone(), it.vs.clone());
            advance_range(
                &mut it,
                &context,
                &bts,
                &bvs,
                func,
                &mut WorkBudget::new(usize::MAX),
                |t, v| out.push((t, v)),
            );
            let dead = it.lo - it.base;
            it.ts.drain(..dead);
            it.vs.drain(..dead);
            it.base += dead;
        }
        let (bts, bvs) = (it.ts.clone(), it.vs.clone());
        it.last_t = Some(i64::MAX);
        advance_range(
            &mut it,
            &context,
            &bts,
            &bvs,
            func,
            &mut WorkBudget::new(usize::MAX),
            |t, v| out.push((t, v)),
        );
        out
    }

    #[test]
    fn advance_range_matches_one_chunk_across_a_base_shift() {
        let (ts, vs) = a_rough_series();
        let plain = Params {
            start_ms: 0,
            end_ms: 20 * M,
            step_ms: 15 * S,
            window_ms: 2 * M,
            offset_ms: 45 * S,
            at_ms: None,
        };
        let params = [
            plain,
            Params {
                offset_ms: 0,
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
        ];
        for p in params {
            for func in ALL {
                let expected = run(func, &ts, &vs, p);
                for base in [1, 7, 1000] {
                    for k in [1, 2, 3, 5, ts.len()] {
                        let got = cursor(func, &ts, &vs, p, base, k);
                        let name = func.as_str();
                        assert_eq!(
                            got.len(),
                            expected.len(),
                            "{name} base={base} k={k} p={p:?}"
                        );
                        for (a, b) in got.iter().zip(&expected) {
                            assert_eq!(a.0, b.0, "{name} base={base} k={k} p={p:?}");
                            assert_eq!(
                                a.1.to_bits(),
                                b.1.to_bits(),
                                "{name} base={base} k={k} p={p:?} at {}: {} vs {}",
                                a.0,
                                a.1,
                                b.1
                            );
                        }
                    }
                }
            }
        }
    }
}
