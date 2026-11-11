//! Range functions over a window that holds native histograms.
//!
//! [`crate::range::evaluate`] is the float path and stays untouched: a
//! window reaches this module only once its series has shown a histogram,
//! and one that holds none is handed straight back to it. Each arm ports
//! what the same function does upstream once `samples.Histograms` is not
//! empty (`promql/functions.go` at 83962c35),
//! which is one of three things: compute on histograms, compute on the
//! floats and ignore the histograms, or answer nothing for a window that
//! mixes the two.
//!
//! Upstream also attaches an annotation in most of the cases that answer
//! nothing or ignore a sample. This engine has no annotation channel yet,
//! so those are silent here; the values are the same.

use std::borrow::Cow;
use std::sync::Arc;

use promql_histogram::{CounterResetHint, FloatHistogram, KahanSum};

use crate::histogram::{Mean, Value};
use crate::range::{self, Func, Window};

/// The histogram at each window position, `None` where the sample is a
/// float. Parallel to the window's `ts`/`vs`.
pub(crate) type Histograms = [Option<Arc<FloatHistogram>>];

/// Apply `func` to a window of a series that has shown a histogram.
pub(crate) fn evaluate(func: Func, w: &Window, hs: &Histograms) -> Option<Value> {
    debug_assert_eq!(w.ts.len(), hs.len());
    if hs.iter().all(Option::is_none) {
        return range::evaluate(func, w).map(Value::Float);
    }
    let histograms = || hs.iter().flatten().map(Arc::as_ref);
    let all_histograms = hs.iter().all(Option::is_some);
    match func {
        Func::Rate => extrapolated_rate(w, hs, true, true),
        Func::Increase => extrapolated_rate(w, hs, true, false),
        Func::Delta => extrapolated_rate(w, hs, false, false),
        Func::Irate => instant_value(w, hs, true),
        Func::Idelta => instant_value(w, hs, false),
        // `funcSumOverTime`: floats and histograms in one range answer
        // nothing, and so does a sum of incompatible histograms.
        Func::SumOverTime => {
            if !all_histograms {
                return None;
            }
            let mut all = histograms();
            let mut sum = KahanSum::new(all.next()?.clone());
            for h in all {
                sum.add(h).ok()?;
            }
            sum.finish().ok().map(histogram)
        }
        Func::AvgOverTime => {
            if !all_histograms {
                return None;
            }
            let mut all = histograms();
            let mut mean = Mean::new(all.next()?.clone());
            for h in all {
                mean.add(h).ok()?;
            }
            mean.finish().ok().map(histogram)
        }
        Func::CountOverTime => Some(Value::Float(hs.len() as f64)),
        Func::PresentOverTime => Some(Value::Float(1.0)),
        // `funcLastOverTime`: the newest sample, of either type.
        Func::LastOverTime => Some(match hs.last()? {
            Some(h) => Value::Histogram(Arc::clone(h)),
            None => Value::Float(w.vs[w.vs.len() - 1]),
        }),
        Func::Changes => Some(Value::Float(pairs(w, hs, |prev, cur| match (prev, cur) {
            (Value::Float(p), Value::Float(c)) => c != p && !(c.is_nan() && p.is_nan()),
            (Value::Histogram(p), Value::Histogram(c)) => !c.equals(p),
            _ => true,
        }))),
        Func::Resets => Some(Value::Float(pairs(w, hs, |prev, cur| match (prev, cur) {
            (Value::Float(p), Value::Float(c)) => c < p,
            (Value::Histogram(p), Value::Histogram(c)) => c.detect_reset(p),
            _ => true,
        }))),
        // `compareOverTime`, `varianceOverTime`, `funcDeriv`: the floats
        // alone, and nothing where there are none.
        Func::MinOverTime
        | Func::MaxOverTime
        | Func::StddevOverTime
        | Func::StdvarOverTime
        | Func::Deriv => {
            let (ts, vs) = floats_of(w, hs);
            range::evaluate(
                func,
                &Window {
                    ts: &ts,
                    vs: &vs,
                    ..*w
                },
            )
            .map(Value::Float)
        }
    }
}

/// The float samples of a window, for the functions that ignore
/// histograms. Borrowed when there is nothing to leave out.
pub(crate) fn floats_of<'a>(w: &Window<'a>, hs: &Histograms) -> (Cow<'a, [i64]>, Cow<'a, [f64]>) {
    if hs.iter().all(Option::is_none) {
        return (Cow::Borrowed(w.ts), Cow::Borrowed(w.vs));
    }
    let keep = || hs.iter().enumerate().filter(|(_, h)| h.is_none());
    (
        keep().map(|(i, _)| w.ts[i]).collect(),
        keep().map(|(i, _)| w.vs[i]).collect(),
    )
}

fn histogram(h: FloatHistogram) -> Value {
    Value::Histogram(Arc::new(h))
}

/// How many adjacent sample pairs `differs` holds for, the walk
/// `funcChanges` and `funcResets` share.
fn pairs(w: &Window, hs: &Histograms, differs: impl Fn(&Value, &Value) -> bool) -> f64 {
    let at = |i: usize| match &hs[i] {
        Some(h) => Value::Histogram(Arc::clone(h)),
        None => Value::Float(w.vs[i]),
    };
    (1..hs.len())
        .filter(|&i| differs(&at(i - 1), &at(i)))
        .count() as f64
}

/// `extrapolatedRate` once the range holds a histogram: at least two
/// histograms and no float, or nothing.
fn extrapolated_rate(
    w: &Window,
    hs: &Histograms,
    is_counter: bool,
    is_rate: bool,
) -> Option<Value> {
    let points: Vec<&FloatHistogram> = hs.iter().map(|h| h.as_deref()).collect::<Option<_>>()?;
    let n = points.len();
    if n < 2 {
        return None;
    }
    let mut result = histogram_rate(&points, is_counter)?;
    let (first_t, last_t) = (w.ts[0], w.ts[n - 1]);

    let mut duration_to_start = (first_t - w.range_start) as f64 / 1000.0;
    let mut duration_to_end = (w.range_end - last_t) as f64 / 1000.0;
    let sampled_interval = (last_t - first_t) as f64 / 1000.0;
    let average_between_samples = sampled_interval / (n - 1) as f64;
    let extrapolation_threshold = average_between_samples * 1.1;
    if duration_to_start >= extrapolation_threshold {
        duration_to_start = average_between_samples / 2.0;
    }
    if is_counter {
        // The histogram's count stands in for the float counter's value
        // when finding where the series would have been zero.
        let mut duration_to_zero = duration_to_start;
        if result.count > 0.0 && points[0].count >= 0.0 {
            duration_to_zero = sampled_interval * (points[0].count / result.count);
        }
        if duration_to_zero < duration_to_start {
            duration_to_start = duration_to_zero;
        }
    }
    if duration_to_end >= extrapolation_threshold {
        duration_to_end = average_between_samples / 2.0;
    }
    let mut factor = (sampled_interval + duration_to_start + duration_to_end) / sampled_interval;
    if is_rate {
        factor /= w.range_ms as f64 / 1000.0;
    }
    result.mul(factor);
    Some(histogram(result))
}

/// `histogramRate`: the last histogram minus the first, at the coarsest
/// schema among them, with every counter reset's lost count added back.
/// `None` where the histograms cannot be combined.
fn histogram_rate(points: &[&FloatHistogram], is_counter: bool) -> Option<FloatHistogram> {
    let last = points[points.len() - 1];
    let mut first = Cow::Borrowed(points[0]);
    let mut custom = first.uses_custom_buckets();
    // A reset between the first two samples makes the first irrelevant,
    // its bucket layout included.
    if is_counter && points[1].detect_reset(&first) {
        first = Cow::Owned(FloatHistogram {
            schema: points[1].schema,
            custom_values: points[1].custom_values.clone(),
            ..FloatHistogram::default()
        });
        custom = points[1].uses_custom_buckets();
    }
    if last.uses_custom_buckets() != custom {
        return None;
    }
    let mut min_schema = last.schema.min(first.schema);
    if is_counter {
        for h in &points[1..points.len() - 1] {
            min_schema = min_schema.min(h.schema);
            if h.uses_custom_buckets() != custom {
                return None;
            }
        }
    }
    let mut result = if min_schema == last.schema {
        last.clone()
    } else {
        last.copy_to_schema(min_schema)
    };
    result.sub(&first).ok()?;
    if is_counter {
        let mut prev: &FloatHistogram = &first;
        for h in &points[1..] {
            if h.detect_reset(prev) {
                result.add(prev).ok()?;
            }
            prev = h;
        }
    }
    result.counter_reset_hint = CounterResetHint::Gauge;
    result.compact();
    Some(result)
}

/// `instantValue`: the last two samples, which must be of one type.
fn instant_value(w: &Window, hs: &Histograms, is_rate: bool) -> Option<Value> {
    let n = hs.len();
    if n < 2 {
        return None;
    }
    let sampled_interval = w.ts[n - 1] - w.ts[n - 2];
    if sampled_interval == 0 {
        return None;
    }
    match (&hs[n - 2], &hs[n - 1]) {
        (None, None) => {
            let func = if is_rate { Func::Irate } else { Func::Idelta };
            let last_two = Window {
                ts: &w.ts[n - 2..],
                vs: &w.vs[n - 2..],
                ..*w
            };
            range::evaluate(func, &last_two).map(Value::Float)
        }
        (Some(prev), Some(last)) => {
            let mut result = FloatHistogram::clone(last);
            // A counter reset leaves the result at the newer histogram.
            if !is_rate || !last.detect_reset(prev) {
                result.sub(prev).ok()?;
            }
            result.counter_reset_hint = CounterResetHint::Gauge;
            result.compact();
            if is_rate {
                result.div(sampled_interval as f64 / 1000.0);
            }
            Some(histogram(result))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use promql_histogram::Span;

    use super::*;

    fn h(count: f64, hint: CounterResetHint) -> Arc<FloatHistogram> {
        Arc::new(FloatHistogram {
            counter_reset_hint: hint,
            count,
            sum: count * 2.0,
            positive_spans: vec![Span {
                offset: 0,
                length: 1,
            }],
            positive_buckets: vec![count],
            ..FloatHistogram::default()
        })
    }

    fn window<'a>(ts: &'a [i64], vs: &'a [f64]) -> Window<'a> {
        Window {
            ts,
            vs,
            range_start: 0,
            range_end: 60_000,
            range_ms: 60_000,
        }
    }

    fn count_of(v: Option<Value>) -> Option<f64> {
        match v? {
            Value::Histogram(h) => Some(h.count),
            Value::Float(_) => panic!("expected a histogram"),
        }
    }

    const UNKNOWN: CounterResetHint = CounterResetHint::Unknown;

    #[test]
    fn a_rate_over_histograms_extrapolates_like_a_float_counter() {
        // Samples every 15s from 15s to 60s: count grows 3 per sample.
        let ts = [15_000, 30_000, 45_000, 60_000];
        let vs = [0.0; 4];
        let hs = [
            h(3.0, UNKNOWN),
            h(6.0, UNKNOWN),
            h(9.0, UNKNOWN),
            h(12.0, UNKNOWN),
        ]
        .map(Some);
        let floats = [3.0, 6.0, 9.0, 12.0];
        for func in [Func::Rate, Func::Increase, Func::Delta] {
            let want = range::evaluate(func, &window(&ts, &floats)).unwrap();
            let got = count_of(evaluate(func, &window(&ts, &vs), &hs)).unwrap();
            assert!((got - want).abs() < 1e-12, "{func:?}: {got} != {want}");
        }
        let Some(Value::Histogram(rate)) = evaluate(Func::Rate, &window(&ts, &vs), &hs) else {
            panic!("expected a histogram");
        };
        assert_eq!(rate.counter_reset_hint, CounterResetHint::Gauge);
        assert_eq!(rate.positive_buckets.len(), 1);
    }

    #[test]
    fn a_counter_reset_adds_the_lost_count_back() {
        let ts = [15_000, 30_000, 45_000, 60_000];
        let vs = [0.0; 4];
        let hs = [
            h(3.0, UNKNOWN),
            h(6.0, UNKNOWN),
            h(1.0, UNKNOWN),
            h(4.0, UNKNOWN),
        ]
        .map(Some);
        let floats = [3.0, 6.0, 1.0, 4.0];
        let want = range::evaluate(Func::Increase, &window(&ts, &floats)).unwrap();
        let got = count_of(evaluate(Func::Increase, &window(&ts, &vs), &hs)).unwrap();
        assert!((got - want).abs() < 1e-12, "{got} != {want}");
    }

    #[test]
    fn a_range_mixing_floats_and_histograms_has_no_rate_or_sum() {
        let ts = [15_000, 30_000, 45_000];
        let vs = [1.0, 0.0, 0.0];
        let hs = [None, Some(h(1.0, UNKNOWN)), Some(h(2.0, UNKNOWN))];
        for func in [
            Func::Rate,
            Func::Increase,
            Func::Delta,
            Func::SumOverTime,
            Func::AvgOverTime,
        ] {
            assert_eq!(evaluate(func, &window(&ts, &vs), &hs), None, "{func:?}");
        }
        // The last two are both histograms, which is all `irate` reads.
        assert!(evaluate(Func::Irate, &window(&ts, &vs), &hs).is_some());
        let float_last = [Some(h(1.0, UNKNOWN)), None, None];
        let float_vs = [0.0, 1.0, 3.0];
        assert_eq!(
            evaluate(Func::Idelta, &window(&ts, &float_vs), &float_last),
            Some(Value::Float(2.0))
        );
        let split = [None, None, Some(h(1.0, UNKNOWN))];
        assert_eq!(evaluate(Func::Irate, &window(&ts, &vs), &split), None);
    }

    #[test]
    fn a_single_histogram_has_no_rate() {
        let hs = [Some(h(1.0, UNKNOWN))];
        assert_eq!(evaluate(Func::Rate, &window(&[30_000], &[0.0]), &hs), None);
        assert_eq!(evaluate(Func::Irate, &window(&[30_000], &[0.0]), &hs), None);
    }

    #[test]
    fn sums_and_means_of_histograms() {
        let ts = [15_000, 30_000, 45_000];
        let vs = [0.0; 3];
        let hs = [h(1.0, UNKNOWN), h(2.0, UNKNOWN), h(6.0, UNKNOWN)].map(Some);
        assert_eq!(
            count_of(evaluate(Func::SumOverTime, &window(&ts, &vs), &hs)),
            Some(9.0)
        );
        assert_eq!(
            count_of(evaluate(Func::AvgOverTime, &window(&ts, &vs), &hs)),
            Some(3.0)
        );
        assert_eq!(
            evaluate(Func::CountOverTime, &window(&ts, &vs), &hs),
            Some(Value::Float(3.0))
        );
        assert_eq!(
            count_of(evaluate(Func::LastOverTime, &window(&ts, &vs), &hs)),
            Some(6.0)
        );
    }

    #[test]
    fn float_only_functions_read_the_floats_and_skip_the_histograms() {
        let ts = [10_000, 20_000, 30_000, 40_000];
        let vs = [5.0, 0.0, 1.0, 9.0];
        let hs = [None, Some(h(100.0, UNKNOWN)), None, None];
        assert_eq!(
            evaluate(Func::MaxOverTime, &window(&ts, &vs), &hs),
            Some(Value::Float(9.0))
        );
        assert_eq!(
            evaluate(Func::MinOverTime, &window(&ts, &vs), &hs),
            Some(Value::Float(1.0))
        );
        let only = [Some(h(1.0, UNKNOWN))];
        assert_eq!(
            evaluate(Func::MaxOverTime, &window(&[10_000], &[0.0]), &only),
            None
        );
    }

    #[test]
    fn changes_and_resets_count_a_type_switch() {
        let ts = [10_000, 20_000, 30_000, 40_000, 50_000];
        let vs = [1.0, 0.0, 0.0, 0.0, 2.0];
        let hs = [
            None,
            Some(h(5.0, UNKNOWN)),
            Some(h(5.0, UNKNOWN)),
            Some(h(2.0, UNKNOWN)),
            None,
        ];
        // float→histogram, equal histograms, a smaller histogram, histogram→float.
        assert_eq!(
            evaluate(Func::Changes, &window(&ts, &vs), &hs),
            Some(Value::Float(3.0))
        );
        assert_eq!(
            evaluate(Func::Resets, &window(&ts, &vs), &hs),
            Some(Value::Float(3.0))
        );
    }
}
