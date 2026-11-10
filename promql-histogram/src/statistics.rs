// Licensed under the Apache License, Version 2.0.
// Derived from Prometheus promql/functions.go and promql/quantile.go.

use std::cmp::Ordering;

use crate::{arithmetic::kahan_inc, Bucket, FloatHistogram};

const SMALL_DELTA_TOLERANCE: f64 = 1e-12;

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct QuantileResult {
    pub value: f64,
    pub nan_skew: bool,
    pub nan_result: bool,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct FractionResult {
    pub value: f64,
    pub nan_observations: bool,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ClassicBucket {
    pub upper_bound: f64,
    pub count: f64,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ClassicQuantileResult {
    pub value: f64,
    pub forced_monotonicity: bool,
}

#[inline]
pub fn average(histogram: &FloatHistogram) -> f64 {
    histogram.sum / histogram.count
}

pub fn quantile(q: f64, histogram: &FloatHistogram) -> QuantileResult {
    if q < 0.0 {
        return quantile_value(f64::NEG_INFINITY);
    }
    if q > 1.0 {
        return quantile_value(f64::INFINITY);
    }
    if histogram.count == 0.0 || q.is_nan() {
        return quantile_value(f64::NAN);
    }

    let forward = histogram.sum.is_nan() || q < 0.5;
    let mut buckets = if forward {
        histogram.all_buckets()
    } else {
        histogram.all_buckets_rev()
    };
    let mut rank = if forward {
        q * histogram.count
    } else {
        (1.0 - q) * histogram.count
    };
    let mut count = 0.0;
    let mut bucket = Bucket {
        lower: 0.0,
        upper: 0.0,
        lower_inclusive: false,
        upper_inclusive: false,
        count: 0.0,
        index: 0,
    };

    for candidate in buckets.by_ref() {
        bucket = candidate;
        if bucket.count == 0.0 {
            continue;
        }
        count += bucket.count;
        if count >= rank {
            break;
        }
    }

    if !histogram.uses_custom_buckets() && bucket.lower < 0.0 && bucket.upper > 0.0 {
        if histogram.negative_buckets.is_empty() && !histogram.positive_buckets.is_empty() {
            bucket.lower = 0.0;
        } else if histogram.positive_buckets.is_empty() && !histogram.negative_buckets.is_empty() {
            bucket.upper = 0.0;
        }
    } else if histogram.uses_custom_buckets() {
        if bucket.lower == f64::NEG_INFINITY {
            if bucket.upper <= 0.0 {
                return quantile_value(bucket.upper);
            }
            bucket.lower = 0.0;
        } else if bucket.upper == f64::INFINITY {
            return quantile_value(bucket.lower);
        }
    }

    count = count.min(histogram.count);
    if count < rank {
        if histogram.sum.is_nan() {
            return QuantileResult {
                value: f64::NAN,
                nan_skew: false,
                nan_result: true,
            };
        }
        return quantile_value(bucket.upper);
    }

    if forward {
        rank -= count - bucket.count;
    } else {
        rank = count - rank;
    }

    let mut nan_skew = false;
    if histogram.sum.is_nan() {
        for remaining in buckets {
            count += remaining.count;
        }
        nan_skew = count < histogram.count;
    }

    let fraction = rank / bucket.count;
    let value = if histogram.uses_custom_buckets() || bucket.lower <= 0.0 && bucket.upper >= 0.0 {
        bucket.lower + (bucket.upper - bucket.lower) * fraction
    } else {
        let log_lower = bucket.lower.abs().log2();
        let log_upper = bucket.upper.abs().log2();
        if bucket.lower > 0.0 {
            (log_lower + (log_upper - log_lower) * fraction).exp2()
        } else {
            -(log_upper + (log_lower - log_upper) * (1.0 - fraction)).exp2()
        }
    };

    QuantileResult {
        value,
        nan_skew,
        nan_result: false,
    }
}

/// Prometheus `HistogramFraction` at the revision in `UPSTREAM.md`.
pub fn fraction(lower: f64, upper: f64, histogram: &FloatHistogram) -> FractionResult {
    if histogram.count == 0.0 || lower.is_nan() || upper.is_nan() {
        return fraction_value(f64::NAN);
    }
    if lower >= upper {
        return fraction_value(0.0);
    }

    let mut count = 0.0;
    let mut rank = 0.0;
    let mut lower_rank = 0.0;
    let mut upper_rank = 0.0;
    let mut lower_set = false;
    let mut upper_set = false;
    let mut buckets = histogram.all_buckets();

    for mut bucket in buckets.by_ref() {
        count += bucket.count;
        let zero_bucket = bucket.lower <= 0.0 && bucket.upper >= 0.0;
        if zero_bucket {
            if histogram.negative_buckets.is_empty() && !histogram.positive_buckets.is_empty() {
                bucket.lower = 0.0;
            } else if histogram.positive_buckets.is_empty()
                && !histogram.negative_buckets.is_empty()
            {
                bucket.upper = 0.0;
            }
        }

        if !lower_set && bucket.lower >= lower {
            lower_rank = rank;
            lower_set = true;
        }
        if !upper_set && bucket.lower >= upper {
            upper_rank = rank;
            upper_set = true;
        }
        if lower_set && upper_set {
            break;
        }
        if !lower_set && bucket.lower < lower && bucket.upper > lower {
            lower_rank = interpolate_rank(
                rank,
                bucket,
                lower,
                histogram.uses_custom_buckets() || zero_bucket,
            );
            lower_set = true;
        }
        if !upper_set && bucket.lower < upper && bucket.upper > upper {
            upper_rank = interpolate_rank(
                rank,
                bucket,
                upper,
                histogram.uses_custom_buckets() || zero_bucket,
            );
            upper_set = true;
        }
        if lower_set && upper_set {
            break;
        }
        rank += bucket.count;
    }

    let nan_observations = if histogram.sum.is_nan() {
        for bucket in buckets {
            count += bucket.count;
        }
        count < histogram.count
    } else {
        count = histogram.count;
        false
    };

    if !lower_set || lower_rank > count {
        lower_rank = count;
    }
    if !upper_set || upper_rank > count {
        upper_rank = count;
    }

    FractionResult {
        value: (upper_rank - lower_rank) / histogram.count,
        nan_observations,
    }
}

pub fn variance(histogram: &FloatHistogram) -> f64 {
    let mean = average(histogram);
    let mut variance = 0.0;
    let mut compensation = 0.0;
    for bucket in histogram.all_buckets() {
        if bucket.count == 0.0 {
            continue;
        }
        let representative = if histogram.uses_custom_buckets() {
            (bucket.upper + bucket.lower) / 2.0
        } else if bucket.lower <= 0.0 && bucket.upper >= 0.0 {
            0.0
        } else {
            let midpoint = (bucket.upper * bucket.lower).sqrt();
            if bucket.upper < 0.0 {
                -midpoint
            } else {
                midpoint
            }
        };
        let delta = representative - mean;
        (variance, compensation) = kahan_inc(bucket.count * delta * delta, variance, compensation);
    }
    (variance + compensation) / histogram.count
}

#[inline]
pub fn stddev(histogram: &FloatHistogram) -> f64 {
    variance(histogram).sqrt()
}

pub fn classic_quantile(q: f64, buckets: &mut [ClassicBucket]) -> ClassicQuantileResult {
    if q.is_nan() {
        return classic_quantile_value(f64::NAN, false);
    }
    if q < 0.0 {
        return classic_quantile_value(f64::NEG_INFINITY, false);
    }
    if q > 1.0 {
        return classic_quantile_value(f64::INFINITY, false);
    }
    if buckets.is_empty() {
        return classic_quantile_value(f64::NAN, false);
    }

    sort_classic_buckets(buckets);
    if buckets.last().expect("non-empty buckets").upper_bound != f64::INFINITY {
        return classic_quantile_value(f64::NAN, false);
    }
    let buckets = coalesce_classic_buckets(buckets);
    let forced_monotonicity = repair_classic_buckets(buckets);
    if buckets.len() < 2 {
        return classic_quantile_value(f64::NAN, forced_monotonicity);
    }
    let observations = buckets.last().expect("at least two buckets").count;
    if observations == 0.0 {
        return classic_quantile_value(f64::NAN, forced_monotonicity);
    }

    let mut rank = q * observations;
    let bucket_index = buckets[..buckets.len() - 1].partition_point(|bucket| bucket.count < rank);
    if bucket_index == buckets.len() - 1 {
        return classic_quantile_value(buckets[buckets.len() - 2].upper_bound, forced_monotonicity);
    }
    if bucket_index == 0 && buckets[0].upper_bound <= 0.0 {
        return classic_quantile_value(buckets[0].upper_bound, forced_monotonicity);
    }

    let mut bucket_start = 0.0;
    let bucket_end = buckets[bucket_index].upper_bound;
    let mut count = buckets[bucket_index].count;
    if bucket_index > 0 {
        bucket_start = buckets[bucket_index - 1].upper_bound;
        count -= buckets[bucket_index - 1].count;
        rank -= buckets[bucket_index - 1].count;
    }
    classic_quantile_value(
        bucket_start + (bucket_end - bucket_start) * rank / count,
        forced_monotonicity,
    )
}

pub fn classic_fraction(lower: f64, upper: f64, buckets: &mut [ClassicBucket]) -> f64 {
    if buckets.is_empty() {
        return f64::NAN;
    }
    sort_classic_buckets(buckets);
    if buckets.last().expect("non-empty buckets").upper_bound != f64::INFINITY {
        return f64::NAN;
    }
    let buckets = coalesce_classic_buckets(buckets);
    let count = buckets.last().expect("non-empty buckets").count;
    if count == 0.0 || lower.is_nan() || upper.is_nan() {
        return f64::NAN;
    }
    if lower >= upper {
        return 0.0;
    }

    let mut rank = 0.0;
    let mut lower_rank = 0.0;
    let mut upper_rank = 0.0;
    let mut lower_set = false;
    let mut upper_set = false;
    let mut lower_bound = if buckets[0].upper_bound > 0.0 {
        0.0
    } else {
        f64::NEG_INFINITY
    };

    for (index, bucket) in buckets.iter().enumerate() {
        if index > 0 {
            lower_bound = buckets[index - 1].upper_bound;
        }
        let upper_bound = bucket.upper_bound;
        if !lower_set && lower_bound >= lower {
            lower_rank = rank;
            lower_set = true;
        }
        if !upper_set && lower_bound >= upper {
            upper_rank = rank;
            upper_set = true;
        }
        if lower_set && upper_set {
            break;
        }
        if !lower_set && lower_bound < lower && upper_bound > lower {
            lower_rank = classic_interpolate(rank, *bucket, lower_bound, lower);
            lower_set = true;
        }
        if !upper_set && lower_bound < upper && upper_bound > upper {
            upper_rank = classic_interpolate(rank, *bucket, lower_bound, upper);
            upper_set = true;
        }
        if lower_set && upper_set {
            break;
        }
        rank = bucket.count;
    }

    if !lower_set || lower_rank > count {
        lower_rank = count;
    }
    if !upper_set || upper_rank > count {
        upper_rank = count;
    }
    (upper_rank - lower_rank) / count
}

/// Merges adjacent classic buckets with equal upper bounds. The input must be sorted.
pub fn coalesce_classic_buckets(buckets: &mut [ClassicBucket]) -> &mut [ClassicBucket] {
    if buckets.is_empty() {
        return buckets;
    }
    let mut write = 0;
    for read in 1..buckets.len() {
        if buckets[read].upper_bound == buckets[write].upper_bound {
            buckets[write].count += buckets[read].count;
        } else {
            write += 1;
            buckets[write] = buckets[read];
        }
    }
    &mut buckets[..=write]
}

/// Ignores insignificant count deltas and repairs decreasing cumulative counts.
pub fn repair_classic_buckets(buckets: &mut [ClassicBucket]) -> bool {
    let Some((first, rest)) = buckets.split_first_mut() else {
        return false;
    };
    let mut previous = first.count;
    let mut forced = false;
    for bucket in rest {
        if bucket.count == previous {
            continue;
        }
        if almost_equal(previous, bucket.count, SMALL_DELTA_TOLERANCE) {
            bucket.count = previous;
            continue;
        }
        if bucket.count < previous {
            bucket.count = previous;
            forced = true;
            continue;
        }
        previous = bucket.count;
    }
    forced
}

fn interpolate_rank(rank: f64, bucket: Bucket, value: f64, linear: bool) -> f64 {
    if linear {
        if bucket.lower == f64::NEG_INFINITY {
            return bucket.count;
        }
        // Go multiplies before dividing. Factoring this through fraction_below
        // changes rounding and overflow relative to the pinned HistogramFraction.
        return rank + bucket.count * (value - bucket.lower) / (bucket.upper - bucket.lower);
    }
    rank + bucket.count * bucket.fraction_below(value, false)
}

fn sort_classic_buckets(buckets: &mut [ClassicBucket]) {
    buckets.sort_unstable_by(|left, right| {
        left.upper_bound
            .partial_cmp(&right.upper_bound)
            .unwrap_or(Ordering::Equal)
    });
}

fn classic_interpolate(rank: f64, bucket: ClassicBucket, lower_bound: f64, value: f64) -> f64 {
    if lower_bound == f64::NEG_INFINITY {
        return bucket.count;
    }
    rank + (bucket.count - rank) * (value - lower_bound) / (bucket.upper_bound - lower_bound)
}

fn almost_equal(left: f64, right: f64, epsilon: f64) -> bool {
    if left.is_nan() && right.is_nan() {
        return true;
    }
    if left == right {
        return true;
    }
    let absolute_sum = left.abs() + right.abs();
    let difference = (left - right).abs();
    if left == 0.0 || right == 0.0 || absolute_sum < f64::MIN_POSITIVE {
        return difference < epsilon * f64::MIN_POSITIVE;
    }
    difference / absolute_sum.min(f64::MAX) < epsilon
}

const fn quantile_value(value: f64) -> QuantileResult {
    QuantileResult {
        value,
        nan_skew: false,
        nan_result: false,
    }
}

const fn fraction_value(value: f64) -> FractionResult {
    FractionResult {
        value,
        nan_observations: false,
    }
}

const fn classic_quantile_value(value: f64, forced_monotonicity: bool) -> ClassicQuantileResult {
    ClassicQuantileResult {
        value,
        forced_monotonicity,
    }
}
