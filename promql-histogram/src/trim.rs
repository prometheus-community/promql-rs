// Licensed under the Apache License, Version 2.0.
// Derived from Prometheus model/histogram/float_histogram.go.

use crate::{buckets::bucket, Bucket, FloatHistogram, Span};

/// The side of a histogram retained by [`FloatHistogram::trim_buckets`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TrimDirection {
    /// Retain observations less than or equal to the trim value (`h </ x`).
    Upper,
    /// Retain observations greater than the trim value (`h >/ x`).
    Lower,
}

impl FloatHistogram {
    /// Prometheus `FloatHistogram.TrimBuckets`: return a trimmed copy.
    ///
    /// Totals are estimated from bucket midpoints only when a populated bucket
    /// changes. Even a no-op returns independent storage: evaluating a trim
    /// must not mutate a sample shared with another expression.
    #[must_use]
    #[inline]
    pub fn trim_buckets(&self, value: f64, direction: TrimDirection) -> Self {
        let mut trimmed = self.clone();
        trimmed.trim_in_place(value, direction);
        trimmed
    }

    fn trim_in_place(&mut self, value: f64, direction: TrimDirection) {
        let custom = self.uses_custom_buckets();
        let mut updated_count = 0.0;
        let mut updated_sum = 0.0;

        let (positive_changed, has_positive) = trim_side(
            self.schema,
            &self.custom_values,
            &self.positive_spans,
            &mut self.positive_buckets,
            value,
            direction,
            true,
            custom,
            &mut updated_count,
            &mut updated_sum,
        );
        let (negative_changed, has_negative) = trim_side(
            self.schema,
            &self.custom_values,
            &self.negative_spans,
            &mut self.negative_buckets,
            value,
            direction,
            false,
            custom,
            &mut updated_count,
            &mut updated_sum,
        );
        let mut changed = positive_changed || negative_changed;

        if self.zero_count > 0.0 {
            let zero_bucket = Bucket {
                lower: -self.zero_threshold,
                upper: self.zero_threshold,
                lower_inclusive: true,
                upper_inclusive: true,
                count: self.zero_count,
                index: 0,
            };
            let (keep_count, midpoint) =
                trim_zero_bucket(zero_bucket, value, has_negative, has_positive, direction);
            if self.zero_count != keep_count {
                self.zero_count = keep_count;
                changed = true;
            }
            updated_count += keep_count;
            updated_sum += midpoint * keep_count;
        }

        if changed {
            self.count = updated_count;
            self.sum = updated_sum;
            self.compact();
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn trim_side(
    schema: i32,
    custom_values: &[f64],
    spans: &[Span],
    buckets: &mut [f64],
    value: f64,
    direction: TrimDirection,
    positive: bool,
    linear: bool,
    updated_count: &mut f64,
    updated_sum: &mut f64,
) -> (bool, bool) {
    let mut changed = false;
    let mut has_observations = false;
    let mut index = 0i64;
    let mut bucket_position = 0usize;

    for span in spans {
        index += i64::from(span.offset);
        for _ in 0..span.length {
            let count = buckets[bucket_position];
            let decoded = bucket(
                schema,
                custom_values,
                i32::try_from(index).expect("validated histogram bucket index"),
                count,
                positive,
            );
            index += 1;

            if count != 0.0 {
                has_observations = true;
                match direction {
                    TrimDirection::Upper if decoded.upper <= value => {
                        *updated_count += count;
                        *updated_sum +=
                            midpoint(decoded.lower, decoded.upper, positive, linear) * count;
                    }
                    TrimDirection::Lower if decoded.lower >= value => {
                        *updated_count += count;
                        *updated_sum +=
                            midpoint(decoded.lower, decoded.upper, positive, linear) * count;
                    }
                    TrimDirection::Upper if decoded.lower < value => {
                        let (keep_count, kept_midpoint) =
                            trim_bucket(decoded, value, direction, positive, linear);
                        *updated_count += keep_count;
                        *updated_sum += kept_midpoint * keep_count;
                        if count != keep_count {
                            buckets[bucket_position] = keep_count;
                            changed = true;
                        }
                    }
                    TrimDirection::Lower if decoded.upper > value => {
                        let (keep_count, kept_midpoint) =
                            trim_bucket(decoded, value, direction, positive, linear);
                        *updated_count += keep_count;
                        *updated_sum += kept_midpoint * keep_count;
                        if count != keep_count {
                            buckets[bucket_position] = keep_count;
                            changed = true;
                        }
                    }
                    _ => {
                        buckets[bucket_position] = 0.0;
                        changed = true;
                    }
                }
            }
            bucket_position += 1;
        }
    }
    (changed, has_observations)
}

fn trim_bucket(
    bucket: Bucket,
    value: f64,
    direction: TrimDirection,
    positive: bool,
    linear: bool,
) -> (f64, f64) {
    if bucket.lower == f64::NEG_INFINITY || bucket.upper == f64::INFINITY {
        return trim_infinite_bucket(direction, bucket, value);
    }

    let below_count = split_count(bucket, value, linear);
    match direction {
        TrimDirection::Upper => (below_count, midpoint(bucket.lower, value, positive, linear)),
        TrimDirection::Lower => (
            bucket.count - below_count,
            midpoint(value, bucket.upper, positive, linear),
        ),
    }
}

fn split_count(bucket: Bucket, value: f64, linear: bool) -> f64 {
    if value <= bucket.lower {
        return 0.0;
    }
    if value >= bucket.upper {
        return bucket.count;
    }
    bucket.count * bucket.fraction_below(value, linear)
}

fn trim_zero_bucket(
    bucket: Bucket,
    value: f64,
    has_negative: bool,
    has_positive: bool,
    direction: TrimDirection,
) -> (f64, f64) {
    let mut lower = bucket.lower;
    let mut upper = bucket.upper;
    if has_negative && !has_positive {
        upper = 0.0;
    }
    if has_positive && !has_negative {
        lower = 0.0;
    }

    match direction {
        TrimDirection::Upper if value <= lower => (0.0, 0.0),
        TrimDirection::Upper if value >= upper => (bucket.count, (lower + upper) / 2.0),
        TrimDirection::Upper => {
            let fraction = (value - lower) / (upper - lower);
            (bucket.count * fraction, (lower + value) / 2.0)
        }
        TrimDirection::Lower if value <= lower => (bucket.count, (lower + upper) / 2.0),
        TrimDirection::Lower if value >= upper => (0.0, 0.0),
        TrimDirection::Lower => {
            let fraction = (upper - value) / (upper - lower);
            (bucket.count * fraction, (value + upper) / 2.0)
        }
    }
}

fn trim_infinite_bucket(direction: TrimDirection, bucket: Bucket, value: f64) -> (f64, f64) {
    let zero_if_infinite = |value: f64| if value.is_infinite() { 0.0 } else { value };

    if bucket.lower == f64::NEG_INFINITY {
        return match direction {
            TrimDirection::Upper if value >= bucket.upper => (bucket.count, 0.0),
            TrimDirection::Upper
                if value > 0.0 && bucket.upper > 0.0 && bucket.upper.is_finite() =>
            {
                (bucket.count * value / bucket.upper, value / 2.0)
            }
            TrimDirection::Upper if bucket.upper <= 0.0 => (bucket.count, value),
            TrimDirection::Upper => (0.0, zero_if_infinite(bucket.upper)),
            TrimDirection::Lower if value <= bucket.lower => (bucket.count, 0.0),
            TrimDirection::Lower
                if value >= 0.0 && bucket.upper > value && bucket.upper.is_finite() =>
            {
                (
                    bucket.count * (1.0 - value / bucket.upper),
                    (value + bucket.upper) / 2.0,
                )
            }
            TrimDirection::Lower => (0.0, zero_if_infinite(bucket.upper)),
        };
    }

    debug_assert_eq!(bucket.upper, f64::INFINITY);
    match direction {
        TrimDirection::Upper => (0.0, zero_if_infinite(bucket.lower)),
        TrimDirection::Lower if value >= bucket.lower => (bucket.count, value),
        TrimDirection::Lower => (0.0, zero_if_infinite(bucket.lower)),
    }
}

fn midpoint(lower: f64, upper: f64, positive: bool, linear: bool) -> f64 {
    if lower.is_infinite() {
        if upper.is_infinite() {
            return 0.0;
        }
        return if upper > 0.0 { upper / 2.0 } else { upper };
    }
    if upper.is_infinite() {
        return lower;
    }
    if linear {
        return (lower + upper) / 2.0;
    }

    let geometric_mean = (lower * upper).abs().sqrt();
    if positive {
        geometric_mean
    } else {
        -geometric_mean
    }
}
