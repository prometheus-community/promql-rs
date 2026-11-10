use promql_histogram::{
    Bucket, CounterResetHint, FloatHistogram, Span, TrimDirection, CUSTOM_SCHEMA,
};

fn assert_close(actual: f64, expected: f64) {
    let tolerance = 1e-12 * expected.abs().max(1.0);
    assert!(
        (actual - expected).abs() <= tolerance,
        "{actual} != {expected}"
    );
}

fn mixed_exponential() -> FloatHistogram {
    FloatHistogram {
        counter_reset_hint: CounterResetHint::Reset,
        schema: 0,
        count: 45.0,
        sum: 123.0,
        zero_threshold: 0.5,
        zero_count: 9.0,
        negative_spans: vec![Span {
            offset: 0,
            length: 4,
        }],
        negative_buckets: vec![1.0, 2.0, 3.0, 4.0],
        positive_spans: vec![Span {
            offset: 0,
            length: 4,
        }],
        positive_buckets: vec![5.0, 6.0, 7.0, 8.0],
        ..FloatHistogram::default()
    }
}

fn custom(bounds: Vec<f64>, offset: i32, buckets: Vec<f64>) -> FloatHistogram {
    FloatHistogram {
        schema: CUSTOM_SCHEMA,
        count: buckets.iter().sum(),
        positive_spans: vec![Span {
            offset,
            length: buckets.len() as u32,
        }],
        positive_buckets: buckets,
        custom_values: bounds,
        ..FloatHistogram::default()
    }
}

#[test]
fn exact_boundaries_apply_less_equal_and_strict_greater_semantics() {
    let upper = mixed_exponential().trim_buckets(4.0, TrimDirection::Upper);
    assert_eq!(upper.count, 37.0);
    assert_eq!(upper.positive_buckets, vec![5.0, 6.0, 7.0]);
    assert_eq!(
        upper.positive_spans,
        vec![Span {
            offset: 0,
            length: 3
        }]
    );
    assert_eq!(upper.negative_buckets, vec![1.0, 2.0, 3.0, 4.0]);

    let lower = mixed_exponential().trim_buckets(4.0, TrimDirection::Lower);
    assert_eq!(lower.count, 8.0);
    assert_eq!(lower.positive_buckets, vec![8.0]);
    assert_eq!(
        lower.positive_spans,
        vec![Span {
            offset: 3,
            length: 1
        }]
    );
    assert!(lower.negative_buckets.is_empty());
    assert_eq!(lower.zero_count, 0.0);
    assert_close(lower.sum, 8.0 * 32.0f64.sqrt());
}

#[test]
fn exponential_partial_bucket_uses_logarithmic_interpolation() {
    let trim = 8.0f64.sqrt();
    let histogram = FloatHistogram {
        schema: 0,
        count: 8.0,
        sum: 999.0,
        positive_spans: vec![Span {
            offset: 2,
            length: 1,
        }],
        positive_buckets: vec![8.0],
        ..FloatHistogram::default()
    };

    let upper = histogram.trim_buckets(trim, TrimDirection::Upper);
    assert_close(upper.count, 4.0);
    assert_close(upper.positive_buckets[0], 4.0);
    assert_close(upper.sum, 4.0 * (2.0 * trim).sqrt());

    let lower = histogram.trim_buckets(trim, TrimDirection::Lower);
    assert_close(lower.count, 4.0);
    assert_close(lower.positive_buckets[0], 4.0);
    assert_close(lower.sum, 4.0 * (trim * 4.0).sqrt());

    let histogram = FloatHistogram {
        schema: 0,
        count: 8.0,
        negative_spans: vec![Span {
            offset: 2,
            length: 1,
        }],
        negative_buckets: vec![8.0],
        ..FloatHistogram::default()
    };
    let upper = histogram.trim_buckets(-trim, TrimDirection::Upper);
    assert_close(upper.count, 4.0);
    assert_close(upper.sum, -4.0 * (4.0 * trim).sqrt());
    let lower = histogram.trim_buckets(-trim, TrimDirection::Lower);
    assert_close(lower.count, 4.0);
    assert_close(lower.sum, -4.0 * (trim * 2.0).sqrt());
}

#[test]
fn custom_partial_bucket_uses_linear_interpolation() {
    let histogram = custom(vec![0.0, 10.0], 1, vec![10.0]);

    let upper = histogram.trim_buckets(4.0, TrimDirection::Upper);
    assert_eq!(upper.count, 4.0);
    assert_eq!(upper.positive_buckets, vec![4.0]);
    assert_eq!(upper.sum, 8.0);
    assert_eq!(upper.custom_values, vec![0.0, 10.0]);

    let lower = histogram.trim_buckets(4.0, TrimDirection::Lower);
    assert_eq!(lower.count, 6.0);
    assert_eq!(lower.positive_buckets, vec![6.0]);
    assert_eq!(lower.sum, 42.0);
}

#[test]
fn custom_exact_boundary_respects_bucket_inclusivity() {
    let histogram = custom(vec![0.0], 0, vec![2.0, 3.0]);

    let upper = histogram.trim_buckets(0.0, TrimDirection::Upper);
    assert_eq!(upper.count, 2.0);
    assert_eq!(upper.positive_buckets, vec![2.0]);
    assert_eq!(
        upper.positive_spans,
        vec![Span {
            offset: 0,
            length: 1
        }]
    );

    let lower = histogram.trim_buckets(0.0, TrimDirection::Lower);
    assert_eq!(lower.count, 3.0);
    assert_eq!(lower.positive_buckets, vec![3.0]);
    assert_eq!(
        lower.positive_spans,
        vec![Span {
            offset: 1,
            length: 1
        }]
    );
}

#[test]
fn zero_bucket_interpolation_is_biased_toward_populated_sides() {
    let positive_only = FloatHistogram {
        schema: 0,
        count: 12.0,
        zero_threshold: 1.0,
        zero_count: 10.0,
        positive_spans: vec![Span {
            offset: 1,
            length: 1,
        }],
        positive_buckets: vec![2.0],
        ..FloatHistogram::default()
    };
    let positive_only = positive_only.trim_buckets(0.5, TrimDirection::Upper);
    assert_eq!(positive_only.zero_count, 5.0);
    assert_eq!(positive_only.count, 5.0);
    assert_eq!(positive_only.sum, 1.25);
    assert!(positive_only.positive_buckets.is_empty());

    let symmetric = FloatHistogram {
        schema: 0,
        count: 10.0,
        zero_threshold: 1.0,
        zero_count: 10.0,
        ..FloatHistogram::default()
    };
    let symmetric = symmetric.trim_buckets(0.0, TrimDirection::Upper);
    assert_eq!(symmetric.zero_count, 5.0);
    assert_eq!(symmetric.count, 5.0);
    assert_eq!(symmetric.sum, -2.5);
}

#[test]
fn custom_infinite_buckets_follow_prometheus_conservative_rules() {
    let histogram = custom(vec![10.0], 0, vec![10.0, 20.0]);

    let upper = histogram.trim_buckets(4.0, TrimDirection::Upper);
    assert_eq!(upper.count, 4.0);
    assert_eq!(upper.sum, 8.0);
    assert_eq!(upper.positive_buckets, vec![4.0]);
    assert_eq!(
        upper.positive_spans,
        vec![Span {
            offset: 0,
            length: 1
        }]
    );

    let lower = histogram.trim_buckets(12.0, TrimDirection::Lower);
    assert_eq!(lower.count, 20.0);
    assert_eq!(lower.sum, 240.0);
    assert_eq!(lower.positive_buckets, vec![20.0]);
    assert_eq!(
        lower.positive_spans,
        vec![Span {
            offset: 1,
            length: 1
        }]
    );
}

#[test]
fn exponential_infinity_bucket_is_separate_from_max_finite_values() {
    let histogram = FloatHistogram {
        schema: 0,
        count: 3.0,
        sum: f64::INFINITY,
        positive_spans: vec![Span {
            offset: 1025,
            length: 1,
        }],
        positive_buckets: vec![3.0],
        ..FloatHistogram::default()
    };

    let finite = histogram.trim_buckets(f64::MAX, TrimDirection::Upper);
    assert_eq!(finite.count, 0.0);
    assert_eq!(finite.sum, 0.0);
    assert!(finite.positive_buckets.is_empty());

    let infinite_only = histogram.trim_buckets(f64::MAX, TrimDirection::Lower);
    assert_eq!(infinite_only.count, 3.0);
    assert_eq!(infinite_only.sum, f64::INFINITY);
}

#[test]
fn complementary_trims_preserve_bucket_count_identity() {
    let histogram = mixed_exponential();
    let original = histogram.clone();
    let upper = histogram.trim_buckets(3.0, TrimDirection::Upper);
    let lower = histogram.trim_buckets(3.0, TrimDirection::Lower);
    assert_eq!(histogram, original);
    assert_close(upper.count + lower.count, histogram.count);

    let histogram = custom(vec![0.0, 10.0], 1, vec![10.0]);
    let upper = histogram.trim_buckets(3.0, TrimDirection::Upper);
    let lower = histogram.trim_buckets(3.0, TrimDirection::Lower);
    assert_eq!(upper.count + lower.count, histogram.count);
}

#[test]
fn no_op_preserves_bits_layout_and_metadata_in_independent_storage() {
    let histogram = FloatHistogram {
        counter_reset_hint: CounterResetHint::Gauge,
        schema: 0,
        count: f64::from_bits(0x7ff8_0000_0000_0042),
        sum: f64::from_bits(0x8000_0000_0000_0000),
        zero_threshold: 0.5,
        zero_count: 2.0,
        negative_spans: vec![Span {
            offset: 0,
            length: 2,
        }],
        negative_buckets: vec![1.0, 0.0],
        positive_spans: vec![
            Span {
                offset: 0,
                length: 1,
            },
            Span {
                offset: 4,
                length: 0,
            },
        ],
        positive_buckets: vec![3.0],
        ..FloatHistogram::default()
    };
    let original = histogram.clone();
    let positive_ptr = histogram.positive_buckets.as_ptr();
    let negative_ptr = histogram.negative_buckets.as_ptr();

    let histogram = histogram
        .trim_buckets(f64::INFINITY, TrimDirection::Upper)
        .trim_buckets(f64::NEG_INFINITY, TrimDirection::Lower);

    assert_eq!(histogram.count.to_bits(), original.count.to_bits());
    assert_eq!(histogram.sum.to_bits(), original.sum.to_bits());
    assert_eq!(
        histogram.zero_count.to_bits(),
        original.zero_count.to_bits()
    );
    assert_eq!(histogram.positive_spans, original.positive_spans);
    assert_eq!(histogram.negative_spans, original.negative_spans);
    assert_eq!(histogram.positive_buckets, original.positive_buckets);
    assert_eq!(histogram.negative_buckets, original.negative_buckets);
    assert_ne!(histogram.positive_buckets.as_ptr(), positive_ptr);
    assert_ne!(histogram.negative_buckets.as_ptr(), negative_ptr);
    assert_eq!(histogram.counter_reset_hint, CounterResetHint::Gauge);
    assert_eq!(histogram.schema, 0);
    assert_eq!(histogram.zero_threshold, 0.5);
}

#[test]
fn changed_trim_preserves_schema_bounds_threshold_and_reset_hint() {
    let exponential = mixed_exponential().trim_buckets(0.0, TrimDirection::Upper);
    assert_eq!(exponential.schema, 0);
    assert_eq!(exponential.zero_threshold, 0.5);
    assert_eq!(exponential.counter_reset_hint, CounterResetHint::Reset);

    let mut custom = custom(vec![-5.0, 0.0, 10.0], 0, vec![1.0, 2.0, 3.0, 4.0]);
    custom.counter_reset_hint = CounterResetHint::NotReset;
    let custom = custom.trim_buckets(2.0, TrimDirection::Upper);
    assert_eq!(custom.schema, CUSTOM_SCHEMA);
    assert_eq!(custom.custom_values, vec![-5.0, 0.0, 10.0]);
    assert_eq!(custom.counter_reset_hint, CounterResetHint::NotReset);
}

#[test]
fn nan_and_empty_inputs_match_prometheus_behavior() {
    let empty = FloatHistogram {
        schema: 0,
        count: f64::from_bits(0x7ff8_0000_0000_0011),
        sum: f64::from_bits(0x7ff8_0000_0000_0022),
        positive_spans: vec![Span {
            offset: 4,
            length: 1,
        }],
        positive_buckets: vec![0.0],
        ..FloatHistogram::default()
    };
    let count_bits = empty.count.to_bits();
    let sum_bits = empty.sum.to_bits();
    let empty = empty.trim_buckets(f64::NAN, TrimDirection::Upper);
    assert_eq!(empty.count.to_bits(), count_bits);
    assert_eq!(empty.sum.to_bits(), sum_bits);
    assert_eq!(empty.positive_buckets, vec![0.0]);

    let mut populated = custom(vec![0.0, 10.0], 1, vec![3.0]);
    populated.sum = 7.0;
    let populated = populated.trim_buckets(f64::NAN, TrimDirection::Upper);
    assert_eq!(populated.count, 0.0);
    assert_eq!(populated.sum, 0.0);
    assert!(populated.positive_spans.is_empty());
    assert!(populated.positive_buckets.is_empty());
}

#[test]
fn bucket_fraction_below_supports_linear_and_exponential_interpolation() {
    let linear = Bucket {
        lower: 0.0,
        upper: 10.0,
        lower_inclusive: false,
        upper_inclusive: true,
        count: 1.0,
        index: 0,
    };
    assert_eq!(linear.fraction_below(2.5, true), 0.25);

    let positive = Bucket {
        lower: 2.0,
        upper: 4.0,
        ..linear
    };
    assert_close(positive.fraction_below(8.0f64.sqrt(), false), 0.5);
    let negative = Bucket {
        lower: -4.0,
        upper: -2.0,
        ..linear
    };
    assert_close(negative.fraction_below(-8.0f64.sqrt(), false), 0.5);
}
