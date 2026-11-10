use promql_histogram::{
    CounterResetHint, FloatHistogram, HistogramError, HistogramRef, IngressValidation, KahanSum,
    Span, ValidationError, CUSTOM_SCHEMA,
};

fn exponential(schema: i32, offset: i32, buckets: Vec<f64>) -> FloatHistogram {
    FloatHistogram {
        schema,
        count: buckets.iter().sum(),
        positive_spans: vec![Span {
            offset,
            length: buckets.len() as u32,
        }],
        positive_buckets: buckets,
        ..FloatHistogram::default()
    }
}

fn custom(bounds: Vec<f64>, buckets: Vec<f64>) -> FloatHistogram {
    FloatHistogram {
        schema: CUSTOM_SCHEMA,
        count: buckets.iter().sum(),
        positive_spans: vec![Span {
            offset: 0,
            length: buckets.len() as u32,
        }],
        positive_buckets: buckets,
        custom_values: bounds,
        ..FloatHistogram::default()
    }
}

#[test]
fn float_histogram_equals_uses_prometheus_value_semantics() {
    let mut histogram = exponential(1, -2, vec![1.0, 2.0, 3.0]);
    histogram.sum = f64::from_bits(0x7ff8_0000_0000_0042);
    histogram.zero_threshold = 0.0;
    histogram.zero_count = -0.0;
    histogram.counter_reset_hint = CounterResetHint::Reset;

    let mut same = histogram.clone();
    same.counter_reset_hint = CounterResetHint::Gauge;
    same.zero_threshold = -0.0;
    assert!(histogram.equals(&same));
    assert_eq!(histogram, same);

    let mut different_nan = histogram.clone();
    different_nan.sum = f64::from_bits(0x7ff8_0000_0000_0043);
    assert!(!histogram.equals(&different_nan));

    let mut different_zero_count = histogram.clone();
    different_zero_count.zero_count = 0.0;
    assert!(!histogram.equals(&different_zero_count));
}

#[test]
fn float_histogram_equals_normalizes_only_empty_spans() {
    let histogram = FloatHistogram {
        schema: 0,
        count: 3.0,
        positive_spans: vec![
            Span {
                offset: -2,
                length: 1,
            },
            Span {
                offset: 3,
                length: 2,
            },
        ],
        positive_buckets: vec![1.0, 2.0, 0.0],
        ..FloatHistogram::default()
    };
    let mut equivalent = histogram.clone();
    equivalent.positive_spans = vec![
        Span {
            offset: -2,
            length: 1,
        },
        Span {
            offset: 1,
            length: 0,
        },
        Span {
            offset: 2,
            length: 2,
        },
        Span {
            offset: 99,
            length: 0,
        },
    ];
    assert!(histogram.equals(&equivalent));

    let mut extra_empty_bucket = histogram.clone();
    extra_empty_bucket.positive_spans = vec![Span {
        offset: -2,
        length: 5,
    }];
    extra_empty_bucket.positive_buckets = vec![1.0, 0.0, 0.0, 2.0, 0.0];
    assert!(!histogram.equals(&extra_empty_bucket));
}

#[test]
fn float_histogram_equals_checks_custom_bounds_and_unexpected_fields() {
    let histogram = custom(vec![1.0, 2.0], vec![1.0, 2.0, 3.0]);
    let mut different_bounds = histogram.clone();
    different_bounds.custom_values[1] = 3.0;
    assert!(!histogram.equals(&different_bounds));

    let mut signed_zero_bound = histogram.clone();
    signed_zero_bound.custom_values[0] = -0.0;
    let mut positive_zero_bound = histogram.clone();
    positive_zero_bound.custom_values[0] = 0.0;
    assert!(signed_zero_bound.equals(&positive_zero_bound));

    let mut unexpected_zero = histogram.clone();
    unexpected_zero.zero_count = 1.0;
    assert!(!histogram.equals(&unexpected_zero));
}

#[test]
fn ingress_validation_and_reserved_schema_reduction_are_separate() {
    let mut high_resolution = exponential(52, -1, vec![2.0, 3.0]);
    high_resolution.validate_ingress().unwrap();
    high_resolution.normalize_ingress().unwrap();
    assert_eq!(high_resolution.schema, 8);
    assert_eq!(
        high_resolution.positive_spans,
        vec![Span {
            offset: 0,
            length: 1
        }]
    );
    assert_eq!(high_resolution.positive_buckets, vec![5.0]);

    let invalid = exponential(53, 0, vec![1.0]);
    assert_eq!(
        invalid.validate_ingress(),
        Err(ValidationError::InvalidSchema(53))
    );

    let mismatched = FloatHistogram {
        schema: 0,
        positive_spans: vec![Span {
            offset: 0,
            length: 2,
        }],
        positive_buckets: vec![1.0],
        ..FloatHistogram::default()
    };
    assert!(matches!(
        mismatched.validate_ingress(),
        Err(ValidationError::SpanBucketMismatch { .. })
    ));
}

#[test]
fn borrowed_ingress_validation_rejects_nan_zero_threshold() {
    let histogram = FloatHistogram {
        schema: 0,
        zero_threshold: f64::NAN,
        ..FloatHistogram::default()
    };

    assert_eq!(
        HistogramRef::from(&histogram).validate_ingress(),
        Err(ValidationError::NaNZeroThreshold)
    );
    assert_eq!(
        histogram.validate_ingress(),
        Err(ValidationError::NaNZeroThreshold)
    );
}

#[test]
fn borrowed_ingress_validation_resumes_one_entry_at_a_time() {
    let histogram = custom(vec![1.0, 2.0], vec![1.0, 2.0, 3.0]);
    let histogram = HistogramRef::from(&histogram);
    let mut validation = IngressValidation::new();
    let mut inspected = 0;
    let mut polls = 0;

    loop {
        let (complete, used) = validation.validate_chunk(histogram, 1).unwrap();
        assert!(used <= 1);
        inspected += used;
        polls += 1;
        if complete {
            break;
        }
    }

    assert_eq!(inspected, 6);
    assert!(polls >= inspected);
}

#[test]
fn ingress_indices_and_bounds_use_wide_arithmetic() {
    let accepted = exponential(-4, i32::MAX, vec![1.0]);
    accepted.validate_ingress().unwrap();
    let bucket = accepted.json_buckets().next().unwrap();
    assert_eq!((bucket.lower, bucket.upper), (f64::INFINITY, f64::INFINITY));
    let mut combined = accepted.clone();
    combined.add(&accepted).unwrap();
    assert_eq!(combined.positive_buckets, vec![2.0]);

    let unrepresentable = exponential(-4, i32::MAX, vec![1.0, 1.0]);
    assert_eq!(
        unrepresentable.validate_ingress(),
        Err(ValidationError::UnrepresentableSpanIndex { span: 0 })
    );
}

#[test]
fn add_and_sub_reduce_to_the_lower_schema() {
    let mut value = exponential(1, -1, vec![2.0, 3.0]);
    value.zero_count = 4.0;
    value.count += 4.0;
    let mut other = exponential(0, 0, vec![7.0]);
    other.zero_count = 5.0;
    other.count += 5.0;

    value.add(&other).unwrap();
    assert_eq!(value.schema, 0);
    assert_eq!(value.positive_buckets, vec![12.0]);
    assert_eq!(value.zero_count, 9.0);

    value.sub(&other).unwrap();
    assert_eq!(value.positive_buckets, vec![5.0]);
    assert_eq!(value.zero_count, 4.0);
    assert_eq!(value.count, 9.0);
}

#[test]
fn custom_bounds_are_reconciled_to_the_intersection() {
    let mut value = custom(vec![1.0, 2.0, 4.0], vec![1.0, 2.0, 3.0, 4.0]);
    let other = custom(vec![1.0, 3.0, 4.0], vec![10.0, 20.0, 30.0, 40.0]);

    value.add(&other).unwrap();
    assert_eq!(value.custom_values, vec![1.0, 4.0]);
    assert_eq!(value.positive_buckets, vec![11.0, 55.0, 44.0]);

    let mut exponential = exponential(0, 0, vec![1.0]);
    assert_eq!(
        exponential.add(&value),
        Err(HistogramError::IncompatibleSchemas)
    );
}

#[test]
fn mul_and_div_match_prometheus_ieee_and_reset_hint_semantics() {
    let original = FloatHistogram {
        counter_reset_hint: CounterResetHint::Reset,
        schema: 0,
        count: 2.0,
        sum: -4.0,
        zero_count: 1.0,
        positive_spans: vec![Span {
            offset: 0,
            length: 2,
        }],
        positive_buckets: vec![0.0, 2.0],
        ..FloatHistogram::default()
    };

    let mut negative = original.clone();
    negative.mul(-2.0);
    assert_eq!(negative.counter_reset_hint, CounterResetHint::Gauge);
    assert_eq!(negative.positive_buckets, vec![-0.0, -4.0]);

    let mut negative_zero = original.clone();
    negative_zero.mul(-0.0);
    assert_eq!(negative_zero.counter_reset_hint, CounterResetHint::Reset);

    let mut nan = original.clone();
    nan.mul(f64::NAN);
    assert!(nan.count.is_nan());
    assert!(nan.positive_buckets.iter().all(|bucket| bucket.is_nan()));
    assert_eq!(nan.counter_reset_hint, CounterResetHint::Reset);

    let mut zero = original.clone();
    zero.div(0.0);
    assert!(zero.count.is_infinite() && zero.count.is_sign_positive());
    assert!(zero.sum.is_infinite() && zero.sum.is_sign_negative());
    assert!(zero.positive_spans.is_empty());
    assert!(zero.positive_buckets.is_empty());
    assert_eq!(zero.counter_reset_hint, CounterResetHint::Reset);

    let mut negative_divisor = original;
    negative_divisor.div(-2.0);
    assert_eq!(negative_divisor.counter_reset_hint, CounterResetHint::Gauge);
    assert_eq!(negative_divisor.positive_buckets, vec![-0.0, -1.0]);
}

#[test]
fn detects_bucket_resets_and_honors_reset_hints() {
    let previous = exponential(0, 0, vec![5.0]);
    let mut current = exponential(0, 0, vec![4.0]);
    current.count = previous.count;
    assert!(current.detect_reset(&previous));

    current.counter_reset_hint = CounterResetHint::NotReset;
    assert!(!current.detect_reset(&previous));
    current.counter_reset_hint = CounterResetHint::Reset;
    assert!(current.detect_reset(&previous));

    let previous = custom(vec![1.0, 2.0], vec![2.0, 3.0, 4.0]);
    let mut current = custom(vec![1.0, 3.0], vec![3.0, 4.0, 2.0]);
    current.count = previous.count;
    assert!(current.detect_reset(&previous));
}

#[test]
fn kahan_sum_applies_the_final_compensation() {
    let histogram = |sum| FloatHistogram {
        schema: 0,
        sum,
        ..FloatHistogram::default()
    };
    let mut sum = KahanSum::new(histogram(1e16));
    sum.add(&histogram(1.0)).unwrap();
    sum.add(&histogram(-1e16)).unwrap();
    assert_eq!(sum.finish().unwrap().sum, 1.0);
}

#[test]
fn kahan_zero_reconciliation_preserves_the_other_compensation() {
    let first = FloatHistogram {
        schema: 0,
        zero_threshold: 2.0,
        ..FloatHistogram::default()
    };
    let other = FloatHistogram {
        schema: 0,
        count: 1e16 + 2.0,
        zero_count: 1e16,
        positive_spans: vec![Span {
            offset: 0,
            length: 2,
        }],
        positive_buckets: vec![1.0, 1.0],
        ..FloatHistogram::default()
    };

    let mut sum = KahanSum::new(first);
    sum.add(&other).unwrap();
    let result = sum.finish().unwrap();
    assert_eq!(result.zero_count, 1e16 + 2.0);
    assert!(result.positive_buckets.is_empty());
}

#[test]
#[allow(clippy::approx_constant)]
fn json_buckets_match_prometheus_order_codes_and_bounds() {
    let histogram = FloatHistogram {
        schema: 2,
        zero_threshold: 0.001,
        zero_count: 12.0,
        negative_spans: vec![Span {
            offset: 2,
            length: 2,
        }],
        negative_buckets: vec![2.0, 1.0],
        positive_spans: vec![Span {
            offset: 3,
            length: 2,
        }],
        positive_buckets: vec![1.0, 0.0],
        ..FloatHistogram::default()
    };

    let buckets: Vec<_> = histogram.json_buckets().collect();
    assert_eq!(buckets.len(), 4);
    assert_eq!(buckets[0].boundaries, 1);
    assert_eq!(buckets[0].lower, -1.6817928305074288);
    assert_eq!(buckets[0].upper, -1.414213562373095);
    assert_eq!(buckets[1].boundaries, 1);
    assert_eq!(buckets[2].boundaries, 3);
    assert_eq!((buckets[2].lower, buckets[2].upper), (-0.001, 0.001));
    assert_eq!(buckets[3].boundaries, 0);
    assert_eq!(buckets[3].lower, 1.414213562373095);
    assert_eq!(buckets[3].upper, 1.6817928305074288);
}
