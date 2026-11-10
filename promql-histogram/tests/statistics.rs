// Expected values are selected from Prometheus's pinned promql testdata and quantile tests.

use promql_histogram::statistics::{
    average, classic_fraction, classic_quantile, coalesce_classic_buckets, fraction, quantile,
    repair_classic_buckets, stddev, variance, ClassicBucket,
};
use promql_histogram::{FloatHistogram, Span, CUSTOM_SCHEMA};

fn exponential(schema: i32, buckets: Vec<f64>) -> FloatHistogram {
    FloatHistogram {
        schema,
        count: buckets.iter().sum(),
        positive_spans: vec![Span {
            offset: 0,
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

fn assert_close(actual: f64, expected: f64) {
    let tolerance = 1e-14 * expected.abs().max(1.0);
    assert!(
        (actual - expected).abs() <= tolerance,
        "expected {expected}, got {actual}"
    );
}

#[test]
#[allow(clippy::approx_constant)]
fn exponential_quantile_and_fraction_use_prometheus_interpolation() {
    let mut histogram = exponential(0, vec![1.0, 2.0, 1.0]);
    histogram.sum = 5.0;

    assert_eq!(average(&histogram), 1.25);
    assert_close(quantile(0.5, &histogram).value, 1.414213562373095);
    assert_eq!(fraction(1.0, 2.0, &histogram).value, 0.5);
    assert_eq!(quantile(-0.1, &histogram).value, f64::NEG_INFINITY);
    assert_eq!(quantile(1.1, &histogram).value, f64::INFINITY);
    assert!(quantile(f64::NAN, &histogram).value.is_nan());

    let empty = FloatHistogram::default();
    assert!(quantile(0.5, &empty).value.is_nan());
    assert!(fraction(0.0, 1.0, &empty).value.is_nan());
}

#[test]
fn zero_bucket_uses_natural_bounds_and_linear_interpolation() {
    let mut histogram = exponential(0, vec![2.0, 3.0, 0.0, 1.0, 4.0]);
    histogram.count = 12.0;
    histogram.sum = 100.0;
    histogram.zero_count = 2.0;
    histogram.zero_threshold = 0.001;

    assert_eq!(quantile(0.0, &histogram).value, 0.0);
    assert_close(quantile(0.1, &histogram).value, 0.0006);
    assert_close(fraction(0.0, 0.0005, &histogram).value, 1.0 / 12.0);

    histogram.positive_spans.clear();
    histogram.positive_buckets.clear();
    histogram.negative_spans = vec![Span {
        offset: 0,
        length: 5,
    }];
    histogram.negative_buckets = vec![2.0, 3.0, 0.0, 1.0, 4.0];
    assert_eq!(quantile(1.0, &histogram).value, 0.0);
    assert_close(quantile(0.9, &histogram).value, -0.0006);
}

#[test]
fn custom_quantile_and_fraction_handle_infinite_endpoints_and_boundaries() {
    let mut histogram = custom(vec![5.0, 10.0], vec![1.0, 2.0, 1.0]);
    histogram.sum = 5.0;

    assert_eq!(quantile(0.0, &histogram).value, 0.0);
    assert_eq!(quantile(0.5, &histogram).value, 7.5);
    assert_eq!(quantile(1.0, &histogram).value, 10.0);
    assert_eq!(fraction(5.0, 10.0, &histogram).value, 0.5);
    assert_eq!(
        fraction(f64::NEG_INFINITY, f64::INFINITY, &histogram).value,
        1.0
    );

    let negative_endpoint = custom(vec![-5.0, 5.0], vec![1.0, 2.0, 1.0]);
    assert_eq!(quantile(0.125, &negative_endpoint).value, -5.0);
    assert_eq!(quantile(0.875, &negative_endpoint).value, 5.0);
}

#[test]
fn nan_observations_set_only_the_minimal_result_flags() {
    let mut histogram = exponential(0, vec![12.0]);
    histogram.count = 15.0;
    histogram.sum = f64::NAN;

    let skewed = quantile(0.4, &histogram);
    assert_close(skewed.value, 0.7071067811865475);
    assert!(skewed.nan_skew);
    assert!(!skewed.nan_result);

    let missing = quantile(0.81, &histogram);
    assert!(missing.value.is_nan());
    assert!(!missing.nan_skew);
    assert!(missing.nan_result);

    let all = fraction(f64::NEG_INFINITY, f64::INFINITY, &histogram);
    assert_eq!(all.value, 0.8);
    assert!(all.nan_observations);
}

#[test]
fn variance_uses_actual_mean_and_prometheus_bucket_representatives() {
    let mut low_resolution = exponential(2, vec![1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 1.0, 1.0]);
    low_resolution.sum = 10.0;
    assert_eq!(variance(&low_resolution), 1.163807968526718);
    assert_eq!(stddev(&low_resolution), 1.0787993180043811);

    let signed = FloatHistogram {
        schema: 0,
        count: 3.0,
        zero_count: 1.0,
        negative_spans: vec![Span {
            offset: 1,
            length: 1,
        }],
        negative_buckets: vec![1.0],
        positive_spans: vec![Span {
            offset: 1,
            length: 1,
        }],
        positive_buckets: vec![1.0],
        ..FloatHistogram::default()
    };
    assert_close(variance(&signed), 4.0 / 3.0);

    let mut custom = custom(vec![0.0, 2.0, 4.0], vec![0.0, 1.0, 1.0, 0.0]);
    custom.sum = 4.0;
    assert_eq!(variance(&custom), 1.0);
    assert_eq!(stddev(&custom), 1.0);
}

#[test]
fn classic_helpers_coalesce_and_repair_cumulative_buckets() {
    let mut duplicate = [
        ClassicBucket {
            upper_bound: 1.0,
            count: 2.0,
        },
        ClassicBucket {
            upper_bound: 1.0,
            count: 3.0,
        },
        ClassicBucket {
            upper_bound: 2.0,
            count: 4.0,
        },
    ];
    let coalesced = coalesce_classic_buckets(&mut duplicate);
    assert_eq!(coalesced.len(), 2);
    assert_eq!(coalesced[0].count, 5.0);
    assert!(repair_classic_buckets(coalesced));
    assert_eq!(coalesced[1].count, 5.0);

    let mut imprecise = [
        ClassicBucket {
            upper_bound: 10.0,
            count: 10.0,
        },
        ClassicBucket {
            upper_bound: 15.0,
            count: 15.0,
        },
        ClassicBucket {
            upper_bound: 20.0,
            count: 15.00000000001,
        },
        ClassicBucket {
            upper_bound: 30.0,
            count: 15.0,
        },
        ClassicBucket {
            upper_bound: f64::INFINITY,
            count: 15.0,
        },
    ];
    let result = classic_quantile(0.5, &mut imprecise);
    assert_eq!(result.value, 7.5);
    assert!(!result.forced_monotonicity);

    let mut forced = [
        ClassicBucket {
            upper_bound: 10.0,
            count: 10.0,
        },
        ClassicBucket {
            upper_bound: 15.0,
            count: 9.0,
        },
        ClassicBucket {
            upper_bound: f64::INFINITY,
            count: 15.0,
        },
    ];
    assert!(classic_quantile(0.5, &mut forced).forced_monotonicity);

    let mut fractional = [
        ClassicBucket {
            upper_bound: 0.5,
            count: 2.5,
        },
        ClassicBucket {
            upper_bound: 1.0,
            count: 7.5,
        },
        ClassicBucket {
            upper_bound: f64::INFINITY,
            count: 100.0,
        },
    ];
    assert_eq!(classic_fraction(0.1, 0.75, &mut fractional), 0.045);
}

#[test]
fn all_bucket_traversal_is_symmetric_across_multiple_spans() {
    let histogram = FloatHistogram {
        schema: 0,
        zero_threshold: 0.25,
        zero_count: 3.0,
        negative_spans: vec![
            Span {
                offset: -1,
                length: 1,
            },
            Span {
                offset: 2,
                length: 1,
            },
        ],
        negative_buckets: vec![1.0, 2.0],
        positive_spans: vec![
            Span {
                offset: -1,
                length: 1,
            },
            Span {
                offset: 2,
                length: 1,
            },
        ],
        positive_buckets: vec![4.0, 5.0],
        ..FloatHistogram::default()
    };

    let forward: Vec<_> = histogram.all_buckets().collect();
    let reverse: Vec<_> = histogram.all_buckets_rev().collect();
    assert_eq!(forward, reverse.into_iter().rev().collect::<Vec<_>>());
    assert_eq!(
        forward
            .iter()
            .map(|bucket| bucket.count)
            .collect::<Vec<_>>(),
        vec![2.0, 1.0, 3.0, 4.0, 5.0]
    );
}
