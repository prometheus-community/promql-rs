//! Regressions against Prometheus at the revision recorded in UPSTREAM.md.

use promql_histogram::{statistics, FloatHistogram, Span, TrimDirection, CUSTOM_SCHEMA};

fn single_bucket(schema: i32, index: i32) -> FloatHistogram {
    FloatHistogram {
        schema,
        count: 1.0,
        positive_spans: vec![Span {
            offset: index,
            length: 1,
        }],
        positive_buckets: vec![1.0],
        ..Default::default()
    }
}

#[test]
fn get_bound_preserves_finite_buckets_at_the_top_of_each_exponential_schema() {
    for schema in 0..=8 {
        let histogram = single_bucket(schema, (1024 << schema) - 1);
        let bucket = histogram.positive_buckets_iter().next().unwrap();
        assert!(bucket.lower.is_finite(), "schema {schema}: {bucket:?}");
        assert!(bucket.upper.is_finite(), "schema {schema}: {bucket:?}");
        assert!(bucket.upper > bucket.lower);
        // getBound deliberately separates the last finite bound from +Inf.
        let histogram = single_bucket(schema, 1024 << schema);
        assert_eq!(
            histogram.positive_buckets_iter().next().unwrap().upper,
            f64::MAX
        );
    }
    let bucket = single_bucket(1, 2047)
        .positive_buckets_iter()
        .next()
        .unwrap();
    assert_eq!(bucket.upper.to_bits(), 0x7fe6_a09e_667f_3bcc);
}

#[test]
fn get_bound_keeps_subnormals_instead_of_overflowing_an_intermediate_power() {
    for schema in -4..=8 {
        let index = if schema < 0 {
            -1072 >> -schema
        } else {
            -1072 << schema
        };
        let histogram = single_bucket(schema, index);
        let bucket = histogram.positive_buckets_iter().next().unwrap();
        assert_eq!(bucket.upper.to_bits(), 4, "schema {schema}");
    }
    let histogram = single_bucket(0, -1074);
    let bucket = histogram.positive_buckets_iter().next().unwrap();
    assert_eq!(bucket.lower.to_bits(), 0);
    assert_eq!(bucket.upper.to_bits(), 1);
}

#[test]
fn histogram_fraction_preserves_go_multiply_then_divide_order() {
    let histogram = FloatHistogram {
        schema: CUSTOM_SCHEMA,
        count: 3.0,
        positive_spans: vec![Span {
            offset: 1,
            length: 1,
        }],
        positive_buckets: vec![3.0],
        custom_values: vec![1.0, 8.0],
        ..Default::default()
    };
    // HistogramFraction: (3 * (4 - 1) / (8 - 1)) / 3.
    assert_eq!(
        statistics::fraction(1.0, 4.0, &histogram).value.to_bits(),
        0x3fdb_6db6_db6d_b6dc
    );
}

#[test]
fn trim_buckets_does_not_mutate_a_shared_input() {
    let histogram = single_bucket(0, 2);
    let mut upper = histogram.trim_buckets(3.0, TrimDirection::Upper);
    let lower = histogram.trim_buckets(3.0, TrimDirection::Lower);
    assert_eq!(histogram.positive_buckets, [1.0]);
    assert_eq!(upper.count + lower.count, histogram.count);
    upper.positive_buckets[0] = 42.0;
    assert_eq!(histogram.positive_buckets, [1.0]);
    assert!(lower.positive_buckets[0] < 1.0);
}
