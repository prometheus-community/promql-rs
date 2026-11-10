use promql_histogram::{FloatHistogram, Span};

#[test]
fn compact_rebuilds_nonzero_runs_without_reallocating_bucket_storage() {
    let nan = f64::from_bits(0x7ff8_0000_0000_0042);
    let mut histogram = FloatHistogram {
        count: 23.0,
        sum: 42.0,
        positive_spans: vec![
            Span {
                offset: -2,
                length: 6,
            },
            Span {
                offset: 0,
                length: 0,
            },
            Span {
                offset: 0,
                length: 3,
            },
        ],
        positive_buckets: vec![0.0, -0.0, 2.0, 0.0, 3.0, 4.0, 0.0, nan, 5.0],
        ..Default::default()
    };
    histogram.negative_spans = histogram.positive_spans.clone();
    histogram.negative_buckets = histogram.positive_buckets.clone();
    let positive = histogram.positive_buckets.as_ptr();
    let negative = histogram.negative_buckets.as_ptr();
    let capacity = histogram.positive_buckets.capacity();
    histogram.compact();
    assert_eq!(
        histogram.positive_spans,
        vec![
            Span {
                offset: 0,
                length: 1
            },
            Span {
                offset: 1,
                length: 2
            },
            Span {
                offset: 1,
                length: 2
            },
        ]
    );
    assert_eq!(histogram.negative_spans, histogram.positive_spans);
    let expected: Vec<_> = [2.0, 3.0, 4.0, nan, 5.0].map(f64::to_bits).into();
    for buckets in [&histogram.positive_buckets, &histogram.negative_buckets] {
        assert_eq!(
            buckets.iter().map(|v| v.to_bits()).collect::<Vec<_>>(),
            expected
        );
    }
    assert_eq!(histogram.positive_buckets.as_ptr(), positive);
    assert_eq!(histogram.negative_buckets.as_ptr(), negative);
    assert_eq!(histogram.positive_buckets.capacity(), capacity);
    assert_eq!((histogram.count, histogram.sum), (23.0, 42.0));
    let before = histogram.clone();
    histogram.compact();
    assert_eq!(histogram, before);
}

#[test]
fn compact_retains_empty_bucket_capacity_and_removes_empty_spans() {
    let mut histogram = FloatHistogram {
        positive_spans: vec![Span {
            offset: 3,
            length: 4,
        }],
        positive_buckets: vec![0.0; 4],
        ..Default::default()
    };
    let pointer = histogram.positive_buckets.as_ptr();
    histogram.compact();
    assert!(histogram.positive_spans.is_empty());
    assert!(histogram.positive_buckets.is_empty());
    assert_eq!(histogram.positive_buckets.capacity(), 4);
    assert_eq!(histogram.positive_buckets.as_ptr(), pointer);
    histogram.compact();
    assert!(histogram.positive_spans.is_empty());
}

#[test]
fn compact_can_encode_a_gap_wider_than_i32() {
    let spans = vec![
        Span {
            offset: i32::MIN,
            length: 1,
        },
        Span {
            offset: i32::MAX,
            length: 0,
        },
        Span {
            offset: i32::MAX,
            length: 1,
        },
    ];
    let mut histogram = FloatHistogram {
        positive_spans: spans.clone(),
        positive_buckets: vec![1.0, 2.0],
        ..Default::default()
    };
    histogram.validate_ingress().unwrap();
    histogram.compact();
    assert_eq!(histogram.positive_spans, spans);
    assert_eq!(histogram.positive_buckets, [1.0, 2.0]);
}
