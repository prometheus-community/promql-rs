use promql_histogram::{
    ArithmeticOutcome, CounterResetHint, FloatHistogram, HistogramError, KahanSum, Span,
    CUSTOM_SCHEMA,
};

fn combine(
    left: &mut FloatHistogram,
    right: &FloatHistogram,
    operation: &str,
) -> Result<ArithmeticOutcome, HistogramError> {
    match operation {
        "add" => left.add(right),
        "sub" => left.sub(right),
        "kahan_add" => {
            let mut sum = KahanSum::new(left.clone());
            let outcome = sum.add(right)?;
            *left = sum.into_parts().0;
            Ok(outcome)
        }
        _ => unreachable!(),
    }
}

#[test]
fn adjust_counter_reset_reports_only_reset_vs_no_reset_collisions() {
    use CounterResetHint::*;
    for operation in ["add", "sub", "kahan_add"] {
        for left_hint in [Unknown, Reset, NotReset, Gauge] {
            for right_hint in [Unknown, Reset, NotReset, Gauge] {
                let mut left = FloatHistogram {
                    counter_reset_hint: left_hint,
                    ..Default::default()
                };
                let right = FloatHistogram {
                    counter_reset_hint: right_hint,
                    ..Default::default()
                };
                let outcome = combine(&mut left, &right, operation).unwrap();
                assert_eq!(
                    outcome.counter_reset_collision,
                    matches!(
                        (left_hint, right_hint),
                        (Reset, NotReset) | (NotReset, Reset)
                    )
                );
                assert!(!outcome.custom_bounds_reconciled);
                let expected = if left_hint == right_hint {
                    left_hint
                } else if left_hint == Gauge || right_hint == Gauge {
                    Gauge
                } else {
                    Unknown
                };
                assert_eq!(left.counter_reset_hint, expected);
                assert_eq!(right.counter_reset_hint, right_hint);
            }
        }
    }
}

fn custom(bounds: Vec<f64>) -> FloatHistogram {
    FloatHistogram {
        schema: CUSTOM_SCHEMA,
        count: 1.0,
        custom_values: bounds,
        positive_spans: vec![Span {
            offset: 0,
            length: 1,
        }],
        positive_buckets: vec![1.0],
        ..Default::default()
    }
}

#[test]
fn reconciliation_is_reported_even_when_the_counts_do_not_change() {
    for operation in ["add", "sub", "kahan_add"] {
        let original = custom(vec![1.0, 2.0]);
        for bounds in [vec![1.0, 2.0], vec![2.0, 3.0], vec![3.0, 4.0]] {
            let mut left = original.clone();
            let right = custom(bounds);
            let before = right.clone();
            let outcome = combine(&mut left, &right, operation).unwrap();
            assert_eq!(
                outcome.custom_bounds_reconciled,
                original.custom_values != right.custom_values
            );
            assert!(!outcome.counter_reset_collision);
            assert_eq!(right, before);
            assert_eq!(
                left.custom_values,
                original
                    .custom_values
                    .iter()
                    .copied()
                    .filter(|v| right.custom_values.contains(v))
                    .collect::<Vec<_>>()
            );
        }
    }
}

#[test]
fn incompatible_schemas_do_not_mutate_the_receiver() {
    for operation in ["add", "sub", "kahan_add"] {
        let mut left = custom(vec![1.0]);
        left.counter_reset_hint = CounterResetHint::Reset;
        let before = left.clone();
        let right = FloatHistogram {
            counter_reset_hint: CounterResetHint::Gauge,
            ..Default::default()
        };
        assert_eq!(
            combine(&mut left, &right, operation),
            Err(HistogramError::IncompatibleSchemas)
        );
        assert_eq!(left, before);
        assert_eq!(left.counter_reset_hint, before.counter_reset_hint);
    }
}

#[test]
fn merging_partial_sums_keeps_findings_from_both_components() {
    let mut first = custom(vec![1.0, 2.0]);
    first.counter_reset_hint = CounterResetHint::Reset;
    let mut next = custom(vec![2.0, 3.0]);
    next.counter_reset_hint = CounterResetHint::NotReset;
    for (value, compensation) in [(&next, &first), (&first, &next)] {
        let mut sum = KahanSum::new(first.clone());
        let outcome = sum.add_parts(value, compensation).unwrap();
        assert!(outcome.counter_reset_collision);
        assert!(outcome.custom_bounds_reconciled);
    }
}
