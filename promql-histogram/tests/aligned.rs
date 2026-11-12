//! Two histograms of one bucket layout are combined bucket by bucket,
//! without the reconciliation that finds the pairs when layouts differ.
//! These hold that shortcut to the general path it stands in for.
//!
//! The general path is reached with the same histograms by spelling the
//! second one's spans differently: a span cut in two with a zero offset
//! between covers the same buckets and compares unequal as a layout.
//! Compacted, which merges such spans, the two results must be equal.

use promql_histogram::{CounterResetHint, FloatHistogram, KahanSum, Span};

/// Positive buckets at indices 1..=6 and negative ones at 2..=4, all
/// above a zero threshold of 0.5 at schema 0.
fn histogram(seed: u64, hint: CounterResetHint) -> FloatHistogram {
    let mut state = seed;
    let mut next = move || {
        // xorshift: values with enough bits to make rounding visible.
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        (state % 1_000_000) as f64 / 7.0 + 0.1
    };
    let positive: Vec<f64> = (0..6).map(|_| next()).collect();
    let negative: Vec<f64> = (0..3).map(|_| next()).collect();
    let zero = next();
    FloatHistogram {
        counter_reset_hint: hint,
        schema: 0,
        count: positive.iter().sum::<f64>() + negative.iter().sum::<f64>() + zero,
        sum: next(),
        zero_threshold: 0.5,
        zero_count: zero,
        positive_spans: vec![
            Span {
                offset: 1,
                length: 4,
            },
            Span {
                offset: 0,
                length: 0,
            },
            Span {
                offset: 0,
                length: 2,
            },
        ],
        positive_buckets: positive,
        negative_spans: vec![Span {
            offset: 2,
            length: 3,
        }],
        negative_buckets: negative,
        custom_values: Vec::new(),
    }
}

/// The same histogram with other spans over the same buckets.
fn respelled(h: &FloatHistogram) -> FloatHistogram {
    FloatHistogram {
        positive_spans: vec![
            Span {
                offset: 1,
                length: 1,
            },
            Span {
                offset: 0,
                length: 5,
            },
        ],
        negative_spans: vec![
            Span {
                offset: 2,
                length: 2,
            },
            Span {
                offset: 0,
                length: 1,
            },
        ],
        ..h.clone()
    }
}

fn assert_same(aligned: &FloatHistogram, general: &FloatHistogram, what: &str) {
    let (mut aligned, mut general) = (aligned.clone(), general.clone());
    aligned.compact();
    general.compact();
    assert!(
        aligned.equals(&general),
        "{what}: {aligned:?} != {general:?}"
    );
    assert_eq!(
        aligned.counter_reset_hint, general.counter_reset_hint,
        "{what}"
    );
}

#[test]
fn addition_and_subtraction_match_the_general_path() {
    use CounterResetHint::*;
    for seed in 1..40 {
        let a = histogram(seed, NotReset);
        let b = histogram(seed + 1000, Unknown);
        assert_same(&respelled(&b), &b, "the respelling");

        let (mut aligned, mut general) = (a.clone(), a.clone());
        let outcome = aligned.add(&b).unwrap();
        assert_eq!(outcome, general.add(&respelled(&b)).unwrap());
        assert_same(&aligned, &general, "add");

        let (mut aligned, mut general) = (a.clone(), a.clone());
        aligned.sub(&b).unwrap();
        general.sub(&respelled(&b)).unwrap();
        assert_same(&aligned, &general, "sub");
    }
}

#[test]
fn a_kahan_sum_matches_the_general_path() {
    use CounterResetHint::*;
    for seed in 1..20 {
        let terms: Vec<FloatHistogram> =
            (0..8).map(|i| histogram(seed * 31 + i, Unknown)).collect();
        let mut aligned = KahanSum::new(terms[0].clone());
        let mut general = KahanSum::new(terms[0].clone());
        for term in &terms[1..] {
            aligned.add(term).unwrap();
            general.add(&respelled(term)).unwrap();
        }
        assert_same(
            &aligned.finish().unwrap(),
            &general.finish().unwrap(),
            "kahan sum",
        );
    }
}

#[test]
fn reset_detection_matches_the_general_path() {
    use CounterResetHint::*;
    let base = histogram(7, Unknown);
    let grown = {
        let mut h = base.clone();
        h.add(&histogram(8, Unknown)).unwrap();
        h
    };
    let mut cases = vec![(grown.clone(), base.clone()), (base.clone(), grown.clone())];
    // One bucket lower than before, on either side, and the zero bucket.
    for edit in 0..3 {
        let mut lower = grown.clone();
        match edit {
            0 => lower.positive_buckets[3] = base.positive_buckets[3] / 2.0,
            1 => lower.negative_buckets[1] = base.negative_buckets[1] / 2.0,
            _ => lower.zero_count = base.zero_count / 2.0,
        }
        cases.push((lower, base.clone()));
    }
    for (current, previous) in cases {
        assert_eq!(
            current.detect_reset(&previous),
            current.detect_reset(&respelled(&previous)),
            "{current:?} after {previous:?}"
        );
    }
    assert!(!grown.detect_reset(&base));
    assert!(base.detect_reset(&grown));
}

/// A bucket inside the zero bucket is folded into the zero count when two
/// histograms are reconciled, so such a pair is never combined bucket by
/// bucket, however alike the layouts.
#[test]
fn a_bucket_inside_the_zero_bucket_is_still_reconciled() {
    use CounterResetHint::*;
    let inside = |seed| FloatHistogram {
        // Index 0 at schema 0 is (0.5, 1], below a threshold of 1.
        zero_threshold: 1.0,
        positive_spans: vec![Span {
            offset: 0,
            length: 6,
        }],
        ..histogram(seed, Unknown)
    };
    let (a, b) = (inside(3), inside(4));
    let (mut same, mut respelt) = (a.clone(), a.clone());
    same.add(&b).unwrap();
    respelt.add(&respelled_from_zero(&b)).unwrap();
    assert_same(&same, &respelt, "add across the zero bucket");
    assert_eq!(b.detect_reset(&a), b.detect_reset(&respelled_from_zero(&a)));
}

fn respelled_from_zero(h: &FloatHistogram) -> FloatHistogram {
    FloatHistogram {
        positive_spans: vec![
            Span {
                offset: 0,
                length: 2,
            },
            Span {
                offset: 0,
                length: 4,
            },
        ],
        ..h.clone()
    }
}
