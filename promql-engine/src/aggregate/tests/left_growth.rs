//! Unspecified between-series order permits descending singleton spans. These
//! tests bound actual rebases/storage movement, not elapsed time or allocator RSS.

use super::*;

#[test]
fn descending_sparse_singletons_have_linear_movement_and_preserve_all_states() {
    const N: usize = 2048;
    const BASE: usize = 400_003;
    const STRIDE: usize = 3;
    for op in OPS {
        for partial in [false, true] {
            let mut acc = Grouped::new(op, 0, 999_999, 1).unwrap();
            acc.grow(1).unwrap();
            let mut rebases = 0;
            let mut moved = 0;
            let mut initialized = 0;
            for i in (0..N).rev() {
                let index = BASE + i * STRIDE;
                let mut want = State::new(op);
                for values in INCOMING {
                    want.add(values[i % 4]);
                }
                let before = &acc.spans()[0];
                let old_origin = before.first - before.start;
                let old_len = before.seen.len();
                if partial {
                    let (a, b, n, m) = want.to_row();
                    let state = partial_state(
                        state_fields(),
                        vec![
                            Arc::new(TimestampMillisecondArray::from(vec![index as i64])),
                            Arc::new(Float64Array::from(vec![a])),
                            Arc::new(Float64Array::from(vec![b])),
                            Arc::new(Float64Array::from(vec![n])),
                            Arc::new(BooleanArray::from(vec![m])),
                        ],
                    );
                    merge(&mut acc, &state).unwrap();
                } else {
                    for values in INCOMING {
                        acc.add_series(0, &[index as i64], &[values[i % 4]])
                            .unwrap();
                    }
                }
                let span = &acc.spans()[0];
                if old_len != 0 && old_origin != span.first - span.start {
                    rebases += 1;
                    moved += old_len;
                    initialized += span.seen.len();
                } else if old_len == 0 {
                    initialized += span.seen.len();
                }
                assert_same(op, at(&acc, 0, index).unwrap(), want, index);
                assert_eq!(acc.heap_size(), span.size());
                assert!(span.seen.len() <= 2 * span.len());
            }
            let width = (N - 1) * STRIDE + 1;
            let log = usize::BITS - width.leading_zeros();
            assert!(rebases <= log as usize + 1, "{op:?}: {rebases} rebases");
            assert!(
                moved <= 2 * width,
                "{op:?}: {moved} moved for {width} steps"
            );
            assert!(
                initialized <= 4 * width,
                "{op:?}: {initialized} initialized"
            );
            let span = &acc.spans()[0];
            assert_eq!(span.first, BASE);
            assert_eq!(span.len(), width);
            // Headroom is storage, never an absent step turned into an output.
            assert!(span.start > 0);
            let state: ArrayRef = Arc::new(acc.partial(1).unwrap());
            let sent = state_rows(&state);
            assert_eq!(sent.len(), N);
            for (i, row) in sent.iter().enumerate() {
                let index = BASE + i * STRIDE;
                assert_eq!(row.0, index as i64);
                let mut want = State::new(op);
                for values in INCOMING {
                    want.add(values[i % 4]);
                }
                assert_same(op, at(&acc, 0, index).unwrap(), want, index);
                assert_same(
                    op,
                    State::from_row(op, row.1, row.2, row.3, row.4),
                    want,
                    index,
                );
                if i + 1 < N {
                    assert!(at(&acc, 0, index + 1).is_none());
                    assert!(at(&acc, 0, index + 2).is_none());
                }
            }
            let mut copy = Grouped::new(op, 0, 999_999, 1).unwrap();
            merge(&mut copy, &state).unwrap();
            for i in 0..N {
                let index = BASE + i * STRIDE;
                assert_same(
                    op,
                    at(&copy, 0, index).unwrap(),
                    at(&acc, 0, index).unwrap(),
                    i,
                );
            }
            let output = acc.evaluate(EmitTo::All).unwrap();
            let output = rows(output.as_list::<i32>(), 0);
            assert_eq!(output.len(), N);
            for (got, row) in output.iter().zip(sent) {
                assert_eq!(got.0, row.0);
                let want = State::from_row(op, row.1, row.2, row.3, row.4);
                assert_eq!(got.1.to_bits(), want.result(op).to_bits(), "{op:?}");
            }
            assert_eq!(acc.size(), std::mem::size_of::<Grouped>());
        }
    }
}

#[test]
fn rebasing_copies_signed_zero_nan_payload_compensation_and_mean_flags_verbatim() {
    for op in OPS {
        let payload = f64::from_bits(0x7ff8_0000_0000_4321);
        let state = partial_state(
            state_fields(),
            vec![
                Arc::new(TimestampMillisecondArray::from(vec![57, 59])),
                Arc::new(Float64Array::from(vec![-0.0, payload])),
                Arc::new(Float64Array::from(vec![-0.0, 1e-320])),
                Arc::new(Float64Array::from(vec![2.0, 3.0])),
                Arc::new(BooleanArray::from(vec![false, true])),
            ],
        );
        let mut acc = Grouped::new(op, 0, 63, 1).unwrap();
        merge(&mut acc, &state).unwrap();
        let expected = [at(&acc, 0, 57).unwrap(), at(&acc, 0, 59).unwrap()];
        for index in [56, 55, 52, 48, 40, 24, 0] {
            acc.add_series(0, &[index], &[7.0]).unwrap();
            assert_same(op, at(&acc, 0, 57).unwrap(), expected[0], 57);
            assert_same(op, at(&acc, 0, 59).unwrap(), expected[1], 59);
        }
        let serialized = acc.state(EmitTo::All).unwrap().remove(0);
        let mut restored = Grouped::new(op, 0, 63, 1).unwrap();
        merge(&mut restored, &serialized).unwrap();
        assert_same(op, at(&restored, 0, 57).unwrap(), expected[0], 57);
        assert_same(op, at(&restored, 0, 59).unwrap(), expected[1], 59);
    }
}

#[test]
fn consuming_headroom_then_growing_both_ends_and_releasing_a_prefix_preserves_bits() {
    for op in OPS {
        let mut acc = Grouped::new(op, 0, 63, 1).unwrap();
        acc.grow(3).unwrap();
        let mut expected = [None; 64];
        // Cross byte boundaries, exhaust left headroom, reach grid zero,
        // then widen right. Repeat reached positions without sorting updates.
        for index in [31, 30, 29, 25, 17, 16, 1, 0, 62, 63, 31, 16, 0, 63] {
            for values in INCOMING {
                let value = values[index % 4];
                acc.add_series(1, &[index as i64], &[value]).unwrap();
                expected[index].get_or_insert(State::new(op)).add(value);
            }
        }
        acc.state(EmitTo::First(1)).unwrap();
        assert_eq!(acc.spans()[0].first, 0);
        assert_eq!(acc.spans()[0].start, 0);
        assert_eq!(acc.spans()[0].len(), 64);
        let before: ArrayRef = Arc::new(acc.partial(2).unwrap());
        let before = super::bypass::bits(&before);
        for (index, want) in expected.iter().enumerate() {
            assert_eq!(at(&acc, 0, index).is_some(), want.is_some());
            if let Some(want) = want {
                assert_same(op, at(&acc, 0, index).unwrap(), *want, index);
            }
        }
        let emitted = acc.state(EmitTo::First(1)).unwrap().remove(0);
        assert_eq!(super::bypass::bits(&emitted), before);
        assert_eq!(
            acc.heap_size(),
            acc.spans().iter().map(Span::size).sum::<usize>()
        );
        assert!(samples(&acc, 0).is_empty());
        acc.state(EmitTo::All).unwrap();
        assert_eq!(acc.size(), std::mem::size_of::<Grouped>());
    }
}
