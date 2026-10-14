//! Upper-state regressions exercise native timestamp-bearing states,
//! independently of the raw-selector fold lifecycle.

use super::*;

#[test]
fn single_step_updates_and_emission_match_every_scalar_operator_bit_for_bit() {
    let values = [
        [1e16, 1.0, -1e16, 1e-320],
        [f64::MAX, f64::MAX, 1.0, 0.5],
        [-0.0, -0.0, -0.0, -0.0],
        [
            f64::from_bits(0x7ff8_0000_0000_1234),
            7.0,
            f64::INFINITY,
            -3.5,
        ],
    ];
    for op in OPS {
        let mut acc = Grouped::new(op, 41, 49, 10).unwrap();
        assert_eq!(acc.grid.len, 1);
        assert!(matches!(acc.storage, Storage::SingleStep(_)));
        let mut expected = [State::new(op); 4];
        for i in 0..4 {
            let input =
                samples_column(&values.iter().map(|v| vec![(41, v[i])]).collect::<Vec<_>>());
            acc.update_batch(&[input], &[0, 1, 2, 3], None, 8).unwrap();
            for (state, v) in expected.iter_mut().zip(values) {
                state.add(v[i]);
            }
        }
        let input = samples_column(&[(); 3].map(|_| vec![(41, 99.0)]));
        let list = input.as_list::<i32>();
        let null_row: ArrayRef = Arc::new(ListArray::new(
            series::sample_item(),
            list.offsets().clone(),
            list.values().clone(),
            Some(datafusion::arrow::buffer::NullBuffer::from(vec![
                true, true, false,
            ])),
        ));
        acc.update_batch(
            &[null_row],
            &[0, 4, 5],
            Some(&BooleanArray::from(vec![Some(false), None, Some(true)])),
            8,
        )
        .unwrap();
        acc.update_batch(&[samples_column(&[Vec::new()])], &[6], None, 8)
            .unwrap();
        for (g, want) in expected.iter().enumerate() {
            assert_same(op, at(&acc, g, 0).unwrap(), *want, g);
        }
        let state = acc.partial(8).unwrap();
        assert_eq!(state.data_type(), &state_type());
        let state: ArrayRef = Arc::new(state);
        for (g, row) in state_rows(&state).iter().enumerate() {
            assert_eq!(row.0, 41);
            assert_same(
                op,
                State::from_row(op, row.1, row.2, row.3, row.4),
                expected[g],
                g,
            );
        }
        let output = acc.evaluate(EmitTo::All).unwrap();
        let list = output.as_list::<i32>();
        assert_eq!(list.offsets().as_ref(), &[0, 1, 2, 3, 4, 4, 4, 4, 4]);
        assert_eq!(list.data_type(), &series::samples_type());
        for (g, want) in expected.iter().enumerate() {
            let got = rows(list, g);
            assert_eq!(got[0].0, 41);
            assert_eq!(
                got[0].1.to_bits(),
                want.result(op).to_bits(),
                "{op:?} group {g}"
            );
        }
    }
}

#[test]
fn single_step_partial_copy_merge_and_unaligned_prefix_rebase_preserve_bits() {
    for op in OPS {
        let mut source = Grouped::new(op, 7, 7, 3).unwrap();
        source
            .update_batch(&[samples_column(&[vec![(7, -0.0)]])], &[0], None, 5)
            .unwrap();
        let mut dest = Grouped::new(op, 7, 7, 3).unwrap();
        let mut sent = [State::new(op); 4];
        let mut resident = [State::new(op); 4];
        for values in INCOMING {
            source
                .update_batch(
                    &[samples_column(&values.map(|v| vec![(7, v)]))],
                    &[1, 2, 3, 4],
                    None,
                    5,
                )
                .unwrap();
            for (state, v) in sent.iter_mut().zip(values) {
                state.add(v);
            }
        }
        for values in RESIDENT {
            dest.update_batch(
                &[samples_column(&values.map(|v| vec![(7, v)]))],
                &[12, 14],
                None,
                17,
            )
            .unwrap();
            resident[1].add(values[0]);
            resident[3].add(values[1]);
        }
        // Eleven unseen keys leave first: flags cross bytes at an unaligned
        // cut, and the sliced partial keeps a nonzero sample-child offset.
        let empty = dest.state(EmitTo::First(11)).unwrap().remove(0);
        assert_eq!(empty.as_list::<i32>().offsets().as_ref(), &[0; 12]);
        let state = source.state(EmitTo::All).unwrap().remove(0).slice(1, 4);
        assert_eq!(state.as_list::<i32>().offsets()[0], 1);
        dest.merge_batch(&[state], &[0, 1, 2, 3], 6).unwrap();
        let mut expected = sent;
        for i in [1, 3] {
            resident[i].merge(&sent[i]);
            expected[i] = resident[i];
        }
        for (g, want) in expected.iter().enumerate() {
            assert_same(op, at(&dest, g, 0).unwrap(), *want, g);
        }
        assert!(at(&dest, 4, 0).is_none());
        assert!(at(&dest, 5, 0).is_none());
        let prefix = dest.evaluate(EmitTo::First(1)).unwrap();
        assert_eq!(
            rows(prefix.as_list::<i32>(), 0)[0].1.to_bits(),
            expected[0].result(op).to_bits()
        );
        dest.update_batch(&[samples_column(&[vec![(7, -0.0)]])], &[2], None, 5)
            .unwrap();
        expected[3].add(-0.0);
        for (g, want) in expected[1..].iter().enumerate() {
            assert_same(op, at(&dest, g, 0).unwrap(), *want, g);
        }
        let output = dest.state(EmitTo::All).unwrap().remove(0);
        assert_eq!(
            output.as_list::<i32>().offsets().as_ref(),
            &[0, 1, 2, 3, 3, 3]
        );
        for (g, row) in state_rows(&output).iter().enumerate() {
            assert_same(
                op,
                State::from_row(op, row.1, row.2, row.3, row.4),
                expected[g + 1],
                g,
            );
        }
    }
}

#[test]
fn single_step_fresh_partials_copy_nan_payload_zero_and_compensation_verbatim() {
    for op in OPS {
        for (a, b, n, m) in [
            (-0.0, -0.0, 2.0, false),
            (f64::from_bits(0x7ff8_0000_0000_4321), 1e-320, 3.0, true),
            (f64::MAX, 1.0, 4.0, true),
        ] {
            let state = partial_state(
                state_fields(),
                vec![
                    Arc::new(TimestampMillisecondArray::from(vec![7])),
                    Arc::new(Float64Array::from(vec![a])),
                    Arc::new(Float64Array::from(vec![b])),
                    Arc::new(Float64Array::from(vec![n])),
                    Arc::new(BooleanArray::from(vec![m])),
                ],
            );
            let mut acc = Grouped::new(op, 7, 7, 1).unwrap();
            merge(&mut acc, &state).unwrap();
            assert_same(
                op,
                at(&acc, 0, 0).unwrap(),
                State::from_row(op, a, b, n, m),
                0,
            );
        }
    }
}

#[test]
fn single_step_capacity_accounting_is_exact_after_growth_drains_errors_and_reset() {
    for op in OPS {
        let mut acc = Grouped::new(op, 0, 0, 1).unwrap();
        let initial = acc.size();
        let check = |acc: &Grouped| {
            let Storage::SingleStep(flat) = &acc.storage else {
                panic!("flat layout")
            };
            let floats = &flat.lanes.floats;
            let heap = flat.seen.capacity() / 8
                + floats.capacity() * std::mem::size_of::<Vec<f64>>()
                + floats.iter().map(|v| v.capacity() * 8).sum::<usize>()
                + flat
                    .lanes
                    .incremental
                    .as_ref()
                    .map_or(0, |b| b.capacity() / 8);
            assert_eq!(acc.size(), std::mem::size_of::<Grouped>() + heap);
        };
        check(&acc);
        for groups in [9, 73, 101] {
            acc.update_batch(
                &[samples_column(&[vec![(0, 1.0)]])],
                &[groups - 1],
                None,
                groups,
            )
            .unwrap();
            check(&acc);
        }
        for drain in [3, 9, 1] {
            acc.state(EmitTo::First(drain)).unwrap();
            check(&acc);
        }
        assert!(acc
            .update_batch(&[samples_column(&[vec![(1, 1.0)]])], &[0], None, 88)
            .is_err());
        check(&acc);
        acc.evaluate(EmitTo::All).unwrap();
        check(&acc);
        assert_eq!(acc.size(), initial);
    }
}

#[test]
fn single_step_one_group_reads_are_nondestructive_and_bad_timestamps_are_errors() {
    for op in OPS {
        let mut acc = OneGroup::new(Grouped::new(op, 7, 7, 2).unwrap()).unwrap();
        acc.update_batch(&[samples_column(&[(); 2].map(|_| vec![(7, f64::MAX)]))])
            .unwrap();
        let first = acc.evaluate().unwrap();
        let state = acc.state().unwrap();
        assert_eq!(acc.evaluate().unwrap(), first);
        assert_eq!(acc.state().unwrap(), state);
        let mut copy = OneGroup::new(Grouped::new(op, 7, 7, 2).unwrap()).unwrap();
        copy.merge_batch(&[state[0].to_array().unwrap()]).unwrap();
        assert_eq!(copy.state().unwrap(), state);
        assert_eq!(copy.evaluate().unwrap(), first);
        for timestamps in [
            vec![6],
            vec![8],
            vec![9],
            vec![7, 7],
            vec![7, 9, 7],
            vec![i64::MIN],
        ] {
            let mut acc = Grouped::new(op, 7, 7, 2).unwrap();
            let input = samples_column(&[timestamps.iter().map(|&t| (t, 1.0)).collect()]);
            assert!(
                acc.update_batch(&[input], &[0], None, 1).is_err(),
                "{timestamps:?}"
            );
            let mut acc = Grouped::new(op, 7, 7, 2).unwrap();
            assert!(
                merge(&mut acc, &partial_state_at(timestamps.clone())).is_err(),
                "{timestamps:?}"
            );
        }
    }
}

#[test]
fn output_offset_arithmetic_rejects_overflow_without_large_allocations() {
    assert_eq!(checked_offset(0, i32::MAX as usize).unwrap(), i32::MAX);
    assert_eq!(checked_offset(i32::MAX - 3, 3).unwrap(), i32::MAX);
    assert_eq!(checked_offset(i32::MAX, 0).unwrap(), i32::MAX);
    for (rows, count) in [
        (0, i32::MAX as usize + 1),
        (1, i32::MAX as usize),
        (i32::MAX, 1),
        (0, usize::MAX),
    ] {
        let err = checked_offset(rows, count).unwrap_err();
        assert!(matches!(err, DataFusionError::Execution(_)));
        assert!(err.to_string().contains("i32 list offsets"));
    }
}

#[test]
fn output_sizing_counts_only_the_emitted_prefix_in_both_storage_paths() {
    for end in [0, 999_999] {
        let mut acc = Grouped::new(Op::Sum, 0, end, 1).unwrap();
        acc.update_batch(&[samples_column(&[vec![(0, 1.0)]])], &[2], None, 3)
            .unwrap();
        assert_eq!(acc.kept(0).unwrap(), 0);
        assert_eq!(acc.kept(2).unwrap(), 0);
        assert_eq!(acc.kept(3).unwrap(), 1);
        for prefix in [acc.finish(2).unwrap(), acc.partial(2).unwrap()] {
            assert_eq!(prefix.offsets().as_ref(), &[0, 0, 0]);
            assert_eq!(prefix.values().len(), 0);
        }
    }
}

/// Distant blocks can share a batch without allocating intervening query steps.
#[test]
fn long_queries_allocate_only_each_groups_evaluated_span() {
    const END: i64 = 30 * 24 * 60 * 60 * 1000;
    const STEP: i64 = 15_000;
    let near = vec![(60_000, 1.0), (90_000, 2.0)];
    let far = vec![(END - 90_000, 3.0), (END - 60_000, 4.0)];
    let input = samples_column(&[
        near.clone(),
        far,
        vec![(30_000, 5.0)],
        Vec::new(),
        vec![(0, 9.0), (END, 9.0)],
    ]);
    let mut acc = Grouped::new(Op::Sum, 0, END, STEP).unwrap();
    acc.update_batch(
        &[input],
        &[0, 1, 0, 2, 3],
        Some(&BooleanArray::from(vec![true, true, true, true, false])),
        4,
    )
    .unwrap();
    assert_eq!(acc.grid.len, 172_801);
    assert_eq!(acc.spans()[0].len(), 5);
    assert_eq!(acc.spans()[1].len(), 3);
    assert!(acc.spans()[2].seen.is_empty());
    assert!(acc.spans()[3].seen.is_empty());
    assert!(
        acc.size() < 4096,
        "{} bytes for eight dense positions",
        acc.size()
    );
    assert_eq!(
        acc.heap_size(),
        acc.spans().iter().map(Span::size).sum::<usize>()
    );
    assert_eq!(
        samples(&acc, 0),
        vec![(30_000, 5.0), (60_000, 1.0), (90_000, 2.0)]
    );
    let mut short = Grouped::new(Op::Sum, 0, 120_000, STEP).unwrap();
    let mut long = Grouped::new(Op::Sum, 0, END, STEP).unwrap();
    for acc in [&mut short, &mut long] {
        acc.update_batch(
            &[samples_column(std::slice::from_ref(&near))],
            &[0],
            None,
            1,
        )
        .unwrap();
    }
    assert_eq!(short.size(), long.size());
    let before = acc.size();
    acc.evaluate(EmitTo::First(1)).unwrap();
    assert!(acc.size() < before);
    assert_eq!(
        acc.heap_size(),
        acc.spans().iter().map(Span::size).sum::<usize>()
    );
    acc.state(EmitTo::All).unwrap();
    assert_eq!(acc.heap_size(), 0);
    assert_eq!(acc.size(), std::mem::size_of::<Grouped>());
}

#[test]
fn long_grid_partial_merges_and_prefix_emission_match_the_scalar_oracle() {
    const BASE: i64 = 400_003;
    for op in OPS {
        let incoming: Vec<Vec<(i64, f64)>> = INCOMING
            .iter()
            .map(|vs| (0..4).map(|i| (BASE + i as i64, vs[i])).collect())
            .collect();
        let mut source = Grouped::new(op, 0, 999_999, 1).unwrap();
        source
            .update_batch(&[samples_column(&[vec![(900_000, -0.0)]])], &[0], None, 2)
            .unwrap();
        source
            .update_batch(&[samples_column(&incoming)], &[1; 4], None, 2)
            .unwrap();
        let state = source.state(EmitTo::All).unwrap().remove(0).slice(1, 1);
        let resident: Vec<Vec<(i64, f64)>> = RESIDENT
            .iter()
            .map(|vs| vec![(BASE + 1, vs[0]), (BASE + 3, vs[1])])
            .collect();
        let mut dest = Grouped::new(op, 0, 999_999, 1).unwrap();
        dest.update_batch(&[samples_column(&resident)], &[1; 3], None, 2)
            .unwrap();
        let empty = dest.state(EmitTo::First(1)).unwrap().remove(0);
        assert!(empty.as_list::<i32>().value(0).is_empty());
        assert_eq!(dest.spans().len(), 1);
        assert_eq!(dest.spans()[0].first, BASE as usize + 1);
        merge(&mut dest, &state).unwrap();
        assert_eq!(dest.spans()[0].first, BASE as usize);
        assert_eq!(dest.spans()[0].len(), 4);
        for i in 0..4 {
            let mut sent = State::new(op);
            for vs in INCOMING {
                sent.add(vs[i]);
            }
            let want = if i == 1 || i == 3 {
                let mut current = State::new(op);
                for vs in RESIDENT {
                    current.add(vs[usize::from(i == 3)]);
                }
                current.merge(&sent);
                current
            } else {
                sent
            };
            assert_same(op, at(&dest, 0, BASE as usize + i).unwrap(), want, i);
        }
        assert_eq!(
            dest.heap_size(),
            dest.spans().iter().map(Span::size).sum::<usize>()
        );
        let before = dest.partial(1).unwrap();
        dest.update_batch(
            &[samples_column(&[vec![
                (BASE - 3, -0.0),
                (BASE + 7, f64::from_bits(0x7ff0_0000_0000_0002)),
            ]])],
            &[0],
            None,
            1,
        )
        .unwrap();
        let after = dest.state(EmitTo::First(1)).unwrap().remove(0);
        let before: ArrayRef = Arc::new(before);
        let before = state_rows(&before);
        let after = state_rows(&after);
        for (want, got) in before.iter().zip(&after[1..5]) {
            assert_eq!(want.0, got.0);
            assert_eq!(want.1.to_bits(), got.1.to_bits(), "{op:?} a");
            assert_eq!(want.2.to_bits(), got.2.to_bits(), "{op:?} b");
            assert_eq!(want.3.to_bits(), got.3.to_bits(), "{op:?} n");
            assert_eq!(want.4, got.4, "{op:?} flag");
        }
        assert!(dest.spans().is_empty());
        assert_eq!(dest.heap_size(), 0);
    }
}

/// Raw block times are not aggregate output times: offset moves a small span,
/// whereas @ can pin the entire query grid to that same tiny source block.
#[test]
fn offset_and_at_use_the_evaluated_domain_not_raw_block_bounds() {
    use crate::params::Params;
    let p = Params {
        start_ms: 0,
        end_ms: 999_990,
        step_ms: 10,
        window_ms: 30,
        offset_ms: 20,
        at_ms: None,
    };
    let block = series::Block {
        start_ms: 50,
        end_ms: 100,
    };
    let shifted: Vec<_> = p.for_block(block).steps().map(|t| (t, 2.0)).collect();
    let pinned: Vec<_> = Params {
        at_ms: Some(80),
        ..p
    }
    .for_block(block)
    .steps()
    .map(|t| (t, 3.0))
    .collect();
    let mut acc = Grouped::new(Op::Sum, p.start_ms, p.end_ms, p.step_ms).unwrap();
    acc.update_batch(
        &[samples_column(&[shifted.clone(), pinned])],
        &[0, 1],
        None,
        2,
    )
    .unwrap();
    assert_eq!(acc.spans()[0].first, 7);
    assert_eq!(acc.spans()[0].len(), 5);
    assert_eq!(acc.spans()[1].first, 0);
    assert_eq!(acc.spans()[1].len(), 100_000);
    let output = acc.evaluate(EmitTo::All).unwrap();
    assert_eq!(rows(output.as_list::<i32>(), 0), shifted);
    let pinned = rows(output.as_list::<i32>(), 1);
    assert_eq!(pinned.len(), 100_000);
    assert_eq!(pinned.first(), Some(&(0, 3.0)));
    assert_eq!(pinned.last(), Some(&(999_990, 3.0)));
}

#[test]
fn single_group_repeated_evaluation_retains_its_local_span_and_flags() {
    let mut acc = OneGroup::new(Grouped::new(Op::Avg, 0, 999_999, 1).unwrap()).unwrap();
    acc.update_batch(&[samples_column(&[
        vec![(400_003, f64::MAX)],
        vec![(400_003, f64::MAX)],
    ])])
    .unwrap();
    assert_eq!(acc.groups.spans()[0].seen.len(), 1);
    assert!(matches!(at(&acc.groups, 0, 400_003), Some(State::Avg(m)) if m.incremental));
    let first = acc.evaluate().unwrap();
    assert_eq!(acc.evaluate().unwrap(), first);
    let state = acc.state().unwrap().remove(0).to_array().unwrap();
    let mut merged = Grouped::new(Op::Avg, 0, 999_999, 1).unwrap();
    merge(&mut merged, &state).unwrap();
    assert_eq!(samples(&merged, 0), vec![(400_003, f64::MAX)]);
    assert_eq!(acc.evaluate().unwrap(), first);
    assert_eq!(acc.groups.spans()[0].seen.len(), 1);
}

#[test]
fn interior_runs_outside_endpoint_spans_are_errors_not_panics() {
    for timestamps in [vec![100, 105, 107, 102], vec![100, 105, 106, 102]] {
        let mut acc = Grouped::new(Op::Sum, 0, 999_999, 1).unwrap();
        let input = samples_column(&[timestamps
            .iter()
            .copied()
            .zip(vec![1.0; timestamps.len()])
            .collect()]);
        let err = acc
            .update_batch(&[input], &[0], None, 1)
            .unwrap_err()
            .to_string();
        assert!(err.contains("ascending"), "{err}");
        assert_eq!(
            acc.heap_size(),
            acc.spans().iter().map(Span::size).sum::<usize>()
        );
        let mut acc = Grouped::new(Op::Sum, 0, 999_999, 1).unwrap();
        let err = merge(&mut acc, &partial_state_at(timestamps))
            .unwrap_err()
            .to_string();
        assert!(err.contains("ascending"), "{err}");
        assert_eq!(
            acc.heap_size(),
            acc.spans().iter().map(Span::size).sum::<usize>()
        );
    }
}
