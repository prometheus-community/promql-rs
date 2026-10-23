//! Prometheus's `vectorSelectorSingle` kernel from `promql/engine.go` at
//! 83962c35. [`crate::series_eval`] is the sole raw-source driver; this module
//! owns only the per-series numerical walk and its timestamp/staleness rules.

use crate::buffer::{EvalContext, WindowBuffer, WorkBudget};

/// Prometheus's staleness marker: a NaN with this exact payload. It must
/// be compared by bits, since every NaN compares unequal to everything.
pub const STALE_NAN_BITS: u64 = 0x7ff0_0000_0000_0002;

pub fn is_stale(v: f64) -> bool {
    v.to_bits() == STALE_NAN_BITS
}

/// What the selector emits for the sample it picked at a step.
///
/// `timestamp(x)` over a bare selector wants the sample's own time, and
/// only this kernel still has it: every operator above stamps a sample
/// with the step. Upstream keeps the two apart the same way, with
/// `rangeEvalTimestampFunctionOverVectorSelector` reading `t` where
/// `evalVectorSelector` reads `f`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Hash)]
pub enum Pick {
    Value,
    /// `float64(t) / 1000`: the picked sample's time in seconds.
    Timestamp,
}

impl Pick {
    pub(crate) fn of(self, t: i64, v: f64) -> f64 {
        match self {
            Pick::Value => v,
            Pick::Timestamp => t as f64 / 1000.0,
        }
    }
}

/// `vectorSelectorSingle` for every step the buffered samples can answer
/// for good: those whose lookup time is at or before `last_t`, since a
/// later sample is later than that.
///
/// Stale markers stay in the buffer: one that is the latest sample hides
/// the series, which dropping it would undo.
///
/// `ts`/`vs` are the series' samples from ordinal `it.base` on.
pub(crate) fn advance_selector(
    it: &mut WindowBuffer,
    context: &EvalContext,
    ts: &[i64],
    vs: &[f64],
    pick: Pick,
    budget: &mut WorkBudget,
    mut emit: impl FnMut(i64, f64),
) {
    let Some(last_t) = it.last_t else {
        return;
    };
    let p = context.params();
    let steps = context.steps();

    // `@` pins the lookup, so the answer is one value repeated across the
    // grid. The buffer holds only the pinned window's latest sample.
    if let Some(at) = p.at_ms {
        if it.next_step >= steps || at - p.offset_ms > last_t {
            return;
        }
        if budget.exhausted() {
            return;
        }
        let value = *it.pinned_value.get_or_insert_with(|| {
            ts.last()
                .zip(vs.last())
                .and_then(|(&t, &v)| (!is_stale(v)).then(|| pick.of(t, v)))
        });
        while it.next_step < steps && budget.take() {
            if let Some(v) = value {
                emit(context.step_at(it.next_step), v);
            }
            it.next_step += 1;
        }
        return;
    }

    // The walk keeps its cursor in locals, relative to the slice, and moves
    // the step by addition. Through `it` every advance is a store the
    // compiler must keep, and `step_at` is an i128 multiply per step, which
    // together cost the 30-day selector 7%. The addition past the last step
    // may wrap near `i64::MAX`; that value is never read.
    let (mut next, mut h) = (it.next_step, it.hi - it.base);
    let mut step = context.step_at(next);
    'steps: while next < steps {
        let ref_time = step - p.offset_ms;
        if ref_time > last_t {
            break;
        }
        while h < ts.len() && ts[h] <= ref_time {
            if !budget.take() {
                break 'steps;
            }
            h += 1;
        }
        if !budget.take() {
            break;
        }
        if h > 0 {
            let (t, v) = (ts[h - 1], vs[h - 1]);
            if t > ref_time - p.window_ms && !is_stale(v) {
                emit(step, pick.of(t, v));
            }
        }
        next += 1;
        step = step.wrapping_add(p.step_ms);
    }
    it.next_step = next;
    it.hi = it.base + h;
    // The candidate for the next step is the last sample read, or a later one.
    it.lo = it.hi.saturating_sub(1).max(it.base);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::{BufferedSeriesIterator, Kernel};
    use crate::params::Params;
    use crate::series::{self, Block, SamplesBuilder};
    use datafusion::arrow::array::{Array, AsArray, ListArray};

    const M: i64 = 60_000;

    fn run(ts: &[i64], vs: &[f64], p: Params) -> Vec<(i64, f64)> {
        select(&[(ts, vs)], p)
    }

    /// One series pushed as `chunks`, then closed.
    fn select(chunks: &[(&[i64], &[f64])], p: Params) -> Vec<(i64, f64)> {
        select_with(Pick::Value, chunks, p)
    }

    fn select_with(pick: Pick, chunks: &[(&[i64], &[f64])], p: Params) -> Vec<(i64, f64)> {
        fold(Kernel::Selector(pick), chunks, p)
    }

    fn fold(kernel: Kernel, chunks: &[(&[i64], &[f64])], p: Params) -> Vec<(i64, f64)> {
        let context = EvalContext::new(kernel, p);
        let mut it = BufferedSeriesIterator::new(&context);
        let mut out = SamplesBuilder::default();
        for (ts, vs) in chunks {
            it.push(&context, ts, vs, |t, v| out.push(t, v));
        }
        it.close(&context, |t, v| out.push(t, v));
        out.finish_row();
        let mut rows = rows(&out.take_all());
        assert_eq!(rows.len(), 1);
        rows.pop().unwrap()
    }

    fn rows(list: &ListArray) -> Vec<Vec<(i64, f64)>> {
        (0..list.len())
            .map(|r| {
                let row = list.value(r);
                let (ts, vs) = series::sample_slices(row.as_struct());
                ts.iter().copied().zip(vs.iter().copied()).collect()
            })
            .collect()
    }

    fn params() -> Params {
        Params {
            start_ms: 0,
            end_ms: 10 * M,
            step_ms: M,
            window_ms: 5 * M,
            offset_ms: 0,
            at_ms: None,
        }
    }

    /// One sample at t=0, steps every minute. Alive for steps 0..=4
    /// (ages 0m..4m) and gone at step 5 (age exactly 5m, the half-open
    /// boundary), never to return.
    #[test]
    fn the_lookback_window_is_half_open() {
        let out = run(&[0], &[7.0], params());
        assert_eq!(
            out,
            vec![(0, 7.0), (M, 7.0), (2 * M, 7.0), (3 * M, 7.0), (4 * M, 7.0)]
        );
    }

    #[test]
    fn samples_are_stamped_with_the_step_not_their_own_time() {
        // A sample 10s after the step boundary is seen from the next step.
        let out = run(
            &[10_000],
            &[1.0],
            Params {
                end_ms: M,
                ..params()
            },
        );
        assert_eq!(out, vec![(M, 1.0)]);
    }

    #[test]
    fn the_latest_sample_wins() {
        let out = run(
            &[0, 30_000, 90_000],
            &[1.0, 2.0, 3.0],
            Params {
                end_ms: 2 * M,
                ..params()
            },
        );
        assert_eq!(out, vec![(0, 1.0), (M, 2.0), (2 * M, 3.0)]);
    }

    #[test]
    fn a_stale_marker_hides_the_series_until_a_fresh_sample() {
        let stale = f64::from_bits(STALE_NAN_BITS);
        let out = run(
            &[0, M, 2 * M],
            &[1.0, stale, 3.0],
            Params {
                end_ms: 2 * M,
                ..params()
            },
        );
        assert_eq!(out, vec![(0, 1.0), (2 * M, 3.0)]);
        // An ordinary NaN is a value, not a marker.
        let out = run(
            &[0],
            &[f64::NAN],
            Params {
                end_ms: 0,
                ..params()
            },
        );
        assert_eq!(out.len(), 1);
        assert!(out[0].1.is_nan());
    }

    #[test]
    fn offset_shifts_the_lookup_not_the_emission() {
        // With offset 1m, step t looks at t-1m. The sample at 0 is seen
        // from step 1m onward and expires at step 6m.
        let out = run(
            &[0],
            &[1.0],
            Params {
                offset_ms: M,
                ..params()
            },
        );
        assert_eq!(
            out,
            vec![
                (M, 1.0),
                (2 * M, 1.0),
                (3 * M, 1.0),
                (4 * M, 1.0),
                (5 * M, 1.0)
            ]
        );
        // Negative offset looks into the future.
        let out = run(
            &[3 * M],
            &[1.0],
            Params {
                offset_ms: -M,
                end_ms: 3 * M,
                ..params()
            },
        );
        assert_eq!(out, vec![(2 * M, 1.0), (3 * M, 1.0)]);
    }

    /// `timestamp(x)` over a bare selector: the same sample is picked at
    /// each step and its own time, not the step's and not its value,
    /// is what comes out. An offset moves which sample is picked and
    /// leaves its time alone; a stale marker hides the series as it
    /// does for the value; `@` repeats the pinned sample's time.
    #[test]
    fn the_timestamp_pick_emits_the_samples_own_time_in_seconds() {
        let by_time = |chunks: &[(&[i64], &[f64])], p| select_with(Pick::Timestamp, chunks, p);
        let out = by_time(
            &[(&[10_000, 90_000], &[7.0, 8.0])],
            Params {
                end_ms: 2 * M,
                ..params()
            },
        );
        assert_eq!(out, vec![(M, 10.0), (2 * M, 90.0)]);

        let out = by_time(
            &[(&[10_000, 90_000], &[7.0, 8.0])],
            Params {
                offset_ms: M,
                end_ms: 3 * M,
                ..params()
            },
        );
        assert_eq!(out, vec![(2 * M, 10.0), (3 * M, 90.0)]);

        let stale = f64::from_bits(STALE_NAN_BITS);
        let out = by_time(
            &[(&[0, M], &[1.0, stale])],
            Params {
                end_ms: M,
                ..params()
            },
        );
        assert_eq!(out, vec![(0, 0.0)]);

        let out = by_time(
            &[(&[0, M, 2 * M], &[1.0, 2.0, 3.0])],
            Params {
                at_ms: Some(M + 1),
                end_ms: 2 * M,
                ..params()
            },
        );
        assert_eq!(out, vec![(0, 60.0), (M, 60.0), (2 * M, 60.0)]);
    }

    #[test]
    fn one_step_value_and_timestamp_have_literal_modifier_and_overlap_answers() {
        let stale = f64::from_bits(STALE_NAN_BITS);
        let nan = f64::from_bits(0x7ff8_0000_0000_1234);
        let base = Params {
            start_ms: 10 * M,
            end_ms: 10 * M,
            ..params()
        };
        // Each tuple supplies independent Value and Timestamp expectations.
        let cases = [
            (base, vec![vec![(5 * M, 8.0)]], None),
            (base, vec![vec![(10 * M, -0.0)]], Some((-0.0, 600.0))),
            (base, vec![vec![(9 * M, nan)]], Some((nan, 540.0))),
            (
                base,
                vec![vec![(9 * M, stale)], vec![(9 * M, 7.0), (11 * M, 8.0)]],
                None,
            ),
            (
                base,
                vec![
                    vec![(8 * M, 4.0), (11 * M, stale)],
                    vec![(9 * M, 9.0), (12 * M, 10.0)],
                ],
                Some((4.0, 480.0)),
            ),
            (
                base,
                vec![vec![(6 * M, 1.0)], vec![(8 * M, -0.0)], vec![(11 * M, 2.0)]],
                Some((-0.0, 480.0)),
            ),
            (
                Params {
                    offset_ms: M,
                    ..base
                },
                vec![vec![(8 * M, 3.0), (10 * M, 9.0)]],
                Some((3.0, 480.0)),
            ),
            (
                Params {
                    offset_ms: -M,
                    ..base
                },
                vec![vec![(11 * M, nan), (12 * M, 9.0)]],
                Some((nan, 660.0)),
            ),
            (
                Params {
                    at_ms: Some(8 * M + 1),
                    offset_ms: M,
                    ..base
                },
                vec![vec![(7 * M, -0.0), (8 * M, 9.0)]],
                Some((-0.0, 420.0)),
            ),
            (
                Params {
                    at_ms: Some(9 * M),
                    ..base
                },
                vec![vec![(9 * M, stale)]],
                None,
            ),
        ];
        for (p, chunks, expected) in cases {
            let chunks: Vec<(Vec<i64>, Vec<f64>)> = chunks
                .into_iter()
                .map(|chunk| chunk.into_iter().unzip())
                .collect();
            let chunks: Vec<(&[i64], &[f64])> = chunks
                .iter()
                .map(|(ts, vs)| (ts.as_slice(), vs.as_slice()))
                .collect();
            for pick in [Pick::Value, Pick::Timestamp] {
                let got = select_with(pick, &chunks, p);
                let want: Vec<(i64, f64)> = expected
                    .map(|(value, timestamp)| {
                        (
                            p.start_ms,
                            if pick == Pick::Value {
                                value
                            } else {
                                timestamp
                            },
                        )
                    })
                    .into_iter()
                    .collect();
                assert!(
                    close_enough(&got, &want),
                    "{pick:?} {p:?}: {got:?} != {want:?}"
                );
            }
        }
    }

    #[test]
    fn zero_and_multi_step_selectors_preserve_literal_bits_and_staleness() {
        let nan = f64::from_bits(0x7ff8_0000_0000_1234);
        let stale = f64::from_bits(STALE_NAN_BITS);
        let ts = [0, M, 2 * M];
        let vs = [-0.0, nan, stale];
        let p = Params {
            end_ms: 2 * M,
            ..params()
        };
        assert!(close_enough(
            &select_with(Pick::Value, &[(&ts, &vs)], p),
            &[(0, -0.0), (M, nan)]
        ));
        assert_eq!(
            select_with(Pick::Timestamp, &[(&ts, &vs)], p),
            [(0, 0.0), (M, 60.0)]
        );
        let pinned = Params {
            at_ms: Some(M),
            ..p
        };
        assert!(close_enough(
            &select_with(Pick::Value, &[(&ts, &vs)], pinned),
            &[(0, nan), (M, nan), (2 * M, nan)]
        ));
        assert_eq!(
            select_with(Pick::Timestamp, &[(&ts, &vs)], pinned),
            [(0, 60.0), (M, 60.0), (2 * M, 60.0)]
        );
        for pick in [Pick::Value, Pick::Timestamp] {
            let zero = Params {
                start_ms: 1,
                end_ms: 0,
                ..pinned
            };
            assert!(select_with(pick, &[(&ts, &vs)], zero).is_empty());
        }
    }

    #[test]
    fn at_pins_the_lookup_and_repeats_it_on_every_step() {
        let out = run(
            &[0, M, 2 * M],
            &[1.0, 2.0, 3.0],
            Params {
                at_ms: Some(M + 1),
                end_ms: 2 * M,
                ..params()
            },
        );
        assert_eq!(out, vec![(0, 2.0), (M, 2.0), (2 * M, 2.0)]);
        // `@` too far after the data: nothing at any step.
        let out = run(
            &[0],
            &[1.0],
            Params {
                at_ms: Some(10 * M),
                ..params()
            },
        );
        assert!(out.is_empty());
    }

    /// Gaps longer than the lookback, a stale marker, a NaN and repeated
    /// values: every branch `advance_selector` has, across the grids below.
    fn a_rough_series() -> (Vec<i64>, Vec<f64>) {
        let stale = f64::from_bits(STALE_NAN_BITS);
        [
            (0, 1.0),
            (30_000, 2.0),
            (90_000, 2.0),
            (120_000, f64::NAN),
            (150_000, 5.0),
            (180_000, stale),
            (240_000, 4.0),
            (270_000, -0.0),
            (300_000, 9.0),
            (900_000, 3.0),
            (930_000, stale),
            (960_000, 3.0),
            (1_020_000, 2.0),
        ]
        .into_iter()
        .unzip()
    }

    fn grids() -> Vec<Params> {
        let base = Params {
            start_ms: 0,
            end_ms: 20 * M,
            step_ms: 30_000,
            window_ms: 2 * M,
            offset_ms: 0,
            at_ms: None,
        };
        vec![
            base,
            Params {
                step_ms: 7_000,
                ..base
            },
            Params {
                start_ms: 16 * M,
                ..base
            },
            Params {
                offset_ms: 90_000,
                ..base
            },
            Params {
                offset_ms: -M,
                ..base
            },
            Params {
                window_ms: 5 * M,
                ..base
            },
            Params {
                at_ms: Some(150_000),
                ..base
            },
            Params {
                at_ms: Some(160_000),
                offset_ms: -M,
                ..base
            },
            Params {
                at_ms: Some(185_000),
                ..base
            },
            Params {
                at_ms: Some(20 * M),
                ..base
            },
        ]
    }

    fn close_enough(a: &[(i64, f64)], b: &[(i64, f64)]) -> bool {
        a.len() == b.len()
            && a.iter()
                .zip(b)
                .all(|(x, y)| x.0 == y.0 && x.1.to_bits() == y.1.to_bits())
    }

    #[test]
    fn chunk_splits_select_what_one_chunk_selects() {
        let (ts, vs) = a_rough_series();
        for p in grids() {
            let expected = run(&ts, &vs, p);
            for k in 1..=ts.len() {
                let chunks: Vec<(&[i64], &[f64])> = ts.chunks(k).zip(vs.chunks(k)).collect();
                let got = select(&chunks, p);
                assert!(
                    close_enough(&got, &expected),
                    "{p:?}, chunks of {k}: {got:?} != {expected:?}"
                );
            }
        }
    }

    #[test]
    fn chunks_repeating_the_previous_tail_select_the_same() {
        let (ts, vs) = a_rough_series();
        for p in grids() {
            let expected = run(&ts, &vs, p);
            for k in 2..=ts.len() {
                // Each chunk starts one sample back, a duplicate of the
                // previous chunk's last one.
                let chunks: Vec<(&[i64], &[f64])> = (0..ts.len())
                    .step_by(k - 1)
                    .map(|a| {
                        let b = (a + k).min(ts.len());
                        (&ts[a..b], &vs[a..b])
                    })
                    .collect();
                let got = select(&chunks, p);
                assert!(
                    close_enough(&got, &expected),
                    "{p:?}, overlapping chunks of {k}: {got:?} != {expected:?}"
                );
            }
        }
    }

    #[test]
    fn a_block_answers_only_its_own_steps() {
        let grid = Params {
            end_ms: 2 * M,
            ..params()
        };
        let first = run(
            &[0, 30_000],
            &[1.0, 2.0],
            grid.for_block(Block {
                start_ms: 0,
                end_ms: M,
            }),
        );
        let second = run(
            &[0, 30_000, M],
            &[1.0, 2.0, 3.0],
            grid.for_block(Block {
                start_ms: M,
                end_ms: 3 * M,
            }),
        );
        assert_eq!(first, [(0, 1.0)]);
        assert_eq!(second, [(M, 3.0), (2 * M, 3.0)]);
    }

    #[test]
    fn block_local_zero_one_many_grids_preserve_literal_answers() {
        for kernel in [
            Kernel::Selector(Pick::Value),
            Kernel::Selector(Pick::Timestamp),
            Kernel::Range(crate::range::RangeOp::Func(
                crate::range::Func::CountOverTime,
            )),
        ] {
            let grid = Params {
                end_ms: 4 * M,
                ..params()
            };
            let block = |start_ms, end_ms| Block { start_ms, end_ms };
            let blocks = [
                (block(-M, 0), vec![(-30_000, 5.0)]),
                (block(0, M), vec![(-30_000, 11.0)]),
                (block(M, 3 * M), vec![(30_000, 22.0), (90_000, 23.0)]),
                (block(3 * M, 4 * M), vec![(150_000, 33.0)]),
                (block(4 * M, 5 * M), vec![(210_000, 44.0)]),
            ];
            let got = blocks
                .into_iter()
                .map(|(block, samples)| {
                    let (ts, vs): (Vec<_>, Vec<_>) = samples.into_iter().unzip();
                    fold(kernel, &[(&ts, &vs)], grid.for_block(block))
                })
                .collect::<Vec<_>>();
            let want = match kernel {
                Kernel::Selector(Pick::Value) => vec![
                    vec![],
                    vec![(0, 11.0)],
                    vec![(M, 22.0), (2 * M, 23.0)],
                    vec![(3 * M, 33.0)],
                    vec![(4 * M, 44.0)],
                ],
                Kernel::Selector(Pick::Timestamp) => vec![
                    vec![],
                    vec![(0, -30.0)],
                    vec![(M, 30.0), (2 * M, 90.0)],
                    vec![(3 * M, 150.0)],
                    vec![(4 * M, 210.0)],
                ],
                Kernel::Range(_) => vec![
                    vec![],
                    vec![(0, 1.0)],
                    vec![(M, 1.0), (2 * M, 2.0)],
                    vec![(3 * M, 1.0)],
                    vec![(4 * M, 1.0)],
                ],
            };
            assert_eq!(got, want, "{kernel:?}");
        }
    }

    #[test]
    fn a_series_continues_from_one_chunk_to_the_next() {
        let grid = Params {
            end_ms: 2 * M,
            ..params()
        };
        assert_eq!(run(&[0], &[1.0], grid), [(0, 1.0), (M, 1.0), (2 * M, 1.0)]);
        assert_eq!(
            select(&[(&[0], &[5.0]), (&[M], &[6.0])], grid),
            [(0, 5.0), (M, 6.0), (2 * M, 6.0)]
        );
    }
}
