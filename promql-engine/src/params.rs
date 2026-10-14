//! The step grid, shared by every kernel that walks one.
//!
//! An instant selector and a range function differ in what they do with
//! a window, not in how they find one: same grid, same `offset`, same
//! `@`, same strict lower bound on the scan range. They are one struct
//! so that a change to the grid arithmetic cannot land in one kernel and
//! miss the other.

use crate::series::Block;

/// Everything a per-series kernel needs besides the samples.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Hash)]
pub struct Params {
    pub start_ms: i64,
    pub end_ms: i64,
    pub step_ms: i64,
    /// How far back of each step the kernel reads: the lookback delta
    /// for an instant selector, the `[5m]` for a range function.
    pub window_ms: i64,
    /// The selector's offset. Positive looks into the past.
    pub offset_ms: i64,
    /// An `@` modifier, already resolved from `start()`/`end()`.
    pub at_ms: Option<i64>,
}

impl Params {
    /// The scan range a store has to be asked for so that every step can
    /// be answered: `getTimeRangesForSelector` in upstream, the first
    /// window end widened back by the whole window. The window is open at
    /// its start, so the sample at exactly `lo` is read and never used;
    /// the store's first block reaches back from `lo + window_ms` by the
    /// window it is told, and a range one shorter would put that sample
    /// outside every block's reach-back and make the contract lie by one.
    ///
    /// The saturating arithmetic is the lower half of a two-part defence
    /// against an absurd timestamp: the planner rejects an out-of-range
    /// `@` or offset, and this clamps whatever still reaches it.
    pub fn select_range(&self) -> (i64, i64) {
        let (lo, hi) = self.window_ends();
        (
            lo.saturating_sub(self.window_ms)
                .saturating_sub(self.offset_ms),
            hi.saturating_sub(self.offset_ms),
        )
    }

    /// The first and last window end before the offset: `@` pins both.
    fn window_ends(&self) -> (i64, i64) {
        match self.at_ms {
            Some(at) => (at, at),
            None => (self.start_ms, self.end_ms),
        }
    }

    /// This grid cut to the steps `block` answers: those whose window
    /// end, `t - offset` or the `@` time, lies in `[start_ms, end_ms)`
    /// of the block. Under `@` every step shares one window end, so one
    /// block answers all of them and every other answers none.
    ///
    /// The kernels run one series of one block against this, so a
    /// series' steps in another block are never evaluated from the
    /// reach-back this block repeats; an empty grid, `end_ms` before
    /// `start_ms`, is a block that answers nothing for this selector.
    pub(crate) fn for_block(&self, block: Block) -> Params {
        let (step, offset) = (i128::from(self.step_ms), i128::from(self.offset_ms));
        let (lo, hi) = (i128::from(block.start_ms), i128::from(block.end_ms));
        if let Some(at) = self.at_ms {
            let end = i128::from(at) - offset;
            return if lo <= end && end < hi {
                *self
            } else {
                self.empty()
            };
        }
        // Steps `t` with `lo <= t - offset < hi`, on the grid and inside
        // the query, as indices so the arithmetic never leaves the grid.
        let start = i128::from(self.start_ms);
        let count = step_count(self.start_ms, self.end_ms, self.step_ms);
        let first = (lo + offset - start).div_euclid(step).max(0);
        let first = if start + first * step < lo + offset {
            first + 1
        } else {
            first
        };
        let last = (hi + offset - 1 - start).div_euclid(step).min(count - 1);
        if first > last {
            return self.empty();
        }
        Params {
            start_ms: (start + first * step) as i64,
            end_ms: (start + last * step) as i64,
            ..*self
        }
    }

    /// A grid with no steps: `end_ms` before `start_ms`, which
    /// [`step_count`] reads as zero. Fixed values rather than the grid's
    /// own moved by one, which cannot be done at the end of time.
    fn empty(&self) -> Params {
        Params {
            start_ms: 1,
            end_ms: 0,
            ..*self
        }
    }

    /// The grid this evaluates on, whose length is [`step_count`].
    pub(crate) fn steps(&self) -> impl Iterator<Item = i64> {
        let (start, step) = (i128::from(self.start_ms), i128::from(self.step_ms));
        // The count is derived up front, the way `Grid` derives its length,
        // so the last position is <= end by construction: no per-step bound
        // check, and so no probe one step past the end that could overflow.
        let count = step_count(self.start_ms, self.end_ms, self.step_ms);
        (0..count).map(move |i| (start + i * step) as i64)
    }
}

/// How many steps `start_ms..=end_ms` every `step_ms` has.
///
/// In `i128` so that a range as wide as `i64` is a number to compare
/// against a cap rather than an overflow. The planner and
/// [`Grid`](crate::aggregate) both measure with this, so their guards
/// agree by construction; both reject a non-positive step first, with a
/// message of their own.
pub fn step_count(start_ms: i64, end_ms: i64, step_ms: i64) -> i128 {
    if step_ms <= 0 || end_ms < start_ms {
        return 0;
    }
    (i128::from(end_ms) - i128::from(start_ms)) / i128::from(step_ms) + 1
}

#[cfg(test)]
mod tests {
    use super::*;

    const S: i64 = 1000;
    const M: i64 = 60 * S;

    #[test]
    fn the_select_range_is_widened_by_the_whole_window() {
        let p = Params {
            start_ms: 0,
            end_ms: 10 * M,
            step_ms: M,
            window_ms: 5 * M,
            offset_ms: 30 * S,
            at_ms: None,
        };
        assert_eq!(p.select_range(), (-(5 * M) - 30 * S, 10 * M - 30 * S));
    }

    fn grid() -> Params {
        Params {
            start_ms: 0,
            end_ms: 10 * M,
            step_ms: M,
            window_ms: 5 * M,
            offset_ms: 0,
            at_ms: None,
        }
    }

    fn block(start_ms: i64, end_ms: i64) -> Block {
        Block { start_ms, end_ms }
    }

    fn steps_of(p: Params) -> Vec<i64> {
        p.steps().collect()
    }

    /// A block answers the steps whose window end falls in it, the start
    /// inclusive and the end exclusive, and a step between two grid
    /// points belongs to neither.
    #[test]
    fn a_block_answers_the_steps_whose_window_end_it_holds() {
        let p = grid();
        assert_eq!(steps_of(p.for_block(block(2 * M, 4 * M))), [2 * M, 3 * M]);
        assert_eq!(
            steps_of(p.for_block(block(2 * M + 1, 4 * M + 1))),
            [3 * M, 4 * M]
        );
        // Wider than the query on both sides: the whole grid.
        assert_eq!(steps_of(p.for_block(block(-M, 20 * M))), steps_of(p));
        // Past the query, or between two steps: nothing.
        assert!(steps_of(p.for_block(block(11 * M, 12 * M))).is_empty());
        assert!(steps_of(p.for_block(block(M + 1, 2 * M))).is_empty());
        assert_eq!(p.for_block(block(11 * M, 12 * M)).steps().count(), 0);
    }

    /// The window end is `t - offset`, so an offset moves the steps a
    /// block answers later by that much.
    #[test]
    fn an_offset_moves_the_block_along_the_grid() {
        let p = Params {
            offset_ms: 30 * S,
            ..grid()
        };
        // Window ends 2m and 3m are steps 2m30s and 3m30s, which are not
        // on the grid; 2m30s <= t - 30s < 4m holds for t = 3m, 4m.
        assert_eq!(steps_of(p.for_block(block(2 * M, 4 * M))), [3 * M, 4 * M]);
        let p = Params {
            offset_ms: -M,
            ..grid()
        };
        assert_eq!(steps_of(p.for_block(block(2 * M, 4 * M))), [M, 2 * M]);
    }

    /// Under `@` every step's window ends at the pinned time, so the one
    /// block holding it answers the whole grid and the others none.
    #[test]
    fn at_puts_every_step_in_one_block() {
        let p = Params {
            at_ms: Some(3 * M),
            ..grid()
        };
        assert_eq!(steps_of(p.for_block(block(2 * M, 4 * M))), steps_of(p));
        assert!(steps_of(p.for_block(block(4 * M, 6 * M))).is_empty());
        assert!(steps_of(p.for_block(block(0, 3 * M))).is_empty());
        let p = Params {
            at_ms: Some(3 * M),
            offset_ms: M,
            ..grid()
        };
        assert_eq!(steps_of(p.for_block(block(2 * M, 3 * M))), steps_of(p));
    }

    #[test]
    fn a_block_at_the_edge_of_time_cuts_without_overflowing() {
        let p = Params {
            start_ms: i64::MAX - 10,
            end_ms: i64::MAX,
            step_ms: 5,
            ..grid()
        };
        assert_eq!(
            steps_of(p.for_block(block(i64::MAX - 5, i64::MAX))),
            [i64::MAX - 5]
        );
        let p = Params {
            start_ms: i64::MIN,
            end_ms: i64::MIN + 10,
            step_ms: 5,
            ..grid()
        };
        assert_eq!(
            steps_of(p.for_block(block(i64::MIN, i64::MIN + 6))),
            [i64::MIN, i64::MIN + 5]
        );
        assert!(steps_of(p.for_block(block(i64::MIN, i64::MIN))).is_empty());
    }

    /// `rate(up[5m] @ -1e30)` is where an extreme timestamp comes from.
    #[test]
    fn an_extreme_at_timestamp_clamps_the_select_range() {
        let base = Params {
            start_ms: 5 * M,
            end_ms: 5 * M,
            step_ms: 30 * S,
            window_ms: 5 * M,
            offset_ms: 0,
            at_ms: None,
        };
        let p = Params {
            at_ms: Some(i64::MIN),
            offset_ms: i64::MAX,
            ..base
        };
        assert_eq!(p.select_range(), (i64::MIN, i64::MIN));
        let p = Params {
            at_ms: Some(i64::MAX),
            offset_ms: i64::MIN,
            ..base
        };
        assert_eq!(p.select_range(), (i64::MAX, i64::MAX));
        let p = Params {
            start_ms: i64::MIN,
            end_ms: i64::MAX,
            window_ms: i64::MAX,
            ..base
        };
        let (lo, hi) = p.select_range();
        assert_eq!((lo, hi), (i64::MIN, i64::MAX));
        assert!(lo <= hi);
    }

    #[test]
    fn the_step_count_of_the_widest_possible_range_is_a_number() {
        assert_eq!(step_count(0, 9, 1), 10);
        assert_eq!(step_count(0, 9, 10), 1);
        assert_eq!(step_count(10, 0, 1), 0);
        assert_eq!(step_count(0, 10, 0), 0);
        assert_eq!(
            step_count(i64::MIN, i64::MAX, 1),
            i128::from(u64::MAX) + 1,
            "the widest range does not overflow"
        );
    }

    #[test]
    fn the_steps_are_as_many_as_the_count_says() {
        let p = Params {
            start_ms: 0,
            end_ms: 100,
            step_ms: 30,
            window_ms: 0,
            offset_ms: 0,
            at_ms: None,
        };
        assert_eq!(p.steps().collect::<Vec<_>>(), vec![0, 30, 60, 90]);
        assert_eq!(p.steps().count() as i128, step_count(0, 100, 30));
    }

    #[test]
    fn the_step_grid_stops_at_the_end_of_time() {
        let p = Params {
            start_ms: i64::MAX,
            end_ms: i64::MAX,
            step_ms: 1,
            window_ms: 0,
            offset_ms: 0,
            at_ms: None,
        };
        assert_eq!(step_count(p.start_ms, p.end_ms, p.step_ms), 1);

        // Ask for the end explicitly: collecting an unbounded iterator
        // could hang if overflow wraps instead of panicking in release.
        let mut steps = p.steps();
        assert_eq!(steps.next(), Some(i64::MAX));
        assert_eq!(steps.next(), None);
    }
}
