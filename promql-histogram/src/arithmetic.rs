// Licensed under the Apache License, Version 2.0.
// Derived from Prometheus model/histogram/float_histogram.go and generic.go.

use std::collections::BTreeMap;

use thiserror::Error;

use crate::{CounterResetHint, FloatHistogram, Span, CUSTOM_SCHEMA};

#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum HistogramError {
    #[error("cannot combine exponential and custom histograms")]
    IncompatibleSchemas,
}

/// Non-fatal findings from Prometheus `FloatHistogram.Add`, `Sub`, or `KahanAdd`.
/// The evaluator needs these separately from schema errors to emit annotations.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ArithmeticOutcome {
    pub counter_reset_collision: bool,
    pub custom_bounds_reconciled: bool,
}

impl ArithmeticOutcome {
    fn merge(self, other: Self) -> Self {
        Self {
            counter_reset_collision: self.counter_reset_collision || other.counter_reset_collision,
            custom_bounds_reconciled: self.custom_bounds_reconciled
                || other.custom_bounds_reconciled,
        }
    }
}

#[derive(Clone, Debug)]
pub struct KahanSum {
    sum: FloatHistogram,
    compensation: FloatHistogram,
}

impl KahanSum {
    pub fn new(first: FloatHistogram) -> Self {
        let compensation = first.compensation_histogram();
        Self {
            sum: first,
            compensation,
        }
    }

    /// Prometheus `FloatHistogram.KahanAdd`, retaining both annotation flags.
    pub fn add(&mut self, histogram: &FloatHistogram) -> Result<ArithmeticOutcome, HistogramError> {
        self.sum.kahan_add(histogram, &mut self.compensation)
    }

    /// Restores an unfinished sum transported between aggregation stages.
    pub fn from_parts(sum: FloatHistogram, compensation: FloatHistogram) -> Self {
        Self { sum, compensation }
    }

    /// Adds both components of another unfinished sum without rounding it first.
    pub fn add_parts(
        &mut self,
        sum: &FloatHistogram,
        compensation: &FloatHistogram,
    ) -> Result<ArithmeticOutcome, HistogramError> {
        let outcome = self.add(sum)?;
        Ok(outcome.merge(self.add(compensation)?))
    }

    /// Splits an unfinished sum for transport between aggregation stages.
    pub fn into_parts(self) -> (FloatHistogram, FloatHistogram) {
        (self.sum, self.compensation)
    }

    /// Applies the compensation term required to complete Prometheus Kahan summation.
    pub fn finish(mut self) -> Result<FloatHistogram, HistogramError> {
        self.sum.add(&self.compensation)?;
        self.sum.compact();
        Ok(self.sum)
    }

    /// Heap bytes retained by the sum and its compensation buffers.
    pub fn heap_size(&self) -> usize {
        self.sum.heap_size() + self.compensation.heap_size()
    }

    /// Whether the running sum has overflowed, the test Prometheus applies
    /// before switching an average to an incremental mean. The compensation
    /// term is not consulted there either.
    pub fn has_overflow(&self) -> bool {
        self.sum.has_overflow()
    }

    /// Multiplies both the sum and its compensation, as Prometheus scales
    /// `mean` and `kahanC` together while averaging incrementally.
    pub fn mul(&mut self, factor: f64) -> &mut Self {
        self.sum.mul(factor);
        self.compensation.mul(factor);
        self
    }

    /// Divides both the sum and its compensation.
    pub fn div(&mut self, divisor: f64) -> &mut Self {
        self.sum.div(divisor);
        self.compensation.div(divisor);
        self
    }
}

impl FloatHistogram {
    /// Heap bytes retained by span, bucket, and custom-bound buffers.
    pub fn heap_size(&self) -> usize {
        self.negative_spans.capacity() * std::mem::size_of::<Span>()
            + self.negative_buckets.capacity() * std::mem::size_of::<f64>()
            + self.positive_spans.capacity() * std::mem::size_of::<Span>()
            + self.positive_buckets.capacity() * std::mem::size_of::<f64>()
            + self.custom_values.capacity() * std::mem::size_of::<f64>()
    }

    /// Prometheus `FloatHistogram.HasOverflow`: whether any count or the
    /// sum is infinite, which aggregating enough histograms can produce.
    pub fn has_overflow(&self) -> bool {
        self.zero_count.is_infinite()
            || self.count.is_infinite()
            || self.sum.is_infinite()
            || self
                .positive_buckets
                .iter()
                .chain(&self.negative_buckets)
                .any(|bucket| bucket.is_infinite())
    }

    pub fn copy_to_schema(&self, target_schema: i32) -> Self {
        assert!(self.schema != CUSTOM_SCHEMA && target_schema != CUSTOM_SCHEMA);
        assert!(target_schema <= self.schema);
        let mut copy = self.clone();
        copy.reduce_resolution_in_place(target_schema);
        copy
    }

    pub fn mul(&mut self, factor: f64) -> &mut Self {
        self.zero_count *= factor;
        self.count *= factor;
        self.sum *= factor;
        for bucket in self
            .positive_buckets
            .iter_mut()
            .chain(self.negative_buckets.iter_mut())
        {
            *bucket *= factor;
        }
        if factor < 0.0 {
            self.counter_reset_hint = CounterResetHint::Gauge;
        }
        self
    }

    pub fn div(&mut self, divisor: f64) -> &mut Self {
        self.zero_count /= divisor;
        self.count /= divisor;
        self.sum /= divisor;
        if divisor == 0.0 {
            self.positive_spans.clear();
            self.positive_buckets.clear();
            self.negative_spans.clear();
            self.negative_buckets.clear();
            return self;
        }
        for bucket in self
            .positive_buckets
            .iter_mut()
            .chain(self.negative_buckets.iter_mut())
        {
            *bucket /= divisor;
        }
        if divisor < 0.0 {
            self.counter_reset_hint = CounterResetHint::Gauge;
        }
        self
    }

    /// Prometheus `FloatHistogram.Add`: reconcile layouts without modifying `other`.
    pub fn add(&mut self, other: &Self) -> Result<ArithmeticOutcome, HistogramError> {
        self.combine(other, false, false, None)
    }

    /// Prometheus `FloatHistogram.Sub`. A PromQL subtraction operator must also
    /// set the result's reset hint to `Gauge`; incremental means must not.
    pub fn sub(&mut self, other: &Self) -> Result<ArithmeticOutcome, HistogramError> {
        self.combine(other, true, false, None)
    }

    /// Prometheus `FloatHistogram.Compact(0)`: remove empty buckets and merge
    /// adjacent spans. Bucket capacities are retained for subsequent arithmetic.
    pub fn compact(&mut self) -> &mut Self {
        (self.positive_spans, self.positive_buckets) = compact_side(
            std::mem::take(&mut self.positive_spans),
            std::mem::take(&mut self.positive_buckets),
        );
        (self.negative_spans, self.negative_buckets) = compact_side(
            std::mem::take(&mut self.negative_spans),
            std::mem::take(&mut self.negative_buckets),
        );
        self
    }

    pub fn detect_reset(&self, previous: &Self) -> bool {
        if self.counter_reset_hint == CounterResetHint::Reset {
            return true;
        }
        if self.counter_reset_hint == CounterResetHint::NotReset {
            return false;
        }
        if self.count < previous.count
            || self.uses_custom_buckets() != previous.uses_custom_buckets()
        {
            return true;
        }
        if self.uses_custom_buckets() {
            let bounds = intersect_bounds(&self.custom_values, &previous.custom_values);
            let current = remap_custom(self, &bounds);
            let previous = remap_custom(previous, &bounds);
            return previous
                .iter()
                .any(|(index, count)| current.get(index).copied().unwrap_or(0.0) < *count);
        }
        if self.shares_layout(previous) {
            let dropped = |current: &[f64], previous: &[f64]| {
                current.iter().zip(previous).any(|(c, p)| c < p)
            };
            return self.zero_count < previous.zero_count
                || dropped(&self.positive_buckets, &previous.positive_buckets)
                || dropped(&self.negative_buckets, &previous.negative_buckets);
        }
        if self.schema > previous.schema || self.zero_threshold < previous.zero_threshold {
            return true;
        }
        let (previous_zero, threshold, _) = previous.zero_count_for_threshold(self.zero_threshold);
        if threshold != self.zero_threshold || self.zero_count < previous_zero {
            return true;
        }
        for positive in [true, false] {
            let current = self.side_at_schema(positive, self.schema, self.zero_threshold);
            let previous = previous.side_at_schema(positive, self.schema, self.zero_threshold);
            if previous
                .iter()
                .any(|(index, count)| current.get(index).copied().unwrap_or(0.0) < *count)
            {
                return true;
            }
        }
        false
    }

    pub(crate) fn reduce_resolution_in_place(&mut self, target_schema: i32) {
        if target_schema == self.schema {
            return;
        }
        let origin_schema = self.schema;
        (self.positive_spans, self.positive_buckets) = reduce_side(
            &self.positive_spans,
            &self.positive_buckets,
            origin_schema,
            target_schema,
        );
        (self.negative_spans, self.negative_buckets) = reduce_side(
            &self.negative_spans,
            &self.negative_buckets,
            origin_schema,
            target_schema,
        );
        self.schema = target_schema;
    }

    fn kahan_add(
        &mut self,
        other: &Self,
        compensation: &mut Self,
    ) -> Result<ArithmeticOutcome, HistogramError> {
        self.combine(other, false, true, Some(compensation))
    }

    fn combine(
        &mut self,
        other: &Self,
        subtract: bool,
        kahan: bool,
        mut compensation: Option<&mut Self>,
    ) -> Result<ArithmeticOutcome, HistogramError> {
        if self.uses_custom_buckets() != other.uses_custom_buckets() {
            return Err(HistogramError::IncompatibleSchemas);
        }
        let outcome = ArithmeticOutcome {
            counter_reset_collision: self.adjust_reset_hint(other),
            custom_bounds_reconciled: self.uses_custom_buckets()
                && self.custom_values != other.custom_values,
        };

        if kahan {
            let c = compensation.as_deref_mut().expect("Kahan compensation");
            (self.count, c.count) = kahan_inc(other.count, self.count, c.count);
            (self.sum, c.sum) = kahan_inc(other.sum, self.sum, c.sum);
        } else if subtract {
            self.count -= other.count;
            self.sum -= other.sum;
        } else {
            self.count += other.count;
            self.sum += other.sum;
        }

        if self.uses_custom_buckets() {
            self.combine_custom(other, subtract, kahan, compensation);
            return Ok(outcome);
        }
        if self.shares_layout(other)
            && compensation
                .as_deref()
                .is_none_or(|c| self.shares_layout(c))
        {
            self.combine_aligned(other, subtract, compensation);
            return Ok(outcome);
        }

        self.reconcile_zero(other, subtract, kahan, compensation.as_deref_mut());
        let target_schema = self.schema.min(other.schema);
        if self.schema != target_schema {
            if let Some(c) = compensation.as_deref_mut() {
                reduce_histogram_pair(self, c, target_schema);
            } else {
                self.reduce_resolution_in_place(target_schema);
            }
        }

        for positive in [true, false] {
            let mut left = self.side_at_schema(positive, target_schema, self.zero_threshold);
            if kahan {
                let c = compensation.as_deref_mut().expect("Kahan compensation");
                let mut comp = side_map(c, positive);
                let (right, right_comp) =
                    other.side_at_schema_kahan(positive, target_schema, self.zero_threshold);
                kahan_merge(&mut left, &mut comp, &right, &right_comp, subtract);
                set_side(self, positive, &left);
                set_side(c, positive, &comp);
            } else {
                let right = other.side_at_schema(positive, target_schema, self.zero_threshold);
                merge(&mut left, &right, subtract);
                set_side(self, positive, &left);
            }
        }
        Ok(outcome)
    }

    /// Whether `other` has exactly this histogram's exponential layout:
    /// the schema, the zero threshold, the spans, and no bucket reaching
    /// into the zero bucket. Reconciling two such histograms moves no
    /// count from one bucket to another, so [`Self::combine_aligned`] and
    /// the comparison in [`Self::detect_reset`] can walk the two bucket
    /// vectors side by side. Consecutive samples of one series nearly
    /// always share a layout, and that is where `rate` spends its time.
    fn shares_layout(&self, other: &Self) -> bool {
        self.schema == other.schema
            && self.zero_threshold == other.zero_threshold
            && self.positive_spans == other.positive_spans
            && self.negative_spans == other.negative_spans
            && self.positive_buckets.len() == other.positive_buckets.len()
            && self.negative_buckets.len() == other.negative_buckets.len()
            && self.clears_zero_bucket()
    }

    /// Whether every bucket lies wholly outside the zero bucket, so that
    /// none is folded into the zero count when thresholds are reconciled.
    fn clears_zero_bucket(&self) -> bool {
        let clears = |spans: &[Span]| {
            let mut index = 0i64;
            spans.iter().all(|span| {
                index += i64::from(span.offset);
                let first = index;
                index += i64::from(span.length);
                // A bucket's lower bound is the bound of the index below.
                span.length == 0
                    || i32::try_from(first - 1).is_ok_and(|below| {
                        crate::buckets::exponential_bound(below, self.schema) >= self.zero_threshold
                    })
            })
        };
        self.zero_threshold == 0.0 || (clears(&self.positive_spans) && clears(&self.negative_spans))
    }

    /// [`Self::combine`] for two histograms that [`Self::shares_layout`]:
    /// the same additions on the same pairs, without the maps that find
    /// the pairs when layouts differ.
    fn combine_aligned(&mut self, other: &Self, subtract: bool, compensation: Option<&mut Self>) {
        let signed = |value: f64| if subtract { -value } else { value };
        let Some(c) = compensation else {
            self.zero_count += signed(other.zero_count);
            let sides = [
                (&mut self.positive_buckets, &other.positive_buckets),
                (&mut self.negative_buckets, &other.negative_buckets),
            ];
            for (buckets, others) in sides {
                for (bucket, other) in buckets.iter_mut().zip(others) {
                    *bucket += signed(*other);
                }
            }
            return;
        };
        // Each count and then the other's compensation, which is zero for
        // a plain histogram: the two steps `kahan_merge` takes.
        let add = |value: f64, sum: &mut f64, c: &mut f64| {
            (*sum, *c) = kahan_inc(value, *sum, *c);
            (*sum, *c) = kahan_inc(0.0, *sum, *c);
        };
        add(other.zero_count, &mut self.zero_count, &mut c.zero_count);
        let sides = [
            (
                &mut self.positive_buckets,
                &mut c.positive_buckets,
                &other.positive_buckets,
            ),
            (
                &mut self.negative_buckets,
                &mut c.negative_buckets,
                &other.negative_buckets,
            ),
        ];
        for (buckets, compensations, others) in sides {
            for ((bucket, c), other) in buckets.iter_mut().zip(compensations).zip(others) {
                add(signed(*other), bucket, c);
            }
        }
    }

    fn combine_custom(
        &mut self,
        other: &Self,
        subtract: bool,
        kahan: bool,
        compensation: Option<&mut Self>,
    ) {
        let bounds = intersect_bounds(&self.custom_values, &other.custom_values);
        if kahan {
            let c = compensation.expect("Kahan compensation");
            let (mut left, mut comp) = remap_custom_kahan(self, Some(c), &bounds);
            let (right, right_comp) = remap_custom_kahan(other, None, &bounds);
            kahan_merge(&mut left, &mut comp, &right, &right_comp, subtract);
            set_side(self, true, &left);
            set_side(c, true, &comp);
            c.custom_values = bounds.clone();
        } else {
            let mut left = remap_custom(self, &bounds);
            let right = remap_custom(other, &bounds);
            merge(&mut left, &right, subtract);
            set_side(self, true, &left);
        }
        self.custom_values = bounds;
    }

    fn reconcile_zero(
        &mut self,
        other: &Self,
        subtract: bool,
        kahan: bool,
        mut compensation: Option<&mut Self>,
    ) {
        let mut other_threshold = other.zero_threshold;
        let mut other_zero = other.zero_count;
        let mut other_c_zero = 0.0;
        while other_threshold != self.zero_threshold {
            if self.zero_threshold > other_threshold {
                (other_zero, other_threshold, other_c_zero) =
                    other.zero_count_for_threshold(self.zero_threshold);
            }
            if other_threshold > self.zero_threshold {
                if let Some(c) = compensation.as_deref_mut() {
                    let (zero, threshold, c_zero) =
                        self.zero_count_for_threshold_kahan(c, other_threshold);
                    self.zero_count = zero;
                    self.zero_threshold = threshold;
                    c.zero_count = c_zero;
                    c.zero_threshold = threshold;
                    trim_threshold(c);
                } else {
                    (self.zero_count, self.zero_threshold, _) =
                        self.zero_count_for_threshold(other_threshold);
                }
                trim_threshold(self);
            }
        }
        if kahan {
            let c = compensation.expect("Kahan compensation");
            (self.zero_count, c.zero_count) = kahan_inc(other_zero, self.zero_count, c.zero_count);
            (self.zero_count, c.zero_count) =
                kahan_inc(other_c_zero, self.zero_count, c.zero_count);
        } else if subtract {
            self.zero_count -= other_zero;
        } else {
            self.zero_count += other_zero;
        }
    }

    fn zero_count_for_threshold(&self, mut threshold: f64) -> (f64, f64, f64) {
        loop {
            let (mut count, mut compensation) = (self.zero_count, 0.0);
            for bucket in self.positive_buckets_iter() {
                if bucket.lower >= threshold {
                    break;
                }
                (count, compensation) = kahan_inc(bucket.count, count, compensation);
                if bucket.upper > threshold && bucket.count != 0.0 {
                    threshold = bucket.upper;
                }
            }
            let starting_threshold = threshold;
            for bucket in self.negative_buckets_iter() {
                if bucket.upper <= -threshold {
                    break;
                }
                (count, compensation) = kahan_inc(bucket.count, count, compensation);
                if bucket.lower < -threshold && bucket.count != 0.0 {
                    threshold = -bucket.lower;
                    break;
                }
            }
            if threshold == starting_threshold {
                return (count, threshold, compensation);
            }
        }
    }

    fn side_at_schema(&self, positive: bool, schema: i32, threshold: f64) -> BTreeMap<i32, f64> {
        let mut result = BTreeMap::new();
        for (index, count) in side_map(self, positive) {
            if crate::buckets::exponential_bound(index, self.schema) <= threshold {
                continue;
            }
            let target = target_index(index, self.schema, schema);
            *result.entry(target).or_default() += count;
        }
        result
    }

    fn side_at_schema_kahan(
        &self,
        positive: bool,
        schema: i32,
        threshold: f64,
    ) -> (BTreeMap<i32, f64>, BTreeMap<i32, f64>) {
        let mut result = BTreeMap::new();
        let mut compensation = BTreeMap::new();
        for (index, count) in side_map(self, positive) {
            if crate::buckets::exponential_bound(index, self.schema) <= threshold {
                continue;
            }
            let target = target_index(index, self.schema, schema);
            let (sum, c) = kahan_inc(
                count,
                result.get(&target).copied().unwrap_or(0.0),
                compensation.get(&target).copied().unwrap_or(0.0),
            );
            result.insert(target, sum);
            compensation.insert(target, c);
        }
        (result, compensation)
    }

    fn zero_count_for_threshold_kahan(
        &self,
        compensation: &Self,
        mut threshold: f64,
    ) -> (f64, f64, f64) {
        let positive_compensation = side_map(compensation, true);
        let negative_compensation = side_map(compensation, false);
        loop {
            let (mut count, mut c) = (self.zero_count, compensation.zero_count);
            for bucket in self.positive_buckets_iter() {
                if bucket.lower >= threshold {
                    break;
                }
                (count, c) = kahan_inc(bucket.count, count, c);
                (count, c) = kahan_inc(
                    positive_compensation
                        .get(&bucket.index)
                        .copied()
                        .unwrap_or(0.0),
                    count,
                    c,
                );
                if bucket.upper > threshold && bucket.count != 0.0 {
                    threshold = bucket.upper;
                }
            }
            let starting_threshold = threshold;
            for bucket in self.negative_buckets_iter() {
                if bucket.upper <= -threshold {
                    break;
                }
                (count, c) = kahan_inc(bucket.count, count, c);
                (count, c) = kahan_inc(
                    negative_compensation
                        .get(&bucket.index)
                        .copied()
                        .unwrap_or(0.0),
                    count,
                    c,
                );
                if bucket.lower < -threshold && bucket.count != 0.0 {
                    threshold = -bucket.lower;
                    break;
                }
            }
            if threshold == starting_threshold {
                return (count, threshold, c);
            }
        }
    }

    fn compensation_histogram(&self) -> Self {
        Self {
            counter_reset_hint: self.counter_reset_hint,
            schema: self.schema,
            zero_threshold: self.zero_threshold,
            positive_spans: self.positive_spans.clone(),
            positive_buckets: vec![0.0; self.positive_buckets.len()],
            negative_spans: self.negative_spans.clone(),
            negative_buckets: vec![0.0; self.negative_buckets.len()],
            custom_values: self.custom_values.clone(),
            ..Self::default()
        }
    }

    // Prometheus adjustCounterReset: only an explicit reset/no-reset conflict
    // warrants an annotation; unknown and gauge hints are not collisions.
    fn adjust_reset_hint(&mut self, other: &Self) -> bool {
        use CounterResetHint::*;
        let (hint, collision) = match (self.counter_reset_hint, other.counter_reset_hint) {
            (a, b) if a == b => (a, false),
            (Gauge, _) | (_, Gauge) => (Gauge, false),
            (Unknown, _) | (_, Unknown) => (Unknown, false),
            _ => (Unknown, true),
        };
        self.counter_reset_hint = hint;
        collision
    }
}

fn reduce_histogram_pair(
    histogram: &mut FloatHistogram,
    compensation: &mut FloatHistogram,
    schema: i32,
) {
    let old_schema = histogram.schema;
    for positive in [true, false] {
        let values = side_map(histogram, positive);
        let comp = side_map(compensation, positive);
        let mut reduced = BTreeMap::new();
        let mut reduced_comp = BTreeMap::new();
        for (index, value) in values {
            let target = target_index(index, old_schema, schema);
            let (sum, c) = kahan_inc(
                value,
                reduced.get(&target).copied().unwrap_or(0.0),
                reduced_comp.get(&target).copied().unwrap_or(0.0),
            );
            let (sum, c) = kahan_inc(comp.get(&index).copied().unwrap_or(0.0), sum, c);
            reduced.insert(target, sum);
            reduced_comp.insert(target, c);
        }
        set_side(histogram, positive, &reduced);
        set_side(compensation, positive, &reduced_comp);
    }
    histogram.schema = schema;
    compensation.schema = schema;
}

fn trim_threshold(histogram: &mut FloatHistogram) {
    for positive in [true, false] {
        let retained = side_map(histogram, positive)
            .into_iter()
            .filter(|(index, _)| {
                crate::buckets::exponential_bound(*index, histogram.schema)
                    > histogram.zero_threshold
            })
            .collect();
        set_side(histogram, positive, &retained);
    }
}

fn remap_custom(histogram: &FloatHistogram, bounds: &[f64]) -> BTreeMap<i32, f64> {
    let mut target = BTreeMap::new();
    for (index, count) in side_map(histogram, true) {
        let source_bound = histogram
            .custom_values
            .get(index as usize)
            .copied()
            .unwrap_or(f64::INFINITY);
        let target_index = bounds.partition_point(|bound| *bound < source_bound) as i32;
        *target.entry(target_index).or_default() += count;
    }
    target
}

fn remap_custom_kahan(
    histogram: &FloatHistogram,
    compensation: Option<&FloatHistogram>,
    bounds: &[f64],
) -> (BTreeMap<i32, f64>, BTreeMap<i32, f64>) {
    let source_compensation = compensation.map(|c| side_map(c, true));
    let mut target = BTreeMap::new();
    let mut target_compensation = BTreeMap::new();
    for (index, count) in side_map(histogram, true) {
        let source_bound = histogram
            .custom_values
            .get(index as usize)
            .copied()
            .unwrap_or(f64::INFINITY);
        let target_index = bounds.partition_point(|bound| *bound < source_bound) as i32;
        let (sum, c) = kahan_inc(
            count,
            target.get(&target_index).copied().unwrap_or(0.0),
            target_compensation
                .get(&target_index)
                .copied()
                .unwrap_or(0.0),
        );
        let (sum, c) = kahan_inc(
            source_compensation
                .as_ref()
                .and_then(|values| values.get(&index))
                .copied()
                .unwrap_or(0.0),
            sum,
            c,
        );
        target.insert(target_index, sum);
        target_compensation.insert(target_index, c);
    }
    (target, target_compensation)
}

fn intersect_bounds(left: &[f64], right: &[f64]) -> Vec<f64> {
    let mut result = Vec::with_capacity(left.len().min(right.len()));
    let (mut i, mut j) = (0, 0);
    while i < left.len() && j < right.len() {
        if left[i] == right[j] {
            result.push(left[i]);
            i += 1;
            j += 1;
        } else if left[i] < right[j] {
            i += 1;
        } else {
            j += 1;
        }
    }
    result
}

fn reduce_side(
    spans: &[Span],
    buckets: &[f64],
    origin_schema: i32,
    target_schema: i32,
) -> (Vec<Span>, Vec<f64>) {
    let mut reduced = BTreeMap::new();
    for (index, count) in expand(spans, buckets) {
        *reduced
            .entry(target_index(index, origin_schema, target_schema))
            .or_default() += count;
    }
    encode(&reduced)
}

fn target_index(index: i32, origin_schema: i32, target_schema: i32) -> i32 {
    let index = i64::from(index) - 1;
    let shift = i64::from(origin_schema) - i64::from(target_schema);
    let shifted = if shift >= i64::from(i64::BITS - 1) {
        if index < 0 {
            -1
        } else {
            0
        }
    } else {
        index >> shift
    };
    i32::try_from(shifted + 1).expect("reduced histogram bucket index")
}

fn merge(left: &mut BTreeMap<i32, f64>, right: &BTreeMap<i32, f64>, subtract: bool) {
    for (&index, &value) in right {
        *left.entry(index).or_default() += if subtract { -value } else { value };
    }
}

fn kahan_merge(
    left: &mut BTreeMap<i32, f64>,
    compensation: &mut BTreeMap<i32, f64>,
    right: &BTreeMap<i32, f64>,
    right_compensation: &BTreeMap<i32, f64>,
    subtract: bool,
) {
    for (&index, &value) in right {
        let (sum, c) = kahan_inc(
            if subtract { -value } else { value },
            left.get(&index).copied().unwrap_or(0.0),
            compensation.get(&index).copied().unwrap_or(0.0),
        );
        let (sum, c) = kahan_inc(
            if subtract {
                -right_compensation.get(&index).copied().unwrap_or(0.0)
            } else {
                right_compensation.get(&index).copied().unwrap_or(0.0)
            },
            sum,
            c,
        );
        left.insert(index, sum);
        compensation.insert(index, c);
    }
}

pub(crate) fn kahan_inc(value: f64, sum: f64, mut compensation: f64) -> (f64, f64) {
    let total = sum + value;
    if total.is_infinite() {
        compensation = 0.0;
    } else if sum.abs() >= value.abs() {
        compensation += (sum - total) + value;
    } else {
        compensation += (value - total) + sum;
    }
    (total, compensation)
}

pub(crate) fn side_map(histogram: &FloatHistogram, positive: bool) -> BTreeMap<i32, f64> {
    if positive {
        expand(&histogram.positive_spans, &histogram.positive_buckets)
    } else {
        expand(&histogram.negative_spans, &histogram.negative_buckets)
    }
    .into_iter()
    .collect()
}

fn set_side(histogram: &mut FloatHistogram, positive: bool, values: &BTreeMap<i32, f64>) {
    let (spans, buckets) = encode(values);
    if positive {
        histogram.positive_spans = spans;
        histogram.positive_buckets = buckets;
    } else {
        histogram.negative_spans = spans;
        histogram.negative_buckets = buckets;
    }
}

fn expand(spans: &[Span], buckets: &[f64]) -> Vec<(i32, f64)> {
    let mut result = Vec::with_capacity(buckets.len());
    let mut index = 0i64;
    let mut bucket = 0usize;
    for span in spans {
        index += i64::from(span.offset);
        for _ in 0..span.length {
            result.push((
                i32::try_from(index).expect("validated histogram bucket index"),
                buckets[bucket],
            ));
            index += 1;
            bucket += 1;
        }
    }
    result
}

fn encode(values: &BTreeMap<i32, f64>) -> (Vec<Span>, Vec<f64>) {
    let mut spans: Vec<Span> = Vec::new();
    let mut buckets = Vec::with_capacity(values.len());
    let mut previous = None;
    for (&index, &count) in values {
        append_index(&mut spans, previous, index);
        buckets.push(count);
        previous = Some(index);
    }
    (spans, buckets)
}

#[inline]
fn append_index(spans: &mut Vec<Span>, previous: Option<i32>, index: i32) {
    if previous.is_some_and(|previous| i64::from(previous) + 1 == i64::from(index)) {
        spans.last_mut().expect("existing span").length += 1;
        return;
    }
    let mut offset = previous.map_or(i64::from(index), |previous| {
        i64::from(index) - i64::from(previous) - 1
    });
    while offset > i64::from(i32::MAX) {
        spans.push(Span {
            offset: i32::MAX,
            length: 0,
        });
        offset -= i64::from(i32::MAX);
    }
    spans.push(Span {
        offset: i32::try_from(offset).expect("representable histogram span offset"),
        length: 1,
    });
}

// Prometheus FloatHistogram.Compact(0). The span order already sorts the
// buckets; a BTreeMap only adds allocations and comparisons. Compact values
// into their existing buffer and rebuild spans as nonzero runs.
fn compact_side(spans: Vec<Span>, mut buckets: Vec<f64>) -> (Vec<Span>, Vec<f64>) {
    let mut compacted = Vec::with_capacity(spans.len());
    let (mut index, mut read, mut write) = (0i64, 0, 0);
    let mut previous = None;
    for span in spans {
        index += i64::from(span.offset);
        for _ in 0..span.length {
            let count = buckets[read];
            if count != 0.0 {
                let index = i32::try_from(index).expect("validated histogram bucket index");
                append_index(&mut compacted, previous, index);
                buckets[write] = count;
                write += 1;
                previous = Some(index);
            }
            read += 1;
            index += 1;
        }
    }
    buckets.truncate(write);
    (compacted, buckets)
}
