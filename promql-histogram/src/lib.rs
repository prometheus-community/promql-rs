// Licensed under the Apache License, Version 2.0.
// Portions derived from Prometheus, Copyright The Prometheus Authors.

//! Prometheus-compatible native histogram types and operations.
//!
//! This crate provides validation, traversal, arithmetic, statistics, and
//! trimming for exponential and custom-bucket histograms. Its behavior is
//! derived from Prometheus; see `UPSTREAM.md` for the pinned revision and
//! source files.

mod arithmetic;
mod buckets;
pub mod statistics;
mod trim;
mod validation;

pub use arithmetic::{ArithmeticOutcome, HistogramError, KahanSum};
pub use buckets::{AllBuckets, Bucket, BucketCursor, JsonBucket};
pub use trim::TrimDirection;
pub use validation::{HistogramRef, IngressValidation, SpanSequence, ValidationError};

pub const EXPONENTIAL_SCHEMA_MIN: i32 = -4;
pub const EXPONENTIAL_SCHEMA_MAX: i32 = 8;
pub const EXPONENTIAL_SCHEMA_MAX_RESERVED: i32 = 52;
pub const CUSTOM_SCHEMA: i32 = -53;

#[inline]
pub const fn is_exponential_schema(schema: i32) -> bool {
    schema >= EXPONENTIAL_SCHEMA_MIN && schema <= EXPONENTIAL_SCHEMA_MAX
}

#[inline]
pub const fn is_known_ingress_schema(schema: i32) -> bool {
    schema == CUSTOM_SCHEMA
        || schema >= EXPONENTIAL_SCHEMA_MIN && schema <= EXPONENTIAL_SCHEMA_MAX_RESERVED
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[repr(i32)]
pub enum CounterResetHint {
    #[default]
    Unknown = 0,
    Reset = 1,
    NotReset = 2,
    Gauge = 3,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Span {
    pub offset: i32,
    pub length: u32,
}

#[derive(Clone, Debug, Default)]
pub struct FloatHistogram {
    pub counter_reset_hint: CounterResetHint,
    pub schema: i32,
    pub count: f64,
    pub sum: f64,
    pub zero_threshold: f64,
    pub zero_count: f64,
    pub negative_spans: Vec<Span>,
    pub negative_buckets: Vec<f64>,
    pub positive_spans: Vec<Span>,
    pub positive_buckets: Vec<f64>,
    pub custom_values: Vec<f64>,
}

impl FloatHistogram {
    #[inline]
    pub fn uses_custom_buckets(&self) -> bool {
        self.schema == CUSTOM_SCHEMA
    }

    /// Returns whether two histograms have exactly the same Prometheus value.
    ///
    /// Counter reset hints are metadata and are intentionally ignored. Float
    /// counts, sums, and bucket values are compared by bit pattern.
    pub fn equals(&self, other: &Self) -> bool {
        if self.schema != other.schema
            || self.count.to_bits() != other.count.to_bits()
            || self.sum.to_bits() != other.sum.to_bits()
        {
            return false;
        }

        if self.uses_custom_buckets()
            && !float_values_match(&self.custom_values, &other.custom_values, false)
        {
            return false;
        }

        if self.zero_threshold != other.zero_threshold
            || self.zero_count.to_bits() != other.zero_count.to_bits()
        {
            return false;
        }

        spans_match(&self.negative_spans, &other.negative_spans)
            && float_values_match(&self.negative_buckets, &other.negative_buckets, true)
            && spans_match(&self.positive_spans, &other.positive_spans)
            && float_values_match(&self.positive_buckets, &other.positive_buckets, true)
    }
}

impl PartialEq for FloatHistogram {
    fn eq(&self, other: &Self) -> bool {
        self.equals(other)
    }
}

fn float_values_match(left: &[f64], right: &[f64], bitwise: bool) -> bool {
    left.len() == right.len()
        && left.iter().zip(right).all(|(left, right)| {
            if bitwise {
                left.to_bits() == right.to_bits()
            } else {
                left == right
            }
        })
}

fn spans_match(left: &[Span], right: &[Span]) -> bool {
    fn normalized(spans: &[Span]) -> impl Iterator<Item = (i64, u32)> + '_ {
        let mut pending_offset = 0i64;
        spans.iter().filter_map(move |span| {
            pending_offset += i64::from(span.offset);
            if span.length == 0 {
                None
            } else {
                let result = (pending_offset, span.length);
                pending_offset = 0;
                Some(result)
            }
        })
    }

    normalized(left).eq(normalized(right))
}
