// Licensed under the Apache License, Version 2.0.
// Derived from Prometheus model/histogram/generic.go and float_histogram.go.

use thiserror::Error;

use crate::{is_known_ingress_schema, FloatHistogram, Span, CUSTOM_SCHEMA, EXPONENTIAL_SCHEMA_MAX};

#[derive(Clone, Copy, Debug)]
pub struct HistogramRef<'a, S = &'a [Span]> {
    pub schema: i32,
    pub count: f64,
    pub zero_threshold: f64,
    pub zero_count: f64,
    pub negative_spans: S,
    pub negative_buckets: &'a [f64],
    pub positive_spans: S,
    pub positive_buckets: &'a [f64],
    pub custom_values: &'a [f64],
}

/// Borrowed access to histogram spans, including split-column storage.
pub trait SpanSequence: Copy {
    fn span_count(self) -> usize;
    fn span_at(self, index: usize) -> Option<Span>;
}

impl SpanSequence for &[Span] {
    fn span_count(self) -> usize {
        self.len()
    }

    fn span_at(self, index: usize) -> Option<Span> {
        self.get(index).copied()
    }
}

#[derive(Debug, Error, PartialEq)]
pub enum ValidationError {
    #[error("invalid histogram schema {0}")]
    InvalidSchema(i32),
    #[error("span {span} has a negative offset")]
    NegativeSpanOffset { span: usize },
    #[error("span {span} is malformed")]
    MalformedSpan { span: usize },
    #[error("spans describe {spans} buckets but {buckets} were supplied")]
    SpanBucketMismatch { spans: usize, buckets: usize },
    #[error("histogram count is negative")]
    NegativeCount,
    #[error("histogram zero threshold is NaN")]
    NaNZeroThreshold,
    #[error("span {span} reaches an unrepresentable bucket index")]
    UnrepresentableSpanIndex { span: usize },
    #[error("bucket {bucket} has a negative count")]
    NegativeBucketCount { bucket: usize },
    #[error("custom bucket bounds must be strictly increasing and not NaN")]
    InvalidCustomBounds,
    #[error("the final custom bucket bound must not be +Inf")]
    InfiniteCustomBound,
    #[error("custom bucket bounds do not cover all spans")]
    InsufficientCustomBounds,
    #[error("custom histograms must have zero threshold and zero count of zero")]
    CustomZeroBucket,
    #[error("custom histograms must not have negative spans or buckets")]
    CustomNegativeBuckets,
    #[error("exponential histograms must not have custom bounds")]
    ExponentialCustomBounds,
}

impl FloatHistogram {
    /// Validates untrusted ingress data. Arithmetic deliberately does not call this.
    pub fn validate_ingress(&self) -> Result<(), ValidationError> {
        HistogramRef::from(self).validate_ingress()
    }

    /// Establishes the persisted contract at ingress, including reserved-schema reduction.
    pub fn normalize_ingress(&mut self) -> Result<(), ValidationError> {
        self.validate_ingress()?;
        if self.schema > EXPONENTIAL_SCHEMA_MAX {
            self.reduce_resolution_in_place(EXPONENTIAL_SCHEMA_MAX);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ValidationPhase {
    Start,
    CustomBounds,
    PositiveSpans,
    NegativeSpans,
    NegativeBuckets,
    PositiveBuckets,
    Complete,
}

/// Resumable validation state for an untrusted histogram.
#[derive(Clone, Copy, Debug)]
pub struct IngressValidation {
    phase: ValidationPhase,
    index: usize,
    described: usize,
    next_index: i64,
    previous_bound: f64,
    custom: bool,
}

impl Default for IngressValidation {
    fn default() -> Self {
        Self::new()
    }
}

impl IngressValidation {
    pub const fn new() -> Self {
        Self {
            phase: ValidationPhase::Start,
            index: 0,
            described: 0,
            next_index: 0,
            previous_bound: f64::NEG_INFINITY,
            custom: false,
        }
    }

    /// Validates at most `maximum` span, bucket, or custom-bound entries.
    ///
    /// The same histogram must be supplied until validation completes. The
    /// returned tuple contains completion status and entries inspected.
    pub fn validate_chunk<S: SpanSequence>(
        &mut self,
        histogram: HistogramRef<'_, S>,
        mut maximum: usize,
    ) -> Result<(bool, usize), ValidationError> {
        let initial_maximum = maximum;
        loop {
            match self.phase {
                ValidationPhase::Start => {
                    if !is_known_ingress_schema(histogram.schema) {
                        return Err(ValidationError::InvalidSchema(histogram.schema));
                    }
                    if histogram.count < 0.0 {
                        return Err(ValidationError::NegativeCount);
                    }
                    if histogram.zero_threshold.is_nan() {
                        return Err(ValidationError::NaNZeroThreshold);
                    }
                    self.custom = histogram.schema == CUSTOM_SCHEMA;
                    self.phase = if self.custom {
                        ValidationPhase::CustomBounds
                    } else {
                        ValidationPhase::PositiveSpans
                    };
                }
                ValidationPhase::CustomBounds => {
                    if self.index < histogram.custom_values.len() {
                        if maximum == 0 {
                            return Ok((false, initial_maximum));
                        }
                        let bound = histogram.custom_values[self.index];
                        if bound.is_nan() || self.index > 0 && bound <= self.previous_bound {
                            return Err(ValidationError::InvalidCustomBounds);
                        }
                        self.previous_bound = bound;
                        self.index += 1;
                        maximum -= 1;
                        continue;
                    }
                    if self.previous_bound == f64::INFINITY {
                        return Err(ValidationError::InfiniteCustomBound);
                    }
                    self.begin_entries(ValidationPhase::PositiveSpans);
                }
                ValidationPhase::PositiveSpans => {
                    if self.index < histogram.positive_spans.span_count() {
                        if maximum == 0 {
                            return Ok((false, initial_maximum - maximum));
                        }
                        self.validate_span(histogram.positive_spans, self.custom)?;
                        maximum -= 1;
                        continue;
                    }
                    self.finish_spans(histogram.positive_buckets.len())?;
                    if self.custom {
                        if usize::try_from(self.next_index)
                            .ok()
                            .is_none_or(|covered| covered > histogram.custom_values.len() + 1)
                        {
                            return Err(ValidationError::InsufficientCustomBounds);
                        }
                        if histogram.zero_count != 0.0 || histogram.zero_threshold != 0.0 {
                            return Err(ValidationError::CustomZeroBucket);
                        }
                        if histogram.negative_spans.span_count() != 0
                            || !histogram.negative_buckets.is_empty()
                        {
                            return Err(ValidationError::CustomNegativeBuckets);
                        }
                        self.begin_entries(ValidationPhase::PositiveBuckets);
                    } else {
                        self.begin_entries(ValidationPhase::NegativeSpans);
                    }
                }
                ValidationPhase::NegativeSpans => {
                    if self.index < histogram.negative_spans.span_count() {
                        if maximum == 0 {
                            return Ok((false, initial_maximum - maximum));
                        }
                        self.validate_span(histogram.negative_spans, false)?;
                        maximum -= 1;
                        continue;
                    }
                    self.finish_spans(histogram.negative_buckets.len())?;
                    self.begin_entries(ValidationPhase::NegativeBuckets);
                }
                ValidationPhase::NegativeBuckets => {
                    if self.index < histogram.negative_buckets.len() {
                        if maximum == 0 {
                            return Ok((false, initial_maximum - maximum));
                        }
                        validate_bucket(histogram.negative_buckets[self.index], self.index)?;
                        self.index += 1;
                        maximum -= 1;
                        continue;
                    }
                    if histogram.zero_count < 0.0 {
                        return Err(ValidationError::NegativeBucketCount { bucket: 0 });
                    }
                    if !histogram.custom_values.is_empty() {
                        return Err(ValidationError::ExponentialCustomBounds);
                    }
                    self.begin_entries(ValidationPhase::PositiveBuckets);
                }
                ValidationPhase::PositiveBuckets => {
                    if self.index < histogram.positive_buckets.len() {
                        if maximum == 0 {
                            return Ok((false, initial_maximum - maximum));
                        }
                        validate_bucket(histogram.positive_buckets[self.index], self.index)?;
                        self.index += 1;
                        maximum -= 1;
                        continue;
                    }
                    self.phase = ValidationPhase::Complete;
                }
                ValidationPhase::Complete => {
                    return Ok((true, initial_maximum - maximum));
                }
            }
        }
    }

    fn begin_entries(&mut self, phase: ValidationPhase) {
        self.phase = phase;
        self.index = 0;
        self.described = 0;
        self.next_index = 0;
    }

    fn validate_span<S: SpanSequence>(
        &mut self,
        spans: S,
        first_offset_must_be_nonnegative: bool,
    ) -> Result<(), ValidationError> {
        let index = self.index;
        let span = spans
            .span_at(index)
            .ok_or(ValidationError::MalformedSpan { span: index })?;
        if (index > 0 || first_offset_must_be_nonnegative) && span.offset < 0 {
            return Err(ValidationError::NegativeSpanOffset { span: index });
        }
        self.next_index = self
            .next_index
            .checked_add(i64::from(span.offset))
            .ok_or(ValidationError::UnrepresentableSpanIndex { span: index })?;
        if span.length > 0 {
            let last_index = self
                .next_index
                .checked_add(i64::from(span.length) - 1)
                .ok_or(ValidationError::UnrepresentableSpanIndex { span: index })?;
            if self.next_index < i64::from(i32::MIN) || last_index > i64::from(i32::MAX) {
                return Err(ValidationError::UnrepresentableSpanIndex { span: index });
            }
        }
        self.next_index = self
            .next_index
            .checked_add(i64::from(span.length))
            .ok_or(ValidationError::UnrepresentableSpanIndex { span: index })?;
        self.described = self
            .described
            .checked_add(span.length as usize)
            .ok_or(ValidationError::UnrepresentableSpanIndex { span: index })?;
        self.index += 1;
        Ok(())
    }

    fn finish_spans(&self, bucket_count: usize) -> Result<(), ValidationError> {
        if self.described != bucket_count {
            return Err(ValidationError::SpanBucketMismatch {
                spans: self.described,
                buckets: bucket_count,
            });
        }
        Ok(())
    }
}

impl<S: SpanSequence> HistogramRef<'_, S> {
    /// Validates borrowed, untrusted histogram storage without requiring an owned histogram.
    pub fn validate_ingress(self) -> Result<(), ValidationError> {
        let mut validation = IngressValidation::new();
        loop {
            let (complete, _) = validation.validate_chunk(self, usize::MAX)?;
            if complete {
                return Ok(());
            }
        }
    }
}

impl<'a> From<&'a FloatHistogram> for HistogramRef<'a> {
    fn from(histogram: &'a FloatHistogram) -> Self {
        Self {
            schema: histogram.schema,
            count: histogram.count,
            zero_threshold: histogram.zero_threshold,
            zero_count: histogram.zero_count,
            negative_spans: &histogram.negative_spans,
            negative_buckets: &histogram.negative_buckets,
            positive_spans: &histogram.positive_spans,
            positive_buckets: &histogram.positive_buckets,
            custom_values: &histogram.custom_values,
        }
    }
}

fn validate_bucket(count: f64, bucket: usize) -> Result<(), ValidationError> {
    if count < 0.0 {
        return Err(ValidationError::NegativeBucketCount { bucket });
    }
    Ok(())
}
