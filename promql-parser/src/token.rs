//! Token types. Mirrors `ItemType` in `upstream/lex.go` and the `%token`
//! blocks of `upstream/generated_parser.y`. Variant names track upstream
//! (lowered to `CamelCase`) so diffs against future upstream revisions
//! are mechanical.
//!
//! Variants are declared for completeness ahead of the custom lexer
//! that will populate them, ported from upstream `lex.go`.

use std::fmt;

use crate::posrange::{Pos, PositionRange};

/// A single lexed token. Mirrors upstream `Item`.
#[derive(Debug, Clone, PartialEq)]
pub struct Item {
    pub typ: ItemType,
    pub pos: Pos,
    pub val: String,
}

impl Item {
    pub fn new(typ: ItemType, pos: Pos, val: impl Into<String>) -> Self {
        Self {
            typ,
            pos,
            val: val.into(),
        }
    }

    pub fn position_range(&self) -> PositionRange {
        PositionRange::at(self.pos, self.val.len())
    }
}

/// Token kind. Every enum variant corresponds to a `%token` entry in
/// `upstream/generated_parser.y`. Ordering is grouped to match upstream's
/// grouping comments.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ItemType {
    // Framing
    Error,
    Eof,
    Comment,

    // Identifiers and literals
    Identifier,
    MetricIdentifier,
    Number,
    Duration,
    String,

    // Structural punctuation
    LeftBrace,
    RightBrace,
    LeftBracket,
    RightBracket,
    LeftParen,
    RightParen,
    Comma,
    Colon,
    Semicolon,
    Blank,
    Times,
    Space,
    OpenHist,
    CloseHist,

    // Operators (upstream operatorsStart..operatorsEnd)
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Pow,
    Atan2,
    Eql,
    EqlC,
    EqlRegex,
    NeqRegex,
    Neq,
    Lss,
    Lte,
    Gtr,
    Gte,
    Land,
    Lor,
    Lunless,
    At,
    TrimUpper,
    TrimLower,

    // Aggregators
    Avg,
    Bottomk,
    Count,
    CountValues,
    Group,
    Max,
    Min,
    Quantile,
    Stddev,
    Stdvar,
    Sum,
    Topk,
    Limitk,
    LimitRatio,

    // Keywords
    Bool,
    By,
    GroupLeft,
    GroupRight,
    Ignoring,
    Offset,
    On,
    Without,
    Fill,
    FillLeft,
    FillRight,
    Smoothed,
    Anchored,

    // Preprocessors / time anchors
    Start,
    End,
    Step,
    Range,

    // Counter-reset hints (series-description grammar)
    UnknownCounterReset,
    CounterReset,
    NotCounterReset,
    GaugeType,

    // Histogram descriptors (series-description grammar)
    SumDesc,
    CountDesc,
    SchemaDesc,
    OffsetDesc,
    NegativeOffsetDesc,
    BucketsDesc,
    NegativeBucketsDesc,
    ZeroBucketDesc,
    ZeroBucketWidthDesc,
    CustomValuesDesc,
    CounterResetHintDesc,

    // Virtual start symbols. Not emitted by the lexer; the parser entry
    // point prepends one to select a start rule.
    StartMetric,
    StartSeriesDescription,
    StartExpression,
    StartMetricSelector,
}

impl fmt::Display for ItemType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            ItemType::Error => "error",
            ItemType::Eof => "end of input",
            ItemType::Comment => "comment",
            ItemType::Identifier => "identifier",
            ItemType::MetricIdentifier => "metric identifier",
            ItemType::Number => "number",
            ItemType::Duration => "duration",
            ItemType::String => "string",
            ItemType::LeftBrace => "\"{\"",
            ItemType::RightBrace => "\"}\"",
            ItemType::LeftBracket => "\"[\"",
            ItemType::RightBracket => "\"]\"",
            ItemType::LeftParen => "\"(\"",
            ItemType::RightParen => "\")\"",
            ItemType::Comma => "\",\"",
            ItemType::Colon => "\":\"",
            ItemType::Semicolon => "\";\"",
            ItemType::Blank => "\"_\"",
            ItemType::Times => "\"x\"",
            ItemType::Space => "space",
            ItemType::OpenHist => "\"{{\"",
            ItemType::CloseHist => "\"}}\"",
            ItemType::Add => "+",
            ItemType::Sub => "-",
            ItemType::Mul => "*",
            ItemType::Div => "/",
            ItemType::Mod => "%",
            ItemType::Pow => "^",
            ItemType::Atan2 => "atan2",
            ItemType::Eql => "=",
            ItemType::EqlC => "==",
            ItemType::EqlRegex => "=~",
            ItemType::NeqRegex => "!~",
            ItemType::Neq => "!=",
            ItemType::Lss => "<",
            ItemType::Lte => "<=",
            ItemType::Gtr => ">",
            ItemType::Gte => ">=",
            ItemType::Land => "and",
            ItemType::Lor => "or",
            ItemType::Lunless => "unless",
            ItemType::At => "@",
            ItemType::TrimUpper => "</",
            ItemType::TrimLower => ">/",
            ItemType::Avg => "avg",
            ItemType::Bottomk => "bottomk",
            ItemType::Count => "count",
            ItemType::CountValues => "count_values",
            ItemType::Group => "group",
            ItemType::Max => "max",
            ItemType::Min => "min",
            ItemType::Quantile => "quantile",
            ItemType::Stddev => "stddev",
            ItemType::Stdvar => "stdvar",
            ItemType::Sum => "sum",
            ItemType::Topk => "topk",
            ItemType::Limitk => "limitk",
            ItemType::LimitRatio => "limit_ratio",
            ItemType::Bool => "bool",
            ItemType::By => "by",
            ItemType::GroupLeft => "group_left",
            ItemType::GroupRight => "group_right",
            ItemType::Ignoring => "ignoring",
            ItemType::Offset => "offset",
            ItemType::On => "on",
            ItemType::Without => "without",
            ItemType::Fill => "fill",
            ItemType::FillLeft => "fill_left",
            ItemType::FillRight => "fill_right",
            ItemType::Smoothed => "smoothed",
            ItemType::Anchored => "anchored",
            ItemType::Start => "start",
            ItemType::End => "end",
            ItemType::Step => "step",
            ItemType::Range => "range",
            ItemType::UnknownCounterReset => "unknown_counter_reset",
            ItemType::CounterReset => "counter_reset",
            ItemType::NotCounterReset => "not_counter_reset",
            ItemType::GaugeType => "gauge_type",
            ItemType::SumDesc => "sum",
            ItemType::CountDesc => "count",
            ItemType::SchemaDesc => "schema",
            ItemType::OffsetDesc => "offset",
            ItemType::NegativeOffsetDesc => "n_offset",
            ItemType::BucketsDesc => "buckets",
            ItemType::NegativeBucketsDesc => "n_buckets",
            ItemType::ZeroBucketDesc => "z_bucket",
            ItemType::ZeroBucketWidthDesc => "z_bucket_w",
            ItemType::CustomValuesDesc => "custom_values",
            ItemType::CounterResetHintDesc => "counter_reset_hint",
            ItemType::StartMetric
            | ItemType::StartSeriesDescription
            | ItemType::StartExpression
            | ItemType::StartMetricSelector => "<start>",
        };
        f.write_str(s)
    }
}
