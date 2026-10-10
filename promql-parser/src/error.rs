//! Accumulated parse errors. Mirrors upstream's multi-error parse result:
//! actions attach errors to the parser context and continue, and the
//! caller sees the AST along with the error list.

use crate::posrange::PositionRange;

#[derive(Debug, Clone, thiserror::Error)]
#[error("{message} at {}..{}", range.start, range.end)]
pub struct ParseError {
    pub message: String,
    pub range: PositionRange,
    /// The `--enable-feature` value whose gate refused the query, for an
    /// error a feature gate raised. Not in upstream's `ParseErr`; there the
    /// message is the only trace, and a caller sorting queries by the flag
    /// they need would have to match on wording that upstream rephrases.
    pub gate: Option<&'static str>,
}

impl ParseError {
    pub fn new(message: impl Into<String>, range: PositionRange) -> Self {
        Self {
            message: message.into(),
            range,
            gate: None,
        }
    }
}

#[derive(Debug, Clone, thiserror::Error)]
#[error("PromQL parse errors: {}", format_all(.0))]
pub struct ParseErrors(Vec<ParseError>);

impl ParseErrors {
    pub(crate) fn new(errors: Vec<ParseError>) -> Self {
        Self(errors)
    }

    pub fn iter(&self) -> std::slice::Iter<'_, ParseError> {
        self.0.iter()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn into_vec(self) -> Vec<ParseError> {
        self.0
    }
}

impl IntoIterator for ParseErrors {
    type Item = ParseError;
    type IntoIter = std::vec::IntoIter<ParseError>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<'a> IntoIterator for &'a ParseErrors {
    type Item = &'a ParseError;
    type IntoIter = std::slice::Iter<'a, ParseError>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

fn format_all(errs: &[ParseError]) -> String {
    errs.iter()
        .map(|e| e.to_string())
        .collect::<Vec<_>>()
        .join("; ")
}

impl From<ParseError> for ParseErrors {
    fn from(e: ParseError) -> Self {
        Self(vec![e])
    }
}
