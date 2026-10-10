//! The per-parse state grammar actions share. Upstream's `parser`
//! struct, which actions reach as `yylex.(*parser)`: its `options` field
//! and the `addParseErrf` error accumulator.
//!
//! grmtools hands an action only its own symbols and the lexer, so the
//! state travels as the grammar's `%parse-param`, passed to every action
//! as `p`. Errors go in a `RefCell` because the parameter is shared, not
//! mutable, and because a gate reports and keeps parsing: upstream's
//! action `addParseErrf`s and still builds the node, so one parse can
//! report several gates.

use std::cell::RefCell;

use crate::ast::Expr;
use crate::error::{ParseError, ParseErrors};
use crate::options::ParserOptions;
use crate::posrange::PositionRange;

/// Upstream `parser`, minus the lexer state.
#[derive(Debug)]
pub struct ParserCtx {
    options: ParserOptions,
    errors: RefCell<Vec<ParseError>>,
}

impl ParserCtx {
    pub fn new(options: ParserOptions) -> Self {
        Self {
            options,
            errors: RefCell::new(Vec::new()),
        }
    }

    pub fn options(&self) -> &ParserOptions {
        &self.options
    }

    /// Upstream `addParseErrf`.
    pub fn add_parse_err(&self, range: PositionRange, message: impl Into<String>) {
        self.errors
            .borrow_mut()
            .push(ParseError::new(message, range));
    }

    /// `addParseErrf` from a feature gate: the error also names `flag`,
    /// the `--enable-feature` value that would have let the query through.
    pub fn add_gate_err(
        &self,
        flag: &'static str,
        range: PositionRange,
        message: impl Into<String>,
    ) {
        let mut err = ParseError::new(message, range);
        err.gate = Some(flag);
        self.errors.borrow_mut().push(err);
    }

    /// Upstream `experimentalDurationExpr`: grammar actions that build a
    /// duration expression call this with it, and it reports the
    /// expression when `ExperimentalDurationExpr` is off. Nothing calls it
    /// until the duration-expression productions are ported.
    #[allow(dead_code)]
    pub fn experimental_duration_expr(&self, e: &Expr) {
        if !self.options.experimental_duration_expr {
            self.add_gate_err(
                crate::options::DURATION_EXPR,
                e.position_range(),
                "experimental duration expression is not enabled",
            );
        }
    }

    pub(crate) fn has_errors(&self) -> bool {
        !self.errors.borrow().is_empty()
    }

    /// The accumulated errors, then `syntax` — the ones grmtools raised
    /// itself. Upstream interleaves them in the order they happened; an
    /// action's error comes from a reduction, which needs the tokens up to
    /// and including its last symbol, so it precedes a syntax error that
    /// is found afterwards, and the common single-error case is unaffected.
    pub(crate) fn into_errors(self, syntax: Vec<ParseError>) -> ParseErrors {
        let mut all = self.errors.into_inner();
        all.extend(syntax);
        ParseErrors::new(all)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Nothing in the grammar calls the helper yet, so this is the only
    /// thing holding it to upstream's message and position.
    #[test]
    fn a_duration_expression_is_reported_unless_its_gate_is_on() {
        let e = crate::parse_expr("1 + 2").expect("parses");

        let off = ParserCtx::new(ParserOptions::default());
        off.experimental_duration_expr(&e);
        let got = off.into_errors(Vec::new()).into_vec();
        assert_eq!(got.len(), 1);
        assert_eq!(
            got[0].message,
            "experimental duration expression is not enabled"
        );
        assert_eq!(got[0].range, e.position_range());
        assert_eq!(got[0].gate, Some("promql-duration-expr"));

        let on = ParserCtx::new(ParserOptions {
            experimental_duration_expr: true,
            ..Default::default()
        });
        on.experimental_duration_expr(&e);
        assert!(!on.has_errors());
    }
}
