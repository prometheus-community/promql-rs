//! Scalar-typed expressions, folded to a value per step.
//!
//! A scalar in PromQL is not a constant: `time()` changes with the step.
//! But every scalar-typed expression this engine plans is a function of
//! the step alone — no series reaches it — so the whole of it can be
//! evaluated while planning, one `f64` per step, and needs no kernel and
//! no DataFusion expression of its own.
//!
//! `scalar(v)` is the exception, and the reason [`fold`] returns a
//! result rather than a value: it reads a vector, so it cannot be
//! answered here and is reported as the unsupported feature it is.
//! [`crate::to_scalar`] answers it when it is the whole of a query or the
//! argument of a call that takes a vector; as an operand of an operator,
//! or as a function's scalar argument, it still reaches this fold.
//!
//! The arithmetic is upstream's `scalarBinop` (`promql/engine.go:3224`
//! at 83962c35), comparisons included: between two scalars a comparison
//! is 1 or 0. Writing one without `bool` is a query error, raised by
//! [`crate::plan`]'s tree check before any of this runs, so a
//! comparison that reaches here always had the modifier.

use promql_parser::ast::{Call, Expr};
use promql_parser::token::ItemType;

use crate::error::EngineError;
use crate::plan::describe;

/// The value a scalar-typed expression takes at one step.
///
/// `ts_ms` is the step's timestamp, which only `time()` reads.
pub fn fold(expr: &Expr, ts_ms: i64) -> Result<f64, EngineError> {
    match expr {
        Expr::NumberLiteral(n) => Ok(n.val),
        Expr::Paren(p) => fold(&p.expr, ts_ms),
        Expr::StepInvariant(e) => fold(e, ts_ms),
        Expr::Unary(u) => {
            let v = fold(&u.expr, ts_ms)?;
            match u.op {
                ItemType::Sub => Ok(-v),
                ItemType::Add => Ok(v),
                op => Err(EngineError::Unsupported(format!(
                    "the unary {op} operator on a scalar"
                ))),
            }
        }
        Expr::Binary(b) => {
            let (lhs, rhs) = (fold(&b.lhs, ts_ms)?, fold(&b.rhs, ts_ms)?);
            binop(b.op, lhs, rhs)
        }
        Expr::Call(c) => call(c, ts_ms),
        // A duration expression is scalar-typed but its value comes from
        // the query's own step and range, which this fold does not see.
        other => Err(EngineError::Unsupported(describe(other))),
    }
}

/// A scalar-returning call at one step. Its own entry point because the
/// planner meets such a call as a `Call`, not as the `Expr` around it.
pub(crate) fn call(c: &Call, ts_ms: i64) -> Result<f64, EngineError> {
    // Before the name is matched, not after: `time(1)` takes no
    // arguments upstream and must say so rather than quietly answering
    // the time.
    crate::function::check_call(c)?;
    match c.func.name.as_str() {
        "time" => Ok(time(ts_ms)),
        "pi" => Ok(std::f64::consts::PI),
        name => Err(EngineError::Unsupported(format!("the {name} function"))),
    }
}

/// Upstream's `funcTime`: the step in seconds, as a float.
pub(crate) fn time(ts_ms: i64) -> f64 {
    ts_ms as f64 / 1000.0
}

/// Upstream's `scalarBinop`, which is [`crate::binary::Op`] with the
/// `bool` its comparisons always carry: between two scalars the modifier
/// is mandatory, checked by [`crate::plan`] before any of this runs, so
/// a comparison here is Go's `btos` and never filters.
///
/// Shared rather than written out again: `scalarBinop` and
/// `vectorElemBinop` agree operator for operator upstream, and one of
/// them here would drift.
fn binop(op: ItemType, lhs: f64, rhs: f64) -> Result<f64, EngineError> {
    match crate::binary::Op::from_token(op) {
        Some(op) => Ok(op
            .value(lhs, rhs, true)
            .expect("bool keeps every comparison")),
        // `and`, `or`, `unless`: upstream's parser rejects them between
        // scalars, so reaching here means our parser was laxer.
        None => Err(EngineError::Query(format!(
            "set operator {op} not allowed in binary scalar expression"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(query: &str, ts_ms: i64) -> f64 {
        fold(
            &promql_parser::parse_expr(query).expect("the query parses"),
            ts_ms,
        )
        .expect("the expression folds")
    }

    fn value(query: &str) -> f64 {
        at(query, 0)
    }

    /// The corpus's `literals.test`, which is what this exists for.
    #[test]
    fn the_literal_corpus_folds_to_its_expected_values() {
        assert_eq!(value("12.34e6"), 12_340_000.0);
        assert_eq!(value("12.34e-6"), 0.00001234);
        assert_eq!(value("1+1"), 2.0);
        assert_eq!(value("1-1"), 0.0);
        assert_eq!(value("1 - -1"), 2.0);
        assert_eq!(value(".2"), 0.2);
        assert_eq!(value("+0.2"), 0.2);
        assert_eq!(value("-0.2e-6"), -0.0000002);
        assert_eq!(value("+Inf"), f64::INFINITY);
        assert_eq!(value("-Inf"), f64::NEG_INFINITY);
        assert!(value("NaN").is_nan());
        assert_eq!(value("1 / 0"), f64::INFINITY);
        assert_eq!(value("((1) / (0))"), f64::INFINITY);
        assert_eq!(value("-1 / 0"), f64::NEG_INFINITY);
        assert!(value("0 / 0").is_nan());
        assert!(value("1 % 0").is_nan());
        assert_eq!(value("2."), 2.0);
    }

    /// Every arm of upstream's `scalarBinop`, including the sign rule
    /// `%` inherits from Go's `math.Mod`.
    #[test]
    fn the_arithmetic_is_upstreams_scalar_binop() {
        assert_eq!(value("3 * 4"), 12.0);
        assert_eq!(value("2 ^ 10"), 1024.0);
        assert_eq!(value("7 % 3"), 1.0);
        assert_eq!(value("-7 % 3"), -1.0);
        assert_eq!(value("7 % -3"), 1.0);
        // `atan2` is an operator, not a function, upstream and here.
        assert_eq!(value("1 atan2 1"), std::f64::consts::FRAC_PI_4);
    }

    #[test]
    fn a_comparison_between_scalars_is_one_or_zero() {
        for (query, want) in [
            ("1 == bool 1", 1.0),
            ("1 == bool 2", 0.0),
            ("1 != bool 2", 1.0),
            ("2 > bool 1", 1.0),
            ("1 > bool 2", 0.0),
            ("1 < bool 2", 1.0),
            ("1 >= bool 1", 1.0),
            ("1 <= bool 0", 0.0),
        ] {
            assert_eq!(value(query), want, "{query}");
        }
    }

    /// The one scalar that is not constant.
    #[test]
    fn time_is_the_step_in_seconds() {
        assert_eq!(at("time()", 0), 0.0);
        assert_eq!(at("time()", 60_000), 60.0);
        assert_eq!(at("time() - 30", 60_000), 30.0);
        assert_eq!(at("pi()", 12_345), std::f64::consts::PI);
    }

    /// A call the fold knows the name of is still held to its
    /// signature: `time()` takes nothing, and an argument makes the
    /// query wrong rather than the time different.
    #[test]
    fn a_call_is_checked_before_its_name_is_matched() {
        let err = fold(&promql_parser::parse_expr("time(1)").unwrap(), 0).unwrap_err();
        assert_eq!(
            err.to_string(),
            "expected 0 argument(s) in call to \"time\", got 1"
        );
        assert!(matches!(err, EngineError::Query(_)), "{err}");

        let err = fold(&promql_parser::parse_expr("pi(1)").unwrap(), 0).unwrap_err();
        assert!(matches!(err, EngineError::Query(_)), "{err}");
        assert_eq!(at("time()", 60_000), 60.0);
    }

    /// What cannot be folded is named, not guessed at: `scalar(v)`
    /// reads a vector, which this fold never sees.
    #[test]
    fn an_expression_that_needs_data_is_unsupported_by_name() {
        let err = fold(&promql_parser::parse_expr("scalar(up)").unwrap(), 0).unwrap_err();
        assert!(
            matches!(&err, EngineError::Unsupported(f) if f == "the scalar function"),
            "{err}"
        );

        let err = fold(&promql_parser::parse_expr("scalar(up) + 1").unwrap(), 0).unwrap_err();
        assert!(matches!(err, EngineError::Unsupported(_)), "{err}");
    }
}
