//! Public parser entry points. Mirrors upstream `parse.go`.
//!
//! The grammar and both lrlex lexers are compiled by `build.rs`. This
//! module wires them together and surfaces parse errors in a shape that
//! consumers can attach to HTTP responses.
//!
//! # Parse modes
//!
//! `grammar.y` has a single `start` rule that dispatches on a leading
//! `START_*` pseudo-token, exactly as upstream's does. Those tokens
//! have no lexer rule; upstream prepends one to the token stream in
//! `parser.Lex` (guarded by `InjectItem`), and [`InjectingLexer`] below
//! is the same trick against grmtools' `Lexer` trait. That is what lets
//! one grammar serve expressions, metric selectors and series
//! descriptions without duplicating the shared `metric` / `label_set`
//! rules.
//!
//! # Lexer modes
//!
//! Upstream lexes all of those with one lexer carrying a `seriesDesc`
//! bool (`upstream/lex.go`): the flag makes SPACE significant, turns
//! `x` and `_` into TIMES/BLANK, and disables hex literals (`0x…` is
//! ambiguous with the `x` repeat operator). lrlex always starts in
//! state 0, so the mode can't be set by the caller and lives in a
//! second lexer file instead. `series.l`'s start conditions map 1:1
//! onto upstream's state functions:
//!
//! | `series.l` | `upstream/lex.go`   |
//! |------------|---------------------|
//! | `INITIAL`  | `lexStatements`     |
//! | `BRACES`   | `lexInsideBraces`   |
//! | `SERIES`   | `lexValueSequence`  |
//!
//! The `<BRACES>\}` and `<SERIES>\{` transitions stand in for
//! upstream's `if l.seriesDesc` checks at the end of `lexInsideBraces`
//! and `lexKeywordOrIdentifier`. Because start conditions are chosen
//! before the match rather than after, `series.l` needs no equivalent
//! of upstream's `l.peek() != '{'` lookahead.
//!
//! One known divergence: in a value sequence, upstream lexes any
//! identifier and lets the `series_value` action reject anything that
//! isn't `stale`. A longest-match DFA can't do that — an identifier
//! rule would swallow the `x40` in `13.00x40` — so `series.l` matches
//! `stale` literally and any other word is a lex error rather than a
//! parse error. Both reject; only the message differs.

use std::sync::LazyLock;

use lrlex::{LRNonStreamingLexer, LRNonStreamingLexerDef};
use lrpar::{LexError, Lexeme, Lexer, NonStreamingLexer};

use crate::actions::ParseResult;
use crate::ast::{Expr, LabelMatcher, SeriesDescription};
use crate::context::ParserCtx;
use crate::error::{ParseError, ParseErrors};
use crate::options::ParserOptions;
use crate::posrange::{Pos, PositionRange};

mod start_tokens {
    include!(concat!(env!("OUT_DIR"), "/start_tokens.rs"));
}

type LexerTypes = lrlex::DefaultLexerTypes<u32>;
type Lexeme_ = lrlex::DefaultLexeme<u32>;

// Generated `lexerdef()` functions compile every regex rule in `.l` from scratch.
// Build each lexer table once and reuse it across all parse calls.
static EXPR_LEXER: LazyLock<LRNonStreamingLexerDef<LexerTypes>> =
    LazyLock::new(crate::lexer_l::lexerdef);
static SERIES_LEXER: LazyLock<LRNonStreamingLexerDef<LexerTypes>> =
    LazyLock::new(crate::series_l::lexerdef);

/// Wraps a lexer so the stream begins with a synthetic `START_*`
/// lexeme. Mirrors upstream's `parser.InjectItem` / the `p.injecting`
/// branch of `parser.Lex`, which exist for the same reason: yacc allows
/// one start symbol, so extra ones are selected by a leading token.
///
/// The injected lexeme has a zero-width span at offset 0, so it never
/// perturbs the position ranges the actions compute.
struct InjectingLexer<'lexer, 'def, 'input> {
    inner: &'lexer LRNonStreamingLexer<'def, 'input, LexerTypes>,
    input: &'input str,
    inject: u32,
}

impl Lexer<LexerTypes> for InjectingLexer<'_, '_, '_> {
    fn iter<'a>(&'a self) -> Box<dyn Iterator<Item = Result<Lexeme_, lrlex::LRLexError>> + 'a> {
        let input = self.input;
        Box::new(
            std::iter::once(Ok(Lexeme_::new(self.inject, 0, 0))).chain(
                self.inner
                    .iter()
                    .map(move |l| l.map(|l| demote_fill_keyword(input, l))),
            ),
        )
    }
}

impl<'input> NonStreamingLexer<'input, LexerTypes> for InjectingLexer<'_, '_, 'input> {
    fn span_str(&self, span: cfgrammar::Span) -> &'input str {
        self.inner.span_str(span)
    }

    fn span_lines_str(&self, span: cfgrammar::Span) -> &'input str {
        self.inner.span_lines_str(span)
    }

    fn line_col(&self, span: cfgrammar::Span) -> ((usize, usize), (usize, usize)) {
        self.inner.line_col(span)
    }
}

/// `fill`, `fill_left` and `fill_right` are keywords only when the next
/// non-space rune is `(` — upstream's `peekFollowedByLeftParen`, added
/// so the modifiers could ship without breaking queries that already
/// used those words as metric names. lrlex has no lookahead, so the
/// decision moves one step downstream onto the lexeme stream; the `.l`
/// rules and the generated grammar stay as upstream writes them.
fn demote_fill_keyword(input: &str, lexeme: Lexeme_) -> Lexeme_ {
    let tok = lexeme.tok_id();
    if tok != start_tokens::FILL
        && tok != start_tokens::FILL_LEFT
        && tok != start_tokens::FILL_RIGHT
    {
        return lexeme;
    }
    let rest = &input[lexeme.span().end()..];
    if rest
        .trim_start_matches([' ', '\t', '\n', '\r'])
        .starts_with('(')
    {
        return lexeme;
    }
    Lexeme_::new(
        start_tokens::IDENT,
        lexeme.span().start(),
        lexeme.span().len(),
    )
}

/// Which `start` alternative the grammar takes, and therefore which
/// lexer feeds it.
///
/// Upstream keeps these as two independent values — a `START_*` token
/// for the parser and a `seriesDesc` bool for the lexer — and relies on
/// each entry point pairing them correctly. Pairing them in one type
/// instead makes a disagreeing combination (say, `START_SERIES_DESCRIPTION`
/// lexed by `lexer.l`) unrepresentable rather than merely unlikely.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ParseMode {
    Expression,
    MetricSelector,
    SeriesDescription,
    Metric,
}

impl ParseMode {
    /// The pseudo-token prepended to the lexeme stream to select the
    /// `start` alternative.
    fn start_token(self) -> u32 {
        match self {
            Self::Expression => start_tokens::START_EXPRESSION,
            Self::MetricSelector => start_tokens::START_METRIC_SELECTOR,
            Self::SeriesDescription => start_tokens::START_SERIES_DESCRIPTION,
            Self::Metric => start_tokens::START_METRIC,
        }
    }

    /// Whether this mode lexes with `series.l` — upstream's `seriesDesc`
    /// lexer mode — rather than `lexer.l`.
    fn uses_series_lexer(self) -> bool {
        matches!(self, Self::SeriesDescription | Self::Metric)
    }
}

/// The byte range an lrpar error points at.
///
/// Both error kinds carry a span: a lex error its own, a parse error
/// that of the lexeme it stopped on. Every `ParseError` used to be
/// built with `PositionRange::default()`, i.e. `0..0`, so the position
/// survived only inside the `Display` text — which a consumer would
/// have had to parse back out. This module's whole stated purpose is to
/// "surface parse errors in a shape that consumers can attach to HTTP
/// responses", and a range that always says `0..0` cannot do that.
///
/// Note a lex error's span is the zero-width point where matching
/// failed, not a range covering the offending text.
fn error_range(e: &lrpar::LexParseError<u32, LexerTypes>) -> PositionRange {
    let span = match e {
        lrpar::LexParseError::LexError(e) => e.span(),
        lrpar::LexParseError::ParseError(e) => e.lexeme().span(),
    };
    PositionRange::new(span.start() as Pos, span.end() as Pos)
}

/// Run the grammar over `input` in the given parse mode. Upstream's
/// `parseGenerated`.
fn parse_generated(
    input: &str,
    mode: ParseMode,
    options: ParserOptions,
) -> Result<ParseResult, ParseErrors> {
    let def = if mode.uses_series_lexer() {
        &*SERIES_LEXER
    } else {
        &*EXPR_LEXER
    };
    let lexer = def.lexer(input);
    let injecting = InjectingLexer {
        inner: &lexer,
        input,
        inject: mode.start_token(),
    };

    let ctx = ParserCtx::new(options);
    let (ast, errs) = crate::grammar::parse(&injecting, &ctx);
    let syntax_errors: Vec<ParseError> = errs
        .iter()
        .map(|e| ParseError::new(format!("{e}"), error_range(e)))
        .collect();
    // A gate reports and parsing carries on, so the tree can be whole and
    // still be rejected. Upstream's `parseGenerated` likewise returns the
    // accumulated errors in preference to the result.
    if ctx.has_errors() || !syntax_errors.is_empty() {
        return Err(ctx.into_errors(syntax_errors));
    }
    // Neither of these carries a span of its own, so they cover the
    // whole input rather than claiming a misleading `0..0`.
    let whole = PositionRange::new(0, input.len() as Pos);
    match ast {
        Some(Ok(result)) => Ok(result),
        Some(Err(())) => Err(ParseErrors::new(vec![ParseError::new(
            "parse produced an error node",
            whole,
        )])),
        None => Err(ParseErrors::new(vec![ParseError::new(
            "empty parse",
            whole,
        )])),
    }
}

fn unexpected_mode(what: &str, input: &str) -> ParseErrors {
    ParseErrors::new(vec![ParseError::new(
        format!("input is not {what}"),
        PositionRange::new(0, input.len() as Pos),
    )])
}

/// A parser configured with [`ParserOptions`]. Upstream `parser.NewParser`
/// and its `Parser` interface: the options are fixed at construction and
/// every method parses under them.
///
/// The package-level functions below ([`parse_expr`] and the rest) are
/// upstream's `ParseExpr` and friends, which parse with the zero
/// `Options`: every experimental gate off, as stock Prometheus has it.
#[derive(Debug, Clone, Copy, Default)]
pub struct Parser {
    options: ParserOptions,
}

impl Parser {
    /// Upstream `NewParser`.
    pub fn new(options: ParserOptions) -> Self {
        Self { options }
    }

    pub fn options(&self) -> &ParserOptions {
        &self.options
    }

    /// Parse a PromQL expression. Upstream: `Parser.ParseExpr`.
    pub fn parse_expr(&self, input: &str) -> Result<Expr, ParseErrors> {
        match parse_generated(input, ParseMode::Expression, self.options)? {
            ParseResult::Expr(e) => Ok(e),
            _ => Err(unexpected_mode("an expression", input)),
        }
    }

    /// Parse a metric selector, returning its label matchers.
    /// Upstream: `Parser.ParseMetricSelector`.
    pub fn parse_metric_selector(&self, input: &str) -> Result<Vec<LabelMatcher>, ParseErrors> {
        let expr = match parse_generated(input, ParseMode::MetricSelector, self.options)? {
            ParseResult::Expr(e) => e,
            _ => return Err(unexpected_mode("a metric selector", input)),
        };
        match expr {
            Expr::VectorSelector(mut vs) => {
                if !vs.name.is_empty() {
                    vs.label_matchers.push(LabelMatcher {
                        name: "__name__".to_string(),
                        op: crate::ast::MatchOp::Equal,
                        value: std::mem::take(&mut vs.name),
                        pos_range: vs.pos_range,
                    });
                }
                Ok(vs.label_matchers)
            }
            _ => Err(unexpected_mode("a metric selector", input)),
        }
    }

    /// Parse a series description — one `metric{...} <values>` line of a
    /// promqltest load block. Upstream: `Parser.ParseSeriesDesc`.
    ///
    /// Value sequences are expanded at parse time, as upstream does: a
    /// `<value>+<step>x<count>` run yields `count + 1` points, the extra
    /// one being "time 0, which we ignore in tests".
    ///
    /// Native-histogram descriptors (`{{schema:1 …}}`) are not supported
    /// yet; those alternatives of `series_item` reference rules that are
    /// still on the sidecar's skip list, so they are rejected as parse
    /// errors.
    pub fn parse_series_desc(&self, input: &str) -> Result<SeriesDescription, ParseErrors> {
        match parse_generated(input, ParseMode::SeriesDescription, self.options)? {
            ParseResult::SeriesDescription(sd) => Ok(sd),
            _ => Err(unexpected_mode("a series description", input)),
        }
    }

    /// Parse a bare label set (`{foo="bar"}`). Upstream's `START_METRIC`
    /// mode, used by promtool's unit-test loader. Upstream:
    /// `Parser.ParseMetric`.
    pub fn parse_metric(&self, input: &str) -> Result<Vec<LabelMatcher>, ParseErrors> {
        match parse_generated(input, ParseMode::Metric, self.options)? {
            ParseResult::Metric(m) => Ok(m),
            _ => Err(unexpected_mode("a metric", input)),
        }
    }
}

/// Parse a PromQL expression with every experimental gate off. Upstream:
/// `parser.ParseExpr`.
pub fn parse_expr(input: &str) -> Result<Expr, ParseErrors> {
    Parser::default().parse_expr(input)
}

/// Upstream: `parser.ParseMetricSelector`. See [`Parser::parse_metric_selector`].
pub fn parse_metric_selector(input: &str) -> Result<Vec<LabelMatcher>, ParseErrors> {
    Parser::default().parse_metric_selector(input)
}

/// Upstream: `parser.ParseSeriesDesc`. See [`Parser::parse_series_desc`].
pub fn parse_series_desc(input: &str) -> Result<SeriesDescription, ParseErrors> {
    Parser::default().parse_series_desc(input)
}

/// Upstream's `START_METRIC` mode. See [`Parser::parse_metric`].
pub fn parse_metric(input: &str) -> Result<Vec<LabelMatcher>, ParseErrors> {
    Parser::default().parse_metric(input)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn concurrent_parses_share_cached_lexers() {
        let expr_lexer = &*EXPR_LEXER;
        let series_lexer = &*SERIES_LEXER;
        std::thread::scope(|s| {
            for _ in 0..4 {
                s.spawn(|| {
                    assert!(std::ptr::eq(&*EXPR_LEXER, expr_lexer));
                    assert!(std::ptr::eq(&*SERIES_LEXER, series_lexer));
                    for _ in 0..32 {
                        // Fail mid-`<BRACES>` in both lexers before parsing keyword-bearing
                        // inputs to verify start-condition state does not leak across calls.
                        parse_expr(r#"up{by="a""#).expect_err("unclosed expr brace");
                        parse_metric(r#"{by="a""#).expect_err("unclosed metric brace");

                        let expr = parse_expr(
                            r#"sum by (job) (rate(http_requests_total{job="api"}[5m]))"#,
                        )
                        .expect("valid expression");
                        assert!(matches!(expr, Expr::Aggregate(_)));

                        let sel = parse_metric_selector(r#"http_requests_total{job="api"}"#)
                            .expect("valid metric selector");
                        assert_eq!(sel.len(), 2);

                        let sd = parse_series_desc(r#"http_requests_total{job="api"} 1+2x3"#)
                            .expect("valid series description");
                        assert_eq!(sd.values.len(), 4);

                        let m = parse_metric(r#"http_requests_total{job="api"}"#)
                            .expect("valid metric");
                        assert_eq!(m.len(), 2);
                    }
                });
            }
        });
    }

    #[test]
    fn stops_at_first_syntax_error_while_preserving_prior_gate_errors() {
        let errs: Vec<_> = parse_expr("foo + * + bar + *")
            .expect_err("syntax error")
            .into_iter()
            .collect();
        assert_eq!(errs.len(), 1);
        assert_eq!(errs[0].range, PositionRange::new(6, 7));
        assert_eq!(errs[0].gate, None);

        let errs: Vec<_> = parse_expr("mad_over_time(x[5m]) +")
            .expect_err("gate and syntax errors")
            .into_iter()
            .collect();
        assert_eq!(errs.len(), 2);
        assert_eq!(errs[0].range, PositionRange::new(0, 13));
        assert_eq!(errs[0].gate, Some("promql-experimental-functions"));
        assert_eq!(errs[1].range, PositionRange::new(22, 22));
        assert_eq!(errs[1].gate, None);

        let errs: Vec<_> = parse_expr("* mad_over_time(x[5m])")
            .expect_err("leading syntax error")
            .into_iter()
            .collect();
        assert_eq!(errs.len(), 1);
        assert_eq!(errs[0].range, PositionRange::new(0, 1));
        assert_eq!(errs[0].gate, None);
    }
}
