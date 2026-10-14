//! The caller's `RangeQuery` is checked once, in `plan`, so a struct
//! literal gets the same treatment as `RangeQuery::new`. Each of these
//! used to reach the kernels: an empty result, an overflow panic, or a
//! store ordering error blamed on the source.

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::error::Result;
use datafusion::physical_plan::ExecutionPlan;
use promql_engine::{
    Engine, EngineError, MemorySeriesSource, RangeQuery, SelectHints, SeriesSource,
};
use promql_parser::ast::LabelMatcher;

fn source() -> MemorySeriesSource {
    let lines = [r#"up{pod="a"} 1+1x15"#];
    let desc: Vec<_> = lines
        .iter()
        .map(|l| promql_parser::parse_series_desc(l).expect("series line parses"))
        .collect();
    MemorySeriesSource::from_descriptions(&desc, 30.0)
}

/// A store that declares one block over the whole query range whatever
/// the lookback, as a store with a fixed block grid would. The default
/// source starts its block at `start + window`; using a fixed block here
/// keeps these regressions about query arithmetic, not source layout.
#[derive(Debug)]
struct WholeRange(MemorySeriesSource);

#[async_trait]
impl SeriesSource for WholeRange {
    async fn select(
        &self,
        state: &dyn Session,
        matchers: &[LabelMatcher],
        mut hints: SelectHints,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        hints.window_ms = 0;
        self.0.select(state, matchers, hints).await
    }
}

type Rows = Vec<(Vec<i64>, Vec<f64>)>;

fn run(query: &str, range: RangeQuery) -> Result<Rows, EngineError> {
    let engine = Engine::blocking().unwrap();
    let batches = engine.range_query(&WholeRange(source()), query, &range)?;
    let out = promql_engine::series::decode(&batches).unwrap();
    Ok(out
        .iter()
        .map(|s| (s.timestamps().to_vec(), s.values().to_vec()))
        .collect())
}

fn literal(start_ms: i64, end_ms: i64, lookback_ms: i64) -> RangeQuery {
    RangeQuery {
        start_ms,
        end_ms,
        step_ms: 30_000,
        lookback_ms,
    }
}

#[test]
fn a_zero_lookback_is_the_default() {
    let default = run("up", RangeQuery::new(0, 300_000, 30_000)).unwrap();
    assert!(!default.is_empty());
    assert_eq!(run("up", literal(0, 300_000, 0)).unwrap(), default);
}

#[test]
fn a_negative_lookback_is_the_default() {
    let default = run("up", RangeQuery::new(0, 300_000, 30_000)).unwrap();
    assert_eq!(run("up", literal(0, 300_000, -1)).unwrap(), default);
    assert_eq!(run("up", literal(0, 300_000, i64::MIN)).unwrap(), default);
}

#[test]
fn a_start_after_the_end_is_a_query_error() {
    let err = run("up", RangeQuery::new(300_000, 0, 30_000)).unwrap_err();
    assert!(matches!(err, EngineError::Query(_)), "{err}");
    assert!(err.to_string().contains("before start"), "{err}");
}

#[test]
fn a_lookback_the_arithmetic_cannot_hold_is_a_query_error() {
    let err = run("up @ 0 offset 1s", literal(0, 300_000, i64::MAX)).unwrap_err();
    assert!(matches!(err, EngineError::Query(_)), "{err}");
    assert!(err.to_string().contains("lookback"), "{err}");
}

#[test]
fn a_start_or_end_the_arithmetic_cannot_hold_is_a_query_error() {
    let err = run("up offset 1m", literal(i64::MIN, i64::MIN, 300_000)).unwrap_err();
    assert!(matches!(err, EngineError::Query(_)), "{err}");
    assert!(err.to_string().contains("start"), "{err}");

    let err = run("up offset 1m", literal(0, i64::MAX, 300_000)).unwrap_err();
    assert!(matches!(err, EngineError::Query(_)), "{err}");
    assert!(err.to_string().contains("end"), "{err}");
}
