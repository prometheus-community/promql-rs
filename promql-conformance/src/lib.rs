//! Conformance testing for the Rust PromQL engine.
//!
//! One suite: Prometheus's own promqltest corpus, vendored under
//! `testdata/prometheus/`, where the expected values ship with the
//! questions. Nothing to ask means nothing can be unavailable, so there
//! is no oracle, no Go toolchain and no network, and it runs in CI on
//! every commit.
//!
//! # Current state
//!
//! [`datafusion::DataFusionEngine`] fills the [`result::Engine`] seam
//! with the `promql-engine` crate. It evaluates vector selectors,
//! aggregations and range functions; every other expression fails with
//! [`result::EngineError::Unsupported`], and those failures are the
//! specification for what to build next. The passing count is the
//! progress meter, tracked per commit by `scripts/progress`.
//!
//! A red test is only useful when it is red for the right reason, so
//! harness breakage is reported as such rather than as a failing engine.
//! `tests/script.rs` holds the line scanner to all 20 vendored files
//! before an engine is involved, which keeps a parse bug from reading as
//! an evaluation bug.

pub mod datafusion;
pub mod prometheus;
pub mod result;

pub use datafusion::DataFusionEngine;
pub use result::{
    Engine, EngineError, HPoint, Labels, Point, QueryResult, Sample, Series, Unimplemented,
};
