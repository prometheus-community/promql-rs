//! PromQL evaluation as DataFusion plans.
//!
//! The engine sits between two things it does not own: a parser
//! ([`promql_parser`]) and a store. The store is anyone's — parquet,
//! vortex, a remote service — so the one thing this crate specifies about
//! it is a Rust trait, [`SeriesSource`], and the Arrow shape its data
//! comes back in, [`series`]. Everything PromQL — lookback, staleness, the
//! step grid, `offset`, `@`, functions and aggregations — happens on the
//! engine's side of that trait as ordinary DataFusion plans and functions.
//!
//! `docs/series-source.md` is the design note for the trait and the shape.
//! Above them, `plan` turns a parsed expression into a `LogicalPlan`
//! over the store's plan, with [`selector`], [`aggregate`] and [`range`]
//! as DataFusion aggregate functions over the samples list, and `engine`
//! runs it.
//!
//! ```text
//! PromQL text ─► parser ─► Expr ─► plan::plan ─► LogicalPlan ─► DataFusion
//!                                      │                          ▲
//!                                      └── SeriesSource::select ──┘
//!                                          (the store's ExecutionPlan,
//!                                           in the canonical schema)
//! ```
//!
//! # Arrow through DataFusion only
//!
//! This crate reaches Arrow exclusively through `datafusion::arrow`. A
//! direct `arrow` dependency would pin a second Arrow version for any
//! consumer that patches DataFusion to a fork, and every `RecordBatch` a
//! store hands over would then be a type mismatch.

pub mod absent;
pub mod aggregate; // benches/kernels.rs
pub mod binary; // benches/kernels.rs
mod buffer;
pub mod elementwise; // benches/kernels.rs
pub(crate) mod engine;
pub(crate) mod error;
pub(crate) mod explain;
pub(crate) mod function;
pub(crate) mod labels;
mod labelset;
mod matcher;
pub mod math; // benches/kernels.rs
pub(crate) mod matrix;
pub(crate) mod memory;
pub(crate) mod params;
pub(crate) mod plan;
pub mod range; // benches/kernels.rs
pub(crate) mod scalar;
pub mod selector; // benches/kernels.rs
pub mod series;
pub(crate) mod sort;
pub mod source;
pub(crate) mod to_scalar;
pub(crate) mod value_type;

pub use engine::{check_selector_plans, Engine, EngineOptions, InstantResult, RangeQuery};
pub use error::{EngineError, SeriesError};
pub use explain::{render as explain_plan, render_physical as explain_physical_plan};
pub use matcher::{CompiledMatcher, METRIC_NAME};
pub use memory::MemorySeriesSource;
pub use params::Params;
pub use series::Series;
pub use source::{Grouping, SelectHints, SelectorTable, SeriesSource, Shard};
