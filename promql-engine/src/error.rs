//! Every way a query fails before, during, or after DataFusion.
//!
//! The variants are kept apart because callers treat them differently: a
//! differential test counts [`EngineError::Unsupported`] as "not built
//! yet" and collapses it, a user error ([`EngineError::is_user_error`]) is
//! an answer (the reference engine also rejects some queries), and
//! [`EngineError::Schema`] identifies rejected series data. Source, Arrow,
//! DataFusion and runtime failures keep their own categories.

use datafusion::arrow::error::ArrowError;
use datafusion::error::DataFusionError;
use promql_parser::ParseErrors;

/// What the [`crate::series`] functions reject: an Arrow kernel failing,
/// or a shape this module's own checks refuse. Kept apart from
/// [`EngineError`] because the functions are called by stores too, which
/// have no use for planner categories.
#[derive(Debug, thiserror::Error)]
pub enum SeriesError {
    #[error("{0}")]
    Arrow(#[from] ArrowError),

    #[error("{0}")]
    Invalid(String),
}

// Lets `?` lift the plain-message failures of `series.rs` without a
// wrapper at every site.
impl From<String> for SeriesError {
    fn from(message: String) -> Self {
        SeriesError::Invalid(message)
    }
}

/// `#[non_exhaustive]` so a new failure mode is not a breaking change for
/// callers that already match on the variants they care about.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum EngineError {
    /// The expression parsed but uses something this engine does not
    /// implement yet. Named so that progress is countable per feature.
    #[error("{0} is not supported yet")]
    Unsupported(String),

    /// The query itself is invalid — a parse error, or a semantic error
    /// Prometheus would also report. This is a result, not a failure.
    #[error("{0}")]
    Query(String),

    /// [`EngineError::Query`] for a query that did not parse. Kept as the
    /// parser's own error so a caller can read each [`ParseError`]'s
    /// position; the `Display` text is the same as `Query`'s was.
    ///
    /// [`ParseError`]: promql_parser::ParseError
    #[error("{0}")]
    Parse(#[source] ParseErrors),

    /// [`EngineError::Query`] for a regular expression matcher that does
    /// not compile. Raised in `select`, so it travels through DataFusion
    /// like [`EngineError::Source`].
    #[error("invalid regular expression {pattern:?} for label {label:?}: {source}")]
    Regex {
        label: String,
        pattern: String,
        #[source]
        source: regex::Error,
    },

    /// Series data failed a schema or shape check, including explicit
    /// series helpers and collected-result validation. Not limited to plan
    /// time: trusted source batches are not audited at the source boundary.
    #[error("series source schema: {0}")]
    Schema(String),

    /// An Arrow failure from a series operation, with the original error
    /// kept. Timing follows that operation, including result processing;
    /// this does not imply source-boundary validation.
    #[error("series source schema: {0}")]
    Arrow(#[source] ArrowError),

    /// A source contract error explicitly reported by a source or adapter.
    /// When raised during execution it travels through DataFusion and is
    /// unwrapped here. Source identity and ordering are trusted, not audited
    /// at runtime; label and ID sorting are not source requirements.
    #[error("series source order: {0}")]
    Source(String),

    /// DataFusion refused or failed the plan.
    #[error("datafusion: {0}")]
    DataFusion(#[source] DataFusionError),

    /// A blocking call on an engine without a runtime, or a runtime that
    /// could not be built.
    #[error("runtime: {0}")]
    Runtime(String),

    /// The runtime of [`EngineError::Runtime`] could not be built.
    #[error("runtime: {0}")]
    RuntimeBuild(#[source] std::io::Error),
}

impl From<SeriesError> for EngineError {
    fn from(e: SeriesError) -> Self {
        match e {
            SeriesError::Arrow(e) => EngineError::Arrow(e),
            SeriesError::Invalid(m) => EngineError::Schema(m),
        }
    }
}

impl From<DataFusionError> for EngineError {
    /// An operator can only fail with a `DataFusionError`, so an engine
    /// error raised inside one travels as `External` and is taken back out
    /// here; otherwise every such error would reach callers as an opaque
    /// `DataFusion` string.
    fn from(e: DataFusionError) -> Self {
        match e {
            DataFusionError::External(inner) => match inner.downcast::<EngineError>() {
                Ok(engine) => *engine,
                Err(inner) => EngineError::DataFusion(DataFusionError::External(inner)),
            },
            DataFusionError::Context(ctx, inner) => match EngineError::from(*inner) {
                EngineError::DataFusion(inner) => {
                    EngineError::DataFusion(DataFusionError::Context(ctx, Box::new(inner)))
                }
                engine => engine.with_context(&ctx),
            },
            // A repartition hands one input error to every output partition,
            // so it arrives shared and usually cannot be moved out.
            DataFusionError::Shared(shared) => match std::sync::Arc::try_unwrap(shared) {
                Ok(e) => EngineError::from(e),
                Err(shared) => match shared.as_ref() {
                    DataFusionError::External(inner) => inner
                        .downcast_ref::<EngineError>()
                        .and_then(EngineError::copied)
                        .unwrap_or(EngineError::DataFusion(DataFusionError::Shared(shared))),
                    _ => EngineError::DataFusion(DataFusionError::Shared(shared)),
                },
            },
            e => EngineError::DataFusion(e),
        }
    }
}

impl EngineError {
    /// Whether the query is at fault rather than the engine or the store,
    /// the errors an HTTP front end answers with a 4xx. The enum is
    /// `#[non_exhaustive]`, so a caller cannot list these variants itself
    /// and stay correct; a new variant for a bad query belongs here.
    pub fn is_user_error(&self) -> bool {
        matches!(
            self,
            EngineError::Query(_) | EngineError::Parse(_) | EngineError::Regex { .. }
        )
    }

    /// Keep the context DataFusion wrapped an unwrapped engine error in.
    /// The message variants take it as a prefix. The ones holding a cause
    /// have no message to prefix and drop it: their variant and cause are
    /// what a caller matches and downcasts on, and none of them is raised
    /// under a DataFusion context today.
    fn with_context(self, ctx: &str) -> Self {
        match self {
            EngineError::Unsupported(m) => EngineError::Unsupported(format!("{ctx}: {m}")),
            EngineError::Query(m) => EngineError::Query(format!("{ctx}: {m}")),
            EngineError::Schema(m) => EngineError::Schema(format!("{ctx}: {m}")),
            EngineError::Source(m) => EngineError::Source(format!("{ctx}: {m}")),
            EngineError::Runtime(m) => EngineError::Runtime(format!("{ctx}: {m}")),
            other => other,
        }
    }

    /// An engine error held by a `Shared` that other partitions still
    /// reference, rebuilt since it cannot be moved out. `None` only for a
    /// cause that cannot be rebuilt: a `DataFusionError`, or an Arrow
    /// error wrapping a foreign one.
    fn copied(&self) -> Option<EngineError> {
        Some(match self {
            EngineError::Unsupported(m) => EngineError::Unsupported(m.clone()),
            EngineError::Query(m) => EngineError::Query(m.clone()),
            EngineError::Parse(e) => EngineError::Parse(e.clone()),
            EngineError::Regex {
                label,
                pattern,
                source,
            } => EngineError::Regex {
                label: label.clone(),
                pattern: pattern.clone(),
                source: source.clone(),
            },
            EngineError::Schema(m) => EngineError::Schema(m.clone()),
            EngineError::Source(m) => EngineError::Source(m.clone()),
            EngineError::Runtime(m) => EngineError::Runtime(m.clone()),
            EngineError::Arrow(e) => EngineError::Arrow(copied_arrow(e)?),
            EngineError::RuntimeBuild(e) => EngineError::RuntimeBuild(copied_io(e)),
            EngineError::DataFusion(_) => return None,
        })
    }
}

/// `ArrowError` is not `Clone`, but every variant except `ExternalError`
/// holds only text. The match is exhaustive on purpose: a variant a new
/// Arrow release adds should fail the build here rather than degrade a
/// shared error to an opaque one.
fn copied_arrow(e: &ArrowError) -> Option<ArrowError> {
    use ArrowError as A;
    Some(match e {
        A::NotYetImplemented(m) => A::NotYetImplemented(m.clone()),
        A::ExternalError(_) => return None,
        A::CastError(m) => A::CastError(m.clone()),
        A::MemoryError(m) => A::MemoryError(m.clone()),
        A::ParseError(m) => A::ParseError(m.clone()),
        A::SchemaError(m) => A::SchemaError(m.clone()),
        A::ComputeError(m) => A::ComputeError(m.clone()),
        A::DivideByZero => A::DivideByZero,
        A::ArithmeticOverflow(m) => A::ArithmeticOverflow(m.clone()),
        A::CsvError(m) => A::CsvError(m.clone()),
        A::JsonError(m) => A::JsonError(m.clone()),
        A::AvroError(m) => A::AvroError(m.clone()),
        A::IoError(m, io) => A::IoError(m.clone(), copied_io(io)),
        A::IpcError(m) => A::IpcError(m.clone()),
        A::InvalidArgumentError(m) => A::InvalidArgumentError(m.clone()),
        A::ParquetError(m) => A::ParquetError(m.clone()),
        A::CDataInterface(m) => A::CDataInterface(m.clone()),
        A::DictionaryKeyOverflowError => A::DictionaryKeyOverflowError,
        A::RunEndIndexOverflowError => A::RunEndIndexOverflowError,
        A::OffsetOverflowError(n) => A::OffsetOverflowError(*n),
    })
}

/// Kind and text survive; the inner error and OS code do not, and no
/// caller reads them off an engine error.
fn copied_io(e: &std::io::Error) -> std::io::Error {
    std::io::Error::new(e.kind(), e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// An explicitly reported source contract error must retain its variant
    /// through an `ExecutionPlan`'s `DataFusionError` boundary.
    #[test]
    fn a_source_error_survives_the_trip_through_datafusion() {
        assert_eq!(
            EngineError::Source("out of order".into()).to_string(),
            "series source order: out of order"
        );
        let wrapped = crate::source::source_error("out of order".into());
        assert!(
            matches!(EngineError::from(wrapped), EngineError::Source(m) if m == "out of order")
        );

        let in_context = DataFusionError::Context(
            "while collecting".into(),
            Box::new(DataFusionError::External(Box::new(EngineError::Source(
                "x".into(),
            )))),
        );
        assert!(matches!(
            EngineError::from(in_context),
            EngineError::Source(_)
        ));
    }

    #[test]
    fn a_source_error_survives_a_repartition_sharing_it() {
        let external =
            || DataFusionError::External(Box::new(EngineError::Source("out of order".into())));
        let alone = DataFusionError::Shared(std::sync::Arc::new(external()));
        assert!(matches!(EngineError::from(alone), EngineError::Source(_)));

        let held = std::sync::Arc::new(external());
        let other_partition = std::sync::Arc::clone(&held);
        assert!(matches!(
            EngineError::from(DataFusionError::Shared(held)),
            EngineError::Source(m) if m == "out of order"
        ));
        drop(other_partition);
    }

    /// `Context` text said where an error was raised; unwrapping the
    /// engine error out of it must not throw that away.
    #[test]
    fn the_context_of_an_unwrapped_error_is_kept() {
        let in_context = DataFusionError::Context(
            "while collecting".into(),
            Box::new(DataFusionError::External(Box::new(EngineError::Source(
                "out of order".into(),
            )))),
        );
        match EngineError::from(in_context) {
            EngineError::Source(m) => {
                assert!(
                    m.contains("while collecting") && m.contains("out of order"),
                    "{m}"
                )
            }
            other => panic!("expected Source, got {other:?}"),
        }
    }

    fn regex_error() -> EngineError {
        let pattern = String::from("(");
        EngineError::Regex {
            label: "pod".into(),
            source: regex::Regex::new(&pattern).unwrap_err(),
            pattern,
        }
    }

    fn external(e: EngineError) -> DataFusionError {
        DataFusionError::External(Box::new(e))
    }

    #[test]
    fn a_cause_holding_error_keeps_its_variant_out_of_a_context() {
        let in_context = |e| DataFusionError::Context("while selecting".into(), Box::new(e));

        let regex = EngineError::from(in_context(external(regex_error())));
        assert!(matches!(regex, EngineError::Regex { .. }), "{regex:?}");
        assert!(std::error::Error::source(&regex).is_some_and(|c| c.is::<regex::Error>()));

        let arrow = EngineError::Arrow(ArrowError::ComputeError("x".into()));
        let arrow = EngineError::from(in_context(external(arrow)));
        assert!(matches!(arrow, EngineError::Arrow(_)), "{arrow:?}");
    }

    /// The other partition keeps its reference, so the error cannot be
    /// moved out of the `Arc` and has to be rebuilt.
    fn shared_while_held(e: EngineError) -> EngineError {
        let held = std::sync::Arc::new(external(e));
        let other_partition = std::sync::Arc::clone(&held);
        let out = EngineError::from(DataFusionError::Shared(held));
        drop(other_partition);
        out
    }

    #[test]
    fn a_cause_holding_error_survives_a_repartition_sharing_it() {
        let io = || std::io::Error::new(std::io::ErrorKind::BrokenPipe, "pipe");
        let arrow = shared_while_held(EngineError::Arrow(ArrowError::IoError("ipc".into(), io())));
        match &arrow {
            EngineError::Arrow(ArrowError::IoError(m, e)) => {
                assert_eq!(
                    (m.as_str(), e.kind()),
                    ("ipc", std::io::ErrorKind::BrokenPipe)
                )
            }
            other => panic!("expected Arrow(IoError), got {other:?}"),
        }
        assert_eq!(
            arrow.to_string(),
            EngineError::Arrow(ArrowError::IoError("ipc".into(), io())).to_string()
        );

        let runtime = shared_while_held(EngineError::RuntimeBuild(io()));
        assert!(
            matches!(&runtime, EngineError::RuntimeBuild(e) if e.kind() == std::io::ErrorKind::BrokenPipe),
            "{runtime:?}"
        );
        assert!(matches!(
            shared_while_held(regex_error()),
            EngineError::Regex { .. }
        ));
    }

    /// The one cause that cannot be rebuilt: a foreign error inside Arrow's.
    #[test]
    fn a_shared_foreign_arrow_error_stays_a_datafusion_error() {
        let foreign = ArrowError::ExternalError(Box::new(std::fmt::Error));
        let e = shared_while_held(EngineError::Arrow(foreign));
        assert!(
            matches!(e, EngineError::DataFusion(DataFusionError::Shared(_))),
            "{e:?}"
        );
    }

    #[test]
    fn only_a_bad_query_is_a_user_error() {
        let parse = promql_parser::parse_expr("up{").unwrap_err();
        for user in [
            EngineError::Query("bad".into()),
            EngineError::Parse(parse),
            regex_error(),
            EngineError::from(external(regex_error())),
        ] {
            assert!(user.is_user_error(), "{user:?}");
        }
        for other in [
            EngineError::Unsupported("x".into()),
            EngineError::Schema("x".into()),
            EngineError::Source("x".into()),
            EngineError::Arrow(ArrowError::ComputeError("x".into())),
            EngineError::Runtime("x".into()),
            EngineError::DataFusion(DataFusionError::Execution("x".into())),
        ] {
            assert!(!other.is_user_error(), "{other:?}");
        }
    }

    #[test]
    fn an_arrow_error_is_kept_as_the_cause() {
        let e = EngineError::from(SeriesError::from(ArrowError::ComputeError("x".into())));
        assert!(matches!(e, EngineError::Arrow(_)), "{e:?}");
        assert!(std::error::Error::source(&e).is_some_and(|c| c.is::<ArrowError>()));
        assert_eq!(e.to_string(), "series source schema: Compute error: x");

        let m = EngineError::from(SeriesError::Invalid("bad".into()));
        assert_eq!(m.to_string(), "series source schema: bad");
    }

    #[test]
    fn a_runtime_build_failure_keeps_the_io_error() {
        let e = EngineError::RuntimeBuild(std::io::Error::other("no threads"));
        assert!(std::error::Error::source(&e).is_some_and(|c| c.is::<std::io::Error>()));
        assert_eq!(e.to_string(), "runtime: no threads");
    }

    #[test]
    fn any_other_datafusion_error_stays_one() {
        let e = EngineError::from(DataFusionError::Execution("boom".into()));
        assert!(matches!(e, EngineError::DataFusion(_)), "{e:?}");
        assert!(std::error::Error::source(&e).is_some_and(|c| c.is::<DataFusionError>()));
    }
}
