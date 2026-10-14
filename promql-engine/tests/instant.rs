//! Instant queries answer with the type their expression has.
//!
//! Upstream's `execEvalStmt` runs an instant query as a range of one step
//! and reads the matrix back by `Expr.Type()`; `NewRangeQuery` refuses the
//! types that only an instant query can answer.

use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{
    ArrayRef, AsArray, FixedSizeBinaryArray, Int64Array, ListArray, RecordBatch, StructArray,
    UInt64Array,
};
use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema};
use datafusion::catalog::Session;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::physical_plan::ExecutionPlan;
use promql_engine::{
    series, Engine, EngineError, InstantResult, MemorySeriesSource, RangeQuery, SelectHints,
    SeriesSource,
};
use promql_parser::SeriesDescription;

fn load(lines: &[&str]) -> Vec<SeriesDescription> {
    lines
        .iter()
        .map(|l| promql_parser::parse_series_desc(l).expect("series line parses"))
        .collect()
}

/// Two series 30s apart: at 60s oslo is 3 and lima is 12.
fn source() -> Arc<MemorySeriesSource> {
    Arc::new(MemorySeriesSource::from_descriptions(
        &load(&[
            r#"temperature{city="oslo"} 1+1x4"#,
            r#"temperature{city="lima"} 10+1x4"#,
        ]),
        30.0,
    ))
}

fn instant(query: &str, at_ms: i64) -> Result<InstantResult, EngineError> {
    Engine::blocking()
        .unwrap()
        .instant_query(source().as_ref(), query, at_ms)
}

/// A string is not a series; it is the literal without its quotes, through
/// any parentheses.
#[test]
fn a_string_is_the_literal_without_its_quotes() {
    for (query, want) in [(r#"(" Foo ")"#, " Foo "), (r#""""#, "")] {
        match instant(query, 0).unwrap() {
            InstantResult::String(s) => assert_eq!(s, want, "{query}"),
            other => panic!("{query}: {other:?}"),
        }
    }
}

/// A vector keeps the engine's batches, one sample per series stamped
/// with the evaluation time.
#[test]
fn an_instant_vector_is_a_sample_per_series_at_the_evaluation_time() {
    let InstantResult::Vector(batches) = instant("temperature", 60_000).unwrap() else {
        panic!("not a vector")
    };
    let decoded = series::decode(&batches).unwrap();
    let got: Vec<(String, Vec<i64>, Vec<f64>)> = decoded
        .iter()
        .map(|s| {
            (
                s.label("city").to_string(),
                s.timestamps().to_vec(),
                s.values().to_vec(),
            )
        })
        .collect();
    assert_eq!(
        got,
        [
            ("lima".to_string(), vec![60_000], vec![12.0]),
            ("oslo".to_string(), vec![60_000], vec![3.0]),
        ]
    );
}

/// A scalar is read off the one-series plan it shares with `vector(x)`, so
/// only the expression's type tells it from `sum(x)`.
#[test]
fn a_scalar_expression_answers_a_number_and_a_vector_of_one_does_not() {
    assert!(matches!(
        instant("1 + 1", 0).unwrap(),
        InstantResult::Scalar(v) if v == 2.0
    ));
    assert!(matches!(
        instant("time()", 90_000).unwrap(),
        InstantResult::Scalar(v) if v == 90.0
    ));
    assert!(matches!(
        instant("vector(2)", 0).unwrap(),
        InstantResult::Vector(_)
    ));
    assert!(matches!(
        instant("sum(temperature)", 60_000).unwrap(),
        InstantResult::Vector(_)
    ));
}

/// `NewRangeQuery` takes a scalar or an instant vector and nothing else;
/// the words are upstream's.
#[test]
fn a_range_query_refuses_what_only_an_instant_query_answers() {
    let engine = Engine::blocking().unwrap();
    let range = RangeQuery::new(0, 120_000, 60_000);
    for (query, typ) in [
        (r#"("Foo")"#, "string"),
        ("temperature[1m]", "range vector"),
        ("temperature[1m:30s]", "range vector"),
    ] {
        let err = engine
            .range_query(source().as_ref(), query, &range)
            .unwrap_err();
        assert!(
            matches!(&err, EngineError::Query(m) if m == &format!(
                "invalid expression type \"{typ}\" for range query, must be Scalar or instant Vector"
            )),
            "{query}: {err}"
        );
    }
}

/// One series of a matrix: its `city` and its samples.
type Row = (String, Vec<(i64, f64)>);

fn matrix_over(source: &MemorySeriesSource, query: &str, at_ms: i64) -> Vec<Row> {
    let InstantResult::Matrix(batches) = Engine::blocking()
        .unwrap()
        .instant_query(source, query, at_ms)
        .unwrap_or_else(|e| panic!("{query}: {e}"))
    else {
        panic!("{query}: not a matrix")
    };
    series::coalesce(series::decode(&batches).unwrap())
        .unwrap()
        .iter()
        .map(|s| {
            (
                s.label("city").to_string(),
                s.timestamps()
                    .iter()
                    .copied()
                    .zip(s.values().iter().copied())
                    .collect(),
            )
        })
        .collect()
}

fn matrix(query: &str, at_ms: i64) -> Vec<Row> {
    matrix_over(source().as_ref(), query, at_ms)
}

fn row(city: &str, points: &[(i64, f64)]) -> Row {
    (city.to_string(), points.to_vec())
}

/// `matrixSelector`: the samples under their own timestamps, not the
/// step's, in a window that is open at its start and closed at its end.
#[test]
fn a_range_selector_is_the_samples_of_its_window_open_at_the_start() {
    // oslo is 1, 2, 3 at 0, 30s, 60s: the sample at exactly 0 is mint.
    assert_eq!(
        matrix(r#"temperature{city="oslo"}[1m]"#, 60_000),
        [row("oslo", &[(30_000, 2.0), (60_000, 3.0)])]
    );
    assert_eq!(
        matrix("temperature[1m]", 60_000),
        [
            row("lima", &[(30_000, 11.0), (60_000, 12.0)]),
            row("oslo", &[(30_000, 2.0), (60_000, 3.0)]),
        ]
    );
}

/// A series with no sample in the window is not in the result, and a
/// selector that matches nothing is an empty matrix, not an error.
#[test]
fn a_series_with_no_sample_in_the_window_is_not_in_the_matrix() {
    assert_eq!(matrix("temperature[10s]", 45_000), []);
    assert_eq!(matrix("nonexistent[1m]", 60_000), []);
    // The last sample is at 120s; ten minutes on, nothing is in reach.
    assert_eq!(matrix("temperature[1m]", 600_000), []);
}

/// `offset` moves the window and `@` pins it; the samples keep their own
/// timestamps in both, which is why upstream does not duplicate a range
/// selector's result per step.
#[test]
fn offset_and_at_move_the_window_and_not_the_timestamps() {
    assert_eq!(
        matrix(r#"temperature{city="oslo"}[1m] offset 30s"#, 60_000),
        [row("oslo", &[(0, 1.0), (30_000, 2.0)])]
    );
    assert_eq!(
        matrix(r#"temperature{city="oslo"}[1m] @ 120"#, 0),
        [row("oslo", &[(90_000, 4.0), (120_000, 5.0)])]
    );
}

/// A stale marker ends a series' run and is no sample of a range
/// selector, as `matrixIterSlice` drops it.
#[test]
fn a_stale_marker_is_not_a_sample_of_the_window() {
    let source = MemorySeriesSource::from_descriptions(
        &load(&[r#"temperature{city="oslo"} 1 stale 3"#]),
        60.0,
    );
    assert_eq!(
        matrix_over(&source, "temperature[3m]", 180_000),
        [row("oslo", &[(120_000, 3.0)])]
    );
    assert_eq!(
        matrix_over(&source, "temperature[3m]", 120_000),
        [row("oslo", &[(0, 1.0), (120_000, 3.0)])]
    );
}

/// Whatever shape the store cuts its answer into, a label set's chunks
/// are one series once joined, with every sample of the window once.
#[test]
fn a_range_selector_is_the_same_across_store_shapes() {
    let plain = matrix("temperature[2m]", 120_000);
    assert_eq!(plain.len(), 2);
    let stores = [
        Arc::try_unwrap(source()).unwrap().chunked(0),
        Arc::try_unwrap(source()).unwrap().chunked(45_000),
        Arc::try_unwrap(source())
            .unwrap()
            .blocks(NonZeroU64::new(60_000).unwrap()),
        Arc::try_unwrap(source())
            .unwrap()
            .partitions(NonZeroUsize::new(3).unwrap()),
    ];
    for store in &stores {
        assert_eq!(matrix_over(store, "temperature[2m]", 120_000), plain);
    }
}

/// A range selector in parentheses is the same call argument upstream's
/// `unwrapParenExpr` makes it.
#[test]
fn parentheses_around_a_range_selector_argument_change_nothing() {
    let engine = Engine::blocking().unwrap();
    let range = RangeQuery::new(60_000, 120_000, 30_000);
    let run = |query: &str| {
        let batches = engine
            .range_query(source().as_ref(), query, &range)
            .unwrap_or_else(|e| panic!("{query}: {e}"));
        series::decode(&batches)
            .unwrap()
            .iter()
            .map(|s| (s.label("city").to_string(), s.values().to_vec()))
            .collect::<Vec<_>>()
    };
    for func in ["rate", "changes", "sum_over_time"] {
        let plain = run(&format!("{func}(temperature[1m])"));
        assert!(!plain.is_empty(), "{func}");
        assert_eq!(run(&format!("{func}((temperature[1m]))")), plain, "{func}");
        assert_eq!(
            run(&format!("{func}(((temperature[1m])))")),
            plain,
            "{func}"
        );
    }
    assert!(matches!(
        run("absent_over_time((nonexistent[1m]))").as_slice(),
        [(_, v)] if v == &[1.0, 1.0, 1.0]
    ));
}

fn scalar_over(source: &MemorySeriesSource, query: &str, at_ms: i64) -> f64 {
    match Engine::blocking()
        .unwrap()
        .instant_query(source, query, at_ms)
        .unwrap_or_else(|e| panic!("{query}: {e}"))
    {
        InstantResult::Scalar(v) => v,
        other => panic!("{query}: not a scalar: {other:?}"),
    }
}

/// `funcScalar`: the one series' value, and NaN for none or several, an
/// empty vector included.
#[test]
fn scalar_of_a_vector_is_its_one_value_or_nan() {
    let source = source();
    assert_eq!(
        scalar_over(&source, r#"scalar(temperature{city="oslo"})"#, 60_000),
        3.0
    );
    assert!(scalar_over(&source, "scalar(temperature)", 60_000).is_nan());
    assert!(scalar_over(&source, "scalar(nonexistent)", 60_000).is_nan());
}

/// The store's partitions each see part of the vector, and the Final that
/// merges them has to count across them: two series in two partitions are
/// still two.
#[test]
fn scalar_counts_across_the_stores_partitions() {
    for n in [1, 2, 3, 8] {
        let store = Arc::try_unwrap(source())
            .unwrap()
            .partitions(NonZeroUsize::new(n).unwrap());
        assert!(
            scalar_over(&store, "scalar(temperature)", 60_000).is_nan(),
            "{n} partitions"
        );
        assert_eq!(
            scalar_over(&store, r#"scalar(temperature{city="lima"})"#, 60_000),
            12.0,
            "{n} partitions"
        );
    }
}

/// A range query's `scalar(v)` has a sample at every step, which no
/// other operator does: a step the vector is empty at is NaN, not absent.
#[test]
fn scalar_has_a_sample_at_every_step_of_a_range_query() {
    let engine = Engine::blocking().unwrap();
    let range = RangeQuery::new(0, 120_000, 60_000);
    let run = |query: &str| {
        let batches = engine
            .range_query(source().as_ref(), query, &range)
            .unwrap_or_else(|e| panic!("{query}: {e}"));
        let decoded = series::decode(&batches).unwrap();
        assert_eq!(decoded.len(), 1, "{query}: one unlabelled series");
        assert_eq!(decoded[0].labels().count(), 0, "{query}");
        (
            decoded[0].timestamps().to_vec(),
            decoded[0].values().to_vec(),
        )
    };
    let (ts, vs) = run(r#"scalar(temperature{city="oslo"})"#);
    assert_eq!(ts, [0, 60_000, 120_000]);
    assert_eq!(vs, [1.0, 3.0, 5.0]);

    // Nothing is in reach ten minutes back, at any step.
    let (ts, vs) = run(r#"scalar(temperature{city="oslo"} offset 10m)"#);
    assert_eq!(ts, [0, 60_000, 120_000]);
    assert!(vs.iter().all(|v| v.is_nan()), "{vs:?}");
}

/// A source with a sliced samples child, nested metadata, opaque IDs and
/// noncanonical column order. The extra column and ID must not escape a
/// matrix projection, while scalar(v) must accept the native evaluator's
/// metadata-bearing samples without a canonicalizing cast.
#[derive(Debug)]
struct SlicedRawSource {
    fixed_ids: bool,
}

#[async_trait]
impl SeriesSource for SlicedRawSource {
    async fn select(
        &self,
        _: &dyn Session,
        _: &[promql_parser::ast::LabelMatcher],
        hints: SelectHints,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let names = ["__name__".to_string(), "city".to_string()];
        let rows = [
            series::Series::new(
                &[("__name__", "temperature"), ("city", "before")],
                vec![30_000],
                vec![99.0],
            )
            .unwrap(),
            series::Series::new(
                &[("__name__", "temperature"), ("city", "oslo")],
                vec![0, 15_000, 30_000, 60_000, 90_000],
                vec![
                    1.0,
                    f64::from_bits(promql_engine::selector::STALE_NAN_BITS),
                    2.0,
                    -0.0,
                    9.0,
                ],
            )
            .unwrap(),
            series::Series::new(
                &[("__name__", "temperature"), ("city", "after")],
                vec![30_000],
                vec![99.0],
            )
            .unwrap(),
        ];
        let block = series::Block {
            start_ms: hints.start_ms + hints.window_ms,
            end_ms: hints.end_ms + 1,
        };
        let batch = series::encode(&names, &rows, block).unwrap();
        let samples = batch
            .column_by_name(series::SAMPLES)
            .unwrap()
            .as_list::<i32>();
        let entries = samples.values().as_struct();
        let fields: Fields = entries
            .fields()
            .iter()
            .map(|f| {
                Arc::new(
                    f.as_ref()
                        .clone()
                        .with_metadata([("child".into(), f.name().clone())].into_iter().collect()),
                )
            })
            .collect();
        let entries = StructArray::new(fields.clone(), entries.columns().to_vec(), None);
        let item = Arc::new(
            Field::new(series::LIST_ITEM, DataType::Struct(fields), false)
                .with_metadata([("item".into(), "preserved".into())].into_iter().collect()),
        );
        let samples: ArrayRef = Arc::new(ListArray::new(
            item,
            samples.offsets().clone(),
            Arc::new(entries),
            None,
        ));
        let ids: ArrayRef = if self.fixed_ids {
            Arc::new(
                FixedSizeBinaryArray::try_from_iter(
                    [10_u64, 20, 30].into_iter().map(u64::to_be_bytes),
                )
                .unwrap(),
            )
        } else {
            Arc::new(UInt64Array::from(vec![10, 20, 30]))
        };
        let columns = vec![
            samples,
            Arc::new(Int64Array::from(vec![7, 7, 7])) as ArrayRef,
            ids,
            batch.column_by_name(series::BLOCK_END).unwrap().clone(),
            batch.column_by_name(series::LABELS).unwrap().clone(),
            batch.column_by_name(series::BLOCK_START).unwrap().clone(),
        ];
        let schema = Arc::new(Schema::new_with_metadata(
            [
                series::SAMPLES,
                "extra",
                series::SERIES_ID,
                series::BLOCK_END,
                series::LABELS,
                series::BLOCK_START,
            ]
            .into_iter()
            .zip(&columns)
            .map(|(name, array)| {
                Field::new(name, array.data_type().clone(), false)
                    .with_metadata([("source-field".into(), name.into())].into_iter().collect())
            })
            .collect::<Vec<_>>(),
            [("owner".into(), "source".into())].into_iter().collect(),
        ));
        // Only oslo is part of the source. Both adjacent rows remain in the
        // backing buffers, so their in-window samples expose a span mismatch.
        let batch = RecordBatch::try_new(schema.clone(), columns)?.slice(1, 1);
        assert_eq!(
            batch
                .column_by_name(series::SAMPLES)
                .unwrap()
                .as_list::<i32>()
                .offsets()[0],
            1
        );
        Ok(MemorySourceConfig::try_new_exec(
            &[vec![batch]],
            schema,
            None,
        )?)
    }
}

#[tokio::test]
async fn instant_matrix_and_scalar_accept_sliced_reordered_sources_with_metadata() {
    let engine = Engine::new();
    for fixed_ids in [false, true] {
        let source = SlicedRawSource { fixed_ids };
        let InstantResult::Matrix(batches) = engine
            .instant_query_async(&source, "temperature[1m]", 60_000)
            .await
            .unwrap()
        else {
            panic!("not a matrix")
        };
        let decoded = series::decode(&batches).unwrap();
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].label("city"), "oslo");
        assert_eq!(decoded[0].timestamps(), &[30_000, 60_000]);
        assert_eq!(
            decoded[0]
                .values()
                .iter()
                .map(|v| v.to_bits())
                .collect::<Vec<_>>(),
            [2.0f64.to_bits(), (-0.0f64).to_bits()]
        );
        for batch in &batches {
            let schema = batch.schema();
            assert_eq!(
                schema
                    .fields()
                    .iter()
                    .map(|f| f.name().as_str())
                    .collect::<Vec<_>>(),
                [
                    series::LABELS,
                    series::SAMPLES,
                    series::BLOCK_START,
                    series::BLOCK_END
                ]
            );
            assert_eq!(
                schema.metadata().get("owner").map(String::as_str),
                Some("source")
            );
            for field in schema.fields() {
                assert_eq!(field.metadata().get("source-field"), Some(field.name()));
            }
            let DataType::List(item) = schema.field_with_name(series::SAMPLES).unwrap().data_type()
            else {
                panic!("not a samples list")
            };
            assert_eq!(
                item.metadata().get("item").map(String::as_str),
                Some("preserved")
            );
            let DataType::Struct(fields) = item.data_type() else {
                panic!("not samples")
            };
            for field in fields {
                assert_eq!(field.metadata().get("child"), Some(field.name()));
            }
        }
        let InstantResult::Scalar(value) = engine
            .instant_query_async(&source, "scalar(temperature)", 60_000)
            .await
            .unwrap()
        else {
            panic!("not a scalar")
        };
        assert_eq!(value.to_bits(), (-0.0f64).to_bits());
        let InstantResult::Vector(batches) = engine
            .instant_query_async(&source, "-temperature", 60_000)
            .await
            .unwrap()
        else {
            panic!("not a vector")
        };
        let decoded = series::decode(&batches).unwrap();
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].label("__name__"), "");
        assert_eq!(decoded[0].values()[0].to_bits(), 0.0f64.to_bits());
    }
}
