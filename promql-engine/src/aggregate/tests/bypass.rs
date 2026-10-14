//! Exercise the registered UDAF under both DF55 native partial streams. Low
//! probe thresholds alone are not proof of bypass: assert its skipped-row metric.

use super::*;
use datafusion::arrow::buffer::NullBuffer;
use datafusion::datasource::{memory::MemorySourceConfig, source::DataSourceExec};
use datafusion::logical_expr::registry::FunctionRegistry;
use datafusion::physical_expr::{
    aggregate::AggregateExprBuilder,
    expressions::{col, lit},
};
use datafusion::physical_plan::{
    aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy},
    collect, ExecutionPlan, InputOrderMode,
};

type StateBits = Vec<(i64, u64, u64, u64, bool)>;

pub(super) fn bits(state: &ArrayRef) -> StateBits {
    state_rows(state)
        .into_iter()
        .map(|(t, a, b, n, m)| (t, a.to_bits(), b.to_bits(), n.to_bits(), m))
        .collect()
}

fn nullable(input: &ArrayRef, valid: Vec<bool>) -> ArrayRef {
    let list = input.as_list::<i32>();
    Arc::new(ListArray::new(
        series::sample_item(),
        list.offsets().clone(),
        list.values().clone(),
        Some(NullBuffer::from(valid)),
    ))
}

#[test]
fn conversion_is_bit_identical_to_independent_row_partials_and_leaves_state_untouched() {
    let values = [
        -0.0,
        0.0,
        f64::from_bits(0x7ff8_0000_0000_1234),
        f64::from_bits(0x7ff0_0000_0000_0002),
        f64::INFINITY,
        f64::NEG_INFINITY,
        f64::MAX,
        1e-320,
        1e16,
        -1e16,
    ];
    for end in [7, 999_999] {
        let mut series = vec![vec![(7, 99.0)]];
        for v in values {
            series.push(if end == 7 {
                vec![(7, v)]
            } else {
                vec![(7, v), (10, v), (999_997, v)]
            });
        }
        series.extend([
            Vec::new(),
            vec![(i64::MIN, 123.0)],
            vec![(i64::MAX, 456.0)],
            vec![(6, 789.0)],
        ]);
        let mut valid = vec![true; series.len()];
        let null_row = series.len() - 3;
        valid[null_row] = false;
        let input = nullable(&samples_column(&series), valid).slice(1, series.len() - 1);
        assert_ne!(input.as_list::<i32>().offsets()[0], 0);
        let mut filter = vec![Some(true); series.len()];
        filter[series.len() - 2] = Some(false);
        filter[series.len() - 1] = None;
        let filter = BooleanArray::from(filter).slice(1, series.len() - 1);
        let groups: Vec<_> = (0..input.len()).collect();
        for op in OPS {
            let acc = accumulate(op, &[&[(7, f64::MAX)], &[(7, f64::MAX)]], 7, end, 3);
            let before: ArrayRef = Arc::new(acc.partial(1).unwrap());
            let size = acc.size();
            let converted = acc
                .convert_to_state(std::slice::from_ref(&input), Some(&filter))
                .unwrap()
                .remove(0);
            let mut independent = Grouped::new(op, 7, end, 3).unwrap();
            independent
                .update_batch(
                    std::slice::from_ref(&input),
                    &groups,
                    Some(&filter),
                    groups.len(),
                )
                .unwrap();
            let expected = independent.state(EmitTo::All).unwrap().remove(0);
            assert_eq!(converted.data_type(), &state_type());
            let list = converted.as_list::<i32>();
            assert_eq!(list.null_count(), 0);
            assert_eq!(list.offsets(), expected.as_list::<i32>().offsets());
            assert_eq!(bits(&converted), bits(&expected), "{op:?}, end={end}");
            let entries = list.values().as_struct();
            assert_eq!(entries.null_count(), 0);
            assert!(entries.columns().iter().all(|a| a.null_count() == 0));
            assert_eq!(acc.size(), size);
            assert!(converted.get_array_memory_size() < 16 * 1024);
            let after: ArrayRef = Arc::new(acc.partial(1).unwrap());
            assert_eq!(bits(&after), bits(&before));
            let mut merged = Grouped::new(op, 7, end, 3).unwrap();
            merged
                .merge_batch(&[converted], &groups, groups.len())
                .unwrap();
            let roundtrip = merged.state(EmitTo::All).unwrap().remove(0);
            assert_eq!(bits(&roundtrip), bits(&expected), "{op:?}");
        }
    }
}

#[test]
fn conversion_without_filters_handles_empty_batches_empty_lists_and_null_rows() {
    for op in OPS {
        for series in [
            Vec::new(),
            vec![Vec::new()],
            vec![vec![(1, -0.0)], Vec::new(), vec![(3, f64::INFINITY)]],
        ] {
            let input = samples_column(&series);
            let groups: Vec<_> = (0..series.len()).collect();
            for input in [input.clone(), nullable(&input, vec![false; series.len()])] {
                let mut acc = Grouped::new(op, 0, 3, 1).unwrap();
                let converted = acc
                    .convert_to_state(std::slice::from_ref(&input), None)
                    .unwrap()
                    .remove(0);
                acc.update_batch(&[input], &groups, None, groups.len())
                    .unwrap();
                let expected = acc.state(EmitTo::All).unwrap().remove(0);
                assert_eq!(
                    converted.as_list::<i32>().offsets(),
                    expected.as_list::<i32>().offsets()
                );
                assert_eq!(bits(&converted), bits(&expected), "{op:?}");
            }
        }
    }
}

#[test]
fn conversion_rejects_the_same_malformed_timestamps_as_updates() {
    for timestamps in [
        vec![-1],
        vec![4],
        vec![i64::MIN],
        vec![0, 0],
        vec![3, 2],
        vec![0, 2, 1, 3],
    ] {
        for op in OPS {
            let input = samples_column(&[timestamps.iter().map(|&t| (t, 1.0)).collect()]);
            let acc = Grouped::new(op, 0, 3, 1).unwrap();
            assert!(
                acc.convert_to_state(std::slice::from_ref(&input), None)
                    .is_err(),
                "{timestamps:?}"
            );
            // A filtered or null row must not validate its ignored payload.
            let empty = acc
                .convert_to_state(
                    std::slice::from_ref(&input),
                    Some(&BooleanArray::from(vec![false])),
                )
                .unwrap();
            assert!(empty[0].as_list::<i32>().value(0).is_empty());
            let empty = acc
                .convert_to_state(&[nullable(&input, vec![false])], None)
                .unwrap();
            assert!(empty[0].as_list::<i32>().value(0).is_empty());
        }
    }
    let acc = Grouped::new(Op::Sum, 0, 6, 2).unwrap();
    assert!(acc
        .convert_to_state(&[samples_column(&[vec![(1, 1.0)]])], None)
        .is_err());
}

/// Native streams flush a partition's accumulated groups before bypassing;
/// cross-partition scheduling cannot pin singleton-before-multi arrival. Drive
/// the same conversion/state/merge APIs explicitly in both arrival orders.
#[test]
fn bypass_singleton_and_multi_sample_partial_merge_in_both_orders() {
    for (values, variance, stddev) in [
        ([1.5e154, 1.5e154], 5e307, 7.071067811865475e153),
        ([0.0, 3e154], f64::INFINITY, f64::INFINITY),
        ([f64::NAN, 1.5e154], f64::NAN, f64::NAN),
        ([f64::INFINITY, 1.5e154], f64::NAN, f64::NAN),
        ([f64::NEG_INFINITY, 1.5e154], f64::NAN, f64::NAN),
    ] {
        for op in [Op::Stdvar, Op::Stddev] {
            let converter = Grouped::new(op, 0, 0, 1).unwrap();
            let singleton = converter
                .convert_to_state(&[samples_column(&[vec![(0, 0.0)]])], None)
                .unwrap()
                .remove(0);
            let mut partial = Grouped::new(op, 0, 0, 1).unwrap();
            partial
                .update_batch(
                    &[samples_column(&values.map(|v| vec![(0, v)]))],
                    &[0, 0],
                    None,
                    1,
                )
                .unwrap();
            let partial = partial.state(EmitTo::All).unwrap().remove(0);
            assert_eq!(state_rows(&singleton)[0].3, 1.0);
            assert_eq!(state_rows(&partial)[0].3, 2.0);
            for singleton_first in [true, false] {
                let states = if singleton_first {
                    [&singleton, &partial]
                } else {
                    [&partial, &singleton]
                };
                let mut final_acc = Grouped::new(op, 0, 0, 1).unwrap();
                for state in states {
                    merge(&mut final_acc, state).unwrap();
                }
                let output = final_acc.evaluate(EmitTo::All).unwrap();
                let got = rows(output.as_list::<i32>(), 0);
                assert_eq!(got.len(), 1);
                assert_eq!(got[0].0, 0);
                let got = got[0].1;
                let oracle = if op == Op::Stdvar { variance } else { stddev };
                if oracle.is_nan() {
                    assert!(got.is_nan(), "{op:?}, singleton_first={singleton_first}");
                } else if oracle.is_infinite() {
                    assert_eq!(got, oracle, "{op:?}, singleton_first={singleton_first}");
                } else {
                    assert!(got.is_finite(), "{op:?}, singleton_first={singleton_first}");
                    assert!(
                        (got - oracle).abs() <= oracle * 1e-14,
                        "{op:?}, singleton_first={singleton_first}: {got} != {oracle}"
                    );
                }
            }
        }
    }
}

fn native_input(end: i64) -> Arc<dyn ExecutionPlan> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("job", DataType::Utf8, false),
        Field::new(series::SAMPLES, series::samples_type(), true),
        Field::new("keep", DataType::Boolean, true),
    ]));
    let mut batches = Vec::new();
    for batch in 0..4 {
        let mut jobs = Vec::new();
        let mut series = Vec::new();
        let mut valid = Vec::new();
        let mut filter = Vec::new();
        for group in 0..64 {
            // Edge groups occur once, so exact singleton-state rules can be
            // compared without conflating bypass with parallel merge rounding.
            let edge = group % 8 < 4;
            jobs.push(format!(
                "j{}",
                if edge { group + batch * 64 } else { group }
            ));
            let value = match group % 8 {
                0 => -0.0,
                1 => f64::from_bits(0x7ff8_0000_0000_1234),
                2 => f64::INFINITY,
                3 => f64::NEG_INFINITY,
                4 => [1e16, 1.0, -1e16, 0.5][batch],
                _ => (group + batch) as f64,
            };
            let times = if end == 0 { vec![0] } else { vec![0, 3, end] };
            series.push(if group % 13 == 0 {
                Vec::new()
            } else {
                times.into_iter().map(|t| (t, value)).collect()
            });
            valid.push(group % 11 != 0);
            filter.push(match group % 7 {
                0 => None,
                1 => Some(false),
                _ => Some(true),
            });
        }
        batches.push(
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(StringArray::from(jobs)),
                    nullable(&samples_column(&series), valid),
                    Arc::new(BooleanArray::from(filter)),
                ],
            )
            .unwrap(),
        );
    }
    let source = MemorySourceConfig::try_new(&[batches], schema, None).unwrap();
    Arc::new(DataSourceExec::new(Arc::new(source)))
}

fn native_large_variance_input() -> Arc<dyn ExecutionPlan> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("job", DataType::Utf8, false),
        Field::new(series::SAMPLES, series::samples_type(), true),
        Field::new("keep", DataType::Boolean, true),
    ]));
    // Matching keys make the final accumulator merge, rather than just copy,
    // the singleton states emitted before and after the native bypass switch.
    let batches: Vec<_> = [0.0, 1.5e154]
        .into_iter()
        .map(|value| {
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(StringArray::from(
                        (0..64)
                            .map(|group| format!("j{group:02}"))
                            .collect::<Vec<_>>(),
                    )),
                    samples_column(&vec![vec![(0, value)]; 64]),
                    Arc::new(BooleanArray::from(vec![true; 64])),
                ],
            )
            .unwrap()
        })
        .collect();
    let source = MemorySourceConfig::try_new(&[batches], schema, None).unwrap();
    Arc::new(DataSourceExec::new(Arc::new(source)))
}

type Answers = Vec<(String, Vec<(i64, f64)>)>;

async fn native(op: Op, end: i64, migration: bool, bypass: bool, filtered: bool) -> Answers {
    native_with_input(op, end, migration, bypass, filtered, native_input(end)).await
}

async fn native_with_input(
    op: Op,
    end: i64,
    migration: bool,
    bypass: bool,
    filtered: bool,
    input: Arc<dyn ExecutionPlan>,
) -> Answers {
    let mut config = SessionConfig::new()
        .with_target_partitions(1)
        .with_batch_size(64);
    let execution = &mut config.options_mut().execution;
    execution.enable_migration_aggregate = migration;
    execution.skip_partial_aggregation_probe_rows_threshold = 1;
    execution.skip_partial_aggregation_probe_ratio_threshold = if bypass { 0.01 } else { 1.0 };
    let ctx = SessionContext::new_with_config(config);
    ctx.register_udaf(udaf());
    let schema = input.schema();
    let group =
        PhysicalGroupBy::new_single(vec![(col("job", &schema).unwrap(), "job".to_string())]);
    let expr = AggregateExprBuilder::new(
        ctx.udaf(NAME).unwrap(),
        vec![
            col(series::SAMPLES, &schema).unwrap(),
            lit(op.as_str()),
            lit(0i64),
            lit(end),
            lit(1i64),
        ],
    )
    .schema(schema.clone())
    .alias("out")
    .build()
    .unwrap();
    let partial = Arc::new(
        AggregateExec::try_new(
            AggregateMode::Partial,
            group.clone(),
            vec![Arc::new(expr)],
            vec![filtered.then(|| col("keep", &schema).unwrap())],
            input,
            schema,
        )
        .unwrap(),
    );
    assert_eq!(partial.input_order_mode(), &InputOrderMode::Linear);
    let final_agg = Arc::new(
        AggregateExec::try_new(
            AggregateMode::Final,
            group,
            partial.aggr_expr().to_vec(),
            partial.filter_expr().to_vec(),
            partial.clone(),
            partial.input_schema(),
        )
        .unwrap(),
    );
    let output = collect(final_agg, ctx.task_ctx()).await.unwrap();
    let skipped = partial
        .metrics()
        .unwrap()
        .sum_by_name("skipped_aggregation_rows")
        .map_or(0, |m| m.as_usize());
    assert_eq!(
        skipped > 0,
        bypass,
        "{op:?}, migration={migration}, skipped={skipped}"
    );
    let mut answers = Vec::new();
    for batch in output {
        for row in 0..batch.num_rows() {
            answers.push((
                batch.column(0).as_string::<i32>().value(row).to_string(),
                rows(batch.column(1).as_list::<i32>(), row),
            ));
        }
    }
    answers.sort_by(|a, b| a.0.cmp(&b.0));
    answers
}

/// Prometheus 83962c35's `aggregation` keeps these variances finite: its
/// sequential update forms delta * residual, while squaring delta first overflows.
#[tokio::test]
async fn native_adaptive_bypass_keeps_large_singleton_variance_finite() {
    for migration in [false, true] {
        for op in [Op::Stdvar, Op::Stddev] {
            let expected = native_with_input(
                op,
                0,
                migration,
                false,
                false,
                native_large_variance_input(),
            )
            .await;
            let got =
                native_with_input(op, 0, migration, true, false, native_large_variance_input())
                    .await;
            // native_with_input also requires skipped_aggregation_rows > 0 only
            // for the bypass run, under each DF55 aggregation implementation.
            assert_eq!(expected.len(), 64);
            assert_eq!(got.len(), 64);
            let oracle = match op {
                Op::Stdvar => 5.625e307,
                Op::Stddev => 7.5e153,
                _ => unreachable!(),
            };
            for (group, ((job, got), (want_job, want))) in got.iter().zip(&expected).enumerate() {
                assert_eq!(job, &format!("j{group:02}"));
                assert_eq!(job, want_job);
                assert_eq!(got.len(), 1);
                assert_eq!(want.len(), 1);
                assert_eq!(got[0].0, 0);
                assert_eq!(want[0].0, 0);
                let (got, want) = (got[0].1, want[0].1);
                for value in [got, want] {
                    assert!(
                        value.is_finite(),
                        "{op:?} {job}, migration={migration}: {value}"
                    );
                    assert!(
                        (value - oracle).abs() <= oracle * 1e-14,
                        "{op:?} {job}, migration={migration}: {value} != {oracle}"
                    );
                }
                assert!(
                    (got - want).abs() <= want.abs() * 1e-14,
                    "{op:?} {job}, migration={migration}: {got} != {want}"
                );
            }
        }
    }
}

#[tokio::test]
async fn native_adaptive_bypass_matches_non_bypass_for_every_operator() {
    for migration in [false, true] {
        for end in [0, 63] {
            for filtered in [false, true] {
                for op in OPS {
                    let expected = native(op, end, migration, false, filtered).await;
                    let got = native(op, end, migration, true, filtered).await;
                    assert_eq!(expected.len(), 160, "keys survive empty/filtered rows");
                    assert_eq!(got.len(), expected.len(), "{op:?}");
                    for ((job, got), (want_job, want)) in got.iter().zip(&expected) {
                        assert_eq!(job, want_job);
                        assert_eq!(got.len(), want.len(), "{op:?} {job}");
                        for ((t, got), (want_t, want)) in got.iter().zip(want) {
                            assert_eq!(t, want_t);
                            if got.is_nan() && want.is_nan() {
                                continue;
                            }
                            if matches!(op, Op::Stddev | Op::Stdvar)
                                && got.is_finite()
                                && want.is_finite()
                            {
                                let tol = 1e-12 * got.abs().max(want.abs()).max(1.0);
                                assert!((got - want).abs() <= tol, "{op:?} {job}: {got} != {want}");
                            } else {
                                assert_eq!(got.to_bits(), want.to_bits(), "{op:?} {job}");
                            }
                        }
                    }
                }
            }
        }
    }
}
