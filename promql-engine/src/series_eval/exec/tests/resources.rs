use super::*;
use crate::source::source_error;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;

fn limited_context(limit: usize) -> SessionContext {
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::new(GreedyMemoryPool::new(limit)))
        .build_arc()
        .unwrap();
    SessionContext::new_with_config_rt(SessionConfig::new(), runtime)
}

#[tokio::test(flavor = "current_thread")]
async fn one_raw_row_and_one_large_eof_close_yield_to_cancellation() {
    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
    use std::time::Instant;

    for closing in [false, true] {
        let end = 900_000;
        let points: Vec<_> = if closing {
            vec![(0, 1.)]
        } else {
            (0..end).map(|t| (t, (t % 17) as f64)).collect()
        };
        let source = Raw(vec![batch(
            &[(block(0, end + 1), 1, "one-expensive-row", &points)],
            false,
        )]);
        let range = RangeQuery {
            lookback_ms: end + 1,
            ..RangeQuery::new(0, end, 1)
        };
        let query = if closing {
            "{pod=~\".*\"}"
        } else {
            "sum_over_time({pod=~\".*\"}[32ms])"
        };
        let physical = plan(&source, query, &range).await;
        let ctx = SessionContext::new();
        let other =
            MemoryConsumer::new("concurrent query").register(&ctx.runtime_env().memory_pool);
        other.try_resize(128).unwrap();
        let mut stream = private_stream(lower(&physical), &ctx, 8);
        let polls = Arc::new(AtomicUsize::new(0));
        let max_poll_ns = Arc::new(AtomicU64::new(0));
        let task_polls = polls.clone();
        let task_max = max_poll_ns.clone();
        let task = tokio::spawn(async move {
            // Observe actual poll durations, not a row-count proxy for stalls.
            let result = futures::future::poll_fn(|cx| {
                let start = Instant::now();
                let result = Pin::new(&mut stream).poll_next(cx);
                task_max.fetch_max(start.elapsed().as_nanos() as u64, Ordering::Relaxed);
                task_polls.fetch_add(1, Ordering::SeqCst);
                result
            })
            .await;
            assert!(result.is_some());
        });
        // Use an independently runnable peer, not the block_on root or
        // yield_now: Tokio may defer those until its event interval, after a
        // short sequence of cooperative polls has already completed.
        let peer_polls = polls.clone();
        let peer_pool = ctx.runtime_env().memory_pool.clone();
        let abort = task.abort_handle();
        let (finished, reserved) = tokio::spawn(async move {
            futures::future::poll_fn(|cx| {
                if peer_polls.load(Ordering::SeqCst) >= 3 || abort.is_finished() {
                    Poll::Ready(())
                } else {
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
            })
            .await;
            let observed = (abort.is_finished(), peer_pool.reserved());
            abort.abort();
            observed
        })
        .await
        .unwrap();
        assert!(polls.load(Ordering::SeqCst) >= 3, "closing={closing}");
        assert!(!finished, "single-row work monopolised the runtime");
        assert!(reserved > other.size());
        eprintln!(
            "closing={closing}: max observed evaluator poll={}us",
            max_poll_ns.load(Ordering::Relaxed) / 1000
        );
        assert!(task.await.unwrap_err().is_cancelled());
        assert_eq!(ctx.runtime_env().memory_pool.reserved(), other.size());
        drop(other);
        assert_eq!(ctx.runtime_env().memory_pool.reserved(), 0);
    }
}

#[tokio::test]
async fn a_single_row_keeps_its_cursor_and_reservation_across_sample_budget_boundaries() {
    let points: Vec<_> = (0..200_000).map(|t| (t, t as f64)).collect();
    let source = Raw(vec![batch(
        &[(block(0, 200_001), 1, "one", &points)],
        false,
    )]);
    let physical = plan(
        &source,
        "count_over_time({pod=~\".*\"}[32ms])",
        &RangeQuery::new(0, 200_000, 1),
    )
    .await;
    let ctx = SessionContext::new();
    let mut stream = private_stream(lower(&physical), &ctx, 8);
    for _ in 0..3 {
        assert!(matches!(futures::poll!(stream.next()), Poll::Pending));
        let (_, row) = stream.pending.as_ref().expect("partly read row");
        assert_eq!(*row, 0);
        assert!(!stream.current.as_ref().unwrap().sealed);
        assert_charged(&stream);
    }
    let output = stream.next().await.unwrap().unwrap();
    assert_eq!(output.num_rows(), 1);
    let samples = output.column(1).as_list::<i32>();
    let (ts, vs) = series::sample_slices(samples.values().as_struct());
    assert_eq!(ts.len(), 200_001);
    for (i, (&t, &v)) in ts.iter().zip(vs).enumerate() {
        assert_eq!(t, i as i64);
        let expected = if i == 200_000 { 31 } else { (i + 1).min(32) };
        assert_eq!(v.to_bits(), (expected as f64).to_bits());
    }
    assert!(stream.next().await.is_none());
    assert_eq!(ctx.runtime_env().memory_pool.reserved(), 0);
}

#[tokio::test]
async fn suspended_transition_close_does_not_open_the_next_identity() {
    let span = block(0, 200_001);
    let source = Raw(vec![batch(
        &[
            (span, 9, "closing", &[(0, 1.)]),
            (span, 3, "next", &[(200_000, 2.)]),
        ],
        true,
    )]);
    let range = RangeQuery {
        lookback_ms: 300_000,
        ..RangeQuery::new(0, 200_000, 1)
    };
    let physical = plan(&source, "{pod=~\".*\"}", &range).await;
    let ctx = SessionContext::new();
    let mut stream = private_stream(lower(&physical), &ctx, 1);
    // First poll ingests only row 0; the following poll begins closing it
    // against the still-unconsumed foreign ID.
    for _ in 0..2 {
        assert!(matches!(futures::poll!(stream.next()), Poll::Pending));
        let (raw, row) = stream.pending.as_ref().unwrap();
        assert_eq!(*row, 1);
        let current = stream.current.as_ref().unwrap();
        assert!(current
            .id
            .matches(raw.column_by_name(SERIES_ID).unwrap(), 0));
        assert!(!current.sealed);
        assert!(stream.output.is_none());
        assert_charged(&stream);
    }
    let first = stream.next().await.unwrap().unwrap();
    assert_eq!(first.num_rows(), 1);
    assert_eq!(first.column(1).as_list::<i32>().value_length(0), 200_001);
    assert_eq!(stream.pending.as_ref().unwrap().1, 1);
    assert!(stream.current.is_none());
    let second = stream.next().await.unwrap().unwrap();
    assert_eq!(
        answers(&[second]),
        vec![("next".into(), vec![(200_000, 2f64.to_bits())])]
    );
    assert!(stream.next().await.is_none());
    assert_eq!(ctx.runtime_env().memory_pool.reserved(), 0);
}

#[tokio::test]
async fn suspended_close_growth_cannot_hide_behind_releasing_numerical_input() {
    let source = Raw(vec![batch(
        &[(block(0, 200_001), 1, "closing", &[(0, 1.)])],
        false,
    )]);
    let range = RangeQuery {
        lookback_ms: 300_000,
        ..RangeQuery::new(0, 200_000, 1)
    };
    let physical = plan(&source, "{pod=~\".*\"}", &range).await;
    let limit = 16 << 20;
    let ctx = limited_context(limit);
    let mut stream = private_stream(lower(&physical), &ctx, 1);
    for _ in 0..2 {
        assert!(matches!(futures::poll!(stream.next()), Poll::Pending));
        assert_charged(&stream);
    }
    assert!(stream.pending.is_none());
    assert!(!stream.current.as_ref().unwrap().sealed);
    assert!(stream.current.as_ref().unwrap().fold.out.len() > 1);
    let other = MemoryConsumer::new("concurrent query").register(&ctx.runtime_env().memory_pool);
    other.try_resize(limit - stream.reservation.size()).unwrap();
    let error = stream.next().await.unwrap().unwrap_err();
    assert!(error.to_string().contains("Resources exhausted"));
    assert!(stream.current.is_none());
    assert!(stream.next().await.is_none());
    assert_eq!(ctx.runtime_env().memory_pool.reserved(), other.size());
}

#[tokio::test]
async fn finished_prefix_emits_before_the_block_or_next_identity_is_consumed() {
    let names: Vec<_> = (0..64)
        .map(|i| format!("series-{i:02}-long-label"))
        .collect();
    let samples = [(590, 1.)];
    let rows: Vec<_> = names
        .iter()
        .enumerate()
        .map(|(i, label)| (BLOCK, 100 - i as u64, label.as_str(), &samples[..]))
        .collect();
    let source = Raw(vec![batch(&rows, true)]);
    let physical = plan(&source, "{pod=~\".*\"}", &RangeQuery::new(600, 600, 1)).await;
    let ctx = SessionContext::new();
    let mut stream = private_stream(lower(&physical), &ctx, 1);
    let first = stream.next().await.unwrap().unwrap();
    assert_eq!(first.num_rows(), 1);
    assert_eq!(
        answers(&[first]),
        vec![(names[0].clone(), vec![(600, 1f64.to_bits())])]
    );
    assert!(stream.current.is_none());
    assert!(stream.output.is_none());
    let (raw, from) = stream.pending.as_ref().unwrap();
    assert_eq!(*from, 1); // the next identity is not even copied yet
    assert_eq!(raw.num_rows(), 64);
    assert_eq!(stream.input_bytes, raw.get_array_memory_size());
    assert_charged(&stream);
    drop(stream);
    assert_eq!(ctx.runtime_env().memory_pool.reserved(), 0);
}

#[tokio::test]
async fn oversized_ready_input_keeps_full_backing_and_yields_at_the_row_work_cap() {
    let names: Vec<_> = (0..INPUT_ROWS + 9).map(|i| format!("series-{i}")).collect();
    let samples = [(590, 1.)];
    let rows: Vec<_> = names
        .iter()
        .enumerate()
        .map(|(i, label)| (BLOCK, i as u64, label.as_str(), &samples[..]))
        .collect();
    let source = Raw(vec![batch(&rows, false)]);
    let physical = plan(&source, "{pod=~\".*\"}", &RangeQuery::new(600, 600, 1)).await;
    // MemorySource also honors the session batch size; let the oversized raw
    // frame reach the evaluator rather than being split by the fixture source.
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_batch_size(rows.len()));
    let mut stream = private_stream(lower(&physical), &ctx, rows.len());
    assert!(matches!(futures::poll!(stream.next()), Poll::Pending));
    let (raw, from) = stream.pending.as_ref().unwrap();
    assert_eq!(*from, INPUT_ROWS);
    assert_eq!(stream.input_bytes, raw.get_array_memory_size());
    assert_eq!(stream.output.as_ref().unwrap().rows(), INPUT_ROWS - 1);
    assert!(stream
        .current
        .as_ref()
        .unwrap()
        .id
        .matches(raw.column(4), INPUT_ROWS - 1));
    assert_charged(&stream);
    drop(stream);
    assert_eq!(ctx.runtime_env().memory_pool.reserved(), 0);
}

#[tokio::test]
async fn open_series_cancellation_and_upstream_error_release_only_their_reservation() {
    let source = Raw(vec![batch(&[(BLOCK, 1, "ready", &[(600, 1.)])], false)]);
    let physical = plan(&source, "{pod=~\".*\"}", &RangeQuery::new(600, 600, 1)).await;
    let ctx = SessionContext::new();
    let other = MemoryConsumer::new("concurrent query").register(&ctx.runtime_env().memory_pool);
    other.try_resize(128).unwrap();
    for fail in [false, true] {
        let mut stream = private_stream(lower(&physical), &ctx, 1);
        stream.input = Some(Box::pin(RecordBatchStreamAdapter::new(
            source.0[0].schema(),
            futures::stream::iter([
                Ok(source.0[0].clone()),
                Err(source_error(
                    "transport failed after candidate was ready".into(),
                )),
            ]),
        )));
        assert!(matches!(futures::poll!(stream.next()), Poll::Pending));
        assert!(stream.current.is_some());
        assert!(ctx.runtime_env().memory_pool.reserved() > other.size());
        if fail {
            assert!(stream
                .next()
                .await
                .unwrap()
                .unwrap_err()
                .to_string()
                .contains("transport failed"));
            assert!(stream.current.is_none());
            assert!(stream.output.is_none());
            assert!(stream.pending.is_none());
            assert!(stream.next().await.is_none());
        }
        drop(stream);
        assert_eq!(ctx.runtime_env().memory_pool.reserved(), other.size());
    }
    drop(other);
    assert_eq!(ctx.runtime_env().memory_pool.reserved(), 0);
}

#[tokio::test]
async fn consumed_raw_children_and_opaque_label_payload_die_while_current_values_live() {
    use datafusion::arrow::buffer::Buffer;
    let source = Raw(vec![batch(
        &[(BLOCK, 1, "placeholder", &[(590, 1.)])],
        false,
    )]);
    let physical = plan(&source, "{pod=~\".*\"}", &RangeQuery::new(600, 600, 1)).await;
    let ctx = SessionContext::new();
    let mut stream = private_stream(lower(&physical), &ctx, 1);
    let owner = Arc::new(vec![b'x'; 1 << 20]);
    let payload = Arc::downgrade(&owner);
    let pointer = std::ptr::NonNull::new(owner.as_ptr() as *mut u8).unwrap();
    let buffer = unsafe { Buffer::from_custom_allocation(pointer, 1 << 20, owner.clone()) };
    let mut builder = StringViewBuilder::new();
    let block = builder.append_block(buffer);
    builder.try_append_view(block, 0, 32).unwrap();
    let child: ArrayRef = Arc::new(builder.finish());
    let labels_child = Arc::downgrade(&child);
    let mut columns = batch(&[(BLOCK, 1, "placeholder", &[(590, 1.)])], false)
        .columns()
        .to_vec();
    columns[0] = Arc::new(StructArray::new(stream.fields.clone(), vec![child], None));
    let raw = RecordBatch::try_new(source.0[0].schema(), columns).unwrap();
    let samples_child = Arc::downgrade(raw.column(1).as_list::<i32>().values());
    stream.input = Some(Box::pin(RecordBatchStreamAdapter::new(
        raw.schema(),
        futures::stream::iter([Ok(raw)]),
    )));
    drop(owner);
    assert!(matches!(futures::poll!(stream.next()), Poll::Pending));
    assert!(stream.pending.is_none());
    assert_eq!(stream.input_bytes, 0);
    assert!(labels_child.upgrade().is_none());
    assert!(payload.upgrade().is_none());
    assert!(samples_child.upgrade().is_none());
    assert_eq!(
        stream.current.as_ref().unwrap().labels.as_ref().unwrap(),
        &vec!["x".repeat(32)]
    );
    assert!(stream.reservation.size() < 4096);
    let output = stream.next().await.unwrap().unwrap();
    assert_eq!(
        output
            .column(0)
            .as_struct()
            .column(0)
            .as_string_view()
            .value(0),
        "x".repeat(32)
    );
    assert!(stream.next().await.is_none());
    assert_eq!(ctx.runtime_env().memory_pool.reserved(), 0);
}

#[tokio::test]
async fn useful_chunk_history_copies_do_not_pin_unrelated_source_sample_backing() {
    use datafusion::arrow::buffer::Buffer;
    let source = Raw(vec![batch(
        &[(block(0, 100_000), 1, "same-series", &[(1, 1.)])],
        false,
    )]);
    // Both steps are still ahead of this chunk. A two-step selector opens a
    // Window, not the inline Candidate, and cannot have completed at this poll.
    let physical = plan(
        &source,
        "{pod=~\".*\"}",
        &RangeQuery::new(50_000, 60_000, 10_000),
    )
    .await;
    let ctx = SessionContext::new();
    let mut stream = private_stream(lower(&physical), &ctx, 1);
    let points: Vec<_> = (1..=1_000).map(|t| (t, t as f64)).collect();
    let unrelated: Vec<_> = (2..=8).map(|id| format!("unrelated-{id}")).collect();
    let mut rows = vec![(block(0, 100_000), 1, "same-series", points.as_slice())];
    rows.extend(unrelated.iter().enumerate().map(|(i, name)| {
        (
            block(0, 100_000),
            i as u64 + 2,
            name.as_str(),
            points.as_slice(),
        )
    }));
    // Track the allocations themselves: a sliced Arrow child could drop the
    // original StructArray header yet still pin all eight identities' buffers.
    let ts_owner = Arc::new(
        rows.iter()
            .flat_map(|row| row.3.iter().map(|p| p.0))
            .collect::<Vec<_>>(),
    );
    let vs_owner = Arc::new(
        rows.iter()
            .flat_map(|row| row.3.iter().map(|p| p.1))
            .collect::<Vec<_>>(),
    );
    let ts_backing = Arc::downgrade(&ts_owner);
    let vs_backing = Arc::downgrade(&vs_owner);
    let ts_pointer = std::ptr::NonNull::new(ts_owner.as_ptr() as *mut u8).unwrap();
    let vs_pointer = std::ptr::NonNull::new(vs_owner.as_ptr() as *mut u8).unwrap();
    // Each pointer is aligned for its primitive type and valid for the full
    // vector length; the Buffer's owner Arc keeps that region alive.
    let ts_buffer =
        unsafe { Buffer::from_custom_allocation(ts_pointer, ts_owner.len() * 8, ts_owner.clone()) };
    let vs_buffer =
        unsafe { Buffer::from_custom_allocation(vs_pointer, vs_owner.len() * 8, vs_owner.clone()) };
    let input = batch(&rows, false);
    let samples = ListArray::new(
        series::sample_item(),
        input.column(1).as_list::<i32>().offsets().clone(),
        Arc::new(StructArray::new(
            series::sample_fields(),
            vec![
                Arc::new(TimestampMillisecondArray::new(ts_buffer.into(), None)),
                Arc::new(Float64Array::new(vs_buffer.into(), None)),
            ],
            None,
        )),
        None,
    );
    let mut columns = input.columns().to_vec();
    columns[1] = Arc::new(samples);
    let raw = RecordBatch::try_new(input.schema(), columns)
        .unwrap()
        .slice(0, 1);
    drop(input);
    drop(ts_owner);
    drop(vs_owner);
    let backing = Arc::downgrade(raw.column(1).as_list::<i32>().values());
    // Arrow reports zero capacity for custom allocations. Compare against
    // the real eight-identity payload, not that native-estimate blind spot.
    let raw_size = raw.column(1).as_list::<i32>().values().len() * (8 + 8);
    assert_eq!(raw_size, 8 * points.len() * (8 + 8));
    stream.input = Some(Box::pin(RecordBatchStreamAdapter::new(
        raw.schema(),
        futures::stream::iter([Ok(raw)]),
    )));
    assert!(ts_backing.upgrade().is_some());
    assert!(vs_backing.upgrade().is_some());
    assert!(matches!(futures::poll!(stream.next()), Poll::Pending));
    assert!(stream.pending.is_none());
    assert_eq!(stream.input_bytes, 0);
    let current = stream.current.as_ref().unwrap();
    assert!(!current.sealed);
    assert_eq!(stream.block.as_ref().unwrap().context.steps(), 2);
    assert_eq!(current.fold.out.len(), 0);
    // No finalized-result heap can satisfy this bound: all 1000 useful
    // timestamp/value pairs must still live in the unfinished Window.
    assert!(current.fold.heap_size() >= points.len() * (8 + 8));
    assert!(backing.upgrade().is_none());
    assert!(ts_backing.upgrade().is_none());
    assert!(vs_backing.upgrade().is_none());
    assert!(stream.reservation.size() < raw_size);
    assert_charged(&stream);
    // EOF is the only remaining input. These literal answers prove useful
    // history, rather than cleared capacities, survived the weak assertion.
    let output = stream.next().await.unwrap().unwrap();
    assert_eq!(
        answers(&[output]),
        vec![(
            "same-series".into(),
            vec![(50_000, 1000f64.to_bits()), (60_000, 1000f64.to_bits()),]
        )]
    );
    assert!(stream.next().await.is_none());
    assert_eq!(ctx.runtime_env().memory_pool.reserved(), 0);
}

#[tokio::test]
async fn raw_and_copied_label_values_must_fit_before_consumption() {
    let name = "x".repeat(1 << 18);
    let input = batch(&[(BLOCK, 1, name.as_str(), &[(590, 1.)])], false);
    let raw_bytes = input.get_array_memory_size();
    let source = Raw(vec![input]);
    let physical = plan(&source, "{pod=~\".*\"}", &RangeQuery::new(600, 600, 1)).await;
    let ctx = limited_context(raw_bytes + std::mem::size_of::<SeriesStream>() + name.len() / 2);
    let mut stream = private_stream(lower(&physical), &ctx, 1);
    assert!(stream
        .next()
        .await
        .unwrap()
        .unwrap_err()
        .to_string()
        .contains("Resources exhausted"));
    assert!(stream.current.is_none());
    assert!(stream.pending.is_none());
    assert!(stream.next().await.is_none());
    assert_eq!(ctx.runtime_env().memory_pool.reserved(), 0);
}

#[tokio::test]
async fn larger_external_same_id_export_must_fit_raw_admission_without_sticky_capture_charge() {
    use datafusion::arrow::buffer::Buffer;
    for fits in [false, true] {
        let source = Raw(vec![batch(
            &[(BLOCK, 1, "x".repeat(32).as_str(), &[(590, 1.)])],
            true,
        )]);
        let physical = plan(&source, "{pod=~\".*\"}", &RangeQuery::new(600, 600, 1)).await;
        let ctx = limited_context(if fits { 2 << 20 } else { 1 << 19 });
        let mut stream = private_stream(lower(&physical), &ctx, 1);
        let owner = Arc::new(vec![b'x'; 1 << 20]);
        let pointer = std::ptr::NonNull::new(owner.as_ptr() as *mut u8).unwrap();
        let buffer = unsafe { Buffer::from_custom_allocation(pointer, 1 << 20, owner.clone()) };
        let mut builder = StringViewBuilder::new();
        let block = builder.append_block(buffer);
        builder.try_append_view(block, 0, 32).unwrap();
        let child: ArrayRef = Arc::new(builder.finish());
        let weak = Arc::downgrade(&child);
        let mut columns = batch(&[(BLOCK, 1, "unused", &[(599, 2.)])], true)
            .columns()
            .to_vec();
        columns[0] = Arc::new(StructArray::new(stream.fields.clone(), vec![child], None));
        let large = RecordBatch::try_new(source.0[0].schema(), columns).unwrap();
        stream.input = Some(Box::pin(RecordBatchStreamAdapter::new(
            large.schema(),
            futures::stream::iter([Ok(source.0[0].clone()), Ok(large)]),
        )));
        assert!(matches!(futures::poll!(stream.next()), Poll::Pending));
        let captured = stream.current.as_ref().unwrap().label_bytes;
        if fits {
            assert!(matches!(futures::poll!(stream.next()), Poll::Pending));
            assert!(weak.upgrade().is_none());
            assert_eq!(stream.input_bytes, 0);
            assert_eq!(stream.current.as_ref().unwrap().label_bytes, captured);
            assert!(stream.reservation.size() < 4096);
            let output = stream.next().await.unwrap().unwrap();
            assert_eq!(
                answers(&[output]),
                vec![("x".repeat(32), vec![(600, 2f64.to_bits())])]
            );
        } else {
            assert!(stream
                .next()
                .await
                .unwrap()
                .unwrap_err()
                .to_string()
                .contains("Resources exhausted"));
            assert!(stream.current.is_none());
        }
        assert!(stream.next().await.is_none());
        assert_eq!(ctx.runtime_env().memory_pool.reserved(), 0);
    }
}

#[tokio::test]
async fn one_raw_cursor_keeps_its_full_native_charge_across_three_blocks() {
    for binary in [false, true] {
        let source = Raw(vec![batch(
            &[
                (block(0, 500), 1, "unchanged-long-label", &[(400, 1.)]),
                (
                    block(500, 700),
                    1,
                    "unchanged-long-label",
                    &[(400, 1.), (600, 2.)],
                ),
                (
                    block(700, 800),
                    1,
                    "unchanged-long-label",
                    &[(400, 1.), (600, 2.), (750, 3.)],
                ),
            ],
            binary,
        )]);
        let physical = plan(&source, "{pod=~\".*\"}", &RangeQuery::new(400, 750, 50)).await;
        let ctx = SessionContext::new();
        let mut stream = private_stream(lower(&physical), &ctx, 1);
        let raw_bytes = source.0[0].get_array_memory_size();
        let mut outputs = Vec::new();
        assert!(matches!(futures::poll!(stream.next()), Poll::Pending));
        assert_eq!(stream.input_bytes, raw_bytes);
        let captured = stream.current.as_ref().unwrap().label_bytes;
        for cursor in [1, 2] {
            outputs.push(stream.next().await.unwrap().unwrap());
            assert!(stream.current.is_none());
            let (raw, from) = stream.pending.as_ref().unwrap();
            assert_eq!(*from, cursor);
            assert_eq!(stream.input_bytes, raw_bytes);
            assert_eq!(stream.input_bytes, raw.get_array_memory_size());
            assert_charged(&stream);
            assert!(matches!(futures::poll!(stream.next()), Poll::Pending));
            assert_eq!(stream.current.as_ref().unwrap().label_bytes, captured);
            assert_eq!(stream.input_bytes, if cursor == 1 { raw_bytes } else { 0 });
            assert_charged(&stream);
        }
        outputs.push(stream.next().await.unwrap().unwrap());
        assert!(stream.next().await.is_none());
        assert_eq!(ctx.runtime_env().memory_pool.reserved(), 0);
        assert_eq!(
            answers(&outputs),
            vec![
                (
                    "unchanged-long-label".into(),
                    vec![(400, 1f64.to_bits()), (450, 1f64.to_bits())]
                ),
                (
                    "unchanged-long-label".into(),
                    vec![
                        (500, 1f64.to_bits()),
                        (550, 1f64.to_bits()),
                        (600, 2f64.to_bits()),
                        (650, 2f64.to_bits())
                    ]
                ),
                (
                    "unchanged-long-label".into(),
                    vec![(700, 2f64.to_bits()), (750, 3f64.to_bits())]
                ),
            ]
        );
    }
}

#[tokio::test]
async fn sweep_window_and_finalized_output_must_fit_together_before_release() {
    let span = block(0, 7000);
    let points: Vec<_> = (0..6000)
        .map(|t| (t, if t % 2 == 0 { 4. } else { 1. }))
        .collect();
    for binary in [false, true] {
        let source = Raw(vec![batch(&[(span, 1, "pod", &points)], binary)]);
        let physical = plan(
            &source,
            "rate({pod=~\".*\"}[6s])",
            &RangeQuery::new(3000, 6000, 3000),
        )
        .await;
        let exec = lower(&physical);
        let limit = 1_000_000;
        let ctx = limited_context(limit);
        let mut stream = private_stream(exec, &ctx, 1);
        assert!(matches!(futures::poll!(stream.next()), Poll::Pending));
        assert!(stream.pending.is_none());
        assert_eq!(stream.input_bytes, 0);
        let current = stream.current.as_ref().unwrap();
        assert_eq!(current.fold.out.len(), 1);
        let old_heap = current.fold.heap_size();
        assert!(old_heap > 1000 * std::mem::size_of::<usize>());
        let context = &stream.block.as_ref().unwrap().context;
        let (ts, vs): (Vec<_>, Vec<_>) = points.iter().copied().unzip();
        let mut finalized = state::SeriesFold::new(context);
        finalized.push(context, &ts, &vs);
        finalized.finish_output(context);
        assert_eq!(finalized.out.len(), 2);
        let growth = finalized.heap_size() - old_heap;
        assert!(growth > 8);
        finalized.release_input();
        assert!(finalized.heap_size() < old_heap);
        let state_bytes = stream.size().unwrap();
        let other =
            MemoryConsumer::new("concurrent query").register(&ctx.runtime_env().memory_pool);
        other.try_resize(limit - state_bytes - growth + 1).unwrap();
        let error = stream.next().await.unwrap().unwrap_err();
        assert!(error.to_string().contains("Resources exhausted"));
        assert_eq!(exec.metrics.clone_inner().output_rows().unwrap(), 0);
        assert!(stream.current.is_none());
        assert!(stream.next().await.is_none());
        assert_eq!(ctx.runtime_env().memory_pool.reserved(), other.size());
        drop(other);
        assert_eq!(ctx.runtime_env().memory_pool.reserved(), 0);
    }
}

#[tokio::test]
async fn sample_budget_lookahead_seals_once_and_owns_only_one_binary_id() {
    let span = block(0, 800_000);
    let points = [(0, 1.), (599_999, 2.)];
    let source = Raw(vec![batch(
        &[(span, 9, "a", &points), (span, 3, "b", &points)],
        true,
    )]);
    let range = RangeQuery {
        lookback_ms: 800_000,
        ..RangeQuery::new(0, 600_000, 1)
    };
    let physical = plan(&source, "{pod=~\".*\"}", &range).await;
    let ctx = SessionContext::new();
    let mut stream = private_stream(lower(&physical), &ctx, 8);
    let first = stream.next().await.unwrap().unwrap();
    assert_eq!(first.num_rows(), 1);
    assert_eq!(first.column(1).as_list::<i32>().value_length(0), 600_001);
    let current = stream.current.as_ref().unwrap();
    assert!(current.sealed);
    assert_eq!(current.id.heap_size(), 16);
    assert_eq!(current.fold.out.len(), 600_001);
    let before = current.fold.heap_size();
    assert_charged(&stream);
    stream
        .current
        .as_mut()
        .unwrap()
        .fold
        .finish_output(&stream.block.as_ref().unwrap().context);
    assert_eq!(stream.current.as_ref().unwrap().fold.heap_size(), before);
    let second = stream.next().await.unwrap().unwrap();
    assert_eq!(second.num_rows(), 1);
    assert_eq!(second.column(1).as_list::<i32>().value_length(0), 600_001);
    assert!(stream.current.is_none());
    assert!(stream.next().await.is_none());
    assert_eq!(ctx.runtime_env().memory_pool.reserved(), 0);
}

#[tokio::test]
async fn one_maximum_supported_grid_row_owns_only_its_output_backing() {
    let span = block(0, 2_000_000);
    let max = crate::aggregate::MAX_STEPS;
    let range = RangeQuery {
        lookback_ms: 2_000_000,
        ..RangeQuery::new(0, max as i64 - 1, 1)
    };
    let source = Raw(vec![batch(&[(span, 1, "a", &[(0, 1.)])], false)]);
    let physical = plan(&source, "{pod=~\".*\"}", &range).await;
    let ctx = SessionContext::new();
    let output = collect(physical, ctx.task_ctx()).await.unwrap();
    assert_eq!(output.len(), 1);
    assert_eq!(output[0].num_rows(), 1);
    assert_eq!(output[0].column(1).as_list::<i32>().values().len(), max);
    assert_eq!(ctx.runtime_env().memory_pool.reserved(), 0);
}

#[tokio::test]
async fn copied_current_labels_and_results_must_fit_with_destination_builders() {
    let name = "x".repeat(1 << 18);
    let source = Raw(vec![batch(
        &[(BLOCK, 1, name.as_str(), &[(590, 1.)])],
        false,
    )]);
    let physical = plan(&source, "{pod=~\".*\"}", &RangeQuery::new(600, 600, 1)).await;
    let ctx = limited_context(2 << 20);
    let mut stream = private_stream(lower(&physical), &ctx, 1);
    assert!(matches!(futures::poll!(stream.next()), Poll::Pending));
    let state_bytes = stream.size().unwrap();
    let other = MemoryConsumer::new("concurrent query").register(&ctx.runtime_env().memory_pool);
    other
        .try_resize((2 << 20) - state_bytes - name.len() / 2)
        .unwrap();
    assert!(stream
        .next()
        .await
        .unwrap()
        .unwrap_err()
        .to_string()
        .contains("Resources exhausted"));
    assert!(stream.current.is_none());
    assert!(stream.output.is_none());
    assert!(stream.next().await.is_none());
    assert_eq!(ctx.runtime_env().memory_pool.reserved(), other.size());
    drop(other);
    assert_eq!(ctx.runtime_env().memory_pool.reserved(), 0);
}

#[tokio::test]
async fn upstream_pending_flushes_finished_rows_without_closing_the_open_series() {
    let source = Raw(vec![batch(
        &[
            (BLOCK, 9, "first", &[(590, 1.)]),
            (BLOCK, 3, "second", &[(590, 2.)]),
        ],
        false,
    )]);
    let physical = plan(&source, "{pod=~\".*\"}", &RangeQuery::new(600, 600, 1)).await;
    let ctx = SessionContext::new();
    let mut stream = private_stream(lower(&physical), &ctx, 8);
    stream.input = Some(Box::pin(RecordBatchStreamAdapter::new(
        source.0[0].schema(),
        futures::stream::iter([Ok(source.0[0].clone())]).chain(futures::stream::pending()),
    )));
    let output = stream.next().await.unwrap().unwrap();
    assert_eq!(
        answers(&[output]),
        vec![("first".into(), vec![(600, 1f64.to_bits())])]
    );
    assert!(stream
        .current
        .as_ref()
        .unwrap()
        .id
        .matches(source.0[0].column(4), 1));
    assert!(!stream.current.as_ref().unwrap().sealed);
    assert!(stream.output.is_none());
    assert!(stream.pending.is_none());
    assert!(matches!(futures::poll!(stream.next()), Poll::Pending));
    assert_charged(&stream);
    drop(stream);
    assert_eq!(ctx.runtime_env().memory_pool.reserved(), 0);
}

#[tokio::test]
async fn canonical_output_native_sum_controls_admission_including_projected_away_arrays() {
    // Independent array construction pins the native checkpoint byte-for-byte.
    // Builder charging is tested separately; only block arrays are allocated
    // after builder ownership moves to Arrow here.
    for projection in [0, 1, 2] {
        for fits in [false, true] {
            let source = Raw(vec![batch(
                &[(BLOCK, 1, "a-long-label-value", &[(590, 1.)])],
                false,
            )]);
            let physical = plan(&source, "{pod=~\".*\"}", &RangeQuery::new(600, 600, 1)).await;
            let base = lower(&physical);
            let mut exec =
                SeriesEvalExec::try_new(base.input.clone(), base.kernel, base.grid).unwrap();
            if projection != 0 {
                let columns = if projection == 1 { vec![0] } else { vec![] };
                exec = exec.with_projection(columns).unwrap();
            }
            let limit = 1_000_000;
            let ctx = limited_context(limit);
            let mut stream = private_stream(&exec, &ctx, 1);
            assert!(matches!(futures::poll!(stream.next()), Poll::Pending));
            stream.finish_series().unwrap();
            let output = stream.output.as_ref().unwrap();
            let mut samples = SamplesBuilder::default();
            samples.push(600, 1.);
            samples.finish_row();
            let samples = samples.take_all();
            let [start, end] = BLOCK.columns(1);
            let labels = if projection == 2 {
                None
            } else {
                let mut builder = StringViewBuilder::with_capacity(0);
                builder.append_value("a-long-label-value");
                Some(StructArray::new(
                    stream.fields.clone(),
                    vec![Arc::new(builder.finish())],
                    None,
                ))
            };
            let arrays = samples.get_array_memory_size()
                + start.get_array_memory_size()
                + end.get_array_memory_size()
                + labels
                    .as_ref()
                    .map_or(0, |labels| labels.get_array_memory_size());
            // Only the inline empty builder/vector headers survive finish;
            // retain those charges exactly as production does at the checkpoint.
            let empty_label_bytes = output.labels.as_ref().map_or(0, |builders| {
                builders.capacity() * std::mem::size_of::<StringViewBuilder>()
            });
            let after_finish = std::mem::size_of::<SeriesStream>()
                + std::mem::size_of::<i32>()
                + empty_label_bytes;
            let expected = after_finish + arrays;
            assert!(expected >= stream.reservation.size());
            let extra = if fits { expected } else { expected - 1 };
            let other =
                MemoryConsumer::new("concurrent query").register(&ctx.runtime_env().memory_pool);
            other.try_resize(limit - extra).unwrap();
            if fits {
                let batch = stream.next().await.unwrap().unwrap();
                assert_eq!(batch.num_rows(), 1);
                if projection == 1 {
                    assert_eq!(batch.num_columns(), 1);
                    assert_eq!(batch.schema().field(0).name(), LABELS);
                }
                if projection == 2 {
                    assert_eq!(batch.num_columns(), 0);
                }
            } else {
                assert!(stream
                    .next()
                    .await
                    .unwrap()
                    .unwrap_err()
                    .to_string()
                    .contains("Resources exhausted"));
                assert!(stream.current.is_none());
                assert!(stream.output.is_none());
            }
            assert!(stream.next().await.is_none());
            assert_eq!(ctx.runtime_env().memory_pool.reserved(), other.size());
            drop(other);
            assert_eq!(ctx.runtime_env().memory_pool.reserved(), 0);
        }
    }
}
