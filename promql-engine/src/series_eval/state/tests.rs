//! Shared fold and copied label-value regressions. End-to-end lifecycle,
//! reservation, projection, and bounded-emission tests live in `exec::tests`.
use super::*;
use crate::buffer::Kernel;
use crate::error::EngineError;
use crate::params::Params;
use crate::range::{Func, RangeOp};
use datafusion::arrow::array::{Array, AsArray, StringViewArray, StringViewBuilder};
use datafusion::arrow::buffer::Buffer;
use datafusion::arrow::datatypes::{DataType, Field};

fn grid() -> Params {
    Params {
        start_ms: 0,
        end_ms: 200,
        step_ms: 50,
        window_ms: 100,
        offset_ms: 0,
        at_ms: None,
    }
}

fn export(owner: &Arc<Vec<u8>>, extent: usize) -> StructArray {
    let pointer = std::ptr::NonNull::new(owner.as_ptr() as *mut u8).unwrap();
    // A tiny visible value must not keep this opaque source owner alive.
    let buffer = unsafe { Buffer::from_custom_allocation(pointer, extent, owner.clone()) };
    let mut builder = StringViewBuilder::new();
    let block = builder.append_block(buffer);
    builder.try_append_view(block, 0, 32).unwrap();
    StructArray::new(
        vec![Field::new("pod", DataType::Utf8View, false)].into(),
        vec![Arc::new(builder.finish())],
        None,
    )
}

#[test]
fn copied_values_release_source_children_and_opaque_payload_while_values_live() {
    let owner = Arc::new(vec![b'x'; 1 << 20]);
    let payload = Arc::downgrade(&owner);
    let labels = export(&owner, 1 << 20);
    let child = Arc::downgrade(labels.column(0));
    let first = copy_label_values(&labels, 0);
    let second = copy_label_values(&labels, 0);
    assert_eq!(first, second);
    assert_ne!(first[0].as_ptr(), second[0].as_ptr());
    let expected = first.capacity() * std::mem::size_of::<String>()
        + first.iter().map(String::capacity).sum::<usize>();
    assert_eq!(label_values_heap_size(&first).unwrap(), expected);
    assert!(expected < 1024);
    drop(labels);
    drop(owner);
    assert!(child.upgrade().is_none());
    assert!(payload.upgrade().is_none());
    assert_eq!(first, vec!["x".repeat(32)]);
    assert_eq!(second, first);
}

#[test]
fn owned_value_charge_uses_actual_vector_and_string_capacity() {
    let mut values = Vec::with_capacity(17);
    for value in ["", "non-ASCII-東京", "repeated-long-label-value"] {
        let mut owned = String::with_capacity(value.len() + 100);
        owned.push_str(value);
        values.push(owned);
    }
    let expected = values.capacity() * std::mem::size_of::<String>()
        + values.iter().map(String::capacity).sum::<usize>();
    assert_eq!(label_values_heap_size(&values).unwrap(), expected);
    assert!(expected > values.iter().map(String::len).sum::<usize>());
    assert_eq!(label_values_heap_size(&Vec::new()).unwrap(), 0);
}

#[test]
fn label_capacity_overflow_is_a_resource_error() {
    assert_eq!(checked_label_capacity(0, []).unwrap(), 0);
    assert_eq!(checked_label_capacity(0, [usize::MAX]).unwrap(), usize::MAX);
    for (vector_capacity, string_capacities) in [
        (usize::MAX / std::mem::size_of::<String>() + 1, [0, 0]),
        (1, [usize::MAX, 0]),
        (0, [usize::MAX, 1]),
    ] {
        let error = checked_label_capacity(vector_capacity, string_capacities).unwrap_err();
        assert!(
            matches!(&error, DataFusionError::ResourcesExhausted(m) if m.contains("label charge overflow")),
            "{error:?}"
        );
        let error = EngineError::from(error);
        assert!(!error.is_user_error());
        assert!(
            matches!(
                error,
                EngineError::DataFusion(DataFusionError::ResourcesExhausted(_))
            ),
            "{error:?}"
        );
    }
}

#[test]
fn aliased_source_children_become_independent_owned_values_without_interning() {
    let child = Arc::new(StringViewArray::from_iter_values([
        "long-source-label-value",
    ]));
    let labels = StructArray::new(
        vec![
            Field::new("first", DataType::Utf8View, false),
            Field::new("second", DataType::Utf8View, false),
        ]
        .into(),
        vec![child.clone(), child.clone()],
        None,
    );
    let values = copy_label_values(&labels, 0);
    assert_eq!(
        values,
        vec!["long-source-label-value", "long-source-label-value"]
    );
    assert_ne!(values[0].as_ptr(), values[1].as_ptr());
    let expected = values.capacity() * std::mem::size_of::<String>()
        + values[0].capacity()
        + values[1].capacity();
    assert_eq!(label_values_heap_size(&values).unwrap(), expected);
}

#[test]
fn finalization_retains_window_and_sweep_capacity_until_release_checkpoint() {
    let context = EvalContext::new(
        Kernel::Range(RangeOp::Func(Func::Rate)),
        Params {
            start_ms: 3000,
            end_ms: 6000,
            step_ms: 3000,
            window_ms: 6000,
            ..grid()
        },
    );
    let mut fold = SeriesFold::new(&context);
    let ts: Vec<_> = (0..=3000).collect();
    let vs: Vec<_> = ts
        .iter()
        .map(|t| if t % 2 == 0 { 4. } else { 1. })
        .collect();
    fold.push(&context, &ts, &vs);
    let numeric =
        fold.iterator.as_ref().unwrap().size() - std::mem::size_of::<BufferedSeriesIterator>();
    assert!(numeric >= 1000 * std::mem::size_of::<usize>());
    fold.finish_output(&context);
    assert!(fold.iterator.is_some());
    assert_eq!(fold.out.len(), 2);
    let finalized = fold.heap_size();
    assert_eq!(finalized, numeric + fold.out.heap_size());
    fold.finish_output(&context);
    assert_eq!(fold.heap_size(), finalized);
    fold.release_input();
    assert!(fold.iterator.is_none());
    assert_eq!(finalized - fold.heap_size(), numeric);
    fold.finish_output(&context);
    assert_eq!(fold.out.len(), 2);
}

#[test]
fn empty_chunks_keep_the_block_grid_and_batch_owner_emits_one_empty_row() {
    for kernel in [
        Kernel::Selector(crate::selector::Pick::Value),
        Kernel::Selector(crate::selector::Pick::Timestamp),
        Kernel::Range(RangeOp::Func(Func::Rate)),
    ] {
        let context = EvalContext::new(kernel, grid());
        let mut fold = SeriesFold::new(&context);
        let initial = fold.heap_size();
        fold.push(&context, &[], &[]);
        assert_eq!(fold.heap_size(), initial);
        assert_eq!(context.params(), grid());
        fold.finish_output(&context);
        assert_eq!(fold.out.len(), 0);
        fold.finish_output(&context);
        fold.release_input();
        fold.finish_output(&context);
        assert_eq!(fold.heap_size(), 0);
        let mut output = SamplesBuilder::default();
        fold.out.append_to(&mut output);
        output.finish_row();
        let array = output.take_all();
        assert_eq!(array.len(), 1);
        assert_eq!(array.value_length(0), 0);
        assert!(!array.is_null(0));
    }
}

#[test]
fn singleton_finish_is_idempotent_before_and_after_input_release() {
    for value in [
        -0.0,
        f64::from_bits(0x7ff8_0000_0000_0017),
        f64::from_bits(crate::selector::STALE_NAN_BITS),
    ] {
        let context = EvalContext::new(
            Kernel::Selector(crate::selector::Pick::Value),
            Params {
                start_ms: 50,
                end_ms: 50,
                ..grid()
            },
        );
        let mut fold = SeriesFold::new(&context);
        fold.push(&context, &[49], &[value]);
        fold.finish_output(&context);
        let expected = usize::from(value.to_bits() != crate::selector::STALE_NAN_BITS);
        assert_eq!(fold.out.len(), expected);
        assert_eq!(fold.out.heap_size(), 0);
        fold.finish_output(&context);
        assert_eq!(fold.out.len(), expected);
        fold.release_input();
        fold.finish_output(&context);
        assert_eq!(fold.out.len(), expected);
        if let SingleSeriesSamples::Inline(Some((timestamp, output))) = fold.out {
            assert_eq!(timestamp, 50);
            assert_eq!(output.to_bits(), value.to_bits());
        }
    }
}

#[test]
fn compact_single_series_storage_preserves_bits_and_appends_to_an_open_row() {
    let points = [
        (1, -0.0),
        (2, f64::from_bits(0x7ff8_0000_0000_0017)),
        (3, f64::from_bits(crate::selector::STALE_NAN_BITS)),
        (4, f64::INFINITY),
        (5, f64::NEG_INFINITY),
    ];
    let mut samples = SingleSeriesSamples::Inline(None);
    assert_eq!(samples.len(), 0);
    assert_eq!(samples.heap_size(), 0);
    let mut output = SamplesBuilder::default();
    output.push(0, 9.);
    samples.append_to(&mut output);
    samples.push(points[0].0, points[0].1);
    assert_eq!(samples.heap_size(), 0);
    samples.append_to(&mut output);
    output.finish_row();
    for &(t, v) in &points[1..] {
        samples.push(t, v);
    }
    assert!(matches!(samples, SingleSeriesSamples::Heap { .. }));
    assert_eq!(samples.len(), points.len());
    assert!(samples.heap_size() >= points.len() * 16);
    output.push(-1, 0.);
    samples.append_to(&mut output);
    output.finish_row();
    let array = output.take_all();
    assert_eq!(array.value_offsets(), &[0, 2, 8]);
    let (ts, vs) = crate::series::sample_slices(array.values().as_struct());
    assert_eq!(ts, &[0, 1, -1, 1, 2, 3, 4, 5]);
    assert_eq!(
        vs.iter().map(|v| v.to_bits()).collect::<Vec<_>>(),
        [
            9.,
            points[0].1,
            0.,
            points[0].1,
            points[1].1,
            points[2].1,
            points[3].1,
            points[4].1
        ]
        .map(f64::to_bits)
    );
}

#[test]
fn copied_output_owns_strings_and_empty_schema_has_real_rows() {
    let long = "long-label-payload".repeat(1000);
    let fields: Fields = vec![Field::new("pod", DataType::Utf8View, false)].into();
    let labels = StructArray::new(
        fields.clone(),
        vec![Arc::new(StringViewArray::from_iter_values([long.as_str()]))],
        None,
    );
    let payload = labels.column(0).as_string_view().data_buffers()[0].data_ptr();
    let values = copy_label_values(&labels, 0);
    let output = emit_label_rows(fields, std::iter::once(&values)).unwrap();
    let output_labels = output.as_struct().column(0).as_string_view();
    assert_ne!(output_labels.data_buffers()[0].data_ptr(), payload);
    assert_ne!(output_labels.value(0).as_ptr(), values[0].as_ptr());
    drop(labels);
    drop(values);
    assert_eq!(output_labels.value(0), long);
    let empty = StructArray::new_empty_fields(3, None);
    let rows = [copy_label_values(&empty, 2), copy_label_values(&empty, 0)];
    assert_eq!(
        emit_label_rows(Fields::empty(), rows.iter()).unwrap().len(),
        2
    );
}

#[test]
fn copied_labels_keep_sliced_rows_field_order_empty_values_and_nested_metadata() {
    let metadata = std::collections::HashMap::from([("label".into(), "preserved".into())]);
    let fields: Fields = vec![
        Field::new("z", DataType::Utf8View, false).with_metadata(metadata.clone()),
        Field::new("a", DataType::Utf8View, false),
    ]
    .into();
    let labels = StructArray::new(
        fields.clone(),
        vec![
            Arc::new(StringViewArray::from_iter_values(["discard", "", "東京"])),
            Arc::new(StringViewArray::from_iter_values([
                "discard",
                "first-long-label-value",
                "",
            ])),
        ],
        None,
    )
    .slice(1, 2);
    let rows = [copy_label_values(&labels, 1), copy_label_values(&labels, 0)];
    drop(labels);
    let output = emit_label_rows(fields.clone(), rows.iter()).unwrap();
    let labels = output.as_struct();
    assert_eq!(labels.fields(), &fields);
    assert_eq!(labels.fields()[0].metadata(), &metadata);
    assert_eq!(labels.column(0).as_string_view().value(0), "東京");
    assert_eq!(labels.column(1).as_string_view().value(0), "");
    assert_eq!(labels.column(0).as_string_view().value(1), "");
    assert_eq!(
        labels.column(1).as_string_view().value(1),
        "first-long-label-value"
    );
}

#[test]
fn output_sample_count_rejects_list_offset_overflow_before_copying() {
    let max = i32::MAX as usize;
    assert_eq!(checked_sample_count([]).unwrap(), 0);
    assert_eq!(checked_sample_count([max - 1, 1, 0]).unwrap(), max);
    assert_eq!(checked_sample_count([max / 2, max / 2, 1]).unwrap(), max);
    for counts in [
        [max, 1],
        [0, max + 1],
        [max / 2 + 1, max / 2 + 1],
        [1, usize::MAX],
    ] {
        let error = checked_sample_count(counts).unwrap_err();
        assert!(
            matches!(&error, DataFusionError::Execution(m) if m.contains("i32::MAX")),
            "{error:?}"
        );
        let error = EngineError::from(error);
        assert!(!error.is_user_error());
        assert!(
            matches!(
                error,
                EngineError::DataFusion(DataFusionError::Execution(_))
            ),
            "{error:?}"
        );
    }
}
