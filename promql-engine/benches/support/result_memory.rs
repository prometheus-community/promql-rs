//! Result accounting independent of the allocator probe. Run the owner walk
//! **after** reading allocator peaks: building Arrow ArrayData/owner maps is
//! measurement overhead, not query state.

use std::collections::HashMap;

use datafusion::arrow::array::{ArrayData, AsArray, RecordBatch};
use datafusion::arrow::buffer::Buffer;
use promql_engine::series::{LABELS, SAMPLES};

/// Logical payload: visible sample pairs (16 bytes each) plus UTF-8 label
/// value bytes. Excludes offsets, views, block columns, capacities and headers.
/// A repeated label value counts once per logical occurrence, not per owner.
pub fn logical_bytes(batch: &RecordBatch) -> usize {
    let samples = batch.column_by_name(SAMPLES).unwrap().as_list::<i32>();
    let points = samples.offsets()[batch.num_rows()] - samples.offsets()[0];
    let labels = batch.column_by_name(LABELS).unwrap().as_struct();
    let label_bytes: usize = labels
        .columns()
        .iter()
        .map(|column| {
            column
                .as_string_view()
                .iter()
                .flatten()
                .map(str::len)
                .sum::<usize>()
        })
        .sum();
    points as usize * 16 + label_bytes
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct Retained {
    /// Known Arrow backing allocation capacities, including hidden portions
    /// of slices, counted once across every column and batch. Not exclusive
    /// ownership: buffers also held by a source still keep the result alive.
    pub bytes: usize,
    /// Custom/external owners expose capacity zero. Do not invent their size
    /// from a visible slice; report that this part of retention is unknown.
    pub external_owners: usize,
}

/// Does not count schemas, array/RecordBatch headers, Arc control blocks or
/// Vec capacities. Those belong to the allocator's held/peak readings.
/// Owner base addresses (not slice pointers) remain valid throughout this
/// walk because the caller holds every batch. Never reuse this set across
/// drained streaming batches: allocators can recycle their addresses.
pub fn retained(batches: &[RecordBatch]) -> Retained {
    fn buffer(owners: &mut HashMap<usize, usize>, buffer: &Buffer) {
        if buffer.capacity() != 0 || !buffer.is_empty() {
            owners
                .entry(buffer.data_ptr().as_ptr() as usize)
                .and_modify(|size| *size = (*size).max(buffer.capacity()))
                .or_insert(buffer.capacity());
        }
    }
    fn visit(owners: &mut HashMap<usize, usize>, data: &ArrayData) {
        for b in data.buffers() {
            buffer(owners, b);
        }
        if let Some(nulls) = data.nulls() {
            buffer(owners, nulls.buffer());
        }
        for child in data.child_data() {
            visit(owners, child);
        }
    }
    let mut owners = HashMap::new();
    for batch in batches {
        for column in batch.columns() {
            visit(&mut owners, &column.to_data());
        }
    }
    Retained {
        bytes: owners.values().sum(),
        external_owners: owners.values().filter(|&&size| size == 0).count(),
    }
}
