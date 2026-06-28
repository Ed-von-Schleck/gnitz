//! Guard routing for shard compaction: the per-bucket guard-key lookup and the
//! `compact_shards` orchestration that merges N inputs into a single output shard.

use std::ffi::CStr;

use super::super::batch::write_to_batch;
use super::super::error::StorageError;
use super::super::merge::{run_merge, UnifiedSource};
use super::super::scatter::scatter_unified_sources_with_weights;
use super::super::shard_file::PkUniqueChecker;
use super::merge::open_shards;
use crate::schema::SchemaDescriptor;

pub(super) fn find_guard_for_key(guard_keys: &[u128], key: u128) -> usize {
    guard_keys.partition_point(|&g| g <= key).saturating_sub(1)
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

pub fn compact_shards(
    input_files: &[&CStr],
    output_file: &CStr,
    schema: &SchemaDescriptor,
    table_id: u32,
    can_tag_pk_unique: bool,
) -> Result<(), StorageError> {
    let shards = open_shards(input_files, schema)?;
    // `total_rows` is the survivor upper bound (sizes only the `survivors`
    // reservation); `total_blob` is the output blob upper bound (the output blob
    // is a relocated subset of the inputs').
    let total_rows: usize = shards.iter().map(|s| s.count).sum();
    let total_blob: usize = shards.iter().map(|s| s.blob_len).sum();

    // One pass: collect survivors for the column-first scatter and observe each
    // PK into the checker in the same sorted `(PK, payload)` order. `run_merge`
    // borrows `&shards` immutably, so the closure may also read `shards[src]` —
    // the exact shared-borrow shape `open_and_merge` already compiles. Observing
    // on the sorted merge stream is load-bearing: `PkUniqueChecker` disqualifies
    // on an *adjacent* duplicate, so it is correct only on a sorted PK stream.
    let mut survivors: Vec<(u32, u32, i64)> = Vec::with_capacity(total_rows);
    let mut checker = PkUniqueChecker::new();
    run_merge(&shards, schema, |src, row, w| {
        survivors.push((src as u32, row as u32, w));
        if can_tag_pk_unique {
            checker.observe(shards[src].get_pk_bytes(row), w);
        }
    });

    // Materialize the survivors column-at-a-time. The `UnifiedSource` views hold
    // raw pointers into each shard's mmap (no lifetime tie); `shards` outlives
    // them and the scatter, all within this call. Size the zeroed fixed-region
    // arena to the exact `survivors.len()` (known post-merge), not the
    // pre-cancellation `total_rows`.
    let unified: Vec<UnifiedSource> = shards.iter().map(|s| s.to_unified(schema)).collect();
    let batch = write_to_batch(schema, survivors.len(), total_blob, |writer| {
        scatter_unified_sources_with_weights(&unified, &survivors, writer);
    });
    batch.write_as_shard_with_flags(output_file, table_id, checker.flags_if(can_tag_pk_unique))
}
