//! Shard compaction: N-way (PK, payload) merge of sorted shard files, routed to
//! per-guard output shards.
//!
//! [`open_shards`] maps the inputs; [`merge_and_route`] orchestrates —
//! open → merge → route → column-first scatter → one output shard per guard run.
//! The merge kernel itself is the shared
//! [`run_merge`](super::super::merge::run_merge), which owns the (PK, payload)
//! total order; this module only drives it and materializes survivors.

use std::ffi::CStr;

use super::super::batch::write_to_batch;
use super::super::error::StorageError;
use super::super::merge::{run_merge, UnifiedSource};
use super::super::scatter::scatter_unified_sources_with_weights;
use super::super::shard_file::ShardWriteOpts;
use super::super::shard_reader::MappedShard;
use crate::schema::key::pack_pk_be;
use crate::schema::SchemaDescriptor;

/// Open the input shards into owned `MappedShard`s, validating checksums. File
/// I/O lives here so the monomorphised merge loop in [`run_merge`] carries no
/// duplicated open/error code; the differential-test oracle reuses it too.
pub(super) fn open_shards(input_files: &[&CStr], schema: &SchemaDescriptor) -> Result<Vec<MappedShard>, StorageError> {
    input_files.iter().map(|f| MappedShard::open(f, schema, true)).collect()
}

/// Guard owning `key` (see [`super::super::super::guard_slot`]). Test-only: the
/// production split is [`merge_and_route`]'s per-guard `partition_point` over the
/// sorted survivor buffer, and the differential oracles route row-at-a-time
/// through this instead so the two derivations stay independent.
#[cfg(test)]
pub(super) fn find_guard_for_key(guard_keys: &[u128], key: u128) -> usize {
    crate::storage::lsm::guard_slot(guard_keys, key, |&g| g)
}

/// Compact `input_files` across `guard_keys`: run the N-way (PK, payload) merge
/// into a survivor buffer, route each survivor to its guard, and write one
/// column-first output shard per non-empty guard into `output_dir`, named by the
/// compaction grammar (`naming::compact_shard_name`).
///
/// Returns `(guard_key, path)` per written shard in increasing guard-index order.
/// On a write error every shard already written this call is removed before
/// returning `Err` (atomic-or-nothing).
pub fn merge_and_route(
    input_files: &[&CStr],
    output_dir: &str,
    guard_keys: &[u128],
    schema: &SchemaDescriptor,
    table_id: u32,
    level_num: u32,
    compact_seq: u64,
) -> Result<Vec<(u128, String)>, StorageError> {
    // An empty guard list would drop every survivor on the floor while the caller
    // went on to clear the source tier — silent data loss, so reject it.
    assert!(!guard_keys.is_empty(), "merge_and_route requires at least one guard");

    let shards = open_shards(input_files, schema)?;
    let counts: Vec<usize> = shards.iter().map(|s| s.count).collect();
    let total_rows: usize = counts.iter().sum(); // survivor upper bound
    let total_blob: usize = shards.iter().map(|s| s.blob_len).sum();

    // Phase 1 — merge into survivors, sorted (PK, payload). The emit stays a bare
    // push: `pack_pk_be` preserves the merge order, so each guard's survivors are
    // one contiguous run and the split points are `guard_keys.len()` binary
    // searches over the finished buffer rather than a guard lookup per row.
    let mut survivors: Vec<(u32, u32, i64)> = Vec::with_capacity(total_rows);
    run_merge(&shards, &counts, schema, |src, row, w| {
        survivors.push((src as u32, row as u32, w));
    });

    // `bounds[g]..bounds[g + 1]` is guard `g`'s slice. Guard 0 also owns anything
    // below `guard_keys[0]`, matching the read router's saturating guard slot.
    let prefix_at = |&(src, row, _): &(u32, u32, i64)| pack_pk_be(shards[src as usize].get_pk_bytes(row as usize));
    let bounds: Vec<usize> = std::iter::once(0)
        .chain((1..guard_keys.len()).map(|g| survivors.partition_point(|s| prefix_at(s) < guard_keys[g])))
        .chain(std::iter::once(survivors.len()))
        .collect();

    // Phase 2 — one shard per guard, each scattered column-at-a-time from its
    // contiguous survivor slice. The `UnifiedSource` views hold raw pointers into
    // each shard's mmap (no lifetime tie); `shards` outlives them and every
    // scatter, all within this call.
    let unified: Vec<UnifiedSource> = shards.iter().map(|s| s.to_unified(schema)).collect();
    let nsurv = survivors.len();
    let mut out: Vec<(u128, String)> = Vec::with_capacity(guard_keys.len());

    for (g, &guard_key) in guard_keys.iter().enumerate() {
        let bucket = &survivors[bounds[g]..bounds[g + 1]];
        if bucket.is_empty() {
            continue;
        }
        let path = format!(
            "{output_dir}/{}",
            super::super::naming::compact_shard_name(table_id, compact_seq, level_num as usize, guard_key)
        );
        // Reserve this run's row-proportional share of the blob arena, not the
        // whole `total_blob` per guard: a string-heavy split would otherwise
        // malloc the full arena once per guard (and `DirectWriter` still grows it
        // if a run's strings exceed the estimate). Widen to u128 for the product
        // so a huge (blob-bytes × rows) intermediate can't overflow the estimate.
        let blob_cap = (total_blob as u128 * bucket.len() as u128 / nsurv.max(1) as u128).max(1) as usize;
        let batch = write_to_batch(schema, bucket.len(), blob_cap, |writer| {
            scatter_unified_sources_with_weights(&unified, bucket, writer);
        });
        let written = super::super::cstr(path.as_str())
            .and_then(|cpath| batch.write_as_shard(&cpath, schema, ShardWriteOpts::COMPACTION));
        if let Err(e) = written {
            // Roll back this call's shards so a compaction that cannot finalize
            // leaves the source tier intact.
            for (_, f) in &out {
                let _ = std::fs::remove_file(f);
            }
            return Err(e);
        }
        out.push((guard_key, path));
    }
    Ok(out)
}
