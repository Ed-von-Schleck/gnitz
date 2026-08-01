//! Shard compaction: N-way (PK, payload) merge of sorted shard files, routed to
//! per-guard output shards.
//!
//! [`open_shards`] maps the inputs; [`compact_routed`] is the sole orchestrator —
//! open → merge → route → column-first scatter → one output shard per guard run.
//! The N-way merge + inline-consolidation kernel itself is the shared
//! [`run_merge`](super::super::merge::run_merge) (the sole pending-group
//! drain owner; re-extracting a local drain loop would fork the
//! (PK, payload) total order); this module only drives it and materializes
//! survivors. [`compact_shards`] (single-target) and [`merge_and_route`]
//! (multi-target L0→L1 / vertical) are thin wrappers over `compact_routed` that
//! differ only in shard naming and whether an empty guard still emits a 0-row shard.

use std::ffi::CStr;

use super::super::batch::write_to_batch;
use super::super::error::StorageError;
use super::super::merge::{run_merge, UnifiedSource};
use super::super::scatter::scatter_unified_sources_with_weights;
use super::super::shard_file::ShardWriteOpts;
use super::super::shard_reader::MappedShard;
use crate::schema::key::pack_pk_be;
use crate::schema::SchemaDescriptor;

// ---------------------------------------------------------------------------
// Shard open + guard lookup
// ---------------------------------------------------------------------------

/// Open the input shards into owned `MappedShard`s, validating checksums. File
/// I/O lives here so the monomorphised merge loop in [`run_merge`] carries no
/// duplicated open/error code; the differential-test oracle reuses it too.
pub(super) fn open_shards(input_files: &[&CStr], schema: &SchemaDescriptor) -> Result<Vec<MappedShard>, StorageError> {
    let mut shards: Vec<MappedShard> = Vec::with_capacity(input_files.len());
    for f in input_files {
        shards.push(MappedShard::open(f, schema, true)?);
    }
    Ok(shards)
}

/// Guard owning `key` (see [`super::super::super::guard_slot`]). Test-only: the
/// production split is [`compact_routed`]'s per-guard `partition_point` over the
/// sorted survivor buffer, and the differential oracles route row-at-a-time
/// through this instead so the two derivations stay independent.
#[cfg(test)]
pub(super) fn find_guard_for_key(guard_keys: &[u128], key: u128) -> usize {
    crate::storage::lsm::guard_slot(guard_keys, key, |&g| g)
}

// ---------------------------------------------------------------------------
// The routed compaction core
// ---------------------------------------------------------------------------

/// Sole owner of shard-compaction orchestration: open the inputs, run the N-way
/// (PK, payload) merge into a survivor buffer, route each survivor to its guard,
/// and write one column-first output shard per guard run, named
/// `name_for(guard_key)` — the destination guard's stable key.
///
/// `emit_empty_guards` decides a guard with no survivors: `false` skips it
/// (multi-target — an empty guard must not register an L1 shard); `true` still
/// writes its 0-row shard (single-target — the caller owes exactly one file).
///
/// Returns `(guard_key, path)` per written shard in increasing guard-index order.
/// On an overlong path or a write error, every shard already written this call is
/// removed before returning `Err` (atomic-or-nothing).
pub(super) fn compact_routed(
    input_files: &[&CStr],
    guard_keys: &[u128],
    schema: &SchemaDescriptor,
    emit_empty_guards: bool,
    name_for: &mut dyn FnMut(u128) -> String,
) -> Result<Vec<(u128, String)>, StorageError> {
    // An empty guard list would make find_guard_for_key index a nonexistent
    // guard; every caller passes a non-empty list, but don't rely on it silently.
    assert!(!guard_keys.is_empty(), "compact_routed requires at least one guard");

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
    // below `guard_keys[0]`, which is what `find_guard_for_key`'s clamp does.
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

    // Roll back every shard already written this call (overlong-path / write
    // failure) so a compaction that can't finalize leaves L0 intact.
    fn unlink_written(out: &[(u128, String)]) {
        for (_, written) in out {
            let _ = std::fs::remove_file(written);
        }
    }

    for g in 0..guard_keys.len() {
        let bucket = &survivors[bounds[g]..bounds[g + 1]];
        if bucket.is_empty() && !emit_empty_guards {
            continue;
        }
        let path = name_for(guard_keys[g]);
        if path.len() >= 256 {
            unlink_written(&out);
            return Err(StorageError::InvalidPath);
        }
        // Reserve this run's row-proportional share of the blob arena, not the
        // whole `total_blob` per guard: a string-heavy split would otherwise
        // malloc the full arena once per guard (and `DirectWriter` still grows it
        // if a run's strings exceed the estimate). Widen to u128 for the product
        // so a huge (blob-bytes × rows) intermediate can't overflow the estimate.
        let blob_cap = (total_blob as u128 * bucket.len() as u128 / nsurv.max(1) as u128).max(1) as usize;
        let batch = write_to_batch(schema, bucket.len(), blob_cap, |writer| {
            scatter_unified_sources_with_weights(&unified, bucket, writer);
        });
        let cpath = match super::super::cstr(path.as_str()) {
            Ok(c) => c,
            Err(e) => {
                unlink_written(&out);
                return Err(e);
            }
        };
        if let Err(e) = batch.write_as_shard(&cpath, schema, ShardWriteOpts::COMPACTION) {
            unlink_written(&out);
            return Err(e);
        }
        out.push((guard_keys[g], path));
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// Public wrappers
// ---------------------------------------------------------------------------

/// Compact `input_files` into exactly one output shard. A thin single-guard
/// wrapper over [`compact_routed`]: every survivor routes to guard 0
/// (`find_guard_for_key(&[0], k) = 0` for all `k`), and `emit_empty_guards = true`
/// guarantees the one shard even when every row cancels.
pub fn compact_shards(
    input_files: &[&CStr],
    output_file: &CStr,
    schema: &SchemaDescriptor,
) -> Result<(), StorageError> {
    let path = output_file.to_str().unwrap_or("").to_string();
    compact_routed(input_files, &[0], schema, true, &mut |_| path.clone())?;
    Ok(())
}

/// Compact `input_files` across `guard_keys` into one column-first output shard
/// per non-empty guard, each named by the compaction grammar
/// (`naming::compact_shard_name` — see its collision-freedom notes).
pub fn merge_and_route(
    input_files: &[&CStr],
    output_dir: &CStr,
    guard_keys: &[u128],
    schema: &SchemaDescriptor,
    table_id: u32,
    level_num: u32,
    compact_seq: u64,
) -> Result<Vec<(u128, String)>, StorageError> {
    let dir = output_dir.to_str().unwrap_or("").to_string();
    compact_routed(input_files, guard_keys, schema, false, &mut |gk| {
        format!(
            "{dir}/{}",
            super::super::naming::compact_shard_name(table_id, compact_seq, level_num as usize, gk)
        )
    })
}
