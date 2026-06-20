//! N-way (PK, payload) merge kernel for shard compaction.
//!
//! Opens the input shards, builds per-shard cursors + the keyless loser tree,
//! and runs the fused merge + inline consolidation loop through `drive_merge`
//! (§8 cluster-1 — the sole pending-group drain owner). The
//! `with_payload_cmp!` / `with_pk_ord!` dispatch selects the payload comparator
//! then the stride once each, never per comparison, so the inner loop is fully
//! monomorphised. `merge_and_route` is the routing-aware merge that splits the
//! consolidated stream across the guard buckets.

use std::ffi::CStr;

use super::super::batch::Batch;
use super::super::error::StorageError;
use super::super::merge::{pack_pk_be, run_merge, BlobCacheGuard};
use super::super::shard_file::PkUniqueChecker;
use super::super::shard_reader::MappedShard;
use super::route::{find_guard_for_key, GuardResult};
use crate::schema::SchemaDescriptor;

// ---------------------------------------------------------------------------
// Shard compaction merge: open the shards, drive the shared `run_merge`
// ---------------------------------------------------------------------------

/// Open the input shards and run the shared N-way (PK, payload) merge +
/// consolidation ([`run_merge`]) over them. Calls `emit(key, net_weight, shard,
/// row)` for each surviving consolidated row, where `key` is the row's
/// `pack_pk_be` guard-routing key — re-derived from its OPK bytes in the emit,
/// since the keyless heap carries no cached key. Ghost (weight-0) rows are
/// skipped inside `run_merge` via `MappedShard`'s `MergeSource::next_non_ghost`.
///
/// File I/O lives here so the monomorphised merge loop in `run_merge` carries no
/// duplicated open/error code.
pub(super) fn open_and_merge(
    input_files: &[&CStr],
    schema: &SchemaDescriptor,
    mut emit: impl FnMut(u128, i64, &MappedShard, usize),
) -> Result<(), StorageError> {
    let mut shards: Vec<MappedShard> = Vec::with_capacity(input_files.len());
    for f in input_files {
        shards.push(MappedShard::open(f, schema, true)?);
    }

    run_merge(&shards, schema, |src, row, w| {
        emit(pack_pk_be(shards[src].get_pk_bytes(row)), w, &shards[src], row);
    });
    Ok(())
}

// ---------------------------------------------------------------------------
// Routing-aware merge entry point
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
pub fn merge_and_route(
    input_files: &[&CStr],
    output_dir: &CStr,
    guard_keys: &[u128],
    schema: &SchemaDescriptor,
    table_id: u32,
    level_num: u32,
    lsn_tag: u64,
    out_results: &mut [GuardResult],
    can_tag_pk_unique: bool,
) -> Result<usize, StorageError> {
    // Empty guard_keys would make find_guard_for_key return 0 (via
    // saturating_sub(1)) and then index batches[0] out of bounds. Callers
    // always pass a non-empty list, but don't rely on that silently.
    if guard_keys.is_empty() {
        return Err(StorageError::InvalidPath);
    }
    let num_guards = guard_keys.len();
    let out_dir_str = output_dir.to_str().unwrap_or("");

    let mut batches: Vec<Batch> = (0..num_guards).map(|_| Batch::with_schema(*schema, 256)).collect();
    let mut blob_caches: Vec<BlobCacheGuard> = (0..num_guards).map(|_| BlobCacheGuard::acquire(schema, 256)).collect();
    let mut checkers: Vec<PkUniqueChecker> = (0..num_guards).map(|_| PkUniqueChecker::new()).collect();
    let out_filenames: Vec<String> = (0..num_guards)
        .map(|i| format!("{out_dir_str}/shard_{table_id}_{lsn_tag}_L{level_num}_G{i}.db"))
        .collect();

    // `key` is the order-preserving sort key — the guard-routing key (matching
    // `l1_guard_keys`, now also OPK). The PK itself is copied from the source
    // bytes so wide PKs are not truncated.
    open_and_merge(input_files, schema, |key, weight, shard, row| {
        let gi = find_guard_for_key(guard_keys, key);
        let pk_bytes = shard.get_pk_bytes(row);
        if can_tag_pk_unique {
            checkers[gi].observe(pk_bytes, weight);
        }
        batches[gi].append_row_from_source_bytes(pk_bytes, weight, shard, row, blob_caches[gi].get_mut());
    })?;

    // Validate all output paths fit in GuardResult.filename before writing anything.
    for i in 0..num_guards {
        if batches[i].count > 0 && out_filenames[i].len() >= 256 {
            return Err(StorageError::InvalidPath);
        }
    }

    let mut result_count: usize = 0;
    for i in 0..num_guards {
        if batches[i].count == 0 {
            continue;
        }
        let cpath = std::ffi::CString::new(out_filenames[i].as_str()).unwrap();
        let flags = checkers[i].flags_if(can_tag_pk_unique);
        if let Err(e) = batches[i].write_as_shard_with_flags(&cpath, table_id, flags) {
            // Only guards with rows were written (the loop `continue`s on empty
            // shards), so only those filenames exist on disk. Removing a path for
            // an empty guard could `unlink` an unrelated file sharing the name.
            for (j, fname) in out_filenames.iter().enumerate().take(i) {
                if batches[j].count > 0 {
                    let _ = std::fs::remove_file(fname);
                }
            }
            return Err(e);
        }
        let ri = result_count;
        assert!(
            ri < out_results.len(),
            "merge_and_route: out_results buffer too small ({} slots for {} guards)",
            out_results.len(),
            num_guards,
        );
        out_results[ri].guard_key = guard_keys[i];
        let name_bytes = out_filenames[i].as_bytes();
        let len = name_bytes.len().min(255);
        out_results[ri].filename[..len].copy_from_slice(&name_bytes[..len]);
        out_results[ri].filename[len] = 0;
        result_count += 1;
    }

    Ok(result_count)
}
