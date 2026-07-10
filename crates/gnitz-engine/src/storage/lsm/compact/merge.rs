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
use super::super::shard_file::{PkUniqueChecker, ShardWriteOpts};
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

/// Guard owning `key` (see [`super::super::super::guard_slot`]). Monotone in
/// `key`, so over the sorted merge stream each guard's survivors form one
/// contiguous run.
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
    can_tag_pk_unique: bool,
    emit_empty_guards: bool,
    mut name_for: impl FnMut(u128) -> String,
) -> Result<Vec<(u128, String)>, StorageError> {
    // An empty guard list would make find_guard_for_key index a nonexistent
    // guard; every caller passes a non-empty list, but don't rely on it silently.
    assert!(!guard_keys.is_empty(), "compact_routed requires at least one guard");

    let shards = open_shards(input_files, schema)?;
    let counts: Vec<usize> = shards.iter().map(|s| s.count).collect();
    let total_rows: usize = counts.iter().sum(); // survivor upper bound
    let total_blob: usize = shards.iter().map(|s| s.blob_len).sum();

    // Phase 1 — merge into survivors (sorted (PK, payload)), counting the rows
    // routed to each guard. `find_guard_for_key` is monotone over the sorted
    // stream, so each guard's survivors are contiguous; observing on that sorted
    // stream is also what lets `PkUniqueChecker` spot adjacent duplicate PKs.
    // Checkers exist only when tagging is on — a never-observed checker
    // vacuously qualifies, which must not leak a spurious PK_UNIQUE tag.
    let mut survivors: Vec<(u32, u32, i64)> = Vec::with_capacity(total_rows);
    let mut checkers: Option<Vec<PkUniqueChecker>> =
        can_tag_pk_unique.then(|| (0..guard_keys.len()).map(|_| PkUniqueChecker::new()).collect());
    let mut run_len = vec![0usize; guard_keys.len()];
    if guard_keys.len() == 1 && !can_tag_pk_unique {
        // Single-guard, untagged (every view/scratch-table compaction): no
        // routing and no observation — the emit is a bare survivor push.
        run_merge(&shards, &counts, schema, |src, row, w| {
            survivors.push((src as u32, row as u32, w));
        });
        run_len[0] = survivors.len();
    } else {
        run_merge(&shards, &counts, schema, |src, row, w| {
            survivors.push((src as u32, row as u32, w));
            let pk = shards[src].get_pk_bytes(row);
            let prefix = pack_pk_be(pk);
            let g = find_guard_for_key(guard_keys, prefix);
            if let Some(checkers) = checkers.as_mut() {
                checkers[g].observe(prefix, pk, w);
            }
            run_len[g] += 1;
        });
    }

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

    let mut start = 0;
    for g in 0..guard_keys.len() {
        let bucket = &survivors[start..start + run_len[g]];
        start += run_len[g];
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
        let opts = ShardWriteOpts {
            durable: true, // compaction outputs must survive a crash on their own
            flags: checkers.as_ref().map_or(0, |c| c[g].flags()),
            pack_ints: true,
        };
        if let Err(e) = batch.write_as_shard(&cpath, schema, opts) {
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
    can_tag_pk_unique: bool,
) -> Result<(), StorageError> {
    let path = output_file.to_str().unwrap_or("").to_string();
    compact_routed(input_files, &[0], schema, can_tag_pk_unique, true, |_| path.clone())?;
    Ok(())
}

/// Compact `input_files` across `guard_keys` into one column-first output shard
/// per non-empty guard, each named by the compaction grammar
/// (`naming::compact_shard_name` — see its collision-freedom notes).
#[allow(clippy::too_many_arguments)]
pub fn merge_and_route(
    input_files: &[&CStr],
    output_dir: &CStr,
    guard_keys: &[u128],
    schema: &SchemaDescriptor,
    table_id: u32,
    level_num: u32,
    compact_seq: u64,
    can_tag_pk_unique: bool,
) -> Result<Vec<(u128, String)>, StorageError> {
    let dir = output_dir.to_str().unwrap_or("").to_string();
    compact_routed(input_files, guard_keys, schema, can_tag_pk_unique, false, move |gk| {
        format!(
            "{dir}/{}",
            super::super::naming::compact_shard_name(table_id, compact_seq, level_num as usize, gk)
        )
    })
}
