//! Shard compaction: N-way (PK, payload) merge of sorted shard files, routed to
//! per-guard output shards.
//!
//! [`open_shards`] maps the inputs; [`merge_and_route`] orchestrates —
//! open → merge → route → column-first scatter → one output shard per guard run.
//! The merge kernel itself is the shared
//! [`run_merge`](super::merge::run_merge), which owns the (PK, payload) total
//! order; this module only drives it and materializes survivors.

use std::ffi::CStr;

use super::batch::write_to_batch;
use super::error::StorageError;
use super::merge::prorated_blob_cap;
use super::merge::{run_merge, ColPtr, UnifiedSource};
use super::scatter::scatter_unified_sources;
use super::shard_file::ShardWriteOpts;
use super::shard_reader::MappedShard;
use crate::schema::key::{pack_pk_be, pk_bytes_eq};
use crate::schema::SchemaDescriptor;

/// Open the input shards into owned `MappedShard`s, validating checksums. File
/// I/O lives here so the monomorphised merge loop in [`run_merge`] carries no
/// duplicated open/error code; the differential-test oracle reuses it too.
pub(super) fn open_shards(input_files: &[&CStr], schema: &SchemaDescriptor) -> Result<Vec<MappedShard>, StorageError> {
    input_files.iter().map(|f| MappedShard::open(f, schema, true)).collect()
}

/// The PK-only projection of `schema`: the same PK columns in the same PK-list
/// order — hence the same `pk_stride` and the same OPK bytes — and no payload.
/// A skeleton shard is serialized under this, so its regions are
/// `[pk, weight, null, blob]`.
pub(super) fn skeleton_schema(schema: &SchemaDescriptor) -> SchemaDescriptor {
    crate::schema::project_schema(schema, &[])
}

/// Fold `bucket` — one guard's (PK, payload)-sorted survivor slice — into one
/// `(PK, Σweight)` row per key, dropping net-zero keys. The per-PK fold is what
/// makes the output one row per key even on a guard's *first* dehydration, whose
/// survivors are still (PK, payload) groups; it also strips the payload
/// breakdown of hydrated groups sinking into an already-dehydrated guard.
///
/// A fold's per-PK sum is the PK-projection of the view integral over a
/// per-key time prefix of the store's history (fold totality: L0 is consumed
/// whole, a guard fold takes all of the guard's entries, a vertical takes the
/// whole source guard plus every overlapping destination guard), and the
/// integral is positive — so a retraction's balancing insertion is always inside
/// the prefix and the sum is never negative. `debug_assert!(w > 0)` is the
/// tripwire: a future *partial* compaction that broke fold totality would trip
/// it instead of silently corrupting bounded views.
fn fold_bucket_per_pk(shards: &[MappedShard], bucket: &[(u32, u32, i64)]) -> Vec<(u32, u32, i64)> {
    let pk_of = |&(src, row, _): &(u32, u32, i64)| shards[src as usize].get_pk_bytes(row as usize);
    bucket
        .chunk_by(|a, b| pk_bytes_eq(pk_of(a), pk_of(b)))
        .filter_map(|group| {
            let sum: i64 = group.iter().map(|&(_, _, w)| w).sum();
            (sum != 0).then(|| {
                debug_assert!(sum > 0, "skeleton fold produced a negative coarse weight");
                (group[0].0, group[0].1, sum)
            })
        })
        .collect()
}

/// Where one compaction's outputs go: the four values `naming::compact_shard_name`
/// needs, plus whether the store they belong to is ever point-probed by PK. They
/// travel together and are decided together, by `ShardIndex::compact_into`.
pub(super) struct Output<'a> {
    pub dir: &'a str,
    pub table_id: u32,
    pub level_num: u32,
    pub compact_seq: u64,
    pub skip_pk_filter: bool,
}

/// Compact `input_files` across `guards`: run the N-way (PK, payload) merge
/// into a survivor buffer, route each survivor to its guard, and write one
/// column-first output shard per non-empty guard into `dest.dir`, named by the
/// compaction grammar (`naming::compact_shard_name`).
///
/// Each destination is a `(guard_key, skeleton)` pair: a set flag writes that
/// guard's slice as a payload-free skeleton shard (one `(PK, Σweight)` row per
/// key) under [`skeleton_schema`] instead of the full-width form. A guard whose
/// keys all cancel writes no shard, skeleton or not.
///
/// Returns `(guard_key, path)` per written shard in increasing guard-index order.
/// On a write error every shard already written this call is removed before
/// returning `Err` (atomic-or-nothing).
pub(super) fn merge_and_route(
    input_files: &[&CStr],
    guards: &[(u128, bool)],
    schema: &SchemaDescriptor,
    dest: Output<'_>,
) -> Result<Vec<(u128, String)>, StorageError> {
    // An empty guard list would drop every survivor on the floor while the caller
    // went on to clear the source tier — silent data loss, so reject it.
    assert!(!guards.is_empty(), "merge_and_route requires at least one guard");

    let shards = open_shards(input_files, schema)?;
    let total_rows: usize = shards.iter().map(|s| s.count).sum(); // survivor upper bound
    let total_blob: usize = shards.iter().map(|s| s.blob_len).sum();

    // Phase 1 — merge into survivors, sorted (PK, payload). The emit stays a bare
    // push: `pack_pk_be` preserves the merge order, so each guard's survivors are
    // one contiguous run and the split points are `guards.len()` binary
    // searches over the finished buffer rather than a guard lookup per row.
    let mut survivors: Vec<(u32, u32, i64)> = Vec::with_capacity(total_rows);
    run_merge(&shards, schema, |src, row, w| {
        survivors.push((src as u32, row as u32, w));
    });

    // `bounds[g]..bounds[g + 1]` is guard `g`'s slice. Guard 0 also owns anything
    // below `guards[0]`'s key, matching the read router's saturating guard slot.
    let prefix_at = |&(src, row, _): &(u32, u32, i64)| pack_pk_be(shards[src as usize].get_pk_bytes(row as usize));
    let bounds: Vec<usize> = std::iter::once(0)
        .chain((1..guards.len()).map(|g| survivors.partition_point(|s| prefix_at(s) < guards[g].0)))
        .chain(std::iter::once(survivors.len()))
        .collect();

    // Phase 2 — one shard per guard, each scattered column-at-a-time from its
    // contiguous survivor slice. The `UnifiedSource` views hold raw pointers into
    // each shard's mmap (no lifetime tie); `shards` outlives them and every
    // scatter, all within this call.
    let mut cols: Vec<ColPtr> = Vec::new();
    let unified: Vec<UnifiedSource> = shards.iter().map(|s| s.to_unified(schema, &mut cols)).collect();
    let nsurv = survivors.len();
    let mut out: Vec<(u128, String)> = Vec::with_capacity(guards.len());

    // Only built when some destination guard is dehydrated; a hydrated store
    // never derives it.
    let skel_schema = guards.iter().any(|&(_, s)| s).then(|| skeleton_schema(schema));

    for (g, &(guard_key, skeleton)) in guards.iter().enumerate() {
        let bucket = &survivors[bounds[g]..bounds[g + 1]];
        if bucket.is_empty() {
            continue;
        }
        // Per-PK fold first, so a guard whose keys all cancel writes nothing.
        let folded = skeleton.then(|| fold_bucket_per_pk(&shards, bucket));
        if folded.as_ref().is_some_and(|f| f.is_empty()) {
            continue;
        }
        let path = format!(
            "{}/{}",
            dest.dir,
            super::naming::compact_shard_name(dest.table_id, dest.compact_seq, dest.level_num as usize, guard_key)
        );
        // A skeleton guard writes its folded rows under the PK-only schema; a
        // hydrated one writes the bucket at full width. The writer's schema drives
        // the payload loop, so under a zero-payload schema `write_to_batch` carves
        // no payload buffer and the scatter iterates zero payload columns while the
        // fused pass still writes PK and weight.
        let (wschema, rows, blob_cap, opts) = match &folded {
            Some(rows) => (
                skel_schema
                    .as_ref()
                    .expect("a skeleton guard derived a skeleton schema"),
                rows.as_slice(),
                0,
                ShardWriteOpts {
                    skip_pk_filter: dest.skip_pk_filter,
                    ..ShardWriteOpts::SKELETON
                },
            ),
            None => (
                schema,
                bucket,
                prorated_blob_cap(total_blob, nsurv, bucket.len()),
                ShardWriteOpts {
                    skip_pk_filter: dest.skip_pk_filter,
                    ..ShardWriteOpts::COMPACTION
                },
            ),
        };
        let mut batch = write_to_batch(wschema, rows.len(), blob_cap, |writer| {
            scatter_unified_sources(&unified, &cols, rows, writer);
        });
        if folded.is_some() {
            // The fused pass copied the *source's* payload null bits, which mean
            // nothing without a payload. Zeroing collapses the region to
            // `ENCODING_CONSTANT` — 8 bytes for the whole file.
            batch.null_bmp_data_mut().fill(0);
        }
        let written = super::cstr(path.as_str()).and_then(|cpath| batch.write_as_shard(&cpath, wschema, opts));
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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "tests/compact.rs"]
mod tests;
