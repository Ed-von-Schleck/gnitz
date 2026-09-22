//! Shard compaction: N-way (PK, payload) merge of sorted shard files, routed to
//! per-guard output shards.
//!
//! [`merge_and_route`] orchestrates — verify → merge → route → column-first
//! scatter → one output batch per guard run, handed to the caller to write.
//! The merge kernel itself is the shared
//! [`run_merge`](super::merge::run_merge), which owns the (PK, payload) total
//! order; this module only drives it and materializes survivors.

use super::batch::Batch;
use super::error::StorageError;
use super::merge::prorated_blob_cap;
use super::merge::run_merge;
use super::scatter::UnifiedSet;
use super::shard_reader::MappedShard;
use crate::schema::key::{compare_pk_ordering, pk_bytes_eq, PkBuf};
use crate::schema::SchemaDescriptor;
use gnitz_expr::RowSource;

/// The PK-only projection of `schema`: the same PK columns in the same PK-list
/// order — hence the same `pk_stride` and the same OPK bytes — and no payload.
/// A skeleton shard is serialized under this, so its regions are
/// `[pk, weight, null, blob]`.
pub(super) fn skeleton_schema(schema: &SchemaDescriptor) -> SchemaDescriptor {
    // An empty projection adds no column, so only the source PK is pushed and
    // the builder's bounds cannot be reached.
    crate::schema::project_schema(schema, &[]).expect("a schema's own PK fits the PK limit")
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

/// Merge `shards` by (PK, payload) and hand `emit` each `(guard_key, skeleton)`
/// destination's non-empty slice as one batch, in guard order. A skeleton
/// destination's batch is one `(PK, Σweight)` row per key, under
/// [`skeleton_schema`].
pub(super) fn merge_and_route(
    shards: &[MappedShard],
    guards: &[(PkBuf, bool)],
    schema: &SchemaDescriptor,
    mut emit: impl FnMut(&(PkBuf, bool), Batch) -> Result<(), StorageError>,
) -> Result<(), StorageError> {
    // An empty guard list would drop every survivor on the floor while the caller
    // went on to clear the source tier — silent data loss, so reject it.
    assert!(!guards.is_empty(), "merge_and_route requires at least one guard");

    for s in shards {
        s.verify_body()?;
    }
    let total_rows: usize = shards.iter().map(|s| s.count).sum(); // survivor upper bound
    let total_blob: usize = shards.iter().map(|s| s.blob().len()).sum();

    // Phase 1 — merge into survivors, sorted (PK, payload). The merge order is
    // the guard order, so each guard's survivors are one contiguous run and the
    // split points below are `guards.len()` binary searches rather than a guard
    // lookup per row.
    let mut survivors: Vec<(u32, u32, i64)> = Vec::with_capacity(total_rows);
    run_merge(shards, schema, |src, row, w| {
        survivors.push((src as u32, row as u32, w));
    });

    // `bounds[g]..bounds[g + 1]` is guard `g`'s slice: guard `g` owns
    // `key >= guards[g]`, and guard 0 also owns everything below its own key —
    // the read router's saturating guard slot. Keys are compared off the mmap,
    // whole, so two rows differing past byte 16 route apart.
    let pk_at = |&(src, row, _): &(u32, u32, i64)| shards[src as usize].get_pk_bytes(row as usize);
    let bounds: Vec<usize> = std::iter::once(0)
        .chain(
            (1..guards.len())
                .map(|g| survivors.partition_point(|s| compare_pk_ordering(pk_at(s), guards[g].0.pk_bytes()).is_lt())),
        )
        .chain(std::iter::once(survivors.len()))
        .collect();

    // Phase 2 — one batch per guard, each scattered column-at-a-time from its
    // contiguous survivor slice.
    let set = UnifiedSet::of(shards, schema);
    let nsurv = survivors.len();

    // Only built when some destination guard is dehydrated; a hydrated store
    // never derives it.
    let skel_schema = guards.iter().any(|&(_, s)| s).then(|| skeleton_schema(schema));

    for (g, dest @ &(_, skeleton)) in guards.iter().enumerate() {
        let bucket = &survivors[bounds[g]..bounds[g + 1]];
        if bucket.is_empty() {
            continue;
        }
        // Per-PK fold first, so a guard whose keys all cancel writes nothing.
        let folded = skeleton.then(|| fold_bucket_per_pk(shards, bucket));
        if folded.as_ref().is_some_and(|f| f.is_empty()) {
            continue;
        }
        // A skeleton guard writes its folded rows under the PK-only schema; a
        // hydrated one writes the bucket at full width.
        let (wschema, rows, blob_cap) = match &folded {
            Some(rows) => (
                skel_schema
                    .as_ref()
                    .expect("a skeleton guard derived a skeleton schema"),
                rows.as_slice(),
                0,
            ),
            None => (schema, bucket, prorated_blob_cap(total_blob, nsurv, bucket.len())),
        };
        let mut batch = set.materialize(wschema, rows, blob_cap);
        if folded.is_some() {
            // The fused pass copied the *source's* payload null bits, which mean
            // nothing without a payload. Zeroing collapses the region to
            // `ENCODING_CONSTANT` — 8 bytes for the whole file.
            batch.null_bmp_data_mut().fill(0);
        }
        emit(dest, batch)?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "tests/compact.rs"]
mod tests;
