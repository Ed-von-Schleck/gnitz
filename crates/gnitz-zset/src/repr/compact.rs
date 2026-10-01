//! Shard compaction: N-way (PK, payload) merge of sorted shard files, routed to
//! per-guard output shards.
//!
//! [`merge_and_route`] orchestrates — verify → merge → route → column-first
//! scatter → one output batch per guard run, handed to the caller to write.
//! The merge kernel itself is the shared
//! [`run_merge`](crate::repr::merge::run_merge), which owns the (PK, payload) total
//! order; this module only drives it and materializes survivors.

use super::batch::Batch;
use super::error::StorageError;
use super::merge::run_merge;
use super::scatter::UnifiedSet;
use super::shard_reader::MappedShard;
use crate::schema::key::{compare_pk_ordering, pk_bytes_eq, PkBuf};
use crate::schema::SchemaDescriptor;
use gnitz_expr::RowSource;

/// Slot owning `key` in a sorted guard list: the last guard `≤ key`, saturating
/// to slot 0 for keys below the first guard.
pub fn guard_slot<T>(guards: &[T], key: &[u8], gk: impl Fn(&T) -> &[u8]) -> usize {
    guards
        .partition_point(|g| compare_pk_ordering(gk(g), key).is_le())
        .saturating_sub(1)
}

/// Fold `bucket` — one guard's (PK, payload)-sorted survivor slice — into one
/// `(PK, Σweight)` row per key, dropping net-zero keys. Every fold takes a
/// per-key time prefix of a positive integral, so no sum is negative.
fn fold_bucket_per_pk(shards: &[&MappedShard], bucket: &[(u32, u32, i64)]) -> Vec<(u32, u32, i64)> {
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

/// What [`merge_and_route`] hands each destination's batch to. `dyn` — one call
/// per destination — so the merge is compiled once, here, rather than once per
/// caller's closure.
pub type EmitGuard<'a> = dyn FnMut(&(PkBuf, bool), Batch) -> Result<(), StorageError> + 'a;

/// Merge `shards` by (PK, payload) and hand `emit` each `(guard_key, skeleton)`
/// destination's non-empty slice as one batch, in guard order. A skeleton
/// destination's batch is one PK-only `(PK, Σweight)` row per key.
pub fn merge_and_route(
    shards: &[&MappedShard],
    guards: &[(PkBuf, bool)],
    schema: &SchemaDescriptor,
    emit: &mut EmitGuard<'_>,
) -> Result<(), StorageError> {
    // An empty guard list would drop every survivor on the floor while the caller
    // went on to clear the source tier — silent data loss, so reject it.
    assert!(!guards.is_empty(), "merge_and_route requires at least one guard");

    for s in shards {
        s.verify_body()?;
    }
    let total_rows: usize = shards.iter().map(|s| s.row_count()).sum(); // survivor upper bound

    // Phase 1 — merge into survivors, sorted (PK, payload). `guard_slot` is
    // monotone in the key, so each guard's survivors are one contiguous run and
    // the split points below are binary searches rather than a lookup per row.
    let mut survivors: Vec<(u32, u32, i64)> = Vec::with_capacity(total_rows);
    run_merge(shards, schema, |src, row, w| {
        survivors.push((src as u32, row as u32, w));
    });

    // `bounds[g]..bounds[g + 1]` is guard `g`'s slice.
    let pk_at = |&(src, row, _): &(u32, u32, i64)| shards[src as usize].get_pk_bytes(row as usize);
    let bounds: Vec<usize> = (0..=guards.len())
        .map(|g| survivors.partition_point(|s| guard_slot(guards, pk_at(s), |(k, _)| k.pk_bytes()) < g))
        .collect();

    // Phase 2 — one batch per guard, each scattered column-at-a-time from its
    // contiguous survivor slice.
    let hydrated = guards
        .iter()
        .any(|&(_, s)| !s)
        .then(|| UnifiedSet::whole(shards, schema));
    let skeletal = guards
        .iter()
        .any(|&(_, s)| s)
        .then(|| UnifiedSet::whole(shards, &schema.pk_only()));
    let nsurv = survivors.len();

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
        let (set, rows) = match &folded {
            Some(rows) => (
                skeletal.as_ref().expect("a skeleton guard built the skeleton set"),
                rows.as_slice(),
            ),
            None => (
                hydrated.as_ref().expect("a hydrated guard built the hydrated set"),
                bucket,
            ),
        };
        emit(dest, set.materialize(rows, nsurv))?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "tests/compact.rs"]
mod tests;
