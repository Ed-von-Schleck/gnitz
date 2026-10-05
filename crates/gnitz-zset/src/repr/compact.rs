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
use crate::schema::key::compare_pk_ordering;
use crate::schema::SchemaDescriptor;
use gnitz_wire::PkBuf;
use gnitz_wire::RowSource;

/// Slot owning `key` in a sorted guard list: the last guard `≤ key`, saturating
/// to slot 0 for keys below the first guard.
pub fn guard_slot<T>(guards: &[T], key: &[u8], gk: impl Fn(&T) -> &[u8]) -> usize {
    guards
        .partition_point(|g| compare_pk_ordering(gk(g), key).is_le())
        .saturating_sub(1)
}

/// Merge `shards` and hand `emit` each guard's non-empty slice as one batch, in
/// guard order, with whether it is a skeleton batch — one PK-only
/// `(PK, Σweight)` row per key. Every batch is one when `dehydrate` is set or
/// any input is a skeleton shard: a skeleton row carries no payload to write
/// back full width. A skeleton merge runs under the PK-only schema, where a
/// key's rows are one element; every other merge is by (PK, payload).
///
/// `guards` is sorted and distinct. `emit` is `dyn` — one call per guard — and
/// the function stays out of line, so the merge is compiled once, here: inlined
/// into a caller, the payload comparator drops out of the merge loop.
#[inline(never)]
pub fn merge_and_route(
    shards: &[&MappedShard],
    guards: &[PkBuf],
    dehydrate: bool,
    schema: &SchemaDescriptor,
    emit: &mut dyn FnMut(PkBuf, bool, Batch) -> Result<(), StorageError>,
) -> Result<(), StorageError> {
    // An empty guard list would drop every survivor on the floor while the caller
    // went on to clear the source tier — silent data loss, so reject it.
    assert!(!guards.is_empty(), "merge_and_route requires at least one guard");
    debug_assert!(guards.is_sorted_by(|a, b| a < b), "guards must be sorted and distinct");

    for s in shards {
        s.verify_body()?;
    }
    let skeleton = dehydrate || shards.iter().any(|s| s.is_skeleton());
    let total_rows: usize = shards.iter().map(|s| s.row_count()).sum(); // survivor upper bound

    // Phase 1 — merge into survivors, in `out_schema`'s order. `guard_slot` is
    // monotone in the key, so each guard's survivors are one contiguous run and
    // the split points below are binary searches rather than a lookup per row.
    let out_schema = if skeleton { schema.pk_only() } else { *schema };
    let mut survivors: Vec<(u32, u32, i64)> = Vec::with_capacity(total_rows);
    run_merge(shards, &out_schema, |src, row, w| {
        survivors.push((src as u32, row as u32, w));
    });

    // `bounds[g]..bounds[g + 1]` is guard `g`'s slice.
    let pk_at = |&(src, row, _): &(u32, u32, i64)| shards[src as usize].get_pk_bytes(row as usize);
    let bounds: Vec<usize> = (0..=guards.len())
        .map(|g| survivors.partition_point(|s| guard_slot(guards, pk_at(s), PkBuf::pk_bytes) < g))
        .collect();

    // Phase 2 — one batch per guard, each scattered column-at-a-time from its
    // contiguous survivor slice.
    let set = UnifiedSet::whole(shards, &out_schema);
    let nsurv = survivors.len();
    for (g, &key) in guards.iter().enumerate() {
        let bucket = &survivors[bounds[g]..bounds[g + 1]];
        if !bucket.is_empty() {
            emit(key, skeleton, set.materialize(bucket, nsurv))?;
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "tests/compact.rs"]
mod tests;

#[cfg(test)]
#[path = "benches/compact.rs"]
mod bench;
