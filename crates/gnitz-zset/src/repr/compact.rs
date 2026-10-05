//! Shard compaction: N-way (PK, payload) merge of sorted shard files, routed to
//! per-guard output shards.
//!
//! [`merge_guard`] merges the rows one guard owns → column-first scatter → one
//! output batch, handed to the caller to write; [`merge_and_route`] is every
//! guard of a fold in one call. The merge kernel itself is the shared
//! [`run_merge_in`](crate::repr::merge::run_merge_in), which owns the
//! (PK, payload) total order; this module only drives it and materializes
//! survivors.

use super::batch::Batch;
use super::error::StorageError;
use super::merge::run_merge_in;
use super::scatter::UnifiedSet;
use super::shard_reader::MappedShard;
use crate::schema::key::compare_pk_ordering;
use crate::schema::SchemaDescriptor;
use gnitz_wire::PkBuf;
use gnitz_wire::RowSource;
use std::ops::Range;

/// Slot owning `key` in a sorted guard list: the last guard `≤ key`, saturating
/// to slot 0 for keys below the first guard.
pub fn guard_slot<T>(guards: &[T], key: &[u8], gk: impl Fn(&T) -> &[u8]) -> usize {
    guards
        .partition_point(|g| compare_pk_ordering(gk(g), key).is_le())
        .saturating_sub(1)
}

/// Merge the rows of `shards` that guard `g` of `guards` owns into one batch,
/// with whether it is a skeleton batch — one PK-only `(PK, Σweight)` row per
/// key. `None` when every one of them cancelled. Every batch is a skeleton one
/// when `dehydrate` is set or any input is a skeleton shard: a skeleton row
/// carries no payload to write back full width. A skeleton merge runs under the
/// PK-only schema, where a key's rows are one element; every other merge is by
/// (PK, payload).
///
/// Each shard is sorted, so the rows a guard owns are one window of it: from
/// `starts`, up to where the next guard's key starts. A window cuts at a key,
/// never inside one, and guard 0 owns every key below its own. `starts` is
/// zeroed for guard 0 and left at guard `g + 1`'s windows, so the guards are
/// merged in order, each as a step of its own.
///
/// `guards` is sorted and distinct, and the shards' bodies are verified by the
/// caller. Out of line, so the merge is compiled once, here: inlined into a
/// caller, the payload comparator drops out of the merge loop.
#[inline(never)]
pub fn merge_guard(
    shards: &[&MappedShard],
    guards: &[PkBuf],
    g: usize,
    starts: &mut [usize],
    dehydrate: bool,
    schema: &SchemaDescriptor,
) -> Option<(bool, Batch)> {
    debug_assert!(guards.is_sorted_by(|a, b| a < b), "guards must be sorted and distinct");
    let skeleton = dehydrate || shards.iter().any(|s| s.is_skeleton());
    let out_schema = if skeleton { schema.pk_only() } else { *schema };
    let windows: Vec<Range<usize>> = shards
        .iter()
        .zip(starts)
        .map(|(shard, start)| {
            let end = match guards.get(g + 1) {
                Some(next) => shard.advance_to(next.pk_bytes(), *start),
                None => shard.row_count(),
            };
            std::mem::replace(start, end)..end
        })
        .collect();
    let mut survivors: Vec<(u32, u32, i64)> = Vec::with_capacity(windows.iter().map(Range::len).sum());
    run_merge_in(shards, &out_schema, windows.iter().cloned(), |src, row, w| {
        survivors.push((src as u32, row as u32, w));
    });
    if survivors.is_empty() {
        return None;
    }
    let set = UnifiedSet::of(shards, &out_schema, windows);
    Some((skeleton, set.materialize(&survivors, set.src_rows())))
}

/// [`merge_guard`] over every guard in order, handing `emit` each non-empty
/// batch under its guard key.
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
    for s in shards {
        s.verify_body()?;
    }
    let mut starts = vec![0usize; shards.len()];
    for (g, &key) in guards.iter().enumerate() {
        if let Some((skeleton, batch)) = merge_guard(shards, guards, g, &mut starts, dehydrate, schema) {
            emit(key, skeleton, batch)?;
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
