//! Base-table unique-PK enforcement: the DML policy that turns a pushed batch
//! into the effective batch the store and every downstream view see.

use std::collections::hash_map::Entry;

use rustc_hash::FxHashMap;

use crate::storage::Table;

use gnitz_zset::repr::{merge_consolidated, Batch, StoredRow};
use gnitz_zset::schema::SchemaDescriptor;

/// How a base table's write meets its unique PK.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PkRule {
    /// Per PK, the batch's last nonzero row decides over the store's row: see
    /// [`enforce_unique_pk`].
    Enforce,
    /// The writer proved, against the store this batch lands on, that every
    /// key is distinct, weighted 1 and not held, so the rule's answer is the
    /// batch itself and is not computed. A debug build checks the proof.
    Fresh,
}

/// The effective batch of `batch` under `rule`, against `store`.
#[inline]
pub(super) fn under_pk_rule(store: &Table, batch: Batch, rule: PkRule) -> Batch {
    match rule {
        PkRule::Enforce => enforce_unique_pk(store, batch),
        PkRule::Fresh => admit_fresh(store, batch),
    }
}

/// [`PkRule::Fresh`]'s effective batch: `batch` itself, under the store's schema.
fn admit_fresh(store: &Table, mut batch: Batch) -> Batch {
    batch.set_schema(store.schema());
    debug_assert!(
        fresh_against(store, &batch),
        "a batch admitted as fresh repeats a key, weighs a row other than 1, or meets a held key"
    );
    batch
}

/// Whether every row of `batch` weighs 1 under a key no other row of it and no
/// live row of `store` holds.
fn fresh_against(store: &Table, batch: &Batch) -> bool {
    let mut seen = rustc_hash::FxHashSet::default();
    (0..batch.len()).all(|row| {
        let pk = batch.get_pk_bytes(row);
        batch.get_weight(row) == 1 && seen.insert(pk) && store.unique_live_row_at(pk).is_none()
    })
}

/// Enforce unique-PK semantics on an ingest batch: per PK, the batch's last
/// non-zero row decides over whatever the store holds. A held key's stored row
/// is retracted; the last row survives, at `+1`, iff it is an insert. A batch
/// pushed in PK order comes back consolidated.
pub(super) fn enforce_unique_pk(store: &Table, mut batch: Batch) -> Batch {
    let schema = store.schema();
    batch.set_schema(schema);
    // A push at `w > 1` is the row repeated; a live base-table row weighs 1.
    batch.map_weights(|w| w.min(1));

    let n = batch.len();
    // `dropped` and `stored` stay unallocated until their first push, which
    // reserves for the rest of the batch.
    let mut dropped: Vec<usize> = Vec::new();
    let mut stored: Vec<StoredRow> = Vec::new();
    // A batch already in PK order holds each key's rows adjacent, so a run of
    // equal PKs shares one slot and no map is built.
    let ordered = batch.is_consolidated() || batch.pk().chunks_exact(batch.schema().pk_stride()).is_sorted();
    {
        let mut fetch = |pk: &[u8], row: usize| {
            if let Some(s) = store.unique_live_row_at(pk) {
                push_reserving(&mut stored, s, n - row);
            }
        };
        let mut live_insert: FxHashMap<&[u8], Option<usize>> = if ordered {
            FxHashMap::default()
        } else {
            FxHashMap::with_capacity_and_hasher(n, Default::default())
        };
        let mut last_pk: Option<&[u8]> = None;
        let mut run_slot: Option<usize> = None;
        for row in 0..n {
            let w = batch.get_weight(row);
            if w <= 0 {
                push_reserving(&mut dropped, row, n - row);
            }
            if w == 0 {
                continue;
            }
            let pk = batch.get_pk_bytes(row);
            let slot = if ordered {
                if last_pk != Some(pk) {
                    fetch(pk, row);
                    last_pk = Some(pk);
                    run_slot = None;
                }
                &mut run_slot
            } else {
                match live_insert.entry(pk) {
                    Entry::Occupied(e) => e.into_mut(),
                    Entry::Vacant(e) => {
                        fetch(pk, row);
                        e.insert(None)
                    }
                }
            };
            if let Some(displaced) = std::mem::replace(slot, (w > 0).then_some(row)) {
                push_reserving(&mut dropped, displaced, n - row);
            }
        }
    }

    if ordered {
        return sum_in_order(batch, dropped, &stored);
    }
    if dropped.is_empty() {
        batch.append_stored(&stored, -1);
        return batch;
    }
    let kept = complement(dropped, n);
    let mut effective = Batch::from_ranges(&batch, &kept, stored.len());
    effective.append_stored(&stored, -1);
    effective
}

/// The effective batch of `batch`, walked in PK order: its rows less
/// `dropped`, one per key at `+1`, and the stored rows they retract, one per
/// key at `-1`. Each is consolidated, so their sum is a merge, which cancels an
/// update that rewrites a row unchanged.
fn sum_in_order(mut batch: Batch, dropped: Vec<usize>, stored: &[StoredRow]) -> Batch {
    let n = batch.len();
    if dropped.len() == n && !stored.is_empty() {
        return retraction(batch.schema(), stored);
    }
    if !dropped.is_empty() {
        batch = Batch::from_ranges(&batch, &complement(dropped, n), 0);
    }
    // The rows kept are one per key.
    batch.certify_consolidated();
    if stored.is_empty() {
        return batch;
    }
    let retracted = retraction(batch.schema(), stored);
    merge_consolidated(&[batch.as_mem_batch(), retracted.as_mem_batch()], batch.schema())
}

/// `stored`, rows of distinct keys in PK order, each at weight `-1`.
fn retraction(schema: &SchemaDescriptor, stored: &[StoredRow]) -> Batch {
    let mut retracted = Batch::with_capacity(schema, stored.len());
    retracted.append_stored(stored, -1);
    retracted.certify_consolidated();
    retracted
}

/// Push `x`, first reserving `remaining` slots if `v` has never allocated. The
/// callers pass the rows still to come, which always covers `stored`, and
/// covers `dropped` unless a retraction also displaces a held insert.
#[inline(always)]
fn push_reserving<T>(v: &mut Vec<T>, x: T, remaining: usize) {
    if v.capacity() == 0 {
        first_reserve(v, remaining);
    }
    v.push(x);
}

#[cold]
#[inline(never)]
fn first_reserve<T>(v: &mut Vec<T>, remaining: usize) {
    v.reserve(remaining);
}

/// The `[start, end)` runs of `0..count` that miss every one of `rows`.
fn complement(mut rows: Vec<usize>, count: usize) -> Vec<(usize, usize)> {
    rows.sort_unstable();
    let mut runs = Vec::with_capacity(rows.len() + 1);
    let mut from = 0;
    for &r in rows.iter().chain(std::iter::once(&count)) {
        if from < r {
            runs.push((from, r));
        }
        from = r + 1;
    }
    runs
}

#[cfg(test)]
#[path = "tests/unique_pk.rs"]
mod tests;

#[cfg(test)]
#[path = "benches/unique_pk.rs"]
mod bench;
