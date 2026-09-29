//! Base-table unique-PK enforcement: the DML policy that turns a pushed batch
//! into the effective batch the store and every downstream view see.

use std::collections::hash_map::Entry;

use rustc_hash::FxHashMap;

use crate::storage::{Batch, StoredRow, Table};

/// Enforce unique-PK semantics on an ingest batch: per PK, the batch's last
/// non-zero row decides over whatever the store holds. A held key's stored row
/// is retracted; the last row survives, at `+1`, iff it is an insert.
pub(crate) fn enforce_unique_pk(store: &Table, mut batch: Batch) -> Batch {
    let schema = store.schema();
    batch.set_schema(schema);
    // A push at `w > 1` is the row repeated; a live base-table row weighs 1.
    batch.map_weights(|w| w.min(1));

    let mut dropped: Vec<usize> = Vec::new();
    let mut stored: Vec<StoredRow> = Vec::new();
    let mut live_insert: FxHashMap<&[u8], Option<usize>> =
        FxHashMap::with_capacity_and_hasher(batch.count, Default::default());
    for row in 0..batch.count {
        let w = batch.get_weight(row);
        if w <= 0 {
            dropped.push(row);
        }
        if w == 0 {
            continue;
        }
        let pk = batch.get_pk_bytes(row);
        let slot = match live_insert.entry(pk) {
            Entry::Occupied(e) => e.into_mut(),
            Entry::Vacant(e) => {
                stored.extend(store.live_row_at(pk).1);
                e.insert(None)
            }
        };
        if let Some(displaced) = std::mem::replace(slot, (w > 0).then_some(row)) {
            dropped.push(displaced);
        }
    }

    if dropped.is_empty() && stored.is_empty() {
        return batch;
    }
    let kept = complement(dropped, batch.count);
    let mut effective = Batch::from_ranges(&batch, &kept, stored.len());
    if !stored.is_empty() {
        let mut sink = effective.append_session(stored.len());
        for s in &stored {
            let (src, row) = s.source();
            sink.push_row(src, row, -1);
        }
    }
    effective
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
#[path = "bench_unique_pk.rs"]
mod bench;
