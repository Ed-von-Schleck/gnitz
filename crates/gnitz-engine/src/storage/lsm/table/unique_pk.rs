//! Base-table unique-PK enforcement: the DML policy that turns a pushed batch
//! into the effective batch the store and every downstream view see.
//!
//! A free function, not an inherent `Table` method, because it does not answer a
//! question about a `Table`: it returns the *effective batch* the caller feeds
//! downstream, and the store is only one of its two inputs. Which relations may
//! run it is settled a layer up, by `RelationKind::is_base_table`.

use rustc_hash::FxHashMap;

use super::Table;
use crate::schema::SchemaDescriptor;
use crate::storage::{Batch, BlobCacheGuard, MemBatch};

/// Retract input row `prev_pos`. The literal `-1` is exact because the ±1 clamp
/// below runs before the walk, so a live `last_insert` row weighs exactly `+1`.
fn retract(effective: &mut Batch, src: &MemBatch, prev_pos: usize, guard: &mut BlobCacheGuard) {
    effective.append_row_from_source_bytes(src.get_pk_bytes(prev_pos), -1, src, prev_pos, guard.get_mut());
}

/// Bulk-copy the verbatim run `[kept_from, end)` into the effective batch,
/// creating it on the first cut, and return it for the divergent rows the caller
/// is about to append. A German string spanning two cuts is relocated by each
/// (one `AppendSession` per call): wasted bytes, never a wrong row.
fn cut<'a>(
    effective: &'a mut Option<Batch>,
    schema: &SchemaDescriptor,
    batch: &Batch,
    kept_from: usize,
    end: usize,
) -> &'a mut Batch {
    // `batch.count` rows, not 2×: a 2× provision puts a bulk push over
    // `POOL_BYPASS_BYTES`, so it mmaps its own arena instead of taking a pooled
    // one, and the arena grows on demand anyway.
    let eff = effective.get_or_insert_with(|| Batch::with_capacity(*schema, batch.count));
    eff.append_batch(batch, kept_from, end);
    eff
}

/// Enforce unique-PK semantics on an ingest batch: retract any stored row with
/// the same PK before inserting the new one, and resolve duplicate PKs within
/// the batch so each surviving PK nets to a single live row.
///
/// Emits the stored-row retraction (`-1`, old payload) into the effective batch
/// so downstream views see the old payload removed before the new one lands.
/// Keys on `get_pk_bytes` (verbatim OPK) and dedups on `&[u8]` slices borrowed
/// from the batch's PK region — correct for every PK width. Never round-trips
/// through a native `u128` (which `opk_key` would re-encode, double-flipping a
/// signed PK's sign bit, so the probe would match no stored row and the
/// retraction would be silently dropped).
///
/// A `w <= 0` row is never carried through: dropping a retraction of a key that
/// is neither stored nor inserted here is what keeps base-table weights positive,
/// and is idempotent under delete replay. Those rows and the ones that emit a
/// retraction ahead of themselves are the only ones that cut the verbatim run,
/// which is bulk-copied; a push of fresh keys cuts nothing and comes back as it
/// arrived. Arrival order survives either way — sorting first would turn
/// intra-batch last-insert-wins into sorted-last-wins.
///
/// `schema` is a parameter rather than `store`'s own copy: a column ALTER
/// publishes the new descriptor through the registry, which reaches a
/// non-owned store's `Table::schema` not at all, so the field can lag the
/// caller's.
pub(crate) fn enforce_unique_pk(store: &Table, schema: &SchemaDescriptor, mut batch: Batch) -> Batch {
    // Empty-batch guard: empty batches reach the engine via the
    // `CatalogStore` ingest wrappers, which — unlike the worker loop — do
    // not pre-filter `count == 0`.
    if batch.count == 0 {
        return batch;
    }
    // Base-table contract: per-PK accumulated weight ∈ {0, 1}. A pushed row
    // at |w| > 1 is the row repeated; retract-before-insert collapses
    // repeats to one live instance (and a delete removes at most one), so
    // normalize weights to ±1 before the enforcement walk. Must run on the
    // input batch, not at append time: the intra-batch retraction re-reads the
    // original row, so clamping only the appended copy would emit `+1` then
    // `-w` for the same element and drive intra-batch dedup net-negative — and
    // it is what makes `retract`'s literal `-1` exact.
    batch.map_weights(|w| w.clamp(-1, 1));

    let mut effective: Option<Batch> = None;
    // First input row of the verbatim run not yet copied into `effective`.
    let mut kept_from = 0usize;
    // One relocation cache for the whole walk, as every other multi-row append
    // holds; `acquire` clamps the sizing hint and no-ops on a string-free schema.
    let mut guard = BlobCacheGuard::acquire(schema, batch.count);
    // Present ⇔ this PK's store probe has run; the value is its last still-live
    // `+1`, as a *batch* row index (the retraction re-reads the input row).
    // Nothing removes an entry, so re-probing on a re-insert after a delete —
    // which would emit the stored-row retraction twice — is unrepresentable.
    // Grown on demand: one entry per *distinct* PK, so a bulk push that reserved
    // per row would ask for megabytes it never fills.
    let mut probed: FxHashMap<&[u8], Option<usize>> = FxHashMap::default();

    let mb = batch.as_mem_batch();
    for row in 0..batch.count {
        let w = batch.get_weight(row);
        if w == 0 {
            cut(&mut effective, schema, &batch, kept_from, row);
            kept_from = row + 1;
            continue;
        }
        let pkb = batch.get_pk_bytes(row);

        // Creating the entry *is* the probe. `retract_pk_bytes` mutates nothing
        // and the store cannot change mid-batch, so one probe per PK is exact.
        let mut stored = None;
        let entry = probed.entry(pkb).or_insert_with(|| {
            stored = store.retract_pk_bytes(pkb).1;
            None
        });
        // Superseded by this row, whichever sign it carries.
        let last_insert = entry.take();

        if w > 0 {
            *entry = Some(row);
            if stored.is_none() && last_insert.is_none() {
                continue; // nothing to emit ahead of it — the run stays open
            }
        }

        let eff = cut(&mut effective, schema, &batch, kept_from, row);
        if let Some(stored_row) = stored {
            eff.append_row_from_source_bytes(pkb, -1, &stored_row.run, stored_row.row, guard.get_mut());
        }
        if let Some(prev_pos) = last_insert {
            retract(eff, &mb, prev_pos, &mut guard);
        }
        // An insert heads the next run — its own copy still has to land. A
        // delete's row never does: the removal is the retractions just emitted.
        kept_from = if w > 0 { row } else { row + 1 };
    }

    match effective {
        None => {
            // Nothing diverged, so the input *is* the effective batch. It still
            // needs the caller's descriptor: `effective` would have been built
            // with it, and it may be newer than the store's.
            batch.set_schema(*schema);
            batch
        }
        Some(mut eff) => {
            eff.append_batch(&batch, kept_from, batch.count);
            eff
        }
    }
}

#[cfg(test)]
#[path = "../tests/unique_pk.rs"]
mod tests;
