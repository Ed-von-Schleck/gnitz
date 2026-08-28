//! Base-table unique-PK enforcement: the DML policy that turns a pushed batch
//! into the effective batch the store and every downstream view see.
//!
//! A free function, not an inherent `Table` method: `Table` also backs view
//! output stores, operator traces, secondary indexes and system tables, none of
//! which may run base-table DML policy, and an inherent method would offer it on
//! all of them.

use rustc_hash::FxHashMap;

use super::Table;
use crate::schema::SchemaDescriptor;
use crate::storage::{Batch, BlobCacheGuard, MemBatch};

/// Retract input row `prev_pos`. The literal `-1` is exact because the ±1 clamp
/// below runs before the walk, so a live `last_insert` row weighs exactly `+1`.
fn retract(effective: &mut Batch, src: &MemBatch, prev_pos: usize, guard: &mut BlobCacheGuard) {
    effective.append_row_from_source_bytes(src.get_pk_bytes(prev_pos), -1, src, prev_pos, guard.get_mut());
}

/// The per-PK batch state of the enforcement walk. `store_probed` must stay
/// sticky across a delete of the same PK (only the insert fact is cleared):
/// losing it would re-emit the stored-row retraction on a later re-insert of the
/// PK and drive base-table weights negative.
#[derive(Default, Clone, Copy)]
struct UniquePkRowState {
    /// Batch row index of the last `+1` insertion of this PK, if still live.
    /// A batch index (not an effective index): the retraction re-reads the row
    /// from `batch`, and the effective batch carries extra store-retraction
    /// rows that break any 1:1 correspondence.
    last_insert: Option<usize>,
    /// The store was already probed (and any stored row retracted) for this PK.
    /// `retract_pk_bytes` is a pure lookup (it only arms the `found_*`
    /// accessors) and the store cannot change mid-batch, so one probe per PK is
    /// exact — and the stored-row retraction must be emitted at most once, or
    /// downstream weights go negative.
    store_probed: bool,
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
/// `schema` is a parameter rather than `store`'s own copy: a column ALTER
/// publishes the new descriptor through the registry, which reaches a
/// non-owned store's `Table::schema` not at all, so the field can lag the
/// caller's.
pub(crate) fn enforce_unique_pk(store: &mut Table, schema: &SchemaDescriptor, mut batch: Batch) -> Batch {
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

    // `batch.count` rows, not 2×: the stored-row retraction that could double a
    // row is one per *distinct* PK already present, and the arena grows on
    // demand. Provisioning 2× puts a bulk push over `POOL_BYPASS_BYTES`, so it
    // mmaps and munmaps its arena per push instead of taking a pooled one.
    let mut effective = Batch::with_capacity(*schema, batch.count);
    // One relocation cache for the whole walk, as every other multi-row append
    // holds; `acquire` clamps the sizing hint and no-ops on a string-free schema.
    let mut guard = BlobCacheGuard::acquire(schema, batch.count);
    // Grown on demand rather than reserved for `batch.count`: the map holds one
    // entry per *distinct* PK, and a bulk push reserving per row asks for
    // megabytes it never fills.
    let mut state: FxHashMap<&[u8], UniquePkRowState> = FxHashMap::default();

    let mb = batch.as_mem_batch();
    for row in 0..batch.count {
        let w = batch.get_weight(row);
        if w == 0 {
            continue;
        }
        let pkb = batch.get_pk_bytes(row);
        let st = state.entry(pkb).or_default();

        // Stored-row retraction — shared by insert and delete. Probe the
        // store the first time this PK is seen and, if found, emit a
        // retraction of the stored (PK, payload) so downstream views drop
        // the old payload. Gating on `store_probed` skips the repeated LSM
        // point lookup for a PK the batch touches again.
        if !st.store_probed {
            st.store_probed = true;
            let (_existing_w, stored_row) = store.retract_pk_bytes(pkb);
            if let Some(stored_row) = stored_row {
                // The located stored row is an owned `ColumnarSource` view;
                // copy it in at weight -1 via the canonical source-append.
                effective.append_row_from_source_bytes(pkb, -1, &stored_row.run, stored_row.row, guard.get_mut());
            }
        }

        if w > 0 {
            // Insert. If this PK was already inserted in this batch, retract
            // that earlier insertion (intra-batch upsert: last value wins).
            if let Some(prev_pos) = st.last_insert {
                retract(&mut effective, &mb, prev_pos, &mut guard);
            }
            st.last_insert = Some(row);
            effective.append_row_from_source_bytes(pkb, w, &mb, row, guard.get_mut());
        } else {
            // Delete (w < 0). The stored-row retraction above already emitted
            // the removal; here only cancel a prior intra-batch insertion and
            // clear the insert fact so a later re-insert of this PK is not
            // re-negated (`store_probed` stays sticky — see its doc).
            // A retraction of a key that is neither stored nor seen has
            // nothing to cancel — passing it through would store a
            // negative-weight phantom row (violating base-table positivity),
            // and dropping it is idempotent under delete replay.
            if let Some(prev_pos) = st.last_insert.take() {
                retract(&mut effective, &mb, prev_pos, &mut guard);
            }
        }
    }

    effective
}

#[cfg(test)]
#[path = "../tests/unique_pk.rs"]
mod tests;
