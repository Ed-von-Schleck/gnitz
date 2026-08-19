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
mod tests {
    use super::enforce_unique_pk;
    use crate::schema::type_code;
    use crate::storage::{Batch, RecoverySource, Table};
    use crate::test_support::{opk_pk, pk_i64_schema, wide_pk_3xu64_schema, wide_row};

    // R2 regression: a DELETE/UPDATE retraction on a *signed* (I64) PK must
    // actually retract the stored row. The pre-fix narrow arm fed `get_pk`
    // (OPK-widened, sign-flipped) to `retract_pk(u128)`, which re-OPK-encoded it
    // (a second sign flip) so the probe matched no stored row and the retraction
    // was silently dropped. The byte path keys on verbatim OPK and is correct.
    #[test]
    fn test_enforce_unique_pk_signed_negative_retraction() {
        let schema = pk_i64_schema(type_code::I64);
        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("enforce_signed");
        let mut pt = Table::new(
            tdir.to_str().unwrap(),
            schema,
            1234,
            RecoverySource::Rederive { resume_at: None },
        )
        .unwrap();

        // Seed the store with a negative-PK row (PK=-5, payload=100).
        let mut seed = Batch::with_capacity(schema, 1);
        seed.extend_pk_opk(&schema, &[(-5i64 as u64) as u128]);
        seed.extend_weight(&1i64.to_le_bytes());
        seed.extend_null_bmp(&0u64.to_le_bytes());
        seed.extend_col(0, &100i64.to_le_bytes());
        seed.count += 1;
        pt.ingest_owned_batch(seed).unwrap();
        let mut opk = [0u8; 8];
        gnitz_wire::encode_pk_column(&(-5i64).to_le_bytes(), type_code::I64, &mut opk);
        assert!(pt.has_pk_bytes(&opk), "seed row must be present");

        // DELETE PK=-5: a -1 retraction batch.
        let mut del = Batch::with_capacity(schema, 1);
        del.extend_pk_opk(&schema, &[(-5i64 as u64) as u128]);
        del.extend_weight(&(-1i64).to_le_bytes());
        del.extend_null_bmp(&0u64.to_le_bytes());
        del.extend_col(0, &100i64.to_le_bytes());
        del.count += 1;

        let effective = enforce_unique_pk(&mut pt, &schema, del);

        // The store row must have been *found*: the effective batch carries a
        // single net -1 for PK=-5 with the stored payload. (`retract_pk_bytes` is
        // read-only — the store row is removed when the effective batch is
        // re-ingested, mirroring the real DML pipeline below.)
        assert_eq!(effective.count, 1, "one net retraction row expected");
        assert_eq!(effective.get_weight(0), -1, "effective weight must be -1");
        assert_eq!(effective.get_pk_bytes(0), &opk[..], "retraction PK must be OPK(-5)");

        // Re-ingest the effective batch as the DML pipeline does; the stored
        // negative-PK row then nets to zero and is gone.
        pt.ingest_owned_batch(effective).unwrap();
        assert!(
            !pt.has_pk_bytes(&opk),
            "stored negative-PK row must be gone after retraction"
        );
    }

    // Base-table contract: per-PK accumulated weight ∈ {0, 1}. A pushed row at
    // |w| > 1 is the row repeated; retract-before-insert collapses repeats to
    // one live instance, so the effective batch must carry unit weights —
    // otherwise a weight-2 PK row lands (two live instances the -1-normalized
    // retraction arm can never fully delete) and the CREATE UNIQUE INDEX PK
    // short-circuit's premise breaks.
    #[test]
    fn test_enforce_unique_pk_weight_normalized() {
        let schema = pk_i64_schema(type_code::U64);
        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("enforce_weight_norm");
        let mut pt = Table::new(
            tdir.to_str().unwrap(),
            schema,
            1234,
            RecoverySource::Rederive { resume_at: None },
        )
        .unwrap();

        let row_pk1 = |payload: i64, weight: i64| {
            let mut b = Batch::with_capacity(schema, 1);
            b.extend_pk_opk(&schema, &[1u128]);
            b.extend_weight(&weight.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &payload.to_le_bytes());
            b.count += 1;
            b
        };
        let mut opk = [0u8; 8];
        gnitz_wire::encode_pk_column(&1u64.to_le_bytes(), type_code::U64, &mut opk);

        // Case 1 — fresh insert at weight 2: one effective row at weight 1.
        let effective = enforce_unique_pk(&mut pt, &schema, row_pk1(100, 2));
        assert_eq!(effective.count, 1, "one effective insert row expected");
        assert_eq!(effective.get_weight(0), 1, "insert weight must be normalized to 1");
        pt.ingest_owned_batch(effective).unwrap();
        assert!(pt.has_pk_bytes(&opk), "row must be live after the clamped insert");

        // Case 2 — upsert at weight 2 over the committed row: retraction of the
        // stored payload at -1, then the new payload at 1.
        let effective = enforce_unique_pk(&mut pt, &schema, row_pk1(200, 2));
        assert_eq!(effective.count, 2, "stored retraction + new insert expected");
        assert_eq!(effective.get_weight(0), -1, "stored-row retraction must be -1");
        assert_eq!(effective.get_weight(1), 1, "upsert weight must be normalized to 1");
        pt.ingest_owned_batch(effective).unwrap();
        assert!(pt.has_pk_bytes(&opk), "row must be live after the upsert");

        // Case 3 — DELETE at weight -3: one -1 retraction; re-ingest nets the
        // store to exactly zero (no ghost weight survives, no negative net).
        let effective = enforce_unique_pk(&mut pt, &schema, row_pk1(200, -3));
        assert_eq!(effective.count, 1, "one net retraction row expected");
        assert_eq!(
            effective.get_weight(0),
            -1,
            "retraction weight must be normalized to -1"
        );
        pt.ingest_owned_batch(effective).unwrap();
        assert!(!pt.has_pk_bytes(&opk), "row must be fully gone after the delete");
    }

    // Positivity regression: a retraction of a key that is absent (never inserted)
    // or tombstoned (inserted then removed) must NOT pass a negative-weight phantom
    // row through to the store. Pre-fix the `else if !found` arm appended the raw
    // `(-1, filler)` row, leaving a base table at net weight -1.
    #[test]
    fn test_enforce_unique_pk_absent_key_drops_phantom() {
        let schema = pk_i64_schema(type_code::I64);
        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("enforce_absent");
        let mut pt = Table::new(
            tdir.to_str().unwrap(),
            schema,
            1234,
            RecoverySource::Rederive { resume_at: None },
        )
        .unwrap();

        // Seed an unrelated row (PK=-5) so the store is non-empty.
        let mut seed = Batch::with_capacity(schema, 1);
        seed.extend_pk_opk(&schema, &[(-5i64 as u64) as u128]);
        seed.extend_weight(&1i64.to_le_bytes());
        seed.extend_null_bmp(&0u64.to_le_bytes());
        seed.extend_col(0, &100i64.to_le_bytes());
        seed.count += 1;
        pt.ingest_owned_batch(seed).unwrap();

        let retract_pk7 = || {
            let mut del = Batch::with_capacity(schema, 1);
            del.extend_pk_opk(&schema, &[7u128]);
            del.extend_weight(&(-1i64).to_le_bytes());
            del.extend_null_bmp(&0u64.to_le_bytes());
            del.extend_col(0, &0i64.to_le_bytes());
            del.count += 1;
            del
        };

        // Case 1 — absent key: PK=7 was never inserted. The phantom pass-through
        // is dropped, so the effective batch is empty.
        let effective = enforce_unique_pk(&mut pt, &schema, retract_pk7());
        assert_eq!(effective.count, 0, "absent-key retraction must emit no phantom row");

        // Case 2 — tombstoned key: insert PK=7, retract to net zero, then retract
        // a second time. The first retraction finds the stored row (count 1); the
        // second finds nothing and emits no phantom.
        let mut ins = Batch::with_capacity(schema, 1);
        ins.extend_pk_opk(&schema, &[7u128]);
        ins.extend_weight(&1i64.to_le_bytes());
        ins.extend_null_bmp(&0u64.to_le_bytes());
        ins.extend_col(0, &42i64.to_le_bytes());
        ins.count += 1;
        pt.ingest_owned_batch(ins).unwrap();

        let eff1 = enforce_unique_pk(&mut pt, &schema, retract_pk7());
        assert_eq!(
            eff1.count, 1,
            "retracting a present key emits the stored-row retraction"
        );
        pt.ingest_owned_batch(eff1).unwrap(); // PK=7 now nets to zero (tombstoned)

        let eff2 = enforce_unique_pk(&mut pt, &schema, retract_pk7());
        assert_eq!(eff2.count, 0, "tombstoned-key retraction must emit no phantom row");
    }

    // Wide-PK enforcement: a >16-byte compound PK must key the intra-batch
    // `seen` map, the store probe, and the emitted retraction on its full OPK
    // bytes. The other enforce_unique_pk unit tests use narrow single-column
    // PKs, so this is the only coverage of `&[u8]` slice keying at pk_stride > 16.
    #[test]
    fn test_enforce_unique_pk_wide_pk() {
        let schema = wide_pk_3xu64_schema();
        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("enforce_wide");
        let mut pt = Table::new(
            tdir.to_str().unwrap(),
            schema,
            555,
            RecoverySource::Rederive { resume_at: None },
        )
        .unwrap();

        let pk24 = |a: u64, b: u64, c: u64| opk_pk(&schema, &[a as u128, b as u128, c as u128]);

        // Seed the store with K=(1,2,3) → 100.
        let eff = enforce_unique_pk(&mut pt, &schema, wide_row(&schema, &pk24(1, 2, 3), 1, 100));
        assert_eq!(eff.count, 1, "fresh wide-PK insert is a single +1 row");
        pt.ingest_owned_batch(eff).unwrap();
        assert!(pt.has_pk_bytes(&pk24(1, 2, 3)), "seed row must be live");

        // Cross-batch upsert: a +1 on the same wide PK retracts the stored
        // payload (keyed and emitted on the full 24 bytes) and inserts the new.
        let eff = enforce_unique_pk(&mut pt, &schema, wide_row(&schema, &pk24(1, 2, 3), 1, 200));
        assert_eq!(eff.count, 2, "stored-row retraction + new insert");
        assert_eq!(eff.get_weight(0), -1, "stored retraction at -1");
        assert_eq!(
            eff.get_pk_bytes(0),
            &pk24(1, 2, 3)[..],
            "retraction keys on the full 24-byte PK"
        );
        assert_eq!(eff.get_weight(1), 1, "new insert at +1");
        pt.ingest_owned_batch(eff).unwrap();
        assert!(pt.has_pk_bytes(&pk24(1, 2, 3)), "row stays live after the upsert");

        // Intra-batch +1, -1, +1 on a fresh wide PK K2=(7,8,9): the delete must
        // clear the `seen` entry so the re-insert is not re-negated. Net +1.
        let mut b = Batch::with_capacity(schema, 3);
        for (payload, w) in [(10i64, 1i64), (10, -1), (20, 1)] {
            b.extend_pk_bytes(&pk24(7, 8, 9));
            b.extend_weight(&w.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &payload.to_le_bytes());
            b.count += 1;
        }
        let eff = enforce_unique_pk(&mut pt, &schema, b);
        pt.ingest_owned_batch(eff).unwrap();
        assert!(pt.has_pk_bytes(&pk24(7, 8, 9)), "K2 survives +1,-1,+1 at net +1");
    }
}
