//! Secondary-index construction: the chunked base-table scan that projects into
//! an index circuit's store, its two entry points (a fresh CREATE INDEX and the
//! worker's boot rebuild), the UNIQUE promote, and FK auto-index creation. The
//! store itself is the registry's — opened by `add_index`, rehomed by `rehome`.

use super::*;
use gnitz_store::schema::key::PkBuf;

impl CatalogEngine {
    /// Locally retract an index registration whose +1 was applied but never
    /// broadcast. `Err` means the rollback itself failed, leaving the +1 in
    /// sys_indices while the client is handed an error — a permanently diverged
    /// catalog, and the next boot's replay would open a missing index directory.
    /// The caller that owns a watchdog fail-stops on it, matching
    /// `compensate_stage_a`'s rollback.
    pub(super) fn rollback_index_registration(&mut self, undo: Batch, index_id: i64) -> Result<(), String> {
        self.submit_local(SysFamily::Index, undo).map_err(|undo_err| {
            format!(
                "catalog: index registration rollback failed (index_id={index_id}): {undo_err} \
                 — catalog state permanently diverged"
            )
        })
    }

    // -- Index backfill (scan source, project into index table) ------------

    /// Fill the circuit on `cols` of `owner_id` from the owner's local slice.
    /// The circuit is already entered — the projection ingests through it.
    pub(in crate::catalog) fn backfill_index(
        &mut self,
        owner_id: i64,
        cols: &[u32],
        pass: IndexPass,
    ) -> Result<(), String> {
        // The relation filled here is the *index store* (`owner_id` is the
        // indexed relation, a durable base table). Refused unless empty, in every
        // build: an index that resumed from its checkpoint already holds these
        // rows, and projecting them again would double every weight — a wrong
        // answer no later check looks for. `estimated_rows` counts the memtable,
        // so a store the hook just filled reads non-empty with no flush.
        let held = self
            .registry
            .index_circuit_for_cols(owner_id, cols)
            .ok_or_else(|| format!("backfill_index: no circuit on {cols:?} of {owner_id}"))?
            .estimated_rows();
        if held != 0 {
            return Err(format!(
                "backfill_index into a non-empty index (owner {owner_id}): would double-count"
            ));
        }
        self.stream_index_projection(owner_id, &[PkColList::from_slice(cols)], pass)
    }

    /// Worker-boot index rebuild: fill every index store that neither resumed
    /// from its checkpoint nor holds rows, from the rehomed base slice. Runs
    /// BEFORE SAL replay, which projects the committed tail into the index once
    /// more. Returns how many were filled — 0 on a clean restart at the same
    /// topology, and 0 again on a repeat call, since a filled store is skipped.
    pub(crate) fn backfill_all_indexes(&mut self) -> Result<usize, String> {
        // Snapshotted: the projection mutably borrows `self`.
        let worklist: Vec<(i64, Vec<PkColList>)> = self
            .registry
            .entries()
            .map(|(owner_id, entry)| {
                let targets: Vec<PkColList> = entry
                    .index_circuits
                    .iter()
                    .filter(|ic| !ic.resumed_from_checkpoint() && ic.estimated_rows() == 0)
                    .map(|ic| ic.col_indices)
                    .collect();
                (owner_id, targets)
            })
            .filter(|(_, targets)| !targets.is_empty())
            .collect();

        let mut rebuilt = 0usize;
        for (owner_id, targets) in worklist {
            rebuilt += targets.len();
            self.stream_index_projection(owner_id, &targets, IndexPass::Fill)?;
        }
        Ok(rebuilt)
    }

    /// One chunked scan of `owner_id`, each chunk projected into every target
    /// circuit's index layout and handled as `pass` says. Peak memory is
    /// O(chunk × row_width) plus, when checking, one `PkBuf` per scanned key.
    fn stream_index_projection(&mut self, owner_id: i64, targets: &[PkColList], pass: IndexPass) -> Result<(), String> {
        if targets.is_empty() {
            return Ok(());
        }
        let check_dups = pass != IndexPass::Fill;
        let chunk_rows = self.registry.scan_chunk_rows();
        let mut seen: Vec<rustc_hash::FxHashSet<PkBuf>> = if check_dups {
            targets.iter().map(|_| rustc_hash::FxHashSet::default()).collect()
        } else {
            Vec::new()
        };

        let Some(mut handle) = self.registry.open_store_cursor(owner_id) else {
            return Ok(());
        };
        while let Some(chunk) = handle.drain_chunk(chunk_rows) {
            for (ti, cols) in targets.iter().enumerate() {
                let ic = self
                    .registry
                    .index_circuit_for_cols(owner_id, cols.as_slice())
                    .ok_or_else(|| format!("index circuit on {:?} of {owner_id} vanished", cols.as_slice()))?;
                let projected = gnitz_store::storage::batch_project_index(&chunk, &ic.key_spec, &ic.index_schema);
                if projected.is_empty() {
                    continue;
                }
                // The duplicate check applies to the full composite leading span.
                if check_dups && projected_chunk_has_dup_keys(&projected, ic.key_spec.key_size(), &mut seen[ti]) {
                    return Err(self.unique_create_dup_err(owner_id, cols.as_slice()));
                }
                if pass != IndexPass::VerifyUnique {
                    ic.ingest_owned_batch(projected)
                        .map_err(|e| format!("index backfill: ingest failed (owner {owner_id}): {e}"))?;
                }
            }
        }
        Ok(())
    }

    /// Promote the existing index circuit on `col_idx` to unique, after verifying
    /// the committed base rows contain no duplicate keys. Used when a UNIQUE index
    /// registers over a column that already has a circuit (an FK auto-index, or a
    /// prior non-unique index): the per-column dedup keeps one circuit, so the
    /// uniqueness is folded into the incumbent — no second index store is built
    /// (the index schema does not depend on `is_unique`; uniqueness is the flag
    /// plus the duplicate check, not a different storage layout). Empty base table
    /// → pure flag flip. The verification scan runs on a first apply only: replayed
    /// and compensated data passed its duplicate check when originally written,
    /// and under compensation a spurious `Err` is a fatal abort. The flag flip
    /// below runs unconditionally.
    pub(in crate::catalog) fn promote_index_to_unique(
        &mut self,
        owner_id: i64,
        col_indices: &[u32],
    ) -> Result<(), String> {
        if self.ctx.mode() == ApplyMode::Live {
            self.stream_index_projection(owner_id, &[PkColList::from_slice(col_indices)], IndexPass::VerifyUnique)?;
        }
        self.registry.set_index_circuit_uniqueness(owner_id, col_indices, true);
        Ok(())
    }

    // -- FK auto-index creation -------------------------------------------

    pub(in crate::catalog) fn create_fk_indices(&mut self, table_id: i64) -> Result<(), String> {
        // An unregistered owner has no columns to auto-index; checked before the
        // COL_TAB read so the miss costs nothing. The registered schema is where
        // the PK list lives: `register_relation` builds it from the same
        // `TABLE_TAB.pk_col_idx` the row persisted.
        let Some(owner_schema) = self.registry.entry(table_id).map(|e| e.schema) else {
            return Ok(());
        };
        let pk_cols = owner_schema.pk_indices();
        let col_defs = self.read_column_defs(table_id);

        for (col_idx, cd) in col_defs.iter().enumerate() {
            if cd.fk_table_id == 0 {
                continue;
            }
            // Skip every PK column: the PK region already stores them, so an
            // FK whose column is part of the PK needs no separate auto-index.
            if pk_cols.contains(&(col_idx as u32)) {
                continue;
            }
            let index_name = make_fk_index_name(table_id, col_idx);
            if self.caches.index_by_name.contains_key(&index_name) {
                continue;
            }

            let index_id = self
                .allocate_index_id()
                .map_err(|e| format!("index id allocation failed: {e}"))?;

            // Write index record to sys_indices (FK indices are not unique).
            // `submit_local` bypasses the precheck, and is the only path allowed
            // to set the internal bit.
            let packed_cols = gnitz_wire::pack_pk_cols(&[col_idx as u32]);
            let props = gnitz_wire::IndexProps {
                is_unique: false,
                is_internal: true,
            };
            let batch = idx_tab_batch(index_id, table_id, packed_cols, &index_name, props, 1);
            // hook_cascade_fk fires on master and every worker, so each side
            // creates its own FK indices locally; submit_local applies + fires
            // hooks without a broadcast. submit would broadcast IDX_TAB before
            // TABLE_TAB and duplicate the rows the worker already produced.
            if let Err(e) = self.submit_local(SysFamily::Index, batch) {
                // The +1 reached sys_indices but its directory/cache setup failed
                // and was never broadcast. Submit the matching -1 locally to
                // reverse the storage write and any partial cache updates;
                // otherwise the next boot's replay opens a missing index
                // directory and crashes.
                let undo = idx_tab_batch(index_id, table_id, packed_cols, &index_name, props, -1);
                self.rollback_index_registration(undo, index_id)?;
                return Err(e);
            }
        }
        Ok(())
    }
}

/// What one `stream_index_projection` pass does with each projected chunk. The
/// boot rebuild only fills: its rows passed the unique check when first
/// written, and a slice-local check cannot see a duplicate straddling two
/// workers' slices.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(in crate::catalog) enum IndexPass {
    /// Ingest through the circuit.
    Fill,
    /// Ingest, refusing a duplicate leading key — a live CREATE UNIQUE INDEX.
    FillUnique,
    /// Refuse a duplicate leading key and ingest nothing — a UNIQUE promotion
    /// of an already-filled circuit.
    VerifyUnique,
}

/// True if a positive-weight row in a projected index chunk shares its leading
/// index key (the first `key_size` bytes of the index PK) with another row of
/// this chunk or any earlier chunk recorded in `seen`.
///
/// Compound-PK index layout: index PK is `(indexed_key…, src_pk…)`. Uniqueness
/// applies to the leading `indexed_key` span only — two rows differing only in
/// their source-PK suffix represent two source rows sharing the indexed value.
/// For a composite `UNIQUE (a, b, …)` the span is the sum of every promoted
/// column's width and can exceed 16 bytes, so the dedup token is a `PkBuf`
/// holding the raw span (a stack key, no per-row heap allocation), not a
/// truncating `u128`.
///
/// `seen` is caller-owned because the scan is chunked: cross-chunk duplicates
/// are only visible through state carried across calls. Shared by
/// `backfill_index` (fresh unique index) and `promote_index_to_unique`
/// (UNIQUE folded into an existing circuit) so both gate on the same predicate.
fn projected_chunk_has_dup_keys(projected: &Batch, key_size: usize, seen: &mut rustc_hash::FxHashSet<PkBuf>) -> bool {
    for row in 0..projected.len() {
        let weight = projected.get_weight(row);
        if weight <= 0 {
            continue;
        }
        // Base-table scan chunks are consolidated: weight ≥ 2 is the same
        // (PK, payload) row inserted multiple times — that many live
        // instances of the same index key. NULL-valued rows never reach
        // here (batch_project_index skips them).
        if weight > 1 {
            return true;
        }
        let pk_bytes = projected.get_pk_bytes(row);
        if !seen.insert(PkBuf::from_bytes(&pk_bytes[..key_size])) {
            return true;
        }
    }
    false
}

#[cfg(test)]
#[path = "tests/index_backfill.rs"]
mod tests;
