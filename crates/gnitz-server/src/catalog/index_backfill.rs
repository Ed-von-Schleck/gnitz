//! Secondary-index construction: the chunked base-table scan that projects into
//! an index circuit's store, and its two entry points (a fresh CREATE INDEX and
//! the worker's boot rebuild). The store itself is the registry's — opened by
//! `add_index`, rehomed by `rehome`.

use super::*;
use gnitz_foundation::fault::Seam;

/// `GNITZ_INJECT_INDEX_BACKFILL_ERROR`: fail a live index registration inside its
/// staged directory, for the rollback tests. One-shot, so the retry that proves
/// the rollback left nothing blocking still succeeds.
static INDEX_BACKFILL_ERROR: Seam = Seam::new("GNITZ_INJECT_INDEX_BACKFILL_ERROR");

impl CatalogEngine {
    // -- Index backfill (scan source, project into index table) ------------

    /// Fill the circuit on `cols` of `owner_id` from the owner's local slice.
    /// The circuit is already entered — the projection ingests through it.
    pub(in crate::catalog) fn backfill_index(&mut self, owner_id: i64, cols: &[u32]) -> Result<(), String> {
        if INDEX_BACKFILL_ERROR.take_once() {
            return Err("injected index backfill fault".to_string());
        }
        // The relation filled here is the *index store* (`owner_id` is the
        // indexed relation, a durable base table). Refused unless empty, in every
        // build: an index that resumed from its checkpoint already holds these
        // rows, and projecting them again would double every weight — a wrong
        // answer no later check looks for. `estimated_rows` counts the memtable,
        // so a store the hook just filled reads non-empty with no flush.
        let held = self
            .registry
            .relation(owner_id)
            .and_then(|r| r.index_on(cols))
            .ok_or_else(|| format!("backfill_index: no circuit on {cols:?} of {owner_id}"))?
            .estimated_rows();
        if held != 0 {
            return Err(format!(
                "backfill_index into a non-empty index (owner {owner_id}): would double-count"
            ));
        }
        self.stream_index_projection(owner_id, &[PkColList::from_slice(cols)])
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
            .relations()
            .map(|entry| {
                let targets: Vec<PkColList> = entry
                    .indexes()
                    .iter()
                    .filter(|ic| !ic.resumed() && ic.estimated_rows() == 0)
                    .map(|ic| ic.cols())
                    .collect();
                (entry.id(), targets)
            })
            .filter(|(_, targets)| !targets.is_empty())
            .collect();

        let mut rebuilt = 0usize;
        for (owner_id, targets) in worklist {
            rebuilt += targets.len();
            self.stream_index_projection(owner_id, &targets)?;
        }
        Ok(rebuilt)
    }

    /// One chunked scan of `owner_id`, each chunk projected into every target
    /// circuit's index layout and ingested through it. Peak memory is
    /// O(chunk × row_width).
    ///
    /// No uniqueness check, for a fresh unique index or a promotion alike:
    /// `validate_unique_index_create` ran the global pre-flight before the
    /// IDX_TAB `+1`, and a partition-local check cannot see a duplicate
    /// straddling two workers' slices.
    fn stream_index_projection(&mut self, owner_id: i64, targets: &[PkColList]) -> Result<(), String> {
        if targets.is_empty() {
            return Ok(());
        }
        let chunk_rows = self.registry.scan_chunk_rows();
        let Some(mut handle) = self.registry.relation(owner_id).map(|r| r.cursor()) else {
            return Ok(());
        };
        while let Some(chunk) = handle.drain_chunk(chunk_rows) {
            for cols in targets {
                let ic = self
                    .registry
                    .relation_mut(owner_id)
                    .and_then(|r| r.index_on_mut(cols.as_slice()))
                    .ok_or_else(|| format!("index circuit on {:?} of {owner_id} vanished", cols.as_slice()))?;
                let projected = gnitz_store::storage::batch_project_index(&chunk, &ic.key_spec(), &ic.schema());
                if projected.is_empty() {
                    continue;
                }
                ic.ingest_owned_batch(projected)
                    .map_err(|e| format!("index backfill: ingest failed (owner {owner_id}): {e}"))?;
            }
        }
        Ok(())
    }
}
