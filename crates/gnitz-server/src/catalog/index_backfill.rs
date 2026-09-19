//! Secondary-index construction: which indexes need filling and when it is safe
//! to fill them, at its two entry points — a fresh CREATE INDEX and the boot
//! rebuild. The scan that fills them is the registry's, as is the store.

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
        self.fill_indexes(owner_id, &[PkColList::from_slice(cols)])
    }

    /// Fill every index store that neither resumed from its checkpoint nor holds
    /// rows, from this process's base slice. Returns how many were filled.
    pub(in crate::catalog) fn backfill_all_indexes(&mut self) -> Result<usize, String> {
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
            self.fill_indexes(owner_id, &targets)?;
        }
        Ok(rebuilt)
    }

    /// Fill `targets` from `owner_id`'s own slice.
    ///
    /// No uniqueness check, for a fresh unique index or a promotion alike:
    /// `validate_unique_index_create` ran the global pre-flight before the
    /// IDX_TAB `+1`, and a partition-local check cannot see a duplicate
    /// straddling two workers' slices.
    fn fill_indexes(&mut self, owner_id: i64, targets: &[PkColList]) -> Result<(), String> {
        self.registry
            .project_into_indexes(owner_id, targets)
            .map_err(|e| format!("index backfill: {e}"))
    }
}
