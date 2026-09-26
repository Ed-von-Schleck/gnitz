//! Per-process store lifecycle across the fork — the store open, the boot
//! relayout and child-dir reclamation, view reset and rebuild start — and the
//! system families' replay floors recovery reads.

use super::{RelationKind, RelationRegistry, Residency, SecondaryIndex};
use crate::storage::{reclaim_retired_children, remove_child, subdir_names, ChildAddr, ChildKind, Slot, StoreError};

impl RelationRegistry {
    // -- Store management (for multi-worker fork) -----------------------------

    /// Take rank `rank` of the launched count as `residency`, open this process's
    /// store for every relation and index, and fill each index that did not resume.
    /// Returns how many were filled.
    pub fn open_stores(&mut self, rank: u32, residency: Residency) -> Result<usize, StoreError> {
        assert_eq!(
            self.residency,
            Residency::Master,
            "open_stores runs once, on a master registry"
        );
        assert!(self.children_reconciled, "open_stores before reconcile_child_dirs");
        assert!(residency.owns_stores());
        self.slot = Slot::new(rank, self.slot.of);
        self.residency = residency;
        let tids: Vec<i64> = self
            .tables
            .iter()
            .filter(|(_, e)| e.kind() != RelationKind::SystemCatalog)
            .map(|(&tid, _)| tid)
            .collect();
        let mut filled = 0usize;
        for tid in tids {
            filled += self.reopen_stores(tid, "open store")?;
        }
        // A worker applies the same catalog deltas as the master, but only the
        // master writes `_sys/` shards.
        if residency == Residency::Worker {
            for entry in self.tables.values_mut() {
                if entry.kind() == RelationKind::SystemCatalog {
                    entry.store.held_mut().hold_in_ram();
                }
            }
        }
        Ok(filled)
    }

    /// Reopen every store of `tid` at this process's slot, and fill each index
    /// that did not resume. Returns how many it filled.
    fn reopen_stores(&mut self, tid: i64, what: &str) -> Result<usize, StoreError> {
        assert!(self.residency.owns_stores(), "{what}: this process holds no user store");
        let (schema, kind) = {
            let e = self.relation_or_err(tid).map_err(|e| e.in_context(what))?;
            (e.schema(), e.kind())
        };
        let (slot, recovery, budgets) = (
            self.slot,
            self.rederive_source(self.resume_enabled),
            self.store_budgets(),
        );
        let chunk_rows = self.config.scan_chunk_rows;
        let entry = self.tables.get(&tid).expect("entry read above");
        let stores = self
            .build_relation_store(kind, &entry.directory, tid, schema, false)
            .map_err(|e| e.in_context(&format!("{what} tid={tid}")))?;
        let entry = self.tables.get_mut(&tid).expect("entry read above");
        (entry.store, entry.delta) = stores;
        for ix in &mut entry.indexes {
            ix.store = Self::open_index_store(slot, recovery, budgets, &entry.directory, ix.index_id, ix.schema())?;
        }
        let mut targets: Vec<&mut SecondaryIndex> = entry.indexes.iter_mut().filter(|ix| !ix.resumed()).collect();
        super::ingest::fill_indexes(&entry.store, chunk_rows, tid, &mut targets)?;
        Ok(targets.len())
    }

    /// Relay each base table's children onto this boot's worker count, then
    /// reclaim every child directory that count no longer owns. Idempotent.
    pub fn reconcile_child_dirs(&mut self) -> Result<(), StoreError> {
        // A relay removes the set it read, and the reclaim deletes directories.
        assert_eq!(
            self.residency,
            Residency::Master,
            "reconcile_child_dirs runs on a process holding no user store"
        );
        for entry in self.tables.values() {
            // System tables are single-partition `Table`s with no children.
            if entry.kind() == RelationKind::SystemCatalog {
                continue;
            }
            // Only a base table carries rows across a worker-count change.
            if entry.kind().is_base_table() {
                crate::storage::repartition_relation(
                    entry.directory(),
                    &entry.schema(),
                    self.slot.of,
                    self.config.ram_tier_bytes,
                    self.config.scan_chunk_rows,
                )?;
            }
            reclaim_retired_children(entry.directory(), self.slot.of)
                .map_err(|e| StoreError::storage(format!("reclaim children of {}", entry.directory()), e))?;
        }
        self.children_reconciled = true;
        Ok(())
    }

    /// Empty a view's output store: without its manifest, the reopen erases the
    /// shards instead of resuming them.
    pub fn reset_view(&mut self, vid: i64) -> Result<(), StoreError> {
        self.relation_mut_or_err(vid)
            .map_err(|e| e.in_context("reset_view"))?
            .store
            .held_mut()
            .unlink_manifest()
            .map_err(|e| StoreError::storage(format!("reset_view: unlink manifest of {vid}"), e))?;
        self.reopen_stores(vid, "reset view output").map(drop)
    }

    /// The bytes `id`'s next published manifest carries beside its rows.
    pub fn set_caller_record(&mut self, id: i64, record: Vec<u8>) -> Result<(), StoreError> {
        self.relation_mut_or_err(id)
            .map_err(|e| e.in_context("set_caller_record"))?
            .store
            .table_mut()
            .ok_or_else(|| StoreError::rejected(format!("relation {id} holds no store in this process")))?
            .set_caller_record(record);
        Ok(())
    }

    /// Start rebuilding `vid` if it is non-resumable: remove the operator traces
    /// a previous compile left on this worker, and unmark it.
    pub fn begin_rebuild(&mut self, vid: i64) -> Result<(), StoreError> {
        if !self.non_resumable.contains(&vid) {
            return Ok(());
        }
        let dir = self
            .relation_or_err(vid)
            .map_err(|e| e.in_context("begin_rebuild"))?
            .directory();
        let slot = self.slot;
        let scratch_err = |e| StoreError::storage(format!("begin_rebuild: scratch of {vid}"), e);
        for name in subdir_names(dir).map_err(scratch_err)? {
            if ChildAddr::parse(&name).is_some_and(|c| matches!(c.kind, ChildKind::Scratch(_)) && c.slot == slot) {
                remove_child(&format!("{dir}/{name}")).map_err(scratch_err)?;
            }
        }
        self.non_resumable.remove(&vid);
        Ok(())
    }

    /// Whether every checkpointed child of `view_id`, on **every launched rank**,
    /// carries a manifest at this registry's resume generation — the store half
    /// of the resume verdict. `false` for an id this registry does not hold.
    pub fn view_children_resumable(&self, view_id: i64) -> bool {
        // A worker cannot speak for its peers.
        assert!(
            matches!(self.residency, Residency::Master | Residency::Origin),
            "view_children_resumable reads every rank's children"
        );
        self.relation(view_id).is_some_and(|e| {
            crate::storage::children_at_generation(e.directory(), self.slot.of, self.resume_generation)
        })
    }

    /// The system families' `table id → replay floor` their stores opened with:
    /// the floors of the master's pre-fork SAL walk.
    pub fn system_replay_floors(&self) -> std::collections::HashMap<i64, u64> {
        self.tables
            .iter()
            .filter(|(_, entry)| entry.kind() == RelationKind::SystemCatalog)
            .map(|(&tid, entry)| (tid, entry.store.held().replay_floor()))
            .collect()
    }
}
