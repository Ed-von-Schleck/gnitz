//! Per-process store lifecycle across the fork — the store open, the boot
//! relayout and child-dir reclamation, invalid-view reset — and the flushed-LSN
//! bookkeeping that recovery and the DDL zone allocator read.

use super::{RelationKind, RelationRegistry, Residency, Store};
use crate::storage::{reclaim_retired_children, remove_child, subdir_names, ChildAddr, Slot, StoreError};

impl RelationRegistry {
    // -- Store management (for multi-worker fork) -----------------------------

    /// Take rank `rank` of the launched count as `residency`, and open this
    /// process's store for every relation and index.
    pub fn open_stores(&mut self, rank: u32, residency: Residency) -> Result<(), StoreError> {
        assert_eq!(
            self.residency,
            Residency::Master,
            "open_stores runs once, on a master registry"
        );
        assert!(self.children_reconciled, "open_stores before reconcile_child_dirs");
        assert!(residency.owns_stores());
        let slot = Slot::new(rank, self.slot.of);
        self.slot = slot;
        self.residency = residency;
        let tids: Vec<i64> = self
            .tables
            .iter()
            .filter(|(_, e)| e.kind() != RelationKind::SystemCatalog)
            .map(|(&tid, _)| tid)
            .collect();
        for tid in tids {
            self.rebuild_relation_store(tid, "open store")?;
        }
        let (recovery, budgets) = (self.rederive_source(), self.store_budgets());
        for entry in self.tables.values_mut() {
            let owner_dir = entry.directory().to_string();
            for ix in &mut entry.indexes {
                let (index_id, schema) = (ix.index_id, ix.store.schema());
                ix.store = Store::owned(
                    Self::open_index_table(
                        slot,
                        recovery,
                        budgets,
                        &ChildAddr::Index { id: index_id }.dir(&owner_dir),
                        index_id,
                        schema,
                    )?,
                    schema,
                );
            }
        }
        // A worker applies the same catalog deltas as the master, but only the
        // master writes `_sys/` shards.
        if residency == Residency::Worker {
            for entry in self.tables.values_mut() {
                if let (RelationKind::SystemCatalog, Some(t)) = (entry.kind(), entry.store.table_mut()) {
                    t.hold_in_ram();
                }
            }
        }
        Ok(())
    }

    /// Rebuild `tid`'s store handle from its registered spec and install it, homed
    /// at whatever slot this process now runs as. The caller does any on-disk
    /// preparation first. Both stores are rebuilt, so a fed view cannot come back
    /// declaring a feed it has no store for.
    pub(crate) fn rebuild_relation_store(&mut self, tid: i64, what: &str) -> Result<(), StoreError> {
        let (dir, schema, kind, props) = {
            let e = self
                .tables
                .get(&tid)
                .ok_or_else(|| StoreError::rejected(format!("{what}: relation {tid} is not registered")))?;
            (e.directory().to_string(), e.schema(), e.kind(), e.props())
        };
        let stores = self
            .build_relation_store(kind, &dir, tid, schema, props)
            .map_err(|e| e.in_context(&format!("{what} tid={tid}")))?;
        self.tables.get_mut(&tid).expect("entry read above").set_stores(stores);
        Ok(())
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
                    entry.id() as u32,
                    self.slot.of,
                    self.config.ram_tier_bytes,
                    self.config.scan_chunk_rows,
                )?;
            }
            // A storeless relation's `directory` names a path that was never created;
            // `reclaim_retired_children` reads it as having no children and returns.
            reclaim_retired_children(entry.directory(), self.slot.of)
                .map_err(|e| StoreError::storage(format!("reclaim children of {}", entry.directory()), e))?;
        }
        self.children_reconciled = true;
        Ok(())
    }

    /// Reset a relation's store and per-worker operator scratch to an empty,
    /// well-formed state, before an invalid view is rebuilt. The caller drops the
    /// cached plan.
    ///
    /// The manifest is unlinked first so the rebuild's `Rederive` open peeks
    /// `None` and *erases* the stale shards — without which a transitively-invalid
    /// view whose own manifests are still at the resume generation would reload
    /// them.
    pub fn reset_view(&mut self, vid: i64) -> Result<(), StoreError> {
        let entry = self
            .tables
            .get(&vid)
            .ok_or_else(|| StoreError::rejected(format!("reset_view: relation {vid} is not registered")))?;
        let dir = entry.directory().to_string();
        if let Some(t) = entry.store().table() {
            t.unlink_manifest()
                .map_err(|e| StoreError::storage(format!("reset_view: unlink manifest of {vid}"), e))?;
        }

        let rank = self.slot.rank;

        // Rebuild empty. `Table::new` erases the stale shards (manifest now
        // absent → `Rederive` peek `None`).
        self.rebuild_relation_store(vid, "reset view output")?;

        // Remove this worker's per-view operator scratch dirs (rank-stamped).
        let scratch_err = |e| StoreError::storage(format!("reset_view: scratch of {vid}"), e);
        for name in subdir_names(&dir).map_err(scratch_err)? {
            if matches!(ChildAddr::parse(&name), Some(ChildAddr::Scratch { rank: r, .. }) if r == rank) {
                remove_child(&format!("{dir}/{name}")).map_err(scratch_err)?;
            }
        }
        Ok(())
    }

    /// Whether every rederived child of `view_id`, on **every launched rank**,
    /// carries a manifest at this registry's resume generation — the store half
    /// of the resume verdict. It reads the operator scratch alongside the output
    /// store, which is what rejects an output store one generation ahead of the
    /// integral beneath it: the ephemeral round stamps every output store but
    /// only a *compiled* view's traces. `false` for an id this registry does not
    /// hold.
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

    /// Raise `id`'s store LSN counter to at least `lsn`; a no-op for an unregistered
    /// id or a detached store.
    pub fn pin_lsn(&mut self, id: i64, lsn: u64) {
        if let Some(entry) = self.tables.get_mut(&id) {
            entry.store.pin_lsn(lsn);
        }
    }

    /// The system families' `table id → LSN counter`: the replay floors of the
    /// master's pre-fork SAL walk.
    pub fn system_flushed_lsns(&self) -> std::collections::HashMap<i64, u64> {
        self.system_lsns().collect()
    }

    /// The highest system-family LSN counter.
    pub fn max_system_lsn(&self) -> u64 {
        self.system_lsns().map(|(_, lsn)| lsn).max().unwrap_or(0)
    }

    fn system_lsns(&self) -> impl Iterator<Item = (i64, u64)> + '_ {
        self.tables
            .iter()
            .filter(|(_, entry)| entry.kind() == RelationKind::SystemCatalog)
            .map(|(&tid, entry)| (tid, entry.current_lsn()))
    }
}
