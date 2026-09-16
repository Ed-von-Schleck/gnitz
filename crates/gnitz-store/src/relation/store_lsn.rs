//! Per-worker store lifecycle across the fork — detach, re-home, child-dir
//! reclamation, invalid-view reset — and the flushed-LSN bookkeeping that
//! recovery and the DDL zone allocator read.

use super::{RelationKind, RelationRegistry, Residency, Store};
use crate::storage::{reclaim_retired_children, remove_child, subdir_names, ChildAddr, Slot, StoreError};

impl RelationRegistry {
    // -- Store management (for multi-worker fork) -----------------------------

    /// Detach every user relation's store (master after fork), so master and
    /// worker 0 do not both hold a live `Table` on `w0of{W}`: two processes
    /// writing one directory is the hazard `naming.rs` exists to prevent. The
    /// system families are excluded because the master goes on serving the
    /// catalog out of them after the fork.
    ///
    /// The fork's two exits from [`Residency::Origin`], this and
    /// [`Self::rehome`], are mutually exclusive and each runs once, so both
    /// assert the residency they leave rather than trusting the caller.
    pub fn detach(&mut self) {
        assert!(
            self.residency == Residency::Origin,
            "detach runs once per process, on a process that still owns its stores",
        );
        self.residency = Residency::Detached;
        for entry in self.tables.values_mut() {
            if entry.kind() == RelationKind::SystemCatalog {
                continue;
            }
            // The delta store and every index go with it: two live `Table`s on
            // one directory is the hazard this pass exists to prevent.
            let schema = entry.schema();
            entry.set_stores((Store::detached(schema), None));
            for ix in &mut entry.indexes {
                ix.store = Store::detached(ix.store.schema());
            }
        }
    }

    /// Panic unless this process is [`Residency::Origin`] — the only one that may
    /// read or delete across ranks, because the workers do not exist yet and the
    /// post-fork master owns no store to speak for them.
    pub(crate) fn assert_origin(&self, who: &str) {
        assert!(
            self.residency == Residency::Origin,
            "{who} must run at Residency::Origin, on a process that still owns its stores",
        );
    }

    /// Become `slot`: re-open every relation and index store this process
    /// inherited on another rank's child at `slot`'s own. A store already homed
    /// there is kept — re-opening it would put two live `Table`s on one
    /// directory — and so is every system family, which is never at a child.
    /// Once per process, post-fork, before any other registry verb.
    ///
    /// One test for all of them: every store of a non-system relation opens
    /// under `ChildAddr::worker` of the registry's slot.
    pub fn rehome(&mut self, slot: Slot) -> Result<(), StoreError> {
        assert!(
            self.residency == Residency::Origin,
            "rehome runs once per process, on a process that still owns its stores",
        );
        let previous = std::mem::replace(&mut self.slot, slot);
        // A rehome moves between ranks of one layout, never between layouts: a
        // host latches its resume verdict against this count once.
        debug_assert_eq!(previous.of, slot.of, "rehome must keep the launched worker count");
        self.residency = Residency::Worker;
        // The master's system-family stores stay open here and replay the same
        // catalog deltas, but only the master writes `_sys/` shards.
        for entry in self.tables.values_mut() {
            if let (RelationKind::SystemCatalog, Some(t)) = (entry.kind(), entry.store.table_mut()) {
                t.hold_in_ram();
            }
        }
        if previous == slot {
            return Ok(());
        }
        let tids: Vec<i64> = self
            .tables
            .iter()
            // A system family's store is flat, so it is homed nowhere and stays
            // put — and re-opening one would put two live `Table`s on its
            // directory, since the rebuild opens before it drops the old store.
            .filter(|(_, e)| e.kind() != RelationKind::SystemCatalog && e.store.table().is_some())
            .map(|(&tid, _)| tid)
            .collect();
        for tid in tids {
            self.rebuild_relation_store(tid, "rehome store")?;
        }
        let (recovery, budgets) = (self.rederive_source(), self.store_budgets());
        for entry in self.tables.values_mut() {
            if entry.kind() == RelationKind::SystemCatalog {
                continue;
            }
            let owner_dir = entry.directory().to_string();
            for ix in &mut entry.indexes {
                if ix.store.table().is_none() {
                    continue;
                }
                let (index_id, index_schema) = (ix.index_id, ix.store.schema());
                ix.store = Store::owned(
                    Self::open_index_table(
                        slot,
                        recovery,
                        budgets,
                        &ChildAddr::Index { id: index_id }.dir(&owner_dir),
                        index_id,
                        index_schema,
                    )?,
                    index_schema,
                );
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

    /// Reclaim every live relation's child directories that this boot's worker
    /// count no longer owns — the on-disk counterpart of `rehome`. Runs after the
    /// boot relayout, so what it deletes the relayout has already consumed.
    ///
    /// Idempotent and unconditional: a boot dying between this and
    /// `record_topology` leaves the recorded count unchanged, so a "the count
    /// changed" trigger would skip the repair on the retry.
    pub fn reconcile_child_dirs(&self) -> Result<(), StoreError> {
        self.assert_origin("reconcile_child_dirs");
        for entry in self.tables.values() {
            // System tables are single-partition `Table`s with no children.
            if entry.kind() == RelationKind::SystemCatalog {
                continue;
            }
            // A storeless relation's `directory` names a path that was never created;
            // `reclaim_retired_children` reads it as having no children and returns.
            reclaim_retired_children(entry.directory(), self.slot.of)
                .map_err(|e| StoreError::storage(format!("reclaim children of {}", entry.directory()), e))?;
        }
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
        self.assert_origin("view_children_resumable");
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
