//! Per-worker store lifecycle across the fork — detach, re-home, child-dir
//! reclamation, invalid-view reset — and the flushed-LSN bookkeeping that
//! recovery and the DDL zone allocator read.

use super::{RelationKind, RelationRegistry, RelationStores, StoreHandle};
use crate::storage::{reclaim_retired_children, remove_child, subdir_names, ChildAddr, Slot, StoreError};

impl RelationRegistry {
    // -- Store management (for multi-worker fork) -----------------------------

    /// Detach every user relation's store (master after fork), so master and
    /// worker 0 do not both hold a live `Table` on `w0of{W}`: two processes
    /// writing one directory is the hazard `naming.rs` exists to prevent. The
    /// system families are single-partition and are the catalog the master goes
    /// on serving from, so they are excluded — by kind, which is what says so.
    pub fn detach_user_stores(&mut self) {
        self.owns_stores = false;
        for entry in self.tables.values_mut() {
            if entry.kind == RelationKind::SystemCatalog {
                continue;
            }
            // The delta store and every index circuit go with it: two live
            // `Table`s on one directory is the hazard this pass exists to prevent.
            entry.set_stores(RelationStores::elsewhere());
            for ic in &mut entry.index_circuits {
                ic.handle = StoreHandle::Elsewhere;
            }
        }
    }

    /// Panic unless this is the pre-fork master. Both boot passes below reach
    /// across the whole cluster — one reclaims every relation's retired children,
    /// the other peeks every launched rank's manifest — which only that one
    /// process may do: the workers do not exist yet and the post-fork master owns
    /// no store to speak for them.
    pub fn assert_pre_fork(&self, who: &str) {
        assert!(
            self.owns_stores && !self.rehomed,
            "{who} must run pre-fork on the master, which still owns its stores",
        );
    }

    /// Become `slot`: re-open every relation and index store this process
    /// inherited on another rank's child at `slot`'s own. A store already homed
    /// there is kept — re-opening it would put two live `Table`s on one
    /// directory — and so is every system family, which is never at a child.
    /// Once per process, post-fork, before any other registry verb.
    pub fn rehome(&mut self, slot: Slot) -> Result<(), StoreError> {
        assert!(!self.rehomed, "rehome runs once per process");
        self.slot = slot;
        self.rehomed = true;
        let home = ChildAddr::worker(slot);
        let tids: Vec<i64> = self
            .tables
            .iter()
            .filter(|(_, e)| {
                // A system family is single-partition — its store is flat, never
                // at a `w{k}of{n}` child — so the address test would match it.
                e.kind != RelationKind::SystemCatalog
                    && e.handle
                        .as_owned()
                        .is_some_and(|t| t.directory() != home.dir(&e.directory))
            })
            .map(|(&tid, _)| tid)
            .collect();
        for tid in tids {
            self.rebuild_relation_store(tid, "rehome store")?;
        }
        let (recovery, ram) = (self.rederive_source(), self.config.ram);
        for entry in self.tables.values_mut() {
            if entry.kind == RelationKind::SystemCatalog {
                continue;
            }
            let owner_dir = &entry.directory;
            for ic in &mut entry.index_circuits {
                let idx_dir = ChildAddr::Index { id: ic.index_id }.dir(owner_dir);
                if ic.handle.as_owned().is_none_or(|t| t.directory() == home.dir(&idx_dir)) {
                    continue;
                }
                ic.handle = StoreHandle::owned(Self::open_index_table(
                    slot,
                    recovery,
                    ram,
                    &idx_dir,
                    ic.index_id,
                    ic.index_schema,
                )?);
            }
        }
        Ok(())
    }

    /// Rebuild `tid`'s store handle from its registered spec and install it, homed
    /// at whatever slot this process now runs as. The caller does any on-disk
    /// preparation first. Both stores are rebuilt, so a fed view cannot come back
    /// declaring a feed it has no store for.
    pub(crate) fn rebuild_relation_store(&mut self, tid: i64, what: &str) -> Result<(), StoreError> {
        let (dir, schema, kind, budgets) = {
            let e = self
                .tables
                .get(&tid)
                .ok_or_else(|| StoreError::rejected(format!("{what}: relation {tid} is not registered")))?;
            (e.directory.clone(), e.schema, e.kind, e.budgets)
        };
        let stores = self
            .build_relation_store(kind, &dir, tid, schema, budgets)
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
    pub fn reconcile_child_dirs(&self) {
        self.assert_pre_fork("reconcile_child_dirs");
        for entry in self.tables.values() {
            // System tables are single-partition `Table`s with no children.
            if entry.kind == RelationKind::SystemCatalog {
                continue;
            }
            // A storeless relation's `directory` names a path that was never created;
            // `reclaim_retired_children` reads it as having no children and returns.
            reclaim_retired_children(&entry.directory, self.slot.of);
        }
    }

    /// Reset a relation's store and per-worker operator scratch to an empty,
    /// well-formed state, before an invalid view is rebuilt. The caller drops the
    /// cached plan.
    ///
    /// The manifest is unlinked first so the rebuild's `Rederive` open peeks
    /// `None` and *erases* the stale shards — without which a transitively-invalid
    /// view whose own manifests are still at the resume generation would reload
    /// them.
    pub fn reset_store(&mut self, vid: i64) -> Result<(), StoreError> {
        let dir = self
            .tables
            .get(&vid)
            .ok_or_else(|| StoreError::rejected(format!("reset_store: relation {vid} is not registered")))?
            .directory
            .clone();

        let rank = self.slot.rank;
        let _ = std::fs::remove_file(ChildAddr::worker(self.slot).manifest(&dir));

        // Rebuild empty. `Table::new` erases the stale shards (manifest now
        // absent → `Rederive` peek `None`).
        self.rebuild_relation_store(vid, "reset view output")?;

        // Remove this worker's per-view operator scratch dirs (rank-stamped).
        // Through `remove_child` so a crash mid-removal cannot leave a manifest
        // behind whose shards are gone — `remove_dir_all` deletes in readdir order.
        for name in subdir_names(&dir) {
            if matches!(ChildAddr::parse(&name), Some(ChildAddr::Scratch { rank: r, .. }) if r == rank) {
                remove_child(&format!("{dir}/{name}"));
            }
        }
        Ok(())
    }

    /// Every registered relation's `(table id, kind, current_lsn)` — the one walk
    /// behind both the recovery dedup maps and the zone-allocator floor. The
    /// registry owns a system family's store exactly as it owns a user
    /// relation's, so one walk covers both bands.
    fn all_store_lsns(&self) -> impl Iterator<Item = (i64, RelationKind, u64)> + '_ {
        self.tables
            .iter()
            .map(|(&tid, entry)| (tid, entry.kind, entry.handle.current_lsn()))
    }

    /// The system families' `table id → max flushed LSN`: the dedup filter for the
    /// master's pre-fork SAL walk. Selected by the kind the iterator already
    /// yields, not by an id band — every system family is registered
    /// `SystemCatalog`, and nothing else is.
    pub fn system_flushed_lsns(&self) -> std::collections::HashMap<i64, u64> {
        self.all_store_lsns()
            .filter(|&(_, kind, _)| kind == RelationKind::SystemCatalog)
            .map(|(tid, _, lsn)| (tid, lsn))
            .collect()
    }

    /// The user relations' `table id → max flushed LSN`: the dedup filter for a
    /// worker's post-fork SAL walk. A storeless relation is absent rather than
    /// present at 0 — there is nothing to recover into, and a stream admitted at
    /// LSN 0 would make the worker skip its own replay.
    pub fn user_flushed_lsns(&self) -> std::collections::HashMap<i64, u64> {
        self.all_store_lsns()
            .filter(|&(_, kind, _)| matches!(kind, RelationKind::BaseTable | RelationKind::View))
            .map(|(tid, _, lsn)| (tid, lsn))
            .collect()
    }

    /// Maximum `current_lsn` across all tables — system and user. The
    /// executor seeds its zone-LSN allocator from this at boot and passes it
    /// as the reservation floor per DDL, so every allocated zone LSN is
    /// strictly greater than each table's current counter: no recovery
    /// watermark a checkpoint persisted can cover a committed-but-unflushed
    /// zone, and a failed zone's pinned LSN is never reused.
    pub fn max_table_current_lsn(&self) -> u64 {
        self.all_store_lsns().map(|(_, _, lsn)| lsn).max().unwrap_or(0)
    }
}
