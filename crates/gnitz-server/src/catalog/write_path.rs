//! Catalog write path: `precheck_family` → `apply_bundle_family` → `apply_local`
//! → `fire_hooks`, the broadcast and directory-deletion queues, and Stage-A
//! (DDL rollback) compensation.

use rustc_hash::FxHashMap;
use std::num::NonZeroU64;

use super::*;

/// Every id `map` lists under any of `owners`, widened to the key width
/// `retract_pk_list` takes.
fn owned_ids(map: &FxHashMap<i64, Vec<i64>>, owners: &[i64]) -> Vec<u128> {
    owners
        .iter()
        .filter_map(|o| map.get(o))
        .flatten()
        .map(|&id| id as u128)
        .collect()
}

impl CatalogEngine {
    // -- The applied-delta entry points ----------------------------------------

    /// Precheck, apply and enqueue one system-family delta — the in-process test
    /// spelling of one `DDL_TXN` family step.
    #[cfg(test)]
    pub(crate) fn submit(&mut self, family: SysFamily, batch: Batch) -> Result<(), String> {
        self.precheck_family(family, &batch)?;
        self.apply_bundle_family(family, batch, &mut None)
    }

    /// Apply without enqueueing and without an LSN pin: for [`Self::ddl_sync`],
    /// whose delta is the broadcast, and compensation, whose rows never reach the SAL.
    pub(in crate::catalog) fn submit_local(&mut self, family: SysFamily, mut batch: Batch) -> Result<(), String> {
        self.apply_local(family, &mut batch, None)
    }

    /// Apply and enqueue one `DDL_TXN` family behind the retraction of every system
    /// row its dropped relations own. `applied` holds the batch applied but not yet
    /// enqueued, which `compensate_stage_a` must negate beyond the queue.
    pub(crate) fn apply_bundle_family(
        &mut self,
        family: SysFamily,
        batch: Batch,
        applied: &mut Option<(SysFamily, Batch)>,
    ) -> Result<(), String> {
        for (family, batch) in self.behind_owned_retractions(family, batch) {
            *applied = Some((family, batch.clone()));
            self.apply_and_enqueue_family(family, batch)?;
            *applied = None;
        }
        Ok(())
    }

    /// `batch`, preceded by one retraction batch per family of the rows its dropped
    /// relations own. The retractions copy live rows, so they need no precheck.
    fn behind_owned_retractions(&self, family: SysFamily, mut batch: Batch) -> Vec<(SysFamily, Batch)> {
        let mut owners = match family {
            SysFamily::Table | SysFamily::View => family_pk_partition(family, &batch).drops,
            _ => Vec::new(),
        };
        if owners.is_empty() {
            return vec![(family, batch)];
        }
        if family == SysFamily::View {
            // A view's segments drop in its own batch, unless the bundle already names them.
            let segs: Vec<u128> = owned_ids(&self.caches.segments_by_owner, &owners)
                .into_iter()
                .filter(|&s| !owners.contains(&(s as i64)))
                .collect();
            owners.extend(segs.iter().map(|&s| s as i64));
            let mut merged = retract_pk_list(self.sys_relation(SysFamily::View), segs);
            merged.append_batch(&batch, 0, batch.len());
            batch = merged;
        }
        owners.sort_unstable();
        let owned = if family == SysFamily::Table {
            let ids = owned_ids(&self.caches.indices_by_owner, &owners);
            (
                SysFamily::Index,
                retract_pk_list(self.sys_relation(SysFamily::Index), ids),
            )
        } else {
            let rel = self.sys_relation(SysFamily::CircuitNodes);
            (
                SysFamily::CircuitNodes,
                retract_bands(rel, SysFamily::CircuitNodes, &owners),
            )
        };
        let columns = retract_bands(self.sys_relation(SysFamily::Column), SysFamily::Column, &owners);
        vec![owned, (SysFamily::Column, columns), (family, batch)]
    }

    /// Worker DDL sync: apply a master-broadcast system-table delta. Workers
    /// update their registry from these; durability is master-side (fsynced
    /// SAL + the master's own system-table flush). The worker's inherited copy
    /// lives in RAM (memtable + the RAM tier) and is never flushed by the
    /// worker — only the master writes `_sys/` shards.
    ///
    /// Errors propagate into the worker's DdlSync-fatal path (dispatch treats a
    /// DdlSync error as fatal: STATUS_ERROR + shutdown and _exit, which the
    /// master's watchdog turns into a cluster abort) — a swallowed failure here
    /// diverges this worker's catalog from the master.
    pub(crate) fn ddl_sync(&mut self, table_id: i64, batch: Batch) -> Result<(), String> {
        let family = SysFamily::from_id(table_id).ok_or_else(|| "ddl_sync only for system tables".to_string())?;
        self.submit_local(family, batch)
    }

    // -- System table accessors ------------------------------------------------

    /// This family's relation, from the registry that owns it.
    pub(in crate::catalog) fn sys_relation(&self, family: SysFamily) -> &Relation {
        self.registry
            .relation(family.id())
            .expect("every system family is registered at open")
    }

    /// Apply one delta to its family's storage and fire the reaction hooks —
    /// the shared tail of [`Self::submit`] / [`Self::submit_local`]. When
    /// `pin_lsn` is `Some(lsn)` it pins the family's `current_lsn` to `lsn.get()`
    /// (the DDL zone LSN) so recovery's dedup check (`msg.lsn <= flushed`) matches
    /// the SAL group LSN. Pins never regress the counter: every zone is reserved
    /// with a floor that dominates the pinned family's `current_lsn`
    /// (`ZoneLsnAllocator::reserve`), so even counters drifted by un-pinned
    /// auto-bump ingests sit strictly below the zone LSN pinning them. Does NOT
    /// broadcast.
    pub(super) fn apply_local(
        &mut self,
        family: SysFamily,
        batch: &mut Batch,
        pin_lsn: Option<NonZeroU64>,
    ) -> Result<(), String> {
        let id = family.id();
        // Live DDL's one sound non-fatal exit: a failure here is pre-broadcast
        // (nothing durable, nothing broadcast, client not ACKed), so Stage-A
        // compensation can unwind it. Propagate rather than abort.
        self.registry
            .ingest_borrowed(id, batch, pin_lsn)
            .map_err(|e| format!("apply_local: sys-table ingest failed (family={id}): {e}"))?;
        batch.set_schema(family.schema());
        self.fire_hooks(family, batch)
    }

    // -- Broadcast / dir-deletion queues ------------------------------------

    /// Apply one system-family delta pinned to the open DDL zone LSN, and enqueue
    /// it for broadcast. An empty batch applies and queues nothing.
    pub(crate) fn apply_and_enqueue_family(&mut self, family: SysFamily, mut batch: Batch) -> Result<(), String> {
        if batch.is_empty() {
            return Ok(());
        }
        self.apply_local(family, &mut batch, self.ctx.ddl_zone_lsn())?;
        self.pending_broadcasts.push((family, batch));
        Ok(())
    }

    /// Drain the pending-broadcast queue. Master calls this once per
    /// top-level DDL and forwards each entry to `broadcast_ddl`. Workers
    /// receive system-table changes via `DdlSync` → `ddl_sync`, which
    /// bypasses [`Self::submit`] entirely, so the queue stays empty there.
    pub(crate) fn drain_pending_broadcasts(&mut self) -> Vec<(SysFamily, Batch)> {
        std::mem::take(&mut self.pending_broadcasts)
    }

    /// Physically remove a batch of queued directory paths. An existence guard
    /// keeps a re-queued path (drop applied, dir already gone) quiet.
    pub(super) fn remove_queued_dirs(dirs: Vec<String>) {
        for dir in dirs {
            if std::path::Path::new(&dir).exists() {
                let _ = std::fs::remove_dir_all(&dir);
            }
        }
    }

    /// Physically remove the directories queued by table/view/index drop
    /// hooks. The executor calls this only after the DDL zone's fdatasync
    /// confirms the drop is durable — see `pending_dir_deletions`.
    pub(in crate::catalog) fn drain_pending_dir_deletions(&mut self) {
        Self::remove_queued_dirs(std::mem::take(&mut self.pending_dir_deletions));
    }

    /// Drop the queued directory paths *without* deleting them. Used when a DDL
    /// fails or to clear stale entries left by a prior failed DDL — the entity
    /// did not durably drop, so its files must survive.
    pub(crate) fn discard_pending_dir_deletions(&mut self) {
        self.pending_dir_deletions.clear();
    }

    /// Move durably-dropped directories from the in-flight DDL queue into the
    /// checkpoint-gated queue instead of removing them now. See
    /// `checkpoint_gated_deletions`. Used on the DROP-success path where worker
    /// processes may still be applying the entity's CREATE.
    pub(crate) fn defer_pending_dir_deletions(&mut self) {
        self.checkpoint_gated_deletions.append(&mut self.pending_dir_deletions);
    }

    /// Physically remove every checkpoint-gated directory. SAFE only at a
    /// checkpoint boundary, after the per-worker `Flush` ACKs prove all
    /// workers consumed past the DROP that queued each entry.
    pub(crate) fn drain_checkpoint_gated_deletions(&mut self) {
        Self::remove_queued_dirs(std::mem::take(&mut self.checkpoint_gated_deletions));
    }

    /// Cancel a pending removal of `dir` from *both* deletion queues. Required
    /// when an entity whose on-disk path is *name-based* (not `<name>_<tid>`) is
    /// recreated before the gating checkpoint drains the queue: only schemas
    /// have name-based paths (`<base>/<schema>`), so a `DROP SCHEMA s` +
    /// `CREATE SCHEMA s` would otherwise leave `<base>/s` queued, and the next
    /// checkpoint's `remove_dir_all` would wipe the recreated schema and every
    /// new table beneath it. Tables/indices encode a monotonic tid in their
    /// path, so a recreate never collides with a gated entry and needs no
    /// cancellation.
    ///
    /// Both queues are filtered: in normal operation the DROP and CREATE are
    /// separate DDL RPCs, so the DROP's entry was already moved to the gated
    /// queue by its own `defer` and clearing `pending_dir_deletions` is a no-op.
    /// During SAL recovery, however, a replayed `DROP s` and a replayed
    /// `CREATE s` land in the *same* `pending_dir_deletions` with no intervening
    /// `defer`; the CREATE's own pre-stage `truncate` removes only its push and
    /// leaves the DROP's `<base>/s` — the live, recreated path — in the queue.
    /// Clearing it here lets the recreating hook reclaim that residue before the
    /// boot-time `gc_orphan_directories` drain (or any later checkpoint) can
    /// `remove_dir_all` the live schema and the tables beneath it.
    pub(in crate::catalog) fn cancel_gated_deletion(&mut self, dir: &str) {
        self.checkpoint_gated_deletions.retain(|d| d != dir);
        self.pending_dir_deletions.retain(|d| d != dir);
    }

    /// Remove table, view, and index directories on disk that belong to no live
    /// entity — the residue of a DROP whose checkpoint-gated deletion was lost to
    /// a crash before the next checkpoint drained it. Best-effort: a failure to
    /// remove one orphan is logged and never aborts recovery.
    ///
    /// Two reclamation mechanisms run here: a schema-scoped path scan for orphans
    /// under every live schema, and a `drain_pending_dir_deletions` for every dir
    /// SAL replay re-queued — including a dropped *schema*'s subtree, which the
    /// path scan cannot reach because the schema is gone from `schema_by_id`.
    ///
    /// Must run only after BOTH shard replay (`replay_catalog`) and SAL replay
    /// (`recover_system_tables_from_sal`) have populated the registry; otherwise a
    /// table whose CREATE committed to the SAL but was not yet flushed would be
    /// absent from the registry and its live directory wrongly deleted.
    ///
    /// Sound only because `cancel_gated_deletion` filters `pending_dir_deletions`
    /// too: otherwise the drain could remove a recreated same-name schema whose
    /// live path SAL replay left in the queue.
    pub(crate) fn gc_orphan_directories(&mut self) {
        // Scan only schemas the catalog knows about. We never enumerate
        // `base_dir` for unknown directories: a schema path is an arbitrary user
        // name with no structural marker (`<base>/<schema>`), so removing an
        // unrecognized entry could wipe unrelated host data if base_dir is
        // shared. The real system catalog (`<base>/_system_catalog`) is never a
        // registered schema name, so it is never reached; the `_system`/`public`
        // logical-schema dirs are scanned but the system tables live under
        // `_system_catalog`, so nothing system-owned is ever a candidate.
        self.registry.reclaim_orphan_relation_dirs(
            self.caches
                .schema_by_id
                .values()
                .map(|schema_name| schema_dir(&self.base_dir, schema_name)),
        );

        // SAL replay of any DROP fired hooks that re-pushed the dropped directory
        // onto `pending_dir_deletions` (the committed-but-unflushed crash window).
        // The schema-scoped scan above already removed the orphans under live
        // schemas, but a dropped *schema*'s subtree is unreachable by that scan
        // (the schema is gone from `schema_by_id`). Physically remove everything
        // the replay re-queued so those dirs are reclaimed and no recovery residue
        // is carried into the first DDL/checkpoint. Safe because
        // `cancel_gated_deletion` filters this queue too, so no recreated
        // same-name (live) schema path survives in it.
        self.drain_pending_dir_deletions();
    }

    /// Compile a just-registered view's circuit and throw the result away, so a
    /// circuit the engine cannot run is rejected while the DDL is still undoable.
    /// The path is built here because `catalog::utils` owns every entity
    /// directory's shape, and removed here because its creator is its remover.
    pub(crate) fn preflight_view_compile(&self, vid: i64) -> Result<(), String> {
        let Some((schema_name, _)) = self.caches.entity_by_id.get(&vid) else {
            return Err(format!("pre-flight: view {vid} is not registered"));
        };
        let root = preflight_dir(&self.base_dir, schema_name, vid);
        let CatalogEngine { registry, dag, .. } = self;
        let verdict = dag.preflight_compile(registry, vid, &root);
        Self::remove_queued_dirs(vec![root]);
        verdict
    }

    // -----------------------------------------------------------------------
    // Stage-A compensation (DDL rollback)
    // -----------------------------------------------------------------------

    /// Compensate a failed `DDL_TXN` bundle: undo every in-memory mutation that
    /// was applied before the failure so the catalog is exactly as it was, in
    /// master memory, before any worker sees a byte.
    ///
    /// The drained `pending_broadcasts` already holds every family that was
    /// applied **and enqueued** (the families before the failing one). The
    /// handler additionally passes the single family that was applied but **not
    /// yet enqueued** — a hook/panic failure inside `apply_and_enqueue_family` —
    /// as `applied_not_enqueued`, so it too is negated. On a **precheck** failure
    /// the handler passes `None`: nothing was applied for that family, so nothing
    /// is reconstructed and **no ghost `-1` is written**. At most one family is
    /// ever applied-not-enqueued, so `Option` is the exact type.
    ///
    /// `Err` means the compensation itself failed and the catalog cannot be
    /// restored; the caller that owns a watchdog aborts rather than serve a
    /// diverged catalog.
    pub(crate) fn compensate_stage_a(
        &mut self,
        applied_not_enqueued: Option<(SysFamily, Batch)>,
    ) -> Result<(), String> {
        let mut rollback_list = self.drain_pending_broadcasts();

        if let Some(entry) = applied_not_enqueued {
            rollback_list.push(entry);
        }

        // Precheck-failed first family: nothing applied, trivial no-op.
        if rollback_list.is_empty() {
            return Ok(());
        }

        // Directories of entities the bundle DROPPED are queued for removal, and
        // this rollback restores those entities — so their files must survive.
        // (A hook that staged a directory and then failed already reclaimed it
        // itself; see `with_staged_dir`.) What the rollback queues *below* is a
        // different thing: residue of a creation that never committed.
        self.discard_pending_dir_deletions();

        // Undo each PK by what the bundle did to *it*, not by what the
        // bundle did overall — one family can do both. An ALTER VIEW retires the
        // old vid and registers the new chain in a single VIEW_TAB batch, so the
        // rollback has to tear one down and restore the other, and the two need
        // opposite family orders.
        //
        // A PK carrying both signs is a rewrite pair (a rename): its net stays
        // live, so it counts as a creation and its rows stay in ONE submit. Split
        // across the two phases they would drop the entity's net weight to zero
        // mid-rollback, firing the teardown hook and queueing the live entity's
        // directory for removal.
        let mut undo_create: Vec<(SysFamily, Batch)> = Vec::new();
        let mut undo_drop: Vec<(SysFamily, Batch)> = Vec::new();
        for (family, batch) in rollback_list {
            // Total over every row: `pk_signatures` skips zero-weight rows, and
            // `precheck_family` rejects a delta carrying one before it can be
            // applied — so no PK reaches here with a zero-weight row alone.
            let net: FxHashMap<u128, i64> = pk_signatures(family, &batch).iter().map(|s| (s.pk, s.sum)).collect();
            let (created, dropped): (Vec<u32>, Vec<u32>) =
                (0..batch.len() as u32).partition(|&i| net[&batch.get_pk(i as usize)] >= 0);
            if dropped.is_empty() {
                undo_create.push((family, batch));
            } else if created.is_empty() {
                undo_drop.push((family, batch));
            } else {
                // Both index lists are ascending (a `partition` over an ascending
                // range), so each subset keeps the source's sorted/consolidated
                // tag — which the sign-preserving `map_weights` negation below
                // leaves in place.
                undo_create.push((family, batch.ascending_subset(&created)));
                undo_drop.push((family, batch.ascending_subset(&dropped)));
            }
        }

        // Tear down what the bundle created — dependents before dependencies,
        // DESCENDING…
        undo_create.sort_by_key(|(f, _)| std::cmp::Reverse(f.topo_priority()));
        // …then restore what it dropped — dependencies before dependents,
        // ASCENDING, so a restored view finds its columns, deps, and circuit rows
        // already back when its own VIEW_TAB row re-registers it. Creations first,
        // so a name the bundle moved from one id to another is free again by the
        // time the incumbent reclaims it.
        undo_drop.sort_by_key(|(f, _)| f.topo_priority());
        undo_create.append(&mut undo_drop);

        // Replay each with negated weight through the no-broadcast path.
        // fire_hooks still fires so caches, the registry, and pending_dir_deletions
        // are updated.
        let result = undo_create.into_iter().try_for_each(|(family, mut batch)| {
            batch.map_weights(i64::wrapping_neg);
            self.submit_local(family, batch)
        });

        // Everything the rollback queued is a creation that never committed.
        self.drain_pending_dir_deletions();

        result.map_err(|e| {
            format!(
                "Stage-A DDL compensation failed — catalog cannot be restored, \
                 and serving it would serve a diverged catalog. Cause: {e}"
            )
        })
    }
}
