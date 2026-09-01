//! Catalog write-path spine — the ingest/apply pipeline every system-table
//! mutation flows through: `submit` → `precheck_family` → `apply_local` →
//! `fire_hooks`, plus the broadcast queue, the directory-deletion queues, and
//! Stage-A (DDL rollback) compensation. The three steps it delegates live in
//! their own modules: `precheck.rs`, `hooks.rs`, and `sys_tables.rs` /
//! `apply_context.rs` for `SysFamily` / `ApplyContext`.

use rustc_hash::FxHashMap;
use std::num::NonZeroU64;

use super::*;

impl CatalogEngine {
    // -- The applied-delta entry points ----------------------------------------

    /// Apply one system-family delta and enqueue it for broadcast: precheck →
    /// storage write → `fire_hooks` → enqueue. This is how DDL code mutates a
    /// system family; the cascades that drop columns/indices/circuit rows are the
    /// applier's declared reaction to a retraction, fired from inside
    /// `fire_hooks`, not the emitter's concern. The batch is taken by value so
    /// the applier moves it straight into `pending_broadcasts` (one storage
    /// clone, no hooks clone).
    ///
    /// Two paths deliberately write without it: `bootstrap_ingest` seeds a fresh
    /// database before the DAG exists (and `replay_catalog` fires the hooks over
    /// those rows immediately after), and `advance_sequence` writes the
    /// memtable-only object-id high-water. The DDL_TXN handler drives the same
    /// two steps itself so it can tell a precheck rejection from a post-apply
    /// failure.
    pub(in crate::catalog) fn submit(&mut self, family: SysFamily, batch: Batch) -> Result<(), String> {
        if self.ctx.in_rollback() {
            // During rollback all cascade writes must bypass pending_broadcasts
            // so no compensating row is re-broadcast to workers.
            return self.submit_local(family, batch);
        }
        self.precheck_family(family, &batch)?;
        self.apply_and_enqueue_family(family, batch)
    }

    /// Apply locally without enqueuing a broadcast. ONLY for rows the workers
    /// already produce themselves (FK indices auto-created from the same
    /// `TABLE_TAB` delta), for rollback compensation, and for [`Self::ddl_sync`],
    /// where the delta being applied IS the broadcast. Re-broadcasting these
    /// would deliver phantom deltas.
    ///
    /// No LSN pin: local applies own no zone's durability. `None` is deliberate
    /// even while a DDL zone is active — these rows are not in the SAL, so
    /// pinning their family's `current_lsn` would advance the recovery dedup
    /// watermark with no matching SAL group.
    pub(in crate::catalog) fn submit_local(&mut self, family: SysFamily, mut batch: Batch) -> Result<(), String> {
        self.apply_local(family, &mut batch, None)
    }

    /// [`Self::submit`] for a row the applier built itself: an owner-drop
    /// cascade's `-1`, copied out of the very store the precheck would re-read.
    /// Skips the precheck, because there is nothing left for it to check —
    /// its CAS compares the copy against its own source, its guards exist to
    /// police a *standalone* user DROP (the owner's drop already passed its own
    /// FK/view-dep precheck), and its CREATE arms cannot fire on an all-negative
    /// batch. Keeps `submit`'s rollback redirect: a compensating cascade must
    /// still bypass the broadcast queue.
    pub(super) fn submit_cascade(&mut self, family: SysFamily, batch: Batch) -> Result<(), String> {
        if self.ctx.in_rollback() {
            return self.submit_local(family, batch);
        }
        self.apply_and_enqueue_family(family, batch)
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

    /// This family's owned store.
    pub(in crate::catalog) fn sys_store(&self, family: SysFamily) -> &Table {
        &self.sys_stores[family.index()]
    }

    pub(in crate::catalog) fn sys_store_mut(&mut self, family: SysFamily) -> &mut Table {
        &mut self.sys_stores[family.index()]
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
        let table = self.sys_store_mut(family);
        // Live DDL's one sound non-fatal exit: a failure here is pre-broadcast
        // (nothing durable, nothing broadcast, client not ACKed), so Stage-A
        // compensation can unwind it. Propagate rather than abort.
        table
            .ingest_borrowed_batch(batch)
            .map_err(|e| format!("apply_local: sys-table ingest failed (family={id}): {e}"))?;
        if let Some(lsn) = pin_lsn {
            table.pin_lsn(lsn);
        }
        batch.set_schema(family.schema());
        self.fire_hooks(family, batch)
    }

    // -- System ingestion entry + broadcast / dir-deletion queues -----------

    /// Ingest a batch into any relation, system or user: a system family goes
    /// through [`Self::submit`] (precheck → ingest → hooks → broadcast-queue), a
    /// user table through [`RelationRegistry::ingest_returning_effective`] (PK
    /// enforcement, store, index projection). The `&Batch` entry serves callers
    /// that hold a borrow; an emitter with an owned batch calls `submit` directly
    /// to skip the clone. Every production caller targets a system family, so the
    /// user-table arm's clone is on a test-only path.
    pub(crate) fn ingest_to_family(&mut self, table_id: i64, batch: &Batch) -> Result<(), String> {
        if table_id < FIRST_USER_TABLE_ID {
            let family = SysFamily::from_id(table_id).ok_or_else(|| format!("Unknown system family {table_id}"))?;
            self.submit(family, batch.clone())
        } else {
            self.registry
                .ingest_returning_effective(table_id, batch.clone_batch())
                .map(drop)
                .map_err(|e| format!("ingest failed for table_id={table_id}: {e}"))
        }
    }

    /// Apply one system-family delta to storage, fire its hooks, and enqueue it
    /// for broadcast — the mutating half of [`Self::submit`], pinned
    /// to the open DDL zone LSN. Takes the batch by value so it moves straight
    /// into `pending_broadcasts` (one storage clone, no hooks clone). Enqueue
    /// happens after hooks so nested cascade pushes land first and the executor
    /// broadcasts children → parent; empty batches are dropped so worker-side
    /// no-op cascades don't accumulate unread entries.
    pub(crate) fn apply_and_enqueue_family(&mut self, family: SysFamily, mut batch: Batch) -> Result<(), String> {
        self.apply_local(family, &mut batch, self.ctx.ddl_zone_lsn())?;
        if batch.count > 0 {
            self.pending_broadcasts.push((family, batch));
        }
        Ok(())
    }

    /// Drain the pending-broadcast queue. Master calls this once per
    /// top-level DDL and forwards each entry to `broadcast_ddl`. Workers
    /// receive system-table changes via FLAG_DDL_SYNC → `ddl_sync`, which
    /// bypasses `ingest_to_family` entirely, so the queue stays empty there.
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
    /// checkpoint boundary, after the per-worker FLAG_FLUSH ACKs prove all
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
        // Full on-disk path of every live table/view (user + system).
        let live_tables: rustc_hash::FxHashSet<&str> = self.registry.directories().collect();

        // Full on-disk path of every live index: `<owner_dir>/idx_<idx_id>`.
        // Named from each circuit's `index_id` — the id the directory was
        // created under — and not from `caches.indices_by_owner`, which holds
        // every IDX_TAB id. The two diverge when a second index registers on a
        // column list already covered and the first is then dropped: the circuit
        // and its directory survive under the first registrant's id, so a cache
        // read would find no live entry and remove a live index tree.
        let mut live_indices: rustc_hash::FxHashSet<String> = rustc_hash::FxHashSet::default();
        for (_, entry) in self.registry.entries() {
            for ic in &entry.index_circuits {
                live_indices.insert(index_dir(&entry.directory, ic.index_id));
            }
        }

        // Scan only schemas the catalog knows about. We never enumerate
        // `base_dir` for unknown directories: a schema path is an arbitrary user
        // name with no structural marker (`<base>/<schema>`), so removing an
        // unrecognized entry could wipe unrelated host data if base_dir is
        // shared. The real system catalog (`<base>/_system_catalog`) is never a
        // registered schema name, so it is never reached; the `_system`/`public`
        // logical-schema dirs are scanned but the system tables live under
        // `_system_catalog`, so nothing system-owned is ever a candidate.
        for schema_name in self.caches.schema_by_id.values() {
            let schema_dir = schema_dir(&self.base_dir, schema_name);
            for name in subdir_names(&schema_dir) {
                let full = format!("{schema_dir}/{name}");

                if live_tables.contains(full.as_str()) {
                    // Live table/view: sweep orphaned `idx_<id>` sub-dirs left by
                    // a standalone DROP INDEX whose gated deletion was lost to a
                    // crash.
                    for idx_name in subdir_names(&full) {
                        if !matches!(ChildAddr::parse(&idx_name), Some(ChildAddr::Index { .. })) {
                            continue;
                        }
                        let idx_full = format!("{full}/{idx_name}");
                        if live_indices.contains(&idx_full) {
                            // Its per-worker children are `reconcile_child_dirs`'
                            // job — the sweep descends into an index dir.
                            continue;
                        }
                        match std::fs::remove_dir_all(&idx_full) {
                            Ok(()) => gnitz_debug!("recovery: removed orphan index dir {}", idx_full),
                            Err(e) => gnitz_debug!("recovery: failed to remove orphan index dir {}: {}", idx_full, e),
                        }
                    }
                    continue;
                }

                // Defense in depth: only `<something>_<digits>` dirs — the shape
                // of table (`t_<tid>`), view (`v_<vid>`), and the pre-flight's
                // throwaway root (`_preflight_<vid>`) — are eligible for removal.
                // Never touch an unexpected entry. Those three are the only
                // writers directly under a schema dir, so a matching name absent
                // from `live_tables` is orphaned either way.
                if !is_table_dir_name(&name) {
                    continue;
                }
                match std::fs::remove_dir_all(&full) {
                    Ok(()) => gnitz_debug!("recovery: removed orphan table/view dir {}", full),
                    Err(e) => gnitz_debug!("recovery: failed to remove orphan dir {}: {}", full, e),
                }
            }
        }

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
            let net: FxHashMap<u128, i64> = pk_signatures(&batch).iter().map(|s| (s.pk, s.sum)).collect();
            let (created, dropped): (Vec<u32>, Vec<u32>) =
                (0..batch.count as u32).partition(|&i| net[&batch.get_pk(i as usize)] >= 0);
            if dropped.is_empty() {
                undo_create.push((family, batch));
            } else if created.is_empty() {
                undo_drop.push((family, batch));
            } else {
                // Both index lists are ascending (a `partition` over an ascending
                // range), so each subset keeps the source's sorted/consolidated
                // tag — which the sign-preserving `map_weights` negation below
                // leaves in place.
                let schema = family.schema();
                undo_create.push((family, batch.ascending_subset(&created, &schema)));
                undo_drop.push((family, batch.ascending_subset(&dropped, &schema)));
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
        // are updated. The rollback gate in `submit` ensures any cascade that
        // calls back into `submit` also bypasses broadcasts.
        let result = self.with_rollback_compensation(|s| -> Result<(), String> {
            for (family, mut batch) in undo_create {
                batch.map_weights(i64::wrapping_neg);
                s.submit_local(family, batch)?;
            }
            Ok(())
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
