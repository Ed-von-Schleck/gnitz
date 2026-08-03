//! Multi-worker partition management (fork / trim / rehome / replication)
//! and the flushed-LSN bookkeeping that recovery and the DDL zone
//! allocator read.

use super::*;
use rustc_hash::FxHashSet;

impl CatalogEngine {
    // -- Partition management (for multi-worker fork) -------------------------

    /// Set active partition range for user tables.
    pub fn set_active_partitions(&mut self, start: u32, end: u32) {
        self.active_part_start = start;
        self.active_part_end = end;
    }

    /// True when this process owns a non-empty slice of the base-table tiling.
    /// The post-fork master owns none — it holds zero child `Table`s and stays
    /// inert — so anything reading local base data must check this first.
    pub(crate) fn owns_partitions(&self) -> bool {
        self.active_part_start != self.active_part_end
    }

    /// Close all partitions in user tables (master after fork). System tables
    /// hold Borrowed (non-partitioned) handles, so the filter is the handle.
    pub fn close_user_table_partitions(&mut self) {
        for entry in self.dag.tables.values() {
            if let Some(ptable) = entry.handle.as_partitioned_mut() {
                ptable.close_all_partitions();
            }
        }
    }

    /// Panic unless this is the pre-fork master with the full active range. Both
    /// boot passes below read each store's routing as the shape every worker will
    /// build, which only holds here: after the fork the master keeps an unhashed
    /// store's routing but holds no children, and reports every hashed store as an
    /// empty range.
    fn assert_pre_fork_full_range(&self, who: &str) {
        assert!(
            !crate::foundation::worker_ctx::is_worker() && self.active_part_end == NUM_PARTITIONS,
            "{who} must run pre-fork over the full active range",
        );
    }

    /// Trim worker partitions to assigned range. System tables hold Borrowed
    /// (non-partitioned) handles, so the filter is the handle.
    ///
    /// **Unhashed stores are exempt.** A `Routing::Unhashed` store
    /// holds its whole local dataset in one rank-stamped child, inherited across
    /// the fork at child index 0. Every worker but worker 0 owns a partition
    /// range excluding 0 and would otherwise drop it here, leaving the subsequent
    /// FLAG_PUSH replay to no-op into an empty `tables` vec — silent data loss
    /// after every reboot with W > 1.
    pub fn trim_worker_partitions(&mut self, start: u32, end: u32) {
        for entry in self.dag.tables.values() {
            if let Some(ptable) = entry.handle.as_partitioned_mut() {
                if ptable.is_unhashed() {
                    continue;
                }
                ptable.close_partitions_outside(start, end);
            }
        }
    }

    /// Re-home every inherited unhashed store to THIS worker's own
    /// `rep_{rank}` dir. The pre-fork master builds a replicated base table (or
    /// replicated-derived view) at `rep_0`; workers inherit that store across the
    /// fork. Because all workers share the data directory, leaving them at `rep_0`
    /// collides on flush (every worker writing the same shard files). The
    /// inherited store is empty — the master ingests no user data — so nothing is
    /// lost: the checkpointed shards under `rep_{rank}` (put there pre-fork by
    /// `reconcile_child_dirs` if this rank had no copy) load on open, and the
    /// subsequent FLAG_PUSH replay adds the SAL tail. Called post-fork after
    /// `set_active_partitions`, before the user-data replay. (The live CREATE path
    /// already builds the store at the worker's own rank, so it needs no re-home;
    /// this only repairs the recovery inherit.)
    pub fn rehome_unhashed_stores(&mut self) -> Result<(), String> {
        // Worker 0's re-home target is `rep_0`, where the inherited store already
        // lives. Skip the no-op reopen.
        if crate::foundation::worker_ctx::worker_rank() == 0 {
            return Ok(());
        }
        let tids: Vec<i64> = self
            .dag
            .tables
            .iter()
            .filter(|(_, e)| e.handle.is_unhashed())
            .map(|(&tid, _)| tid)
            .collect();
        for tid in tids {
            let (dir, schema, kind) = {
                let e = self.dag.tables.get(&tid).expect("tid taken from iter");
                (e.directory.clone(), e.schema, e.kind)
            };
            let pt = self
                .build_partitioned_storage(kind, &dir, "", tid, schema)
                .map_err(|e| format!("rehome unhashed store tid={tid}: {e}"))?;
            self.dag.tables.get_mut(&tid).expect("tid taken from iter").handle =
                StoreHandle::Partitioned(std::cell::UnsafeCell::new(Box::new(pt)));
        }
        Ok(())
    }

    /// Bring every live relation's child directories into this boot's worker
    /// count, and give every launched rank a copy of each replicated base table —
    /// the on-disk counterpart of `rehome_unhashed_stores`, which then
    /// opens what this leaves behind. Reclaiming before seeding is what lets the
    /// seed treat "has a manifest" as "current".
    ///
    /// Only a replicated **base table**'s children are copies of one another; a
    /// replicated-derived view's child is the slice that worker produced, so a
    /// missing one has to be re-derived, never copied.
    ///
    /// Master, pre-fork, asserted: the active range is still full here, so the
    /// stored routing is the shape every worker will build. After the fork an
    /// unhashed store's routing survives but the master holds no children, and
    /// a hashed store reports an empty range.
    ///
    /// Needs no recorded trigger: the manifest witnesses a complete child, so the
    /// pass is idempotent and converges after a crash at any point. A trigger of
    /// the form "the launched count differs from the recorded one" would be
    /// unsound — a boot that dies between this pass and `record_topology` leaves
    /// the recorded count unchanged, and rebooting at *that* count would skip a
    /// repair whose reclamation already ran.
    pub fn reconcile_child_dirs(&self, num_workers: u32) -> Result<(), String> {
        self.assert_pre_fork_full_range("reconcile_child_dirs");
        for entry in self.dag.tables.values() {
            // System tables are `Borrowed` single `Table`s with no children.
            let Some(pt) = entry.handle.as_partitioned_mut() else {
                continue;
            };
            reclaim_retired_children(&entry.directory, pt.routing(), num_workers);
            if entry.schema.placement().is_replicated() && entry.kind.is_base_table() {
                crate::storage::seed_missing_locals(&entry.directory, num_workers)
                    .map_err(|e| format!("seed replicated table {} copies: {e}", entry.directory))?;
            }
        }
        Ok(())
    }

    /// Reset an invalid view's output store and per-worker operator scratch to an
    /// empty, well-formed state, then drop its cached plan — recovery step-4, run
    /// per worker on its own owned partitions before the view is rebuilt.
    ///
    /// Unlinks every owned child's manifest first, so the empty rebuild below (a
    /// `RederiveCheckpointed` open) peeks `None` and *erases* the stale
    /// generation-`g` shards rather than reloading them — without which a
    /// transitively-invalid view whose own manifests are still at `g` would reload
    /// them. Then rebuilds the handle empty via `build_partitioned_storage` (same
    /// shape), removes this worker's scratch operator dirs, and invalidates the
    /// plan cache so the next backfill recompiles against the empty store + fresh
    /// scratch.
    pub(crate) fn reset_view_output_for_rebuild(&mut self, vid: i64) -> Result<(), String> {
        let (dir, schema, kind, routing) = {
            let entry = self
                .dag
                .tables
                .get(&vid)
                .ok_or_else(|| format!("reset_view_output_for_rebuild: view {vid} not registered"))?;
            let routing = entry
                .handle
                .as_partitioned_mut()
                .map(|pt| pt.routing())
                .ok_or_else(|| format!("reset_view_output_for_rebuild: view {vid} has no partitioned store"))?;
            (entry.directory.clone(), entry.schema, entry.kind, routing)
        };

        for child in routing.children() {
            let _ = std::fs::remove_file(child.manifest(&dir));
        }

        // Rebuild empty (same shape — the schema's placement is what chose the
        // routing above). Each child's `Table::new` erases the stale shards
        // (manifest now absent → `RederiveCheckpointed` peek `None`).
        let pt = self.build_partitioned_storage(kind, &dir, "", vid, schema)?;
        self.dag.tables.get_mut(&vid).expect("view present").handle =
            StoreHandle::Partitioned(std::cell::UnsafeCell::new(Box::new(pt)));

        // Remove this worker's per-view operator scratch dirs (rank-stamped).
        let rank = crate::foundation::worker_ctx::worker_rank();
        for name in subdir_names(&dir) {
            if matches!(ChildAddr::parse(&name), Some(ChildAddr::Scratch { rank: r, .. }) if r == rank) {
                let _ = std::fs::remove_dir_all(format!("{dir}/{name}"));
            }
        }

        // Drop the cached plan so the next backfill recompiles against the empty
        // store + fresh scratch.
        self.dag.invalidate(vid);
        Ok(())
    }

    /// True iff a read of relation `id` must be **single-sourced** because its
    /// output is replicated — a full identical copy lives on every worker. Every
    /// relation kind carries the answer as the `Placement` stamped on its schema
    /// (see `DagEngine::view_placement` for how a view folds its sources'), so
    /// this is one lookup. Consulted by the scan dispatch so a replicated relation
    /// is read from one worker instead of gathering N identical copies. SEEK
    /// already unicasts to one worker, so it needs no equivalent check.
    pub fn relation_output_is_replicated(&self, id: i64) -> bool {
        self.dag.relation_is_replicated(id)
    }

    /// Invalidate all cached plans.
    pub fn invalidate_all_plans(&mut self) {
        self.dag.invalidate_all();
    }

    /// Get max flushed LSN for a table. Recovery itself reads the bulk map
    /// from `collect_all_flushed_lsns`; this single-table form is test-only.
    #[cfg(test)]
    pub(crate) fn get_max_flushed_lsn(&self, table_id: i64) -> u64 {
        if table_id > 0 && table_id < FIRST_USER_TABLE_ID {
            return self.sys_table_current_lsn(table_id);
        }
        let entry = match self.dag.tables.get(&table_id) {
            Some(e) => e,
            None => return 0,
        };
        entry.handle.current_lsn()
    }

    /// Read `current_lsn` from a system table by id. Returns 0 for unknown
    /// ids.
    #[cfg(test)]
    fn sys_table_current_lsn(&self, table_id: i64) -> u64 {
        self.sys_table(table_id).map_or(0, |t| t.current_lsn())
    }

    /// Build a map of every known table id → max flushed LSN, covering
    /// both system tables and user tables. Recovery uses this as the
    /// dedup filter for the unified two-pass walk.
    pub fn collect_all_flushed_lsns(&self) -> std::collections::HashMap<i64, u64> {
        let mut map = std::collections::HashMap::new();
        for (info, table) in SYS_FAMILIES.iter().zip(&self.sys_stores) {
            map.insert(info.id, table.current_lsn());
        }
        for (&tid, entry) in self.dag.tables.iter() {
            if tid >= FIRST_USER_TABLE_ID {
                // recovery_lsn (min across partitions), not current_lsn (max):
                // a partial family flush must not over-skip a lagging
                // partition's still-in-SAL rows. The allocator below keeps the
                // max via max_table_current_lsn for its upper-bound needs.
                map.insert(tid, entry.handle.recovery_lsn());
            }
        }
        map
    }

    /// Compute the set of view ids whose checkpointed output state must be
    /// rejected at boot and rebuilt, rather than resumed from its manifests.
    ///
    /// A view is **valid** (resumed) iff:
    ///   * the recorded topology matches the launched `(worker_count, STATE_FORMAT)`
    ///     — a different worker count re-shapes every hashed store's partition map;
    ///   * every one of its output-store partition manifests is stamped with the
    ///     committed checkpoint generation — `worker_ctx::committed_generation()`,
    ///     the in-memory recovered `G`, NOT the recovery-start-bumped durable
    ///     `G+1` — matching what `Table::new`'s conditional load peeks; and
    ///   * every VIEW it scans is itself valid — else it could read a rebuilt
    ///     sibling's freshly-emptied output store.
    ///
    /// Two phases. Phase 1 decides each view's **local** validity (topology +
    /// output manifests). Phase 2 propagates invalidity to any view scanning an
    /// invalid source, walking the views in dependency order so one pass reaches
    /// the whole cascade.
    ///
    /// Output manifests are enumerated by **store shape**: an unhashed store has
    /// one child per launched worker, homed at that worker's rank; a hashed store
    /// spreads over all 256 partitions. The two grammars are disjoint, so a view
    /// whose shape flipped since its checkpoint finds no manifest at all and is
    /// rebuilt.
    pub fn compute_invalid_views(&mut self, launched_workers: u32) -> FxHashSet<i64> {
        self.assert_pre_fork_full_range("compute_invalid_views");
        let g = crate::foundation::worker_ctx::committed_generation();
        let topo_value = crate::storage::topology_word(launched_workers);
        let topo_valid = self.recorded_topology == topo_value;

        let view_ids = self.dag.view_ids();

        // Phase 1: local validity (topology + every output-partition manifest at g).
        let mut invalid: FxHashSet<i64> = FxHashSet::default();
        for &vid in &view_ids {
            let local_ok = topo_valid && {
                let entry = self.dag.tables.get(&vid).expect("vid taken from tables iter");
                let dir = &entry.directory;
                let at_g = |child: ChildAddr| match std::ffi::CString::new(child.manifest(dir)) {
                    Ok(c) => matches!(crate::storage::peek_generation(&c), Ok(Some(mg)) if mg == g),
                    Err(_) => false,
                };
                // The whole cluster's children, not just this process's.
                entry.handle.cluster_children(launched_workers).all(at_g)
            };
            if !local_ok {
                invalid.insert(vid);
            }
        }
        if invalid.is_empty() {
            // Clean same-topology restart: nothing to propagate, skip the
            // dependency-map reads below.
            return invalid;
        }

        // Phase 2: propagate invalidity to any still-valid view that scans an
        // invalid source. Base sources never enter `invalid`, so they pass. A
        // source view precedes every view scanning it in `order_by_view_deps`,
        // so a single pass carries invalidity down the whole chain.
        for vid in self.dag.order_by_view_deps(&view_ids) {
            if !invalid.contains(&vid) && self.dag.get_source_ids(vid).iter().any(|s| invalid.contains(s)) {
                invalid.insert(vid);
            }
        }
        invalid
    }

    /// Maximum `current_lsn` across all tables — system and user. The
    /// executor seeds its zone-LSN allocator from this at boot and passes it
    /// as the reservation floor per DDL, so every allocated zone LSN is
    /// strictly greater than each table's current counter: no recovery
    /// watermark a checkpoint persisted can cover a committed-but-unflushed
    /// zone, and a failed zone's pinned LSN is never reused.
    pub fn max_table_current_lsn(&self) -> u64 {
        let mut max_lsn = 0u64;
        for table in &self.sys_stores {
            max_lsn = max_lsn.max(table.current_lsn());
        }
        for entry in self.dag.tables.values() {
            max_lsn = max_lsn.max(entry.handle.current_lsn());
        }
        max_lsn
    }
}
