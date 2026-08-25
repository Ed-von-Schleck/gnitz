//! Per-worker store lifecycle across the fork — detach, re-home, child-dir
//! reclamation, invalid-view reset — and the flushed-LSN bookkeeping that
//! recovery and the DDL zone allocator read.

use super::*;
use rustc_hash::FxHashSet;

impl CatalogEngine {
    // -- Store management (for multi-worker fork) -----------------------------

    /// Detach every user relation's store (master after fork), so master and
    /// worker 0 do not both hold a live `Table` on `w0of{W}`: two processes
    /// writing one directory is the hazard `naming.rs` exists to prevent. System
    /// tables keep their `Borrowed` handles, so the filter is the handle.
    pub fn detach_user_stores(&mut self) {
        self.owns_stores = false;
        for entry in self.dag.tables.values_mut() {
            if !matches!(entry.handle, StoreHandle::Borrowed(_)) {
                entry.handle = StoreHandle::Detached;
            }
            // A fed view's delta store goes too. The pre-fork master builds one at
            // `ChildAddr::Delta { rank: 0 }` during boot replay, and worker 0 homes
            // its own there — so leaving it would have two processes holding a live
            // `Table` on one directory, which is what this whole function prevents.
            entry.delta = None;
        }
    }

    /// Panic unless this is the pre-fork master. Both boot passes below reach
    /// across the whole cluster — one reclaims every relation's retired children,
    /// the other peeks every launched rank's manifest — which only that one
    /// process may do: the workers do not exist yet and the post-fork master owns
    /// no store to speak for them.
    fn assert_pre_fork(&self, who: &str) {
        assert!(
            !crate::foundation::worker_ctx::is_worker() && self.owns_stores,
            "{who} must run pre-fork on the master, which still owns its stores",
        );
    }

    /// Re-open every store this worker inherited on someone else's child.
    ///
    /// The pre-fork master builds every relation at rank 0 and every worker
    /// inherits those handles; since all workers share the data directory,
    /// leaving them there would have every worker flush the same shard files.
    /// The inherited handle is empty — the master ingests no user data — so
    /// re-opening loses nothing: this rank's checkpointed shards load from its
    /// own child and the FLAG_PUSH replay adds the SAL tail.
    ///
    /// A handle already homed here is left alone rather than special-cased by
    /// rank: that covers worker 0's inherited child and the live CREATE path,
    /// which builds at the worker's own rank to begin with. Re-opening one would
    /// briefly put two live `Table`s on one directory.
    pub fn rehome_stores(&mut self) -> Result<(), String> {
        let home = ChildAddr::this_worker(self.num_workers);
        let tids: Vec<i64> = self
            .dag
            .tables
            .iter()
            .filter(|(_, e)| {
                e.handle
                    .as_owned()
                    .is_some_and(|t| t.directory() != home.dir(&e.directory))
            })
            .map(|(&tid, _)| tid)
            .collect();
        for tid in tids {
            self.rebuild_relation_store(tid, "rehome store")?;
        }
        Ok(())
    }

    /// Rebuild `tid`'s store handle from its registered `(directory, schema,
    /// kind)` and install it. `build_relation_store` opens this worker's own
    /// child, so the rebuilt handle is homed wherever the caller now runs. The
    /// caller does whatever on-disk preparation its case needs first.
    ///
    /// Both stores are rebuilt, and both are installed: a fed view whose delta
    /// store did not come back here would still carry its `delta_bytes` in the
    /// catalog and have no delta store on any worker, on every boot, with no error
    /// anywhere.
    fn rebuild_relation_store(&mut self, tid: i64, what: &str) -> Result<(), String> {
        let (dir, schema, kind, budgets) = {
            let e = self
                .dag
                .tables
                .get(&tid)
                .ok_or_else(|| format!("{what}: relation {tid} not registered"))?;
            (e.directory.clone(), e.schema, e.kind, e.budgets())
        };
        let stores = self
            .build_relation_store(kind, &dir, tid, schema, budgets)
            .map_err(|e| format!("{what} tid={tid}: {e}"))?;
        let entry = self.dag.tables.get_mut(&tid).expect("entry read above");
        entry.handle = stores.handle;
        entry.delta = stores.delta;
        Ok(())
    }

    /// Reclaim every live relation's child directories that this boot's worker
    /// count no longer owns — the on-disk counterpart of `rehome_stores`, which
    /// then opens what this leaves behind. Runs after the boot relayout, so what
    /// it deletes is a set the relayout has already consumed.
    ///
    /// Unconditional rather than triggered on "the launched count changed": a
    /// boot that dies between this sweep and `record_topology` leaves the
    /// recorded count unchanged, so such a trigger would skip the repair on the
    /// retry. It is idempotent, so running it every boot converges instead.
    pub fn reconcile_child_dirs(&self) {
        self.assert_pre_fork("reconcile_child_dirs");
        for entry in self.dag.tables.values() {
            // System tables are `Borrowed` single `Table`s with no children.
            if matches!(entry.handle, StoreHandle::Borrowed(_)) {
                continue;
            }
            // A storeless relation's `directory` names a path that was never created;
            // `reclaim_retired_children` reads it as having no children and returns.
            reclaim_retired_children(&entry.directory, self.num_workers);
        }
    }

    /// Reset an invalid view's output store and per-worker operator scratch to an
    /// empty, well-formed state, then drop its cached plan — recovery step-4, run
    /// per worker on its own store before the view is rebuilt.
    ///
    /// Unlinks this worker's child manifest first, so the empty rebuild below (a
    /// `Rederive` open) peeks `None` and *erases* the stale
    /// generation-`g` shards rather than reloading them — without which a
    /// transitively-invalid view whose own manifests are still at `g` would reload
    /// them. Then rebuilds the handle empty via `build_relation_store`,
    /// removes this worker's scratch operator dirs, and invalidates the
    /// plan cache so the next backfill recompiles against the empty store + fresh
    /// scratch.
    pub fn reset_view_output_for_rebuild(&mut self, vid: i64) -> Result<(), String> {
        let dir = self
            .dag
            .tables
            .get(&vid)
            .ok_or_else(|| format!("reset_view_output_for_rebuild: view {vid} not registered"))?
            .directory
            .clone();

        let rank = crate::foundation::worker_ctx::worker_rank();
        let _ = std::fs::remove_file(ChildAddr::this_worker(self.num_workers).manifest(&dir));

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

        // Drop the cached plan so the next backfill recompiles against the empty
        // store + fresh scratch.
        self.dag.invalidate(vid);
        Ok(())
    }

    /// Invalidate all cached plans.
    pub fn invalidate_all_plans(&mut self) {
        self.dag.invalidate_all();
    }

    /// Get max flushed LSN for a table. Recovery itself reads the bulk map
    /// from `collect_all_flushed_lsns`; this single-table form is test-only.
    #[cfg(test)]
    pub(crate) fn get_max_flushed_lsn(&self, table_id: i64) -> u64 {
        self.dag.tables.get(&table_id).map_or(0, |e| e.handle.current_lsn())
    }

    /// Every registered relation's `(table id, current_lsn)` — the one walk
    /// behind both the recovery dedup map and the zone-allocator floor. A system
    /// family's registry handle is a `Borrowed` re-export of its `sys_stores`
    /// box, so this reads the same counter its own store would report.
    fn all_store_lsns(&self) -> impl Iterator<Item = (i64, u64)> + '_ {
        self.dag
            .tables
            .iter()
            .map(|(&tid, entry)| (tid, entry.handle.current_lsn()))
    }

    /// Build a map of every known table id → max flushed LSN, covering
    /// both system tables and user tables. Recovery uses this as the
    /// dedup filter for the unified two-pass walk.
    pub fn collect_all_flushed_lsns(&self) -> std::collections::HashMap<i64, u64> {
        self.all_store_lsns().collect()
    }

    /// Compute the set of view ids whose checkpointed state — output stores and
    /// operator traces alike — must be rejected at boot and rebuilt, rather than
    /// resumed from its manifests.
    ///
    /// A view is **valid** (resumed) iff:
    ///   * the recorded topology matches the launched `(worker_count, STATE_FORMAT)`
    ///     — a different worker count re-shapes every keyed store's row placement;
    ///   * every child that carries its state — its output-store children and its
    ///     owned operator scratch — is stamped with the committed checkpoint
    ///     generation — `resume_generation`, the in-memory recovered `G`, NOT the
    ///     recovery-start-bumped durable `G+1` — matching what `Table::new`'s
    ///     conditional load peeks; and
    ///   * every VIEW it scans is itself valid — else it could read a rebuilt
    ///     sibling's freshly-emptied output store; and
    ///   * none of its sources is a STREAM — a stream-fed view's manifests are
    ///     published normally, so without this it would resume at the committed
    ///     generation onto state whose stream inputs are gone.
    ///
    /// The topology word is decided once for the whole set. Then phase 1 decides
    /// each view's **local** validity (direct sources + child manifests), and
    /// phase 2 propagates invalidity to any view scanning an invalid source,
    /// walking the views in dependency order so one pass reaches the whole cascade.
    ///
    /// Which children carry that state is [`state_child_manifests`].
    /// Reading the operator scratch alongside the output stores is what rejects an
    /// output store one generation ahead of the integral beneath it — the ephemeral
    /// round stamps every output store but only a *compiled* view's traces.
    pub fn compute_invalid_views(&mut self) {
        self.assert_pre_fork("compute_invalid_views");
        let launched_workers = self.num_workers;
        // The same field `rederive_source` reads for the store-open half of the
        // verdict, so both halves answer from one value.
        let g = self.resume_generation;
        let topo_valid = self.topology_matches();

        let view_ids = self.dag.view_ids();
        // A worker-count or STATE_FORMAT change re-shapes every keyed store, so no
        // view resumes and nothing below need be read.
        if !topo_valid {
            self.invalid_views = view_ids.into_iter().collect();
            return;
        }

        // Phase 1: local validity (no direct stream source + every output child's
        // manifest at g). A *transitive* stream source needs no walk here: phase 2
        // propagates invalidity down every dependency chain. The source test comes
        // first, so it short-circuits the per-child manifest reads.
        // An unpeekable manifest reads as a mismatch, which is the verdict a child
        // whose manifest a previous open erased must get: its siblings may still
        // be at `g`.
        let at_g = |m: String| match std::ffi::CString::new(m) {
            Ok(c) => matches!(peek_header(&c), Ok(Some(h)) if h.checkpoint_gen == g),
            Err(_) => false,
        };

        let mut invalid: FxHashSet<i64> = FxHashSet::default();
        for &vid in &view_ids {
            let stream_fed = self
                .dag
                .get_source_ids(vid)
                .iter()
                .any(|s| self.dag.relation_kind(*s) == Some(RelationKind::Stream));
            if stream_fed {
                invalid.insert(vid);
                continue;
            }
            let dir = &self.dag.tables.get(&vid).expect("vid taken from tables iter").directory;
            if !state_child_manifests(dir, launched_workers).into_iter().all(at_g) {
                invalid.insert(vid);
            }
        }
        if invalid.is_empty() {
            // Clean restart: nothing to propagate, so skip the ordering walk below.
            self.invalid_views = invalid;
            return;
        }

        // Phase 2: propagate invalidity to any still-valid view that scans an
        // invalid source. Base and stream sources never enter `invalid`, so they
        // pass — a stream's own dependents were caught in phase 1 instead. A
        // source view precedes every view scanning it in `order_by_view_deps`,
        // so a single pass carries invalidity down the whole chain.
        for vid in self.dag.order_by_view_deps(&view_ids) {
            if !invalid.contains(&vid) && self.dag.get_source_ids(vid).iter().any(|s| invalid.contains(s)) {
                invalid.insert(vid);
            }
        }
        self.invalid_views = invalid;
    }

    /// Maximum `current_lsn` across all tables — system and user. The
    /// executor seeds its zone-LSN allocator from this at boot and passes it
    /// as the reservation floor per DDL, so every allocated zone LSN is
    /// strictly greater than each table's current counter: no recovery
    /// watermark a checkpoint persisted can cover a committed-but-unflushed
    /// zone, and a failed zone's pinned LSN is never reused.
    pub fn max_table_current_lsn(&self) -> u64 {
        self.all_store_lsns().map(|(_, lsn)| lsn).max().unwrap_or(0)
    }
}
