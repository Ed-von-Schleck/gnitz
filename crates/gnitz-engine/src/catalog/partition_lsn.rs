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

    /// Close all partitions in user tables (master after fork). System tables
    /// hold Borrowed (non-partitioned) handles, so the filter is the handle.
    pub fn close_user_table_partitions(&mut self) {
        for entry in self.dag.tables.values() {
            if let Some(ptable) = entry.handle.as_partitioned_mut() {
                ptable.close_all_partitions();
            }
        }
    }

    /// Trim worker partitions to assigned range. System tables hold Borrowed
    /// (non-partitioned) handles, so the filter is the handle.
    ///
    /// **Single-partition stores are exempt.** A `Routing::Replicated` store
    /// (a replicated base table's full copy, or a replicated-derived view's local
    /// slice) holds its whole local dataset at partition 0, built across the fork
    /// at `[0, 1)`. A worker whose range excludes 0 (every worker but worker 0)
    /// would otherwise drop it here, and the subsequent FLAG_PUSH replay would
    /// no-op into an empty `tables` vec — silent data loss after every reboot with
    /// W > 1. Exempting them keeps the copy on every worker, which is the whole
    /// point of replication.
    pub fn trim_worker_partitions(&mut self, start: u32, end: u32) {
        for entry in self.dag.tables.values() {
            if let Some(ptable) = entry.handle.as_partitioned_mut() {
                if ptable.is_replicated() {
                    continue;
                }
                ptable.close_partitions_outside(start, end);
            }
        }
    }

    /// Re-home every inherited single-partition store to THIS worker's own
    /// partition index `part_start`. The pre-fork master builds a replicated base
    /// table (or replicated-derived view) at `part_0`; workers inherit that
    /// `part_0` store across the fork. Because all workers share the data
    /// directory, leaving them at `part_0` collides on flush (every worker writing
    /// the same shard files). Each worker therefore rebuilds the store at
    /// `part_{part_start}`. The inherited store is empty — the master ingests no
    /// user data — so nothing is lost; the subsequent FLAG_PUSH replay fills the
    /// re-homed store. Called post-fork after `set_active_partitions`, before the
    /// user-data replay. (The live CREATE path already builds the store at the
    /// worker's own `part_start`, so it needs no re-home; this only repairs the
    /// recovery inherit.)
    pub fn rehome_single_partition_stores(&mut self, part_start: u32) -> Result<(), String> {
        // Worker 0's `part_start` is 0 — the inherited store already lives at
        // `part_0`, so its re-home target is its current home. Skip the no-op
        // rebuild (and the empty-store allocation it would do).
        if part_start == 0 {
            return Ok(());
        }
        let tids: Vec<i64> = self
            .dag
            .tables
            .iter()
            .filter(|(_, e)| e.handle.is_replicated())
            .map(|(&tid, _)| tid)
            .collect();
        for tid in tids {
            let entry = self.dag.tables.get_mut(&tid).expect("tid taken from iter");
            let pt = PartitionedTable::new(
                &entry.directory,
                entry.schema,
                tid as u32,
                Routing::Replicated,
                entry.kind.recovery_source(),
                part_start,
                part_start + 1,
            )
            .map_err(|e| format!("rehome single-partition store tid={tid}: {e:?}"))?;
            entry.handle = StoreHandle::Partitioned(std::cell::UnsafeCell::new(Box::new(pt)));
        }
        Ok(())
    }

    /// Reset an invalid view's output store and per-worker operator scratch to an
    /// empty, well-formed state, then drop its cached plan — recovery step-4, run
    /// per worker on its own owned partitions before the view is rebuilt.
    ///
    /// Unlinks every owned partition's `manifest.bin` first, so the empty rebuild
    /// below (a `RederiveCheckpointed` open) peeks `None` and *erases* the stale
    /// generation-`g` shards rather than reloading them — load-bearing for a
    /// transitively-invalid view whose own manifests are still at `g`. Then
    /// rebuilds the handle empty via `build_partitioned_storage` (same shape:
    /// replicated → one child at `part_start`, else the hashed active range),
    /// removes this worker's `scratch_*_w{rank}` operator dirs, and invalidates the
    /// plan cache so the next backfill recompiles against the empty store + fresh
    /// scratch.
    pub(crate) fn reset_view_output_for_rebuild(&mut self, vid: i64) -> Result<(), String> {
        let (dir, schema, kind, replicated) = {
            let entry = self
                .dag
                .tables
                .get(&vid)
                .ok_or_else(|| format!("reset_view_output_for_rebuild: view {vid} not registered"))?;
            (
                entry.directory.clone(),
                entry.schema,
                entry.kind,
                entry.handle.is_replicated(),
            )
        };

        // Owned partition indices on THIS worker: one child at `part_start` for a
        // replicated store, else the full active range.
        let (p_start, p_end) = if replicated {
            (self.active_part_start, self.active_part_start + 1)
        } else {
            (self.active_part_start, self.active_part_end)
        };
        for p in p_start..p_end {
            let _ = std::fs::remove_file(partition_manifest_path(&dir, p));
        }

        // Rebuild empty (same shape). Each partition's `Table::new` erases the
        // stale shards (manifest now absent → `RederiveCheckpointed` peek `None`).
        let pt = self.build_partitioned_storage(kind, &dir, "", vid, schema, replicated)?;
        self.dag.tables.get_mut(&vid).expect("view present").handle =
            StoreHandle::Partitioned(std::cell::UnsafeCell::new(Box::new(pt)));

        // Remove this worker's per-view operator scratch dirs (rank-stamped).
        for name in subdir_names(&dir) {
            if crate::query::is_worker_scratch_dir_name(&name) {
                let _ = std::fs::remove_dir_all(format!("{dir}/{name}"));
            }
        }

        // Drop the cached plan so the next backfill recompiles against the empty
        // store + fresh scratch.
        self.dag.invalidate(vid);
        Ok(())
    }

    /// True iff a read of relation `id` (base table or view) must be
    /// **single-sourced** because its output is replicated — a full identical
    /// copy lives on every worker. A base table carries the flag on its schema;
    /// a view is replicated iff all its sources are (design §4.2). Consulted by
    /// the scan dispatch so a replicated relation is read from one worker instead
    /// of gathering N identical copies. SEEK already unicasts to one worker, so it
    /// needs no equivalent check.
    pub fn relation_output_is_replicated(&mut self, id: i64) -> bool {
        match self.dag.tables.get(&id) {
            Some(e) if e.kind.is_base_table() => return e.schema.replicated(),
            Some(_) => {} // view — fall through (releases the `tables` borrow)
            None => return false,
        }
        self.dag.view_all_sources_replicated(id)
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
    ///   * every VIEW it scans (ScanDelta cascade dep OR ScanTrace static
    ///     `ext_trace` read) is itself valid — else it could read a rebuilt
    ///     sibling's freshly-emptied output store.
    ///
    /// Two phases. Phase 1 decides each view's **local** validity (topology +
    /// output manifests). Phase 2 propagates invalidity to any view scanning an
    /// invalid source, iterating to a fixpoint — robust to arbitrary source
    /// orderings, including a `ScanTrace`-of-view (not a cascade dependency, so its
    /// `depth` need not sit below its reader's).
    ///
    /// Output-partition manifests are enumerated by **store shape**: a replicated
    /// store (one child per worker, homed at `part_{worker.part_start}`) has one
    /// manifest per launched worker; a hashed store spreads over all 256 partitions.
    pub fn compute_invalid_views(&mut self, launched_workers: u32) -> FxHashSet<i64> {
        let g = crate::foundation::worker_ctx::committed_generation();
        let topo_value = ((launched_workers as u64) << 32) | crate::storage::STATE_FORMAT as u64;
        let topo_valid = self.recorded_topology == topo_value;

        let view_ids: Vec<i64> = self
            .dag
            .tables
            .iter()
            .filter(|(_, e)| e.kind == RelationKind::View)
            .map(|(&vid, _)| vid)
            .collect();

        // Phase 1: local validity (topology + every output-partition manifest at g).
        let mut invalid: FxHashSet<i64> = FxHashSet::default();
        for &vid in &view_ids {
            let local_ok = topo_valid && {
                let entry = self.dag.tables.get(&vid).expect("vid taken from tables iter");
                let dir = &entry.directory;
                let at_g = |p: u32| match std::ffi::CString::new(partition_manifest_path(dir, p)) {
                    Ok(c) => matches!(crate::storage::peek_generation(&c), Ok(Some(mg)) if mg == g),
                    Err(_) => false,
                };
                if entry.handle.is_replicated() {
                    // One child per worker, homed at its range start.
                    (0..launched_workers)
                        .map(|w| partition_range(w, launched_workers).0)
                        .all(at_g)
                } else {
                    (0..NUM_PARTITIONS).all(at_g)
                }
            };
            if !local_ok {
                invalid.insert(vid);
            }
        }
        if invalid.is_empty() {
            // Clean same-topology restart: nothing to propagate, skip the
            // meta-circuit loads below.
            return invalid;
        }

        // Phase 2: propagate invalidity to any still-valid view that scans an
        // invalid source (ScanDelta or ScanTrace). Base sources never enter
        // `invalid`, so they pass. Fixpoint over the (finite) view set; each
        // view's scan sources are loaded once up front (`all_scan_source_ids`
        // reads the meta circuit off the system tables).
        let scan_sources: Vec<(i64, Vec<i64>)> = view_ids
            .iter()
            .filter(|vid| !invalid.contains(vid))
            .map(|&vid| (vid, self.dag.all_scan_source_ids(vid)))
            .collect();
        loop {
            let mut changed = false;
            for (vid, sources) in &scan_sources {
                if !invalid.contains(vid) && sources.iter().any(|s| invalid.contains(s)) {
                    invalid.insert(*vid);
                    changed = true;
                }
            }
            if !changed {
                break;
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
