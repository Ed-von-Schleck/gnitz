use super::*;

impl CatalogEngine {
    // -- Hook processing ---------------------------------------------------
    //
    // Dispatch is a static per-system-table sequence. The order of calls is
    // the dependency order; previous revisions stored this order in a system
    // table (`sys_catalog_caches`) and rebuilt the schedule from PK sort
    // order at startup, which produced a silent divergence between fresh
    // install and recovery (see git history).
    //
    // Two categories of handler are dispatched from here:
    //
    //   * `apply_*`  — pure cache-delta appliers. Each row's weight drives
    //     a HashMap/HashSet insert (+1) or remove (-1). Naturally idempotent
    //     under retract+insert because HashMap ops commute with weight sign.
    //   * `hook_*`   — side-effectful, edge-triggered handlers that build
    //     directories, allocate partitions, register DAG entries, or
    //     backfill derived state. These are NOT idempotent across a
    //     retract+insert pair on the same row; today's DDL surface never
    //     produces such a pair for sys_tables / sys_views / sys_indices,
    //     so edge-triggering is safe. If that ever changes (e.g. an ALTER
    //     path that rewrites a row via -1,+1), these handlers need to be
    //     revisited.
    //
    // Cross-sys-table ordering contract (required by `hook_table_register`
    // and `hook_view_register`, which read sys_columns via
    // `read_column_defs`):
    //
    //   COL_TAB writes MUST precede TABLE_TAB / VIEW_TAB writes.
    //
    // Where this is enforced:
    //   * Live DDL: every catalog write arrives as one `FLAG_DDL_TXN` bundle,
    //     and the server ingest loop (`handle_ddl_txn`) applies the bundle's
    //     families in ascending `catalog_topo_priority` order under one zone —
    //     COL_TAB(1) before the TABLE_TAB(6)/VIEW_TAB(7) register hook that reads
    //     it. The client send order is irrelevant; the server sorts.
    //   * Wire: the SAL is a single FIFO; worker `FLAG_DDL_SYNC` dispatch
    //     preserves master broadcast order.
    //   * Replay (`bootstrap.rs::replay_catalog`): TABLE_TAB is replayed
    //     before COL_TAB, but `read_column_defs` reads sys_columns storage
    //     directly (loaded at open time), not the cache, so the ordering
    //     holds.
    //
    // Retraction dependencies to be aware of:
    //   * apply_entity_by_qname reads entity_by_id on retract → must fire before apply_entity_by_id.

    pub(crate) fn fire_hooks(&mut self, family: SysFamily, batch: &Batch) -> Result<(), String> {
        // Exhaustive over `SysFamily`: a newly-added family is a compile error
        // here, not a silently-skipped `_` arm. The no-op families
        // (Sequence/Circuit*) are named, not swallowed by a wildcard.
        match family {
            SysFamily::Schema => {
                self.apply_schema_by_name(batch)?;
                self.apply_schema_by_id(batch)?;
                self.hook_schema_dir(batch)?;
            }
            SysFamily::Table => {
                self.apply_entity_by_qname(batch)?;
                self.apply_entity_by_id(batch)?;
                self.apply_schema_of(TABLE_TAB_ID, batch)?;
                self.apply_pk_col_of(TABLE_TAB_ID, batch)?;
                self.hook_table_register(batch)?;
                self.apply_needs_lock(TABLE_TAB_ID, batch)?;
                self.hook_cascade_fk(batch)?;
            }
            SysFamily::View => {
                self.apply_entity_by_qname(batch)?;
                self.apply_entity_by_id(batch)?;
                self.apply_schema_of(VIEW_TAB_ID, batch)?;
                self.apply_pk_col_of(VIEW_TAB_ID, batch)?;
                self.hook_view_register(batch)?;
            }
            SysFamily::Column => {
                self.apply_col_names_invalidate(batch)?;
                self.apply_fk_constraints(batch)?;
                self.apply_needs_lock(COL_TAB_ID, batch)?;
            }
            SysFamily::Index => {
                self.apply_index_by_name(batch)?;
                self.apply_index_by_id(batch)?;
                self.hook_index_register(batch)?;
                self.apply_needs_lock(IDX_TAB_ID, batch)?;
            }
            SysFamily::ViewDep => {
                self.dag.invalidate_dep_map();
            }
            // Restore the user-sequence high-water from a durably-committed or
            // SAL-replayed advance so a committed SERIAL id is never re-issued.
            SysFamily::Sequence => {
                self.hook_sequence_register(batch)?;
            }
            // No cache/side-effect reactions: the circuit graph is loaded by
            // `load_circuit`, not by hooks.
            SysFamily::CircuitNodes | SysFamily::CircuitEdges | SysFamily::CircuitNodeColumns => {}
        }
        Ok(())
    }

    /// Fold a `sys_sequences` advance into the in-memory `user_sequences` map.
    /// Fires via `submit` (live durable range advances and SAL replay at
    /// recovery). Catalog sequences (`seq_id < FIRST_USER_TABLE_ID`) recover via
    /// the object-id hooks instead, so they are skipped — the guard is defensive
    /// since `advance_sequence` bypasses `submit` and never reaches here.
    fn hook_sequence_register(&mut self, batch: &Batch) -> Result<(), String> {
        for i in 0..batch.count {
            if batch.get_weight(i) <= 0 {
                continue;
            }
            let seq_id = batch.get_pk(i) as i64;
            let hw = self.read_batch_u64(batch, i, SEQTAB_PAY_VALUE) as i64;
            self.observe_user_sequence(seq_id, hw);
        }
        Ok(())
    }

    // -- Hook handlers ---------------------------------------------------------

    fn hook_schema_dir(&mut self, batch: &Batch) -> Result<(), String> {
        for i in 0..batch.count {
            let weight = batch.get_weight(i);
            let name = self.read_batch_string(batch, i, 0);
            let path = schema_dir(&self.base_dir, &name);
            if weight > 0 {
                // A prior DROP SCHEMA may have queued this exact (name-based)
                // path for checkpoint-gated removal; recreating it now must
                // cancel that, or the gating checkpoint would wipe the new
                // schema and its tables.
                self.cancel_gated_deletion(&path);
                // Pre-stage for cleanup before ensure_dir so that if Stage-A
                // fails after the directory is created, compensate_stage_a's
                // drain_pending_dir_deletions removes it.
                let cleanup_idx = self.pending_dir_deletions.len();
                self.pending_dir_deletions.push(path.clone());
                ensure_dir(&path)?;
                // Success: remove the pre-staged entry.
                self.pending_dir_deletions.truncate(cleanup_idx);
                fsync_dir(&path);
                fsync_dir(&self.base_dir);
            } else {
                // Queue for deletion on successful DROP SCHEMA and on CREATE SCHEMA
                // rollback when compensate_stage_a re-fires this hook with -1.
                self.pending_dir_deletions.push(path);
            }
        }
        Ok(())
    }

    /// Build the partitioned storage for a top-level relation. Durability and
    /// Pk-unique tagging are derived from `kind`, so a relation cannot be
    /// (e.g.) ephemeral-but-Pk-tagged. Only user relations are built here
    /// (system catalog tables are plain single `Table`s built at bootstrap),
    /// so the partition count is always `NUM_PARTITIONS`.
    /// The pre-stage/truncate crash-cleanup that wraps the call in
    /// `hook_table_register`/`hook_view_register` (`pending_dir_deletions` push then
    /// truncate) is deliberately left at each call site: this helper takes `&self`
    /// and is pure construction, so callers own crash-cleanup. Hoisting it would need
    /// `&mut self` and still would not unify with the index path, whose cleanup also
    /// wraps `backfill_index`.
    fn build_partitioned_storage(
        &self,
        kind: RelationKind,
        directory: &str,
        name: &str,
        id: i64,
        schema: SchemaDescriptor,
        single_partition: bool,
    ) -> Result<PartitionedTable, String> {
        // A `single_partition` relation is a `Routing::Replicated` store on every
        // node with a non-empty active range. It is either a
        // replicated base table (full copy on every worker) or a replicated-derived
        // view (the local slice each worker produces from a join/reduce against a
        // replicated source — the output is keyed by the join/group key but produced
        // on the source side's worker, so it does NOT fit a 256-partition store
        // trimmed to the worker's range; partition routing would silently drop every
        // row whose key partition the worker does not own). Both cases hold their
        // whole local dataset at one partition and read by single-source (replicated)
        // or union-gather (locally partitioned), governed by the read path, not the
        // store shape.
        //
        // The single partition is built at THIS node's own range start
        // (`[part_start, part_start + 1)`), not a fixed `part_0`: workers share the
        // data directory, so each worker's full copy must land in a distinct
        // `part_{start}` dir (a fixed `part_0` would collide on flush). A live CREATE
        // runs this hook on each worker post-fork at its own `part_start`, so each
        // builds a distinct dir directly. The pre-fork master (range start 0) builds
        // `part_0`; a worker that inherits it across the fork re-homes it to its own
        // start before any flush (`rehome_single_partition_stores`). The single-
        // partition path must NOT fire for an empty active range (the post-fork
        // master, `[0, 0)`): there the master builds zero partition Tables and stays
        // inert via the `tables.is_empty()` guards.
        let part_start = self.active_part_start;
        let active_nonempty = self.active_part_end > part_start;
        let (routing, part_end) = if single_partition && active_nonempty {
            (Routing::Replicated, part_start + 1)
        } else {
            (Routing::Hashed, self.active_part_end)
        };
        let mut pt = PartitionedTable::new(
            directory,
            name,
            schema,
            id as u32,
            routing,
            kind.recovery_source(),
            part_start,
            part_end,
        )
        .map_err(|e| format!("Failed to create '{name}': error {e} (dir={directory})"))?;
        if kind.is_base_table() {
            // Tag base-table shards as PkUnique so the read cursor can skip
            // payload comparison on a cross-source PK tie. Every base table
            // enables tagging regardless of its `unique_pk` flag — the
            // flush-time checker only marks shards whose data is actually
            // unique.
            pt.enable_pk_unique_tagging();
        }
        Ok(pt)
    }

    fn hook_table_register(&mut self, batch: &Batch) -> Result<(), String> {
        for i in 0..batch.count {
            let weight = batch.get_weight(i);
            let tid = batch.get_pk(i) as i64;

            if weight > 0 {
                // System tables are pre-registered by `register_system_table_families`
                // before `replay_catalog` fires hooks, so their rows show up here
                // with the DAG already populated. Skip to avoid double-registration.
                if self.dag.tables.contains_key(&tid) {
                    continue;
                }

                let sid = self.read_batch_u64(batch, i, 0) as i64;
                let name = self.read_batch_string(batch, i, 1);
                let pk = unpack_pk_cols(self.read_batch_u64(batch, i, 3));
                let flags = self.read_batch_u64(batch, i, 5);
                let is_unique = gnitz_wire::table_flags_unique(flags);
                // Distribution prefix length k (0 = default = full PK).
                // `new_with_dist` clamps an out-of-range k, so a crafted flag
                // cannot index out of bounds.
                let dist_prefix_len = gnitz_wire::table_flags_dist_prefix(flags);
                // Replicated: a full copy on every worker (single partition 0).
                // Rides on the SchemaDescriptor so the write scatter, read gather,
                // join co-partition analyzer, and bootstrap trim all see it.
                let is_replicated = gnitz_wire::table_flags_replicated(flags);

                // REPLICATED and a non-default CLUSTER BY prefix are mutually
                // exclusive — a hash-distribution prefix is meaningless when every
                // worker holds the full copy. The flags word cannot make the conflict
                // unrepresentable (`replicated` is a bit, `k` a byte), so the planner
                // rejects it; re-check at the catalog trust boundary so a crafted or
                // corrupt row can never build a schema that is both replicated and
                // prefix-distributed (consumers branch on the two bits independently).
                if is_replicated && dist_prefix_len != 0 {
                    return Err(format!(
                        "catalog invariant violated: table '{name}' (tid={tid}) is \
                         REPLICATED with a non-default distribution prefix \
                         (k={dist_prefix_len}); these are mutually exclusive.",
                    ));
                }

                let col_defs = self.read_column_defs(tid);
                if col_defs.is_empty() {
                    return Err(format!(
                        "catalog invariant violated: table '{name}' (tid={tid}) registered \
                         before its column records. COL_TAB writes must precede \
                         TABLE_TAB writes (see hooks.rs dispatch doc).",
                    ));
                }

                validate_pk_cols(&col_defs, &pk)?;

                if col_defs.len() > crate::schema::MAX_COLUMNS {
                    return Err(format!(
                        "catalog invariant violated: table '{}' (tid={}) has {} column defs (max {}); \
                         type_codes={:?}",
                        name,
                        tid,
                        col_defs.len(),
                        crate::schema::MAX_COLUMNS,
                        col_defs.iter().map(|c| c.type_code).collect::<Vec<_>>(),
                    ));
                }

                let schema_name = self.caches.schema_by_id.get(&sid).cloned().unwrap_or_default();
                let directory = table_dir(&self.base_dir, &schema_name, &name, tid);
                let tbl_schema = self
                    .build_schema_from_col_defs(&col_defs, pk.as_slice(), dist_prefix_len)
                    .with_replicated(is_replicated);

                // One kind drives the whole property bundle: durability and
                // Pk-unique tagging.
                let kind = RelationKind::BaseTable { unique_pk: is_unique };
                gnitz_debug!(
                    "catalog: creating table dir={} name={} tid={} parts={}",
                    directory,
                    name,
                    tid,
                    NUM_PARTITIONS
                );
                // Pre-stage for cleanup so that if Stage-A fails after the table
                // directory is created, compensate_stage_a's drain removes it.
                let cleanup_idx = self.pending_dir_deletions.len();
                self.pending_dir_deletions.push(directory.clone());
                let pt = self.build_partitioned_storage(kind, &directory, &name, tid, tbl_schema, is_replicated)?;
                // Success: remove the pre-staged entry.
                self.pending_dir_deletions.truncate(cleanup_idx);

                fsync_dir(&schema_dir(&self.base_dir, &schema_name));
                self.dag.register_table(
                    tid,
                    StoreHandle::Partitioned(std::cell::UnsafeCell::new(Box::new(pt))),
                    tbl_schema,
                    kind,
                    0,
                    directory,
                );
                raise_id_counter(&mut self.next_table_id, tid);
            } else if let Some(directory) = self.dag.tables.get(&tid).map(|e| e.directory.clone()) {
                // Safe to cascade unconditionally: precheck_sys_ingest rejects
                // FK/view-dep-blocked drops before the -1 row reaches the WAL.
                // cascade_retract_indices cleans up any in-transaction FK indices
                // (those were never broadcast and must be physically removed).
                // During rollback the rollback gate in `CatalogDeltaSink::submit`
                // ensures these writes bypass pending_broadcasts.
                self.cascade_retract_indices(tid)?;
                // Under atomic CREATE the COL_TAB rows are applied via the enqueuing
                // path, so they are in compensation's drained set and negated
                // directly. CREATE rollback replays descending topo, so this
                // TABLE_TAB(6) -1 fires its DROP-branch cascade BEFORE the drained
                // COL_TAB(1) -1: an unguarded cascade_retract_columns would retract
                // the columns, then the drained -1 would retract them again → a net
                // -1 ghost. Skip it during rollback so compensation's direct negate
                // is the sole retractor. (The unguarded cascade_retract_indices above
                // is the dual: FK auto-indices use submit_local — never enqueued — so
                // the cascade is their only retractor and must run.)
                if !self.ctx.in_rollback() {
                    self.cascade_retract_columns(tid)?;
                }
                self.dag.unregister_table(tid);
                self.pending_dir_deletions.push(directory);
                // Purge both per-table version counters AFTER the cascade above:
                // cascade_retract_indices' apply_index_by_id bump and (on the
                // non-rollback path) cascade_retract_columns' invalidate_col_names
                // bump would otherwise `or_insert` them straight back.
                self.caches.purge_table_versions(tid);
            }
        }
        Ok(())
    }

    fn cascade_retract_indices(&mut self, owner_id: i64) -> Result<(), String> {
        // Clone the idx_id list before any mutation so ingest_to_family → apply_index_by_id
        // can safely remove entries from indices_by_owner as each retraction fires.
        let idx_ids: Vec<i64> = match self.caches.indices_by_owner.get(&owner_id) {
            Some(ids) if !ids.is_empty() => ids.clone(),
            _ => return Ok(()),
        };
        let schema = sys_tab_schema(IDX_TAB_ID);
        // These retractions are part of the owner's drop, not a standalone
        // DROP INDEX, so the IDX_TAB integrity guard must not block them.
        self.with_cascade_drop(|s| {
            for idx_id in idx_ids {
                let batch = retract_single_row(&s.sys_indices, &schema, idx_id as u128);
                if batch.count > 0 {
                    s.submit(SysFamily::Index, batch)?;
                }
            }
            Ok(())
        })
    }

    fn cascade_retract_columns(&mut self, owner_id: i64) -> Result<(), String> {
        let schema = sys_tab_schema(COL_TAB_ID);
        let batch = retract_rows_in_pk_range(
            &self.sys_columns,
            &schema,
            pack_column_id(owner_id, 0) as u128,
            pack_column_id(owner_id + 1, 0) as u128,
        );
        if batch.count > 0 {
            self.submit(SysFamily::Column, batch)?;
        }
        Ok(())
    }

    fn hook_view_register(&mut self, batch: &Batch) -> Result<(), String> {
        for i in 0..batch.count {
            let weight = batch.get_weight(i);
            let vid = batch.get_pk(i) as i64;

            if weight > 0 {
                // See hook_table_register: system tables are pre-registered, user
                // views are not, so this only skips re-registration on replay.
                if self.dag.tables.contains_key(&vid) {
                    continue;
                }

                let sid = self.read_batch_u64(batch, i, 0) as i64;
                let name = self.read_batch_string(batch, i, 1);

                let col_defs = self.read_column_defs(vid);
                if col_defs.is_empty() {
                    return Err(format!(
                        "catalog invariant violated: view '{name}' (vid={vid}) registered \
                         before its column records. COL_TAB writes must precede \
                         VIEW_TAB writes (see hooks.rs dispatch doc).",
                    ));
                }

                // The view's physical PK is the persisted leading-k column list:
                // a single synthetic hash column for join/set-op/distinct views,
                // or the source PK passed through (0..k) for a plain projection
                // over a compound-PK table. A bare `0` decodes back to `[0]`.
                let pk = unpack_pk_cols(self.read_batch_u64(batch, i, VIEWTAB_PAY_PK_COL_IDX));
                validate_pk_cols(&col_defs, &pk)?;

                // Mirror hook_table_register: reject an over-wide schema cleanly
                // rather than letting build_schema_from_col_defs' assert abort the
                // catalog/worker process. Compound-PK plain projection makes this
                // reachable — projection prepends the k source PK columns, so
                // SELECT * over a wide compound-PK table can cross MAX_COLUMNS.
                if col_defs.len() > crate::schema::MAX_COLUMNS {
                    return Err(format!(
                        "catalog invariant violated: view '{}' (vid={}) has {} column defs (max {}); \
                         type_codes={:?}",
                        name,
                        vid,
                        col_defs.len(),
                        crate::schema::MAX_COLUMNS,
                        col_defs.iter().map(|c| c.type_code).collect::<Vec<_>>()
                    ));
                }

                let schema_name = self.caches.schema_by_id.get(&sid).cloned().unwrap_or_default();
                let directory = view_dir(&self.base_dir, &schema_name, &name, vid);
                // Views are not distributed by a chosen key (§2): `0` is the
                // full-PK default sentinel every non-CLUSTER BY caller passes.
                let view_schema = self.build_schema_from_col_defs(&col_defs, pk.as_slice(), 0);

                // See hook_table_register: one kind drives the bundle.
                let kind = RelationKind::View;
                // A view with any replicated source is replicated-derived: its
                // output (a join/reduce against a full copy) is keyed by the
                // join/group key but physically produced on the source side's
                // worker, so it does NOT fit a 256-partition store trimmed to the
                // worker's range (partition routing would drop every output row
                // whose key partition this worker does not own — see
                // `build_partitioned_storage`). Build it single-partition; the
                // read path single-sources it (all sources replicated ⇒ replicated
                // output) or union-gathers it (mixed ⇒ locally partitioned). The
                // circuit's `circuit_nodes` are persisted before this VIEW_TAB row,
                // so `get_source_ids` resolves here.
                let source_ids = self.dag.get_source_ids(vid);
                let has_replicated_source = source_ids
                    .iter()
                    .any(|id| self.dag.tables.get(id).is_some_and(|e| e.schema.replicated()));
                let cleanup_idx = self.pending_dir_deletions.len();
                self.pending_dir_deletions.push(directory.clone());
                let et =
                    self.build_partitioned_storage(kind, &directory, &name, vid, view_schema, has_replicated_source)?;
                self.pending_dir_deletions.truncate(cleanup_idx);

                let max_depth = source_ids
                    .iter()
                    .filter_map(|id| self.dag.tables.get(id))
                    .map(|e| e.depth + 1)
                    .max()
                    .unwrap_or(0);

                self.dag.register_table(
                    vid,
                    StoreHandle::Partitioned(std::cell::UnsafeCell::new(Box::new(et))),
                    view_schema,
                    kind,
                    max_depth,
                    directory,
                );
                raise_id_counter(&mut self.next_table_id, vid);

                // During DROP VIEW rollback the partition files are intact; re-pushing
                // source rows through the circuit would double every aggregation.
                // Boot no longer backfills views inline: recover_from_sal restores
                // unflushed base rows (bypassing view derivation), then the worker
                // post-recovery pass rebuilds cascade-unreachable non-exchange views
                // and the master's backfill_exchange_views cascade re-derives the rest.
                //
                // For a live CREATE, only a PLAIN single-source view (no exchange
                // round, no join-shard scatter) is backfilled inline here — the
                // single-process `backfill_view` is the right driver for it. Every
                // exchange view AND every equi-join (`view_seeds_exchange_backfill`)
                // is left empty here and driven by the live DDL handler's
                // distributed, view-scoped `fan_out_backfill` (and at boot by
                // `backfill_exchange_views`): the single-process driver under-fills
                // them (an exchange view gets no shuffle; a multi-worker equi-join
                // joins only each worker's local shard). The handler drains the new
                // view's base sources before this runs, so the inline scan sees
                // committed data and no deferred tick double-drives it.
                if !self.ctx.in_rollback()
                    && self.ctx.is_live()
                    && self.active_part_start != self.active_part_end
                    && self.dag.ensure_compiled(vid)
                    && !self.dag.view_seeds_exchange_backfill(vid)
                {
                    self.backfill_view(vid);
                }
            } else if let Some(directory) = self.dag.tables.get(&vid).map(|e| e.directory.clone()) {
                // Under atomic CREATE the circuit/dep/COL_TAB rows are applied via
                // the enqueuing path, so they are in compensation's drained set and
                // negated directly. CREATE rollback replays descending topo, so this
                // VIEW_TAB(7) -1 fires its DROP-branch cascade BEFORE the drained
                // circuit(3–5)/DEP(2)/COL(1) -1s: an unguarded cascade would retract
                // them, then the drained -1s would retract them again → a net -1
                // ghost. Skip it during rollback so compensation's direct negate is
                // the sole retractor.
                if !self.ctx.in_rollback() {
                    self.cascade_retract_circuit_and_deps(vid)?;
                    self.cascade_retract_columns(vid)?;
                }
                self.dag.unregister_table(vid);
                self.pending_dir_deletions.push(directory);
                // See hook_table_register: purge post-cascade so the cascade's
                // own counter bumps can't re-create a dropped view's entries.
                // In rollback cascade_retract_columns is skipped (schema_version
                // not re-created) but the tail purge cleans both either way.
                self.caches.purge_table_versions(vid);
            }
        }
        Ok(())
    }

    fn cascade_retract_circuit_and_deps(&mut self, vid: i64) -> Result<(), String> {
        let view_id = vid as u64;
        for family in [
            SysFamily::CircuitNodes,
            SysFamily::CircuitEdges,
            SysFamily::CircuitNodeColumns,
            SysFamily::ViewDep,
        ] {
            let schema = sys_tab_schema(family.id());
            let batch = {
                let table = self.sys_table(family.id()).unwrap();
                retract_rows_by_view(table, &schema, view_id)
            };
            if batch.count > 0 {
                self.submit(family, batch)?;
            }
        }
        Ok(())
    }

    fn hook_index_register(&mut self, batch: &Batch) -> Result<(), String> {
        // Index name lives in the batch's payload column 3; apply_index_by_name
        // already populated the name-indexed caches from there, so we don't
        // read the string here — it would just be a wasted allocation.
        for i in 0..batch.count {
            let weight = batch.get_weight(i);
            let idx_id = batch.get_pk(i) as i64;
            let owner_id = self.read_batch_u64(batch, i, 0) as i64;
            // source_cols carries pack_pk_cols(&col_indices); decode it (a
            // single-column index is the 1-element degenerate case).
            let cols = gnitz_wire::unpack_pk_cols(self.read_batch_u64(batch, i, 2));
            if !cols.is_well_formed() {
                return Err(format!(
                    "Index: column list count {} out of range 1..={}",
                    cols.decoded_count(),
                    gnitz_wire::PK_LIST_MAX_COLS
                ));
            }
            let is_unique = self.read_batch_u64(batch, i, 4) != 0;

            if weight > 0 {
                // Keep worker next_index_id in sync with master-assigned IDs so that
                // create_fk_indices → allocate_index_id never collides with an explicit
                // user index that was broadcast via IDX_TAB +1.
                raise_id_counter(&mut self.next_index_id, idx_id);

                // One circuit per column list (dedup by ordered list). If an
                // incumbent exists, don't build a second table — but a UNIQUE
                // newcomer over a non-unique incumbent must promote it. Promotion
                // is order-independent (the circuit is unique iff ANY index on the
                // column list is unique), so replay reconstructs an identical
                // result regardless of index_id ordering.
                let incumbent_unique = self
                    .dag
                    .tables
                    .get(&owner_id)
                    .and_then(|e| {
                        e.index_circuits
                            .iter()
                            .find(|ic| ic.col_indices.as_slice() == cols.as_slice())
                    })
                    .map(|ic| ic.is_unique);
                if let Some(was_unique) = incumbent_unique {
                    if is_unique && !was_unique {
                        self.promote_index_to_unique(owner_id, cols.as_slice())?;
                    }
                    continue;
                }

                let entry = self
                    .dag
                    .tables
                    .get(&owner_id)
                    .ok_or_else(|| format!("Index: owner table {owner_id} not found"))?;
                // Boot replay and worker ddl_sync reach this hook without
                // precheck_sys_ingest, so re-reject a non-base-table owner
                // here: a persisted or broadcast IDX_TAB row naming a view
                // would otherwise register a circuit that backfills once and
                // then silently serves stale results (index projection runs
                // only on the base-table DML paths).
                if !entry.kind.is_base_table() {
                    return Err(format!("Index: owner {owner_id} is not a base table"));
                }
                let owner_schema = entry.schema;
                // make_index_schema bounds-checks and promotes every column
                // (defence in depth at the catalog trust boundary; a crafted wire
                // row could name an out-of-range or ineligible column).
                let idx_schema = make_index_schema(cols.as_slice(), &owner_schema)?;

                let owner_dir = self
                    .dag
                    .tables
                    .get(&owner_id)
                    .map(|e| e.directory.clone())
                    .unwrap_or_default();
                let idx_dir = index_dir(&owner_dir, idx_id);

                // Pre-stage before Table::new so that if backfill_index fails the
                // directory is in pending_dir_deletions for cleanup. Truncate only
                // after ALL fallible operations succeed.
                let cleanup_idx = self.pending_dir_deletions.len();
                self.pending_dir_deletions.push(idx_dir.clone());

                let idx_table = Table::new(
                    &idx_dir,
                    &format!("_idx_{idx_id}"),
                    idx_schema,
                    idx_id as u32,
                    SYS_TABLE_ARENA,
                    RecoverySource::Rederive,
                )
                .map_err(|e| format!("Failed to create index table: error {e}"))?;

                let mut idx_table_box = Box::new(idx_table);
                let idx_table_ptr = &mut *idx_table_box as *mut Table;
                if !self.ctx.in_rollback() {
                    self.backfill_index(owner_id, cols.as_slice(), is_unique, idx_table_ptr, &idx_schema)?;
                }
                self.dag
                    .add_index_circuit(owner_id, cols.as_slice(), idx_id, idx_table_box, idx_schema, is_unique);

                // Only truncate after all fallible steps succeed.
                self.pending_dir_deletions.truncate(cleanup_idx);
            } else {
                // DROP INDEX: determine what remains for this column list in
                // sys_indices (the -1 row has already been applied to sys_indices
                // by ingest_to_family before fire_hooks runs, so its net weight is
                // 0 and the scan skips it).
                let (has_any, remains_unique) = self.check_remaining_index_uniqueness(owner_id, cols.as_slice());
                if has_any {
                    // Another index (e.g. the FK auto-index) still covers this
                    // column list. Demote the circuit rather than destroying it.
                    self.dag
                        .set_index_circuit_uniqueness(owner_id, cols.as_slice(), remains_unique);
                } else if let Some((owner_dir, creating_idx_id)) = self.dag.tables.get(&owner_id).and_then(|e| {
                    e.index_circuits
                        .iter()
                        .find(|ic| ic.col_indices.as_slice() == cols.as_slice())
                        .map(|ic| (e.directory.clone(), ic.index_id))
                }) {
                    // No index remains on the column list — drop the circuit. Use
                    // the creating index_id for the directory path, not the dropped
                    // index_id: when a second index promoted an incumbent circuit,
                    // the real directory on disk carries the first registrant's id.
                    self.dag.remove_index_circuit(owner_id, cols.as_slice());
                    self.pending_dir_deletions.push(index_dir(&owner_dir, creating_idx_id));
                }
            }
        }
        Ok(())
    }

    /// Scan sys_indices (after the -1 retraction has already been applied by
    /// ingest_to_family before fire_hooks runs) to find any remaining index rows
    /// on `owner_id` / `col_idx`. Returns `(has_any, remains_unique)` to drive
    /// circuit demotion or deletion in `hook_index_register`'s retraction branch.
    fn check_remaining_index_uniqueness(&self, owner_id: i64, cols: &[u32]) -> (bool, bool) {
        let mut has_any = false;
        let mut remains_unique = false;
        self.for_each_index_on_cols(owner_id, cols, |_row_id, is_uniq| {
            has_any = true;
            remains_unique |= is_uniq;
        });
        (has_any, remains_unique)
    }

    fn hook_cascade_fk(&mut self, batch: &Batch) -> Result<(), String> {
        // During CREATE TABLE rollback the TABLE +1 compensation re-registers the
        // table; running hook_cascade_fk here would allocate new index IDs for FK
        // indices that conflict with the IDX +1 rows the topological replay
        // restores a moment later.
        if self.ctx.in_rollback() {
            return Ok(());
        }
        for i in 0..batch.count {
            let weight = batch.get_weight(i);
            if weight <= 0 {
                continue;
            }
            let tid = batch.get_pk(i) as i64;
            if self.ctx.is_live() && self.dag.tables.contains_key(&tid) {
                self.create_fk_indices(tid)?;
            }
        }
        Ok(())
    }
}
