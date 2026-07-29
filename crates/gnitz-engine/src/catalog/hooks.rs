use super::*;

use crate::schema::make_index_schema;
use rustc_hash::FxHashMap;

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
    //     a HashMap/HashSet insert (weight > 0) or remove (weight < 0). They
    //     run over a sign-partitioned view of the batch (all retractions before
    //     all insertions, §3.1 `sign_partition_batch`), so a rewrite pair (a
    //     rename's -1,+1 on one PK) applies its retraction before its insertion
    //     — the order the retract-then-insert appliers require.
    //   * `hook_*`   — side-effectful handlers that build directories, allocate
    //     partitions, register DAG entries, or backfill derived state. Storage
    //     is applied before hooks fire, so the register/cascade hooks gate on
    //     the row's *net* live state (`seek_exact_live`) rather than its own
    //     sign: a rename pair (net-live before and after) fires no teardown and
    //     no re-registration, in any row order, on every application path.
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
    pub(crate) fn fire_hooks(&mut self, family: SysFamily, batch: &Batch) -> Result<(), String> {
        // §3.1: dispatch every handler over a sign-partitioned view (retractions
        // before insertions) so a rewrite pair applies in retract-then-insert
        // order for every fold, on the forward and the (negated) compensation
        // path alike. A weight-homogeneous CREATE/DROP bundle is single-sign, so
        // the partition is identity and `sign_partition_batch` returns `None` (no
        // copy); only a mixed-sign bundle (a rename pair) is reordered into a
        // private copy — the received `batch` (broadcast to workers, negated on
        // rollback) is left untouched.
        let reordered = Self::sign_partition_batch(batch, &sys_tab_schema(family.id()));
        let batch = reordered.as_ref().unwrap_or(batch);
        // Exhaustive over `SysFamily`: a newly-added family is a compile error
        // here, not a silently-skipped `_` arm. The no-op families
        // (Sequence/Circuit*) are named, not swallowed by a wildcard.
        match family {
            SysFamily::Schema => {
                self.apply_schema_caches(batch)?;
                self.hook_schema_dir(batch)?;
            }
            SysFamily::Table => {
                self.apply_entity_caches(batch)?;
                self.apply_schema_of(SysFamily::Table, batch)?;
                self.apply_pk_col_of(TABTAB_PAY_PK_COL_IDX, batch)?;
                self.hook_table_register(batch)?;
                self.apply_needs_lock(SysFamily::Table, batch)?;
                self.hook_cascade_fk(batch)?;
            }
            SysFamily::View => {
                self.apply_entity_caches(batch)?;
                self.apply_schema_of(SysFamily::View, batch)?;
                self.apply_pk_col_of(VIEWTAB_PAY_PK_COL_IDX, batch)?;
                self.hook_view_register(batch)?;
            }
            SysFamily::Column => {
                self.apply_col_names_invalidate(batch)?;
                self.apply_fk_constraints(batch)?;
                self.apply_needs_lock(SysFamily::Column, batch)?;
                // MUST be last — after `apply_col_names_invalidate` evicts the
                // cached col defs — so the DROP NOT NULL descriptor rebuild reads
                // the post-ALTER column defs (§5).
                self.hook_column_alter(batch)?;
            }
            SysFamily::Index => {
                self.apply_index_by_name(batch)?;
                self.apply_index_by_id(batch)?;
                self.hook_index_register(batch)?;
                self.apply_needs_lock(SysFamily::Index, batch)?;
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

    /// A sign-partitioned copy of `batch` — all `weight < 0` rows first (in
    /// original order), then all `weight > 0` rows — when it carries both signs;
    /// `None` when weight-homogeneous (the partition is identity, so the caller
    /// dispatches the original with no copy). This restores the natural Z-set
    /// application order (every retraction observes the pre-bundle state for its
    /// key) for every `apply_*` fold at once, on the forward path and on the
    /// compensation path (where in-place negation flips a client's `-1`-first
    /// pair to `+1`-first, which this re-canonicalizes by the batch's *current*
    /// signs). Catalog bundles are ±1, so the `from_indexed_rows` no-weights
    /// gather (which asserts nonzero weights) is sound.
    fn sign_partition_batch(batch: &Batch, schema: &SchemaDescriptor) -> Option<Batch> {
        let (mut has_neg, mut has_pos) = (false, false);
        for i in 0..batch.count {
            let w = batch.get_weight(i);
            has_neg |= w < 0;
            has_pos |= w > 0;
            if has_neg && has_pos {
                break;
            }
        }
        if !(has_neg && has_pos) {
            return None;
        }
        let mut indices: Vec<u32> = Vec::with_capacity(batch.count);
        indices.extend((0..batch.count).filter(|&i| batch.get_weight(i) < 0).map(|i| i as u32));
        indices.extend((0..batch.count).filter(|&i| batch.get_weight(i) > 0).map(|i| i as u32));
        Some(Batch::from_indexed_rows(&batch.as_mem_batch(), &indices, schema))
    }

    /// Whether `pk_bytes` (the OPK key) names a *net-live* row in `family`'s
    /// store — net positive weight through the merged cursor. Storage is applied
    /// before hooks fire, so a rename pair's shared PK reads net-live (its `-1`
    /// and `+1` are both present) while a genuine drop reads net-dead. The gate
    /// for the reconciling register hooks (§3.2): a different question than
    /// batch-local pair detection, and robust even to the consolidated sys tables
    /// at boot replay (where a rename pair has folded to net `+1`, no `-1`
    /// surviving) and to a multi-op recovery batch on one id.
    fn sys_pk_is_live(&self, family: SysFamily, pk_bytes: &[u8]) -> bool {
        self.sys_store(family).open_cursor().seek_exact_live(pk_bytes)
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
            let hw = batch.read_payload_u64(i, SEQTAB_PAY_VALUE) as i64;
            self.observe_user_sequence(seq_id, hw);
        }
        Ok(())
    }

    // -- Hook handlers ---------------------------------------------------------

    fn hook_schema_dir(&mut self, batch: &Batch) -> Result<(), String> {
        for i in 0..batch.count {
            let weight = batch.get_weight(i);
            let name = batch.read_payload_string(i, SCHEMATAB_PAY_NAME);
            let path = schema_dir(&self.base_dir, &name);
            if weight > 0 {
                // A prior DROP SCHEMA may have queued this exact (name-based)
                // path for checkpoint-gated removal; recreating it now must
                // cancel that, or the gating checkpoint would wipe the new
                // schema and its tables.
                self.cancel_gated_deletion(&path);
                // Staged so that if Stage-A fails after the directory is
                // created, compensate_stage_a's drain removes it.
                self.with_staged_dir(path.clone(), |_| ensure_dir(&path))?;
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
    /// The `with_staged_dir` crash-cleanup wrapping the call in
    /// `hook_table_register`/`hook_view_register` is deliberately left at each
    /// call site: this helper takes `&self` and is pure construction, so
    /// callers own crash-cleanup.
    pub(crate) fn build_partitioned_storage(
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
        // The single child is homed at THIS process's own worker rank (see
        // `PartitionedTable::new`), so a live CREATE on each worker post-fork
        // builds a distinct dir directly. The single-partition path must NOT fire
        // for an empty active range (the post-fork master, `[0, 0)`): there the
        // master builds zero child Tables and stays inert via the
        // `tables.is_empty()` guards.
        let active_nonempty = self.active_part_end > self.active_part_start;
        let routing = if single_partition && active_nonempty {
            Routing::Replicated {
                rank: crate::foundation::worker_ctx::worker_rank(),
            }
        } else {
            Routing::Hashed {
                start: self.active_part_start,
                end: self.active_part_end,
            }
        };
        let mut pt = PartitionedTable::new(directory, schema, id as u32, routing, kind.recovery_source())
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
            // §3.2: gate side effects on the row's NET live state (storage is
            // already applied), not its own sign. A rename pair (net-live before
            // and after) fires neither the `+1` registration nor the `-1`
            // teardown, in any row order and on every application path.
            let net_live = self.sys_pk_is_live(SysFamily::Table, batch.get_pk_bytes(i));

            if weight > 0 {
                // System tables are pre-registered by `register_system_table_families`
                // before `replay_catalog` fires hooks, so their rows show up here
                // with the DAG already populated; a rename pair's `+1` likewise
                // finds the id already registered. Skip to avoid double-register.
                // A `+1` whose net folds to dead (a create cancelled within its
                // own bundle) registers nothing.
                if !net_live || self.dag.tables.contains_key(&tid) {
                    continue;
                }

                let (sid, name, pk, flags) = read_table_tab_row(batch, i);
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
                validate_relation_defs("table", tid, &name, &col_defs, &pk)?;

                let schema_name = self.caches.schema_by_id.get(&sid).cloned().unwrap_or_default();
                let directory = table_dir(&self.base_dir, &schema_name, tid);
                let tbl_schema = build_schema_from_col_defs(&col_defs, pk.as_slice(), dist_prefix_len)?
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
                // Staged so that if Stage-A fails after the table directory
                // is created, compensate_stage_a's drain removes it.
                let pt = self.with_staged_dir(directory.clone(), |s| {
                    s.build_partitioned_storage(kind, &directory, &name, tid, tbl_schema, is_replicated)
                })?;

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
            } else if !net_live {
                // A genuine drop (net-dead): run the teardown cascade. A rename
                // pair's `-1` is net-live, so this branch is skipped and the
                // registration survives untouched.
                if let Some(directory) = self.dag.tables.get(&tid).map(|e| e.directory.clone()) {
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
                let batch = retract_single_row(s.sys_store(SysFamily::Index), &schema, idx_id as u128);
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
            self.sys_store(SysFamily::Column),
            &schema,
            pack_column_id(owner_id, 0) as u128,
            pack_column_id(owner_id + 1, 0) as u128,
        );
        // These whole-table COL retractions are part of the owner's drop and run
        // while the owner is still registered (before `unregister_table`), so the
        // Column precheck arm's unpaired-`-1` reject must be bypassed — mirror
        // `cascade_retract_indices`' `with_cascade_drop` guard.
        self.with_cascade_drop(|s| {
            if batch.count > 0 {
                s.submit(SysFamily::Column, batch)?;
            }
            Ok(())
        })
    }

    /// ALTER … DROP NOT NULL side effect: when a COL_TAB rewrite pair flips a
    /// base table's column `is_nullable 0→1`, the whole-schema payload comparator
    /// moves `FixedIntNonnull → Generic`. Rebuild the descriptor from the
    /// (freshly invalidated) column defs and publish it into the store in place,
    /// so no path can sort a now-nullable column under the null-blind fast
    /// comparator and let a NULL consolidate against a real `0` (§5).
    ///
    /// Trigger is **batch-shape-derived, never context-derived**: this hook fires
    /// on live apply, worker sync, and boot replay alike, with no cascade flag, so
    /// the shape is the only reliable signal. It acts only on a **rewrite pair** —
    /// a column PK carrying both a `-1` and a `+1` row — whose owner is a
    /// registered base table. Only a live column ALTER (or its rollback
    /// compensation) produces one: CREATE TABLE bundles and boot replay are
    /// all-`+1`, DROP TABLE/VIEW cascades all-`-1` — both skipped wholesale.
    /// RENAME COLUMN, DROP COLUMN, and an already-nullable DROP NOT NULL pairs
    /// reach the rebuild and no-op there (`SchemaDescriptor::eq`): none change a
    /// descriptor field.
    fn hook_column_alter(&mut self, batch: &Batch) -> Result<(), String> {
        // Distinct owner tids with a `-1`/`+1` pair (COL_TAB PK = pack_col_id).
        let mut owners: Vec<i64> = Vec::new();
        for i in 0..batch.count {
            if batch.get_weight(i) >= 0 {
                continue;
            }
            let pk = batch.get_pk(i);
            if !(0..batch.count).any(|j| batch.get_weight(j) > 0 && batch.get_pk(j) == pk) {
                continue;
            }
            let owner = gnitz_wire::unpack_col_id(pk as u64).0 as i64;
            if !owners.contains(&owner) {
                owners.push(owner);
            }
        }
        for owner in owners {
            let Some(entry) = self.dag.tables.get(&owner) else {
                continue;
            };
            if !entry.kind.is_base_table() {
                continue;
            }
            let cur = entry.schema;
            // Rebuild from the post-invalidate col defs, re-applying the routing
            // fields `eq` ignores (replicated / dist_prefix_len). Only an
            // is_nullable 0→1 flip changes the descriptor; when it does, publish
            // infallibly in place (equal-region descriptor swap).
            let col_defs = self.read_column_defs(owner);
            let rebuilt = build_schema_from_col_defs(&col_defs, cur.pk_indices(), cur.dist_prefix_len() as usize)
                .map_err(|e| format!("column ALTER on table id={owner}: {e}"))?
                .with_replicated(cur.replicated());
            if rebuilt != cur {
                self.dag.swap_table_schema(owner, rebuilt);
            }
        }
        Ok(())
    }

    /// Row indices in the order [`hook_view_register`](Self::hook_view_register)
    /// must process them: retractions first (as `fire_hooks` hands them over),
    /// then the live rows in dependency order, since registering a view reads its
    /// sources' `replicated` bit and `depth`. Neither order the hook actually sees
    /// is dependency order — boot replay walks VIEW_TAB in PK order, in which a
    /// chain's user-named view sorts *before* the hidden segments it scans (its id
    /// is minted before the body is bound). Without this a view is replicated
    /// before a restart and partitioned after.
    fn view_row_order(&mut self, batch: &Batch) -> Vec<usize> {
        let mut rows: Vec<usize> = (0..batch.count).filter(|&i| batch.get_weight(i) <= 0).collect();
        let mut live: Vec<usize> = (0..batch.count).filter(|&i| batch.get_weight(i) > 0).collect();
        if live.len() > 1 {
            let ids: Vec<i64> = live.iter().map(|&i| batch.get_pk(i) as i64).collect();
            let rank: FxHashMap<i64, usize> = self
                .dag
                .order_by_view_deps(&ids)
                .into_iter()
                .enumerate()
                .map(|(r, vid)| (vid, r))
                .collect();
            // Stable, so a malformed batch's duplicate `+1`s on one id keep row
            // order among themselves.
            live.sort_by_key(|&i| rank[&(batch.get_pk(i) as i64)]);
        }
        rows.append(&mut live);
        rows
    }

    fn hook_view_register(&mut self, batch: &Batch) -> Result<(), String> {
        for i in self.view_row_order(batch) {
            let weight = batch.get_weight(i);
            let vid = batch.get_pk(i) as i64;
            // §3.2: gate on NET live state, so a rename pair (net-live before and
            // after) fires neither registration nor teardown.
            let net_live = self.sys_pk_is_live(SysFamily::View, batch.get_pk_bytes(i));

            if weight > 0 {
                // See hook_table_register: system tables are pre-registered, a
                // rename pair's `+1` finds its vid already registered, and a
                // net-dead `+1` registers nothing — so this only registers a live,
                // not-yet-registered view (the genuine CREATE).
                if !net_live || self.dag.tables.contains_key(&vid) {
                    continue;
                }

                // The view's physical PK is the persisted leading-k column list:
                // a single synthetic hash column for join/set-op/distinct views,
                // or the source PK passed through (0..k) for a plain projection
                // over a compound-PK table.
                // The over-wide rejection inside `validate_relation_defs` is
                // genuinely reachable here: compound-PK plain projection
                // prepends the k source PK columns, so SELECT * over a wide
                // compound-PK table can cross MAX_COLUMNS.
                let (sid, name, pk) = read_view_tab_row(batch, i);
                let col_defs = self.read_column_defs(vid);
                validate_relation_defs("view", vid, &name, &col_defs, &pk)?;

                let schema_name = self.caches.schema_by_id.get(&sid).cloned().unwrap_or_default();
                let directory = view_dir(&self.base_dir, &schema_name, vid);
                // The circuit's `circuit_nodes` are persisted before this VIEW_TAB
                // row, so `get_source_ids` resolves here.
                let source_ids = self.dag.get_source_ids(vid);
                // A view's output is replicated exactly when every source it scans
                // is: each worker then holds every input in full and computes the
                // whole result locally (`execute_multi_worker_step` arm 1), and the
                // read single-sources worker 0. Stamping it on the schema makes the
                // property transitive — `view_row_order` registers this view after
                // its sources, so a view over it reads the bit right here.
                let replicated_source = |tid: &i64| self.dag.tables.get(tid).is_some_and(|e| e.schema.replicated());
                let has_replicated_source = source_ids.iter().any(replicated_source);
                let all_sources_replicated = !source_ids.is_empty() && source_ids.iter().all(replicated_source);
                // Views are not distributed by a chosen key (§2): `0` is the
                // full-PK default sentinel every non-CLUSTER BY caller passes.
                let view_schema =
                    build_schema_from_col_defs(&col_defs, pk.as_slice(), 0)?.with_replicated(all_sources_replicated);

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
                // output) or union-gathers it (mixed ⇒ locally partitioned).
                let et = self.with_staged_dir(directory.clone(), |s| {
                    s.build_partitioned_storage(kind, &directory, &name, vid, view_schema, has_replicated_source)
                })?;

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
                // Boot never backfills views inline either: valid views resume from
                // their checkpoint and the master drives the recovery tick sweep +
                // invalid-view rebuild (see runtime/bootstrap.rs).
                //
                // For a live CREATE, only a PLAIN single-source view (no exchange
                // round, no join-shard scatter) is backfilled inline here — the
                // single-process `backfill_view` is the right driver for it. Every
                // exchange view AND every equi-join (`view_seeds_exchange_backfill`)
                // is left empty here and driven by the live DDL handler's
                // distributed, view-scoped `fan_out_backfill`: the single-process
                // driver under-fills them (an exchange view gets no shuffle; a
                // multi-worker equi-join joins only each worker's local shard). The
                // handler drains the new view's base sources before this runs, so
                // the inline scan sees committed data and no deferred tick
                // double-drives it.
                if !self.ctx.in_rollback()
                    && self.ctx.is_live()
                    && self.active_part_start != self.active_part_end
                    && self.dag.ensure_compiled(vid)
                    && !self.dag.view_seeds_exchange_backfill(vid)
                {
                    self.backfill_view(vid);
                }
            } else if !net_live {
                // A genuine drop (net-dead): a rename pair's `-1` is net-live, so
                // this teardown is skipped and the view registration survives.
                if let Some(directory) = self.dag.tables.get(&vid).map(|e| e.directory.clone()) {
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
            let (owner_id, cols, is_unique) = read_idx_tab_row(batch, i);

            if weight > 0 {
                // Keep worker next_index_id in sync with master-assigned IDs so that
                // create_fk_indices → allocate_index_id never collides with an explicit
                // user index that was broadcast via IDX_TAB +1.
                raise_id_counter(&mut self.next_index_id, idx_id);

                // Boot replay and worker ddl_sync reach this hook without
                // precheck_sys_ingest, so re-run the shared registration guards
                // here (see `validate_index_registration`). Resolve the owner
                // entry once for everything below.
                let entry = self.validate_index_registration(owner_id, &cols)?;
                let owner_schema = entry.schema;
                let owner_dir = entry.directory.clone();

                // One circuit per column list (dedup by ordered list). If an
                // incumbent exists, don't build a second table — but a UNIQUE
                // newcomer over a non-unique incumbent must promote it. Promotion
                // is order-independent (the circuit is unique iff ANY index on the
                // column list is unique), so replay reconstructs an identical
                // result regardless of index_id ordering.
                let incumbent_unique = entry
                    .index_circuits
                    .iter()
                    .find(|ic| ic.col_indices.as_slice() == cols.as_slice())
                    .map(|ic| ic.is_unique);
                if let Some(was_unique) = incumbent_unique {
                    if is_unique && !was_unique {
                        self.promote_index_to_unique(owner_id, cols.as_slice())?;
                    }
                    continue;
                }

                // make_index_schema bounds-checks and promotes every column
                // (defence in depth at the catalog trust boundary; a crafted wire
                // row could name an out-of-range or ineligible column).
                let idx_schema = make_index_schema(cols.as_slice(), &owner_schema)?;

                let idx_dir = index_dir(&owner_dir, idx_id);

                // Staged before Table::new so that if backfill_index fails the
                // directory is in pending_dir_deletions for cleanup; the stage
                // clears only after ALL fallible steps succeed.
                self.with_staged_dir(idx_dir.clone(), |s| {
                    // Homed at THIS process's per-rank dir (workers) or the index
                    // dir itself (master/standalone). The parent `idx_dir` is what
                    // stays staged in pending_dir_deletions — recursive removal
                    // reclaims the rank subdirs too on a failed create or a DROP.
                    let mut idx_table_box = Box::new(new_index_table(&idx_dir, idx_id, idx_schema)?);
                    let idx_table_ptr = &mut *idx_table_box as *mut Table;
                    // The master never populates its index copies (they stay
                    // permanently empty; distributed HAS_PK/seek probes union the
                    // workers' slice-local copies). Workers and standalone backfill
                    // from their local base slice.
                    if !s.ctx.in_rollback() && !crate::foundation::worker_ctx::is_master() {
                        s.backfill_index(
                            owner_id,
                            cols.as_slice(),
                            idx_table_ptr,
                            &idx_schema,
                            is_unique && s.ctx.is_live(),
                        )?;
                    }
                    s.dag
                        .add_index_circuit(owner_id, cols.as_slice(), idx_id, idx_table_box, idx_schema, is_unique);
                    Ok(())
                })?;
            } else {
                // DROP INDEX: determine what remains for this column list in
                // sys_indices (the -1 row has already been applied to sys_indices
                // before fire_hooks runs, so its net weight is 0 and the scan
                // skips it).
                let (mut has_any, mut remains_unique) = (false, false);
                self.for_each_index_on_cols(owner_id, cols.as_slice(), |_row_id, is_uniq| {
                    has_any = true;
                    remains_unique |= is_uniq;
                });
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

    fn hook_cascade_fk(&mut self, batch: &Batch) -> Result<(), String> {
        // During CREATE TABLE rollback the TABLE +1 compensation re-registers the
        // table; running hook_cascade_fk here would allocate new index IDs for FK
        // indices that conflict with the IDX +1 rows the topological replay
        // restores a moment later.
        if self.ctx.in_rollback() {
            return Ok(());
        }
        // §3.2: a rename pair's `+1` must NOT re-mint this table's FK auto-indices
        // — the auto-index name embeds the *current* (new) table name, so its
        // by-name dedup cannot suppress the duplicate a rename would produce for a
        // non-PK FK column. Skip any `+1` whose tid also carries a `-1` in THIS
        // same TABLE_TAB batch (pair-exclusion derived batch-locally). Load-bearing
        // over a master-only threaded set: this hook fires on every `is_live()`
        // path — live apply, worker `ddl_sync`, master SAL-tail recovery — where a
        // threaded set would be absent and let a renamed FK-child mint a duplicate
        // `__fk_` index (diverging the worker catalog / a recovery duplicate).
        let renamed: Vec<i64> = (0..batch.count)
            .filter(|&i| batch.get_weight(i) < 0)
            .map(|i| batch.get_pk(i) as i64)
            .collect();
        for i in 0..batch.count {
            let weight = batch.get_weight(i);
            if weight <= 0 {
                continue;
            }
            let tid = batch.get_pk(i) as i64;
            if renamed.contains(&tid) {
                continue;
            }
            if self.ctx.is_live() && self.dag.tables.contains_key(&tid) {
                self.create_fk_indices(tid)?;
            }
        }
        Ok(())
    }
}
