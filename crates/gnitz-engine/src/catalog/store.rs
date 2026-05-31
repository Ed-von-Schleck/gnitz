use super::*;

/// Cached schema wire block plus derived properties needed by the SAL
/// ingest fast path. Returned by `get_cached_schema_wire_block`; all four
/// fields share the same invalidation lifecycle (DDL on the owning table
/// drops every entry together).
pub struct CachedSchemaWire {
    pub block: Rc<Vec<u8>>,
    pub version: u16,
    /// True when every column has a fixed-width 8-aligned stride and no
    /// STRING columns. Drives the `scatter_wire_group` fast path.
    pub wire_safe: bool,
    /// Sum of pk_stride + 8 (weight) + 8 (null_bmp) + every payload column's
    /// stride. Only meaningful when `wire_safe`.
    pub wire_row_fixed_stride: u32,
}

const SYS_TABLE_IDS: &[i64] = &[
    SCHEMA_TAB_ID, TABLE_TAB_ID, VIEW_TAB_ID, COL_TAB_ID, IDX_TAB_ID,
    DEP_TAB_ID, SEQ_TAB_ID,
    CIRCUIT_NODES_TAB_ID, CIRCUIT_EDGES_TAB_ID, CIRCUIT_NODE_COLUMNS_TAB_ID,
];

impl CatalogEngine {
    // -- System table accessor -----------------------------------------------

    /// Map a system table ID to a mutable reference. Returns None for unknown IDs.
    pub(crate) fn sys_table(&self, table_id: i64) -> Option<&Table> {
        match table_id {
            SCHEMA_TAB_ID => Some(&self.sys_schemas),
            TABLE_TAB_ID => Some(&self.sys_tables),
            VIEW_TAB_ID => Some(&self.sys_views),
            COL_TAB_ID => Some(&self.sys_columns),
            IDX_TAB_ID => Some(&self.sys_indices),
            DEP_TAB_ID => Some(&self.sys_view_deps),
            SEQ_TAB_ID => Some(&self.sys_sequences),
            CIRCUIT_NODES_TAB_ID => Some(&self.sys_circuit_nodes),
            CIRCUIT_EDGES_TAB_ID => Some(&self.sys_circuit_edges),
            CIRCUIT_NODE_COLUMNS_TAB_ID => Some(&self.sys_circuit_node_columns),
            _ => None,
        }
    }

    pub(crate) fn sys_table_mut(&mut self, table_id: i64) -> Option<&mut Table> {
        match table_id {
            SCHEMA_TAB_ID => Some(&mut self.sys_schemas),
            TABLE_TAB_ID => Some(&mut self.sys_tables),
            VIEW_TAB_ID => Some(&mut self.sys_views),
            COL_TAB_ID => Some(&mut self.sys_columns),
            IDX_TAB_ID => Some(&mut self.sys_indices),
            DEP_TAB_ID => Some(&mut self.sys_view_deps),
            SEQ_TAB_ID => Some(&mut self.sys_sequences),
            CIRCUIT_NODES_TAB_ID => Some(&mut self.sys_circuit_nodes),
            CIRCUIT_EDGES_TAB_ID => Some(&mut self.sys_circuit_edges),
            CIRCUIT_NODE_COLUMNS_TAB_ID => Some(&mut self.sys_circuit_node_columns),
            _ => None,
        }
    }

    // -- Server ingestion / scan / seek / flush -----------------------------

    /// Ingest a batch into a table family (unique_pk + store + index projection + hooks).
    /// System tables go through precheck → ingest → hooks → broadcast-queue.
    /// User tables delegate to `DagEngine::ingest_to_family`.
    pub fn ingest_to_family(&mut self, table_id: i64, batch: &Batch) -> Result<(), String> {
        if table_id < FIRST_USER_TABLE_ID {
            if self.in_rollback {
                // During rollback all cascade writes must bypass pending_broadcasts
                // so no compensating row is re-broadcast to workers.
                return self.ingest_to_family_no_broadcast(table_id, batch);
            }
            // Reject BEFORE any WAL write; avoids dangling retractions that
            // would later trip replay_catalog.
            self.precheck_sys_ingest(table_id, batch)?;

            let schema = sys_tab_schema(table_id);
            let zone_lsn = self.ddl_zone_lsn;
            let table = self.sys_table_mut(table_id)
                .ok_or_else(|| format!("Unknown system table_id {}", table_id))?;
            ingest_batch_into(table, batch);
            // Pin every cascading write in the same zone to the same LSN so
            // recovery's dedup check (`msg.lsn <= flushed`) matches the SAL
            // group LSN. ingest_lsn is seeded above every table's current_lsn
            // at boot, so direct assignment never regresses the counter.
            if zone_lsn > 0 {
                table.current_lsn = zone_lsn;
            }
            let mut batch_for_hooks = batch.clone();
            batch_for_hooks.set_schema(schema);
            self.fire_hooks(table_id, &batch_for_hooks)?;

            // Enqueue after hooks so nested cascade pushes land first and the
            // executor broadcasts children → parent. Drop empty batches so
            // worker-side no-op cascades don't accumulate unread entries.
            if batch_for_hooks.count > 0 {
                self.pending_broadcasts.push((table_id, batch_for_hooks));
            }
            Ok(())
        } else {
            let rc = self.dag.ingest_by_ref(table_id, batch);
            if rc < 0 {
                Err(format!("ingest_to_family failed for table_id={} rc={}", table_id, rc))
            } else {
                Ok(())
            }
        }
    }

    /// Like `ingest_to_family` for a system table, but does NOT enqueue the
    /// batch into `pending_broadcasts`. Used by `create_index`'s rollback: the
    /// failed +1 was never broadcast (hook_index_register failed before the
    /// enqueue), so broadcasting the compensating −1 would deliver a phantom
    /// retraction to workers and leave an orphaned negative-weight row. The
    /// hooks still fire so the master-side caches (index_by_name/index_by_id)
    /// are reversed.
    pub(crate) fn ingest_to_family_no_broadcast(
        &mut self,
        table_id: i64,
        batch: &Batch,
    ) -> Result<(), String> {
        // Only valid for system tables.
        debug_assert!(table_id < FIRST_USER_TABLE_ID);
        let schema = sys_tab_schema(table_id);
        let table = self.sys_table_mut(table_id)
            .ok_or_else(|| format!("Unknown system table_id {}", table_id))?;
        ingest_batch_into(table, batch);
        let mut batch_for_hooks = batch.clone();
        batch_for_hooks.set_schema(schema);
        self.fire_hooks(table_id, &batch_for_hooks)
        // Deliberately no push to pending_broadcasts.
    }

    /// Reject a CREATE whose qualified `schema.name` collides with an existing
    /// entity. Re-ingesting the same row (e.g. a pk-col update of `self_id`) is
    /// allowed; only a genuinely different entity under that name is rejected.
    /// Also resolves `sid`, erroring if the schema does not exist.
    fn precheck_qname_unique(&self, sid: i64, name: &str, self_id: i64) -> Result<(), String> {
        let schema_name = self.caches.schema_by_id.get(&sid)
            .ok_or_else(|| format!("Schema with ID {} does not exist", sid))?;
        let qualified = format!("{}.{}", schema_name, name);
        if let Some(&existing) = self.caches.entity_by_qname.get(&qualified) {
            if existing != self_id {
                return Err(format!("Table or view already exists: {}", qualified));
            }
        }
        Ok(())
    }

    /// Validate a system-table write before any mutation (memtable or hooks).
    /// Covers both positive-weight (CREATE) invariants and negative-weight (DROP)
    /// integrity guards so that no invalid state is ever written.
    fn precheck_sys_ingest(&mut self, table_id: i64, batch: &Batch) -> Result<(), String> {
        if table_id != SCHEMA_TAB_ID
            && table_id != TABLE_TAB_ID
            && table_id != VIEW_TAB_ID
            && table_id != IDX_TAB_ID
        {
            return Ok(());
        }

        // -- Positive-weight (CREATE) checks ----------------------------------
        for i in 0..batch.count {
            if batch.get_weight(i) <= 0 { continue; }

            match table_id {
                SCHEMA_TAB_ID => {
                    let name = self.read_batch_string(batch, i, 0);
                    if self.caches.schema_by_name.contains_key(&name) {
                        return Err(format!("Schema already exists: {}", name));
                    }
                }
                TABLE_TAB_ID => {
                    let tid = batch.get_pk(i) as i64;
                    let sid = self.read_batch_u64(batch, i, 0) as i64;
                    let name = self.read_batch_string(batch, i, 1);
                    let pk = unpack_pk_cols(self.read_batch_u64(batch, i, 3));

                    // Collect ColumnDefs, rejecting any gap/duplicate in the
                    // column-index sequence (would mismap columns downstream).
                    let col_defs = self.scan_column_defs(tid, true)?;

                    if col_defs.is_empty() {
                        return Err(format!(
                            "catalog invariant violated: table '{}' (tid={}) registered \
                             before its column records. COL_TAB writes must precede \
                             TABLE_TAB writes (see hooks.rs dispatch doc).",
                            name, tid));
                    }
                    validate_pk_cols(&col_defs, &pk)?;
                    if col_defs.len() > crate::schema::MAX_COLUMNS {
                        return Err(format!(
                            "table '{}' (tid={}) has {} columns (max {})",
                            name, tid, col_defs.len(), crate::schema::MAX_COLUMNS));
                    }

                    let first_pk = pk.as_slice()[0];
                    let self_pk_type = col_defs[first_pk as usize].type_code;
                    for (col_idx, cd) in col_defs.iter().enumerate() {
                        if cd.fk_table_id != 0 {
                            self.validate_fk_column(cd, col_idx, tid, first_pk, self_pk_type)?;
                        }
                    }

                    self.precheck_qname_unique(sid, &name, tid)?;
                }
                VIEW_TAB_ID => {
                    let vid = batch.get_pk(i) as i64;
                    let sid = self.read_batch_u64(batch, i, 0) as i64;
                    let name = self.read_batch_string(batch, i, 1);

                    // Reject any gap/duplicate in the column-index sequence;
                    // only the count is needed here.
                    let col_count = self.scan_column_defs(vid, true)?.len();
                    if col_count == 0 {
                        return Err(format!(
                            "catalog invariant violated: view '{}' (vid={}) registered \
                             before its column records. COL_TAB writes must precede \
                             VIEW_TAB writes (see hooks.rs dispatch doc).",
                            name, vid));
                    }
                    self.precheck_qname_unique(sid, &name, vid)?;
                }
                IDX_TAB_ID => {
                    let owner_id = self.read_batch_u64(batch, i, 0) as i64;
                    let source_col_idx = self.read_batch_u64(batch, i, 2) as usize;
                    let index_name = self.read_batch_string(batch, i, 3);

                    let entry = self.dag.tables.get(&owner_id)
                        .ok_or_else(|| format!("Index: owner table {} not found", owner_id))?;

                    if source_col_idx >= entry.schema.num_columns() {
                        return Err(format!(
                            "Index: column index {} out of bounds for table '{}' \
                             (tid={}, columns={})",
                            source_col_idx,
                            self.caches.entity_by_id.get(&owner_id)
                                .map(|(_, n)| n.as_str()).unwrap_or("?"),
                            owner_id,
                            entry.schema.num_columns()));
                    }

                    let col_type = entry.schema.columns[source_col_idx].type_code;
                    get_index_key_type(col_type)?;

                    let idx_id = batch.get_pk(i) as i64;
                    if let Some(&existing) = self.caches.index_by_name.get(&index_name) {
                        if existing != idx_id {
                            return Err(format!("Index already exists: {}", index_name));
                        }
                    }
                }
                _ => {}
            }
        }

        // -- Negative-weight (DROP) checks ------------------------------------
        let mut drop_ids: Vec<i64> = Vec::new();
        for i in 0..batch.count {
            if batch.get_weight(i) < 0 {
                drop_ids.push(batch.get_pk(i) as i64);
            }
        }
        if drop_ids.is_empty() {
            return Ok(());
        }
        drop_ids.sort_unstable();
        drop_ids.dedup();

        // A UNIQUE index referenced by a FK, and any internal `__fk_` index the
        // RESTRICT seek depends on, are load-bearing — block their drop. A
        // DROP TABLE cascade legitimately retracts the owner's own indices, so
        // it is exempt (the table drop already passed its own precheck).
        if table_id == IDX_TAB_ID {
            if self.cascading_drop {
                return Ok(());
            }
            for &idx_id in &drop_ids {
                if let Some(name) = self.caches.index_by_id.get(&idx_id) {
                    if name.contains(FK_INDEX_INFIX) {
                        return Err("Integrity violation: cannot drop an internal FK index".into());
                    }
                }
                let (owner_id, src_col) = {
                    let mut cursor = self.sys_indices.open_cursor();
                    // sys_indices has a single U64 PK; OPK == big-endian.
                    cursor.cursor.seek_bytes(&(idx_id as u64).to_be_bytes());
                    if !cursor.cursor.valid || cursor.cursor.current_key as u64 != idx_id as u64 {
                        continue;
                    }
                    (cursor_read_u64(&cursor, IDXTAB_COL_OWNER_ID) as i64,
                     cursor_read_u64(&cursor, IDXTAB_COL_SOURCE_COL_IDX) as usize)
                };
                if self.fk_children_of(owner_id).iter().any(|r| r.parent_col_idx == src_col) {
                    let (sn, tn) = self.caches.entity_by_id.get(&owner_id).cloned().unwrap_or_default();
                    return Err(format!(
                        "Integrity violation: index on '{}.{}' is referenced by a foreign key", sn, tn));
                }
            }
            return Ok(());
        }

        if table_id == TABLE_TAB_ID {
            for &tid in &drop_ids {
                // A FK child being co-dropped in this same batch is
                // self-resolving — only a child *outside* the batch blocks the
                // drop. Mirrors the view-dependency binary_search filter below.
                let blocking = self.fk_children_of(tid)
                    .iter()
                    .find(|r| drop_ids.binary_search(&r.child_tid).is_err());
                if let Some(r) = blocking {
                    let (sn, tn) = self.caches.entity_by_id.get(&r.child_tid)
                        .cloned().unwrap_or_default();
                    return Err(format!(
                        "Integrity violation: table referenced by '{}.{}'", sn, tn
                    ));
                }
            }
        }

        // The view-dependency guard applies only to TABLE/VIEW drops. A
        // SCHEMA_TAB drop carries the schema id, and schema ids (allocated from
        // FIRST_USER_SCHEMA_ID = 3) share an i64 space with table ids (from
        // FIRST_USER_TABLE_ID = 16): as both counters climb, a schema id
        // eventually equals an earlier table id. dep_map is keyed by *table*
        // id, so probing it with a schema id would spuriously match an
        // unrelated table's dependents. A schema's own members were already
        // dropped (and individually dep-checked) by the drop_schema cascade
        // before this row is emitted, so the schema row itself needs no guard.
        if table_id == TABLE_TAB_ID || table_id == VIEW_TAB_ID {
            let dep_map = self.dag.get_dep_map();
            for &id in &drop_ids {
                if let Some(dependents) = dep_map.get(&id) {
                    // A dependent that is itself being dropped in this same
                    // batch is self-resolving — only an *outside* dependent
                    // blocks the drop. drop_ids is sorted+deduped above, so
                    // binary_search is O(N log M) vs the O(N·M) of `contains`.
                    let still_active = dependents.iter()
                        .any(|&dep_id| drop_ids.binary_search(&dep_id).is_err());
                    if still_active {
                        let (sn, tn) = self.caches.entity_by_id.get(&id)
                            .cloned().unwrap_or_default();
                        return Err(format!("View dependency: entity '{}.{}'", sn, tn));
                    }
                }
            }
        }
        Ok(())
    }

    /// Drain the pending-broadcast queue. Master calls this once per
    /// top-level DDL and forwards each entry to `broadcast_ddl`. Workers
    /// receive system-table changes via FLAG_DDL_SYNC → `ddl_sync`, which
    /// bypasses `ingest_to_family` entirely, so the queue stays empty there.
    pub fn drain_pending_broadcasts(&mut self) -> Vec<(i64, Batch)> {
        std::mem::take(&mut self.pending_broadcasts)
    }

    /// Physically remove a batch of queued directory paths. An existence guard
    /// keeps a re-queued path (drop applied, dir already gone) quiet.
    fn remove_queued_dirs(dirs: Vec<String>) {
        for dir in dirs {
            if std::path::Path::new(&dir).exists() {
                let _ = std::fs::remove_dir_all(&dir);
            }
        }
    }

    /// Physically remove the directories queued by table/view/index drop
    /// hooks. The executor calls this only after the DDL zone's fdatasync
    /// confirms the drop is durable — see `pending_dir_deletions`.
    pub fn drain_pending_dir_deletions(&mut self) {
        Self::remove_queued_dirs(std::mem::take(&mut self.pending_dir_deletions));
    }

    /// Drop the queued directory paths *without* deleting them. Used when a DDL
    /// fails or to clear stale entries left by a prior failed DDL — the entity
    /// did not durably drop, so its files must survive.
    pub fn discard_pending_dir_deletions(&mut self) {
        self.pending_dir_deletions.clear();
    }

    /// Move durably-dropped directories from the in-flight DDL queue into the
    /// checkpoint-gated queue instead of removing them now. See
    /// `checkpoint_gated_deletions`. Used on the DROP-success path where worker
    /// processes may still be applying the entity's CREATE.
    pub fn defer_pending_dir_deletions(&mut self) {
        self.checkpoint_gated_deletions
            .append(&mut self.pending_dir_deletions);
    }

    /// Physically remove every checkpoint-gated directory. SAFE only at a
    /// checkpoint boundary, after the per-worker FLAG_FLUSH ACKs prove all
    /// workers consumed past the DROP that queued each entry.
    pub fn drain_checkpoint_gated_deletions(&mut self) {
        Self::remove_queued_dirs(std::mem::take(&mut self.checkpoint_gated_deletions));
    }

    /// Cancel a pending checkpoint-gated removal of `dir`. Required when an
    /// entity whose on-disk path is *name-based* (not `<name>_<tid>`) is
    /// recreated before the gating checkpoint drains the queue: only schemas
    /// have name-based paths (`<base>/<schema>`), so a `DROP SCHEMA s` +
    /// `CREATE SCHEMA s` would otherwise leave `<base>/s` queued, and the next
    /// checkpoint's `remove_dir_all` would wipe the recreated schema and every
    /// new table beneath it. Tables/indices encode a monotonic tid in their
    /// path, so a recreate never collides with a gated entry and needs no
    /// cancellation.
    pub fn cancel_gated_deletion(&mut self, dir: &str) {
        self.checkpoint_gated_deletions.retain(|d| d != dir);
    }

    // -----------------------------------------------------------------------
    // Stage-A compensation (DDL rollback)
    // -----------------------------------------------------------------------

    /// Topological creation priority for rollback sort. Lower = earlier in the
    /// dependency chain (created first, destroyed last). Priorities must be
    /// distinct for TABLE_TAB and VIEW_TAB so the relative order is stable.
    fn rollback_topo_priority(tid: i64) -> u8 {
        match tid {
            SCHEMA_TAB_ID                => 0,
            COL_TAB_ID                   => 1,
            DEP_TAB_ID                   => 2,
            CIRCUIT_NODES_TAB_ID         => 3,
            CIRCUIT_EDGES_TAB_ID         => 4,
            CIRCUIT_NODE_COLUMNS_TAB_ID  => 5,
            TABLE_TAB_ID                 => 6,
            VIEW_TAB_ID                  => 7,
            IDX_TAB_ID                   => 8,
            _                            => 99,
        }
    }

    /// Negate every row's weight in-place.
    pub(crate) fn negate_batch_in_place(batch: &mut Batch) {
        for i in 0..batch.count {
            let w = batch.get_weight(i);
            let negated = (-w).to_le_bytes();
            let off = i * 8;
            batch.weight_data_mut()[off..off + 8].copy_from_slice(&negated);
        }
    }

    /// Compensate a failed Stage-A DDL: undo every in-memory mutation that
    /// was applied before the failure so the catalog is exactly as it was.
    ///
    /// Called from the executor's Stage-A error arm. `original_batch` is the
    /// batch that was passed to `ingest_to_family` before the failure.
    pub(crate) fn compensate_stage_a(
        &mut self,
        target_id: i64,
        original_batch: &Batch,
    ) {
        let mut rollback_list = self.drain_pending_broadcasts();

        // pending_broadcasts is children-before-parents: the top-level batch is
        // pushed last (after fire_hooks returns Ok). It is present iff fire_hooks
        // succeeded (evaluate_dag then panicked); absent iff fire_hooks itself
        // failed. Use iter().any() for robustness over .last().
        let top_level_present = rollback_list.iter().any(|(tid, _)| *tid == target_id);
        if !top_level_present {
            let mut b = original_batch.clone();
            b.set_schema(sys_tab_schema(target_id));
            rollback_list.push((target_id, b));
        }

        let is_create = (0..original_batch.count)
            .any(|i| original_batch.get_weight(i) > 0);

        debug_assert!(
            original_batch.count == 0
                || (0..original_batch.count).all(|i| original_batch.get_weight(i) > 0)
                || (0..original_batch.count).all(|i| original_batch.get_weight(i) < 0),
            "compensate_stage_a assumes a weight-homogeneous top-level batch; a mixed \
             CREATE/DROP batch would misclassify the rollback direction and mishandle \
             pending_dir_deletions"
        );

        // For CREATE rollback: dependents unregistered before dependencies — DESCENDING.
        // For DROP rollback: dependencies restored before dependents — ASCENDING.
        if is_create {
            rollback_list.sort_by_key(|(tid, _)| std::cmp::Reverse(Self::rollback_topo_priority(*tid)));
        } else {
            rollback_list.sort_by_key(|(tid, _)| Self::rollback_topo_priority(*tid));
        }

        // Replay each with negated weight through the no-broadcast path.
        // fire_hooks still fires so caches, dag.tables, and pending_dir_deletions
        // are updated. The in_rollback gate in ingest_to_family ensures any
        // cascade that calls back into ingest_to_family also bypasses broadcasts.
        self.in_rollback = true;
        let result = (|| -> Result<(), String> {
            for (tid, mut batch) in rollback_list {
                Self::negate_batch_in_place(&mut batch);
                self.ingest_to_family_no_broadcast(tid, &batch)?;
            }
            Ok(())
        })();
        self.in_rollback = false;

        // For CREATE: drain cleans up any pre-staged directories from hooks.
        // For DROP: discard keeps the entity files on disk (drop was not durable).
        if is_create {
            self.drain_pending_dir_deletions();
        } else {
            self.discard_pending_dir_deletions();
        }

        result.unwrap_or_else(|e| {
            gnitz_fatal_abort!(
                "Stage-A DDL compensation failed — catalog cannot be restored; \
                 aborting to prevent serving a diverged catalog. Cause: {}", e
            );
        });
    }

    /// Pin all system-table writes in the current DDL to `lsn`. Must be
    /// called before the mutate phase so every cascading hook sees the same
    /// value; cleared by `clear_ddl_zone_lsn` after fsync (or on error).
    pub(crate) fn set_ddl_zone_lsn(&mut self, lsn: u64) {
        self.ddl_zone_lsn = lsn;
    }

    /// Release the DDL zone LSN after the zone is durably committed (or
    /// rolled back). Subsequent non-DDL ingest paths use the auto-bump.
    pub(crate) fn clear_ddl_zone_lsn(&mut self) {
        self.ddl_zone_lsn = 0;
    }

    /// Ingest a user-table batch and return the effective delta (after unique_pk
    /// dedup).  Used by multi-worker push where the worker needs the effective
    /// batch for later DAG evaluation but does NOT evaluate immediately.
    /// System tables are NOT supported (use `ingest_to_family` for those).
    pub fn ingest_returning_effective(
        &mut self, table_id: i64, batch: Batch,
    ) -> Result<Batch, String> {
        if table_id < FIRST_USER_TABLE_ID {
            return Err("ingest_returning_effective not supported for system tables".to_string());
        }
        let (rc, effective_opt) = self.dag.ingest_returning_effective(table_id, batch);
        if rc < 0 {
            return Err(format!("ingest failed for table_id={} rc={}", table_id, rc));
        }
        match effective_opt {
            Some(eff) => Ok(eff),
            None => Err(format!("ingest returned no effective batch for table_id={}", table_id)),
        }
    }

    /// Ingest a batch into a user table AND run the single-worker DAG cascade.
    /// For unique_pk tables, the effective batch (with auto-retractions) is
    /// passed to the DAG evaluator so views see correct deltas.
    /// System tables are NOT supported; use ingest_to_family for those.
    pub fn push_and_evaluate(&mut self, table_id: i64, batch: Batch) -> Result<(), String> {
        if table_id < FIRST_USER_TABLE_ID {
            return Err("push_and_evaluate not supported for system tables".to_string());
        }

        let (rc, effective_opt) = self.dag.ingest_returning_effective(table_id, batch);
        if rc < 0 {
            return Err(format!("ingest failed for table_id={} rc={}", table_id, rc));
        }

        // Flush the source table
        let _ = self.dag.flush(table_id);

        // Run DAG cascade with the effective batch
        if let Some(effective) = effective_opt {
            if effective.count > 0 {
                self.dag.evaluate_dag(table_id, effective);
            }
        }

        Ok(())
    }

    /// Scan all positive-weight rows from a table.
    pub fn scan_family(&mut self, table_id: i64) -> Result<Rc<Batch>, String> {
        let schema = if table_id < FIRST_USER_TABLE_ID {
            sys_tab_schema(table_id)
        } else {
            self.dag.tables.get(&table_id)
                .map(|e| e.schema)
                .ok_or_else(|| format!("Unknown table_id {}", table_id))?
        };
        // The CIRCUIT_* tables are now SQL-introspectable: every operator's
        // parameter shape is expressible in catalog schema.
        if let Some(table) = self.sys_table_mut(table_id) {
            return Ok(table.full_scan());
        }
        Ok(self.scan_store(table_id, &schema))
    }

    /// Point lookup by PK. Returns a single-row batch if found, None otherwise.
    pub fn seek_family(&mut self, table_id: i64, pk: u128) -> Result<Option<Batch>, String> {
        let schema = if table_id < FIRST_USER_TABLE_ID {
            sys_tab_schema(table_id)
        } else {
            self.dag.tables.get(&table_id)
                .map(|e| e.schema)
                .ok_or_else(|| format!("Unknown table_id {}", table_id))?
        };

        // Create cursor and seek. The new CircuitNodes/CircuitEdges/
        // CircuitNodeColumns layout is SQL-introspectable; only unrecognised
        // system table IDs return None.
        let mut cursor = if table_id < FIRST_USER_TABLE_ID {
            if let Some(table) = self.sys_table_mut(table_id) {
                table.open_cursor()
            } else {
                return Ok(None);
            }
        } else {
            self.dag.tables.get(&table_id).unwrap().handle.open_cursor()
        };

        let (opk, stride) = crate::storage::opk_key(&schema, pk);
        cursor.cursor.seek_bytes(&opk[..stride]);
        if !cursor.cursor.valid { return Ok(None); }
        if cursor.cursor.current_pk_bytes() != &opk[..stride] {
            return Ok(None);
        }
        if cursor.cursor.current_weight <= 0 { return Ok(None); }

        let mut batch = Batch::with_schema(schema, 1);
        self.copy_cursor_row_to_batch(&cursor, &mut batch);
        Ok(Some(batch))
    }

    /// Byte-keyed sibling of [`seek_family`]: point lookup by full PK bytes,
    /// correct for `pk_stride > 16` where a `u128` key cannot encode the PK.
    /// Mirrors `seek_family` exactly — open the merged cursor, `seek_bytes`,
    /// confirm exact PK and positive weight.
    pub fn seek_family_bytes(&mut self, table_id: i64, pk: &[u8]) -> Result<Option<Batch>, String> {
        let schema = if table_id < FIRST_USER_TABLE_ID {
            sys_tab_schema(table_id)
        } else {
            self.dag.tables.get(&table_id)
                .map(|e| e.schema)
                .ok_or_else(|| format!("Unknown table_id {}", table_id))?
        };

        let mut cursor = if table_id < FIRST_USER_TABLE_ID {
            match self.sys_table_mut(table_id) {
                Some(table) => table.open_cursor(),
                None => return Ok(None),
            }
        } else {
            self.dag.tables.get(&table_id).unwrap().handle.open_cursor()
        };

        cursor.cursor.seek_bytes(pk);
        if !cursor.cursor.valid { return Ok(None); }
        if cursor.cursor.current_pk_bytes() != pk { return Ok(None); }
        if cursor.cursor.current_weight <= 0 { return Ok(None); }

        let mut batch = Batch::with_schema(schema, 1);
        self.copy_cursor_row_to_batch(&cursor, &mut batch);
        Ok(Some(batch))
    }

    /// Batched point lookup. Open one cursor on `table_id` and seek each PK in
    /// `pks` (verbatim OPK bytes), appending the stored row (weight 1) for every
    /// present, live key into a result batch projected to `project`. Each `seek`
    /// re-probes every source independently, so order is not required for
    /// correctness; passing `pks` ascending keeps the per-source binary-search
    /// probes monotonic for better cache locality.
    /// Absent / retracted keys are skipped — identical to `seek_family`'s
    /// single-key `None` — so a removed PK with no committed row contributes
    /// nothing. `project` lists the parent column indices to return (all
    /// non-PK scalar columns); an empty `project` returns PK-only rows.
    ///
    /// Reuses one cursor across all keys (cheaper than N `seek_family` calls,
    /// each of which re-opens a cursor). Projection keeps the result scalar-
    /// only — FK-referenced columns are never STRING/BLOB — so the blob arena
    /// is never touched. Works for both narrow and wide PKs: the OPK bytes are
    /// seeked verbatim, with no native→OPK re-encode.
    pub fn gather_family_bytes(
        &mut self,
        table_id: i64,
        pks: &[crate::storage::PkBuf],
        project: &[u8],
    ) -> Result<Batch, String> {
        let schema = self.dag.tables.get(&table_id)
            .map(|e| e.schema)
            .ok_or_else(|| format!("Unknown table_id {}", table_id))?;
        let result_schema = project_schema(&schema, project);
        let mut out = Batch::with_schema(result_schema, pks.len());
        let mut cursor = self.dag.tables.get(&table_id).unwrap().handle.open_cursor();
        for pk in pks {
            let bytes = pk.pk_bytes();
            cursor.cursor.seek_bytes(bytes);
            if !cursor.cursor.valid { continue; }
            if cursor.cursor.current_pk_bytes() != bytes { continue; }
            if cursor.cursor.current_weight <= 0 { continue; }
            copy_cursor_cols_to_batch(&cursor, &mut out, &schema, project);
        }
        Ok(out)
    }

    /// Index-assisted lookup: prefix-scan the index by the leading indexed
    /// column value, reconstruct the source PK from the index PK suffix,
    /// and resolve to the source row.
    ///
    /// `prefix` is the indexed column value in LE bytes (at most 8 bytes for
    /// a promoted U64 — `make_index_schema` always promotes to ≤8). The
    /// trailing bytes of the stored index PK are zero in `batch_project_index`
    /// for narrow indexed columns, so a `starts_with(prefix)` check terminates
    /// the scan at the right key boundary.
    pub fn seek_by_index(&mut self, table_id: i64, col_idx: u32, prefix: &[u8])
        -> Result<Option<Batch>, String>
    {
        let entry = self.dag.tables.get(&table_id)
            .ok_or_else(|| format!("Unknown table_id {}", table_id))?;

        let ic = entry.index_circuits.iter()
            .find(|ic| ic.col_idx == col_idx)
            .ok_or_else(|| format!("No index on col_idx {} for table {}", col_idx, table_id))?;

        let src_pk_stride = entry.schema.pk_stride() as usize;
        let idx_key_size = ic.index_schema.columns[0].size() as usize;
        let idx_key_type = ic.index_schema.columns[0].type_code;

        // `prefix` is the indexed value in native little-endian bytes (source-
        // column width). The index PK region is OPK-at-rest, so zero-extend to
        // the promoted index key width and OPK-encode to match stored entries.
        let mut native_le = [0u8; 16];
        let n = prefix.len().min(idx_key_size);
        native_le[..n].copy_from_slice(&prefix[..n]);
        let opk = crate::schema::index_opk_prefix(
            u128::from_le_bytes(native_le), idx_key_type, idx_key_size,
        );
        let opk_prefix = &opk[..idx_key_size];

        let idx_table = ic.table_mut();
        let mut cursor = idx_table.open_cursor();

        // Seek to the first positive-weight match, then walk forward with
        // `walk_to_positive_with_prefix` after each consumed entry. Re-calling
        // `seek_first_positive_with_prefix` inside the loop would re-seek and
        // re-find the same entry forever — an orphaned index entry (positive
        // weight, no source row) would spin.
        // A non-unique indexed value matches multiple rows; accumulate ALL of
        // them on this worker, not just the first. (A unique index yields one
        // match, so this is equivalent there.) Index entries co-locate with
        // their source rows (partitioned by source PK), so a value's matches can
        // be spread across workers: this returns one worker's partial set and
        // the master (`fan_out_seek_by_index_collect_async`) merges across all.
        let mut result: Option<Batch> = None;
        let mut hit = cursor.cursor.seek_first_positive_with_prefix(opk_prefix);
        while hit {
            // Copy the source PK out of the index PK suffix into a fixed buffer
            // before the `&mut self` resolve below releases the cursor borrow.
            // Widened to MAX_PK_BYTES so wide (`src_pk_stride > 16`) sources
            // resolve via `seek_family_bytes`.
            let current_pk = cursor.cursor.current_pk_bytes();
            let mut src_pk_buf = [0u8; crate::schema::MAX_PK_BYTES];
            src_pk_buf[..src_pk_stride].copy_from_slice(
                &current_pk[idx_key_size..idx_key_size + src_pk_stride],
            );
            if let Some(batch) = self.seek_family_bytes(table_id, &src_pk_buf[..src_pk_stride])? {
                match result.as_mut() {
                    Some(acc) => acc.append_batch(&batch, 0, batch.count),
                    None => result = Some(batch),
                }
            }
            cursor.cursor.advance();
            hit = cursor.cursor.walk_to_positive_with_prefix(opk_prefix);
        }
        Ok(result)
    }

    /// Flush a table's WAL.
    pub fn flush_family(&mut self, table_id: i64) -> Result<(), String> {
        if table_id < FIRST_USER_TABLE_ID {
            if let Some(table) = self.sys_table_mut(table_id) {
                table.flush().map_err(|e| format!("flush error: {}", e))?;
                // Compact so L0 shards don't accumulate without bound across
                // DDL-heavy sessions (system catalog tables are scanned on every
                // boot and DDL op).
                table.compact_if_needed().map_err(|e| format!("compaction error: {:?}", e))?;
            }
            Ok(())
        } else {
            let rc = self.dag.flush(table_id);
            if rc < 0 {
                Err(format!("flush failed for table_id={} rc={}", table_id, rc))
            } else {
                Ok(())
            }
        }
    }

    /// Phase 1 across a table family. System tables flush inline (legacy
    /// path; they checkpoint during DDL, not at `flush_all`).
    pub fn flush_family_prepare(
        &mut self,
        table_id: i64,
    ) -> Result<Vec<(usize, crate::storage::FlushWork)>, String> {
        if table_id < FIRST_USER_TABLE_ID {
            if let Some(table) = self.sys_table_mut(table_id) {
                table.flush().map_err(|e| format!("flush error: {}", e))?;
                table.compact_if_needed().map_err(|e| format!("compaction error: {:?}", e))?;
            }
            Ok(Vec::new())
        } else {
            self.dag.flush_prepare(table_id)
        }
    }

    /// Phase 3 across a table family. Returns dirfds to fsync.
    pub fn flush_family_commit_batch(
        &mut self,
        table_id: i64,
        works: Vec<(usize, crate::storage::FlushWork)>,
    ) -> Result<Vec<libc::c_int>, String> {
        if table_id < FIRST_USER_TABLE_ID {
            // System tables commit inline; no FlushWork should arrive here.
            debug_assert!(works.is_empty());
            Ok(Vec::new())
        } else {
            self.dag.flush_commit_batch(table_id, works)
        }
    }

    /// Worker DDL sync: memonly ingest into system table + fire hooks.
    /// Workers receive DDL deltas from master and need to update their registry
    /// without writing to WAL (master owns durability).
    pub fn ddl_sync(&mut self, table_id: i64, batch: Batch) -> Result<(), String> {
        if table_id >= FIRST_USER_TABLE_ID {
            return Err("ddl_sync only for system tables".into());
        }
        // Fire hooks first (borrow only); the ingest below moves `batch`.
        // Hooks have no observable ordering dependency on the storage write.
        self.fire_hooks(table_id, &batch)?;
        let table = self.sys_table_mut(table_id)
            .ok_or_else(|| format!("Unknown system table_id {}", table_id))?;
        let _ = table.ingest_owned_batch_memonly(batch);
        Ok(())
    }

    /// Raw store ingest: SAL recovery path — no unique_pk, no hooks, no index projection.
    pub fn raw_store_ingest(&mut self, table_id: i64, batch: Batch) -> Result<(), String> {
        let entry = self.dag.tables.get(&table_id)
            .ok_or_else(|| format!("Unknown table_id {}", table_id))?;
        let _ = entry.handle.ingest_owned_batch(batch);
        Ok(())
    }

    /// SAL recovery replay — unique_pk-aware. Routes user-table batches
    /// through the full `ingest_returning_effective` path so that
    /// retractions (which carry zero-padded payloads on the wire) are
    /// resolved against the actual stored payload instead of being
    /// added as orphaned rows. The retract-and-insert pattern in
    /// `enforce_unique_pk_partitioned` makes the replay idempotent
    /// w.r.t. already-flushed data.
    ///
    /// Index shards see duplicate `(+1, -1)` projections when a batch
    /// is replayed after already having been flushed. These consolidate
    /// to zero on read and are pruned at the next compaction.
    pub fn replay_ingest(&mut self, table_id: i64, batch: Batch) -> Result<(), String> {
        if table_id < FIRST_USER_TABLE_ID {
            // System tables: use raw ingest (no unique_pk semantics).
            return self.raw_store_ingest(table_id, batch);
        }
        let (rc, _effective) = self.dag.ingest_returning_effective(table_id, batch);
        if rc < 0 {
            return Err(format!("replay_ingest failed for table_id={} rc={}", table_id, rc));
        }
        Ok(())
    }

    // -- Partition management (for multi-worker fork) -------------------------

    /// Set active partition range for user tables.
    pub fn set_active_partitions(&mut self, start: u32, end: u32) {
        self.active_part_start = start;
        self.active_part_end = end;
    }

    /// Close all partitions in user tables (master after fork).
    pub fn close_user_table_partitions(&mut self) {
        for (&tid, entry) in &self.dag.tables {
            if tid < FIRST_USER_TABLE_ID { continue; }
            if let Some(ptable) = entry.handle.as_partitioned_mut() {
                ptable.close_all_partitions();
            }
        }
    }

    /// Trim worker partitions to assigned range.
    pub fn trim_worker_partitions(&mut self, start: u32, end: u32) {
        for (&tid, entry) in &self.dag.tables {
            if tid < FIRST_USER_TABLE_ID { continue; }
            if let Some(ptable) = entry.handle.as_partitioned_mut() {
                ptable.close_partitions_outside(start, end);
            }
        }
    }

    /// Invalidate all cached plans.
    pub fn invalidate_all_plans(&mut self) {
        self.dag.invalidate_all();
    }

    // -- FK / index metadata queries (for distributed validation) -------------

    /// Number of FK constraints on a table.
    pub fn get_fk_count(&self, table_id: i64) -> usize {
        self.caches.fk_by_child.get(&table_id).map(|v| v.len()).unwrap_or(0)
    }

    /// Get FK constraint at index: (fk_col_idx, target_table_id, target_col_idx).
    pub fn get_fk_constraint(&self, table_id: i64, idx: usize) -> Option<(usize, i64, usize)> {
        self.caches.fk_by_child.get(&table_id)
            .and_then(|v| v.get(idx))
            .map(|c| (c.fk_col_idx, c.target_table_id, c.target_col_idx))
    }

    /// Get FK column type code (for promote_to_key in distributed validation).
    pub fn get_fk_col_type(&self, table_id: i64, fk_col_idx: usize) -> u8 {
        self.dag.tables.get(&table_id)
            .map(|e| e.schema.columns[fk_col_idx].type_code)
            .unwrap_or(0)
    }

    /// Number of index circuits on a table.
    pub fn get_index_circuit_count(&self, table_id: i64) -> usize {
        self.dag.tables.get(&table_id)
            .map(|e| e.index_circuits.len())
            .unwrap_or(0)
    }

    /// Get index circuit info at index: (col_idx, is_unique, type_code).
    pub fn get_index_circuit_info(&self, table_id: i64, idx: usize)
        -> Option<(u32, bool, u8)>
    {
        let entry = self.dag.tables.get(&table_id)?;
        let ic = entry.index_circuits.get(idx)?;
        let type_code = entry.schema.columns[ic.col_idx as usize].type_code;
        Some((ic.col_idx, ic.is_unique, type_code))
    }

    /// Get index store handle for a specific column index (for worker has_pk via index).
    pub fn get_index_store_handle(&self, table_id: i64, col_idx: u32) -> *const Table {
        self.dag.tables.get(&table_id)
            .and_then(|e| e.index_circuits.iter().find(|ic| ic.col_idx == col_idx))
            .map(|ic| ic.table_mut() as *const Table)
            .unwrap_or(std::ptr::null())
    }

    /// Get the SchemaDescriptor for the index circuit at position idx.
    pub fn get_index_circuit_schema(&self, table_id: i64, idx: usize) -> Option<SchemaDescriptor> {
        self.dag.tables.get(&table_id)
            .and_then(|e| e.index_circuits.get(idx))
            .map(|ic| ic.index_schema)
    }

    /// Number of child tables that reference `parent_id` via FK.
    pub fn get_fk_children_count(&self, parent_id: i64) -> usize {
        self.caches.fk_by_parent.get(&parent_id).map(|v| v.len()).unwrap_or(0)
    }

    /// True if column `col_idx` of `table_id` is covered by a UNIQUE secondary
    /// index. A unique index yields at most one match for any value, so a
    /// SEEK_BY_INDEX can be unicast to a single worker; a non-unique index's
    /// matches scatter across workers and must be merged.
    pub fn index_col_is_unique(&self, table_id: i64, col_idx: u32) -> bool {
        self.dag.tables.get(&table_id)
            .and_then(|e| e.index_circuits.iter().find(|ic| ic.col_idx == col_idx))
            .map(|ic| ic.is_unique)
            .unwrap_or(false)
    }

    /// True if the table has at least one unique secondary index circuit.
    /// Used to decide whether distributed unique-index validation is needed.
    /// Non-unique circuits (e.g. FK indices) do not count.
    pub fn has_any_unique_index(&self, table_id: i64) -> bool {
        self.dag.tables.get(&table_id)
            .map(|e| e.index_circuits.iter().any(|ic| ic.is_unique))
            .unwrap_or(false)
    }

    /// True if the table was created with `unique_pk=true`. Used by the
    /// distributed validator to decide whether Error-mode inserts need
    /// an against-store PK rejection broadcast.
    pub fn table_has_unique_pk(&self, table_id: i64) -> bool {
        self.dag.tables.get(&table_id)
            .map(|e| e.unique_pk)
            .unwrap_or(false)
    }

    /// Get child info at index: (child_table_id, fk_col_idx, parent_col_idx).
    pub fn get_fk_child_info(&self, parent_id: i64, idx: usize) -> Option<(i64, usize, usize)> {
        self.caches.fk_by_parent.get(&parent_id)
            .and_then(|v| v.get(idx))
            .map(|r| (r.child_tid, r.fk_col_idx, r.parent_col_idx))
    }

    /// Get the index schema for a specific column's FK index on a table.
    pub fn get_index_schema_by_col(&self, table_id: i64, col_idx: u32) -> Option<SchemaDescriptor> {
        self.dag.tables.get(&table_id)
            .and_then(|e| e.index_circuits.iter().find(|ic| ic.col_idx == col_idx))
            .map(|ic| ic.index_schema)
    }

    /// Get column names for a table/view. Cached after first lookup.
    pub fn get_column_names(&mut self, table_id: i64) -> Vec<String> {
        if let Some(names) = self.caches.col_names.get(&table_id) {
            return names.clone();
        }
        let names: Vec<String> = self.read_column_defs(table_id)
            .into_iter().map(|cd| cd.name).collect();
        self.caches.col_names.insert(table_id, names.clone());
        names
    }

    /// Get column names as byte vectors. Backed by col_names cache; lazy-populated.
    pub fn get_col_names_bytes(&mut self, table_id: i64) -> Rc<Vec<Vec<u8>>> {
        if let Some(bytes) = self.caches.col_names_bytes.get(&table_id) {
            return bytes.clone();
        }
        let names = self.get_column_names(table_id);
        let bytes = Rc::new(names.iter().map(|n| n.as_bytes().to_vec()).collect::<Vec<_>>());
        self.caches.col_names_bytes.insert(table_id, bytes.clone());
        bytes
    }

    /// Return the cached encoded schema wire block, current schema version,
    /// and derived wire properties (`wire_safe`, `wire_row_fixed_stride`)
    /// for `table_id`, or `None` if the block isn't yet cached. Wire props
    /// are paired with the block so they share invalidation.
    pub fn get_cached_schema_wire_block(&self, table_id: i64) -> Option<CachedSchemaWire> {
        let (block, wire_safe, wire_row_fixed_stride) =
            self.caches.schema_wire_cache.get(&table_id)?.clone();
        let version = self.caches.get_schema_version(table_id);
        Some(CachedSchemaWire { block, version, wire_safe, wire_row_fixed_stride })
    }

    /// Return the current schema version for `table_id` (1 if unknown).
    pub fn get_schema_version(&self, table_id: i64) -> u16 {
        self.caches.get_schema_version(table_id)
    }

    /// Store an encoded schema wire block in the cache, along with its
    /// derived wire properties. The two are written together so the
    /// invalidation in `clear_col_cache_no_bump` keeps them consistent.
    pub fn set_schema_wire_block(
        &mut self,
        table_id: i64,
        block: Rc<Vec<u8>>,
        wire_safe: bool,
        wire_row_fixed_stride: u32,
    ) {
        self.caches.schema_wire_cache.insert(table_id, (block, wire_safe, wire_row_fixed_stride));
    }

    /// True if any lock is needed for inserts into this table.
    /// Cached: set on table/index create/drop, no per-call overhead.
    pub fn needs_table_lock(&self, table_id: i64) -> bool {
        self.caches.needs_lock.contains(&table_id)
    }

    /// Return the full set of table IDs that must be locked together for a
    /// write to `table_id`, sorted ascending to guarantee deadlock-free
    /// acquisition. Includes `table_id` itself plus all FK parents (to
    /// guard concurrent parent DELETE) and FK children (to guard concurrent
    /// child INSERT during a parent DELETE). Returns an empty vec if this
    /// table requires no lock at all.
    pub fn fk_lock_set(&self, table_id: i64) -> Vec<i64> {
        if !self.caches.needs_lock.contains(&table_id) {
            return Vec::new();
        }
        let mut tids = vec![table_id];
        if let Some(constraints) = self.caches.fk_by_child.get(&table_id) {
            for c in constraints {
                tids.push(c.target_table_id);
            }
        }
        if let Some(children) = self.caches.fk_by_parent.get(&table_id) {
            for r in children {
                tids.push(r.child_tid);
            }
        }
        tids.sort_unstable();
        tids.dedup();
        tids
    }

    // -- Store handle accessors -----------------------------------------------

    /// Get raw PartitionedTable handle for a user table.
    pub fn get_ptable_handle(&self, table_id: i64) -> Option<*mut PartitionedTable> {
        self.dag.tables.get(&table_id).and_then(|e| {
            match &e.handle {
                StoreHandle::Partitioned(cell) => Some(unsafe { &mut **cell.get() as *mut PartitionedTable }),
                _ => None,
            }
        })
    }

    /// Get schema descriptor for a table.
    pub fn get_schema_desc(&self, table_id: i64) -> Option<SchemaDescriptor> {
        if table_id > 0 && table_id < FIRST_USER_TABLE_ID {
            Some(sys_tab_schema(table_id))
        } else {
            self.dag.tables.get(&table_id).map(|e| e.schema)
        }
    }

    /// Get a raw mutable pointer to the DagEngine.
    pub fn get_dag_ptr(&mut self) -> *mut DagEngine {
        &mut self.dag as *mut DagEngine
    }

    // -- Iteration helpers ----------------------------------------------------

    /// Collect all user table IDs.
    pub fn iter_user_table_ids(&self) -> Vec<i64> {
        self.dag.tables.keys()
            .filter(|&&tid| tid >= FIRST_USER_TABLE_ID)
            .copied()
            .collect()
    }

    /// Get max flushed LSN for a table (for SAL recovery). Handles both
    /// user tables (via the DAG) and system tables (via direct lookup).
    pub fn get_max_flushed_lsn(&self, table_id: i64) -> u64 {
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
    /// ids. Implemented without `&mut self` so recovery callers can hold
    /// other shared state.
    fn sys_table_current_lsn(&self, table_id: i64) -> u64 {
        let table: &Table = match table_id {
            SCHEMA_TAB_ID => &self.sys_schemas,
            TABLE_TAB_ID => &self.sys_tables,
            VIEW_TAB_ID => &self.sys_views,
            COL_TAB_ID => &self.sys_columns,
            IDX_TAB_ID => &self.sys_indices,
            DEP_TAB_ID => &self.sys_view_deps,
            SEQ_TAB_ID => &self.sys_sequences,
            CIRCUIT_NODES_TAB_ID => &self.sys_circuit_nodes,
            CIRCUIT_EDGES_TAB_ID => &self.sys_circuit_edges,
            CIRCUIT_NODE_COLUMNS_TAB_ID => &self.sys_circuit_node_columns,
            _ => return 0,
        };
        table.current_lsn
    }

    /// Build a map of every known table id → max flushed LSN, covering
    /// both system tables and user tables. Recovery uses this as the
    /// dedup filter for the unified two-pass walk.
    pub fn collect_all_flushed_lsns(&self) -> std::collections::HashMap<i64, u64> {
        let mut map = std::collections::HashMap::new();
        for &tid in SYS_TABLE_IDS {
            map.insert(tid, self.sys_table_current_lsn(tid));
        }
        for &tid in self.dag.tables.keys() {
            if tid >= FIRST_USER_TABLE_ID {
                map.insert(tid, self.get_max_flushed_lsn(tid));
            }
        }
        map
    }

    /// Maximum `current_lsn` across all tables — system and user. The
    /// executor seeds `shared.ingest_lsn` from this at boot so the next
    /// allocated zone LSN is strictly greater than every table's current
    /// counter, keeping the direct assignment in `ingest_to_family`
    /// monotonic across upgrades and restarts.
    pub fn max_table_current_lsn(&self) -> u64 {
        let mut max_lsn = 0u64;
        for &tid in SYS_TABLE_IDS {
            max_lsn = max_lsn.max(self.sys_table_current_lsn(tid));
        }
        for entry in self.dag.tables.values() {
            max_lsn = max_lsn.max(entry.handle.current_lsn());
        }
        max_lsn
    }

    // -- Read column definitions from sys_columns --------------------------

    /// Scan sys_columns for every positive-weight column record owned by
    /// `owner_id`, in column-index order. When `check_contiguity` is set the
    /// column indices (lower 9 bits of the packed PK) must run 0,1,2,… with no
    /// gap or duplicate — a gap would silently mismap columns in
    /// `build_schema_from_col_defs`, so the create-precheck path rejects it.
    /// The non-checking form is infallible by construction.
    pub(crate) fn scan_column_defs(
        &self,
        owner_id: i64,
        check_contiguity: bool,
    ) -> Result<Vec<ColumnDef>, String> {
        let start_pk = pack_column_id(owner_id, 0);
        let end_pk = pack_column_id(owner_id + 1, 0);
        let mut cursor = self.sys_columns.open_cursor();
        // sys_columns has a single U64 PK; OPK == big-endian.
        cursor.cursor.seek_bytes(&start_pk.to_be_bytes());

        let mut defs = Vec::new();
        let mut expected: i64 = 0;
        while cursor.cursor.valid {
            let pk = cursor.cursor.current_key as u64;
            if pk >= end_pk { break; }
            if cursor.cursor.current_weight > 0 {
                if check_contiguity {
                    // pack_column_id layout: owner_id << 9 | col_idx (lower 9 bits).
                    let actual = (pk & 0x1FF) as i64;
                    if actual != expected {
                        return Err(format!(
                            "entity (owner_id={}): column records are non-contiguous; \
                             expected index {}, got {}",
                            owner_id, expected, actual));
                    }
                    expected += 1;
                }
                defs.push(ColumnDef {
                    name:        cursor_read_string(&cursor, COLTAB_COL_NAME),
                    type_code:   cursor_read_u64(&cursor, COLTAB_COL_TYPE_CODE) as u8,
                    is_nullable: cursor_read_u64(&cursor, COLTAB_COL_IS_NULLABLE) != 0,
                    fk_table_id: cursor_read_u64(&cursor, COLTAB_COL_FK_TABLE_ID) as i64,
                    fk_col_idx:  cursor_read_u64(&cursor, COLTAB_COL_FK_COL_IDX) as u32,
                });
            }
            cursor.cursor.advance();
        }
        Ok(defs)
    }

    pub(crate) fn read_column_defs(&self, owner_id: i64) -> Vec<ColumnDef> {
        self.scan_column_defs(owner_id, false).unwrap()
    }

    pub(crate) fn build_schema_from_col_defs(&self, col_defs: &[ColumnDef], pk_cols: &[u32]) -> SchemaDescriptor {
        assert!(
            col_defs.len() <= crate::schema::MAX_COLUMNS,
            "build_schema_from_col_defs: too many columns ({}) for entity (type_codes: {:?})",
            col_defs.len(),
            col_defs.iter().map(|c| c.type_code).collect::<Vec<_>>(),
        );
        let mut cols = [zero_col(); crate::schema::MAX_COLUMNS];
        for (i, cd) in col_defs.iter().enumerate() {
            cols[i] = SchemaColumn::new(cd.type_code, if cd.is_nullable { 1 } else { 0 });
        }
        SchemaDescriptor::new(&cols[..col_defs.len()], pk_cols)
    }

    // -- FK constraint queries ---------------------------------------------

    /// Returns all child tables that have FK constraints targeting `parent_id`.
    pub(crate) fn fk_children_of(&self, parent_id: i64) -> &[FkParentRef] {
        self.caches.fk_by_parent.get(&parent_id).map(|v| v.as_slice()).unwrap_or(&[])
    }

    // -- Batch field readers -----------------------------------------------

    pub(crate) fn read_batch_u64(&self, batch: &Batch, row: usize, payload_col: usize) -> u64 {
        let off = row * 8;
        let col = batch.col_data(payload_col);
        if off + 8 > col.len() { return 0; }
        u64::from_le_bytes(col[off..off + 8].try_into().unwrap_or([0; 8]))
    }

    pub(crate) fn read_batch_string(&self, batch: &Batch, row: usize, payload_col: usize) -> String {
        let off = row * 16;
        let data = batch.col_data(payload_col);
        if off + 16 > data.len() { return String::new(); }
        let st: [u8; 16] = data[off..off + 16].try_into().unwrap_or([0; 16]);
        let bytes = crate::schema::decode_german_string(&st, &batch.blob);
        String::from_utf8(bytes).unwrap_or_default()
    }

    // -- Registry query methods -----------------------------------------------

    pub fn has_id(&self, table_id: i64) -> bool {
        self.dag.tables.contains_key(&table_id)
    }

    pub fn get_schema(&self, table_id: i64) -> Option<SchemaDescriptor> {
        self.dag.tables.get(&table_id).map(|e| e.schema)
    }

    pub fn get_schema_name_by_id(&self, schema_id: i64) -> &str {
        self.caches.schema_by_id.get(&schema_id).map(|s| s.as_str()).unwrap_or("")
    }

    pub fn has_schema(&self, name: &str) -> bool {
        self.caches.schema_by_name.contains_key(name)
    }

    pub fn get_schema_id(&self, name: &str) -> i64 {
        self.caches.schema_by_name.get(name).copied().unwrap_or(-1)
    }

    pub fn schema_is_empty(&self, schema_name: &str) -> bool {
        let sid = match self.caches.schema_by_name.get(schema_name) {
            Some(&sid) => sid,
            None => return true,
        };
        let t_empty = self.caches.tables_by_schema.get(&sid).map(|s| s.is_empty()).unwrap_or(true);
        let v_empty = self.caches.views_by_schema.get(&sid).map(|s| s.is_empty()).unwrap_or(true);
        t_empty && v_empty
    }

    pub fn allocate_schema_id(&mut self) -> i64 {
        let sid = self.next_schema_id;
        self.next_schema_id += 1;
        sid
    }

    pub fn allocate_table_id(&mut self) -> i64 {
        let tid = self.next_table_id;
        self.next_table_id += 1;
        tid
    }

    pub fn allocate_index_id(&mut self) -> i64 {
        let iid = self.next_index_id;
        self.next_index_id += 1;
        iid
    }

    pub fn get_depth(&self, table_id: i64) -> i32 {
        self.dag.tables.get(&table_id).map(|e| e.depth).unwrap_or(0)
    }

    pub fn get_qualified_name(&self, table_id: i64) -> Option<(&str, &str)> {
        self.caches.entity_by_id.get(&table_id).map(|(s, t)| (s.as_str(), t.as_str()))
    }

    pub fn get_by_name(&self, schema_name: &str, table_name: &str) -> Option<i64> {
        let qualified = format!("{}.{}", schema_name, table_name);
        self.caches.entity_by_qname.get(&qualified).copied()
    }

    pub fn has_index_by_name(&self, name: &str) -> bool {
        self.caches.index_by_name.contains_key(name)
    }

    // -- Sequence management -----------------------------------------------

    pub fn advance_sequence(&mut self, seq_id: i64, old_val: i64, new_val: i64) {
        let schema = seq_tab_schema();
        let mut bb = BatchBuilder::new(schema);
        // Retract old
        bb.begin_row(seq_id as u128, -1);
        bb.put_u64(old_val as u64);
        bb.end_row();
        // Insert new
        bb.begin_row(seq_id as u128, 1);
        bb.put_u64(new_val as u64);
        bb.end_row();
        let batch = bb.finish();
        ingest_batch_into(&mut self.sys_sequences, &batch);
    }

    // -- Scan store -------------------------------------------------------

    pub(crate) fn scan_store(&mut self, table_id: i64, schema: &SchemaDescriptor) -> Rc<Batch> {
        let entry = match self.dag.tables.get(&table_id) {
            Some(e) => e,
            None => return Rc::new(Batch::empty_with_schema(schema)),
        };
        entry.handle.open_cursor().cursor.materialize()
    }
}

/// Build the schema for a `gather_family` result: the PK columns of `schema`
/// (in pk-list order, so the packed PK round-trips identically) followed by
/// the projected columns in `project` order as payload. `project` must list
/// only non-PK columns (PK members are resolved from the packed PK without a
/// gather); a projected PK column would be emitted twice.
fn project_schema(schema: &SchemaDescriptor, project: &[u8]) -> SchemaDescriptor {
    let mut cols: Vec<SchemaColumn> = Vec::with_capacity(schema.pk_indices().len() + project.len());
    let mut pk_idx: Vec<u32> = Vec::with_capacity(schema.pk_indices().len());
    for (_, ci, col) in schema.pk_columns() {
        pk_idx.push(cols.len() as u32);
        cols.push(*col);
    }
    for &p in project {
        debug_assert!(!schema.is_pk_col(p as usize),
            "project_schema: projected column {} is a PK column", p);
        cols.push(schema.columns[p as usize]);
    }
    SchemaDescriptor::new(&cols, &pk_idx)
}

/// Projecting sibling of `copy_cursor_row_with_weight`: append the cursor's
/// current row to `out` (which has the `project_schema` layout) with weight 1,
/// copying only the columns named in `project`. The projected payload column
/// at position `k` corresponds to source column `project[k]`; the projected
/// null bit `k` mirrors the source row's null bit for that column. Projected
/// columns are scalar, so no blob relocation is required.
fn copy_cursor_cols_to_batch(
    cursor: &CursorHandle,
    out: &mut Batch,
    src_schema: &SchemaDescriptor,
    project: &[u8],
) {
    // `current_pk_bytes()` is the verbatim OPK PK region for any width, and the
    // read cursor always tracks it regardless of stride. For narrow PKs it is
    // byte-identical to the old `extend_pk(current_key)` round-trip
    // (`current_key == widen_pk_be(current_pk_bytes)`), so one path serves both.
    out.extend_pk_bytes(cursor.cursor.current_pk_bytes());
    out.extend_weight(&1i64.to_le_bytes());

    let src_null = cursor.cursor.current_null_word;
    let mut proj_null = 0u64;
    for (k, &p) in project.iter().enumerate() {
        let pi = src_schema.payload_idx(p as usize);
        if src_null & (1u64 << pi) != 0 {
            proj_null |= 1u64 << k;
        }
    }
    out.extend_null_bmp(&proj_null.to_le_bytes());

    for (k, &p) in project.iter().enumerate() {
        let col_size = src_schema.columns[p as usize].size() as usize;
        let ptr = cursor.cursor.col_ptr(p as usize, col_size);
        if !ptr.is_null() {
            let data = unsafe { std::slice::from_raw_parts(ptr, col_size) };
            out.extend_col(k, data);
        } else {
            out.fill_col_zero(k, col_size);
        }
    }
    out.count += 1;
}
