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

    /// Reject FK-blocked or view-dep-blocked drops before they hit the WAL.
    /// Only TABLE_TAB and VIEW_TAB retractions have pre-conditions; other
    /// system tables (including cascade targets) are no-ops.
    fn precheck_sys_ingest(&mut self, table_id: i64, batch: &Batch) -> Result<(), String> {
        if table_id != TABLE_TAB_ID && table_id != VIEW_TAB_ID && table_id != IDX_TAB_ID {
            return Ok(());
        }

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
                    cursor.cursor.seek(crate::util::make_pk(idx_id as u64, 0));
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
                if let Some(r) = self.fk_children_of(tid).first() {
                    let (sn, tn) = self.caches.entity_by_id.get(&r.child_tid)
                        .cloned().unwrap_or_default();
                    return Err(format!(
                        "Integrity violation: table referenced by '{}.{}'", sn, tn
                    ));
                }
            }
        }

        let dep_map = self.dag.get_dep_map();
        for &id in &drop_ids {
            if let Some(dependents) = dep_map.get(&id) {
                // A dependent that is itself being dropped in this same batch
                // is self-resolving — only an *outside* dependent blocks the
                // drop. drop_ids is sorted+deduped above, so binary_search is
                // O(N log M) vs the O(N·M) of `contains`.
                let still_active = dependents.iter()
                    .any(|&dep_id| drop_ids.binary_search(&dep_id).is_err());
                if still_active {
                    let (sn, tn) = self.caches.entity_by_id.get(&id)
                        .cloned().unwrap_or_default();
                    return Err(format!("View dependency: entity '{}.{}'", sn, tn));
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

        cursor.cursor.seek(pk);
        if !cursor.cursor.valid { return Ok(None); }
        if cursor.cursor.current_key != pk {
            return Ok(None);
        }
        if cursor.cursor.current_weight <= 0 { return Ok(None); }

        let mut batch = Batch::with_schema(schema, 1);
        self.copy_cursor_row_to_batch(&cursor, &mut batch);
        Ok(Some(batch))
    }

    /// Batched point lookup. Open one cursor on `table_id` and seek each PK in
    /// `pks`, appending the stored row (weight 1) for every present, live key
    /// into a result batch projected to `project`. Each `seek` re-probes every
    /// source independently, so order is not required for correctness; passing
    /// `pks` ascending keeps the per-source binary-search probes monotonic for
    /// better cache locality.
    /// Absent / retracted keys are skipped — identical to `seek_family`'s
    /// single-key `None` — so a removed PK with no committed row contributes
    /// nothing. `project` lists the parent column indices to return (all
    /// non-PK scalar columns); an empty `project` returns PK-only rows.
    ///
    /// Reuses one cursor across all keys (cheaper than N `seek_family` calls,
    /// each of which re-opens a cursor). Projection keeps the result scalar-
    /// only — FK-referenced columns are never STRING/BLOB — so the blob arena
    /// is never touched.
    pub fn gather_family(
        &mut self,
        table_id: i64,
        pks: &[u128],
        project: &[u8],
    ) -> Result<Batch, String> {
        let schema = self.dag.tables.get(&table_id)
            .map(|e| e.schema)
            .ok_or_else(|| format!("Unknown table_id {}", table_id))?;
        let result_schema = project_schema(&schema, project);
        let mut out = Batch::with_schema(result_schema, pks.len());
        let mut cursor = self.dag.tables.get(&table_id).unwrap().handle.open_cursor();
        for &pk in pks {
            cursor.cursor.seek(pk);
            if !cursor.cursor.valid { continue; }
            if cursor.cursor.current_key != pk { continue; }
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
        if src_pk_stride > 16 {
            return Err("wide-PK source not yet supported in seek_by_index".to_string());
        }
        let idx_key_size = ic.index_schema.columns[0].size() as usize;

        let idx_table = ic.table_mut();
        let mut cursor = idx_table.open_cursor();

        // Seek to the first positive-weight match, then walk forward with
        // `walk_to_positive_with_prefix` after each consumed entry. Re-calling
        // `seek_first_positive_with_prefix` inside the loop would re-seek and
        // re-find the same entry forever — an orphaned index entry (positive
        // weight, no source row) would spin.
        let mut hit = cursor.cursor.seek_first_positive_with_prefix(prefix);
        while hit {
            let current_pk = cursor.cursor.current_pk_bytes();
            let mut src_pk_buf = [0u8; 16];
            src_pk_buf[..src_pk_stride].copy_from_slice(
                &current_pk[idx_key_size..idx_key_size + src_pk_stride],
            );
            let src_pk = u128::from_le_bytes(src_pk_buf);
            if let Some(batch) = self.seek_family(table_id, src_pk)? {
                return Ok(Some(batch));
            }
            cursor.cursor.advance();
            hit = cursor.cursor.walk_to_positive_with_prefix(prefix);
        }
        Ok(None)
    }

    /// Flush a table's WAL.
    pub fn flush_family(&mut self, table_id: i64) -> Result<(), String> {
        if table_id < FIRST_USER_TABLE_ID {
            if let Some(table) = self.sys_table_mut(table_id) {
                table.flush().map_err(|e| format!("flush error: {}", e))?;
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
            .map(|ic| std::ptr::addr_of!(*ic.index_table))
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
                StoreHandle::Partitioned(ref pt) => Some(std::ptr::addr_of!(**pt) as *mut PartitionedTable),
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

    pub(crate) fn read_column_defs(&self, owner_id: i64) -> Vec<ColumnDef> {
        let start_pk = pack_column_id(owner_id, 0);
        let end_pk = pack_column_id(owner_id + 1, 0);
        let mut cursor = self.sys_columns.open_cursor();
        cursor.cursor.seek(start_pk as u128);

        let mut defs = Vec::new();
        while cursor.cursor.valid {
            let pk = cursor.cursor.current_key as u64;
            if pk >= end_pk { break; }
            if cursor.cursor.current_weight > 0 {
                let type_code = cursor_read_u64(&cursor, COLTAB_COL_TYPE_CODE) as u8;
                let is_nullable = cursor_read_u64(&cursor, COLTAB_COL_IS_NULLABLE) != 0;
                let name = cursor_read_string(&cursor, COLTAB_COL_NAME);
                let fk_table_id = cursor_read_u64(&cursor, COLTAB_COL_FK_TABLE_ID) as i64;
                let fk_col_idx = cursor_read_u64(&cursor, COLTAB_COL_FK_COL_IDX) as u32;
                defs.push(ColumnDef {
                    name,
                    type_code,
                    is_nullable,
                    fk_table_id,
                    fk_col_idx,
                });
            }
            cursor.cursor.advance();
        }
        defs
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
    out.extend_pk(cursor.cursor.current_key);
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
