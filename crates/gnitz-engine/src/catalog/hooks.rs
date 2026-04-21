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
    //   * Client DDL (`gnitz-core/src/client.rs::create_table`): sequential
    //     `conn.push(COL_TAB, ...)` then `conn.push(TABLE_TAB, ...)`. Each
    //     push is a full RPC (ingest + broadcast + fsync + ACK).
    //   * Wire: the SAL is a single FIFO; worker `FLAG_DDL_SYNC` dispatch
    //     preserves master broadcast order.
    //   * Replay (`bootstrap.rs::replay_catalog`): TABLE_TAB is replayed
    //     before COL_TAB, but `read_column_defs` reads sys_columns storage
    //     directly (loaded at open time), not the cache, so the ordering
    //     holds.
    //
    // Retraction dependencies to be aware of:
    //   * apply_entity_by_qname reads entity_by_id on retract → must fire before apply_entity_by_id.

    pub(crate) fn fire_hooks(&mut self, sys_table_id: i64, batch: &Batch) -> Result<(), String> {
        match sys_table_id {
            SCHEMA_TAB_ID => {
                self.apply_schema_by_name(batch)?;
                self.apply_schema_by_id(batch)?;
                self.hook_schema_dir(batch)?;
            }
            TABLE_TAB_ID => {
                self.apply_entity_by_qname(batch)?;
                self.apply_entity_by_id(batch)?;
                self.apply_schema_of(batch)?;
                self.apply_pk_col_of(TABLE_TAB_ID, batch)?;
                self.hook_table_register(batch)?;
                self.apply_needs_lock(TABLE_TAB_ID, batch)?;
                self.hook_cascade_fk(batch)?;
            }
            VIEW_TAB_ID => {
                self.apply_entity_by_qname(batch)?;
                self.apply_entity_by_id(batch)?;
                self.apply_schema_of(batch)?;
                self.apply_pk_col_of(VIEW_TAB_ID, batch)?;
                self.hook_view_register(batch)?;
            }
            COL_TAB_ID => {
                self.apply_col_names_invalidate(batch)?;
                self.apply_fk_by_child(batch)?;
                self.apply_fk_by_parent(batch)?;
                self.apply_needs_lock(COL_TAB_ID, batch)?;
            }
            IDX_TAB_ID => {
                self.apply_index_by_name(batch)?;
                self.apply_index_by_id(batch)?;
                self.hook_index_register(batch)?;
                self.apply_needs_lock(IDX_TAB_ID, batch)?;
            }
            DEP_TAB_ID => {
                self.dag.invalidate_dep_map();
            }
            _ => {}
        }
        Ok(())
    }

    // -- Hook handlers ---------------------------------------------------------

    fn hook_schema_dir(&mut self, batch: &Batch) -> Result<(), String> {
        for i in 0..batch.count {
            let weight = batch.get_weight(i);
            if weight > 0 {
                let name = self.read_batch_string(batch, i, 0);
                let path = format!("{}/{}", self.base_dir, name);
                ensure_dir(&path)?;
                fsync_dir(&self.base_dir);
            }
        }
        Ok(())
    }

    fn hook_table_register(&mut self, batch: &Batch) -> Result<(), String> {
        for i in 0..batch.count {
            let weight = batch.get_weight(i);
            let tid = batch.get_pk(i) as i64;
            let sid = self.read_batch_u64(batch, i, 0) as i64;
            let name = self.read_batch_string(batch, i, 1);
            let pk_col_idx = self.read_batch_u64(batch, i, 3) as u32;
            let flags = self.read_batch_u64(batch, i, 5);
            let is_unique = (flags & TABLETAB_FLAG_UNIQUE_PK) != 0;

            if weight > 0 {
                // System tables are pre-registered by `register_system_table_families`
                // before `replay_catalog` fires hooks, so their rows show up here
                // with the DAG already populated. Skip to avoid double-registration.
                if self.dag.tables.contains_key(&tid) { continue; }

                let col_defs = self.read_column_defs(tid)?;
                if col_defs.is_empty() {
                    return Err(format!(
                        "catalog invariant violated: table '{}' (tid={}) registered \
                         before its column records. COL_TAB writes must precede \
                         TABLE_TAB writes (see hooks.rs dispatch doc).",
                        name, tid,
                    ));
                }

                if (pk_col_idx as usize) < col_defs.len() {
                    let pk_type = col_defs[pk_col_idx as usize].type_code;
                    if pk_type != type_code::U64 && pk_type != type_code::U128 {
                        return Err(format!("Primary Key must be TYPE_U64 or TYPE_U128, got type_code={}", pk_type));
                    }
                }

                let schema_name = self.caches.schema_by_id.get(&sid).cloned().unwrap_or_default();
                let directory = format!("{}/{}/{}_{}", self.base_dir, schema_name, name, tid);
                let tbl_schema = self.build_schema_from_col_defs(&col_defs, pk_col_idx);

                let num_parts = if tid < FIRST_USER_TABLE_ID { 1 } else { NUM_PARTITIONS };
                let arena = partition_arena_size(num_parts);
                gnitz_debug!("catalog: creating table dir={} name={} tid={} parts={}", directory, name, tid, num_parts);
                let pt = PartitionedTable::new(
                    &directory, &name, tbl_schema, tid as u32, num_parts,
                    true, self.active_part_start, self.active_part_end, arena,
                ).map_err(|e| format!("Failed to create table '{}': error {} (dir={})", name, e, directory))?;

                fsync_dir(&format!("{}/{}", self.base_dir, schema_name));
                self.dag.register_table(tid, StoreHandle::Partitioned(Box::new(pt)), tbl_schema, 0, is_unique, directory);
                if tid + 1 > self.next_table_id { self.next_table_id = tid + 1; }
            } else if self.dag.tables.contains_key(&tid) {
                // Safe to cascade unconditionally: precheck_sys_ingest rejects
                // FK/view-dep-blocked drops before the -1 row reaches the WAL.
                self.cascade_retract_indices(tid)?;
                self.cascade_retract_columns(tid)?;
                self.dag.unregister_table(tid);
            }
        }
        Ok(())
    }

    fn cascade_retract_indices(&mut self, owner_id: i64) -> Result<(), String> {
        let schema = sys_tab_schema(IDX_TAB_ID);
        let mut batch = Batch::with_schema(schema, 4);
        {
            let mut cursor = match self.sys_indices.create_cursor() {
                Ok(c) => c,
                Err(_) => return Ok(()),
            };
            while cursor.cursor.valid {
                if cursor.cursor.current_weight > 0
                    && cursor_read_u64(&cursor, IDXTAB_COL_OWNER_ID) as i64 == owner_id
                {
                    copy_cursor_row_with_weight(&cursor, &schema, &mut batch, -1);
                }
                cursor.cursor.advance();
            }
        }
        if batch.count > 0 {
            self.ingest_to_family(IDX_TAB_ID, &batch)?;
        }
        Ok(())
    }

    fn cascade_retract_columns(&mut self, owner_id: i64) -> Result<(), String> {
        let schema = sys_tab_schema(COL_TAB_ID);
        let batch = retract_rows_by_pk_lo_range(
            &mut self.sys_columns, &schema,
            pack_column_id(owner_id, 0),
            pack_column_id(owner_id + 1, 0),
        );
        if batch.count > 0 {
            self.ingest_to_family(COL_TAB_ID, &batch)?;
        }
        Ok(())
    }

    fn hook_view_register(&mut self, batch: &Batch) -> Result<(), String> {
        for i in 0..batch.count {
            let weight = batch.get_weight(i);
            let vid = batch.get_pk(i) as i64;
            let sid = self.read_batch_u64(batch, i, 0) as i64;
            let name = self.read_batch_string(batch, i, 1);

            if weight > 0 {
                // See hook_table_register: system tables are pre-registered, user
                // views are not, so this only skips re-registration on replay.
                if self.dag.tables.contains_key(&vid) { continue; }

                let col_defs = self.read_column_defs(vid)?;
                if col_defs.is_empty() {
                    return Err(format!(
                        "catalog invariant violated: view '{}' (vid={}) registered \
                         before its column records. COL_TAB writes must precede \
                         VIEW_TAB writes (see hooks.rs dispatch doc).",
                        name, vid,
                    ));
                }

                let schema_name = self.caches.schema_by_id.get(&sid).cloned().unwrap_or_default();
                let directory = format!("{}/{}/view_{}_{}", self.base_dir, schema_name, name, vid);
                let view_schema = self.build_schema_from_col_defs(&col_defs, 0);

                let num_parts = if vid < FIRST_USER_TABLE_ID { 1 } else { NUM_PARTITIONS };
                let arena = partition_arena_size(num_parts);
                let et = PartitionedTable::new(
                    &directory, &name, view_schema, vid as u32, num_parts,
                    false, self.active_part_start, self.active_part_end, arena,
                ).map_err(|e| format!("Failed to create view '{}': error {}", name, e))?;

                let source_ids = self.dag.get_source_ids(vid);
                let max_depth = source_ids.iter()
                    .filter_map(|id| self.dag.tables.get(id))
                    .map(|e| e.depth + 1)
                    .max()
                    .unwrap_or(0);

                self.dag.register_table(vid, StoreHandle::Partitioned(Box::new(et)), view_schema, max_depth, false, directory);
                if vid + 1 > self.next_table_id { self.next_table_id = vid + 1; }

                if self.active_part_start != self.active_part_end
                    && self.dag.ensure_compiled(vid)
                    && !self.dag.view_needs_exchange(vid)
                {
                    self.backfill_view(vid);
                }
            } else if self.dag.tables.contains_key(&vid) {
                self.cascade_retract_circuit_and_deps(vid)?;
                self.cascade_retract_columns(vid)?;
                self.dag.unregister_table(vid);
            }
        }
        Ok(())
    }

    fn cascade_retract_circuit_and_deps(&mut self, vid: i64) -> Result<(), String> {
        let pk_hi = vid as u64;
        for tab_id in [
            CIRCUIT_NODES_TAB_ID,
            CIRCUIT_EDGES_TAB_ID,
            CIRCUIT_SOURCES_TAB_ID,
            CIRCUIT_PARAMS_TAB_ID,
            CIRCUIT_GROUP_COLS_TAB_ID,
            DEP_TAB_ID,
        ] {
            let schema = sys_tab_schema(tab_id);
            let batch = {
                let table = self.sys_table_mut(tab_id).unwrap();
                retract_rows_by_pk_hi(table, &schema, pk_hi)
            };
            if batch.count > 0 {
                self.ingest_to_family(tab_id, &batch)?;
            }
        }
        Ok(())
    }

    fn hook_index_register(&mut self, batch: &Batch) -> Result<(), String> {
        for i in 0..batch.count {
            let weight = batch.get_weight(i);
            let idx_id = batch.get_pk(i) as i64;
            let owner_id = self.read_batch_u64(batch, i, 0) as i64;
            let source_col_idx = self.read_batch_u64(batch, i, 2) as u32;
            let name = self.read_batch_string(batch, i, 3);
            let is_unique = self.read_batch_u64(batch, i, 4) != 0;

            if weight > 0 {
                // Use dag presence check since apply_index_by_name may have already fired
                let already_registered = self.dag.tables.get(&owner_id)
                    .map(|e| e.index_circuits.iter().any(|ic| ic.col_idx == source_col_idx))
                    .unwrap_or(false);
                if already_registered { continue; }

                let entry = self.dag.tables.get(&owner_id)
                    .ok_or_else(|| format!("Index: owner table {} not found", owner_id))?;
                let owner_schema = entry.schema;
                let source_col_type = owner_schema.columns[source_col_idx as usize].type_code;
                let source_pk_type = owner_schema.columns[owner_schema.pk_index as usize].type_code;

                let index_key_type = get_index_key_type(source_col_type)?;
                let idx_schema = make_index_schema(index_key_type, source_pk_type);

                let owner_dir = self.dag.tables.get(&owner_id)
                    .map(|e| e.directory.clone()).unwrap_or_default();
                let idx_dir = format!("{}/idx_{}", owner_dir, idx_id);

                let idx_table = Table::new(
                    &idx_dir, &format!("_idx_{}", idx_id), idx_schema, idx_id as u32,
                    SYS_TABLE_ARENA, false,
                ).map_err(|e| format!("Failed to create index table: error {}", e))?;

                let mut idx_table_box = Box::new(idx_table);
                let idx_table_ptr = &mut *idx_table_box as *mut Table;
                self.backfill_index(owner_id, source_col_idx, is_unique, idx_table_ptr, &idx_schema)?;
                self.dag.add_index_circuit(owner_id, source_col_idx, idx_table_box, idx_schema, is_unique);
                let _ = name; // name captured via apply_index_by_name
            } else {
                let is_registered = self.dag.tables.get(&owner_id)
                    .map(|e| e.index_circuits.iter().any(|ic| ic.col_idx == source_col_idx))
                    .unwrap_or(false);
                if is_registered {
                    self.dag.remove_index_circuit(owner_id, source_col_idx);
                }
                let _ = name;
            }
        }
        Ok(())
    }

    fn hook_cascade_fk(&mut self, batch: &Batch) -> Result<(), String> {
        for i in 0..batch.count {
            let weight = batch.get_weight(i);
            if weight <= 0 { continue; }
            let tid = batch.get_pk(i) as i64;
            if self.caches.cascade_enabled && self.dag.tables.contains_key(&tid) {
                self.create_fk_indices(tid)?;
            }
        }
        Ok(())
    }
}
