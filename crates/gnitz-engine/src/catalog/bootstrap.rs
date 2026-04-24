use super::*;

impl CatalogEngine {
    // -- Open engine (main entry point) ------------------------------------

    /// Opens or creates a GnitzDB instance at `base_dir`.
    /// Equivalent of `open_engine()`.
    pub fn open(base_dir: &str) -> Result<Self, String> {
        ensure_dir(base_dir)?;

        let sys_dir = format!("{}/{}", base_dir, SYS_CATALOG_DIRNAME);
        ensure_dir(&sys_dir)?;

        // Create system tables (persistent, single-partition)
        let create_sys_table = |info: &SysTabInfo| -> Result<Box<Table>, String> {
            let dir = format!("{}/{}", sys_dir, info.subdir);
            let schema = sys_tab_schema(info.id);
            Table::new(&dir, info.name, schema, info.id as u32, SYS_TABLE_ARENA, true)
                .map(Box::new)
                .map_err(|e| format!("Failed to create system table '{}': error {}", info.name, e))
        };

        let sys_schemas = create_sys_table(&SYS_TAB_INFOS[0])?;
        let mut sys_tables = create_sys_table(&SYS_TAB_INFOS[1])?;
        let sys_views = create_sys_table(&SYS_TAB_INFOS[2])?;
        let sys_columns = create_sys_table(&SYS_TAB_INFOS[3])?;
        let sys_indices = create_sys_table(&SYS_TAB_INFOS[4])?;
        let sys_view_deps = create_sys_table(&SYS_TAB_INFOS[5])?;
        let sys_sequences = create_sys_table(&SYS_TAB_INFOS[6])?;
        let sys_circuit_nodes = create_sys_table(&SYS_TAB_INFOS[7])?;
        let sys_circuit_edges = create_sys_table(&SYS_TAB_INFOS[8])?;
        let sys_circuit_sources = create_sys_table(&SYS_TAB_INFOS[9])?;
        let sys_circuit_params = create_sys_table(&SYS_TAB_INFOS[10])?;
        let sys_circuit_group_cols = create_sys_table(&SYS_TAB_INFOS[11])?;

        // Check if this is a fresh database (no table records yet)
        let is_new = {
            match sys_tables.create_cursor() {
                Ok(cursor) => !cursor.cursor.valid,
                Err(_) => true,
            }
        };

        let dag = DagEngine::new();

        let mut engine = CatalogEngine {
            dag,
            base_dir: base_dir.to_string(),
            caches: CatalogCacheSet::new(),
            next_schema_id: FIRST_USER_SCHEMA_ID,
            next_table_id: FIRST_USER_TABLE_ID,
            next_index_id: FIRST_USER_INDEX_ID,
            active_part_start: 0,
            active_part_end: NUM_PARTITIONS,
            sys_schemas,
            sys_tables,
            sys_views,
            sys_columns,
            sys_indices,
            sys_view_deps,
            sys_sequences,
            sys_circuit_nodes,
            sys_circuit_edges,
            sys_circuit_sources,
            sys_circuit_params,
            sys_circuit_group_cols,
            pending_broadcasts: Vec::new(),
        };

        if is_new {
            engine.bootstrap_system_tables()?;
        }

        // Phase 1: Recover sequence counters
        engine.recover_sequences();

        // Register system table families
        engine.register_system_table_families();

        // Set system table handles on DagEngine
        engine.setup_dag_sys_tables();

        // Phase 2: Replay catalog through hooks
        engine.replay_catalog()?;

        // Enable cascading effects (e.g. auto-create FK indices)
        engine.caches.cascade_enabled = true;

        Ok(engine)
    }

    // -- Bootstrap (fresh database) ----------------------------------------

    fn bootstrap_system_tables(&mut self) -> Result<(), String> {
        let sys_dir = format!("{}/{}", self.base_dir, SYS_CATALOG_DIRNAME);

        // 1. Core schema records
        {
            let schema = schema_tab_schema();
            let mut bb = BatchBuilder::new(schema);
            // _system schema
            bb.begin_row(SYSTEM_SCHEMA_ID as u64, 0, 1);
            bb.put_string("_system");
            bb.end_row();
            // public schema
            bb.begin_row(PUBLIC_SCHEMA_ID as u64, 0, 1);
            bb.put_string("public");
            bb.end_row();
            let batch = bb.finish();
            ingest_batch_into(&mut self.sys_schemas, &batch);
        }

        // 2. Table records (self-registration of system tables)
        {
            let schema = table_tab_schema();
            let mut bb = BatchBuilder::new(schema);
            for info in SYS_TAB_INFOS {
                let dir = format!("{}/{}", sys_dir, info.subdir);
                bb.begin_row(info.id as u64, 0, 1);
                bb.put_u64(SYSTEM_SCHEMA_ID as u64);  // schema_id
                bb.put_string(info.name);               // name
                bb.put_string(&dir);                    // directory
                bb.put_u64(0);                          // pk_col_idx
                bb.put_u64(0);                          // created_lsn
                bb.put_u64(0);                          // flags
                bb.end_row();
            }
            let batch = bb.finish();
            ingest_batch_into(&mut self.sys_tables, &batch);
        }

        // 3. Column records for all system tables
        {
            let schema = col_tab_schema();
            let mut bb = BatchBuilder::new(schema);

            let system_cols: &[(i64, &[(& str, u8)])] = &[
                (SCHEMA_TAB_ID, &[("schema_id", type_code::U64), ("name", type_code::STRING)]),
                (TABLE_TAB_ID, &[
                    ("table_id", type_code::U64), ("schema_id", type_code::U64),
                    ("name", type_code::STRING), ("directory", type_code::STRING),
                    ("pk_col_idx", type_code::U64), ("created_lsn", type_code::U64),
                    ("flags", type_code::U64),
                ]),
                (VIEW_TAB_ID, &[
                    ("view_id", type_code::U64), ("schema_id", type_code::U64),
                    ("name", type_code::STRING), ("sql_definition", type_code::STRING),
                    ("cache_directory", type_code::STRING), ("created_lsn", type_code::U64),
                ]),
                (COL_TAB_ID, &[
                    ("column_id", type_code::U64), ("owner_id", type_code::U64),
                    ("owner_kind", type_code::U64), ("col_idx", type_code::U64),
                    ("name", type_code::STRING), ("type_code", type_code::U64),
                    ("is_nullable", type_code::U64), ("fk_table_id", type_code::U64),
                    ("fk_col_idx", type_code::U64),
                ]),
                (IDX_TAB_ID, &[
                    ("index_id", type_code::U64), ("owner_id", type_code::U64),
                    ("owner_kind", type_code::U64), ("source_col_idx", type_code::U64),
                    ("name", type_code::STRING), ("is_unique", type_code::U64),
                    ("cache_directory", type_code::STRING),
                ]),
                (SEQ_TAB_ID, &[("seq_id", type_code::U64), ("next_val", type_code::U64)]),
                (CIRCUIT_NODES_TAB_ID, &[("node_pk", type_code::U128), ("opcode", type_code::U64)]),
                (CIRCUIT_EDGES_TAB_ID, &[
                    ("edge_pk", type_code::U128), ("src_node", type_code::U64),
                    ("dst_node", type_code::U64), ("dst_port", type_code::U64),
                ]),
                (CIRCUIT_SOURCES_TAB_ID, &[("source_pk", type_code::U128), ("table_id", type_code::U64)]),
                (CIRCUIT_PARAMS_TAB_ID, &[
                    ("param_pk", type_code::U128), ("value", type_code::U64),
                    ("str_value", type_code::STRING),
                ]),
                (CIRCUIT_GROUP_COLS_TAB_ID, &[("gcol_pk", type_code::U128), ("col_idx", type_code::U64)]),
            ];

            for &(tid, cols) in system_cols {
                for (i, &(name, tcode)) in cols.iter().enumerate() {
                    let pk = pack_column_id(tid, i as i64);
                    bb.begin_row(pk, 0, 1);
                    bb.put_u64(tid as u64);          // owner_id
                    bb.put_u64(OWNER_KIND_TABLE as u64); // owner_kind
                    bb.put_u64(i as u64);            // col_idx
                    bb.put_string(name);             // name
                    bb.put_u64(tcode as u64);        // type_code
                    bb.put_u64(0);                   // is_nullable
                    bb.put_u64(0);                   // fk_table_id
                    bb.put_u64(0);                   // fk_col_idx
                    bb.end_row();
                }
            }
            let batch = bb.finish();
            ingest_batch_into(&mut self.sys_columns, &batch);
        }

        // 4. Sequence high-water marks
        {
            let schema = seq_tab_schema();
            let mut bb = BatchBuilder::new(schema);
            bb.begin_row(SEQ_ID_SCHEMAS as u64, 0, 1);
            bb.put_u64((FIRST_USER_SCHEMA_ID - 1) as u64);
            bb.end_row();
            bb.begin_row(SEQ_ID_TABLES as u64, 0, 1);
            bb.put_u64((FIRST_USER_TABLE_ID - 1) as u64);
            bb.end_row();
            bb.begin_row(SEQ_ID_INDICES as u64, 0, 1);
            bb.put_u64((FIRST_USER_INDEX_ID - 1) as u64);
            bb.end_row();
            bb.begin_row(SEQ_ID_PROGRAMS as u64, 0, 1);
            bb.put_u64((FIRST_USER_TABLE_ID - 1) as u64);
            bb.end_row();
            let batch = bb.finish();
            ingest_batch_into(&mut self.sys_sequences, &batch);
        }

        // Flush all foundational metadata to disk
        let _ = self.sys_schemas.flush();
        let _ = self.sys_tables.flush();
        let _ = self.sys_columns.flush();
        let _ = self.sys_sequences.flush();

        Ok(())
    }

    // -- Recover sequence counters from sys_sequences ----------------------

    fn recover_sequences(&mut self) {
        let mut cursor = match self.sys_sequences.create_cursor() {
            Ok(c) => c,
            Err(_) => return,
        };
        while cursor.cursor.valid {
            if cursor.cursor.current_weight > 0 {
                let seq_id = cursor.cursor.current_key_lo as i64;
                let val = cursor_read_u64(&cursor, SEQTAB_COL_VALUE) as i64;
                match seq_id {
                    SEQ_ID_SCHEMAS => {
                        let next = val + 1;
                        if next > self.next_schema_id { self.next_schema_id = next; }
                    }
                    SEQ_ID_TABLES => {
                        let next = val + 1;
                        if next > self.next_table_id { self.next_table_id = next; }
                    }
                    SEQ_ID_INDICES => {
                        let next = val + 1;
                        if next > self.next_index_id { self.next_index_id = next; }
                    }
                    SEQ_ID_PROGRAMS => {
                        let next = val + 1;
                        if next > self.next_table_id { self.next_table_id = next; }
                    }
                    _ => {}
                }
            }
            cursor.cursor.advance();
        }
    }

    // -- Register system table families ------------------------------------

    fn register_system_table_families(&mut self) {
        let sys_dir = format!("{}/{}", self.base_dir, SYS_CATALOG_DIRNAME);

        // Collect system table pointers (order matches SYS_TAB_INFOS)
        let table_ptrs: [*mut Table; 12] = [
            &mut *self.sys_schemas,
            &mut *self.sys_tables,
            &mut *self.sys_views,
            &mut *self.sys_columns,
            &mut *self.sys_indices,
            &mut *self.sys_view_deps,
            &mut *self.sys_sequences,
            &mut *self.sys_circuit_nodes,
            &mut *self.sys_circuit_edges,
            &mut *self.sys_circuit_sources,
            &mut *self.sys_circuit_params,
            &mut *self.sys_circuit_group_cols,
        ];

        self.caches.schema_by_name.insert("_system".into(), SYSTEM_SCHEMA_ID);
        self.caches.schema_by_id.insert(SYSTEM_SCHEMA_ID, "_system".into());

        for (i, info) in SYS_TAB_INFOS.iter().enumerate() {
            let dir = format!("{}/{}", sys_dir, info.subdir);
            let qualified = format!("_system.{}", info.name);
            self.caches.entity_by_qname.insert(qualified, info.id);
            self.caches.entity_by_id.insert(info.id, ("_system".into(), info.name.into()));
            self.caches.schema_of.insert(info.id, SYSTEM_SCHEMA_ID);
            self.caches.pk_col_of.insert(info.id, 0);
            let schema = sys_tab_schema(info.id);
            self.dag.register_table(
                info.id,
                StoreHandle::Borrowed(table_ptrs[i]),
                schema,
                0,
                false,
                dir,
            );
        }
    }

    // -- Setup DagEngine system table references ---------------------------

    fn setup_dag_sys_tables(&mut self) {
        use crate::dag::SysTableRefs;
        self.dag.set_sys_tables(SysTableRefs {
            nodes: &mut *self.sys_circuit_nodes as *mut Table,
            edges: &mut *self.sys_circuit_edges as *mut Table,
            sources: &mut *self.sys_circuit_sources as *mut Table,
            params: &mut *self.sys_circuit_params as *mut Table,
            group_cols: &mut *self.sys_circuit_group_cols as *mut Table,
            dep_tab: &mut *self.sys_view_deps as *mut Table,
            nodes_schema: circuit_nodes_schema(),
            edges_schema: circuit_edges_schema(),
            sources_schema: circuit_sources_schema(),
            params_schema: circuit_params_schema(),
            group_cols_schema: circuit_group_cols_schema(),
            dep_tab_schema: dep_tab_schema(),
        });
    }

    // -- Replay catalog (recovery) -----------------------------------------

    fn replay_catalog(&mut self) -> Result<(), String> {
        self.replay_system_table(SCHEMA_TAB_ID)?;
        self.replay_system_table(TABLE_TAB_ID)?;
        self.replay_system_table(VIEW_TAB_ID)?;
        self.replay_system_table(COL_TAB_ID)?;   // FK wiring + col_names invalidation
        self.replay_system_table(IDX_TAB_ID)?;
        Ok(())
    }

    fn replay_system_table(&mut self, sys_table_id: i64) -> Result<(), String> {
        let schema = sys_tab_schema(sys_table_id);
        // Only the five catalog tables below need hook-driven replay. Circuit_*
        // and sys_sequences are loaded directly by other open-time paths.
        match sys_table_id {
            SCHEMA_TAB_ID | TABLE_TAB_ID | VIEW_TAB_ID | COL_TAB_ID | IDX_TAB_ID => {}
            _ => return Ok(()),
        }
        let table_ptr: *mut Table = self.sys_table_mut(sys_table_id)
            .expect("sys_table_mut covers the match arms above")
            as *mut Table;

        // Scan all live rows and process through hooks
        let mut cursor = unsafe { (*table_ptr).create_cursor().map_err(|e| format!("cursor error: {}", e))? };
        let mut batch = Batch::with_schema(schema, 512);
        let mut count = 0;

        while cursor.cursor.valid {
            if cursor.cursor.current_weight > 0 {
                // Copy row into batch
                self.copy_cursor_row_to_batch(&cursor, &mut batch);
                count += 1;

                if count >= 512 {
                    self.fire_hooks(sys_table_id, &batch)?;
                    batch = Batch::with_schema(schema, 512);
                    count = 0;
                }
            }
            cursor.cursor.advance();
        }

        if count > 0 {
            self.fire_hooks(sys_table_id, &batch)?;
        }

        Ok(())
    }

    pub(crate) fn copy_cursor_row_to_batch(
        &self,
        cursor: &CursorHandle,
        batch: &mut Batch,
    ) {
        copy_cursor_row_with_weight(cursor, batch, cursor.cursor.current_weight);
    }

    // -- Close engine ------------------------------------------------------

    /// Flush all system tables (memtable → shard). Called at checkpoint and close.
    pub fn flush_all_system_tables(&mut self) {
        for info in SYS_TAB_INFOS {
            if let Some(table) = self.sys_table_mut(info.id) {
                let _ = table.flush_durable();
            }
        }
    }

    pub fn close(&mut self) {
        // Flush and close all user tables before clearing DagEngine
        for (&tid, entry) in self.dag.tables.iter_mut() {
            if tid < FIRST_USER_TABLE_ID { continue; }
            match &mut entry.handle {
                StoreHandle::Single(t) => { let _ = t.flush(); }
                StoreHandle::Partitioned(pt) => { let _ = pt.flush(); pt.close(); }
                StoreHandle::Borrowed(_) => {} // system tables flushed below
            }
        }
        // tables.clear() in dag.close() drops Box<Table>/Box<PartitionedTable> automatically.
        self.dag.close();
        self.flush_all_system_tables();
    }
}
