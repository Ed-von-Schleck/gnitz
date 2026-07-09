use super::*;

impl CatalogEngine {
    // -- Open engine (main entry point) ------------------------------------

    /// Opens or creates a GnitzDB instance at `base_dir`.
    /// Equivalent of `open_engine()`.
    pub fn open(base_dir: &str) -> Result<Self, String> {
        ensure_dir(base_dir)?;

        let sys_dir = format!("{base_dir}/{SYS_CATALOG_DIRNAME}");
        ensure_dir(&sys_dir)?;

        // Create system tables (single-partition; durability derived from the
        // kind they are later registered under).
        let create_sys_table = |info: &SysTabInfo| -> Result<Box<Table>, String> {
            let dir = format!("{}/{}", sys_dir, info.subdir);
            let schema = sys_tab_schema(info.id);
            Table::new(
                &dir,
                schema,
                info.id as u32,
                SYS_TABLE_ARENA,
                RelationKind::SystemCatalog.recovery_source(),
            )
            .map(Box::new)
            .map_err(|e| format!("Failed to create system table '{}': error {}", info.name, e))
        };

        let sys_schemas = create_sys_table(&SYS_TAB_INFOS[0])?;
        let sys_tables = create_sys_table(&SYS_TAB_INFOS[1])?;
        let sys_views = create_sys_table(&SYS_TAB_INFOS[2])?;
        let sys_columns = create_sys_table(&SYS_TAB_INFOS[3])?;
        let sys_indices = create_sys_table(&SYS_TAB_INFOS[4])?;
        let sys_view_deps = create_sys_table(&SYS_TAB_INFOS[5])?;
        let sys_sequences = create_sys_table(&SYS_TAB_INFOS[6])?;
        let sys_circuit_nodes = create_sys_table(&SYS_TAB_INFOS[7])?;
        let sys_circuit_edges = create_sys_table(&SYS_TAB_INFOS[8])?;
        let sys_circuit_node_columns = create_sys_table(&SYS_TAB_INFOS[9])?;

        // Check if this is a fresh database (no table records yet)
        let is_new = !sys_tables.open_cursor().valid;

        let dag = DagEngine::new();

        let mut engine = CatalogEngine {
            dag,
            base_dir: base_dir.to_string(),
            caches: CatalogCacheSet::new(),
            next_schema_id: FIRST_USER_SCHEMA_ID,
            next_table_id: FIRST_USER_TABLE_ID,
            next_index_id: FIRST_USER_INDEX_ID,
            user_sequences: std::collections::HashMap::new(),
            active_part_start: 0,
            active_part_end: NUM_PARTITIONS,
            committed_generation: 0,
            recorded_topology: 0,
            invalid_views: rustc_hash::FxHashSet::default(),
            sys_schemas,
            sys_tables,
            sys_views,
            sys_columns,
            sys_indices,
            sys_view_deps,
            sys_sequences,
            sys_circuit_nodes,
            sys_circuit_edges,
            sys_circuit_node_columns,
            pending_broadcasts: Vec::new(),
            pending_dir_deletions: Vec::new(),
            checkpoint_gated_deletions: Vec::new(),
            ctx: ApplyContext::new(),
            // Rows per chunk for the chunked DDL scans (view + index backfill).
            // `GNITZ_DDL_SCAN_CHUNK_ROWS` overrides the default — chiefly so
            // multi-worker E2E tests can shrink it to force many chunked backfill
            // rounds (lockstep padding, SAL reclaim) over small tables. A 0 or
            // unparseable value falls back to the default (drain_chunk requires
            // max_rows > 0).
            ddl_scan_chunk_rows: std::env::var("GNITZ_DDL_SCAN_CHUNK_ROWS")
                .ok()
                .and_then(|v| v.parse::<usize>().ok())
                .filter(|&n| n > 0)
                .unwrap_or(crate::storage::DDL_SCAN_CHUNK_ROWS),
        };

        if is_new {
            engine.bootstrap_system_tables()?;
        }

        // Phase 1: Recover sequence counters
        engine.recover_sequences();

        // Publish the recovered checkpoint generation so it is COW-inherited by
        // forked workers and stamped into any manifest the master publishes
        // before the first checkpoint bump.
        crate::foundation::worker_ctx::set_committed_generation(engine.committed_generation);

        // Register system table families
        engine.register_system_table_families();

        // Set system table handles on DagEngine
        engine.setup_dag_sys_tables();

        // Phase 2: Replay catalog through hooks
        engine.replay_catalog()?;

        // Boot shard replay done: enter the live phase (see `ApplyContext`).
        engine.ctx.go_live();

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
            bb.begin_row(SYSTEM_SCHEMA_ID as u128, 1);
            bb.put_string("_system");
            bb.end_row();
            // public schema
            bb.begin_row(PUBLIC_SCHEMA_ID as u128, 1);
            bb.put_string("public");
            bb.end_row();
            let batch = bb.finish();
            self.sys_schemas
                .ingest_borrowed_batch(&batch)
                .map_err(|e| format!("bootstrap: sys_schemas ingest failed: {e}"))?;
        }

        // 2. Table records (self-registration of system tables)
        {
            let schema = table_tab_schema();
            let mut bb = BatchBuilder::new(schema);
            for info in SYS_TAB_INFOS {
                let dir = format!("{}/{}", sys_dir, info.subdir);
                bb.begin_row(info.id as u128, 1);
                bb.put_u64(SYSTEM_SCHEMA_ID as u64); // schema_id
                bb.put_string(info.name); // name
                bb.put_string(&dir); // directory
                bb.put_u64(0); // pk_col_idx
                bb.put_u64(0); // created_lsn
                bb.put_u64(0); // flags
                bb.end_row();
            }
            let batch = bb.finish();
            self.sys_tables
                .ingest_borrowed_batch(&batch)
                .map_err(|e| format!("bootstrap: sys_tables ingest failed: {e}"))?;
        }

        // 3. Column records for all system tables
        {
            let schema = col_tab_schema();
            let mut bb = BatchBuilder::new(schema);

            // Each entry: (name, type_code, is_nullable).
            #[allow(clippy::type_complexity)]
            let system_cols: &[(i64, &[(&str, u8, u64)])] = &[
                (
                    SCHEMA_TAB_ID,
                    &[("schema_id", type_code::U64, 0), ("name", type_code::STRING, 0)],
                ),
                (
                    TABLE_TAB_ID,
                    &[
                        ("table_id", type_code::U64, 0),
                        ("schema_id", type_code::U64, 0),
                        ("name", type_code::STRING, 0),
                        ("directory", type_code::STRING, 0),
                        ("pk_col_idx", type_code::U64, 0),
                        ("created_lsn", type_code::U64, 0),
                        ("flags", type_code::U64, 0),
                    ],
                ),
                (
                    VIEW_TAB_ID,
                    &[
                        ("view_id", type_code::U64, 0),
                        ("schema_id", type_code::U64, 0),
                        ("name", type_code::STRING, 0),
                        ("sql_definition", type_code::STRING, 0),
                        ("cache_directory", type_code::STRING, 0),
                        ("created_lsn", type_code::U64, 0),
                        ("pk_col_idx", type_code::U64, 0),
                    ],
                ),
                (
                    COL_TAB_ID,
                    &[
                        ("column_id", type_code::U64, 0),
                        ("owner_id", type_code::U64, 0),
                        ("owner_kind", type_code::U64, 0),
                        ("col_idx", type_code::U64, 0),
                        ("name", type_code::STRING, 0),
                        ("type_code", type_code::U64, 0),
                        ("is_nullable", type_code::U64, 0),
                        ("fk_table_id", type_code::U64, 0),
                        ("fk_col_idx", type_code::U64, 0),
                    ],
                ),
                (
                    IDX_TAB_ID,
                    &[
                        ("index_id", type_code::U64, 0),
                        ("owner_id", type_code::U64, 0),
                        ("owner_kind", type_code::U64, 0),
                        ("source_col_idx", type_code::U64, 0),
                        ("name", type_code::STRING, 0),
                        ("is_unique", type_code::U64, 0),
                        ("cache_directory", type_code::STRING, 0),
                    ],
                ),
                // _view_deps: compound PK (view_id, dep_table_id) at cols [0, 1];
                // dep_view_id is the only payload. All three columns are
                // non-nullable U64.
                (
                    DEP_TAB_ID,
                    &[
                        ("view_id", type_code::U64, 0),
                        ("dep_table_id", type_code::U64, 0),
                        ("dep_view_id", type_code::U64, 0),
                    ],
                ),
                (
                    SEQ_TAB_ID,
                    &[("seq_id", type_code::U64, 0), ("next_val", type_code::U64, 0)],
                ),
                // Circuit tables use a real compound PK (view_id: U64, sub: U64)
                // at cols [0, 1] — two U64 fields, NOT a single synthetic U128 —
                // matching the actual wire schemas (CIRCUIT_*_COLS in gnitz-wire).
                (
                    CIRCUIT_NODES_TAB_ID,
                    &[
                        ("view_id", type_code::U64, 0),
                        ("sub", type_code::U64, 0),
                        ("node_id", type_code::U64, 0),
                        ("opcode", type_code::U64, 0),
                        ("source_table", type_code::U64, 1),
                        ("expr_program", type_code::BLOB, 1),
                    ],
                ),
                (
                    CIRCUIT_EDGES_TAB_ID,
                    &[
                        ("view_id", type_code::U64, 0),
                        ("sub", type_code::U64, 0),
                        ("dst_node", type_code::U64, 0),
                        ("dst_port", type_code::U64, 0),
                        ("src_node", type_code::U64, 0),
                    ],
                ),
                (
                    CIRCUIT_NODE_COLUMNS_TAB_ID,
                    &[
                        ("view_id", type_code::U64, 0),
                        ("sub", type_code::U64, 0),
                        ("node_id", type_code::U64, 0),
                        ("kind", type_code::U64, 0),
                        ("position", type_code::U64, 0),
                        ("value1", type_code::U64, 0),
                        ("value2", type_code::U64, 0),
                    ],
                ),
            ];

            for &(tid, cols) in system_cols {
                for (i, &(name, tcode, nullable)) in cols.iter().enumerate() {
                    let pk = pack_column_id(tid, i as i64);
                    bb.begin_row(pk as u128, 1);
                    bb.put_u64(tid as u64); // owner_id
                    bb.put_u64(OWNER_KIND_TABLE as u64); // owner_kind
                    bb.put_u64(i as u64); // col_idx
                    bb.put_string(name); // name
                    bb.put_u64(tcode as u64); // type_code
                    bb.put_u64(nullable); // is_nullable
                    bb.put_u64(0); // fk_table_id
                    bb.put_u64(0); // fk_col_idx
                    bb.put_u64(0); // is_serial (system columns are never SERIAL)
                    bb.put_u64(0); // is_hidden (system columns are never hidden)
                    bb.end_row();
                }
            }
            let batch = bb.finish();
            self.sys_columns
                .ingest_borrowed_batch(&batch)
                .map_err(|e| format!("bootstrap: sys_columns ingest failed: {e}"))?;
        }

        // 4. Sequence high-water marks
        {
            let schema = seq_tab_schema();
            let mut bb = BatchBuilder::new(schema);
            bb.begin_row(SEQ_ID_SCHEMAS as u128, 1);
            bb.put_u64((FIRST_USER_SCHEMA_ID - 1) as u64);
            bb.end_row();
            bb.begin_row(SEQ_ID_TABLES as u128, 1);
            bb.put_u64((FIRST_USER_TABLE_ID - 1) as u64);
            bb.end_row();
            bb.begin_row(SEQ_ID_INDICES as u128, 1);
            bb.put_u64((FIRST_USER_INDEX_ID - 1) as u64);
            bb.end_row();
            let batch = bb.finish();
            self.sys_sequences
                .ingest_borrowed_batch(&batch)
                .map_err(|e| format!("bootstrap: sys_sequences ingest failed: {e}"))?;
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
        let mut cursor = self.sys_sequences.open_cursor();
        while cursor.valid {
            if cursor.current_weight > 0 {
                let seq_id = cursor.current_key_narrow() as u64 as i64;
                let val = cursor_read_u64(&cursor, SEQTAB_COL_VALUE) as i64;
                match seq_id {
                    SEQ_ID_SCHEMAS => raise_id_counter(&mut self.next_schema_id, val),
                    SEQ_ID_TABLES => raise_id_counter(&mut self.next_table_id, val),
                    SEQ_ID_INDICES => raise_id_counter(&mut self.next_index_id, val),
                    // Checkpoint generation is monotonic; a mid-checkpoint crash
                    // may leave two rows, so take the max. Topology is a single
                    // latest-wins value. Both fall in the 4..16 gap
                    // `observe_user_sequence` ignores, so they never leak into
                    // `user_sequences`.
                    SEQ_ID_CHECKPOINT_GEN => self.committed_generation = self.committed_generation.max(val as u64),
                    SEQ_ID_TOPOLOGY => self.recorded_topology = val as u64,
                    // User-table SERIAL sequence (seq_id == table_id ≥
                    // FIRST_USER_TABLE_ID). Store the high-water; next id =
                    // high_water + 1. `observe_user_sequence` ignores a stray
                    // catalog-range seq_id in the empty 4..16 gap, so it is never
                    // misclassified as a user sequence.
                    other => self.observe_user_sequence(other, val),
                }
            }
            cursor.advance();
        }
    }

    // -- Register system table families ------------------------------------

    fn register_system_table_families(&mut self) {
        let sys_dir = format!("{}/{}", self.base_dir, SYS_CATALOG_DIRNAME);

        // Collect system table pointers (order matches SYS_TAB_INFOS)
        let table_ptrs: [*mut Table; 10] = [
            &mut *self.sys_schemas,
            &mut *self.sys_tables,
            &mut *self.sys_views,
            &mut *self.sys_columns,
            &mut *self.sys_indices,
            &mut *self.sys_view_deps,
            &mut *self.sys_sequences,
            &mut *self.sys_circuit_nodes,
            &mut *self.sys_circuit_edges,
            &mut *self.sys_circuit_node_columns,
        ];

        self.caches.schema_by_name.insert("_system".into(), SYSTEM_SCHEMA_ID);
        self.caches.schema_by_id.insert(SYSTEM_SCHEMA_ID, "_system".into());

        for (i, info) in SYS_TAB_INFOS.iter().enumerate() {
            let dir = format!("{}/{}", sys_dir, info.subdir);
            let qualified = format!("_system.{}", info.name);
            self.caches.entity_by_qname.insert(qualified, info.id);
            self.caches
                .entity_by_id
                .insert(info.id, ("_system".into(), info.name.into()));
            self.caches.schema_of.insert(info.id, SYSTEM_SCHEMA_ID);
            self.caches.pk_col_of.insert(info.id, PkColList::single(0));
            let schema = sys_tab_schema(info.id);
            self.dag.register_table(
                info.id,
                StoreHandle::Borrowed(table_ptrs[i]),
                schema,
                RelationKind::SystemCatalog,
                0,
                dir,
            );
        }
    }

    // -- Setup DagEngine system table references ---------------------------

    fn setup_dag_sys_tables(&mut self) {
        use crate::query::SysTableRefs;
        self.dag.set_sys_tables(SysTableRefs {
            nodes: &mut *self.sys_circuit_nodes as *mut Table,
            edges: &mut *self.sys_circuit_edges as *mut Table,
            node_columns: &mut *self.sys_circuit_node_columns as *mut Table,
            dep_tab: &mut *self.sys_view_deps as *mut Table,
        });
    }

    // -- Replay catalog (recovery) -----------------------------------------

    fn replay_catalog(&mut self) -> Result<(), String> {
        self.replay_system_table(SCHEMA_TAB_ID)?;
        self.replay_system_table(TABLE_TAB_ID)?;
        self.replay_system_table(VIEW_TAB_ID)?;
        self.replay_system_table(COL_TAB_ID)?; // FK wiring + col_names invalidation
        self.replay_system_table(IDX_TAB_ID)?;
        Ok(())
    }

    fn replay_system_table(&mut self, sys_table_id: i64) -> Result<(), String> {
        // Only the five catalog tables below need hook-driven replay. Circuit_*
        // and sys_sequences are loaded directly by other open-time paths.
        match sys_table_id {
            SCHEMA_TAB_ID | TABLE_TAB_ID | VIEW_TAB_ID | COL_TAB_ID | IDX_TAB_ID => {}
            _ => return Ok(()),
        }
        let family = SysFamily::from_id(sys_table_id).expect("from_id covers the match arms above");
        let table = self
            .sys_table_mut(sys_table_id)
            .expect("sys_table_mut covers the match arms above");
        let arc = table.full_scan();
        if arc.count > 0 {
            self.fire_hooks(family, &arc)?;
        }
        Ok(())
    }

    pub(crate) fn copy_cursor_row_to_batch(&self, cursor: &ReadCursor, batch: &mut Batch) {
        cursor.copy_current_row_into(batch, cursor.current_weight);
    }

    // -- Close engine ------------------------------------------------------

    /// Flush all system tables (memtable → shard). Called at checkpoint and close.
    /// Returns the first failure (with the offending sys table id) so the boot
    /// path can abort before the SAL — the only durable copy of replayed DDL —
    /// is reset.
    pub fn flush_all_system_tables(&mut self) -> Result<(), String> {
        for info in SYS_TAB_INFOS {
            if let Some(table) = self.sys_table_mut(info.id) {
                // System tables are `SalReplay`, so `flush()` folds memtable + L0
                // and writes a durable shard synchronously.
                table
                    .flush()
                    .map_err(|e| format!("boot flush of system table {} failed: {}", info.id, e))?;
            }
        }
        Ok(())
    }

    /// Graceful close for tests; the server never closes the catalog (it
    /// flushes durably per zone and exits via abort or process teardown).
    #[cfg(test)]
    pub(crate) fn close(&mut self) {
        // Flush all user tables before clearing DagEngine. System tables hold
        // Borrowed handles and are flushed below.
        for entry in self.dag.tables.values_mut() {
            if let StoreHandle::Partitioned(cell) = &mut entry.handle {
                let _ = cell.get_mut().flush();
            }
        }
        // tables.clear() in dag.close() drops Box<PartitionedTable> automatically.
        self.dag.close();
        let _ = self.flush_all_system_tables();
    }
}
