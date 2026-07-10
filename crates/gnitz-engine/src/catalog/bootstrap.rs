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
        let mut stores = Vec::with_capacity(SysFamily::COUNT);
        for info in &SYS_FAMILIES {
            let table = Table::new(
                &format!("{}/{}", sys_dir, info.name),
                sys_tab_schema(info.id),
                info.id as u32,
                SYS_TABLE_ARENA,
                RelationKind::SystemCatalog.recovery_source(),
            )
            .map(Box::new)
            .map_err(|e| format!("Failed to create system table '{}': error {}", info.name, e))?;
            stores.push(table);
        }
        let sys_stores: [Box<Table>; SysFamily::COUNT] = stores
            .try_into()
            .unwrap_or_else(|_| unreachable!("one store per family"));

        // Check if this is a fresh database (no table records yet)
        let is_new = !sys_stores[SysFamily::Table.index()].open_cursor().valid;

        let dag = DagEngine::new();

        let mut engine = CatalogEngine {
            dag,
            base_dir: base_dir.to_string(),
            caches: CatalogCacheSet::default(),
            next_schema_id: FIRST_USER_SCHEMA_ID,
            next_table_id: FIRST_USER_TABLE_ID,
            next_index_id: FIRST_USER_INDEX_ID,
            user_sequences: std::collections::HashMap::new(),
            active_part_start: 0,
            active_part_end: NUM_PARTITIONS,
            committed_generation: 0,
            recorded_topology: 0,
            invalid_views: rustc_hash::FxHashSet::default(),
            sys_stores,
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
            let mut bb = BatchBuilder::new(SysFamily::Schema.schema());
            // _system schema
            bb.begin_row(SYSTEM_SCHEMA_ID as u128, 1);
            bb.put_string("_system");
            bb.end_row();
            // public schema
            bb.begin_row(PUBLIC_SCHEMA_ID as u128, 1);
            bb.put_string("public");
            bb.end_row();
            let batch = bb.finish();
            self.sys_store_mut(SysFamily::Schema)
                .ingest_borrowed_batch(&batch)
                .map_err(|e| format!("bootstrap: sys_schemas ingest failed: {e}"))?;
        }

        // 2. Table records (self-registration of system tables)
        {
            let mut bb = BatchBuilder::new(SysFamily::Table.schema());
            for info in &SYS_FAMILIES {
                let dir = format!("{}/{}", sys_dir, info.name);
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
            self.sys_store_mut(SysFamily::Table)
                .ingest_borrowed_batch(&batch)
                .map_err(|e| format!("bootstrap: sys_tables ingest failed: {e}"))?;
        }

        // 3. Column records for all system tables — the COL_TAB self-description,
        // derived from the same gnitz-wire slices the schemas are built from, so
        // the introspectable shape can never drift from the physical one.
        {
            let mut bb = BatchBuilder::new(SysFamily::Column.schema());
            for info in &SYS_FAMILIES {
                for (i, c) in info.cols.iter().enumerate() {
                    let pk = pack_column_id(info.id, i as i64);
                    bb.begin_row(pk as u128, 1);
                    bb.put_u64(info.id as u64); // owner_id
                    bb.put_u64(OWNER_KIND_TABLE as u64); // owner_kind
                    bb.put_u64(i as u64); // col_idx
                    bb.put_string(c.name); // name
                    bb.put_u64(c.type_code as u64); // type_code
                    bb.put_u64(c.nullable as u64); // is_nullable
                    bb.put_u64(0); // fk_table_id
                    bb.put_u64(0); // fk_col_idx
                    bb.put_u64(0); // is_serial (system columns are never SERIAL)
                    bb.put_u64(0); // is_hidden (system columns are never hidden)
                    bb.end_row();
                }
            }
            let batch = bb.finish();
            self.sys_store_mut(SysFamily::Column)
                .ingest_borrowed_batch(&batch)
                .map_err(|e| format!("bootstrap: sys_columns ingest failed: {e}"))?;
        }

        // 4. Sequence high-water marks
        {
            let mut bb = BatchBuilder::new(SysFamily::Sequence.schema());
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
            self.sys_store_mut(SysFamily::Sequence)
                .ingest_borrowed_batch(&batch)
                .map_err(|e| format!("bootstrap: sys_sequences ingest failed: {e}"))?;
        }

        // Flush the foundational metadata to disk. Deliberately partial: only
        // the four families bootstrap wrote above.
        let _ = self.sys_store_mut(SysFamily::Schema).flush();
        let _ = self.sys_store_mut(SysFamily::Table).flush();
        let _ = self.sys_store_mut(SysFamily::Column).flush();
        let _ = self.sys_store_mut(SysFamily::Sequence).flush();

        Ok(())
    }

    // -- Recover sequence counters from sys_sequences ----------------------

    fn recover_sequences(&mut self) {
        let mut cursor = self.sys_store(SysFamily::Sequence).open_cursor();
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

        self.caches.schema_by_name.insert("_system".into(), SYSTEM_SCHEMA_ID);
        self.caches.schema_by_id.insert(SYSTEM_SCHEMA_ID, "_system".into());

        for (info, store) in SYS_FAMILIES.iter().zip(self.sys_stores.iter_mut()) {
            let dir = format!("{}/{}", sys_dir, info.name);
            let qualified = format!("_system.{}", info.name);
            self.caches.entity_by_qname.insert(qualified, info.id);
            self.caches
                .entity_by_id
                .insert(info.id, ("_system".into(), info.name.into()));
            self.caches.schema_of.insert(info.id, SYSTEM_SCHEMA_ID);
            self.caches.pk_col_of.insert(info.id, PkColList::single(0));
            self.dag.register_table(
                info.id,
                StoreHandle::Borrowed(&mut **store),
                sys_tab_schema(info.id),
                RelationKind::SystemCatalog,
                0,
                dir,
            );
        }
    }

    // -- Setup DagEngine system table references ---------------------------

    fn setup_dag_sys_tables(&mut self) {
        use crate::query::SysTableRefs;
        let refs = SysTableRefs {
            nodes: self.sys_store_ptr(SysFamily::CircuitNodes),
            edges: self.sys_store_ptr(SysFamily::CircuitEdges),
            node_columns: self.sys_store_ptr(SysFamily::CircuitNodeColumns),
            dep_tab: self.sys_store_ptr(SysFamily::ViewDep),
        };
        self.dag.set_sys_tables(refs);
    }

    // -- Replay catalog (recovery) -----------------------------------------

    fn replay_catalog(&mut self) -> Result<(), String> {
        // Only these five families need hook-driven replay — Circuit* and
        // sys_sequences are loaded directly by other open-time paths — and
        // their ORDER is load-bearing (see the hooks.rs dispatch doc).
        self.replay_system_table(SysFamily::Schema)?;
        self.replay_system_table(SysFamily::Table)?;
        self.replay_system_table(SysFamily::View)?;
        self.replay_system_table(SysFamily::Column)?; // FK wiring + col_names invalidation
        self.replay_system_table(SysFamily::Index)?;
        Ok(())
    }

    fn replay_system_table(&mut self, family: SysFamily) -> Result<(), String> {
        let arc = self.sys_store_mut(family).full_scan();
        if arc.count > 0 {
            self.fire_hooks(family, &arc)?;
        }
        Ok(())
    }

    // -- Close engine ------------------------------------------------------

    /// Flush all system tables (memtable → shard). Called at checkpoint and close.
    /// Returns the first failure (with the offending sys table id) so the boot
    /// path can abort before the SAL — the only durable copy of replayed DDL —
    /// is reset.
    pub fn flush_all_system_tables(&mut self) -> Result<(), String> {
        for (info, table) in SYS_FAMILIES.iter().zip(self.sys_stores.iter_mut()) {
            // System tables are `SalReplay`, so `flush()` folds memtable + L0
            // and writes a durable shard synchronously.
            table
                .flush()
                .map_err(|e| format!("boot flush of system table {} failed: {}", info.id, e))?;
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
