use super::*;
use crate::foundation::env::env_usize;
use gnitz_wire::sys_rows::{write_schema_tab_row, SchemaTabRow};
use gnitz_wire::SEQTAB_COL_VALUE;

impl CatalogEngine {
    // -- Open engine (main entry point) ------------------------------------

    /// Opens or creates a GnitzDB instance at `base_dir`, laid out for
    /// `num_workers` workers — passed in rather than read off `worker_ctx`, for
    /// the reason on [`CatalogEngine::num_workers`].
    pub fn open(base_dir: &str, num_workers: u32) -> Result<Self, String> {
        ensure_dir(base_dir)?;

        ensure_dir(&sys_catalog_dir(base_dir))?;

        // Create system tables (one `Table` each; durability derived from the
        // kind they are later registered under).
        let mut stores = Vec::with_capacity(SysFamily::COUNT);
        for info in &SYS_FAMILIES {
            let table = Table::new(
                &sys_family_dir(base_dir, info.wire.name),
                sys_tab_schema(info.id()),
                info.id() as u32,
                RelationKind::SystemCatalog
                    .recovery_source()
                    .expect("a system catalog family owns a store"),
            )
            .map(Box::new)
            .map_err(|e| format!("Failed to create system table '{}': error {}", info.wire.name, e))?;
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
            num_workers,
            owns_stores: true,
            durable_generation: 0,
            resume_generation: 0,
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
            ddl_scan_chunk_rows: env_usize("GNITZ_DDL_SCAN_CHUNK_ROWS", crate::catalog::DDL_SCAN_CHUNK_ROWS),
            // Per-worker distinct-group cap for the ad-hoc aggregate fold.
            // `GNITZ_ADHOC_GROUP_CAP` overrides it (E2E can shrink it to force
            // the cap error).
            adhoc_group_cap: env_usize("GNITZ_ADHOC_GROUP_CAP", super::ADHOC_GROUP_CAP),
        };

        if is_new {
            engine.bootstrap_system_tables()?;
        }

        // Phase 1: Recover sequence counters
        engine.recover_sequences();

        // Before `replay_catalog`, whose index hook reads it.
        // `recovery_start_generation_bump` leaves this where it is, so a clean
        // restart resumes from the last completed checkpoint.
        engine.set_resume_generation(engine.durable_generation);

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
        // 1. Core schema records
        {
            let mut bb = BatchBuilder::new(SysFamily::Schema.schema());
            for (schema_id, name) in [(SYSTEM_SCHEMA_ID, "_system"), (PUBLIC_SCHEMA_ID, "public")] {
                write_schema_tab_row(
                    &mut bb,
                    &SchemaTabRow {
                        schema_id: schema_id as u64,
                        name,
                    },
                    1,
                );
            }
            let batch = bb.finish();
            self.sys_store_mut(SysFamily::Schema)
                .ingest_borrowed_batch(&batch)
                .map_err(|e| format!("bootstrap: sys_schemas ingest failed: {e}"))?;
        }

        // 2. Table records (self-registration of system tables)
        {
            let mut bb = BatchBuilder::new(SysFamily::Table.schema());
            for info in &SYS_FAMILIES {
                push_table_tab_row(&mut bb, info.id(), SYSTEM_SCHEMA_ID, info.wire.name, 0, 0, 1);
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
                for (i, c) in info.wire.cols.iter().enumerate() {
                    let cd = ColumnDef {
                        name: c.name.to_string(),
                        type_code: c.type_code as u8,
                        is_nullable: c.nullable,
                        ..Default::default()
                    };
                    push_col_tab_row(&mut bb, info.id(), OWNER_KIND_TABLE, i as i64, &cd, 1);
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

        // Publish the foundational metadata. The families bootstrap did not
        // write are empty and cost nothing to include.
        self.flush_all_system_tables()
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
                    SEQ_ID_CHECKPOINT_GEN => self.durable_generation = self.durable_generation.max(val as u64),
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
        self.caches.schema_by_name.insert("_system".into(), SYSTEM_SCHEMA_ID);
        self.caches.schema_by_id.insert(SYSTEM_SCHEMA_ID, "_system".into());

        let base_dir = self.base_dir.clone();
        for (info, store) in SYS_FAMILIES.iter().zip(self.sys_stores.iter_mut()) {
            let dir = sys_family_dir(&base_dir, info.wire.name);
            let qualified = format!("_system.{}", info.wire.name);
            self.caches.entity_by_qname.insert(qualified, info.id());
            self.caches
                .entity_by_id
                .insert(info.id(), ("_system".into(), info.wire.name.into()));
            self.dag.register_table(
                info.id(),
                crate::query::TableEntry::new(
                    StoreHandle::Borrowed(&mut **store),
                    sys_tab_schema(info.id()),
                    RelationKind::SystemCatalog,
                    0,
                    dir,
                ),
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
        };
        self.dag.set_sys_tables(refs);
    }

    // -- Replay catalog (recovery) -----------------------------------------

    fn replay_catalog(&mut self) -> Result<(), String> {
        // Only these five families need hook-driven replay — Circuit* and
        // sys_sequences are loaded directly by other open-time paths — and
        // their ORDER is the dependency order (see the hooks.rs dispatch doc).
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
        // One barrier over the whole set, not ten: the round batches every
        // family's manifest, data and directory syncs into three submissions and
        // builds at most one io_uring. System tables are `SalReplay`, so each
        // folds memtable + L0 into a durable shard and re-stamps its manifest.
        let tables = self.sys_stores.iter_mut().map(|b| &mut **b as *mut Table);
        crate::storage::flush_barrier(tables, crate::storage::FlushRound::Base)
            .map_err(|e| format!("boot flush of the system catalog failed: {e:?}"))
    }

    /// Graceful close for tests; the server never closes the catalog (it
    /// flushes durably per zone and exits via abort or process teardown).
    #[cfg(test)]
    pub(crate) fn close(&mut self) {
        // Flush all user tables before clearing DagEngine. System tables hold
        // Borrowed handles and are flushed below.
        for entry in self.dag.tables.values_mut() {
            if let StoreHandle::Owned(cell) = &mut entry.handle {
                let _ = cell.get_mut().flush();
            }
        }
        // tables.clear() in dag.close() drops the owned `Box<Table>` automatically.
        self.dag.close();
        let _ = self.flush_all_system_tables();
    }
}
