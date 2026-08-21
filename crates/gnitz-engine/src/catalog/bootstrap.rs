use super::*;
use crate::foundation::env::env_num;
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
        for family in SysFamily::ALL {
            let table = Table::new(
                &sys_family_dir(base_dir, family.name()),
                family.schema(),
                family.id() as u32,
                RelationKind::SystemCatalog
                    .recovery_source()
                    .expect("a system catalog family owns a store"),
            )
            .map(Box::new)
            .map_err(|e| format!("Failed to create system table '{}': error {}", family.name(), e))?;
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
            // unparseable value falls back to the default: a zero chunk size
            // drains nothing, so a backfill would never make progress.
            ddl_scan_chunk_rows: env_num("GNITZ_DDL_SCAN_CHUNK_ROWS", crate::catalog::DDL_SCAN_CHUNK_ROWS),
            // Per-worker distinct-group cap for the ad-hoc aggregate fold.
            // `GNITZ_ADHOC_GROUP_CAP` overrides it (E2E can shrink it to force
            // the cap error).
            adhoc_group_cap: env_num("GNITZ_ADHOC_GROUP_CAP", super::ADHOC_GROUP_CAP),
        };

        if is_new {
            engine.bootstrap_system_tables()?;
        }

        // Phase 1: Recover sequence counters
        engine.recover_sequences();

        // Before `replay_catalog`, whose view and index register hooks read it
        // (through `RelationKind::recovery_source`). `recovery_start_generation_bump`
        // leaves this where it is, so a clean restart resumes from the last
        // completed checkpoint.
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

    /// Write one family's seed rows straight to its store. The one path that
    /// skips `submit`'s precheck and hooks: the DAG is not wired up yet, and
    /// `replay_catalog` fires the hooks over these very rows a few lines later.
    fn bootstrap_ingest(&mut self, family: SysFamily, bb: BatchBuilder) -> Result<(), String> {
        let batch = bb.finish();
        self.sys_store_mut(family)
            .ingest_borrowed_batch(&batch)
            .map_err(|e| format!("bootstrap: {} ingest failed: {e}", family.name()))
    }

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
            self.bootstrap_ingest(SysFamily::Schema, bb)?;
        }

        // 2. Table records (self-registration of system tables)
        {
            let mut bb = BatchBuilder::new(SysFamily::Table.schema());
            for family in SysFamily::ALL {
                push_table_tab_row(&mut bb, family.id(), SYSTEM_SCHEMA_ID, family.name(), 0, 0, 1);
            }
            self.bootstrap_ingest(SysFamily::Table, bb)?;
        }

        // 3. Column records for all system tables — the COL_TAB self-description,
        // derived from the same gnitz-wire slices the schemas are built from, so
        // the introspectable shape can never drift from the physical one.
        {
            let mut bb = BatchBuilder::new(SysFamily::Column.schema());
            for family in SysFamily::ALL {
                for (i, c) in family.wire().cols.iter().enumerate() {
                    let cd = ColumnDef {
                        name: c.name.to_string(),
                        type_code: c.type_code as u8,
                        is_nullable: c.nullable,
                        ..Default::default()
                    };
                    push_col_tab_row(&mut bb, family.id(), OWNER_KIND_TABLE, i as i64, &cd, 1);
                }
            }
            self.bootstrap_ingest(SysFamily::Column, bb)?;
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

    /// Enter each system family in the DAG registry as a `Borrowed` handle on
    /// its `sys_stores` box, so every store path resolves a system table through
    /// the same registry lookup as a user relation. Their name/id caches are not
    /// seeded here: `replay_catalog` (next) replays SCHEMA_TAB and TABLE_TAB
    /// through the appliers, which fill them from the persisted rows. The
    /// registration itself must precede that replay — `hook_relation_register`
    /// skips an already-registered id, which is what keeps a system family from
    /// being re-registered as a user relation.
    fn register_system_table_families(&mut self) {
        let base_dir = self.base_dir.clone();
        for (family, store) in SysFamily::ALL.into_iter().zip(self.sys_stores.iter_mut()) {
            let dir = sys_family_dir(&base_dir, family.name());
            self.dag.register_table(
                family.id(),
                crate::query::TableEntry::new(
                    StoreHandle::Borrowed(&mut **store),
                    family.schema(),
                    RelationKind::SystemCatalog,
                    0,
                    dir,
                    None,
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
        // Only these five families need hook-driven replay; Circuit* and
        // sys_sequences are loaded directly by other open-time paths. Schema
        // must precede the two relation families (their qualified names need it),
        // and Index must follow them. COL_TAB replaying last does NOT violate the
        // COL-before-relation contract: the register hooks read sys_columns
        // storage directly, which is already loaded (see the hooks.rs dispatch
        // doc).
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

    /// Flush every store this engine owns — each user table's owned handle and
    /// then the system tables — and clear the DAG. There is no `Drop` doing any
    /// of it, so a caller that wants the tree on disk complete must call this.
    ///
    /// The server never does: it flushes durably per zone and exits via abort or
    /// process teardown.
    pub fn close(&mut self) {
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
