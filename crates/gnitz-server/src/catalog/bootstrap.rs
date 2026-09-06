use super::*;
use gnitz_store::foundation::env::env_num;
use gnitz_wire::sys_rows::{write_schema_tab_row, SchemaTabRow};

impl CatalogEngine {
    // -- Open engine (main entry point) ------------------------------------

    /// Opens or creates a GnitzDB instance at `base_dir`, laid out for
    /// `num_workers` workers, as a standalone process — neither the master nor a
    /// forked worker, which is what every unit test is.
    #[cfg(test)]
    pub(crate) fn open(base_dir: &str, num_workers: u32) -> Result<Self, String> {
        Self::open_as(base_dir, num_workers, false)
    }

    /// [`Self::open`] as the master process: its index copies stay empty.
    pub(crate) fn open_master(base_dir: &str, num_workers: u32) -> Result<Self, String> {
        Self::open_as(base_dir, num_workers, true)
    }

    /// The forked child becomes worker `slot`: its index hook backfills from
    /// here on, and every inherited store is re-homed at its own child. Once per
    /// child, before any other catalog work.
    pub(crate) fn become_worker(&mut self, slot: Slot) -> Result<(), String> {
        self.is_master = false;
        self.registry
            .rehome(slot)
            .map_err(|e| format!("rehome stores failed: {e}"))
    }

    fn open_as(base_dir: &str, num_workers: u32, is_master: bool) -> Result<Self, String> {
        // Before any store opens: two writers on one directory mint identical
        // shard names and corrupt it silently.
        let dir_lock = lock_data_dir(base_dir, DIR_LOCK_RETRY_FOR)?;

        ensure_dir(&sys_catalog_dir(base_dir))?;

        // Every store this process opens — the system tables here, every
        // relation and index through the registry, every view's operator scratch
        // through the compiler — is tuned by this one value, read from the
        // environment once. `env_num` refuses a zero and an unparseable value.
        let defaults = StoreConfig::default();
        let config = StoreConfig {
            ram: RamBudgets {
                ram_tier_bytes: env_num("GNITZ_RAM_TIER_BYTES", defaults.ram.ram_tier_bytes),
                ..defaults.ram
            },
            scan_chunk_rows: env_num("GNITZ_SCAN_CHUNK_ROWS", defaults.scan_chunk_rows),
            adhoc_group_cap: env_num("GNITZ_ADHOC_GROUP_CAP", defaults.adhoc_group_cap),
        };

        // Create system tables (one `Table` each; durability derived from the
        // kind they are later registered under).
        let mut stores: Vec<Table> = Vec::with_capacity(SysFamily::COUNT);
        for family in SysFamily::ALL {
            let table = Table::with_budgets(
                &sys_family_dir(base_dir, family.name()),
                *family.schema(),
                family.id() as u32,
                RecoverySource::SalReplay,
                config.ram,
            )
            .map_err(|e| format!("Failed to create system table '{}': error {}", family.name(), e))?;
            stores.push(table);
        }

        // Check if this is a fresh database (no table records yet)
        let is_new = !stores[SysFamily::Table.index()].open_cursor().valid;

        let mut engine = CatalogEngine {
            registry: RelationRegistry::new(Slot::new(0, num_workers), config),
            dag: DagEngine::new(),
            base_dir: base_dir.to_string(),
            _dir_lock: dir_lock,
            caches: CatalogCacheSet::default(),
            is_master,
            next_schema_id: FIRST_USER_SCHEMA_ID,
            next_table_id: FIRST_USER_TABLE_ID,
            next_index_id: FIRST_USER_INDEX_ID,
            user_sequences: std::collections::HashMap::new(),
            durable_generation: 0,
            invalid_views: rustc_hash::FxHashSet::default(),
            pending_broadcasts: Vec::new(),
            pending_dir_deletions: Vec::new(),
            checkpoint_gated_deletions: Vec::new(),
            ctx: ApplyContext::new(),
        };

        // Ahead of every path below, all of which reach a store through
        // `sys_store`, which resolves through the registry.
        engine.register_system_table_families(stores);

        if is_new {
            engine.bootstrap_system_tables()?;
        }

        // Phase 1: Recover sequence counters
        engine.recover_sequences();

        // Before `replay_catalog`, whose view and index register hooks read it
        // (through `CatalogEngine::recovery_source`). `recovery_start_generation_bump`
        // leaves this where it is, so a clean restart resumes from the last
        // completed checkpoint.
        let g = engine.durable_generation;
        engine.registry_mut().set_resume_generation(g);

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
            .ingest_owned_batch(batch)
            .map_err(|e| format!("bootstrap: {} ingest failed: {e}", family.name()))
    }

    fn bootstrap_system_tables(&mut self) -> Result<(), String> {
        // 1. Core schema records
        {
            let mut bb = BatchBuilder::new(*SysFamily::Schema.schema());
            for (schema_id, name) in [(SYSTEM_SCHEMA_ID, "_system"), (PUBLIC_SCHEMA_ID, "public")] {
                write_schema_tab_row(&mut bb, &SchemaTabRow { schema_id: schema_id as u64, name }, 1);
            }
            self.bootstrap_ingest(SysFamily::Schema, bb)?;
        }

        // 2. Table records (self-registration of system tables)
        {
            let mut bb = BatchBuilder::new(*SysFamily::Table.schema());
            for family in SysFamily::ALL {
                let pk = gnitz_wire::pack_pk_cols(family.wire().pk_cols);
                push_table_tab_row(&mut bb, family.id(), SYSTEM_SCHEMA_ID, family.name(), pk, 0, 1);
            }
            self.bootstrap_ingest(SysFamily::Table, bb)?;
        }

        // 3. Column records for all system tables — the COL_TAB self-description,
        // derived from the same gnitz-wire slices the schemas are built from, so
        // the introspectable shape can never drift from the physical one.
        {
            let mut bb = BatchBuilder::new(*SysFamily::Column.schema());
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
        cursor.for_each_positive(|c| {
            let seq_id = c.current_key_narrow() as u64 as i64;
            let (src, row) = c.current_row_source();
            let val = payload_u64(src, row, gnitz_wire::SEQTAB_PAY_VALUE) as i64;
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
                SEQ_ID_TOPOLOGY => self.registry.set_recorded_topology(val as u64),
                // User-table SERIAL sequence (seq_id == table_id ≥
                // FIRST_USER_TABLE_ID). Store the high-water; next id =
                // high_water + 1. `observe_user_sequence` ignores a stray
                // catalog-range seq_id in the empty 4..16 gap, so it is never
                // misclassified as a user sequence.
                other => self.observe_user_sequence(other, val),
            }
        });
    }

    // -- Register system table families ------------------------------------

    /// Hand each system family's store to the registry, which owns it from here
    /// on — so a system table resolves like any user relation, and the later
    /// `replay_catalog` skips its id instead of re-entering it as a user one.
    fn register_system_table_families(&mut self, stores: Vec<Table>) {
        let base_dir = self.base_dir.clone();
        for (family, store) in SysFamily::ALL.into_iter().zip(stores) {
            let spec = RelationSpec {
                id: family.id(),
                kind: RelationKind::SystemCatalog,
                schema: *family.schema(),
                directory: sys_family_dir(&base_dir, family.name()),
                budgets: ViewBudgets::default(),
            };
            self.registry.register_owned(spec, Box::new(store));
        }
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
        if !arc.is_empty() {
            self.fire_hooks(family, &arc)?;
        }
        Ok(())
    }

    // -- Close engine ------------------------------------------------------

    /// Flush all system tables (memtable → shard). Called at checkpoint and close.
    /// Returns the first failure (with the offending sys table id) so the boot
    /// path can abort before the SAL — the only durable copy of replayed DDL —
    /// is reset.
    pub(crate) fn flush_all_system_tables(&mut self) -> Result<(), String> {
        // One barrier over the whole set, not ten: the round batches every
        // family's manifest, data and directory syncs into three submissions and
        // builds at most one io_uring. System tables are `SalReplay`, so each
        // folds memtable + L0 into a durable shard and re-stamps its manifest.
        base_round(self.registry.collect_system_flush_tables(), "system catalog flush")
    }

    /// The base round over every store this process owns, in **one** barrier —
    /// the user-relation sibling of [`Self::flush_all_system_tables`], and the
    /// same argument: a per-table loop builds an io_uring and forces a journal
    /// commit per table, where the batched set joins one. It trades peak dirty
    /// page cache and a table id in the error message for that.
    pub(crate) fn flush_base_round(&mut self) -> Result<(), String> {
        base_round(self.registry.collect_base_flush_tables(), "base flush")
    }

    /// The ephemeral checkpoint round: force-persist every view's operator-trace
    /// tables and output stores, stamped with this engine's resume generation.
    ///
    /// Two global passes — traces first, then outputs — so that any output at
    /// generation `G` implies that view's own traces are already durable at `G`.
    /// That holds only for a **compiled** view: the collector reads the plan
    /// cache, and nothing here compiles one to widen it. An output store stamped
    /// with no trace beside it is what `compute_invalid_views` rejects at the
    /// next boot — which is also the shape a mirror's copies have, since a mirror
    /// runs the output half alone and owns no traces. Batching each pass into one barrier also beats per-view
    /// interleaving.
    ///
    /// The caller latches the generation first: the server off the `FlushEph`
    /// message, an embedder through [`Self::bump_checkpoint_generation`].
    pub(crate) fn flush_ephemeral_round(&mut self) -> Result<(), String> {
        let generation = self.registry.resume_generation();
        let CatalogEngine { registry, dag, .. } = self;
        let traces = dag.collect_ephemeral_trace_tables(registry);
        gnitz_store::storage::flush_barrier(traces, gnitz_store::storage::FlushRound::Ephemeral(generation))
            .map_err(|e| format!("ephemeral trace flush: {e}"))?;
        registry.flush_ephemeral_outputs(generation)?;
        Ok(())
    }

    /// Unlink the manifest of every store [`Self::flush_ephemeral_round`]
    /// publishes, so the next open peeks `None` and erases those shards instead
    /// of resuming them. Its inverse, over the same two collections in the same
    /// order — which is why the two live together.
    pub(crate) fn unlink_derived_manifests(&mut self) {
        let CatalogEngine { registry, dag, .. } = self;
        let traces = dag.collect_ephemeral_trace_tables(registry);
        for t in traces.into_iter().chain(registry.collect_ephemeral_output_tables()) {
            t.unlink_manifest();
        }
    }

    /// Flush every store this engine owns that a restart could read back — each
    /// user table's own store and then the system tables — and clear the DAG.
    /// Consuming, so nothing can touch a closed engine; dropping one instead
    /// releases the directory lock without flushing, which is what the
    /// crash-semantics tests want.
    ///
    /// A fed view's delta store is skipped for the reason it is in neither
    /// checkpoint round: it is erased at open, so flushing it would write bytes
    /// the next boot deletes.
    ///
    /// The server never calls this: it flushes durably per zone and exits via
    /// abort or process teardown.
    #[cfg(test)]
    pub(crate) fn close(mut self) {
        // Both rounds run before `registry.close()` drops the stores they flush;
        // with no SAL under a unit test this close is their only durability.
        let _ = self.flush_base_round();
        let _ = self.flush_all_system_tables();
        self.registry.close();
        self.dag.close();
        // `self` drops here: the stores first, then the lock, in field order.
    }
}

/// One `Base` barrier over `tables`, labelled `what`. Both base rounds above go
/// through it, so neither can flush a user store under a different round than
/// the system catalog gets.
fn base_round<'a>(tables: impl IntoIterator<Item = &'a mut Table>, what: &str) -> Result<(), String> {
    gnitz_store::storage::flush_barrier(tables, gnitz_store::storage::FlushRound::Base)
        .map_err(|e| format!("{what}: {e}"))
}
