use super::*;
use gnitz_wire::sys_rows::{
    write_col_tab_row, write_schema_tab_row, write_table_tab_row, ColTabRow, SchemaTabRow, TableTabRow,
};

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

        // Every store this process opens — the system tables here, every
        // relation and index through the registry, every view's operator scratch
        // through the compiler — is tuned by this one value, read from the
        // environment once under the server's own `GNITZ_` namespace.
        let config = StoreConfig::from_env("GNITZ_");

        let mut engine = CatalogEngine {
            registry: RelationRegistry::new(Slot::new(0, num_workers), config),
            dag: DagEngine::new(),
            base_dir: base_dir.to_string(),
            _dir_lock: dir_lock,
            caches: CatalogCacheSet::default(),
            is_master,
            next_id: FIRST_ALLOCATED_ID,
            durable_generation: 0,
            recorded_topology: 0,
            invalid_views: rustc_hash::FxHashSet::default(),
            pending_broadcasts: Vec::new(),
        };

        // Ahead of every path below, all of which reach a store through
        // `sys_relation`, which resolves through the registry.
        for family in SysFamily::ALL {
            engine
                .registry
                .register(
                    RelationSpec {
                        id: family.id(),
                        kind: RelationKind::SystemCatalog,
                        schema: *family.schema(),
                        directory: relation_dir(base_dir, RelationKind::SystemCatalog, family.id()),
                        props: ViewProps::default(),
                    },
                    OnRegister::Live,
                )
                .map_err(|e| format!("Failed to create system table '{}': error {e}", family.name()))?;
        }

        // A fresh database has no table records yet — asked of the registry, so
        // it reads the store the registrations above just opened.
        let is_new = !engine
            .registry
            .relation(SysFamily::Table.id())
            .is_some_and(|r| r.cursor().valid);

        if is_new {
            engine.bootstrap_system_tables()?;
        }

        // Phase 1: the `_sequences` scalars.
        engine.load_sequence_scalars();

        // Phase 2: Replay catalog through hooks
        engine.replay_catalog()?;

        Ok(engine)
    }

    // -- Bootstrap (fresh database) ----------------------------------------

    /// Write one family's seed rows straight to its store. The one path that
    /// skips `submit`'s precheck and hooks: the DAG is not wired up yet, and
    /// `replay_catalog` fires the hooks over these very rows a few lines later.
    fn bootstrap_ingest(&mut self, family: SysFamily, bb: BatchBuilder) -> Result<(), String> {
        self.registry
            .ingest(family.id(), bb.finish())
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
                write_table_tab_row(
                    &mut bb,
                    &TableTabRow {
                        table_id: family.id() as u64,
                        schema_id: SYSTEM_SCHEMA_ID as u64,
                        name: family.name(),
                        pk_col_idx: gnitz_wire::pack_pk_cols(family.wire().pk_cols),
                        flags: 0,
                    },
                    1,
                );
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
                    write_col_tab_row(
                        &mut bb,
                        &ColTabRow {
                            owner_id: family.id() as u64,
                            col_idx: i as u64,
                            owner_kind: gnitz_wire::OWNER_KIND_TABLE,
                            name: c.name,
                            type_code: c.type_code as u64,
                            is_nullable: c.nullable,
                            fk_table_id: 0,
                            fk_col_idx: 0,
                            is_serial: false,
                            is_hidden: false,
                            scale: 0,
                        },
                        1,
                    );
                }
            }
            self.bootstrap_ingest(SysFamily::Column, bb)?;
        }

        // Publish the foundational metadata. The families bootstrap did not
        // write are empty and cost nothing to include.
        self.flush_all_system_tables()
    }

    // -- Replay catalog (recovery) -----------------------------------------

    fn replay_catalog(&mut self) -> Result<(), String> {
        // Only these six families need hook-driven replay; `_sequences` is loaded by
        // `load_sequence_scalars`. CircuitNodes precedes View: view registration reads
        // the dependency map it fills. Schema
        // must precede the two relation families (their qualified names need it),
        // and Index must follow them. COL_TAB replaying last does NOT violate the
        // COL-before-relation contract: the register hooks read sys_columns
        // storage directly, which is already loaded (see the hooks.rs dispatch
        // doc).
        self.replay_system_table(SysFamily::Schema)?;
        self.replay_system_table(SysFamily::Table)?;
        self.replay_system_table(SysFamily::CircuitNodes)?;
        self.replay_system_table(SysFamily::View)?;
        self.replay_system_table(SysFamily::Column)?; // FK wiring + col_names invalidation
        self.replay_system_table(SysFamily::Index)?;
        Ok(())
    }

    fn replay_system_table(&mut self, family: SysFamily) -> Result<(), String> {
        let arc = self.sys_relation(family).full_scan();
        if !arc.is_empty() {
            self.fire_hooks(family, &arc, OnRegister::BootReplay)?;
        }
        Ok(())
    }

    // -- Close engine ------------------------------------------------------

    /// The base round over every store this process owns, in **one** barrier —
    /// the user-relation sibling of [`Self::flush_all_system_tables`], and the
    /// same argument: a per-table loop builds an io_uring and forces a journal
    /// commit per table, where the batched set joins one. It trades peak dirty
    /// page cache and a table id in the error message for that.
    pub(crate) fn flush_base_round(&mut self) -> Result<(), String> {
        self.registry.checkpoint_base().map_err(|e| e.to_string())
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
        let state = dag.collect_ephemeral_state(registry);
        registry
            .checkpoint_ephemeral(generation, state)
            .map_err(|e| e.to_string())
    }

    /// Unlink the manifest of every store [`Self::flush_ephemeral_round`]
    /// publishes, so the next open peeks `None` and erases those shards instead
    /// of resuming them. Its inverse, over the same two collections in the same
    /// order — which is why the two live together.
    pub(crate) fn unlink_derived_manifests(&mut self) {
        let CatalogEngine { registry, dag, .. } = self;
        let state = dag.collect_ephemeral_state(registry);
        registry.unlink_ephemeral_manifests(state);
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
