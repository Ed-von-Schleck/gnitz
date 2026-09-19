use super::*;

impl CatalogEngine {
    // -- Open engine (main entry point) ------------------------------------

    /// Opens or creates the database at `base_dir`, laid out for `num_workers`
    /// workers, as the master: registers every relation, opens only the system
    /// families' stores.
    pub(crate) fn open_master(base_dir: &str, num_workers: u32) -> Result<Self, String> {
        // Before any store opens.
        let dir_lock = lock_data_dir(base_dir, DIR_LOCK_RETRY_FOR)?;

        let mut engine = CatalogEngine {
            registry: RelationRegistry::master(num_workers, StoreConfig::from_env("GNITZ_")),
            dag: DagEngine::new(),
            base_dir: base_dir.to_string(),
            _dir_lock: dir_lock,
            caches: CatalogCacheSet::default(),
            next_id: FIRST_ALLOCATED_ID,
            invalid_views: rustc_hash::FxHashSet::default(),
            pending_broadcasts: Vec::new(),
        };

        for family in SysFamily::ALL {
            engine
                .registry
                .register(RelationSpec {
                    id: family.id(),
                    kind: RelationKind::SystemCatalog,
                    schema: *family.schema(),
                    directory: relation_dir(base_dir, RelationKind::SystemCatalog, family.id()),
                    props: ViewProps::default(),
                })
                .map_err(|e| format!("Failed to create system table '{}': error {e}", family.name()))?;
            engine.enter_relation(
                family.id(),
                RelationKind::SystemCatalog,
                family.schema(),
                &family.column_defs(),
            );
        }

        engine.seed_system_tables()?;
        engine.load_sequence_scalars();
        engine.replay_catalog()?;
        Ok(engine)
    }

    /// Take rank `rank` as `residency`: open this process's stores and rebuild
    /// every index that did not resume. Returns how many were rebuilt.
    pub(crate) fn open_stores(&mut self, rank: u32, residency: Residency) -> Result<usize, String> {
        self.registry.open_stores(rank, residency)?;
        self.backfill_all_indexes()
    }

    /// [`Self::open_master`], then the rest of a store-owning boot, as a
    /// standalone host at rank 0.
    #[cfg(test)]
    pub(crate) fn open(base_dir: &str, num_workers: u32) -> Result<Self, String> {
        let mut engine = Self::open_master(base_dir, num_workers)?;
        engine.registry.reconcile_child_dirs()?;
        engine.open_stores(0, Residency::Origin)?;
        Ok(engine)
    }

    // -- Seed (fresh database) ---------------------------------------------

    /// Seed every family whose store has never published, straight into the
    /// store: `replay_catalog` fires the hooks over these rows.
    fn seed_system_tables(&mut self) -> Result<(), String> {
        // Per family: a crashed first boot may have published any subset of them.
        for family in SysFamily::ALL {
            if self.sys_relation(family).cursor().valid {
                continue;
            }
            let mut bb = BatchBuilder::new(*family.schema());
            family.write_seed_rows(&mut bb);
            self.registry
                .ingest(family.id(), bb.finish())
                .map_err(|e| format!("seeding {} failed: {e}", family.name()))?;
        }
        Ok(())
    }

    // -- Replay catalog (recovery) -----------------------------------------

    fn replay_catalog(&mut self) -> Result<(), String> {
        let mut families = SysFamily::ALL;
        families.sort_by_key(|f| f.topo_priority());
        // Each registration reads its own column records; the system families'
        // entries are compile-time data.
        for family in families.into_iter().filter(|&f| f != SysFamily::Column) {
            let rows = self.sys_relation(family).full_scan();
            self.fire_hooks(family, &rows)?;
        }
        Ok(())
    }

    // -- Close engine ------------------------------------------------------

    /// Flush every base and system store, then drop the engine. Dropping one
    /// without `close` is a crash.
    #[cfg(test)]
    pub(crate) fn close(mut self) {
        let _ = self.registry.checkpoint_base();
        let _ = self.flush_all_system_tables();
    }
}
