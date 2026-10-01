use super::*;
use gnitz_expr::ColumnTable;

/// A master catalog with only its system families registered, their stores open.
/// [`Self::replay`] registers the rest.
pub(crate) struct UnreplayedCatalog(CatalogEngine);

impl UnreplayedCatalog {
    /// Each system family's replay floor.
    pub(crate) fn system_replay_floors(&self) -> std::collections::HashMap<u64, u64> {
        self.0.registry.system_replay_floors()
    }

    /// Ingest one recovered DdlSync group into `table_id`'s store, firing no hook.
    pub(crate) fn stage(&mut self, table_id: u64, lsn: u64, batch: Batch) -> Result<(), String> {
        let family = SysFamily::from_id(table_id).ok_or_else(|| format!("{table_id} is not a system table"))?;
        let engine = &mut self.0;
        engine.raise_next_id(family, &batch, 0..batch.len());
        engine.registry.ingest(table_id, batch)?;
        engine.mark_zone_applied(lsn);
        Ok(())
    }

    /// Load the sequence scalars and fire every family's hooks over its rows.
    pub(crate) fn replay(self) -> Result<CatalogEngine, String> {
        let mut engine = self.0;
        engine.load_sequence_scalars();
        engine.replay_catalog()?;
        Ok(engine)
    }
}

impl CatalogEngine {
    // -- Open engine (main entry point) ------------------------------------

    /// Opens or creates the database at `base_dir`, laid out for `num_workers`
    /// workers, as the master: registers the system families and opens their
    /// stores.
    pub(crate) fn open_master(base_dir: &str, num_workers: u32) -> Result<UnreplayedCatalog, String> {
        // Before any store opens.
        let dir_lock = lock_data_dir(base_dir)?;

        let mut engine = CatalogEngine {
            registry: RelationRegistry::master(base_dir, num_workers, StoreConfig::from_env("GNITZ_")),
            dag: DagEngine::default(),
            _dir_lock: dir_lock,
            caches: CatalogCacheSet::default(),
            next_id: FIRST_ALLOCATED_ID,
            pending_broadcasts: Vec::new(),
            system_zone: 0,
        };

        for family in SysFamily::ALL {
            engine
                .registry
                .register(RelationSpec {
                    id: family.id(),
                    kind: RelationKind::SystemCatalog,
                    schema: *family.schema(),
                    // DDL is master-broadcast, so each worker holds a full copy.
                    placement: Placement::Replicated,
                })
                .map_err(|e| format!("Failed to create system table '{}': error {e}", family.name()))?;
            engine.enter_relation(
                family.id(),
                RelationKind::SystemCatalog,
                family.schema().pk_cols(),
                &family.column_defs(),
                RelFacts::default(),
            );
        }

        engine.system_zone = engine.registry.system_replay_floors().into_values().max().unwrap_or(0);
        engine.seed_system_tables()?;
        Ok(UnreplayedCatalog(engine))
    }

    /// [`Self::open_master`] and its replay, then the rest of a store-owning
    /// boot, as a standalone host at rank 0.
    #[cfg(test)]
    pub(crate) fn open(base_dir: &str, num_workers: u32) -> Result<Self, String> {
        let mut engine = Self::open_master(base_dir, num_workers)?.replay()?;
        engine.registry.reconcile_child_dirs()?;
        engine.open_stores(0, gnitz_store::relation::Residency::Origin)?;
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
