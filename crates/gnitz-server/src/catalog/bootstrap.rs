use std::rc::Rc;

use gnitz_store::relation::{RelationKind, RelationRegistry, RelationSpec, StoreConfig};
use gnitz_zset::algebra::Placement;
use gnitz_zset::repr::{Batch, BatchBuilder};
use rustc_hash::FxHashSet;

use super::relations::{CatalogCacheSet, RelationEntry};
use super::sys_tables::SysFamily;
use super::CatalogEngine;
use crate::query::DagEngine;

/// A master catalog with only its system families registered, their stores open.
/// [`Self::replay`] registers the rest.
pub(crate) struct UnreplayedCatalog(CatalogEngine);

impl UnreplayedCatalog {
    /// Each system family's replay floor.
    pub(crate) fn system_replay_floors(&self) -> std::collections::HashMap<u64, u64> {
        self.0.registry.system_replay_floors()
    }

    /// Ingest one recovered DdlSync group into `family`'s store, firing no hook.
    pub(crate) fn stage(&mut self, family: SysFamily, lsn: u64, batch: Batch) -> Result<(), String> {
        let engine = &mut self.0;
        engine.raise_next_id(family, &batch);
        engine.registry.ingest(family.id(), batch)?;
        engine.mark_zone_applied(lsn);
        Ok(())
    }

    /// Load the sequence scalars, fire every family's hooks over its rows, relay
    /// and reclaim children for the launched worker count, then decide which views
    /// rebuild — what a store open reads.
    pub(crate) fn replay(self) -> Result<CatalogEngine, String> {
        let mut engine = self.0;
        engine.load_sequence_scalars();
        engine.replay_catalog()?;
        engine.id_floor = engine.next_id;
        // Relay each base table onto the launched worker count and drop the children
        // no relation owns any more before the verdict reads every launched rank's
        // manifests — here, where no worker exists yet.
        engine
            .registry
            .reconcile_child_dirs(|id| engine.dag.layout_of(id))
            .map_err(|e| format!("child-dir sweep failed: {e}"))?;
        engine.dag.decide_rebuild(&engine.registry, engine.resume_at);
        Ok(engine)
    }
}

impl CatalogEngine {
    // -- Open engine (main entry point) ------------------------------------

    /// Opens or creates the database at `base_dir`, laid out for `num_workers`
    /// workers, as the master: registers the system families and opens their
    /// stores, every store of the engine sized by `config`.
    pub(crate) fn open_master(
        base_dir: &str,
        num_workers: u32,
        config: StoreConfig,
    ) -> Result<UnreplayedCatalog, String> {
        let mut engine = CatalogEngine {
            registry: RelationRegistry::master(base_dir, num_workers, config),
            dag: DagEngine::default(),
            caches: CatalogCacheSet::default(),
            next_id: gnitz_wire::FIRST_USER_TABLE_ID,
            id_floor: gnitz_wire::FIRST_USER_TABLE_ID,
            used_ids: FxHashSet::default(),
            system_zone: 0,
            resume_at: None,
        };

        for family in SysFamily::ALL {
            engine
                .registry
                .register(
                    RelationSpec {
                        id: family.id(),
                        kind: RelationKind::SystemCatalog,
                        // DDL is master-broadcast, so each worker holds a full copy.
                        placement: Placement::Replicated,
                        pk_repeats: false,
                        compress: false,
                    },
                    *family.schema(),
                )
                .map_err(|e| format!("Failed to create system table '{}': error {e}", family.name()))?;
            engine.caches.enter(
                family.id(),
                RelationEntry::new(Rc::from(family.record()), Vec::new(), false),
            );
        }

        engine.system_zone = engine.registry.system_replay_floors().into_values().max().unwrap_or(0);
        engine.seed_system_tables()?;
        Ok(UnreplayedCatalog(engine))
    }

    // -- Seed (fresh database) ---------------------------------------------

    /// Seed every family whose store has never published, straight into the
    /// store: `replay_catalog` fires the hooks over these rows.
    fn seed_system_tables(&mut self) -> Result<(), String> {
        // Per family: a crashed first boot may have published any subset of them.
        for family in SysFamily::ALL {
            if self.sys_relation(family).cursor().is_valid() {
                continue;
            }
            let mut bb = BatchBuilder::new(family.schema());
            family.write_seed_rows(&mut bb);
            self.registry
                .ingest(family.id(), bb.finish())
                .map_err(|e| format!("seeding {} failed: {e}", family.name()))?;
        }
        Ok(())
    }

    // -- Replay catalog (recovery) -----------------------------------------

    fn replay_catalog(&mut self) -> Result<(), String> {
        for family in SysFamily::ALL {
            let rows = self.sys_relation(family).full_scan();
            self.raise_next_id(family, &rows);
            self.fire_hooks(family, &rows)?;
        }
        Ok(())
    }
}
