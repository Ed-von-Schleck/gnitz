use gnitz_store::relation::{RelationKind, RelationRegistry, RelationSpec, StoreConfig};
use gnitz_zset::algebra::Placement;
use gnitz_zset::repr::{Batch, BatchBuilder};
use rustc_hash::FxHashSet;

use super::relations::{CatalogCacheSet, RelationEntry};
use super::sys_tables::SysFamily;
use super::CatalogEngine;
use crate::query::DagEngine;

impl CatalogEngine {
    // -- Open engine (main entry point) ------------------------------------

    /// Opens or creates the database at `base_dir`, laid out for `num_workers`
    /// workers, as the master, every store of the engine sized by `config`:
    /// registers the system families and opens their stores, stages `tail` — the
    /// log's committed DdlSync groups, each with its LSN, in log order —
    /// then fires every family's hooks over its rows, relays and reclaims
    /// children for the launched worker count, and decides which views rebuild —
    /// what a store open reads.
    pub(crate) fn open_master(
        base_dir: &str,
        num_workers: u32,
        config: StoreConfig,
        tail: impl IntoIterator<Item = Result<(SysFamily, u64, Batch), String>>,
    ) -> Result<CatalogEngine, String> {
        let mut engine = CatalogEngine {
            registry: RelationRegistry::master(base_dir, num_workers, config),
            dag: DagEngine::default(),
            caches: CatalogCacheSet::default(),
            id_floor: gnitz_wire::FIRST_USER_TABLE_ID,
            used_ids: FxHashSet::default(),
            applied_zone: 0,
            boot: super::BootVerdict::default(),
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
                RelationEntry::new(family.schema(), &family.column_defs(), Vec::new()),
            );
            engine.mark_zone_applied(engine.replay_floor(family));
        }
        engine.seed_system_tables()?;

        let mut staged: u32 = 0;
        for group in tail {
            let (family, lsn, batch) = group?;
            if engine.stage(family, lsn, batch)? {
                staged += 1;
            }
        }
        if staged > 0 {
            gnitz_note!("SAL system table recovery: replayed {staged} entries");
        }

        engine.replay_catalog()?;
        engine.id_floor = engine.next_catalog_id();
        // Relay each base table onto the launched worker count and drop the children
        // no relation owns any more before the verdict reads every launched rank's
        // manifests — here, where no worker exists yet.
        engine
            .registry
            .reconcile_child_dirs()
            .map_err(|e| format!("child-dir sweep failed: {e}"))?;
        let resume_at = engine.durable_generation();
        let rebuild = engine.dag.decide_rebuild(&engine.registry, resume_at);
        engine.boot = super::BootVerdict { resume_at, rebuild };
        Ok(engine)
    }

    /// The zone `family`'s store was last flushed at: its shards hold every
    /// group of the log at or below it.
    pub(in crate::catalog) fn replay_floor(&self, family: SysFamily) -> u64 {
        self.sys_relation(family).loaded_mark().unwrap_or(0)
    }

    /// Ingest one recovered DdlSync group into `family`'s store, firing no hook,
    /// unless the store was flushed with it. Answers whether it was ingested.
    fn stage(&mut self, family: SysFamily, lsn: u64, batch: Batch) -> Result<bool, String> {
        if lsn <= self.replay_floor(family) {
            return Ok(false);
        }
        self.registry.ingest(family.id(), batch).map_err(|e| {
            format!(
                "SAL system-table recovery apply failed (table_id={}, lsn={lsn}): {e}",
                family.id()
            )
        })?;
        self.mark_zone_applied(lsn);
        Ok(true)
    }

    // -- Seed (fresh database) ---------------------------------------------

    /// Seed every family whose store holds no row, straight into the store:
    /// `replay_catalog` fires the hooks over these rows.
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
            // Every id a stored row leads with was allocated, and the allocation
            // is logged before the DDL that uses it.
            debug_assert!(
                !family.allocates_ids() || {
                    let next = self.next_catalog_id();
                    (0..rows.len()).all(|i| gnitz_wire::sys_leading_id(rows.get_pk_bytes(i)) < next)
                },
                "a stored {} row's id was never allocated",
                family.row_noun()
            );
            self.fire_hooks(family, &rows)?;
        }
        Ok(())
    }
}
