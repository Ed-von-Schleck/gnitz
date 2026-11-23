use gnitz_foundation::gnitz_note;
use gnitz_store::relation::{RelationKind, RelationRegistry, RelationSpec, StoreConfig};
use gnitz_zset::algebra::Placement;
use gnitz_zset::repr::{Batch, BatchBuilder};
use rustc_hash::FxHashSet;

use super::CatalogEngine;
use super::relations::{CatalogCacheSet, RelationEntry};
use super::sys_tables::{SysFamily, SysFamilyExt};
use crate::query::DagEngine;

impl CatalogEngine {
    /// Opens or creates the database at `base_dir`, laid out for `num_workers`
    /// workers, as the master, every store of the engine sized by `config`:
    /// registers the system families, opens their stores and seeds an empty one,
    /// stages `tail` — the log's committed DdlSync groups, each with its LSN, in
    /// log order — then fires every family's hooks over its rows, relays and
    /// reclaims children for the launched worker count, and decides which views
    /// rebuild — what a store open reads.
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
                RelationEntry::new(family.def().pk_cols, &family.column_defs(), Vec::new()),
            );
            engine.mark_zone_applied(engine.replay_floor(family));
            // Seeded straight into the store, per family: a crashed first boot may
            // have published any subset of them. The replay below fires the hooks
            // over these rows.
            if !engine.sys_relation(family).cursor().is_valid() {
                let mut bb = BatchBuilder::new(family.schema());
                family.write_seed_rows(&mut bb);
                engine
                    .registry
                    .ingest(family.id(), bb.finish())
                    .map_err(|e| format!("seeding {} failed: {e}", family.name()))?;
            }
        }

        // Each recovered group is ingested hook-free, unless its store was
        // flushed with it.
        let mut staged: u32 = 0;
        for group in tail {
            let (family, lsn, batch) = group?;
            if lsn <= engine.replay_floor(family) {
                continue;
            }
            engine.registry.ingest(family.id(), batch).map_err(|e| {
                format!(
                    "SAL system-table recovery apply failed (table_id={}, lsn={lsn}): {e}",
                    family.id()
                )
            })?;
            engine.mark_zone_applied(lsn);
            staged += 1;
        }
        if staged > 0 {
            gnitz_note!("SAL system table recovery: replayed {staged} entries");
        }

        for family in SysFamily::ALL {
            let rows = engine.sys_relation(family).full_scan();
            // Every id a stored row leads with was allocated, and the allocation
            // is logged before the DDL that uses it.
            debug_assert!(
                !family.allocates_ids() || {
                    let next = engine.next_catalog_id();
                    (0..rows.len()).all(|i| gnitz_wire::sys_leading_id(rows.get_pk_bytes(i)) < next)
                },
                "a stored {} row's id was never allocated",
                family.row_noun()
            );
            engine.fire_hooks(family, &rows)?;
        }
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
}
