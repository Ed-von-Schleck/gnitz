//! Per-process store lifecycle across the fork — the store open, the boot
//! relayout and child-dir reclamation — and the system families' replay floors
//! recovery reads.

use super::dirs::children_at_generation;
use super::relation_dir;
use super::ChildKind;
use super::{RelationKind, RelationRegistry, RelationSpec, Residency, SecondaryIndex, Store};
use crate::storage::{RecoverySource, Table};
use gnitz_zset::algebra::Slot;

impl RelationRegistry {
    // -- Store management (for multi-worker fork) -----------------------------

    /// Become rank `rank` as `residency`: open this process's store of every
    /// relation and index, a rederived one from its manifest at `resume_at(id)` or
    /// rebuilt, and fill each index that did not resume. Returns how many it filled.
    pub fn open_stores(
        &mut self,
        rank: u32,
        residency: Residency,
        resume_at: impl Fn(u64) -> Option<u64>,
    ) -> Result<usize, String> {
        assert_eq!(
            self.residency,
            Residency::Master,
            "open_stores runs once, on a master registry"
        );
        assert!(residency.owns_stores());
        self.slot = Slot::new(rank, self.slot.of);
        self.residency = residency;
        let tids: Vec<u64> = self
            .tables
            .iter()
            .filter(|(_, e)| e.kind() != RelationKind::SystemCatalog)
            .map(|(&tid, _)| tid)
            .collect();
        let chunk_rows = self.config.scan_chunk_rows;
        let mut filled = 0usize;
        for tid in tids {
            let e = &self.tables[&tid];
            let resume_at = resume_at(tid);
            let spec = RelationSpec {
                id: tid,
                kind: e.kind(),
                schema: e.schema(),
                placement: e.placement(),
                pk_repeats: e.pk_repeats(),
            };
            let (store, feed) = self.build_relation_store(spec, resume_at)?;
            let index_stores = e
                .indexes
                .iter()
                .map(|ix| {
                    self.open_child(
                        tid,
                        ChildKind::Index(ix.cols),
                        ix.schema(),
                        RecoverySource::Rederive { resume_at },
                        None,
                    )
                })
                .collect::<Result<Vec<_>, _>>()?;
            let entry = self.tables.get_mut(&tid).expect("listed above");
            (entry.store, entry.feed) = (store, feed);
            for (ix, t) in entry.indexes.iter_mut().zip(index_stores) {
                ix.store = Store::Held(Box::new(t));
            }
            let mut targets: Vec<&mut SecondaryIndex> = entry.indexes.iter_mut().filter(|ix| !ix.resumed()).collect();
            super::ingest::fill_indexes(&entry.store, chunk_rows, tid, &mut targets)?;
            filled += targets.len();
        }
        // A worker applies the same catalog deltas as the master, but only the
        // master writes the system tables' shards.
        if residency == Residency::Worker {
            self.collect_system_tables().for_each(Table::hold_in_ram);
        }
        Ok(filled)
    }

    /// Relay each base table's children onto this boot's worker count, then
    /// [`Self::reclaim_orphan_relation_dirs`]. Idempotent.
    pub fn reconcile_child_dirs(&self) -> Result<(), String> {
        // A relay removes the set it read, and the reclaim deletes directories.
        assert_eq!(
            self.residency,
            Residency::Master,
            "reconcile_child_dirs runs on a process holding no user store"
        );
        // Only a base table carries rows across a worker-count change.
        for (&id, entry) in self.tables.iter().filter(|(_, e)| e.kind().is_base_table()) {
            super::repartition::repartition_relation(
                &relation_dir(&self.base_dir, id),
                &entry.schema(),
                entry.placement(),
                self.slot.of,
                self.config.ram_tier_bytes,
                self.config.scan_chunk_rows,
            )?;
        }
        self.reclaim_orphan_relation_dirs()
    }

    /// The bytes `id`'s next published manifest carries beside its rows.
    pub fn set_caller_record(&mut self, id: u64, record: Vec<u8>) -> Result<(), String> {
        self.relation_mut_or_err(id)?
            .store
            .table_mut()
            .ok_or_else(|| format!("relation {id} holds no store in this process"))?
            .set_caller_record(record);
        Ok(())
    }

    /// Whether every checkpointed child of `view_id`, on **every launched rank**,
    /// carries a manifest at `generation` — the store half of the resume verdict.
    /// `false` for an id this registry does not hold.
    pub fn view_children_resumable(&self, view_id: u64, generation: u64) -> bool {
        // A worker cannot speak for its peers.
        assert!(
            matches!(self.residency, Residency::Master | Residency::Origin),
            "view_children_resumable reads every rank's children"
        );
        self.has_id(view_id) && children_at_generation(&relation_dir(&self.base_dir, view_id), self.slot.of, generation)
    }

    /// The system families' `table id → replay floor` their stores opened with:
    /// the floors of the master's pre-fork SAL walk.
    pub fn system_replay_floors(&self) -> std::collections::HashMap<u64, u64> {
        self.tables
            .iter()
            .filter(|(_, entry)| entry.kind() == RelationKind::SystemCatalog)
            .map(|(&tid, entry)| (tid, entry.store.held().loaded_mark().unwrap_or(0)))
            .collect()
    }
}
