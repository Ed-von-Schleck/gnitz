//! Catalog write path: `submit` (precheck → owned-row cascade → ingest →
//! `fire_hooks`), the broadcast queue, the zone pin, Stage-A compensation, and the
//! orphan-directory sweep.

use rustc_hash::FxHashMap;

use super::*;

/// Every id `map` lists under any of `owners`.
fn owned_ids<'a>(map: &'a FxHashMap<i64, Vec<i64>>, owners: &'a [i64]) -> impl Iterator<Item = i64> + 'a {
    owners.iter().filter_map(|o| map.get(o)).flatten().copied()
}

impl CatalogEngine {
    // -- The applied-delta entry points ----------------------------------------

    /// Precheck one system-family delta and apply it with the retraction of every
    /// system row its dropped relations own. Each batch is queued before its ingest,
    /// which can fail after writing rows `compensate_stage_a` must negate.
    pub(crate) fn submit(&mut self, family: SysFamily, batch: Batch) -> Result<(), String> {
        let net_dead = self.precheck_family(family, &batch)?;
        for (family, batch) in self.with_owned_retractions(family, batch, net_dead) {
            if batch.is_empty() {
                continue;
            }
            self.pending_broadcasts.push((family, batch.clone()));
            self.apply_family(family, batch)?;
        }
        Ok(())
    }

    /// `batch` with the retraction of its dropped relations' rows: index rows first,
    /// since their hook reads the owner; circuit and column rows last, since undoing
    /// the drop re-registers the owner from them.
    fn with_owned_retractions(
        &self,
        family: SysFamily,
        mut batch: Batch,
        net_dead: Vec<i64>,
    ) -> Vec<(SysFamily, Batch)> {
        if !matches!(family, SysFamily::Table | SysFamily::View) || net_dead.is_empty() {
            return vec![(family, batch)];
        }
        let mut owners = net_dead;
        // A view's segments drop in its own batch, unless the bundle already names them.
        let segs: Vec<i64> = owned_ids(&self.caches.segments_by_owner, &owners)
            .filter(|s| owners.binary_search(s).is_err())
            .collect();
        if !segs.is_empty() {
            let mut merged = self.retract_pk_list(SysFamily::View, segs.iter().map(|&s| s as u128).collect());
            merged.append_batch(&batch, 0, batch.len());
            batch = merged;
            owners.extend(segs);
            owners.sort_unstable();
        }
        let indices = self.retract_pk_list(
            SysFamily::Index,
            owned_ids(&self.caches.indices_by_owner, &owners)
                .map(|id| id as u128)
                .collect(),
        );
        // A SERIAL row's key is its table id.
        let sequences = self.retract_pk_list(SysFamily::Sequence, owners.iter().map(|&o| o as u128).collect());
        let circuits = self.retract_bands(SysFamily::CircuitNodes, &owners);
        let columns = self.retract_bands(SysFamily::Column, &owners);
        vec![
            (SysFamily::Index, indices),
            (SysFamily::Sequence, sequences),
            (family, batch),
            (SysFamily::CircuitNodes, circuits),
            (SysFamily::Column, columns),
        ]
    }

    /// Ingest one delta into its family's store and fire its hooks.
    fn apply_family(&mut self, family: SysFamily, mut batch: Batch) -> Result<(), String> {
        let id = family.id();
        self.registry
            .ingest(id, batch.clone())
            .map_err(|e| format!("sys-table ingest failed (family={id}): {e}"))?;
        batch.set_schema(family.schema());
        self.fire_hooks(family, &batch, OnRegister::Live)
    }

    /// Apply one DdlSync group — a worker's broadcast or the master's pre-fork SAL
    /// recovery — and pin its family to the group's zone LSN. Never queues.
    pub(crate) fn ddl_sync(&mut self, table_id: i64, zone_lsn: u64, batch: Batch) -> Result<(), String> {
        let family = SysFamily::from_id(table_id).ok_or_else(|| "ddl_sync only for system tables".to_string())?;
        self.apply_family(family, batch)?;
        self.registry.pin_lsn(table_id, zone_lsn);
        Ok(())
    }

    // -- System table accessors ------------------------------------------------

    /// This family's relation, from the registry that owns it.
    pub(in crate::catalog) fn sys_relation(&self, family: SysFamily) -> &Relation {
        self.registry
            .relation(family.id())
            .expect("every system family is registered at open")
    }

    // -- Broadcast queue, zone pin, directory sweep -----------------------------

    /// Pin every queued family to `zone_lsn`. Called before anything awaits: a
    /// checkpoint round holding a `SalExcl` can flush the system tables before the
    /// zone is emitted.
    pub(crate) fn pin_queued_to_zone(&mut self, zone_lsn: u64) {
        for (family, _) in &self.pending_broadcasts {
            self.registry.pin_lsn(family.id(), zone_lsn);
        }
    }

    /// Drain the pending-broadcast queue. Taken by `emit_zone_to_sal` on success and
    /// by `compensate_stage_a` on failure.
    pub(crate) fn drain_pending_broadcasts(&mut self) -> Vec<(SysFamily, Batch)> {
        std::mem::take(&mut self.pending_broadcasts)
    }

    /// Remove every relation and index directory no live entity owns. Only sound once
    /// every worker has ACKed a round written after the last DdlSync, so it declines
    /// while an applied DDL's broadcasts are still queued.
    pub(crate) fn reclaim_orphan_dirs(&self) {
        if self.pending_broadcasts.is_empty() {
            self.registry.reclaim_orphan_relation_dirs(&self.base_dir);
        }
    }

    /// Compile a just-registered view's circuit and throw the result away, so a
    /// circuit the engine cannot run is rejected while the DDL is still undoable.
    pub(crate) fn preflight_view_compile(&self, vid: i64) -> Result<(), String> {
        let root = preflight_dir(&self.base_dir, vid);
        let verdict = self.dag.preflight_compile(&self.registry, vid, &root);
        let _ = std::fs::remove_dir_all(&root);
        verdict
    }

    // -----------------------------------------------------------------------
    // Stage-A compensation (DDL rollback)
    // -----------------------------------------------------------------------

    /// Undo a failed `DDL_TXN` bundle in master memory: negate every batch it applied,
    /// newest first. `Err` means the catalog cannot be restored; the caller aborts.
    pub(crate) fn compensate_stage_a(&mut self) -> Result<(), String> {
        self.drain_pending_broadcasts()
            .into_iter()
            .rev()
            .try_for_each(|(family, mut batch)| {
                batch.map_weights(i64::wrapping_neg);
                self.apply_family(family, batch)
            })
            .map_err(|e| {
                format!(
                    "Stage-A DDL compensation failed — catalog cannot be restored, \
                     and serving it would serve a diverged catalog. Cause: {e}"
                )
            })
    }
}
