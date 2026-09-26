//! Catalog write path: `submit` (precheck → owned-row cascade → ingest →
//! `fire_hooks`), the broadcast queue, the zone pin, Stage-A compensation, and the
//! orphan-directory sweep.

use super::*;

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
        let segs: Vec<i64> = self
            .ids_naming(SysFamily::View, gnitz_wire::VIEWTAB_PAY_OWNER_VIEW_ID, &owners)
            .into_iter()
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
            self.ids_naming(SysFamily::Index, gnitz_wire::IDXTAB_PAY_OWNER_ID, &owners)
                .into_iter()
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
    fn apply_family(&mut self, family: SysFamily, batch: Batch) -> Result<(), String> {
        let id = family.id();
        let mut applied = self
            .registry
            .ingest_returning(id, batch)
            .map_err(|e| format!("sys-table ingest failed (family={id}): {e}"))?;
        // The hooks read the rows through the catalog's descriptor, not the
        // descriptor the delta arrived under.
        applied.set_schema(family.schema());
        self.fire_hooks(family, &applied)
    }

    /// Apply a push to ingestion point `tid`'s store, and hold its effect for
    /// `tid`'s next tick.
    pub(crate) fn ingest_unticked(&mut self, tid: i64, batch: Batch) -> Result<(), StoreError> {
        let effective = self.registry.ingest_returning(tid, batch)?;
        self.dag.buffer_unticked(tid, effective);
        Ok(())
    }

    /// Apply one DdlSync group. Never queues.
    pub(crate) fn ddl_sync(&mut self, table_id: i64, batch: Batch) -> Result<(), String> {
        let family = SysFamily::from_id(table_id).ok_or_else(|| "ddl_sync only for system tables".to_string())?;
        self.apply_family(family, batch)
    }

    // -- System table accessors ------------------------------------------------

    /// This family's relation, from the registry that owns it.
    pub(in crate::catalog) fn sys_relation(&self, family: SysFamily) -> &Relation {
        self.registry
            .relation(family.id())
            .expect("every system family is registered at open")
    }

    // -- Broadcast queue, applied zone, directory sweep -------------------------

    /// Record that the system families hold every row of zone `zone_lsn`, before
    /// awaiting the SAL: a checkpoint holding it flushes with this floor.
    pub(crate) fn mark_zone_applied(&mut self, zone_lsn: u64) {
        self.system_zone = self.system_zone.max(zone_lsn);
    }

    /// The newest SAL zone applied to the system families.
    pub(crate) fn system_zone(&self) -> u64 {
        self.system_zone
    }

    /// Catalog families are applied in memory but not yet committed to the SAL.
    pub(crate) fn has_uncommitted_families(&self) -> bool {
        !self.pending_broadcasts.is_empty()
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
            self.registry.reclaim_orphan_relation_dirs();
        }
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
            .try_for_each(|(family, batch)| self.apply_family(family, gnitz_store::ops::op_negate(batch)))
            .map_err(|e| {
                format!(
                    "Stage-A DDL compensation failed — catalog cannot be restored, \
                     and serving it would serve a diverged catalog. Cause: {e}"
                )
            })
    }
}
