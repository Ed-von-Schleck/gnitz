//! Catalog write path: `submit` (precheck → owned-row cascade → ingest →
//! `fire_hooks`), the broadcast queue, the zone pin, Stage-A compensation, and the
//! orphan-directory sweep.

use super::*;
use gnitz_store::ops::op_negate;

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
        if family == SysFamily::View {
            let segs = self.sys_rows_where(SysFamily::View, |s, i| {
                owners
                    .binary_search(&(payload_u64(s, i, gnitz_wire::VIEWTAB_PAY_OWNER_VIEW_ID) as i64))
                    .is_ok()
                    && owners.binary_search(&(s.get_pk(i) as i64)).is_err()
            });
            if !segs.is_empty() {
                owners.extend((0..segs.len()).map(|i| segs.get_pk(i) as i64));
                owners.sort_unstable();
                let mut merged = op_negate(segs);
                merged.append_batch(&batch, 0, batch.len());
                batch = merged;
            }
        }
        let indices = op_negate(self.sys_rows_where(SysFamily::Index, |s, i| {
            owners
                .binary_search(&(payload_u64(s, i, gnitz_wire::IDXTAB_PAY_OWNER_ID) as i64))
                .is_ok()
        }));
        // A SERIAL row's key is its table id.
        let sequences = self.retract_under(SysFamily::Sequence, &owners);
        let circuits = self.retract_under(SysFamily::CircuitNodes, &owners);
        let columns = self.retract_under(SysFamily::Column, &owners);
        vec![
            (SysFamily::Index, indices),
            (SysFamily::Sequence, sequences),
            (family, batch),
            (SysFamily::CircuitNodes, circuits),
            (SysFamily::Column, columns),
        ]
    }

    /// The negation of every live row of `family` whose leading key column is one of
    /// `ids` (strictly ascending): each row for a single-column key, each owner's band
    /// for a pair.
    pub(in crate::catalog) fn retract_under(&self, family: SysFamily, ids: &[i64]) -> Batch {
        debug_assert!(ids.windows(2).all(|w| w[0] < w[1]));
        let mut batch = Batch::with_capacity(family.schema(), 0);
        for &id in ids {
            self.for_each_row_under(family, id, |c| c.copy_current_row_into(&mut batch, -c.current_weight));
        }
        batch
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

    /// Apply a push to ingestion point `tid`'s store, and hold its effect for the
    /// next tick of the views that scan `tid`.
    pub(crate) fn ingest_unticked(&mut self, tid: i64, batch: Batch) -> Result<(), StoreError> {
        if !self.dag.is_scanned(tid) {
            return self.registry.ingest(tid, batch);
        }
        let effective = self.registry.ingest_returning(tid, batch)?;
        self.dag.buffer_unticked(tid, effective);
        Ok(())
    }

    /// Apply one DdlSync group. Never queues.
    pub(crate) fn ddl_sync(&mut self, table_id: i64, batch: Batch) -> Result<(), String> {
        let family = SysFamily::from_id(table_id).ok_or_else(|| "ddl_sync only for system tables".to_string())?;
        self.apply_family(family, batch)
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
        if !self.pending_broadcasts.is_empty() {
            return;
        }
        if let Err(e) = self.registry.reclaim_orphan_relation_dirs() {
            gnitz_warn!("catalog: orphan directory sweep failed: {}", e);
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
            .try_for_each(|(family, batch)| self.apply_family(family, op_negate(batch)))
            .map_err(|e| {
                format!(
                    "Stage-A DDL compensation failed — catalog cannot be restored, \
                     and serving it would serve a diverged catalog. Cause: {e}"
                )
            })
    }
}
