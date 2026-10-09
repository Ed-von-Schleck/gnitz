//! Catalog write path: `apply_bundle` over `submit` (precheck → owned-row cascade →
//! ingest → `fire_hooks`), the zone a write returns and its undo, the zone pin,
//! and the orphan-directory sweep.

use gnitz_wire::payload_u64;
use gnitz_wire::sys_rows::{IdxTabSlot, ViewTabSlot};
use gnitz_zset::repr::Batch;

use super::sys_reads::IdSet;
use super::sys_tables::{family_pk_partition, SysFamily};
use super::CatalogEngine;

/// One system-family delta as the master applied it: one group of the zone a
/// catalog write emits.
pub(crate) struct ZoneGroup {
    pub(crate) family: SysFamily,
    pub(crate) batch: Batch,
    /// A view scanned the family when the group was applied, so a worker's
    /// `ddl_sync` holds it above its cut, owed a tick.
    pub(crate) scanned: bool,
}

/// Why a catalog write emits no zone.
#[derive(Debug)]
pub(crate) enum ZoneError {
    /// Refused; the catalog is as it was.
    Refused(String),
    /// The catalog holds part of the write and cannot be put back.
    Diverged(String),
}

impl CatalogEngine {
    // -- The applied-delta entry points ----------------------------------------

    /// Precheck one system-family delta and apply it with the retraction of every
    /// system row its dropped relations own. Each group is pushed onto `zone`
    /// before its ingest, which can fail after writing rows [`Self::undo`] must
    /// negate.
    fn submit(&mut self, family: SysFamily, batch: Batch, zone: &mut Vec<ZoneGroup>) -> Result<(), String> {
        let net_dead = self.precheck_family(family, &batch)?;
        for (family, batch) in self.with_owned_retractions(family, batch, net_dead) {
            if batch.is_empty() {
                continue;
            }
            // What a worker's `ddl_sync` of this group will read: it applies the
            // same groups in the same order.
            let scanned = self.dag.is_scanned(family.id());
            zone.push(ZoneGroup { family, batch: batch.clone(), scanned });
            self.apply_family(family, batch, false)?;
        }
        Ok(())
    }

    /// `batch` with the retraction of every row its dropped relations own, in
    /// reversed apply order: an index's hook reads its owner, and undoing the drop
    /// re-registers the owner from its circuit and column rows.
    fn with_owned_retractions(&self, family: SysFamily, mut batch: Batch, net_dead: IdSet) -> Vec<(SysFamily, Batch)> {
        if !matches!(family, SysFamily::Table | SysFamily::View) || net_dead.is_empty() {
            return vec![(family, batch)];
        }
        let mut owners = net_dead;
        // A view's segments drop in its own batch, unless the bundle already names them.
        if family == SysFamily::View {
            let segs = self.sys_rows_where(SysFamily::View, |s, i| {
                owners.contains(payload_u64(s, i, ViewTabSlot::owner_view_id as usize))
                    && !owners.contains(s.get_pk(i) as u64)
            });
            if !segs.is_empty() {
                owners = owners.with((0..segs.len()).map(|i| segs.get_pk(i) as u64));
                let mut merged = segs.negated();
                merged.append_batch(&batch);
                batch = merged;
            }
        }
        let indices = self
            .sys_rows_where(SysFamily::Index, |s, i| {
                owners.contains(payload_u64(s, i, IdxTabSlot::owner_id as usize))
            })
            .negated();
        // A SERIAL row's key is its table id.
        let sequences = self.retract_under(SysFamily::Sequence, owners.ids());
        let circuits = self.retract_under(SysFamily::Circuit, owners.ids());
        let columns = self.retract_under(SysFamily::Column, owners.ids());
        let mut parts = vec![
            (SysFamily::Index, indices),
            (SysFamily::Sequence, sequences),
            (family, batch),
            (SysFamily::Circuit, circuits),
            (SysFamily::Column, columns),
        ];
        parts.sort_by_key(|(f, _)| std::cmp::Reverse(f.index()));
        parts
    }

    /// Apply one `DDL_TXN` bundle and answer the zone it applied. A bundle that
    /// fails is undone here: `Refused` leaves the catalog as it was.
    pub(crate) fn apply_bundle(
        &mut self,
        families: [Option<Batch>; SysFamily::COUNT],
    ) -> Result<Vec<ZoneGroup>, ZoneError> {
        let mut zone = Vec::new();
        let Err(cause) = self.apply_families(families, &mut zone) else {
            return Ok(zone);
        };
        Err(match self.undo(zone) {
            Ok(()) => ZoneError::Refused(cause),
            Err(e) => ZoneError::Diverged(format!("DDL refused ({cause}); undoing what it applied failed: {e}")),
        })
    }

    /// Negate every group of `zone`, newest first: a group's hook reads what the
    /// groups before it registered.
    pub(crate) fn undo(&mut self, zone: Vec<ZoneGroup>) -> Result<(), String> {
        zone.into_iter()
            .rev()
            .try_for_each(|g| self.apply_family(g.family, g.batch.negated(), false))
    }

    /// The cross-family guards, then each family through [`Self::submit`]. A bundle
    /// that creates is applied in [`SysFamily::ALL`] order, so every register hook
    /// finds the families it reads applied; one that only drops in the reverse, so
    /// a dependent is retired first. On `Err`, `zone` holds what was applied.
    fn apply_families(
        &mut self,
        families: [Option<Batch>; SysFamily::COUNT],
        zone: &mut Vec<ZoneGroup>,
    ) -> Result<(), String> {
        let new_views = families[SysFamily::View.index()]
            .as_ref()
            .map(|b| family_pk_partition(SysFamily::View, b).creates)
            .unwrap_or_default();
        self.precheck_bundle(&families, &new_views)?;
        let mut ordered: Vec<(SysFamily, Batch)> = SysFamily::ALL
            .into_iter()
            .zip(families)
            .filter_map(|(f, b)| Some((f, b?)))
            .collect();
        if ordered.iter().all(|(_, b)| (0..b.len()).all(|i| b.get_weight(i) < 0)) {
            ordered.reverse();
        }
        for (family, batch) in ordered {
            self.submit(family, batch, zone)?;
        }
        Ok(())
    }

    /// Ingest one delta into its family's store — above its cut iff `above` —
    /// and fire its hooks.
    fn apply_family(&mut self, family: SysFamily, mut batch: Batch, above: bool) -> Result<(), String> {
        let id = family.id();
        match above {
            true => self.registry.ingest_pending(id, batch.clone()),
            false => self.registry.ingest(id, batch.clone()),
        }?;
        // The hooks read the rows through the catalog's descriptor, not the
        // descriptor the delta arrived under.
        batch.set_schema(family.schema());
        self.fire_hooks(family, &batch)
    }

    /// Apply a push to ingestion point `tid`'s store, and hold its effect for the
    /// next tick of the views that scan `tid`. `Err` for a relation that is not
    /// an ingestion point.
    // Inlined: `batch` is taken by value, and at a call it is copied whole.
    #[inline(always)]
    pub(crate) fn ingest_unticked(&mut self, tid: u64, batch: Batch) -> Result<(), String> {
        let kind = self.registry.relation_or_err(tid)?.kind();
        if !kind.is_ingestion_point() {
            return Err(format!("relation {tid} is a {}, not an ingestion point", kind.noun()));
        }
        match self.dag.is_ticked(tid) {
            true => self.registry.ingest_pending(tid, batch),
            false => self.registry.ingest(tid, batch),
        }
    }

    /// Apply one DdlSync group: above the store's cut when a view scans the
    /// family, for the tick the master sends behind the zone.
    pub(crate) fn ddl_sync(&mut self, table_id: u64, batch: Batch) -> Result<(), String> {
        let family = SysFamily::from_id(table_id).ok_or_else(|| "ddl_sync only for system tables".to_string())?;
        self.apply_family(family, batch, self.dag.is_scanned(table_id))
    }

    // -- Applied zone, directory sweep -------------------------------------------

    /// Record that the system families hold every row of zone `zone_lsn`: every
    /// system flush after it records this floor.
    pub(crate) fn mark_zone_applied(&mut self, zone_lsn: u64) {
        self.system_zone = self.system_zone.max(zone_lsn);
    }

    /// Remove every relation and index directory no live entity owns. Only sound once
    /// every worker has ACKed a round written after the last DdlSync.
    pub(crate) fn reclaim_orphan_dirs(&self) {
        if let Err(e) = self.registry.reclaim_orphan_relation_dirs() {
            gnitz_warn!("catalog: orphan directory sweep failed: {}", e);
        }
    }
}
