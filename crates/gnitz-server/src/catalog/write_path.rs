//! Catalog write path: `apply_bundle` (precheck → owned-row cascade →
//! layout through the caller's sink → ingest → `fire_hooks`), the undo of a
//! refused write, what an applied bundle did, and the zone pin.

use gnitz_store::relation::{Landing, PkRule};
use gnitz_wire::{IdxTabRow, IdxTabSlot, PkColList, ViewTabSlot, WireFault, payload_u64};
use gnitz_zset::repr::Batch;
use rustc_hash::FxHashSet;

use super::CatalogEngine;
use super::sys_tables::{Block, DdlBundle, IdSet, PkShape, SysFamily, SysFamilyExt, pk_signatures};

/// Where a catalog write lays each group out, before applying it. An `Err`
/// refuses the write, which leaves the catalog as it was.
pub(crate) type ZoneSink<'a> = dyn FnMut(SysFamily, &Batch) -> Result<(), WireFault> + 'a;

/// One system-family delta as the master applied it: the undo log of a write,
/// and what [`AppliedDdl`] reads.
struct ZoneGroup {
    family: SysFamily,
    batch: Batch,
    /// A view scanned the family when the group was applied, so a worker's
    /// `ddl_sync` holds it above its cut, owed a tick.
    scanned: bool,
}

/// Why a catalog write emits no zone.
#[derive(Debug)]
pub(crate) enum ZoneError {
    /// Refused; the catalog is as it was.
    Refused(WireFault),
    /// The catalog holds part of the write and cannot be put back.
    Diverged(String),
}

/// What an applied `DDL_TXN` bundle did, read off the groups it applied — its
/// drop cascade included.
#[derive(Default)]
pub(crate) struct AppliedDdl {
    /// The system families the zone wrote while a view scanned them, in
    /// `SysFamily::ALL` order: a worker holds those groups above its cut, owed
    /// a tick.
    pub(crate) held_above: Vec<u64>,
    pub(crate) created_views: Vec<u64>,
    /// Tables and views, chain segments included.
    pub(crate) dropped_relations: Vec<u64>,
    /// `(owner, cols)` of every index dropped, cascaded ones included.
    pub(crate) dropped_indexes: Vec<(u64, PkColList)>,
}

impl AppliedDdl {
    fn of(zone: &[ZoneGroup]) -> Self {
        let held_above = SysFamily::ALL
            .into_iter()
            .filter(|f| zone.iter().any(|g| g.family == *f && g.scanned))
            .map(SysFamily::id)
            .collect();
        let mut out = AppliedDdl { held_above, ..Default::default() };
        for g in zone {
            for sig in pk_signatures(&g.batch) {
                match (g.family, sig.shape()) {
                    (SysFamily::View, PkShape::Create) => out.created_views.push(sig.leading),
                    (SysFamily::Table | SysFamily::View, PkShape::Drop) => out.dropped_relations.push(sig.leading),
                    (SysFamily::Index, PkShape::Drop) => {
                        let r = IdxTabRow::read(&g.batch, sig.row).expect("an applied index row decoded in its hook");
                        out.dropped_indexes.push((r.owner_id, r.source_col_idx));
                    }
                    _ => {}
                }
            }
        }
        out
    }
}

impl CatalogEngine {
    // -- The applied-delta entry points ----------------------------------------

    /// `batch` with the retraction of every row its dropped relations own, in
    /// reversed apply order: an index's hook reads its owner, and undoing the drop
    /// re-registers the owner from its circuit and column rows.
    fn with_owned_retractions(&self, family: SysFamily, mut batch: Batch, dropped: IdSet) -> Vec<(SysFamily, Batch)> {
        if !matches!(family, SysFamily::Table | SysFamily::View) || dropped.is_empty() {
            return vec![(family, batch)];
        }
        let mut owners = dropped;
        // A view's segments drop in its own batch, unless the bundle already names them.
        if family == SysFamily::View {
            let segs = self.sys_rows_where(SysFamily::View, |s, i| {
                owners.contains(payload_u64(s, i, ViewTabSlot::owner_view_id as usize))
                    && !owners.contains(gnitz_wire::sys_leading_id(s.get_pk_bytes(i)))
            });
            if !segs.is_empty() {
                let seg_ids = (0..segs.len()).map(|i| gnitz_wire::sys_leading_id(segs.get_pk_bytes(i)));
                owners = IdSet::new(owners.ids().iter().copied().chain(seg_ids));
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

    /// Apply one `DDL_TXN` bundle, laying each group out through `sink` before it
    /// is applied, and answer what it did; the ids it created are spent. A bundle
    /// that fails is undone here: `Refused` leaves the catalog as it was, its ids
    /// unspent. Undoing a drop registers the relation again, with the rows its
    /// directory holds: a master's own stores are none of a user relation's, but
    /// a store-owning host loses that relation's unflushed rows.
    pub(crate) fn apply_bundle(
        &mut self,
        families: DdlBundle,
        sink: &mut ZoneSink<'_>,
    ) -> Result<AppliedDdl, ZoneError> {
        let mut zone = Vec::new();
        let cause = match self.apply_families(families, &mut zone, sink) {
            Ok(created) => {
                self.used_ids.extend(created);
                return Ok(AppliedDdl::of(&zone));
            }
            Err(cause) => cause,
        };
        // Newest first: a group's hook reads what the groups before it registered.
        let undone = zone
            .into_iter()
            .rev()
            .try_for_each(|g| self.apply_family(g.family, &g.batch.negated(), Landing::CUT));
        Err(match undone {
            Ok(()) => ZoneError::Refused(cause),
            Err(e) => ZoneError::Diverged(format!("DDL refused ({cause}); undoing what it applied failed: {e}")),
        })
    }

    /// The cross-family guards, then each family's precheck and its apply with the
    /// retraction of every system row its dropped relations own. A bundle that
    /// creates is applied in [`SysFamily::ALL`] order, so every register hook
    /// finds the families it reads applied; one that only drops in the reverse, so
    /// a dependent is retired first. Each group is laid out through `sink`, then
    /// pushed onto `zone` before its ingest, which can fail after writing rows the
    /// undo must negate. Answers the ids the bundle created; on `Err`, `zone`
    /// holds what was applied.
    fn apply_families(
        &mut self,
        families: DdlBundle,
        zone: &mut Vec<ZoneGroup>,
        sink: &mut ZoneSink<'_>,
    ) -> Result<FxHashSet<u64>, WireFault> {
        let created = self.precheck_bundle(&families)?;
        let mut ordered: Vec<(SysFamily, Block)> = families.into_blocks().collect();
        if ordered
            .iter()
            .all(|(_, b)| b.sigs.iter().all(|s| s.shape() == PkShape::Drop))
        {
            ordered.reverse();
        }
        for (family, Block { batch, sigs }) in ordered {
            let dropped = self.precheck_family(family, &batch, &sigs)?;
            for (family, batch) in self.with_owned_retractions(family, batch, dropped) {
                if batch.is_empty() {
                    continue;
                }
                sink(family, &batch)?;
                // What a worker's `ddl_sync` of this group will read: it applies the
                // same groups in the same order.
                let scanned = self.dag.is_scanned(family.id());
                zone.push(ZoneGroup { family, batch, scanned });
                let applied = &zone[zone.len() - 1].batch;
                self.apply_family(family, applied, Landing::CUT)?;
            }
        }
        Ok(created)
    }

    /// Ingest one delta, under its family's schema, into its family's store
    /// where `landing` puts it, and fire its hooks.
    fn apply_family(&mut self, family: SysFamily, batch: &Batch, landing: Landing) -> Result<(), String> {
        debug_assert!(
            *batch.schema() == *family.schema(),
            "a {} delta under another descriptor",
            family.name()
        );
        self.registry
            .land(family.id(), batch.clone(), landing, PkRule::Enforce)?;
        self.fire_hooks(family, batch)
    }

    /// Apply a push to ingestion point `tid`'s store, a base table's under
    /// `rule`, and hold its effect for the next tick of the views that scan
    /// `tid`. `Err` for a relation that is not an ingestion point.
    // Inlined: `batch` is taken by value, and at a call it is copied whole.
    #[inline(always)]
    pub(crate) fn ingest_unticked(&mut self, tid: u64, batch: Batch, rule: PkRule) -> Result<(), String> {
        let kind = self.registry.relation_or_err(tid)?.kind();
        if !kind.is_ingestion_point() {
            return Err(format!("relation {tid} is a {}, not an ingestion point", kind.noun()));
        }
        self.registry.land(tid, batch, self.landing_of(tid), rule).map(drop)
    }

    /// Apply one DdlSync group: above the store's cut when a view scans the
    /// family, for the tick the master sends behind the zone.
    pub(crate) fn ddl_sync(&mut self, family: SysFamily, batch: Batch) -> Result<(), String> {
        self.apply_family(family, &batch, self.landing_of(family.id()))
    }

    /// Where a worker's write to `id` lands: above the cut while a view scans
    /// it, for the tick that view is owed; at the cut otherwise.
    fn landing_of(&self, id: u64) -> Landing {
        match self.dag.is_scanned(id) {
            true => Landing::Pending,
            false => Landing::CUT,
        }
    }

    // -- Applied zone -------------------------------------------------------------

    /// Record that the system families hold every row of zone `zone_lsn`: every
    /// system flush after it records this floor.
    pub(crate) fn mark_zone_applied(&mut self, zone_lsn: u64) {
        self.applied_zone = self.applied_zone.max(zone_lsn);
    }

    /// The newest zone the system families hold: their replay floor at the next
    /// system flush.
    pub(crate) fn applied_zone(&self) -> u64 {
        self.applied_zone
    }
}
