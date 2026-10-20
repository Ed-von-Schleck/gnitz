//! Master-side dispatcher: fans out push/scan operations to worker processes
//! via the shared append-only log (SAL) and collects responses via per-worker
//! W2M regions.

pub(crate) mod scatter;

mod dispatch;
mod fk_presence;
mod preflight;
mod unique_filter;
mod unique_preflight;

/// The inert dispatcher the `master` submodules' unit tests construct.
#[cfg(test)]
#[path = "tests/fixtures.rs"]
mod fixtures;

use std::cell::{Cell, RefCell};

use rustc_hash::FxHashMap;

use crate::catalog::CatalogEngine;
use crate::runtime::reactor::{Reactor, TrainLease};
use crate::runtime::sal::WorkerSet;
use crate::runtime::sal::{Apply, DirectGroup, GroupData, GroupTargets, Read, SalExcl, SalRequest, SalWriter};
use gnitz_store::relation::Relation;
use gnitz_wire::{BoundPeek, PkColList, Probe, WireFault};
use gnitz_zset::algebra::Placement;
use gnitz_zset::repr::Batch;
use gnitz_zset::schema::SchemaDescriptor;

pub(crate) use dispatch::WORKER_WATCH;
pub(crate) use unique_filter::UniqueFilter;

// ---------------------------------------------------------------------------
// MasterDispatcher
// ---------------------------------------------------------------------------

pub struct MasterDispatcher {
    worker_pids: Vec<i32>,
    sal: SalWriter,
    /// The master's one event loop, which reads every worker's replies.
    reactor: Reactor,
    /// Dereferenced by [`MasterDispatcher::cat`] and [`MasterDispatcher::cat_mut`] alone.
    catalog: *mut CatalogEngine,
    /// The filter of each unique index that has one, by (table_id, column
    /// list). Every entry holds all of its index's spans.
    unique_filters: RefCell<FxHashMap<(u64, PkColList), UniqueFilter>>,

    /// The presence cache of each table an FK probe found a key of, by table id.
    fk_presence: RefCell<FxHashMap<u64, fk_presence::FkPresence>>,
    /// Keys one presence cache holds before it is dropped whole.
    /// `GNITZ_FK_PRESENCE_KEYS` lowers it, so tests reach it on small tables.
    fk_presence_cap: usize,

    /// The last tick round allocated, one per tid a tick group carries. Starts at 1, which is
    /// never emitted, so `after_tick = 0` names no round.
    tick_round: Cell<u64>,

    /// Feed-enabled view id → the last round that reached it, absent reading as 1.
    /// May name a round that left the view no rows; never misses one that did.
    last_delta_round: RefCell<FxHashMap<u64, u64>>,

    /// A `u64` taken from the OS at boot, mixed into every delta reply's cursor
    /// tag; `MasterDispatcher::delta_cursor_tag` states what the tag answers.
    boot_nonce: u64,

    /// Bytes of rows one write's cascade may delete, at each row's fixed width:
    /// what a client's own delete of them could carry in one frame.
    /// `GNITZ_CASCADE_BYTES` lowers it, so tests reach it on small tables.
    cascade_bytes: usize,
}

// ---------------------------------------------------------------------------
// Read routing
// ---------------------------------------------------------------------------

/// Which workers answer a read, and what each is sent: `per_worker[w]` replaces
/// the request's blob for worker `w` when present.
struct ReadRoute {
    set: WorkerSet,
    per_worker: Option<Vec<Vec<u8>>>,
}

/// Which workers answer `read` of `rel`, and what each is sent.
fn route_read(read: &Read<'_>, rel: &Relation, nw: usize) -> ReadRoute {
    let schema = rel.schema();
    let (bound, placement) = match read {
        // Index entries are partitioned by their source row, not by the probed span,
        // and a span may happen to be as wide as the PK.
        Read::HasPk { probe: Probe::Index(..), .. } | Read::KeySpans { .. } => (None, rel.placement()),
        // Every worker holds a replicated table whole, so its own-key probes spread
        // over all of them by the full key.
        Read::HasPk { keys, .. } => (
            gnitz_wire::peek_bound(keys),
            match rel.placement() {
                Placement::Replicated => Placement::full_pk(&schema),
                p => p,
            },
        ),
        Read::ScanSpec { spec, .. } | Read::Delta { spec, .. } => (gnitz_wire::peek_bound(spec), rel.placement()),
        Read::Pin { .. } | Read::Unpin { .. } | Read::SnapshotPage { .. } | Read::Backup { .. } => {
            unreachable!("a read of no live relation's rows is routed by ScanCut::read_all")
        }
    };
    let whole = |set| ReadRoute { set, per_worker: None };
    let owner = |k: &[u8]| placement.owner(k, nw).expect("a key-routed placement owns every key");
    match (placement, bound) {
        (Placement::Replicated, _) => whole(WorkerSet::one(Placement::REPLICA_OWNER as usize)),
        (_, Some(BoundPeek::Range(r))) => whole(
            placement
                .confined_worker(&schema, &r, nw)
                .map_or(WorkerSet::ALL, WorkerSet::one),
        ),
        (_, Some(BoundPeek::PkSet(s))) if placement.is_key_routed() && s.stride == schema.pk_stride() => {
            let keys = || s.keys.chunks_exact(s.stride);
            let set = keys().fold(WorkerSet::EMPTY, |set, k| set.with(owner(k)));
            match set.len() {
                0 => whole(WorkerSet::one(0)),
                1 => whole(set),
                _ => {
                    let mut by_owner = vec![Vec::new(); nw];
                    for k in keys() {
                        by_owner[owner(k)].extend_from_slice(k);
                    }
                    let per_worker = by_owner
                        .iter()
                        .map(|k| if k.is_empty() { Vec::new() } else { s.with_keys(k) })
                        .collect();
                    ReadRoute { set, per_worker: Some(per_worker) }
                }
            }
        }
        _ => whole(WorkerSet::ALL),
    }
}

/// Scan-shaped requests written under one SAL hold: one cut, whose replies each
/// worker sends in request order.
pub(crate) struct ScanCut<'d> {
    disp: &'d MasterDispatcher,
    excl: SalExcl<'d>,
    scans: Vec<TrainLease>,
}

impl<'d> ScanCut<'d> {
    /// A read of `view`'s deltas after `after_tick`; the round it reaches,
    /// the view's last.
    pub(crate) fn delta(
        &mut self,
        view: u64,
        after_tick: u64,
        spec: &[u8],
        reply_layout: u64,
    ) -> Result<u64, WireFault> {
        self.read(Read::Delta {
            view,
            after_tick,
            reply_layout,
            spec: spec.into(),
        })?;
        Ok(self.disp.last_delta_round(view))
    }

    /// `read` written to the workers its target and bound reach, each sent its
    /// own key set when the bound splits one, under a reply id leased for them.
    pub(crate) fn read(&mut self, read: Read<'_>) -> Result<(), WireFault> {
        let group = DirectGroup::new(read);
        let SalRequest::Read(read) = &group.request else {
            unreachable!("built from a read")
        };
        let target_id = group.request.template().target_id;
        let rel = self
            .disp
            .cat()
            .registry
            .relation(target_id)
            .unwrap_or_else(|| panic!("master: no relation for target_id={target_id}"));
        let route = route_read(read, rel, self.disp.num_workers());
        let data = route.per_worker.as_deref().map_or(group.data, GroupData::Blobs);
        let lease = self.disp.reactor.lease_train(route.set, group.request.kind());
        debug_assert!(lease.workers().len() > 0, "every read owes at least one reply");
        self.excl
            .write(&DirectGroup { data, targets: lease.targets(), ..group })?;
        self.scans.push(lease);
        Ok(())
    }

    /// `read` written to every worker, under a reply id leased for them: a read
    /// no live relation's placement routes.
    pub(crate) fn read_all(&mut self, read: Read<'_>) -> Result<(), WireFault> {
        let group = DirectGroup::new(read);
        let lease = self.disp.reactor.lease_train(WorkerSet::ALL, group.request.kind());
        self.excl.write(&DirectGroup { targets: lease.targets(), ..group })?;
        self.scans.push(lease);
        Ok(())
    }
}

impl MasterDispatcher {
    /// Bring every cache derived from `tid`'s committed rows up to a `batch` of
    /// it that is now durable.
    pub(crate) fn committed(&self, tid: u64, batch: &Batch) {
        self.unique_filter_ingest_batch(tid, batch);
        self.fk_presence_ingest_batch(tid, batch);
    }

    /// Drop everything held for a dropped relation. Ids are never reused, so
    /// nothing else would ever reclaim it.
    pub(crate) fn forget_relation(&self, id: u64) {
        self.last_delta_round.borrow_mut().remove(&id);
        self.unique_filter_invalidate_table(id);
        self.fk_presence_invalidate_table(id);
    }

    /// The scan-shaped requests `build` writes under one SAL hold, one lease
    /// each; each worker replies in request order.
    pub(crate) async fn scan_cut(
        &self,
        build: impl FnOnce(&mut ScanCut<'_>) -> Result<(), WireFault>,
    ) -> Result<Vec<TrainLease>, WireFault> {
        let mut cut = ScanCut {
            disp: self,
            excl: self.sal.lock().await,
            scans: Vec::new(),
        };
        build(&mut cut)?;
        Ok(cut.scans)
    }

    /// One scan-shaped read under its own SAL hold.
    pub(crate) async fn scan(&self, read: Read<'_>) -> Result<TrainLease, WireFault> {
        let mut leases = self.scan_cut(|cut| cut.read(read)).await?;
        Ok(leases.pop().expect("one read, one lease"))
    }
}

#[cfg(test)]
#[path = "tests/master.rs"]
mod tests;
