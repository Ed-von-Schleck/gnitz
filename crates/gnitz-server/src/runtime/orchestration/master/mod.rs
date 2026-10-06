//! Master-side dispatcher: fans out push/scan operations to worker processes
//! via the shared append-only log (SAL) and collects responses via per-worker
//! W2M regions.

pub(crate) mod scatter;

mod dispatch;
mod preflight;
mod train;
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
use crate::runtime::sal::{
    Apply, DirectGroup, GroupData, GroupTargets, Read, SalExcl, SalRequest, SalWriter, WorkerSet,
};
use gnitz_wire::{BoundPeek, PkColList, WireFault};
use gnitz_zset::repr::Batch;
use gnitz_zset::schema::{Placement, SchemaDescriptor};

pub(crate) use dispatch::WORKER_WATCH;
pub(crate) use train::forward_scan;
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

    /// A push group was written since the last base round.
    unflushed_pushes: Cell<bool>,

    /// The last tick round allocated, one per tid a tick group carries. Starts at 1, which is
    /// never emitted, so `after_tick = 0` names no round.
    tick_round: Cell<u64>,

    /// Feed-enabled view id → the last round that reached it, absent reading as 1.
    /// May name a round that left the view no rows; never misses one that did.
    last_delta_round: RefCell<FxHashMap<u64, u64>>,

    /// A `u64` taken from the OS at boot, mixed into every delta reply's cursor
    /// tag; `MasterDispatcher::delta_cursor_tag` states what the tag answers.
    boot_nonce: u64,
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

/// Which workers answer a read, bounded by `blob`, of a relation with `schema`
/// placed by `placement`, and what each is sent.
fn route_read(schema: &SchemaDescriptor, placement: Placement, blob: Option<&[u8]>, nw: usize) -> ReadRoute {
    let whole = |set| ReadRoute { set, per_worker: None };
    let owner = |k: &[u8]| placement.owner(k, nw).expect("a key-routed placement owns every key");
    match (placement, blob.and_then(gnitz_wire::peek_bound)) {
        (Placement::Replicated, _) => whole(WorkerSet::one(Placement::REPLICA_OWNER as usize)),
        (_, Some(BoundPeek::Range(r))) => whole(
            placement
                .confined_worker(schema, &r, nw)
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

/// Scan-shaped requests being written under one SAL hold.
pub(crate) struct ScanCut<'d> {
    disp: &'d MasterDispatcher,
    excl: SalExcl<'d>,
    scans: Vec<TrainLease>,
}

impl<'d> ScanCut<'d> {
    /// Lease a reply id for `set`, then write `group` to it.
    pub(crate) fn push(&mut self, set: WorkerSet, group: DirectGroup<'_>) -> Result<(), WireFault> {
        let lease = self.disp.reactor.lease_train(set, group.request.kind());
        debug_assert!(lease.workers().len() > 0, "every read owes at least one reply");
        self.excl.write(&DirectGroup {
            // Only a later scan of a cut has a train of the cut to stay behind.
            targets: lease.targets(!self.scans.is_empty()),
            ..group
        })?;
        self.scans.push(lease);
        Ok(())
    }

    /// `group`, a read, written to the workers its target and bound reach, each
    /// sent its own key set when the bound splits one.
    pub(crate) fn read(&mut self, group: DirectGroup<'_>) -> Result<(), WireFault> {
        let SalRequest::Read(read) = &group.request else {
            unreachable!("a cut routes reads; {:?} is none", group.request)
        };
        let bound = match read {
            Read::ScanSpec { spec, .. } | Read::Delta { read: spec, .. } => Some(&spec[..]),
            _ => None,
        };
        let tid = read.target();
        let (schema, placement) = self
            .disp
            .cat()
            .registry
            .relation(tid)
            .map(|rel| (rel.schema(), rel.placement()))
            .unwrap_or_else(|| panic!("master: no relation for target_id={tid}"));
        let route = route_read(&schema, placement, bound, self.disp.num_workers());
        self.push(
            route.set,
            DirectGroup {
                extras: route.per_worker.as_deref(),
                ..group
            },
        )
    }
}

impl MasterDispatcher {
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
        let mut leases = self.scan_cut(|cut| cut.read(DirectGroup::new(read))).await?;
        Ok(leases.pop().expect("one read, one lease"))
    }
}

#[cfg(test)]
#[path = "tests/master.rs"]
mod tests;
