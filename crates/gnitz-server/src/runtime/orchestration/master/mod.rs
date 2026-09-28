//! Master-side dispatcher: fans out push/scan operations to worker processes
//! via the shared append-only log (SAL) and collects responses via per-worker
//! W2M regions, whose headers also carry each worker's park on the SAL.

mod exchange;
pub(crate) mod scatter;

mod dispatch;
mod preflight;
mod train;
mod unique_filter;
mod unique_preflight;

/// Fixtures shared by the `master` submodules' unit tests: the batch/schema
/// builders and the inert dispatcher those suites construct.
#[cfg(test)]
#[path = "tests/fixtures.rs"]
mod fixtures;

use std::cell::{Cell, RefCell};
use std::rc::Rc;

use rustc_hash::FxHashMap;

use crate::catalog::CatalogEngine;
use crate::runtime::reactor::{AckLease, Reactor, TrainLease};
use crate::runtime::sal::{DirectGroup, GroupData, GroupTargets, SalExcl, SalMessageKind, SalWriter, WorkerSet};
use crate::runtime::wire;
use gnitz_store::schema::{Placement, SchemaDescriptor};
use gnitz_store::storage::Batch;
use gnitz_wire::{BoundPeek, PkColList, WireFault, WireFlags};

pub(crate) use dispatch::{FlushRound, WORKER_WATCH};
pub(crate) use train::forward_scan;
pub(crate) use unique_filter::UniqueFilter;

// ---------------------------------------------------------------------------
// RelayPrepared — output of prepare_relay, input of emit_relay
// ---------------------------------------------------------------------------

/// An exchange round scattered into its relay; only the SAL write is left.
pub(crate) struct RelayPrepared {
    /// The view the relay targets.
    view: wire::WireSchema,
    source_id: i64,
    /// One batch per worker (scatter), or a single batch every worker is sent
    /// (broadcast).
    dest: Vec<Batch>,
    /// Every worker's source partition is drained: a backfill's stop signal.
    drained: bool,
}

impl RelayPrepared {
    /// The relay's SAL group; `arg0` echoes `source_id`.
    pub(crate) fn group(&self) -> DirectGroup<'_> {
        DirectGroup {
            template: self.view.frame(wire::WireMsg {
                arg0: self.source_id as u64,
                flags: WireFlags {
                    drained: self.drained,
                    ..Default::default()
                },
                ..Default::default()
            }),
            data: GroupData::batches(&self.dest),
            ..DirectGroup::new(SalMessageKind::ExchangeRelay)
        }
    }
}

// ---------------------------------------------------------------------------
// MasterDispatcher
// ---------------------------------------------------------------------------

pub struct MasterDispatcher {
    worker_pids: Vec<i32>,
    sal: SalWriter,
    /// The master's one event loop, which reads every worker's replies.
    reactor: Rc<Reactor>,
    /// Dereferenced by [`MasterDispatcher::cat`] alone.
    catalog: *mut CatalogEngine,
    /// Per-(table_id, column list) filter skipping redundant unique-index
    /// occupancy broadcasts. Keyed by the decoded list, so a composite index is
    /// identified by its whole column list and dropping `(a, b)` never touches
    /// a distinct single-column filter on `a`.
    unique_filters: RefCell<FxHashMap<(i64, PkColList), UniqueFilter>>,

    /// The generation the last ephemeral round stamped. Set unconditionally, so
    /// `derived_needs_restamp` reads it in release builds too.
    last_ephemeral_gen: Cell<u64>,
    /// A push group was written since the last base round.
    unflushed_pushes: Cell<bool>,

    /// The last tick round allocated, one per tick group. Starts at 1, which is
    /// never emitted, so `after_tick = 0` names no round.
    tick_round: Cell<u64>,

    /// Feed-enabled view id → the last round that reached it, absent reading as 1.
    /// May name a round that left the view no rows; never misses one that did.
    last_delta_round: RefCell<FxHashMap<i64, u64>>,

    /// A `u64` taken from the OS at boot, mixed into every delta reply's cursor
    /// tag; `MasterDispatcher::delta_cursor_tag` states what the tag answers.
    boot_nonce: u64,
}

// ---------------------------------------------------------------------------
// Read routing
// ---------------------------------------------------------------------------

/// Which workers answer a read, and what each is sent: `per_worker[w]` replaces
/// the template's blob for worker `w` when present.
struct ReadRoute {
    set: WorkerSet,
    per_worker: Option<Vec<Vec<u8>>>,
}

/// Which workers answer a read of a relation with `schema` bounded by `blob`,
/// and what each is sent. A replicated relation is read off worker 0. A key set
/// reaches the owners of its keys, each sent its own; an empty one is answered
/// by worker 0.
fn route_read(schema: &SchemaDescriptor, blob: Option<&[u8]>, nw: usize) -> ReadRoute {
    let whole = |set| ReadRoute { set, per_worker: None };
    match (schema.placement(), blob.and_then(gnitz_wire::peek_bound)) {
        (Placement::Replicated, _) => whole(WorkerSet::one(Placement::REPLICA_OWNER as usize)),
        (_, Some(BoundPeek::Range(r))) => whole(schema.confined_worker(&r, nw).map_or(WorkerSet::ALL, WorkerSet::one)),
        (Placement::Keyed { .. }, Some(BoundPeek::PkSet(s))) if s.stride == schema.pk_stride() => {
            let keys = || s.keys.chunks_exact(s.stride);
            let set = keys().fold(WorkerSet::EMPTY, |set, k| set.with(schema.worker_for_pk(k, nw)));
            match set.len() {
                0 => whole(WorkerSet::one(0)),
                1 => whole(set),
                _ => {
                    let mut by_owner = vec![Vec::new(); nw];
                    for k in keys() {
                        by_owner[schema.worker_for_pk(k, nw)].extend_from_slice(k);
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
    in_request_order: bool,
    scans: Vec<TrainLease>,
}

impl<'d> ScanCut<'d> {
    /// Lease a reply id for `set`, then write the `kind` group `write` builds on it.
    pub(crate) fn push(
        &mut self,
        kind: SalMessageKind,
        set: WorkerSet,
        write: impl FnOnce(&SalExcl<'d>, GroupTargets) -> Result<(), WireFault>,
    ) -> Result<(), WireFault> {
        let lease = self.disp.reactor.lease_train(set, kind);
        debug_assert!(lease.workers().len() > 0, "every read owes at least one reply");
        write(
            &self.excl,
            GroupTargets::Leased {
                set: lease.workers(),
                request_id: lease.id(),
                in_request_order: self.in_request_order,
            },
        )?;
        self.scans.push(lease);
        Ok(())
    }

    /// `group` written to the workers its target and bound reach, each sent its
    /// own key set when the bound splits one.
    pub(crate) fn read(&mut self, group: DirectGroup<'_>) -> Result<(), WireFault> {
        let bound = group.kind.carries_read_bound().then_some(group.template.blob);
        let schema = self.disp.schema_desc_for(group.template.target_id as i64);
        let route = route_read(&schema, bound, self.disp.num_workers());
        self.push(group.kind, route.set, |excl, targets| {
            excl.write(&DirectGroup {
                extras: route.per_worker.as_deref(),
                targets,
                ..group
            })
        })
    }
}

impl MasterDispatcher {
    /// `n` scan-shaped requests written under one SAL hold, one lease each. At
    /// `n > 1` each worker replies in request order.
    pub(crate) async fn scan_cut(
        &self,
        n: usize,
        build: impl FnOnce(&mut ScanCut<'_>) -> Result<(), WireFault>,
    ) -> Result<Vec<TrainLease>, WireFault> {
        let mut cut = ScanCut {
            disp: self,
            excl: self.sal.lock().await,
            in_request_order: n > 1,
            scans: Vec::with_capacity(n),
        };
        build(&mut cut)?;
        debug_assert_eq!(cut.scans.len(), n);
        Ok(cut.scans)
    }

    /// One scan-shaped read under its own SAL hold.
    pub(crate) async fn scan(&self, group: DirectGroup<'_>) -> Result<TrainLease, WireFault> {
        let mut leases = self.scan_cut(1, |cut| cut.read(group)).await?;
        Ok(leases.pop().expect("one read, one lease"))
    }
}
