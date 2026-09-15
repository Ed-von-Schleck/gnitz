//! Master-side dispatcher: fans out push/scan operations to worker processes
//! via the shared append-only log (SAL) and collects responses via per-worker
//! W2M regions, whose headers also carry each worker's park on the SAL.
//!
//! `tests/fixtures.rs` holds what more than one submodule's tests need, reached
//! as `super::super::fixtures`.

pub(crate) mod exchange;
pub(crate) mod scatter;

use std::cell::{Cell, RefCell};
use std::rc::Rc;

use rustc_hash::{FxHashMap, FxHashSet};

use crate::catalog::CatalogEngine;
use gnitz_store::relation::Relation;
use gnitz_store::schema::{IndexKeySpec, Placement, SchemaDescriptor};
use gnitz_wire::{BoundPeek, PkColList};

use super::guard_panic;
use crate::query::RelayRoute;
use crate::runtime::peer::Peer;
use crate::runtime::reactor::{Lease, Reactor};
use crate::runtime::sal::{
    DirectGroup, GroupData, GroupTargets, SalExcl, SalFit, SalMessageKind, SalScope, SalWriter, WorkerSet,
};
use crate::runtime::w2m::W2mSlot;
use crate::runtime::wire::{self, unique_preflight_wire_schema};
use exchange::PendingRelay;
use gnitz_store::ops::{op_relay_broadcast, op_relay_scatter_consolidated, op_repartition_batches, ScatterSpec};
use gnitz_store::schema::key::PkBuf;
use gnitz_store::storage::Batch;
use gnitz_wire::control::peek_control_block;
use gnitz_wire::{BackfillDecision, WireConflictMode, WireFault, WireFlags, WireStatus};
use scatter::{with_commit_indices, with_group, with_worker_indices};

// ---------------------------------------------------------------------------
// RelayPrepared — output of prepare_relay, input of emit_relay
// ---------------------------------------------------------------------------

/// Materialised exchange relay: shard columns already resolved, payloads
/// already scattered into per-worker batches. Only the SAL write is left, which
/// `emit_relay` does under a [`SalExcl`].
pub(crate) struct RelayPrepared {
    /// The view the relay targets, its schema block encoded once in
    /// `prepare_relay`: the group is measured before the SAL lock and emitted
    /// under it, and both passes read these same bytes.
    view: wire::WireSchema,
    source_id: i64,
    dest: RelayDest,
    /// SAL bytes the emission will write, sized from `dest` while it was built.
    pub(crate) footprint: usize,
}

/// The relay's destination payloads: one batch per worker (scatter), or a
/// single batch referenced by every worker slot (broadcast — the SAL encode
/// re-encodes per slot regardless, so no per-worker copy is materialized).
pub(crate) enum RelayDest {
    PerWorker(Vec<Batch>),
    Broadcast(Box<Batch>),
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
    /// a distinct single-column filter on `a`. See the UniqueFilter comment
    /// block.
    unique_filters: RefCell<FxHashMap<(i64, PkColList), UniqueFilter>>,

    /// The generation the last ephemeral round stamped. Set unconditionally, so
    /// `derived_needs_restamp` reads it in release builds too.
    last_ephemeral_gen: Cell<u64>,

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

mod dispatch;
mod preflight;
mod train;
mod unique_filter;
mod unique_preflight;

use super::TxnFamily;
pub(crate) use dispatch::{FlushRound, WORKER_WATCH};
pub(crate) use train::forward_scan;
use train::{decode_train_slot, drain_index_scan};
pub(crate) use unique_filter::UniqueFilter;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Worker `w`'s reply as a fault, keeping its status, or `None` when it succeeded.
pub(crate) fn worker_error(w: usize, op: &str, ctrl: &gnitz_wire::control::DecodedControl) -> Option<WireFault> {
    (ctrl.hdr.status != WireStatus::Ok).then(|| {
        let msg = String::from_utf8_lossy(&ctrl.blob);
        WireFault {
            status: ctrl.hdr.status,
            text: format!("worker {w}: {op}: {msg}"),
        }
    })
}

/// Which workers answer a read, and what each is sent: `per_worker[w]` replaces
/// the template's blob for worker `w` when present.
pub(crate) struct ReadRoute {
    pub(crate) set: WorkerSet,
    pub(crate) per_worker: Option<Vec<Vec<u8>>>,
}

/// [`MasterDispatcher::read_route`] over the relation's schema, `None` for an
/// unregistered one.
fn route_read(schema: Option<SchemaDescriptor>, blob: Option<&[u8]>, nw: usize) -> ReadRoute {
    let whole = |set| ReadRoute { set, per_worker: None };
    let Some(schema) = schema else {
        return whole(WorkerSet::ALL);
    };
    match (schema.placement(), blob.and_then(gnitz_wire::peek_bound)) {
        (Placement::Replicated, _) => whole(WorkerSet::one(0)),
        (_, Some(BoundPeek::PkRange(r))) => {
            whole(schema.confined_worker(&r, nw).map_or(WorkerSet::ALL, WorkerSet::one))
        }
        (Placement::Keyed { .. }, Some(BoundPeek::PkSet(s))) if s.stride == schema.pk_stride() => {
            let keys = || s.keys.chunks_exact(s.stride);
            let set = keys().fold(WorkerSet::EMPTY, |set, k| set.with(schema.worker_for_pk(k, nw)));
            if set.len() <= 1 {
                return whole(if set == WorkerSet::EMPTY {
                    WorkerSet::one(0)
                } else {
                    set
                });
            }
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
        _ => whole(WorkerSet::ALL),
    }
}

/// Scan-shaped requests being written under one SAL hold.
pub(crate) struct ScanCut<'d> {
    excl: SalExcl<'d>,
    reactor: &'d Reactor,
    nw: usize,
    in_request_order: bool,
    scans: Vec<Lease>,
}

impl<'d> ScanCut<'d> {
    /// Lease a reply id for `set`, then write the group `write` builds on it.
    pub(crate) fn push(
        &mut self,
        set: WorkerSet,
        write: impl FnOnce(&SalExcl<'d>, GroupTargets) -> Result<(), WireFault>,
    ) -> Result<(), WireFault> {
        let set = set.within(self.nw);
        debug_assert!(set.len() > 0, "every read owes at least one reply");
        let lease = self.reactor.lease_train(set);
        write(
            &self.excl,
            GroupTargets::Leased {
                set,
                request_id: lease.id(0),
                in_request_order: self.in_request_order,
            },
        )?;
        self.scans.push(lease);
        Ok(())
    }
}

impl MasterDispatcher {
    /// Which workers answer a read of `target_id` bounded by `blob`, and what each is
    /// sent. A replicated relation is read off worker 0. A key set reaches the owners
    /// of its keys, each sent its own; an empty one is answered by worker 0.
    pub(crate) fn read_route(&self, target_id: i64, blob: Option<&[u8]>) -> ReadRoute {
        let schema = self.cat().registry().relation(target_id).map(Relation::schema);
        route_read(schema, blob, self.num_workers())
    }

    /// `n` scan-shaped requests written under one SAL hold, one lease each. At
    /// `n > 1` each worker replies in request order.
    pub(crate) async fn scan_cut(
        &self,
        n: usize,
        build: impl FnOnce(&mut ScanCut<'_>) -> Result<(), WireFault>,
    ) -> Result<Vec<Lease>, WireFault> {
        let mut cut = ScanCut {
            excl: self.sal.lock().await,
            reactor: &self.reactor,
            nw: self.num_workers(),
            in_request_order: n > 1,
            scans: Vec::with_capacity(n),
        };
        build(&mut cut)?;
        debug_assert_eq!(cut.scans.len(), n);
        Ok(cut.scans)
    }
}

/// Fixtures shared by the `master` submodules' unit tests: the batch/schema
/// builders and the inert dispatcher those suites construct.
#[cfg(test)]
#[path = "tests/fixtures.rs"]
pub(super) mod fixtures;
