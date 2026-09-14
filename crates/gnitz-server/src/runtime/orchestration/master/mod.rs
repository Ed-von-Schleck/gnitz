//! Master-side dispatcher: fans out push/scan operations to worker processes
//! via the shared append-only log (SAL) and collects responses via per-worker
//! W2M regions. Eventfds provide cross-process signaling.
//!
//! `tests/fixtures.rs` holds what more than one submodule's tests need, reached
//! as `super::super::fixtures`.

pub(crate) mod exchange;
pub(crate) mod scatter;

use std::cell::{Cell, RefCell};
use std::rc::Rc;

use rustc_hash::{FxHashMap, FxHashSet};

use crate::catalog::CatalogEngine;
use gnitz_store::schema::{IndexKeySpec, SchemaDescriptor};
use gnitz_wire::{payload_native_key, pk_native_key};
use gnitz_wire::{PkColList, SpecBytes};

use crate::query::RelayRoute;
use crate::runtime::peer::Peer;
use crate::runtime::reactor::{AsyncMutex, Lease};
use crate::runtime::sal::{DirectGroup, GroupData, GroupTargets, SalFit, SalMessageKind, SalScope, SalWriter};
use crate::runtime::w2m::{W2mReceiver, W2mSlot};
use crate::runtime::wire::{
    self, unique_preflight_wire_schema, BACKFILL_DECISION_CHECKPOINT, BACKFILL_DECISION_CONTINUE,
    BACKFILL_DECISION_STOP, FLAG_SCAN_LAST,
};
use exchange::PendingRelay;
use gnitz_store::ops::{op_relay_broadcast, op_relay_scatter_consolidated, op_repartition_batches, ScatterSpec};
use gnitz_store::schema::key::PkBuf;
use gnitz_store::storage::Batch;
use gnitz_wire::control::peek_control_block_ipc;
use gnitz_wire::{WireConflictMode, FLAG_CONTINUATION, FLAG_HAS_DATA, FLAG_HAS_SCHEMA};
use scatter::{with_commit_indices, with_group, with_worker_indices};

// ---------------------------------------------------------------------------
// RelayPrepared — output of prepare_relay, input of emit_relay_with_decision
// ---------------------------------------------------------------------------

/// Materialised exchange relay: shard columns already resolved, payloads
/// already scattered into per-worker batches. Only the SAL write is
/// left, which `emit_relay_with_decision` does synchronously under
/// `sal_writer_excl`.
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
    worker_pids: RefCell<Vec<i32>>,
    sal: SalWriter,
    /// SAL-writer exclusivity, guarding `sal` above. The rule, not a roster of
    /// today's holders: hold it across the synchronous write and the wake that
    /// follows, drop it before awaiting. Without it a `Flush` landing between a
    /// `Tick` and its `ExchangeRelay` bumps the worker epoch and the relay is
    /// skipped with no error anywhere.
    ///
    /// **Non-reentrant**: never `.await` anything that re-acquires it.
    sal_writer_excl: AsyncMutex,
    /// Per-worker wakeup eventfds, in worker order. Signalling is not framing:
    /// the SAL writer decides a group's shape from its own worker count, and this
    /// only tells the workers to go look.
    m2w_efds: Vec<i32>,
    /// Shared with the reactor, which is the other reader. Every `W2mReceiver`
    /// method takes `&self` (its cursors live in the shared-memory rings), so
    /// both hold a clone rather than handing ownership across.
    w2m: Rc<W2mReceiver>,
    // Catalog pointer — reborrowed per-call because &mut self borrows conflict.
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

    /// The last tick round allocated. **Strictly increasing**, one per emitted
    /// tick group, and carried to the workers in that group's header `lsn` field,
    /// which is otherwise `0` for every command verb.
    ///
    /// A round is **per source, not per tick**: `run_tick` writes one group per
    /// pending tid, so a tick that drains K tids allocates K consecutive rounds
    /// and a view reached by two of them takes two delta rows rather than one.
    /// That is the behaviour to want — the rounds stay strictly increasing,
    /// `(n, T]` still names a contiguous span, and a view reached twice would
    /// otherwise have two deltas folded onto one `_tick` key, hiding the second
    /// source behind the first for no gain.
    ///
    /// It cannot be `lsn_alloc.published()`, which `run_tick` snapshots: that
    /// value is not unique per tick. The committer fires the auto-tick in Phase C,
    /// *before* Phase D publishes the zone LSN, so a round triggered by commit N
    /// snapshots commit N−1's; and a stream-only batch opens no zone and publishes
    /// nothing at all, so every tick in a pure-stream workload — the headline use
    /// case — would carry an identical value.
    ///
    /// Initialised to **1**, so every emitted round is ≥ 2. Round 1 is the boot
    /// itself: never emitted, stamping nothing. It exists so that the `T` in every
    /// reply is at least 1, which makes `0` the one `u64` that is not a round —
    /// what lets `after_tick = 0` mean "I hold nothing" without colliding with a
    /// real cursor. Without it a subscriber to a view that has not ticked since
    /// boot would be handed `T = 0`, store it, ask again with `0`, and be sent the
    /// whole view on every poll forever.
    ///
    /// It never has to survive a restart: the delta store does not either, and a
    /// restart mints a new `boot_nonce`, which every cursor's tag is checked
    /// against.
    tick_round: Cell<u64>,

    /// Feed-enabled view id → the last round that reached it. A poll at or beyond
    /// that value has no rows to return, because no round in `(after_tick, T]`
    /// reached the view at all, so the master answers it locally: one recv, one
    /// send, zero SAL bytes, zero wakeups.
    ///
    /// Written where the round is allocated, in `write_tick_group`, and for the
    /// same reason: a map maintained in `run_tick` would miss every round a
    /// `drain_tick_blocking` emits, and a view reached by one of those would then
    /// be gated as unchanged — a silent hole.
    ///
    /// An **over-approximation** that errs in the one safe direction: it is raised
    /// for every view in the emitted tid's forward closure, but a view whose
    /// partition saw no change produces no output and writes no delta row, so this
    /// can name a round that reached the view and left nothing. That poll falls
    /// through to the store and comes back empty — work, not a wrong answer. The
    /// direction that would be a wrong answer is the other one, and the closure
    /// cannot under-report: it is computed from the DAG edges the tick walk itself
    /// follows.
    ///
    /// A view absent from the map has never been reached and reads as round 1, so
    /// a bootstrap (`after_tick = 0`, below 1) is never gated. No boot seeding is
    /// needed: `MasterDispatcher::new` runs after every relation is registered and
    /// before the recovery tick sweep, so that sweep's own rounds populate it
    /// through this same function.
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
pub(crate) use dispatch::SeekReply;
use train::{drain_index_scan, expect_single_frame, forward_scan_slots, parse_train_header, scan_decode_err};
pub(crate) use unique_filter::UniqueFilter;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// A worker's failure reply, carried to the client as the pair the frame gave —
/// a worker may mint its own typed status (`STATUS_DELTA_EXPIRED`) and the
/// client reacts to the code rather than to the text.
///
/// The type is `gnitz-wire`'s, not this module's: the engine's `scan_spec`
/// mints exactly this pair, the worker splits it onto the wire, and
/// [`worker_error`] reassembles it here, so all three name one definition.
pub(crate) use gnitz_wire::WireFault as WorkerFault;

/// Render worker `w`'s reply as a fault, or `None` when it succeeded. The one
/// place the fault contract of a worker reply is read: `status != 0` means the
/// worker failed, `error_msg` holds its (UTF-8) text, and both halves travel on.
pub(crate) fn worker_error(w: usize, op: &str, ctrl: &gnitz_wire::control::DecodedControl) -> Option<WorkerFault> {
    (ctrl.status != 0).then(|| {
        let msg = String::from_utf8_lossy(&ctrl.error_msg);
        WorkerFault {
            status: ctrl.status,
            text: format!("worker {w}: {op}: {msg}"),
        }
    })
}

/// Which workers a scan-shaped dispatch goes to, before any request id exists.
#[derive(Clone, Copy)]
pub(crate) enum Fanout {
    /// Every worker; each answers for the rows it owns. One distinct scan
    /// request id per worker, and `nw` replies awaited in worker order.
    Broadcast,
    /// This worker alone. ONE scan request id, and one reply.
    One(usize),
}

/// Which workers answer a read of `target_id`: the one read-routing rule, taking
/// the `ReadSpec` its verb already unpacked when there is one.
///
/// Single-sourcing a replicated relation is **correctness**, not thrift, for two
/// of the callers: a fan-out returns the same row `nw` times, which
/// `fan_out_seek_by_index_collect` merges into `nw` duplicates for the client and
/// `PreflightAccumulator::offer` reads as a duplicate key.
pub(crate) fn read_fanout(disp: &MasterDispatcher, target_id: i64, spec: Option<SpecBytes<'_>>) -> Fanout {
    let Some(entry) = disp.cat().registry().relation(target_id) else {
        return Fanout::Broadcast;
    };
    if entry.is_replicated() {
        return Fanout::One(0);
    }
    spec.and_then(gnitz_wire::peek_pk_range)
        .and_then(|r| entry.schema().confined_worker(&r, disp.num_workers()))
        .map_or(Fanout::Broadcast, Fanout::One)
}

/// A dispatched scan: reply `i` arrives on its lease's id `i`, from
/// [`Self::worker`]. Dropping it discards every frame not yet drained.
pub(crate) struct ScanDispatch {
    lease: Lease,
    /// The one worker a unicast wrote to; `None` when reply `i` is worker `i`'s.
    worker: Option<usize>,
}

impl ScanDispatch {
    /// Lease this fan-out's request ids, before any await.
    fn alloc(reactor: &crate::runtime::reactor::Reactor, nw: usize, fanout: Fanout) -> ScanDispatch {
        match fanout {
            Fanout::Broadcast => ScanDispatch {
                lease: reactor.lease_train(nw),
                worker: None,
            },
            Fanout::One(w) => ScanDispatch {
                lease: reactor.lease_train(1),
                worker: Some(w),
            },
        }
    }

    /// The slots this scan's group writes, and the id each answers on.
    pub(crate) fn targets(&self) -> GroupTargets {
        match self.worker {
            None => GroupTargets::All(self.lease.id(0)),
            Some(worker) => GroupTargets::One { worker, req_id: self.lease.id(0) },
        }
    }

    /// The worker that produced reply `i`.
    pub(crate) fn worker(&self, i: usize) -> usize {
        self.worker.unwrap_or(i)
    }

    /// Await the first frame of every reply, in reply order.
    pub(crate) async fn await_slots(&self) -> Vec<W2mSlot> {
        self.lease.first_frames().await
    }

    /// The next frame of reply `i`'s train.
    pub(crate) async fn next_frame(&self, i: usize) -> W2mSlot {
        self.lease.next_frame(i).await
    }

    /// Await this scan's replies and forward every worker's train to the client
    /// in ascending worker order — the await+drain half of `fan_out_scan`, which
    /// the multi-scan path reaches on its own after dispatching every relation
    /// under one SAL cut. `Ok(false)` on client disconnect.
    ///
    /// Draining relation `i` fully before relation `i+1` is the FIFO invariant's
    /// supported usage: under `FLAG_SCAN_FIFO_REPLY` each worker streams the
    /// relations in request order, so relation `i`'s frames sit at the front of
    /// every ring with a live consumer.
    pub(crate) async fn await_and_forward(&self, peer: &Peer) -> Result<bool, WorkerFault> {
        let slots = self.await_slots().await;
        forward_scan_slots(peer, slots, self).await
    }
}

/// Fan a scan/seek group out to the workers under `submit` and await their
/// raw `W2mSlot` replies, returned so the caller can forward or merge them
/// without an intermediate decode/copy.
///
/// `unicast` selects the shape ([`Fanout`] states what each costs). The
/// returned `Vec` holds one slot per reply this scan expects, in the same
/// order.
///
/// `sal_excl` is held only for the synchronous write + signal phase and
/// released before awaiting replies. This serialises the SAL write against
/// concurrent checkpoint Flush groups: without the lock a fan-out
/// could write with the old epoch during the checkpoint window, workers
/// would skip it, and the caller would hang waiting for an ACK.
pub(crate) async fn dispatch_scan_fanout<F>(
    disp: &MasterDispatcher,
    reactor: &crate::runtime::reactor::Reactor,
    unicast: Fanout,
    submit: F,
) -> Result<(Vec<W2mSlot>, ScanDispatch), WorkerFault>
where
    F: FnOnce(GroupTargets) -> Result<(), WorkerFault>,
{
    let scan = ScanDispatch::alloc(reactor, disp.num_workers(), unicast);

    {
        let _guard = disp.sal_excl().lock().await;
        submit(scan.targets())?;
        disp.signal_reached([unicast]);
    }
    let slots = scan.await_slots().await;
    Ok((slots, scan))
}

/// One SAL cut across N scan-shaped requests: write all N groups back-to-back
/// under a single `sal_writer_excl` hold, then return each one's dispatch
/// handle. The read-side sibling of `commit_pushes`'s "N groups under one hold"
/// — the mutual exclusion forces the one cut, so every worker snapshots all N
/// at the same SAL position.
///
/// Deliberately NOT a loop over `dispatch_scan_fanout`: that re-locks per call,
/// which would destroy the cut.
///
/// `submit` writes request `i`'s group to `targets`, and MUST carry the `flags`
/// word it is handed: at N > 1 that is `FLAG_SCAN_FIFO_REPLY`, without which
/// ring order can differ from request order and the drain deadlocks (see the
/// worker's `pending_streams`). At N = 1 there is nothing to misorder, so the
/// flag is not stamped and a fitting reply keeps the inline path.
///
/// The **round** the groups are cut at is sampled under the hold and handed to
/// `submit` to stamp, then returned — so a terminal built from it names the cut
/// the workers actually read, and no caller has to smuggle it out of the
/// closure. An empty `fanouts` takes no hold and signals no worker.
pub(crate) async fn dispatch_scan_multi_fanout<F>(
    disp: &MasterDispatcher,
    reactor: &crate::runtime::reactor::Reactor,
    fanouts: &[Fanout],
    mut submit: F,
) -> Result<(Vec<ScanDispatch>, u64), WorkerFault>
where
    F: FnMut(usize, GroupTargets, u64, u64) -> Result<(), WorkerFault>,
{
    if fanouts.is_empty() {
        return Ok((Vec::new(), 0));
    }
    let nw = disp.num_workers();
    // Lease every request's ids BEFORE the lock (no await between here and the
    // write).
    let dispatches: Vec<ScanDispatch> = fanouts
        .iter()
        .map(|&unicast| ScanDispatch::alloc(reactor, nw, unicast))
        .collect();
    let fifo = if fanouts.len() > 1 {
        gnitz_wire::FLAG_SCAN_FIFO_REPLY
    } else {
        0
    };

    // One hold: write every group at a consecutive `write_cursor` position, then
    // signal. The reactor is single-threaded and each write has no `.await`, so
    // the groups land contiguously — the single cut. The lock releases at block
    // end, before the caller's first await.
    let round = {
        let _guard = disp.sal_excl().lock().await;
        let round = disp.last_tick_round();
        for (i, d) in dispatches.iter().enumerate() {
            submit(i, d.targets(), fifo, round)?;
        }
        disp.signal_reached(fanouts.iter().copied());
        round
    };
    Ok((dispatches, round))
}

/// Fixtures shared by the `master` submodules' unit tests: the batch/schema
/// builders and the inert dispatcher those suites construct.
#[cfg(test)]
#[path = "tests/fixtures.rs"]
pub(super) mod fixtures;
