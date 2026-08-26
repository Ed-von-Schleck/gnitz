//! Master-side dispatcher: fans out push/scan operations to worker processes
//! via the shared append-only log (SAL) and collects responses via per-worker
//! W2M regions. Eventfds provide cross-process signaling.

pub(crate) mod scatter;

use std::cell::{Cell, RefCell};
use std::rc::Rc;

use rustc_hash::{FxHashMap, FxHashSet};

use gnitz_engine::catalog::CatalogEngine;
use gnitz_engine::schema::{unique_preflight_wire_schema, IndexKeySpec, SchemaDescriptor};
use gnitz_wire::PkColList;
use gnitz_wire::{payload_native_key, pk_native_key};

use crate::runtime::peer::Peer;
use crate::runtime::reactor::{AsyncMutex, PendingRelay, ScanLease};
use crate::runtime::reactor::{BACKFILL_DECISION_CHECKPOINT, BACKFILL_DECISION_CONTINUE, BACKFILL_DECISION_STOP};
use crate::runtime::sal::{
    DirectGroup, GroupData, GroupTargets, SalFit, SalWriter, FLAG_BACKFILL, FLAG_DDL_SYNC, FLAG_EXCHANGE_RELAY,
    FLAG_FLUSH, FLAG_FLUSH_EPH, FLAG_GATHER, FLAG_HAS_PK, FLAG_PUSH, FLAG_SEEK, FLAG_SEEK_BY_INDEX, FLAG_SHUTDOWN,
    FLAG_TICK, FLAG_UNIQUE_PREFLIGHT,
};
use crate::runtime::w2m::{W2mReceiver, W2mSlot};
use crate::runtime::wire::{
    self, peek_control_block_ipc, DecodedWire, SchemaWithVersion, WireConflictMode, FLAG_CONTINUATION, FLAG_EXCHANGE,
    FLAG_HAS_DATA, FLAG_HAS_SCHEMA, FLAG_SCAN_LAST,
};
use gnitz_engine::ops::{op_relay_broadcast, op_relay_scatter_consolidated_mode, op_repartition_batches_mode};
use gnitz_engine::query::RelayRoute;
use gnitz_engine::schema::key::PkBuf;
use gnitz_engine::storage::Batch;
use gnitz_wire::wire_flags_set_conflict_mode;
use scatter::{with_commit_indices, with_worker_indices};

// ---------------------------------------------------------------------------
// RelayPrepared — output of prepare_relay, input of emit_relay_with_decision
// ---------------------------------------------------------------------------

/// Materialised exchange relay: shard columns already resolved, payloads
/// already scattered into per-worker batches. Only the SAL write is
/// left, which `emit_relay_with_decision` does synchronously under
/// `sal_writer_excl`.
pub(crate) struct RelayPrepared {
    view_id: i64,
    source_id: i64,
    dest: RelayDest,
    schema: SchemaDescriptor,
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
    num_workers: usize,
    worker_pids: RefCell<Vec<i32>>,
    sal: SalWriter,
    /// SAL-writer exclusivity, guarding `sal` above. The rule, not a roster of
    /// today's holders: hold it across the synchronous write + `signal_all`, drop
    /// it before awaiting. Without it a `FLAG_FLUSH` landing between a `FLAG_TICK`
    /// and its `FLAG_EXCHANGE_RELAY` bumps the worker epoch and the relay is
    /// skipped with no error anywhere.
    ///
    /// **Non-reentrant**: never `.await` anything that re-acquires it.
    sal_writer_excl: Rc<AsyncMutex>,
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
    /// Per-(table_id, packed_col_list) filter skipping redundant unique-index
    /// occupancy broadcasts. The `u64` is `pack_pk_cols(col_indices)` — the same
    /// value stored in `IDXTAB_PAY_SOURCE_COLS` — so a composite index is
    /// identified by its whole column list, and dropping `(a, b)` never touches
    /// a distinct single-column filter on `a`. See the UniqueFilter comment
    /// block.
    unique_filters: RefCell<FxHashMap<(i64, u64), UniqueFilter>>,

    /// Pool of `Batch`es reused by the check builders for FK / unique-index
    /// validation, keyed per probe target and key columns (see
    /// `PipelinedCheck::pool_slot`). After the awaited pipeline returns,
    /// `reclaim_check_batches` pushes each check's batch back here; the next
    /// probe on the same slot reuses it via `clear` + reload. Schema staleness
    /// (DDL between bursts) is still checked at pop time.
    check_batch_pool: RefCell<FxHashMap<preflight::PoolSlot, Vec<Batch>>>,

    /// The generation the last ephemeral round stamped. Read only inside a
    /// `debug_assert!` (`note_flush_round`); two tests pin it.
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
    /// same reason: a map maintained in `run_tick` would miss every round the
    /// three `drain_tick_blocking` callers emit, and a view reached by one of those
    /// would then be gated as unchanged — a silent hole.
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
    /// tag. Not from a seeded generator: two back-to-back boots must not collide.
    boot_nonce: u64,
}

mod dispatch;
mod preflight;
mod train;
mod unique_filter;

pub(crate) use dispatch::scan_spec_route;
#[cfg(test)]
pub(crate) use preflight::PreflightAccumulator;
pub(crate) use preflight::TxnFamily;
use train::{drain_index_scan, expect_single_frame, forward_scan_slots, parse_train_header, scan_decode_err};
pub(crate) use unique_filter::UniqueFilter;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// A worker's failure reply, carried to the client as the pair the frame gave —
/// a worker may mint its own typed status (`STATUS_DELTA_EXPIRED`) and the
/// client reacts to the code rather than to the text.
///
/// The type is `gnitz-wire`'s, not this module's: the engine's `scan_spec_family`
/// mints exactly this pair, the worker splits it onto the wire, and
/// [`worker_error`] reassembles it here, so all three name one definition.
pub(crate) use gnitz_wire::WireFault as WorkerFault;

/// Render worker `w`'s reply as a fault, or `None` when it succeeded. The one
/// place the fault contract of a worker reply is read: `status != 0` means the
/// worker failed, `error_msg` holds its (UTF-8) text, and both halves travel on.
pub(crate) fn worker_error(w: usize, op: &str, ctrl: &wire::DecodedControl) -> Option<WorkerFault> {
    (ctrl.status != 0).then(|| {
        let msg = String::from_utf8_lossy(&ctrl.error_msg);
        WorkerFault {
            status: ctrl.status,
            text: format!("worker {w}: {op}: {msg}"),
        }
    })
}

/// The first worker error across a fan-out's replies, in worker-index order.
/// Slots are `Some` once `join_into`'s future resolves; a `None` is a bug in
/// the join driver.
///
/// Flattened to its text: these are the ACK-shaped fan-outs (push, tick, flush,
/// relay), whose callers report a failure and have no typed status to forward.
/// Only the scan-forward stack carries the whole [`WorkerFault`].
pub(crate) fn first_worker_error_opt(op: &str, decoded: &[Option<DecodedWire>]) -> Option<String> {
    decoded.iter().enumerate().find_map(|(w, d)| {
        let d = d.as_ref().expect("join_into left a None slot — logic bug");
        worker_error(w, op, &d.control).map(|f| f.text)
    })
}

/// Await one ACK per id in `ids`, returning the first worker error. Clears
/// `acks` before returning so the `DecodedWire` heap fields (data batch, error
/// message) are freed rather than held while the caller parks; the capacity of
/// both scratch vectors survives for the next round.
pub(crate) async fn await_worker_acks(
    reactor: &crate::runtime::reactor::Reactor,
    ids: &[u64],
    op: &str,
    futs: &mut Vec<crate::runtime::reactor::ReplyFuture>,
    acks: &mut Vec<Option<DecodedWire>>,
) -> Result<(), String> {
    futs.clear();
    futs.extend(ids.iter().map(|&id| reactor.await_reply(id)));
    crate::runtime::reactor::join_into(futs, acks).await;
    let err = first_worker_error_opt(op, acks);
    acks.clear();
    err.map_or(Ok(()), Err)
}

/// Which workers a scan-shaped dispatch goes to, before any request id exists.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Fanout {
    /// Every worker; each answers for the rows it owns. One distinct scan
    /// request id per worker, and `nw` replies awaited in worker order.
    Broadcast,
    /// This worker alone. ONE scan request id, and one reply.
    One(usize),
}

/// Fan-out shape for a scan-shaped dispatch over `target_id`: worker 0 alone
/// when the relation is REPLICATED — every worker holds an identical full copy,
/// so a broadcast would stream/merge the same rows `nw` times — else broadcast.
/// The single owner of the replicated→single-source routing policy for
/// `dispatch_scan_fanout` callers.
pub(crate) fn replicated_unicast(disp: &MasterDispatcher, target_id: i64) -> Fanout {
    if disp.cat().dag().relation_is_replicated(target_id) {
        Fanout::One(0)
    } else {
        Fanout::Broadcast
    }
}

/// A dispatched scan: who answers it, on which request ids, and the lease
/// keeping those ids registered until the caller finishes draining.
///
/// Replies are addressed only through [`Self::reply`], which returns the
/// producing worker and its request id together.
///
/// **Hold it to the end of the drain.** Dropping it releases the lease, which
/// deregisters the ids; `route_scan_slot` then discards every queued and future
/// frame at the ring boundary, which is what cancels a scan on client death and
/// what would silently truncate one that is still wanted.
pub(crate) struct ScanDispatch {
    /// Reply `i` arrives on `ids[i]`, for `i < n`. Indexed by **reply**, never
    /// by worker — a unicast has one reply, so it uses one slot.
    ids: [u64; crate::runtime::sal::MAX_WORKERS],
    n: usize,
    /// The one worker a unicast wrote to; `None` when reply `i` is worker `i`'s.
    worker: Option<usize>,
    _lease: ScanLease,
}

impl ScanDispatch {
    /// Allocate this fan-out's request ids and register them into a fresh
    /// `ScanLease`, before any await.
    fn alloc(reactor: &crate::runtime::reactor::Reactor, nw: usize, fanout: Fanout) -> ScanDispatch {
        let mut ids = [0u64; crate::runtime::sal::MAX_WORKERS];
        let n = match fanout {
            Fanout::Broadcast => nw,
            Fanout::One(_) => 1,
        };
        let mut scan_ids = [0u32; crate::runtime::sal::MAX_WORKERS];
        for (r, s) in ids[..n].iter_mut().zip(&mut scan_ids[..n]) {
            *r = reactor.alloc_scan_request_id();
            *s = *r as u32;
        }
        ScanDispatch {
            ids,
            n,
            worker: match fanout {
                Fanout::Broadcast => None,
                Fanout::One(w) => Some(w),
            },
            _lease: reactor.scan_lease(&scan_ids[..n]),
        }
    }

    /// The slots this scan's group writes, and the id each answers on.
    pub(crate) fn targets(&self) -> GroupTargets<'_> {
        match self.worker {
            None => GroupTargets::All(&self.ids[..self.n]),
            Some(worker) => GroupTargets::One {
                worker,
                req_id: self.ids[0],
            },
        }
    }

    /// Replies to expect, in arrival order — one per written slot.
    pub(crate) fn reply_count(&self) -> usize {
        self.n
    }

    /// Reply `i`: the worker that produced it, and the request id it arrives on.
    pub(crate) fn reply(&self, i: usize) -> (usize, u64) {
        (self.worker.unwrap_or(i), self.ids[i])
    }
}

/// Fan a scan/seek group out to the workers under `submit` and await their
/// raw `W2mSlot` replies, returned so the caller can forward or merge them
/// without an intermediate decode/copy.
///
/// `unicast` selects the shape ([`Fanout`] states what each costs). The
/// returned `Vec` holds one slot per [`ScanDispatch::reply_count`], in the same
/// order.
///
/// `sal_excl` is held only for the synchronous write + signal phase and
/// released before awaiting replies. This serialises the SAL write against
/// concurrent checkpoint FLAG_FLUSH groups: without the lock a fan-out
/// could write with the old epoch during the checkpoint window, workers
/// would skip it, and the caller would hang waiting for an ACK.
pub(crate) async fn dispatch_scan_fanout<F>(
    disp: &MasterDispatcher,
    reactor: &crate::runtime::reactor::Reactor,
    unicast: Fanout,
    submit: F,
) -> Result<(Vec<W2mSlot>, ScanDispatch), String>
where
    F: FnOnce(GroupTargets<'_>) -> Result<(), String>,
{
    let scan = ScanDispatch::alloc(reactor, disp.num_workers, unicast);

    {
        let _guard = disp.sal_excl().lock().await;
        submit(scan.targets())?;
        match unicast {
            Fanout::One(w) => disp.signal_one(w),
            Fanout::Broadcast => disp.signal_all(),
        }
    }
    let slots = dispatch::await_scan_slots(reactor, &scan).await;
    Ok((slots, scan))
}

/// One SAL cut across N relations: write all N scan groups back-to-back under a
/// single `sal_writer_excl` hold, then return each relation's dispatch handle.
/// The read-side sibling of `commit_pushes`'s "N groups under one hold" — the
/// mutual exclusion both forces the one cut (no push / tick / commit-zone group
/// can land between the scan groups, so every worker snapshots all N relations
/// at the same SAL position) and, as a side effect, serialises two concurrent
/// multi-scans.
///
/// Deliberately NOT a loop over `dispatch_scan_fanout`: that re-locks per call,
/// so N calls would reopen the lock N times and destroy the one cut. This awaits
/// nothing — the lock is held only for the synchronous write + signal, and the
/// caller drains each relation's reply train sequentially in request order (the
/// `FLAG_SCAN_FIFO_REPLY` contract). Every group carries that flag so workers
/// queue the reply in request order.
///
/// `relations` gives, per relation in request order, `(tid, routing,
/// effective_client_version)`; returns one [`ScanDispatch`] per relation in the
/// same order.
pub(crate) async fn dispatch_scan_multi_fanout(
    disp: &MasterDispatcher,
    reactor: &crate::runtime::reactor::Reactor,
    client_id: u64,
    relations: &[(i64, Fanout, u16)],
) -> Result<Vec<ScanDispatch>, String> {
    let nw = disp.num_workers;
    // Allocate ids + register every relation's lease BEFORE the lock (no await
    // between here and the write).
    let dispatches: Vec<ScanDispatch> = relations
        .iter()
        .map(|&(_tid, unicast, _ver)| ScanDispatch::alloc(reactor, nw, unicast))
        .collect();

    // One hold: write every scan group at a consecutive `write_cursor` position,
    // then signal once. The reactor is single-threaded and each write has no
    // `.await`, so the groups land contiguously — the single cut. The lock
    // releases at block end, before the caller's first await.
    {
        let _guard = disp.sal_excl().lock().await;
        for (&(tid, _unicast, eff_ver), d) in relations.iter().zip(&dispatches) {
            let wire_flags = gnitz_wire::wire_flags_set_schema_version(0, eff_ver) | gnitz_wire::FLAG_SCAN_FIFO_REPLY;
            disp.write_group(
                wire::WireMsg {
                    target_id: tid as u64,
                    client_id,
                    flags: wire_flags,
                    ..Default::default()
                },
                GroupData::NONE,
                0,
                0,
                d.targets(),
            )?;
        }
        disp.signal_all();
    }
    Ok(dispatches)
}

/// Fixtures shared by the `master` submodules' unit tests, so a schema or
/// row-builder change is made once rather than in three test modules.
#[cfg(test)]
pub(super) mod fixtures {
    use gnitz_engine::schema::key::PkBuf;
    use gnitz_engine::schema::{SchemaColumn, SchemaDescriptor};
    use gnitz_engine::storage::Batch;
    use gnitz_wire::type_code;

    /// A single U64 PK column, no payload.
    pub(super) fn u64_schema() -> SchemaDescriptor {
        SchemaDescriptor::new(&[SchemaColumn::new(type_code::U64, 0)], &[0])
    }

    /// PK U64 at index 0, payload U64 at index 1.
    pub(super) fn two_col_schema() -> SchemaDescriptor {
        SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::U64, 1),
            ],
            &[0],
        )
    }

    /// Rows are `(pk, weight, null_word, payload_col1_value)`.
    pub(super) fn make_row_batch(schema: SchemaDescriptor, rows: &[(u128, i64, u64, i64)]) -> Batch {
        let mut batch = Batch::with_capacity(schema, rows.len().max(1));
        for &(pk, weight, null_word, payload_val) in rows {
            let lo = [payload_val];
            let hi = [0u64];
            let null_ptr: *const u8 = std::ptr::null();
            let ptrs = [null_ptr];
            let lens = [0u32];
            unsafe {
                batch.append_row_simple(pk, weight, null_word, &lo, &hi, &ptrs, &lens);
            }
        }
        batch
    }

    /// Concatenate per-column OPK byte images into one compound PK.
    pub(super) fn compound_pk_bytes(parts: &[&[u8]]) -> PkBuf {
        let mut v = Vec::new();
        for p in parts {
            v.extend_from_slice(p);
        }
        PkBuf::from_bytes(&v)
    }
}
