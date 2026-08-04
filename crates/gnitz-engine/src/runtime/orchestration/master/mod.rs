//! Master-side dispatcher: fans out push/scan operations to worker processes
//! via the shared append-only log (SAL) and collects responses via per-worker
//! W2M regions. Eventfds provide cross-process signaling.

pub(crate) mod scatter;

use std::cell::RefCell;
use std::rc::Rc;

use rustc_hash::{FxHashMap, FxHashSet};

use crate::catalog::CatalogEngine;
use crate::schema::{IndexKeySpec, SchemaDescriptor};
use gnitz_wire::PkColList;
use gnitz_wire::{payload_native_key, pk_native_key};

use crate::ops::{op_relay_broadcast, op_relay_scatter_consolidated_mode, op_repartition_batches_mode, RouteMode};
use crate::runtime::peer::Peer;
use crate::runtime::reactor::{AsyncMutex, PendingRelay, ScanLease};
use crate::runtime::sal::{
    pack_gather_cols, unique_preflight_wire_schema, SalWriter, BACKFILL_DECISION_CHECKPOINT,
    BACKFILL_DECISION_CONTINUE, BACKFILL_DECISION_STOP, FLAG_BACKFILL, FLAG_DDL_SYNC, FLAG_EXCHANGE,
    FLAG_EXCHANGE_RELAY, FLAG_FLUSH, FLAG_FLUSH_EPH, FLAG_GATHER, FLAG_HAS_PK, FLAG_PUSH, FLAG_SEEK,
    FLAG_SEEK_BY_INDEX, FLAG_SHUTDOWN, FLAG_TICK, FLAG_UNIQUE_PREFLIGHT,
};
use crate::runtime::w2m::{W2mReceiver, W2mSlot};
use crate::runtime::wire::{
    self, peek_control_block, DecodedWire, SchemaWithVersion, WireConflictMode, FLAG_CONTINUATION, FLAG_HAS_DATA,
    FLAG_HAS_SCHEMA, FLAG_SCAN_LAST,
};
use crate::schema::key::PkBuf;
use crate::storage::Batch;
use gnitz_wire::wire_flags_set_conflict_mode;
use gnitz_wire::worker_for_partition;
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
}

mod dispatch;
mod preflight;
mod train;
mod unique_filter;

pub(crate) use dispatch::{scan_spec_route, TxnFit};
#[cfg(test)]
pub(crate) use preflight::PreflightAccumulator;
pub(crate) use preflight::TxnFamily;
use train::{drain_index_scan, expect_single_frame, forward_scan_slots, parse_train_header, scan_decode_err};
use unique_filter::UniqueFilter;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Render worker `w`'s reply as an error, or `None` when it succeeded. The one
/// place the fault contract of a worker reply is read: `status != 0` means the
/// worker failed and `error_msg` holds its (UTF-8) text.
pub(crate) fn worker_error(w: usize, op: &str, ctrl: &wire::DecodedControl) -> Option<String> {
    (ctrl.status != 0).then(|| {
        let msg = String::from_utf8_lossy(&ctrl.error_msg);
        format!("worker {w}: {op}: {msg}")
    })
}

/// The first worker error across a fan-out's replies, in worker-index order.
/// Slots are `Some` once `join_into`'s future resolves; a `None` is a bug in
/// the join driver.
pub(crate) fn first_worker_error_opt(op: &str, decoded: &[Option<DecodedWire>]) -> Option<String> {
    decoded.iter().enumerate().find_map(|(w, d)| {
        let d = d.as_ref().expect("join_into left a None slot — logic bug");
        worker_error(w, op, &d.control)
    })
}

/// Which workers a scan-shaped dispatch goes to.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Fanout {
    /// Every worker; each answers for the partitions it owns.
    Broadcast,
    /// This worker alone.
    One(usize),
}

impl Fanout {
    /// The SAL group's slot selector: `-1` writes every worker's slot, `>= 0`
    /// only that one's. The single conversion to the wire-side encoding.
    fn sal_slot(self) -> i32 {
        match self {
            Fanout::Broadcast => -1,
            Fanout::One(w) => w as i32,
        }
    }

    /// The worker that produced reply `i`. Under `One` every reply is that
    /// worker's, so the index does not name it.
    fn worker_of(self, i: usize) -> usize {
        match self {
            Fanout::Broadcast => i,
            Fanout::One(w) => w,
        }
    }
}

/// Fan-out shape for a scan-shaped dispatch over `target_id`: worker 0 alone
/// when the relation is REPLICATED — every worker holds an identical full copy,
/// so a broadcast would stream/merge the same rows `nw` times — else broadcast.
/// The single owner of the replicated→single-source routing policy for
/// `dispatch_scan_fanout` callers.
pub(crate) fn replicated_unicast(disp: &MasterDispatcher, target_id: i64) -> Fanout {
    if disp.cat().dag.relation_is_replicated(target_id) {
        Fanout::One(0)
    } else {
        Fanout::Broadcast
    }
}

/// Allocate a scan group's per-worker request ids and register them into a
/// fresh `ScanLease`, the setup both `dispatch_scan_fanout` and
/// `dispatch_scan_multi_fanout` need before their SAL write. `Fanout::One`
/// allocates ONE id mirrored across the array — only that worker's slot is
/// written and replies; `Fanout::Broadcast` allocates `nw` distinct ids. The lease is registered BEFORE any await, so a cancelled
/// drain still deregisters the ids and `route_scan_slot` discards late frames.
/// The returned lease MUST be bound to a named local held to the end of the
/// caller's drain scope (never a bare `_`, which would drop it immediately and
/// re-open the wedge).
fn alloc_scan_req_ids_and_lease(
    reactor: &crate::runtime::reactor::Reactor,
    nw: usize,
    unicast: Fanout,
) -> ([u64; crate::runtime::sal::MAX_WORKERS], ScanLease) {
    let mut req_ids = [0u64; crate::runtime::sal::MAX_WORKERS];
    if unicast != Fanout::Broadcast {
        // Single-source: one id mirrored across every slot; only that worker's
        // slot is written and replies, so the lease holds that one id.
        let id = reactor.alloc_scan_request_id();
        req_ids[..nw].fill(id);
        (req_ids, reactor.scan_lease(&[id as u32]))
    } else {
        // Broadcast: a distinct id per worker, each registered in the lease.
        let mut scan_ids = [0u32; crate::runtime::sal::MAX_WORKERS];
        for (r, s) in req_ids[..nw].iter_mut().zip(&mut scan_ids[..nw]) {
            let id = reactor.alloc_scan_request_id();
            *r = id;
            *s = id as u32;
        }
        (req_ids, reactor.scan_lease(&scan_ids[..nw]))
    }
}

/// Fan a scan/seek group out to the workers under `submit` and await their
/// raw `W2mSlot` replies, returned so the caller can forward or merge them
/// without an intermediate decode/copy.
///
/// `unicast` selects the shape and is also passed to `submit` so the group
/// write's routing can never diverge from it:
/// - `Broadcast`: allocate `nw` distinct per-worker scan request ids, signal
///   every worker, and await every slot in worker order — the returned `Vec`
///   holds `nw` slots. The shape a full fan-out or a PK-scatter needs.
/// - `One(w)`: allocate ONE scan request id mirrored across the whole `req_ids`
///   array (`write_group_direct` keys replies by worker slot, and only `w`'s
///   slot is written and replies — all on this id), signal only `w`, and await
///   its single slot — the returned `Vec` holds one slot. Used for a REPLICATED
///   relation (see `replicated_unicast`) and the single-worker seek paths. The
///   downstream drains (`drain_index_scan` / `merge_index_scan`) are
///   count-agnostic — they iterate `slots.len()` — so a length-1 `Vec` merges
///   exactly that worker's stream.
///
/// `sal_excl` is held only for the synchronous write + signal phase and
/// released before awaiting replies. This serialises the SAL write against
/// concurrent checkpoint FLAG_FLUSH groups: without the lock a fan-out
/// could write with the old epoch during the checkpoint window, workers
/// would skip it, and the caller would hang waiting for an ACK.
pub(crate) async fn dispatch_scan_fanout<F>(
    disp: &MasterDispatcher,
    reactor: &crate::runtime::reactor::Reactor,
    sal_excl: &Rc<AsyncMutex>,
    unicast: Fanout,
    submit: F,
) -> Result<(Vec<W2mSlot>, [u64; crate::runtime::sal::MAX_WORKERS], ScanLease), String>
where
    F: FnOnce(&MasterDispatcher, &[u64], Fanout) -> Result<(), String>,
{
    let nw = disp.num_workers;
    let (req_ids, lease) = alloc_scan_req_ids_and_lease(reactor, nw, unicast);

    {
        let _guard = sal_excl.lock().await;
        submit(disp, &req_ids[..nw], unicast)?;
        match unicast {
            Fanout::One(w) => disp.signal_one(w),
            Fanout::Broadcast => disp.signal_all(),
        }
    }
    let slots = dispatch::await_scan_slots(reactor, unicast, &req_ids, nw).await;
    Ok((slots, req_ids, lease))
}

/// One relation's dispatch handle from `dispatch_scan_multi_fanout`: its
/// per-worker request ids, its routing, and the live `ScanLease` keeping those
/// ids registered
/// until the master finishes draining the relation. The caller holds every
/// dispatch (hence every lease) for the whole of the sequential drain; dropping
/// them deregisters the ids and discards any queued/future frames, cancelling
/// the multi-scan on client death.
pub(crate) struct MultiScanDispatch {
    pub(crate) req_ids: [u64; crate::runtime::sal::MAX_WORKERS],
    pub(crate) unicast: Fanout,
    // Held only for its RAII effect (the `_` name silences the never-read lint);
    // its drop deregisters the relation's scan ids.
    _lease: ScanLease,
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
/// effective_client_version)`; returns one `MultiScanDispatch` per relation in
/// the same order. Each relation's ids are registered into their own lease
/// BEFORE the lock (and before any await), so a cancelled drain still
/// deregisters them and `route_scan_slot` discards late frames.
pub(crate) async fn dispatch_scan_multi_fanout(
    disp: &MasterDispatcher,
    reactor: &crate::runtime::reactor::Reactor,
    sal_excl: &Rc<AsyncMutex>,
    client_id: u64,
    relations: &[(i64, Fanout, u16)],
) -> Result<Vec<MultiScanDispatch>, String> {
    let nw = disp.num_workers;
    // Allocate ids + register every relation's lease BEFORE the lock (no await
    // between here and the write). A broadcast relation gets one id per worker;
    // a unicast one gets a single id mirrored across the array (only its
    // worker's slot is written and replies).
    let mut dispatches: Vec<MultiScanDispatch> = Vec::with_capacity(relations.len());
    for &(_tid, unicast, _ver) in relations {
        let (req_ids, lease) = alloc_scan_req_ids_and_lease(reactor, nw, unicast);
        dispatches.push(MultiScanDispatch {
            req_ids,
            unicast,
            _lease: lease,
        });
    }

    // One hold: write every scan group at a consecutive `write_cursor` position,
    // then signal once. The reactor is single-threaded and each write has no
    // `.await`, so the groups land contiguously — the single cut. The lock
    // releases at block end, before the caller's first await.
    {
        let _guard = sal_excl.lock().await;
        {
            for (&(tid, unicast, eff_ver), d) in relations.iter().zip(&dispatches) {
                let wire_flags =
                    gnitz_wire::wire_flags_set_schema_version(0, eff_ver) | gnitz_wire::FLAG_SCAN_FIFO_REPLY;
                disp.write_scan_group(tid, 0, wire_flags, &d.req_ids[..nw], unicast, client_id, &[])?;
            }
            disp.signal_all();
        }
    }
    Ok(dispatches)
}

/// Fixtures shared by the `master` submodules' unit tests, so a schema or
/// row-builder change is made once rather than in three test modules.
#[cfg(test)]
pub(super) mod fixtures {
    use crate::schema::key::PkBuf;
    use crate::schema::{type_code, SchemaColumn, SchemaDescriptor};
    use crate::storage::Batch;

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
