//! Master-side dispatcher: fans out push/scan operations to worker processes
//! via the shared append-only log (SAL) and collects responses via per-worker
//! W2M regions. Eventfds provide cross-process signaling.

use std::rc::Rc;

use rustc_hash::{FxHashMap, FxHashSet};

use crate::catalog::CatalogEngine;
use crate::schema::SchemaDescriptor;
use crate::schema::{payload_native_key, pk_native_key, IndexKeySpec, SchemaColumn};
use gnitz_wire::PkColList;

use crate::ops::{
    op_relay_broadcast, op_relay_scatter_consolidated_mode, op_repartition_batches_mode, with_broadcast_indices,
    with_worker_indices, worker_for_partition, RouteMode,
};
use crate::runtime::peer::Peer;
use crate::runtime::reactor::{AsyncMutex, PendingRelay, ScanLease};
use crate::runtime::sal::{
    pack_gather_cols, unique_preflight_wire_schema, SalWriter, BACKFILL_DECISION_CHECKPOINT,
    BACKFILL_DECISION_CONTINUE, BACKFILL_DECISION_STOP, FLAG_BACKFILL, FLAG_DDL_SYNC, FLAG_EXCHANGE,
    FLAG_EXCHANGE_RELAY, FLAG_FLUSH, FLAG_FLUSH_EPH, FLAG_GATHER, FLAG_HAS_PK, FLAG_PUSH, FLAG_SEEK,
    FLAG_SEEK_BY_INDEX, FLAG_SEEK_BY_INDEX_RANGE_SAL, FLAG_SHUTDOWN, FLAG_TICK, FLAG_UNIQUE_PREFLIGHT,
};
use crate::runtime::w2m::{W2mReceiver, W2mSlot};
use crate::runtime::wire::{
    self, col_names_as_refs, peek_control_block, DecodedWire, SchemaWithVersion, WireConflictMode, FLAG_CONTINUATION,
    FLAG_HAS_DATA, FLAG_SCAN_LAST,
};
use crate::storage::{Batch, PkBuf};
use gnitz_wire::wire_flags_set_conflict_mode;
use index_router::PartitionRouter;

// ---------------------------------------------------------------------------
// RelayPrepared — output of prepare_relay, input of emit_relay
// ---------------------------------------------------------------------------

/// Materialised exchange relay: shard columns already resolved, payloads
/// already scattered into per-worker batches. Only the SAL write is
/// left, which `emit_relay` does synchronously under `sal_writer_excl`.
pub(crate) struct RelayPrepared {
    view_id: i64,
    source_id: i64,
    dest_batches: Vec<Batch>,
    schema: SchemaDescriptor,
    name_bytes: Rc<Vec<Vec<u8>>>,
}

// ---------------------------------------------------------------------------
// MasterDispatcher
// ---------------------------------------------------------------------------

pub struct MasterDispatcher {
    num_workers: usize,
    worker_pids: Vec<i32>,
    sal: SalWriter,
    w2m: Option<W2mReceiver>,
    /// Set after `take_w2m` hands the receiver to the reactor: a pointer to the
    /// reactor's stable `OnceCell` slot so `w2m()` keeps working for the one
    /// post-handoff caller — the reactor-parked stop-the-world CREATE-VIEW
    /// backfill. Null during boot (the receiver lives in `w2m` then). See
    /// `set_w2m_receiver_ptr` / `w2m`.
    w2m_ptr: *const W2mReceiver,
    // Catalog pointer — reborrowed per-call because &mut self borrows conflict.
    catalog: *mut CatalogEngine,
    router: PartitionRouter,
    /// Per-(table_id, packed_col_list) filter skipping redundant Phase 2
    /// unique-index broadcasts. The `u64` is `pack_pk_cols(col_indices)` — the
    /// same value stored in `IDXTAB_PAY_SOURCE_COLS` — so a composite index is
    /// identified by its whole column list, and dropping `(a, b)` never touches a
    /// distinct single-column filter on `a`. See the UniqueFilter comment block.
    unique_filters: FxHashMap<(i64, u64), UniqueFilter>,

    /// Per-`target_id` pool of `Batch`es reused by `build_check_batch` for
    /// FK / unique-index validation. After the awaited pipeline returns,
    /// each check's batch is taken via `mem::replace` and pushed back here;
    /// the next check on the same target reuses it via `clear` + reload.
    /// Schema staleness (DDL between bursts) is checked at pop time.
    check_batch_pool: FxHashMap<i64, Vec<Batch>>,
}

// Safety: MasterDispatcher is single-threaded (master process event loop).
unsafe impl Send for MasterDispatcher {}

mod dispatch;
mod index_router;
mod preflight;
mod unique_filter;

pub(crate) use dispatch::TxnFit;
#[cfg(test)]
pub(crate) use preflight::PreflightAccumulator;
pub(crate) use preflight::TxnFamily;
use unique_filter::UniqueFilter;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Return the first `worker N: <op>: <msg>` error in `decoded` (worker-index
/// order), or `None` if every reply has status 0. Shared by every fan-out
/// site that emits per-worker req_ids and decodes their replies in order.
pub(crate) fn first_worker_error(op: &str, decoded: &[DecodedWire]) -> Option<String> {
    worker_error_scan(op, decoded.iter().enumerate())
}

/// Variant of `first_worker_error` for the `Vec<Option<DecodedWire>>` slot
/// shape produced by `join_into`. Slots are guaranteed `Some` after the
/// future resolves; an unfilled slot indicates a bug in the join driver.
pub(crate) fn first_worker_error_opt(op: &str, decoded: &[Option<DecodedWire>]) -> Option<String> {
    worker_error_scan(
        op,
        decoded
            .iter()
            .enumerate()
            .map(|(w, d)| (w, d.as_ref().expect("join_into left a None slot — logic bug"))),
    )
}

fn worker_error_scan<'a>(op: &str, it: impl Iterator<Item = (usize, &'a DecodedWire)>) -> Option<String> {
    for (w, d) in it {
        if d.control.status != 0 {
            let msg = String::from_utf8_lossy(&d.control.error_msg);
            return Some(format!("worker {w}: {op}: {msg}"));
        }
    }
    None
}

fn scan_decode_err(w: usize, e: &'static str) -> String {
    format!("scan: worker {w}: decode error: {e}")
}

/// Fan-out shape for a scan-shaped dispatch over `target_id`: `0`
/// (single-source worker 0) when the relation is REPLICATED — every worker
/// holds an identical full copy, so a broadcast would stream/merge the same
/// rows `nw` times — else `-1` (broadcast). The single owner of the
/// replicated→single-source routing policy for `dispatch_scan_fanout` callers.
pub(crate) fn replicated_unicast(disp_ptr: *mut MasterDispatcher, target_id: i64) -> i32 {
    let replicated = unsafe { (*(*disp_ptr).catalog).relation_output_is_replicated(target_id) };
    if replicated {
        0
    } else {
        -1
    }
}

/// Allocate a scan group's per-worker request ids and register them into a
/// fresh `ScanLease`, the setup both `dispatch_scan_fanout` and
/// `dispatch_scan_multi_fanout` need before their SAL write. `unicast >= 0`
/// (single-source) allocates ONE id mirrored across the array — only that
/// worker's slot is written and replies; `unicast < 0` (broadcast) allocates
/// `nw` distinct ids. The lease is registered BEFORE any await, so a cancelled
/// drain still deregisters the ids and `route_scan_slot` discards late frames.
/// The returned lease MUST be bound to a named local held to the end of the
/// caller's drain scope (never a bare `_`, which would drop it immediately and
/// re-open the wedge).
fn alloc_scan_req_ids_and_lease(
    reactor: &crate::runtime::reactor::Reactor,
    nw: usize,
    unicast: i32,
) -> ([u64; crate::runtime::sal::MAX_WORKERS], ScanLease) {
    let mut req_ids = [0u64; crate::runtime::sal::MAX_WORKERS];
    if unicast >= 0 {
        // Single-source: one id mirrored across every slot; only worker
        // `unicast`'s slot is written and replies, so the lease holds that one id.
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
/// write's unicast argument can never diverge from it:
/// - `< 0` (broadcast): allocate `nw` distinct per-worker scan request ids,
///   signal every worker, and await every slot in worker order — the returned
///   `Vec` holds `nw` slots. The shape a full fan-out or a PK-scatter needs.
/// - `>= 0` (single-source, the worker index): allocate ONE scan request id
///   mirrored across the whole `req_ids` array (`write_group_direct` keys
///   replies by worker slot, and under unicast only that worker's slot is
///   written and replies — all on this id), signal only worker `unicast`, and
///   await its single slot — the returned `Vec` holds one slot. Used for a
///   REPLICATED relation (see `replicated_unicast`) and the single-worker
///   seek paths. The downstream drains
///   (`drain_index_scan` / `merge_index_scan`) are count-agnostic — they
///   iterate `slots.len()` — so a length-1 `Vec` merges exactly that worker's
///   stream.
///
/// `sal_excl` is held only for the synchronous write + signal phase and
/// released before awaiting replies. This serialises the SAL write against
/// concurrent checkpoint FLAG_FLUSH groups: without the lock a fan-out
/// could write with the old epoch during the checkpoint window, workers
/// would skip it, and the caller would hang waiting for an ACK.
pub(crate) async fn dispatch_scan_fanout<F>(
    disp_ptr: *mut MasterDispatcher,
    reactor: &crate::runtime::reactor::Reactor,
    sal_excl: &Rc<AsyncMutex<()>>,
    unicast: i32,
    submit: F,
) -> Result<(Vec<W2mSlot>, [u64; crate::runtime::sal::MAX_WORKERS], ScanLease), String>
where
    F: FnOnce(&mut MasterDispatcher, &[u64], i32) -> Result<(), String>,
{
    let nw = unsafe { (*disp_ptr).num_workers };
    let single = unicast >= 0;
    let (req_ids, lease) = alloc_scan_req_ids_and_lease(reactor, nw, unicast);

    {
        let _guard = sal_excl.lock().await;
        unsafe {
            let disp = &mut *disp_ptr;
            submit(disp, &req_ids[..nw], unicast)?;
            if single {
                disp.signal_one(unicast as usize);
            } else {
                disp.signal_all();
            }
        }
    }
    let slots = dispatch::await_scan_slots(reactor, unicast, &req_ids, nw).await;
    Ok((slots, req_ids, lease))
}

/// One relation's dispatch handle from `dispatch_scan_multi_fanout`: its
/// per-worker request ids, its unicast routing (`>= 0` = single worker index,
/// `-1` = broadcast), and the live `ScanLease` keeping those ids registered
/// until the master finishes draining the relation. The caller holds every
/// dispatch (hence every lease) for the whole of the sequential drain; dropping
/// them deregisters the ids and discards any queued/future frames, cancelling
/// the multi-scan on client death.
pub(crate) struct MultiScanDispatch {
    pub(crate) req_ids: [u64; crate::runtime::sal::MAX_WORKERS],
    pub(crate) unicast: i32,
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
/// `relations` gives, per relation in request order, `(tid, unicast,
/// effective_client_version)`; returns one `MultiScanDispatch` per relation in
/// the same order. Each relation's ids are registered into their own lease
/// BEFORE the lock (and before any await), so a cancelled drain still
/// deregisters them and `route_scan_slot` discards late frames.
pub(crate) async fn dispatch_scan_multi_fanout(
    disp_ptr: *mut MasterDispatcher,
    reactor: &crate::runtime::reactor::Reactor,
    sal_excl: &Rc<AsyncMutex<()>>,
    client_id: u64,
    relations: &[(i64, i32, u16)],
) -> Result<Vec<MultiScanDispatch>, String> {
    let nw = unsafe { (*disp_ptr).num_workers };
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
        unsafe {
            let disp = &mut *disp_ptr;
            for (&(tid, unicast, eff_ver), d) in relations.iter().zip(&dispatches) {
                let wire_flags =
                    gnitz_wire::wire_flags_set_schema_version(0, eff_ver) | gnitz_wire::FLAG_SCAN_FIFO_REPLY;
                disp.write_one_scan_group(tid, wire_flags, &d.req_ids[..nw], unicast, client_id)?;
            }
            disp.signal_all();
        }
    }
    Ok(dispatches)
}

/// Parse one frame header of worker `w`'s continuation train. Returns the
/// control block plus whether more frames follow, or `Err` (prefixed with
/// `what`) on a fault frame or a corrupt header. Every caller holds the
/// `ScanLease` from `dispatch_scan_fanout`, so propagating the `Err` is the
/// whole disposal story: the lease drop discards the undrained remainder at
/// the ring boundary.
///
/// A corrupt header yields no frame structure to follow, and a fault frame is
/// terminal by contract — `send_error` reports it as a single frame with
/// `status = STATUS_ERROR` and flags `0` (no `FLAG_SCAN_LAST`), and the
/// worker emits nothing more for the request. The error therefore MUST stop
/// the drain: keying `has_more` off `FLAG_SCAN_LAST` alone would read the
/// fault frame's `0` flags as "more coming" and block forever in
/// `await_scan_slot` on a frame that never arrives.
///
/// A healthy frame WITHOUT `FLAG_CONTINUATION` is a length-1 train: the
/// single-frame `send_response` reply shape carries no train flags at all, so
/// "no continuation flag" must read as terminal. Every multi-frame train
/// producer (worker scan chunks, chunked seek/gather replies, unique
/// pre-flight frames) sets `FLAG_CONTINUATION` on every frame and
/// `FLAG_SCAN_LAST` on the terminal one. This is the single definition of
/// that train contract, shared by every train consumer (`fan_out_scan_async`,
/// `drain_index_scan`, the pre-flight merge).
fn parse_train_header(slot: &W2mSlot, w: usize, what: &str) -> Result<(wire::DecodedControl, bool), String> {
    let ctrl = peek_control_block(slot.bytes()).map_err(|e| scan_decode_err(w, e))?;
    if ctrl.status != 0 {
        return Err(format!(
            "worker {}: {}: {}",
            w,
            what,
            String::from_utf8_lossy(&ctrl.error_msg)
        ));
    }
    let has_more = ctrl.flags & FLAG_SCAN_LAST == 0 && ctrl.flags & FLAG_CONTINUATION != 0;
    Ok((ctrl, has_more))
}

/// Enforce the single-frame reply contract on `slot`: a fault frame or corrupt
/// header errors like `parse_train_header`, and ANY `FLAG_CONTINUATION` frame
/// — including a length-1 train's terminal `FLAG_SCAN_LAST` frame — is
/// rejected. The slot is forwarded verbatim as a complete reply, so a chunked
/// train would be truncated to its first frame with the remainder silently
/// discarded by the lease drop. Callers only route requests whose replies fit
/// one frame (e.g. a unique point seek); a train means that invariant broke
/// (e.g. a shrunken GNITZ_REPLY_FRAME_BUDGET) — fail loudly instead.
fn expect_single_frame(slot: &W2mSlot, w: usize, what: &str) -> Result<wire::DecodedControl, String> {
    let (ctrl, _) = parse_train_header(slot, w, what)?;
    if ctrl.flags & FLAG_CONTINUATION != 0 {
        return Err(format!(
            "worker {w}: {what}: unexpected chunked reply on a single-frame path"
        ));
    }
    Ok(ctrl)
}

/// Fan-out reply-train drain shared by the unique-filter warmup and the
/// collect paths (index seek/range merge, gather). Invokes `on_batch` with the
/// zero-copy `MemBatch` and the raw frame byte length of every non-empty
/// frame, in worker order.
///
/// Returns on the FIRST error — worker fault, corrupt/undecodable frame,
/// schema mismatch, or an `Err` from `on_batch` — without draining the
/// remaining trains. All callers hold the `ScanLease` from
/// `dispatch_scan_fanout`; when the early `Err` unwinds it, the lease drop
/// (`reactor/mod.rs`) frees every parked slot and `route_scan_slot` discards
/// all later frames at the ring boundary, advancing `consume_cursor` — a
/// still-streaming worker cannot wedge in `send_encoded`, so decoding the
/// doomed remainder would be pure waste.
///
/// `expected` is validated against each train's first schema-bearing frame.
/// Worker reply schemas can lag the master's during DDL
/// races (`run_tick` releases the catalog read lock before awaiting ACKs; a
/// worker inside `do_exchange_wait` defers `DdlSync` but serves seeks inline;
/// seek handlers take no catalog lock at all), and the batch append helpers do
/// not validate shape — an unguarded mismatch is memory-unsafe garbage handed
/// onward under the master's schema block.
///
/// Continuation frames carry no schema; the schema + version saved from the
/// first frame is reused as a decode hint. The zero-copy `MemBatch` borrows
/// from `slot.bytes()`, so the slot is dropped only after `on_batch` returns.
async fn drain_index_scan(
    slots: Vec<W2mSlot>,
    req_ids: &[u64; crate::runtime::sal::MAX_WORKERS],
    reactor: &crate::runtime::reactor::Reactor,
    what: &str,
    expected: &SchemaDescriptor,
    mut on_batch: impl FnMut(&crate::storage::MemBatch<'_>, usize) -> Result<(), String>,
) -> Result<(), String> {
    for (w, mut slot) in slots.into_iter().enumerate() {
        let mut saved_schema: Option<(SchemaDescriptor, u16)> = None;
        loop {
            let (ctrl, has_more) = parse_train_header(&slot, w, what)?;
            let server_version = gnitz_wire::wire_flags_get_schema_version(ctrl.flags);
            let frame_len = slot.bytes().len();
            let schema_hint = saved_schema.as_ref().map(|(s, v)| SchemaWithVersion {
                descriptor: s,
                version: *v,
            });
            let zc = wire::decode_wire_ipc_zero_copy_with_ctrl(slot.bytes(), ctrl, schema_hint)
                .map_err(|e| scan_decode_err(w, e))?;
            if saved_schema.is_none() {
                if let Some(ref s) = zc.schema {
                    crate::schema::validate_schema_match(s, expected)
                        .map_err(|e| format!("worker {w}: {what}: {e}"))?;
                    saved_schema = Some((*s, server_version));
                }
            }
            if let Some(ref mb) = zc.data_batch {
                if mb.count > 0 {
                    on_batch(mb, frame_len)?;
                }
            }
            drop(zc); // borrows slot
            drop(slot);
            if !has_more {
                break;
            }
            slot = reactor.await_scan_slot(req_ids[w] as u32).await;
        }
    }
    Ok(())
}
