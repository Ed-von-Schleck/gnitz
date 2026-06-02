//! Worker process event loop.
//!
//! Owns a subset of partitions for every user table. Receives requests from
//! the master via the SAL (shared append-only log), sends responses via a
//! per-worker W2M shared region.

use std::collections::HashMap;
use std::rc::Rc;

use crate::catalog::{CatalogEngine, FIRST_USER_TABLE_ID};
use crate::schema::SchemaDescriptor;
use crate::dag::ExchangeCallback;
use crate::storage::{BlobCacheGuard, FlushWork, PkBuf};
use crate::runtime::wire::{self as ipc, STATUS_OK, STATUS_ERROR, FLAG_CONTINUATION, FLAG_SCAN_LAST};
use crate::runtime::sal::{
    SAL_MMAP_SIZE, FLAG_EXCHANGE, FLAG_CHECKPOINT,
    SalReader, SalMessageKind,
    schema_wire_safe,
};
use crate::runtime::w2m::W2mWriter;
use crate::runtime::w2m_ring;
use crate::storage::Batch;

// ---------------------------------------------------------------------------
// PendingScan
// ---------------------------------------------------------------------------

/// State for a multi-chunk SCAN response in progress. The worker emits one
/// frame per `drain_sal` pass; `next_row == 0` means the first chunk still
/// needs a schema block. Non-zero `next_row` is a pure-data continuation.
struct PendingScan {
    batch: Rc<Batch>,
    next_row: usize,
    request_id: u64,
    client_id: u64,
    target_id: u64,
    prebuilt_schema: Option<Rc<Vec<u8>>>,
    server_version: u16,
}

// ---------------------------------------------------------------------------
// WorkerExchangeHandler
// ---------------------------------------------------------------------------

/// Lookup target for FLAG_HAS_PK requests.
enum HasPkLookup {
    /// Check the table's primary-key store.
    PrimaryKey,
    /// Check a unique secondary index on `col_idx` (0-indexed).
    UniqueIndex(u32),
}

impl HasPkLookup {
    fn from_wire(seek_col_idx: u64) -> Self {
        if seek_col_idx > 0 {
            HasPkLookup::UniqueIndex((seek_col_idx - 1) as u32)
        } else {
            HasPkLookup::PrimaryKey
        }
    }
}

/// A DDL_SYNC message received during an exchange wait, decoded eagerly.
struct DeferredDdl {
    target_id: i64,
    batch: Batch,
}

// ---------------------------------------------------------------------------
// DispatchContext + DispatchOutcome
//
// `dispatch` is the single source of truth for the inline-vs-defer matrix
// (see the doc above `WorkerProcess::dispatch`). The context tells the
// dispatcher whether the worker is at top-level draining the SAL or
// blocked inside `do_exchange_wait`; the same `(context, kind)` pair
// always maps to the same decision so behavior cannot drift between the
// two call sites.
// ---------------------------------------------------------------------------

#[allow(clippy::large_enum_variant)]
#[derive(Clone, Copy)]
enum DispatchContext {
    /// The worker is draining the SAL from its main run loop.
    TopLevel,
    /// The worker is blocked in `do_exchange_wait`, awaiting an
    /// EXCHANGE_RELAY whose `(view_id, source_id)` matches `want_key`.
    /// `schema` is the outgoing batch's schema; used to fabricate an
    /// empty relay payload if the master sends a header-only relay.
    InsideExchangeWait { want_key: (i64, i64), schema: Option<SchemaDescriptor> },
}

/// Result of a single `dispatch` call.
#[allow(clippy::large_enum_variant)]
enum DispatchOutcome {
    /// The dispatcher handled the message; the caller should keep
    /// draining the SAL.
    Continue,
    /// The worker is shutting down; unwind out of the loops.
    Shutdown,
    /// `InsideExchangeWait` saw the relay it was waiting for; return
    /// this batch to the DAG.
    RelayMatched(Batch),
}

struct WorkerExchangeHandler {
    stash: HashMap<i64, Batch>,
    deferred: Vec<DeferredDdl>,
    /// FLAG_TICK messages encountered inside `do_exchange_wait` while waiting
    /// for a FLAG_EXCHANGE_RELAY. Stashed here and replayed (via
    /// `replay_deferred_ticks`) after the current tick's ACK is sent, so the
    /// master observes ACKs in SAL arrival order and a later tick cannot
    /// re-enter `view_id` while an outer exchange for the same view is still
    /// awaiting its relay. The req_id is the original FLAG_TICK request id
    /// so the replayed ACK is routable.
    deferred_ticks: Vec<(i64, u64)>,
    /// FLAG_EXCHANGE_RELAY messages whose `(view_id, source_id)` doesn't
    /// match the active exchange wait. Keyed by the tuple so a stashed
    /// relay for one source never satisfies a wait for a different source
    /// of the same view (which would drive the inline DAG re-entry with
    /// the wrong sharding columns).
    pending_relays: HashMap<(i64, i64), Batch>,
}

impl WorkerExchangeHandler {
    /// Stash a `FLAG_PRELOADED_EXCHANGE` batch for `view_id`.
    ///
    /// **Invariant: at most one preloaded batch per `view_id` per epoch.**
    /// The stash is keyed by `view_id` only — a second insert for the same
    /// view (typical for joins where the master emits preloaded data per
    /// input source) would silently overwrite the first. The debug assert
    /// turns that latent bug into a panic in dev/test builds; the master
    /// must batch all preloaded data for a view into a single SAL group
    /// before any consumer (`do_exchange_wait`) drains it.
    ///
    /// As of this writing the master does not emit `FLAG_PRELOADED_EXCHANGE`
    /// at all; this code path is dead and the assert documents the
    /// constraint that any future emitter must respect. If multi-source
    /// preloaded groups become a real protocol need, re-key `stash` to
    /// `(view_id, source_id)` (mirroring `pending_relays`) and update both
    /// `dispatch_top_level`'s prelude drain and `do_exchange_wait`'s stash
    /// lookup.
    fn stash_preloaded(&mut self, view_id: i64, batch: Batch) {
        debug_assert!(
            !self.stash.contains_key(&view_id),
            "stash_preloaded: view_id={} already has a preloaded batch — \
             multi-source preloaded for the same view would silently overwrite",
            view_id,
        );
        self.stash.insert(view_id, batch);
    }
}

/// Bridges the DAG's `ExchangeCallback` requirement to `WorkerProcess`.
/// Holds a mutable reference to the worker so `do_exchange` can re-enter
/// the worker's handlers (`handle_push`, `handle_tick`) inline when those
/// messages arrive mid-wait. `tick_request_id` is the id of the message
/// that kicked off this DAG evaluation; echoed on FLAG_EXCHANGE so the
/// master's accumulator stays routable.
struct WorkerExchangeCtx<'a> {
    worker: &'a mut WorkerProcess,
    tick_request_id: u64,
}

impl<'a> ExchangeCallback for WorkerExchangeCtx<'a> {
    fn do_exchange(
        &mut self,
        view_id: i64,
        batch: &Batch,
        source_id: i64,
    ) -> Batch {
        self.worker.do_exchange_wait(view_id, batch, source_id, self.tick_request_id)
    }
}

// ---------------------------------------------------------------------------
// WorkerProcess
// ---------------------------------------------------------------------------

pub struct WorkerProcess {
    worker_id: u32,
    #[allow(dead_code)]
    num_workers: usize,
    master_pid: i32,
    catalog: *mut CatalogEngine,
    sal_reader: SalReader,
    w2m_writer: W2mWriter,
    exchange: WorkerExchangeHandler,
    pending_deltas: HashMap<i64, Batch>,
    pending_scan: Option<PendingScan>,
    read_cursor: u64,
    expected_epoch: u32,
}

/// Deduplicate a list of (dev, ino, fd) triples by (dev, ino), returning one fd per
/// unique directory inode. Both fields are u64 to handle varying ino_t/dev_t widths.
pub(crate) fn dedup_dirfds(mut inodes: Vec<(u64, u64, libc::c_int)>) -> Vec<libc::c_int> {
    // Returns one fd per unique (dev, ino) so a shared directory is fsynced
    // once. The fds are per-flush and owned by the caller, which closes EVERY
    // collected fd (not just this deduped subset) after the fsync — so this
    // helper must NOT close the duplicates it drops.
    inodes.sort_unstable_by_key(|&(dev, ino, _)| (dev, ino));
    inodes.dedup_by_key(|&mut (dev, ino, _)| (dev, ino));
    inodes.into_iter().map(|(_, _, fd)| fd).collect()
}

/// Submit one FSYNC(DATASYNC) SQE per fd and await completion of all of them.
/// Drains the SQ when full so a chunk of more than `sq_entries` fds requires
/// multiple `submit_and_wait` calls.
pub(crate) fn uring_batch_fdatasync(
    ring: &mut io_uring::IoUring,
    fds: &[libc::c_int],
) -> Result<(), String> {
    uring_batch_fdatasync_with(ring, fds, |r, want| r.submit_and_wait(want))
}

fn uring_batch_fdatasync_with(
    ring: &mut io_uring::IoUring,
    fds: &[libc::c_int],
    mut submit: impl FnMut(&mut io_uring::IoUring, usize) -> std::io::Result<usize>,
) -> Result<(), String> {
    if fds.is_empty() { return Ok(()); }
    let sq_capacity = ring.params().sq_entries() as usize;
    let mut completed = 0usize;
    let mut pushed = 0usize;
    while completed < fds.len() {
        while pushed < fds.len() {
            if ring.submission().len() >= sq_capacity { break; }
            let sqe = io_uring::opcode::Fsync::new(io_uring::types::Fd(fds[pushed]))
                .flags(io_uring::types::FsyncFlags::DATASYNC)
                .build();
            unsafe {
                ring.submission().push(&sqe).map_err(|_| "uring SQ push".to_string())?;
            }
            pushed += 1;
        }
        let want = pushed - completed;
        match submit(ring, want) {
            Ok(_) => {}
            Err(ref e) if e.raw_os_error() == Some(libc::EINTR) => {
                // Drain any CQEs that arrived before the signal.
                for cqe in ring.completion() {
                    if cqe.result() < 0 {
                        return Err(format!("fdatasync via uring failed: {}", cqe.result()));
                    }
                    completed += 1;
                }
                continue;
            }
            Err(e) => return Err(format!("uring submit_and_wait: {}", e)),
        }
        for cqe in ring.completion() {
            if cqe.result() < 0 {
                return Err(format!("fdatasync via uring failed: {}", cqe.result()));
            }
            completed += 1;
        }
    }
    Ok(())
}

/// Filter a check-batch to the rows whose PK `exists` accepts, copying each
/// matched row into the result. Keys on verbatim OPK bytes, correct for every
/// PK width. `exists` receives the row's raw OPK PK bytes.
fn filter_by_pk_bytes(
    batch: &Option<Batch>,
    schema: SchemaDescriptor,
    n: usize,
    mut exists: impl FnMut(&[u8]) -> bool,
) -> Batch {
    let mut result = Batch::with_schema(schema, n);
    if let Some(ref b) = batch {
        let mut blob_cache = BlobCacheGuard::acquire(&schema, n);
        for i in 0..n {
            let pkb = b.get_pk_bytes(i);
            if exists(pkb) {
                result.append_row_from_source_bytes(pkb, 1, b, i, blob_cache.get_mut());
            }
        }
    }
    result
}

impl WorkerProcess {
    pub fn new(
        worker_id: u32,
        num_workers: usize,
        master_pid: i32,
        catalog: *mut CatalogEngine,
        sal_reader: SalReader,
        w2m_writer: W2mWriter,
    ) -> Self {
        // Isolate this worker's scratch operator-state tables from siblings'
        // (all forked workers share the view directory under one base_dir).
        crate::compiler::set_worker_rank(worker_id);
        WorkerProcess {
            worker_id,
            num_workers,
            master_pid,
            catalog,
            sal_reader,
            w2m_writer,
            exchange: WorkerExchangeHandler {
                stash: HashMap::new(),
                deferred: Vec::new(),
                deferred_ticks: Vec::new(),
                pending_relays: HashMap::new(),
            },
            pending_deltas: HashMap::new(),
            pending_scan: None,
            read_cursor: 0,
            expected_epoch: 1,
        }
    }

    fn cat(&mut self) -> &mut CatalogEngine {
        unsafe { &mut *self.catalog }
    }

    // ── Main event loop ────────────────────────────────────────────────

    pub fn run(&mut self) -> i32 {
        // Startup ACK is unsolicited; request_id=0 is the reserved untagged slot.
        self.send_ack(0, 0, 0);

        loop {
            // Skip the SAL wait when a chunked SCAN is in progress: the pending
            // state drives the next drain_sal to emit the next chunk immediately.
            if self.pending_scan.is_none() {
                let ready = self.sal_reader.wait(1000);
                if ready == 0 {
                    let ppid = unsafe { libc::getppid() };
                    if ppid != self.master_pid {
                        self.shutdown();
                        return 0;
                    }
                    continue;
                }
                if ready < 0 {
                    continue;
                }
            }

            if self.drain_sal() {
                return 0;
            }
        }
    }

    /// Process all pending SAL message groups. Returns true on shutdown.
    fn drain_sal(&mut self) -> bool {
        // Emit the next pending scan chunk before draining new SAL messages.
        // One chunk per drain_sal pass; send_encoded provides backpressure.
        if self.pending_scan.is_some() {
            self.emit_pending_scan_chunk();
        }
        while let Some((kind, target_id, wire)) = self.next_sal_message() {
            match self.dispatch_top_level(kind, target_id, wire) {
                DispatchOutcome::Continue => {
                    // Replay ticks deferred during any exchange wait now that the
                    // outer tick's ACK has been sent. Pushes are handled inline in
                    // `do_exchange_wait` (safe because user-table push only
                    // appends to `pending_deltas`) so we don't defer them.
                    if !self.exchange.deferred_ticks.is_empty() {
                        self.replay_deferred_ticks();
                    }
                }
                DispatchOutcome::Shutdown => return true,
                DispatchOutcome::RelayMatched(_) => {
                    // RelayMatched is only produced inside `do_exchange_wait`;
                    // the top-level dispatcher classifies ExchangeRelay as a
                    // protocol bug and never emits this outcome.
                    debug_assert!(false, "RelayMatched at top-level drain_sal");
                }
            }
        }
        false
    }

    /// Emit one frame of a pending chunked scan. Clears `pending_scan` when
    /// the last chunk is sent. Called at the top of every `drain_sal` pass when
    /// `pending_scan.is_some()`.
    fn emit_pending_scan_chunk(&mut self) {
        let (batch, next_row, request_id, client_id, target_id, prebuilt_schema, server_version) = {
            let ps = match self.pending_scan.as_ref() {
                Some(ps) => ps,
                None => return,
            };
            (
                Rc::clone(&ps.batch),
                ps.next_row,
                ps.request_id,
                ps.client_id,
                ps.target_id,
                ps.prebuilt_schema.clone(),
                ps.server_version,
            )
        };

        let is_first = next_row == 0;
        // prebuilt_opt drives the schema block: Some on the first chunk when the
        // client needs the schema, None on continuations and schema-suppressed frames.
        // encode_wire_into_range uses the prebuilt bytes directly; no schema arg needed.
        let prebuilt_opt: Option<&[u8]> = if is_first {
            prebuilt_schema.as_deref().map(Vec::as_slice)
        } else {
            None
        };

        let remaining = batch.count - next_row;
        // Compute rows per chunk via linear interpolation (wire-safe schemas have
        // constant per-row stride, so wire size is linear in count).
        let sz_0 = ipc::wire_size_range(STATUS_OK, &[], None, None, &batch, 0, prebuilt_opt);
        let sz_1 = ipc::wire_size_range(STATUS_OK, &[], None, None, &batch, 1, prebuilt_opt);
        let per_row = sz_1.saturating_sub(sz_0);
        #[allow(clippy::manual_checked_ops)]
        let max_rows = if per_row > 0 {
            let usable = (w2m_ring::MAX_W2M_MSG as usize).saturating_sub(sz_0);
            (usable / per_row).max(1).min(remaining)
        } else {
            remaining.max(1)
        };
        let has_more = next_row + max_rows < batch.count;
        // FLAG_CONTINUATION is always set on worker scan frames so the client's
        // loop termination ("stop on no FLAG_CONTINUATION") still works.
        // FLAG_SCAN_LAST is the W2M-internal signal that this is the last chunk.
        // server_version is always embedded so the master decode path can verify.
        let flags: u64 = gnitz_wire::wire_flags_set_schema_version(
            FLAG_CONTINUATION | if !has_more { FLAG_SCAN_LAST } else { 0 },
            server_version,
        );
        // wire_size_range is linear in count for wire-safe schemas; avoid a third call.
        let sz = sz_0 + per_row * max_rows;
        self.w2m_writer.send_encoded(sz, request_id as u32, |buf| {
            ipc::encode_wire_into_range(
                buf, 0, target_id, client_id, flags,
                0, STATUS_OK, // request_id=0 in payload; ring prefix carries the req_id
                None, &batch,
                next_row, max_rows,
                prebuilt_opt,
            );
        });

        if has_more {
            debug_assert!(self.pending_scan.is_some());
            self.pending_scan.as_mut().unwrap().next_row = next_row + max_rows;
        } else {
            self.pending_scan = None;
        }
    }

    /// Drain one SAL group, advancing `read_cursor` only on a clean read.
    ///
    /// `expected_epoch` is re-read on each call: a FLAG_FLUSH dispatched
    /// inline bumps it, and a stale snapshot would cause every
    /// post-checkpoint message to fail the epoch check forever (spinning
    /// at an unchanged cursor). The cursor is advanced *only* on a
    /// successful epoch match — a post-epoch group remains parked at the
    /// cursor until the cursor is reset by checkpoint.
    fn next_sal_message(&mut self)
        -> Option<(SalMessageKind, i64, Option<&'static [u8]>)>
    {
        if self.read_cursor + 8 >= SAL_MMAP_SIZE as u64 {
            return None;
        }
        let (msg, new_cursor) = self.sal_reader.try_read(self.read_cursor)?;
        if msg.epoch != self.expected_epoch {
            return None;
        }
        self.read_cursor = new_cursor;
        Some((
            msg.kind,
            msg.target_id as i64,
            msg.wire_data,
        ))
    }

    /// Dispatch a SAL message at the top level — drains any preloaded
    /// prelude groups before handing the underlying message to the
    /// unified dispatcher.
    fn dispatch_top_level(
        &mut self,
        mut kind: SalMessageKind,
        mut target_id: i64,
        mut wire: Option<&'static [u8]>,
    ) -> DispatchOutcome {
        // Drain a (possibly chained) preloaded-exchange prelude before
        // processing the underlying message. Each preloaded group stashes
        // a batch keyed by view_id; the next group at the same epoch
        // becomes the message we actually dispatch.
        while kind == SalMessageKind::PreloadedExchange {
            let _ = self.dispatch(DispatchContext::TopLevel, kind, target_id, wire);
            match self.next_sal_message() {
                Some((k, t, w)) => { kind = k; target_id = t; wire = w; }
                None => return DispatchOutcome::Continue,
            }
        }
        self.dispatch(DispatchContext::TopLevel, kind, target_id, wire)
    }

    /// Decode wire bytes for dispatch.
    ///
    /// At top-level we go through the per-target schema cache — the wire
    /// for hot kinds (PUSH/TICK/SEEK) carries the same data schema for
    /// every group with that `target_id`, so re-skipping the encoded
    /// schema block on cache hit is a worthwhile optimization. DDL_SYNC
    /// (catalog schema) and HAS_PK (index or parent-table schema) bypass
    /// the cache: their schema blocks may differ from the cached
    /// user-table schema for the same `target_id`.
    ///
    /// Inside an exchange wait we always do a full decode — same reason
    /// as DDL_SYNC/HAS_PK — and the schema cache is intentionally
    /// untouched there.
    /// The single source of truth for the inline-vs-defer matrix.
    /// Match exhaustiveness (`match (ctx, kind)`) means a new
    /// `SalMessageKind` variant cannot be added without explicitly
    /// deciding its behavior in both contexts. The walk-the-matrix
    /// test (`tests::test_dispatch_matrix_*`) makes the *behavioral*
    /// spec for each non-trivial cell explicit.
    ///
    /// | Kind              | TopLevel               | InsideExchangeWait                    |
    /// |-------------------|------------------------|---------------------------------------|
    /// | Shutdown          | inline                 | inline                                |
    /// | Flush             | inline (resets cursor) | inline (same)                         |
    /// | DdlSync           | apply via cat().ddl_sync | defer to exchange.deferred          |
    /// | ExchangeRelay     | (unreachable, warn)    | match want_key OR park in pending_relays |
    /// | PreloadedExchange | drained by dispatch_top_level prelude | stash by view_id    |
    /// | Backfill          | inline                 | inline                                |
    /// | HasPk             | inline                 | inline                                |
    /// | Push              | inline (must)          | inline (must — sal_writer_excl deadlock) |
    /// | Tick              | inline + replay defer  | defer to exchange.deferred_ticks      |
    /// | SeekByIndex       | inline                 | inline                                |
    /// | Seek              | inline                 | inline                                |
    /// | Scan              | inline                 | inline                                |
    ///
    /// Reasons for the non-trivial rules (each cites the bug or
    /// invariant that fixing the rule violates):
    ///
    /// * **Tick defer inside exchange.** An inline tick eval would
    ///   re-enter `view_id` with a different source and produce
    ///   schema-mismatched relays. Defer + replay after the outer
    ///   tick's ACK so the master observes ACKs in SAL arrival order.
    ///   See `WorkerExchangeHandler::deferred_ticks` and the SAL
    ///   exchange-interleaving fix (a later tick re-entering an
    ///   in-flight view exposed the original bug).
    ///
    /// * **DdlSync defer inside exchange.** Applying a catalog mutation
    ///   inline would race in-flight DAG eval and create schema
    ///   inconsistency across nested exchanges. Stage the batch in
    ///   `exchange.deferred` and apply it after the DAG returns
    ///   (`dispatch_deferred`).
    ///
    /// * **Push inline (mandatory in both contexts).** The master
    ///   committer holds `sal_writer_excl` for the entire push group's
    ///   write+signal+fsync window while awaiting the push ACK; the
    ///   relay_loop needs the same mutex to publish this exchange's
    ///   relay, so deferring the push would deadlock the worker
    ///   against the committer. See `committer::Shared::sal_writer_excl`
    ///   and `commit_pushes` for the lock discipline.
    ///
    /// * **ExchangeRelay key match.** `(view_id, source_id)` — keying by
    ///   view alone breaks when a join view has multiple input tables:
    ///   a relay for source A would satisfy a wait for source B and
    ///   drive the inline DAG with the wrong sharding columns. Mismatched
    ///   relays are parked in `pending_relays` and picked up by the next
    ///   nested wait that asks for the right pair.
    ///
    /// * **PreloadedExchange stash.** Keyed by `view_id` only on the
    ///   stash side (`WorkerExchangeHandler::stash`). See `stash_preloaded`
    ///   for the single-source-per-view invariant.
    fn dispatch(
        &mut self,
        ctx: DispatchContext,
        kind: SalMessageKind,
        target_id: i64,
        wire: Option<&'static [u8]>,
    ) -> DispatchOutcome {
        // Unicast kinds without a per-worker payload aren't for us.
        if wire.is_none() && !kind.is_broadcast() {
            return DispatchOutcome::Continue;
        }

        match (ctx, kind) {
            // ── Tick: inline at top-level, defer inside exchange wait ──
            (DispatchContext::TopLevel, SalMessageKind::Tick) => {
                self.run_via_dispatch_inner(kind, target_id, wire)
            }
            (DispatchContext::InsideExchangeWait { .. }, SalMessageKind::Tick) => {
                let req_id = wire
                    .and_then(|d| ipc::decode_wire(d).ok())
                    .map(|d| d.control.request_id)
                    .unwrap_or(0);
                self.exchange.deferred_ticks.push((target_id, req_id));
                DispatchOutcome::Continue
            }

            // ── DdlSync: apply at top-level, defer inside ──────────────
            (DispatchContext::TopLevel, SalMessageKind::DdlSync) => {
                self.run_via_dispatch_inner(kind, target_id, wire)
            }
            (DispatchContext::InsideExchangeWait { .. }, SalMessageKind::DdlSync) => {
                if let Some(data) = wire {
                    match ipc::decode_wire(data) {
                        Ok(decoded) => {
                            if let Some(batch) = decoded.data_batch {
                                self.exchange.deferred.push(DeferredDdl { target_id, batch });
                            }
                        }
                        Err(e) => {
                            // A dropped DDL permanently diverges this worker's
                            // catalog from the master — silently wrong results.
                            // Shut down rather than continue with a stale catalog.
                            gnitz_warn!(
                                "W{} FATAL: failed to decode deferred DDL for tid={}: {}",
                                self.worker_id, target_id, e,
                            );
                            self.shutdown();  // calls libc::_exit — does not return
                            return DispatchOutcome::Shutdown;  // unreachable; satisfies type
                        }
                    }
                }
                DispatchOutcome::Continue
            }

            // ── ExchangeRelay: unreachable at top, match-or-queue inside
            (DispatchContext::TopLevel, SalMessageKind::ExchangeRelay) => {
                gnitz_warn!(
                    "W{} unexpected ExchangeRelay at top-level dispatch tid={}",
                    self.worker_id, target_id,
                );
                DispatchOutcome::Continue
            }
            (DispatchContext::InsideExchangeWait { want_key, schema },
                SalMessageKind::ExchangeRelay) => {
                // source_id is echoed back via seek_pk_lo (see
                // master::relay_exchange). Decode once, before taking
                // the batch out.
                let decoded = wire.and_then(|d| ipc::decode_wire(d).ok());
                let relay_source_id = decoded.as_ref()
                    .map(|d| d.control.seek_pk as i64)
                    .unwrap_or(0);
                let relay_batch = decoded
                    .and_then(|d| d.data_batch)
                    .unwrap_or_else(|| {
                        let empty_schema = schema.unwrap_or_default();
                        Batch::with_schema(empty_schema, 0)
                    });
                let relay_key = (target_id, relay_source_id);
                if relay_key == want_key {
                    return DispatchOutcome::RelayMatched(relay_batch);
                }
                self.exchange.pending_relays.insert(relay_key, relay_batch);
                DispatchOutcome::Continue
            }

            // ── PreloadedExchange: stash in both contexts ──────────────
            // (At TopLevel, dispatch_top_level's prelude loop calls us
            // for each chained preloaded group before the underlying
            // message; the stash side-effect is the same in both
            // contexts.)
            (_, SalMessageKind::PreloadedExchange) => {
                if let Some(data) = wire {
                    if let Ok(decoded) = ipc::decode_wire(data) {
                        if let Some(batch) = decoded.data_batch {
                            self.exchange.stash_preloaded(target_id, batch);
                        } else if let Some(s) = decoded.schema {
                            self.exchange.stash_preloaded(target_id, Batch::with_schema(s, 0));
                        }
                    }
                }
                DispatchOutcome::Continue
            }

            // ── All others: identical inline behavior in both contexts ─
            (_, SalMessageKind::Shutdown)
            | (_, SalMessageKind::Flush)
            | (_, SalMessageKind::Backfill)
            | (_, SalMessageKind::HasPk)
            | (_, SalMessageKind::Gather)
            | (_, SalMessageKind::Push)
            | (_, SalMessageKind::SeekByIndex)
            | (_, SalMessageKind::Seek)
            | (_, SalMessageKind::Scan) => {
                self.run_via_dispatch_inner(kind, target_id, wire)
            }
        }
    }

    /// Decode (with the context-appropriate schema cache policy) and
    /// dispatch through `dispatch_inner`, mapping its `DispatchResult`
    /// into a `DispatchOutcome`. Errors are sent on the W2M ring with
    /// the inbound request_id so the master reactor can route the
    /// failure back to the original caller.
    fn run_via_dispatch_inner(
        &mut self,
        kind: SalMessageKind,
        target_id: i64,
        wire: Option<&'static [u8]>,
    ) -> DispatchOutcome {
        let decoded = wire.and_then(|data| ipc::decode_wire(data).ok());
        let request_id = decoded.as_ref().map(|d| d.control.request_id).unwrap_or(0);
        match self.dispatch_inner(kind, target_id, decoded, request_id) {
            DispatchResult::Continue => DispatchOutcome::Continue,
            DispatchResult::Error(msg) => {
                self.send_error(&msg, request_id);
                if kind == SalMessageKind::DdlSync {
                    // DDL application failure on trusted master→worker IPC means
                    // memory corruption or an engine bug; continuing would leave
                    // this worker with a permanently stale catalog.
                    gnitz_warn!(
                        "W{} FATAL: DdlSync application failed for tid={}: {}. Shutting down.",
                        self.worker_id, target_id, msg,
                    );
                    self.shutdown();  // calls libc::_exit — does not return
                    return DispatchOutcome::Shutdown;  // unreachable; satisfies type
                }
                DispatchOutcome::Continue
            }
            DispatchResult::Shutdown => DispatchOutcome::Shutdown,
        }
    }

    fn dispatch_inner(
        &mut self,
        kind: SalMessageKind,
        target_id: i64,
        decoded: Option<ipc::DecodedWire>,
        request_id: u64,
    ) -> DispatchResult {
        // Extract control fields before consuming decoded
        let seek_pk = decoded.as_ref().map(|d| d.control.seek_pk).unwrap_or(0);
        let seek_col_idx = decoded.as_ref().map(|d| d.control.seek_col_idx).unwrap_or(0);
        let client_id = decoded.as_ref().map(|d| d.control.client_id).unwrap_or(0);
        let ctrl_wire_flags = decoded.as_ref().map(|d| d.control.flags).unwrap_or(0);
        let client_version = gnitz_wire::wire_flags_get_schema_version(ctrl_wire_flags);
        // Wide-PK seek key tail (bytes 16..stride); empty for narrow PKs. Must be
        // extracted before `decoded` is consumed by the `data_batch` take below.
        let seek_pk_extra: Vec<u8> = decoded.as_ref()
            .map(|d| d.control.seek_pk_extra.clone())
            .unwrap_or_default();

        // Extract batch (consumes decoded)
        let batch = decoded.and_then(|d| d.data_batch);

        match kind {
            SalMessageKind::Shutdown => {
                self.shutdown();
                DispatchResult::Shutdown
            }

            SalMessageKind::Flush => {
                self.read_cursor = 0;
                self.expected_epoch += 1;
                match self.handle_flush_all() {
                    Ok(()) => self.send_ack(0, FLAG_CHECKPOINT as u64, request_id),
                    Err(msg) => self.send_error(&msg, request_id),
                }
                DispatchResult::Continue
            }

            SalMessageKind::DdlSync => {
                if let Some(batch) = batch {
                    if batch.count > 0 {
                        if let Err(msg) = self.cat().ddl_sync(target_id, batch) {
                            return DispatchResult::Error(msg);
                        }
                        // Drop hooks queue the entity's directory, but the master
                        // (which shares this on-disk tree) physically removes it
                        // after the DDL zone is durable. Discard the worker's
                        // redundant queue so it cannot grow unbounded.
                        self.cat().discard_pending_dir_deletions();
                        gnitz_debug!("W{} ddl_sync tid={}", self.worker_id, target_id);
                    }
                }
                DispatchResult::Continue
            }

            SalMessageKind::Backfill => {
                if let Err(msg) = self.handle_backfill(target_id, request_id) {
                    return DispatchResult::Error(msg);
                }
                self.send_ack(target_id as u64, 0, request_id);
                DispatchResult::Continue
            }

            SalMessageKind::HasPk => {
                let lookup = HasPkLookup::from_wire(seek_col_idx);
                if let Err(msg) = self.handle_has_pk(target_id, batch, lookup, request_id, client_id, seek_pk) {
                    return DispatchResult::Error(msg);
                }
                DispatchResult::Continue
            }

            SalMessageKind::Gather => {
                // The projected column mask rides in `seek_col_idx`. The PK
                // batch arrives in `data_batch` (a worker with an empty
                // sublist still replies — the master joins one reply per
                // worker). PKs come pre-sorted from the master's global sort
                // (scatter preserves per-worker order), aiding the cursor.
                let project: Vec<u8> =
                    crate::runtime::sal::unpack_gather_cols(seek_col_idx).collect();
                // The batch PK region holds verbatim OPK bytes (the master packs
                // them via `extend_pk_bytes`), so seek them directly with
                // `gather_family_bytes` for every PK width. Round-tripping a
                // narrow key back through `get_pk` → `opk_key` would re-OPK-encode
                // it (double sign-flip for signed; scrambled compound bytes),
                // probing a key that matches no stored row. `get_pk_bytes` works
                // for both narrow and wide PKs.
                let pks: Vec<PkBuf> = match &batch {
                    Some(b) => (0..b.count).map(|i| PkBuf::from_bytes(b.get_pk_bytes(i))).collect(),
                    None => Vec::new(),
                };
                let gathered = self.cat().gather_family_bytes(target_id, &pks, &project);
                match gathered {
                    Ok(result) => {
                        let schema = result.schema;
                        self.send_response(
                            target_id as u64, Some(&result), schema.as_ref(),
                            request_id, client_id, 0,
                        );
                    }
                    Err(msg) => return DispatchResult::Error(msg),
                }
                DispatchResult::Continue
            }

            SalMessageKind::Push => {
                if let Some(batch) = batch {
                    if batch.count > 0 {
                        if let Err(msg) = self.handle_push(target_id, batch, request_id) {
                            return DispatchResult::Error(msg);
                        }
                    }
                }
                self.send_ack(target_id as u64, 0, request_id);
                DispatchResult::Continue
            }

            SalMessageKind::Tick => {
                if let Err(msg) = self.handle_tick(target_id, request_id) {
                    return DispatchResult::Error(msg);
                }
                self.send_ack(target_id as u64, 0, request_id);
                DispatchResult::Continue
            }

            SalMessageKind::SeekByIndex => {
                let col_idx = seek_col_idx as u32;
                // The indexed source column is at most 16 bytes (UUID/U128) and
                // we never need `seek_pk_extra` here — index keys are always
                // promoted to ≤16 bytes. We pass the value's native source-width
                // bytes; `seek_by_index` zero-extends and OPK-encodes them to the
                // promoted index key before seeking the OPK-at-rest index.
                let col_size = self.cat()
                    .get_schema_desc(target_id)
                    .and_then(|s| s.columns.get(col_idx as usize).map(|c| c.size() as usize))
                    .unwrap_or(0);
                if col_size == 0 {
                    return DispatchResult::Error(format!(
                        "seek_by_index: invalid column index {} for table {}",
                        col_idx, target_id,
                    ));
                }
                let key_bytes = seek_pk.to_le_bytes();
                let prefix = &key_bytes[..col_size.min(16)];
                match self.cat().seek_by_index(target_id, col_idx, prefix) {
                    Ok(result) => {
                        let schema = self.cat().get_schema_desc(target_id);
                        self.send_response(target_id as u64, result.as_ref(), schema.as_ref(), request_id, client_id, seek_pk);
                    }
                    Err(msg) => return DispatchResult::Error(msg),
                }
                DispatchResult::Continue
            }

            SalMessageKind::Seek => {
                // User tables may have a wide PK (stride > 16) whose full key
                // arrives as seek_pk (low 16 bytes) + seek_pk_extra (16..stride).
                // System tables (target_id < FIRST_USER_TABLE_ID) always have a
                // narrow PK, so they stay on the u128 seek_family path.
                let wide_schema = if target_id >= FIRST_USER_TABLE_ID {
                    self.cat().get_schema_desc(target_id).filter(|s| s.pk_is_wide())
                } else {
                    None
                };
                let result = if let Some(s) = wide_schema {
                    let stride = s.pk_stride() as usize;
                    let buf = match crate::schema::assemble_wide_pk(&s, seek_pk, &seek_pk_extra, stride) {
                        Ok(buf) => buf,
                        Err(e) => return DispatchResult::Error(format!("seek: {e}")),
                    };
                    self.cat().seek_family_bytes(target_id, &buf[..stride])
                } else {
                    self.cat().seek_family(target_id, seek_pk)
                };
                match result {
                    Ok(result) => {
                        let schema = self.cat().get_schema_desc(target_id);
                        self.send_response(target_id as u64, result.as_ref(), schema.as_ref(), request_id, client_id, seek_pk);
                    }
                    Err(msg) => return DispatchResult::Error(msg),
                }
                DispatchResult::Continue
            }

            SalMessageKind::Scan => {
                match self.cat().scan_family(target_id) {
                    Ok(result) => {
                        let schema = self.cat().get_schema_desc(target_id);
                        if let Some(err) = self.send_scan_response(
                            target_id as u64, result, schema.as_ref(), request_id, client_id, client_version,
                        ) {
                            return DispatchResult::Error(err);
                        }
                    }
                    Err(msg) => return DispatchResult::Error(msg),
                }
                DispatchResult::Continue
            }

            // ExchangeRelay only ever arrives while the worker is blocked
            // inside `do_exchange_wait`; that loop consumes it directly
            // and never returns through `dispatch_inner`. PreloadedExchange
            // is drained by `dispatch_top_level` before reaching here.
            // Either reaching dispatch_inner is a master-side bug.
            SalMessageKind::ExchangeRelay | SalMessageKind::PreloadedExchange => {
                gnitz_warn!(
                    "W{} unexpected {:?} at top-level dispatch_inner tid={}",
                    self.worker_id, kind, target_id,
                );
                DispatchResult::Continue
            }
        }
    }

    // ── W2M response helpers ───────────────────────────────────────────

    fn send_ack(&self, target_id: u64, flags: u64, request_id: u64) {
        let sz = ipc::wire_size(STATUS_OK, &[], None, None, None, None, &[]);
        self.w2m_writer.send_encoded(sz, request_id as u32, |buf| {
            ipc::encode_wire_into_ipc(
                buf, 0, target_id, 0, flags,
                0u128, 0, request_id, STATUS_OK, &[], None, None, None, None, &[],
            );
        });
    }

    fn send_response(
        &mut self,
        target_id: u64,
        result: Option<&Batch>,
        schema: Option<&SchemaDescriptor>,
        request_id: u64,
        client_id: u64,
        seek_pk: u128,
    ) {
        let tid_key = target_id as i64;
        // HasPk UniqueIndex passes an index schema; reusing the catalog's
        // table-schema wire block for it would corrupt the master decoder.
        let prebuilt_rc: Option<Rc<Vec<u8>>> = schema.map(|s| {
            let table_schema_matches = self.cat()
                .get_schema_desc(tid_key)
                .map(|t| t.num_columns() == s.num_columns() && t.pk_indices() == s.pk_indices())
                .unwrap_or(false);
            if !table_schema_matches {
                return Rc::new(ipc::build_schema_wire_block(s, &[], target_id as u32));
            }
            if let Some(cached) = self.cat().get_cached_schema_wire_block(tid_key) {
                return cached.block;
            }
            let col_names = self.cat().get_col_names_bytes(tid_key);
            let (name_refs, n) = ipc::col_names_as_refs(&col_names);
            let block = Rc::new(ipc::build_schema_wire_block(s, &name_refs[..n], target_id as u32));
            let (wire_safe, wire_row_stride) = crate::runtime::sal::compute_wire_props(s);
            self.cat().set_schema_wire_block(tid_key, block.clone(), wire_safe, wire_row_stride);
            block
        });
        let prebuilt: Option<&[u8]> = prebuilt_rc.as_deref().map(|v| v.as_slice());
        let server_version = if schema.is_some() { self.cat().get_schema_version(tid_key) } else { 0 };
        let flags = gnitz_wire::wire_flags_set_schema_version(0, server_version);
        let sz = ipc::wire_size(STATUS_OK, &[], schema, None, result, prebuilt, &[]);
        self.w2m_writer.send_encoded(sz, request_id as u32, |buf| {
            ipc::encode_wire_into(
                buf, 0, target_id, client_id, flags,
                seek_pk, 0, request_id, STATUS_OK, &[],
                schema, None, result, prebuilt, &[],
            );
        });
    }

    /// Send a SCAN response for `batch`. For wire-safe schemas, large batches
    /// are split across multiple frames via `pending_scan`; the first chunk is
    /// emitted at the top of the next `drain_sal` pass. For non-wire-safe
    /// (STRING-column) schemas, a single frame is sent; returns an error message
    /// if the batch exceeds `MAX_W2M_MSG`.
    fn send_scan_response(
        &mut self,
        target_id: u64,
        batch: Rc<Batch>,
        schema: Option<&SchemaDescriptor>,
        request_id: u64,
        client_id: u64,
        client_version: u16,
    ) -> Option<String> {
        let tid_key = target_id as i64;
        // Obtain prebuilt schema block + server version. include_schema controls
        // whether the first frame carries a schema block; server_version is always
        // embedded in wire_flags so the client can cache/verify.
        let (prebuilt_rc, server_version) = if let Some(s) = schema {
            let (block, server_version) = match self.cat().get_cached_schema_wire_block(tid_key) {
                Some(cached) => (cached.block, cached.version),
                None => {
                    let col_names = self.cat().get_col_names_bytes(tid_key);
                    let (name_refs, n) = ipc::col_names_as_refs(&col_names);
                    let block = Rc::new(ipc::build_schema_wire_block(s, &name_refs[..n], target_id as u32));
                    let ver = self.cat().get_schema_version(tid_key);
                    let (wire_safe, wire_row_stride) = crate::runtime::sal::compute_wire_props(s);
                    self.cat().set_schema_wire_block(tid_key, block.clone(), wire_safe, wire_row_stride);
                    (block, ver)
                }
            };
            let prebuilt = if gnitz_wire::wire_should_include_schema(client_version, server_version) { Some(block) } else { None };
            (prebuilt, server_version)
        } else {
            (None, 0u16)
        };
        let schema_version_flags = gnitz_wire::wire_flags_set_schema_version(0, server_version);

        // When schema omission is in effect (prebuilt_rc=None), pass schema=None to
        // the encode functions so has_schema stays false. Passing schema=Some with
        // prebuilt=None would cause encode_wire_into_range to emit a schema block
        // with empty column names, corrupting the client's schema cache.
        let schema_for_encode = if prebuilt_rc.is_some() { schema } else { None };

        let is_wire_safe = schema.map(schema_wire_safe).unwrap_or(true);

        if !is_wire_safe {
            // STRING-column tables: no chunking. Check size; error if too big.
            let prebuilt = prebuilt_rc.as_deref().map(Vec::as_slice);
            let wire_sz = ipc::wire_size(STATUS_OK, &[], schema_for_encode, None, Some(&*batch), prebuilt, &[]);
            if wire_sz > w2m_ring::MAX_W2M_MSG as usize {
                return Some(format!(
                    "scan: batch wire_size={wire_sz} > MAX_W2M_MSG={}; \
                     STRING-column chunking not yet implemented",
                    w2m_ring::MAX_W2M_MSG
                ));
            }
            let flags = schema_version_flags | FLAG_CONTINUATION | FLAG_SCAN_LAST;
            self.w2m_writer.send_encoded(wire_sz, request_id as u32, |buf| {
                ipc::encode_wire_into(
                    buf, 0, target_id, client_id, flags,
                    0u128, 0, 0, STATUS_OK, &[],
                    schema_for_encode, None, Some(&*batch), prebuilt, &[],
                );
            });
            return None;
        }

        // Wire-safe path: range encoder supports chunking.
        let total_rows = batch.count;
        let total_sz = {
            let prebuilt = prebuilt_rc.as_deref().map(Vec::as_slice);
            ipc::wire_size_range(STATUS_OK, &[], schema_for_encode, None, &batch, total_rows, prebuilt)
        };

        if total_sz <= w2m_ring::MAX_W2M_MSG as usize {
            // Single-frame response: FLAG_CONTINUATION keeps the client reading
            // (terminal frame signals scan end); FLAG_SCAN_LAST tells master this
            // worker's chunk train is done.
            let prebuilt = prebuilt_rc.as_deref().map(Vec::as_slice);
            let flags = schema_version_flags | FLAG_CONTINUATION | FLAG_SCAN_LAST;
            self.w2m_writer.send_encoded(total_sz, request_id as u32, |buf| {
                ipc::encode_wire_into_range(
                    buf, 0, target_id, client_id, flags,
                    0, STATUS_OK,
                    schema_for_encode, &batch, 0, total_rows, prebuilt,
                );
            });
        } else {
            // Multi-chunk: store pending state; first chunk emitted next drain_sal.
            self.pending_scan = Some(PendingScan {
                batch,
                next_row: 0,
                request_id,
                client_id,
                target_id,
                prebuilt_schema: prebuilt_rc,
                server_version,
            });
        }

        None
    }

    fn send_error(&self, error_msg: &str, request_id: u64) {
        let msg = error_msg.as_bytes();
        let sz = ipc::wire_size(STATUS_ERROR, msg, None, None, None, None, &[]);
        self.w2m_writer.send_encoded(sz, request_id as u32, |buf| {
            ipc::encode_wire_into_ipc(
                buf, 0, 0, 0, 0,
                0u128, 0, request_id, STATUS_ERROR, msg, None, None, None, None, &[],
            );
        });
    }

    // ── Request handlers ───────────────────────────────────────────────

    fn handle_push(
        &mut self, target_id: i64, batch: Batch, _request_id: u64,
    ) -> Result<(), String> {
        // Master pre-partitions FLAG_PUSH rows in `scatter_wire_group`,
        // so every slot already contains only this worker's rows. A second
        // partition-hash filter here would be pure overhead.
        if batch.count == 0 {
            return Ok(());
        }
        let row_count = batch.count;
        if target_id < FIRST_USER_TABLE_ID {
            // Master never sends FLAG_PUSH for system tables; system-table
            // changes arrive via FLAG_DDL_SYNC → ddl_sync. Reaching here
            // means a protocol invariant was violated.
            return Err(format!(
                "W{}: FLAG_PUSH for system table_id={}; expected DDL_SYNC",
                self.worker_id, target_id
            ));
        } else {
            let effective = self.cat().ingest_returning_effective(target_id, batch)?;
            if let Some(existing) = self.pending_deltas.get_mut(&target_id) {
                existing.append_batch(&effective, 0, effective.count);
            } else {
                self.pending_deltas.insert(target_id, effective);
            }
        }
        gnitz_debug!("W{} push tid={} rows={}", self.worker_id, target_id, row_count);
        Ok(())
    }

    fn handle_tick(&mut self, target_id: i64, request_id: u64) -> Result<(), String> {
        let delta = if let Some(d) = self.pending_deltas.remove(&target_id) {
            d
        } else {
            if !self.cat().has_id(target_id) {
                return Ok(());
            }
            let schema = self.cat().get_schema_desc(target_id)
                .ok_or_else(|| format!("no schema for tid={}", target_id))?;
            Batch::with_schema(schema, 0)
        };
        self.evaluate_dag(target_id, delta, request_id);
        Ok(())
    }

    fn handle_backfill(&mut self, source_tid: i64, request_id: u64) -> Result<(), String> {
        if !self.cat().has_id(source_tid) {
            return Ok(());
        }
        let local_batch = self.cat().scan_family(source_tid)?;
        let owned = Rc::try_unwrap(local_batch).unwrap_or_else(|a| (*a).clone());
        self.evaluate_dag(source_tid, owned, request_id);
        Ok(())
    }

    fn handle_has_pk(
        &mut self,
        target_id: i64,
        batch: Option<Batch>,
        lookup: HasPkLookup,
        request_id: u64,
        client_id: u64,
        seek_pk: u128,
    ) -> Result<(), String> {
        let n = batch.as_ref().map(|b| b.count).unwrap_or(0);

        let (schema, result) = match lookup {
            HasPkLookup::UniqueIndex(col_idx) => {
                let index_handle = self.cat().get_index_store_handle(target_id, col_idx);
                if index_handle.is_null() {
                    return Err(format!(
                        "No unique index on column {} for table {}", col_idx, target_id
                    ));
                }
                // The check target is the unique INDEX table, whose schema is
                // `(indexed_col, src_pk…)` — NOT the owner table's schema.
                // `idx_key_size` and the PK-byte interpretation below are taken
                // from this schema, so an owner-table fallback would compute the
                // wrong prefix width. The broadcast always carries the index
                // schema today, so the fallback is currently unreachable, but
                // `get_index_schema_by_col` keeps it type-correct.
                let schema = batch.as_ref()
                    .and_then(|b| b.schema)
                    .or_else(|| self.cat().get_index_schema_by_col(target_id, col_idx))
                    .ok_or_else(|| format!("no index schema for tid={} col={}", target_id, col_idx))?;
                // Index layout: PK = (indexed-key field, src_pk_cols). The
                // check-batch carries the promoted indexed value in the low
                // bytes of its PK. Any positive-weight match means the value
                // is already in the index. `open_cursor` avoids letting a
                // compaction Io/InvalidShard failure silently turn a present
                // key into "absent".
                let table = unsafe { &*index_handle };
                let mut cursor = table.open_cursor();
                // The check batch's PK is the OPK index composite
                // `(indexed-value, src_pk_cols)`; the leading `idx_key_size`
                // bytes are the OPK-encoded indexed value. Prefix-match that
                // whole leading column — OPK puts the distinguishing bytes last,
                // so a source-width prefix would match only the zero high bytes.
                let idx_key_size = schema.columns[0].size() as usize;
                let result = filter_by_pk_bytes(&batch, schema, n, |pkb| {
                    cursor.cursor.seek_first_positive_with_prefix(&pkb[..idx_key_size])
                });
                (schema, result)
            }
            HasPkLookup::PrimaryKey => {
                let schema = self.cat().get_schema_desc(target_id)
                    .ok_or_else(|| format!("no schema for tid={}", target_id))?;
                let ptable_handle = self.cat().get_ptable_handle(target_id);
                // Route on verbatim OPK bytes for every PK width. The old narrow
                // arm fed `get_pk` (OPK-widened) to `has_pk(u128)`, which
                // re-OPK-encodes it — a double sign-flip that misses signed PKs.
                let result = filter_by_pk_bytes(&batch, schema, n, |pkb| {
                    ptable_handle.is_some_and(|pt_ptr| unsafe { &mut *pt_ptr }.has_pk_bytes(pkb))
                });
                (schema, result)
            }
        };
        self.send_response(target_id as u64, Some(&result), Some(&schema), request_id, client_id, seek_pk);
        Ok(())
    }

    fn handle_flush_all(&mut self) -> Result<(), String> {
        self.pending_deltas.clear();
        let ids = self.cat().iter_user_table_ids();
        let mut dir_inodes: Vec<(u64, u64, libc::c_int)> = Vec::new();

        let mut ring = io_uring::IoUring::new(256)
            .map_err(|e| format!("io_uring::new failed: {}", e))?;

        const FD_CHUNK_THRESHOLD: usize = 256;
        let mut pending: Vec<(i64, Vec<(usize, FlushWork)>)> = Vec::new();
        let mut pending_fds = 0usize;

        for tid in ids {
            let works = self.cat().flush_family_prepare(tid)
                .map_err(|e| format!("flush_prepare tid={}: {}", tid, e))?;
            if works.is_empty() { continue; }
            let added_fds: usize = works.iter()
                .map(|(_, w)| w.shard_fd().is_some() as usize
                    + w.manifest_fd().is_some() as usize)
                .sum();
            pending.push((tid, works));
            pending_fds += added_fds;
            if pending_fds >= FD_CHUNK_THRESHOLD {
                self.flush_chunk(&mut ring, &mut pending, &mut dir_inodes)?;
                pending_fds = 0;
            }
        }
        self.flush_chunk(&mut ring, &mut pending, &mut dir_inodes)?;

        // The fds are per-flush dir fds, owned by this batch (opened in
        // `flush_prepare`, returned by `flush_commit`). Fsync one fd per unique
        // directory inode, then close every fd so the flush leaks none.
        let all_fds: Vec<libc::c_int> =
            dir_inodes.iter().map(|&(_, _, fd)| fd).collect();
        let mut fsync_err = None;
        for fd in dedup_dirfds(dir_inodes) {
            if let Err(e) = crate::util::fsync_eintr(fd) {
                fsync_err = Some(e);
                break;
            }
        }
        for fd in all_fds {
            unsafe { libc::close(fd); }
        }
        if let Some(err) = fsync_err {
            return Err(format!("dir fsync failed: {}", err));
        }
        Ok(())
    }

    fn flush_chunk(
        &mut self,
        ring: &mut io_uring::IoUring,
        pending: &mut Vec<(i64, Vec<(usize, FlushWork)>)>,
        dir_inodes: &mut Vec<(u64, u64, libc::c_int)>,
    ) -> Result<(), String> {
        if pending.is_empty() { return Ok(()); }

        let fds: Vec<libc::c_int> = pending.iter()
            .flat_map(|(_, ws)| ws.iter())
            .flat_map(|(_, w)| w.shard_fd().into_iter().chain(w.manifest_fd()))
            .collect();
        uring_batch_fdatasync(ring, &fds)?;

        for (_, ws) in pending.iter_mut() {
            for (_, w) in ws { w.close_fds(); }
        }

        for (tid, works) in pending.drain(..) {
            let dirfds = self.cat().flush_family_commit_batch(tid, works)
                .map_err(|e| format!("flush_commit tid={}: {}", tid, e))?;
            for dirfd in dirfds {
                let mut stat: libc::stat = unsafe { std::mem::zeroed() };
                if unsafe { libc::fstat(dirfd, &mut stat) } < 0 {
                    let err = std::io::Error::last_os_error();
                    return Err(format!("fstat: {}", err));
                }
                dir_inodes.push((stat.st_dev as u64, stat.st_ino as u64, dirfd));
            }
        }
        Ok(())
    }

    /// Run multi-worker DAG evaluation with the exchange context.
    /// `request_id` is the master's request id of the message that
    /// triggered this evaluation (FLAG_TICK / FLAG_PUSH / FLAG_BACKFILL);
    /// echoed by `do_exchange_wait` so the master accumulator's wakers
    /// stay routable.
    fn evaluate_dag(&mut self, source_id: i64, delta: Batch, request_id: u64) {
        let dag = self.cat().get_dag_ptr();
        let mut ctx = WorkerExchangeCtx {
            worker: self,
            tick_request_id: request_id,
        };
        unsafe { &mut *dag }.evaluate_dag_multi_worker(source_id, delta, &mut ctx);
        // Apply DDL_SYNC messages deferred during exchange waits.
        self.dispatch_deferred();
    }

    fn dispatch_deferred(&mut self) {
        for ddl in std::mem::take(&mut self.exchange.deferred) {
            let _ = self.cat().ddl_sync(ddl.target_id, ddl.batch);
        }
        // See the DdlSync dispatch arm: the master owns physical directory
        // removal for the shared tree; the worker only discards its queue.
        self.cat().discard_pending_dir_deletions();
    }

    /// Send FLAG_EXCHANGE to the master and block until its FLAG_EXCHANGE_RELAY
    /// for `view_id` comes back on the SAL. Messages that arrive mid-wait are
    /// dispatched inline — handle_push, handle_tick — so ACKs flow back through
    /// the master reactor in their natural arrival order, routed by req_id.
    /// Relays whose view_id does not match the innermost wait are parked in
    /// `pending_relays`; the next nested wait to ask for them will pick them
    /// up without re-reading the SAL.
    fn do_exchange_wait(
        &mut self,
        view_id: i64,
        batch: &Batch,
        source_id: i64,
        tick_request_id: u64,
    ) -> Batch {
        if let Some(stashed) = self.exchange.stash.remove(&view_id) {
            return stashed;
        }

        let schema = batch.schema;
        let sz = ipc::wire_size(STATUS_OK, &[], schema.as_ref(), None, Some(batch), None, &[]);
        self.w2m_writer.send_encoded(sz, tick_request_id as u32, |buf| {
            ipc::encode_wire_into_ipc(
                buf, 0, view_id as u64, 0, FLAG_EXCHANGE as u64,
                source_id as u128, 0, tick_request_id, STATUS_OK, &[],
                schema.as_ref(), None, Some(batch), None, &[],
            );
        });

        let master_pid = self.master_pid;
        let want_key = (view_id, source_id);
        let ctx = DispatchContext::InsideExchangeWait { want_key, schema };

        loop {
            if let Some(b) = self.exchange.pending_relays.remove(&want_key) {
                return b;
            }

            self.sal_reader.wait(30000);

            // If the master died (killed or gnitz_fatal_abort) while we
            // were waiting, exit like the main run loop does.
            if master_pid != 0 && unsafe { libc::getppid() } != master_pid {
                unsafe { libc::_exit(0); }
            }

            loop {
                if let Some(b) = self.exchange.pending_relays.remove(&want_key) {
                    return b;
                }

                let (kind, target_id, wire) = match self.next_sal_message() {
                    Some(v) => v,
                    None => break, // no more entries — back to outer wait
                };

                match self.dispatch(ctx, kind, target_id, wire) {
                    DispatchOutcome::Continue => {}
                    DispatchOutcome::RelayMatched(batch) => return batch,
                    DispatchOutcome::Shutdown => {
                        // dispatch_inner's Shutdown arm already called
                        // libc::_exit — unreachable in practice.
                        return Batch::with_schema(
                            schema.unwrap_or_default(), 0,
                        );
                    }
                }
            }
        }
    }

    /// Replay FLAG_TICK messages deferred during an exchange wait. Runs
    /// after the current tick's ACK has been sent so the master observes
    /// ACKs in SAL arrival order. Each replayed tick may itself trigger
    /// more exchanges and defer more ticks, so loop until the queue is
    /// empty.
    fn replay_deferred_ticks(&mut self) {
        while !self.exchange.deferred_ticks.is_empty() {
            let ticks: Vec<(i64, u64)> =
                std::mem::take(&mut self.exchange.deferred_ticks);
            for (target_id, req_id) in ticks {
                match self.handle_tick(target_id, req_id) {
                    Ok(()) => self.send_ack(target_id as u64, 0, req_id),
                    Err(e) => self.send_error(&e, req_id),
                }
            }
        }
    }


    fn shutdown(&mut self) {
        let _ = self.handle_flush_all();
        unsafe { libc::_exit(0); }
    }
}

enum DispatchResult {
    Continue,
    Shutdown,
    Error(String),
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::SchemaDescriptor;

    #[test]
    fn test_dedup_dirfds_removes_duplicates() {
        // dedup_dirfds only deduplicates by (dev, ino) — it does not close the
        // duplicates it drops (the caller closes every collected fd).
        let inodes = vec![
            (1u64, 10u64, 3i32),
            (1u64, 10u64, 7i32),  // duplicate of (1,10)
            (1u64, 20u64, 5i32),
            (2u64, 10u64, 9i32),  // same ino, different dev — not a duplicate
        ];
        let mut result = dedup_dirfds(inodes);
        result.sort_unstable();
        assert_eq!(result.len(), 3, "one fd per unique (dev, ino)");
        // No duplicate fds for (1, 10)
        assert!(result.contains(&3i32) || result.contains(&7i32));
        assert!(result.contains(&5i32));
        assert!(result.contains(&9i32));
    }

    #[test]
    fn test_dedup_dirfds_empty() {
        assert_eq!(dedup_dirfds(vec![]), Vec::<libc::c_int>::new());
    }

    #[test]
    fn test_dedup_dirfds_all_unique() {
        let inodes = vec![(1u64, 1u64, 10i32), (1u64, 2u64, 20i32), (2u64, 1u64, 30i32)];
        let mut result = dedup_dirfds(inodes);
        result.sort_unstable();
        assert_eq!(result, vec![10i32, 20i32, 30i32]);
    }

    /// Build an io_uring ring, or return `None` if the platform denies the
    /// syscall (no io_uring support, no CAP_SYS_ADMIN, or AppArmor/seccomp
    /// restriction) so the caller can skip rather than panic.
    fn try_new_ring(entries: u32) -> Option<io_uring::IoUring> {
        match io_uring::IoUring::new(entries) {
            Ok(r) => Some(r),
            Err(e) if e.raw_os_error().is_some_and(|c| {
                c == libc::ENOSYS || c == libc::EPERM || c == libc::EACCES
            }) => None,
            Err(e) => panic!("io_uring::new: {}", e),
        }
    }

    /// uring_batch_fdatasync must complete one CQE per fd it submitted, and
    /// drain the SQ when the fd count exceeds the ring's SQ entries (forcing
    /// multiple submit_and_wait rounds).
    #[test]
    fn test_uring_batch_fdatasync_chunked() {
        // Tiny ring forces multiple submit_and_wait rounds for >4 fds.
        let Some(mut ring) = try_new_ring(4) else { return };
        let dir = tempfile::tempdir().unwrap();

        let mut fds: Vec<libc::c_int> = Vec::new();
        for i in 0..10 {
            let path = dir.path().join(format!("f_{}.bin", i));
            let path_c = std::ffi::CString::new(path.to_str().unwrap()).unwrap();
            let fd = unsafe {
                libc::open(
                    path_c.as_ptr(),
                    libc::O_WRONLY | libc::O_CREAT | libc::O_TRUNC,
                    0o644 as libc::mode_t,
                )
            };
            assert!(fd >= 0, "open failed: {}", std::io::Error::last_os_error());
            // Some content so fdatasync has data to flush.
            let buf = b"hello";
            let rc = unsafe { libc::write(fd, buf.as_ptr() as *const libc::c_void, buf.len()) };
            assert_eq!(rc as usize, buf.len());
            fds.push(fd);
        }

        uring_batch_fdatasync(&mut ring, &fds).expect("batch fdatasync");

        for fd in fds {
            unsafe { libc::close(fd); }
        }
    }

    #[test]
    fn test_uring_batch_fdatasync_empty() {
        let Some(mut ring) = try_new_ring(8) else { return };
        uring_batch_fdatasync(&mut ring, &[]).expect("empty batch should succeed");
    }

    /// EINTR on the first submit_and_wait must not stall: any CQEs that arrived
    /// before the interrupt are drained, the loop retries, and all fds complete.
    #[test]
    fn test_uring_batch_fdatasync_eintr_retries() {
        let Some(mut ring) = try_new_ring(8) else { return };
        let dir = tempfile::tempdir().unwrap();

        let mut fds: Vec<libc::c_int> = Vec::new();
        for i in 0..3 {
            let path = dir.path().join(format!("eintr_{}.bin", i));
            let path_c = std::ffi::CString::new(path.to_str().unwrap()).unwrap();
            let fd = unsafe {
                libc::open(path_c.as_ptr(), libc::O_WRONLY | libc::O_CREAT | libc::O_TRUNC, 0o644)
            };
            assert!(fd >= 0);
            let buf = b"x";
            unsafe { libc::write(fd, buf.as_ptr() as *const libc::c_void, 1) };
            fds.push(fd);
        }

        let mut call_count = 0usize;
        let result = uring_batch_fdatasync_with(&mut ring, &fds, |r, want| {
            call_count += 1;
            if call_count == 1 {
                // Simulate EINTR without submitting: the SQEs stay queued in the
                // ring buffer. The loop must drain 0 CQEs, continue, and retry.
                Err(std::io::Error::from_raw_os_error(libc::EINTR))
            } else {
                r.submit_and_wait(want)
            }
        });

        assert!(result.is_ok(), "EINTR should be retried, got: {:?}", result);
        assert_eq!(call_count, 2, "exactly one EINTR then one successful submit expected");

        for fd in fds {
            unsafe { libc::close(fd); }
        }
    }

    fn test_schema() -> SchemaDescriptor {
        use crate::schema::{SchemaColumn, type_code};
        SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::U64, 0),
            ],
            &[0],
        )
    }

    fn make_handler() -> WorkerExchangeHandler {
        WorkerExchangeHandler {
            stash: HashMap::new(),
            deferred: Vec::<DeferredDdl>::new(),
            deferred_ticks: Vec::new(),
            pending_relays: HashMap::new(),
        }
    }

    #[test]
    fn test_exchange_handler_stash_and_retrieve() {
        let mut handler = make_handler();
        let schema = test_schema();
        let batch = Batch::with_schema(schema, 0);
        handler.stash_preloaded(42, batch);
        assert!(handler.stash.contains_key(&42));
        let retrieved = handler.stash.remove(&42);
        assert!(retrieved.is_some());
        assert!(!handler.stash.contains_key(&42));
    }

    /// Locks the single-source-per-view invariant on `stash_preloaded`.
    /// A second insert for the same `view_id` while the slot is still
    /// occupied is forbidden — see the doc comment on `stash_preloaded`.
    /// (If the master ever needs to send preloaded data from multiple
    /// sources for the same view, the stash must be re-keyed to
    /// `(view_id, source_id)` to match `pending_relays`.)
    #[test]
    #[should_panic(expected = "already has a preloaded batch")]
    fn test_exchange_handler_stash_rejects_double_insert() {
        let mut handler = make_handler();
        let schema = test_schema();
        let batch1 = Batch::with_schema(schema, 0);
        let batch2 = Batch::with_schema(schema, 0);
        handler.stash_preloaded(42, batch1);
        handler.stash_preloaded(42, batch2);
    }

    #[test]
    fn test_pending_deltas_accumulation() {
        let schema = test_schema();
        let mut pending: HashMap<i64, Batch> = HashMap::new();

        let mut b1 = Batch::with_schema(schema, 1);
        b1.extend_pk(1u128);
        b1.extend_weight(&1i64.to_le_bytes());
        b1.extend_null_bmp(&0u64.to_le_bytes());
        b1.extend_col(0, &10u64.to_le_bytes());
        b1.count = 1;

        let mut b2 = Batch::with_schema(schema, 1);
        b2.extend_pk(2u128);
        b2.extend_weight(&1i64.to_le_bytes());
        b2.extend_null_bmp(&0u64.to_le_bytes());
        b2.extend_col(0, &20u64.to_le_bytes());
        b2.count = 1;

        // First insert
        pending.insert(100, b1);
        // Accumulate
        if let Some(existing) = pending.get_mut(&100) {
            existing.append_batch(&b2, 0, b2.count);
        }
        assert_eq!(pending[&100].count, 2);
    }

    // -- pending_relays tests ------------------------------------------------

    /// Relays arriving for a different (view_id, source_id) while an outer
    /// exchange waits are parked in `pending_relays`; the next nested wait
    /// to ask for that exact pair pulls them from the queue instead of
    /// re-reading the SAL.
    #[test]
    fn test_pending_relays_queue_and_drain() {
        let mut h = make_handler();
        let schema = test_schema();
        let mut batch_b = Batch::with_schema(schema, 0);
        batch_b.count = 7;
        let mut batch_c = Batch::with_schema(schema, 0);
        batch_c.count = 9;
        h.pending_relays.insert((200, 0), batch_b);
        h.pending_relays.insert((300, 0), batch_c);

        // Nested wait for view 200 finds its relay without touching the SAL.
        let b = h.pending_relays.remove(&(200, 0)).expect("view 200 relay queued");
        assert_eq!(b.count, 7);
        assert!(!h.pending_relays.contains_key(&(200, 0)));
        // Unrelated view 300 remains parked for its own wait.
        assert!(h.pending_relays.contains_key(&(300, 0)));
    }

    /// Two exchange rounds for the *same* view_id but different
    /// source_ids (typical for a join view with multiple input tables)
    /// must stash independently. Keying by view_id alone was the
    /// root-cause bug — a relay for source A satisfied a wait for
    /// source B.
    #[test]
    fn test_pending_relays_keyed_by_view_and_source() {
        let mut h = make_handler();
        let schema = test_schema();
        let mut batch_a = Batch::with_schema(schema, 0);
        batch_a.count = 3;
        let mut batch_b = Batch::with_schema(schema, 0);
        batch_b.count = 11;

        // Same view_id=100, different source_ids 10 and 20.
        h.pending_relays.insert((100, 10), batch_a);
        h.pending_relays.insert((100, 20), batch_b);

        // Retrieving one does NOT retrieve the other.
        let a = h.pending_relays.remove(&(100, 10)).expect("(100,10) queued");
        assert_eq!(a.count, 3);
        assert!(h.pending_relays.contains_key(&(100, 20)),
            "retrieving (100,10) must leave (100,20) in place");
        let b = h.pending_relays.remove(&(100, 20)).expect("(100,20) queued");
        assert_eq!(b.count, 11);
    }

    /// Stage 0 wire-protocol contract: every reply helper (`send_ack`,
    /// `send_response`, `send_error`) must echo the inbound request_id back
    /// on the W2M region so the master reactor can route it. We fake out the
    /// W2M writer with a real anonymous mmap, fire each helper with a
    /// distinct id, then read the messages back through `decode_wire` and
    /// assert the ids round-trip.
    #[test]
    fn test_send_helpers_echo_request_id() {
        use crate::runtime::wire as ipc;
        use crate::runtime::w2m_ring;
        // Use the production-sized region — the mmap reservation is
        // lazy-populated, so the 1 GiB backing is cheap.
        let region_size = w2m_ring::W2M_REGION_SIZE;
        let region_ptr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                region_size,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_ANONYMOUS | libc::MAP_SHARED,
                -1, 0,
            )
        };
        assert_ne!(region_ptr, libc::MAP_FAILED);
        let region_ptr = region_ptr as *mut u8;
        unsafe { w2m_ring::init_region(region_ptr, region_size as u64); }

        let w2m_writer = W2mWriter::new(region_ptr, region_size as u64);

        let mut wp = WorkerProcess {
            worker_id: 0,
            num_workers: 1,
            master_pid: 0,
            catalog: std::ptr::null_mut(),
            sal_reader: unsafe { std::mem::zeroed() },
            w2m_writer,
            exchange: make_handler(),
            pending_deltas: HashMap::new(),
            pending_scan: None,
            read_cursor: 0,
            expected_epoch: 1,
        };

        let req_ack: u64 = 42;
        let req_resp: u64 = 0xCAFE_BABE_DEAD_BEEF;
        let req_err: u64 = u64::MAX;
        wp.send_ack(7, 0, req_ack);
        // Pass schema=None: send_response consults the catalog only when a
        // schema is present, and this test uses a null catalog pointer.
        // The id round-trip is the assertion of interest.
        wp.send_response(8, None, None, req_resp, 0, 0u128);
        wp.send_error("boom", req_err);

        // Decode the three messages back from the ring via try_consume.
        let hdr = unsafe { w2m_ring::W2mRingHeader::from_raw(region_ptr as *const u8) };
        let mut rc = w2m_ring::W2M_HEADER_SIZE as u64;
        let mut decoded_ids = Vec::new();
        for _ in 0..3 {
            let (data_ptr, sz, new_rc, _req_id) = unsafe {
                w2m_ring::try_consume(hdr, region_ptr as *const u8, rc)
                    .expect("expected a message")
            };
            let data = unsafe { std::slice::from_raw_parts(data_ptr, sz as usize) };
            let decoded = ipc::decode_wire(data).expect("decode_wire");
            decoded_ids.push(decoded.control.request_id);
            rc = new_rc;
        }
        assert_eq!(decoded_ids, vec![req_ack, req_resp, req_err]);

        unsafe {
            libc::munmap(region_ptr as *mut libc::c_void, region_size);
        }
    }

    #[test]
    fn from_wire_zero_is_primary_key() {
        assert!(matches!(HasPkLookup::from_wire(0), HasPkLookup::PrimaryKey));
    }

    #[test]
    fn from_wire_one_is_unique_index_col0() {
        assert!(matches!(HasPkLookup::from_wire(1), HasPkLookup::UniqueIndex(0)));
    }

    #[test]
    fn from_wire_two_is_unique_index_col1() {
        assert!(matches!(HasPkLookup::from_wire(2), HasPkLookup::UniqueIndex(1)));
    }

    #[test]
    fn from_wire_large_value_is_unique_index_not_primary_key() {
        // Before the fix, (u64::MAX as i32) = -1 < 0 would return PrimaryKey.
        assert!(matches!(HasPkLookup::from_wire(u64::MAX), HasPkLookup::UniqueIndex(_)));
        assert!(matches!(HasPkLookup::from_wire(i32::MAX as u64 + 1), HasPkLookup::UniqueIndex(_)));
    }

    // -- Walk-the-matrix dispatch tests ---------------------------------------

    /// Build a worker that's safe for `dispatch` calls whose behavior
    /// does not enter the catalog (Tick/DdlSync/ExchangeRelay/PreloadedExchange
    /// inside an exchange wait, plus ExchangeRelay/PreloadedExchange at
    /// top-level which warn-or-stash without touching the catalog).
    ///
    /// The W2M ring is unused by these arms; sal_reader is also unused
    /// because we drive `dispatch` directly.
    fn make_worker_for_matrix() -> WorkerProcess {
        WorkerProcess {
            worker_id: 0,
            num_workers: 1,
            master_pid: 0,
            catalog: std::ptr::null_mut(),
            sal_reader: unsafe { std::mem::zeroed() },
            w2m_writer: unsafe { std::mem::zeroed() },
            exchange: make_handler(),
            pending_deltas: HashMap::new(),
            pending_scan: None,
            read_cursor: 0,
            expected_epoch: 1,
        }
    }

    /// Tick inside an exchange wait MUST defer to `deferred_ticks`,
    /// not run inline. Cited bug: an inline tick eval re-enters `view_id`
    /// with a different source and produces schema-mismatched relays.
    #[test]
    fn test_dispatch_matrix_tick_defers_inside_exchange() {
        let mut wp = make_worker_for_matrix();
        let ctx = DispatchContext::InsideExchangeWait {
            want_key: (100, 5),
            schema: Some(test_schema()),
        };
        assert!(wp.exchange.deferred_ticks.is_empty());
        let outcome = wp.dispatch(ctx, SalMessageKind::Tick, 999, None);
        assert!(matches!(outcome, DispatchOutcome::Continue));
        assert_eq!(wp.exchange.deferred_ticks.len(), 1);
        assert_eq!(wp.exchange.deferred_ticks[0].0, 999,
            "Tick target_id must be carried into deferred_ticks");
    }

    /// ExchangeRelay inside an exchange wait whose `(view_id, source_id)`
    /// matches `want_key` returns `RelayMatched(batch)`; a non-matching
    /// pair is parked in `pending_relays`. Driving with `Some(&[])`
    /// gives a decode failure → relay_source_id=0, empty batch.
    #[test]
    fn test_dispatch_matrix_exchange_relay_inside_exchange() {
        let mut wp = make_worker_for_matrix();
        let schema = test_schema();
        let want_key = (100, 0);
        let ctx = DispatchContext::InsideExchangeWait { want_key, schema: Some(schema) };

        // Mismatched view: parked.
        let empty: &'static [u8] = &[];
        let outcome = wp.dispatch(ctx, SalMessageKind::ExchangeRelay, 200, Some(empty));
        assert!(matches!(outcome, DispatchOutcome::Continue));
        assert!(wp.exchange.pending_relays.contains_key(&(200, 0)),
            "non-matching relay must be parked in pending_relays");

        // Matching key: returned via RelayMatched.
        let outcome = wp.dispatch(ctx, SalMessageKind::ExchangeRelay, 100, Some(empty));
        assert!(matches!(outcome, DispatchOutcome::RelayMatched(_)),
            "key-matching relay must short-circuit with RelayMatched");
    }

    /// ExchangeRelay at TopLevel is a protocol bug — it can only arrive
    /// while the worker is blocked in `do_exchange_wait`. The dispatcher
    /// warns and continues; no observable state change.
    #[test]
    fn test_dispatch_matrix_exchange_relay_top_level_warns_and_continues() {
        let mut wp = make_worker_for_matrix();
        let empty: &'static [u8] = &[];
        let outcome = wp.dispatch(
            DispatchContext::TopLevel,
            SalMessageKind::ExchangeRelay,
            100,
            Some(empty),
        );
        assert!(matches!(outcome, DispatchOutcome::Continue));
        assert!(wp.exchange.pending_relays.is_empty(),
            "TopLevel must NOT park relays — they belong to do_exchange_wait");
    }

    /// PreloadedExchange returns Continue in both contexts and
    /// (with a non-decoding empty wire) makes no state change.
    /// The decoded-payload path is exercised by the existing
    /// `test_exchange_handler_stash_*` tests.
    #[test]
    fn test_dispatch_matrix_preloaded_exchange_both_contexts() {
        for ctx in [
            DispatchContext::TopLevel,
            DispatchContext::InsideExchangeWait {
                want_key: (1, 1),
                schema: Some(test_schema()),
            },
        ] {
            let mut wp = make_worker_for_matrix();
            let empty: &'static [u8] = &[];
            let outcome = wp.dispatch(ctx, SalMessageKind::PreloadedExchange, 7, Some(empty));
            assert!(matches!(outcome, DispatchOutcome::Continue));
        }
    }

    /// Walk every `SalMessageKind` and assert a *behavior contract*
    /// for the InsideExchangeWait context. Kinds that defer must NOT
    /// touch the catalog; kinds that go inline through `dispatch_inner`
    /// are not exercised here (they need a real catalog) — the contract
    /// here is that the *defer* decisions are correctly wired.
    ///
    /// Adding a new variant to `SalMessageKind` and forgetting to slot
    /// it into `dispatch`'s `match (ctx, kind)` is already a compile
    /// error (exhaustiveness). This test catches the next-most-likely
    /// regression: routing a should-defer kind to the inline path.
    #[test]
    fn test_dispatch_matrix_walk_kinds_defer_decisions() {
        // Kinds the InsideExchangeWait context MUST defer.
        let must_defer = [SalMessageKind::Tick, SalMessageKind::DdlSync];
        // ExchangeRelay is its own special case (RelayMatched / park).
        // PreloadedExchange stashes (no defer queue).
        // The rest go through dispatch_inner inline.

        for kind in SalMessageKind::ALL {
            if !must_defer.contains(&kind) { continue; }

            let mut wp = make_worker_for_matrix();
            let ctx = DispatchContext::InsideExchangeWait {
                want_key: (0, 0),
                schema: Some(test_schema()),
            };

            let before_ticks = wp.exchange.deferred_ticks.len();
            let before_ddl = wp.exchange.deferred.len();
            let _ = wp.dispatch(ctx, kind, 42, None);
            let after_ticks = wp.exchange.deferred_ticks.len();
            let after_ddl = wp.exchange.deferred.len();

            match kind {
                SalMessageKind::Tick => {
                    assert_eq!(after_ticks, before_ticks + 1,
                        "Tick must defer to deferred_ticks inside exchange wait");
                    assert_eq!(after_ddl, before_ddl,
                        "Tick must NOT touch the deferred (DDL) queue");
                }
                SalMessageKind::DdlSync => {
                    // DdlSync requires a decodable wire to actually defer
                    // (the deferred batch is taken from decoded.data_batch).
                    // With wire=None the dispatch arm is reached and runs
                    // its (trivially empty) effect: this test asserts that
                    // it returns Continue and doesn't panic, and that the
                    // deferred queue did not gain a synthetic entry
                    // (None wire ⇒ no data_batch ⇒ no push).
                    assert_eq!(after_ddl, before_ddl,
                        "DdlSync with no wire data must not synthesize a deferred entry");
                }
                _ => unreachable!(),
            }
        }
    }

    // -- next_sal_message invariant tests -------------------------------------

    /// Drive `next_sal_message` against a hand-built SAL with mixed-epoch
    /// entries. The invariant:
    ///
    /// 1. A successful read advances `read_cursor`.
    /// 2. An epoch mismatch returns `None` and leaves `read_cursor`
    ///    untouched, so a future epoch bump (via FLAG_FLUSH inline
    ///    handling) will pick the same group up.
    /// 3. The expected_epoch is re-read on every call — bumping it
    ///    between calls flips a previously-rejected group into a
    ///    consumable one without rewinding the cursor.
    #[test]
    fn test_next_sal_message_epoch_gating() {
        use crate::runtime::sal::{
            sal_write_group, SalReader, FLAG_PUSH, FLAG_DDL_SYNC,
        };

        const SAL_SIZE: usize = 1 << 20;
        let sal_ptr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                SAL_SIZE,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_ANONYMOUS | libc::MAP_SHARED,
                -1, 0,
            )
        };
        assert_ne!(sal_ptr, libc::MAP_FAILED);
        let sal_ptr = sal_ptr as *mut u8;
        unsafe { std::ptr::write_bytes(sal_ptr, 0, SAL_SIZE); }

        // Single worker; each group puts a one-byte payload at slot 0.
        let nw = 1u32;
        let payload = [0u8; 1];
        let ptrs: Vec<*const u8> = vec![payload.as_ptr()];
        let sizes: Vec<u32> = vec![1];

        // Group 1: epoch=1, FLAG_PUSH, target=42
        let r1 = unsafe {
            sal_write_group(
                sal_ptr, 0, nw, 42, 100, FLAG_PUSH, 1, SAL_SIZE as u64,
                ptrs.as_ptr(), sizes.as_ptr(),
            )
        };
        assert_eq!(r1.status, 0);
        // Group 2: epoch=1, FLAG_DDL_SYNC, target=43
        let r2 = unsafe {
            sal_write_group(
                sal_ptr, r1.new_cursor, nw, 43, 101, FLAG_DDL_SYNC, 1, SAL_SIZE as u64,
                ptrs.as_ptr(), sizes.as_ptr(),
            )
        };
        assert_eq!(r2.status, 0);
        // Group 3: epoch=2 — must be skipped while expected_epoch==1.
        let r3 = unsafe {
            sal_write_group(
                sal_ptr, r2.new_cursor, nw, 44, 102, FLAG_PUSH, 2, SAL_SIZE as u64,
                ptrs.as_ptr(), sizes.as_ptr(),
            )
        };
        assert_eq!(r3.status, 0);

        let sal_reader = SalReader::new(sal_ptr as *const u8, 0, SAL_SIZE, -1);
        let mut wp = WorkerProcess {
            worker_id: 0,
            num_workers: 1,
            master_pid: 0,
            catalog: std::ptr::null_mut(),
            sal_reader,
            w2m_writer: unsafe { std::mem::zeroed() },
            exchange: make_handler(),
            pending_deltas: HashMap::new(),
            pending_scan: None,
            read_cursor: 0,
            expected_epoch: 1,
        };

        // First call: epoch 1 group 1 — consumed.
        let cursor_before = wp.read_cursor;
        let (kind, target_id, _wire) = wp.next_sal_message().expect("group 1");
        assert_eq!(kind, SalMessageKind::Push);
        assert_eq!(target_id, 42);
        assert!(wp.read_cursor > cursor_before, "cursor must advance on success");

        // Second call: epoch 1 group 2 — consumed.
        let cursor_before = wp.read_cursor;
        let (kind, target_id, _wire) = wp.next_sal_message().expect("group 2");
        assert_eq!(kind, SalMessageKind::DdlSync);
        assert_eq!(target_id, 43);
        assert!(wp.read_cursor > cursor_before);

        // Third call: epoch 2 group 3 — REJECTED while expected_epoch==1.
        // Must return None and leave the cursor untouched.
        let cursor_at_epoch_fence = wp.read_cursor;
        assert!(wp.next_sal_message().is_none(),
            "epoch-mismatched group must be rejected");
        assert_eq!(wp.read_cursor, cursor_at_epoch_fence,
            "cursor must NOT advance on epoch mismatch (group remains parked)");

        // Bumping expected_epoch makes the same group consumable on the
        // next call — this is the load-bearing invariant cited by the
        // doc comment in `next_sal_message`. (Cursor was unchanged, so
        // we re-read the same group.)
        wp.expected_epoch = 2;
        let (kind, target_id, _wire) = wp.next_sal_message()
            .expect("group 3 should be consumable after epoch bump");
        assert_eq!(kind, SalMessageKind::Push);
        assert_eq!(target_id, 44);
        assert!(wp.read_cursor > cursor_at_epoch_fence);

        // No more groups.
        assert!(wp.next_sal_message().is_none());

        unsafe { libc::munmap(sal_ptr as *mut libc::c_void, SAL_SIZE); }
    }

    // -- pending_scan chunking tests -----------------------------------------------

    fn make_ring() -> (*mut u8, W2mWriter) {
        let size = w2m_ring::W2M_REGION_SIZE;
        let ptr = unsafe {
            libc::mmap(
                std::ptr::null_mut(), size,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_ANONYMOUS | libc::MAP_SHARED,
                -1, 0,
            ) as *mut u8
        };
        assert_ne!(ptr, libc::MAP_FAILED as *mut u8);
        unsafe { w2m_ring::init_region(ptr, size as u64); }
        (ptr, W2mWriter::new(ptr, size as u64))
    }

    fn make_n_row_batch(schema: SchemaDescriptor, n: usize) -> Batch {
        let mut b = Batch::with_schema(schema, n.max(1));
        for i in 0..n {
            b.extend_pk(i as u128);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &(i as u64).to_le_bytes());
            b.count += 1;
        }
        b
    }

    fn consume_one(ptr: *mut u8) -> Vec<u8> {
        let hdr = unsafe { w2m_ring::W2mRingHeader::from_raw(ptr as *const u8) };
        let rc = w2m_ring::W2M_HEADER_SIZE as u64;
        let (data_ptr, sz, _, _) = unsafe {
            w2m_ring::try_consume(hdr, ptr as *const u8, rc)
                .expect("expected one ring message")
        };
        unsafe { std::slice::from_raw_parts(data_ptr, sz as usize) }.to_vec()
    }

    /// First (and only) PendingScan chunk — next_row == 0, so the prebuilt schema
    /// block must appear in the frame and decode_wire_ipc must succeed without a hint.
    #[test]
    fn test_pending_scan_first_chunk_includes_schema() {
        let schema = test_schema();
        let batch = make_n_row_batch(schema, 10);
        let schema_block = Rc::new(ipc::build_schema_wire_block(&schema, &[], 1));

        let (ptr, writer) = make_ring();
        let mut wp = WorkerProcess {
            worker_id: 0, num_workers: 1, master_pid: 0,
            catalog: std::ptr::null_mut(),
            sal_reader: unsafe { std::mem::zeroed() },
            w2m_writer: writer,
            exchange: make_handler(),
            pending_deltas: HashMap::new(),
            pending_scan: Some(PendingScan {
                batch: Rc::new(batch),
                next_row: 0,
                request_id: 7,
                client_id: 42,
                target_id: 1,
                prebuilt_schema: Some(schema_block),
                server_version: 0,
            }),
            read_cursor: 0,
            expected_epoch: 1,
        };

        wp.emit_pending_scan_chunk();
        assert!(wp.pending_scan.is_none(), "10 rows fit in one chunk; pending_scan must clear");

        let data = consume_one(ptr);
        let decoded = ipc::decode_wire_ipc(&data)
            .expect("first chunk must decode without schema hint");
        assert!(decoded.schema.is_some(), "first chunk must carry schema block");
        let b = decoded.data_batch.expect("first chunk must carry data");
        assert_eq!(b.count, 10);
        for i in 0..10usize { assert_eq!(b.get_pk(i), i as u128); }
        assert_ne!(decoded.control.flags & FLAG_SCAN_LAST, 0,
            "FLAG_SCAN_LAST must be set on the only chunk");
        assert_ne!(decoded.control.flags & FLAG_CONTINUATION, 0,
            "FLAG_CONTINUATION must always be set on worker scan frames");

        unsafe { libc::munmap(ptr as *mut libc::c_void, w2m_ring::W2M_REGION_SIZE); }
    }

    /// Continuation chunk — next_row > 0, prebuilt_schema == None. The frame carries
    /// no schema block; decode_wire_ipc fails but decode_wire_ipc_with_schema succeeds
    /// and returns only the remaining rows (rows [5, 10)).
    #[test]
    fn test_pending_scan_continuation_chunk_excludes_schema() {
        let schema = test_schema();
        let batch = make_n_row_batch(schema, 10);

        let (ptr, writer) = make_ring();
        let mut wp = WorkerProcess {
            worker_id: 0, num_workers: 1, master_pid: 0,
            catalog: std::ptr::null_mut(),
            sal_reader: unsafe { std::mem::zeroed() },
            w2m_writer: writer,
            exchange: make_handler(),
            pending_deltas: HashMap::new(),
            pending_scan: Some(PendingScan {
                batch: Rc::new(batch),
                next_row: 5,
                request_id: 9,
                client_id: 0,
                target_id: 1,
                prebuilt_schema: None,
                server_version: 0,
            }),
            read_cursor: 0,
            expected_epoch: 1,
        };

        wp.emit_pending_scan_chunk();
        assert!(wp.pending_scan.is_none(), "remaining 5 rows fit in one chunk");

        let data = consume_one(ptr);
        assert!(
            ipc::decode_wire_ipc(&data).is_err(),
            "continuation frame without schema must fail decode_wire_ipc"
        );
        let hint = ipc::SchemaWithVersion { descriptor: &schema, version: 0 };
        let decoded = ipc::decode_wire_ipc_with_schema(&data, hint)
            .expect("decode_wire_ipc_with_schema must succeed for continuation frame");
        let b = decoded.data_batch.expect("continuation chunk must carry data");
        assert_eq!(b.count, 5);
        for i in 0..5usize { assert_eq!(b.get_pk(i), (i + 5) as u128); }
        assert_ne!(decoded.control.flags & FLAG_SCAN_LAST, 0,
            "FLAG_SCAN_LAST must be set on the last chunk");
        assert_ne!(decoded.control.flags & FLAG_CONTINUATION, 0);

        unsafe { libc::munmap(ptr as *mut libc::c_void, w2m_ring::W2M_REGION_SIZE); }
    }

    /// send_scan_response with schema=None (avoids catalog) emits a single ring
    /// message with FLAG_CONTINUATION | FLAG_SCAN_LAST for a small wire-safe batch,
    /// and leaves pending_scan == None.
    #[test]
    fn test_send_scan_response_single_frame() {
        let schema = test_schema();
        let batch = make_n_row_batch(schema, 5);

        let (ptr, writer) = make_ring();
        let mut wp = WorkerProcess {
            worker_id: 0, num_workers: 1, master_pid: 0,
            catalog: std::ptr::null_mut(),
            sal_reader: unsafe { std::mem::zeroed() },
            w2m_writer: writer,
            exchange: make_handler(),
            pending_deltas: HashMap::new(),
            pending_scan: None,
            read_cursor: 0,
            expected_epoch: 1,
        };

        let err = wp.send_scan_response(1, Rc::new(batch), None, 3, 0, 0);
        assert!(err.is_none(), "small wire-safe batch must not error");
        assert!(wp.pending_scan.is_none(),
            "batch fits in one frame; send_scan_response must not set pending_scan");

        let data = consume_one(ptr);
        let ctrl = ipc::peek_control_block(&data).expect("peek_control_block");
        assert_eq!(ctrl.status, STATUS_OK);
        assert_ne!(ctrl.flags & FLAG_SCAN_LAST, 0, "single-frame response must set FLAG_SCAN_LAST");
        assert_ne!(ctrl.flags & FLAG_CONTINUATION, 0);

        let hint = ipc::SchemaWithVersion { descriptor: &schema, version: 0 };
        let decoded = ipc::decode_wire_ipc_with_schema(&data, hint)
            .expect("decode with schema hint");
        let b = decoded.data_batch.expect("data block");
        assert_eq!(b.count, 5);
        for i in 0..5usize { assert_eq!(b.get_pk(i), i as u128); }

        unsafe { libc::munmap(ptr as *mut libc::c_void, w2m_ring::W2M_REGION_SIZE); }
    }

    /// STRING-column schemas are not wire-safe: send_scan_response sends them as a
    /// single frame without chunking (and returns an error if the batch exceeds
    /// MAX_W2M_MSG). The full error path requires a CatalogEngine + a > 256 MiB
    /// batch; this test verifies the predicate that gates that branch.
    #[test]
    fn test_string_schema_not_wire_safe() {
        use crate::schema::{SchemaColumn, type_code};
        let sd = SchemaDescriptor::new(
            &[SchemaColumn::new(type_code::U64, 0), SchemaColumn::new(type_code::STRING, 0)],
            &[0],
        );
        assert!(!schema_wire_safe(&sd),
            "STRING-column schema must not be wire-safe (no chunking)");
        assert!(schema_wire_safe(&test_schema()),
            "U64-only schema must be wire-safe (chunking enabled)");
    }

}
