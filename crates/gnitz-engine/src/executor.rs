//! Server executor: client-facing event loop that dispatches DML/DDL,
//! manages tick coalescing, and coordinates multi-worker fan-out.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Instant;

use gnitz_transport::transport::{OutputSlots, Transport};
use gnitz_transport::uring::IoUringRing;

use crate::catalog::{CatalogEngine, FIRST_USER_TABLE_ID, SEQ_ID_SCHEMAS, SEQ_ID_TABLES, SEQ_ID_INDICES};
use crate::schema::SchemaDescriptor;
use crate::ipc::{self, DecodedWire, STATUS_OK, STATUS_ERROR, W2M_HEADER_SIZE, WireConflictMode};
use crate::master::MasterDispatcher;
use crate::reactor::{Reactor, TickIdleBarrier};
use crate::storage::Batch;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const MAX_PENDING_ROWS: usize = 100_000;
const TICK_COALESCE_ROWS: usize = 10_000;
const TICK_DEADLINE_MS: u64 = 20;
const FLUSH_DEADLINE_MS: u64 = 1;
const POLL_BUF_SIZE: usize = 256;

/// Client wire protocol flags (low bits of the flags field).
const FLAG_ALLOCATE_TABLE_ID: u64  = 1;
const FLAG_ALLOCATE_SCHEMA_ID: u64 = 2;
const FLAG_SEEK: u64               = 128;
const FLAG_SEEK_BY_INDEX: u64      = 256;
const FLAG_ALLOCATE_INDEX_ID: u64  = 512;

// ---------------------------------------------------------------------------
// Tick state machine
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq)]
enum TickState {
    Idle,
    Active,
}

// ---------------------------------------------------------------------------
// Deferred reply: result of a SELECT spawned as a reactor task
// ---------------------------------------------------------------------------
//
// Going through a shared queue keeps the spawned task `Send + 'static`
// without requiring `transport` to be shareable.

struct DeferredReply {
    fd: i32,
    client_id: u64,
    target_id: i64,
    status: u32,
    error_msg: String,
    result: Option<Arc<Batch>>,
    lsn: u64,
}

#[derive(Clone, Copy)]
enum DeferredKind {
    SeekByIndex,
    Seek,
    Scan,
}

struct PendingDdlResponse {
    fd: i32,
    client_id: u64,
    target_id: i64,
    lsn: u64,
}

// ---------------------------------------------------------------------------
// Pending batch accumulator (multi-worker buffered pushes)
// ---------------------------------------------------------------------------

struct PendingEntry {
    fd: i32,
    client_id: u64,
    target_id: i64,
    batch: Batch,
    mode: WireConflictMode,
}

struct PendingBatch {
    // Grouped by target_id — within-tid order matches insertion order so
    // per-entry responses go out in the order clients submitted them.
    // Across-tid order is undefined; `flush_pending_pushes` sorts before use.
    by_tid: HashMap<i64, Vec<PendingEntry>>,
    row_count: usize,
}

impl PendingBatch {
    fn new() -> Self {
        PendingBatch {
            by_tid: HashMap::with_capacity(8),
            row_count: 0,
        }
    }

    fn add(
        &mut self, fd: i32, client_id: u64, target_id: i64,
        batch: Batch, mode: WireConflictMode,
    ) {
        self.row_count += batch.count;
        self.by_tid.entry(target_id).or_default()
            .push(PendingEntry { fd, client_id, target_id, batch, mode });
    }

    fn is_empty(&self) -> bool {
        self.row_count == 0
    }

    fn drain_all(&mut self) -> Vec<PendingEntry> {
        let total: usize = self.by_tid.values().map(|v| v.len()).sum();
        let mut out = Vec::with_capacity(total);
        for v in self.by_tid.values_mut() {
            out.append(v); // moves items, retains v's capacity in the map for next cycle
        }
        self.row_count = 0;
        out
    }

    fn drain_tid(&mut self, tid: i64) -> Vec<PendingEntry> {
        let v = self.by_tid.remove(&tid).unwrap_or_default();
        let dropped: usize = v.iter().map(|e| e.batch.count).sum();
        self.row_count -= dropped;
        v
    }
}

/// Per-target_id group metadata used during group commit.
struct GroupInfo {
    start: usize,
    end: usize,
    target_id: i64,
    merged_count: usize,
    error: Option<String>,
    current_lsn: u64,
}

/// State for push groups that were pre-written to the SAL during an async
/// tick.  Phase A (SAL write + fdatasync submit) runs overlapped with worker
/// tick evaluation; Phase B (signal + ACK collection + client response) runs
/// after the tick completes.
struct PreWritten {
    groups: Vec<GroupInfo>,
    entries: Vec<PendingEntry>,
    /// Reactor-assigned fsync id. `Some` iff any group wrote successfully
    /// (i.e., an fdatasync was submitted); invariant enforced in
    /// `pre_write_pushes`.
    fsync_id: Option<u64>,
}

// ---------------------------------------------------------------------------
// ServerExecutor
// ---------------------------------------------------------------------------

pub struct ServerExecutor {
    catalog: *mut CatalogEngine,
    dispatcher: *mut MasterDispatcher,

    // Tick coalescing
    tick_tids: Vec<i64>,
    tick_rows: HashMap<i64, usize>,
    t_last_push: Option<Instant>,

    // Deadline-based group commit
    t_first_pending: Option<Instant>,

    // LSN tracking (monotonic)
    ingest_lsn: u64,
    last_tick_lsn: u64,

    // Async tick state machine (multi-worker only)
    tick_state: TickState,
    tick_queue_tids: Vec<i64>,

    // SELECTs that arrive during an active tick are spawned as reactor
    // tasks; each task awaits `tick_idle_barrier`, runs the sync fan-out,
    // and pushes a result into `deferred_replies` for the main loop to
    // ship back to the client.
    reactor: Rc<Reactor>,
    tick_idle_barrier: Rc<TickIdleBarrier>,
    deferred_replies: Rc<RefCell<Vec<DeferredReply>>>,

    // DDL response batching (deferred until end-of-cycle fdatasync)
    pending_ddl_responses: Vec<PendingDdlResponse>,

    // Schema descriptor cache (table IDs are monotonic, never reused)
    schema_cache: HashMap<i64, SchemaDescriptor>,

    // Column name bytes cache: avoids re-cloning Vec<String> and re-encoding
    // to bytes on every client response. Populated lazily alongside schema_cache.
    col_names_cache: HashMap<i64, Vec<Vec<u8>>>,

    // SAL fd; passed to `reactor.submit_fsync` on each push-path fdatasync.
    sal_fd: i32,

    // Push groups pre-written to SAL during async tick (Phase A of overlap).
    pre_written: Option<PreWritten>,
}

// Safety: ServerExecutor is single-threaded (master process event loop).
unsafe impl Send for ServerExecutor {}

impl ServerExecutor {
    pub fn new(
        catalog: *mut CatalogEngine,
        dispatcher: *mut MasterDispatcher,
        sal_fd: i32,
    ) -> Self {
        let reactor = Rc::new(Reactor::new(64)
            .expect("Failed to create reactor io_uring"));
        ServerExecutor {
            catalog,
            dispatcher,
            tick_tids: Vec::with_capacity(8),
            tick_rows: HashMap::new(),
            t_last_push: None,
            t_first_pending: None,
            ingest_lsn: 0,
            last_tick_lsn: 0,
            tick_state: TickState::Idle,
            tick_queue_tids: Vec::with_capacity(8),
            reactor,
            tick_idle_barrier: Rc::new(TickIdleBarrier::new()),
            deferred_replies: Rc::new(RefCell::new(Vec::new())),
            pending_ddl_responses: Vec::with_capacity(8),
            schema_cache: HashMap::new(),
            col_names_cache: HashMap::new(),
            sal_fd,
            pre_written: None,
        }
    }

    // -- Accessors for borrowed catalog / dispatcher --------------------------

    #[inline]
    fn cat(&mut self) -> &mut CatalogEngine {
        unsafe { &mut *self.catalog }
    }

    #[inline]
    fn disp(&mut self) -> &mut MasterDispatcher {
        unsafe { &mut *self.dispatcher }
    }

    // -- Schema helpers -------------------------------------------------------

    fn get_schema_desc(&mut self, target_id: i64) -> SchemaDescriptor {
        if let Some(&cached) = self.schema_cache.get(&target_id) {
            return cached;
        }
        let desc = self.cat().get_schema_desc(target_id)
            .unwrap_or_else(|| SchemaDescriptor::minimal_u64());
        self.schema_cache.insert(target_id, desc);
        desc
    }

    // -- Data operations -------------------------------------------------------

    fn handle_push(
        &mut self, target_id: i64, in_batch: Option<Batch>,
        mode: WireConflictMode,
    ) -> Result<(Option<Arc<Batch>>, u64), String> {
        if target_id >= FIRST_USER_TABLE_ID {
            if let Some(batch) = in_batch {
                if batch.count > 0 {
                    self.cat().validate_unique_indices(target_id, &batch)?;
                    self.disp().validate_all_distributed(target_id, &batch, mode)?;
                    self.disp().fan_out_push(target_id, &batch, mode)?;
                    // fan_out_push is synchronous (SAL write + sync + ACKs
                    // before return), so the batch is durable here. Feed
                    // the master-side unique-index filter.
                    self.disp().unique_filter_ingest_batch(target_id, &batch);
                    return Ok((None, 0));
                }
            }
            // Empty batch = SCAN
            self.fire_pending_ticks();
            let lsn = self.last_tick_lsn;
            let disp = self.disp();
            let result = disp.fan_out_scan(target_id)?.map(Arc::new);
            return Ok((result, lsn));
        }

        // System table path
        if let Some(batch) = in_batch {
            if batch.count > 0 {
                self.cat().ingest_to_family(target_id, &batch)?;
                let dag = self.cat().get_dag_ptr();
                unsafe { &mut *dag }.evaluate_dag(target_id, batch);
                self.ingest_lsn += 1;
                self.last_tick_lsn = self.ingest_lsn;
                return Ok((None, self.ingest_lsn));
            }
        }

        // Empty/None batch = SCAN
        let lsn = self.last_tick_lsn;
        let batch = self.cat().scan_family(target_id)?;
        let result = if batch.count > 0 { Some(batch) } else { None };
        Ok((result, lsn))
    }


    fn seek_family(&mut self, target_id: i64, pk_lo: u64, pk_hi: u64) -> Option<Batch> {
        match self.cat().seek_family(target_id, pk_lo, pk_hi) {
            Ok(opt) => opt,
            Err(_) => None,
        }
    }

    fn seek_by_index(
        &mut self, target_id: i64, col_idx: u32, key_lo: u64, key_hi: u64,
    ) -> Option<Batch> {
        match self.cat().seek_by_index(target_id, col_idx, key_lo, key_hi) {
            Ok(opt) => opt,
            Err(_) => None,
        }
    }

    // -- Response helpers -----------------------------------------------------

    fn send_response(
        &mut self,
        transport: &mut Transport<IoUringRing>,
        fd: i32,
        target_id: i64,
        result: Option<&Batch>,
        status: u32,
        error_msg: &[u8],
        client_id: u64,
        schema: &SchemaDescriptor,
        seek_pk_lo: u64,
    ) {
        let col_names_opt;
        let mut name_refs_arr = [&[] as &[u8]; 64];

        if status == STATUS_OK {
            if !self.col_names_cache.contains_key(&target_id) {
                let names = self.cat().get_column_names(target_id);
                let bytes: Vec<Vec<u8>> = names.into_iter().map(|n| n.into_bytes()).collect();
                self.col_names_cache.insert(target_id, bytes);
            }
            let cached = self.col_names_cache.get(&target_id).unwrap();
            for (i, n) in cached.iter().enumerate() {
                name_refs_arr[i] = n.as_slice();
            }
            col_names_opt = Some(&name_refs_arr[..cached.len()]);
        } else {
            col_names_opt = None;
        }

        let sz = ipc::wire_size(status, error_msg, Some(schema), col_names_opt, result);
        transport.send_encoded(fd, sz, |buf| {
            ipc::encode_wire_into(
                buf, 0,
                target_id as u64, client_id,
                0, seek_pk_lo, 0, 0, 0,
                status, error_msg,
                Some(schema), col_names_opt, result,
            )
        });
    }

    fn send_alloc_response(
        &mut self,
        transport: &mut Transport<IoUringRing>,
        fd: i32,
        new_id: i64,
        client_id: u64,
    ) {
        let sz = ipc::wire_size(STATUS_OK, b"", None, None, None);
        transport.send_encoded(fd, sz, |buf| {
            ipc::encode_wire_into(
                buf, 0,
                new_id as u64, client_id,
                0, 0, 0, 0, 0,
                STATUS_OK, b"",
                None, None, None,
            )
        });
    }

    fn send_error_response(
        &mut self,
        transport: &mut Transport<IoUringRing>,
        fd: i32,
        error_msg: &[u8],
        target_id: i64,
        client_id: u64,
    ) {
        let schema = SchemaDescriptor::minimal_u64();
        let sz = ipc::wire_size(STATUS_ERROR, error_msg, Some(&schema), None, None);
        transport.send_encoded(fd, sz, |buf| {
            ipc::encode_wire_into(
                buf, 0,
                target_id as u64, client_id,
                0, 0, 0, 0, 0,
                STATUS_ERROR, error_msg,
                Some(&schema), None, None,
            )
        });
    }

    // -- Pending batch management (multi-worker) ------------------------------

    fn flush_pending_for_tid(
        &mut self,
        transport: &mut Transport<IoUringRing>,
        target_id: i64,
        pending: &mut PendingBatch,
    ) {
        let flush_entries = pending.drain_tid(target_id);
        if flush_entries.is_empty() {
            return;
        }
        if pending.is_empty() {
            self.t_first_pending = None;
        }
        self.flush_pending_pushes(transport, flush_entries);
    }

    fn flush_pending_pushes(
        &mut self,
        transport: &mut Transport<IoUringRing>,
        mut entries: Vec<PendingEntry>,
    ) {
        if entries.is_empty() {
            return;
        }

        // Sort by (target_id, mode) so runs are homogeneous in mode.
        entries.sort_by_key(|e| (e.target_id, e.mode.as_u8()));

        // Phase 0: checkpoint before writing any groups.
        {
            let disp = self.disp();
            if let Err(e) = disp.maybe_checkpoint() {
                self.send_batch_errors(transport, &entries, 0, entries.len(), &e);
                return;
            }
        }

        // Phase 1: merge batches per (target_id, mode), write each group
        // to SAL (no sync). Mode must match across merged entries.
        let n = entries.len();
        let mut groups: Vec<GroupInfo> = Vec::new();
        let mut run_start = 0;

        while run_start < n {
            let target_id = entries[run_start].target_id;
            let mode = entries[run_start].mode;
            let mut run_end = run_start + 1;
            while run_end < n
                && entries[run_end].target_id == target_id
                && entries[run_end].mode == mode
            {
                run_end += 1;
            }

            let merged_count;
            let merged = {
                let schema = self.get_schema_desc(target_id);
                let total_rows: usize = entries[run_start..run_end]
                    .iter()
                    .map(|e| e.batch.count)
                    .sum();
                let mut m = Batch::with_schema(schema, total_rows.max(1));
                for k in run_start..run_end {
                    let b = &entries[k].batch;
                    m.append_batch(b, 0, b.count);
                }
                merged_count = m.count;
                m
            };

            let error = {
                let disp = self.disp();
                disp.write_ingest(target_id, &merged, mode).err()
            };

            groups.push(GroupInfo {
                start: run_start, end: run_end, target_id,
                merged_count, error, current_lsn: 0,
            });
            run_start = run_end;
        }

        let any_written = groups.iter().any(|g| g.error.is_none());

        // Phase 2: signal workers + submit async fdatasync, then collect ACKs.
        // Key insight: workers see SAL data via atomic release/acquire (no fsync
        // needed). Submit fdatasync asynchronously, collect ACKs while it runs,
        // then wait for fsync before ACKing clients.
        if any_written {
            let nw = self.disp().num_workers();
            // Signal workers immediately, submit async fdatasync in parallel
            // through the reactor so it overlaps with ACK collection below.
            let fsync_id = {
                let disp = unsafe { &mut *self.dispatcher };
                disp.signal_all();
                self.reactor.submit_fsync(self.sal_fd)
            };

            // Collect ACKs (blocking) — workers process while fsync runs.
            let mut w2m_rcs = vec![W2M_HEADER_SIZE as u64; nw];
            let mut any_success = false;
            for gi in 0..groups.len() {
                if groups[gi].error.is_some() { continue; }
                let disp = self.disp();
                match disp.collect_acks_at(&mut w2m_rcs) {
                    Ok(()) => {
                        self.ingest_lsn += 1;
                        groups[gi].current_lsn = self.ingest_lsn;
                        let tid = groups[gi].target_id;
                        let mc = groups[gi].merged_count;
                        if !self.tick_rows.contains_key(&tid) {
                            self.tick_tids.push(tid);
                            self.tick_rows.insert(tid, 0);
                        }
                        *self.tick_rows.get_mut(&tid).unwrap() += mc;
                        any_success = true;
                    }
                    Err(e) => {
                        for gj in gi..groups.len() {
                            if groups[gj].error.is_none() {
                                groups[gj].error = Some(e.clone());
                            }
                        }
                        break;
                    }
                }
            }
            if any_success {
                self.t_last_push = Some(Instant::now());
            }

            self.disp().reset_w2m_cursors();

            // Workers already see the SAL via atomics and have committed.
            // If fdatasync fails, ACKing the client would split-brain the
            // system (worker durable, master not). Crash-stop instead.
            //
            // W2M interleave invariant: all sync W2M reads above are
            // complete, and `tick_idle_barrier` is still active (no
            // `notify_all` yet), so any W2M replies the reactor reads
            // during `block_on_fsync` fall into the unrouted path (logged,
            // cursor advanced, in_flight untouched). Do NOT move a
            // barrier `notify_all` above this call.
            let fsync_rc = self.reactor.block_on_fsync(fsync_id);
            if fsync_rc < 0 {
                gnitz_fatal_abort!("SAL fdatasync (async) failed rc={}", fsync_rc);
            }

            // Update the master-side unique-index filter from successfully
            // durable batches. Any group that errored (either at SAL write,
            // worker ACK, or fsync) must have its filter invalidated — the
            // filter state may be out of sync with worker state.
            for g in &groups {
                let disp = unsafe { &mut *self.dispatcher };
                if g.error.is_some() {
                    disp.unique_filter_invalidate_table(g.target_id);
                    continue;
                }
                for k in g.start..g.end {
                    disp.unique_filter_ingest_batch(g.target_id, &entries[k].batch);
                }
            }
        }

        // Phase 3: send responses.
        for g in &groups {
            let schema = self.get_schema_desc(g.target_id);
            let col_names_owned = if g.error.is_none() {
                Some(self.cat().get_column_names(g.target_id))
            } else {
                None
            };
            let ok_refs: Option<Vec<&[u8]>> = col_names_owned.as_ref().map(|names|
                names.iter().map(|s| s.as_bytes()).collect()
            );
            let sz = if g.error.is_some() {
                ipc::wire_size(STATUS_ERROR, g.error.as_ref().unwrap().as_bytes(), Some(&schema), None, None)
            } else {
                ipc::wire_size(STATUS_OK, b"", Some(&schema), Some(ok_refs.as_ref().unwrap()), None)
            };
            for k in g.start..g.end {
                let e = &entries[k];
                let rc = if let Some(ref err_msg) = g.error {
                    transport.send_encoded(e.fd, sz, |buf| {
                        ipc::encode_wire_into(
                            buf, 0,
                            g.target_id as u64, e.client_id,
                            0, 0, 0, 0, 0,
                            STATUS_ERROR, err_msg.as_bytes(),
                            Some(&schema), None, None,
                        )
                    })
                } else {
                    transport.send_encoded(e.fd, sz, |buf| {
                        ipc::encode_wire_into(
                            buf, 0,
                            g.target_id as u64, e.client_id,
                            0, g.current_lsn, 0, 0, 0,
                            STATUS_OK, b"",
                            Some(&schema), Some(ok_refs.as_ref().unwrap()), None,
                        )
                    })
                };
                if rc < 0 {
                    transport.close_fd(e.fd);
                }
            }
        }
    }

    /// Phase A of push overlap: write push groups to SAL + submit async
    /// fdatasync, but do NOT signal workers or collect ACKs.  Called during
    /// TickState::Active so that encoding + fdatasync run concurrently with
    /// worker tick evaluation.  Phase B (complete_pre_written) runs after the
    /// tick completes.
    /// Returns `Some(entries)` if checkpoint failed so the caller can re-insert
    /// them into `pending` for a later synchronous flush.
    fn pre_write_pushes(&mut self, mut entries: Vec<PendingEntry>) -> Option<Vec<PendingEntry>> {
        if entries.is_empty() { return None; }

        entries.sort_by_key(|e| (e.target_id, e.mode.as_u8()));

        // Phase 0: checkpoint.  Can block — same as flush_pending_pushes.
        {
            let disp = self.disp();
            if let Err(_e) = disp.maybe_checkpoint() {
                // No transport here to send errors; return the entries so the
                // caller re-inserts them into pending and flushes them
                // synchronously after the tick via the normal path.
                return Some(entries);
            }
        }

        // Phase 1: merge + write_ingest to SAL (no signal).
        let n = entries.len();
        let mut groups: Vec<GroupInfo> = Vec::new();
        let mut run_start = 0;

        while run_start < n {
            let target_id = entries[run_start].target_id;
            let mode = entries[run_start].mode;
            let mut run_end = run_start + 1;
            while run_end < n
                && entries[run_end].target_id == target_id
                && entries[run_end].mode == mode
            {
                run_end += 1;
            }

            let merged_count;
            let merged = {
                let schema = self.get_schema_desc(target_id);
                let total_rows: usize = entries[run_start..run_end]
                    .iter()
                    .map(|e| e.batch.count)
                    .sum();
                let mut m = Batch::with_schema(schema, total_rows.max(1));
                for k in run_start..run_end {
                    let b = &entries[k].batch;
                    m.append_batch(b, 0, b.count);
                }
                merged_count = m.count;
                m
            };

            let error = {
                let disp = self.disp();
                disp.write_ingest(target_id, &merged, mode).err()
            };

            groups.push(GroupInfo {
                start: run_start, end: run_end, target_id,
                merged_count, error, current_lsn: 0,
            });
            run_start = run_end;
        }

        let any_written = groups.iter().any(|g| g.error.is_none());
        let fsync_id = if any_written {
            Some(self.reactor.submit_fsync(self.sal_fd))
        } else {
            None
        };

        self.pre_written = Some(PreWritten { groups, entries, fsync_id });
        None
    }

    /// Phase B of push overlap: signal workers, collect push ACKs, wait
    /// fdatasync, and send client responses.  Called after the async tick
    /// completes — the W2M read cursors from tick collection are still valid
    /// (finish_async_tick does not reset them).
    fn complete_pre_written(&mut self, transport: &mut Transport<IoUringRing>) {
        let pre = match self.pre_written.take() {
            Some(p) => p,
            None => return,
        };
        let PreWritten { mut groups, entries, fsync_id } = pre;

        let any_written = groups.iter().any(|g| g.error.is_none());
        if any_written {
            // Signal workers so they drain the pre-written SAL groups.
            {
                let disp = unsafe { &mut *self.dispatcher };
                disp.signal_all();
            }

            // Collect one push ACK round per group, using the W2M cursors
            // left behind by poll_tick_progress (positioned after tick ACKs).
            let mut any_success = false;
            for gi in 0..groups.len() {
                if groups[gi].error.is_some() { continue; }
                let disp = self.disp();
                match disp.collect_acks_continuing() {
                    Ok(()) => {
                        self.ingest_lsn += 1;
                        groups[gi].current_lsn = self.ingest_lsn;
                        let tid = groups[gi].target_id;
                        let mc = groups[gi].merged_count;
                        if !self.tick_rows.contains_key(&tid) {
                            self.tick_tids.push(tid);
                            self.tick_rows.insert(tid, 0);
                        }
                        *self.tick_rows.get_mut(&tid).unwrap() += mc;
                        any_success = true;
                    }
                    Err(e) => {
                        for gj in gi..groups.len() {
                            if groups[gj].error.is_none() {
                                groups[gj].error = Some(e.clone());
                            }
                        }
                        break;
                    }
                }
            }
            if any_success {
                self.t_last_push = Some(Instant::now());
            }

            // Wait for fdatasync (submitted in pre_write_pushes, ran during
            // tick). Crash-stop on failure — same policy as flush_pending_pushes.
            //
            // W2M interleave invariant: sync W2M draining via
            // `collect_acks_continuing` above is complete and
            // `tick_idle_barrier` is still active; `block_on_fsync` only
            // sees unrouted W2M replies, which the reactor logs + drops.
            // Do NOT move a barrier `notify_all` above this call.
            let fsync_rc = self.reactor.block_on_fsync(
                fsync_id.expect("any_written implies fsync submitted"),
            );
            if fsync_rc < 0 {
                gnitz_fatal_abort!("SAL fdatasync (pre-written) failed rc={}", fsync_rc);
            }

            // Update unique-index filters.
            for g in &groups {
                let disp = unsafe { &mut *self.dispatcher };
                if g.error.is_some() {
                    disp.unique_filter_invalidate_table(g.target_id);
                    continue;
                }
                for k in g.start..g.end {
                    disp.unique_filter_ingest_batch(g.target_id, &entries[k].batch);
                }
            }
        }

        // Send client responses.
        for g in &groups {
            let schema = self.get_schema_desc(g.target_id);
            let col_names_owned = if g.error.is_none() {
                Some(self.cat().get_column_names(g.target_id))
            } else {
                None
            };
            let ok_refs: Option<Vec<&[u8]>> = col_names_owned.as_ref().map(|names|
                names.iter().map(|s| s.as_bytes()).collect()
            );
            let sz = if g.error.is_some() {
                ipc::wire_size(STATUS_ERROR, g.error.as_ref().unwrap().as_bytes(), Some(&schema), None, None)
            } else {
                ipc::wire_size(STATUS_OK, b"", Some(&schema), Some(ok_refs.as_ref().unwrap()), None)
            };
            for k in g.start..g.end {
                let e = &entries[k];
                let rc = if let Some(ref err_msg) = g.error {
                    transport.send_encoded(e.fd, sz, |buf| {
                        ipc::encode_wire_into(
                            buf, 0,
                            g.target_id as u64, e.client_id,
                            0, 0, 0, 0, 0,
                            STATUS_ERROR, err_msg.as_bytes(),
                            Some(&schema), None, None,
                        )
                    })
                } else {
                    transport.send_encoded(e.fd, sz, |buf| {
                        ipc::encode_wire_into(
                            buf, 0,
                            g.target_id as u64, e.client_id,
                            0, g.current_lsn, 0, 0, 0,
                            STATUS_OK, b"",
                            Some(&schema), Some(ok_refs.as_ref().unwrap()), None,
                        )
                    })
                };
                if rc < 0 {
                    transport.close_fd(e.fd);
                }
            }
        }
    }

    fn send_batch_errors(
        &mut self,
        transport: &mut Transport<IoUringRing>,
        entries: &[PendingEntry],
        start: usize,
        end: usize,
        err_msg: &str,
    ) {
        for k in start..end {
            let e = &entries[k];
            let schema = self.get_schema_desc(e.target_id);
            let sz = ipc::wire_size(STATUS_ERROR, err_msg.as_bytes(), Some(&schema), None, None);
            if transport.send_encoded(e.fd, sz, |buf| {
                ipc::encode_wire_into(
                    buf, 0,
                    e.target_id as u64, e.client_id,
                    0, 0, 0, 0, 0,
                    STATUS_ERROR, err_msg.as_bytes(),
                    Some(&schema), None, None,
                )
            }) < 0 {
                transport.close_fd(e.fd);
            }
        }
    }

    // -- Tick coalescing + async tick ------------------------------------------

    fn should_fire_ticks(&self) -> bool {
        if self.tick_tids.is_empty() {
            return false;
        }
        for tid in &self.tick_tids {
            if let Some(&rows) = self.tick_rows.get(tid) {
                if rows >= TICK_COALESCE_ROWS {
                    return true;
                }
            }
        }
        if let Some(t) = self.t_last_push {
            if t.elapsed().as_millis() as u64 >= TICK_DEADLINE_MS {
                return true;
            }
        }
        false
    }

    fn fire_pending_ticks(&mut self) {
        if self.tick_tids.is_empty() {
            return;
        }
        let tids = std::mem::replace(&mut self.tick_tids, Vec::with_capacity(8));
        self.tick_rows.clear();
        self.t_last_push = None;

        for tid in tids {
            if !self.cat().has_id(tid) {
                continue;
            }
            let disp = self.disp();
            if let Err(e) = disp.fan_out_tick(tid) {
                gnitz_warn!("fan_out_tick error tid={}: {}", tid, e);
            }
        }
        self.last_tick_lsn = self.ingest_lsn;
    }

    fn check_and_fire_pending_ticks_async(&mut self) {
        if !self.should_fire_ticks() {
            return;
        }

        // Move all pending tids to tick_queue_tids, then fire them as one batch.
        self.tick_queue_tids = std::mem::replace(&mut self.tick_tids, Vec::with_capacity(8));
        self.tick_rows.clear();
        self.t_last_push = None;

        self.start_all_async_ticks();
    }

    /// Fire all tids in tick_queue_tids as a single batched tick.
    /// Writes N FLAG_TICK groups to the SAL and signals workers once.
    /// Dropped tables are filtered out before firing; if all tids are dropped
    /// the function is a no-op.
    fn start_all_async_ticks(&mut self) {
        // Drain first to release the borrow on tick_queue_tids before calling
        // self.cat() in the filter — the compiler cannot see that drain() and
        // cat() access disjoint fields.
        let mut tids: Vec<i64> = self.tick_queue_tids.drain(..).collect();
        tids.retain(|&tid| self.cat().has_id(tid));

        if tids.is_empty() {
            return;
        }

        self.tick_state = TickState::Active;
        self.tick_idle_barrier.set_active();
        if let Err(e) = self.disp().start_ticks_async_batch(&tids) {
            gnitz_warn!("start_ticks_async_batch error: {}", e);
            self.tick_state = TickState::Idle;
            self.tick_idle_barrier.notify_all();
        }
    }

    /// Spawn a SELECT that arrived during an active tick as a reactor task.
    /// The catalog/dispatcher are captured as raw pointers wrapped in a
    /// `Send` marker; safe because the master is single-threaded and
    /// `reactor.poll_nonblocking()` runs synchronously inside the main
    /// event loop.
    fn spawn_deferred_select(
        &mut self,
        fd: i32,
        client_id: u64,
        target_id: i64,
        kind: DeferredKind,
        seek_pk_lo: u64,
        seek_pk_hi: u64,
        seek_col_idx: u64,
    ) {
        struct DispPtr(*mut MasterDispatcher);
        unsafe impl Send for DispPtr {}
        struct CatPtr(*mut CatalogEngine);
        unsafe impl Send for CatPtr {}

        let disp_ptr = DispPtr(self.dispatcher);
        let cat_ptr = CatPtr(self.catalog);
        let barrier = Rc::clone(&self.tick_idle_barrier);
        let replies = Rc::clone(&self.deferred_replies);
        let reactor = Rc::clone(&self.reactor);
        let scan_lsn = self.last_tick_lsn;

        self.reactor.spawn(async move {
            barrier.wait().await;

            let is_user_table = target_id >= FIRST_USER_TABLE_ID;
            let (lsn, result) = match kind {
                DeferredKind::SeekByIndex => (0, if is_user_table {
                    resolve_async(
                        crate::master::MasterDispatcher::fan_out_seek_by_index_async(
                            disp_ptr.0, &reactor, target_id,
                            seek_col_idx as u32, seek_pk_lo, seek_pk_hi,
                        ).await,
                        || unsafe {
                            (&mut *disp_ptr.0).fan_out_seek_by_index(
                                target_id, seek_col_idx as u32, seek_pk_lo, seek_pk_hi,
                            )
                        },
                    )
                } else {
                    let cat = unsafe { &mut *cat_ptr.0 };
                    Ok(cat.seek_by_index(target_id, seek_col_idx as u32,
                        seek_pk_lo, seek_pk_hi).unwrap_or(None))
                }),
                DeferredKind::Seek => (0, if is_user_table {
                    crate::master::MasterDispatcher::fan_out_seek_async(
                        disp_ptr.0, &reactor, target_id, seek_pk_lo, seek_pk_hi,
                    ).await.map_err(|e| match e {
                        crate::master::FanOutError::Worker(msg) => msg,
                        // Seek has no fallback: pk routing is always cached.
                        crate::master::FanOutError::NeedsSyncFallback => unreachable!(),
                    })
                } else {
                    let cat = unsafe { &mut *cat_ptr.0 };
                    Ok(cat.seek_family(target_id, seek_pk_lo, seek_pk_hi)
                        .unwrap_or(None))
                }),
                DeferredKind::Scan => (scan_lsn, resolve_async(
                    // dispatch_payload only spawns SCAN for user tables.
                    crate::master::MasterDispatcher::fan_out_scan_async(
                        disp_ptr.0, &reactor, target_id,
                    ).await,
                    || unsafe { (&mut *disp_ptr.0).fan_out_scan(target_id) },
                )),
            };

            replies.borrow_mut().push(build_deferred_reply(fd, client_id, target_id, lsn, result));
        });
    }

    fn poll_tick_active(
        &mut self,
        transport: &mut Transport<IoUringRing>,
        pending: &mut PendingBatch,
    ) {
        let done = {
            let disp = self.disp();
            match disp.poll_tick_progress() {
                Ok(d) => d,
                Err(e) => {
                    gnitz_warn!("poll_tick_progress error: {}", e);
                    // On error, discard any pre-written state — we can't
                    // safely collect push ACKs from a failed tick.
                    self.pre_written = None;
                    true
                }
            }
        };

        if !done {
            return;
        }

        gnitz_debug!("async tick complete");
        self.last_tick_lsn = self.ingest_lsn;

        // Complete pre-written pushes (Phase B) before resetting W2M.
        // The W2M read cursors from poll_tick_progress are positioned after
        // all tick ACKs; push ACKs follow them in the W2M ring.
        if self.pre_written.is_some() {
            self.complete_pre_written(transport);
        }
        self.disp().reset_w2m_cursors();

        // All ticks in the batch are complete — transition to IDLE.
        // tick_queue_tids is always empty here: start_all_async_ticks drains
        // it in one shot before firing the batch.
        debug_assert!(self.tick_queue_tids.is_empty(),
            "tick_queue_tids must be empty after start_all_async_ticks");
        self.tick_state = TickState::Idle;
        self.tick_idle_barrier.notify_all();

        // Deferred SELECTs must complete before any subsequent sync W2M
        // op runs: the reactor owns the W2M ring during this window.
        self.reactor.block_until_idle();
        self.flush_deferred_replies(transport);

        // Flush pushes accumulated during the tick (any that arrived after
        // the pre-write, or all of them if pre-write wasn't triggered).
        if !pending.is_empty() {
            let entries = pending.drain_all();
            self.flush_pending_pushes(transport, entries);
        }
    }

    // -- Message dispatch -----------------------------------------------------

    fn dispatch_message(
        &mut self,
        transport: &mut Transport<IoUringRing>,
        fd: i32,
        data: &[u8],
        pending: &mut PendingBatch,
    ) {
        let decoded = {
            // Hot path: peek target_id, skip schema decode if cached.
            let target_id = ipc::peek_target_id(data).unwrap_or(-1);
            let result = if let Some(&cached) = self.schema_cache.get(&target_id) {
                ipc::decode_wire_with_schema(data, &cached)
            } else {
                ipc::decode_wire(data)
            };
            match result {
                Ok(d) => d,
                Err(e) => {
                    let msg = format!("decode error: {}", e);
                    self.send_error_response(transport, fd, msg.as_bytes(), 0, 0);
                    return;
                }
            }
        };

        let client_id = decoded.control.client_id;
        let target_id = decoded.control.target_id as i64;

        // Catch panics from catalog/DAG operations to prevent server crash.
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.dispatch_payload(transport, fd, decoded, pending)
        }));

        let err_msg = match result {
            Ok(Ok(())) => return,
            Ok(Err(msg)) => msg,
            Err(_) => "internal server error (panic)".to_string(),
        };

        let schema = SchemaDescriptor::minimal_u64();
        let sz = ipc::wire_size(STATUS_ERROR, err_msg.as_bytes(), Some(&schema), None, None);
        if transport.send_encoded(fd, sz, |buf| {
            ipc::encode_wire_into(
                buf, 0,
                target_id as u64, client_id,
                0, 0, 0, 0, 0,
                STATUS_ERROR, err_msg.as_bytes(),
                Some(&schema), None, None,
            )
        }) < 0 {
            transport.close_fd(fd);
        }
    }

    fn dispatch_payload(
        &mut self,
        transport: &mut Transport<IoUringRing>,
        fd: i32,
        decoded: DecodedWire,
        pending: &mut PendingBatch,
    ) -> Result<(), String> {
        let client_id = decoded.control.client_id;
        let target_id = decoded.control.target_id as i64;
        let flags = decoded.control.flags;

        // ID allocation — immediate response, no data/schema needed
        if target_id == 0 {
            if flags & FLAG_ALLOCATE_TABLE_ID != 0 {
                let new_id = self.cat().allocate_table_id();
                self.cat().advance_sequence(SEQ_ID_TABLES, new_id - 1, new_id);
                self.send_alloc_response(transport, fd, new_id, client_id);
                return Ok(());
            }
            if flags & FLAG_ALLOCATE_SCHEMA_ID != 0 {
                let new_id = self.cat().allocate_schema_id();
                self.cat().advance_sequence(SEQ_ID_SCHEMAS, new_id - 1, new_id);
                self.send_alloc_response(transport, fd, new_id, client_id);
                return Ok(());
            }
            if flags & FLAG_ALLOCATE_INDEX_ID != 0 {
                let new_id = self.cat().allocate_index_id();
                self.cat().advance_sequence(SEQ_ID_INDICES, new_id - 1, new_id);
                self.send_alloc_response(transport, fd, new_id, client_id);
                return Ok(());
            }
        }

        // During TICK_ACTIVE, defer reads (so they see the post-tick state)
        // by spawning them as reactor tasks awaiting `tick_idle_barrier`.
        if self.tick_state == TickState::Active {
            if flags & FLAG_SEEK_BY_INDEX != 0 {
                self.spawn_deferred_select(
                    fd, client_id, target_id, DeferredKind::SeekByIndex,
                    decoded.control.seek_pk_lo,
                    decoded.control.seek_pk_hi,
                    decoded.control.seek_col_idx,
                );
                return Ok(());
            }
            if flags & FLAG_SEEK != 0 {
                self.spawn_deferred_select(
                    fd, client_id, target_id, DeferredKind::Seek,
                    decoded.control.seek_pk_lo,
                    decoded.control.seek_pk_hi,
                    0,
                );
                return Ok(());
            }
            // Scans (empty-batch push to user table)
            if target_id >= FIRST_USER_TABLE_ID
                && decoded.data_batch.as_ref().map_or(true, |b| b.count == 0)
            {
                self.spawn_deferred_select(
                    fd, client_id, target_id, DeferredKind::Scan, 0, 0, 0,
                );
                return Ok(());
            }
        }

        // Index seek — flush pushes for this table + fire ticks
        if flags & FLAG_SEEK_BY_INDEX != 0 {
            self.flush_pending_for_tid(transport, target_id, pending);
            self.fire_pending_ticks();
            let col_idx = decoded.control.seek_col_idx as u32;
            let schema = self.get_schema_desc(target_id);
            let result = if target_id >= FIRST_USER_TABLE_ID {
                let disp = self.disp();
                disp.fan_out_seek_by_index(
                    target_id, col_idx,
                    decoded.control.seek_pk_lo,
                    decoded.control.seek_pk_hi,
                )?
            } else {
                self.seek_by_index(
                    target_id, col_idx,
                    decoded.control.seek_pk_lo,
                    decoded.control.seek_pk_hi,
                )
            };
            self.send_response(
                transport, fd, target_id, result.as_ref(), STATUS_OK, b"",
                client_id, &schema, 0,
            );
            return Ok(());
        }

        // PK seek — flush pushes for this table (no tick needed)
        if flags & FLAG_SEEK != 0 {
            self.flush_pending_for_tid(transport, target_id, pending);
            let schema = self.get_schema_desc(target_id);
            let result = if target_id >= FIRST_USER_TABLE_ID {
                let disp = self.disp();
                disp.fan_out_seek(
                    target_id,
                    decoded.control.seek_pk_lo,
                    decoded.control.seek_pk_hi,
                )?
            } else {
                self.seek_family(
                    target_id,
                    decoded.control.seek_pk_lo,
                    decoded.control.seek_pk_hi,
                )
            };
            self.send_response(
                transport, fd, target_id, result.as_ref(), STATUS_OK, b"",
                client_id, &schema, 0,
            );
            return Ok(());
        }

        // Schema validation on incoming wire data
        let has_batch = decoded.data_batch.is_some();
        if has_batch {
            if let Some(ref wire_schema) = decoded.schema {
                let expected = self.get_schema_desc(target_id);
                validate_schema_match(wire_schema, &expected)?;
            }
        }

        // Bufferable: user-table DML with data
        if target_id >= FIRST_USER_TABLE_ID
            && has_batch
            && decoded.data_batch.as_ref().map_or(false, |b| b.count > 0)
        {
            // Decode the wire conflict mode (defaults to Update for
            // legacy clients that don't know about the new mode bit).
            let mode = ipc::decode_conflict_mode(
                decoded.control.flags,
                decoded.control.seek_col_idx,
            );

            // The pre-flush exists solely to give distributed validation
            // fresh worker state. Skip it when no distributed validator
            // would actually broadcast — all three validators below
            // early-return on unconstrained tables.
            let needs_flush = {
                let cat = self.cat();
                cat.get_fk_count(target_id) > 0
                    || cat.get_fk_children_count(target_id) > 0
                    || cat.has_any_unique_index(target_id)
                    || (matches!(mode, WireConflictMode::Error) && cat.table_has_unique_pk(target_id))
            };
            // If a distributed validation is required AND an async tick is
            // currently in flight, drain the tick first. The validators
            // below call `execute_pipeline`, which uses (and resets) the
            // shared w2m ring cursors. Running it concurrently with an
            // active tick consumes the tick's worker ACKs, leaving the
            // master deadlocked in `poll_tick_progress`.
            if needs_flush {
                while self.tick_state == TickState::Active {
                    self.poll_tick_active(transport, pending);
                }
                self.flush_pending_for_tid(transport, target_id, pending);
            }

            let batch = decoded.data_batch.unwrap();
            self.cat().validate_unique_indices(target_id, &batch)?;
            {
                let disp = self.disp();
                disp.validate_all_distributed(target_id, &batch, mode)?;
            }
            pending.add(fd, client_id, target_id, batch, mode);
            if self.t_first_pending.is_none() {
                self.t_first_pending = Some(Instant::now());
            }
            return Ok(());
        }

        // Non-bufferable: flush pushes for this table, then process inline
        self.flush_pending_for_tid(transport, target_id, pending);

        // DDL broadcast needs the batch after handle_push, but handle_push
        // consumes it. Clone upfront when broadcast is needed.
        let ddl_clone = if target_id < FIRST_USER_TABLE_ID
            && has_batch
        {
            decoded.data_batch.as_ref().map(|b| b.clone_batch())
        } else {
            None
        };

        // System tables and empty-batch scans: no conflict mode is
        // relevant. Default to Update to preserve existing behavior.
        let push_mode = ipc::decode_conflict_mode(
            decoded.control.flags, decoded.control.seek_col_idx,
        );
        let (result, lsn) = self.handle_push(target_id, decoded.data_batch, push_mode)?;

        // DDL broadcast for system tables (after local handle_push)
        if let Some(ddl_batch) = ddl_clone {
            let disp = self.disp();
            if let Err(e) = disp.broadcast_ddl(target_id, &ddl_batch) {
                gnitz_warn!("broadcast_ddl error tid={}: {}", target_id, e);
            }
            // Defer response until end-of-cycle fdatasync
            self.pending_ddl_responses.push(PendingDdlResponse {
                fd, client_id, target_id, lsn,
            });
        } else {
            let schema = self.get_schema_desc(target_id);
            self.send_response(
                transport, fd, target_id, result.as_deref(), STATUS_OK, b"",
                client_id, &schema, lsn,
            );
        }
        Ok(())
    }

    fn flush_pending_ddl_responses(&mut self, transport: &mut Transport<IoUringRing>) {
        if self.pending_ddl_responses.is_empty() {
            return;
        }
        let rc = self.disp().sync();
        if rc < 0 {
            gnitz_fatal_abort!("SAL fdatasync (DDL responses) failed rc={}", rc);
        }
        for r in std::mem::replace(&mut self.pending_ddl_responses, Vec::with_capacity(8)) {
            let schema = self.get_schema_desc(r.target_id);
            self.send_response(
                transport, r.fd, r.target_id, None, STATUS_OK, b"",
                r.client_id, &schema, r.lsn,
            );
        }
    }

    // -- Main event loop ------------------------------------------------------

    pub fn run(
        catalog: *mut CatalogEngine,
        dispatcher: *mut MasterDispatcher,
        server_fd: i32,
    ) -> i32 {
        let ring = match IoUringRing::new(256) {
            Ok(r) => r,
            Err(e) => {
                eprintln!("io_uring init failed: {}", e);
                return -1;
            }
        };
        let mut transport = Transport::new(ring, POLL_BUF_SIZE);
        transport.accept_conn(server_fd);

        let sal_fd = unsafe { &*dispatcher }.sal_fd();
        let mut exec = ServerExecutor::new(catalog, dispatcher, sal_fd);
        exec.reactor.attach_w2m(unsafe { &*dispatcher }.w2m_handle());
        exec.run_loop(&mut transport);
        0
    }

    fn run_loop(&mut self, transport: &mut Transport<IoUringRing>) {
        let mut out_fds = vec![0i32; POLL_BUF_SIZE];
        let mut out_ptrs = vec![std::ptr::null_mut::<u8>(); POLL_BUF_SIZE];
        let mut out_lens = vec![0u32; POLL_BUF_SIZE];
        let mut pending = PendingBatch::new();

        loop {
            // Worker crash detection
            {
                let disp = unsafe { &mut *self.dispatcher };
                let crashed = disp.check_workers();
                if crashed >= 0 {
                    let base_dir = &self.cat().base_dir;
                    eprintln!(
                        "Worker {} crashed (log: {}/worker_{}.log), shutting down",
                        crashed, base_dir, crashed,
                    );
                    disp.shutdown_workers();
                    return;
                }
            }

            // Compute poll timeout
            let mut timeout_ms = if self.tick_state == TickState::Active {
                1 // short poll: yield CPU to workers
            } else {
                let mut t = 500i32;
                if !self.tick_tids.is_empty() {
                    if let Some(last) = self.t_last_push {
                        let elapsed = last.elapsed().as_millis() as i32;
                        let remaining = TICK_DEADLINE_MS as i32 - elapsed;
                        if remaining < t {
                            t = if remaining > 0 { remaining } else { 0 };
                        }
                    }
                }
                if let Some(first) = self.t_first_pending {
                    let elapsed = first.elapsed().as_millis() as i32;
                    let remaining = FLUSH_DEADLINE_MS as i32 - elapsed;
                    if remaining < t {
                        t = if remaining > 0 { remaining } else { 0 };
                    }
                }
                t
            };
            // Cap the poll wait when the reactor has tasks ready to run,
            // so async SELECT tasks don't starve behind a long-blocking
            // transport.poll (which would otherwise sleep up to 500 ms).
            if self.reactor.has_pending_tasks() && timeout_ms > 1 {
                timeout_ms = 1;
            }

            let mut out = OutputSlots {
                fds: &mut out_fds,
                ptrs: &mut out_ptrs,
                lens: &mut out_lens,
            };

            let n = transport.poll(timeout_ms, &mut out);

            for i in 0..n as usize {
                let fd = out_fds[i];
                let ptr = out_ptrs[i];
                let len = out_lens[i] as usize;

                let data = unsafe { std::slice::from_raw_parts(ptr, len) };
                self.dispatch_message(transport, fd, data, &mut pending);

                Transport::<IoUringRing>::free_recv(ptr);

                if pending.row_count >= MAX_PENDING_ROWS {
                    if self.tick_state != TickState::Active {
                        self.t_first_pending = None;
                        let entries = pending.drain_all();
                        self.flush_pending_pushes(transport, entries);
                    } else if self.pre_written.is_none() {
                        // Tick active: pre-write to SAL (Phase A overlap).
                        // Only one pre-write per tick — further pushes buffer
                        // in pending until the tick completes.
                        self.t_first_pending = None;
                        let entries = pending.drain_all();
                        if let Some(returned) = self.pre_write_pushes(entries) {
                            // Checkpoint failed; re-insert so they flush
                            // synchronously after the tick via the normal path.
                            for e in returned {
                                pending.add(e.fd, e.client_id, e.target_id, e.batch, e.mode);
                            }
                        }
                    }
                }
            }

            if !pending.is_empty() && self.tick_state != TickState::Active {
                let deadline_expired = self.t_first_pending
                    .map_or(false, |t| t.elapsed().as_millis() as u64 >= FLUSH_DEADLINE_MS);
                if pending.row_count >= MAX_PENDING_ROWS || deadline_expired {
                    self.t_first_pending = None;
                    let entries = pending.drain_all();
                    self.flush_pending_pushes(transport, entries);
                }
            }

            if !self.pending_ddl_responses.is_empty() {
                self.flush_pending_ddl_responses(transport);
            }

            if self.tick_state == TickState::Active {
                self.poll_tick_active(transport, &mut pending);
            } else {
                self.check_and_fire_pending_ticks_async();
            }
        }
    }

    fn flush_deferred_replies(&mut self, transport: &mut Transport<IoUringRing>) {
        // `drain(..)` preserves the source Vec's capacity across iterations;
        // `mem::take` would reset it to 0 every time and re-allocate on the
        // next push.
        let queued: Vec<DeferredReply> = self.deferred_replies.borrow_mut().drain(..).collect();
        if queued.is_empty() {
            return;
        }
        for r in queued {
            if r.status == STATUS_ERROR {
                self.send_error_response(
                    transport, r.fd, r.error_msg.as_bytes(),
                    r.target_id, r.client_id,
                );
            } else {
                let schema = self.get_schema_desc(r.target_id);
                self.send_response(
                    transport, r.fd, r.target_id,
                    r.result.as_deref(), STATUS_OK, b"",
                    r.client_id, &schema, r.lsn,
                );
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Deferred-reply helper used by spawn_deferred_select
// ---------------------------------------------------------------------------

/// Project a `FanOutError` outcome onto a plain `Result<Option<Batch>, String>`,
/// invoking `sync` only when the async path requested a sync fallback.
fn resolve_async<F>(
    async_result: Result<Option<Batch>, crate::master::FanOutError>,
    sync: F,
) -> Result<Option<Batch>, String>
where
    F: FnOnce() -> Result<Option<Batch>, String>,
{
    match async_result {
        Ok(r) => Ok(r),
        Err(crate::master::FanOutError::NeedsSyncFallback) => sync(),
        Err(crate::master::FanOutError::Worker(e)) => Err(e),
    }
}

fn build_deferred_reply(
    fd: i32,
    client_id: u64,
    target_id: i64,
    lsn: u64,
    result: Result<Option<Batch>, String>,
) -> DeferredReply {
    match result {
        Ok(opt) => DeferredReply {
            fd, client_id, target_id, lsn,
            status: STATUS_OK,
            error_msg: String::new(),
            result: opt.map(Arc::new),
        },
        Err(e) => DeferredReply {
            fd, client_id, target_id, lsn: 0,
            status: STATUS_ERROR,
            error_msg: e,
            result: None,
        },
    }
}

// ---------------------------------------------------------------------------
// Schema validation helper
// ---------------------------------------------------------------------------

fn validate_schema_match(
    wire: &SchemaDescriptor,
    expected: &SchemaDescriptor,
) -> Result<(), String> {
    if wire.num_columns != expected.num_columns {
        return Err(format!(
            "Schema mismatch: expected {} columns, got {}",
            expected.num_columns, wire.num_columns,
        ));
    }
    if wire.pk_index != expected.pk_index {
        return Err(format!(
            "Schema mismatch: expected pk_index={}, got {}",
            expected.pk_index, wire.pk_index,
        ));
    }
    for i in 0..wire.num_columns as usize {
        if wire.columns[i].type_code != expected.columns[i].type_code {
            return Err(format!(
                "Schema mismatch at column {}: expected type {}, got {}",
                i, expected.columns[i].type_code, wire.columns[i].type_code,
            ));
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::SchemaDescriptor;

    /// Build a minimal one-row U64 batch (PK-only schema).
    fn one_row_batch(pk: u64) -> Batch {
        let schema = SchemaDescriptor::minimal_u64();
        let mut b = Batch::with_schema(schema, 1);
        // minimal_u64 has a single PK column and no non-PK columns.
        unsafe { b.append_row_simple(pk as u128, 1, 0, &[], &[], &[], &[]) };
        b
    }

    fn make_entry(target_id: i64, rows: u64) -> PendingEntry {
        PendingEntry {
            fd: -1,
            client_id: rows,
            target_id,
            batch: one_row_batch(rows),
            mode: WireConflictMode::Update,
        }
    }

    // -----------------------------------------------------------------------
    // PendingBatch invariants
    // -----------------------------------------------------------------------

    /// drain_all empties the batch and zeroes row_count.
    #[test]
    fn test_pending_batch_drain_clears() {
        let mut pb = PendingBatch::new();
        pb.add(-1, 0, 1, one_row_batch(1), WireConflictMode::Update);
        pb.add(-1, 1, 1, one_row_batch(2), WireConflictMode::Update);
        assert_eq!(pb.row_count, 2);

        let drained = pb.drain_all();
        assert_eq!(drained.len(), 2);
        assert_eq!(pb.row_count, 0);
        assert!(pb.is_empty());
    }

    /// Re-inserting drained entries via add() fully restores row_count.
    /// This is the exact code path used at the pre_write_pushes call site
    /// when the checkpoint fails and entries must be returned to pending.
    #[test]
    fn test_pending_batch_drain_and_reinsert_restores_row_count() {
        let mut pb = PendingBatch::new();
        for i in 0..5u64 {
            pb.add(-1, i, 1, one_row_batch(i), WireConflictMode::Update);
        }
        let original_row_count = pb.row_count;
        assert_eq!(original_row_count, 5);

        // Simulate what happens at the call site when pre_write_pushes
        // returns Some(entries) due to checkpoint failure.
        let returned = pb.drain_all();
        assert_eq!(pb.row_count, 0, "drain_all must zero row_count");

        for e in returned {
            pb.add(e.fd, e.client_id, e.target_id, e.batch, e.mode);
        }
        assert_eq!(pb.row_count, original_row_count,
            "re-inserting drained entries must restore row_count");
        assert_eq!(pb.by_tid.values().map(|v| v.len()).sum::<usize>(), 5,
            "re-inserting drained entries must restore entry count");
    }

    /// row_count tracks batch.count, not the number of PendingEntry structs.
    #[test]
    fn test_pending_batch_row_count_tracks_batch_rows() {
        let mut pb = PendingBatch::new();
        // One entry with 1 row.
        pb.add(-1, 0, 1, one_row_batch(1), WireConflictMode::Update);
        assert_eq!(pb.row_count, 1);

        // A second entry on a different target still contributes.
        pb.add(-1, 1, 2, one_row_batch(2), WireConflictMode::Update);
        assert_eq!(pb.row_count, 2);

        let _ = pb.drain_all();
        assert_eq!(pb.row_count, 0);
    }

    /// Cross-check that make_entry produces the expected row count.
    #[test]
    fn test_pending_batch_multi_entry_round_trip() {
        let entries: Vec<PendingEntry> = (0..8).map(|i| make_entry(1, i)).collect();
        let total_rows: usize = entries.iter().map(|e| e.batch.count).sum();

        let mut pb = PendingBatch::new();
        for e in entries {
            pb.add(e.fd, e.client_id, e.target_id, e.batch, e.mode);
        }
        assert_eq!(pb.row_count, total_rows);

        // Drain and re-insert — round trip must be lossless.
        let returned = pb.drain_all();
        assert_eq!(returned.len(), 8);
        for e in returned {
            pb.add(e.fd, e.client_id, e.target_id, e.batch, e.mode);
        }
        assert_eq!(pb.row_count, total_rows);
    }

    // -----------------------------------------------------------------------
    // start_all_async_ticks filtering logic
    // -----------------------------------------------------------------------

    /// Simulate the tick_queue_tids drain-and-filter step that
    /// start_all_async_ticks performs: dropped tables are excluded while
    /// live tables are preserved in order.
    ///
    /// This exercises the filter logic in isolation without needing the full
    /// MasterDispatcher/SAL stack.  The integration path is covered by E2E
    /// tests (make e2e).
    #[test]
    fn test_start_all_async_ticks_filter_pattern() {
        // Simulate a queue with some live tids and one dropped tid.
        // In production, `cat().has_id(tid)` decides; here we use a closure.
        let queue: Vec<i64> = vec![10, 20, 30, 40];
        let live: std::collections::HashSet<i64> = [10, 30, 40].iter().copied().collect();

        // Mirrors the filter in start_all_async_ticks.
        let valid: Vec<i64> = queue.iter().copied()
            .filter(|tid| live.contains(tid))
            .collect();

        assert_eq!(valid, vec![10, 30, 40], "dropped tid 20 must be excluded");
        assert_eq!(valid.len(), 3);
    }

    /// When all tids in tick_queue_tids are dropped, start_all_async_ticks
    /// must be a no-op (valid_tids empty → no call to dispatcher).
    /// Verified structurally: the filter produces an empty vec.
    #[test]
    fn test_start_all_async_ticks_all_dropped_is_noop() {
        let queue: Vec<i64> = vec![10, 20, 30];
        let live: std::collections::HashSet<i64> = std::collections::HashSet::new();

        let valid: Vec<i64> = queue.iter().copied()
            .filter(|tid| live.contains(tid))
            .collect();

        assert!(valid.is_empty(), "all tids dropped: valid list must be empty");
    }

    // -----------------------------------------------------------------------
    // Deferred-reply construction (Stage 2)
    // -----------------------------------------------------------------------

    /// `build_deferred_reply` maps `Ok(Some(batch))` to a STATUS_OK reply
    /// with the batch wrapped in `Arc`, preserving the LSN.
    #[test]
    fn test_build_deferred_reply_ok_some() {
        let batch = one_row_batch(42);
        let r = build_deferred_reply(7, 8, 9, 100, Ok(Some(batch)));
        assert_eq!(r.fd, 7);
        assert_eq!(r.client_id, 8);
        assert_eq!(r.target_id, 9);
        assert_eq!(r.status, STATUS_OK);
        assert_eq!(r.lsn, 100);
        assert!(r.error_msg.is_empty());
        assert!(r.result.is_some());
    }

    /// `build_deferred_reply` maps `Ok(None)` to STATUS_OK with no batch
    /// (the empty-result case for SCAN / SEEK miss).
    #[test]
    fn test_build_deferred_reply_ok_none() {
        let r = build_deferred_reply(1, 2, 3, 50, Ok(None));
        assert_eq!(r.status, STATUS_OK);
        assert!(r.result.is_none());
        assert_eq!(r.lsn, 50);
    }

    /// `build_deferred_reply` maps `Err(msg)` to STATUS_ERROR with the
    /// message preserved and lsn=0 (no successful execution).
    #[test]
    fn test_build_deferred_reply_err() {
        let r = build_deferred_reply(1, 2, 3, 50, Err("worker boom".into()));
        assert_eq!(r.status, STATUS_ERROR);
        assert_eq!(r.error_msg, "worker boom");
        assert_eq!(r.lsn, 0, "error reply must reset LSN to 0");
        assert!(r.result.is_none());
    }
}
