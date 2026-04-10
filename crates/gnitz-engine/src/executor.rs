//! Server executor: client-facing event loop that dispatches DML/DDL,
//! manages tick coalescing, and coordinates multi-worker fan-out.
//!
//! Replaces gnitz/server/executor.py (955 LOC). The entire event loop
//! runs in Rust — no FFI round-trips on the request path.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use gnitz_transport::transport::{OutputSlots, Transport};
use gnitz_transport::uring::IoUringRing;

use crate::catalog::{CatalogEngine, FIRST_USER_TABLE_ID, SEQ_ID_SCHEMAS, SEQ_ID_TABLES, SEQ_ID_INDICES};
use crate::schema::SchemaDescriptor;
use crate::ipc::{self, DecodedWire, STATUS_OK, STATUS_ERROR, W2M_HEADER_SIZE};
use crate::master::MasterDispatcher;
use crate::storage::OwnedBatch;

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
// Deferred request (scan/seek blocked during async tick)
// ---------------------------------------------------------------------------

struct DeferredRequest {
    fd: i32,
    client_id: u64,
    target_id: i64,
    flags: u64,
    seek_pk_lo: u64,
    seek_pk_hi: u64,
    seek_col_idx: u64,
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
    batch: OwnedBatch,
}

struct PendingBatch {
    entries: Vec<PendingEntry>,
    row_count: usize,
}

impl PendingBatch {
    fn new() -> Self {
        PendingBatch {
            entries: Vec::with_capacity(16),
            row_count: 0,
        }
    }

    fn add(&mut self, fd: i32, client_id: u64, target_id: i64, batch: OwnedBatch) {
        self.row_count += batch.count;
        self.entries.push(PendingEntry { fd, client_id, target_id, batch });
    }

    fn is_empty(&self) -> bool {
        self.entries.is_empty()
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
    deferred_requests: Vec<DeferredRequest>,

    // DDL response batching (deferred until end-of-cycle fdatasync)
    pending_ddl_responses: Vec<PendingDdlResponse>,

    // Schema descriptor cache (table IDs are monotonic, never reused)
    schema_cache: HashMap<i64, SchemaDescriptor>,

    // Async fdatasync: overlaps fsync with worker ACK collection
    async_fsync: ipc::AsyncFsync,
}

// Safety: ServerExecutor is single-threaded (master process event loop).
unsafe impl Send for ServerExecutor {}

impl ServerExecutor {
    pub fn new(
        catalog: *mut CatalogEngine,
        dispatcher: *mut MasterDispatcher,
        sal_fd: i32,
    ) -> Self {
        let async_fsync = ipc::AsyncFsync::new(sal_fd)
            .expect("Failed to create AsyncFsync io_uring");
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
            deferred_requests: Vec::with_capacity(8),
            pending_ddl_responses: Vec::with_capacity(8),
            schema_cache: HashMap::new(),
            async_fsync,
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

    // -- Data operations (direct catalog calls, no FFI) -----------------------

    fn handle_push(
        &mut self, target_id: i64, in_batch: Option<Box<OwnedBatch>>,
    ) -> Result<(Option<Arc<OwnedBatch>>, u64), String> {
        if target_id >= FIRST_USER_TABLE_ID {
            if let Some(batch) = in_batch {
                if batch.count > 0 {
                    self.cat().validate_unique_indices(target_id, &batch)?;
                    self.disp().validate_all_distributed(target_id, &batch)?;
                    self.disp().fan_out_push(target_id, &batch)?;
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
                self.cat().ingest_to_family(target_id, &*batch)?;
                let dag = self.cat().get_dag_ptr();
                unsafe { &mut *dag }.evaluate_dag(target_id, *batch);
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


    fn seek_family(&mut self, target_id: i64, pk_lo: u64, pk_hi: u64) -> Option<OwnedBatch> {
        match self.cat().seek_family(target_id, pk_lo, pk_hi) {
            Ok(opt) => opt,
            Err(_) => None,
        }
    }

    fn seek_by_index(
        &mut self, target_id: i64, col_idx: u32, key_lo: u64, key_hi: u64,
    ) -> Option<OwnedBatch> {
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
        result: Option<&OwnedBatch>,
        status: u32,
        error_msg: &[u8],
        client_id: u64,
        schema: &SchemaDescriptor,
        seek_pk_lo: u64,
    ) {
        let col_names_owned;
        let col_name_refs: Vec<&[u8]>;
        let col_names_opt;

        if status == STATUS_OK {
            col_names_owned = self.cat().get_column_names(target_id);
            col_name_refs = col_names_owned.iter().map(|s| s.as_bytes()).collect();
            col_names_opt = Some(col_name_refs.as_slice());
        } else {
            col_names_opt = None;
        }

        let sz = ipc::wire_size(status, error_msg, Some(schema), col_names_opt, result);
        transport.send_encoded(fd, sz, |buf| {
            ipc::encode_wire_into(
                buf, 0,
                target_id as u64, client_id,
                0, seek_pk_lo, 0, 0,
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
                0, 0, 0, 0,
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
                0, 0, 0, 0,
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
        if pending.is_empty() || !pending.entries.iter().any(|e| e.target_id == target_id) {
            return;
        }

        let mut flush_entries: Vec<PendingEntry> = Vec::new();
        let mut keep_entries: Vec<PendingEntry> = Vec::new();

        for entry in pending.entries.drain(..) {
            if entry.target_id == target_id {
                flush_entries.push(entry);
            } else {
                keep_entries.push(entry);
            }
        }

        // Recompute row_count from kept entries
        pending.row_count = 0;
        for e in &keep_entries {
            pending.row_count += e.batch.count;
        }
        pending.entries = keep_entries;

        if pending.is_empty() {
            self.t_first_pending = None;
        }

        if !flush_entries.is_empty() {
            self.flush_pending_pushes(transport, flush_entries);
        }
    }

    fn flush_pending_pushes(
        &mut self,
        transport: &mut Transport<IoUringRing>,
        mut entries: Vec<PendingEntry>,
    ) {
        if entries.is_empty() {
            return;
        }

        entries.sort_by_key(|e| e.target_id);

        // Phase 0: checkpoint before writing any groups.
        {
            let disp = self.disp();
            if let Err(e) = disp.maybe_checkpoint() {
                self.send_batch_errors(transport, &entries, 0, entries.len(), &e);
                return;
            }
        }

        // Phase 1: merge batches per target_id, write each group to SAL (no sync).
        let n = entries.len();
        let mut groups: Vec<GroupInfo> = Vec::new();
        let mut run_start = 0;

        while run_start < n {
            let target_id = entries[run_start].target_id;
            let mut run_end = run_start + 1;
            while run_end < n && entries[run_end].target_id == target_id {
                run_end += 1;
            }

            let merged_count;
            let merged = {
                let schema = self.get_schema_desc(target_id);
                let first = &entries[run_start];
                let ncols = first.batch.num_payload_cols();
                let mut m = OwnedBatch::empty(ncols);
                m.schema = Some(schema);
                m.append_batch(&first.batch, 0, first.batch.count);
                for k in (run_start + 1)..run_end {
                    let b = &entries[k].batch;
                    m.append_batch(b, 0, b.count);
                }
                merged_count = m.count;
                m
            };

            let error = {
                let disp = self.disp();
                disp.write_ingest(target_id, &merged).err()
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
            // Signal workers immediately, submit async fdatasync in parallel.
            {
                let disp = unsafe { &mut *self.dispatcher };
                disp.signal_and_submit_fsync(&mut self.async_fsync);
            }

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

            // Wait for async fdatasync to complete before ACKing clients.
            let fsync_rc = self.async_fsync.wait_complete();
            if fsync_rc < 0 {
                let err = format!("fdatasync failed: {}", fsync_rc);
                for g in groups.iter_mut() {
                    if g.error.is_none() {
                        g.error = Some(err.clone());
                    }
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
                            0, 0, 0, 0,
                            STATUS_ERROR, err_msg.as_bytes(),
                            Some(&schema), None, None,
                        )
                    })
                } else {
                    transport.send_encoded(e.fd, sz, |buf| {
                        ipc::encode_wire_into(
                            buf, 0,
                            g.target_id as u64, e.client_id,
                            0, g.current_lsn, 0, 0,
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
                    0, 0, 0, 0,
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

        // Move pending ticks to the async queue
        self.tick_queue_tids = std::mem::replace(&mut self.tick_tids, Vec::with_capacity(8));
        self.tick_rows.clear();
        self.t_last_push = None;

        self.start_next_async_tick();
    }

    fn start_next_async_tick(&mut self) {
        if self.tick_queue_tids.is_empty() {
            return;
        }
        let tid = self.tick_queue_tids.remove(0);

        if !self.cat().has_id(tid) {
            // Table dropped — skip to next
            if !self.tick_queue_tids.is_empty() {
                self.start_next_async_tick();
            }
            return;
        }

        self.tick_state = TickState::Active;
        let disp = self.disp();
        match disp.start_tick_async(tid) {
            Ok(()) => {}
            Err(e) => {
                gnitz_warn!("start_tick_async error tid={}: {}", tid, e);
                self.tick_state = TickState::Idle;
                if !self.tick_queue_tids.is_empty() {
                    self.start_next_async_tick();
                }
            }
        }
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
                    true
                }
            }
        };

        if !done {
            return;
        }

        gnitz_debug!("async tick complete");
        self.last_tick_lsn = self.ingest_lsn;

        // More tids queued?
        if !self.tick_queue_tids.is_empty() {
            self.start_next_async_tick();
            return;
        }

        // All ticks complete — transition to IDLE
        self.tick_state = TickState::Idle;

        // Flush pushes accumulated during the tick
        if !pending.is_empty() {
            let entries = std::mem::take(&mut pending.entries);
            pending.row_count = 0;
            self.flush_pending_pushes(transport, entries);
        }

        // Process deferred scan/seek requests
        self.process_deferred_requests(transport);
    }

    fn process_deferred_requests(&mut self, transport: &mut Transport<IoUringRing>) {
        let deferred = std::mem::replace(
            &mut self.deferred_requests, Vec::with_capacity(8));

        for req in deferred {
            let _: Result<(), ()> = (|| {
                if req.flags & FLAG_SEEK_BY_INDEX != 0 {
                    let schema = self.get_schema_desc(req.target_id);
                    let result = if req.target_id >= FIRST_USER_TABLE_ID {
                        let disp = self.disp();
                        disp.fan_out_seek_by_index(
                            req.target_id, req.seek_col_idx as u32,
                            req.seek_pk_lo, req.seek_pk_hi,
                        ).map_err(|e| {
                            self.send_error_response(
                                transport, req.fd, e.as_bytes(), req.target_id, req.client_id);
                        })?
                    } else {
                        self.seek_by_index(
                            req.target_id, req.seek_col_idx as u32,
                            req.seek_pk_lo, req.seek_pk_hi,
                        )
                    };
                    self.send_response(
                        transport, req.fd, req.target_id,
                        result.as_ref(), STATUS_OK, b"",
                        req.client_id, &schema, 0,
                    );
                } else if req.flags & FLAG_SEEK != 0 {
                    let schema = self.get_schema_desc(req.target_id);
                    let result = if req.target_id >= FIRST_USER_TABLE_ID {
                        let disp = self.disp();
                        disp.fan_out_seek(
                            req.target_id, req.seek_pk_lo, req.seek_pk_hi,
                        ).map_err(|e| {
                            self.send_error_response(
                                transport, req.fd, e.as_bytes(), req.target_id, req.client_id);
                        })?
                    } else {
                        self.seek_family(req.target_id, req.seek_pk_lo, req.seek_pk_hi)
                    };
                    self.send_response(
                        transport, req.fd, req.target_id,
                        result.as_ref(), STATUS_OK, b"",
                        req.client_id, &schema, 0,
                    );
                } else {
                    // Deferred scan (empty-batch push)
                    let schema = self.get_schema_desc(req.target_id);
                    let lsn = self.last_tick_lsn;
                    let disp = self.disp();
                    let result = disp.fan_out_scan(req.target_id).map_err(|e| {
                        self.send_error_response(
                            transport, req.fd, e.as_bytes(), req.target_id, req.client_id);
                    })?.map(Arc::new);
                    self.send_response(
                        transport, req.fd, req.target_id,
                        result.as_deref(), STATUS_OK, b"",
                        req.client_id, &schema, lsn,
                    );
                }
                Ok(())
            })();

            // Error (if any) already sent via send_error_response in map_err above.
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
        let decoded = match ipc::decode_wire(data) {
            Ok(d) => d,
            Err(e) => {
                let msg = format!("decode error: {}", e);
                self.send_error_response(transport, fd, msg.as_bytes(), 0, 0);
                return;
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
                0, 0, 0, 0,
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

        // During TICK_ACTIVE, defer reads that need consistent view state
        if self.tick_state == TickState::Active {
            if flags & FLAG_SEEK_BY_INDEX != 0 {
                self.deferred_requests.push(DeferredRequest {
                    fd, client_id, target_id, flags,
                    seek_pk_lo: decoded.control.seek_pk_lo,
                    seek_pk_hi: decoded.control.seek_pk_hi,
                    seek_col_idx: decoded.control.seek_col_idx,
                });
                return Ok(());
            }
            if flags & FLAG_SEEK != 0 {
                self.deferred_requests.push(DeferredRequest {
                    fd, client_id, target_id, flags,
                    seek_pk_lo: decoded.control.seek_pk_lo,
                    seek_pk_hi: decoded.control.seek_pk_hi,
                    seek_col_idx: 0,
                });
                return Ok(());
            }
            // Scans (empty-batch push to user table)
            if target_id >= FIRST_USER_TABLE_ID
                && decoded.data_batch.as_ref().map_or(true, |b| b.count == 0)
            {
                self.deferred_requests.push(DeferredRequest {
                    fd, client_id, target_id, flags: 0,
                    seek_pk_lo: 0, seek_pk_hi: 0, seek_col_idx: 0,
                });
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
            // The pre-flush exists solely to give distributed validation
            // fresh worker state. Skip it when no distributed validator
            // would actually broadcast — all three validators below
            // early-return on unconstrained tables.
            let needs_flush = {
                let cat = self.cat();
                cat.get_fk_count(target_id) > 0
                    || cat.get_fk_children_count(target_id) > 0
                    || cat.has_any_unique_index(target_id)
            };
            if needs_flush {
                self.flush_pending_for_tid(transport, target_id, pending);
            }

            let batch = decoded.data_batch.unwrap();
            self.cat().validate_unique_indices(target_id, &batch)?;
            {
                let disp = self.disp();
                disp.validate_all_distributed(target_id, &batch)?;
            }
            pending.add(fd, client_id, target_id, *batch);
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

        let (result, lsn) = self.handle_push(target_id, decoded.data_batch)?;

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
        self.disp().sync();
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
            let timeout_ms = if self.tick_state == TickState::Active {
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

                if pending.row_count >= MAX_PENDING_ROWS && self.tick_state != TickState::Active {
                    self.t_first_pending = None;
                    let entries = std::mem::take(&mut pending.entries);
                    pending.row_count = 0;
                    self.flush_pending_pushes(transport, entries);
                }
            }

            if !pending.is_empty() && self.tick_state != TickState::Active {
                let deadline_expired = self.t_first_pending
                    .map_or(false, |t| t.elapsed().as_millis() as u64 >= FLUSH_DEADLINE_MS);
                if pending.row_count >= MAX_PENDING_ROWS || deadline_expired {
                    self.t_first_pending = None;
                    let entries = std::mem::take(&mut pending.entries);
                    pending.row_count = 0;
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
