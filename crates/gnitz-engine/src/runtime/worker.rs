//! Worker process event loop.
//!
//! Owns a subset of partitions for every user table. Receives requests from
//! the master via the SAL (shared append-only log), sends responses via a
//! per-worker W2M shared region.

use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;

use crate::catalog::{CatalogEngine, FIRST_USER_TABLE_ID};
use crate::schema::SchemaDescriptor;
use crate::dag::ExchangeCallback;
use crate::storage::partition_for_key;
use crate::ops::worker_for_partition;
use crate::runtime::wire::{self as ipc, STATUS_OK, STATUS_ERROR};
use crate::runtime::sal::{
    SAL_MMAP_SIZE,
    FLAG_SHUTDOWN, FLAG_DDL_SYNC, FLAG_EXCHANGE, FLAG_EXCHANGE_RELAY, FLAG_PUSH, FLAG_HAS_PK,
    FLAG_SEEK, FLAG_SEEK_BY_INDEX, FLAG_PRELOADED_EXCHANGE, FLAG_BACKFILL,
    FLAG_TICK, FLAG_CHECKPOINT, FLAG_FLUSH,
    SalReader, SalMessage,
};
use crate::runtime::w2m::W2mWriter;
use crate::storage::Batch;

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
    fn stash_preloaded(&mut self, view_id: i64, batch: Batch) {
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
    num_workers: usize,
    master_pid: i32,
    catalog: *mut CatalogEngine,
    sal_reader: SalReader,
    w2m_writer: W2mWriter,
    exchange: WorkerExchangeHandler,
    pending_deltas: HashMap<i64, Batch>,
    read_cursor: u64,
    expected_epoch: u32,
    // Per-target schema cache: skip the 512-byte schema block parse on hot paths.
    // Populated lazily from decoded.schema on first non-DDL message per target.
    schema_cache: HashMap<i64, SchemaDescriptor>,
    // Per-target encoded schema wire block (no col_names on the worker path).
    // Keyed by target_id; value = (num_columns, pk_index, block) so a cached
    // table block is not reused when send_response is called with an index
    // schema for the same target_id (FLAG_HAS_PK UniqueIndex path). On
    // fingerprint mismatch the block is built inline without evicting the cache.
    schema_wire_block_cache: HashMap<i64, (u32, u32, Rc<Vec<u8>>)>,
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
            read_cursor: 0,
            expected_epoch: 1,
            schema_cache: HashMap::new(),
            schema_wire_block_cache: HashMap::new(),
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

            if self.drain_sal() {
                return 0;
            }
        }
    }

    /// Process all pending SAL message groups. Returns true on shutdown.
    fn drain_sal(&mut self) -> bool {
        loop {
            if self.read_cursor + 8 >= SAL_MMAP_SIZE as u64 {
                break;
            }
            // Read into local to avoid borrow conflict with dispatch_message
            let read_result = self.sal_reader.try_read(self.read_cursor);
            let (msg, new_cursor) = match read_result {
                Some(v) => v,
                None => break,
            };
            if msg.epoch != self.expected_epoch {
                break;
            }
            self.read_cursor = new_cursor;

            if self.dispatch_message(&msg) {
                return true;
            }
        }
        false
    }

    /// Dispatch a single SAL message. Returns true on shutdown.
    fn dispatch_message(&mut self, msg: &SalMessage<'_>) -> bool {
        let mut flags = msg.flags;
        let mut target_id = msg.target_id as i64;
        let mut wire_data = msg.wire_data;
        if wire_data.is_none() {
            // No data for this worker — skip unless broadcast flags
            if flags & (FLAG_SHUTDOWN | FLAG_FLUSH | FLAG_DDL_SYNC | FLAG_TICK
                | FLAG_PUSH | FLAG_BACKFILL | FLAG_HAS_PK
                | FLAG_PRELOADED_EXCHANGE) == 0 {
                return false;
            }
        }

        // Drain preloaded exchange groups before processing push
        while flags & FLAG_PRELOADED_EXCHANGE != 0 {
            let vid = target_id;
            if let Some(data) = wire_data {
                if let Ok(decoded) = ipc::decode_wire(data) {
                    if let Some(batch) = decoded.data_batch {
                        self.exchange.stash_preloaded(vid, batch);
                    } else if let Some(schema) = decoded.schema {
                        self.exchange.stash_preloaded(vid, Batch::with_schema(schema, 0));
                    }
                }
            }
            if self.read_cursor + 8 >= SAL_MMAP_SIZE as u64 {
                return false;
            }
            let (next, new_cursor) = match self.sal_reader.try_read(self.read_cursor) {
                Some(v) => v,
                None => return false,
            };
            // Honour the epoch fence: a new epoch starts after FLAG_FLUSH
            // resets the write cursor. Consuming a post-epoch message here
            // would silently treat it as preloaded exchange data.
            if next.epoch != self.expected_epoch {
                return false;
            }
            self.read_cursor = new_cursor;
            flags = next.flags;
            target_id = next.target_id as i64;
            wire_data = next.wire_data;
        }

        // Decode wire data if present. For DDL_SYNC the payload carries catalog
        // schema (not the target table's data schema), so always do a full decode.
        // For FLAG_HAS_PK (unique/FK checks) the schema block is the index or
        // parent-table schema, which can differ from the cached user-table schema
        // for the same target_id — always do a full decode for these too.
        // For all other messages, skip schema-block parsing when we have a cached
        // descriptor — decode_wire_with_schema reads only the block size to advance
        // the offset.
        let decoded = if let Some(data) = wire_data {
            if flags & (FLAG_DDL_SYNC | FLAG_HAS_PK) == 0 {
                if let Some(&schema) = self.schema_cache.get(&target_id) {
                    ipc::decode_wire_with_schema(data, &schema).ok()
                } else {
                    let d = ipc::decode_wire(data).ok();
                    if let Some(ref d) = d {
                        if let Some(s) = d.schema {
                            self.schema_cache.insert(target_id, s);
                            // Invalidate the prebuilt wire block so the next
                            // send_response rebuilds it from the new schema.
                            self.schema_wire_block_cache.remove(&target_id);
                        }
                    }
                    d
                }
            } else {
                ipc::decode_wire(data).ok()
            }
        } else {
            None
        };

        // Captured before `decoded` moves into dispatch_inner so the reply
        // helpers can echo it back.
        let request_id = decoded.as_ref().map(|d| d.control.request_id).unwrap_or(0);

        let result = self.dispatch_inner(flags, target_id, decoded, request_id);
        match result {
            DispatchResult::Continue => {
                // Replay ticks deferred during any exchange wait now that the
                // outer tick's ACK has been sent. Pushes are handled inline in
                // `do_exchange_wait` (safe because user-table push only
                // appends to `pending_deltas`) so we don't defer them.
                self.replay_deferred_ticks();
                false
            }
            DispatchResult::Shutdown => true,
            DispatchResult::Error(msg) => {
                self.send_error(&msg, request_id);
                false
            }
        }
    }

    fn dispatch_inner(
        &mut self,
        flags: u32,
        target_id: i64,
        decoded: Option<ipc::DecodedWire>,
        request_id: u64,
    ) -> DispatchResult {
        // Extract control fields before consuming decoded
        let seek_pk_lo = decoded.as_ref().map(|d| d.control.seek_pk_lo).unwrap_or(0);
        let seek_pk_hi = decoded.as_ref().map(|d| d.control.seek_pk_hi).unwrap_or(0);
        let seek_col_idx = decoded.as_ref().map(|d| d.control.seek_col_idx).unwrap_or(0);

        // Extract batch (consumes decoded)
        let batch = decoded.and_then(|d| d.data_batch);

        if flags & FLAG_SHUTDOWN != 0 {
            self.shutdown();
            return DispatchResult::Shutdown;
        }

        if flags & FLAG_FLUSH != 0 {
            self.read_cursor = 0;
            self.expected_epoch += 1;
            match self.handle_flush_all() {
                Ok(()) => self.send_ack(0, FLAG_CHECKPOINT, request_id),
                Err(msg) => self.send_error(&msg, request_id),
            }
            return DispatchResult::Continue;
        }

        if flags & FLAG_DDL_SYNC != 0 {
            if let Some(batch) = batch {
                if batch.count > 0 {
                    if let Err(msg) = self.cat().ddl_sync(target_id, batch) {
                        return DispatchResult::Error(msg);
                    }
                    gnitz_debug!("W{} ddl_sync tid={}", self.worker_id, target_id);
                }
            }
            return DispatchResult::Continue;
        }

        if flags & FLAG_BACKFILL != 0 {
            match self.handle_backfill(target_id, request_id) {
                Ok(()) => {}
                Err(msg) => return DispatchResult::Error(msg),
            }
            self.send_ack(target_id as u64, 0, request_id);
            return DispatchResult::Continue;
        }

        if flags & FLAG_HAS_PK != 0 {
            let lookup = HasPkLookup::from_wire(seek_col_idx);
            match self.handle_has_pk(target_id, batch, lookup, request_id) {
                Ok(()) => {}
                Err(msg) => return DispatchResult::Error(msg),
            }
            return DispatchResult::Continue;
        }

        if flags & FLAG_PUSH != 0 {
            if let Some(batch) = batch {
                if batch.count > 0 {
                    match self.handle_push(target_id, batch, request_id) {
                        Ok(()) => {}
                        Err(msg) => return DispatchResult::Error(msg),
                    }
                }
            }
            self.send_ack(target_id as u64, 0, request_id);
            return DispatchResult::Continue;
        }

        if flags & FLAG_TICK != 0 {
            match self.handle_tick(target_id, request_id) {
                Ok(()) => {}
                Err(msg) => return DispatchResult::Error(msg),
            }
            self.send_ack(target_id as u64, 0, request_id);
            return DispatchResult::Continue;
        }

        if flags & FLAG_SEEK_BY_INDEX != 0 {
            let col_idx = seek_col_idx as u32;
            match self.cat().seek_by_index(target_id, col_idx, seek_pk_lo, seek_pk_hi) {
                Ok(result) => {
                    let schema = self.cat().get_schema_desc(target_id);
                    self.send_response(target_id as u64, result.as_ref(), schema.as_ref(), request_id);
                }
                Err(msg) => return DispatchResult::Error(msg),
            }
            return DispatchResult::Continue;
        }

        if flags & FLAG_SEEK != 0 {
            match self.cat().seek_family(target_id, seek_pk_lo, seek_pk_hi) {
                Ok(result) => {
                    let schema = self.cat().get_schema_desc(target_id);
                    self.send_response(target_id as u64, result.as_ref(), schema.as_ref(), request_id);
                }
                Err(msg) => return DispatchResult::Error(msg),
            }
            return DispatchResult::Continue;
        }

        // Default: scan
        match self.cat().scan_family(target_id) {
            Ok(result) => {
                let schema = self.cat().get_schema_desc(target_id);
                self.send_response(target_id as u64, Some(&*result), schema.as_ref(), request_id);
            }
            Err(msg) => return DispatchResult::Error(msg),
        }
        DispatchResult::Continue
    }

    // ── W2M response helpers ───────────────────────────────────────────

    fn send_ack(&self, target_id: u64, flags: u32, request_id: u64) {
        let sz = ipc::wire_size(STATUS_OK, &[], None, None, None, None);
        self.w2m_writer.send_encoded(sz, |buf| {
            ipc::encode_wire_into(
                buf, 0, target_id, 0, flags as u64,
                0, 0, 0, request_id, STATUS_OK, &[], None, None, None, None,
            );
        });
    }

    fn send_response(
        &mut self,
        target_id: u64,
        result: Option<&Batch>,
        schema: Option<&SchemaDescriptor>,
        request_id: u64,
    ) {
        let tid_key = target_id as i64;
        // Rc::clone releases the mutable borrow on schema_wire_block_cache
        // before w2m_writer is accessed below. On fingerprint mismatch (index
        // schema vs cached table schema for the same target_id) build inline
        // without overwriting — the cached table block remains valid for SEEK/SCAN.
        let prebuilt_rc: Option<Rc<Vec<u8>>> = schema.map(|s| {
            let nc = s.num_columns;
            let pi = s.pk_index;
            if let Some(entry) = self.schema_wire_block_cache.get(&tid_key) {
                if entry.0 == nc && entry.1 == pi {
                    return Rc::clone(&entry.2);
                }
                return Rc::new(ipc::build_schema_wire_block(s, &[], target_id as u32));
            }
            let block = Rc::new(ipc::build_schema_wire_block(s, &[], target_id as u32));
            self.schema_wire_block_cache.insert(tid_key, (nc, pi, block.clone()));
            block
        });
        let prebuilt: Option<&[u8]> = prebuilt_rc.as_deref().map(|v| v.as_slice());
        let sz = ipc::wire_size(STATUS_OK, &[], schema, None, result, prebuilt);
        self.w2m_writer.send_encoded(sz, |buf| {
            ipc::encode_wire_into(
                buf, 0, target_id, 0, 0,
                0, 0, 0, request_id, STATUS_OK, &[],
                schema, None, result, prebuilt,
            );
        });
    }

    fn send_error(&self, error_msg: &str, request_id: u64) {
        let msg = error_msg.as_bytes();
        let sz = ipc::wire_size(STATUS_ERROR, msg, None, None, None, None);
        self.w2m_writer.send_encoded(sz, |buf| {
            ipc::encode_wire_into(
                buf, 0, 0, 0, 0,
                0, 0, 0, request_id, STATUS_ERROR, msg, None, None, None, None,
            );
        });
    }

    // ── Request handlers ───────────────────────────────────────────────

    /// Flush a family to disk, logging on failure without propagating — the
    /// data remains safe in the memtable until the next checkpoint flush.
    fn flush_family_best_effort(&mut self, target_id: i64) {
        if let Err(rc) = self.cat().flush_family(target_id) {
            gnitz_info!("W{} flush_family tid={} rc={} (data in memtable)", self.worker_id, target_id, rc);
        }
    }

    fn handle_push(
        &mut self, target_id: i64, batch: Batch, request_id: u64,
    ) -> Result<(), String> {
        // Filter to only rows belonging to this worker's partitions.
        let batch = self.filter_my_partition(batch);
        if batch.count == 0 {
            return Ok(());
        }
        let row_count = batch.count;
        if target_id < FIRST_USER_TABLE_ID {
            self.cat().ingest_to_family(target_id, &batch)?;
            self.flush_family_best_effort(target_id);
            self.evaluate_dag(target_id, batch, request_id);
        } else {
            let effective = self.cat().ingest_returning_effective(target_id, batch)?;
            self.flush_family_best_effort(target_id);
            if let Some(existing) = self.pending_deltas.get_mut(&target_id) {
                existing.append_batch(&effective, 0, effective.count);
            } else {
                self.pending_deltas.insert(target_id, effective);
            }
        }
        gnitz_debug!("W{} push tid={} rows={}", self.worker_id, target_id, row_count);
        Ok(())
    }

    /// Filter a broadcast batch to only rows belonging to this worker's partitions.
    fn filter_my_partition(&self, batch: Batch) -> Batch {
        let schema = match batch.schema {
            Some(s) => s,
            None => return batch,
        };
        let n = batch.count;
        if n == 0 {
            return batch;
        }

        // Single pass: collect matching row indices.
        let wid = self.worker_id as usize;
        let nw = self.num_workers;
        let mut indices: Vec<u32> = Vec::new();
        for i in 0..n {
            if worker_for_partition(partition_for_key(batch.get_pk(i)), nw) == wid {
                indices.push(i as u32);
            }
        }
        if indices.len() == n {
            return batch;
        }
        if indices.is_empty() {
            return Batch::with_schema(schema, 0);
        }

        // Vectorized scatter: one bulk copy per column, no per-row overhead.
        let mb = batch.as_mem_batch();
        Batch::from_indexed_rows(&mb, &indices, &schema)
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
        let owned = Arc::try_unwrap(local_batch).unwrap_or_else(|a| (*a).clone());
        self.evaluate_dag(source_tid, owned, request_id);
        Ok(())
    }

    fn handle_has_pk(
        &mut self,
        target_id: i64,
        batch: Option<Batch>,
        lookup: HasPkLookup,
        request_id: u64,
    ) -> Result<(), String> {
        let n = batch.as_ref().map(|b| b.count).unwrap_or(0);

        match lookup {
            HasPkLookup::UniqueIndex(col_idx) => {
                let index_handle = self.cat().get_index_store_handle(target_id, col_idx);
                if index_handle.is_null() {
                    return Err(format!(
                        "No unique index on column {} for table {}", col_idx, target_id
                    ));
                }
                let schema = batch.as_ref()
                    .and_then(|b| b.schema)
                    .or_else(|| self.cat().get_schema_desc(target_id))
                    .ok_or_else(|| format!("no schema for tid={}", target_id))?;
                let mut result = Batch::with_schema(schema, n);
                if let Some(ref b) = batch {
                    let table = unsafe { &mut *index_handle };
                    for i in 0..n {
                        let exists = table.has_pk(b.get_pk(i));
                        result.append_row_from_batch(b, i, if exists { 1 } else { 0 });
                    }
                }
                self.send_response(target_id as u64, Some(&result), Some(&schema), request_id);
            }
            HasPkLookup::PrimaryKey => {
                let schema = self.cat().get_schema_desc(target_id)
                    .ok_or_else(|| format!("no schema for tid={}", target_id))?;
                let ptable_handle = self.cat().get_ptable_handle(target_id);
                let mut result = Batch::with_schema(schema, n);
                if let Some(ref b) = batch {
                    for i in 0..n {
                        let exists = if let Some(pt_ptr) = ptable_handle {
                            unsafe { &mut *pt_ptr }.has_pk(b.get_pk(i))
                        } else {
                            false
                        };
                        result.append_row_from_batch(b, i, if exists { 1 } else { 0 });
                    }
                }
                self.send_response(target_id as u64, Some(&result), Some(&schema), request_id);
            }
        }
        Ok(())
    }

    fn handle_flush_all(&mut self) -> Result<(), String> {
        self.pending_deltas.clear();
        let ids = self.cat().iter_user_table_ids();
        for tid in ids {
            self.cat()
                .flush_family(tid)
                .map_err(|e| format!("flush_family tid={} rc={}", tid, e))?;
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
        let sz = ipc::wire_size(STATUS_OK, &[], schema.as_ref(), None, Some(batch), None);
        self.w2m_writer.send_encoded(sz, |buf| {
            ipc::encode_wire_into(
                buf, 0, view_id as u64, 0, FLAG_EXCHANGE as u64,
                source_id as u64, 0, 0, tick_request_id, STATUS_OK, &[],
                schema.as_ref(), None, Some(batch), None,
            );
        });

        let master_pid = self.master_pid;
        let want_key = (view_id, source_id);

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

                // Re-read `self.expected_epoch` on every iteration: a
                // FLAG_FLUSH dispatched inline below bumps it, and a stale
                // snapshot would cause every post-checkpoint message to
                // fail the epoch check forever (spin at unchanged cursor).
                let (msg_flags, msg_target, msg_wire) = {
                    let (msg, new_cursor) = match self.sal_reader.try_read(self.read_cursor) {
                        Some(v) => v,
                        None => break, // no more entries — back to outer wait
                    };
                    if msg.epoch != self.expected_epoch {
                        break;
                    }
                    self.read_cursor = new_cursor;
                    (msg.flags, msg.target_id as i64, msg.wire_data)
                };

                if msg_flags == FLAG_EXCHANGE_RELAY {
                    // source_id is echoed back via seek_pk_lo (see
                    // master::relay_exchange). Decode once, before
                    // taking the batch out.
                    let decoded = msg_wire.and_then(|d| ipc::decode_wire(d).ok());
                    let relay_source_id = decoded.as_ref()
                        .map(|d| d.control.seek_pk_lo as i64)
                        .unwrap_or(0);
                    let relay_batch = decoded
                        .and_then(|d| d.data_batch)
                        .unwrap_or_else(|| {
                            let empty_schema = schema.unwrap_or(SchemaDescriptor::default());
                            Batch::with_schema(empty_schema, 0)
                        });
                    let relay_key = (msg_target, relay_source_id);
                    if relay_key == want_key {
                        return relay_batch;
                    }
                    self.exchange.pending_relays.insert(relay_key, relay_batch);
                    continue;
                }

                if msg_flags & FLAG_DDL_SYNC != 0 {
                    if let Some(data) = msg_wire {
                        if let Ok(decoded) = ipc::decode_wire(data) {
                            if let Some(batch) = decoded.data_batch {
                                self.exchange.deferred.push(DeferredDdl {
                                    target_id: msg_target,
                                    batch,
                                });
                            }
                        }
                    }
                    continue;
                }

                // FLAG_TICK triggers evaluate_dag which can open more exchanges;
                // running it inline would re-enter the same `view_id` with a
                // different source and produce schema-mismatched relays.
                // Defer + replay after the outer tick ACK (see
                // `replay_deferred_ticks` in `dispatch_message`).
                if msg_flags & FLAG_TICK != 0 {
                    let tick_req_id = msg_wire
                        .and_then(|d| ipc::decode_wire(d).ok())
                        .map(|d| d.control.request_id)
                        .unwrap_or(0);
                    self.exchange.deferred_ticks.push((msg_target, tick_req_id));
                    continue;
                }

                // FLAG_PUSH for user tables only appends to `pending_deltas`
                // (no DAG eval, no exchange), so it's safe to handle inline —
                // and it must be, because the master's committer holds
                // `sal_writer_excl` while awaiting the push ACK, which would
                // deadlock against the relay_loop that needs the same mutex
                // to publish this exchange's relay.
                if msg_flags & FLAG_PUSH != 0 {
                    let decoded = msg_wire.and_then(|d| ipc::decode_wire(d).ok());
                    let req_id = decoded.as_ref().map(|d| d.control.request_id).unwrap_or(0);
                    let push_batch = decoded.and_then(|d| d.data_batch);
                    let push_err = if let Some(b) = push_batch {
                        if b.count > 0 {
                            self.handle_push(msg_target, b, req_id).err()
                        } else { None }
                    } else { None };
                    if let Some(e) = push_err {
                        self.send_error(&e, req_id);
                    } else {
                        self.send_ack(msg_target as u64, 0, req_id);
                    }
                    continue;
                }

                if msg_flags & FLAG_PRELOADED_EXCHANGE != 0 {
                    if let Some(data) = msg_wire {
                        if let Ok(decoded) = ipc::decode_wire(data) {
                            if let Some(batch) = decoded.data_batch {
                                self.exchange.stash_preloaded(msg_target, batch);
                            } else if let Some(s) = decoded.schema {
                                self.exchange.stash_preloaded(
                                    msg_target,
                                    Batch::with_schema(s, 0),
                                );
                            }
                        }
                    }
                    continue;
                }

                // Read-only / control messages (SEEK, SEEK_BY_INDEX, HAS_PK,
                // BACKFILL, FLUSH, SHUTDOWN) respond inline so clients don't
                // block behind the current tick. Delegate to `dispatch_inner`
                // so the same handlers as the main loop run.
                //
                // Skip unicast messages not for this worker (matches the
                // filter in `dispatch_message`).
                const BROADCAST_FLAGS: u32 = FLAG_SHUTDOWN | FLAG_FLUSH
                    | FLAG_BACKFILL | FLAG_HAS_PK;
                if msg_wire.is_none() && msg_flags & BROADCAST_FLAGS == 0 {
                    continue;
                }
                let decoded = msg_wire.and_then(|d| ipc::decode_wire(d).ok());
                let req_id = decoded.as_ref().map(|d| d.control.request_id).unwrap_or(0);
                match self.dispatch_inner(msg_flags, msg_target, decoded, req_id) {
                    DispatchResult::Continue => {}
                    DispatchResult::Error(msg) => self.send_error(&msg, req_id),
                    DispatchResult::Shutdown => {
                        // dispatch_inner's FLAG_SHUTDOWN branch already called
                        // libc::_exit — unreachable in practice.
                        return Batch::with_schema(
                            schema.unwrap_or(SchemaDescriptor::default()), 0,
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

// ---------------------------------------------------------------------------
// Batch helper: append_row_from_batch with specified weight
// ---------------------------------------------------------------------------

impl Batch {
    /// Append a single row from another batch, with a caller-specified weight.
    fn append_row_from_batch(&mut self, src: &Batch, row: usize, weight: i64) {
        if row >= src.count { return; }
        let pk = src.get_pk(row);
        let (lo, hi) = crate::util::split_pk(pk);
        self.extend_pk_lo(&lo.to_le_bytes());
        self.extend_pk_hi(&hi.to_le_bytes());
        self.extend_weight(&weight.to_le_bytes());
        let null_word = src.get_null_word(row);
        self.extend_null_bmp(&null_word.to_le_bytes());

        let schema = self.schema.unwrap_or_else(|| src.schema.unwrap());
        for (pi, _ci, col_desc) in schema.payload_columns() {
            let cs = col_desc.size as usize;
            let is_null = (null_word >> pi) & 1 != 0;
            if is_null {
                self.fill_col_zero(pi, cs);
            } else if col_desc.type_code == crate::schema::type_code::STRING {
                let off = row * cs;
                let src_slice = &src.col_data(pi)[off..off + cs];
                let (mut dest, is_long) = crate::schema::prep_german_string_copy(src_slice);
                if is_long {
                    let length = u32::from_le_bytes(src_slice[0..4].try_into().unwrap()) as usize;
                    let src_blob_ptr = if src.blob.is_empty() { std::ptr::null() } else { src.blob.as_ptr() };
                    assert!(!src_blob_ptr.is_null(), "append_row_from_batch: long string but src blob is empty");
                    let old_offset = u64::from_le_bytes(src_slice[8..16].try_into().unwrap()) as usize;
                    let src_data = unsafe { std::slice::from_raw_parts(src_blob_ptr.add(old_offset), length) };
                    let new_offset = self.blob.len();
                    self.blob.extend_from_slice(src_data);
                    dest[8..16].copy_from_slice(&(new_offset as u64).to_le_bytes());
                }
                self.extend_col(pi, &dest);
            } else {
                let off = row * cs;
                self.extend_col(pi, &src.col_data(pi)[off..off + cs]);
            }
        }
        self.count += 1;
        self.sorted = false;
        self.consolidated = false;
    }
}

// ---------------------------------------------------------------------------
// SchemaDescriptor Default impl (for empty schema fallback)
// ---------------------------------------------------------------------------

impl Default for SchemaDescriptor {
    fn default() -> Self {
        use crate::schema::SchemaColumn;
        SchemaDescriptor {
            num_columns: 0,
            pk_index: 0,
            columns: [SchemaColumn { type_code: 0, size: 0, nullable: 0, _pad: 0 }; 64],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::SchemaDescriptor;

    fn test_schema() -> SchemaDescriptor {
        use crate::schema::{SchemaColumn, type_code};
        let mut sd = SchemaDescriptor::default();
        sd.num_columns = 2;
        sd.pk_index = 0;
        sd.columns[0] = SchemaColumn::new(type_code::U64, 0);
        sd.columns[1] = SchemaColumn::new(type_code::U64, 0);
        sd
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

    #[test]
    fn test_exchange_handler_stash_overwrite() {
        let mut handler = make_handler();
        let schema = test_schema();
        let batch1 = Batch::with_schema(schema, 0);
        let mut batch2 = Batch::with_schema(schema, 1);
        // Add a row to batch2 to distinguish
        batch2.extend_pk_lo(&100u64.to_le_bytes());
        batch2.extend_pk_hi(&0u64.to_le_bytes());
        batch2.extend_weight(&1i64.to_le_bytes());
        batch2.extend_null_bmp(&0u64.to_le_bytes());
        batch2.extend_col(0, &99u64.to_le_bytes());
        batch2.count = 1;

        handler.stash_preloaded(42, batch1);
        handler.stash_preloaded(42, batch2); // overwrite
        let retrieved = handler.stash.remove(&42).unwrap();
        assert_eq!(retrieved.count, 1);
    }

    #[test]
    fn test_pending_deltas_accumulation() {
        let schema = test_schema();
        let mut pending: HashMap<i64, Batch> = HashMap::new();

        let mut b1 = Batch::with_schema(schema, 1);
        b1.extend_pk_lo(&1u64.to_le_bytes());
        b1.extend_pk_hi(&0u64.to_le_bytes());
        b1.extend_weight(&1i64.to_le_bytes());
        b1.extend_null_bmp(&0u64.to_le_bytes());
        b1.extend_col(0, &10u64.to_le_bytes());
        b1.count = 1;

        let mut b2 = Batch::with_schema(schema, 1);
        b2.extend_pk_lo(&2u64.to_le_bytes());
        b2.extend_pk_hi(&0u64.to_le_bytes());
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

    // -- filter_my_partition tests ----------------------------------------

    /// Build a batch with the given PK values (pk_lo only, pk_hi=0).
    fn make_batch(schema: SchemaDescriptor, pks: &[u64]) -> Batch {
        let mut b = Batch::with_schema(schema, pks.len());
        for &pk in pks {
            b.extend_pk_lo(&pk.to_le_bytes());
            b.extend_pk_hi(&0u64.to_le_bytes());
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &pk.to_le_bytes());
            b.count += 1;
        }
        b
    }

    #[test]
    fn test_filter_my_partition_all_workers_cover_all_rows() {
        let schema = test_schema();
        let pks: Vec<u64> = (0..200).collect();
        let batch = make_batch(schema, &pks);
        let nw = 4usize;

        let mut total = 0usize;
        for wid in 0..nw as u32 {
            let wp = WorkerProcess {
                worker_id: wid,
                num_workers: nw,
                master_pid: 0,
                catalog: std::ptr::null_mut(),
                sal_reader: unsafe { std::mem::zeroed() },
                w2m_writer: unsafe { std::mem::zeroed() },
                exchange: make_handler(),
                pending_deltas: HashMap::new(),
                read_cursor: 0,
                expected_epoch: 1,
                schema_cache: HashMap::new(),
                schema_wire_block_cache: HashMap::new(),
            };
            let filtered = wp.filter_my_partition(batch.clone_batch());
            // Every row in the filtered batch must belong to this worker.
            for i in 0..filtered.count {
                let part = partition_for_key(filtered.get_pk(i));
                assert_eq!(worker_for_partition(part, nw), wid as usize);
            }
            total += filtered.count;
        }
        // No rows lost or duplicated.
        assert_eq!(total, 200);
    }

    #[test]
    fn test_filter_my_partition_empty_batch() {
        let schema = test_schema();
        let batch = Batch::with_schema(schema, 0);
        let wp = WorkerProcess {
            worker_id: 0,
            num_workers: 4,
            master_pid: 0,
            catalog: std::ptr::null_mut(),
            sal_reader: unsafe { std::mem::zeroed() },
            w2m_writer: unsafe { std::mem::zeroed() },
            exchange: make_handler(),
            pending_deltas: HashMap::new(),
            read_cursor: 0,
            expected_epoch: 1,
            schema_cache: HashMap::new(),
            schema_wire_block_cache: HashMap::new(),
        };
        let result = wp.filter_my_partition(batch);
        assert_eq!(result.count, 0);
    }

    #[test]
    fn test_filter_my_partition_no_schema_passthrough() {
        let schema = test_schema();
        let mut batch = Batch::with_schema(schema, 1);
        batch.extend_pk_lo(&1u64.to_le_bytes());
        batch.extend_pk_hi(&0u64.to_le_bytes());
        batch.extend_weight(&1i64.to_le_bytes());
        batch.extend_null_bmp(&0u64.to_le_bytes());
        batch.extend_col(0, &1u64.to_le_bytes());
        batch.count = 1;
        // filter_my_partition short-circuits when schema is None.
        batch.schema = None;
        let wp = WorkerProcess {
            worker_id: 0,
            num_workers: 4,
            master_pid: 0,
            catalog: std::ptr::null_mut(),
            sal_reader: unsafe { std::mem::zeroed() },
            w2m_writer: unsafe { std::mem::zeroed() },
            exchange: make_handler(),
            pending_deltas: HashMap::new(),
            read_cursor: 0,
            expected_epoch: 1,
            schema_cache: HashMap::new(),
            schema_wire_block_cache: HashMap::new(),
        };
        let result = wp.filter_my_partition(batch);
        assert_eq!(result.count, 1);
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
            read_cursor: 0,
            expected_epoch: 1,
            schema_cache: HashMap::new(),
            schema_wire_block_cache: HashMap::new(),
        };

        let req_ack: u64 = 42;
        let req_resp: u64 = 0xCAFE_BABE_DEAD_BEEF;
        let req_err: u64 = u64::MAX;
        wp.send_ack(7, 0, req_ack);
        let schema = test_schema();
        wp.send_response(8, None, Some(&schema), req_resp);
        wp.send_error("boom", req_err);

        // Decode the three messages back from the ring via try_consume.
        let hdr = unsafe { w2m_ring::W2mRingHeader::from_raw(region_ptr as *const u8) };
        let mut rc = w2m_ring::W2M_HEADER_SIZE as u64;
        let mut decoded_ids = Vec::new();
        for _ in 0..3 {
            let (data_ptr, sz, new_rc) = unsafe {
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
    fn append_row_from_batch_copies_long_string_blob() {
        use crate::schema::{SchemaColumn, type_code, encode_german_string, decode_german_string};
        // Schema: pk (U64) + name (STRING, size=16).
        let mut sd = SchemaDescriptor::default();
        sd.num_columns = 2;
        sd.pk_index = 0;
        sd.columns[0] = SchemaColumn::new(type_code::U64, 0);
        sd.columns[1] = SchemaColumn { type_code: type_code::STRING, size: 16, nullable: 0, _pad: 0 };

        let long_name = b"this name is definitely longer than twelve bytes";

        // Build source batch with a long string stored in src.blob.
        let mut src = Batch::with_schema(sd, 1);
        src.extend_pk_lo(&7u64.to_le_bytes());
        src.extend_pk_hi(&0u64.to_le_bytes());
        src.extend_weight(&1i64.to_le_bytes());
        src.extend_null_bmp(&0u64.to_le_bytes());
        // payload column 0 = name (pi=0 in payload order)
        let german = encode_german_string(long_name, &mut src.blob);
        src.extend_col(0, &german);
        src.count = 1;

        // Destination batch: starts with an existing blob entry so the offset
        // written into dest.blob is non-zero (verifies the offset is updated,
        // not hard-coded to 0).
        let mut dst = Batch::with_schema(sd, 1);
        dst.blob.extend_from_slice(b"prefixdata"); // 10 bytes already in blob
        dst.schema = Some(sd);

        dst.append_row_from_batch(&src, 0, 2);

        assert_eq!(dst.count, 1);
        // The long string cell lives in col(0) of the payload (pi=0).
        let cell = &dst.col_data(0)[0..16];
        let decoded = decode_german_string(cell.try_into().unwrap(), &dst.blob);
        assert_eq!(decoded, long_name, "blob content must round-trip through append_row_from_batch");
        // The new blob offset must be >= 10 (after the prefix).
        let new_offset = u64::from_le_bytes(cell[8..16].try_into().unwrap()) as usize;
        assert!(new_offset >= 10, "blob offset should be past the pre-existing prefix data");
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

}
