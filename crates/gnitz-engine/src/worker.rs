//! Worker process event loop.
//!
//! Owns a subset of partitions for every user table. Receives requests from
//! the master via the SAL (shared append-only log), sends responses via a
//! per-worker W2M shared region.

use std::collections::HashMap;
use std::sync::Arc;

use crate::catalog::{CatalogEngine, FIRST_USER_TABLE_ID};
use crate::schema::SchemaDescriptor;
use crate::dag::ExchangeCallback;
use crate::storage::partition_for_key;
use crate::ops::worker_for_partition_pub;
use crate::ipc::{
    self, SAL_MMAP_SIZE, STATUS_OK, STATUS_ERROR,
    FLAG_SHUTDOWN, FLAG_DDL_SYNC, FLAG_EXCHANGE, FLAG_EXCHANGE_RELAY, FLAG_PUSH, FLAG_HAS_PK,
    FLAG_SEEK, FLAG_SEEK_BY_INDEX, FLAG_PRELOADED_EXCHANGE, FLAG_BACKFILL,
    FLAG_TICK, FLAG_CHECKPOINT, FLAG_FLUSH,
    SalReader, SalMessage, W2mWriter,
};
use crate::storage::Batch;

// ---------------------------------------------------------------------------
// WorkerExchangeHandler
// ---------------------------------------------------------------------------

/// A DDL_SYNC message received during an exchange wait, decoded eagerly.
struct DeferredDdl {
    target_id: i64,
    batch: Batch,
}

/// A FLAG_PUSH message received during an exchange wait, decoded eagerly.
/// Replayed after the tick ACK so push ACKs follow tick ACKs on W2M.
struct DeferredPush {
    target_id: i64,
    batch: Batch,
}

struct WorkerExchangeHandler {
    stash: HashMap<i64, Batch>,
    deferred: Vec<DeferredDdl>,
    deferred_pushes: Vec<DeferredPush>,
    /// FLAG_TICK messages encountered inside `do_exchange_impl` while waiting
    /// for a FLAG_EXCHANGE_RELAY.  In a multi-table batched tick, the master
    /// writes TICK[A], TICK[B], TICK[C] to the SAL before signalling once.
    /// When the worker is blocked waiting for A's relay, it sees TICK[B] and
    /// TICK[C] in the SAL. They are stashed here and replayed (via
    /// `replay_deferred_ticks`) after the current tick's ACK is sent, so the
    /// master receives ACK[A], ACK[B], ACK[C] in order.
    deferred_ticks: Vec<i64>,
}

impl WorkerExchangeHandler {
    fn stash_preloaded(&mut self, view_id: i64, batch: Batch) {
        self.stash.insert(view_id, batch);
    }

    /// Perform exchange IPC: send pre-exchange output to master, receive relay.
    fn do_exchange_impl(
        &mut self,
        sal_reader: &SalReader,
        w2m_writer: &W2mWriter,
        view_id: i64,
        batch: &Batch,
        source_id: i64,
        read_cursor: &mut u64,
        expected_epoch: u32,
        master_pid: i32,
    ) -> Batch {
        if let Some(stashed) = self.stash.remove(&view_id) {
            return stashed;
        }

        let schema = batch.schema;
        let sz = ipc::wire_size(STATUS_OK, &[], schema.as_ref(), None, Some(batch));
        w2m_writer.send_encoded(sz, |buf| {
            ipc::encode_wire_into(
                buf, 0, view_id as u64, 0, FLAG_EXCHANGE as u64,
                source_id as u64, 0, 0, 0, STATUS_OK, &[],
                schema.as_ref(), None, Some(batch),
            );
        });

        // Blocking loop: wait for exchange relay (FLAG_EXCHANGE_RELAY) from master.
        // Drain all available entries before re-waiting so that interleaved
        // DDL broadcasts don't stall the exchange (each broadcast consumes
        // the eventfd signal, and re-waiting would block for up to 30 s).
        loop {
            sal_reader.wait(30000);

            // If the master died (killed or gnitz_fatal_abort) while we
            // were waiting, exit like the main run loop does. Without
            // this, do_exchange_impl would spin on 30 s waits forever.
            // Can't call self.shutdown() here because do_exchange_impl
            // borrows self; _exit(0) matches the shutdown end state.
            if master_pid != 0 && unsafe { libc::getppid() } != master_pid {
                unsafe { libc::_exit(0); }
            }

            loop {
                let (msg, new_cursor) = match sal_reader.try_read(*read_cursor) {
                    Some(v) => v,
                    None => break, // no more entries — back to outer wait
                };
                if msg.epoch != expected_epoch {
                    break;
                }
                *read_cursor = new_cursor;

                if msg.flags == FLAG_EXCHANGE_RELAY {
                    if let Some(data) = msg.wire_data {
                        if let Ok(decoded) = ipc::decode_wire(data) {
                            if let Some(batch) = decoded.data_batch {
                                return batch;
                            }
                        }
                    }
                    let empty_schema = schema.unwrap_or(SchemaDescriptor::default());
                    return Batch::with_schema(empty_schema, 0);
                }

                // Not an exchange relay. Defer DDL_SYNC, FLAG_PUSH, and
                // FLAG_TICK; discard others.
                if msg.flags & FLAG_DDL_SYNC != 0 {
                    if let Some(data) = msg.wire_data {
                        if let Ok(decoded) = ipc::decode_wire(data) {
                            if let Some(batch) = decoded.data_batch {
                                self.deferred.push(DeferredDdl {
                                    target_id: msg.target_id as i64,
                                    batch,
                                });
                            }
                        }
                    }
                } else if msg.flags & FLAG_PUSH != 0 {
                    if let Some(data) = msg.wire_data {
                        if let Ok(decoded) = ipc::decode_wire(data) {
                            if let Some(batch) = decoded.data_batch {
                                self.deferred_pushes.push(DeferredPush {
                                    target_id: msg.target_id as i64,
                                    batch,
                                });
                            }
                        }
                    }
                } else if msg.flags & FLAG_TICK != 0 {
                    // A tick for another table arrived while waiting for this
                    // exchange relay (multi-table batched tick scenario).
                    // Stash the target_id; replay_deferred_ticks processes it
                    // after the current tick's ACK is sent.
                    self.deferred_ticks.push(msg.target_id as i64);
                }
            }
        }
    }
}

/// Wrapper that carries channel handles, read_cursor, and expected_epoch from
/// WorkerProcess into the ExchangeCallback trait.
struct WorkerExchangeCtx<'a> {
    handler: &'a mut WorkerExchangeHandler,
    sal_reader: &'a SalReader,
    w2m_writer: &'a W2mWriter,
    read_cursor: &'a mut u64,
    expected_epoch: u32,
    master_pid: i32,
}

impl<'a> ExchangeCallback for WorkerExchangeCtx<'a> {
    fn do_exchange(
        &mut self,
        view_id: i64,
        batch: &Batch,
        source_id: i64,
    ) -> Batch {
        self.handler.do_exchange_impl(
            self.sal_reader, self.w2m_writer,
            view_id, batch, source_id,
            self.read_cursor, self.expected_epoch,
            self.master_pid,
        )
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
            exchange: WorkerExchangeHandler { stash: HashMap::new(), deferred: Vec::new(), deferred_pushes: Vec::new(), deferred_ticks: Vec::new() },
            pending_deltas: HashMap::new(),
            read_cursor: 0,
            expected_epoch: 1,
            schema_cache: HashMap::new(),
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
                // Replay deferred ticks BEFORE deferred pushes so all tick
                // ACKs land on W2M before any push ACKs. The master counts
                // N tick ACKs per worker via poll_tick_progress, then reads
                // push ACKs via collect_acks_continuing — this ordering is
                // required for that to work correctly.
                self.replay_deferred_ticks();
                // Replay any FLAG_PUSH messages stashed during exchange.
                // This runs AFTER all tick ACKs are sent.
                self.replay_deferred_pushes();
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
            match self.handle_backfill(target_id) {
                Ok(()) => {}
                Err(msg) => return DispatchResult::Error(msg),
            }
            self.send_ack(target_id as u64, 0, request_id);
            return DispatchResult::Continue;
        }

        if flags & FLAG_HAS_PK != 0 {
            let col_hint = seek_col_idx as i32;
            match self.handle_has_pk(target_id, batch, col_hint, request_id) {
                Ok(()) => {}
                Err(msg) => return DispatchResult::Error(msg),
            }
            return DispatchResult::Continue;
        }

        if flags & FLAG_PUSH != 0 {
            if let Some(batch) = batch {
                if batch.count > 0 {
                    match self.handle_push(target_id, batch) {
                        Ok(()) => {}
                        Err(msg) => return DispatchResult::Error(msg),
                    }
                }
            }
            self.send_ack(target_id as u64, 0, request_id);
            return DispatchResult::Continue;
        }

        if flags & FLAG_TICK != 0 {
            match self.handle_tick(target_id) {
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
        let sz = ipc::wire_size(STATUS_OK, &[], None, None, None);
        self.w2m_writer.send_encoded(sz, |buf| {
            ipc::encode_wire_into(
                buf, 0, target_id, 0, flags as u64,
                0, 0, 0, request_id, STATUS_OK, &[], None, None, None,
            );
        });
    }

    fn send_response(
        &self,
        target_id: u64,
        result: Option<&Batch>,
        schema: Option<&SchemaDescriptor>,
        request_id: u64,
    ) {
        let sz = ipc::wire_size(STATUS_OK, &[], schema, None, result);
        self.w2m_writer.send_encoded(sz, |buf| {
            ipc::encode_wire_into(
                buf, 0, target_id, 0, 0,
                0, 0, 0, request_id, STATUS_OK, &[],
                schema, None, result,
            );
        });
    }

    fn send_error(&self, error_msg: &str, request_id: u64) {
        let msg = error_msg.as_bytes();
        let sz = ipc::wire_size(STATUS_ERROR, msg, None, None, None);
        self.w2m_writer.send_encoded(sz, |buf| {
            ipc::encode_wire_into(
                buf, 0, 0, 0, 0,
                0, 0, 0, request_id, STATUS_ERROR, msg, None, None, None,
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
        &mut self, target_id: i64, batch: Batch,
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
            self.evaluate_dag(target_id, batch);
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
            if worker_for_partition_pub(partition_for_key(batch.get_pk(i)), nw) == wid {
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

    fn handle_tick(&mut self, target_id: i64) -> Result<(), String> {
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
        self.evaluate_dag(target_id, delta);
        Ok(())
    }

    fn handle_backfill(&mut self, source_tid: i64) -> Result<(), String> {
        if !self.cat().has_id(source_tid) {
            return Ok(());
        }
        let local_batch = self.cat().scan_family(source_tid)?;
        let owned = Arc::try_unwrap(local_batch).unwrap_or_else(|a| (*a).clone());
        self.evaluate_dag(source_tid, owned);
        Ok(())
    }

    fn handle_has_pk(
        &mut self,
        target_id: i64,
        batch: Option<Batch>,
        col_hint: i32,
        request_id: u64,
    ) -> Result<(), String> {
        let n = batch.as_ref().map(|b| b.count).unwrap_or(0);

        if col_hint > 0 {
            let col_idx = (col_hint - 1) as u32;
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
        } else {
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
    fn evaluate_dag(&mut self, source_id: i64, delta: Batch) {
        let dag = self.cat().get_dag_ptr();
        let master_pid = self.master_pid;
        let mut ctx = WorkerExchangeCtx {
            handler: &mut self.exchange,
            sal_reader: &self.sal_reader,
            w2m_writer: &self.w2m_writer,
            read_cursor: &mut self.read_cursor,
            expected_epoch: self.expected_epoch,
            master_pid,
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

    /// Replay FLAG_TICK messages stashed during do_exchange_impl (batched tick).
    /// Called before replay_deferred_pushes so all tick ACKs reach the master
    /// before any push ACKs — the master's collect_acks_continuing depends on
    /// that ordering.
    ///
    /// The while loop handles cascades: if processing deferred tick B causes
    /// B's own exchange wait to stash TICK[C], the next iteration catches it.
    /// For the common single-table case (deferred_ticks always empty) the
    /// is_empty() check short-circuits with no allocation.
    fn replay_deferred_ticks(&mut self) {
        while !self.exchange.deferred_ticks.is_empty() {
            let ticks = std::mem::take(&mut self.exchange.deferred_ticks);
            for tid in ticks {
                // Deferred ticks lose their original request_id when stashed.
                match self.handle_tick(tid) {
                    Ok(()) => self.send_ack(tid as u64, 0, 0),
                    Err(msg) => self.send_error(&msg, 0),
                }
            }
        }
    }

    /// Replay FLAG_PUSH messages that were stashed during do_exchange_impl.
    /// Called after the tick ACK is sent so push ACKs follow tick ACKs on W2M.
    fn replay_deferred_pushes(&mut self) {
        let pushes = std::mem::take(&mut self.exchange.deferred_pushes);
        if pushes.is_empty() { return; }
        for p in pushes {
            match self.handle_push(p.target_id, p.batch) {
                Ok(()) => self.send_ack(p.target_id as u64, 0, 0),
                Err(msg) => self.send_error(&msg, 0),
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
        let pk_index = schema.pk_index as usize;
        let mut pi = 0usize;
        for ci in 0..schema.num_columns as usize {
            if ci == pk_index { continue; }
            let col_desc = &schema.columns[ci];
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
            pi += 1;
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
        WorkerExchangeHandler { stash: HashMap::new(), deferred: Vec::<DeferredDdl>::new(), deferred_pushes: Vec::new(), deferred_ticks: Vec::new() }
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
            };
            let filtered = wp.filter_my_partition(batch.clone_batch());
            // Every row in the filtered batch must belong to this worker.
            for i in 0..filtered.count {
                let part = partition_for_key(filtered.get_pk(i));
                assert_eq!(worker_for_partition_pub(part, nw), wid as usize);
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
        };
        let result = wp.filter_my_partition(batch);
        assert_eq!(result.count, 1);
    }

    // -- deferred_ticks tests ------------------------------------------------

    /// WorkerExchangeHandler initialises with an empty deferred_ticks list.
    #[test]
    fn test_deferred_ticks_init_empty() {
        let h = make_handler();
        assert!(h.deferred_ticks.is_empty());
    }

    /// Ticks stashed in deferred_ticks are drained in FIFO order, exactly once.
    /// This mirrors the `std::mem::take` pattern inside replay_deferred_ticks.
    #[test]
    fn test_deferred_ticks_stash_and_drain_fifo() {
        let mut h = make_handler();
        h.deferred_ticks.push(101);
        h.deferred_ticks.push(202);
        h.deferred_ticks.push(303);
        assert_eq!(h.deferred_ticks.len(), 3);

        let drained = std::mem::take(&mut h.deferred_ticks);
        assert_eq!(drained, vec![101, 202, 303], "FIFO order must be preserved");
        assert!(h.deferred_ticks.is_empty(), "take must leave source empty");

        // A second take on an already-empty list returns an empty vec.
        let empty = std::mem::take(&mut h.deferred_ticks);
        assert!(empty.is_empty());
    }

    /// replay_deferred_ticks is a no-op (no panic) when deferred_ticks is empty.
    /// Uses catalog=null because no actual tick processing is triggered.
    #[test]
    fn test_replay_deferred_ticks_noop_when_empty() {
        let mut wp = WorkerProcess {
            worker_id: 0,
            num_workers: 1,
            master_pid: 0,
            catalog: std::ptr::null_mut(),
            sal_reader: unsafe { std::mem::zeroed() },
            w2m_writer: unsafe { std::mem::zeroed() },
            exchange: make_handler(),
            pending_deltas: HashMap::new(),
            read_cursor: 0,
            expected_epoch: 1,
            schema_cache: HashMap::new(),
        };
        // deferred_ticks is empty: the inner loop must break immediately.
        wp.replay_deferred_ticks();
        assert!(wp.exchange.deferred_ticks.is_empty());
    }

    /// Stage 0 wire-protocol contract: every reply helper (`send_ack`,
    /// `send_response`, `send_error`) must echo the inbound request_id back
    /// on the W2M region so the master reactor can route it. We fake out the
    /// W2M writer with a real anonymous mmap, fire each helper with a
    /// distinct id, then read the messages back through `decode_wire` and
    /// assert the ids round-trip.
    #[test]
    fn test_send_helpers_echo_request_id() {
        use crate::ipc;
        const REGION_SIZE: usize = 1 << 20;
        // mmap an anonymous region; eventfd_create gives a real fd that
        // accepts eventfd_signal calls without disturbing anything.
        let region_ptr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                REGION_SIZE,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_ANONYMOUS | libc::MAP_SHARED,
                -1, 0,
            )
        };
        assert_ne!(region_ptr, libc::MAP_FAILED);
        let region_ptr = region_ptr as *mut u8;
        // Init read cursor at W2M_HEADER_SIZE.
        unsafe { *(region_ptr as *mut u64) = ipc::W2M_HEADER_SIZE as u64; }

        let efd = crate::ipc_sys::eventfd_create();
        assert!(efd >= 0);
        let w2m_writer = ipc::W2mWriter::new(region_ptr, REGION_SIZE as u64, efd);

        let wp = WorkerProcess {
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
        };

        let req_ack: u64 = 42;
        let req_resp: u64 = 0xCAFE_BABE_DEAD_BEEF;
        let req_err: u64 = u64::MAX;
        wp.send_ack(7, 0, req_ack);
        let schema = test_schema();
        wp.send_response(8, None, Some(&schema), req_resp);
        wp.send_error("boom", req_err);

        // Decode the three messages back from the region and check ids.
        let mut rc = ipc::W2M_HEADER_SIZE as u64;
        let mut decoded_ids = Vec::new();
        for _ in 0..3 {
            let (new_rc, sz, data_ptr) = unsafe {
                ipc::w2m_read(region_ptr as *const u8, rc, REGION_SIZE as u64)
            };
            assert!(sz > 0, "expected a message at cursor {}", rc);
            let data = unsafe { std::slice::from_raw_parts(data_ptr, sz as usize) };
            let decoded = ipc::decode_wire(data).expect("decode_wire");
            decoded_ids.push(decoded.control.request_id);
            rc = new_rc;
        }
        assert_eq!(decoded_ids, vec![req_ack, req_resp, req_err]);

        unsafe {
            libc::munmap(region_ptr as *mut libc::c_void, REGION_SIZE);
            libc::close(efd);
        }
    }

    /// Simulates the cascade scenario at the data-structure level: the first
    /// pass of replay_deferred_ticks takes [B, C], and if processing B were
    /// to stash D into deferred_ticks, the loop would pick it up in pass 2.
    /// We can't call handle_tick without a real catalog, so we directly
    /// exercise the take-and-repopulate cycle that makes the loop correct.
    #[test]
    fn test_deferred_ticks_cascade_loop_terminates() {
        // Step 1: deferred_ticks has [B, C] after A's exchange wait.
        let mut h = make_handler();
        h.deferred_ticks.push(200); // B
        h.deferred_ticks.push(300); // C

        // Step 2: first pass of the loop drains [B, C].
        let pass1 = std::mem::take(&mut h.deferred_ticks);
        assert_eq!(pass1, vec![200, 300]);
        assert!(h.deferred_ticks.is_empty());

        // Step 3: processing B's exchange causes D to be stashed (simulated).
        h.deferred_ticks.push(400); // D (stashed during B's exchange)

        // Step 4: second pass picks up D.
        let pass2 = std::mem::take(&mut h.deferred_ticks);
        assert_eq!(pass2, vec![400]);
        assert!(h.deferred_ticks.is_empty());

        // Step 5: third pass: empty → loop terminates.
        let pass3 = std::mem::take(&mut h.deferred_ticks);
        assert!(pass3.is_empty(), "loop must terminate when no more deferred ticks");
    }
}
