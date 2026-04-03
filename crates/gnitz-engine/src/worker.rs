//! Worker process event loop (Rust port of gnitz/server/worker.py).
//!
//! Owns a subset of partitions for every user table. Receives requests from
//! the master via the SAL (shared append-only log), sends responses via a
//! per-worker W2M shared region.
//!
//! Moving this from RPython to Rust eliminates ~20 FFI round-trips per
//! message dispatch.

use std::collections::HashMap;

use crate::catalog::{CatalogEngine, FIRST_USER_TABLE_ID};
use crate::compact::SchemaDescriptor;
use crate::dag::ExchangeCallback;
use crate::ipc::{
    self, SAL_MMAP_SIZE, STATUS_OK, STATUS_ERROR,
    FLAG_SHUTDOWN, FLAG_DDL_SYNC, FLAG_EXCHANGE, FLAG_PUSH, FLAG_HAS_PK,
    FLAG_SEEK, FLAG_SEEK_BY_INDEX, FLAG_PRELOADED_EXCHANGE, FLAG_BACKFILL,
    FLAG_TICK, FLAG_CHECKPOINT, FLAG_FLUSH,
};
use crate::ipc_sys;
use crate::memtable::OwnedBatch;

// ---------------------------------------------------------------------------
// IPC handles — shared by WorkerProcess and WorkerExchangeHandler
// ---------------------------------------------------------------------------

struct IpcHandles {
    sal_ptr: *const u8,
    m2w_efd: i32,
    w2m_region_ptr: *mut u8,
    w2m_region_size: u64,
    w2m_efd: i32,
}

impl IpcHandles {
    /// Encode wire data, write to W2M region, and signal master.
    fn w2m_send(&self, wire: &[u8]) {
        unsafe {
            ipc::w2m_write(
                self.w2m_region_ptr,
                wire.as_ptr(),
                wire.len() as u32,
                self.w2m_region_size,
            );
        }
        ipc_sys::eventfd_signal(self.w2m_efd);
    }
}

// ---------------------------------------------------------------------------
// WorkerExchangeHandler
// ---------------------------------------------------------------------------

struct WorkerExchangeHandler {
    worker_id: u32,
    stash: HashMap<i64, OwnedBatch>,
}

impl WorkerExchangeHandler {
    fn stash_preloaded(&mut self, view_id: i64, batch: OwnedBatch) {
        self.stash.insert(view_id, batch);
    }

    /// Perform exchange IPC: send pre-exchange output to master, receive relay.
    fn do_exchange_impl(
        &mut self,
        ipc_h: &IpcHandles,
        view_id: i64,
        batch: &OwnedBatch,
        source_id: i64,
        read_cursor: &mut u64,
        expected_epoch: u32,
    ) -> OwnedBatch {
        if let Some(stashed) = self.stash.remove(&view_id) {
            return stashed;
        }

        let schema = batch.schema;
        let wire = ipc::encode_wire(
            view_id as u64, 0, FLAG_EXCHANGE as u64,
            source_id as u64, 0, 0,
            STATUS_OK, &[],
            schema.as_ref(), None, Some(batch),
        );
        ipc_h.w2m_send(&wire);

        // Blocking loop: wait for relay from master via SAL
        loop {
            ipc_sys::eventfd_wait(ipc_h.m2w_efd, 30000);

            let size = unsafe {
                ipc::atomic_load_u64(ipc_h.sal_ptr.add(*read_cursor as usize))
            };
            if size == 0 {
                continue;
            }

            // Use sal_read_group_header for epoch fence (avoid raw pointer math)
            let result = unsafe {
                ipc::sal_read_group_header(ipc_h.sal_ptr, *read_cursor, self.worker_id)
            };
            if result.epoch != expected_epoch {
                continue;
            }
            *read_cursor += result.advance;

            if result.status == 0 && result.data_size > 0 {
                let data = unsafe {
                    std::slice::from_raw_parts(result.data_ptr, result.data_size as usize)
                };
                if let Ok(decoded) = ipc::decode_wire(data) {
                    if let Some(batch_box) = decoded.data_batch {
                        return *batch_box;
                    }
                }
            }

            let empty_schema = schema.unwrap_or(SchemaDescriptor::default());
            return OwnedBatch::with_schema(empty_schema, 0);
        }
    }
}

/// Wrapper that carries IPC handles, read_cursor, and expected_epoch from
/// WorkerProcess into the ExchangeCallback trait.
struct WorkerExchangeCtx<'a> {
    handler: &'a mut WorkerExchangeHandler,
    ipc_h: &'a IpcHandles,
    read_cursor: &'a mut u64,
    expected_epoch: u32,
}

impl<'a> ExchangeCallback for WorkerExchangeCtx<'a> {
    fn do_exchange(
        &mut self,
        view_id: i64,
        batch: &OwnedBatch,
        source_id: i64,
    ) -> OwnedBatch {
        self.handler.do_exchange_impl(
            self.ipc_h, view_id, batch, source_id,
            self.read_cursor, self.expected_epoch,
        )
    }
}

// ---------------------------------------------------------------------------
// WorkerProcess
// ---------------------------------------------------------------------------

pub struct WorkerProcess {
    worker_id: u32,
    master_pid: i32,
    catalog: *mut CatalogEngine,
    ipc_h: IpcHandles,
    exchange: WorkerExchangeHandler,
    pending_deltas: HashMap<i64, OwnedBatch>,
    read_cursor: u64,
    expected_epoch: u32,
}

impl WorkerProcess {
    pub fn new(
        worker_id: u32,
        master_pid: i32,
        catalog: *mut CatalogEngine,
        sal_ptr: *const u8,
        m2w_efd: i32,
        w2m_region_ptr: *mut u8,
        w2m_region_size: u64,
        w2m_efd: i32,
    ) -> Self {
        WorkerProcess {
            worker_id,
            master_pid,
            catalog,
            ipc_h: IpcHandles { sal_ptr, m2w_efd, w2m_region_ptr, w2m_region_size, w2m_efd },
            exchange: WorkerExchangeHandler { worker_id, stash: HashMap::new() },
            pending_deltas: HashMap::new(),
            read_cursor: 0,
            expected_epoch: 1,
        }
    }

    fn cat(&mut self) -> &mut CatalogEngine {
        unsafe { &mut *self.catalog }
    }

    // ── Main event loop ────────────────────────────────────────────────

    pub fn run(&mut self) -> i32 {
        self.send_ack(0, 0);

        loop {
            let ready = ipc_sys::eventfd_wait(self.ipc_h.m2w_efd, 1000);
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
            // Use sal_read_group_header for both size check and epoch fence
            let result = unsafe {
                ipc::sal_read_group_header(
                    self.ipc_h.sal_ptr, self.read_cursor, self.worker_id,
                )
            };
            if result.status == -1 {
                break; // no message at cursor
            }
            if result.epoch != self.expected_epoch {
                break;
            }
            self.read_cursor += result.advance;

            if self.dispatch_message(&result) {
                return true;
            }
        }
        false
    }

    /// Dispatch a single SAL message. Returns true on shutdown.
    fn dispatch_message(&mut self, msg: &ipc::SalReadResult) -> bool {
        let mut flags = msg.flags;
        let mut target_id = msg.target_id as i64;
        let mut data_ptr = msg.data_ptr;
        let mut data_size = msg.data_size;

        // Skip messages not targeted at this worker
        if msg.status != 0 && msg.advance > 0 {
            // status=1 means no data for this worker — skip
            // But we still need to check if flags indicate a broadcast
            if flags & (FLAG_SHUTDOWN | FLAG_FLUSH | FLAG_DDL_SYNC | FLAG_TICK
                | FLAG_PUSH | FLAG_BACKFILL | FLAG_HAS_PK
                | FLAG_PRELOADED_EXCHANGE) == 0 {
                return false;
            }
            // Broadcast flags (shutdown, flush, tick, etc.) still processed
            // even when there's no worker-specific data
        }

        // Drain preloaded exchange groups before processing push
        while flags & FLAG_PRELOADED_EXCHANGE != 0 {
            let vid = target_id;
            if data_size > 0 && !data_ptr.is_null() {
                let data = unsafe { std::slice::from_raw_parts(data_ptr, data_size as usize) };
                if let Ok(decoded) = ipc::decode_wire(data) {
                    if let Some(batch_box) = decoded.data_batch {
                        self.exchange.stash_preloaded(vid, *batch_box);
                    } else if let Some(schema) = decoded.schema {
                        self.exchange.stash_preloaded(vid, OwnedBatch::with_schema(schema, 0));
                    }
                }
            }
            if self.read_cursor + 8 >= SAL_MMAP_SIZE as u64 {
                return false;
            }
            let next = unsafe {
                ipc::sal_read_group_header(
                    self.ipc_h.sal_ptr, self.read_cursor, self.worker_id,
                )
            };
            if next.status == -1 {
                return false;
            }
            self.read_cursor += next.advance;
            flags = next.flags;
            target_id = next.target_id as i64;
            data_ptr = next.data_ptr;
            data_size = next.data_size;
        }

        // Decode wire data if present
        let decoded = if data_size > 0 && !data_ptr.is_null() {
            let data = unsafe { std::slice::from_raw_parts(data_ptr, data_size as usize) };
            ipc::decode_wire(data).ok()
        } else {
            None
        };

        let result = self.dispatch_inner(flags, target_id, decoded);
        match result {
            DispatchResult::Continue => false,
            DispatchResult::Shutdown => true,
            DispatchResult::Error(msg) => {
                self.send_error(&msg);
                false
            }
        }
    }

    fn dispatch_inner(
        &mut self,
        flags: u32,
        target_id: i64,
        decoded: Option<ipc::DecodedWire>,
    ) -> DispatchResult {
        // Extract control fields before consuming decoded
        let seek_pk_lo = decoded.as_ref().map(|d| d.control.seek_pk_lo).unwrap_or(0);
        let seek_pk_hi = decoded.as_ref().map(|d| d.control.seek_pk_hi).unwrap_or(0);
        let seek_col_idx = decoded.as_ref().map(|d| d.control.seek_col_idx).unwrap_or(0);

        // Extract batch (consumes decoded)
        let batch = decoded.and_then(|d| d.data_batch.map(|b| *b));

        if flags & FLAG_SHUTDOWN != 0 {
            self.shutdown();
            return DispatchResult::Shutdown;
        }

        if flags & FLAG_FLUSH != 0 {
            self.handle_flush_all();
            self.read_cursor = 0;
            self.expected_epoch += 1;
            self.send_ack(0, FLAG_CHECKPOINT);
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
            self.send_ack(target_id as u64, 0);
            return DispatchResult::Continue;
        }

        if flags & FLAG_HAS_PK != 0 {
            let col_hint = seek_col_idx as i32;
            match self.handle_has_pk(target_id, batch, col_hint) {
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
            self.send_ack(target_id as u64, 0);
            return DispatchResult::Continue;
        }

        if flags & FLAG_TICK != 0 {
            match self.handle_tick(target_id) {
                Ok(()) => {}
                Err(msg) => return DispatchResult::Error(msg),
            }
            self.send_ack(target_id as u64, 0);
            return DispatchResult::Continue;
        }

        if flags & FLAG_SEEK_BY_INDEX != 0 {
            let col_idx = seek_col_idx as u32;
            match self.cat().seek_by_index(target_id, col_idx, seek_pk_lo, seek_pk_hi) {
                Ok(result) => {
                    let schema = self.cat().get_schema_desc(target_id);
                    self.send_response(target_id as u64, result.as_ref(), schema.as_ref());
                }
                Err(msg) => return DispatchResult::Error(msg),
            }
            return DispatchResult::Continue;
        }

        if flags & FLAG_SEEK != 0 {
            match self.cat().seek_family(target_id, seek_pk_lo, seek_pk_hi) {
                Ok(result) => {
                    let schema = self.cat().get_schema_desc(target_id);
                    self.send_response(target_id as u64, result.as_ref(), schema.as_ref());
                }
                Err(msg) => return DispatchResult::Error(msg),
            }
            return DispatchResult::Continue;
        }

        // Default: scan
        match self.cat().scan_family(target_id) {
            Ok(result) => {
                let schema = self.cat().get_schema_desc(target_id);
                self.send_response(target_id as u64, Some(&result), schema.as_ref());
            }
            Err(msg) => return DispatchResult::Error(msg),
        }
        DispatchResult::Continue
    }

    // ── W2M response helpers ───────────────────────────────────────────

    fn send_ack(&self, target_id: u64, flags: u32) {
        let wire = ipc::encode_wire(
            target_id, 0, flags as u64,
            0, 0, 0, STATUS_OK, &[],
            None, None, None,
        );
        self.ipc_h.w2m_send(&wire);
    }

    fn send_response(&self, target_id: u64, result: Option<&OwnedBatch>, schema: Option<&SchemaDescriptor>) {
        let wire = ipc::encode_wire(
            target_id, 0, 0,
            0, 0, 0, STATUS_OK, &[],
            schema, None, result,
        );
        self.ipc_h.w2m_send(&wire);
    }

    fn send_error(&self, error_msg: &str) {
        let wire = ipc::encode_wire(
            0, 0, 0,
            0, 0, 0, STATUS_ERROR, error_msg.as_bytes(),
            None, None, None,
        );
        self.ipc_h.w2m_send(&wire);
    }

    // ── Request handlers ───────────────────────────────────────────────

    fn handle_push(&mut self, target_id: i64, batch: OwnedBatch) -> Result<(), String> {
        let row_count = batch.count;
        if target_id < FIRST_USER_TABLE_ID {
            let batch_for_dag = batch.clone_batch();
            self.cat().ingest_to_family(target_id, batch)?;
            let _ = self.cat().flush_family(target_id);
            self.evaluate_dag(target_id, batch_for_dag);
        } else {
            let effective = self.cat().ingest_returning_effective(target_id, batch)?;
            let _ = self.cat().flush_family(target_id);
            if let Some(existing) = self.pending_deltas.get_mut(&target_id) {
                existing.append_batch(&effective, 0, effective.count);
            } else {
                self.pending_deltas.insert(target_id, effective);
            }
        }
        gnitz_debug!("W{} push tid={} rows={}", self.worker_id, target_id, row_count);
        Ok(())
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
            OwnedBatch::with_schema(schema, 0)
        };
        self.evaluate_dag(target_id, delta);
        Ok(())
    }

    fn handle_backfill(&mut self, source_tid: i64) -> Result<(), String> {
        if !self.cat().has_id(source_tid) {
            return Ok(());
        }
        let local_batch = self.cat().scan_family(source_tid)?;
        self.evaluate_dag(source_tid, local_batch);
        Ok(())
    }

    fn handle_has_pk(
        &mut self,
        target_id: i64,
        batch: Option<OwnedBatch>,
        col_hint: i32,
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
            let mut result = OwnedBatch::with_schema(schema, n);
            if let Some(ref b) = batch {
                let table = unsafe { &mut *index_handle };
                for i in 0..n {
                    let exists = table.has_pk(b.get_pk_lo(i), b.get_pk_hi(i));
                    result.append_row_from_batch(b, i, if exists { 1 } else { 0 });
                }
            }
            self.send_response(target_id as u64, Some(&result), Some(&schema));
        } else {
            let schema = self.cat().get_schema_desc(target_id)
                .ok_or_else(|| format!("no schema for tid={}", target_id))?;
            let ptable_handle = self.cat().get_ptable_handle(target_id);
            let mut result = OwnedBatch::with_schema(schema, n);
            if let Some(ref b) = batch {
                for i in 0..n {
                    let exists = if let Some(pt_ptr) = ptable_handle {
                        unsafe { &mut *pt_ptr }.has_pk(b.get_pk_lo(i), b.get_pk_hi(i))
                    } else {
                        false
                    };
                    result.append_row_from_batch(b, i, if exists { 1 } else { 0 });
                }
            }
            self.send_response(target_id as u64, Some(&result), Some(&schema));
        }
        Ok(())
    }

    fn handle_flush_all(&mut self) {
        self.pending_deltas.clear();
        let ids = self.cat().iter_user_table_ids();
        for tid in ids {
            let _ = self.cat().flush_family(tid);
        }
    }

    /// Run multi-worker DAG evaluation with the exchange context.
    fn evaluate_dag(&mut self, source_id: i64, delta: OwnedBatch) {
        let dag = self.cat().get_dag_ptr();
        let mut ctx = WorkerExchangeCtx {
            handler: &mut self.exchange,
            ipc_h: &self.ipc_h,
            read_cursor: &mut self.read_cursor,
            expected_epoch: self.expected_epoch,
        };
        unsafe { &mut *dag }.evaluate_dag_multi_worker(source_id, delta, &mut ctx);
    }

    fn shutdown(&mut self) {
        self.handle_flush_all();
        unsafe { libc::_exit(0); }
    }
}

enum DispatchResult {
    Continue,
    Shutdown,
    Error(String),
}

// ---------------------------------------------------------------------------
// OwnedBatch helper: append_row_from_batch with specified weight
// ---------------------------------------------------------------------------

impl OwnedBatch {
    /// Append a single row from another batch, with a caller-specified weight.
    fn append_row_from_batch(&mut self, src: &OwnedBatch, row: usize, weight: i64) {
        if row >= src.count { return; }
        self.pk_lo.extend_from_slice(&src.get_pk_lo(row).to_le_bytes());
        self.pk_hi.extend_from_slice(&src.get_pk_hi(row).to_le_bytes());
        self.weight.extend_from_slice(&weight.to_le_bytes());
        let null_word = src.get_null_word(row);
        self.null_bmp.extend_from_slice(&null_word.to_le_bytes());

        let schema = self.schema.unwrap_or_else(|| src.schema.unwrap());
        let pk_index = schema.pk_index as usize;
        let mut pi = 0usize;
        for ci in 0..schema.num_columns as usize {
            if ci == pk_index { continue; }
            let col_desc = &schema.columns[ci];
            let cs = col_desc.size as usize;
            let is_null = (null_word >> pi) & 1 != 0;
            if is_null {
                let new_len = self.col_data[pi].len() + cs;
                self.col_data[pi].resize(new_len, 0);
            } else if col_desc.type_code == crate::compact::type_code::STRING {
                let off = row * cs;
                crate::ops::write_string_from_raw(
                    &mut self.col_data[pi],
                    &mut self.blob,
                    &src.col_data[pi][off..off + cs],
                    if src.blob.is_empty() { std::ptr::null() } else { src.blob.as_ptr() },
                );
            } else {
                let off = row * cs;
                self.col_data[pi].extend_from_slice(&src.col_data[pi][off..off + cs]);
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
        use crate::compact::SchemaColumn;
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
    use crate::compact::SchemaDescriptor;

    fn test_schema() -> SchemaDescriptor {
        use crate::compact::{SchemaColumn, type_code};
        let mut sd = SchemaDescriptor::default();
        sd.num_columns = 2;
        sd.pk_index = 0;
        sd.columns[0] = SchemaColumn::new(type_code::U64, 0);
        sd.columns[1] = SchemaColumn::new(type_code::U64, 0);
        sd
    }

    fn make_handler() -> WorkerExchangeHandler {
        WorkerExchangeHandler { worker_id: 0, stash: HashMap::new() }
    }

    #[test]
    fn test_exchange_handler_stash_and_retrieve() {
        let mut handler = make_handler();
        let schema = test_schema();
        let batch = OwnedBatch::with_schema(schema, 0);
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
        let batch1 = OwnedBatch::with_schema(schema, 0);
        let mut batch2 = OwnedBatch::with_schema(schema, 1);
        // Add a row to batch2 to distinguish
        batch2.pk_lo.extend_from_slice(&100u64.to_le_bytes());
        batch2.pk_hi.extend_from_slice(&0u64.to_le_bytes());
        batch2.weight.extend_from_slice(&1i64.to_le_bytes());
        batch2.null_bmp.extend_from_slice(&0u64.to_le_bytes());
        batch2.col_data[0].extend_from_slice(&99u64.to_le_bytes());
        batch2.count = 1;

        handler.stash_preloaded(42, batch1);
        handler.stash_preloaded(42, batch2); // overwrite
        let retrieved = handler.stash.remove(&42).unwrap();
        assert_eq!(retrieved.count, 1);
    }

    #[test]
    fn test_pending_deltas_accumulation() {
        let schema = test_schema();
        let mut pending: HashMap<i64, OwnedBatch> = HashMap::new();

        let mut b1 = OwnedBatch::with_schema(schema, 1);
        b1.pk_lo.extend_from_slice(&1u64.to_le_bytes());
        b1.pk_hi.extend_from_slice(&0u64.to_le_bytes());
        b1.weight.extend_from_slice(&1i64.to_le_bytes());
        b1.null_bmp.extend_from_slice(&0u64.to_le_bytes());
        b1.col_data[0].extend_from_slice(&10u64.to_le_bytes());
        b1.count = 1;

        let mut b2 = OwnedBatch::with_schema(schema, 1);
        b2.pk_lo.extend_from_slice(&2u64.to_le_bytes());
        b2.pk_hi.extend_from_slice(&0u64.to_le_bytes());
        b2.weight.extend_from_slice(&1i64.to_le_bytes());
        b2.null_bmp.extend_from_slice(&0u64.to_le_bytes());
        b2.col_data[0].extend_from_slice(&20u64.to_le_bytes());
        b2.count = 1;

        // First insert
        pending.insert(100, b1);
        // Accumulate
        if let Some(existing) = pending.get_mut(&100) {
            existing.append_batch(&b2, 0, b2.count);
        }
        assert_eq!(pending[&100].count, 2);
    }
}
