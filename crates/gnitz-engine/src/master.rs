//! Master-side dispatcher: fans out push/scan operations to worker processes
//! via the shared append-only log (SAL) and collects responses via per-worker
//! W2M regions. Eventfds provide cross-process signaling.

use std::collections::HashMap;
use std::collections::HashSet;

use crate::catalog::CatalogEngine;
use crate::schema::SchemaDescriptor;
use crate::schema::promote_to_index_key;
use crate::ipc;
use crate::ipc::{
    FLAG_SHUTDOWN, FLAG_DDL_SYNC, FLAG_EXCHANGE, FLAG_EXCHANGE_RELAY, FLAG_PUSH, FLAG_HAS_PK,
    FLAG_SEEK, FLAG_SEEK_BY_INDEX, FLAG_PRELOADED_EXCHANGE, FLAG_BACKFILL,
    FLAG_TICK, FLAG_FLUSH, DecodedWire,
    SalWriter, W2mReceiver, W2M_HEADER_SIZE,
};
use crate::storage::{OwnedBatch, partition_for_key};
use crate::ops::{
    PartitionRouter, op_repartition_batch, op_relay_scatter, op_multi_scatter,
    worker_for_partition_pub,
};

// ---------------------------------------------------------------------------
// ExchangeAccumulator
// ---------------------------------------------------------------------------

struct ExchangeAccumulator {
    payloads:   HashMap<i64, Vec<Option<OwnedBatch>>>,
    counts:     HashMap<i64, usize>,
    schemas:    HashMap<i64, SchemaDescriptor>,
    source_ids: HashMap<i64, i64>,
    nw: usize,
}

impl ExchangeAccumulator {
    fn new(nw: usize) -> Self {
        ExchangeAccumulator {
            payloads:   HashMap::new(),
            counts:     HashMap::new(),
            schemas:    HashMap::new(),
            source_ids: HashMap::new(),
            nw,
        }
    }

    fn clear(&mut self) {
        self.payloads.clear();
        self.counts.clear();
        self.schemas.clear();
        self.source_ids.clear();
    }

    fn process(&mut self, w: usize, decoded: DecodedWire) -> Result<Option<PendingRelay>, String> {
        let vid = decoded.control.target_id as i64;
        let ex_source_id = decoded.control.seek_pk_lo as i64;
        let nw = self.nw;

        let payloads = self.payloads
            .entry(vid)
            .or_insert_with(|| vec![None; nw]);
        let count = self.counts.entry(vid).or_insert(0);

        payloads[w] = decoded.data_batch.map(|b| *b);
        if let Some(schema) = decoded.schema {
            self.schemas.insert(vid, schema);
        }
        if ex_source_id > 0 {
            self.source_ids.insert(vid, ex_source_id);
        }
        *count += 1;

        if *count == nw {
            let schema = self.schemas.remove(&vid)
                .ok_or_else(|| format!("exchange: no schema received for view_id={}", vid))?;
            let source_id = self.source_ids.remove(&vid).unwrap_or(0);
            let payloads_vec = self.payloads.remove(&vid).unwrap();
            self.counts.remove(&vid);
            Ok(Some(PendingRelay { view_id: vid, payloads: payloads_vec, schema, source_id }))
        } else {
            Ok(None)
        }
    }
}

struct PendingRelay {
    view_id:   i64,
    payloads:  Vec<Option<OwnedBatch>>,
    schema:    SchemaDescriptor,
    source_id: i64,
}

// ---------------------------------------------------------------------------
// MasterDispatcher
// ---------------------------------------------------------------------------

pub struct MasterDispatcher {
    num_workers: usize,
    worker_pids: Vec<i32>,
    sal: SalWriter,
    w2m: W2mReceiver,
    // Catalog pointer — reborrowed per-call because &mut self borrows conflict.
    catalog: *mut CatalogEngine,
    router: PartitionRouter,
    // Async tick state
    async_remaining: usize,
    async_collected: Vec<bool>,
    async_w2m_rcs: Vec<u64>,
    acc: ExchangeAccumulator,
    async_active: bool,
    /// Reusable scratch batch for scan fan-out. Retains its large data Vec
    /// across ticks, eliminating the doubling-cascade page faults.
    reusable_scan_output: Option<OwnedBatch>,
}

// Safety: MasterDispatcher is single-threaded (master process event loop).
unsafe impl Send for MasterDispatcher {}

impl MasterDispatcher {
    pub fn new(
        num_workers: usize,
        worker_pids: Vec<i32>,
        catalog: *mut CatalogEngine,
        sal: SalWriter,
        w2m: W2mReceiver,
    ) -> Self {
        MasterDispatcher {
            num_workers,
            worker_pids,
            sal,
            w2m,
            catalog,
            router: PartitionRouter::new(),
            async_remaining: 0,
            async_collected: vec![false; num_workers],
            async_w2m_rcs: vec![W2M_HEADER_SIZE as u64; num_workers],
            acc: ExchangeAccumulator::new(num_workers),
            async_active: false,
            reusable_scan_output: None,
        }
    }

    pub fn reset_sal(&mut self, write_cursor: u64, epoch: u32) {
        self.sal.reset(write_cursor, epoch);
    }

    fn get_schema_and_names(&mut self, target_id: i64) -> (SchemaDescriptor, Vec<Vec<u8>>) {
        let cat = unsafe { &mut *self.catalog };
        let schema = cat.get_schema_desc(target_id).unwrap_or_else(|| {
            panic!("master: no schema for target_id={}", target_id);
        });
        let names = cat.get_column_names(target_id);
        let name_bytes: Vec<Vec<u8>> = names.into_iter().map(|s| s.into_bytes()).collect();
        (schema, name_bytes)
    }

    // -----------------------------------------------------------------------
    // Core send/receive helpers
    // -----------------------------------------------------------------------

    /// Encode per-worker data directly into SAL mmap.
    /// Does NOT fdatasync or signal.
    fn write_group(
        &mut self,
        target_id: i64,
        flags: u32,
        worker_batches: &[Option<&OwnedBatch>],
        schema: &SchemaDescriptor,
        col_names: &[Vec<u8>],
        seek_pk_lo: u64,
        seek_pk_hi: u64,
        seek_col_idx: u64,
        unicast_worker: i32,
    ) -> Result<(), String> {
        let mut name_refs = [&[] as &[u8]; 64];
        for (i, n) in col_names.iter().enumerate().take(64) {
            name_refs[i] = n.as_slice();
        }
        let col_names_opt = if col_names.is_empty() {
            None
        } else {
            Some(&name_refs[..col_names.len()])
        };

        self.sal.write_group_direct(
            target_id as u32, flags, worker_batches,
            schema, col_names_opt,
            seek_pk_lo, seek_pk_hi, seek_col_idx, unicast_worker,
        )
    }

    /// Encode batch once directly into SAL mmap, replicate to all workers.
    fn write_broadcast(
        &mut self,
        target_id: i64,
        flags: u32,
        batch: Option<&OwnedBatch>,
        schema: &SchemaDescriptor,
        col_names: &[Vec<u8>],
        seek_pk_lo: u64,
        seek_pk_hi: u64,
        seek_col_idx: u64,
    ) -> Result<(), String> {
        let mut name_refs = [&[] as &[u8]; 64];
        for (i, n) in col_names.iter().enumerate().take(64) {
            name_refs[i] = n.as_slice();
        }
        let col_names_opt = if col_names.is_empty() {
            None
        } else {
            Some(&name_refs[..col_names.len()])
        };

        self.sal.write_broadcast_direct(
            target_id as u32, flags, batch, schema, col_names_opt,
            seek_pk_lo, seek_pk_hi, seek_col_idx,
        )
    }

    /// Encode once, write to all workers, signal (no fdatasync).
    fn send_broadcast(
        &mut self,
        target_id: i64,
        flags: u32,
        batch: Option<&OwnedBatch>,
        schema: &SchemaDescriptor,
        col_names: &[Vec<u8>],
        seek_pk_lo: u64,
        seek_pk_hi: u64,
        seek_col_idx: u64,
    ) -> Result<(), String> {
        self.write_broadcast(target_id, flags, batch, schema, col_names,
                            seek_pk_lo, seek_pk_hi, seek_col_idx)?;
        self.signal_all();
        Ok(())
    }

    pub(crate) fn sync_and_signal_all(&self) {
        self.sal.sync_and_signal_all();
    }

    fn signal_all(&self) { self.sal.signal_all(); }
    fn signal_one(&self, worker: usize) { self.sal.signal_one(worker); }
    pub(crate) fn sync(&self) { self.sal.sync(); }

    /// Signal workers + submit async fdatasync.
    pub(crate) fn signal_and_submit_fsync(&mut self, async_fsync: &mut ipc::AsyncFsync) {
        self.sal.signal_all();
        async_fsync.submit();
    }

    pub(crate) fn sal_fd(&self) -> i32 { self.sal.sal_fd() }


    /// Write per-worker message group + signal all workers (no fdatasync).
    fn send_to_workers(
        &mut self,
        target_id: i64,
        flags: u32,
        worker_batches: &[Option<&OwnedBatch>],
        schema: &SchemaDescriptor,
        col_names: &[Vec<u8>],
        seek_pk_lo: u64,
        seek_pk_hi: u64,
        seek_col_idx: u64,
    ) -> Result<(), String> {
        self.write_group(target_id, flags, worker_batches, schema, col_names,
                         seek_pk_lo, seek_pk_hi, seek_col_idx, -1)?;
        self.signal_all();
        Ok(())
    }

    pub(crate) fn reset_w2m_cursors(&self) {
        self.w2m.reset_all();
    }

    /// Wait for one response from each worker. ALWAYS resets W2M cursors.
    fn wait_all_workers(&mut self) -> Result<Vec<Option<DecodedWire>>, String> {
        let nw = self.num_workers;
        let mut results: Vec<Option<DecodedWire>> = (0..nw).map(|_| None).collect();
        let mut remaining = nw;
        let mut collected = vec![false; nw];
        let mut w2m_rcs = vec![W2M_HEADER_SIZE as u64; nw];

        let err = (|| -> Result<(), String> {
            while remaining > 0 {
                self.w2m.poll(1000);
                for w in 0..nw {
                    if collected[w] { continue; }
                    if let Some((decoded, new_rc)) = self.w2m.try_read(w, w2m_rcs[w]) {
                        w2m_rcs[w] = new_rc;
                        if decoded.control.status != 0 {
                            let msg = String::from_utf8_lossy(&decoded.control.error_msg);
                            return Err(format!("worker {}: {}", w, msg));
                        }
                        results[w] = Some(decoded);
                        collected[w] = true;
                        remaining -= 1;
                    }
                }
            }
            Ok(())
        })();
        self.reset_w2m_cursors();
        err?;
        Ok(results)
    }

    pub fn collect_acks(&mut self) -> Result<(), String> {
        self.wait_all_workers()?;
        Ok(())
    }

    pub(crate) fn num_workers(&self) -> usize { self.num_workers }

    /// Write-only ingest: broadcast batch to all workers via SAL without
    /// fdatasync/signal/collect.  Used by group-commit in flush_pending_pushes.
    /// Workers filter their own partitions on receipt (Phase 2 broadcast).
    pub(crate) fn write_ingest(&mut self, target_id: i64, batch: &OwnedBatch) -> Result<(), String> {
        let (schema, col_names) = self.get_schema_and_names(target_id);
        self.write_broadcast(target_id, FLAG_PUSH, Some(batch), &schema, &col_names, 0, 0, 0)
    }

    /// Collect one ACK per worker using caller-managed read cursors.
    /// Does NOT reset W2M — the caller must call `reset_w2m_cursors()` after
    /// all rounds of collection are done.
    pub(crate) fn collect_acks_at(&mut self, w2m_rcs: &mut [u64]) -> Result<(), String> {
        let nw = self.num_workers;
        let mut remaining = nw;
        let mut collected = vec![false; nw];
        while remaining > 0 {
            self.w2m.poll(1000);
            for w in 0..nw {
                if collected[w] { continue; }
                if let Some((decoded, new_rc)) = self.w2m.try_read(w, w2m_rcs[w]) {
                    w2m_rcs[w] = new_rc;
                    if decoded.control.status != 0 {
                        let msg = String::from_utf8_lossy(&decoded.control.error_msg);
                        return Err(format!("worker {}: {}", w, msg));
                    }
                    collected[w] = true;
                    remaining -= 1;
                }
            }
        }
        Ok(())
    }

    /// Collect ACKs from all workers, relaying exchange messages inline.
    fn collect_acks_and_relay(&mut self, _target_id: i64) -> Result<(), String> {
        let nw = self.num_workers;
        let mut remaining = nw;
        let mut collected = vec![false; nw];
        let mut w2m_rcs = vec![W2M_HEADER_SIZE as u64; nw];

        self.acc.clear();

        let err = (|| -> Result<(), String> {
            while remaining > 0 {
                self.w2m.poll(1000);
                for w in 0..nw {
                    if collected[w] { continue; }
                    while let Some((decoded, new_rc)) = self.w2m.try_read(w, w2m_rcs[w]) {
                        w2m_rcs[w] = new_rc;

                        if (decoded.control.flags as u32) & FLAG_EXCHANGE != 0 {
                            if let Some(relay) = self.acc.process(w, decoded)? {
                                self.relay_exchange(relay)?;
                            }
                            break; // re-check write_cursor after relay
                        } else {
                            if decoded.control.status != 0 {
                                let msg = String::from_utf8_lossy(&decoded.control.error_msg);
                                return Err(format!("worker {}: {}", w, msg));
                            }
                            collected[w] = true;
                            remaining -= 1;
                            break;
                        }
                    }
                }
            }
            Ok(())
        })();
        self.acc.clear();
        self.reset_w2m_cursors();
        err
    }

    /// Collect data responses from all workers, concatenate into one batch.
    /// Reuses a scratch batch from `reusable_scan_output` if the schema matches,
    /// avoiding the doubling-cascade page faults on repeated scans.
    fn collect_responses(&mut self, schema: &SchemaDescriptor) -> Result<OwnedBatch, String> {
        let npc = schema.num_columns as usize - 1;
        let results = self.wait_all_workers()?;

        let mut out = self.reusable_scan_output.take()
            .filter(|b| b.schema.map_or(false, |s|
                s.num_columns == schema.num_columns && s.pk_index == schema.pk_index))
            .unwrap_or_else(|| {
                let mut b = OwnedBatch::empty(npc);
                b.schema = Some(*schema);
                b
            });
        out.clear();

        for decoded_opt in &results {
            if let Some(decoded) = decoded_opt {
                if let Some(ref batch) = decoded.data_batch {
                    if batch.count > 0 {
                        out.append_batch(batch, 0, batch.count);
                    }
                }
            }
        }
        Ok(out)
    }

    /// Collect a single response from one worker.
    fn collect_one(&mut self, worker: usize) -> Result<DecodedWire, String> {
        let mut w2m_rc = W2M_HEADER_SIZE as u64;
        let result = (|| -> Result<DecodedWire, String> {
            loop {
                self.w2m.wait_one(worker, 1000);
                if let Some((decoded, new_rc)) = self.w2m.try_read(worker, w2m_rc) {
                    w2m_rc = new_rc;
                    return Ok(decoded);
                }
            }
        })();
        self.w2m.reset_one(worker);
        result
    }

    // -----------------------------------------------------------------------
    // SAL Checkpoint
    // -----------------------------------------------------------------------

    pub(crate) fn maybe_checkpoint(&mut self) -> Result<(), String> {
        if !self.sal.needs_checkpoint() {
            return Ok(());
        }
        self.do_checkpoint()
    }

    fn do_checkpoint(&mut self) -> Result<(), String> {
        let schema = SchemaDescriptor::minimal_u64();
        self.send_broadcast(0, FLAG_FLUSH, None, &schema, &[], 0, 0, 0)?;
        self.collect_acks()?;

        // Flush system tables before resetting SAL — their data lives in
        // SAL entries that are about to be discarded.
        let cat = unsafe { &mut *self.catalog };
        cat.flush_all_system_tables();

        self.sal.checkpoint_reset();
        gnitz_info!("SAL checkpoint epoch={}", self.sal.epoch());
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Exchange relay
    // -----------------------------------------------------------------------

    fn relay_exchange(&mut self, relay: PendingRelay) -> Result<(), String> {
        let remaining = self.sal.mmap_size() - self.sal.cursor();
        if remaining < (self.sal.mmap_size() >> 3) {
            return Err(format!(
                "SAL space exhausted during exchange relay ({} bytes left)", remaining));
        }

        let PendingRelay { view_id, payloads, schema, source_id } = relay;

        let cat = unsafe { &mut *self.catalog };
        let shard_cols = if source_id > 0 {
            let cols = cat.dag.get_join_shard_cols(view_id, source_id);
            if cols.is_empty() { cat.dag.get_shard_cols(view_id) } else { cols }
        } else {
            cat.dag.get_shard_cols(view_id)
        };

        let sources: Vec<Option<&OwnedBatch>> = payloads.iter()
            .map(|opt| opt.as_ref())
            .collect();

        let col_indices: Vec<u32> = shard_cols.iter().map(|&c| c as u32).collect();
        let dest_batches = op_relay_scatter(&sources, &col_indices, &schema, self.num_workers);

        let cat = unsafe { &mut *self.catalog };
        let names = cat.get_column_names(view_id);
        let name_bytes: Vec<Vec<u8>> = names.into_iter().map(|s| s.into_bytes()).collect();

        let refs: Vec<Option<&OwnedBatch>> = dest_batches.iter()
            .map(|b| if b.count > 0 { Some(b) } else { None })
            .collect();
        self.send_to_workers(view_id, FLAG_EXCHANGE_RELAY, &refs, &schema, &name_bytes, 0, 0, 0)
    }

    fn record_index_routing(
        &mut self, target_id: i64, schema: &SchemaDescriptor, per_worker_batches: &[OwnedBatch],
    ) {
        let cat = unsafe { &mut *self.catalog };
        let n_idx = cat.get_index_circuit_count(target_id);
        if n_idx == 0 { return; }

        for ci in 0..n_idx {
            if let Some((col_idx, is_unique, _tc)) = cat.get_index_circuit_info(target_id, ci) {
                if !is_unique { continue; }
                for w in 0..self.num_workers {
                    let sb = &per_worker_batches[w];
                    if sb.count > 0 {
                        self.router.record_routing(sb, schema, target_id as u32, col_idx, w as u32);
                    }
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // Fan-out operations
    // -----------------------------------------------------------------------

    pub fn fan_out_ingest(&mut self, target_id: i64, batch: &OwnedBatch) -> Result<(), String> {
        self.maybe_checkpoint()?;
        self.write_ingest(target_id, batch)?;
        self.sync_and_signal_all();
        self.collect_acks()?;
        gnitz_debug!("fan_out_ingest tid={} rows={}", target_id, batch.count);
        Ok(())
    }

    pub fn fan_out_tick(&mut self, target_id: i64) -> Result<(), String> {
        self.maybe_checkpoint()?;
        let (schema, col_names) = self.get_schema_and_names(target_id);
        self.send_broadcast(target_id, FLAG_TICK, None, &schema, &col_names, 0, 0, 0)?;
        self.collect_acks_and_relay(target_id)?;
        gnitz_debug!("fan_out_tick tid={}", target_id);
        Ok(())
    }

    pub fn fan_out_push(&mut self, target_id: i64, batch: &OwnedBatch) -> Result<(), String> {
        self.maybe_checkpoint()?;
        let n = batch.count;
        let (schema, col_names) = self.get_schema_and_names(target_id);

        let preloadable = {
            let cat = unsafe { &mut *self.catalog };
            cat.dag.get_preloadable_views(target_id)
        };

        let use_preload = if !preloadable.is_empty() {
            !(0..n).any(|i| batch.get_weight(i) < 0)
        } else {
            false
        };

        if !use_preload {
            let pk_col = &[schema.pk_index];
            let sub_batches = op_repartition_batch(batch, pk_col, &schema, self.num_workers);
            self.record_index_routing(target_id, &schema, &sub_batches);
            let refs: Vec<Option<&OwnedBatch>> = sub_batches.iter()
                .map(|b| if b.count > 0 { Some(b) } else { None })
                .collect();
            self.write_group(target_id, FLAG_PUSH, &refs, &schema, &col_names, 0, 0, 0, -1)?;
            self.sync_and_signal_all();
        } else {
            let mut col_specs_owned: Vec<Vec<u32>> = Vec::with_capacity(preloadable.len() + 1);
            col_specs_owned.push(vec![schema.pk_index]);
            for (_vid, cols) in &preloadable {
                col_specs_owned.push(cols.iter().map(|&c| c as u32).collect());
            }
            let col_specs: Vec<&[u32]> = col_specs_owned.iter().map(|v| v.as_slice()).collect();
            let all_batches = op_multi_scatter(batch, &col_specs, &schema, self.num_workers);

            let pk_batches = &all_batches[0];
            self.record_index_routing(target_id, &schema, pk_batches);

            for si in 0..preloadable.len() {
                let vid = preloadable[si].0;
                let preload_batches = &all_batches[si + 1];
                let refs: Vec<Option<&OwnedBatch>> = preload_batches.iter()
                    .map(|b| if b.count > 0 { Some(b) } else { None })
                    .collect();
                self.write_group(vid, FLAG_PRELOADED_EXCHANGE, &refs, &schema,
                                &col_names, 0, 0, 0, -1)?;
            }

            let refs: Vec<Option<&OwnedBatch>> = pk_batches.iter()
                .map(|b| if b.count > 0 { Some(b) } else { None })
                .collect();
            self.write_group(target_id, FLAG_PUSH, &refs, &schema,
                            &col_names, 0, 0, 0, -1)?;

            self.sync_and_signal_all();
        }

        self.collect_acks_and_relay(target_id)?;
        gnitz_debug!("fan_out_push tid={} rows={}", target_id, n);
        Ok(())
    }

    pub fn fan_out_backfill(&mut self, view_id: i64, source_id: i64) -> Result<(), String> {
        self.maybe_checkpoint()?;
        let (schema, col_names) = self.get_schema_and_names(source_id);
        self.send_broadcast(source_id, FLAG_BACKFILL, None, &schema, &col_names,
                           view_id as u64, 0, 0)?;
        self.collect_acks_and_relay(source_id)
    }

    pub fn fan_out_scan(&mut self, target_id: i64) -> Result<Option<OwnedBatch>, String> {
        self.maybe_checkpoint()?;
        let (schema, col_names) = self.get_schema_and_names(target_id);
        self.send_broadcast(target_id, 0, None, &schema, &col_names, 0, 0, 0)?;
        let result = self.collect_responses(&schema)?;
        gnitz_debug!("fan_out_scan tid={} result_rows={}", target_id, result.count);
        if result.count == 0 {
            // Stash back for reuse on next scan — retains allocated capacity.
            self.reusable_scan_output = Some(result);
            Ok(None)
        } else {
            Ok(Some(result))
        }
    }

    pub fn fan_out_seek(
        &mut self, target_id: i64, pk_lo: u64, pk_hi: u64,
    ) -> Result<Option<OwnedBatch>, String> {
        self.maybe_checkpoint()?;
        let (schema, col_names) = self.get_schema_and_names(target_id);
        let pk = crate::util::make_pk(pk_lo, pk_hi);
        let worker = worker_for_partition_pub(
            partition_for_key(pk),
            self.num_workers,
        );

        let empty: Vec<Option<&OwnedBatch>> = vec![None; self.num_workers];
        self.write_group(target_id, FLAG_SEEK, &empty, &schema, &col_names,
                        pk_lo, pk_hi, 0, worker as i32)?;
        self.signal_one(worker);

        let decoded = self.collect_one(worker)?;
        if decoded.control.status != 0 {
            let msg = String::from_utf8_lossy(&decoded.control.error_msg);
            return Err(format!("worker {}: seek: {}", worker, msg));
        }
        Ok(decoded.data_batch.and_then(|b| {
            if b.count > 0 {
                let mut result = OwnedBatch::empty(schema.num_columns as usize - 1);
                result.schema = Some(schema);
                result.append_batch(&b, 0, b.count);
                Some(result)
            } else {
                None
            }
        }))
    }

    pub fn fan_out_seek_by_index(
        &mut self, target_id: i64, col_idx: u32, key_lo: u64, key_hi: u64,
    ) -> Result<Option<OwnedBatch>, String> {
        self.maybe_checkpoint()?;
        let (schema, col_names) = self.get_schema_and_names(target_id);

        let cached_worker = self.router.worker_for_index_key(target_id as u32, col_idx, key_lo);
        if cached_worker >= 0 {
            let w = cached_worker as usize;
            let empty: Vec<Option<&OwnedBatch>> = vec![None; self.num_workers];
            self.write_group(target_id, FLAG_SEEK_BY_INDEX, &empty, &schema, &col_names,
                            key_lo, key_hi, col_idx as u64, cached_worker)?;
            self.signal_one(w);

            let decoded = self.collect_one(w)?;
            if decoded.control.status != 0 {
                let msg = String::from_utf8_lossy(&decoded.control.error_msg);
                return Err(format!("worker {}: seek_by_index: {}", w, msg));
            }
            return Ok(extract_single_batch(&schema, decoded));
        }

        // Cache miss: broadcast
        let empty: Vec<Option<&OwnedBatch>> = vec![None; self.num_workers];
        self.send_to_workers(target_id, FLAG_SEEK_BY_INDEX, &empty, &schema, &col_names,
                            key_lo, key_hi, col_idx as u64)?;
        let results = self.wait_all_workers()?;
        for decoded_opt in results {
            if let Some(decoded) = decoded_opt {
                if let Some(ref batch) = decoded.data_batch {
                    if batch.count > 0 {
                        return Ok(extract_single_batch(&schema, decoded));
                    }
                }
            }
        }
        Ok(None)
    }

    pub fn broadcast_ddl(&mut self, target_id: i64, batch: &OwnedBatch) -> Result<(), String> {
        self.maybe_checkpoint()?;
        let (schema, col_names) = self.get_schema_and_names(target_id);
        self.write_broadcast(target_id, FLAG_DDL_SYNC, Some(batch), &schema, &col_names,
                            0, 0, 0)?;
        self.signal_all();
        gnitz_debug!("broadcast_ddl tid={} rows={}", target_id, batch.count);
        Ok(())
    }

    pub fn check_pk_exists_broadcast(
        &mut self, owner_table_id: i64, source_col_idx: u32, check_batch: &OwnedBatch,
    ) -> Result<bool, String> {
        self.maybe_checkpoint()?;
        let schema = check_batch.schema
            .ok_or_else(|| "check_pk_exists_broadcast: batch has no schema".to_string())?;
        let col_hint = (source_col_idx + 1) as u64;
        self.write_broadcast(owner_table_id, FLAG_HAS_PK, Some(check_batch), &schema,
                            &[], 0, 0, col_hint)?;
        self.signal_all();

        let results = self.wait_all_workers()?;
        for decoded_opt in &results {
            if let Some(decoded) = decoded_opt {
                if let Some(ref batch) = decoded.data_batch {
                    for j in 0..batch.count {
                        if batch.get_weight(j) == 1 {
                            return Ok(true);
                        }
                    }
                }
            }
        }
        Ok(false)
    }

    // Like check_pk_exists_broadcast but returns the full set of keys found, so
    // the caller can distinguish which specific keys are occupied.
    fn check_index_keys(
        &mut self, owner_table_id: i64, source_col_idx: u32, check_batch: &OwnedBatch,
    ) -> Result<HashSet<u128>, String> {
        self.maybe_checkpoint()?;
        let schema = check_batch.schema
            .ok_or_else(|| "check_index_keys: batch has no schema".to_string())?;
        let col_hint = (source_col_idx + 1) as u64;
        self.write_broadcast(owner_table_id, FLAG_HAS_PK, Some(check_batch), &schema,
                            &[], 0, 0, col_hint)?;
        self.signal_all();

        let results = self.wait_all_workers()?;
        let mut occupied: HashSet<u128> = HashSet::new();
        for decoded_opt in &results {
            if let Some(decoded) = decoded_opt {
                if let Some(ref batch) = decoded.data_batch {
                    for j in 0..batch.count {
                        if batch.get_weight(j) == 1 {
                            occupied.insert(batch.get_pk(j));
                        }
                    }
                }
            }
        }
        Ok(occupied)
    }

    fn check_pk_existence(
        &mut self, target_id: i64, check_batch: &OwnedBatch,
    ) -> Result<HashSet<u128>, String> {
        self.maybe_checkpoint()?;
        let schema = check_batch.schema
            .ok_or_else(|| "check_pk_existence: batch has no schema".to_string())?;
        let pk_col = &[schema.pk_index];
        let sub_batches = op_repartition_batch(check_batch, pk_col, &schema, self.num_workers);
        let refs: Vec<Option<&OwnedBatch>> = sub_batches.iter()
            .map(|b| if b.count > 0 { Some(b) } else { None })
            .collect();
        self.send_to_workers(target_id, FLAG_HAS_PK, &refs, &schema, &[], 0, 0, 0)?;

        let results = self.wait_all_workers()?;
        let mut existing: HashSet<u128> = HashSet::new();
        for decoded_opt in &results {
            if let Some(decoded) = decoded_opt {
                if let Some(ref batch) = decoded.data_batch {
                    for j in 0..batch.count {
                        if batch.get_weight(j) == 1 {
                            existing.insert(batch.get_pk(j));
                        }
                    }
                }
            }
        }
        Ok(existing)
    }

    // -----------------------------------------------------------------------
    // Async tick API
    // -----------------------------------------------------------------------

    pub fn start_tick_async(&mut self, target_id: i64) -> Result<(), String> {
        self.maybe_checkpoint()?;
        let (schema, col_names) = self.get_schema_and_names(target_id);
        self.send_broadcast(target_id, FLAG_TICK, None, &schema, &col_names, 0, 0, 0)?;

        self.async_remaining = self.num_workers;
        for w in 0..self.num_workers {
            self.async_collected[w] = false;
            self.async_w2m_rcs[w] = W2M_HEADER_SIZE as u64;
        }
        self.acc.clear();
        self.async_active = true;
        Ok(())
    }

    /// Non-blocking progress check. Returns true when tick is complete.
    pub fn poll_tick_progress(&mut self) -> Result<bool, String> {
        if !self.async_active {
            return Ok(true);
        }

        let nw = self.num_workers;
        self.w2m.poll(1);

        for w in 0..nw {
            if self.async_collected[w] { continue; }
            while let Some((decoded, new_rc)) = self.w2m.try_read(w, self.async_w2m_rcs[w]) {
                self.async_w2m_rcs[w] = new_rc;

                if (decoded.control.flags as u32) & FLAG_EXCHANGE != 0 {
                    match self.acc.process(w, decoded) {
                        Ok(Some(relay)) => {
                            if let Err(e) = self.relay_exchange(relay) {
                                self.finish_async_tick();
                                return Err(e);
                            }
                        }
                        Ok(None) => {}
                        Err(e) => {
                            self.finish_async_tick();
                            return Err(e);
                        }
                    }
                    break; // re-check write_cursor after relay
                } else {
                    if decoded.control.status != 0 {
                        let msg = String::from_utf8_lossy(&decoded.control.error_msg);
                        self.finish_async_tick();
                        return Err(format!("worker {}: {}", w, msg));
                    }
                    self.async_collected[w] = true;
                    self.async_remaining -= 1;
                    break;
                }
            }
        }

        if self.async_remaining == 0 {
            self.finish_async_tick();
            return Ok(true);
        }
        Ok(false)
    }

    fn finish_async_tick(&mut self) {
        self.reset_w2m_cursors();
        self.async_active = false;
    }

    // -----------------------------------------------------------------------
    // Lifecycle
    // -----------------------------------------------------------------------

    pub fn check_workers(&self) -> i32 {
        for w in 0..self.num_workers {
            let pid = self.worker_pids[w];
            if pid <= 0 { continue; }
            let mut status: i32 = 0;
            let rpid = unsafe { libc::waitpid(pid, &mut status, libc::WNOHANG) };
            if rpid < 0 || rpid > 0 {
                return w as i32;
            }
        }
        -1
    }

    pub fn shutdown_workers(&mut self) {
        let schema = SchemaDescriptor::minimal_u64();
        let _ = self.send_broadcast(0, FLAG_SHUTDOWN, None, &schema, &[], 0, 0, 0);
        for w in 0..self.num_workers {
            let pid = self.worker_pids[w];
            if pid > 0 {
                let mut status: i32 = 0;
                unsafe { libc::waitpid(pid, &mut status, 0); }
            }
        }
    }

    // -----------------------------------------------------------------------
    // Unique index validation (moved from executor.py)
    // -----------------------------------------------------------------------

    pub fn validate_unique_distributed(
        &mut self, target_id: i64, batch: &OwnedBatch,
    ) -> Result<(), String> {
        let cat = unsafe { &mut *self.catalog };
        let n_circuits = cat.get_index_circuit_count(target_id);
        if n_circuits == 0 { return Ok(()); }

        let mut has_unique = false;
        for ci in 0..n_circuits {
            if let Some((_col_idx, is_unique, _tc)) = cat.get_index_circuit_info(target_id, ci) {
                if is_unique { has_unique = true; break; }
            }
        }
        if !has_unique { return Ok(()); }

        let source_schema = cat.get_schema_desc(target_id)
            .ok_or_else(|| format!("no schema for target_id={}", target_id))?;
        let n = batch.count;

        // Find which PKs already exist so we can skip UPSERT rows
        let mut pk_lo_list: Vec<u64> = Vec::new();
        let mut pk_hi_list: Vec<u64> = Vec::new();
        for i in 0..n {
            if batch.get_weight(i) <= 0 { continue; }
            let pk = batch.get_pk(i);
            let (lo, hi) = crate::util::split_pk(pk);
            pk_lo_list.push(lo);
            pk_hi_list.push(hi);
        }

        let existing_pks = if !pk_lo_list.is_empty() {
            self.check_pk_existence(target_id, &build_check_batch(&source_schema, &pk_lo_list, &pk_hi_list))?
        } else {
            HashSet::new()
        };

        let cat = unsafe { &mut *self.catalog };
        for ci in 0..n_circuits {
            let (col_idx, is_unique, type_code) = match cat.get_index_circuit_info(target_id, ci) {
                Some(info) => info,
                None => continue,
            };
            if !is_unique { continue; }

            let idx_schema = match cat.get_index_circuit_schema(target_id, ci) {
                Some(s) => s,
                None => continue,
            };

            let source_col = col_idx as usize;
            let pki = source_schema.pk_index as usize;
            let is_pk_col = source_col == pki;
            let src_payload_idx = if is_pk_col {
                usize::MAX
            } else if source_col < pki {
                source_col
            } else {
                source_col - 1
            };
            let col_size = source_schema.columns[source_col].size as usize;

            // (key_lo, key_hi, pk: u128) — deferred for batch verification below.
            let mut upsert_keys: Vec<(u64, u64, u128)> = Vec::new();
            let mut check_lo: Vec<u64> = Vec::new();
            let mut check_hi: Vec<u64> = Vec::new();
            let mut seen: HashSet<u128> = HashSet::new();

            for i in 0..n {
                if batch.get_weight(i) <= 0 { continue; }

                if !is_pk_col {
                    let null_word = batch.get_null_word(i);
                    if null_word & (1u64 << src_payload_idx) != 0 { continue; }
                }

                let pk_i = batch.get_pk(i);

                let (key_lo, key_hi) = if is_pk_col {
                    crate::util::split_pk(pk_i)
                } else {
                    let col_data = batch.col_data(src_payload_idx);
                    promote_to_index_key(col_data, i * col_size, col_size, type_code)
                };

                if existing_pks.contains(&pk_i) {
                    // UPSERT row: defer unique-value conflict check to the batch below.
                    upsert_keys.push((key_lo, key_hi, pk_i));
                    continue;
                }

                let key_pk = crate::util::make_pk(key_lo, key_hi);
                if seen.contains(&key_pk) {
                    let col_name = self.get_col_name(target_id, source_col);
                    return Err(format!(
                        "Unique index violation on column '{}': duplicate in batch", col_name));
                }
                seen.insert(key_pk);
                check_lo.push(key_lo);
                check_hi.push(key_hi);
            }

            // UPSERT rows: one broadcast to find which unique values are already occupied,
            // then a targeted seek only for the occupied ones to verify the holder is the
            // same row (self-update is fine; any other holder is a violation).
            if !upsert_keys.is_empty() {
                let u_lo: Vec<u64> = upsert_keys.iter().map(|&(lo, _, _)| lo).collect();
                let u_hi: Vec<u64> = upsert_keys.iter().map(|&(_, hi, _)| hi).collect();
                let u_batch = build_check_batch(&idx_schema, &u_lo, &u_hi);
                let occupied = self.check_index_keys(target_id, col_idx, &u_batch)?;
                for &(key_lo, key_hi, pk_i) in &upsert_keys {
                    let key_pk = crate::util::make_pk(key_lo, key_hi);
                    if !occupied.contains(&key_pk) { continue; }
                    // Value is occupied; confirm the holder is this same row.
                    match self.fan_out_seek_by_index(target_id, col_idx, key_lo, key_hi) {
                        Err(e) => return Err(e),
                        Ok(None) => {}
                        Ok(Some(ref found)) if found.count > 0 => {
                            if found.get_pk(0) != pk_i {
                                let col_name = self.get_col_name(target_id, source_col);
                                return Err(format!(
                                    "Unique index violation on column '{}'", col_name));
                            }
                        }
                        Ok(Some(_)) => {}
                    }
                }
            }

            if check_lo.is_empty() { continue; }

            let chk_batch = build_check_batch(&idx_schema, &check_lo, &check_hi);
            if self.check_pk_exists_broadcast(target_id, col_idx, &chk_batch)? {
                let col_name = self.get_col_name(target_id, source_col);
                return Err(format!("Unique index violation on column '{}'", col_name));
            }
        }
        Ok(())
    }

    fn get_col_name(&mut self, target_id: i64, col_idx: usize) -> String {
        let cat = unsafe { &mut *self.catalog };
        cat.get_column_names(target_id)
            .get(col_idx)
            .cloned()
            .unwrap_or_else(|| "?".to_string())
    }

    fn get_qualified_name_owned(&mut self, table_id: i64) -> (String, String) {
        let cat = unsafe { &mut *self.catalog };
        cat.get_qualified_name(table_id)
            .map(|(s, t)| (s.to_string(), t.to_string()))
            .unwrap_or_default()
    }

    // -----------------------------------------------------------------------
    // Distributed FK validation
    // -----------------------------------------------------------------------

    /// INSERT side: for each FK constraint on `target_id`, check that all
    /// referenced parent PKs exist across workers.
    pub fn validate_fk_distributed(
        &mut self, target_id: i64, batch: &OwnedBatch,
    ) -> Result<(), String> {
        let cat = unsafe { &mut *self.catalog };
        let n_fk = cat.get_fk_count(target_id);
        if n_fk == 0 { return Ok(()); }

        let source_schema = cat.get_schema_desc(target_id)
            .ok_or_else(|| format!("validate_fk_distributed: no schema for table {}", target_id))?;
        let pki = source_schema.pk_index as usize;

        for fi in 0..n_fk {
            let cat = unsafe { &mut *self.catalog };
            let (fk_col_idx, parent_table_id) = match cat.get_fk_constraint(target_id, fi) {
                Some(c) => c,
                None => continue,
            };
            let col_type = cat.get_fk_col_type(target_id, fk_col_idx);
            let parent_schema = cat.get_schema_desc(parent_table_id)
                .ok_or_else(|| format!("FK parent table {} schema not found", parent_table_id))?;

            let payload_col = if fk_col_idx < pki { fk_col_idx } else { fk_col_idx - 1 };
            let col_size = source_schema.columns[fk_col_idx].size as usize;

            let mut seen: HashSet<u128> = HashSet::new();
            let mut lo_list: Vec<u64> = Vec::new();
            let mut hi_list: Vec<u64> = Vec::new();

            for i in 0..batch.count {
                if batch.get_weight(i) <= 0 { continue; }
                let null_word = batch.get_null_word(i);
                if null_word & (1u64 << payload_col) != 0 { continue; }

                let col_data = batch.col_data(payload_col);
                let (fk_lo, fk_hi) = promote_to_index_key(col_data, i * col_size, col_size, col_type);
                let fk_key = crate::util::make_pk(fk_lo, fk_hi);
                if seen.insert(fk_key) {
                    lo_list.push(fk_lo);
                    hi_list.push(fk_hi);
                }
            }

            if lo_list.is_empty() { continue; }

            let check_batch = build_check_batch(&parent_schema, &lo_list, &hi_list);
            let existing = self.check_pk_existence(parent_table_id, &check_batch)?;

            if existing.len() < lo_list.len() {
                let (sn, tn) = self.get_qualified_name_owned(target_id);
                let (tsn, ttn) = self.get_qualified_name_owned(parent_table_id);
                return Err(format!(
                    "Foreign Key violation in '{}.{}': value not found in target '{}.{}'",
                    sn, tn, tsn, ttn
                ));
            }
        }
        Ok(())
    }

    /// DELETE side: for each child table with FK to `target_id`, check that
    /// no child rows reference the PKs being deleted.
    pub fn validate_fk_parent_restrict_distributed(
        &mut self, target_id: i64, batch: &OwnedBatch,
    ) -> Result<(), String> {
        let cat = unsafe { &mut *self.catalog };
        let n_children = cat.get_fk_children_count(target_id);
        if n_children == 0 { return Ok(()); }

        let mut neg_pks: HashSet<u128> = HashSet::new();
        for i in 0..batch.count {
            if batch.get_weight(i) < 0 {
                neg_pks.insert(batch.get_pk(i));
            }
        }
        if neg_pks.is_empty() { return Ok(()); }

        // UPSERT handling: remove PKs that also appear with positive weight
        // (retract + insert = UPDATE, not DELETE).
        for i in 0..batch.count {
            if batch.get_weight(i) > 0 {
                neg_pks.remove(&batch.get_pk(i));
            }
        }
        if neg_pks.is_empty() { return Ok(()); }

        let mut lo_list: Vec<u64> = Vec::with_capacity(neg_pks.len());
        let mut hi_list: Vec<u64> = Vec::with_capacity(neg_pks.len());
        for &pk in &neg_pks {
            let (lo, hi) = crate::util::split_pk(pk);
            lo_list.push(lo);
            hi_list.push(hi);
        }

        for ci in 0..n_children {
            let cat = unsafe { &mut *self.catalog };
            let (child_tid, fk_col_idx) = match cat.get_fk_child_info(target_id, ci) {
                Some(info) => info,
                None => continue,
            };

            let idx_schema = cat.get_index_schema_by_col(child_tid, fk_col_idx as u32)
                .ok_or_else(|| format!(
                    "FK RESTRICT check failed: no index on child table {} col {}",
                    child_tid, fk_col_idx,
                ))?;

            let check_batch = build_check_batch(&idx_schema, &lo_list, &hi_list);
            let found = self.check_index_keys(child_tid, fk_col_idx as u32, &check_batch)?;

            if !found.is_empty() {
                let (sn, tn) = self.get_qualified_name_owned(target_id);
                let (csn, ctn) = self.get_qualified_name_owned(child_tid);
                return Err(format!(
                    "Foreign Key violation: cannot delete from '{}.{}', row still referenced by '{}.{}'",
                    sn, tn, csn, ctn,
                ));
            }
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn extract_single_batch(schema: &SchemaDescriptor, decoded: DecodedWire) -> Option<OwnedBatch> {
    decoded.data_batch.and_then(|b| {
        if b.count > 0 {
            let mut result = OwnedBatch::empty(schema.num_columns as usize - 1);
            result.schema = Some(*schema);
            result.append_batch(&b, 0, b.count);
            Some(result)
        } else {
            None
        }
    })
}

fn build_check_batch(schema: &SchemaDescriptor, lo_list: &[u64], hi_list: &[u64]) -> OwnedBatch {
    let npc = schema.num_columns as usize - 1;
    let mut batch = OwnedBatch::with_schema(*schema, lo_list.len());
    let null_word: u64 = if npc > 0 { (1u64 << npc) - 1 } else { 0 };
    for k in 0..lo_list.len() {
        batch.ensure_row_capacity();
        batch.extend_pk_lo(&lo_list[k].to_le_bytes());
        batch.extend_pk_hi(&hi_list[k].to_le_bytes());
        batch.extend_weight(&1i64.to_le_bytes());
        batch.extend_null_bmp(&null_word.to_le_bytes());
        for c in 0..npc {
            let col_size = schema.columns[if c < schema.pk_index as usize { c } else { c + 1 }].size as usize;
            batch.fill_col_zero(c, col_size);
        }
        batch.count += 1;
    }
    batch
}
