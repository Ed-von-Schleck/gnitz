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
    FLAG_TICK, FLAG_FLUSH, FLAG_CONFLICT_MODE_PRESENT, WireConflictMode, DecodedWire,
    SalWriter, W2mReceiver, W2M_HEADER_SIZE,
};
use crate::storage::{Batch, partition_for_key};
use crate::ops::{
    PartitionRouter, op_repartition_batch, op_relay_scatter, op_multi_scatter,
    worker_for_partition_pub,
};

// ---------------------------------------------------------------------------
// ExchangeAccumulator
// ---------------------------------------------------------------------------

struct ExchangeAccumulator {
    payloads:   HashMap<i64, Vec<Option<Batch>>>,
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

        payloads[w] = decoded.data_batch;
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
    payloads:  Vec<Option<Batch>>,
    schema:    SchemaDescriptor,
    source_id: i64,
}

// ---------------------------------------------------------------------------
// Pipelined validation checks
// ---------------------------------------------------------------------------

/// How the check payload is routed to workers.
enum CheckPayload {
    /// Replicate the same batch to every worker; each worker filters
    /// its local partition.
    Broadcast(Batch),
    /// Per-worker sub-batches (caller has already routed by key).
    Partitioned(Vec<Batch>),
}

/// A single distributed has-pk check queued for pipelined execution.
/// `flags` is typically FLAG_HAS_PK; `col_hint` is (source_col_idx + 1)
/// for an index check or 0 for a PK check.
struct PipelinedCheck {
    target_id: i64,
    flags: u32,
    col_hint: u64,
    payload: CheckPayload,
    schema: SchemaDescriptor,
}

/// Side table for interpreting Phase 1 results in `validate_all_distributed`.
/// Each label lines up positionally with a `PipelinedCheck` submitted to
/// the Phase 1 pipeline.
enum P1Label {
    /// FK parent existence. `expected_count` is the number of distinct
    /// non-null parent keys; anything less in the result set is a violation.
    FkParent { parent_table_id: i64, expected_count: usize },
    /// FK child restrict: a non-empty result set is a violation.
    FkRestrict { child_tid: i64 },
    /// UPSERT PK identification; the result set is captured as the
    /// `existing_pks` feeding Phase 2 planning.
    UpsertPkId,
}

/// Side table for interpreting Phase 2 results (unique-index checks).
enum P2Label {
    /// Non-UPSERT values for one unique index: any hit is a violation.
    NonUpsert { source_col: usize },
    /// UPSERT values for one unique index: hits must be verified via
    /// `fan_out_seek_by_index` to confirm the holder is the same row.
    Upsert {
        col_idx: u32,
        source_col: usize,
        upsert_keys: Vec<(u64, u64, u128)>,
    },
}

// ---------------------------------------------------------------------------
// Master-side unique-index filter
// ---------------------------------------------------------------------------
//
// For each `(table_id, unique_col_idx)` we maintain a HashSet of all
// indexed values known to exist in the unique index. Phase 2 of
// `validate_all_distributed` consults this filter before building a
// broadcast: if every new value is definitely absent from the filter,
// that index's broadcast is skipped entirely.
//
// Correctness invariants:
//   1. A value is only added to the filter AFTER fsync confirms the
//      owning batch is durable. Before fsync, the filter is NOT updated,
//      so an in-flight insert never creates a false "present" entry.
//   2. On any flush error, filters for affected tables are invalidated
//      (dropped from the map) and lazily re-warmed on next use from
//      authoritative worker state.
//   3. Warmup is lazy: the first query for an unwarmed `(tid, col)`
//      triggers a `fan_out_scan` against the main table, from which
//      the indexed-column values are extracted. Subsequent queries
//      read the HashSet directly.
//   4. Deletes are NOT removed from the filter — they leave stale
//      "possibly present" entries that cause harmless fall-through
//      broadcasts. Stale accumulation is bounded by `UNIQUE_FILTER_CAP`:
//      once exceeded, the filter flips to `capped = true` and disables
//      itself until invalidated.
//   5. The master event loop is single-threaded, so there is no race
//      between query, warmup, and ingest.

/// Maximum number of values tracked per `(table_id, col_idx)` filter.
/// Chosen to bound worst-case memory to ~32 MB/filter (u128 HashSet
/// overhead ≈ 32 B/entry). Filters that would exceed this disable
/// themselves.
const UNIQUE_FILTER_CAP: usize = 1_000_000;

struct UniqueFilter {
    values: HashSet<u128>,
    /// True once the filter has exceeded `UNIQUE_FILTER_CAP`. In that
    /// state `values` is cleared and the filter always reports
    /// "possibly present" (falls through to broadcast).
    capped: bool,
}

impl UniqueFilter {
    fn new() -> Self {
        UniqueFilter { values: HashSet::new(), capped: false }
    }

    fn insert(&mut self, key: u128) {
        if self.capped { return; }
        self.values.insert(key);
        if self.values.len() > UNIQUE_FILTER_CAP {
            self.values = HashSet::new();
            self.capped = true;
        }
    }
}

/// Column-extraction descriptor for one unique index on a table.
/// Precomputed so the hot update path avoids catalog reborrows per row.
#[derive(Clone, Copy)]
struct UniqueIndexDesc {
    col_idx: u32,
    type_code: u8,
    is_pk_col: bool,
    /// Index into `batch.col_data(idx)` for the source column, or
    /// `usize::MAX` when `is_pk_col` (value is read from batch PK).
    src_payload_idx: usize,
    col_size: usize,
}

/// Walk every positive-weight, non-null row of `batch` and insert the
/// indexed-column value into `filter`. Respects the filter's capped state.
fn extract_into_filter(filter: &mut UniqueFilter, batch: &Batch, d: &UniqueIndexDesc) {
    for i in 0..batch.count {
        if filter.capped { return; }
        if batch.get_weight(i) <= 0 { continue; }
        if !d.is_pk_col {
            let null_word = batch.get_null_word(i);
            if null_word & (1u64 << d.src_payload_idx) != 0 { continue; }
        }
        let (lo, hi) = if d.is_pk_col {
            crate::util::split_pk(batch.get_pk(i))
        } else {
            let col_data = batch.col_data(d.src_payload_idx);
            promote_to_index_key(col_data, i * d.col_size, d.col_size, d.type_code)
        };
        filter.insert(crate::util::make_pk(lo, hi));
    }
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
    /// Per-worker countdown of tick ACKs still outstanding in the current batch.
    /// Initialised to N (the number of ticks in the batch) by begin_async_collection;
    /// decremented on each non-exchange ACK; a worker is collected when it reaches 0.
    /// Replacing the previous (async_collected, async_acks_per_worker,
    /// async_acks_collected) triple with a single countdown eliminates redundant
    /// state and removes the per-tick overhead for the common N=1 case.
    async_acks_remaining: Vec<usize>,
    async_w2m_rcs: Vec<u64>,
    acc: ExchangeAccumulator,
    async_active: bool,
    /// Per-(table_id, unique_col_idx) filter skipping redundant Phase 2
    /// unique-index broadcasts. See the UniqueFilter comment block above.
    unique_filters: HashMap<(i64, u32), UniqueFilter>,

    // Cache: table_id → (schema, col_name_bytes).
    // Avoids re-cloning Vec<String> from the catalog cache and re-encoding
    // to bytes on every fan-out. Populated lazily; table schemas are immutable.
    schema_names_cache: HashMap<i64, (SchemaDescriptor, Vec<Vec<u8>>)>,
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
            async_acks_remaining: vec![0; num_workers],
            async_w2m_rcs: vec![W2M_HEADER_SIZE as u64; num_workers],
            acc: ExchangeAccumulator::new(num_workers),
            async_active: false,
            unique_filters: HashMap::new(),
            schema_names_cache: HashMap::new(),
        }
    }

    pub fn reset_sal(&mut self, write_cursor: u64, epoch: u32) {
        self.sal.reset(write_cursor, epoch);
    }

    fn get_schema_and_names(&mut self, target_id: i64) -> (SchemaDescriptor, Vec<Vec<u8>>) {
        if let Some(cached) = self.schema_names_cache.get(&target_id) {
            return (cached.0, cached.1.clone());
        }
        let cat = unsafe { &mut *self.catalog };
        let schema = cat.get_schema_desc(target_id).unwrap_or_else(|| {
            panic!("master: no schema for target_id={}", target_id);
        });
        let names = cat.get_column_names(target_id);
        let name_bytes: Vec<Vec<u8>> = names.into_iter().map(|s| s.into_bytes()).collect();
        self.schema_names_cache.insert(target_id, (schema, name_bytes.clone()));
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
        worker_batches: &[Option<&Batch>],
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
        batch: Option<&Batch>,
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
        batch: Option<&Batch>,
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

    pub(crate) fn sync_and_signal_all(&self) -> i32 {
        self.sal.sync_and_signal_all()
    }

    pub(crate) fn signal_all(&self) { self.sal.signal_all(); }
    fn signal_one(&self, worker: usize) { self.sal.signal_one(worker); }
    pub(crate) fn sync(&self) -> i32 { self.sal.sync() }

    /// Signal workers + submit async fdatasync.
    pub(crate) fn signal_and_submit_fsync(&mut self, async_fsync: &mut ipc::AsyncFsync) {
        self.sal.signal_all();
        async_fsync.submit();
    }

    /// Submit async fdatasync without signaling workers.
    pub(crate) fn submit_fsync(&self, async_fsync: &mut ipc::AsyncFsync) {
        async_fsync.submit();
    }

    pub(crate) fn sal_fd(&self) -> i32 { self.sal.sal_fd() }


    /// Write per-worker message group + signal all workers (no fdatasync).
    fn send_to_workers(
        &mut self,
        target_id: i64,
        flags: u32,
        worker_batches: &[Option<&Batch>],
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
    /// `mode` is encoded into the control block so the worker knows whether
    /// to run SQL-standard rejection or silent-upsert semantics.
    pub(crate) fn write_ingest(
        &mut self, target_id: i64, batch: &Batch, mode: WireConflictMode,
    ) -> Result<(), String> {
        let (schema, col_names) = self.get_schema_and_names(target_id);
        self.write_broadcast(
            target_id,
            FLAG_PUSH | FLAG_CONFLICT_MODE_PRESENT,
            Some(batch), &schema, &col_names,
            0, 0, mode.as_u8() as u64,
        )
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
    /// Pre-sums worker counts to allocate the destination buffer once via
    /// `with_schema` (which goes through `batch_pool`), eliminating the
    /// doubling-cascade `reserve_rows` grow loop.
    fn collect_responses(&mut self, schema: &SchemaDescriptor) -> Result<Batch, String> {
        let results = self.wait_all_workers()?;

        // Sum first so the buffer is allocated to its final size in one shot.
        // Eliminates the doubling crawl through reserve_rows.
        let total_rows: usize = results.iter()
            .filter_map(|r| r.as_ref())
            .filter_map(|d| d.data_batch.as_ref())
            .map(|b| b.count)
            .sum();

        // with_schema goes through batch_pool::acquire_buf, so steady-state
        // calls reuse a recycled Vec<u8> with no allocation.
        let mut out = Batch::with_schema(*schema, total_rows.max(1));

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
                if let Some((decoded, new_rc)) = self.w2m.try_read(worker, w2m_rc) {
                    w2m_rc = new_rc;
                    return Ok(decoded);
                }
                self.w2m.wait_one(worker, 1000);
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

        let sources: Vec<Option<&Batch>> = payloads.iter()
            .map(|opt| opt.as_ref())
            .collect();

        let col_indices: Vec<u32> = shard_cols.iter().map(|&c| c as u32).collect();
        let dest_batches = op_relay_scatter(&sources, &col_indices, &schema, self.num_workers);

        let cat = unsafe { &mut *self.catalog };
        let names = cat.get_column_names(view_id);
        let name_bytes: Vec<Vec<u8>> = names.into_iter().map(|s| s.into_bytes()).collect();

        let refs: Vec<Option<&Batch>> = dest_batches.iter()
            .map(|b| if b.count > 0 { Some(b) } else { None })
            .collect();
        self.send_to_workers(view_id, FLAG_EXCHANGE_RELAY, &refs, &schema, &name_bytes, 0, 0, 0)
    }

    fn record_index_routing(
        &mut self, target_id: i64, schema: &SchemaDescriptor, per_worker_batches: &[Batch],
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

    pub fn fan_out_ingest(
        &mut self, target_id: i64, batch: &Batch, mode: WireConflictMode,
    ) -> Result<(), String> {
        self.maybe_checkpoint()?;
        self.write_ingest(target_id, batch, mode)?;
        let rc = self.sync_and_signal_all();
        if rc < 0 {
            return Err(format!("fdatasync failed rc={}", rc));
        }
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

    pub fn fan_out_push(
        &mut self, target_id: i64, batch: &Batch, mode: WireConflictMode,
    ) -> Result<(), String> {
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

        let push_flags = FLAG_PUSH | FLAG_CONFLICT_MODE_PRESENT;
        let mode_byte = mode.as_u8() as u64;

        if !use_preload {
            let pk_col = &[schema.pk_index];
            let sub_batches = op_repartition_batch(batch, pk_col, &schema, self.num_workers);
            self.record_index_routing(target_id, &schema, &sub_batches);
            let refs: Vec<Option<&Batch>> = sub_batches.iter()
                .map(|b| if b.count > 0 { Some(b) } else { None })
                .collect();
            self.write_group(target_id, push_flags, &refs, &schema, &col_names,
                            0, 0, mode_byte, -1)?;
            let rc = self.sync_and_signal_all();
            if rc < 0 {
                return Err(format!("fdatasync failed rc={}", rc));
            }
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
                let refs: Vec<Option<&Batch>> = preload_batches.iter()
                    .map(|b| if b.count > 0 { Some(b) } else { None })
                    .collect();
                self.write_group(vid, FLAG_PRELOADED_EXCHANGE, &refs, &schema,
                                &col_names, 0, 0, 0, -1)?;
            }

            let refs: Vec<Option<&Batch>> = pk_batches.iter()
                .map(|b| if b.count > 0 { Some(b) } else { None })
                .collect();
            self.write_group(target_id, push_flags, &refs, &schema,
                            &col_names, 0, 0, mode_byte, -1)?;

            let rc = self.sync_and_signal_all();
            if rc < 0 {
                return Err(format!("fdatasync failed rc={}", rc));
            }
        }

        self.collect_acks_and_relay(target_id)?;
        gnitz_debug!("fan_out_push tid={} rows={} mode={:?}", target_id, n, mode);
        Ok(())
    }

    pub fn fan_out_backfill(&mut self, view_id: i64, source_id: i64) -> Result<(), String> {
        self.maybe_checkpoint()?;
        let (schema, col_names) = self.get_schema_and_names(source_id);
        self.send_broadcast(source_id, FLAG_BACKFILL, None, &schema, &col_names,
                           view_id as u64, 0, 0)?;
        self.collect_acks_and_relay(source_id)
    }

    pub fn fan_out_scan(&mut self, target_id: i64) -> Result<Option<Batch>, String> {
        self.maybe_checkpoint()?;
        let (schema, col_names) = self.get_schema_and_names(target_id);
        self.send_broadcast(target_id, 0, None, &schema, &col_names, 0, 0, 0)?;
        let result = self.collect_responses(&schema)?;
        gnitz_debug!("fan_out_scan tid={} result_rows={}", target_id, result.count);
        if result.count == 0 {
            Ok(None)
        } else {
            Ok(Some(result))
        }
    }

    pub fn fan_out_seek(
        &mut self, target_id: i64, pk_lo: u64, pk_hi: u64,
    ) -> Result<Option<Batch>, String> {
        self.maybe_checkpoint()?;
        let (schema, col_names) = self.get_schema_and_names(target_id);
        let pk = crate::util::make_pk(pk_lo, pk_hi);
        let worker = worker_for_partition_pub(
            partition_for_key(pk),
            self.num_workers,
        );

        let empty: Vec<Option<&Batch>> = vec![None; self.num_workers];
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
                let mut result = Batch::empty(schema.num_columns as usize - 1);
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
    ) -> Result<Option<Batch>, String> {
        self.maybe_checkpoint()?;
        let (schema, col_names) = self.get_schema_and_names(target_id);

        let cached_worker = self.router.worker_for_index_key(target_id as u32, col_idx, key_lo);
        if cached_worker >= 0 {
            let w = cached_worker as usize;
            let empty: Vec<Option<&Batch>> = vec![None; self.num_workers];
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
        let empty: Vec<Option<&Batch>> = vec![None; self.num_workers];
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

    pub fn broadcast_ddl(&mut self, target_id: i64, batch: &Batch) -> Result<(), String> {
        self.maybe_checkpoint()?;
        let (schema, col_names) = self.get_schema_and_names(target_id);
        self.write_broadcast(target_id, FLAG_DDL_SYNC, Some(batch), &schema, &col_names,
                            0, 0, 0)?;
        self.signal_all();
        gnitz_debug!("broadcast_ddl tid={} rows={}", target_id, batch.count);
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Async tick API
    // -----------------------------------------------------------------------

    fn begin_async_collection(&mut self, acks_per_worker: usize) {
        self.async_remaining = self.num_workers;
        for w in 0..self.num_workers {
            self.async_acks_remaining[w] = acks_per_worker;
            self.async_w2m_rcs[w] = W2M_HEADER_SIZE as u64;
        }
        self.acc.clear();
        self.async_active = true;
    }

    pub fn start_tick_async(&mut self, target_id: i64) -> Result<(), String> {
        self.maybe_checkpoint()?;
        let (schema, col_names) = self.get_schema_and_names(target_id);
        self.send_broadcast(target_id, FLAG_TICK, None, &schema, &col_names, 0, 0, 0)?;

        self.begin_async_collection(1);
        Ok(())
    }

    /// Write N FLAG_TICK groups to the SAL and signal all workers once.
    /// Workers process the ticks sequentially, sending one ACK per tick.
    /// `poll_tick_progress` collects N ACKs per worker (N × num_workers total)
    /// before declaring the batch complete.
    ///
    /// Exchange relays for all tables are handled inline by `poll_tick_progress`
    /// exactly as for a single tick: the ExchangeAccumulator is keyed by
    /// view_id, so relays for different tables are demuxed automatically.
    ///
    /// # Panics (debug)
    /// Panics if `tids` is empty — callers must filter to non-empty slices.
    pub fn start_ticks_async_batch(&mut self, tids: &[i64]) -> Result<(), String> {
        debug_assert!(!tids.is_empty(), "start_ticks_async_batch called with empty tids");
        self.maybe_checkpoint()?;

        // Write all FLAG_TICK groups without signalling between them.
        // If write_broadcast fails mid-way, the partial writes are harmless:
        // no signal has been sent yet, so workers will not process them.
        // The next maybe_checkpoint (FLAG_FLUSH) resets worker epochs,
        // invalidating any unread SAL entries from this failed batch.
        for &tid in tids {
            let (schema, col_names) = self.get_schema_and_names(tid);
            self.write_broadcast(tid, FLAG_TICK, None, &schema, &col_names, 0, 0, 0)?;
        }
        // Single signal after all writes — one eventfd kick for N ticks.
        self.signal_all();

        self.begin_async_collection(tids.len());
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
            if self.async_acks_remaining[w] == 0 { continue; }
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
                    // One ACK received; worker is collected when its countdown hits 0.
                    self.async_acks_remaining[w] -= 1;
                    if self.async_acks_remaining[w] == 0 {
                        self.async_remaining -= 1;
                    }
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
        // Note: does NOT reset W2M cursors. The caller is responsible for
        // calling reset_w2m_cursors() after it has finished reading any
        // remaining W2M data (e.g. push ACKs that follow tick ACKs).
        self.async_active = false;
    }

    /// Collect one push ACK per worker using the W2M read cursors left
    /// behind by poll_tick_progress.  This lets the caller read push ACKs
    /// that workers appended to W2M *after* their tick ACKs.
    pub(crate) fn collect_acks_continuing(&mut self) -> Result<(), String> {
        let nw = self.num_workers;
        let mut remaining = nw;
        let mut collected = vec![false; nw];
        while remaining > 0 {
            self.w2m.poll(1000);
            for w in 0..nw {
                if collected[w] { continue; }
                if let Some((decoded, new_rc)) = self.w2m.try_read(w, self.async_w2m_rcs[w]) {
                    self.async_w2m_rcs[w] = new_rc;
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
    // Unique-index filter
    // -----------------------------------------------------------------------

    /// Collect column-extraction descriptors for every unique index on
    /// `table_id`. Returns (col_idx, type_code, is_pk_col, payload_idx,
    /// col_size) per unique circuit — the shape expected by
    /// `extract_unique_index_key`.
    fn unique_index_descriptors(
        &mut self, table_id: i64,
    ) -> Option<(SchemaDescriptor, Vec<UniqueIndexDesc>)> {
        let cat = unsafe { &mut *self.catalog };
        let schema = cat.get_schema_desc(table_id)?;
        let n_circuits = cat.get_index_circuit_count(table_id);
        let pki = schema.pk_index as usize;

        let mut out: Vec<UniqueIndexDesc> = Vec::new();
        for ci in 0..n_circuits {
            if let Some((col_idx, is_unique, type_code)) =
                cat.get_index_circuit_info(table_id, ci)
            {
                if !is_unique { continue; }
                let source_col = col_idx as usize;
                let is_pk_col = source_col == pki;
                let src_payload_idx = if is_pk_col {
                    usize::MAX
                } else if source_col < pki {
                    source_col
                } else {
                    source_col - 1
                };
                let col_size = schema.columns[source_col].size as usize;
                out.push(UniqueIndexDesc {
                    col_idx, type_code, is_pk_col, src_payload_idx, col_size,
                });
            }
        }
        if out.is_empty() { None } else { Some((schema, out)) }
    }

    /// Warm up any unwarmed unique-index filters for `table_id`. A single
    /// `fan_out_scan` on the main table is enough to seed all of them —
    /// the indexed-column values are extracted from the scan result.
    /// Idempotent: already-warm filters are left alone.
    fn ensure_unique_filters_warm(&mut self, table_id: i64) -> Result<(), String> {
        let (_schema, descs) = match self.unique_index_descriptors(table_id) {
            Some(x) => x,
            None => return Ok(()),
        };
        let missing: Vec<UniqueIndexDesc> = descs.into_iter()
            .filter(|d| !self.unique_filters.contains_key(&(table_id, d.col_idx)))
            .collect();
        if missing.is_empty() {
            return Ok(());
        }

        // Seed empty filters for each missing one so that an empty table
        // scan still produces a warm (empty) filter.
        for d in &missing {
            self.unique_filters.insert((table_id, d.col_idx), UniqueFilter::new());
        }

        // One scan feeds all of them.
        let scan_result = self.fan_out_scan(table_id)?;
        if let Some(batch) = scan_result {
            for d in &missing {
                if let Some(filter) = self.unique_filters.get_mut(&(table_id, d.col_idx)) {
                    extract_into_filter(filter, &batch, d);
                    if filter.capped { continue; }
                }
            }
        }
        Ok(())
    }

    /// True if every key in `keys` is definitely absent from the filter
    /// for `(table_id, col_idx)`. Returns false if the filter is capped,
    /// not warm (caller is expected to warm it first), or contains any
    /// key. On false, caller must fall through to the Phase 2 broadcast.
    fn unique_filter_all_absent(
        &self, table_id: i64, col_idx: u32, keys: &[u128],
    ) -> bool {
        let filter = match self.unique_filters.get(&(table_id, col_idx)) {
            Some(f) => f,
            None => return false,
        };
        if filter.capped {
            return false;
        }
        keys.iter().all(|k| !filter.values.contains(k))
    }

    /// Record every unique-index value from a successfully-flushed
    /// `batch` on `table_id` into the corresponding filters. No-op for
    /// filters that are not yet warm (warmup will pick them up), and
    /// for index circuits that are not unique.
    pub(crate) fn unique_filter_ingest_batch(&mut self, table_id: i64, batch: &Batch) {
        let (_schema, descs) = match self.unique_index_descriptors(table_id) {
            Some(x) => x,
            None => return,
        };
        for d in descs {
            let filter = match self.unique_filters.get_mut(&(table_id, d.col_idx)) {
                Some(f) => f,
                None => continue, // not warm — warmup will pick this up
            };
            if filter.capped { continue; }
            extract_into_filter(filter, batch, &d);
        }
    }

    /// Drop every filter entry for `table_id`. Called on flush errors
    /// (where filter state may be out of sync with workers) and on DDL
    /// changes (DROP TABLE, DROP/CREATE INDEX).
    pub(crate) fn unique_filter_invalidate_table(&mut self, table_id: i64) {
        self.unique_filters.retain(|&(t, _), _| t != table_id);
    }

    // -----------------------------------------------------------------------
    // Pipelined distributed validation
    // -----------------------------------------------------------------------
    //
    // A user INSERT may trigger up to O(N) distributed has-pk checks
    // (one per FK constraint, one per FK child with restrict deletes,
    // one for UPSERT PK identification, plus one or two per unique
    // secondary index). Running them sequentially costs N master↔worker
    // round trips. `execute_pipeline` writes the whole burst into SAL,
    // signals once, then collects N responses per worker in a single
    // poll loop — cursor position on the W2M ring correlates each
    // response with its originating check, so no explicit correlation
    // ID is needed. `validate_all_distributed` composes the bursts into
    // at most 2 rounds: Phase 1 is fully independent; Phase 2 depends
    // on Phase 1's UPSERT-identification result.

    /// Submit `checks` as a single pipelined burst; return per-check
    /// aggregated result sets (union of PKs returned with weight=1 across
    /// all workers). Drains all responses before returning even on error
    /// to keep W2M cursors consistent for the next IPC round.
    fn execute_pipeline(
        &mut self, checks: &[PipelinedCheck],
    ) -> Result<Vec<HashSet<u128>>, String> {
        let num_checks = checks.len();
        if num_checks == 0 {
            return Ok(Vec::new());
        }

        // Reserve SAL capacity for the whole burst up front.
        self.maybe_checkpoint()?;

        // Write all SAL messages back-to-back (no signal between them).
        for check in checks {
            match &check.payload {
                CheckPayload::Broadcast(batch) => {
                    self.write_broadcast(
                        check.target_id, check.flags, Some(batch),
                        &check.schema, &[], 0, 0, check.col_hint,
                    )?;
                }
                CheckPayload::Partitioned(batches) => {
                    let refs: Vec<Option<&Batch>> = batches.iter()
                        .map(|b| if b.count > 0 { Some(b) } else { None })
                        .collect();
                    self.write_group(
                        check.target_id, check.flags, &refs,
                        &check.schema, &[], 0, 0, check.col_hint, -1,
                    )?;
                }
            }
        }

        // One signal for the whole burst.
        self.signal_all();

        // Drain num_checks * num_workers responses. Workers process SAL
        // messages in submission order, so the k-th response from worker
        // w belongs to check[k].
        let nw = self.num_workers;
        let mut results: Vec<HashSet<u128>> = checks.iter().map(|check| {
            let cap = match &check.payload {
                CheckPayload::Broadcast(b) => b.count,
                CheckPayload::Partitioned(bs) => bs.iter().map(|b| b.count).sum(),
            };
            HashSet::with_capacity(cap)
        }).collect();
        let mut responses_collected = vec![0usize; nw];
        let mut w2m_rcs = vec![W2M_HEADER_SIZE as u64; nw];
        let mut total_remaining = nw * num_checks;
        let mut first_error: Option<String> = None;

        while total_remaining > 0 {
            self.w2m.poll(1000);
            for w in 0..nw {
                while responses_collected[w] < num_checks {
                    let (decoded, new_rc) = match self.w2m.try_read(w, w2m_rcs[w]) {
                        Some(x) => x,
                        None => break,
                    };
                    w2m_rcs[w] = new_rc;
                    let check_idx = responses_collected[w];
                    responses_collected[w] += 1;
                    total_remaining -= 1;

                    if decoded.control.status != 0 {
                        if first_error.is_none() {
                            let msg = String::from_utf8_lossy(&decoded.control.error_msg);
                            first_error = Some(format!("worker {}: {}", w, msg));
                        }
                        continue;
                    }

                    if let Some(ref batch) = decoded.data_batch {
                        for j in 0..batch.count {
                            if batch.get_weight(j) == 1 {
                                results[check_idx].insert(batch.get_pk(j));
                            }
                        }
                    }
                }
            }
        }

        self.reset_w2m_cursors();

        if let Some(err) = first_error {
            return Err(err);
        }
        Ok(results)
    }

    /// Run all distributed validation for a user-table DML batch in at
    /// most two pipelined rounds. Phase 1 issues the independent checks
    /// (FK parent existence, FK child restrict, UPSERT PK identification).
    /// Phase 2 issues the unique-index checks, which depend on the
    /// UPSERT set from Phase 1.
    ///
    /// `mode` controls SQL semantics: `Error` rejects the batch on any
    /// PK/unique conflict; `Update` allows retract-and-insert.
    pub fn validate_all_distributed(
        &mut self, target_id: i64, batch: &Batch, mode: WireConflictMode,
    ) -> Result<(), String> {
        // Snapshot catalog metadata up front.
        let (n_fk, n_children, n_circuits, has_unique, unique_pk, source_schema) = {
            let cat = unsafe { &mut *self.catalog };
            let n_fk = cat.get_fk_count(target_id);
            let n_children = cat.get_fk_children_count(target_id);
            let n_circuits = cat.get_index_circuit_count(target_id);
            let has_unique = cat.has_any_unique_index(target_id);
            let unique_pk = cat.table_has_unique_pk(target_id);
            let source_schema = cat.get_schema_desc(target_id)
                .ok_or_else(|| format!(
                    "validate_all_distributed: no schema for table {}", target_id))?;
            (n_fk, n_children, n_circuits, has_unique, unique_pk, source_schema)
        };

        // In Error mode on a unique_pk table we must also run the PK
        // existence check even if no FK / unique-secondary-index
        // validator would otherwise fire — a plain duplicate-PK INSERT
        // needs to be rejected.
        let needs_pk_rejection = matches!(mode, WireConflictMode::Error) && unique_pk;

        if n_fk == 0 && n_children == 0 && !has_unique && !needs_pk_rejection {
            return Ok(());
        }

        let pki = source_schema.pk_index as usize;

        // One-pass aggregation over the batch: per-PK (net_weight,
        // positive_row_count). Used by both the Error-mode intra-batch
        // rejection check below and the Phase 1 UpsertPkId planning
        // further down, avoiding a second walk of the batch. Allocated
        // only when either consumer needs it.
        let pk_agg: HashMap<u128, (i64, u32)> = if has_unique || needs_pk_rejection {
            let mut m: HashMap<u128, (i64, u32)> = HashMap::with_capacity(batch.count);
            for i in 0..batch.count {
                let w = batch.get_weight(i);
                if w == 0 { continue; }
                let entry = m.entry(batch.get_pk(i)).or_insert((0, 0));
                entry.0 += w;
                if w > 0 { entry.1 += 1; }
            }
            m
        } else {
            HashMap::new()
        };

        // Error-mode: reject any PK whose positive-row-count > 1 in
        // this batch (e.g. `INSERT INTO t VALUES (1,10),(1,20)`).
        if needs_pk_rejection {
            for (&pk, &(_, pos_count)) in &pk_agg {
                if pos_count > 1 {
                    let pk_name = self.get_col_name(target_id, pki);
                    let (sn, tn) = self.get_qualified_name_owned(target_id);
                    let key_str = format_pk_value(pk, &source_schema);
                    return Err(format!(
                        "duplicate key value violates unique constraint \"{}_{}_pkey\": Batch contains multiple rows with key ({})=({})",
                        sn, tn, pk_name, key_str,
                    ));
                }
            }
        }

        // ----- Phase 1 plan -----------------------------------------------
        let mut p1_checks: Vec<PipelinedCheck> = Vec::new();
        let mut p1_labels: Vec<P1Label> = Vec::new();

        // FK parent existence (one per constraint with non-null referrers)
        for fi in 0..n_fk {
            let (fk_col_idx, parent_table_id, col_type, parent_schema) = {
                let cat = unsafe { &mut *self.catalog };
                let (fk_col_idx, parent_table_id) = match cat.get_fk_constraint(target_id, fi) {
                    Some(c) => c,
                    None => continue,
                };
                let col_type = cat.get_fk_col_type(target_id, fk_col_idx);
                let parent_schema = cat.get_schema_desc(parent_table_id)
                    .ok_or_else(|| format!(
                        "FK parent table {} schema not found", parent_table_id))?;
                (fk_col_idx, parent_table_id, col_type, parent_schema)
            };

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
                let (fk_lo, fk_hi) = promote_to_index_key(
                    col_data, i * col_size, col_size, col_type);
                let fk_key = crate::util::make_pk(fk_lo, fk_hi);
                if seen.insert(fk_key) {
                    lo_list.push(fk_lo);
                    hi_list.push(fk_hi);
                }
            }

            if lo_list.is_empty() { continue; }

            let expected_count = lo_list.len();
            let check_batch = build_check_batch(&parent_schema, &lo_list, &hi_list);
            let pk_col = &[parent_schema.pk_index];
            let sub_batches = op_repartition_batch(
                &check_batch, pk_col, &parent_schema, self.num_workers);

            p1_labels.push(P1Label::FkParent { parent_table_id, expected_count });
            p1_checks.push(PipelinedCheck {
                target_id: parent_table_id,
                flags: FLAG_HAS_PK,
                col_hint: 0,
                payload: CheckPayload::Partitioned(sub_batches),
                schema: parent_schema,
            });
        }

        // FK child restrict (one per child, DELETE side). Shared PK list.
        if n_children > 0 {
            let mut neg_pks: HashSet<u128> = HashSet::new();
            for i in 0..batch.count {
                if batch.get_weight(i) < 0 {
                    neg_pks.insert(batch.get_pk(i));
                }
            }
            // UPSERT handling: drop PKs that also appear with positive weight
            // (retract + insert = UPDATE, not DELETE).
            for i in 0..batch.count {
                if batch.get_weight(i) > 0 {
                    neg_pks.remove(&batch.get_pk(i));
                }
            }

            if !neg_pks.is_empty() {
                let mut lo_list: Vec<u64> = Vec::with_capacity(neg_pks.len());
                let mut hi_list: Vec<u64> = Vec::with_capacity(neg_pks.len());
                for &pk in &neg_pks {
                    let (lo, hi) = crate::util::split_pk(pk);
                    lo_list.push(lo);
                    hi_list.push(hi);
                }

                for ci in 0..n_children {
                    let (child_tid, fk_col_idx, idx_schema) = {
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
                        (child_tid, fk_col_idx, idx_schema)
                    };

                    let check_batch = build_check_batch(&idx_schema, &lo_list, &hi_list);
                    p1_labels.push(P1Label::FkRestrict { child_tid });
                    p1_checks.push(PipelinedCheck {
                        target_id: child_tid,
                        flags: FLAG_HAS_PK,
                        col_hint: (fk_col_idx as u64) + 1,
                        payload: CheckPayload::Broadcast(check_batch),
                        schema: idx_schema,
                    });
                }
            }
        }

        // UPSERT PK identification — needed in two cases:
        //   1. `has_unique` so Phase 2 can classify upsert rows.
        //   2. `needs_pk_rejection`: Error-mode on a unique_pk table —
        //      repurposed as the authoritative PK rejection broadcast.
        //      Any PK already in the store is a SQL-standard violation.
        //
        // Intra-batch consolidation: only PKs with net_weight > 0 are
        // checked against the store. This preserves z-set primitives
        // (a +1 then -1 on the same PK is net zero and must not trigger
        // an against-store conflict).
        if has_unique || needs_pk_rejection {
            let mut lo_list: Vec<u64> = Vec::with_capacity(pk_agg.len());
            let mut hi_list: Vec<u64> = Vec::with_capacity(pk_agg.len());
            for (&pk, &(net_weight, _)) in &pk_agg {
                if net_weight <= 0 { continue; }
                let (lo, hi) = crate::util::split_pk(pk);
                lo_list.push(lo);
                hi_list.push(hi);
            }

            if !lo_list.is_empty() {
                let check_batch = build_check_batch(&source_schema, &lo_list, &hi_list);
                let pk_col = &[source_schema.pk_index];
                let sub_batches = op_repartition_batch(
                    &check_batch, pk_col, &source_schema, self.num_workers);
                p1_labels.push(P1Label::UpsertPkId);
                p1_checks.push(PipelinedCheck {
                    target_id,
                    flags: FLAG_HAS_PK,
                    col_hint: 0,
                    payload: CheckPayload::Partitioned(sub_batches),
                    schema: source_schema,
                });
            }
        }

        // ----- Phase 1 execute + interpret --------------------------------
        let mut existing_pks: HashSet<u128> = HashSet::new();
        if !p1_checks.is_empty() {
            let mut p1_results = self.execute_pipeline(&p1_checks)?;

            for (idx, label) in p1_labels.iter().enumerate() {
                match label {
                    P1Label::FkParent { parent_table_id, expected_count } => {
                        if p1_results[idx].len() < *expected_count {
                            let (sn, tn) = self.get_qualified_name_owned(target_id);
                            let (tsn, ttn) = self.get_qualified_name_owned(*parent_table_id);
                            return Err(format!(
                                "Foreign Key violation in '{}.{}': value not found in target '{}.{}'",
                                sn, tn, tsn, ttn
                            ));
                        }
                    }
                    P1Label::FkRestrict { child_tid } => {
                        if !p1_results[idx].is_empty() {
                            let (sn, tn) = self.get_qualified_name_owned(target_id);
                            let (csn, ctn) = self.get_qualified_name_owned(*child_tid);
                            return Err(format!(
                                "Foreign Key violation: cannot delete from '{}.{}', row still referenced by '{}.{}'",
                                sn, tn, csn, ctn,
                            ));
                        }
                    }
                    P1Label::UpsertPkId => {
                        existing_pks = std::mem::take(&mut p1_results[idx]);
                        // Error mode: any existing PK in the result set is
                        // a SQL-standard duplicate-key violation.
                        if matches!(mode, WireConflictMode::Error)
                            && !existing_pks.is_empty()
                        {
                            let conflict_pk = *existing_pks.iter().next().unwrap();
                            let pk_name = self.get_col_name(target_id, pki);
                            let (sn, tn) = self.get_qualified_name_owned(target_id);
                            let key_str = format_pk_value(conflict_pk, &source_schema);
                            return Err(format!(
                                "duplicate key value violates unique constraint \"{}_{}_pkey\": Key ({})=({}) already exists",
                                sn, tn, pk_name, key_str,
                            ));
                        }
                    }
                }
            }
        }

        // ----- Phase 2 plan (unique index checks) -------------------------
        if !has_unique {
            return Ok(());
        }

        // Lazily warm the unique-index filters for this table. One scan
        // seeds every filter; already-warm filters are left alone.
        self.ensure_unique_filters_warm(target_id)?;

        let mut p2_checks: Vec<PipelinedCheck> = Vec::new();
        let mut p2_labels: Vec<P2Label> = Vec::new();

        for ci in 0..n_circuits {
            let (col_idx, type_code, idx_schema) = {
                let cat = unsafe { &mut *self.catalog };
                let (col_idx, is_unique, type_code) = match cat.get_index_circuit_info(target_id, ci) {
                    Some(info) => info,
                    None => continue,
                };
                if !is_unique { continue; }
                let idx_schema = match cat.get_index_circuit_schema(target_id, ci) {
                    Some(s) => s,
                    None => continue,
                };
                (col_idx, type_code, idx_schema)
            };

            let source_col = col_idx as usize;
            let is_pk_col = source_col == pki;
            let src_payload_idx = if is_pk_col {
                usize::MAX
            } else if source_col < pki {
                source_col
            } else {
                source_col - 1
            };
            let col_size = source_schema.columns[source_col].size as usize;

            let mut upsert_keys: Vec<(u64, u64, u128)> = Vec::new();
            let mut check_lo: Vec<u64> = Vec::new();
            let mut check_hi: Vec<u64> = Vec::new();
            let mut seen: HashSet<u128> = HashSet::new();

            for i in 0..batch.count {
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
                    // UPSERT row: unique-value conflict is OK iff it's held by pk_i itself.
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

            if !check_lo.is_empty() {
                // Consult the master-side filter first: if every new
                // value is definitely absent, skip this index's broadcast.
                let filter_keys: Vec<u128> = check_lo.iter().zip(check_hi.iter())
                    .map(|(&lo, &hi)| crate::util::make_pk(lo, hi))
                    .collect();
                let skip_broadcast = self.unique_filter_all_absent(
                    target_id, col_idx, &filter_keys);

                if !skip_broadcast {
                    let chk_batch = build_check_batch(&idx_schema, &check_lo, &check_hi);
                    p2_labels.push(P2Label::NonUpsert { source_col });
                    p2_checks.push(PipelinedCheck {
                        target_id,
                        flags: FLAG_HAS_PK,
                        col_hint: (col_idx as u64) + 1,
                        payload: CheckPayload::Broadcast(chk_batch),
                        schema: idx_schema,
                    });
                }
            }

            if !upsert_keys.is_empty() {
                let u_lo: Vec<u64> = upsert_keys.iter().map(|&(lo, _, _)| lo).collect();
                let u_hi: Vec<u64> = upsert_keys.iter().map(|&(_, hi, _)| hi).collect();
                let u_batch = build_check_batch(&idx_schema, &u_lo, &u_hi);
                p2_labels.push(P2Label::Upsert { col_idx, source_col, upsert_keys });
                p2_checks.push(PipelinedCheck {
                    target_id,
                    flags: FLAG_HAS_PK,
                    col_hint: (col_idx as u64) + 1,
                    payload: CheckPayload::Broadcast(u_batch),
                    schema: idx_schema,
                });
            }
        }

        // ----- Phase 2 execute + interpret --------------------------------
        if p2_checks.is_empty() {
            return Ok(());
        }

        let p2_results = self.execute_pipeline(&p2_checks)?;

        for (idx, label) in p2_labels.iter().enumerate() {
            match label {
                P2Label::NonUpsert { source_col } => {
                    if !p2_results[idx].is_empty() {
                        let col_name = self.get_col_name(target_id, *source_col);
                        return Err(format!(
                            "Unique index violation on column '{}'", col_name));
                    }
                }
                P2Label::Upsert { col_idx, source_col, upsert_keys } => {
                    let occupied = &p2_results[idx];
                    for &(key_lo, key_hi, pk_i) in upsert_keys {
                        let key_pk = crate::util::make_pk(key_lo, key_hi);
                        if !occupied.contains(&key_pk) { continue; }
                        // Value is occupied; confirm the holder is this same row.
                        match self.fan_out_seek_by_index(target_id, *col_idx, key_lo, key_hi) {
                            Err(e) => return Err(e),
                            Ok(None) => {}
                            Ok(Some(ref found)) if found.count > 0 => {
                                if found.get_pk(0) != pk_i {
                                    let col_name = self.get_col_name(target_id, *source_col);
                                    return Err(format!(
                                        "Unique index violation on column '{}'", col_name));
                                }
                            }
                            Ok(Some(_)) => {}
                        }
                    }
                }
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
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn extract_single_batch(schema: &SchemaDescriptor, decoded: DecodedWire) -> Option<Batch> {
    decoded.data_batch.and_then(|b| {
        if b.count > 0 {
            let mut result = Batch::empty(schema.num_columns as usize - 1);
            result.schema = Some(*schema);
            result.append_batch(&b, 0, b.count);
            Some(result)
        } else {
            None
        }
    })
}

/// Render a PK u128 as a human-readable decimal for error messages.
/// Single-column PKs only; the high 64 bits are only non-zero for U128
/// PKs, which we stringify as a combined u128 decimal.
fn format_pk_value(pk: u128, schema: &SchemaDescriptor) -> String {
    let pk_col = schema.columns[schema.pk_index as usize];
    match pk_col.type_code {
        crate::schema::type_code::U128 => format!("{}", pk),
        crate::schema::type_code::I64 => format!("{}", pk as u64 as i64),
        crate::schema::type_code::I32 => format!("{}", pk as u64 as i32),
        crate::schema::type_code::I16 => format!("{}", pk as u64 as i16),
        crate::schema::type_code::I8  => format!("{}", pk as u64 as i8),
        _ => format!("{}", pk as u64),
    }
}

fn build_check_batch(schema: &SchemaDescriptor, lo_list: &[u64], hi_list: &[u64]) -> Batch {
    let npc = schema.num_columns as usize - 1;
    let mut batch = Batch::with_schema(*schema, lo_list.len());
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

#[cfg(test)]
mod unique_filter_tests {
    use super::*;
    use crate::schema::{SchemaColumn, type_code};

    fn u64_schema() -> SchemaDescriptor {
        // Single U64 PK column.
        let mut columns = [SchemaColumn::new(0, 0); 64];
        columns[0] = SchemaColumn::new(type_code::U64, 0);
        SchemaDescriptor { num_columns: 1, pk_index: 0, columns }
    }

    fn two_col_schema() -> SchemaDescriptor {
        // PK U64 at index 0, payload U64 at index 1.
        let mut columns = [SchemaColumn::new(0, 0); 64];
        columns[0] = SchemaColumn::new(type_code::U64, 0);
        columns[1] = SchemaColumn::new(type_code::U64, 1);
        SchemaDescriptor { num_columns: 2, pk_index: 0, columns }
    }

    fn make_row_batch(schema: SchemaDescriptor, rows: &[(u128, i64, u64, i64)]) -> Batch {
        // rows: (pk, weight, null_word, payload_col1_i64_value)
        let mut batch = Batch::with_schema(schema, rows.len().max(1));
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

    #[test]
    fn filter_insert_basic() {
        let mut f = UniqueFilter::new();
        f.insert(1);
        f.insert(2);
        assert!(f.values.contains(&1));
        assert!(f.values.contains(&2));
        assert!(!f.capped);
    }

    #[test]
    fn filter_cap_clears_values() {
        // Temporarily lower the effective cap by filling the filter
        // up to UNIQUE_FILTER_CAP + 1. We fill enough entries to exceed
        // the cap, verify the filter flips to capped, and its values
        // HashSet is cleared.
        let mut f = UniqueFilter::new();
        // Insert slightly above the cap; each insert is cheap (u128).
        for k in 0..(UNIQUE_FILTER_CAP as u128 + 2) {
            f.insert(k);
            if f.capped { break; }
        }
        assert!(f.capped, "filter should be capped after exceeding the limit");
        assert!(f.values.is_empty(), "values cleared once capped");
        // Further inserts are no-ops.
        f.insert(99999999);
        assert!(f.values.is_empty());
    }

    #[test]
    fn extract_into_filter_pk_col() {
        // Schema: PK-only U64. Test that is_pk_col=true path extracts PKs.
        let schema = u64_schema();
        let batch = make_row_batch(schema, &[
            (10, 1, 0, 0),
            (20, 1, 0, 0),
            (30, -1, 0, 0),  // delete row — should be skipped
        ]);
        let desc = UniqueIndexDesc {
            col_idx: 0,
            type_code: type_code::U64,
            is_pk_col: true,
            src_payload_idx: usize::MAX,
            col_size: 8,
        };
        let mut filter = UniqueFilter::new();
        extract_into_filter(&mut filter, &batch, &desc);
        assert!(filter.values.contains(&10u128));
        assert!(filter.values.contains(&20u128));
        assert!(!filter.values.contains(&30u128), "negative weight skipped");
    }

    #[test]
    fn extract_into_filter_payload_col_skips_nulls() {
        // Schema: PK U64, payload U64 (nullable). Test extraction by col 1.
        let schema = two_col_schema();
        let batch = make_row_batch(schema, &[
            (1, 1, 0, 100),  // payload=100, not null
            (2, 1, 1, 200),  // null bit set → should be skipped
            (3, 1, 0, 300),
        ]);
        let desc = UniqueIndexDesc {
            col_idx: 1,
            type_code: type_code::U64,
            is_pk_col: false,
            src_payload_idx: 0, // first payload column
            col_size: 8,
        };
        let mut filter = UniqueFilter::new();
        extract_into_filter(&mut filter, &batch, &desc);
        assert!(filter.values.contains(&100u128));
        assert!(!filter.values.contains(&200u128), "null values skipped");
        assert!(filter.values.contains(&300u128));
        assert_eq!(filter.values.len(), 2);
    }

    #[test]
    fn extract_into_filter_respects_capped() {
        let schema = u64_schema();
        let batch = make_row_batch(schema, &[
            (10, 1, 0, 0),
            (20, 1, 0, 0),
        ]);
        let desc = UniqueIndexDesc {
            col_idx: 0,
            type_code: type_code::U64,
            is_pk_col: true,
            src_payload_idx: usize::MAX,
            col_size: 8,
        };
        let mut filter = UniqueFilter::new();
        filter.capped = true;
        extract_into_filter(&mut filter, &batch, &desc);
        assert!(filter.values.is_empty(), "no-op on capped filter");
    }
}
