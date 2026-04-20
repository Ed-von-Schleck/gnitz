//! Master-side dispatcher: fans out push/scan operations to worker processes
//! via the shared append-only log (SAL) and collects responses via per-worker
//! W2M regions. Eventfds provide cross-process signaling.

use std::collections::HashMap;
use std::collections::HashSet;
use std::rc::Rc;

use crate::catalog::CatalogEngine;
use crate::schema::SchemaDescriptor;
use crate::schema::promote_to_index_key;
use crate::ipc::{
    FLAG_SHUTDOWN, FLAG_DDL_SYNC, FLAG_EXCHANGE, FLAG_EXCHANGE_RELAY, FLAG_PUSH, FLAG_HAS_PK,
    FLAG_SEEK, FLAG_SEEK_BY_INDEX, FLAG_BACKFILL,
    FLAG_TICK, FLAG_FLUSH, FLAG_CONFLICT_MODE_PRESENT, WireConflictMode, DecodedWire,
    SalWriter, W2mReceiver,
};
use crate::reactor::PendingRelay;
use crate::storage::{Batch, partition_for_key};
use crate::ops::{
    PartitionRouter, op_repartition_batch, op_relay_scatter,
    worker_for_partition_pub,
};

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
    name_bytes: Rc<[Vec<u8>]>,
}

// ---------------------------------------------------------------------------
// MasterDispatcher
// ---------------------------------------------------------------------------

pub struct MasterDispatcher {
    num_workers: usize,
    worker_pids: Vec<i32>,
    sal: SalWriter,
    w2m: Rc<W2mReceiver>,
    // Catalog pointer — reborrowed per-call because &mut self borrows conflict.
    catalog: *mut CatalogEngine,
    router: PartitionRouter,
    /// Per-(table_id, unique_col_idx) filter skipping redundant Phase 2
    /// unique-index broadcasts. See the UniqueFilter comment block above.
    unique_filters: HashMap<(i64, u32), UniqueFilter>,

    // Cache: table_id → (schema, col_name_bytes).
    // Shared via Rc so cache hits cost a refcount bump, not a deep clone of
    // the name bytes. Populated lazily; table schemas are immutable.
    schema_names_cache: HashMap<i64, (SchemaDescriptor, Rc<[Vec<u8>]>)>,
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
            w2m: Rc::new(w2m),
            catalog,
            router: PartitionRouter::new(),
            unique_filters: HashMap::new(),
            schema_names_cache: HashMap::new(),
        }
    }

    pub fn reset_sal(&mut self, write_cursor: u64, epoch: u32) {
        self.sal.reset(write_cursor, epoch);
    }

    fn get_schema_and_names(&mut self, target_id: i64) -> (SchemaDescriptor, Rc<[Vec<u8>]>) {
        let cat_ptr = self.catalog;
        let entry = self.schema_names_cache.entry(target_id).or_insert_with(|| {
            let cat = unsafe { &mut *cat_ptr };
            let schema = cat.get_schema_desc(target_id)
                .unwrap_or_else(|| panic!("master: no schema for target_id={}", target_id));
            let names: Rc<[Vec<u8>]> = cat.get_column_names(target_id)
                .into_iter()
                .map(String::into_bytes)
                .collect();
            (schema, names)
        });
        (entry.0, Rc::clone(&entry.1))
    }

    // -----------------------------------------------------------------------
    // Core send/receive helpers
    // -----------------------------------------------------------------------

    /// Encode per-worker data directly into SAL mmap.
    /// Does NOT fdatasync or signal.
    ///
    /// Sync callers pass `request_id=0` (or some single id) which is
    /// replicated across all workers. Async callers use
    /// `write_group_with_req_ids` to thread distinct per-worker ids.
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
        request_id: u64,
        unicast_worker: i32,
    ) -> Result<(), String> {
        let nw = self.num_workers;
        let mut ids = [0u64; crate::ipc::MAX_WORKERS];
        for w in 0..nw { ids[w] = request_id; }
        self.write_group_with_req_ids(
            target_id, flags, worker_batches, schema, col_names,
            seek_pk_lo, seek_pk_hi, seek_col_idx, &ids[..nw], unicast_worker,
        )
    }

    /// Encode per-worker data with per-worker request ids. Used by async
    /// fan-outs that need distinct ids per worker for reply routing.
    fn write_group_with_req_ids(
        &mut self,
        target_id: i64,
        flags: u32,
        worker_batches: &[Option<&Batch>],
        schema: &SchemaDescriptor,
        col_names: &[Vec<u8>],
        seek_pk_lo: u64,
        seek_pk_hi: u64,
        seek_col_idx: u64,
        req_ids: &[u64],
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
            seek_pk_lo, seek_pk_hi, seek_col_idx, req_ids, unicast_worker,
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
        request_id: u64,
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
            seek_pk_lo, seek_pk_hi, seek_col_idx, request_id,
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
        request_id: u64,
    ) -> Result<(), String> {
        self.write_broadcast(target_id, flags, batch, schema, col_names,
                            seek_pk_lo, seek_pk_hi, seek_col_idx, request_id)?;
        self.signal_all();
        Ok(())
    }

    pub(crate) fn signal_all(&self) { self.sal.signal_all(); }
    fn signal_one(&self, worker: usize) { self.sal.signal_one(worker); }

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
        request_id: u64,
    ) -> Result<(), String> {
        self.write_group(target_id, flags, worker_batches, schema, col_names,
                         seek_pk_lo, seek_pk_hi, seek_col_idx, request_id, -1)?;
        self.signal_all();
        Ok(())
    }

    /// Expose the shared W2M handle so the executor can attach it to the
    /// reactor for async fan-out reply routing.
    pub fn w2m_handle(&self) -> Rc<W2mReceiver> {
        Rc::clone(&self.w2m)
    }

    /// Wait for one response from each worker. Bootstrap-only: runs
    /// before the reactor is up, so we drive each worker's ring via
    /// `W2mReceiver::wait_for` (sync FUTEX_WAIT on `reader_seq`). The
    /// tail-chasing ring self-maintains — no reset needed.
    fn wait_all_workers(&mut self) -> Result<Vec<Option<DecodedWire>>, String> {
        let nw = self.num_workers;
        let mut results: Vec<Option<DecodedWire>> = (0..nw).map(|_| None).collect();
        for w in 0..nw {
            loop {
                match self.w2m.try_read(w) {
                    Some(decoded) => {
                        if decoded.control.status != 0 {
                            let msg = String::from_utf8_lossy(&decoded.control.error_msg);
                            return Err(format!("worker {}: {}", w, msg));
                        }
                        results[w] = Some(decoded);
                        break;
                    }
                    None => {
                        let _ = self.w2m.wait_for(w, 1000);
                    }
                }
            }
        }
        Ok(results)
    }

    pub fn collect_acks(&mut self) -> Result<(), String> {
        self.wait_all_workers()?;
        Ok(())
    }

    pub(crate) fn num_workers(&self) -> usize { self.num_workers }

    /// Collect ACKs from all workers, relaying exchange messages
    /// inline. Bootstrap-only: called by `fan_out_backfill` before the
    /// reactor is up, so we walk each ring serially with
    /// `W2mReceiver::wait_for`. Maintains its own ExchangeAccumulator
    /// since the reactor's is not yet wired.
    fn collect_acks_and_relay(&mut self, _target_id: i64) -> Result<(), String> {
        let nw = self.num_workers;
        let mut collected = vec![false; nw];
        let mut remaining = nw;
        let mut acc = crate::reactor::ExchangeAccumulator::new(nw);

        while remaining > 0 {
            // One full pass over all workers per iteration. If a pass
            // makes no progress, we wait_for on the first still-active
            // worker. Exchange replies from any worker may trigger
            // further SAL writes + replies, so we loop broadly.
            let mut progressed = false;
            for w in 0..nw {
                if collected[w] { continue; }
                let Some(decoded) = self.w2m.try_read(w) else {
                    continue;
                };
                progressed = true;
                if (decoded.control.flags as u32) & FLAG_EXCHANGE != 0 {
                    if let Some(relay) = acc.process(w, decoded) {
                        let prep = self.prepare_relay(relay)?;
                        self.emit_relay(prep)?;
                    }
                } else {
                    if decoded.control.status != 0 {
                        let msg = String::from_utf8_lossy(&decoded.control.error_msg);
                        return Err(format!("worker {}: {}", w, msg));
                    }
                    collected[w] = true;
                    remaining -= 1;
                }
            }
            if !progressed {
                // Wait for the first still-active worker to publish.
                if let Some(next) = (0..nw).find(|&w| !collected[w]) {
                    let _ = self.w2m.wait_for(next, 1000);
                }
            }
        }
        Ok(())
    }

    // -----------------------------------------------------------------------
    // SAL Checkpoint
    // -----------------------------------------------------------------------

    /// Invariant: callers must live on a path that owns SAL checkpoint
    /// exclusivity. Today that is the bootstrap backfill and the
    /// committer task. Async fan-out / tick / DDL paths must NOT call
    /// this — a concurrent FLAG_FLUSH races the committer's own and
    /// orphans SAL writes straddling `sal.checkpoint_reset`. See
    /// async-invariants.md §III.3a.
    pub(crate) fn maybe_checkpoint(&mut self) -> Result<(), String> {
        if !self.sal.needs_checkpoint() {
            return Ok(());
        }
        self.do_checkpoint()
    }

    fn do_checkpoint(&mut self) -> Result<(), String> {
        let schema = SchemaDescriptor::minimal_u64();
        self.send_broadcast(0, FLAG_FLUSH, None, &schema, &[], 0, 0, 0, 0)?;
        self.collect_acks()?;
        self.checkpoint_post_ack();
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Exchange relay
    // -----------------------------------------------------------------------

    /// CPU-only first half of exchange relay: looks up shard columns via
    /// the catalog DAG, scatters the payloads into per-worker batches,
    /// and collects column names. No SAL write yet — `relay_loop` runs
    /// this without `sal_writer_excl` so the lock covers only the
    /// synchronous SAL write in `emit_relay`.
    pub(crate) fn prepare_relay(&mut self, relay: PendingRelay) -> Result<RelayPrepared, String> {
        let remaining = self.sal.mmap_size() - self.sal.cursor();
        if remaining < (self.sal.mmap_size() >> 3) {
            return Err(format!(
                "SAL space exhausted during exchange relay ({} bytes left)", remaining));
        }

        // Only `Relay` ever reaches here — `Fence` is handled in the
        // relay loop and never forwarded to prepare_relay.
        let (view_id, payloads, schema, source_id) = match relay {
            PendingRelay::Relay { view_id, payloads, schema, source_id } =>
                (view_id, payloads, schema, source_id),
            PendingRelay::Fence { .. } => {
                return Err("prepare_relay called with Fence variant".into());
            }
        };

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

        let (_, name_bytes) = self.get_schema_and_names(view_id);

        Ok(RelayPrepared { view_id, source_id, dest_batches, schema, name_bytes })
    }

    /// Synchronous second half: writes the FLAG_EXCHANGE_RELAY group to
    /// SAL and signals workers. Caller holds `sal_writer_excl` for the
    /// duration of this call; no awaits inside.
    pub(crate) fn emit_relay(&mut self, prep: RelayPrepared) -> Result<(), String> {
        let RelayPrepared { view_id, source_id, dest_batches, schema, name_bytes } = prep;
        let refs: Vec<Option<&Batch>> = dest_batches.iter()
            .map(|b| if b.count > 0 { Some(b) } else { None })
            .collect();
        // Echo `source_id` back via `seek_pk_lo` so the worker's
        // `do_exchange_wait` can match on (view_id, source_id). Without
        // this, a multi-source view (join over 2+ tables) can deliver
        // the wrong source's relay to a waiting exchange and the worker
        // demuxes against the wrong sharding columns.
        self.send_to_workers(view_id, FLAG_EXCHANGE_RELAY, &refs, &schema, &name_bytes,
                              source_id as u64, 0, 0, 0)
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

    pub fn fan_out_backfill(&mut self, view_id: i64, source_id: i64) -> Result<(), String> {
        self.maybe_checkpoint()?;
        let (schema, col_names) = self.get_schema_and_names(source_id);
        self.send_broadcast(source_id, FLAG_BACKFILL, None, &schema, &col_names,
                           view_id as u64, 0, 0, 0)?;
        self.collect_acks_and_relay(source_id)
    }

    // Async fan-outs take `*mut Self` instead of `&mut self` because the
    // exclusive borrow must end before `.await`: other reactor tasks
    // re-enter the dispatcher in the meantime. Only call from inside a
    // reactor task driven by `block_until_idle`.

    pub async fn fan_out_seek_async(
        disp_ptr: *mut MasterDispatcher,
        reactor: &crate::reactor::Reactor,
        target_id: i64, pk_lo: u64, pk_hi: u64,
    ) -> Result<Option<Batch>, String> {
        let worker = unsafe {
            let pk = crate::util::make_pk(pk_lo, pk_hi);
            worker_for_partition_pub(partition_for_key(pk), (*disp_ptr).num_workers)
        };
        single_worker_async(disp_ptr, reactor, target_id, FLAG_SEEK,
                            worker, pk_lo, pk_hi, 0, "seek").await
    }

    pub async fn fan_out_seek_by_index_async(
        disp_ptr: *mut MasterDispatcher,
        reactor: &crate::reactor::Reactor,
        target_id: i64, col_idx: u32, key_lo: u64, key_hi: u64,
    ) -> Result<Option<Batch>, String> {
        let cached = unsafe {
            (*disp_ptr).router.worker_for_index_key(
                target_id as u32, col_idx, key_lo,
            )
        };
        if cached >= 0 {
            let worker = cached as usize;
            return single_worker_async(
                disp_ptr, reactor, target_id, FLAG_SEEK_BY_INDEX,
                worker, key_lo, key_hi, col_idx as u64, "seek_by_index",
            ).await;
        }

        // Cache miss: broadcast to all workers with per-worker req_ids.
        let schema = unsafe { (*disp_ptr).get_schema_and_names(target_id).0 };
        let decoded_vec = dispatch_fanout(disp_ptr, reactor, |disp, req_ids| {
            let (schema, col_names) = disp.get_schema_and_names(target_id);
            disp.write_group_with_req_ids(
                target_id, FLAG_SEEK_BY_INDEX, &[], &schema, &col_names,
                key_lo, key_hi, col_idx as u64, req_ids, -1,
            )
        }).await?;
        if let Some(e) = first_worker_error("seek_by_index", &decoded_vec) {
            return Err(e);
        }
        for decoded in decoded_vec {
            if let Some(ref batch) = decoded.data_batch {
                if batch.count > 0 {
                    return Ok(extract_single_batch(&schema, decoded));
                }
            }
        }
        Ok(None)
    }

    pub async fn fan_out_scan_async(
        disp_ptr: *mut MasterDispatcher,
        reactor: &crate::reactor::Reactor,
        target_id: i64,
    ) -> Result<Option<Batch>, String> {
        let schema = unsafe { (*disp_ptr).get_schema_and_names(target_id).0 };
        let decoded_vec = dispatch_fanout(disp_ptr, reactor, |disp, req_ids| {
            let (schema, col_names) = disp.get_schema_and_names(target_id);
            disp.write_group_with_req_ids(
                target_id, 0, &[], &schema, &col_names,
                0, 0, 0, req_ids, -1,
            )
        }).await?;
        if let Some(e) = first_worker_error("scan", &decoded_vec) {
            return Err(e);
        }
        let total_rows: usize = decoded_vec.iter()
            .filter_map(|d| d.data_batch.as_ref())
            .map(|b| b.count)
            .sum();
        let mut out = Batch::with_schema(schema, total_rows.max(1));
        for decoded in &decoded_vec {
            if let Some(ref batch) = decoded.data_batch {
                if batch.count > 0 {
                    out.append_batch(batch, 0, batch.count);
                }
            }
        }
        if out.count == 0 { Ok(None) } else { Ok(Some(out)) }
    }

    /// Async version of `execute_pipeline`. Writes each check with
    /// per-worker req_ids, signals once, and joins all replies.
    async fn execute_pipeline_async(
        disp_ptr: *mut MasterDispatcher,
        reactor: &crate::reactor::Reactor,
        checks: Vec<PipelinedCheck>,
    ) -> Result<Vec<HashSet<u128>>, String> {
        let num_checks = checks.len();
        if num_checks == 0 {
            return Ok(Vec::new());
        }

        let (nw, mut all_req_ids): (usize, Vec<Vec<u64>>) = unsafe {
            let disp = &mut *disp_ptr;
            let nw = disp.num_workers;
            let mut rids: Vec<Vec<u64>> = Vec::with_capacity(num_checks);
            for _ in 0..num_checks {
                rids.push((0..nw).map(|_| reactor.alloc_request_id()).collect());
            }
            for (idx, check) in checks.iter().enumerate() {
                let refs: Vec<Option<&Batch>> = match &check.payload {
                    CheckPayload::Broadcast(batch) => (0..nw).map(|_| Some(batch)).collect(),
                    CheckPayload::Partitioned(batches) => batches.iter()
                        .map(|b| if b.count > 0 { Some(b) } else { None })
                        .collect(),
                };
                disp.write_group_with_req_ids(
                    check.target_id, check.flags, &refs,
                    &check.schema, &[], 0, 0, check.col_hint,
                    &rids[idx], -1,
                )?;
            }
            disp.signal_all();
            (nw, rids)
        };

        // Flatten futures in (check_idx, worker) order.
        let mut futs = Vec::with_capacity(num_checks * nw);
        for rids in all_req_ids.drain(..) {
            for rid in rids {
                futs.push(reactor.await_reply(rid));
            }
        }
        let decoded_vec: Vec<DecodedWire> = crate::reactor::join_all(futs).await;

        let mut results: Vec<HashSet<u128>> = checks.iter().map(|check| {
            let cap = match &check.payload {
                CheckPayload::Broadcast(b) => b.count,
                CheckPayload::Partitioned(bs) => bs.iter().map(|b| b.count).sum(),
            };
            HashSet::with_capacity(cap)
        }).collect();

        if let Some(err) = first_worker_error("pipeline", &decoded_vec) {
            return Err(err);
        }
        for check_idx in 0..num_checks {
            for w in 0..nw {
                let decoded = &decoded_vec[check_idx * nw + w];
                if let Some(ref batch) = decoded.data_batch {
                    for j in 0..batch.count {
                        if batch.get_weight(j) == 1 {
                            results[check_idx].insert(batch.get_pk(j));
                        }
                    }
                }
            }
        }
        Ok(results)
    }

    pub fn broadcast_ddl(&mut self, target_id: i64, batch: &Batch) -> Result<(), String> {
        let (schema, col_names) = self.get_schema_and_names(target_id);
        self.write_broadcast(target_id, FLAG_DDL_SYNC, Some(batch), &schema, &col_names,
                            0, 0, 0, 0)?;
        self.signal_all();
        gnitz_debug!("broadcast_ddl tid={} rows={}", target_id, batch.count);
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Tick group writer (used by the async tick task in executor.rs)
    // -----------------------------------------------------------------------

    /// Write a FLAG_TICK group for `tid` with per-worker req_ids. Does
    /// NOT signal — the caller batches multiple `write_tick_group` calls
    /// followed by a single `signal_all` (IV.6). The underlying SAL
    /// encoder reuses `write_group_with_req_ids`; per-worker slots all
    /// carry the corresponding req_id from `req_ids[w]`.
    pub(crate) fn write_tick_group(
        &mut self, tid: i64, req_ids: &[u64],
    ) -> Result<(), String> {
        let (schema, col_names) = self.get_schema_and_names(tid);
        self.write_group_with_req_ids(
            tid, FLAG_TICK, &[], &schema, &col_names,
            0, 0, 0, req_ids, -1,
        )
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
        let _ = self.send_broadcast(0, FLAG_SHUTDOWN, None, &schema, &[], 0, 0, 0, 0);
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


    /// Async equivalent of `validate_all_distributed`. Identical semantics;
    /// uses `execute_pipeline_async` + `fan_out_seek_by_index_async` so it
    /// runs without blocking the reactor.
    pub async fn validate_all_distributed_async(
        disp_ptr: *mut MasterDispatcher,
        reactor: &crate::reactor::Reactor,
        target_id: i64, batch: &Batch, mode: WireConflictMode,
    ) -> Result<(), String> {
        let (n_fk, n_children, n_circuits, has_unique, unique_pk, source_schema, num_workers) = unsafe {
            let disp = &mut *disp_ptr;
            let cat = &mut *disp.catalog;
            let n_fk = cat.get_fk_count(target_id);
            let n_children = cat.get_fk_children_count(target_id);
            let n_circuits = cat.get_index_circuit_count(target_id);
            let has_unique = cat.has_any_unique_index(target_id);
            let unique_pk = cat.table_has_unique_pk(target_id);
            let source_schema = cat.get_schema_desc(target_id)
                .ok_or_else(|| format!(
                    "validate_all_distributed: no schema for table {}", target_id))?;
            (n_fk, n_children, n_circuits, has_unique, unique_pk, source_schema, disp.num_workers)
        };

        let needs_pk_rejection = matches!(mode, WireConflictMode::Error) && unique_pk;

        if n_fk == 0 && n_children == 0 && !has_unique && !needs_pk_rejection {
            return Ok(());
        }

        let pki = source_schema.pk_index as usize;

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

        if needs_pk_rejection {
            for (&pk, &(_, pos_count)) in &pk_agg {
                if pos_count > 1 {
                    let (pk_name, sn, tn) = unsafe {
                        let disp = &mut *disp_ptr;
                        (disp.get_col_name(target_id, pki),
                         disp.get_qualified_name_owned(target_id).0,
                         disp.get_qualified_name_owned(target_id).1)
                    };
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

        for fi in 0..n_fk {
            let (fk_col_idx, parent_table_id, col_type, parent_schema) = unsafe {
                let disp = &mut *disp_ptr;
                let cat = &mut *disp.catalog;
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
                &check_batch, pk_col, &parent_schema, num_workers);

            p1_labels.push(P1Label::FkParent { parent_table_id, expected_count });
            p1_checks.push(PipelinedCheck {
                target_id: parent_table_id,
                flags: FLAG_HAS_PK,
                col_hint: 0,
                payload: CheckPayload::Partitioned(sub_batches),
                schema: parent_schema,
            });
        }

        if n_children > 0 {
            let mut neg_pks: HashSet<u128> = HashSet::new();
            for i in 0..batch.count {
                if batch.get_weight(i) < 0 {
                    neg_pks.insert(batch.get_pk(i));
                }
            }
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
                    let (child_tid, fk_col_idx, idx_schema) = unsafe {
                        let disp = &mut *disp_ptr;
                        let cat = &mut *disp.catalog;
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
                    &check_batch, pk_col, &source_schema, num_workers);
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
            let mut p1_results = Self::execute_pipeline_async(disp_ptr, reactor, p1_checks).await?;

            for (idx, label) in p1_labels.iter().enumerate() {
                match label {
                    P1Label::FkParent { parent_table_id, expected_count } => {
                        if p1_results[idx].len() < *expected_count {
                            let (sn, tn, tsn, ttn) = unsafe {
                                let disp = &mut *disp_ptr;
                                let (s, t) = disp.get_qualified_name_owned(target_id);
                                let (ts, tt) = disp.get_qualified_name_owned(*parent_table_id);
                                (s, t, ts, tt)
                            };
                            return Err(format!(
                                "Foreign Key violation in '{}.{}': value not found in target '{}.{}'",
                                sn, tn, tsn, ttn
                            ));
                        }
                    }
                    P1Label::FkRestrict { child_tid } => {
                        if !p1_results[idx].is_empty() {
                            let (sn, tn, csn, ctn) = unsafe {
                                let disp = &mut *disp_ptr;
                                let (s, t) = disp.get_qualified_name_owned(target_id);
                                let (cs, ct) = disp.get_qualified_name_owned(*child_tid);
                                (s, t, cs, ct)
                            };
                            return Err(format!(
                                "Foreign Key violation: cannot delete from '{}.{}', row still referenced by '{}.{}'",
                                sn, tn, csn, ctn,
                            ));
                        }
                    }
                    P1Label::UpsertPkId => {
                        existing_pks = std::mem::take(&mut p1_results[idx]);
                        if matches!(mode, WireConflictMode::Error)
                            && !existing_pks.is_empty()
                        {
                            let conflict_pk = *existing_pks.iter().next().unwrap();
                            let (pk_name, sn, tn) = unsafe {
                                let disp = &mut *disp_ptr;
                                let pkn = disp.get_col_name(target_id, pki);
                                let (s, t) = disp.get_qualified_name_owned(target_id);
                                (pkn, s, t)
                            };
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

        // Lazily warm the unique-index filters. In the async path this
        // may trigger a scan; the scan is itself async.
        Self::ensure_unique_filters_warm_async(disp_ptr, reactor, target_id).await?;

        let mut p2_checks: Vec<PipelinedCheck> = Vec::new();
        let mut p2_labels: Vec<P2Label> = Vec::new();

        for ci in 0..n_circuits {
            let (col_idx, type_code, idx_schema) = unsafe {
                let disp = &mut *disp_ptr;
                let cat = &mut *disp.catalog;
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
                    upsert_keys.push((key_lo, key_hi, pk_i));
                    continue;
                }

                let key_pk = crate::util::make_pk(key_lo, key_hi);
                if seen.contains(&key_pk) {
                    let col_name = unsafe { (*disp_ptr).get_col_name(target_id, source_col) };
                    return Err(format!(
                        "Unique index violation on column '{}': duplicate in batch", col_name));
                }
                seen.insert(key_pk);
                check_lo.push(key_lo);
                check_hi.push(key_hi);
            }

            if !check_lo.is_empty() {
                let filter_keys: Vec<u128> = check_lo.iter().zip(check_hi.iter())
                    .map(|(&lo, &hi)| crate::util::make_pk(lo, hi))
                    .collect();
                let skip_broadcast = unsafe {
                    (*disp_ptr).unique_filter_all_absent(target_id, col_idx, &filter_keys)
                };

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

        if p2_checks.is_empty() {
            return Ok(());
        }

        let p2_results = Self::execute_pipeline_async(disp_ptr, reactor, p2_checks).await?;

        for (idx, label) in p2_labels.iter().enumerate() {
            match label {
                P2Label::NonUpsert { source_col } => {
                    if !p2_results[idx].is_empty() {
                        let col_name = unsafe { (*disp_ptr).get_col_name(target_id, *source_col) };
                        return Err(format!(
                            "Unique index violation on column '{}'", col_name));
                    }
                }
                P2Label::Upsert { col_idx, source_col, upsert_keys } => {
                    let occupied = &p2_results[idx];
                    for &(key_lo, key_hi, pk_i) in upsert_keys {
                        let key_pk = crate::util::make_pk(key_lo, key_hi);
                        if !occupied.contains(&key_pk) { continue; }
                        match Self::fan_out_seek_by_index_async(
                            disp_ptr, reactor, target_id, *col_idx, key_lo, key_hi,
                        ).await {
                            Err(e) => return Err(e),
                            Ok(None) => {}
                            Ok(Some(ref found)) if found.count > 0 => {
                                if found.get_pk(0) != pk_i {
                                    let col_name = unsafe {
                                        (*disp_ptr).get_col_name(target_id, *source_col)
                                    };
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

    /// Async version of `ensure_unique_filters_warm`. Feeds each worker's
    /// scan reply directly into the filter(s) instead of concatenating
    /// them into a single master-side `Batch` the way `fan_out_scan_async`
    /// does — on a table with tens of millions of rows the concatenation
    /// step would peak at ~2× the total scan size and risk OOM.
    ///
    /// We intentionally go through `join_all` rather than awaiting each
    /// `await_reply` serially: `join_all`'s first poll registers every
    /// `req_id`'s waker before any worker reply can arrive, while a
    /// serial await would register them one at a time and `route_reply`
    /// would drop any reply whose waker isn't yet in `reply_wakers`
    /// (and forget to decrement `in_flight[w]`, stalling the W2M ring).
    /// The peak we do pay is one `DecodedWire` per worker during the
    /// processing loop, not the full concatenated batch.
    async fn ensure_unique_filters_warm_async(
        disp_ptr: *mut MasterDispatcher,
        reactor: &crate::reactor::Reactor,
        table_id: i64,
    ) -> Result<(), String> {
        let missing: Vec<UniqueIndexDesc> = unsafe {
            let disp = &mut *disp_ptr;
            let (_schema, descs) = match disp.unique_index_descriptors(table_id) {
                Some(x) => x,
                None => return Ok(()),
            };
            let missing: Vec<UniqueIndexDesc> = descs.into_iter()
                .filter(|d| !disp.unique_filters.contains_key(&(table_id, d.col_idx)))
                .collect();
            if missing.is_empty() { return Ok(()); }
            for d in &missing {
                disp.unique_filters.insert((table_id, d.col_idx), UniqueFilter::new());
            }
            missing
        };

        let decoded_vec = dispatch_fanout(disp_ptr, reactor, |disp, req_ids| {
            let (schema, col_names) = disp.get_schema_and_names(table_id);
            disp.write_group_with_req_ids(
                table_id, 0, &[], &schema, &col_names,
                0, 0, 0, req_ids, -1,
            )
        }).await?;

        if let Some(e) = first_worker_error("scan", &decoded_vec) {
            return Err(e);
        }

        unsafe {
            let disp = &mut *disp_ptr;
            for decoded in decoded_vec {
                let batch = match decoded.data_batch {
                    Some(b) if b.count > 0 => b,
                    _ => continue,
                };
                for d in &missing {
                    if let Some(filter) = disp.unique_filters.get_mut(&(table_id, d.col_idx)) {
                        if !filter.capped {
                            extract_into_filter(filter, &batch, d);
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Commit N push batches as a single SAL group write. Called from
    /// the committer task. Returns (groups, req_ids, fsync_id) — the
    /// caller awaits fsync + per-worker req_ids separately so they can
    /// `join!` them.
    pub(crate) fn write_commit_group(
        &mut self,
        target_id: i64, batch: &Batch, mode: WireConflictMode,
        req_ids: &[u64],
    ) -> Result<(), String> {
        let (schema, col_names) = self.get_schema_and_names(target_id);
        let pk_col = &[schema.pk_index];
        let sub_batches = op_repartition_batch(batch, pk_col, &schema, self.num_workers);
        self.record_index_routing(target_id, &schema, &sub_batches);
        let refs: Vec<Option<&Batch>> = sub_batches.iter()
            .map(|b| if b.count > 0 { Some(b) } else { None })
            .collect();
        self.write_group_with_req_ids(
            target_id, FLAG_PUSH | FLAG_CONFLICT_MODE_PRESENT, &refs,
            &schema, &col_names, 0, 0, mode.as_u8() as u64, req_ids, -1,
        )
    }

    /// Write a FLAG_FLUSH checkpoint group with per-worker req_ids.
    /// Does NOT sync/signal. Caller signals + awaits replies.
    pub(crate) fn write_checkpoint_group(
        &mut self, req_ids: &[u64],
    ) -> Result<(), String> {
        let schema = SchemaDescriptor::minimal_u64();
        // One "slot" per worker with empty batch — each worker replies
        // after flushing its system tables and advancing its epoch.
        let refs: Vec<Option<&Batch>> = (0..self.num_workers).map(|_| None).collect();
        self.write_group_with_req_ids(
            0, FLAG_FLUSH, &refs, &schema, &[], 0, 0, 0, req_ids, -1,
        )
    }

    /// Post-ACK checkpoint cleanup: flush system tables before resetting
    /// the SAL cursor (their data lives in SAL entries about to be
    /// discarded), then advance the epoch. Called by both the bootstrap
    /// sync path (`do_checkpoint`) and the async committer after it
    /// collects FLAG_FLUSH ACKs.
    pub(crate) fn checkpoint_post_ack(&mut self) {
        let cat = unsafe { &mut *self.catalog };
        cat.flush_all_system_tables();
        self.sal.checkpoint_reset();
        gnitz_info!("SAL checkpoint epoch={}", self.sal.epoch());
    }

    /// Accessor for the committer. True when the SAL write cursor has
    /// crossed the configured checkpoint threshold.
    pub fn sal_needs_checkpoint(&self) -> bool {
        self.sal.needs_checkpoint()
    }

    /// Get the schema descriptor for a target_id. Panics if the table
    /// has no schema (committer should only see tables that validated).
    pub fn schema_desc_for(&mut self, target_id: i64) -> SchemaDescriptor {
        self.get_schema_and_names(target_id).0
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

/// Return the first `worker N: <op>: <msg>` error in `decoded` (worker-index
/// order), or `None` if every reply has status 0. Shared by every fan-out
/// site that emits per-worker req_ids and decodes their replies in order.
pub(crate) fn first_worker_error(op: &str, decoded: &[DecodedWire])
    -> Option<String>
{
    for (w, d) in decoded.iter().enumerate() {
        if d.control.status != 0 {
            let msg = String::from_utf8_lossy(&d.control.error_msg);
            return Some(format!("worker {}: {}: {}", w, op, msg));
        }
    }
    None
}

/// Allocate `nw` per-worker request ids, run `submit`, increment in-flight
/// counters, signal all workers, then await every reply via `join_all`.
/// Centralises the write + signal + await sequence every async fan-out
/// repeats. Returns decoded replies in worker order.
pub(crate) async fn dispatch_fanout<F>(
    disp_ptr: *mut MasterDispatcher,
    reactor: &crate::reactor::Reactor,
    submit: F,
) -> Result<Vec<DecodedWire>, String>
where
    F: FnOnce(&mut MasterDispatcher, &[u64]) -> Result<(), String>,
{
    let nw = unsafe { (*disp_ptr).num_workers };
    let req_ids: Vec<u64> = (0..nw).map(|_| reactor.alloc_request_id()).collect();
    unsafe {
        let disp = &mut *disp_ptr;
        submit(disp, &req_ids)?;
        disp.signal_all();
    }
    let futs: Vec<_> = req_ids.iter().map(|&id| reactor.await_reply(id)).collect();
    Ok(crate::reactor::join_all(futs).await)
}

/// Common body for every single-worker async fan-out. Submits the SAL
/// message, signals the worker, yields, and returns the decoded reply.
/// The closures in the public wrappers compute the worker; everything
/// else is here so there is a single place to maintain the unsafe
/// borrow-and-release pattern.
async fn single_worker_async(
    disp_ptr: *mut MasterDispatcher,
    reactor: &crate::reactor::Reactor,
    target_id: i64,
    flags: u32,
    worker: usize,
    seek_pk_lo: u64, seek_pk_hi: u64, seek_col_idx: u64,
    op_name: &'static str,
) -> Result<Option<Batch>, String> {
    let (schema, req_id) = unsafe {
        let disp = &mut *disp_ptr;
        let (schema, col_names) = disp.get_schema_and_names(target_id);
        let req_id = reactor.alloc_request_id();
        disp.write_group(target_id, flags, &[], &schema, &col_names,
                         seek_pk_lo, seek_pk_hi, seek_col_idx, req_id, worker as i32)?;
        disp.signal_one(worker);
        (schema, req_id)
    };
    let decoded = reactor.await_reply(req_id).await;
    if decoded.control.status != 0 {
        let msg = String::from_utf8_lossy(&decoded.control.error_msg);
        return Err(format!("worker {}: {}: {}", worker, op_name, msg));
    }
    Ok(extract_single_batch(&schema, decoded))
}

fn extract_single_batch(schema: &SchemaDescriptor, decoded: DecodedWire) -> Option<Batch> {
    decoded.data_batch.and_then(|b| {
        if b.count > 0 {
            let mut result = Batch::with_schema(*schema, b.count);
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
