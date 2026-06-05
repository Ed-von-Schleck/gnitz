//! Master-side dispatcher: fans out push/scan operations to worker processes
//! via the shared append-only log (SAL) and collects responses via per-worker
//! W2M regions. Eventfds provide cross-process signaling.

use std::cell::RefCell;
use std::rc::Rc;

use rustc_hash::{FxHashMap, FxHashSet};

thread_local! {
    /// Pooled aggregation map for PK net-weight + positive-count tracking,
    /// reused across `validate_all_distributed_async` calls. Cleared (capacity
    /// retained) on entry; never held across `.await`.
    static PK_AGG_POOL: RefCell<FxHashMap<u128, (i64, u32)>> =
        RefCell::new(FxHashMap::default());
}

use crate::catalog::CatalogEngine;
use crate::schema::SchemaDescriptor;
use crate::schema::{pk_native_key, payload_native_key};

use crate::runtime::sal::{
    FLAG_SHUTDOWN, FLAG_DDL_SYNC, FLAG_EXCHANGE, FLAG_EXCHANGE_RELAY, FLAG_PUSH, FLAG_HAS_PK,
    FLAG_SEEK, FLAG_SEEK_BY_INDEX, FLAG_BACKFILL, FLAG_GATHER,
    FLAG_TICK, FLAG_FLUSH, SalWriter, pack_gather_cols,
};
use crate::runtime::wire::{self, FLAG_HAS_DATA, FLAG_SCAN_LAST, WireConflictMode, SchemaWithVersion, DecodedWire, col_names_as_refs, peek_control_block};
use gnitz_wire::wire_flags_set_conflict_mode;
use crate::runtime::w2m::{W2mReceiver, W2mSlot};
use crate::runtime::reactor::{AsyncMutex, PendingRelay, ScanLease};
use crate::storage::{Batch, ConsolidatedBatch, partition_for_pk_bytes, PkBuf};
use crate::ops::{
    PartitionRouter, RouteMode, op_repartition_batches_mode, op_relay_scatter_consolidated_mode,
    worker_for_partition, with_worker_indices,
};


// ---------------------------------------------------------------------------
// Pipelined validation checks
// ---------------------------------------------------------------------------

/// How the check payload is routed to workers.
#[allow(clippy::large_enum_variant)]
enum CheckPayload {
    /// Replicate the same batch to every worker; each worker filters
    /// its local partition.
    Broadcast(Batch),
    /// Pre-partitioned by the schema PK: source batch delivered via
    /// `scatter_wire_group` without materializing intermediate per-worker
    /// `Batch`es. `execute_pipeline_async` computes the per-worker routing
    /// itself from `check.schema.pk_indices()` via `with_worker_indices`.
    ScatterSource { source: Batch },
}

/// A single distributed has-pk check queued for pipelined execution.
/// `flags` is typically FLAG_HAS_PK; `col_hint` is (source_col_idx + 1)
/// for an index check or 0 for a PK check.
struct PipelinedCheck {
    target_id: i64,
    flags: u32,
    col_hint: u64,
    payload: Option<CheckPayload>,
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
        /// Type and byte width of the leading index column, used to reproduce
        /// the OPK probe bytes (`index_opk_prefix`) the workers echoed into the
        /// `occupied` result set — the native lookup key must be encoded the
        /// same way before the membership test.
        idx_key_type: u8,
        idx_key_size: usize,
        /// Byte width of the index table's PK (the indexed column). Used to
        /// look up the index key in the `HashSet<PkBuf>` result set.
        idx_pk_stride: u8,
        /// (index-column key, row PK). The index column is always ≤ 16
        /// bytes (→ `u128`); the row PK is byte-form to support wide PKs.
        upsert_keys: Vec<(u128, PkBuf)>,
    },
    /// UPDATE retired a referenced UNIQUE value that is still present in a
    /// child index: a non-empty result set is a RESTRICT violation.
    FkRestrict { child_tid: i64 },
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
    values: FxHashSet<u128>,
    /// True once the filter has exceeded `UNIQUE_FILTER_CAP`. In that
    /// state `values` is cleared and the filter always reports
    /// "possibly present" (falls through to broadcast).
    capped: bool,
    /// False until the warmup scan has fully populated `values`. While
    /// false the filter still accepts ingestion (so keys committed during
    /// the scan window are not lost) but `unique_filter_all_absent`
    /// refuses the broadcast-skip shortcut — an empty/partial filter must
    /// never be trusted to prove absence.
    warm: bool,
}

impl UniqueFilter {
    fn new() -> Self {
        UniqueFilter { values: FxHashSet::default(), capped: false, warm: false }
    }

    fn insert(&mut self, key: u128) {
        if self.capped { return; }
        self.values.insert(key);
        if self.values.len() > UNIQUE_FILTER_CAP {
            self.values = FxHashSet::default();
            self.capped = true;
        }
    }
}

/// RAII guard that removes cold UniqueFilter entries if the warmup future is
/// dropped before it completes. Uses a raw pointer rather than `&mut Map` to
/// avoid holding a mutable reference across `.await` suspension points, which
/// would violate noalias assumptions if the committer task touches the map
/// while the master future is parked.
struct WarmupGuard {
    disp_ptr: *mut MasterDispatcher,
    table_id: i64,
    cols: Vec<u32>,
    disarmed: bool,
}

impl Drop for WarmupGuard {
    fn drop(&mut self) {
        if !self.disarmed {
            unsafe {
                let disp = &mut *self.disp_ptr;
                for &col in &self.cols {
                    disp.unique_filter_remove_col(self.table_id, col);
                }
            }
        }
    }
}

// Safety: WarmupGuard is only used inside the single-threaded master event loop.
unsafe impl Send for WarmupGuard {}

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
    /// Byte offset of this column within the packed PK region. Only
    /// meaningful when `is_pk_col`.
    pk_field_off: usize,
}

/// Invoke `f` with the native `u128` index key of every positive-weight,
/// non-null row of `batch` for the column described by `d`. Returning `false`
/// from `f` stops *this batch's* walk early (callers use it to bail once a
/// filter caps or a duplicate is found); it does NOT stop the surrounding
/// frame drain — `drain_index_scan` keeps consuming every frame regardless.
/// This is the single definition of "what key does this column's value map
/// to" — shared by the steady-state unique-filter warmup, the INSERT-path
/// extraction, and the CREATE-time pre-flight validator, so all three agree
/// by construction.
///
/// The key is the column's value decoded OPK→native: from the packed PK region
/// when the column is itself a PK column, else from its native-LE payload slot.
/// `get_pk` is wrong for signed single-column PKs (it returns the sign-flipped
/// OPK value, which `index_opk_prefix` then flips again, seeking the wrong index
/// key); `pk_native_key` is correct for every width and signedness, and equals
/// `get_pk` for unsigned single PKs.
///
/// Accepts a `MemBatch` so callers can feed wire bytes without an intermediate
/// owned `Batch` allocation (zero-copy scan path); owned batches use
/// `Batch::as_mem_batch()`.
fn for_each_index_key(
    batch: &crate::storage::MemBatch<'_>,
    d: &UniqueIndexDesc,
    mut f: impl FnMut(u128) -> bool,
) {
    for i in 0..batch.count {
        if batch.get_weight(i) <= 0 { continue; }
        if !d.is_pk_col {
            // `src_payload_idx` is a payload-column index; payload columns are
            // capped at 64 by MAX_COLUMNS = 65 (one PK col min, one u64 null
            // word per row), so `1u64 << idx` never overflows.
            let null_word = batch.get_null_word(i);
            if null_word & (1u64 << d.src_payload_idx) != 0 { continue; }
        }
        let key = if d.is_pk_col {
            pk_native_key(
                batch.get_pk_bytes(i), d.pk_field_off, d.col_size, d.type_code)
        } else {
            let col_data = batch.col_data(d.src_payload_idx, d.col_size);
            payload_native_key(col_data, i * d.col_size, d.col_size, d.type_code)
        };
        if !f(key) { return; }
    }
}

/// Walk every positive-weight, non-null row of `batch` and insert the
/// indexed-column value into `filter`. Respects the filter's capped state by
/// stopping the walk once the filter caps. Thin caller of `for_each_index_key`
/// so the key contract is shared with the CREATE-time validator.
fn extract_into_filter(
    filter: &mut UniqueFilter,
    batch: &crate::storage::MemBatch<'_>,
    d: &UniqueIndexDesc,
) {
    for_each_index_key(batch, d, |key| {
        filter.insert(key);
        !filter.capped // stop walking once the filter caps
    });
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
    // Catalog pointer — reborrowed per-call because &mut self borrows conflict.
    catalog: *mut CatalogEngine,
    router: PartitionRouter,
    /// Per-(table_id, unique_col_idx) filter skipping redundant Phase 2
    /// unique-index broadcasts. See the UniqueFilter comment block above.
    unique_filters: FxHashMap<(i64, u32), UniqueFilter>,

    /// Per-`target_id` pool of `Batch`es reused by `build_check_batch` for
    /// FK / unique-index validation. After the awaited pipeline returns,
    /// each check's batch is taken via `mem::replace` and pushed back here;
    /// the next check on the same target reuses it via `clear` + reload.
    /// Schema staleness (DDL between bursts) is checked at pop time.
    check_batch_pool: FxHashMap<i64, Vec<Batch>>,

    /// Monotonic LSN allocator. The SAL writer no longer owns a counter;
    /// the dispatcher hands out LSNs (one per zone) via `next_lsn`.
    /// Reset on checkpoint via `reset_sal`.
    next_lsn: u64,
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
            w2m: Some(w2m),
            catalog,
            router: PartitionRouter::new(),
            unique_filters: FxHashMap::default(),
            check_batch_pool: FxHashMap::default(),
            next_lsn: 0,
        }
    }

    pub fn reset_sal(&mut self, write_cursor: u64, epoch: u32) {
        self.sal.reset(write_cursor, epoch);
    }

    /// Allocate the next monotonic LSN. Single allocator seam — after Phase 1,
    /// every SAL group LSN flows from here. Phases 3 and 6 will replace this
    /// with caller-supplied zone LSNs for DDL and push paths.
    pub fn next_lsn(&mut self) -> u64 {
        let v = self.next_lsn;
        self.next_lsn += 1;
        v
    }

    fn get_schema_and_names(&mut self, target_id: i64) -> (SchemaDescriptor, Rc<Vec<Vec<u8>>>) {
        let cat = unsafe { &mut *self.catalog };
        let schema = cat.get_schema_desc(target_id)
            .unwrap_or_else(|| panic!("master: no schema for target_id={}", target_id));
        let names = cat.get_col_names_bytes(target_id);
        (schema, names)
    }

    /// Return the schema descriptor, a cached prebuilt schema wire block,
    /// and the derived `(wire_safe, wire_row_fixed_stride)` for `target_id`.
    /// The block is built lazily on first call and stored in the catalog
    /// cache; it is invalidated alongside col_names whenever DDL modifies
    /// the table. Used by SAL write paths (commit/tick/broadcast) to skip
    /// per-call `build_schema_wire_block` allocations and per-column
    /// iteration in `scatter_wire_group`.
    fn cached_schema_block(&mut self, target_id: i64)
        -> (SchemaDescriptor, Rc<Vec<u8>>, bool, u32)
    {
        let cat = unsafe { &mut *self.catalog };
        let schema = cat.get_schema_desc(target_id)
            .unwrap_or_else(|| panic!("master: no schema for target_id={}", target_id));
        if let Some(cached) = cat.get_cached_schema_wire_block(target_id) {
            return (schema, cached.block, cached.wire_safe, cached.wire_row_fixed_stride);
        }
        let names = cat.get_col_names_bytes(target_id);
        let (name_refs, n) = col_names_as_refs(&names);
        let block = Rc::new(crate::runtime::wire::build_schema_wire_block(
            &schema, &name_refs[..n], target_id as u32,
        ));
        let (wire_safe, wire_row_stride) = crate::runtime::sal::compute_wire_props(&schema);
        cat.set_schema_wire_block(target_id, block.clone(), wire_safe, wire_row_stride);
        (schema, block, wire_safe, wire_row_stride)
    }

    fn pool_pop_batch(&mut self, id: i64) -> Option<Batch> {
        self.check_batch_pool.get_mut(&id).and_then(|v| v.pop())
    }

    // -----------------------------------------------------------------------
    // Core send/receive helpers
    // -----------------------------------------------------------------------

    /// Encode per-worker data directly into SAL mmap.
    /// Does NOT fdatasync or signal. `lsn` is supplied by the caller.
    ///
    /// Sync callers pass `request_id=0` (or some single id) which is
    /// replicated across all workers. Async callers use
    /// `write_group_with_req_ids` to thread distinct per-worker ids.
    #[allow(clippy::too_many_arguments)]
    fn write_group(
        &mut self,
        target_id: i64,
        lsn: u64,
        sal_flags: u32,
        worker_batches: &[Option<&Batch>],
        schema: &SchemaDescriptor,
        col_names: &[Vec<u8>],
        seek_pk: u128,
        seek_col_idx: u64,
        request_id: u64,
        unicast_worker: i32,
        seek_pk_extra: &[u8],
    ) -> Result<(), String> {
        let nw = self.num_workers;
        let mut ids = [0u64; crate::runtime::sal::MAX_WORKERS];
        for item in ids.iter_mut().take(nw) { *item = request_id; }
        self.write_group_with_req_ids(
            target_id, lsn, sal_flags, 0, worker_batches, schema, col_names,
            seek_pk, seek_col_idx, &ids[..nw], unicast_worker, 0, None,
            seek_pk_extra,
        )
    }

    /// Encode per-worker data with per-worker request ids. Used by async
    /// fan-outs that need distinct ids per worker for reply routing.
    /// `lsn` is supplied by the caller.
    ///
    /// `prebuilt_schema_block`: when `Some`, must be paired with empty
    /// `col_names` (computing names only to discard them negates the savings).
    #[allow(clippy::too_many_arguments)]
    fn write_group_with_req_ids(
        &mut self,
        target_id: i64,
        lsn: u64,
        sal_flags: u32,
        wire_flags: u64,
        worker_batches: &[Option<&Batch>],
        schema: &SchemaDescriptor,
        col_names: &[Vec<u8>],
        seek_pk: u128,
        seek_col_idx: u64,
        req_ids: &[u64],
        unicast_worker: i32,
        client_id: u64,
        prebuilt_schema_block: Option<&[u8]>,
        seek_pk_extra: &[u8],
    ) -> Result<(), String> {
        let (name_refs, n) = col_names_as_refs(col_names);
        let col_names_opt = if n == 0 || prebuilt_schema_block.is_some() {
            None
        } else {
            Some(&name_refs[..n])
        };

        self.sal.write_group_direct(
            target_id as u32, lsn, sal_flags, wire_flags, worker_batches,
            schema, col_names_opt,
            seek_pk, seek_col_idx, req_ids, unicast_worker, client_id,
            prebuilt_schema_block, seek_pk_extra,
        )
    }

    /// Encode batch once directly into SAL mmap, replicate to all workers.
    /// `lsn` is supplied by the caller.
    ///
    /// `prebuilt_schema_block`: when `Some`, must be paired with empty
    /// `col_names` (computing names only to discard them negates the savings).
    #[allow(clippy::too_many_arguments)]
    fn write_broadcast(
        &mut self,
        target_id: i64,
        lsn: u64,
        sal_flags: u32,
        batch: Option<&Batch>,
        schema: &SchemaDescriptor,
        col_names: &[Vec<u8>],
        seek_pk: u128,
        seek_col_idx: u64,
        request_id: u64,
        prebuilt_schema_block: Option<&[u8]>,
    ) -> Result<(), String> {
        let (name_refs, n) = col_names_as_refs(col_names);
        let col_names_opt = if n == 0 || prebuilt_schema_block.is_some() {
            None
        } else {
            Some(&name_refs[..n])
        };

        self.sal.write_broadcast_direct(
            target_id as u32, lsn, sal_flags, 0, batch, schema, col_names_opt,
            seek_pk, seek_col_idx, request_id, prebuilt_schema_block,
        )
    }

    /// Encode once, write to all workers, signal (no fdatasync).
    /// `lsn` is supplied by the caller.
    #[allow(clippy::too_many_arguments)]
    fn send_broadcast(
        &mut self,
        target_id: i64,
        lsn: u64,
        flags: u32,
        batch: Option<&Batch>,
        schema: &SchemaDescriptor,
        col_names: &[Vec<u8>],
        seek_pk: u128,
        seek_col_idx: u64,
        request_id: u64,
    ) -> Result<(), String> {
        self.write_broadcast(target_id, lsn, flags, batch, schema, col_names,
                            seek_pk, seek_col_idx, request_id, None)?;
        self.signal_all();
        Ok(())
    }

    pub(crate) fn signal_all(&self) { self.sal.signal_all(); }
    fn signal_one(&self, worker: usize) { self.sal.signal_one(worker); }

    pub(crate) fn sal_fd(&self) -> i32 { self.sal.sal_fd() }


    /// Write per-worker message group + signal all workers (no fdatasync).
    /// `lsn` is supplied by the caller.
    #[allow(clippy::too_many_arguments)]
    fn send_to_workers(
        &mut self,
        target_id: i64,
        lsn: u64,
        flags: u32,
        worker_batches: &[Option<&Batch>],
        schema: &SchemaDescriptor,
        col_names: &[Vec<u8>],
        seek_pk: u128,
        seek_col_idx: u64,
        request_id: u64,
    ) -> Result<(), String> {
        self.write_group(target_id, lsn, flags, worker_batches, schema, col_names,
                         seek_pk, seek_col_idx, request_id, -1, &[])?;
        self.signal_all();
        Ok(())
    }

    /// Transfer ownership of the W2M receiver to the reactor.
    /// Called once by the executor after bootstrap; panics if called twice.
    pub fn take_w2m(&mut self) -> W2mReceiver {
        self.w2m.take().expect("take_w2m called twice")
    }

    fn w2m(&self) -> &W2mReceiver {
        self.w2m.as_ref().expect("W2mReceiver already handed off to reactor")
    }

    /// Wait for one response from each worker. Bootstrap-only: runs
    /// before the reactor is up, so we drive each worker's ring via
    /// `W2mReceiver::wait_for` (sync FUTEX_WAIT on `reader_seq`). The
    /// tail-chasing ring self-maintains — no reset needed.
    #[allow(clippy::needless_range_loop)]
    fn wait_all_workers(&mut self) -> Result<Vec<Option<DecodedWire>>, String> {
        let nw = self.num_workers;
        let mut results: Vec<Option<DecodedWire>> = (0..nw).map(|_| None).collect();
        for w in 0..nw {
            loop {
                match self.w2m().try_read(w) {
                    Some(decoded) => {
                        if decoded.control.status != 0 {
                            let msg = String::from_utf8_lossy(&decoded.control.error_msg);
                            return Err(format!("worker {}: {}", w, msg));
                        }
                        results[w] = Some(decoded);
                        break;
                    }
                    None => {
                        let _ = self.w2m().wait_for(w, 1000);
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
    #[allow(clippy::needless_range_loop)]
    fn collect_acks_and_relay(&mut self, _target_id: i64) -> Result<(), String> {
        let nw = self.num_workers;
        let mut collected = vec![false; nw];
        let mut remaining = nw;
        let mut acc = crate::runtime::reactor::ExchangeAccumulator::new(nw);

        while remaining > 0 {
            // One full pass over all workers per iteration. If a pass
            // makes no progress, we wait_for on the first still-active
            // worker. Exchange replies from any worker may trigger
            // further SAL writes + replies, so we loop broadly.
            let mut progressed = false;
            for w in 0..nw {
                if collected[w] { continue; }
                let Some(decoded) = self.w2m().try_read(w) else {
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
                    let _ = self.w2m().wait_for(next, 1000);
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
        let lsn = self.next_lsn();
        self.send_broadcast(0, lsn, FLAG_FLUSH, None, &schema, &[], 0, 0, 0)?;
        self.collect_acks()?;
        self.checkpoint_post_ack();
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Exchange relay
    // -----------------------------------------------------------------------

    /// CPU-only first half of exchange relay: looks up shard columns via
    /// the catalog DAG, scatters the payloads into per-worker batches,
    /// True when enough SAL space remains for a relay write.  The threshold
    /// mirrors the one in `prepare_relay` but is checked *before* consuming
    /// the relay so a low-space condition can be resolved (checkpoint) rather
    /// than silently discarding the relay and deadlocking blocked workers.
    pub(crate) fn sal_has_relay_space(&self) -> bool {
        self.sal.mmap_size() - self.sal.cursor() >= (self.sal.mmap_size() >> 3)
    }

    /// and collects column names. No SAL write yet — `relay_loop` runs
    /// this without `sal_writer_excl` so the lock covers only the
    /// synchronous SAL write in `emit_relay`.
    pub(crate) fn prepare_relay(&mut self, relay: PendingRelay) -> Result<RelayPrepared, String> {
        let remaining = self.sal.mmap_size() - self.sal.cursor();
        if remaining < (self.sal.mmap_size() >> 3) {
            return Err(format!(
                "SAL space exhausted during exchange relay ({} bytes left)", remaining));
        }

        let PendingRelay { view_id, payloads, schema, source_id } = relay;

        let cat = unsafe { &mut *self.catalog };
        // A join-shard scatter (cols from a reindex chain) must route by the
        // reindex key so a row lands on the worker that owns its `_join_pk`
        // partition; a GROUP BY / set-op exchange scatter routes by the group
        // key (consistent with op_reduce's output PK). See `RouteMode`.
        let (shard_cols, is_join) = if source_id > 0 {
            let cols = cat.dag.get_join_shard_cols(view_id, source_id);
            if cols.is_empty() { (cat.dag.get_shard_cols(view_id), false) } else { (cols, true) }
        } else {
            (cat.dag.get_shard_cols(view_id), false)
        };

        let col_indices: Vec<u32> = shard_cols.iter().map(|&c| c as u32).collect();

        let consolidated_sources: Option<Vec<Option<&ConsolidatedBatch>>> = payloads.iter()
            .map(|opt| match opt {
                None => Some(None),
                Some(b) => ConsolidatedBatch::from_batch_ref(b).map(Some),
            })
            .collect();

        let mode = if is_join { RouteMode::JoinPromote } else { RouteMode::GroupKey };
        let dest_batches = match consolidated_sources {
            Some(sources) => {
                op_relay_scatter_consolidated_mode(&sources, &col_indices, &schema, self.num_workers, mode)
            }
            None => {
                let sources: Vec<Option<&Batch>> = payloads.iter().map(|opt| opt.as_ref()).collect();
                op_repartition_batches_mode(&sources, &col_indices, &schema, self.num_workers, mode)
            }
        };

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
        let lsn = self.next_lsn();
        // Echo `source_id` back via `seek_pk_lo` so the worker's
        // `do_exchange_wait` can match on (view_id, source_id). Without
        // this, a multi-source view (join over 2+ tables) can deliver
        // the wrong source's relay to a waiting exchange and the worker
        // demuxes against the wrong sharding columns.
        self.send_to_workers(view_id, lsn, FLAG_EXCHANGE_RELAY, &refs, &schema, &name_bytes,
                              source_id as u128, 0, 0)
    }

    fn record_index_routing(
        &mut self,
        target_id: i64,
        schema: &SchemaDescriptor,
        source_batch: &Batch,
        worker_indices: &[Vec<u32>],
    ) {
        let cat = unsafe { &mut *self.catalog };
        let n_idx = cat.get_index_circuit_count(target_id);
        if n_idx == 0 { return; }

        for ci in 0..n_idx {
            if let Some((col_idx, is_unique, _tc)) = cat.get_index_circuit_info(target_id, ci) {
                if !is_unique { continue; }
                for (w, wi) in worker_indices[..self.num_workers].iter().enumerate() {
                    if !wi.is_empty() {
                        self.router.record_routing_from_source(
                            source_batch, wi, schema,
                            target_id as u32, col_idx, w as u32,
                        );
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
        let lsn = self.next_lsn();
        self.send_broadcast(source_id, lsn, FLAG_BACKFILL, None, &schema, &col_names,
                           view_id as u128, 0, 0)?;
        self.collect_acks_and_relay(source_id)
    }

    // Async fan-outs take `*mut Self` instead of `&mut self` because the
    // exclusive borrow must end before `.await`: other reactor tasks
    // re-enter the dispatcher in the meantime. Only call from inside a
    // reactor task driven by `block_until_idle`.

    pub async fn fan_out_seek_async(
        disp_ptr: *mut MasterDispatcher,
        reactor: &crate::runtime::reactor::Reactor,
        sal_excl: &Rc<AsyncMutex<()>>,
        target_id: i64, pk: u128, seek_pk_extra: &[u8],
    ) -> Result<W2mSlot, String> {
        let num_workers = unsafe { (*disp_ptr).num_workers };
        let schema = unsafe {
            (*(*disp_ptr).catalog).get_schema_desc(target_id)
                .ok_or_else(|| format!("seek: table {} not found", target_id))?
        };
        let stride = schema.pk_stride() as usize;
        let worker = if schema.pk_is_wide() {
            let full_pk_buf = crate::schema::assemble_wide_pk(&schema, pk, seek_pk_extra, stride)
                .map_err(|e| format!("seek: table {target_id}: {e}"))?;
            worker_for_partition(partition_for_pk_bytes(&full_pk_buf[..stride]), num_workers)
        } else {
            // Narrow path: `pk` is the native seek key, but ingestion routes on
            // the OPK bytes (partition_for_key(get_pk) == partition_for_pk_bytes(opk)).
            // Encode native → OPK and route on those bytes; hashing the native
            // value misroutes signed/compound narrow PKs to the wrong worker.
            let (opk, _) = crate::storage::opk_key(&schema, pk);
            worker_for_partition(partition_for_pk_bytes(&opk[..stride]), num_workers)
        };
        single_worker_async(disp_ptr, reactor, sal_excl, target_id, FLAG_SEEK,
                            worker, pk, 0, "seek", seek_pk_extra).await
    }

    pub async fn fan_out_seek_by_index_async(
        disp_ptr: *mut MasterDispatcher,
        reactor: &crate::runtime::reactor::Reactor,
        sal_excl: &Rc<AsyncMutex<()>>,
        target_id: i64, col_idx: u32, key: u128,
    ) -> Result<W2mSlot, String> {
        // The routing cache is keyed by `extract_col_key` (OPK-widened for
        // integer columns, XXH3 for STRING/BLOB), but `key` is the native seek
        // value. Transform it into the stored representation before probing;
        // a raw native query always misses for signed integers (and could
        // spuriously hit a different value's OPK-widened key).
        let cached = unsafe {
            let schema = (*(*disp_ptr).catalog).get_schema_desc(target_id);
            match schema.and_then(|s| index_route_key(&s, col_idx, key)) {
                Some(rk) => (*disp_ptr).router.worker_for_index_key(target_id as u32, col_idx, rk),
                None => -1,
            }
        };
        if cached >= 0 {
            let worker = cached as usize;
            return single_worker_async(
                disp_ptr, reactor, sal_excl, target_id, FLAG_SEEK_BY_INDEX,
                worker, key, col_idx as u64, "seek_by_index", &[],
            ).await;
        }

        // Cache miss: broadcast to all workers with per-worker req_ids and
        // forward the slot whose worker found a row (or slot 0 if none).
        // `_lease` keeps the scan active across the single-frame inspection
        // below; its workers also stream, so dropping it early would let the
        // gate discard a late frame.
        let (mut slots, _req_ids, _lease) = dispatch_scan_fanout(disp_ptr, reactor, sal_excl, |disp, req_ids| {
            let (schema, col_names) = disp.get_schema_and_names(target_id);
            let lsn = disp.next_lsn();
            disp.write_group_with_req_ids(
                target_id, lsn, FLAG_SEEK_BY_INDEX, 0, &[], &schema, &col_names,
                key, col_idx as u64, req_ids, -1, 0, None, &[],
            )
        }).await?;

        let mut data_idx = None;
        for (w, slot) in slots.iter().enumerate() {
            let ctrl = peek_control_block(slot.bytes())
                .map_err(|e| format!("seek_by_index: worker {}: {}", w, e))?;
            if ctrl.status != 0 {
                return Err(format!(
                    "worker {}: seek_by_index: {}",
                    w, String::from_utf8_lossy(&ctrl.error_msg)));
            }
            if ctrl.flags & FLAG_HAS_DATA != 0 {
                data_idx = Some(w);
            }
        }
        Ok(slots.swap_remove(data_idx.unwrap_or(0)))
    }

    /// SELECT-path index lookup: broadcast to ALL workers and MERGE every
    /// matching base row into one batch.
    ///
    /// A non-unique indexed value matches rows scattered across workers (the
    /// per-key routing cache is only populated for unique indexes), so unlike
    /// `fan_out_seek_by_index_async` (which returns a single worker's slot, used
    /// by the UPSERT identity check where the index is unique) this must
    /// aggregate. Returns the merged base rows, or `None` when no row matches.
    pub async fn fan_out_seek_by_index_collect_async(
        disp_ptr: *mut MasterDispatcher,
        reactor: &crate::runtime::reactor::Reactor,
        sal_excl: &Rc<AsyncMutex<()>>,
        target_id: i64, col_idx: u32, key: u128,
    ) -> Result<Option<Batch>, String> {
        // `_lease` held across the single-frame collect: its workers stream, so
        // releasing the gate before the slots are inspected risks a discarded
        // late frame.
        let (slots, _req_ids, _lease) = dispatch_scan_fanout(disp_ptr, reactor, sal_excl, |disp, req_ids| {
            let (schema, col_names) = disp.get_schema_and_names(target_id);
            let lsn = disp.next_lsn();
            disp.write_group_with_req_ids(
                target_id, lsn, FLAG_SEEK_BY_INDEX, 0, &[], &schema, &col_names,
                key, col_idx as u64, req_ids, -1, 0, None, &[],
            )
        }).await?;

        let mut acc: Option<Batch> = None;
        for (w, slot) in slots.iter().enumerate() {
            let ctrl = peek_control_block(slot.bytes())
                .map_err(|e| format!("seek_by_index: worker {}: {}", w, e))?;
            if ctrl.status != 0 {
                return Err(format!(
                    "worker {}: seek_by_index: {}",
                    w, String::from_utf8_lossy(&ctrl.error_msg)));
            }
            if ctrl.flags & FLAG_HAS_DATA == 0 { continue; }
            let decoded = wire::decode_wire_ipc(slot.bytes())
                .map_err(|e| format!("seek_by_index: worker {}: decode: {}", w, e))?;
            if let Some(b) = decoded.data_batch {
                if b.count == 0 { continue; }
                match acc.as_mut() {
                    Some(a) => a.append_batch(&b, 0, b.count),
                    None => acc = Some(b),
                }
            }
        }
        Ok(acc)
    }

    /// Fan out a SCAN to all workers, forward every response frame directly
    /// to `fd` (including continuation chunks), and return `Ok(true)` when
    /// all workers finish. Returns `Ok(false)` if the client disconnects
    /// mid-stream; returns `Err` on a worker error.
    ///
    /// Each slot is dropped (advancing `consume_cursor`) before the next
    /// continuation frame is awaited to prevent W2M ring deadlock.
    pub async fn fan_out_scan_async(
        disp_ptr: *mut MasterDispatcher,
        reactor: &crate::runtime::reactor::Reactor,
        sal_excl: &Rc<AsyncMutex<()>>,
        target_id: i64,
        client_id: u64,
        fd: i32,
        client_version: u16,
    ) -> Result<bool, String> {
        // `_lease` held across the entire continuation drain: every worker
        // streams a multi-frame train, and a cancelled drain (client
        // disconnect) must keep the ids active until the lease drops so the
        // gate discards — not parks — late frames.
        let (slots, req_ids, _lease) = dispatch_scan_fanout(disp_ptr, reactor, sal_excl, |disp, req_ids| {
            let (schema, col_names) = disp.get_schema_and_names(target_id);
            let lsn = disp.next_lsn();
            // Embed client_version in wire_flags bits 24-39 so workers can
            // decide whether to include the schema block in their response.
            let wire_flags = gnitz_wire::wire_flags_set_schema_version(0, client_version);
            disp.write_group_with_req_ids(
                target_id, lsn, 0, wire_flags, &[], &schema, &col_names,
                0, 0, req_ids, -1, client_id, None, &[],
            )
        }).await?;

        // A worker fault or decode error must NOT short-circuit the loop: a
        // later worker still streaming its scan train would wedge in
        // `send_encoded` on a full W2M ring if we returned before draining it.
        // Capture the first error and surface it only after every worker's
        // train is fully drained (or dropped, if the client already left).
        let mut disconnected = false;
        let mut deferred_err: Option<String> = None;
        for (w, mut slot) in slots.into_iter().enumerate() {
            loop {
                let ctrl = match peek_control_block(slot.bytes()) {
                    Ok(c) => c,
                    Err(e) => {
                        if deferred_err.is_none() { deferred_err = Some(scan_decode_err(w, e)); }
                        drop(slot);
                        break;
                    }
                };
                // Status-gated for the same reason as `drain_index_scan`: an error
                // frame carries flags `0`, so keying `has_more` off
                // `FLAG_SCAN_LAST` alone would await a frame the faulted worker
                // never sends. FLAG_CONTINUATION is always set on worker frames.
                let has_more = ctrl.status == 0 && (ctrl.flags & FLAG_SCAN_LAST == 0);
                if ctrl.status != 0 {
                    if deferred_err.is_none() {
                        let msg = String::from_utf8_lossy(&ctrl.error_msg).to_string();
                        deferred_err = Some(format!("worker {}: scan: {}", w, msg));
                    }
                    drop(slot); // terminal fault frame; free the ring slot
                } else if !disconnected {
                    // Forward slot to client; send_slot drops it on return.
                    let rc = reactor.send_slot(fd, slot).await;
                    if rc < 0 { disconnected = true; }
                } else {
                    // Client disconnected: drop slot to free W2M ring space so
                    // the worker can finish emitting its pending_scan train.
                    drop(slot);
                }
                if !has_more { break; }
                slot = reactor.await_scan_slot(req_ids[w] as u32).await;
            }
        }
        if let Some(err) = deferred_err { return Err(err); }
        Ok(!disconnected)
    }

    /// Async version of `execute_pipeline`. Writes each check with
    /// per-worker req_ids, signals once, and joins all replies.
    ///
    /// `sal_excl` is held only for the synchronous write + signal phase;
    /// see `dispatch_scan_fanout` for the rationale.
    async fn execute_pipeline_async(
        disp_ptr: *mut MasterDispatcher,
        reactor: &crate::runtime::reactor::Reactor,
        sal_excl: &Rc<AsyncMutex<()>>,
        checks: &mut [PipelinedCheck],
    ) -> Result<Vec<FxHashSet<PkBuf>>, String> {
        let num_checks = checks.len();
        if num_checks == 0 {
            return Ok(Vec::new());
        }

        let (nw, all_req_ids): (usize, Vec<u64>) = {
            let _guard = sal_excl.lock().await;
            unsafe {
                let disp = &mut *disp_ptr;
                let nw = disp.num_workers;
                let mut rids: Vec<u64> = Vec::with_capacity(num_checks * nw);
                for _ in 0..(num_checks * nw) {
                    rids.push(reactor.alloc_request_id());
                }
                for (idx, check) in checks.iter().enumerate() {
                    let lsn = disp.next_lsn();
                    let req_slice = &rids[idx * nw..(idx + 1) * nw];
                    match check.payload.as_ref().expect("payload consumed") {
                        CheckPayload::Broadcast(batch) => {
                            let refs: Vec<Option<&Batch>> = (0..nw).map(|_| Some(batch)).collect();
                            disp.write_group_with_req_ids(
                                check.target_id, lsn, check.flags, 0, &refs,
                                &check.schema, &[], 0, check.col_hint,
                                req_slice, -1, 0, None, &[],
                            )?;
                        }
                        CheckPayload::ScatterSource { source } => {
                            // Routing is always by the schema PK; compute the
                            // per-worker indices on the fly. No reentrancy: this
                            // loop body has no `.await`, so the SCATTER_INDICES
                            // borrow is released before the next iteration.
                            let pk_cols = check.schema.pk_indices();
                            with_worker_indices(source, pk_cols, &check.schema, nw, |worker_indices| {
                                disp.sal.scatter_wire_group(
                                    source, worker_indices, &check.schema, None,
                                    check.target_id as u32, lsn, check.flags, 0,
                                    0, check.col_hint, req_slice, -1, None, None,
                                )
                            })?;
                        }
                    }
                }
                disp.signal_all();
                (nw, rids)
            }
        };

        let decoded_vec: Vec<DecodedWire> = crate::runtime::reactor::join_all_unpin(
            all_req_ids.iter().map(|&id| reactor.await_reply(id))
        ).await;

        let mut results: Vec<FxHashSet<PkBuf>> = checks.iter().map(|check| {
            let cap = match check.payload.as_ref().expect("payload consumed") {
                CheckPayload::Broadcast(b) => b.count,
                CheckPayload::ScatterSource { source } => source.count,
            };
            FxHashSet::with_capacity_and_hasher(cap, Default::default())
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
                            results[check_idx].insert(PkBuf::from_bytes(batch.get_pk_bytes(j)));
                        }
                    }
                }
            }
        }
        Ok(results)
    }

    /// Batched stored-row gather. Scatters `pks` to their owning workers (one
    /// group, partitioned by the parent PK columns so each worker only reads
    /// rows it stores), each worker reads the committed rows for its PKs and
    /// replies with them projected to `project` (the referenced parent column
    /// indices). Returns a `pk → projected values` map: each value is the
    /// promoted index key, or `None` for a NULL referenced value; PKs whose
    /// committed row is absent are omitted entirely. The per-row `Vec` is
    /// aligned to `project`.
    ///
    /// This is the `O(num_workers)`-round-trip replacement for the per-row
    /// serial single-key seek loop used by FK RESTRICT on non-PK UNIQUE
    /// targets. It is a sibling of `execute_pipeline_async` (which returns only
    /// existence) rather than a modification of it: the has-pk pipeline echoes
    /// the caller's payload (`filter_by_pk`), so it structurally cannot return
    /// a stored column the caller does not already hold.
    ///
    /// `sal_excl` is held only across the synchronous write + signal; there is
    /// no `.await` between `signal_all()` and the first poll of `join_all_unpin`
    /// — that first poll registers every req id's waker before any worker reply
    /// can arrive, so a reply cannot be dropped (see `execute_pipeline_async`).
    async fn execute_gather_async(
        disp_ptr: *mut MasterDispatcher,
        reactor: &crate::runtime::reactor::Reactor,
        sal_excl: &Rc<AsyncMutex<()>>,
        target_id: i64,
        mut pks: Vec<PkBuf>,
        project: &[u8],
    ) -> Result<FxHashMap<PkBuf, Vec<Option<u128>>>, String> {
        if pks.is_empty() {
            return Ok(FxHashMap::default());
        }
        // Sort so each worker's sublist reaches `gather_family` ascending:
        // `removed`/updated PKs are extracted from an FxHashMap (arbitrary
        // order) and `scatter_wire_group` preserves per-worker relative order,
        // so a globally sorted input yields per-worker-sorted sublists.
        pks.sort_unstable_by(|a, b| a.pk_bytes().cmp(b.pk_bytes()));
        let col_mask = pack_gather_cols(project)
            .ok_or("gather: more than 8 projected columns")?;

        let parent_schema = unsafe {
            (&*(*disp_ptr).catalog).get_schema_desc(target_id)
                .ok_or_else(|| format!("gather: no schema for table {}", target_id))?
        };

        let req_ids: Vec<u64> = {
            let _guard = sal_excl.lock().await;
            unsafe {
                let disp = &mut *disp_ptr;
                let nw = disp.num_workers;
                let pk_cols = parent_schema.pk_indices();
                let pooled = disp.pool_pop_batch(target_id);
                let batch = build_check_batch_pkbuf(&parent_schema, &pks, pooled);
                let rids: Vec<u64> =
                    (0..nw).map(|_| reactor.alloc_request_id()).collect();
                let lsn = disp.next_lsn();
                with_worker_indices(&batch, pk_cols, &parent_schema, nw, |worker_indices| {
                    disp.sal.scatter_wire_group(
                        &batch, worker_indices, &parent_schema, None,
                        target_id as u32, lsn, FLAG_GATHER,
                        /* wire_flags */ 0, /* seek_pk */ 0, /* seek_col_idx */ col_mask,
                        &rids, /* unicast_worker */ -1, None, None,
                    )
                })?;
                disp.signal_all();
                // The scatter batch is fully consumed by the synchronous
                // scatter_wire_group above; return it to the pool.
                recycle_check_batch(disp, target_id, batch);
                rids
            }
        };

        let decoded: Vec<DecodedWire> = crate::runtime::reactor::join_all_unpin(
            req_ids.iter().map(|&id| reactor.await_reply(id))
        ).await;
        if let Some(err) = first_worker_error("gather", &decoded) {
            return Err(err);
        }

        // Precompute (type_code, col_size) per projected column from the parent
        // schema; the reply's projected payload index k corresponds to project[k].
        let proj_meta: Vec<(u8, usize)> = project.iter().map(|&p| {
            let col = parent_schema.columns[p as usize];
            (col.type_code, col.size() as usize)
        }).collect();

        let mut out: FxHashMap<PkBuf, Vec<Option<u128>>> = FxHashMap::default();
        for d in &decoded {
            if let Some(ref b) = d.data_batch {
                for j in 0..b.count {
                    let mut vals: Vec<Option<u128>> = Vec::with_capacity(proj_meta.len());
                    let null_word = b.get_null_word(j);
                    for (k, &(col_type, col_size)) in proj_meta.iter().enumerate() {
                        if null_word & (1u64 << k) != 0 {
                            vals.push(None);
                        } else {
                            let col_data = b.col_data(k);
                            vals.push(Some(payload_native_key(
                                col_data, j * col_size, col_size, col_type)));
                        }
                    }
                    out.insert(PkBuf::from_bytes(b.get_pk_bytes(j)), vals);
                }
            }
        }
        Ok(out)
    }

    /// Broadcast a DDL batch to every worker. `lsn` is supplied by the
    /// caller — Phase 3 uses one zone-LSN across all broadcasts in a DDL
    /// so recovery can group them as an atomic zone.
    pub fn broadcast_ddl(&mut self, target_id: i64, batch: &Batch, lsn: u64) -> Result<(), String> {
        let (schema, schema_block, _safe, _stride) = self.cached_schema_block(target_id);
        self.write_broadcast(target_id, lsn, FLAG_DDL_SYNC, Some(batch), &schema, &[],
                            0, 0, 0, Some(schema_block.as_slice()))?;
        self.signal_all();
        gnitz_debug!("broadcast_ddl tid={} rows={} lsn={}", target_id, batch.count, lsn);
        Ok(())
    }

    /// Close an atomic zone at `lsn`: write the empty commit sentinel
    /// and signal workers. All preceding groups at this LSN belong to
    /// the zone; recovery applies them only when this sentinel reaches
    /// disk before the crash.
    pub fn commit_zone(&mut self, lsn: u64) -> Result<(), String> {
        self.sal.write_commit_sentinel(lsn)?;
        self.signal_all();
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Tick group writer (used by the async tick task in executor.rs)
    // -----------------------------------------------------------------------

    /// Write a FLAG_TICK group for `tid` with per-worker req_ids. Does
    /// NOT signal — the caller batches multiple `write_tick_group` calls
    /// followed by a single `signal_all` (IV.6). The underlying SAL
    /// encoder reuses `write_group_with_req_ids`; per-worker slots all
    /// carry the corresponding req_id from `req_ids[w]`. `lsn` is
    /// supplied by the caller.
    pub(crate) fn write_tick_group(
        &mut self, tid: i64, lsn: u64, req_ids: &[u64],
    ) -> Result<(), String> {
        let (schema, schema_block, _safe, _stride) = self.cached_schema_block(tid);
        self.write_group_with_req_ids(
            tid, lsn, FLAG_TICK, 0, &[], &schema, &[],
            0, 0, req_ids, -1, 0, Some(schema_block.as_slice()), &[],
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
            if rpid != 0 {
                return w as i32;
            }
        }
        -1
    }

    pub fn shutdown_workers(&mut self) {
        let schema = SchemaDescriptor::minimal_u64();
        let lsn = self.next_lsn();
        let _ = self.send_broadcast(0, lsn, FLAG_SHUTDOWN, None, &schema, &[], 0, 0, 0);
        for w in 0..self.num_workers {
            let pid = self.worker_pids[w];
            if pid > 0 {
                let mut status: i32 = 0;
                unsafe { libc::waitpid(pid, &mut status, 0); }
            }
        }
        // All workers reaped: no process can race a removal. Reclaim any dirs
        // still gated (dropped entities whose gating checkpoint never arrived).
        unsafe { &mut *self.catalog }.drain_checkpoint_gated_deletions();
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

        let mut out: Vec<UniqueIndexDesc> = Vec::new();
        for ci in 0..n_circuits {
            if let Some((col_idx, is_unique, type_code)) =
                cat.get_index_circuit_info(table_id, ci)
            {
                if !is_unique { continue; }
                let source_col = col_idx as usize;
                let is_pk_col = schema.is_pk_col(source_col);
                let src_payload_idx = if is_pk_col {
                    usize::MAX
                } else {
                    schema.payload_idx(source_col)
                };
                let col_size = schema.columns[source_col].size() as usize;
                let pk_field_off = if is_pk_col {
                    schema.pk_byte_offset(source_col) as usize
                } else { 0 };
                out.push(UniqueIndexDesc {
                    col_idx, type_code, is_pk_col, src_payload_idx, col_size,
                    pk_field_off,
                });
            }
        }
        if out.is_empty() { None } else { Some((schema, out)) }
    }

    /// Column-extraction descriptor for `col_idx` of `owner_id`, built from the
    /// owner schema — used at CREATE time, before the index circuit exists, so
    /// it cannot read `index_circuits`. Matches `unique_index_descriptors`
    /// field-for-field, so the pre-flight key equals the steady-state
    /// enforcement key.
    fn unique_index_desc_for_col(&mut self, owner_id: i64, col_idx: u32) -> Option<UniqueIndexDesc> {
        let schema = unsafe { (*self.catalog).get_schema_desc(owner_id) }?;
        let col = col_idx as usize;
        // `schema.columns` is a fixed `[SchemaColumn; MAX_COLUMNS]` array, so its
        // `.len()` is the capacity (MAX_COLUMNS = 65), not the table's active
        // column count. Bounding on it instead of `num_columns()` would let an
        // out-of-range `col_idx` index a zero-initialized slot and build a bogus
        // descriptor (wrong size/offset/type) rather than return None.
        if col >= schema.num_columns() { return None; }
        let is_pk_col = schema.is_pk_col(col);
        Some(UniqueIndexDesc {
            col_idx,
            type_code: schema.columns[col].type_code,
            is_pk_col,
            src_payload_idx: if is_pk_col { usize::MAX } else { schema.payload_idx(col) },
            col_size: schema.columns[col].size() as usize,
            pk_field_off: if is_pk_col { schema.pk_byte_offset(col) as usize } else { 0 },
        })
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
        if !filter.warm || filter.capped {
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
        let mb = batch.as_mem_batch();
        for d in descs {
            let filter = match self.unique_filters.get_mut(&(table_id, d.col_idx)) {
                Some(f) => f,
                None => continue, // not warm — warmup will pick this up
            };
            if filter.capped { continue; }
            extract_into_filter(filter, &mb, &d);
        }
    }

    /// Drop every filter entry for `table_id`. Called on flush errors
    /// (where filter state may be out of sync with workers) and on DDL
    /// changes (DROP TABLE, DROP/CREATE INDEX).
    pub(crate) fn unique_filter_invalidate_table(&mut self, table_id: i64) {
        self.unique_filters.retain(|&(t, _), _| t != table_id);
        // The check-batch pool is a pure allocation cache keyed by table id;
        // drop the dropped table's entry so it doesn't leak across DDL cycles.
        self.check_batch_pool.remove(&table_id);
    }

    /// Remove the unique-filter entry for a single (owner_table_id, col_idx)
    /// pair. Called on DROP INDEX so subsequent INSERTs re-trigger warmup for
    /// the now-absent index while leaving unrelated filters on the same table.
    pub(crate) fn unique_filter_remove_col(&mut self, owner_id: i64, col_idx: u32) {
        self.unique_filters.remove(&(owner_id, col_idx));
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
        reactor: &crate::runtime::reactor::Reactor,
        sal_excl: &Rc<AsyncMutex<()>>,
        target_id: i64, batch: &Batch, mode: WireConflictMode,
    ) -> Result<(), String> {
        let (n_fk, n_children, n_circuits, has_unique, unique_pk, source_schema) = unsafe {
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
            (n_fk, n_children, n_circuits, has_unique, unique_pk, source_schema)
        };

        let needs_pk_rejection = matches!(mode, WireConflictMode::Error) && unique_pk;

        if n_fk == 0 && n_children == 0 && !has_unique && !needs_pk_rejection {
            return Ok(());
        }

        // Wide PKs (pk_stride > 16) cannot pack into a u128 word; their
        // aggregation/identification paths key on byte-form `PkBuf` instead.
        // Narrow paths stay verbatim on u128 to avoid the per-row PkBuf init
        // cost on the common (large-INSERT) path.
        let wide = source_schema.pk_is_wide();

        // Build PK aggregation. Narrow path uses the pooled u128 map and yields
        // `pk_lo_hi`; the wide path uses a local PkBuf-keyed map and yields
        // `pk_lo_hi_wide`. Both feed the UPSERT PK-identification check below.
        let mut pk_lo_hi: Option<Vec<u128>> = None;
        let mut pk_lo_hi_wide: Option<Vec<PkBuf>> = None;
        if has_unique || needs_pk_rejection {
            if !wide {
                pk_lo_hi = PK_AGG_POOL.with(|cell| -> Result<Option<Vec<u128>>, String> {
                    let mut m = cell.borrow_mut();
                    // Reclaim capacity after an unusually large batch to prevent
                    // the thread-local from growing without bound.
                    if m.capacity() > 65_536 {
                        *m = FxHashMap::default();
                    } else {
                        m.clear();
                    }
                    m.reserve(batch.count);
                    for i in 0..batch.count {
                        let w = batch.get_weight(i);
                        if w == 0 { continue; }
                        let entry = m.entry(batch.get_pk(i)).or_insert((0, 0));
                        entry.0 += w;
                        if w > 0 { entry.1 += 1; }
                    }
                    if needs_pk_rejection {
                        for (&pk, &(_, pos_count)) in m.iter() {
                            if pos_count > 1 {
                                let key_str = format_pk_value(pk, &source_schema);
                                return Err(unsafe {
                                    (*disp_ptr).batch_dup_pk_err(target_id, &source_schema, &key_str)
                                });
                            }
                        }
                    }
                    let mut keys: Vec<u128> = Vec::with_capacity(m.len());
                    for (&pk, &(net_weight, _)) in m.iter() {
                        if net_weight <= 0 { continue; }
                        keys.push(pk);
                    }
                    Ok(Some(keys))
                })?;
            } else {
                let mut m: FxHashMap<PkBuf, (i64, u32)> =
                    FxHashMap::with_capacity_and_hasher(batch.count, Default::default());
                for i in 0..batch.count {
                    let w = batch.get_weight(i);
                    if w == 0 { continue; }
                    let entry = m.entry(PkBuf::from_bytes(batch.get_pk_bytes(i))).or_insert((0, 0));
                    entry.0 += w;
                    if w > 0 { entry.1 += 1; }
                }
                if needs_pk_rejection {
                    for (pk, &(_, pos_count)) in m.iter() {
                        if pos_count > 1 {
                            let key_str = format_pk_value_bytes(pk.pk_bytes(), &source_schema);
                            return Err(unsafe {
                                (*disp_ptr).batch_dup_pk_err(target_id, &source_schema, &key_str)
                            });
                        }
                    }
                }
                let mut keys: Vec<PkBuf> = Vec::with_capacity(m.len());
                for (pk, &(net_weight, _)) in m.iter() {
                    if net_weight <= 0 { continue; }
                    keys.push(*pk);
                }
                pk_lo_hi_wide = Some(keys);
            }
        }

        // ----- Phase 1 plan -----------------------------------------------
        let mut p1_checks: Vec<PipelinedCheck> = Vec::new();
        let mut p1_labels: Vec<P1Label> = Vec::new();

        for fi in 0..n_fk {
            let (fk_col_idx, parent_table_id, parent_col_idx, col_type, parent_schema) = unsafe {
                let disp = &mut *disp_ptr;
                let cat = &mut *disp.catalog;
                let (fk_col_idx, parent_table_id, parent_col_idx) = match cat.get_fk_constraint(target_id, fi) {
                    Some(c) => c,
                    None => continue,
                };
                let col_type = cat.get_fk_col_type(target_id, fk_col_idx);
                let parent_schema = cat.get_schema_desc(parent_table_id)
                    .ok_or_else(|| format!(
                        "FK parent table {} schema not found", parent_table_id))?;
                (fk_col_idx, parent_table_id, parent_col_idx, col_type, parent_schema)
            };

            // The FK column may itself be a PK column of the child table (e.g.
            // `id BIGINT PRIMARY KEY REFERENCES parent(pid)`). In that case it
            // has no payload slot; `payload_idx` would return the PK sentinel
            // and `col_data(sentinel)` indexes out of bounds. Read the value
            // from the PK region instead.
            let is_fk_pk_col = source_schema.is_pk_col(fk_col_idx);
            let payload_col  = if is_fk_pk_col { usize::MAX } else { source_schema.payload_idx(fk_col_idx) };
            let pk_field_off = if is_fk_pk_col { source_schema.pk_byte_offset(fk_col_idx) as usize } else { 0 };
            let col_size     = source_schema.columns[fk_col_idx].size() as usize;

            let mut seen: FxHashSet<u128> = FxHashSet::default();
            let mut keys: Vec<u128> = Vec::new();

            for i in 0..batch.count {
                if batch.get_weight(i) <= 0 { continue; }
                let fk_key = if is_fk_pk_col {
                    pk_native_key(batch.get_pk_bytes(i), pk_field_off, col_size, col_type)
                } else {
                    let null_word = batch.get_null_word(i);
                    if null_word & (1u64 << payload_col) != 0 { continue; }
                    payload_native_key(batch.col_data(payload_col), i * col_size, col_size, col_type)
                };
                if seen.insert(fk_key) {
                    keys.push(fk_key);
                }
            }

            if keys.is_empty() { continue; }
            let expected_count = keys.len();

            // PK fast-path only when the referenced column *is* the parent's
            // lone PK. Otherwise probe the parent's UNIQUE index by broadcast,
            // since index entries are distributed independently of the PK.
            let pk = parent_schema.pk_indices();
            let is_parent_pk = pk.len() == 1 && pk[0] as usize == parent_col_idx;

            if is_parent_pk {
                let pooled = unsafe { (*disp_ptr).pool_pop_batch(parent_table_id) };
                let check_batch = build_check_batch(&parent_schema, &keys, pooled);
                p1_labels.push(P1Label::FkParent { parent_table_id, expected_count });
                p1_checks.push(PipelinedCheck {
                    target_id: parent_table_id,
                    flags: FLAG_HAS_PK,
                    col_hint: 0,
                    payload: Some(CheckPayload::ScatterSource { source: check_batch }),
                    schema: parent_schema,
                });
            } else {
                let idx_schema = unsafe {
                    let cat = &mut *(*disp_ptr).catalog;
                    cat.get_index_schema_by_col(parent_table_id, parent_col_idx as u32)
                        .ok_or_else(|| format!(
                            "FK check: no unique index on parent table {} col {}",
                            parent_table_id, parent_col_idx))?
                };
                let pooled = unsafe { (*disp_ptr).pool_pop_batch(parent_table_id) };
                let check_batch = build_check_batch(&idx_schema, &keys, pooled);
                p1_labels.push(P1Label::FkParent { parent_table_id, expected_count });
                p1_checks.push(PipelinedCheck {
                    target_id: parent_table_id,
                    flags: FLAG_HAS_PK,
                    col_hint: (parent_col_idx as u64) + 1,
                    payload: Some(CheckPayload::Broadcast(check_batch)),
                    schema: idx_schema,
                });
            }
        }

        // Parent DELETE → child RESTRICT. `target_id` / `source_schema` here
        // are the PARENT (the table receiving the deletes). The probe keys are
        // the *referenced parent column's* values of the removed rows, which
        // differ per child (different children may reference different columns).
        if n_children > 0 {
            // net_pk aggregation runs at batch.count scale: stay on u128 for
            // the narrow path; the wide path keys on byte-form PkBuf. The
            // *distinct* removed PKs are few, so both produce a uniform
            // `Vec<PkBuf>` consumed by the gather and per-child key building.
            let removed_pks: Vec<PkBuf> = if !wide {
                let mut net_pk: FxHashMap<u128, i64> = FxHashMap::default();
                for i in 0..batch.count {
                    let w = batch.get_weight(i);
                    if w == 0 { continue; }
                    *net_pk.entry(batch.get_pk(i)).or_insert(0) += w;
                }
                let stride = source_schema.pk_stride();
                net_pk.into_iter().filter(|&(_, w)| w < 0)
                    .map(|(k, _)| u128_to_pkbuf(k, stride)).collect()
            } else {
                let mut net_pk: FxHashMap<PkBuf, i64> = FxHashMap::default();
                for i in 0..batch.count {
                    let w = batch.get_weight(i);
                    if w == 0 { continue; }
                    *net_pk.entry(PkBuf::from_bytes(batch.get_pk_bytes(i))).or_insert(0) += w;
                }
                net_pk.into_iter().filter(|&(_, w)| w < 0).map(|(k, _)| k).collect()
            };

            if !removed_pks.is_empty() {
                // Resolve every non-PK referenced parent column in ONE batched
                // gather instead of one serial seek per (removed PK × child).
                let project = unsafe {
                    collect_fk_projection(
                        &*(*disp_ptr).catalog, target_id, n_children, &source_schema)
                };
                let gathered = if project.is_empty() {
                    FxHashMap::default()
                } else {
                    Self::execute_gather_async(
                        disp_ptr, reactor, sal_excl, target_id,
                        removed_pks.clone(), &project,
                    ).await?
                };

                for ci in 0..n_children {
                    let (child_tid, fk_col_idx, parent_col_idx, idx_schema) = unsafe {
                        let cat = &mut *(*disp_ptr).catalog;
                        let (child_tid, fk_col_idx, parent_col_idx) = match cat.get_fk_child_info(target_id, ci) {
                            Some(info) => info,
                            None => continue,
                        };
                        let idx_schema = cat.get_index_schema_by_col(child_tid, fk_col_idx as u32)
                            .ok_or_else(|| format!(
                                "FK RESTRICT check failed: no index on child table {} col {}",
                                child_tid, fk_col_idx))?;
                        (child_tid, fk_col_idx, parent_col_idx, idx_schema)
                    };

                    // PK-column target → value is on the wire inside the packed
                    // PK; extract just that column. Non-PK UNIQUE target → the
                    // delete carries no payload, so read the value resolved by
                    // the gather above. A NULL or absent referenced value is
                    // omitted from the gather map and so never blocks the
                    // delete (matching the old per-row seek's skip).
                    let keys: Vec<u128> = if source_schema.is_pk_col(parent_col_idx) {
                        let col_type = source_schema.columns[parent_col_idx].type_code;
                        let col_size = source_schema.columns[parent_col_idx].size() as usize;
                        let pk_field_off = source_schema.pk_byte_offset(parent_col_idx) as usize;
                        removed_pks.iter().map(|pk| {
                            pk_native_key(pk.pk_bytes(), pk_field_off, col_size, col_type)
                        }).collect()
                    } else {
                        let proj_pos = project.iter()
                            .position(|&c| c == parent_col_idx as u8)
                            .expect("non-PK referenced column missing from gather projection");
                        let mut vals: Vec<u128> = Vec::with_capacity(removed_pks.len());
                        for pk in &removed_pks {
                            // Zero-copy lookup via Borrow<[u8]>.
                            if let Some(row) = gathered.get(pk.pk_bytes()) {
                                if let Some(v) = row[proj_pos] {
                                    vals.push(v);
                                }
                            }
                        }
                        vals
                    };
                    if keys.is_empty() { continue; }

                    let pooled = unsafe { (*disp_ptr).pool_pop_batch(child_tid) };
                    let check_batch = build_check_batch(&idx_schema, &keys, pooled);
                    p1_labels.push(P1Label::FkRestrict { child_tid });
                    p1_checks.push(PipelinedCheck {
                        target_id: child_tid,
                        flags: FLAG_HAS_PK,
                        col_hint: (fk_col_idx as u64) + 1,
                        payload: Some(CheckPayload::Broadcast(check_batch)),
                        schema: idx_schema,
                    });
                }
            }
        }

        // UPSERT PK identification: which incoming PKs already exist in storage.
        // Routing is computed inside execute_pipeline_async; here we only build
        // the check batch (narrow u128 keys, or byte-form for wide PKs).
        let upsert_pk_batch: Option<Batch> = match (&pk_lo_hi, &pk_lo_hi_wide) {
            (Some(keys), _) if !keys.is_empty() => {
                let pooled = unsafe { (*disp_ptr).pool_pop_batch(target_id) };
                // `keys` are get_pk (OPK-widened) main-table PK values — write
                // their OPK bytes verbatim. `build_check_batch` is the *index*
                // builder: it would re-OPK-encode column 0 (double sign-flip for
                // signed) and zero the compound suffix, mangling the main-table PK
                // probe. This branch is narrow-only (`pk_lo_hi` is Some only when
                // `!source_schema.pk_is_wide()`), so `extend_pk` is valid.
                Some(build_check_batch_with(&source_schema, keys, pooled, |b, &k| b.extend_pk(k)))
            }
            (_, Some(keys)) if !keys.is_empty() => {
                let pooled = unsafe { (*disp_ptr).pool_pop_batch(target_id) };
                Some(build_check_batch_pkbuf(&source_schema, keys, pooled))
            }
            _ => None,
        };
        if let Some(check_batch) = upsert_pk_batch {
            p1_labels.push(P1Label::UpsertPkId);
            p1_checks.push(PipelinedCheck {
                target_id,
                flags: FLAG_HAS_PK,
                col_hint: 0,
                payload: Some(CheckPayload::ScatterSource { source: check_batch }),
                schema: source_schema,
            });
        }

        // ----- Phase 1 execute + interpret --------------------------------
        let mut existing_pks: FxHashSet<PkBuf> = FxHashSet::default();
        if !p1_checks.is_empty() {
            let mut p1_results = Self::execute_pipeline_async(disp_ptr, reactor, sal_excl, &mut p1_checks).await?;
            unsafe { reclaim_check_batches(&mut *disp_ptr, &mut p1_checks); }

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
                            let conflict_pk = existing_pks.iter().next().unwrap();
                            let (pk_names, sn, tn) = unsafe {
                                (*disp_ptr).pk_violation_context(target_id, &source_schema)
                            };
                            let key_str = format_pk_value_bytes(conflict_pk.pk_bytes(), &source_schema);
                            return Err(format!(
                                "duplicate key value violates unique constraint \"{}_{}_pkey\": Key ({})=({}) already exists",
                                sn, tn, pk_names, key_str,
                            ));
                        }
                    }
                }
            }
        }

        // ----- Phase 2 plan (unique index checks) -------------------------
        //
        // UPDATE of a referenced non-PK UNIQUE column can orphan child rows:
        // the validation batch holds only the new `+1` row (no retraction), so
        // the Phase-1 net-negative scan never sees the retired value. Resolve
        // the OLD committed value for each updated PK and RESTRICT-check any
        // value that both changed and is still referenced. Pipelined into the
        // Phase-2 burst alongside the unique-index broadcasts.
        let mut p2_restrict: Vec<(i64, u32, SchemaDescriptor, Vec<u128>)> = Vec::new();
        if n_children > 0 && !existing_pks.is_empty() {
            // Resolve the OLD committed values for all updated PKs in ONE
            // batched gather, rather than one serial seek per (updated PK ×
            // child). UPDATE cannot change PK columns, so only non-PK
            // referenced columns participate.
            let project = unsafe {
                collect_fk_projection(
                    &*(*disp_ptr).catalog, target_id, n_children, &source_schema)
            };

            if !project.is_empty() {
                // Batch rows that are UPDATEs: positive weight and a PK that
                // already exists in committed storage (not an INSERT). Computed
                // once; the per-child loop below indexes into this instead of
                // re-walking the whole batch and re-checking `existing_pks`.
                let update_rows: Vec<usize> = (0..batch.count)
                    .filter(|&i| batch.get_weight(i) > 0
                        && existing_pks.contains(batch.get_pk_bytes(i)))
                    .collect();

                let mut updated: Vec<PkBuf> = Vec::new();
                let mut seen: FxHashSet<PkBuf> = FxHashSet::default();
                for &i in &update_rows {
                    let pk = PkBuf::from_bytes(batch.get_pk_bytes(i));
                    if seen.insert(pk) { updated.push(pk); }
                }

                // Old values come from committed storage (validation is
                // pre-commit, under the FK table locks held by the executor,
                // so the committed parent state is static across the gather).
                let gathered = Self::execute_gather_async(
                    disp_ptr, reactor, sal_excl, target_id, updated, &project,
                ).await?;

                for ci in 0..n_children {
                    let (child_tid, fk_col_idx, parent_col_idx, idx_schema) = unsafe {
                        let cat = &mut *(*disp_ptr).catalog;
                        let (child_tid, fk_col_idx, parent_col_idx) = match cat.get_fk_child_info(target_id, ci) {
                            Some(info) => info,
                            None => continue,
                        };
                        // PK columns are immutable under UPDATE — nothing to enforce.
                        if source_schema.is_pk_col(parent_col_idx) { continue; }
                        let idx_schema = match cat.get_index_schema_by_col(child_tid, fk_col_idx as u32) {
                            Some(s) => s,
                            None => continue,
                        };
                        (child_tid, fk_col_idx, parent_col_idx, idx_schema)
                    };

                    let col_type    = source_schema.columns[parent_col_idx].type_code;
                    let col_size    = source_schema.columns[parent_col_idx].size() as usize;
                    let payload_col = source_schema.payload_idx(parent_col_idx);
                    let proj_pos = project.iter()
                        .position(|&c| c == parent_col_idx as u8)
                        .expect("non-PK referenced column missing from gather projection");

                    let mut retired: Vec<u128> = Vec::new();
                    for &i in &update_rows {
                        let new_val = if batch.get_null_word(i) & (1u64 << payload_col) != 0 {
                            None
                        } else {
                            Some(payload_native_key(batch.col_data(payload_col), i * col_size, col_size, col_type))
                        };

                        // Absent committed row → omitted from the gather map →
                        // skip (matches the old per-row seek's None). Zero-copy
                        // lookup via Borrow<[u8]>.
                        let old_val = match gathered.get(batch.get_pk_bytes(i)) {
                            Some(row) => row[proj_pos],
                            None => continue,
                        };

                        // Only a changed, non-null old value can orphan child rows.
                        if old_val != new_val {
                            if let Some(v) = old_val { retired.push(v); }
                        }
                    }
                    if !retired.is_empty() {
                        p2_restrict.push((child_tid, fk_col_idx as u32, idx_schema, retired));
                    }
                }
            }
        }

        if !has_unique && p2_restrict.is_empty() {
            return Ok(());
        }

        // Lazily warm the unique-index filters. In the async path this
        // may trigger a scan; the scan is itself async.
        Self::ensure_unique_filters_warm_async(disp_ptr, reactor, sal_excl, target_id).await?;

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
            let is_pk_col = source_schema.is_pk_col(source_col);
            let src_payload_idx = if is_pk_col {
                usize::MAX
            } else {
                source_schema.payload_idx(source_col)
            };
            let col_size = source_schema.columns[source_col].size() as usize;
            let pk_field_off = if is_pk_col {
                source_schema.pk_byte_offset(source_col) as usize
            } else { 0 };

            // Index-column key is always ≤ 16 bytes → u128. Row PK is byte-form
            // (PkBuf) to support wide PKs.
            let mut upsert_keys: Vec<(u128, PkBuf)> = Vec::new();
            let mut check_keys: Vec<u128> = Vec::new();
            let mut seen: FxHashSet<u128> = FxHashSet::default();

            for i in 0..batch.count {
                if batch.get_weight(i) <= 0 { continue; }

                if !is_pk_col {
                    let null_word = batch.get_null_word(i);
                    if null_word & (1u64 << src_payload_idx) != 0 { continue; }
                }

                // Decode the indexed PK column OPK→native. `get_pk` yields the
                // OPK-widened value (sign-flipped for signed), which the
                // downstream `index_opk_prefix` would re-OPK-encode — seeking the
                // wrong key for a signed single PK. `pk_native_key` is correct for
                // every width/signedness and equals `get_pk` for unsigned singles.
                let key = if is_pk_col {
                    pk_native_key(
                        batch.get_pk_bytes(i), pk_field_off, col_size, type_code)
                } else {
                    let col_data = batch.col_data(src_payload_idx);
                    payload_native_key(col_data, i * col_size, col_size, type_code)
                };

                // In-batch duplicate detection runs for ALL positive-weight
                // rows, INCLUDING UPSERTs: two rows setting the same new unique
                // value in one transaction is a violation regardless of whether
                // their PKs already exist.
                if !seen.insert(key) {
                    return Err(unsafe {
                        (*disp_ptr).unique_violation_err(target_id, source_col, true)
                    });
                }

                // Zero-copy lookup via Borrow<[u8]>.
                if existing_pks.contains(batch.get_pk_bytes(i)) {
                    upsert_keys.push((key, PkBuf::from_bytes(batch.get_pk_bytes(i))));
                    continue;
                }
                check_keys.push(key);
            }

            if !check_keys.is_empty() {
                let skip_broadcast = unsafe {
                    (*disp_ptr).unique_filter_all_absent(target_id, col_idx, &check_keys)
                };

                if !skip_broadcast {
                    let pooled = unsafe { (*disp_ptr).pool_pop_batch(target_id) };
                    let chk_batch = build_check_batch(&idx_schema, &check_keys, pooled);
                    p2_labels.push(P2Label::NonUpsert { source_col });
                    p2_checks.push(PipelinedCheck {
                        target_id,
                        flags: FLAG_HAS_PK,
                        col_hint: (col_idx as u64) + 1,
                        payload: Some(CheckPayload::Broadcast(chk_batch)),
                        schema: idx_schema,
                    });
                }
            }

            if !upsert_keys.is_empty() {
                let u_keys: Vec<u128> = upsert_keys.iter().map(|&(k, _)| k).collect();
                let pooled = unsafe { (*disp_ptr).pool_pop_batch(target_id) };
                let u_batch = build_check_batch(&idx_schema, &u_keys, pooled);
                let idx_pk_stride = idx_schema.pk_stride();
                let idx_key_type = idx_schema.columns[0].type_code;
                let idx_key_size = idx_schema.columns[0].size() as usize;
                p2_labels.push(P2Label::Upsert {
                    col_idx, source_col, idx_key_type, idx_key_size, idx_pk_stride, upsert_keys,
                });
                p2_checks.push(PipelinedCheck {
                    target_id,
                    flags: FLAG_HAS_PK,
                    col_hint: (col_idx as u64) + 1,
                    payload: Some(CheckPayload::Broadcast(u_batch)),
                    schema: idx_schema,
                });
            }
        }

        // RESTRICT probes for referenced UNIQUE values retired by UPDATE.
        for (child_tid, fk_col, idx_schema, keys) in p2_restrict {
            let pooled = unsafe { (*disp_ptr).pool_pop_batch(child_tid) };
            let chk_batch = build_check_batch(&idx_schema, &keys, pooled);
            p2_labels.push(P2Label::FkRestrict { child_tid });
            p2_checks.push(PipelinedCheck {
                target_id: child_tid,
                flags: FLAG_HAS_PK,
                col_hint: (fk_col as u64) + 1,
                payload: Some(CheckPayload::Broadcast(chk_batch)),
                schema: idx_schema,
            });
        }

        if p2_checks.is_empty() {
            return Ok(());
        }

        let p2_results = Self::execute_pipeline_async(disp_ptr, reactor, sal_excl, &mut p2_checks).await?;
        unsafe { reclaim_check_batches(&mut *disp_ptr, &mut p2_checks); }

        for (idx, label) in p2_labels.iter().enumerate() {
            match label {
                P2Label::NonUpsert { source_col } => {
                    if !p2_results[idx].is_empty() {
                        return Err(unsafe {
                            (*disp_ptr).unique_violation_err(target_id, *source_col, false)
                        });
                    }
                }
                P2Label::FkRestrict { child_tid } => {
                    if !p2_results[idx].is_empty() {
                        let (sn, tn, csn, ctn) = unsafe {
                            let disp = &mut *disp_ptr;
                            let (s, t) = disp.get_qualified_name_owned(target_id);
                            let (cs, ct) = disp.get_qualified_name_owned(*child_tid);
                            (s, t, cs, ct)
                        };
                        return Err(format!(
                            "Foreign Key violation: cannot update '{}.{}', row still referenced by '{}.{}'",
                            sn, tn, csn, ctn,
                        ));
                    }
                }
                P2Label::Upsert { col_idx, source_col, idx_key_type, idx_key_size, idx_pk_stride, upsert_keys } => {
                    // Fan out all seeks before collecting: sal_excl is released
                    // before each reply wait, so the seek futures run
                    // concurrently rather than serializing one RTT per key.
                    let occupied = &p2_results[idx];
                    let mut pk_is: Vec<PkBuf> = Vec::new();
                    let mut futs = Vec::new();
                    for &(key_pk, pk_i) in upsert_keys {
                        // occupied holds index-table PKs as the probe wrote them:
                        // index_opk_prefix(native key) ++ zero suffix — i.e. OPK
                        // bytes, not native LE. Reproduce that encoding exactly
                        // before the membership test (the returned buf is zero-
                        // filled past idx_key_size, so one slice covers narrow and
                        // wide index PKs).
                        let buf = crate::schema::index_opk_prefix(key_pk, *idx_key_type, *idx_key_size);
                        if !occupied.contains(&buf[..*idx_pk_stride as usize]) { continue; }
                        pk_is.push(pk_i);
                        // async fn futures are !Unpin; box-pin for join_all_unpin.
                        futs.push(Box::pin(Self::fan_out_seek_by_index_async(
                            disp_ptr, reactor, sal_excl, target_id, *col_idx, key_pk,
                        )));
                    }
                    let slots = crate::runtime::reactor::join_all_unpin(futs).await;
                    for (pk_i, slot_result) in pk_is.into_iter().zip(slots) {
                        let slot = slot_result?;
                        let ctrl = peek_control_block(slot.bytes())
                            .map_err(|e| e.to_string())?;
                        if ctrl.flags & FLAG_HAS_DATA != 0 {
                            let zc = wire::decode_wire_ipc_zero_copy_with_ctrl(
                                slot.bytes(), ctrl.block_size, ctrl, None,
                            ).map_err(|e| e.to_string())?;
                            if let Some(ref found) = zc.data_batch {
                                if found.count > 0 && found.get_pk_bytes(0) != pk_i.pk_bytes() {
                                    return Err(unsafe {
                                        (*disp_ptr).unique_violation_err(target_id, *source_col, false)
                                    });
                                }
                            }
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
        reactor: &crate::runtime::reactor::Reactor,
        sal_excl: &Rc<AsyncMutex<()>>,
        table_id: i64,
    ) -> Result<(), String> {
        let (missing, mut guard): (Vec<UniqueIndexDesc>, WarmupGuard) = unsafe {
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
            let missing_cols: Vec<u32> = missing.iter().map(|d| d.col_idx).collect();
            let guard = WarmupGuard {
                disp_ptr,
                table_id,
                cols: missing_cols,
                disarmed: false,
            };
            (missing, guard)
        };

        // `_lease` held across the full continuation drain below; its workers
        // stream multi-frame trains, so releasing the gate early (e.g. on a
        // mid-scan cancellation) would let a still-streaming worker wedge.
        let (slots, req_ids, _lease) = dispatch_scan_fanout(disp_ptr, reactor, sal_excl, |disp, req_ids| {
            let (schema, col_names) = disp.get_schema_and_names(table_id);
            let lsn = disp.next_lsn();
            disp.write_group_with_req_ids(
                table_id, lsn, 0, 0, &[], &schema, &col_names,
                0, 0, req_ids, -1, 0, None, &[],
            )
        }).await?;

        // Drain every worker's continuation-frame train into the cold filters.
        // `drain_index_scan` owns the wedge-safety invariant (every frame from
        // every worker is consumed, even on a worker-side error), the zero-copy
        // `MemBatch` lifetime, and the continuation-schema-hint handling.
        let scan_result = drain_index_scan(slots, &req_ids, reactor, |mb| unsafe {
            let disp = &mut *disp_ptr;
            for d in &missing {
                if let Some(filter) = disp.unique_filters.get_mut(&(table_id, d.col_idx)) {
                    if !filter.capped { extract_into_filter(filter, mb, d); }
                }
            }
        }).await;

        // On success the filters are fully populated → mark warm so the
        // broadcast-skip shortcut may trust them. Disarm the guard so the
        // drop handler does not remove the now-warm entries. On failure (worker
        // crash mid-scan or cancellation) let the guard's Drop handler remove
        // the cold entries so the next validation retries warmup from scratch.
        match scan_result {
            Ok(()) => {
                let disp = unsafe { &mut *disp_ptr };
                for d in &missing {
                    if let Some(f) = disp.unique_filters.get_mut(&(table_id, d.col_idx)) {
                        f.warm = true;
                    }
                }
                guard.disarmed = true;
                Ok(())
            }
            Err(e) => {
                // Guard is not disarmed → Drop removes the cold entries.
                Err(e)
            }
        }
    }

    /// Pre-flight global uniqueness check for CREATE UNIQUE INDEX. Scans the
    /// owner table across ALL workers, decodes the indexed column of every row
    /// to its native key, and fails if any value repeats — catching both
    /// within-partition and cross-partition duplicates that no per-worker
    /// `backfill_index` can see. On success the index is safe to commit and
    /// broadcast and the returned distinct key set seeds the master's unique
    /// filter; on failure the caller returns a client error and never
    /// broadcasts, so no worker reaches the fatal `DdlSync` backfill path.
    ///
    /// MUST run inside the DDL critical section (committer barrier drained,
    /// catalog write lock held) and BEFORE the IDX_TAB +1 is appended/broadcast,
    /// so the scanned snapshot is exactly the data each worker will later
    /// backfill and no concurrent INSERT can be ordered between the snapshot and
    /// the backfill.
    ///
    /// An out-of-range `col_idx` yields an empty set (nothing to validate).
    pub async fn validate_unique_index_create_async(
        disp_ptr: *mut MasterDispatcher,
        reactor: &crate::runtime::reactor::Reactor,
        sal_excl: &Rc<AsyncMutex<()>>,
        owner_id: i64,
        col_idx: u32,
    ) -> Result<FxHashSet<u128>, String> {
        let desc = match unsafe { (*disp_ptr).unique_index_desc_for_col(owner_id, col_idx) } {
            Some(d) => d,
            None => return Ok(FxHashSet::default()), // column out of range ⇒ nothing to validate
        };

        // Fan out a full-table Scan (sal_flags = 0 ⇒ SalMessageKind::Scan); each
        // worker streams its local partition rows in continuation frames. Same
        // primitive as the warmup.
        // `_lease` held across the full continuation drain below; the CREATE
        // UNIQUE INDEX validator scans every worker's whole partition train,
        // so the gate must stay open until the drain (or a cancellation) ends.
        let (slots, req_ids, _lease) = dispatch_scan_fanout(disp_ptr, reactor, sal_excl, |disp, req_ids| {
            let (schema, col_names) = disp.get_schema_and_names(owner_id);
            let lsn = disp.next_lsn();
            disp.write_group_with_req_ids(
                owner_id, lsn, 0, 0, &[], &schema, &col_names,
                0, 0, req_ids, -1, 0, None, &[],
            )
        })
        .await?;

        // Exact, uncapped collision set: correctness requires catching ANY
        // duplicate, so unlike `UniqueFilter` (capped at UNIQUE_FILTER_CAP) this
        // never gives up. The index key is ≤16 bytes (unique STRING/BLOB rejected
        // at create time; float/STRING rejected for any index by the index key
        // type), so the u128 key is lossless.
        let mut seen: FxHashSet<u128> = FxHashSet::default();
        let mut duplicate = false;
        drain_index_scan(slots, &req_ids, reactor, |mb| {
            if duplicate { return; } // stop inserting; drain keeps consuming frames
            for_each_index_key(mb, &desc, |key| {
                if !seen.insert(key) { duplicate = true; }
                !duplicate // stop walking this frame once a dup is found
            });
        })
        .await?;

        if duplicate {
            return Err(unsafe {
                (*disp_ptr).unique_create_dup_err(owner_id, col_idx as usize)
            });
        }
        Ok(seen)
    }

    /// Seed the `(table_id, col_idx)` filter from a complete distinct key set
    /// captured under the catalog write lock (the CREATE-time pre-flight). Marks
    /// it warm so the first INSERT skips `ensure_unique_filters_warm_async`. Over
    /// the cap ⇒ leave the filter absent so the lazy path caps it exactly as it
    /// would. Symmetric counterpart of `unique_filter_remove_col`, keeping the
    /// capping invariant in one place.
    pub(crate) fn unique_filter_seed(
        &mut self, table_id: i64, col_idx: u32, seen: FxHashSet<u128>,
    ) {
        if seen.len() > UNIQUE_FILTER_CAP { return; }
        let mut filter = UniqueFilter::new();
        filter.values = seen;   // exact distinct set; same type, move not re-hash
        filter.warm = true;     // pre-flight scanned every worker under the write lock
        self.unique_filters.insert((table_id, col_idx), filter);
    }

    /// Commit N push batches as a single SAL group write. Called from
    /// the committer task. Returns (groups, req_ids, fsync_id) — the
    /// caller awaits fsync + per-worker req_ids separately so they can
    /// `join!` them. `lsn` is supplied by the caller.
    pub(crate) fn write_commit_group(
        &mut self,
        target_id: i64, lsn: u64, batch: &Batch, mode: WireConflictMode,
        req_ids: &[u64],
    ) -> Result<(), String> {
        let (schema, schema_block, wire_safe, wire_row_stride) =
            self.cached_schema_block(target_id);
        let pk_col = schema.pk_indices();
        let wire_flags = wire_flags_set_conflict_mode(0, mode);
        with_worker_indices(batch, pk_col, &schema, self.num_workers, |worker_indices| {
            self.record_index_routing(target_id, &schema, batch, worker_indices);
            self.sal.scatter_wire_group(
                batch, worker_indices, &schema, None,
                target_id as u32, lsn, FLAG_PUSH,
                wire_flags,
                0, 0, req_ids, -1,
                Some(schema_block.as_slice()),
                Some((wire_safe, wire_row_stride)),
            )
        })
    }

    /// Write a FLAG_FLUSH checkpoint group with per-worker req_ids.
    /// Does NOT sync/signal. Caller signals + awaits replies. `lsn` is
    /// supplied by the caller.
    pub(crate) fn write_checkpoint_group(
        &mut self, lsn: u64, req_ids: &[u64],
    ) -> Result<(), String> {
        let schema = SchemaDescriptor::minimal_u64();
        // One "slot" per worker with empty batch — each worker replies
        // after flushing its system tables and advancing its epoch.
        let refs: Vec<Option<&Batch>> = (0..self.num_workers).map(|_| None).collect();
        self.write_group_with_req_ids(
            0, lsn, FLAG_FLUSH, 0, &refs, &schema, &[], 0, 0, req_ids, -1, 0, None, &[],
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
        // Now safe: every worker ACKed the FLUSH, so all have consumed past any
        // DROP that gated a directory — hence finished the matching CREATE.
        cat.drain_checkpoint_gated_deletions();
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

    /// Delegate unique-index violation formatting to the catalog, the single
    /// source of truth for the message text and the table/column name lookups.
    fn unique_violation_err(&mut self, target_id: i64, col_idx: usize, in_batch: bool) -> String {
        unsafe { (*self.catalog).unique_violation_err(target_id, col_idx, in_batch) }
    }

    /// Delegate `CREATE UNIQUE INDEX` duplicate-value rejection to the catalog.
    fn unique_create_dup_err(&mut self, owner_id: i64, col_idx: usize) -> String {
        unsafe { (*self.catalog).unique_create_dup_err(owner_id, col_idx) }
    }

    /// Build the `(pk_names_joined, schema_name, table_name)` triple used
    /// in PG-style "violates unique constraint \"{sn}_{tn}_pkey\": ... key
    /// ({pk_names_joined})=(...)" error messages. Compound PKs join column
    /// names with commas in declaration order.
    fn pk_violation_context(
        &mut self, target_id: i64, schema: &SchemaDescriptor,
    ) -> (String, String, String) {
        let names: Vec<String> = schema.pk_indices().iter()
            .map(|&ci| self.get_col_name(target_id, ci as usize))
            .collect();
        let (sn, tn) = self.get_qualified_name_owned(target_id);
        (names.join(", "), sn, tn)
    }

    /// Format the "two rows in one batch share a PK" rejection message.
    /// `key_str` is the already-rendered offending key (narrow or wide).
    fn batch_dup_pk_err(
        &mut self, target_id: i64, schema: &SchemaDescriptor, key_str: &str,
    ) -> String {
        let (pk_names, sn, tn) = self.pk_violation_context(target_id, schema);
        format!(
            "duplicate key value violates unique constraint \"{}_{}_pkey\": Batch contains multiple rows with key ({})=({})",
            sn, tn, pk_names, key_str,
        )
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
    worker_error_scan(op, decoded.iter().enumerate())
}

/// Variant of `first_worker_error` for the `Vec<Option<DecodedWire>>` slot
/// shape produced by `join_into`. Slots are guaranteed `Some` after the
/// future resolves; an unfilled slot indicates a bug in the join driver.
pub(crate) fn first_worker_error_opt(op: &str, decoded: &[Option<DecodedWire>])
    -> Option<String>
{
    worker_error_scan(
        op,
        decoded.iter().enumerate().map(|(w, d)| {
            (w, d.as_ref().expect("join_into left a None slot — logic bug"))
        }),
    )
}

fn worker_error_scan<'a>(
    op: &str,
    it: impl Iterator<Item = (usize, &'a DecodedWire)>,
) -> Option<String> {
    for (w, d) in it {
        if d.control.status != 0 {
            let msg = String::from_utf8_lossy(&d.control.error_msg);
            return Some(format!("worker {}: {}: {}", w, op, msg));
        }
    }
    None
}

/// Allocate `nw` per-worker scan request ids, run `submit`, signal all
/// workers, then await every slot via `join_all`. Returns the raw `W2mSlot`s
/// in worker order so the caller can forward them to the client without an
/// intermediate decode/copy.
///
/// `sal_excl` is held only for the synchronous write + signal phase and
/// released before awaiting replies. This serialises the SAL write against
/// concurrent checkpoint FLAG_FLUSH groups: without the lock a fan-out
/// could write with the old epoch during the checkpoint window, workers
/// would skip it, and the caller would hang waiting for an ACK.
fn scan_decode_err(w: usize, e: &'static str) -> String {
    format!("scan: worker {w}: decode error: {e}")
}

pub(crate) async fn dispatch_scan_fanout<F>(
    disp_ptr: *mut MasterDispatcher,
    reactor: &crate::runtime::reactor::Reactor,
    sal_excl: &Rc<AsyncMutex<()>>,
    submit: F,
) -> Result<(Vec<W2mSlot>, [u64; crate::runtime::sal::MAX_WORKERS], ScanLease), String>
where
    F: FnOnce(&mut MasterDispatcher, &[u64]) -> Result<(), String>,
{
    let nw = unsafe { (*disp_ptr).num_workers };
    let mut req_ids = [0u64; crate::runtime::sal::MAX_WORKERS];
    for id in req_ids[..nw].iter_mut() {
        *id = reactor.alloc_scan_request_id();
    }

    // Register the ids active BEFORE any await so a cancelled first-frame await
    // still deregisters them and route_scan_slot discards late frames. The
    // returned lease MUST be bound to a named local held to end of the caller's
    // drain scope (never a bare `_`, which would drop it immediately and
    // re-open the wedge).
    let mut scan_ids = [0u32; crate::runtime::sal::MAX_WORKERS];
    for (d, &s) in scan_ids[..nw].iter_mut().zip(&req_ids[..nw]) { *d = s as u32; }
    let lease = reactor.scan_lease(&scan_ids[..nw]);

    {
        let _guard = sal_excl.lock().await;
        unsafe {
            let disp = &mut *disp_ptr;
            submit(disp, &req_ids[..nw])?;
            disp.signal_all();
        }
    }
    let slots = crate::runtime::reactor::join_all_unpin(
        req_ids[..nw].iter().map(|&id| reactor.await_scan_slot(id as u32))
    ).await;
    Ok((slots, req_ids, lease))
}

/// Fan-out scan drain shared by the unique-filter warmup and the CREATE-time
/// validator. Invokes `on_batch` with the zero-copy `MemBatch` of every
/// non-empty continuation frame, in worker order.
///
/// Drains EVERY worker's frame train to its terminal frame before returning:
/// `FLAG_SCAN_LAST` on a successful train, or a single `STATUS_ERROR` frame on
/// a worker-side scan fault (a fault frame is terminal — the worker emits no
/// further frames for that request, so the drain stops that worker and moves to
/// the next). A worker whose scan train exceeds its W2M ring blocks in
/// `send_encoded` until the master drains it (`w2m.rs`), so failing to drain a
/// still-streaming worker would wedge it. The first worker / decode error is
/// captured and surfaced only after every worker is drained. `on_batch` may
/// stop doing useful work at any point (filter capped, duplicate found); the
/// drain continues anyway.
///
/// Continuation frames carry no schema; the schema + version saved from the
/// first frame is reused as a decode hint. The zero-copy `MemBatch` borrows
/// from `slot.bytes()`, so the slot is dropped only after `on_batch` returns.
async fn drain_index_scan(
    slots: Vec<W2mSlot>,
    req_ids: &[u64; crate::runtime::sal::MAX_WORKERS],
    reactor: &crate::runtime::reactor::Reactor,
    mut on_batch: impl FnMut(&crate::storage::MemBatch<'_>),
) -> Result<(), String> {
    let mut deferred: Option<String> = None;
    for (w, mut slot) in slots.into_iter().enumerate() {
        let mut saved_schema: Option<(SchemaDescriptor, u16)> = None;
        loop {
            // A corrupt header yields no frame structure: abandon THIS worker's
            // drain (the outer loop still drains the rest) rather than wedge the
            // whole fan-out. peek_control_block returns an owned ControlBlock.
            let ctrl = match peek_control_block(slot.bytes()) {
                Ok(c) => c,
                Err(e) => {
                    if deferred.is_none() { deferred = Some(scan_decode_err(w, e)); }
                    drop(slot);
                    break;
                }
            };
            // A worker fault is reported by `send_error` as a single frame with
            // `status = STATUS_ERROR` and flags `0` (no `FLAG_SCAN_LAST`), tagged
            // with this scan's request_id, and is terminal — the worker emits
            // nothing more for the request. `has_more` MUST gate on status:
            // keying off `FLAG_SCAN_LAST` alone reads the `0` flags as "more
            // coming" and blocks forever in `await_scan_slot` on a frame that
            // never arrives.
            let has_more = ctrl.status == 0 && (ctrl.flags & FLAG_SCAN_LAST == 0);
            if ctrl.status != 0 {
                if deferred.is_none() {
                    deferred = Some(format!(
                        "worker {}: scan: {}",
                        w, String::from_utf8_lossy(&ctrl.error_msg)));
                }
                // terminal fault frame: `has_more` is false, so the loop breaks
                // below and the outer `for` advances to the next worker.
            } else if deferred.is_none() {
                let server_version = gnitz_wire::wire_flags_get_schema_version(ctrl.flags);
                let ctrl_size = ctrl.block_size;
                let schema_hint = saved_schema.as_ref().map(|(s, v)| SchemaWithVersion {
                    descriptor: s, version: *v,
                });
                match wire::decode_wire_ipc_zero_copy_with_ctrl(
                    slot.bytes(), ctrl_size, ctrl, schema_hint,
                ) {
                    Ok(zc) => {
                        if saved_schema.is_none() {
                            if let Some(ref s) = zc.schema {
                                saved_schema = Some((*s, server_version));
                            }
                        }
                        if let Some(ref mb) = zc.data_batch {
                            if mb.count > 0 { on_batch(mb); }
                        }
                        // zc borrows `slot`; dropped here, before `drop(slot)`.
                    }
                    Err(e) => {
                        if deferred.is_none() { deferred = Some(scan_decode_err(w, e)); }
                    }
                }
            }
            drop(slot);
            if !has_more { break; }
            slot = reactor.await_scan_slot(req_ids[w] as u32).await;
        }
    }
    match deferred {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

/// Common body for every single-worker async fan-out. Submits the SAL
/// message, signals the worker, yields, and returns the raw `W2mSlot` so the
/// caller can forward it to the client without an intermediate decode/copy.
/// The closures in the public wrappers compute the worker; everything else
/// is here so there is a single place to maintain the unsafe
/// borrow-and-release pattern.
///
/// `sal_excl` is held only for the synchronous write + signal phase;
/// see `dispatch_scan_fanout` for the rationale.
#[allow(clippy::too_many_arguments)]
async fn single_worker_async(
    disp_ptr: *mut MasterDispatcher,
    reactor: &crate::runtime::reactor::Reactor,
    sal_excl: &Rc<AsyncMutex<()>>,
    target_id: i64,
    flags: u32,
    worker: usize,
    seek_pk: u128, seek_col_idx: u64,
    op_name: &'static str,
    seek_pk_extra: &[u8],
) -> Result<W2mSlot, String> {
    let req_id = {
        let _guard = sal_excl.lock().await;
        unsafe {
            let disp = &mut *disp_ptr;
            let (schema, col_names) = disp.get_schema_and_names(target_id);
            let req_id = reactor.alloc_scan_request_id();
            let lsn = disp.next_lsn();
            disp.write_group(target_id, lsn, flags, &[], &schema, &col_names,
                             seek_pk, seek_col_idx, req_id, worker as i32, seek_pk_extra)?;
            disp.signal_one(worker);
            req_id
        }
    };
    // Hold the scan active across the await; without this the gate in
    // route_scan_slot discards the reply and the await never resolves. Dropped
    // on return (nothing to purge) or on cancellation (freeing any in-flight
    // slot). One frame, no continuation train, so the lease lives in this body.
    let _lease = reactor.scan_lease(&[req_id as u32]);
    let slot = reactor.await_scan_slot(req_id as u32).await;
    let ctrl = peek_control_block(slot.bytes())
        .expect("W2M ctrl corrupt in single_worker_async");
    if ctrl.status != 0 {
        let msg = String::from_utf8_lossy(&ctrl.error_msg);
        return Err(format!("worker {}: {}: {}", worker, op_name, msg));
    }
    Ok(slot)
}

fn format_uuid_hyphenated(v: u128) -> String {
    format!("{:08x}-{:04x}-{:04x}-{:04x}-{:012x}",
        (v >> 96) as u32,
        (v >> 80) as u16,
        (v >> 64) as u16,
        (v >> 48) as u16,
        v & 0x0000_ffff_ffff_ffff)
}

/// Render a PK from its raw OPK byte form for error messages.
/// Compound PKs are formatted as comma-separated per-column values in
/// declaration order; `pk_bytes` holds all PK columns concatenated as OPK
/// (big-endian, sign-flipped for signed), so we slice and decode each column
/// back to native before rendering. Works for wide PKs (`pk_stride > 16`)
/// where a `u128` cannot encode the key.
fn format_pk_value_bytes(pk_bytes: &[u8], schema: &SchemaDescriptor) -> String {
    let mut parts: Vec<String> = Vec::new();
    let mut off = 0usize;
    for &ci in schema.pk_indices() {
        let col = schema.columns[ci as usize];
        let size = col.size() as usize;
        // pk_bytes are OPK; decode each column back to native LE before reading
        // its scalar value. Reading OPK as native LE would render garbage for
        // signed columns (flipped sign bit) and any multi-byte unsigned column.
        let mut le = [0u8; 16];
        gnitz_wire::decode_pk_column(&pk_bytes[off..off + size], col.type_code, &mut le[..size]);
        let v = u128::from_le_bytes(le);
        let s = match col.type_code {
            crate::schema::type_code::U128 => format!("{}", v),
            crate::schema::type_code::UUID => format_uuid_hyphenated(v),
            crate::schema::type_code::I64  => format!("{}", v as u64 as i64),
            crate::schema::type_code::I32  => format!("{}", v as u64 as i32),
            crate::schema::type_code::I16  => format!("{}", v as u64 as i16),
            crate::schema::type_code::I8   => format!("{}", v as u64 as i8),
            _ => format!("{}", v as u64),
        };
        parts.push(s);
        off += size;
    }
    parts.join(", ")
}

/// Narrow-PK convenience over `format_pk_value_bytes`. `pk` is a `get_pk`
/// (OPK-widened) value, whose OPK bytes at rest are the trailing `pk_stride`
/// bytes of its big-endian image.
fn format_pk_value(pk: u128, schema: &SchemaDescriptor) -> String {
    let stride = schema.pk_stride() as usize;
    let be = pk.to_be_bytes();
    format_pk_value_bytes(&be[16 - stride..], schema)
}

/// Shared scaffold for the check-batch builders: pool-reuse with a schema
/// staleness guard, then one zero-payload row per key.
///
/// Schema staleness guard: a pooled batch built before a DDL change has
/// the wrong column layout; populating it would silently corrupt rows or
/// panic on column writes. When `pooled.schema != Some(schema)`, the
/// pooled allocation is dropped and a fresh batch is allocated instead.
///
/// `push_pk` writes the per-row PK region (the only step that differs
/// between the `u128` and `PkBuf` key forms).
fn build_check_batch_with<K>(
    schema: &SchemaDescriptor,
    keys: &[K],
    pooled: Option<Batch>,
    mut push_pk: impl FnMut(&mut Batch, &K),
) -> Batch {
    let npc = schema.num_payload_cols();
    let mut batch = match pooled {
        Some(b) if b.schema.as_ref() == Some(schema) => {
            let mut b = b;
            b.clear();
            b.reserve_rows(keys.len());
            b
        }
        _ => Batch::with_schema(*schema, keys.len()),
    };
    let null_word: u64 = crate::ops::util::all_payload_null_mask(npc);
    for key in keys {
        batch.ensure_row_capacity();
        push_pk(&mut batch, key);
        batch.extend_weight(&1i64.to_le_bytes());
        batch.extend_null_bmp(&null_word.to_le_bytes());
        for (c, _ci, col) in schema.payload_columns() {
            batch.fill_col_zero(c, col.size() as usize);
        }
        batch.count += 1;
    }
    batch
}

/// Build a constraint-check batch from narrow `u128` PK keys.
fn build_check_batch(
    schema: &SchemaDescriptor,
    keys: &[u128],
    pooled: Option<Batch>,
) -> Batch {
    // The index PK composite is `(indexed-value, src_pk_cols)` and is OPK-at-
    // rest. `keys` carry the indexed value (native u128); OPK-encode it into the
    // leading promoted key column and leave the source-PK suffix zero — only the
    // leading column is prefix-matched for the existence check. Narrow and wide
    // composites share this layout (the suffix width differs, the leading does
    // not), so there is no narrow/wide split.
    let stride = schema.pk_stride() as usize;
    let idx_key_size = schema.columns[0].size() as usize;
    let idx_key_type = schema.columns[0].type_code;
    build_check_batch_with(schema, keys, pooled, |b, &k| {
        let buf = crate::schema::index_opk_prefix(k, idx_key_type, idx_key_size);
        b.extend_pk_bytes(&buf[..stride]);
    })
}

/// Convert a narrow-PK `get_pk` (OPK-widened) value to its byte-form `PkBuf`.
/// `pk`'s OPK bytes at rest are the trailing `pk_stride` bytes of its big-endian
/// image (exactly what `extend_pk` writes); `to_le_bytes` would reverse them and
/// probe a key matching no stored row. Used at gather call sites to feed the
/// unified `Vec<PkBuf>` input once per call (not per incoming batch row).
#[inline]
fn u128_to_pkbuf(pk: u128, pk_stride: u8) -> PkBuf {
    PkBuf::from_bytes(&pk.to_be_bytes()[16 - pk_stride as usize..])
}

/// Encode a native index-seek key into the routing-cache representation
/// `extract_col_key` stores: OPK-widened for integer / U128 / UUID columns.
/// Returns `None` for STRING/BLOB, whose cache keys are XXH3 of the bytes and
/// cannot be rebuilt from a `u128` — those seeks fall through to the broadcast
/// path (which is already correct). `extract_col_key`'s PK-column and integer-
/// payload arms both reduce to `widen_pk_be(encode_pk_column(native))`, so one
/// encoder covers both regardless of whether the column is itself a PK column.
fn index_route_key(schema: &SchemaDescriptor, col_idx: u32, native: u128) -> Option<u128> {
    use crate::schema::type_code;
    let col = schema.columns[col_idx as usize];
    match col.type_code {
        type_code::STRING | type_code::BLOB => None,
        type_code::U128 | type_code::UUID => Some(native),
        _ => {
            let sz = col.size() as usize;
            let mut opk = [0u8; 16];
            gnitz_wire::encode_pk_column(&native.to_le_bytes()[..sz], col.type_code, &mut opk[..sz]);
            Some(gnitz_wire::widen_pk_be(&opk[..sz], sz))
        }
    }
}

/// Byte-form sibling of `build_check_batch`, taking `&[PkBuf]` keys. Kept
/// as a distinct `&[PkBuf]` entry point (rather than converting to `u128`)
/// because the `pk_lo_hi` → UPSERT PK-check path can have up to
/// `batch.count` entries and must not pay per-entry `PkBuf` construction;
/// the gather inputs are bounded by distinct changed rows.
fn build_check_batch_pkbuf(
    schema: &SchemaDescriptor,
    keys: &[PkBuf],
    pooled: Option<Batch>,
) -> Batch {
    build_check_batch_with(schema, keys, pooled, |b, k| b.extend_pk_bytes(k.pk_bytes()))
}

/// Return `batch` to `disp.check_batch_pool[target_id]` and cap the pool depth.
fn recycle_check_batch(disp: &mut MasterDispatcher, target_id: i64, batch: Batch) {
    const POOL_MAX_DEPTH: usize = 4;
    const MAX_RETAIN_BYTES: usize = 512 * 1024;
    // A single large validation batch (bulk load, large FK check) would pin its
    // allocation in the pool indefinitely — Batch::clear() doesn't shrink.
    if batch.total_bytes() > MAX_RETAIN_BYTES {
        return;  // let the allocator reclaim the oversized buffer
    }
    let pool = disp.check_batch_pool.entry(target_id).or_default();
    pool.push(batch);
    if pool.len() > POOL_MAX_DEPTH {
        pool.remove(0);
    }
}

/// Take each `Batch` out of `checks` via `Option::take` (no pool round-trip for
/// a sentinel), push it into `disp.check_batch_pool[target_id]`, and cap the
/// pool depth. Called after `execute_pipeline_async` to recycle allocations.
fn reclaim_check_batches(disp: &mut MasterDispatcher, checks: &mut [PipelinedCheck]) {
    for check in checks.iter_mut() {
        if let Some(payload) = check.payload.take() {
            let batch = match payload {
                CheckPayload::Broadcast(b) => b,
                CheckPayload::ScatterSource { source } => source,
            };
            recycle_check_batch(disp, check.target_id, batch);
        }
    }
}

/// Collect the distinct non-PK parent columns referenced by `target_id`'s FK
/// children, as a projection list for `execute_gather_async`. A PK-target child
/// reads its referenced value from the packed PK on the wire and needs no
/// gather, so PK columns are excluded.
fn collect_fk_projection(
    cat: &CatalogEngine,
    target_id: i64,
    n_children: usize,
    source_schema: &SchemaDescriptor,
) -> Vec<u8> {
    let mut project: Vec<u8> = Vec::new();
    for ci in 0..n_children {
        if let Some((_, _, parent_col_idx)) = cat.get_fk_child_info(target_id, ci) {
            if !source_schema.is_pk_col(parent_col_idx)
                && !project.contains(&(parent_col_idx as u8))
            {
                project.push(parent_col_idx as u8);
            }
        }
    }
    project
}

#[cfg(test)]
mod unique_filter_tests {
    use super::*;
    use crate::schema::{SchemaColumn, type_code};

    fn u64_schema() -> SchemaDescriptor {
        // Single U64 PK column.
        SchemaDescriptor::new(&[SchemaColumn::new(type_code::U64, 0)], &[0])
    }

    fn two_col_schema() -> SchemaDescriptor {
        // PK U64 at index 0, payload U64 at index 1.
        SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::U64, 1),
            ],
            &[0],
        )
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
    fn check_batch_64_payload_cols_full_null_word() {
        // 65 columns: 1 U64 PK + 64 nullable payload cols → npc == 64, the
        // shift-by-width boundary. Must not panic (debug) and must mark every
        // payload column null (release silent-miss guard).
        let mut cols = vec![SchemaColumn::new(type_code::U64, 0)];
        for _ in 0..64 {
            cols.push(SchemaColumn::new(type_code::I64, 1));
        }
        let schema = SchemaDescriptor::new(&cols, &[0]);
        assert_eq!(schema.num_payload_cols(), 64);

        let keys = vec![42u128];
        let batch = build_check_batch(&schema, &keys, None);
        assert_eq!(batch.count, 1);

        let null_word = u64::from_le_bytes(batch.null_bmp_data()[0..8].try_into().unwrap());
        assert_eq!(null_word, u64::MAX, "all 64 payload columns must be marked null");
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
            pk_field_off: 0,
        };
        let mut filter = UniqueFilter::new();
        extract_into_filter(&mut filter, &batch.as_mem_batch(), &desc);
        assert!(filter.values.contains(&10u128));
        assert!(filter.values.contains(&20u128));
        assert!(!filter.values.contains(&30u128), "negative weight skipped");
    }

    #[test]
    fn extract_into_filter_signed_pk_col_uses_native_key() {
        // Single signed I64 PK. The OPK bytes at rest are sign-bit-flipped, so
        // `get_pk` (OPK-widened) differs from the native value. The extraction
        // must yield the *native* key (`pk_native_key`) — feeding the OPK value
        // would make `index_opk_prefix` flip the sign bit again and seek a wrong
        // key, hiding genuine duplicates.
        let schema = SchemaDescriptor::new(&[SchemaColumn::new(type_code::I64, 0)], &[0]);
        let mut opk = [0u8; 8];
        gnitz_wire::encode_pk_column(&(-5i64).to_le_bytes(), type_code::I64, &mut opk);
        let keys = [PkBuf::from_bytes(&opk)];
        let batch = build_check_batch_pkbuf(&schema, &keys, None);

        let native = (-5i64) as u64 as u128;
        let opk_widened = gnitz_wire::widen_pk_be(&opk, 8);
        assert_ne!(native, opk_widened, "signed OPK value must differ from native");

        let desc = UniqueIndexDesc {
            col_idx: 0,
            type_code: type_code::I64,
            is_pk_col: true,
            src_payload_idx: usize::MAX,
            col_size: 8,
            pk_field_off: 0,
        };
        let mut filter = UniqueFilter::new();
        extract_into_filter(&mut filter, &batch.as_mem_batch(), &desc);
        assert!(filter.values.contains(&native), "filter holds native signed key");
        assert!(!filter.values.contains(&opk_widened), "must not hold OPK-widened key");
    }

    #[test]
    fn index_route_key_hits_signed_routing_cache() {
        // The routing cache stores `extract_col_key` (OPK-widened) keys, but
        // seeks arrive with the native value. For a signed column the two differ,
        // so a raw native query misses; `index_route_key` must transform it to hit.
        let schema = SchemaDescriptor::new(&[SchemaColumn::new(type_code::I64, 0)], &[0]);
        let mut opk = [0u8; 8];
        gnitz_wire::encode_pk_column(&(-7i64).to_le_bytes(), type_code::I64, &mut opk);
        let keys = [PkBuf::from_bytes(&opk)];
        let batch = build_check_batch_pkbuf(&schema, &keys, None);

        let mut router = PartitionRouter::new();
        router.record_routing(&batch, &schema, 1, 0, 2);

        let native = (-7i64) as u64 as u128;
        // Raw native query misses: native != OPK-widened for a signed column.
        assert_eq!(router.worker_for_index_key(1, 0, native), -1);
        // Transformed query hits.
        let rk = index_route_key(&schema, 0, native).expect("integer column has a route key");
        assert_eq!(router.worker_for_index_key(1, 0, rk), 2);
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
            pk_field_off: 0,
        };
        let mut filter = UniqueFilter::new();
        extract_into_filter(&mut filter, &batch.as_mem_batch(), &desc);
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
            pk_field_off: 0,
        };
        let mut filter = UniqueFilter::new();
        filter.capped = true;
        extract_into_filter(&mut filter, &batch.as_mem_batch(), &desc);
        assert!(filter.values.is_empty(), "no-op on capped filter");
    }

    /// `drain_index_scan` must surface a worker-side scan fault as a deferred
    /// error returned only AFTER every other worker's frame train is fully
    /// drained — and the faulted worker's drain must terminate on its single
    /// error frame WITHOUT awaiting a continuation that will never arrive.
    ///
    /// Layout: worker 0 emits one `STATUS_ERROR` frame (flags 0, like
    /// `send_error`); worker 1 is healthy with a two-frame train (a non-terminal
    /// frame plus a preloaded `FLAG_SCAN_LAST` continuation). The drain is driven
    /// by a single manual poll with a noop waker:
    ///  - If `has_more` were keyed on `FLAG_SCAN_LAST` alone (not status-gated),
    ///    worker 0's flags-0 error frame reads as "more coming", the drain awaits
    ///    `w0_req` (never preloaded), and the first poll returns `Pending` —
    ///    caught here as a failed assert instead of an infinite hang.
    ///  - If the drain early-returned on the error (the old warmup bug), worker
    ///    1's preloaded continuation would never be consumed; the post-drain
    ///    `await_scan_slot(w1_req)` would then resolve immediately rather than
    ///    park — also caught here.
    #[test]
    fn drain_index_scan_defers_error_and_drains_survivor_train() {
        use std::future::Future;
        use std::task::{Context, Poll, Waker};
        use crate::runtime::reactor::Reactor;
        use crate::runtime::w2m::{W2mReceiver, W2mWriter};
        use crate::runtime::w2m_ring;
        use crate::runtime::wire::{self as ipc, STATUS_OK, STATUS_ERROR};

        const CAPACITY: usize = 64 * 1024;

        // Two W2M rings (one per worker). Anonymous shared mappings — no fork.
        let mk_ring = || unsafe {
            let ptr = libc::mmap(
                std::ptr::null_mut(), CAPACITY,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_ANONYMOUS | libc::MAP_SHARED, -1, 0,
            ) as *mut u8;
            assert!(!ptr.is_null(), "mmap failed");
            std::ptr::write_bytes(ptr, 0, CAPACITY);
            w2m_ring::init_region_for_tests(ptr, CAPACITY as u64);
            ptr
        };
        let ptr0 = mk_ring();
        let ptr1 = mk_ring();

        let reactor = Reactor::new(16).expect("reactor");
        let w0_req = reactor.alloc_scan_request_id() as u32;
        let w1_req = reactor.alloc_scan_request_id() as u32;
        let mut req_ids = [0u64; crate::runtime::sal::MAX_WORKERS];
        req_ids[0] = w0_req as u64;
        req_ids[1] = w1_req as u64;

        // Worker 0: single STATUS_ERROR frame (flags 0), tagged with w0_req.
        let writer0 = W2mWriter::new(ptr0, CAPACITY as u64);
        let err = b"boom";
        let esz = ipc::wire_size(STATUS_ERROR, err, None, None, None, None, &[]);
        writer0.send_encoded(esz, w0_req, |buf| {
            ipc::encode_wire_into_ipc(
                buf, 0, 0, 0, 0, 0u128, 0, 0, STATUS_ERROR, err, None, None, None, None, &[],
            );
        });

        // Worker 1: frame A (non-terminal, flags 0) then frame B (FLAG_SCAN_LAST),
        // both STATUS_OK and tagged with w1_req.
        let writer1 = W2mWriter::new(ptr1, CAPACITY as u64);
        let osz = ipc::wire_size(STATUS_OK, &[], None, None, None, None, &[]);
        writer1.send_encoded(osz, w1_req, |buf| {
            ipc::encode_wire_into_ipc(
                buf, 0, 0, 0, 0, 0u128, 0, 0, STATUS_OK, &[], None, None, None, None, &[],
            );
        });
        writer1.send_encoded(osz, w1_req, |buf| {
            ipc::encode_wire_into_ipc(
                buf, 0, 0, 0, FLAG_SCAN_LAST, 0u128, 0, 0, STATUS_OK, &[], None, None, None, None, &[],
            );
        });

        let receiver = W2mReceiver::new(vec![ptr0, ptr1]);
        // Mark both scans active: route_scan_slot now gates on active_scans, so
        // without a lease the parked continuation below would be discarded
        // (dispatch_scan_fanout normally holds this lease across the drain).
        let lease = reactor.scan_lease(&[w0_req, w1_req]);
        // Initial slots handed to the drain (what dispatch_scan_fanout returns).
        let slot0 = receiver.try_read_slot(0).expect("worker 0 frame");
        let slot1a = receiver.try_read_slot(1).expect("worker 1 frame A");
        // Worker 1's continuation: park it so await_scan_slot(w1_req) resolves.
        let slot1b = receiver.try_read_slot(1).expect("worker 1 frame B");
        reactor.test_route_scan_slot(slot1b);

        let slots = vec![slot0, slot1a];

        let noop: &Waker = Waker::noop();
        let mut cx = Context::from_waker(noop);

        let mut drain_fut = Box::pin(drain_index_scan(slots, &req_ids, &reactor, |_| {}));
        let result = match drain_fut.as_mut().poll(&mut cx) {
            Poll::Ready(r) => r,
            Poll::Pending => panic!(
                "drain_index_scan did not complete in one poll: status-gating \
                 regression would await a phantom continuation for the error frame"
            ),
        };
        drop(drain_fut);

        let err = result.expect_err("worker fault must surface as Err");
        assert!(err.contains("worker 0"), "deferred error names the faulted worker: {err}");
        assert!(err.contains("boom"), "deferred error carries the worker message: {err}");

        // Worker 1's continuation was consumed by the drain, so nothing remains
        // parked for w1_req: a fresh await must park (Pending), not resolve.
        let mut after = Box::pin(reactor.await_scan_slot(w1_req));
        match after.as_mut().poll(&mut cx) {
            Poll::Pending => {}
            Poll::Ready(_) => panic!(
                "worker 1's continuation frame was left parked — drain returned \
                 before draining a survivor's train"
            ),
        }
        drop(after);

        // Drop the lease before the rings: its Drop purges scan_parked, which
        // would drop any still-queued W2mSlot borrowing the soon-to-be-unmapped
        // region. (The queue is already drained here, but keep the ordering
        // honest.)
        drop(lease);
        drop(reactor);
        drop(receiver);
        unsafe {
            libc::munmap(ptr0 as *mut libc::c_void, CAPACITY);
            libc::munmap(ptr1 as *mut libc::c_void, CAPACITY);
        }
    }

    #[test]
    fn format_pk_value_uuid_full_128_bits() {
        // A UUID with non-zero high 64 bits. The lower 64 bits alone would be
        // misread as a different (truncated) value.
        let uuid: u128 = 0x550e8400_e29b_41d4_a716_446655440000u128;
        let schema = SchemaDescriptor::new(&[SchemaColumn::new(type_code::UUID, 0)], &[0]);

        let s = format_pk_value(uuid, &schema);
        // Must not be the lower-64 truncation (11975073520896 or similar).
        let truncated = format!("{}", uuid as u64);
        assert_ne!(s, truncated, "UUID must not be formatted as truncated u64");
        // Must contain the high-word hex digits.
        assert!(s.contains("550e8400"), "UUID formatting must include high bits");
    }

    #[test]
    fn format_pk_value_uuid_two_distinct_uuids_differ() {
        // Two UUIDs that differ only in the high 64 bits must produce different strings.
        let uuid_a: u128 = 0x11111111_0000_0000_0000_000000000001u128;
        let uuid_b: u128 = 0x22222222_0000_0000_0000_000000000001u128;
        let schema = SchemaDescriptor::new(&[SchemaColumn::new(type_code::UUID, 0)], &[0]);

        let sa = format_pk_value(uuid_a, &schema);
        let sb = format_pk_value(uuid_b, &schema);
        assert_ne!(sa, sb, "UUIDs differing in high bits must format differently");
    }

    fn compound_pk_bytes(parts: &[&[u8]]) -> PkBuf {
        let mut v = Vec::new();
        for p in parts { v.extend_from_slice(p); }
        PkBuf::from_bytes(&v)
    }

    #[test]
    fn format_pk_value_narrow_compound_agrees_with_bytes() {
        // (I32, U32) compound PK: A=-5, B=42. The renderer consumes OPK bytes,
        // so build the fixture as OPK (per-column encode, then widen for the
        // u128 arg). Reading raw native LE would mangle the signed column.
        let schema = SchemaDescriptor::new(&[
            SchemaColumn::new(type_code::I32, 0),
            SchemaColumn::new(type_code::U32, 0),
        ], &[0, 1]);
        assert!(!schema.pk_is_wide());
        let mut opk = [0u8; 8];
        gnitz_wire::encode_pk_column(&(-5i32).to_le_bytes(), type_code::I32, &mut opk[0..4]);
        gnitz_wire::encode_pk_column(&42u32.to_le_bytes(), type_code::U32, &mut opk[4..8]);
        let widened = gnitz_wire::widen_pk_be(&opk, 8);
        assert_eq!(format_pk_value(widened, &schema), "-5, 42");
        // The bytes renderer agrees with the (delegating) u128 renderer.
        assert_eq!(format_pk_value_bytes(&opk, &schema), "-5, 42");
    }

    #[test]
    fn format_pk_value_bytes_wide_compound_u64x3() {
        // Three U64 columns = 24-byte PK: too wide for a u128, exercising the
        // byte-form renderer where the old format_pk_value would truncate.
        // OPK for unsigned U64 is big-endian.
        let schema = SchemaDescriptor::new(&[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U64, 0),
        ], &[0, 1, 2]);
        assert!(schema.pk_is_wide());
        let pk = compound_pk_bytes(&[
            &7u64.to_be_bytes(), &8u64.to_be_bytes(), &9u64.to_be_bytes(),
        ]);
        assert_eq!(format_pk_value_bytes(pk.pk_bytes(), &schema), "7, 8, 9");
    }

    #[test]
    fn extract_into_filter_compound_pk_extracts_single_column() {
        // (A U32, B U32) both PK, unique index on A. Two rows share A=5 but
        // differ in B. Pre-fix get_pk(i) returned the packed (A,B) key, so the
        // filter held two distinct values; the fix slices out A only.
        let schema = SchemaDescriptor::new(&[
            SchemaColumn::new(type_code::U32, 0),
            SchemaColumn::new(type_code::U32, 0),
        ], &[0, 1]);
        assert_eq!(schema.pk_stride(), 8);
        // PK region is OPK; for unsigned U32 that is big-endian. extract_into_filter
        // decodes OPK→native via pk_native_key, so the fixture must be OPK.
        let keys = [
            compound_pk_bytes(&[&5u32.to_be_bytes(), &1u32.to_be_bytes()]),
            compound_pk_bytes(&[&5u32.to_be_bytes(), &2u32.to_be_bytes()]),
        ];
        let batch = build_check_batch_pkbuf(&schema, &keys, None);
        let desc = UniqueIndexDesc {
            col_idx: 0,
            type_code: type_code::U32,
            is_pk_col: true,
            src_payload_idx: usize::MAX,
            col_size: 4,
            pk_field_off: 0,
        };
        let mut filter = UniqueFilter::new();
        extract_into_filter(&mut filter, &batch.as_mem_batch(), &desc);
        assert!(filter.values.contains(&5u128), "filter holds column A's value");
        assert_eq!(filter.values.len(), 1, "the shared A=5 collapses to one entry");
    }

    #[test]
    fn extract_into_filter_compound_pk_second_column_offset() {
        // Unique index on B (the second PK column at byte offset 4). Confirms
        // pk_field_off slices the right column out of the packed key.
        let schema = SchemaDescriptor::new(&[
            SchemaColumn::new(type_code::U32, 0),
            SchemaColumn::new(type_code::U32, 0),
        ], &[0, 1]);
        // OPK PK region: unsigned U32 columns are stored big-endian.
        let keys = [
            compound_pk_bytes(&[&5u32.to_be_bytes(), &11u32.to_be_bytes()]),
            compound_pk_bytes(&[&6u32.to_be_bytes(), &22u32.to_be_bytes()]),
        ];
        let batch = build_check_batch_pkbuf(&schema, &keys, None);
        let desc = UniqueIndexDesc {
            col_idx: 1,
            type_code: type_code::U32,
            is_pk_col: true,
            src_payload_idx: usize::MAX,
            col_size: 4,
            pk_field_off: 4,
        };
        let mut filter = UniqueFilter::new();
        extract_into_filter(&mut filter, &batch.as_mem_batch(), &desc);
        assert!(filter.values.contains(&11u128));
        assert!(filter.values.contains(&22u128));
        assert_eq!(filter.values.len(), 2);
    }
}
