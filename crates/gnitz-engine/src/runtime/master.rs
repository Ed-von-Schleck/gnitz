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
use crate::schema::{pk_native_key, payload_native_key, IndexKeySpec, SchemaColumn};
use gnitz_wire::PkColList;

use crate::runtime::sal::{
    FLAG_SHUTDOWN, FLAG_DDL_SYNC, FLAG_EXCHANGE, FLAG_EXCHANGE_RELAY, FLAG_PUSH, FLAG_HAS_PK,
    FLAG_SEEK, FLAG_SEEK_BY_INDEX, FLAG_SEEK_BY_INDEX_RANGE_SAL, FLAG_BACKFILL, FLAG_GATHER,
    FLAG_UNIQUE_PREFLIGHT, FLAG_TICK, FLAG_FLUSH, SalWriter, pack_gather_cols,
    unique_preflight_wire_schema,
};
use crate::runtime::wire::{self, FLAG_HAS_DATA, FLAG_CONTINUATION, FLAG_SCAN_LAST, WireConflictMode, SchemaWithVersion, DecodedWire, col_names_as_refs, peek_control_block};
use gnitz_wire::wire_flags_set_conflict_mode;
use crate::runtime::w2m::{W2mReceiver, W2mSlot};
use crate::runtime::reactor::{AsyncMutex, PendingRelay, ScanLease};
use crate::storage::{Batch, ConsolidatedBatch, partition_for_pk_bytes, PkBuf};
use crate::ops::{
    PartitionRouter, RouteMode, op_relay_broadcast,
    op_repartition_batches_mode, op_relay_scatter_consolidated_mode,
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
/// `flags` is typically FLAG_HAS_PK; `col_hint` is `pack_pk_cols(&[col])` for an
/// index check (the packed flag at bit 63 is always set, so it never collides
/// with the PK sentinel) or 0 for a PK check.
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
    /// `col_indices` is the index's full column list, for the composite-aware
    /// error message.
    NonUpsert { col_indices: PkColList },
    /// UPSERT values for one unique index: hits must be verified via a per-holder
    /// seek to confirm the holder is the same row. `col_indices` drives the seek
    /// routing and the error; the index schema (PK stride and per-column
    /// promoted types/widths for the span decode) is read from the positionally
    /// paired `PipelinedCheck`, which the verify loop still holds.
    Upsert {
        col_indices: PkColList,
        /// (index-value span, is_upsert) per pending verify. `is_upsert` is true
        /// only for a genuine upsert target (net-positive committed PK); it gates
        /// the implicit exemption so a fresh-PK row routed here by
        /// `retracted_vals` cannot ride a holder's implicit retraction.
        upsert_keys: Vec<(PkBuf, bool)>,
        /// (source PK, value span) pairs retracted in this batch. At the verify a
        /// different holder is exempt only when it is retracting the value here;
        /// combined with the holder being an upserted PK (`existing_pks`), this
        /// also admits a bulk shift.
        retracted_pairs: FxHashSet<(PkBuf, PkBuf)>,
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

/// Maximum number of values tracked per `(table_id, packed_cols)` filter.
/// Filters that would exceed this disable themselves. Kept at 1M even though a
/// `PkBuf` key (81 bytes: `[u8; 80]` + len) is ~5× a `u128`'s 16: at the cap,
/// hashbrown's inline storage (7/8 max load → 2²¹ power-of-two buckets × 82 B)
/// allocates ≈170 MB per maxed filter, vs ≈36 MB for the old `u128` keys —
/// bounded, and only reached by a table holding 1M distinct unique values.
/// Lowering the cap would shrink the broadcast-skip reach (every unique index
/// with > cap distinct values reverts to always-broadcast on the insert hot
/// path); the cap stays the lever for a memory-constrained deployment. The
/// PkBuf width is identical for the single-column ≤16-byte and the composite
/// cases — one key type, one code path.
const UNIQUE_FILTER_CAP: usize = 1_000_000;

struct UniqueFilter {
    /// The OPK leading-key spans known present in the index. A `PkBuf` holds the
    /// full composite span at any width, so a `UNIQUE (a, b)` whose span exceeds
    /// 16 bytes is tracked without truncation (a truncating `u128` could prove a
    /// present key absent and wrongly skip the broadcast).
    values: FxHashSet<PkBuf>,
    /// Maximum distinct values tracked: `UNIQUE_FILTER_CAP` in production,
    /// parameterizable so tests exercise the cap discipline cheaply.
    cap: usize,
    /// True once the filter has exceeded `cap`. In that
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
        Self::with_cap(UNIQUE_FILTER_CAP)
    }

    fn with_cap(cap: usize) -> Self {
        UniqueFilter { values: FxHashSet::default(), cap, capped: false, warm: false }
    }

    /// On overflow the set is cleared WHOLE, never truncated: a partial set
    /// would prove "absent" for a present key — a uniqueness hole.
    fn insert(&mut self, key: PkBuf) {
        if self.capped { return; }
        self.values.insert(key);
        if self.values.len() > self.cap {
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
    /// `pack_pk_cols(col_indices)` per cold filter — the `unique_filters` map
    /// key, so the drop handler removes exactly the entries this warmup created.
    keys: Vec<u64>,
    disarmed: bool,
}

impl Drop for WarmupGuard {
    fn drop(&mut self) {
        if !self.disarmed {
            unsafe {
                let disp = &mut *self.disp_ptr;
                for &packed in &self.keys {
                    disp.unique_filter_remove(self.table_id, packed);
                }
            }
        }
    }
}

// Safety: WarmupGuard is only used inside the single-threaded master event loop.
unsafe impl Send for WarmupGuard {}

/// Column-extraction descriptor for one unique index on a table, `Copy` (built
/// fresh per batch on the hot ingest path — no heap allocation). `packed`
/// (= `pack_pk_cols(col_indices)`) keys the `unique_filters` map and is the
/// exact `IDXTAB_PAY_SOURCE_COLS` value, so seed/drop/warmup all derive the
/// same key; `spec` is the per-circuit span encode plan.
#[derive(Clone, Copy)]
struct UniqueIndexDesc {
    packed: u64,
    spec: IndexKeySpec,
}

/// Walk every positive-weight, non-null row of `batch` and insert the indexed
/// columns' OPK leading-key span into `filter`. Respects the filter's capped
/// state by stopping the walk once the filter caps. A row with a NULL in any
/// indexed column is skipped (`key_bytes` → false), sharing the NULL-distinct
/// key contract with the CREATE-time validator and the projection.
fn extract_into_filter(
    filter: &mut UniqueFilter,
    batch: &crate::storage::MemBatch<'_>,
    spec: &IndexKeySpec,
) {
    let mut keybuf = PkBuf::empty(0);
    for row in 0..batch.count {
        if batch.get_weight(row) <= 0 { continue; }
        if !spec.key_bytes(batch, row, &mut keybuf) { continue; }
        filter.insert(keybuf);
        if filter.capped { return; } // stop walking once the filter caps
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
    /// Per-(table_id, packed_col_list) filter skipping redundant Phase 2
    /// unique-index broadcasts. The `u64` is `pack_pk_cols(col_indices)` — the
    /// same value stored in `IDXTAB_PAY_SOURCE_COLS` — so a composite index is
    /// identified by its whole column list, and dropping `(a, b)` never touches a
    /// distinct single-column filter on `a`. See the UniqueFilter comment block.
    unique_filters: FxHashMap<(i64, u64), UniqueFilter>,

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
            .unwrap_or_else(|| panic!("master: no schema for target_id={target_id}"));
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
            .unwrap_or_else(|| panic!("master: no schema for target_id={target_id}"));
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

    /// Liveness gate for the pre-reactor bootstrap wait loops. A crashed worker
    /// (panic / OOM-kill / SIGKILL) leaves its `reader_seq` frozen, so `wait_for`
    /// only ever times out and the wait loop would spin forever. Callers probe
    /// before parking and surface the dead worker as an error instead of hanging
    /// the master; `context` names the bootstrap phase ("before completing
    /// recovery sync", "during backfill relay"). On these paths workers stay
    /// alive after acking, so a reaped worker has not published the awaited
    /// frame — no ack is lost.
    fn fail_if_worker_dead(&mut self, context: &str) -> Result<(), String> {
        let dead = self.check_workers();
        if dead >= 0 {
            return Err(format!("worker {dead} exited {context}"));
        }
        Ok(())
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
                            return Err(format!("worker {w}: {msg}"));
                        }
                        results[w] = Some(decoded);
                        break;
                    }
                    None => {
                        self.fail_if_worker_dead("before completing recovery sync")?;
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
                        // Backfill keeps fail-on-low-space (prepare_relay no
                        // longer checks): injecting a FLAG_FLUSH mid-backfill
                        // would orphan already-written backfill groups that
                        // workers have not yet consumed and hang boot.
                        if !self.sal_relay_space_ok_raw() {
                            let remaining = self.sal.mmap_size() - self.sal.cursor();
                            return Err(format!(
                                "SAL space exhausted during backfill exchange relay \
                                 ({remaining} bytes left)"));
                        }
                        let prep = self.prepare_relay(relay)?;
                        self.emit_relay(prep)?;
                    }
                } else {
                    if decoded.control.status != 0 {
                        let msg = String::from_utf8_lossy(&decoded.control.error_msg);
                        return Err(format!("worker {w}: {msg}"));
                    }
                    collected[w] = true;
                    remaining -= 1;
                }
            }
            if !progressed {
                self.fail_if_worker_dead("during backfill relay")?;
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
        self.checkpoint_post_ack()
    }

    // -----------------------------------------------------------------------
    // Exchange relay
    // -----------------------------------------------------------------------

    #[cfg(debug_assertions)]
    fn seam_armed_epoch() -> &'static std::sync::atomic::AtomicU32 {
        static ARMED: std::sync::atomic::AtomicU32 =
            std::sync::atomic::AtomicU32::new(u32::MAX);
        &ARMED
    }

    /// Raw SAL relay-space threshold: at least 1/8 of the mmap still free.
    /// Seam-free — the boot backfill relay checks this directly because it
    /// must keep failing-on-low-space without observing the relay_loop test
    /// seam (which would spuriously fail an in-progress backfill).
    fn sal_relay_space_ok_raw(&self) -> bool {
        self.sal.mmap_size() - self.sal.cursor() >= (self.sal.mmap_size() >> 3)
    }

    /// True when enough SAL space remains for a relay write (>= 1/8 of the
    /// mmap). Checked *before* consuming a relay so a low-space condition
    /// can be resolved (checkpoint) rather than silently discarding the
    /// relay and deadlocking blocked workers. While the debug seam is armed,
    /// reports low until the next checkpoint bumps the SAL epoch.
    pub(crate) fn sal_has_relay_space(&self) -> bool {
        #[cfg(debug_assertions)]
        if Self::seam_armed_epoch().load(std::sync::atomic::Ordering::Relaxed)
            == self.sal.epoch()
        {
            return false;
        }
        self.sal_relay_space_ok_raw()
    }

    /// relay_loop's variant: with GNITZ_INJECT_RELAY_SPACE_LOW set, the first
    /// call arms the seam at the current epoch (one-shot: the CAS from the
    /// u32::MAX sentinel succeeds once per process), then defers to
    /// sal_has_relay_space() so relay_loop and the committer see the same
    /// verdict until a checkpoint bumps the epoch and disarms it.
    pub(crate) fn sal_has_relay_space_arming(&self) -> bool {
        #[cfg(debug_assertions)]
        if std::env::var("GNITZ_INJECT_RELAY_SPACE_LOW").is_ok() {
            let _ = Self::seam_armed_epoch().compare_exchange(
                u32::MAX, self.sal.epoch(),
                std::sync::atomic::Ordering::Relaxed,
                std::sync::atomic::Ordering::Relaxed);
        }
        self.sal_has_relay_space()
    }

    /// CPU-only first half of exchange relay: looks up shard columns via
    /// the catalog DAG, scatters the payloads into per-worker batches, and
    /// collects column names. No SAL write yet — `relay_loop` runs this
    /// without `sal_writer_excl` so the lock covers only the synchronous
    /// SAL write in `emit_relay`.
    pub(crate) fn prepare_relay(&mut self, relay: PendingRelay) -> Result<RelayPrepared, String> {
        let PendingRelay { view_id, payloads, schema, source_id } = relay;

        let cat = unsafe { &mut *self.catalog };
        // A join-shard scatter (cols from a reindex chain) must route by the
        // reindex key so a row lands on the worker that owns its `_join_pk`
        // partition; a GROUP BY / set-op exchange scatter routes by the group
        // key (consistent with op_reduce's output PK). See `RouteMode`.
        // A join-shard scatter carries (reindex col, carried promotion target tc)
        // pairs; a GROUP BY / set-op scatter carries plain shard cols (no
        // promotion). Split the pairs into a column list + a parallel target-tc
        // list for the scatter packer.
        let (shard_cols, target_tcs, is_join): (Vec<i32>, Vec<u8>, bool) = if source_id > 0 {
            let pairs = cat.dag.get_join_shard_cols(view_id, source_id);
            if pairs.is_empty() {
                (cat.dag.get_shard_cols(view_id), Vec::new(), false)
            } else {
                let cols = pairs.iter().map(|&(c, _)| c).collect();
                let tcs = pairs.iter().map(|&(_, t)| t).collect();
                (cols, tcs, true)
            }
        } else {
            (cat.dag.get_shard_cols(view_id), Vec::new(), false)
        };

        // A range-join INPUT relay (source_id > 0, is_join over a DeltaTraceRange
        // view): `view_range_join_n_eq` reads the equality-conjunct count straight
        // off the join node. The trace-side reindex key is [eq cols…, range col]
        // (len n_eq + 1). A band join (n_eq ≥ 1) scatters by the eq PREFIX — route
        // by the first n_eq slots, dropping the trailing range slot, so equal
        // eq-values co-partition both sides and the range probe is partition-local.
        // A pure range join (n_eq == 0) has no eq prefix: its matches are spread
        // over the whole key space, so it BROADCASTS the full delta and each worker
        // trims to its owned slice (PartitionFilter) before integrating. The output
        // relay (source_id == 0) is NOT a join relay (is_join is false there) and
        // keeps the GroupKey scatter.
        let range_n_eq = if is_join { cat.dag.view_range_join_n_eq(view_id) } else { None };

        let dest_batches = if range_n_eq == Some(0) {
            // Pure range join: broadcast the full delta to every worker.
            let sources: Vec<Option<&Batch>> = payloads.iter().map(|o| o.as_ref()).collect();
            op_relay_broadcast(&sources, &schema, self.num_workers)
        } else {
            // Scatter. Band join (range_n_eq == Some(n_eq ≥ 1)): route by the eq
            // prefix shard_cols[..n_eq]. Equi-join: full shard cols. Both
            // JoinPromote. GROUP BY / set-op: full shard cols, GroupKey.
            let route_len = range_n_eq.map_or(shard_cols.len(), |n_eq| n_eq as usize);
            debug_assert!(range_n_eq.is_none_or(|n_eq| shard_cols.len() == n_eq as usize + 1),
                "range-join reindex key = [eq…, range]: len must be n_eq + 1");
            let col_indices: Vec<u32> = shard_cols[..route_len].iter().map(|&c| c as u32).collect();
            // target_tcs is EMPTY for a GroupKey scatter (no promotion) and has
            // length shard_cols.len() for any join; slice it to the routing prefix
            // when promoting, empty otherwise.
            let route_tcs: &[u8] = if is_join { &target_tcs[..route_len] } else { &[] };
            let mode = if is_join { RouteMode::JoinPromote } else { RouteMode::GroupKey };
            let consolidated_sources: Option<Vec<Option<&ConsolidatedBatch>>> = payloads.iter()
                .map(|opt| match opt {
                    None => Some(None),
                    Some(b) => ConsolidatedBatch::from_batch_ref(b).map(Some),
                })
                .collect();
            match consolidated_sources {
                Some(sources) => {
                    op_relay_scatter_consolidated_mode(&sources, &col_indices, route_tcs, &schema, self.num_workers, mode)
                }
                None => {
                    let sources: Vec<Option<&Batch>> = payloads.iter().map(|o| o.as_ref()).collect();
                    op_repartition_batches_mode(&sources, &col_indices, route_tcs, &schema, self.num_workers, mode)
                }
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
            // The routing cache is keyed (table, col, u128); only SINGLE-COLUMN
            // unique circuits populate it. A composite unique seek
            // broadcasts-and-merges instead — widening the cache's unbounded
            // per-distinct-value map to composite `PkBuf` keys would grow every
            // entry ~5× for the dominant single-column population — so its
            // routing is never recorded.
            let col_idx = match cat.unique_index_circuit_cols(target_id, ci) {
                Some(c) if c.len() == 1 => c[0],
                _ => continue,
            };
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
                .ok_or_else(|| format!("seek: table {target_id} not found"))?
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
                .map_err(|e| format!("seek_by_index: worker {w}: {e}"))?;
            if ctrl.status != 0 {
                return Err(format!(
                    "worker {}: seek_by_index: {}",
                    w, String::from_utf8_lossy(&ctrl.error_msg)));
            }
            // This path inspects-and-forwards exactly one slot per worker; a
            // chunked train here would be forwarded truncated, its remainder
            // silently discarded by the lease drop. A unique single-column
            // seek returns at most one row, so a train means the invariant
            // broke (e.g. a shrunken GNITZ_REPLY_FRAME_BUDGET) — fail loudly.
            if ctrl.flags & FLAG_CONTINUATION != 0 {
                return Err(format!(
                    "worker {w}: seek_by_index: unexpected chunked reply on a \
                     single-frame path"));
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
        target_id: i64, seek_col_idx: u64, seek_pk: u128, seek_pk_extra: &[u8],
    ) -> Result<Option<Batch>, String> {
        Self::fan_out_index_collect_common(
            disp_ptr, reactor, sal_excl, target_id,
            FLAG_SEEK_BY_INDEX, seek_pk, seek_col_idx, seek_pk_extra, "seek_by_index",
        ).await
    }

    /// SELECT-path ordered range scan over a secondary index: broadcast the range
    /// descriptor to ALL workers and MERGE every matching base row into one batch.
    ///
    /// Differs from `fan_out_seek_by_index_collect_async` only in the
    /// master→worker leg: the `u32` SAL dispatch flag
    /// `FLAG_SEEK_BY_INDEX_RANGE_SAL` (so the worker classifies it as
    /// `SeekByIndexRange`, not a point seek), and the descriptor riding
    /// `seek_pk_extra` (arbitrary length — this leg is not `PkTuple`-bound; the
    /// worker is the sole OPK encoder, so the descriptor is forwarded verbatim).
    /// A range's matches scatter by source PK, so broadcast-and-merge is the
    /// correct, in-tree mechanism — no range-aware exchange is needed.
    pub async fn fan_out_seek_by_index_range_collect_async(
        disp_ptr: *mut MasterDispatcher,
        reactor: &crate::runtime::reactor::Reactor,
        sal_excl: &Rc<AsyncMutex<()>>,
        target_id: i64, seek_col_idx: u64, seek_pk_extra: &[u8],
    ) -> Result<Option<Batch>, String> {
        Self::fan_out_index_collect_common(
            disp_ptr, reactor, sal_excl, target_id,
            FLAG_SEEK_BY_INDEX_RANGE_SAL, 0, seek_col_idx, seek_pk_extra, "seek_by_index_range",
        ).await
    }

    /// Shared skeleton of the two broadcast-and-merge index seeks above:
    /// fan one frame out to ALL workers under `sal_flag` and merge every
    /// worker's matching base rows into one batch via the train drain
    /// (an oversized worker reply arrives as a chunked train; a single-frame
    /// reply is a length-1 train).
    ///
    /// The master forwards the client's wire payload verbatim — it never
    /// decodes seek_pk_extra into u128s and re-encodes them (the worker is
    /// the sole OPK encoder). seek_col_idx carries pack_pk_cols(col_indices),
    /// already validated by the caller.
    /// `_lease` held across the full drain: its workers stream, so releasing
    /// the gate before every train is consumed (or the drain errors and the
    /// lease drop discards the rest) risks a discarded late frame.
    #[allow(clippy::too_many_arguments)]
    async fn fan_out_index_collect_common(
        disp_ptr: *mut MasterDispatcher,
        reactor: &crate::runtime::reactor::Reactor,
        sal_excl: &Rc<AsyncMutex<()>>,
        target_id: i64, sal_flag: u32, seek_pk: u128,
        seek_col_idx: u64, seek_pk_extra: &[u8], op: &str,
    ) -> Result<Option<Batch>, String> {
        // `expected` is captured inside the fan-out closure so the reply
        // guard is definitionally the schema the request was built from; a
        // separate pre-fanout catalog read could diverge across the
        // `sal_excl` await if a DDL interleaves, failing healthy replies.
        let mut expected: Option<SchemaDescriptor> = None;
        let (slots, req_ids, _lease) = dispatch_scan_fanout(disp_ptr, reactor, sal_excl, |disp, req_ids| {
            let (schema, col_names) = disp.get_schema_and_names(target_id);
            let lsn = disp.next_lsn();
            expected = Some(schema);
            disp.write_group_with_req_ids(
                target_id, lsn, sal_flag, 0, &[], &schema, &col_names,
                seek_pk, seek_col_idx, req_ids, -1, 0, None, seek_pk_extra,
            )
        }).await?;
        let expected = expected.expect("fan-out closure ran");

        let mut acc: Option<Batch> = None;
        let mut merged_bytes = 0usize;
        drain_index_scan(slots, &req_ids, reactor, op, &expected, |mb, frame_len| {
            // Σ frame bytes ≥ the merged single-frame encode size (every frame
            // re-counts its header and the first one the schema block), so this
            // cap can never let a reply through that the client would reject
            // (MAX_FRAME_PAYLOAD_CLIENT) — and it bounds the master's merge heap.
            merged_bytes += frame_len;
            if merged_bytes > gnitz_wire::MAX_FRAME_PAYLOAD_CLIENT {
                return Err(format!(
                    "{op}: result exceeds the {} MiB reply cap; add a tighter \
                     predicate or LIMIT",
                    gnitz_wire::MAX_FRAME_PAYLOAD_CLIENT >> 20));
            }
            let a = acc.get_or_insert_with(|| Batch::with_schema(expected, mb.count));
            a.append_mem_batch_range(mb, 0, mb.count, None);
            Ok(())
        }).await?;
        // The sink runs only for non-empty frames, so `Some` implies rows.
        Ok(acc)
    }

    /// Resolve the committed holder of a unique index value: seek the index by
    /// the value's native per-column keys and return the holder's source PK (or
    /// `None`). Used by the UPSERT verify, which must confirm a colliding
    /// committed value is held by the same row (or a row releasing it in this
    /// batch); the caller decodes the OPK span to `natives` via
    /// `span_to_natives`.
    ///
    /// Arity gates the routing: a single-column unique seek keeps the unicast
    /// routing-cache fast path (the common same-PK upsert whose value is
    /// unchanged lands here, so it is hot); a composite unique seek
    /// broadcasts-and-merges (the routing cache stays single-column — see
    /// `record_index_routing`), with the trailing native values riding
    /// `seek_pk_extra` as 16-byte LE slots, the exact wire form the worker's
    /// SeekByIndex handler reassembles. A unique index yields at most one
    /// holder, so merging one row is correct.
    async fn seek_unique_holder(
        disp_ptr: *mut MasterDispatcher,
        reactor: &crate::runtime::reactor::Reactor,
        sal_excl: &Rc<AsyncMutex<()>>,
        target_id: i64,
        col_indices: PkColList,
        natives: [u128; gnitz_wire::PK_LIST_MAX_COLS],
    ) -> Result<Option<PkBuf>, String> {
        let cols = col_indices.as_slice();
        if cols.len() == 1 {
            // single-column unique: unicast to the one owning worker (routing cache).
            let slot = Self::fan_out_seek_by_index_async(
                disp_ptr, reactor, sal_excl, target_id, cols[0], natives[0]).await?;
            let ctrl = peek_control_block(slot.bytes()).map_err(|e| e.to_string())?;
            if ctrl.flags & FLAG_HAS_DATA == 0 { return Ok(None); }
            let zc = wire::decode_wire_ipc_zero_copy_with_ctrl(
                slot.bytes(), ctrl.block_size, ctrl, None).map_err(|e| e.to_string())?;
            Ok(zc.data_batch.filter(|b| b.count > 0)
                .map(|b| PkBuf::from_bytes(b.get_pk_bytes(0))))
        } else {
            // composite unique: broadcast-and-merge. natives[0] → seek_pk;
            // natives[1..] → 16-byte LE slots in seek_pk_extra.
            let mut extra = [0u8; (gnitz_wire::PK_LIST_MAX_COLS - 1) * 16];
            for (slot, &v) in extra.chunks_exact_mut(16).zip(&natives[1..cols.len()]) {
                slot.copy_from_slice(&v.to_le_bytes());
            }
            let batch = Self::fan_out_seek_by_index_collect_async(
                disp_ptr, reactor, sal_excl, target_id,
                gnitz_wire::pack_pk_cols(cols), natives[0],
                &extra[..(cols.len() - 1) * 16]).await?;
            Ok(batch.map(|b| PkBuf::from_bytes(b.get_pk_bytes(0))))
        }
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

        // Return on the FIRST worker fault, decode error, or client
        // disconnect: `_lease` drops on return and `route_scan_slot` discards
        // every undrained frame at the ring boundary, advancing
        // `consume_cursor`, so a still-streaming worker cannot wedge in
        // `send_encoded` — draining the doomed trains would be pure waste. On
        // a fault the client sees its data frames followed by a STATUS_ERROR
        // frame, which `recv_scan_response` handles mid-stream.
        for (w, mut slot) in slots.into_iter().enumerate() {
            loop {
                let (_, has_more) = parse_train_header(&slot, w, "scan")?;
                // Forward slot to client; send_slot drops it on return.
                let rc = reactor.send_slot(fd, slot).await;
                if rc < 0 { return Ok(false); }
                if !has_more { break; }
                slot = reactor.await_scan_slot(req_ids[w] as u32).await;
            }
        }
        Ok(true)
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
    /// committed row is absent are omitted entirely. Each row's values are
    /// aligned to `project`.
    ///
    /// This is the `O(num_workers)`-round-trip replacement for the per-row
    /// serial single-key seek loop used by FK RESTRICT on non-PK UNIQUE
    /// targets. It is a sibling of `execute_pipeline_async` (which returns only
    /// existence) rather than a modification of it: the has-pk pipeline echoes
    /// the caller's payload (`filter_by_pk`), so it structurally cannot return
    /// a stored column the caller does not already hold.
    ///
    /// Replies arrive as reply trains (an oversized gather reply chunks; a
    /// single-frame reply is a length-1 train), so the fan-out uses scan
    /// request ids and the train drain. The expected projected schema guards
    /// each train's first frame — a worker whose catalog lags a DDL would
    /// otherwise hand back rows the master mis-decodes.
    async fn execute_gather_async(
        disp_ptr: *mut MasterDispatcher,
        reactor: &crate::runtime::reactor::Reactor,
        sal_excl: &Rc<AsyncMutex<()>>,
        target_id: i64,
        mut pks: Vec<PkBuf>,
        project: &[u8],
    ) -> Result<GatherMap, String> {
        if pks.is_empty() {
            return Ok(GatherMap::default());
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
                .ok_or_else(|| format!("gather: no schema for table {target_id}"))?
        };
        // The exact constructor the worker uses for its reply schema, so a
        // matching reply validates by construction.
        let expected = crate::catalog::project_schema(&parent_schema, project);

        // `_lease` held across the full drain below (see `dispatch_scan_fanout`).
        let (slots, req_ids, _lease) = dispatch_scan_fanout(disp_ptr, reactor, sal_excl, |disp, rids| {
            let nw = disp.num_workers;
            let pk_cols = parent_schema.pk_indices();
            let pooled = disp.pool_pop_batch(target_id);
            let batch = build_check_batch_pkbuf(&parent_schema, &pks, pooled);
            let lsn = disp.next_lsn();
            with_worker_indices(&batch, pk_cols, &parent_schema, nw, |worker_indices| {
                disp.sal.scatter_wire_group(
                    &batch, worker_indices, &parent_schema, None,
                    target_id as u32, lsn, FLAG_GATHER,
                    /* wire_flags */ 0, /* seek_pk */ 0, /* seek_col_idx */ col_mask,
                    rids, /* unicast_worker */ -1, None, None,
                )
            })?;
            // The scatter batch is fully consumed by the synchronous
            // scatter_wire_group above; return it to the pool.
            recycle_check_batch(disp, target_id, batch);
            Ok(())
        }).await?;

        // Precompute (type_code, col_size) per projected column from the parent
        // schema; the reply's projected payload index k corresponds to project[k].
        let proj_meta: Vec<(u8, usize)> = project.iter().map(|&p| {
            let col = parent_schema.columns[p as usize];
            (col.type_code, col.size() as usize)
        }).collect();

        let mut out = GatherMap::new(proj_meta.len());
        drain_index_scan(slots, &req_ids, reactor, "gather", &expected, |b, _| {
            // The column slices are invariant across a frame's rows; derive
            // each once per frame instead of once per (row × column). The
            // arity is hard-capped by `pack_gather_cols` (one u8 per u64 byte).
            let mut col_slices: [&[u8]; 8] = [&[]; 8];
            for (k, &(_, col_size)) in proj_meta.iter().enumerate() {
                col_slices[k] = b.col_data(k, col_size);
            }
            for j in 0..b.count {
                let null_word = b.get_null_word(j);
                out.push_row(
                    PkBuf::from_bytes(b.get_pk_bytes(j)),
                    proj_meta.iter().enumerate().map(|(k, &(col_type, col_size))| {
                        if null_word & (1u64 << k) != 0 {
                            None
                        } else {
                            Some(payload_native_key(
                                col_slices[k], j * col_size, col_size, col_type))
                        }
                    }),
                );
            }
            Ok(())
        }).await?;
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

    pub fn check_workers(&mut self) -> i32 {
        // Probe each worker by its own pid, not `waitpid(-1)`. A per-pid
        // `waitpid` returns ECHILD — a detected death — even if the zombie was
        // reaped elsewhere, whereas `waitpid(-1)` returns 0 ("some child is
        // alive") and silently misses one worker's death while others run, so it
        // would go blind the moment a SIGCHLD/signalfd reaper or SA_NOCLDWAIT is
        // ever added. It also names the exact dead worker for the error/log.
        for w in 0..self.num_workers {
            let pid = self.worker_pids[w];
            if pid <= 0 { continue; }
            let mut status: i32 = 0;
            loop {
                let rpid = unsafe { libc::waitpid(pid, &mut status, libc::WNOHANG) };
                if rpid > 0 {
                    self.worker_pids[w] = 0;        // reaped — never waitpid it again
                    return w as i32;
                }
                if rpid == 0 {
                    break;                          // still running
                }
                // rpid == -1
                let err = crate::runtime::sys::errno();
                if err == libc::EINTR {
                    continue;                       // signal, not death — retry
                }
                if err == libc::ECHILD {
                    self.worker_pids[w] = 0;        // already gone
                    return w as i32;
                }
                break;                              // unexpected errno: treat as alive
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
    /// `table_id` — the filter-map key plus the span encode plan per unique
    /// circuit, the shape `extract_into_filter` consumes.
    fn unique_index_descriptors(
        &mut self, table_id: i64,
    ) -> Option<(SchemaDescriptor, Vec<UniqueIndexDesc>)> {
        let cat = unsafe { &mut *self.catalog };
        let schema = cat.get_schema_desc(table_id)?;
        let n_circuits = cat.get_index_circuit_count(table_id);

        let mut out: Vec<UniqueIndexDesc> = Vec::new();
        for ci in 0..n_circuits {
            let Some(cols) = cat.unique_index_circuit_cols(table_id, ci) else { continue };
            let idx_schema = match cat.get_index_circuit_schema(table_id, ci) {
                Some(s) => s,
                None => continue,
            };
            out.push(UniqueIndexDesc {
                packed: gnitz_wire::pack_pk_cols(cols),
                spec: IndexKeySpec::new(cols, &schema, &idx_schema),
            });
        }
        if out.is_empty() { None } else { Some((schema, out)) }
    }

    /// True if every key in `keys` is definitely absent from the filter
    /// for `(table_id, packed)`. Returns false if the filter is capped,
    /// not warm (caller is expected to warm it first), or contains any
    /// key. On false, caller must fall through to the Phase 2 broadcast.
    fn unique_filter_all_absent(
        &self, table_id: i64, packed: u64, keys: &[PkBuf],
    ) -> bool {
        let filter = match self.unique_filters.get(&(table_id, packed)) {
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
            let filter = match self.unique_filters.get_mut(&(table_id, d.packed)) {
                Some(f) => f,
                None => continue, // not warm — warmup will pick this up
            };
            if filter.capped { continue; }
            extract_into_filter(filter, &mb, &d.spec);
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

    /// Remove the unique-filter entry for a single (owner_table_id, packed)
    /// pair. `packed` is the `pack_pk_cols(col_indices)` / `IDXTAB_PAY_SOURCE_COLS`
    /// value. Called on DROP INDEX so subsequent INSERTs re-trigger warmup for
    /// the now-absent index while leaving unrelated filters on the same table; a
    /// non-existent key (e.g. a non-unique FK index) is a harmless no-op.
    pub(crate) fn unique_filter_remove(&mut self, owner_id: i64, packed: u64) {
        self.unique_filters.remove(&(owner_id, packed));
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
                    "validate_all_distributed: no schema for table {target_id}"))?;
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

        // Borrowed view for the per-column locator reads (FK insert / unique
        // enforcement); zero-allocation over `batch`'s pages.
        let mb = batch.as_mem_batch();

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
                        // A +w row is w insertions of the PK; Error mode must
                        // reject it like the w separate +1 rows it encodes.
                        if w > 0 { entry.1 += if w > 1 { 2 } else { 1 }; }
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
                    // A +w row is w insertions of the PK; Error mode must
                    // reject it like the w separate +1 rows it encodes.
                    if w > 0 { entry.1 += if w > 1 { 2 } else { 1 }; }
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
            let (fk_col_idx, parent_table_id, parent_col_idx, parent_schema) = unsafe {
                let disp = &mut *disp_ptr;
                let cat = &mut *disp.catalog;
                let (fk_col_idx, parent_table_id, parent_col_idx) = match cat.get_fk_constraint(target_id, fi) {
                    Some(c) => c,
                    None => continue,
                };
                let parent_schema = cat.get_schema_desc(parent_table_id)
                    .ok_or_else(|| format!(
                        "FK parent table {parent_table_id} schema not found"))?;
                (fk_col_idx, parent_table_id, parent_col_idx, parent_schema)
            };

            // The FK column may itself be a PK column of the child table (e.g.
            // `id BIGINT PRIMARY KEY REFERENCES parent(pid)`); `locate` resolves
            // the PK-or-payload read once, so a PK FK column reads from the
            // packed PK region instead of a (nonexistent) payload slot.
            let loc = source_schema.locate(fk_col_idx);

            let mut seen: FxHashSet<u128> = FxHashSet::default();
            let mut keys: Vec<u128> = Vec::new();

            for i in 0..batch.count {
                if batch.get_weight(i) <= 0 { continue; }
                if loc.is_null(&mb, i) { continue; }
                let fk_key = loc.native_key(&mb, i);
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
                // Base table: the PK column does not promote, so its own type is
                // both the source and the leading-key type (identity encode).
                let src_type = parent_schema.columns[parent_col_idx].type_code;
                let pooled = unsafe { (*disp_ptr).pool_pop_batch(parent_table_id) };
                let check_batch = build_check_batch(&parent_schema, &keys, src_type, pooled);
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
                    cat.get_index_schema_by_cols(parent_table_id, &[parent_col_idx as u32])
                        .ok_or_else(|| format!(
                            "FK check: no unique index on parent table {parent_table_id} col {parent_col_idx}"))?
                };
                let pooled = unsafe { (*disp_ptr).pool_pop_batch(parent_table_id) };
                // `loc` is the child FK column; its type drives the sign-extension
                // into the parent index's promoted leading key.
                let check_batch = build_check_batch(&idx_schema, &keys, loc.type_code(), pooled);
                p1_labels.push(P1Label::FkParent { parent_table_id, expected_count });
                p1_checks.push(PipelinedCheck {
                    target_id: parent_table_id,
                    flags: FLAG_HAS_PK,
                    col_hint: gnitz_wire::pack_pk_cols(&[parent_col_idx as u32]),
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
                    GatherMap::default()
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
                        let idx_schema = cat.get_index_schema_by_cols(child_tid, &[fk_col_idx as u32])
                            .ok_or_else(|| format!(
                                "FK RESTRICT check failed: no index on child table {child_tid} col {fk_col_idx}"))?;
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

                    // The keys are the referenced parent column's values; its type
                    // drives the sign-extension into the child index's promoted
                    // leading key (equal logical values sign-extend to the same
                    // promoted image from either side's width).
                    let src_type = source_schema.columns[parent_col_idx].type_code;
                    let pooled = unsafe { (*disp_ptr).pool_pop_batch(child_tid) };
                    let check_batch = build_check_batch(&idx_schema, &keys, src_type, pooled);
                    p1_labels.push(P1Label::FkRestrict { child_tid });
                    p1_checks.push(PipelinedCheck {
                        target_id: child_tid,
                        flags: FLAG_HAS_PK,
                        col_hint: gnitz_wire::pack_pk_cols(&[fk_col_idx as u32]),
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
                                "Foreign Key violation in '{sn}.{tn}': value not found in target '{tsn}.{ttn}'"
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
                                "Foreign Key violation: cannot delete from '{sn}.{tn}', row still referenced by '{csn}.{ctn}'",
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
                                "duplicate key value violates unique constraint \"{sn}_{tn}_pkey\": Key ({pk_names})=({key_str}) already exists",
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
        // (child_tid, fk_col, child_idx_schema, src_type, retired_parent_values).
        // `src_type` is the referenced parent column's type, threaded into the
        // child index's OPK seek so a signed column sign-extends correctly.
        let mut p2_restrict: Vec<(i64, u32, SchemaDescriptor, u8, Vec<u128>)> = Vec::new();
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
                        let idx_schema = match cat.get_index_schema_by_cols(child_tid, &[fk_col_idx as u32]) {
                            Some(s) => s,
                            None => continue,
                        };
                        (child_tid, fk_col_idx, parent_col_idx, idx_schema)
                    };

                    // PK columns are skipped above (immutable under UPDATE), so
                    // this referenced column is always a payload column.
                    let loc = source_schema.locate(parent_col_idx);
                    let proj_pos = project.iter()
                        .position(|&c| c == parent_col_idx as u8)
                        .expect("non-PK referenced column missing from gather projection");

                    let mut retired: Vec<u128> = Vec::new();
                    for &i in &update_rows {
                        let new_val = if loc.is_null(&mb, i) {
                            None
                        } else {
                            Some(loc.native_key(&mb, i))
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
                        // `loc` is the referenced parent (payload) column; its
                        // type drives the sign-extension at the seek encode.
                        p2_restrict.push((
                            child_tid, fk_col_idx as u32, idx_schema, loc.type_code(), retired));
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

        // Index-independent; computed once. `retracted_vals` allocation is reused
        // across indices (`retracted_pairs` is moved into the label below, so it
        // is rebuilt each index).
        let has_retractions = (0..batch.count).any(|i| batch.get_weight(i) < 0);
        let mut retracted_vals: FxHashSet<PkBuf> = FxHashSet::default();
        // Reused scratch the per-row OPK leading-key span is written into; each
        // `key_bytes` call overwrites it. No per-row allocation.
        let mut keybuf = PkBuf::empty(0);

        for ci in 0..n_circuits {
            let (col_indices, idx_schema) = unsafe {
                let disp = &mut *disp_ptr;
                let cat = &mut *disp.catalog;
                let cols = match cat.unique_index_circuit_cols(target_id, ci) {
                    Some(c) => c,
                    None => continue,
                };
                let idx_schema = match cat.get_index_circuit_schema(target_id, ci) {
                    Some(s) => s,
                    None => continue,
                };
                // Own the list (PkColList is Copy) so it survives the catalog
                // borrow and feeds the P2Label / error formatters.
                (PkColList::from_slice(cols), idx_schema)
            };
            let cols = col_indices.as_slice();

            // Per-circuit read/encode plan: `key_bytes` builds the OPK
            // leading-key span — null-correct (skip on any NULL) and
            // equality-correct at any width.
            let spec = IndexKeySpec::new(cols, &source_schema, &idx_schema);
            // The (table, this) filter-map key AND the worker's index-store hint —
            // a single-column hint cannot locate a composite index.
            let packed = gnitz_wire::pack_pk_cols(cols);

            // `upsert_keys` carries the row's own `is_upsert` flag (the row PK is
            // no longer read at the verify). `check_keys`/`seen` take every
            // positive row on a pure INSERT — size them to `batch.count` to skip
            // growth reallocs on the hot path; `upsert_keys` stays `Vec::new()`
            // (empty on an insert-only batch).
            let mut upsert_keys: Vec<(PkBuf, bool)> = Vec::new();
            let mut check_keys: Vec<PkBuf> = Vec::with_capacity(batch.count);
            let mut seen: FxHashSet<PkBuf> =
                FxHashSet::with_capacity_and_hasher(batch.count, Default::default());

            // `retracted_vals` routes a fresh-PK insertion of a retracted value
            // into the verify path (where the holder is discovered);
            // `retracted_pairs` carries the precise `(holder PK, value span)` check
            // into it. `retracted_pairs` is moved into the label below (so it is
            // rebuilt per index), and is sized only when the batch actually retracts.
            retracted_vals.clear();
            let mut retracted_pairs: FxHashSet<(PkBuf, PkBuf)> = if has_retractions {
                FxHashSet::with_capacity_and_hasher(batch.count, Default::default())
            } else {
                FxHashSet::default()
            };
            if has_retractions {
                for i in 0..batch.count {
                    if batch.get_weight(i) >= 0 { continue; }
                    if !spec.key_bytes(&mb, i, &mut keybuf) { continue; }
                    retracted_vals.insert(keybuf);
                    retracted_pairs.insert((PkBuf::from_bytes(batch.get_pk_bytes(i)), keybuf));
                }
            }

            for i in 0..batch.count {
                let w = batch.get_weight(i);
                if w <= 0 { continue; }

                // A row NULL in any indexed column is not indexed (NULL-distinct).
                if !spec.key_bytes(&mb, i, &mut keybuf) { continue; }

                // One row at weight w is the value w times. On a non-unique_pk
                // table that is w live instances (enforce_unique_pk collapses
                // it to one on unique_pk tables) — the same violation as w
                // separate +1 rows, which `seen` below rejects.
                if !unique_pk && w > 1 {
                    return Err(unsafe {
                        (*disp_ptr).unique_violation_err(target_id, cols, true)
                    });
                }

                // In-batch duplicate detection runs for ALL positive-weight
                // rows, INCLUDING UPSERTs: two rows setting the same new unique
                // value in one transaction is a violation regardless of whether
                // their PKs already exist.
                if !seen.insert(keybuf) {
                    return Err(unsafe {
                        (*disp_ptr).unique_violation_err(target_id, cols, true)
                    });
                }

                // UPSERT (committed PK on a unique_pk table — enforce_unique_pk
                // retracts the old row at apply) OR a fresh insertion whose value
                // is explicitly retracted in this batch (transfer onto a fresh PK):
                // both need per-holder verification. On a non-unique_pk table an
                // existing PK is NOT an upsert (no enforce_unique_pk), so it must
                // take the broadcast path where any committed hit is a violation.
                let is_upsert = unique_pk && existing_pks.contains(batch.get_pk_bytes(i));
                if is_upsert || retracted_vals.contains(&keybuf) {
                    // Carry the row's own `is_upsert`: a fresh-PK row routed here by
                    // a value in `retracted_vals` must get only the
                    // explicit-retraction exemption at the verify, never the
                    // implicit one.
                    upsert_keys.push((keybuf, is_upsert));
                    continue;
                }
                check_keys.push(keybuf);
            }

            // Unique check batches zero-pad each OPK span to the index PK stride
            // (leading span = the value, suffix = zero — byte-identical to the old
            // single-column `build_check_batch`): `padded` is sound because
            // `key_bytes` keeps the tail past the span zero.
            let stride = idx_schema.pk_stride() as usize;

            if !check_keys.is_empty() {
                let skip_broadcast = unsafe {
                    (*disp_ptr).unique_filter_all_absent(target_id, packed, &check_keys)
                };

                if !skip_broadcast {
                    let pooled = unsafe { (*disp_ptr).pool_pop_batch(target_id) };
                    let chk_batch = build_check_batch_with(
                        &idx_schema, &check_keys, pooled,
                        |b, k| b.extend_pk_bytes(k.padded(stride)));
                    p2_labels.push(P2Label::NonUpsert { col_indices });
                    p2_checks.push(PipelinedCheck {
                        target_id,
                        flags: FLAG_HAS_PK,
                        col_hint: packed,
                        payload: Some(CheckPayload::Broadcast(chk_batch)),
                        schema: idx_schema,
                    });
                }
            }

            if !upsert_keys.is_empty() {
                let pooled = unsafe { (*disp_ptr).pool_pop_batch(target_id) };
                let u_batch = build_check_batch_with(
                    &idx_schema, &upsert_keys, pooled,
                    |b, (k, _)| b.extend_pk_bytes(k.padded(stride)));
                p2_labels.push(P2Label::Upsert {
                    col_indices, upsert_keys, retracted_pairs,
                });
                p2_checks.push(PipelinedCheck {
                    target_id,
                    flags: FLAG_HAS_PK,
                    col_hint: packed,
                    payload: Some(CheckPayload::Broadcast(u_batch)),
                    schema: idx_schema,
                });
            }
        }

        // RESTRICT probes for referenced UNIQUE values retired by UPDATE.
        for (child_tid, fk_col, idx_schema, src_type, keys) in p2_restrict {
            let pooled = unsafe { (*disp_ptr).pool_pop_batch(child_tid) };
            let chk_batch = build_check_batch(&idx_schema, &keys, src_type, pooled);
            p2_labels.push(P2Label::FkRestrict { child_tid });
            p2_checks.push(PipelinedCheck {
                target_id: child_tid,
                flags: FLAG_HAS_PK,
                col_hint: gnitz_wire::pack_pk_cols(&[fk_col]),
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
                P2Label::NonUpsert { col_indices } => {
                    if !p2_results[idx].is_empty() {
                        return Err(unsafe {
                            (*disp_ptr).unique_violation_err(target_id, col_indices.as_slice(), false)
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
                            "Foreign Key violation: cannot update '{sn}.{tn}', row still referenced by '{csn}.{ctn}'",
                        ));
                    }
                }
                P2Label::Upsert { col_indices, upsert_keys, retracted_pairs } => {
                    // Fan out all seeks before collecting: sal_excl is released
                    // before each reply wait, so the seek futures run
                    // concurrently rather than serializing one RTT per key.
                    let occupied = &p2_results[idx];
                    // The positionally paired check still holds the index schema
                    // (reclaim_check_batches takes only the payload).
                    let idx_schema = &p2_checks[idx].schema;
                    let stride = idx_schema.pk_stride() as usize;
                    let idx_cols = &idx_schema.columns[..col_indices.as_slice().len()];
                    // Each pending entry carries the index-value span and the row's
                    // own `is_upsert` flag; the row PK no longer participates (the
                    // same-PK case is subsumed by `existing_pks.contains(found_pk)`).
                    let mut pending: Vec<(PkBuf, bool)> = Vec::new();
                    let mut futs = Vec::new();
                    for &(key_pk, is_upsert) in upsert_keys {
                        // occupied holds index PKs at full stride, zero-suffixed
                        // (the probe wrote the OPK span padded to the stride).
                        // `key_pk` is already that OPK span — pad to the stride and
                        // test directly; do NOT re-OPK-encode an encoded span.
                        if !occupied.contains(key_pk.padded(stride)) { continue; }
                        pending.push((key_pk, is_upsert));
                        // async fn futures are !Unpin; box-pin for join_all_unpin.
                        // One homogeneous future type (all inputs Copy, no `dyn`):
                        // `seek_unique_holder` gates arity internally.
                        futs.push(Box::pin(Self::seek_unique_holder(
                            disp_ptr, reactor, sal_excl, target_id,
                            *col_indices, span_to_natives(&key_pk, idx_cols),
                        )));
                    }
                    let holders = crate::runtime::reactor::join_all_unpin(futs).await;
                    for ((key_pk, is_upsert), holder_result) in pending.into_iter().zip(holders) {
                        // A unique index yields at most one holder; the merged
                        // `Option<PkBuf>` is the committed holder's source PK.
                        let Some(found_pk) = holder_result? else { continue };
                        // The committed holder is acceptable only if it releases
                        // this value in this batch. Implicit release (holder is
                        // itself an upserted PK, so enforce_unique_pk retracts its
                        // committed row — covers a same-PK re-upsert and a bulk
                        // shift) applies ONLY when the colliding row is also an
                        // upsert: `is_upsert` implies `unique_pk &&
                        // existing_pks.contains(row_pk)`. A fresh-PK row routed here
                        // by `retracted_vals` has is_upsert=false, so it cannot ride
                        // the holder's implicit retraction and land a second live
                        // holder. Explicit release (holder retracts this exact
                        // (PK, value span) pair) needs no such gate and admits a
                        // transfer onto a fresh PK. `seen` already barred two live
                        // rows sharing the value.
                        let exempt = (is_upsert && existing_pks.contains(found_pk.pk_bytes()))
                            || retracted_pairs.contains(&(found_pk, key_pk));
                        if !exempt {
                            return Err(unsafe {
                                (*disp_ptr).unique_violation_err(target_id, col_indices.as_slice(), false)
                            });
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
        let (schema, missing, mut guard): (SchemaDescriptor, Vec<UniqueIndexDesc>, WarmupGuard) = unsafe {
            let disp = &mut *disp_ptr;
            let (schema, descs) = match disp.unique_index_descriptors(table_id) {
                Some(x) => x,
                None => return Ok(()),
            };
            let missing: Vec<UniqueIndexDesc> = descs.into_iter()
                .filter(|d| !disp.unique_filters.contains_key(&(table_id, d.packed)))
                .collect();
            if missing.is_empty() { return Ok(()); }
            for d in &missing {
                disp.unique_filters.insert((table_id, d.packed), UniqueFilter::new());
            }
            let missing_keys: Vec<u64> = missing.iter().map(|d| d.packed).collect();
            let guard = WarmupGuard {
                disp_ptr,
                table_id,
                keys: missing_keys,
                disarmed: false,
            };
            (schema, missing, guard)
        };

        // `_lease` held across the full continuation drain below; its workers
        // stream multi-frame trains, and on an early error return (or a
        // mid-scan cancellation) the lease drop discards every undrained
        // frame at the ring boundary.
        let (slots, req_ids, _lease) = dispatch_scan_fanout(disp_ptr, reactor, sal_excl, |disp, req_ids| {
            let (schema, col_names) = disp.get_schema_and_names(table_id);
            let lsn = disp.next_lsn();
            disp.write_group_with_req_ids(
                table_id, lsn, 0, 0, &[], &schema, &col_names,
                0, 0, req_ids, -1, 0, None, &[],
            )
        }).await?;

        // Drain every worker's continuation-frame train into the cold filters.
        // `drain_index_scan` owns the early-return error contract (the lease
        // drop above discards any undrained frames at the ring boundary), the
        // schema guard against DDL-lagged worker replies, the zero-copy
        // `MemBatch` lifetime, and the continuation-schema-hint handling.
        let scan_result = drain_index_scan(slots, &req_ids, reactor, "scan", &schema, |mb, _| {
            let disp = unsafe { &mut *disp_ptr };
            for d in &missing {
                if let Some(filter) = disp.unique_filters.get_mut(&(table_id, d.packed)) {
                    if !filter.capped {
                        extract_into_filter(filter, mb, &d.spec);
                    }
                }
            }
            Ok(())
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
                    if let Some(f) = disp.unique_filters.get_mut(&(table_id, d.packed)) {
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

    /// Pre-flight global uniqueness check for CREATE UNIQUE INDEX, distributed:
    /// each worker projects its committed partition to the indexed columns' OPK
    /// leading-key spans, sorts them locally (byte-lexicographic), and streams
    /// the SORTED spans back; the master runs a streaming k-way merge
    /// (`merge_index_scan`) whose single adjacent-equal check catches both
    /// within-partition and cross-partition duplicates that no per-worker
    /// `backfill_index` can see. Master memory is `O(num_workers)` plus the
    /// (≤ cap) filter seed — never the table's distinct-key cardinality. The OPK
    /// leading-key span is lossless and injective for every type a unique index
    /// permits (`index_key_type` rejects floats/STRING/BLOB), so byte equality ⟺
    /// index-value equality and byte-lexicographic order is a valid merge order
    /// at any width — replacing the old numeric `u128` order, which a composite
    /// key has no meaningful value for.
    ///
    /// On success the index is safe to commit and broadcast and the returned
    /// distinct span set seeds the master's unique filter; on failure the
    /// caller returns a client error and never broadcasts, so no worker
    /// reaches the fatal `DdlSync` backfill path.
    ///
    /// MUST run inside the DDL critical section (committer barrier drained,
    /// catalog write lock held) and BEFORE the IDX_TAB +1 is appended/broadcast,
    /// so the scanned snapshot is exactly the data each worker will later
    /// backfill and no concurrent INSERT can be ordered between the snapshot and
    /// the backfill.
    ///
    /// An unknown table yields an empty set (nothing to validate).
    pub async fn validate_unique_index_create_async(
        disp_ptr: *mut MasterDispatcher,
        reactor: &crate::runtime::reactor::Reactor,
        sal_excl: &Rc<AsyncMutex<()>>,
        owner_id: i64,
        col_indices: &[u32],
    ) -> Result<(FxHashSet<PkBuf>, bool), String> {
        let (idx_schema, packed) = unsafe {
            let cat = &mut *(*disp_ptr).catalog;
            let owner_schema = match cat.get_schema_desc(owner_id) {
                Some(s) => s,
                None => return Ok((FxHashSet::default(), false)),
            };
            // Trivial-uniqueness short-circuit, generalised to the compound PK:
            // a composite index whose columns equal the table's enforced-unique
            // PK (in any order) can never collide, so the scan is skipped.
            // `group_cols_eq_pk` is order-insensitive set equality and returns
            // false for an out-of-range index, so no separate bounds check is
            // needed; it subsumes the old sole-PK-column case exactly. The
            // empty uncapped seed is NOT the committed value set (the scan is
            // skipped), but warm-empty stays sound: value-equality is
            // PK-equality here, and rows whose PK is committed never reach
            // `check_keys` (Error mode fails the Phase-1 PK check; upsert mode
            // routes them to the always-broadcast upsert path), so every
            // `check_keys` key carries a fresh PK — hence a fresh value — and
            // "absent" is always the correct verdict.
            if cat.table_has_unique_pk(owner_id) && owner_schema.group_cols_eq_pk(col_indices) {
                return Ok((FxHashSet::default(), false));
            }
            // Build the index schema (the circuit is not registered until this
            // pre-flight succeeds) for the merge's reply-frame layout and the
            // promoted per-column widths. Identical inputs to each worker's own
            // build, so the frame schema agrees by construction. `packed` is
            // the column list the worker resolves the seek by.
            (crate::catalog::make_index_schema(col_indices, &owner_schema)?,
             gnitz_wire::pack_pk_cols(col_indices))
        };
        let frame_schema = unique_preflight_wire_schema(&idx_schema, col_indices.len());

        // Fan out the pre-flight command (the packed column list rides in
        // seek_col_idx); each worker answers with its sorted-span
        // continuation-frame train. `_lease` held to end of scope: when the
        // merge returns early (error or duplicate verdict) the lease drop
        // discards the undrained trains at the ring boundary.
        let (slots, req_ids, _lease) = dispatch_scan_fanout(disp_ptr, reactor, sal_excl, |disp, req_ids| {
            let (schema, col_names) = disp.get_schema_and_names(owner_id);
            let lsn = disp.next_lsn();
            disp.write_group_with_req_ids(
                owner_id, lsn, FLAG_UNIQUE_PREFLIGHT, 0, &[], &schema, &col_names,
                0, packed, req_ids, -1, 0, None, &[],
            )
        })
        .await?;

        let merged = merge_index_scan(slots, &req_ids, reactor, &frame_schema).await?;
        if merged.duplicate {
            return Err(unsafe {
                (*disp_ptr).unique_create_dup_err(owner_id, col_indices)
            });
        }
        Ok(merged.into_seed())
    }

    /// Seed the `(table_id, col_idx)` filter from the CREATE-time pre-flight,
    /// captured under the catalog write lock. Marks it warm so the first
    /// INSERT skips `ensure_unique_filters_warm_async`. `capped = true` (the
    /// accumulator overflowed and cleared its set whole — `seen` arrives
    /// empty) publishes a warm+capped filter: `unique_filter_all_absent` then
    /// always falls through to the broadcast — the same steady state the lazy
    /// warmup converges to, without paying a redundant full-cluster scan on
    /// the first INSERT. Symmetric counterpart of `unique_filter_remove`.
    pub(crate) fn unique_filter_seed(
        &mut self, table_id: i64, packed: u64, seen: FxHashSet<PkBuf>, capped: bool,
    ) {
        let mut filter = UniqueFilter::new();
        filter.warm = true;     // pre-flight scanned every worker under the write lock
        if capped {
            filter.capped = true;
        } else {
            filter.values = seen;   // exact distinct set; same type, move not re-hash
        }
        self.unique_filters.insert((table_id, packed), filter);
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
    ///
    /// Returns the flush error WITHOUT resetting the SAL when the system-table
    /// flush fails: the SAL entries about to be discarded are that data's only
    /// durable copy, so resetting on a swallowed failure destroys it — the same
    /// hazard the boot path guards via `flush_all_system_tables`. Callers leave
    /// the SAL intact and retry on a later checkpoint (committer) or abort boot
    /// (`do_checkpoint`).
    pub(crate) fn checkpoint_post_ack(&mut self) -> Result<(), String> {
        let cat = unsafe { &mut *self.catalog };
        cat.flush_all_system_tables()?;
        // Now safe: every worker ACKed the FLUSH, so all have consumed past any
        // DROP that gated a directory — hence finished the matching CREATE.
        cat.drain_checkpoint_gated_deletions();
        self.sal.checkpoint_reset();
        gnitz_info!("SAL checkpoint epoch={}", self.sal.epoch());
        Ok(())
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
    /// `col_indices` is the index's full column list (composite-aware).
    fn unique_violation_err(&mut self, target_id: i64, col_indices: &[u32], in_batch: bool) -> String {
        unsafe { (*self.catalog).unique_violation_err(target_id, col_indices, in_batch) }
    }

    /// Delegate `CREATE UNIQUE INDEX` duplicate-value rejection to the catalog.
    fn unique_create_dup_err(&mut self, owner_id: i64, col_indices: &[u32]) -> String {
        unsafe { (*self.catalog).unique_create_dup_err(owner_id, col_indices) }
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
            "duplicate key value violates unique constraint \"{sn}_{tn}_pkey\": Batch contains multiple rows with key ({pk_names})=({key_str})",
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
            return Some(format!("worker {w}: {op}: {msg}"));
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

/// Parse one frame header of worker `w`'s continuation train. Returns the
/// control block plus whether more frames follow, or `Err` (prefixed with
/// `what`) on a fault frame or a corrupt header. Every caller holds the
/// `ScanLease` from `dispatch_scan_fanout`, so propagating the `Err` is the
/// whole disposal story: the lease drop discards the undrained remainder at
/// the ring boundary.
///
/// A corrupt header yields no frame structure to follow, and a fault frame is
/// terminal by contract — `send_error` reports it as a single frame with
/// `status = STATUS_ERROR` and flags `0` (no `FLAG_SCAN_LAST`), and the
/// worker emits nothing more for the request. The error therefore MUST stop
/// the drain: keying `has_more` off `FLAG_SCAN_LAST` alone would read the
/// fault frame's `0` flags as "more coming" and block forever in
/// `await_scan_slot` on a frame that never arrives.
///
/// A healthy frame WITHOUT `FLAG_CONTINUATION` is a length-1 train: the
/// single-frame `send_response` reply shape carries no train flags at all, so
/// "no continuation flag" must read as terminal. Every multi-frame train
/// producer (worker scan chunks, chunked seek/gather replies, unique
/// pre-flight frames) sets `FLAG_CONTINUATION` on every frame and
/// `FLAG_SCAN_LAST` on the terminal one. This is the single definition of
/// that train contract, shared by every train consumer (`fan_out_scan_async`,
/// `drain_index_scan`, the pre-flight merge).
fn parse_train_header(
    slot: &W2mSlot,
    w: usize,
    what: &str,
) -> Result<(wire::DecodedControl, bool), String> {
    let ctrl = peek_control_block(slot.bytes())
        .map_err(|e| scan_decode_err(w, e))?;
    if ctrl.status != 0 {
        return Err(format!(
            "worker {}: {}: {}",
            w, what, String::from_utf8_lossy(&ctrl.error_msg)));
    }
    let has_more = ctrl.flags & FLAG_SCAN_LAST == 0
        && ctrl.flags & FLAG_CONTINUATION != 0;
    Ok((ctrl, has_more))
}

/// Fan-out reply-train drain shared by the unique-filter warmup and the
/// collect paths (index seek/range merge, gather). Invokes `on_batch` with the
/// zero-copy `MemBatch` and the raw frame byte length of every non-empty
/// frame, in worker order.
///
/// Returns on the FIRST error — worker fault, corrupt/undecodable frame,
/// schema mismatch, or an `Err` from `on_batch` — without draining the
/// remaining trains. All callers hold the `ScanLease` from
/// `dispatch_scan_fanout`; when the early `Err` unwinds it, the lease drop
/// (`reactor/mod.rs`) frees every parked slot and `route_scan_slot` discards
/// all later frames at the ring boundary, advancing `consume_cursor` — a
/// still-streaming worker cannot wedge in `send_encoded`, so decoding the
/// doomed remainder would be pure waste.
///
/// `expected` is validated against each train's first schema-bearing frame.
/// Worker reply schemas can lag the master's during DDL
/// races (`run_tick` releases the catalog read lock before awaiting ACKs; a
/// worker inside `do_exchange_wait` defers `DdlSync` but serves seeks inline;
/// seek handlers take no catalog lock at all), and the batch append helpers do
/// not validate shape — an unguarded mismatch is memory-unsafe garbage handed
/// onward under the master's schema block.
///
/// Continuation frames carry no schema; the schema + version saved from the
/// first frame is reused as a decode hint. The zero-copy `MemBatch` borrows
/// from `slot.bytes()`, so the slot is dropped only after `on_batch` returns.
async fn drain_index_scan(
    slots: Vec<W2mSlot>,
    req_ids: &[u64; crate::runtime::sal::MAX_WORKERS],
    reactor: &crate::runtime::reactor::Reactor,
    what: &str,
    expected: &SchemaDescriptor,
    mut on_batch: impl FnMut(&crate::storage::MemBatch<'_>, usize) -> Result<(), String>,
) -> Result<(), String> {
    for (w, mut slot) in slots.into_iter().enumerate() {
        let mut saved_schema: Option<(SchemaDescriptor, u16)> = None;
        loop {
            let (ctrl, has_more) = parse_train_header(&slot, w, what)?;
            let server_version = gnitz_wire::wire_flags_get_schema_version(ctrl.flags);
            let frame_len = slot.bytes().len();
            let schema_hint = saved_schema.as_ref()
                .map(|(s, v)| SchemaWithVersion { descriptor: s, version: *v });
            let zc = wire::decode_wire_ipc_zero_copy_with_ctrl(
                slot.bytes(), ctrl.block_size, ctrl, schema_hint,
            ).map_err(|e| scan_decode_err(w, e))?;
            if saved_schema.is_none() {
                if let Some(ref s) = zc.schema {
                    crate::schema::validate_schema_match(s, expected)
                        .map_err(|e| format!("worker {w}: {what}: {e}"))?;
                    saved_schema = Some((*s, server_version));
                }
            }
            if let Some(ref mb) = zc.data_batch {
                if mb.count > 0 { on_batch(mb, frame_len)?; }
            }
            drop(zc); // borrows slot
            drop(slot);
            if !has_more { break; }
            slot = reactor.await_scan_slot(req_ids[w] as u32).await;
        }
    }
    Ok(())
}

/// `pk → projected committed values` result of `execute_gather_async`. Rows
/// live in one flat arena, `stride` values each, instead of one heap `Vec`
/// per row — a large UPDATE/DELETE validation gathers tens of thousands of
/// rows, and per-row allocations would dominate the merge.
#[derive(Default)]
struct GatherMap {
    stride: usize,
    index: FxHashMap<PkBuf, u32>,
    vals: Vec<Option<u128>>,
}

impl GatherMap {
    fn new(stride: usize) -> Self {
        GatherMap { stride, ..Default::default() }
    }

    fn push_row(&mut self, pk: PkBuf, row: impl Iterator<Item = Option<u128>>) {
        let idx = (self.vals.len() / self.stride) as u32;
        self.vals.extend(row);
        debug_assert!(self.vals.len() == (idx as usize + 1) * self.stride,
            "gather row arity must equal the projection stride");
        self.index.insert(pk, idx);
    }

    /// The projected values for `pk`'s committed row, aligned to the gather's
    /// `project` list; `None` when the committed row is absent. Zero-copy
    /// lookup via `Borrow<[u8]>`.
    fn get(&self, pk: &[u8]) -> Option<&[Option<u128>]> {
        self.index.get(pk).map(|&i| {
            let start = i as usize * self.stride;
            &self.vals[start..start + self.stride]
        })
    }
}

/// Per-worker state for one sorted-key stream in `merge_index_scan`.
///
/// `mb` is a zero-copy view into `slot`'s ring bytes with its lifetime
/// erased: it is valid only while `slot` is held, so `attach_frame` clears
/// `mb` before dropping or replacing `slot`, and nothing reads `mb` after the
/// stream's slot is released.
struct PreflightKeyStream {
    /// Worker index (error attribution) and scan request id (frame pulls).
    w: usize,
    req_id: u64,
    /// Ring slot backing `mb`. Holding it parks the frame's ring bytes.
    slot: Option<W2mSlot>,
    /// Zero-copy view of the current frame's key batch (`None` for an empty
    /// terminal frame or after a decode error).
    mb: Option<crate::storage::MemBatch<'static>>,
    /// Cursor into `mb`.
    row: usize,
    /// Current frame is non-terminal: status 0 and no FLAG_SCAN_LAST.
    has_more: bool,
}

impl PreflightKeyStream {
    fn new(w: usize, req_id: u64) -> Self {
        PreflightKeyStream {
            w,
            req_id,
            slot: None,
            mb: None,
            row: 0,
            has_more: false,
        }
    }

    /// Install `slot` as the current frame, decoding its keys zero-copy.
    /// A fault/corrupt/undecodable frame is an immediate `Err` — the caller
    /// unwinds to the `ScanLease` drop, which discards the undrained trains.
    fn attach_frame(
        &mut self,
        slot: W2mSlot,
        frame_schema: &SchemaDescriptor,
    ) -> Result<(), String> {
        // The view must die before its backing slot.
        self.mb = None;
        self.slot = None;
        self.row = 0;
        let (ctrl, has_more) = parse_train_header(&slot, self.w, "unique pre-flight")?;
        self.has_more = has_more;
        // Every frame decodes against the shared compile-time wire schema
        // (version 0): the first frame's embedded schema block equals it by
        // construction (`send_unique_preflight_keys`), and continuation
        // frames carry no schema and resolve through the hint.
        let ctrl_size = ctrl.block_size;
        let schema_hint = Some(SchemaWithVersion { descriptor: frame_schema, version: 0 });
        // SAFETY: the slice points into the W2M ring slot that `slot` pins
        // until dropped. `self.mb` borrows it, and every path that drops or
        // replaces `self.slot` clears `self.mb` first (top of this fn and
        // `next_key`'s terminal arm), so the view never outlives the pin. The
        // lifetime erasure exists only because slot and view live in the same
        // struct.
        let bytes: &'static [u8] = unsafe { std::mem::transmute::<&[u8], &'static [u8]>(slot.bytes()) };
        let zc = wire::decode_wire_ipc_zero_copy_with_ctrl(bytes, ctrl_size, ctrl, schema_hint)
            .map_err(|e| scan_decode_err(self.w, e))?;
        self.mb = zc.data_batch;
        self.slot = Some(slot);
        Ok(())
    }

    /// Yield this worker's next span, pulling continuation frames on demand.
    /// Returns `Ok(None)` once the train is terminal. The whole PK region IS
    /// the OPK leading-key span (`pk_stride == idx_key_size`), read verbatim
    /// as a `PkBuf` — no column-0 decode, so the wire is byte-transparent for
    /// a span of any width.
    async fn next_key(
        &mut self,
        frame_schema: &SchemaDescriptor,
        reactor: &crate::runtime::reactor::Reactor,
    ) -> Result<Option<PkBuf>, String> {
        loop {
            if let Some(mb) = &self.mb {
                if self.row < mb.count {
                    let key = PkBuf::from_bytes(mb.get_pk_bytes(self.row));
                    self.row += 1;
                    return Ok(Some(key));
                }
            }
            if !self.has_more {
                self.mb = None;
                self.slot = None;
                return Ok(None);
            }
            let slot = reactor.await_scan_slot(self.req_id as u32).await;
            self.attach_frame(slot, frame_schema)?;
        }
    }
}

/// Per-key accounting for the pre-flight merge: duplicate verdict + inline
/// seed collection, fed keys in globally-sorted merge order. Split from the
/// frame-pulling loop so the verdict and the all-or-nothing seed rule are
/// directly testable with a small cap.
pub(crate) struct PreflightAccumulator {
    prev: Option<PkBuf>,
    pub(crate) duplicate: bool,
    /// Seed collection reuses `UniqueFilter`'s cap discipline: on overflow
    /// `insert` clears the set WHOLE and disables itself, so the seed is
    /// complete-or-empty, never truncated — a truncated seed would publish a
    /// warm but incomplete filter whose "proven absent" answers would let a
    /// genuine duplicate skip the INSERT broadcast. Every span reaching
    /// `insert` is distinct (spans arrive sorted, so duplicates are adjacent
    /// and stop at the `prev` check).
    filter: UniqueFilter,
}

impl PreflightAccumulator {
    pub(crate) fn new(cap: usize) -> Self {
        PreflightAccumulator {
            prev: None,
            duplicate: false,
            filter: UniqueFilter::with_cap(cap),
        }
    }

    /// Offer the next span in globally-sorted merge order. Returns `false`
    /// once a duplicate is found — the verdict is monotonic, so the caller
    /// stops merging useful spans (but still drains every worker's train).
    /// Span equality (byte-equal ⟺ value-equal) replaces the old `u128` equality.
    pub(crate) fn offer(&mut self, key: PkBuf) -> bool {
        if self.duplicate { return false; }
        if self.prev == Some(key) {
            self.duplicate = true;
            return false;
        }
        self.prev = Some(key);
        self.filter.insert(key);
        true
    }

    /// The complete distinct span set plus the capped verdict. `capped = true`
    /// means the set overflowed and was cleared whole — the caller must
    /// publish a capped (always-broadcast) filter, never a warm-empty one.
    pub(crate) fn into_seed(self) -> (FxHashSet<PkBuf>, bool) {
        (self.filter.values, self.filter.capped)
    }
}

/// Streaming k-way merge over the per-worker SORTED key streams of a unique
/// pre-flight fan-out. Master memory is `O(num_workers)`: the heap, one
/// cursor and one live zero-copy frame view per worker; frame bytes the merge
/// has not reached yet stay in the fixed per-worker W2M shared-memory rings.
///
/// One adjacent-equal check (`prev == popped`) catches BOTH duplicate
/// classes: two equal keys from one worker are adjacent in its sorted run and
/// pop consecutively (within-partition), and the same value held by two
/// workers surfaces as two equal heads (cross-partition). Takes no catalog
/// lock — it only compares OPK spans (`PkBuf`) read verbatim from the frames'
/// PK regions against `frame_schema`.
///
/// Returns on the FIRST error (fault, corrupt or undecodable frame) and on
/// the first duplicate (the verdict is monotonic) — as in `drain_index_scan`,
/// without draining the remaining trains: the caller holds the `ScanLease` to
/// end of scope, so on return or cancellation the lease drop deregisters the
/// req_ids and `route_scan_slot` discards every undrained frame at the ring
/// boundary — a still-streaming worker never wedges in `send_encoded`.
async fn merge_index_scan(
    slots: Vec<W2mSlot>,
    req_ids: &[u64; crate::runtime::sal::MAX_WORKERS],
    reactor: &crate::runtime::reactor::Reactor,
    frame_schema: &SchemaDescriptor,
) -> Result<PreflightAccumulator, String> {
    use std::cmp::Reverse;
    use std::collections::BinaryHeap;

    // Both sides agree on the frame layout by construction: `frame_schema` is
    // `unique_preflight_wire_schema` of the same idx_schema the worker encodes
    // against, so no per-stream schema capture from the first frame is needed.
    // The PK region IS the OPK leading-key span; the merge reads it verbatim
    // as a `PkBuf`.
    let nw = slots.len();
    let mut streams: Vec<PreflightKeyStream> = Vec::with_capacity(nw);

    // Seed each stream with its first frame and prime the heap with each
    // worker's minimum (next_key pulls continuations if a first frame is
    // empty but non-terminal). Ordering the heap by (span, worker) — byte-
    // lexicographic via `PkBuf: Ord` — pops equal spans adjacently regardless
    // of which workers hold them, the merge order replacing numeric `u128`.
    let mut heap: BinaryHeap<Reverse<(PkBuf, usize)>> = BinaryHeap::with_capacity(nw);
    for (w, slot) in slots.into_iter().enumerate() {
        let mut s = PreflightKeyStream::new(w, req_ids[w]);
        s.attach_frame(slot, frame_schema)?;
        if let Some(key) = s.next_key(frame_schema, reactor).await? {
            heap.push(Reverse((key, w)));
        }
        streams.push(s);
    }

    let mut acc = PreflightAccumulator::new(UNIQUE_FILTER_CAP);
    while let Some(Reverse((key, w))) = heap.pop() {
        if !acc.offer(key) { break; } // first duplicate is conclusive
        if let Some(next) = streams[w].next_key(frame_schema, reactor).await? {
            heap.push(Reverse((next, w)));
        }
    }
    Ok(acc)
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
        return Err(format!("worker {worker}: {op_name}: {msg}"));
    }
    // The slot is forwarded as a complete reply; a chunked train here would
    // be truncated to its first frame, the remainder silently discarded by
    // the lease drop. Callers only route requests whose replies fit one frame
    // (e.g. a unique point seek), so a train means that invariant broke
    // (e.g. a shrunken GNITZ_REPLY_FRAME_BUDGET) — fail loudly instead.
    if ctrl.flags & FLAG_CONTINUATION != 0 {
        return Err(format!(
            "worker {worker}: {op_name}: unexpected chunked reply on a \
             single-frame path"));
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
            crate::schema::type_code::U128 => format!("{v}"),
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

/// Build a constraint-check batch from narrow `u128` PK keys. `src_type` is the
/// type of the column the keys came from (the child FK column, or the parent
/// PK/indexed column): a signed source sign-extends from its native width before
/// OPK-encoding at the leading promoted key column, byte-identical to the
/// write-side `IndexKeySpec::write_span`. For a base-table schema (the FK parent
/// fast-path) the key column does not promote, so `src_type == idx_key_type` and
/// the encode is the identity path.
fn build_check_batch(
    schema: &SchemaDescriptor,
    keys: &[u128],
    src_type: u8,
    pooled: Option<Batch>,
) -> Batch {
    // The index PK composite is `(indexed-value, src_pk_cols)` and is OPK-at-
    // rest. `keys` carry the indexed value (native u128); OPK-encode it into the
    // leading promoted key column and leave the source-PK suffix zero — only the
    // leading column is prefix-matched for the existence check. Narrow and wide
    // composites share this layout (the suffix width differs, the leading does
    // not), so there is no narrow/wide split.
    let stride = schema.pk_stride() as usize;
    // The leading key column is the index's column 0 for an index schema, but
    // the PK column for a base-table schema (the FK parent fast-path passes the
    // parent base table, whose lone PK may be declared at any column position).
    // `pk_indices()[0]` resolves both: an index schema is laid out
    // `(promoted_c0, src_pk…)`, so `pk_indices()[0] == 0 == columns[0]`.
    let key_col = schema.pk_indices()[0] as usize;
    let idx_key_type = schema.columns[key_col].type_code;
    build_check_batch_with(schema, keys, pooled, |b, &k| {
        let buf = crate::schema::index_opk_prefix(k, src_type, idx_key_type);
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

/// Decode an OPK leading-key span back to its native per-column values — the
/// inverse of `IndexKeySpec::write_span`, so the master stays the OPK *decoder*
/// and the worker the sole OPK *encoder*. `idx_cols` are the span's promoted
/// index columns; trailing array slots stay zero.
fn span_to_natives(span: &PkBuf, idx_cols: &[SchemaColumn]) -> [u128; gnitz_wire::PK_LIST_MAX_COLS] {
    let mut natives = [0u128; gnitz_wire::PK_LIST_MAX_COLS];
    let mut off = 0;
    for (native, col) in natives.iter_mut().zip(idx_cols) {
        let sz = col.size() as usize;
        let le = gnitz_wire::decode_pk_column_owned(
            &span.pk_bytes()[off..off + sz], col.type_code);
        *native = u128::from_le_bytes(le);
        off += sz;
    }
    natives
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

    /// OPK leading-key span of a single U64 value — the form `key_bytes`
    /// produces for a U64-promoted index column (U64 OPK == big-endian). Used to
    /// build expected filter/accumulator keys in these unit tests.
    fn span_u64(v: u64) -> PkBuf {
        PkBuf::from_bytes(&v.to_be_bytes())
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

    /// Span-extraction spec for a unique index on `cols` of `schema`, promoted
    /// via `make_index_schema` exactly as production circuit registration does.
    fn test_spec(cols: &[u32], schema: &SchemaDescriptor) -> IndexKeySpec {
        let idx_schema = crate::catalog::make_index_schema(cols, schema).unwrap();
        IndexKeySpec::new(cols, schema, &idx_schema)
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
        let batch = build_check_batch(&schema, &keys, type_code::U64, None);
        assert_eq!(batch.count, 1);

        let null_word = u64::from_le_bytes(batch.null_bmp_data()[0..8].try_into().unwrap());
        assert_eq!(null_word, u64::MAX, "all 64 payload columns must be marked null");
    }

    #[test]
    fn check_batch_nonleading_pk_probe_matches_stored_opk() {
        // Base table `(label STRING, id U64 PRIMARY KEY)` — PK at column 1.
        // The FK parent fast-path passes a base-table schema whose lone PK may
        // be declared at any column position; resolving the leading key column
        // from `columns[0]` (the STRING) would encode the probe at the wrong
        // type/width and mangle the existence check.
        let cols = vec![
            SchemaColumn::new(type_code::STRING, 0),
            SchemaColumn::new(type_code::U64, 0),
        ];
        let schema = SchemaDescriptor::new(&cols, &[1]);

        let keys = vec![42u128];
        let batch = build_check_batch(&schema, &keys, type_code::U64, None);

        // The parent stores id=42 as `encode_pk_column(42, U64)` = 42u64.to_be_bytes().
        let mut expected = [0u8; 8];
        gnitz_wire::encode_pk_column(&42u64.to_le_bytes(), type_code::U64, &mut expected);
        assert_eq!(batch.get_pk_bytes(0), &expected[..],
            "probe key must equal the stored OPK PK for a non-leading PK column");
    }

    #[test]
    fn filter_insert_basic() {
        let mut f = UniqueFilter::new();
        f.insert(span_u64(1));
        f.insert(span_u64(2));
        assert!(f.values.contains(&span_u64(1)));
        assert!(f.values.contains(&span_u64(2)));
        assert!(!f.capped);
    }

    #[test]
    fn filter_cap_clears_values() {
        // Exceed a small parameterized cap, verify the filter flips to
        // capped and its values HashSet is cleared whole.
        let mut f = UniqueFilter::with_cap(8);
        for k in 0..10u64 {
            f.insert(span_u64(k));
            if f.capped { break; }
        }
        assert!(f.capped, "filter should be capped after exceeding the limit");
        assert!(f.values.is_empty(), "values cleared once capped");
        // Further inserts are no-ops.
        f.insert(span_u64(99999999));
        assert!(f.values.is_empty());
    }

    #[test]
    fn extract_into_filter_pk_col() {
        // Schema: PK-only U64. Test that the PK-column locator extracts PKs.
        let schema = u64_schema();
        let batch = make_row_batch(schema, &[
            (10, 1, 0, 0),
            (20, 1, 0, 0),
            (30, -1, 0, 0),  // delete row — should be skipped
        ]);
        let mut filter = UniqueFilter::new();
        extract_into_filter(&mut filter, &batch.as_mem_batch(), &test_spec(&[0], &schema));
        assert!(filter.values.contains(&span_u64(10)));
        assert!(filter.values.contains(&span_u64(20)));
        assert!(!filter.values.contains(&span_u64(30)), "negative weight skipped");
    }

    #[test]
    fn extract_into_filter_signed_pk_col_uses_native_key() {
        // Single signed I64 PK indexed by itself. Its leading index key now
        // promotes to a *signed* I64 (order-preserving), so the extracted span is
        // the I64-OPK (sign-bit-flipped) of the NATIVE value — for a self-indexed
        // I64 PK that equals the source's at-rest OPK bytes, since the source type
        // already matches the index type. The extraction must build the span from
        // the *native* key (`pk_native_key`) re-encoded at the promoted index type
        // — feeding the OPK-widened `get_pk` value would double-flip and seek a
        // wrong key, hiding genuine duplicates.
        let schema = SchemaDescriptor::new(&[SchemaColumn::new(type_code::I64, 0)], &[0]);
        let mut opk = [0u8; 8];
        gnitz_wire::encode_pk_column(&(-5i64).to_le_bytes(), type_code::I64, &mut opk);
        let keys = [PkBuf::from_bytes(&opk)];
        let batch = build_check_batch_pkbuf(&schema, &keys, None);

        // The promoted leading key is I64, so the filter span is the I64-OPK of
        // the native value (= the at-rest OPK bytes here).
        let promoted_span = PkBuf::from_bytes(&opk);
        // The old U64-promotion image (BE of the two's-complement u64 bits, no
        // sign flip) is a DIFFERENT, non-order-preserving span the extractor must
        // NOT hold.
        let unsigned_span = PkBuf::from_bytes(&((-5i64) as u64).to_be_bytes());
        assert_ne!(promoted_span, unsigned_span,
            "signed I64 OPK (sign-flipped) differs from the unsigned image");

        let mut filter = UniqueFilter::new();
        extract_into_filter(&mut filter, &batch.as_mem_batch(), &test_spec(&[0], &schema));
        assert!(filter.values.contains(&promoted_span),
            "filter holds the I64-promoted native span");
        assert!(!filter.values.contains(&unsigned_span),
            "must not hold the non-order-preserving unsigned image");
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
        let mut filter = UniqueFilter::new();
        // Single payload column promoted to a U64 index column (8-byte span).
        extract_into_filter(&mut filter, &batch.as_mem_batch(), &test_spec(&[1], &schema));
        assert!(filter.values.contains(&span_u64(100)));
        assert!(!filter.values.contains(&span_u64(200)), "null values skipped");
        assert!(filter.values.contains(&span_u64(300)));
        assert_eq!(filter.values.len(), 2);
    }

    #[test]
    fn extract_into_filter_respects_capped() {
        let schema = u64_schema();
        let batch = make_row_batch(schema, &[
            (10, 1, 0, 0),
            (20, 1, 0, 0),
        ]);
        let mut filter = UniqueFilter::new();
        filter.capped = true;
        extract_into_filter(&mut filter, &batch.as_mem_batch(), &test_spec(&[0], &schema));
        assert!(filter.values.is_empty(), "no-op on capped filter");
    }

    /// Zero workers, null catalog, dummy SAL: the unique-filter methods
    /// touch only the `unique_filters` map.
    fn filter_dispatcher() -> MasterDispatcher {
        MasterDispatcher::new(
            0,
            Vec::new(),
            std::ptr::null_mut(),
            SalWriter::new(std::ptr::null_mut(), -1, 0, Vec::new()),
            W2mReceiver::new(Vec::new()),
        )
    }

    /// The full CREATE-time seed chain on a pre-flight that overflowed the
    /// cap: the accumulator's seed is empty-because-capped, not
    /// empty-because-the-table-is-empty, so the published filter must never
    /// prove a key absent (it must fall through to the broadcast).
    #[test]
    fn capped_preflight_seed_never_proves_absence() {
        let mut acc = PreflightAccumulator::new(3);
        for k in [10u64, 20, 30, 40] {
            assert!(acc.offer(span_u64(k)), "distinct keys never flip the verdict");
        }
        let (seed, capped) = acc.into_seed();
        assert!(capped, "cap + 1 distinct keys must report capped");
        let mut disp = filter_dispatcher();
        disp.unique_filter_seed(7, 0, seed, capped);
        assert!(
            !disp.unique_filter_all_absent(7, 0, &[span_u64(10)]),
            "a capped pre-flight seed must fall through to the broadcast",
        );
    }

    /// Exactly-at-cap pre-flight: the seed is the complete distinct set, so
    /// the published filter proves absence for fresh keys and reports seeded
    /// keys as possibly present.
    #[test]
    fn at_cap_preflight_seed_proves_absence() {
        let mut acc = PreflightAccumulator::new(3);
        for k in [10u64, 20, 30] {
            assert!(acc.offer(span_u64(k)));
        }
        let (seed, capped) = acc.into_seed();
        assert!(!capped, "exactly cap distinct keys must keep the full seed");
        let mut disp = filter_dispatcher();
        disp.unique_filter_seed(7, 0, seed, capped);
        assert!(disp.unique_filter_all_absent(7, 0, &[span_u64(40)]), "fresh key is provably absent");
        assert!(!disp.unique_filter_all_absent(7, 0, &[span_u64(20)]), "seeded key falls through");
    }

    /// A capped seed publishes a warm+capped filter whose entry exists in
    /// `unique_filters` — so `ensure_unique_filters_warm_async`'s
    /// `contains_key` skip applies and no key is ever proven absent.
    #[test]
    fn unique_filter_seed_capped_publishes_warm_capped_entry() {
        let mut disp = filter_dispatcher();
        disp.unique_filter_seed(7, 0, FxHashSet::default(), true);
        let filter = disp.unique_filters.get(&(7, 0)).expect("entry must exist");
        assert!(filter.warm);
        assert!(filter.capped);
        assert!(filter.values.is_empty());
        assert!(!disp.unique_filter_all_absent(7, 0, &[span_u64(12345)]));
    }

    // -- drain_index_scan unit tests ------------------------------------------
    //
    // Synthetic-train pattern: anonymous-mmap W2M rings (no fork), frames
    // pre-written via W2mWriter, the drain driven by a single manual poll with
    // a noop waker. Every fixture parks all continuation frames up front, so
    // a healthy drain never returns `Pending` — a `Pending` poll IS the
    // failure signal for a phantom-continuation regression.

    struct DrainFixture {
        ptrs: Vec<*mut u8>,
        reactor: crate::runtime::reactor::Reactor,
        receiver: crate::runtime::w2m::W2mReceiver,
        req_ids: [u64; crate::runtime::sal::MAX_WORKERS],
    }

    const DRAIN_RING_CAPACITY: usize = 64 * 1024;

    impl DrainFixture {
        fn new(n_workers: usize) -> (Self, Vec<crate::runtime::w2m::W2mWriter>) {
            use crate::runtime::w2m::{W2mReceiver, W2mWriter};
            use crate::runtime::w2m_ring;
            let mut ptrs = Vec::with_capacity(n_workers);
            let mut writers = Vec::with_capacity(n_workers);
            for _ in 0..n_workers {
                let ptr = unsafe {
                    let p = libc::mmap(
                        std::ptr::null_mut(), DRAIN_RING_CAPACITY,
                        libc::PROT_READ | libc::PROT_WRITE,
                        libc::MAP_ANONYMOUS | libc::MAP_SHARED, -1, 0,
                    ) as *mut u8;
                    assert!(!p.is_null(), "mmap failed");
                    std::ptr::write_bytes(p, 0, DRAIN_RING_CAPACITY);
                    w2m_ring::init_region_for_tests(p, DRAIN_RING_CAPACITY as u64);
                    p
                };
                writers.push(W2mWriter::new(ptr, DRAIN_RING_CAPACITY as u64));
                ptrs.push(ptr);
            }
            let reactor = crate::runtime::reactor::Reactor::new(16).expect("reactor");
            let mut req_ids = [0u64; crate::runtime::sal::MAX_WORKERS];
            for id in req_ids[..n_workers].iter_mut() {
                *id = reactor.alloc_scan_request_id();
            }
            let receiver = W2mReceiver::new(ptrs.clone());
            (DrainFixture { ptrs, reactor, receiver, req_ids }, writers)
        }

        /// Hand the first frame of every worker to the caller (what
        /// `dispatch_scan_fanout` returns) and park every later frame so
        /// `await_scan_slot` resolves without a live worker.
        fn initial_slots(&self) -> Vec<crate::runtime::w2m::W2mSlot> {
            let n = self.ptrs.len();
            let mut slots = Vec::with_capacity(n);
            for w in 0..n {
                slots.push(self.receiver.try_read_slot(w).expect("first frame"));
                while let Some(cont) = self.receiver.try_read_slot(w) {
                    self.reactor.test_route_scan_slot(cont);
                }
            }
            slots
        }

        fn teardown(self, lease: ScanLease) {
            // Drop the lease before the rings: its Drop purges scan_parked,
            // which would drop any still-queued W2mSlot borrowing the
            // soon-to-be-unmapped region.
            drop(lease);
            drop(self.reactor);
            drop(self.receiver);
            for ptr in self.ptrs {
                unsafe { libc::munmap(ptr as *mut libc::c_void, DRAIN_RING_CAPACITY); }
            }
        }
    }

    /// Encode one reply frame onto a test ring, mirroring the worker's reply
    /// shapes: `flags` carries the train flags (0 = single-frame reply).
    fn write_test_frame(
        writer: &crate::runtime::w2m::W2mWriter,
        req: u32,
        flags: u64,
        status: u32,
        error_msg: &[u8],
        schema: Option<&SchemaDescriptor>,
        batch: Option<&Batch>,
    ) {
        use crate::runtime::wire::{self as ipc};
        let sz = ipc::wire_size(status, error_msg, schema, None, batch, None, &[]);
        writer.send_encoded(sz, req, |buf| {
            ipc::encode_wire_into_ipc(
                buf, 0, 1, 0, flags, 0u128, 0, 0, status, error_msg,
                schema, None, batch, None, &[],
            );
        });
    }

    /// Poll a future exactly once with a noop waker; `None` on `Pending`.
    fn try_poll_once<T>(fut: impl std::future::Future<Output = T>) -> Option<T> {
        use std::task::{Context, Poll, Waker};
        let mut cx = Context::from_waker(Waker::noop());
        let mut fut = Box::pin(fut);
        match fut.as_mut().poll(&mut cx) {
            Poll::Ready(r) => Some(r),
            Poll::Pending => None,
        }
    }

    /// Drive a future to completion in exactly one poll, panicking on Pending.
    fn poll_once<T>(fut: impl std::future::Future<Output = T>) -> T {
        try_poll_once(fut).unwrap_or_else(|| panic!(
            "future did not complete in one poll: the drain awaited a \
             frame that is not (and will never be) parked"
        ))
    }

    /// A frame must still be parked for `req`: the drain returned without
    /// consuming it (the lease drop, not the drain, owns its disposal).
    fn assert_frame_still_parked(reactor: &crate::runtime::reactor::Reactor, req: u32) {
        assert!(
            try_poll_once(reactor.await_scan_slot(req)).is_some(),
            "expected an undrained parked frame — the drain consumed frames \
             past its early-return point"
        );
    }

    /// A worker fault must surface as an IMMEDIATE `Err` in a single poll —
    /// without draining the survivor's train (the caller's `ScanLease` drop
    /// owns the discard of undrained frames).
    ///
    /// Layout: worker 0 emits one `STATUS_ERROR` frame (flags 0, like
    /// `send_error`); worker 1 is healthy with a two-frame train whose
    /// `FLAG_SCAN_LAST` continuation is parked.
    ///  - If `has_more` were keyed on `FLAG_SCAN_LAST` alone (not
    ///    status-gated), worker 0's flags-0 error frame would read as "more
    ///    coming" and the first poll would return `Pending` — caught as a
    ///    failed assert instead of an infinite hang.
    ///  - If the drain still deferred the error (the pre-early-return shape),
    ///    worker 1's parked continuation would be consumed — caught by the
    ///    still-parked assert.
    #[test]
    fn drain_index_scan_errs_immediately_on_fault_frame() {
        use crate::runtime::wire::{STATUS_OK, STATUS_ERROR};

        let (fx, writers) = DrainFixture::new(2);
        let w0_req = fx.req_ids[0] as u32;
        let w1_req = fx.req_ids[1] as u32;

        write_test_frame(&writers[0], w0_req, 0, STATUS_ERROR, b"boom", None, None);
        write_test_frame(&writers[1], w1_req, FLAG_CONTINUATION, STATUS_OK, b"", None, None);
        write_test_frame(&writers[1], w1_req, FLAG_CONTINUATION | FLAG_SCAN_LAST,
                         STATUS_OK, b"", None, None);

        let lease = fx.reactor.scan_lease(&[w0_req, w1_req]);
        let slots = fx.initial_slots();

        // The frames carry no schema block, so `expected` is never consulted.
        let result = poll_once(drain_index_scan(
            slots, &fx.req_ids, &fx.reactor, "scan", &two_col_schema(), |_, _| Ok(()),
        ));
        let err = result.expect_err("worker fault must surface as Err");
        assert!(err.contains("worker 0"), "error names the faulted worker: {err}");
        assert!(err.contains("boom"), "error carries the worker message: {err}");

        // Early return: worker 1's continuation must still be parked.
        assert_frame_still_parked(&fx.reactor, w1_req);

        fx.teardown(lease);
    }

    /// Multi-frame trains from one worker merge with a single-frame (flag-free,
    /// length-1 train) reply from another: the sink sees every row, the
    /// continuation decodes against the schema hint saved from the first
    /// frame, and the flag-free frame terminates its train (the new
    /// `parse_train_header` contract — under the old "no FLAG_SCAN_LAST ⇒
    /// more" rule this test would hang at `Pending`).
    #[test]
    fn drain_index_scan_merges_chunked_and_single_frame_trains() {
        use crate::runtime::wire::STATUS_OK;

        let schema = two_col_schema();
        let chunk_a = make_row_batch(schema, &[(1, 1, 0, 10), (2, 1, 0, 20)]);
        let chunk_b = make_row_batch(schema, &[(3, 1, 0, 30)]);
        let single = make_row_batch(schema, &[(4, 1, 0, 40), (5, -1, 0, 50)]);

        let (fx, writers) = DrainFixture::new(2);
        let w0_req = fx.req_ids[0] as u32;
        let w1_req = fx.req_ids[1] as u32;

        // Worker 0: chunked train — schema on the first frame only.
        write_test_frame(&writers[0], w0_req, FLAG_CONTINUATION, STATUS_OK, b"",
                         Some(&schema), Some(&chunk_a));
        write_test_frame(&writers[0], w0_req, FLAG_CONTINUATION | FLAG_SCAN_LAST,
                         STATUS_OK, b"", None, Some(&chunk_b));
        // Worker 1: single-frame reply, no train flags (send_response shape).
        write_test_frame(&writers[1], w1_req, 0, STATUS_OK, b"",
                         Some(&schema), Some(&single));

        let lease = fx.reactor.scan_lease(&[w0_req, w1_req]);
        let slots = fx.initial_slots();

        let mut rows: Vec<(u128, i64)> = Vec::new();
        let result = poll_once(drain_index_scan(
            slots, &fx.req_ids, &fx.reactor, "seek_by_index", &schema,
            |mb, frame_len| {
                assert!(frame_len > 0, "sink receives the raw frame byte length");
                for i in 0..mb.count {
                    rows.push((mb.get_pk(i), mb.get_weight(i)));
                }
                Ok(())
            },
        ));
        result.expect("healthy trains must drain cleanly");
        assert_eq!(
            rows,
            vec![(1, 1), (2, 1), (3, 1), (4, 1), (5, -1)],
            "every frame's rows reach the sink in worker order, weights verbatim",
        );

        fx.teardown(lease);
    }

    /// A first-frame schema that does not match `expected` must error before
    /// any row reaches the sink: the batch append helpers do not validate
    /// shape, so a DDL-lagged worker reply would otherwise be mis-decoded.
    #[test]
    fn drain_index_scan_rejects_first_frame_schema_mismatch() {
        use crate::runtime::wire::STATUS_OK;

        let wire_schema = two_col_schema();
        let expected = u64_schema(); // different column count
        let batch = make_row_batch(wire_schema, &[(1, 1, 0, 10)]);

        let (fx, writers) = DrainFixture::new(1);
        let w0_req = fx.req_ids[0] as u32;
        write_test_frame(&writers[0], w0_req, 0, STATUS_OK, b"",
                         Some(&wire_schema), Some(&batch));

        let lease = fx.reactor.scan_lease(&[w0_req]);
        let slots = fx.initial_slots();

        let mut sink_calls = 0usize;
        let result = poll_once(drain_index_scan(
            slots, &fx.req_ids, &fx.reactor, "gather", &expected,
            |_, _| { sink_calls += 1; Ok(()) },
        ));
        let err = result.expect_err("schema mismatch must surface as Err");
        assert!(err.contains("Schema mismatch"), "error names the mismatch: {err}");
        assert!(err.contains("worker 0"), "error names the worker: {err}");
        assert_eq!(sink_calls, 0, "no row may reach the sink under a wrong schema");

        fx.teardown(lease);
    }

    /// A sink `Err` (the reply-cap path in `fan_out_index_collect_common`)
    /// aborts the drain immediately; the train's parked continuation stays
    /// parked for the lease drop to discard.
    #[test]
    fn drain_index_scan_sink_error_aborts_drain() {
        use crate::runtime::wire::STATUS_OK;

        let schema = two_col_schema();
        let chunk = make_row_batch(schema, &[(1, 1, 0, 10)]);

        let (fx, writers) = DrainFixture::new(1);
        let w0_req = fx.req_ids[0] as u32;
        write_test_frame(&writers[0], w0_req, FLAG_CONTINUATION, STATUS_OK, b"",
                         Some(&schema), Some(&chunk));
        write_test_frame(&writers[0], w0_req, FLAG_CONTINUATION | FLAG_SCAN_LAST,
                         STATUS_OK, b"", None, Some(&chunk));

        let lease = fx.reactor.scan_lease(&[w0_req]);
        let slots = fx.initial_slots();

        let result = poll_once(drain_index_scan(
            slots, &fx.req_ids, &fx.reactor, "seek_by_index", &schema,
            |_, _| Err("seek_by_index: result exceeds the reply cap".to_string()),
        ));
        let err = result.expect_err("sink error must abort the drain");
        assert!(err.contains("reply cap"), "sink error surfaces verbatim: {err}");

        // The terminal continuation was never consumed.
        assert_frame_still_parked(&fx.reactor, w0_req);

        fx.teardown(lease);
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
        let mut filter = UniqueFilter::new();
        // Index on a U32 column promotes to a U64 (8-byte) index column.
        extract_into_filter(&mut filter, &batch.as_mem_batch(), &test_spec(&[0], &schema));
        assert!(filter.values.contains(&span_u64(5)), "filter holds column A's value");
        assert_eq!(filter.values.len(), 1, "the shared A=5 collapses to one entry");
    }

    #[test]
    fn extract_into_filter_compound_pk_second_column_offset() {
        // Unique index on B (the second PK column at byte offset 4). Confirms
        // the locator slices the right column out of the packed key.
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
        let mut filter = UniqueFilter::new();
        extract_into_filter(&mut filter, &batch.as_mem_batch(), &test_spec(&[1], &schema));
        assert!(filter.values.contains(&span_u64(11)));
        assert!(filter.values.contains(&span_u64(22)));
        assert_eq!(filter.values.len(), 2);
    }
}

#[cfg(test)]
mod worker_liveness_tests {
    use super::*;
    use crate::runtime::sal::SalWriter;
    use crate::runtime::w2m::W2mReceiver;
    use crate::runtime::w2m_ring;

    const RING_CAP: usize = 64 * 1024;

    // Build an inert dispatcher for the pre-reactor liveness-probe paths: real
    // but empty W2M rings so the bootstrap wait loops can `try_read` (always
    // None here, so they reach the park / no-progress arm that probes), a null
    // SAL and catalog (untouched on the no-frame path), and the given worker
    // pids. Returns the ring pointers so the caller can munmap after dropping
    // the dispatcher (W2mReceiver holds the raw ptrs but does not own them).
    fn probe_dispatcher(worker_pids: Vec<i32>) -> (MasterDispatcher, Vec<*mut u8>) {
        let nw = worker_pids.len();
        let mut ptrs = Vec::with_capacity(nw);
        for _ in 0..nw {
            let p = unsafe {
                let p = libc::mmap(
                    std::ptr::null_mut(), RING_CAP,
                    libc::PROT_READ | libc::PROT_WRITE,
                    libc::MAP_ANONYMOUS | libc::MAP_SHARED, -1, 0,
                ) as *mut u8;
                assert!(!p.is_null(), "mmap failed");
                std::ptr::write_bytes(p, 0, RING_CAP);
                w2m_ring::init_region_for_tests(p, RING_CAP as u64);
                p
            };
            ptrs.push(p);
        }
        let disp = MasterDispatcher::new(
            nw,
            worker_pids,
            std::ptr::null_mut(),
            SalWriter::new(std::ptr::null_mut(), -1, 0, Vec::new()),
            W2mReceiver::new(ptrs.clone()),
        );
        (disp, ptrs)
    }

    fn free_rings(ptrs: &[*mut u8]) {
        for &p in ptrs {
            unsafe { libc::munmap(p as *mut libc::c_void, RING_CAP); }
        }
    }

    // Fork a child that exits immediately, then block-reap it. The pid is now a
    // confirmed non-child, so a later `waitpid` on it yields ECHILD — a
    // deterministic "dead" verdict with no race against the probe.
    fn spawn_and_reap_dead() -> i32 {
        let pid = unsafe { libc::fork() };
        assert!(pid >= 0, "fork failed");
        if pid == 0 {
            unsafe { libc::_exit(0) };
        }
        let mut status = 0;
        unsafe { libc::waitpid(pid, &mut status, 0) };
        pid
    }

    #[test]
    fn check_workers_reports_neg1_for_live_worker() {
        // Child blocks in pause() so it is unambiguously alive across the probe.
        let pid = unsafe { libc::fork() };
        assert!(pid >= 0, "fork failed");
        if pid == 0 {
            unsafe {
                libc::pause();
                libc::_exit(0);
            }
        }
        let (mut disp, ptrs) = probe_dispatcher(vec![pid]);
        assert_eq!(disp.check_workers(), -1, "a live worker must not be reported dead");
        assert_eq!(disp.worker_pids[0], pid, "a live worker's pid must be retained");
        unsafe {
            libc::kill(pid, libc::SIGKILL);
            let mut status = 0;
            libc::waitpid(pid, &mut status, 0);
        }
        drop(disp);
        free_rings(&ptrs);
    }

    #[test]
    fn check_workers_reaps_and_zeroes_then_does_not_re_report() {
        // An exited child becomes a zombie; the detecting `waitpid(WNOHANG)`
        // reaps it (rpid > 0) and must zero the slot so a second probe does not
        // re-`waitpid` a non-child and re-report the same worker as dead.
        let pid = unsafe { libc::fork() };
        assert!(pid >= 0, "fork failed");
        if pid == 0 {
            unsafe { libc::_exit(0) };
        }
        let (mut disp, ptrs) = probe_dispatcher(vec![pid]);
        // Bounded poll until the zombie is reaped by the probe (the child exits
        // near-instantly). The bound keeps a regression from hanging the suite.
        let mut detected = -1;
        for _ in 0..2000 {
            detected = disp.check_workers();
            if detected >= 0 {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        assert_eq!(detected, 0, "the exited worker must be detected dead");
        assert_eq!(disp.worker_pids[0], 0, "a reaped pid must be zeroed");
        assert_eq!(disp.check_workers(), -1, "a zeroed worker must not be re-reported");
        drop(disp);
        free_rings(&ptrs);
    }

    #[test]
    fn collect_acks_errors_when_worker_dies_before_acking() {
        // Boot-recovery path: wait_all_workers finds an empty ring and reaches
        // the park arm, whose liveness probe must surface the dead worker as a
        // clean error instead of looping on `wait_for` forever.
        let dead = spawn_and_reap_dead();
        let (mut disp, ptrs) = probe_dispatcher(vec![dead]);
        let err = disp.collect_acks().expect_err("a dead worker must fail ack collection");
        assert!(err.contains("worker 0"), "error names the dead worker: {err}");
        assert!(err.contains("recovery sync"), "error identifies the recovery path: {err}");
        drop(disp);
        free_rings(&ptrs);
    }

    #[test]
    fn collect_acks_and_relay_errors_when_worker_dies_mid_backfill() {
        // Backfill path: collect_acks_and_relay makes no progress on an empty
        // ring and reaches the !progressed arm, whose probe must surface the
        // dead worker.
        let dead = spawn_and_reap_dead();
        let (mut disp, ptrs) = probe_dispatcher(vec![dead]);
        let err = disp.collect_acks_and_relay(0)
            .expect_err("a dead worker must fail the backfill relay");
        assert!(err.contains("worker 0"), "error names the dead worker: {err}");
        assert!(err.contains("backfill relay"), "error identifies the backfill path: {err}");
        drop(disp);
        free_rings(&ptrs);
    }
}
