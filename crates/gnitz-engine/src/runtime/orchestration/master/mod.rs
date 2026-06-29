//! Master-side dispatcher: fans out push/scan operations to worker processes
//! via the shared append-only log (SAL) and collects responses via per-worker
//! W2M regions. Eventfds provide cross-process signaling.

use std::rc::Rc;

use rustc_hash::{FxHashMap, FxHashSet};

use crate::catalog::CatalogEngine;
use crate::schema::SchemaDescriptor;
use crate::schema::{payload_native_key, pk_native_key, IndexKeySpec, SchemaColumn};
use gnitz_wire::PkColList;

use crate::ops::{
    op_relay_broadcast, op_relay_scatter_consolidated_mode, op_repartition_batches_mode, with_broadcast_indices,
    with_worker_indices, worker_for_partition, PartitionRouter, RouteMode,
};
use crate::runtime::reactor::{AsyncMutex, PendingRelay, ScanLease};
use crate::runtime::sal::{
    pack_gather_cols, unique_preflight_wire_schema, SalWriter, BACKFILL_DECISION_CHECKPOINT,
    BACKFILL_DECISION_CONTINUE, BACKFILL_DECISION_STOP, FLAG_BACKFILL, FLAG_DDL_SYNC, FLAG_EXCHANGE,
    FLAG_EXCHANGE_RELAY, FLAG_FLUSH, FLAG_GATHER, FLAG_HAS_PK, FLAG_PUSH, FLAG_SEEK, FLAG_SEEK_BY_INDEX,
    FLAG_SEEK_BY_INDEX_RANGE_SAL, FLAG_SHUTDOWN, FLAG_TICK, FLAG_UNIQUE_PREFLIGHT,
};
use crate::runtime::w2m::{W2mReceiver, W2mSlot};
use crate::runtime::wire::{
    self, col_names_as_refs, peek_control_block, DecodedWire, SchemaWithVersion, WireConflictMode, FLAG_CONTINUATION,
    FLAG_HAS_DATA, FLAG_SCAN_LAST,
};
use crate::storage::{Batch, PkBuf};
use gnitz_wire::wire_flags_set_conflict_mode;

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
    FkParent {
        parent_table_id: i64,
        expected_count: usize,
    },
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
        UniqueFilter {
            values: FxHashSet::default(),
            cap,
            capped: false,
            warm: false,
        }
    }

    /// On overflow the set is cleared WHOLE, never truncated: a partial set
    /// would prove "absent" for a present key — a uniqueness hole.
    fn insert(&mut self, key: PkBuf) {
        if self.capped {
            return;
        }
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
fn extract_into_filter(filter: &mut UniqueFilter, batch: &crate::storage::MemBatch<'_>, spec: &IndexKeySpec) {
    let mut keybuf = PkBuf::empty(0);
    for row in 0..batch.count {
        if batch.get_weight(row) <= 0 {
            continue;
        }
        if !spec.key_bytes(batch, row, &mut keybuf) {
            continue;
        }
        filter.insert(keybuf);
        if filter.capped {
            return;
        } // stop walking once the filter caps
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
    /// Set after `take_w2m` hands the receiver to the reactor: a pointer to the
    /// reactor's stable `OnceCell` slot so `w2m()` keeps working for the one
    /// post-handoff caller — the reactor-parked stop-the-world CREATE-VIEW
    /// backfill. Null during boot (the receiver lives in `w2m` then). See
    /// `set_w2m_receiver_ptr` / `w2m`.
    w2m_ptr: *const W2mReceiver,
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

mod dispatch;
mod preflight;
mod unique_filter;

#[cfg(test)]
pub(crate) use preflight::PreflightAccumulator;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Return the first `worker N: <op>: <msg>` error in `decoded` (worker-index
/// order), or `None` if every reply has status 0. Shared by every fan-out
/// site that emits per-worker req_ids and decodes their replies in order.
pub(crate) fn first_worker_error(op: &str, decoded: &[DecodedWire]) -> Option<String> {
    worker_error_scan(op, decoded.iter().enumerate())
}

/// Variant of `first_worker_error` for the `Vec<Option<DecodedWire>>` slot
/// shape produced by `join_into`. Slots are guaranteed `Some` after the
/// future resolves; an unfilled slot indicates a bug in the join driver.
pub(crate) fn first_worker_error_opt(op: &str, decoded: &[Option<DecodedWire>]) -> Option<String> {
    worker_error_scan(
        op,
        decoded
            .iter()
            .enumerate()
            .map(|(w, d)| (w, d.as_ref().expect("join_into left a None slot — logic bug"))),
    )
}

fn worker_error_scan<'a>(op: &str, it: impl Iterator<Item = (usize, &'a DecodedWire)>) -> Option<String> {
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
    for (d, &s) in scan_ids[..nw].iter_mut().zip(&req_ids[..nw]) {
        *d = s as u32;
    }
    let lease = reactor.scan_lease(&scan_ids[..nw]);

    {
        let _guard = sal_excl.lock().await;
        unsafe {
            let disp = &mut *disp_ptr;
            submit(disp, &req_ids[..nw])?;
            disp.signal_all();
        }
    }
    let slots =
        crate::runtime::reactor::join_all_unpin(req_ids[..nw].iter().map(|&id| reactor.await_scan_slot(id as u32)))
            .await;
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
fn parse_train_header(slot: &W2mSlot, w: usize, what: &str) -> Result<(wire::DecodedControl, bool), String> {
    let ctrl = peek_control_block(slot.bytes()).map_err(|e| scan_decode_err(w, e))?;
    if ctrl.status != 0 {
        return Err(format!(
            "worker {}: {}: {}",
            w,
            what,
            String::from_utf8_lossy(&ctrl.error_msg)
        ));
    }
    let has_more = ctrl.flags & FLAG_SCAN_LAST == 0 && ctrl.flags & FLAG_CONTINUATION != 0;
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
            let schema_hint = saved_schema.as_ref().map(|(s, v)| SchemaWithVersion {
                descriptor: s,
                version: *v,
            });
            let zc = wire::decode_wire_ipc_zero_copy_with_ctrl(slot.bytes(), ctrl.block_size, ctrl, schema_hint)
                .map_err(|e| scan_decode_err(w, e))?;
            if saved_schema.is_none() {
                if let Some(ref s) = zc.schema {
                    crate::schema::validate_schema_match(s, expected)
                        .map_err(|e| format!("worker {w}: {what}: {e}"))?;
                    saved_schema = Some((*s, server_version));
                }
            }
            if let Some(ref mb) = zc.data_batch {
                if mb.count > 0 {
                    on_batch(mb, frame_len)?;
                }
            }
            drop(zc); // borrows slot
            drop(slot);
            if !has_more {
                break;
            }
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
        GatherMap {
            stride,
            ..Default::default()
        }
    }

    fn push_row(&mut self, pk: PkBuf, row: impl Iterator<Item = Option<u128>>) {
        let idx = (self.vals.len() / self.stride) as u32;
        self.vals.extend(row);
        debug_assert!(
            self.vals.len() == (idx as usize + 1) * self.stride,
            "gather row arity must equal the projection stride"
        );
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

fn format_uuid_hyphenated(v: u128) -> String {
    format!(
        "{:08x}-{:04x}-{:04x}-{:04x}-{:012x}",
        (v >> 96) as u32,
        (v >> 80) as u16,
        (v >> 64) as u16,
        (v >> 48) as u16,
        v & 0x0000_ffff_ffff_ffff
    )
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
            crate::schema::type_code::I64 => format!("{}", v as u64 as i64),
            crate::schema::type_code::I32 => format!("{}", v as u64 as i32),
            crate::schema::type_code::I16 => format!("{}", v as u64 as i16),
            crate::schema::type_code::I8 => format!("{}", v as u64 as i8),
            _ => format!("{}", v as u64),
        };
        parts.push(s);
        off += size;
    }
    parts.join(", ")
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
    let null_word: u64 = crate::ops::all_payload_null_mask(npc);
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
fn build_check_batch(schema: &SchemaDescriptor, keys: &[u128], src_type: u8, pooled: Option<Batch>) -> Batch {
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

/// Build the UPSERT PK-identification check batch from `keys`, the distinct
/// net-positive PK byte spans collected by the preflight aggregation. Writes
/// each `PkBuf`'s OPK bytes verbatim into the PK region: the spans are already
/// main-table PKs, so unlike the index builder `build_check_batch` no column-0
/// re-encoding is applied. The sole PK (rather than index) check-batch builder.
fn build_check_batch_pkbuf(schema: &SchemaDescriptor, keys: &[PkBuf], pooled: Option<Batch>) -> Batch {
    build_check_batch_with(schema, keys, pooled, |b, k| b.extend_pk_bytes(k.pk_bytes()))
}

/// Return `batch` to `disp.check_batch_pool[target_id]` and cap the pool depth.
fn recycle_check_batch(disp: &mut MasterDispatcher, target_id: i64, batch: Batch) {
    const POOL_MAX_DEPTH: usize = 4;
    const MAX_RETAIN_BYTES: usize = 512 * 1024;
    // A single large validation batch (bulk load, large FK check) would pin its
    // allocation in the pool indefinitely — Batch::clear() doesn't shrink.
    if batch.total_bytes() > MAX_RETAIN_BYTES {
        return; // let the allocator reclaim the oversized buffer
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

#[cfg(test)]
mod unique_filter_tests {
    use super::*;
    use crate::schema::{type_code, SchemaColumn};

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
        assert_eq!(
            batch.get_pk_bytes(0),
            &expected[..],
            "probe key must equal the stored OPK PK for a non-leading PK column"
        );
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
            if f.capped {
                break;
            }
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
        let batch = make_row_batch(
            schema,
            &[
                (10, 1, 0, 0),
                (20, 1, 0, 0),
                (30, -1, 0, 0), // delete row — should be skipped
            ],
        );
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
        assert_ne!(
            promoted_span, unsigned_span,
            "signed I64 OPK (sign-flipped) differs from the unsigned image"
        );

        let mut filter = UniqueFilter::new();
        extract_into_filter(&mut filter, &batch.as_mem_batch(), &test_spec(&[0], &schema));
        assert!(
            filter.values.contains(&promoted_span),
            "filter holds the I64-promoted native span"
        );
        assert!(
            !filter.values.contains(&unsigned_span),
            "must not hold the non-order-preserving unsigned image"
        );
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
        let batch = make_row_batch(
            schema,
            &[
                (1, 1, 0, 100), // payload=100, not null
                (2, 1, 1, 200), // null bit set → should be skipped
                (3, 1, 0, 300),
            ],
        );
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
        let batch = make_row_batch(schema, &[(10, 1, 0, 0), (20, 1, 0, 0)]);
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
        assert!(
            disp.unique_filter_all_absent(7, 0, &[span_u64(40)]),
            "fresh key is provably absent"
        );
        assert!(
            !disp.unique_filter_all_absent(7, 0, &[span_u64(20)]),
            "seeded key falls through"
        );
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
                        std::ptr::null_mut(),
                        DRAIN_RING_CAPACITY,
                        libc::PROT_READ | libc::PROT_WRITE,
                        libc::MAP_ANONYMOUS | libc::MAP_SHARED,
                        -1,
                        0,
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
            (
                DrainFixture {
                    ptrs,
                    reactor,
                    receiver,
                    req_ids,
                },
                writers,
            )
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
                unsafe {
                    libc::munmap(ptr as *mut libc::c_void, DRAIN_RING_CAPACITY);
                }
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
                buf,
                0,
                1,
                0,
                flags,
                0u128,
                0,
                0,
                status,
                error_msg,
                schema,
                None,
                batch,
                None,
                &[],
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
        try_poll_once(fut).unwrap_or_else(|| {
            panic!(
                "future did not complete in one poll: the drain awaited a \
             frame that is not (and will never be) parked"
            )
        })
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
        use crate::runtime::wire::{STATUS_ERROR, STATUS_OK};

        let (fx, writers) = DrainFixture::new(2);
        let w0_req = fx.req_ids[0] as u32;
        let w1_req = fx.req_ids[1] as u32;

        write_test_frame(&writers[0], w0_req, 0, STATUS_ERROR, b"boom", None, None);
        write_test_frame(&writers[1], w1_req, FLAG_CONTINUATION, STATUS_OK, b"", None, None);
        write_test_frame(
            &writers[1],
            w1_req,
            FLAG_CONTINUATION | FLAG_SCAN_LAST,
            STATUS_OK,
            b"",
            None,
            None,
        );

        let lease = fx.reactor.scan_lease(&[w0_req, w1_req]);
        let slots = fx.initial_slots();

        // The frames carry no schema block, so `expected` is never consulted.
        let result = poll_once(drain_index_scan(
            slots,
            &fx.req_ids,
            &fx.reactor,
            "scan",
            &two_col_schema(),
            |_, _| Ok(()),
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
        write_test_frame(
            &writers[0],
            w0_req,
            FLAG_CONTINUATION,
            STATUS_OK,
            b"",
            Some(&schema),
            Some(&chunk_a),
        );
        write_test_frame(
            &writers[0],
            w0_req,
            FLAG_CONTINUATION | FLAG_SCAN_LAST,
            STATUS_OK,
            b"",
            None,
            Some(&chunk_b),
        );
        // Worker 1: single-frame reply, no train flags (send_response shape).
        write_test_frame(&writers[1], w1_req, 0, STATUS_OK, b"", Some(&schema), Some(&single));

        let lease = fx.reactor.scan_lease(&[w0_req, w1_req]);
        let slots = fx.initial_slots();

        let mut rows: Vec<(u128, i64)> = Vec::new();
        let result = poll_once(drain_index_scan(
            slots,
            &fx.req_ids,
            &fx.reactor,
            "seek_by_index",
            &schema,
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
        write_test_frame(&writers[0], w0_req, 0, STATUS_OK, b"", Some(&wire_schema), Some(&batch));

        let lease = fx.reactor.scan_lease(&[w0_req]);
        let slots = fx.initial_slots();

        let mut sink_calls = 0usize;
        let result = poll_once(drain_index_scan(
            slots,
            &fx.req_ids,
            &fx.reactor,
            "gather",
            &expected,
            |_, _| {
                sink_calls += 1;
                Ok(())
            },
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
        write_test_frame(
            &writers[0],
            w0_req,
            FLAG_CONTINUATION,
            STATUS_OK,
            b"",
            Some(&schema),
            Some(&chunk),
        );
        write_test_frame(
            &writers[0],
            w0_req,
            FLAG_CONTINUATION | FLAG_SCAN_LAST,
            STATUS_OK,
            b"",
            None,
            Some(&chunk),
        );

        let lease = fx.reactor.scan_lease(&[w0_req]);
        let slots = fx.initial_slots();

        let result = poll_once(drain_index_scan(
            slots,
            &fx.req_ids,
            &fx.reactor,
            "seek_by_index",
            &schema,
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

        let s = format_pk_value_bytes(&uuid.to_be_bytes(), &schema);
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

        let sa = format_pk_value_bytes(&uuid_a.to_be_bytes(), &schema);
        let sb = format_pk_value_bytes(&uuid_b.to_be_bytes(), &schema);
        assert_ne!(sa, sb, "UUIDs differing in high bits must format differently");
    }

    fn compound_pk_bytes(parts: &[&[u8]]) -> PkBuf {
        let mut v = Vec::new();
        for p in parts {
            v.extend_from_slice(p);
        }
        PkBuf::from_bytes(&v)
    }

    #[test]
    fn format_pk_value_bytes_wide_compound_u64x3() {
        // Three U64 columns = 24-byte PK: too wide for a u128, exercising the
        // byte-form renderer where a u128 key would truncate.
        // OPK for unsigned U64 is big-endian.
        let schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::U64, 0),
            ],
            &[0, 1, 2],
        );
        assert!(schema.pk_stride() > 16);
        let pk = compound_pk_bytes(&[&7u64.to_be_bytes(), &8u64.to_be_bytes(), &9u64.to_be_bytes()]);
        assert_eq!(format_pk_value_bytes(pk.pk_bytes(), &schema), "7, 8, 9");
    }

    #[test]
    fn extract_into_filter_compound_pk_extracts_single_column() {
        // (A U32, B U32) both PK, unique index on A. Two rows share A=5 but
        // differ in B. Pre-fix get_pk(i) returned the packed (A,B) key, so the
        // filter held two distinct values; the fix slices out A only.
        let schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U32, 0),
                SchemaColumn::new(type_code::U32, 0),
            ],
            &[0, 1],
        );
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
        let schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U32, 0),
                SchemaColumn::new(type_code::U32, 0),
            ],
            &[0, 1],
        );
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
