//! Distributed PK / FK / unique-index preflight validation and violation
//! formatting: the check types (`PipelinedCheck` / `CheckPayload`), the
//! pipelined executors (`execute_pipeline` / `execute_gather` and the
//! check-batch builders/pool), the gather/merge key streams
//! (`PreflightKeyStream` / `PreflightAccumulator` / `merge_index_scan`), and
//! the error renderers.
//!
//! Every user write — a plain push and an atomic multi-family transaction
//! alike — is validated by `validate_txn_distributed`: a plain push is a
//! bundle of one family, so the four rules (U-PK, U-SEC, F1, F2) have one
//! implementation each. `validate_unique_index_create` is separate: it is the
//! DDL-time pre-flight for CREATE UNIQUE INDEX.

use super::*;

use super::unique_filter::{UniqueFilter, UNIQUE_FILTER_CAP};
use crate::catalog::FkEdge;
use gnitz_expr::{ColumnLocator, SchemaFacts};
use gnitz_store::storage::MemBatch;

// ---------------------------------------------------------------------------
// Pipelined validation checks
// ---------------------------------------------------------------------------

/// How a check's probe batch reaches the workers.
pub(super) enum CheckRoute {
    /// Replicate the same batch to every worker; each worker filters its local
    /// partition.
    Broadcast,
    /// Partition by the schema PK and scatter, so each worker is sent only the
    /// keys it stores. Delivered via `scatter::with_group` without materializing
    /// intermediate per-worker `Batch`es — `execute_pipeline` computes the
    /// routing from the check schema's `pk_indices()` via `with_worker_indices`.
    ScatterByPk,
}

/// A single distributed has-pk check queued for pipelined execution
/// (always dispatched under HasPk). `col_hint` is the worker's
/// `seek_col_idx`: `pack_pk_cols(&[col…])` for an index check (the packed flag
/// at bit 63 is always set, so it never collides with the PK sentinel) or 0 for
/// a PK check, optionally OR'd with `HAS_PK_WANT_HOLDER`.
pub(super) struct PipelinedCheck {
    pub(super) target_id: i64,
    pub(super) col_hint: u64,
    pub(super) route: CheckRoute,
    pub(super) batch: Batch,
    /// Carried, not looked up from `target_id`: for a unique secondary index
    /// this is the INDEX table's schema `(indexed_col, src_pk…)` sent under the
    /// owner table's id, which `handle_has_pk` reads back to size
    /// `idx_key_size`. The owner's own schema would give the wrong prefix width.
    pub(super) schema: wire::WireSchema,
}

impl PipelinedCheck {
    /// The `check_batch_pool` slot this check's batch belongs to: the probe
    /// target plus its key columns (`0` = the table's own PK). Probes on
    /// different key columns build against different schemas — a table-PK batch
    /// and an index batch — so a bare `target_id` key would hand each probe the
    /// other's batch and the staleness guard would drop the allocation on every
    /// pop, leaving the pool a pure cost.
    fn pool_slot(&self) -> PoolSlot {
        (self.target_id, gnitz_wire::pk_cols_word(self.col_hint))
    }
}

/// `(target_id, packed key columns)` — see [`PipelinedCheck::pool_slot`].
pub(super) type PoolSlot = (i64, u64);

/// Shared scaffold for the check-batch builders: pool-reuse with a schema
/// staleness guard, then one zero-payload row per key.
///
/// Schema staleness guard: a pooled batch built before a DDL change has
/// the wrong column layout; populating it would silently corrupt rows or
/// panic on column writes. When `pooled.schema != Some(schema)`, the
/// pooled allocation is dropped and a fresh batch is allocated instead.
///
/// `pk_of` yields the row's OPK key bytes (the only step that differs between
/// the `u128` and byte-span key forms). Keys arrive as an iterator so a caller
/// that already holds them in a map or a tuple list need not materialize a
/// second vector to probe them.
fn build_check_batch_with<K>(
    schema: &SchemaDescriptor,
    keys: impl ExactSizeIterator<Item = K>,
    pooled: Option<Batch>,
    mut pk_of: impl FnMut(&K) -> PkBuf,
) -> Batch {
    let n = keys.len();
    let mut batch = match pooled {
        Some(b) if b.schema == *schema => {
            let mut b = b;
            b.clear();
            b.reserve_rows(n);
            b
        }
        _ => Batch::with_capacity(*schema, n),
    };
    let null_word: u64 = gnitz_expr::SchemaFacts::nullable_payload_slots(schema);
    for key in keys {
        batch.push_zero_filled_row(pk_of(&key).pk_bytes(), 1, null_word);
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
pub(super) fn build_check_batch(
    schema: &SchemaDescriptor,
    keys: &[u128],
    src_type: u8,
    pooled: Option<Batch>,
) -> Batch {
    // The index PK composite is `(indexed-value, src_pk_cols)` and is OPK-at-
    // rest. `keys` carry the indexed value (native u128); `enc_key` OPK-encodes
    // it into the leading promoted key column and leaves the source-PK suffix
    // zero — only the leading column is prefix-matched for the existence check.
    // Narrow and wide composites share this layout (the suffix width differs,
    // the leading does not), so there is no narrow/wide split.
    build_check_batch_with(schema, keys.iter(), pooled, |&&k| enc_key(schema, k, src_type))
}

/// Build a check batch from `keys`, OPK byte spans already in the target
/// schema's key layout — the distinct PKs the preflight aggregation collected,
/// or a unique index's leading-key spans. Each span lands verbatim in the PK
/// region, so unlike `build_check_batch` no column-0 re-encoding is applied.
pub(super) fn build_check_batch_pk_bytes<'k>(
    schema: &SchemaDescriptor,
    keys: impl ExactSizeIterator<Item = &'k [u8]>,
    pooled: Option<Batch>,
) -> Batch {
    build_check_batch_with(schema, keys, pooled, |k| PkBuf::from_bytes(k))
}

/// Return `batch` to `disp.check_batch_pool[target_id]` and cap the pool depth.
pub(super) fn recycle_check_batch(disp: &MasterDispatcher, slot: PoolSlot, batch: Batch) {
    const POOL_MAX_DEPTH: usize = 4;
    const MAX_RETAIN_BYTES: usize = 512 * 1024;
    // A single large validation batch (bulk load, large FK check) would pin its
    // allocation in the pool indefinitely — Batch::clear() doesn't shrink.
    if batch.total_bytes() > MAX_RETAIN_BYTES {
        return; // let the allocator reclaim the oversized buffer
    }
    let mut map = disp.check_batch_pool.borrow_mut();
    let pool = map.entry(slot).or_default();
    pool.push(batch);
    if pool.len() > POOL_MAX_DEPTH {
        pool.remove(0);
    }
}

/// Push every check's batch back into its pool slot. Called after
/// `execute_pipeline` to recycle the allocations.
pub(super) fn reclaim_check_batches(disp: &MasterDispatcher, checks: Vec<PipelinedCheck>) {
    for check in checks {
        recycle_check_batch(disp, check.pool_slot(), check.batch);
    }
}

/// Per-worker state for one sorted-key stream in `merge_index_scan`.
///
/// The frame's decoded key region is kept as an offset+stride into the pinned
/// ring slot rather than as a borrowed view, so there is no self-reference to
/// keep alive and no drop-order contract between the two.
struct PreflightKeyStream {
    /// Worker index (error attribution) and scan request id (frame pulls).
    w: usize,
    req_id: u64,
    /// The frame currently being read; pins its ring bytes until replaced.
    slot: Option<W2mSlot>,
    /// The frame's PK region as `(byte offset into the slot, per-key stride)`
    /// and its key count. Zeroed for an empty frame.
    pk_off: usize,
    pk_stride: usize,
    count: usize,
    /// Cursor into the current frame's keys.
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
            pk_off: 0,
            pk_stride: 0,
            count: 0,
            row: 0,
            has_more: false,
        }
    }

    /// Install `slot` as the current frame, locating its key region.
    /// A fault/corrupt/undecodable frame is an immediate `Err` — the caller
    /// unwinds to the `ScanLease` drop, which discards the undrained trains.
    fn attach_frame(&mut self, slot: W2mSlot, frame_schema: &SchemaDescriptor) -> Result<(), String> {
        self.row = 0;
        self.count = 0;
        let (ctrl, has_more) = parse_train_header(&slot, self.w, "unique pre-flight").map_err(|f| f.text)?;
        self.has_more = has_more;
        // Every frame decodes against the shared compile-time wire schema
        // (version 0): the first frame's embedded schema block equals it by
        // construction (`send_unique_preflight_keys`), and continuation
        // frames carry no schema and resolve through the hint.
        let schema_hint = Some(SchemaWithVersion {
            descriptor: frame_schema,
            version: 0,
        });
        let bytes = slot.bytes();
        let mut offsets = [0usize; gnitz_store::storage::MAX_BATCH_REGIONS];
        let zc = wire::decode_wire_ipc_zero_copy_with_ctrl(bytes, ctrl, schema_hint, &mut offsets)
            .map_err(|e| scan_decode_err(self.w, e).text)?;
        if let Some(mb) = zc.data_batch.as_ref() {
            let pk = mb.pk();
            // `pk` points inside `bytes`, which `slot` owns for as long as it is
            // held; record where, then let the view go.
            self.pk_off = pk.as_ptr() as usize - bytes.as_ptr() as usize;
            self.pk_stride = mb.pk_stride as usize;
            self.count = mb.len();
        }
        drop(zc); // borrows `bytes`
        self.slot = Some(slot);
        Ok(())
    }

    /// The key at `row` of the current frame.
    fn key_at(&self, row: usize) -> PkBuf {
        let bytes = self.slot.as_ref().expect("frame attached").bytes();
        let start = self.pk_off + row * self.pk_stride;
        PkBuf::from_bytes(&bytes[start..start + self.pk_stride])
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
            if self.row < self.count {
                let key = self.key_at(self.row);
                self.row += 1;
                return Ok(Some(key));
            }
            if !self.has_more {
                self.slot = None; // release the ring slot at the train's end
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
    /// The filter this pre-flight will publish. `insert` owns the cap
    /// discipline: on overflow it drops the set whole and disables itself, so
    /// the seed is never truncated — a truncated seed would publish a warm but
    /// incomplete filter whose "proven absent" answers would let a genuine
    /// duplicate skip the INSERT broadcast. Every span reaching `insert` is
    /// distinct (spans arrive sorted, so duplicates are adjacent and stop at
    /// the `prev` check).
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
    /// Spans are byte-equal iff value-equal, so equality is a plain compare.
    pub(crate) fn offer(&mut self, key: PkBuf) -> bool {
        if self.duplicate {
            return false;
        }
        if self.prev == Some(key) {
            self.duplicate = true;
            return false;
        }
        self.prev = Some(key);
        self.filter.insert(key.pk_bytes());
        true
    }

    /// The filter holding every distinct span this pre-flight saw, ready to
    /// publish.
    pub(crate) fn into_seed(self) -> UniqueFilter {
        self.filter
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
/// boundary — a still-streaming worker never wedges in `W2mWriter::send_msg`.
async fn merge_index_scan(
    slots: Vec<W2mSlot>,
    scan: &ScanDispatch,
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
    for (i, slot) in slots.into_iter().enumerate() {
        let (w, req_id) = scan.reply(i);
        let mut s = PreflightKeyStream::new(w, req_id);
        s.attach_frame(slot, frame_schema)?;
        if let Some(key) = s.next_key(frame_schema, reactor).await? {
            heap.push(Reverse((key, w)));
        }
        streams.push(s);
    }

    let mut acc = PreflightAccumulator::new(UNIQUE_FILTER_CAP);
    while let Some(Reverse((key, w))) = heap.pop() {
        if !acc.offer(key) {
            break;
        } // first duplicate is conclusive
        if let Some(next) = streams[w].next_key(frame_schema, reactor).await? {
            heap.push(Reverse((next, w)));
        }
    }
    Ok(acc)
}

/// Cap on the distinct referenced values one write may fetch committed children
/// for (Rule F2). Beyond it the write is rejected, keeping a delete-heavy one
/// from turning validation into an unbounded master-side materialization under
/// the held table locks.
const TXN_RESTRICT_FETCH_LIMIT: usize = 1024;

/// One decoded, shape-validated transaction family: the target `tid`, its
/// conflict `mode`, and the decoded batch. The family schema is resolved from
/// the catalog by tid (every family of a tid shares one schema, checked at shape
/// time). The executor decodes into these, the validator borrows them, and the
/// committer takes them by value and emits each as one `Push` group.
pub struct TxnFamily {
    pub tid: i64,
    pub mode: WireConflictMode,
    pub batch: Batch,
}

/// Per-PK last operation in a table's whole-bundle fold. `Inserted` names the
/// surviving row by `(family index into the bundle's family list, row index)`.
#[derive(Clone, Copy)]
enum FoldOp {
    Inserted(u32, u32),
    Deleted,
}

/// One table's fold: the last op per PK. The key is the row's OPK bytes
/// borrowed from the family batch's PK region — the families outlive every
/// bundle built over them, and a bulk push folds millions of rows, where an
/// owned 81-byte `PkBuf` key would cost several times the borrowed slice.
type Overlay<'a> = FxHashMap<&'a [u8], FoldOp>;

/// How a bundled parent write removed a referenced value, which decides the
/// verb of the RESTRICT rejection.
#[derive(Clone, Copy, PartialEq, Eq)]
enum RetireVerb {
    /// The holding row is gone after the transaction.
    Delete,
    /// The holding row survives holding a different value.
    Update,
}

/// The `(retired, added)` referenced-value sets of one bundled FK parent column
/// (see `parent_retired_added`). Each retired value carries the verb of the
/// write that retired it; a value retired by both a delete and an update reads
/// as a delete, so the verdict does not depend on fold order.
type ParentDelta = (FxHashMap<u128, RetireVerb>, FxHashSet<u128>);

/// `(parent tid, referenced col)` → its delta. Resolved once per key and shared
/// by rules F1 and F2, which both turn on it.
type ParentDeltas = FxHashMap<(i64, usize), ParentDelta>;

/// The [`ParentDeltas`] key `e`'s referenced value lives under. `FkEdge` is the
/// engine's type, so an inherent method on it is not available here.
fn delta_key(e: &FkEdge) -> (i64, usize) {
    (e.parent_tid, e.parent_col)
}

impl FkProbePlan {
    /// Whether the probe found `v` occupied. Both rules compare against the
    /// found-set through this one encode, which `build_check_batch` wrote the
    /// probe keys with, so the byte images match by construction.
    fn probed_present(&self, found: &FxHashSet<PkBuf>, v: u128) -> bool {
        found.contains(&enc_key(&self.schema, v, self.src_type))
    }
}

/// Fold family `fi`'s rows into `overlay` — last op per PK wins (`w > 0` ⇒
/// `Inserted`, `w < 0` ⇒ `Deleted`, `w == 0` rows skipped). Applied over a
/// table's families in frame order it yields the post-transaction fold; applied
/// over a prefix of them it yields the state an Error family is checked against.
fn fold_family<'a>(overlay: &mut Overlay<'a>, fi: usize, b: &'a Batch) {
    for row in 0..b.len() {
        let w = b.get_weight(row);
        if w == 0 {
            continue;
        }
        overlay.insert(
            b.get_pk_bytes(row),
            if w > 0 {
                FoldOp::Inserted(fi as u32, row as u32)
            } else {
                FoldOp::Deleted
            },
        );
    }
}

/// What one batch does to one PK.
#[derive(Default)]
struct PkFold {
    /// Summed weight over the PK's rows.
    net: i64,
    /// Positive occurrences. A `+w` row counts as `w` insertions — Error-mode
    /// duplicate rejection treats it like the `w` separate `+1` rows it encodes
    /// — so any value `> 1` is a within-batch duplicate.
    dups: u32,
}

/// Fold one batch per PK, keyed by the row's borrowed OPK bytes. Feeds the
/// Error-mode PK rule, which counts a PK's insertions within one family — what
/// the last-op-wins whole-bundle `Overlay` cannot express.
fn pk_fold(batch: &Batch) -> FxHashMap<&[u8], PkFold> {
    let mut fold: FxHashMap<&[u8], PkFold> = FxHashMap::with_capacity_and_hasher(batch.len(), Default::default());
    for i in 0..batch.len() {
        let w = batch.get_weight(i);
        if w == 0 {
            continue;
        }
        let e = fold.entry(batch.get_pk_bytes(i)).or_default();
        e.net += w;
        if w > 0 {
            e.dups += if w > 1 { 2 } else { 1 };
        }
    }
    fold
}

/// The verb for the RESTRICT rejection on referenced value `v`: how the bundled
/// parent write removed it. Both removals are a "cannot do this to the row"
/// rejection, so anything but a surviving row holding a new value reads as a
/// delete.
fn restrict_verb(retired: &FxHashMap<u128, RetireVerb>, v: u128) -> &'static str {
    match retired.get(&v) {
        Some(RetireVerb::Update) => "update",
        _ => "delete from",
    }
}

/// Whether any rule will read `tid`'s overlay — U-SEC walks the survivors of a
/// table with a unique index, F1 those of an FK child, and F2 /
/// `parent_retired_added` read the overlay of an FK child or parent. A plain
/// `INSERT` into a table with neither constraint reads none, so the O(rows) fold
/// is skipped entirely there. A rule that reads an overlay outside this
/// predicate panics on the missing entry rather than silently seeing an empty
/// fold.
fn reads_overlay(disp: &MasterDispatcher, tid: i64) -> bool {
    disp.cat().has_row_constraints(tid)
}

/// A decoded transaction bundle plus everything its rules share: the per-table
/// family lists in frame order, each table's catalog schema, and the
/// whole-bundle fold. Built once; every rule reads it instead of re-walking the
/// families.
struct TxnBundle<'a> {
    families: &'a [TxnFamily],
    /// One borrowed columnar view per family, built once. `MemBatch` is ~600
    /// bytes (its region-offset array dominates), so rebuilding it per row —
    /// as every rule's surviving-row walk would otherwise do — costs a
    /// half-kilobyte copy per row.
    mems: Vec<MemBatch<'a>>,
    /// The bundle's tids, in first-appearance order.
    order: Vec<i64>,
    /// tid → indices into `families`, in frame order.
    by_tid: FxHashMap<i64, Vec<usize>>,
    schemas: FxHashMap<i64, SchemaDescriptor>,
    /// Per-table last-op-per-PK fold, present only for the tables some rule
    /// actually reads (see `reads_overlay`).
    overlays: FxHashMap<i64, Overlay<'a>>,
}

impl<'a> TxnBundle<'a> {
    fn new(disp: &MasterDispatcher, families: &'a [TxnFamily]) -> Result<Self, String> {
        let mut order: Vec<i64> = Vec::new();
        let mut by_tid: FxHashMap<i64, Vec<usize>> = FxHashMap::default();
        for (fi, fam) in families.iter().enumerate() {
            by_tid
                .entry(fam.tid)
                .or_insert_with(|| {
                    order.push(fam.tid);
                    Vec::new()
                })
                .push(fi);
        }
        let mut schemas = FxHashMap::default();
        let mut overlays = FxHashMap::default();
        for &tid in &order {
            schemas.insert(tid, disp.cat().registry().table_entry(tid)?.schema);
            if !reads_overlay(disp, tid) {
                continue;
            }
            let rows = by_tid[&tid].iter().map(|&fi| families[fi].batch.len()).sum();
            let mut overlay = Overlay::with_capacity_and_hasher(rows, Default::default());
            for &fi in &by_tid[&tid] {
                fold_family(&mut overlay, fi, &families[fi].batch);
            }
            overlays.insert(tid, overlay);
        }
        Ok(TxnBundle {
            mems: families.iter().map(|f| f.batch.as_mem_batch()).collect(),
            families,
            order,
            by_tid,
            schemas,
            overlays,
        })
    }

    /// Is `tid` one of the bundle's tables?
    fn has(&self, tid: i64) -> bool {
        self.by_tid.contains_key(&tid)
    }

    fn schema(&self, tid: i64) -> &SchemaDescriptor {
        &self.schemas[&tid]
    }

    fn overlay(&self, tid: i64) -> &Overlay<'a> {
        &self.overlays[&tid]
    }

    /// Family `fam`'s columnar view, built once in `new`.
    fn mem(&self, fam: u32) -> &MemBatch<'a> {
        &self.mems[fam as usize]
    }

    fn family_indices(&self, tid: i64) -> &[usize] {
        &self.by_tid[&tid]
    }

    fn families_of(&self, tid: i64) -> impl Iterator<Item = &TxnFamily> {
        self.by_tid[&tid].iter().map(|&fi| &self.families[fi])
    }

    /// The rows of `tid` that survive the whole bundle: `(pk, family, row)` —
    /// the `Inserted` projection of its overlay. Walked, never materialized: a
    /// bulk push's survivor list would be a second copy of the overlay, and
    /// each reader (U-SEC once per unique index, F1 once per constraint)
    /// consumes it in one pass.
    fn surviving(&self, tid: i64) -> impl Iterator<Item = (&'a [u8], u32, u32)> + '_ {
        self.overlays[&tid].iter().filter_map(|(pk, op)| match op {
            FoldOp::Inserted(f, r) => Some((*pk, *f, *r)),
            FoldOp::Deleted => None,
        })
    }
}

/// One planned FK probe: a committed-occupancy check for `values`, encoded
/// under `schema`/`src_type`. Which side of `edge` is probed and which a
/// violation names is fixed by the rule, not stored — F1 probes the parent and
/// reports the child, F2 probes the child and reports the parent.
struct FkProbePlan {
    edge: FkEdge,
    schema: SchemaDescriptor,
    src_type: u8,
    values: Vec<u128>,
}

/// One planned unique-secondary-index check: the circuit's columns, the key
/// encoder (whose `key_size()` splits each reply entry into `[span ‖ holder]`),
/// and the `(span, surviving claimant PK)` pairs to verify against the
/// `[span ‖ committed holder PK]` entries the pipelined probe returns.
struct UniquePlan<'a> {
    tid: i64,
    col_indices: PkColList,
    spec: IndexKeySpec,
    /// The distinct surviving spans and who claims each, sorted by span — the
    /// order the check batch is emitted in, so a reply is looked up by binary
    /// search. Claimant PKs borrow the family batch's PK region, like the
    /// overlay keys.
    by_span: Vec<(PkBuf, &'a [u8])>,
}

/// Encode a native value `v` (from a column of type `src_type`) into the OPK
/// leading-key image of `schema`'s primary key. The one encoder for these probe
/// keys: `build_check_batch` writes exactly this, so a membership test against
/// the pipeline found-set (which echoes matched probe keys) compares identical
/// byte images by construction.
///
/// The leading key column is the index's column 0 for an index schema, but the
/// PK column for a base-table schema (the FK parent fast-path passes the parent
/// base table, whose lone PK may be declared at any column position).
/// `pk_indices()[0]` resolves both: an index schema is laid out
/// `(promoted_c0, src_pk…)`, so `pk_indices()[0] == 0 == columns[0]`.
fn enc_key(schema: &SchemaDescriptor, v: u128, src_type: u8) -> PkBuf {
    let key_col = schema.pk_indices()[0] as usize;
    let idx_key_type = schema.columns[key_col].type_code;
    gnitz_store::schema::key::index_opk_prefix(v, src_type, idx_key_type).widened(schema.pk_stride() as usize)
}

impl MasterDispatcher {
    /// Fire one pipelined probe burst and recycle the check batches back to the
    /// pool. `execute_pipeline` already returns an empty result for an empty
    /// `checks`, so callers need no length guard. The paired reclaim is centralized
    /// here so no probe site can forget it (a leaked pooled batch).
    async fn execute_and_reclaim(
        disp: &MasterDispatcher,
        reactor: &crate::runtime::reactor::Reactor,
        checks: Vec<PipelinedCheck>,
    ) -> Result<Vec<FxHashSet<PkBuf>>, String> {
        let results = Self::execute_pipeline(disp, reactor, &checks).await?;
        reclaim_check_batches(disp, checks);
        Ok(results)
    }

    /// Validate a user-table write bundle against its post-transaction state
    /// (the simulated fold of all families), except Error-mode PK existence
    /// which is cumulative in frame order. The whole bundle passes or one
    /// violation aborts it — all pre-SAL. Every write comes through here: a
    /// plain push is a bundle of one family, so each of the four rules has one
    /// implementation. Each rule plans every probe it needs first and issues
    /// them as ONE pipelined burst. Committed state is stable across every
    /// probe because the caller holds the involved tables' locks and the
    /// catalog read lock through the ACK.
    pub async fn validate_txn_distributed(
        &self,
        reactor: &crate::runtime::reactor::Reactor,
        families: &[TxnFamily],
    ) -> Result<(), String> {
        // No family whose write reads committed state ⇒ every rule below would
        // find nothing to check, so the bundle (an O(rows) fold) is not built.
        let cat = self.cat();
        let reads_committed = families.iter().any(|f| cat.push_reads_committed_state(f.tid, f.mode));
        if !reads_committed {
            return Ok(());
        }
        let bundle = TxnBundle::new(self, families)?;
        let committed = Self::txn_check_pk(self, reactor, &bundle).await?;
        Self::txn_check_unique_indices(self, reactor, &bundle).await?;
        Self::txn_check_foreign_keys(self, reactor, &bundle, &committed).await
    }

    /// Rule U-PK: Error-mode PK existence, cumulative in frame order. One
    /// committed-existence probe per probed table, all issued in one burst;
    /// then each table's families are walked in frame order and each Error
    /// family checked against the running prefix fold before being folded into
    /// it.
    ///
    /// Returns each probed table's committed PK set, which `parent_retired_added`
    /// reads: only a touched PK that exists committed has an old referenced value
    /// to retire.
    async fn txn_check_pk(
        disp: &MasterDispatcher,
        reactor: &crate::runtime::reactor::Reactor,
        b: &TxnBundle<'_>,
    ) -> Result<FxHashMap<i64, FxHashSet<PkBuf>>, String> {
        let mut checks: Vec<PipelinedCheck> = Vec::new();
        for &tid in &b.order {
            let error_mode = b.families_of(tid).any(|f| matches!(f.mode, WireConflictMode::Error));
            // A bundled FK parent makes the probe worth issuing even without an
            // Error family: `parent_retired_added` decides which touched PKs have
            // an old referenced value from exactly this answer, and without it
            // falls back to synthesising one for every touched PK.
            let fk_parent = !disp.cat().fk_children_of(tid).is_empty();
            if !error_mode && !fk_parent {
                continue;
            }
            // Candidate PKs to probe committed, borrowed from the family batches'
            // PK regions. For an FK parent that is every touched PK — a deleted one
            // retires its referenced value just as an overwritten one does. For
            // Error mode alone it is the PKs an Error family inserts positively (a
            // superset of each family's net-positive set); with none, no Error
            // family can carry a net-positive PK, so the whole table's walk is
            // vacuous.
            let keys: Vec<&[u8]> = if fk_parent {
                b.overlay(tid).keys().copied().collect()
            } else {
                let mut candidate: FxHashSet<&[u8]> = FxHashSet::default();
                for fam in b.families_of(tid).filter(|f| matches!(f.mode, WireConflictMode::Error)) {
                    for row in 0..fam.batch.len() {
                        if fam.batch.get_weight(row) > 0 {
                            candidate.insert(fam.batch.get_pk_bytes(row));
                        }
                    }
                }
                candidate.into_iter().collect()
            };
            if keys.is_empty() {
                continue;
            }
            let schema = *b.schema(tid);
            let pooled = disp.pool_pop_batch((tid, 0));
            checks.push(PipelinedCheck {
                target_id: tid,
                col_hint: 0,
                route: CheckRoute::ScatterByPk,
                batch: build_check_batch_pk_bytes(&schema, keys.into_iter(), pooled),
                schema: wire::WireSchema::encoded(tid, schema),
            });
        }
        let probed: Vec<i64> = checks.iter().map(|c| c.target_id).collect();
        let results = Self::execute_and_reclaim(disp, reactor, checks).await?;

        let mut committed_by_tid: FxHashMap<i64, FxHashSet<PkBuf>> = FxHashMap::default();
        for (tid, committed) in probed.into_iter().zip(results) {
            let schema = *b.schema(tid);
            // The state each Error family is checked against: the fold of the
            // families before it. A single-family table needs none, so the
            // whole-batch fold is skipped there — the common case, since a plain
            // push is one family.
            let fis = b.family_indices(tid);
            let mut prefix: Overlay = Overlay::default();
            for (n, &fi) in fis.iter().enumerate() {
                let batch = &b.families[fi].batch;
                if matches!(b.families[fi].mode, WireConflictMode::Error) {
                    // Error-family PK existence, checked against the running
                    // prefix fold then committed state.
                    for (&pk, f) in &pk_fold(batch) {
                        if f.dups > 1 {
                            return Err(disp.cat().pk_violation_err(tid, &schema, pk, true));
                        }
                        if f.net <= 0 {
                            continue;
                        }
                        let exists = match prefix.get(pk) {
                            Some(FoldOp::Inserted(..)) => true,
                            Some(FoldOp::Deleted) => false,
                            None => committed.contains(pk),
                        };
                        if exists {
                            return Err(disp.cat().pk_violation_err(tid, &schema, pk, false));
                        }
                    }
                }
                if n + 1 < fis.len() {
                    fold_family(&mut prefix, fi, batch);
                }
            }
            committed_by_tid.insert(tid, committed);
        }
        Ok(committed_by_tid)
    }

    /// Rule U-SEC: unique secondary indexes, post-transaction. Every
    /// (table, unique circuit)'s surviving spans are planned up front and their
    /// committed-occupancy probes issued in ONE burst — the only round trip the
    /// rule takes. The probe runs under `HAS_PK_WANT_HOLDER`, so each occupied
    /// span comes back as `[span ‖ committed holder PK]`: the answer that decides
    /// the verdict is read out of the same reply that established the span is
    /// occupied, from the same per-worker index store, with no interval in which
    /// it could go stale.
    ///
    /// A warm unique filter keeps even that burst off the hot path, eliding the
    /// whole plan for a provably-absent span set (the steady state of a fresh-key
    /// insert stream).
    async fn txn_check_unique_indices<'a>(
        disp: &MasterDispatcher,
        reactor: &crate::runtime::reactor::Reactor,
        b: &TxnBundle<'a>,
    ) -> Result<(), String> {
        let mut plans: Vec<UniquePlan<'a>> = Vec::new();
        let mut checks: Vec<PipelinedCheck> = Vec::new();
        for &tid in &b.order {
            let cat = disp.cat();
            let (n_circuits, has_unique) = (
                cat.registry().index_circuits(tid).len(),
                cat.registry().has_any_unique_index(tid),
            );
            if !has_unique {
                continue;
            }
            if b.surviving(tid).next().is_none() {
                continue;
            }
            // Warm the filters before planning: a provably-absent span set
            // elides the whole broadcast below. The warm-up is one O(table)
            // scan fan-out per (table, index) per process.
            Self::ensure_unique_filters_warm(disp, reactor, tid).await?;
            for ci in 0..n_circuits {
                // One circuit lookup: its column list, index schema, and the
                // span-encode plan baked at registration. Copied out so the
                // catalog borrow ends before the `&mut` dispatcher calls below.
                let (col_indices, idx_schema, spec) = {
                    let ic = &disp.cat().registry().index_circuits(tid)[ci];
                    if !ic.is_unique {
                        continue;
                    }
                    (ic.col_indices, ic.index_schema, ic.key_spec)
                };
                let cols = col_indices.as_slice();
                let stride = idx_schema.pk_stride() as usize;

                // Surviving span → holder PK. `surviving` yields each PK once,
                // so once sorted an adjacent-equal pair always comes from two
                // different rows — an in-bundle duplicate. The sort also fixes
                // the order the check batch is emitted in: the worker probes it
                // with one cursor, and `advance_to` gallops in place only on a
                // strictly greater key, so an unsorted batch forfeits the
                // gallop and repositions every source on each backward step.
                let mut by_span: Vec<(PkBuf, &'a [u8])> = Vec::with_capacity(b.overlay(tid).len());
                let mut keybuf = PkBuf::zeroed(0);
                for (pk, fam, row) in b.surviving(tid) {
                    if !spec.key_bytes(b.mem(fam), row as usize, &mut keybuf) {
                        continue; // NULL in an indexed column ⇒ unindexed
                    }
                    by_span.push((keybuf, pk));
                }
                if by_span.is_empty() {
                    continue;
                }
                by_span.sort_unstable_by_key(|&(span, _)| span);
                if by_span.windows(2).any(|w| w[0].0 == w[1].0) {
                    return Err(disp.cat().unique_violation_err(tid, cols, true));
                }

                // Every planned span provably absent from the committed index ⇒
                // the broadcast would answer "none occupied" and leave nothing to
                // verify. Skipping the whole plan is what keeps a fresh-key INSERT
                // stream a one-burst operation.
                let packed = gnitz_wire::pack_pk_cols(cols);
                if disp.unique_filter_all_absent(tid, packed, by_span.iter().map(|(s, _)| s.pk_bytes())) {
                    continue;
                }
                let pooled = disp.pool_pop_batch((tid, packed));
                let chk =
                    build_check_batch_pk_bytes(&idx_schema, by_span.iter().map(|(s, _)| s.padded(stride)), pooled);
                checks.push(PipelinedCheck {
                    target_id: tid,
                    // The reply must name the committed holder of each occupied
                    // span, not echo the probe key back.
                    col_hint: packed | gnitz_wire::HAS_PK_WANT_HOLDER,
                    route: CheckRoute::Broadcast,
                    batch: chk,
                    schema: wire::WireSchema::encoded(tid, idx_schema),
                });
                plans.push(UniquePlan {
                    tid,
                    col_indices,
                    spec,
                    by_span,
                });
            }
        }
        let results = Self::execute_and_reclaim(disp, reactor, checks).await?;

        // Each reply entry is an occupied span plus the committed row holding it,
        // `[span ‖ holder PK]`, split back apart by index layout alone.
        //
        // A span held on two different workers contributes two entries and each
        // holder is verified on its own; the `FxHashSet` collapses a replicated
        // owner's `W` identical answers to one.
        let mut hspan = PkBuf::zeroed(0);
        for (plan, occupied) in plans.iter().zip(&results) {
            for entry in occupied {
                let (span, holder) = plan.spec.split_entry(entry.pk_bytes());
                // Every entry answers a span this plan probed, so the claimer is
                // always present.
                let Ok(i) = plan
                    .by_span
                    .binary_search_by(|(s, _)| gnitz_store::schema::key::compare_pk_bytes(s.pk_bytes(), span))
                else {
                    continue;
                };
                let claimer = plan.by_span[i].1;
                // The holder IS the surviving row claiming the span — nothing to
                // vacate.
                if holder == claimer {
                    continue;
                }
                // Otherwise the bundle must retire it: the holder's surviving state
                // is absent, or it no longer holds this span.
                let retired = match b.overlay(plan.tid).get(holder) {
                    None => false,
                    Some(FoldOp::Deleted) => true,
                    Some(FoldOp::Inserted(hf, hr)) => {
                        !plan.spec.key_bytes(b.mem(*hf), *hr as usize, &mut hspan) || hspan.pk_bytes() != span
                    }
                };
                if !retired {
                    return Err(disp
                        .cat()
                        .unique_violation_err(plan.tid, plan.col_indices.as_slice(), false));
                }
            }
        }
        Ok(())
    }

    /// Rules F1 (FK existence) and F2 (FK RESTRICT), post-transaction.
    ///
    /// Both rules turn on the `(retired, added)` referenced-value sets of a
    /// bundled parent, so those are resolved once per `(parent tid, referenced
    /// col)` and shared. F1's parent-existence probes then issue in one burst,
    /// F2's child-reference probes in a second, and F2's per-value exemption
    /// fetches fan out concurrently.
    async fn txn_check_foreign_keys(
        disp: &MasterDispatcher,
        reactor: &crate::runtime::reactor::Reactor,
        b: &TxnBundle<'_>,
        committed_pks: &FxHashMap<i64, FxHashSet<PkBuf>>,
    ) -> Result<(), String> {
        // Every FK whose child is bundled (F1 checks those rows' values exist),
        // and every FK whose parent is bundled (F2 checks the values it removes
        // are unreferenced).
        let mut constraints: Vec<FkEdge> = Vec::new();
        let mut children: Vec<FkEdge> = Vec::new();
        for &tid in &b.order {
            let cat = disp.cat();
            constraints.extend(cat.fk_constraints_of(tid).iter().copied());
            children.extend(cat.fk_children_of(tid).iter().copied());
        }
        if constraints.is_empty() && children.is_empty() {
            return Ok(());
        }

        // Resolve `(retired, added)` once per bundled (parent tid, referenced col).
        // Two FKs onto the same parent column would otherwise redo the identical
        // overlay walk and committed-value gather.
        let mut needed: Vec<(i64, usize)> = constraints
            .iter()
            .filter(|e| b.has(e.parent_tid))
            .chain(children.iter())
            .map(delta_key)
            .collect();
        needed.sort_unstable();
        needed.dedup();
        // Fan the per-parent gathers out concurrently — they run under the full
        // lock union, so overlapping their reply waits (as the F2 exemption
        // fetches already do) beats one sequential round trip per parent column.
        let futs: Vec<_> = needed
            .iter()
            .map(|&(ptid, pcol)| Box::pin(Self::parent_retired_added(disp, reactor, b, committed_pks, ptid, pcol)))
            .collect();
        let resolved = crate::runtime::reactor::join_all_unpin(futs).await;
        let mut deltas: ParentDeltas = ParentDeltas::default();
        for (&(ptid, pcol), d) in needed.iter().zip(resolved) {
            deltas.insert((ptid, pcol), d?);
        }

        Self::txn_check_fk_existence(disp, reactor, b, &constraints, &deltas).await?;
        Self::txn_check_fk_restrict(disp, reactor, b, &children, &deltas).await
    }

    /// Rule F1: every surviving row's FK value must reference a row that exists
    /// after the transaction — present in committed state and not retired by the
    /// bundle, or added by it.
    async fn txn_check_fk_existence(
        disp: &MasterDispatcher,
        reactor: &crate::runtime::reactor::Reactor,
        b: &TxnBundle<'_>,
        constraints: &[FkEdge],
        deltas: &ParentDeltas,
    ) -> Result<(), String> {
        let mut plans: Vec<FkProbePlan> = Vec::new();
        let mut checks: Vec<PipelinedCheck> = Vec::new();
        for &edge in constraints {
            let FkEdge {
                child_tid: tid,
                fk_col,
                parent_tid,
                parent_col,
            } = edge;
            let source_schema = b.schema(tid);
            let loc = source_schema.locate(fk_col);

            // The distinct non-NULL FK values of this table's surviving rows.
            let mut seen: FxHashSet<u128> = FxHashSet::default();
            let mut values: Vec<u128> = Vec::new();
            for (_pk, fam, row) in b.surviving(tid) {
                let mb = b.mem(fam);
                let Some(v) = loc.native_key_opt(mb, row as usize) else {
                    continue;
                };
                if seen.insert(v) {
                    values.push(v);
                }
            }
            if values.is_empty() {
                continue;
            }

            // PK fast-path only when the referenced column *is* the parent's lone
            // PK; otherwise probe the parent's UNIQUE index by broadcast, since
            // index entries are distributed independently of the PK.
            let parent_schema = disp.cat().registry().table_entry(parent_tid)?.schema;
            let src_type = loc.type_code();
            let (probe_schema, col_hint, broadcast) = if parent_schema.is_lone_pk_col(parent_col) {
                (parent_schema, 0u64, false)
            } else {
                let idx_schema = disp
                    .cat()
                    .registry()
                    .index_circuit_for_cols(parent_tid, &[parent_col as u32])
                    .map(|ic| ic.index_schema)
                    .ok_or_else(|| format!("FK check: no unique index on parent {parent_tid} col {parent_col}"))?;
                (idx_schema, gnitz_wire::pack_pk_cols(&[parent_col as u32]), true)
            };
            let pooled = disp.pool_pop_batch((parent_tid, col_hint));
            let chk = build_check_batch(&probe_schema, &values, src_type, pooled);
            checks.push(PipelinedCheck {
                target_id: parent_tid,
                col_hint,
                route: if broadcast {
                    CheckRoute::Broadcast
                } else {
                    CheckRoute::ScatterByPk
                },
                batch: chk,
                schema: wire::WireSchema::encoded(parent_tid, probe_schema),
            });
            plans.push(FkProbePlan {
                edge,
                schema: probe_schema,
                src_type,
                values,
            });
        }
        let results = Self::execute_and_reclaim(disp, reactor, checks).await?;

        // A non-bundled parent has no delta (the degenerate plain-push case).
        let no_delta: ParentDelta = (FxHashMap::default(), FxHashSet::default());
        for (plan, probed) in plans.iter().zip(&results) {
            let (retired, added) = deltas.get(&delta_key(&plan.edge)).unwrap_or(&no_delta);
            for v in &plan.values {
                let in_committed = plan.probed_present(probed, *v);
                if (in_committed && !retired.contains_key(v)) || added.contains(v) {
                    continue;
                }
                return Err(disp.cat().fk_missing_err(plan.edge.child_tid, plan.edge.parent_tid));
            }
        }
        Ok(())
    }

    /// Rule F2: a referenced value the bundle removes and does not re-add must
    /// have no surviving child row referencing it. `exists_after(v)` for
    /// `v ∈ retired` reduces to `added.contains(v)` (the committed term is masked
    /// by `v ∈ retired`), so the checked set is `retired ∖ added`.
    async fn txn_check_fk_restrict(
        disp: &MasterDispatcher,
        reactor: &crate::runtime::reactor::Reactor,
        b: &TxnBundle<'_>,
        children: &[FkEdge],
        deltas: &ParentDeltas,
    ) -> Result<(), String> {
        let mut plans: Vec<FkProbePlan> = Vec::new();
        let mut checks: Vec<PipelinedCheck> = Vec::new();
        for &edge in children {
            let FkEdge {
                child_tid,
                fk_col,
                parent_tid,
                parent_col,
            } = edge;
            let (retired, added) = &deltas[&delta_key(&edge)];
            let v_check: Vec<u128> = retired.keys().copied().filter(|v| !added.contains(v)).collect();
            if v_check.is_empty() {
                continue;
            }
            let idx_schema = disp
                .cat()
                .registry()
                .index_circuit_for_cols(child_tid, &[fk_col as u32])
                .map(|ic| ic.index_schema)
                .ok_or_else(|| format!("FK RESTRICT: no index on child {child_tid} col {fk_col}"))?;
            let src_type = b.schema(parent_tid).columns[parent_col].type_code;
            let col_hint = gnitz_wire::pack_pk_cols(&[fk_col as u32]);
            let pooled = disp.pool_pop_batch((child_tid, col_hint));
            checks.push(PipelinedCheck {
                target_id: child_tid,
                col_hint,
                route: CheckRoute::Broadcast,
                batch: build_check_batch(&idx_schema, &v_check, src_type, pooled),
                schema: wire::WireSchema::encoded(child_tid, idx_schema),
            });
            plans.push(FkProbePlan {
                edge,
                schema: idx_schema,
                src_type,
                values: v_check,
            });
        }
        let results = Self::execute_and_reclaim(disp, reactor, checks).await?;

        // A committed child reference is fatal unless the bundle also touches the
        // child table and every referencing child row is retired or re-pointed —
        // which needs the child rows themselves. Collect those fetches first, then
        // fan them out.
        let mut fetches: Vec<(usize, u128)> = Vec::new();
        for (i, (plan, probed)) in plans.iter().zip(&results).enumerate() {
            for v in &plan.values {
                if !plan.probed_present(probed, *v) {
                    continue; // no committed children reference v
                }
                if !b.has(plan.edge.child_tid) {
                    // Untouched committed children exist and no bundled child
                    // family can exempt them.
                    let verb = restrict_verb(&deltas[&delta_key(&plan.edge)].0, *v);
                    return Err(disp
                        .cat()
                        .fk_restrict_err(plan.edge.parent_tid, plan.edge.child_tid, verb));
                }
                fetches.push((i, *v));
            }
        }
        if fetches.is_empty() {
            return Ok(());
        }
        // The limit counts referenced values that still have committed children,
        // not the rows being deleted, so splitting the statement does not help.
        // Deleting leaf-first does, exactly: once a value's children are gone
        // from committed state the probe above finds no hit and plans no fetch.
        if fetches.len() > TXN_RESTRICT_FETCH_LIMIT {
            return Err(format!(
                "too many referenced values still have committed children (limit \
                 {TXN_RESTRICT_FETCH_LIMIT}); delete referencing rows before the rows they reference"
            ));
        }
        let futs: Vec<_> = fetches
            .iter()
            .map(|&(i, v)| {
                let plan = &plans[i];
                Box::pin(Self::fan_out_seek_by_index_collect(
                    disp,
                    reactor,
                    plan.edge.child_tid,
                    gnitz_wire::pack_pk_cols(&[plan.edge.fk_col as u32]),
                    v,
                    &[],
                ))
            })
            .collect();
        let fetched = crate::runtime::reactor::join_all_unpin(futs).await;

        for (&(i, v), rows) in fetches.iter().zip(fetched) {
            let plan = &plans[i];
            // A worker fault, a schema mismatch and the per-value reply cap are
            // distinct failures; only the last is about the number of children,
            // and it names itself in its own message.
            let Some(rows) = rows? else { continue };
            let child_loc = b.schema(plan.edge.child_tid).locate(plan.edge.fk_col);
            let child_overlay = b.overlay(plan.edge.child_tid);
            for j in 0..rows.len() {
                let still_refs = match child_overlay.get(rows.get_pk_bytes(j)) {
                    None => true, // untouched committed child still references v
                    Some(FoldOp::Deleted) => false,
                    Some(FoldOp::Inserted(cf, cr)) => {
                        let smb = b.mem(*cf);
                        child_loc.native_key_opt(smb, *cr as usize) == Some(v)
                    }
                };
                if still_refs {
                    let verb = restrict_verb(&deltas[&delta_key(&plan.edge)].0, v);
                    return Err(disp
                        .cat()
                        .fk_restrict_err(plan.edge.parent_tid, plan.edge.child_tid, verb));
                }
            }
        }
        Ok(())
    }

    /// The `(retired, added)` referenced-column value sets of bundled FK parent
    /// `parent_tid` for column `ref_col`: `added` are the surviving parent rows'
    /// (non-NULL) values; `retired` are the old committed values of touched PKs
    /// whose surviving state is absent or holds a different value. The old values
    /// come from the packed PK (a PK column) or one batched `execute_gather`
    /// (a non-PK referenced column). NULL values are unindexed and excluded.
    ///
    /// Only a PK that exists in committed state has an old value to retire, so
    /// `committed_pks` — U-PK's per-table answer, when it probed one — bounds
    /// the walk and the gather. A pure INSERT into a parent table touches no
    /// committed PK, which empties both.
    async fn parent_retired_added(
        disp: &MasterDispatcher,
        reactor: &crate::runtime::reactor::Reactor,
        b: &TxnBundle<'_>,
        committed_pks: &FxHashMap<i64, FxHashSet<PkBuf>>,
        parent_tid: i64,
        ref_col: usize,
    ) -> Result<ParentDelta, String> {
        let parent_schema = b.schema(parent_tid);
        let overlay = b.overlay(parent_tid);
        let committed = committed_pks.get(&parent_tid);
        let touched = || {
            overlay
                .keys()
                .copied()
                .filter(|p| committed.is_none_or(|c| c.contains(*p)))
        };

        // Old committed value per touched PK (non-NULL only).
        let mut old_of: FxHashMap<&[u8], u128> = FxHashMap::default();
        if parent_schema.is_pk_col(ref_col) {
            let col_type = parent_schema.columns[ref_col].type_code;
            let col_size = parent_schema.columns[ref_col].size() as usize;
            let off = parent_schema.pk_byte_offset(ref_col) as usize;
            for p in touched() {
                old_of.insert(p, pk_native_key(p, off, col_size, col_type));
            }
        } else {
            let pks: Vec<PkBuf> = touched().map(PkBuf::from_bytes).collect();
            if !pks.is_empty() {
                let gathered = Self::execute_gather(disp, reactor, parent_tid, pks, ref_col as u8).await?;
                for p in touched() {
                    if let Some(&v) = gathered.get(p) {
                        old_of.insert(p, v);
                    }
                }
            }
        }

        let loc = parent_schema.locate(ref_col);
        let mut added: FxHashSet<u128> = FxHashSet::default();
        let mut retired: FxHashMap<u128, RetireVerb> = FxHashMap::default();
        for (p, op) in overlay {
            let surviving_val: Option<u128> = match op {
                FoldOp::Inserted(f, r) => {
                    let mb = b.mem(*f);
                    loc.native_key_opt(mb, *r as usize)
                }
                FoldOp::Deleted => None,
            };
            if let Some(sv) = surviving_val {
                added.insert(sv);
            }
            if let Some(&ov) = old_of.get(p) {
                if surviving_val != Some(ov) {
                    // The row is gone, or survives holding something else. A
                    // value both deleted and updated away reads as deleted, so
                    // the verb does not depend on the overlay's iteration order.
                    let verb = match op {
                        FoldOp::Deleted => RetireVerb::Delete,
                        FoldOp::Inserted(..) => RetireVerb::Update,
                    };
                    let e = retired.entry(ov).or_insert(verb);
                    if verb == RetireVerb::Delete {
                        *e = RetireVerb::Delete;
                    }
                }
            }
        }
        Ok((retired, added))
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
    /// at any width, including a composite key that no single number could
    /// represent.
    ///
    /// On success the index is safe to commit and broadcast and the returned
    /// filter seeds the master's unique-filter cache; on failure the
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
    pub async fn validate_unique_index_create(
        &self,
        reactor: &crate::runtime::reactor::Reactor,
        owner_id: i64,
        col_indices: &[u32],
    ) -> Result<UniqueFilter, String> {
        let (idx_schema, packed) = {
            let cat = self.cat();
            let owner_schema = match cat.registry().get_schema_desc(owner_id) {
                Some(s) => s,
                None => return Ok(UniqueFilter::new()),
            };
            // Trivial-uniqueness short-circuit, generalised to the compound PK:
            // a composite index whose columns are the table's enforced-unique PK
            // (set equality — the SQL may list them in another order) can never
            // collide, so the scan is skipped and the seed stays empty. The index
            // key IS the PK here, so a span collision would be a PK collision,
            // which `enforce_unique_pk` already makes impossible.
            let pk = owner_schema.pk_indices();
            if col_indices.len() == pk.len() && pk.iter().all(|p| col_indices.contains(p)) {
                return Ok(UniqueFilter::new());
            }
            // Build the index schema (the circuit is not registered until this
            // pre-flight succeeds) for the merge's reply-frame layout and the
            // promoted per-column widths. Identical inputs to each worker's own
            // build, so the frame schema agrees by construction. `packed` is
            // the column list the worker resolves the seek by.
            (
                gnitz_store::schema::make_index_schema(col_indices, &owner_schema)?,
                gnitz_wire::pack_pk_cols(col_indices),
            )
        };
        let frame_schema = unique_preflight_wire_schema(&idx_schema, col_indices.len());

        // Single-source a REPLICATED owner: under a fan-out each distinct value
        // would arrive `nw` times and pop adjacently in the merge —
        // `PreflightAccumulator::offer` reads that as a duplicate and fails the
        // CREATE on a genuinely unique table. The count-agnostic merge still
        // catches real within-copy duplicates via the same adjacent-equal
        // check, and the seed reflects true cardinality. Hashed owners keep the
        // full fan-out (genuine cross-partition duplicates surface as equal
        // spans from different workers).
        let unicast = replicated_unicast(self, owner_id);

        // Fan out the pre-flight command (the packed column list rides in
        // seek_col_idx); each worker answers with its sorted-span
        // continuation-frame train. `_lease` held to end of scope: when the
        // merge returns early (error or duplicate verdict) the lease drop
        // discards the undrained trains at the ring boundary.
        let (slots, scan) = dispatch_scan_fanout(self, reactor, unicast, |targets| {
            // The worker's `UniquePreflight` arm resolves the owner's schema
            // from its own catalog.
            self.write_group(
                wire::WireMsg {
                    target_id: owner_id as u64,
                    seek_col_idx: packed,
                    ..Default::default()
                },
                GroupData::NONE,
                0,
                SalMessageKind::UniquePreflight,
                ZoneMark::Plain,
                targets,
            )
        })
        .await
        .map_err(|f| f.text)?;

        let merged = merge_index_scan(slots, &scan, reactor, &frame_schema).await?;
        if merged.duplicate {
            return Err(self.cat().unique_create_dup_err(owner_id, col_indices));
        }
        Ok(merged.into_seed())
    }
}

impl MasterDispatcher {
    /// Writes each check with per-worker req_ids, signals once, and joins all
    /// replies.
    ///
    /// One write can need O(N) distributed has-pk checks (one per FK
    /// constraint, one per FK child with restrict deletes, one committed-PK
    /// probe per table, one per unique secondary index). Issuing them
    /// sequentially would cost N master↔worker round trips; this writes the
    /// whole burst into SAL, signals once, then collects N responses per worker
    /// in a single poll loop, correlating each by the per-(check, worker)
    /// request id it was issued under. Each rule therefore plans every probe it
    /// needs before issuing any.
    ///
    /// `sal_excl` is held only for the synchronous write + signal phase;
    /// see `dispatch_scan_fanout` for the rationale.
    pub(super) async fn execute_pipeline(
        disp: &MasterDispatcher,
        reactor: &crate::runtime::reactor::Reactor,
        checks: &[PipelinedCheck],
    ) -> Result<Vec<FxHashSet<PkBuf>>, String> {
        let num_checks = checks.len();
        if num_checks == 0 {
            return Ok(Vec::new());
        }

        let (nw, all_req_ids) = {
            let _guard = disp.sal_excl().lock().await;
            let nw = disp.num_workers();
            let rids = reactor.alloc_replies(num_checks * nw);
            for (idx, check) in checks.iter().enumerate() {
                let req_slice = &rids[idx * nw..(idx + 1) * nw];
                match check.route {
                    CheckRoute::Broadcast => disp
                        .write_group(
                            check.schema.frame(wire::WireMsg {
                                seek_col_idx: check.col_hint,
                                ..Default::default()
                            }),
                            GroupData::Same(wire::WireData::Whole(Some(&check.batch))),
                            0,
                            SalMessageKind::HasPk,
                            ZoneMark::Plain,
                            GroupTargets::All(req_slice),
                        )
                        .map_err(|f| f.text)?,
                    CheckRoute::ScatterByPk => disp
                        .write_scatter_group(
                            &check.batch,
                            &check.schema,
                            SalMessageKind::HasPk,
                            check.col_hint,
                            GroupTargets::All(req_slice),
                        )
                        .map_err(|f| f.text)?,
                }
            }
            disp.signal_all();
            (nw, rids)
        };

        let decoded_vec: Vec<DecodedWire> =
            crate::runtime::reactor::join_all_unpin(all_req_ids.iter().map(|&id| reactor.await_reply(id))).await;

        // Replies are laid out check-major, so the worker index is the position
        // within each check's block of `nw`.
        if let Some(err) = decoded_vec
            .iter()
            .enumerate()
            .find_map(|(i, d)| worker_error(i % nw, "pipeline", &d.control))
        {
            return Err(err.text);
        }
        // Each set holds only the probe keys that turned out to exist committed
        // — for a fresh-key insert stream, none — so it is grown on demand
        // rather than reserved at the probe count.
        let mut results: Vec<FxHashSet<PkBuf>> = (0..num_checks).map(|_| FxHashSet::default()).collect();
        for check_idx in 0..num_checks {
            for w in 0..nw {
                let decoded = &decoded_vec[check_idx * nw + w];
                if let Some(ref batch) = decoded.data_batch {
                    for j in 0..batch.len() {
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
    /// replies with them projected to `ref_col` (the referenced parent column
    /// index). Returns a `pk → promoted index key` map. A PK is absent from it
    /// when its committed row is absent OR when that row holds NULL in
    /// `ref_col` — the caller indexes referenced values, and a NULL one is
    /// unindexed either way.
    ///
    /// This is the `O(num_workers)`-round-trip replacement for the per-row
    /// serial single-key seek loop used by FK RESTRICT on non-PK UNIQUE
    /// targets. It is a sibling of `execute_pipeline` rather than a modification
    /// of it: the has-pk pipeline answers one key per matched probe row — the
    /// probe key echoed back, or under `HAS_PK_WANT_HOLDER` the matched index
    /// entry — so it can name the row that matched but never project its
    /// columns, which is what a gather is for.
    ///
    /// Replies arrive as reply trains (an oversized gather reply chunks; a
    /// single-frame reply is a length-1 train), so the fan-out uses scan
    /// request ids and the train drain. The expected projected schema guards
    /// each train's first frame — a worker whose catalog lags a DDL would
    /// otherwise hand back rows the master mis-decodes.
    pub(super) async fn execute_gather(
        disp: &MasterDispatcher,
        reactor: &crate::runtime::reactor::Reactor,
        target_id: i64,
        mut pks: Vec<PkBuf>,
        ref_col: u8,
    ) -> Result<FxHashMap<PkBuf, u128>, String> {
        if pks.is_empty() {
            return Ok(FxHashMap::default());
        }
        // Sort so each worker's sublist reaches `gather_family` ascending:
        // `removed`/updated PKs are extracted from an FxHashMap (arbitrary
        // order) and `scatter::with_group` preserves per-worker relative order,
        // so a globally sorted input yields per-worker-sorted sublists.
        pks.sort_unstable();

        let parent_schema = disp.cat().registry().table_entry(target_id)?.schema;
        // The exact constructor the worker uses for its reply schema, so a
        // matching reply validates by construction. A PK `ref_col` would be
        // skipped and leave the reply payload-less; the sole caller branches on
        // `is_pk_col` and reaches this only on the payload arm.
        debug_assert!(!parent_schema.is_pk_col(ref_col as usize));
        let expected = gnitz_store::schema::project_schema(&parent_schema, &[ref_col as u32])
            .expect("a one-column projection fits MAX_COLUMNS");
        let parent = wire::WireSchema::encoded(target_id, parent_schema);

        // `_lease` held across the full drain below (see `dispatch_scan_fanout`).
        let (slots, scan) = dispatch_scan_fanout(disp, reactor, Fanout::Broadcast, |targets| {
            let pooled = disp.pool_pop_batch((target_id, 0));
            let batch = build_check_batch_pk_bytes(&parent_schema, pks.iter().map(|p| p.pk_bytes()), pooled);
            disp.write_scatter_group(&batch, &parent, SalMessageKind::Gather, ref_col as u64, targets)?;
            // The scatter batch is fully consumed by the synchronous
            // scatter above; return it to the pool.
            recycle_check_batch(disp, (target_id, 0), batch);
            Ok(())
        })
        .await
        .map_err(|f| f.text)?;

        // The reply's one payload column, resolved off the schema the frames are
        // guarded against rather than off the parent's — the projection is what
        // decides which slot and width the rows carry. It is not column 0:
        // `project_schema` keeps the PK region ahead of it.
        let projected = SchemaFacts::payload_col_idx(&expected, 0);
        let ColumnLocator::Payload { slot, size, type_code } = expected.locate(projected) else {
            unreachable!("a gather projects one payload column")
        };
        let (slot, size) = (slot as usize, size as usize);

        let mut out: FxHashMap<PkBuf, u128> = FxHashMap::default();
        drain_index_scan(slots, &scan, reactor, "gather", &expected, |b, _| {
            // The column slice is invariant across a frame's rows; derive it
            // once per frame rather than once per row. `ColumnLocator`'s own
            // readers cannot: each re-resolves the window through `get_col_ptr`.
            let col_data = b.col_data(slot, size);
            for j in 0..b.len() {
                if !gnitz_wire::null_word_get(b.get_null_word(j), slot) {
                    out.insert(
                        PkBuf::from_bytes(b.get_pk_bytes(j)),
                        payload_native_key(col_data, j * size, size, type_code),
                    );
                }
            }
            Ok(())
        })
        .await
        .map_err(|f| f.text)?;
        Ok(out)
    }
}

#[cfg(test)]
#[path = "tests/preflight.rs"]
mod tests;
