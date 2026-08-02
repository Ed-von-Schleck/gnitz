//! Distributed PK / FK / unique-index preflight validation and violation
//! formatting: the check types (`PipelinedCheck` / `CheckPayload`), the
//! pipelined executors (`execute_pipeline` / `execute_gather` + `GatherMap`
//! and the check-batch builders/pool), the gather/merge key streams
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
use crate::storage::MemBatch;

// ---------------------------------------------------------------------------
// Pipelined validation checks
// ---------------------------------------------------------------------------

/// How the check payload is routed to workers.
#[allow(clippy::large_enum_variant)]
pub(super) enum CheckPayload {
    /// Replicate the same batch to every worker; each worker filters
    /// its local partition.
    Broadcast(Batch),
    /// Pre-partitioned by the schema PK: source batch delivered via
    /// `scatter_wire_group` without materializing intermediate per-worker
    /// `Batch`es. `execute_pipeline` computes the per-worker routing
    /// itself from `check.schema.pk_indices()` via `with_worker_indices`.
    ScatterSource { source: Batch },
}

/// A single distributed has-pk check queued for pipelined execution
/// (always dispatched under FLAG_HAS_PK). `col_hint` is
/// `pack_pk_cols(&[col])` for an index check (the packed flag at bit 63 is
/// always set, so it never collides with the PK sentinel) or 0 for a PK check.
pub(super) struct PipelinedCheck {
    pub(super) target_id: i64,
    pub(super) col_hint: u64,
    pub(super) payload: Option<CheckPayload>,
    pub(super) schema: SchemaDescriptor,
}

/// `pk → projected committed values` result of `execute_gather`. Rows
/// live in one flat arena, `stride` values each, instead of one heap `Vec`
/// per row — a large UPDATE/DELETE validation gathers tens of thousands of
/// rows, and per-row allocations would dominate the merge.
#[derive(Default)]
pub(super) struct GatherMap {
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

/// Render a PK from its raw OPK byte form for error messages.
/// Compound PKs are formatted as comma-separated per-column values in
/// declaration order; `pk_bytes` holds all PK columns concatenated as OPK
/// (big-endian, sign-flipped for signed), so we slice and decode each column
/// back to native before rendering. Works for wide PKs (`pk_stride > 16`)
/// where a `u128` cannot encode the key.
pub(super) fn format_pk_value_bytes(pk_bytes: &[u8], schema: &SchemaDescriptor) -> String {
    let mut parts: Vec<String> = Vec::new();
    let mut off = 0usize;
    for &ci in schema.pk_indices() {
        let col = schema.columns[ci as usize];
        let size = col.size() as usize;
        // pk_bytes are OPK; decode each column back to native LE before reading
        // its scalar value. Reading OPK as native LE would render garbage for
        // signed columns (flipped sign bit) and any multi-byte unsigned column.
        let v = gnitz_wire::pk_native_key(pk_bytes, off, size, col.type_code);
        let s = match col.type_code {
            crate::schema::type_code::U128 => format!("{v}"),
            crate::schema::type_code::UUID => gnitz_wire::format_uuid(v),
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
/// between the `u128` and `PkBuf` key forms). Keys arrive as an iterator so a
/// caller that already holds them in a map or a tuple list need not materialize
/// a second vector to probe them.
fn build_check_batch_with<K>(
    schema: &SchemaDescriptor,
    keys: impl ExactSizeIterator<Item = K>,
    pooled: Option<Batch>,
    mut push_pk: impl FnMut(&mut Batch, &K),
) -> Batch {
    let npc = schema.num_payload_cols();
    let n = keys.len();
    let mut batch = match pooled {
        Some(b) if b.schema.as_ref() == Some(schema) => {
            let mut b = b;
            b.clear();
            b.reserve_rows(n);
            b
        }
        _ => Batch::with_capacity(*schema, n),
    };
    let null_word: u64 = crate::ops::all_payload_null_mask(npc);
    for key in keys {
        batch.ensure_row_capacity();
        push_pk(&mut batch, &key);
        batch.extend_weight(&1i64.to_le_bytes());
        batch.extend_null_bmp(&null_word.to_le_bytes());
        for (c, col) in schema.payload_columns() {
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
pub(super) fn build_check_batch(
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
    let (stride, idx_key_type) = opk_leading_key(schema);
    build_check_batch_with(schema, keys.iter(), pooled, |b, &&k| {
        let buf = crate::schema::key::index_opk_prefix(k, src_type, idx_key_type);
        b.extend_pk_bytes(buf.padded(stride));
    })
}

/// The `(stride, leading-key type)` an OPK leading-key span for `schema` encodes
/// at. The leading key column is the index's column 0 for an index schema, but
/// the PK column for a base-table schema (the FK parent fast-path passes the
/// parent base table, whose lone PK may be declared at any column position).
/// `pk_indices()[0]` resolves both: an index schema is laid out
/// `(promoted_c0, src_pk…)`, so `pk_indices()[0] == 0 == columns[0]`.
fn opk_leading_key(schema: &SchemaDescriptor) -> (usize, u8) {
    let stride = schema.pk_stride() as usize;
    let key_col = schema.pk_indices()[0] as usize;
    (stride, schema.columns[key_col].type_code)
}

/// Build the committed-PK-existence check batch from `keys`, the distinct PK
/// byte spans collected by the preflight aggregation. Writes each span's OPK
/// bytes verbatim into the PK region: the spans are already main-table PKs, so
/// unlike the index builder `build_check_batch` no column-0 re-encoding is
/// applied. The sole PK (rather than index) check-batch builder.
pub(super) fn build_check_batch_pk_bytes<'k>(
    schema: &SchemaDescriptor,
    keys: impl ExactSizeIterator<Item = &'k [u8]>,
    pooled: Option<Batch>,
) -> Batch {
    build_check_batch_with(schema, keys, pooled, |b, k| b.extend_pk_bytes(k))
}

/// Return `batch` to `disp.check_batch_pool[target_id]` and cap the pool depth.
pub(super) fn recycle_check_batch(disp: &mut MasterDispatcher, target_id: i64, batch: Batch) {
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
/// pool depth. Called after `execute_pipeline` to recycle allocations.
pub(super) fn reclaim_check_batches(disp: &mut MasterDispatcher, checks: &mut [PipelinedCheck]) {
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

/// The `(view, slot)` drop-order contract in one place: `mb` is a zero-copy
/// view into `slot`'s W2M ring bytes with its lifetime erased, so it is valid
/// only while `slot` is held and MUST drop first. Bundling the two here — view
/// field before slot field — makes whole-struct drop order correct by
/// construction, so the enclosing `PreflightKeyStream` can order its other
/// fields freely. `attach_frame` still clears `mb` before replacing `slot` to
/// cover the mid-life (non-drop) replacement.
#[derive(Default)]
struct FrameView {
    mb: Option<crate::storage::MemBatch<'static>>,
    slot: Option<W2mSlot>,
}

/// Per-worker state for one sorted-key stream in `merge_index_scan`. Nothing
/// reads `frame.mb` after its slot is released.
struct PreflightKeyStream {
    /// Worker index (error attribution) and scan request id (frame pulls).
    w: usize,
    req_id: u64,
    /// Current frame: a zero-copy key-batch view (`None` for an empty terminal
    /// frame or after a decode error) pinned by its backing ring slot.
    frame: FrameView,
    /// Cursor into `frame.mb`.
    row: usize,
    /// Current frame is non-terminal: status 0 and no FLAG_SCAN_LAST.
    has_more: bool,
}

impl PreflightKeyStream {
    fn new(w: usize, req_id: u64) -> Self {
        PreflightKeyStream {
            w,
            req_id,
            frame: FrameView::default(),
            row: 0,
            has_more: false,
        }
    }

    /// Install `slot` as the current frame, decoding its keys zero-copy.
    /// A fault/corrupt/undecodable frame is an immediate `Err` — the caller
    /// unwinds to the `ScanLease` drop, which discards the undrained trains.
    fn attach_frame(&mut self, slot: W2mSlot, frame_schema: &SchemaDescriptor) -> Result<(), String> {
        // The view must die before its backing slot.
        self.frame.mb = None;
        self.frame.slot = None;
        self.row = 0;
        let (ctrl, has_more) = parse_train_header(&slot, self.w, "unique pre-flight")?;
        self.has_more = has_more;
        // Every frame decodes against the shared compile-time wire schema
        // (version 0): the first frame's embedded schema block equals it by
        // construction (`send_unique_preflight_keys`), and continuation
        // frames carry no schema and resolve through the hint.
        let schema_hint = Some(SchemaWithVersion {
            descriptor: frame_schema,
            version: 0,
        });
        // SAFETY: the slice points into the W2M ring slot that `slot` pins
        // until dropped. `self.mb` borrows it, and every path that drops or
        // replaces `self.slot` clears `self.mb` first (top of this fn and
        // `next_key`'s terminal arm), so the view never outlives the pin. The
        // lifetime erasure exists only because slot and view live in the same
        // struct.
        let bytes: &'static [u8] = unsafe { std::mem::transmute::<&[u8], &'static [u8]>(slot.bytes()) };
        let zc = wire::decode_wire_ipc_zero_copy_with_ctrl(bytes, ctrl, schema_hint)
            .map_err(|e| scan_decode_err(self.w, e))?;
        self.frame.mb = zc.data_batch;
        self.frame.slot = Some(slot);
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
            if let Some(mb) = &self.frame.mb {
                if self.row < mb.count {
                    let key = PkBuf::from_bytes(mb.get_pk_bytes(self.row));
                    self.row += 1;
                    return Ok(Some(key));
                }
            }
            if !self.has_more {
                self.frame.mb = None;
                self.frame.slot = None;
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
        if self.duplicate {
            return false;
        }
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
        if !acc.offer(key) {
            break;
        } // first duplicate is conclusive
        if let Some(next) = streams[w].next_key(frame_schema, reactor).await? {
            heap.push(Reverse((next, w)));
        }
    }
    Ok(acc)
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
        // `pk_native_key` *is* "decode this OPK column window to its native
        // zero-extended u128" — the same rule the FK/index key space uses
        // everywhere else, so the decode is not respelled here.
        *native = pk_native_key(span.pk_bytes(), off, sz, col.type_code);
        off += sz;
    }
    natives
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
/// committer takes them by value and emits each as one `FLAG_PUSH` group.
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

/// One FK constraint, in the same field order however it was collected. The two
/// column positions are easy to transpose, so they are named rather than left
/// as a bare tuple.
#[derive(Clone, Copy)]
struct FkEdge {
    child_tid: i64,
    fk_col: usize,
    parent_tid: i64,
    parent_col: usize,
}

impl FkEdge {
    /// The `ParentDeltas` key this edge's referenced value lives under.
    fn delta_key(&self) -> (i64, usize) {
        (self.parent_tid, self.parent_col)
    }
}

/// Fold family `fi`'s rows into `overlay` — last op per PK wins (`w > 0` ⇒
/// `Inserted`, `w < 0` ⇒ `Deleted`, `w == 0` rows skipped). Applied over a
/// table's families in frame order it yields the post-transaction fold; applied
/// over a prefix of them it yields the state an Error family is checked against.
fn fold_family<'a>(overlay: &mut Overlay<'a>, fi: usize, b: &'a Batch) {
    for row in 0..b.count {
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
    let mut fold: FxHashMap<&[u8], PkFold> = FxHashMap::with_capacity_and_hasher(batch.count, Default::default());
    for i in 0..batch.count {
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

fn schema_of(disp_ptr: *mut MasterDispatcher, tid: i64) -> Result<SchemaDescriptor, String> {
    unsafe { (*(*disp_ptr).catalog).get_schema_desc(tid) }.ok_or_else(|| format!("no schema for table {tid}"))
}

/// The one spelling of "a row a child still references cannot be removed".
/// `verb` names what the parent write was doing: `"delete from"` when the row
/// goes away, `"update"` when the referenced value changes under it.
fn restrict_err(disp_ptr: *mut MasterDispatcher, parent_tid: i64, child_tid: i64, verb: &str) -> String {
    let (sn, tn, csn, ctn) = unsafe {
        let disp = &mut *disp_ptr;
        let (s, t) = disp.get_qualified_name_owned(parent_tid);
        let (cs, ct) = disp.get_qualified_name_owned(child_tid);
        (s, t, cs, ct)
    };
    format!("Foreign Key violation: cannot {verb} '{sn}.{tn}', row still referenced by '{csn}.{ctn}'")
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

/// The one spelling of "an inserted child row references a value the parent does
/// not hold".
fn fk_missing_err(disp_ptr: *mut MasterDispatcher, child_tid: i64, parent_tid: i64) -> String {
    let (sn, tn, tsn, ttn) = unsafe {
        let disp = &mut *disp_ptr;
        let (s, t) = disp.get_qualified_name_owned(child_tid);
        let (ts, tt) = disp.get_qualified_name_owned(parent_tid);
        (s, t, ts, tt)
    };
    format!("Foreign Key violation in '{sn}.{tn}': value not found in target '{tsn}.{ttn}'")
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
    overlays: FxHashMap<i64, Overlay<'a>>,
}

impl<'a> TxnBundle<'a> {
    fn new(disp_ptr: *mut MasterDispatcher, families: &'a [TxnFamily]) -> Result<Self, String> {
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
            schemas.insert(tid, schema_of(disp_ptr, tid)?);
            let mut overlay = Overlay::default();
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

    fn has(&self, tid: i64) -> bool {
        self.overlays.contains_key(&tid)
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

/// One planned FK probe: the committed-occupancy check is sent to `probe_tid` on
/// column `col` under `schema`/`src_type` for the given `values`, and a violation
/// names the counterpart table `report_tid`. Shared by the F1 existence check
/// (probe = parent, report = child) and the F2 RESTRICT check (probe = child,
/// report = parent).
struct FkProbePlan {
    edge: FkEdge,
    /// The side the occupancy probe goes to (F1: the parent; F2: the child) and
    /// the column probed on it.
    probe_tid: i64,
    col: usize,
    /// The counterpart a violation names (F1: the child; F2: the parent).
    report_tid: i64,
    schema: SchemaDescriptor,
    src_type: u8,
    values: Vec<u128>,
}

/// One planned unique-secondary-index check: the circuit's columns and index
/// schema, the key encoder, and the `(span, surviving holder PK)` claims to
/// verify against the committed occupancy the pipelined probe returns.
struct UniquePlan<'a> {
    tid: i64,
    col_indices: PkColList,
    idx_schema: SchemaDescriptor,
    spec: IndexKeySpec,
    stride: usize,
    /// The distinct surviving spans and who claims each. Holder PKs borrow the
    /// family batch's PK region, like the overlay keys.
    by_span: FxHashMap<PkBuf, &'a [u8]>,
    /// Upper bound on the committed spans this bundle can free on this table,
    /// or `None` when no bound was resolved.
    vacatable: Option<usize>,
}

/// Encode a native value `v` (from a column of type `src_type`) into the OPK
/// leading-key image of `schema`'s primary key — byte-identical to what
/// `build_check_batch` writes, so a membership test against the pipeline
/// found-set (which echoes matched probe keys) compares identical byte images.
fn enc_key(schema: &SchemaDescriptor, v: u128, src_type: u8) -> PkBuf {
    let (stride, idx_key_type) = opk_leading_key(schema);
    crate::schema::key::index_opk_prefix(v, src_type, idx_key_type).widened(stride)
}

impl MasterDispatcher {
    /// Fire one pipelined probe burst and recycle the check batches back to the
    /// pool. `execute_pipeline` already returns an empty result for an empty
    /// `checks`, so callers need no length guard. The paired reclaim is centralized
    /// here so no probe site can forget it (a leaked pooled batch).
    async fn execute_and_reclaim(
        disp_ptr: *mut MasterDispatcher,
        reactor: &crate::runtime::reactor::Reactor,
        sal_excl: &Rc<AsyncMutex>,
        checks: &mut [PipelinedCheck],
    ) -> Result<Vec<FxHashSet<PkBuf>>, String> {
        let results = Self::execute_pipeline(disp_ptr, reactor, sal_excl, checks).await?;
        unsafe { reclaim_check_batches(&mut *disp_ptr, checks) };
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
        disp_ptr: *mut MasterDispatcher,
        reactor: &crate::runtime::reactor::Reactor,
        sal_excl: &Rc<AsyncMutex>,
        families: &[TxnFamily],
    ) -> Result<(), String> {
        // No family whose write reads committed state ⇒ every rule below would
        // find nothing to check, so the bundle (an O(rows) fold) is not built.
        let reads_committed = unsafe {
            let cat = &*(*disp_ptr).catalog;
            families.iter().any(|f| cat.push_reads_committed_state(f.tid, f.mode))
        };
        if !reads_committed {
            return Ok(());
        }
        let bundle = TxnBundle::new(disp_ptr, families)?;
        let committed = Self::txn_check_pk(disp_ptr, reactor, sal_excl, &bundle).await?;
        Self::txn_check_unique_indices(disp_ptr, reactor, sal_excl, &bundle, &committed).await?;
        Self::txn_check_foreign_keys(disp_ptr, reactor, sal_excl, &bundle, &committed).await
    }

    /// Rule U-PK: Error-mode PK existence, cumulative in frame order. One
    /// committed-existence probe per probed table, all issued in one burst;
    /// then each table's families are walked in frame order and each Error
    /// family checked against the running prefix fold before being folded into
    /// it.
    ///
    /// Returns each probed table's committed PK set, which U-SEC reads: a
    /// touched PK that exists committed is the only thing that can vacate a
    /// committed index span, so the set bounds how many occupied spans the
    /// bundle can possibly free.
    async fn txn_check_pk(
        disp_ptr: *mut MasterDispatcher,
        reactor: &crate::runtime::reactor::Reactor,
        sal_excl: &Rc<AsyncMutex>,
        b: &TxnBundle<'_>,
    ) -> Result<FxHashMap<i64, FxHashSet<PkBuf>>, String> {
        let mut tids: Vec<i64> = Vec::new();
        let mut checks: Vec<PipelinedCheck> = Vec::new();
        for &tid in &b.order {
            let error_mode = b.families_of(tid).any(|f| matches!(f.mode, WireConflictMode::Error));
            // A unique index makes the probe worth issuing even without an Error
            // family: U-SEC reads the same answer, and one `ScatterSource` burst
            // replaces a committed-holder seek per colliding row.
            let has_unique = unsafe { (*(*disp_ptr).catalog).has_any_unique_index(tid) };
            if !error_mode && !has_unique {
                continue;
            }
            // Candidate PKs to probe committed, borrowed from the family batches'
            // PK regions. For U-SEC that is every touched PK — a deleted one
            // vacates its span just as an overwritten one does. For Error mode
            // alone it is the PKs an Error family inserts positively (a superset
            // of each family's net-positive set); with none, no Error family can
            // carry a net-positive PK, so the whole table's walk is vacuous.
            let keys: Vec<&[u8]> = if has_unique {
                b.overlay(tid).keys().copied().collect()
            } else {
                let mut candidate: FxHashSet<&[u8]> = FxHashSet::default();
                for fam in b.families_of(tid).filter(|f| matches!(f.mode, WireConflictMode::Error)) {
                    for row in 0..fam.batch.count {
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
            let pooled = unsafe { (*disp_ptr).pool_pop_batch(tid) };
            tids.push(tid);
            checks.push(PipelinedCheck {
                target_id: tid,
                col_hint: 0,
                payload: Some(CheckPayload::ScatterSource {
                    source: build_check_batch_pk_bytes(&schema, keys.into_iter(), pooled),
                }),
                schema,
            });
        }
        let mut results = Self::execute_and_reclaim(disp_ptr, reactor, sal_excl, &mut checks).await?;

        let mut committed_by_tid: FxHashMap<i64, FxHashSet<PkBuf>> = FxHashMap::default();
        for (i, &tid) in tids.iter().enumerate() {
            let committed = std::mem::take(&mut results[i]);
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
                        let key_str = || format_pk_value_bytes(pk, &schema);
                        if f.dups > 1 {
                            return Err(unsafe { (*disp_ptr).batch_dup_pk_err(tid, &schema, &key_str()) });
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
                            let (pk_names, sn, tn) = unsafe { (*disp_ptr).pk_violation_context(tid, &schema) };
                            return Err(format!(
                                "duplicate key value violates unique constraint \"{sn}_{tn}_pkey\": Key ({pk_names})=({}) already exists",
                                key_str(),
                            ));
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
    /// committed-occupancy probes issued in one burst; the per-span committed-
    /// holder seeks then fan out concurrently rather than one round trip each.
    ///
    /// Two things keep that fan-out off the hot path: a warm unique filter
    /// elides the whole plan for a provably-absent span set (the steady state of
    /// a fresh-key insert stream), and U-PK's committed-PK sets bound how many
    /// occupied spans the bundle could free.
    async fn txn_check_unique_indices<'a>(
        disp_ptr: *mut MasterDispatcher,
        reactor: &crate::runtime::reactor::Reactor,
        sal_excl: &Rc<AsyncMutex>,
        b: &TxnBundle<'a>,
        committed_pks: &FxHashMap<i64, FxHashSet<PkBuf>>,
    ) -> Result<(), String> {
        let mut plans: Vec<UniquePlan<'a>> = Vec::new();
        let mut checks: Vec<PipelinedCheck> = Vec::new();
        for &tid in &b.order {
            let (n_circuits, has_unique) = unsafe {
                let cat = &*(*disp_ptr).catalog;
                (cat.get_index_circuit_count(tid), cat.has_any_unique_index(tid))
            };
            if n_circuits == 0 || !has_unique {
                continue;
            }
            if b.surviving(tid).next().is_none() {
                continue;
            }
            // Warm the filters before planning: a provably-absent span set
            // elides the whole broadcast below. The warm-up is one O(table)
            // scan fan-out per (table, index) per process.
            Self::ensure_unique_filters_warm(disp_ptr, reactor, sal_excl, tid).await?;
            // Bound on the committed spans this bundle can free on this table:
            // only a touched PK that exists committed can vacate one, and it
            // vacates at most one per index. `None` when U-PK probed no committed
            // set for this table, leaving no bound. Resolved on the first circuit
            // that actually probes, so a fully elided table never walks the
            // overlay for it.
            let mut vacatable: Option<Option<usize>> = None;
            for ci in 0..n_circuits {
                // One circuit lookup: its column list, index schema, and the
                // span-encode plan baked at registration. Copied out so the
                // catalog borrow ends before the `&mut` dispatcher calls below.
                let (col_indices, idx_schema, spec) = unsafe {
                    let ic = &(*(*disp_ptr).catalog).index_circuits(tid)[ci];
                    if ic.unique_cols().is_none() {
                        continue;
                    }
                    (ic.col_indices, ic.index_schema, ic.key_spec)
                };
                let cols = col_indices.as_slice();
                let stride = idx_schema.pk_stride() as usize;

                // Surviving span → holder PK. `surviving` yields each PK once, so
                // a span already present always comes from a different row — an
                // in-bundle duplicate.
                let mut by_span: FxHashMap<PkBuf, &'a [u8]> = FxHashMap::default();
                let mut keybuf = PkBuf::zeroed(0);
                for (pk, fam, row) in b.surviving(tid) {
                    if !spec.key_bytes(b.mem(fam), row as usize, &mut keybuf) {
                        continue; // NULL in an indexed column ⇒ unindexed
                    }
                    if by_span.insert(keybuf, pk).is_some() {
                        return Err(unsafe { (*disp_ptr).unique_violation_err(tid, cols, true) });
                    }
                }
                if by_span.is_empty() {
                    continue;
                }

                // Every planned span provably absent from the committed index ⇒
                // the broadcast would answer "none occupied" and leave nothing to
                // verify. Skipping the whole plan is what keeps a fresh-key INSERT
                // stream a one-burst operation.
                let packed = gnitz_wire::pack_pk_cols(cols);
                if unsafe { (*disp_ptr).unique_filter_all_absent(tid, packed, by_span.keys().copied()) } {
                    continue;
                }
                let vacatable = *vacatable.get_or_insert_with(|| {
                    committed_pks
                        .get(&tid)
                        .map(|c| b.overlay(tid).keys().filter(|&&pk| c.contains(pk)).count())
                });
                let pooled = unsafe { (*disp_ptr).pool_pop_batch(tid) };
                let chk = build_check_batch_with(&idx_schema, by_span.keys(), pooled, |bat, k| {
                    bat.extend_pk_bytes(k.padded(stride))
                });
                checks.push(PipelinedCheck {
                    target_id: tid,
                    col_hint: packed,
                    payload: Some(CheckPayload::Broadcast(chk)),
                    schema: idx_schema,
                });
                plans.push(UniquePlan {
                    tid,
                    col_indices,
                    idx_schema,
                    spec,
                    stride,
                    by_span,
                    vacatable,
                });
            }
        }
        let results = Self::execute_and_reclaim(disp_ptr, reactor, sal_excl, &mut checks).await?;

        // Every occupied surviving span needs its committed holder. Fan the seeks
        // out across all plans before collecting, so they overlap on the wire
        // instead of costing one round trip each.
        let mut pending: Vec<(usize, PkBuf, &[u8])> = Vec::new();
        let mut futs = Vec::new();
        for (pi, plan) in plans.iter().enumerate() {
            let occupied = &results[pi];
            let idx_cols = &plan.idx_schema.columns[..plan.col_indices.as_slice().len()];
            let mut hits = 0usize;
            for (span, holder_pk) in &plan.by_span {
                if !occupied.contains(span.padded(plan.stride)) {
                    continue;
                }
                // Each occupied span needs a distinct committed row to vacate it,
                // and only a touched-and-committed PK can. More occupied spans
                // than that means one of them keeps its holder — a violation,
                // decided without seeking any of them. This is what stops a bulk
                // load of colliding fresh rows from costing one seek per row.
                hits += 1;
                if plan.vacatable.is_some_and(|v| hits > v) {
                    return Err(unsafe {
                        (*disp_ptr).unique_violation_err(plan.tid, plan.col_indices.as_slice(), false)
                    });
                }
                pending.push((pi, *span, holder_pk));
                // async fn futures are !Unpin; box-pin for join_all_unpin.
                futs.push(Box::pin(Self::seek_unique_holder(
                    disp_ptr,
                    reactor,
                    sal_excl,
                    plan.tid,
                    plan.col_indices,
                    span_to_natives(span, idx_cols),
                )));
            }
        }
        let holders = crate::runtime::reactor::join_all_unpin(futs).await;

        // A committed holder is acceptable only if the bundle retires it: either
        // it is the surviving row that claims the span, or its own surviving state
        // is absent / no longer holds that span.
        for ((pi, span, holder_pk), holder_result) in pending.into_iter().zip(holders) {
            let Some(h) = holder_result? else { continue };
            if h.pk_bytes() == holder_pk {
                continue;
            }
            let plan = &plans[pi];
            let retired = match b.overlay(plan.tid).get(h.pk_bytes()) {
                None => false,
                Some(FoldOp::Deleted) => true,
                Some(FoldOp::Inserted(hf, hr)) => {
                    let mut hspan = PkBuf::zeroed(0);
                    !plan.spec.key_bytes(b.mem(*hf), *hr as usize, &mut hspan) || hspan != span
                }
            };
            if !retired {
                return Err(unsafe { (*disp_ptr).unique_violation_err(plan.tid, plan.col_indices.as_slice(), false) });
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
        disp_ptr: *mut MasterDispatcher,
        reactor: &crate::runtime::reactor::Reactor,
        sal_excl: &Rc<AsyncMutex>,
        b: &TxnBundle<'_>,
        committed_pks: &FxHashMap<i64, FxHashSet<PkBuf>>,
    ) -> Result<(), String> {
        // Every FK whose child is bundled (F1 checks those rows' values exist),
        // and every FK whose parent is bundled (F2 checks the values it removes
        // are unreferenced).
        let mut constraints: Vec<FkEdge> = Vec::new();
        let mut children: Vec<FkEdge> = Vec::new();
        for &tid in &b.order {
            unsafe {
                let cat = &*(*disp_ptr).catalog;
                constraints.extend(cat.fk_constraints_of(tid).iter().map(|c| FkEdge {
                    child_tid: tid,
                    fk_col: c.fk_col_idx,
                    parent_tid: c.target_table_id,
                    parent_col: c.target_col_idx,
                }));
                children.extend(cat.fk_children_of(tid).iter().map(|r| FkEdge {
                    child_tid: r.child_tid,
                    fk_col: r.fk_col_idx,
                    parent_tid: tid,
                    parent_col: r.parent_col_idx,
                }));
            }
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
            .map(|e| e.delta_key())
            .collect();
        needed.sort_unstable();
        needed.dedup();
        // Fan the per-parent gathers out concurrently — they run under the full
        // lock union, so overlapping their reply waits (as the U-SEC holder seeks
        // and F2 exemption fetches already do) beats one sequential round trip
        // per parent column.
        let futs: Vec<_> = needed
            .iter()
            .map(|&(ptid, pcol)| {
                Box::pin(Self::parent_retired_added(
                    disp_ptr,
                    reactor,
                    sal_excl,
                    b,
                    committed_pks,
                    ptid,
                    pcol,
                ))
            })
            .collect();
        let resolved = crate::runtime::reactor::join_all_unpin(futs).await;
        let mut deltas: ParentDeltas = ParentDeltas::default();
        for (&(ptid, pcol), d) in needed.iter().zip(resolved) {
            deltas.insert((ptid, pcol), d?);
        }

        Self::txn_check_fk_existence(disp_ptr, reactor, sal_excl, b, &constraints, &deltas).await?;
        Self::txn_check_fk_restrict(disp_ptr, reactor, sal_excl, b, &children, &deltas).await
    }

    /// Rule F1: every surviving row's FK value must reference a row that exists
    /// after the transaction — present in committed state and not retired by the
    /// bundle, or added by it.
    async fn txn_check_fk_existence(
        disp_ptr: *mut MasterDispatcher,
        reactor: &crate::runtime::reactor::Reactor,
        sal_excl: &Rc<AsyncMutex>,
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
                if loc.is_null(mb, row as usize) {
                    continue;
                }
                let v = loc.native_key(mb, row as usize);
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
            let parent_schema = schema_of(disp_ptr, parent_tid)?;
            let ppk = parent_schema.pk_indices();
            let src_type = loc.type_code();
            let (probe_schema, col_hint, broadcast) = if ppk.len() == 1 && ppk[0] as usize == parent_col {
                (parent_schema, 0u64, false)
            } else {
                let idx_schema =
                    unsafe { (*(*disp_ptr).catalog).get_index_schema_by_cols(parent_tid, &[parent_col as u32]) }
                        .ok_or_else(|| format!("FK check: no unique index on parent {parent_tid} col {parent_col}"))?;
                (idx_schema, gnitz_wire::pack_pk_cols(&[parent_col as u32]), true)
            };
            let pooled = unsafe { (*disp_ptr).pool_pop_batch(parent_tid) };
            let chk = build_check_batch(&probe_schema, &values, src_type, pooled);
            checks.push(PipelinedCheck {
                target_id: parent_tid,
                col_hint,
                payload: Some(if broadcast {
                    CheckPayload::Broadcast(chk)
                } else {
                    CheckPayload::ScatterSource { source: chk }
                }),
                schema: probe_schema,
            });
            plans.push(FkProbePlan {
                edge,
                probe_tid: parent_tid,
                col: parent_col,
                report_tid: tid,
                schema: probe_schema,
                src_type,
                values,
            });
        }
        let results = Self::execute_and_reclaim(disp_ptr, reactor, sal_excl, &mut checks).await?;

        // A non-bundled parent has no delta (the degenerate plain-push case).
        let no_delta: ParentDelta = (FxHashMap::default(), FxHashSet::default());
        for (i, plan) in plans.iter().enumerate() {
            let (retired, added) = deltas.get(&plan.edge.delta_key()).unwrap_or(&no_delta);
            for v in &plan.values {
                let in_committed = results[i].contains(&enc_key(&plan.schema, *v, plan.src_type));
                if (in_committed && !retired.contains_key(v)) || added.contains(v) {
                    continue;
                }
                return Err(fk_missing_err(disp_ptr, plan.report_tid, plan.probe_tid));
            }
        }
        Ok(())
    }

    /// Rule F2: a referenced value the bundle removes and does not re-add must
    /// have no surviving child row referencing it. `exists_after(v)` for
    /// `v ∈ retired` reduces to `added.contains(v)` (the committed term is masked
    /// by `v ∈ retired`), so the checked set is `retired ∖ added`.
    async fn txn_check_fk_restrict(
        disp_ptr: *mut MasterDispatcher,
        reactor: &crate::runtime::reactor::Reactor,
        sal_excl: &Rc<AsyncMutex>,
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
            let (retired, added) = &deltas[&edge.delta_key()];
            let v_check: Vec<u128> = retired.keys().copied().filter(|v| !added.contains(v)).collect();
            if v_check.is_empty() {
                continue;
            }
            let idx_schema = unsafe { (*(*disp_ptr).catalog).get_index_schema_by_cols(child_tid, &[fk_col as u32]) }
                .ok_or_else(|| format!("FK RESTRICT: no index on child {child_tid} col {fk_col}"))?;
            let src_type = b.schema(parent_tid).columns[parent_col].type_code;
            let pooled = unsafe { (*disp_ptr).pool_pop_batch(child_tid) };
            let chk = build_check_batch(&idx_schema, &v_check, src_type, pooled);
            checks.push(PipelinedCheck {
                target_id: child_tid,
                col_hint: gnitz_wire::pack_pk_cols(&[fk_col as u32]),
                payload: Some(CheckPayload::Broadcast(chk)),
                schema: idx_schema,
            });
            plans.push(FkProbePlan {
                edge,
                probe_tid: child_tid,
                col: fk_col,
                report_tid: parent_tid,
                schema: idx_schema,
                src_type,
                values: v_check,
            });
        }
        let results = Self::execute_and_reclaim(disp_ptr, reactor, sal_excl, &mut checks).await?;

        // A committed child reference is fatal unless the bundle also touches the
        // child table and every referencing child row is retired or re-pointed —
        // which needs the child rows themselves. Collect those fetches first, then
        // fan them out.
        let mut fetches: Vec<(usize, u128)> = Vec::new();
        for (i, plan) in plans.iter().enumerate() {
            for v in &plan.values {
                if !results[i].contains(&enc_key(&plan.schema, *v, plan.src_type)) {
                    continue; // no committed children reference v
                }
                if !b.has(plan.probe_tid) {
                    // Untouched committed children exist and no bundled child
                    // family can exempt them.
                    let verb = restrict_verb(&deltas[&plan.edge.delta_key()].0, *v);
                    return Err(restrict_err(disp_ptr, plan.report_tid, plan.probe_tid, verb));
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
                    disp_ptr,
                    reactor,
                    sal_excl,
                    plan.probe_tid,
                    gnitz_wire::pack_pk_cols(&[plan.col as u32]),
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
            let child_loc = b.schema(plan.probe_tid).locate(plan.col);
            let child_overlay = b.overlay(plan.probe_tid);
            for j in 0..rows.count {
                let still_refs = match child_overlay.get(rows.get_pk_bytes(j)) {
                    None => true, // untouched committed child still references v
                    Some(FoldOp::Deleted) => false,
                    Some(FoldOp::Inserted(cf, cr)) => {
                        let smb = b.mem(*cf);
                        !child_loc.is_null(smb, *cr as usize) && child_loc.native_key(smb, *cr as usize) == v
                    }
                };
                if still_refs {
                    let verb = restrict_verb(&deltas[&plan.edge.delta_key()].0, v);
                    return Err(restrict_err(disp_ptr, plan.report_tid, plan.probe_tid, verb));
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
        disp_ptr: *mut MasterDispatcher,
        reactor: &crate::runtime::reactor::Reactor,
        sal_excl: &Rc<AsyncMutex>,
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
                let gathered =
                    Self::execute_gather(disp_ptr, reactor, sal_excl, parent_tid, pks, &[ref_col as u8]).await?;
                for p in touched() {
                    if let Some(Some(v)) = gathered.get(p).map(|row| row[0]) {
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
                    (!loc.is_null(mb, *r as usize)).then(|| loc.native_key(mb, *r as usize))
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
    pub async fn validate_unique_index_create(
        disp_ptr: *mut MasterDispatcher,
        reactor: &crate::runtime::reactor::Reactor,
        sal_excl: &Rc<AsyncMutex>,
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
            // needed. The seed is empty rather than the committed value set,
            // which stays sound because the index key IS the PK here: a span
            // collision would be a PK collision, and `enforce_unique_pk` already
            // makes those impossible.
            if owner_schema.group_cols_eq_pk(col_indices) {
                return Ok((FxHashSet::default(), false));
            }
            // Build the index schema (the circuit is not registered until this
            // pre-flight succeeds) for the merge's reply-frame layout and the
            // promoted per-column widths. Identical inputs to each worker's own
            // build, so the frame schema agrees by construction. `packed` is
            // the column list the worker resolves the seek by.
            (
                crate::schema::make_index_schema(col_indices, &owner_schema)?,
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
        let unicast = replicated_unicast(disp_ptr, owner_id);

        // Fan out the pre-flight command (the packed column list rides in
        // seek_col_idx); each worker answers with its sorted-span
        // continuation-frame train. `_lease` held to end of scope: when the
        // merge returns early (error or duplicate verdict) the lease drop
        // discards the undrained trains at the ring boundary.
        let (slots, req_ids, _lease) =
            dispatch_scan_fanout(disp_ptr, reactor, sal_excl, unicast, |disp, req_ids, unicast| {
                // The worker's `UniquePreflight` arm resolves the owner's schema
                // from its own catalog.
                disp.write_command_group(
                    owner_id,
                    0,
                    FLAG_UNIQUE_PREFLIGHT,
                    0,
                    0,
                    packed,
                    req_ids,
                    unicast,
                    0,
                    &[],
                )
            })
            .await?;

        let merged = merge_index_scan(slots, &req_ids, reactor, &frame_schema).await?;
        if merged.duplicate {
            return Err(unsafe { (*disp_ptr).unique_create_dup_err(owner_id, col_indices) });
        }
        Ok(merged.into_seed())
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
    fn pk_violation_context(&mut self, target_id: i64, schema: &SchemaDescriptor) -> (String, String, String) {
        let names: Vec<String> = schema
            .pk_indices()
            .iter()
            .map(|&ci| self.get_col_name(target_id, ci as usize))
            .collect();
        let (sn, tn) = self.get_qualified_name_owned(target_id);
        (names.join(", "), sn, tn)
    }

    /// Format the "two rows in one batch share a PK" rejection message.
    /// `key_str` is the already-rendered offending key (narrow or wide).
    fn batch_dup_pk_err(&mut self, target_id: i64, schema: &SchemaDescriptor, key_str: &str) -> String {
        let (pk_names, sn, tn) = self.pk_violation_context(target_id, schema);
        format!(
            "duplicate key value violates unique constraint \"{sn}_{tn}_pkey\": Batch contains multiple rows with key ({pk_names})=({key_str})",
        )
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
    /// in a single poll loop — cursor position on the W2M ring correlates each
    /// response with its originating check, so no explicit correlation ID is
    /// needed. Each rule therefore plans every probe it needs before issuing
    /// any.
    ///
    /// `sal_excl` is held only for the synchronous write + signal phase;
    /// see `dispatch_scan_fanout` for the rationale.
    pub(super) async fn execute_pipeline(
        disp_ptr: *mut MasterDispatcher,
        reactor: &crate::runtime::reactor::Reactor,
        sal_excl: &Rc<AsyncMutex>,
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
                    let req_slice = &rids[idx * nw..(idx + 1) * nw];
                    match check.payload.as_ref().expect("payload consumed") {
                        CheckPayload::Broadcast(batch) => {
                            let refs: Vec<Option<&Batch>> = (0..nw).map(|_| Some(batch)).collect();
                            // `check.schema` is not derivable from the target id:
                            // for a unique secondary index it is the INDEX
                            // table's schema `(indexed_col, src_pk…)` sent under
                            // the owner table's id, and `handle_has_pk` reads it
                            // back to size `idx_key_size`. A catalog lookup on
                            // the target id would give the owner's prefix width.
                            disp.write_data_group(
                                check.target_id,
                                FLAG_HAS_PK,
                                &refs,
                                &check.schema,
                                0,
                                check.col_hint,
                                req_slice,
                            )?;
                        }
                        CheckPayload::ScatterSource { source } => {
                            // Routing is always by the schema PK; compute
                            // per-worker indices on the fly. No reentrancy: this
                            // loop body has no `.await`, so the SCATTER_INDICES
                            // borrow is released before the next iteration.
                            with_worker_indices(source, &check.schema, nw, |worker_indices| {
                                disp.sal.scatter_wire_group(
                                    source,
                                    worker_indices,
                                    &check.schema,
                                    check.target_id as u32,
                                    0,
                                    FLAG_HAS_PK,
                                    0,
                                    check.col_hint,
                                    req_slice,
                                    None,
                                    None,
                                )
                            })?;
                        }
                    }
                }
                disp.signal_all();
                (nw, rids)
            }
        };

        let decoded_vec: Vec<DecodedWire> =
            crate::runtime::reactor::join_all_unpin(all_req_ids.iter().map(|&id| reactor.await_reply(id))).await;

        let mut results: Vec<FxHashSet<PkBuf>> = checks
            .iter()
            .map(|check| {
                let cap = match check.payload.as_ref().expect("payload consumed") {
                    CheckPayload::Broadcast(b) => b.count,
                    CheckPayload::ScatterSource { source } => source.count,
                };
                FxHashSet::with_capacity_and_hasher(cap, Default::default())
            })
            .collect();

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
    /// targets. It is a sibling of `execute_pipeline` (which returns only
    /// existence) rather than a modification of it: the has-pk pipeline echoes
    /// the caller's payload (`filter_by_pk`), so it structurally cannot return
    /// a stored column the caller does not already hold.
    ///
    /// Replies arrive as reply trains (an oversized gather reply chunks; a
    /// single-frame reply is a length-1 train), so the fan-out uses scan
    /// request ids and the train drain. The expected projected schema guards
    /// each train's first frame — a worker whose catalog lags a DDL would
    /// otherwise hand back rows the master mis-decodes.
    pub(super) async fn execute_gather(
        disp_ptr: *mut MasterDispatcher,
        reactor: &crate::runtime::reactor::Reactor,
        sal_excl: &Rc<AsyncMutex>,
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
        let col_mask = pack_gather_cols(project).ok_or("gather: more than 8 projected columns")?;

        let parent_schema = unsafe {
            (&*(*disp_ptr).catalog)
                .get_schema_desc(target_id)
                .ok_or_else(|| format!("gather: no schema for table {target_id}"))?
        };
        // The exact constructor the worker uses for its reply schema, so a
        // matching reply validates by construction.
        let expected = crate::schema::project_schema(&parent_schema, project);

        // `_lease` held across the full drain below (see `dispatch_scan_fanout`).
        let (slots, req_ids, _lease) = dispatch_scan_fanout(disp_ptr, reactor, sal_excl, -1, |disp, rids, _unicast| {
            let nw = disp.num_workers;
            let pooled = disp.pool_pop_batch(target_id);
            let batch = build_check_batch_pk_bytes(&parent_schema, pks.iter().map(|p| p.pk_bytes()), pooled);
            with_worker_indices(&batch, &parent_schema, nw, |worker_indices| {
                disp.sal.scatter_wire_group(
                    &batch,
                    worker_indices,
                    &parent_schema,
                    target_id as u32,
                    0,
                    FLAG_GATHER,
                    /* wire_flags */ 0,
                    /* seek_col_idx */ col_mask,
                    rids,
                    None,
                    None,
                )
            })?;
            // The scatter batch is fully consumed by the synchronous
            // scatter_wire_group above; return it to the pool.
            recycle_check_batch(disp, target_id, batch);
            Ok(())
        })
        .await?;

        // Precompute (type_code, col_size) per projected column from the parent
        // schema; the reply's projected payload index k corresponds to project[k].
        let proj_meta: Vec<(u8, usize)> = project
            .iter()
            .map(|&p| {
                let col = parent_schema.columns[p as usize];
                (col.type_code, col.size() as usize)
            })
            .collect();

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
                        if gnitz_wire::null_word_get(null_word, k) {
                            None
                        } else {
                            Some(payload_native_key(col_slices[k], j * col_size, col_size, col_type))
                        }
                    }),
                );
            }
            Ok(())
        })
        .await?;
        Ok(out)
    }
}
