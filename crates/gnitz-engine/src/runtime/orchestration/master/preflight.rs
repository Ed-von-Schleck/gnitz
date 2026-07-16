//! Distributed PK / FK / unique-index preflight validation and violation
//! formatting: the check types (`PipelinedCheck` / `CheckPayload` /
//! `P1Label` / `P2Label`), the pipelined executors
//! (`execute_pipeline_async` / `execute_gather_async` + `GatherMap` and the
//! check-batch builders/pool), the `validate_*` pipelines, the gather/merge
//! key streams (`PreflightKeyStream` / `PreflightAccumulator` /
//! `merge_index_scan`), and the error renderers.

use super::*;

use super::unique_filter::{UniqueFilter, UNIQUE_FILTER_CAP};

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
    /// `Batch`es. `execute_pipeline_async` computes the per-worker routing
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

/// Side table for interpreting Phase 1 results in `validate_all_distributed`.
/// Each label lines up positionally with a `PipelinedCheck` submitted to
/// the Phase 1 pipeline.
pub(super) enum P1Label {
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
pub(super) enum P2Label {
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

/// `pk → projected committed values` result of `execute_gather_async`. Rows
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
        let mut le = [0u8; 16];
        gnitz_wire::decode_pk_column(&pk_bytes[off..off + size], col.type_code, &mut le[..size]);
        let v = u128::from_le_bytes(le);
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
    build_check_batch_with(schema, keys, pooled, |b, &k| {
        let buf = crate::schema::index_opk_prefix(k, src_type, idx_key_type);
        b.extend_pk_bytes(&buf[..stride]);
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

/// Build the UPSERT PK-identification check batch from `keys`, the distinct
/// net-positive PK byte spans collected by the preflight aggregation. Writes
/// each `PkBuf`'s OPK bytes verbatim into the PK region: the spans are already
/// main-table PKs, so unlike the index builder `build_check_batch` no column-0
/// re-encoding is applied. The sole PK (rather than index) check-batch builder.
pub(super) fn build_check_batch_pkbuf(schema: &SchemaDescriptor, keys: &[PkBuf], pooled: Option<Batch>) -> Batch {
    build_check_batch_with(schema, keys, pooled, |b, k| b.extend_pk_bytes(k.pk_bytes()))
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
/// pool depth. Called after `execute_pipeline_async` to recycle allocations.
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
        let le = gnitz_wire::decode_pk_column_owned(&span.pk_bytes()[off..off + sz], col.type_code);
        *native = u128::from_le_bytes(le);
        off += sz;
    }
    natives
}

/// Collect the distinct non-PK parent columns referenced by `target_id`'s FK
/// children, as a projection list for `execute_gather_async`. A PK-target child
/// reads its referenced value from the packed PK on the wire and needs no
/// gather, so PK columns are excluded.
fn collect_fk_projection(cat: &CatalogEngine, target_id: i64, source_schema: &SchemaDescriptor) -> Vec<u8> {
    let mut project: Vec<u8> = Vec::new();
    for r in cat.fk_children_of(target_id) {
        if !source_schema.is_pk_col(r.parent_col_idx) && !project.contains(&(r.parent_col_idx as u8)) {
            project.push(r.parent_col_idx as u8);
        }
    }
    project
}

/// Cap on distinct RESTRICT exemption-fetch values per transaction (Rule F2
/// step 3). Beyond it the transaction is rejected, keeping a delete-heavy bundle
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

/// One table's fold: the last op per PK.
type Overlay = FxHashMap<PkBuf, FoldOp>;

/// The `(retired, added)` referenced-value sets of one bundled FK parent column
/// (see `parent_retired_added`).
type ParentDelta = (FxHashSet<u128>, FxHashSet<u128>);

/// `(parent tid, referenced col)` → its delta. Resolved once per key and shared
/// by rules F1 and F2, which both turn on it.
type ParentDeltas = FxHashMap<(i64, usize), ParentDelta>;

/// Fold family `fi`'s rows into `overlay` — last op per PK wins (`w > 0` ⇒
/// `Inserted`, `w < 0` ⇒ `Deleted`, `w == 0` rows skipped). Applied over a
/// table's families in frame order it yields the post-transaction fold; applied
/// over a prefix of them it yields the state an Error family is checked against.
fn fold_family(overlay: &mut Overlay, fi: usize, b: &Batch) {
    for row in 0..b.count {
        let w = b.get_weight(row);
        if w == 0 {
            continue;
        }
        overlay.insert(
            PkBuf::from_bytes(b.get_pk_bytes(row)),
            if w > 0 {
                FoldOp::Inserted(fi as u32, row as u32)
            } else {
                FoldOp::Deleted
            },
        );
    }
}

/// Per-PK `(net weight, positive-occurrence count)` over one batch, keyed by the
/// row's borrowed OPK bytes (zero-allocation, width-universal, sized to
/// `batch.count`). A `+w` row counts as `w` insertions of the PK — Error-mode
/// duplicate rejection treats it like the `w` separate `+1` rows it encodes — so
/// the occurrence count saturates at 2 (any value `> 1` is a within-batch
/// duplicate). Shared by the plain-push validator and the transaction PK check;
/// the borrowed keys never outlive the caller's synchronous prelude.
fn pk_net_weight_and_dup_count(batch: &Batch) -> FxHashMap<&[u8], (i64, u32)> {
    let mut net: FxHashMap<&[u8], (i64, u32)> = FxHashMap::with_capacity_and_hasher(batch.count, Default::default());
    for i in 0..batch.count {
        let w = batch.get_weight(i);
        if w == 0 {
            continue;
        }
        let e = net.entry(batch.get_pk_bytes(i)).or_insert((0, 0));
        e.0 += w;
        if w > 0 {
            e.1 += if w > 1 { 2 } else { 1 };
        }
    }
    net
}

fn schema_of(disp_ptr: *mut MasterDispatcher, tid: i64) -> Result<SchemaDescriptor, String> {
    unsafe { (*(*disp_ptr).catalog).get_schema_desc(tid) }.ok_or_else(|| format!("TXN: no schema for table {tid}"))
}

fn restrict_err(disp_ptr: *mut MasterDispatcher, parent_tid: i64, child_tid: i64) -> String {
    let (sn, tn, csn, ctn) = unsafe {
        let disp = &mut *disp_ptr;
        let (s, t) = disp.get_qualified_name_owned(parent_tid);
        let (cs, ct) = disp.get_qualified_name_owned(child_tid);
        (s, t, cs, ct)
    };
    format!("Foreign Key violation: cannot delete from '{sn}.{tn}', row still referenced by '{csn}.{ctn}'")
}

/// A decoded transaction bundle plus everything its rules share: the per-table
/// family lists in frame order, each table's catalog schema, and the
/// whole-bundle fold. Built once; every rule reads it instead of re-walking the
/// families.
struct TxnBundle<'a> {
    families: &'a [TxnFamily],
    /// The bundle's tids, in first-appearance order.
    order: Vec<i64>,
    /// tid → indices into `families`, in frame order.
    by_tid: FxHashMap<i64, Vec<usize>>,
    schemas: FxHashMap<i64, SchemaDescriptor>,
    overlays: FxHashMap<i64, Overlay>,
    /// tid → the rows that survive the whole bundle (`(pk, family, row)`), the
    /// `Inserted` projection of `overlays`. Precomputed once so the FK and
    /// unique-index rules — F1 walks it per constraint — never rebuild it.
    surviving: FxHashMap<i64, Vec<(PkBuf, u32, u32)>>,
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
        let mut surviving = FxHashMap::default();
        for &tid in &order {
            schemas.insert(tid, schema_of(disp_ptr, tid)?);
            let mut overlay = Overlay::default();
            for &fi in &by_tid[&tid] {
                fold_family(&mut overlay, fi, &families[fi].batch);
            }
            let survivors: Vec<(PkBuf, u32, u32)> = overlay
                .iter()
                .filter_map(|(pk, op)| match op {
                    FoldOp::Inserted(f, r) => Some((*pk, *f, *r)),
                    FoldOp::Deleted => None,
                })
                .collect();
            overlays.insert(tid, overlay);
            surviving.insert(tid, survivors);
        }
        Ok(TxnBundle {
            families,
            order,
            by_tid,
            schemas,
            overlays,
            surviving,
        })
    }

    fn has(&self, tid: i64) -> bool {
        self.overlays.contains_key(&tid)
    }

    fn schema(&self, tid: i64) -> &SchemaDescriptor {
        &self.schemas[&tid]
    }

    fn overlay(&self, tid: i64) -> &Overlay {
        &self.overlays[&tid]
    }

    fn batch(&self, fam: u32) -> &Batch {
        &self.families[fam as usize].batch
    }

    fn family_indices(&self, tid: i64) -> &[usize] {
        &self.by_tid[&tid]
    }

    fn families_of(&self, tid: i64) -> impl Iterator<Item = &TxnFamily> {
        self.by_tid[&tid].iter().map(|&fi| &self.families[fi])
    }

    /// The rows of `tid` that survive the whole bundle: `(pk, family, row)`.
    /// Precomputed in `new`.
    fn surviving(&self, tid: i64) -> &[(PkBuf, u32, u32)] {
        &self.surviving[&tid]
    }
}

/// One planned FK probe: the committed-occupancy check is sent to `probe_tid` on
/// column `col` under `schema`/`src_type` for the given `values`, and a violation
/// names the counterpart table `report_tid`. Shared by the F1 existence check
/// (probe = parent, report = child) and the F2 RESTRICT check (probe = child,
/// report = parent).
struct FkProbePlan {
    probe_tid: i64,
    report_tid: i64,
    col: usize,
    schema: SchemaDescriptor,
    src_type: u8,
    values: Vec<u128>,
}

/// One planned unique-secondary-index check: the circuit's columns and index
/// schema, the key encoder, and the `(span, surviving holder PK)` claims to
/// verify against the committed occupancy the pipelined probe returns.
struct UniquePlan {
    tid: i64,
    col_indices: PkColList,
    idx_schema: SchemaDescriptor,
    spec: IndexKeySpec,
    stride: usize,
    span_holders: Vec<(PkBuf, PkBuf)>,
}

/// Encode a native value `v` (from a column of type `src_type`) into the OPK
/// leading-key image of `schema`'s primary key — byte-identical to what
/// `build_check_batch` writes, so a membership test against the pipeline
/// found-set (which echoes matched probe keys) compares identical byte images.
fn enc_key(schema: &SchemaDescriptor, v: u128, src_type: u8) -> PkBuf {
    let (stride, idx_key_type) = opk_leading_key(schema);
    PkBuf::from_bytes(&crate::schema::index_opk_prefix(v, src_type, idx_key_type)[..stride])
}

impl MasterDispatcher {
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
        target_id: i64,
        batch: &Batch,
        mode: WireConflictMode,
    ) -> Result<(), String> {
        let (n_fk, n_children, n_circuits, has_unique, unique_pk, source_schema) = unsafe {
            let disp = &mut *disp_ptr;
            let cat = &mut *disp.catalog;
            let n_fk = cat.fk_constraints_of(target_id).len();
            let n_children = cat.fk_children_of(target_id).len();
            let n_circuits = cat.get_index_circuit_count(target_id);
            let has_unique = cat.has_any_unique_index(target_id);
            let unique_pk = cat.table_has_unique_pk(target_id);
            let source_schema = cat
                .get_schema_desc(target_id)
                .ok_or_else(|| format!("validate_all_distributed: no schema for table {target_id}"))?;
            (n_fk, n_children, n_circuits, has_unique, unique_pk, source_schema)
        };

        let needs_pk_rejection = matches!(mode, WireConflictMode::Error) && unique_pk;

        if n_fk == 0 && n_children == 0 && !has_unique && !needs_pk_rejection {
            return Ok(());
        }

        // Borrowed view for the per-column locator reads (FK insert / unique
        // enforcement); zero-allocation over `batch`'s pages.
        let mb = batch.as_mem_batch();

        // Net-positive PK byte spans feed the UPSERT PK-identification check
        // below; the borrowed keys never escape this synchronous prelude.
        let mut pk_keys: Vec<PkBuf> = Vec::new();
        if has_unique || needs_pk_rejection {
            let net = pk_net_weight_and_dup_count(batch);
            if needs_pk_rejection {
                for (&pk, &(_, pos_count)) in net.iter() {
                    if pos_count > 1 {
                        let key_str = format_pk_value_bytes(pk, &source_schema);
                        return Err(unsafe { (*disp_ptr).batch_dup_pk_err(target_id, &source_schema, &key_str) });
                    }
                }
            }
            pk_keys = net
                .iter()
                .filter(|(_, &(nw, _))| nw > 0)
                .map(|(&pk, _)| PkBuf::from_bytes(pk))
                .collect();
        }

        // ----- Phase 1 plan -----------------------------------------------
        let mut p1_checks: Vec<PipelinedCheck> = Vec::new();
        let mut p1_labels: Vec<P1Label> = Vec::new();

        // SAFETY (typed FK slices, here and in the two child loops below): the
        // slice borrows the catalog's FK cache across this planning code and
        // the awaits around it. That is sound not by borrowck but because the
        // push read lock is held across the whole preflight (executor.rs), so
        // no DDL can mutate the FK caches while the borrow is live — the same
        // contract every raw `disp_ptr` deref here relies on.
        for c in unsafe { (*(*disp_ptr).catalog).fk_constraints_of(target_id) } {
            let (fk_col_idx, parent_table_id, parent_col_idx) = (c.fk_col_idx, c.target_table_id, c.target_col_idx);
            let parent_schema = unsafe { (*(*disp_ptr).catalog).get_schema_desc(parent_table_id) }
                .ok_or_else(|| format!("FK parent table {parent_table_id} schema not found"))?;

            // The FK column may itself be a PK column of the child table (e.g.
            // `id BIGINT PRIMARY KEY REFERENCES parent(pid)`); `locate` resolves
            // the PK-or-payload read once, so a PK FK column reads from the
            // packed PK region instead of a (nonexistent) payload slot.
            let loc = source_schema.locate(fk_col_idx);

            let mut seen: FxHashSet<u128> = FxHashSet::default();
            let mut keys: Vec<u128> = Vec::new();

            for i in 0..batch.count {
                if batch.get_weight(i) <= 0 {
                    continue;
                }
                if loc.is_null(&mb, i) {
                    continue;
                }
                let fk_key = loc.native_key(&mb, i);
                if seen.insert(fk_key) {
                    keys.push(fk_key);
                }
            }

            if keys.is_empty() {
                continue;
            }
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
                p1_labels.push(P1Label::FkParent {
                    parent_table_id,
                    expected_count,
                });
                p1_checks.push(PipelinedCheck {
                    target_id: parent_table_id,
                    col_hint: 0,
                    payload: Some(CheckPayload::ScatterSource { source: check_batch }),
                    schema: parent_schema,
                });
            } else {
                let idx_schema = unsafe {
                    let cat = &mut *(*disp_ptr).catalog;
                    cat.get_index_schema_by_cols(parent_table_id, &[parent_col_idx as u32])
                        .ok_or_else(|| {
                            format!("FK check: no unique index on parent table {parent_table_id} col {parent_col_idx}")
                        })?
                };
                let pooled = unsafe { (*disp_ptr).pool_pop_batch(parent_table_id) };
                // `loc` is the child FK column; its type drives the sign-extension
                // into the parent index's promoted leading key.
                let check_batch = build_check_batch(&idx_schema, &keys, loc.type_code(), pooled);
                p1_labels.push(P1Label::FkParent {
                    parent_table_id,
                    expected_count,
                });
                p1_checks.push(PipelinedCheck {
                    target_id: parent_table_id,
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
            // net_pk aggregation at batch.count scale on one map keyed by the
            // borrowed OPK bytes. The *distinct* removed PKs are few, so the map
            // stays local and unpooled; `into_iter` drops it before the owned
            // `removed_pks` leaves the block, releasing the batch borrow.
            let removed_pks: Vec<PkBuf> = {
                let mut net: FxHashMap<&[u8], i64> = FxHashMap::default();
                for i in 0..batch.count {
                    let w = batch.get_weight(i);
                    if w == 0 {
                        continue;
                    }
                    *net.entry(batch.get_pk_bytes(i)).or_insert(0) += w;
                }
                net.into_iter()
                    .filter(|&(_, w)| w < 0)
                    .map(|(k, _)| PkBuf::from_bytes(k))
                    .collect()
            };

            if !removed_pks.is_empty() {
                // Resolve every non-PK referenced parent column in ONE batched
                // gather instead of one serial seek per (removed PK × child).
                let project = unsafe { collect_fk_projection(&*(*disp_ptr).catalog, target_id, &source_schema) };
                let gathered = if project.is_empty() {
                    GatherMap::default()
                } else {
                    Self::execute_gather_async(disp_ptr, reactor, sal_excl, target_id, removed_pks.clone(), &project)
                        .await?
                };

                for r in unsafe { (*(*disp_ptr).catalog).fk_children_of(target_id) } {
                    let (child_tid, fk_col_idx, parent_col_idx) = (r.child_tid, r.fk_col_idx, r.parent_col_idx);
                    let idx_schema =
                        unsafe { (*(*disp_ptr).catalog).get_index_schema_by_cols(child_tid, &[fk_col_idx as u32]) }
                            .ok_or_else(|| {
                                format!(
                                    "FK RESTRICT check failed: no index on child table {child_tid} col {fk_col_idx}"
                                )
                            })?;

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
                        removed_pks
                            .iter()
                            .map(|pk| pk_native_key(pk.pk_bytes(), pk_field_off, col_size, col_type))
                            .collect()
                    } else {
                        let proj_pos = project
                            .iter()
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
                    if keys.is_empty() {
                        continue;
                    }

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
                        col_hint: gnitz_wire::pack_pk_cols(&[fk_col_idx as u32]),
                        payload: Some(CheckPayload::Broadcast(check_batch)),
                        schema: idx_schema,
                    });
                }
            }
        }

        // UPSERT PK identification: which incoming PKs already exist in storage.
        // Routing is computed inside execute_pipeline_async; here we only build
        // the check batch from the collected net-positive PK byte spans.
        let upsert_pk_batch: Option<Batch> = if pk_keys.is_empty() {
            None
        } else {
            let pooled = unsafe { (*disp_ptr).pool_pop_batch(target_id) };
            Some(build_check_batch_pkbuf(&source_schema, &pk_keys, pooled))
        };
        if let Some(check_batch) = upsert_pk_batch {
            p1_labels.push(P1Label::UpsertPkId);
            p1_checks.push(PipelinedCheck {
                target_id,
                col_hint: 0,
                payload: Some(CheckPayload::ScatterSource { source: check_batch }),
                schema: source_schema,
            });
        }

        // ----- Phase 1 execute + interpret --------------------------------
        let mut existing_pks: FxHashSet<PkBuf> = FxHashSet::default();
        if !p1_checks.is_empty() {
            let mut p1_results = Self::execute_pipeline_async(disp_ptr, reactor, sal_excl, &mut p1_checks).await?;
            unsafe {
                reclaim_check_batches(&mut *disp_ptr, &mut p1_checks);
            }

            for (idx, label) in p1_labels.iter().enumerate() {
                match label {
                    P1Label::FkParent {
                        parent_table_id,
                        expected_count,
                    } => {
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
                        if matches!(mode, WireConflictMode::Error) && !existing_pks.is_empty() {
                            let conflict_pk = existing_pks.iter().next().unwrap();
                            let (pk_names, sn, tn) =
                                unsafe { (*disp_ptr).pk_violation_context(target_id, &source_schema) };
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
            let project = unsafe { collect_fk_projection(&*(*disp_ptr).catalog, target_id, &source_schema) };

            if !project.is_empty() {
                // Batch rows that are UPDATEs: positive weight and a PK that
                // already exists in committed storage (not an INSERT). Computed
                // once; the per-child loop below indexes into this instead of
                // re-walking the whole batch and re-checking `existing_pks`.
                let update_rows: Vec<usize> = (0..batch.count)
                    .filter(|&i| batch.get_weight(i) > 0 && existing_pks.contains(batch.get_pk_bytes(i)))
                    .collect();

                let mut updated: Vec<PkBuf> = Vec::new();
                let mut seen: FxHashSet<PkBuf> = FxHashSet::default();
                for &i in &update_rows {
                    let pk = PkBuf::from_bytes(batch.get_pk_bytes(i));
                    if seen.insert(pk) {
                        updated.push(pk);
                    }
                }

                // Old values come from committed storage (validation is
                // pre-commit, under the FK table locks held by the executor,
                // so the committed parent state is static across the gather).
                let gathered =
                    Self::execute_gather_async(disp_ptr, reactor, sal_excl, target_id, updated, &project).await?;

                for r in unsafe { (*(*disp_ptr).catalog).fk_children_of(target_id) } {
                    let (child_tid, fk_col_idx, parent_col_idx) = (r.child_tid, r.fk_col_idx, r.parent_col_idx);
                    // PK columns are immutable under UPDATE — nothing to enforce.
                    if source_schema.is_pk_col(parent_col_idx) {
                        continue;
                    }
                    let idx_schema = match unsafe {
                        (*(*disp_ptr).catalog).get_index_schema_by_cols(child_tid, &[fk_col_idx as u32])
                    } {
                        Some(s) => s,
                        None => continue,
                    };

                    // PK columns are skipped above (immutable under UPDATE), so
                    // this referenced column is always a payload column.
                    let loc = source_schema.locate(parent_col_idx);
                    let proj_pos = project
                        .iter()
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
                            if let Some(v) = old_val {
                                retired.push(v);
                            }
                        }
                    }
                    if !retired.is_empty() {
                        // `loc` is the referenced parent (payload) column; its
                        // type drives the sign-extension at the seek encode.
                        p2_restrict.push((child_tid, fk_col_idx as u32, idx_schema, loc.type_code(), retired));
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
            let (col_indices, idx_schema, spec) = unsafe {
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
                // borrow and feeds the P2Label / error formatters. The span
                // plan is the circuit's baked `key_spec` (registration built it
                // from the same owner/index schemas): `key_bytes` builds the
                // OPK leading-key span — null-correct (skip on any NULL) and
                // equality-correct at any width.
                let spec = cat.index_circuits(target_id)[ci].key_spec;
                (PkColList::from_slice(cols), idx_schema, spec)
            };
            let cols = col_indices.as_slice();
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
            let mut seen: FxHashSet<PkBuf> = FxHashSet::with_capacity_and_hasher(batch.count, Default::default());

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
                    if batch.get_weight(i) >= 0 {
                        continue;
                    }
                    if !spec.key_bytes(&mb, i, &mut keybuf) {
                        continue;
                    }
                    retracted_vals.insert(keybuf);
                    retracted_pairs.insert((PkBuf::from_bytes(batch.get_pk_bytes(i)), keybuf));
                }
            }

            for i in 0..batch.count {
                let w = batch.get_weight(i);
                if w <= 0 {
                    continue;
                }

                // A row NULL in any indexed column is not indexed (NULL-distinct).
                if !spec.key_bytes(&mb, i, &mut keybuf) {
                    continue;
                }

                // One row at weight w is the value w times. On a non-unique_pk
                // table that is w live instances (enforce_unique_pk collapses
                // it to one on unique_pk tables) — the same violation as w
                // separate +1 rows, which `seen` below rejects.
                if !unique_pk && w > 1 {
                    return Err(unsafe { (*disp_ptr).unique_violation_err(target_id, cols, true) });
                }

                // In-batch duplicate detection runs for ALL positive-weight
                // rows, INCLUDING UPSERTs: two rows setting the same new unique
                // value in one transaction is a violation regardless of whether
                // their PKs already exist.
                if !seen.insert(keybuf) {
                    return Err(unsafe { (*disp_ptr).unique_violation_err(target_id, cols, true) });
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
                let skip_broadcast = unsafe { (*disp_ptr).unique_filter_all_absent(target_id, packed, &check_keys) };

                if !skip_broadcast {
                    let pooled = unsafe { (*disp_ptr).pool_pop_batch(target_id) };
                    let chk_batch = build_check_batch_with(&idx_schema, &check_keys, pooled, |b, k| {
                        b.extend_pk_bytes(k.padded(stride))
                    });
                    p2_labels.push(P2Label::NonUpsert { col_indices });
                    p2_checks.push(PipelinedCheck {
                        target_id,
                        col_hint: packed,
                        payload: Some(CheckPayload::Broadcast(chk_batch)),
                        schema: idx_schema,
                    });
                }
            }

            if !upsert_keys.is_empty() {
                let pooled = unsafe { (*disp_ptr).pool_pop_batch(target_id) };
                let u_batch = build_check_batch_with(&idx_schema, &upsert_keys, pooled, |b, (k, _)| {
                    b.extend_pk_bytes(k.padded(stride))
                });
                p2_labels.push(P2Label::Upsert {
                    col_indices,
                    upsert_keys,
                    retracted_pairs,
                });
                p2_checks.push(PipelinedCheck {
                    target_id,
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
                col_hint: gnitz_wire::pack_pk_cols(&[fk_col]),
                payload: Some(CheckPayload::Broadcast(chk_batch)),
                schema: idx_schema,
            });
        }

        if p2_checks.is_empty() {
            return Ok(());
        }

        let p2_results = Self::execute_pipeline_async(disp_ptr, reactor, sal_excl, &mut p2_checks).await?;
        unsafe {
            reclaim_check_batches(&mut *disp_ptr, &mut p2_checks);
        }

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
                P2Label::Upsert {
                    col_indices,
                    upsert_keys,
                    retracted_pairs,
                } => {
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
                        if !occupied.contains(key_pk.padded(stride)) {
                            continue;
                        }
                        pending.push((key_pk, is_upsert));
                        // async fn futures are !Unpin; box-pin for join_all_unpin.
                        // One homogeneous future type (all inputs Copy, no `dyn`):
                        // `seek_unique_holder` gates arity internally.
                        futs.push(Box::pin(Self::seek_unique_holder(
                            disp_ptr,
                            reactor,
                            sal_excl,
                            target_id,
                            *col_indices,
                            span_to_natives(&key_pk, idx_cols),
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
    /// Validate an atomic user-table transaction bundle against its
    /// post-transaction state (the simulated fold of all families), except
    /// Error-mode PK existence which is cumulative in frame order. The whole
    /// transaction passes or one violation aborts it — all pre-SAL. The three
    /// rule checks reuse the plain-push distributed probe primitives
    /// (`execute_pipeline_async`, `seek_unique_holder`, `execute_gather_async`,
    /// `fan_out_seek_by_index_collect_async`), fed fold-derived keys instead of a
    /// single batch's raw rows, and — like the plain-push validator — each plans
    /// every one of its probes first and issues them as ONE pipelined burst.
    /// Committed state is stable across every probe because the caller holds the
    /// involved tables' locks and the catalog read lock through the ACK.
    /// Fire one pipelined probe burst and recycle the check batches back to the
    /// pool. `execute_pipeline_async` already returns an empty result for an empty
    /// `checks`, so callers need no length guard. The paired reclaim is centralized
    /// here so no probe site can forget it (a leaked pooled batch).
    async fn execute_and_reclaim(
        disp_ptr: *mut MasterDispatcher,
        reactor: &crate::runtime::reactor::Reactor,
        sal_excl: &Rc<AsyncMutex<()>>,
        checks: &mut [PipelinedCheck],
    ) -> Result<Vec<FxHashSet<PkBuf>>, String> {
        let results = Self::execute_pipeline_async(disp_ptr, reactor, sal_excl, checks).await?;
        unsafe { reclaim_check_batches(&mut *disp_ptr, checks) };
        Ok(results)
    }

    pub async fn validate_txn_distributed_async(
        disp_ptr: *mut MasterDispatcher,
        reactor: &crate::runtime::reactor::Reactor,
        sal_excl: &Rc<AsyncMutex<()>>,
        families: &[TxnFamily],
    ) -> Result<(), String> {
        let bundle = TxnBundle::new(disp_ptr, families)?;
        Self::txn_check_pk(disp_ptr, reactor, sal_excl, &bundle).await?;
        Self::txn_check_unique_indices(disp_ptr, reactor, sal_excl, &bundle).await?;
        Self::txn_check_foreign_keys(disp_ptr, reactor, sal_excl, &bundle).await
    }

    /// Rule U-PK: Error-mode PK existence, cumulative in frame order. One
    /// committed-existence probe per Error-bearing table, all issued in one
    /// burst; then each table's families are walked in frame order and each
    /// Error family checked against the running prefix fold before being folded
    /// into it.
    async fn txn_check_pk(
        disp_ptr: *mut MasterDispatcher,
        reactor: &crate::runtime::reactor::Reactor,
        sal_excl: &Rc<AsyncMutex<()>>,
        b: &TxnBundle<'_>,
    ) -> Result<(), String> {
        let mut tids: Vec<i64> = Vec::new();
        let mut checks: Vec<PipelinedCheck> = Vec::new();
        for &tid in &b.order {
            if !b.families_of(tid).any(|f| matches!(f.mode, WireConflictMode::Error)) {
                continue;
            }
            // Candidate PKs to probe committed: any PK inserted positively by an
            // Error family (a superset of each family's net-positive set). With
            // none, no Error family can carry a net-positive PK, so the whole
            // table's walk is vacuous.
            let mut candidate: FxHashSet<PkBuf> = FxHashSet::default();
            for fam in b.families_of(tid).filter(|f| matches!(f.mode, WireConflictMode::Error)) {
                for row in 0..fam.batch.count {
                    if fam.batch.get_weight(row) > 0 {
                        candidate.insert(PkBuf::from_bytes(fam.batch.get_pk_bytes(row)));
                    }
                }
            }
            if candidate.is_empty() {
                continue;
            }
            let schema = *b.schema(tid);
            let keys: Vec<PkBuf> = candidate.into_iter().collect();
            let pooled = unsafe { (*disp_ptr).pool_pop_batch(tid) };
            tids.push(tid);
            checks.push(PipelinedCheck {
                target_id: tid,
                col_hint: 0,
                payload: Some(CheckPayload::ScatterSource {
                    source: build_check_batch_pkbuf(&schema, &keys, pooled),
                }),
                schema,
            });
        }
        let mut results = Self::execute_and_reclaim(disp_ptr, reactor, sal_excl, &mut checks).await?;

        for (i, &tid) in tids.iter().enumerate() {
            let committed = std::mem::take(&mut results[i]);
            let schema = *b.schema(tid);
            let mut prefix: Overlay = Overlay::default();
            for &fi in b.family_indices(tid) {
                let batch = &b.families[fi].batch;
                if matches!(b.families[fi].mode, WireConflictMode::Error) {
                    // Error-family PK existence, checked against the running prefix
                    // fold then committed state. A +w row is w insertions (see
                    // `pk_net_weight_and_dup_count`), so `pos > 1` is an in-family
                    // duplicate.
                    let net = pk_net_weight_and_dup_count(batch);
                    for (&pk, &(net_w, pos)) in &net {
                        let key_str = || format_pk_value_bytes(pk, &schema);
                        if pos > 1 {
                            return Err(unsafe { (*disp_ptr).batch_dup_pk_err(tid, &schema, &key_str()) });
                        }
                        if net_w <= 0 {
                            continue;
                        }
                        let pkbuf = PkBuf::from_bytes(pk);
                        let exists = match prefix.get(&pkbuf) {
                            Some(FoldOp::Inserted(..)) => true,
                            Some(FoldOp::Deleted) => false,
                            None => committed.contains(&pkbuf),
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
                fold_family(&mut prefix, fi, batch);
            }
        }
        Ok(())
    }

    /// Rule U-SEC: unique secondary indexes, post-transaction. Every
    /// (table, unique circuit)'s surviving spans are planned up front and their
    /// committed-occupancy probes issued in one burst; the per-span committed-
    /// holder seeks then fan out concurrently rather than one round trip each.
    async fn txn_check_unique_indices(
        disp_ptr: *mut MasterDispatcher,
        reactor: &crate::runtime::reactor::Reactor,
        sal_excl: &Rc<AsyncMutex<()>>,
        b: &TxnBundle<'_>,
    ) -> Result<(), String> {
        let mut plans: Vec<UniquePlan> = Vec::new();
        let mut checks: Vec<PipelinedCheck> = Vec::new();
        for &tid in &b.order {
            let (n_circuits, has_unique) = unsafe {
                let cat = &*(*disp_ptr).catalog;
                (cat.get_index_circuit_count(tid), cat.has_any_unique_index(tid))
            };
            if n_circuits == 0 || !has_unique {
                continue;
            }
            let surviving = b.surviving(tid);
            if surviving.is_empty() {
                continue;
            }
            for ci in 0..n_circuits {
                let (col_indices, idx_schema, spec) = unsafe {
                    let cat = &mut *(*disp_ptr).catalog;
                    let cols = match cat.unique_index_circuit_cols(tid, ci) {
                        Some(c) => PkColList::from_slice(c),
                        None => continue,
                    };
                    let idx_schema = match cat.get_index_circuit_schema(tid, ci) {
                        Some(s) => s,
                        None => continue,
                    };
                    // The circuit's baked span plan (registration built it from
                    // the same owner/index schemas).
                    (cols, idx_schema, cat.index_circuits(tid)[ci].key_spec)
                };
                let cols = col_indices.as_slice();
                let stride = idx_schema.pk_stride() as usize;

                // Surviving (span, holder PK) pairs; reject an in-bundle duplicate
                // (two survivors, same span, different PK) on the way.
                let mut by_span: FxHashMap<PkBuf, PkBuf> = FxHashMap::default();
                let mut span_holders: Vec<(PkBuf, PkBuf)> = Vec::new();
                let mut keybuf = PkBuf::empty(0);
                for (pk, fam, row) in surviving {
                    let mb = b.batch(*fam).as_mem_batch();
                    if !spec.key_bytes(&mb, *row as usize, &mut keybuf) {
                        continue; // NULL in an indexed column ⇒ unindexed
                    }
                    match by_span.get(&keybuf) {
                        Some(other) if other != pk => {
                            return Err(unsafe { (*disp_ptr).unique_violation_err(tid, cols, true) });
                        }
                        Some(_) => {}
                        None => {
                            by_span.insert(keybuf, *pk);
                        }
                    }
                    span_holders.push((keybuf, *pk));
                }
                if by_span.is_empty() {
                    continue;
                }

                let spans: Vec<PkBuf> = by_span.keys().cloned().collect();
                let pooled = unsafe { (*disp_ptr).pool_pop_batch(tid) };
                let chk = build_check_batch_with(&idx_schema, &spans, pooled, |bat, k| {
                    bat.extend_pk_bytes(k.padded(stride))
                });
                checks.push(PipelinedCheck {
                    target_id: tid,
                    col_hint: gnitz_wire::pack_pk_cols(cols),
                    payload: Some(CheckPayload::Broadcast(chk)),
                    schema: idx_schema,
                });
                plans.push(UniquePlan {
                    tid,
                    col_indices,
                    idx_schema,
                    spec,
                    stride,
                    span_holders,
                });
            }
        }
        let results = Self::execute_and_reclaim(disp_ptr, reactor, sal_excl, &mut checks).await?;

        // Every occupied surviving span needs its committed holder. Fan the seeks
        // out across all plans before collecting, so they overlap on the wire
        // instead of costing one round trip each.
        let mut pending: Vec<(usize, PkBuf, PkBuf)> = Vec::new();
        let mut futs = Vec::new();
        for (pi, plan) in plans.iter().enumerate() {
            let idx_cols = &plan.idx_schema.columns[..plan.col_indices.as_slice().len()];
            for (span, holder_pk) in &plan.span_holders {
                if !results[pi].contains(span.padded(plan.stride)) {
                    continue;
                }
                pending.push((pi, *span, *holder_pk));
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
            if h == holder_pk {
                continue;
            }
            let plan = &plans[pi];
            let retired = match b.overlay(plan.tid).get(&h) {
                None => false,
                Some(FoldOp::Deleted) => true,
                Some(FoldOp::Inserted(hf, hr)) => {
                    let hmb = b.batch(*hf).as_mem_batch();
                    let mut hspan = PkBuf::empty(0);
                    !plan.spec.key_bytes(&hmb, *hr as usize, &mut hspan) || hspan != span
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
        sal_excl: &Rc<AsyncMutex<()>>,
        b: &TxnBundle<'_>,
    ) -> Result<(), String> {
        // (child tid, fk col, parent tid, parent col) for every FK whose child is
        // bundled; (parent tid, child tid, fk col, referenced col) for every FK
        // whose parent is bundled.
        let mut constraints: Vec<(i64, usize, i64, usize)> = Vec::new();
        let mut children: Vec<(i64, i64, usize, usize)> = Vec::new();
        for &tid in &b.order {
            unsafe {
                let cat = &*(*disp_ptr).catalog;
                constraints.extend(
                    cat.fk_constraints_of(tid)
                        .iter()
                        .map(|c| (tid, c.fk_col_idx, c.target_table_id, c.target_col_idx)),
                );
                children.extend(
                    cat.fk_children_of(tid)
                        .iter()
                        .map(|r| (tid, r.child_tid, r.fk_col_idx, r.parent_col_idx)),
                );
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
            .filter(|(_, _, ptid, _)| b.has(*ptid))
            .map(|&(_, _, ptid, pcol)| (ptid, pcol))
            .chain(children.iter().map(|&(ptid, _, _, pcol)| (ptid, pcol)))
            .collect();
        needed.sort_unstable();
        needed.dedup();
        // Fan the per-parent gathers out concurrently — they run under the full
        // lock union, so overlapping their reply waits (as the U-SEC holder seeks
        // and F2 exemption fetches already do) beats one sequential round trip
        // per parent column.
        let futs: Vec<_> = needed
            .iter()
            .map(|&(ptid, pcol)| Box::pin(Self::parent_retired_added(disp_ptr, reactor, sal_excl, b, ptid, pcol)))
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
        sal_excl: &Rc<AsyncMutex<()>>,
        b: &TxnBundle<'_>,
        constraints: &[(i64, usize, i64, usize)],
        deltas: &ParentDeltas,
    ) -> Result<(), String> {
        let mut plans: Vec<FkProbePlan> = Vec::new();
        let mut checks: Vec<PipelinedCheck> = Vec::new();
        for &(tid, fk_col, parent_tid, parent_col) in constraints {
            let source_schema = b.schema(tid);
            let loc = source_schema.locate(fk_col);

            // The distinct non-NULL FK values of this table's surviving rows.
            let mut seen: FxHashSet<u128> = FxHashSet::default();
            let mut values: Vec<u128> = Vec::new();
            for (_pk, fam, row) in b.surviving(tid) {
                let mb = b.batch(*fam).as_mem_batch();
                if loc.is_null(&mb, *row as usize) {
                    continue;
                }
                let v = loc.native_key(&mb, *row as usize);
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
                        .ok_or_else(|| {
                            format!("TXN FK check: no unique index on parent {parent_tid} col {parent_col}")
                        })?;
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
                probe_tid: parent_tid,
                report_tid: tid,
                col: parent_col,
                schema: probe_schema,
                src_type,
                values,
            });
        }
        let results = Self::execute_and_reclaim(disp_ptr, reactor, sal_excl, &mut checks).await?;

        // A non-bundled parent has no delta (the degenerate plain-push case).
        let no_delta: ParentDelta = (FxHashSet::default(), FxHashSet::default());
        for (i, plan) in plans.iter().enumerate() {
            let (retired, added) = deltas.get(&(plan.probe_tid, plan.col)).unwrap_or(&no_delta);
            for v in &plan.values {
                let in_committed = results[i].contains(&enc_key(&plan.schema, *v, plan.src_type));
                if (in_committed && !retired.contains(v)) || added.contains(v) {
                    continue;
                }
                let (sn, tn, tsn, ttn) = unsafe {
                    let disp = &mut *disp_ptr;
                    let (s, t) = disp.get_qualified_name_owned(plan.report_tid);
                    let (ts, tt) = disp.get_qualified_name_owned(plan.probe_tid);
                    (s, t, ts, tt)
                };
                return Err(format!(
                    "Foreign Key violation in '{sn}.{tn}': value not found in target '{tsn}.{ttn}'"
                ));
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
        sal_excl: &Rc<AsyncMutex<()>>,
        b: &TxnBundle<'_>,
        children: &[(i64, i64, usize, usize)],
        deltas: &ParentDeltas,
    ) -> Result<(), String> {
        let mut plans: Vec<FkProbePlan> = Vec::new();
        let mut checks: Vec<PipelinedCheck> = Vec::new();
        for &(tid, child_tid, fk_col, ref_col) in children {
            let (retired, added) = &deltas[&(tid, ref_col)];
            let v_check: Vec<u128> = retired.iter().copied().filter(|v| !added.contains(v)).collect();
            if v_check.is_empty() {
                continue;
            }
            let idx_schema = unsafe { (*(*disp_ptr).catalog).get_index_schema_by_cols(child_tid, &[fk_col as u32]) }
                .ok_or_else(|| format!("TXN RESTRICT: no index on child {child_tid} col {fk_col}"))?;
            let src_type = b.schema(tid).columns[ref_col].type_code;
            let pooled = unsafe { (*disp_ptr).pool_pop_batch(child_tid) };
            let chk = build_check_batch(&idx_schema, &v_check, src_type, pooled);
            checks.push(PipelinedCheck {
                target_id: child_tid,
                col_hint: gnitz_wire::pack_pk_cols(&[fk_col as u32]),
                payload: Some(CheckPayload::Broadcast(chk)),
                schema: idx_schema,
            });
            plans.push(FkProbePlan {
                probe_tid: child_tid,
                report_tid: tid,
                col: fk_col,
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
                    return Err(restrict_err(disp_ptr, plan.report_tid, plan.probe_tid));
                }
                fetches.push((i, *v));
            }
        }
        if fetches.is_empty() {
            return Ok(());
        }
        if fetches.len() > TXN_RESTRICT_FETCH_LIMIT {
            return Err("TXN: RESTRICT exemption fetch limit exceeded; split the transaction".to_string());
        }
        let futs: Vec<_> = fetches
            .iter()
            .map(|&(i, v)| {
                let plan = &plans[i];
                Box::pin(Self::fan_out_seek_by_index_collect_async(
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
            let rows = rows.map_err(|_| {
                "TXN: RESTRICT exemption fetch for a hot FK value exceeds the reply cap; split the transaction"
                    .to_string()
            })?;
            let Some(rows) = rows else { continue };
            let child_loc = b.schema(plan.probe_tid).locate(plan.col);
            let child_overlay = b.overlay(plan.probe_tid);
            for j in 0..rows.count {
                let cpk = PkBuf::from_bytes(rows.get_pk_bytes(j));
                let still_refs = match child_overlay.get(&cpk) {
                    None => true, // untouched committed child still references v
                    Some(FoldOp::Deleted) => false,
                    Some(FoldOp::Inserted(cf, cr)) => {
                        let smb = b.batch(*cf).as_mem_batch();
                        !child_loc.is_null(&smb, *cr as usize) && child_loc.native_key(&smb, *cr as usize) == v
                    }
                };
                if still_refs {
                    return Err(restrict_err(disp_ptr, plan.report_tid, plan.probe_tid));
                }
            }
        }
        Ok(())
    }

    /// The `(retired, added)` referenced-column value sets of bundled FK parent
    /// `parent_tid` for column `ref_col`: `added` are the surviving parent rows'
    /// (non-NULL) values; `retired` are the old committed values of touched PKs
    /// whose surviving state is absent or holds a different value. The old values
    /// come from the packed PK (a PK column) or one batched `execute_gather_async`
    /// (a non-PK referenced column). NULL values are unindexed and excluded.
    async fn parent_retired_added(
        disp_ptr: *mut MasterDispatcher,
        reactor: &crate::runtime::reactor::Reactor,
        sal_excl: &Rc<AsyncMutex<()>>,
        b: &TxnBundle<'_>,
        parent_tid: i64,
        ref_col: usize,
    ) -> Result<ParentDelta, String> {
        let parent_schema = b.schema(parent_tid);
        let overlay = b.overlay(parent_tid);

        // Old committed value per touched PK (non-NULL only).
        let mut old_of: FxHashMap<PkBuf, u128> = FxHashMap::default();
        if parent_schema.is_pk_col(ref_col) {
            let col_type = parent_schema.columns[ref_col].type_code;
            let col_size = parent_schema.columns[ref_col].size() as usize;
            let off = parent_schema.pk_byte_offset(ref_col) as usize;
            for p in overlay.keys() {
                old_of.insert(*p, pk_native_key(p.pk_bytes(), off, col_size, col_type));
            }
        } else {
            let gathered = Self::execute_gather_async(
                disp_ptr,
                reactor,
                sal_excl,
                parent_tid,
                overlay.keys().copied().collect(),
                &[ref_col as u8],
            )
            .await?;
            for p in overlay.keys() {
                if let Some(Some(v)) = gathered.get(p.pk_bytes()).map(|row| row[0]) {
                    old_of.insert(*p, v);
                }
            }
        }

        let loc = parent_schema.locate(ref_col);
        let mut added: FxHashSet<u128> = FxHashSet::default();
        let mut retired: FxHashSet<u128> = FxHashSet::default();
        for (p, op) in overlay {
            let surviving_val: Option<u128> = match op {
                FoldOp::Inserted(f, r) => {
                    let mb = b.batch(*f).as_mem_batch();
                    (!loc.is_null(&mb, *r as usize)).then(|| loc.native_key(&mb, *r as usize))
                }
                FoldOp::Deleted => None,
            };
            if let Some(sv) = surviving_val {
                added.insert(sv);
            }
            if let Some(&ov) = old_of.get(p) {
                if surviving_val != Some(ov) {
                    retired.insert(ov);
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
                let (schema, col_names) = disp.get_schema_and_names(owner_id);
                disp.write_group_with_req_ids(
                    owner_id,
                    FLAG_UNIQUE_PREFLIGHT,
                    0,
                    &[],
                    &schema,
                    &col_names,
                    0,
                    packed,
                    req_ids,
                    unicast,
                    0,
                    None,
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
    /// Async version of `execute_pipeline`. Writes each check with
    /// per-worker req_ids, signals once, and joins all replies.
    ///
    /// `sal_excl` is held only for the synchronous write + signal phase;
    /// see `dispatch_scan_fanout` for the rationale.
    pub(super) async fn execute_pipeline_async(
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
                    let req_slice = &rids[idx * nw..(idx + 1) * nw];
                    match check.payload.as_ref().expect("payload consumed") {
                        CheckPayload::Broadcast(batch) => {
                            let refs: Vec<Option<&Batch>> = (0..nw).map(|_| Some(batch)).collect();
                            disp.write_group_with_req_ids(
                                check.target_id,
                                FLAG_HAS_PK,
                                0,
                                &refs,
                                &check.schema,
                                &[],
                                0,
                                check.col_hint,
                                req_slice,
                                -1,
                                0,
                                None,
                                &[],
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
    pub(super) async fn execute_gather_async(
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
            let batch = build_check_batch_pkbuf(&parent_schema, &pks, pooled);
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
                        if null_word & (1u64 << k) != 0 {
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
