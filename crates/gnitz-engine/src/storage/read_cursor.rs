//! Opaque read cursor: N-way merge over in-memory batches + mmap'd shards.
//!
//! Produces rows in (PK, payload) order with inline ghost elimination
//! (net weight=0 rows are skipped).

use std::cell::{Cell, OnceCell};
use std::cmp::Ordering;
use std::ptr;
use std::rc::Rc;

use super::batch::{Batch, FIXED_REGION_BYTES};
use super::columnar::{self, ColumnarSource, PkOrd};
use super::{with_payload_cmp, with_pk_ord};
use crate::schema::{SchemaDescriptor, MAX_COLUMNS};
use super::heap::{drive_merge, HeapNode, LoserTree};
use super::merge::{pack_pk_be, ColPtr, MemBatch, UnifiedSource};
use super::shard_reader::{MappedShard, RegionView};

thread_local! {
    /// Reusable per-thread scratch buffer for `drain_sorted_into`. Each
    /// 16-byte tuple is much smaller than the corresponding output row, so
    /// keeping peak capacity for the thread's lifetime is cheap relative to
    /// the batches it feeds.
    ///
    /// `Cell<Vec<_>>` (not `RefCell`) — `DrainGuard` moves the Vec out via
    /// `Cell::take` and returns it on drop, skipping the runtime borrow
    /// check `RefCell` would impose.
    static DRAIN_BUFFER: Cell<Vec<(u32, u32, i64)>> =
        const { Cell::new(Vec::new()) };
}

#[cfg(test)]
thread_local! {
    pub(crate) static REWIND_CALLS: std::cell::Cell<u32> = const { std::cell::Cell::new(0) };
}

/// RAII handle wrapping the thread-local drain scratch buffer.  Behaves like
/// `&mut Vec<_>` via `Deref`/`DerefMut`.  On unwind `Drop` swaps in an empty
/// Vec via `mem::take`, costing a one-time capacity reset (not a poison).
pub(crate) struct DrainGuard {
    inner: Vec<(u32, u32, i64)>,
}

impl DrainGuard {
    #[inline]
    pub(crate) fn new() -> Self {
        // The thread-local Vec is reused across queries and may hold stale
        // elements; clear so `new()` always yields an empty buffer. The
        // elements are `Copy`, so this is an O(1) length reset.
        let mut inner = DRAIN_BUFFER.with(|b| b.take());
        inner.clear();
        Self { inner }
    }
}

impl std::ops::Deref for DrainGuard {
    type Target = Vec<(u32, u32, i64)>;
    fn deref(&self) -> &Self::Target { &self.inner }
}

impl std::ops::DerefMut for DrainGuard {
    fn deref_mut(&mut self) -> &mut Self::Target { &mut self.inner }
}

/// 65 536 × 16 bytes = 1 MB. Caps the retained scratch so a one-off oversized
/// query can't pin an unusually large allocation for the thread's lifetime.
const MAX_DRAIN_BUFFER_CAP: usize = 65_536;

/// Default `drain_chunk` size for DDL scans (index/view backfill, unique
/// pre-flight). Bounds peak backfill memory at O(chunk × row_width); also the
/// default for `CatalogEngine::ddl_scan_chunk_rows`, which tests shrink to
/// exercise chunk boundaries.
pub(crate) const DDL_SCAN_CHUNK_ROWS: usize = 65_536;

impl Drop for DrainGuard {
    #[inline]
    fn drop(&mut self) {
        DRAIN_BUFFER.with(|b| {
            // Keep whichever Vec has the larger capacity. A nested cursor may
            // drop a smaller guard after we took the (larger) thread-local;
            // unconditionally writing our own inner back would shrink it.
            let mut cached = b.take();
            if self.inner.capacity() > cached.capacity()
                && self.inner.capacity() <= MAX_DRAIN_BUFFER_CAP
            {
                cached = std::mem::take(&mut self.inner);
            }
            b.set(cached);
        });
    }
}

// ---------------------------------------------------------------------------
// CursorSource — unified access to in-memory batches or shard mmaps
// ---------------------------------------------------------------------------
//
// Both variants own their backing data via `Rc`, so a `CursorSource` (and
// therefore the enclosing `ReadCursor`) is a self-contained owning value with
// no borrow lifetime.  This is what lets us hand `CursorHandle` (and pointers
// to it) across DAG/VM boundaries without needing a `'static` transmute.

enum CursorSource {
    /// Rc-owned in-memory batch.  The Rc keeps the data alive for the
    /// cursor's lifetime; multiple cursors can share a snapshot.
    Batch(Rc<Batch>),
    /// Rc-owned reference to a MappedShard.  The Rc keeps the mmap alive.
    Shard(Rc<MappedShard>),
}

impl CursorSource {
    #[inline]
    fn get_weight(&self, row: usize) -> i64 {
        match self {
            CursorSource::Batch(b) => b.get_weight(row),
            CursorSource::Shard(s) => s.get_weight(row),
        }
    }

    #[inline]
    fn get_null_word(&self, row: usize) -> u64 {
        match self {
            CursorSource::Batch(b) => b.get_null_word(row),
            CursorSource::Shard(s) => s.get_null_word(row),
        }
    }

    /// Column data as a slice, indexed by PAYLOAD column position.
    #[inline]
    fn get_col_ptr(&self, row: usize, payload_col: usize, col_size: usize) -> &[u8] {
        match self {
            CursorSource::Batch(b) => b.get_col_ptr(row, payload_col, col_size),
            CursorSource::Shard(s) => s.get_col_ptr(row, payload_col, col_size),
        }
    }

    #[inline]
    fn blob_ptr(&self) -> *const u8 {
        match self {
            CursorSource::Batch(b) => {
                if b.blob.is_empty() {
                    ptr::null()
                } else {
                    b.blob.as_ptr()
                }
            }
            CursorSource::Shard(s) => s.blob_ptr(),
        }
    }

    fn blob_slice(&self) -> &[u8] {
        match self {
            CursorSource::Batch(b) => &b.blob,
            CursorSource::Shard(s) => s.blob_slice(),
        }
    }

    /// First row whose OPK bytes are `>= key`. A raw `memcmp` binary search
    /// over the order-preserving PK regions — correct at every PK width with no
    /// schema dependency. `key` must be exactly `pk_stride` OPK bytes.
    fn find_lower_bound_bytes(&self, key: &[u8]) -> usize {
        match self {
            CursorSource::Batch(b) => b.find_lower_bound_bytes(key),
            CursorSource::Shard(s) => s.find_lower_bound_bytes(key),
        }
    }

    /// Galloping forward lower bound seeded at `hint` (this source's live
    /// position). Forwards to the per-source `advance_to`.
    fn advance_to(&self, key: &[u8], hint: usize) -> usize {
        match self {
            CursorSource::Batch(b) => b.advance_to(key, hint),
            CursorSource::Shard(s) => s.advance_to(key, hint),
        }
    }

    #[inline]
    fn get_pk_bytes(&self, row: usize) -> &[u8] {
        match self {
            CursorSource::Batch(b) => b.get_pk_bytes(row),
            CursorSource::Shard(s) => s.get_pk_bytes(row),
        }
    }

    /// Build a `UnifiedSource` view backed by either a `MemBatch`'s flat data
    /// buffer (always Raw regions) or a `MappedShard`'s mmap (Raw or Constant
    /// regions, indexed by payload position).
    ///
    /// Infallible: `MappedShard::open` validates all encoding constraints and
    /// region sizes at open time, so no arm here can fail.
    fn to_unified(&self, schema: &SchemaDescriptor) -> UnifiedSource {
        let mut cols = [ColPtr { base: ptr::null(), stride: 0 }; MAX_COLUMNS - 1];
        match self {
            CursorSource::Batch(b) => {
                let mb = b.as_mem_batch();
                let data_ptr = mb.data.as_ptr();
                let pk_stride = mb.pk_stride as usize;
                let pk_off = mb.offsets[super::batch::REG_PK];
                let nbm_off = mb.offsets[super::batch::REG_NULL_BMP];
                for (pi, _ci, col) in schema.payload_columns() {
                    let off = mb.offsets[super::batch::REG_PAYLOAD_START + pi];
                    cols[pi] = ColPtr {
                        base: unsafe { data_ptr.add(off) },
                        stride: col.size() as usize,
                    };
                }
                UnifiedSource {
                    pk: ColPtr {
                        base: unsafe { data_ptr.add(pk_off) },
                        stride: pk_stride,
                    },
                    null_bmp: ColPtr {
                        base: unsafe { data_ptr.add(nbm_off) },
                        stride: FIXED_REGION_BYTES,
                    },
                    cols,
                    blob_ptr: mb.blob.as_ptr(),
                    blob_len: mb.blob.len(),
                }
            }
            CursorSource::Shard(s) => {
                let pk_stride = s.pk_stride as usize;
                let data_ptr = s.data().as_ptr();

                let pk = match &s.pk {
                    RegionView::Raw { offset, .. } => {
                        ColPtr { base: unsafe { data_ptr.add(*offset) }, stride: pk_stride }
                    }
                    // stride=0: base.add(ri * 0) == base for every row, reads
                    // the constant value identically to null_bmp Constant.
                    RegionView::Constant { value, .. } => ColPtr {
                        base: value.as_ptr(),
                        stride: 0,
                    },
                    RegionView::TwoValue { .. } => unreachable!(),
                };

                let null_bmp = match &s.null_bmp {
                    RegionView::Raw { offset, .. } => {
                        ColPtr {
                            base: unsafe { data_ptr.add(*offset) },
                            stride: FIXED_REGION_BYTES,
                        }
                    }
                    RegionView::Constant { value, .. } => ColPtr {
                        base: value.as_ptr(),
                        stride: 0,
                    },
                    RegionView::TwoValue { .. } => unreachable!(),
                };

                for (pi, _ci, col) in schema.payload_columns() {
                    let cs = col.size() as usize;
                    cols[pi] = match &s.col_regions[pi] {
                        RegionView::Raw { offset, .. } => {
                            ColPtr { base: unsafe { data_ptr.add(*offset) }, stride: cs }
                        }
                        RegionView::Constant { value, .. } => ColPtr {
                            base: value.as_ptr(),
                            stride: 0,
                        },
                        RegionView::TwoValue { .. } => unreachable!(),
                    };
                }

                let blob = s.blob_slice();
                UnifiedSource {
                    pk,
                    null_bmp,
                    cols,
                    blob_ptr: blob.as_ptr(),
                    blob_len: blob.len(),
                }
            }
        }
    }
}

impl ColumnarSource for CursorSource {
    #[inline]
    fn get_null_word(&self, row: usize) -> u64 {
        CursorSource::get_null_word(self, row)
    }
    #[inline]
    fn get_col_ptr(&self, row: usize, payload_col: usize, col_size: usize) -> &[u8] {
        CursorSource::get_col_ptr(self, row, payload_col, col_size)
    }
    #[inline]
    fn blob_slice(&self) -> &[u8] {
        CursorSource::blob_slice(self)
    }
}

// ---------------------------------------------------------------------------
// CursorState — per-source position tracker (struct-of-arrays pair to
// `sources`).  Splitting source from state lets the borrow checker see
// `&self.sources` (immutable) and `&mut self.states` (mutable) as disjoint
// fields, so the merge driver can hold both at once without dance.
// ---------------------------------------------------------------------------

struct CursorState {
    position: usize,
    /// Cached so `is_valid()` and `estimated_length()` work on
    /// `&[CursorState]` alone, without a parallel borrow of `&[CursorSource]`.
    count: usize,
}

impl CursorState {
    #[inline]
    fn is_valid(&self) -> bool {
        self.position < self.count
    }

    fn advance(&mut self, src: &CursorSource) {
        if self.is_valid() {
            self.position += 1;
            self.skip_ghosts(src);
        }
    }

    /// Seek to the first row whose OPK bytes are `>= key`. `key` must be exactly
    /// `pk_stride` OPK bytes.
    fn seek_bytes(&mut self, src: &CursorSource, key: &[u8]) {
        self.position = src.find_lower_bound_bytes(key);
        self.skip_ghosts(src);
    }

    /// Galloping forward seek to the first row whose OPK bytes are `>= key`,
    /// seeded at the live `position`. Forward-only and position-owned, so a
    /// stale or non-monotone hint is unrepresentable — equals `seek_bytes`'s
    /// landing index for any key, only cheaper when the boundary moves forward.
    fn advance_to(&mut self, src: &CursorSource, key: &[u8]) {
        self.position = src.advance_to(key, self.position);
        self.skip_ghosts(src);
    }

    fn skip_ghosts(&mut self, src: &CursorSource) {
        // Batch sources are always consolidated — no ghost rows.
        if let CursorSource::Shard(s) = src {
            self.position = s.next_non_ghost(self.position);
        }
    }
}

// ---------------------------------------------------------------------------
// ReadCursor
// ---------------------------------------------------------------------------

/// Dispatch on source count, replacing the previous `tree: Option<LoserTree>` +
/// parallel `entries.len() == 1` checks. The variants are exhaustive so each call
/// site dispatches once. `Pair` (exactly 2 sources) bypasses the loser tree with
/// a one-compare-per-row 2-head merge read straight from `states[0]`/`states[1]`
/// — the frequent DBSP shape (a delta against a well-compacted trace) that would
/// otherwise pay the heap's fixed per-emit cost (`bench_merge_scan_by_k`'s
/// k1→k2 cliff). `Pair` keeps no separate head state, so the seek/rewind paths
/// (which only rebuild a `Multi` tree) re-drive it with no special handling.
enum SourceMode {
    Empty,
    Single,
    Pair,
    Multi(LoserTree),
}

pub struct ReadCursor {
    sources: Vec<CursorSource>,
    states: Vec<CursorState>,
    /// Many cursor consumers (point lookups, seeks) never call
    /// `scatter_drained_into`; build on first use.
    unified_sources: OnceCell<Vec<UnifiedSource>>,
    mode: SourceMode,
    schema: SchemaDescriptor,
    /// Every source is a PkUnique shard ⇒ payload comparison is skipped on PK
    /// ties (a cross-source PK match is the same Z-set element). The non-PkUnique
    /// comparator is read live from `schema.payload_cmp` via `with_payload_cmp!`.
    is_pk_unique: bool,
    // Current row state
    pub valid: bool,
    /// The current row's PK as a u128: the full PK for `pk_stride ≤ 16`, the
    /// low-16 LE prefix for wide PKs (non-authoritative — wide consumers must
    /// read `current_pk_bytes()` and compare via `compare_pk_bytes`).
    pub current_key: u128,
    pub current_weight: i64,
    pub current_null_word: u64,
    current_entry_idx: usize,
    current_row: usize,
}

/// Row comparator alias for the monomorphized `_with` variants.  `Copy` lets
/// callers forward the same comparator down the call chain at zero cost.
trait RowComparator: Fn(&SchemaDescriptor, &CursorSource, usize, &CursorSource, usize) -> Ordering + Copy {}
impl<F> RowComparator for F where F: Fn(&SchemaDescriptor, &CursorSource, usize, &CursorSource, usize) -> Ordering + Copy {}

/// The full loser-tree order — the stride-dispatched OPK byte order (`pk_ord`),
/// then the payload `row_cmp`. The **single** source of truth for "which head
/// sorts first": the tree build, every `drive`, and the forward-seek fast path
/// all key the heap through it, so a tree maintained in place can never order
/// rows differently from how it was built. `pk_ord.cmp` on each player's full OPK
/// bytes (read straight from its source via `(source_idx, row)`) is exact at
/// every width — no cached key, no wide-prefix tie-break. All-PkUnique callers
/// pass a trivial `|_, _, _, _, _| Ordering::Equal` (a PK tie ⇒ the same Z-set
/// element).
#[inline]
fn heap_less_with<'a, PK: PkOrd + 'a, RowCmp: RowComparator + 'a>(
    schema: &'a SchemaDescriptor,
    sources: &'a [CursorSource],
    pk_ord: PK,
    row_cmp: RowCmp,
) -> impl Fn(&HeapNode, &HeapNode) -> bool + Copy + 'a {
    move |a, b| {
        let (a_src, a_row) = (a.source_idx as usize, a.row as usize);
        let (b_src, b_row) = (b.source_idx as usize, b.row as usize);
        match pk_ord.cmp(sources[a_src].get_pk_bytes(a_row), sources[b_src].get_pk_bytes(b_row)) {
            Ordering::Less => true,
            Ordering::Greater => false,
            Ordering::Equal => {
                row_cmp(schema, &sources[a_src], a_row, &sources[b_src], b_row) == Ordering::Less
            }
        }
    }
}

impl ReadCursor {
    #[inline]
    fn build_tree_with<RowCmp: RowComparator, PK: PkOrd>(
        sources: &[CursorSource],
        states: &[CursorState],
        schema: &SchemaDescriptor,
        row_cmp: RowCmp,
        pk_ord: PK,
    ) -> LoserTree {
        // Keyless leaf: it carries only the row index. The comparator reads each
        // player's OPK bytes through `(source_idx, row)` — the stride-dispatched
        // `pk_ord`, then the payload tiebreak — the shared `heap_less_with` order
        // the drive and forward-seek paths reuse.
        let init = |i: usize| states[i].is_valid().then(|| states[i].position as u32);
        LoserTree::build(sources.len(), init, heap_less_with(schema, sources, pk_ord, row_cmp))
    }

    /// Non-PkUnique payload-dispatch layer; defers to the stride dispatch in
    /// `build_tree_with`. Split so `with_payload_cmp!` can hand it the
    /// monomorphized `row_cmp`, which it then threads through `with_pk_ord!`.
    #[inline]
    fn build_tree_with_payload<RowCmp: RowComparator>(
        sources: &[CursorSource],
        states: &[CursorState],
        schema: &SchemaDescriptor,
        row_cmp: RowCmp,
    ) -> LoserTree {
        with_pk_ord!(schema, Self::build_tree_with, sources, states, schema, row_cmp)
    }

    fn build_tree(
        sources: &[CursorSource],
        states: &[CursorState],
        schema: &SchemaDescriptor,
        is_pk_unique: bool,
    ) -> LoserTree {
        // PkUnique skips the payload tiebreak (a PK tie ⇒ same element) and
        // dispatches only the stride; non-PkUnique dispatches payload (outer) then
        // stride (inner). Each arm passes non-capturing closures so the inner
        // helper inlines them — a direct fn-item reference would fix the
        // source-ref lifetime and conflict with the `_with` HRTB Fn bound.
        if is_pk_unique {
            with_pk_ord!(schema, Self::build_tree_with, sources, states, schema, |_, _, _, _, _| Ordering::Equal)
        } else {
            with_payload_cmp!(schema, Self::build_tree_with_payload, sources, states, schema)
        }
    }

    fn new(sources: Vec<CursorSource>, states: Vec<CursorState>, schema: SchemaDescriptor) -> Self {
        debug_assert_eq!(sources.len(), states.len());
        let is_pk_unique = !sources.is_empty() && sources.iter().all(|s| match s {
            CursorSource::Shard(shard) => shard.is_pk_unique,
            CursorSource::Batch(_)     => false,
        });
        let mode = match sources.len() {
            0 => SourceMode::Empty,
            1 => SourceMode::Single,
            2 => SourceMode::Pair,
            _ => SourceMode::Multi(Self::build_tree(&sources, &states, &schema, is_pk_unique)),
        };
        let mut cursor = ReadCursor {
            sources,
            states,
            unified_sources: OnceCell::new(),
            mode,
            schema,
            is_pk_unique,
            valid: false,
            current_key: 0,
            current_weight: 0,
            current_null_word: 0,
            current_entry_idx: 0,
            current_row: 0,
        };
        cursor.drive();
        cursor
    }

    /// Rebuild the loser tree (when multi-source) after the per-source
    /// positions have been moved, then drive to the first live row. Shared
    /// tail of [`seek`], [`seek_bytes`], and [`rewind`]: repositioning a source
    /// invalidates the tree's cached head comparisons, so it must be rebuilt
    /// before the next `drive`.
    fn rebuild_and_drive(&mut self) {
        if let SourceMode::Multi(_) = &self.mode {
            self.mode = SourceMode::Multi(
                Self::build_tree(&self.sources, &self.states, &self.schema, self.is_pk_unique),
            );
        }
        self.drive();
    }

    /// Reset every source to its first row and re-drive, positioning the cursor
    /// at the first row in storage order. Use this instead of `seek(u128::MIN)`
    /// to rewind a cursor: a signed-PK trace's minimum is a negative value, not
    /// the all-zero bytes `seek(0)` lands on, so `seek(u128::MIN)` would skip
    /// every negative-keyed row. Rewinding by row index is correct at every PK
    /// shape.
    pub fn rewind(&mut self) {
        #[cfg(test)]
        REWIND_CALLS.with(|c| c.set(c.get() + 1));
        for (src, state) in self.sources.iter().zip(self.states.iter_mut()) {
            state.position = 0;
            state.skip_ghosts(src);
        }
        self.rebuild_and_drive();
    }

    /// Byte-addressed sibling of [`seek`]. `key` must be exactly `pk_stride`
    /// bytes. Correct at every PK width: the inner `drive`/`build_tree` key the
    /// heap via the stride-dispatched `pk_ord` over the full OPK bytes, so this
    /// survives `pk_stride > 16` where the u128 `seek` cannot.
    pub fn seek_bytes(&mut self, key: &[u8]) {
        for (src, state) in self.sources.iter().zip(self.states.iter_mut()) {
            state.seek_bytes(src, key);
        }
        self.rebuild_and_drive();
    }

    /// Galloping forward lower-bound seek, seeding each source's search at that
    /// source's live `position` (the stream owns its hint — there is no hint
    /// parameter). Lands on the identical row `seek_bytes(key)` would
    /// (`gallop_lower_bound_bytes` == `find_lower_bound_bytes` for every hint),
    /// only cheaper when the boundary moves forward. Backward-capable: a
    /// non-monotone hint forfeits the speedup (the search falls back to a bounded
    /// `[0, position)` scan) but never correctness — so monotone consumers (the
    /// co-group merges, the natural-PK reduce reads) get the skip while
    /// non-monotone ones (the unordered `gather_family_bytes` resolve, the
    /// `Lt`/`Le` range reset) stay correct. `key` must be exactly `pk_stride` OPK
    /// bytes.
    ///
    /// On a live multi-source cursor a *strictly-forward* seek additionally
    /// maintains the loser tree in place (`seek_forward_multi`) instead of
    /// rebuilding it: the dominant access pattern (the co-group / range / reduce
    /// drivers all repositioning in ascending key order) thus pays one
    /// `Θ(log num_sources)` walk-up per galloped laggard and **zero** allocation,
    /// not a from-scratch `Θ(num_sources)` rebuild + its two Vec allocations on
    /// every step.
    pub fn advance_to(&mut self, key: &[u8]) {
        // Strictly-forward seek on a live multi-source cursor: maintain the loser
        // tree in place instead of rebuilding it. The boundary is strict — `key`
        // must be > the current emitted PK. At `key == current_pk` the lower bound
        // of `key` can be a row this cursor already consumed (an earlier payload at
        // the same PK), which a forward gallop cannot reach; that case — and every
        // backward / exhausted / Single / Empty case — takes the absolute
        // reposition + rebuild below. `self.valid` is checked first so
        // `current_pk_cmp_bytes` never reads an unpositioned cursor.
        if self.valid
            && matches!(self.mode, SourceMode::Multi(_))
            && self.current_pk_cmp_bytes(key) == Ordering::Less
        {
            self.seek_forward_multi(key);
            return;
        }
        for (src, state) in self.sources.iter().zip(self.states.iter_mut()) {
            state.advance_to(src, key);
        }
        self.rebuild_and_drive();
    }

    /// Gallop the `Multi` loser tree forward to the first head `>= key`, then
    /// drive the first live group. The gallop keys the heap through the shared
    /// `heap_less_with` order, so an all-PkUnique cursor passes the trivial
    /// payload tiebreak while everything else routes the live comparator through
    /// `with_payload_cmp!`. Precondition (enforced by [`advance_to`]'s dispatch):
    /// `matches!(self.mode, SourceMode::Multi)` and `key` > the current emitted PK.
    fn seek_forward_multi(&mut self, key: &[u8]) {
        if self.is_pk_unique {
            with_pk_ord!(self.schema, Self::seek_forward_multi_pk_unique, self, key);
        } else {
            with_payload_cmp!(self.schema, Self::seek_forward_multi_with, self, key);
        }
    }

    /// All-PkUnique forward seek: gallop with the trivial payload tiebreak (a PK
    /// tie ⇒ the same Z-set element), then drive. `pk_ord` is reused for the
    /// gallop and the drive, so the stride is dispatched once for both.
    #[inline]
    fn seek_forward_multi_pk_unique<PK: PkOrd>(&mut self, key: &[u8], pk_ord: PK) {
        self.gallop_heap_forward(key, pk_ord, |_, _, _, _, _| Ordering::Equal);
        self.drive_pk_unique_inner(pk_ord);
    }

    /// Non-PkUnique forward seek: gallop with the live payload comparator, then
    /// drive. Split so `with_payload_cmp!` hands it the monomorphized `row_cmp`,
    /// which it threads through `with_pk_ord!` for the stride dispatch.
    #[inline]
    fn seek_forward_multi_with<RowCmp: RowComparator>(&mut self, key: &[u8], row_cmp: RowCmp) {
        with_pk_ord!(self.schema, Self::seek_forward_multi_both, self, key, row_cmp);
    }

    #[inline]
    fn seek_forward_multi_both<RowCmp: RowComparator, PK: PkOrd>(
        &mut self,
        key: &[u8],
        row_cmp: RowCmp,
        pk_ord: PK,
    ) {
        self.gallop_heap_forward(key, pk_ord, row_cmp);
        self.drive_with_inner(row_cmp, pk_ord);
    }

    /// Maintain the `Multi` loser tree in place while galloping its heads forward
    /// to `key`, keyed by the shared `heap_less_with(.., pk_ord, row_cmp)` order.
    /// Scoping the heap/sources/states borrows to this call frees `self` for the
    /// caller's following `drive`. Precondition: `matches!(self.mode, SourceMode::Multi)`.
    fn gallop_heap_forward<PK: PkOrd, RowCmp: RowComparator>(
        &mut self,
        key: &[u8],
        pk_ord: PK,
        row_cmp: RowCmp,
    ) {
        let heap = match &mut self.mode {
            SourceMode::Multi(h) => h,
            _ => unreachable!("gallop_heap_forward requires SourceMode::Multi"),
        };
        let less = heap_less_with(&self.schema, &self.sources, pk_ord, row_cmp);
        Self::seek_phase(heap, &self.sources, &mut self.states, key, pk_ord, &less);
    }

    /// Advance every laggard head (OPK `< key`) to its own `lower_bound(key)`,
    /// restoring the loser tree with one `replace_top`/`pop_top` per gallop. Only
    /// the current root is ever a laggard — it is the global min, so once it is
    /// `>= key` every head is — and each gallop moves that source to `>= key`, so
    /// the loop touches at most `num_sources` leaves and always terminates. On
    /// return every head is `>= key`, leaving the tree positioned exactly as a
    /// from-scratch rebuild at `key` would; the caller's `drive` then folds the
    /// first live group. `key` is exactly `pk_stride` OPK bytes; `pk_ord` is the
    /// stride-dispatched OPK comparator.
    fn seek_phase<PK: PkOrd>(
        heap: &mut LoserTree,
        sources: &[CursorSource],
        states: &mut [CursorState],
        key: &[u8],
        pk_ord: PK,
        less: &impl Fn(&HeapNode, &HeapNode) -> bool,
    ) {
        while !heap.is_empty() {
            let HeapNode { source_idx: src, row } = *heap.peek();
            let (src, row) = (src as usize, row as usize);
            // The root is the global min, so once its OPK bytes reach `key` every
            // head has. `pk_ord.cmp` compares the full stride bytes — exact at
            // every width, so no wide-prefix recheck. Otherwise the root lags;
            // gallop it forward.
            if pk_ord.cmp(sources[src].get_pk_bytes(row), key) != Ordering::Less {
                break;
            }
            states[src].advance_to(&sources[src], key); // gallop this laggard
            if states[src].is_valid() {
                heap.replace_top(states[src].position as u32, less);
            } else {
                heap.pop_top(less);
            }
        }
    }

    /// Seek to `key` (exact `pk_stride` OPK bytes) and report whether it landed
    /// on a *present, live* row: positioned, PK byte-equal, and net weight > 0.
    /// The weight gate is load-bearing, not redundant — a single-source cursor
    /// (`drive_single`) does not consolidate, so it can surface a tombstone an
    /// uncompacted source still holds; every point lookup must filter it. Folds
    /// the `seek_bytes` + valid/exact-PK/positive-weight predicate that point
    /// seeks (`seek_family`, `gather_family_bytes`, index resolve, the
    /// unique-upsert probe, single-row retract) would otherwise open-code.
    pub fn seek_exact_live(&mut self, key: &[u8]) -> bool {
        self.seek_bytes(key);
        self.valid && self.current_pk_eq(key) && self.current_weight > 0
    }

    /// Galloping forward sibling of [`seek_exact_live`]: `advance_to(key)` then
    /// the same present/exact-PK/positive-weight gate. For an **ascending** probe
    /// sequence (each `key` ≥ the last) the per-source search is seeded at the
    /// live position, turning K scattered point-seeks into one monotone forward
    /// sweep that keeps shard pages and merge state hot. Correct for any probe
    /// order — `advance_to` is backward-capable — so an out-of-order key only
    /// forfeits the speedup, never correctness. `key` must be exactly `pk_stride`
    /// OPK bytes. Both an ascending resolve loop (the sorted-`&[PkBuf]` callers)
    /// and an unordered batch sharing one cursor (`gather_family_bytes`) use this:
    /// the former gets the full sweep, the latter still beats re-seeking from
    /// scratch via the bounded `[0, position)` backward branch. A lone random
    /// point lookup can keep `seek_exact_live`.
    pub fn advance_to_exact_live(&mut self, key: &[u8]) -> bool {
        self.advance_to(key);
        self.valid && self.current_pk_eq(key) && self.current_weight > 0
    }

    /// PK region of the current row as raw bytes, without copying. Byte-addressed
    /// sibling of the `current_key` field; correct for compound/wide PKs.
    pub fn current_pk_bytes(&self) -> &[u8] {
        self.sources[self.current_entry_idx].get_pk_bytes(self.current_row)
    }

    /// Seek to a group's PK by its OPK `key_bytes` (the bytes from
    /// `get_pk_bytes`/`current_pk_bytes`, already order-preserving at rest).
    /// Correct at every PK width and PK signedness.
    pub fn seek_group(&mut self, key_bytes: &[u8]) {
        self.seek_bytes(key_bytes);
    }

    /// Whether the current row's PK equals the group's OPK `key_bytes`. Callers
    /// must gate on `valid` first — this reads the current row's bytes, which is
    /// undefined on an unpositioned cursor.
    #[inline]
    pub fn current_pk_eq(&self, key_bytes: &[u8]) -> bool {
        self.current_pk_bytes() == key_bytes
    }

    /// Walk the equal-PK group at the current position to its end, reporting
    /// whether any entry has positive weight — i.e. whether `key` is a member of
    /// `Distinct(trace)`. An uncompacted trace may present a tombstone
    /// (weight ≤ 0) *before* a live row, so the whole group is scanned, not just
    /// its head. Consumes the group: on return the cursor sits at the first row
    /// past it, satisfying the co-group callback's "walk the whole group"
    /// contract. `key` is the group's OPK bytes (the `key` handed to a
    /// `cogroup_left`/`cogroup_intersection` callback).
    pub fn group_has_positive(&mut self, key: &[u8]) -> bool {
        let mut present = false;
        self.walk_pk_group(key, |w| present |= w > 0);
        present
    }

    /// Walk the equal-`key` PK group from the current position to its end,
    /// invoking `f` with each non-ghost `(PK, payload)` sub-group's **net**
    /// weight — exactly the rows the old `while current_pk_eq { advance }` loop
    /// observed, one per consolidated sub-group (never the whole PK group summed,
    /// which would mis-report a group whose payloads' weights offset). The
    /// comparator dispatch is hoisted out of the per-row loop. On return the
    /// cursor sits at the first row past the group with every `current_*` field
    /// committed (or `valid == false` at end of source) — the same postcondition
    /// the replaced loop left, which the next co-group `key()` / loop guard reads.
    fn walk_pk_group<F: FnMut(i64)>(&mut self, key: &[u8], mut f: F) {
        // Single bypass: a tight position scan reading only `get_weight` /
        // `get_pk_bytes` (the cursor-free floor's cost), skipping the per-row
        // `current_*` materialization (`current_key` recompute, null word, …).
        // The terminating `drive_single` commits the exit row so the
        // postcondition holds. This is the cogroup-benchmark's hot path.
        if matches!(self.mode, SourceMode::Single) {
            while self.states[0].is_valid() {
                let pos = self.states[0].position;
                if self.sources[0].get_pk_bytes(pos) != key {
                    break;
                }
                f(self.sources[0].get_weight(pos));
                self.states[0].advance(&self.sources[0]);
            }
            self.drive_single();
            return;
        }
        // Empty / Multi: the per-row-commit walk with the comparator dispatch
        // hoisted out of the advance loop (Empty's guard fails on `!valid`). The
        // Multi heap fold dominates the per-row `current_*` commit, so a lean
        // emit there saves little; correctness and the dispatch hoist are the win.
        self.for_each_pk_group_row(key, |c| f(c.current_weight));
    }

    /// Walk the equal-`key` PK group, invoking `f(&*self)` at each emitted row so
    /// the callback can read the current trace row's columns / weight (e.g.
    /// `write_join_row`, `push_current_row`). Unlike [`walk_pk_group`] this commits
    /// `current_*` per row (the callback reads through it), but hoists the
    /// `is_pk_unique` + `with_payload_cmp!` dispatch out of the loop. `f` must not
    /// re-enter the cursor. Same exit postcondition and `(PK, payload)`-sub-group
    /// granularity as [`walk_pk_group`]; same group-walk contract as the
    /// `while current_pk_eq { advance }` loops it replaces.
    pub(crate) fn for_each_pk_group_row<F: FnMut(&ReadCursor)>(&mut self, key: &[u8], f: F) {
        if self.is_pk_unique {
            self.for_each_pk_group_row_pk_unique(key, f);
        } else {
            with_payload_cmp!(self.schema, Self::for_each_pk_group_row_with, self, key, f);
        }
    }

    /// All-PkUnique specialization of [`for_each_pk_group_row`]: advances via the
    /// payload-comparator-free `advance_pk_unique`.
    fn for_each_pk_group_row_pk_unique<F: FnMut(&ReadCursor)>(&mut self, key: &[u8], mut f: F) {
        while self.valid && self.current_pk_eq(key) {
            f(&*self);
            self.advance_pk_unique();
        }
    }

    /// Non-PkUnique specialization of [`for_each_pk_group_row`]: the monomorphized
    /// `row_cmp` (selected once by `with_payload_cmp!`) is threaded through each
    /// `advance_with`, so the per-row advance never re-selects the comparator.
    #[inline]
    fn for_each_pk_group_row_with<RowCmp: RowComparator, F: FnMut(&ReadCursor)>(
        &mut self,
        key: &[u8],
        mut f: F,
        row_cmp: RowCmp,
    ) {
        while self.valid && self.current_pk_eq(key) {
            f(&*self);
            self.advance_with(row_cmp);
        }
    }

    /// Storage-order comparison of the current row's PK against an OPK `key`
    /// (already-encoded bytes). Universal byte path; correct at every PK width.
    /// Callers must gate on `valid` first.
    #[inline]
    pub fn current_pk_cmp_bytes(&self, key: &[u8]) -> Ordering {
        self.current_pk_bytes().cmp(key)
    }

    /// Compute the `current_key` field from a row's OPK bytes. Narrow PKs
    /// (`stride ≤ 16`) recover the native (unsigned) value via right-aligned BE
    /// — the form catalog readers expect (`current_key as u64`). Wide PKs
    /// (`stride > 16`) get a non-authoritative left-aligned OPK prefix; wide
    /// consumers must read `current_pk_bytes()` instead.
    #[inline]
    fn current_key_from_bytes(bytes: &[u8]) -> u128 {
        if bytes.len() <= 16 {
            gnitz_wire::widen_pk_be(bytes, bytes.len())
        } else {
            pack_pk_be(bytes)
        }
    }

    /// Position the cursor at the first row whose PK begins with `prefix` and
    /// whose weight is `> 0`. Returns `true` on a hit (cursor stays positioned;
    /// `current_pk_bytes`, `current_weight` etc. are valid). Returns `false`
    /// on miss (cursor may be past the prefix range or fully invalid).
    ///
    /// Centralises the seek-pad-walk dance used by the compound-PK secondary
    /// index: zero-pads `prefix` to `pk_stride` for `seek_bytes`, then walks
    /// forward until the prefix terminates or a positive-weight match appears.
    /// `prefix` is the OPK image of the leading PK column(s).
    pub fn seek_first_positive_with_prefix(&mut self, prefix: &[u8]) -> bool {
        let stride = self.schema.pk_stride() as usize;
        let mut key = [0u8; crate::schema::MAX_PK_BYTES];
        let copy_len = prefix.len().min(stride);
        key[..copy_len].copy_from_slice(&prefix[..copy_len]);
        // Suffix stays zero: 0x00 is the OPK minimum for every PK type (signed
        // MIN maps to all-zeros after the sign flip), so the zero-padded key is
        // the correct lower bound for the suffix columns at every type.
        self.seek_bytes(&key[..stride]);
        self.walk_to_positive_with_prefix(prefix)
    }

    /// Walk forward from the current position (no seek) to the next row whose
    /// PK begins with `prefix` and whose weight is `> 0`. Returns `false` once
    /// the prefix range ends or the cursor exhausts. The seek-less companion to
    /// [`seek_first_positive_with_prefix`]: callers that already consumed one
    /// match `advance()` past it then call this to find the next, instead of
    /// re-seeking (which would re-find the same entry and spin forever).
    pub fn walk_to_positive_with_prefix(&mut self, prefix: &[u8]) -> bool {
        while self.valid {
            if !self.current_pk_bytes().starts_with(prefix) {
                return false;
            }
            if self.current_weight > 0 {
                return true;
            }
            self.advance();
        }
        false
    }

    pub fn advance(&mut self) {
        if !self.valid { return; }
        if self.is_pk_unique {
            self.advance_pk_unique();
        } else {
            with_payload_cmp!(self.schema, Self::advance_with, self);
        }
    }

    /// Step past the previously-emitted row before a re-drive. Only `Single`
    /// pre-steps: its `drive` just commits the row at the current position, so we
    /// must advance off it first. `Empty`/`Pair`/`Multi` do not — their drives
    /// already consumed the emitted group (the `Pair` fold and the heap fold both
    /// advance every head past it), so the next drive opens a fresh group.
    #[inline]
    fn pre_step_single(&mut self) {
        if matches!(self.mode, SourceMode::Single) {
            self.states[0].advance(&self.sources[0]);
        }
    }

    #[inline]
    fn advance_pk_unique(&mut self) {
        self.pre_step_single();
        self.drive_pk_unique();
    }

    #[inline]
    fn advance_with<RowCmp: RowComparator>(&mut self, row_cmp: RowCmp) {
        self.pre_step_single();
        self.drive_with(row_cmp);
    }

    #[inline]
    fn drive(&mut self) {
        if self.is_pk_unique {
            self.drive_pk_unique();
        } else {
            with_payload_cmp!(self.schema, Self::drive_with, self);
        }
    }

    /// Drive the non-`Multi` modes, returning `true` if one handled the step;
    /// `Multi` returns `false` so the caller dispatches its own monomorphized
    /// `*_inner`. Shared by `drive_pk_unique` and `drive_with` so the
    /// Empty/Single/Pair ladder lives in one place. `row_cmp` reaches only the
    /// `Pair` bypass (PkUnique passes the trivial equal-PK ⇒ same-element cmp).
    #[inline]
    fn drive_small_modes<RowCmp: RowComparator>(&mut self, row_cmp: RowCmp) -> bool {
        if matches!(self.mode, SourceMode::Empty) { self.valid = false; return true; }
        if matches!(self.mode, SourceMode::Single) { self.drive_single(); return true; }
        if matches!(self.mode, SourceMode::Pair) {
            with_pk_ord!(self.schema, Self::drive_pair_inner, self, row_cmp);
            return true;
        }
        false
    }

    /// N-way merge driver for all-PkUnique cursors. Skips payload comparison: a
    /// cross-source PK tie implies the same Z-set element (no retractions exist in
    /// any source). The narrow/wide fork is gone — the stride-dispatched `pk_ord`
    /// compares the full OPK bytes at every width, so a wide prefix collision can
    /// no longer false-merge two distinct PKs.
    #[inline]
    fn drive_pk_unique(&mut self) {
        // PkUnique Empty/Single/Pair: equal PK ⇒ same element, so the payload
        // tiebreak is trivial (the Pair fold still sums weights across the heads).
        if self.drive_small_modes(|_, _, _, _, _| Ordering::Equal) { return; }
        with_pk_ord!(self.schema, Self::drive_pk_unique_inner, self);
    }

    /// `Multi` PkUnique drive, monomorphized on the stride comparator `pk_ord`.
    /// Precondition: `matches!(self.mode, SourceMode::Multi)`.
    #[inline]
    fn drive_pk_unique_inner<PK: PkOrd>(&mut self, pk_ord: PK) {
        let heap = match &mut self.mode {
            SourceMode::Multi(h) => h,
            _ => unreachable!("drive_pk_unique_inner requires SourceMode::Multi"),
        };
        let schema  = &self.schema;
        let sources = &self.sources;
        let states  = &mut self.states;
        // `less` is the pure OPK order (no payload term). `same_pk` is the
        // width-agnostic OPK byte equality; matching PK ⇒ same element, so
        // `eq_payload` is trivially true. The debug-assert pins the PkUnique
        // invariant (equal PK ⇒ equal payload), firing only on a real violation.
        let less = |a: &HeapNode, b: &HeapNode| -> bool {
            pk_ord.cmp(
                sources[a.source_idx as usize].get_pk_bytes(a.row as usize),
                sources[b.source_idx as usize].get_pk_bytes(b.row as usize),
            ).is_lt()
        };
        let same_pk = |a_src: usize, a_row: usize, b_src: usize, b_row: usize| {
            let eq = sources[a_src].get_pk_bytes(a_row) == sources[b_src].get_pk_bytes(b_row);
            debug_assert!(
                !eq || columnar::compare_rows(schema, &sources[a_src], a_row, &sources[b_src], b_row)
                    == Ordering::Equal,
                "PkUnique shard has a PK tie with different payloads — corrupted data or wrong flag",
            );
            eq
        };
        let eq_payload = |_, _, _, _| true;
        let emitted = Self::drive_inner(heap, sources, states, less, same_pk, eq_payload);
        self.commit_emitted(emitted);
    }

    /// Commit (or invalidate from) the `(net_weight, source_idx, row)` a `Multi`
    /// drive emitted. `current_key` is recomputed from the row's PK bytes here
    /// (off the comparator) to honour its native-value / wide-prefix contract.
    #[inline]
    fn commit_emitted(&mut self, emitted: Option<(i64, usize, usize)>) {
        if let Some((weight, idx, row)) = emitted {
            self.valid = true;
            self.current_key = Self::current_key_from_bytes(self.sources[idx].get_pk_bytes(row));
            self.current_weight = weight;
            self.current_entry_idx = idx;
            self.current_row = row;
            self.current_null_word = self.sources[idx].get_null_word(row);
        } else {
            self.valid = false;
        }
    }

    /// Single-source bypass: no heap, no ghost filter.
    /// `Batch` inputs are pre-consolidated and `CursorState::advance`
    /// already calls `skip_ghosts` for shard sources, so the state's
    /// current position is either valid emit-ready or past-end.
    #[inline]
    fn drive_single(&mut self) {
        let state = &self.states[0];
        if state.is_valid() {
            let pos = state.position;
            let src = &self.sources[0];
            self.valid = true;
            // current_key carries the native value for narrow PKs (the form
            // catalog readers and op_distinct expect); a non-authoritative OPK
            // prefix for wide (wide consumers read current_pk_bytes()).
            self.current_key = Self::current_key_from_bytes(src.get_pk_bytes(pos));
            self.current_weight = src.get_weight(pos);
            self.current_null_word = src.get_null_word(pos);
            self.current_entry_idx = 0;
            self.current_row = pos;
        } else {
            self.valid = false;
        }
    }

    /// Shared N-way merge driver body for the `Multi` drives. Mirrors storage's
    /// `merge_batches_inner`: the caller selects `less` / `same_pk` / `eq_payload`
    /// (per stride and payload), and monomorphization compiles each combination to
    /// its own branch-free copy of the (selector-independent) advance/weight/emit
    /// hot loop. Returns the emitted group as `(weight, source_idx, row)`, or
    /// `None` if drained — `current_key` is recomputed from PK bytes at the commit
    /// point (`commit_emitted`), off the comparator.
    #[inline]
    fn drive_inner<L, SP, EQ>(
        heap: &mut LoserTree,
        sources: &[CursorSource],
        states: &mut [CursorState],
        less: L,
        same_pk: SP,
        eq_payload: EQ,
    ) -> Option<(i64, usize, usize)>
    where
        L: Fn(&HeapNode, &HeapNode) -> bool + Copy,
        SP: Fn(usize, usize, usize, usize) -> bool,
        EQ: Fn(usize, usize, usize, usize) -> bool,
    {
        let mut emitted: Option<(i64, usize, usize)> = None;
        drive_merge(
            heap, less,
            |src| {
                states[src].advance(&sources[src]);
                states[src].is_valid().then(|| states[src].position as u32)
            },
            same_pk,
            eq_payload,
            |src, row| sources[src].get_weight(row),
            |gs, gr, nw| {
                emitted = Some((nw, gs, gr));
                std::ops::ControlFlow::Break(())
            },
        );
        emitted
    }

    /// Drive to the next group with the live payload comparator. Empty/Single/Pair
    /// go through `drive_small_modes`; the `Multi` heap path dispatches the stride
    /// comparator (`with_pk_ord!`) into `drive_with_inner`.
    ///
    /// On the heap path `drive_merge` folds tied rows for us; we hand it `Break`
    /// from `emit` the first time we see a non-ghost group to return immediately.
    /// Ghost groups (net weight = 0) cause `drive_merge` to skip emit and open the
    /// next group, so the loop naturally walks past them.
    #[inline]
    fn drive_with<RowCmp: RowComparator>(&mut self, row_cmp: RowCmp) {
        if self.drive_small_modes(row_cmp) { return; }
        with_pk_ord!(self.schema, Self::drive_with_inner, self, row_cmp);
    }

    /// `Multi` non-PkUnique drive, monomorphized on payload (`row_cmp`) and stride
    /// (`pk_ord`). Precondition: `matches!(self.mode, SourceMode::Multi)`.
    #[inline]
    fn drive_with_inner<RowCmp: RowComparator, PK: PkOrd>(&mut self, row_cmp: RowCmp, pk_ord: PK) {
        let heap = match &mut self.mode {
            SourceMode::Multi(h) => h,
            _ => unreachable!("drive_with_inner requires SourceMode::Multi"),
        };
        let schema  = &self.schema;
        let sources = &self.sources;
        let states  = &mut self.states;
        // `drive_inner` returns the emitted group as a tuple rather than writing
        // `self.current_*` directly: its closures already reborrow `&sources` +
        // `&mut states`, so also capturing `&mut self.current_*` would force a
        // whole-`self` borrow. `commit_emitted` writes the tuple after it returns.
        //
        // `less` is the shared `heap_less_with` order: the stride-dispatched OPK
        // compare settles the PK axis, then the payload `row_cmp`. `same_pk` is the
        // width-agnostic OPK byte equality (so two distinct wide PKs sharing a
        // prefix never fold); `eq_payload` is now payload-only (the PK term is
        // `same_pk`).
        let less = heap_less_with(schema, sources, pk_ord, row_cmp);
        let same_pk = |a_src: usize, a_row: usize, b_src: usize, b_row: usize| {
            sources[a_src].get_pk_bytes(a_row) == sources[b_src].get_pk_bytes(b_row)
        };
        let eq_payload = |a_src: usize, a_row: usize, b_src: usize, b_row: usize| {
            row_cmp(schema, &sources[a_src], a_row, &sources[b_src], b_row) == Ordering::Equal
        };
        let emitted = Self::drive_inner(heap, sources, states, less, same_pk, eq_payload);
        self.commit_emitted(emitted);
    }

    /// Two-head merge bypass for `SourceMode::Pair` — the heap-free analogue of
    /// `drive_single`. Reads the two heads straight from `states[0]`/`states[1]`
    /// (no cached head positions, so the seek/rewind paths re-drive a Pair with no
    /// special handling), merges by one stride-dispatched PK compare (`pk_ord`)
    /// plus the payload `row_cmp` on a PK tie, folds equal `(PK, payload)` across
    /// both heads, and skips ghost groups — committing the first live group's
    /// exemplar. Consumes the emitted group (advances both heads past it), so the
    /// next call opens the next group — the same postcondition `drive_with_inner`'s
    /// heap fold leaves, which is why `advance` must NOT pre-step for a Pair.
    /// Monomorphized on payload (`row_cmp`) and stride (`pk_ord`); PkUnique passes
    /// the trivial `row_cmp` (equal PK ⇒ same element).
    #[inline]
    fn drive_pair_inner<RowCmp: RowComparator, PK: PkOrd>(&mut self, row_cmp: RowCmp, pk_ord: PK) {
        debug_assert!(matches!(self.mode, SourceMode::Pair));
        let schema  = &self.schema;
        let sources = &self.sources;
        let states  = &mut self.states;

        let emitted: Option<(i64, usize, usize)> = loop {
            let v0 = states[0].is_valid();
            let v1 = states[1].is_valid();
            if !v0 && !v1 {
                break None;
            }
            // Pick the exemplar (the smaller head by (PK, payload)), capturing its
            // PK bytes from the same fetch, and note whether the OTHER head's
            // current row is also in this group. The latter is true ONLY when the
            // two heads tie on both PK and payload: a PK difference (Less/Greater)
            // or a same-PK payload difference makes the loser a strictly-later
            // group, so it cannot fold now — and the selection compare already
            // told us which, letting the common (distinct-PK) path skip the other
            // head's fold probe entirely.
            let (ex_src, ex_pk, other_in_group): (usize, &[u8], bool) = if !v1 {
                (0, sources[0].get_pk_bytes(states[0].position), false)
            } else if !v0 {
                (1, sources[1].get_pk_bytes(states[1].position), false)
            } else {
                let p0 = sources[0].get_pk_bytes(states[0].position);
                let p1 = sources[1].get_pk_bytes(states[1].position);
                match pk_ord.cmp(p0, p1) {
                    Ordering::Less => (0, p0, false),
                    Ordering::Greater => (1, p1, false),
                    Ordering::Equal => {
                        match row_cmp(schema, &sources[0], states[0].position,
                                              &sources[1], states[1].position) {
                            Ordering::Greater => (1, p1, false),
                            Ordering::Less => (0, p0, false),
                            Ordering::Equal => (0, p0, true), // identical (PK,payload): fold both
                        }
                    }
                }
            };
            let ex_row = states[ex_src].position;

            // Account the exemplar's weight and consume it WITHOUT a same-group
            // test — by construction it is the group's first row (mirrors
            // `drive_merge`'s group-open, which skips the first-row test).
            let mut net = sources[ex_src].get_weight(ex_row);
            states[ex_src].advance(&sources[ex_src]);

            // Fold one head's run into `net`: accumulate every remaining row equal
            // to the exemplar `(ex_pk, payload)` and advance past it. Applied to
            // the exemplar's own source (intra-source duplicates — a memtable run
            // is sorted but not necessarily consolidated) and, when it joined this
            // group, to the other head.
            let mut fold_run = |s: usize, net: &mut i64| {
                while states[s].is_valid() {
                    let pos = states[s].position;
                    if sources[s].get_pk_bytes(pos) != ex_pk
                        || row_cmp(schema, &sources[s], pos, &sources[ex_src], ex_row)
                            != Ordering::Equal
                    {
                        break;
                    }
                    *net += sources[s].get_weight(pos);
                    states[s].advance(&sources[s]);
                }
            };
            fold_run(ex_src, &mut net);
            if other_in_group {
                fold_run(1 - ex_src, &mut net);
            }
            if net != 0 {
                break Some((net, ex_src, ex_row));
            }
            // Ghost group (net weight 0 across both heads): walk to the next.
        };
        self.commit_emitted(emitted);
    }

    /// Approximate the number of rows remaining in this cursor (upper bound).
    /// Used by the adaptive-swap heuristic in join/semi-join operators.
    pub fn estimated_length(&self) -> usize {
        self.states.iter().map(|s| s.count.saturating_sub(s.position)).sum()
    }

    /// Raw column pointer for the current row, indexed by LOGICAL column index.
    ///
    /// Returns null for the PK column — use `current_key` instead.
    /// Returning only `pk_lo` for a 16-byte PK would silently truncate the
    /// high half regardless of backing source.
    pub fn col_ptr(&self, col_idx: usize, col_size: usize) -> *const u8 {
        if !self.valid {
            return ptr::null();
        }
        if self.schema.is_pk_col(col_idx) {
            return ptr::null();
        }
        let src = &self.sources[self.current_entry_idx];
        let row = self.current_row;

        match src {
            CursorSource::Shard(s) => s.col_ptr_by_logical(row, col_idx, col_size),
            CursorSource::Batch(b) => {
                // Map logical → payload index. A PK column has no payload slot
                // (the `is_pk_col` early-return above already covers it, but the
                // fallible map keeps the PK case structurally a null result).
                let Some(payload_idx) = self.schema.try_payload_idx(col_idx) else {
                    return ptr::null();
                };
                if payload_idx < b.num_payload_cols() {
                    b.get_col_ptr(row, payload_idx, col_size).as_ptr()
                } else {
                    ptr::null()
                }
            }
        }
    }

    /// Blob arena base pointer for the current row's source.
    pub fn blob_ptr(&self) -> *const u8 {
        if !self.valid {
            return ptr::null();
        }
        self.sources[self.current_entry_idx].blob_ptr()
    }

    /// Blob arena length for the current row's source.
    pub fn blob_len(&self) -> usize {
        if !self.valid {
            return 0;
        }
        self.sources[self.current_entry_idx].blob_slice().len()
    }

    /// Blob arena slice (bounds-carrying) for the current row's source.
    pub fn blob_slice(&self) -> &[u8] {
        if !self.valid { return &[]; }
        self.sources[self.current_entry_idx].blob_slice()
    }

    /// Copy the current row into `batch` with an explicit weight, leaving
    /// `batch`'s sorted/consolidated flags untouched.
    ///
    /// This is `Batch::append_row_from_source_bytes` applied at the cursor's
    /// current position: `CursorSource` implements `ColumnarSource`, so the shared
    /// appender does the weight/null/payload write (German-string blob relocation
    /// included) with no hand-rolled per-column loop here. The byte-form PK is
    /// correct at every width.
    pub(crate) fn copy_current_row_into(&self, batch: &mut Batch, weight: i64) {
        // `current_pk_bytes()` and the appender both index the positioned source,
        // which panics on an unpositioned cursor (empty `sources`). Keep the
        // no-op-on-`!valid` contract.
        if !self.valid {
            return;
        }
        // Stay flag-neutral: whether appending a row preserves sort/consolidation
        // depends on the caller's access pattern, which only the caller knows. The
        // catalog retraction helpers build in (PK, payload) order and keep the
        // batch's initial `true,true`; op_gather/op_reduce re-assert the flags they
        // need. The shared appender clears both unconditionally (its bulk callers
        // append out of order), so save and restore them across the delegation.
        let (sorted, consolidated) = (batch.sorted, batch.consolidated);
        batch.append_row_from_source_bytes(
            self.current_pk_bytes(),
            weight,
            &self.sources[self.current_entry_idx],
            self.current_row,
            None,
        );
        batch.sorted = sorted;
        batch.consolidated = consolidated;
    }

    /// Materialize all non-zero-weight rows in merge order into an owned
    /// `Rc<Batch>`.
    pub(crate) fn materialize(mut self) -> Rc<Batch> {
        if self.sources.len() == 1 && self.states[0].position == 0 {
            match &self.sources[0] {
                CursorSource::Batch(rc) if rc.consolidated => return Rc::clone(rc),
                CursorSource::Shard(rc) if !rc.has_ghosts => {
                    return Rc::new(rc.to_owned_batch(&self.schema));
                }
                _ => {}
            }
        }
        if !self.valid {
            return Rc::new(Batch::empty_with_schema(&self.schema));
        }

        let mut merge_order = DrainGuard::new();
        self.drain_sorted_into(0, &mut merge_order);
        if merge_order.is_empty() {
            return Rc::new(Batch::empty_with_schema(&self.schema));
        }
        let blob_cap = self.total_blob_len();
        let mut batch = super::batch::write_to_batch(
            &self.schema,
            merge_order.len(),
            blob_cap,
            |writer| self.scatter_drained_into(&merge_order, writer),
        );
        // The merge walk emits in (PK, payload) order with consolidated
        // weights; `write_to_batch` doesn't know that, so restore the flags.
        batch.sorted = true;
        batch.consolidated = true;
        Rc::new(batch)
    }

    /// Drain up to `max_rows` net rows in merge order into an owned `Batch`
    /// (sorted + consolidated, like `materialize`). Returns `None` once the
    /// cursor is exhausted. Chunk boundaries cannot split a (PK, payload)
    /// group: each drained entry is one fully-folded merge group.
    ///
    /// DDL backfills and uniqueness scans call this in a loop instead of
    /// `materialize` so peak memory is O(chunk) instead of O(relation).
    pub(crate) fn drain_chunk(&mut self, max_rows: usize) -> Option<Batch> {
        debug_assert!(max_rows > 0, "drain_chunk: 0 means unlimited in the drain helpers");
        if !self.valid {
            return None;
        }
        if let Some(batch) = self.drain_single_source(max_rows) {
            return if batch.count == 0 { None } else { Some(batch) };
        }
        // Merge path: `materialize`'s body with a row cap — same merge-order
        // collection, same column-major scatter, pooled arenas reused per chunk.
        let mut merge_order = DrainGuard::new();
        self.drain_sorted_into(max_rows, &mut merge_order);
        if merge_order.is_empty() {
            return None;
        }
        let mut batch = super::batch::write_to_batch(
            &self.schema,
            merge_order.len(),
            // Blob grows on demand inside DirectWriter; reserving
            // `total_blob_len()` here would re-reserve the whole relation's
            // blob arena for every chunk.
            0,
            |writer| self.scatter_drained_into(&merge_order, writer),
        );
        batch.sorted = true;
        batch.consolidated = true;
        Some(batch)
    }

    /// Bulk-drain a single-source cursor into an Batch, bypassing
    /// per-row iteration. Returns `None` for multi-source cursors or
    /// sources with ghosts, signaling the caller to fall back to row-at-a-time.
    ///
    /// `limit == 0` means drain all remaining rows.
    pub(crate) fn drain_single_source(
        &mut self,
        limit: usize,
    ) -> Option<super::batch::Batch> {
        if !self.valid || self.sources.len() != 1 {
            return None;
        }
        let state = &self.states[0];
        let start = state.position;
        let remaining = state.count - start;
        let row_count = if limit > 0 { remaining.min(limit) } else { remaining };
        let schema = &self.schema;

        let batch = match &self.sources[0] {
            CursorSource::Batch(b) => {
                // Batch sources may wrap unconsolidated intermediate batches;
                // propagate the source flags rather than asserting both.
                let end = start + row_count;
                let mut out = Batch::with_schema(*schema, row_count.max(1));
                out.append_batch(b, start, end);
                out.sorted = b.sorted;
                out.consolidated = b.consolidated;
                out
            }
            CursorSource::Shard(s) => {
                if s.has_ghosts {
                    return None;
                }
                s.slice_to_owned_batch(start, row_count, schema)
            }
        };

        // Advance position past the drained rows
        self.states[0].position = start + row_count;
        self.drive();
        Some(batch)
    }

    /// If this cursor is backed by exactly one in-memory `Batch` source,
    /// returns a `MemBatch` view over it and the current row position
    /// (`states[0].position`).  Returns `None` for multi-source or
    /// shard-backed cursors.
    ///
    /// Used by the bulk retraction path in the catalog to avoid per-row
    /// overhead when copying a contiguous range of rows from a single
    /// in-memory source.
    pub(crate) fn single_mem_batch(&self) -> Option<(MemBatch<'_>, usize)> {
        if !self.valid || self.sources.len() != 1 {
            return None;
        }
        match &self.sources[0] {
            CursorSource::Batch(b) => Some((b.as_mem_batch(), self.states[0].position)),
            CursorSource::Shard(_) => None,
        }
    }

    /// Sum of blob arena sizes across every source. Tight upper bound on the
    /// blob bytes a full drain can produce; callers use this to size the
    /// output blob arena.
    pub(crate) fn total_blob_len(&self) -> usize {
        self.sources.iter().map(|s| s.blob_slice().len()).sum()
    }

    /// Current row's `(entry_idx, row, weight)`. Unlike `push_current_row`, applies
    /// no validity / zero-weight filter — a caller scanning under a `valid` guard
    /// tags each row with an external group id and filters weight itself.
    pub(crate) fn current_row_loc(&self) -> (u32, u32, i64) {
        (self.current_entry_idx as u32, self.current_row as u32, self.current_weight)
    }

    /// Append the cursor's current `(entry_idx, row, weight)` to `buf` if the
    /// row is valid and has non-zero weight. Companion to `drain_sorted_into`
    /// for callers whose termination condition is custom (group-bounded or
    /// predicate-filtered).
    pub(crate) fn push_current_row(&self, buf: &mut Vec<(u32, u32, i64)>) {
        if !self.valid {
            return;
        }
        let w = self.current_weight;
        if w == 0 {
            return;
        }
        buf.push((
            self.current_entry_idx as u32,
            self.current_row as u32,
            w,
        ));
    }

    /// Walk the merge order and fill `out` with `(entry_idx, row_idx, weight)`
    /// for every row whose net consolidated weight is non-zero.  Drains up to
    /// `limit` rows (`limit == 0` means unlimited).  Clears `out` first.
    ///
    /// The buffered weight is the **net** weight produced by the merge —
    /// callers must not read it back from the exemplar source's stored weight,
    /// which is the per-source contribution and may not equal the net.
    /// Callers needing custom termination (group-bounded iteration, predicate
    /// filters) collect into a local `Vec` instead — this helper only supports
    /// row-count and full-cursor termination.
    pub(crate) fn drain_sorted_into(
        &mut self,
        limit: usize,
        out: &mut Vec<(u32, u32, i64)>,
    ) {
        if self.is_pk_unique {
            self.drain_sorted_into_pk_unique(limit, out);
        } else {
            with_payload_cmp!(self.schema, Self::drain_sorted_into_with, self, limit, out);
        }
    }

    /// Drain for all-PkUnique cursors: advances using `drive_pk_unique`.
    fn drain_sorted_into_pk_unique(
        &mut self,
        limit: usize,
        out: &mut Vec<(u32, u32, i64)>,
    ) {
        out.clear();
        let mut count = 0usize;
        while self.valid {
            if limit > 0 && count >= limit { break; }
            let w = self.current_weight;
            if w != 0 {
                out.push((
                    self.current_entry_idx as u32,
                    self.current_row as u32,
                    w,
                ));
                count += 1;
            }
            self.advance_pk_unique();
        }
    }

    #[inline]
    fn drain_sorted_into_with<RowCmp: RowComparator>(
        &mut self,
        limit: usize,
        out: &mut Vec<(u32, u32, i64)>,
        row_cmp: RowCmp,
    ) {
        out.clear();
        let mut count = 0usize;
        while self.valid {
            if limit > 0 && count >= limit {
                break;
            }
            let w = self.current_weight;
            if w != 0 {
                // src_idx is u32 because partitioned-table cursors can exceed
                // 256 entries; a u8 cast would wrap silently.
                out.push((
                    self.current_entry_idx as u32,
                    self.current_row as u32,
                    w,
                ));
                count += 1;
            }
            self.advance_with(row_cmp);
        }
    }

    pub(crate) fn scatter_drained_into(
        &self,
        rows: &[(u32, u32, i64)],
        writer: &mut super::merge::DirectWriter<'_>,
    ) {
        if rows.is_empty() { return; }
        let unified = self.unified_sources.get_or_init(|| {
            self.sources.iter()
                .map(|s| s.to_unified(&self.schema))
                .collect()
        });
        super::merge::scatter_unified_sources_with_weights(unified, rows, writer);
    }
}

/// Build a ReadCursor from in-memory batches + shard Rcs.
///
/// Both inputs are passed by `Rc`, so the cursor owns its data and has no
/// borrow lifetime — see the `CursorSource` doc comment.
pub(crate) fn create_read_cursor(
    batches: &[Rc<Batch>],
    shard_arcs: &[Rc<MappedShard>],
    schema: SchemaDescriptor,
) -> ReadCursor {
    let cap = batches.len() + shard_arcs.len();
    let mut sources = Vec::with_capacity(cap);
    let mut states = Vec::with_capacity(cap);

    for batch in batches {
        if batch.count > 0 {
            let count = batch.count;
            sources.push(CursorSource::Batch(Rc::clone(batch)));
            states.push(CursorState { position: 0, count });
        }
    }

    for shard in shard_arcs {
        if shard.count > 0 {
            let count = shard.count;
            let src = CursorSource::Shard(Rc::clone(shard));
            let mut state = CursorState { position: 0, count };
            state.skip_ghosts(&src);
            sources.push(src);
            states.push(state);
        }
    }

    ReadCursor::new(sources, states, schema)
}

// ---------------------------------------------------------------------------
// CursorHandle — owning wrapper around ReadCursor
// ---------------------------------------------------------------------------

/// Owns a `ReadCursor`.  Since the cursor now owns its data via `Rc`s in
/// each `CursorSource`, this is a thin newtype — the separate "owned
/// snapshots" vector that previously kept `Rc<Batch>` alive alongside
/// transmuted-to-`'static` `MemBatch` views is gone.
pub struct CursorHandle {
    pub(crate) cursor: ReadCursor,
}

impl CursorHandle {
    /// Build a CursorHandle from owned in-memory batches (no shards).
    ///
    /// Convenience wrapper used by test code.
    #[cfg(test)]
    pub(crate) fn from_owned(
        snapshots: &[Rc<Batch>],
        schema: crate::schema::SchemaDescriptor,
    ) -> CursorHandle {
        create_cursor_from_snapshots(snapshots, &[], schema)
    }

    /// Mutable access to the inner ReadCursor.
    pub(crate) fn cursor_mut(&mut self) -> &mut ReadCursor {
        &mut self.cursor
    }
}

/// Build a CursorHandle from Rust-owned snapshots + shard Rcs.
///
/// Each snapshot's `Rc<Batch>` and shard's `Rc<MappedShard>` is cloned into
/// the corresponding `CursorSource` entry, keeping the data alive for the
/// cursor's entire lifetime without any unsafe lifetime extension.
pub(crate) fn create_cursor_from_snapshots(
    snapshots: &[Rc<Batch>],
    shard_arcs: &[Rc<MappedShard>],
    schema: SchemaDescriptor,
) -> CursorHandle {
    let cursor = create_read_cursor(snapshots, shard_arcs, schema);
    CursorHandle { cursor }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{SchemaColumn, SchemaDescriptor, type_code};
    use crate::test_support::wide_pk_3xu64_schema;

    fn make_schema_i64() -> SchemaDescriptor {
        SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U128, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0],
        )
    }

    /// Build an `Rc<Batch>` with i64-payload rows.  Tests pre-sort their
    /// inputs and have at most one row per (PK, payload), so we mark the
    /// batch as sorted+consolidated.
    fn make_batch(rows: &[(u128, i64, i64)]) -> Rc<Batch> {
        let schema = make_schema_i64();
        let mut b = Batch::with_schema(schema, rows.len().max(1));
        for &(pk, w, val) in rows {
            b.extend_pk(pk);
            b.extend_weight(&w.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &val.to_le_bytes());
            b.count += 1;
        }
        b.sorted = true;
        b.consolidated = true;
        Rc::new(b)
    }

    fn scan_all(cursor: &mut ReadCursor) -> Vec<(u64, u64, i64)> {
        let mut rows = Vec::new();
        while cursor.valid {
            rows.push((
                cursor.current_key as u64,
                (cursor.current_key >> 64) as u64,
                cursor.current_weight,
            ));
            cursor.advance();
        }
        rows
    }

    #[test]
    fn test_empty_cursor() {
        let schema = make_schema_i64();
        let cursor = create_read_cursor(&[], &[], schema);
        assert!(!cursor.valid);
    }

    #[test]
    fn test_single_batch_scan() {
        let schema = make_schema_i64();
        let batch = make_batch(&[(1, 1, 10), (2, 1, 20), (3, 1, 30)]);
        let mut cursor = create_read_cursor(&[batch], &[], schema);
        let rows = scan_all(&mut cursor);
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0], (1, 0, 1));
        assert_eq!(rows[1], (2, 0, 1));
        assert_eq!(rows[2], (3, 0, 1));
    }

    #[test]
    fn test_two_batch_merge() {
        let schema = make_schema_i64();
        let b1 = make_batch(&[(1, 1, 10), (3, 1, 30)]);
        let b2 = make_batch(&[(2, 1, 20), (4, 1, 40)]);
        let mut cursor = create_read_cursor(&[b1, b2], &[], schema);
        let rows = scan_all(&mut cursor);
        assert_eq!(rows.len(), 4);
        assert_eq!(rows[0].0, 1);
        assert_eq!(rows[1].0, 2);
        assert_eq!(rows[2].0, 3);
        assert_eq!(rows[3].0, 4);
    }

    #[test]
    fn test_ghost_elimination_across_sources() {
        let schema = make_schema_i64();
        // Batch 1: pk=5 val=50 w=+1, pk=10 val=100 w=+1
        let b1 = make_batch(&[(5, 1, 50), (10, 1, 100)]);
        // Batch 2: pk=5 val=50 w=-1 (retraction)
        let b2 = make_batch(&[(5, -1, 50)]);
        let mut cursor = create_read_cursor(&[b1, b2], &[], schema);
        let rows = scan_all(&mut cursor);
        // pk=5 cancelled (w=+1-1=0), only pk=10 survives
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0], (10, 0, 1));
    }

    #[test]
    fn test_seek() {
        let schema = make_schema_i64();
        let batch = make_batch(&[(1, 1, 10), (5, 1, 50), (10, 1, 100)]);
        let mut cursor = create_read_cursor(&[batch], &[], schema);

        // Seek to pk >= 5. OPK for a U128 PK is the value's big-endian bytes.
        cursor.seek_bytes(&5u128.to_be_bytes());
        assert!(cursor.valid);
        assert_eq!(cursor.current_key, 5);

        // Seek to pk >= 7 → lands on 10
        cursor.seek_bytes(&7u128.to_be_bytes());
        assert!(cursor.valid);
        assert_eq!(cursor.current_key, 10);

        // Seek past end
        cursor.seek_bytes(&100u128.to_be_bytes());
        assert!(!cursor.valid);
    }

    fn make_schema_compound_u64() -> SchemaDescriptor {
        // PK = (col0:U64, col1:U64); payload = I64. Stored first-column-major.
        SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0, 1],
        )
    }

    /// OPK bytes for a `(U64, U64)` compound PK: each column big-endian,
    /// concatenated in pk-list order (the at-rest form).
    fn compound_pk_bytes(col0: u64, col1: u64) -> [u8; 16] {
        let mut b = [0u8; 16];
        b[..8].copy_from_slice(&col0.to_be_bytes());
        b[8..].copy_from_slice(&col1.to_be_bytes());
        b
    }

    /// A compound `(U64, U64)` PK's raw u128 order is last-column-major while
    /// storage (OPK memcmp) sorts first-column-major. `seek_bytes` must land on
    /// the exact row, not the u128-nearest one.
    #[test]
    fn test_seek_compound_pk_lands_on_exact_row() {
        let schema = make_schema_compound_u64();
        // Canonical (first-column-major) storage order: (1,5) then (2,3).
        // As u128 the order is reversed: pack(2,3) < pack(1,5).
        let mut b = Batch::with_schema(schema, 2);
        for &(c0, c1, v) in &[(1u64, 5u64, 100i64), (2, 3, 200)] {
            b.extend_pk_bytes(&compound_pk_bytes(c0, c1));
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &v.to_le_bytes());
            b.count += 1;
        }
        b.sorted = true;
        b.consolidated = true;
        let mut cursor = create_read_cursor(&[Rc::new(b)], &[], schema);

        cursor.seek_bytes(&compound_pk_bytes(2, 3));
        assert!(cursor.valid);
        assert_eq!(cursor.current_pk_bytes(), &compound_pk_bytes(2, 3));

        // Seek the first group too.
        cursor.seek_bytes(&compound_pk_bytes(1, 5));
        assert!(cursor.valid);
        assert_eq!(cursor.current_pk_bytes(), &compound_pk_bytes(1, 5));
    }

    fn make_schema_signed_i64() -> SchemaDescriptor {
        // PK = single I64 column; payload = I64.
        SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::I64, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0],
        )
    }

    /// OPK bytes for a single I64 PK column (big-endian, sign bit flipped).
    fn i64_opk(v: i64) -> [u8; 8] {
        let mut out = [0u8; 8];
        gnitz_wire::encode_pk_column(&v.to_le_bytes(), type_code::I64, &mut out);
        out
    }

    /// A signed single-column PK's negative keys sort *after* positives in raw
    /// u128 order, while OPK (BE + sign-bit flip) sorts them first. `seek_bytes`
    /// on the OPK key must land on the matching row.
    #[test]
    fn test_seek_signed_pk_lands_on_negative_row() {
        let schema = make_schema_signed_i64();
        // Storage (signed) order: -3, -1, 2.
        let mut b = Batch::with_schema(schema, 3);
        for &(pk, v) in &[(-3i64, 30i64), (-1, 10), (2, 20)] {
            b.extend_pk_bytes(&i64_opk(pk));
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &v.to_le_bytes());
            b.count += 1;
        }
        b.sorted = true;
        b.consolidated = true;
        let mut cursor = create_read_cursor(&[Rc::new(b)], &[], schema);

        cursor.seek_bytes(&i64_opk(-1));
        assert!(cursor.valid);
        assert_eq!(cursor.current_pk_bytes(), &i64_opk(-1));

        cursor.seek_bytes(&i64_opk(-3));
        assert!(cursor.valid);
        assert_eq!(cursor.current_pk_bytes(), &i64_opk(-3));

        // A positive key still lands correctly.
        cursor.seek_bytes(&i64_opk(2));
        assert!(cursor.valid);
        assert_eq!(cursor.current_pk_bytes(), &i64_opk(2));
    }

    #[test]
    fn test_same_pk_different_payload_ordering() {
        let schema = make_schema_i64();
        // Two entries with same PK but different payloads
        let b1 = make_batch(&[(5, 1, 200)]);
        let b2 = make_batch(&[(5, 1, 100)]);
        let mut cursor = create_read_cursor(&[b1, b2], &[], schema);
        let rows = scan_all(&mut cursor);
        // Both survive, sorted by payload (100 < 200)
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0], (5, 0, 1)); // payload=100
        assert_eq!(rows[1], (5, 0, 1)); // payload=200
    }

    /// Drain that READS THE PAYLOAD VALUE of each row (logical col 1, the
    /// I64 payload), unlike `scan_all` which only captures PK + weight. The
    /// payload read is what gives the root-adjacency pin its teeth: a PK-only
    /// merge leaks an *extra* row whose payload value distinguishes it from
    /// the survivor.
    fn scan_all_with_val(cursor: &mut ReadCursor) -> Vec<(u64, i64, i64)> {
        let mut rows = Vec::new();
        while cursor.valid {
            let p = cursor.col_ptr(1, 8);
            assert!(!p.is_null(), "payload col_ptr null for a valid cursor row");
            let val = i64::from_le_bytes(
                unsafe { std::slice::from_raw_parts(p, 8) }.try_into().unwrap(),
            );
            rows.push((cursor.current_key as u64, cursor.current_weight, val));
            cursor.advance();
        }
        rows
    }

    /// PIN — root adjacency of equal-(PK, payload) rows across cursor sources.
    /// Three single-row batches, all PK=5: b1/b3 carry the *same* payload
    /// (val=100) with opposite weights, b2 carries a different payload
    /// (val=200) and sits between them in source order. Each batch is
    /// (PK, payload)-sorted, but the matching val=100 rows are NOT adjacent in
    /// source order.
    ///
    /// The cursor's N-way merge heap MUST order by (PK, payload) so the two
    /// val=100 rows reach the fold root consecutively and their +1/-1 weights
    /// cancel via ghost elimination; only val=200 survives. A PK-only heap
    /// `less` (dropping the payload tiebreak) leaves the three same-PK rows
    /// unordered among themselves, the fold breaks on the first payload
    /// mismatch, and the +1/-1 pair never folds — surfacing a spurious row.
    /// We assert on the PAYLOAD VALUE (via `scan_all_with_val`) so the leaked
    /// row cannot hide behind a matching PK/weight.
    #[test]
    fn test_cursor_same_pk_nonadjacent_payload_fold() {
        let schema = make_schema_i64();
        let b1 = make_batch(&[(5, 1, 100)]);
        let b2 = make_batch(&[(5, 1, 200)]);
        let b3 = make_batch(&[(5, -1, 100)]);

        let mut cursor = create_read_cursor(&[b1, b2, b3], &[], schema);
        let rows = scan_all_with_val(&mut cursor);
        assert_eq!(
            rows,
            vec![(5, 1, 200)],
            "val=100 +1/-1 pair must ghost-cancel; only (pk=5, w=1, val=200) survives"
        );
    }

    #[test]
    fn test_weight_accumulation_across_sources() {
        let schema = make_schema_i64();
        // Same (PK, payload) in two batches: weights should sum
        let b1 = make_batch(&[(5, 3, 50)]);
        let b2 = make_batch(&[(5, 7, 50)]);
        let mut cursor = create_read_cursor(&[b1, b2], &[], schema);
        let rows = scan_all(&mut cursor);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0], (5, 0, 10)); // 3 + 7 = 10
    }

    #[test]
    fn test_drain_single_source_full() {
        let schema = make_schema_i64();
        let batch = make_batch(&[(1, 1, 10), (2, 1, 20), (3, 1, 30)]);
        let mut cursor = create_read_cursor(&[batch], &[], schema);

        let result = cursor.drain_single_source(0);
        assert!(result.is_some());
        let out = result.unwrap();
        assert_eq!(out.count, 3);
        assert_eq!(out.get_pk(0), 1);
        assert_eq!(out.get_pk(2), 3);
        assert!(!cursor.valid);
    }

    #[test]
    fn test_drain_single_source_with_limit() {
        let schema = make_schema_i64();
        let batch = make_batch(&[(1, 1, 10), (2, 1, 20), (3, 1, 30), (4, 1, 40)]);
        let mut cursor = create_read_cursor(&[batch], &[], schema);

        // Drain first 2
        let out1 = cursor.drain_single_source(2).unwrap();
        assert_eq!(out1.count, 2);
        assert_eq!(out1.get_pk(0), 1);
        assert_eq!(out1.get_pk(1), 2);
        assert!(cursor.valid);

        // Drain remaining 2
        let out2 = cursor.drain_single_source(0).unwrap();
        assert_eq!(out2.count, 2);
        assert_eq!(out2.get_pk(0), 3);
        assert_eq!(out2.get_pk(1), 4);
        assert!(!cursor.valid);
    }

    #[test]
    fn test_drain_multi_source_returns_none() {
        let schema = make_schema_i64();
        let b1 = make_batch(&[(1, 1, 10)]);
        let b2 = make_batch(&[(2, 1, 20)]);
        let mut cursor = create_read_cursor(&[b1, b2], &[], schema);
        assert!(cursor.drain_single_source(0).is_none());
    }

    #[test]
    fn test_col_ptr_pk_returns_null() {
        // PK (logical col 0, pk_index=0) must always return null — callers read
        // the PK through current_key instead.
        let schema = make_schema_i64();
        let batch = make_batch(&[(42, 1, 99)]);
        let cursor = create_read_cursor(&[batch], &[], schema);
        assert!(cursor.valid);
        let pk_index = cursor.schema.pk_indices()[0] as usize; // 0
        let ptr = cursor.col_ptr(pk_index, 16);
        assert!(ptr.is_null(), "col_ptr for PK index must return null");
    }

    #[test]
    fn test_col_ptr_payload_returns_valid_pointer() {
        // Payload col at logical index 1 must return a non-null pointer with
        // the correct value.
        let schema = make_schema_i64();
        let batch = make_batch(&[(7, 1, 1234)]);
        let cursor = create_read_cursor(&[batch], &[], schema);
        assert!(cursor.valid);
        let ptr = cursor.col_ptr(1, 8); // logical col 1 = i64 payload
        assert!(!ptr.is_null(), "col_ptr for payload col must not be null");
        let val = i64::from_le_bytes(unsafe { *(ptr as *const [u8; 8]) });
        assert_eq!(val, 1234);
    }

    #[test]
    fn test_col_ptr_invalid_cursor_returns_null() {
        let schema = make_schema_i64();
        let cursor = create_read_cursor(&[], &[], schema);
        assert!(!cursor.valid);
        assert!(cursor.col_ptr(1, 8).is_null());
    }

    #[test]
    fn test_estimated_length_reflects_remaining() {
        let schema = make_schema_i64();
        let batch = make_batch(&[(1, 1, 10), (2, 1, 20), (3, 1, 30)]);
        let mut cursor = create_read_cursor(&[batch], &[], schema);
        assert_eq!(cursor.estimated_length(), 3);
        cursor.advance();
        assert_eq!(cursor.estimated_length(), 2);
        cursor.advance();
        assert_eq!(cursor.estimated_length(), 1);
        cursor.advance();
        assert_eq!(cursor.estimated_length(), 0);
    }

    #[test]
    fn test_current_key() {
        let schema = make_schema_i64();
        let expected = (0xBEEFu128 << 64) | 0xDEADu128;
        let batch = make_batch(&[(expected, 1, 0)]);
        let cursor = create_read_cursor(&[batch], &[], schema);
        assert!(cursor.valid);
        assert_eq!(cursor.current_key, expected);
    }

    /// Cursor backed by a shard whose PK region is Constant-encoded (single-row
    /// shard).  Previously `to_unified` returned `None` for this, falling back
    /// to the row-major scatter.  Now the column-major path handles it directly.
    #[test]
    fn test_scatter_constant_pk_shard() {
        crate::foundation::posix_io::raise_fd_limit_for_tests();
        let dir = tempfile::tempdir().unwrap();
        let schema = make_schema_i64();

        // PK region holds OPK (order-preserving big-endian) bytes at rest.
        let pk_bytes: Vec<u8> = 42u128.to_be_bytes().to_vec();
        let weights: Vec<i64> = vec![1i64];
        let null_bm: Vec<u64> = vec![0u64];
        let col_data: Vec<i64> = vec![999i64];
        let blob: Vec<u8> = Vec::new();
        let regions: Vec<(*const u8, usize)> = vec![
            (pk_bytes.as_ptr(), pk_bytes.len()),
            (weights.as_ptr() as *const u8, weights.len() * 8),
            (null_bm.as_ptr() as *const u8, null_bm.len() * 8),
            (col_data.as_ptr() as *const u8, col_data.len() * 8),
            (blob.as_ptr(), blob.len()),
        ];
        let image = super::super::shard_file::build_shard_image(0, 1, &regions);
        let path = dir.path().join("const_pk.db");
        std::fs::write(&path, &image).unwrap();
        let cpath = std::ffi::CString::new(path.to_str().unwrap()).unwrap();
        let shard = Rc::new(
            super::super::shard_reader::MappedShard::open(&cpath, &schema, false).unwrap(),
        );

        let cursor = create_read_cursor(&[], &[shard], schema);
        let result = cursor.materialize();

        assert_eq!(result.count, 1);
        assert_eq!(result.get_pk(0), 42u128);
    }

    /// Cursor with more than 16 entries (formerly above `MAX_INLINE_BATCH_SOURCES`)
    /// previously fell through to the row-major scatter.  Now the column-major
    /// path handles any number of sources.
    #[test]
    fn seek_bytes_lands_on_lower_bound_narrow() {
        // Narrow single-PK (U128, stride 16): seek_bytes lands on the first row
        // whose PK >= the (OPK) key — the lower bound. OPK for a U128 PK is the
        // value's big-endian bytes.
        let schema = make_schema_i64();
        let keys: &[u128] = &[10, 20, 30, 40];
        let batch = make_batch(&[(10u128, 1, 100), (20, 1, 200), (30, 1, 300), (40, 1, 400)]);
        let probes: &[u128] = &[0u128, 5, 10, 15, 20, 25, 30, 35, 40, 41];
        for &key in probes {
            let mut c = create_read_cursor(&[Rc::clone(&batch)], &[], schema);
            c.seek_bytes(&key.to_be_bytes());

            // Independent oracle: first stored key >= probe.
            let expected = keys.iter().copied().find(|&k| k >= key);
            match expected {
                Some(k) => {
                    assert!(c.valid, "key={key} should land on {k}");
                    assert_eq!(c.current_key, k, "key={key}");
                    // current_pk_bytes is OPK (BE) of the native value.
                    assert_eq!(c.current_pk_bytes(), &k.to_be_bytes()[..], "key={key}");
                }
                None => assert!(!c.valid, "key={key} past end must be invalid"),
            }
        }
    }

    /// Drive one reused cursor over `sources` through `probes`, asserting each
    /// `advance_to` lands exactly where a from-scratch `seek_bytes` on a fresh
    /// cursor would. The fresh cursor is the oracle for any probe order — a
    /// strict-forward step (the in-place loser-tree gallop), an `Equal` re-seek
    /// of the current key, or a backward step (both the rebuild fallback). The
    /// reused position must never change the landing — including the emitted
    /// weight, which pins cross-source ghost folding.
    fn assert_advance_to_matches_seek_oracle(
        schema: SchemaDescriptor,
        sources: &[Rc<Batch>],
        probes: &[u128],
    ) {
        let n = sources.len();
        let mut adv = create_read_cursor(sources, &[], schema);
        for &key in probes {
            adv.advance_to(&key.to_be_bytes());
            let mut fresh = create_read_cursor(sources, &[], schema);
            fresh.seek_bytes(&key.to_be_bytes());
            assert_eq!(adv.valid, fresh.valid, "n_src={n} key={key}");
            if adv.valid {
                assert_eq!(adv.current_key, fresh.current_key, "n_src={n} key={key}");
                assert_eq!(adv.current_pk_bytes(), fresh.current_pk_bytes(), "n_src={n} key={key}");
                assert_eq!(adv.current_weight, fresh.current_weight, "n_src={n} key={key}");
            }
        }
    }

    /// `advance_to` (forward-only, position-seeded) lands on the identical row a
    /// from-scratch `seek_bytes` would, across a monotone ascending probe sweep —
    /// for both a single-source cursor (rebuild fallback) and a multi-source
    /// cursor (the in-place loser-tree gallop). The sweep also re-seeks the
    /// current key (probe `20` after landing on `20` from probe `15`), exercising
    /// the `Equal` rebuild fallback on the same cursor.
    #[test]
    fn advance_to_lands_like_seek_bytes_monotone() {
        let schema = make_schema_i64();
        let b0 = make_batch(&[(10u128, 1, 100), (30, 1, 300), (50, 1, 500), (70, 1, 700)]);
        let b1 = make_batch(&[(20u128, 1, 200), (40, 1, 400), (60, 1, 600)]);
        // Monotone ascending: below-min, present, absent-between, above-max.
        let probes: &[u128] = &[0, 10, 15, 20, 35, 50, 55, 70, 71];
        assert_advance_to_matches_seek_oracle(schema, &[Rc::clone(&b0)], probes);
        assert_advance_to_matches_seek_oracle(schema, &[b0, b1], probes);
    }

    /// A strict-forward seek must skip a source already past the probe key while
    /// galloping a lagging source up to it — the loser-tree maintenance touches
    /// only laggards. After emitting `5`, `b_lag`'s head is `30` and `b_ahead`'s
    /// is `50`; seeking `40` gallops `b_lag` (30 → 90) while leaving `b_ahead`
    /// (50) untouched, then lands on `50`. The trailing `95` then exhausts the
    /// last live source (the `pop_top` branch).
    #[test]
    fn advance_to_forward_skips_ahead_source() {
        let schema = make_schema_i64();
        let b_lag = make_batch(&[(5u128, 1, 50), (30, 1, 300), (90, 1, 900)]);
        let b_ahead = make_batch(&[(50u128, 1, 500), (60, 1, 600), (70, 1, 700)]);
        assert_advance_to_matches_seek_oracle(schema, &[b_lag, b_ahead], &[40, 55, 65, 95]);
    }

    /// Interleaved forward/backward sweep on one reused multi-source cursor: the
    /// forward steps take the in-place gallop, the backward / current-key steps
    /// the rebuild fallback. Each landing still matches the from-scratch oracle —
    /// the fast path must leave the cursor in a state the rebuild can recover.
    #[test]
    fn advance_to_interleaved_forward_backward() {
        let schema = make_schema_i64();
        let b0 = make_batch(&[(10u128, 1, 100), (30, 1, 300), (50, 1, 500), (70, 1, 700)]);
        let b1 = make_batch(&[(20u128, 1, 200), (40, 1, 400), (60, 1, 600)]);
        // up, up, up, BACK, up, BACK, up, BACK.
        assert_advance_to_matches_seek_oracle(schema, &[b0, b1], &[0, 30, 50, 20, 60, 10, 70, 5]);
    }

    /// A forward gallop that lands on a ghost group (PK nets to 0 across two runs
    /// at the seek target) must fold it and land on the first *live* row past it,
    /// exactly as a from-scratch `seek_bytes` would. Pins the seek-phase /
    /// ghost-fold handoff.
    #[test]
    fn advance_to_forward_lands_past_straddling_ghost() {
        let schema = make_schema_i64();
        // PK=200 nets to 0: +1 from b_a, -1 from b_b. Live trace = {10, 20, 400}.
        let b_a = make_batch(&[(10u128, 1, 100), (200, 1, 2000), (400, 1, 4000)]);
        let b_b = make_batch(&[(20u128, 1, 200), (200, -1, 2000)]);
        // After emitting 10, b_a head=200, b_b head=20. Seeking 150 gallops b_b
        // (20 → 200) past its live row, positions both at the ghost 200, folds it
        // to zero, and must land on the first live row past it (400).
        let mut adv = create_read_cursor(&[Rc::clone(&b_a), Rc::clone(&b_b)], &[], schema);
        adv.advance_to(&(150u128).to_be_bytes());
        assert!(adv.valid, "must land on a live row past the ghost");
        assert_eq!(adv.current_key, 400, "ghost 200 must be folded; first live row is 400");
        assert_eq!(adv.current_weight, 1, "landed row's net weight");
        // The same landing also matches the from-scratch oracle.
        assert_advance_to_matches_seek_oracle(schema, &[b_a, b_b], &[150]);
    }

    /// A source whose `lower_bound(key)` is its end exhausts mid-sweep, forcing
    /// the seek-phase `pop_top` branch; the remaining source must still merge
    /// correctly, and a later forward seek over the now-drained heap must
    /// invalidate cleanly (the fast path no-ops on an empty tree).
    #[test]
    fn advance_to_forward_exhausts_source_mid_sweep() {
        let schema = make_schema_i64();
        let b_short = make_batch(&[(10u128, 1, 100), (20, 1, 200)]); // max 20
        let b_long = make_batch(&[(10u128, 1, 100), (50, 1, 500), (90, 1, 900)]);
        // 40 gallops b_short to its end (pop_top), leaving b_long to emit 50;
        // 60 → 90; 100 → exhausted (fast path over an empty heap).
        assert_advance_to_matches_seek_oracle(schema, &[b_short, b_long], &[40, 60, 100]);
    }

    #[test]
    fn test_scatter_many_sources_beyond_old_cap() {
        let schema = make_schema_i64();
        let n = 33usize;
        let batches: Vec<Rc<super::super::batch::Batch>> = (0..n)
            .map(|i| make_batch(&[(i as u128, 1i64, (i * 100) as i64)]))
            .collect();
        let cursor = create_read_cursor(&batches, &[], schema);
        let result = cursor.materialize();

        assert_eq!(result.count, n);
        for i in 0..n {
            assert_eq!(result.get_pk(i), i as u128);
        }
    }

    fn make_compound_pk_schema() -> SchemaDescriptor {
        // 2×U64 compound PK (stride 16): col_A in PK bytes 0..8, col_B in 8..16.
        SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0, 1],
        )
    }

    /// OPK bytes for a `(U64, U64)` compound PK: col_A big-endian ++ col_B
    /// big-endian (the at-rest form). memcmp of these equals (col_A, col_B)
    /// order, the inverse of the raw-u128 (col_B, col_A) order.
    fn compound_opk(a: u64, b: u64) -> [u8; 16] {
        let mut k = [0u8; 16];
        k[..8].copy_from_slice(&a.to_be_bytes());
        k[8..].copy_from_slice(&b.to_be_bytes());
        k
    }

    /// Multi-source merge over a compound `(col_A, col_B)` PK must order by
    /// `(col_A, col_B)`. As a raw `u128`, col_B occupies the high 64 bits, so
    /// the integer-comparison shortcut would (wrongly) order by `(col_B,
    /// col_A)`. Chosen rows make the two orderings disagree.
    #[test]
    fn test_compound_pk_multi_source_merge_order() {
        let schema = make_compound_pk_schema();
        let make = |a: u64, b: u64, val: i64| -> Rc<Batch> {
            let mut bt = Batch::with_schema(schema, 1);
            bt.extend_pk_bytes(&compound_opk(a, b));
            bt.extend_weight(&1i64.to_le_bytes());
            bt.extend_null_bmp(&0u64.to_le_bytes());
            bt.extend_col(0, &val.to_le_bytes());
            bt.count += 1;
            bt.sorted = true;
            bt.consolidated = true;
            Rc::new(bt)
        };
        // (1,2) precedes (2,1) by (col_A, col_B); the raw-u128 order is reversed.
        let b1 = make(1, 2, 100);
        let b2 = make(2, 1, 200);
        let mut cursor = create_read_cursor(&[b1, b2], &[], schema);

        // current_key for a stride-16 PK is widen_pk_be of the OPK bytes: the
        // BE reading places col_A in the high 64 bits and col_B in the low.
        let mut emitted = Vec::new();
        while cursor.valid {
            let a = (cursor.current_key >> 64) as u64;
            let b = cursor.current_key as u64;
            emitted.push((a, b));
            cursor.advance();
        }
        assert_eq!(
            emitted,
            vec![(1u64, 2u64), (2u64, 1u64)],
            "compound PK must order by (col_A, col_B), not raw u128 (col_B, col_A)",
        );
    }

    /// OPK bytes for a 3×U64 compound PK: each column big-endian, concatenated
    /// in pk-list order (the at-rest form).
    fn pk3(a: u64, b: u64, c: u64) -> [u8; 24] {
        let mut k = [0u8; 24];
        k[0..8].copy_from_slice(&a.to_be_bytes());
        k[8..16].copy_from_slice(&b.to_be_bytes());
        k[16..24].copy_from_slice(&c.to_be_bytes());
        k
    }

    fn make_wide_batch(rows: &[([u8; 24], i64, i64)]) -> Rc<Batch> {
        let schema = wide_pk_3xu64_schema();
        let mut bt = Batch::with_schema(schema, rows.len().max(1));
        for (pk, w, val) in rows {
            bt.extend_pk_bytes(pk);
            bt.extend_weight(&w.to_le_bytes());
            bt.extend_null_bmp(&0u64.to_le_bytes());
            bt.extend_col(0, &val.to_le_bytes());
            bt.count += 1;
        }
        bt.sorted = true;
        bt.consolidated = true;
        Rc::new(bt)
    }

    /// seek_bytes over a 24-byte PK whose third column lies past the 16-byte
    /// heap prefix. Two rows share their low-16 prefix `(col_0, col_1)=(1,0)`
    /// and differ only in `col_2`; the prefix tie-break must keep them ordered.
    #[test]
    fn seek_bytes_wide_pk_24_byte_stride() {
        let schema = wide_pk_3xu64_schema();
        let pk_a = pk3(0, 0, 0);
        let pk_b = pk3(1, 0, 0);
        let pk_c = pk3(1, 0, 1); // differs from pk_b only past byte 16
        let batch = make_wide_batch(&[(pk_a, 1, 100), (pk_b, 1, 200), (pk_c, 1, 300)]);

        let mut cursor = create_read_cursor(&[batch], &[], schema);
        cursor.seek_bytes(&pk_b);
        assert!(cursor.valid);
        assert_eq!(cursor.current_pk_bytes(), &pk_b[..]);
        assert_eq!(cursor.current_weight, 1);

        cursor.advance();
        assert!(cursor.valid);
        assert_eq!(cursor.current_pk_bytes(), &pk_c[..],
            "the third row, distinguished only in its trailing 8 bytes, must follow");
    }

    /// Two wide PKs that collide on their low-16 prefix `(col_0, col_1)=(1,1)`,
    /// differ only in `col_2` (100 vs 200) and carry EQUAL payload must survive
    /// as distinct outputs with their own weights — never folded into one
    /// summed group. Regression for the `eq_payload` PK-equality term: a
    /// payload-only `eq_payload` would collapse them.
    #[test]
    fn wide_pk_prefix_collision_not_consolidated() {
        let schema = wide_pk_3xu64_schema();
        let pk_x = pk3(1, 1, 100);
        let pk_y = pk3(1, 1, 200);
        // Two sources so the merge heap, not a pre-sorted single batch, drives
        // the group fold.
        let b1 = make_wide_batch(&[(pk_x, 3, 42)]);
        let b2 = make_wide_batch(&[(pk_y, 5, 42)]); // identical payload (42)
        let mut cursor = create_read_cursor(&[b1, b2], &[], schema);

        let mut emitted: Vec<([u8; 24], i64)> = Vec::new();
        while cursor.valid {
            let mut k = [0u8; 24];
            k.copy_from_slice(cursor.current_pk_bytes());
            emitted.push((k, cursor.current_weight));
            cursor.advance();
        }
        assert_eq!(emitted.len(), 2, "distinct wide PKs must not be folded");
        assert_eq!(emitted[0], (pk_x, 3));
        assert_eq!(emitted[1], (pk_y, 5));
    }

    /// Secondary-index shape `(U64 indexed_col, I64 source_pk)` (stride 16).
    /// `seek_first_positive_with_prefix` on the leading column must return ALL
    /// rows sharing that prefix, including ones whose signed suffix is negative.
    /// Zero-padding the suffix (the bug) decodes to 0 and skips negatives.
    #[test]
    fn seek_first_positive_with_prefix_includes_negative_suffix() {
        let schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::I64, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0, 1],
        );
        assert_eq!(schema.pk_stride(), 16);
        // OPK: U64 column big-endian; I64 column big-endian with sign bit flipped.
        let mk = |a: u64, b: i64| -> [u8; 16] {
            let mut k = [0u8; 16];
            k[..8].copy_from_slice(&a.to_be_bytes());
            gnitz_wire::encode_pk_column(&b.to_le_bytes(), type_code::I64, &mut k[8..]);
            k
        };
        // Sorted by compare_pk_bytes: col0 asc, col1 signed asc (negatives first).
        let rows = [
            (mk(1, -5), 1i64),
            (mk(1, -1), 1),
            (mk(1, 3), 1),
            (mk(2, -9), 1),
        ];
        let mut b = Batch::with_schema(schema, rows.len());
        for (pk, val) in &rows {
            b.extend_pk_bytes(pk);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &val.to_le_bytes());
            b.count += 1;
        }
        b.sorted = true;
        b.consolidated = true;
        let batch = Rc::new(b);
        let mut cursor = create_read_cursor(&[batch], &[], schema);

        // Prefix is the OPK image of the leading U64 column (big-endian).
        let prefix = 1u64.to_be_bytes();
        let mut found: Vec<[u8; 16]> = Vec::new();
        if cursor.seek_first_positive_with_prefix(&prefix) {
            loop {
                let mut k = [0u8; 16];
                k.copy_from_slice(cursor.current_pk_bytes());
                found.push(k);
                cursor.advance();
                if !cursor.walk_to_positive_with_prefix(&prefix) { break; }
            }
        }
        assert_eq!(found.len(), 3, "negative-suffix rows must not be skipped");
        assert_eq!(found[0], mk(1, -5));
        assert_eq!(found[1], mk(1, -1));
        assert_eq!(found[2], mk(1, 3));
    }

    /// Drain a cursor via repeated `drain_chunk(n)` calls, concatenating
    /// `(pk, weight, val)` rows. Asserts every chunk respects the row cap,
    /// is non-empty, and carries the sorted+consolidated flags.
    fn drain_chunks(cursor: &mut ReadCursor, n: usize) -> Vec<(u128, i64, i64)> {
        let mut rows = Vec::new();
        while let Some(chunk) = cursor.drain_chunk(n) {
            assert!(chunk.count <= n, "chunk overflow: {} > {}", chunk.count, n);
            assert!(chunk.count > 0, "drain_chunk returned an empty Some chunk");
            assert!(chunk.sorted && chunk.consolidated);
            for row in 0..chunk.count {
                let val = i64::from_le_bytes(
                    chunk.get_col_ptr(row, 0, 8).try_into().unwrap());
                rows.push((chunk.get_pk(row), chunk.get_weight(row), val));
            }
        }
        assert!(cursor.drain_chunk(n).is_none(), "exhausted cursor must stay exhausted");
        rows
    }

    fn materialize_rows(sources: &[Rc<Batch>]) -> Vec<(u128, i64, i64)> {
        let cursor = create_read_cursor(sources, &[], make_schema_i64());
        let batch = cursor.materialize();
        (0..batch.count)
            .map(|row| {
                let val = i64::from_le_bytes(
                    batch.get_col_ptr(row, 0, 8).try_into().unwrap());
                (batch.get_pk(row), batch.get_weight(row), val)
            })
            .collect()
    }

    /// Single-source fast path: exact multiple of the chunk size, with
    /// remainder, single oversized chunk — all equal to `materialize`.
    #[test]
    fn drain_chunk_single_source_matches_materialize() {
        let rows: Vec<(u128, i64, i64)> =
            (1..=5).map(|i| (i as u128, 1i64, (i * 10) as i64)).collect();
        let batch = make_batch(&rows);
        let expected = materialize_rows(&[Rc::clone(&batch)]);
        for chunk_rows in [1, 2, 5, 100] {
            let mut cursor = create_read_cursor(&[Rc::clone(&batch)], &[], make_schema_i64());
            assert_eq!(drain_chunks(&mut cursor, chunk_rows), expected,
                "chunk_rows={chunk_rows}");
        }
        // 4 rows / chunk 2: exact multiple (no trailing partial chunk).
        let even = make_batch(&rows[..4]);
        let mut cursor = create_read_cursor(&[Rc::clone(&even)], &[], make_schema_i64());
        assert_eq!(drain_chunks(&mut cursor, 2), materialize_rows(&[even]));
    }

    #[test]
    fn drain_chunk_empty_cursor_returns_none() {
        let mut cursor = create_read_cursor(&[], &[], make_schema_i64());
        assert!(cursor.drain_chunk(4).is_none());
    }

    /// Multi-source cursor (merge path) folds weights, drops ghosts, and
    /// never splits a (PK, payload) group across chunks; the concatenation
    /// equals both `materialize` and the single-source equivalent.
    #[test]
    fn drain_chunk_multi_source_matches_single_source() {
        // pk=1: +1; pk=2: +2-1 = +1 (cross-source fold); pk=3: +1-1 = ghost;
        // pk=4: weight 2 in one source.
        let b1 = make_batch(&[(1, 1, 10), (2, 2, 20), (3, 1, 30), (4, 2, 40)]);
        let b2 = make_batch(&[(2, -1, 20), (3, -1, 30)]);
        let consolidated_equivalent = make_batch(&[(1, 1, 10), (2, 1, 20), (4, 2, 40)]);

        let expected = materialize_rows(&[Rc::clone(&b1), Rc::clone(&b2)]);
        assert_eq!(expected, materialize_rows(&[Rc::clone(&consolidated_equivalent)]));
        for chunk_rows in [1, 2, 100] {
            let mut multi = create_read_cursor(
                &[Rc::clone(&b1), Rc::clone(&b2)], &[], make_schema_i64());
            assert_eq!(drain_chunks(&mut multi, chunk_rows), expected,
                "merge path, chunk_rows={chunk_rows}");

            let mut single = create_read_cursor(
                &[Rc::clone(&consolidated_equivalent)], &[], make_schema_i64());
            assert_eq!(drain_chunks(&mut single, chunk_rows), expected,
                "fast path, chunk_rows={chunk_rows}");
        }
    }

    /// `copy_current_row_into` on an invalid cursor must be a no-op; the
    /// byte-form PK write would otherwise index empty `sources` and panic.
    #[test]
    fn copy_current_row_into_invalid_is_noop() {
        let schema = make_schema_i64();
        let cursor = create_read_cursor(&[], &[], schema);
        assert!(!cursor.valid);
        let mut out = Batch::with_schema(schema, 1);
        cursor.copy_current_row_into(&mut out, 1);
        assert_eq!(out.count, 0, "invalid cursor copy must not write a row");
    }

    // -- Phase A: walk_pk_group / for_each_pk_group_row --------------------

    /// `group_has_positive` must fold per `(PK, payload)` sub-group, never sum
    /// the whole PK group: a group with one payload at net +1 and another at net
    /// −1 is *present* (the +1 payload is live), yet a whole-group sum (0) would
    /// report it absent. Pinned for both the Single lean scan and the Multi fold.
    #[test]
    fn group_has_positive_offsetting_payloads_is_present() {
        let schema = make_schema_i64();
        // Single source, pre-consolidated: PK=5 payload 100 @ +1, payload 200 @ −1.
        let single = make_batch(&[(5, 1, 100), (5, -1, 200)]);
        let mut c = create_read_cursor(std::slice::from_ref(&single), &[], schema);
        assert!(c.group_has_positive(&5u128.to_be_bytes()),
            "Single: the +1 payload is live; a whole-group sum (0) reports absent");

        // Multi source: the +1 and −1 payloads arrive in different batches, so the
        // heap fold (drive) produces the two sub-groups — the path a "sum the PK
        // group" regression would corrupt.
        let b1 = make_batch(&[(5, 1, 100)]);
        let b2 = make_batch(&[(5, -1, 200)]);
        let mut c = create_read_cursor(&[b1, b2], &[], schema);
        assert!(c.group_has_positive(&5u128.to_be_bytes()),
            "Multi: per-(PK,payload) fold must keep the +1 payload visible");
    }

    /// After walking the equal-PK group at `key`, the cursor's committed
    /// `current_*` must equal a from-scratch `seek_bytes(next_key)` — `next_key`
    /// being the first PK strictly past the group. This is what catches a wrong
    /// exit-state commit (e.g. a naive heap-root peek) that would corrupt the
    /// next co-group group's `key()` / loop guard.
    fn assert_walk_exit_matches_seek(sources: &[Rc<Batch>], key: u128, next_key: u128) {
        let schema = make_schema_i64();
        let mut walked = create_read_cursor(sources, &[], schema);
        walked.advance_to(&key.to_be_bytes()); // position at the group head, as a co-group does
        walked.group_has_positive(&key.to_be_bytes());

        let mut fresh = create_read_cursor(sources, &[], schema);
        fresh.seek_bytes(&next_key.to_be_bytes());

        assert_eq!(walked.valid, fresh.valid, "valid after walk(key={key})");
        if walked.valid {
            assert_eq!(walked.current_key, fresh.current_key, "current_key key={key}");
            assert_eq!(walked.current_pk_bytes(), fresh.current_pk_bytes(), "pk_bytes key={key}");
            assert_eq!(walked.current_weight, fresh.current_weight, "weight key={key}");
            assert_eq!(walked.current_null_word, fresh.current_null_word, "null_word key={key}");
        }
    }

    #[test]
    fn group_has_positive_exit_state_matches_fresh_seek() {
        // Single source: walk a mid group (→ lands on PK=10) and the last group
        // (→ exhausts). The −1 payload at PK=5 exercises the tombstone-before-end.
        let single = make_batch(&[(5, 1, 100), (5, -1, 200), (10, 1, 1000), (10, 1, 2000)]);
        assert_walk_exit_matches_seek(std::slice::from_ref(&single), 5, 10);
        assert_walk_exit_matches_seek(std::slice::from_ref(&single), 10, 11);

        // Multi source: same PKs split across batches (the heap-fold path).
        let b1 = make_batch(&[(5, 1, 100), (10, 1, 1000)]);
        let b2 = make_batch(&[(5, -1, 200), (10, 1, 2000)]);
        assert_walk_exit_matches_seek(&[Rc::clone(&b1), Rc::clone(&b2)], 5, 10);
        assert_walk_exit_matches_seek(&[b1, b2], 10, 11);
    }

    /// `for_each_pk_group_row` visits one entry per non-ghost (PK, payload)
    /// sub-group with `current_*` committed (the callback reads columns/weight),
    /// then leaves the exit state at the first row past the group.
    #[test]
    fn for_each_pk_group_row_visits_subgroups_and_exits_clean() {
        let schema = make_schema_i64();
        let b1 = make_batch(&[(5, 1, 100), (10, 1, 1000)]);
        let b2 = make_batch(&[(5, 3, 200)]); // PK=5 payload 200 @ +3
        let mut c = create_read_cursor(&[b1, b2], &[], schema);

        let mut seen: Vec<(u64, i64)> = Vec::new();
        c.for_each_pk_group_row(&5u128.to_be_bytes(), |cur| {
            seen.push((cur.current_key as u64, cur.current_weight));
        });
        // Two sub-groups at PK=5: payload 100 @ +1, payload 200 @ +3 (payload-sorted).
        assert_eq!(seen, vec![(5, 1), (5, 3)]);
        // Exit: positioned at PK=10, fully committed.
        assert!(c.valid);
        assert_eq!(c.current_key, 10);
        assert_eq!(c.current_weight, 1);
    }

    // -- Phase C: Pair (k=2) bypass ≡ Multi --------------------------------

    /// Build a 2-source cursor but FORCE `SourceMode::Multi` (the production
    /// selector always picks `Pair` at len 2, so there is otherwise no Multi(k=2)
    /// to compare against). Re-seats each head at the start, builds the loser
    /// tree, and drives — mirroring `new()`'s Multi arm.
    fn create_cursor_force_multi(batches: &[Rc<Batch>]) -> ReadCursor {
        let mut c = create_read_cursor(batches, &[], make_schema_i64());
        for (src, state) in c.sources.iter().zip(c.states.iter_mut()) {
            state.position = 0;
            state.skip_ghosts(src);
        }
        c.mode = SourceMode::Multi(
            ReadCursor::build_tree(&c.sources, &c.states, &c.schema, c.is_pk_unique),
        );
        c.drive();
        c
    }

    /// Pair (the production k=2 path) must produce output identical to the loser
    /// tree on the same two sources — full scan and every `advance_to` landing —
    /// including cross-source ghost folding, same-PK / multi-payload groups, and
    /// cross-source weight sums.
    #[test]
    fn pair_equiv_multi() {
        type Rows = &'static [(u128, i64, i64)];
        let cases: &[(Rows, Rows)] = &[
            (&[(1, 1, 10), (3, 1, 30), (5, 1, 50)], &[(2, 1, 20), (4, 1, 40)]), // interleaved
            (&[(1, 1, 10), (2, 1, 20)], &[(2, -1, 20), (3, 1, 30)]),            // pk=2 ghost-folds
            (&[(5, 1, 100)], &[(5, 1, 200)]),                                   // same PK, two payloads
            (&[(5, 1, 100)], &[(5, -1, 100)]),                                  // same (PK,payload) → ghost
            (&[(5, 3, 100), (5, 1, 200)], &[(5, -1, 100)]),                     // multi-payload, partial fold
            (&[(1, 2, 10), (2, 5, 20)], &[(1, 1, 10), (2, 1, 20)]),             // cross-source weight sum
        ];
        for (a, b) in cases {
            let srcs = [make_batch(a), make_batch(b)];

            let mut pair = create_read_cursor(&srcs, &[], make_schema_i64());
            assert!(matches!(pair.mode, SourceMode::Pair), "production must pick Pair at len 2");
            let mut multi = create_cursor_force_multi(&srcs);
            assert!(matches!(multi.mode, SourceMode::Multi(_)));
            assert_eq!(scan_all(&mut pair), scan_all(&mut multi), "scan a={a:?} b={b:?}");

            // advance_to: each landing (valid, PK bytes, net weight) matches Multi.
            for key in 0u128..=6 {
                let mut p = create_read_cursor(&srcs, &[], make_schema_i64());
                let mut m = create_cursor_force_multi(&srcs);
                p.advance_to(&key.to_be_bytes());
                m.advance_to(&key.to_be_bytes());
                assert_eq!(p.valid, m.valid, "advance_to({key}) valid a={a:?} b={b:?}");
                if p.valid {
                    assert_eq!(p.current_pk_bytes(), m.current_pk_bytes(), "advance_to({key}) pk a={a:?} b={b:?}");
                    assert_eq!(p.current_weight, m.current_weight, "advance_to({key}) weight a={a:?} b={b:?}");
                }
            }
        }
    }
}
