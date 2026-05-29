//! Opaque read cursor: N-way merge over in-memory batches + mmap'd shards.
//!
//! Produces rows in (PK, payload) order with inline ghost elimination
//! (net weight=0 rows are skipped).

use std::cell::{Cell, OnceCell};
use std::cmp::Ordering;
use std::ptr;
use std::rc::Rc;

use super::batch::{Batch, FIXED_REGION_BYTES};
use super::columnar::{self, ColumnarSource};
use crate::schema::{SchemaDescriptor, type_code, MAX_COLUMNS};
use super::heap::{drive_merge, HeapNode, LoserTree};
use super::merge::{pack_pk_be, pk_sort_key, ColPtr, MemBatch, UnifiedSource};
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

    #[inline]
    fn get_pk_bytes(&self, row: usize) -> &[u8] {
        match self {
            CursorSource::Batch(b) => b.get_pk_bytes(row),
            CursorSource::Shard(s) => s.get_pk_bytes(row),
        }
    }

    /// Loser-tree heap key: the order-preserving `pk_sort_key`. For
    /// `pk_stride ≤ 16` this is the whole PK (authoritative); for
    /// `pk_stride > 16` it is the order-preserving 16-byte OPK prefix
    /// (authoritative when two prefixes differ, else tie-broken via
    /// `compare_pk_bytes` on the full `get_pk_bytes` slice). Decoupled from the
    /// public `current_key` field, which keeps the raw `pack_pk_le` value.
    #[inline]
    fn get_pk_prefix(&self, row: usize) -> u128 {
        pk_sort_key(self.get_pk_bytes(row))
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

/// Three-way dispatch on source count, replacing the previous
/// `tree: Option<LoserTree>` + parallel `entries.len() == 1` checks.
/// `Empty`/`Single`/`Multi` are exhaustive so each call site dispatches once.
enum SourceMode {
    Empty,
    Single,
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
    is_fast: bool,
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

impl ReadCursor {
    #[inline]
    fn build_tree_with<RowCmp: RowComparator>(
        sources: &[CursorSource],
        states: &[CursorState],
        schema: &SchemaDescriptor,
        row_cmp: RowCmp,
    ) -> LoserTree {
        // Heap key is the order-preserving `pk_sort_key` (whole PK for narrow,
        // OPK 16-byte prefix for wide). A differing key settles the order; a key
        // tie falls to a raw OPK-byte compare (implied-equal for narrow,
        // separating wide low-16 collisions) then the payload tiebreak.
        let init = |i: usize| {
            if states[i].is_valid() {
                Some((sources[i].get_pk_prefix(states[i].position), states[i].position))
            } else {
                None
            }
        };
        let less = move |a: &HeapNode, b: &HeapNode| {
            match a.key.cmp(&b.key) {
                Ordering::Less => true,
                Ordering::Greater => false,
                Ordering::Equal => match columnar::compare_pk_bytes(
                    sources[a.source_idx].get_pk_bytes(a.row),
                    sources[b.source_idx].get_pk_bytes(b.row),
                ) {
                    Ordering::Less => true,
                    Ordering::Greater => false,
                    Ordering::Equal => row_cmp(
                        schema,
                        &sources[a.source_idx], a.row,
                        &sources[b.source_idx], b.row,
                    ) == Ordering::Less,
                },
            }
        };
        LoserTree::build(sources.len(), init, less)
    }

    fn build_tree(
        sources: &[CursorSource],
        states: &[CursorState],
        schema: &SchemaDescriptor,
        is_fast: bool,
    ) -> LoserTree {
        // Wrap as a non-capturing closure: a direct fn-item reference would
        // fix the source-ref lifetime, conflicting with `_with`'s HRTB Fn bound.
        #[allow(clippy::redundant_closure)]
        if is_fast {
            Self::build_tree_with(sources, states, schema,
                |s, a, ai, b, bi| columnar::compare_rows_int_nonnull(s, a, ai, b, bi))
        } else {
            Self::build_tree_with(sources, states, schema,
                |s, a, ai, b, bi| columnar::compare_rows(s, a, ai, b, bi))
        }
    }

    fn new(sources: Vec<CursorSource>, states: Vec<CursorState>, schema: SchemaDescriptor) -> Self {
        debug_assert_eq!(sources.len(), states.len());
        let is_fast = columnar::schema_is_int_nonnull(&schema);
        let mode = match sources.len() {
            0 => SourceMode::Empty,
            1 => SourceMode::Single,
            _ => SourceMode::Multi(
                Self::build_tree(&sources, &states, &schema, is_fast),
            ),
        };
        let mut cursor = ReadCursor {
            sources,
            states,
            unified_sources: OnceCell::new(),
            mode,
            schema,
            is_fast,
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
                Self::build_tree(&self.sources, &self.states, &self.schema, self.is_fast),
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
        for (src, state) in self.sources.iter().zip(self.states.iter_mut()) {
            state.position = 0;
            state.skip_ghosts(src);
        }
        self.rebuild_and_drive();
    }

    /// Byte-addressed sibling of [`seek`]. `key` must be exactly `pk_stride`
    /// bytes. Correct at every PK width: the inner `drive`/`build_tree` key the
    /// heap via `get_pk_prefix` (prefix + `compare_pk_bytes` tie-break for
    /// wide), so this survives `pk_stride > 16` where the u128 `seek` cannot.
    pub fn seek_bytes(&mut self, key: &[u8]) {
        for (src, state) in self.sources.iter().zip(self.states.iter_mut()) {
            state.seek_bytes(src, key);
        }
        self.rebuild_and_drive();
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
        if !self.valid {
            return;
        }
        #[allow(clippy::redundant_closure)]
        if self.is_fast {
            self.advance_with(|s, a, ai, b, bi| columnar::compare_rows_int_nonnull(s, a, ai, b, bi));
        } else {
            self.advance_with(|s, a, ai, b, bi| columnar::compare_rows(s, a, ai, b, bi));
        }
    }

    /// Step past the previously-emitted row, then drive to the next group.
    /// For single-source cursors `drive` just commits the row at the
    /// current position, so we must step past it first. Multi-source
    /// cursors don't pre-advance: `drive`'s inner fold popped the
    /// previously-emitted group's rows from the heap, so the next call
    /// opens a new group from the heap root.
    #[inline]
    fn advance_with<RowCmp: RowComparator>(&mut self, row_cmp: RowCmp) {
        if matches!(self.mode, SourceMode::Single) {
            self.states[0].advance(&self.sources[0]);
        }
        self.drive_with(row_cmp);
    }

    #[inline]
    fn drive(&mut self) {
        #[allow(clippy::redundant_closure)]
        if self.is_fast {
            self.drive_with(|s, a, ai, b, bi| columnar::compare_rows_int_nonnull(s, a, ai, b, bi));
        } else {
            self.drive_with(|s, a, ai, b, bi| columnar::compare_rows(s, a, ai, b, bi));
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

    /// Shared N-way merge driver body for [`drive_with`]. Mirrors storage's
    /// `merge_batches_inner`: the caller selects only `less`/`eq_payload` per PK
    /// width, and monomorphization compiles each width to its own branch-free
    /// copy of the (width-independent) advance/weight/emit hot loop. Returns the
    /// emitted group as `(weight, source_idx, row)`, or `None` if drained. The
    /// heap group key is not returned — `current_key` is recomputed from PK
    /// bytes at the commit point to honour its raw `pack_pk_le` contract.
    #[inline]
    fn drive_inner<L, EQ>(
        heap: &mut LoserTree,
        sources: &[CursorSource],
        states: &mut [CursorState],
        _schema: &SchemaDescriptor,
        less: L,
        eq_payload: EQ,
    ) -> Option<(i64, usize, usize)>
    where
        L: Fn(&HeapNode, &HeapNode) -> bool + Copy,
        EQ: Fn(usize, usize, usize, usize) -> bool,
    {
        let mut emitted: Option<(i64, usize, usize)> = None;
        drive_merge(
            heap, less,
            |src| {
                states[src].advance(&sources[src]);
                if states[src].is_valid() {
                    Some((sources[src].get_pk_prefix(states[src].position), states[src].position))
                } else {
                    None
                }
            },
            eq_payload,
            |src, row| sources[src].get_weight(row),
            |gs, gr, _gk, nw| {
                emitted = Some((nw, gs, gr));
                std::ops::ControlFlow::Break(())
            },
        );
        emitted
    }

    /// Heap path: emit one consolidated non-ghost group, or invalidate.
    ///
    /// `drive_merge` folds tied rows for us; we hand it `Break` from `emit`
    /// the first time we see a non-ghost group to return immediately.
    /// Ghost groups (net weight = 0) cause `drive_merge` to skip emit and
    /// open the next group, so the loop naturally walks past them.
    #[inline]
    fn drive_with<RowCmp: RowComparator>(&mut self, row_cmp: RowCmp) {
        let heap = match &mut self.mode {
            SourceMode::Empty  => { self.valid = false; return; }
            SourceMode::Single => { self.drive_single(); return; }
            SourceMode::Multi(h) => h,
        };
        let schema  = &self.schema;
        let sources = &self.sources;
        let states  = &mut self.states;
        // `drive_inner` returns the emitted group as a tuple rather than writing
        // `self.current_*` directly: its closures already reborrow `&sources` +
        // `&mut states`, so also capturing `&mut self.current_*` would force a
        // whole-`self` borrow. We commit the tuple (and fetch `get_null_word`)
        // after it returns.
        //
        // `node.key` (the order-preserving `pk_sort_key`) settles distinct keys
        // with no column work; a key tie falls to a raw OPK-byte compare
        // (implied-equal for narrow, separating wide low-16 collisions) then the
        // payload tiebreak. `eq_payload` keeps the byte term so two distinct wide
        // PKs sharing a prefix AND a payload are not folded into one group.
        let less = |a: &HeapNode, b: &HeapNode| -> bool {
            match a.key.cmp(&b.key) {
                Ordering::Less => true,
                Ordering::Greater => false,
                Ordering::Equal => match columnar::compare_pk_bytes(
                    sources[a.source_idx].get_pk_bytes(a.row),
                    sources[b.source_idx].get_pk_bytes(b.row)) {
                    Ordering::Less => true,
                    Ordering::Greater => false,
                    Ordering::Equal => row_cmp(schema, &sources[a.source_idx], a.row,
                                                       &sources[b.source_idx], b.row) == Ordering::Less,
                },
            }
        };
        let eq_payload = |a_src: usize, a_row: usize, b_src: usize, b_row: usize| {
            sources[a_src].get_pk_bytes(a_row) == sources[b_src].get_pk_bytes(b_row)
            && row_cmp(schema, &sources[a_src], a_row,
                               &sources[b_src], b_row) == Ordering::Equal
        };
        let emitted = Self::drive_inner(heap, sources, states, schema, less, eq_payload);
        // Heap key is the OPK; recompute current_key (narrow native value /
        // wide prefix) here, off the comparator. One pack per emitted group.
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
                // Map logical → payload index
                let payload_idx = self.schema.payload_idx(col_idx);
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

    /// Copy the current row into `batch` with an explicit weight.
    pub(crate) fn copy_current_row_into(&self, batch: &mut Batch, weight: i64) {
        // `current_pk_bytes()` indexes `self.sources[current_entry_idx]`, which
        // panics on an unpositioned/Empty cursor (empty `sources`). The col/blob
        // helpers below already no-op on `!valid`; restore that contract here so
        // the byte-form PK write does too.
        if !self.valid {
            return;
        }
        // Byte-form is correct at every width; for narrow PKs these bytes equal
        // what `extend_pk(current_key)` would have re-encoded.
        batch.extend_pk_bytes(self.current_pk_bytes());
        batch.extend_weight(&weight.to_le_bytes());
        batch.extend_null_bmp(&self.current_null_word.to_le_bytes());
        let blob_ptr = self.blob_ptr();
        let src_blob: &[u8] = if blob_ptr.is_null() { &[] } else {
            unsafe { std::slice::from_raw_parts(blob_ptr, self.blob_len()) }
        };
        for (payload_idx, ci, col) in self.schema.payload_columns() {
            let col_size = col.size() as usize;
            let ptr = self.col_ptr(ci, col_size);
            if !ptr.is_null() {
                let data = unsafe { std::slice::from_raw_parts(ptr, col_size) };
                if (col.type_code == type_code::STRING || col.type_code == type_code::BLOB) && col_size == 16 {
                    let cell = crate::schema::relocate_german_string_vec(data, src_blob, &mut batch.blob, None);
                    batch.extend_col(payload_idx, &cell);
                } else {
                    batch.extend_col(payload_idx, data);
                }
            } else {
                batch.fill_col_zero(payload_idx, col_size);
            }
        }
        batch.count += 1;
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
        #[allow(clippy::redundant_closure)]
        if self.is_fast {
            self.drain_sorted_into_with(limit, out,
                |s, a, ai, b, bi| columnar::compare_rows_int_nonnull(s, a, ai, b, bi));
        } else {
            self.drain_sorted_into_with(limit, out,
                |s, a, ai, b, bi| columnar::compare_rows(s, a, ai, b, bi));
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
pub fn create_read_cursor(
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

/// Owns a `ReadCursor`.  Since the cursor now owns its data via `Arc`s in
/// each `CursorSource`, this is a thin newtype — the separate "owned
/// snapshots" vector that previously kept `Arc<Batch>` alive alongside
/// transmuted-to-`'static` `MemBatch` views is gone.
pub struct CursorHandle {
    pub(crate) cursor: ReadCursor,
}

impl CursorHandle {
    /// Build a CursorHandle from owned in-memory batches (no shards).
    ///
    /// Convenience wrapper used by test code.
    #[cfg(test)]
    pub fn from_owned(
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
pub fn create_cursor_from_snapshots(
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
        crate::util::raise_fd_limit_for_tests();
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

    /// 3×U64 compound PK (stride 24, wide): col_0 in PK bytes 0..8, col_1 in
    /// 8..16, col_2 in 16..24. The u128 heap key only carries the low-16
    /// prefix, so col_2 lives entirely past it.
    fn make_wide_pk_schema() -> SchemaDescriptor {
        SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0, 1, 2],
        )
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
        let schema = make_wide_pk_schema();
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
        let schema = make_wide_pk_schema();
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
        let schema = make_wide_pk_schema();
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
}
