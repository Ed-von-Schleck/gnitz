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
use super::merge::{ColPtr, MemBatch, UnifiedSource};
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
        Self { inner: DRAIN_BUFFER.with(|b| b.take()) }
    }
}

impl std::ops::Deref for DrainGuard {
    type Target = Vec<(u32, u32, i64)>;
    fn deref(&self) -> &Self::Target { &self.inner }
}

impl std::ops::DerefMut for DrainGuard {
    fn deref_mut(&mut self) -> &mut Self::Target { &mut self.inner }
}

impl Drop for DrainGuard {
    #[inline]
    fn drop(&mut self) {
        DRAIN_BUFFER.with(|b| b.set(std::mem::take(&mut self.inner)));
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
    fn get_pk(&self, row: usize) -> u128 {
        match self {
            CursorSource::Batch(b) => b.get_pk(row),
            CursorSource::Shard(s) => s.get_pk(row),
        }
    }

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

    fn find_lower_bound(&self, key: u128) -> usize {
        match self {
            CursorSource::Batch(b) => b.find_lower_bound(key),
            CursorSource::Shard(s) => s.find_lower_bound(key),
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
                let pk_off = mb.offsets[super::batch::REG_PK] as usize;
                let nbm_off = mb.offsets[super::batch::REG_NULL_BMP] as usize;
                for (pi, _ci, col) in schema.payload_columns() {
                    let off = mb.offsets[super::batch::REG_PAYLOAD_START + pi] as usize;
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

    fn seek(&mut self, src: &CursorSource, key: u128) {
        self.position = src.find_lower_bound(key);
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
        let less = move |a: &HeapNode, b: &HeapNode| {
            if a.key != b.key {
                return a.key < b.key;
            }
            row_cmp(
                schema,
                &sources[a.source_idx], a.row,
                &sources[b.source_idx], b.row,
            ) == Ordering::Less
        };
        LoserTree::build(
            sources.len(),
            |i| {
                if states[i].is_valid() {
                    Some((sources[i].get_pk(states[i].position), states[i].position))
                } else {
                    None
                }
            },
            less,
        )
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
            _ => SourceMode::Multi(Self::build_tree(&sources, &states, &schema, is_fast)),
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

    pub fn seek(&mut self, key: u128) {
        for (src, state) in self.sources.iter().zip(self.states.iter_mut()) {
            state.seek(src, key);
        }
        if let SourceMode::Multi(_) = &self.mode {
            self.mode = SourceMode::Multi(
                Self::build_tree(&self.sources, &self.states, &self.schema, self.is_fast),
            );
        }
        self.drive();
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
            self.current_key = src.get_pk(pos);
            self.current_weight = src.get_weight(pos);
            self.current_null_word = src.get_null_word(pos);
            self.current_entry_idx = 0;
            self.current_row = pos;
        } else {
            self.valid = false;
        }
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
        // The closure already borrows `&sources` + `&mut states`; capturing
        // the five `&mut self.current_*` fields in addition would force
        // `&mut self` (incompatible with the field-level reborrow above).
        // Stage into a stack tuple, commit after `drive_merge` returns.
        // `get_null_word` is fetched after return — no need to evaluate
        // it inside the emit closure that runs at most once per call.
        let mut emitted: Option<(u128, i64, usize, usize)> = None;

        let less = |a: &HeapNode, b: &HeapNode| -> bool {
            if a.key != b.key { return a.key < b.key; }
            row_cmp(schema, &sources[a.source_idx], a.row,
                            &sources[b.source_idx], b.row) == Ordering::Less
        };
        drive_merge(
            heap, less,
            |src| {
                states[src].advance(&sources[src]);
                if states[src].is_valid() {
                    Some((sources[src].get_pk(states[src].position), states[src].position))
                } else {
                    None
                }
            },
            |a_src, a_row, b_src, b_row| {
                row_cmp(schema, &sources[a_src], a_row,
                                &sources[b_src], b_row) == Ordering::Equal
            },
            |src, row| sources[src].get_weight(row),
            |gs, gr, gk, nw| {
                emitted = Some((gk, nw, gs, gr));
                std::ops::ControlFlow::Break(())
            },
        );
        if let Some((key, weight, idx, row)) = emitted {
            self.valid = true;
            self.current_key = key;
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

    /// Copy the current row into `batch` with an explicit weight.
    pub(crate) fn copy_current_row_into(&self, batch: &mut Batch, weight: i64) {
        batch.extend_pk(self.current_key);
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
                // Batch sources are always consolidated (no ghosts).
                let end = start + row_count;
                let mut out = Batch::with_schema(*schema, row_count.max(1));
                out.append_batch(b, start, end);
                out.sorted = true;
                out.consolidated = true;
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

        // Seek to pk >= 5
        cursor.seek(5);
        assert!(cursor.valid);
        assert_eq!(cursor.current_key, 5);

        // Seek to pk >= 7 → lands on 10
        cursor.seek(7);
        assert!(cursor.valid);
        assert_eq!(cursor.current_key, 10);

        // Seek past end
        cursor.seek(100);
        assert!(!cursor.valid);
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
        let pk_index = cursor.schema.pk_index_single() as usize; // 0
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

        let pk_bytes: Vec<u8> = 42u128.to_le_bytes().to_vec();
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

}
