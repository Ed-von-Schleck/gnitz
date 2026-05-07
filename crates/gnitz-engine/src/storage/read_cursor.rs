//! Opaque read cursor: N-way merge over in-memory batches + mmap'd shards.
//!
//! Produces rows in (PK, payload) order with inline ghost elimination
//! (net weight=0 rows are skipped).

use std::cell::RefCell;
use std::cmp::Ordering;
use std::ptr;
use std::rc::Rc;

use super::batch::{Batch, FIXED_REGION_BYTES};
use super::columnar::{self, ColumnarSource};
use crate::schema::{SchemaDescriptor, type_code, MAX_COLUMNS};
use super::heap::MergeHeap;
use super::merge::{ColPtr, MemBatch, UnifiedSource};
use super::shard_reader::{MappedShard, RegionView};

thread_local! {
    /// Reusable per-thread scratch buffer for `drain_sorted_into`. Each
    /// 16-byte tuple is much smaller than the corresponding output row, so
    /// keeping peak capacity for the thread's lifetime is cheap relative to
    /// the batches it feeds.
    pub(crate) static DRAIN_BUFFER: RefCell<Vec<(u32, u32, i64)>> =
        const { RefCell::new(Vec::new()) };
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
                        stride: col.size as usize,
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
                    let cs = col.size as usize;
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

/// Compare two heap nodes by (PK, payload) for ReadCursorEntry-based heaps.
#[inline]
fn entry_cmp(
    entries: &[ReadCursorEntry],
    schema: &SchemaDescriptor,
    is_fast: bool,
    a: &super::heap::HeapNode,
    b: &super::heap::HeapNode,
) -> Ordering {
    let key_ord = a.key.cmp(&b.key);
    if key_ord != Ordering::Equal {
        return key_ord;
    }
    columnar::compare_rows_dispatch(
        schema,
        &entries[a.idx].source,
        entries[a.idx].position,
        &entries[b.idx].source,
        entries[b.idx].position,
        is_fast,
    )
}

// ---------------------------------------------------------------------------
// ReadCursorEntry — per-source position tracker
// ---------------------------------------------------------------------------

struct ReadCursorEntry {
    source: CursorSource,
    position: usize,
    count: usize,
}

impl ReadCursorEntry {
    fn new_batch(batch: Rc<Batch>) -> Self {
        let count = batch.count;
        ReadCursorEntry {
            source: CursorSource::Batch(batch),
            position: 0,
            count,
        }
    }

    fn new_shard(shard: Rc<MappedShard>) -> Self {
        let count = shard.count;
        let mut entry = ReadCursorEntry {
            source: CursorSource::Shard(shard),
            position: 0,
            count,
        };
        entry.skip_ghosts();
        entry
    }

    #[inline]
    fn is_valid(&self) -> bool {
        self.position < self.count
    }

    fn advance(&mut self) {
        if self.is_valid() {
            self.position += 1;
            self.skip_ghosts();
        }
    }

    fn seek(&mut self, key: u128) {
        self.position = self.source.find_lower_bound(key);
        self.skip_ghosts();
    }

    fn skip_ghosts(&mut self) {
        // Batch sources are always consolidated — no ghost rows.
        let CursorSource::Shard(s) = &self.source else { return; };
        if !s.has_ghosts { return; }
        while self.position < self.count {
            if self.source.get_weight(self.position) != 0 {
                return;
            }
            self.position += 1;
        }
    }

    #[inline]
    fn peek_key(&self) -> u128 {
        if self.is_valid() {
            self.source.get_pk(self.position)
        } else {
            u128::MAX
        }
    }

    #[inline]
    fn weight(&self) -> i64 {
        if self.is_valid() {
            self.source.get_weight(self.position)
        } else {
            0
        }
    }

    #[inline]
    fn source_blob_len(&self) -> usize {
        self.source.blob_slice().len()
    }
}

// ---------------------------------------------------------------------------
// ReadCursor
// ---------------------------------------------------------------------------

/// Dispatch shape for the merge cursor.
///
/// `None` covers 0- and 1-entry cursors (no merging needed).  `Two` is the
/// hot two-pointer fast path for `entries.len() == 2`, which avoids the heap
/// entirely.  `Heap` is the general N-way merge for `entries.len() >= 3`.
enum TreeKind {
    None,
    Two,
    Heap(MergeHeap),
}

// advance_mask bits used by the TreeKind::Two path.
const MASK_LEFT: u8 = 0b01;
const MASK_RIGHT: u8 = 0b10;
const MASK_BOTH: u8 = 0b11;

pub struct ReadCursor {
    entries: Vec<ReadCursorEntry>,
    unified_sources: Vec<UnifiedSource>,
    tree: TreeKind,
    schema: SchemaDescriptor,
    is_fast: bool,
    // Current row state
    pub valid: bool,
    pub current_key: u128,
    pub current_weight: i64,
    pub current_null_word: u64,
    current_entry_idx: usize,
    current_row: usize,
    // Set by find_next_non_ghost (Two path), consumed by next advance().
    // Mirrors the heap path's min_indices contract: find_next_non_ghost
    // selects the next row but does not advance entries past it; advance()
    // does that on the following call.
    advance_mask: u8,
    // Reusable buffer for advance indices (heap path only)
    advance_buf: Vec<usize>,
}

impl ReadCursor {
    fn build_tree(
        entries: &[ReadCursorEntry],
        schema: &SchemaDescriptor,
        is_fast: bool,
    ) -> MergeHeap {
        let less = |a: &super::heap::HeapNode, b: &super::heap::HeapNode| {
            entry_cmp(entries, schema, is_fast, a, b).is_lt()
        };
        MergeHeap::build(
            entries.len(),
            |i| {
                if entries[i].is_valid() {
                    Some(entries[i].peek_key())
                } else {
                    None
                }
            },
            less,
        )
    }

    fn new(entries: Vec<ReadCursorEntry>, schema: SchemaDescriptor) -> Self {
        let n = entries.len();
        let is_fast = columnar::schema_is_int_nonnull(&schema);
        let unified_sources: Vec<UnifiedSource> = entries.iter()
            .map(|e| e.source.to_unified(&schema))
            .collect();
        let tree = match n {
            0 | 1 => TreeKind::None,
            2 => TreeKind::Two,
            _ => TreeKind::Heap(Self::build_tree(&entries, &schema, is_fast)),
        };
        let advance_buf = if matches!(tree, TreeKind::Heap(_)) {
            Vec::with_capacity(n)
        } else {
            Vec::new()
        };
        let mut cursor = ReadCursor {
            entries,
            unified_sources,
            tree,
            schema,
            is_fast,
            valid: false,
            current_key: 0,
            current_weight: 0,
            current_null_word: 0,
            current_entry_idx: 0,
            current_row: 0,
            advance_mask: 0,
            advance_buf,
        };
        cursor.find_next_non_ghost();
        cursor
    }

    pub fn seek(&mut self, key: u128) {
        for e in &mut self.entries {
            e.seek(key);
        }
        // Two: nothing to rebuild — find_next_non_ghost recomputes from
        // entry positions every call.  Heap: re-build from seeked positions.
        if let TreeKind::Heap(_) = self.tree {
            self.tree = TreeKind::Heap(Self::build_tree(&self.entries, &self.schema, self.is_fast));
        }
        self.find_next_non_ghost();
    }

    pub fn advance(&mut self) {
        if !self.valid {
            return;
        }

        match &mut self.tree {
            TreeKind::None => {
                if self.entries.len() == 1 {
                    self.entries[0].advance();
                }
            }
            TreeKind::Two => {
                if self.advance_mask & MASK_LEFT != 0 { self.entries[0].advance(); }
                if self.advance_mask & MASK_RIGHT != 0 { self.entries[1].advance(); }
            }
            TreeKind::Heap(tree) => {
                self.advance_buf.clear();
                self.advance_buf.extend_from_slice(&tree.min_indices);
                let is_fast = self.is_fast;
                for &ei in &self.advance_buf {
                    self.entries[ei].advance();
                    let new_key = if self.entries[ei].is_valid() {
                        Some(self.entries[ei].peek_key())
                    } else {
                        None
                    };
                    let less = |a: &super::heap::HeapNode, b: &super::heap::HeapNode| {
                        entry_cmp(&self.entries, &self.schema, is_fast, a, b).is_lt()
                    };
                    tree.advance(ei, new_key, &less);
                }
            }
        }

        self.find_next_non_ghost();
    }

    fn find_next_non_ghost(&mut self) {
        match &mut self.tree {
            TreeKind::None => {
                if self.entries.len() == 1 && self.entries[0].is_valid() {
                    let w = self.entries[0].source.get_weight(self.entries[0].position);
                    self.commit_row(0, w);
                } else {
                    self.valid = false;
                }
            }
            TreeKind::Two => {
                self.find_next_non_ghost_two();
            }
            TreeKind::Heap(_) => {
                self.find_next_non_ghost_heap();
            }
        }
    }

    /// Two-pointer fast path for entries.len() == 2.  Sets self.advance_mask
    /// indicating which entries should be bumped on the next advance() call;
    /// does NOT advance entries on a non-ghost row (mirrors the heap path's
    /// contract via tree.min_indices).  Ghost rows ARE advanced inline so the
    /// caller never sees a zero-weight emit.
    fn find_next_non_ghost_two(&mut self) {
        loop {
            let v0 = self.entries[0].is_valid();
            let v1 = self.entries[1].is_valid();
            match (v0, v1) {
                (false, false) => {
                    self.valid = false;
                    self.advance_mask = 0;
                    return;
                }
                (true, false) => {
                    self.commit_two(0, MASK_LEFT);
                    return;
                }
                (false, true) => {
                    self.commit_two(1, MASK_RIGHT);
                    return;
                }
                (true, true) => {
                    let ka = self.entries[0].peek_key();
                    let kb = self.entries[1].peek_key();
                    if ka != kb {
                        if ka < kb {
                            self.commit_two(0, MASK_LEFT);
                        } else {
                            self.commit_two(1, MASK_RIGHT);
                        }
                        return;
                    }
                    // PKs equal — element identity also requires payload
                    // equality (foundations.md §1).  PK-equal-payload-different
                    // is two distinct rows in deterministic payload order.
                    let ord = columnar::compare_rows_dispatch(
                        &self.schema,
                        &self.entries[0].source, self.entries[0].position,
                        &self.entries[1].source, self.entries[1].position,
                        self.is_fast,
                    );
                    match ord {
                        Ordering::Less => {
                            self.commit_two(0, MASK_LEFT);
                            return;
                        }
                        Ordering::Greater => {
                            self.commit_two(1, MASK_RIGHT);
                            return;
                        }
                        Ordering::Equal => {
                            // Tie group: sum weights, ghost-check.  Only here
                            // does net-weight summation apply.
                            let net = self.entries[0].weight() + self.entries[1].weight();
                            if net != 0 {
                                // Tied non-ghost — exemplar is entry 0,
                                // advance both on next advance() call.
                                self.commit_row(0, net);
                                self.advance_mask = MASK_BOTH;
                                return;
                            }
                            // Ghost: net weight zero.  Skip both and continue.
                            self.entries[0].advance();
                            self.entries[1].advance();
                        }
                    }
                }
            }
        }
    }

    /// Two-pointer commit: copy entries[pick]'s stored weight as the emitted
    /// weight and stash the advance bitmask.  The tied-non-ghost case calls
    /// `commit_row` directly with the summed net weight instead.
    #[inline]
    fn commit_two(&mut self, pick: usize, mask: u8) {
        let weight = self.entries[pick].source.get_weight(self.entries[pick].position);
        self.commit_row(pick, weight);
        self.advance_mask = mask;
    }

    /// Copy the row at entries[idx] into the public cursor state with an
    /// explicit emit weight (which can be a per-source weight or a summed
    /// net weight, depending on the caller's path).  Shared by the
    /// two-pointer and heap paths.
    #[inline]
    fn commit_row(&mut self, idx: usize, weight: i64) {
        let e = &self.entries[idx];
        let pos = e.position;
        self.valid = true;
        self.current_key = e.source.get_pk(pos);
        self.current_weight = weight;
        self.current_null_word = e.source.get_null_word(pos);
        self.current_entry_idx = idx;
        self.current_row = pos;
    }

    fn find_next_non_ghost_heap(&mut self) {
        let TreeKind::Heap(ref mut tree) = self.tree else {
            unreachable!("dispatched from TreeKind::Heap arm")
        };
        let schema = &self.schema;
        let is_fast = self.is_fast;

        while !tree.is_empty() {
            let eq_root = |a: &super::heap::HeapNode, b: &super::heap::HeapNode| -> Ordering {
                entry_cmp(&self.entries, schema, is_fast, a, b)
            };
            let num_min = tree.collect_min_indices(&eq_root);
            if num_min == 0 {
                break;
            }

            let mut net_weight: i64 = 0;
            for i in 0..num_min {
                net_weight += self.entries[tree.min_indices[i]].weight();
            }

            if net_weight != 0 {
                let exemplar = tree.min_indices[0];
                self.commit_row(exemplar, net_weight);
                return;
            }

            self.advance_buf.clear();
            self.advance_buf
                .extend_from_slice(&tree.min_indices[..num_min]);
            for &ei in &self.advance_buf {
                self.entries[ei].advance();
                let new_key = if self.entries[ei].is_valid() {
                    Some(self.entries[ei].peek_key())
                } else {
                    None
                };
                let less = |a: &super::heap::HeapNode, b: &super::heap::HeapNode| {
                    entry_cmp(&self.entries, schema, is_fast, a, b).is_lt()
                };
                tree.advance(ei, new_key, &less);
            }
        }

        self.valid = false;
    }

    /// Approximate the number of rows remaining in this cursor (upper bound).
    /// Used by the adaptive-swap heuristic in join/semi-join operators.
    pub fn estimated_length(&self) -> usize {
        self.entries.iter().map(|e| e.count.saturating_sub(e.position)).sum()
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
        if col_idx == self.schema.pk_index as usize {
            return ptr::null();
        }
        let entry = &self.entries[self.current_entry_idx];
        let row = self.current_row;
        let pk_index = self.schema.pk_index as usize;

        match &entry.source {
            CursorSource::Shard(s) => s.col_ptr_by_logical(row, col_idx, col_size),
            CursorSource::Batch(b) => {
                // Map logical → payload index
                let payload_idx = if col_idx < pk_index { col_idx } else { col_idx - 1 };
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
        self.entries[self.current_entry_idx].source.blob_ptr()
    }

    /// Blob arena length for the current row's source.
    pub fn blob_len(&self) -> usize {
        if !self.valid {
            return 0;
        }
        self.entries[self.current_entry_idx].source.blob_slice().len()
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
            let col_size = col.size as usize;
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
        if self.entries.len() == 1 && self.entries[0].position == 0 {
            match &self.entries[0].source {
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

        DRAIN_BUFFER.with(|buf| {
            let mut merge_order = buf.borrow_mut();
            self.drain_sorted_into(0, &mut merge_order);
            if merge_order.is_empty() {
                return Rc::new(Batch::empty_with_schema(&self.schema));
            }
            let blob_cap: usize = self.entries.iter().map(|e| e.source_blob_len()).sum();
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
        })
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
        if !self.valid || self.entries.len() != 1 {
            return None;
        }
        let entry = &self.entries[0];
        let start = entry.position;
        let remaining = entry.count - start;
        let row_count = if limit > 0 { remaining.min(limit) } else { remaining };
        let schema = &self.schema;

        let batch = match &entry.source {
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
        self.entries[0].position = start + row_count;
        self.find_next_non_ghost();
        Some(batch)
    }

    /// If this cursor is backed by exactly one in-memory `Batch` source,
    /// returns a `MemBatch` view over it and the current row position
    /// (`entry.position`).  Returns `None` for multi-source or shard-backed
    /// cursors.
    ///
    /// Used by the bulk retraction path in the catalog to avoid per-row
    /// overhead when copying a contiguous range of rows from a single
    /// in-memory source.
    pub(crate) fn single_mem_batch(&self) -> Option<(MemBatch<'_>, usize)> {
        if !self.valid || self.entries.len() != 1 {
            return None;
        }
        let entry = &self.entries[0];
        match &entry.source {
            CursorSource::Batch(b) => Some((b.as_mem_batch(), entry.position)),
            CursorSource::Shard(_) => None,
        }
    }

    /// Sum of blob arena sizes across every source. Tight upper bound on the
    /// blob bytes a full drain can produce; callers use this to size the
    /// output blob arena.
    pub(crate) fn total_blob_len(&self) -> usize {
        self.entries.iter().map(|e| e.source_blob_len()).sum()
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
            self.advance();
        }
    }

    pub(crate) fn scatter_drained_into(
        &self,
        rows: &[(u32, u32, i64)],
        writer: &mut super::merge::DirectWriter<'_>,
    ) {
        if rows.is_empty() { return; }
        super::merge::scatter_unified_sources_with_weights(&self.unified_sources, rows, writer);
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
    let mut entries = Vec::with_capacity(batches.len() + shard_arcs.len());

    for batch in batches {
        if batch.count > 0 {
            entries.push(ReadCursorEntry::new_batch(Rc::clone(batch)));
        }
    }

    for shard in shard_arcs {
        if shard.count > 0 {
            entries.push(ReadCursorEntry::new_shard(Rc::clone(shard)));
        }
    }

    ReadCursor::new(entries, schema)
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
    use crate::schema::{SchemaColumn, SchemaDescriptor, MAX_COLUMNS};

    fn make_schema_i64() -> SchemaDescriptor {
        let mut columns = [SchemaColumn {
            type_code: 0,
            size: 0,
            nullable: 0,
            _pad: 0,
        }; MAX_COLUMNS];
        columns[0] = SchemaColumn {
            type_code: crate::schema::type_code::U128,
            size: 16,
            nullable: 0,
            _pad: 0,
        };
        columns[1] = SchemaColumn {
            type_code: crate::schema::type_code::I64,
            size: 8,
            nullable: 0,
            _pad: 0,
        };
        SchemaDescriptor {
            num_columns: 2,
            pk_index: 0,
            columns,
        }
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
        let entries: Vec<ReadCursorEntry> = vec![];
        let cursor = ReadCursor::new(entries, schema);
        assert!(!cursor.valid);
    }

    #[test]
    fn test_single_batch_scan() {
        let schema = make_schema_i64();
        let batch = make_batch(&[(1, 1, 10), (2, 1, 20), (3, 1, 30)]);
        let entries = vec![ReadCursorEntry::new_batch(batch)];
        let mut cursor = ReadCursor::new(entries, schema);
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
        let entries = vec![
            ReadCursorEntry::new_batch(b1),
            ReadCursorEntry::new_batch(b2),
        ];
        let mut cursor = ReadCursor::new(entries, schema);
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
        let entries = vec![
            ReadCursorEntry::new_batch(b1),
            ReadCursorEntry::new_batch(b2),
        ];
        let mut cursor = ReadCursor::new(entries, schema);
        let rows = scan_all(&mut cursor);
        // pk=5 cancelled (w=+1-1=0), only pk=10 survives
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0], (10, 0, 1));
    }

    #[test]
    fn test_seek() {
        let schema = make_schema_i64();
        let batch = make_batch(&[(1, 1, 10), (5, 1, 50), (10, 1, 100)]);
        let entries = vec![ReadCursorEntry::new_batch(batch)];
        let mut cursor = ReadCursor::new(entries, schema);

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
        let entries = vec![
            ReadCursorEntry::new_batch(b1),
            ReadCursorEntry::new_batch(b2),
        ];
        let mut cursor = ReadCursor::new(entries, schema);
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
        let entries = vec![
            ReadCursorEntry::new_batch(b1),
            ReadCursorEntry::new_batch(b2),
        ];
        let mut cursor = ReadCursor::new(entries, schema);
        let rows = scan_all(&mut cursor);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0], (5, 0, 10)); // 3 + 7 = 10
    }

    #[test]
    fn test_drain_single_source_full() {
        let schema = make_schema_i64();
        let batch = make_batch(&[(1, 1, 10), (2, 1, 20), (3, 1, 30)]);
        let entries = vec![ReadCursorEntry::new_batch(batch)];
        let mut cursor = ReadCursor::new(entries, schema);

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
        let entries = vec![ReadCursorEntry::new_batch(batch)];
        let mut cursor = ReadCursor::new(entries, schema);

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
        let entries = vec![
            ReadCursorEntry::new_batch(b1),
            ReadCursorEntry::new_batch(b2),
        ];
        let mut cursor = ReadCursor::new(entries, schema);
        assert!(cursor.drain_single_source(0).is_none());
    }

    #[test]
    fn test_col_ptr_pk_returns_null() {
        // PK (logical col 0, pk_index=0) must always return null — callers read
        // the PK through current_key instead.
        let schema = make_schema_i64();
        let batch = make_batch(&[(42, 1, 99)]);
        let entries = vec![ReadCursorEntry::new_batch(batch)];
        let cursor = ReadCursor::new(entries, schema);
        assert!(cursor.valid);
        let pk_index = cursor.schema.pk_index as usize; // 0
        let ptr = cursor.col_ptr(pk_index, 16);
        assert!(ptr.is_null(), "col_ptr for PK index must return null");
    }

    #[test]
    fn test_col_ptr_payload_returns_valid_pointer() {
        // Payload col at logical index 1 must return a non-null pointer with
        // the correct value.
        let schema = make_schema_i64();
        let batch = make_batch(&[(7, 1, 1234)]);
        let entries = vec![ReadCursorEntry::new_batch(batch)];
        let cursor = ReadCursor::new(entries, schema);
        assert!(cursor.valid);
        let ptr = cursor.col_ptr(1, 8); // logical col 1 = i64 payload
        assert!(!ptr.is_null(), "col_ptr for payload col must not be null");
        let val = i64::from_le_bytes(unsafe { *(ptr as *const [u8; 8]) });
        assert_eq!(val, 1234);
    }

    #[test]
    fn test_col_ptr_invalid_cursor_returns_null() {
        let schema = make_schema_i64();
        let entries: Vec<ReadCursorEntry> = vec![];
        let cursor = ReadCursor::new(entries, schema);
        assert!(!cursor.valid);
        assert!(cursor.col_ptr(1, 8).is_null());
    }

    #[test]
    fn test_estimated_length_reflects_remaining() {
        let schema = make_schema_i64();
        let batch = make_batch(&[(1, 1, 10), (2, 1, 20), (3, 1, 30)]);
        let entries = vec![ReadCursorEntry::new_batch(batch)];
        let mut cursor = ReadCursor::new(entries, schema);
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
        let entries = vec![ReadCursorEntry::new_batch(batch)];
        let cursor = ReadCursor::new(entries, schema);
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

        let entries = vec![ReadCursorEntry::new_shard(shard)];
        let cursor = ReadCursor::new(entries, schema);
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
        let entries: Vec<ReadCursorEntry> = batches.iter()
            .map(|b| ReadCursorEntry::new_batch(Rc::clone(b)))
            .collect();
        let cursor = ReadCursor::new(entries, schema);
        let result = cursor.materialize();

        assert_eq!(result.count, n);
        for i in 0..n {
            assert_eq!(result.get_pk(i), i as u128);
        }
    }

    /// Drain a cursor capturing the full per-row state both code paths must
    /// agree on: PK, net weight, null word, and the i64 payload column.
    fn drain_with_state(cursor: &mut ReadCursor) -> Vec<(u128, i64, u64, i64)> {
        let mut out = Vec::new();
        while cursor.valid {
            let val_ptr = cursor.col_ptr(1, 8);
            let val = if val_ptr.is_null() {
                0
            } else {
                i64::from_le_bytes(unsafe { *(val_ptr as *const [u8; 8]) })
            };
            out.push((
                cursor.current_key,
                cursor.current_weight,
                cursor.current_null_word,
                val,
            ));
            cursor.advance();
        }
        out
    }

    /// Property test: the n=2 fast path (TreeKind::Two) must emit identical
    /// rows to the heap path (TreeKind::Heap) for every input shape.  We
    /// force the heap path by appending a sentinel-empty third entry; its
    /// `is_valid()` is always false so it never participates in heap
    /// operations but n>=3 selects TreeKind::Heap.
    #[test]
    fn test_n2_path_matches_heap_path() {
        // (rows_a, rows_b) — both inputs assumed pre-sorted by (PK, payload).
        let cases: &[(&[(u128, i64, i64)], &[(u128, i64, i64)])] = &[
            // Distinct PKs interleaved.
            (&[(1, 1, 10), (3, 1, 30)], &[(2, 1, 20), (4, 1, 40)]),
            // Tied (PK, payload) with non-zero net weight (weights sum).
            (&[(5, 3, 50)], &[(5, 7, 50)]),
            // Tied (PK, payload) summing to zero — ghost, should be skipped.
            (&[(5, 1, 50), (10, 1, 100)], &[(5, -1, 50)]),
            // PK-equal but payload-different on both sides — two distinct rows.
            (&[(5, 1, 200)], &[(5, 1, 100)]),
            // One-side exhaustion + remainder on the other.
            (&[(1, 1, 10)], &[(2, 1, 20), (3, 1, 30), (4, 1, 40)]),
            // Other-side exhaustion + remainder.
            (&[(2, 1, 20), (3, 1, 30), (4, 1, 40)], &[(1, 1, 10)]),
            // Mixed: distinct, tied non-zero, then ghost.
            (
                &[(1, 1, 10), (5, 1, 50), (10, 1, 100)],
                &[(3, 1, 30), (5, 1, 50), (10, -1, 100)],
            ),
            // Both sides emit interleaved ghosts: every PK is a tied ghost.
            (
                &[(1, 1, 10), (2, 1, 20), (3, 1, 30)],
                &[(1, -1, 10), (2, -1, 20), (3, -1, 30)],
            ),
            // A single side empty.
            (&[], &[(1, 1, 10), (2, 1, 20)]),
            (&[(1, 1, 10), (2, 1, 20)], &[]),
        ];

        for (i, (a, b)) in cases.iter().enumerate() {
            // Skip cases where both sides are empty (would produce TreeKind::None).
            if a.is_empty() && b.is_empty() {
                continue;
            }

            // n=2 path: include even empty batches so entries.len() == 2.
            let two_entries: Vec<ReadCursorEntry> = vec![
                ReadCursorEntry::new_batch(make_batch(a)),
                ReadCursorEntry::new_batch(make_batch(b)),
            ];
            let mut cursor_two = ReadCursor::new(two_entries, make_schema_i64());
            let drained_two = drain_with_state(&mut cursor_two);

            // Heap path: same two batches plus a sentinel-empty third entry
            // (is_valid() always false → never enters min_indices).
            let three_entries: Vec<ReadCursorEntry> = vec![
                ReadCursorEntry::new_batch(make_batch(a)),
                ReadCursorEntry::new_batch(make_batch(b)),
                ReadCursorEntry::new_batch(make_batch(&[])),
            ];
            let mut cursor_heap = ReadCursor::new(three_entries, make_schema_i64());
            let drained_heap = drain_with_state(&mut cursor_heap);

            assert_eq!(
                drained_two, drained_heap,
                "case {} mismatch:\n  two={:?}\n  heap={:?}",
                i, drained_two, drained_heap,
            );
        }
    }
}
