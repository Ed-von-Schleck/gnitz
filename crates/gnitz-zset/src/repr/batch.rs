//! Owned columnar batch type for Z-set rows.
//!
//! `Batch` owns its memory (two pooled buffers — data + blob); `MemBatch<'a>`
//! is its borrowed slice-view counterpart.

use std::ops::Range;

use super::batch_pool::{is_tight, PooledBuf};
use super::merge;
use super::run::StoredRow;
use super::scatter::{copy_ranges, Pick};
use super::seek::ColPtr;
use super::string_heap::{self, relocated_cell, BlobCache};
use super::writer::DirectWriter;
use crate::schema::{Generic, PayloadOrder, SchemaDescriptor};
use gnitz_expr::{BatchView, ColumnLocator};
use gnitz_wire::{
    read_i64_le, read_u64_le, Heap, RowSource, TypeCode, FIXED_REGION_BYTES, REG_NULL_BMP, REG_PAYLOAD_START, REG_PK,
    REG_WEIGHT,
};

/// Total rows in a `[start, end)` row-range list — the shape every range-driven
/// path sizes its destination by.
#[inline]
pub(crate) fn range_rows(ranges: &[(usize, usize)]) -> usize {
    ranges.iter().map(|&(s, e)| e - s).sum()
}

/// The maximal `[start, end)` runs of `items`' positions whose item `keep`
/// admits, ascending.
pub(crate) fn runs_where<T>(items: &[T], keep: impl Fn(&T) -> bool) -> Vec<(usize, usize)> {
    runs_where_zip(items, items, |x, _| keep(x))
}

/// [`runs_where`] over two equal-length slices read in lockstep: the runs of
/// positions `i` where `keep(&a[i], &b[i])`. Each 64-position word is a fold
/// over a zip of slice chunks, so no position is bounds-checked.
pub(crate) fn runs_where_zip<A, B>(a: &[A], b: &[B], keep: impl Fn(&A, &B) -> bool) -> Vec<(usize, usize)> {
    debug_assert_eq!(a.len(), b.len());
    let kept: Vec<u64> = a
        .chunks(64)
        .zip(b.chunks(64))
        .map(|(ca, cb)| {
            ca.iter()
                .zip(cb)
                .enumerate()
                .fold(0, |bits, (j, (x, y))| bits | u64::from(keep(x, y)) << j)
        })
        .collect();
    let mut runs = Vec::new();
    gnitz_wire::set_bit_runs(&kept, a.len(), &mut runs);
    runs
}

/// Copy `count` rows of every region from an arena of `src_cap` rows onto one
/// of `dst_cap` rows, both laid out under `schema`. Arenas of one capacity
/// hold every region at the same place, so they copy as one run.
fn copy_regions(schema: &SchemaDescriptor, src: &[u8], src_cap: usize, dst: &mut [u8], dst_cap: usize, count: usize) {
    if src_cap == dst_cap {
        let last = schema.num_fixed_regions() - 1;
        let used = schema.region_start(last, src_cap) + count * schema.region_stride(last);
        dst[..used].copy_from_slice(&src[..used]);
        return;
    }
    for r in 0..schema.num_fixed_regions() {
        let len = count * schema.region_stride(r);
        let (s, d) = (schema.region_start(r, src_cap), schema.region_start(r, dst_cap));
        dst[d..d + len].copy_from_slice(&src[s..s + len]);
    }
}

/// A batch's end at one moment: its row count, and its heap's length and
/// dead-byte bound.
#[derive(Clone, Copy)]
pub(crate) struct RowMark {
    count: usize,
    blob_len: usize,
    dead_heap: usize,
}

/// Which row of a batch before [`Batch::consolidate_recording`] each row after
/// it was.
pub(crate) enum RowOrigins {
    /// The rows stood where they were.
    Unmoved,
    /// The pick [`merge::consolidate_groups`] found per output row.
    Moved(Vec<Pick>),
}

impl RowOrigins {
    /// The row that output row `out_row` was.
    #[inline]
    pub(crate) fn source_row(&self, out_row: usize) -> usize {
        match self {
            RowOrigins::Unmoved => out_row,
            RowOrigins::Moved(survivors) => survivors[out_row].1 as usize,
        }
    }
}

/// Owned columnar batch.  All fixed-stride column data lives in a single
/// contiguous `data` buffer.  Blob data is separate (variable-length).
///
/// Layout of `data`: `[pk | weight | null_bmp | col_0 | ... | col_{N-1}]`,
/// the regions back to back. Each has `capacity * stride` bytes allocated;
/// `count * stride` bytes contain data. The PK region uses pk_stride bytes/row;
/// 8 for U64, 16 for U128, wider for compound PKs.
///
/// **2 heap allocations**: `data` and `blob`.
pub struct Batch {
    data: PooledBuf,
    blob: PooledBuf,
    /// An upper bound on the bytes of `blob` no string cell references.
    dead_heap: usize,
    /// Rows the arena has room for: region `r` starts at
    /// `schema.region_start(r, capacity)`.
    capacity: usize,
    /// Live row count.
    count: usize,
    /// When set, the rows are strictly (PK, payload)-increasing and ghost-free;
    /// a debug build verifies it wherever it is raised.
    consolidated: bool,
    /// The schema this batch's rows were laid out under, and the one every
    /// operation on this batch alone runs under. Replaced by
    /// [`Self::set_schema`] and by [`Self::append_above`] onto an empty batch.
    schema: SchemaDescriptor,
}

impl Batch {
    // ── Constructors ────────────────────────────────────────────────────

    /// Zero-allocation empty batch under `schema`.
    pub fn empty_with_schema(schema: &SchemaDescriptor) -> Self {
        Batch {
            data: PooledBuf::default(),
            blob: PooledBuf::default(),
            dead_heap: 0,
            capacity: 0,
            count: 0,
            consolidated: false,
            schema: *schema,
        }
    }

    /// Move this batch out, leaving an empty one of its schema behind — the
    /// VM register-swap idiom, with the slot's shape kept truthful.
    pub fn take(&mut self) -> Self {
        let empty = Self::empty_with_schema(&self.schema);
        std::mem::replace(self, empty)
    }

    /// An empty batch with room for `rows` rows. The arena is uninitialized
    /// (poisoned in debug builds), and filled as `DirectWriter` says.
    pub fn with_capacity(schema: &SchemaDescriptor, rows: usize) -> Self {
        let mut b = Self::empty_with_schema(schema);
        b.capacity = schema.arena_rows(rows);
        // SAFETY: a `Batch` reads no row at or past `count`, which is 0.
        b.data = unsafe { PooledBuf::uninit(b.capacity * schema.row_width()) };
        b.debug_poison_rows(0..b.capacity);
        b
    }

    /// [`Self::with_capacity`] with the blob heap pre-sized. For the callers that
    /// know the byte count up front; `with_capacity` leaves it empty because most
    /// writers do not.
    fn with_capacity_blob(schema: &SchemaDescriptor, rows: usize, blob_bytes: usize) -> Self {
        let mut b = Self::with_capacity(schema, rows);
        b.reserve_blob(blob_bytes);
        b
    }

    /// Room for `bytes` more blob heap bytes, taken from the buffer pool —
    /// a bare [`Vec::reserve`] would take them from the global allocator. A heap
    /// already holding bytes has to grow in place. A schema with no string
    /// column has no heap to reserve.
    fn reserve_blob(&mut self, bytes: usize) {
        if !self.schema.has_german_string() {
            return;
        }
        match self.blob.capacity() {
            0 => self.blob = PooledBuf::with_capacity(bytes),
            _ => self.blob.reserve(bytes),
        }
    }

    /// An owned copy of `mb`'s `count` rows and its whole heap, in an arena
    /// sized for them: unconsolidated, `mb`'s dead-byte bound.
    pub(in crate::repr) fn from_mem_batch(mb: &MemBatch) -> Self {
        let schema = mb.schema;
        let mut b = Self::empty_with_schema(schema);
        let cap = schema.arena_rows(mb.count);
        // SAFETY: `copy_regions` writes the `count` rows a `Batch` reads.
        b.data = unsafe { PooledBuf::uninit(cap * schema.row_width()) };
        copy_regions(schema, mb.data, mb.cap, &mut b.data, cap, mb.count);
        b.blob = PooledBuf::with_capacity(mb.blob.len());
        b.blob.extend_from_slice(mb.blob);
        b.dead_heap = mb.dead_heap;
        b.capacity = cap;
        b.count = mb.count;
        b.debug_poison_rows(mb.count..cap);
        b
    }

    // ── Schema installation ─────────────────────────────────────────────

    /// The schema this batch's rows were laid out under.
    #[inline]
    pub fn schema(&self) -> &SchemaDescriptor {
        &self.schema
    }

    /// Relabel this batch under `s`, a schema of its own regions: the same PK
    /// types in PK-list order and payload types in payload order, so its bytes
    /// and its (PK, payload) order are unchanged, and only the column numbering
    /// and nullability may differ. A debug build asserts that.
    #[inline]
    pub fn set_schema(&mut self, s: &SchemaDescriptor) {
        debug_assert!(
            self.schema.same_region_types(s),
            "Batch::set_schema: a schema of other regions",
        );
        self.schema = *s;
    }

    // ── Read accessors ──────────────────────────────────────────────────

    /// Live row count.
    #[inline(always)]
    pub fn len(&self) -> usize {
        self.count
    }
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// The string heap the long German-string cells index into.
    #[inline(always)]
    pub fn blob(&self) -> &[u8] {
        &self.blob
    }

    /// The bytes the string heap has room for.
    #[cfg(test)]
    pub(in crate::repr) fn blob_capacity(&self) -> usize {
        self.blob.capacity()
    }

    /// An upper bound on the bytes of the heap no string cell references.
    #[inline]
    pub fn dead_heap(&self) -> usize {
        self.dead_heap
    }

    /// Charge `bytes` more of the heap as unreferenced, capped at its length.
    #[inline]
    pub(super) fn charge_dead(&mut self, bytes: usize) {
        self.dead_heap = self.dead_heap.saturating_add(bytes).min(self.blob.len());
    }

    /// These rows over their heap padded with `pad` unreferenced bytes, charged dead.
    #[cfg(test)]
    pub(crate) fn padded(mut self, pad: usize) -> Batch {
        self.blob.extend(std::iter::repeat_n(0u8, pad));
        self.dead_heap += pad;
        self
    }

    /// The live `count * stride` bytes of region `r` — the one range computation
    /// every fixed-region accessor below shares, and the slice the wire and shard
    /// framers build their region lists from.
    #[inline(always)]
    pub(super) fn region(&self, r: usize) -> &[u8] {
        let off = self.region_start(r);
        &self.data[off..off + self.count * self.schema.region_stride(r)]
    }

    /// Where region `r` starts in the arena.
    #[inline(always)]
    fn region_start(&self, r: usize) -> usize {
        self.schema.region_start(r, self.capacity)
    }

    #[inline]
    pub fn pk(&self) -> &[u8] {
        self.region(REG_PK)
    }
    #[inline]
    pub(crate) fn weight(&self) -> &[u8] {
        self.region(REG_WEIGHT)
    }
    #[inline]
    pub(crate) fn null_bmp(&self) -> &[u8] {
        self.region(REG_NULL_BMP)
    }

    /// The `[start, end)` runs of rows with no null bit of `mask` set.
    pub(crate) fn runs_without_nulls(&self, mask: u64) -> Vec<(usize, usize)> {
        let words = self.null_bmp().as_chunks::<FIXED_REGION_BYTES>().0;
        // A batch with none of them set takes only this branch-free pass.
        if words.iter().fold(0u64, |a, w| a | u64::from_le_bytes(*w)) & mask == 0 {
            return vec![(0, self.count)];
        }
        runs_where(words, |w| u64::from_le_bytes(*w) & mask == 0)
    }
    #[inline]
    pub fn col_data(&self, pi: usize) -> &[u8] {
        self.region(REG_PAYLOAD_START + pi)
    }

    // ── Row accessors ───────────────────────────────────────────────────

    /// Owned-`Batch` sibling of `MemBatch::get_pk_bytes`: row `row`'s
    /// `pk_stride` OPK bytes.
    #[inline(always)]
    pub fn get_pk_bytes(&self, row: usize) -> &[u8] {
        let stride = self.schema.pk_stride();
        let off = row * stride;
        &self.data[off..off + stride]
    }
    #[inline(always)]
    pub fn get_weight(&self, row: usize) -> i64 {
        read_i64_le(&self.data, self.region_start(REG_WEIGHT) + row * FIXED_REGION_BYTES)
    }
    /// Indices of the live rows — those at positive weight, the elements the
    /// batch asserts.
    #[inline(always)]
    pub fn live_rows(&self) -> impl Iterator<Item = usize> + '_ {
        (0..self.count).filter(move |&i| self.get_weight(i) > 0)
    }
    /// Indices of the retracted rows — those at negative weight.
    #[inline(always)]
    pub fn retracted_rows(&self) -> impl Iterator<Item = usize> + '_ {
        (0..self.count).filter(move |&i| self.get_weight(i) < 0)
    }
    /// Apply `f` to every row's weight in place. Generic so the per-epoch
    /// callers (negate, delta doubling) monomorphize to a tight loop. The
    /// consolidated claim is untouched: weights are not part of element identity, so
    /// `f` must send no non-zero weight to zero.
    #[inline]
    pub fn map_weights(&mut self, f: impl Fn(i64) -> i64) {
        let off = self.region_start(REG_WEIGHT);
        for chunk in self.data[off..off + self.count * FIXED_REGION_BYTES]
            .as_chunks_mut::<FIXED_REGION_BYTES>()
            .0
        {
            *chunk = f(i64::from_le_bytes(*chunk)).to_le_bytes();
        }
        debug_assert!(
            !(self.consolidated && self.has_ghost()),
            "map_weights: a weight mapped to zero under the consolidated claim",
        );
    }
    /// Every weight's sign flipped: the Z-set inverse. `wrapping_neg` because
    /// `i64::MIN` must not panic; element identity is untouched, so the
    /// consolidated claim carries over.
    pub fn negated(mut self) -> Batch {
        self.map_weights(i64::wrapping_neg);
        self
    }
    /// True iff every row's weight is `> 0` — vacuously true for an empty batch.
    /// Branch-free over the contiguous weight region, so
    /// the conforming case is one pass with no early exit to serialize it.
    #[inline]
    pub fn all_weights_positive(&self) -> bool {
        !self
            .weight()
            .as_chunks::<FIXED_REGION_BYTES>()
            .0
            .iter()
            .fold(false, |bad, w| bad | (i64::from_le_bytes(*w) <= 0))
    }
    /// Whether any row is a ghost, at weight zero. Branch-free over the weight
    /// region, like [`Self::all_weights_positive`].
    #[inline]
    pub fn has_ghost(&self) -> bool {
        self.weight()
            .as_chunks::<FIXED_REGION_BYTES>()
            .0
            .iter()
            .fold(false, |any, w| any | (*w == [0; FIXED_REGION_BYTES]))
    }
    #[inline(always)]
    pub fn get_null_word(&self, row: usize) -> u64 {
        read_u64_le(&self.data, self.region_start(REG_NULL_BMP) + row * FIXED_REGION_BYTES)
    }
    #[inline(always)]
    pub(crate) fn get_col_ptr(&self, row: usize, payload_col: usize, col_size: usize) -> &[u8] {
        let off = self.region_start(REG_PAYLOAD_START + payload_col) + row * col_size;
        &self.data[off..off + col_size]
    }

    /// A span-dedup cache sized for `rows` rows of this batch's schema: the most
    /// string cells they hold.
    #[inline]
    pub(super) fn blob_cache(&self, rows: usize) -> BlobCache {
        BlobCache::new(rows * self.schema.string_payload_slots().count_ones() as usize)
    }

    // ── Extend methods (building batches row-by-row) ────────────────────

    /// Ensure the data buffer has room for at least `n` more rows beyond `count`.
    /// `#[inline]` for the already-has-room test, which is the whole call on
    /// every append but the growing one.
    #[inline]
    fn reserve_rows(&mut self, n: usize) {
        if self.count + n <= self.capacity {
            return;
        }
        let schema = &self.schema;
        let new_cap = schema.arena_rows((self.capacity * 2).max(8).max(self.count + n));
        let new_total = new_cap * schema.row_width();
        if new_total > self.data.capacity() {
            // SAFETY: `copy_regions` writes the `count` rows a `Batch` reads.
            let mut new_data = unsafe { PooledBuf::uninit(new_total) };
            copy_regions(schema, &self.data, self.capacity, &mut new_data, new_cap, self.count);
            self.data = new_data;
        } else {
            // The arena has room already: shift each region to its new start, last
            // first, so none lands on one still to move.
            // SAFETY: `new_total` is within the capacity, and the bytes past the old
            // length are rows no reader reaches.
            unsafe { self.data.set_len(new_total) };
            for r in (0..schema.num_fixed_regions()).rev() {
                let old = schema.region_start(r, self.capacity);
                let len = self.count * schema.region_stride(r);
                self.data.copy_within(old..old + len, schema.region_start(r, new_cap));
            }
        }
        self.capacity = new_cap;
        self.debug_poison_rows(self.count..new_cap);
    }

    /// In debug builds, fill `rows` of every region with `0xA5`, so reading a row
    /// nothing wrote sees an implausible value rather than a plausible leftover.
    fn debug_poison_rows(&mut self, rows: Range<usize>) {
        if cfg!(debug_assertions) {
            for r in 0..self.schema.num_fixed_regions() {
                let (start, s) = (self.region_start(r), self.schema.region_stride(r));
                self.data[start + rows.start * s..start + rows.end * s].fill(0xA5);
            }
        }
    }

    /// The open row's cell of region `r`, whose cells are `stride` bytes.
    #[inline(always)]
    fn row_cell_mut(&mut self, r: usize, stride: usize) -> &mut [u8] {
        debug_assert!(self.count < self.capacity, "no row is open: `begin_row` makes its room");
        let off = self.region_start(r) + self.count * stride;
        &mut self.data[off..off + stride]
    }

    /// Write `d`, one cell, into payload column `pi` of the open row.
    #[inline(always)]
    pub(crate) fn extend_col(&mut self, pi: usize, d: &[u8]) {
        let r = REG_PAYLOAD_START + pi;
        debug_assert_eq!(
            d.len(),
            self.schema.region_stride(r),
            "extend_col: a cell of another width"
        );
        self.row_cell_mut(r, d.len()).copy_from_slice(d);
    }

    /// [`Self::extend_col`] for a STRING/BLOB column: `content` is encoded into
    /// this batch's own blob heap, whose offsets no other batch's cell can name.
    #[inline]
    pub(crate) fn extend_col_blob(&mut self, pi: usize, content: &[u8]) {
        let cell = gnitz_wire::encode_german_string(content, &mut self.blob);
        self.extend_col(pi, &cell);
    }

    /// NULL in payload column `pi` of the open row: a zeroed cell under its bit.
    #[inline]
    pub(crate) fn put_null(&mut self, pi: usize) {
        let r = REG_PAYLOAD_START + pi;
        let width = self.schema.region_stride(r);
        self.row_cell_mut(r, width).fill(0);
        self.row_cell_mut(REG_NULL_BMP, FIXED_REGION_BYTES)[pi / 8] |= 1 << (pi % 8);
    }

    /// Open a row: its key (exactly `pk_stride` OPK bytes), its weight, and a
    /// null word with no bit set. The caller then writes every payload column,
    /// in any order, and closes with [`Self::commit_row`]; nothing reads the
    /// row before that.
    #[inline(always)]
    pub(crate) fn begin_row(&mut self, pk_bytes: &[u8], weight: i64) {
        self.open_row(pk_bytes, weight, 0);
    }

    /// [`Self::begin_row`] under a null word the caller already holds.
    #[inline(always)]
    fn open_row(&mut self, pk_bytes: &[u8], weight: i64, null_word: u64) {
        assert_eq!(
            pk_bytes.len(),
            self.schema.pk_stride(),
            "begin_row: the key must be exactly pk_stride bytes",
        );
        if self.count == self.capacity {
            self.reserve_rows(1);
        }
        self.row_cell_mut(REG_PK, pk_bytes.len()).copy_from_slice(pk_bytes);
        self.row_cell_mut(REG_WEIGHT, FIXED_REGION_BYTES)
            .copy_from_slice(&weight.to_le_bytes());
        self.row_cell_mut(REG_NULL_BMP, FIXED_REGION_BYTES)
            .copy_from_slice(&null_word.to_le_bytes());
    }

    /// Close the row [`Self::begin_row`] opened.
    #[inline(always)]
    pub(crate) fn commit_row(&mut self) {
        self.count += 1;
        self.consolidated = false;
    }

    /// Append one whole row of a **payload-free** schema — an index entry, whose
    /// columns are all PK, so its key is the entire row.
    #[inline]
    pub fn push_key_row(&mut self, pk: &[u8], weight: i64) {
        debug_assert_eq!(
            self.schema.num_payload_cols(),
            0,
            "push_key_row is the payload-free schema; use BatchBuilder",
        );
        self.begin_row(pk, weight);
        self.commit_row();
    }

    /// Write `rows` rows past the last through `fill`, relocating strings under
    /// one span-dedup cache. They are counted once `fill` returns, so no reader
    /// reaches a row before its regions are written.
    #[inline(always)]
    pub(crate) fn write_rows<R>(&mut self, rows: usize, fill: impl FnOnce(&mut DirectWriter<'_>) -> R) -> R {
        let mut cache = self.blob_cache(rows);
        self.write_at_most(rows, &mut cache, |w| (rows, fill(w)))
    }

    /// [`Self::write_rows`] under the caller's `cache`, whose every relocation
    /// landed in this batch's heap, for a `fill` that may write fewer than `rows`
    /// rows: it returns how many it wrote beside its result, and only those are
    /// counted.
    #[inline(always)]
    pub(super) fn write_at_most<R>(
        &mut self,
        rows: usize,
        cache: &mut BlobCache,
        fill: impl FnOnce(&mut DirectWriter<'_>) -> (usize, R),
    ) -> R {
        self.reserve_rows(rows);
        self.consolidated = false;
        let (written, out) = fill(&mut DirectWriter::over(
            &mut self.data,
            self.capacity,
            self.count,
            rows,
            &self.schema,
            &mut self.blob,
            Some(cache),
        ));
        assert!(written <= rows, "write_at_most: more rows written than it had room for");
        self.count += written;
        out
    }

    /// Append every listed range of `src`, a batch of this one's layout, in list
    /// order: fixed-width regions in bulk, its string cells shifted onto its heap
    /// when [`Self::carry_heap`] carries it, or relocated under `cache`, whose
    /// every relocation landed in this batch's heap.
    fn push_ranges(&mut self, src: &MemBatch<'_>, ranges: &[(usize, usize)], cache: &mut BlobCache) {
        debug_assert!(
            self.schema.same_region_types(src.schema),
            "push_ranges: a source of another layout"
        );
        let total: usize = ranges
            .iter()
            .map(|&(start, end)| {
                assert!(start <= end, "push_ranges: start ({start}) > end ({end})");
                assert!(end <= src.count, "push_ranges: end ({end}) > src.count ({})", src.count);
                end - start
            })
            .sum();
        if total == 0 {
            return;
        }
        let heap_at = self.carry_heap(src, self.schema.string_payload_slots(), ranges);
        self.write_at_most(total, cache, |w| (total, copy_ranges(src, heap_at, ranges, w)));
    }

    // ── View, claim, row edits and sizes ────────────────────────────────

    /// Create a borrowed `MemBatch` view over this batch's data.
    ///
    /// `#[inline]`: derived per range and per chunk on paths whose whole body is
    /// a few reads through it.
    #[inline]
    pub fn as_mem_batch(&self) -> MemBatch<'_> {
        MemBatch {
            data: &self.data,
            schema: &self.schema,
            cap: self.capacity,
            blob: &self.blob,
            count: self.count,
            dead_heap: self.dead_heap,
        }
    }

    /// True if the rows are consolidated (strictly (PK, payload)-increasing and
    /// ghost-free). No batch of under two live rows can violate that, so those
    /// answer `true` whatever the cached claim says — which is what keeps a
    /// single-row DML push off the arena + argsort + scatter path.
    #[inline]
    pub fn is_consolidated(&self) -> bool {
        self.count == 0 || self.consolidated || (self.count == 1 && self.get_weight(0) != 0)
    }

    /// Raise the claim. A debug build verifies it; a release build takes it on
    /// trust.
    #[inline]
    pub fn certify_consolidated(&mut self) {
        self.debug_verify_null_bits();
        self.debug_verify_consolidated(self);
        self.consolidated = true;
    }

    /// The batch's current end, to [`Self::truncate_to`] later.
    #[inline]
    pub(crate) fn mark(&self) -> RowMark {
        RowMark {
            count: self.count,
            blob_len: self.blob.len(),
            dead_heap: self.dead_heap,
        }
    }

    /// The rows appended since `mark`.
    #[inline]
    pub(crate) fn rows_since(&self, mark: RowMark) -> usize {
        self.count - mark.count
    }

    /// Drop every row, and every blob byte, appended since `mark`. The dropped
    /// rows must reference only heap bytes appended since it, as relocated or
    /// carried cells do, so the heap left behind is as dead as it was then.
    #[inline]
    pub(crate) fn truncate_to(&mut self, mark: RowMark) {
        debug_assert!(mark.count <= self.count && mark.blob_len <= self.blob.len());
        self.debug_poison_rows(mark.count..self.count);
        self.count = mark.count;
        self.blob.truncate(mark.blob_len);
        self.dead_heap = mark.dead_heap;
    }

    /// Exchange rows `a` and `b`. Their strings index this batch's own blob, so
    /// they move with the rows.
    pub(crate) fn swap_rows(&mut self, a: usize, b: usize) {
        debug_assert!(a < self.count && b < self.count);
        if a == b {
            return;
        }
        self.consolidated = false;
        let (lo, hi) = (a.min(b), a.max(b));
        for r in 0..self.schema.num_fixed_regions() {
            let stride = self.schema.region_stride(r);
            let start = self.region_start(r);
            let (head, tail) = self.data[start..start + self.count * stride].split_at_mut(hi * stride);
            head[lo * stride..(lo + 1) * stride].swap_with_slice(&mut tail[..stride]);
        }
    }

    /// Copy `src`'s claim: sound for a copy of `src` that keeps its
    /// (PK, payload) order, distinctness and weights, which a debug build
    /// verifies.
    #[inline]
    fn inherit_consolidated(&mut self, src: &Batch) {
        self.consolidated = src.consolidated;
        if self.consolidated {
            self.debug_verify_consolidated(self);
        }
    }

    /// Debug-only: every null bit sits under a nullable payload column, over a
    /// zeroed cell.
    pub fn debug_verify_null_bits(&self) {
        if !cfg!(debug_assertions) {
            return;
        }
        if let Some((breach, (row, slot))) = super::batch_wire::null_breach(&self.as_mem_batch()) {
            panic!(
                "batch holds {breach} at row {row}, payload slot {slot}, of {} rows",
                self.count
            );
        }
    }

    /// Debug-only: assert the data is fully consolidated — strictly increasing by
    /// (PK, payload) (no unfolded duplicate) AND no zero-weight row (ghost
    /// eliminated, §2). The weights are this batch's; the rows are compared as
    /// `rows` reads them, which is the batch itself or its rows over another heap.
    pub(super) fn debug_verify_consolidated(&self, rows: &impl RowSource) {
        if !cfg!(debug_assertions) {
            return;
        }
        for i in 0..self.count {
            debug_assert_ne!(
                self.get_weight(i),
                0,
                "batch flagged consolidated, but row {i} has weight 0 (ghost not eliminated)"
            );
            if i + 1 < self.count {
                let ord = Generic.compare_full(&self.schema, rows, i, rows, i + 1);
                debug_assert_eq!(
                    ord,
                    std::cmp::Ordering::Less,
                    "batch flagged consolidated, but rows {i},{} are not strictly \
                     increasing (unsorted or unfolded duplicate)",
                    i + 1
                );
            }
        }
    }

    /// Bytes its two buffers hold allocated, rows or not: what holding this
    /// batch costs, where [`Self::total_bytes`] is what its rows take.
    pub fn allocated_bytes(&self) -> usize {
        self.data.capacity() + self.blob.capacity()
    }

    /// Live row bytes: each region bounded to `count`, plus the blob. Excludes
    /// unused capacity, so a caller sizing a RAM budget against it is measuring
    /// rows held, not bytes allocated.
    pub fn total_bytes(&self) -> usize {
        self.count * self.schema.row_width() + self.blob.len()
    }

    // ── Copies ──────────────────────────────────────────────────────────

    /// The rows `indices` names, in that order, at their own weights; none may be
    /// weight 0.
    pub(crate) fn indexed_rows(&self, indices: &[u32]) -> Self {
        let blob_cap = string_heap::prorated_blob_cap(self.blob.len(), self.count, indices.len());
        write_to_batch(&self.schema, indices.len(), blob_cap, |writer| {
            super::scatter::scatter_copy(&self.as_mem_batch(), indices, writer);
        })
    }

    /// The rows named by a strictly ascending `indices`, inheriting this batch's
    /// claim: taking rows in source order preserves (PK, payload) ordering and
    /// leaves weights untouched, so a consolidated source yields a consolidated
    /// subset. A reordering caller wants
    /// [`indexed_rows`](Self::indexed_rows), whose weight-0 precondition
    /// this inherits.
    pub fn ascending_subset(&self, indices: &[u32]) -> Self {
        debug_assert!(
            indices.windows(2).all(|w| w[0] < w[1]),
            "ascending_subset requires a strictly ascending index list",
        );
        let mut out = self.indexed_rows(indices);
        out.inherit_consolidated(self);
        out
    }

    /// Every row copied into a fresh `out_schema` batch: its weight, and every
    /// payload column landed at output slot `first_slot + pi` over this batch's
    /// heap. `rest` writes what remains — the PK region, the null words, and
    /// any output column no input column lands in.
    pub(crate) fn copied_into(
        &self,
        out_schema: &SchemaDescriptor,
        first_slot: usize,
        rest: impl FnOnce(&mut DirectWriter<'_>),
    ) -> Batch {
        let in_schema = &self.schema;
        debug_assert!(
            out_schema.num_payload_cols() >= first_slot + in_schema.num_payload_cols(),
            "copied_into lands every payload column at first_slot + its own index",
        );
        let n = self.count;
        let mut out = Self::with_capacity(out_schema, n);
        let heap_at = out.carry_heap(&self.as_mem_batch(), in_schema.string_payload_slots(), &[(0, n)]);
        out.write_rows(n, |w| {
            w.weight_mut().copy_from_slice(self.weight());
            for (pi, col) in in_schema.payload_columns() {
                w.col_mut(first_slot + pi).copy_from_slice(self.col_data(pi));
                if col.type_code.is_german_string() {
                    w.rebase_string_col(first_slot + pi, Heap::Plain(&self.blob), heap_at);
                }
            }
            rest(w);
        });
        out
    }

    /// Every row copied into `out_schema`, whose payload space is this batch's,
    /// keyed by its leading key bytes — as many as `out_schema`'s stride. Left
    /// unconsolidated: rows may now share a key.
    pub fn keyed_by_prefix(&self, out_schema: &SchemaDescriptor) -> Batch {
        debug_assert_eq!(out_schema.num_payload_cols(), self.schema.num_payload_cols());
        self.copied_into(out_schema, 0, |w| {
            let (pk, _, nulls) = w.fixed_mut();
            nulls.copy_from_slice(self.null_bmp());
            let keys = pk.chunks_exact_mut(out_schema.pk_stride());
            for (dst, src) in keys.zip(self.pk().chunks_exact(self.schema.pk_stride())) {
                dst.copy_from_slice(&src[..dst.len()]);
            }
        })
    }

    /// Copy every row into `out_schema`, which extends this batch's schema with
    /// NULL-filled payload columns — ahead of this batch's own under
    /// `nulls_first`, behind them otherwise. It must share this batch's PK stride.
    ///
    /// The consolidated claim carries over: an all-NULL column compares equal on every
    /// row, so (PK, payload) order and distinctness stay the original columns'.
    pub fn widened_with_nulls(&self, out_schema: &SchemaDescriptor, nulls_first: bool) -> Self {
        let in_schema = &self.schema;
        debug_assert_eq!(out_schema.pk_stride(), in_schema.pk_stride());
        let in_npc = in_schema.num_payload_cols();
        let out_npc = out_schema.num_payload_cols();
        debug_assert!(out_npc >= in_npc);
        let n_new = out_npc - in_npc;
        let (first_slot, new_slots) = match nulls_first {
            true => (n_new, 0..n_new),
            false => (0, in_npc..out_npc),
        };
        let new_null_bits = gnitz_wire::low_bits_mask(n_new);

        let mut output = self.copied_into(out_schema, first_slot, |w| {
            // The new columns are NULL, and a NULL cell is zeroed.
            for pi in new_slots {
                w.col_mut(pi).fill(0);
            }
            let (pk, _, nulls) = w.fixed_mut();
            pk.copy_from_slice(self.pk());
            let out_words = nulls.as_chunks_mut::<8>().0.iter_mut();
            for (out, word) in out_words.zip(self.null_bmp().as_chunks::<8>().0) {
                let in_null = u64::from_le_bytes(*word);
                let out_null = match nulls_first {
                    true => new_null_bits | gnitz_wire::null_word_at(in_null, n_new),
                    false => in_null | gnitz_wire::null_word_at(new_null_bits, in_npc),
                };
                *out = out_null.to_le_bytes();
            }
        });
        output.inherit_consolidated(self);
        output
    }

    // ── Seeks ───────────────────────────────────────────────────────────

    /// The PK region as the [`ColPtr`] the OPK seeks read through. Its base
    /// aliases `self.data`, so it is read only while `self` is borrowed.
    #[inline]
    fn pk_col_ptr(&self) -> ColPtr {
        ColPtr {
            // The PK region is the arena's first.
            base: self.data.as_ptr(),
            stride: self.schema.pk_stride(),
        }
    }

    /// The first row whose OPK bytes are `>= key`, galloping from `hint` where
    /// there is one; `key` is exactly `pk_stride` bytes.
    pub(crate) fn seek(&self, key: &[u8], hint: Option<usize>) -> usize {
        let (count, stride, pk) = (self.count, self.schema.pk_stride(), self.pk_col_ptr());
        // SAFETY: the PK region holds a key for every row.
        unsafe { super::seek::seek_lower_bound(count, stride, pk, key, hint) }
    }

    /// The rows whose PK is the OPK `key`, exactly `pk_stride` bytes: none, at
    /// its lower bound, where the batch, which is in key order, holds no such row.
    pub fn pk_rows(&self, key: &[u8]) -> Range<usize> {
        // SAFETY: the PK region holds a key for every row.
        unsafe { super::seek::seek_pk_rows(self.count, self.schema.pk_stride(), self.pk_col_ptr(), key, None) }
    }

    /// Whether this batch holds the element of `src`'s row `row`, at any weight.
    pub fn holds_element_of(&self, src: &Batch, row: usize) -> bool {
        let rows = self.pk_rows(src.get_pk_bytes(row));
        let cmp = |i| Generic.compare(&self.schema, self, i, src, row);
        let at = super::seek::lower_bound_by(rows.start, rows.end, |i| cmp(i).is_lt());
        at < rows.end && cmp(at).is_eq()
    }

    /// The rows of this batch, which is in key order, whose PK is one of
    /// `keys`, which ascend: as maximal runs into `out` (cleared first).
    pub fn key_ranges<'k>(&self, keys: impl IntoIterator<Item = &'k [u8]>, out: &mut Vec<(usize, usize)>) {
        debug_assert!(merge::in_consolidated_order(self));
        out.clear();
        let (count, stride, pk) = (self.count, self.schema.pk_stride(), self.pk_col_ptr());
        let mut from = 0;
        for key in keys {
            // SAFETY: the PK region holds a key for every row.
            let rows = unsafe { super::seek::seek_pk_rows(count, stride, pk, key, Some(from)) };
            if rows.start == count {
                break;
            }
            from = rows.end;
            if !rows.is_empty() {
                match out.last_mut() {
                    Some(last) if last.1 == rows.start => last.1 = rows.end,
                    _ => out.push((rows.start, rows.end)),
                }
            }
        }
    }

    // ── Appends, builds and resets ──────────────────────────────────────

    /// Copy every `[start, end)` range of `src`, ascending and disjoint, onto
    /// this batch's tail, one append setup serving the whole list.
    pub fn append_ranges(&mut self, src: &MemBatch<'_>, ranges: &[(usize, usize)]) {
        let mut cache = self.blob_cache(range_rows(ranges));
        self.push_ranges(src, ranges, &mut cache);
    }

    /// Copy each of `rows` onto this batch's tail at `weight`.
    pub fn append_stored(&mut self, rows: &[StoredRow], weight: i64) {
        if rows.is_empty() {
            return;
        }
        self.reserve_rows(rows.len());
        let mut cache = self.blob_cache(rows.len());
        for r in rows {
            self.append_row_from_source(weight, &r.run, r.row, Some(&mut cache));
        }
    }

    /// One `-1` row per key of `pks`, whole `schema` PKs back to back, every
    /// payload cell a zeroed non-NULL filler: the retraction a base table's
    /// unique-PK rule resolves by key alone.
    pub fn key_retractions(schema: &SchemaDescriptor, pks: &[u8]) -> Batch {
        let n = pks.len() / schema.pk_stride();
        debug_assert_eq!(n * schema.pk_stride(), pks.len(), "key_retractions: pks are whole keys");
        write_to_batch(schema, n, 0, |w| {
            for pi in 0..schema.num_payload_cols() {
                w.col_mut(pi).fill(0);
            }
            let (pk, weight, nulls) = w.fixed_mut();
            pk.copy_from_slice(pks);
            nulls.fill(0);
            for word in weight.as_chunks_mut::<8>().0 {
                *word = (-1i64).to_le_bytes();
            }
        })
    }

    /// Copy every row of `src` onto this batch's tail.
    pub fn append_batch(&mut self, src: &Batch) {
        debug_assert_eq!(
            src.schema.nullable_payload_slots() & !self.schema.nullable_payload_slots(),
            0,
            "append_batch: the source admits a NULL this batch's label refuses",
        );
        self.append_ranges(&src.as_mem_batch(), &[(0, src.count)]);
    }

    /// All of `next`, every row of which sorts above every row of this batch in
    /// (PK, payload) order. Consolidated if both were.
    pub fn append_above(&mut self, next: Batch) {
        if self.count == 0 {
            *self = next;
            return;
        }
        if next.count > 0 {
            let order = Generic.compare_full(&self.schema, &*self, self.count - 1, &next, 0);
            assert!(
                order.is_lt(),
                "append_above: the appended rows do not sort above this batch's"
            );
        }
        let consolidated = self.is_consolidated() && next.is_consolidated();
        self.append_batch(&next);
        self.consolidated = consolidated;
    }

    /// The rows of `src`'s disjoint ascending `[start, end)` ranges, inheriting its
    /// claim, in an arena with room for `spare_rows` more.
    pub fn from_ranges(src: &Batch, ranges: &[(usize, usize)], spare_rows: usize) -> Batch {
        let mut out = Batch::with_capacity(&src.schema, range_rows(ranges) + spare_rows);
        out.append_ranges(&src.as_mem_batch(), ranges);
        out.inherit_consolidated(src);
        out
    }

    /// Every source's rows, in order, in one fresh unconsolidated batch.
    pub fn concat<'s>(schema: &SchemaDescriptor, sources: impl Iterator<Item = MemBatch<'s>> + Clone) -> Batch {
        let (mut rows, mut blob) = (0usize, 0usize);
        for src in sources.clone() {
            rows += src.count;
            blob += src.blob.len();
        }
        let mut out = Batch::with_capacity_blob(schema, rows, blob);
        // One dedup cache serves every source, so a fold of one-row runs pays its
        // setup once.
        let mut cache = out.blob_cache(rows);
        for src in sources {
            out.push_ranges(&src, &[(0, src.count)], &mut cache);
        }
        out
    }

    /// No rows and no string heap — so this batch already *is* its own cleared
    /// and its own copied form, and `clear`/`clone` can hand back what
    /// they were given.
    #[inline]
    fn holds_nothing(&self) -> bool {
        self.count == 0 && self.blob.is_empty()
    }

    /// Reset to empty and return both buffers to the pool, in place. Leaves what
    /// [`Self::empty_with_schema`] would build, without `take`'s two moves of the
    /// whole struct — and returns immediately when the batch is already free, which is
    /// what the VM's per-epoch register clear mostly does.
    pub fn release_buffers(&mut self) {
        if self.data.capacity() == 0 && self.blob.capacity() == 0 {
            return;
        }
        self.data = PooledBuf::default();
        self.blob = PooledBuf::default();
        self.capacity = 0;
        self.count = 0;
        self.dead_heap = 0;
        self.consolidated = false;
    }

    /// Reset to empty without freeing buffer allocations.
    pub fn clear(&mut self) {
        if self.holds_nothing() {
            return;
        }
        // The data buffer stays allocated, at its capacity.
        self.truncate_to(RowMark { count: 0, blob_len: 0, dead_heap: 0 });
        self.consolidated = false;
    }

    /// Carry `src`'s heap onto this one to copy the string slots `kept_slots`
    /// of the rows `kept`, when [`string_heap::carry_dead`] admits it: the base
    /// the copied cells shift by, with every byte they cannot reference charged
    /// dead. Declined, it reserves the rows' share of `src`'s heap for the cells
    /// relocated instead — that share, not the whole heap: a many-run merge
    /// appends into one output, and the whole heap per run ratchets capacity.
    pub(crate) fn carry_heap(&mut self, src: &MemBatch<'_>, kept_slots: u64, kept: &[(usize, usize)]) -> Option<usize> {
        let Some(dead) = string_heap::carry_dead(src, kept_slots, kept) else {
            if !src.blob.is_empty() {
                self.reserve_blob(string_heap::prorated_blob_cap(
                    src.blob.len(),
                    src.count,
                    range_rows(kept),
                ));
            }
            return None;
        };
        let base = self.blob.len();
        self.reserve_blob(src.blob.len());
        self.blob.extend_from_slice(src.blob);
        self.charge_dead(dead);
        Some(base)
    }

    /// Append `source[row]` at `weight`; a `blob_cache` dedups repeated long-string
    /// spans.
    pub(super) fn append_row_from_source<S: RowSource>(
        &mut self,
        weight: i64,
        source: &S,
        row: usize,
        mut blob_cache: Option<&mut BlobCache>,
    ) {
        debug_assert_ne!(
            weight, 0,
            "append_row_from_source: zero-weight row (filter before copying)"
        );
        self.open_row(source.get_pk_bytes(row), weight, source.get_null_word(row));
        let src_heap = source.heap();
        for pi in 0..self.schema.num_payload_cols() {
            let col = self.schema.columns()[self.schema.payload_col_idx(pi)];
            let cell = source.get_col_ptr(row, pi, col.size());
            self.append_payload_cell(pi, col.type_code, cell, src_heap, blob_cache.as_deref_mut());
        }
        self.commit_row();
    }

    /// Append `cell` into output slot `out_pi`, relocating a STRING/BLOB struct
    /// into `self.blob` out of `src_heap`.
    #[inline(always)]
    fn append_payload_cell(
        &mut self,
        out_pi: usize,
        type_code: TypeCode,
        cell: &[u8],
        src_heap: Heap<'_>,
        blob_cache: Option<&mut BlobCache>,
    ) {
        if type_code.is_german_string() {
            let dest = relocated_cell(cell.first_chunk().unwrap(), src_heap, &mut self.blob, blob_cache);
            self.extend_col(out_pi, &dest);
        } else {
            self.extend_col(out_pi, cell);
        }
    }

    /// Append the columns at `locs` of `src`'s row as the open row's payload
    /// slots, in order, a NULL one as a NULL.
    #[inline(always)]
    pub(crate) fn append_cells_from<S: RowSource>(&mut self, locs: &[ColumnLocator], src: &S, row: usize) {
        for (out_pi, loc) in locs.iter().enumerate() {
            match *loc {
                ColumnLocator::Pk { .. } => {
                    let mut scratch = [0u8; 16];
                    self.extend_col(out_pi, loc.native_le_bytes(src, row, &mut scratch));
                }
                ColumnLocator::Payload { slot, size, type_code } => {
                    if loc.is_null(src, row) {
                        self.put_null(out_pi);
                        continue;
                    }
                    let cell = src.get_col_ptr(row, slot as usize, size as usize);
                    self.append_payload_cell(out_pi, type_code, cell, src.heap(), None);
                }
            }
        }
    }

    // ── Consolidation and trimming ──────────────────────────────────────

    /// Consume this batch, consolidating it if needed. An already-consolidated
    /// (or empty) `self` is returned by move, allocating nothing.
    ///
    /// `#[inline]`: the move copies the whole struct.
    #[inline]
    pub fn into_consolidated(mut self) -> Batch {
        self.consolidate_in_place();
        self
    }

    /// Consolidate this batch where it stands.
    #[inline]
    pub fn consolidate_in_place(&mut self) {
        if !self.is_consolidated() {
            self.consolidate_unclaimed();
        }
    }

    /// [`Self::consolidate_in_place`] of a batch without the claim.
    #[inline(never)]
    fn consolidate_unclaimed(&mut self) {
        self.consolidate_recording();
    }

    /// Consolidate this batch: rows that stand in consolidated order are
    /// certified where they are, and any others are sorted and weight-folded
    /// into a fresh batch. Returns which row each output row was.
    pub(crate) fn consolidate_recording(&mut self) -> RowOrigins {
        if self.stands_consolidated() {
            self.certify_consolidated();
            return RowOrigins::Unmoved;
        }
        let mut survivors = Vec::with_capacity(self.count);
        *self = Self::consolidated_from(self, &mut survivors);
        RowOrigins::Moved(survivors)
    }

    /// An owned consolidated copy: folds if needed, else clones. The borrowed
    /// counterpart of [`Self::into_consolidated`].
    pub fn to_consolidated(&self) -> Batch {
        if !self.stands_consolidated() {
            return Self::consolidated_from(self, &mut Vec::with_capacity(self.count));
        }
        let mut copy = self.clone();
        if !copy.consolidated {
            copy.certify_consolidated();
        }
        copy
    }

    /// Whether the rows are consolidated, with the claim or without it: a
    /// producer that wrote them in order leaves them unclaimed, and one forward
    /// pass finds that out.
    pub(crate) fn stands_consolidated(&self) -> bool {
        self.is_consolidated() || merge::in_consolidated_order(self)
    }

    /// Sort and weight-fold `batch` into a fresh certified batch, handing back in
    /// `survivors` what [`merge::consolidate_groups`] found: per output
    /// row, the `batch` row it is.
    #[inline(always)]
    fn consolidated_from(batch: &Batch, survivors: &mut Vec<Pick>) -> Batch {
        let schema = &batch.schema;
        let mb = batch.as_mem_batch();
        // Folded first, so the arena is sized to the survivors.
        merge::consolidate_groups(&mb, survivors);
        let mut result = super::scatter::materialize_carrying(std::slice::from_ref(&mb), schema, survivors);
        result.certify_consolidated();
        result
    }

    /// Whether the heap is [`string_heap::heap_is_wasteful`]. The bound counts a
    /// span several cells share once per cell, so a wasteful one is measured
    /// before a compaction is paid for.
    fn heap_is_wasteful(&mut self) -> bool {
        self.debug_verify_dead_heap();
        if string_heap::heap_is_wasteful(self.dead_heap, self.blob.len()) {
            self.dead_heap = string_heap::measure_dead_heap(&self.as_mem_batch());
        }
        string_heap::heap_is_wasteful(self.dead_heap, self.blob.len())
    }

    /// `self`, or a tight copy when its buffers are not [`is_tight`] for its rows
    /// — a [`Self::compacted`] one when its heap is wasteful.
    pub fn trimmed(mut self) -> Batch {
        if self.heap_is_wasteful() {
            return self.compacted();
        }
        let need = self.schema.arena_rows(self.count) * self.schema.row_width() + self.blob.len();
        if is_tight(self.data.capacity() + self.blob.capacity(), need) {
            return self;
        }
        Batch::clone(&self)
    }

    /// `self` in buffers of exactly what its rows take, with a wasteful heap
    /// compacted first: each buffer that holds more is copied into one fresh off
    /// the allocator, since a pooled buffer may hold twice what is asked of it,
    /// and one shrunk in place keeps its tail resident.
    pub fn shrunk(mut self) -> Batch {
        if self.heap_is_wasteful() {
            self = self.compacted();
        }
        let cap = self.schema.arena_rows(self.count);
        let need = cap * self.schema.row_width();
        if self.data.capacity() != need {
            let mut data = vec![0; need];
            copy_regions(&self.schema, &self.data, self.capacity, &mut data, cap, self.count);
            self.data = PooledBuf(data);
            self.capacity = cap;
        }
        if self.blob.capacity() != self.blob.len() {
            self.blob = PooledBuf(self.blob.to_vec());
        }
        self
    }

    /// A tight copy with every long string relocated, so its heap holds no
    /// dead bytes; a copy left with no rows or no long strings holds no heap
    /// buffer at all.
    fn compacted(&self) -> Batch {
        let (n, mb) = (self.count, self.as_mem_batch());
        let mut out = write_to_batch(&self.schema, n, self.blob.len(), |w| {
            copy_ranges(&mb, None, &[(0, n)], w)
        });
        out.inherit_consolidated(self);
        out
    }

    /// Debug-only: `dead_heap` bounds the heap's unreferenced bytes from above.
    pub(super) fn debug_verify_dead_heap(&self) {
        if cfg!(debug_assertions) {
            let measured = string_heap::measure_dead_heap(&self.as_mem_batch());
            debug_assert!(
                measured <= self.dead_heap,
                "heap holds {measured} unreferenced bytes, past its bound of {} ({} bytes, {} rows)",
                self.dead_heap,
                self.blob.len(),
                self.count,
            );
        }
    }
}

impl Clone for Batch {
    /// Clone all buffers into a new independent Batch (2 allocations).
    fn clone(&self) -> Self {
        // Rebuilt rather than copied through two pooled arenas holding zero bytes.
        if self.holds_nothing() {
            return Self::empty_with_schema(&self.schema);
        }
        // Only the used portion of data (count-based, not capacity-based).
        let mut b = Self::from_mem_batch(&self.as_mem_batch());
        b.consolidated = self.consolidated;
        b
    }
}

impl RowSource for Batch {
    #[inline(always)]
    fn get_pk_bytes(&self, row: usize) -> &[u8] {
        Batch::get_pk_bytes(self, row)
    }
    #[inline(always)]
    fn get_null_word(&self, row: usize) -> u64 {
        Batch::get_null_word(self, row)
    }
    #[inline(always)]
    fn get_col_ptr(&self, row: usize, payload_col: usize, col_size: usize) -> &[u8] {
        Batch::get_col_ptr(self, row, payload_col, col_size)
    }
    #[inline(always)]
    fn heap(&self) -> Heap<'_> {
        Heap::Plain(Batch::blob(self))
    }
    #[inline(always)]
    fn row_count(&self) -> usize {
        Batch::len(self)
    }
}

/// A borrowed view of batch rows: an arena of `cap` rows laid out under
/// `schema`, of which the first `count` are rows. A [`Batch`]'s own, or a WAL
/// block's fixed bytes, which are an arena exactly as large as its rows.
#[derive(Clone, Copy)]
pub struct MemBatch<'a> {
    pub(in crate::repr) data: &'a [u8],
    /// The layout: region `r` starts at `schema.region_start(r, cap)`.
    pub(in crate::repr) schema: &'a SchemaDescriptor,
    pub(in crate::repr) cap: usize,
    pub(in crate::repr) blob: &'a [u8],
    /// Row count of the view. Read from outside through [`MemBatch::len`].
    pub(in crate::repr) count: usize,
    /// An upper bound on the bytes of `blob` no cell references.
    pub(in crate::repr) dead_heap: usize,
}

impl<'a> MemBatch<'a> {
    /// Row count of the view.
    #[inline(always)]
    pub fn len(&self) -> usize {
        self.count
    }
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Byte width of the PK region per row.
    #[inline(always)]
    pub(crate) fn pk_stride(&self) -> usize {
        self.schema.pk_stride()
    }

    /// Where region `r` starts in the arena.
    #[inline(always)]
    pub(super) fn region_start(&self, r: usize) -> usize {
        self.schema.region_start(r, self.cap)
    }

    /// The `count` rows of region `r`.
    #[inline(always)]
    pub(super) fn region(&self, r: usize) -> &'a [u8] {
        let off = self.region_start(r);
        &self.data[off..off + self.count * self.schema.region_stride(r)]
    }

    /// PK region as a contiguous slice (`count * pk_stride` bytes).
    #[inline]
    pub fn pk(&self) -> &'a [u8] {
        self.region(REG_PK)
    }

    /// Weight region as a contiguous slice (`count * 8` bytes).
    #[inline]
    pub(crate) fn weight(&self) -> &'a [u8] {
        self.region(REG_WEIGHT)
    }

    /// Wrapping sum of the weights of rows `[start, end)`, over one
    /// bounds-checked slice of the weight region.
    #[inline]
    pub(crate) fn sum_weights(&self, start: usize, end: usize) -> i64 {
        self.weight()[start * FIXED_REGION_BYTES..end * FIXED_REGION_BYTES]
            .as_chunks::<FIXED_REGION_BYTES>()
            .0
            .iter()
            .fold(0i64, |a, w| a.wrapping_add(i64::from_le_bytes(*w)))
    }

    /// Null bitmap region as a contiguous slice (`count * 8` bytes).
    #[inline(always)]
    pub(crate) fn null_bmp(&self) -> &'a [u8] {
        self.region(REG_NULL_BMP)
    }

    /// Payload column `pi` as a contiguous slice (`count * width` bytes).
    #[inline(always)]
    pub(crate) fn col_data(&self, pi: usize) -> &'a [u8] {
        self.region(REG_PAYLOAD_START + pi)
    }

    #[inline(always)]
    pub fn get_pk_bytes(&self, row: usize) -> &'a [u8] {
        let stride = self.pk_stride();
        let off = row * stride;
        &self.data[off..off + stride]
    }
    /// The `n` bytes at `at` of row `row`'s PK.
    #[inline(always)]
    pub(crate) fn get_pk_range(&self, row: usize, at: usize, n: usize) -> &'a [u8] {
        debug_assert!(at + n <= self.pk_stride());
        let off = row * self.pk_stride() + at;
        &self.data[off..off + n]
    }
    #[inline(always)]
    pub fn get_weight(&self, row: usize) -> i64 {
        read_i64_le(self.data, self.region_start(REG_WEIGHT) + row * FIXED_REGION_BYTES)
    }
    #[inline(always)]
    pub(crate) fn get_null_word(&self, row: usize) -> u64 {
        read_u64_le(self.data, self.region_start(REG_NULL_BMP) + row * FIXED_REGION_BYTES)
    }
    #[inline(always)]
    pub(crate) fn get_col_ptr(&self, row: usize, payload_col: usize, col_size: usize) -> &'a [u8] {
        let off = self.region_start(REG_PAYLOAD_START + payload_col) + row * col_size;
        &self.data[off..off + col_size]
    }
}

/// Each method forwards via UFCS to the inherent accessor of the same name, so
/// the call binds to the concrete read rather than recursing into the trait.
/// The forwarder and the accessor it calls are both `#[inline(always)]`: at
/// `-O0`, which the E2E suite's debug build runs at, a plain `#[inline]` is
/// not inlined.
impl<'a> RowSource for MemBatch<'a> {
    #[inline(always)]
    fn get_pk_bytes(&self, row: usize) -> &[u8] {
        MemBatch::get_pk_bytes(self, row)
    }
    #[inline(always)]
    fn get_null_word(&self, row: usize) -> u64 {
        MemBatch::get_null_word(self, row)
    }
    #[inline(always)]
    fn get_col_ptr(&self, row: usize, payload_col: usize, col_size: usize) -> &[u8] {
        MemBatch::get_col_ptr(self, row, payload_col, col_size)
    }
    #[inline(always)]
    fn heap(&self) -> Heap<'_> {
        Heap::Plain(self.blob)
    }
    #[inline(always)]
    fn row_count(&self) -> usize {
        MemBatch::len(self)
    }
}

/// The region half, forwarded to the inherent accessors as the per-row half is.
impl<'a> BatchView for MemBatch<'a> {
    #[inline(always)]
    fn col_data(&self, payload_col: usize, col_size: usize) -> &[u8] {
        debug_assert_eq!(col_size, self.schema.region_stride(REG_PAYLOAD_START + payload_col));
        MemBatch::col_data(self, payload_col)
    }
    #[inline(always)]
    fn null_bmp(&self) -> &[u8] {
        MemBatch::null_bmp(self)
    }
    #[inline(always)]
    fn pk_region(&self) -> (&[u8], usize) {
        (MemBatch::pk(self), self.pk_stride())
    }
}

/// A batch of `rows` rows, every one written in place by `write_fn` through a
/// [`DirectWriter`] over its uninitialized arena.
pub(crate) fn write_to_batch(
    schema: &SchemaDescriptor,
    rows: usize,
    max_blob: usize,
    write_fn: impl FnOnce(&mut DirectWriter),
) -> Batch {
    if rows == 0 {
        return Batch::empty_with_schema(schema);
    }
    let mut b = Batch::with_capacity_blob(schema, rows, max_blob);
    b.write_rows(rows, write_fn);
    if b.blob.is_empty() {
        b.blob = PooledBuf::default();
    }
    b
}

#[cfg(test)]
#[path = "tests/batch.rs"]
mod tests;

#[cfg(test)]
#[path = "benches/batch.rs"]
mod bench;
