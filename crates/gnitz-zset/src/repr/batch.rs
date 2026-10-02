//! Owned columnar batch type for Z-set rows.
//!
//! `Batch` owns its memory (two `Vec<u8>` buffers — data + blob).
//! `MemBatch<'a>` in the merge module is the borrowed slice-view counterpart.

use std::ops::Range;

use super::batch_pool::{acquire_arena, acquire_uninit, is_tight, recycle_buf};
use super::merge::{self, ColPtr, MemBatch};
use super::run::StoredRow;
use super::string_heap::{self, copy_string_cells, relocate_german_string_vec, BlobCache};
use crate::schema::{ColumnLocator, SchemaDescriptor, SchemaFacts};
use gnitz_expr::RowSource;
use gnitz_wire::{read_i64_le, read_u64_le, TypeCode};

/// Max regions **including** the trailing blob region — the bound for the
/// WAL/wire region arrays (ptrs / sizes / offsets / positions). Owned by
/// `gnitz_wire::region` (the framer's region cap); the in-memory cap below
/// derives from it.
pub(crate) use gnitz_wire::MAX_WIRE_REGIONS;

/// Regions tracked in the `offsets`/`strides` arrays: 3 fixed (pk, weight,
/// null_bmp) + the payload columns. The blob is not one of them — it lives in
/// `self.blob` — so this is the wire cap less that slot.
pub(crate) const MAX_BATCH_REGIONS: usize = MAX_WIRE_REGIONS - 1;

// ── Region indices into `offsets` / `strides` ───────────────────────────────
//
// Three fixed regions (PK is `pk_stride` bytes/row; weight and null_bmp are
// 8 bytes/row); payload columns start at `REG_PAYLOAD_START` and continue for
// `num_payload_cols()` slots. Use these constants instead of bare numeric
// literals. Owned by `gnitz_wire::region` (the client, the wire codec, and the
// engine all encode the same convention), same as `MAX_WIRE_REGIONS`.
pub(in crate::repr) use gnitz_wire::{REG_NULL_BMP, REG_PAYLOAD_START, REG_PK, REG_WEIGHT};
/// Stride (in bytes) of the weight and null_bmp fixed regions.
const FIXED_REGION_STRIDE: u8 = 8;
pub(in crate::repr) const FIXED_REGION_BYTES: usize = FIXED_REGION_STRIDE as usize;

/// Total rows in a `[start, end)` row-range list — the shape every range-driven
/// path (`append_ranges`, `Batch::from_ranges`, `MapPlan::append_map_ranges`)
/// sizes its destination by.
#[inline]
pub(crate) fn range_rows(ranges: &[(usize, usize)]) -> usize {
    ranges.iter().map(|&(s, e)| e - s).sum()
}

/// The maximal `[start, end)` runs of `0..n` whose rows `keep` admits, ascending.
pub(crate) fn runs_where(n: usize, keep: impl Fn(usize) -> bool) -> Vec<(usize, usize)> {
    let kept: Vec<u64> = (0..n)
        .step_by(64)
        .map(|base| (base..n.min(base + 64)).fold(0, |bits, row| bits | u64::from(keep(row)) << (row - base)))
        .collect();
    let mut runs = Vec::new();
    gnitz_expr::scan_filter_bits(&kept, n, &mut runs);
    runs
}

/// Write each region's byte offset into `offsets`, returning the total arena
/// size. Entries past `num_regions` are untouched; every caller passes an
/// all-zero array. An out-parameter rather than a return: the array is
/// sized for `MAX_BATCH_REGIONS` and this does not inline.
pub(in crate::repr) fn compute_offsets_into(
    strides: &[u8; MAX_BATCH_REGIONS],
    num_regions: usize,
    capacity: usize,
    offsets: &mut [usize; MAX_BATCH_REGIONS],
) -> usize {
    let mut off = 0usize;
    for i in 0..num_regions {
        off = off.next_multiple_of(8);
        offsets[i] = off;
        off += capacity * strides[i] as usize;
    }
    off
}

/// Build a strides array from a SchemaDescriptor.
pub(in crate::repr) fn strides_from_schema(schema: &SchemaDescriptor) -> [u8; MAX_BATCH_REGIONS] {
    let mut strides = [0u8; MAX_BATCH_REGIONS];
    // Lossless: `SchemaDescriptor::new` asserts `pk_stride <= MAX_PK_BYTES`.
    strides[REG_PK] = schema.pk_stride() as u8;
    strides[REG_WEIGHT] = FIXED_REGION_STRIDE;
    strides[REG_NULL_BMP] = FIXED_REGION_STRIDE;
    for (pi, col) in schema.payload_columns() {
        strides[REG_PAYLOAD_START + pi] = col.size();
    }
    strides
}

/// Cells [`Batch::carry_heap`] charges dead between two waste checks.
const DEAD_CHECK_CELLS: usize = 1024;

/// One row's bytes across the fixed regions of `strides`.
pub(super) fn row_width(strides: &[u8]) -> usize {
    strides.iter().map(|&s| s as usize).sum()
}

/// Copy `count` rows of each region from `src` (regions at `src_offsets`) into
/// `dst` (regions at `dst_offsets`), one bulk copy per region.
fn copy_regions(
    src: &[u8],
    src_offsets: &[usize],
    dst: &mut [u8],
    dst_offsets: &[usize],
    strides: &[u8],
    count: usize,
) {
    for ((&s, &d), &stride) in src_offsets.iter().zip(dst_offsets).zip(strides) {
        let len = count * stride as usize;
        if len == 0 {
            continue;
        }
        dst[d..d + len].copy_from_slice(&src[s..s + len]);
    }
}

/// A batch's end at one moment: its row count and blob length.
#[derive(Clone, Copy)]
pub(crate) struct RowMark {
    count: usize,
    blob_len: usize,
}

/// Owned columnar batch.  All fixed-stride column data lives in a single
/// contiguous `data` buffer.  Blob data is separate (variable-length).
///
/// Layout of `data`: `[pk | weight | null_bmp | col_0 | ... | col_{N-1}]`
/// Each region has `capacity * stride` bytes allocated; `count * stride` bytes
/// contain data.  The PK region uses pk_stride bytes/row; 8 for U64, 16 for
/// U128, wider for compound PKs.
///
/// **2 heap allocations**: `data` and `blob`.
///
/// A row is appended through `begin_row`/`commit_row`, one of
/// the `push_*_row` shorthands, or [`super::batch_builder::BatchBuilder`]; the
/// region writers and `count` are internal.
pub struct Batch {
    data: Vec<u8>,
    pub(in crate::repr) blob: Vec<u8>,
    /// An upper bound on the bytes of `blob` no string cell references.
    pub(in crate::repr) dead_heap: usize,
    // `usize`, not `u32`: a single large batch's cumulative region offset can
    // exceed 4 GB (see `compute_offsets_into`). In-memory only — never serialized.
    offsets: [usize; MAX_BATCH_REGIONS],
    strides: [u8; MAX_BATCH_REGIONS],
    capacity: usize,
    /// Live row count; [`Batch::len`] outside the crate. Not `pub`: a counted
    /// row whose regions were not all written reads uninitialised arena bytes.
    pub(crate) count: usize,
    /// The rows are strictly (PK, payload)-increasing and ghost-free. Cleared by
    /// every mutable region borrow and every append; raised by
    /// [`Self::certify_consolidated`], by copying a source's claim, and by
    /// [`Self::append_above`], which checks the order it extends.
    consolidated: bool,
    /// The schema this batch's rows were laid out under, and the one every
    /// operation on this batch alone runs under. Moved only through
    /// [`Self::set_schema`].
    schema: SchemaDescriptor,
}

impl Batch {
    // ── Constructors ────────────────────────────────────────────────────

    /// The one zero-allocation empty constructor: shape (strides / region
    /// count / schema) supplied by the caller, everything else empty.
    fn empty_from(strides: [u8; MAX_BATCH_REGIONS], schema: &SchemaDescriptor) -> Self {
        Batch {
            data: Vec::new(),
            blob: Vec::new(),
            dead_heap: 0,
            offsets: [0usize; MAX_BATCH_REGIONS],
            strides,
            capacity: 0,
            count: 0,
            consolidated: false,
            schema: *schema,
        }
    }

    /// Zero-allocation empty batch with strides pre-filled from `schema`.
    pub fn empty_with_schema(schema: &SchemaDescriptor) -> Self {
        Self::empty_from(strides_from_schema(schema), schema)
    }

    /// Zero-allocation empty batch with this batch's exact shape (strides,
    /// region count, schema). The empty return / swap-placeholder constructor.
    fn empty_like(&self) -> Self {
        Self::empty_from(self.strides, &self.schema)
    }

    /// Move this batch out, leaving an `empty_like` placeholder behind — the
    /// VM register-swap idiom, with the slot's shape kept truthful.
    pub fn take(&mut self) -> Self {
        let empty = self.empty_like();
        std::mem::replace(self, empty)
    }

    /// An empty batch with room for `rows` rows. The arena is uninitialized
    /// (poisoned in debug builds): every counted row must have every region written.
    pub fn with_capacity(schema: &SchemaDescriptor, rows: usize) -> Self {
        let strides = strides_from_schema(schema);
        let mut b = Self::empty_from(strides, schema);
        b.capacity = rows;
        let total_size = compute_offsets_into(&strides, b.arena_regions(), rows, &mut b.offsets);
        // SAFETY: a `Batch` reads no row at or past `count`, which is 0.
        b.data = unsafe { acquire_uninit(total_size) };
        b.debug_poison_rows(0..rows);
        b
    }

    /// [`Self::with_capacity`] with the blob heap pre-sized. For the callers that
    /// know the byte count up front; `with_capacity` leaves it empty because most
    /// writers do not.
    pub(crate) fn with_capacity_blob(schema: &SchemaDescriptor, rows: usize, blob_bytes: usize) -> Self {
        let mut b = Self::with_capacity(schema, rows);
        b.reserve_blob(blob_bytes);
        b
    }

    /// Room for `bytes` more blob heap bytes, taken from the arena pool `Drop`
    /// recycles into — a bare [`Vec::reserve`] would take them from the global
    /// allocator. A heap already holding bytes has to grow in place.
    pub(crate) fn reserve_blob(&mut self, bytes: usize) {
        match self.blob.capacity() {
            0 => self.blob = acquire_arena(bytes),
            _ => self.blob.reserve(bytes),
        }
    }

    /// An owned, tightly packed copy of `mb`'s `count` rows and its whole heap,
    /// laid out under `schema`: unconsolidated, `mb`'s dead-byte bound.
    pub(in crate::repr) fn from_mem_batch(mb: &MemBatch, schema: &SchemaDescriptor) -> Self {
        let strides = strides_from_schema(schema);
        let mut b = Self::empty_from(strides, schema);
        let nr = b.arena_regions();
        let size = compute_offsets_into(&strides, nr, mb.count, &mut b.offsets);
        // SAFETY: `data` holds exactly `count` rows, all written by `copy_regions`.
        b.data = unsafe { acquire_uninit(size) };
        copy_regions(
            mb.data,
            &mb.offsets[..nr],
            &mut b.data,
            &b.offsets[..nr],
            &strides[..nr],
            mb.count,
        );
        b.blob = acquire_arena(mb.blob.len());
        b.blob.extend_from_slice(mb.blob);
        b.dead_heap = mb.dead_heap;
        b.capacity = mb.count;
        b.count = mb.count;
        b
    }

    // ── Schema installation ─────────────────────────────────────────────

    /// The schema this batch's rows were laid out under.
    #[inline]
    pub fn schema(&self) -> &SchemaDescriptor {
        &self.schema
    }

    /// Install a schema on this batch after verifying it implies the batch's
    /// physical region strides. Every code path that wants to move a batch's
    /// schema after construction MUST go through this helper: it turns a latent
    /// "batch shape != declared shape" bug into a localized panic at the first
    /// assignment, instead of a cryptic OOB slice panic several call-frames
    /// later — or a shard whose regions its reader mis-sizes.
    #[inline]
    pub fn set_schema(&mut self, s: &SchemaDescriptor) {
        debug_assert_eq!(
            strides_from_schema(s),
            self.strides,
            "Batch::set_schema: strides disagree with the schema",
        );
        self.schema = *s;
    }

    // ── Read accessors ──────────────────────────────────────────────────

    /// Live row count.
    #[inline]
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

    /// The live `count * stride` bytes of region `r` — the one range computation
    /// every fixed-region accessor below shares, and the slice the wire and shard
    /// framers build their region lists from.
    #[inline(always)]
    pub(super) fn region_at(&self, r: usize) -> &[u8] {
        let off = self.offsets[r];
        &self.data[off..off + self.count * self.strides[r] as usize]
    }

    /// [`Self::region_at`] as a mutable borrow.
    #[inline(always)]
    fn region_at_mut(&mut self, r: usize) -> &mut [u8] {
        self.consolidated = false;
        let off = self.offsets[r];
        let end = off + self.count * self.strides[r] as usize;
        &mut self.data[off..end]
    }

    #[inline]
    pub fn pk_data(&self) -> &[u8] {
        self.region_at(REG_PK)
    }
    #[inline]
    pub(crate) fn weight_data(&self) -> &[u8] {
        self.region_at(REG_WEIGHT)
    }
    #[inline]
    pub(crate) fn null_bmp_data(&self) -> &[u8] {
        self.region_at(REG_NULL_BMP)
    }

    /// The `[start, end)` runs of rows with no null bit of `mask` set.
    pub(crate) fn runs_without_nulls(&self, mask: u64) -> Vec<(usize, usize)> {
        let words = self.null_bmp_data().as_chunks::<8>().0;
        // A batch with none of them set takes only this branch-free pass.
        if words.iter().fold(0u64, |a, w| a | u64::from_le_bytes(*w)) & mask == 0 {
            return vec![(0, self.count)];
        }
        runs_where(self.count, |row| u64::from_le_bytes(words[row]) & mask == 0)
    }
    #[inline]
    pub fn col_data(&self, pi: usize) -> &[u8] {
        self.region_at(REG_PAYLOAD_START + pi)
    }
    /// `#[inline(always)]`: two byte loads off the embedded descriptor, on the
    /// per-row appenders' loop bound.
    #[inline(always)]
    pub fn num_payload_cols(&self) -> usize {
        self.schema.num_payload_cols()
    }

    /// Regions in the `offsets`/`strides` arrays — the three fixed ones plus one
    /// per payload column. Also the blob region's index in the wire/shard
    /// layout, which has no slot in either array.
    #[inline(always)]
    pub(super) fn arena_regions(&self) -> usize {
        REG_PAYLOAD_START + self.num_payload_cols()
    }

    // ── Mutable slice accessors ─────────────────────────────────────────

    #[inline]
    pub(crate) fn pk_data_mut(&mut self) -> &mut [u8] {
        self.region_at_mut(REG_PK)
    }
    #[inline]
    pub(crate) fn weight_data_mut(&mut self) -> &mut [u8] {
        self.region_at_mut(REG_WEIGHT)
    }
    #[inline]
    pub(crate) fn null_bmp_data_mut(&mut self) -> &mut [u8] {
        self.region_at_mut(REG_NULL_BMP)
    }
    #[inline]
    pub(crate) fn col_data_mut(&mut self, pi: usize) -> &mut [u8] {
        self.region_at_mut(REG_PAYLOAD_START + pi)
    }

    /// Split borrow of payload column `pi`'s region, the NULL bitmap, and the
    /// blob heap. One call resolves all three, hoisting the region arithmetic out
    /// of the row loops that would otherwise reach for them one at a time — the
    /// per-cell string relocators (column + blob), and the map's emit, which writes a
    /// computed column's slots and sets that column's bit for the same rows
    /// (column + bitmap, plus the heap for a string register).
    ///
    /// Regions are laid out in region-index order and `REG_NULL_BMP` always
    /// precedes any payload region, so the split is a plain `split_at_mut` at the
    /// column's offset; `blob` is a field beside `data` and splits off with it.
    #[inline]
    pub(crate) fn col_null_and_blob_mut(&mut self, pi: usize) -> (&mut [u8], &mut [u8], &mut Vec<u8>) {
        self.consolidated = false;
        let n_off = self.offsets[REG_NULL_BMP];
        let n_end = n_off + self.count * FIXED_REGION_BYTES;
        let r = REG_PAYLOAD_START + pi;
        let c_off = self.offsets[r];
        let c_end = c_off + self.count * self.strides[r] as usize;
        debug_assert!(n_end <= c_off, "null bitmap region must precede payload region {pi}");
        let (lo, hi) = self.data.split_at_mut(c_off);
        (&mut hi[..c_end - c_off], &mut lo[n_off..n_end], &mut self.blob)
    }

    // ── Row accessors ───────────────────────────────────────────────────

    #[inline(always)]
    pub fn get_pk(&self, row: usize) -> u128 {
        let stride = self.strides[REG_PK] as usize;
        let off = self.offsets[REG_PK] + row * stride;
        gnitz_wire::widen_pk_be(&self.data[off..off + stride])
    }

    /// Owned-`Batch` sibling of `MemBatch::get_pk_bytes`. Returns exactly
    /// `pk_stride` bytes, so a wide PK (`pk_stride > 16`) survives where a
    /// `u128` would truncate it.
    #[inline(always)]
    pub fn get_pk_bytes(&self, row: usize) -> &[u8] {
        let stride = self.strides[REG_PK] as usize;
        let off = self.offsets[REG_PK] + row * stride;
        &self.data[off..off + stride]
    }
    #[inline(always)]
    pub fn get_weight(&self, row: usize) -> i64 {
        read_i64_le(&self.data, self.offsets[REG_WEIGHT] + row * FIXED_REGION_BYTES)
    }
    /// Indices of the live rows — those at positive weight, the elements the
    /// batch asserts. Twin of `ZSetBatch::live_rows` in `gnitz-core`.
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
    /// callers only need a map that sends no non-zero weight to zero.
    #[inline]
    pub fn map_weights(&mut self, f: impl Fn(i64) -> i64) {
        let off = self.offsets[REG_WEIGHT];
        for chunk in self.data[off..off + self.count * FIXED_REGION_BYTES]
            .as_chunks_mut::<FIXED_REGION_BYTES>()
            .0
        {
            *chunk = f(i64::from_le_bytes(*chunk)).to_le_bytes();
        }
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
            .weight_data()
            .as_chunks::<FIXED_REGION_BYTES>()
            .0
            .iter()
            .fold(false, |bad, w| bad | (i64::from_le_bytes(*w) <= 0))
    }
    #[inline(always)]
    pub fn get_null_word(&self, row: usize) -> u64 {
        read_u64_le(&self.data, self.offsets[REG_NULL_BMP] + row * FIXED_REGION_BYTES)
    }
    #[inline(always)]
    pub fn get_col_ptr(&self, row: usize, payload_col: usize, col_size: usize) -> &[u8] {
        let off = self.offsets[REG_PAYLOAD_START + payload_col] + row * col_size;
        &self.data[off..off + col_size]
    }

    // ── Extend methods (building batches row-by-row) ────────────────────

    /// Ensure the data buffer has room for at least `n` more rows beyond `count`.
    /// `#[inline]` for the already-has-room test, which is the whole call on
    /// every append but the growing one.
    #[inline]
    pub(crate) fn reserve_rows(&mut self, n: usize) {
        if self.count + n <= self.capacity {
            return;
        }
        let nr = self.arena_regions();
        let new_cap = (self.capacity * 2).max(8).max(self.count + n);
        let mut new_offsets = [0usize; MAX_BATCH_REGIONS];
        let new_total = compute_offsets_into(&self.strides, nr, new_cap, &mut new_offsets);

        if new_total > self.data.capacity() {
            // Each region lands at its new offset in one copy.
            // SAFETY: `copy_regions` writes the `count` rows a `Batch` reads.
            let mut new_data = unsafe { acquire_uninit(new_total) };
            copy_regions(
                &self.data,
                &self.offsets[..nr],
                &mut new_data,
                &new_offsets[..nr],
                &self.strides[..nr],
                self.count,
            );

            let old_data = std::mem::replace(&mut self.data, new_data);
            recycle_buf(old_data);
        } else {
            // The arena has room already: shift each region to its new offset.
            unsafe {
                self.data.set_len(new_total);
                for i in (0..nr).rev() {
                    let old_start = self.offsets[i];
                    let new_start = new_offsets[i];
                    let data_len = self.count * self.strides[i] as usize;
                    if old_start != new_start && data_len > 0 {
                        std::ptr::copy(
                            self.data.as_ptr().add(old_start),
                            self.data.as_mut_ptr().add(new_start),
                            data_len,
                        );
                    }
                }
            }
        }
        self.offsets = new_offsets;
        self.capacity = new_cap;
        self.debug_poison_rows(self.count..new_cap);
    }

    /// Count `n` more rows and return the first one's index. Their regions are
    /// uninitialized (see [`Self::with_capacity`]): the caller writes every one
    /// through the region accessors, which reach a row only once it is counted.
    pub(crate) fn grow_rows(&mut self, n: usize) -> usize {
        self.reserve_rows(n);
        let at = self.count;
        self.count += n;
        self.consolidated = false;
        at
    }

    /// In debug builds, fill `rows` of every region with `0xA5`, so reading a row
    /// nothing wrote sees an implausible value rather than a plausible leftover.
    fn debug_poison_rows(&mut self, rows: Range<usize>) {
        if cfg!(debug_assertions) {
            let nr = self.arena_regions();
            for (&start, &s) in self.offsets[..nr].iter().zip(&self.strides[..nr]) {
                let s = s as usize;
                self.data[start + rows.start * s..start + rows.end * s].fill(0xA5);
            }
        }
    }

    /// The current row's `len` bytes of region `r`, growing first if the row is
    /// past capacity.
    #[inline(always)]
    fn row_cell_mut(&mut self, r: usize, len: usize) -> &mut [u8] {
        if self.count >= self.capacity {
            self.reserve_rows(1);
        }
        let off = self.offsets[r] + self.count * self.strides[r] as usize;
        &mut self.data[off..off + len]
    }

    /// Write `src` into region `r` at the current row position.
    #[inline(always)]
    fn extend_region(&mut self, r: usize, src: &[u8]) {
        debug_assert_eq!(
            src.len(),
            self.strides[r] as usize,
            "extend_region: src len {} != stride {} for region {}",
            src.len(),
            self.strides[r],
            r
        );
        self.row_cell_mut(r, src.len()).copy_from_slice(src);
    }

    #[inline]
    pub(crate) fn extend_col(&mut self, pi: usize, d: &[u8]) {
        self.extend_region(REG_PAYLOAD_START + pi, d);
    }

    /// [`Self::extend_col`] for a STRING/BLOB column: `content` is encoded into
    /// this batch's own blob heap, whose offsets no other batch's cell can name.
    #[inline]
    pub(crate) fn extend_col_blob(&mut self, pi: usize, content: &[u8]) {
        let cell = gnitz_wire::encode_german_string(content, &mut self.blob);
        self.extend_col(pi, &cell);
    }

    /// Open a row: PK region (exactly `pk_stride` OPK bytes) and weight. The
    /// caller then writes every payload column, in any order, and closes with
    /// [`Self::commit_row`].
    #[inline(always)]
    pub(crate) fn begin_row(&mut self, pk_bytes: &[u8], weight: i64) {
        assert_eq!(
            pk_bytes.len(),
            self.strides[REG_PK] as usize,
            "begin_row: the key must be exactly pk_stride bytes",
        );
        self.extend_region(REG_PK, pk_bytes);
        self.extend_region(REG_WEIGHT, &weight.to_le_bytes());
    }

    /// Close the row [`Self::begin_row`] opened: the NULL word, then the count,
    /// then the dropped consolidated claim. The count moves last, so no reader
    /// sees a row before every region carries it.
    #[inline(always)]
    pub(crate) fn commit_row(&mut self, null_word: u64) {
        self.extend_region(REG_NULL_BMP, &null_word.to_le_bytes());
        self.count += 1;
        self.consolidated = false;
    }

    /// Append one whole row of a **payload-free** schema — an index entry, whose
    /// columns are all PK, so its key is the entire row.
    #[inline]
    pub fn push_key_row(&mut self, pk: &[u8], weight: i64) {
        debug_assert_eq!(
            self.num_payload_cols(),
            0,
            "push_key_row is the payload-free schema; use push_zero_filled_row or BatchBuilder",
        );
        self.push_zero_filled_row(pk, weight);
    }

    /// Append one row whose payload carries no value: the PK region from `pk`
    /// (exactly `pk_stride` OPK bytes), `weight`, then every payload column
    /// zero-filled. The payload-free case is [`Self::push_key_row`].
    #[inline(always)]
    pub(crate) fn push_zero_filled_row(&mut self, pk: &[u8], weight: i64) {
        self.begin_row(pk, weight);
        for pi in 0..self.num_payload_cols() {
            self.fill_col_zero(pi);
        }
        self.commit_row(0);
    }

    /// Zero-fill payload column `pi` at the current row position.
    #[inline]
    pub(crate) fn fill_col_zero(&mut self, pi: usize) {
        let r = REG_PAYLOAD_START + pi;
        let width = self.strides[r] as usize;
        self.row_cell_mut(r, width).fill(0);
    }

    /// Bulk-copy a range of rows from `src_region_data` into region `r`.
    fn bulk_copy_region(&mut self, r: usize, src_region_data: &[u8], start: usize, end: usize) {
        let stride = self.strides[r] as usize;
        let n = end - start;
        let dst_off = self.offsets[r] + self.count * stride;
        let src_off = start * stride;
        self.data[dst_off..dst_off + n * stride].copy_from_slice(&src_region_data[src_off..src_off + n * stride]);
    }

    /// Open an [`AppendSession`] over this batch; `hint_rows` sizes its blob
    /// dedup cache.
    pub(crate) fn append_session(&mut self, hint_rows: usize) -> AppendSession<'_> {
        AppendSession {
            mask: self.schema.string_payload_slots(),
            dst: self,
            cache: BlobCache::new(hint_rows),
        }
    }
}

/// An open append into one destination batch: the string-slot mask and blob
/// dedup cache every push reuses, so appending runs of a row or two stays cheap.
pub(crate) struct AppendSession<'d> {
    dst: &'d mut Batch,
    /// Bit `pi` set = payload slot `pi` is a German string.
    mask: u64,
    cache: BlobCache,
}

impl AppendSession<'_> {
    /// [`Batch::carry_heap`] into the session's destination: the `heap_at` to
    /// push `src`'s rows at.
    pub(crate) fn carry(&mut self, src: &MemBatch<'_>, kept: &[(usize, usize)]) -> Option<usize> {
        self.dst.carry_heap(src, self.mask, self.mask, kept)
    }

    /// Append every listed range of `src`, a batch of the destination's layout,
    /// in list order: fixed-width regions in bulk, its string cells shifted onto
    /// its heap carried at `heap_at`, or relocated under the session's cache.
    pub(crate) fn push_ranges(&mut self, src: &MemBatch<'_>, heap_at: Option<usize>, ranges: &[(usize, usize)]) {
        let dst = &mut *self.dst;
        debug_assert_eq!(
            src.pk_stride, dst.strides[REG_PK],
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
        dst.reserve_rows(total);
        let npc = dst.num_payload_cols();
        if heap_at.is_none() && !src.blob.is_empty() {
            // The rows this call copies, not the whole source heap: a many-run merge
            // appends into one output, and the whole heap per run ratchets capacity.
            dst.reserve_blob(string_heap::prorated_blob_cap(src.blob.len(), src.count, total));
        }
        for &(start, end) in ranges {
            let n = end - start;
            if n == 0 {
                continue;
            }
            dst.bulk_copy_region(REG_PK, src.pk(), start, end);
            dst.bulk_copy_region(REG_WEIGHT, src.weight(), start, end);
            dst.bulk_copy_region(REG_NULL_BMP, src.null_bmp(), start, end);
            for pi in 0..npc {
                let r = REG_PAYLOAD_START + pi;
                let cs = dst.strides[r] as usize;
                if (self.mask >> pi) & 1 != 0 {
                    let at = dst.offsets[r] + dst.count * 16;
                    let cells = &src.col_data(pi, 16)[start * 16..end * 16];
                    copy_string_cells(
                        &mut dst.data[at..at + n * 16],
                        cells,
                        src.blob,
                        &mut dst.blob,
                        heap_at,
                        &mut self.cache,
                    );
                } else if cs > 0 {
                    dst.bulk_copy_region(r, src.col_data(pi, cs), start, end);
                }
            }
            dst.count += n;
        }
        dst.consolidated = false;
    }

    /// Append one row of `src` at an explicit weight, under the session's own
    /// blob dedup cache. A zero weight appends nothing.
    pub(crate) fn push_row<S: RowSource>(&mut self, src: &S, row: usize, weight: i64) {
        self.dst.append_row_from_source(weight, src, row, Some(&mut self.cache));
    }

    /// [`Self::push_row`] for a `src` whose heap may be carried at `heap_at`. A
    /// zero weight appends nothing and [leaves the row out](Self::leave_out).
    pub(crate) fn push_row_at(&mut self, src: &MemBatch<'_>, heap_at: Option<usize>, row: usize, weight: i64) {
        match heap_at {
            _ if weight == 0 => self.leave_out(src, heap_at, row),
            None => self.push_row(src, row, weight),
            Some(_) => {
                self.push_ranges(src, heap_at, &[(row, row + 1)]);
                let last = self.dst.count - 1;
                self.dst.weight_data_mut()[last * 8..].copy_from_slice(&weight.to_le_bytes());
            }
        }
    }

    /// Charge `src[row]`'s long bytes dead when its heap is carried at
    /// `heap_at` but the row is not copied.
    pub(crate) fn leave_out(&mut self, src: &MemBatch<'_>, heap_at: Option<usize>, row: usize) {
        if heap_at.is_some() {
            self.dst.charge_dead(string_heap::row_long_bytes(src, self.mask, row));
        }
    }
}

impl Batch {
    // ── Lifecycle ───────────────────────────────────────────────────────

    /// Create a borrowed `MemBatch` view over this batch's data.
    ///
    /// Zero-allocation and near-free: every field is a pointer or a scalar, the
    /// region offsets included (lent from this batch's own array).
    ///
    /// `#[inline]`: derived per range and per chunk on paths whose whole body is
    /// a few reads through it, and the debug profile the E2E suite runs is
    /// `opt-level = 0`, where the hint is what keeps this off the call path.
    #[inline]
    pub fn as_mem_batch(&self) -> MemBatch<'_> {
        MemBatch {
            data: &self.data,
            offsets: &self.offsets,
            pk_stride: self.strides[REG_PK],
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

    /// `is_consolidated()`, additionally asserting in debug builds that the data
    /// really is consolidated whenever the cached claim says so. Prefer this over
    /// `is_consolidated()` at any skip-point that trusts the claim to avoid a
    /// re-fold.
    ///
    /// The one exception is a run its store already verified on the way in.
    #[inline]
    pub fn consolidated_verified(&self) -> bool {
        if self.consolidated {
            self.debug_verify_consolidated();
        }
        self.is_consolidated()
    }

    /// Raise the claim, the only way it goes up. A debug build verifies it; a
    /// release build takes it on trust.
    #[inline]
    pub fn certify_consolidated(&mut self) {
        self.debug_verify_null_bits();
        self.debug_verify_consolidated();
        self.consolidated = true;
    }

    /// The batch's current end, to [`Self::truncate_to`] later.
    #[inline]
    pub(crate) fn mark(&self) -> RowMark {
        RowMark {
            count: self.count,
            blob_len: self.blob.len(),
        }
    }

    /// The rows appended since `mark`.
    #[inline]
    pub(crate) fn rows_since(&self, mark: RowMark) -> usize {
        self.count - mark.count
    }

    /// Drop every row, and every blob byte, appended since `mark`. The dropped
    /// rows must reference only heap bytes appended since it, as relocated or
    /// carried cells do, so the heap left behind holds no new dead bytes.
    #[inline]
    pub(crate) fn truncate_to(&mut self, mark: RowMark) {
        debug_assert!(mark.count <= self.count && mark.blob_len <= self.blob.len());
        self.debug_poison_rows(mark.count..self.count);
        self.count = mark.count;
        self.blob.truncate(mark.blob_len);
    }

    /// Exchange rows `a` and `b`. Their strings index this batch's own blob, so
    /// they move with the rows.
    pub(crate) fn swap_rows(&mut self, a: usize, b: usize) {
        debug_assert!(a < self.count && b < self.count);
        if a == b {
            return;
        }
        let (lo, hi) = (a.min(b), a.max(b));
        for r in 0..self.arena_regions() {
            let stride = self.strides[r] as usize;
            let (head, tail) = self.region_at_mut(r).split_at_mut(hi * stride);
            head[lo * stride..(lo + 1) * stride].swap_with_slice(&mut tail[..stride]);
        }
    }

    /// Copy `src`'s claim without re-verifying: sound for a copy of `src` that
    /// keeps its (PK, payload) order, distinctness and weights.
    #[inline]
    pub(crate) fn inherit_consolidated(&mut self, src: &Batch) {
        self.consolidated = src.consolidated;
    }

    /// Test-only: raise the claim without verifying the data — for tests that
    /// deliberately construct an inconsistent (spoofed) batch to exercise a
    /// consumer's debug verifier, which only a debug build has. Production has no
    /// such path: `certify_consolidated` always verifies.
    #[cfg(all(test, debug_assertions))]
    pub(crate) fn set_consolidated_unchecked(&mut self) {
        self.consolidated = true;
    }

    /// Debug-only: every null bit sits under a nullable payload column, over a
    /// zeroed cell.
    pub fn debug_verify_null_bits(&self) {
        if !cfg!(debug_assertions) {
            return;
        }
        let schema = &self.schema;
        let violation = gnitz_wire::first_not_null_violation(schema.not_null_payload_slots(), self.null_bmp_data());
        debug_assert!(
            violation.is_none(),
            "batch sets a null bit under a payload column the operating schema declares \
             NOT NULL: (row, payload slot) = {violation:?} of {} rows",
            self.count,
        );
        let valued = super::batch_wire::first_valued_null_cell(&self.as_mem_batch(), schema);
        debug_assert!(
            valued.is_none(),
            "batch holds a non-zero cell under a null bit: (row, payload slot) = {valued:?} of {} rows",
            self.count,
        );
    }

    /// Debug-only: assert the data is fully consolidated — strictly increasing by
    /// (PK, payload) (no unfolded duplicate) AND no zero-weight row (ghost
    /// eliminated, §2).
    pub(crate) fn debug_verify_consolidated(&self) {
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
                let ord = crate::schema::payload_order::compare_full_rows(&self.schema, self, i, self, i + 1);
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

    /// Live row bytes: each region bounded to `count`, plus the blob. Excludes
    /// unused capacity and inter-region padding, so a caller sizing a RAM budget
    /// against it is measuring rows held, not bytes allocated.
    pub fn total_bytes(&self) -> usize {
        self.count * row_width(self.strides()) + self.blob.len()
    }

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

    /// A fresh `out_schema` batch of this batch's rows carrying everything but
    /// the PK and NULL regions: blob heap, weights, and every payload column,
    /// landed at output slot `first_slot + pi`.
    ///
    /// **`count` is published while the PK and NULL regions are unwritten** — the
    /// caller must write both, or a release build reads uninitialized arena bytes.
    fn shell_for(&self, out_schema: &SchemaDescriptor, first_slot: usize) -> Batch {
        let in_schema = &self.schema;
        debug_assert!(
            out_schema.num_payload_cols() >= first_slot + in_schema.num_payload_cols(),
            "shell_for copies every payload column at first_slot + its own index",
        );
        let n = self.count;
        let mut out = Self::with_capacity(out_schema, n);
        out.grow_rows(n);
        let mask = in_schema.string_payload_slots();
        let heap_at = out.carry_heap(&self.as_mem_batch(), mask, mask, &[(0, n)]);
        let mut cache = BlobCache::new(n);
        out.weight_data_mut().copy_from_slice(self.weight_data());
        for (pi, col) in in_schema.payload_columns() {
            let src = &self.col_data(pi)[..n * col.size() as usize];
            let (dst, _, dst_blob) = out.col_null_and_blob_mut(first_slot + pi);
            match col.type_code.is_german_string() {
                true => copy_string_cells(dst, src, &self.blob, dst_blob, heap_at, &mut cache),
                false => dst.copy_from_slice(src),
            }
        }
        out
    }

    /// Every row copied into `out_schema`, whose payload space is this batch's
    /// and whose key is `prefix` followed by this batch's. The consolidated claim
    /// carries over: one prefix on every key keeps (PK, payload) order and
    /// distinctness.
    pub fn with_key_prefix(&self, out_schema: &SchemaDescriptor, prefix: &[u8]) -> Batch {
        let mut out = self.rekeyed(out_schema, |src, dst| {
            let (head, key) = dst.split_at_mut(prefix.len());
            head.copy_from_slice(prefix);
            key.copy_from_slice(src);
        });
        out.inherit_consolidated(self);
        out
    }

    /// Every row copied into `out_schema`, whose payload space is this batch's
    /// and whose key is this batch's less its leading bytes — as many as the two
    /// strides differ by. Left unconsolidated: rows that differed only in the prefix now
    /// share a key.
    pub fn without_key_prefix(&self, out_schema: &SchemaDescriptor) -> Batch {
        let cut = self.schema.pk_stride() - out_schema.pk_stride();
        self.rekeyed(out_schema, |src, dst| dst.copy_from_slice(&src[cut..]))
    }

    /// Every row copied into `out_schema`, whose payload space is this batch's;
    /// each output key written by `rekey(src_key, dst_key)`. Left unconsolidated.
    fn rekeyed(&self, out_schema: &SchemaDescriptor, rekey: impl Fn(&[u8], &mut [u8])) -> Batch {
        debug_assert_eq!(out_schema.num_payload_cols(), self.schema.num_payload_cols());
        let mut output = self.shell_for(out_schema, 0);
        output.null_bmp_data_mut().copy_from_slice(self.null_bmp_data());
        let out_stride = out_schema.pk_stride();
        let in_stride = self.schema.pk_stride();
        for (dst, src) in output
            .pk_data_mut()
            .chunks_exact_mut(out_stride)
            .zip(self.pk_data().chunks_exact(in_stride))
        {
            rekey(src, dst);
        }
        output
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

        let first_slot = if nulls_first { n_new } else { 0 };
        let mut output = self.shell_for(out_schema, first_slot);
        output.pk_data_mut().copy_from_slice(self.pk_data());

        // The appended columns are NULL, and a NULL cell is zeroed.
        let new_slots = match nulls_first {
            true => 0..n_new,
            false => in_npc..out_npc,
        };
        for pi in new_slots {
            output.col_data_mut(pi).fill(0);
        }
        let new_null_bits = gnitz_wire::low_bits_mask(n_new);
        let out_words = output.null_bmp_data_mut().as_chunks_mut::<8>().0;
        for (out, word) in out_words.iter_mut().zip(self.null_bmp_data().as_chunks::<8>().0) {
            let in_null = u64::from_le_bytes(*word);
            let out_null = match nulls_first {
                true => new_null_bits | gnitz_wire::null_word_at(in_null, n_new),
                false => in_null | gnitz_wire::null_word_at(new_null_bits, in_npc),
            };
            *out = out_null.to_le_bytes();
        }

        output.inherit_consolidated(self);
        output
    }

    /// The PK region as a uniform [`ColPtr`] view (always Raw for an owned
    /// `Batch`): the single addressing source for the OPK seeks via
    /// [`ColPtr::row`], hoisting the `data`/offset/stride reload out of the
    /// per-probe closure. The base aliases `self.data`; keep `self` alive while
    /// the view is read (the seek closures run synchronously within the call).
    #[inline]
    fn pk_col_ptr(&self) -> ColPtr {
        ColPtr {
            base: unsafe { self.data.as_ptr().add(self.offsets[REG_PK]) },
            stride: self.strides[REG_PK] as usize,
        }
    }

    /// First row whose OPK bytes are `>= key`; `key` is exactly `pk_stride`
    /// bytes. Correct at every PK width with no schema dependency.
    pub fn find_lower_bound_bytes(&self, key: &[u8]) -> usize {
        let cp = self.pk_col_ptr();
        unsafe { super::seek::seek_lower_bound(self.count, cp.stride, cp, key) }
    }

    /// Galloping forward lower bound seeded at `hint` (the caller's live
    /// position): `O(log gap)` when the boundary is just ahead, `O(1)` when it
    /// IS the hint, never worse than `find_lower_bound_bytes`. Used by the join's
    /// equi merge walk, whose probe keys ascend, so the boundary only moves
    /// forward. `key` must be exactly `pk_stride` OPK bytes.
    pub(crate) fn advance_to(&self, key: &[u8], hint: usize) -> usize {
        let cp = self.pk_col_ptr();
        unsafe { super::seek::seek_advance_to(self.count, cp.stride, cp, key, hint) }
    }

    /// Copy every `[start, end)` range of `src`, ascending and disjoint, onto
    /// this batch's tail, one append setup serving the whole list.
    pub(crate) fn append_ranges(&mut self, src: &MemBatch<'_>, ranges: &[(usize, usize)]) {
        let rows = range_rows(ranges);
        // Before the session, which derives a payload string mask and takes a
        // pooled blob cache to copy nothing. An all-DELETE push emits one empty
        // range per row.
        if rows == 0 {
            return;
        }
        let mut s = self.append_session(rows);
        let at = s.carry(src, ranges);
        s.push_ranges(src, at, ranges);
    }

    /// Copy each of `rows` onto this batch's tail at `weight`.
    pub fn append_stored(&mut self, rows: &[StoredRow], weight: i64) {
        if rows.is_empty() {
            return;
        }
        let mut s = self.append_session(rows.len());
        for r in rows {
            s.push_row(&r.run, r.row, weight);
        }
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
            let order = crate::schema::payload_order::compare_full_rows(&self.schema, &*self, self.count - 1, &next, 0);
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
        if rows > 0 {
            // One session for every source: its setup would otherwise be paid
            // per source, which a fold of one-row runs is made of.
            let mut session = out.append_session(rows);
            for src in sources.filter(|src| src.count > 0) {
                let whole = [(0, src.count)];
                let heap_at = session.carry(&src, &whole);
                session.push_ranges(&src, heap_at, &whole);
            }
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
    /// `Self::empty_like` would build, without `take`'s two moves of the whole
    /// struct — and returns immediately when the batch is already free, which is
    /// what the VM's per-epoch register clear mostly does (`batch_release_bench`).
    pub fn release_buffers(&mut self) {
        if self.data.capacity() == 0 && self.blob.capacity() == 0 {
            return;
        }
        let nr = self.arena_regions();
        self.offsets[..nr].fill(0);
        recycle_buf(std::mem::take(&mut self.data));
        recycle_buf(std::mem::take(&mut self.blob));
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
        // data buffer stays allocated — capacity and offsets remain valid.
        self.truncate_to(RowMark { count: 0, blob_len: 0 });
        self.dead_heap = 0;
        self.consolidated = false;
    }

    /// Carry `src`'s heap onto this one to copy the string slots `kept_slots`
    /// of the rows `kept`, unless relocating them is cheaper or the carried heap
    /// would be wasteful. Returns the base the copied cells shift by, with every
    /// byte they cannot reference charged dead. `mask` is `src`'s string slots.
    pub(crate) fn carry_heap(
        &mut self,
        src: &MemBatch<'_>,
        mask: u64,
        kept_slots: u64,
        kept: &[(usize, usize)],
    ) -> Option<usize> {
        debug_assert!(kept.windows(2).all(|w| w[0].1 <= w[1].0) && kept.last().is_none_or(|k| k.1 <= src.count));
        debug_assert_eq!(kept_slots & !mask, 0, "carry_heap: a kept slot is not a string slot");
        let heap = src.blob.len();
        if kept_slots == 0 && heap != 0 {
            return None;
        }
        let mut dead = src.dead_heap;
        for pi in gnitz_wire::BitIter(mask & !kept_slots) {
            let cells = src.col_data(pi, 16).as_chunks::<16>().0;
            for &(start, end) in kept {
                for run in cells[start..end].chunks(DEAD_CHECK_CELLS) {
                    dead += run.iter().map(|cell| string_heap::cell_long_bytes(cell)).sum::<usize>();
                    if string_heap::heap_is_wasteful(dead, heap) {
                        return None;
                    }
                }
            }
        }
        let excluded = || string_heap::long_bytes_outside(src, mask, kept);
        let dead = string_heap::carried_dead(heap, dead, src.count, range_rows(kept), excluded)?;
        let base = self.blob.len();
        self.reserve_blob(src.blob.len());
        self.blob.extend_from_slice(src.blob);
        self.charge_dead(dead);
        Some(base)
    }

    /// Per-row byte stride of every fixed region, in region order.
    pub(super) fn strides(&self) -> &[u8] {
        &self.strides[..self.arena_regions()]
    }

    /// Append `source[row]` at `weight`; a `blob_cache` dedups repeated long-string
    /// spans.
    pub(crate) fn append_row_from_source<S: RowSource>(
        &mut self,
        weight: i64,
        source: &S,
        row: usize,
        mut blob_cache: Option<&mut BlobCache>,
    ) {
        if weight == 0 {
            return;
        }
        self.begin_row(source.get_pk_bytes(row), weight);
        let src_blob = source.blob();
        for pi in 0..self.schema.num_payload_cols() {
            let col = self.schema.columns[self.schema.payload_col_idx(pi)];
            let cell = source.get_col_ptr(row, pi, col.size() as usize);
            self.append_payload_cell(pi, col.type_code, cell, src_blob, blob_cache.as_deref_mut());
        }
        self.commit_row(source.get_null_word(row));
    }

    /// Append `cell` into output slot `out_pi`, relocating a STRING/BLOB struct
    /// into `self.blob` against `src_blob`.
    #[inline(always)]
    fn append_payload_cell(
        &mut self,
        out_pi: usize,
        type_code: TypeCode,
        cell: &[u8],
        src_blob: &[u8],
        blob_cache: Option<&mut BlobCache>,
    ) {
        if type_code.is_german_string() {
            let dest = relocate_german_string_vec(cell, src_blob, &mut self.blob, blob_cache);
            self.extend_col(out_pi, &dest);
        } else {
            self.extend_col(out_pi, cell);
        }
    }

    /// Append the columns at `locs` of `src`'s row into output slots `first..`,
    /// setting each NULL one's bit in `null_word`.
    #[inline(always)]
    pub(crate) fn append_cells_from<S: RowSource>(
        &mut self,
        first: usize,
        locs: &[ColumnLocator],
        src: &S,
        row: usize,
        null_word: &mut u64,
    ) {
        for (out_pi, loc) in (first..).zip(locs) {
            match *loc {
                ColumnLocator::Pk { .. } => {
                    let mut scratch = [0u8; 16];
                    self.extend_col(out_pi, loc.native_le_bytes(src, row, &mut scratch));
                }
                ColumnLocator::Payload { slot, size, type_code } => {
                    if loc.is_null(src, row) {
                        gnitz_wire::null_word_set(null_word, out_pi, true);
                    }
                    let cell = src.get_col_ptr(row, slot as usize, size as usize);
                    self.append_payload_cell(out_pi, type_code, cell, src.blob(), None);
                }
            }
        }
    }

    /// Consume this batch, consolidating it under its own label if needed.
    ///
    /// Fast path: an already-consolidated (or empty) `self` is returned by move,
    /// allocating nothing. Slow path: sorts and weight-folds into a fresh batch,
    /// then drops `self`.
    ///
    /// `#[inline]`: the move copies the whole struct, and the call is
    /// cross-crate, so without a hint it is not an inline candidate outside LTO.
    #[inline]
    pub fn into_consolidated(self) -> Batch {
        if self.consolidated_verified() {
            return self;
        }
        Self::consolidate_into_new(&self)
    }

    /// Consolidate this batch where it stands.
    pub fn consolidate_in_place(&mut self) {
        if !self.is_consolidated() {
            *self = Self::consolidate_into_new(self);
        }
    }

    /// An owned consolidated copy: folds if needed, else clones. The borrowed
    /// counterpart of [`Self::into_consolidated`].
    pub fn to_consolidated(&self) -> Batch {
        match self.consolidated_verified() {
            true => self.clone(),
            false => Self::consolidate_into_new(self),
        }
    }

    /// Sort and weight-fold `batch` into a fresh certified batch — the
    /// consolidation slow path the entry points above share.
    ///
    /// The fold runs first so the arena is sized to the survivor count, not the
    /// input row count.
    fn consolidate_into_new(batch: &Batch) -> Batch {
        let schema = &batch.schema;
        let mb = batch.as_mem_batch();
        let mut survivors: Vec<(u32, u32, i64)> = Vec::with_capacity(batch.count);
        merge::consolidate_groups(&mb, schema, &mut survivors);
        let mut result = super::scatter::materialize_carrying(std::slice::from_ref(&mb), schema, &survivors);
        result.certify_consolidated();
        result
    }

    /// `self`, or a tight copy when its buffers are not [`is_tight`] for its rows
    /// — a [`Self::compacted`] one when its heap is [`string_heap::heap_is_wasteful`].
    /// The bound counts a span several cells share once per cell, so a wasteful
    /// one is measured before a compaction is paid for.
    pub fn trimmed(mut self) -> Batch {
        self.debug_verify_dead_heap();
        if string_heap::heap_is_wasteful(self.dead_heap, self.blob.len()) {
            self.dead_heap = string_heap::measure_dead_heap(&self.as_mem_batch(), &self.schema);
            if string_heap::heap_is_wasteful(self.dead_heap, self.blob.len()) {
                return self.compacted();
            }
        }
        let mut offsets = [0usize; MAX_BATCH_REGIONS];
        let need =
            compute_offsets_into(&self.strides, self.arena_regions(), self.count, &mut offsets) + self.blob.len();
        if is_tight(self.data.capacity() + self.blob.capacity(), need) {
            return self;
        }
        Batch::clone(&self)
    }

    /// A tight copy with every long string relocated, so its heap holds no
    /// dead bytes.
    pub(crate) fn compacted(&self) -> Batch {
        let n = self.count;
        let mut out = Batch::with_capacity(&self.schema, n);
        out.append_session(n).push_ranges(&self.as_mem_batch(), None, &[(0, n)]);
        out.inherit_consolidated(self);
        out
    }

    /// Debug-only: `dead_heap` bounds the heap's unreferenced bytes from above.
    pub(super) fn debug_verify_dead_heap(&self) {
        if cfg!(debug_assertions) {
            let measured = string_heap::measure_dead_heap(&self.as_mem_batch(), &self.schema);
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

impl Drop for Batch {
    fn drop(&mut self) {
        recycle_buf(std::mem::take(&mut self.data));
        recycle_buf(std::mem::take(&mut self.blob));
    }
}

impl Clone for Batch {
    /// Clone all buffers into a new independent Batch (2 allocations).
    fn clone(&self) -> Self {
        // Rebuilt rather than copied through two pooled arenas holding zero bytes.
        // `empty_like` drops the consolidated claim, which is not observable on a
        // batch holding no rows and no heap.
        if self.holds_nothing() {
            return self.empty_like();
        }
        // Only the used portion of data (count-based, not capacity-based).
        let mut b = Self::from_mem_batch(&self.as_mem_batch(), &self.schema);
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
    fn blob(&self) -> &[u8] {
        Batch::blob(self)
    }
    #[inline(always)]
    fn row_count(&self) -> usize {
        Batch::len(self)
    }
}

impl gnitz_expr::MapTarget for Batch {
    #[inline(always)]
    fn null_bmp_mut(&mut self) -> &mut [u8] {
        self.null_bmp_data_mut()
    }
    #[inline(always)]
    fn slot_mut(&mut self, pi: usize) -> (&mut [u8], &mut [u8], &mut Vec<u8>) {
        self.col_null_and_blob_mut(pi)
    }
}

/// A batch of up to `max_rows` rows, written in place by `write_fn` through a
/// [`merge::DirectWriter`] over its uninitialized arena.
pub(crate) fn write_to_batch(
    schema: &SchemaDescriptor,
    max_rows: usize,
    max_blob: usize,
    write_fn: impl FnOnce(&mut merge::DirectWriter),
) -> Batch {
    if max_rows == 0 {
        return Batch::empty_with_schema(schema);
    }
    let mut b = Batch::with_capacity_blob(schema, max_rows, max_blob);
    let rows = {
        let nr = b.arena_regions();
        let mut writer = merge::DirectWriter::over_regions(
            &mut b.data,
            &b.offsets[..nr],
            &b.strides[..nr],
            b.capacity,
            &b.schema,
            &mut b.blob,
        );
        write_fn(&mut writer);
        writer.count
    };
    b.count = rows;
    if b.blob.is_empty() {
        recycle_buf(std::mem::take(&mut b.blob));
    }
    b
}

#[cfg(test)]
#[path = "tests/batch.rs"]
mod tests;
