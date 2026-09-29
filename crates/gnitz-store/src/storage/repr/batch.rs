//! Owned columnar batch type for Z-set rows.
//!
//! `Batch` owns its memory (two `Vec<u8>` buffers — data + blob).
//! `MemBatch<'a>` in the merge module is the borrowed slice-view counterpart.

use std::ops::Range;

use super::batch_pool::{acquire_arena, acquire_uninit, is_tight, recycle_buf};
use super::merge::{self, copy_string_cells, relocate_german_string_vec, BlobCache, ColPtr, MemBatch};
use crate::schema::key::NarrowPkOpk;
use crate::schema::{ColumnLocator, SchemaDescriptor, SchemaFacts};
use gnitz_expr::RowSource;
use gnitz_wire::{read_i64_le, read_u64_le, write_u64_le, TypeCode};

/// Max regions **including** the trailing blob region — the bound for the
/// WAL/wire region-directory arrays (ptrs / sizes / offsets / positions).
/// Owned by `gnitz_wire::region` (the framer's directory cap); the in-memory
/// cap below derives from it.
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
pub(in crate::storage) use gnitz_wire::{REG_NULL_BMP, REG_PAYLOAD_START, REG_PK, REG_WEIGHT};
/// Stride (in bytes) of the weight and null_bmp fixed regions.
const FIXED_REGION_STRIDE: u8 = 8;
pub(in crate::storage) const FIXED_REGION_BYTES: usize = FIXED_REGION_STRIDE as usize;

/// Total rows in a `[start, end)` row-range list — the shape every range-driven
/// path (`append_ranges`, `Batch::from_ranges`, `ops::MapPlan::append_map_ranges`)
/// sizes its destination by.
#[inline]
pub(crate) fn range_rows(ranges: &[(usize, usize)]) -> usize {
    ranges.iter().map(|&(s, e)| e - s).sum()
}

/// Write each region's byte offset into `offsets`, returning the total arena
/// size. Entries past `num_regions` are untouched; every caller passes an
/// all-zero array. An out-parameter rather than a return because the array is
/// 544 bytes and this does not inline, so a return was a `memcpy` per batch.
pub(in crate::storage) fn compute_offsets_into(
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

/// Append payload strides from `schema` into `strides` starting at `start`.
/// Returns the next free index (i.e. `start + num_payload_cols`).
fn fill_payload_strides(schema: &SchemaDescriptor, strides: &mut [u8; MAX_BATCH_REGIONS], start: usize) -> usize {
    let mut idx = start;
    for (_, col) in schema.payload_columns() {
        strides[idx] = col.size();
        idx += 1;
    }
    idx
}

/// Build a strides array from a SchemaDescriptor.
pub(in crate::storage) fn strides_from_schema(schema: &SchemaDescriptor) -> ([u8; MAX_BATCH_REGIONS], u8) {
    let mut strides = [0u8; MAX_BATCH_REGIONS];
    // Lossless: `SchemaDescriptor::new` asserts `pk_stride <= MAX_PK_BYTES`.
    strides[REG_PK] = schema.pk_stride() as u8;
    strides[REG_WEIGHT] = FIXED_REGION_STRIDE;
    strides[REG_NULL_BMP] = FIXED_REGION_STRIDE;
    let nr = fill_payload_strides(schema, &mut strides, REG_PAYLOAD_START);
    (strides, nr as u8)
}

/// Cells [`Batch::carry_heap`] charges dead between two waste checks.
const DEAD_CHECK_CELLS: usize = 1024;

/// One row's bytes across the fixed regions of `strides`.
pub(super) fn row_width(strides: &[u8]) -> usize {
    strides.iter().map(|&s| s as usize).sum()
}

/// Copy `count` rows of each region in `regions` from `src` (regions at
/// `src_offsets`) into `dst` (regions at `dst_offsets`), one bulk copy per
/// region. A caller whose two sides disagree on a region's pitch leaves that
/// region out and writes it itself.
///
/// # Safety
/// `src` and `dst` are distinct allocations; for every region `i` in `regions`,
/// both `src_offsets[i] + count * strides[i]` and `dst_offsets[i] + count *
/// strides[i]` are in bounds (both sides sized by `compute_offsets_into` for at
/// least `count` rows).
pub(super) unsafe fn copy_regions(
    src: &[u8],
    src_offsets: &[usize; MAX_BATCH_REGIONS],
    dst: &mut [u8],
    dst_offsets: &[usize; MAX_BATCH_REGIONS],
    strides: &[u8; MAX_BATCH_REGIONS],
    regions: std::ops::Range<usize>,
    count: usize,
) {
    for i in regions {
        let len = count * strides[i] as usize;
        if len == 0 {
            continue;
        }
        std::ptr::copy_nonoverlapping(
            src.as_ptr().add(src_offsets[i]),
            dst.as_mut_ptr().add(dst_offsets[i]),
            len,
        );
    }
}

/// Cached row-layout guarantee. Every mutation clears it; only code that has
/// just verified or produced the property raises it.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Layout {
    /// No order/fold guarantee.
    Raw,
    /// Strictly (PK, payload)-increasing and ghost-free (weights folded). The
    /// `into_consolidated` / merge fast paths trust this to skip a re-fold.
    Consolidated,
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
    pub(in crate::storage) blob: Vec<u8>,
    /// An upper bound on the bytes of `blob` no string cell references.
    pub(in crate::storage) dead_heap: usize,
    // `usize`, not `u32`: a single large batch's cumulative region offset can
    // exceed 4 GB (see `compute_offsets_into`). In-memory only — never serialized.
    offsets: [usize; MAX_BATCH_REGIONS],
    strides: [u8; MAX_BATCH_REGIONS],
    capacity: usize,
    /// Live row count. In-crate code reads and advances it directly; outside,
    /// [`Batch::len`] reads it and the row appenders are the only writers, so
    /// no counted row can exist without every region written for it.
    pub(crate) count: usize,
    /// Cached row-layout claim (private; mutated only through the layout API).
    /// Fresh batches default to `Raw` — a forgotten raise degrades to a safe
    /// re-fold, never a lie.
    layout: Layout,
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
            layout: Layout::Raw,
            schema: *schema,
        }
    }

    /// Zero-allocation empty batch with strides pre-filled from `schema`.
    ///
    /// Use this when the caller intends to populate the batch via `extend_*`,
    /// `append_batch`, or similar.  Strides and `schema` are set up front so
    /// no one-shot realloc fires on the first column write.
    pub fn empty_with_schema(schema: &SchemaDescriptor) -> Self {
        let (strides, _) = strides_from_schema(schema);
        Self::empty_from(strides, schema)
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
        let (strides, nr) = strides_from_schema(schema);
        let mut b = Self::empty_from(strides, schema);
        b.capacity = rows;
        let total_size = compute_offsets_into(&strides, nr as usize, rows, &mut b.offsets);
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

    /// `rows` all-zero rows, already published. The one shape [`Self::with_capacity`]
    /// cannot serve: a test that needs a batch of a given wire size but no
    /// particular content, and so writes no rows at all.
    ///
    /// `vec![0u8; _]` deliberately — a calloc of this size is demand-zero mmap
    /// the caller never faults in, where `with_capacity` + a memset would touch
    /// every page of what is routinely a 256 MiB arena.
    pub fn zeroed(schema: &SchemaDescriptor, rows: usize) -> Self {
        let (strides, nr) = strides_from_schema(schema);
        let mut b = Self::empty_from(strides, schema);
        b.capacity = rows.max(1);
        b.count = rows;
        let total_size = compute_offsets_into(&strides, nr as usize, rows.max(1), &mut b.offsets);
        b.data = vec![0u8; total_size];
        b
    }

    /// An owned, tightly packed copy of `mb`'s `count` rows and its whole heap,
    /// laid out under `schema`: `Raw`, `mb`'s dead-byte bound.
    pub(in crate::storage) fn from_mem_batch(mb: &MemBatch, schema: &SchemaDescriptor) -> Self {
        let (strides, nr) = strides_from_schema(schema);
        let nr = nr as usize;
        let mut b = Self::empty_from(strides, schema);
        let size = compute_offsets_into(&strides, nr, mb.count, &mut b.offsets);
        // SAFETY: `data` holds exactly `count` rows, all written by
        // `copy_regions`; distinct allocations; `mb`'s regions hold `count ×
        // stride` bytes each (a `Batch` by construction, a wire view by its parse).
        b.data = unsafe { acquire_uninit(size) };
        unsafe {
            copy_regions(
                mb.data,
                mb.offsets,
                &mut b.data,
                &b.offsets,
                &strides,
                REG_PK..nr,
                mb.count,
            )
        };
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
            strides_from_schema(s).0,
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
    #[inline]
    pub fn col_data(&self, pi: usize) -> &[u8] {
        self.region_at(REG_PAYLOAD_START + pi)
    }
    /// `#[inline(always)]`: two byte loads off the embedded descriptor, on the
    /// per-row appenders' loop bound.
    #[inline(always)]
    pub(crate) fn num_payload_cols(&self) -> usize {
        self.schema.num_payload_cols()
    }

    /// Regions in the `offsets`/`strides` arrays — the three fixed ones plus one
    /// per payload column. Also the blob region's index in the wire/shard
    /// layout, which has no slot in either array.
    #[inline(always)]
    pub(super) fn arena_regions(&self) -> usize {
        REG_PAYLOAD_START + self.num_payload_cols()
    }
    /// Byte width of the PK region (8 for U64 PK, 16 for U128/wide-narrow,
    /// `> 16` for compound wide PKs). Exposed for stride-consistency checks.
    #[inline]
    pub fn pk_stride(&self) -> u8 {
        self.strides[REG_PK]
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
    /// layout tag is untouched: weights are not part of element identity, so
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
    /// `i64::MIN` must not panic; element identity is untouched, so the layout
    /// claim carries over.
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
    /// Overwrite `row`'s null-bitmap word (bit N = payload slot N is NULL).
    #[inline]
    fn set_null_word(&mut self, row: usize, word: u64) {
        let off = self.offsets[REG_NULL_BMP] + row * FIXED_REGION_BYTES;
        write_u64_le(&mut self.data, off, word);
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
            // SAFETY: `copy_regions` writes the `count` rows a `Batch` reads;
            // distinct allocations; both sides sized per `compute_offsets_into`.
            let mut new_data = unsafe { acquire_uninit(new_total) };
            unsafe {
                copy_regions(
                    &self.data,
                    &self.offsets,
                    &mut new_data,
                    &new_offsets,
                    &self.strides,
                    0..nr,
                    self.count,
                );
            }

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
    pub(crate) fn extend_weight(&mut self, d: &[u8]) {
        self.extend_region(REG_WEIGHT, d);
    }
    #[inline]
    pub(crate) fn extend_null_bmp(&mut self, d: &[u8]) {
        self.extend_region(REG_NULL_BMP, d);
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

    /// Append a narrow PK from a `u128` — the [`NarrowPkOpk`] image (right-aligned
    /// big-endian) appended through [`Self::extend_pk_bytes`]. Valid for an
    /// **all-unsigned** PK only: OPK == BE there, so `widen_pk_be(extend_pk(v))`
    /// round-trips and a compound key is just the big-endian concatenation a
    /// packed `u128` already spells. A signed column anywhere needs
    /// `extend_pk_opk` / `extend_pk_bytes`; the assert below holds every call
    /// site to that.
    #[inline]
    pub(crate) fn extend_pk(&mut self, pk: u128) {
        debug_assert!(
            !self.schema.pk_columns().any(|(_, c)| c.is_signed()),
            "extend_pk writes an unflipped right-aligned big-endian key: a PK with a signed column \
             must use extend_pk_opk / extend_pk_bytes",
        );
        let key = NarrowPkOpk::new(pk, self.strides[REG_PK] as usize);
        self.extend_pk_bytes(key.bytes());
    }

    #[inline]
    pub(crate) fn extend_pk_bytes(&mut self, bytes: &[u8]) {
        assert_eq!(
            bytes.len(),
            self.strides[REG_PK] as usize,
            "extend_pk_bytes: length must equal pk_stride",
        );
        self.extend_region(REG_PK, bytes);
    }

    /// Open a row: PK region (exactly `pk_stride` OPK bytes) and weight. The
    /// caller then writes every payload column, in any order, and closes with
    /// [`Self::commit_row`].
    #[inline(always)]
    pub(crate) fn begin_row(&mut self, pk_bytes: &[u8], weight: i64) {
        self.extend_pk_bytes(pk_bytes);
        self.extend_weight(&weight.to_le_bytes());
    }

    /// Close the row [`Self::begin_row`] opened: the NULL word, then the count,
    /// then the dropped layout claim. The count moves last, so no reader sees a
    /// row before every region carries it.
    #[inline(always)]
    pub(crate) fn commit_row(&mut self, null_word: u64) {
        self.extend_null_bmp(&null_word.to_le_bytes());
        self.count += 1;
        self.layout = Layout::Raw;
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
    pub fn push_zero_filled_row(&mut self, pk: &[u8], weight: i64) {
        self.begin_row(pk, weight);
        for pi in 0..self.num_payload_cols() {
            self.fill_col_zero(pi);
        }
        self.commit_row(0);
    }

    /// Append a row's PK from native per-column values, OPK-encoding them
    /// (big-endian, with the sign-bit flip for signed columns) before the
    /// bytes are written. `native_col_vals` holds one native value per PK
    /// column in `pk_columns()` order. Use for signed or compound PK test
    /// tables where `extend_pk` (no sign flip) writes incorrect OPK bytes.
    ///
    /// Encodes through the production `schema::key` encoder — a layer *below*
    /// storage — so this stays a downward edge. Not test-only: it is what
    /// `BatchBuilder::begin_row_opk`, and through it the `SysRowSink` the
    /// catalog writes rows with, dispatches to.
    pub(crate) fn extend_pk_opk(&mut self, native_col_vals: &[u128]) {
        self.extend_pk_bytes(self.schema.opk_key_cols(native_col_vals).pk_bytes());
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

    /// Copy every `[start, end)` range of `src`, a batch of this one's layout,
    /// onto the tail: fixed-width regions in bulk, string cells rebased per
    /// `heap_at`. Leaves the layout `Raw`.
    fn append_ranges_inner(
        &mut self,
        src: &MemBatch<'_>,
        ranges: &[(usize, usize)],
        string_mask: u64,
        heap_at: Option<usize>,
        cache: &mut BlobCache,
    ) {
        debug_assert_eq!(
            src.pk_stride,
            self.pk_stride(),
            "append_ranges_inner: a source of another layout"
        );
        let total: usize = ranges
            .iter()
            .map(|&(start, end)| {
                assert!(start <= end, "append_ranges_inner: start ({start}) > end ({end})");
                assert!(
                    end <= src.count,
                    "append_ranges_inner: end ({end}) > src.count ({})",
                    src.count
                );
                end - start
            })
            .sum();
        if total == 0 {
            return;
        }
        self.reserve_rows(total);
        let npc = self.num_payload_cols();
        if heap_at.is_none() && !src.blob.is_empty() {
            // The rows this call copies, not the whole source heap: a many-run merge
            // appends into one output, and the whole heap per run ratchets capacity.
            self.reserve_blob(merge::prorated_blob_cap(src.blob.len(), src.count, total));
        }
        for &(start, end) in ranges {
            let n = end - start;
            if n == 0 {
                continue;
            }
            self.bulk_copy_region(REG_PK, src.pk(), start, end);
            self.bulk_copy_region(REG_WEIGHT, src.weight(), start, end);
            self.bulk_copy_region(REG_NULL_BMP, src.null_bmp(), start, end);
            for pi in 0..npc {
                let r = REG_PAYLOAD_START + pi;
                let cs = self.strides[r] as usize;
                if (string_mask >> pi) & 1 != 0 {
                    let at = self.offsets[r] + self.count * 16;
                    let cells = &src.col_data(pi, 16)[start * 16..end * 16];
                    copy_string_cells(
                        &mut self.data[at..at + n * 16],
                        cells,
                        src.blob,
                        &mut self.blob,
                        heap_at,
                        cache,
                    );
                } else if cs > 0 {
                    self.bulk_copy_region(r, src.col_data(pi, cs), start, end);
                }
            }
            self.count += n;
        }
        self.downgrade();
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

    /// Append every listed range of `src`, in list order: its string cells
    /// shifted onto its heap carried at `heap_at`, or relocated under the
    /// session's cache.
    pub(crate) fn push_ranges(&mut self, src: &MemBatch<'_>, heap_at: Option<usize>, ranges: &[(usize, usize)]) {
        self.dst
            .append_ranges_inner(src, ranges, self.mask, heap_at, &mut self.cache);
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
            self.dst.charge_dead(merge::row_long_bytes(src, self.mask, row));
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

    /// The cached layout claim (non-verifying). Use `layout()`/`is_*()` where the
    /// boolean suffices; use the verifying `*_verified` readers at trust sites.
    #[inline]
    pub fn layout(&self) -> Layout {
        self.layout
    }

    /// True if the rows are consolidated (strictly (PK, payload)-increasing and
    /// ghost-free). No batch of under two live rows can violate that, so those
    /// answer `true` whatever the cached tag says — which is what keeps a
    /// single-row DML push off the arena + argsort + scatter path.
    #[inline]
    pub fn is_consolidated(&self) -> bool {
        self.count == 0 || self.layout == Layout::Consolidated || (self.count == 1 && self.get_weight(0) != 0)
    }

    /// `is_consolidated()`, additionally asserting in debug builds that the data
    /// really is consolidated whenever the cached tag claims it. Prefer this over
    /// `is_consolidated()` at any skip-point that trusts the claim to avoid a
    /// re-fold.
    ///
    /// The one exception is a run out of a `RunSet`, which `RunSet::push` already
    /// verified on the way in.
    #[inline]
    pub(crate) fn consolidated_verified(&self) -> bool {
        #[cfg(debug_assertions)]
        if self.layout == Layout::Consolidated {
            self.debug_verify_consolidated(&self.schema);
        }
        self.is_consolidated()
    }

    /// Raise this batch's layout to `layout`, debug-verifying the data first. The
    /// ONLY way the guarantee goes up. Both provenance kernels and the wire-decode
    /// trust boundary call it, so an over-claiming kernel and a lying wire frame
    /// are caught identically — at the producer, schema in hand.
    ///
    /// **In release the verification does not run** — the `debug_verify_*` calls
    /// below are `#[cfg(debug_assertions)]`, so this is a single field store and
    /// the claim is entirely the caller's. Claiming `Consolidated` over data that
    /// is not sorted-and-summed makes every downstream skip-point fold weights
    /// against the wrong element, with no error and no assertion. Call it only
    /// where the code just produced the property it names.
    #[cfg_attr(not(debug_assertions), allow(unused_variables))]
    #[inline]
    pub fn certify_layout(&mut self, layout: Layout) {
        #[cfg(debug_assertions)]
        self.debug_verify_null_bits(&self.schema);
        #[cfg(debug_assertions)]
        match layout {
            Layout::Raw => {}
            Layout::Consolidated => self.debug_verify_consolidated(&self.schema),
        }
        self.layout = layout;
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
        self.downgrade();
    }

    /// Reset to no layout claim. Every order/fold-destroying mutator calls this.
    #[inline]
    pub(crate) fn downgrade(&mut self) {
        self.layout = Layout::Raw;
    }

    /// Copy `src`'s layout tag without re-verifying: sound for a copy of `src`
    /// that keeps its (PK, payload) order, distinctness and weights.
    #[inline]
    pub(crate) fn inherit_layout(&mut self, src: &Batch) {
        self.layout = src.layout;
    }

    /// Test-only: force the layout tag without verifying the data — for tests that
    /// deliberately construct an inconsistent (spoofed) batch to exercise a
    /// consumer's debug verifier or a defensive re-fold. Production has no such
    /// path: `certify_layout` always verifies.
    #[cfg(test)]
    pub(crate) fn set_layout_unchecked(&mut self, layout: Layout) {
        self.layout = layout;
    }

    /// Debug-only: every null bit sits under a payload column `schema` declares
    /// nullable, over a zeroed cell — what wire ingress checks in release.
    #[cfg(debug_assertions)]
    fn debug_verify_null_bits(&self, schema: &SchemaDescriptor) {
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
    #[cfg(debug_assertions)]
    pub(crate) fn debug_verify_consolidated(&self, schema: &SchemaDescriptor) {
        for i in 0..self.count {
            debug_assert_ne!(
                self.get_weight(i),
                0,
                "batch flagged consolidated, but row {i} has weight 0 (ghost not eliminated)"
            );
            if i + 1 < self.count {
                let ord = crate::schema::payload_order::compare_full_rows(schema, self, i, self, i + 1);
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
    pub(crate) fn total_bytes(&self) -> usize {
        self.count * row_width(self.strides()) + self.blob.len()
    }

    /// The rows `indices` names, in that order, at their own weights; none may be
    /// weight 0.
    pub(crate) fn indexed_rows(&self, indices: &[u32]) -> Self {
        let blob_cap = merge::prorated_blob_cap(self.blob.len(), self.count, indices.len());
        write_to_batch(&self.schema, indices.len(), blob_cap, |writer| {
            super::scatter::scatter_copy(&self.as_mem_batch(), indices, writer);
        })
    }

    /// The rows named by a strictly ascending `indices`, inheriting this batch's
    /// layout: taking rows in source order preserves (PK, payload) ordering and
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
        out.inherit_layout(self);
        out
    }

    /// Project every live row into one secondary-index entry. An index schema is
    /// all PK and no payload, so the entry *is* its key `(indexed col(s) [promoted]
    /// ‖ source PK)`, composed by `spec.write_entry` — which also decides the SQL
    /// NULL-distinctness skip.
    pub fn project_index(&self, spec: &crate::schema::KeySpec, idx_schema: &SchemaDescriptor) -> Batch {
        let idx_stride = idx_schema.pk_stride();
        assert_eq!(
            idx_stride,
            spec.key_size() + self.schema.pk_stride(),
            "index schema of another spec"
        );

        let mut out = Batch::with_capacity(idx_schema, self.count);
        // `SchemaDescriptor::new` bounds every index `pk_stride` by `MAX_PK_BYTES`.
        let mut idx_pk_buf = [0u8; crate::schema::MAX_PK_BYTES];

        let mb = self.as_mem_batch();

        for row in 0..self.count {
            let weight = mb.get_weight(row);
            if weight == 0 {
                continue;
            }
            if !spec.write_entry(&mb, row, &mut idx_pk_buf) {
                continue;
            }
            out.push_key_row(&idx_pk_buf[..idx_stride], weight);
        }

        // Left `Raw`: entries arrive in source order, and the index ingest folds them.
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
        out.count = n;
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

    /// Every row copied into `out_schema`, whose payload space is this batch's;
    /// each output key written by `rekey(src_key, dst_key)`. Left `Raw`.
    pub(crate) fn rekeyed(&self, out_schema: &SchemaDescriptor, rekey: impl Fn(&[u8], &mut [u8])) -> Batch {
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
    /// The layout claim carries over: an all-NULL column compares equal on every
    /// row, so (PK, payload) order and distinctness stay the original columns'.
    pub fn widened_with_nulls(&self, out_schema: &SchemaDescriptor, nulls_first: bool) -> Self {
        let in_schema = &self.schema;
        debug_assert_eq!(out_schema.pk_stride(), in_schema.pk_stride());
        let in_npc = in_schema.num_payload_cols();
        let out_npc = out_schema.num_payload_cols();
        debug_assert!(out_npc >= in_npc);
        let n = self.count;
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
        for row in 0..n {
            let in_null = self.get_null_word(row);
            let out_null = match nulls_first {
                true => new_null_bits | gnitz_wire::null_word_at(in_null, n_new),
                false => in_null | gnitz_wire::null_word_at(new_null_bits, in_npc),
            };
            output.set_null_word(row, out_null);
        }

        output.inherit_layout(self);
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
        let stride = self.pk_stride() as usize;
        let cp = self.pk_col_ptr();
        unsafe { super::seek::seek_lower_bound(self.count, stride, cp, key) }
    }

    /// Galloping forward lower bound seeded at `hint` (the caller's live
    /// position): `O(log gap)` when the boundary is just ahead, `O(1)` when it
    /// IS the hint, never worse than `find_lower_bound_bytes`. Used by the join's
    /// equi merge walk, whose probe keys ascend, so the boundary only moves
    /// forward. `key` must be exactly `pk_stride` OPK bytes.
    pub(crate) fn advance_to(&self, key: &[u8], hint: usize) -> usize {
        let stride = self.pk_stride() as usize;
        let cp = self.pk_col_ptr();
        unsafe { super::seek::seek_advance_to(self.count, stride, cp, key, hint) }
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
        if consolidated {
            self.layout = Layout::Consolidated;
        }
    }

    /// The rows of `src`'s disjoint ascending `[start, end)` ranges, inheriting its
    /// layout, in an arena with room for `spare_rows` more.
    pub(crate) fn from_ranges(src: &Batch, ranges: &[(usize, usize)], spare_rows: usize) -> Batch {
        let mut out = Batch::with_capacity(&src.schema, range_rows(ranges) + spare_rows);
        out.append_ranges(&src.as_mem_batch(), ranges);
        out.inherit_layout(src);
        out
    }

    /// Every source's rows, in order, in one fresh `Raw` batch.
    pub fn concat<'s>(schema: &SchemaDescriptor, sources: impl Iterator<Item = MemBatch<'s>> + Clone) -> Batch {
        let (mut rows, mut blob) = (0usize, 0usize);
        for src in sources.clone() {
            rows += src.count;
            blob += src.blob.len();
        }
        let mut out = Batch::with_capacity_blob(schema, rows, blob);
        for src in sources {
            out.append_ranges(&src, &[(0, src.count)]);
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
    /// `Self::empty_like` would build, without `take`'s two moves of a 1 KiB
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
        self.downgrade();
    }

    /// Reset to empty without freeing buffer allocations.
    pub fn clear(&mut self) {
        if self.holds_nothing() {
            return;
        }
        // data buffer stays allocated — capacity and offsets remain valid.
        self.truncate_to(RowMark { count: 0, blob_len: 0 });
        self.dead_heap = 0;
        self.downgrade();
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
                    dead += run.iter().map(|cell| merge::cell_long_bytes(cell)).sum::<usize>();
                    if merge::heap_is_wasteful(dead, heap) {
                        return None;
                    }
                }
            }
        }
        let excluded = || merge::long_bytes_outside(src, mask, kept);
        let dead = merge::carried_dead(heap, dead, src.count, range_rows(kept), excluded)?;
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
        blob_cache: Option<&mut BlobCache>,
    ) {
        if weight == 0 {
            return;
        }
        self.begin_row(source.get_pk_bytes(row), weight);
        self.append_payload_cols(0..self.schema.num_payload_cols(), source, row, blob_cache);
        self.commit_row(source.get_null_word(row));
    }

    /// Append `src`'s row `row` payload columns into output slots `out`, fed from
    /// source slot `out_pi - out.start`.
    #[inline]
    pub(crate) fn append_payload_cols<S: RowSource>(
        &mut self,
        out: Range<usize>,
        src: &S,
        row: usize,
        mut blob_cache: Option<&mut BlobCache>,
    ) {
        let src_blob = src.blob();
        let base = out.start;
        for out_pi in out {
            let col = self.schema.columns[self.schema.payload_col_idx(out_pi)];
            let cell = src.get_col_ptr(row, out_pi - base, col.size() as usize);
            self.append_payload_cell(out_pi, col.type_code, cell, src_blob, blob_cache.as_deref_mut());
        }
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

    /// Consume this batch, consolidating it under its own label if needed. The
    /// returned batch is certified `Consolidated`.
    ///
    /// Fast path: an already-consolidated (or empty) `self` is returned by move,
    /// allocating nothing. Slow path: sorts and weight-folds into a fresh batch,
    /// then drops `self`.
    ///
    /// `#[inline]`: the move is still a 1 KiB struct copy, and the call is
    /// cross-crate, so without a hint it is not an inline candidate outside LTO.
    #[inline]
    pub fn into_consolidated(mut self) -> Batch {
        if self.consolidated_verified() {
            // Already consolidated, or empty (structurally consolidated): return
            // by move. Pin the tag so an empty `Raw` batch still reports
            // `Consolidated` to downstream trust sites.
            self.layout = Layout::Consolidated;
            return self;
        }
        Self::consolidate_into_new(&self)
    }

    /// Consolidate this batch where it stands. The tag is read first so an
    /// already-folded batch is not moved out and back for nothing;
    /// [`Self::into_consolidated`] then owns the fold.
    pub fn consolidate_in_place(&mut self) {
        if !self.is_consolidated() {
            *self = self.take().into_consolidated();
        }
    }

    /// Consolidate a borrowed batch if needed. Returns `None` when the batch is
    /// already consolidated or empty (caller borrows the original). Returns
    /// `Some(batch)` — certified `Consolidated` — when a new batch was allocated.
    ///
    /// Idiomatic usage:
    /// ```ignore
    /// let cs = Batch::consolidate_if_needed(delta);
    /// let c: &Batch = cs.as_ref().unwrap_or(delta);
    /// ```
    pub(crate) fn consolidate_if_needed(batch: &Batch) -> Option<Batch> {
        (!batch.consolidated_verified()).then(|| Self::consolidate_into_new(batch))
    }

    /// An owned, certified-`Consolidated` copy: folds if needed, else clones and
    /// pins the tag. The borrowed counterpart of [`Self::into_consolidated`].
    pub(crate) fn to_consolidated(&self) -> Batch {
        match Self::consolidate_if_needed(self) {
            Some(folded) => folded,
            None => {
                let mut c = Batch::clone(self);
                c.layout = Layout::Consolidated;
                c
            }
        }
    }

    /// Sort and weight-fold `batch` into a fresh certified batch — the
    /// consolidation slow path both entry points above share.
    ///
    /// The fold runs first so the arena is sized to the survivor count, not the
    /// input row count.
    fn consolidate_into_new(batch: &Batch) -> Batch {
        let schema = &batch.schema;
        let mb = batch.as_mem_batch();
        let mut survivors: Vec<(u32, u32, i64)> = Vec::with_capacity(batch.count);
        merge::consolidate_groups(&mb, schema, &mut survivors);
        let mut result = super::scatter::materialize_carrying(std::slice::from_ref(&mb), schema, &survivors);
        result.certify_layout(Layout::Consolidated);
        result
    }

    /// `self`, or a tight copy when its buffers are not [`is_tight`] for its rows
    /// — a [`Self::compacted`] one when its heap is [`merge::heap_is_wasteful`].
    /// The bound counts a span several cells share once per cell, so a wasteful
    /// one is measured before a compaction is paid for.
    pub(crate) fn trimmed(mut self) -> Batch {
        self.debug_verify_dead_heap();
        if merge::heap_is_wasteful(self.dead_heap, self.blob.len()) {
            self.dead_heap = super::batch_wire::measure_dead_heap(&self.as_mem_batch(), &self.schema);
            if merge::heap_is_wasteful(self.dead_heap, self.blob.len()) {
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
        out.inherit_layout(self);
        out
    }

    /// Debug-only: `dead_heap` bounds the heap's unreferenced bytes from above.
    pub(super) fn debug_verify_dead_heap(&self) {
        if cfg!(debug_assertions) {
            let measured = super::batch_wire::measure_dead_heap(&self.as_mem_batch(), &self.schema);
            debug_assert!(
                measured <= self.dead_heap,
                "heap holds {measured} unreferenced bytes, past its bound of {} ({} bytes, {} rows)",
                self.dead_heap,
                self.blob.len(),
                self.count,
            );
        }
    }

    #[cfg(test)]
    pub(crate) fn data_capacity(&self) -> usize {
        self.data.capacity()
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
        // `empty_like` drops the layout tag, which is not observable on a batch
        // holding no rows and no heap.
        if self.holds_nothing() {
            return self.empty_like();
        }
        // Only the used portion of data (count-based, not capacity-based).
        let mut b = Self::from_mem_batch(&self.as_mem_batch(), &self.schema);
        b.layout = self.layout;
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
