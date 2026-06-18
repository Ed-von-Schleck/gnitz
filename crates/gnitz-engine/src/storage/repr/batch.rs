//! Owned columnar batch type for Z-set rows.
//!
//! `Batch` owns its memory (two `Vec<u8>` buffers — data + blob).
//! `MemBatch<'a>` in the merge module is the borrowed slice-view counterpart.

use std::sync::atomic::{AtomicU64, Ordering};

use super::columnar::ColumnarSource;
use super::merge::{self, MemBatch};
use crate::foundation::codec::{read_i64_le, read_u64_le};
use crate::schema::{self, type_code, BlobCache, SchemaColumn, SchemaDescriptor};

static BLOB_ID_CTR: AtomicU64 = AtomicU64::new(1);
#[inline(always)]
fn next_blob_id() -> u64 {
    BLOB_ID_CTR.fetch_add(1, Ordering::Relaxed)
}

/// Minimum allocation size to request transparent hugepage backing.
/// Below this, hugepage promotion is impossible (no full 2MB PMD region).
const HUGEPAGE_THRESHOLD: usize = 2 * 1024 * 1024;

/// Maximum regions tracked in the `offsets`/`strides` arrays:
/// 3 fixed (pk, weight, null_bmp) + up to 63 payload columns
/// (schema max is 64 total columns, 1 is the PK).  The blob is not in this
/// array; it lives in `self.blob` and is accounted for separately.
/// 3 + 63 = 66, rounded up to 68 to keep the array size as a multiple of 4.
pub(crate) const MAX_BATCH_REGIONS: usize = 68;

// ── Region indices into `offsets` / `strides` ───────────────────────────────
//
// Three fixed regions (PK is 16 bytes/row; weight and null_bmp are 8 bytes/row);
// payload columns start at index 3 and continue for `num_payload_cols()` slots.
// Use these constants instead of bare numeric literals.
pub(in crate::storage) const REG_PK: usize = 0;
pub(in crate::storage) const REG_WEIGHT: usize = 1;
pub(in crate::storage) const REG_NULL_BMP: usize = 2;
pub(in crate::storage) const REG_PAYLOAD_START: usize = 3;
/// Stride (in bytes) of the weight and null_bmp fixed regions.
const FIXED_REGION_STRIDE: u8 = 8;
pub(in crate::storage) const FIXED_REGION_BYTES: usize = FIXED_REGION_STRIDE as usize;

/// Allocate a zeroed buffer and request hugepage backing for large allocations.
///
/// For sizes >= HUGEPAGE_THRESHOLD, calls `calloc` (via `vec!`) which for large
/// allocations uses an internal `mmap`, returning demand-zero pages not yet faulted
/// in. The subsequent `madvise(MADV_HUGEPAGE)` causes the kernel to allocate 2MB
/// zero pages on first access instead of 4KB pages.
///
/// For sizes < HUGEPAGE_THRESHOLD, behaves identically to `vec![0u8; size]`.
#[inline]
fn alloc_large_zeroed(size: usize) -> Vec<u8> {
    let v = vec![0u8; size];
    if size >= HUGEPAGE_THRESHOLD {
        crate::foundation::posix_io::madvise_hugepage(v.as_ptr() as *mut u8, size);
    }
    v
}

/// Compute byte offsets for each region given strides and row capacity.
///
/// Every region start is padded up to an 8-byte boundary. The null-bitmap and
/// weight regions are read/written as `*mut u64` (e.g. `plan.rs` casts
/// `null_bmp_data_mut()`), which is UB on an unaligned pointer; an odd
/// `capacity * pk_stride` (small catalog batches, a final partial morsel) would
/// otherwise misalign the following region. MORSEL=256 batches are already
/// aligned, but smaller batches are not. Total allocation grows by at most 7
/// bytes per region boundary.
pub(in crate::storage) fn compute_offsets(
    strides: &[u8; MAX_BATCH_REGIONS],
    num_regions: usize,
    capacity: usize,
) -> ([usize; MAX_BATCH_REGIONS], usize) {
    // Offsets are `usize`, not `u32`: a single large batch (a wide multi-column
    // join, a bulk full-scan/merge) can have a cumulative offset > 4 GB even
    // though each individual region is still capped at 4 GB by the u32 wire
    // region sizes. A `u32` store silently truncated the per-region offset, so
    // `region_ptr` aliased an earlier region — silent corruption. Not a wire
    // change: the WAL/exchange encoding serializes region *sizes* and recomputes
    // offsets via this fn on receive, so offsets never cross a process boundary.
    let mut offsets = [0usize; MAX_BATCH_REGIONS];
    let mut off = 0usize;
    for i in 0..num_regions {
        off = (off + 7) & !7;
        offsets[i] = off;
        off += capacity * strides[i] as usize;
    }
    (offsets, off)
}

/// Copy a 16-byte German String struct from source into a destination slice,
/// relocating long-string blob data into `dst_blob`.
pub(in crate::storage) fn relocate_string_cell(
    src_struct: &[u8],
    src_blob: &[u8],
    dst: &mut [u8],
    dst_blob: &mut Vec<u8>,
) {
    let dest = crate::schema::relocate_german_string_vec(src_struct, src_blob, dst_blob, None);
    dst[..16].copy_from_slice(&dest);
}

/// Append payload strides from `schema` into `strides` starting at `start`.
/// Returns the next free index (i.e. `start + num_payload_cols`).
fn fill_payload_strides(schema: &SchemaDescriptor, strides: &mut [u8; MAX_BATCH_REGIONS], start: usize) -> usize {
    let mut idx = start;
    for (_, _, col) in schema.payload_columns() {
        // A join carries both sides' payload columns through the intermediate
        // batch, so two wide tables can drive `idx` past the region limit. Turn
        // the would-be bare index-OOB into a named diagnostic (the correct
        // long-term fix is a plan-time query error, tracked separately).
        assert!(
            idx < MAX_BATCH_REGIONS,
            "fill_payload_strides: combined payload column count exceeds the batch \
             region limit ({MAX_BATCH_REGIONS} = {REG_PAYLOAD_START} + {} payload cols)",
            MAX_BATCH_REGIONS - REG_PAYLOAD_START,
        );
        strides[idx] = col.size();
        idx += 1;
    }
    idx
}

/// Physical byte stride for the PK region: 8 for U64 PK, 16 for U128 PK.
pub(super) fn pk_stride(schema: &SchemaDescriptor) -> u8 {
    schema.pk_stride()
}

/// Build a strides array from a SchemaDescriptor.
pub(in crate::storage) fn strides_from_schema(schema: &SchemaDescriptor) -> ([u8; MAX_BATCH_REGIONS], u8) {
    let mut strides = [0u8; MAX_BATCH_REGIONS];
    strides[REG_PK] = pk_stride(schema);
    strides[REG_WEIGHT] = FIXED_REGION_STRIDE;
    strides[REG_NULL_BMP] = FIXED_REGION_STRIDE;
    let nr = fill_payload_strides(schema, &mut strides, REG_PAYLOAD_START);
    (strides, nr as u8)
}

/// Carve a contiguous arena into the four `DirectWriter` regions: PK, weight,
/// null bitmap, then one slice per payload column. Region starts honour the
/// exact same 8-byte-aligned offsets `compute_offsets` produces (and the
/// resulting `Batch` reads back through), so writer and reader never disagree
/// when `rows * pk_stride` is not 8-aligned.
#[allow(clippy::type_complexity)]
pub(crate) fn carve_writer_slices<'a>(
    data: &'a mut [u8],
    schema: &SchemaDescriptor,
    rows: usize,
) -> (&'a mut [u8], &'a mut [u8], &'a mut [u8], Vec<&'a mut [u8]>) {
    let (strides, nr) = strides_from_schema(schema);
    let nr = nr as usize;
    let (offsets, _total) = compute_offsets(&strides, nr, rows);

    // Walk regions in order, splitting off [alignment pad | region] for each.
    // `base` tracks the absolute offset of `rest[0]` within `data`, so
    // `offsets[r] - base` is the padding to discard before region `r`.
    let mut slices: Vec<&mut [u8]> = Vec::with_capacity(nr);
    let mut rest: &mut [u8] = data;
    let mut base = 0usize;
    for r in 0..nr {
        let pad = offsets[r] - base;
        let after_pad = std::mem::take(&mut rest).split_at_mut(pad).1;
        let sz = rows * strides[r] as usize;
        let (region, remainder) = after_pad.split_at_mut(sz);
        slices.push(region);
        base = offsets[r] + sz;
        rest = remainder;
    }

    let mut it = slices.into_iter();
    let pk = it.next().expect("REG_PK");
    let weight = it.next().expect("REG_WEIGHT");
    let null_bmp = it.next().expect("REG_NULL_BMP");
    let col_slices: Vec<&mut [u8]> = it.collect();
    (pk, weight, null_bmp, col_slices)
}

/// Owned columnar batch.  All fixed-stride column data lives in a single
/// contiguous `data` buffer.  Blob data is separate (variable-length).
///
/// Layout of `data`: `[pk | weight | null_bmp | col_0 | ... | col_{N-1}]`
/// Each region has `capacity * stride` bytes allocated; `count * stride` bytes
/// contain data.  The PK region uses pk_stride bytes/row; 8 for U64, 16 for
/// U128, wider for compound PKs.
///
/// **2 heap allocations** (data + blob) instead of N+7.
pub struct Batch {
    data: Vec<u8>,
    pub blob: Vec<u8>,
    // `usize`, not `u32`: a single large batch's cumulative region offset can
    // exceed 4 GB (see `compute_offsets`). In-memory only — never serialized.
    offsets: [usize; MAX_BATCH_REGIONS],
    strides: [u8; MAX_BATCH_REGIONS],
    num_regions: u8,
    capacity: u32,
    pub count: usize,
    pub(crate) sorted: bool,
    pub(crate) consolidated: bool,
    pub schema: Option<SchemaDescriptor>,
    /// Identity token for blob-sharing: two batches with equal `blob_id` have
    /// identical blob content, making verbatim 16-byte German String struct
    /// copies safe.  Set by `share_blob_from` and checked by
    /// `append_batch_no_blob_reloc`.
    pub(crate) blob_id: u64,
}

impl Batch {
    // ── Constructors ────────────────────────────────────────────────────

    /// Create an empty, zero-allocation placeholder batch with only the three
    /// fixed regions (pk/weight/null_bmp) described.  Payload strides
    /// remain zero, so the batch is NOT safe to populate via `extend_*` or
    /// `append_batch` — use `empty_with_schema` or `with_schema` for that.
    ///
    /// Intended for zero-row return values and swap-placeholder slots.
    pub fn empty(num_payload_cols: usize, pk_stride: u8) -> Self {
        // Zero stride is accepted: `SchemaDescriptor::default()` has no PK
        // columns and is a legitimate placeholder caller.
        debug_assert!(
            pk_stride as usize <= schema::MAX_PK_BYTES,
            "pk_stride out of range: {pk_stride}",
        );
        let nr = REG_PAYLOAD_START + num_payload_cols;
        let mut strides = [0u8; MAX_BATCH_REGIONS];
        strides[REG_PK] = pk_stride;
        strides[REG_WEIGHT] = FIXED_REGION_STRIDE;
        strides[REG_NULL_BMP] = FIXED_REGION_STRIDE;
        Batch {
            data: Vec::new(),
            blob: Vec::new(),
            offsets: [0usize; MAX_BATCH_REGIONS],
            strides,
            num_regions: nr as u8,
            capacity: 0,
            count: 0,
            sorted: true,
            consolidated: true,
            schema: None,
            blob_id: next_blob_id(),
        }
    }

    /// Zero-allocation empty batch with strides pre-filled from `schema`.
    ///
    /// Use this — not `empty(npc)` — when the caller intends to populate the
    /// batch via `extend_*`, `append_batch`, or similar.  Strides and
    /// `schema` are set up front so no one-shot realloc fires on the first
    /// column write.
    pub fn empty_with_schema(schema: &SchemaDescriptor) -> Self {
        let (strides, nr) = strides_from_schema(schema);
        Batch {
            data: Vec::new(),
            blob: Vec::new(),
            offsets: [0usize; MAX_BATCH_REGIONS],
            strides,
            num_regions: nr,
            capacity: 0,
            count: 0,
            sorted: true,
            consolidated: true,
            schema: Some(*schema),
            blob_id: next_blob_id(),
        }
    }

    /// Zero-allocation empty batch whose payload columns are `left_schema`'s
    /// non-PK columns followed by `right_schema`'s non-PK columns.  Used for
    /// join outputs ([left_PK, left_payload..., right_payload...]), which
    /// have no single `SchemaDescriptor` in the engine.
    pub fn empty_joined(left_schema: &SchemaDescriptor, right_schema: &SchemaDescriptor) -> Self {
        let mut strides = [0u8; MAX_BATCH_REGIONS];
        strides[REG_PK] = pk_stride(left_schema);
        strides[REG_WEIGHT] = FIXED_REGION_STRIDE;
        strides[REG_NULL_BMP] = FIXED_REGION_STRIDE;
        let mid = fill_payload_strides(left_schema, &mut strides, REG_PAYLOAD_START);
        let out_idx = fill_payload_strides(right_schema, &mut strides, mid);
        Batch {
            data: Vec::new(),
            blob: Vec::new(),
            offsets: [0usize; MAX_BATCH_REGIONS],
            strides,
            num_regions: out_idx as u8,
            capacity: 0,
            count: 0,
            sorted: true,
            consolidated: true,
            schema: None,
            blob_id: next_blob_id(),
        }
    }

    /// Create an empty batch with schema, pre-allocated for `initial_capacity` rows.
    pub fn with_schema(schema: SchemaDescriptor, initial_capacity: usize) -> Self {
        let cap = initial_capacity.max(1);
        let (strides, nr) = strides_from_schema(&schema);
        let (offsets, total_size) = compute_offsets(&strides, nr as usize, cap);

        // Invariant: `data` is fully zero-filled on return.  Some callers
        // (null-extend, scalar-func EMIT_NULL) leave payload columns unwritten
        // and depend on that.  Undersized pool buffers are recycled rather
        // than grown in place: Vec::reserve on a too-small buffer copies the
        // old contents forward before zeroing, which is slower than a fresh
        // zeroed allocation.
        let data = if total_size >= HUGEPAGE_THRESHOLD {
            let mut v = vec![0u8; total_size];
            crate::foundation::posix_io::madvise_hugepage(v.as_mut_ptr(), total_size);
            v
        } else {
            let mut buf = super::batch_pool::acquire_buf();
            if buf.capacity() >= total_size {
                buf.resize(total_size, 0);
                buf
            } else {
                drop(buf); // evict the undersized buffer; pool converges to larger sizes
                vec![0u8; total_size]
            }
        };

        Batch {
            data,
            blob: Vec::with_capacity(64),
            offsets,
            strides,
            num_regions: nr,
            capacity: cap as u32,
            count: 0,
            sorted: true,
            consolidated: true,
            schema: Some(schema),
            blob_id: next_blob_id(),
        }
    }

    /// Construct by copying data in from region pointers.
    ///
    /// Region layout: pk (16 bytes/row), weight, null_bmp,
    /// payload_col_0 .. payload_col_{N-1}, blob.
    ///
    /// # Safety
    /// `ptrs[i]` must point to at least `sizes[i]` readable bytes.
    #[allow(clippy::uninit_vec)]
    pub unsafe fn from_regions(ptrs: &[*const u8], sizes: &[u32], count: usize, num_payload_cols: usize) -> Self {
        if count == 0 {
            return Self::empty(num_payload_cols, 16);
        }

        let nr = REG_PAYLOAD_START + num_payload_cols;
        // Derive strides from sizes / count.
        let mut strides = [0u8; MAX_BATCH_REGIONS];
        for i in 0..nr {
            assert_eq!(
                sizes[i] as usize % count,
                0,
                "from_regions: region {i} size ({}) is not a multiple of row count ({count})",
                sizes[i],
            );
            let stride = sizes[i] as usize / count;
            assert!(
                stride <= u8::MAX as usize,
                "from_regions: region {i} stride ({stride}) exceeds u8::MAX",
            );
            strides[i] = stride as u8;
        }
        let (offsets, total_size) = compute_offsets(&strides, nr, count);

        let mut data = super::batch_pool::acquire_buf();
        data.clear();
        data.resize(total_size, 0);

        // Copy each region into the contiguous buffer.
        for i in 0..nr {
            let sz = sizes[i] as usize;
            if sz > 0 && !ptrs[i].is_null() {
                let off = offsets[i];
                std::ptr::copy_nonoverlapping(ptrs[i], data.as_mut_ptr().add(off), sz);
            }
        }

        // Blob is separate — try pool first.
        let blob_idx = REG_PAYLOAD_START + num_payload_cols;
        let blob_sz = sizes[blob_idx] as usize;
        let blob = if blob_sz > 0 && !ptrs[blob_idx].is_null() {
            // SAFETY: copy_nonoverlapping below writes all blob_sz bytes.
            let mut v = super::batch_pool::acquire_buf();
            v.clear();
            v.reserve(blob_sz);
            unsafe {
                v.set_len(blob_sz);
            }
            std::ptr::copy_nonoverlapping(ptrs[blob_idx], v.as_mut_ptr(), blob_sz);
            v
        } else {
            Vec::new()
        };

        Batch {
            data,
            blob,
            offsets,
            strides,
            num_regions: nr as u8,
            capacity: count as u32,
            count,
            sorted: false,
            consolidated: false,
            schema: None,
            blob_id: next_blob_id(),
        }
    }

    /// Construct a `Batch` from fully pre-built, correctly-laid-out buffers.
    ///
    /// `data` must be at least `count * strides[i]` bytes starting at
    /// `offsets[i]` for every `i < num_regions`, as produced by
    /// `compute_offsets`.  Used by `slice_to_owned_batch` to avoid an
    /// intermediate copy.
    ///
    /// # Safety
    /// Caller must guarantee the layout invariant described above.
    pub(in crate::storage) unsafe fn from_prebuilt(
        data: Vec<u8>,
        blob: Vec<u8>,
        strides: [u8; MAX_BATCH_REGIONS],
        offsets: [usize; MAX_BATCH_REGIONS],
        num_regions: u8,
        count: usize,
    ) -> Self {
        Batch {
            data,
            blob,
            offsets,
            strides,
            num_regions,
            capacity: count as u32,
            count,
            sorted: false,
            consolidated: false,
            schema: None,
            blob_id: next_blob_id(),
        }
    }

    // ── Schema installation ─────────────────────────────────────────────

    /// Install a schema on this batch after verifying its column count
    /// matches the batch's physical payload regions. Every code path that
    /// wants to mutate `batch.schema` from outside the constructors MUST go
    /// through this helper: it turns a latent "batch shape != declared
    /// shape" bug into a localized panic at the first assignment, instead
    /// of a cryptic OOB slice panic 5 call-frames later (e.g. in
    /// scalar_func::copy_column).
    #[inline]
    pub fn set_schema(&mut self, s: SchemaDescriptor) {
        // The batch carries one combined PK region (all PK columns
        // concatenated) + one payload region per non-PK column. So the
        // batch's payload-region count must equal the schema's non-PK
        // column count regardless of single-vs-compound PK.
        debug_assert_eq!(
            self.num_payload_cols(),
            s.num_payload_cols(),
            "Batch::set_schema: batch has {} payload cols, schema declares {} payload cols \
             (pk_count={})",
            self.num_payload_cols(),
            s.num_payload_cols(),
            s.pk_indices().len()
        );
        self.schema = Some(s);
    }

    // ── Read accessors ──────────────────────────────────────────────────

    #[inline]
    pub fn pk_data(&self) -> &[u8] {
        let off = self.offsets[REG_PK];
        &self.data[off..off + self.count * self.strides[REG_PK] as usize]
    }
    #[inline]
    pub fn weight_data(&self) -> &[u8] {
        let off = self.offsets[REG_WEIGHT];
        &self.data[off..off + self.count * 8]
    }
    #[inline]
    pub fn null_bmp_data(&self) -> &[u8] {
        let off = self.offsets[REG_NULL_BMP];
        &self.data[off..off + self.count * 8]
    }
    #[inline]
    pub fn col_data(&self, pi: usize) -> &[u8] {
        let r = REG_PAYLOAD_START + pi;
        let off = self.offsets[r];
        &self.data[off..off + self.count * self.strides[r] as usize]
    }
    #[inline]
    pub fn num_payload_cols(&self) -> usize {
        self.num_regions as usize - REG_PAYLOAD_START
    }
    /// Byte width of the PK region (8 for U64 PK, 16 for U128/wide-narrow,
    /// `> 16` for compound wide PKs). Exposed for stride-consistency checks.
    #[inline]
    pub fn pk_stride(&self) -> u8 {
        self.strides[REG_PK]
    }

    // ── Mutable slice accessors ─────────────────────────────────────────

    #[inline]
    pub fn pk_data_mut(&mut self) -> &mut [u8] {
        let off = self.offsets[REG_PK];
        let end = off + self.count * self.strides[REG_PK] as usize;
        &mut self.data[off..end]
    }
    #[inline]
    pub fn weight_data_mut(&mut self) -> &mut [u8] {
        let off = self.offsets[REG_WEIGHT];
        let end = off + self.count * 8;
        &mut self.data[off..end]
    }
    #[inline]
    pub fn null_bmp_data_mut(&mut self) -> &mut [u8] {
        let off = self.offsets[REG_NULL_BMP];
        let end = off + self.count * 8;
        &mut self.data[off..end]
    }
    #[inline]
    pub fn col_data_mut(&mut self, pi: usize) -> &mut [u8] {
        let r = REG_PAYLOAD_START + pi;
        let off = self.offsets[r];
        let end = off + self.count * self.strides[r] as usize;
        &mut self.data[off..end]
    }

    /// Return a `DirectWriter` over this batch's data buffer, sized for
    /// `capacity` rows (not `count`). Used to re-fill a batch whose allocation
    /// is already live but whose row count has been reset (`clear()`, then
    /// `reserve_rows`).
    ///
    /// The recycled data buffer is **not** re-zeroed by `clear()`, so writers
    /// must use the unconditional-copy `scatter_*` variants — a nullable-skip
    /// would leak stale bytes through. Caller must update `self.count` after
    /// the writer is dropped.
    pub(crate) fn capacity_writer(&mut self) -> merge::DirectWriter<'_> {
        let cap = self.capacity as usize;
        let schema = self.schema.expect("capacity_writer requires schema");
        let (pk, weight, null_bmp, col_slices) = carve_writer_slices(&mut self.data, &schema, cap);
        merge::DirectWriter::new(pk, weight, null_bmp, col_slices, &mut self.blob, schema, cap)
    }

    // ── Row accessors ───────────────────────────────────────────────────

    #[inline(always)]
    pub fn get_pk(&self, row: usize) -> u128 {
        let stride = self.strides[REG_PK] as usize;
        let off = self.offsets[REG_PK] + row * stride;
        gnitz_wire::widen_pk_be(&self.data[off..off + stride], stride)
    }

    /// Owned-`Batch` sibling of `MemBatch::get_pk_bytes`. Returns exactly
    /// `pk_stride` bytes; the wide-PK (`pk_stride > 16`) catalog constraint
    /// checks in `catalog/validation.rs` key on it where a `u128` cannot
    /// encode the PK.
    #[inline]
    pub fn get_pk_bytes(&self, row: usize) -> &[u8] {
        let stride = self.strides[REG_PK] as usize;
        let off = self.offsets[REG_PK] + row * stride;
        &self.data[off..off + stride]
    }
    #[inline]
    pub fn get_weight(&self, row: usize) -> i64 {
        read_i64_le(&self.data[self.offsets[REG_WEIGHT]..], row * 8)
    }
    /// Overwrite a row's weight in place. Clears `consolidated`: the new
    /// weight may equal an adjacent row's element weight or zero.
    #[inline]
    pub fn set_weight(&mut self, row: usize, w: i64) {
        debug_assert!(
            row < self.count,
            "set_weight: row {} out of bounds ({})",
            row,
            self.count
        );
        let off = self.offsets[REG_WEIGHT] + row * 8;
        self.data[off..off + 8].copy_from_slice(&w.to_le_bytes());
        self.consolidated = false;
    }
    #[inline]
    pub fn get_null_word(&self, row: usize) -> u64 {
        read_u64_le(&self.data[self.offsets[REG_NULL_BMP]..], row * 8)
    }
    #[inline]
    pub fn get_col_ptr(&self, row: usize, payload_col: usize, col_size: usize) -> &[u8] {
        let off = self.offsets[REG_PAYLOAD_START + payload_col] + row * col_size;
        &self.data[off..off + col_size]
    }

    // ── Extend methods (building batches row-by-row) ────────────────────

    /// Ensure the data buffer has room for at least one more row.
    pub(crate) fn ensure_row_capacity(&mut self) {
        self.reserve_rows(1);
    }

    /// Ensure the data buffer has room for at least `n` more rows beyond `count`.
    #[allow(clippy::uninit_vec, clippy::needless_range_loop)]
    pub(crate) fn reserve_rows(&mut self, n: usize) {
        if self.count + n <= self.capacity as usize {
            return;
        }
        let nr = self.num_regions as usize;
        let new_cap = (self.capacity as usize * 2).max(8).max(self.count + n);
        let (new_offsets, new_total) = compute_offsets(&self.strides, nr, new_cap);

        if new_total > self.data.capacity() {
            // Out-of-place grow.  Vec::reserve on a too-small buffer triggers
            // realloc, which copies ALL old bytes to a new allocation (copy #1),
            // then copy_within would shift regions to new offsets (copy #2).
            // Bypass that by scatter-copying directly into a fresh buffer.
            let mut new_data = if new_total >= HUGEPAGE_THRESHOLD {
                // Zeroing is not needed: the scatter-copy loop below fills
                // every live byte, and all accessors are bounded by `count`.
                // We still want THP backing for large buffers.
                let mut v = Vec::with_capacity(new_total);
                unsafe {
                    v.set_len(new_total);
                }
                crate::foundation::posix_io::madvise_hugepage(v.as_ptr() as *mut u8, new_total);
                v
            } else {
                let mut buf = super::batch_pool::acquire_buf();
                if buf.capacity() >= new_total {
                    unsafe {
                        buf.set_len(new_total);
                    }
                    buf
                } else {
                    drop(buf); // evict the undersized buffer; pool converges to larger sizes
                    let mut v = Vec::with_capacity(new_total);
                    unsafe {
                        v.set_len(new_total);
                    }
                    v
                }
            };

            for i in 0..nr {
                let len = self.count * self.strides[i] as usize;
                if len == 0 {
                    continue;
                }
                let old_off = self.offsets[i];
                let new_off = new_offsets[i];
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        self.data.as_ptr().add(old_off),
                        new_data.as_mut_ptr().add(new_off),
                        len,
                    );
                }
            }

            let old_data = std::mem::replace(&mut self.data, new_data);
            super::batch_pool::recycle_buf(old_data);
        } else {
            // Vec already has sufficient capacity (e.g. cleared batch being
            // refilled).  copy_within shifts regions in one pass.
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
        self.capacity = new_cap as u32;
    }

    /// Write data into region `r` at the current row position.
    /// Auto-grows capacity if needed.  Strides must be set at construction —
    /// use `empty_with_schema`, `empty_joined`, or `with_schema`.
    #[inline]
    fn extend_region(&mut self, r: usize, src: &[u8]) {
        debug_assert_eq!(
            src.len(),
            self.strides[r] as usize,
            "extend_region: src len {} != stride {} for region {}",
            src.len(),
            self.strides[r],
            r
        );
        if self.count >= self.capacity as usize {
            self.ensure_row_capacity();
        }
        let off = self.offsets[r] + self.count * self.strides[r] as usize;
        self.data[off..off + src.len()].copy_from_slice(src);
    }

    #[inline]
    pub fn extend_weight(&mut self, d: &[u8]) {
        self.extend_region(REG_WEIGHT, d);
    }
    #[inline]
    pub fn extend_null_bmp(&mut self, d: &[u8]) {
        self.extend_region(REG_NULL_BMP, d);
    }
    #[inline]
    pub fn extend_col(&mut self, pi: usize, d: &[u8]) {
        self.extend_region(REG_PAYLOAD_START + pi, d);
    }

    /// Append a 128-bit primary key at the current row position.
    ///
    /// Narrow strides (1/2/4) write the low `stride` bytes of the `pk`
    /// argument verbatim. The contract is that the *caller* placed the
    /// on-disk LE bytes in the low `stride` bytes — `extract_pk_value`,
    /// `try_col_eq_literal`, and `try_extract_pk_in` all follow this rule
    /// (signed integers via `(v as iN as uN) as u128`, narrow unsigned via
    /// zero-extension). The high
    /// `16 - stride` bytes are dropped on truncation, which is correct
    /// under that contract for every PK type GnitzDB allows.
    /// Append a narrow PK from a `u128`, writing **right-aligned big-endian**
    /// bytes (the low `stride` bytes of `pk.to_be_bytes()`). For UNSIGNED PKs
    /// this is the correct OPK encoding (OPK == BE for unsigned), and
    /// `widen_pk_be(extend_pk(v))` round-trips. SIGNED or compound PKs are NOT
    /// sign-flipped here and must use `extend_pk_opk` / `extend_pk_bytes`.
    #[inline]
    pub fn extend_pk(&mut self, pk: u128) {
        let stride = self.strides[REG_PK] as usize;
        if stride > 16 {
            panic!("extend_pk: wide region; use extend_pk_bytes");
        }
        debug_assert!(
            stride >= 16 || (pk >> (stride * 8)) == 0,
            "narrow batch requires high bits == 0",
        );
        self.extend_region(REG_PK, &pk.to_be_bytes()[16 - stride..]);
    }

    #[inline]
    pub fn extend_pk_bytes(&mut self, bytes: &[u8]) {
        assert_eq!(
            bytes.len(),
            self.strides[REG_PK] as usize,
            "extend_pk_bytes: length must equal pk_stride",
        );
        self.extend_region(REG_PK, bytes);
    }

    /// Append a row's PK from native per-column values, OPK-encoding them
    /// (big-endian, with the sign-bit flip for signed columns) before the
    /// bytes are written. `native_col_vals` holds one native value per PK
    /// column in `pk_columns()` order. Use for signed or compound PK test
    /// tables where `extend_pk` (no sign flip) writes incorrect OPK bytes.
    #[cfg(test)]
    pub(crate) fn extend_pk_opk(&mut self, schema: &SchemaDescriptor, native_col_vals: &[u128]) {
        self.extend_pk_bytes(&crate::test_support::opk_pk(schema, native_col_vals));
    }

    /// Overwrite the narrow PK at `row` with a `u128`, writing right-aligned
    /// big-endian bytes — matching `extend_pk` / `widen_pk_be`. Unsigned-only
    /// (no sign flip); signed/compound callers use `set_pk_at_bytes`.
    /// Test-only: production reindex paths write OPK via `set_pk_at_bytes`.
    #[cfg(test)]
    #[inline]
    pub(crate) fn set_pk_at(&mut self, row: usize, pk: u128) {
        let stride = self.strides[REG_PK] as usize;
        if stride > 16 {
            panic!("set_pk_at: wide region; use set_pk_at_bytes");
        }
        debug_assert!(
            stride >= 16 || (pk >> (stride * 8)) == 0,
            "narrow batch requires high bits == 0",
        );
        let off = self.offsets[REG_PK] + row * stride;
        self.data[off..off + stride].copy_from_slice(&pk.to_be_bytes()[16 - stride..]);
    }

    #[inline]
    pub fn set_pk_at_bytes(&mut self, row: usize, bytes: &[u8]) {
        let stride = self.strides[REG_PK] as usize;
        debug_assert_eq!(bytes.len(), stride, "set_pk_at_bytes: length must equal pk_stride",);
        let off = self.offsets[REG_PK] + row * stride;
        self.data[off..off + stride].copy_from_slice(bytes);
    }

    /// Iterate PKs as `u128`. Test-only (the only caller is a batch round-trip
    /// test); production reads PK regions as OPK bytes via `get_pk_bytes`.
    #[cfg(test)]
    #[inline]
    pub(crate) fn pk_iter(&self) -> impl Iterator<Item = u128> + '_ {
        (0..self.count).map(|row| self.get_pk(row))
    }

    /// Fill `nbytes` of zeros at the current row position in a payload column.
    #[inline]
    pub fn fill_col_zero(&mut self, pi: usize, nbytes: usize) {
        let r = REG_PAYLOAD_START + pi;
        if self.count >= self.capacity as usize {
            self.ensure_row_capacity();
        }
        let off = self.offsets[r] + self.count * self.strides[r] as usize;
        self.data[off..off + nbytes].fill(0);
    }

    /// Bulk-copy a range of rows from `src_region_data` into region `r`.
    fn bulk_copy_region(&mut self, r: usize, src_region_data: &[u8], start: usize, end: usize) {
        let stride = self.strides[r] as usize;
        let n = end - start;
        let dst_off = self.offsets[r] + self.count * stride;
        let src_off = start * stride;
        self.data[dst_off..dst_off + n * stride].copy_from_slice(&src_region_data[src_off..src_off + n * stride]);
    }

    /// Bulk-copy rows `[start, end)` from a `MemBatch` into `self`.
    ///
    /// `weight_override`:
    /// - `Some(w)`: broadcast `w` into every copied row's weight column.
    /// - `None`: copy per-row weights verbatim from `src`.
    ///
    /// Non-STRING payload columns: one `copy_from_slice` per region.
    /// STRING payload columns: per-row blob relocation via `relocate_string_cell`.
    ///
    /// Preconditions:
    /// - `start <= end <= src.count`
    /// - `self` has the same schema (column count and strides) as `src`
    pub(crate) fn append_mem_batch_range(
        &mut self,
        src: &MemBatch<'_>,
        start: usize,
        end: usize,
        weight_override: Option<i64>,
    ) {
        assert!(start <= end, "append_mem_batch_range: start ({start}) > end ({end})");
        assert!(
            end <= src.count,
            "append_mem_batch_range: end ({end}) > src.count ({})",
            src.count
        );
        let n = end - start;
        if n == 0 {
            return;
        }
        self.reserve_rows(n);
        if !src.blob.is_empty() {
            self.blob.reserve(src.blob.len());
        }
        self.bulk_copy_region(REG_PK, src.pk(), start, end);
        match weight_override {
            Some(w) => {
                let dst_off = self.offsets[REG_WEIGHT] + self.count * 8;
                let w_bytes = w.to_le_bytes();
                let dest = &mut self.data[dst_off..dst_off + n * 8];
                for chunk in dest.chunks_exact_mut(8) {
                    chunk.copy_from_slice(&w_bytes);
                }
            }
            None => {
                self.bulk_copy_region(REG_WEIGHT, src.weight(), start, end);
            }
        }
        self.bulk_copy_region(REG_NULL_BMP, src.null_bmp(), start, end);
        let npc = self.num_payload_cols();
        let mut is_string_at = [false; MAX_BATCH_REGIONS];
        if let Some(s) = self.schema {
            for (pi, _ci, col) in s.payload_columns() {
                if pi >= npc {
                    break;
                }
                is_string_at[pi] = gnitz_wire::is_german_string(col.type_code);
            }
        }
        for (pi, &is_str) in is_string_at[..npc].iter().enumerate() {
            let cs = self.strides[REG_PAYLOAD_START + pi] as usize;
            if is_str && cs == 16 {
                for row in start..end {
                    let src_struct = src.get_col_ptr(row, pi, 16);
                    let dst_off = self.offsets[REG_PAYLOAD_START + pi] + (self.count + row - start) * 16;
                    relocate_string_cell(
                        src_struct,
                        src.blob,
                        &mut self.data[dst_off..dst_off + 16],
                        &mut self.blob,
                    );
                }
            } else if cs > 0 {
                self.bulk_copy_region(REG_PAYLOAD_START + pi, src.col_data(pi, cs), start, end);
            }
        }
        self.count += n;
    }

    // ── Lifecycle ───────────────────────────────────────────────────────

    /// Create a borrowed `MemBatch` view over this batch's data.
    ///
    /// Zero-allocation: copies the by-value `offsets` array and forwards
    /// references to `data` and `blob`.
    pub fn as_mem_batch(&self) -> MemBatch<'_> {
        MemBatch {
            data: &self.data,
            offsets: self.offsets,
            pk_stride: self.strides[REG_PK],
            blob: &self.blob,
            count: self.count,
        }
    }

    /// Returns `None` for unsorted batches; `count <= 1` is always sorted.
    pub(in crate::storage) fn as_sorted_mem_batch(&self) -> Option<merge::SortedMemBatch<'_>> {
        if self.sorted || self.count <= 1 {
            Some(merge::SortedMemBatch::new_unchecked(self.as_mem_batch()))
        } else {
            None
        }
    }

    /// Total bytes occupied by all buffers.
    pub fn total_bytes(&self) -> usize {
        let nr = self.num_regions as usize;
        let mut total = self.blob.len();
        for i in 0..nr {
            total += self.count * self.strides[i] as usize;
        }
        total
    }

    /// Scatter-copy selected rows from a MemBatch into a new Batch.
    pub fn from_indexed_rows(batch: &MemBatch, indices: &[u32], schema: &SchemaDescriptor) -> Self {
        if indices.is_empty() {
            return Self::empty_with_schema(schema);
        }
        let blob_cap = batch.blob.len().max(1);
        write_to_batch(schema, indices.len(), blob_cap, |writer| {
            super::scatter::scatter_copy(batch, indices, &[], writer);
        })
    }

    /// Clone all buffers into a new independent Batch.
    /// 2 allocations (data + blob) instead of N+7.
    #[allow(clippy::uninit_vec, clippy::needless_range_loop)]
    pub fn clone_batch(&self) -> Self {
        // Only clone the actually-used portion of data (count-based, not capacity-based).
        let nr = self.num_regions as usize;
        let (packed_offsets, packed_size) = compute_offsets(&self.strides, nr, self.count);
        let mut new_data = super::batch_pool::acquire_buf();
        new_data.clear();
        new_data.reserve(packed_size);
        // SAFETY: copy_nonoverlapping fills all `packed_size` bytes below;
        // src and dst are non-overlapping (different allocations).
        unsafe {
            new_data.set_len(packed_size);
            for i in 0..nr {
                let stride = self.strides[i] as usize;
                let len = self.count * stride;
                let src_off = self.offsets[i];
                let dst_off = packed_offsets[i];
                if len > 0 {
                    std::ptr::copy_nonoverlapping(
                        self.data.as_ptr().add(src_off),
                        new_data.as_mut_ptr().add(dst_off),
                        len,
                    );
                }
            }
        }
        let mut new_blob = super::batch_pool::acquire_buf();
        new_blob.clear();
        new_blob.extend_from_slice(&self.blob);
        Batch {
            data: new_data,
            blob: new_blob,
            offsets: packed_offsets,
            strides: self.strides,
            num_regions: self.num_regions,
            capacity: self.count as u32,
            count: self.count,
            sorted: self.sorted,
            consolidated: self.consolidated,
            schema: self.schema,
            blob_id: self.blob_id,
        }
    }

    /// Decompose into owned buffers (for pool recycling).
    /// Takes the buffers out, leaving zero-capacity vecs that Drop will ignore.
    pub(crate) fn into_buffers(mut self) -> (Vec<u8>, Vec<u8>) {
        let data = std::mem::take(&mut self.data);
        let blob = std::mem::take(&mut self.blob);
        (data, blob)
    }

    /// Binary search for the first row whose OPK bytes are `>= key`. After the
    /// OPK-at-rest flip this is a raw `memcmp` search with no schema dependency,
    /// correct for compound, signed, and wide (`pk_stride > 16`) PKs alike.
    ///
    /// `key` must be exactly `pk_stride` OPK bytes — identical width to the
    /// stored regions it is compared against.
    pub fn find_lower_bound_bytes(&self, key: &[u8]) -> usize {
        super::columnar::binary_lower_bound(0, self.count, key, &|i| self.get_pk_bytes(i))
    }

    /// Galloping forward lower bound seeded at `hint` (the caller's live
    /// position): `O(log gap)` when the boundary is just ahead, `O(1)` when it
    /// IS the hint, never worse than `find_lower_bound_bytes`. Used by the
    /// sorted-stream co-group merge, whose probe keys ascend, so the boundary
    /// only moves forward. `key` must be exactly `pk_stride` OPK bytes.
    pub fn advance_to(&self, key: &[u8], hint: usize) -> usize {
        super::columnar::gallop_lower_bound_bytes(self.count, key, hint, |i| self.get_pk_bytes(i))
    }

    /// Bulk-copy rows [start, end) from another Batch (same schema).
    ///
    /// `self` must have strides pre-set (see `empty_with_schema` / `with_schema`).
    pub fn append_batch(&mut self, src: &Batch, start: usize, end: usize) {
        let end = if end > src.count { src.count } else { end };
        if start >= end {
            return;
        }
        self.sorted = false;
        self.consolidated = false;
        self.append_rows_inner(src, start, end, false);
    }

    /// Bulk-copy rows with negated weights.
    pub fn append_batch_negated(&mut self, src: &Batch, start: usize, end: usize) {
        let end = if end > src.count { src.count } else { end };
        if start >= end {
            return;
        }
        self.sorted = false;
        self.consolidated = false;
        self.append_rows_inner(src, start, end, true);
    }

    /// Bulk-copy rows [start, end) verbatim — no string blob relocation.
    ///
    /// # Precondition
    /// `self.blob` must already equal `src.blob` (e.g. assigned with
    /// `self.blob = src.blob.clone()` before the first call).  The 16-byte
    /// German String structs are copied as-is; their heap offsets remain valid
    /// because both batches share the same blob content.
    pub fn append_batch_no_blob_reloc(&mut self, src: &Batch, start: usize, end: usize) {
        let end = end.min(src.count);
        if start >= end {
            return;
        }
        // Guard for the precondition: both batches must share the same blob identity,
        // established by calling `share_blob_from` (or `clone_batch`) before this.
        // Matching IDs guarantees identical blob content; same-length-different-content
        // blobs would produce dangling heap offsets in the copied German String structs.
        debug_assert_eq!(
            self.blob_id, src.blob_id,
            "append_batch_no_blob_reloc: blobs must be identical; \
             call share_blob_from(&src) before appending (self.blob_id={}, src.blob_id={})",
            self.blob_id, src.blob_id,
        );
        let n = end - start;
        self.reserve_rows(n);
        // All three system regions
        self.bulk_copy_region(REG_PK, &src.data[src.offsets[REG_PK]..], start, end);
        self.bulk_copy_region(REG_WEIGHT, &src.data[src.offsets[REG_WEIGHT]..], start, end);
        self.bulk_copy_region(REG_NULL_BMP, &src.data[src.offsets[REG_NULL_BMP]..], start, end);
        // Payload columns — string structs copied verbatim (blob already shared)
        let npc = self.num_payload_cols();
        for pi in 0..npc {
            if self.strides[REG_PAYLOAD_START + pi] > 0 {
                self.bulk_copy_region(
                    REG_PAYLOAD_START + pi,
                    &src.data[src.offsets[REG_PAYLOAD_START + pi]..],
                    start,
                    end,
                );
            }
        }
        self.count += n;
        self.sorted = false;
        self.consolidated = false;
    }

    #[allow(clippy::needless_range_loop)]
    fn append_rows_inner(&mut self, src: &Batch, start: usize, end: usize, negate: bool) {
        let n = end - start;
        self.reserve_rows(n);

        // Fixed columns: bulk copy from src's data buffer.
        self.bulk_copy_region(REG_PK, &src.data[src.offsets[REG_PK]..], start, end);
        if negate {
            let w_off = self.offsets[REG_WEIGHT];
            for i in start..end {
                let dst = w_off + (self.count + i - start) * 8;
                self.data[dst..dst + 8].copy_from_slice(&(-src.get_weight(i)).to_le_bytes());
            }
        } else {
            self.bulk_copy_region(REG_WEIGHT, &src.data[src.offsets[REG_WEIGHT]..], start, end);
        }
        self.bulk_copy_region(REG_NULL_BMP, &src.data[src.offsets[REG_NULL_BMP]..], start, end);

        // Payload columns.  When the schema is installed, we need to know
        // which payload positions hold STRING values so the blob can be
        // relocated; without a schema, treat all payload columns as opaque
        // bytes (no blob relocation is possible without type info anyway).
        //
        // `is_string_at[pi]` is true iff payload column `pi` is a STRING.
        let npc = self.num_payload_cols();
        let mut is_string_at = [false; MAX_BATCH_REGIONS];
        if let Some(s) = self.schema {
            for (pi, _ci, col) in s.payload_columns() {
                if pi >= npc {
                    break;
                }
                is_string_at[pi] = gnitz_wire::is_german_string(col.type_code);
            }
        }

        for pi in 0..npc {
            let cs = self.strides[REG_PAYLOAD_START + pi] as usize;
            if is_string_at[pi] && cs == 16 {
                for row in start..end {
                    let src_off = src.offsets[REG_PAYLOAD_START + pi] + row * 16;
                    let src_struct = &src.data[src_off..src_off + 16];
                    let dst_off = self.offsets[REG_PAYLOAD_START + pi] + (self.count + row - start) * 16;
                    relocate_string_cell(
                        src_struct,
                        &src.blob,
                        &mut self.data[dst_off..dst_off + 16],
                        &mut self.blob,
                    );
                }
            } else if cs > 0 {
                self.bulk_copy_region(
                    REG_PAYLOAD_START + pi,
                    &src.data[src.offsets[REG_PAYLOAD_START + pi]..],
                    start,
                    end,
                );
            }
        }

        self.count += n;
        self.sorted = false;
        self.consolidated = false;
    }

    /// Append a single row from raw C-style region pointers.
    ///
    /// # Safety
    /// `col_ptrs[i]` must point to at least `col_sizes[i]` readable bytes for
    /// every non-null, non-STRING column.  For STRING columns the pointer must
    /// point to a 16-byte German String struct.  `blob_src` must contain the
    /// blob bytes referenced by any long-string structs.
    #[cfg(test)]
    pub unsafe fn append_row(
        &mut self,
        pk: u128,
        weight: i64,
        null_word: u64,
        col_ptrs: &[*const u8],
        col_sizes: &[u32],
        blob_src: &[u8],
    ) {
        // extend_pk writes right-aligned BE (correct OPK only for unsigned PKs).
        // A signed single-col PK needs the sign flip — use extend_pk_opk.
        debug_assert!(
            self.schema.is_none_or(|s| !s.pk_is_signed_single_col()),
            "append_row: signed single-col PK requires extend_pk_bytes (extend_pk \
             writes right-aligned BE without the sign flip)",
        );
        self.ensure_row_capacity();
        self.extend_pk(pk);
        self.extend_weight(&weight.to_le_bytes());
        self.extend_null_bmp(&null_word.to_le_bytes());

        let schema = self.schema;

        // `pi` is the dense payload index; it equals `enumerate`'s counter.
        for (pi, (ptr, &sz)) in col_ptrs.iter().zip(col_sizes.iter()).enumerate() {
            let ci = schema.map_or(pi, |s| s.payload_col_idx(pi));

            let is_string =
                schema.is_some_and(|s| ci < s.num_columns() && gnitz_wire::is_german_string(s.columns[ci].type_code));
            let is_null = (null_word >> pi) & 1 != 0;
            let col_size = sz as usize;

            if is_null {
                self.fill_col_zero(pi, col_size);
            } else if is_string && col_size == 16 {
                let src = std::slice::from_raw_parts(*ptr, 16);
                let dest = crate::schema::relocate_german_string_vec(src, blob_src, &mut self.blob, None);
                self.extend_col(pi, &dest);
            } else {
                let src = std::slice::from_raw_parts(*ptr, col_size);
                self.extend_col(pi, src);
            }
        }

        self.count += 1;
        self.sorted = false;
        self.consolidated = false;
    }

    /// Append a row from RowBuilder-style value arrays.
    ///
    /// # Safety
    /// For STRING columns, `str_ptrs[i]` must be valid for `str_lens[i]` bytes.
    #[cfg(test)]
    #[allow(clippy::too_many_arguments)]
    pub unsafe fn append_row_simple(
        &mut self,
        pk: u128,
        weight: i64,
        null_word: u64,
        lo_values: &[i64],
        hi_values: &[u64],
        str_ptrs: &[*const u8],
        str_lens: &[u32],
    ) {
        self.ensure_row_capacity();
        self.extend_pk(pk);
        self.extend_weight(&weight.to_le_bytes());
        self.extend_null_bmp(&null_word.to_le_bytes());

        let schema = self.schema.expect("append_row_simple requires schema");

        for (pi, _ci, col) in schema.payload_columns() {
            let col_size = col.size() as usize;
            let is_null = (null_word >> pi) & 1 != 0;

            if is_null {
                self.fill_col_zero(pi, col_size);
            } else {
                match col.type_code {
                    crate::schema::type_code::STRING | crate::schema::type_code::BLOB => {
                        let ptr = str_ptrs[pi];
                        let slen = str_lens[pi] as usize;
                        let bytes: &[u8] = if ptr.is_null() || slen == 0 {
                            &[]
                        } else {
                            std::slice::from_raw_parts(ptr, slen)
                        };
                        let gs = crate::schema::encode_german_string(bytes, &mut self.blob);
                        self.extend_col(pi, &gs);
                    }
                    crate::schema::type_code::F64 => {
                        self.extend_col(pi, &lo_values[pi].to_le_bytes());
                    }
                    crate::schema::type_code::F32 => {
                        let f64_val = f64::from_bits(lo_values[pi] as u64);
                        let f32_val = f64_val as f32;
                        self.extend_col(pi, &f32_val.to_le_bytes());
                    }
                    crate::schema::type_code::U128 | crate::schema::type_code::I128 => {
                        let mut bytes = [0u8; 16];
                        bytes[..8].copy_from_slice(&(lo_values[pi] as u64).to_le_bytes());
                        bytes[8..].copy_from_slice(&hi_values[pi].to_le_bytes());
                        self.extend_col(pi, &bytes);
                    }
                    _ => {
                        let bytes = lo_values[pi].to_le_bytes();
                        self.extend_col(pi, &bytes[..col_size]);
                    }
                }
            }
        }

        self.count += 1;
        self.sorted = false;
        self.consolidated = false;
    }

    /// Reset to empty without freeing buffer allocations.
    pub fn clear(&mut self) {
        self.count = 0;
        self.blob.clear();
        self.sorted = true;
        self.consolidated = true;
        self.blob_id = next_blob_id();
        // data buffer stays allocated — capacity and offsets remain valid.
    }

    /// Copy blob content from `src` and record that this batch shares `src`'s
    /// blob identity.  Must be called before `append_batch_no_blob_reloc`.
    pub fn share_blob_from(&mut self, src: &Batch) {
        // Reuse the pooled destination buffer rather than dropping it for a
        // fresh exact-sized clone (an allocation per call). The blob bytes are
        // identical, so the shared blob_id and every German-string offset stay
        // valid — a behavioral no-op apart from the saved allocation.
        self.blob.clear();
        self.blob.extend_from_slice(&src.blob);
        self.blob_id = src.blob_id;
    }

    /// Number of regions in the standard layout (including blob).
    pub fn num_regions_total(&self) -> usize {
        self.num_regions as usize + 1
    }

    /// Get region pointer by index.
    pub fn region_ptr(&self, idx: usize) -> *const u8 {
        let blob_idx = REG_PAYLOAD_START + self.num_payload_cols();
        if idx < blob_idx {
            self.data[self.offsets[idx]..].as_ptr()
        } else if idx == blob_idx {
            self.blob.as_ptr()
        } else {
            panic!(
                "region_ptr: index {idx} out of range (num_regions_total = {})",
                blob_idx + 1
            );
        }
    }

    /// Get region size by index.
    pub fn region_size(&self, idx: usize) -> usize {
        let blob_idx = REG_PAYLOAD_START + self.num_payload_cols();
        if idx < blob_idx {
            self.count * self.strides[idx] as usize
        } else {
            self.blob.len()
        }
    }

    /// Per-row byte stride of a fixed/payload region. Used by the range-wire
    /// encoders in `batch_wire`, which size regions for an arbitrary row count
    /// rather than `self.count` (so `region_size` does not fit).
    pub(super) fn region_stride(&self, idx: usize) -> u8 {
        self.strides[idx]
    }

    pub fn regions(&self) -> Vec<(*const u8, usize)> {
        let npc = self.num_payload_cols();
        let mut r = Vec::with_capacity(REG_PAYLOAD_START + npc + 1);
        for i in 0..REG_PAYLOAD_START + npc {
            let off = self.offsets[i];
            let len = self.count * self.strides[i] as usize;
            r.push((self.data[off..].as_ptr(), len));
        }
        r.push((self.blob.as_ptr(), self.blob.len()));
        r
    }

    /// Append a single row from any ColumnarSource with blob deduplication.
    ///
    /// Pass `None` for `blob_cache` when the schema has no STRING columns or
    /// when cross-row dedup isn't worth the bookkeeping. Pass `Some(...)` to
    /// dedup repeated source long-string spans into a single destination copy.
    ///
    /// Takes a **native** `u128` PK. The FK check-batch filter now keys on
    /// verbatim OPK bytes (`append_row_from_source_bytes`), so this native entry
    /// point has no production caller and is retained only for unit tests.
    #[cfg(test)]
    pub(crate) fn append_row_from_source<S: ColumnarSource>(
        &mut self,
        key: u128,
        weight: i64,
        source: &S,
        row: usize,
        blob_cache: Option<&mut BlobCache>,
    ) {
        if weight == 0 {
            return;
        }
        self.ensure_row_capacity();
        self.extend_pk(key);
        self.append_row_tail_from_source(weight, source, row, blob_cache);
    }

    /// Raw-PK-bytes counterpart of [`append_row_from_source`], for wide
    /// (`pk_stride > 16`) PKs where `extend_pk` would panic. `pk_bytes` must be
    /// exactly `pk_stride` bytes (asserted by `extend_pk_bytes`). Also valid for
    /// narrow PKs — the only difference from the `u128` entry point is how the
    /// PK region is written.
    pub fn append_row_from_source_bytes<S: ColumnarSource>(
        &mut self,
        pk_bytes: &[u8],
        weight: i64,
        source: &S,
        row: usize,
        blob_cache: Option<&mut BlobCache>,
    ) {
        if weight == 0 {
            return;
        }
        self.ensure_row_capacity();
        self.extend_pk_bytes(pk_bytes);
        self.append_row_tail_from_source(weight, source, row, blob_cache);
    }

    /// Shared tail of the two `append_row_from_source*` entry points: writes
    /// weight, null bitmap, and the relocated payload columns, then bumps
    /// `count`. The caller must have already written the PK region. `#[inline]`
    /// so the compaction/merge emit loop pays nothing for the extraction.
    #[inline]
    fn append_row_tail_from_source<S: ColumnarSource>(
        &mut self,
        weight: i64,
        source: &S,
        row: usize,
        mut blob_cache: Option<&mut BlobCache>,
    ) {
        let schema = self.schema.expect("append_row_from_source requires schema");

        self.extend_weight(&weight.to_le_bytes());
        let null_word = source.get_null_word(row);
        self.extend_null_bmp(&null_word.to_le_bytes());

        let src_blob = source.blob_slice();
        for (pi, _ci, col) in schema.payload_columns() {
            let cs = col.size() as usize;
            let is_null = (null_word >> pi) & 1 != 0;

            if is_null {
                self.fill_col_zero(pi, cs);
            } else if gnitz_wire::is_german_string(col.type_code) {
                let src_struct = source.get_col_ptr(row, pi, cs);
                let dest = crate::schema::relocate_german_string_vec(
                    src_struct,
                    src_blob,
                    &mut self.blob,
                    blob_cache.as_deref_mut(),
                );
                self.extend_col(pi, &dest);
            } else {
                self.extend_col(pi, source.get_col_ptr(row, pi, cs));
            }
        }

        self.count += 1;
        self.sorted = false;
        self.consolidated = false;
    }

    /// Consume this batch, consolidating it if needed.
    ///
    /// Fast path: if already consolidated or empty, wraps `self` in a
    /// `ConsolidatedBatch` without any allocation (free move).
    /// Slow path: sorts and weight-folds into a fresh batch, then drops `self`.
    pub fn into_consolidated(self, schema: &SchemaDescriptor) -> ConsolidatedBatch {
        if self.consolidated || self.count == 0 {
            return ConsolidatedBatch(self);
        }
        let already_sorted = self.sorted;
        let mb = self.as_mem_batch();
        let blob_cap = mb.blob.len().max(1);
        let mut result = write_to_batch(schema, self.count, blob_cap, |writer| {
            if already_sorted {
                merge::fold_sorted(&mb, schema, writer);
            } else {
                merge::sort_and_consolidate(&mb, schema, writer);
            }
        });
        result.sorted = true;
        result.consolidated = true;
        ConsolidatedBatch(result)
    }

    /// Consolidate a borrowed batch if needed. Returns `None` when the batch is already
    /// consolidated or empty (caller borrows the original). Returns `Some(ConsolidatedBatch)`
    /// when a new consolidated batch was allocated (caller borrows that instead).
    ///
    /// Idiomatic usage:
    /// ```ignore
    /// let cs = Batch::consolidate_if_needed(delta, schema);
    /// let c: &Batch = cs.as_deref().unwrap_or(delta);
    /// ```
    pub fn consolidate_if_needed(batch: &Batch, schema: &SchemaDescriptor) -> Option<ConsolidatedBatch> {
        if batch.consolidated || batch.count == 0 {
            return None;
        }
        let mb = batch.as_mem_batch();
        let blob_cap = mb.blob.len().max(1);
        let mut result = write_to_batch(schema, batch.count, blob_cap, |writer| {
            merge::sort_and_consolidate(&mb, schema, writer);
        });
        result.sorted = true;
        result.consolidated = true;
        Some(ConsolidatedBatch::new_unchecked(result))
    }

    #[cfg(test)]
    pub(crate) fn data_capacity(&self) -> usize {
        self.data.capacity()
    }

    pub fn mark_sorted(&mut self) {
        self.sorted = true;
    }
    pub fn mark_consolidated(&mut self) {
        self.consolidated = true;
    }
}

impl Drop for Batch {
    fn drop(&mut self) {
        super::batch_pool::recycle_buf(std::mem::take(&mut self.data));
        super::batch_pool::recycle_buf(std::mem::take(&mut self.blob));
    }
}

impl Clone for Batch {
    fn clone(&self) -> Self {
        self.clone_batch()
    }
}

impl ColumnarSource for Batch {
    #[inline]
    fn get_null_word(&self, row: usize) -> u64 {
        Batch::get_null_word(self, row)
    }
    #[inline]
    fn get_col_ptr(&self, row: usize, payload_col: usize, col_size: usize) -> &[u8] {
        Batch::get_col_ptr(self, row, payload_col, col_size)
    }
    #[inline]
    fn blob_slice(&self) -> &[u8] {
        &self.blob
    }
}

/// Owned `Batch` certified to be consolidated (sorted, no duplicate PKs, weights folded).
///
/// Obtain via `Batch::into_consolidated` (checked) or `new_unchecked` (caller asserts).
#[repr(transparent)]
pub struct ConsolidatedBatch(Batch);

impl ConsolidatedBatch {
    pub(crate) fn new_unchecked(batch: Batch) -> Self {
        debug_assert!(batch.sorted || batch.count == 0, "ConsolidatedBatch must be sorted");
        debug_assert!(
            batch.consolidated || batch.count == 0,
            "ConsolidatedBatch must be consolidated"
        );
        ConsolidatedBatch(batch)
    }
    pub fn into_inner(self) -> Batch {
        self.0
    }
    /// Reinterpret `&Batch` as `&ConsolidatedBatch` when `batch.consolidated` is set.
    /// Empty batches are always considered consolidated.
    // SAFETY: ConsolidatedBatch is #[repr(transparent)] over Batch.
    pub(crate) fn from_batch_ref(batch: &Batch) -> Option<&Self> {
        (batch.consolidated || batch.count == 0)
            .then(|| unsafe { &*(batch as *const Batch as *const ConsolidatedBatch) })
    }
}

impl std::ops::Deref for ConsolidatedBatch {
    type Target = Batch;
    fn deref(&self) -> &Batch {
        &self.0
    }
}

/// Allocate a single contiguous arena, run a merge/copy operation via
/// DirectWriter, and return the arena as a Batch — zero copy-out.
///
/// In steady state the arena is recycled from the thread-local buffer pool,
/// so this path allocates nothing.
pub fn write_to_batch(
    schema: &SchemaDescriptor,
    max_rows: usize,
    max_blob: usize,
    write_fn: impl FnOnce(&mut merge::DirectWriter),
) -> Batch {
    let (strides, nr) = strides_from_schema(schema);
    let nr = nr as usize;

    // Arena layout: [pk | weight | null | col_0 | ... | col_{N-1}]
    // Sized for max_rows; blob is separate.
    let (offsets, arena_size) = compute_offsets(&strides, nr, max_rows);

    // Match `with_schema`: a too-small pooled buffer is evicted rather than
    // grown in place — `Vec::reserve` would copy old bytes forward before the
    // tail is zeroed, slower than a fresh zeroed allocation.
    let mut data = {
        let mut buf = super::batch_pool::acquire_buf();
        if buf.capacity() >= arena_size {
            buf.resize(arena_size, 0);
            buf
        } else {
            drop(buf); // evict the undersized buffer; pool converges to larger sizes
            alloc_large_zeroed(arena_size)
        }
    };
    // DirectWriter grows blob length via `extend_from_slice`; reserve capacity
    // up front but do not zero-fill.
    let mut blob = {
        let mut buf = super::batch_pool::acquire_buf();
        buf.clear();
        buf.reserve(max_blob);
        if max_blob >= HUGEPAGE_THRESHOLD {
            crate::foundation::posix_io::madvise_hugepage(buf.as_mut_ptr(), buf.capacity());
        }
        buf
    };

    let actual_rows;
    {
        let (pk, weight, null_bmp, col_slices) = carve_writer_slices(&mut data, schema, max_rows);
        let mut writer = merge::DirectWriter::new(pk, weight, null_bmp, col_slices, &mut blob, *schema, max_rows);
        write_fn(&mut writer);
        actual_rows = writer.row_count();
    }

    Batch {
        data,
        blob,
        offsets,
        strides,
        num_regions: nr as u8,
        blob_id: next_blob_id(),
        capacity: max_rows as u32,
        count: actual_rows,
        sorted: false,
        consolidated: false,
        schema: Some(*schema),
    }
}

// ---------------------------------------------------------------------------
// BatchBuilder — construct Batch rows for system table mutations
//
// A pure storage utility: it holds no catalog state and builds a `Batch`
// row-by-row from a schema, so it lives here with `Batch`. Re-exported from
// `catalog` for its DDL/bootstrap/store callers; `runtime::executor` and the
// `compiler` tests import it from `storage` directly.
// ---------------------------------------------------------------------------

/// Lightweight row-by-row builder for constructing Batch in Rust.
/// Operates on Batch directly; the schema lives on the batch itself.
pub(crate) struct BatchBuilder {
    pub(crate) batch: Batch,
    // per-row state
    pub(crate) curr_null_word: u64,
    pub(crate) curr_col: usize,
}

impl BatchBuilder {
    pub(crate) fn new(schema: SchemaDescriptor) -> Self {
        BatchBuilder {
            batch: Batch::with_schema(schema, 8),
            curr_null_word: 0,
            curr_col: 0,
        }
    }

    /// Begin a new row with the given PK and weight.
    pub(crate) fn begin_row(&mut self, pk: u128, weight: i64) {
        self.batch.ensure_row_capacity();
        self.batch.extend_pk(pk);
        self.batch.extend_weight(&weight.to_le_bytes());
        self.curr_null_word = 0;
        self.curr_col = 0;
    }

    /// Put a u64 value for the current payload column.
    pub(crate) fn put_u64(&mut self, val: u64) {
        self.batch.extend_col(self.curr_col, &val.to_le_bytes());
        self.curr_col += 1;
    }

    /// Put a string value for the current payload column.
    pub(crate) fn put_string(&mut self, s: &str) {
        let st = crate::schema::encode_german_string(s.as_bytes(), &mut self.batch.blob);
        self.batch.extend_col(self.curr_col, &st);
        self.curr_col += 1;
    }

    // The non-u64/string put variants are exercised only by the catalog tests
    // (production system-table rows are u64/string-shaped); `#[cfg(test)]`
    // keeps them out of production builds, mirroring `ddl.rs::create_table`.

    /// Put a u128 value for the current payload column.
    #[cfg(test)]
    pub(crate) fn put_u128(&mut self, val: u128) {
        self.batch.extend_col(self.curr_col, &val.to_le_bytes());
        self.curr_col += 1;
    }

    /// Put a u8 value for the current payload column.
    #[cfg(test)]
    pub(crate) fn put_u8(&mut self, val: u8) {
        self.batch.extend_col(self.curr_col, &[val]);
        self.curr_col += 1;
    }

    /// Put a u16 value for the current payload column.
    #[cfg(test)]
    pub(crate) fn put_u16(&mut self, val: u16) {
        self.batch.extend_col(self.curr_col, &val.to_le_bytes());
        self.curr_col += 1;
    }

    /// Put a u32 value for the current payload column.
    #[cfg(test)]
    pub(crate) fn put_u32(&mut self, val: u32) {
        self.batch.extend_col(self.curr_col, &val.to_le_bytes());
        self.curr_col += 1;
    }

    /// Put a NULL value for the current payload column.
    #[cfg(test)]
    pub(crate) fn put_null(&mut self) {
        let col_size = self.schema().columns[self.physical_col_idx()].size() as usize;
        self.batch.fill_col_zero(self.curr_col, col_size);
        self.curr_null_word |= 1u64 << self.curr_col;
        self.curr_col += 1;
    }

    /// Finish the current row (writes null bitmap).
    pub(crate) fn end_row(&mut self) {
        self.batch.extend_null_bmp(&self.curr_null_word.to_le_bytes());
        self.batch.count += 1;
        self.batch.sorted = false;
        self.batch.consolidated = false;
    }

    /// Convenience: begin + put columns + end for a simple row.
    pub(crate) fn finish(self) -> Batch {
        self.batch
    }

    #[cfg(test)]
    fn schema(&self) -> &SchemaDescriptor {
        self.batch
            .schema
            .as_ref()
            .expect("BatchBuilder batch always carries a schema")
    }

    #[cfg(test)]
    fn physical_col_idx(&self) -> usize {
        self.schema().payload_col_idx(self.curr_col)
    }
}

// ---------------------------------------------------------------------------
// Schema-shaping free functions
//
// Built purely from a `SchemaDescriptor` (no catalog state). Re-exported from
// `catalog` so its callers compile unchanged; the runtime gather/preflight
// paths also build the same descriptors.
// ---------------------------------------------------------------------------

/// Build a compound-PK index schema for a secondary index on `source_cols`
/// of `source`, validating the column list along the way.
///
/// Layout: `(promoted_c0, promoted_c1, …, src_pk_0, src_pk_1, …)` — every
/// indexed column promoted independently and packed in declared order, then the
/// source PK columns, all in the PK with zero payload columns. The leading
/// indexed-key region is `Σ promoted widths`; `seek_by_index` prefix-scans it
/// (full or leading-prefix), then reads the source PK bytes directly out of the
/// index PK suffix. The 1-element list is the single-column index.
///
/// Bounds-checks every column, promotes it (rejecting STRING/BLOB/float), and
/// validates the index-schema PK limits — all **before** calling
/// `SchemaDescriptor::new`: that constructor is a `const fn` whose `assert!`s
/// fire in release and abort the master. An over-limit schema is reachable only
/// for a *composite* index (a single-column index — including every FK
/// auto-index — always fits, since `PK_LIST_MAX_COLS < MAX_PK_COLUMNS` reserves
/// the prefix slot), via a raw `gnitz-core` client or a crafted/over-range
/// persisted row replayed at boot, neither of which goes through the SQL
/// planner's pre-check. Validating here converts the abort into a clean ingest
/// `Err` for every path (defence in depth at the catalog trust boundary).
pub(crate) fn make_index_schema(source_cols: &[u32], source: &SchemaDescriptor) -> Result<SchemaDescriptor, String> {
    let mut col_types: Vec<u8> = Vec::with_capacity(source_cols.len());
    for &c in source_cols {
        if c as usize >= source.num_columns() {
            return Err(format!(
                "Index: column index {} out of bounds (columns={})",
                c,
                source.num_columns()
            ));
        }
        col_types.push(source.columns[c as usize].type_code);
    }
    let src_pk = source.pk_indices();
    // Shared with the SQL planner's CREATE INDEX pre-check, so the promotion
    // rule and the arity/stride limits can never disagree across the layers.
    let promoted = gnitz_wire::index_key_types(&col_types, src_pk.len(), source.pk_stride() as usize)?;
    let n = promoted.len();
    let arity = n + src_pk.len();
    let mut cols: Vec<SchemaColumn> = Vec::with_capacity(arity);
    let mut pk_indices: Vec<u32> = Vec::with_capacity(arity);
    for (i, &t) in promoted.iter().enumerate() {
        cols.push(SchemaColumn::new(t, 0));
        pk_indices.push(i as u32);
    }
    for (j, &ci) in src_pk.iter().enumerate() {
        cols.push(SchemaColumn::new(source.columns[ci as usize].type_code, 0));
        pk_indices.push((n + j) as u32);
    }
    Ok(SchemaDescriptor::new(&cols, &pk_indices))
}

/// Wire schema for the GET_INDICES descriptor list: `(packed_cols PK, is_unique)`.
/// The PK carries `pack_pk_cols(&col_indices)` — unique per circuit (circuits
/// dedup by column list), so a valid PK. The server ships this block on the data
/// path; the client decodes against the wire schema and reads columns by position.
pub(crate) fn index_meta_schema_desc() -> SchemaDescriptor {
    let u64c = SchemaColumn::new(type_code::U64, 0);
    SchemaDescriptor::new(&[u64c, u64c], &[0]) // [packed_cols (PK), is_unique]
}
pub(crate) const INDEX_META_COL_NAMES: [&[u8]; 2] = [b"cols", b"is_unique"];

/// Build the schema for a `gather_family` result: the PK columns of `schema`
/// (in pk-list order, so the packed PK round-trips identically) followed by
/// the projected columns in `project` order as payload. `project` must list
/// only non-PK columns (PK members are resolved from the packed PK without a
/// gather); a projected PK column would be emitted twice.
///
/// `pub(crate)`: the master's gather drain builds the same descriptor as the
/// expected reply schema, so a projected reply with the wrong shape errors
/// instead of mis-decoding.
pub(crate) fn project_schema(schema: &SchemaDescriptor, project: &[u8]) -> SchemaDescriptor {
    let mut cols: Vec<SchemaColumn> = Vec::with_capacity(schema.pk_indices().len() + project.len());
    let mut pk_idx: Vec<u32> = Vec::with_capacity(schema.pk_indices().len());
    for (_, _, col) in schema.pk_columns() {
        pk_idx.push(cols.len() as u32);
        cols.push(*col);
    }
    for &p in project {
        debug_assert!(
            !schema.is_pk_col(p as usize),
            "project_schema: projected column {p} is a PK column"
        );
        cols.push(schema.columns[p as usize]);
    }
    SchemaDescriptor::new(&cols, &pk_idx)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{type_code, SchemaColumn, SchemaDescriptor};
    use crate::test_support::wide_pk_3xu64_schema;

    fn single_col_pk_schema(tc: u8) -> SchemaDescriptor {
        SchemaDescriptor::new(&[SchemaColumn::new(tc, 0), SchemaColumn::new(type_code::I64, 0)], &[0])
    }

    // Compound-PK regression guard for the precomputed payload→logical
    // mapping. 4-column schema with pk_indices=[1, 2]: payload slot 0
    // must map to logical column 0, slot 1 to logical column 3 — never
    // 0 and 1 (the bug the previous single-PK reimplementation would
    // have introduced for any non-leading compound PK).
    #[test]
    fn batch_builder_physical_col_idx_compound_pk() {
        let cols = [
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U64, 0),
        ];
        let schema = SchemaDescriptor::new(&cols, &[1, 2]);
        let mut bb = BatchBuilder::new(schema);
        assert_eq!(bb.curr_col, 0);
        assert_eq!(bb.physical_col_idx(), 0);
        bb.curr_col = 1;
        assert_eq!(bb.physical_col_idx(), 3);
    }

    #[test]
    fn widen_pk_be_recovers_unsigned_value() {
        // OPK bytes of an unsigned PK are its big-endian image; widen_pk_be
        // right-aligns them and recovers the native value. A left-align bug
        // would return value·2^k — assert it does not.
        let v: u64 = 0x1122_3344_5566_7788;
        assert_eq!(gnitz_wire::widen_pk_be(&v.to_be_bytes(), 8), v as u128);

        let key = 0x0102_0304_0506_0708_090A_0B0C_0D0E_0F10u128;
        assert_eq!(gnitz_wire::widen_pk_be(&key.to_be_bytes(), 16), key);

        for &v in &[1u64, 5, 42] {
            assert_eq!(gnitz_wire::widen_pk_be(&v.to_be_bytes(), 8), v as u128);
        }
        for &v in &[0u32, 0xFFFF_FFFE, 0xFFFF_FFFF] {
            assert_eq!(gnitz_wire::widen_pk_be(&v.to_be_bytes(), 4), v as u128);
        }
        // Narrow non-power-of-two stride: right-aligned recovery.
        let mut opk12 = [0u8; 12];
        opk12[11] = 0x2A; // value 42 in the low byte of a 12-byte OPK region
        assert_eq!(gnitz_wire::widen_pk_be(&opk12, 12), 42u128);
    }

    #[test]
    fn widen_pk_be_extend_pk_round_trips() {
        // extend_pk writes right-aligned BE; get_pk_bytes is the stored OPK and
        // get_pk recovers the value. extend_pk(widen_pk_be(bytes)) == bytes.
        let schema = single_col_pk_schema(type_code::U64);
        let mut b = Batch::empty_with_schema(&schema);
        b.reserve_rows(1);
        b.extend_pk(42);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &0i64.to_le_bytes());
        b.count += 1;
        assert_eq!(b.get_pk_bytes(0), &42u64.to_be_bytes());
        assert_eq!(b.get_pk(0), 42u128);
    }

    #[test]
    fn extend_pk_narrow_strides_round_trip() {
        // For each narrow stride, extend_pk writes the low `stride` bytes of
        // the u128 argument verbatim; get_pk reads them back via widen_pk_le.
        // The (type, value, expected u128) triples mirror what
        // extract_pk_value writes for the corresponding PK type.
        let cases: &[(u8, u128)] = &[
            // I8 PK = -1: extract_pk_value writes (-1i8 as u8) as u128 = 0xFF
            (type_code::I8, 0xFFu128),
            // U8 PK = 200
            (type_code::U8, 200u128),
            // I16 PK = -1: low 2 bytes = 0xFFFF
            (type_code::I16, 0xFFFFu128),
            // U16 PK = u16::MAX
            (type_code::U16, u16::MAX as u128),
            // I32 PK = -1: low 4 bytes = 0xFFFF_FFFF
            (type_code::I32, 0xFFFF_FFFFu128),
            // U32 PK = u32::MAX
            (type_code::U32, u32::MAX as u128),
        ];
        for &(tc, pk) in cases {
            let schema = single_col_pk_schema(tc);
            let mut b = Batch::empty_with_schema(&schema);
            b.reserve_rows(1);
            b.extend_pk(pk);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &0i64.to_le_bytes());
            b.count += 1;
            assert_eq!(b.get_pk(0), pk, "type_code {tc} narrow-stride round-trip");
        }
    }

    #[test]
    fn write_to_batch_narrow_pk_odd_rowcount_round_trips() {
        // Regression: `carve_writer_slices` (the writer) must carve at the same
        // 8-byte-aligned region offsets `compute_offsets` (the reader) produces.
        // At an odd row count a narrow `pk_stride` makes `rows * pk_stride`
        // non-8-aligned, so a back-to-back writer carve placed every post-PK
        // region a few bytes earlier than the reader expected — corrupting
        // weight/payload and dropping rows on a plain INSERT + scan. Build a
        // correct source via the extend path (the reader-offset oracle), rebuild
        // it through `write_to_batch` → `carve_writer_slices`, and assert the
        // read-back is byte-exact for every narrow stride at 3 rows.
        let rows: [(u128, i64, i64); 3] = [(1, 1, 30), (2, 1, 10), (3, 1, 20)];
        // U8/U16/U32 = strides 1/2/4 (the buggy non-8-aligned cases at 3 rows);
        // U64 = stride 8 (always aligned) as a control.
        for tc in [type_code::U8, type_code::U16, type_code::U32, type_code::U64] {
            let schema = single_col_pk_schema(tc);
            let stride = schema.pk_stride() as usize;

            let mut src = Batch::empty_with_schema(&schema);
            src.reserve_rows(rows.len());
            for &(pk, w, v) in &rows {
                src.extend_pk(pk);
                src.extend_weight(&w.to_le_bytes());
                src.extend_null_bmp(&0u64.to_le_bytes());
                src.extend_col(0, &v.to_le_bytes());
                src.count += 1;
            }
            let src_mb = src.as_mem_batch();

            let out = write_to_batch(&schema, rows.len(), 0, |w| {
                for i in 0..rows.len() {
                    w.write_row(&src_mb, i, src_mb.get_weight(i));
                }
            });

            assert_eq!(out.count, rows.len(), "tc={tc} stride={stride}: row count");
            for (i, &(pk, w, v)) in rows.iter().enumerate() {
                assert_eq!(
                    out.get_pk_bytes(i),
                    &pk.to_be_bytes()[16 - stride..],
                    "tc={tc} stride={stride}: pk row {i}"
                );
                assert_eq!(out.get_weight(i), w, "tc={tc} stride={stride}: weight row {i}");
                let col = out.get_col_ptr(i, 0, 8);
                assert_eq!(
                    i64::from_le_bytes(col.try_into().unwrap()),
                    v,
                    "tc={tc} stride={stride}: payload row {i}"
                );
            }
        }
    }

    #[test]
    fn extend_pk_roundtrip_across_u64_boundary() {
        let schema = single_col_pk_schema(type_code::U128);
        let mut b = Batch::with_schema(schema, 8);
        let keys: [u128; 5] = [0, 1, u64::MAX as u128, (u64::MAX as u128) + 1, u128::MAX];
        for &pk in &keys {
            b.extend_pk(pk);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &0i64.to_le_bytes());
            b.count += 1;
        }
        for (i, &pk) in keys.iter().enumerate() {
            assert_eq!(b.get_pk(i), pk, "row {i} pk roundtrip");
        }
        let iter_pks: Vec<u128> = b.pk_iter().collect();
        assert_eq!(iter_pks, keys);
    }

    #[test]
    fn set_pk_at_overwrites_existing_row() {
        let schema = single_col_pk_schema(type_code::U128);
        let mut b = Batch::with_schema(schema, 4);
        for pk in [10u128, 20, 30, 40] {
            b.extend_pk(pk);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &0i64.to_le_bytes());
            b.count += 1;
        }
        let new_pk = (u64::MAX as u128) + 7;
        b.set_pk_at(2, new_pk);
        assert_eq!(b.get_pk(0), 10);
        assert_eq!(b.get_pk(1), 20);
        assert_eq!(b.get_pk(2), new_pk);
        assert_eq!(b.get_pk(3), 40);
    }

    fn minimal_u64_with_i64_schema() -> SchemaDescriptor {
        SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0],
        )
    }

    #[test]
    fn pk_stride_u64_roundtrip() {
        let schema = minimal_u64_with_i64_schema();
        let mut b = Batch::empty_with_schema(&schema);
        b.reserve_rows(4);
        for &pk in &[0u64, 1, 1 << 32, u64::MAX] {
            b.extend_pk(pk as u128);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &0i64.to_le_bytes());
            b.count += 1;
        }
        assert_eq!(b.strides[REG_PK], 8);
        assert_eq!(b.pk_data().len(), 4 * 8);
        for (i, &pk) in [0u64, 1, 1 << 32, u64::MAX].iter().enumerate() {
            assert_eq!(b.get_pk(i), pk as u128);
        }
    }

    #[test]
    fn pk_stride_u128_roundtrip() {
        let schema = single_col_pk_schema(type_code::U128);
        let pks: &[u128] = &[0, 1, u64::MAX as u128, (u64::MAX as u128) + 1, u128::MAX];
        let mut b = Batch::empty_with_schema(&schema);
        b.reserve_rows(pks.len());
        for &pk in pks {
            b.extend_pk(pk);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &0i64.to_le_bytes());
            b.count += 1;
        }
        assert_eq!(b.strides[REG_PK], 16);
        assert_eq!(b.pk_data().len(), pks.len() * 16);
        for (i, &pk) in pks.iter().enumerate() {
            assert_eq!(b.get_pk(i), pk);
        }
    }

    #[test]
    #[cfg(debug_assertions)]
    #[should_panic]
    fn extend_pk_u64_batch_rejects_wide_pk() {
        let schema = minimal_u64_with_i64_schema();
        let mut b = Batch::empty_with_schema(&schema);
        b.reserve_rows(1);
        b.extend_pk((u64::MAX as u128) + 1);
    }

    #[test]
    fn extend_pk_bytes_then_get_pk_bytes_roundtrip_u64() {
        let schema = minimal_u64_with_i64_schema();
        let mut b = Batch::empty_with_schema(&schema);
        b.reserve_rows(4);
        let pks: [[u8; 8]; 4] = [
            [0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08],
            [0xff, 0xee, 0xdd, 0xcc, 0xbb, 0xaa, 0x99, 0x88],
            [0, 0, 0, 0, 0, 0, 0, 0],
            [0xff; 8],
        ];
        for pk in &pks {
            b.extend_pk_bytes(pk);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &0i64.to_le_bytes());
            b.count += 1;
        }
        for (i, pk) in pks.iter().enumerate() {
            assert_eq!(b.get_pk_bytes(i), pk, "row {i} bytes roundtrip");
            assert_eq!(b.get_pk(i), u64::from_be_bytes(*pk) as u128, "row {i} u128");
        }
    }

    #[test]
    fn extend_pk_bytes_then_get_pk_bytes_roundtrip_u128() {
        let schema = single_col_pk_schema(type_code::U128);
        let mut b = Batch::empty_with_schema(&schema);
        b.reserve_rows(3);
        let pks: [[u8; 16]; 3] = [
            [
                0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10,
            ],
            [0; 16],
            [0xff; 16],
        ];
        for pk in &pks {
            b.extend_pk_bytes(pk);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &0i64.to_le_bytes());
            b.count += 1;
        }
        for (i, pk) in pks.iter().enumerate() {
            assert_eq!(b.get_pk_bytes(i), pk, "row {i} bytes roundtrip");
            assert_eq!(b.get_pk(i), u128::from_be_bytes(*pk), "row {i} u128");
        }
    }

    #[test]
    fn set_pk_at_bytes_overwrites_existing_row() {
        let schema = single_col_pk_schema(type_code::U128);
        let mut b = Batch::with_schema(schema, 4);
        for pk in [10u128, 20, 30] {
            b.extend_pk(pk);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &0i64.to_le_bytes());
            b.count += 1;
        }
        let new_pk_bytes: [u8; 16] = [
            0xde, 0xad, 0xbe, 0xef, 0xfe, 0xed, 0xfa, 0xce, 0xca, 0xfe, 0xba, 0xbe, 0x12, 0x34, 0x56, 0x78,
        ];
        b.set_pk_at_bytes(1, &new_pk_bytes);
        assert_eq!(b.get_pk(0), 10);
        assert_eq!(b.get_pk_bytes(1), &new_pk_bytes);
        assert_eq!(b.get_pk(1), u128::from_be_bytes(new_pk_bytes));
        assert_eq!(b.get_pk(2), 30);
    }

    #[test]
    #[should_panic(expected = "extend_pk_bytes: length must equal pk_stride")]
    fn extend_pk_bytes_length_mismatch_panics() {
        let schema = minimal_u64_with_i64_schema();
        let mut b = Batch::empty_with_schema(&schema);
        b.reserve_rows(1);
        b.extend_pk_bytes(&[0u8; 7]);
    }

    #[test]
    fn batch_empty_accepts_wide_strides() {
        // Pin the loosened assertion: zero stride (the
        // SchemaDescriptor::default() case) and compound strides up to
        // MAX_PK_BYTES must construct without panicking.
        for stride in [0u8, 8, 16, 24, 32, 64, 80] {
            let b = Batch::empty(2, stride);
            assert_eq!(b.strides[REG_PK], stride, "stride {stride}");
            assert_eq!(b.count, 0);
        }
    }

    #[test]
    fn find_lower_bound_bytes_narrow_opk() {
        // Narrow single-PK: find_lower_bound_bytes is now a raw memcmp search
        // over OPK bytes. For an unsigned U64 PK the OPK is big-endian, so the
        // probe key is `to_be_bytes()`. Validate against a linear reference.
        let schema = minimal_u64_with_i64_schema();
        let mut b = Batch::empty_with_schema(&schema);
        let pks: [u64; 5] = [10, 20, 30, 40, 50];
        b.reserve_rows(pks.len());
        for &pk in &pks {
            b.extend_pk(pk as u128); // stored as OPK (BE) for unsigned
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &0i64.to_le_bytes());
            b.count += 1;
        }
        for probe in [0u64, 5, 10, 15, 20, 25, 30, 40, 50, 51, u64::MAX] {
            let key = probe.to_be_bytes();
            let expected = (0..b.count).find(|&i| b.get_pk_bytes(i) >= &key[..]).unwrap_or(b.count);
            assert_eq!(b.find_lower_bound_bytes(&key), expected, "probe={probe}");
        }
    }

    #[test]
    fn find_lower_bound_bytes_compound_pk_matches_compare_pk_bytes() {
        // Compound PK (3xU64, stride 24): exercises the column-walk path.
        // The result of find_lower_bound_bytes must equal the first index
        // where compare_pk_bytes(row, key) is not Less, for every probe.
        let schema = wide_pk_3xu64_schema();
        assert_eq!(schema.pk_stride(), 24);
        let mut b = Batch::empty_with_schema(&schema);
        // Grouped by 8-byte u64 column boundaries (3xU64 compound PK).
        // Sorted in compare_pk_bytes order (lexicographic over the
        // u64 columns: column 0 high priority, then 1, then 2).
        #[rustfmt::skip]
        let pks: [[u8; 24]; 5] = [
            [0,0,0,0,0,0,0,0,  0,0,0,0,0,0,0,0,  0,0,0,0,0,0,0,0],
            [1,0,0,0,0,0,0,0,  0,0,0,0,0,0,0,0,  0,0,0,0,0,0,0,0],
            [1,0,0,0,0,0,0,0,  5,0,0,0,0,0,0,0,  0,0,0,0,0,0,0,0],
            [1,0,0,0,0,0,0,0,  5,0,0,0,0,0,0,0,  9,0,0,0,0,0,0,0],
            [2,0,0,0,0,0,0,0,  0,0,0,0,0,0,0,0,  0,0,0,0,0,0,0,0],
        ];
        b.reserve_rows(pks.len());
        for pk in &pks {
            b.extend_pk_bytes(pk);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &0i64.to_le_bytes());
            b.count += 1;
        }
        #[rustfmt::skip]
        let probes: &[[u8; 24]] = &[
            [0,0,0,0,0,0,0,0,  0,0,0,0,0,0,0,0,  0,0,0,0,0,0,0,0],
            [0,0,0,0,0,0,0,0,  0,0,0,0,0,0,0,0,  1,0,0,0,0,0,0,0],
            [1,0,0,0,0,0,0,0,  0,0,0,0,0,0,0,0,  0,0,0,0,0,0,0,0],
            [1,0,0,0,0,0,0,0,  5,0,0,0,0,0,0,0,  0,0,0,0,0,0,0,0],
            [1,0,0,0,0,0,0,0,  5,0,0,0,0,0,0,0,  9,0,0,0,0,0,0,0],
            [1,0,0,0,0,0,0,0,  5,0,0,0,0,0,0,0,  10,0,0,0,0,0,0,0],
            [3,0,0,0,0,0,0,0,  0,0,0,0,0,0,0,0,  0,0,0,0,0,0,0,0],
        ];
        for key in probes {
            // Expected: first row where compare_pk_bytes(row, key) is not Less.
            let expected = (0..b.count)
                .find(|&i| super::super::columnar::compare_pk_bytes(b.get_pk_bytes(i), key) != std::cmp::Ordering::Less)
                .unwrap_or(b.count);
            let got = b.find_lower_bound_bytes(key);
            assert_eq!(got, expected, "probe={key:?}");
        }
    }

    #[test]
    fn found_row_append_narrow_byte_roundtrip() {
        // Narrow single-PK round-trip through append_row_from_source_bytes fed by
        // a found-row ColumnarSource view. The byte-typed PK path must preserve
        // byte-for-byte equivalence with the old extend_pk(pk) path: the stored
        // PK region bytes are the same LE bytes extend_pk would have written.
        crate::foundation::posix_io::raise_fd_limit_for_tests();
        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("appendrow_byte_test");
        let schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0],
        );
        let mut pt = crate::storage::PartitionedTable::new(
            tdir.to_str().unwrap(),
            "test",
            schema,
            100,
            1,
            crate::storage::Persistence::Ephemeral,
            0,
            1,
            crate::storage::partition_arena_size(1),
        )
        .unwrap();

        // Ingest one row so retract_pk can set a found-row.
        let mut src = Batch::with_schema(schema, 1);
        let pk_val: u64 = 0xDEAD_BEEFu64;
        src.extend_pk(pk_val as u128);
        src.extend_weight(&1i64.to_le_bytes());
        src.extend_null_bmp(&0u64.to_le_bytes());
        src.extend_col(0, &0x4242i64.to_le_bytes());
        src.count += 1;
        pt.ingest_owned_batch(src).unwrap();

        // retract_pk arms `last_found_partition`; found_row() exposes the stored
        // row as a ColumnarSource that append_row_from_source_bytes copies in.
        let (_w, found) = pt.retract_pk(pk_val as u128);
        assert!(found);
        let found_row = pt.found_row().expect("retract_pk armed the found row");

        let mut dst = Batch::with_schema(schema, 1);
        // PK region is OPK (big-endian) at rest; the lookup key must match.
        let pk_bytes = pk_val.to_be_bytes();
        dst.append_row_from_source_bytes(&pk_bytes, -1, &found_row, 0, None);
        assert_eq!(dst.count, 1);
        assert_eq!(dst.get_pk_bytes(0), &pk_bytes);
        assert_eq!(dst.get_pk(0), pk_val as u128);
        assert_eq!(dst.get_weight(0), -1);
        // Payload (column 0, I64) preserved from ptable.
        let payload = dst.get_col_ptr(0, 0, 8);
        assert_eq!(i64::from_le_bytes(payload.try_into().unwrap()), 0x4242i64);
    }

    #[test]
    fn extend_pk_bytes_roundtrip_compound_pk() {
        // 3-column compound PK (U64 + U64 + U64) — pk_stride = 24, exercising
        // the wide-stride byte API. extend_pk panics for this stride, so the
        // test must use extend_pk_bytes / get_pk_bytes throughout.
        let schema = wide_pk_3xu64_schema();
        assert_eq!(schema.pk_stride(), 24);
        let mut b = Batch::empty_with_schema(&schema);
        b.reserve_rows(3);
        let pks: [[u8; 24]; 3] = [
            [
                0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x21,
                0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28,
            ],
            [0u8; 24],
            [0xffu8; 24],
        ];
        for pk in &pks {
            b.extend_pk_bytes(pk);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &0i64.to_le_bytes());
            b.count += 1;
        }
        assert_eq!(b.pk_data().len(), pks.len() * 24);
        for (i, pk) in pks.iter().enumerate() {
            assert_eq!(b.get_pk_bytes(i), pk, "row {i} bytes roundtrip");
        }
    }

    /// Narrow non-power-of-two stride 12 ((U64, U32)) must round-trip through
    /// the u128-keyed extend_pk path (low 12 bytes verbatim), not panic.
    #[test]
    fn extend_pk_stride12_roundtrip() {
        let schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::U32, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0, 1],
        );
        assert_eq!(schema.pk_stride(), 12);
        let mut b = Batch::empty_with_schema(&schema);
        b.reserve_rows(3);
        // (U64, U32) packed little-endian into the low 12 bytes of a u128. The
        // `| (0 << 64)` keeps the (low, high) structure visible across all three.
        #[allow(clippy::identity_op)]
        let pks: [u128; 3] = [
            1u128 | (0u128 << 64),
            1u128 | ((u32::MAX as u128) << 64),
            7u128 | (7u128 << 64),
        ];
        for &pk in &pks {
            b.extend_pk(pk);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &0i64.to_le_bytes());
            b.count += 1;
        }
        for (i, &pk) in pks.iter().enumerate() {
            assert_eq!(b.get_pk(i), pk, "stride-12 row {i} roundtrip");
        }
        // set_pk_at must also handle stride 12.
        let new_pk = 9u128 | (123u128 << 64);
        b.set_pk_at(1, new_pk);
        assert_eq!(b.get_pk(1), new_pk);
    }

    /// extend_pk on a stride-12 batch must reject a u128 with bits set above the
    /// 12-byte (96-bit) window in debug builds (silent truncation guard).
    #[test]
    #[should_panic(expected = "narrow batch requires high bits == 0")]
    fn extend_pk_stride12_high_bits_panics() {
        let schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::U32, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0, 1],
        );
        let mut b = Batch::empty_with_schema(&schema);
        b.reserve_rows(1);
        // Bit 100 is set — above the 96-bit stride window.
        b.extend_pk(1u128 << 100);
    }

    /// A long-string German cell whose heap region [offset, offset+len) overruns
    /// the source blob must relocate to an empty string, not read out of bounds.
    /// This is the safety primitive that `append_row_from_source_bytes` and
    /// `write_join_row` rely on instead of the deleted `write_string_from_raw`.
    #[test]
    fn relocate_german_string_oob_falls_back_to_empty() {
        // Build a long-string cell: length=100 (> 12), prefix bytes, and a
        // heap offset of 0 — but the source blob is empty, so [0, 100) overruns.
        let mut cell = [0u8; 16];
        cell[0..4].copy_from_slice(&100u32.to_le_bytes()); // length
        cell[4..8].copy_from_slice(b"abcd"); // inline prefix
        cell[8..16].copy_from_slice(&0u64.to_le_bytes()); // heap offset 0
        let src_blob: &[u8] = &[]; // empty: any long string overruns

        let mut dst_blob: Vec<u8> = Vec::new();
        let out = crate::schema::relocate_german_string_vec(&cell, src_blob, &mut dst_blob, None);
        // Fallback: length field zeroed, nothing appended to dst_blob.
        let out_len = u32::from_le_bytes(out[0..4].try_into().unwrap());
        assert_eq!(out_len, 0, "OOB long string must relocate to empty");
        assert!(dst_blob.is_empty(), "no bytes should be copied on overrun");
    }

    #[test]
    #[should_panic(expected = "exceeds the batch region limit")]
    fn empty_joined_wide_combined_payload_panics() {
        // Each side: 1 U64 PK + 64 U64 payload columns (MAX_COLUMNS = 65). A join
        // carries both sides' payloads (128) through the intermediate batch,
        // overflowing the 65 payload-region slots — a named assert, not a bare OOB.
        let mut cols = vec![SchemaColumn::new(type_code::U64, 0)];
        for _ in 0..64 {
            cols.push(SchemaColumn::new(type_code::U64, 0));
        }
        let schema = SchemaDescriptor::new(&cols, &[0]);
        let _ = Batch::empty_joined(&schema, &schema);
    }
}
