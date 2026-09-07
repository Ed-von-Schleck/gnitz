//! Owned columnar batch type for Z-set rows.
//!
//! `Batch` owns its memory (two `Vec<u8>` buffers — data + blob).
//! `MemBatch<'a>` in the merge module is the borrowed slice-view counterpart.

use std::sync::atomic::{AtomicU64, Ordering};

use super::batch_pool::{acquire_arena, debug_poison, Fill};
use super::columnar::ColumnarSource;
use super::merge::{self, relocate_german_string_vec, BlobCache, BlobCacheGuard, ColPtr, MemBatch};
use crate::schema::key::NarrowPkOpk;
use crate::schema::{SchemaDescriptor, DELTA_TICK_COL};
use gnitz_expr::RowSource;
use gnitz_wire::{align8, read_i64_le, read_u64_le};

static BLOB_ID_CTR: AtomicU64 = AtomicU64::new(1);
#[inline(always)]
fn next_blob_id() -> u64 {
    BLOB_ID_CTR.fetch_add(1, Ordering::Relaxed)
}

/// Max regions **including** the trailing blob region — the bound for the
/// WAL/wire region-directory arrays (ptrs / sizes / offsets / positions).
/// Owned by `gnitz_wire::wal` (the framer's directory cap); the two in-memory
/// caps below derive from it.
pub(crate) use gnitz_wire::MAX_WIRE_REGIONS;

/// Regions tracked in the `offsets`/`strides` arrays: 3 fixed (pk, weight,
/// null_bmp) + the payload columns. The blob is not one of them — it lives in
/// `self.blob` — so this is the wire cap less that slot.
pub const MAX_BATCH_REGIONS: usize = MAX_WIRE_REGIONS - 1;

/// How many payload columns the region array can hold — the writer's own cap.
/// Deliberately looser than the semantic cap a real table hits first (64, from
/// `MAX_COLUMNS` with at least one PK column), so a change to the PK rules
/// cannot turn a valid shard into `InvalidShard`.
pub(in crate::storage) const MAX_PAYLOAD_REGIONS: usize = MAX_BATCH_REGIONS - REG_PAYLOAD_START;

// ── Region indices into `offsets` / `strides` ───────────────────────────────
//
// Three fixed regions (PK is `pk_stride` bytes/row; weight and null_bmp are
// 8 bytes/row); payload columns start at `REG_PAYLOAD_START` and continue for
// `num_payload_cols()` slots. Use these constants instead of bare numeric
// literals. Owned by `gnitz_wire::wal` (the client, the wire codec, and the
// engine all encode the same convention), same as `MAX_WIRE_REGIONS`.
pub(in crate::storage) use gnitz_wire::{REG_NULL_BMP, REG_PAYLOAD_START, REG_PK, REG_WEIGHT};
/// Stride (in bytes) of the weight and null_bmp fixed regions.
const FIXED_REGION_STRIDE: u8 = 8;
pub(in crate::storage) const FIXED_REGION_BYTES: usize = FIXED_REGION_STRIDE as usize;

/// Total rows in a `[start, end)` row-range list — the shape every range-driven
/// path (`append_ranges`, `Batch::from_ranges`, `MapPlan::append_map_ranges`)
/// sizes its destination by.
#[inline]
pub(crate) fn range_rows(ranges: &[(usize, usize)]) -> usize {
    ranges.iter().map(|&(s, e)| e - s).sum()
}

/// Write each region's byte offset into `offsets`, returning the total arena
/// size. Entries past `num_regions` are untouched; every caller passes an
/// all-zero array. An out-parameter rather than a return because the array is
/// 544 bytes and this does not inline, so a return was a `memcpy` per batch.
///
/// Region starts pad through `gnitz_wire::align8` — the same primitive
/// `wal`'s directory walk pads with, which is what makes
/// `encode_scattered_to_wire` land its regions where the directory it already
/// wrote names them.
pub(in crate::storage) fn compute_offsets_into(
    strides: &[u8; MAX_BATCH_REGIONS],
    num_regions: usize,
    capacity: usize,
    offsets: &mut [usize; MAX_BATCH_REGIONS],
) -> usize {
    // Offsets are `usize`, not `u32`: a single large batch (a wide multi-column
    // join, a bulk full-scan/merge) can have a cumulative offset > 4 GB even
    // though each individual region is still capped at 4 GB by the u32 wire
    // region sizes. A `u32` store silently truncated the per-region offset, so
    // `region_or_blob` aliased an earlier region — silent corruption. Not a wire
    // change: the WAL/exchange encoding serializes region *sizes* and recomputes
    // offsets via this fn on receive, so offsets never cross a process boundary.
    let mut off = 0usize;
    for i in 0..num_regions {
        off = align8(off);
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

/// Copy `count` rows of every region from `src` (regions at `src_offsets`)
/// into `dst` (regions at `dst_offsets`), one bulk copy per region. Shared by
/// the `reserve_rows` out-of-place grow, `clone_batch`, and the wire decode in
/// `batch_wire`.
///
/// # Safety
/// `src` and `dst` are distinct allocations; for every region `i < nr`, both
/// `src_offsets[i] + count * strides[i]` and `dst_offsets[i] + count *
/// strides[i]` are in bounds (both sides sized by `compute_offsets_into` for at
/// least `count` rows).
pub(super) unsafe fn copy_regions(
    src: &[u8],
    src_offsets: &[usize; MAX_BATCH_REGIONS],
    dst: &mut [u8],
    dst_offsets: &[usize; MAX_BATCH_REGIONS],
    strides: &[u8; MAX_BATCH_REGIONS],
    nr: usize,
    count: usize,
) {
    for i in 0..nr {
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

/// Cached row-layout guarantee. A mutation can only clear it; two paths set it —
/// `certify_layout`, which debug-verifies first, and `inherit_layout`, which
/// copies an already-verified tag across a faithful copy. `pub` so callers name
/// the variants; the `Batch.layout` field itself is private.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Layout {
    /// No order/fold guarantee.
    Raw,
    /// Strictly (PK, payload)-increasing and ghost-free (weights folded). The
    /// `into_consolidated` / merge fast paths trust this to skip a re-fold.
    Consolidated,
}

impl Layout {
    /// This claim as wire flag bits; [`Layout::from_wire_flags`] is its inverse.
    pub fn to_wire_flags(self) -> u64 {
        match self {
            Layout::Raw => 0,
            Layout::Consolidated => gnitz_wire::FLAG_BATCH_CONSOLIDATED,
        }
    }

    /// Recover a claim from wire flag bits. The `Batch` constructor already
    /// defaults `Raw`, so this is the value fed to `certify_layout` at the
    /// decode boundary (which debug-verifies the data against the claim).
    pub fn from_wire_flags(flags: u64) -> Layout {
        if flags & gnitz_wire::FLAG_BATCH_CONSOLIDATED != 0 {
            Layout::Consolidated
        } else {
            Layout::Raw
        }
    }
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
    pub blob: Vec<u8>,
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
    pub schema: SchemaDescriptor,
    /// Identity token for blob-sharing: two batches with equal `blob_id` have
    /// identical blob content, making verbatim 16-byte German String struct
    /// copies safe.  Set by `share_blob_from` and read by
    /// `append_ranges_inner` (via `MemBatch::blob_id`) to skip per-cell
    /// relocation.
    pub(crate) blob_id: u64,
}

impl Batch {
    // ── Constructors ────────────────────────────────────────────────────

    /// The one zero-allocation empty constructor: shape (strides / region
    /// count / schema) supplied by the caller, everything else empty.
    fn empty_from(strides: [u8; MAX_BATCH_REGIONS], schema: &SchemaDescriptor) -> Self {
        Batch {
            data: Vec::new(),
            blob: Vec::new(),
            offsets: [0usize; MAX_BATCH_REGIONS],
            strides,
            capacity: 0,
            count: 0,
            layout: Layout::Raw,
            schema: *schema,
            blob_id: next_blob_id(),
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
    /// region count, schema) — honest for schema-less join-shaped batches too.
    /// The empty return / swap-placeholder constructor.
    fn empty_like(&self) -> Self {
        Self::empty_from(self.strides, &self.schema)
    }

    /// Move this batch out, leaving an `empty_like` placeholder behind — the
    /// VM register-swap idiom, with the slot's shape kept truthful.
    pub fn take(&mut self) -> Self {
        let empty = self.empty_like();
        std::mem::replace(self, empty)
    }

    /// An empty batch with schema, pre-allocated for `rows` rows. The arena is
    /// **not zero-filled** — the batch invariant is *every counted row writes
    /// every region*, so nothing may read a byte it has not written. Every
    /// writer upholds it (a NULL cell takes `fill_col_zero`, not a skip) and
    /// every reader is `count`-bounded; [`Self::reserve_rows`] grows into an
    /// uninitialized arena too, so the invariant is not optional.
    ///
    /// Publishing `count` before filling is fine — `MapPlan::map_ranges_into`
    /// bulk-publishes the whole window, then writes it. Leaving a counted cell
    /// unwritten is not: it holds recycled bytes, poisoned in debug builds (see
    /// [`debug_poison`]). A writer that wants a cell to read zero writes the
    /// zero.
    pub fn with_capacity(schema: &SchemaDescriptor, rows: usize) -> Self {
        let cap = rows.max(1);
        let (strides, nr) = strides_from_schema(schema);
        // The offsets land in the batch's own array rather than a stack temp the
        // constructor would then copy in — see [`compute_offsets_into`].
        let mut b = Batch {
            data: Vec::new(),
            // No head start: every writer grows the heap on demand, so a fixed
            // reservation charges every string-free batch a malloc.
            blob: Vec::new(),
            offsets: [0usize; MAX_BATCH_REGIONS],
            strides,
            capacity: cap,
            count: 0,
            layout: Layout::Raw,
            schema: *schema,
            blob_id: next_blob_id(),
        };
        let total_size = compute_offsets_into(&strides, nr as usize, cap, &mut b.offsets);
        b.data = acquire_arena(total_size, Fill::Uninit);
        b
    }

    /// [`Self::with_capacity`] with the blob heap pre-sized. For the callers that
    /// know the byte count up front; `with_capacity` leaves it empty because most
    /// writers do not.
    pub fn with_capacity_blob(schema: &SchemaDescriptor, rows: usize, blob_bytes: usize) -> Self {
        let mut b = Self::with_capacity(schema, rows);
        b.reserve_blob(blob_bytes);
        b
    }

    /// Room for `bytes` more blob heap bytes, taken from the arena pool `Drop`
    /// recycles into — a bare [`Vec::reserve`] would take them from the global
    /// allocator. A heap already holding bytes has to grow in place.
    pub fn reserve_blob(&mut self, bytes: usize) {
        match self.blob.capacity() {
            0 => self.blob = acquire_arena(bytes, Fill::Reserve),
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
    ///
    /// Every caller is a test. It carries no `#[cfg(test)]` because the test
    /// helpers also compile inside `gnitz-server` and as `gnitz-store-testkit`,
    /// ordinary dependent crates, which see only what this library publishes.
    pub fn zeroed(schema: &SchemaDescriptor, rows: usize) -> Self {
        let (strides, nr) = strides_from_schema(schema);
        let mut b = Batch {
            data: Vec::new(),
            blob: Vec::new(),
            offsets: [0usize; MAX_BATCH_REGIONS],
            strides,
            capacity: rows.max(1),
            count: rows,
            layout: Layout::Raw,
            schema: *schema,
            blob_id: next_blob_id(),
        };
        let total_size = compute_offsets_into(&strides, nr as usize, rows.max(1), &mut b.offsets);
        b.data = vec![0u8; total_size];
        b
    }

    /// Construct a `Batch` from fully pre-built, correctly-laid-out buffers.
    ///
    /// `data` must be at least `count * strides[i]` bytes starting at
    /// `offsets[i]` for every region of `schema`, as produced by
    /// `compute_offsets_into`.  Used by `slice_to_owned_batch` to avoid an
    /// intermediate copy.
    ///
    /// # Safety
    /// Caller must guarantee the layout invariant described above.
    pub(in crate::storage) unsafe fn from_prebuilt(
        data: Vec<u8>,
        blob: Vec<u8>,
        strides: [u8; MAX_BATCH_REGIONS],
        offsets: [usize; MAX_BATCH_REGIONS],
        count: usize,
        schema: SchemaDescriptor,
    ) -> Self {
        // The one cross-check on a caller-supplied layout triple.
        debug_assert_eq!(
            strides,
            strides_from_schema(&schema).0,
            "from_prebuilt: strides disagree with the schema",
        );
        Batch {
            data,
            blob,
            offsets,
            strides,
            capacity: count,
            count,
            layout: Layout::Raw,
            schema,
            blob_id: next_blob_id(),
        }
    }

    // ── Schema installation ─────────────────────────────────────────────

    /// Install a schema on this batch after verifying its column count
    /// matches the batch's physical payload regions. Every code path that
    /// wants to mutate `batch.schema` from outside the constructors MUST go
    /// through this helper: it turns a latent "batch shape != declared
    /// shape" bug into a localized panic at the first assignment, instead
    /// of a cryptic OOB slice panic several call-frames later.
    #[inline]
    pub fn set_schema(&mut self, s: &SchemaDescriptor) {
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

    /// The live `count * stride` bytes of region `r` — the one range computation
    /// every fixed-region accessor below shares.
    #[inline(always)]
    fn region_at(&self, r: usize) -> &[u8] {
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
    pub(crate) fn pk_data(&self) -> &[u8] {
        self.region_at(REG_PK)
    }
    #[inline]
    pub(crate) fn weight_data(&self) -> &[u8] {
        self.region_at(REG_WEIGHT)
    }
    #[inline]
    pub fn null_bmp_data(&self) -> &[u8] {
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
    pub(super) fn num_regions(&self) -> usize {
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
        read_i64_le(&self.data[self.offsets[REG_WEIGHT]..], row * FIXED_REGION_BYTES)
    }
    /// Apply `f` to every row's weight in place. Generic so the per-epoch
    /// callers (negate, delta doubling) monomorphize to a tight loop. The
    /// layout tag is untouched: callers pass sign-preserving maps (negation,
    /// ×2, non-zero clamping) that cannot mint ghosts or fold duplicates.
    #[inline]
    pub fn map_weights(&mut self, f: impl Fn(i64) -> i64) {
        let off = self.offsets[REG_WEIGHT];
        for chunk in self.data[off..off + self.count * FIXED_REGION_BYTES].chunks_exact_mut(FIXED_REGION_BYTES) {
            let w = i64::from_le_bytes(chunk.try_into().unwrap());
            chunk.copy_from_slice(&f(w).to_le_bytes());
        }
    }
    /// Overwrite every row's weight with `weights`, one per row in row order.
    /// Arbitrary weights can mint ghosts, so the layout claim is dropped.
    pub(crate) fn overwrite_weights(&mut self, weights: &[i64]) {
        debug_assert_eq!(weights.len(), self.count, "overwrite_weights: one weight per row");
        for (dst, w) in self.weight_data_mut().chunks_exact_mut(FIXED_REGION_BYTES).zip(weights) {
            dst.copy_from_slice(&w.to_le_bytes());
        }
        self.downgrade();
    }
    /// Summed weight of rows `[start, end)`, read straight off the contiguous
    /// weight region so the fold vectorizes (rather than a `get_weight` per row).
    #[inline]
    pub(crate) fn sum_weights(&self, start: usize, end: usize) -> i64 {
        self.weight_data()[start * FIXED_REGION_BYTES..end * FIXED_REGION_BYTES]
            .chunks_exact(FIXED_REGION_BYTES)
            .map(|w| i64::from_le_bytes(w.try_into().unwrap()))
            .sum()
    }
    /// True iff every row's weight is `> 0` — vacuously true for an empty batch.
    /// Branch-free over the same contiguous region [`Self::sum_weights`] reads, so
    /// the conforming case is one pass with no early exit to serialize it.
    #[inline]
    pub fn all_weights_positive(&self) -> bool {
        !self
            .weight_data()
            .chunks_exact(FIXED_REGION_BYTES)
            .fold(false, |bad, w| bad | (i64::from_le_bytes(w.try_into().unwrap()) <= 0))
    }
    #[inline(always)]
    pub fn get_null_word(&self, row: usize) -> u64 {
        read_u64_le(&self.data[self.offsets[REG_NULL_BMP]..], row * FIXED_REGION_BYTES)
    }
    /// Overwrite `row`'s null-bitmap word (bit N = payload slot N is NULL).
    #[inline]
    fn set_null_word(&mut self, row: usize, word: u64) {
        let off = self.offsets[REG_NULL_BMP] + row * FIXED_REGION_BYTES;
        self.data[off..off + FIXED_REGION_BYTES].copy_from_slice(&word.to_le_bytes());
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
    pub fn reserve_rows(&mut self, n: usize) {
        if self.count + n <= self.capacity {
            return;
        }
        let nr = self.num_regions();
        let new_cap = (self.capacity * 2).max(8).max(self.count + n);
        let mut new_offsets = [0usize; MAX_BATCH_REGIONS];
        let new_total = compute_offsets_into(&self.strides, nr, new_cap, &mut new_offsets);

        if new_total > self.data.capacity() {
            // Out-of-place grow.  Vec::reserve on a too-small buffer triggers
            // realloc, which copies ALL old bytes to a new allocation (copy #1),
            // then copy_within would shift regions to new offsets (copy #2).
            // Bypass that by scatter-copying directly into a fresh buffer.
            // Zeroing is not needed (`Uninit`): `copy_regions` fills every live
            // byte, and all accessors are bounded by `count`.
            let mut new_data = acquire_arena(new_total, Fill::Uninit);
            // SAFETY: distinct allocations; both sides sized per compute_offsets_into.
            unsafe {
                copy_regions(
                    &self.data,
                    &self.offsets,
                    &mut new_data,
                    &new_offsets,
                    &self.strides,
                    nr,
                    self.count,
                );
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
            // The `set_len` above exposes `[capacity, new_cap)` of every region
            // holding whatever the previous fill left there — the one capacity
            // grow that does not run through `acquire_arena`. Poison it too, so
            // the tripwire has no hole (see `debug_poison`).
            if cfg!(debug_assertions) {
                for (&start, &stride) in new_offsets[..nr].iter().zip(&self.strides[..nr]) {
                    let live = start + self.count * stride as usize;
                    let end = start + new_cap * stride as usize;
                    debug_poison(&mut self.data[live..end]);
                }
            }
        }
        self.offsets = new_offsets;
        self.capacity = new_cap;
    }

    /// Write data into region `r` at the current row position.
    /// Auto-grows capacity if needed.  Strides must be set at construction —
    /// use `empty_with_schema` or `with_capacity`.
    ///
    /// Grows *before* it writes, and `count` moves only at `commit_row`, so
    /// within one row only the first region write can grow — which is why no
    /// appender pre-reserves and why a grow cannot strand a half-written row.
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
        if self.count >= self.capacity {
            self.reserve_rows(1);
        }
        let off = self.offsets[r] + self.count * self.strides[r] as usize;
        self.data[off..off + src.len()].copy_from_slice(src);
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
        self.push_zero_filled_row(pk, weight, 0);
    }

    /// Append one row whose payload carries no value: the PK region from `pk`
    /// (exactly `pk_stride` OPK bytes), `weight`, `null_word`, then every payload
    /// column zero-filled. The payload-free case is [`Self::push_key_row`].
    #[inline]
    pub fn push_zero_filled_row(&mut self, pk: &[u8], weight: i64, null_word: u64) {
        self.begin_row(pk, weight);
        for pi in 0..self.num_payload_cols() {
            let width = self.strides[REG_PAYLOAD_START + pi] as usize;
            self.fill_col_zero(pi, width);
        }
        self.commit_row(null_word);
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
    pub fn extend_pk_opk(&mut self, schema: &SchemaDescriptor, native_col_vals: &[u128]) {
        let cols = schema.pk_columns().map(|(_, col)| (col.type_code, *col));
        self.extend_pk_bytes(crate::schema::key::encode_leading_opk(cols, native_col_vals).pk_bytes());
    }

    /// Fill `nbytes` of zeros at the current row position in a payload column.
    #[inline]
    pub(crate) fn fill_col_zero(&mut self, pi: usize, nbytes: usize) {
        let r = REG_PAYLOAD_START + pi;
        if self.count >= self.capacity {
            self.reserve_rows(1);
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

    /// Open an [`AppendSession`] over this batch. Every bulk append runs through
    /// one; `hint_rows` sizes its blob dedup cache.
    pub(crate) fn append_session(&mut self, hint_rows: usize) -> AppendSession<'_> {
        AppendSession::open(self, hint_rows)
    }

    /// The one bulk-append body: copy every `[start, end)` row range of `src`, in
    /// list order, onto `self`'s tail. `start <= end <= src.count` per range, and
    /// `self` must share `src`'s schema (column count and strides).
    ///
    /// Non-STRING payload columns: one `copy_from_slice` per region per range.
    /// STRING payload columns: per-cell blob relocation under `cache` — unless
    /// `self` already holds `src`'s blob (see [`Self::shares_blob_with`]), in
    /// which case the 16-byte structs copy verbatim with every other column,
    /// their heap offsets still valid. The destination knows whether it owns the
    /// source's bytes, so no caller has to say.
    ///
    /// Taking the whole list rather than one range hoists the fixed per-call
    /// setup — the capacity reserve, the blob reserve, and the payload
    /// string-column map — out of the range loop. A predicate uncorrelated with
    /// the PK order yields thousands of 1-row ranges per chunk, where that setup
    /// otherwise dominates the copy it is setting up.
    ///
    /// Downgrades the layout to `Raw`: any append can break (PK, payload) order or
    /// introduce a duplicate/ghost, so no prior claim survives. This closes the
    /// W2M-decode trap where a stale strong claim could outlive an appender.
    fn append_ranges_inner(
        &mut self,
        src: &MemBatch<'_>,
        ranges: &[(usize, usize)],
        string_mask: u64,
        mut cache: Option<&mut BlobCache>,
    ) {
        for &(start, end) in ranges {
            assert!(start <= end, "append_ranges_inner: start ({start}) > end ({end})");
            assert!(
                end <= src.count,
                "append_ranges_inner: end ({end}) > src.count ({})",
                src.count
            );
        }
        let total = range_rows(ranges);
        if total == 0 {
            return;
        }
        self.reserve_rows(total);
        let npc = self.num_payload_cols();
        // A shared blob needs no per-cell relocation, so it needs no string map
        // either — every column takes the bulk region copy below.
        let shares_blob = self.shares_blob_with(src);
        let string_mask = if shares_blob { 0 } else { string_mask };
        if !shares_blob && !src.blob.is_empty() {
            // The rows this call copies, not the whole source heap: a many-run merge
            // appends into one output, and the whole heap per run ratchets capacity.
            self.blob
                .reserve(merge::prorated_blob_cap(src.blob.len(), src.count, total));
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
                let cs = self.strides[REG_PAYLOAD_START + pi] as usize;
                if (string_mask >> pi) & 1 != 0 && cs == 16 {
                    let mut dst_off = self.offsets[REG_PAYLOAD_START + pi] + self.count * 16;
                    for row in start..end {
                        let cell = relocate_german_string_vec(
                            src.get_col_ptr(row, pi, 16),
                            src.blob,
                            &mut self.blob,
                            cache.as_deref_mut(),
                        );
                        self.data[dst_off..dst_off + 16].copy_from_slice(&cell);
                        dst_off += 16;
                    }
                } else if cs > 0 {
                    self.bulk_copy_region(REG_PAYLOAD_START + pi, src.col_data(pi, cs), start, end);
                }
            }
            self.count += n;
        }
        self.downgrade();
    }
}

/// An open append into one destination batch — the only way to reach
/// [`Batch::append_ranges_inner`], so no caller can skip its setup or repay it.
///
/// That setup — the payload string-column mask and one pooled blob dedup cache —
/// is resolved once and reused by every push, which is what makes a *run*-at-a-
/// time appender viable. A merge whose runs are short is where that matters: two
/// sorted streams of uniform 128-bit keys have an expected run length of 2, so
/// the per-call setup would otherwise dominate the copy it sets up.
pub(crate) struct AppendSession<'d> {
    dst: &'d mut Batch,
    /// Bit `pi` set = payload slot `pi` is a German string. Read only when the
    /// source's blob is not already the destination's (a shared blob copies the
    /// 16-byte structs verbatim), which each push re-decides per source.
    mask: u64,
    guard: BlobCacheGuard,
}

impl<'d> AppendSession<'d> {
    fn string_mask(dst: &Batch) -> u64 {
        let npc = dst.num_payload_cols();
        debug_assert!(npc <= 64, "string mask indexes payload slots, bounded by the null word");
        let mut mask = 0u64;
        for (pi, col) in dst.schema.payload_columns() {
            if pi >= npc {
                break;
            }
            if gnitz_wire::is_german_string(col.type_code) {
                mask |= 1 << pi;
            }
        }
        mask
    }

    /// The session holds a pooled blob dedup cache for its whole life, so
    /// repeated long-string spans are appended once across every push.
    pub(crate) fn open(dst: &'d mut Batch, hint_rows: usize) -> Self {
        let mask = Self::string_mask(dst);
        let guard = BlobCacheGuard::acquire(&dst.schema, hint_rows);
        AppendSession { dst, mask, guard }
    }

    /// Append rows `[start, end)` of `src`.
    pub(crate) fn push_range(&mut self, src: &MemBatch<'_>, start: usize, end: usize) {
        self.push_ranges(src, &[(start, end)]);
    }

    /// Append every listed range of `src`, in list order.
    pub(crate) fn push_ranges(&mut self, src: &MemBatch<'_>, ranges: &[(usize, usize)]) {
        self.dst
            .append_ranges_inner(src, ranges, self.mask, self.guard.get_mut());
    }

    /// Append one row of `src` at an explicit weight, under the session's own
    /// blob dedup cache. A zero weight appends nothing.
    pub(crate) fn push_row(&mut self, src: &MemBatch<'_>, row: usize, weight: i64) {
        self.dst
            .append_row_from_source_bytes(src.get_pk_bytes(row), weight, src, row, self.guard.get_mut());
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
            blob_id: self.blob_id,
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
    #[cfg_attr(not(debug_assertions), allow(unused_variables))]
    #[inline]
    pub(crate) fn consolidated_verified(&self, schema: &SchemaDescriptor) -> bool {
        #[cfg(debug_assertions)]
        if self.layout == Layout::Consolidated {
            self.debug_verify_consolidated(schema);
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
    pub fn certify_layout(&mut self, layout: Layout, schema: &SchemaDescriptor) {
        #[cfg(debug_assertions)]
        self.debug_verify_null_bits(schema);
        #[cfg(debug_assertions)]
        match layout {
            Layout::Raw => {}
            Layout::Consolidated => self.debug_verify_consolidated(schema),
        }
        self.layout = layout;
    }

    /// Reset to no layout claim. Every order/fold-destroying mutator calls this.
    #[inline]
    pub(crate) fn downgrade(&mut self) {
        self.layout = Layout::Raw;
    }

    /// Copy a faithful-propagation source's already-verified layout tag without
    /// re-verifying: the source was verified at its birth and a subset / faithful
    /// copy (filter, null-extend, partition-filter, single-source sub-slice)
    /// preserves order, weights, and (PK, payload) distinctness.
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

    /// Debug-only (PK, payload) order of adjacent rows `i` and `i + 1` — the total
    /// order every merge/consolidation path sorts by. The verifier selects
    /// `row_cmp` once and threads it in; `with_payload_cmp!` expands at its call
    /// site, so selecting it here would run per row.
    #[cfg(debug_assertions)]
    fn adjacent_pair_ord<RowCmp>(&self, schema: &SchemaDescriptor, i: usize, row_cmp: RowCmp) -> std::cmp::Ordering
    where
        RowCmp: super::merge::RowComparator<Batch>,
    {
        use crate::schema::key::compare_pk_bytes;
        compare_pk_bytes(self.get_pk_bytes(i), self.get_pk_bytes(i + 1))
            .then_with(|| row_cmp(schema, self, i, self, i + 1))
    }

    /// Debug-only: assert no row sets a null bit under a payload column `schema`
    /// declares NOT NULL — the invariant the client-decode boundary enforces in
    /// release, checked here against every batch the engine itself builds.
    /// Runs for every layout including `Raw`, since an unsorted arrival (a union
    /// concat, a null-extend widening) is exactly where a null-fill enters.
    ///
    /// Consumers are free to read the bit raw (`compare_by_group_cols`) or to
    /// believe the declaration (`FixedIntNonnull`, the evaluator's
    /// `nullable_slots`, a projection's `NullPerm`) only because this holds.
    #[cfg(debug_assertions)]
    fn debug_verify_null_bits(&self, schema: &SchemaDescriptor) {
        let nonnull = gnitz_expr::SchemaFacts::not_null_payload_slots(schema);
        if nonnull == 0 {
            return;
        }
        let mut acc = 0u64;
        for row in 0..self.count {
            acc |= self.get_null_word(row);
        }
        debug_assert_eq!(
            acc & nonnull,
            0,
            "batch sets a null bit under a payload column the operating schema declares \
             NOT NULL (slots={:#x}, first offending row={:?} of {})",
            acc & nonnull,
            (0..self.count).find(|&r| self.get_null_word(r) & nonnull != 0),
            self.count,
        );
    }

    /// Debug-only: assert the data is fully consolidated — strictly increasing by
    /// (PK, payload) (no unfolded duplicate) AND no zero-weight row (ghost
    /// eliminated, §2).
    #[cfg(debug_assertions)]
    pub(crate) fn debug_verify_consolidated(&self, schema: &SchemaDescriptor) {
        super::columnar::with_payload_cmp!(schema, Self::debug_verify_consolidated_body, self, schema)
    }

    #[cfg(debug_assertions)]
    fn debug_verify_consolidated_body<RowCmp>(&self, schema: &SchemaDescriptor, row_cmp: RowCmp)
    where
        RowCmp: super::merge::RowComparator<Batch>,
    {
        for i in 0..self.count {
            debug_assert_ne!(
                self.get_weight(i),
                0,
                "batch flagged consolidated, but row {i} has weight 0 (ghost not eliminated)"
            );
            if i + 1 < self.count {
                debug_assert_eq!(
                    self.adjacent_pair_ord(schema, i, row_cmp),
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
        let nr = self.num_regions();
        let mut total = self.blob.len();
        for i in 0..nr {
            total += self.count * self.strides[i] as usize;
        }
        total
    }

    /// Scatter-copy selected rows from a MemBatch into a new Batch, each carrying
    /// its own weight. A caller emitting *different* weights follows with
    /// `overwrite_weights` — one sequential blit,
    /// against a per-(row, column) dispatch in the scatter.
    pub fn from_indexed_rows(batch: &MemBatch, indices: &[u32], schema: &SchemaDescriptor) -> Self {
        if indices.is_empty() {
            return Self::empty_with_schema(schema);
        }
        let blob_cap = merge::prorated_blob_cap(batch.blob.len(), batch.count, indices.len());
        write_to_batch(schema, indices.len(), blob_cap, |writer| {
            super::scatter::scatter_copy(batch, indices, writer);
        })
    }

    /// The rows named by a strictly ascending `indices`, inheriting this batch's
    /// layout: taking rows in source order preserves (PK, payload) ordering and
    /// leaves weights untouched, so a consolidated source yields a consolidated
    /// subset. A reordering caller wants
    /// [`from_indexed_rows`](Self::from_indexed_rows).
    pub fn ascending_subset(&self, indices: &[u32], schema: &SchemaDescriptor) -> Self {
        debug_assert!(
            indices.windows(2).all(|w| w[0] < w[1]),
            "ascending_subset requires a strictly ascending index list",
        );
        let mut out = Self::from_indexed_rows(&self.as_mem_batch(), indices, schema);
        out.inherit_layout(self);
        out
    }

    /// A fresh `out_schema` batch of this batch's rows, with what a widen, a
    /// stamp and a strip all copy identically already taken: blob heap, weights,
    /// and every payload column of `in_schema`. Each caller rewrites the one
    /// region it owns.
    ///
    /// **`count` is published while the PK and NULL regions are unwritten** — the
    /// caller must write both, or a release build reads uninitialized arena bytes.
    fn shell_for(&self, in_schema: &SchemaDescriptor, out_schema: &SchemaDescriptor) -> Batch {
        debug_assert!(
            out_schema.num_payload_cols() >= in_schema.num_payload_cols(),
            "shell_for copies every payload column of in_schema at its own index",
        );
        let n = self.count;
        let mut out = Self::with_capacity(out_schema, n);
        out.count = n;
        // Share the input heap so long (> 12 byte) STRING/BLOB values, whose
        // 16-byte structs are copied verbatim below, still resolve. Sharing an
        // empty blob is a no-op, so no emptiness guard.
        if in_schema.has_german_string() {
            out.share_blob_from(self);
        }
        out.weight_data_mut().copy_from_slice(self.weight_data());
        for (pi, col) in in_schema.payload_columns() {
            let stride = col.size() as usize;
            out.col_data_mut(pi).copy_from_slice(&self.col_data(pi)[..n * stride]);
        }
        out
    }

    /// Copy every row into `out_schema`, whose key is `prefix` (eight big-endian
    /// bytes) followed by this batch's own key and whose payload space is
    /// unchanged — the shape [`crate::schema::make_delta_schema`] derives.
    ///
    /// Prepending one constant to every key preserves both sortedness and
    /// distinctness, so the layout claim carries across: a batch that arrives
    /// `Consolidated` stays `Consolidated` and the stamp never forces a re-sort
    /// the caller would not otherwise have paid for.
    ///
    /// The sibling of [`Self::widened_with_null_tail`] rather than a call into it:
    /// that one asserts the two schemas share a PK stride, and this changes it by
    /// eight bytes. The NULL words copy whole here because the payload space is
    /// identical — the delta schema is a reordering of the view's columns, not a
    /// shift of them.
    pub fn stamped_with_pk_prefix(
        &self,
        in_schema: &SchemaDescriptor,
        out_schema: &SchemaDescriptor,
        prefix: u64,
    ) -> Self {
        let in_stride = in_schema.pk_stride();
        let out_stride = out_schema.pk_stride();
        let stamp_bytes = DELTA_TICK_COL.size() as usize;
        debug_assert_eq!(out_stride, in_stride + stamp_bytes);
        debug_assert_eq!(out_schema.num_payload_cols(), in_schema.num_payload_cols());
        if self.count == 0 {
            return Self::empty_with_schema(out_schema);
        }

        let mut output = self.shell_for(in_schema, out_schema);
        output.null_bmp_data_mut().copy_from_slice(self.null_bmp_data());

        let stamp = prefix.to_be_bytes();
        let src_pk = self.pk_data();
        for (dst, src) in output
            .pk_data_mut()
            .chunks_exact_mut(out_stride)
            .zip(src_pk.chunks_exact(in_stride))
        {
            dst[..stamp_bytes].copy_from_slice(&stamp);
            dst[stamp_bytes..].copy_from_slice(src);
        }

        output.inherit_layout(self);
        output
    }

    /// Copy every row into `out_schema`, dropping the eight leading big-endian
    /// bytes [`Self::stamped_with_pk_prefix`] wrote — the inverse of that call,
    /// with the schemas swapped.
    ///
    /// **It does not inherit the layout claim, where the stamp does.** Removing
    /// the prefix preserves neither sortedness nor distinctness once the batch
    /// spans more than one stamp value: the rows are ordered by stamp first, so
    /// round 5's key 100 sits before round 6's key 3, and the same
    /// `(key, payload)` element legitimately appears under two stamps. The result
    /// claims `Layout::Raw` so the consumer's sort-and-fold runs.
    pub fn stripped_of_pk_prefix(&self, in_schema: &SchemaDescriptor, out_schema: &SchemaDescriptor) -> Self {
        let in_stride = in_schema.pk_stride();
        let out_stride = out_schema.pk_stride();
        let stamp_bytes = DELTA_TICK_COL.size() as usize;
        debug_assert_eq!(in_stride, out_stride + stamp_bytes);
        debug_assert_eq!(out_schema.num_payload_cols(), in_schema.num_payload_cols());
        if self.count == 0 {
            return Self::empty_with_schema(out_schema);
        }

        let mut output = self.shell_for(in_schema, out_schema);
        output.null_bmp_data_mut().copy_from_slice(self.null_bmp_data());

        let src_pk = self.pk_data();
        for (dst, src) in output
            .pk_data_mut()
            .chunks_exact_mut(out_stride)
            .zip(src_pk.chunks_exact(in_stride))
        {
            dst.copy_from_slice(&src[stamp_bytes..]);
        }

        output
    }

    /// Copy every row into `out_schema`, which must extend this batch's schema
    /// with extra trailing payload columns, filling those columns with NULL.
    /// The PK region, weights and existing payload columns carry over verbatim,
    /// so the layout does too.
    ///
    /// `out_schema` must share this batch's PK stride and have at least as many
    /// payload columns; the caller states the input schema because a `Batch`
    /// carries only its region strides.
    pub fn widened_with_null_tail(&self, in_schema: &SchemaDescriptor, out_schema: &SchemaDescriptor) -> Self {
        debug_assert_eq!(out_schema.pk_stride(), in_schema.pk_stride());
        let in_npc = in_schema.num_payload_cols();
        let out_npc = out_schema.num_payload_cols();
        debug_assert!(out_npc >= in_npc);
        let n = self.count;
        if n == 0 {
            return Self::empty_with_schema(out_schema);
        }

        let mut output = self.shell_for(in_schema, out_schema);
        output.pk_data_mut().copy_from_slice(self.pk_data());

        // Zeroing just the appended columns keeps every counted row fully written
        // without provisioning the whole arena zeroed (`Batch::with_capacity`).
        // Nothing reads the value: every reader decides on the null bit.
        for pi in in_npc..out_npc {
            output.col_data_mut(pi).fill(0);
        }
        let tail_null_bits = gnitz_wire::all_payload_null_mask(out_npc - in_npc);
        for row in 0..n {
            let out_null = gnitz_wire::merge_null_words(self.get_null_word(row), tail_null_bits, in_npc);
            output.set_null_word(row, out_null);
        }

        output.inherit_layout(self);
        output
    }

    /// Clone all buffers into a new independent Batch (2 allocations).
    pub fn clone_batch(&self) -> Self {
        // Rebuilt rather than copied through two pooled arenas holding zero bytes.
        // `empty_like` mints a fresh `blob_id` and drops the layout tag; neither
        // is observable on a batch holding no rows and no heap.
        if self.holds_nothing() {
            return self.empty_like();
        }
        // Only clone the actually-used portion of data (count-based, not capacity-based).
        let nr = self.num_regions();
        let mut packed_offsets = [0usize; MAX_BATCH_REGIONS];
        let packed_size = compute_offsets_into(&self.strides, nr, self.count, &mut packed_offsets);
        let mut new_data = acquire_arena(packed_size, Fill::Uninit);
        // SAFETY: distinct allocations; `new_data` sized per compute_offsets_into.
        unsafe {
            copy_regions(
                &self.data,
                &self.offsets,
                &mut new_data,
                &packed_offsets,
                &self.strides,
                nr,
                self.count,
            );
        }
        let mut new_blob = acquire_arena(self.blob.len(), Fill::Reserve);
        new_blob.extend_from_slice(&self.blob);
        Batch {
            data: new_data,
            blob: new_blob,
            offsets: packed_offsets,
            strides: self.strides,
            capacity: self.count,
            count: self.count,
            layout: self.layout,
            schema: self.schema,
            blob_id: self.blob_id,
        }
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

    /// Binary search for the first row whose OPK bytes are `>= key`. After the
    /// OPK-at-rest flip this is a raw `memcmp` search with no schema dependency,
    /// correct for compound, signed, and wide (`pk_stride > 16`) PKs alike.
    ///
    /// `key` must be exactly `pk_stride` OPK bytes — identical width to the
    /// stored regions it is compared against.
    pub(crate) fn find_lower_bound_bytes(&self, key: &[u8]) -> usize {
        let stride = self.pk_stride() as usize;
        let cp = self.pk_col_ptr();
        unsafe { super::columnar::seek_lower_bound(self.count, stride, cp, key) }
    }

    /// Galloping forward lower bound seeded at `hint` (the caller's live
    /// position): `O(log gap)` when the boundary is just ahead, `O(1)` when it
    /// IS the hint, never worse than `find_lower_bound_bytes`. Used by the
    /// sorted-stream co-group merge, whose probe keys ascend, so the boundary
    /// only moves forward. `key` must be exactly `pk_stride` OPK bytes.
    pub(crate) fn advance_to(&self, key: &[u8], hint: usize) -> usize {
        let stride = self.pk_stride() as usize;
        let cp = self.pk_col_ptr();
        unsafe { super::columnar::seek_advance_to(self.count, stride, cp, key, hint) }
    }

    /// Bulk-copy every `[start, end)` range of `src`, in list order, onto this
    /// batch's tail — the one bulk-append entry point, relocating German-string
    /// blob data into `self`'s heap. One capacity reserve, one string-column mask
    /// and one blob dedup cache serve the whole list, so a filter pass's thousands
    /// of one-row ranges pay it once.
    ///
    /// Call [`Self::share_blob_from`] first to skip per-cell string relocation
    /// (see `append_ranges_inner`); it is a pure optimization, correct either
    /// way.
    pub fn append_ranges(&mut self, src: &MemBatch<'_>, ranges: &[(usize, usize)]) {
        let rows = range_rows(ranges);
        // Before the session, which derives a payload string mask and takes a
        // pooled blob cache to copy nothing. An all-DELETE push emits one empty
        // range per row.
        if rows == 0 {
            return;
        }
        self.append_session(rows).push_ranges(src, ranges);
    }

    /// All of `src` — the full-range decode/accumulate entry point (W2M ingest,
    /// the master's index-scan merge).
    pub fn append_mem_batch(&mut self, src: &MemBatch<'_>) {
        self.append_ranges(src, &[(0, src.count)]);
    }

    /// Rows `[start, end)` of another `Batch` of the same schema.
    pub fn append_batch(&mut self, src: &Batch, start: usize, end: usize) {
        self.append_ranges(&src.as_mem_batch(), &[(start, end)]);
    }

    /// Gather every `[start, end)` row range of `src`, in list order, into a fresh
    /// batch — a filter pass's survivor list, or one slice of a RAM-tier run.
    ///
    /// Disjoint ascending ranges (debug-checked) make the result a subset *in source
    /// order*, which is what lets it inherit `src`'s layout tag; an overlap would
    /// repeat a row and break the distinctness half of a `Consolidated` claim. The
    /// blob arm is [`merge::should_relocate_blob`]'s call.
    pub(crate) fn from_ranges(src: &Batch, ranges: &[(usize, usize)], schema: &SchemaDescriptor) -> Batch {
        debug_assert!(
            ranges.windows(2).all(|w| w[0].1 <= w[1].0),
            "from_ranges: ranges must be disjoint and ascending",
        );
        let rows = range_rows(ranges);
        if rows == 0 {
            // Not `with_capacity(_, 0)`, which rounds up to a 1-row arena. The `Raw`
            // tag needs no repair: both layout readers short-circuit on `count == 0`.
            return Batch::empty_with_schema(schema);
        }
        let mut out = Batch::with_capacity(schema, rows);
        if !merge::should_relocate_blob(src.blob.len(), src.count, rows) {
            out.share_blob_from(src);
        }
        out.append_ranges(&src.as_mem_batch(), ranges);
        // `append_ranges` downgraded `out` to `Raw` first.
        out.inherit_layout(src);
        out
    }

    /// No rows and no string heap — so this batch already *is* its own cleared
    /// and its own copied form, and `clear`/`clone_batch` can hand back what
    /// they were given. Both keep `blob_id`: `shares_blob_with` also requires
    /// equal blob lengths, so a kept id can only ever match another empty heap.
    #[inline]
    fn holds_nothing(&self) -> bool {
        self.count == 0 && self.blob.is_empty()
    }

    /// Reset to empty without freeing buffer allocations.
    pub fn clear(&mut self) {
        if self.holds_nothing() {
            return;
        }
        // data buffer stays allocated — capacity and offsets remain valid. The
        // rows just dropped are the one way live batch bytes get re-exposed
        // without passing through `acquire_arena` or `reserve_rows`, so poison
        // them too: a refill that skips a cell must trip, not silently ship the
        // previous fill's bytes (see `debug_poison`). Only the live prefix of
        // each region — everything past `count` is already poisoned, and this
        // runs per epoch on every VM delta register.
        if cfg!(debug_assertions) {
            let nr = self.num_regions();
            for (&start, &stride) in self.offsets[..nr].iter().zip(&self.strides[..nr]) {
                debug_poison(&mut self.data[start..start + self.count * stride as usize]);
            }
        }
        self.count = 0;
        self.blob.clear();
        self.downgrade();
        self.blob_id = next_blob_id();
    }

    /// Whether this batch's blob content is byte-identical to `src`'s, so
    /// German-string structs can be copied verbatim (their heap offsets resolve
    /// the same on both sides) instead of relocating each cell.
    ///
    /// Equal `blob_id` alone is not enough: it is minted per construction and
    /// propagated by [`Self::share_blob_from`] / `clone_batch`, but a *relocating*
    /// append from some other source afterwards grows `self.blob` past `src`'s.
    /// Blobs only ever grow by appending, so length equality closes exactly that
    /// gap and the pair is an exact test.
    ///
    /// A *wire-borrowed* `MemBatch` carries `blob_id == 0` while every `Batch`
    /// mints one from 1 up, so a wire source always answers `false` — which is
    /// what makes relocation the canonicalizing gate for W2M frames, the one
    /// German-string ingress that skips `validate_string_heap_extents`.
    #[inline]
    pub(crate) fn shares_blob_with(&self, src: &MemBatch<'_>) -> bool {
        debug_assert!(
            self.blob_id != 0,
            "shares_blob_with: a Batch must never carry the wire blob_id 0"
        );
        self.blob_id == src.blob_id && self.blob.len() == src.blob.len()
    }

    /// Copy blob content from `src` and record that this batch shares `src`'s
    /// blob identity, so a subsequent `append_ranges`/`append_batch` from `src`
    /// copies German-string structs verbatim instead of relocating each cell.
    pub(crate) fn share_blob_from(&mut self, src: &Batch) {
        debug_assert!(
            self.blob.is_empty(),
            "share_blob_from replaces the heap: a batch already holding rows would re-resolve them",
        );
        // Reuse the pooled destination buffer rather than dropping it for a
        // fresh exact-sized clone (an allocation per call). The blob bytes are
        // identical, so the shared blob_id and every German-string offset stay
        // valid — a behavioral no-op apart from the saved allocation.
        self.blob.clear();
        self.blob.extend_from_slice(&src.blob);
        self.blob_id = src.blob_id;
    }

    /// Safe `&[u8]` view of region `idx` — `count * stride` bytes for a fixed
    /// region, the whole heap for the trailing blob — for callers that frame the
    /// batch into a byte buffer (`batch_wire`'s wire encoders) or hand it to the
    /// shard writer. The region copy stays bounds-checked, no raw pointers.
    pub(crate) fn region_or_blob(&self, idx: usize) -> &[u8] {
        let blob_idx = self.num_regions();
        if idx < blob_idx {
            self.region_at(idx)
        } else if idx == blob_idx {
            &self.blob
        } else {
            panic!(
                "region_or_blob: index {idx} out of range ({} regions incl. blob)",
                blob_idx + 1
            );
        }
    }

    /// Per-row byte stride of a fixed/payload region. Used by the range-wire
    /// encoders in `batch_wire`, which size regions for an arbitrary row count
    /// rather than `self.count` (so `region_size` does not fit).
    pub(super) fn region_stride(&self, idx: usize) -> u8 {
        self.strides[idx]
    }

    /// Append `source[row]` under a raw-OPK-bytes key, with blob deduplication.
    ///
    /// Pass `None` for `blob_cache` when the schema has no STRING columns or when
    /// cross-row dedup isn't worth the bookkeeping; `Some(..)` dedups repeated
    /// source long-string spans into one destination copy. `pk_bytes` must be
    /// exactly `pk_stride` bytes (asserted by `extend_pk_bytes`). Also valid for
    /// narrow PKs — the only difference from the `u128` entry point is how the
    /// PK region is written.
    pub fn append_row_from_source_bytes<S: RowSource>(
        &mut self,
        pk_bytes: &[u8],
        weight: i64,
        source: &S,
        row: usize,
        mut blob_cache: Option<&mut BlobCache>,
    ) {
        if weight == 0 {
            return;
        }
        self.begin_row(pk_bytes, weight);
        let null_word = source.get_null_word(row);

        // Walks this batch's own schema by index, re-reading the 4-byte
        // `SchemaColumn` per column, rather than calling the shared
        // `append_payload_cols`: that takes the schema by reference, which the
        // `&mut self` cell writes below would alias, and copying the descriptor
        // out to dodge that puts a 360-byte `memcpy` on this per-row path. The
        // cell body is still the shared `append_payload_cell`.
        let src_blob = source.blob();
        let num_payload = self.schema.num_payload_cols();
        for pi in 0..num_payload {
            let col = {
                let s = &self.schema;
                s.columns[s.payload_col_idx(pi)]
            };
            let cs = col.size() as usize;
            let cell = (!gnitz_wire::null_word_get(null_word, pi)).then(|| source.get_col_ptr(row, pi, cs));
            self.append_payload_cell(pi, col.type_code, cs, cell, src_blob, blob_cache.as_deref_mut());
        }

        self.commit_row(null_word);
    }

    /// Append the payload columns described by `schema` from `src[row]` into
    /// this batch's payload slots starting at `out_pi_base`, relocating German
    /// strings into `self.blob`. `null_word` is the **source-side** null word
    /// (bit `pi` per source payload slot); a null column zero-fills its slot.
    /// Shared by the whole-row appender above (`out_pi_base == 0`, `schema` =
    /// the batch's own) and the join row writer, which appends the left half at
    /// base 0 and the right half at the left payload count. Does not bump
    /// `count` or touch the layout. `#[inline]` — no per-row cross-file call
    /// boundary.
    #[inline]
    pub(crate) fn append_payload_cols<S: RowSource>(
        &mut self,
        out_pi_base: usize,
        schema: &SchemaDescriptor,
        src: &S,
        row: usize,
        null_word: u64,
        mut blob_cache: Option<&mut BlobCache>,
    ) {
        let src_blob = src.blob();
        for (pi, col) in schema.payload_columns() {
            let cs = col.size() as usize;
            let is_null = gnitz_wire::null_word_get(null_word, pi);
            let cell = (!is_null).then(|| src.get_col_ptr(row, pi, cs));
            self.append_payload_cell(
                out_pi_base + pi,
                col.type_code,
                cs,
                cell,
                src_blob,
                blob_cache.as_deref_mut(),
            );
        }
    }

    /// Append one payload cell into output slot `out_pi` — the single
    /// null/German-string/plain copy body every payload-cell writer shares.
    /// `src_cell` is the source cell's at-rest bytes (`size` wide; the 16-byte
    /// struct for STRING/BLOB), or `None` for a NULL cell (zero-fills the
    /// slot). STRING/BLOB structs are relocated into `self.blob` against
    /// `src_blob`; everything else copies verbatim. Does not bump `count`,
    /// touch the null word, or change the layout.
    #[inline]
    pub(crate) fn append_payload_cell(
        &mut self,
        out_pi: usize,
        type_code: u8,
        size: usize,
        src_cell: Option<&[u8]>,
        src_blob: &[u8],
        blob_cache: Option<&mut BlobCache>,
    ) {
        match src_cell {
            None => self.fill_col_zero(out_pi, size),
            Some(cell) if gnitz_wire::is_german_string(type_code) => {
                let dest = relocate_german_string_vec(cell, src_blob, &mut self.blob, blob_cache);
                self.extend_col(out_pi, &dest);
            }
            Some(cell) => self.extend_col(out_pi, cell),
        }
    }

    /// Consume this batch, consolidating it if needed. The returned batch is
    /// certified `Consolidated`.
    ///
    /// Fast path: an already-consolidated (or empty) `self` is returned by move
    /// with no allocation. Slow path: sorts and weight-folds into a fresh batch,
    /// then drops `self`.
    pub fn into_consolidated(mut self, schema: &SchemaDescriptor) -> Batch {
        if self.consolidated_verified(schema) {
            // Already consolidated, or empty (structurally consolidated): return
            // by move. Pin the tag so an empty `Raw` batch still reports
            // `Consolidated` to downstream trust sites.
            self.layout = Layout::Consolidated;
            return self;
        }
        Self::consolidate_into_new(&self, schema)
    }

    /// Consolidate a borrowed batch if needed. Returns `None` when the batch is
    /// already consolidated or empty (caller borrows the original). Returns
    /// `Some(batch)` — certified `Consolidated` — when a new batch was allocated.
    ///
    /// Idiomatic usage:
    /// ```ignore
    /// let cs = Batch::consolidate_if_needed(delta, schema);
    /// let c: &Batch = cs.as_ref().unwrap_or(delta);
    /// ```
    pub(crate) fn consolidate_if_needed(batch: &Batch, schema: &SchemaDescriptor) -> Option<Batch> {
        (!batch.consolidated_verified(schema)).then(|| Self::consolidate_into_new(batch, schema))
    }

    /// Sort and weight-fold `batch` into a fresh certified batch — the
    /// consolidation slow path both entry points above share.
    ///
    /// The fold runs first so the arena is sized to the survivor count, not the
    /// input row count: that is poison fill skipped in debug, and in release a
    /// heavily-cancelling fold that stays under `POOL_BYPASS_BYTES` and is
    /// recycled. Blob capacity is reserved, not zeroed, so the whole source heap
    /// is a free bound.
    fn consolidate_into_new(batch: &Batch, schema: &SchemaDescriptor) -> Batch {
        let mb = batch.as_mem_batch();
        let mut survivors: Vec<(u32, u32, i64)> = Vec::with_capacity(batch.count);
        merge::consolidate_groups(&mb, schema, &mut survivors);
        let mut cols = Vec::with_capacity(schema.num_payload_cols());
        let unified = [merge::mem_batch_to_unified(&mb, schema, &mut cols)];
        let mut result = write_to_batch(schema, survivors.len(), mb.blob.len(), |writer| {
            super::scatter::scatter_unified_sources(&unified, &cols, &survivors, writer);
        });
        result.certify_layout(Layout::Consolidated, schema);
        result
    }

    #[cfg(test)]
    pub(crate) fn data_capacity(&self) -> usize {
        self.data.capacity()
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
        &self.blob
    }
    #[inline(always)]
    fn row_count(&self) -> usize {
        Batch::len(self)
    }
}

impl ColumnarSource for Batch {
    #[inline(always)]
    fn get_weight(&self, row: usize) -> i64 {
        Batch::get_weight(self, row)
    }
}

/// Allocate a single contiguous arena, run a merge/copy operation via
/// DirectWriter, and return the arena as a Batch — zero copy-out.
///
/// In steady state the arena is recycled from the thread-local buffer pool,
/// so this path allocates nothing.
///
/// The arena is **uninitialized** ([`Batch::with_capacity`]'s contract): every
/// [`merge::DirectWriter`] entry point writes each live byte of each row it
/// counts, and every reader — accessor, `region_or_blob`, `total_bytes` — bounds
/// the batch to `count`, so the `[count, capacity)` tail and the inter-region
/// alignment padding are never read and never serialized.
pub(crate) fn write_to_batch(
    schema: &SchemaDescriptor,
    max_rows: usize,
    max_blob: usize,
    write_fn: impl FnOnce(&mut merge::DirectWriter),
) -> Batch {
    let mut b = Batch::with_capacity_blob(schema, max_rows, max_blob);
    let rows = {
        // `b.capacity`, not `max_rows`: the writer must carve at the offsets the
        // batch will read back through.
        let mut writer = merge::DirectWriter::over_arena(&mut b.data, &b.schema, b.capacity, &mut b.blob);
        write_fn(&mut writer);
        writer.count
    };
    b.count = rows;
    // Nothing was written: return the buffer rather than park it in a batch whose
    // `total_bytes` cannot see it. `max_blob` is only an estimate — an all-short
    // STRING column leaves the heap empty.
    if b.blob.is_empty() {
        super::batch_pool::recycle_buf(std::mem::take(&mut b.blob));
    }
    b
}

#[cfg(test)]
#[path = "tests/batch.rs"]
mod tests;
