//! Owned columnar batch type for Z-set rows.
//!
//! `Batch` owns its memory (two `Vec<u8>` buffers — data + blob).
//! `MemBatch<'a>` in the merge module is the borrowed slice-view counterpart.

use std::sync::atomic::{AtomicU64, Ordering};

use super::columnar::ColumnarSource;
use super::merge::{self, relocate_german_string_vec, BlobCache, BlobCacheGuard, ColPtr, MemBatch};
use crate::schema::key::NarrowPkOpk;
use crate::schema::SchemaDescriptor;
use gnitz_expr::RowSource;
use gnitz_wire::{align8, read_i64_le, read_u64_le};

static BLOB_ID_CTR: AtomicU64 = AtomicU64::new(1);
#[inline(always)]
fn next_blob_id() -> u64 {
    BLOB_ID_CTR.fetch_add(1, Ordering::Relaxed)
}

/// The one boundary between "serve from the buffer pool" and "allocate fresh".
/// `batch_pool::MAX_RECYCLE_CAPACITY` aliases it, so it is also the pool's only
/// bound on bytes (`MAX_POOLED` bounds its count) — what stops one outsized
/// buffer from trapping memory for the process's lifetime.
///
/// It is *not* a hugepage threshold, despite the size: `acquire_arena` cannot
/// `madvise(MADV_HUGEPAGE)` these buffers at all. glibc serves a `Vec` this
/// large from its own `mmap` and hands back `base + 0x10`, which is not
/// page-aligned, so the call only ever returned `EINVAL`.
pub(super) const POOL_BYPASS_BYTES: usize = 2 * 1024 * 1024;

/// Cost of relocating one German-string cell, in bytes of whole-heap memcpy — the
/// unit that lets [`Batch::should_relocate_blob`] weigh the two arms with one
/// comparison. Set from `slice_blob_relocate_bench`, which sweeps both arms over
/// slice fraction × string width; the resulting crossovers (~3 % of a 16-byte-string
/// source, ~7 % at 40 bytes, ~34 % at 256, ~67 % at 1024) are what this value fits.
const RELOCATE_CELL_COST_BYTES: usize = 500;

/// Maximum regions tracked in the `offsets`/`strides` arrays:
/// 3 fixed (pk, weight, null_bmp) + up to 64 payload columns
/// (schema max is `MAX_COLUMNS` = 65 total columns, 1 is the PK).  The blob is
/// not in this array; it lives in `self.blob` and is accounted for separately.
/// 3 + 64 = 67, rounded up to 68 to keep the array size as a multiple of 4.
pub const MAX_BATCH_REGIONS: usize = 68;

/// How many payload columns the region array can hold — the writer's own cap,
/// enforced by `fill_payload_strides`. Deliberately looser than the semantic cap
/// a real table hits first (64, from `MAX_COLUMNS` with at least one PK column),
/// so a change to the PK rules cannot turn a valid shard into `InvalidShard`.
pub(in crate::storage) const MAX_PAYLOAD_REGIONS: usize = MAX_BATCH_REGIONS - REG_PAYLOAD_START;

/// Max regions **including** the trailing blob region — the bound for the
/// WAL/wire region-directory arrays (ptrs / sizes / offsets / positions).
/// Owned by `gnitz_wire::wal` (the framer's directory cap); the engine ties its
/// in-memory offsets/strides capacity (`MAX_BATCH_REGIONS`, no blob) to it: the
/// wire encoders enumerate one more region (the blob heap), so the directory
/// needs exactly one extra slot.
pub(crate) use gnitz_wire::MAX_WIRE_REGIONS;
const _: () = assert!(MAX_WIRE_REGIONS == MAX_BATCH_REGIONS + 1); // = 69

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

/// How [`acquire_arena`] initializes the returned buffer.
pub(in crate::storage) enum Fill {
    /// `len == size`, contents uninitialized — the caller writes every live
    /// byte before any read. See [`debug_poison`] for the debug-build tripwire.
    Uninit,
    /// `len == 0`, `capacity >= size` — for growable arenas filled by append
    /// (blob heaps).
    Reserve,
}

/// Debug-build poison for bytes a caller has not written yet — a value that is
/// not a plausible zero, weight, or PK.
///
/// The batch invariant is *every counted row writes every region* ([`Fill::Uninit`],
/// [`Batch::with_capacity`], [`super::merge::DirectWriter`]), so nothing may read
/// an unwritten byte. But a fresh OS allocation arrives demand-zero, so a caller
/// that wrongly relies on a zero passes its tests and only misbehaves in
/// production once the buffer pool starts recycling. Poisoning every byte the
/// batch exposes-but-has-not-written makes that mistake deterministic instead.
///
/// Applied wherever batch bytes become writable-but-unwritten: [`acquire_arena`]'s
/// `Uninit` arms, [`Batch::reserve_rows`]'s in-place grow (whose `set_len` would
/// otherwise leave the freshly exposed tail un-poisoned — the steady-state path
/// for a recycled batch being refilled), and [`Batch::clear`]'s dropped rows.
#[inline]
pub(in crate::storage) fn debug_poison(bytes: &mut [u8]) {
    if cfg!(debug_assertions) {
        bytes.fill(0xA5);
    }
}

/// The one arena-provisioning path for batch data/blob buffers.
///
/// Sizes `>= POOL_BYPASS_BYTES` allocate fresh: the pool retains nothing above
/// that size, so probing it would pop the LIFO head, find it undersized, and
/// discard a hot buffer for nothing. (Exactly at the boundary a pooled buffer
/// could have served; the bypass gives up that one size to stay one compare.)
///
/// Below the threshold the pool is tried first; an undersized pooled buffer is
/// evicted rather than grown in place — `Vec::reserve` on a too-small buffer
/// copies the old bytes forward before the tail is written, slower than a fresh
/// allocation, and eviction converges the pool to larger sizes.
#[allow(clippy::uninit_vec)] // `Fill::Uninit` is the documented contract: callers write every live byte
pub(in crate::storage) fn acquire_arena(size: usize, fill: Fill) -> Vec<u8> {
    #[inline]
    fn fresh(size: usize, fill: &Fill) -> Vec<u8> {
        match fill {
            Fill::Uninit => {
                let mut v = Vec::with_capacity(size);
                // SAFETY: u8 needs no init; callers write every `count`-bounded
                // live byte before it is read (the `Uninit` contract above).
                unsafe { v.set_len(size) };
                debug_poison(&mut v);
                v
            }
            Fill::Reserve => Vec::with_capacity(size),
        }
    }

    if size >= POOL_BYPASS_BYTES {
        return fresh(size, &fill);
    }
    let mut buf = super::batch_pool::acquire_buf();
    if buf.capacity() < size {
        drop(buf); // evict the undersized buffer; pool converges to larger sizes
        return fresh(size, &fill);
    }
    // SAFETY: capacity checked above; `Uninit` is the documented
    // write-before-read contract.
    match fill {
        Fill::Uninit => {
            unsafe { buf.set_len(size) };
            debug_poison(&mut buf);
        }
        Fill::Reserve => {}
    }
    buf
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
    // `region_slice` aliased an earlier region — silent corruption. Not a wire
    // change: the WAL/exchange encoding serializes region *sizes* and recomputes
    // offsets via this fn on receive, so offsets never cross a process boundary.
    let mut offsets = [0usize; MAX_BATCH_REGIONS];
    let mut off = 0usize;
    for i in 0..num_regions {
        off = align8(off);
        offsets[i] = off;
        off += capacity * strides[i] as usize;
    }
    (offsets, off)
}

/// Append payload strides from `schema` into `strides` starting at `start`.
/// Returns the next free index (i.e. `start + num_payload_cols`).
fn fill_payload_strides(schema: &SchemaDescriptor, strides: &mut [u8; MAX_BATCH_REGIONS], start: usize) -> usize {
    let mut idx = start;
    for (_, col) in schema.payload_columns() {
        // A join carries both sides' payload columns through the intermediate
        // batch, so two wide tables can drive `idx` past the region limit. Turn
        // the would-be bare index-OOB into a named diagnostic (the correct
        // long-term fix is a plan-time query error, tracked separately).
        assert!(
            idx < MAX_BATCH_REGIONS,
            "fill_payload_strides: combined payload column count exceeds the batch \
             region limit ({MAX_BATCH_REGIONS} = {REG_PAYLOAD_START} + {MAX_PAYLOAD_REGIONS} payload cols)",
        );
        strides[idx] = col.size();
        idx += 1;
    }
    idx
}

/// Build a strides array from a SchemaDescriptor.
pub(in crate::storage) fn strides_from_schema(schema: &SchemaDescriptor) -> ([u8; MAX_BATCH_REGIONS], u8) {
    let mut strides = [0u8; MAX_BATCH_REGIONS];
    strides[REG_PK] = schema.pk_stride();
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
    carve_at(data, &strides, nr, &offsets, rows)
}

/// The layout-taking core of [`carve_writer_slices`], for callers that already
/// computed `(strides, nr, offsets)` for the same arena (`write_to_batch`).
#[allow(clippy::type_complexity)]
fn carve_at<'a>(
    data: &'a mut [u8],
    strides: &[u8; MAX_BATCH_REGIONS],
    nr: usize,
    offsets: &[usize; MAX_BATCH_REGIONS],
    rows: usize,
) -> (&'a mut [u8], &'a mut [u8], &'a mut [u8], Vec<&'a mut [u8]>) {
    // Walk regions in order, splitting off [alignment pad | region] for each.
    // `base` tracks the absolute offset of `rest[0]` within `data`, so
    // `offsets[r] - base` is the padding to discard before region `r`. The three
    // fixed regions land in their own bindings and only the payload columns
    // accumulate into the returned Vec.
    let mut pk: Option<&mut [u8]> = None;
    let mut weight: Option<&mut [u8]> = None;
    let mut null_bmp: Option<&mut [u8]> = None;
    let mut col_slices: Vec<&mut [u8]> = Vec::with_capacity(nr.saturating_sub(3));
    let mut rest: &mut [u8] = data;
    let mut base = 0usize;
    for r in 0..nr {
        let pad = offsets[r] - base;
        let after_pad = std::mem::take(&mut rest).split_at_mut(pad).1;
        let sz = rows * strides[r] as usize;
        let (region, remainder) = after_pad.split_at_mut(sz);
        match r {
            REG_PK => pk = Some(region),
            REG_WEIGHT => weight = Some(region),
            REG_NULL_BMP => null_bmp = Some(region),
            _ => col_slices.push(region),
        }
        base = offsets[r] + sz;
        rest = remainder;
    }

    (
        pk.expect("REG_PK"),
        weight.expect("REG_WEIGHT"),
        null_bmp.expect("REG_NULL_BMP"),
        col_slices,
    )
}

/// Copy `count` rows of every region from `src` (regions at `src_offsets`)
/// into `dst` (regions at `dst_offsets`), one bulk copy per region. Shared by
/// the `reserve_rows` out-of-place grow, `clone_batch`, and the wire decode in
/// `batch_wire`.
///
/// # Safety
/// `src` and `dst` are distinct allocations; for every region `i < nr`, both
/// `src_offsets[i] + count * strides[i]` and `dst_offsets[i] + count *
/// strides[i]` are in bounds (both sides sized by `compute_offsets` for at
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

/// Cached row-layout guarantee. Ordered ladder `Raw < Sorted < Consolidated`,
/// where `Consolidated` implies `Sorted`, so `#[derive(Ord)]` makes
/// `is_sorted()` a `>= Sorted` test. A mutation can only *lower* it; the only
/// raise path is `certify_layout`, which debug-verifies the data first.
///
/// `pub(crate)` so callers name the variants, but the `Batch.layout` field is
/// private — only `certify_layout` / `inherit_layout` / `downgrade` (and
/// `set_weight`'s `Sorted` ceiling) mutate it.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub enum Layout {
    /// No order/fold guarantee.
    Raw,
    /// Rows are (PK, payload)-sorted (non-decreasing), but may carry unfolded
    /// duplicates or zero-weight ghosts.
    Sorted,
    /// Strictly (PK, payload)-increasing and ghost-free (weights folded). The
    /// `into_consolidated` / merge fast paths trust this to skip a re-fold.
    Consolidated,
}

impl Layout {
    /// This claim as wire flag bits. `Consolidated` normalizes to *both* bits,
    /// which is what makes [`Layout::from_wire_flags`] its inverse.
    pub fn to_wire_flags(self) -> u64 {
        match self {
            Layout::Raw => 0,
            Layout::Sorted => gnitz_wire::FLAG_BATCH_SORTED,
            Layout::Consolidated => gnitz_wire::FLAG_BATCH_SORTED | gnitz_wire::FLAG_BATCH_CONSOLIDATED,
        }
    }

    /// Recover a claim from wire flag bits. The `Batch` constructor already
    /// defaults `Raw`, so this is the value fed to `certify_layout` at the
    /// decode boundary (which debug-verifies the data against the claim).
    pub fn from_wire_flags(flags: u64) -> Layout {
        if flags & gnitz_wire::FLAG_BATCH_CONSOLIDATED != 0 {
            Layout::Consolidated
        } else if flags & gnitz_wire::FLAG_BATCH_SORTED != 0 {
            Layout::Sorted
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
/// A row is appended through [`BatchBuilder`], [`Batch::push_key_row`] or
/// [`Batch::push_zero_filled_row`]; the region writers and `count` are internal.
pub struct Batch {
    data: Vec<u8>,
    pub blob: Vec<u8>,
    // `usize`, not `u32`: a single large batch's cumulative region offset can
    // exceed 4 GB (see `compute_offsets`). In-memory only — never serialized.
    offsets: [usize; MAX_BATCH_REGIONS],
    strides: [u8; MAX_BATCH_REGIONS],
    /// Fixed-region count (pk, weight, null_bmp, payload…) — also the blob
    /// region's index in the wire/shard layout. Storage-visible so the serde
    /// side never re-derives it from the payload column count.
    pub(in crate::storage) num_regions: u8,
    capacity: u32,
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
    fn empty_from(strides: [u8; MAX_BATCH_REGIONS], num_regions: u8, schema: SchemaDescriptor) -> Self {
        Batch {
            data: Vec::new(),
            blob: Vec::new(),
            offsets: [0usize; MAX_BATCH_REGIONS],
            strides,
            num_regions,
            capacity: 0,
            count: 0,
            layout: Layout::Raw,
            schema,
            blob_id: next_blob_id(),
        }
    }

    /// Zero-allocation empty batch with strides pre-filled from `schema`.
    ///
    /// Use this when the caller intends to populate the batch via `extend_*`,
    /// `append_batch`, or similar.  Strides and `schema` are set up front so
    /// no one-shot realloc fires on the first column write.
    pub fn empty_with_schema(schema: &SchemaDescriptor) -> Self {
        let (strides, nr) = strides_from_schema(schema);
        Self::empty_from(strides, nr, *schema)
    }

    /// Zero-allocation empty batch with this batch's exact shape (strides,
    /// region count, schema) — honest for schema-less join-shaped batches too.
    /// The empty return / swap-placeholder constructor.
    pub(crate) fn empty_like(&self) -> Self {
        Self::empty_from(self.strides, self.num_regions, self.schema)
    }

    /// Move this batch out, leaving an `empty_like` placeholder behind — the
    /// VM register-swap idiom, with the slot's shape kept truthful.
    pub fn take(&mut self) -> Self {
        let empty = self.empty_like();
        std::mem::replace(self, empty)
    }

    /// An empty batch with schema, pre-allocated for `rows` rows. The arena is
    /// **not zero-filled** — the batch invariant is *every counted row writes
    /// every region*, so nothing may read a byte it has not written.
    ///
    /// Every writer already upholds it: each `append_*` writes the PK, weight,
    /// null and payload regions of each row it counts (a NULL cell takes
    /// `fill_col_zero`, not a skip), so does every [`super::merge::DirectWriter`]
    /// entry point, and every reader is `count`-bounded. The invariant is not
    /// optional either — [`Self::reserve_rows`] grows into an uninitialized
    /// arena, so a batch that outgrows its initial capacity would be on
    /// uninitialized memory from that row on regardless.
    ///
    /// Skipping the memset is worth 11–14% of scatter time
    /// (`write_to_batch_arena_provision_bench`), and more above
    /// `POOL_BYPASS_BYTES`, where every arena is a fresh allocation whose pages
    /// the memset would fault in.
    ///
    /// Publishing `count` before filling is fine (`capacity_writer` does it);
    /// leaving a counted cell unwritten is not — it holds recycled bytes,
    /// poisoned in debug builds (see [`debug_poison`]). A writer that wants a
    /// cell to read zero writes the zero.
    pub fn with_capacity(schema: SchemaDescriptor, rows: usize) -> Self {
        let cap = rows.max(1);
        let (strides, nr) = strides_from_schema(&schema);
        let (offsets, total_size) = compute_offsets(&strides, nr as usize, cap);
        let data = acquire_arena(total_size, Fill::Uninit);

        Batch {
            data,
            // No head start: every writer (the string relocator,
            // `share_blob_from`, the explicit `blob.reserve`s) grows on demand,
            // so a fixed 64-byte `with_capacity` only buys one growth step for a
            // string schema while charging every string-free batch a malloc.
            blob: Vec::new(),
            offsets,
            strides,
            num_regions: nr,
            capacity: cap as u32,
            count: 0,
            layout: Layout::Raw,
            schema,
            blob_id: next_blob_id(),
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
    pub fn zeroed(schema: SchemaDescriptor, rows: usize) -> Self {
        let (strides, nr) = strides_from_schema(&schema);
        let (offsets, total_size) = compute_offsets(&strides, nr as usize, rows.max(1));
        Batch {
            data: vec![0u8; total_size],
            blob: Vec::new(),
            offsets,
            strides,
            num_regions: nr,
            capacity: rows.max(1) as u32,
            count: rows,
            layout: Layout::Raw,
            schema,
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
        schema: SchemaDescriptor,
    ) -> Self {
        // `set_schema`'s shape check, applied where the buffers arrive rather
        // than one statement later.
        debug_assert_eq!(
            num_regions as usize - REG_PAYLOAD_START,
            schema.num_payload_cols(),
            "from_prebuilt: payload regions disagree with the schema",
        );
        Batch {
            data,
            blob,
            offsets,
            strides,
            num_regions,
            capacity: count as u32,
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
        self.schema = s;
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

    #[inline]
    pub(crate) fn pk_data(&self) -> &[u8] {
        let off = self.offsets[REG_PK];
        &self.data[off..off + self.count * self.strides[REG_PK] as usize]
    }
    #[inline]
    pub(crate) fn weight_data(&self) -> &[u8] {
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
    pub(crate) fn num_payload_cols(&self) -> usize {
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
    pub(crate) fn pk_data_mut(&mut self) -> &mut [u8] {
        let off = self.offsets[REG_PK];
        let end = off + self.count * self.strides[REG_PK] as usize;
        &mut self.data[off..end]
    }
    #[inline]
    pub(crate) fn weight_data_mut(&mut self) -> &mut [u8] {
        let off = self.offsets[REG_WEIGHT];
        let end = off + self.count * 8;
        &mut self.data[off..end]
    }
    #[inline]
    pub(crate) fn null_bmp_data_mut(&mut self) -> &mut [u8] {
        let off = self.offsets[REG_NULL_BMP];
        let end = off + self.count * 8;
        &mut self.data[off..end]
    }
    #[inline]
    pub(crate) fn col_data_mut(&mut self, pi: usize) -> &mut [u8] {
        let r = REG_PAYLOAD_START + pi;
        let off = self.offsets[r];
        let end = off + self.count * self.strides[r] as usize;
        &mut self.data[off..end]
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
        let n_end = n_off + self.count * 8;
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
        gnitz_wire::widen_pk_be(&self.data[off..off + stride], stride)
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
        read_i64_le(&self.data[self.offsets[REG_WEIGHT]..], row * 8)
    }
    /// Read one row's value from a fixed 8-byte payload column, 0 when the
    /// region is short (defensive against a truncated wire batch).
    pub fn read_payload_u64(&self, row: usize, pi: usize) -> u64 {
        let off = row * 8;
        let col = self.col_data(pi);
        if off + 8 > col.len() {
            return 0;
        }
        u64::from_le_bytes(col[off..off + 8].try_into().unwrap_or([0; 8]))
    }
    /// Read one row's bytes from a German-string (STRING or BLOB) payload
    /// column; empty on a short region or a malformed descriptor.
    pub fn read_payload_bytes(&self, row: usize, pi: usize) -> &[u8] {
        let off = row * 16;
        let data = self.col_data(pi);
        if off + 16 > data.len() {
            return &[];
        }
        gnitz_wire::german_string_content(&data[off..off + 16], &self.blob)
    }
    /// [`Self::read_payload_bytes`] as a `String`; empty when not UTF-8.
    pub fn read_payload_string(&self, row: usize, pi: usize) -> String {
        String::from_utf8(self.read_payload_bytes(row, pi).to_vec()).unwrap_or_default()
    }
    /// Apply `f` to every row's weight in place. Generic so the per-epoch
    /// callers (negate, delta doubling) monomorphize to a tight loop. The
    /// layout tag is untouched: callers pass sign-preserving maps (negation,
    /// ×2, non-zero clamping) that cannot mint ghosts or fold duplicates.
    #[inline]
    pub fn map_weights(&mut self, f: impl Fn(i64) -> i64) {
        let off = self.offsets[REG_WEIGHT];
        for chunk in self.data[off..off + self.count * 8].chunks_exact_mut(8) {
            let w = i64::from_le_bytes(chunk.try_into().unwrap());
            chunk.copy_from_slice(&f(w).to_le_bytes());
        }
    }
    /// Overwrite every row's weight with `weights`, one per row in row order.
    /// Arbitrary weights can mint ghosts, so the layout claim is dropped.
    pub fn overwrite_weights(&mut self, weights: &[i64]) {
        debug_assert_eq!(weights.len(), self.count, "overwrite_weights: one weight per row");
        for (dst, w) in self.weight_data_mut().chunks_exact_mut(8).zip(weights) {
            dst.copy_from_slice(&w.to_le_bytes());
        }
        self.downgrade();
    }
    /// Summed weight of rows `[start, end)`, read straight off the contiguous
    /// weight region so the fold vectorizes (rather than a `get_weight` per row).
    #[inline]
    pub(crate) fn sum_weights(&self, start: usize, end: usize) -> i64 {
        self.weight_data()[start * 8..end * 8]
            .chunks_exact(8)
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
            .chunks_exact(8)
            .fold(false, |bad, w| bad | (i64::from_le_bytes(w.try_into().unwrap()) <= 0))
    }
    #[inline(always)]
    pub fn get_null_word(&self, row: usize) -> u64 {
        read_u64_le(&self.data[self.offsets[REG_NULL_BMP]..], row * 8)
    }
    /// Overwrite `row`'s null-bitmap word (bit N = payload slot N is NULL).
    #[inline]
    pub(crate) fn set_null_word(&mut self, row: usize, word: u64) {
        let off = self.offsets[REG_NULL_BMP] + row * 8;
        self.data[off..off + 8].copy_from_slice(&word.to_le_bytes());
    }
    #[inline(always)]
    pub fn get_col_ptr(&self, row: usize, payload_col: usize, col_size: usize) -> &[u8] {
        let off = self.offsets[REG_PAYLOAD_START + payload_col] + row * col_size;
        &self.data[off..off + col_size]
    }

    // ── Extend methods (building batches row-by-row) ────────────────────

    /// Ensure the data buffer has room for at least one more row.
    #[inline]
    pub(crate) fn ensure_row_capacity(&mut self) {
        self.reserve_rows(1);
    }

    /// Ensure the data buffer has room for at least `n` more rows beyond `count`.
    /// `#[inline]` for the already-has-room test, which is the whole call on
    /// every append but the growing one.
    #[inline]
    pub fn reserve_rows(&mut self, n: usize) {
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
            // Zeroing is not needed (`Uninit`): `copy_regions` fills every live
            // byte, and all accessors are bounded by `count`.
            let mut new_data = acquire_arena(new_total, Fill::Uninit);
            // SAFETY: distinct allocations; both sides sized per compute_offsets.
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
        self.capacity = new_cap as u32;
    }

    /// Write data into region `r` at the current row position.
    /// Auto-grows capacity if needed.  Strides must be set at construction —
    /// use `empty_with_schema` or `with_capacity`.
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
        self.extend_pk_bytes(pk);
        self.extend_weight(&weight.to_le_bytes());
        self.extend_null_bmp(&null_word.to_le_bytes());
        for pi in 0..self.num_payload_cols() {
            let width = self.strides[REG_PAYLOAD_START + pi] as usize;
            self.fill_col_zero(pi, width);
        }
        self.count += 1;
    }

    /// Append a row's PK from native per-column values, OPK-encoding them
    /// (big-endian, with the sign-bit flip for signed columns) before the
    /// bytes are written. `native_col_vals` holds one native value per PK
    /// column in `pk_columns()` order. Use for signed or compound PK test
    /// tables where `extend_pk` (no sign flip) writes incorrect OPK bytes.
    ///
    /// Encodes through the production `schema::key` encoder — a layer *below*
    /// storage — so this stays a downward edge even though only tests call it.
    pub fn extend_pk_opk(&mut self, schema: &SchemaDescriptor, native_col_vals: &[u128]) {
        let cols = schema.pk_columns().map(|(_, col)| (col.type_code, *col));
        self.extend_pk_bytes(crate::schema::key::encode_leading_opk(cols, native_col_vals).pk_bytes());
    }

    /// Fill `nbytes` of zeros at the current row position in a payload column.
    #[inline]
    pub(crate) fn fill_col_zero(&mut self, pi: usize, nbytes: usize) {
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
        is_string_at: &[bool; MAX_BATCH_REGIONS],
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
        let no_strings = [false; MAX_BATCH_REGIONS];
        let is_string_at = if shares_blob { &no_strings } else { is_string_at };
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
            for (pi, &is_str) in is_string_at[..npc].iter().enumerate() {
                let cs = self.strides[REG_PAYLOAD_START + pi] as usize;
                if is_str && cs == 16 {
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
/// That setup — the payload string-column map and one pooled blob dedup cache —
/// is resolved once and reused by every push, which is what makes a *run*-at-a-
/// time appender viable. `op_union`'s merge is the extreme: for a set operation
/// the branches carry `reindex_hash_row` synthetic PKs, so two sorted streams of
/// uniform 128-bit keys have an expected run length of 2 and the setup dominated
/// the copy it was setting up.
pub(crate) struct AppendSession<'d> {
    dst: &'d mut Batch,
    /// Payload slot → is a German string. Read only when the source's blob is not
    /// already the destination's (a shared blob copies the 16-byte structs
    /// verbatim), which each push re-decides per source.
    is_string_at: [bool; MAX_BATCH_REGIONS],
    guard: BlobCacheGuard,
}

impl<'d> AppendSession<'d> {
    fn string_map(dst: &Batch) -> [bool; MAX_BATCH_REGIONS] {
        let mut is_string_at = [false; MAX_BATCH_REGIONS];
        let npc = dst.num_payload_cols();
        for (pi, col) in dst.schema.payload_columns() {
            if pi >= npc {
                break;
            }
            is_string_at[pi] = gnitz_wire::is_german_string(col.type_code);
        }
        is_string_at
    }

    /// The session holds a pooled blob dedup cache for its whole life, so
    /// repeated long-string spans are appended once across every push.
    pub(crate) fn open(dst: &'d mut Batch, hint_rows: usize) -> Self {
        let is_string_at = Self::string_map(dst);
        let guard = BlobCacheGuard::acquire(&dst.schema, hint_rows);
        AppendSession {
            dst,
            is_string_at,
            guard,
        }
    }

    /// Append rows `[start, end)` of `src`.
    pub(crate) fn push_range(&mut self, src: &MemBatch<'_>, start: usize, end: usize) {
        self.push_ranges(src, &[(start, end)]);
    }

    /// Append every listed range of `src`, in list order.
    pub(crate) fn push_ranges(&mut self, src: &MemBatch<'_>, ranges: &[(usize, usize)]) {
        // Destructured so the disjoint fields lend one borrow each, rather than
        // copying the 68-byte map through `self` on every push.
        let Self {
            dst,
            is_string_at,
            guard,
        } = self;
        dst.append_ranges_inner(src, ranges, is_string_at, guard.get_mut());
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
    /// a few reads through it, and release builds have no LTO and use default
    /// codegen-units, so the hint is what carries the inline across the module
    /// boundary.
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

    /// True if the rows are (PK, payload)-sorted. An empty batch is sorted
    /// structurally (no pair can be out of order), independent of the cached tag —
    /// so the constructor default of `Raw` needs no per-reader special-casing.
    #[inline]
    pub fn is_sorted(&self) -> bool {
        self.count == 0 || self.layout >= Layout::Sorted
    }

    /// True if the rows are consolidated (strictly (PK, payload)-increasing and
    /// ghost-free). An empty batch is consolidated structurally.
    #[inline]
    pub fn is_consolidated(&self) -> bool {
        self.count == 0 || self.layout == Layout::Consolidated
    }

    /// `is_sorted()`, additionally asserting in debug builds that the data really
    /// is (PK, payload)-sorted whenever the cached tag claims it. Prefer this over
    /// `is_sorted()` at any skip-point that trusts the claim to avoid a re-sort: a
    /// lying tag is caught here, exactly where it would otherwise cause silent
    /// weight errors.
    #[cfg_attr(not(debug_assertions), allow(unused_variables))]
    #[inline]
    pub(crate) fn sorted_verified(&self, schema: &SchemaDescriptor) -> bool {
        #[cfg(debug_assertions)]
        if self.layout >= Layout::Sorted {
            self.debug_verify_sorted(schema);
        }
        self.is_sorted()
    }

    /// `is_consolidated()`, additionally asserting in debug builds that the data
    /// really is consolidated whenever the cached tag claims it. Prefer this over
    /// `is_consolidated()` at any skip-point that trusts the claim to avoid a
    /// re-fold.
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
            Layout::Sorted => self.debug_verify_sorted(schema),
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
    /// order every merge/consolidation path sorts by. Shared by both verifiers.
    #[cfg(debug_assertions)]
    fn adjacent_pair_ord(&self, schema: &SchemaDescriptor, i: usize) -> std::cmp::Ordering {
        use super::columnar::compare_rows;
        use crate::schema::key::compare_pk_bytes;
        compare_pk_bytes(self.get_pk_bytes(i), self.get_pk_bytes(i + 1))
            .then_with(|| compare_rows(schema, self, i, self, i + 1))
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

    /// Debug-only: assert the data is sorted by (PK, payload) — non-decreasing,
    /// adjacent ties permitted. The `sorted` contract, nothing more.
    #[cfg(debug_assertions)]
    fn debug_verify_sorted(&self, schema: &SchemaDescriptor) {
        for i in 0..self.count.saturating_sub(1) {
            debug_assert_ne!(
                self.adjacent_pair_ord(schema, i),
                std::cmp::Ordering::Greater,
                "batch flagged sorted, but row {i} > row {} by (PK, payload)",
                i + 1
            );
        }
    }

    /// Debug-only: assert the data is fully consolidated — strictly increasing by
    /// (PK, payload) (no unfolded duplicate) AND no zero-weight row (ghost
    /// eliminated, §2). Subsumes `debug_verify_sorted`.
    #[cfg(debug_assertions)]
    pub(crate) fn debug_verify_consolidated(&self, schema: &SchemaDescriptor) {
        for i in 0..self.count {
            debug_assert_ne!(
                self.get_weight(i),
                0,
                "batch flagged consolidated, but row {i} has weight 0 (ghost not eliminated)"
            );
            if i + 1 < self.count {
                debug_assert_eq!(
                    self.adjacent_pair_ord(schema, i),
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
        let nr = self.num_regions as usize;
        let mut total = self.blob.len();
        for i in 0..nr {
            total += self.count * self.strides[i] as usize;
        }
        total
    }

    /// Scatter-copy selected rows from a MemBatch into a new Batch, each carrying
    /// its own weight. A caller emitting *different* weights follows with
    /// [`overwrite_weights`](Self::overwrite_weights) — one sequential blit,
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

    /// The three regions a widen and a stamp copy identically: the blob heap, the
    /// weights, and every payload column of `in_schema`. Each of the two rewrites
    /// exactly one more region — the widen the NULL words, the stamp the PK — so
    /// what they share is one call rather than forty duplicated lines that would
    /// have to be kept in step through every change to the region layout.
    ///
    /// `self.count` must already be the source's row count and the two schemas
    /// must agree on payload indices; the caller states the source schema because
    /// a `Batch` carries only its region strides.
    fn take_blob_weights_and_payload(&mut self, src: &Batch, in_schema: &SchemaDescriptor) {
        // Share the input heap so long (> 12 byte) STRING/BLOB values, whose
        // 16-byte structs are copied verbatim below, still resolve. Sharing an
        // empty blob is a no-op, so no emptiness guard.
        if in_schema.has_german_string() {
            self.share_blob_from(src);
        }
        self.weight_data_mut().copy_from_slice(src.weight_data());
        let n = src.count;
        for (pi, col) in in_schema.payload_columns() {
            let stride = col.size() as usize;
            self.col_data_mut(pi).copy_from_slice(&src.col_data(pi)[..n * stride]);
        }
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
        let in_stride = in_schema.pk_stride() as usize;
        let out_stride = out_schema.pk_stride() as usize;
        debug_assert_eq!(out_stride, in_stride + 8);
        debug_assert_eq!(out_schema.num_payload_cols(), in_schema.num_payload_cols());
        let n = self.count;
        if n == 0 {
            return Self::empty_with_schema(out_schema);
        }

        let mut output = Self::with_capacity(*out_schema, n);
        output.count = n;
        output.take_blob_weights_and_payload(self, in_schema);
        output.null_bmp_data_mut().copy_from_slice(self.null_bmp_data());

        let stamp = prefix.to_be_bytes();
        let src_pk = self.pk_data();
        for (dst, src) in output
            .pk_data_mut()
            .chunks_exact_mut(out_stride)
            .zip(src_pk.chunks_exact(in_stride))
        {
            dst[..8].copy_from_slice(&stamp);
            dst[8..].copy_from_slice(src);
        }

        output.inherit_layout(self);
        output
    }

    /// Copy every row into `out_schema`, dropping the eight leading big-endian
    /// bytes [`Self::stamped_with_pk_prefix`] wrote — the inverse of that call,
    /// with the schemas swapped.
    ///
    /// **It is not that one's mirror image in the layout claim, and that is what
    /// makes the ingest below it correct.** The stamp inherits the input's claim
    /// because prepending one constant to every key preserves sortedness and
    /// distinctness. *Removing* the prefix preserves neither once the batch spans
    /// more than one stamp value: the rows are then ordered by stamp first, so
    /// round 5's key 100 sits before round 6's key 3, and the same
    /// `(key, payload)` element legitimately appears under two stamps. So the
    /// result claims `Layout::Raw`, and the ingest's sort-and-fold is what sums
    /// those weights onto the right element. An inherited `Consolidated` claim
    /// would make `into_consolidated` a no-op and push an unsorted,
    /// duplicate-carrying run straight into the memtable, where the RunSet merge
    /// reads each run linearly and accumulates weights against the wrong element
    /// — with no error outside `#[cfg(debug_assertions)]`.
    pub fn stripped_of_pk_prefix(&self, in_schema: &SchemaDescriptor, out_schema: &SchemaDescriptor) -> Self {
        let in_stride = in_schema.pk_stride() as usize;
        let out_stride = out_schema.pk_stride() as usize;
        debug_assert_eq!(in_stride, out_stride + 8);
        debug_assert_eq!(out_schema.num_payload_cols(), in_schema.num_payload_cols());
        let n = self.count;
        if n == 0 {
            return Self::empty_with_schema(out_schema);
        }

        let mut output = Self::with_capacity(*out_schema, n);
        output.count = n;
        output.take_blob_weights_and_payload(self, in_schema);
        output.null_bmp_data_mut().copy_from_slice(self.null_bmp_data());

        let src_pk = self.pk_data();
        for (dst, src) in output
            .pk_data_mut()
            .chunks_exact_mut(out_stride)
            .zip(src_pk.chunks_exact(in_stride))
        {
            dst.copy_from_slice(&src[8..]);
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

        let mut output = Self::with_capacity(*out_schema, n);
        output.count = n;
        output.take_blob_weights_and_payload(self, in_schema);
        output.pk_data_mut().copy_from_slice(self.pk_data());

        // A NULL cell is zero — the invariant `DirectWriter::write_row` and
        // `BatchBuilder::put_null` uphold actively. Zeroing just the appended
        // columns keeps every counted row fully written without provisioning the
        // whole arena zeroed (see `Batch::with_capacity`).
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
        if self.holds_nothing() {
            return Batch {
                data: Vec::new(),
                blob: Vec::new(),
                offsets: [0usize; MAX_BATCH_REGIONS],
                strides: self.strides,
                num_regions: self.num_regions,
                capacity: 0,
                count: 0,
                layout: self.layout,
                schema: self.schema,
                blob_id: self.blob_id,
            };
        }
        // Only clone the actually-used portion of data (count-based, not capacity-based).
        let nr = self.num_regions as usize;
        let (packed_offsets, packed_size) = compute_offsets(&self.strides, nr, self.count);
        let mut new_data = acquire_arena(packed_size, Fill::Uninit);
        // SAFETY: distinct allocations; `new_data` sized per compute_offsets.
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
            num_regions: self.num_regions,
            capacity: self.count as u32,
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

    /// First row index past the equal-PK group beginning at `start`. Requires
    /// `start < self.count`. A linear step, not a seek: the co-group callers
    /// reach it having just landed on the group's first row, where the group is
    /// short and [`advance_to`](Self::advance_to)'s gallop would cost more.
    #[inline]
    pub(crate) fn pk_group_end(&self, start: usize) -> usize {
        let k = self.get_pk_bytes(start);
        let mut j = start + 1;
        while j < self.count && crate::schema::key::pk_bytes_eq(self.get_pk_bytes(j), k) {
            j += 1;
        }
        j
    }

    /// Append all of `src`, relocating German-string blob data into `self`'s
    /// heap. The full-range decode/accumulate entry point (W2M ingest, the
    /// master's index-scan merge).
    pub fn append_mem_batch(&mut self, src: &MemBatch<'_>) {
        self.append_session(src.count).push_range(src, 0, src.count);
    }

    /// Bulk-copy rows [start, end) from another Batch (same schema).
    ///
    /// `self` must have strides pre-set (see `empty_with_schema` / `with_capacity`).
    pub fn append_batch(&mut self, src: &Batch, start: usize, end: usize) {
        let end = end.min(src.count);
        if start >= end {
            return;
        }
        self.append_session(end - start)
            .push_range(&src.as_mem_batch(), start, end);
    }

    /// Bulk-copy every `[start, end)` range of `src`, in list order, onto this
    /// batch's tail. The whole-list form of [`Self::append_batch`]: one capacity
    /// reserve, one string-column map, and one blob dedup cache for the entire
    /// survivor list of a filter pass, instead of one per range.
    ///
    /// Call [`Self::share_blob_from`] first to skip per-cell string relocation
    /// (see `append_ranges_inner`); it is a pure optimization, correct either
    /// way.
    pub(crate) fn append_ranges(&mut self, src: &Batch, ranges: &[(usize, usize)]) {
        self.append_session(range_rows(ranges))
            .push_ranges(&src.as_mem_batch(), ranges);
    }

    /// Whether copying `row_count` rows out of a `src_count`-row source whose heap
    /// is `src_blob_len` bytes should relocate the slice's own string cells rather
    /// than carry the source's whole heap.
    ///
    /// The two arms cost: relocation, one cell rewrite per row plus the slice's own
    /// share of the heap (`src_blob_len / src_count` per row); the whole-heap copy,
    /// `src_blob_len` regardless of how few rows are kept. Expressing the per-cell
    /// rewrite as [`RELOCATE_CELL_COST_BYTES`] of memcpy makes that one comparison.
    ///
    /// Only worth consulting where both arms are available — a destination that
    /// cannot carry the source's heap verbatim (different blob identity, gathered
    /// rather than contiguous rows, a result that is shipped) must relocate
    /// regardless.
    pub(crate) fn should_relocate_blob(row_count: usize, src_count: usize, src_blob_len: usize) -> bool {
        src_count > 0 && src_blob_len > row_count.saturating_mul(RELOCATE_CELL_COST_BYTES + src_blob_len / src_count)
    }

    /// Gather every `[start, end)` row range of `src`, in list order, into a fresh
    /// batch — a filter pass's survivor list, or one slice of a RAM-tier run.
    ///
    /// Disjoint ascending ranges (debug-checked) make the result a subset *in source
    /// order*, which is what lets it inherit `src`'s layout tag; an overlap would
    /// repeat a row and break the distinctness half of a `Consolidated` claim. The
    /// blob arm is [`should_relocate_blob`](Self::should_relocate_blob)'s call.
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
        let mut out = Batch::with_capacity(*schema, rows);
        if !Batch::should_relocate_blob(rows, src.count, src.blob.len()) {
            out.share_blob_from(src);
        }
        out.append_ranges(src, ranges);
        // `append_ranges` downgraded `out` to `Raw` first.
        out.inherit_layout(src);
        out
    }

    /// Append a single row from raw C-style region pointers.
    ///
    /// # Safety
    /// `col_ptrs[i]` must point to at least `col_sizes[i]` readable bytes for
    /// every non-null, non-STRING column.  For STRING columns the pointer must
    /// point to a 16-byte German String struct.  `blob_src` must contain the
    /// blob bytes referenced by any long-string structs.
    ///
    /// `#[cfg(test)]`, so it is compiled out of every consumer's build: the
    /// production append paths go through `AppendSession`.
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
        self.ensure_row_capacity();
        self.extend_pk(pk);
        self.extend_weight(&weight.to_le_bytes());
        self.extend_null_bmp(&null_word.to_le_bytes());

        let schema = self.schema;

        // `pi` is the dense payload index; it equals `enumerate`'s counter.
        for (pi, (ptr, &sz)) in col_ptrs.iter().zip(col_sizes.iter()).enumerate() {
            let ci = schema.payload_col_idx(pi);
            let type_code = if ci < schema.num_columns() {
                schema.columns[ci].type_code
            } else {
                0
            };
            let is_null = gnitz_wire::null_word_get(null_word, pi);
            let col_size = sz as usize;
            let cell = (!is_null).then(|| std::slice::from_raw_parts(*ptr, col_size));
            self.append_payload_cell(pi, type_code, col_size, cell, blob_src, None);
        }

        self.count += 1;
        self.downgrade();
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
            for (&start, &stride) in self.offsets[..self.num_regions as usize]
                .iter()
                .zip(&self.strides[..self.num_regions as usize])
            {
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
        // Reuse the pooled destination buffer rather than dropping it for a
        // fresh exact-sized clone (an allocation per call). The blob bytes are
        // identical, so the shared blob_id and every German-string offset stay
        // valid — a behavioral no-op apart from the saved allocation.
        self.blob.clear();
        self.blob.extend_from_slice(&src.blob);
        self.blob_id = src.blob_id;
    }

    /// Number of regions in the standard layout (including blob).
    pub(crate) fn num_regions_total(&self) -> usize {
        self.num_regions as usize + 1
    }

    /// Safe `&[u8]` view of region `idx` — `count * stride` bytes for a fixed
    /// region, the whole heap for the trailing blob — for callers that frame the
    /// batch into a byte buffer (`batch_wire`'s wire encoders) or hand it to the
    /// shard writer. The region copy stays bounds-checked, no raw pointers.
    pub(crate) fn region_slice(&self, idx: usize) -> &[u8] {
        let blob_idx = self.num_regions as usize;
        if idx < blob_idx {
            let off = self.offsets[idx];
            &self.data[off..off + self.count * self.strides[idx] as usize]
        } else if idx == blob_idx {
            &self.blob
        } else {
            panic!(
                "region_slice: index {idx} out of range (num_regions_total = {})",
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

    /// All regions as bounds-checked byte slices in canonical order (pk, weight,
    /// null, payload…, blob) — the safe region view the shard writer consumes.
    /// Mapping [`Batch::region_slice`] over every region, blob included.
    pub(crate) fn regions(&self) -> Vec<&[u8]> {
        (0..self.num_regions_total()).map(|i| self.region_slice(i)).collect()
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
    fn append_row_tail_from_source<S: RowSource>(
        &mut self,
        weight: i64,
        source: &S,
        row: usize,
        mut blob_cache: Option<&mut BlobCache>,
    ) {
        self.extend_weight(&weight.to_le_bytes());
        let null_word = source.get_null_word(row);
        self.extend_null_bmp(&null_word.to_le_bytes());

        // Walks this batch's own schema by index, re-reading the 4-byte
        // `SchemaColumn` per column, rather than calling the shared
        // `append_payload_cols`: that takes the schema by reference, which the
        // `&mut self` cell writes below would alias, and copying the descriptor
        // out to dodge that puts a 424-byte `memcpy` on this per-row path. The
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

        self.count += 1;
        self.downgrade();
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

    /// Sort (if needed) and weight-fold `batch` into a fresh certified batch —
    /// the consolidation slow path both entry points above share, so the two can
    /// never diverge on how the output arena is provisioned or which merge kernel
    /// runs.
    fn consolidate_into_new(batch: &Batch, schema: &SchemaDescriptor) -> Batch {
        let already_sorted = batch.sorted_verified(schema);
        let mb = batch.as_mem_batch();
        let blob_cap = mb.blob.len().max(1);
        let mut result = write_to_batch(schema, batch.count, blob_cap, |writer| {
            if already_sorted {
                merge::fold_sorted(&mb, schema, writer);
            } else {
                merge::sort_and_consolidate(&mb, schema, writer);
            }
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
/// counts, and every reader — accessor, `region_slice`, `regions`, `total_bytes`
/// — bounds the batch to `count`, so the `[count, capacity)` tail and the
/// inter-region alignment padding are never read and never serialized.
pub(crate) fn write_to_batch(
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
    let mut data = acquire_arena(arena_size, Fill::Uninit);
    // DirectWriter grows blob length via `extend_from_slice`; reserve capacity
    // up front but do not zero-fill.
    let mut blob = acquire_arena(max_blob, Fill::Reserve);

    let actual_rows;
    {
        let (pk, weight, null_bmp, col_slices) = carve_at(&mut data, &strides, nr, &offsets, max_rows);
        let mut writer = merge::DirectWriter::new(pk, weight, null_bmp, col_slices, &mut blob, schema, max_rows);
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
        layout: Layout::Raw,
        schema: *schema,
    }
}

// ---------------------------------------------------------------------------
// BatchBuilder — construct Batch rows for system table mutations
//
// A pure storage utility: it holds no catalog state and builds a `Batch`
// row-by-row from a schema, so it lives here with `Batch`. Re-exported from
// `catalog` for its DDL/bootstrap/store callers; `gnitz-server`'s executor and
// the `compiler` tests import it from `storage` directly.
// ---------------------------------------------------------------------------

/// Lightweight row-by-row builder for constructing Batch in Rust.
/// Operates on Batch directly; the schema lives on the batch itself.
pub struct BatchBuilder {
    pub(crate) batch: Batch,
    // per-row state
    pub(crate) curr_null_word: u64,
    pub(crate) curr_col: usize,
}

/// The engine half of the shared catalog row codecs: the sink
/// `gnitz_wire::sys_rows` writes a system-table row into.
impl gnitz_wire::sys_rows::SysRowSink for BatchBuilder {
    fn begin_row(&mut self, pk: &[u128], weight: i64) {
        BatchBuilder::begin_row_opk(self, pk, weight);
    }
    fn put_u64(&mut self, v: u64) {
        BatchBuilder::put_u64(self, v);
    }
    fn put_string(&mut self, s: &str) {
        BatchBuilder::put_string(self, s);
    }
    fn put_bytes(&mut self, b: &[u8]) {
        BatchBuilder::put_blob(self, b);
    }
    fn put_null(&mut self) {
        BatchBuilder::put_null(self);
    }
    fn end_row(&mut self) {
        BatchBuilder::end_row(self);
    }
}

impl BatchBuilder {
    pub fn new(schema: SchemaDescriptor) -> Self {
        BatchBuilder {
            // Uninitialized, like every batch arena: every row writes every
            // column (`put_null` zero-fills rather than skipping).
            batch: Batch::with_capacity(schema, 8),
            curr_null_word: 0,
            curr_col: 0,
        }
    }

    /// Begin a new row with the given single-column PK and weight.
    pub fn begin_row(&mut self, pk: u128, weight: i64) {
        self.batch.ensure_row_capacity();
        self.batch.extend_pk(pk);
        self.begin_row_tail(weight);
    }

    /// [`Self::begin_row`] for a **compound** PK: `natives` are the PK columns'
    /// native values in PK-list order, OPK-encoded into the packed PK region.
    /// Without this, every compound-PK test hand-rolls the
    /// `ensure_row_capacity`/`extend_pk_opk`/`extend_weight`/…/`count += 1`
    /// protocol — and a native-LE concatenation into `extend_pk_bytes` (the
    /// obvious wrong spelling) is not the at-rest form at all.
    pub fn begin_row_opk(&mut self, natives: &[u128], weight: i64) {
        self.batch.ensure_row_capacity();
        let schema = *self.schema();
        self.batch.extend_pk_opk(&schema, natives);
        self.begin_row_tail(weight);
    }

    /// [`Self::begin_row`] for a PK already in its at-rest OPK image: `pk` is
    /// written verbatim and must be exactly `pk_stride` bytes. For a caller
    /// holding native column values, [`Self::begin_row_opk`] encodes them.
    pub fn begin_row_bytes(&mut self, pk: &[u8], weight: i64) {
        self.batch.ensure_row_capacity();
        self.batch.extend_pk_bytes(pk);
        self.begin_row_tail(weight);
    }

    fn begin_row_tail(&mut self, weight: i64) {
        self.batch.extend_weight(&weight.to_le_bytes());
        self.curr_null_word = 0;
        self.curr_col = 0;
    }

    /// Put an integer value for the current payload column, in that column's own
    /// width — so a value and its column cannot be mismatched, the shape
    /// [`Self::put_null`] below already has.
    ///
    /// Signed values are passed as `v as u128`: sign-extending to 128 bits leaves
    /// the low `size()` bytes exactly the two's-complement image the column
    /// holds. That is the same native convention [`Batch::extend_pk_opk`] takes
    /// for PK columns.
    pub fn put_int(&mut self, val: u128) {
        let col_size = self.schema().columns[self.physical_col_idx()].size() as usize;
        debug_assert!(
            col_size == 16 || {
                // The bytes about to be dropped must carry no information: all
                // zero for an unsigned value, all one for a sign-extended
                // negative. Anything else is a value too wide for its column.
                let dropped = val >> (col_size * 8);
                dropped == 0 || dropped == u128::MAX >> (col_size * 8)
            },
            "put_int: {val:#x} does not fit the column's {col_size} bytes",
        );
        self.batch.extend_col(self.curr_col, &val.to_le_bytes()[..col_size]);
        self.curr_col += 1;
    }

    /// [`Self::put_int`] under the name `SysRowSink` requires; every system-table
    /// payload column is a U64.
    pub fn put_u64(&mut self, val: u64) {
        self.put_int(val as u128);
    }

    /// Put a float for the current payload column, narrowed to that column's own
    /// width — the same value-fits-its-column shape [`Self::put_int`] has. An F32
    /// column stores the `as f32` narrowing, so a caller need not know the width.
    #[cfg(test)]
    pub(crate) fn put_float(&mut self, val: f64) {
        let col_size = self.schema().columns[self.physical_col_idx()].size() as usize;
        match col_size {
            4 => self.batch.extend_col(self.curr_col, &(val as f32).to_le_bytes()),
            _ => self.batch.extend_col(self.curr_col, &val.to_le_bytes()),
        }
        self.curr_col += 1;
    }

    /// Put raw bytes for the current STRING/BLOB payload column — the one
    /// German-string encode site; `read_german_bytes` is the read-back twin.
    pub fn put_blob(&mut self, b: &[u8]) {
        let st = gnitz_wire::encode_german_string(b, &mut self.batch.blob);
        self.batch.extend_col(self.curr_col, &st);
        self.curr_col += 1;
    }

    /// Put a string value for the current payload column.
    pub fn put_string(&mut self, s: &str) {
        self.put_blob(s.as_bytes());
    }

    /// Put a NULL value for the current payload column.
    pub fn put_null(&mut self) {
        let col_size = self.schema().columns[self.physical_col_idx()].size() as usize;
        self.batch.fill_col_zero(self.curr_col, col_size);
        gnitz_wire::null_word_set(&mut self.curr_null_word, self.curr_col, true);
        self.curr_col += 1;
    }

    /// Finish the current row (writes null bitmap). The batch stays `Raw` (its
    /// constructor default; `extend_*` never raises the layout).
    pub fn end_row(&mut self) {
        // Nothing else notices a row that skipped a column: the count still
        // advances and the short region keeps whatever bytes were there.
        debug_assert_eq!(
            self.curr_col,
            self.schema().num_payload_cols(),
            "BatchBuilder row got {} of {} payload columns",
            self.curr_col,
            self.schema().num_payload_cols(),
        );
        self.batch.extend_null_bmp(&self.curr_null_word.to_le_bytes());
        self.batch.count += 1;
    }

    /// Consume the builder, returning the built batch.
    pub fn finish(self) -> Batch {
        self.batch
    }

    fn schema(&self) -> &SchemaDescriptor {
        &self.batch.schema
    }

    fn physical_col_idx(&self) -> usize {
        self.schema().payload_col_idx(self.curr_col)
    }
}

#[cfg(test)]
#[path = "tests/batch.rs"]
mod tests;
