//! In-memory merge for run-set consolidation, and the two-way batch merge.
//!
//! Operates on flat columnar buffers: pk[OPK big-endian, `pk_stride` B/row],
//! weight[i64 LE], null_bitmap[u64 LE], payload columns, blob arena.
//!
//! The N-way merge is a fused k-way merge + inline consolidation: rows with the
//! same (PK, payload) have their weights summed; rows whose net weight is zero
//! are dropped. [`Batch::merged_consolidated`] is the two-input counterpart over
//! the same comparator family — Z-Set `+`, fold included.

use std::cell::Cell;
use std::cmp::Ordering;
use std::ops::ControlFlow;

use super::batch::Batch;
use super::batch_pool::tls_pool;
use super::columnar::{with_payload_cmp, ColumnarSource};
use super::heap::{HeapNode, LoserTree};
use crate::schema::key::{compare_pk_bytes, compare_pk_ordering, pk_bytes_eq, pk_width_dispatch, PkSortKey};
use crate::schema::SchemaDescriptor;
use gnitz_expr::{BatchView, RowSource};
use gnitz_wire::read_u64_le;
use rustc_hash::FxHashMap;

// ---------------------------------------------------------------------------
// ColPtr / UnifiedSource: type-erased column accessors that work uniformly
// for in-memory `MemBatch` regions (always Raw, base = data + offset) and
// shard `RegionView` regions (Raw via mmap offset, Constant via its single
// inline element with stride 0). Stride 0 makes
// `base.add(ri * stride) == base` for every row, so a Constant region reads
// the same bytes for every output row without any branch in the hot loop.
// ---------------------------------------------------------------------------

#[derive(Clone, Copy)]
pub(crate) struct ColPtr {
    pub base: *const u8,
    pub stride: usize,
}

impl ColPtr {
    /// Raw pointer to row `i`'s value: `base + i*stride`. A `stride == 0`
    /// (Constant) region yields `base` for every row.
    ///
    /// # Safety
    /// The caller must keep the backing region alive for the borrow and pass an
    /// in-bounds `i`. For shard/`MemBatch` regions this holds because open-time
    /// validation guarantees `offset + count*stride <= len` (the same invariant
    /// `to_unified` / `mem_batch_to_unified` already rely on).
    #[inline(always)]
    pub(crate) unsafe fn row_ptr(self, i: usize) -> *const u8 {
        self.base.add(i * self.stride)
    }

    /// Row `i` as a `len`-byte slice over the backing region; a `stride == 0`
    /// (Constant) region reads its first `len` bytes for every row. Same
    /// aliasing/in-bounds contract as [`row_ptr`](Self::row_ptr).
    ///
    /// # Safety
    /// See [`row_ptr`](Self::row_ptr); additionally `len` must not exceed the
    /// region's element width.
    #[inline(always)]
    pub(crate) unsafe fn row<'a>(self, i: usize, len: usize) -> &'a [u8] {
        std::slice::from_raw_parts(self.row_ptr(i), len)
    }
}

#[derive(Clone, Copy)]
pub(crate) struct UnifiedSource {
    pub pk: ColPtr,
    pub null_bmp: ColPtr,
    /// Payload-column null bits this source cannot answer for, OR'd into every
    /// null word the scatter copies out. Non-zero only for a shard written
    /// before an `ALTER TABLE … ADD COLUMN` (`MappedShard::null_pad_mask`); an
    /// in-memory batch always matches its schema, so `mem_batch_to_unified`
    /// sets `0`.
    pub null_pad_mask: u64,
    /// Index of this source's first payload `ColPtr` in the caller-owned table
    /// the scatter reads through: column `pi` is `cols[cols_off + pi]`. Out of
    /// line because a by-value `[ColPtr; MAX_COLUMNS]` is 1 KiB zeroed per source
    /// per call, whatever the schema's real column count.
    pub cols_off: usize,
    pub blob_ptr: *const u8,
    pub blob_len: usize,
}

/// Derive a `UnifiedSource` view over an in-memory `MemBatch`: every region
/// becomes a `(base, stride)` `ColPtr` into the batch's `data`, and the blob
/// arena is carried by pointer+len. Pure pointer arithmetic — no allocation,
/// no scan. The payload `ColPtr`s are appended to `cols`, the flat table the
/// scatter indexes through `UnifiedSource::cols_off`.
///
/// Shared by the read-cursor drain (shard-vs-MemBatch polymorphism) and the
/// flush phase-2 scatter, which has only `MemBatch` runs.
pub(crate) fn mem_batch_to_unified(mb: &MemBatch, schema: &SchemaDescriptor, cols: &mut Vec<ColPtr>) -> UnifiedSource {
    let data_ptr = mb.data.as_ptr();
    let cols_off = cols.len();
    for (pi, col) in schema.payload_columns() {
        let off = mb.offsets[super::batch::REG_PAYLOAD_START + pi];
        cols.push(ColPtr {
            base: unsafe { data_ptr.add(off) },
            stride: col.size() as usize,
        });
    }
    UnifiedSource {
        pk: ColPtr {
            base: unsafe { data_ptr.add(mb.offsets[super::batch::REG_PK]) },
            stride: mb.pk_stride as usize,
        },
        null_bmp: ColPtr {
            base: unsafe { data_ptr.add(mb.offsets[super::batch::REG_NULL_BMP]) },
            stride: super::batch::FIXED_REGION_BYTES,
        },
        null_pad_mask: 0,
        cols_off,
        blob_ptr: mb.blob.as_ptr(),
        blob_len: mb.blob.len(),
    }
}

/// Identity-keyed dedup cache for `relocate_german_string_vec`.
///
/// Key: `(src_blob.as_ptr() as usize, old_offset, length)`. The same source
/// span is copied at most once per merge — the cached value is the offset
/// inside the destination blob where the bytes were appended.
pub(crate) type BlobCache = FxHashMap<(usize, usize, usize), usize>;

/// Reserve hint for a destination heap taking `out_rows` of a `src_rows`-row
/// source whose heap is `src_blob` bytes: that slice's row-proportional share.
///
/// Reserving the *whole* source heap per target instead would ask for N× the
/// bytes any one of them can write, evicting pooled buffers and mallocing fresh
/// above the recycle cap.
///
/// Rounds the per-row share up, so a source holding fewer heap bytes than rows
/// still reserves a byte per row rather than nothing; computes the product in
/// `u128`, so a large heap times a large row count cannot overflow into a small
/// estimate; and clamps to `src_blob`, since no slice needs more than the whole
/// heap. A hint only — every consumer grows on demand — so being off costs one
/// realloc. An empty source heap asks for zero: the destination writes no heap
/// bytes either.
pub(crate) fn prorated_blob_cap(src_blob: usize, src_rows: usize, out_rows: usize) -> usize {
    if src_blob == 0 || src_rows == 0 {
        return 0;
    }
    let per_row = src_blob.div_ceil(src_rows) as u128;
    (per_row * out_rows as u128).min(src_blob as u128) as usize
}

/// Cost of relocating one German-string cell, in bytes of whole-heap memcpy — the
/// unit that lets [`should_relocate_blob`] weigh the two arms with one
/// comparison. Set from `slice_blob_relocate_bench`, which sweeps both arms over
/// slice fraction × string width; the resulting crossovers (~3 % of a 16-byte-string
/// source, ~7 % at 40 bytes, ~34 % at 256, ~67 % at 1024) are what this value fits.
const RELOCATE_CELL_COST_BYTES: usize = 500;

/// Whether copying `out_rows` rows out of a `src_rows`-row source whose heap is
/// `src_blob` bytes should relocate the slice's own string cells rather than
/// carry the source's whole heap.
///
/// The two arms cost: relocation, one cell rewrite per row plus the slice's own
/// share of the heap (`src_blob / src_rows` per row); the whole-heap copy,
/// `src_blob` regardless of how few rows are kept. Expressing the per-cell
/// rewrite as [`RELOCATE_CELL_COST_BYTES`] of memcpy makes that one comparison.
///
/// Only worth consulting where both arms are available — a destination that
/// cannot carry the source's heap verbatim (different blob identity, gathered
/// rather than contiguous rows, a result that is shipped) must relocate
/// regardless.
pub(crate) fn should_relocate_blob(src_blob: usize, src_rows: usize, out_rows: usize) -> bool {
    src_rows > 0 && src_blob > out_rows.saturating_mul(RELOCATE_CELL_COST_BYTES + src_blob / src_rows)
}

/// Copy a 16-byte German string cell and (for long strings) migrate the
/// out-of-line payload from `src_blob` into `dst_blob`.
///
/// The returned cell is ready to write into the output column buffer and is
/// always **canonical** (`german_string_cell_ok`): pad bytes past the content
/// are rebuilt as zero rather than copied through, so a skewed cell that
/// reached memory some other way cannot propagate a compare-visible pad — the
/// divergence where two rows hash equal but order unequal and never
/// consolidate. Short strings resolve entirely inline; `src_blob` is unused.
///
/// When `cache` is `Some`, the appended blob data is deduplicated by
/// `(src_blob.as_ptr(), old_offset, length)` — i.e. the same source span is
/// only copied once per merge.
///
/// **Malformed-input fallback:** a long header declaring a region that overruns
/// `src_blob` yields the canonical empty string rather than an out-of-bounds
/// read, keeping trusted in-memory callers panic-free on data that slipped past
/// validation.
#[inline]
pub(crate) fn relocate_german_string_vec(
    src_cell: &[u8],
    src_blob: &[u8],
    dst_blob: &mut Vec<u8>,
    cache: Option<&mut BlobCache>,
) -> [u8; 16] {
    // One bounds check for the whole relocation: every read below is a constant
    // index into a proven 16-byte cell.
    let src: &[u8; 16] = src_cell[..16]
        .try_into()
        .expect("relocate_german_string_vec: src must be a 16-byte German string cell");
    if gnitz_wire::read_u32_le(src, 0) as usize <= gnitz_wire::SHORT_STRING_THRESHOLD {
        return gnitz_wire::canonical_short_cell(src);
    }
    relocate_long_german_string(src, src_blob, dst_blob, cache)
}

/// The out-of-line half of `relocate_german_string_vec` — kept separate so the
/// short path stays a branchless inline sequence with no call and no frame.
///
/// Deliberately reads the cell layout directly rather than through
/// `gnitz_wire::german_string_inline` / `german_string_heap`: that split is what
/// keeps the short arm branchless, and this runs per row on every compaction, so
/// re-routing it needs a retired-instruction measurement.
fn relocate_long_german_string(
    src: &[u8; 16],
    src_blob: &[u8],
    dst_blob: &mut Vec<u8>,
    cache: Option<&mut BlobCache>,
) -> [u8; 16] {
    let length = gnitz_wire::read_u32_le(src, 0) as usize;
    let old_offset = gnitz_wire::read_u64_le(src, 8);
    let mut dest = [0u8; 16];
    let Some(span) = gnitz_wire::blob_extent(src_blob.len(), old_offset, length) else {
        // Malformed: the all-zero cell is the canonical empty string, and
        // `dst_blob` is left untouched.
        return dest;
    };
    // `length > SHORT_STRING_THRESHOLD ≥ 4`, so all four prefix bytes are content.
    dest[0..8].copy_from_slice(&src[0..8]);
    let new_offset = dst_blob.len();
    let off = match cache {
        Some(cache) => {
            let key = (src_blob.as_ptr() as usize, span.start, length);
            *cache.entry(key).or_insert_with(|| {
                dst_blob.extend_from_slice(&src_blob[span]);
                new_offset
            })
        }
        None => {
            dst_blob.extend_from_slice(&src_blob[span]);
            new_offset
        }
    };
    dest[8..16].copy_from_slice(&(off as u64).to_le_bytes());
    dest
}

// ---------------------------------------------------------------------------
// Blob cache: TLS-pooled map from a source long-string span to its offset in the
// destination heap, used by `relocate_german_string_vec` to copy each span once.
// Allocating the HashMap on every scan was hot in the profile; pool it across
// calls and only acquire one when the schema actually contains a STRING column.
// ---------------------------------------------------------------------------

/// Don't recycle caches that grew beyond this many buckets — keeps idle pool
/// memory bounded. Sized for typical merge fan-in of a few thousand unique
/// long-string spans; oversized caches are dropped instead of pooled.
const BLOB_CACHE_RECYCLE_CAP: usize = 65_536;

/// Upper bound on the up-front `reserve` in [`BlobCacheGuard::acquire`].
///
/// The row count callers pass is an upper bound on *rows*, but only long
/// (`> SHORT_STRING_THRESHOLD`) cells ever reach the map, so it wildly
/// over-estimates the entry count on the whole-relation sizes `write_to_batch`
/// is handed (a full scan passes its Σ-input row count). Reserving that far also
/// pushes capacity past `BLOB_CACHE_RECYCLE_CAP`, so the cache is dropped instead
/// of pooled — turning the pool into a guaranteed malloc/free per call. The map
/// grows on demand past this, and the pool converges to the real working set.
const BLOB_CACHE_RESERVE_CAP: usize = 4096;

thread_local! {
    static BLOB_CACHE_POOL: Cell<Vec<BlobCache>> =
        const { Cell::new(Vec::new()) };
}

/// RAII wrapper that returns a pooled blob cache only when the schema has at
/// least one STRING column, and recycles it on drop.
pub struct BlobCacheGuard(Option<BlobCache>);

impl BlobCacheGuard {
    /// `max_rows` is a sizing hint, clamped to [`BLOB_CACHE_RESERVE_CAP`] here so
    /// no caller has to remember to bound it.
    pub fn acquire(schema: &SchemaDescriptor, max_rows: usize) -> Self {
        if schema.has_german_string() {
            let mut cache = tls_pool::acquire(&BLOB_CACHE_POOL);
            cache.reserve(max_rows.min(BLOB_CACHE_RESERVE_CAP));
            Self(Some(cache))
        } else {
            Self(None)
        }
    }

    /// A guard holding no cache — for callers that already decided the
    /// relocate/dedup path is not needed (e.g. blob passthrough).
    pub(crate) fn empty() -> Self {
        Self(None)
    }

    pub fn get_mut(&mut self) -> Option<&mut BlobCache> {
        self.0.as_mut()
    }
}

impl Drop for BlobCacheGuard {
    fn drop(&mut self) {
        if let Some(mut cache) = self.0.take() {
            if cache.capacity() > BLOB_CACHE_RECYCLE_CAP {
                return;
            }
            cache.clear();
            tls_pool::recycle(&BLOB_CACHE_POOL, cache);
        }
    }
}

// ---------------------------------------------------------------------------
// MemBatch: a view over flat columnar buffers (one batch / sorted run)
// ---------------------------------------------------------------------------

/// Borrowed slice-view of a `Batch`.
///
/// The full data buffer is referenced as `data: &[u8]`, with `offsets` recording
/// the byte offset of each region (PK, weight, null_bmp, payload_0..N). This
/// lets `Batch::as_mem_batch` return a `MemBatch` without allocating a
/// `Vec<&[u8]>` of column slices.
///
/// Per-column strides are not stored here — callers iterate
/// `schema.payload_columns()` and pass the column size explicitly.
#[derive(Clone)]
pub struct MemBatch<'a> {
    pub(crate) data: &'a [u8],
    /// Borrowed, not owned: `usize` × `MAX_BATCH_REGIONS` is ~½ KiB, and this
    /// view exists to be derived per range, per chunk and per operator call.
    /// `Batch` already holds the array inline, so `as_mem_batch` lends it; the
    /// one view with no owning `Batch` (a borrowed wire frame) has its decoder's
    /// caller hold the array beside the view.
    pub(crate) offsets: &'a [usize; super::batch::MAX_BATCH_REGIONS],
    pub pk_stride: u8, // byte width of the PK region per row
    pub(crate) blob: &'a [u8],
    /// Row count of the view. Read from outside through [`MemBatch::len`].
    pub(crate) count: usize,
    /// The source [`Batch::blob_id`], carried so an appending destination can
    /// recognize its *own* blob and copy German-string structs verbatim instead
    /// of relocating each cell (see `Batch::append_ranges_inner`). A borrowed
    /// wire view has no such identity and uses `0`, which no live batch ever has
    /// (the counter starts at 1).
    pub(crate) blob_id: u64,
}

impl<'a> MemBatch<'a> {
    /// Row count of the view.
    #[inline]
    pub fn len(&self) -> usize {
        self.count
    }
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// PK region as a contiguous slice (`count * pk_stride` bytes).
    #[inline]
    pub fn pk(&self) -> &'a [u8] {
        let off = self.offsets[super::batch::REG_PK];
        &self.data[off..off + self.count * self.pk_stride as usize]
    }

    /// Weight region as a contiguous slice (`count * 8` bytes).
    #[inline]
    pub(crate) fn weight(&self) -> &'a [u8] {
        let off = self.offsets[super::batch::REG_WEIGHT];
        &self.data[off..off + self.count * 8]
    }

    /// Null bitmap region as a contiguous slice (`count * 8` bytes).
    #[inline(always)]
    pub(crate) fn null_bmp(&self) -> &'a [u8] {
        let off = self.offsets[super::batch::REG_NULL_BMP];
        &self.data[off..off + self.count * 8]
    }

    /// Payload column `pi` as a contiguous slice (`count * stride` bytes).
    /// Caller supplies the stride from the schema (see `payload_columns`).
    #[inline(always)]
    pub fn col_data(&self, pi: usize, stride: usize) -> &'a [u8] {
        let off = self.offsets[super::batch::REG_PAYLOAD_START + pi];
        &self.data[off..off + self.count * stride]
    }

    #[inline(always)]
    pub fn get_pk_bytes(&self, row: usize) -> &'a [u8] {
        let stride = self.pk_stride as usize;
        let off = self.offsets[super::batch::REG_PK] + row * stride;
        &self.data[off..off + stride]
    }
    #[inline(always)]
    pub fn get_weight(&self, row: usize) -> i64 {
        gnitz_wire::read_i64_le(self.data, self.offsets[super::batch::REG_WEIGHT] + row * 8)
    }
    #[inline(always)]
    pub fn get_null_word(&self, row: usize) -> u64 {
        read_u64_le(self.data, self.offsets[super::batch::REG_NULL_BMP] + row * 8)
    }
    #[inline(always)]
    pub(crate) fn get_col_ptr(&self, row: usize, payload_col: usize, col_size: usize) -> &'a [u8] {
        let off = self.offsets[super::batch::REG_PAYLOAD_START + payload_col] + row * col_size;
        &self.data[off..off + col_size]
    }
}

/// `MemBatch` is the sole physical batch the expression evaluator and the
/// resolved-addressing types read through — [`RowSource`] per row, [`BatchView`]
/// by region. Both traits live down in the leaf `gnitz-expr` crate, so those
/// types never name this L2 type (and the SQL client can lend its own buffers
/// through the same shapes). Each method forwards via UFCS to the inherent
/// accessor of the same name, so the call binds to the concrete read rather than
/// recursing into the trait.
///
/// Every forwarder — **and every inherent accessor it UFCS-calls** — is
/// `#[inline(always)]`; see [`BatchView`] for why the plain hint is not enough.
/// Promoting only the forwarder would leave the terminus a real call and do half
/// the job.
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
    fn blob(&self) -> &[u8] {
        self.blob
    }
    #[inline(always)]
    fn row_count(&self) -> usize {
        MemBatch::len(self)
    }
}

/// The region half. The per-row accessors above are **not** derived from these
/// (see [`BatchView`]): the derived form costs a row-count load, a second
/// multiply and a second range check per read, which `-O0` cannot hoist.
impl<'a> BatchView for MemBatch<'a> {
    #[inline(always)]
    fn col_data(&self, payload_col: usize, col_size: usize) -> &[u8] {
        MemBatch::col_data(self, payload_col, col_size)
    }
    #[inline(always)]
    fn null_bmp(&self) -> &[u8] {
        MemBatch::null_bmp(self)
    }
    #[inline(always)]
    fn pk_region(&self) -> (&[u8], usize) {
        (MemBatch::pk(self), self.pk_stride as usize)
    }
}

impl<'a> ColumnarSource for MemBatch<'a> {
    #[inline(always)]
    fn get_weight(&self, row: usize) -> i64 {
        MemBatch::get_weight(self, row)
    }
}

// ---------------------------------------------------------------------------
// PosCursor: one source's position within the N-way merge
// ---------------------------------------------------------------------------

/// One source's merge position: `position` walks `[0, count)`. Sources are
/// ghost-free by construction (a shard is *written* consolidated, a RAM-tier run
/// folded on the way in) and [`drive`]'s fold drops a stray ghost anyway, so
/// advancing is a bare `position + 1`.
///
/// `count` is the walk's upper bound, which need not be the source's row count:
/// a read cursor's range seek clamps it to the range's exclusive end so the walk
/// simply exhausts at the cut.
pub(crate) struct PosCursor {
    pub(crate) position: usize,
    pub(crate) count: usize,
}

impl PosCursor {
    #[inline]
    pub(crate) fn is_valid(&self) -> bool {
        self.position < self.count
    }

    /// Step past the current row. Callers advance only a position they have
    /// established is valid (the merge drivers advance the heap root).
    #[inline]
    pub(crate) fn advance(&mut self) {
        self.position += 1;
    }
}

// ---------------------------------------------------------------------------
// DirectWriter: writes into pre-allocated output buffers
// ---------------------------------------------------------------------------

/// Writes rows into the pre-allocated region buffers of a `write_to_batch`
/// arena.
///
/// **Every entry point must write each live byte of each row it counts** — the
/// batch invariant (see `Batch::with_capacity`), and here the whole reason the
/// arena can skip its memset. A skipped cell within a counted row leaks the
/// recycled buffer's previous contents rather than reading back a zero. Hence
/// the `repr::scatter` kernels write PK/weight/null in one fused pass and every
/// payload cell unconditionally, a null cell included. Rows the writer declines
/// to count (a ghost weight) need nothing: every reader bounds the batch to
/// `count`.
pub(crate) struct DirectWriter<'a> {
    // The repartition-scatter cluster (sibling `repr::scatter`) writes these
    // fixed-region buffers directly in its fused per-row loops, so they are
    // `pub(super)` (visible within `repr`); `blob`/`blob_cache` stay private —
    // scatter reaches the heap only through `write_string_cell`.
    pub(super) pk: &'a mut [u8],
    pub(super) pk_stride: u8,
    pub(super) weight: &'a mut [u8],
    pub(super) null_bmp: &'a mut [u8],
    pub(super) col_bufs: Vec<&'a mut [u8]>,
    /// Growable blob arena; capacity is reserved up-front by `write_to_batch`,
    /// and `blob.len()` doubles as the next-write offset.
    blob: &'a mut Vec<u8>,
    blob_cache: BlobCacheGuard,
    pub(super) count: usize,
    /// Borrowed, not owned: the scatter reads it per column, and a
    /// `SchemaDescriptor` is 360 bytes (pinned in `schema`) — copying it in would
    /// put a `memcpy` of that size on every writer open.
    pub schema: &'a SchemaDescriptor,
}

impl<'a> DirectWriter<'a> {
    /// Open over a contiguous arena of `rows` rows, carving it at the offsets
    /// [`super::batch::compute_offsets_into`] gives for `schema` — so the arena's
    /// eventual reader addresses each region where this wrote it.
    pub(crate) fn over_arena(
        data: &'a mut [u8],
        schema: &'a SchemaDescriptor,
        rows: usize,
        blob: &'a mut Vec<u8>,
    ) -> Self {
        use super::batch::{compute_offsets_into, strides_from_schema, MAX_BATCH_REGIONS, REG_PAYLOAD_START};

        let (strides, nr) = strides_from_schema(schema);
        let nr = nr as usize;
        let mut offsets = [0usize; MAX_BATCH_REGIONS];
        compute_offsets_into(&strides, nr, rows, &mut offsets);

        // Walk regions in order, splitting off [alignment pad | region] for each;
        // `base` is the absolute offset of `rest[0]`, so `offsets[r] - base` is
        // the pad to discard before region `r`. The region order IS the field
        // order — `REG_PK`, `REG_WEIGHT`, `REG_NULL_BMP`, then the payload
        // columns — so the split below is the whole carve.
        let mut regions: Vec<&mut [u8]> = Vec::with_capacity(nr);
        let mut rest: &mut [u8] = data;
        let mut base = 0usize;
        for r in 0..nr {
            let pad = offsets[r] - base;
            let after_pad = std::mem::take(&mut rest).split_at_mut(pad).1;
            let sz = rows * strides[r] as usize;
            let (region, remainder) = after_pad.split_at_mut(sz);
            regions.push(region);
            base = offsets[r] + sz;
            rest = remainder;
        }
        let col_bufs = regions.split_off(REG_PAYLOAD_START);
        let [pk, weight, null_bmp] = <[&mut [u8]; REG_PAYLOAD_START]>::try_from(regions)
            .expect("strides_from_schema always emits the three fixed regions first");

        DirectWriter {
            pk,
            pk_stride: schema.pk_stride() as u8,
            weight,
            null_bmp,
            col_bufs,
            blob,
            blob_cache: BlobCacheGuard::acquire(schema, rows),
            count: 0,
            schema,
        }
    }

    /// The region slices supplied one by one, for a test that builds them by
    /// hand rather than out of one arena.
    #[cfg(test)]
    pub(crate) fn new(
        pk: &'a mut [u8],
        weight: &'a mut [u8],
        null_bmp: &'a mut [u8],
        col_bufs: Vec<&'a mut [u8]>,
        blob: &'a mut Vec<u8>,
        schema: &'a SchemaDescriptor,
        blob_cache_capacity: usize,
    ) -> Self {
        let pk_stride = schema.pk_stride() as u8;
        DirectWriter {
            pk,
            pk_stride,
            weight,
            null_bmp,
            col_bufs,
            blob,
            blob_cache: BlobCacheGuard::acquire(schema, blob_cache_capacity),
            count: 0,
            schema,
        }
    }

    /// Write one 16-byte German string struct from raw source slices.
    ///
    /// `#[inline]`: called per row in the German-string column pass of all three
    /// `repr::scatter` entry points, a sibling module — the hint is what carries
    /// the inline across that boundary.
    #[inline]
    pub(super) fn write_string_cell(&mut self, payload_col: usize, src_struct: &[u8], src_blob: &[u8], out_row: usize) {
        let dest = relocate_german_string_vec(src_struct, src_blob, self.blob, self.blob_cache.get_mut());
        let off = out_row * 16;
        self.col_bufs[payload_col][off..off + 16].copy_from_slice(&dest);
    }
}

// ---------------------------------------------------------------------------
// run_merge: the generic N-way (PK, payload) merge + consolidation
// ---------------------------------------------------------------------------

/// N-way (PK, payload) merge + consolidation over any sorted columnar sources —
/// the single owner of the merge that flush (`RunSet::fold`) and shard
/// compaction (`compact::merge_and_route`) share.
///
/// Rows with the same (PK, payload) have their weights summed; zero-weight
/// (PK, payload) groups are dropped. The payload-aware heap ordering puts equal
/// (PK, payload) entries consecutively at the root, so the single pending-group
/// drain in [`drive`] folds both intra-source duplicates (consecutive
/// matching rows inside one sorted source) and cross-source duplicates in one
/// pass.
///
/// Builds the payload comparator once via `with_payload_cmp!`, seats one
/// [`PosCursor`] per source, and drives [`drive`]; the PK axis is
/// settled by `compare_pk_ordering` (one byte comparator at every width).
/// Each source's walk bound is its own
/// [`ColumnarSource::row_count`].
/// `emit(group_src, group_row, net_weight)` fires once per surviving (net ≠ 0)
/// group; the caller turns `(src, row)` into its output (a `DirectWriter` row for
/// flush, a guard-routed shard append for compaction).
pub(crate) fn run_merge<S: ColumnarSource>(
    sources: &[S],
    schema: &SchemaDescriptor,
    emit: impl FnMut(usize, usize, i64),
) {
    if sources.is_empty() {
        return;
    }
    let mut cursors: Vec<PosCursor> = sources
        .iter()
        .map(|s| PosCursor { position: 0, count: s.row_count() })
        .collect();

    // Dispatch the payload comparator, monomorphizing one branch-free copy of the
    // merge loop; the PK axis is `compare_pk_ordering` (no stride dispatch).
    with_payload_cmp!(schema, run_merge_body, sources, &mut cursors, schema, emit)
}

/// The payload comparator every merge path is monomorphized over: row `ai` of an
/// `A` against row `bi` of a `B`, one source type at every seat but the read
/// cursor's walk against an in-memory batch. `Copy` lets one selected comparator
/// forward down a whole call chain at no cost. `with_payload_cmp!` picks it.
pub(crate) trait RowComparator<A, B = A>:
    Fn(&SchemaDescriptor, &A, usize, &B, usize) -> Ordering + Copy
{
}
impl<A, B, F> RowComparator<A, B> for F where F: Fn(&SchemaDescriptor, &A, usize, &B, usize) -> Ordering + Copy {}

/// The merge's heap order, shared by [`drive`] and by the read cursor's own tree
/// rebuild and forward seek, so the two can never diverge on the (PK, payload)
/// total order. An `#[inline]` closure builder generic over the concrete source
/// type — no `dyn` — so each caller monomorphizes its own branch-free copy.
///
/// `compare_pk_ordering` on each player's full OPK bytes (exact at every width —
/// no cached key, no stride dispatch), then the payload `row_cmp`.
///
/// `coarsen` collapses the payload axis for a cursor holding a skeleton run: a
/// skeleton row sorts before every hydrated row of its PK, making it the exemplar
/// [`drive`] folds the whole PK group into. Tested ahead of `row_cmp`, so a
/// skeleton row's absent columns are never read. A runtime flag, not a second
/// monomorphisation axis: it is constant for a cursor's life and reached only on
/// a PK tie.
#[inline]
pub(crate) fn merge_less<'a, S, RowCmp>(
    schema: &'a SchemaDescriptor,
    sources: &'a [S],
    row_cmp: RowCmp,
    coarsen: bool,
) -> impl Fn(&HeapNode, &HeapNode) -> bool + Copy + 'a
where
    S: ColumnarSource,
    RowCmp: RowComparator<S> + 'a,
{
    move |a, b| {
        let (a_src, a_row) = (a.source_idx as usize, a.row as usize);
        let (b_src, b_row) = (b.source_idx as usize, b.row as usize);
        match compare_pk_ordering(sources[a_src].get_pk_bytes(a_row), sources[b_src].get_pk_bytes(b_row)) {
            Ordering::Less => true,
            Ordering::Greater => false,
            Ordering::Equal => {
                if coarsen {
                    let (sa, sb) = (sources[a_src].is_skeleton(), sources[b_src].is_skeleton());
                    if sa || sb {
                        return sa && !sb;
                    }
                }
                row_cmp(schema, &sources[a_src], a_row, &sources[b_src], b_row) == Ordering::Less
            }
        }
    }
}

/// Drive an N-way (PK, payload) merge to completion over `sources`, folding each
/// group's weights and calling `emit(group_src, group_row, net_weight)` once per
/// surviving (net ≠ 0) group; a `Break` returns immediately. The output PK is
/// re-derived from `(group_src, group_row)` by the caller — there is no cached
/// key to hand it.
///
/// Every merge in the tree runs through this — the flush/compaction kernel
/// ([`run_merge`]) and the read cursor's advance and drain — and it builds all
/// four closures itself, so no caller can pair a heap order with a mismatched
/// group boundary. `coarsen` is [`merge_less`]'s flag and means the same here.
///
/// `#[inline(always)]`: each caller's `emit` returns a constant `ControlFlow`, so
/// forced inlining folds the branch and drops the unused arm per monomorphisation.
#[inline(always)]
pub(crate) fn drive<S, RowCmp>(
    tree: &mut LoserTree,
    schema: &SchemaDescriptor,
    sources: &[S],
    cursors: &mut [PosCursor],
    row_cmp: RowCmp,
    coarsen: bool,
    mut emit: impl FnMut(usize, usize, i64) -> ControlFlow<()>,
) where
    S: ColumnarSource,
    RowCmp: RowComparator<S>,
{
    // `less` and `same_group` read `(source_idx, row)` out of the heap node and
    // never touch `cursors`, so they coexist with the `&mut cursors` `step!` holds.
    let less = merge_less(schema, sources, row_cmp, coarsen);
    let same_group = |a_src: usize, a_row: usize, b_src: usize, b_row: usize| {
        if !pk_bytes_eq(sources[a_src].get_pk_bytes(a_row), sources[b_src].get_pk_bytes(b_row)) {
            return false;
        }
        if coarsen && (sources[a_src].is_skeleton() || sources[b_src].is_skeleton()) {
            return true;
        }
        row_cmp(schema, &sources[a_src], a_row, &sources[b_src], b_row) == Ordering::Equal
    };
    // Sources are ghost-free, so the advance is a bare `position + 1`.
    macro_rules! step {
        ($src:expr) => {{
            let c = &mut cursors[$src];
            c.advance();
            let next = c.is_valid().then(|| c.position as u32);
            tree.step_top(next, &less);
        }};
    }

    loop {
        if tree.is_empty() {
            return;
        }

        let (group_src, group_row) = {
            let top = tree.peek();
            (top.source_idx as usize, top.row as usize)
        };

        // Open the group: take the root's weight and step past it. The first row
        // is the exemplar, so `same_group` would be tautologically true — and its
        // payload term walks every column.
        let mut net_weight: i64 = sources[group_src].get_weight(group_row);
        step!(group_src);

        // Fold tied rows: each iteration peeks the new root, breaks at the group
        // boundary, otherwise accumulates weight and steps again.
        while !tree.is_empty() {
            let (cur_src, cur_row) = {
                let top = tree.peek();
                (top.source_idx as usize, top.row as usize)
            };
            if !same_group(group_src, group_row, cur_src, cur_row) {
                break;
            }
            net_weight += sources[cur_src].get_weight(cur_row);
            step!(cur_src);
        }

        if net_weight != 0 && emit(group_src, group_row, net_weight).is_break() {
            return;
        }
    }
}

/// The tripwire and the tournament build for [`run_merge`]; the walk itself is
/// [`drive`]. Monomorphised per (source, payload) so the hot loop stays
/// branch-free.
#[inline]
fn run_merge_body<S, RowCmp>(
    sources: &[S],
    cursors: &mut [PosCursor],
    schema: &SchemaDescriptor,
    mut emit: impl FnMut(usize, usize, i64),
    row_cmp: RowCmp,
) where
    S: ColumnarSource,
    RowCmp: RowComparator<S>,
{
    // §2's one silent failure: a merge reads each source linearly, so an
    // out-of-order input makes the heap deliver duplicates non-adjacently and
    // the fold sums weights against the wrong element — no error, no assertion.
    // Checked here rather than in a wrapper type at one seat, so flush,
    // compaction, relay and shard sources are all covered.
    #[cfg(debug_assertions)]
    for (si, src) in sources.iter().enumerate() {
        for r in 1..src.row_count() {
            let ord = compare_pk_ordering(src.get_pk_bytes(r - 1), src.get_pk_bytes(r))
                .then_with(|| row_cmp(schema, src, r - 1, src, r));
            debug_assert_ne!(ord, Ordering::Greater, "run_merge: source {si} unsorted at row {r}");
        }
    }

    let mut tree = LoserTree::build(
        cursors.len(),
        |i| cursors[i].is_valid().then(|| cursors[i].position as u32),
        merge_less(schema, sources, row_cmp, false),
    );
    // `coarsen: false` — skeleton folding is a read-path concern, and compaction
    // re-materializes per PK anyway (`compact::merge_and_route`). The literal also
    // const-folds the skeleton test out of this monomorphisation.
    drive(&mut tree, schema, sources, cursors, row_cmp, false, |src, row, w| {
        emit(src, row, w);
        ControlFlow::Continue(())
    });
}

// ---------------------------------------------------------------------------
// The two-way merge: Z-Set `+` over two consolidated batches
// ---------------------------------------------------------------------------

impl Batch {
    /// Z-Set `+` of two consolidated batches: both sides' rows in (PK, payload)
    /// order, equal elements' weights summed into one row, net-zero elements
    /// dropped. The result is itself consolidated.
    ///
    /// Only the shared-PK arm folds: a galloped run stops at the other side's
    /// head PK, so nothing in it can share a (PK, payload) across sides, and a
    /// consolidated input repeats none internally.
    pub(crate) fn merged_consolidated(&self, other: &Batch, schema: &SchemaDescriptor) -> Batch {
        debug_assert!(
            self.is_consolidated() && other.is_consolidated(),
            "merged_consolidated: both inputs must be consolidated",
        );
        with_payload_cmp!(schema, merged_consolidated_body, self, other, schema)
    }

    /// `other`'s rows appended onto this batch — Z-Set `+` where an input is
    /// unsorted, so the output claims no order and nothing folds. Appending in
    /// place copies only the right side and relocates no left-side string cell:
    /// `self` already owns the heap those point into.
    pub(crate) fn concatenated(mut self, other: &Batch, schema: &SchemaDescriptor) -> Batch {
        let n_b = other.count;
        // Up front: a batch at capacity would otherwise grow by `capacity * 2`
        // and re-copy the left side.
        self.reserve_rows(n_b);
        self.append_session(n_b).push_range(&other.as_mem_batch(), 0, n_b);
        // Physically identical to both inputs' (`union_nullability_merge` returns
        // nothing else), so `self`'s region strides still describe it.
        self.set_schema(schema);
        self
    }
}

#[inline]
fn merged_consolidated_body<RowCmp>(a: &Batch, b: &Batch, schema: &SchemaDescriptor, row_cmp: RowCmp) -> Batch
where
    RowCmp: for<'x> RowComparator<MemBatch<'x>>,
{
    let (n_a, n_b) = (a.count, b.count);
    let (mb_a, mb_b) = (a.as_mem_batch(), b.as_mem_batch());

    let mut out = Batch::with_capacity(schema, n_a + n_b);
    {
        // One session for the whole merge: expected run length is 2 for a set
        // operation's uniform 128-bit PKs, so per-run setup would dominate.
        let mut sink = out.append_session(n_a + n_b);
        let (mut ia, mut jb) = (0usize, 0usize);
        while ia < n_a && jb < n_b {
            match compare_pk_ordering(a.get_pk_bytes(ia), b.get_pk_bytes(jb)) {
                // A single-source run: one galloping skip, one bulk append.
                Ordering::Less => {
                    let s = ia;
                    ia = a.advance_to(b.get_pk_bytes(jb), ia);
                    sink.push_range(&mb_a, s, ia);
                }
                Ordering::Greater => {
                    let s = jb;
                    jb = b.advance_to(a.get_pk_bytes(ia), jb);
                    sink.push_range(&mb_b, s, jb);
                }
                // A shared PK: bracket both equal-PK groups and interleave them
                // by payload. The only arm that folds, and the only one that
                // reads row by row.
                Ordering::Equal => {
                    let (ga, gb) = (a.pk_group_end(ia), b.pk_group_end(jb));
                    // The single-source stretch still open, ending at its own
                    // side's live cursor. `flush!` closes it into one push.
                    enum Open {
                        None,
                        A(usize),
                        B(usize),
                    }
                    let mut open = Open::None;
                    macro_rules! flush {
                        () => {
                            match std::mem::replace(&mut open, Open::None) {
                                Open::A(s) => sink.push_range(&mb_a, s, ia),
                                Open::B(s) => sink.push_range(&mb_b, s, jb),
                                Open::None => {}
                            }
                        };
                    }
                    while ia < ga && jb < gb {
                        match row_cmp(schema, &mb_a, ia, &mb_b, jb) {
                            Ordering::Less => {
                                if !matches!(open, Open::A(_)) {
                                    flush!();
                                    open = Open::A(ia);
                                }
                                ia += 1;
                            }
                            Ordering::Greater => {
                                if !matches!(open, Open::B(_)) {
                                    flush!();
                                    open = Open::B(jb);
                                }
                                jb += 1;
                            }
                            // One element carried by both sides: `+` sums its two
                            // weights into one row, and a zero sum drops it (§2),
                            // which is why this merge can emit fewer rows than it read.
                            Ordering::Equal => {
                                flush!();
                                sink.push_row(&mb_a, ia, a.get_weight(ia) + b.get_weight(jb));
                                ia += 1;
                                jb += 1;
                            }
                        }
                    }
                    // One group ended, so the open stretch runs on into its own
                    // side's unpicked tail, and the other side's remainder — which
                    // nothing left can fold against — follows.
                    match open {
                        Open::A(s) => {
                            sink.push_range(&mb_a, s, ga);
                            sink.push_range(&mb_b, jb, gb);
                        }
                        Open::B(s) => {
                            sink.push_range(&mb_b, s, gb);
                            sink.push_range(&mb_a, ia, ga);
                        }
                        Open::None => {
                            sink.push_range(&mb_a, ia, ga);
                            sink.push_range(&mb_b, jb, gb);
                        }
                    }
                    // The only advance that is not an `advance_to`.
                    ia = ga;
                    jb = gb;
                }
            }
        }
        sink.push_range(&mb_a, ia, n_a);
        sink.push_range(&mb_b, jb, n_b);
    }
    out
}

// ---------------------------------------------------------------------------
// Single-batch sort + consolidation
// ---------------------------------------------------------------------------

/// Sort a single batch by (PK, payload) and consolidate: sum the weights of
/// identical rows, drop ghosts. Appends one `(0, row, net weight)` survivor per
/// group to `out` — [`run_merge`]'s single-source counterpart, in the shape
/// [`super::scatter::scatter_unified_sources`] materializes and the caller sizes
/// its arena from.
pub(crate) fn consolidate_groups(batch: &MemBatch, schema: &SchemaDescriptor, out: &mut Vec<(u32, u32, i64)>) {
    let n = batch.count;
    if n == 0 {
        return;
    }

    with_payload_cmp!(schema, consolidate_groups_inner, n, batch, schema, out)
}

/// A `(sort key, row-index)` pair. Keeps the key co-located with its index so the
/// comparator reads from the element being positioned rather than chasing a
/// separate key array.
#[derive(Copy, Clone)]
struct SortEntry<K> {
    key: K,
    idx: u32,
}

/// The argsort half of [`consolidate_groups`]. [`PkSortKey`] is the whole OPK
/// image up to a 32-byte stride, so the key compare is exact and a tie goes
/// straight to the payload comparator; wider strides compare the bytes.
#[inline]
fn consolidate_groups_inner<RowCmp>(
    n: usize,
    batch: &MemBatch,
    schema: &SchemaDescriptor,
    out: &mut Vec<(u32, u32, i64)>,
    row_cmp: RowCmp,
) where
    RowCmp: for<'x> RowComparator<MemBatch<'x>>,
{
    pk_width_dispatch!(
        batch.pk_stride as usize,
        |K| {
            let mut entries: Vec<SortEntry<K>> = (0..n as u32)
                .map(|i| SortEntry {
                    key: K::from_opk(batch.get_pk_bytes(i as usize)),
                    idx: i,
                })
                .collect();
            entries.sort_unstable_by(|a, b| {
                let (x, y) = (a.idx as usize, b.idx as usize);
                a.key.cmp(&b.key).then_with(|| row_cmp(schema, batch, x, batch, y))
            });
            drain_groups(n, batch, schema, row_cmp, |pos| entries[pos].idx as usize, out);
        },
        {
            let mut order: Vec<u32> = (0..n as u32).collect();
            order.sort_unstable_by(|&a, &b| {
                let (x, y) = (a as usize, b as usize);
                compare_pk_bytes(batch.get_pk_bytes(x), batch.get_pk_bytes(y))
                    .then_with(|| row_cmp(schema, batch, x, batch, y))
            });
            drain_groups(n, batch, schema, row_cmp, |pos| order[pos] as usize, out);
        }
    )
}

/// The fold half of [`consolidate_groups`]: walk the sorted order and push one
/// survivor per (PK, payload) group whose weights do not cancel.
///
/// `resolve(pos)` maps a position in the sorted order to the batch row index.
/// Group detection is [`pk_bytes_eq`] on the two rows' OPK bytes, then the
/// payload `row_cmp` — the same two terms [`drive`]'s group boundary uses.
#[inline]
fn drain_groups<RowCmp>(
    n: usize,
    batch: &MemBatch,
    schema: &SchemaDescriptor,
    row_cmp: RowCmp,
    resolve: impl Fn(usize) -> usize,
    out: &mut Vec<(u32, u32, i64)>,
) where
    RowCmp: for<'x> RowComparator<MemBatch<'x>>,
{
    let mut pending_idx = resolve(0);
    let mut pending_weight = batch.get_weight(pending_idx);

    for pos in 1..n {
        let cur_idx = resolve(pos);
        let same_group = pk_bytes_eq(batch.get_pk_bytes(pending_idx), batch.get_pk_bytes(cur_idx))
            && row_cmp(schema, batch, pending_idx, batch, cur_idx) == Ordering::Equal;

        if same_group {
            pending_weight += batch.get_weight(cur_idx);
        } else {
            if pending_weight != 0 {
                out.push((0, pending_idx as u32, pending_weight));
            }
            pending_idx = cur_idx;
            pending_weight = batch.get_weight(cur_idx);
        }
    }
    if pending_weight != 0 {
        out.push((0, pending_idx as u32, pending_weight));
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
#[path = "tests/merge.rs"]
mod tests;
