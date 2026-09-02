//! In-memory merge for run-set consolidation, and the two-way batch merge.
//!
//! Operates on flat columnar buffers: pk[OPK big-endian, `pk_stride` B/row],
//! weight[i64 LE], null_bitmap[u64 LE], payload columns, blob arena.
//!
//! The N-way merge is a fused k-way merge + inline consolidation: rows with the
//! same (PK, payload) have their weights summed; rows whose net weight is zero
//! are dropped. [`Batch::merged_sorted`] is the two-input, fold-free counterpart
//! (Z-Set `+`), over the same comparator family.

use std::cell::Cell;
use std::cmp::Ordering;

use super::batch::Batch;
use super::batch_pool::tls_pool;
use super::columnar::{schema_is_fixedint_nonnull, with_payload_cmp, ColumnarSource};
use super::heap::{drive_merge, HeapNode, LoserTree};
use crate::schema::key::{compare_pk_bytes, compare_pk_ordering, pk_bytes_eq, pk_width_dispatch, PkSortKey};
use crate::schema::SchemaDescriptor;
use gnitz_expr::{BatchView, RowSource};
use gnitz_wire::{is_german_string, read_u64_le};
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
/// Every N-way split — the per-worker ingest scatter, the per-worker relay
/// batches, the per-guard compaction outputs, a shard row-slice — needs this, and
/// reserving the *whole* source heap per target instead would ask for N× the
/// bytes any one of them can write, evicting pooled buffers and mallocing fresh
/// above the recycle cap.
///
/// Rounds the per-row share up, so a source holding fewer heap bytes than rows
/// still reserves something rather than nothing; computes the product in `u128`,
/// so a large heap times a large row count cannot overflow into a small estimate;
/// and clamps to `src_blob`, since no slice needs more than the whole heap. A
/// hint only — every consumer grows on demand — so being off costs one realloc.
pub(crate) fn prorated_blob_cap(src_blob: usize, src_rows: usize, out_rows: usize) -> usize {
    if src_blob == 0 || src_rows == 0 {
        return 1;
    }
    let per_row = src_blob.div_ceil(src_rows) as u128;
    let est = (per_row * out_rows as u128).min(src_blob as u128) as usize;
    est.max(1)
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
/// ghost-free by construction (shards are verified at open; RAM-tier runs are
/// consolidated), so advancing is a bare `position + 1` and
/// `drive_merge`'s net-weight fold drops any cross-source zero.
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

/// Write one payload cell of `src.len()` bytes at `off` in a column buffer.
///
/// The width is dispatched to a literal because the row-at-a-time writer only
/// knows it as a runtime `col.size()`, and `copy_from_slice` on a runtime length
/// lowers to an out-of-line `memcpy` call — an indirect libc call to move 8
/// bytes, once per (row, column). Through `&mut [u8; N]` the copy is a typed
/// move instead. The arms are every `SchemaColumn::size()` a payload column can
/// have; the fallback keeps the function total.
///
/// The column-first `repr::scatter` kernels get the same effect from their
/// const-`N` gathers; this is the row-at-a-time twin.
#[inline(always)]
fn write_cell(dst: &mut [u8], off: usize, src: &[u8]) {
    macro_rules! fixed {
        ($n:literal) => {{
            // Both slices are exactly `$n` bytes here — `dst` by the range, `src`
            // by the arm's own `src.len()` match — so neither conversion can fail.
            let d: &mut [u8; $n] = (&mut dst[off..off + $n]).try_into().expect("dst cell width");
            let s: &[u8; $n] = src.try_into().expect("src cell width");
            *d = *s;
        }};
    }
    match src.len() {
        1 => fixed!(1),
        2 => fixed!(2),
        4 => fixed!(4),
        8 => fixed!(8),
        16 => fixed!(16),
        n => dst[off..off + n].copy_from_slice(src),
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
/// `write_row` `fill(0)`s a null cell instead of skipping it, and the
/// `repr::scatter` kernels write PK/weight/null in one fused pass and every
/// payload cell unconditionally. Rows the writer declines to count (a ghost
/// weight) need nothing: every reader bounds the batch to `count`.
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
    /// Borrowed, not owned: `write_row` reads it per row, and a
    /// `SchemaDescriptor` is 360 bytes (pinned in `schema`) — copying it in would
    /// put a `memcpy` of that size on the per-row path.
    pub schema: &'a SchemaDescriptor,
}

impl<'a> DirectWriter<'a> {
    pub(crate) fn new(
        pk: &'a mut [u8],
        weight: &'a mut [u8],
        null_bmp: &'a mut [u8],
        col_bufs: Vec<&'a mut [u8]>,
        blob: &'a mut Vec<u8>,
        schema: &'a SchemaDescriptor,
        blob_cache_capacity: usize,
    ) -> Self {
        let pk_stride = schema.pk_stride();
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

    // The row-at-a-time twin of the column-first `repr::scatter` kernels, kept for
    // the one path that streams: `fold_sorted` walks an already-sorted batch and
    // emits as it goes, with no survivor list to scatter from.
    pub(crate) fn write_row(&mut self, batch: &MemBatch, row: usize, weight: i64) {
        if weight == 0 {
            return;
        }
        let out_row = self.count;
        self.count += 1;

        let pk_bytes = batch.get_pk_bytes(row);
        let null_word = batch.get_null_word(row);

        let stride = self.pk_stride as usize;
        // extend_pk_bytes already asserts bytes.len() == pk_stride at ingest
        // time, so a stride mismatch is caught there.
        self.pk[out_row * stride..][..stride].copy_from_slice(pk_bytes);
        self.weight[out_row * 8..out_row * 8 + 8].copy_from_slice(&weight.to_le_bytes());
        self.null_bmp[out_row * 8..out_row * 8 + 8].copy_from_slice(&null_word.to_le_bytes());

        let schema = self.schema;
        // Fast path: every payload column is a non-nullable fixed-width int (the
        // cached `FixedIntNonnull` class). No null bit can be set and no column
        // is a German string, so skip the per-column null test and string-type
        // branch and copy each cell straight through.
        if schema_is_fixedint_nonnull(schema) {
            for (payload_idx, col) in schema.payload_columns() {
                let col_size = col.size() as usize;
                let src = batch.get_col_ptr(row, payload_idx, col_size);
                write_cell(self.col_bufs[payload_idx], out_row * col_size, src);
            }
            return;
        }

        for (payload_idx, col) in schema.payload_columns() {
            let col_size = col.size() as usize;
            let is_null = gnitz_wire::null_word_get(null_word, payload_idx);

            if is_null {
                let off = out_row * col_size;
                self.col_bufs[payload_idx][off..off + col_size].fill(0);
            } else if is_german_string(col.type_code) {
                let src_struct = batch.get_col_ptr(row, payload_idx, 16);
                self.write_string_cell(payload_idx, src_struct, batch.blob, out_row);
            } else {
                let src = batch.get_col_ptr(row, payload_idx, col_size);
                write_cell(self.col_bufs[payload_idx], out_row * col_size, src);
            }
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

    pub(crate) fn row_count(&self) -> usize {
        self.count
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
/// drain in `drive_merge` folds both intra-source duplicates (consecutive
/// matching rows inside one sorted source) and cross-source duplicates in one
/// pass.
///
/// Builds the payload comparator once via `with_payload_cmp!`, seats one
/// [`PosCursor`] per source, and drives `drive_merge`; the PK axis is
/// settled by `compare_pk_ordering` (one byte comparator at every width).
/// Sources are ghost-free by construction (shards are verified at open;
/// RAM-tier runs are consolidated); `drive_merge`'s net-weight fold drops
/// cross-source zeros. Each source's walk bound is its own
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
        .map(|s| PosCursor {
            position: 0,
            count: s.row_count(),
        })
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

/// The (PK, payload) merge comparator trio, shared by the flush/compaction
/// kernel ([`run_merge_body`]) and the read cursor's loser-tree drives so the
/// two can never diverge on the (PK, payload) total order. All three are `#[inline]`
/// closure builders, generic over the concrete source type — no `dyn` — so
/// each caller monomorphizes its own branch-free copy.
///
/// `merge_less` is the full heap order: `compare_pk_ordering` on each player's
/// full OPK bytes (exact at every width — no cached key, no stride dispatch),
/// then the payload `row_cmp`.
///
/// `coarsen` collapses the payload axis for a cursor that holds a skeleton run: a
/// skeleton row sorts *before* every hydrated row of its PK (two skeleton rows
/// tie), which makes it the group exemplar `drive_merge` compares against, and
/// [`merge_eq_payload`] then folds the whole PK group into it. The tiebreak is
/// tested ahead of `row_cmp`, so the payload comparator never reads a skeleton
/// row's columns. It is constant for a cursor's whole life (a cursor's source set
/// never changes) and reached only once two players tie on PK, so it stays a
/// runtime flag rather than a second monomorphisation axis over this kernel.
/// `false` at the flush/compaction seat and at every cursor whose runs are all
/// hydrated.
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

/// The PK-equality term of the merge's grouping trio, through the canonical
/// [`pk_bytes_eq`], which carries why a raw slice `==` is the wrong spelling.
#[inline]
pub(crate) fn merge_same_pk<S: RowSource>(sources: &[S]) -> impl Fn(usize, usize, usize, usize) -> bool + Copy + '_ {
    move |a_src, a_row, b_src, b_row| {
        pk_bytes_eq(sources[a_src].get_pk_bytes(a_row), sources[b_src].get_pk_bytes(b_row))
    }
}

/// The payload-equality term of the trio (`row_cmp == Equal`; the PK term is
/// [`merge_same_pk`]).
///
/// Under `coarsen` a pair where either row is skeleton groups unconditionally: a
/// skeleton row carries the PK's whole coarse weight, so its PK group folds to
/// one row rather than to (PK, payload) groups. Tested ahead of `row_cmp`, so a
/// skeleton row's (absent) columns are never read. See [`merge_less`] for why
/// the ordering half is what makes the skeleton row the group's exemplar.
#[inline]
pub(crate) fn merge_eq_payload<'a, S, RowCmp>(
    schema: &'a SchemaDescriptor,
    sources: &'a [S],
    row_cmp: RowCmp,
    coarsen: bool,
) -> impl Fn(usize, usize, usize, usize) -> bool + Copy + 'a
where
    S: ColumnarSource,
    RowCmp: RowComparator<S> + 'a,
{
    move |a_src, a_row, b_src, b_row| {
        if coarsen && (sources[a_src].is_skeleton() || sources[b_src].is_skeleton()) {
            return true;
        }
        row_cmp(schema, &sources[a_src], a_row, &sources[b_src], b_row) == Ordering::Equal
    }
}

/// N-way merge closure builder + driver. The keyless heap reads each player's
/// OPK bytes through `(source_idx, row)`: `compare_pk_ordering` settles the PK
/// axis (one byte comparator at every width), then the payload `row_cmp`;
/// `same_pk` and `eq_payload` are the two equality terms (see [`merge_same_pk`]
/// for why the PK term is the register comparator, not a raw byte `==`).
/// Monomorphised per (source, payload) so `drive_merge`'s hot loop stays
/// branch-free; sources are ghost-free, so the advance is a bare `position + 1`.
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
    // compaction and shard sources are all covered.
    #[cfg(debug_assertions)]
    for (si, src) in sources.iter().enumerate() {
        for r in 1..src.row_count() {
            let ord = compare_pk_ordering(src.get_pk_bytes(r - 1), src.get_pk_bytes(r))
                .then_with(|| row_cmp(schema, src, r - 1, src, r));
            debug_assert_ne!(ord, Ordering::Greater, "run_merge: source {si} unsorted at row {r}");
        }
    }

    // `less` reads `a.row` / `b.row` from the heap node directly — never
    // touches `cursors` — so it coexists with the `&mut cursors` borrow held
    // by `advance`.  `source_idx` doubles as the source index here.
    // No coarsening: skeleton folding is a read-path concern. Compaction
    // re-materializes a dehydrated destination guard per PK anyway (see
    // `compact::merge_and_route`), which subsumes any grouping a comparator
    // could do here.
    let less = merge_less(schema, sources, row_cmp, false);
    let same_pk = merge_same_pk(sources);
    let eq_payload = merge_eq_payload(schema, sources, row_cmp, false);
    let mut tree = LoserTree::build(
        cursors.len(),
        |i| cursors[i].is_valid().then(|| cursors[i].position as u32),
        less,
    );
    drive_merge(
        &mut tree,
        less,
        |src| {
            // Advance (sources are ghost-free) and report validity.
            cursors[src].advance();
            cursors[src].is_valid().then(|| cursors[src].position as u32)
        },
        same_pk,
        eq_payload,
        |src, row| sources[src].get_weight(row),
        |group_src, group_row, w| {
            emit(group_src, group_row, w);
            std::ops::ControlFlow::Continue(())
        },
    );
}

// ---------------------------------------------------------------------------
// The two-way merge: Z-Set `+` over two sorted batches
// ---------------------------------------------------------------------------

impl Batch {
    /// Both batches' rows in (PK, payload) order — Z-Set `+` without the fold: every
    /// row survives at its own weight, and two sharing a (PK, payload) land adjacent
    /// for a later consolidation to sum. Sorted inputs are a debug-checked
    /// precondition; the output carries no layout claim.
    pub(crate) fn merged_sorted(&self, other: &Batch, schema: &SchemaDescriptor) -> Batch {
        with_payload_cmp!(schema, merged_sorted_body, self, other, schema)
    }

    /// Both batches' rows, `self`'s first — Z-Set `+` where an input is unsorted,
    /// so the output can claim no order. One session covers both sides.
    pub(crate) fn concatenated(&self, other: &Batch, schema: &SchemaDescriptor) -> Batch {
        let (n_a, n_b) = (self.count, other.count);
        let mut out = Batch::with_capacity(*schema, n_a + n_b);
        {
            let mut sink = out.append_session(n_a + n_b);
            sink.push_range(&self.as_mem_batch(), 0, n_a);
            sink.push_range(&other.as_mem_batch(), 0, n_b);
        }
        out
    }
}

#[inline]
fn merged_sorted_body<RowCmp>(a: &Batch, b: &Batch, schema: &SchemaDescriptor, row_cmp: RowCmp) -> Batch
where
    RowCmp: for<'x> RowComparator<MemBatch<'x>>,
{
    let (n_a, n_b) = (a.count, b.count);
    let (mb_a, mb_b) = (a.as_mem_batch(), b.as_mem_batch());

    // §2's one silent failure: an out-of-order input makes the walk sum weights
    // against the wrong element, with no error. Same tripwire as `run_merge_body`.
    #[cfg(debug_assertions)]
    for (side, src) in [("a", a), ("b", b)] {
        let mb = src.as_mem_batch();
        for r in 1..src.count {
            let ord = compare_pk_ordering(src.get_pk_bytes(r - 1), src.get_pk_bytes(r))
                .then_with(|| row_cmp(schema, &mb, r - 1, &mb, r));
            debug_assert_ne!(
                ord,
                Ordering::Greater,
                "merged_sorted: input {side} unsorted at row {r}"
            );
        }
    }

    let mut out = Batch::with_capacity(*schema, n_a + n_b);
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
                // A shared PK: bracket both equal-PK groups and interleave them by
                // payload, coalescing each single-source stretch into one push. The
                // comparison that ends a stretch also picks the next row.
                Ordering::Equal => {
                    let (ga, gb) = (a.pk_group_end(ia), b.pk_group_end(jb));
                    let mut prev_a = row_cmp(schema, &mb_a, ia, &mb_b, jb) != Ordering::Greater;
                    let mut run_start = if prev_a { ia } else { jb };
                    if prev_a {
                        ia += 1;
                    } else {
                        jb += 1;
                    }
                    while ia < ga && jb < gb {
                        let pick_a = row_cmp(schema, &mb_a, ia, &mb_b, jb) != Ordering::Greater;
                        if pick_a != prev_a {
                            if prev_a {
                                sink.push_range(&mb_a, run_start, ia);
                                run_start = jb;
                            } else {
                                sink.push_range(&mb_b, run_start, jb);
                                run_start = ia;
                            }
                            prev_a = pick_a;
                        }
                        if pick_a {
                            ia += 1;
                        } else {
                            jb += 1;
                        }
                    }
                    // Flush the in-progress stretch, folding in its side's tail —
                    // rows left unpicked because the *other* group ended first.
                    if prev_a {
                        sink.push_range(&mb_a, run_start, ga);
                        if jb < gb {
                            sink.push_range(&mb_b, jb, gb);
                        }
                    } else {
                        sink.push_range(&mb_b, run_start, gb);
                        if ia < ga {
                            sink.push_range(&mb_a, ia, ga);
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

/// Sort a single batch by (PK, payload) and consolidate: sum weights for
/// identical (PK, payload) rows, drop ghosts (net weight == 0).
///
pub(crate) fn sort_and_consolidate(batch: &MemBatch, schema: &SchemaDescriptor, writer: &mut DirectWriter) {
    let n = batch.count;
    if n == 0 {
        return;
    }

    with_payload_cmp!(schema, sort_consolidate_inner, n, batch, schema, writer)
}

/// A `(sort key, row-index)` pair. Keeps the key co-located with its index so the
/// comparator reads from the element being positioned rather than chasing a
/// separate key array.
#[derive(Copy, Clone)]
struct SortEntry<K> {
    key: K,
    idx: u32,
}

/// Sort-plus-consolidate. The sort key is the width-matched [`PkSortKey`], which
/// is the *whole* OPK image up to a 32-byte stride — so the key compare is exact
/// and a tie goes straight to the payload comparator. The previous fixed `u128`
/// key was only an order-preserving *prefix*, forcing an OPK-byte tiebreak on
/// every equal-key pair; below stride 17 that compare is provably `Equal`
/// (`pack_pk_be` is injective there) yet still ran as an out-of-line `bcmp` on
/// each one, and duplicate PKs are the normal case here (`map_reindex` group
/// keys, join output keyed by the left PK, the MIN/MAX value index). Strides past
/// the widest register key keep the byte compare.
#[inline]
fn sort_consolidate_inner<RowCmp>(
    n: usize,
    batch: &MemBatch,
    schema: &SchemaDescriptor,
    writer: &mut DirectWriter,
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
            scatter_groups(n, batch, schema, writer, row_cmp, |pos| entries[pos].idx as usize);
        },
        {
            let mut order: Vec<u32> = (0..n as u32).collect();
            order.sort_unstable_by(|&a, &b| {
                let (x, y) = (a as usize, b as usize);
                compare_pk_bytes(batch.get_pk_bytes(x), batch.get_pk_bytes(y))
                    .then_with(|| row_cmp(schema, batch, x, batch, y))
            });
            scatter_groups(n, batch, schema, writer, row_cmp, |pos| order[pos] as usize);
        }
    )
}

/// [`drain_groups`] materialized **column-at-a-time**: collect the surviving
/// `(row, net weight)` groups, then hand them to the shared column-first scatter,
/// which makes each per-column decision (null test, German-string type test,
/// cell-width dispatch) once per column instead of once per (row, column).
///
/// Only for the sorting caller: it already allocates an n-element index array to
/// sort, so the survivor list is a second allocation of the same order, and
/// nothing about the walk streams. `fold_sorted` genuinely streams and stays
/// row-at-a-time.
#[inline]
fn scatter_groups<RowCmp>(
    n: usize,
    batch: &MemBatch,
    schema: &SchemaDescriptor,
    writer: &mut DirectWriter,
    row_cmp: RowCmp,
    resolve: impl Fn(usize) -> usize,
) where
    RowCmp: for<'x> RowComparator<MemBatch<'x>>,
{
    let mut survivors: Vec<(u32, u32, i64)> = Vec::with_capacity(n);
    drain_groups(n, batch, schema, row_cmp, resolve, |row, w| {
        survivors.push((0, row as u32, w))
    });
    if survivors.is_empty() {
        return;
    }
    let mut cols = Vec::new();
    let unified = [mem_batch_to_unified(batch, schema, &mut cols)];
    super::scatter::scatter_unified_sources(&unified, &cols, &survivors, writer);
}

/// Weight-fold an already-sorted batch: sum weights for identical (PK, payload)
/// rows and drop ghosts (net weight == 0). Caller must guarantee sorted input.
pub(crate) fn fold_sorted(batch: &MemBatch, schema: &SchemaDescriptor, writer: &mut DirectWriter) {
    let n = batch.count;
    if n == 0 {
        return;
    }
    // No ordered PK comparison here: input is already sorted. Group detection
    // in `drain_groups` is `pk_bytes_eq` on the OPK bytes, then the payload term.
    with_payload_cmp!(schema, fold_with, n, batch, schema, writer)
}

/// `fold_sorted` closure dispatcher: forwards to the single generic drain, whose
/// PK term is `pk_bytes_eq` at every width.
#[inline]
fn fold_with<RowCmp>(n: usize, batch: &MemBatch, schema: &SchemaDescriptor, writer: &mut DirectWriter, row_cmp: RowCmp)
where
    RowCmp: for<'x> RowComparator<MemBatch<'x>>,
{
    // Input is already sorted, so the iteration position is the batch row index.
    drain_groups(
        n,
        batch,
        schema,
        row_cmp,
        |pos| pos,
        |row, w| writer.write_row(batch, row, w),
    );
}

/// Shared pending-group drain loop: fires `emit(batch_row, net_weight)` once per
/// surviving (net ≠ 0) (PK, payload) group.
///
/// `resolve(pos)` maps an iteration position to the batch row index. For
/// `sort_and_consolidate` this is an indirection through a sorted index array;
/// for `fold_sorted` the input is already sorted so `pos == batch_row_idx`.
///
/// Group detection is [`pk_bytes_eq`] on the two rows' OPK bytes, then the
/// payload `row_cmp` — the same PK term the N-way merge fold uses (see
/// [`merge_same_pk`]).
#[inline]
fn drain_groups<RowCmp>(
    n: usize,
    batch: &MemBatch,
    schema: &SchemaDescriptor,
    row_cmp: RowCmp,
    resolve: impl Fn(usize) -> usize,
    mut emit: impl FnMut(usize, i64),
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
                emit(pending_idx, pending_weight);
            }
            pending_idx = cur_idx;
            pending_weight = batch.get_weight(cur_idx);
        }
    }
    if pending_weight != 0 {
        emit(pending_idx, pending_weight);
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
#[path = "tests/merge.rs"]
mod tests;
