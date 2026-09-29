//! The borrowed batch view [`MemBatch`], the columnar source abstraction and
//! the direct row writer over it, German-string blob relocation, and the merges
//! built on them: the N-way run merge, in-batch consolidation, and the two-way
//! batch merge.
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
use std::collections::VecDeque;
use std::ops::{ControlFlow, Range};

use super::batch::{Batch, Layout};
use super::batch_pool::tls_pool;
use super::heap::{HeapNode, LoserTree};
use super::scatter::DecodedColumns;
use super::seek::pk_group_end;
use crate::schema::key::{compare_pk_ordering, pk_bytes_eq, pk_width_dispatch, PkSortKey};
use crate::schema::payload_order::{with_payload_cmp, PayloadOrder};
use crate::schema::SchemaDescriptor;
use gnitz_expr::{BatchView, RowSource};
use gnitz_wire::{read_u64_le, write_u64_le};
use rustc_hash::FxHashMap;

// ---------------------------------------------------------------------------
// ColPtr / UnifiedSource: type-erased column accessors, one `(base, stride)`
// `ColPtr` per region, for in-memory `MemBatch` and shard sources alike.
// ---------------------------------------------------------------------------

/// A column as `base + row * stride`. Stride 0 reads one shared element for
/// every row. `base` itself need not lie inside the column: a decoded window is
/// rebased so that its first row lands on its first cell.
#[derive(Clone, Copy)]
pub(crate) struct ColPtr {
    pub base: *const u8,
    pub stride: usize,
}

impl ColPtr {
    /// Row `i`'s address.
    #[inline(always)]
    pub(crate) fn row_ptr(self, i: usize) -> *const u8 {
        self.base.wrapping_add(i * self.stride)
    }

    /// Row `i`'s first `len` bytes.
    ///
    /// # Safety
    /// Row `i` is a row of a live column this addresses, and `len` does not
    /// exceed its element width.
    #[inline(always)]
    pub(crate) unsafe fn row<'a>(self, i: usize, len: usize) -> &'a [u8] {
        std::slice::from_raw_parts(self.row_ptr(i), len)
    }
}

#[derive(Clone, Copy)]
pub(crate) struct UnifiedSource<'a> {
    pub pk: ColPtr,
    pub null_bmp: ColPtr,
    /// Null bits OR'd into every null word the scatter copies out.
    pub null_pad_mask: u64,
    /// Index of this source's first payload `ColPtr` in the caller-owned table
    /// the scatter reads through: column `pi` is `cols[cols_off + pi]`. Out of
    /// line because a by-value `[ColPtr; MAX_COLUMNS]` is 1 KiB zeroed per source
    /// per call, whatever the schema's real column count.
    pub cols_off: usize,
    pub blob: &'a [u8],
    /// Where `blob` already lies in the scatter's destination heap, when carried
    /// whole; `None` relocates each cell.
    pub heap_at: Option<usize>,
}

/// Derive a `UnifiedSource` view over an in-memory `MemBatch`: every region
/// becomes a `(base, stride)` `ColPtr` into the batch's `data`, and the blob
/// arena is borrowed. Pure pointer arithmetic — no allocation,
/// no scan. The payload `ColPtr`s are appended to `cols`, the flat table the
/// scatter indexes through `UnifiedSource::cols_off`.
pub(crate) fn mem_batch_to_unified<'a>(
    mb: &MemBatch<'a>,
    schema: &SchemaDescriptor,
    cols: &mut Vec<ColPtr>,
) -> UnifiedSource<'a> {
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
        blob: mb.blob,
        heap_at: None,
    }
}

type SpanKey = (usize, usize, usize);
type SpanMap = FxHashMap<SpanKey, usize>;

/// One source span, identified by the heap it lives in. Shared so a sizing pass
/// charges exactly the spans a relocation copies.
#[inline]
pub(super) fn blob_span_key(src_blob: &[u8], start: usize, length: usize) -> SpanKey {
    (src_blob.as_ptr() as usize, start, length)
}

/// Reserve hint for a destination heap taking `out_rows` of a `src_rows`-row
/// source whose heap is `src_blob` bytes: that slice's row-proportional share,
/// rounded up, and at most the whole heap.
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
pub(crate) fn should_relocate_blob(src_blob: usize, src_rows: usize, out_rows: usize) -> bool {
    src_rows > 0 && src_blob > out_rows.saturating_mul(RELOCATE_CELL_COST_BYTES + src_blob / src_rows)
}

/// A heap more than a quarter dead is compacted rather than carried.
pub(crate) fn heap_is_wasteful(dead: usize, heap: usize) -> bool {
    dead.saturating_mul(4) > heap
}

/// The dead-byte bound a destination takes on by carrying a source's whole
/// heap to copy `kept_rows` of its `src_rows` rows, or `None` when those rows'
/// cells are relocated instead: the cheaper copy by [`should_relocate_blob`],
/// or a carried heap that would be [wasteful](heap_is_wasteful).
///
/// `dead` is the source's own bound; `excluded` sums the long bytes of the rows
/// left behind, and runs only once the row-prorated share of those rows has not
/// already refused the carry — so a small slice of a large source never walks
/// the rest of it.
pub(super) fn carried_dead(
    heap: usize,
    dead: usize,
    src_rows: usize,
    kept_rows: usize,
    excluded: impl FnOnce() -> usize,
) -> Option<usize> {
    if kept_rows == 0 {
        return None;
    }
    if heap == 0 {
        return Some(0);
    }
    let left = src_rows - kept_rows;
    if should_relocate_blob(heap, src_rows, kept_rows)
        || heap_is_wasteful(dead + prorated_blob_cap(heap, src_rows, left), heap)
    {
        return None;
    }
    let dead = if left == 0 { dead } else { dead + excluded() };
    (!heap_is_wasteful(dead, heap)).then_some(dead)
}

/// The heap bytes `row`'s long cells in the German-string slots `mask` name.
/// A span two cells share counts twice.
#[inline]
pub(super) fn row_long_bytes<S: RowSource>(src: &S, mask: u64, row: usize) -> usize {
    gnitz_wire::BitIter(mask)
        .filter_map(|pi| gnitz_wire::german_string_heap(src.get_col_ptr(row, pi, 16), usize::MAX))
        .map(|span| span.len())
        .sum()
}

/// Where a copy's German-string cells find their bytes. The dedup cache belongs
/// to relocation alone, so it lives in that arm.
pub(crate) enum HeapArm<'c> {
    /// Each cell's span, copied into the destination heap once under the cache.
    Relocate(&'c mut BlobCache),
    /// The source's heap, already in the destination's at this base.
    At(usize),
}

/// Copy the German-string cells `src` (whole 16-byte cells, their spans in
/// `src_blob`) onto `dst` as `heap` says: shifted onto the carried heap, or
/// each relocated into `dst_blob`.
#[inline]
pub(crate) fn copy_string_cells(
    dst: &mut [u8],
    src: &[u8],
    src_blob: &[u8],
    dst_blob: &mut Vec<u8>,
    heap: &mut HeapArm<'_>,
) {
    match heap {
        HeapArm::At(base) => {
            dst.copy_from_slice(src);
            gnitz_wire::shift_german_string_heaps(dst, *base);
        }
        HeapArm::Relocate(cache) => {
            for (d, s) in dst.as_chunks_mut::<16>().0.iter_mut().zip(src.as_chunks::<16>().0) {
                *d = relocate_german_string_vec(s, src_blob, dst_blob, Some(&mut **cache));
            }
        }
    }
}

/// `src_cell` rebased onto `dst_blob`: a short cell with its pad zeroed, a long
/// one with its content appended to `dst_blob` — once per source span when
/// `cache` is `Some`. A long cell overrunning `src_blob` becomes the empty
/// string.
#[inline]
pub(crate) fn relocate_german_string_vec(
    src_cell: &[u8],
    src_blob: &[u8],
    dst_blob: &mut Vec<u8>,
    cache: Option<&mut BlobCache>,
) -> [u8; 16] {
    let src: &[u8; 16] = src_cell[..16]
        .try_into()
        .expect("relocate_german_string_vec: src must be a 16-byte German string cell");
    if let Some(cell) = gnitz_wire::canonical_short_cell(src) {
        return cell;
    }
    relocate_long_german_string(src, src_blob, dst_blob, cache)
}

/// The long arm of `relocate_german_string_vec`, out of line so the short arm
/// inlines alone.
fn relocate_long_german_string(
    src: &[u8; 16],
    src_blob: &[u8],
    dst_blob: &mut Vec<u8>,
    cache: Option<&mut BlobCache>,
) -> [u8; 16] {
    let mut dest = [0u8; 16];
    let Some(span) = gnitz_wire::german_string_heap(src, src_blob.len()) else {
        return dest;
    };
    let length = span.end - span.start;
    // A long cell's length and prefix carry through; only its offset moves.
    dest[0..8].copy_from_slice(&src[0..8]);
    let new_offset = dst_blob.len();
    let off = match cache {
        Some(cache) => {
            let key = blob_span_key(src_blob, span.start, length);
            *cache.map().entry(key).or_insert_with(|| {
                dst_blob.extend_from_slice(&src_blob[span]);
                new_offset
            })
        }
        None => {
            dst_blob.extend_from_slice(&src_blob[span]);
            new_offset
        }
    };
    write_u64_le(&mut dest, 8, off as u64);
    dest
}

// ---------------------------------------------------------------------------
// Blob cache
// ---------------------------------------------------------------------------

/// The most entries a [`BlobCache`] reserves up front. Callers pass a row count,
/// but only long strings reach the map.
const BLOB_CACHE_RESERVE_CAP: usize = 4096;

thread_local! {
    static BLOB_CACHE_POOL: Cell<VecDeque<SpanMap>> = const { Cell::new(VecDeque::new()) };
}

/// Dedups long-string spans relocated into one destination heap, so each is
/// copied once. Its map is pooled, and taken at the first relocation.
pub(crate) struct BlobCache {
    map: Option<SpanMap>,
    reserve: usize,
}

impl BlobCache {
    pub(crate) fn new(rows: usize) -> Self {
        BlobCache {
            map: None,
            reserve: rows.min(BLOB_CACHE_RESERVE_CAP),
        }
    }

    pub(super) fn map(&mut self) -> &mut SpanMap {
        let reserve = self.reserve;
        self.map.get_or_insert_with(|| {
            let mut map = tls_pool::take(&BLOB_CACHE_POOL, |_| true).unwrap_or_default();
            map.reserve(reserve);
            map
        })
    }
}

impl Drop for BlobCache {
    fn drop(&mut self) {
        if let Some(mut map) = self.map.take() {
            let bytes = map.capacity() * std::mem::size_of::<(SpanKey, usize)>();
            map.clear();
            tls_pool::recycle(&BLOB_CACHE_POOL, map, bytes);
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
    /// An upper bound on the bytes of `blob` no cell references.
    pub(in crate::storage) dead_heap: usize,
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

    /// Wrapping sum of the weights of rows `[start, end)`, over one
    /// bounds-checked slice of the weight region.
    #[inline]
    pub(crate) fn sum_weights(&self, start: usize, end: usize) -> i64 {
        self.weight()[start * 8..end * 8]
            .as_chunks::<8>()
            .0
            .iter()
            .fold(0i64, |a, w| a.wrapping_add(i64::from_le_bytes(*w)))
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
    /// The leading `n` bytes of row `row`'s PK.
    #[inline(always)]
    pub(crate) fn get_pk_prefix(&self, row: usize, n: usize) -> &'a [u8] {
        debug_assert!(n <= self.pk_stride as usize);
        let off = self.offsets[super::batch::REG_PK] + row * self.pk_stride as usize;
        &self.data[off..off + n]
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

/// Each method forwards via UFCS to the inherent accessor of the same name, so
/// the call binds to the concrete read rather than recursing into the trait.
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

    fn to_unified(
        &self,
        schema: &SchemaDescriptor,
        cols: &mut Vec<ColPtr>,
        _window: Range<usize>,
        _decoded: &mut DecodedColumns,
    ) -> UnifiedSource<'_> {
        mem_batch_to_unified(self, schema, cols)
    }

    #[inline(always)]
    fn is_skeleton(&self) -> bool {
        false
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
    /// A cursor over `[0, count)`, from row 0.
    #[inline]
    pub(crate) fn new(count: usize) -> Self {
        debug_assert!(
            count < u32::MAX as usize,
            "merge source exceeds the heap node's u32 row"
        );
        PosCursor { position: 0, count }
    }

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
    // `repr`'s row-copy kernels write these fixed regions directly, so they are
    // `pub(super)`; `blob`/`blob_cache` stay private — the heap is reached only
    // through `write_string_cell`.
    pub(super) pk: &'a mut [u8],
    pub(super) pk_stride: u8,
    pub(super) weight: &'a mut [u8],
    pub(super) null_bmp: &'a mut [u8],
    pub(super) col_bufs: Vec<&'a mut [u8]>,
    /// Growable blob arena; capacity is reserved up-front by `write_to_batch`,
    /// and `blob.len()` doubles as the next-write offset.
    blob: &'a mut Vec<u8>,
    blob_cache: BlobCache,
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
        use super::batch::{compute_offsets_into, strides_from_schema, MAX_BATCH_REGIONS};

        let (strides, nr) = strides_from_schema(schema);
        let nr = nr as usize;
        let mut offsets = [0usize; MAX_BATCH_REGIONS];
        compute_offsets_into(&strides, nr, rows, &mut offsets);
        Self::over_regions(data, &offsets, &strides, nr, rows, schema, blob)
    }

    /// One writable slice per fixed region, carved at `offsets[r]` from
    /// `data`'s own start — so a wire block's header comes out as
    /// the first region's leading pad.
    pub(crate) fn over_regions(
        data: &'a mut [u8],
        offsets: &[usize],
        strides: &[u8],
        nr: usize,
        rows: usize,
        schema: &'a SchemaDescriptor,
        blob: &'a mut Vec<u8>,
    ) -> Self {
        use super::batch::REG_PAYLOAD_START;
        debug_assert!(nr >= REG_PAYLOAD_START, "a carve must cover the three fixed regions");

        let mut fixed: [&mut [u8]; REG_PAYLOAD_START] = [&mut [], &mut [], &mut []];
        let mut col_bufs: Vec<&mut [u8]> = Vec::with_capacity(nr - REG_PAYLOAD_START);
        let mut rest: &mut [u8] = data;
        let mut base = 0usize;
        for r in 0..nr {
            let after_pad = std::mem::take(&mut rest).split_at_mut(offsets[r] - base).1;
            let sz = rows * strides[r] as usize;
            let (region, remainder) = after_pad.split_at_mut(sz);
            match fixed.get_mut(r) {
                Some(slot) => *slot = region,
                None => col_bufs.push(region),
            }
            base = offsets[r] + sz;
            rest = remainder;
        }
        let [pk, weight, null_bmp] = fixed;

        DirectWriter {
            pk,
            pk_stride: schema.pk_stride() as u8,
            weight,
            null_bmp,
            col_bufs,
            blob,
            blob_cache: BlobCache::new(rows),
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
            blob_cache: BlobCache::new(blob_cache_capacity),
            count: 0,
            schema,
        }
    }

    /// Write one 16-byte German string struct from raw source slices.
    ///
    /// `#[inline]`: called per row from sibling modules in `repr`; the hint is
    /// what carries the inline across that boundary.
    #[inline]
    pub(super) fn write_string_cell(&mut self, payload_col: usize, src_struct: &[u8], src_blob: &[u8], out_row: usize) {
        let dest = relocate_german_string_vec(src_struct, src_blob, self.blob, Some(&mut self.blob_cache));
        let off = out_row * 16;
        self.col_bufs[payload_col][off..off + 16].copy_from_slice(&dest);
    }

    /// Carry a source heap whole onto the end of this writer's heap. Returns the
    /// base the caller shifts its copied cells' offsets by.
    pub(super) fn adopt_heap(&mut self, src_blob: &[u8]) -> usize {
        let base = self.blob.len();
        self.blob.extend_from_slice(src_blob);
        base
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
/// Selects the payload order once via `with_payload_cmp!`, seats one
/// [`PosCursor`] per source, and drives [`drive`]; the PK axis is settled by
/// `compare_pk_ordering` (one byte comparator at every width). Each source's
/// walk bound is its own [`RowSource::row_count`].
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
    let mut cursors: Vec<PosCursor> = sources.iter().map(|s| PosCursor::new(s.row_count())).collect();

    // Dispatch the payload order, monomorphizing one branch-free copy of the
    // merge loop; the PK axis is `compare_pk_ordering` (no stride dispatch).
    with_payload_cmp!(schema, run_merge_body, sources, &mut cursors, schema, emit)
}

/// Z-set `+` of `sources`, each consolidated, as one consolidated batch.
pub(crate) fn merge_consolidated(sources: &[MemBatch<'_>], schema: &SchemaDescriptor) -> Batch {
    let mut rows: Vec<(u32, u32, i64)> = Vec::with_capacity(sources.iter().map(|s| s.count).sum());
    run_merge(sources, schema, |src, row, w| rows.push((src as u32, row as u32, w)));
    let mut out = super::scatter::materialize_carrying(sources, schema, &rows);
    out.certify_layout(Layout::Consolidated);
    out
}

/// A `RowSource` that also carries the Z-set weight: a storage row the N-way
/// merge folds.
pub(crate) trait ColumnarSource: RowSource {
    /// The row's signed Z-set weight / multiplicity (region[1]).
    fn get_weight(&self, row: usize) -> i64;

    /// A [`UnifiedSource`] over this source's regions, readable at rows in
    /// `window`, with one payload `ColPtr` per payload column of `schema`
    /// appended to `cols`.
    fn to_unified(
        &self,
        schema: &SchemaDescriptor,
        cols: &mut Vec<ColPtr>,
        window: Range<usize>,
        decoded: &mut DecodedColumns,
    ) -> UnifiedSource<'_>;

    /// Whether this source's rows are (PK, coarse weight) pairs with no payload —
    /// a capacity-bounded view's skeleton shard.
    fn is_skeleton(&self) -> bool;
}

/// A borrowed source reads as the source it borrows.
impl<T: ColumnarSource + ?Sized> ColumnarSource for &T {
    #[inline(always)]
    fn get_weight(&self, row: usize) -> i64 {
        (**self).get_weight(row)
    }

    #[inline(always)]
    fn to_unified(
        &self,
        schema: &SchemaDescriptor,
        cols: &mut Vec<ColPtr>,
        window: Range<usize>,
        decoded: &mut DecodedColumns,
    ) -> UnifiedSource<'_> {
        (**self).to_unified(schema, cols, window, decoded)
    }

    #[inline(always)]
    fn is_skeleton(&self) -> bool {
        (**self).is_skeleton()
    }
}

/// The heap order [`drive`] and the read cursor share. Generic over the source
/// type, so each caller monomorphizes its own branch-free copy.
///
/// `compare_pk_ordering` on each player's full OPK bytes (exact at every width —
/// no cached key, no stride dispatch), then the `payload` order.
///
/// `coarsen` collapses the payload axis for a cursor holding a skeleton run: a
/// skeleton row sorts before every hydrated row of its PK, making it the exemplar
/// [`drive`] folds the whole PK group into. Tested ahead of `payload`, so a
/// skeleton row's absent columns are never read. A runtime flag, not a second
/// monomorphisation axis: it is constant for a cursor's life and reached only on
/// a PK tie.
#[inline]
pub(crate) fn merge_less<'a, S, P>(
    schema: &'a SchemaDescriptor,
    sources: &'a [S],
    payload: P,
    coarsen: bool,
) -> impl Fn(&HeapNode, &HeapNode) -> bool + Copy + 'a
where
    S: ColumnarSource,
    P: PayloadOrder + 'a,
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
                payload.compare(schema, &sources[a_src], a_row, &sources[b_src], b_row) == Ordering::Less
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
pub(crate) fn drive<S, P>(
    tree: &mut LoserTree,
    schema: &SchemaDescriptor,
    sources: &[S],
    cursors: &mut [PosCursor],
    payload: P,
    coarsen: bool,
    mut emit: impl FnMut(usize, usize, i64) -> ControlFlow<()>,
) where
    S: ColumnarSource,
    P: PayloadOrder,
{
    // `less` and `same_group` read `(source_idx, row)` out of the heap node and
    // never touch `cursors`, so they coexist with the `&mut cursors` `step!` holds.
    let less = merge_less(schema, sources, payload, coarsen);
    let same_group = |a_src: usize, a_row: usize, b_src: usize, b_row: usize| {
        if !pk_bytes_eq(sources[a_src].get_pk_bytes(a_row), sources[b_src].get_pk_bytes(b_row)) {
            return false;
        }
        if coarsen && (sources[a_src].is_skeleton() || sources[b_src].is_skeleton()) {
            return true;
        }
        payload.compare(schema, &sources[a_src], a_row, &sources[b_src], b_row) == Ordering::Equal
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

    while let Some(top) = tree.peek() {
        let (group_src, group_row) = (top.source_idx as usize, top.row as usize);

        // Open the group: take the root's weight and step past it. The first row
        // is the exemplar, so `same_group` would be tautologically true — and its
        // payload term walks every column.
        let mut net_weight: i64 = sources[group_src].get_weight(group_row);
        step!(group_src);

        // Fold tied rows: each iteration peeks the new root, breaks at the group
        // boundary, otherwise accumulates weight and steps again.
        while let Some(top) = tree.peek() {
            let (cur_src, cur_row) = (top.source_idx as usize, top.row as usize);
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

/// The tournament build for [`run_merge`]; the walk itself is [`drive`].
/// Monomorphised per (source, payload) so the hot loop stays branch-free.
#[inline]
fn run_merge_body<S, P>(
    sources: &[S],
    cursors: &mut [PosCursor],
    schema: &SchemaDescriptor,
    mut emit: impl FnMut(usize, usize, i64),
    payload: P,
) where
    S: ColumnarSource,
    P: PayloadOrder,
{
    let mut tree = LoserTree::build(
        cursors.len(),
        |i| cursors[i].is_valid().then(|| cursors[i].position as u32),
        merge_less(schema, sources, payload, false),
    );
    // `coarsen: false` — skeleton folding is a read-path concern, and compaction
    // re-materializes per PK anyway (`compact::merge_and_route`). The literal also
    // const-folds the skeleton test out of this monomorphisation.
    drive(&mut tree, schema, sources, cursors, payload, false, |src, row, w| {
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
    /// dropped.
    ///
    /// Only the shared-PK arm folds: a galloped run stops at the other side's
    /// head PK, so nothing in it can share a (PK, payload) across sides, and a
    /// consolidated input repeats none internally.
    pub(crate) fn merged_consolidated(&self, other: &Batch, schema: &SchemaDescriptor) -> Batch {
        debug_assert!(
            self.is_consolidated() && other.is_consolidated(),
            "merged_consolidated: both inputs must be consolidated",
        );
        let mut out = with_payload_cmp!(schema, merged_consolidated_body, self, other, schema);
        out.certify_layout(Layout::Consolidated);
        out
    }
}

#[inline]
fn merged_consolidated_body<P: PayloadOrder>(a: &Batch, b: &Batch, schema: &SchemaDescriptor, payload: P) -> Batch {
    let (n_a, n_b) = (a.count, b.count);
    let (mb_a, mb_b) = (a.as_mem_batch(), b.as_mem_batch());

    let mut out = Batch::with_capacity(schema, n_a + n_b);
    {
        // One session for the whole merge: expected run length is 2 for a set
        // operation's uniform 128-bit PKs, so per-run setup would dominate.
        let mut sink = out.append_session(n_a + n_b);
        let carry_a = sink.carry(&mb_a, &[(0, n_a)]);
        let carry_b = sink.carry(&mb_b, &[(0, n_b)]);
        let (mut ia, mut jb) = (0usize, 0usize);
        while ia < n_a && jb < n_b {
            match compare_pk_ordering(a.get_pk_bytes(ia), b.get_pk_bytes(jb)) {
                // A single-source run: one galloping skip, one bulk append.
                Ordering::Less => {
                    let s = ia;
                    ia = a.advance_to(b.get_pk_bytes(jb), ia);
                    sink.push_ranges(&mb_a, carry_a, &[(s, ia)]);
                }
                Ordering::Greater => {
                    let s = jb;
                    jb = b.advance_to(a.get_pk_bytes(ia), jb);
                    sink.push_ranges(&mb_b, carry_b, &[(s, jb)]);
                }
                // A shared PK: bracket both equal-PK groups and interleave them
                // by payload. The only arm that folds, and the only one that
                // reads row by row.
                Ordering::Equal => {
                    let (ga, gb) = (pk_group_end(a, ia), pk_group_end(b, jb));
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
                                Open::A(s) => sink.push_ranges(&mb_a, carry_a, &[(s, ia)]),
                                Open::B(s) => sink.push_ranges(&mb_b, carry_b, &[(s, jb)]),
                                Open::None => {}
                            }
                        };
                    }
                    while ia < ga && jb < gb {
                        match payload.compare(schema, &mb_a, ia, &mb_b, jb) {
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
                                let w = a.get_weight(ia) + b.get_weight(jb);
                                sink.push_row_at(&mb_a, carry_a, ia, w);
                                sink.leave_out(&mb_b, carry_b, jb);
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
                            sink.push_ranges(&mb_a, carry_a, &[(s, ga)]);
                            sink.push_ranges(&mb_b, carry_b, &[(jb, gb)]);
                        }
                        Open::B(s) => {
                            sink.push_ranges(&mb_b, carry_b, &[(s, gb)]);
                            sink.push_ranges(&mb_a, carry_a, &[(ia, ga)]);
                        }
                        Open::None => {
                            sink.push_ranges(&mb_a, carry_a, &[(ia, ga)]);
                            sink.push_ranges(&mb_b, carry_b, &[(jb, gb)]);
                        }
                    }
                    // The only advance that is not an `advance_to`.
                    ia = ga;
                    jb = gb;
                }
            }
        }
        sink.push_ranges(&mb_a, carry_a, &[(ia, n_a)]);
        sink.push_ranges(&mb_b, carry_b, &[(jb, n_b)]);
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

/// The argsort half of [`consolidate_groups`]: [`PkSortKey`] carries the whole
/// OPK image, so a key tie goes straight to the payload order.
#[inline]
fn consolidate_groups_inner<P: PayloadOrder>(
    n: usize,
    batch: &MemBatch,
    schema: &SchemaDescriptor,
    out: &mut Vec<(u32, u32, i64)>,
    payload: P,
) {
    pk_width_dispatch!(batch.pk_stride as usize, |K| {
        let mut entries: Vec<SortEntry<K>> = (0..n as u32)
            .map(|i| SortEntry {
                key: K::from_opk(batch.get_pk_bytes(i as usize)),
                idx: i,
            })
            .collect();
        entries.sort_unstable_by(|a, b| {
            let (x, y) = (a.idx as usize, b.idx as usize);
            Ord::cmp(&a.key, &b.key).then_with(|| payload.compare(schema, batch, x, batch, y))
        });
        drain_groups(&entries, batch, schema, payload, out);
    })
}

/// The fold half of [`consolidate_groups`]: walk the sorted entries and push one
/// survivor per (PK, payload) group whose weights do not cancel.
#[inline]
fn drain_groups<K: Copy + Eq, P: PayloadOrder>(
    entries: &[SortEntry<K>],
    batch: &MemBatch,
    schema: &SchemaDescriptor,
    payload: P,
    out: &mut Vec<(u32, u32, i64)>,
) {
    let mut pending = entries[0];
    let mut pending_weight = batch.get_weight(pending.idx as usize);

    for cur in &entries[1..] {
        let (pi, ci) = (pending.idx as usize, cur.idx as usize);
        if pending.key == cur.key && payload.compare(schema, batch, pi, batch, ci).is_eq() {
            pending_weight += batch.get_weight(ci);
        } else {
            if pending_weight != 0 {
                out.push((0, pending.idx, pending_weight));
            }
            pending = *cur;
            pending_weight = batch.get_weight(ci);
        }
    }
    if pending_weight != 0 {
        out.push((0, pending.idx, pending_weight));
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
#[path = "tests/merge.rs"]
mod tests;
