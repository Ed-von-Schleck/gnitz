//! The German-string heap: relocating a cell's span into another heap, the
//! span dedup a relocation runs under, and the dead-byte accounting that
//! decides between carrying a source heap whole and relocating out of it.

use std::cell::Cell;
use std::collections::VecDeque;

use super::batch_pool::tls_pool;
use super::merge::MemBatch;
use crate::schema::SchemaDescriptor;
use gnitz_expr::RowSource;
use gnitz_wire::{write_u64_le, TypeCode};
use rustc_hash::FxHashMap;

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

/// Cost of relocating one German-string cell, in bytes of whole-heap memcpy;
/// `slice_blob_relocate_bench` measures it.
const RELOCATE_CELL_COST_BYTES: usize = 500;

/// Whether relocating `out_rows` rows' cells — a cell rewrite plus a row's share
/// of the heap each — costs less than copying the whole `src_blob`-byte heap.
pub(crate) fn should_relocate_blob(src_blob: usize, src_rows: usize, out_rows: usize) -> bool {
    src_rows > 0 && src_blob > out_rows.saturating_mul(RELOCATE_CELL_COST_BYTES + src_blob / src_rows)
}

/// A heap more than a quarter dead is compacted rather than carried.
pub(crate) fn heap_is_wasteful(dead: usize, heap: usize) -> bool {
    dead.saturating_mul(4) > heap
}

/// The dead-byte bound a destination takes on by carrying a whole heap, already
/// `dead`, to keep `kept_rows` of its `src_rows` rows; `None` when relocating
/// them is cheaper or the carried heap would be wasteful. `excluded`, the long
/// bytes of the rows left behind, runs only once the prorated estimate passes.
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

/// The heap bytes a long German-string cell names; none for a short one.
#[inline]
pub(super) fn cell_long_bytes(cell: &[u8]) -> usize {
    gnitz_wire::german_string_heap(cell, usize::MAX).map_or(0, |span| span.len())
}

/// [`row_long_bytes`] summed over every row of `src` outside the ascending,
/// disjoint `kept` ranges.
pub(super) fn long_bytes_outside<S: RowSource>(src: &S, mask: u64, kept: &[(usize, usize)]) -> usize {
    let bounds = kept.iter().copied().chain([(src.row_count(), src.row_count())]);
    let gaps = bounds.scan(0, |next, (start, end)| {
        let gap = *next..start;
        *next = end;
        Some(gap)
    });
    gaps.flatten().map(|row| row_long_bytes(src, mask, row)).sum()
}

/// The heap bytes `row`'s long cells in the German-string slots `mask` name.
/// A span two cells share counts twice.
#[inline]
pub(super) fn row_long_bytes<S: RowSource>(src: &S, mask: u64, row: usize) -> usize {
    gnitz_wire::BitIter(mask)
        .map(|pi| cell_long_bytes(src.get_col_ptr(row, pi, 16)))
        .sum()
}

/// Copy the whole German-string cells `src` onto `dst`, each rebased as
/// [`rebase_string_cell`] rebases one: the carried arm shifts them in bulk.
#[inline]
pub(crate) fn copy_string_cells(
    dst: &mut [u8],
    src: &[u8],
    src_blob: &[u8],
    dst_blob: &mut Vec<u8>,
    heap_at: Option<usize>,
    cache: &mut BlobCache,
) {
    match heap_at {
        Some(base) => {
            dst.copy_from_slice(src);
            gnitz_wire::shift_german_string_heaps(dst, base);
        }
        None => {
            for (d, s) in dst.as_chunks_mut::<16>().0.iter_mut().zip(src.as_chunks::<16>().0) {
                *d = relocate_german_string_vec(s, src_blob, dst_blob, Some(&mut *cache));
            }
        }
    }
}

/// Write `src_cell` into `dst`, rebased onto the destination heap: shifted
/// onto `src_blob` carried there at `heap_at`, or relocated into `dst_blob`
/// under `cache`.
#[inline(always)]
pub(super) fn rebase_string_cell(
    dst: &mut [u8],
    src_cell: &[u8],
    src_blob: &[u8],
    dst_blob: &mut Vec<u8>,
    heap_at: Option<usize>,
    cache: &mut BlobCache,
) {
    match heap_at {
        Some(base) => {
            dst.copy_from_slice(&src_cell[..16]);
            gnitz_wire::shift_german_string_heaps(dst, base);
        }
        None => dst.copy_from_slice(&relocate_german_string_vec(src_cell, src_blob, dst_blob, Some(cache))),
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

    #[inline]
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

/// The exact count of `mb`'s heap bytes no long string cell references, a cell
/// overrunning the heap referencing none.
pub(super) fn measure_dead_heap(mb: &MemBatch<'_>, schema: &SchemaDescriptor) -> usize {
    walk_heap_spans(mb, schema, |_, _| true).expect("an accepting walk")
}

/// Mark every German-string cell's heap span, after `accept` has passed the
/// cell, and answer the heap bytes left unmarked; `None` at the first cell
/// `accept` refuses.
pub(super) fn walk_heap_spans(
    mb: &MemBatch<'_>,
    schema: &SchemaDescriptor,
    mut accept: impl FnMut(&[u8; 16], TypeCode) -> bool,
) -> Option<usize> {
    let heap = mb.blob.len();
    if !schema.has_german_string() {
        return Some(heap);
    }
    let mut live = vec![0u64; heap.div_ceil(64)];
    for (pi, col) in schema.payload_columns() {
        if !col.type_code.is_german_string() {
            continue;
        }
        for cell in mb.col_data(pi, 16).as_chunks::<16>().0 {
            if !accept(cell, col.type_code) {
                return None;
            }
            if let Some(span) = gnitz_wire::german_string_heap(cell, heap) {
                mark_bits(&mut live, span);
            }
        }
    }
    let marked: usize = live.iter().map(|w| w.count_ones() as usize).sum();
    Some(heap - marked)
}

/// Set bits `span` of the bitset `bits`.
fn mark_bits(bits: &mut [u64], span: std::ops::Range<usize>) {
    let (mut at, end) = (span.start, span.end);
    while at < end {
        let (word, bit) = (at / 64, at % 64);
        let n = (64 - bit).min(end - at);
        bits[word] |= gnitz_wire::low_bits_mask(n) << bit;
        at += n;
    }
}

#[cfg(test)]
#[path = "tests/string_heap.rs"]
mod tests;
