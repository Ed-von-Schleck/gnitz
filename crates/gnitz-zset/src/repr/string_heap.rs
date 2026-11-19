//! The German-string heap: relocating a cell's span into another heap, the
//! span dedup a relocation runs under, and the dead-byte accounting that
//! decides between carrying a source heap whole and relocating out of it.

use std::cell::Cell;
use std::collections::VecDeque;

use super::batch::{MemBatch, range_rows};
use super::codebook::Codebook;
use super::pool::{self, is_tight};
use crate::schema::SchemaDescriptor;
use gnitz_wire::TypeCode;
use gnitz_wire::{Heap, HeapValue, RowSource};
use rustc_hash::FxHashMap;

type SpanKey = (usize, usize);
type SpanMap = FxHashMap<SpanKey, usize>;

/// One plain source span, identified by its address and length, as
/// [`HeapValue::key`] identifies a value. Shared so a sizing pass charges
/// exactly the spans a relocation copies.
#[inline]
pub(super) fn blob_span_key(content: &[u8]) -> SpanKey {
    (content.as_ptr() as usize, content.len())
}

/// Reserve hint for a destination heap taking `out_rows` of a `src_rows`-row
/// source whose heap is `src_blob` bytes: that slice's row-proportional share,
/// rounded up, and at most the whole heap.
pub fn prorated_blob_cap(src_blob: usize, src_rows: usize, out_rows: usize) -> usize {
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
pub fn should_relocate_blob(src_blob: usize, src_rows: usize, out_rows: usize) -> bool {
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
pub fn carried_dead(
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

/// Cells [`carry_dead`] charges dead between two waste checks.
const DEAD_CHECK_CELLS: usize = 1024;

/// [`carried_dead`] for copying the German-string slots `kept_slots` of `src`'s
/// rows `kept`, ascending and disjoint: the string slots it leaves behind are
/// charged dead as well. `None` when `src` has no string slot, when no kept
/// slot reads a non-empty heap, or when carrying loses to relocating.
#[inline]
pub(super) fn carry_dead(src: &MemBatch<'_>, kept_slots: u64, kept: &[(usize, usize)]) -> Option<usize> {
    let mask = src.schema.string_payload_slots();
    if mask == 0 {
        // No string cell to rebase onto a heap.
        return None;
    }
    debug_assert!(kept.is_sorted_by(|a, b| a.1 <= b.0) && kept.last().is_none_or(|k| k.1 <= src.count));
    debug_assert_eq!(kept_slots & !mask, 0, "carry_dead: a kept slot is not a string slot");
    let heap = src.blob.len();
    if kept_slots == 0 && heap != 0 {
        return None;
    }
    let mut dead = src.dead_heap;
    for pi in gnitz_wire::BitIter(mask & !kept_slots) {
        let cells = src.col_data(pi).as_chunks::<16>().0;
        for &(start, end) in kept {
            for run in cells[start..end].chunks(DEAD_CHECK_CELLS) {
                dead += run.iter().map(gnitz_wire::german_string_long_len).sum::<usize>();
                if heap_is_wasteful(dead, heap) {
                    return None;
                }
            }
        }
    }
    // The long bytes of the gaps between the kept ranges.
    let excluded = || {
        let ends = kept.iter().map(|&(_, e)| e);
        let starts = kept.iter().map(|&(s, _)| s).chain([src.count]);
        (std::iter::once(0).chain(ends).zip(starts))
            .map(|(gap_start, gap_end)| long_bytes(src, mask, gap_start..gap_end))
            .sum()
    };
    carried_dead(heap, dead, src.count, range_rows(kept), excluded)
}

/// The heap bytes the long cells of rows `rows` of `src` name in the
/// German-string slots `mask` name, a column at a time. A span two cells share
/// counts twice.
pub fn long_bytes(src: &MemBatch<'_>, mask: u64, rows: std::ops::Range<usize>) -> usize {
    gnitz_wire::BitIter(mask)
        .map(|pi| {
            let cells = &src.col_data(pi).as_chunks::<16>().0[rows.clone()];
            cells.iter().map(gnitz_wire::german_string_long_len).sum::<usize>()
        })
        .sum()
}

/// [`row_long_bytes`] summed over every row of `src` outside the ascending,
/// disjoint `kept` ranges.
pub fn long_bytes_outside<S: RowSource>(src: &S, mask: u64, kept: &[(usize, usize)]) -> usize {
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
pub fn row_long_bytes<S: RowSource>(src: &S, mask: u64, row: usize) -> usize {
    gnitz_wire::BitIter(mask)
        .map(|pi| gnitz_wire::german_string_long_len(src.german_cell(row, pi)))
        .sum()
}

/// Where a long value of a German-string cell copied verbatim from a source
/// lands in the destination heap.
#[derive(Clone, Copy)]
pub enum StringDest<'b> {
    /// The source heap was carried whole onto the destination's at this base:
    /// each cell's offset is shifted by it.
    Carried(usize),
    /// Each value is placed on its own: decoded into a plain heap, or coded
    /// under the codebook where one is given (see [`Codebook::append`]).
    Placed(Option<&'b Codebook>),
}

impl From<Option<usize>> for StringDest<'_> {
    /// A carried plain heap's base, else each value relocated as it is.
    fn from(heap_at: Option<usize>) -> Self {
        heap_at.map_or(StringDest::Placed(None), StringDest::Carried)
    }
}

/// Rebase `cells`, whole German-string cells copied verbatim from a source
/// whose heap is `src`, onto the destination heap `dst_blob` as `dest` says,
/// under `cache` when there is one.
#[inline]
pub(super) fn rebase_string_cells(
    cells: &mut [u8],
    src: Heap<'_>,
    dst_blob: &mut Vec<u8>,
    dest: StringDest<'_>,
    mut cache: Option<&mut BlobCache>,
) {
    match dest {
        StringDest::Carried(base) => gnitz_wire::shift_german_string_heaps(cells, base),
        StringDest::Placed(None) => relocate_cells(cells.as_chunks_mut::<16>().0, src, dst_blob, cache),
        StringDest::Placed(book) => {
            for cell in cells.as_chunks_mut::<16>().0 {
                rebase_string_cell(cell, src, dst_blob, StringDest::Placed(book), cache.as_deref_mut());
            }
        }
    }
}

/// [`rebase_string_cells`] for one cell, inlined into a loop that picks a
/// source per cell.
#[inline(always)]
pub(super) fn rebase_string_cell(
    cell: &mut [u8; 16],
    src: Heap<'_>,
    dst_blob: &mut Vec<u8>,
    dest: StringDest<'_>,
    cache: Option<&mut BlobCache>,
) {
    match dest {
        StringDest::Carried(base) => gnitz_wire::shift_german_string_heaps(cell, base),
        StringDest::Placed(None) => *cell = relocated_cell(cell, src, dst_blob, cache),
        StringDest::Placed(book) => {
            *cell = gnitz_wire::relocate_german_value_with(cell, src, |value| place_value(value, dst_blob, book, cache))
        }
    }
}

/// Relocate each of `cells`. Out of line: a caller's loop over fixed-width
/// regions is compiled without it.
#[inline(never)]
fn relocate_cells(cells: &mut [[u8; 16]], src: Heap<'_>, dst_blob: &mut Vec<u8>, cache: Option<&mut BlobCache>) {
    // The heap and the cache are matched once, so no loop tests for either per cell.
    match (src, cache) {
        (Heap::Plain(blob), Some(cache)) => {
            for cell in cells {
                relocate_plain(cell, blob, dst_blob, Some(&mut *cache));
            }
        }
        (Heap::Plain(blob), None) => {
            for cell in cells {
                relocate_plain(cell, blob, dst_blob, None);
            }
        }
        (Heap::Coded(_), cache) => relocate_coded_cells(cells, src, dst_blob, cache),
    }
}

/// [`relocate_cells`] out of a coded heap. Out of line: inlined into
/// `relocate_cells`, it changes how the plain arm's loop compiles.
#[inline(never)]
fn relocate_coded_cells(
    cells: &mut [[u8; 16]],
    src: Heap<'_>,
    dst_blob: &mut Vec<u8>,
    mut cache: Option<&mut BlobCache>,
) {
    for cell in cells {
        relocate_value(cell, src, dst_blob, cache.as_deref_mut());
    }
}

/// `cell` rebased onto `dst_blob`: [`gnitz_wire::relocate_german_string_with`]
/// appending to it, copying each source value once when `cache` is `Some`.
#[inline]
pub fn relocated_cell(
    cell: &[u8; 16],
    src: Heap<'_>,
    dst_blob: &mut Vec<u8>,
    cache: Option<&mut BlobCache>,
) -> [u8; 16] {
    let mut cell = *cell;
    match src {
        Heap::Plain(blob) => relocate_plain(&mut cell, blob, dst_blob, cache),
        Heap::Coded(_) => relocate_value(&mut cell, src, dst_blob, cache),
    }
    cell
}

/// [`relocated_cell`] out of the plain heap `src_blob`.
#[inline]
fn relocate_plain(cell: &mut [u8; 16], src_blob: &[u8], dst_blob: &mut Vec<u8>, cache: Option<&mut BlobCache>) {
    *cell = gnitz_wire::relocate_german_string_with(cell, src_blob, |content| place_span(content, dst_blob, cache));
}

/// [`relocated_cell`] out of a coded heap, each value decoded into
/// `dst_blob`. Out of line: a plain heap's relocation inlines without it.
#[inline(never)]
fn relocate_value(cell: &mut [u8; 16], src: Heap<'_>, dst_blob: &mut Vec<u8>, cache: Option<&mut BlobCache>) {
    *cell = gnitz_wire::relocate_german_value_with(cell, src, |value| place_value(value, dst_blob, None, cache));
}

/// Where `content`, a span of a plain source heap, sits in `dst_blob`:
/// appended, or where `cache` already put it.
fn place_span(content: &[u8], dst_blob: &mut Vec<u8>, cache: Option<&mut BlobCache>) -> usize {
    let at = dst_blob.len();
    match cache {
        Some(cache) => *cache.map().entry(blob_span_key(content)).or_insert_with(|| {
            dst_blob.extend_from_slice(content);
            at
        }),
        None => {
            dst_blob.extend_from_slice(content);
            at
        }
    }
}

/// [`place_span`] of a value of any heap into `dst_blob`: decoded into a plain
/// one, or coded under `book` (see [`Codebook::append`]).
fn place_value(
    value: HeapValue<'_>,
    dst_blob: &mut Vec<u8>,
    book: Option<&Codebook>,
    cache: Option<&mut BlobCache>,
) -> usize {
    let at = dst_blob.len();
    match cache {
        Some(cache) => *cache.map().entry(value.key()).or_insert_with(|| {
            append_value(value, dst_blob, book);
            at
        }),
        None => {
            append_value(value, dst_blob, book);
            at
        }
    }
}

/// Append `value` to `dst_blob`: decoded, or coded under `book`.
#[inline(always)]
pub fn append_value(value: HeapValue<'_>, dst_blob: &mut Vec<u8>, book: Option<&Codebook>) {
    match book {
        None => value.append_to(dst_blob),
        Some(book) => book.append(value, dst_blob),
    }
}

// ---------------------------------------------------------------------------
// Blob cache
// ---------------------------------------------------------------------------

/// The most entries a [`BlobCache`] reserves up front. Callers pass a cell count,
/// but only long strings reach the map.
const BLOB_CACHE_RESERVE_CAP: usize = 4096;

thread_local! {
    static BLOB_CACHE_POOL: Cell<VecDeque<SpanMap>> = const { Cell::new(VecDeque::new()) };
}

/// Dedups long-string spans relocated into one destination heap, so each is
/// copied once. Its map is pooled, and taken at the first relocation.
pub struct BlobCache {
    map: Option<SpanMap>,
    /// String cells the cache relocates at most: what its map is sized and
    /// chosen by.
    cells: usize,
}

impl BlobCache {
    fn new(cells: usize) -> Self {
        BlobCache { map: None, cells }
    }

    /// A cache sized for `rows` rows of `schema`: the most string cells they
    /// hold.
    pub fn for_rows(schema: &SchemaDescriptor, rows: usize) -> Self {
        BlobCache::new(rows * schema.string_payload_slots().count_ones() as usize)
    }

    #[inline]
    pub(super) fn map(&mut self) -> &mut SpanMap {
        let cells = self.cells;
        self.map.get_or_insert_with(|| {
            // Clearing a map costs its capacity, whatever it held, so a cache
            // takes none larger than twice its own cells — and none smaller
            // than its reserve, which would grow it.
            let reserve = cells.min(BLOB_CACHE_RESERVE_CAP);
            let fits = |m: &SpanMap| m.capacity() >= reserve && is_tight(m.capacity(), cells.max(4));
            let mut map = pool::take(&BLOB_CACHE_POOL, fits).unwrap_or_default();
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
            pool::recycle(&BLOB_CACHE_POOL, map, bytes);
        }
    }
}

/// The exact count of `mb`'s heap bytes no long string cell references, a cell
/// overrunning the heap referencing none.
pub(crate) fn measure_dead_heap(mb: &MemBatch<'_>) -> usize {
    walk_heap_spans(mb, |_, _| true).expect("an accepting walk")
}

/// Mark every German-string cell's heap span, after `accept` has passed the
/// cell, and answer the heap bytes left unmarked; `None` at the first cell
/// `accept` refuses.
pub(super) fn walk_heap_spans(mb: &MemBatch<'_>, mut accept: impl FnMut(&[u8; 16], TypeCode) -> bool) -> Option<usize> {
    let schema = mb.schema;
    let heap = mb.blob.len();
    if !schema.has_german_string() {
        return Some(heap);
    }
    let mut live = vec![0u64; heap.div_ceil(64)];
    for (pi, col) in schema.payload_columns() {
        if !col.type_code.is_german_string() {
            continue;
        }
        for cell in mb.col_data(pi).as_chunks::<16>().0 {
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
