//! The direct row writer: a batch's tail, a batch's own rows, or a wire block's
//! fixed bytes, written a region at a time.

use super::batch::MemBatch;
use super::string_heap::{BlobCache, StringDest, rebase_string_cells};
use crate::schema::SchemaDescriptor;
use gnitz_wire::{FIXED_REGION_BYTES, Heap, REG_NULL_BMP, REG_PAYLOAD_START, REG_PK, REG_WEIGHT};
use std::ops::Range;

/// Writes rows `[at, at + rows)` of a pre-allocated arena, each region handed
/// out as a slice of exactly those rows: a batch's tail, a batch's own rows
/// ([`Batch::rewrite`](super::Batch::rewrite)), or a wire block's fixed bytes.
///
/// **Whoever fills a writer over a tail or a block writes every byte of every
/// row it hands out, a NULL cell's included**: the arena is uninitialised, and
/// a batch counts the rows only once the writer is done. A rewrite writes whole
/// every cell it overwrites.
pub struct DirectWriter<'a> {
    /// An arena of `cap` rows under `schema`.
    data: &'a mut [u8],
    cap: usize,
    at: usize,
    rows: usize,
    /// The heap a relocated string lands in; `blob.len()` is the next offset.
    blob: &'a mut Vec<u8>,
    /// `None` relocates every cell on its own: two cells naming one source span
    /// each get a copy.
    blob_cache: Option<&'a mut BlobCache>,
    pub schema: &'a SchemaDescriptor,
    /// Some German-string cell was handed out to write, or the heap beside more
    /// than one column: a rewrite may have left a long value unreferenced.
    pub(super) strings_touched: bool,
}

impl<'a> DirectWriter<'a> {
    /// A writer of rows `[at, at + rows)` of `data`, an arena of `cap` rows
    /// under `schema`. A wire block's fixed bytes are an arena of exactly its
    /// rows.
    #[inline(always)]
    pub(super) fn over(
        data: &'a mut [u8],
        cap: usize,
        at: usize,
        rows: usize,
        schema: &'a SchemaDescriptor,
        blob: &'a mut Vec<u8>,
        blob_cache: Option<&'a mut BlobCache>,
    ) -> Self {
        debug_assert!(at + rows <= cap && data.len() >= cap * schema.row_width());
        DirectWriter {
            data,
            cap,
            at,
            rows,
            blob,
            blob_cache,
            schema,
            strings_touched: false,
        }
    }

    /// Rows this writer fills.
    #[inline(always)]
    pub fn rows(&self) -> usize {
        self.rows
    }

    /// The arena through this writer's last row, read back: the rows before
    /// this writer's and whatever of its own it has written. Its dead-heap bound
    /// is the whole heap, since the writer does not know the batch's.
    #[inline]
    pub(crate) fn written(&self) -> MemBatch<'_> {
        MemBatch {
            data: self.data,
            schema: self.schema,
            cap: self.cap,
            blob: self.blob.as_slice(),
            count: self.at + self.rows,
            dead_heap: self.blob.len(),
        }
    }

    /// The index in [`Self::written`] of this writer's first row.
    #[inline(always)]
    pub(crate) fn first_row(&self) -> usize {
        self.at
    }

    /// The bytes of `data` that hold this writer's rows of region `r`.
    #[inline(always)]
    fn span(&self, r: usize) -> Range<usize> {
        let stride = self.schema.region_stride(r);
        let start = self.schema.region_start(r, self.cap) + self.at * stride;
        start..start + self.rows * stride
    }

    /// Region `r`, bounded to this writer's rows.
    #[inline(always)]
    pub(super) fn region_mut(&mut self, r: usize) -> &mut [u8] {
        let s = self.span(r);
        &mut self.data[s]
    }

    /// The PK region, bounded to this writer's rows.
    #[inline(always)]
    pub fn pk_mut(&mut self) -> &mut [u8] {
        self.region_mut(REG_PK)
    }

    /// The weight region, bounded to this writer's rows.
    #[inline(always)]
    pub fn weight_mut(&mut self) -> &mut [u8] {
        self.region_mut(REG_WEIGHT)
    }

    /// The null-word region, bounded to this writer's rows.
    #[inline(always)]
    pub fn null_bmp_mut(&mut self) -> &mut [u8] {
        self.region_mut(REG_NULL_BMP)
    }

    /// Payload column `pi`'s null bit of each `(row, null)` of `rows`, set where
    /// `null` and cleared elsewhere.
    #[inline(always)]
    pub fn set_null_bits(&mut self, pi: usize, rows: impl Iterator<Item = (usize, bool)>) {
        let (byte, bit) = (pi / 8, 1u8 << (pi % 8));
        let words = self.null_bmp_mut().as_chunks_mut::<8>().0;
        for (row, null) in rows {
            let b = &mut words[row][byte];
            *b = (*b & !bit) | if null { bit } else { 0 };
        }
    }

    /// The PK, weight and null-word regions, each bounded to this writer's rows.
    #[inline]
    pub fn fixed_mut(&mut self) -> (&mut [u8], &mut [u8], &mut [u8]) {
        let (pk, wt, nb) = (self.span(REG_PK), self.span(REG_WEIGHT), self.span(REG_NULL_BMP));
        debug_assert!(wt.len() == self.rows * FIXED_REGION_BYTES && nb.len() == self.rows * FIXED_REGION_BYTES);
        let (lo, hi) = self.data.split_at_mut(nb.start);
        let (lo, mid) = lo.split_at_mut(wt.start);
        (&mut lo[pk], &mut mid[..wt.len()], &mut hi[..nb.len()])
    }

    /// Every region in order, each bounded to this writer's rows, with the heap
    /// a relocation appends to and the dedup cache it runs under.
    pub(crate) fn split_mut(&mut self) -> (impl Iterator<Item = &mut [u8]>, &mut Vec<u8>, Option<&mut BlobCache>) {
        self.strings_touched = true;
        let (schema, cap, at, rows) = (self.schema, self.cap, self.at, self.rows);
        let mut rest = &mut *self.data;
        let regions = (0..schema.num_fixed_regions()).map(move |r| {
            let stride = schema.region_stride(r);
            let (region, tail) = std::mem::take(&mut rest).split_at_mut(cap * stride);
            rest = tail;
            &mut region[at * stride..(at + rows) * stride]
        });
        (regions, &mut *self.blob, self.blob_cache.as_deref_mut())
    }

    /// The PK region and payload columns `cols`' regions, each bounded to this
    /// writer's rows, with the heap a string cell's bytes are appended to.
    #[inline(always)]
    pub fn pk_and_payload_mut(
        &mut self,
        cols: Range<usize>,
    ) -> (&mut [u8], impl Iterator<Item = &mut [u8]>, &mut Vec<u8>) {
        let (mut regions, blob, _) = self.split_mut();
        let pk = regions.nth(REG_PK).expect("a batch has a PK region");
        let cols = regions
            .skip(REG_PAYLOAD_START + cols.start - REG_PK - 1)
            .take(cols.len());
        (pk, cols, blob)
    }

    /// Payload column `pi`'s region, bounded to this writer's rows.
    #[inline(always)]
    pub fn col_mut(&mut self, pi: usize) -> &mut [u8] {
        self.strings_touched |= gnitz_wire::null_word_get(self.schema.string_payload_slots(), pi);
        self.region_mut(REG_PAYLOAD_START + pi)
    }

    /// String column `pi`'s region, with the heap a relocation into it appends
    /// to and the dedup cache it runs under.
    #[inline(always)]
    pub fn string_col_mut(&mut self, pi: usize) -> (&mut [u8], &mut Vec<u8>, Option<&mut BlobCache>) {
        debug_assert_eq!(self.schema.region_stride(REG_PAYLOAD_START + pi), 16);
        self.strings_touched = true;
        let s = self.span(REG_PAYLOAD_START + pi);
        (&mut self.data[s], &mut *self.blob, self.blob_cache.as_deref_mut())
    }

    /// Rebase string column `pi`, whose cells were copied verbatim from a source
    /// whose heap is `src`, as `dest` says; see [`rebase_string_cells`].
    #[inline]
    pub fn rebase_string_col(&mut self, pi: usize, src: Heap<'_>, dest: StringDest<'_>) {
        let (cells, blob, cache) = self.string_col_mut(pi);
        rebase_string_cells(cells, src, blob, dest, cache);
    }
}

impl gnitz_expr::MapTarget for DirectWriter<'_> {
    #[inline(always)]
    fn slot_mut(&mut self, pi: usize) -> (&mut [u8], &mut [u8], &mut Vec<u8>) {
        // Only a string slot's write reaches the heap.
        self.strings_touched |= gnitz_wire::null_word_get(self.schema.string_payload_slots(), pi);
        let (cells, nulls) = (self.span(REG_PAYLOAD_START + pi), self.span(REG_NULL_BMP));
        // The null words lie below every payload column.
        let (lo, hi) = self.data.split_at_mut(cells.start);
        (&mut hi[..cells.len()], &mut lo[nulls], &mut *self.blob)
    }
}
