//! The direct row writer: a batch's tail or a wire block's fixed bytes, written
//! a region at a time.

use super::batch::{FIXED_REGION_BYTES, REG_NULL_BMP, REG_PAYLOAD_START, REG_PK, REG_WEIGHT};
use super::merge::MemBatch;
use super::string_heap::{rebase_string_cells, BlobCache};
use crate::schema::SchemaDescriptor;

/// Writes rows `[at, at + rows)` of a pre-allocated arena, each region handed
/// out as a slice of exactly those rows: a batch's tail, or a wire block's fixed
/// bytes. A batch counts the rows only once its writer is done with them (see
/// `AppendSession::write`), so no reader reaches a row before its regions are
/// written.
///
/// **Whoever fills a writer writes every byte of every one of its rows** — the
/// batch invariant (see `Batch::with_capacity`): the arena skips its memset, so
/// a skipped cell leaks the recycled buffer's previous contents rather than
/// reading back a zero. Hence the `repr::scatter` kernels take exactly `rows`
/// rows and write every payload cell unconditionally, a null cell included.
pub(crate) struct DirectWriter<'a> {
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
    /// Borrowed, not owned: the scatter reads it per column, and copying it in
    /// would put a `memcpy` on every writer open.
    pub schema: &'a SchemaDescriptor,
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
        }
    }

    /// Rows this writer fills.
    #[inline(always)]
    pub(crate) fn rows(&self) -> usize {
        self.rows
    }

    /// The arena through this writer's last row, read back: the rows before
    /// this writer's and whatever of its own it has written.
    #[inline]
    pub(crate) fn written(&self) -> MemBatch<'_> {
        MemBatch {
            data: self.data,
            schema: self.schema,
            cap: self.cap,
            blob: self.blob.as_slice(),
            count: self.at + self.rows,
            dead_heap: 0,
        }
    }

    /// The index in [`Self::written`] of this writer's first row.
    #[inline(always)]
    pub(crate) fn first_row(&self) -> usize {
        self.at
    }

    /// Region `r`, bounded to this writer's rows.
    #[inline(always)]
    pub(crate) fn region_mut(&mut self, r: usize) -> &mut [u8] {
        let stride = self.schema.region_stride(r);
        let start = self.schema.region_start(r, self.cap) + self.at * stride;
        &mut self.data[start..start + self.rows * stride]
    }

    /// The PK region, bounded to this writer's rows.
    #[inline(always)]
    pub(crate) fn pk_mut(&mut self) -> &mut [u8] {
        self.region_mut(REG_PK)
    }

    /// The weight region, bounded to this writer's rows.
    #[inline(always)]
    pub(crate) fn weight_mut(&mut self) -> &mut [u8] {
        self.region_mut(REG_WEIGHT)
    }

    /// The null-word region, bounded to this writer's rows.
    #[inline(always)]
    pub(crate) fn null_bmp_mut(&mut self) -> &mut [u8] {
        self.region_mut(REG_NULL_BMP)
    }

    /// The PK, weight and null-word regions, each bounded to this writer's rows.
    #[inline]
    pub(crate) fn fixed_mut(&mut self) -> (&mut [u8], &mut [u8], &mut [u8]) {
        let pk_stride = self.schema.pk_stride();
        let (pk, rest) = self.data.split_at_mut(self.cap * pk_stride);
        let (weight, rest) = rest.split_at_mut(self.cap * 8);
        (
            &mut pk[self.at * pk_stride..][..self.rows * pk_stride],
            &mut weight[self.at * 8..][..self.rows * 8],
            &mut rest[self.at * 8..][..self.rows * 8],
        )
    }

    /// Every region in order, each bounded to this writer's rows, with the heap
    /// a relocation appends to and the dedup cache it runs under.
    pub(crate) fn split_mut(&mut self) -> (impl Iterator<Item = &mut [u8]>, &mut Vec<u8>, Option<&mut BlobCache>) {
        let (schema, cap, at, rows) = (self.schema, self.cap, self.at, self.rows);
        let mut rest = &mut *self.data;
        let regions = (0..schema.num_regions()).map(move |r| {
            let stride = schema.region_stride(r);
            let (region, tail) = std::mem::take(&mut rest).split_at_mut(cap * stride);
            rest = tail;
            &mut region[at * stride..(at + rows) * stride]
        });
        (regions, &mut *self.blob, self.blob_cache.as_deref_mut())
    }

    /// Payload column `pi`'s region, bounded to this writer's rows.
    #[inline(always)]
    pub(crate) fn col_mut(&mut self, pi: usize) -> &mut [u8] {
        self.region_mut(REG_PAYLOAD_START + pi)
    }

    /// String column `pi`'s region, with the heap a relocation into it appends
    /// to and the dedup cache it runs under.
    #[inline(always)]
    pub(crate) fn string_col_mut(&mut self, pi: usize) -> (&mut [u8], &mut Vec<u8>, Option<&mut BlobCache>) {
        let start = self.schema.region_start(REG_PAYLOAD_START + pi, self.cap) + self.at * 16;
        (
            &mut self.data[start..start + self.rows * 16],
            &mut *self.blob,
            self.blob_cache.as_deref_mut(),
        )
    }

    /// Rebase string column `pi`, whose cells were copied verbatim from a source
    /// whose heap is `src_blob`; see [`rebase_string_cells`].
    #[inline]
    pub(crate) fn rebase_string_col(&mut self, pi: usize, src_blob: &[u8], heap_at: Option<usize>) {
        let (cells, blob, cache) = self.string_col_mut(pi);
        rebase_string_cells(cells, src_blob, blob, heap_at, cache);
    }

    /// Carry a source heap whole onto the end of this writer's heap. Returns the
    /// base the caller shifts its copied cells' offsets by.
    pub(super) fn adopt_heap(&mut self, src_blob: &[u8]) -> usize {
        let base = self.blob.len();
        self.blob.extend_from_slice(src_blob);
        base
    }
}

impl gnitz_expr::MapTarget for DirectWriter<'_> {
    #[inline(always)]
    fn null_bmp_mut(&mut self) -> &mut [u8] {
        DirectWriter::null_bmp_mut(self)
    }
    #[inline(always)]
    fn slot_mut(&mut self, pi: usize) -> (&mut [u8], &mut [u8], &mut Vec<u8>) {
        let r = REG_PAYLOAD_START + pi;
        let stride = self.schema.region_stride(r);
        // The null words lie below every payload column.
        let (lo, hi) = self.data.split_at_mut(self.schema.region_start(r, self.cap));
        let nulls = self.schema.region_start(REG_NULL_BMP, self.cap) + self.at * FIXED_REGION_BYTES;
        (
            &mut hi[self.at * stride..][..self.rows * stride],
            &mut lo[nulls..][..self.rows * FIXED_REGION_BYTES],
            &mut *self.blob,
        )
    }
}
