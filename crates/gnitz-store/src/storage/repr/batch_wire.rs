//! Wire serialization for `Batch`: wire sizing and chunking, encoding, and
//! WAL-block decoding with the validation it runs.

use super::batch::{row_width, strides_from_schema, Batch, MAX_BATCH_REGIONS, REG_PAYLOAD_START, REG_PK};
use super::batch_pool::{acquire_arena, recycle_buf};
use super::merge::{DirectWriter, MemBatch};
use super::string_heap::{blob_span_key, copy_string_cells, prorated_blob_cap, walk_heap_spans, BlobCache};
use crate::schema::{SchemaDescriptor, SchemaFacts};
use gnitz_wire::wal;
use gnitz_wire::{Regions, TypeCode};

/// Each fixed region's offset in a `rows`-row block.
fn wire_offsets(strides: &[u8], nr: usize, rows: usize, offsets: &mut [usize; MAX_BATCH_REGIONS]) {
    let mut off = wal::WAL_HEADER_SIZE;
    for r in 0..nr {
        offsets[r] = off;
        off += rows * strides[r] as usize;
    }
}

/// The rows of one reply frame, as [`Batch::wire_frame_within`] sized them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WireFrame {
    start: usize,
    rows: usize,
    /// Heap bytes the encode relocates for the rows.
    heap: usize,
}

impl WireFrame {
    pub fn rows(&self) -> usize {
        self.rows
    }

    /// The row after the frame's last.
    pub fn end(&self) -> usize {
        self.start + self.rows
    }
}

impl Batch {
    // ── Wire serialization (used by the server's SAL and frame codecs) ─────

    /// Byte count of the WAL-block encoding for this batch.
    pub fn wire_byte_size(&self) -> usize {
        self.wire_byte_size_range(self.count) + self.blob.len()
    }

    /// Byte count of the WAL-block encoding for `count` rows from this batch,
    /// with an empty heap.
    pub fn wire_byte_size_range(&self, count: usize) -> usize {
        wal::WAL_HEADER_SIZE + count * row_width(self.strides())
    }

    /// The frame from `start` that fits `budget` beside `overhead` bytes of
    /// frame around it; at least one row while any remain, however wide.
    pub fn wire_frame_within(&self, start: usize, overhead: usize, budget: usize) -> WireFrame {
        let slots = self.heap_referencing_slots();
        let (rows, heap) = match slots {
            0 => (self.rows_by_width(start, overhead, budget), 0),
            _ => self.rows_with_heap(start..self.count, slots, overhead, budget),
        };
        let frame = WireFrame { start, rows, heap };
        debug_assert!(
            rows <= 1 || overhead + self.wire_frame_size(&frame) <= budget,
            "a frame of {rows} rows encodes past {budget}",
        );
        frame
    }

    /// Bytes [`Self::encode_frame`] writes for `frame`.
    pub fn wire_frame_size(&self, frame: &WireFrame) -> usize {
        self.wire_byte_size_range(frame.rows) + frame.heap
    }

    /// Payload slots whose cells can reference this batch's heap.
    pub fn heap_referencing_slots(&self) -> u64 {
        if self.blob.is_empty() {
            0
        } else {
            self.schema().string_payload_slots()
        }
    }

    /// Rows from `start` that fit `budget` by fixed width alone.
    fn rows_by_width(&self, start: usize, overhead: usize, budget: usize) -> usize {
        let slope = row_width(self.strides());
        (self.count - start).min((budget.saturating_sub(overhead + wal::WAL_HEADER_SIZE) / slope).max(1))
    }

    /// How many of `rows`, from the front, fit `budget` once the heap bytes they
    /// relocate are charged, and those heap bytes. `slots` names the
    /// German-string payload slots.
    fn rows_with_heap(
        &self,
        rows: impl ExactSizeIterator<Item = usize>,
        slots: u64,
        overhead: usize,
        budget: usize,
    ) -> (usize, usize) {
        let slope = row_width(self.strides());
        let base = overhead + wal::WAL_HEADER_SIZE;
        let mut seen = BlobCache::new(rows.len());
        let mut heap = 0usize;
        let mut fit = 0usize;
        for row in rows {
            let row_heap = heap + self.row_heap_cost(row, slots, &mut seen);
            // The first row goes in whatever it costs; a frame carries whole rows.
            if fit > 0 && base + (fit + 1) * slope + row_heap > budget {
                break;
            }
            heap = row_heap;
            fit += 1;
        }
        (fit, heap)
    }

    /// Heap bytes row `row` adds to a chunk whose spans are already in `seen`.
    fn row_heap_cost(&self, row: usize, slots: u64, seen: &mut BlobCache) -> usize {
        let mut cost = 0;
        for pi in gnitz_wire::BitIter(slots) {
            let cell = self.get_col_ptr(row, pi, 16);
            let Some(span) = gnitz_wire::german_string_heap(cell, self.blob.len()) else {
                continue;
            };
            let len = span.end - span.start;
            if seen
                .map()
                .insert(blob_span_key(&self.blob, span.start, len), 0)
                .is_none()
            {
                cost += len;
            }
        }
        cost
    }

    /// Every fixed region narrowed to rows `[start, start + rows)`, in canonical
    /// order, with no blob slot yet.
    fn fixed_regions(&self, start: usize, rows: usize) -> Regions<'_> {
        let mut out = Regions::new();
        for (r, &stride) in self.strides().iter().enumerate() {
            let stride = stride as usize;
            out.push(&self.region_at(r)[start * stride..(start + rows) * stride]);
        }
        out
    }

    /// Every fixed region followed by the blob heap, in canonical order.
    pub fn wire_regions(&self) -> Regions<'_> {
        let mut out = self.fixed_regions(0, self.count);
        out.push(&self.blob);
        out
    }

    /// Encode self into WAL wire format at the front of `out`, the header
    /// stating this batch's dead-heap bound. Returns bytes written.
    pub fn encode_to_wire(&self, out: &mut [u8]) -> usize {
        self.debug_verify_dead_heap();
        wal::write_block(&self.wire_regions(), self.dead_heap, out)
    }

    /// `frame` as one WAL block at the front of `out`, its strings relocated
    /// into the block's own heap. Returns bytes written.
    pub fn encode_frame(&self, frame: &WireFrame, out: &mut [u8]) -> usize {
        let WireFrame { start, rows, .. } = *frame;
        let slots = self.heap_referencing_slots();
        if slots == 0 {
            let mut r = self.fixed_regions(start, rows);
            r.push(&[]);
            return wal::write_block(&r, 0, out);
        }
        self.encode_relocating(rows, out, |data, offsets, heap| {
            let mut cache = BlobCache::new(rows);
            for (r, &stride) in self.strides().iter().enumerate() {
                let len = rows * stride as usize;
                let src = &self.region_at(r)[start * stride as usize..][..len];
                let dst = &mut data[offsets[r]..offsets[r] + len];
                match r.checked_sub(REG_PAYLOAD_START) {
                    Some(pi) if (slots >> pi) & 1 != 0 => {
                        copy_string_cells(dst, src, &self.blob, heap, None, &mut cache)
                    }
                    _ => dst.copy_from_slice(src),
                }
            }
        })
        .expect("encode_frame: the block does not fit `out`")
    }

    /// The rows `indices` selects, in order, as one WAL block at the front of
    /// `out`, their strings relocated into the block's own heap. Returns bytes
    /// written; `None` when the block does not fit `out`.
    pub fn encode_scattered_to_wire(&self, indices: &[u32], out: &mut [u8]) -> Option<usize> {
        let count = indices.len();
        self.encode_relocating(count, out, |data, offsets, heap| {
            let mut writer = DirectWriter::over_regions(data, offsets, self.strides(), count, self.schema(), heap);
            super::scatter::scatter_copy(&self.as_mem_batch(), indices, &mut writer);
        })
    }

    /// A WAL block of `count` rows at the front of `out`, whose regions `fill`
    /// writes at `offsets` and whose heap it relocates into. `None` when the
    /// block does not fit `out`.
    fn encode_relocating(
        &self,
        count: usize,
        out: &mut [u8],
        fill: impl FnOnce(&mut [u8], &[usize], &mut Vec<u8>),
    ) -> Option<usize> {
        let strides = self.strides();
        let fixed = count * row_width(strides);
        let heap_at = wal::WAL_HEADER_SIZE + fixed;
        if heap_at > out.len() {
            return None;
        }
        let mut offsets = [0usize; MAX_BATCH_REGIONS];
        wire_offsets(strides, strides.len(), count, &mut offsets);
        let cap = match self.heap_referencing_slots() {
            0 => 0,
            _ => prorated_blob_cap(self.blob.len(), self.count, count),
        };
        let mut heap = acquire_arena(cap);
        fill(&mut out[..heap_at], &offsets[..strides.len()], &mut heap);
        let total = heap_at + heap.len();
        let Some(dst) = out.get_mut(heap_at..total) else {
            recycle_buf(heap);
            return None;
        };
        dst.copy_from_slice(&heap);
        // Relocated cell by cell, so every heap byte is referenced.
        let written = wal::write_head(out, count, fixed, heap.len(), 0);
        recycle_buf(heap);
        Some(written)
    }

    /// The longest front run of the rows `indices` selects, ascending, that fits
    /// `out` as one block, written there: its row count and bytes. `None` when
    /// not even the first row fits.
    pub fn encode_scattered_prefix(&self, indices: &[u32], out: &mut [u8]) -> Option<(usize, usize)> {
        let fixed_fit = out.len().saturating_sub(wal::WAL_HEADER_SIZE) / row_width(self.strides());
        let rows = &indices[..indices.len().min(fixed_fit)];
        if rows.is_empty() {
            return None;
        }
        // Every row, ascending, is the batch itself: framed whole, heap as it is.
        if indices.len() == self.count && self.wire_byte_size() <= out.len() {
            return Some((self.count, self.encode_to_wire(out)));
        }
        let slots = self.heap_referencing_slots();
        // A heap-free block fits by width alone, and the sizing pass is skipped
        // when the source's whole heap fits too.
        if slots == 0 || self.wire_byte_size_range(rows.len()) + self.blob.len() <= out.len() {
            if let Some(len) = self.encode_scattered_to_wire(rows, out) {
                return Some((rows.len(), len));
            }
        }
        let n = self
            .rows_with_heap(rows.iter().map(|&i| i as usize), slots, 0, out.len())
            .0;
        self.encode_scattered_to_wire(&rows[..n], out).map(|len| (n, len))
    }

    /// Decode a WAL block the engine wrote into an owned `Raw` batch. String
    /// cells are copied verbatim with their heap.
    pub fn decode_from_wal_block(data: &[u8], schema: &SchemaDescriptor) -> Result<Self, &'static str> {
        Ok(Batch::from_mem_batch(&WalBlock::parse(data, schema)?.view(), schema))
    }

    /// [`Self::decode_from_wal_block`] for a block a peer wrote, validated
    /// first. The header's dead-heap bound is not trusted: the heap is measured.
    pub fn decode_foreign_wal_block(data: &[u8], schema: &SchemaDescriptor) -> Result<Self, &'static str> {
        let block = WalBlock::parse(data, schema)?;
        let mut mb = block.view();
        mb.dead_heap = validate_string_cells(&mb, schema)?;
        if gnitz_wire::first_not_null_violation(schema.not_null_payload_slots(), mb.null_bmp()).is_some() {
            return Err("a null bit on a NOT NULL column");
        }
        if first_valued_null_cell(&mb, schema).is_some() {
            return Err("a non-zero cell under a NULL");
        }
        Ok(Batch::from_mem_batch(&mb, schema))
    }
}

/// The first `(row, payload slot)` of `mb` whose NULL cell is not zeroed: a key
/// reader packs or routes a cell whatever its null bit says, so a second
/// encoding would key a row and its retraction apart.
pub(super) fn first_valued_null_cell(mb: &MemBatch<'_>, schema: &SchemaDescriptor) -> Option<(usize, usize)> {
    let col = |pi| {
        let width = schema.columns[schema.payload_col_idx(pi)].size() as usize;
        (mb.col_data(pi, width), width)
    };
    gnitz_wire::first_valued_null(schema.nullable_payload_slots(), mb.null_bmp(), col)
}

/// Every German-string cell of `mb` is in canonical form against its own heap,
/// and every STRING cell's content is UTF-8. Answers the heap's exact count of
/// bytes no cell references.
fn validate_string_cells(mb: &MemBatch<'_>, schema: &SchemaDescriptor) -> Result<usize, &'static str> {
    // Non-ASCII STRING contents, each closed by an ASCII byte so no character
    // runs from one into the next.
    let mut text = Vec::new();
    let dead = walk_heap_spans(mb, schema, |cell, tc| {
        if !gnitz_wire::german_string_cell_ok(cell, mb.blob) {
            return false;
        }
        if tc == TypeCode::String && !gnitz_wire::german_string_short_ascii(cell) {
            let c = gnitz_wire::german_string_content(cell, mb.blob);
            if !c.is_ascii() {
                text.extend_from_slice(c);
                text.push(0);
            }
        }
        true
    })
    .ok_or("data WAL German string is not in canonical form")?;
    simdutf8::basic::from_utf8(&text).map_err(|_| "a STRING cell is not UTF-8")?;
    Ok(dead)
}

/// A WAL block validated under `schema`, borrowing `data`, with the region
/// offsets its [`MemBatch`] view reads through.
///
/// Its string cells and dead-heap bound are taken as the engine wrote them; a
/// block a peer wrote goes through [`Batch::decode_foreign_wal_block`].
pub struct WalBlock<'a> {
    offsets: [usize; MAX_BATCH_REGIONS],
    data: &'a [u8],
    blob: &'a [u8],
    count: usize,
    dead_heap: usize,
    pk_stride: u8,
}

impl<'a> WalBlock<'a> {
    pub fn parse(data: &'a [u8], schema: &SchemaDescriptor) -> Result<Self, &'static str> {
        let (strides, nr) = strides_from_schema(schema);
        let nr = nr as usize;
        let (count, _, blob, dead_heap) = wal::parse_block(data, row_width(&strides[..nr]))?;
        let mut offsets = [0usize; MAX_BATCH_REGIONS];
        wire_offsets(&strides, nr, count, &mut offsets);
        Ok(WalBlock {
            offsets,
            data,
            blob,
            count,
            dead_heap,
            pk_stride: strides[REG_PK],
        })
    }

    /// The block's rows, as [`Batch::as_mem_batch`] views a batch's.
    #[inline]
    pub fn view(&self) -> MemBatch<'_> {
        MemBatch {
            data: self.data,
            offsets: &self.offsets,
            pk_stride: self.pk_stride,
            blob: self.blob,
            count: self.count,
            dead_heap: self.dead_heap,
        }
    }
}

#[cfg(test)]
#[path = "tests/batch_wire.rs"]
mod tests;
