//! Wire serialization for `Batch`: wire sizing and chunking, encoding, and
//! WAL-block decoding with the validation it runs.

use super::batch::{strides_from_schema, Batch, MAX_BATCH_REGIONS, REG_PK};
use super::batch_pool::{acquire_arena, recycle_buf};
use super::merge::{blob_span_key, prorated_blob_cap, BlobCache, DirectWriter, MemBatch};
use crate::schema::{SchemaDescriptor, SchemaFacts};
use gnitz_wire::wal;
use gnitz_wire::Regions;

/// One row's bytes across the fixed regions of `strides`.
fn row_width(strides: &[u8]) -> usize {
    strides.iter().map(|&s| s as usize).sum()
}

/// Each fixed region's offset in a `rows`-row block.
fn wire_offsets(strides: &[u8], nr: usize, rows: usize, offsets: &mut [usize; MAX_BATCH_REGIONS]) {
    let mut off = wal::WAL_HEADER_SIZE;
    for r in 0..nr {
        offsets[r] = off;
        off += rows * strides[r] as usize;
    }
}

/// What one frame carries: rows `[start, start + len)` of this batch, framed
/// straight from it, or as a batch of its own when the heap must be compacted.
// Boxing would add an allocation to the arm that has just built a `Batch`.
#[allow(clippy::large_enum_variant)]
pub enum WireChunk {
    Range { rows: usize },
    Owned(Batch),
}

impl WireChunk {
    /// Rows this chunk carries.
    pub fn rows(&self) -> usize {
        match self {
            WireChunk::Range { rows } => *rows,
            WireChunk::Owned(b) => b.len(),
        }
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

    /// What one frame carries from `start` within `budget` bytes beside
    /// `overhead` bytes of frame around it, and the size that frame encodes to.
    ///
    /// Always at least one row while any remain: a row too wide for `budget`
    /// comes back over it rather than not at all.
    pub fn wire_chunk_within(&self, start: usize, overhead: usize, budget: usize) -> (WireChunk, usize) {
        let slots = self.heap_referencing_slots();
        let (chunk, size) = if slots == 0 {
            let rows = self.rows_by_width(start, overhead, budget);
            (WireChunk::Range { rows }, overhead + self.wire_byte_size_range(rows))
        } else {
            let rows = self.rows_with_heap(start..self.count, slots, overhead, budget);
            let owned = self.compacted(start..start + rows);
            let size = overhead + owned.wire_byte_size();
            (WireChunk::Owned(owned), size)
        };
        debug_assert!(
            chunk.rows() <= 1 || size <= budget,
            "a chunk of {} rows encodes to {size} > {budget}",
            chunk.rows()
        );
        (chunk, size)
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
    /// relocate are charged. `slots` names the German-string payload slots.
    fn rows_with_heap(
        &self,
        rows: impl ExactSizeIterator<Item = usize>,
        slots: u64,
        overhead: usize,
        budget: usize,
    ) -> usize {
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
        fit
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
    fn fixed_regions<'s>(&'s self, start: usize, rows: usize, out: &mut Regions<'s>) {
        out.clear();
        for (r, &stride) in self.strides().iter().enumerate() {
            let stride = stride as usize;
            out.push(&self.region_at(r)[start * stride..(start + rows) * stride]);
        }
    }

    /// Every fixed region followed by the blob heap, in canonical order.
    pub fn wire_regions<'s>(&'s self, out: &mut Regions<'s>) {
        self.fixed_regions(0, self.count, out);
        out.push(&self.blob);
    }

    /// Encode self into WAL wire format at the front of `out`, the header
    /// stating this batch's dead-heap bound. Returns bytes written.
    pub fn encode_to_wire(&self, out: &mut [u8]) -> usize {
        self.debug_verify_dead_heap();
        let mut r = Regions::new();
        self.wire_regions(&mut r);
        wal::write_block(&r, self.dead_heap, out)
    }

    /// Rows `[start, start + rows)` as one WAL block at the front of `out`,
    /// framed off this batch with no heap. Returns bytes written.
    pub fn encode_range_to_wire(&self, start: usize, rows: usize, out: &mut [u8]) -> usize {
        // A narrowed block carries no heap, so its trailing region is empty.
        let mut r = Regions::new();
        self.fixed_regions(start, rows, &mut r);
        r.push(&[]);
        wal::write_block(&r, 0, out)
    }

    /// The rows `indices` selects, in order, as one WAL block at the front of
    /// `out`, their strings relocated into the block's own heap. Returns bytes
    /// written; `None` when the block does not fit `out`.
    pub fn encode_scattered_to_wire(&self, indices: &[u32], out: &mut [u8]) -> Option<usize> {
        let count = indices.len();
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
        let mut writer = DirectWriter::over_regions(
            &mut out[..heap_at],
            &offsets,
            strides,
            strides.len(),
            count,
            self.schema(),
            &mut heap,
        );
        super::scatter::scatter_copy(&self.as_mem_batch(), indices, &mut writer);
        drop(writer);
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
        let n = self.rows_with_heap(rows.iter().map(|&i| i as usize), slots, 0, out.len());
        self.encode_scattered_to_wire(&rows[..n], out).map(|len| (n, len))
    }

    /// Decode a WAL block the engine wrote into an owned `Raw` batch. String
    /// cells are copied verbatim with their heap.
    pub fn decode_from_wal_block(data: &[u8], schema: &SchemaDescriptor) -> Result<Self, &'static str> {
        let mut offsets = [0usize; MAX_BATCH_REGIONS];
        let mb = decode_mem_batch_from_wal_block(data, schema, &mut offsets)?;
        Ok(Batch::from_mem_batch(&mb, schema))
    }

    /// [`Self::decode_from_wal_block`] for a block a peer wrote, validated
    /// first. The header's dead-heap bound is not trusted: the heap is measured.
    pub fn decode_foreign_wal_block(data: &[u8], schema: &SchemaDescriptor) -> Result<Self, &'static str> {
        let mut offsets = [0usize; MAX_BATCH_REGIONS];
        let mut mb = decode_mem_batch_from_wal_block(data, schema, &mut offsets)?;
        mb.dead_heap = validate_string_heap_extents(&mb, schema)?;
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

/// Every German-string cell of `mb` is in canonical form against its own heap.
/// Answers the heap's exact count of bytes no cell references.
fn validate_string_heap_extents(mb: &MemBatch<'_>, schema: &SchemaDescriptor) -> Result<usize, &'static str> {
    walk_heap_spans(mb, schema, |cell| gnitz_wire::german_string_cell_ok(cell, mb.blob))
        .ok_or("data WAL German string is not in canonical form")
}

/// The exact count of `mb`'s heap bytes no long string cell references, a cell
/// overrunning the heap referencing none.
pub(super) fn measure_dead_heap(mb: &MemBatch<'_>, schema: &SchemaDescriptor) -> usize {
    walk_heap_spans(mb, schema, |_| true).expect("an accepting walk")
}

/// Mark every German-string cell's heap span, after `accept` has passed the
/// cell, and answer the heap bytes left unmarked; `None` at the first cell
/// `accept` refuses.
fn walk_heap_spans(mb: &MemBatch<'_>, schema: &SchemaDescriptor, accept: impl Fn(&[u8; 16]) -> bool) -> Option<usize> {
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
            if !accept(cell) {
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

/// A WAL block validated under `schema`, as a `MemBatch` borrowing `data`, its
/// region offsets in `offsets`.
///
/// Its string cells and dead-heap bound are taken as the engine wrote them; a
/// block a peer wrote goes through [`Batch::decode_foreign_wal_block`].
pub fn decode_mem_batch_from_wal_block<'a>(
    data: &'a [u8],
    schema: &SchemaDescriptor,
    offsets: &'a mut [usize; MAX_BATCH_REGIONS],
) -> Result<MemBatch<'a>, &'static str> {
    let (strides, nr) = strides_from_schema(schema);
    let nr = nr as usize;
    let (n, _, blob, dead_heap) = wal::parse_block(data, row_width(&strides[..nr]))?;
    wire_offsets(&strides, nr, n, offsets);

    Ok(MemBatch {
        data,
        offsets,
        pk_stride: strides[REG_PK],
        blob,
        count: n,
        dead_heap,
    })
}

#[cfg(test)]
#[path = "tests/batch_wire.rs"]
mod tests;
