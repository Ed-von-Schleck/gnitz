//! Wire serialization for `Batch`: wire sizing and chunking, encoding, and
//! WAL-block decoding with the validation it runs.

use super::batch::{strides_from_schema, string_mask, Batch, MAX_BATCH_REGIONS, REG_PK};
use super::merge::{blob_span_key, BlobCache, BlobCacheGuard, DirectWriter, MemBatch};
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
            let rows = self.rows_with_heap(start, slots, overhead, budget);
            let owned = self.wire_chunk(start, rows);
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
            string_mask(self)
        }
    }

    /// Rows `[start, start + count)` as a batch of their own, heap compacted.
    ///
    /// A contiguous source-order subrange keeps order and distinctness, so the
    /// source's layout claim survives the append's downgrade.
    fn wire_chunk(&self, start: usize, count: usize) -> Batch {
        let mut out = Batch::with_capacity(self.schema(), count);
        out.append_batch(self, start, start + count);
        out.inherit_layout(self);
        out
    }

    /// Rows from `start` that fit `budget` by fixed width alone.
    fn rows_by_width(&self, start: usize, overhead: usize, budget: usize) -> usize {
        let slope = row_width(self.strides());
        (self.count - start).min((budget.saturating_sub(overhead + wal::WAL_HEADER_SIZE) / slope).max(1))
    }

    /// Rows from `start` that fit `budget` once the heap bytes they relocate are
    /// charged. `slots` names the German-string payload slots.
    fn rows_with_heap(&self, start: usize, slots: u64, overhead: usize, budget: usize) -> usize {
        let slope = row_width(self.strides());
        let base = overhead + wal::WAL_HEADER_SIZE;
        let remaining = self.count - start;
        let mut guard = BlobCacheGuard::acquire(self.schema(), remaining);
        let seen = guard.get_mut().expect("a German-string column implies a blob cache");
        let mut heap = 0usize;
        let mut rows = 0usize;
        while rows < remaining {
            let row_heap = heap + self.row_heap_cost(start + rows, slots, seen);
            // The first row goes in whatever it costs; a frame carries whole rows.
            if rows > 0 && base + (rows + 1) * slope + row_heap > budget {
                break;
            }
            heap = row_heap;
            rows += 1;
        }
        rows
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
            if seen.insert(blob_span_key(&self.blob, span.start, len), 0).is_none() {
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

    /// Encode self into WAL wire format at the front of `out`. Returns bytes
    /// written.
    pub fn encode_to_wire(&self, out: &mut [u8]) -> usize {
        let mut r = Regions::new();
        self.wire_regions(&mut r);
        wal::write_block(&r, out)
    }

    /// Rows `[start, start + rows)` as one WAL block at the front of `out`,
    /// framed off this batch with no heap. Returns bytes written.
    pub fn encode_range_to_wire(&self, start: usize, rows: usize, out: &mut [u8]) -> usize {
        // A narrowed block carries no heap, so its trailing region is empty.
        let mut r = Regions::new();
        self.fixed_regions(start, rows, &mut r);
        r.push(&[]);
        wal::write_block(&r, out)
    }

    /// Encode the rows `indices` selects, in order, as one WAL block of
    /// `wire_byte_size_range(indices.len())` bytes at the front of `out`.
    pub fn encode_scattered_to_wire(&self, indices: &[u32], out: &mut [u8]) -> usize {
        debug_assert!(
            self.heap_referencing_slots() == 0,
            "a row scatter writes no heap bytes, so no cell may reference one"
        );
        let count = indices.len();
        let strides = self.strides();
        let nr = strides.len();
        let total_size = wal::write_head(out, count, count * row_width(strides), 0);
        let block = &mut out[..total_size];

        let mut offsets = [0usize; MAX_BATCH_REGIONS];
        wire_offsets(strides, nr, count, &mut offsets);
        let mut no_heap: Vec<u8> = Vec::new();
        let mut writer = DirectWriter::over_regions(block, &offsets, strides, nr, count, self.schema(), &mut no_heap);
        super::scatter::scatter_copy(&self.as_mem_batch(), indices, &mut writer);
        total_size
    }

    /// Decode a WAL block the engine wrote into an owned `Raw` batch. String
    /// cells are copied verbatim with their heap.
    pub fn decode_from_wal_block(data: &[u8], schema: &SchemaDescriptor) -> Result<Self, &'static str> {
        let mut offsets = [0usize; MAX_BATCH_REGIONS];
        let mb = decode_mem_batch_from_wal_block(data, schema, &mut offsets)?;
        Ok(Batch::from_mem_batch(&mb, schema))
    }

    /// [`Self::decode_from_wal_block`] for a block a peer wrote, validated
    /// first.
    pub fn decode_foreign_wal_block(data: &[u8], schema: &SchemaDescriptor) -> Result<Self, &'static str> {
        let mut offsets = [0usize; MAX_BATCH_REGIONS];
        let mb = decode_mem_batch_from_wal_block(data, schema, &mut offsets)?;
        validate_string_heap_extents(&mb, schema)?;
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
fn validate_string_heap_extents(mb: &MemBatch<'_>, schema: &SchemaDescriptor) -> Result<(), &'static str> {
    if !schema.has_german_string() {
        return Ok(());
    }
    for (pi, col) in schema.payload_columns() {
        if !col.type_code.is_german_string() {
            continue;
        }
        for cell in mb.col_data(pi, 16).as_chunks::<16>().0 {
            if !gnitz_wire::german_string_cell_ok(cell, mb.blob) {
                return Err("data WAL German string is not in canonical form");
            }
        }
    }
    Ok(())
}

/// A WAL block validated under `schema`, as a `MemBatch` borrowing `data`, its
/// region offsets in `offsets`.
///
/// String cells are not canonicalized: relocate them on the way in
/// (`Batch::append_ranges`), validate them
/// ([`Batch::decode_foreign_wal_block`]), or read no string column.
pub fn decode_mem_batch_from_wal_block<'a>(
    data: &'a [u8],
    schema: &SchemaDescriptor,
    offsets: &'a mut [usize; MAX_BATCH_REGIONS],
) -> Result<MemBatch<'a>, &'static str> {
    let (strides, nr) = strides_from_schema(schema);
    let nr = nr as usize;
    let (n, _, blob) = wal::parse_block(data, row_width(&strides[..nr]))?;
    wire_offsets(&strides, nr, n, offsets);

    Ok(MemBatch {
        data,
        offsets,
        pk_stride: strides[REG_PK],
        blob,
        count: n,
        // A borrowed wire view shares no blob identity with any batch.
        blob_id: 0,
    })
}

#[cfg(test)]
#[path = "tests/batch_wire.rs"]
mod tests;
