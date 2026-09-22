//! Wire serialization for `Batch`.
//!
//! Keeping the serialization cluster here rather than in `batch.rs` lets the
//! pure in-memory repr name no wire module.
//!
//! These run per-flush / per-IPC, not per-row; the region-copy loops stay
//! `#[inline]`-friendly and read every stride/offset off the `Batch` /
//! `SchemaDescriptor` view.

use super::batch::{strides_from_schema, string_mask, Batch, MAX_BATCH_REGIONS, REG_PK};
use super::merge::{blob_span_key, BlobCache, BlobCacheGuard, DirectWriter, MemBatch};
use crate::schema::SchemaDescriptor;
use gnitz_wire::wal;
use gnitz_wire::{num_regions, Regions};

/// A block's size is affine in its row count: `base + rows · per_row`, plus the
/// heap.
fn block_terms(strides: &[u8]) -> (usize, usize) {
    (
        wal::body_start(strides.len() + 1),
        strides.iter().map(|&s| s as usize).sum(),
    )
}

/// Each fixed region's offset from the block start, given where the first one
/// begins: they pack end to end from there.
fn wire_offsets(strides: &[u8], nr: usize, rows: usize, start: usize, offsets: &mut [usize; MAX_BATCH_REGIONS]) {
    let mut off = start;
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
        let (base, per_row) = block_terms(self.strides());
        base + self.count * per_row + self.blob.len()
    }

    /// Byte count of the WAL-block encoding for `count` rows from this batch,
    /// with an empty heap.
    pub fn wire_byte_size_range(&self, count: usize) -> usize {
        let (base, per_row) = block_terms(self.strides());
        base + count * per_row
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
    fn heap_referencing_slots(&self) -> u64 {
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
        let (base, slope) = block_terms(self.strides());
        (self.count - start).min((budget.saturating_sub(overhead + base) / slope).max(1))
    }

    /// Rows from `start` that fit `budget` once the heap bytes they relocate are
    /// charged. `slots` names the German-string payload slots.
    fn rows_with_heap(&self, start: usize, slots: u64, overhead: usize, budget: usize) -> usize {
        let (base, slope) = block_terms(self.strides());
        let base = overhead + base;
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
            let length = gnitz_wire::read_u32_le(cell, 0) as usize;
            if length <= gnitz_wire::SHORT_STRING_THRESHOLD {
                continue;
            }
            let Some(span) = gnitz_wire::german_string_heap(cell, self.blob.len()) else {
                continue;
            };
            if seen.insert(blob_span_key(&self.blob, span.start, length), 0).is_none() {
                cost += span.len();
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

    /// Encode self into WAL wire format at out[offset..]. Returns bytes written.
    pub fn encode_to_wire(&self, table_id: u32, out: &mut [u8], offset: usize) -> usize {
        let mut block = wal::WalBlock::new(table_id, self.count as u32);
        self.wire_regions(&mut block.regions);
        block.write(&mut out[offset..offset + block.size()])
    }

    /// Rows `[start, start + rows)` as one WAL block at `out[offset..]`, framed
    /// off this batch with no heap. Returns bytes written.
    pub fn encode_range_to_wire(
        &self,
        start: usize,
        rows: usize,
        table_id: u32,
        out: &mut [u8],
        offset: usize,
    ) -> usize {
        // A narrowed block carries no heap, so its trailing region is empty.
        let mut block = wal::WalBlock::new(table_id, rows as u32);
        self.fixed_regions(start, rows, &mut block.regions);
        block.regions.push(&[]);
        block.write(&mut out[offset..offset + block.size()])
    }

    /// [`Self::encode_to_wire`] into a buffer of its own, sized by
    /// [`Self::wire_byte_size`].
    pub fn encode_to_wire_vec(&self, table_id: u32) -> Vec<u8> {
        let mut block = wal::WalBlock::new(table_id, self.count as u32);
        self.wire_regions(&mut block.regions);
        let mut out = Vec::with_capacity(self.wire_byte_size());
        block.append_to(&mut out);
        debug_assert_eq!(
            out.len(),
            self.wire_byte_size(),
            "wire_byte_size must size its own encode"
        );
        out
    }

    /// Encode the rows `indices` selects, in order, as one WAL block of
    /// `wire_byte_size_range(indices.len())` bytes at `out[offset..]`.
    pub fn encode_scattered_to_wire(&self, indices: &[u32], table_id: u32, out: &mut [u8], offset: usize) -> usize {
        debug_assert!(
            !self.schema().has_german_string(),
            "a row scatter writes no heap bytes, so it cannot carry a string column"
        );
        let count = indices.len();
        let strides = self.strides();
        let nr = strides.len();
        // One entry per fixed region, then the empty blob heap.
        let sizes = (0..nr + 1).map(|r| strides.get(r).map_or(0, |&s| (count * s as usize) as u32));
        let (block, body_at) = wal::frame(&mut out[offset..], table_id, count as u32, sizes);
        let total_size = block.len();
        debug_assert_eq!(
            total_size,
            self.wire_byte_size_range(count),
            "the frame must fill the slot its caller sized"
        );

        let mut offsets = [0usize; MAX_BATCH_REGIONS];
        wire_offsets(strides, nr, count, body_at, &mut offsets);
        // No German-string columns here; `DirectWriter` still wants a blob arena,
        // so hand it a 0-cap stack local it must not grow.
        let mut empty_blob: Vec<u8> = Vec::new();
        let mut writer =
            DirectWriter::over_regions(block, &offsets, strides, nr, count, self.schema(), &mut empty_blob);
        super::scatter::scatter_copy(&self.as_mem_batch(), indices, &mut writer);
        total_size
    }

    /// Decode a WAL block the engine wrote into an owned `Raw` batch. String
    /// cells are copied verbatim with their heap.
    pub fn decode_from_wal_block(data: &[u8], schema: &SchemaDescriptor) -> Result<Self, &'static str> {
        let mut offsets = [0usize; MAX_BATCH_REGIONS];
        let mb = decode_mem_batch_from_wal_block(data, schema, &mut offsets)?;
        Ok(Batch::from_mem_batch(&mb, schema, schema))
    }

    /// [`Self::decode_from_wal_block`] for a block a peer wrote, refusing a
    /// German-string cell not in canonical form: a heap extent past the blob, or
    /// padding that would split one element's weight across two rows.
    ///
    /// The refusal lands on the borrowed view, so a corrupt block costs neither
    /// the region copy nor the heap copy `from_mem_batch` would pay. `out_schema`
    /// is what the rows land in — see [`Batch::from_mem_batch`] for what the two
    /// schemas may differ in; a reader that wants the block as it stands passes
    /// `in_schema` twice.
    pub fn decode_foreign_wal_block(
        data: &[u8],
        in_schema: &SchemaDescriptor,
        out_schema: &SchemaDescriptor,
    ) -> Result<Self, &'static str> {
        let mut offsets = [0usize; MAX_BATCH_REGIONS];
        let mb = decode_mem_batch_from_wal_block(data, in_schema, &mut offsets)?;
        validate_string_heap_extents(&mb, in_schema)?;
        Ok(Batch::from_mem_batch(&mb, in_schema, out_schema))
    }
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

/// The one WAL-block parser: validate the block, check every fixed region's
/// size is exactly `count * stride` for `schema` (every producer writes exact
/// sizes; the blob region is variable), and return a borrowed `MemBatch` view
/// over `data` and `offsets`.
///
/// String cells are not canonicalized: relocate them on the way in
/// (`Batch::append_mem_batch*`), validate them
/// ([`Batch::decode_foreign_wal_block`]), or read no string column.
pub fn decode_mem_batch_from_wal_block<'a>(
    data: &'a [u8],
    schema: &SchemaDescriptor,
    offsets: &'a mut [usize; MAX_BATCH_REGIONS],
) -> Result<MemBatch<'a>, &'static str> {
    // The writer↔reader region contract: `strides` is each fixed region's
    // per-row width, `nr` the trailing blob region's index — the same
    // derivation the shard writer and the shard reader's bind share.
    let (strides, nr) = strides_from_schema(schema);
    let nr = nr as usize;

    let mut regions = Regions::new();
    let n = wal::validate_and_parse(data, &mut regions).map_err(|_| "data WAL block invalid")? as usize;

    if regions.len() != num_regions(schema.num_payload_cols()) {
        return Err("data WAL block region count mismatch");
    }

    // Every fixed region exactly `n` rows at its schema stride. Holds at
    // `n == 0` too, where it demands zero-size regions — so a block whose COUNT
    // was flipped to 0 still carries its rows' bytes and fails here.
    for r in 0..nr {
        if regions[r].len() != n * strides[r] as usize {
            return Err("data WAL region size mismatch");
        }
    }
    wire_offsets(&strides, nr, n, wal::body_start(regions.len()), offsets);
    debug_assert!(
        (0..nr).all(|r| data
            .get(offsets[r]..)
            .is_some_and(|d| std::ptr::eq(regions[r].as_ptr(), d.as_ptr()))),
        "the derived offsets must land on the regions the framer parsed"
    );

    // A zero-row block references no heap byte, so its declared heap is dropped.
    let blob = if n == 0 { &[][..] } else { regions[nr] };

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
