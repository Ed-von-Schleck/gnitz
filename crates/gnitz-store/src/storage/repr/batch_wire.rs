//! Wire serialization for `Batch`.
//!
//! Keeping the serialization cluster here rather than in `batch.rs` lets the
//! pure in-memory repr name no wire module.
//!
//! These run per-flush / per-IPC, not per-row; the region-copy loops stay
//! `#[inline]`-friendly and read every stride/offset off the `Batch` /
//! `SchemaDescriptor` view.

use super::batch::{strides_from_schema, string_mask, Batch, MAX_BATCH_REGIONS, MAX_WIRE_REGIONS, REG_PK};
use super::merge::{blob_span_key, BlobCache, BlobCacheGuard, DirectWriter, MemBatch};
use crate::schema::SchemaDescriptor;
use gnitz_wire::wal;

/// Total WAL-block byte size for `count` rows of `schema` carrying `blob_size`
/// heap bytes.
pub fn wire_block_size(schema: &SchemaDescriptor, count: usize, blob_size: usize) -> usize {
    let (strides, nr) = strides_from_schema(schema);
    wal::strided_block_size(&strides[..nr as usize], count, blob_size)
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
        wal::strided_block_size(self.strides(), self.count, self.blob.len())
    }

    /// Byte count of the WAL-block encoding for `count` rows from this batch,
    /// with an empty heap.
    pub fn wire_byte_size_range(&self, count: usize) -> usize {
        wal::strided_block_size(self.strides(), count, 0)
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

    /// A block's size as `base + rows · slope`, with `overhead` folded into `base`.
    fn block_terms(&self, overhead: usize) -> (usize, usize) {
        let strides = self.strides();
        let base = overhead + wal::body_start(strides.len() + 1);
        (base, strides.iter().map(|&s| s as usize).sum())
    }

    /// Rows from `start` that fit `budget` by fixed width alone.
    fn rows_by_width(&self, start: usize, overhead: usize, budget: usize) -> usize {
        let (base, slope) = self.block_terms(overhead);
        (self.count - start).min((budget.saturating_sub(base) / slope).max(1))
    }

    /// Rows from `start` that fit `budget` once the heap bytes they relocate are
    /// charged. `slots` names the German-string payload slots.
    fn rows_with_heap(&self, start: usize, slots: u64, overhead: usize, budget: usize) -> usize {
        let (base, slope) = self.block_terms(overhead);
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
        let mut rest = slots;
        while rest != 0 {
            let pi = rest.trailing_zeros() as usize;
            rest &= rest - 1;
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

    /// Fill `out` with every fixed region narrowed to rows `[start, start + rows)`
    /// and an empty trailing blob slot, in canonical order; returns the region count.
    pub fn fill_regions_range<'a>(
        &'a self,
        start: usize,
        rows: usize,
        out: &mut [&'a [u8]; MAX_WIRE_REGIONS],
    ) -> usize {
        let blob_idx = self.num_regions();
        let strides = self.strides();
        for (r, region) in out[..blob_idx].iter_mut().enumerate() {
            let stride = strides[r] as usize;
            *region = &self.region_at(r)[start * stride..(start + rows) * stride];
        }
        out[blob_idx] = &[];
        blob_idx + 1
    }

    /// Fill `out` with every fixed region followed by the blob heap, in
    /// canonical order. Returns the region count — the `&[&[u8]]` both the WAL
    /// framer and the shard writer take, built on the caller's stack, and the
    /// mirror's decode input.
    pub fn fill_regions<'a>(&'a self, out: &mut [&'a [u8]; MAX_WIRE_REGIONS]) -> usize {
        let n = self.fill_regions_range(0, self.count, out);
        out[n - 1] = &self.blob;
        n
    }

    /// Encode self into WAL wire format at out[offset..]. Returns bytes written.
    pub fn encode_to_wire(&self, table_id: u32, out: &mut [u8], offset: usize, checksum: bool) -> usize {
        let mut regions: [&[u8]; MAX_WIRE_REGIONS] = [&[]; MAX_WIRE_REGIONS];
        let n = self.fill_regions(&mut regions);
        let end = wal::encode(out, offset, table_id, self.count as u32, &regions[..n], checksum)
            .expect("WAL encode failed: buffer too small");
        end - offset
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
        let mut regions: [&[u8]; MAX_WIRE_REGIONS] = [&[]; MAX_WIRE_REGIONS];
        let n = self.fill_regions_range(start, rows, &mut regions);
        let end = wal::encode(out, offset, table_id, rows as u32, &regions[..n], false)
            .expect("WAL encode failed: buffer too small");
        end - offset
    }

    /// [`Self::encode_to_wire`] into a buffer of its own, sized by
    /// [`Self::wire_byte_size`].
    pub fn encode_to_wire_vec(&self, table_id: u32, checksum: bool) -> Vec<u8> {
        let mut out = vec![0u8; self.wire_byte_size()];
        let written = self.encode_to_wire(table_id, &mut out, 0, checksum);
        debug_assert_eq!(written, out.len(), "wire_byte_size must size its own encode");
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
        let (total_size, regions) = wal::frame_in_place(out, offset, table_id, count, self.strides());
        // No German-string columns here; `DirectWriter` still wants a blob arena,
        // so hand it a 0-cap stack local it must not grow.
        let mut empty_blob: Vec<u8> = Vec::new();
        let mut writer = DirectWriter::over_regions(regions, self.schema(), &mut empty_blob, count);
        super::scatter::scatter_copy(&self.as_mem_batch(), indices, &mut writer);
        total_size
    }

    /// Decode a WAL block the engine wrote into an owned `Raw` batch. String
    /// cells are copied verbatim with their heap.
    pub fn decode_from_wal_block(
        data: &[u8],
        schema: &SchemaDescriptor,
        verify_checksum: bool,
    ) -> Result<Self, &'static str> {
        let mut offsets = [0usize; MAX_BATCH_REGIONS];
        let mb = decode_mem_batch_from_wal_block(data, schema, verify_checksum, &mut offsets)?;
        Ok(Batch::from_mem_batch(&mb, schema))
    }

    /// [`Self::decode_from_wal_block`] for a block a peer wrote, refusing a
    /// German-string cell not in canonical form: a heap extent past the blob, or
    /// padding that would split one element's weight across two rows.
    ///
    /// The refusal lands on the borrowed view, so a corrupt block costs neither
    /// the region copy nor the heap copy `from_mem_batch` would pay.
    pub fn decode_foreign_wal_block(data: &[u8], schema: &SchemaDescriptor) -> Result<Self, &'static str> {
        let mut offsets = [0usize; MAX_BATCH_REGIONS];
        let mb = decode_mem_batch_from_wal_block(data, schema, false, &mut offsets)?;
        validate_string_heap_extents(&mb, schema)?;
        Ok(Batch::from_mem_batch(&mb, schema))
    }
}

/// Every German-string cell of `mb` is in canonical form against its own heap.
fn validate_string_heap_extents(mb: &MemBatch<'_>, schema: &SchemaDescriptor) -> Result<(), &'static str> {
    if !schema.has_german_string() {
        return Ok(());
    }
    for (pi, col) in schema.payload_columns() {
        if !gnitz_wire::is_german_string(col.type_code) {
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
    verify_checksum: bool,
    offsets: &'a mut [usize; MAX_BATCH_REGIONS],
) -> Result<MemBatch<'a>, &'static str> {
    // The writer↔reader region contract: `strides` is each fixed region's
    // per-row width, `nr` the trailing blob region's index — the same
    // derivation the shard writer and `MappedShard::open` share.
    let (strides, nr) = strides_from_schema(schema);
    let nr = nr as usize;

    let mut wal_offsets = [0u64; MAX_WIRE_REGIONS];
    let mut sizes = [0u32; MAX_WIRE_REGIONS];

    let header = wal::validate_and_parse(data, &mut wal_offsets, &mut sizes, verify_checksum)
        .map_err(|_| "data WAL block invalid")?;

    if header.num_regions as usize != nr + 1 {
        return Err("data WAL block region count mismatch");
    }

    let n = header.entry_count as usize;

    // Each fixed region must be exactly `n` rows at its schema stride —
    // every producer writes exact sizes, and stride-deriving consumers divide
    // size by count, so an inexact size is a corrupt or mis-schema'd block. `validate_and_parse` already bounded
    // `off + sz` to the block, so an exact-size region is also fully in-bounds.
    //
    // The relation holds at `n == 0` too, where it demands zero-size regions: a
    // block whose COUNT was flipped to 0 still carries its rows' bytes and fails.
    for (r, offset) in offsets.iter_mut().enumerate().take(nr) {
        if sizes[r] as usize != n * strides[r] as usize {
            return Err("data WAL region size mismatch");
        }
        *offset = wal_offsets[r] as usize;
    }

    // The blob extent is bounded by `validate_and_parse` like every region;
    // a zero-size heap is an empty slice.
    let blob_r = nr;
    let blob = {
        let off = wal_offsets[blob_r] as usize;
        let sz = sizes[blob_r] as usize;
        // A zero-row block references no heap byte, so its declared heap is dropped.
        if sz == 0 || n == 0 {
            &[][..]
        } else {
            &data[off..off + sz]
        }
    };

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
