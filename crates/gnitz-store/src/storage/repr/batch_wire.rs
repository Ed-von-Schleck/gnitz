//! Wire serialization for `Batch`.
//!
//! Keeping the serialization cluster here rather than in `batch.rs` lets the
//! pure in-memory repr name no wire module: this is the only place that knows
//! both the batch layout and the `wal` block format.
//!
//! These run per-flush / per-IPC, not per-row; the region-copy loops stay
//! `#[inline]`-friendly and read every stride/offset off the `Batch` /
//! `SchemaDescriptor` view — region math is never re-derived here.

use super::batch::{
    compute_offsets_into, copy_regions, strides_from_schema, Batch, MAX_BATCH_REGIONS, MAX_WIRE_REGIONS, REG_PK,
};
use super::batch_pool::{acquire_arena, Fill};
use super::merge::{BlobCacheGuard, DirectWriter, MemBatch};
use crate::schema::SchemaDescriptor;
use gnitz_wire::wal;

/// Region byte sizes of the WAL wire block for `count` rows of `schema` into
/// `out` — canonical order (pk, weight, null_bmp, payload…, blob), returning the
/// region count. The schema-level face of the writer↔reader region contract
/// (`strides_from_schema`), for a caller holding no `Batch`. Out-parameter like
/// `compute_offsets_into`: 276 bytes, so a return costs two memcpys per call.
pub(crate) fn wire_region_sizes_into(
    schema: &SchemaDescriptor,
    count: usize,
    blob_size: usize,
    out: &mut [u32; MAX_WIRE_REGIONS],
) -> usize {
    let (strides, nr) = strides_from_schema(schema);
    let nr = nr as usize;
    for (size, &stride) in out[..nr].iter_mut().zip(&strides[..nr]) {
        *size = (count * stride as usize) as u32;
    }
    out[nr] = blob_size as u32;
    nr + 1
}

/// Total WAL-block byte size for `count` rows of `schema` carrying `blob_size`
/// heap bytes — `wire_region_sizes_into` fed through the shared block framer.
pub fn wire_block_size(schema: &SchemaDescriptor, count: usize, blob_size: usize) -> usize {
    let mut sizes = [0u32; MAX_WIRE_REGIONS];
    let nr = wire_region_sizes_into(schema, count, blob_size, &mut sizes);
    wal::block_size(&sizes[..nr])
}

impl Batch {
    // ── Wire serialization (used by the server's SAL and frame codecs) ─────

    /// WAL-block byte size for `count` rows of this batch's fixed regions plus a
    /// `blob_len`-byte heap — the sizing half of the region convention, and the
    /// one both size accessors below read.
    fn wire_size_of(&self, count: usize, blob_len: usize) -> usize {
        let blob_idx = self.num_regions();
        let mut sizes = [0u32; MAX_WIRE_REGIONS];
        for (i, size) in sizes[..blob_idx].iter_mut().enumerate() {
            *size = (count * self.region_stride(i) as usize) as u32;
        }
        sizes[blob_idx] = blob_len as u32;
        wal::block_size(&sizes[..blob_idx + 1])
    }

    /// Byte count of the WAL-block encoding for this batch.
    pub fn wire_byte_size(&self) -> usize {
        self.wire_size_of(self.count, self.blob.len())
    }

    /// Byte count of the WAL-block encoding for `count` rows from this batch,
    /// with an empty heap. Monotone non-decreasing in `count`, which is what lets
    /// [`Self::rows_within`] search it.
    fn wire_byte_size_range(&self, count: usize) -> usize {
        self.wire_size_of(count, 0)
    }

    /// Rows from `start` as a batch of their own, encoding within `budget` bytes
    /// beside `overhead` bytes of frame around it, and carrying a string heap
    /// compacted to just the spans those rows reference.
    ///
    /// Always at least one row while any remain: a row too wide for `budget`
    /// comes back over it rather than not at all.
    pub fn wire_chunk_within(&self, start: usize, overhead: usize, budget: usize) -> Batch {
        let mut rows = self.rows_within(start, overhead, budget);
        loop {
            let chunk = self.wire_chunk(start, rows);
            let size = overhead + chunk.wire_byte_size();
            if size <= budget || rows == 1 {
                return chunk;
            }
            // `rows_within` walks the relocation rule `wire_chunk` applies, so a
            // repeat means the two drifted apart.
            debug_assert!(
                false,
                "rows_within over-predicted: {rows} rows encode to {size} > {budget}"
            );
            rows = (rows * budget / size).clamp(1, rows - 1);
        }
    }

    /// Rows `[start, start + count)` as a batch of their own, heap compacted.
    /// `inherit_layout` restores the claim `append_ranges_inner`'s `downgrade()`
    /// drops and the frame encoder ships.
    fn wire_chunk(&self, start: usize, count: usize) -> Batch {
        let mut out = Batch::with_capacity(self.schema(), count);
        out.append_batch(self, start, start + count);
        out.inherit_layout(self);
        out
    }

    /// Row count [`Self::wire_chunk_within`] starts from. The blob region is
    /// last, so `wire_size_of(n, heap) == wire_byte_size_range(n) + heap` and
    /// both arms measure against that one identity.
    fn rows_within(&self, start: usize, overhead: usize, budget: usize) -> usize {
        let remaining = self.count - start;
        if remaining == 0 {
            return 0;
        }
        let fits = |n: usize, heap: usize| overhead + self.wire_byte_size_range(n) + heap <= budget;
        if self.blob.is_empty() {
            // Searched, not inverted: `wal::block_size_from` re-`align8`s per
            // region, so the size is monotone in `n` but affine only while every
            // stride is a multiple of 8 — and `pk_stride` is routinely 4 or 12.
            let (mut lo, mut hi) = (0usize, remaining);
            while lo < hi {
                let mid = lo + (hi - lo).div_ceil(2);
                if fits(mid, 0) {
                    lo = mid;
                } else {
                    hi = mid - 1;
                }
            }
            return lo.max(1);
        }

        // Each long span costs its bytes once, keyed as
        // `relocate_german_string_vec` dedups — which is what will build the
        // chunk. A join fans one row's string out across many, and counting it
        // per row would emit orders of magnitude too many frames.
        let mut guard = BlobCacheGuard::acquire(self.schema(), remaining);
        let seen = guard
            .get_mut()
            .expect("a non-empty heap implies a German-string column");
        let mut heap = 0usize;
        let mut rows = 0usize;
        while rows < remaining {
            let row_heap = heap + self.row_heap_cost(start + rows, seen);
            // The first row goes in whatever it costs; a frame carries whole rows.
            if rows > 0 && !fits(rows + 1, row_heap) {
                break;
            }
            heap = row_heap;
            rows += 1;
        }
        rows
    }

    /// Heap bytes row `row` adds to a chunk whose spans so far are in `seen`: the
    /// sizing twin of one `relocate_german_string_vec` pass, branching as it
    /// does. `wire_chunk_within`'s retry is what covers the two drifting apart.
    fn row_heap_cost(&self, row: usize, seen: &mut super::merge::BlobCache) -> usize {
        let mut cost = 0;
        for (pi, col) in self.schema().payload_columns() {
            if !gnitz_wire::is_german_string(col.type_code) {
                continue;
            }
            let cell = self.get_col_ptr(row, pi, 16);
            let length = gnitz_wire::read_u32_le(cell, 0) as usize;
            if length <= gnitz_wire::SHORT_STRING_THRESHOLD {
                continue;
            }
            let Some(span) = gnitz_wire::german_string_heap(cell, self.blob.len()) else {
                continue;
            };
            if seen
                .insert((self.blob.as_ptr() as usize, span.start, length), 0)
                .is_none()
            {
                cost += span.len();
            }
        }
        cost
    }

    /// Fill `out` with every fixed region followed by the blob heap, in
    /// canonical order. Returns the region count — the `&[&[u8]]` both the WAL
    /// framer and the shard writer take, built on the caller's stack.
    pub(super) fn fill_regions<'a>(&'a self, out: &mut [&'a [u8]; MAX_WIRE_REGIONS]) -> usize {
        let blob_idx = self.num_regions();
        for (i, region) in out[..blob_idx].iter_mut().enumerate() {
            *region = self.region_or_blob(i);
        }
        out[blob_idx] = &self.blob;
        blob_idx + 1
    }

    /// Encode self into WAL wire format at out[offset..]. Returns bytes written.
    pub fn encode_to_wire(&self, table_id: u32, out: &mut [u8], offset: usize, checksum: bool) -> usize {
        let mut regions: [&[u8]; MAX_WIRE_REGIONS] = [&[]; MAX_WIRE_REGIONS];
        let n = self.fill_regions(&mut regions);
        let end = wal::encode(out, offset, table_id, self.count as u32, &regions[..n], checksum)
            .expect("WAL encode failed: buffer too small");
        end - offset
    }

    /// [`Self::encode_to_wire`] into a buffer of its own, sized by
    /// [`Self::wire_byte_size`] — which is where the two must agree, so the
    /// check lives here rather than at each caller.
    pub fn encode_to_wire_vec(&self, table_id: u32, checksum: bool) -> Vec<u8> {
        let mut out = vec![0u8; self.wire_byte_size()];
        let written = self.encode_to_wire(table_id, &mut out, 0, checksum);
        debug_assert_eq!(written, out.len(), "wire_byte_size must size its own encode");
        out
    }

    /// Encode the rows `indices` selects, in that order, as one WAL block at
    /// `out[offset..]`. Returns bytes written. Unlike its two siblings this one
    /// reads its region strides from `schema` rather than from the batch, so a
    /// caller must size the destination with [`wire_block_size`] over the same
    /// schema — those two are the writer↔reader region contract's two faces.
    ///
    /// Only valid for a schema with no German-string column, so the row scatter
    /// writes no heap bytes. Padded strides are fine: the destination is sized
    /// by `wire_block_size` and carved by `compute_offsets_into`, and both walk the
    /// same `align8` region layout.
    pub fn encode_scattered_to_wire(
        &self,
        indices: &[u32],
        schema: &SchemaDescriptor,
        table_id: u32,
        out: &mut [u8],
        offset: usize,
        checksum: bool,
    ) -> usize {
        let count = indices.len();
        // Region sizes in canonical order: pk, weight, null_bmp, payload…, blob(0).
        let mut sizes = [0u32; MAX_WIRE_REGIONS];
        let nr = wire_region_sizes_into(schema, count, 0, &mut sizes);
        let total_size = wal::block_size(&sizes[..nr]);
        let block = &mut out[offset..offset + total_size];

        wal::write_header_and_directory(block, table_id, count as u32, sizes[..nr].iter().copied(), total_size);

        // The writer carves `rest` (body after header+directory) into per-region
        // slices: [pk | weight | null | col_0 | ...], each sized for `count` rows.
        let (_, rest) = block.split_at_mut(wal::body_start(nr));
        // No German-string columns here; `DirectWriter` still
        // wants a blob arena, so hand it a 0-cap stack local it must not grow.
        let mut empty_blob: Vec<u8> = Vec::new();
        let mut writer = DirectWriter::over_arena(rest, schema, count, &mut empty_blob);
        super::scatter::scatter_copy(&self.as_mem_batch(), indices, &mut writer);
        debug_assert!(
            empty_blob.is_empty(),
            "a schema with no German string scatters no blob bytes"
        );

        if checksum {
            wal::stamp_checksum(block, total_size);
        }
        total_size
    }

    /// Decode a WAL block from `data` using `schema` into an owned `Batch`.
    /// Returns (Batch, bytes_consumed). Does not set sorted/consolidated —
    /// caller derives those from wire header flags. One parse
    /// (`decode_mem_batch_from_wal_block`) + one bulk copy per region.
    /// Set `verify_checksum = false` for trusted IPC paths (W2M ring).
    ///
    /// The destination is always fresh, so the block's blob heap is copied
    /// wholesale and the 16-byte German-string structs bulk-copy verbatim with
    /// the payload regions — their heap offsets are absolute from blob start
    /// and stay valid at base 0. No per-row string relocation. The relocation
    /// also canonicalized hostile long-string structs, so the passthrough
    /// validates every long string's heap extent first and rejects the frame
    /// (like the region-size validations) instead of persisting corrupt
    /// structs. Exchange ingest deliberately does NOT use this decode: its
    /// frames can carry full unfiltered blobs from filter/map blob sharing,
    /// and `append_mem_batch`'s relocation is the compaction point there.
    pub fn decode_from_wal_block(
        data: &[u8],
        schema: &SchemaDescriptor,
        verify_checksum: bool,
    ) -> Result<(Self, usize), &'static str> {
        let mut wire_offsets = [0usize; MAX_BATCH_REGIONS];
        let (mb, bytes_consumed) = decode_mem_batch_inner(data, schema, verify_checksum, &mut wire_offsets)?;
        // Zero-row block: use the schema-correct empty batch so callers never
        // observe a stale stride (e.g. empty transaction boundaries in the SAL).
        if mb.count == 0 {
            return Ok((Batch::empty_with_schema(schema), bytes_consumed));
        }
        validate_string_heap_extents(&mb, schema)?;

        let (strides, nr) = strides_from_schema(schema);
        let nr_usize = nr as usize;
        let mut offsets = [0usize; MAX_BATCH_REGIONS];
        let total = compute_offsets_into(&strides, nr_usize, mb.count, &mut offsets);
        // Uninit arena sized for exactly `mb.count` rows: every live byte of
        // every region is written by the bulk copies below; only inter-region
        // align8 padding stays uninit, which no reader or re-encoder touches
        // (`wal::encode` copies each region's `count * stride` bytes and skips
        // the padding, and shard/wire/clone paths are all `count`-bounded).
        let mut data_buf = acquire_arena(total, Fill::Uninit);
        // SAFETY: both extents are in bounds — each source region was validated to
        // exactly `count * stride` bytes by the parse above, the destination sized
        // by `compute_offsets_into` for `mb.count` rows; distinct allocations.
        unsafe {
            copy_regions(
                mb.data,
                mb.offsets,
                &mut data_buf,
                &offsets,
                &strides,
                nr_usize,
                mb.count,
            );
        }
        let mut blob = acquire_arena(mb.blob.len(), Fill::Reserve);
        blob.extend_from_slice(mb.blob);

        // SAFETY: `data_buf`/`offsets` were laid out by compute_offsets_into for
        // `mb.count` rows of these strides and every region was filled above.
        let batch = unsafe { Batch::from_prebuilt(data_buf, blob, strides, offsets, mb.count, *schema) };
        Ok((batch, bytes_consumed))
    }
}

/// Trust-boundary guard for the blob-passthrough decode: every German-string
/// cell must be in canonical form (`german_string_cell_ok`) against the block's
/// blob heap. The passthrough copies the 16-byte structs verbatim, where the
/// per-row relocation it replaced used to canonicalize them, so this is the one
/// place a hostile client push is stopped from persisting a cell that
/// `german_string_content` and `compare_german_strings` would read differently
/// — an overrunning heap extent, or a padding/prefix skew that splits one Z-set
/// element's weight across two rows consolidation will never merge. Null cells
/// are zeroed (length 0 → canonical short) and pass trivially.
fn validate_string_heap_extents(mb: &MemBatch<'_>, schema: &SchemaDescriptor) -> Result<(), &'static str> {
    if !schema.has_german_string() {
        return Ok(());
    }
    for (pi, col) in schema.payload_columns() {
        if !gnitz_wire::is_german_string(col.type_code) {
            continue;
        }
        for row in 0..mb.count {
            let cell = mb.get_col_ptr(row, pi, 16);
            if !gnitz_wire::german_string_cell_ok(cell, mb.blob) {
                return Err("data WAL German string is not in canonical form");
            }
        }
    }
    Ok(())
}

/// Decode a WAL data block into a `MemBatch<'a>` borrowing `data`, with the
/// block's region offsets written into the caller's `offsets` (which the view
/// then borrows). No allocation; the caller keeps both live. Checksum
/// verification is skipped (IPC trusted path).
///
/// **Contract — this decode skips `validate_string_heap_extents`.** It is for
/// the W2M ring only: worker-written shared memory, unreachable from a client
/// (every client frame lands in `Batch::decode_from_wal_block`, which does
/// validate). A caller must therefore either relocate every German-string cell
/// on the way in (`Batch::append_mem_batch*`, which canonicalizes) or use a
/// schema with no STRING/BLOB payload column — index-record and preflight
/// schemas qualify, since `gnitz_wire::index_key_type` rejects both. Sites that
/// read only the PK region are trivially fine: a PK column can never be a
/// German string (`is_pk_eligible`).
pub fn decode_mem_batch_from_wal_block<'a>(
    data: &'a [u8],
    schema: &SchemaDescriptor,
    offsets: &'a mut [usize; MAX_BATCH_REGIONS],
) -> Result<MemBatch<'a>, &'static str> {
    decode_mem_batch_inner(data, schema, false, offsets).map(|(mb, _)| mb)
}

/// The one WAL-block parser: validate the block, check every fixed region's
/// size is exactly `count * stride` for `schema` (every producer writes exact
/// sizes; the blob region is variable), and return a borrowed `MemBatch` view
/// plus the block's total size (the bytes one block consumes in a multi-block
/// buffer).
fn decode_mem_batch_inner<'a>(
    data: &'a [u8],
    schema: &SchemaDescriptor,
    verify_checksum: bool,
    offsets: &'a mut [usize; MAX_BATCH_REGIONS],
) -> Result<(MemBatch<'a>, usize), &'static str> {
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
        if sz == 0 {
            &[]
        } else {
            &data[off..off + sz]
        }
    };

    Ok((
        MemBatch {
            data,
            offsets,
            pk_stride: strides[REG_PK],
            blob,
            count: n,
            // A borrowed wire view shares no blob identity with any batch.
            blob_id: 0,
        },
        header.total_size,
    ))
}

#[cfg(test)]
#[path = "tests/batch_wire.rs"]
mod tests;
