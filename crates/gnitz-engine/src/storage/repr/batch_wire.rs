//! Wire / shard serialization for `Batch`.
//!
//! This is the one `storage` repr-side module that depends *up* on the disk
//! half (`wal` block layout, `shard_file` image writing). Hoisting the
//! serialization cluster out of `batch.rs` lets the pure in-memory repr there
//! name no disk/LSM module.
//!
//! These run per-flush / per-IPC, not per-row; the region-copy loops stay
//! `#[inline]`-friendly and read every stride/offset off the `Batch` /
//! `SchemaDescriptor` view — region math is never re-derived here.

use std::ffi::CStr;

use super::super::error::StorageError;
use super::batch::{
    acquire_arena, compute_offsets, strides_from_schema, Batch, Fill, MAX_BATCH_REGIONS, MAX_WIRE_REGIONS, REG_PK,
};
use super::merge::MemBatch;
use super::shard_file;
use super::wal;
use crate::schema::SchemaDescriptor;

impl Batch {
    /// Write this batch as a shard file directly to disk. `schema` is passed
    /// explicitly (not read from `Batch.schema`, which is `Option` and absent
    /// for some constructors); `opts` carries the durability / flags / FoR
    /// packing policy (see [`shard_file::ShardWriteOpts`]).
    pub fn write_as_shard(
        &self,
        path: &CStr,
        schema: &SchemaDescriptor,
        opts: shard_file::ShardWriteOpts,
    ) -> Result<(), StorageError> {
        let regions = self.regions();
        shard_file::write_shard_streaming(libc::AT_FDCWD, path, self.count as u32, &regions, schema, opts)
    }

    // ── Wire serialization (used by runtime::sal / runtime::wire) ───────────

    /// Byte count of the WAL-block encoding for this batch.
    pub fn wire_byte_size(&self) -> usize {
        let nr_wire = self.num_regions_total();
        let mut sizes = [0u32; MAX_WIRE_REGIONS];
        for (i, size) in sizes[..nr_wire].iter_mut().enumerate() {
            *size = self.region_size(i) as u32;
        }
        wal::block_size(&sizes[..nr_wire])
    }

    /// Byte count of the WAL-block encoding for `count` rows from this batch.
    /// Only valid for wire-safe schemas — all region strides are multiples of 8
    /// so there is no alignment padding and the result is linear in `count`.
    pub fn wire_byte_size_range(&self, count: usize) -> usize {
        let blob_idx = self.num_regions as usize;
        let nr_wire = blob_idx + 1;
        let mut sizes = [0u32; MAX_WIRE_REGIONS];
        for (i, size) in sizes[..blob_idx].iter_mut().enumerate() {
            *size = (count * self.region_stride(i) as usize) as u32;
        }
        // blob is always empty for wire-safe schemas
        sizes[blob_idx] = 0;
        wal::block_size(&sizes[..nr_wire])
    }

    /// Encode rows `[start_row, start_row + count)` into WAL V4 wire format at
    /// `out[offset..]`. Returns bytes written. Only valid for wire-safe schemas
    /// (no STRING columns, all strides 8-aligned). The blob region is encoded
    /// as empty since wire-safe batches carry no long strings.
    pub fn encode_range_to_wire(
        &self,
        start_row: usize,
        count: usize,
        table_id: u32,
        out: &mut [u8],
        offset: usize,
        checksum: bool,
    ) -> usize {
        // Release-active: this bounds the `unsafe` region_ptr().add(start_row *
        // stride) below. A debug-only check would strip in release and let a
        // bad range form an out-of-bounds pointer (UB) instead of aborting.
        // `saturating_add` (not `+`) so an overflowing range fails the bound
        // rather than wrapping to a small value that slips past it.
        let end = start_row.saturating_add(count);
        assert!(
            end <= self.count,
            "encode_range_to_wire: range [{start_row}, {end}) out of bounds (batch count = {})",
            self.count,
        );
        // Wire-safe precondition: a wire-safe schema carries no long strings, so
        // the blob heap is empty and is intentionally dropped below. Callers gate
        // this encoder on `schema_wire_safe`; a STRING/BLOB batch routes to the
        // full-blob `encode_wire_into` path. Assert it so a future caller that
        // mis-routes string data fails loudly here rather than shipping structs
        // whose heap vanished.
        debug_assert!(
            self.blob.is_empty(),
            "encode_range_to_wire on a batch with a {}-byte blob: wire-safe schemas \
             carry no long strings; the heap would be silently dropped",
            self.blob.len(),
        );
        let blob_idx = self.num_regions as usize;
        let nr_wire = blob_idx + 1;
        let mut ptrs = [std::ptr::null::<u8>(); MAX_WIRE_REGIONS];
        let mut sizes = [0u32; MAX_WIRE_REGIONS];
        for i in 0..blob_idx {
            let stride = self.region_stride(i) as usize;
            // SAFETY: start_row * stride is within the allocated region (the
            // assert above guarantees start_row + count <= self.count).
            ptrs[i] = unsafe { self.region_ptr(i).add(start_row * stride) };
            sizes[i] = (count * stride) as u32;
        }
        // blob: null ptr with 0 bytes — no long strings in wire-safe schemas
        ptrs[blob_idx] = std::ptr::null();
        sizes[blob_idx] = 0;
        let new_offset = wal::encode(
            out,
            offset,
            table_id,
            count as u32,
            &ptrs[..nr_wire],
            &sizes[..nr_wire],
            checksum,
        )
        .expect("WAL encode failed: buffer too small");
        new_offset - offset
    }

    /// Encode self into WAL wire format at out[offset..]. Returns bytes written.
    pub fn encode_to_wire(&self, table_id: u32, out: &mut [u8], offset: usize, checksum: bool) -> usize {
        let nr_wire = self.num_regions_total();
        let mut ptrs = [std::ptr::null::<u8>(); MAX_WIRE_REGIONS];
        let mut sizes = [0u32; MAX_WIRE_REGIONS];
        for i in 0..nr_wire {
            ptrs[i] = self.region_ptr(i);
            sizes[i] = self.region_size(i) as u32;
        }
        let new_offset = wal::encode(
            out,
            offset,
            table_id,
            self.count as u32,
            &ptrs[..nr_wire],
            &sizes[..nr_wire],
            checksum,
        )
        .expect("WAL encode failed: buffer too small");
        new_offset - offset
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
        let (mb, bytes_consumed) = decode_mem_batch_inner(data, schema, verify_checksum)?;
        // Zero-row block: use the schema-correct empty batch so callers never
        // observe a stale stride (e.g. empty transaction boundaries in the SAL).
        if mb.count == 0 {
            return Ok((Batch::empty_with_schema(schema), bytes_consumed));
        }
        validate_string_heap_extents(&mb, schema)?;

        let (strides, nr) = strides_from_schema(schema);
        let nr_usize = nr as usize;
        let (offsets, total) = compute_offsets(&strides, nr_usize, mb.count);
        // Uninit arena sized for exactly `mb.count` rows: every live byte of
        // every region is written by the bulk copies below; only inter-region
        // align8 padding stays uninit, which no reader or re-encoder touches
        // (`wal::encode`'s run coalescing breaks exactly where padding exists,
        // and shard/wire/clone paths are all `count`-bounded).
        let mut data_buf = acquire_arena(total, Fill::Uninit);
        for r in 0..nr_usize {
            let len = mb.count * strides[r] as usize;
            if len == 0 {
                continue;
            }
            // SAFETY: both extents are in bounds — the source region was
            // validated to `count * stride` bytes, the destination sized by
            // compute_offsets for `mb.count` rows; distinct allocations.
            unsafe {
                std::ptr::copy_nonoverlapping(
                    mb.data.as_ptr().add(mb.offsets[r]),
                    data_buf.as_mut_ptr().add(offsets[r]),
                    len,
                );
            }
        }
        let mut blob = acquire_arena(mb.blob.len(), Fill::Reserve);
        blob.extend_from_slice(mb.blob);

        // SAFETY: `data_buf`/`offsets` were laid out by compute_offsets for
        // `mb.count` rows of these strides and every region was filled above.
        let mut batch = unsafe { Batch::from_prebuilt(data_buf, blob, strides, offsets, nr, mb.count) };
        batch.set_schema(*schema);
        Ok((batch, bytes_consumed))
    }
}

/// Trust-boundary guard for the blob-passthrough decode: every long-string
/// struct's `[offset, offset + len)` must lie inside the block's blob heap.
/// Without this, a hostile client push could ship an overrunning struct that
/// fires `german_string_tail`'s debug_assert (a debug-build DoS) and persists
/// corrupt structs — the per-row relocation used to canonicalize these to the
/// empty string. Null cells are zeroed (length 0 → short) and skip themselves.
fn validate_string_heap_extents(mb: &MemBatch<'_>, schema: &SchemaDescriptor) -> Result<(), &'static str> {
    if !schema.has_german_string() {
        return Ok(());
    }
    for (pi, _ci, col) in schema.payload_columns() {
        if !gnitz_wire::is_german_string(col.type_code) {
            continue;
        }
        for row in 0..mb.count {
            let cell = mb.get_col_ptr(row, pi, 16);
            let len = u32::from_le_bytes(cell[0..4].try_into().unwrap()) as usize;
            if len > crate::schema::SHORT_STRING_THRESHOLD {
                let off = u64::from_le_bytes(cell[8..16].try_into().unwrap()) as usize;
                if off.saturating_add(len) > mb.blob.len() {
                    return Err("data WAL long string overruns blob heap");
                }
            }
        }
    }
    Ok(())
}

/// Decode a WAL data block into a `MemBatch<'a>` that borrows the buffer
/// directly. No allocation; caller must keep `data` live for as long as the
/// returned `MemBatch` is used. Checksum verification is skipped (IPC trusted
/// path).
pub fn decode_mem_batch_from_wal_block<'a>(
    data: &'a [u8],
    schema: &SchemaDescriptor,
) -> Result<MemBatch<'a>, &'static str> {
    decode_mem_batch_inner(data, schema, false).map(|(mb, _)| mb)
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

    let mut offsets = [0usize; MAX_BATCH_REGIONS];

    // Each fixed region must be exactly `n` rows at its schema stride —
    // every producer writes exact sizes, and stride-deriving consumers divide
    // size by count, so an inexact size is a corrupt or mis-schema'd block. `validate_and_parse` already bounded
    // `off + sz` to the block, so an exact-size region is also fully in-bounds.
    for (r, offset) in offsets.iter_mut().enumerate().take(nr) {
        if n == 0 {
            continue; // offsets stay 0; regions are empty
        }
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
        },
        header.total_size,
    ))
}

#[cfg(test)]
mod tests {
    use super::super::batch::{REG_PAYLOAD_START, REG_WEIGHT};
    use super::*;
    use crate::schema::{type_code, SchemaColumn, SchemaDescriptor};

    fn single_col_pk_schema(tc: u8) -> SchemaDescriptor {
        SchemaDescriptor::new(&[SchemaColumn::new(tc, 0), SchemaColumn::new(type_code::I64, 0)], &[0])
    }

    #[test]
    fn decode_from_wal_block_rejects_mismatched_pk_stride() {
        let schema = single_col_pk_schema(type_code::U64); // pk_stride = 8
        let mut b = Batch::with_schema(schema, 1);
        b.extend_pk(42u128);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &7i64.to_le_bytes());
        b.count += 1;

        let sz = b.wire_byte_size();
        let mut buf = vec![0u8; sz];
        b.encode_to_wire(1, &mut buf, 0, false);
        // Corrupt the REG_PK (region 0) size directory entry: claim 24 bytes.
        let size_off = gnitz_wire::WAL_HEADER_SIZE + REG_PK * 8 + 4;
        buf[size_off..size_off + 4].copy_from_slice(&24u32.to_le_bytes());
        let r = Batch::decode_from_wal_block(&buf, &schema, false);
        assert_eq!(r.err(), Some("data WAL region size mismatch"));
    }

    #[test]
    fn decode_from_wal_block_rejects_mismatched_weight_region() {
        let schema = single_col_pk_schema(type_code::U64);
        let mut b = Batch::with_schema(schema, 1);
        b.extend_pk(42u128);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &7i64.to_le_bytes());
        b.count += 1;

        let sz = b.wire_byte_size();
        let mut buf = vec![0u8; sz];
        b.encode_to_wire(1, &mut buf, 0, false);
        // Corrupt the REG_WEIGHT (region 1) size: claim 4 bytes instead of 8.
        let size_off = gnitz_wire::WAL_HEADER_SIZE + REG_WEIGHT * 8 + 4;
        buf[size_off..size_off + 4].copy_from_slice(&4u32.to_le_bytes());
        let r = Batch::decode_from_wal_block(&buf, &schema, false);
        assert_eq!(r.err(), Some("data WAL region size mismatch"));
    }

    #[test]
    fn decode_from_wal_block_rejects_region_offset_past_block() {
        let schema = single_col_pk_schema(type_code::U64);
        let mut b = Batch::with_schema(schema, 1);
        b.extend_pk(42u128);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &7i64.to_le_bytes());
        b.count += 1;

        let sz = b.wire_byte_size();
        let mut buf = vec![0u8; sz];
        b.encode_to_wire(1, &mut buf, 0, false);
        // Corrupt the REG_PK (region 0) OFFSET directory entry: point it past the
        // block while leaving its size (= n*pk_stride) schema-valid. The region's
        // [off, off + sz) now overruns the block, so `validate_and_parse` rejects
        // it instead of the decoder silently zero-filling the PK column.
        let block_end = buf.len() as u32;
        let off_off = gnitz_wire::WAL_HEADER_SIZE + REG_PK * 8;
        buf[off_off..off_off + 4].copy_from_slice(&block_end.to_le_bytes());
        // verify_checksum = false: the unverified IPC path is the one this guards.
        let r = Batch::decode_from_wal_block(&buf, &schema, false);
        assert_eq!(r.err(), Some("data WAL block invalid"));
    }

    #[test]
    fn decode_mem_batch_rejects_blob_region_past_block() {
        let schema = single_col_pk_schema(type_code::U64);
        let mut b = Batch::with_schema(schema, 1);
        b.extend_pk(42u128);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &7i64.to_le_bytes());
        b.count += 1;

        let sz = b.wire_byte_size();
        let mut buf = vec![0u8; sz];
        b.encode_to_wire(1, &mut buf, 0, false);
        // Fixed regions stay schema-valid; corrupt only the variable-length BLOB
        // region (index REG_PAYLOAD_START + npc) so [off, off + sz) overruns the
        // block: offset = block end, size = 8. `validate_and_parse` rejects the
        // OOB extent, so the decoder never resolves strings against an empty heap.
        let blob_r = REG_PAYLOAD_START + schema.num_payload_cols();
        let entry = gnitz_wire::WAL_HEADER_SIZE + blob_r * 8;
        let block_end = buf.len() as u32;
        buf[entry..entry + 4].copy_from_slice(&block_end.to_le_bytes()); // offset
        buf[entry + 4..entry + 8].copy_from_slice(&8u32.to_le_bytes()); // size
        let r = decode_mem_batch_from_wal_block(&buf, &schema);
        assert_eq!(r.err(), Some("data WAL block invalid"));
    }

    #[test]
    #[should_panic(expected = "wire-safe schemas")]
    fn encode_range_to_wire_panics_on_nonempty_blob() {
        let schema = single_col_pk_schema(type_code::U64);
        let mut b = Batch::with_schema(schema, 1);
        b.extend_pk(1u128);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &7i64.to_le_bytes());
        b.count += 1;
        // A non-empty heap on a wire-safe encode must fail loudly, not vanish.
        b.blob.push(0xAB);
        let mut out = vec![0u8; b.wire_byte_size() + 16];
        b.encode_range_to_wire(0, 1, 1, &mut out, 0, false);
    }
}
