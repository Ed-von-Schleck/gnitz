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
use super::super::lsm::shard_file;
use super::super::lsm::wal;
use super::batch::{pk_stride, Batch, MAX_BATCH_REGIONS, REG_NULL_BMP, REG_PAYLOAD_START, REG_PK, REG_WEIGHT};
use super::merge::MemBatch;
use crate::schema::SchemaDescriptor;

impl Batch {
    /// Write this batch as a shard file directly to disk.
    #[cfg(test)]
    pub fn write_as_shard(&self, path: &CStr, table_id: u32) -> Result<(), StorageError> {
        let regions = self.regions();
        shard_file::write_shard_streaming(libc::AT_FDCWD, path, table_id, self.count as u32, &regions, true, 0)
    }

    /// Write this batch as a shard file with an explicit flags byte.
    /// Use `SHARD_FLAG_PK_UNIQUE` when the output was verified by `PkUniqueChecker`.
    pub fn write_as_shard_with_flags(&self, path: &CStr, table_id: u32, flags: u8) -> Result<(), StorageError> {
        let regions = self.regions();
        shard_file::write_shard_streaming(libc::AT_FDCWD, path, table_id, self.count as u32, &regions, true, flags)
    }

    // ── Wire serialization (used by runtime::sal / runtime::wire) ───────────

    /// Byte count of the WAL-block encoding for this batch.
    pub fn wire_byte_size(&self, _table_id: u32) -> usize {
        let nr_wire = self.num_regions_total();
        let mut sizes = [0u32; MAX_BATCH_REGIONS + 1];
        for (i, size) in sizes[..nr_wire].iter_mut().enumerate() {
            *size = self.region_size(i) as u32;
        }
        wal::block_size(nr_wire, &sizes[..nr_wire])
    }

    /// Byte count of the WAL-block encoding for `count` rows from this batch.
    /// Only valid for wire-safe schemas — all region strides are multiples of 8
    /// so there is no alignment padding and the result is linear in `count`.
    pub fn wire_byte_size_range(&self, count: usize) -> usize {
        let nr_wire = self.num_regions_total();
        let blob_idx = nr_wire - 1;
        let mut sizes = [0u32; MAX_BATCH_REGIONS + 1];
        for (i, size) in sizes[..blob_idx].iter_mut().enumerate() {
            *size = (count * self.region_stride(i) as usize) as u32;
        }
        // blob is always empty for wire-safe schemas
        sizes[blob_idx] = 0;
        wal::block_size(nr_wire, &sizes[..nr_wire])
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
        let nr_wire = self.num_regions_total();
        let blob_idx = nr_wire - 1;
        let mut ptrs = [std::ptr::null::<u8>(); MAX_BATCH_REGIONS + 1];
        let mut sizes = [0u32; MAX_BATCH_REGIONS + 1];
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
            0,
            table_id,
            count as u32,
            &ptrs[..nr_wire],
            &sizes[..nr_wire],
            0,
            checksum,
        )
        .expect("WAL encode failed: buffer too small");
        new_offset - offset
    }

    /// Encode self into WAL V4 wire format at out[offset..]. Returns bytes written.
    pub fn encode_to_wire(&self, table_id: u32, out: &mut [u8], offset: usize, checksum: bool) -> usize {
        let nr_wire = self.num_regions_total();
        let mut ptrs = [std::ptr::null::<u8>(); MAX_BATCH_REGIONS + 1];
        let mut sizes = [0u32; MAX_BATCH_REGIONS + 1];
        for i in 0..nr_wire {
            ptrs[i] = self.region_ptr(i);
            sizes[i] = self.region_size(i) as u32;
        }
        let blob_size = self.blob.len() as u64;
        let new_offset = wal::encode(
            out,
            offset,
            0,
            table_id,
            self.count as u32,
            &ptrs[..nr_wire],
            &sizes[..nr_wire],
            blob_size,
            checksum,
        )
        .expect("WAL encode failed: buffer too small");
        new_offset - offset
    }

    /// Decode a WAL block from `data` using `schema`. Returns (Batch, bytes_consumed).
    /// Does not set sorted/consolidated — caller derives those from wire header flags.
    /// Set `verify_checksum = false` for trusted IPC paths (W2M ring).
    pub fn decode_from_wal_block(
        data: &[u8],
        schema: &SchemaDescriptor,
        verify_checksum: bool,
    ) -> Result<(Self, usize), &'static str> {
        let npc = schema.num_payload_cols();
        // V4: 3 fixed regions (pk pk_stride*B, weight 8B, null_bmp 8B) + npc payload + blob
        let expected_regions = 3 + npc + 1;

        let mut lsn = 0u64;
        let mut tid = 0u32;
        let mut count = 0u32;
        let mut num_regions = 0u32;
        let mut blob_size = 0u64;
        let mut offsets = [0u64; 128];
        let mut sizes = [0u32; 128];

        if wal::validate_and_parse(
            data,
            &mut lsn,
            &mut tid,
            &mut count,
            &mut num_regions,
            &mut blob_size,
            &mut offsets,
            &mut sizes,
            128,
            verify_checksum,
        )
        .is_err()
        {
            return Err("WAL block validation failed");
        }
        if (num_regions as usize) != expected_regions {
            return Err("WAL block region count mismatch");
        }

        let bytes_consumed = u32::from_le_bytes(data[16..20].try_into().unwrap()) as usize;
        let n = count as usize;

        // Zero-row block: `from_regions` would hard-code pk_stride=16; use the
        // schema-correct empty batch so callers never observe a stale stride
        // (e.g. empty transaction boundaries in the SAL).
        if n == 0 {
            let mut batch = Batch::empty_with_schema(schema);
            batch.set_schema(*schema);
            return Ok((batch, bytes_consumed));
        }

        // Validate per-region byte sizes against the schema-expected strides
        // (`n > 0` here — the zero-row block returned early above). `from_regions`
        // derives strides by dividing the supplied sizes by `count`, so a
        // mismatch would silently propagate into the decoded Batch — and
        // `get_pk` reads `strides[REG_PK]`, which a stride > 16 would make
        // `widen_pk_le` panic on. (Blob region is variable-length.)
        let pk_stride = schema.pk_stride() as usize;
        if sizes[REG_PK] as usize != n * pk_stride {
            return Err("WAL block PK region size mismatch");
        }
        if sizes[REG_WEIGHT] as usize != n * 8 {
            return Err("WAL block weight region size mismatch");
        }
        if sizes[REG_NULL_BMP] as usize != n * 8 {
            return Err("WAL block null bitmap region size mismatch");
        }
        for (pi, _, col) in schema.payload_columns() {
            if sizes[REG_PAYLOAD_START + pi] as usize != n * col.size() as usize {
                return Err("WAL block payload region size mismatch");
            }
        }

        let nr_mem = expected_regions;
        let mut ptrs = [std::ptr::null::<u8>(); MAX_BATCH_REGIONS + 1];
        let mut region_sizes = [0u32; MAX_BATCH_REGIONS + 1];

        // `validate_and_parse` guaranteed every region's `[off, off + sz)` lies
        // within the block, so each pointer is in-bounds; a zero-size region
        // keeps its null pointer.
        for i in 0..nr_mem {
            region_sizes[i] = sizes[i];
            if sizes[i] > 0 {
                ptrs[i] = unsafe { data.as_ptr().add(offsets[i] as usize) };
            }
        }

        let mut batch = unsafe { Batch::from_regions(&ptrs[..nr_mem], &region_sizes[..nr_mem], n, npc) };
        batch.set_schema(*schema);
        Ok((batch, bytes_consumed))
    }
}

/// Decode a WAL data block into a `MemBatch<'a>` that borrows the buffer
/// directly. No allocation; caller must keep `data` live for as long as the
/// returned `MemBatch` is used. Checksum verification is skipped (IPC trusted
/// path).
///
/// The returned `MemBatch` carries the parsed region offsets by value.
/// `validate_and_parse` guarantees every region's `[offset, offset + size)`
/// lies within the block; here we additionally require each fixed region to be
/// large enough for `count` rows at its schema stride, so the hot-path `get_*`
/// accessors never read past a region on a corrupted WAL block.
pub fn decode_mem_batch_from_wal_block<'a>(
    data: &'a [u8],
    schema: &SchemaDescriptor,
) -> Result<MemBatch<'a>, &'static str> {
    let npc = schema.num_payload_cols();
    let expected_regions = 3 + npc + 1; // pk, weight, null_bmp, payload…, blob

    let mut _lsn = 0u64;
    let mut _tid = 0u32;
    let mut count = 0u32;
    let mut num_regions = 0u32;
    let mut _blob_size = 0u64;
    let mut wal_offsets = [0u64; MAX_BATCH_REGIONS];
    let mut sizes = [0u32; MAX_BATCH_REGIONS];

    wal::validate_and_parse(
        data,
        &mut _lsn,
        &mut _tid,
        &mut count,
        &mut num_regions,
        &mut _blob_size,
        &mut wal_offsets,
        &mut sizes,
        MAX_BATCH_REGIONS as u32,
        false,
    )
    .map_err(|_| "data WAL block invalid")?;

    if num_regions as usize != expected_regions {
        return Err("data WAL block region count mismatch");
    }

    let n = count as usize;
    let pk_stride_val = pk_stride(schema);

    let mut offsets = [0usize; MAX_BATCH_REGIONS];

    // Each fixed region must be large enough for `n` rows at its schema stride.
    // `validate_and_parse` already bounded `off + sz` to the block, so a region
    // big enough for the rows is also fully in-bounds.
    let validate = |r: usize, row_stride: usize| -> Result<usize, &'static str> {
        if n == 0 {
            return Ok(0);
        }
        if (sizes[r] as usize) < n * row_stride {
            return Err("data WAL region too small");
        }
        Ok(wal_offsets[r] as usize)
    };

    offsets[REG_PK] = validate(REG_PK, pk_stride_val as usize)?;
    offsets[REG_WEIGHT] = validate(REG_WEIGHT, 8)?;
    offsets[REG_NULL_BMP] = validate(REG_NULL_BMP, 8)?;

    for (pi, _ci, col) in schema.payload_columns() {
        let stride = col.size() as usize;
        offsets[REG_PAYLOAD_START + pi] = validate(REG_PAYLOAD_START + pi, stride)?;
    }

    // The blob extent is bounded by `validate_and_parse` like every region;
    // a zero-size heap is an empty slice.
    let blob_r = REG_PAYLOAD_START + npc;
    let blob = {
        let off = wal_offsets[blob_r] as usize;
        let sz = sizes[blob_r] as usize;
        if sz == 0 {
            &[]
        } else {
            &data[off..off + sz]
        }
    };

    Ok(MemBatch {
        data,
        offsets,
        pk_stride: pk_stride_val,
        blob,
        count: n,
    })
}

#[cfg(test)]
mod tests {
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

        let sz = b.wire_byte_size(1);
        let mut buf = vec![0u8; sz];
        b.encode_to_wire(1, &mut buf, 0, false);
        // Corrupt the REG_PK (region 0) size directory entry: claim 24 bytes.
        let size_off = gnitz_wire::WAL_HEADER_SIZE + REG_PK * 8 + 4;
        buf[size_off..size_off + 4].copy_from_slice(&24u32.to_le_bytes());
        let r = Batch::decode_from_wal_block(&buf, &schema, false);
        assert_eq!(r.err(), Some("WAL block PK region size mismatch"));
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

        let sz = b.wire_byte_size(1);
        let mut buf = vec![0u8; sz];
        b.encode_to_wire(1, &mut buf, 0, false);
        // Corrupt the REG_WEIGHT (region 1) size: claim 4 bytes instead of 8.
        let size_off = gnitz_wire::WAL_HEADER_SIZE + REG_WEIGHT * 8 + 4;
        buf[size_off..size_off + 4].copy_from_slice(&4u32.to_le_bytes());
        let r = Batch::decode_from_wal_block(&buf, &schema, false);
        assert_eq!(r.err(), Some("WAL block weight region size mismatch"));
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

        let sz = b.wire_byte_size(1);
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
        assert_eq!(r.err(), Some("WAL block validation failed"));
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

        let sz = b.wire_byte_size(1);
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
        let mut out = vec![0u8; b.wire_byte_size(1) + 16];
        b.encode_range_to_wire(0, 1, 1, &mut out, 0, false);
    }
}
