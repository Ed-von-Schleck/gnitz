use std::ptr;

use super::super::error::StorageError;
use super::batch::MAX_WIRE_REGIONS;
use crate::foundation::codec::{align8, read_u32_le, read_u64_le, write_u32_le, write_u64_le};
use crate::foundation::xxh;

// Header layout: see `gnitz_wire::wal` — the one definition client and engine
// share; the local renames below are brevity only.
use gnitz_wire::{
    WAL_FORMAT_VERSION as FORMAT_VERSION, WAL_HEADER_SIZE as HEADER_SIZE, WAL_OFF_CHECKSUM as OFF_CHECKSUM,
    WAL_OFF_COUNT as OFF_COUNT, WAL_OFF_NUM_REGIONS as OFF_NUM_REGIONS, WAL_OFF_SIZE as OFF_SIZE,
    WAL_OFF_TID as OFF_TID, WAL_OFF_VERSION as OFF_VERSION,
};

/// Compute the total byte size of a WAL block with the given regions.
/// The region count is `region_sizes.len()` — no separate count param.
pub(crate) fn block_size(region_sizes: &[u32]) -> usize {
    let mut pos = HEADER_SIZE + region_sizes.len() * 8;
    for &sz in region_sizes {
        pos = align8(pos);
        pos += sz as usize;
    }
    pos
}

/// Write the 32-byte WAL header (all fields) and the region directory into
/// `block`, zero-filling inter-region align8 gaps, and return each region's
/// absolute start position within the block. Shared by [`encode`] (which then
/// copies prebuilt region bytes) and the SAL scatter writer
/// (`write_scattered_data_block`), which carves the body and scatters rows
/// directly — the one place the block framing is spelled out.
///
/// The caller stamps the body checksum afterwards via [`stamp_checksum`]
/// (or leaves the zeroed checksum field for unchecksummed IPC paths).
pub(crate) fn write_header_and_directory(
    block: &mut [u8],
    table_id: u32,
    entry_count: u32,
    region_sizes: &[u32],
    total_size: usize,
) -> [usize; MAX_WIRE_REGIONS] {
    let num_regions = region_sizes.len();
    // Sized to MAX_WIRE_REGIONS (= MAX_BATCH_REGIONS + 1) — the max region count
    // including the trailing blob region (pk/weight/null_bmp + payload + blob).
    debug_assert!(
        num_regions <= MAX_WIRE_REGIONS,
        "num_regions={num_regions} exceeds positions array capacity"
    );
    block[..HEADER_SIZE].fill(0);
    let mut positions = [0usize; MAX_WIRE_REGIONS];
    let mut pos = HEADER_SIZE + num_regions * 8;
    for (i, &sz) in region_sizes.iter().enumerate() {
        let aligned = align8(pos);
        if aligned > pos {
            block[pos..aligned].fill(0);
        }
        pos = aligned;
        positions[i] = pos;
        let dir_off = HEADER_SIZE + i * 8;
        write_u32_le(block, dir_off, pos as u32);
        write_u32_le(block, dir_off + 4, sz);
        pos += sz as usize;
    }
    write_u32_le(block, OFF_TID, table_id);
    write_u32_le(block, OFF_COUNT, entry_count);
    write_u32_le(block, OFF_SIZE, total_size as u32);
    write_u32_le(block, OFF_VERSION, FORMAT_VERSION);
    write_u32_le(block, OFF_NUM_REGIONS, num_regions as u32);
    positions
}

/// Stamp the XXH3 body checksum of a fully-written block into its header.
pub(crate) fn stamp_checksum(block: &mut [u8], total_size: usize) {
    if total_size > HEADER_SIZE {
        let cs = xxh::checksum(&block[HEADER_SIZE..total_size]);
        write_u64_le(block, OFF_CHECKSUM, cs);
    }
}

/// Header fields of a validated WAL block, as returned by
/// [`validate_and_parse`].
#[derive(Clone, Copy, Default, PartialEq, Eq, Debug)]
pub struct WalBlockHeader {
    pub table_id: u32,
    pub entry_count: u32,
    pub num_regions: u32,
    /// Total block size (header + directory + regions) — the bytes one block
    /// consumes in a multi-block buffer.
    pub total_size: usize,
}

/// Encode a WAL block from region data into `out_buf` starting at `out_offset`.
///
/// Returns the new offset (= `out_offset + total_block_size`) on success, or
/// `Err(StorageError::BufferTooSmall)` if `out_buf` cannot fit the encoded
/// block.
///
/// The block layout is:
///   [32B header][directory: num_regions * 8B][data regions, 8B-aligned]
///
/// Directory entries store offsets relative to block start (not buffer start).
///
/// Set `checksum = false` for trusted IPC paths (W2M ring) to skip the XXH3
/// computation.  Always use `true` for durable WAL writes and network responses.
pub fn encode(
    out_buf: &mut [u8],
    out_offset: usize,
    table_id: u32,
    entry_count: u32,
    region_ptrs: &[*const u8],
    region_sizes: &[u32],
    checksum: bool,
) -> Result<usize, StorageError> {
    debug_assert_eq!(region_ptrs.len(), region_sizes.len());
    let num_regions = region_sizes.len();
    let total_size = block_size(region_sizes);

    if out_offset + total_size > out_buf.len() {
        return Err(StorageError::BufferTooSmall);
    }

    let block = &mut out_buf[out_offset..out_offset + total_size];

    // Phase 1: header + directory, returning per-region start positions.
    let positions = write_header_and_directory(block, table_id, entry_count, region_sizes, total_size);

    // Phase 2: coalesce adjacent runs into single copies.
    // A run extends while consecutive regions are both source-adjacent (no gap
    // in the source buffer) and dest-adjacent (no align8 padding between them).
    let mut i = 0;
    while i < num_regions {
        let sz = region_sizes[i] as usize;
        if sz == 0 {
            i += 1;
            continue;
        }
        if region_ptrs[i].is_null() {
            block[positions[i]..positions[i] + sz].fill(0);
            i += 1;
            continue;
        }
        let run_src = region_ptrs[i];
        let run_dst = positions[i];
        let mut run_len = sz;
        let mut j = i + 1;
        while j < num_regions {
            let sz_j = region_sizes[j] as usize;
            if sz_j == 0 || region_ptrs[j].is_null() {
                break;
            }
            // Source must be adjacent to previous region's end.
            if unsafe { region_ptrs[j] != region_ptrs[j - 1].add(region_sizes[j - 1] as usize) } {
                break;
            }
            // Destination must be gap-free (only holds when previous size is 8-aligned).
            if positions[j] != positions[j - 1] + region_sizes[j - 1] as usize {
                break;
            }
            run_len += sz_j;
            j += 1;
        }
        unsafe {
            ptr::copy_nonoverlapping(run_src, block[run_dst..].as_mut_ptr(), run_len);
        }
        i = j;
    }

    if checksum {
        stamp_checksum(block, total_size);
    }

    Ok(out_offset + total_size)
}

/// Validate a WAL block and extract its header + directory entries.
///
/// On success: the returned [`WalBlockHeader`] carries the header fields, the
/// first `num_regions` slots of `out_region_offsets`/`out_region_sizes` are
/// populated, and every region's `[offset, offset + size)` extent is guaranteed
/// to lie within the block (`offset + size <= total_size <= block.len()`) —
/// decoders can index each region without a further bounds check.
///
/// A block whose region count exceeds `MAX_WIRE_REGIONS` (or the out slices)
/// is rejected as `InvalidShard` — no well-formed producer emits one.
///
/// Set `verify_checksum = false` for trusted IPC paths (W2M ring) to skip the
/// XXH3 verification.  Always use `true` for WAL recovery and network decoding.
pub fn validate_and_parse(
    block: &[u8],
    out_region_offsets: &mut [u64],
    out_region_sizes: &mut [u32],
    verify_checksum: bool,
) -> Result<WalBlockHeader, StorageError> {
    if block.len() < HEADER_SIZE {
        return Err(StorageError::Truncated);
    }

    let version = read_u32_le(block, OFF_VERSION);
    if version != FORMAT_VERSION {
        return Err(StorageError::InvalidVersion);
    }

    let total_size = read_u32_le(block, OFF_SIZE) as usize;
    if total_size > block.len() || total_size < HEADER_SIZE {
        return Err(StorageError::Truncated);
    }

    if verify_checksum && total_size > HEADER_SIZE {
        let expected_cs = read_u64_le(block, OFF_CHECKSUM);
        let actual_cs = xxh::checksum(&block[HEADER_SIZE..total_size]);
        if actual_cs != expected_cs {
            return Err(StorageError::ChecksumMismatch);
        }
    }

    let header = WalBlockHeader {
        table_id: read_u32_le(block, OFF_TID),
        entry_count: read_u32_le(block, OFF_COUNT),
        num_regions: read_u32_le(block, OFF_NUM_REGIONS),
        total_size,
    };

    let n = header.num_regions as usize;
    if n > MAX_WIRE_REGIONS || n > out_region_offsets.len() || n > out_region_sizes.len() {
        return Err(StorageError::InvalidShard);
    }
    for i in 0..n {
        let dir_off = HEADER_SIZE + i * 8;
        if dir_off + 8 > total_size {
            return Err(StorageError::Truncated);
        }
        let off = read_u32_le(block, dir_off) as usize;
        let sz = read_u32_le(block, dir_off + 4) as usize;
        // The region's data extent must lie within the block. `total_size <=
        // block.len()` (checked above), so this is the tightest in-block bound.
        // Without it a directory entry could name a region running past the
        // block end, which decoders would silently zero-fill (a dropped column,
        // or an emptied blob heap) rather than reject. `off`/`sz` are u32, so
        // `off + sz` cannot overflow usize on a 64-bit target.
        if off + sz > total_size {
            return Err(StorageError::Truncated);
        }
        out_region_offsets[i] = off as u64;
        out_region_sizes[i] = sz as u32;
    }

    Ok(header)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_test_regions() -> (Vec<Vec<u8>>, Vec<*const u8>, Vec<u32>) {
        // 3 regions: pk_lo (16B), pk_hi (16B), weight (16B) — simulating 2 rows
        let r0 = vec![1u8, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0];
        let r1 = vec![0u8; 16];
        let r2 = vec![1u8, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0];
        let ptrs = vec![r0.as_ptr(), r1.as_ptr(), r2.as_ptr()];
        let sizes = vec![16u32, 16, 16];
        (vec![r0, r1, r2], ptrs, sizes)
    }

    #[test]
    fn encode_decode_roundtrip() {
        let (_regions, ptrs, sizes) = make_test_regions();
        let mut buf = vec![0u8; 4096];

        let block_len = encode(&mut buf, 0, 7, 2, &ptrs, &sizes, true).unwrap();

        let mut offsets = [0u64; 16];
        let mut rsizes = [0u32; 16];
        let h = validate_and_parse(&buf[..block_len], &mut offsets, &mut rsizes, true).unwrap();
        assert_eq!(h.table_id, 7);
        assert_eq!(h.entry_count, 2);
        assert_eq!(h.num_regions, 3);
        assert_eq!(h.total_size, block_len);

        // Verify region data
        for i in 0..3 {
            let off = offsets[i] as usize;
            let sz = rsizes[i] as usize;
            assert_eq!(sz, 16);
            assert_eq!(&buf[off..off + sz], &_regions[i][..]);
        }
    }

    #[test]
    fn encode_append_semantics() {
        let (_regions, ptrs, sizes) = make_test_regions();
        let mut buf = vec![0u8; 8192];

        // Encode two blocks sequentially (like IPC does)
        let off1 = encode(&mut buf, 0, 10, 2, &ptrs, &sizes, true).unwrap();
        let off2 = encode(&mut buf, off1, 20, 2, &ptrs, &sizes, true).unwrap();
        assert!(off2 > off1);

        // Decode both blocks
        let mut offsets = [0u64; 16];
        let mut rsizes = [0u32; 16];

        let h1 = validate_and_parse(&buf[..off1], &mut offsets, &mut rsizes, true).unwrap();
        assert_eq!(h1.table_id, 10);

        let h2 = validate_and_parse(&buf[off1..off2], &mut offsets, &mut rsizes, true).unwrap();
        assert_eq!(h2.table_id, 20);
    }

    #[test]
    fn alignment() {
        // Region of 5 bytes should still result in 8-byte aligned next region
        let r0 = [1u8, 2, 3, 4, 5];
        let r1 = [6u8, 7, 8, 9];
        let ptrs = vec![r0.as_ptr(), r1.as_ptr()];
        let sizes = vec![5u32, 4];
        let mut buf = vec![0u8; 4096];

        let new_offset = encode(&mut buf, 0, 0, 2, &ptrs, &sizes, true).unwrap();

        let mut offsets = [0u64; 16];
        let mut rsizes = [0u32; 16];
        validate_and_parse(&buf[..new_offset], &mut offsets, &mut rsizes, true).unwrap();

        // Second region offset must be 8-byte aligned
        assert_eq!(offsets[1] % 8, 0, "region 1 not 8-byte aligned: {}", offsets[1]);
    }

    #[test]
    fn bad_version() {
        let mut buf = vec![0u8; HEADER_SIZE];
        write_u32_le(&mut buf, OFF_VERSION, 99);
        write_u32_le(&mut buf, OFF_SIZE, HEADER_SIZE as u32);

        let rc = validate_and_parse(&buf, &mut [], &mut [], true);
        assert_eq!(rc, Err(StorageError::InvalidVersion));
    }

    #[test]
    fn bad_checksum() {
        let (_regions, ptrs, sizes) = make_test_regions();
        let mut buf = vec![0u8; 4096];
        let new_offset = encode(&mut buf, 0, 1, 2, &ptrs, &sizes, true).unwrap();

        // Corrupt one byte in the body
        buf[HEADER_SIZE + 1] ^= 0xFF;

        let rc = validate_and_parse(&buf[..new_offset], &mut [], &mut [], true);
        assert_eq!(rc, Err(StorageError::ChecksumMismatch));
    }

    #[test]
    fn truncated_block() {
        // Too short for header
        let rc = validate_and_parse(&[0u8; 10], &mut [], &mut [], true);
        assert_eq!(rc, Err(StorageError::Truncated));
    }

    #[test]
    fn empty_regions() {
        let mut buf = vec![0u8; 4096];
        let new_offset = encode(&mut buf, 0, 1, 0, &[], &[], true).unwrap();
        assert_eq!(new_offset, HEADER_SIZE); // just a header, no directory, no data

        let h = validate_and_parse(&buf[..new_offset], &mut [], &mut [], true).unwrap();
        assert_eq!(h.num_regions, 0);
    }

    #[test]
    fn rejects_overlong_region_count() {
        // A forged block whose region count exceeds MAX_WIRE_REGIONS (or the
        // caller's out slices) must be rejected, never silently clamped.
        let mut buf = vec![0u8; 4096];
        let block_len = encode(&mut buf, 0, 1, 1, &[], &[], false).unwrap();
        write_u32_le(&mut buf, OFF_NUM_REGIONS, (MAX_WIRE_REGIONS + 1) as u32);

        let mut offsets = [0u64; MAX_WIRE_REGIONS + 1];
        let mut rsizes = [0u32; MAX_WIRE_REGIONS + 1];
        let rc = validate_and_parse(&buf[..block_len], &mut offsets, &mut rsizes, false);
        assert_eq!(rc, Err(StorageError::InvalidShard));
    }

    #[test]
    fn rejects_region_extent_past_block() {
        // A directory entry whose [offset, offset + size) runs past total_size
        // must be rejected, not silently accepted — decoders rely on this to
        // avoid zero-filling an out-of-bounds region. checksum=false isolates
        // the extent check from the (directory-covering) checksum.
        let (_regions, ptrs, sizes) = make_test_regions();
        let mut buf = vec![0u8; 4096];
        let block_len = encode(&mut buf, 0, 1, 2, &ptrs, &sizes, false).unwrap();

        // Point region 0's offset at the block end; its size (16) now overruns.
        write_u32_le(&mut buf, HEADER_SIZE, block_len as u32);

        let mut offsets = [0u64; 16];
        let mut rsizes = [0u32; 16];
        let rc = validate_and_parse(&buf[..block_len], &mut offsets, &mut rsizes, false);
        assert_eq!(rc, Err(StorageError::Truncated));
    }

    #[test]
    fn buffer_too_small() {
        let r0 = [0u8; 100];
        let ptrs = [r0.as_ptr()];
        let sizes = [100u32];
        let mut buf = vec![0u8; 64]; // too small

        let rc = encode(&mut buf, 0, 0, 0, &ptrs, &sizes, true);
        assert_eq!(rc, Err(StorageError::BufferTooSmall));
    }
}
