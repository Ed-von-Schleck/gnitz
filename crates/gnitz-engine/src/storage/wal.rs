use std::ptr;

use crate::util::{read_u32_le, read_u64_le, write_u32_le, write_u64_le};
use crate::xxh;
use super::error::StorageError;

//  WAL block header layout (48 bytes):
//  [0,8)   LSN        u64
//  [8,12)  TID        u32
//  [12,16) COUNT      u32
//  [16,20) SIZE       u32
//  [20,24) VERSION    u32
//  [24,32) CHECKSUM   u64
//  [32,36) NUM_REGIONS u32
//  [36,40) RESERVED   u32
//  [40,48) BLOB_SIZE  u64

pub const HEADER_SIZE: usize = gnitz_wire::WAL_HEADER_SIZE;
pub const FORMAT_VERSION: u32 = gnitz_wire::WAL_FORMAT_VERSION;

const OFF_LSN: usize = 0;
const OFF_TID: usize = 8;
const OFF_COUNT: usize = 12;
const OFF_SIZE: usize = 16;
const OFF_VERSION: usize = 20;
const OFF_CHECKSUM: usize = 24;
const OFF_NUM_REGIONS: usize = 32;
const OFF_BLOB_SIZE: usize = 40;

use crate::util::align8;

/// Compute the total byte size of a WAL block with the given regions.
pub(crate) fn block_size(num_regions: usize, region_sizes: &[u32]) -> usize {
    let mut pos = HEADER_SIZE + num_regions * 8;
    for sz in region_sizes.iter().take(num_regions) {
        pos = align8(pos);
        pos += *sz as usize;
    }
    pos
}

/// Encode a WAL block from region data into `out_buf` starting at `out_offset`.
///
/// Returns the new offset (= `out_offset + total_block_size`) on success, or
/// `Err(StorageError::BufferTooSmall)` if `out_buf` cannot fit the encoded
/// block.
///
/// The block layout is:
///   [48B header][directory: num_regions * 8B][data regions, 8B-aligned]
///
/// Directory entries store offsets relative to block start (not buffer start).
#[allow(clippy::too_many_arguments)]
pub fn encode(
    out_buf: &mut [u8],
    out_offset: usize,
    lsn: u64,
    table_id: u32,
    entry_count: u32,
    region_ptrs: &[*const u8],
    region_sizes: &[u32],
    blob_size: u64,
) -> Result<usize, StorageError> {
    let num_regions = region_ptrs.len().min(region_sizes.len());
    let dir_size = num_regions * 8;
    let total_size = block_size(num_regions, region_sizes);

    if out_offset + total_size > out_buf.len() {
        return Err(StorageError::BufferTooSmall);
    }

    let block = &mut out_buf[out_offset..out_offset + total_size];
    block[..HEADER_SIZE].fill(0);

    let mut pos = HEADER_SIZE + dir_size;
    for i in 0..num_regions {
        pos = align8(pos);
        let sz = region_sizes[i] as usize;
        if sz > 0 && !region_ptrs[i].is_null() {
            unsafe {
                ptr::copy_nonoverlapping(region_ptrs[i], block[pos..].as_mut_ptr(), sz);
            }
        }
        let dir_off = HEADER_SIZE + i * 8;
        write_u32_le(block, dir_off, pos as u32);
        write_u32_le(block, dir_off + 4, sz as u32);
        pos += sz;
    }

    write_u64_le(block, OFF_LSN, lsn);
    write_u32_le(block, OFF_TID, table_id);
    write_u32_le(block, OFF_COUNT, entry_count);
    write_u32_le(block, OFF_SIZE, total_size as u32);
    write_u32_le(block, OFF_VERSION, FORMAT_VERSION);
    write_u32_le(block, OFF_NUM_REGIONS, num_regions as u32);
    write_u64_le(block, OFF_BLOB_SIZE, blob_size);

    if total_size > HEADER_SIZE {
        let cs = xxh::checksum(&block[HEADER_SIZE..total_size]);
        write_u64_le(block, OFF_CHECKSUM, cs);
    }

    Ok(out_offset + total_size)
}

/// Validate a WAL block and extract header fields + directory entries.
///
/// On success: header fields and directory arrays are populated.
///
/// `out_region_offsets` and `out_region_sizes` must have at least `max_regions`
/// entries. Only `min(num_regions, max_regions)` entries are written.
#[allow(clippy::too_many_arguments)]
pub fn validate_and_parse(
    block: &[u8],
    out_lsn: &mut u64,
    out_tid: &mut u32,
    out_count: &mut u32,
    out_num_regions: &mut u32,
    out_blob_size: &mut u64,
    out_region_offsets: &mut [u64],
    out_region_sizes: &mut [u32],
    max_regions: u32,
) -> Result<(), StorageError> {
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

    let expected_cs = read_u64_le(block, OFF_CHECKSUM);
    if total_size > HEADER_SIZE {
        let actual_cs = xxh::checksum(&block[HEADER_SIZE..total_size]);
        if actual_cs != expected_cs {
            return Err(StorageError::ChecksumMismatch);
        }
    }

    *out_lsn = read_u64_le(block, OFF_LSN);
    *out_tid = read_u32_le(block, OFF_TID);
    *out_count = read_u32_le(block, OFF_COUNT);
    let num_regions = read_u32_le(block, OFF_NUM_REGIONS);
    *out_num_regions = num_regions;
    *out_blob_size = read_u64_le(block, OFF_BLOB_SIZE);

    let n = (num_regions as usize).min(max_regions as usize);
    for i in 0..n {
        let dir_off = HEADER_SIZE + i * 8;
        if dir_off + 8 > total_size {
            break;
        }
        out_region_offsets[i] = read_u32_le(block, dir_off) as u64;
        out_region_sizes[i] = read_u32_le(block, dir_off + 4);
    }

    Ok(())
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

        let block_len = encode(&mut buf, 0, 42, 7, 2, &ptrs, &sizes, 0).unwrap();

        let mut lsn = 0u64;
        let mut tid = 0u32;
        let mut count = 0u32;
        let mut num_regions = 0u32;
        let mut blob_size = 0u64;
        let mut offsets = [0u64; 16];
        let mut rsizes = [0u32; 16];

        validate_and_parse(
            &buf[..block_len],
            &mut lsn, &mut tid, &mut count, &mut num_regions, &mut blob_size,
            &mut offsets, &mut rsizes, 16,
        ).unwrap();
        assert_eq!(lsn, 42);
        assert_eq!(tid, 7);
        assert_eq!(count, 2);
        assert_eq!(num_regions, 3);
        assert_eq!(blob_size, 0);

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
        let off1 = encode(&mut buf, 0, 1, 10, 2, &ptrs, &sizes, 0).unwrap();
        let off2 = encode(&mut buf, off1, 2, 20, 2, &ptrs, &sizes, 0).unwrap();
        assert!(off2 > off1);

        // Decode both blocks
        let mut lsn = 0u64;
        let mut tid = 0u32;
        let mut count = 0u32;
        let mut num_regions = 0u32;
        let mut blob_size = 0u64;
        let mut offsets = [0u64; 16];
        let mut rsizes = [0u32; 16];

        validate_and_parse(
            &buf[..off1],
            &mut lsn, &mut tid, &mut count, &mut num_regions, &mut blob_size,
            &mut offsets, &mut rsizes, 16,
        ).unwrap();
        assert_eq!(lsn, 1);
        assert_eq!(tid, 10);

        validate_and_parse(
            &buf[off1..off2],
            &mut lsn, &mut tid, &mut count, &mut num_regions, &mut blob_size,
            &mut offsets, &mut rsizes, 16,
        ).unwrap();
        assert_eq!(lsn, 2);
        assert_eq!(tid, 20);
    }

    #[test]
    fn alignment() {
        // Region of 5 bytes should still result in 8-byte aligned next region
        let r0 = [1u8, 2, 3, 4, 5];
        let r1 = [6u8, 7, 8, 9];
        let ptrs = vec![r0.as_ptr(), r1.as_ptr()];
        let sizes = vec![5u32, 4];
        let mut buf = vec![0u8; 4096];

        let new_offset = encode(&mut buf, 0, 0, 0, 2, &ptrs, &sizes, 0).unwrap();

        let mut lsn = 0u64;
        let mut tid = 0u32;
        let mut count = 0u32;
        let mut num_regions = 0u32;
        let mut blob_size = 0u64;
        let mut offsets = [0u64; 16];
        let mut rsizes = [0u32; 16];

        validate_and_parse(
            &buf[..new_offset],
            &mut lsn, &mut tid, &mut count, &mut num_regions, &mut blob_size,
            &mut offsets, &mut rsizes, 16,
        ).unwrap();

        // Second region offset must be 8-byte aligned
        assert_eq!(offsets[1] % 8, 0, "region 1 not 8-byte aligned: {}", offsets[1]);
    }

    #[test]
    fn bad_version() {
        let mut buf = vec![0u8; HEADER_SIZE];
        write_u32_le(&mut buf, OFF_VERSION, 99);
        write_u32_le(&mut buf, OFF_SIZE, HEADER_SIZE as u32);

        let mut lsn = 0u64;
        let mut tid = 0u32;
        let mut count = 0u32;
        let mut num_regions = 0u32;
        let mut blob_size = 0u64;

        let rc = validate_and_parse(
            &buf, &mut lsn, &mut tid, &mut count, &mut num_regions, &mut blob_size,
            &mut [], &mut [], 0,
        );
        assert_eq!(rc, Err(StorageError::InvalidVersion));
    }

    #[test]
    fn bad_checksum() {
        let (_regions, ptrs, sizes) = make_test_regions();
        let mut buf = vec![0u8; 4096];
        let new_offset = encode(&mut buf, 0, 1, 1, 2, &ptrs, &sizes, 0).unwrap();

        // Corrupt one byte in the body
        buf[HEADER_SIZE + 1] ^= 0xFF;

        let mut lsn = 0u64;
        let mut tid = 0u32;
        let mut count = 0u32;
        let mut num_regions = 0u32;
        let mut blob_size = 0u64;

        let rc = validate_and_parse(
            &buf[..new_offset],
            &mut lsn, &mut tid, &mut count, &mut num_regions, &mut blob_size,
            &mut [], &mut [], 0,
        );
        assert_eq!(rc, Err(StorageError::ChecksumMismatch));
    }

    #[test]
    fn truncated_block() {
        let mut lsn = 0u64;
        let mut tid = 0u32;
        let mut count = 0u32;
        let mut num_regions = 0u32;
        let mut blob_size = 0u64;

        // Too short for header
        let rc = validate_and_parse(
            &[0u8; 10],
            &mut lsn, &mut tid, &mut count, &mut num_regions, &mut blob_size,
            &mut [], &mut [], 0,
        );
        assert_eq!(rc, Err(StorageError::Truncated));
    }

    #[test]
    fn empty_regions() {
        let mut buf = vec![0u8; 4096];
        let new_offset = encode(&mut buf, 0, 1, 1, 0, &[], &[], 0).unwrap();
        assert_eq!(new_offset, HEADER_SIZE); // just a header, no directory, no data

        let mut lsn = 0u64;
        let mut tid = 0u32;
        let mut count = 0u32;
        let mut num_regions = 0u32;
        let mut blob_size = 0u64;

        validate_and_parse(
            &buf[..new_offset],
            &mut lsn, &mut tid, &mut count, &mut num_regions, &mut blob_size,
            &mut [], &mut [], 0,
        ).unwrap();
        assert_eq!(num_regions, 0);
    }

    #[test]
    fn buffer_too_small() {
        let r0 = [0u8; 100];
        let ptrs = [r0.as_ptr()];
        let sizes = [100u32];
        let mut buf = vec![0u8; 64]; // too small

        let rc = encode(&mut buf, 0, 0, 0, 0, &ptrs, &sizes, 0);
        assert_eq!(rc, Err(StorageError::BufferTooSmall));
    }
}
