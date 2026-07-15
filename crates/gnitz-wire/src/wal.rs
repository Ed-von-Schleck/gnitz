//! The low-level WAL-block codec — the one framer client and engine agree on.
//!
//! A WAL block is `[32B header][directory: num_regions × 8B][data regions,
//! 8B-aligned]`. This module owns the header constants, the region-directory
//! framer ([`encode`] / [`validate_and_parse`]), the size/header/checksum
//! helpers the SAL scatter writer shares, and the region-count cap.
//!
//! It is representation-agnostic: regions are raw byte slices, with no `Batch`
//! or schema knowledge. Each side layers its own per-column transcode
//! (OPK / German-string glue on the client, arena region gather on the engine)
//! on top.
//!
//! Header layout (32 bytes):
//!   [0,4)   TID          u32
//!   [4,8)   COUNT        u32
//!   [8,12)  SIZE         u32 (total block size, header + directory + regions)
//!   [12,16) VERSION      u32
//!   [16,24) CHECKSUM     u64 (XXH3 over [WAL_HEADER_SIZE, SIZE))
//!   [24,28) NUM_REGIONS  u32
//!   [28,32) RESERVED     u32

use crate::{align8, checksum, read_u32_le, read_u64_le, write_u32_le, write_u64_le, WalError};

pub const WAL_HEADER_SIZE: usize = 32;
pub const WAL_FORMAT_VERSION: u32 = 6;

pub const WAL_OFF_TID: usize = 0;
pub const WAL_OFF_COUNT: usize = 4;
pub const WAL_OFF_SIZE: usize = 8;
pub const WAL_OFF_VERSION: usize = 12;
pub const WAL_OFF_CHECKSUM: usize = 16;
pub const WAL_OFF_NUM_REGIONS: usize = 24;

pub const IPC_CONTROL_TID: u32 = 0xFFFF_FFFF;

/// Maximum region count a block directory may name, including the trailing blob
/// region. A legitimate schema has ≤ 65 columns (1 PK), so ≤ 68 regions
/// (pk + weight + null + ≤ 64 payload + blob); 69 only ever caps a forged
/// block. The engine ties its own arena capacity to this
/// (`MAX_WIRE_REGIONS == MAX_BATCH_REGIONS + 1`).
pub const MAX_WIRE_REGIONS: usize = 69;

/// Compute the total byte size of a WAL block with the given regions.
/// The region count is `region_sizes.len()` — no separate count param.
pub fn block_size(region_sizes: &[u32]) -> usize {
    let mut pos = WAL_HEADER_SIZE + region_sizes.len() * 8;
    for &sz in region_sizes {
        pos = align8(pos);
        pos += sz as usize;
    }
    pos
}

/// Total byte size of the WAL block that would frame `regions` — the
/// slice-taking sibling of [`block_size`]. A caller that already holds the
/// `&[&[u8]]` [`encode`] takes can size its output buffer straight from it,
/// without materializing a parallel `&[u32]` size array. [`encode`] derives
/// the identical total internally.
pub fn block_size_of(regions: &[&[u8]]) -> usize {
    let mut pos = WAL_HEADER_SIZE + regions.len() * 8;
    for r in regions {
        pos = align8(pos);
        pos += r.len();
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
pub fn write_header_and_directory(
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
    block[..WAL_HEADER_SIZE].fill(0);
    let mut positions = [0usize; MAX_WIRE_REGIONS];
    let mut pos = WAL_HEADER_SIZE + num_regions * 8;
    for (i, &sz) in region_sizes.iter().enumerate() {
        let aligned = align8(pos);
        if aligned > pos {
            block[pos..aligned].fill(0);
        }
        pos = aligned;
        positions[i] = pos;
        let dir_off = WAL_HEADER_SIZE + i * 8;
        write_u32_le(block, dir_off, pos as u32);
        write_u32_le(block, dir_off + 4, sz);
        pos += sz as usize;
    }
    write_u32_le(block, WAL_OFF_TID, table_id);
    write_u32_le(block, WAL_OFF_COUNT, entry_count);
    write_u32_le(block, WAL_OFF_SIZE, total_size as u32);
    write_u32_le(block, WAL_OFF_VERSION, WAL_FORMAT_VERSION);
    write_u32_le(block, WAL_OFF_NUM_REGIONS, num_regions as u32);
    positions
}

/// Stamp the XXH3 body checksum of a fully-written block into its header.
pub fn stamp_checksum(block: &mut [u8], total_size: usize) {
    if total_size > WAL_HEADER_SIZE {
        let cs = checksum(&block[WAL_HEADER_SIZE..total_size]);
        write_u64_le(block, WAL_OFF_CHECKSUM, cs);
    }
}

/// Read the `(offset, size)` directory entry for region `r`, both relative to
/// block start. Unchecked — panics on a slice error, so the caller must already
/// know the block covers its full directory: production decoders reach the
/// directory through [`validate_and_parse`] (which bounds every entry), and the
/// remaining callers are the 1-row control-block reader (each region size-guarded
/// on use) and the malformed-block tests that patch a directory byte. The one
/// place the entry layout (`WAL_HEADER_SIZE + r*8`, offset then size) is spelled out.
#[inline]
pub fn dir_entry(block: &[u8], r: usize) -> (usize, usize) {
    let base = WAL_HEADER_SIZE + r * 8;
    (read_u32_le(block, base) as usize, read_u32_le(block, base + 4) as usize)
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

/// Encode a WAL block from region byte slices into `out_buf` starting at
/// `out_offset`.
///
/// Returns the new offset (= `out_offset + total_block_size`) on success, or
/// `Err(WalError::BufferTooSmall)` if `out_buf` cannot fit the encoded block.
///
/// The block layout is:
///   [32B header][directory: num_regions * 8B][data regions, 8B-aligned]
///
/// Directory entries store offsets relative to block start (not buffer start).
/// Region sizes are `regions[i].len()`; a zero-length region occupies a
/// directory slot but no body bytes. Each region is copied verbatim — the
/// `&[u8]` slices make the length structural, so there is no separate size
/// array to keep in sync and no null-pointer sentinel.
///
/// Set `checksum_body = false` for trusted IPC paths (W2M ring) to skip the
/// XXH3 computation. Always use `true` for durable WAL writes and network
/// responses.
pub fn encode(
    out_buf: &mut [u8],
    out_offset: usize,
    table_id: u32,
    entry_count: u32,
    regions: &[&[u8]],
    checksum_body: bool,
) -> Result<usize, WalError> {
    let num_regions = regions.len();
    let mut region_sizes = [0u32; MAX_WIRE_REGIONS];
    for (dst, r) in region_sizes[..num_regions].iter_mut().zip(regions) {
        *dst = r.len() as u32;
    }
    let region_sizes = &region_sizes[..num_regions];
    let total_size = block_size(region_sizes);

    if out_offset + total_size > out_buf.len() {
        return Err(WalError::BufferTooSmall);
    }

    let block = &mut out_buf[out_offset..out_offset + total_size];

    // Phase 1: header + directory, returning per-region start positions.
    let positions = write_header_and_directory(block, table_id, entry_count, region_sizes, total_size);

    // Phase 2: copy each region's bytes to its directory position. Bounds-checked
    // `copy_from_slice` — one memcpy per non-empty region. (An earlier version
    // coalesced source-adjacent runs into a single `copy_nonoverlapping`; that
    // is unsound once regions arrive as independent `&[u8]` slices — a copy
    // spanning region `i`'s length reads past its provenance — and the per-call
    // saving is a fixed ~1-2 ns/region, off the durable-write scatter path.)
    for (i, r) in regions.iter().enumerate() {
        if r.is_empty() {
            continue;
        }
        let dst = positions[i];
        block[dst..dst + r.len()].copy_from_slice(r);
    }

    if checksum_body {
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
) -> Result<WalBlockHeader, WalError> {
    if block.len() < WAL_HEADER_SIZE {
        return Err(WalError::Truncated);
    }

    let version = read_u32_le(block, WAL_OFF_VERSION);
    if version != WAL_FORMAT_VERSION {
        return Err(WalError::InvalidVersion);
    }

    let total_size = read_u32_le(block, WAL_OFF_SIZE) as usize;
    if total_size > block.len() || total_size < WAL_HEADER_SIZE {
        return Err(WalError::Truncated);
    }

    if verify_checksum && total_size > WAL_HEADER_SIZE {
        let expected_cs = read_u64_le(block, WAL_OFF_CHECKSUM);
        let actual_cs = checksum(&block[WAL_HEADER_SIZE..total_size]);
        if actual_cs != expected_cs {
            return Err(WalError::ChecksumMismatch);
        }
    }

    let header = WalBlockHeader {
        table_id: read_u32_le(block, WAL_OFF_TID),
        entry_count: read_u32_le(block, WAL_OFF_COUNT),
        num_regions: read_u32_le(block, WAL_OFF_NUM_REGIONS),
        total_size,
    };

    let n = header.num_regions as usize;
    if n > MAX_WIRE_REGIONS || n > out_region_offsets.len() || n > out_region_sizes.len() {
        return Err(WalError::InvalidShard);
    }
    for i in 0..n {
        let dir_off = WAL_HEADER_SIZE + i * 8;
        if dir_off + 8 > total_size {
            return Err(WalError::Truncated);
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
            return Err(WalError::Truncated);
        }
        out_region_offsets[i] = off as u64;
        out_region_sizes[i] = sz as u32;
    }

    Ok(header)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_test_regions() -> Vec<Vec<u8>> {
        // 3 regions: pk_lo (16B), pk_hi (16B), weight (16B) — simulating 2 rows
        let r0 = vec![1u8, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0];
        let r1 = vec![0u8; 16];
        let r2 = vec![1u8, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0];
        vec![r0, r1, r2]
    }

    fn as_slices(regions: &[Vec<u8>]) -> Vec<&[u8]> {
        regions.iter().map(|r| r.as_slice()).collect()
    }

    #[test]
    fn encode_decode_roundtrip() {
        let regions = make_test_regions();
        let mut buf = vec![0u8; 4096];

        let block_len = encode(&mut buf, 0, 7, 2, &as_slices(&regions), true).unwrap();

        let mut offsets = [0u64; 16];
        let mut rsizes = [0u32; 16];
        let h = validate_and_parse(&buf[..block_len], &mut offsets, &mut rsizes, true).unwrap();
        assert_eq!(h.table_id, 7);
        assert_eq!(h.entry_count, 2);
        assert_eq!(h.num_regions, 3);
        assert_eq!(h.total_size, block_len);

        // Verify region data
        for (i, region) in regions.iter().enumerate() {
            let off = offsets[i] as usize;
            let sz = rsizes[i] as usize;
            assert_eq!(sz, 16);
            assert_eq!(&buf[off..off + sz], &region[..]);
        }
    }

    #[test]
    fn encode_append_semantics() {
        let regions = make_test_regions();
        let slices = as_slices(&regions);
        let mut buf = vec![0u8; 8192];

        // Encode two blocks sequentially (like IPC does)
        let off1 = encode(&mut buf, 0, 10, 2, &slices, true).unwrap();
        let off2 = encode(&mut buf, off1, 20, 2, &slices, true).unwrap();
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
        let regions: [&[u8]; 2] = [&r0, &r1];
        let mut buf = vec![0u8; 4096];

        let new_offset = encode(&mut buf, 0, 0, 2, &regions, true).unwrap();

        let mut offsets = [0u64; 16];
        let mut rsizes = [0u32; 16];
        validate_and_parse(&buf[..new_offset], &mut offsets, &mut rsizes, true).unwrap();

        // Second region offset must be 8-byte aligned
        assert_eq!(offsets[1] % 8, 0, "region 1 not 8-byte aligned: {}", offsets[1]);
    }

    #[test]
    fn bad_version() {
        let mut buf = vec![0u8; WAL_HEADER_SIZE];
        write_u32_le(&mut buf, WAL_OFF_VERSION, 99);
        write_u32_le(&mut buf, WAL_OFF_SIZE, WAL_HEADER_SIZE as u32);

        let rc = validate_and_parse(&buf, &mut [], &mut [], true);
        assert_eq!(rc, Err(WalError::InvalidVersion));
    }

    #[test]
    fn bad_checksum() {
        let regions = make_test_regions();
        let mut buf = vec![0u8; 4096];
        let new_offset = encode(&mut buf, 0, 1, 2, &as_slices(&regions), true).unwrap();

        // Corrupt one byte in the body
        buf[WAL_HEADER_SIZE + 1] ^= 0xFF;

        let rc = validate_and_parse(&buf[..new_offset], &mut [], &mut [], true);
        assert_eq!(rc, Err(WalError::ChecksumMismatch));
    }

    #[test]
    fn truncated_block() {
        // Too short for header
        let rc = validate_and_parse(&[0u8; 10], &mut [], &mut [], true);
        assert_eq!(rc, Err(WalError::Truncated));
    }

    #[test]
    fn empty_regions() {
        let mut buf = vec![0u8; 4096];
        let new_offset = encode(&mut buf, 0, 1, 0, &[], true).unwrap();
        assert_eq!(new_offset, WAL_HEADER_SIZE); // just a header, no directory, no data

        let h = validate_and_parse(&buf[..new_offset], &mut [], &mut [], true).unwrap();
        assert_eq!(h.num_regions, 0);
    }

    #[test]
    fn rejects_overlong_region_count() {
        // A forged block whose region count exceeds MAX_WIRE_REGIONS (or the
        // caller's out slices) must be rejected, never silently clamped.
        let mut buf = vec![0u8; 4096];
        let block_len = encode(&mut buf, 0, 1, 1, &[], false).unwrap();
        write_u32_le(&mut buf, WAL_OFF_NUM_REGIONS, (MAX_WIRE_REGIONS + 1) as u32);

        let mut offsets = [0u64; MAX_WIRE_REGIONS + 1];
        let mut rsizes = [0u32; MAX_WIRE_REGIONS + 1];
        let rc = validate_and_parse(&buf[..block_len], &mut offsets, &mut rsizes, false);
        assert_eq!(rc, Err(WalError::InvalidShard));
    }

    #[test]
    fn rejects_region_extent_past_block() {
        // A directory entry whose [offset, offset + size) runs past total_size
        // must be rejected, not silently accepted — decoders rely on this to
        // avoid zero-filling an out-of-bounds region. checksum=false isolates
        // the extent check from the (directory-covering) checksum.
        let regions = make_test_regions();
        let mut buf = vec![0u8; 4096];
        let block_len = encode(&mut buf, 0, 1, 2, &as_slices(&regions), false).unwrap();

        // Point region 0's offset at the block end; its size (16) now overruns.
        write_u32_le(&mut buf, WAL_HEADER_SIZE, block_len as u32);

        let mut offsets = [0u64; 16];
        let mut rsizes = [0u32; 16];
        let rc = validate_and_parse(&buf[..block_len], &mut offsets, &mut rsizes, false);
        assert_eq!(rc, Err(WalError::Truncated));
    }

    #[test]
    fn buffer_too_small() {
        let r0 = [0u8; 100];
        let regions: [&[u8]; 1] = [&r0];
        let mut buf = vec![0u8; 64]; // too small

        let rc = encode(&mut buf, 0, 0, 0, &regions, true);
        assert_eq!(rc, Err(WalError::BufferTooSmall));
    }
}
