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

/// The fixed regions that precede the payload columns in the region
/// convention, in order: PK, weight, null bitmap. Payload column `pi` lives at region
/// `REG_PAYLOAD_START + pi`, and a batch has `NUM_FIXED_REGIONS + num_payload + 1`
/// regions in total (the trailing blob heap). One home for a layout the client,
/// the wire codec, and the engine all encode — the engine's `storage` re-exports
/// these rather than restating them.
pub const REG_PK: usize = 0;
pub const REG_WEIGHT: usize = 1;
pub const REG_NULL_BMP: usize = 2;
/// The fixed regions are exactly `REG_PK..=REG_NULL_BMP`, so the first payload
/// region index is also how many fixed regions precede it.
pub const REG_PAYLOAD_START: usize = REG_NULL_BMP + 1;

/// Region count of a batch with `num_payload_cols` payload columns: the fixed
/// three, one per payload column, and the trailing blob heap. The blob region's
/// index is `REG_PAYLOAD_START + num_payload_cols`, i.e. `num_regions(n) - 1`.
pub const fn num_regions(num_payload_cols: usize) -> usize {
    REG_PAYLOAD_START + num_payload_cols + 1
}

pub const WAL_HEADER_SIZE: usize = 32;
/// WAL/SAL block format version, and the client↔server HELLO version. Bumped by
/// hand on a block-layout change, and on a system-family shape change — which
/// this module's digest pin enforces. A SAL frame carries its own schema block
/// and replay decodes against that, so nothing but this word rejects a stale
/// frame, or an old client's catalog writes.
pub const WAL_FORMAT_VERSION: u32 = 15;

pub const WAL_OFF_TID: usize = 0;
pub const WAL_OFF_COUNT: usize = 4;
pub const WAL_OFF_SIZE: usize = 8;
pub const WAL_OFF_VERSION: usize = 12;
pub const WAL_OFF_CHECKSUM: usize = 16;
pub const WAL_OFF_NUM_REGIONS: usize = 24;

pub(crate) const IPC_CONTROL_TID: u32 = 0xFFFF_FFFF;

/// Maximum region count a block directory may name, including the trailing blob
/// region. A legitimate schema has ≤ 65 columns (1 PK), so ≤ 68 regions
/// (pk + weight + null + ≤ 64 payload + blob); 69 only ever caps a forged
/// block. The engine derives its own arena capacity from this — the blob heap
/// is not in the arena, so `MAX_BATCH_REGIONS` is this less one slot.
pub const MAX_WIRE_REGIONS: usize = 69;

/// Compute the total byte size of a WAL block with the given regions.
/// The region count is `region_sizes.len()` — no separate count param.
pub fn block_size(region_sizes: &[u32]) -> usize {
    block_size_from(region_sizes.len(), region_sizes.iter().map(|&sz| sz as usize))
}

/// Total byte size of the WAL block that would frame `regions` — the
/// slice-taking sibling of [`block_size`], for a caller that already holds the
/// `&[&[u8]]` [`encode`] takes and would otherwise materialize a parallel
/// `&[u32]` size array just to measure it.
pub fn block_size_of(regions: &[&[u8]]) -> usize {
    block_size_from(regions.len(), regions.iter().map(|r| r.len()))
}

/// Where a block's first region begins: the header, then one 8-byte directory
/// entry per region, `align8`ed as the region walk below pads. The one spelling
/// of that offset — a block whose regions are all empty is exactly this long.
pub const fn body_start(num_regions: usize) -> usize {
    align8(WAL_HEADER_SIZE + num_regions * 8)
}

/// The block-size walk both public forms share: header, directory, then each
/// region `align8`-padded before its data — the same walk
/// [`write_header_and_directory`] performs.
fn block_size_from(count: usize, sizes: impl Iterator<Item = usize>) -> usize {
    let mut pos = body_start(count);
    for sz in sizes {
        pos = align8(pos) + sz;
    }
    pos
}

/// Write the 32-byte WAL header (all fields) and the region directory into
/// `block`, zero-filling inter-region align8 gaps. Shared by [`encode`] (which
/// then copies prebuilt region bytes) and the SAL scatter writer
/// (`encode_scattered_to_wire`), which carves the body and scatters rows
/// directly — the one place the block framing is spelled out.
///
/// Each region's start position goes into its own directory entry, read back
/// through [`dir_entry`]. The caller stamps the body checksum afterwards via [`stamp_checksum`]
/// (or leaves the zeroed checksum field for unchecksummed IPC paths).
pub fn write_header_and_directory(
    block: &mut [u8],
    table_id: u32,
    entry_count: u32,
    region_sizes: impl ExactSizeIterator<Item = u32>,
    total_size: usize,
) {
    let num_regions = region_sizes.len();
    debug_assert!(
        num_regions <= MAX_WIRE_REGIONS,
        "num_regions={num_regions} exceeds the block directory's capacity"
    );
    block[..WAL_HEADER_SIZE].fill(0);
    let mut pos = body_start(num_regions);
    for (i, sz) in region_sizes.enumerate() {
        // At most 7 bytes by construction, so this stays a handful of stores
        // rather than a `memset` call per region.
        let aligned = align8(pos);
        for b in block[pos..aligned].iter_mut() {
            *b = 0;
        }
        pos = aligned;
        let dir_off = dir_entry_offset(i);
        write_u32_le(block, dir_off, pos as u32);
        write_u32_le(block, dir_off + 4, sz);
        pos += sz as usize;
    }
    write_u32_le(block, WAL_OFF_TID, table_id);
    write_u32_le(block, WAL_OFF_COUNT, entry_count);
    write_u32_le(block, WAL_OFF_SIZE, total_size as u32);
    write_u32_le(block, WAL_OFF_VERSION, WAL_FORMAT_VERSION);
    write_u32_le(block, WAL_OFF_NUM_REGIONS, num_regions as u32);
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
/// directory through [`validate_and_parse`], which bounds every entry. The
/// entry's position comes from [`dir_entry_offset`]; the size follows the offset.
#[inline]
pub fn dir_entry(block: &[u8], r: usize) -> (usize, usize) {
    let base = dir_entry_offset(r);
    (read_u32_le(block, base) as usize, read_u32_le(block, base + 4) as usize)
}

/// Byte offset of region `r`'s directory entry within the block. The directory
/// follows the header, 8 bytes per entry (offset then size) — the one place that
/// layout is spelled out.
#[inline]
pub const fn dir_entry_offset(r: usize) -> usize {
    WAL_HEADER_SIZE + r * 8
}

/// Header fields of a validated WAL block, as returned by
/// [`validate_and_parse`].
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
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
    let total_size = block_size_of(regions);

    if out_offset + total_size > out_buf.len() {
        return Err(WalError::BufferTooSmall);
    }

    let block = &mut out_buf[out_offset..out_offset + total_size];

    // Phase 1: header + directory.
    write_header_and_directory(
        block,
        table_id,
        entry_count,
        regions.iter().map(|r| r.len() as u32),
        total_size,
    );

    // Phase 2: copy each region to the position its directory entry names.
    // Coalescing source-adjacent runs into one `copy_nonoverlapping` is unsound:
    // the regions are independent `&[u8]`s, so a copy spanning past region `i`'s
    // length reads outside its provenance.
    for (i, r) in regions.iter().enumerate() {
        if r.is_empty() {
            continue;
        }
        let (dst, _) = dir_entry(block, i);
        block[dst..dst + r.len()].copy_from_slice(r);
    }

    if checksum_body {
        stamp_checksum(block, total_size);
    }

    Ok(out_offset + total_size)
}

/// The WAL block starting at `off`, sized by its own `SIZE` field.
///
/// Every framed decoder walks a run of concatenated blocks this way — control,
/// then schema, then data — so the bounds rule lives here rather than at each
/// step. A declared size below the header length is rejected here, where the
/// cause is legible, instead of reaching [`validate_and_parse`] as a truncated
/// block or a caller as a short slice.
pub fn block_slice_at(data: &[u8], off: usize) -> Result<&[u8], WalError> {
    if off + WAL_HEADER_SIZE > data.len() {
        return Err(WalError::Truncated);
    }
    let size = read_u32_le(data, off + WAL_OFF_SIZE) as usize;
    if size < WAL_HEADER_SIZE || off + size > data.len() {
        return Err(WalError::Truncated);
    }
    Ok(&data[off..off + size])
}

/// Verify a framed block's XXH3 body checksum on its own — the standalone form of
/// the check [`validate_and_parse`] folds into a full parse, for a framed decoder
/// that walks a run of blocks by header alone. A header-only block carries no
/// checksum and passes.
pub fn verify_body_checksum(block: &[u8]) -> Result<(), WalError> {
    if block.len() < WAL_HEADER_SIZE {
        return Err(WalError::Truncated);
    }
    let total_size = read_u32_le(block, WAL_OFF_SIZE) as usize;
    if total_size > block.len() || total_size < WAL_HEADER_SIZE {
        return Err(WalError::Truncated);
    }
    if total_size > WAL_HEADER_SIZE
        && checksum(&block[WAL_HEADER_SIZE..total_size]) != read_u64_le(block, WAL_OFF_CHECKSUM)
    {
        return Err(WalError::ChecksumMismatch);
    }
    Ok(())
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
        let dir_off = dir_entry_offset(i);
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
#[path = "tests/wal.rs"]
mod tests;
