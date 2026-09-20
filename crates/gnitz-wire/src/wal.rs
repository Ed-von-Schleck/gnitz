//! The low-level WAL-block codec — the one framer client and engine agree on.
//!
//! A WAL block is `[32B header][directory: num_regions × 8B][data regions, packed
//! end to end]`. This module owns the header constants, the region-directory
//! framer ([`WalBlock`] / [`validate_and_parse`]), the size/header/checksum
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

use std::mem::MaybeUninit;
use std::ops::Deref;

use crate::{checksum, read_u32_le, read_u64_le, write_u32_le, write_u64_le, WalError};

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
/// this module's digest pin enforces. A SAL frame's *data* block carries this
/// word and replay decodes against it, so nothing but this word rejects a stale
/// frame, or an old client's catalog writes. Its schema record carries no
/// version of its own.
pub const WAL_FORMAT_VERSION: u32 = 25;

pub const WAL_OFF_TID: usize = 0;
pub const WAL_OFF_COUNT: usize = 4;
pub const WAL_OFF_SIZE: usize = 8;
pub const WAL_OFF_VERSION: usize = 12;
pub const WAL_OFF_CHECKSUM: usize = 16;
pub const WAL_OFF_NUM_REGIONS: usize = 24;

/// Maximum region count a block directory may name, including the trailing blob
/// region. A legitimate schema has ≤ 65 columns (1 PK), so ≤ 68 regions
/// (pk + weight + null + ≤ 64 payload + blob); 69 only ever caps a forged
/// block. The engine derives its own arena capacity from this — the blob heap
/// is not in the arena, so `MAX_BATCH_REGIONS` is this less one slot.
pub const MAX_WIRE_REGIONS: usize = 69;

/// A block's canonical region list, held inline.
pub struct Regions<'a> {
    slots: [MaybeUninit<&'a [u8]>; MAX_WIRE_REGIONS],
    len: usize,
}

impl<'a> Regions<'a> {
    pub const fn new() -> Self {
        Regions {
            slots: [const { MaybeUninit::uninit() }; MAX_WIRE_REGIONS],
            len: 0,
        }
    }

    /// Append `region`. Panics past [`MAX_WIRE_REGIONS`].
    pub fn push(&mut self, region: &'a [u8]) {
        self.slots[self.len].write(region);
        self.len += 1;
    }
}

impl Default for Regions<'_> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'a> Deref for Regions<'a> {
    type Target = [&'a [u8]];

    fn deref(&self) -> &Self::Target {
        // SAFETY: `push` initialised every slot below `len`, and `MaybeUninit<T>` has `T`'s layout.
        unsafe { std::slice::from_raw_parts(self.slots.as_ptr().cast(), self.len) }
    }
}

/// One WAL block as the region list it is framed from.
pub struct WalBlock<'a> {
    pub table_id: u32,
    pub entry_count: u32,
    pub regions: Regions<'a>,
}

impl WalBlock<'_> {
    /// The framed size: header, directory and every region.
    pub fn size(&self) -> usize {
        body_start(self.regions.len()) + self.regions.iter().map(|r| r.len()).sum::<usize>()
    }

    /// The header and directory, into the first [`body_start`] bytes of `block`.
    fn write_head(&self, block: &mut [u8]) {
        write_header_and_directory(
            block,
            self.table_id,
            self.entry_count,
            self.regions.len(),
            self.regions.iter().map(|r| r.len() as u32),
            self.size(),
        );
    }

    /// Frame into `dst`, which must hold [`Self::size`] bytes. `checksum_body`
    /// stamps the XXH3 body checksum, which only SAL recovery reads.
    pub fn write(&self, dst: &mut [u8], checksum_body: bool) {
        let total_size = self.size();
        let block = &mut dst[..total_size];
        self.write_head(block);
        let mut at = body_start(self.regions.len());
        for r in self.regions.iter() {
            block[at..at + r.len()].copy_from_slice(r);
            at += r.len();
        }
        if checksum_body {
            stamp_checksum(block, total_size);
        }
    }

    /// Append the framed block to `out`, with no body checksum.
    pub fn append_to(&self, out: &mut Vec<u8>) {
        let at = out.len();
        out.reserve(self.size());
        out.resize(at + body_start(self.regions.len()), 0);
        self.write_head(&mut out[at..]);
        for r in self.regions.iter() {
            out.extend_from_slice(r);
        }
    }
}

/// Where a block's first region begins: the header, then one 8-byte directory
/// entry per region. The one spelling of that offset — a block whose regions are
/// all empty is exactly this long.
pub const fn body_start(num_regions: usize) -> usize {
    WAL_HEADER_SIZE + num_regions * 8
}

/// Byte size of the block [`frame_in_place`] frames: `count` rows over the fixed
/// regions `row_bytes` (each one's per-row width), with a `blob_len`-byte heap
/// last.
pub fn strided_block_size(row_bytes: &[u8], count: usize, blob_len: usize) -> usize {
    body_start(row_bytes.len() + 1) + count * row_bytes.iter().map(|&s| s as usize).sum::<usize>() + blob_len
}

/// Write the 32-byte WAL header and the region directory into `block` — the one
/// place a block's framing is spelled out. Each region's start goes into its own
/// directory entry, read back through [`dir_entry`].
///
/// The checksum field is left zeroed; [`stamp_checksum`] fills it where a reader
/// verifies one.
fn write_header_and_directory(
    block: &mut [u8],
    table_id: u32,
    entry_count: u32,
    num_regions: usize,
    region_sizes: impl Iterator<Item = u32>,
    total_size: usize,
) {
    debug_assert!(
        num_regions <= MAX_WIRE_REGIONS,
        "num_regions={num_regions} exceeds the block directory's capacity"
    );
    block[..WAL_HEADER_SIZE].fill(0);
    let mut pos = body_start(num_regions);
    for (i, sz) in region_sizes.enumerate() {
        let dir_off = dir_entry_offset(i);
        write_u32_le(block, dir_off, pos as u32);
        write_u32_le(block, dir_off + 4, sz);
        pos += sz as usize;
    }
    debug_assert_eq!(pos, total_size, "the directory walk must cover the whole block");
    write_u32_le(block, WAL_OFF_TID, table_id);
    write_u32_le(block, WAL_OFF_COUNT, entry_count);
    write_u32_le(block, WAL_OFF_SIZE, total_size as u32);
    write_u32_le(block, WAL_OFF_VERSION, WAL_FORMAT_VERSION);
    write_u32_le(block, WAL_OFF_NUM_REGIONS, num_regions as u32);
}

/// Frame a block of `count` rows over the fixed regions `row_bytes` (each one's
/// per-row width) into `out[offset..]`: returns the total size and one writable
/// slice per fixed region, in directory order.
///
/// The trailing blob region is named at size zero, so the block carries no heap.
pub fn frame_in_place<'a>(
    out: &'a mut [u8],
    offset: usize,
    table_id: u32,
    count: usize,
    row_bytes: &[u8],
) -> (usize, Vec<&'a mut [u8]>) {
    let num_regions = row_bytes.len() + 1;
    let total_size = strided_block_size(row_bytes, count, 0);
    let block = &mut out[offset..offset + total_size];

    write_header_and_directory(
        block,
        table_id,
        count as u32,
        num_regions,
        row_bytes
            .iter()
            .map(|&s| (count * s as usize) as u32)
            .chain(std::iter::once(0)),
        total_size,
    );

    let mut rest: &mut [u8] = &mut block[body_start(num_regions)..];
    let mut regions: Vec<&mut [u8]> = Vec::with_capacity(row_bytes.len());
    for &s in row_bytes {
        let (region, remainder) = std::mem::take(&mut rest).split_at_mut(count * s as usize);
        regions.push(region);
        rest = remainder;
    }
    (total_size, regions)
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

/// A framed block's XXH3 body checksum; a header-only block carries none.
fn verify_body_checksum(block: &[u8]) -> Result<(), WalError> {
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

/// Validate a WAL block and slice out its header and regions.
///
/// `verify_checksum` verifies the XXH3 body checksum, which only SAL recovery —
/// the one reader of blocks written with one — passes `true` for.
pub fn validate_and_parse(block: &[u8], verify_checksum: bool) -> Result<(WalBlockHeader, Regions<'_>), WalError> {
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

    if verify_checksum {
        verify_body_checksum(block)?;
    }

    let header = WalBlockHeader {
        table_id: read_u32_le(block, WAL_OFF_TID),
        entry_count: read_u32_le(block, WAL_OFF_COUNT),
        num_regions: read_u32_le(block, WAL_OFF_NUM_REGIONS),
        total_size,
    };

    let n = header.num_regions as usize;
    if n > MAX_WIRE_REGIONS {
        return Err(WalError::InvalidShard);
    }
    let mut regions = Regions::new();
    for i in 0..n {
        let dir_off = dir_entry_offset(i);
        if dir_off + 8 > total_size {
            return Err(WalError::Truncated);
        }
        let off = read_u32_le(block, dir_off) as usize;
        let sz = read_u32_le(block, dir_off + 4) as usize;
        // Against `total_size`, not `block.len()`: a block in a multi-block
        // buffer must not name a region reaching into the next one. `off`/`sz`
        // are u32, so the sum cannot overflow a 64-bit usize.
        if off + sz > total_size {
            return Err(WalError::Truncated);
        }
        regions.push(&block[off..off + sz]);
    }

    Ok((header, regions))
}

#[cfg(test)]
#[path = "tests/wal.rs"]
mod tests;
