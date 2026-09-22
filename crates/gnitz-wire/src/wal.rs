//! The low-level WAL-block codec — the one framer client and engine agree on.
//!
//! A WAL block is a header, a directory of one `u32` region size each, and the
//! regions packed end to end. A region's offset is the sum of the sizes before
//! it, so the directory stores sizes alone and an inconsistent offset is
//! unrepresentable. The `WAL_OFF_*` constants below are the header's layout.
//!
//! Regions are raw byte slices here; each side layers its own per-column
//! transcode on top.

use crate::region::{Regions, MAX_WIRE_REGIONS};
use crate::{read_u32_le, write_u32_le, SYS_SCHEMA_DIGEST};

pub const WAL_HEADER_SIZE: usize = 20;

/// Bumped by hand for a block-layout change or a change to any payload the client and
/// the engine both decode.
pub(crate) const WAL_EPOCH: u32 = 27;

/// WAL/SAL block format version, and the client↔server HELLO version — the
/// only thing that rejects a stale SAL frame or an old client's catalog write.
/// [`SYS_SCHEMA_DIGEST`] folds in so a system-family shape change moves it too.
pub const WAL_FORMAT_VERSION: u32 = WAL_EPOCH ^ ((SYS_SCHEMA_DIGEST ^ (SYS_SCHEMA_DIGEST >> 32)) as u32);

pub const WAL_OFF_TID: usize = 0;
pub const WAL_OFF_COUNT: usize = 4;
/// Total block size: header, directory and every region.
pub(crate) const WAL_OFF_SIZE: usize = 8;
pub const WAL_OFF_VERSION: usize = 12;
pub const WAL_OFF_NUM_REGIONS: usize = 16;

/// The relation id a framed block carries.
#[inline]
pub fn block_tid(block: &[u8]) -> u32 {
    read_u32_le(block, WAL_OFF_TID)
}

/// Byte offset of region `r`'s directory entry.
#[inline]
pub const fn dir_entry_offset(r: usize) -> usize {
    WAL_HEADER_SIZE + r * 4
}

/// Where a block's first region begins: past the last directory entry.
pub const fn body_start(num_regions: usize) -> usize {
    dir_entry_offset(num_regions)
}

/// One WAL block as the region list it is framed from.
pub struct WalBlock<'a> {
    pub table_id: u32,
    pub entry_count: u32,
    pub regions: Regions<'a>,
}

impl<'a> WalBlock<'a> {
    /// An empty block; the caller fills `regions` in place.
    pub const fn new(table_id: u32, entry_count: u32) -> Self {
        WalBlock {
            table_id,
            entry_count,
            regions: Regions::new(),
        }
    }

    /// The framed size: header, directory and every region.
    pub fn size(&self) -> usize {
        body_start(self.regions.len()) + self.regions.iter().map(|r| r.len()).sum::<usize>()
    }

    /// Frame into `dst`, which must hold [`Self::size`] bytes. Returns the
    /// bytes written.
    pub fn write(&self, dst: &mut [u8]) -> usize {
        let sizes = self.regions.iter().map(|r| r.len() as u32);
        let (block, mut at) = frame(dst, self.table_id, self.entry_count, sizes);
        for r in self.regions.iter() {
            block[at..at + r.len()].copy_from_slice(r);
            at += r.len();
        }
        debug_assert_eq!(at, block.len(), "the region walk must cover the whole block");
        block.len()
    }

    /// Append the framed block to `out`.
    pub fn append_to(&self, out: &mut Vec<u8>) {
        let n = self.size();
        out.reserve(n);
        let at = out.len();
        // SAFETY: `reserve` leaves `n` writable bytes at `at`, and `write`
        // initialises and returns the prefix of them it framed.
        let written = unsafe { self.write(std::slice::from_raw_parts_mut(out.as_mut_ptr().add(at), n)) };
        unsafe { out.set_len(at + written) };
    }
}

/// Write the header and directory for regions of `sizes` into `dst`. Returns
/// the whole framed block, its region bytes still the caller's to fill, and
/// where the first of them starts.
pub fn frame(
    dst: &mut [u8],
    table_id: u32,
    count: u32,
    sizes: impl ExactSizeIterator<Item = u32>,
) -> (&mut [u8], usize) {
    let n = sizes.len();
    debug_assert!(
        n <= MAX_WIRE_REGIONS,
        "num_regions={n} exceeds the block directory's capacity"
    );

    let mut total_size = body_start(n);
    for (i, sz) in sizes.enumerate() {
        write_u32_le(dst, dir_entry_offset(i), sz);
        total_size += sz as usize;
    }
    write_u32_le(dst, WAL_OFF_TID, table_id);
    write_u32_le(dst, WAL_OFF_COUNT, count);
    write_u32_le(dst, WAL_OFF_SIZE, total_size as u32);
    write_u32_le(dst, WAL_OFF_VERSION, WAL_FORMAT_VERSION);
    write_u32_le(dst, WAL_OFF_NUM_REGIONS, n as u32);

    (&mut dst[..total_size], body_start(n))
}

/// The WAL block starting at `off`, sized by its own `SIZE` field.
pub fn block_slice_at(data: &[u8], off: usize) -> Result<&[u8], &'static str> {
    if off + WAL_HEADER_SIZE > data.len() {
        return Err("block shorter than header");
    }
    let size = read_u32_le(data, off + WAL_OFF_SIZE) as usize;
    if size < WAL_HEADER_SIZE || off + size > data.len() {
        return Err("declared size past buffer");
    }
    Ok(&data[off..off + size])
}

/// Validate a WAL block, fill `out` with its regions, and return its row count.
pub fn validate_and_parse<'a>(block: &'a [u8], out: &mut Regions<'a>) -> Result<u32, &'static str> {
    out.clear();
    if block.len() < WAL_HEADER_SIZE {
        return Err("block shorter than header");
    }
    if read_u32_le(block, WAL_OFF_VERSION) != WAL_FORMAT_VERSION {
        return Err("unknown block version");
    }
    let total_size = read_u32_le(block, WAL_OFF_SIZE) as usize;
    if total_size > block.len() || total_size < WAL_HEADER_SIZE {
        return Err("declared size past buffer");
    }

    let n = read_u32_le(block, WAL_OFF_NUM_REGIONS) as usize;
    if n > MAX_WIRE_REGIONS {
        return Err("region count past cap");
    }
    if body_start(n) > total_size {
        return Err("directory past block");
    }

    let mut off = body_start(n);
    for i in 0..n {
        // Against `total_size`, not `block.len()`: a block in a multi-block
        // buffer must not reach into the next one. Both terms came from a u32,
        // so the sum cannot overflow.
        let sz = read_u32_le(block, dir_entry_offset(i)) as usize;
        if off + sz > total_size {
            return Err("region extent past block");
        }
        out.push(&block[off..off + sz]);
        off += sz;
    }
    if off != total_size {
        return Err("directory does not cover the block");
    }

    Ok(read_u32_le(block, WAL_OFF_COUNT))
}

#[cfg(test)]
#[path = "tests/wal.rs"]
mod tests;
