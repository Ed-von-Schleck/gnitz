//! The low-level WAL-block codec — the one framer client and engine agree on.
//!
//! A WAL block is a header of the `WAL_OFF_*` words, then the canonical region
//! list packed end to end. A block is read only under its schema, so the header
//! carries only what a schema cannot supply: the row count and the heap length.

use crate::{read_u32_le, write_u32_le, SYS_SCHEMA_DIGEST};

pub const WAL_HEADER_SIZE: usize = 16;

/// Bumped by hand for a block-layout change or a change to any payload the client and
/// the engine both decode.
pub(crate) const WAL_EPOCH: u32 = 28;

/// WAL/SAL block format version, and the client↔server HELLO version — the
/// only thing that rejects a stale SAL frame or an old client's catalog write.
/// [`SYS_SCHEMA_DIGEST`] folds in so a system-family shape change moves it too.
pub const WAL_FORMAT_VERSION: u32 = WAL_EPOCH ^ ((SYS_SCHEMA_DIGEST ^ (SYS_SCHEMA_DIGEST >> 32)) as u32);

/// Total block size: header, fixed regions and heap.
pub(crate) const WAL_OFF_SIZE: usize = 0;
pub const WAL_OFF_VERSION: usize = 4;
pub const WAL_OFF_ROWS: usize = 8;
pub const WAL_OFF_HEAP_LEN: usize = 12;

/// The framed size of a block over the canonical region list `regions`.
pub fn block_size(regions: &[&[u8]]) -> usize {
    WAL_HEADER_SIZE + regions.iter().map(|r| r.len()).sum::<usize>()
}

/// Write a block header into `dst[..WAL_HEADER_SIZE]` for `rows` rows whose
/// fixed regions total `fixed_len` bytes, followed by a `heap_len`-byte heap.
/// Returns the block's total size.
pub fn write_head(dst: &mut [u8], rows: usize, fixed_len: usize, heap_len: usize) -> usize {
    let total = WAL_HEADER_SIZE + fixed_len + heap_len;
    write_u32_le(dst, WAL_OFF_SIZE, total as u32);
    write_u32_le(dst, WAL_OFF_VERSION, WAL_FORMAT_VERSION);
    write_u32_le(dst, WAL_OFF_ROWS, rows as u32);
    write_u32_le(dst, WAL_OFF_HEAP_LEN, heap_len as u32);
    total
}

/// Frame `rows` rows laid out as the canonical region list `regions` into
/// `dst`. Returns the bytes written.
pub fn write_block(rows: usize, regions: &[&[u8]], dst: &mut [u8]) -> usize {
    let (heap, fixed) = regions.split_last().expect("a region list ends with its heap");
    let total = write_head(dst, rows, fixed.iter().map(|r| r.len()).sum(), heap.len());
    let mut at = WAL_HEADER_SIZE;
    for r in regions {
        dst[at..at + r.len()].copy_from_slice(r);
        at += r.len();
    }
    debug_assert_eq!(at, total);
    total
}

/// [`write_block`] onto the end of `out`.
pub fn append_block(rows: usize, regions: &[&[u8]], out: &mut Vec<u8>) {
    let (heap, fixed) = regions.split_last().expect("a region list ends with its heap");
    let mut head = [0u8; WAL_HEADER_SIZE];
    let total = write_head(&mut head, rows, fixed.iter().map(|r| r.len()).sum(), heap.len());
    out.reserve(total);
    out.extend_from_slice(&head);
    for r in regions {
        out.extend_from_slice(r);
    }
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

/// Validate a block whose rows are `row_width` bytes of fixed regions each,
/// and return its row count, its fixed-region bytes and its heap. A zero-row
/// block's heap is dropped: no cell can reference it.
pub fn parse_block(block: &[u8], row_width: usize) -> Result<(usize, &[u8], &[u8]), &'static str> {
    if block.len() < WAL_HEADER_SIZE {
        return Err("block shorter than header");
    }
    if read_u32_le(block, WAL_OFF_VERSION) != WAL_FORMAT_VERSION {
        return Err("unknown block version");
    }
    let size = read_u32_le(block, WAL_OFF_SIZE) as usize;
    if size > block.len() {
        return Err("declared size past buffer");
    }
    let rows = read_u32_le(block, WAL_OFF_ROWS) as usize;
    let heap_len = read_u32_le(block, WAL_OFF_HEAP_LEN) as usize;
    let fixed_end = rows.checked_mul(row_width).and_then(|f| f.checked_add(WAL_HEADER_SIZE));
    let Some(fixed_end) = fixed_end.filter(|&e| e.checked_add(heap_len) == Some(size)) else {
        return Err("block size does not match its rows and heap");
    };
    let heap = if rows == 0 { &[][..] } else { &block[fixed_end..size] };
    Ok((rows, &block[WAL_HEADER_SIZE..fixed_end], heap))
}

#[cfg(test)]
#[path = "tests/wal.rs"]
mod tests;
