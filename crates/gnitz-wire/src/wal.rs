//! The low-level WAL-block codec — the one framer client and engine agree on.
//!
//! A WAL block is a header of the `WAL_OFF_*` words, then the canonical region
//! list packed end to end. A block is read only under its schema, so the header
//! carries only what a schema cannot supply: the row count, the heap length, and
//! an upper bound on the heap bytes no string cell references.
//! The dead-byte bound is the writer's claim, which an untrusting reader
//! recomputes.

use crate::{read_u32_le, write_u32_le, SYS_SCHEMA_DIGEST};

pub const WAL_HEADER_SIZE: usize = 20;

/// Bumped by hand for a block-layout change or a change to any payload the client and
/// the engine both decode.
pub(crate) const WAL_EPOCH: u32 = 32;

/// WAL/SAL block format version, and the client↔server HELLO version — the
/// only thing that rejects a stale SAL frame or an old client's catalog write.
/// [`SYS_SCHEMA_DIGEST`] folds in so a system-family shape change moves it too.
pub const WAL_FORMAT_VERSION: u32 = WAL_EPOCH ^ ((SYS_SCHEMA_DIGEST ^ (SYS_SCHEMA_DIGEST >> 32)) as u32);

/// Total block size: header, fixed regions and heap.
pub(crate) const WAL_OFF_SIZE: usize = 0;
const WAL_OFF_VERSION: usize = 4;
const WAL_OFF_ROWS: usize = 8;
const WAL_OFF_HEAP_LEN: usize = 12;
/// An upper bound on the heap bytes no string cell of the block references.
const WAL_OFF_HEAP_DEAD: usize = 16;
const _: () = assert!(WAL_OFF_HEAP_DEAD + 4 == WAL_HEADER_SIZE);

/// The framed size of a block over the canonical region list `regions`.
pub(crate) fn block_size(regions: &[&[u8]]) -> usize {
    WAL_HEADER_SIZE + regions.iter().map(|r| r.len()).sum::<usize>()
}

/// Write a block header into `dst[..WAL_HEADER_SIZE]` for `rows` rows whose
/// fixed regions total `fixed_len` bytes, followed by a `heap_len`-byte heap of
/// which at most `heap_dead` bytes are unreferenced. Returns the block's total
/// size.
pub fn write_head(dst: &mut [u8], rows: usize, fixed_len: usize, heap_len: usize, heap_dead: usize) -> usize {
    debug_assert!(heap_dead <= heap_len, "a heap holds at most its own length dead");
    let total = WAL_HEADER_SIZE + fixed_len + heap_len;
    write_u32_le(dst, WAL_OFF_SIZE, total as u32);
    write_u32_le(dst, WAL_OFF_VERSION, WAL_FORMAT_VERSION);
    write_u32_le(dst, WAL_OFF_ROWS, rows as u32);
    write_u32_le(dst, WAL_OFF_HEAP_LEN, heap_len as u32);
    write_u32_le(dst, WAL_OFF_HEAP_DEAD, heap_dead as u32);
    total
}

/// The row count of the canonical region list `regions`, read off its weight
/// region.
fn region_rows(regions: &[&[u8]]) -> usize {
    let w = regions[crate::REG_WEIGHT].len();
    debug_assert_eq!(w % 8, 0, "a weight region holds whole i64s");
    w / 8
}

/// The header of the block framing the canonical region list `regions`, and
/// that block's total size.
fn head(regions: &[&[u8]], heap_dead: usize) -> ([u8; WAL_HEADER_SIZE], usize) {
    let (heap, fixed) = regions.split_last().expect("a region list ends with its heap");
    let mut head = [0u8; WAL_HEADER_SIZE];
    let fixed_len = fixed.iter().map(|r| r.len()).sum();
    let total = write_head(&mut head, region_rows(regions), fixed_len, heap.len(), heap_dead);
    (head, total)
}

/// Frame the canonical region list `regions` into `dst`, its heap at most
/// `heap_dead` bytes unreferenced. Returns the bytes written.
pub fn write_block(regions: &[&[u8]], heap_dead: usize, dst: &mut [u8]) -> usize {
    let (head, total) = head(regions, heap_dead);
    dst[..WAL_HEADER_SIZE].copy_from_slice(&head);
    let mut at = WAL_HEADER_SIZE;
    for r in regions {
        dst[at..at + r.len()].copy_from_slice(r);
        at += r.len();
    }
    debug_assert_eq!(at, total);
    total
}

/// [`write_block`] onto the end of `out`.
pub fn append_block(regions: &[&[u8]], heap_dead: usize, out: &mut Vec<u8>) {
    let (head, total) = head(regions, heap_dead);
    out.reserve(total);
    out.extend_from_slice(&head);
    for r in regions {
        out.extend_from_slice(r);
    }
}

/// The WAL block at the front of `tail`, sized by its own `SIZE` field.
pub(crate) fn block_slice(tail: &[u8]) -> Result<&[u8], &'static str> {
    if tail.len() < WAL_HEADER_SIZE {
        return Err("block shorter than header");
    }
    let size = read_u32_le(tail, WAL_OFF_SIZE) as usize;
    if size < WAL_HEADER_SIZE || size > tail.len() {
        return Err("declared size past buffer");
    }
    Ok(&tail[..size])
}

/// A parsed block: its row count, its fixed-region bytes, its heap, and the
/// writer's bound on that heap's unreferenced bytes.
pub type ParsedBlock<'a> = (usize, &'a [u8], &'a [u8], usize);

/// Validate a block whose rows are `row_width` bytes of fixed regions each,
/// and return its row count, its fixed-region bytes, its heap and its
/// dead-byte bound. A block holds at least one row: an empty delta ships no
/// block.
pub fn parse_block(block: &[u8], row_width: usize) -> Result<ParsedBlock<'_>, &'static str> {
    let block = block_slice(block)?;
    if read_u32_le(block, WAL_OFF_VERSION) != WAL_FORMAT_VERSION {
        return Err("unknown block version");
    }
    let size = block.len();
    let rows = read_u32_le(block, WAL_OFF_ROWS) as usize;
    if rows == 0 {
        return Err("block holds no rows");
    }
    let heap_len = read_u32_le(block, WAL_OFF_HEAP_LEN) as usize;
    let heap_dead = read_u32_le(block, WAL_OFF_HEAP_DEAD) as usize;
    let fixed_end = rows.checked_mul(row_width).and_then(|f| f.checked_add(WAL_HEADER_SIZE));
    let Some(fixed_end) = fixed_end.filter(|&e| e.checked_add(heap_len) == Some(size)) else {
        return Err("block size does not match its rows and heap");
    };
    if heap_dead > heap_len {
        return Err("block declares more dead heap than heap");
    }
    Ok((
        rows,
        &block[WAL_HEADER_SIZE..fixed_end],
        &block[fixed_end..size],
        heap_dead,
    ))
}

#[cfg(test)]
#[path = "tests/wal.rs"]
mod tests;
