//! The low-level WAL-block codec — the one framer client and engine agree on.
//!
//! A WAL block is a header of the `WAL_OFF_*` words, then the canonical region
//! list packed end to end. A block is read only under its schema, so the header
//! carries only what a schema cannot supply: the total size, the row count, and
//! an upper bound on the heap bytes no string cell references. The heap is what
//! follows the rows' fixed regions.
//! The dead-byte bound is the writer's claim, which an untrusting reader
//! recomputes.
//!
//! A block carries no format version: its container does. A connection's HELLO,
//! a frame recording's header and the SAL's anchor page each carry
//! [`crate::WIRE_VERSION`], and each is refused when written under another.

use crate::{FIXED_REGION_BYTES, Reader, read_u32_le, write_u32_le};

pub const WAL_HEADER_SIZE: usize = 12;

/// Total block size: header, fixed regions and heap.
const WAL_OFF_SIZE: usize = 0;
const WAL_OFF_ROWS: usize = 4;
/// An upper bound on the heap bytes no string cell of the block references.
const WAL_OFF_HEAP_DEAD: usize = 8;
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
    write_u32_le(dst, WAL_OFF_ROWS, rows as u32);
    write_u32_le(dst, WAL_OFF_HEAP_DEAD, heap_dead as u32);
    total
}

/// The row count of the canonical region list `regions`, read off its weight
/// region.
fn region_rows(regions: &[&[u8]]) -> usize {
    let w = regions[crate::REG_WEIGHT].len();
    debug_assert_eq!(w % FIXED_REGION_BYTES, 0, "a weight region holds whole i64s");
    w / FIXED_REGION_BYTES
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

/// A parsed block.
#[derive(Debug, PartialEq, Eq)]
pub struct ParsedBlock<'a> {
    pub rows: usize,
    /// The fixed regions, packed end to end.
    pub fixed: &'a [u8],
    pub heap: &'a [u8],
    /// The writer's bound on the heap's unreferenced bytes.
    pub heap_dead: usize,
}

/// Validate a block whose rows are `row_width` bytes of fixed regions each,
/// and split it into its parts. `block` is exactly one block: its declared size
/// is its length. A block holds at least one row: an empty delta ships no block.
///
/// Out of line: it runs once per block, and inlined into a caller's
/// per-column loop its header words stay live across that loop.
#[inline(never)]
pub fn parse_block(block: &[u8], row_width: usize) -> Result<ParsedBlock<'_>, &'static str> {
    if block.len() < WAL_HEADER_SIZE {
        return Err("block shorter than header");
    }
    let mut r = Reader::new(block);
    // The header's words in layout order; the length check has seen it whole.
    let mut word = || r.u32().expect("a whole header") as usize;
    let (size, rows, heap_dead) = (word(), word(), word());
    debug_assert_eq!(r.pos(), WAL_HEADER_SIZE);
    if size != block.len() {
        return Err("declared size is not the block's length");
    }
    if rows == 0 {
        return Err("block holds no rows");
    }
    let fixed_end = rows.checked_mul(row_width).and_then(|f| f.checked_add(WAL_HEADER_SIZE));
    let Some(fixed_end) = fixed_end.filter(|&e| e <= size) else {
        return Err("block is too short for its rows");
    };
    if heap_dead > size - fixed_end {
        return Err("block declares more dead heap than heap");
    }
    Ok(ParsedBlock {
        rows,
        fixed: &block[WAL_HEADER_SIZE..fixed_end],
        heap: &block[fixed_end..size],
        heap_dead,
    })
}

#[cfg(test)]
#[path = "tests/wal.rs"]
mod tests;
