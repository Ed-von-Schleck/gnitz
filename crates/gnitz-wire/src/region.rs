//! The region convention: the order a batch's column buffers are listed in, the
//! list type that holds them, the null word one of those regions carries, and
//! the per-row reader over them. `wal` frames a list into a block; nothing here
//! knows what a block looks like.

use std::mem::MaybeUninit;
use std::ops::Deref;

/// Fixed regions, in order. Payload column `pi` is at `REG_PAYLOAD_START + pi`,
/// and the blob heap is last.
pub const REG_PK: usize = 0;
pub const REG_WEIGHT: usize = 1;
pub const REG_NULL_BMP: usize = 2;
pub const REG_PAYLOAD_START: usize = REG_NULL_BMP + 1;

/// The fixed three, one per payload column, and the blob heap.
pub const fn num_regions(num_payload_cols: usize) -> usize {
    REG_PAYLOAD_START + num_payload_cols + 1
}

/// One payload column wider than any legal schema: a real one has at least one
/// PK column, so at most `MAX_COLUMNS - 1` payload columns.
pub const MAX_WIRE_REGIONS: usize = num_regions(crate::MAX_COLUMNS);

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

/// True iff payload null-bit `pi` is set (the column is NULL). Bit `pi` is the
/// `pi`-th non-PK column in schema order.
#[inline(always)]
pub fn null_word_get(word: u64, pi: usize) -> bool {
    (word >> pi) & 1 == 1
}

/// See [`null_word_get`].
#[inline(always)]
pub fn null_word_set(word: &mut u64, pi: usize, is_null: bool) {
    if is_null {
        *word |= 1u64 << pi;
    } else {
        *word &= !(1u64 << pi);
    }
}

/// The first `(row, payload slot)` whose little-endian `u64` null word in
/// `null_bmp` sets a bit in `not_null`.
pub fn first_not_null_violation(not_null: u64, null_bmp: &[u8]) -> Option<(usize, usize)> {
    let words = null_bmp.as_chunks::<8>().0;
    // A conforming batch takes only this branch-free pass.
    if not_null == 0 || words.iter().fold(0u64, |a, w| a | u64::from_le_bytes(*w)) & not_null == 0 {
        return None;
    }
    words.iter().enumerate().find_map(|(row, w)| {
        let bad = u64::from_le_bytes(*w) & not_null;
        (bad != 0).then(|| (row, bad.trailing_zeros() as usize))
    })
}

/// The first `(row, payload slot)` whose null bit in `null_bmp`, among the
/// `nullable` slots, sits over a non-zero cell: NULL encodes as a zeroed cell.
/// `col(slot)` is that slot's region and its cell width.
pub fn first_valued_null<'a>(
    nullable: u64,
    null_bmp: &[u8],
    col: impl Fn(usize) -> (&'a [u8], usize),
) -> Option<(usize, usize)> {
    for (row, word) in null_bmp.as_chunks::<8>().0.iter().enumerate() {
        let mut set = u64::from_le_bytes(*word) & nullable;
        while set != 0 {
            let slot = set.trailing_zeros() as usize;
            set &= set - 1;
            let (cells, width) = col(slot);
            if cells[row * width..(row + 1) * width].iter().any(|&b| b != 0) {
                return Some((row, slot));
            }
        }
    }
    None
}

/// A row's null word rebased onto output payload slot `at`; slot 64 and beyond
/// hold no bit.
#[inline(always)]
pub fn null_word_at(word: u64, at: usize) -> u64 {
    if at < 64 {
        word << at
    } else {
        0
    }
}

/// Read one row's columns. The **one** per-row access shape in the system: the
/// resolved-addressing types (`ColumnLocator`) bind to it, the engine's
/// `ColumnarSource` extends it with the Z-set weight, and `BatchView` extends
/// it with the region accessors the vectorized kernels need. A source that can
/// address cells but has no contiguous `rows * col_size` region to hand out — a
/// shard mapping whose column is a scalar constant — implements this and
/// nothing more. Every source is a whole multi-row batch, which is why
/// [`Self::row_count`] sits here and not one level up.
///
/// Static dispatch only wherever a *cell* is read per row: a `&dyn RowSource`
/// there would put an indirect call on every `ColumnLocator` read.
/// A whole-batch consumer that resolves the regions once per morsel is not one
/// of those, and the evaluator's kernels take `&dyn BatchView`.
/// (Convention: the trait is dyn-compatible, nothing enforces this.)
///
/// All lifetimes are tied to `&self`, NOT decoupled — a client adapter owns the
/// buffers it materializes and can only lend them for `&self`.
pub trait RowSource {
    /// The row's packed PK-region bytes (`pk_stride` wide), **order-preserving
    /// at-rest (OPK)** at every source, a client `ZSetBatch` included. That is
    /// what the OPK-inverting readers on `ColumnLocator` assume.
    fn get_pk_bytes(&self, row: usize) -> &[u8];
    /// The row's null-bitmap word (bit N = payload slot N is NULL).
    fn get_null_word(&self, row: usize) -> u64;
    /// `col_size` bytes of payload column `payload_col` (native LE) in `row`.
    fn get_col_ptr(&self, row: usize, payload_col: usize, col_size: usize) -> &[u8];
    /// The variable-length string/blob heap the German-string cells point into.
    fn blob(&self) -> &[u8];
    /// Rows in this source. The bound every whole-source walk reads — the
    /// evaluator's `RowFilter::ranges`, the engine's N-way merge
    /// — so that a caller can never drive a view past its own end with a count it
    /// carried alongside. `#[inline(always)]` on every implementor: the per-row
    /// and per-morsel callers live in gnitz-zset, at opt-level 0.
    fn row_count(&self) -> usize;
}

/// A borrowed source reads as the source it borrows.
impl<T: RowSource + ?Sized> RowSource for &T {
    #[inline(always)]
    fn get_pk_bytes(&self, row: usize) -> &[u8] {
        (**self).get_pk_bytes(row)
    }
    #[inline(always)]
    fn get_null_word(&self, row: usize) -> u64 {
        (**self).get_null_word(row)
    }
    #[inline(always)]
    fn get_col_ptr(&self, row: usize, payload_col: usize, col_size: usize) -> &[u8] {
        (**self).get_col_ptr(row, payload_col, col_size)
    }
    #[inline(always)]
    fn blob(&self) -> &[u8] {
        (**self).blob()
    }
    #[inline(always)]
    fn row_count(&self) -> usize {
        (**self).row_count()
    }
}

/// One row's fixed 8-byte payload slot `pi`, little-endian.
pub fn payload_u64<S: RowSource>(src: &S, row: usize, pi: usize) -> u64 {
    crate::read_u64_le(src.get_col_ptr(row, pi, 8), 0)
}

/// One row's STRING or BLOB payload slot `pi`, resolved through the source's
/// heap.
#[inline(always)]
pub fn payload_bytes<S: RowSource>(src: &S, row: usize, pi: usize) -> &[u8] {
    crate::german_string_content(src.get_col_ptr(row, pi, 16), src.blob())
}

/// [`payload_bytes`] as a `&str`; an error when the cell is not UTF-8.
pub fn payload_str<S: RowSource>(src: &S, row: usize, pi: usize) -> Result<&str, std::str::Utf8Error> {
    std::str::from_utf8(payload_bytes(src, row, pi))
}

/// Whether one row's payload slot `pi` is NULL.
pub fn payload_is_null<S: RowSource>(src: &S, row: usize, pi: usize) -> bool {
    null_word_get(src.get_null_word(row), pi)
}

#[cfg(test)]
#[path = "tests/region.rs"]
mod tests;
