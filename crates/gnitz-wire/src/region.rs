//! The region convention: the order a batch's column buffers are listed in, the
//! list type that holds them, and the null word one of those regions carries.
//! `wal` frames a list into a block; nothing here knows what a block looks like.

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

    pub fn clear(&mut self) {
        self.len = 0;
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

#[cfg(test)]
#[path = "tests/region.rs"]
mod tests;
