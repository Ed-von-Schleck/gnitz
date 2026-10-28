//! Resolved column addressing: where a logical column physically lives in a row, and the reads
//! through it.

use std::cmp::Ordering;

use gnitz_wire::{RowSource, TypeCode};

/// Where a logical column's value physically lives in a row, resolved once from
/// the schema.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ColumnLocator {
    /// In the OPK-encoded PK region at `byte_off`; never NULL.
    Pk {
        byte_off: u8,
        size: u8,
        type_code: TypeCode,
    },
    /// Native little-endian in payload slot `slot`, which is also its null bit.
    Payload { slot: u8, size: u8, type_code: TypeCode },
}

const _: () = assert!(
    std::mem::size_of::<ColumnLocator>() <= 8,
    "ColumnLocator must stay within 8 bytes",
);
const _: () = assert!(
    gnitz_wire::MAX_PK_BYTES <= u8::MAX as usize,
    "ColumnLocator::Pk::byte_off is u8; the PK region no longer fits it",
);
const _: () = assert!(
    gnitz_wire::MAX_COLUMNS <= u8::MAX as usize,
    "ColumnLocator::Payload::slot is u8; the payload slot space no longer fits it",
);

impl ColumnLocator {
    #[inline(always)]
    pub fn size(&self) -> usize {
        match *self {
            ColumnLocator::Pk { size, .. } | ColumnLocator::Payload { size, .. } => size as usize,
        }
    }

    #[inline(always)]
    pub fn type_code(&self) -> TypeCode {
        match *self {
            ColumnLocator::Pk { type_code, .. } | ColumnLocator::Payload { type_code, .. } => type_code,
        }
    }

    /// The payload slot, which is also the null bit; `None` for a PK column.
    #[inline(always)]
    pub fn payload_slot(&self) -> Option<usize> {
        match *self {
            ColumnLocator::Pk { .. } => None,
            ColumnLocator::Payload { slot, .. } => Some(slot as usize),
        }
    }

    /// This column's bit in a null word; no bit for a PK column, which is never NULL.
    #[inline(always)]
    pub fn null_bit(&self) -> u64 {
        self.payload_slot().map_or(0, |slot| 1 << slot)
    }

    /// True iff this column is NULL in `row`.
    #[inline(always)]
    pub fn is_null(&self, mb: &impl RowSource, row: usize) -> bool {
        match *self {
            ColumnLocator::Pk { .. } => false,
            ColumnLocator::Payload { .. } => self.is_null_word(mb.get_null_word(row)),
        }
    }

    /// [`Self::is_null`] against `row`'s already-read null word.
    #[inline(always)]
    pub fn is_null_word(&self, null_word: u64) -> bool {
        match *self {
            ColumnLocator::Pk { .. } => false,
            ColumnLocator::Payload { slot, .. } => gnitz_wire::null_word_get(null_word, slot as usize),
        }
    }

    /// Raw at-rest bytes of the column in `row`: OPK for a PK column, native
    /// little-endian for a payload column, the 16-byte German-string cell for a
    /// STRING/BLOB.
    #[inline(always)]
    pub fn bytes<'b>(&self, mb: &'b impl RowSource, row: usize) -> &'b [u8] {
        match *self {
            ColumnLocator::Pk { byte_off, size, .. } => {
                let o = byte_off as usize;
                &mb.get_pk_bytes(row)[o..o + size as usize]
            }
            ColumnLocator::Payload { slot, size, .. } => mb.get_col_ptr(row, slot as usize, size as usize),
        }
    }

    /// A STRING/BLOB column's content in `row`, resolved through the source's
    /// heap.
    #[inline(always)]
    pub fn content<'b>(&self, mb: &'b impl RowSource, row: usize) -> std::borrow::Cow<'b, [u8]> {
        gnitz_wire::german_string_bytes(self.bytes(mb, row), mb.heap())
    }

    /// Native little-endian value bytes of the column in `row`; a PK column is
    /// OPK-decoded into `scratch`.
    #[inline(always)]
    pub fn native_le_bytes<'a, 'b: 'a>(
        &self,
        mb: &'b impl RowSource,
        row: usize,
        scratch: &'a mut [u8; 16],
    ) -> &'a [u8] {
        match *self {
            ColumnLocator::Pk { size, type_code, .. } => {
                let dst = &mut scratch[..size as usize];
                gnitz_wire::decode_pk_cell(self.bytes(mb, row), type_code.is_signed_int(), dst);
                dst
            }
            ColumnLocator::Payload { .. } => self.bytes(mb, row),
        }
    }

    /// The ≤8-byte integer value in `row`, widened to `i64`. `fi` is the
    /// column's own type.
    #[inline(always)]
    pub fn decode_i64(&self, mb: &impl RowSource, row: usize, fi: gnitz_wire::FixedInt) -> i64 {
        debug_assert_eq!(gnitz_wire::FixedInt::from_type_code(self.type_code()), Some(fi));
        match *self {
            ColumnLocator::Pk { .. } => gnitz_wire::decode_opk_i64(self.bytes(mb, row), fi),
            ColumnLocator::Payload { .. } => fi.decode_le_i64(self.bytes(mb, row)),
        }
    }

    /// Order two rows on this column, **both known non-NULL**: a PK window by
    /// its OPK bytes, a payload window by typed value (STRING/BLOB by content).
    #[inline(always)]
    pub fn cmp_non_null<A: RowSource, B: RowSource>(&self, a: &A, ra: usize, b: &B, rb: usize) -> Ordering {
        match *self {
            ColumnLocator::Pk { .. } => self.bytes(a, ra).cmp(self.bytes(b, rb)),
            ColumnLocator::Payload { type_code, .. } if type_code.is_german_string() => {
                let cell = |s: &[u8]| -> [u8; 16] { s.try_into().unwrap() };
                gnitz_wire::compare_german_strings(
                    &cell(self.bytes(a, ra)),
                    a.heap(),
                    &cell(self.bytes(b, rb)),
                    b.heap(),
                )
            }
            ColumnLocator::Payload { type_code, .. } => {
                gnitz_wire::cmp_col_window(self.bytes(a, ra), self.bytes(b, rb), type_code)
            }
        }
    }

    /// The value in `row` as its OPK bytes read as a big-endian integer; a NULL
    /// cell encodes as its zero value. A STRING/BLOB yields its cell, not its
    /// content.
    #[inline(always)]
    pub fn opk_image(&self, mb: &impl RowSource, row: usize) -> u128 {
        let cell = self.bytes(mb, row);
        match *self {
            ColumnLocator::Pk { .. } => gnitz_wire::widen_pk_be(cell),
            ColumnLocator::Payload { size, type_code, .. } => {
                let signed = type_code.is_signed_int();
                if size == 16 {
                    u128::from_le_bytes(cell.try_into().unwrap()) ^ ((signed as u128) << 127)
                } else {
                    (gnitz_wire::read_unsigned_exact(cell) ^ ((signed as u64) << (size * 8 - 1))) as u128
                }
            }
        }
    }
}

#[cfg(test)]
#[path = "tests/locator.rs"]
mod tests;
