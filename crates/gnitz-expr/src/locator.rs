//! Resolved column addressing: where a logical column physically lives in a
//! row, and the canonical `u128` keys derived from it.

use std::cmp::Ordering;

use gnitz_wire::TypeCode;

use crate::{RowSource, SchemaFacts};

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
    "ColumnLocator must stay packed; a usize coordinate would balloon it to 24 bytes",
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
                gnitz_wire::decode_pk_column(self.bytes(mb, row), type_code, dst);
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
            ColumnLocator::Payload { type_code, .. } => {
                gnitz_wire::cmp_col_window(self.bytes(a, ra), a.blob(), self.bytes(b, rb), b.blob(), type_code)
            }
        }
    }

    /// Write this column's value in `row` as `out_tc`'s OPK bytes into `dst`;
    /// `out_tc` must hold every value of this column's type.
    #[inline(always)]
    pub fn encode_opk_promoted(&self, mb: &impl RowSource, row: usize, out_tc: TypeCode, dst: &mut [u8]) {
        gnitz_wire::store_opk_image(self.opk_image(mb, row), self.type_code(), self.size(), out_tc, dst);
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

/// One resolved ORDER BY key.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OrderLocator {
    pub loc: ColumnLocator,
    pub desc: bool,
    /// NULLs sort first, whatever `desc` says.
    pub nulls_first: bool,
}

impl OrderLocator {
    /// The key `wire` names, over a column already located.
    #[inline]
    pub fn of(loc: ColumnLocator, wire: &gnitz_wire::OrderKey) -> Self {
        OrderLocator {
            loc,
            desc: wire.desc,
            nulls_first: wire.nulls_first,
        }
    }
}

/// The keys `order` names over `schema`, then — unless there are none — the
/// identity tiebreak. Panics on a column `schema` does not have.
pub fn order_locators(order: &[gnitz_wire::OrderKey], schema: &dyn SchemaFacts) -> Vec<OrderLocator> {
    let mut keys: Vec<OrderLocator> = order
        .iter()
        .map(|k| OrderLocator::of(schema.locate(k.col as usize), k))
        .collect();
    if !keys.is_empty() {
        push_identity_tiebreak(&mut keys, schema);
    }
    keys
}

/// Every column ascending, NULLS FIRST, PK columns leading as in the (PK,
/// payload) consolidation order: a total order over distinct rows.
fn push_identity_tiebreak(keys: &mut Vec<OrderLocator>, schema: &dyn SchemaFacts) {
    let asc = |loc| OrderLocator { loc, desc: false, nulls_first: true };
    keys.extend(schema.pk_cols().iter().map(|&c| asc(schema.locate(c as usize))));
    keys.extend(schema.payload_locators().into_iter().map(asc));
}

/// Lexicographic over `keys`.
#[inline]
pub fn cmp_order_keys<A: RowSource, B: RowSource>(
    keys: &[OrderLocator],
    a: &A,
    ra: usize,
    b: &B,
    rb: usize,
) -> Ordering {
    let na = a.get_null_word(ra);
    let nb = b.get_null_word(rb);
    for key in keys {
        let (xa, xb) = (key.loc.is_null_word(na), key.loc.is_null_word(nb));
        if xa != xb {
            return if xa == key.nulls_first {
                Ordering::Less
            } else {
                Ordering::Greater
            };
        }
        if xa {
            continue;
        }
        let mut ord = key.loc.cmp_non_null(a, ra, b, rb);
        if key.desc {
            ord = ord.reverse();
        }
        if ord != Ordering::Equal {
            return ord;
        }
    }
    Ordering::Equal
}

#[cfg(test)]
#[path = "tests/locator.rs"]
mod tests;
