//! Resolved column addressing: where a logical column physically lives in a
//! row, and the canonical `u128` keys derived from it.
//!
//! Every method here is `#[inline(always)]`, per the crate-root inlining rule —
//! all seven, not just the one-liners. `native_key`, `route_key` and
//! `native_le_bytes` do no work of their own either: each is a two-arm match
//! whose arms are a single `gnitz_wire` call, so a plain `#[inline]` would cost
//! every per-row caller two frames where one would do.

use crate::RowSource;

/// The dense payload-slot byte that means "this column has no payload slot" —
/// it is a PK column. `u8::MAX`, not 0, so it is unambiguous against a real
/// payload index of 0, and out of range for every schema (a payload slot is
/// below `MAX_COLUMNS`), so anything that addresses a column with it trips a
/// bounds check rather than silently reading slot 0.
///
/// Two users, both relying on that out-of-range property: the engine schema's
/// `payload_mapping[ci]` table stores it for a PK column, and `resolve` writes
/// it into a payload-only opcode whose column operand is a PK — reachable only
/// for an unvalidated program, i.e. tests, since every constructor validates
/// first. Neither hands it onward as a *value*: reads go through
/// [`SchemaFacts::payload_slot`](crate::SchemaFacts::payload_slot), which is
/// `Option`-shaped.
pub const PAYLOAD_MAPPING_PK_SENTINEL: u8 = u8::MAX;

/// Where a logical column's value physically lives in a row, resolved once from
/// the schema. The only sanctioned way to read a column whose index is not
/// statically known to be a payload column: it cannot silently treat a PK
/// column as payload (the corruption `payload_idx`'s sentinel return invited).
/// A 4-byte `Copy` value (three `u8` fields + a 1-byte tag). The coordinates
/// match the schema's own widths — a PK byte offset is `u8` (PK stride ≤
/// `MAX_PK_BYTES` = 80), payload slots are `u8` (< `MAX_COLUMNS` = 65, so ≤ 63
/// with at least one PK column), and every fixed-width column is ≤ 16 bytes —
/// so `locate` stores them without widening and a `Vec<ColumnLocator>`
/// (group-key / emit columns) stays dense.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ColumnLocator {
    /// PK column: value is OPK-at-rest in the PK region at `byte_off`, width
    /// `size`, type `type_code`. PK columns are non-nullable.
    Pk { byte_off: u8, size: u8, type_code: u8 },
    /// Payload column: value is native-LE in dense payload slot `slot` (also its
    /// null-bitmap bit position), width `size`, type `type_code`.
    Payload { slot: u8, size: u8, type_code: u8 },
}

const _: () = assert!(
    std::mem::size_of::<ColumnLocator>() <= 8,
    "ColumnLocator must stay packed; a usize coordinate would balloon it to 24 bytes",
);

impl ColumnLocator {
    #[inline(always)]
    pub fn size(&self) -> usize {
        match *self {
            ColumnLocator::Pk { size, .. } | ColumnLocator::Payload { size, .. } => size as usize,
        }
    }

    #[inline(always)]
    pub fn type_code(&self) -> u8 {
        match *self {
            ColumnLocator::Pk { type_code, .. } | ColumnLocator::Payload { type_code, .. } => type_code,
        }
    }

    /// True iff this column is NULL in `row`. PK columns are never null.
    #[inline(always)]
    pub fn is_null(&self, mb: &impl RowSource, row: usize) -> bool {
        match *self {
            ColumnLocator::Pk { .. } => false,
            ColumnLocator::Payload { slot, .. } => gnitz_wire::null_word_get(mb.get_null_word(row), slot as usize),
        }
    }

    /// Raw at-rest bytes of the column in `row`: OPK/big-endian for a PK column,
    /// native little-endian for a payload column. For hashing, group keys, and
    /// verbatim copies. On a STRING/BLOB column these are the 16-byte German-string
    /// struct (a blob heap offset for long strings), not the content — content
    /// callers resolve through the blob arena. The returned slice borrows the
    /// batch (`'b`), not the `&self` receiver, so it stays valid after the locator
    /// borrow is dropped (matching [`RowSource::get_pk_bytes`]/
    /// [`RowSource::get_col_ptr`], whose returns are tied to the batch reference).
    /// The batch lifetime must be named: under elision `&self` would capture the
    /// return and [`Self::native_le_bytes`]'s `Payload` arm — which hands this
    /// slice back as `&'a [u8]` — would stop compiling.
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

    /// Native little-endian value bytes of the column in `row`: a payload
    /// column verbatim, a PK column OPK-decoded into `scratch` (undoing the
    /// big-endian sign-flipped at-rest form). The value-reading counterpart to
    /// [`Self::bytes`] — every consumer that interprets a column's *value*
    /// (aggregation, order-encoding, exemplar copies) must read through here so
    /// a PK-source column can never be consumed in its at-rest byte order.
    #[inline(always)]
    pub fn native_le_bytes<'a, 'b: 'a>(
        &self,
        mb: &'b impl RowSource,
        row: usize,
        scratch: &'a mut [u8; 16],
    ) -> &'a [u8] {
        match *self {
            ColumnLocator::Pk { size, type_code, .. } => {
                *scratch = gnitz_wire::decode_pk_column_owned(self.bytes(mb, row), type_code);
                &scratch[..size as usize]
            }
            ColumnLocator::Payload { .. } => self.bytes(mb, row),
        }
    }

    /// The ≤8-byte integer value in `row`, widened to `i64` under `fi`'s
    /// signedness. The fused form of [`Self::native_le_bytes`] followed by
    /// `FixedInt::decode_le_i64`, and the one place either kind of column becomes
    /// an integer: a PK column goes through the OPK inverse without materializing
    /// its native image first, a payload column reads verbatim.
    ///
    /// `fi` must be the column's own type (`FixedInt::from_type_code(type_code())`);
    /// the width assert inside `decode_opk_i64` is what catches a caller that
    /// pairs a locator with someone else's.
    #[inline(always)]
    pub fn decode_i64(&self, mb: &impl RowSource, row: usize, fi: gnitz_wire::FixedInt) -> i64 {
        match *self {
            ColumnLocator::Pk { .. } => gnitz_wire::decode_opk_i64(self.bytes(mb, row), fi),
            ColumnLocator::Payload { .. } => fi.decode_le_i64(self.bytes(mb, row)),
        }
    }

    /// Canonical native u128 key for the value in `row` (sign-aware; the form
    /// `has_pk` and the index seeks compare on). Callers must `is_null`-gate a
    /// nullable payload column first; a PK column is never null.
    #[inline(always)]
    pub fn native_key(&self, mb: &impl RowSource, row: usize) -> u128 {
        match *self {
            ColumnLocator::Pk {
                byte_off,
                size,
                type_code,
            } => gnitz_wire::pk_native_key(mb.get_pk_bytes(row), byte_off as usize, size as usize, type_code),
            ColumnLocator::Payload { slot, size, type_code } => gnitz_wire::payload_native_key(
                mb.get_col_ptr(row, slot as usize, size as usize),
                0,
                size as usize,
                type_code,
            ),
        }
    }

    /// Canonical sign-aware *routing* key for the value in `row` — the form
    /// `partition_for_pk_bytes` compares on, and the routing counterpart to
    /// [`Self::native_key`]. A PK column widens its OPK
    /// bytes; a payload column OPK-encodes then widens, so equal logical values
    /// route to the same partition whether stored as a PK or a payload column.
    /// Callers must `is_null`-gate first. STRING/BLOB have no order-preserving
    /// routing image (this returns `payload_route_key`'s raw low-8-byte image for
    /// them); a caller routing by string content hashes it before reaching here.
    #[inline(always)]
    pub fn route_key(&self, mb: &impl RowSource, row: usize) -> u128 {
        match *self {
            ColumnLocator::Pk { byte_off, size, .. } => {
                gnitz_wire::pk_route_key(mb.get_pk_bytes(row), byte_off as usize, size as usize)
            }
            ColumnLocator::Payload { slot, size, type_code } => gnitz_wire::payload_route_key(
                mb.get_col_ptr(row, slot as usize, size as usize),
                0,
                size as usize,
                type_code,
            ),
        }
    }
}

#[cfg(test)]
mod tests;
