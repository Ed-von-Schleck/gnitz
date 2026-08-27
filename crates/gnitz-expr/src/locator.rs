//! Resolved column addressing: where a logical column physically lives in a
//! row, and the canonical `u128` keys derived from it.
//!
//! Every method here is `#[inline(always)]`: the per-row callers live in
//! gnitz-engine, which builds at opt-level 0 in dev, where only the
//! always-inline pass runs.

use std::cmp::Ordering;

use crate::RowSource;

/// Where a logical column's value physically lives in a row, resolved once from
/// the schema. The only sanctioned way to read a column whose index is not
/// statically known to be a payload column: it cannot silently treat a PK
/// column as payload.
/// A 4-byte `Copy` value (three `u8` fields + a 1-byte tag). The coordinates
/// match the schema's own widths — a PK byte offset and a payload slot both fit
/// a `u8`, and every fixed-width column is ≤ 16 bytes — so `locate` stores them
/// without widening and a `Vec<ColumnLocator>` (group-key / emit columns) stays
/// dense. The two width claims are enforced below rather than asserted here.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ColumnLocator {
    /// PK column: the value lives in the PK region at `byte_off`, width `size`,
    /// type `type_code`. PK columns are non-nullable, and the region is OPK at
    /// every [`RowSource`], which is what the OPK-inverting readers below undo.
    Pk { byte_off: u8, size: u8, type_code: u8 },
    /// Payload column: value is native-LE in dense payload slot `slot` (also its
    /// null-bitmap bit position), width `size`, type `type_code`.
    Payload { slot: u8, size: u8, type_code: u8 },
}

const _: () = assert!(
    std::mem::size_of::<ColumnLocator>() <= 8,
    "ColumnLocator must stay packed; a usize coordinate would balloon it to 24 bytes",
);
// The `u8` coordinates above only address the whole schema while these hold.
// `MAX_PK_BYTES` is derived (`MAX_PK_COLUMNS * 16`), so it can move without
// anyone editing this file.
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
    pub fn type_code(&self) -> u8 {
        match *self {
            ColumnLocator::Pk { type_code, .. } | ColumnLocator::Payload { type_code, .. } => type_code,
        }
    }

    /// True iff this column is NULL in `row`. PK columns are never null, and
    /// short-circuit *before* the null-word load — per-row callers hitting that
    /// arm must not pay a load for a value they never read.
    #[inline(always)]
    pub fn is_null(&self, mb: &impl RowSource, row: usize) -> bool {
        match *self {
            ColumnLocator::Pk { .. } => false,
            ColumnLocator::Payload { .. } => self.is_null_word(mb.get_null_word(row)),
        }
    }

    /// [`Self::is_null`] against an already-read null word, for the callers that
    /// hold one: a row loop reading several columns, or one that needs the word
    /// for a group fold anyway.
    #[inline(always)]
    pub fn is_null_word(&self, null_word: u64) -> bool {
        match *self {
            ColumnLocator::Pk { .. } => false,
            ColumnLocator::Payload { slot, .. } => gnitz_wire::null_word_get(null_word, slot as usize),
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

    /// Order two rows on this column, **both known non-NULL** — the caller keeps
    /// its own NULL policy, direction and tiebreak. A PK window is OPK, so plain
    /// byte order *is* its typed order (an LE decode would invert it); a payload
    /// window is native LE and goes through the typed dispatch, which is also
    /// what routes STRING/BLOB to content comparison.
    ///
    /// The two sources are separate types so a cursor-vs-exemplar compare and an
    /// intra-batch argsort share one body.
    #[inline(always)]
    pub fn cmp_non_null<A: RowSource, B: RowSource>(&self, a: &A, ra: usize, b: &B, rb: usize) -> Ordering {
        match *self {
            ColumnLocator::Pk { .. } => self.bytes(a, ra).cmp(self.bytes(b, rb)),
            ColumnLocator::Payload { type_code, .. } => {
                gnitz_wire::cmp_col_window(self.bytes(a, ra), a.blob(), self.bytes(b, rb), b.blob(), type_code)
            }
        }
    }

    /// Encode this column's value in `row` as the OPK image of `out_tc`, into
    /// `dst` (exactly `out_tc`'s width). For an **integer** source: order-
    /// preserving, sign-correct and equality-correct at any source width, and the
    /// identity copy when `out_tc` already equals this column's type. A **float**
    /// source is equality-correct only: `widen_native_le` sign-extends on
    /// `is_signed_int`, false for F32/F64, so `-1.0` encodes above `1.0`.
    /// Callers must [`Self::is_null`]-gate a nullable payload column first.
    ///
    /// One method rather than the same dispatch at each site because index-key
    /// projection and join-key repartitioning must emit byte-identical keys for
    /// one logical value.
    #[inline(always)]
    pub fn encode_opk_promoted(&self, mb: &impl RowSource, row: usize, out_tc: u8, dst: &mut [u8]) {
        let src = self.bytes(mb, row);
        match *self {
            ColumnLocator::Pk { type_code, .. } => gnitz_wire::promote_opk_column(src, type_code, out_tc, dst),
            ColumnLocator::Payload { type_code, .. } => {
                gnitz_wire::encode_pk_column_promoted(src, type_code, out_tc, dst)
            }
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
    /// `worker_for_pk_bytes` compares on, and the routing counterpart to
    /// [`Self::native_key`]. A PK column widens its OPK
    /// bytes; a payload column OPK-encodes then widens, so equal logical values
    /// route to the same worker whether stored as a PK or a payload column.
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
