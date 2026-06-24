//! Column type codes and the typed `TypeCode` enum.

#[allow(dead_code)]
pub mod type_code {
    pub const U8: u8 = 1;
    pub const I8: u8 = 2;
    pub const U16: u8 = 3;
    pub const I16: u8 = 4;
    pub const U32: u8 = 5;
    pub const I32: u8 = 6;
    pub const F32: u8 = 7;
    pub const U64: u8 = 8;
    pub const I64: u8 = 9;
    pub const F64: u8 = 10;
    pub const STRING: u8 = 11;
    pub const U128: u8 = 12;
    pub const UUID: u8 = 13;
    pub const BLOB: u8 = 14;
    pub const I128: u8 = 15;
}

/// Typed column type code enum, mirroring the `type_code::*` constants.
///
/// `#[repr(u8)]` — discriminants equal the corresponding `type_code::*` constant.
/// Stored as `u8` on disk (`SchemaColumn.type_code`); use `from_validated_u8` to
/// convert in-memory data that has already passed DDL validation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum TypeCode {
    U8 = type_code::U8,
    I8 = type_code::I8,
    U16 = type_code::U16,
    I16 = type_code::I16,
    U32 = type_code::U32,
    I32 = type_code::I32,
    F32 = type_code::F32,
    U64 = type_code::U64,
    I64 = type_code::I64,
    F64 = type_code::F64,
    String = type_code::STRING,
    U128 = type_code::U128,
    UUID = type_code::UUID,
    Blob = type_code::BLOB,
    I128 = type_code::I128,
}

impl TypeCode {
    /// Convert a wire u8 that has already passed DDL validation. Panics on unknown codes.
    #[inline]
    pub fn from_validated_u8(v: u8) -> Self {
        Self::try_from_u8(v).unwrap_or_else(|| panic!("invalid type_code {v} in validated schema"))
    }

    /// Convert a raw u8 wire value. Returns `None` for unknown codes.
    #[inline]
    pub const fn try_from_u8(v: u8) -> Option<Self> {
        use type_code as tc;
        match v {
            tc::U8 => Some(TypeCode::U8),
            tc::I8 => Some(TypeCode::I8),
            tc::U16 => Some(TypeCode::U16),
            tc::I16 => Some(TypeCode::I16),
            tc::U32 => Some(TypeCode::U32),
            tc::I32 => Some(TypeCode::I32),
            tc::F32 => Some(TypeCode::F32),
            tc::U64 => Some(TypeCode::U64),
            tc::I64 => Some(TypeCode::I64),
            tc::F64 => Some(TypeCode::F64),
            tc::STRING => Some(TypeCode::String),
            tc::U128 => Some(TypeCode::U128),
            tc::UUID => Some(TypeCode::UUID),
            tc::BLOB => Some(TypeCode::Blob),
            tc::I128 => Some(TypeCode::I128),
            _ => None,
        }
    }

    /// Byte stride of this type in a column payload.
    pub const fn stride(&self) -> u8 {
        match self {
            TypeCode::U8 | TypeCode::I8 => 1,
            TypeCode::U16 | TypeCode::I16 => 2,
            TypeCode::F32 | TypeCode::U32 | TypeCode::I32 => 4,
            TypeCode::F64 | TypeCode::U64 | TypeCode::I64 => 8,
            TypeCode::U128 | TypeCode::UUID | TypeCode::String | TypeCode::Blob | TypeCode::I128 => 16,
        }
    }

    pub const fn is_float(&self) -> bool {
        matches!(self, TypeCode::F32 | TypeCode::F64)
    }

    /// Whether this type uses the 16-byte "German string" layout (a 4-byte
    /// length, a 4-byte inline prefix, and an inline-or-out-of-line tail).
    /// STRING and BLOB share this representation; both must compare, relocate,
    /// and copy via the german-string paths (`compare_german_strings`, the blob
    /// heap), never via fixed-width byte ops. Allow-list so new variants are
    /// excluded until explicitly vetted.
    pub const fn is_german_string(&self) -> bool {
        matches!(self, TypeCode::String | TypeCode::Blob)
    }

    /// Whether this type is a signed integer (I8/I16/I32/I64/I128). Typed
    /// counterpart of the free [`is_signed_int`]: the order-preserving encoders
    /// flip the sign bit for these so two's-complement negatives sort below
    /// non-negatives. Unsigned, float, and string types are not signed.
    pub const fn is_signed_int(&self) -> bool {
        is_signed_int(*self as u8)
    }

    /// Whether this type may be a PRIMARY KEY column. PK regions are compared
    /// as raw bytes, which is correct only for integer scalars: String/Blob
    /// carry out-of-line blob heaps that cannot be bulk-copied in the PK
    /// region, and IEEE-754 floats break the byte-equal contract (-0.0/+0.0
    /// compare unequal byte-wise but equal numerically, and NaN bit patterns
    /// have no single canonical form). Allow-list (not deny-list) so new
    /// variants are PK-ineligible until explicitly vetted. Integer scalars are
    /// eligible: U8..U64, I8..I64, U128, UUID, I128 (the last produced only as a
    /// cross-sign equijoin `_join_pk`, never via DDL).
    pub const fn is_pk_eligible(&self) -> bool {
        matches!(
            self,
            TypeCode::U8
                | TypeCode::U16
                | TypeCode::U32
                | TypeCode::U64
                | TypeCode::U128
                | TypeCode::UUID
                | TypeCode::I8
                | TypeCode::I16
                | TypeCode::I32
                | TypeCode::I64
                | TypeCode::I128,
        )
    }

    /// Wire stride in bytes. Alias for `stride()` returning `usize`.
    #[inline]
    pub fn wire_stride(self) -> usize {
        self.stride() as usize
    }

    /// Output PK type for an equijoin synthetic reindex key built from a key
    /// column of this type: a ≤8-byte integer key keeps its native width (stride
    /// 8 for U64); everything wider or non-integer — U128/UUID, the STRING/BLOB
    /// 128-bit content hash, and PK-ineligible floats — collapses to the 16-byte
    /// U128 key. Single source of truth for the reindex / `_join_pk` PK width:
    /// the engine compiler (reindex Map output schema) and the SQL planner
    /// (`_join_pk` stamp) both derive their col-0 stride from this, and they MUST
    /// agree or every cross-process consumer re-derives a mismatched stride and
    /// the exchange wire decode hard-rejects ("WAL block PK region size mismatch").
    #[inline]
    pub fn reindex_output_type(self) -> TypeCode {
        TypeCode::from_validated_u8(reindex_output_type_code(self as u8))
    }

    /// Common reindex output type for an equijoin key pair, or `None` if the pair
    /// cannot co-partition under an existing type code. Typed counterpart of the
    /// free [`join_key_common_type`]; see it for the promotion ladder.
    #[inline]
    pub fn join_key_common_type(self, other: TypeCode) -> Option<TypeCode> {
        join_key_common_type(self as u8, other as u8).map(TypeCode::from_validated_u8)
    }

    /// The carried target tc to persist for this source column given its pair's
    /// resolved common type `common`. Typed counterpart of the free
    /// [`carried_reindex_tc`].
    #[inline]
    pub fn carried_reindex_tc(self, common: TypeCode) -> u8 {
        carried_reindex_tc(self as u8, common as u8)
    }
}

/// Whether a raw wire type code may be a PRIMARY KEY column. u8-based
/// counterpart to [`TypeCode::is_pk_eligible`] for callers holding a raw
/// `type_code` (mirrors the free `wire_stride`). Unknown codes are ineligible.
#[inline]
pub fn is_pk_eligible(tc: u8) -> bool {
    TypeCode::try_from_u8(tc).is_some_and(|t| t.is_pk_eligible())
}

/// Promote a base-table column's type to the leading-key type its secondary
/// index stores: an unsigned ≤8-byte integer (U8..U64) promotes to `U64`, a
/// signed ≤8-byte integer (I8..I64) to `I64`; `U128`/`UUID` keep their 16-byte
/// width; STRING/BLOB/float (and any unknown code) are index-ineligible and
/// return `Err`. Signed columns keep a *signed* promoted code so the OPK leading
/// key is order-preserving (`encode_pk_column` sign-flips only signed codes);
/// `wire_stride(I64) == wire_stride(U64) == 8`, so the index record's arity and
/// stride are identical to the old unsigned promotion. The single source of
/// truth for index-key promotion, shared by the engine's `make_index_schema` and
/// the SQL planner's CREATE INDEX limit pre-check so the nice SQL error and the
/// engine backstop can never disagree on a column's promoted width.
pub fn index_key_type(field_type_code: u8) -> Result<u8, String> {
    use type_code as tc;
    match field_type_code {
        tc::U128 => Ok(tc::U128),
        tc::UUID => Ok(tc::UUID),
        tc::U64 | tc::U32 | tc::U16 | tc::U8 => Ok(tc::U64),
        tc::I64 | tc::I32 | tc::I16 | tc::I8 => Ok(tc::I64),
        tc::F32 | tc::F64 | tc::STRING | tc::BLOB => Err(format!(
            "Secondary index on column type {field_type_code} not supported"
        )),
        _ => Err(format!("Unknown column type code: {field_type_code}")),
    }
}

/// Promote every indexed column type via [`index_key_type`] and validate the
/// resulting index-record layout. An index schema is
/// `(promoted_0, …, promoted_{n-1}, src_pk_0, …)` with every column in the PK,
/// so its PK arity is `n + src_pk_count` (capped by `MAX_PK_COLUMNS`) and its
/// PK stride is `Σ wire_stride(promoted_i) + src_pk_stride` (capped by
/// `MAX_PK_BYTES`). Returns the promoted type list. The single source of truth
/// shared by the SQL planner's CREATE INDEX pre-check and the engine's
/// `make_index_schema`, so the friendly planner error and the engine backstop
/// can never disagree on a column's promoted width or the limits.
pub fn index_key_types(col_types: &[u8], src_pk_count: usize, src_pk_stride: usize) -> Result<Vec<u8>, String> {
    let promoted: Vec<u8> = col_types.iter().map(|&t| index_key_type(t)).collect::<Result<_, _>>()?;
    let n = promoted.len();
    if n + src_pk_count > crate::MAX_PK_COLUMNS {
        return Err(format!(
            "index arity {n} + source PK arity {src_pk_count} exceeds the limit of {}",
            crate::MAX_PK_COLUMNS,
        ));
    }
    let stride: usize = promoted.iter().map(|&t| wire_stride(t)).sum::<usize>() + src_pk_stride;
    if stride > crate::MAX_PK_BYTES {
        return Err(format!(
            "index record stride {stride} exceeds the limit of {} bytes",
            crate::MAX_PK_BYTES,
        ));
    }
    Ok(promoted)
}

/// Whether a raw wire type code uses the 16-byte German-string layout. u8-based
/// counterpart to [`TypeCode::is_german_string`] for callers holding a raw
/// `type_code` (mirrors the free `wire_stride`/`is_pk_eligible`). Unknown codes
/// are not german strings.
#[inline]
pub fn is_german_string(tc: u8) -> bool {
    tc == type_code::STRING || tc == type_code::BLOB
}

/// Whether a raw wire type code is a *signed* fixed-width integer
/// (I8/I16/I32/I64/I128). The order-preserving encoders/comparators flip the
/// sign bit for these so two's-complement negatives sort below non-negatives.
/// Unsigned, float, string, and unknown codes are not signed.
pub const fn is_signed_int(tc: u8) -> bool {
    matches!(
        tc,
        type_code::I8 | type_code::I16 | type_code::I32 | type_code::I64 | type_code::I128
    )
}

/// Whether a raw wire type code is a fixed-width integer of ≤ 8 bytes
/// (U8/I8/U16/I16/U32/I32/U64/I64, any sign). Excludes U128/UUID (16 bytes) and
/// all float/string/blob types — these are exactly the payload columns the
/// fixed-int fast-path row comparator can compare via a single `u64` load.
pub const fn is_fixed_int(tc: u8) -> bool {
    matches!(
        tc,
        type_code::U8
            | type_code::I8
            | type_code::U16
            | type_code::I16
            | type_code::U32
            | type_code::I32
            | type_code::U64
            | type_code::I64
    )
}

/// A fixed-width integer column type — ≤ 8 bytes, any sign. This is the exact
/// domain on which "decode little-endian bytes → i64" is total. Construct via
/// `from_type_code`; *holding* a `FixedInt` is proof the column is a narrow
/// integer, so `decode_le_i64` needs no wildcard and cannot panic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FixedInt {
    U8,
    I8,
    U16,
    I16,
    U32,
    I32,
    U64,
    I64,
}

impl FixedInt {
    /// Exhaustive over `TypeCode` (no `_` arm): a new `TypeCode` variant is a
    /// compile error here until someone decides whether it is a narrow integer.
    pub const fn from_type_code(tc: TypeCode) -> Option<Self> {
        match tc {
            TypeCode::U8 => Some(Self::U8),
            TypeCode::I8 => Some(Self::I8),
            TypeCode::U16 => Some(Self::U16),
            TypeCode::I16 => Some(Self::I16),
            TypeCode::U32 => Some(Self::U32),
            TypeCode::I32 => Some(Self::I32),
            TypeCode::U64 => Some(Self::U64),
            TypeCode::I64 => Some(Self::I64),
            TypeCode::F32
            | TypeCode::F64
            | TypeCode::U128
            | TypeCode::UUID
            | TypeCode::String
            | TypeCode::Blob
            | TypeCode::I128 => None,
        }
    }

    /// Byte width (1/2/4/8).
    pub const fn width(self) -> usize {
        match self {
            Self::U8 | Self::I8 => 1,
            Self::U16 | Self::I16 => 2,
            Self::U32 | Self::I32 => 4,
            Self::U64 | Self::I64 => 8,
        }
    }

    /// The representable `(min, max)` of this integer type, widened to `i128`
    /// so one pair covers signed and unsigned variants. The SQL layer
    /// classifies literals against this — declining an out-of-range
    /// PK/equality literal, saturating an out-of-range range bound to the
    /// type-edge cut (`Cut::type_edges`) — instead of wrapping it into a
    /// different in-range value.
    pub const fn range(self) -> (i128, i128) {
        match self {
            Self::U8 => (0, u8::MAX as i128),
            Self::I8 => (i8::MIN as i128, i8::MAX as i128),
            Self::U16 => (0, u16::MAX as i128),
            Self::I16 => (i16::MIN as i128, i16::MAX as i128),
            Self::U32 => (0, u32::MAX as i128),
            Self::I32 => (i32::MIN as i128, i32::MAX as i128),
            Self::U64 => (0, u64::MAX as i128),
            Self::I64 => (i64::MIN as i128, i64::MAX as i128),
        }
    }

    /// Pack an in-range value (per [`Self::range`]) into the column's native
    /// LE `u128`: the low `width()` bytes are the value's native encoding —
    /// two's complement at native width for signed types, so `-1` on an `I8`
    /// column packs to `0xFF`, not `0xFFFF…` — and the rest stay zero. This is
    /// the convention every native PK/equality/range value on the wire uses
    /// (the SQL layer's literal parsing routes through here).
    pub const fn pack(self, v: i128) -> u128 {
        debug_assert!(self.range().0 <= v && v <= self.range().1);
        (v as u128) & (u128::MAX >> (128 - 8 * self.width()))
    }

    /// Decode the leading `width()` little-endian bytes of `b` as this integer,
    /// sign- or zero-extended into `i64`. Total: every arm is a real ≤8-byte
    /// integer with a pinned width, so `try_into` cannot fail.
    pub fn decode_le_i64(self, b: &[u8]) -> i64 {
        debug_assert!(b.len() >= self.width());
        match self {
            Self::U8 => b[0] as i64,
            Self::I8 => b[0] as i8 as i64,
            Self::U16 => u16::from_le_bytes(b[..2].try_into().unwrap()) as i64,
            Self::I16 => i16::from_le_bytes(b[..2].try_into().unwrap()) as i64,
            Self::U32 => u32::from_le_bytes(b[..4].try_into().unwrap()) as i64,
            Self::I32 => i32::from_le_bytes(b[..4].try_into().unwrap()) as i64,
            Self::U64 => u64::from_le_bytes(b[..8].try_into().unwrap()) as i64,
            Self::I64 => i64::from_le_bytes(b[..8].try_into().unwrap()),
        }
    }
}

/// u8-based counterpart to [`TypeCode::reindex_output_type`] for callers holding
/// a raw `type_code` (mirrors the free `is_fixed_int`/`wire_stride`). See that
/// method for the width policy and the engine ↔ planner lockstep it anchors.
pub const fn reindex_output_type_code(tc: u8) -> u8 {
    // ≤8-byte ints and the signed-128 join key keep their own width-and-sign slot;
    // every other wide/non-int type (U128/UUID, the STRING/BLOB hash, floats)
    // collapses to the unsigned 16-byte U128 key.
    if is_fixed_int(tc) || tc == type_code::I128 {
        tc
    } else {
        type_code::U128
    }
}

/// Common reindex output type code for an equijoin key pair, or `None` if the
/// pair cannot co-partition under an existing type code. Floats and one-sided
/// german-string pairs are rejected upstream (in `validate_join_key_pair`) and
/// never reach here. The returned code is the reindex OUTPUT type directly (the
/// promoted integer type with its true sign for ≤8-byte ints; U128 for the
/// unsigned-16B and german-string cases), so it is exactly what the slot type,
/// the `ColPromoter`, and the `_join_pk` stamp all need.
pub fn join_key_common_type(l: u8, r: u8) -> Option<u8> {
    // Equal types: the reindex output type (identity for fixed ints; U128 for
    // U128/UUID and STRING/BLOB content hashes). Keeps same-type joins
    // byte-identical to the pre-promotion behaviour.
    if l == r {
        return Some(reindex_output_type_code(l));
    }
    // Both german strings: a 16-byte XXH3 content hash (U128 slot). A one-sided
    // string pair is rejected in validate_join_key_pair and never reaches here.
    if is_german_string(l) && is_german_string(r) {
        return Some(type_code::U128);
    }
    // Both signed ≤8-byte integers → the wider signed type.
    if is_signed_int(l) && is_signed_int(r) {
        return Some(if wire_stride(l) >= wire_stride(r) { l } else { r });
    }
    // Both unsigned (U8..U64 and the 16-byte U128/UUID) → the wider unsigned
    // type; a 16-byte operand carries the pair to U128. `is_routable_int` is the
    // U8..U64 + U128/UUID set; minus the signed ones leaves the unsigned routables.
    let is_unsigned_int = |tc: u8| is_routable_int(tc) && !is_signed_int(tc);
    if is_unsigned_int(l) && is_unsigned_int(r) {
        let wider = if wire_stride(l) >= wire_stride(r) { l } else { r };
        return Some(if wire_stride(wider) == 16 {
            type_code::U128
        } else {
            wider
        });
    }
    // Cross-sign integer keys: one side signed, the other unsigned. (Equal,
    // both-signed, and both-unsigned pairs all returned above, so any remaining
    // routable-int pair is opposite-sign.) The common type must be a SIGNED type
    // (a) strictly wider than the unsigned operand — a signed type of equal width
    // cannot represent the unsigned operand's full range, so distinct values
    // would alias — and (b) at least as wide as the signed operand. The unsigned
    // side zero-extends and the signed side sign-extends into it
    // (`encode_pk_column_promoted`), so equal numeric values pack byte-identically.
    // wu ∈ {1,2,4,8,16}; only a U128/UUID unsigned operand (wu == 16) needs a
    // signed-256 type that does not exist → None.
    if is_routable_int(l) && is_routable_int(r) {
        let (s, u) = if is_signed_int(l) { (l, r) } else { (r, l) };
        let common_w = (wire_stride(u) * 2).max(wire_stride(s));
        return match common_w {
            2 => Some(type_code::I16),
            4 => Some(type_code::I32),
            8 => Some(type_code::I64),
            16 => Some(type_code::I128),
            // wu == 16 (U128/UUID) ⇒ common_w == 32: a signed-256 type, none exists.
            _ => None,
        };
    }
    None
}

/// Final reindex slot type code for a key column: the carried promotion target
/// `T` when non-zero, else the per-column default policy. The single home of the
/// "carried-or-derive" rule, so the reindex Map's output schema and the
/// `ReindexPacker` cannot derive divergent slot widths.
#[inline]
pub const fn resolve_reindex_type(src_tc: u8, carried_tc: u8) -> u8 {
    if carried_tc != 0 {
        carried_tc
    } else {
        reindex_output_type_code(src_tc)
    }
}

/// Inverse of [`resolve_reindex_type`]: the per-slot carried target tc to PERSIST
/// for a key column of source type `src_tc` whose pair resolved to common type
/// `common_tc`. Returns `0` ("derive from source") when the column already
/// self-derives to `common_tc` — keeping same-type / U128-vs-UUID / string
/// circuits byte-identical to the pre-promotion serialization — else the carried
/// `common_tc`. Co-located with `resolve_reindex_type` so the encode (planner)
/// and decode (engine) ends of the rule round-trip:
/// `resolve_reindex_type(src, carried_reindex_tc(src, t)) == t` for any `t` a key
/// of `src` can be promoted to (see the round-trip test).
#[inline]
pub const fn carried_reindex_tc(src_tc: u8, common_tc: u8) -> u8 {
    if reindex_output_type_code(src_tc) != common_tc {
        common_tc
    } else {
        0
    }
}

/// Whether a raw wire type code is any integer-valued fixed-width type that
/// `payload_route_key` can encode as an order-preserving routing key:
/// U8..U64, I8..I64, plus the 16-byte U128/UUID/I128. The superset of
/// `is_fixed_int` that also admits the wide integer types. Floats, strings, and
/// blobs route via the content-hash path instead, so they are excluded.
pub const fn is_routable_int(tc: u8) -> bool {
    is_fixed_int(tc) || matches!(tc, type_code::U128 | type_code::UUID | type_code::I128)
}

/// Wire stride (byte width) for a column type code. Delegates to the single
/// width source [`TypeCode::stride`]; unknown codes return 8 (load-bearing —
/// engine `compare_rows` and the trailing-slot fill depend on it).
pub const fn wire_stride(tc: u8) -> usize {
    match TypeCode::try_from_u8(tc) {
        Some(t) => t.stride() as usize,
        None => 8,
    }
}

// Pin the discriminant↔code round-trip so a `try_from_u8` typo (e.g. mapping a
// code to the wrong-discriminant variant) can no longer silently mis-stride a
// column via `wire_stride`. Width can no longer drift from `stride()` because
// that match is now the only width table.
const _: () = {
    let mut v: u16 = 0; // u16 so `v += 1` cannot overflow at 255
    while v <= 255 {
        if let Some(t) = TypeCode::try_from_u8(v as u8) {
            assert!(
                t as u8 == v as u8,
                "TypeCode discriminant must round-trip through try_from_u8"
            );
        }
        v += 1;
    }
    assert!(
        wire_stride(0) == 8 && wire_stride(16) == 8,
        "unknown-code default must be 8"
    );
};

#[cfg(test)]
mod tests {
    use super::*;

    /// Index-key promotion: unsigned ≤8-byte ints → U64, signed ≤8-byte ints →
    /// I64 (order-preserving signed leading key); U128/UUID keep width; STRING/
    /// BLOB/float (and unknown) are rejected. `wire_stride` of both U64 and I64 is
    /// 8, so the index record stride is unchanged by the signed promotion.
    #[test]
    fn index_key_type_promotion() {
        use type_code as tc;
        for t in [tc::U8, tc::U16, tc::U32, tc::U64] {
            assert_eq!(index_key_type(t).unwrap(), tc::U64, "{t} must promote to U64");
        }
        for t in [tc::I8, tc::I16, tc::I32, tc::I64] {
            assert_eq!(index_key_type(t).unwrap(), tc::I64, "{t} must promote to I64");
            assert_eq!(
                wire_stride(index_key_type(t).unwrap()),
                wire_stride(tc::U64),
                "signed promotion must keep the 8-byte width"
            );
        }
        assert_eq!(index_key_type(tc::U128).unwrap(), tc::U128);
        assert_eq!(index_key_type(tc::UUID).unwrap(), tc::UUID);
        for t in [tc::F32, tc::F64, tc::STRING, tc::BLOB] {
            assert!(index_key_type(t).is_err(), "{t} must be rejected");
        }
    }

    /// The reindex / `_join_pk` width policy (the single source of truth the
    /// engine compiler and SQL planner both derive from): a ≤8-byte integer key
    /// keeps its native width; STRING/BLOB, U128/UUID, and floats collapse to
    /// U128. Verified through both the `u8` free fn and the `TypeCode` method.
    #[test]
    fn reindex_output_type_policy() {
        let narrow = [
            TypeCode::U8,
            TypeCode::I8,
            TypeCode::U16,
            TypeCode::I16,
            TypeCode::U32,
            TypeCode::I32,
            TypeCode::U64,
            TypeCode::I64,
        ];
        for tc in narrow {
            assert_eq!(tc.reindex_output_type(), tc, "{tc:?} keeps native width");
            assert_eq!(reindex_output_type_code(tc as u8), tc as u8);
        }
        let wide = [
            TypeCode::U128,
            TypeCode::UUID,
            TypeCode::String,
            TypeCode::Blob,
            TypeCode::F32,
            TypeCode::F64,
        ];
        for tc in wide {
            assert_eq!(tc.reindex_output_type(), TypeCode::U128, "{tc:?} collapses to U128");
            assert_eq!(reindex_output_type_code(tc as u8), type_code::U128);
        }
        // I128 keeps width 16 WITH its sign (a signed-16 reindex slot), unlike the
        // unsigned 16-byte types that collapse to U128.
        assert_eq!(TypeCode::I128.reindex_output_type(), TypeCode::I128);
        assert_eq!(reindex_output_type_code(type_code::I128), type_code::I128);
    }

    /// Every accepted same-sign-class ladder pair promotes to the wider type
    /// (U128 for any 16-byte unsigned operand), and the result is symmetric.
    #[test]
    fn join_key_common_type_accepts_ladders() {
        use type_code::*;
        let cases: &[(u8, u8, u8)] = &[
            // signed ladder → wider signed
            (I8, I16, I16),
            (I8, I32, I32),
            (I8, I64, I64),
            (I16, I32, I32),
            (I16, I64, I64),
            (I32, I64, I64),
            // signed ladder reaching the new 16-byte signed type
            (I8, I128, I128),
            (I64, I128, I128),
            (I128, I128, I128),
            // unsigned ladder → wider unsigned
            (U8, U16, U16),
            (U8, U32, U32),
            (U8, U64, U64),
            (U16, U32, U32),
            (U16, U64, U64),
            (U32, U64, U64),
            // unsigned with a 16-byte operand → U128
            (U32, U128, U128),
            (U64, U128, U128),
            (U32, UUID, U128),
            (U8, U128, U128),
            // same type → identity (fixed ints) / U128 (16-byte / string)
            (I32, I32, I32),
            (U64, U64, U64),
            (U128, U128, U128),
            (UUID, UUID, U128),
            (STRING, STRING, U128),
            (BLOB, BLOB, U128),
            // both german strings (different variants) → U128 content hash
            (STRING, BLOB, U128),
        ];
        for &(l, r, t) in cases {
            assert_eq!(join_key_common_type(l, r), Some(t), "common({l},{r}) should be {t}");
            assert_eq!(join_key_common_type(r, l), Some(t), "common is symmetric for ({l},{r})");
        }
    }

    /// Cross-sign integer pairs whose unsigned operand is ≤ 8 bytes (`U64`) promote
    /// to the narrowest signed type that faithfully holds both ranges: strictly
    /// wider than the unsigned operand AND at least as wide as the signed operand. A
    /// `U64` unsigned side carries the pair to the new signed-128 type. Symmetric.
    #[test]
    fn join_key_common_type_accepts_cross_sign() {
        use type_code::*;
        let cases: &[(u8, u8, u8)] = &[
            (U8, I8, I16),
            (U8, I16, I16),
            (U8, I32, I32),
            (U8, I64, I64),
            (U16, I8, I32),
            (U16, I16, I32),
            (U16, I32, I32),
            (U16, I64, I64),
            (U32, I8, I64),
            (U32, I16, I64),
            (U32, I32, I64),
            (U32, I64, I64),
            // U64 unsigned side ⇒ the signed-128 common type.
            (U64, I8, I128),
            (U64, I16, I128),
            (U64, I32, I128),
            (U64, I64, I128),
        ];
        for &(u, s, t) in cases {
            assert_eq!(
                join_key_common_type(u, s),
                Some(t),
                "cross-sign common({u},{s}) should be {t}"
            );
            assert_eq!(
                join_key_common_type(s, u),
                Some(t),
                "cross-sign common is symmetric for ({u},{s})"
            );
        }
    }

    /// The surviving cross-sign reject contract: the unsigned operand is 128-bit
    /// (`U128`/`UUID`), whose faithful common type is a signed-256 type that does
    /// not exist. (A `U64` unsigned side now promotes to `I128` — see
    /// `join_key_common_type_accepts_cross_sign`.) The one-sided string and float
    /// cases are screened out earlier (in the planner) and are not this fn's job.
    #[test]
    fn join_key_common_type_rejects_wide_unsigned_cross_sign() {
        use type_code::*;
        let reject: &[(u8, u8)] = &[
            (U128, I8),
            (U128, I64),
            (UUID, I32),
            (UUID, I64),
            // a 128-bit unsigned side paired with the signed-128 type still has no
            // faithful (signed-256) common type.
            (U128, I128),
            (UUID, I128),
        ];
        for &(l, r) in reject {
            assert_eq!(join_key_common_type(l, r), None, "({l},{r}) must reject");
            assert_eq!(join_key_common_type(r, l), None, "({r},{l}) must reject");
        }
    }

    /// `resolve_reindex_type`: a non-zero carried `T` wins; `0` falls back to the
    /// per-column default policy.
    #[test]
    fn resolve_reindex_type_carried_or_derive() {
        use type_code::*;
        // Carried T wins.
        assert_eq!(resolve_reindex_type(I32, I64), I64);
        assert_eq!(resolve_reindex_type(U8, U64), U64);
        // 0 = derive from source policy.
        assert_eq!(resolve_reindex_type(I32, 0), I32);
        assert_eq!(resolve_reindex_type(STRING, 0), U128);
        assert_eq!(resolve_reindex_type(F64, 0), U128);
    }

    /// `carried_reindex_tc` is the inverse of `resolve_reindex_type`: a self-
    /// deriving slot collapses to `0` (byte-compat), a cross-width slot carries
    /// `T`, and the two always round-trip.
    #[test]
    fn carried_reindex_tc_round_trips_with_resolve() {
        use type_code::*;
        // Self-deriving slots collapse to 0 — the legacy / byte-identical path.
        assert_eq!(carried_reindex_tc(I32, I32), 0);
        assert_eq!(carried_reindex_tc(U64, U64), 0);
        assert_eq!(carried_reindex_tc(STRING, U128), 0);
        assert_eq!(carried_reindex_tc(U128, U128), 0);
        assert_eq!(carried_reindex_tc(UUID, U128), 0);
        // Cross-width slots carry the promoted target.
        assert_eq!(carried_reindex_tc(I32, I64), I64);
        assert_eq!(carried_reindex_tc(U8, U64), U64);
        assert_eq!(carried_reindex_tc(U32, U128), U128);
        // Round-trip: resolve(src, carried(src, T)) == T for any valid promotion,
        // including cross-sign promotions where the source promotes to a wider
        // signed type (e.g. the unsigned U8/U16/U32 sides and a same-sign signed
        // side both landing on a signed T).
        for &(src, t) in &[
            (I8, I64),
            (I32, I32),
            (I32, I64),
            (U8, U64),
            (U32, U64),
            (U32, U128),
            (U64, U64),
            (STRING, U128),
            (U128, U128),
            (UUID, U128),
            (U8, I16),
            (U16, I32),
            (U32, I64),
            (I32, I64),
            // cross-sign and same-sign promotions reaching the signed-128 slot:
            // the unsigned U64 side and every signed side land on I128.
            (U64, I128),
            (I8, I128),
            (I16, I128),
            (I32, I128),
            (I64, I128),
        ] {
            let carried = carried_reindex_tc(src, t);
            assert_eq!(
                resolve_reindex_type(src, carried),
                t,
                "round-trip failed for src={src} T={t}"
            );
            // Idempotency of promotion: a *carried* (non-zero) target re-derives to
            // itself through join_key_common_type. The engine compiler relies on
            // exactly this to validate a carried `_join_pk` slot against the planner
            // without re-implementing the sign/width ladder — so it must hold for
            // every promotion a source can carry. (Self-deriving slots carry 0 and
            // are validated by the per-column default policy, not this rule.)
            if carried != 0 {
                assert_eq!(
                    join_key_common_type(src, t),
                    Some(t),
                    "promotion not idempotent for src={src} T={t}: \
                     compiler carried-slot guard would diverge from the planner"
                );
            }
        }
    }

    #[test]
    fn fixed_int_predicate_matches_witness() {
        use TypeCode::*;
        for tc in [
            U8, I8, U16, I16, U32, I32, F32, U64, I64, F64, String, U128, UUID, Blob, I128,
        ] {
            assert_eq!(
                is_fixed_int(tc as u8),
                FixedInt::from_type_code(tc).is_some(),
                "mismatch for {tc:?}"
            );
        }
    }

    #[test]
    fn signed_int_predicate_classifies_and_method_agrees() {
        use TypeCode::*;
        let signed = [I8, I16, I32, I64, I128];
        for tc in [
            U8, I8, U16, I16, U32, I32, F32, U64, I64, F64, String, U128, UUID, Blob, I128,
        ] {
            let want = signed.contains(&tc);
            assert_eq!(is_signed_int(tc as u8), want, "free fn mismatch for {tc:?}");
            assert_eq!(tc.is_signed_int(), want, "method mismatch for {tc:?}");
        }
    }

    #[test]
    fn fixed_int_from_type_code_coverage() {
        assert!(FixedInt::from_type_code(TypeCode::U8).is_some());
        assert!(FixedInt::from_type_code(TypeCode::I8).is_some());
        assert!(FixedInt::from_type_code(TypeCode::U16).is_some());
        assert!(FixedInt::from_type_code(TypeCode::I16).is_some());
        assert!(FixedInt::from_type_code(TypeCode::U32).is_some());
        assert!(FixedInt::from_type_code(TypeCode::I32).is_some());
        assert!(FixedInt::from_type_code(TypeCode::U64).is_some());
        assert!(FixedInt::from_type_code(TypeCode::I64).is_some());
        for tc in [
            TypeCode::F32,
            TypeCode::F64,
            TypeCode::U128,
            TypeCode::UUID,
            TypeCode::String,
            TypeCode::Blob,
            TypeCode::I128,
        ] {
            assert!(FixedInt::from_type_code(tc).is_none(), "{tc:?} should be None");
        }
    }

    #[test]
    fn fixed_int_decode_le_i64_round_trips() {
        assert_eq!(FixedInt::U8.decode_le_i64(&[0xff]), 255i64);
        assert_eq!(FixedInt::I8.decode_le_i64(&[0xff]), -1i64);
        assert_eq!(FixedInt::U16.decode_le_i64(&[0xff, 0x00]), 255i64);
        assert_eq!(FixedInt::I16.decode_le_i64(&[0x00, 0x80]), i16::MIN as i64);
        assert_eq!(FixedInt::U32.decode_le_i64(&[0xff, 0xff, 0xff, 0xff]), u32::MAX as i64);
        assert_eq!(FixedInt::I32.decode_le_i64(&[0x00, 0x00, 0x00, 0x80]), i32::MIN as i64);
        assert_eq!(FixedInt::U64.decode_le_i64(&u64::MAX.to_le_bytes()), -1i64);
        assert_eq!(FixedInt::I64.decode_le_i64(&i64::MIN.to_le_bytes()), i64::MIN);
    }
}
