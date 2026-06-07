//! Column type codes and the typed `TypeCode` enum.

#[allow(dead_code)]
pub mod type_code {
    pub const U8:     u8 = 1;
    pub const I8:     u8 = 2;
    pub const U16:    u8 = 3;
    pub const I16:    u8 = 4;
    pub const U32:    u8 = 5;
    pub const I32:    u8 = 6;
    pub const F32:    u8 = 7;
    pub const U64:    u8 = 8;
    pub const I64:    u8 = 9;
    pub const F64:    u8 = 10;
    pub const STRING: u8 = 11;
    pub const U128:   u8 = 12;
    pub const UUID:   u8 = 13;
    pub const BLOB:   u8 = 14;
}

/// Typed column type code enum, mirroring the `type_code::*` constants.
///
/// `#[repr(u8)]` — discriminants equal the corresponding `type_code::*` constant.
/// Stored as `u8` on disk (`SchemaColumn.type_code`); use `from_validated_u8` to
/// convert in-memory data that has already passed DDL validation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum TypeCode {
    U8     = type_code::U8,
    I8     = type_code::I8,
    U16    = type_code::U16,
    I16    = type_code::I16,
    U32    = type_code::U32,
    I32    = type_code::I32,
    F32    = type_code::F32,
    U64    = type_code::U64,
    I64    = type_code::I64,
    F64    = type_code::F64,
    String = type_code::STRING,
    U128   = type_code::U128,
    UUID   = type_code::UUID,
    Blob   = type_code::BLOB,
}

impl TypeCode {
    /// Convert a wire u8 that has already passed DDL validation. Panics on unknown codes.
    #[inline]
    pub fn from_validated_u8(v: u8) -> Self {
        Self::try_from_u8(v)
            .unwrap_or_else(|| panic!("invalid type_code {} in validated schema", v))
    }

    /// Convert a raw u8 wire value. Returns `None` for unknown codes.
    #[inline]
    pub fn try_from_u8(v: u8) -> Option<Self> {
        use type_code as tc;
        match v {
            tc::U8     => Some(TypeCode::U8),
            tc::I8     => Some(TypeCode::I8),
            tc::U16    => Some(TypeCode::U16),
            tc::I16    => Some(TypeCode::I16),
            tc::U32    => Some(TypeCode::U32),
            tc::I32    => Some(TypeCode::I32),
            tc::F32    => Some(TypeCode::F32),
            tc::U64    => Some(TypeCode::U64),
            tc::I64    => Some(TypeCode::I64),
            tc::F64    => Some(TypeCode::F64),
            tc::STRING => Some(TypeCode::String),
            tc::U128   => Some(TypeCode::U128),
            tc::UUID   => Some(TypeCode::UUID),
            tc::BLOB   => Some(TypeCode::Blob),
            _          => None,
        }
    }

    /// Byte stride of this type in a column payload.
    pub const fn stride(&self) -> u8 {
        match self {
            TypeCode::U8  | TypeCode::I8  => 1,
            TypeCode::U16 | TypeCode::I16 => 2,
            TypeCode::F32 | TypeCode::U32 | TypeCode::I32 => 4,
            TypeCode::F64 | TypeCode::U64 | TypeCode::I64 => 8,
            TypeCode::U128 | TypeCode::UUID | TypeCode::String | TypeCode::Blob => 16,
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

    /// Whether this type may be a PRIMARY KEY column. PK regions are compared
    /// as raw bytes, which is correct only for integer scalars: String/Blob
    /// carry out-of-line blob heaps that cannot be bulk-copied in the PK
    /// region, and IEEE-754 floats break the byte-equal contract (-0.0/+0.0
    /// compare unequal byte-wise but equal numerically, and NaN bit patterns
    /// have no single canonical form). Allow-list (not deny-list) so new
    /// variants are PK-ineligible until explicitly vetted.
    pub const fn is_pk_eligible(&self) -> bool {
        matches!(
            self,
            TypeCode::U8 | TypeCode::U16 | TypeCode::U32 | TypeCode::U64
                | TypeCode::U128 | TypeCode::UUID
                | TypeCode::I8 | TypeCode::I16 | TypeCode::I32 | TypeCode::I64,
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

/// Whether a raw wire type code uses the 16-byte German-string layout. u8-based
/// counterpart to [`TypeCode::is_german_string`] for callers holding a raw
/// `type_code` (mirrors the free `wire_stride`/`is_pk_eligible`). Unknown codes
/// are not german strings.
#[inline]
pub fn is_german_string(tc: u8) -> bool {
    tc == type_code::STRING || tc == type_code::BLOB
}

/// Whether a raw wire type code is a *signed* fixed-width integer
/// (I8/I16/I32/I64). The order-preserving encoders/comparators flip the sign
/// bit for these so two's-complement negatives sort below non-negatives.
/// Unsigned, float, string, and unknown codes are not signed.
pub const fn is_signed_int(tc: u8) -> bool {
    matches!(tc, type_code::I8 | type_code::I16 | type_code::I32 | type_code::I64)
}

/// Whether a raw wire type code is a fixed-width integer of ≤ 8 bytes
/// (U8/I8/U16/I16/U32/I32/U64/I64, any sign). Excludes U128/UUID (16 bytes) and
/// all float/string/blob types — these are exactly the payload columns the
/// fixed-int fast-path row comparator can compare via a single `u64` load.
pub const fn is_fixed_int(tc: u8) -> bool {
    matches!(
        tc,
        type_code::U8 | type_code::I8 | type_code::U16 | type_code::I16
            | type_code::U32 | type_code::I32 | type_code::U64 | type_code::I64
    )
}

/// u8-based counterpart to [`TypeCode::reindex_output_type`] for callers holding
/// a raw `type_code` (mirrors the free `is_fixed_int`/`wire_stride`). See that
/// method for the width policy and the engine ↔ planner lockstep it anchors.
pub const fn reindex_output_type_code(tc: u8) -> u8 {
    if is_fixed_int(tc) { tc } else { type_code::U128 }
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
        return Some(if wire_stride(wider) == 16 { type_code::U128 } else { wider });
    }
    // Cross-sign integer keys (U* vs I*) — deferred: need sign-class promotion,
    // and U64/U128-vs-signed need a signed-128 OPK type that does not exist yet.
    None
}

/// Final reindex slot type code for a key column: the carried promotion target
/// `T` when non-zero, else the per-column default policy. The single home of the
/// "carried-or-derive" rule, so the reindex Map's output schema and the
/// `ReindexPacker` cannot derive divergent slot widths.
#[inline]
pub const fn resolve_reindex_type(src_tc: u8, carried_tc: u8) -> u8 {
    if carried_tc != 0 { carried_tc } else { reindex_output_type_code(src_tc) }
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
    if reindex_output_type_code(src_tc) != common_tc { common_tc } else { 0 }
}

/// Whether a raw wire type code is any integer-valued fixed-width type that
/// `payload_route_key` can encode as an order-preserving routing key:
/// U8..U64, I8..I64, plus the 16-byte U128/UUID. The superset of `is_fixed_int`
/// that also admits the wide integer types. Floats, strings, and blobs route via
/// the content-hash path instead, so they are excluded.
pub const fn is_routable_int(tc: u8) -> bool {
    is_fixed_int(tc) || matches!(tc, type_code::U128 | type_code::UUID)
}

/// Wire stride (byte width) for a column type code.
/// Returns 8 for unknown codes (engine compare_rows depends on this default).
pub const fn wire_stride(tc: u8) -> usize {
    match tc {
        1 | 2           => 1,   // U8, I8
        3 | 4           => 2,   // U16, I16
        5..=7           => 4,   // U32, I32, F32
        8..=10          => 8,   // U64, I64, F64
        11..=14           => 16,  // STRING, U128, UUID, BLOB
        _                 => 8,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The reindex / `_join_pk` width policy (the single source of truth the
    /// engine compiler and SQL planner both derive from): a ≤8-byte integer key
    /// keeps its native width; STRING/BLOB, U128/UUID, and floats collapse to
    /// U128. Verified through both the `u8` free fn and the `TypeCode` method.
    #[test]
    fn reindex_output_type_policy() {
        let narrow = [
            TypeCode::U8, TypeCode::I8, TypeCode::U16, TypeCode::I16,
            TypeCode::U32, TypeCode::I32, TypeCode::U64, TypeCode::I64,
        ];
        for tc in narrow {
            assert_eq!(tc.reindex_output_type(), tc, "{tc:?} keeps native width");
            assert_eq!(reindex_output_type_code(tc as u8), tc as u8);
        }
        let wide = [
            TypeCode::U128, TypeCode::UUID, TypeCode::String,
            TypeCode::Blob, TypeCode::F32, TypeCode::F64,
        ];
        for tc in wide {
            assert_eq!(tc.reindex_output_type(), TypeCode::U128, "{tc:?} collapses to U128");
            assert_eq!(reindex_output_type_code(tc as u8), type_code::U128);
        }
    }

    /// Every accepted same-sign-class ladder pair promotes to the wider type
    /// (U128 for any 16-byte unsigned operand), and the result is symmetric.
    #[test]
    fn join_key_common_type_accepts_ladders() {
        use type_code::*;
        let cases: &[(u8, u8, u8)] = &[
            // signed ladder → wider signed
            (I8, I16, I16), (I8, I32, I32), (I8, I64, I64),
            (I16, I32, I32), (I16, I64, I64), (I32, I64, I64),
            // unsigned ladder → wider unsigned
            (U8, U16, U16), (U8, U32, U32), (U8, U64, U64),
            (U16, U32, U32), (U16, U64, U64), (U32, U64, U64),
            // unsigned with a 16-byte operand → U128
            (U32, U128, U128), (U64, U128, U128), (U32, UUID, U128),
            (U8, U128, U128),
            // same type → identity (fixed ints) / U128 (16-byte / string)
            (I32, I32, I32), (U64, U64, U64), (U128, U128, U128),
            (UUID, UUID, U128), (STRING, STRING, U128), (BLOB, BLOB, U128),
            // both german strings (different variants) → U128 content hash
            (STRING, BLOB, U128),
        ];
        for &(l, r, t) in cases {
            assert_eq!(join_key_common_type(l, r), Some(t),
                "common({l},{r}) should be {t}");
            assert_eq!(join_key_common_type(r, l), Some(t),
                "common is symmetric for ({l},{r})");
        }
    }

    /// Cross-sign integer pairs (and the no-existing-type U64=I64 case) are
    /// rejected pending signed-128 promotion. The one-sided string and float
    /// cases are screened out earlier (in the planner) and are not this fn's job.
    #[test]
    fn join_key_common_type_rejects_cross_sign() {
        use type_code::*;
        let reject: &[(u8, u8)] = &[
            (U8, I8), (U8, I16), (U32, I32), (U64, I64),
            (U64, I32), (I64, U32), (U128, I64), (UUID, I32),
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
        // Round-trip: resolve(src, carried(src, T)) == T for any valid promotion.
        for &(src, t) in &[
            (I8, I64), (I32, I32), (I32, I64), (U8, U64), (U32, U64),
            (U32, U128), (U64, U64), (STRING, U128), (U128, U128), (UUID, U128),
        ] {
            assert_eq!(resolve_reindex_type(src, carried_reindex_tc(src, t)), t,
                "round-trip failed for src={src} T={t}");
        }
    }
}
