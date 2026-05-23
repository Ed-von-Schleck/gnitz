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
}

/// Whether a raw wire type code may be a PRIMARY KEY column. u8-based
/// counterpart to [`TypeCode::is_pk_eligible`] for callers holding a raw
/// `type_code` (mirrors the free `wire_stride`). Unknown codes are ineligible.
#[inline]
pub fn is_pk_eligible(tc: u8) -> bool {
    TypeCode::try_from_u8(tc).is_some_and(|t| t.is_pk_eligible())
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
