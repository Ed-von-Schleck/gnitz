//! Column type codes and the typed `TypeCode` enum.

use core::cmp::Ordering;

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
    /// Every variant, in wire-code order — the one enumeration of the type
    /// table. Clients that must reproduce the table (the Python `TypeCode`
    /// IntEnum) build it from here rather than re-typing the constants, so a
    /// new variant reaches them without an edit on their side.
    pub const ALL: [TypeCode; 15] = [
        TypeCode::U8,
        TypeCode::I8,
        TypeCode::U16,
        TypeCode::I16,
        TypeCode::U32,
        TypeCode::I32,
        TypeCode::F32,
        TypeCode::U64,
        TypeCode::I64,
        TypeCode::F64,
        TypeCode::String,
        TypeCode::U128,
        TypeCode::UUID,
        TypeCode::Blob,
        TypeCode::I128,
    ];

    /// The type's name in the wire vocabulary — the spelling the
    /// `type_code::*` constants use, which is what the client bindings expose.
    /// Exhaustive on purpose: a new variant fails to compile until named.
    pub const fn wire_name(self) -> &'static str {
        match self {
            TypeCode::U8 => "U8",
            TypeCode::I8 => "I8",
            TypeCode::U16 => "U16",
            TypeCode::I16 => "I16",
            TypeCode::U32 => "U32",
            TypeCode::I32 => "I32",
            TypeCode::F32 => "F32",
            TypeCode::U64 => "U64",
            TypeCode::I64 => "I64",
            TypeCode::F64 => "F64",
            TypeCode::String => "STRING",
            TypeCode::U128 => "U128",
            TypeCode::UUID => "UUID",
            TypeCode::Blob => "BLOB",
            TypeCode::I128 => "I128",
        }
    }

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

    /// Whether this type is one of the two IEEE-754 column types. Typed
    /// counterpart of the free [`is_float`].
    pub const fn is_float(self) -> bool {
        is_float(self as u8)
    }

    /// The type of the 8-byte register image the engine materializes for a
    /// computed value of this source type. Typed counterpart of the free
    /// [`register_image_type`].
    #[inline]
    pub fn register_image(self) -> TypeCode {
        TypeCode::from_validated_u8(register_image_type(self as u8))
    }

    /// The 16-byte integer-ish types (U128, UUID, I128) with no i64 slot in the
    /// expression VM: a bound on such a column cannot be re-imposed by a
    /// compiled predicate, so a range walk over it must be byte-exact
    /// (un-gated). The SQL layer reads this when deciding which conjuncts it may
    /// strip; it then ships the verdict as the `exact` bit on the bound.
    pub const fn is_wide_int(self) -> bool {
        is_wide_int(self as u8)
    }

    /// Whether this type uses the 16-byte "German string" layout (a 4-byte
    /// length, a 4-byte inline prefix, and an inline-or-out-of-line tail).
    /// STRING and BLOB share this representation; both must compare, relocate,
    /// and copy via the german-string paths (`compare_german_strings`, the blob
    /// heap), never via fixed-width byte ops. Allow-list so new variants are
    /// excluded until explicitly vetted.
    pub const fn is_german_string(self) -> bool {
        is_german_string(self as u8)
    }

    /// Whether this type is a signed integer (I8/I16/I32/I64/I128). Typed
    /// counterpart of the free [`is_signed_int`]: the order-preserving encoders
    /// flip the sign bit for these so two's-complement negatives sort below
    /// non-negatives. Unsigned, float, and string types are not signed.
    pub const fn is_signed_int(self) -> bool {
        is_signed_int(self as u8)
    }

    /// Typed counterpart of the free [`is_pk_eligible`], which owns the rule.
    pub const fn is_pk_eligible(self) -> bool {
        is_pk_eligible(self as u8)
    }

    /// Whether a single non-nullable column of this type may serve as a reduce's
    /// output primary key directly, rather than a synthetic U128 group fold —
    /// the type half of [`crate::ReduceOutKey::for_group_cols`]'s
    /// `SingleNaturalCol` predicate. Only the natural key-width unsigned types
    /// qualify.
    #[inline]
    pub(crate) const fn is_natural_reduce_key(self) -> bool {
        matches!(self, TypeCode::U64 | TypeCode::U128 | TypeCode::UUID)
    }

    /// Byte stride (width) of this type in a column payload. The single width
    /// table for the enum; the free [`wire_stride`] delegates here.
    #[inline(always)]
    pub const fn wire_stride(self) -> usize {
        match self {
            TypeCode::U8 | TypeCode::I8 => 1,
            TypeCode::U16 | TypeCode::I16 => 2,
            TypeCode::F32 | TypeCode::U32 | TypeCode::I32 => 4,
            TypeCode::F64 | TypeCode::U64 | TypeCode::I64 => 8,
            TypeCode::U128 | TypeCode::UUID | TypeCode::String | TypeCode::Blob | TypeCode::I128 => 16,
        }
    }

    /// Output PK type for an equijoin synthetic reindex key built from a key
    /// column of this type: a ≤8-byte integer key keeps its native width (stride
    /// 8 for U64); everything wider or non-integer — U128/UUID, the STRING/BLOB
    /// 128-bit content hash, and PK-ineligible floats — collapses to the 16-byte
    /// U128 key. Single source of truth for the reindex / `_join_pk` PK width:
    /// the engine compiler (reindex Map output schema) and the SQL planner
    /// (`_join_pk` stamp) both derive their col-0 stride from this, and they MUST
    /// agree or every cross-process consumer re-derives a mismatched stride and
    /// the exchange wire decode hard-rejects the block.
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

    /// Inverse of [`resolve_reindex_type`]: the target to persist for a key
    /// column of this source type whose pair resolved to `common`, or `None`
    /// where this column already self-derives to it.
    #[inline]
    pub fn carried_reindex_tc(self, common: TypeCode) -> Option<TypeCode> {
        (reindex_output_type_code(self as u8) != common as u8).then_some(common)
    }
}

/// Compare two equal-length little-endian byte windows of a fixed-width column
/// under the given raw `u8` type code: unsigned magnitude for U8–U64/U128/UUID,
/// signed two's-complement for I8–I64/I128 (the `_ =>` default, covering any
/// unknown code), and `total_cmp` for F32/F64. STRING/BLOB are not handled here
/// — callers must dispatch German strings to content comparison first; a
/// mis-routed 16-byte string window hits the width `unreachable!` rather than
/// silently mis-comparing.
#[inline]
pub fn cmp_typed_le(a: &[u8], b: &[u8], tc: u8) -> Ordering {
    // Deliberately not `debug_assert_eq!`: that takes both lengths by reference
    // and spills them, on a per-row path.
    debug_assert!(a.len() == b.len(), "cmp_typed_le: windows must be equal length");
    // Pin `b` to `a`'s width once. Without it every arm below dispatches on
    // `b.len()` a second time, since the debug assert above is gone in release.
    let b = &b[..a.len()];
    match tc {
        type_code::U128 | type_code::UUID => {
            u128::from_le_bytes(a.try_into().unwrap()).cmp(&u128::from_le_bytes(b.try_into().unwrap()))
        }
        type_code::I128 => i128::from_le_bytes(a.try_into().unwrap()).cmp(&i128::from_le_bytes(b.try_into().unwrap())),
        type_code::F64 => {
            f64::from_le_bytes(a.try_into().unwrap()).total_cmp(&f64::from_le_bytes(b.try_into().unwrap()))
        }
        type_code::F32 => {
            f32::from_le_bytes(a.try_into().unwrap()).total_cmp(&f32::from_le_bytes(b.try_into().unwrap()))
        }
        // The windows ARE the columns (equal length, asserted above), so the
        // exact form applies and no sub-slice bound is paid per comparison.
        type_code::U8 | type_code::U16 | type_code::U32 | type_code::U64 => {
            crate::read_unsigned_exact(a).cmp(&crate::read_unsigned_exact(b))
        }
        _ => crate::read_signed_exact(a).cmp(&crate::read_signed_exact(b)), // I8/I16/I32/I64
    }
}

/// Compare two equal-width column windows of the given raw `u8` type code:
/// German strings (STRING/BLOB) by content through their backing blob arenas,
/// every fixed-width type through [`cmp_typed_le`]. The blob slices back each
/// side's heap payload, and are ignored for non-string columns.
///
/// The single home for "STRING and BLOB share the 16-byte layout, so they must
/// be compared by content before the fixed-width dispatch" — a missed site would
/// mis-order a BLOB key. In `gnitz-wire` because the client-side comparators are
/// held to the same order as the engine's.
///
/// `#[inline(always)]` for the reason [`crate::promote_opk_column`] carries it:
/// this is one payload comparison of a sort, monomorphised into crates that
/// build at opt-level 0, where a plain hint inlines nothing.
#[inline(always)]
pub fn cmp_col_window(a: &[u8], a_blob: &[u8], b: &[u8], b_blob: &[u8], type_code: u8) -> Ordering {
    if is_german_string(type_code) {
        crate::compare_german_strings(a, a_blob, b, b_blob)
    } else {
        cmp_typed_le(a, b, type_code)
    }
}

/// Whether a raw wire type code may be a PRIMARY KEY column: the integer
/// scalars of every width, and nothing else. A PK region is compared as raw
/// bytes, which String/Blob heap offsets and IEEE-754 floats (±0.0 differ
/// byte-wise but compare equal) do not survive. Both operands are allow-lists,
/// so an unknown code is ineligible.
#[inline(always)]
pub const fn is_pk_eligible(tc: u8) -> bool {
    is_fixed_int(tc) || is_wide_int(tc)
}

/// Promote a base-table column's type to the leading-key type its secondary
/// index stores: an unsigned ≤8-byte integer (U8..U64) promotes to `U64`, a
/// signed ≤8-byte integer (I8..I64) to `I64`; `U128`/`UUID` keep their 16-byte
/// width; STRING/BLOB/float (and any unknown code) are index-ineligible and
/// return `Err`. Signed columns keep a *signed* promoted code so the OPK leading
/// key is order-preserving (`encode_pk_column` sign-flips only signed codes);
/// `wire_stride(I64) == wire_stride(U64) == 8`, so the sign the promotion picks
/// never moves the index record's arity or stride. The single source of
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
pub fn index_key_types(col_types: &[u8], src_pk_count: usize, src_pk_stride: usize) -> Result<Vec<u8>, IndexKeyRule> {
    let mut promoted: Vec<u8> = Vec::with_capacity(col_types.len());
    for (col, &t) in col_types.iter().enumerate() {
        // Indexed by position, so the layer above can name the SQL column that
        // failed; `index_key_type`'s own string says only the type code.
        let p = index_key_type(t).map_err(|_| IndexKeyRule::NotEligible { col, type_code: t })?;
        promoted.push(p);
    }
    let n = promoted.len();
    if n + src_pk_count > crate::MAX_PK_COLUMNS {
        return Err(IndexKeyRule::ArityOutOfRange { n, src_pk_count });
    }
    let stride: usize = promoted.iter().map(|&t| wire_stride(t)).sum::<usize>() + src_pk_stride;
    if stride > crate::MAX_PK_BYTES {
        return Err(IndexKeyRule::StrideOutOfRange { stride });
    }
    Ok(promoted)
}

/// Which rule a candidate secondary-index key broke — [`PkRule`]'s counterpart
/// for [`index_key_types`], so the SQL planner can name the offending column by
/// its SQL identifier. `col` indexes `col_types`, never the appended source PK:
/// only the indexed columns are promoted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexKeyRule {
    /// STRING/BLOB/float (or an unknown code) has no order-preserving
    /// fixed-width index key to promote to.
    NotEligible { col: usize, type_code: u8 },
    /// The index record is the indexed columns plus the source PK, and every one
    /// of them is a PK column, so their total arity is capped by
    /// [`crate::MAX_PK_COLUMNS`].
    ArityOutOfRange { n: usize, src_pk_count: usize },
    /// The same record's packed PK region must fit [`crate::MAX_PK_BYTES`].
    StrideOutOfRange { stride: usize },
}

impl core::fmt::Display for IndexKeyRule {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match *self {
            IndexKeyRule::NotEligible { col, type_code } => write!(
                f,
                "Secondary index on column type {type_code} not supported (index key column {col})"
            ),
            IndexKeyRule::ArityOutOfRange { n, src_pk_count } => write!(
                f,
                "index arity {n} + source PK arity {src_pk_count} exceeds the limit of {}",
                crate::MAX_PK_COLUMNS
            ),
            IndexKeyRule::StrideOutOfRange { stride } => write!(
                f,
                "index record stride {stride} exceeds the limit of {} bytes",
                crate::MAX_PK_BYTES
            ),
        }
    }
}

/// Which rule a candidate primary key broke. Returned by
/// [`validate_pk_indices`] / [`validate_pk_tuple`] instead of a formatted
/// string, so the *rule set* stays in one place while a layer that can say more
/// than the rule knows renders its own message. Only the SQL planner does: it
/// names the offending column by its SQL identifier. The client and the engine
/// catalog both take `Display`'s neutral wording.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PkRule {
    /// The word carries no [`crate::PK_LIST_PACKED_FLAG`]. The one rule about the
    /// *word*; every other presupposes a decoded list.
    NotPacked,
    /// No PK columns at all. Every base table has an enforced primary key.
    Empty,
    /// Arity past [`crate::PK_LIST_MAX_COLS`], the persisted PK-list codec capacity.
    TooManyColumns { count: usize },
    /// A PK index that names no column.
    IndexOutOfRange { col: u32 },
    /// The same column listed twice — it would double-count in the stride and
    /// yield duplicates from the PK-column walk.
    Duplicate { col: u32 },
    /// STRING/BLOB (an unrelocatable heap offset) or a float (IEEE-754 breaks
    /// the byte-equal key contract the OPK encoder rests on).
    NotEligible { col: u32, type_code: u8 },
    /// The PK region carries no null bitmap, so a NULL has nowhere to live.
    Nullable { col: u32 },
    /// The packed PK region must fit [`MAX_PK_BYTES`].
    StrideOutOfRange { stride: usize },
}

/// Which list a [`PkRule`] is about: [`validate_pk_indices`]' four structural
/// rules are equally a secondary index's column-list rules, so only the noun
/// differs. Not a `&str`, which a call site could spell wrong unnoticed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PkListRole {
    PrimaryKey,
    ColumnList,
}

impl PkListRole {
    /// The noun every rule's message opens with.
    fn noun(self) -> &'static str {
        match self {
            PkListRole::PrimaryKey => "primary key",
            PkListRole::ColumnList => "column list",
        }
    }
}

impl PkRule {
    /// This rule's message, worded for the list it is about.
    pub fn for_role(&self, role: PkListRole) -> String {
        let what = role.noun();
        match *self {
            PkRule::NotPacked => format!("{what} word carries no packed-list flag"),
            PkRule::Empty => format!("{what} must name at least one column"),
            PkRule::TooManyColumns { count } => format!(
                "{what} column count {count} out of range 1..={}",
                crate::PK_LIST_MAX_COLS
            ),
            PkRule::IndexOutOfRange { col } => format!("{what} index {col} out of bounds"),
            PkRule::Duplicate { col } => format!("{what} names column {col} twice"),
            PkRule::NotEligible { col, type_code } => format!(
                "{what} column {col} has type_code {type_code}; only fixed-width integer, \
                 U128, UUID, and I128 columns can be PK columns \
                 (String, Blob, and float columns cannot)"
            ),
            PkRule::Nullable { col } => format!("{what} column {col} must not be nullable"),
            PkRule::StrideOutOfRange { stride } => format!(
                "{what} total stride must be 1..={} bytes, got {stride}",
                crate::MAX_PK_BYTES
            ),
        }
    }
}

/// The primary-key wording — the role every caller that does not say otherwise
/// is about.
impl core::fmt::Display for PkRule {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(&self.for_role(PkListRole::PrimaryKey))
    }
}

/// The structural half of the primary-key admission rule: non-empty, within the
/// [`crate::PK_LIST_MAX_COLS`] arity cap, every index naming a real column, no
/// duplicates. Split from the column-type half so a caller holding only the PK
/// list can run it alone; `ncols` is whatever bound that caller has — a real
/// column count, or a field width for a list whose columns do not exist yet.
pub fn validate_pk_indices(pk_cols: &[u32], ncols: usize) -> Result<(), PkRule> {
    if !crate::pk_list_arity_ok(pk_cols.len()) {
        return Err(if pk_cols.is_empty() {
            PkRule::Empty
        } else {
            PkRule::TooManyColumns { count: pk_cols.len() }
        });
    }
    for (j, &c) in pk_cols.iter().enumerate() {
        if c as usize >= ncols {
            return Err(PkRule::IndexOutOfRange { col: c });
        }
        if pk_cols[..j].contains(&c) {
            return Err(PkRule::Duplicate { col: c });
        }
    }
    Ok(())
}

/// The typed half of [`validate_pk_tuple`], which runs the structural half
/// first, so `col` may assume its index is in range. Returns the validated
/// `pk_stride`. Base-table counterpart of [`index_key_types`].
fn validate_pk_column_types(pk_cols: &[u32], col: impl Fn(u32) -> (u8, bool)) -> Result<usize, PkRule> {
    let mut stride = 0usize;
    for &c in pk_cols {
        let (type_code, nullable) = col(c);
        if !is_pk_eligible(type_code) {
            return Err(PkRule::NotEligible { col: c, type_code });
        }
        if nullable {
            return Err(PkRule::Nullable { col: c });
        }
        stride += wire_stride(type_code);
    }
    // `stride == 0` is unreachable once every column passed `is_pk_eligible`
    // (each eligible type is ≥ 1 byte); rejected explicitly so an empty list
    // reaching here through the typed half alone cannot pass.
    if stride == 0 || stride > crate::MAX_PK_BYTES {
        return Err(PkRule::StrideOutOfRange { stride });
    }
    Ok(stride)
}

/// Both halves of the primary-key admission rule, for the callers that hold the
/// columns up front. Returns the validated `pk_stride`.
pub fn validate_pk_tuple(pk_cols: &[u32], ncols: usize, col: impl Fn(u32) -> (u8, bool)) -> Result<usize, PkRule> {
    validate_pk_indices(pk_cols, ncols)?;
    validate_pk_column_types(pk_cols, col)
}

/// Whether a raw wire type code uses the 16-byte German-string layout. u8-based
/// counterpart to [`TypeCode::is_german_string`] for callers holding a raw
/// `type_code` (mirrors the free `wire_stride`/`is_pk_eligible`). Unknown codes
/// are not german strings.
#[inline(always)]
pub const fn is_german_string(tc: u8) -> bool {
    tc == type_code::STRING || tc == type_code::BLOB
}

/// True iff `tc` is one of the two IEEE-754 column types. The `u8` counterpart
/// of [`TypeCode::is_float`], for the raw-type-code paths.
#[inline(always)]
pub const fn is_float(tc: u8) -> bool {
    matches!(tc, type_code::F32 | type_code::F64)
}

/// True iff `tc` is a 16-byte integer type (U128/UUID/I128). The `u8`
/// counterpart of [`TypeCode::is_wide_int`], for the raw-type-code paths.
#[inline(always)]
pub const fn is_wide_int(tc: u8) -> bool {
    matches!(tc, type_code::U128 | type_code::UUID | type_code::I128)
}

/// True iff `tc` decodes to a known [`TypeCode`]. The one predicate every
/// trust boundary that turns raw client type-code bytes into a schema column
/// asks: an unknown code is not inert — [`wire_stride`] reports 8 for it (so a
/// width test passes it through) and [`TypeCode::from_validated_u8`] panics on
/// it at every downstream consumer.
#[inline(always)]
pub const fn is_valid_type_code(tc: u8) -> bool {
    TypeCode::try_from_u8(tc).is_some()
}

/// The type of the register image the engine materializes for a computed value
/// of source type `tc`: any float lands as `F64` (`LOAD_COL_FLOAT` widens `F32`
/// on load), `U64` stays unsigned so a downstream compare re-seeds the unsigned
/// variant, and every other integer normalizes to `I64`. A register sink stores that image
/// whole, so a computed column typed any narrower would ship the low half of an
/// `f64` or wrap a negative value into an unsigned slot.
///
/// `STRING` maps to itself — the VM has a string register class beside the
/// scalar one — which is what makes the rule total enough for expression typing
/// to read it unconditionally; `BLOB` has a register of neither class and falls
/// in with the rest.
#[inline]
pub(crate) const fn register_image_type(tc: u8) -> u8 {
    if is_float(tc) {
        type_code::F64
    } else if tc == type_code::U64 {
        type_code::U64
    } else if tc == type_code::STRING {
        type_code::STRING
    } else {
        type_code::I64
    }
}

/// True iff every value of integer type `src` is representable in `target`.
/// Crossing into signed needs strictly more width, an equal-width signed type
/// not holding the unsigned range; `UUID` is in no domain, U128's width
/// notwithstanding. The *value* domain — [`join_key_common_type`] answers the
/// same-looking question for a reindex key, whose codomain collapses onto U128.
pub const fn int_domain_fits(src: u8, target: u8) -> bool {
    if !is_int(src) || !is_int(target) {
        return false;
    }
    if is_signed_int(src) == is_signed_int(target) {
        wire_stride(target) >= wire_stride(src)
    } else {
        is_signed_int(target) && wire_stride(target) > wire_stride(src)
    }
}

/// True iff `target` is a value-preserving *widening promotion* of `src` — the
/// only type change a column copy performs (`widen_native_le` sign/zero-extends
/// a narrower integer into a wider slot; there is no narrowing and no
/// representation change). The `is_fixed_int(target)` gate is this caller's own
/// scope, not a screen on the rule: a column copy only ever widens into a
/// ≤8-byte slot. Shared by the engine compiler's carried-target validation and
/// the expression validator's column-sink slot check so the two cannot drift.
#[inline]
pub fn is_widening_promotion(src: u8, target: u8) -> bool {
    is_fixed_int(target) && int_domain_fits(src, target)
}

/// Whether a raw wire type code is a *signed* fixed-width integer
/// (I8/I16/I32/I64/I128). The order-preserving encoders/comparators flip the
/// sign bit for these so two's-complement negatives sort below non-negatives.
/// Unsigned, float, string, and unknown codes are not signed.
#[inline(always)]
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
#[inline(always)]
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

/// Whether a raw wire type code is an **integer** of any width or sign —
/// [`is_fixed_int`]'s eight codes plus the 128-bit pair. UUID shares U128's
/// width and is not one. The domain [`int_domain_fits`] is defined on, where
/// `is_fixed_int`'s ≤ 8-byte scope would silently exclude a 128-bit column.
pub(crate) const fn is_int(tc: u8) -> bool {
    is_fixed_int(tc) || tc == type_code::U128 || tc == type_code::I128
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

    /// The type code this width names — the inverse of [`Self::from_type_code`],
    /// so a narrowed target can be written back to the wire.
    pub const fn type_code(self) -> TypeCode {
        match self {
            Self::U8 => TypeCode::U8,
            Self::I8 => TypeCode::I8,
            Self::U16 => TypeCode::U16,
            Self::I16 => TypeCode::I16,
            Self::U32 => TypeCode::U32,
            Self::I32 => TypeCode::I32,
            Self::U64 => TypeCode::U64,
            Self::I64 => TypeCode::I64,
        }
    }

    /// Byte width (1/2/4/8).
    ///
    /// `#[inline(always)]`: a `const fn` returning one of four constants, on the
    /// evaluator's per-instruction PK-load path. Without the attribute it is an
    /// out-of-line cross-crate call at `-O0` — the profile the E2E suite runs.
    #[inline(always)]
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

    /// Whether this integer type is signed. The order-preserving encoders flip
    /// the top bit for these, so two's-complement negatives sort below
    /// non-negatives; [`ScalarKind::order_inverse`] flips it back.
    #[inline(always)]
    pub const fn is_signed(self) -> bool {
        matches!(self, Self::I8 | Self::I16 | Self::I32 | Self::I64)
    }

    /// Decode the leading `width()` little-endian bytes of `b` as this integer,
    /// sign- or zero-extended into `i64`. Total: every arm is a real ≤8-byte
    /// integer with a pinned width, so `try_into` cannot fail.
    #[inline(always)]
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

/// The ≤8-byte scalar register image of a column type: the domain on which
/// "read these native-LE bytes as a number" is total. THE shared rule — the SQL
/// binder's cast and aggregate gates and the engine's reduce kernel and value
/// index all resolve a column through it, so they cannot disagree about one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScalarKind {
    Int(FixedInt),
    F32,
    F64,
}

impl ScalarKind {
    /// Exhaustive over `TypeCode` (no `_` arm), so a new variant is a compile
    /// error here until someone decides whether it has a scalar image.
    pub const fn from_type_code(tc: TypeCode) -> Option<Self> {
        match tc {
            TypeCode::F32 => Some(Self::F32),
            TypeCode::F64 => Some(Self::F64),
            TypeCode::U8 => Some(Self::Int(FixedInt::U8)),
            TypeCode::I8 => Some(Self::Int(FixedInt::I8)),
            TypeCode::U16 => Some(Self::Int(FixedInt::U16)),
            TypeCode::I16 => Some(Self::Int(FixedInt::I16)),
            TypeCode::U32 => Some(Self::Int(FixedInt::U32)),
            TypeCode::I32 => Some(Self::Int(FixedInt::I32)),
            TypeCode::U64 => Some(Self::Int(FixedInt::U64)),
            TypeCode::I64 => Some(Self::Int(FixedInt::I64)),
            TypeCode::U128 | TypeCode::UUID | TypeCode::String | TypeCode::Blob | TypeCode::I128 => None,
        }
    }

    #[inline(always)]
    pub const fn is_float(self) -> bool {
        matches!(self, Self::F32 | Self::F64)
    }

    /// Inverse of `ColumnLocator::order_bits`: the value's own little-endian bits
    /// back out of the order image — IEEE bits at the source's own width for a
    /// float, the sign- or zero-extended integer otherwise.
    #[inline(always)]
    pub fn order_inverse(self, e: u64) -> u64 {
        match self {
            // The forward direction xors the same bit, so signed round-trips and
            // unsigned is the identity.
            Self::Int(fi) => e ^ ((fi.is_signed() as u64) << 63),
            Self::F32 => ieee_order_bits_f32_reverse(e) as u64,
            Self::F64 => ieee_order_bits_reverse(e),
        }
    }
}

// ---------------------------------------------------------------------------
// Order-preserving float image: `ColumnLocator::order_bits` encodes a float
// through the forward half, `ScalarKind::order_inverse` undoes it with the reverse.
// ---------------------------------------------------------------------------

/// IEEE 754 order-preserving encoding of an `f64`'s raw bits: negatives invert
/// wholly, non-negatives flip the sign bit, so plain unsigned order over the
/// result is `total_cmp` order.
#[inline(always)]
pub fn ieee_order_bits(raw_bits: u64) -> u64 {
    if raw_bits >> 63 != 0 {
        !raw_bits
    } else {
        raw_bits ^ (1u64 << 63)
    }
}

/// [`ieee_order_bits`] for 32-bit floats, returning u64. Checks the F32 sign bit
/// (bit 31), not bit 63.
#[inline(always)]
pub fn ieee_order_bits_f32(raw_bits: u32) -> u64 {
    (if raw_bits >> 31 != 0 {
        !raw_bits
    } else {
        raw_bits ^ (1u32 << 31)
    }) as u64
}

/// Reverse of [`ieee_order_bits`].
#[inline(always)]
fn ieee_order_bits_reverse(encoded: u64) -> u64 {
    if encoded >> 63 != 0 {
        encoded ^ (1u64 << 63)
    } else {
        !encoded
    }
}

/// Reverse of [`ieee_order_bits_f32`].
#[inline(always)]
fn ieee_order_bits_f32_reverse(encoded: u64) -> u32 {
    let e = encoded as u32;
    if e >> 31 != 0 {
        e ^ (1u32 << 31)
    } else {
        !e
    }
}

// `ScalarKind` and `FixedInt` each spell their own exhaustive `TypeCode` match,
// so a new variant must be classified in both — this holds their answers equal
// rather than leaving it to whoever adds one.
const _: () = {
    let mut i = 0;
    while i < TypeCode::ALL.len() {
        let tc = TypeCode::ALL[i];
        let kind = ScalarKind::from_type_code(tc);
        match FixedInt::from_type_code(tc) {
            Some(fi) => {
                assert!(matches!(kind, Some(ScalarKind::Int(_))), "a FixedInt has an Int image");
                assert!(
                    fi.type_code() as u8 == tc as u8,
                    "FixedInt::type_code must invert from_type_code"
                );
                assert!(
                    fi.is_signed() == is_signed_int(tc as u8),
                    "FixedInt::is_signed must match the raw-code predicate"
                );
                // `FixedInt::width` restates the table `wire_stride` owns, and
                // `encode_pk_column` dispatches on the destination slice's
                // length — so a typo writes the wrong width silently in release.
                assert!(
                    fi.width() == wire_stride(tc as u8),
                    "FixedInt::width must be the type's wire stride"
                );
            }
            None => assert!(
                kind.is_some() == tc.is_float(),
                "a non-FixedInt has a scalar image iff it is a float"
            ),
        }
        i += 1;
    }
};

/// The width policy behind [`TypeCode::reindex_output_type`] and
/// [`resolve_reindex_type`], which are what every external consumer calls. See
/// that method for the policy and the engine ↔ planner lockstep it anchors.
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
    // U128/UUID and STRING/BLOB content hashes).
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
    // type; a 16-byte operand carries the pair to U128. `is_pk_eligible` is the
    // integer-scalar set; minus the signed ones leaves the unsigned ones.
    let is_unsigned_int = |tc: u8| is_pk_eligible(tc) && !is_signed_int(tc);
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
    // integer-scalar pair is opposite-sign.) The common type must be a SIGNED type
    // (a) strictly wider than the unsigned operand — a signed type of equal width
    // cannot represent the unsigned operand's full range, so distinct values
    // would alias — and (b) at least as wide as the signed operand. The unsigned
    // side zero-extends and the signed side sign-extends into it
    // (`encode_pk_column_promoted`), so equal numeric values pack byte-identically.
    // wu ∈ {1,2,4,8,16}; only a U128/UUID unsigned operand (wu == 16) needs a
    // signed-256 type that does not exist → None.
    if is_pk_eligible(l) && is_pk_eligible(r) {
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
/// when the planner disagreed with the per-column default policy, else that
/// policy. The single home of the "carried-or-derive" rule, so the reindex Map's
/// output schema and the `ReindexPacker` cannot derive divergent slot widths.
#[inline]
pub const fn resolve_reindex_type(src_tc: u8, carried: Option<TypeCode>) -> u8 {
    match carried {
        Some(t) => t as u8,
        None => reindex_output_type_code(src_tc),
    }
}

/// Wire stride (byte width) for a column type code. Delegates to the single
/// width source [`TypeCode::wire_stride`]; an unknown code returns 8, the
/// survival width `SchemaColumn::new` — the one caller that can pass one —
/// documents its need for.
#[inline(always)]
pub const fn wire_stride(tc: u8) -> usize {
    match TypeCode::try_from_u8(tc) {
        Some(t) => t.wire_stride(),
        None => 8,
    }
}

// Pin the discriminant↔code round-trip: a `try_from_u8` typo (e.g. mapping a
// code to the wrong-discriminant variant) would otherwise silently mis-stride a
// column via `wire_stride`.
const _: () = {
    let mut v: u16 = 0; // u16 so `v += 1` cannot overflow at 255
    while v <= 255 {
        // `ALL` is the one enumeration of the type table — the Python `TypeCode`
        // IntEnum is built from it — so it must agree with `try_from_u8` on
        // membership in *both* directions. A code `try_from_u8` accepts but
        // `ALL` omits is invisible to a client-side list of names.
        let mut in_all = false;
        let mut k = 0;
        while k < TypeCode::ALL.len() {
            if TypeCode::ALL[k] as u8 == v as u8 {
                assert!(!in_all, "duplicate code in TypeCode::ALL");
                in_all = true;
            }
            k += 1;
        }
        if let Some(t) = TypeCode::try_from_u8(v as u8) {
            assert!(
                t as u8 == v as u8,
                "TypeCode discriminant must round-trip through try_from_u8"
            );
            assert!(in_all, "try_from_u8 accepts a code TypeCode::ALL omits");
        } else {
            assert!(!in_all, "TypeCode::ALL carries a code try_from_u8 rejects");
        }
        // `is_fixed_int` is the domain of `read_{signed,unsigned}_exact` and of
        // the engine's fixed-int fast-path row comparator, whose branchless
        // sign-flip shifts by `size*8 - 1` into a u64 — so widening the
        // predicate past 8 bytes must fail here rather than there.
        let w = wire_stride(v as u8);
        assert!(
            !is_fixed_int(v as u8) || (w == 1 || w == 2 || w == 4 || w == 8),
            "is_fixed_int must imply a 1/2/4/8-byte width"
        );
        v += 1;
    }
    assert!(
        wire_stride(0) == 8 && wire_stride(16) == 8,
        "an unknown code must get a non-zero survival width, not a colliding 0"
    );
};

#[cfg(test)]
#[path = "tests/types.rs"]
mod tests;
