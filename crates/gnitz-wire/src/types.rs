//! Column types: the typed `TypeCode` enum and the scale-carrying `ColType`.

use core::cmp::Ordering;

crate::wire_enum! {
    /// A column's type code. The discriminant is the wire byte, and
    /// [`Self::from_wire`] decodes one.
    pub enum TypeCode: u8 {
        U8 = 1,
        I8 = 2,
        U16 = 3,
        I16 = 4,
        U32 = 5,
        I32 = 6,
        F32 = 7,
        U64 = 8,
        I64 = 9,
        F64 = 10,
        String = 11,
        U128 = 12,
        UUID = 13,
        Blob = 14,
        I128 = 15,
        /// Days since 1970-01-01, physically an `I32`.
        Date = 16,
        /// Microseconds since 1970-01-01T00:00:00, physically an `I64`.
        Timestamp = 17,
        /// A fixed-point number: physically an `I64` holding the value times
        /// `10^scale`. The scale is a per-column fact carried beside the code
        /// ([`ColType`], the schema record's scale byte, `COL_TAB.scale`); storage, ordering,
        /// routing and the VM see the integer alone.
        Decimal = 18,
    }
}

impl TypeCode {
    /// The type's name in the wire vocabulary, which is what the client bindings
    /// expose. Exhaustive on purpose: a new variant fails to compile until named.
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
            TypeCode::Date => "DATE",
            TypeCode::Timestamp => "TIMESTAMP",
            TypeCode::Decimal => "DECIMAL",
        }
    }

    /// The two calendar types. Each is an integer of a fixed width under a
    /// different name: every storage, ordering and VM path treats it as
    /// [`Self::storage_type`].
    #[inline(always)]
    pub const fn is_temporal(self) -> bool {
        matches!(self, TypeCode::Date | TypeCode::Timestamp)
    }

    /// The integer type a value of this type is stored, ordered and computed as:
    /// `I32` for `Date`, `I64` for `Timestamp` and `Decimal`, and the type
    /// itself otherwise.
    #[inline(always)]
    pub const fn storage_type(self) -> TypeCode {
        match self {
            TypeCode::Date => TypeCode::I32,
            TypeCode::Timestamp | TypeCode::Decimal => TypeCode::I64,
            t => t,
        }
    }

    /// Whether this type is one of the two IEEE-754 column types.
    #[inline(always)]
    pub const fn is_float(self) -> bool {
        matches!(self, TypeCode::F32 | TypeCode::F64)
    }

    /// The type of the register image the engine materializes for a computed
    /// value of this source type: any float lands as `F64` (`LoadColFloat`
    /// widens `F32` on load), `U64` stays unsigned so a downstream compare
    /// re-seeds the unsigned variant, and every other integer normalizes to
    /// `I64`. A register sink stores that image whole, so a computed column typed
    /// any narrower would ship the low half of an `f64` or wrap a negative value
    /// into an unsigned slot.
    ///
    /// `STRING` maps to itself — the VM has a string register class beside the
    /// scalar one — which is what makes the rule total enough for expression
    /// typing to read it unconditionally; `BLOB` has a register of neither class
    /// and falls in with the rest.
    ///
    /// A temporal or decimal type also maps to itself: the register holds the
    /// 8-byte integer while the declared column keeps its name. A `DATE` slot is
    /// narrower than that register, so a sink into one is admitted only behind a
    /// cast that range-checks the value into that width (`check_emit_slot`).
    #[inline]
    pub const fn register_image(self) -> TypeCode {
        match self {
            TypeCode::F32 | TypeCode::F64 => TypeCode::F64,
            TypeCode::U64 | TypeCode::String | TypeCode::Date | TypeCode::Timestamp | TypeCode::Decimal => self,
            _ => TypeCode::I64,
        }
    }

    /// The 16-byte integer-ish types (U128, UUID, I128), which have no i64 slot in
    /// the expression VM.
    #[inline(always)]
    pub const fn is_wide_int(self) -> bool {
        matches!(self, TypeCode::U128 | TypeCode::UUID | TypeCode::I128)
    }

    /// Whether this type uses the 16-byte "German string" layout (a 4-byte
    /// length, a 4-byte inline prefix, and an inline-or-out-of-line tail).
    /// STRING and BLOB share this representation; both must compare, relocate,
    /// and copy via the german-string paths (`compare_german_strings`, the blob
    /// heap), never via fixed-width byte ops.
    #[inline(always)]
    pub const fn is_german_string(self) -> bool {
        matches!(self, TypeCode::String | TypeCode::Blob)
    }

    /// Whether this type is a signed integer (I8/I16/I32/I64/I128, and the
    /// types stored as one): the order-preserving encoders flip the sign bit for
    /// these so two's-complement negatives sort below non-negatives. Unsigned,
    /// float, and string types are not signed.
    #[inline(always)]
    pub const fn is_signed_int(self) -> bool {
        matches!(
            self.storage_type(),
            TypeCode::I8 | TypeCode::I16 | TypeCode::I32 | TypeCode::I64 | TypeCode::I128
        )
    }

    /// Whether this type is a fixed-width integer of ≤ 8 bytes, any sign — the
    /// domain of [`FixedInt`], which answers through the storage type. These are
    /// exactly the payload columns the fixed-int fast-path row comparator can
    /// compare via a single `u64` load.
    #[inline(always)]
    pub const fn is_fixed_int(self) -> bool {
        FixedInt::from_type_code(self).is_some()
    }

    /// Whether this type is an **integer** of any width or sign —
    /// [`Self::is_fixed_int`]'s domain plus the 128-bit pair. UUID shares U128's
    /// width and is not one. The domain [`Self::int_domain_fits`] is defined on,
    /// where `is_fixed_int`'s ≤ 8-byte scope would silently exclude a 128-bit
    /// column.
    #[inline(always)]
    const fn is_int(self) -> bool {
        self.is_fixed_int() || matches!(self, TypeCode::U128 | TypeCode::I128)
    }

    /// Whether this type may be a PRIMARY KEY column: the integer scalars of
    /// every width, and nothing else. A PK region is compared as raw bytes, which
    /// String/Blob heap offsets and IEEE-754 floats (±0.0 differ byte-wise but
    /// compare equal) do not survive.
    #[inline(always)]
    pub const fn is_pk_eligible(self) -> bool {
        self.is_fixed_int() || self.is_wide_int()
    }

    /// Byte stride (width) of this type in a column payload.
    #[inline(always)]
    pub const fn wire_stride(self) -> usize {
        match self {
            TypeCode::U8 | TypeCode::I8 => 1,
            TypeCode::U16 | TypeCode::I16 => 2,
            TypeCode::F32 | TypeCode::U32 | TypeCode::I32 | TypeCode::Date => 4,
            TypeCode::F64 | TypeCode::U64 | TypeCode::I64 | TypeCode::Timestamp | TypeCode::Decimal => 8,
            TypeCode::U128 | TypeCode::UUID | TypeCode::String | TypeCode::Blob | TypeCode::I128 => 16,
        }
    }

    /// True iff every value of integer type `self` is representable in `target`.
    /// Crossing into signed needs strictly more width, an equal-width signed type
    /// not holding the unsigned range; `UUID` is in no domain, U128's width
    /// notwithstanding. The *value* domain — [`Self::join_key_common_type`]
    /// answers the same-looking question for a reindex key, whose codomain
    /// collapses onto U128.
    pub const fn int_domain_fits(self, target: TypeCode) -> bool {
        if !self.is_int() || !target.is_int() {
            return false;
        }
        if self.is_signed_int() == target.is_signed_int() {
            target.wire_stride() >= self.wire_stride()
        } else {
            target.is_signed_int() && target.wire_stride() > self.wire_stride()
        }
    }

    /// True iff `target` is a value-preserving *widening promotion* of `self` —
    /// the only type change a column copy performs (`widen_native_le`
    /// sign/zero-extends a narrower integer into a wider slot; there is no
    /// narrowing and no representation change). The `is_fixed_int(target)` gate
    /// is this caller's own scope, not a screen on the rule: a column copy only
    /// ever widens into a ≤8-byte slot. The expression validator's
    /// `check_copy_types` asks it of every column sink, and the SQL planner asks
    /// it of a set-op pair's promotion target so the two agree on what a copy may
    /// do.
    #[inline]
    pub const fn is_widening_promotion(self, target: TypeCode) -> bool {
        target.is_fixed_int() && self.int_domain_fits(target)
    }

    /// The key slot type of this column keyed alone.
    #[inline]
    pub const fn reindex_output_type(self) -> TypeCode {
        if self.is_fixed_int() || matches!(self, TypeCode::I128) {
            self
        } else {
            TypeCode::U128
        }
    }

    /// Whether a key column of this type can pack into a `slot`-typed key slot.
    pub const fn packs_at(self, slot: TypeCode) -> bool {
        slot.as_wire() == self.reindex_output_type().as_wire()
            || matches!(self.join_key_common_type(slot), Ok(t) if t.as_wire() == slot.as_wire())
    }

    /// The key slot type both sides of an equijoin key pair pack at.
    pub const fn join_key_common_type(self, other: TypeCode) -> Result<TypeCode, JoinKeyRule> {
        let (l, r) = (self, other);
        if l.is_float() || r.is_float() {
            return Err(JoinKeyRule::Float);
        }
        if l.is_german_string() != r.is_german_string() {
            return Err(JoinKeyRule::StringWithNative);
        }
        if l.as_wire() == r.as_wire() {
            return Ok(l.reindex_output_type());
        }
        if l.is_german_string() {
            return Ok(TypeCode::U128);
        }
        if l.is_temporal() && r.is_temporal() {
            return Err(JoinKeyRule::UnitMismatch);
        }
        // A temporal side keys as its storage integer.
        if l.is_signed_int() && r.is_signed_int() {
            let (l, r) = (l.storage_type(), r.storage_type());
            return Ok(if l.wire_stride() >= r.wire_stride() { l } else { r });
        }
        if !l.is_signed_int() && !r.is_signed_int() {
            let wider = if l.wire_stride() >= r.wire_stride() { l } else { r };
            return Ok(if wider.wire_stride() == 16 {
                TypeCode::U128
            } else {
                wider
            });
        }
        // Cross-sign: the narrowest signed type strictly wider than the unsigned
        // side and at least as wide as the signed one.
        let (s, u) = if l.is_signed_int() { (l, r) } else { (r, l) };
        let uw = u.wire_stride() * 2;
        let common_w = if uw > s.wire_stride() { uw } else { s.wire_stride() };
        match common_w {
            2 => Ok(TypeCode::I16),
            4 => Ok(TypeCode::I32),
            8 => Ok(TypeCode::I64),
            16 => Ok(TypeCode::I128),
            _ => Err(JoinKeyRule::NoSigned256),
        }
    }
}

/// Why an equijoin key pair has no common key type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinKeyRule {
    /// `±0.0` differ byte-wise but compare equal.
    Float,
    /// DATE counts days, TIMESTAMP microseconds; a key copy cannot convert.
    UnitMismatch,
    /// A content hash never equals a native key.
    StringWithNative,
    /// A cross-sign pair whose unsigned side is 128-bit needs a signed-256 type.
    NoSigned256,
}

impl core::fmt::Display for TypeCode {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(self.wire_name())
    }
}

/// A column's logical type: the wire code and, for `DECIMAL`, the scale — the
/// power of ten the stored `I64` is multiplied by. Zero for every other type,
/// so two `ColType`s compare equal exactly when a value of one is a value of the
/// other.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ColType {
    pub tc: TypeCode,
    pub scale: u8,
}

impl ColType {
    pub const fn of(tc: TypeCode) -> Self {
        ColType { tc, scale: 0 }
    }

    pub const fn decimal(scale: u8) -> Self {
        ColType { tc: TypeCode::Decimal, scale }
    }

    pub const fn is_decimal(self) -> bool {
        matches!(self.tc, TypeCode::Decimal)
    }

    /// Whether this is a type a column may have: a scale only on a DECIMAL, and
    /// never past [`crate::decimal::MAX_DECIMAL_SCALE`].
    pub const fn is_admissible(self) -> bool {
        self.scale <= crate::decimal::MAX_DECIMAL_SCALE && (self.scale == 0 || self.is_decimal())
    }

    /// Decode a column type from its wire code and scale bytes, or `None` for an
    /// unknown code or an inadmissible scale.
    pub fn from_wire(code: u8, scale: u8) -> Option<ColType> {
        TypeCode::from_wire(code)
            .map(|tc| ColType { tc, scale })
            .filter(|t| t.is_admissible())
    }

    /// Whether a DECIMAL may be matched with `other` across relations. Only the
    /// identical DECIMAL: the stored integers of two scales never mean the same
    /// number.
    pub const fn decimal_domains_match(self, other: Self) -> bool {
        if !self.is_decimal() && !other.is_decimal() {
            return true;
        }
        self.tc.as_wire() == other.tc.as_wire() && self.scale == other.scale
    }

    /// [`TypeCode::register_image`] with the scale kept: a computed DECIMAL is
    /// still the same DECIMAL.
    pub fn register_image(self) -> Self {
        ColType {
            tc: self.tc.register_image(),
            scale: self.scale,
        }
    }
}

impl From<TypeCode> for ColType {
    fn from(tc: TypeCode) -> Self {
        ColType::of(tc)
    }
}

impl core::fmt::Display for ColType {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self.tc {
            // The precision is the `i64`'s own digit count, not what the column
            // was declared with: a DECIMAL's value is bounded by the integer
            // behind its scale, and `ColType` carries no declared precision.
            TypeCode::Decimal => write!(f, "DECIMAL({}, {})", crate::decimal::MAX_DECIMAL_SCALE, self.scale),
            tc => write!(f, "{tc}"),
        }
    }
}

/// Compare two equal-width column windows of type `tc`: German strings
/// (STRING/BLOB) by content through their backing blob arenas, every
/// fixed-width type by its value — unsigned magnitude, signed two's complement,
/// or `total_cmp` for a float. The blob slices back each side's heap payload,
/// and are ignored for non-string columns.
///
/// In `gnitz-wire` because the client-side comparators are held to the same
/// order as the engine's.
#[inline(always)]
pub fn cmp_col_window(a: &[u8], a_blob: &[u8], b: &[u8], b_blob: &[u8], tc: TypeCode) -> Ordering {
    // Deliberately not `debug_assert_eq!`: that takes both lengths by reference
    // and spills them, on a per-row path.
    debug_assert!(a.len() == b.len(), "cmp_col_window: windows must be equal length");
    #[inline(always)]
    fn arr<const N: usize>(s: &[u8]) -> [u8; N] {
        s.try_into().unwrap()
    }
    use TypeCode as T;
    match tc {
        T::U8 => a[0].cmp(&b[0]),
        T::I8 => (a[0] as i8).cmp(&(b[0] as i8)),
        T::U16 => u16::from_le_bytes(arr(a)).cmp(&u16::from_le_bytes(arr(b))),
        T::I16 => i16::from_le_bytes(arr(a)).cmp(&i16::from_le_bytes(arr(b))),
        T::U32 => u32::from_le_bytes(arr(a)).cmp(&u32::from_le_bytes(arr(b))),
        T::I32 | T::Date => i32::from_le_bytes(arr(a)).cmp(&i32::from_le_bytes(arr(b))),
        T::U64 => u64::from_le_bytes(arr(a)).cmp(&u64::from_le_bytes(arr(b))),
        T::I64 | T::Timestamp | T::Decimal => i64::from_le_bytes(arr(a)).cmp(&i64::from_le_bytes(arr(b))),
        T::U128 | T::UUID => u128::from_le_bytes(arr(a)).cmp(&u128::from_le_bytes(arr(b))),
        T::I128 => i128::from_le_bytes(arr(a)).cmp(&i128::from_le_bytes(arr(b))),
        T::F32 => f32::from_le_bytes(arr(a)).total_cmp(&f32::from_le_bytes(arr(b))),
        T::F64 => f64::from_le_bytes(arr(a)).total_cmp(&f64::from_le_bytes(arr(b))),
        T::String | T::Blob => crate::compare_german_strings(a, a_blob, b, b_blob),
    }
}

/// Promote a base-table column's type to the leading-key type its secondary
/// index stores: an unsigned ≤8-byte integer (U8..U64) promotes to `U64`, a
/// signed ≤8-byte integer (I8..I64) to `I64`, and a temporal or decimal type as
/// its storage integer does — so a `DATE` index key is the exercised 8-byte
/// signed one, not the only 4-byte key in the system; `U128`/`UUID` keep their
/// 16-byte width; STRING/BLOB/float/I128 are index-ineligible and return `None`.
/// Signed columns keep a *signed* promoted type so the OPK leading key is
/// order-preserving (`encode_pk_column` sign-flips only signed types);
/// `wire_stride(I64) == wire_stride(U64) == 8`, so the sign the promotion picks
/// never moves the index record's arity or stride.
pub fn index_key_type(field_type: TypeCode) -> Option<TypeCode> {
    match FixedInt::from_type_code(field_type) {
        Some(fi) if fi.is_signed() => Some(TypeCode::I64),
        Some(_) => Some(TypeCode::U64),
        None if matches!(field_type, TypeCode::U128 | TypeCode::UUID) => Some(field_type),
        None => None,
    }
}

/// Promote every indexed column type via [`index_key_type`] and validate the
/// resulting index-record layout. An index schema is
/// `(promoted_0, …, promoted_{n-1}, src_pk_0, …)` with every column in the PK,
/// so its PK arity is `n + src_pk_count`, capped by `MAX_PK_COLUMNS` — which
/// also keeps its stride within `MAX_PK_BYTES`, every key type being at most 16
/// bytes. Returns the promoted type list. The single source of truth
/// shared by the SQL planner's CREATE INDEX pre-check and the engine's
/// `make_index_schema`, so the friendly planner error and the engine backstop
/// can never disagree on a column's promoted width or the limits.
pub fn index_key_types(col_types: &[TypeCode], src_pk_count: usize) -> Result<Vec<TypeCode>, IndexKeyRule> {
    let mut promoted: Vec<TypeCode> = Vec::with_capacity(col_types.len());
    for (col, &t) in col_types.iter().enumerate() {
        // Indexed by position, so the layer above can name the SQL column that
        // failed.
        let p = index_key_type(t).ok_or(IndexKeyRule::NotEligible { col, type_code: t })?;
        promoted.push(p);
    }
    let n = promoted.len();
    if n + src_pk_count > crate::MAX_PK_COLUMNS {
        return Err(IndexKeyRule::ArityOutOfRange { n, src_pk_count });
    }
    Ok(promoted)
}

/// Which rule a candidate secondary-index key broke — [`PkRule`]'s counterpart
/// for [`index_key_types`], so the SQL planner can name the offending column by
/// its SQL identifier. `col` indexes `col_types`, never the appended source PK:
/// only the indexed columns are promoted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexKeyRule {
    /// A column type [`index_key_type`] promotes to no index key.
    NotEligible { col: usize, type_code: TypeCode },
    /// The index record is the indexed columns plus the source PK, and every one
    /// of them is a PK column, so their total arity is capped by
    /// [`crate::MAX_PK_COLUMNS`].
    ArityOutOfRange { n: usize, src_pk_count: usize },
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
        }
    }
}

/// Which rule a candidate primary key broke. Returned by
/// [`validate_pk_indices`] / [`validate_pk_tuple`] instead of a formatted
/// string, so the *rule set* stays in one place while a layer that can say more
/// than the rule knows renders its own message. Only the SQL planner does: it
/// names the offending column by its SQL identifier. The client and the engine
/// catalog both take [`PkRule::for_role`]'s wording.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PkRule {
    /// The word carries no [`crate::PK_LIST_PACKED_FLAG`]. The one rule about the
    /// *word*; every other presupposes a decoded list.
    NotPacked,
    /// No PK columns at all. Every base table has an enforced primary key.
    Empty,
    /// Arity past the cap the caller passed, which rides along so the message
    /// names the bound that was actually applied.
    TooManyColumns { count: usize, max: usize },
    /// A PK index that names no column.
    IndexOutOfRange { col: u32 },
    /// The same column listed twice — it would double-count in the stride and
    /// yield duplicates from the PK-column walk.
    Duplicate { col: u32 },
    /// STRING/BLOB (an unrelocatable heap offset) or a float (IEEE-754 breaks
    /// the byte-equal key contract the OPK encoder rests on).
    NotEligible { col: u32, type_code: TypeCode },
    /// The PK region carries no null bitmap, so a NULL has nowhere to live.
    Nullable { col: u32 },
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
            PkRule::TooManyColumns { count, max } => {
                format!("{what} column count {count} out of range 1..={max}")
            }
            PkRule::IndexOutOfRange { col } => format!("{what} index {col} out of bounds"),
            PkRule::Duplicate { col } => format!("{what} names column {col} twice"),
            PkRule::NotEligible { col, type_code } => format!(
                "{what} column {col} has type_code {type_code}; only fixed-width integer, \
                 U128, UUID, and I128 columns can be PK columns \
                 (String, Blob, and float columns cannot)"
            ),
            PkRule::Nullable { col } => format!("{what} column {col} must not be nullable"),
        }
    }
}

/// The structural half of the primary-key admission rule: non-empty, within the
/// `max_pk` arity cap, every index naming a real column, no duplicates. Split
/// from the column-type half so a caller holding only the PK list can run it
/// alone; `ncols` is whatever bound that caller has — a real column count, or a
/// field width for a list whose columns do not exist yet.
///
/// `max_pk` is the caller's own arity cap: [`crate::PK_LIST_MAX_COLS`] for a
/// user-declared key, which must round-trip through the persisted PK-list word,
/// and [`crate::MAX_PK_COLUMNS`] for an engine schema derived from one.
pub(crate) fn validate_pk_indices(pk_cols: &[u32], ncols: usize, max_pk: usize) -> Result<(), PkRule> {
    if pk_cols.is_empty() {
        return Err(PkRule::Empty);
    }
    if pk_cols.len() > max_pk {
        return Err(PkRule::TooManyColumns { count: pk_cols.len(), max: max_pk });
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

/// Both halves of the primary-key admission rule — the structural half, then
/// each column's type and nullability — for the callers that hold the columns up
/// front.
pub fn validate_pk_tuple(
    pk_cols: &[u32],
    ncols: usize,
    max_pk: usize,
    col: impl Fn(u32) -> (TypeCode, bool),
) -> Result<(), PkRule> {
    debug_assert!(max_pk <= crate::MAX_PK_COLUMNS);
    validate_pk_indices(pk_cols, ncols, max_pk)?;
    for &c in pk_cols {
        let (type_code, nullable) = col(c);
        if !type_code.is_pk_eligible() {
            return Err(PkRule::NotEligible { col: c, type_code });
        }
        if nullable {
            return Err(PkRule::Nullable { col: c });
        }
    }
    Ok(())
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
    #[inline(always)]
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
            TypeCode::Date => Some(Self::I32),
            TypeCode::Timestamp | TypeCode::Decimal => Some(Self::I64),
            TypeCode::F32
            | TypeCode::F64
            | TypeCode::U128
            | TypeCode::UUID
            | TypeCode::String
            | TypeCode::Blob
            | TypeCode::I128 => None,
        }
    }

    /// `tc` itself as a fixed int: `None` for a type that is only stored as one.
    pub const fn exact(tc: TypeCode) -> Option<Self> {
        match Self::from_type_code(tc) {
            Some(fi) if fi.type_code().as_wire() == tc.as_wire() => Some(fi),
            _ => None,
        }
    }

    /// The type code this fixed int is stored as. It inverts
    /// [`Self::from_type_code`] only where [`Self::exact`] is `Some`.
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
    /// so one pair covers signed and unsigned variants. The SQL layer places a
    /// literal among these values instead of wrapping it into a different
    /// in-range one.
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
        (v as u128) & crate::image_mask(self.width())
    }

    /// Inverse of [`Self::pack`]: `v`'s low `width()` bytes read as this type,
    /// sign- or zero-extended into `i64` — what `decode_le_i64(&v.to_le_bytes())`
    /// answers, without the bytes.
    pub const fn unpack(self, v: u128) -> i64 {
        let shift = 128 - 8 * self.width() as u32;
        if self.is_signed() {
            ((v << shift) as i128 >> shift) as i64
        } else {
            ((v << shift) >> shift) as i64
        }
    }

    /// Whether this integer type is signed.
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
/// "read these native-LE bytes as a number" is total.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScalarKind {
    Int(FixedInt),
    F32,
    F64,
}

impl ScalarKind {
    /// A narrow integer's [`FixedInt`], else a float's own kind.
    pub const fn from_type_code(tc: TypeCode) -> Option<Self> {
        match (tc, FixedInt::from_type_code(tc)) {
            (_, Some(fi)) => Some(Self::Int(fi)),
            (TypeCode::F32, None) => Some(Self::F32),
            (TypeCode::F64, None) => Some(Self::F64),
            _ => None,
        }
    }
}

// A new `TypeCode` is classified by `FixedInt::from_type_code`'s exhaustive
// match; these hold the scalar/wide split and `FixedInt`'s width and sign
// tables to the type's own.
const _: () = {
    let mut i = 0;
    while i < TypeCode::ALL.len() {
        let tc = TypeCode::ALL[i];
        let kind = ScalarKind::from_type_code(tc);
        assert!(
            kind.is_some() != (tc.is_wide_int() || tc.is_german_string()),
            "a type is wide iff it has no scalar image"
        );
        // A temporal or decimal type is its storage integer under another name,
        // so the two must lay out identically — `wire_stride` spells the width
        // table separately from `storage_type`'s map.
        assert!(
            tc.wire_stride() == tc.storage_type().wire_stride(),
            "a type must have its storage type's width"
        );
        if let Some(fi) = FixedInt::from_type_code(tc) {
            assert!(
                fi.type_code().as_wire() == tc.storage_type().as_wire(),
                "FixedInt::type_code must invert from_type_code up to the storage type"
            );
            assert!(
                fi.is_signed() == tc.is_signed_int(),
                "FixedInt::is_signed must match the type's sign"
            );
            assert!(
                fi.width() == tc.wire_stride(),
                "FixedInt::width must be the type's wire stride"
            );
        }
        i += 1;
    }
};

#[cfg(test)]
#[path = "tests/types.rs"]
mod tests;
