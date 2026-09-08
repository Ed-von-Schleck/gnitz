//! Literal parsing + PK/seek key packing — the partition-routing source of
//! truth.
//!
//! `parse_pk_literal_packed` is the single helper through which every INSERT and
//! SEEK PK literal flows ([`PkPlan::push`], plus `try_col_eq_literal` /
//! `try_extract_pk_in` in the WHERE planner), so the master cannot route an
//! INSERT and a DELETE for the same key to different workers.
//!
//! One literal shape reaches those packers: the bound [`BoundLit`], read out of
//! a `BoundExpr` by the WHERE recognizers and out of a [`Constant`] by INSERT
//! VALUES.

use crate::ast_util::Constant;
use crate::error::GnitzSqlError;
use crate::ir::{BExpr, BoundExpr, UnaryOp};
#[cfg(test)]
use gnitz_core::PkBuf;
use gnitz_core::{FixedInt, PkColumn, Schema, TypeCode};

pub(crate) fn parse_uuid_str(s: &str) -> Result<u128, GnitzSqlError> {
    gnitz_wire::parse_uuid(s).ok_or_else(|| GnitzSqlError::Bind(format!("invalid UUID literal: {s:?}")))
}

/// Parse a numeric SQL literal as `i128`, applying `negated` (the literal sat
/// under `Expr::UnaryOp(Minus, _)`). The magnitude parses as `u128` so a type
/// minimum's own digit string is accepted — up to `2^127`, whose `as i128` is
/// `i128::MIN` and whose `wrapping_neg` is itself; parse-as-`i128`-then-negate
/// would reject it. `i128` covers every ≤8-byte type with room to spare, so an
/// out-of-type-range literal is *representable* — callers classify it against
/// `FixedInt::range` and decline or saturate, never wrap.
pub(crate) fn parse_literal_i128(n_str: &str, negated: bool) -> Option<i128> {
    let m = n_str.parse::<u128>().ok()?;
    if negated {
        (m <= i128::MAX as u128 + 1).then(|| (m as i128).wrapping_neg())
    } else {
        (m <= i128::MAX as u128).then_some(m as i128)
    }
}

/// Parse a numeric SQL literal into its packed-u128 PK form: the low
/// `wire_stride` bytes carry the column's native LE encoding
/// (`FixedInt::pack` — e.g. `-1_i8` → `0xFF`, not `0xFFFF_FFFF_FFFF_FFFF`),
/// the rest stay zero. An out-of-type-range literal declines (`None`) instead
/// of wrapping: without the `FixedInt::range` check, `x = 3000000000` on an
/// I32 column would cast to `-1294967296` and seek the wrong value (silent
/// wrong rows — the cff7c58-class trap).
///
/// This helper is the single source of truth for INSERT/SEEK PK routing —
/// [`PkPlan::push`], `try_col_eq_literal`, and `try_extract_pk_in` all
/// dispatch through it so the master cannot send INSERT and DELETE for the
/// same key to different workers.
pub(crate) fn parse_pk_literal_packed(tc: TypeCode, n_str: &str, negated: bool) -> Option<u128> {
    match tc {
        // U128/UUID take the full unsigned range — beyond i128, so they parse as
        // u128 directly rather than through the i128 value path.
        TypeCode::U128 | TypeCode::UUID => {
            if negated {
                return None;
            }
            n_str.parse::<u128>().ok()
        }
        _ => pack_pk_value(tc, parse_literal_i128(n_str, negated)?),
    }
}

/// Pack an already-parsed literal value into its packed-u128 PK form — the
/// value-level half of [`parse_pk_literal_packed`], for callers holding a native
/// integer (a `LitInt`) that would otherwise stringify only to re-parse. Same
/// contract: an out-of-type-range value declines (`None`) instead of wrapping.
/// (A U128/UUID value above `i128::MAX` cannot arrive here — such literals only
/// exist as digit strings and take the u128 parse above.)
pub(crate) fn pack_pk_value(tc: TypeCode, v: i128) -> Option<u128> {
    match tc {
        TypeCode::U128 | TypeCode::UUID => (v >= 0).then_some(v as u128),
        // I128 (the internal join-key promotion type) is two's complement at 16
        // bytes, exactly `as u128`.
        TypeCode::I128 => Some(v as u128),
        _ => {
            let fi = FixedInt::from_type_code(tc)?;
            let (min, max) = fi.range();
            (min <= v && v <= max).then(|| fi.pack(v))
        }
    }
}

// ---------------------------------------------------------------------------
// The bound-literal seam
// ---------------------------------------------------------------------------

/// A bound numeric literal, sign applied. `LitInt` carries its own sign; a
/// `LitWide` magnitude rides under an outer `Neg` when negative.
#[derive(Clone, Copy)]
pub(crate) enum NumLit<'e> {
    /// A native literal (any i64, sign applied — `-(i64::MIN)` fits i128).
    Small(i128),
    /// A wide magnitude digit string + sign. Kept as the raw string because the
    /// `i128`-vs-`u128` parse is the consumer's call: it holds the column
    /// `TypeCode`, and a `LitWide` in the `(i128::MAX, u128::MAX]` band
    /// (`U128`/`UUID`) needs the u128 parse a signed value could not represent.
    Wide(&'e str, bool),
}

impl NumLit<'_> {
    /// Whether the literal is below zero, without committing to a width its
    /// magnitude may not fit.
    fn is_negative(self) -> bool {
        match self {
            NumLit::Small(v) => v < 0,
            NumLit::Wide(_, negated) => negated,
        }
    }

    /// The literal as a signed value, for a consumer that classifies against a
    /// ≤8-byte type's range. `None` for a magnitude past `i128` — only a
    /// `U128`/`UUID` literal, which takes [`pack_num`]'s unsigned path instead.
    pub(crate) fn to_i128(self) -> Option<i128> {
        match self {
            NumLit::Small(v) => Some(v),
            NumLit::Wide(s, negated) => parse_literal_i128(s, negated),
        }
    }
}

/// The one seam from a bound numeric literal to a [`NumLit`].
pub(crate) fn bound_num_literal(e: &BoundExpr) -> Option<NumLit<'_>> {
    if let Some(v) = e.int_literal() {
        return Some(NumLit::Small(v as i128));
    }
    match e {
        BExpr::LitWide(s) => Some(NumLit::Wide(s, false)),
        BExpr::UnaryOp(UnaryOp::Neg, inner) => match inner.as_ref() {
            BExpr::LitWide(s) => Some(NumLit::Wide(s, true)),
            _ => None,
        },
        _ => None,
    }
}

/// Pack a numeric literal as a seek/range key for column type `tc`, byte-exactly
/// (an out-of-type-range literal declines, never wraps).
pub(crate) fn pack_num(tc: TypeCode, lit: NumLit<'_>) -> Option<u128> {
    match lit {
        NumLit::Small(v) => pack_pk_value(tc, v),
        NumLit::Wide(s, negated) => parse_pk_literal_packed(tc, s, negated),
    }
}

/// A bound literal accepted for a seek/range key: a numeric value + sign, or a
/// string (a single-quoted UUID).
pub(crate) enum BoundLit<'e> {
    Num(NumLit<'e>),
    Str(&'e str),
}

pub(crate) fn bound_literal(e: &BoundExpr) -> Option<BoundLit<'_>> {
    if let Some(n) = bound_num_literal(e) {
        return Some(BoundLit::Num(n));
    }
    if let BExpr::LitStr(s) = e {
        return Some(BoundLit::Str(s));
    }
    None
}

/// Why a bound literal is not a key for a column of a given type — classified
/// rather than swallowed, so the INSERT path can name the column and the offence.
pub(crate) enum KeyLitError {
    /// A negative literal against an unsigned column.
    NegativeIntoUnsigned,
    /// A numeric literal outside the column type's range.
    OutOfRange,
    /// A string literal against a UUID column that does not spell a UUID.
    BadUuid,
    /// A string literal against a DATE/TIMESTAMP column that does not spell one.
    BadTemporal,
    /// A literal of a kind no key is ever spelled by (a string outside UUID).
    NotNumeric,
}

/// A bound literal as the packed key of a column of type `tc`: a single-quoted
/// string is a key only for a UUID column, numerics pack through the same
/// `pk_codec` path INSERT takes. The one rule behind `col = literal`,
/// `col IN (literal, …)` and a written INSERT cell, so no two of those spellings
/// can route differently.
pub(crate) fn bound_key_literal(lit: BoundLit<'_>, tc: TypeCode) -> Result<u128, KeyLitError> {
    match lit {
        BoundLit::Str(s) if tc == TypeCode::UUID => parse_uuid_str(s).map_err(|_| KeyLitError::BadUuid),
        BoundLit::Str(s) if tc.is_temporal() => crate::types::temporal_literal(tc, s)
            .ok()
            .and_then(|v| pack_pk_value(tc, v as i128))
            .ok_or(KeyLitError::BadTemporal),
        BoundLit::Str(_) => Err(KeyLitError::NotNumeric),
        BoundLit::Num(n) => pack_num(tc, n).ok_or_else(|| {
            // A PK column's type is PK-eligible: an integer scalar at some
            // width. On that domain "not signed" is "unsigned", which is the
            // half a negative literal can never land in.
            if n.is_negative() && !tc.is_signed_int() {
                KeyLitError::NegativeIntoUnsigned
            } else {
                KeyLitError::OutOfRange
            }
        }),
    }
}

impl Constant {
    /// This constant as a [`BoundLit`], so a VALUES cell takes the bound-literal
    /// key rule rather than re-spelling the `Str`-vs-`Num` dispatch.
    fn bound_lit(&self) -> Option<BoundLit<'_>> {
        Some(match &self.lit {
            BExpr::LitInt(v) => BoundLit::Num(NumLit::Small(if self.negated { -(*v as i128) } else { *v as i128 })),
            BExpr::LitWide(s) => BoundLit::Num(NumLit::Wide(s, self.negated)),
            BExpr::LitStr(s) => BoundLit::Str(s),
            _ => return None,
        })
    }

    /// This constant as the packed key/cell of a column of type `tc`: the low
    /// `tc.wire_stride()` bytes are the column's native LE image.
    pub(crate) fn key_packed(&self, tc: TypeCode) -> Result<u128, KeyLitError> {
        bound_key_literal(self.bound_lit().ok_or(KeyLitError::NotNumeric)?, tc)
    }
}

/// One PK column constant packed, under the PK slot's own error wording.
fn parse_one_pk_literal(c: &Constant, tc: TypeCode, col_name: &str) -> Result<u128, GnitzSqlError> {
    c.key_packed(tc).map_err(|e| {
        GnitzSqlError::Bind(match e {
            KeyLitError::NegativeIntoUnsigned => {
                format!("PK column '{col_name}' of type {tc:?} does not accept negative literals")
            }
            KeyLitError::OutOfRange => format!("PK column '{col_name}' value is not a valid {tc:?}: {c}"),
            KeyLitError::BadUuid => format!("PK column '{col_name}' value is not a valid UUID: {c}"),
            KeyLitError::BadTemporal => format!("PK column '{col_name}' value is not a valid date or timestamp: {c}"),
            KeyLitError::NotNumeric => format!("PK column '{col_name}' value must be a numeric literal"),
        })
    })
}

/// Where each INSERT row's PK comes from, resolved once per statement: drawn
/// from the sequence, or read out of the VALUES slots the row-invariant column
/// plan names.
pub(crate) enum PkPlan<'s> {
    /// A SERIAL PK: `base + row_i`, bounded by `max`, the column type's maximum.
    Serial { schema: &'s Schema, base: u64, max: i128 },
    /// A written PK: per PK column, in pk-list order, the VALUES slot it reads
    /// and the column it names.
    Written {
        schema: &'s Schema,
        cols: Vec<(usize, TypeCode, &'s str)>,
    },
}

impl<'s> PkPlan<'s> {
    /// `slot_of` is the INSERT's physical-column → VALUES-slot map, `None` where
    /// a column takes no user value. A written PK column always takes one.
    pub(crate) fn written(slot_of: &[Option<usize>], schema: &'s Schema) -> Result<Self, GnitzSqlError> {
        let cols = schema
            .pk_cols
            .iter()
            .map(|&pi| {
                let c = &schema.columns[pi as usize];
                let slot =
                    slot_of.get(pi as usize).copied().flatten().ok_or_else(|| {
                        GnitzSqlError::Bind(format!("PK column '{}' missing from INSERT row", c.name))
                    })?;
                Ok((slot, c.type_code, c.name.as_str()))
            })
            .collect::<Result<Vec<_>, GnitzSqlError>>()?;
        Ok(PkPlan::Written { schema, cols })
    }

    /// A SERIAL PK drawn from `base`. The column type's maximum is resolved here,
    /// once per statement, rather than per row at the exhaustion check.
    pub(crate) fn serial(schema: &'s Schema, base: u64, tc: TypeCode) -> Self {
        let max = FixedInt::from_type_code(tc)
            .expect("SERIAL underlying is a fixed int")
            .range()
            .1;
        PkPlan::Serial { schema, base, max }
    }

    /// Whether this statement's PKs are auto-assigned, which is what the arity
    /// error names when a row supplies one value too many.
    pub(crate) fn is_serial(&self) -> bool {
        matches!(self, PkPlan::Serial { .. })
    }

    /// Append row `row_i`'s primary key to `dst`: the next sequence value, or the
    /// written cells' native LE bytes copied into the tuple buffer.
    pub(crate) fn push(&self, row_i: usize, cells: &[Constant], dst: &mut PkColumn) -> Result<(), GnitzSqlError> {
        match self {
            PkPlan::Serial { schema, base, max } => {
                // An exhausted sequence is rejected client-side, so `Bind` like the
                // arity guard. The addition wraps only after ~1.8·10¹⁹ reserved ids.
                let id = base + row_i as u64;
                if id as i128 > *max {
                    return Err(GnitzSqlError::Bind(format!(
                        "SERIAL primary key exhausted: next value {id} exceeds the column type maximum {max}"
                    )));
                }
                dst.push_u128(schema, id as u128);
            }
            PkPlan::Written { schema, cols } => {
                // `PK_LIST_MAX_COLS < MAX_PK_COLUMNS`, so no per-row allocation; every
                // slot is in range because the arity guard pins `cells.len()` to the
                // same map these slots came from.
                let mut natives = [0u128; gnitz_wire::MAX_PK_COLUMNS];
                for (native, &(slot, tc, name)) in natives.iter_mut().zip(cols) {
                    *native = parse_one_pk_literal(&cells[slot], tc, name)?;
                }
                dst.push_natives(schema, &natives[..cols.len()]);
            }
        }
        Ok(())
    }
}

/// Extract the primary key from a VALUES row of written expressions. Test-only
/// convenience over [`PkPlan`] for rows with the identity slot map; the INSERT
/// path builds a hidden/SERIAL-aware map and binds the row once.
#[cfg(test)]
pub(crate) fn extract_pk_value(row: &[sqlparser::ast::Expr], schema: &Schema) -> Result<PkBuf, GnitzSqlError> {
    let slot_of: Vec<Option<usize>> = (0..row.len()).map(Some).collect();
    let cells = row
        .iter()
        .map(crate::ast_util::bind_constant)
        .collect::<Result<Vec<_>, _>>()?;
    let mut pks = PkColumn::empty_for_schema(schema);
    PkPlan::written(&slot_of, schema)?.push(0, &cells, &mut pks)?;
    Ok(pks.get_tuple(0))
}

#[cfg(test)]
#[path = "tests/pk_codec.rs"]
mod tests;
