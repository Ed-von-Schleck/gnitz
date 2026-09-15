//! Literal parsing + PK/seek key packing — the partition-routing source of
//! truth.
//!
//! [`bound_key_literal`] is the one literal→key rule every INSERT and SEEK PK
//! literal flows through, so the master cannot route an INSERT and a DELETE for
//! the same key to different workers.
//!
//! One literal shape reaches those packers: the bound [`BoundLit`], read out of
//! a `BoundExpr` by the WHERE recognizers and out of a [`Constant`] by INSERT
//! VALUES.

use crate::ast_util::Constant;
use crate::error::GnitzSqlError;
use crate::ir::{BExpr, BoundExpr, NumLit};
#[cfg(test)]
use gnitz_core::PkBuf;
use gnitz_core::{ColType, ColumnDef, FixedInt, PkColumn, Schema, TypeCode};
use gnitz_wire::decimal::{parse_decimal_text, rescale};

pub(crate) fn parse_uuid_str(s: &str) -> Result<u128, GnitzSqlError> {
    gnitz_wire::parse_uuid(s).ok_or_else(|| GnitzSqlError::Bind(format!("invalid UUID literal: {s:?}")))
}

// ---------------------------------------------------------------------------
// The bound-literal seam
// ---------------------------------------------------------------------------

/// The one seam from a bound numeric literal to a [`NumLit`].
pub(crate) fn bound_num_literal(e: &BoundExpr) -> Option<NumLit> {
    match e {
        BExpr::LitWide(n) => Some(*n),
        e => e.int_literal().map(|v| NumLit::of_i128(v.into())),
    }
}

/// A numeric literal as the packed key of a column of type `tc`; `None` when the
/// type does not hold it, never a wrapped value.
pub(crate) fn pack_num(tc: TypeCode, lit: NumLit) -> Option<u128> {
    match tc {
        TypeCode::U128 | TypeCode::UUID => (!lit.is_negative()).then_some(lit.mag),
        // I128 (the internal join-key promotion type) is two's complement at 16 bytes.
        TypeCode::I128 => Some(lit.to_i128()? as u128),
        _ => {
            let fi = FixedInt::from_type_code(tc)?;
            let v = lit.to_i128()?;
            let (min, max) = fi.range();
            (min <= v && v <= max).then(|| fi.pack(v))
        }
    }
}

/// A bound literal accepted for a seek/range key: a numeric value + sign, or a
/// string (a single-quoted UUID).
pub(crate) enum BoundLit<'e> {
    Num(NumLit),
    Str(&'e str),
}

fn bound_literal(e: &BoundExpr) -> Option<BoundLit<'_>> {
    if let Some(n) = bound_num_literal(e) {
        return Some(BoundLit::Num(n));
    }
    if let BExpr::LitStr(s) = e {
        return Some(BoundLit::Str(s));
    }
    None
}

/// [`bound_literal`] as a key of column `col`: a DECIMAL column reads an
/// integer or float literal as the integer it stores, when the literal is exact
/// at the column's scale — `d = 1.005` on a scale-2 column names no stored
/// value, so it is no key and stays a filter.
pub(crate) fn col_key_literal<'e>(e: &'e BoundExpr, col: &ColumnDef) -> Option<BoundLit<'e>> {
    if col.type_code != TypeCode::Decimal {
        return bound_literal(e);
    }
    Some(BoundLit::Num(NumLit::of_i128(e.exact_decimal(col.scale)?.into())))
}

/// Why a bound literal is not a key for a column of a given type — classified
/// rather than swallowed, so the INSERT path can name the column and the offence.
pub(crate) enum KeyLitError {
    /// A negative literal against an unsigned column.
    NegativeIntoUnsigned,
    /// A numeric literal outside the column type's range.
    OutOfRange,
    /// A string literal that does not spell a value of the column's type.
    NotOfType,
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
        BoundLit::Str(s) if tc == TypeCode::UUID => parse_uuid_str(s).map_err(|_| KeyLitError::NotOfType),
        BoundLit::Str(s) if tc.is_temporal() => {
            let v = crate::types::temporal_literal(tc, s).map_err(|_| KeyLitError::NotOfType)?;
            Ok(pack_num(tc, NumLit::of_i128(v as i128))
                .expect("temporal_literal's value always fits tc's storage width"))
        }
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
        if let Some(v) = self.lit.int_literal() {
            let v = i128::from(v);
            return Some(BoundLit::Num(NumLit::of_i128(if self.negated { -v } else { v })));
        }
        Some(match &self.lit {
            BExpr::LitWide(n) => BoundLit::Num(NumLit { mag: n.mag, neg: self.negated }),
            BExpr::LitStr(s) => BoundLit::Str(s),
            _ => return None,
        })
    }

    /// This constant as the packed key/cell of a column of type `ty`: the low
    /// `wire_stride` bytes are the column's native LE image. A DECIMAL column
    /// stores the constant at its scale, a longer fraction rounded — the
    /// written-cell rule, where a seek key must be exact.
    pub(crate) fn key_packed(&self, ty: ColType) -> Result<u128, KeyLitError> {
        if ty.is_decimal() {
            let sign = if self.negated { -1 } else { 1 };
            let (v, s) = match &self.lit {
                BExpr::LitStr(text) => parse_decimal_text(text).ok_or(KeyLitError::NotOfType)?,
                BExpr::LitInt(_) | BExpr::LitFloat { .. } | BExpr::LitWide(_) => {
                    self.lit.decimal_literal().ok_or(KeyLitError::OutOfRange)?
                }
                _ => return Err(KeyLitError::NotNumeric),
            };
            let v = rescale(sign * v, s, ty.scale).ok_or(KeyLitError::OutOfRange)?;
            return Ok(FixedInt::I64.pack(v as i128));
        }
        bound_key_literal(self.bound_lit().ok_or(KeyLitError::NotNumeric)?, ty.tc)
    }
}

/// One PK column constant packed, under the PK slot's own error wording.
fn parse_one_pk_literal(c: &Constant, ty: ColType, col_name: &str) -> Result<u128, GnitzSqlError> {
    c.key_packed(ty).map_err(|e| {
        GnitzSqlError::Bind(match e {
            KeyLitError::NegativeIntoUnsigned => {
                format!("PK column '{col_name}' of type {ty} does not accept negative literals")
            }
            KeyLitError::OutOfRange | KeyLitError::NotOfType => {
                format!("PK column '{col_name}' value is not a valid {ty}: {c}")
            }
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
        cols: Vec<(usize, ColType, &'s str)>,
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
                Ok((slot, c.ty(), c.name.as_str()))
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
                for (native, &(slot, ty, name)) in natives.iter_mut().zip(cols) {
                    *native = parse_one_pk_literal(&cells[slot], ty, name)?;
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
