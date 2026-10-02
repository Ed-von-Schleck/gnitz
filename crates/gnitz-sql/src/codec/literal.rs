//! What a literal means against a column type: [`place`] is the one rule, read
//! by seek keys, written cells and VM comparisons alike, and [`assign`] the
//! value a written cell stores.

use crate::ir::BExpr;
use gnitz_expr::calendar::MICROS_PER_DAY;
use gnitz_expr::place::{place_ratio, place_scaled, Round};
use gnitz_wire::decimal::parse_decimal_text;
use gnitz_wire::{ColType, FixedInt, TypeCode};

pub(crate) use gnitz_expr::place::{Compared, Placed};

/// A DATE/TIMESTAMP spelling as the integer a column of type `tc` stores; `None`
/// for text that spells no such value, and for any other type.
pub(crate) fn parse_temporal(tc: TypeCode, s: &str) -> Option<i64> {
    match tc {
        TypeCode::Date => gnitz_expr::calendar::parse_date(s).map(i64::from),
        TypeCode::Timestamp => gnitz_expr::calendar::parse_timestamp(s),
        _ => None,
    }
}

/// The refusal of string `s` as a spelling of type `ty`.
pub(crate) fn invalid_literal(ty: impl std::fmt::Display, s: &str) -> String {
    format!("invalid {ty} literal: '{s}'")
}

/// The value an assignment of `lit` into `ty` stores, as its native image. A
/// float column takes every numeric spelling as its binary64 value.
pub(crate) fn assign<R>(lit: &BExpr<R>, ty: ColType) -> Result<u128, String> {
    let v = match ty.tc {
        TypeCode::F64 => float_value(lit).map(|v| u128::from(v.to_bits())),
        // F32 rounds through binary64.
        TypeCode::F32 => float_value(lit).map(|v| u128::from((v as f32).to_bits())),
        _ => place(lit, ty)
            .map(|p| {
                p.stored()
                    .ok_or_else(|| format!("{ty} value out of range: {}", lit.literal_text()))
            })
            .transpose()?,
    };
    v.ok_or_else(|| match lit {
        BExpr::LitStr(s) => invalid_literal(ty, s),
        _ => format!("{} is not a {ty} value", lit.literal_text()),
    })
}

/// A numeric literal as a float column's value. A magnitude past `i128` is
/// included, which a DOUBLE holds and no integer parse would.
fn float_value<R>(lit: &BExpr<R>) -> Option<f64> {
    match lit {
        BExpr::LitInt(v) | BExpr::LitTemporal { v, .. } => Some(*v as f64),
        BExpr::LitFloat { v, .. } => Some(*v),
        BExpr::LitWide(n) if n.is_negative() => Some(-(n.mag as f64)),
        BExpr::LitWide(n) => Some(n.mag as f64),
        _ => None,
    }
}

/// `lit` among the values of `ty`; `None` when it spells no value of the type:
/// NULL, a string that is not one of the type's spellings, a fraction for a
/// DATE/TIMESTAMP or a 16-byte type, and anything for a type not stored as an
/// integer.
pub(crate) fn place<R>(lit: &BExpr<R>, ty: ColType) -> Option<Placed> {
    let tc = ty.tc;
    if !tc.is_pk_eligible() {
        return None;
    }
    let day = i128::from(MICROS_PER_DAY);
    let (v, s) = match (lit, tc) {
        (BExpr::LitStr(s), TypeCode::UUID) => return gnitz_wire::parse_uuid(s).map(Placed::At),
        (BExpr::LitStr(s), TypeCode::Date | TypeCode::Timestamp) => (parse_temporal(tc, s)?.into(), 0),
        (BExpr::LitStr(s), TypeCode::Decimal) => parse_decimal_text(s)?,
        (BExpr::LitStr(_), _) => return None,
        (BExpr::LitTemporal { tc: TypeCode::Timestamp, v }, TypeCode::Date) => {
            return Some(place_ratio(FixedInt::I32, i128::from(*v), day, Round::Floor));
        }
        (BExpr::LitTemporal { tc: TypeCode::Date, v }, TypeCode::Timestamp) => (i128::from(*v) * day, 0),
        // A temporal literal into a numeric column is its storage integer.
        (BExpr::LitTemporal { v, .. }, _) => (i128::from(*v), 0),
        // Past `i128`, only a U128 or UUID holds a value.
        (BExpr::LitWide(n), _) if n.to_i128().is_none() => {
            return Some(match n.is_negative() {
                true => Placed::Below { nearest: None },
                false if matches!(tc, TypeCode::U128 | TypeCode::UUID) => Placed::At(n.mag),
                false => Placed::Above { nearest: None },
            });
        }
        _ => lit.decimal_literal()?,
    };
    match FixedInt::from_type_code(tc) {
        Some(_) if s > 0 && tc.is_temporal() => None,
        Some(fi) => Some(place_scaled(fi, v, s, ty.scale)),
        None if s > 0 => None,
        None if tc == TypeCode::I128 => Some(Placed::At(v as u128)),
        None if v < 0 => Some(Placed::Below { nearest: None }),
        None => Some(Placed::At(v as u128)),
    }
}

#[cfg(test)]
#[path = "tests/literal.rs"]
mod tests;
