//! What a literal means against a column type: [`place`] is the one rule, read
//! by seek keys, written cells and VM comparisons alike, and [`assign`] the
//! value a written cell stores.

use crate::ir::BExpr;
use gnitz_expr::calendar::MICROS_PER_DAY;
use gnitz_expr::CmpOp;
use gnitz_wire::decimal::parse_decimal_text;
use gnitz_wire::{ColType, FixedInt, TypeCode};

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

/// Where a literal falls among the values of a column type.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Placed {
    /// The value it spells, as the column's native image (low `wire_stride`
    /// bytes, little-endian).
    At(u128),
    /// Strictly between the value `lo` and the next one. `nearest` is the one an
    /// assignment stores: a numeric type rounds half away from zero, as a
    /// DECIMAL→integer CAST does; DATE takes a TIMESTAMP's day.
    Between { lo: u128, nearest: u128 },
    /// Below every value. `nearest` is set when rounding an inexact literal
    /// still lands on the type's minimum (`-128.4` into an I8).
    Below { nearest: Option<u128> },
    /// Above every value; `nearest` as for `Below`.
    Above { nearest: Option<u128> },
}

/// `x CMP lit` over the values of `x`'s type, as [`Placed::compare`] rewrites it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Compared {
    /// The same verdict for every value.
    Always(bool),
    /// `x CMP v` for one value `v` of the type, as its native image.
    Cmp(CmpOp, u128),
}

impl Placed {
    /// Whether the literal lies outside the type's range.
    pub(crate) fn is_outside(self) -> bool {
        matches!(self, Placed::Below { .. } | Placed::Above { .. })
    }

    /// `x cmp lit`, where this is `lit` placed among the values of `x`'s type,
    /// as a comparison against one of those values or a constant verdict. Seek
    /// keys and VM comparisons both read it.
    pub(crate) fn compare(self, cmp: CmpOp) -> Compared {
        use CmpOp::*;
        match self {
            Placed::At(v) => Compared::Cmp(cmp, v),
            Placed::Between { lo, .. } => match cmp {
                Eq | Ne => Compared::Always(cmp == Ne),
                Lt | Le => Compared::Cmp(Le, lo),
                Gt | Ge => Compared::Cmp(Gt, lo),
            },
            Placed::Below { .. } => Compared::Always(matches!(cmp, Ne | Gt | Ge)),
            Placed::Above { .. } => Compared::Always(matches!(cmp, Ne | Lt | Le)),
        }
    }
}

/// The value an assignment of `lit` into `ty` stores, as its native image: the
/// placed value, or the nearest one when rounding lands inside the type. A
/// float column takes every numeric spelling as its binary64 value.
pub(crate) fn assign<R>(lit: &BExpr<R>, ty: ColType) -> Result<u128, String> {
    let v = match ty.tc {
        TypeCode::F64 => float_value(lit).map(|v| u128::from(v.to_bits())),
        // F32 rounds through binary64, as the Python client's `ZSetBatch.append`
        // does for a float, so the two ingest paths agree bit for bit.
        TypeCode::F32 => float_value(lit).map(|v| u128::from((v as f32).to_bits())),
        _ => match place(lit, ty) {
            Some(
                Placed::At(v)
                | Placed::Between { nearest: v, .. }
                | Placed::Below { nearest: Some(v) }
                | Placed::Above { nearest: Some(v) },
            ) => Some(v),
            Some(Placed::Below { .. } | Placed::Above { .. }) => {
                return Err(format!("{ty} value out of range: {}", lit.literal_text()))
            }
            None => None,
        },
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

#[derive(Clone, Copy, PartialEq, Eq)]
enum Round {
    Floor,
    HalfAwayFromZero,
}

/// `v · 10^-s` among the values of `fi` read at scale `scale`.
fn place_scaled(fi: FixedInt, v: i128, s: u8, scale: u8) -> Placed {
    match scale.checked_sub(s) {
        Some(up) => match 10i128.checked_pow(u32::from(up)).and_then(|p| v.checked_mul(p)) {
            Some(n) => place_ratio(fi, n, 1, Round::Floor),
            None if v < 0 => Placed::Below { nearest: None },
            None => Placed::Above { nearest: None },
        },
        None => {
            let den = 10i128
                .checked_pow(u32::from(s - scale))
                .expect("a decimal literal has at most 38 fractional digits");
            place_ratio(fi, v, den, Round::HalfAwayFromZero)
        }
    }
}

/// The rational `num / den` (`den > 0`, in units of the stored integer) among
/// `fi`'s values.
fn place_ratio(fi: FixedInt, num: i128, den: i128, round: Round) -> Placed {
    let (min, max) = fi.range();
    let (lo, rem) = (num.div_euclid(den), num.rem_euclid(den));
    let up = round == Round::HalfAwayFromZero && (rem > den - rem || (rem == den - rem && num > 0));
    let near = lo + i128::from(up);
    let nearest = (min..=max).contains(&near).then(|| fi.pack(near));
    if lo < min {
        Placed::Below { nearest }
    } else if lo > max || (lo == max && rem != 0) {
        Placed::Above { nearest }
    } else if rem == 0 {
        Placed::At(fi.pack(lo))
    } else {
        Placed::Between { lo: fi.pack(lo), nearest: fi.pack(near) }
    }
}

#[cfg(test)]
#[path = "tests/literal.rs"]
mod tests;
