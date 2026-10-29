//! What a literal means against a column type: [`place`] is the one rule, read
//! by seek keys, written cells and VM comparisons alike, and [`assign`] the
//! value a written cell stores. The placement of a decimal number among a
//! fixed-width integer type's values is here too, and the Python driver's
//! DECIMAL push reads it.

use crate::ir::BExpr;
use gnitz_expr::CmpOp;
use gnitz_wire::{parse_decimal_text, ColType, FixedInt, TypeCode, MAX_DECIMAL_TEXT_SCALE, MICROS_PER_DAY};

/// The text spellings of a BOOLEAN, PostgreSQL's: case and surrounding
/// whitespace are ignored.
pub fn parse_bool(s: &str) -> Option<bool> {
    match s.trim().to_ascii_lowercase().as_str() {
        "true" | "t" | "yes" | "y" | "on" | "1" => Some(true),
        "false" | "f" | "no" | "n" | "off" | "0" => Some(false),
        _ => None,
    }
}

/// Where a literal falls among the values of a column type.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Placed {
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
    pub fn is_outside(self) -> bool {
        matches!(self, Placed::Below { .. } | Placed::Above { .. })
    }

    /// The value an assignment stores: the literal's own, or `nearest` where
    /// rounding lands inside the type. `None` when even that lies outside.
    pub fn stored(self) -> Option<u128> {
        match self {
            Placed::At(v)
            | Placed::Between { nearest: v, .. }
            | Placed::Below { nearest: Some(v) }
            | Placed::Above { nearest: Some(v) } => Some(v),
            Placed::Below { nearest: None } | Placed::Above { nearest: None } => None,
        }
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

#[derive(Clone, Copy, PartialEq, Eq)]
enum Round {
    Floor,
    HalfAwayFromZero,
}

/// `v · 10^-s` among the values of `fi` read at scale `scale`.
pub fn place_scaled(fi: FixedInt, v: i128, s: u8, scale: u8) -> Placed {
    match scale.checked_sub(s) {
        Some(up) => match 10i128.checked_pow(u32::from(up)).and_then(|p| v.checked_mul(p)) {
            Some(n) => place_ratio(fi, n, 1, Round::Floor),
            None if v < 0 => Placed::Below { nearest: None },
            None => Placed::Above { nearest: None },
        },
        None => match 10i128.checked_pow(u32::from(s - scale)) {
            Some(den) => place_ratio(fi, v, den, Round::HalfAwayFromZero),
            // `|v| < 10^39 <= den`: strictly inside (-0.5, 0.5), as ±0.1 is.
            None => place_ratio(fi, v.signum(), 10, Round::HalfAwayFromZero),
        },
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

/// A DATE/TIMESTAMP spelling as the integer a column of type `tc` stores; `None`
/// for text that spells no such value, and for any other type.
pub(crate) fn parse_temporal(tc: TypeCode, s: &str) -> Option<i64> {
    match tc {
        TypeCode::Date => gnitz_wire::parse_date(s).map(i64::from),
        TypeCode::Timestamp => gnitz_wire::parse_timestamp(s),
        _ => None,
    }
}

/// The refusal of string `s` as a spelling of type `ty`.
pub(crate) fn invalid_literal(ty: ColType, s: &str) -> String {
    format!("invalid {ty} literal: '{s}'")
}

/// The value an assignment of `lit` into `ty` stores, as its native image. A
/// float column takes every numeric spelling as its binary64 value, an F32 one
/// that value narrowed.
pub(crate) fn assign<R>(lit: &BExpr<R>, ty: ColType) -> Result<u128, String> {
    let stored = match ty.tc {
        // SQL spells no infinity, so an infinite value is a literal past the float type.
        TypeCode::F64 => lit
            .float_literal()
            .map(|v| v.is_finite().then(|| u128::from(v.to_bits()))),
        TypeCode::F32 => lit
            .float_literal()
            .map(|v| v as f32)
            .map(|f| f.is_finite().then(|| u128::from(f.to_bits()))),
        _ => place(lit, ty).map(Placed::stored),
    };
    match (stored, lit) {
        (Some(Some(v)), _) => Ok(v),
        (Some(None), _) => Err(format!("{ty} value out of range: {}", lit.literal_text())),
        (None, BExpr::LitStr(s)) => Err(invalid_literal(ty, s)),
        (None, _) => Err(format!("{} is not a {ty} value", lit.literal_text())),
    }
}

/// `lit` among the values of `ty`; `None` when it spells no value of the type:
/// NULL, a string that is not one of the type's spellings, a fraction for a
/// DATE/TIMESTAMP or a 16-byte type, a number for a BOOLEAN or a BOOLEAN for
/// anything else, and anything for a type not stored as an integer.
pub(crate) fn place<R>(lit: &BExpr<R>, ty: ColType) -> Option<Placed> {
    let tc = ty.tc;
    if !tc.is_pk_eligible() {
        return None;
    }
    let day = i128::from(MICROS_PER_DAY);
    let (v, s) = match (lit, tc) {
        (BExpr::LitBool(b), TypeCode::Bool) => return Some(Placed::At(u128::from(*b))),
        (BExpr::LitStr(s), TypeCode::Bool) => return parse_bool(s).map(|b| Placed::At(u128::from(b))),
        (_, TypeCode::Bool) => return None,
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
        None if v < 0 && tc != TypeCode::I128 => Some(Placed::Below { nearest: None }),
        None => Some(Placed::At(v as u128)),
    }
}

/// The decimal a SQL numeric literal spells, exponent included, as
/// `(unscaled, scale)` at its smallest exact scale — `1.50` is `(15, 1)`.
/// `None` past `i128` or [`MAX_DECIMAL_TEXT_SCALE`] fractional digits.
pub(crate) fn decimal_of_number_text(s: &str) -> Option<(i128, u8)> {
    let (mantissa, exp) = match s.split_once(['e', 'E']) {
        Some((m, e)) => (m, e.parse::<i32>().ok()?),
        None => (s, 0),
    };
    let (mut v, scale) = parse_decimal_text(mantissa)?;
    let mut scale = i32::from(scale).checked_sub(exp)?;
    if scale < 0 {
        v = v.checked_mul(10i128.checked_pow(scale.unsigned_abs())?)?;
        scale = 0;
    }
    while scale > 0 && v % 10 == 0 {
        v /= 10;
        scale -= 1;
    }
    Some((v, u8::try_from(scale).ok().filter(|s| *s <= MAX_DECIMAL_TEXT_SCALE)?))
}

#[cfg(test)]
#[path = "tests/literal.rs"]
mod tests;
