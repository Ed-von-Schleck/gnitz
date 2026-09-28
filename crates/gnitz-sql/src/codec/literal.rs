//! What a literal means against a column type: [`place`] is the one rule, read
//! by seek keys, written cells and VM comparisons alike.

use crate::ir::{BExpr, NumLit};
use gnitz_core::{ColType, FixedInt, TypeCode};
use gnitz_expr::calendar::MICROS_PER_DAY;
use gnitz_wire::decimal::parse_decimal_text;

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
    /// Strictly between the adjacent values `lo` and `hi`. `nearest` is the one
    /// an assignment stores: a numeric type rounds half away from zero, as a
    /// DECIMAL→integer CAST does; DATE takes a TIMESTAMP's day.
    Between { lo: u128, hi: u128, nearest: u128 },
    /// Below every value. `nearest` is set when rounding an inexact literal
    /// still lands on the type's minimum (`-128.4` into an I8).
    Below { nearest: Option<u128> },
    /// Above every value; `nearest` as for `Below`.
    Above { nearest: Option<u128> },
}

impl Placed {
    /// Whether the literal lies outside the type's range.
    pub(crate) fn is_outside(self) -> bool {
        matches!(self, Placed::Below { .. } | Placed::Above { .. })
    }
}

/// The value an assignment of `lit` into `ty` stores, as its native image: the
/// placed value, or the nearest one when rounding lands inside the type.
pub(crate) fn assign<R>(lit: &BExpr<R>, ty: ColType) -> Result<u128, String> {
    match place(lit, ty) {
        Some(
            Placed::At(v)
            | Placed::Between { nearest: v, .. }
            | Placed::Below { nearest: Some(v) }
            | Placed::Above { nearest: Some(v) },
        ) => Ok(v),
        Some(Placed::Below { .. } | Placed::Above { .. }) => {
            Err(format!("{ty} value out of range: {}", lit.literal_text()))
        }
        None => Err(match lit {
            BExpr::LitStr(s) => invalid_literal(ty, s),
            _ => format!("{} is not a {ty} value", lit.literal_text()),
        }),
    }
}

/// `lit` among the values of `ty`; `None` when it spells no value of the type:
/// NULL, a string that is not one of the type's spellings, a fraction for a
/// DATE/TIMESTAMP or a 16-byte type.
pub(crate) fn place<R>(lit: &BExpr<R>, ty: ColType) -> Option<Placed> {
    let tc = ty.tc;
    let day = i128::from(MICROS_PER_DAY);
    match (lit, tc) {
        (BExpr::LitStr(s), TypeCode::UUID) => gnitz_wire::parse_uuid(s).map(Placed::At),
        (BExpr::LitStr(s), TypeCode::Date | TypeCode::Timestamp) => {
            let fi = FixedInt::from_type_code(tc)?;
            Some(place_ratio(fi, parse_temporal(tc, s)?.into(), 1, Round::Floor))
        }
        (BExpr::LitTemporal { tc: TypeCode::Date, v }, TypeCode::Timestamp) => {
            Some(place_ratio(FixedInt::I64, i128::from(*v) * day, 1, Round::Floor))
        }
        (BExpr::LitTemporal { tc: TypeCode::Timestamp, v }, TypeCode::Date) => {
            Some(place_ratio(FixedInt::I32, i128::from(*v), day, Round::Floor))
        }
        (BExpr::LitStr(s), TypeCode::Decimal) => {
            let (v, s) = parse_decimal_text(s)?;
            Some(place_scaled(FixedInt::I64, v, s, ty.scale))
        }
        (BExpr::LitStr(_), _) => None,
        (_, TypeCode::U128 | TypeCode::UUID | TypeCode::I128) => {
            let n = match lit {
                BExpr::LitWide(n) => *n,
                _ => NumLit::of_i128(lit.int_literal()?.into()),
            };
            Some(match n.to_i128() {
                Some(v) if tc == TypeCode::I128 => Placed::At(v as u128),
                _ if n.is_negative() => Placed::Below { nearest: None },
                _ if tc == TypeCode::I128 => Placed::Above { nearest: None },
                _ => Placed::At(n.mag),
            })
        }
        _ => {
            let fi = FixedInt::from_type_code(tc)?;
            let scale = if tc == TypeCode::Decimal { ty.scale } else { 0 };
            let (v, s) = match lit {
                // A temporal literal into a numeric column is its storage integer.
                BExpr::LitTemporal { v, .. } => (i128::from(*v), 0),
                BExpr::LitWide(n) if n.to_i128().is_none() => {
                    return Some(match n.is_negative() {
                        true => Placed::Below { nearest: None },
                        false => Placed::Above { nearest: None },
                    })
                }
                BExpr::LitFloat { .. } if tc.is_temporal() => return None,
                _ => lit.decimal_literal()?,
            };
            Some(place_scaled(fi, v, s, scale))
        }
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
    let up = rem != 0 && round == Round::HalfAwayFromZero && (rem > den - rem || (rem == den - rem && num > 0));
    let near = lo + i128::from(up);
    let nearest = (min..=max).contains(&near).then(|| fi.pack(near));
    if lo < min {
        Placed::Below { nearest }
    } else if lo > max || (lo == max && rem != 0) {
        Placed::Above { nearest }
    } else if rem == 0 {
        Placed::At(fi.pack(lo))
    } else {
        Placed::Between {
            lo: fi.pack(lo),
            hi: fi.pack(lo + 1),
            nearest: fi.pack(near),
        }
    }
}

#[cfg(test)]
#[path = "tests/literal.rs"]
mod tests;
