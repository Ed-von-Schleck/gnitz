//! Where a decimal number falls among the values of a fixed-width integer type:
//! on one, between two, or outside. A written cell, a seek key and a comparison
//! all read a literal through it, in SQL and in the Python client.

use crate::CmpOp;
use gnitz_wire::FixedInt;

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
pub enum Compared {
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
    pub fn compare(self, cmp: CmpOp) -> Compared {
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
pub enum Round {
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
pub fn place_ratio(fi: FixedInt, num: i128, den: i128, round: Round) -> Placed {
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
#[path = "tests/place.rs"]
mod tests;
