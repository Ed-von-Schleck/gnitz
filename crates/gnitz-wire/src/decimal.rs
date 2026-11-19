//! The `DECIMAL` value codec. A column of scale `s` stores an `I64` holding the
//! value times `10^s`; everything here converts between that integer and the
//! text of the number it means, in both directions.

use crate::VALUE_TEXT_MAX;

/// The largest scale a column may declare: `i64` holds 18 full decimal digits.
pub const MAX_DECIMAL_SCALE: u8 = 18;

/// The most fractional digits a decimal text may spell: `i128` holds 38 full
/// decimal digits.
pub const MAX_DECIMAL_TEXT_SCALE: u8 = 38;

/// The digits and scale a plain decimal spelling carries — an optional sign,
/// digits, an optional fraction — as `(unscaled, scale)`. `None` for anything
/// else, an exponent included, for a value past `i128`, and for more than
/// [`MAX_DECIMAL_TEXT_SCALE`] fractional digits.
pub fn parse_decimal_text(s: &str) -> Option<(i128, u8)> {
    let (neg, body) = match s.as_bytes().first()? {
        b'-' => (true, &s[1..]),
        b'+' => (false, &s[1..]),
        _ => (false, s),
    };
    let (int, frac) = body.split_once('.').unwrap_or((body, ""));
    if int.is_empty() && frac.is_empty() || frac.len() > MAX_DECIMAL_TEXT_SCALE as usize {
        return None;
    }
    let mut v: i128 = 0;
    for b in int.bytes().chain(frac.bytes()) {
        v = v.checked_mul(10)?.checked_add((b as char).to_digit(10)?.into())?;
    }
    Some((if neg { -v } else { v }, frac.len() as u8))
}

/// The text a value at scale `scale` prints as — exactly `scale` fractional
/// digits, so a column's values line up and read back through
/// [`parse_decimal_text`] at that scale — written into the tail of `buf` and
/// borrowed from it. Panics on a `scale` past [`MAX_DECIMAL_SCALE`], which no
/// admissible column type has.
pub fn write_decimal(v: i64, scale: u8, buf: &mut [u8; VALUE_TEXT_MAX]) -> &str {
    std::str::from_utf8(scaled_bytes(v.unsigned_abs(), v < 0, scale, buf)).expect("digits, point and sign")
}

/// The longest text [`scaled_bytes`] produces: a sign, 20 digits (all of a
/// `u64`, or the `scale + 1` padded digits of a scale up to
/// [`MAX_DECIMAL_SCALE`]) and a point.
pub(crate) const SCALED_TEXT_MAX: usize = 22;

/// The ASCII text of `magnitude`, negated where `neg`, at scale `scale` — an
/// integer's at scale 0 — written into the tail of `buf` and borrowed from it:
/// [`write_decimal`]'s bytes, unvalidated for a per-row caller that stores
/// bytes. Several times cheaper than `core::fmt` (`int_to_str` of
/// `expr_kernel_bench`). Panics on a `scale` past [`MAX_DECIMAL_SCALE`].
pub fn scaled_bytes(magnitude: u64, neg: bool, scale: u8, buf: &mut [u8; VALUE_TEXT_MAX]) -> &[u8] {
    assert!(
        scale <= MAX_DECIMAL_SCALE,
        "DECIMAL scale {scale} past {MAX_DECIMAL_SCALE}"
    );
    let scale = scale as usize;
    let (mut m, mut i, mut n) = (magnitude, buf.len(), 0);
    // At least `scale + 1` digits, so the point always has a digit in front of it.
    while m != 0 || n <= scale {
        if n == scale && scale > 0 {
            i -= 1;
            buf[i] = b'.';
        }
        i -= 1;
        buf[i] = b'0' + (m % 10) as u8;
        m /= 10;
        n += 1;
    }
    if neg {
        i -= 1;
        buf[i] = b'-';
    }
    &buf[i..]
}

#[cfg(test)]
#[path = "tests/decimal.rs"]
mod tests;
