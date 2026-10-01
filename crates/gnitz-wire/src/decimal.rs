//! The `DECIMAL` value codec. A column of scale `s` stores an `I64` holding the
//! value times `10^s`; everything here is the seam between that integer and the
//! number it means — text in both directions.

/// The largest scale a column may declare: `i64` holds 18 full decimal digits.
pub const MAX_DECIMAL_SCALE: u8 = 18;

/// `10^scale`, for `scale <= MAX_DECIMAL_SCALE`.
#[inline]
pub const fn pow10(scale: u8) -> i64 {
    debug_assert!(scale <= MAX_DECIMAL_SCALE);
    10i64.pow(scale as u32)
}

/// The digits and scale a plain decimal spelling carries — an optional sign,
/// digits, an optional fraction — as `(unscaled, scale)`. `None` for anything
/// else, an exponent included, for a value past `i128`, and for more than 38
/// fractional digits.
pub fn parse_decimal_text(s: &str) -> Option<(i128, u8)> {
    let (neg, body) = match s.as_bytes().first()? {
        b'-' => (true, &s[1..]),
        b'+' => (false, &s[1..]),
        _ => (false, s),
    };
    let (int, frac) = body.split_once('.').unwrap_or((body, ""));
    if int.is_empty() && frac.is_empty() || frac.len() > 38 {
        return None;
    }
    let mut v: i128 = 0;
    for b in int.bytes().chain(frac.bytes()) {
        v = v.checked_mul(10)?.checked_add((b as char).to_digit(10)?.into())?;
    }
    Some((if neg { -v } else { v }, frac.len() as u8))
}

/// The decimal a SQL numeric literal spells, exponent included, as
/// `(unscaled, scale)` at its smallest exact scale — `1.50` is `(15, 1)`.
/// `None` past `i128` or 38 fractional digits.
pub fn decimal_of_number_text(s: &str) -> Option<(i128, u8)> {
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
    Some((v, u8::try_from(scale).ok().filter(|s| *s <= 38)?))
}

/// The text a value at scale `scale` prints as — exactly `scale` fractional
/// digits, so a column's values line up and read back through
/// [`parse_decimal_text`] at that scale.
pub fn format_decimal(v: i128, scale: u8) -> String {
    let scale = scale as usize;
    let digits = v.unsigned_abs().to_string();
    let mut out = String::with_capacity(digits.len().max(scale + 1) + 2);
    if v < 0 {
        out.push('-');
    }
    // Padded to `scale + 1` digits, so the point always has a digit in front of it.
    for _ in digits.len()..=scale {
        out.push('0');
    }
    out.push_str(&digits);
    if scale > 0 {
        out.insert(out.len() - scale, '.');
    }
    out
}

/// The decimal a float spells at the scale of its shortest round-trip print.
/// `None` when it is not finite or has more digits than `i64` holds.
pub fn decimal_of_f64(v: f64) -> Option<(i64, u8)> {
    if !v.is_finite() {
        return None;
    }
    let (u, scale) = parse_decimal_text(&format!("{v}"))?;
    if scale > MAX_DECIMAL_SCALE {
        return None;
    }
    Some((i64::try_from(u).ok()?, scale))
}

#[cfg(test)]
#[path = "tests/decimal.rs"]
mod tests;
