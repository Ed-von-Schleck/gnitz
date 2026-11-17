//! The `DATE` and `TIMESTAMP` value codec: a `DATE` is days since 1970-01-01
//! and a `TIMESTAMP` microseconds since 1970-01-01 00:00:00, both proleptic
//! Gregorian with no time zone. Here are the civil-calendar arithmetic between
//! those counts and a civil date, and the text forms each type parses from and
//! prints as.

use crate::VALUE_TEXT_MAX;

pub const MICROS_PER_SEC: i64 = 1_000_000;
pub const MICROS_PER_MIN: i64 = 60 * MICROS_PER_SEC;
pub const MICROS_PER_HOUR: i64 = 60 * MICROS_PER_MIN;
pub const MICROS_PER_DAY: i64 = 24 * MICROS_PER_HOUR;

/// Days since the epoch of the civil date `y-m-d` (Hinnant's `days_from_civil`).
pub const fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (m as i64 + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d as i64 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// The civil date `(y, m, d)` of a day count (Hinnant's `civil_from_days`).
pub const fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = (if mp < 10 { mp + 3 } else { mp - 9 }) as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

const fn days_in_month(y: i64, m: u32) -> u32 {
    match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        _ => {
            if y % 4 == 0 && (y % 100 != 0 || y % 400 == 0) {
                29
            } else {
                28
            }
        }
    }
}

/// A run of ASCII digits as its value. Deliberately not `str::parse`, which
/// would accept a sign and so let `2024-+2-01` through the field splits below.
fn num(s: &str) -> Option<i64> {
    if s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    s.parse().ok()
}

/// `s` split on `sep` into exactly `N` fields, each a run of ASCII digits.
fn digit_fields<const N: usize>(s: &str, sep: char) -> Option<[i64; N]> {
    let mut it = s.split(sep);
    let mut out = [0i64; N];
    for slot in &mut out {
        *slot = num(it.next()?)?;
    }
    it.next().is_none().then_some(out)
}

/// `YYYY-MM-DD` as days since the epoch; an out-of-calendar date is `None`.
pub fn parse_date(s: &str) -> Option<i32> {
    let s = s.trim();
    let (neg, s) = s.strip_prefix('-').map_or((false, s), |r| (true, r));
    let [y, m, d] = digit_fields::<3>(s, '-')?;
    // An i32 year keeps `days_from_civil` far from overflow; the day-count cut below is the exact one.
    let y = i64::from(i32::try_from(if neg { -y } else { y }).ok()?);
    if !(1..=12).contains(&m) || d < 1 || d > days_in_month(y, m as u32) as i64 {
        return None;
    }
    i32::try_from(days_from_civil(y, m as u32, d as u32)).ok()
}

/// `YYYY-MM-DD[ |T]HH:MM[:SS[.ffffff]]`, or a bare date, as microseconds
/// since the epoch. No time-zone suffix is accepted.
pub fn parse_timestamp(s: &str) -> Option<i64> {
    let s = s.trim();
    let (date, time) = match s.find([' ', 'T']) {
        Some(i) => (&s[..i], Some(s[i + 1..].trim())),
        None => (s, None),
    };
    let days = parse_date(date)? as i64;
    let Some(time) = time else {
        return join_micros(days, 0, 0, 0, 0);
    };
    let (hms, frac) = match time.split_once('.') {
        Some((a, f)) => (a, Some(f)),
        None => (time, None),
    };
    // `HH:MM` and `HH:MM:SS` only; a bare hour or a fourth field is not a time.
    let (h, mi, sec) = match digit_fields::<3>(hms, ':') {
        Some([h, mi, s]) => (h, mi, s),
        None => {
            let [h, mi] = digit_fields::<2>(hms, ':')?;
            (h, mi, 0)
        }
    };
    if h > 23 || mi > 59 || sec > 59 {
        return None;
    }
    let micros = match frac {
        Some(f) if !f.is_empty() && f.len() <= 6 => num(f)? * 10i64.pow(6 - f.len() as u32),
        Some(_) => return None,
        None => 0,
    };
    join_micros(days, h as u32, mi as u32, sec as u32, micros as u32)
}

/// A day count as `YYYY-MM-DD`, [`parse_date`]'s inverse, written into `buf`
/// and borrowed from it. The count is read at a DATE's 32-bit storage width, so
/// a wider one spells the date of its low half. The year is at least four
/// characters wide, a sign included (`-001` is the year before year 0).
pub fn write_date(days: i64, buf: &mut [u8; VALUE_TEXT_MAX]) -> &str {
    std::str::from_utf8(date_bytes(days, buf)).expect("digits, hyphens")
}

/// [`write_date`]'s ASCII bytes, unvalidated for a per-row caller that stores
/// bytes.
pub fn date_bytes(days: i64, buf: &mut [u8; VALUE_TEXT_MAX]) -> &[u8] {
    let n = put_date(buf, i64::from(days as i32));
    &buf[..n]
}

/// A microsecond count as `YYYY-MM-DD HH:MM:SS`, with the fraction only as
/// long as it needs to be, [`parse_timestamp`]'s inverse, written into `buf`
/// and borrowed from it.
pub fn write_timestamp(micros: i64, buf: &mut [u8; VALUE_TEXT_MAX]) -> &str {
    std::str::from_utf8(timestamp_bytes(micros, buf)).expect("digits and separators")
}

/// [`write_timestamp`]'s ASCII bytes, unvalidated for a per-row caller that
/// stores bytes.
pub fn timestamp_bytes(micros: i64, buf: &mut [u8; VALUE_TEXT_MAX]) -> &[u8] {
    let (days, h, mi, s, us) = split_micros(micros);
    let mut n = put_date(buf, days);
    buf[n] = b' ';
    n = put2(buf, n + 1, h);
    buf[n] = b':';
    n = put2(buf, n + 1, mi);
    buf[n] = b':';
    n = put2(buf, n + 1, s);
    if us != 0 {
        buf[n] = b'.';
        n = put_digits(buf, n + 1, us.into(), 6);
        while buf[n - 1] == b'0' {
            n -= 1;
        }
    }
    &buf[..n]
}

/// `YYYY-MM-DD` of `days` at the head of `buf`; returns its length. Every day
/// count an `i32` or a [`split_micros`] of an `i64` yields fits.
fn put_date(buf: &mut [u8], days: i64) -> usize {
    let (y, m, d) = civil_from_days(days);
    let mut n = 0;
    if y < 0 {
        buf[0] = b'-';
        n = 1;
    }
    // Four characters wide with the sign, as `{y:04}` pads.
    n = put_digits(buf, n, y.unsigned_abs(), 4 - n);
    buf[n] = b'-';
    n = put2(buf, n + 1, m);
    buf[n] = b'-';
    put2(buf, n + 1, d)
}

/// `v`, below 100, as two digits at `buf[at..]`; returns the index past them.
fn put2(buf: &mut [u8], at: usize, v: u32) -> usize {
    debug_assert!(v < 100);
    buf[at..at + 2].copy_from_slice(&[b'0' + (v / 10) as u8, b'0' + (v % 10) as u8]);
    at + 2
}

/// `v`'s decimal digits at `buf[at..]`, zero-padded to at least `width`;
/// returns the index past the last.
fn put_digits(buf: &mut [u8], at: usize, v: u64, width: usize) -> usize {
    let len = v.checked_ilog10().map_or(1, |l| l as usize + 1);
    let end = at + len.max(width);
    let mut v = v;
    for slot in buf[at..end].iter_mut().rev() {
        *slot = b'0' + (v % 10) as u8;
        v /= 10;
    }
    end
}

/// A microsecond count as `(days, hour, minute, second, microsecond)`, the
/// day floored so a pre-epoch instant keeps a non-negative time of day.
pub const fn split_micros(us: i64) -> (i64, u32, u32, u32, u32) {
    let tod = us.rem_euclid(MICROS_PER_DAY);
    (
        us.div_euclid(MICROS_PER_DAY),
        (tod / MICROS_PER_HOUR) as u32,
        (tod % MICROS_PER_HOUR / MICROS_PER_MIN) as u32,
        (tod % MICROS_PER_MIN / MICROS_PER_SEC) as u32,
        (tod % MICROS_PER_SEC) as u32,
    )
}

/// [`split_micros`]' inverse: the microsecond count of a day count and a time
/// of day, `None` where it falls outside an `i64`.
pub const fn join_micros(days: i64, h: u32, mi: u32, s: u32, us: u32) -> Option<i64> {
    // Every term is under 2^100, so the i128 sum cannot overflow.
    let us = days as i128 * MICROS_PER_DAY as i128
        + h as i128 * MICROS_PER_HOUR as i128
        + mi as i128 * MICROS_PER_MIN as i128
        + s as i128 * MICROS_PER_SEC as i128
        + us as i128;
    if us < i64::MIN as i128 || us > i64::MAX as i128 {
        None
    } else {
        Some(us as i64)
    }
}

#[cfg(test)]
#[path = "tests/temporal.rs"]
mod tests;
