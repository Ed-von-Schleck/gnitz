//! Civil-calendar arithmetic over the two temporal representations: a `DATE`
//! is days since 1970-01-01 and a `TIMESTAMP` microseconds since
//! 1970-01-01 00:00:00, both proleptic Gregorian with no time zone.

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

/// Monday = 1 … Sunday = 7.
const fn iso_dow(days: i64) -> i64 {
    (days + 3).rem_euclid(7) + 1
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
    let (neg, s) = match s.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, s),
    };
    let [y, m, d] = digit_fields::<3>(s.trim(), '-')?;
    let y = if neg { -y } else { y };
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
        return days.checked_mul(MICROS_PER_DAY);
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
    let tod = h * MICROS_PER_HOUR + mi * MICROS_PER_MIN + sec * MICROS_PER_SEC + micros;
    days.checked_mul(MICROS_PER_DAY)?.checked_add(tod)
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

gnitz_wire::wire_enum! {
    /// One calendar transform of a temporal register. The `Year`..`Epoch`
    /// members are `EXTRACT` fields, the `Trunc*` members `DATE_TRUNC` units,
    /// and the last two the DATE ⇄ TIMESTAMP conversions.
    pub enum CalendarOp: u32 {
        Year = 0,
        Quarter = 1,
        Month = 2,
        /// ISO 8601 week number.
        Week = 3,
        Day = 4,
        /// Sunday = 0 … Saturday = 6.
        Dow = 5,
        /// Monday = 1 … Sunday = 7.
        Isodow = 6,
        Doy = 7,
        Hour = 8,
        Minute = 9,
        Second = 10,
        /// Whole seconds since the epoch.
        Epoch = 11,
        TruncYear = 12,
        TruncQuarter = 13,
        TruncMonth = 14,
        /// To the Monday of the ISO week.
        TruncWeek = 15,
        TruncDay = 16,
        TruncHour = 17,
        TruncMinute = 18,
        TruncSecond = 19,
        /// Days to microseconds; NULL when the product leaves `i64`.
        ToMicros = 20,
        /// Microseconds to the day they fall in. Every `i64` of microseconds
        /// is within `i32` days.
        ToDays = 21,
    }
}

impl CalendarOp {
    /// Whether the result is a value of the operand's own temporal type
    /// (a truncation) rather than a plain integer or the other type.
    pub const fn keeps_type(self) -> bool {
        matches!(
            self,
            Self::TruncYear
                | Self::TruncQuarter
                | Self::TruncMonth
                | Self::TruncWeek
                | Self::TruncDay
                | Self::TruncHour
                | Self::TruncMinute
                | Self::TruncSecond
        )
    }

    /// The truncation to this op's own unit, for the units that have one:
    /// `DATE_TRUNC('month', …)` is `EXTRACT(MONTH …)`'s unit, so the SQL binder
    /// spells the unit vocabulary once and reaches the truncations through here.
    pub const fn trunc_of(self) -> Option<Self> {
        Some(match self {
            Self::Year => Self::TruncYear,
            Self::Quarter => Self::TruncQuarter,
            Self::Month => Self::TruncMonth,
            Self::Week => Self::TruncWeek,
            Self::Day => Self::TruncDay,
            Self::Hour => Self::TruncHour,
            Self::Minute => Self::TruncMinute,
            Self::Second => Self::TruncSecond,
            _ => return None,
        })
    }

    /// Whether this op can NULL an in-range operand — only the widening
    /// conversion can, and [`days_to_micros`] is where it does.
    pub const fn may_null(self) -> bool {
        matches!(self, Self::ToMicros)
    }
}

/// `DATE → TIMESTAMP`: a day count as microseconds, `(0, true)` when the
/// product leaves `i64`. The one calendar op that can NULL, so the one the VM
/// runs through the fail-mask kernel rather than through [`eval`].
pub fn days_to_micros(days: i64) -> (i64, bool) {
    match days.checked_mul(MICROS_PER_DAY) {
        Some(v) => (v, false),
        None => (0, true),
    }
}

/// Apply `op` to `v`, read as microseconds when `micros` and as days
/// otherwise. Total: `ToMicros` yields `days_to_micros`'s value, whose overflow
/// only the caller that wants the NULL asks about.
pub fn eval(op: CalendarOp, v: i64, micros: bool) -> i64 {
    use CalendarOp as C;
    let (days, tod) = if micros {
        (v.div_euclid(MICROS_PER_DAY), v.rem_euclid(MICROS_PER_DAY))
    } else {
        (v, 0)
    };
    let in_days = |d: i64| if micros { d * MICROS_PER_DAY } else { d };
    let ymd = || civil_from_days(days);
    match op {
        C::Year => ymd().0,
        C::Quarter => (ymd().1 as i64 - 1) / 3 + 1,
        C::Month => ymd().1 as i64,
        C::Day => ymd().2 as i64,
        // Sunday = 0 … Saturday = 6, which is the ISO numbering wrapped at 7.
        C::Dow => iso_dow(days) % 7,
        C::Isodow => iso_dow(days),
        C::Doy => days - days_from_civil(ymd().0, 1, 1) + 1,
        C::Week => {
            let thursday = days - iso_dow(days) + 4;
            let y = civil_from_days(thursday).0;
            (thursday - days_from_civil(y, 1, 1)) / 7 + 1
        }
        C::Hour => tod / MICROS_PER_HOUR,
        C::Minute => tod % MICROS_PER_HOUR / MICROS_PER_MIN,
        C::Second => tod % MICROS_PER_MIN / MICROS_PER_SEC,
        C::Epoch => {
            if micros {
                v.div_euclid(MICROS_PER_SEC)
            } else {
                days * (MICROS_PER_DAY / MICROS_PER_SEC)
            }
        }
        C::TruncYear => in_days(days_from_civil(ymd().0, 1, 1)),
        C::TruncQuarter => {
            let (y, m, _) = ymd();
            in_days(days_from_civil(y, (m - 1) / 3 * 3 + 1, 1))
        }
        C::TruncMonth => {
            let (y, m, _) = ymd();
            in_days(days_from_civil(y, m, 1))
        }
        C::TruncWeek => in_days(days - (iso_dow(days) - 1)),
        C::TruncDay => in_days(days),
        C::TruncHour => v - tod % MICROS_PER_HOUR,
        C::TruncMinute => v - tod % MICROS_PER_MIN,
        C::TruncSecond => v - tod % MICROS_PER_SEC,
        C::ToMicros => days_to_micros(v).0,
        C::ToDays => days,
    }
}

#[cfg(test)]
#[path = "tests/calendar.rs"]
mod tests;
