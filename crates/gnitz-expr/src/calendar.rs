//! The arithmetic a `Calendar` instruction runs over the two temporal
//! representations, which [`CalendarOp`] selects. The representations, the
//! civil-calendar conversions and the text forms are `gnitz-wire`'s.

use gnitz_wire::{civil_from_days, days_from_civil, MICROS_PER_DAY, MICROS_PER_HOUR, MICROS_PER_MIN, MICROS_PER_SEC};

/// Monday = 1 … Sunday = 7.
const fn iso_dow(days: i64) -> i64 {
    (days + 3).rem_euclid(7) + 1
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

    /// Whether this op can NULL an in-range operand: only `ToMicros` can, when
    /// the product leaves `i64`.
    pub const fn may_null(self) -> bool {
        matches!(self, Self::ToMicros)
    }
}

/// `DATE → TIMESTAMP`: a day count as microseconds, `(0, true)` when the
/// product leaves `i64`. The one calendar op that can NULL, so the one the VM
/// runs through the fail-mask kernel rather than through [`eval`].
pub(crate) fn days_to_micros(days: i64) -> (i64, bool) {
    match days.checked_mul(MICROS_PER_DAY) {
        Some(v) => (v, false),
        None => (0, true),
    }
}

/// Apply `op` to `v`: microseconds when `micros`, otherwise a DATE's day count
/// read at its 32-bit storage width. Total: a truncation whose unit starts
/// before `i64::MIN` saturates there. `ToMicros`'s arm returns
/// [`days_to_micros`]'s value without its overflow flag; the VM runs that op
/// through the fail-mask kernel instead.
#[inline(always)]
pub(crate) fn eval(op: CalendarOp, v: i64, micros: bool) -> i64 {
    use CalendarOp as C;
    let (days, tod) = if micros {
        (v.div_euclid(MICROS_PER_DAY), v.rem_euclid(MICROS_PER_DAY))
    } else {
        (i64::from(v as i32), 0)
    };
    let in_days = |d: i64| if micros { d.saturating_mul(MICROS_PER_DAY) } else { d };
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
        C::TruncHour => v.saturating_sub(tod % MICROS_PER_HOUR),
        C::TruncMinute => v.saturating_sub(tod % MICROS_PER_MIN),
        C::TruncSecond => v.saturating_sub(tod % MICROS_PER_SEC),
        C::ToMicros => days_to_micros(v).0,
        C::ToDays => days,
    }
}

#[cfg(test)]
#[path = "tests/calendar.rs"]
mod tests;
