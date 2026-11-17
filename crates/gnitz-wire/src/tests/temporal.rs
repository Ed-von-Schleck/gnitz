use super::*;
use crate::VALUE_TEXT_MAX;

/// 2024-02-29.
const LEAP_DAY: i64 = 19_782;

#[test]
fn civil_round_trips_across_eras() {
    assert_eq!(days_from_civil(1970, 1, 1), 0);
    assert_eq!(days_from_civil(2000, 3, 1), 11_017);
    assert_eq!(days_from_civil(1969, 12, 31), -1);
    for z in (-1_000_000..1_000_000).step_by(997) {
        let (y, m, d) = civil_from_days(z);
        assert_eq!(days_from_civil(y, m, d), z, "{y}-{m}-{d}");
    }
}

#[test]
fn a_date_parses_only_as_digit_fields_inside_the_calendar() {
    for (s, want) in [
        ("2024-02-29", Some(LEAP_DAY)),
        ("-0001-01-01", Some(days_from_civil(-1, 1, 1))),
        ("2023-02-29", None),
        ("2024-13-01", None),
        // A sign inside a field is not a digit.
        ("2024-+2-01", None),
        ("2024-02", None),
        ("2024-02-29-1", None),
        // A year past `i32` spells no date.
        ("8232391586451896900-03-01", None),
        // The sign is the first character once the text is trimmed.
        ("- 2024-01-01", None),
        (" -0001-01-01", Some(days_from_civil(-1, 1, 1))),
    ] {
        assert_eq!(parse_date(s).map(i64::from), want, "{s:?}");
    }
}

#[test]
fn a_timestamp_parses_as_a_date_and_an_optional_time_of_day() {
    let at = |h: i64, mi: i64, s: i64, us: i64| {
        Some(LEAP_DAY * MICROS_PER_DAY + h * MICROS_PER_HOUR + mi * MICROS_PER_MIN + s * MICROS_PER_SEC + us)
    };
    for (s, want) in [
        ("2024-02-29 13:45:07.25", at(13, 45, 7, 250_000)),
        ("2024-02-29 13:45:07.123456", at(13, 45, 7, 123_456)),
        ("2024-02-29T13:45", at(13, 45, 0, 0)),
        ("2024-02-29", at(0, 0, 0, 0)),
        ("2024-02-29 24:00", None),
        ("2024-02-29 01:00:00Z", None),
        // A bare hour, a fourth field, a fraction past microseconds, and an
        // empty one are not times.
        ("2024-02-29 13", None),
        ("2024-02-29 1:2:3:4", None),
        ("2024-02-29 13:45:07.1234567", None),
        ("2024-02-29 13:45:07.", None),
    ] {
        assert_eq!(parse_timestamp(s), want, "{s:?}");
    }
    assert_eq!(
        split_micros(at(13, 45, 7, 250_000).unwrap()),
        (LEAP_DAY, 13, 45, 7, 250_000)
    );
    // Pre-epoch splits with a floor, never toward zero.
    assert_eq!(split_micros(-1), (-1, 23, 59, 59, 999_999));
}

#[test]
fn formatted_dates_and_timestamps_parse_back() {
    let buf = &mut [0; VALUE_TEXT_MAX];
    for days in [-719_528, -1, 0, 1, 19_000, 2_932_896, i32::MIN, i32::MAX] {
        let text = write_date(days.into(), buf);
        assert_eq!(parse_date(text), Some(days), "{text}");
    }
    for micros in [0, 1, -1, 1_500_000, 86_399_999_999, 1_700_000_000_123_400] {
        let text = write_timestamp(micros, buf);
        assert_eq!(parse_timestamp(text), Some(micros), "{text}");
    }
}

/// The year is at least four characters wide with its sign, so year -1 is
/// `-001`; a fraction is as long as it needs to be and absent on a whole second.
#[test]
fn dates_and_timestamps_print_their_fixed_spelling() {
    let buf = &mut [0; VALUE_TEXT_MAX];
    let day = |y, m, d| i32::try_from(days_from_civil(y, m, d)).unwrap();
    for (days, want) in [
        (0, "1970-01-01"),
        (day(7, 6, 7), "0007-06-07"),
        (day(-1, 1, 1), "-001-01-01"),
        (day(-12_345, 6, 7), "-12345-06-07"),
        (day(12_345, 6, 7), "12345-06-07"),
        (i32::MIN, "-5877641-06-23"),
        (i32::MAX, "5881580-07-11"),
    ] {
        assert_eq!(write_date(days.into(), buf), want, "{days}");
    }
    for (micros, want) in [
        (0, "1970-01-01 00:00:00"),
        (5 * MICROS_PER_SEC, "1970-01-01 00:00:05"),
        (1_500_000, "1970-01-01 00:00:01.5"),
        (-1, "1969-12-31 23:59:59.999999"),
        (
            days_from_civil(-1, 3, 4) * MICROS_PER_DAY + 7 * MICROS_PER_SEC + 120,
            "-001-03-04 00:00:07.00012",
        ),
        (i64::MIN, "-290308-12-21 19:59:05.224192"),
        (i64::MAX, "294247-01-10 04:00:54.775807"),
    ] {
        assert_eq!(write_timestamp(micros, buf), want, "{micros}");
    }
}

#[test]
fn joined_micros_invert_the_split() {
    for micros in [
        0,
        1,
        -1,
        1_500_000,
        86_399_999_999,
        1_700_000_000_123_400,
        -86_400_000_001,
    ] {
        let (days, h, mi, s, us) = split_micros(micros);
        assert_eq!(join_micros(days, h, mi, s, us), Some(micros), "{micros}");
    }
    assert_eq!(join_micros(i64::MAX / MICROS_PER_DAY + 1, 0, 0, 0, 0), None);
    assert_eq!(join_micros(i64::MAX / MICROS_PER_DAY, 23, 59, 59, 999_999), None);
    assert_eq!(join_micros(0, u32::MAX, u32::MAX, u32::MAX, u32::MAX), None);
    assert_eq!(join_micros(i64::MIN / MICROS_PER_DAY - 1, 0, 0, 0, 0), None);
}
