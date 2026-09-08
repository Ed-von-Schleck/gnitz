use super::*;

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
fn parsing_and_formatting() {
    assert_eq!(parse_date("2024-02-29"), Some(19_782));
    assert_eq!(parse_date("2023-02-29"), None);
    assert_eq!(parse_date("2024-13-01"), None);
    assert_eq!(parse_date("-0001-01-01"), Some(days_from_civil(-1, 1, 1) as i32));
    let ts = parse_timestamp("2024-02-29 13:45:07.25").unwrap();
    assert_eq!(
        ts,
        19_782 * MICROS_PER_DAY + 13 * MICROS_PER_HOUR + 45 * MICROS_PER_MIN + 7_250_000
    );
    assert_eq!(split_micros(ts), (19_782, 13, 45, 7, 250_000));
    assert_eq!(
        parse_timestamp("2024-02-29T13:45"),
        Some(19_782 * MICROS_PER_DAY + 13 * MICROS_PER_HOUR + 45 * MICROS_PER_MIN)
    );
    assert_eq!(parse_timestamp("2024-02-29"), Some(19_782 * MICROS_PER_DAY));
    assert_eq!(parse_timestamp("2024-02-29 24:00"), None);
    assert_eq!(parse_timestamp("2024-02-29 01:00:00Z"), None);
    assert_eq!(split_micros(-1), (-1, 23, 59, 59, 999_999));
}

#[test]
fn extract_fields() {
    use CalendarOp as C;
    // 2024-02-29 is a Thursday, day 60 of the year, ISO week 9.
    let d = 19_782;
    let ts = d * MICROS_PER_DAY + 13 * MICROS_PER_HOUR + 45 * MICROS_PER_MIN + 7 * MICROS_PER_SEC;
    assert_eq!(eval(C::Year, d, false), 2024);
    assert_eq!(eval(C::Quarter, d, false), 1);
    assert_eq!(eval(C::Month, d, false), 2);
    assert_eq!(eval(C::Day, d, false), 29);
    assert_eq!(eval(C::Dow, d, false), 4);
    assert_eq!(eval(C::Isodow, d, false), 4);
    assert_eq!(eval(C::Doy, d, false), 60);
    assert_eq!(eval(C::Week, d, false), 9);
    assert_eq!(eval(C::Hour, ts, true), 13);
    assert_eq!(eval(C::Minute, ts, true), 45);
    assert_eq!(eval(C::Second, ts, true), 7);
    assert_eq!(eval(C::Epoch, ts, true), d * 86_400 + 13 * 3600 + 45 * 60 + 7);
    assert_eq!(eval(C::Epoch, d, false), d * 86_400);
    assert_eq!(eval(C::Hour, d, false), 0);
    // A Sunday: DOW 0, ISODOW 7, and ISO week 52 of the previous year for
    // 2021-01-03.
    let sun = days_from_civil(2021, 1, 3);
    assert_eq!(eval(C::Dow, sun, false), 0);
    assert_eq!(eval(C::Isodow, sun, false), 7);
    assert_eq!(eval(C::Week, sun, false), 53);
    assert_eq!(eval(C::Week, days_from_civil(2021, 1, 4), false), 1);
    // Negative timestamps split with a floor, never toward zero.
    assert_eq!(eval(C::Year, -1, true), 1969);
    assert_eq!(eval(C::Hour, -1, true), 23);
}

#[test]
fn truncation_and_conversion() {
    use CalendarOp as C;
    let d = 19_782;
    let ts = d * MICROS_PER_DAY + 13 * MICROS_PER_HOUR + 45 * MICROS_PER_MIN + 7 * MICROS_PER_SEC + 5;
    assert_eq!(eval(C::TruncYear, d, false), days_from_civil(2024, 1, 1));
    assert_eq!(
        eval(C::TruncQuarter, days_from_civil(2024, 11, 5), false),
        days_from_civil(2024, 10, 1)
    );
    assert_eq!(
        eval(C::TruncMonth, ts, true),
        days_from_civil(2024, 2, 1) * MICROS_PER_DAY
    );
    assert_eq!(eval(C::TruncWeek, d, false), days_from_civil(2024, 2, 26));
    assert_eq!(eval(C::TruncDay, ts, true), d * MICROS_PER_DAY);
    assert_eq!(eval(C::TruncHour, ts, true), d * MICROS_PER_DAY + 13 * MICROS_PER_HOUR);
    assert_eq!(eval(C::TruncMinute, ts, true), ts - 7 * MICROS_PER_SEC - 5);
    assert_eq!(eval(C::TruncSecond, ts, true), ts - 5);
    // A DATE has no time of day, so truncating one below a day is the identity.
    assert_eq!(eval(C::TruncHour, d, false), d);
    assert_eq!(days_to_micros(d), (d * MICROS_PER_DAY, false));
    assert!(days_to_micros(i64::MAX / 2).1, "a day count past i64 micros is NULL");
    assert_eq!(eval(C::ToDays, ts, true), d);
    // The day a pre-epoch instant falls in floors, never truncates toward zero.
    assert_eq!(eval(C::ToDays, -1, true), -1);
    assert_eq!(
        eval(C::ToDays, days_from_civil(1900, 6, 15) * MICROS_PER_DAY, true),
        days_from_civil(1900, 6, 15)
    );
    // Every truncation is reachable from the field of the same unit, which is
    // what lets the binder spell that unit vocabulary once.
    let reachable: Vec<C> = C::ALL.iter().filter_map(|op| op.trunc_of()).collect();
    let truncs: Vec<C> = C::ALL.iter().copied().filter(|op| op.keeps_type()).collect();
    assert_eq!(reachable, truncs);
}
