use super::*;

/// 2024-02-29, a Thursday.
const LEAP_DAY: i64 = 19_782;

/// Anchors against the calendar itself: 2024-02-29 is a Thursday, day 60 of the
/// year, ISO week 9; 2021-01-03 is a Sunday in ISO week 53 of 2020.
#[test]
fn each_op_answers_its_calendar_anchor() {
    use CalendarOp as C;
    let d = LEAP_DAY;
    let ts = d * MICROS_PER_DAY + 13 * MICROS_PER_HOUR + 45 * MICROS_PER_MIN + 7 * MICROS_PER_SEC + 5;
    let sun = days_from_civil(2021, 1, 3);
    for (op, v, micros, want) in [
        (C::Year, d, false, 2024),
        (C::Quarter, d, false, 1),
        (C::Month, d, false, 2),
        (C::Day, d, false, 29),
        (C::Dow, d, false, 4),
        (C::Isodow, d, false, 4),
        (C::Doy, d, false, 60),
        (C::Week, d, false, 9),
        (C::Dow, sun, false, 0),
        (C::Isodow, sun, false, 7),
        (C::Week, sun, false, 53),
        (C::Week, sun + 1, false, 1),
        (C::Hour, ts, true, 13),
        (C::Minute, ts, true, 45),
        (C::Second, ts, true, 7),
        (C::Epoch, ts, true, d * 86_400 + 13 * 3600 + 45 * 60 + 7),
        (C::Epoch, d, false, d * 86_400),
        (C::Hour, d, false, 0),
        (C::Year, -1, true, 1969),
        (C::Hour, -1, true, 23),
        (C::TruncYear, d, false, days_from_civil(2024, 1, 1)),
        (
            C::TruncQuarter,
            days_from_civil(2024, 11, 5),
            false,
            days_from_civil(2024, 10, 1),
        ),
        (C::TruncMonth, ts, true, days_from_civil(2024, 2, 1) * MICROS_PER_DAY),
        (C::TruncWeek, d, false, days_from_civil(2024, 2, 26)),
        (C::TruncDay, ts, true, d * MICROS_PER_DAY),
        (C::TruncHour, ts, true, d * MICROS_PER_DAY + 13 * MICROS_PER_HOUR),
        (C::TruncMinute, ts, true, ts - 7 * MICROS_PER_SEC - 5),
        (C::TruncSecond, ts, true, ts - 5),
        // A DATE has no time of day, so truncating one below a day is the identity.
        (C::TruncHour, d, false, d),
        (C::ToDays, ts, true, d),
        (C::ToDays, -1, true, -1),
        // A truncation whose unit starts before `i64::MIN` saturates there.
        (C::TruncDay, i64::MIN, true, i64::MIN),
        (C::TruncYear, i64::MIN, true, i64::MIN),
        (C::TruncHour, i64::MIN, true, i64::MIN),
        // A day count is read at DATE's 32-bit width: `i64::MAX` is day -1.
        (C::Epoch, i64::MAX, false, -86_400),
        (C::Year, i64::MAX, false, 1969),
    ] {
        assert_eq!(eval(op, v, micros), want, "{op:?}({v}, micros={micros})");
    }
    assert_eq!(days_to_micros(d), (d * MICROS_PER_DAY, false));
    assert!(days_to_micros(i64::MAX / 2).1, "a day count past i64 micros is NULL");
}

/// The laws tying the ops together, over days of both eras and times of day at
/// both ends.
#[test]
fn every_op_obeys_the_calendar_laws() {
    use CalendarOp as C;
    let date_fields = [
        C::Year,
        C::Quarter,
        C::Month,
        C::Week,
        C::Day,
        C::Dow,
        C::Isodow,
        C::Doy,
    ];
    for d in (-1_000_000..1_000_000).step_by(7_919) {
        for tod in [0, 1, MICROS_PER_DAY / 2 + 7, MICROS_PER_DAY - 1] {
            let ts = d * MICROS_PER_DAY + tod;
            assert_eq!(eval(C::ToDays, ts, true), d);
            for op in date_fields {
                assert_eq!(eval(op, ts, true), eval(op, d, false), "{op:?} on day {d}");
            }
            assert_eq!(eval(C::Dow, d, false), eval(C::Isodow, d, false) % 7);
            for (v, micros) in [(d, false), (ts, true)] {
                for field in C::ALL.iter().copied() {
                    let Some(trunc) = field.trunc_of() else { continue };
                    let t = eval(trunc, v, micros);
                    assert!(t <= v, "{trunc:?}({v}) = {t}");
                    assert_eq!(eval(trunc, t, micros), t, "{trunc:?} is idempotent at {v}");
                    assert_eq!(
                        eval(field, t, micros),
                        eval(field, v, micros),
                        "{trunc:?} keeps {field:?} at {v}"
                    );
                }
                assert_eq!(eval(C::Isodow, eval(C::TruncWeek, v, micros), micros), 1);
                assert_eq!(eval(C::Doy, eval(C::TruncYear, v, micros), micros), 1);
                assert_eq!(eval(C::Day, eval(C::TruncMonth, v, micros), micros), 1);
            }
        }
    }
}

/// Every truncation is reachable from the field of the same unit, which is
/// what lets the binder spell that unit vocabulary once.
#[test]
fn every_truncation_is_its_units_field() {
    let reachable: Vec<CalendarOp> = CalendarOp::ALL.iter().filter_map(|op| op.trunc_of()).collect();
    let truncs: Vec<CalendarOp> = CalendarOp::ALL.iter().copied().filter(|op| op.keeps_type()).collect();
    assert_eq!(reachable, truncs);
}
