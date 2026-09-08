use super::*;

#[test]
fn text_round_trips_at_the_column_scale() {
    assert_eq!(parse_decimal("12.5", 2), Some(1250));
    assert_eq!(parse_decimal("-0.5", 3), Some(-500));
    assert_eq!(parse_decimal("+7", 0), Some(7));
    assert_eq!(parse_decimal(".5", 1), Some(5));
    assert_eq!(parse_decimal("5.", 1), Some(50));
    // A longer fraction rounds half away from zero, in both signs.
    assert_eq!(parse_decimal("1.005", 2), Some(101));
    assert_eq!(parse_decimal("-1.005", 2), Some(-101));
    assert_eq!(parse_decimal("1.004", 2), Some(100));
    for bad in ["", ".", "-", "1e3", "1,5", "abc", "1.2.3"] {
        assert_eq!(parse_decimal(bad, 2), None, "{bad:?}");
    }
    // Past i64 at the column scale.
    assert_eq!(parse_decimal("9223372036854775808", 0), None);
    assert_eq!(parse_decimal("92233720368547758.08", 2), None);
    for (v, scale, want) in [
        (1250, 2, "12.50"),
        (-5, 2, "-0.05"),
        (7, 0, "7"),
        (0, 3, "0.000"),
        (-1234, 1, "-123.4"),
    ] {
        assert_eq!(format_decimal(v, scale), want);
        assert_eq!(parse_decimal(want, scale), Some(v));
    }
}

#[test]
fn rescale_widens_exactly_and_narrows_by_rounding() {
    assert_eq!(rescale(15, 1, 3), Some(1500));
    assert_eq!(rescale(1500, 3, 1), Some(15));
    assert_eq!(rescale(1550, 3, 1), Some(16));
    assert_eq!(rescale(-1550, 3, 1), Some(-16));
    assert_eq!(rescale(1549, 3, 1), Some(15));
    assert_eq!(rescale(i64::MAX as i128, 0, 1), None);
}

#[test]
fn a_float_literal_is_the_decimal_it_was_written_as() {
    assert_eq!(decimal_of_f64(1.1), Some((11, 1)));
    assert_eq!(decimal_of_f64(0.1), Some((1, 1)));
    assert_eq!(decimal_of_f64(-2.25), Some((-225, 2)));
    assert_eq!(decimal_of_f64(3.0), Some((3, 0)));
    assert_eq!(decimal_of_f64(1e5), Some((100_000, 0)));
    assert_eq!(decimal_of_f64(f64::NAN), None);
    assert_eq!(decimal_of_f64(f64::INFINITY), None);
    assert_eq!(decimal_of_f64(1e30), None);
}

/// `format_decimal` is the inverse of `parse_decimal` at the same scale, over
/// every scale a column may declare and at the edges of `i64` — the property
/// the Python `Decimal(str)` construction and every error message depend on.
#[test]
fn format_and_parse_are_inverse_at_every_scale() {
    for scale in 0..=MAX_DECIMAL_SCALE {
        for v in [0, 1, -1, 5, -5, 999, -999, i64::MAX, i64::MIN + 1] {
            let text = format_decimal(v, scale);
            assert_eq!(parse_decimal(&text, scale), Some(v), "{v} at scale {scale} → {text:?}");
            // Exactly `scale` fractional digits, so a column's values line up.
            let frac = text.split_once('.').map_or(0, |(_, f)| f.len());
            assert_eq!(frac, scale as usize, "{text:?}");
            assert_eq!(text.starts_with('-'), v < 0, "{text:?}");
        }
    }
    // `i64::MIN` has no positive magnitude; it still prints and re-reads.
    assert_eq!(parse_decimal(&format_decimal(i64::MIN, 4), 4), Some(i64::MIN));
}
