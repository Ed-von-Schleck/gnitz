use super::*;

#[test]
fn text_is_the_digits_and_scale_it_spells() {
    assert_eq!(parse_decimal_text("12.5"), Some((125, 1)));
    assert_eq!(parse_decimal_text("-0.5"), Some((-5, 1)));
    assert_eq!(parse_decimal_text("+7"), Some((7, 0)));
    assert_eq!(parse_decimal_text(".5"), Some((5, 1)));
    assert_eq!(parse_decimal_text("5."), Some((5, 0)));
    assert_eq!(parse_decimal_text("-1.005"), Some((-1005, 3)));
    for bad in ["", ".", "-", "1e3", "1,5", "abc", "1.2.3"] {
        assert_eq!(parse_decimal_text(bad), None, "{bad:?}");
    }
}

/// A numeric literal's text is the decimal it spells, exponent applied, at its
/// smallest exact scale — digits no float print could carry included.
#[test]
fn a_number_text_is_the_decimal_it_spells() {
    for (text, want) in [
        ("1.10", Some((11, 1))),
        ("1.0", Some((1, 0))),
        ("0.0000000100", Some((1, 8))),
        ("1.5e3", Some((1500, 0))),
        ("25e-1", Some((25, 1))),
        ("1234567890123456.78", Some((123456789012345678, 2))),
        ("0.1234567890123456789", Some((1234567890123456789, 19))),
        // Bounded by value and fractional digits, not by digit count.
        (
            "0.49999999999999999999999999999999999999",
            Some((49999999999999999999999999999999999999, 38)),
        ),
        ("100000000000000000000000000000000000000", Some((10i128.pow(38), 0))),
        ("999999999999999999999999999999999999999", None),
        ("1e39", None),
        ("1e-2147483648", None),
    ] {
        assert_eq!(decimal_of_number_text(text), want, "{text}");
    }
}

/// `format_decimal` is the inverse of `parse_decimal_text` at the same scale, over
/// every scale a column may declare and at the edges of `i64` — the property
/// the Python `Decimal(str)` construction and every error message depend on.
#[test]
fn format_and_parse_are_inverse_at_every_scale() {
    for (v, scale, want) in [
        (1250, 2, "12.50"),
        (-5, 2, "-0.05"),
        (7, 0, "7"),
        (0, 3, "0.000"),
        (-1234, 1, "-123.4"),
    ] {
        assert_eq!(format_decimal(v.into(), scale), want);
    }
    for scale in 0..=MAX_DECIMAL_SCALE {
        for v in [0, 1, -1, 5, -5, 999, -999, i64::MAX, i64::MIN] {
            let text = format_decimal(v.into(), scale);
            assert_eq!(
                parse_decimal_text(&text),
                Some((v.into(), scale)),
                "{v} at scale {scale} → {text:?}"
            );
            // Exactly `scale` fractional digits, so a column's values line up.
            let frac = text.split_once('.').map_or(0, |(_, f)| f.len());
            assert_eq!(frac, scale as usize, "{text:?}");
            assert_eq!(text.starts_with('-'), v < 0, "{text:?}");
        }
    }
}
