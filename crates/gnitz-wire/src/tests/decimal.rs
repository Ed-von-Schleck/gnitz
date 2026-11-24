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

/// `write_decimal` agrees with a reference built from the digit string — sign,
/// at least `scale + 1` zero-padded digits, the point `scale` from the end — and
/// is the inverse of `parse_decimal_text` at the same scale, over every scale a
/// column may declare and at the edges of `i64`: the property the Python
/// `Decimal(str)` construction and every error message depend on.
#[test]
fn write_decimal_is_the_padded_digit_string_and_parses_back() {
    fn reference(v: i64, scale: u8) -> String {
        let scale = scale as usize;
        let digits = v.unsigned_abs().to_string();
        let mut out = String::new();
        if v < 0 {
            out.push('-');
        }
        for _ in digits.len()..=scale {
            out.push('0');
        }
        out.push_str(&digits);
        if scale > 0 {
            out.insert(out.len() - scale, '.');
        }
        out
    }
    for scale in 0..=MAX_DECIMAL_SCALE {
        for v in [
            0,
            1,
            -1,
            5,
            -5,
            10,
            -10,
            999,
            -999,
            123_456,
            i64::MAX,
            i64::MIN,
            i64::MIN + 1,
        ] {
            let text = write_decimal(v, scale, &mut [0; VALUE_TEXT_MAX]).to_owned();
            assert_eq!(text, reference(v, scale), "{v} at scale {scale}");
            assert_eq!(parse_decimal_text(&text), Some((v.into(), scale)), "{text:?}");
        }
    }
}

#[test]
#[should_panic(expected = "DECIMAL scale 19")]
fn write_decimal_refuses_a_scale_past_the_maximum() {
    write_decimal(1, MAX_DECIMAL_SCALE + 1, &mut [0; VALUE_TEXT_MAX]);
}

/// `scaled_bytes` holds a whole `u64`, at scale 0 and at the largest scale.
#[test]
fn scaled_bytes_holds_every_u64() {
    let mut buf = [0; VALUE_TEXT_MAX];
    assert_eq!(
        scaled_bytes(u64::MAX, true, 0, &mut buf),
        format!("-{}", u64::MAX).as_bytes()
    );
    let text = String::from_utf8(scaled_bytes(u64::MAX, true, MAX_DECIMAL_SCALE, &mut buf).to_vec()).unwrap();
    assert_eq!(text.len(), SCALED_TEXT_MAX);
    assert_eq!(text.replace('.', ""), format!("-{}", u64::MAX));
}
