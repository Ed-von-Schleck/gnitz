use super::*;
use crate::bind::bind_constant;
use crate::test_support::{ncol, parse_expr_sql};

const U128_MAX: &str = "340282366920938463463374607431768211455";

/// `src` written into a fresh column `c` of type `tc`: the cell and the blob spill.
fn write(tc: TypeCode, src: &str) -> Result<(Vec<u8>, Vec<u8>), String> {
    let (mut col, mut blob) = (Vec::new(), Vec::new());
    let lit = bind_constant(&parse_expr_sql(src)).unwrap_or_else(|e| panic!("{src}: {e}"));
    append_value_to_col(&mut col, &mut blob, &ncol("c", tc), &lit).map_err(|e| e.to_string())?;
    Ok((col, blob))
}

fn cell(tc: TypeCode, src: &str) -> Vec<u8> {
    write(tc, src).unwrap_or_else(|e| panic!("{tc:?} {src}: {e}")).0
}

#[test]
fn a_null_is_a_zeroed_cell_of_every_type_stride() {
    for &tc in TypeCode::ALL {
        assert_eq!(write(tc, "NULL"), Ok((vec![0; tc.wire_stride()], vec![])), "{tc:?}");
    }
}

/// A cell stored as an integer is the low `wire_stride` bytes of its native
/// image, little-endian.
#[test]
fn an_integer_stored_cell_is_the_low_bytes_of_its_value() {
    for (tc, src, want) in [
        (TypeCode::I8, "-5", vec![0xFB]),
        (TypeCode::U16, "65535", 65535u16.to_le_bytes().to_vec()),
        (TypeCode::I32, "-1", (-1i32).to_le_bytes().to_vec()),
        (TypeCode::I64, "-9223372036854775808", i64::MIN.to_le_bytes().to_vec()),
        (TypeCode::U64, "18446744073709551615", u64::MAX.to_le_bytes().to_vec()),
        (TypeCode::Date, "DATE '2020-01-02'", 18263i32.to_le_bytes().to_vec()),
        (
            TypeCode::UUID,
            "'550e8400-e29b-41d4-a716-446655440000'",
            0x550e8400_e29b_41d4_a716_446655440000_u128.to_le_bytes().to_vec(),
        ),
    ] {
        assert_eq!(cell(tc, src), want, "{tc:?} {src}");
    }
}

/// Every numeric spelling reaches a float column: an integer (`-0` is the
/// integer zero, so `+0.0`), a magnitude past `i128`, and a float's own `-0.0`.
#[test]
fn a_float_cell_takes_every_numeric_spelling() {
    for (src, want) in [
        ("5", 5.0),
        ("-0", 0.0),
        ("-0.0", -0.0),
        ("-18446744073709551616", -18446744073709551616.0),
        (U128_MAX, 340282366920938463463374607431768211455.0),
    ] {
        assert_eq!(cell(TypeCode::F64, src), f64::to_le_bytes(want), "{src}");
        // `u128::MAX` rounds past F32's range.
        if (want as f32).is_finite() {
            assert_eq!(cell(TypeCode::F32, src), (want as f32).to_le_bytes(), "{src}");
        }
    }
    // Through binary64: parsed straight to f32 this would be the float above 1.0.
    assert_eq!(cell(TypeCode::F32, "1.00000005960464477539063"), 1.0f32.to_le_bytes());
}

#[test]
fn a_string_cell_spills_past_its_inline_prefix() {
    for s in ["short", "a string past the inline prefix"] {
        let (col, blob) = write(TypeCode::String, &format!("'{s}'")).unwrap();
        assert_eq!(gnitz_wire::german_string_content(&col, &blob), s.as_bytes());
        assert_eq!(blob.is_empty(), s.len() <= gnitz_wire::SHORT_STRING_THRESHOLD, "{s}");
    }
}

/// A refusal names the column it was refused for.
#[test]
fn a_literal_the_column_has_no_value_for_is_refused_naming_the_column() {
    for (tc, src) in [
        (TypeCode::String, "5"),
        (TypeCode::Blob, "5"),
        (TypeCode::Blob, "'5'"),
        (TypeCode::F64, "'5'"),
        (TypeCode::U8, "256"),
    ] {
        let m = write(tc, src).unwrap_err();
        assert!(m.starts_with("column 'c': "), "{tc:?} {src}: {m}");
    }
}
