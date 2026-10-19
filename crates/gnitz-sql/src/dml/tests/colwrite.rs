use super::*;
use crate::bind::bind_constant;
use crate::test_support::{ncol, parse_expr_sql};

/// `src` written into a fresh column `c` of type `tc`: the cell and the blob spill.
fn write(tc: TypeCode, src: &str) -> Result<(Vec<u8>, Vec<u8>), String> {
    let (mut col, mut blob) = (Vec::new(), Vec::new());
    let lit = bind_constant(&parse_expr_sql(src)).unwrap_or_else(|e| panic!("{src}: {e}"));
    append_value_to_col(&mut col, &mut blob, &ncol("c", tc), &lit).map_err(|e| e.to_string())?;
    Ok((col, blob))
}

#[test]
fn a_null_is_a_zeroed_cell_of_every_type_stride() {
    for &tc in TypeCode::ALL {
        assert_eq!(write(tc, "NULL"), Ok((vec![0; tc.wire_stride()], vec![])), "{tc:?}");
    }
}

/// A cell is the low `wire_stride` bytes of its value's native image,
/// little-endian, and spills nothing; a refusal names the column.
#[test]
fn a_cell_is_the_low_bytes_of_its_value_or_a_refusal_naming_the_column() {
    use TypeCode::*;
    for (tc, src, want) in [
        (I8, "-5", Ok(vec![0xFB])),
        (U16, "65535", Ok(vec![0xFF; 2])),
        (I64, "-9223372036854775808", Ok(i64::MIN.to_le_bytes().to_vec())),
        (I128, "-1", Ok(vec![0xFF; 16])),
        (F32, "1.5", Ok(1.5f32.to_le_bytes().to_vec())),
        (Date, "DATE '2020-01-02'", Ok(18263i32.to_le_bytes().to_vec())),
        (
            UUID,
            "'550e8400-e29b-41d4-a716-446655440000'",
            Ok(0x550e8400_e29b_41d4_a716_446655440000_u128.to_le_bytes().to_vec()),
        ),
        (U8, "256", Err("column 'c': U8 value out of range: 256")),
    ] {
        assert_eq!(
            write(tc, src),
            want.map(|c| (c, vec![])).map_err(str::to_owned),
            "{tc:?} {src}"
        );
    }
}

#[test]
fn a_string_cell_spills_past_its_inline_prefix_into_the_blob() {
    let s = "a string past the inline prefix";
    let (col, blob) = write(TypeCode::String, &format!("'{s}'")).unwrap();
    assert_eq!(gnitz_wire::german_string_content(&col, &blob), s.as_bytes());
    assert!(!blob.is_empty());
}
