use super::*;

fn text(tc: TypeCode, native: u128) -> String {
    ValueText(ColType::of(tc), native).to_string()
}

#[test]
fn a_value_is_read_at_its_own_width_and_spelled_as_its_type() {
    assert_eq!(text(TypeCode::I8, 0xff), "-1");
    assert_eq!(text(TypeCode::U8, 0xff), "255");
    assert_eq!(text(TypeCode::I64, u64::MAX as u128), "-1");
    assert_eq!(text(TypeCode::Bool, 1), "true");
    assert_eq!(text(TypeCode::Bool, 0), "false");
    assert_eq!(text(TypeCode::I128, u128::MAX), "-1");
    assert_eq!(text(TypeCode::F32, 0.1f32.to_bits() as u128), "0.1");
    assert_eq!(text(TypeCode::F64, 1e-5f64.to_bits() as u128), "1e-5");
    assert_eq!(text(TypeCode::F64, f64::NAN.to_bits() as u128), "NaN");
    assert_eq!(text(TypeCode::Date, (-1i32) as u32 as u128), "1969-12-31");
    assert_eq!(
        ValueText(ColType::decimal(2), (-110i64) as u64 as u128).to_string(),
        "-1.10"
    );
}
