use super::*;

#[test]
fn narrow_f32_refuses_only_a_finite_overflow() {
    assert_eq!(narrow_f32(0.1), Some(0.1f64 as f32));
    assert_eq!(narrow_f32(1e-300), Some(0.0));
    assert_eq!(narrow_f32(1e39), None);
    assert_eq!(narrow_f32(-1e39), None);
    assert_eq!(narrow_f32(f64::INFINITY), Some(f32::INFINITY));
    assert!(narrow_f32(f64::NAN).is_some_and(f32::is_nan));
    // Just above `f32::MAX` rounds down onto it rather than overflowing.
    assert_eq!(narrow_f32(f32::MAX as f64 + 2.0f64.powi(102)), Some(f32::MAX));
}
