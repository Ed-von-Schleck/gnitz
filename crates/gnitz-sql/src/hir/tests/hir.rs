use super::*;

/// Set-op columns of the same type pass through untouched; a differing pair
/// promotes on the join-key ladder, and only to a target a column copy can widen
/// into.
#[test]
fn set_op_common_type_admits_what_a_column_copy_widens() {
    let common = |l: TypeCode, r: TypeCode| set_op_common_type(l.into(), r.into()).map(|t| t.tc);
    // Identical types are their own common type, at any width and any kind.
    for tc in [TypeCode::U64, TypeCode::String, TypeCode::U128, TypeCode::F64] {
        assert_eq!(common(tc, tc), Some(tc), "{tc:?}");
    }
    assert_eq!(common(TypeCode::U32, TypeCode::I32), Some(TypeCode::I64));
    assert_eq!(common(TypeCode::I32, TypeCode::I64), Some(TypeCode::I64));
    // The I128 collapse and every 16-byte / non-integer differing pair are
    // rejected: a column copy widens only into a ≤8-byte integer slot.
    assert_eq!(common(TypeCode::U64, TypeCode::I64), None);
    assert_eq!(common(TypeCode::U128, TypeCode::I64), None);
    assert_eq!(common(TypeCode::String, TypeCode::U64), None);
    assert_eq!(common(TypeCode::F64, TypeCode::F32), None);
    // A column copy moves bytes, so it cannot turn days into microseconds.
    assert_eq!(common(TypeCode::Date, TypeCode::Timestamp), None);
    // A DECIMAL unions only with the same DECIMAL: the stored integers of two
    // scales, or of a scale and an integer column, never mean the same value.
    assert_eq!(
        set_op_common_type(ColType::decimal(2), ColType::decimal(2)),
        Some(ColType::decimal(2))
    );
    assert_eq!(set_op_common_type(ColType::decimal(2), ColType::decimal(3)), None);
    assert_eq!(set_op_common_type(ColType::decimal(0), TypeCode::I64.into()), None);
}
