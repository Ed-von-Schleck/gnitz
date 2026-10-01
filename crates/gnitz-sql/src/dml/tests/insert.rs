use super::*;
use crate::test_support::{col, compound_schema_u64_u64, ncol, parse_expr_sql, pk_schema, rejected, schema, two_col};
use gnitz_expr::SchemaFacts;
use gnitz_wire::TypeCode;

// `two_col` has columns ["pk", "val"] in schema order.
fn idents(names: &[&str]) -> Vec<ObjectName> {
    names
        .iter()
        .map(|n| ObjectName::from(sqlparser::ast::Ident::new(*n)))
        .collect()
}

fn err_of(r: Result<RowShape, GnitzSqlError>) -> GnitzSqlError {
    r.err().expect("must be rejected")
}

/// `(id SERIAL PK, val I64)` — the shape whose SERIAL column takes no user value.
/// Column 0 is the SERIAL column.
fn serial_schema() -> Schema {
    schema(vec![col("id", TypeCode::U64), ncol("val", TypeCode::I64)], &[0])
}

#[test]
fn insert_no_column_list_maps_every_visible_non_serial_column() {
    let shape = insert_row_shape(&[], &two_col(TypeCode::I64), None).unwrap();
    assert_eq!(shape.slot_of, vec![Some(0), Some(1)]);
    assert_eq!(shape.expected, 2);
}

#[test]
fn insert_no_column_list_skips_the_serial_column() {
    let shape = insert_row_shape(&[], &serial_schema(), Some(0)).unwrap();
    assert_eq!(shape.slot_of, vec![None, Some(0)]);
    assert_eq!(shape.expected, 1);
}

#[test]
fn insert_in_order_full_list_is_ok() {
    // Full set, schema order, case-insensitive → the identity map.
    let shape = insert_row_shape(&idents(&["pk", "VAL"]), &two_col(TypeCode::I64), None).unwrap();
    assert_eq!(shape.slot_of, vec![Some(0), Some(1)]);
    assert_eq!(shape.expected, 2);
}

#[test]
fn insert_reordered_list_remaps_the_slots() {
    let shape = insert_row_shape(&idents(&["val", "pk"]), &two_col(TypeCode::I64), None).unwrap();
    assert_eq!(shape.slot_of, vec![Some(1), Some(0)]);
    assert_eq!(shape.expected, 2);
}

/// A column the list omits takes no slot; the row loop writes it NULL.
#[test]
fn insert_partial_list_leaves_the_omitted_column_unmapped() {
    let shape = insert_row_shape(&idents(&["pk"]), &two_col(TypeCode::I64), None).unwrap();
    assert_eq!(shape.slot_of, vec![Some(0), None]);
    assert_eq!(shape.expected, 1);
}

#[test]
fn insert_wrong_name_is_rejected() {
    let err = err_of(insert_row_shape(
        &idents(&["pk", "nope"]),
        &two_col(TypeCode::I64),
        None,
    ));
    assert!(matches!(err, GnitzSqlError::Rejected(_)), "got {err:?}");
}

#[test]
fn insert_duplicate_name_is_rejected() {
    let err = err_of(insert_row_shape(&idents(&["pk", "PK"]), &two_col(TypeCode::I64), None));
    assert!(err.to_string().contains("more than once"), "got {err}");
}

/// PostgreSQL refuses `INSERT INTO t (t.a)`; a qualifier must not be dropped.
#[test]
fn insert_qualified_name_is_rejected() {
    let qualified = vec![ObjectName::from(vec![
        sqlparser::ast::Ident::new("t"),
        sqlparser::ast::Ident::new("pk"),
    ])];
    let err = err_of(insert_row_shape(&qualified, &two_col(TypeCode::I64), None));
    assert!(matches!(err, GnitzSqlError::Rejected(_)), "got {err:?}");
}

#[test]
fn insert_naming_the_serial_column_is_rejected() {
    let err = err_of(insert_row_shape(&idents(&["id", "val"]), &serial_schema(), Some(0)));
    assert!(err.to_string().contains("SERIAL"), "got {err}");
}

/// A column list that omits the PK is caught where the PK is planned, not by the
/// slot map: an omitted column is otherwise a legal NULL.
#[test]
fn insert_omitting_the_pk_is_rejected_by_the_pk_plan() {
    let schema = two_col(TypeCode::I64);
    let shape = insert_row_shape(&idents(&["val"]), &schema, None).unwrap();
    let err = PkPlan::written(&shape.slot_of, &schema)
        .err()
        .expect("an omitted PK must be rejected");
    assert!(err.to_string().contains("PK column 'pk' missing"), "got {err}");
}

/// The key the VALUES row `row` stores under `schema`, through the INSERT path's
/// `PkPlan` with the identity slot map.
fn written_pk(row: &[&str], schema: &Schema) -> Result<gnitz_wire::PkBuf, GnitzSqlError> {
    let slot_of: Vec<Option<usize>> = (0..row.len()).map(Some).collect();
    let cells = row
        .iter()
        .map(|src| crate::bind::structural::bind_constant(&parse_expr_sql(src)))
        .collect::<Result<Vec<_>, _>>()?;
    let mut pks = PkColumn::empty_for_schema(schema);
    PkPlan::written(&slot_of, schema)?.push(schema, 0, &cells, &mut pks)?;
    Ok(gnitz_wire::PkBuf::from_bytes(pks.get_bytes(0)))
}

/// A written PK packs as its columns' OPK key, in PK-list order and at any stride.
#[test]
fn a_written_pk_packs_its_opk_key() {
    use TypeCode::*;
    const UUID_LIT: &str = "'550e8400-e29b-41d4-a716-446655440000'";
    for (tc, lit, native) in [
        (I8, "-1", (-1i8 as u8) as u128),
        (I16, "-1", (-1i16 as u16) as u128),
        (I32, "-1", (-1i32 as u32) as u128),
        (I64, "-1", (-1i64 as u64) as u128),
        (I64, "-9223372036854775808", (i64::MIN as u64) as u128),
        (U16, "65535", 65535),
        (U32, "4294967295", 4294967295),
        (U64, "18446744073709551615", u64::MAX as u128),
        // A negated zero names the same value `0` does.
        (U128, "-0", 0),
        (UUID, "-0", 0),
        (UUID, UUID_LIT, 0x550e8400_e29b_41d4_a716_446655440000),
    ] {
        let s = pk_schema(tc);
        let pk = written_pk(&[lit, "0"], &s).unwrap_or_else(|e| panic!("{tc:?} {lit}: {e}"));
        assert_eq!(pk, s.opk_key_cols(&[native]), "{tc:?} {lit}");
    }
    let s = compound_schema_u64_u64();
    let mut want = [0u8; 16];
    want[..8].copy_from_slice(&1u64.to_be_bytes());
    want[8..].copy_from_slice(&2u64.to_be_bytes());
    assert_eq!(written_pk(&["1", "2", "99"], &s).unwrap().pk_bytes(), want);
    // A stride past 16 bytes.
    let wide = schema(
        vec![col("a", U64), col("b", U64), col("c", U128), ncol("v", I64)],
        &[0, 1, 2],
    );
    let mut want = [0u8; 32];
    want[..8].copy_from_slice(&1u64.to_be_bytes());
    want[8..16].copy_from_slice(&2u64.to_be_bytes());
    want[16..].copy_from_slice(&3u128.to_be_bytes());
    assert_eq!(written_pk(&["1", "2", "3", "99"], &wide).unwrap().pk_bytes(), want);
}

/// A PK cell its column cannot hold is rejected naming the column.
#[test]
fn a_pk_cell_its_column_cannot_hold_is_rejected() {
    use TypeCode::*;
    for (tc, lit, needles) in [
        (U64, "-1", &["out of range", "'id'"][..]),
        (U128, "-1", &["out of range", "'id'"]),
        (UUID, "'not-a-uuid'", &["invalid UUID", "'id'"]),
        (I64, "NULL", &["violates NOT NULL"]),
    ] {
        let m = rejected(written_pk(&[lit, "0"], &pk_schema(tc)));
        for needle in needles {
            assert!(m.contains(needle), "{tc:?} {lit}: {m}");
        }
    }
}

/// A statement whose last SERIAL id passes the column type's maximum is refused
/// before any row is pushed; one whose last id is the maximum is not.
#[test]
fn a_serial_id_past_the_type_maximum_is_exhausted() {
    let max = i16::MAX as u64;
    let schema = pk_schema(TypeCode::I16);
    let plan = PkPlan::serial(max - 1, 2, TypeCode::I16).unwrap();
    let mut dst = PkColumn::empty_for_schema(&schema);
    plan.push(&schema, 1, &[], &mut dst).unwrap();
    assert_eq!(dst.get_bytes(0), schema.opk_key_cols(&[max as u128]).pk_bytes());
    for (base, n, next) in [(max, 2, max + 1), (max + 5, 1, max + 5), (u64::MAX, 2, u64::MAX)] {
        match PkPlan::serial(base, n, TypeCode::I16) {
            Err(GnitzSqlError::Rejected(m)) => {
                assert!(m.contains(&format!("exhausted: next value {next} ")), "{m}")
            }
            _ => panic!("expected exhaustion for base {base}, {n} rows"),
        }
    }
}
