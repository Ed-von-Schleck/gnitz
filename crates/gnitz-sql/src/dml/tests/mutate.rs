use super::*;
use crate::test_support::{batch_2col, col_def, parse_stmt, two_col};
use gnitz_core::TypeCode;
use sqlparser::ast::Statement;

/// The SET list of an UPDATE, as parsed.
fn assignments(sql: &str) -> Vec<Assignment> {
    match parse_stmt(sql) {
        Statement::Update(u) => u.assignments,
        other => panic!("not an UPDATE: {other}"),
    }
}

/// One `seen` list serves every assignment of a statement, so the second
/// assignment to a column is caught by name whichever clause it sits in.
#[test]
fn a_column_assigned_twice_is_rejected() {
    let schema = two_col(TypeCode::I64);
    let mut seen = Vec::new();
    let [first, second] = assignments("UPDATE t SET val = 1, val = 2").try_into().unwrap();
    assert_eq!(resolve_set_target(&first, &schema, &mut seen, "UPDATE SET").unwrap(), 1);
    match resolve_set_target(&second, &schema, &mut seen, "UPDATE SET") {
        Err(GnitzSqlError::Bind(m)) => assert!(m.contains("multiple assignments to column 'val'"), "{m}"),
        other => panic!("expected Bind, got {other:?}"),
    }
}

/// A constant is range-checked against the column's width before any row is
/// read; one that fits classifies as a constant.
#[test]
fn a_constant_set_value_is_range_checked_at_bind() {
    let schema = two_col(TypeCode::U8);
    match classify_set_rhs(&BoundExpr::LitInt(300), 1, &schema) {
        Err(GnitzSqlError::Bind(m)) => assert!(m.contains("out of range"), "{m}"),
        Err(other) => panic!("expected Bind, got {other:?}"),
        Ok(_) => panic!("300 must not fit a U8 column"),
    }
    assert!(matches!(
        classify_set_rhs(&BoundExpr::LitInt(255), 1, &schema),
        Ok(SetProgram::Const(ColumnValue::Int(255)))
    ));
}

/// Compile a SET list the way the RMW closure does — against the schema the
/// rows carry. A test whose RHS does not compile is a bug in the test.
fn programs(assignments: &[(usize, BoundExpr)], schema: &Schema) -> Vec<(usize, SetProgram)> {
    assignments
        .iter()
        .map(|(ci, e)| {
            (
                *ci,
                classify_set_rhs(e, *ci, schema).expect("test SET RHS must compile"),
            )
        })
        .collect()
}

// ------------------------------------------------------------------
// write_set_rows must update the null bitmap for assignments
// ------------------------------------------------------------------

#[test]
fn test_write_set_clears_null_bit_on_non_null_assignment() {
    // Existing row: val = NULL (null bit set). Assignment: SET val = 99.
    // Expected: val = 99, null bit cleared.
    let schema = two_col(TypeCode::I64);
    let current = batch_2col(vec![0u8; 8], TypeCode::I64, 0b1); // val is NULL

    let assignments = vec![(1usize, BoundExpr::LitInt(99))];
    let mut dst = ZSetBatch::new(&schema);
    write_set_rows(&current, &programs(&assignments, &schema), &schema, &mut dst).unwrap();

    assert_eq!(
        dst.nulls[0] & 0b1,
        0,
        "null bit must be cleared after non-null assignment"
    );
    if let ColData::Fixed(ref buf) = dst.columns[1] {
        assert_eq!(i64::from_le_bytes(buf[..8].try_into().unwrap()), 99);
    }
}

#[test]
fn test_write_set_sets_null_bit_when_source_col_is_null() {
    // Three-column schema: pk, a (non-null), b (null).
    // Assignment: SET a = b (b is NULL → a should become NULL).
    let schema = Schema {
        columns: vec![
            col_def("pk", TypeCode::U64, false),
            col_def("a", TypeCode::I64, true),
            col_def("b", TypeCode::I64, true),
        ],
        pk_cols: vec![0],
    };
    let mut current = ZSetBatch::new(&schema);
    current.pks.push_u128(1u128);
    current.weights.push(1);
    current.nulls.push(0b10); // payload bit 1 (b) is NULL; bit 0 (a) is non-null
    if let ColData::Fixed(ref mut buf) = current.columns[1] {
        buf.extend_from_slice(&5i64.to_le_bytes());
    }
    if let ColData::Fixed(ref mut buf) = current.columns[2] {
        buf.extend_from_slice(&[0u8; 8]);
    }

    // SET a = b  (ColRef(2) = b, which is NULL in current)
    let assignments = vec![(1usize, BoundExpr::ColRef(2))];
    let mut dst = ZSetBatch::new(&schema);
    write_set_rows(&current, &programs(&assignments, &schema), &schema, &mut dst).unwrap();

    // a's null bit (payload_idx 0 → bit 0) must now be set
    assert_ne!(dst.nulls[0] & 0b01, 0, "a must be null after SET a = NULL_col");
    // b's null bit (payload_idx 1 → bit 1) must remain set (not touched)
    assert_ne!(dst.nulls[0] & 0b10, 0, "b must remain null");
}

#[test]
fn test_write_set_preserves_null_bits_for_unassigned_cols() {
    // Unassigned columns must carry their original null status unchanged.
    let schema = Schema {
        columns: vec![
            col_def("pk", TypeCode::U64, false),
            col_def("a", TypeCode::I64, true),
            col_def("b", TypeCode::I64, true),
        ],
        pk_cols: vec![0],
    };
    let mut current = ZSetBatch::new(&schema);
    current.pks.push_u128(1u128);
    current.weights.push(1);
    current.nulls.push(0b10); // b is NULL, a is not
    if let ColData::Fixed(ref mut buf) = current.columns[1] {
        buf.extend_from_slice(&5i64.to_le_bytes());
    }
    if let ColData::Fixed(ref mut buf) = current.columns[2] {
        buf.extend_from_slice(&[0u8; 8]);
    }

    // Only assign to a; b is untouched
    let assignments = vec![(1usize, BoundExpr::LitInt(10))];
    let mut dst = ZSetBatch::new(&schema);
    write_set_rows(&current, &programs(&assignments, &schema), &schema, &mut dst).unwrap();

    assert_eq!(dst.nulls[0] & 0b01, 0, "a must not be null (assigned non-null)");
    assert_ne!(dst.nulls[0] & 0b10, 0, "b must remain null (unassigned)");
}

#[test]
fn write_set_rows_carries_blob_column_through() {
    // UPDATE assigns `v` only; the unmodified BLOB column must carry
    // through. Red against the missing `ColData::Bytes` arm (unreachable!).
    let schema = Schema {
        columns: vec![
            col_def("pk", TypeCode::U64, false),
            col_def("b", TypeCode::Blob, true),
            col_def("v", TypeCode::I64, true),
        ],
        pk_cols: vec![0],
    };
    let mut current = ZSetBatch::new(&schema);
    current.pks.push_u128(1u128);
    current.weights.push(1);
    current.nulls.push(0);
    if let ColData::Bytes(v) = &mut current.columns[1] {
        v.push(Some(vec![1, 2, 3]));
    }
    if let ColData::Fixed(buf) = &mut current.columns[2] {
        buf.extend_from_slice(&7i64.to_le_bytes());
    }

    let assignments = vec![(2usize, BoundExpr::LitInt(99))];
    let mut dst = ZSetBatch::new(&schema);
    write_set_rows(&current, &programs(&assignments, &schema), &schema, &mut dst).unwrap();

    if let ColData::Bytes(v) = &dst.columns[1] {
        assert_eq!(v[0].as_deref(), Some(&[1u8, 2, 3][..]));
    } else {
        panic!("expected Bytes column carried through");
    }
    if let ColData::Fixed(buf) = &dst.columns[2] {
        assert_eq!(i64::from_le_bytes(buf[..8].try_into().unwrap()), 99);
    }
}

#[test]
fn build_merged_row_takes_pk_from_pk_src_and_carries_from_carry_src() {
    // DO UPDATE shape: PK from the incoming (excluded) row; null-seed and
    // carried columns from the existing stored row. Schema: pk, v (I64), b (Blob).
    let schema = Schema {
        columns: vec![
            col_def("pk", TypeCode::U64, false),
            col_def("v", TypeCode::I64, true),
            col_def("b", TypeCode::Blob, true),
        ],
        pk_cols: vec![0],
    };

    // pk_src (incoming): PK = 100; payload irrelevant (only the PK is read).
    let mut pk_src = ZSetBatch::new(&schema);
    pk_src.pks.push_u128(100u128);
    pk_src.weights.push(1);
    pk_src.nulls.push(0);
    if let ColData::Fixed(buf) = &mut pk_src.columns[1] {
        buf.extend_from_slice(&0i64.to_le_bytes());
    }
    if let ColData::Bytes(v) = &mut pk_src.columns[2] {
        v.push(Some(vec![9, 9, 9]));
    }

    // carry_src (existing): PK = 200; v = 7 (non-null); b = [1,2,3] to carry.
    let mut carry_src = ZSetBatch::new(&schema);
    carry_src.pks.push_u128(200u128);
    carry_src.weights.push(1);
    carry_src.nulls.push(0);
    if let ColData::Fixed(buf) = &mut carry_src.columns[1] {
        buf.extend_from_slice(&7i64.to_le_bytes());
    }
    if let ColData::Bytes(v) = &mut carry_src.columns[2] {
        v.push(Some(vec![1, 2, 3]));
    }

    // Resolver: assign v = NULL (must SET its null bit); leave b unassigned (carry).
    let mut dst = ZSetBatch::new(&schema);
    let payload = merge_payload_plan(&schema);
    build_merged_row(&pk_src, 0, &carry_src, 0, &payload, &mut dst, |ci| {
        if ci == 1 {
            Some(ColumnValue::Null)
        } else {
            None
        }
    })
    .unwrap();

    assert_eq!(dst.pks.get_tuple(0), pk_src.pks.get_tuple(0));
    assert_ne!(dst.pks.get_tuple(0), carry_src.pks.get_tuple(0));
    // v's null bit (payload_idx 0) must be SET after the NULL assignment.
    assert_ne!(dst.nulls[0] & 0b01, 0, "v must be null after SET v = NULL");
    // b carried from carry_src ([1,2,3]), not pk_src ([9,9,9]).
    if let ColData::Bytes(v) = &dst.columns[2] {
        assert_eq!(v[0].as_deref(), Some(&[1u8, 2, 3][..]));
    } else {
        panic!("expected carried Bytes column");
    }
}

// ------------------------------------------------------------------
// SET right-hand sides through the shared evaluator
// ------------------------------------------------------------------

/// The written `i64` of `dst`'s single-row payload column `ci`.
fn written_i64(dst: &ZSetBatch, ci: usize) -> i64 {
    match &dst.columns[ci] {
        ColData::Fixed(buf) => i64::from_le_bytes(buf[..8].try_into().unwrap()),
        other => panic!("expected a Fixed column, got {other:?}"),
    }
}

/// A numeric RHS reading a **nullable** source column: the compiled program
/// resolves `no_nulls = false`, so a NULL source must come back as
/// `ColumnValue::Null` and set the destination's null bit rather than reading
/// the filler zeros as a real `0`.
#[test]
fn set_numeric_over_a_nullable_column() {
    let schema = Schema {
        columns: vec![
            col_def("pk", TypeCode::U64, false),
            col_def("a", TypeCode::I64, true),
            col_def("b", TypeCode::I64, true),
        ],
        pk_cols: vec![0],
    };
    // b = 5 (non-null) in row 0, b = NULL in row 1.
    let mut current = ZSetBatch::new(&schema);
    for (i, (bits, null_word)) in [(5i64, 0u64), (0, 0b10)].into_iter().enumerate() {
        current.pks.push_u128(i as u128 + 1);
        current.weights.push(1);
        current.nulls.push(null_word);
        if let ColData::Fixed(buf) = &mut current.columns[1] {
            buf.extend_from_slice(&0i64.to_le_bytes());
        }
        if let ColData::Fixed(buf) = &mut current.columns[2] {
            buf.extend_from_slice(&bits.to_le_bytes());
        }
    }
    // SET a = b + 1
    let rhs = BoundExpr::BinOp(
        Box::new(BoundExpr::ColRef(2)),
        crate::ir::BinOp::Add,
        Box::new(BoundExpr::LitInt(1)),
    );
    let mut dst = ZSetBatch::new(&schema);
    write_set_rows(&current, &programs(&[(1, rhs)], &schema), &schema, &mut dst).unwrap();

    assert_eq!(written_i64(&dst, 1), 6, "non-null source: b + 1");
    assert_eq!(dst.nulls[0] & 0b01, 0, "row 0's a is not null");
    assert_ne!(dst.nulls[1] & 0b01, 0, "NULL + 1 is NULL, and its bit must be set");
}

/// A SET RHS reading the PK column — the PK region through the adapter, which
/// no other SET test exercises. It is also the shape a deferred cross-column
/// cell copy would have aborted on: a PK column's slot in `ZSetBatch.columns`
/// is an empty placeholder.
#[test]
fn set_reads_the_pk_column() {
    let schema = two_col(TypeCode::I64);
    let mut current = ZSetBatch::new(&schema);
    current.pks.push_u128(41u128);
    current.weights.push(1);
    current.nulls.push(0);
    if let ColData::Fixed(buf) = &mut current.columns[1] {
        buf.extend_from_slice(&0i64.to_le_bytes());
    }
    // SET val = pk + 1
    let plus_one = BoundExpr::BinOp(
        Box::new(BoundExpr::ColRef(0)),
        crate::ir::BinOp::Add,
        Box::new(BoundExpr::LitInt(1)),
    );
    let mut dst = ZSetBatch::new(&schema);
    write_set_rows(&current, &programs(&[(1, plus_one)], &schema), &schema, &mut dst).unwrap();
    assert_eq!(written_i64(&dst, 1), 42);

    // And the bare `SET val = pk` form.
    let mut dst = ZSetBatch::new(&schema);
    write_set_rows(
        &current,
        &programs(&[(1, BoundExpr::ColRef(0))], &schema),
        &schema,
        &mut dst,
    )
    .unwrap();
    assert_eq!(written_i64(&dst, 1), 41);
}

/// A float-typed RHS into an integer column is rejected at compile — without
/// it the raw f64 bit pattern would pass `append_column_value`'s guard and
/// commit as a nonsense integer.
#[test]
fn set_int_column_from_float_expression_rejects() {
    let schema = Schema {
        columns: vec![
            col_def("pk", TypeCode::U64, false),
            col_def("n", TypeCode::I64, true),
            col_def("f", TypeCode::F64, true),
        ],
        pk_cols: vec![0],
    };
    let Err(err) = classify_set_rhs(&BoundExpr::ColRef(2), 1, &schema) else {
        panic!("SET int = float must reject");
    };
    assert!(
        err.to_string().contains("floating-point"),
        "error must name the cause: {err}"
    );
    // A float *comparison* is integer-valued (0/1) and stays servable.
    let cmp = BoundExpr::BinOp(
        Box::new(BoundExpr::ColRef(2)),
        crate::ir::BinOp::Gt,
        Box::new(BoundExpr::LitFloat(1.5)),
    );
    assert!(classify_set_rhs(&cmp, 1, &schema).is_ok());
}

/// A computed *string* RHS reads back as a string: as a scalar it would return
/// an i64 — the 16-byte descriptor's prefix as a garbage integer — so the class
/// rides the evaluated result, and nothing downstream re-asks it.
#[test]
fn a_computed_string_rhs_routes_to_the_string_arm_and_evaluates() {
    let schema = two_col(TypeCode::String);
    let upper = BoundExpr::StrCall {
        f: crate::ir::StrFunc::Upper,
        arg: Box::new(BoundExpr::ColRef(1)),
    };
    let p = classify_set_rhs(&upper, 1, &schema).expect("compiles");
    assert!(matches!(&p, SetProgram::Expr(ev) if ev.result_is_str()));

    // Drive it over a real row: a bare column read (`StrCol`) and the
    // computed form must agree on everything but the transform.
    let mut batch = ZSetBatch::new(&schema);
    batch.pks.push_u128(1u128);
    batch.weights.push(1);
    batch.nulls.push(0);
    if let ColData::Strings(ref mut v) = batch.columns[1] {
        v.push(Some("hello".to_string()));
    }
    let mut bufs = ViewBuffers::default();
    let view = bufs.view(&batch, &schema);
    match eval_set_value(&bind_set_program(&p, &view), &view, 0) {
        ColumnValue::Str(s) => assert_eq!(s, "HELLO"),
        _ => panic!("expected a string value"),
    }
}

/// The target-kind check runs on the string arm too, so a string-valued RHS
/// against an integer column is rejected at compile rather than surfacing
/// only once a row matched.
#[test]
fn a_string_rhs_against_an_integer_column_is_rejected() {
    let schema = two_col(TypeCode::I64);
    let concat = BoundExpr::ConcatN {
        args: vec![BoundExpr::LitInt(1)],
    };
    assert!(matches!(
        classify_set_rhs(&concat, 1, &schema),
        Err(GnitzSqlError::Bind(_))
    ));
}
