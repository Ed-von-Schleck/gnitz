use super::*;
use crate::test_support::{col, ncol, parse_expr_sql, parse_stmt, rejected, schema};
use gnitz_core::BatchAppender;
use gnitz_expr::{payload_bytes, payload_is_null, payload_u64};
use gnitz_wire::TypeCode;
use sqlparser::ast::Statement;

/// `(pk U64 PK, cols…)`.
fn table(cols: Vec<ColumnDef>) -> Schema {
    let mut columns = vec![col("pk", TypeCode::U64)];
    columns.extend(cols);
    schema(columns, &[0])
}

/// The SET list of `UPDATE t SET <set>`, bound and compiled against `schema`.
fn compile(set: &str, schema: &Schema) -> Result<Vec<SetCol>, GnitzSqlError> {
    let Statement::Update(u) = parse_stmt(&format!("UPDATE t SET {set}")) else {
        panic!("not an UPDATE");
    };
    bind_set_list(&u.assignments, schema, "t", SetClause::Update)
}

/// `rows` rewritten by `SET <set>`. A SET list that does not compile is a bug in
/// the test.
fn run(set: &str, schema: &Schema, rows: ZSetBatch) -> Result<ZSetBatch, GnitzSqlError> {
    apply_set(
        &mut compile(set, schema).expect("test SET list must compile"),
        rows,
        None,
        schema,
    )
}

/// A batch of `schema`, one `fill` call per row.
fn rows_of(schema: &Schema, n: usize, mut fill: impl FnMut(&mut BatchAppender<'_>, usize)) -> ZSetBatch {
    let mut b = ZSetBatch::new(schema);
    let mut app = BatchAppender::new(&mut b, schema);
    for r in 0..n {
        app.add_row(r as u128 + 1, 1);
        fill(&mut app, r);
    }
    b
}

// ------------------------------------------------------------------
// Binding the target list
// ------------------------------------------------------------------

#[test]
fn a_column_assigned_twice_is_rejected() {
    let schema = table(vec![ncol("val", TypeCode::I64)]);
    let m = rejected(compile("val = 1, val = 2", &schema));
    assert!(m.contains("multiple assignments to column 'val'"), "{m}");
}

/// PostgreSQL refuses `SET t.val = …`; a qualifier must not be dropped.
#[test]
fn a_qualified_target_is_rejected() {
    let schema = table(vec![ncol("val", TypeCode::I64)]);
    match compile("t.val = 1", &schema) {
        Err(GnitzSqlError::Rejected(m)) => assert!(m.contains("column must be a simple identifier"), "{m}"),
        Err(e) => panic!("expected Plan, got {e:?}"),
        Ok(_) => panic!("a qualified target must be rejected"),
    }
}

// ------------------------------------------------------------------
// Literals, through INSERT's cell encoder
// ------------------------------------------------------------------

/// A constant is range-checked against the column's width before any row is
/// read; one that fits classifies as a constant.
#[test]
fn a_constant_set_value_is_range_checked_at_bind() {
    let schema = table(vec![ncol("val", TypeCode::U8)]);
    let m = rejected(compile("val = 300", &schema));
    assert!(m.contains("out of range"), "{m}");
    let set = compile("val = 255", &schema).unwrap();
    assert!(matches!(&set[0].rhs, SetRhs::Const { cell, .. } if cell == &[255]));
}

/// A fraction into an integer column rounds as a DECIMAL→integer CAST does; a
/// string that spells no integer is refused, naming it.
#[test]
fn a_float_or_string_literal_into_an_integer_column() {
    let schema = table(vec![ncol("i", TypeCode::I64)]);
    let set = compile("i = 1.5", &schema).unwrap();
    assert!(matches!(&set[0].rhs, SetRhs::Const { cell, .. } if cell == &2i64.to_le_bytes()));
    let m = rejected(compile("i = 'abc'", &schema));
    assert!(m.contains("column 'i': invalid I64 literal: 'abc'"), "{m}");
}

#[test]
fn a_non_null_assignment_clears_the_null_bit() {
    let schema = table(vec![ncol("val", TypeCode::I64)]);
    let rows = rows_of(&schema, 1, |a, _| {
        a.null();
    });
    let out = run("val = 99", &schema, rows).unwrap();
    assert!(!payload_is_null(&out, 0, 0));
    assert_eq!(payload_u64(&out, 0, 0) as i64, 99);
    assert_eq!(out.weights, [1]);
}

#[test]
fn unassigned_columns_carry_their_null_bits() {
    let schema = table(vec![ncol("a", TypeCode::I64), ncol("b", TypeCode::I64)]);
    let rows = rows_of(&schema, 1, |a, _| {
        a.i64_val(5).null();
    });
    let out = run("a = 10", &schema, rows).unwrap();
    assert!(!payload_is_null(&out, 0, 0), "a assigned non-null");
    assert!(payload_is_null(&out, 0, 1), "b unassigned stays null");
}

#[test]
fn a_decimal_literal_rounds_to_the_column_scale() {
    let schema = table(vec![ColumnDef::typed("d", ColType::decimal(2), true)]);
    let rows = rows_of(&schema, 1, |a, _| {
        a.i64_val(0);
    });
    assert_eq!(payload_u64(&run("d = 1.005", &schema, rows).unwrap(), 0, 0) as i64, 101);
    // A string literal spells a DECIMAL too, as it does in an INSERT cell.
    let rows = rows_of(&schema, 1, |a, _| {
        a.i64_val(0);
    });
    assert_eq!(
        payload_u64(&run("d = '1.25'", &schema, rows).unwrap(), 0, 0) as i64,
        125
    );
}

#[test]
fn a_wide_negative_literal_keeps_its_sign() {
    for tc in [TypeCode::U64, TypeCode::U128] {
        let schema = table(vec![ncol("u", tc)]);
        let m = rejected(compile("u = -18446744073709551615", &schema));
        assert!(m.contains("out of range"), "{tc:?}: {m}");
    }
    let schema = table(vec![ncol("f", TypeCode::F64)]);
    let rows = rows_of(&schema, 1, |a, _| {
        a.f64_val(0.0);
    });
    let out = run("f = -18446744073709551615", &schema, rows).unwrap();
    assert_eq!(f64::from_bits(payload_u64(&out, 0, 0)), -18446744073709551615.0);
}

#[test]
fn a_float_literal_writes_a_double_column() {
    let schema = table(vec![ncol("f", TypeCode::F64)]);
    let rows = rows_of(&schema, 1, |a, _| {
        a.null();
    });
    let out = run("f = 1.5", &schema, rows).unwrap();
    assert_eq!(f64::from_bits(payload_u64(&out, 0, 0)), 1.5);
    assert!(!payload_is_null(&out, 0, 0));
}

/// A string literal is spilled once into the result arena, however many rows
/// take it, and the arena holds nothing else.
#[test]
fn a_long_string_literal_spills_once() {
    let schema = table(vec![ncol("s", TypeCode::String)]);
    let rows = rows_of(&schema, 3, |a, _| {
        a.str_val("a");
    });
    let long = "x".repeat(40);
    let out = run(&format!("s = '{long}'"), &schema, rows).unwrap();
    assert_eq!(out.blob.len(), long.len());
    for r in 0..3 {
        assert_eq!(payload_bytes(&out, r, 0), long.as_bytes());
    }
}

// ------------------------------------------------------------------
// Column copies
// ------------------------------------------------------------------

#[test]
fn a_copy_from_a_null_source_sets_the_null_bit() {
    let schema = table(vec![ncol("a", TypeCode::I64), ncol("b", TypeCode::I64)]);
    let rows = rows_of(&schema, 1, |a, _| {
        a.i64_val(5).null();
    });
    let mut set = compile("a = b", &schema).unwrap();
    assert!(matches!(set[0].rhs, SetRhs::Copy { src: 1, .. }));
    let out = apply_set(&mut set, rows, None, &schema).unwrap();
    assert!(payload_is_null(&out, 0, 0), "a takes b's NULL");
    assert!(payload_is_null(&out, 0, 1), "b stays null");
}

/// Every right-hand side reads the rows as they were.
#[test]
fn a_swap_reads_the_old_row() {
    let schema = table(vec![ncol("a", TypeCode::I64), ncol("b", TypeCode::I64)]);
    let rows = rows_of(&schema, 1, |a, _| {
        a.i64_val(1).i64_val(2);
    });
    let out = run("a = b, b = a", &schema, rows).unwrap();
    assert_eq!((payload_u64(&out, 0, 0) as i64, payload_u64(&out, 0, 1) as i64), (2, 1));
}

#[test]
fn same_type_copies_work_for_float_and_uuid_columns() {
    let schema = table(vec![
        ncol("f", TypeCode::F64),
        ncol("g", TypeCode::F64),
        ncol("u", TypeCode::UUID),
        ncol("w", TypeCode::UUID),
    ]);
    let rows = rows_of(&schema, 1, |a, _| {
        a.f64_val(0.0).f64_val(2.5).u128_val(0).u128_val(u128::MAX - 7);
    });
    let out = run("f = g, u = w", &schema, rows).unwrap();
    assert_eq!(f64::from_bits(payload_u64(&out, 0, 0)), 2.5);
    assert_eq!(
        u128::from_le_bytes(out.payload[2].bytes[..16].try_into().unwrap()),
        u128::MAX - 7
    );
}

/// A non-string SET keeps the rows' arena whole: an unassigned BLOB column's
/// cells still point into it.
#[test]
fn a_non_string_set_leaves_the_arena_unchanged() {
    let schema = table(vec![ncol("b", TypeCode::Blob), ncol("v", TypeCode::I64)]);
    let long = [7u8; 30];
    let rows = rows_of(&schema, 1, |a, _| {
        a.bytes_val(&long).i64_val(7);
    });
    let blob = rows.blob.clone();
    let out = run("v = 99", &schema, rows).unwrap();
    assert_eq!(out.blob, blob);
    assert_eq!(payload_bytes(&out, 0, 0), long);
    assert_eq!(payload_u64(&out, 0, 1) as i64, 99);
}

/// Assigning one string column rebuilds the others into a fresh arena, so the
/// replaced cell's spill does not survive.
#[test]
fn a_text_assignment_leaves_only_referenced_spill() {
    let schema = table(vec![ncol("s", TypeCode::String), ncol("t", TypeCode::String)]);
    let (old, kept) = ("o".repeat(30), "k".repeat(20));
    let rows = rows_of(&schema, 1, |a, _| {
        a.str_val(&old).str_val(&kept);
    });
    let out = run("s = 'x'", &schema, rows).unwrap();
    assert_eq!(out.blob.len(), kept.len());
    assert_eq!(payload_bytes(&out, 0, 0), b"x");
    assert_eq!(payload_bytes(&out, 0, 1), kept.as_bytes());
}

// ------------------------------------------------------------------
// Computed values
// ------------------------------------------------------------------

/// A NULL source must come back as NULL and set the destination's bit rather
/// than reading the filler zeros as a real `0`.
#[test]
fn a_computed_value_over_a_nullable_column() {
    let schema = table(vec![ncol("a", TypeCode::I64), ncol("b", TypeCode::I64)]);
    let rows = rows_of(&schema, 2, |a, r| {
        a.i64_val(0);
        if r == 0 {
            a.i64_val(5);
        } else {
            a.null();
        }
    });
    let out = run("a = b + 1", &schema, rows).unwrap();
    assert_eq!(payload_u64(&out, 0, 0) as i64, 6);
    assert!(!payload_is_null(&out, 0, 0));
    assert!(payload_is_null(&out, 1, 0), "NULL + 1 is NULL");
}

#[test]
fn set_reads_the_pk_column() {
    let schema = table(vec![ncol("val", TypeCode::I64)]);
    for (set, want) in [("val = pk + 1", 2), ("val = pk", 1)] {
        let rows = rows_of(&schema, 1, |a, _| {
            a.i64_val(0);
        });
        assert_eq!(
            payload_u64(&run(set, &schema, rows).unwrap(), 0, 0) as i64,
            want,
            "{set}"
        );
    }
}

/// A float-typed RHS into an integer column is rejected at compile — the raw f64
/// bit pattern would otherwise commit as a nonsense integer.
#[test]
fn set_int_column_from_float_expression_rejects() {
    let schema = table(vec![ncol("n", TypeCode::I64), ncol("f", TypeCode::F64)]);
    let Err(err) = compile("n = f", &schema) else {
        panic!("SET int = float must reject");
    };
    assert!(err.to_string().contains("floating-point"), "{err}");
    // A float *comparison* is integer-valued (0/1) and stays servable.
    assert!(compile("n = f > 1.5", &schema).is_ok());
}

#[test]
fn a_computed_string_evaluates_to_a_string() {
    let schema = table(vec![ncol("val", TypeCode::String)]);
    let mut set = compile("val = UPPER(val)", &schema).unwrap();
    assert!(matches!(&set[0].rhs, SetRhs::Expr { ev, .. } if ev.result_is_str()));
    let rows = rows_of(&schema, 1, |a, _| {
        a.str_val("hello");
    });
    assert_eq!(
        payload_bytes(&apply_set(&mut set, rows, None, &schema).unwrap(), 0, 0),
        b"HELLO"
    );
}

#[test]
fn a_string_rhs_against_an_integer_column_is_rejected() {
    let schema = table(vec![ncol("val", TypeCode::I64)]);
    let concat = BoundExpr::ConcatN { args: vec![BoundExpr::LitInt(1)] };
    rejected(classify_set_rhs(&concat, Scope::Existing, 1, &schema));
}

/// A computed value outside its column's range is rejected per row, not
/// truncated to the low bits; in range it encodes to the native image at every
/// width and sign.
#[test]
fn a_computed_value_is_range_checked_and_encodes_natively() {
    for (tc, v) in [
        (TypeCode::U8, 300i64),
        (TypeCode::U8, -1),
        (TypeCode::I8, 128),
        (TypeCode::U16, 70000),
        (TypeCode::I16, -32769),
        (TypeCode::U32, -1),
        (TypeCode::U64, -1),
    ] {
        let schema = table(vec![ncol("src", TypeCode::I64), ncol("dst", tc)]);
        let rows = rows_of(&schema, 1, |a, _| {
            a.i64_val(v).null();
        });
        let m = rejected(run("dst = src + 0", &schema, rows));
        assert!(m.contains("out of range"), "{tc:?} {v}: {m}");
    }
    for (tc, src_tc, v, want) in [
        (TypeCode::U8, TypeCode::I64, 255u64, vec![255u8]),
        (TypeCode::I8, TypeCode::I64, (-5i64) as u64, vec![(-5i8) as u8]),
        (TypeCode::U16, TypeCode::I64, 65535, 65535u16.to_le_bytes().to_vec()),
        (
            TypeCode::I16,
            TypeCode::I64,
            (-2i64) as u64,
            (-2i16).to_le_bytes().to_vec(),
        ),
        (
            TypeCode::U32,
            TypeCode::I64,
            4294967295,
            4294967295u32.to_le_bytes().to_vec(),
        ),
        (
            TypeCode::I32,
            TypeCode::I64,
            (-1i64) as u64,
            (-1i32).to_le_bytes().to_vec(),
        ),
        (TypeCode::U64, TypeCode::U64, u64::MAX, u64::MAX.to_le_bytes().to_vec()),
        (
            TypeCode::I64,
            TypeCode::I64,
            i64::MIN as u64,
            i64::MIN.to_le_bytes().to_vec(),
        ),
    ] {
        let schema = table(vec![ncol("src", src_tc), ncol("dst", tc)]);
        let rows = rows_of(&schema, 1, |a, _| {
            a.u64_val(v).null();
        });
        let out = run("dst = src + 0", &schema, rows).unwrap();
        assert_eq!(out.payload[1].bytes, want, "{tc:?}");
    }
}

#[test]
fn a_u64_value_past_i64_max_writes() {
    let schema = table(vec![ncol("u", TypeCode::U64)]);
    let rows = rows_of(&schema, 1, |a, _| {
        a.u64_val(1 << 63);
    });
    assert_eq!(
        payload_u64(&run("u = u + 1", &schema, rows).unwrap(), 0, 0),
        (1 << 63) + 1
    );
}

#[test]
fn a_u64_value_past_i64_max_is_out_of_range_for_i64() {
    let schema = table(vec![ncol("i", TypeCode::I64), ncol("u", TypeCode::U64)]);
    let rows = rows_of(&schema, 1, |a, _| {
        a.i64_val(0).u64_val(1 << 63);
    });
    let m = rejected(run("i = u", &schema, rows));
    assert!(m.contains("out of range"), "{m}");
}

#[test]
fn a_decimal_source_rounds_into_an_integer_column() {
    let schema = table(vec![
        ncol("i", TypeCode::I64),
        ColumnDef::typed("d", ColType::decimal(2), true),
    ]);
    let rows = rows_of(&schema, 2, |a, r| {
        a.i64_val(0).i64_val([150, 149][r]);
    });
    let out = run("i = d", &schema, rows).unwrap();
    assert_eq!((payload_u64(&out, 0, 0) as i64, payload_u64(&out, 1, 0) as i64), (2, 1));
}

#[test]
fn a_date_source_converts_into_a_timestamp_column() {
    let schema = table(vec![ncol("d", TypeCode::Date), ncol("ts", TypeCode::Timestamp)]);
    let rows = rows_of(&schema, 1, |a, _| {
        a.int_val(2).i64_val(0);
    });
    let out = run("ts = d", &schema, rows).unwrap();
    assert_eq!(payload_u64(&out, 0, 1) as i64, 2 * 86_400_000_000);
}

/// An `EXCLUDED.col` reference is found wherever `expr_operands` walks, so a
/// compound RHS is rejected rather than the qualifier being silently dropped
/// and the reference bound to the existing row's column.
#[test]
fn an_excluded_reference_is_found_at_any_depth() {
    for (src, found) in [
        ("EXCLUDED.a", true),
        ("excluded.a", true),
        ("(EXCLUDED.a)", true),
        ("val + COALESCE(EXCLUDED.a, 0)", true),
        ("val + 1", false),
        ("t.a", false),
        ("excluded", false),
    ] {
        assert_eq!(expr_contains_excluded(&parse_expr_sql(src)), found, "{src}");
    }
}
