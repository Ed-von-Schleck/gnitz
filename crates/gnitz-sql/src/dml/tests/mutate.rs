use super::*;
use crate::test_support::Cell::{self, Int, Null, Str, F64};
use crate::test_support::{
    assert_rejects, batch_of, catalog, col, ncol, parse_stmt, rel, rows_of, table, typed_schema, TestCatalog,
};
use gnitz_core::BatchAppender;
use gnitz_wire::{RelClass, TypeCode};
use sqlparser::ast::Statement;

fn plan(cat: &dyn Catalog, sql: &str) -> Result<MutationPlan, GnitzSqlError> {
    match parse_stmt(sql) {
        Statement::Update(u) => plan_update(&u, cat),
        Statement::Delete(d) => plan_delete(&d, cat),
        _ => panic!("`{sql}` is neither an UPDATE nor a DELETE"),
    }
}

/// What `UPDATE t SET <set>` writes for the rows `held` of `schema`, each read at
/// weight 3.
fn updated(schema: &Schema, set: &str, held: &[&[Cell]]) -> Result<ZSetBatch, GnitzSqlError> {
    let cat = catalog(vec![("t", table(1, schema.columns.clone(), vec![0]))]);
    let mut plan = plan(&cat, &format!("UPDATE t SET {set}"))?;
    let mut rows = batch_of(schema, held);
    rows.weights.fill(3);
    delta(plan.set.as_deref_mut(), rows, schema)
}

const I: ColType = ColType::of(TypeCode::I64);
const F: ColType = ColType::of(TypeCode::F64);
const S: ColType = ColType::of(TypeCode::String);
const DATE: ColType = ColType::of(TypeCode::Date);
const TS: ColType = ColType::of(TypeCode::Timestamp);

/// A string past the inline prefix, spilled to the arena.
const LONG: &str = "a string past the inline prefix";

/// Over `t (pk, c1, c2, …)` with `pk = 1`: every assigned column takes its value,
/// read off the row as it was; every other column, null bits included, is carried;
/// and the row is written at weight +1 whatever weight it was read at.
#[test]
fn a_set_list_rewrites_the_row_it_read() {
    /// `(payload types, SET list, row read, row written)`.
    type Case<'a> = (&'a [ColType], &'a str, &'a [Cell<'a>], &'a [Cell<'a>]);
    let cases: &[Case] = &[
        // A literal, through INSERT's cell encoder.
        (&[I, I], "c1 = 99", &[Null, Null], &[Int(99), Null]),
        (&[I, I], "c1 = NULL", &[Int(5), Int(6)], &[Null, Int(6)]),
        (&[F], "c1 = 1.5", &[Null], &[F64(1.5)]),
        // A column of the target's own type is copied, NULL and spill included.
        (&[I, I], "c1 = c2", &[Int(5), Null], &[Null, Null]),
        (&[I, I], "c1 = c2, c2 = c1", &[Int(1), Int(2)], &[Int(2), Int(1)]),
        (&[F, F], "c1 = c2", &[F64(0.0), F64(2.5)], &[F64(2.5), F64(2.5)]),
        (&[S, S], "c1 = c2", &[Str("old"), Str(LONG)], &[Str(LONG), Str(LONG)]),
        // A computed value; NULL in is NULL out, not the filler zero.
        (&[I, I], "c1 = c2 + 1", &[Int(0), Int(5)], &[Int(6), Int(5)]),
        (&[I, I], "c1 = c2 + 1", &[Int(0), Null], &[Null, Null]),
        (&[I], "c1 = pk + 1", &[Int(0)], &[Int(2)]),
        (&[I], "c1 = pk", &[Int(0)], &[Int(1)]),
        (
            &[S, S],
            "c1 = UPPER(c2)",
            &[Str("x"), Str("hello")],
            &[Str("HELLO"), Str("hello")],
        ),
        (&[S, S], "c1 = UPPER(c2)", &[Str("x"), Null], &[Null, Null]),
        // A source of another type converts as a CAST to the target does.
        (
            &[I, ColType::decimal(2)],
            "c1 = c2",
            &[Int(0), Int(150)],
            &[Int(2), Int(150)],
        ),
        (
            &[I, ColType::decimal(2)],
            "c1 = c2",
            &[Int(0), Int(149)],
            &[Int(1), Int(149)],
        ),
        (
            &[ColType::decimal(2), I],
            "c1 = c2",
            &[Int(150), Int(5)],
            &[Int(500), Int(5)],
        ),
        (
            &[ColType::decimal(2), ColType::decimal(4)],
            "c1 = c2",
            &[Int(150), Int(12345)],
            &[Int(123), Int(12345)],
        ),
        (
            &[DATE, TS],
            "c2 = c1",
            &[Int(2), Int(0)],
            &[Int(2), Int(172_800_000_000)],
        ),
        (&[DATE, TS], "c1 = c2", &[Int(1), Int(-1)], &[Int(-1), Int(-1)]),
    ];
    for (tys, set, before, after) in cases {
        let schema = typed_schema(tys);
        let out = updated(&schema, set, &[before]).unwrap_or_else(|e| panic!("{set}: {e:?}"));
        assert_eq!(rows_of(&schema, &out), [(after.to_vec(), 1)], "{set} over {before:?}");
    }
}

/// A 16-byte column has no register, so it is assignable only as a copy.
#[test]
fn a_uuid_column_is_copied() {
    let schema = typed_schema(&[TypeCode::UUID, TypeCode::UUID]);
    let mut rows = ZSetBatch::new(&schema);
    BatchAppender::new(&mut rows)
        .add_row(1, 1)
        .u128_val(0)
        .u128_val(u128::MAX - 7);
    let cat = catalog(vec![("t", table(1, schema.columns.clone(), vec![0]))]);
    let mut plan = plan(&cat, "UPDATE t SET c1 = c2").unwrap();
    let out = delta(plan.set.as_deref_mut(), rows, &schema).unwrap();
    assert_eq!(out.payload[0].bytes, (u128::MAX - 7).to_le_bytes());
}

/// A computed value outside its column's range is refused per row, not truncated
/// to the low bits; in range it is written at the column's own width and sign.
#[test]
fn a_computed_value_is_range_checked_at_the_column_width() {
    use TypeCode::*;
    for (src, dst, v, fits) in [
        (I64, U8, 300, false),
        (I64, U8, -1, false),
        (I64, I8, 128, false),
        (I64, U16, 70000, false),
        (I64, I16, -32769, false),
        (I64, U32, -1, false),
        (I64, U64, -1, false),
        (U64, I64, 1 << 63, false),
        (I64, U8, 255, true),
        (I64, I8, -5, true),
        (I64, U16, 65535, true),
        (I64, I16, -2, true),
        (I64, U32, 4294967295, true),
        (I64, I32, -1, true),
        (U64, U64, u64::MAX as i128, true),
        (I64, I64, i64::MIN as i128, true),
    ] {
        let schema = typed_schema(&[src, dst]);
        let out = updated(&schema, "c2 = c1 + 0", &[&[Int(v), Null]]);
        match fits {
            true => assert_eq!(
                rows_of(&schema, &out.unwrap()),
                [(vec![Int(v), Int(v)], 1)],
                "{dst:?} {v}"
            ),
            false => assert_rejects(&format!("{dst:?} {v}"), out, "out of range"),
        }
    }
}

/// A NULL into a NOT NULL column is refused per row, whichever way the value is
/// produced.
#[test]
fn a_null_into_a_not_null_column_is_refused() {
    let schema = crate::test_support::schema(
        vec![
            col("pk", TypeCode::U64),
            ncol("a", TypeCode::I64),
            col("nn", TypeCode::I64),
        ],
        &[0],
    );
    for set in ["nn = NULL", "nn = a", "nn = a + 1"] {
        assert_rejects(
            set,
            updated(&schema, set, &[&[Null, Int(1)]]),
            "column 'nn' violates NOT NULL",
        );
    }
}

/// The result arena holds exactly the spill its cells reference: a string literal
/// is spilled once however many rows take it, assigning a string column drops the
/// replaced cell's spill, and a SET naming no string column keeps the arena as read.
#[test]
fn the_arena_holds_only_referenced_spill() {
    const KEPT: &str = "a shorter spilled string";
    /// `(payload types, SET list, row read, how many of it, row written, arena bytes)`.
    type Case<'a> = (&'a [ColType], String, &'a [Cell<'a>], usize, &'a [Cell<'a>], usize);
    let cases: &[Case] = &[
        (&[S], format!("c1 = '{LONG}'"), &[Str("a")], 3, &[Str(LONG)], LONG.len()),
        (
            &[S, S],
            "c1 = 'x'".into(),
            &[Str(LONG), Str(KEPT)],
            1,
            &[Str("x"), Str(KEPT)],
            KEPT.len(),
        ),
        (
            &[S, I],
            "c2 = 99".into(),
            &[Str(LONG), Int(7)],
            1,
            &[Str(LONG), Int(99)],
            LONG.len(),
        ),
    ];
    for (tys, set, before, n, after, blob_len) in cases {
        let schema = typed_schema(tys);
        let out = updated(&schema, set, &vec![*before; *n]).unwrap();
        assert_eq!(rows_of(&schema, &out), vec![(after.to_vec(), 1); *n], "{set}");
        assert_eq!(out.blob.len(), *blob_len, "{set}");
    }
}

/// | name | shape |
/// |---|---|
/// | `t` | `(id PK, v NOT NULL, s TEXT)` |
/// | `ty` | `(id PK, u8c U8, n, f DOUBLE, s TEXT)` |
/// | `c` | `(a U64, b U64, v)` with `PRIMARY KEY (a, b)` |
/// | `st` | a stream `(id PK, v)` |
/// | `vw` | a view `(id PK, v)` |
fn cat() -> TestCatalog {
    let i = TypeCode::I64;
    let idv = || vec![col("id", i), ncol("v", i)];
    catalog(vec![
        (
            "t",
            table(1, vec![col("id", i), col("v", i), ncol("s", TypeCode::String)], vec![0]),
        ),
        (
            "ty",
            table(
                2,
                vec![
                    col("id", i),
                    ncol("u8c", TypeCode::U8),
                    ncol("n", i),
                    ncol("f", TypeCode::F64),
                    ncol("s", TypeCode::String),
                ],
                vec![0],
            ),
        ),
        (
            "c",
            table(
                3,
                vec![col("a", TypeCode::U64), col("b", TypeCode::U64), ncol("v", i)],
                vec![0, 1],
            ),
        ),
        ("st", rel(4, RelClass::Stream, idv(), vec![0], vec![])),
        ("vw", rel(5, RelClass::View, idv(), vec![0], vec![])),
    ])
}

/// A clause gnitz does not honour is named rather than dropped, as is a target
/// that holds no row to mutate and a SET list no row could take — all before any
/// row is read.
#[test]
fn a_refused_mutation_names_its_rule() {
    let cat = cat();
    for (sql, needle) in [
        ("UPDATE t SET v = 9 WHERE id = 1 RETURNING id", "RETURNING"),
        ("DELETE FROM t WHERE id = 1 RETURNING id", "RETURNING"),
        ("DELETE FROM t LIMIT 1", "LIMIT"),
        ("DELETE FROM t ORDER BY id", "ORDER BY"),
        // The join forms are refused before any name resolves, so `other` need
        // not exist.
        ("UPDATE t SET v = o.v FROM other o WHERE t.id = o.id", "join-update"),
        ("DELETE FROM t USING other o WHERE t.id = o.id", "join-delete"),
        ("UPDATE t JOIN c ON t.id = c.a SET v = 1", "exactly one simple FROM"),
        // A written alias displaces the table name, and a qualifier naming
        // anything else is not silently ignored.
        ("UPDATE t AS x SET v = 1 WHERE t.v = 1", "not found"),
        ("UPDATE t SET v = x.v", "table alias 'x' not found"),
        ("UPDATE t SET v = EXCLUDED.v", "table alias 'EXCLUDED' not found"),
        ("DELETE FROM t WHERE nope.v = 1", "not found"),
        // A SET target is one plain name of a non-key column, assigned once.
        ("UPDATE t SET id = 9 WHERE id = 1", "primary key column in UPDATE SET"),
        ("UPDATE t SET v = 1, v = 2", "multiple assignments to column 'v'"),
        ("UPDATE t SET t.v = 1", "simple identifier"),
        ("UPDATE t SET (v, s) = (1, 'x')", "simple identifier"),
        ("UPDATE t SET nope = 1", "'nope' not found"),
        // A value its column cannot hold: a literal out of range, a float that is
        // not a copy, and a string where an integer goes or the reverse.
        ("UPDATE ty SET u8c = 300", "out of range"),
        ("UPDATE ty SET n = f", "floating-point"),
        ("UPDATE ty SET f = f + 1.0", "floating-point"),
        ("UPDATE ty SET f = n", "cannot assign a value of type I64"),
        ("UPDATE ty SET n = UPPER(s)", "cannot assign a value of type STRING"),
        ("UPDATE ty SET s = n + 1", "cannot assign a value of type I64"),
        ("UPDATE ty SET n = f > 1.5", "cannot assign a value of type BOOLEAN"),
        // A view is read-only, and a stream holds no row.
        ("UPDATE vw SET v = 1 WHERE id = 1", "is a view"),
        ("DELETE FROM vw", "is a view"),
        ("UPDATE st SET v = 1 WHERE id = 1", "is a stream"),
        ("DELETE FROM st WHERE id = 1", "is a stream"),
    ] {
        assert_rejects(sql, plan(&cat, sql), needle);
    }
    // The written alias is the one qualifier that answers.
    let sql = "UPDATE t AS x SET v = 11 WHERE x.v = 10";
    plan(&cat, sql).unwrap_or_else(|e| panic!("`{sql}`: {e:?}"));
}

/// A DELETE writes the retraction of the keys it read.
#[test]
fn a_delete_retracts_the_keys_it_read() {
    let mut plan = plan(&cat(), "DELETE FROM t WHERE v = 5").unwrap();
    let schema = &plan.target.schema;
    let held = batch_of(schema, &[&[Int(5), Null], &[Int(5), Str("x")]]);
    let keys = held.pks.clone();
    let out = delta(plan.set.as_deref_mut(), held, schema).unwrap();
    assert_eq!((out.pks, out.weights), (keys, vec![-1, -1]));
}
