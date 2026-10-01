use super::*;
use crate::dml::plan::access_path;
use crate::test_support::Cell::{self, Int, Null, Str, F64};
use crate::test_support::{
    assert_rejects, batch_of, catalog, col, ncol, parse_expr_sql, parse_stmt, pk_schema, rel, rows_of, schema, table,
};
use gnitz_wire::TypeCode;
use sqlparser::ast::Statement;

/// | name | shape |
/// |---|---|
/// | `t` | `(id PK, v NOT NULL, s TEXT)` |
/// | `f` | `(id PK, f DOUBLE)` |
/// | `c` | `(a U64, b U16, v)` with `PRIMARY KEY (b, a)` — the PK list against schema order |
/// | `h` | `(id PK, gone, v)`, `gone` a dropped (hidden) column |
/// | `sr` | `(id SMALLINT SERIAL PK, name TEXT)` |
/// | `st` | a stream `(id PK, v)` |
/// | `vw` | a view `(id PK, v)` |
fn cat() -> Catalog<'static> {
    let i = TypeCode::I64;
    let idv = || vec![col("id", i), ncol("v", i)];
    let sr = vec![col("id", TypeCode::I16), ncol("name", TypeCode::String)];
    catalog(vec![
        (
            "t",
            table(1, vec![col("id", i), col("v", i), ncol("s", TypeCode::String)], vec![0]),
        ),
        ("f", table(2, vec![col("id", i), ncol("f", TypeCode::F64)], vec![0])),
        (
            "c",
            table(
                3,
                vec![col("a", TypeCode::U64), col("b", TypeCode::U16), ncol("v", i)],
                vec![1, 0],
            ),
        ),
        (
            "h",
            table(4, vec![col("id", i), col("gone", i).hidden(), ncol("v", i)], vec![0]),
        ),
        (
            "sr",
            Arc::new(RelDescriptor {
                tid: 5,
                class: RelClass::Table,
                pk_repeats: false,
                serial: true,
                schema: Arc::new(schema(sr, &[0])),
                indexes: Vec::new(),
            }),
        ),
        ("st", rel(6, RelClass::Stream, idv(), vec![0], vec![])),
        ("vw", rel(7, RelClass::View, idv(), vec![0], vec![])),
    ])
}

fn insert(cat: &Catalog<'_>, sql: &str) -> Result<InsertPlan, GnitzSqlError> {
    let Statement::Insert(insert) = parse_stmt(sql) else {
        panic!("`{sql}` is not an INSERT");
    };
    plan_insert(&insert, cat)
}

/// Each row's key as its native value.
fn keys(schema: &Schema, b: &ZSetBatch) -> Vec<u128> {
    (0..b.pks.len()).map(|r| b.pks.get(schema, r)).collect()
}

/// A column list names which column each value lands in, in any order and case; a
/// column it leaves out is NULL, a dropped column takes a filler, and a SERIAL
/// key is left for execute to stamp.
#[test]
fn values_land_in_the_columns_the_list_names() {
    let cat = cat();
    let one = |cells: &[Cell<'static>]| vec![(cells.to_vec(), 1)];
    for (sql, want_keys, want_rows) in [
        ("INSERT INTO t VALUES (1, 2, 'x')", vec![1], one(&[Int(2), Str("x")])),
        (
            "INSERT INTO t (id, V, s) VALUES (1, 2, 'x')",
            vec![1],
            one(&[Int(2), Str("x")]),
        ),
        (
            "INSERT INTO t (s, v, id) VALUES ('x', 2, 1)",
            vec![1],
            one(&[Int(2), Str("x")]),
        ),
        ("INSERT INTO t (v, id) VALUES (2, 1)", vec![1], one(&[Int(2), Null])),
        (
            "INSERT INTO t VALUES (1, 2, 'x'), (3, 4, NULL)",
            vec![1, 3],
            vec![(vec![Int(2), Str("x")], 1), (vec![Int(4), Null], 1)],
        ),
        ("INSERT INTO h VALUES (1, 5)", vec![1], one(&[Int(0), Int(5)])),
        ("INSERT INTO st VALUES (1, 5)", vec![1], one(&[Int(5)])),
        ("INSERT INTO sr VALUES ('x')", vec![], one(&[Str("x")])),
        ("INSERT INTO sr (name) VALUES ('x')", vec![], one(&[Str("x")])),
    ] {
        let plan = insert(&cat, sql).unwrap_or_else(|e| panic!("`{sql}`: {e:?}"));
        let schema = &plan.target.schema;
        assert_eq!(keys(schema, &plan.rows), want_keys, "`{sql}`");
        assert_eq!(rows_of(schema, &plan.rows), want_rows, "`{sql}`");
    }
}

/// A written key packs its columns in PK-list order, whatever order the schema,
/// the column list or a conflict target has them in.
#[test]
fn a_written_key_packs_in_pk_list_order() {
    let cat = cat();
    for sql in [
        "INSERT INTO c VALUES (1, 2, 99)",
        "INSERT INTO c (v, b, a) VALUES (99, 2, 1)",
        "INSERT INTO c VALUES (1, 2, 99) ON CONFLICT (a, b) DO NOTHING",
    ] {
        let plan = insert(&cat, sql).unwrap_or_else(|e| panic!("`{sql}`: {e:?}"));
        assert_eq!(plan.rows.pks.region(), [0, 2, 0, 0, 0, 0, 0, 0, 0, 1], "`{sql}`");
    }
}

/// The key an INSERT stores under a literal is the key `WHERE id = <literal>`
/// seeks, at every key type.
#[test]
fn an_inserted_key_is_the_key_its_literal_seeks() {
    use TypeCode::*;
    for (tc, lit) in [
        (I8, "-1"),
        (I64, "-9223372036854775808"),
        (U16, "65535"),
        (U64, "18446744073709551615"),
        (U128, "-0"),
        (UUID, "'550e8400-e29b-41d4-a716-446655440000'"),
    ] {
        let s = pk_schema(tc);
        let cat = catalog(vec![("t", table(1, s.columns.clone(), vec![0]))]);
        let plan =
            insert(&cat, &format!("INSERT INTO t VALUES ({lit}, 0)")).unwrap_or_else(|e| panic!("{tc:?}: {e:?}"));
        let (bound, _) = access_path(&s, "t", Some(&parse_expr_sql(&format!("id = {lit}"))), &[]).unwrap();
        assert_eq!(bound, ReadBound::PkSet(plan.rows.pks.keys()), "{tc:?} {lit}");
    }
}

/// A clause gnitz does not honour is named rather than dropped, as is a target
/// that cannot take the write and a row of the wrong shape.
#[test]
fn a_refused_insert_names_its_rule() {
    let cat = cat();
    for (sql, needle) in [
        // A VALUES row carries exactly one value per column, and only values the
        // writer evaluates — an unsupported one echoed as written.
        ("INSERT INTO t VALUES (2, 20)", "expects 3 value(s)"),
        ("INSERT INTO t VALUES (2, 20, 'b', 99)", "expects 3 value(s)"),
        (
            "INSERT INTO t VALUES (2, EXTRACT(YEAR FROM 2), 'b')",
            "EXTRACT(YEAR FROM 2)",
        ),
        ("INSERT INTO t VALUES (2, NULL, 'b')", "column 'v' violates NOT NULL"),
        ("INSERT INTO t (id, s) VALUES (2, 'b')", "column 'v' violates NOT NULL"),
        ("INSERT INTO t VALUES (NULL, 20, 'b')", "column 'id' violates NOT NULL"),
        ("INSERT INTO t DEFAULT VALUES", "without VALUES"),
        ("INSERT INTO t SELECT * FROM t", "only supports VALUES"),
        ("INSERT INTO t VALUES (2, 20, 'b') LIMIT 1", "LIMIT/OFFSET"),
        ("INSERT INTO t VALUES (2, 20, 'b') FOR UPDATE", "FOR UPDATE"),
        ("INSERT IGNORE INTO t VALUES (2, 20, 'b')", "IGNORE"),
        ("REPLACE INTO t VALUES (2, 20, 'b')", "REPLACE INTO"),
        // The column list: each name once, unqualified, visible; the key present.
        ("INSERT INTO t (id, nope) VALUES (1, 2)", "'nope' not found"),
        ("INSERT INTO t (id, ID) VALUES (1, 2)", "more than once"),
        ("INSERT INTO t (t.id) VALUES (1)", "simple identifier"),
        ("INSERT INTO t (v, s) VALUES (20, 'x')", "PK column 'id' missing"),
        ("INSERT INTO h (id, gone) VALUES (1, 2)", "'gone' not found"),
        // A SERIAL key is never written.
        ("INSERT INTO sr VALUES (1, 'x')", "SERIAL primary key is auto-assigned"),
        ("INSERT INTO sr (id, name) VALUES (1, 'x')", "SERIAL"),
        // RETURNING binds as a SELECT list does, so it refuses what one refuses,
        // and it is not accepted beside ON CONFLICT.
        (
            "INSERT INTO t VALUES (2, 20, 'b') RETURNING * REPLACE (v + 1 AS v)",
            "REPLACE",
        ),
        (
            "INSERT INTO t VALUES (2, 20, 'b') ON CONFLICT (id) DO NOTHING RETURNING id",
            "ON CONFLICT: RETURNING",
        ),
        // A conflict target names exactly the primary key: not another column,
        // not more, not a prefix, not a constraint.
        (
            "INSERT INTO t VALUES (2, 20, 'b') ON CONFLICT (v) DO NOTHING",
            "primary key",
        ),
        (
            "INSERT INTO t VALUES (2, 20, 'b') ON CONFLICT (id, v) DO NOTHING",
            "primary key",
        ),
        (
            "INSERT INTO c VALUES (1, 1, 10) ON CONFLICT (a) DO NOTHING",
            "primary key",
        ),
        (
            "INSERT INTO c VALUES (1, 1, 10) ON CONFLICT (a, a) DO NOTHING",
            "primary key",
        ),
        (
            "INSERT INTO t VALUES (2, 20, 'b') ON CONFLICT (nope) DO NOTHING",
            "column not found",
        ),
        (
            "INSERT INTO t VALUES (2, 20, 'b') ON CONFLICT ON CONSTRAINT pk DO NOTHING",
            "ON CONSTRAINT",
        ),
        (
            "INSERT INTO t VALUES (2, 20, 'b') ON DUPLICATE KEY UPDATE v = 1",
            "ON DUPLICATE KEY UPDATE",
        ),
        // DO UPDATE takes a SET list as UPDATE does, each value either computed
        // from the stored row or one `EXCLUDED` column.
        (
            "INSERT INTO t VALUES (1, 20, 'b') ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v WHERE v > 5",
            "DO UPDATE: WHERE",
        ),
        (
            "INSERT INTO t VALUES (1, 20, 'b'), (1, 30, 'c') ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v",
            "second time",
        ),
        (
            "INSERT INTO t VALUES (1, 20, 'b') ON CONFLICT (id) DO UPDATE SET id = 2",
            "primary key column in ON CONFLICT DO UPDATE SET",
        ),
        (
            "INSERT INTO t VALUES (1, 20, 'b') ON CONFLICT (id) DO UPDATE SET v = v + EXCLUDED.v",
            "compound expressions",
        ),
        (
            "INSERT INTO t VALUES (1, 20, 'b') ON CONFLICT (id) DO UPDATE SET v = COALESCE(EXCLUDED.v, 0)",
            "compound expressions",
        ),
        (
            "INSERT INTO t VALUES (1, 20, 'b') ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.nope",
            "EXCLUDED.nope: column not found",
        ),
        (
            "INSERT INTO t VALUES (1, 20, 'b') ON CONFLICT (id) DO UPDATE SET s = EXCLUDED.v",
            "cannot assign an integer value",
        ),
        // A view is read-only, a reserved name is refused before the catalog is
        // probed, and a stream holds no row to resolve a conflict against.
        ("INSERT INTO vw VALUES (2, 20)", "is a view"),
        ("INSERT INTO _seg999999 VALUES (1, 1)", "cannot start with '_'"),
        (
            "INSERT INTO st VALUES (1, 2) ON CONFLICT (id) DO NOTHING",
            "is a stream",
        ),
    ] {
        assert_rejects(sql, insert(&cat, sql), needle);
    }
}

/// ON CONFLICT pushes each VALUES row whose key nothing holds, and for a held
/// key nothing (DO NOTHING) or the held row rewritten by the SET list, which
/// reads `EXCLUDED` off the VALUES row that collided with it. `held` is the rows
/// under keys 1, 2, …; the VALUES run against that order, so each merge must find
/// its own incoming row.
#[test]
fn on_conflict_resolves_each_row_against_the_row_its_key_holds() {
    let cat = cat();
    /// `(sql, held rows, pushed keys, pushed rows)`.
    type Case<'a> = (&'a str, &'a [&'a [Cell<'a>]], Vec<u128>, &'a [&'a [Cell<'a>]]);
    let cases: &[Case] = &[
        // A repeated key's first row stands for it.
        (
            "INSERT INTO t VALUES (2, 20, 'n'), (3, 30, 'n'), (3, 31, 'dup'), (1, 10, 'n') ON CONFLICT DO NOTHING",
            &[&[Int(7), Null], &[Int(8), Null]],
            vec![3],
            &[&[Int(30), Str("n")]],
        ),
        (
            "INSERT INTO t VALUES (2, 200, 'b'), (3, 300, 'c'), (1, 100, 'a') ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v",
            &[&[Int(10), Str("x")], &[Int(20), Null]],
            vec![3, 1, 2],
            &[&[Int(300), Str("c")], &[Int(100), Str("x")], &[Int(200), Null]],
        ),
        (
            "INSERT INTO t VALUES (1, 100, 'a') ON CONFLICT (id) DO UPDATE SET v = t.v + 1, s = (excluded.s)",
            &[&[Int(10), Str("x")]],
            vec![1],
            &[&[Int(11), Str("a")]],
        ),
        // A float has no computed destination, so this is a copy.
        (
            "INSERT INTO f VALUES (1, 2.25) ON CONFLICT (id) DO UPDATE SET f = EXCLUDED.f",
            &[&[F64(1.5)]],
            vec![1],
            &[&[F64(2.25)]],
        ),
    ];
    for (sql, held, want_keys, want_rows) in cases {
        let InsertPlan {
            target,
            rows,
            conflict: ConflictPlan::Resolve { mut set },
            ..
        } = insert(&cat, sql).unwrap_or_else(|e| panic!("`{sql}`: {e:?}"))
        else {
            panic!("`{sql}` has no ON CONFLICT");
        };
        let schema = &target.schema;
        let out = resolve_conflicts(&rows, batch_of(schema, held), set.as_deref_mut(), schema).unwrap();
        assert_eq!(&keys(schema, &out), want_keys, "`{sql}`");
        let want: Vec<_> = want_rows.iter().map(|cells| (cells.to_vec(), 1)).collect();
        assert_eq!(rows_of(schema, &out), want, "`{sql}`");
    }
}

/// A statement whose last SERIAL id passes the column type's maximum is refused;
/// one whose last id is the maximum is not.
#[test]
fn a_serial_id_past_the_type_maximum_is_exhausted() {
    let max = i16::MAX as u64;
    let schema = pk_schema(TypeCode::I16);
    assert_eq!(
        serial_keys(max - 1, 2, &schema).unwrap(),
        PkColumn::from_natives(&schema, [max as u128 - 1, max as u128])
    );
    for (base, n, next) in [(max, 2, max + 1), (max + 5, 1, max + 5)] {
        assert_rejects(
            &format!("{n} from {base}"),
            serial_keys(base, n, &schema),
            &format!("exhausted: next value {next} "),
        );
    }
}
