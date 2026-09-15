#![cfg(feature = "integration")]

//! `CREATE TABLE` / `CREATE INDEX` / `ALTER` through the planner into a private
//! engine: what the catalog holds afterwards, and which statements are refused
//! with which guard.

mod common;
use common::*;
use gnitz_core::{GnitzClient, TypeCode};
use gnitz_sql::SqlResult;

/// `Some(is_unique)` of the single-column index on `table.col`, `None` when the
/// column carries no index.
fn unique_on(client: &mut GnitzClient, sn: &str, table: &str, col: &str) -> Option<bool> {
    let (tid, schema) = client.resolve_table_id(sn, table).unwrap();
    let want = [col_idx(&schema, col) as u32];
    client
        .describe_by_id(tid)
        .unwrap()
        .indexes
        .iter()
        .find(|m| m.cols.as_slice() == want)
        .map(|m| m.is_unique)
}

/// Assert `sql` returns `Altered { object, name }`.
fn assert_altered(client: &mut GnitzClient, sn: &str, sql: &str, object: &str, name: &str) {
    match try_exec(client, sn, sql).unwrap().pop().unwrap() {
        SqlResult::Altered { object: o, name: n } => assert_eq!((o.as_str(), n.as_str()), (object, name), "`{sql}`"),
        other => panic!("`{sql}`: expected Altered, got {other:?}"),
    }
}

// ── CREATE TABLE ─────────────────────────────────────────────────────────────

#[test]
#[allow(clippy::type_complexity)]
fn pk_admission_matrix() {
    let (_srv, mut client, sn) = boot(1);
    use TypeCode::*;
    let accepted: &[(&str, &str, &[u32], usize, &[TypeCode], &[&str])] = &[
        (
            "CREATE TABLE t_int (id INT PRIMARY KEY)",
            "t_int",
            &[0],
            4,
            &[I32],
            &["id"],
        ),
        (
            "CREATE TABLE t_small (id SMALLINT PRIMARY KEY)",
            "t_small",
            &[0],
            2,
            &[I16],
            &["id"],
        ),
        (
            "CREATE TABLE cpk2 (a BIGINT UNSIGNED, b BIGINT UNSIGNED, payload BIGINT, PRIMARY KEY (a, b))",
            "cpk2",
            &[0, 1],
            16,
            &[U64, U64],
            &["a", "b"],
        ),
        (
            "CREATE TABLE cpk4 (a INT UNSIGNED, b INT UNSIGNED, c INT UNSIGNED, d INT UNSIGNED, payload BIGINT, \
             PRIMARY KEY (a, b, c, d))",
            "cpk4",
            &[0, 1, 2, 3],
            16,
            &[U32, U32, U32, U32],
            &["a", "b", "c", "d"],
        ),
        (
            "CREATE TABLE pk12 (a BIGINT UNSIGNED, b INT UNSIGNED, PRIMARY KEY (a, b))",
            "pk12",
            &[0, 1],
            12,
            &[U64, U32],
            &["a", "b"],
        ),
        (
            "CREATE TABLE pk24 (a BIGINT UNSIGNED, b BIGINT UNSIGNED, c BIGINT UNSIGNED, PRIMARY KEY (a, b, c))",
            "pk24",
            &[0, 1, 2],
            24,
            &[U64, U64, U64],
            &["a", "b", "c"],
        ),
        (
            "CREATE TABLE pk64 (a UUID, b UUID, c UUID, d UUID, payload BIGINT, PRIMARY KEY (a, b, c, d))",
            "pk64",
            &[0, 1, 2, 3],
            64,
            &[UUID, UUID, UUID, UUID],
            &["a", "b", "c", "d"],
        ),
        // The PK list, not column order, decides the key order.
        (
            "CREATE TABLE cpk_order (payload BIGINT NOT NULL, a BIGINT UNSIGNED, b BIGINT UNSIGNED, \
             PRIMARY KEY (b, a))",
            "cpk_order",
            &[2, 1],
            16,
            &[U64, U64],
            &["b", "a"],
        ),
        (
            "CREATE TABLE case_pk (id BIGINT, name TEXT, PRIMARY KEY (ID))",
            "case_pk",
            &[0],
            8,
            &[I64],
            &["id"],
        ),
        // A self-referencing FK declared before the inline PK it targets.
        (
            "CREATE TABLE sref (refc BIGINT REFERENCES sref(id), id BIGINT PRIMARY KEY)",
            "sref",
            &[1],
            8,
            &[I64],
            &["id"],
        ),
    ];
    for &(sql, table, pk_indices, stride, tcs, names) in accepted {
        exec(&mut client, &sn, sql);
        let s = client.resolve_table_id(&sn, table).unwrap().1;
        assert_eq!(s.pk_cols, pk_indices, "{table}");
        assert_eq!(s.pk_stride(), stride, "{table}");
        for ((&pi, &tc), &name) in pk_indices.iter().zip(tcs).zip(names) {
            let c = &s.columns[pi as usize];
            assert_eq!(c.type_code, tc, "{table}.{name}");
            assert!(c.name.eq_ignore_ascii_case(name), "{table}: {} != {name}", c.name);
            assert!(!c.is_nullable, "{table}.{name} must be NOT NULL");
        }
    }

    for &(sql, variant, needle) in &[
        ("CREATE TABLE no_pk (id INT)", "Plan", "PRIMARY KEY"),
        (
            "CREATE TABLE mixed_pk (a BIGINT UNSIGNED PRIMARY KEY, b BIGINT UNSIGNED, PRIMARY KEY (a, b))",
            "Plan",
            "Multiple PRIMARY KEY",
        ),
        (
            "CREATE TABLE two_pk (id BIGINT PRIMARY KEY, name TEXT PRIMARY KEY)",
            "Plan",
            "Multiple PRIMARY KEY",
        ),
        (
            "CREATE TABLE dup_pk (a BIGINT UNSIGNED, b BIGINT UNSIGNED, PRIMARY KEY (a, a))",
            "Plan",
            "duplicate column",
        ),
        (
            "CREATE TABLE pk5 (a TINYINT UNSIGNED, b TINYINT UNSIGNED, c TINYINT UNSIGNED, d TINYINT UNSIGNED, \
             e TINYINT UNSIGNED, PRIMARY KEY (a, b, c, d, e))",
            "Unsupported",
            "at most 4",
        ),
        (
            "CREATE TABLE pk_str (a TEXT, b INT UNSIGNED, PRIMARY KEY (a, b))",
            "Unsupported",
            "'a'",
        ),
        ("CREATE TABLE pk_f32 (id REAL PRIMARY KEY)", "Unsupported", "'id'"),
        ("CREATE TABLE pk_f64 (id DOUBLE PRIMARY KEY)", "Unsupported", "'id'"),
        (
            "CREATE TABLE pk_typo (id BIGINT PRIMARY KEY, PRIMARY KEY (typo))",
            "Bind",
            "typo",
        ),
    ] {
        assert_rejects_variant(&mut client, &sn, sql, variant, needle);
    }
}

/// A child FK column adopts the parent PK's type when its declared domain fits
/// inside it, and is refused when adopting would narrow or re-sign it.
#[test]
fn fk_child_adopts_the_parent_pk_type() {
    let (_srv, mut client, sn) = boot(1);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE parent (id BIGINT PRIMARY KEY, name TEXT)",
    );
    exec(
        &mut client,
        &sn,
        "CREATE TABLE child (cid INT PRIMARY KEY, p_id INT REFERENCES parent(id))",
    );
    let s = client.resolve_table_id(&sn, "child").unwrap().1;
    let fk = &s.columns[col_idx(&s, "p_id")];
    assert_eq!(fk.type_code, TypeCode::I64);
    assert!(matches!(fk.fk, Some(gnitz_core::FkTarget::Table { .. })));

    assert_rejects_variant(
        &mut client,
        &sn,
        "CREATE TABLE child2 (cid INT PRIMARY KEY, p_id BIGINT UNSIGNED REFERENCES parent(id))",
        "Bind",
        "FK type mismatch",
    );
}

#[test]
fn inline_unique_matrix() {
    let (_srv, mut client, sn) = boot(1);
    exec(&mut client, &sn, "CREATE TABLE par (x BIGINT PRIMARY KEY)");
    // Every honored spelling on one table. `(d, e_f)` and `(d_e, f)` render the
    // same auto-name base; the second is disambiguated with `_2`.
    exec(
        &mut client,
        &sn,
        "CREATE TABLE u (id BIGINT PRIMARY KEY UNIQUE, a BIGINT UNIQUE, b BIGINT, c BIGINT, \
         d BIGINT, e_f BIGINT, d_e BIGINT, f BIGINT, refc BIGINT UNIQUE REFERENCES par(x), \
         UNIQUE(b), CONSTRAINT u_c UNIQUE(c), UNIQUE(d, e_f), UNIQUE(d_e, f))",
    );
    exec(
        &mut client,
        &sn,
        "CREATE TABLE uc (a BIGINT UNSIGNED, b BIGINT UNSIGNED, PRIMARY KEY(a, b), UNIQUE(a))",
    );
    for (table, col, want) in [
        // The lone PK is unique already; the redundant index is not created.
        ("u", "id", None),
        ("u", "a", Some(true)),
        ("u", "b", Some(true)),
        ("u", "c", Some(true)),
        // The UNIQUE promotes the FK's own index rather than adding a second one.
        ("u", "refc", Some(true)),
        // A compound-PK member is not unique on its own.
        ("uc", "a", Some(true)),
    ] {
        assert_eq!(unique_on(&mut client, &sn, table, col), want, "{table}.{col}");
    }
    exec(&mut client, &sn, "DROP INDEX u_c");
    assert_eq!(unique_on(&mut client, &sn, "u", "c"), None);
    // A column-level `CONSTRAINT <n> UNIQUE` names its index, as the table-level
    // spelling does — so the index is droppable by the name that was written.
    exec(
        &mut client,
        &sn,
        "CREATE TABLE cn (id BIGINT PRIMARY KEY, e BIGINT CONSTRAINT uq_e UNIQUE)",
    );
    exec(&mut client, &sn, "DROP INDEX uq_e");
    assert_eq!(unique_on(&mut client, &sn, "cn", "e"), None);
    exec(&mut client, &sn, &format!("DROP INDEX {sn}__u__idx_d_e_f"));
    exec(&mut client, &sn, &format!("DROP INDEX {sn}__u__idx_d_e_f_2"));

    for (sql, variant, needle) in [
        (
            "CREATE TABLE ud (id BIGINT PRIMARY KEY, a BIGINT, UNIQUE(a), UNIQUE(a))",
            "Plan",
            "duplicate UNIQUE",
        ),
        (
            "CREATE TABLE ux (id BIGINT PRIMARY KEY, name TEXT UNIQUE)",
            "Unsupported",
            "'name'",
        ),
        (
            "CREATE TABLE ui (id BIGINT PRIMARY KEY, a BIGINT, CONSTRAINT _my_idx UNIQUE(a))",
            "Plan",
            "cannot start with '_'",
        ),
        // The index record is the indexed columns plus the source PK: 2 + 4 is
        // past the key arity limit, refused before dispatch.
        (
            "CREATE TABLE ua (a BIGINT, b BIGINT, c BIGINT, d BIGINT, e BIGINT, f BIGINT, \
             UNIQUE(e, f), PRIMARY KEY (a, b, c, d))",
            "Unsupported",
            "index arity",
        ),
    ] {
        assert_rejects_variant(&mut client, &sn, sql, variant, needle);
    }
    assert!(
        client.resolve_table_id(&sn, "ux").is_err(),
        "a rejected table is not created"
    );
}

#[test]
fn relation_names_are_case_insensitive() {
    let (_srv, mut client, sn) = boot(1);
    exec(&mut client, &sn, "CREATE TABLE Foo (id BIGINT PRIMARY KEY)");
    exec(&mut client, &sn, "INSERT INTO FOO (id) VALUES (1)");
    assert_eq!(rows(&mut client, &sn, "SELECT id FROM foo", &["id"]), vec![vec![1, 1]]);
    assert_rejects_variant(
        &mut client,
        &sn,
        "CREATE TABLE foo (id BIGINT PRIMARY KEY)",
        "Exec",
        "already exists",
    );
    exec(&mut client, &sn, "DROP TABLE fOO");
    assert!(client.resolve_table_id(&sn, "foo").is_err());
}

/// `__fk_` is reserved nowhere: an FK index has no name at all, so every
/// surface that mints a user name accepts the infix.
#[test]
fn the_fk_infix_is_reserved_nowhere() {
    let (_srv, mut client, sn) = boot(1);
    for sql in [
        "CREATE TABLE a__fk_b (id BIGINT PRIMARY KEY, x BIGINT)",
        "CREATE TABLE ok1 (id BIGINT PRIMARY KEY, a__fk_b BIGINT)",
        "CREATE VIEW v__fk_w AS SELECT x FROM a__fk_b",
        "CREATE TABLE alt (id BIGINT PRIMARY KEY, a BIGINT, CONSTRAINT my__fk_thing UNIQUE(a))",
        "DROP INDEX my__fk_thing",
        "ALTER TABLE alt ADD COLUMN c__fk_d BIGINT",
        "ALTER TABLE alt RENAME COLUMN a TO e__fk_f",
        "CREATE INDEX ON alt (c__fk_d)",
    ] {
        exec(&mut client, &sn, sql);
    }
    exec(&mut client, &sn, &format!("DROP INDEX {sn}__alt__idx_c__fk_d"));
}

// ── CREATE INDEX ─────────────────────────────────────────────────────────────

#[test]
fn create_index_naming_and_rejections() {
    let (_srv, mut client, sn) = boot(1);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, a BIGINT, b_c BIGINT, a_b BIGINT, c BIGINT, s TEXT, \
         f DOUBLE PRECISION)",
    );
    exec(&mut client, &sn, "CREATE INDEX my_idx ON t(a)");
    // A named index does not carry the auto-name, so the auto-named one on the
    // same column coexists with it.
    assert_rejects_variant(
        &mut client,
        &sn,
        &format!("DROP INDEX {sn}__t__idx_a"),
        "Exec",
        "not found",
    );
    exec(&mut client, &sn, "CREATE INDEX ON t(a)");
    assert_rejects_variant(&mut client, &sn, "CREATE INDEX ON t(a)", "Plan", "already exists");
    assert_rejects_variant(
        &mut client,
        &sn,
        "CREATE INDEX my_idx ON t(c)",
        "Exec",
        "already exists",
    );
    exec(&mut client, &sn, "DROP INDEX my_idx");
    exec(&mut client, &sn, &format!("DROP INDEX {sn}__t__idx_a"));
    exec(
        &mut client,
        &sn,
        "CREATE TABLE st (id BIGINT PRIMARY KEY, v BIGINT) WITH (stream = true)",
    );
    // `(a, b_c)` and `(a_b, c)` render the same base; the second gets `_2`.
    exec(&mut client, &sn, "CREATE INDEX ON t(a, b_c)");
    exec(&mut client, &sn, "CREATE INDEX ON t(a_b, c)");
    exec(&mut client, &sn, &format!("DROP INDEX {sn}__t__idx_a_b_c"));
    exec(&mut client, &sn, &format!("DROP INDEX {sn}__t__idx_a_b_c_2"));

    for (sql, variant, needle) in [
        ("CREATE INDEX _bad ON t(a)", "Plan", "cannot start with '_'"),
        ("DROP INDEX \"__invalid\"", "Plan", "cannot start with '_'"),
        // Every DROP clause gnitz does not honor, named rather than dropped.
        ("DROP TABLE t CASCADE", "Unsupported", "CASCADE"),
        ("DROP TABLE t RESTRICT", "Unsupported", "RESTRICT"),
        ("DROP TABLE t PURGE", "Unsupported", "PURGE"),
        // An ineligible column is named, on both index surfaces.
        ("CREATE INDEX ON t(s)", "Unsupported", "'s'"),
        ("CREATE INDEX ON t(f)", "Unsupported", "'f'"),
        ("CREATE UNIQUE INDEX ON t(s)", "Unsupported", "'s'"),
        ("CREATE INDEX ON t(ghost)", "Bind", "ghost"),
        ("CREATE INDEX ON t(a, a)", "Plan", "duplicate column"),
        ("CREATE INDEX ix ON t (a) WHERE a > 0", "Unsupported", "partial index"),
        // A stream holds no rows to index.
        ("CREATE INDEX ON st(v)", "Unsupported", "is a stream"),
    ] {
        assert_rejects_variant(&mut client, &sn, sql, variant, needle);
    }
    // BTREE is the one index method, so naming it is accepted.
    exec(&mut client, &sn, "CREATE INDEX ixb ON t USING BTREE (c)");
}

// ── ALTER ────────────────────────────────────────────────────────────────────

#[test]
fn alter_results_and_if_exists_no_ops() {
    let (_srv, mut client, sn) = boot(4);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT, a BIGINT NOT NULL, b BIGINT)",
    );
    exec(
        &mut client,
        &sn,
        "INSERT INTO t VALUES (1, 10, 1, 100), (2, 20, 2, 200), (3, 30, 3, 300)",
    );
    exec(&mut client, &sn, "CREATE VIEW vw AS SELECT id, v FROM t WHERE v >= 20");

    assert_altered(&mut client, &sn, "ALTER TABLE t RENAME TO t2", "table", "t2");
    assert_rejects_variant(&mut client, &sn, "SELECT * FROM t", "Bind", "does not exist");
    assert_eq!(
        view_rows(&mut client, &sn, "t2", &["id", "v"]),
        at_weight_one(&[vec![1, 10], vec![2, 20], vec![3, 30]])
    );
    // RENAME COLUMN is allowed under a dependent view: views bind columns by
    // ordinal.
    assert_altered(&mut client, &sn, "ALTER TABLE t2 RENAME COLUMN v TO w", "column", "w");
    assert_eq!(
        visible_names(&read_sql(&mut client, &sn, "SELECT * FROM t2").0),
        ["id", "w", "a", "b"]
    );
    assert_altered(&mut client, &sn, "ALTER TABLE vw RENAME TO vw2", "view", "vw2");

    match try_exec(&mut client, &sn, "ALTER TABLE t2 ADD CONSTRAINT uq UNIQUE (b)")
        .unwrap()
        .pop()
        .unwrap()
    {
        SqlResult::IndexCreated { .. } => {}
        other => panic!("ADD CONSTRAINT UNIQUE returns IndexCreated, got {other:?}"),
    }
    assert_rejects_variant(
        &mut client,
        &sn,
        "INSERT INTO t2 VALUES (4, 40, 4, 100)",
        "Exec",
        "Unique index violation",
    );
    assert_altered(
        &mut client,
        &sn,
        "ALTER TABLE t2 DROP CONSTRAINT uq",
        "constraint",
        "uq",
    );
    exec(&mut client, &sn, "INSERT INTO t2 VALUES (4, 40, 4, 100)");

    assert_altered(
        &mut client,
        &sn,
        "ALTER VIEW vw2 AS SELECT id, w FROM t2 WHERE w >= 30",
        "view",
        "vw2",
    );
    assert_eq!(
        view_rows(&mut client, &sn, "vw2", &["id", "w"]),
        at_weight_one(&[vec![3, 30], vec![4, 40]])
    );
    exec(&mut client, &sn, "DROP VIEW vw2");

    assert_altered(&mut client, &sn, "ALTER TABLE t2 DROP COLUMN b", "column", "b");
    assert_eq!(
        visible_names(&read_sql(&mut client, &sn, "SELECT * FROM t2").0),
        ["id", "w", "a"]
    );
    exec(&mut client, &sn, "INSERT INTO t2 VALUES (5, 50, 5)");
    assert_eq!(
        rows(
            &mut client,
            &sn,
            "SELECT id, w, a FROM t2 WHERE id >= 4",
            &["id", "w", "a"]
        ),
        at_weight_one(&[vec![4, 40, 4], vec![5, 50, 5]])
    );
    assert_rejects_variant(&mut client, &sn, "SELECT b FROM t2", "Bind", "not found");

    assert_altered(
        &mut client,
        &sn,
        "ALTER TABLE t2 ALTER COLUMN a DROP NOT NULL",
        "column",
        "a",
    );
    exec(&mut client, &sn, "INSERT INTO t2 VALUES (6, 60, NULL)");
    assert_eq!(
        rows(&mut client, &sn, "SELECT id FROM t2 WHERE a IS NULL", &["id"]),
        vec![vec![6, 1]]
    );

    // A bounded view keeps its capacity across a rename.
    exec(
        &mut client,
        &sn,
        "CREATE VIEW bv WITH (capacity = '1 MB') AS SELECT id, w FROM t2",
    );
    assert_altered(&mut client, &sn, "ALTER TABLE bv RENAME TO bv2", "view", "bv2");
    assert_rejects_variant(
        &mut client,
        &sn,
        "CREATE VIEW over AS SELECT id FROM bv2",
        "Unsupported",
        "capacity-bounded",
    );

    for (sql, object, name) in [
        ("ALTER TABLE IF EXISTS nope RENAME TO x", "table", "x"),
        ("ALTER TABLE IF EXISTS nope DROP COLUMN a", "column", "a"),
        (
            "ALTER TABLE IF EXISTS nope ADD CONSTRAINT cq UNIQUE (v)",
            "constraint",
            "cq",
        ),
        ("ALTER TABLE t2 DROP CONSTRAINT IF EXISTS nope", "constraint", "nope"),
    ] {
        assert_altered(&mut client, &sn, sql, object, name);
    }
}

#[test]
fn alter_rejection_matrix() {
    let (_srv, mut client, sn) = boot(1);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, a BIGINT NOT NULL, b BIGINT, c BIGINT)",
    );
    exec(&mut client, &sn, "CREATE VIEW vw AS SELECT id, b FROM t");
    exec(&mut client, &sn, "CREATE VIEW base AS SELECT id, c FROM t");
    exec(&mut client, &sn, "CREATE VIEW dep AS SELECT id, c FROM base");
    exec(&mut client, &sn, "CREATE INDEX ix ON t (c)");
    exec(&mut client, &sn, "CREATE TABLE u (id BIGINT PRIMARY KEY, v BIGINT)");
    exec(&mut client, &sn, "CREATE UNIQUE INDEX uq ON u (v)");
    exec(
        &mut client,
        &sn,
        "CREATE VIEW bv WITH (capacity = '1 MB') AS SELECT id, b FROM t",
    );
    exec(
        &mut client,
        &sn,
        "CREATE TABLE st (id BIGINT PRIMARY KEY, v BIGINT) WITH (stream = true)",
    );
    for (sql, variant, needle) in [
        // ADD COLUMN honours a bare nullable append; each refused clause would
        // need a value for the rows already there, a second catalog object, or a
        // physical move, and names what to write instead where there is one.
        ("ALTER TABLE t ADD COLUMN x BIGINT NOT NULL", "Unsupported", "NOT NULL"),
        ("ALTER TABLE t ADD COLUMN x SERIAL", "Unsupported", "SERIAL"),
        ("ALTER TABLE t ADD COLUMN x BIGINT DEFAULT 0", "Unsupported", "DEFAULT"),
        (
            "ALTER TABLE t ADD COLUMN x BIGINT PRIMARY KEY",
            "Unsupported",
            "PRIMARY KEY",
        ),
        (
            "ALTER TABLE t ADD COLUMN x BIGINT UNIQUE",
            "Unsupported",
            "CREATE UNIQUE INDEX",
        ),
        (
            "ALTER TABLE t ADD COLUMN x BIGINT REFERENCES t (id)",
            "Unsupported",
            "ADD CONSTRAINT",
        ),
        (
            "ALTER TABLE t ADD COLUMN x BIGINT CHECK (x > 0)",
            "Unsupported",
            "CHECK",
        ),
        (
            "ALTER TABLE t ADD COLUMN x BIGINT COLLATE utf8",
            "Unsupported",
            "COLLATE",
        ),
        (
            "ALTER TABLE t ADD COLUMN IF NOT EXISTS x BIGINT",
            "Unsupported",
            "IF NOT EXISTS",
        ),
        ("ALTER TABLE t ADD COLUMN x BIGINT FIRST", "Unsupported", "FIRST/AFTER"),
        (
            "ALTER TABLE t ADD COLUMN x BIGINT AFTER a",
            "Unsupported",
            "FIRST/AFTER",
        ),
        ("ALTER TABLE t ADD COLUMN a BIGINT", "Exec", "duplicate column name"),
        ("ALTER TABLE t ADD COLUMN id BIGINT", "Exec", "duplicate column name"),
        ("ALTER TABLE st ADD COLUMN x BIGINT", "Unsupported", "is a stream"),
        (
            "ALTER TABLE st ADD CONSTRAINT su UNIQUE (v)",
            "Unsupported",
            "is a stream",
        ),
        ("ALTER TABLE t RENAME TO otherschema.t2", "Unsupported", "cross-schema"),
        ("ALTER TABLE nope RENAME TO x", "Bind", "does not exist"),
        // The new name is checked before the IF EXISTS no-op.
        (
            "ALTER TABLE IF EXISTS nope RENAME TO _x",
            "Plan",
            "cannot start with '_'",
        ),
        ("ALTER TABLE t RENAME COLUMN a TO b", "Exec", "duplicate column name"),
        ("ALTER TABLE t RENAME COLUMN nope TO z", "Bind", "not found"),
        ("ALTER TABLE vw RENAME COLUMN b TO w", "Unsupported", "base table"),
        (
            "ALTER TABLE t ADD CONSTRAINT cq UNIQUE (b) NOT VALID",
            "Unsupported",
            "NOT VALID",
        ),
        ("ALTER TABLE t ADD CONSTRAINT cq CHECK (b > 0)", "Unsupported", "UNIQUE"),
        ("ALTER TABLE t DROP CONSTRAINT nope", "Exec", "not found"),
        ("ALTER TABLE t DROP CONSTRAINT ix CASCADE", "Unsupported", "CASCADE"),
        // DROP CONSTRAINT drops a UNIQUE index of its own table only.
        ("ALTER TABLE t DROP CONSTRAINT ix", "Exec", "constraint 'ix' not found"),
        ("ALTER TABLE t DROP CONSTRAINT uq", "Exec", "not found"),
        ("ALTER VIEW vw AS SELECT id, b FROM vw", "Unsupported", "itself"),
        ("ALTER VIEW t AS SELECT id FROM t", "Unsupported", "is a table"),
        (
            "ALTER VIEW base AS SELECT id, c FROM t WHERE c > 0",
            "Exec",
            "dependency",
        ),
        // `ALTER VIEW … AS` re-plans the body without its option clause.
        ("ALTER VIEW bv AS SELECT id FROM t", "Unsupported", "capacity-bounded"),
        (
            "ALTER TABLE t ALTER COLUMN b SET NOT NULL",
            "Unsupported",
            "SET NOT NULL",
        ),
        (
            "ALTER TABLE t ALTER COLUMN b SET DATA TYPE INT",
            "Unsupported",
            "SET DATA TYPE",
        ),
        (
            "ALTER TABLE t ALTER COLUMN b ADD GENERATED ALWAYS AS IDENTITY",
            "Unsupported",
            "GENERATED",
        ),
        (
            "ALTER TABLE t RENAME COLUMN a TO a2, RENAME COLUMN b TO b2",
            "Unsupported",
            "more than one operation",
        ),
        ("ALTER TABLE ONLY t RENAME TO t2", "Unsupported", "ONLY"),
        ("ALTER TABLE t DROP COLUMN id", "Exec", "primary-key"),
        ("ALTER TABLE t DROP COLUMN a, b", "Parse", "Expected"),
        ("ALTER TABLE t DROP COLUMN b CASCADE", "Unsupported", "CASCADE"),
        ("ALTER TABLE t DROP COLUMN c", "Exec", "secondary index"),
        ("ALTER TABLE t ALTER COLUMN id DROP NOT NULL", "Exec", "primary-key"),
        // A dependent view blocks a column drop and a nullability change of any
        // column: its traces hold rows under the old comparator.
        ("ALTER TABLE t DROP COLUMN b", "Exec", "dependent view"),
        ("ALTER TABLE t ALTER COLUMN a DROP NOT NULL", "Exec", "dependent view"),
        ("INSERT INTO t VALUES (1, NULL, 1, 1)", "Bind", "NOT NULL"),
    ] {
        assert_rejects_variant(&mut client, &sn, sql, variant, needle);
    }
    // Only if the misdirected constraint drop left `uq` standing.
    exec(&mut client, &sn, "DROP INDEX uq");
}

// ── DROP ─────────────────────────────────────────────────────────────────────

/// One `DROP` statement is one DDL zone: every name goes or none does, and the
/// dependency guards see the whole batch.
#[test]
fn a_multi_name_drop_is_one_atomic_zone() {
    let (_srv, mut client, sn) = boot(1);
    for sql in [
        "CREATE TABLE a (id BIGINT PRIMARY KEY)",
        "CREATE TABLE b (id BIGINT PRIMARY KEY)",
        "CREATE TABLE parent (id BIGINT PRIMARY KEY)",
        "CREATE TABLE child (id BIGINT PRIMARY KEY, p BIGINT REFERENCES parent(id))",
        "CREATE VIEW keeper AS SELECT id FROM b",
    ] {
        exec(&mut client, &sn, sql);
    }

    // `b` is read by a view, so its drop refuses — and `a` survives with it.
    assert_rejects_variant(&mut client, &sn, "DROP TABLE a, b", "Exec", "View dependency");
    assert!(client.resolve_table_id(&sn, "a").is_ok(), "a is untouched");

    // Two `-1`s on one catalog PK is not a retraction the engine accepts.
    assert_rejects_variant(&mut client, &sn, "DROP TABLE a, A", "Plan", "named more than once");

    // A FK child co-dropped in the same batch is self-resolving, so the pair
    // drops in either written order.
    exec(&mut client, &sn, "DROP TABLE parent, child");
    assert!(client.resolve_table_id(&sn, "parent").is_err());
    assert!(client.resolve_table_id(&sn, "child").is_err());

    exec(&mut client, &sn, "DROP VIEW keeper");
    exec(&mut client, &sn, "DROP TABLE a, b");
    assert!(client.resolve_table_id(&sn, "a").is_err());
    assert!(client.resolve_table_id(&sn, "b").is_err());
}

/// A qualifier naming the session schema is accepted; any other is a cross-schema
/// reference, and an index name — being global — takes none at all.
#[test]
fn a_name_qualifier_is_matched_not_dropped() {
    let (_srv, mut client, sn) = boot(1);
    exec(&mut client, &sn, "CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)");
    // The same-schema spelling works on every name surface.
    exec(&mut client, &sn, &format!("INSERT INTO {sn}.t VALUES (1, 10)"));
    assert_eq!(
        rows(&mut client, &sn, &format!("SELECT id FROM {sn}.t"), &["id"]),
        vec![vec![1, 1]]
    );
    exec(
        &mut client,
        &sn,
        &format!("CREATE VIEW {sn}.vw AS SELECT id FROM {sn}.t"),
    );
    exec(&mut client, &sn, &format!("DROP VIEW {sn}.vw"));

    for (sql, variant, needle) in [
        (
            "CREATE TABLE other.x (id BIGINT PRIMARY KEY)",
            "Unsupported",
            "cross-schema",
        ),
        (
            "CREATE TABLE fk (id BIGINT PRIMARY KEY, r BIGINT REFERENCES other.t(id))",
            "Unsupported",
            "cross-schema",
        ),
        ("SELECT id FROM other.t", "Unsupported", "cross-schema"),
        ("INSERT INTO other.t VALUES (2, 20)", "Unsupported", "cross-schema"),
        ("DROP TABLE other.t", "Unsupported", "cross-schema"),
        ("CREATE INDEX ON other.t (v)", "Unsupported", "cross-schema"),
        // An index name is global, so a qualifier on one scopes nothing.
        ("CREATE INDEX other.ix ON t (v)", "Unsupported", "no qualifier"),
        ("DROP INDEX other.ix", "Unsupported", "no qualifier"),
        (&format!("DROP INDEX {sn}.ix"), "Unsupported", "no qualifier"),
    ] {
        assert_rejects_variant(&mut client, &sn, sql, variant, needle);
    }
}
