//! `CREATE TABLE` / `CREATE INDEX` / `ALTER` / `DROP` through the planner into
//! the engine: what the catalog holds afterwards, and which statements are
//! refused by which guard.

use super::*;
use gnitz_wire::WireStatus::{Error, IntegrityViolation, NotFound};

// ── CREATE TABLE ─────────────────────────────────────────────────────────────

/// A foreign key refuses an orphan child row until its parent lands.
#[test]
fn a_foreign_key_refuses_an_orphan_until_its_parent_lands() {
    let mut db = Db::boot(1);
    db.exec(
        "CREATE TABLE parent (id BIGINT PRIMARY KEY, name TEXT);
         CREATE TABLE child (cid INT PRIMARY KEY, p_id INT REFERENCES parent(id))",
    );
    db.refuses("INSERT INTO child VALUES (1, 99)", IntegrityViolation, "");
    db.exec("INSERT INTO parent VALUES (99, 'p'); INSERT INTO child VALUES (1, 99)");
    assert_eq!(db.rows("SELECT cid, p_id FROM child", &["cid", "p_id"]), [[1, 99, 1]]);
}

/// Every honored UNIQUE spelling lands as exactly one unique index — the lone
/// PK's as any other — and the FK's own index promoted rather than a second one
/// added. Each is droppable by the name that was written or minted.
#[test]
fn inline_uniques_land_as_one_index_each() {
    let mut db = Db::boot(1);
    db.exec(
        "CREATE TABLE par (x BIGINT PRIMARY KEY);
         CREATE TABLE u (id BIGINT PRIMARY KEY UNIQUE, a BIGINT UNIQUE, b BIGINT, c BIGINT, \
         d BIGINT, e_f BIGINT, d_e BIGINT, f BIGINT, refc BIGINT UNIQUE REFERENCES par(x), \
         g BIGINT CONSTRAINT uq_g UNIQUE, \
         UNIQUE(b), CONSTRAINT u_c UNIQUE(c), UNIQUE(d, e_f), UNIQUE(d_e, f));
         CREATE TABLE uc (a BIGINT UNSIGNED, b BIGINT UNSIGNED, PRIMARY KEY(a, b), UNIQUE(a))",
    );
    let unique = |cols: &[&str]| (cols.iter().map(|c| c.to_string()).collect::<Vec<_>>(), true);
    assert_eq!(
        db.indexes("u"),
        [
            unique(&["a"]),
            unique(&["b"]),
            unique(&["c"]),
            unique(&["d", "e_f"]),
            unique(&["d_e", "f"]),
            unique(&["g"]),
            unique(&["id"]),
            unique(&["refc"]),
        ]
    );
    assert_eq!(db.indexes("uc"), [unique(&["a"])]);

    let sn = db.sn.clone();
    db.exec(&format!(
        "DROP INDEX u_c; DROP INDEX uq_g; DROP INDEX {sn}__u__idx_d_e_f; DROP INDEX {sn}__u__idx_d_e_f_2"
    ));
    assert_eq!(
        db.indexes("u"),
        [unique(&["a"]), unique(&["b"]), unique(&["id"]), unique(&["refc"])]
    );
}

/// A table's every unique index renders inside its `CREATE TABLE`, so that
/// statement accepts whatever unique index CREATE INDEX and ADD CONSTRAINT made:
/// one on the lone PK, and two named ones on one column list.
#[test]
fn a_rendered_create_table_replays_every_unique_index() {
    let mut db = Db::boot(1);
    db.exec(
        "CREATE TABLE t (id BIGINT PRIMARY KEY UNIQUE, a BIGINT);
         CREATE UNIQUE INDEX ix_id ON t(id);
         ALTER TABLE t ADD CONSTRAINT k UNIQUE (id);
         CREATE UNIQUE INDEX ix1 ON t(a);
         CREATE UNIQUE INDEX ix2 ON t(a)",
    );
    let before = db.indexes("t");
    let catalog = block_on(db.client.catalog()).unwrap();
    let rendered = gnitz_sql::create_table_statement(&catalog, catalog.get(&rel(&db.sn, "t")).unwrap()).unwrap();
    db.exec("DROP TABLE t");
    db.exec(&rendered);
    assert_eq!(db.indexes("t"), before);
    let catalog = block_on(db.client.catalog()).unwrap();
    let again = gnitz_sql::create_table_statement(&catalog, catalog.get(&rel(&db.sn, "t")).unwrap()).unwrap();
    assert_eq!(again, rendered);
}

/// `ALTER VIEW … AS` restates the body alone, so the view keeps every option it
/// was created with — a budget and compression alike — on a new body, and a
/// same-body ALTER keeps the view itself.
#[test]
fn alter_view_keeps_the_standing_options() {
    let mut db = Db::boot(1);
    let sn = db.sn.clone();
    db.exec(
        "CREATE TABLE t (id BIGINT PRIMARY KEY, a BIGINT, b BIGINT);
         CREATE VIEW v WITH (delta = '1 MB', compress = true) AS SELECT id, a FROM t",
    );
    let rendered = |db: &mut Db| {
        let catalog = block_on(db.client.catalog()).unwrap();
        gnitz_sql::create_view_statement(&catalog, catalog.get(&rel(&sn, "v")).unwrap()).unwrap()
    };
    let with = "WITH (delta = '1 MB', compress = true) AS";
    assert!(rendered(&mut db).contains(with));
    let vid = db.rel("v").tid;
    db.exec("ALTER VIEW v AS SELECT id, a FROM t");
    assert_eq!(db.rel("v").tid, vid);
    db.exec("ALTER VIEW v AS SELECT id, b FROM t");
    assert_ne!(db.rel("v").tid, vid);
    let now = rendered(&mut db);
    assert!(now.contains(with) && now.contains("SELECT id, b FROM"), "{now}");
}

/// An inline UNIQUE's auto-name avoids every live index name: one a renamed
/// table left behind, and one written by hand on another table.
#[test]
fn an_inline_unique_auto_name_avoids_live_index_names() {
    let mut db = Db::boot(1);
    let sn = db.sn.clone();
    db.exec(&format!(
        "CREATE TABLE t (id BIGINT PRIMARY KEY, b BIGINT UNIQUE);
         ALTER TABLE t RENAME TO u;
         CREATE TABLE t (id BIGINT PRIMARY KEY, b BIGINT UNIQUE);
         CREATE TABLE o (id BIGINT PRIMARY KEY, x BIGINT);
         CREATE INDEX {sn}__w__idx_b ON o(x);
         CREATE TABLE w (id BIGINT PRIMARY KEY, b BIGINT UNIQUE)"
    ));
    let b = || (vec!["b".to_string()], true);
    assert_eq!(db.indexes("u"), [b()]);
    db.exec(&format!("DROP INDEX {sn}__t__idx_b_2; DROP INDEX {sn}__w__idx_b_2"));
    assert!(db.indexes("t").is_empty() && db.indexes("w").is_empty());
    assert_eq!(db.indexes("u"), [b()]);
}

#[test]
fn relation_names_are_case_insensitive() {
    let mut db = Db::boot(1);
    db.exec("CREATE TABLE Foo (id BIGINT PRIMARY KEY); INSERT INTO FOO (id) VALUES (1)");
    assert_eq!(db.rows("SELECT id FROM foo", &["id"]), [[1, 1]]);
    db.refuses("CREATE TABLE foo (id BIGINT PRIMARY KEY)", Error, "already exists");
    db.exec("DROP TABLE fOO");
    assert!(!db.exists("foo"));
}

/// Every SQL surface reports an absent relation as the one classified `NotFound`,
/// naming the relation as the statement spelled it.
#[test]
fn an_absent_relation_is_one_not_found_everywhere() {
    let mut db = Db::boot(1);
    for (sql, name) in [
        ("SELECT * FROM nope", "nope"),
        ("SELECT * FROM NoPe", "NoPe"),
        ("ALTER TABLE nope ADD COLUMN x BIGINT", "nope"),
        ("INSERT INTO NoPe VALUES (1)", "NoPe"),
        ("UPDATE NoPe SET x = 1", "NoPe"),
        ("DELETE FROM NoPe", "NoPe"),
        ("DROP TABLE NoPe", "NoPe"),
    ] {
        db.refuses(sql, NotFound, &format!("'{name}' not found"));
    }
}

/// `__fk_` is reserved nowhere: an FK index has no name at all, so every
/// surface that mints a user name accepts the infix.
#[test]
fn the_fk_infix_is_reserved_nowhere() {
    let mut db = Db::boot(1);
    let sn = db.sn.clone();
    db.exec(&format!(
        "CREATE TABLE a__fk_b (id BIGINT PRIMARY KEY, x BIGINT);
         CREATE TABLE ok1 (id BIGINT PRIMARY KEY, a__fk_b BIGINT);
         CREATE VIEW v__fk_w AS SELECT x FROM a__fk_b;
         CREATE TABLE alt (id BIGINT PRIMARY KEY, a BIGINT, CONSTRAINT my__fk_thing UNIQUE(a));
         DROP INDEX my__fk_thing;
         ALTER TABLE alt ADD COLUMN c__fk_d BIGINT;
         ALTER TABLE alt RENAME COLUMN a TO e__fk_f;
         CREATE INDEX ON alt (c__fk_d);
         DROP INDEX {sn}__alt__idx_c__fk_d"
    ));
}

// ── CREATE INDEX ─────────────────────────────────────────────────────────────

#[test]
fn create_index_naming_and_rejections() {
    let mut db = Db::boot(1);
    let sn = db.sn.clone();
    db.exec(
        "CREATE TABLE t (id BIGINT PRIMARY KEY, a BIGINT, b_c BIGINT, a_b BIGINT, c BIGINT, s TEXT, \
         f DOUBLE PRECISION);
         CREATE TABLE st (id BIGINT PRIMARY KEY, v BIGINT) WITH (stream = true);
         CREATE VIEW vw AS SELECT * FROM t;
         CREATE INDEX my_idx ON t(a)",
    );
    // An unnamed request is refused by whatever index already serves it,
    // whatever that index is named.
    db.refuses(&format!("DROP INDEX {sn}__t__idx_a"), NotFound, "not found");
    db.refuses("CREATE INDEX ON t(a)", Error, "already exists as 'my_idx'");
    db.refuses("CREATE INDEX my_idx ON t(c)", Error, "already exists");
    // `(a, b_c)` and `(a_b, c)` render the same base; the second gets `_2`, and
    // is found under it when it is asked for again.
    db.exec("DROP INDEX my_idx; CREATE INDEX ON t(a, b_c); CREATE INDEX ON t(a_b, c)");
    db.refuses(
        "CREATE INDEX ON t(a_b, c)",
        Error,
        &format!("already exists as '{sn}__t__idx_a_b_c_2'"),
    );
    db.exec(&format!(
        "DROP INDEX {sn}__t__idx_a_b_c; DROP INDEX {sn}__t__idx_a_b_c_2"
    ));
    assert!(db.indexes("t").is_empty());

    // A plain index does not stand in for a unique one on its column, while the
    // unique one stands in for both. The two share one circuit.
    db.exec("CREATE INDEX ON t(c)");
    assert_eq!(db.indexes("t"), [(vec!["c".to_string()], false)]);
    db.exec("ALTER TABLE t ADD UNIQUE (c)");
    assert_eq!(db.indexes("t"), [(vec!["c".to_string()], true)]);
    db.refuses("ALTER TABLE t ADD UNIQUE (c)", Error, &format!("'{sn}__t__idx_c_2'"));
    db.refuses("CREATE INDEX ON t(c)", Error, &format!("'{sn}__t__idx_c'"));
    db.exec(&format!("DROP INDEX {sn}__t__idx_c; DROP INDEX {sn}__t__idx_c_2"));
    assert!(db.indexes("t").is_empty());

    // Renaming the owner or the column leaves the index serving the request.
    db.exec(
        "CREATE TABLE r (id BIGINT PRIMARY KEY, a BIGINT);
         CREATE INDEX ON r(a); ALTER TABLE r RENAME COLUMN a TO e; ALTER TABLE r RENAME TO r2",
    );
    db.refuses(
        "CREATE INDEX ON r2(e)",
        Error,
        &format!("already exists as '{sn}__r__idx_a'"),
    );
    db.exec("DROP TABLE r2");

    // A quoted column name is not an identifier; the index name built from it is.
    db.exec(&format!(
        "CREATE TABLE q (id BIGINT PRIMARY KEY, \"a b\" BIGINT);
         CREATE INDEX ON q(\"a b\"); DROP INDEX {sn}__q__idx_a_b; DROP TABLE q"
    ));

    for (sql, needle) in [
        ("CREATE INDEX _bad ON t(a)", "cannot start with '_'"),
        ("DROP INDEX \"__invalid\"", "cannot start with '_'"),
        // Every DROP clause gnitz does not honor, named rather than dropped.
        ("DROP TABLE t CASCADE", "CASCADE"),
        ("DROP TABLE t PURGE", "PURGE"),
        // An ineligible column is named, on both index surfaces.
        ("CREATE INDEX ON t(s)", "'s'"),
        (
            "CREATE INDEX ON t(f)",
            "CREATE INDEX: column list column 'f' has type DOUBLE",
        ),
        ("CREATE UNIQUE INDEX ON t(s)", "'s'"),
        ("CREATE INDEX ON t(ghost)", "ghost"),
        ("CREATE INDEX ON t(a, a)", "duplicate column"),
        ("CREATE INDEX ix ON t (a) WHERE a > 0", "partial index"),
        // A stream holds no rows to index, and a reserved name is refused before
        // the catalog is probed.
        ("CREATE INDEX ON st(v)", "is a stream"),
        ("CREATE INDEX ON _seg999999 (v)", "cannot start with '_'"),
    ] {
        db.refuses(sql, Error, needle);
    }
    // BTREE is the one index method, so naming it is accepted.
    db.exec("CREATE INDEX ixb ON t USING BTREE (c)");
    assert_eq!(db.indexes("t"), [(vec!["c".to_string()], false)]);
}

/// `IF NOT EXISTS` tests the index name alone: a taken name is skipped whatever
/// the owner, its kind or the column list would have refused.
#[test]
fn create_index_if_not_exists_skips_a_taken_name_whatever_else_refuses() {
    let mut db = Db::boot(1);
    db.exec(
        "CREATE TABLE t (id BIGINT PRIMARY KEY, a BIGINT, b BIGINT);
         CREATE VIEW vw AS SELECT * FROM t;
         CREATE INDEX ix_a ON t(a)",
    );
    db.exec(
        "CREATE INDEX IF NOT EXISTS ix_a ON t(nosuch);
         CREATE INDEX IF NOT EXISTS ix_a ON t(id);
         CREATE INDEX IF NOT EXISTS ix_a ON nosuch(a);
         CREATE INDEX IF NOT EXISTS ix_a ON vw(a)",
    );
    assert_eq!(db.indexes("t"), [(vec!["a".to_string()], false)]);
    // A free name is still resolved and refused.
    db.refuses("CREATE INDEX IF NOT EXISTS ix_z ON t(nosuch)", Error, "nosuch");
}

/// A view whose store holds each of its keys once, in full, takes an index; one
/// whose store cannot is refused, as is a replacement that would drop an index.
#[test]
fn create_index_owner_rules() {
    let mut db = Db::boot(4);
    db.exec(
        "CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT, w BIGINT);
         CREATE TABLE u (id BIGINT PRIMARY KEY, v BIGINT);
         CREATE VIEW pv AS SELECT id, v FROM t;
         CREATE VIEW fv WITH (delta = '1 MB') AS SELECT id, v FROM t;
         CREATE VIEW bv WITH (capacity = '1 MB') AS SELECT id, v FROM t;
         CREATE VIEW jv AS SELECT t.id, u.v FROM t JOIN u ON t.v = u.v;
         CREATE VIEW tv AS SELECT id, v FROM t ORDER BY v LIMIT 3;
         CREATE INDEX pv_v ON pv(v);
         CREATE INDEX ON fv(v)",
    );
    db.refuses("CREATE INDEX ON bv(v)", Error, "without a capacity");
    // A view that projects its source's key away is keyed on a hidden slot, and
    // an index entry names its row by that slot.
    db.exec("CREATE VIEW hv AS SELECT v, w FROM t; CREATE INDEX ON hv(w)");
    for (sql, needle) in [
        ("CREATE UNIQUE INDEX ON pv(v)", "cannot carry a UNIQUE index"),
        ("CREATE INDEX ON jv(v)", "repeats its primary key"),
        ("CREATE INDEX ON tv(v)", "repeats its primary key"),
        ("CREATE OR REPLACE VIEW pv AS SELECT id, w AS v FROM t", "owns an index"),
        ("ALTER VIEW pv AS SELECT id, w AS v FROM t", "owns an index"),
    ] {
        db.refuses(sql, Error, needle);
    }

    db.exec(
        "DROP INDEX pv_v;
         CREATE OR REPLACE VIEW pv AS SELECT id, w AS v FROM t;
         ALTER VIEW pv AS SELECT id, v FROM t",
    );
    // The drop cascades the index, so its name is free again.
    db.exec(
        "CREATE INDEX pv_v ON pv(v);
         DROP VIEW pv;
         CREATE VIEW pv AS SELECT id, v FROM t;
         CREATE INDEX pv_v ON pv(v)",
    );
}

// ── ALTER ────────────────────────────────────────────────────────────────────

#[test]
fn alter_results_and_if_exists_no_ops() {
    let mut db = Db::boot(4);
    db.exec(
        "CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT, a BIGINT NOT NULL, b BIGINT);
         INSERT INTO t VALUES (1, 10, 1, 100), (2, 20, 2, 200), (3, 30, 3, 300);
         CREATE VIEW vw AS SELECT id, v FROM t WHERE v >= 20;
         ALTER TABLE t RENAME TO t2",
    );
    db.refuses("SELECT * FROM t", NotFound, "not found");
    assert_eq!(
        db.scan("t2", &["id", "v"]),
        at_weight_one(&[vec![1, 10], vec![2, 20], vec![3, 30]])
    );
    // RENAME COLUMN is allowed under a dependent view: views bind columns by
    // ordinal.
    db.exec("ALTER TABLE t2 RENAME COLUMN v TO w; ALTER TABLE vw RENAME TO vw2");
    assert_eq!(
        visible_names(db.read("SELECT * FROM t2").schema()),
        ["id", "w", "a", "b"]
    );
    assert_eq!(db.scan("vw2", &["id", "v"]), at_weight_one(&[vec![2, 20], vec![3, 30]]));

    // A constraint name resolves case-insensitively.
    db.exec("ALTER TABLE t2 ADD CONSTRAINT uq2 UNIQUE (a); ALTER TABLE t2 DROP CONSTRAINT UQ2");
    db.exec("ALTER TABLE t2 ADD CONSTRAINT uq UNIQUE (b)");
    db.refuses(
        "INSERT INTO t2 VALUES (4, 40, 4, 100)",
        IntegrityViolation,
        "Unique index violation",
    );
    db.exec("ALTER TABLE t2 DROP CONSTRAINT uq; INSERT INTO t2 VALUES (4, 40, 4, 100)");

    db.exec("ALTER VIEW vw2 AS SELECT id, w FROM t2 WHERE w >= 30");
    assert_eq!(db.scan("vw2", &["id", "w"]), at_weight_one(&[vec![3, 30], vec![4, 40]]));
    // An existing column ends the statement before the dependent view's RESTRICT.
    db.exec("ALTER TABLE t2 ADD COLUMN IF NOT EXISTS w TEXT");
    db.exec("DROP VIEW vw2; ALTER TABLE t2 DROP COLUMN b");
    assert_eq!(visible_names(db.read("SELECT * FROM t2").schema()), ["id", "w", "a"]);
    db.exec("INSERT INTO t2 VALUES (5, 50, 5)");
    assert_eq!(
        db.rows("SELECT id, w, a FROM t2 WHERE id >= 4", &["id", "w", "a"]),
        at_weight_one(&[vec![4, 40, 4], vec![5, 50, 5]])
    );
    db.refuses("SELECT b FROM t2", Error, "not found");

    db.exec("ALTER TABLE t2 ALTER COLUMN a DROP NOT NULL; INSERT INTO t2 VALUES (6, 60, NULL)");
    assert_eq!(
        db.rows("SELECT id, a FROM t2 WHERE id = 6", &["id", "a"]),
        [[6, NULL, 1]]
    );

    // A dropped column's name is free: IF NOT EXISTS appends a new column under it.
    db.exec("ALTER TABLE t2 ADD COLUMN IF NOT EXISTS b BIGINT; ALTER TABLE t2 ADD COLUMN IF NOT EXISTS b TEXT");
    assert_eq!(
        visible_names(db.read("SELECT * FROM t2").schema()),
        ["id", "w", "a", "b"]
    );
    db.exec("INSERT INTO t2 VALUES (7, 70, 7, 700)");
    assert_eq!(
        db.rows("SELECT id, b FROM t2 WHERE id >= 6", &["id", "b"]),
        at_weight_one(&[vec![6, NULL], vec![7, 700]])
    );

    // A bounded view keeps its capacity across a rename, so it stays a leaf.
    db.exec("CREATE VIEW bv WITH (capacity = '1 MB') AS SELECT id, w FROM t2; ALTER TABLE bv RENAME TO bv2");
    db.refuses("CREATE VIEW over AS SELECT id FROM bv2", Error, "capacity-bounded");

    db.exec(
        "ALTER TABLE IF EXISTS nope RENAME TO x;
         ALTER TABLE IF EXISTS nope DROP COLUMN a;
         ALTER TABLE IF EXISTS nope ADD CONSTRAINT cq UNIQUE (v);
         ALTER TABLE IF EXISTS nope ADD COLUMN IF NOT EXISTS x BIGINT;
         ALTER TABLE t2 DROP CONSTRAINT IF EXISTS nope",
    );
}

#[test]
fn alter_rejection_matrix() {
    let mut db = Db::boot(1);
    db.exec(
        "CREATE TABLE t (id BIGINT PRIMARY KEY, a BIGINT NOT NULL, b BIGINT, c BIGINT);
         CREATE VIEW vw AS SELECT id, b FROM t;
         CREATE VIEW base AS SELECT id, c FROM t;
         CREATE VIEW dep AS SELECT id, c FROM base;
         CREATE INDEX ix ON t (c);
         CREATE TABLE u (id BIGINT PRIMARY KEY, v BIGINT);
         CREATE UNIQUE INDEX uq ON u (v);
         CREATE TABLE st (id BIGINT PRIMARY KEY, v BIGINT) WITH (stream = true)",
    );
    for (sql, want, needle) in [
        // ADD COLUMN honours a bare nullable append; each refused clause would
        // need a value for the rows already there, a second catalog object, or a
        // physical move, and names what to write instead where there is one.
        ("ALTER TABLE t ADD COLUMN x BIGINT NOT NULL", Error, "NOT NULL"),
        ("ALTER TABLE t ADD COLUMN x SERIAL", Error, "SERIAL"),
        ("ALTER TABLE t ADD COLUMN x BIGINT DEFAULT 0", Error, "DEFAULT"),
        ("ALTER TABLE t ADD COLUMN x BIGINT PRIMARY KEY", Error, "PRIMARY KEY"),
        ("ALTER TABLE t ADD COLUMN x BIGINT UNIQUE", Error, "CREATE UNIQUE INDEX"),
        (
            "ALTER TABLE t ADD COLUMN x BIGINT REFERENCES t (id)",
            Error,
            "only at CREATE TABLE",
        ),
        ("ALTER TABLE t ADD COLUMN x BIGINT CHECK (x > 0)", Error, "CHECK"),
        ("ALTER TABLE t ADD COLUMN x BIGINT COLLATE utf8", Error, "COLLATE"),
        ("ALTER TABLE t ADD COLUMN x BIGINT FIRST", Error, "FIRST/AFTER"),
        ("ALTER TABLE t ADD COLUMN x BIGINT AFTER a", Error, "FIRST/AFTER"),
        ("ALTER TABLE t ADD COLUMN a BIGINT", Error, "duplicate column name"),
        ("ALTER TABLE st ADD COLUMN x BIGINT", Error, "is a stream"),
        // The class and the clauses are checked before the IF NOT EXISTS no-op.
        ("ALTER TABLE st ADD COLUMN IF NOT EXISTS v BIGINT", Error, "is a stream"),
        (
            "ALTER TABLE t ADD COLUMN IF NOT EXISTS a BIGINT NOT NULL",
            Error,
            "NOT NULL",
        ),
        ("ALTER TABLE st ADD CONSTRAINT su UNIQUE (v)", Error, "is a stream"),
        ("ALTER TABLE t RENAME TO otherschema.t2", Error, "cross-schema"),
        // The new name is checked before the IF EXISTS no-op.
        (
            "ALTER TABLE IF EXISTS nope RENAME TO _x",
            Error,
            "cannot start with '_'",
        ),
        ("ALTER TABLE t RENAME COLUMN a TO b", Error, "duplicate column name"),
        ("ALTER TABLE t RENAME COLUMN nope TO z", Error, "not found"),
        ("ALTER TABLE vw RENAME COLUMN b TO w", Error, "base table"),
        (
            "ALTER TABLE t ADD CONSTRAINT cq UNIQUE (b) NOT VALID",
            Error,
            "NOT VALID",
        ),
        ("ALTER TABLE t ADD CONSTRAINT cq CHECK (b > 0)", Error, "UNIQUE"),
        ("ALTER TABLE t DROP CONSTRAINT nope", NotFound, "not found"),
        ("ALTER TABLE t DROP CONSTRAINT ix CASCADE", Error, "CASCADE"),
        ("ALTER TABLE t DROP CONSTRAINT _x", Error, "reserved"),
        // DROP CONSTRAINT drops a UNIQUE index of its own table only.
        (
            "ALTER TABLE t DROP CONSTRAINT ix",
            NotFound,
            "constraint 'ix' not found",
        ),
        ("ALTER TABLE t DROP CONSTRAINT uq", NotFound, "not found"),
        (
            "ALTER VIEW base AS SELECT id, c FROM t WHERE c > 0",
            Error,
            "cannot replace '",
        ),
        ("ALTER TABLE t ALTER COLUMN b SET NOT NULL", Error, "SET NOT NULL"),
        ("ALTER TABLE t ALTER COLUMN b SET DATA TYPE INT", Error, "SET DATA TYPE"),
        (
            "ALTER TABLE t ALTER COLUMN b ADD GENERATED ALWAYS AS IDENTITY",
            Error,
            "GENERATED",
        ),
        (
            "ALTER TABLE t RENAME COLUMN a TO a2, RENAME COLUMN b TO b2",
            Error,
            "more than one operation",
        ),
        ("ALTER TABLE ONLY t RENAME TO t2", Error, "ONLY"),
        ("ALTER TABLE t DROP COLUMN id", Error, "primary-key"),
        ("ALTER TABLE t DROP COLUMN b CASCADE", Error, "CASCADE"),
        ("ALTER TABLE t DROP COLUMN c", Error, "secondary index"),
        ("ALTER TABLE t ALTER COLUMN id DROP NOT NULL", Error, "primary-key"),
        // A dependent view blocks a column drop and a nullability change of any
        // column: its traces hold rows under the old comparator.
        ("ALTER TABLE t DROP COLUMN b", Error, "dependent view"),
        ("ALTER TABLE t ALTER COLUMN a DROP NOT NULL", Error, "dependent view"),
    ] {
        db.refuses(sql, want, needle);
    }
    // The misdirected constraint drop left `uq` standing.
    assert_eq!(db.indexes("u"), [(vec!["v".to_string()], true)]);
}

// ── DROP ─────────────────────────────────────────────────────────────────────

/// One `DROP` statement is one DDL zone: every name goes or none does, and the
/// dependency guards see the whole batch.
#[test]
fn a_multi_name_drop_is_one_atomic_zone() {
    let mut db = Db::boot(1);
    db.exec(
        "CREATE TABLE a (id BIGINT PRIMARY KEY);
         CREATE TABLE b (id BIGINT PRIMARY KEY);
         CREATE TABLE parent (id BIGINT PRIMARY KEY);
         CREATE TABLE child (id BIGINT PRIMARY KEY, p BIGINT REFERENCES parent(id));
         CREATE VIEW keeper AS SELECT id FROM b",
    );

    // `b` is read by a view, so its drop refuses — and `a` survives with it.
    db.refuses("DROP TABLE a, b", Error, ".keeper) — drop them first");
    assert!(db.exists("a"), "a is untouched");

    // A name written twice retires once.
    db.exec("DROP TABLE a, A");
    assert!(!db.exists("a"));

    // A FK child co-dropped in the same batch is self-resolving, so the pair
    // drops in either written order.
    db.exec("DROP TABLE parent, child");
    assert!(!db.exists("parent") && !db.exists("child"));

    db.exec("DROP VIEW keeper, keeper; DROP TABLE b");
    assert!(!db.exists("keeper") && !db.exists("b"));
}

/// `IF EXISTS` skips a name whose schema is missing, as PostgreSQL does;
/// without it, the drop is refused as a missing schema.
#[test]
fn if_exists_skips_a_drop_in_a_missing_schema() {
    let mut db = Db::boot(1);
    db.exec("CREATE TABLE t (id BIGINT PRIMARY KEY)");
    db.exec("DROP TABLE IF EXISTS nosuchschema.t, t");
    assert!(!db.exists("t"), "the batch's other name still drops");
    db.exec("DROP VIEW IF EXISTS nosuchschema.v");
    for sql in ["DROP TABLE nosuchschema.t", "DROP VIEW nosuchschema.v"] {
        db.refuses(sql, NotFound, "schema 'nosuchschema' not found");
    }
}

/// A qualifier is the relation's schema on every name surface, and a name
/// without one is the session's. An index name — being global — takes none.
#[test]
fn a_qualified_name_reaches_another_schema() {
    let mut db = Db::boot(4);
    let sn = db.sn.clone();
    let other = unique_schema("o");
    block_on(db.client.create_schema(&other, false)).unwrap();

    // Two relations named `t`, one per schema.
    db.exec(&format!(
        "CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT);
         CREATE TABLE {other}.t (id BIGINT PRIMARY KEY, v BIGINT);
         INSERT INTO t VALUES (1, 10), (2, 20);
         INSERT INTO {other}.t VALUES (1, 100), (3, 300)"
    ));
    let (id_v, mine) = (["id", "v"], [[1, 10, 1], [2, 20, 1]]);
    assert_eq!(db.scan("t", &id_v), mine);
    assert_eq!(db.scan(&format!("{sn}.t"), &id_v), mine);
    assert_eq!(db.scan(&format!("{other}.t"), &id_v), [[1, 100, 1], [3, 300, 1]]);

    // A view joins across schemas, and one is placed in a schema other than
    // the session's; an unqualified name in its body is still the session's.
    db.exec(&format!(
        "CREATE VIEW j AS SELECT a.id, a.v AS av, b.v AS bv FROM t a JOIN {other}.t b ON a.id = b.id;
         CREATE VIEW {other}.doubled AS SELECT id, v + v AS d FROM t"
    ));
    assert_eq!(db.scan("j", &["id", "av", "bv"]), [[1, 10, 100, 1]]);

    assert_eq!(db.affected(&format!("UPDATE {other}.t SET v = 101 WHERE id = 1")), 1);
    assert_eq!(db.affected(&format!("DELETE FROM {other}.t WHERE id = 3")), 1);
    db.exec(&format!("INSERT INTO {other}.t VALUES (2, 200)"));
    assert_eq!(db.scan("j", &["id", "av", "bv"]), [[1, 10, 101, 1], [2, 20, 200, 1]]);
    assert_eq!(
        db.scan(&format!("{other}.doubled"), &["id", "d"]),
        [[1, 20, 1], [2, 40, 1]]
    );

    // A CTE lives in no schema: `t` is the CTE, `{sn}.t` the table it shadows.
    db.exec(&format!(
        "CREATE VIEW c AS WITH t AS (SELECT id, v FROM {other}.t)
         SELECT x.id, x.v AS cv, y.v AS tv FROM t x JOIN {sn}.t y ON x.id = y.id"
    ));
    assert_eq!(db.scan("c", &["id", "cv", "tv"]), [[1, 101, 10, 1], [2, 200, 20, 1]]);

    // A foreign key reaches a parent in another schema, and an auto-named index
    // takes its owner's schema.
    db.exec(&format!(
        "CREATE TABLE child (id BIGINT PRIMARY KEY, p BIGINT REFERENCES {other}.t(id));
         INSERT INTO child VALUES (1, 2);
         CREATE INDEX ON {other}.t (v)"
    ));
    db.refuses("INSERT INTO child VALUES (2, 9)", IntegrityViolation, "");
    let catalog = block_on(db.client.catalog()).unwrap();
    let indexes = catalog.relations.iter().flat_map(|r| &r.indexes);
    assert!(indexes.into_iter().any(|r| r.name == format!("{other}__t__idx_v")));

    // A rename keeps the relation where it is.
    db.exec(&format!("ALTER TABLE {other}.doubled RENAME TO twice"));
    assert_eq!(
        db.scan(&format!("{other}.twice"), &["id", "d"]),
        [[1, 20, 1], [2, 40, 1]]
    );
    db.refuses(
        &format!("ALTER TABLE {other}.twice RENAME TO {sn}.twice"),
        Error,
        "cross-schema",
    );

    for (sql, want, needle) in [
        ("SELECT id FROM nosuch.t", NotFound, "schema 'nosuch' not found"),
        (&*format!("SELECT id FROM {other}.nope"), NotFound, "not found"),
        ("INSERT INTO nosuch.t VALUES (2, 20)", NotFound, "schema 'nosuch'"),
        ("SELECT id FROM db.s.t", Error, "too many name parts"),
        ("SELECT id FROM _other.t", Error, "cannot start with '_'"),
        // `_system` is spelled, and holds the families alone.
        ("SELECT id FROM _system.t", NotFound, "not found"),
        ("SELECT name FROM _system._tables", Error, "cannot start with '_'"),
        (
            "CREATE TABLE _system.mine (id BIGINT PRIMARY KEY)",
            Error,
            "the system schema takes no relation",
        ),
        (
            "CREATE VIEW _system.mine AS SELECT table_id, name FROM _system.tables",
            Error,
            "the system schema takes no relation",
        ),
        // `sequences` holds positions rather than a set, and no worker holds it.
        (
            "CREATE VIEW sv AS SELECT * FROM _system.sequences",
            Error,
            "holds no set a view can scan",
        ),
        ("DROP TABLE _system.tables", Error, "cannot DROP a system table"),
        (
            "ALTER TABLE _system.tables RENAME TO relations",
            Error,
            "cannot ALTER a system table",
        ),
        // A family is no base table to the statements that need one, refused
        // before any row is read.
        (
            "ALTER TABLE _system.tables ADD COLUMN extra BIGINT",
            Error,
            "is a system table",
        ),
        (
            "ALTER TABLE _system.tables DROP COLUMN stream",
            Error,
            "is a system table",
        ),
        (
            "ALTER TABLE _system.tables RENAME COLUMN name TO label",
            Error,
            "is a system table",
        ),
        ("CREATE INDEX ON _system.tables (schema_id)", Error, "is a system table"),
        (
            "CREATE INDEX sysix ON _system.tables (schema_id)",
            Error,
            "is a system table",
        ),
        (
            "CREATE UNIQUE INDEX sysux ON _system.tables (name)",
            Error,
            "is a system table",
        ),
        (
            "CREATE TABLE fk_sys (id BIGINT PRIMARY KEY, \
             t BIGINT UNSIGNED NOT NULL REFERENCES _system.tables(table_id))",
            Error,
            "which is a system table; a FOREIGN KEY must reference a base table",
        ),
        (
            "INSERT INTO _system.schemas VALUES (99, 'forged')",
            Error,
            "is a system table",
        ),
        (
            "UPDATE _system.tables SET name = 'x' WHERE table_id = 2",
            Error,
            "is a system table",
        ),
        (
            "DELETE FROM _system.tables WHERE table_id = 2",
            Error,
            "is a system table",
        ),
        // An index name is global, so a qualifier on one scopes nothing.
        ("CREATE INDEX other.ix ON t (v)", Error, "no qualifier"),
        ("DROP INDEX other.ix", Error, "no qualifier"),
        (&*format!("DROP INDEX {sn}.ix"), Error, "no qualifier"),
    ] {
        db.refuses(sql, want, needle);
    }

    // One statement retires relations of both schemas, or none of them: the
    // parent still has its FK child.
    db.refuses(&format!("DROP TABLE t, {other}.t"), Error, "");
    assert_eq!(db.scan("t", &id_v), mine);
    db.exec(&format!(
        "DROP VIEW j, c, {other}.twice;
         DROP TABLE child, {other}.t, t"
    ));
    for (schema, name) in [(&sn, "t"), (&other, "t"), (&other, "twice"), (&sn, "j")] {
        assert!(
            block_on(db.client.resolve(&rel(schema, name))).unwrap().is_none(),
            "{schema}.{name}"
        );
    }
}
