//! The RESOLVE verb, and a statement resolves each relation once: *what* one
//! reply carries, and *what it costs* — each relation exactly once per statement,
//! and nothing retained across statements to go stale under another client's DDL.

use super::*;
use gnitz_wire::{RelClass, WireStatus::NotFound};

const T_ID_V: &str = "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)";

// ── The descriptor ───────────────────────────────────────────────────────────

/// Found, absent under a live schema, and a missing schema — the last an error,
/// not an absent relation.
#[test]
fn resolve_reports_found_absent_and_missing_schema() {
    let mut db = Db::boot(1);
    db.exec(T_ID_V);
    let rel = db.rel("t");
    assert!(rel.tid >= gnitz_wire::FIRST_USER_TABLE_ID);
    assert_eq!(visible_names(&rel.schema), ["id", "v"]);
    assert!(!db.exists("nope"));
    let err = db.client.resolve("no_such_schema", "t").unwrap_err();
    assert!(
        matches!(&err, ClientError::Refused(WireFault { status: NotFound, .. })),
        "got: {err:?}"
    );
}

/// A name longer than the PK-region head rides the request's explicit blob, so
/// any length round-trips and a near-miss differing only past the head does not
/// resolve to it.
#[test]
fn a_long_relation_name_round_trips() {
    let mut db = Db::boot(1);
    let long = format!("a_relation_name_{}", "x".repeat(80));
    assert!(long.len() > gnitz_wire::MAX_PK_BYTES);
    db.exec(&format!("CREATE TABLE {long} (id BIGINT NOT NULL PRIMARY KEY)"));
    assert_eq!(visible_names(&db.rel(&long).schema), ["id"]);
    assert!(!db.exists(&format!("{long}_two")));
}

/// The facts beyond column names reach the client: each class and whether its
/// PK repeats, the PK list in declared order, a dropped column's hidden slot at
/// its physical position, and SERIAL.
#[test]
fn descriptor_fields_round_trip() {
    let mut db = Db::boot(1);
    db.exec(
        "CREATE TABLE p (id BIGINT NOT NULL PRIMARY KEY, other BIGINT NOT NULL, gone BIGINT NOT NULL);
         ALTER TABLE p DROP COLUMN gone;
         CREATE TABLE c (v BIGINT NOT NULL, a BIGINT UNSIGNED, b INT UNSIGNED, PRIMARY KEY (b, a));
         CREATE TABLE sr (id SERIAL PRIMARY KEY);
         CREATE TABLE st (id BIGINT PRIMARY KEY, v BIGINT) WITH (stream = true);
         CREATE VIEW v AS SELECT id, other FROM p;
         CREATE VIEW bv WITH (capacity = '1 MB') AS SELECT id, other FROM p;
         CREATE VIEW fv WITH (delta = '1 MB') AS SELECT id, other FROM p",
    );
    for (name, class, pk_repeats) in [
        ("p", RelClass::Table, false),
        ("st", RelClass::Stream, true),
        ("v", RelClass::View, false),
        ("bv", RelClass::BoundedView, false),
        ("fv", RelClass::FedView, false),
    ] {
        let rel = db.rel(name);
        assert_eq!((rel.class, rel.pk_repeats), (class, pk_repeats), "{name}");
    }

    let p = db.rel("p");
    let hidden: Vec<bool> = p.schema.columns.iter().map(|c| c.is_hidden).collect();
    assert_eq!(
        hidden,
        [false, false, true],
        "the dropped column is still physically present"
    );
    let c = db.rel("c");
    let pk: Vec<(u32, TypeCode)> = c
        .schema
        .pk_cols
        .iter()
        .map(|&i| (i, c.schema.columns[i as usize].ty.tc))
        .collect();
    assert_eq!(pk, [(2, TypeCode::U32), (1, TypeCode::U64)]);
    assert!(db.rel("sr").serial && !p.serial);
}

/// The index list is exact — an empty list means "no index", never "unchanged".
/// A dropped index must therefore disappear from the very next resolve.
#[test]
fn the_index_list_is_exact_across_create_and_drop() {
    let mut db = Db::boot(1);
    db.exec("CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL, b BIGINT NOT NULL)");
    assert!(db.indexes("t").is_empty(), "no index yet");
    let ab = (vec!["a".to_string(), "b".to_string()], false);
    let a = (vec!["a".to_string()], true);
    db.exec("CREATE INDEX ix_ab ON t(a, b)");
    assert_eq!(db.indexes("t"), std::slice::from_ref(&ab));
    db.exec("CREATE UNIQUE INDEX ix_a ON t(a)");
    assert_eq!(db.indexes("t"), [a.clone(), ab]);
    db.exec("DROP INDEX ix_ab");
    assert_eq!(db.indexes("t"), [a]);
}

/// Every id-addressed probe ends at the same registration gate, so no
/// client-chosen tid reaches the column reader. None of these may abort or hang
/// the master or desync the connection; each is a clean miss.
#[test]
fn an_unregistered_id_is_a_clean_miss() {
    let mut db = Db::boot(1);
    for tid in [
        1_000_000_u64,                  // never allocated
        gnitz_wire::CATALOG_ID_CEILING, // at the catalog id ceiling
        1 << 55,                        // a plausible id no allocation reaches
        u64::MAX,                       // negative once cast to i64
        u64::MAX - 1,
    ] {
        let err = db
            .client
            .describe_by_id(tid)
            .expect_err("an unregistered id must not resolve");
        assert!(
            matches!(&err, ClientError::Refused(WireFault { status: NotFound, .. })),
            "tid {tid}: got {err:?}"
        );
    }
    db.exec(T_ID_V);
    assert!(db.exists("t"));
}

// ── Statement cost ───────────────────────────────────────────────────────────

/// Request frames one `execute` of `sql` costs, whether or not it succeeds.
fn cost(db: &mut Db, sql: &str) -> u64 {
    let before = db.client.requests_sent();
    let _ = db.try_exec(sql);
    db.client.requests_sent() - before
}

/// A statement resolves each relation exactly once — on the success path and on
/// a miss alike, whatever its shape and however many segments its plan has —
/// and a DDL verb acts on the descriptor its statement resolved. A transaction
/// binds each table's name once: after its first statement on a table an INSERT
/// sends nothing until COMMIT and an UPDATE sends only its read; a view takes no
/// writes, so it is resolved afresh each statement. An equality, not a bound,
/// so a regression to a second resolve fails here instead of fitting under a
/// slack ceiling.
#[test]
fn a_statement_resolves_each_relation_once() {
    let mut db = Db::boot(1);
    db.exec(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, g BIGINT NOT NULL, x BIGINT NOT NULL);
         CREATE INDEX ix ON t(x);
         INSERT INTO t (id, g, x) VALUES (1, 1, 10), (2, 1, 10);
         CREATE TABLE w (id BIGINT NOT NULL PRIMARY KEY);
         CREATE VIEW v AS SELECT id, x FROM t",
    );
    for (sql, want, what) in [
        ("SELECT * FROM t", 2, "resolve, read"),
        ("SELECT * FROM t WHERE id = 2", 2, "resolve, read"),
        ("SELECT * FROM t WHERE x = 10", 2, "resolve, read"),
        ("SELECT g, COUNT(*) FROM t WHERE x = 10 GROUP BY g", 2, "resolve, read"),
        (
            "SELECT g, COUNT(*) FROM t WHERE x = 10 GROUP BY g HAVING COUNT(*) > 0",
            2,
            "resolve, read",
        ),
        ("INSERT INTO nope (id) VALUES (1)", 1, "resolve"),
        (
            "ALTER TABLE w ADD CONSTRAINT _bad UNIQUE (id)",
            0,
            "a malformed name is refused by the statement alone",
        ),
        ("ALTER TABLE w ADD COLUMN c BIGINT", 2, "resolve, push"),
        ("ALTER TABLE w DROP COLUMN c", 3, "resolve, seek, push"),
        (
            "BEGIN; INSERT INTO t VALUES (3, 1, 30); INSERT INTO t VALUES (4, 1, 40); COMMIT",
            2,
            "resolve, PUSH_TXN",
        ),
        (
            "BEGIN; UPDATE t SET x = x + 1 WHERE id = 3; UPDATE t SET x = x + 1 WHERE id = 4; COMMIT",
            4,
            "resolve, two reads, PUSH_TXN",
        ),
        (
            "BEGIN; SELECT * FROM v; SELECT * FROM v; ROLLBACK",
            4,
            "a resolve and a read per SELECT",
        ),
        ("ALTER TABLE t RENAME TO u", 3, "resolve, seek, push"),
        ("ALTER VIEW v AS SELECT id FROM u", 5, "two resolves, seek, alloc, push"),
        (
            "CREATE OR REPLACE VIEW v AS SELECT id, x FROM u",
            5,
            "two resolves, seek, alloc, push",
        ),
    ] {
        assert_eq!(cost(&mut db, sql), want, "`{sql}`: {what}");
    }
    assert_eq!(
        db.scan("u", &["id", "x"]),
        at_weight_one(&[vec![1, 10], vec![2, 10], vec![3, 31], vec![4, 41]])
    );
}

/// `DROP SCHEMA` is one bundle whatever the member count: the SCHEMA_TAB probe,
/// one VIEW_TAB scan, one TABLE_TAB scan and one push.
#[test]
fn drop_schema_is_four_requests_whatever_the_member_count() {
    let mut db = Db::boot(1);
    for i in 0..4 {
        db.exec(&format!(
            "CREATE TABLE m{i} (id BIGINT NOT NULL PRIMARY KEY); CREATE VIEW vw{i} AS SELECT id FROM m{i}"
        ));
    }
    let before = db.client.requests_sent();
    db.client.drop_schema(&db.sn).unwrap();
    assert_eq!(db.client.requests_sent() - before, 4);
    db.client.create_schema(&db.sn).unwrap();
    assert!(!db.exists("m0") && !db.exists("vw0"));
}

// ── DDL that a client-side descriptor cache would get wrong ──────────────────

/// `ALTER … RENAME TO` then recreating the old name: an idle second client's
/// next statement must see the *new* relation — nothing is retained across
/// statements to go stale.
#[test]
fn a_second_client_sees_a_rename_then_recreate() {
    let mut a = Db::boot(4);
    let mut b = a.peer();
    a.exec(T_ID_V);
    a.exec("INSERT INTO t (id, v) VALUES (1, 100)");
    assert_eq!(b.scan("t", &["id", "v"]), [[1, 100, 1]]);
    let old_tid = b.rel("t").tid;

    a.exec(&format!(
        "ALTER TABLE t RENAME TO u; {T_ID_V}; INSERT INTO t (id, v) VALUES (2, 200), (3, 300)"
    ));
    assert_ne!(b.rel("t").tid, old_tid, "the name now binds a different relation");
    assert_eq!(b.scan("t", &["id", "v"]), [[2, 200, 1], [3, 300, 1]]);

    b.exec("INSERT INTO t (id, v) VALUES (4, 400)");
    assert_eq!(a.scan("t", &["id", "v"]), [[2, 200, 1], [3, 300, 1], [4, 400, 1]]);
    assert_eq!(a.scan("u", &["id", "v"]), [[1, 100, 1]]);
}

/// `ALTER COLUMN … DROP NOT NULL` on A, then a raw binary `push` on B through
/// `resolve_relation` + `push` — the surface with no SQL layer above it.
#[test]
fn a_second_client_pushes_after_a_column_alter() {
    use gnitz_core::BatchAppender;
    use gnitz_wire::WireConflictMode::Update;
    let mut a = Db::boot(4);
    let mut b = a.peer();
    a.exec(T_ID_V);
    let rel = b.rel("t");
    let mut batch = ZSetBatch::new(&rel.schema);
    BatchAppender::new(&mut batch).add_row(1, 1).i64_val(10);
    b.client.push(rel.tid, &rel.schema, &batch, Update).unwrap();

    a.exec("ALTER TABLE t ALTER COLUMN v DROP NOT NULL");

    let rel2 = b.rel("t");
    assert_eq!(rel2.tid, rel.tid);
    assert!(rel2.schema.columns[1].is_nullable, "B sees the relaxed column");
    let mut batch = ZSetBatch::new(&rel2.schema);
    BatchAppender::new(&mut batch).add_row(2, 1).null();
    b.client.push(rel2.tid, &rel2.schema, &batch, Update).unwrap();

    assert_eq!(a.scan("t", &["id", "v"]), [[1, 10, 1], [2, NULL, 1]]);
}

/// A schema dropped and recreated under the same name resolves only its new
/// members — the qname denormalizes the schema *name* at insert time, so a
/// recreated schema must rebuild its members' qnames rather than resurrect the
/// old ones.
#[test]
fn a_recreated_schema_resolves_its_new_members() {
    let mut db = Db::boot(1);
    db.exec("CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY)");
    let old_tid = db.rel("t").tid;

    db.client.drop_schema(&db.sn).unwrap();
    db.client.create_schema(&db.sn).unwrap();
    assert!(
        !db.exists("t"),
        "the old member must not resurrect under the recreated schema"
    );

    db.exec(T_ID_V);
    let rel = db.rel("t");
    assert_ne!(rel.tid, old_tid);
    assert_eq!(visible_names(&rel.schema), ["id", "v"]);
}
