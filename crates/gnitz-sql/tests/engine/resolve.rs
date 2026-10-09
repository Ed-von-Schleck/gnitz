//! The RESOLVE verb, and the descriptor a client keeps of it: *what* one reply
//! carries, *what it costs* — a name is resolved once and kept — and what finds
//! a kept descriptor out once another client's DDL has made it stale.

use super::*;
use gnitz_core::{block_on, BlockingHost, Host, Interest, Job};
use gnitz_wire::{RelClass, WireStatus::NotFound};
use std::os::fd::BorrowedFd;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};

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
    let err = block_on(db.client.resolve(&crate::rel("no_such_schema", "t"))).unwrap_err();
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
    let hidden: Vec<bool> = p.schema.columns().iter().map(|c| c.is_hidden).collect();
    assert_eq!(
        hidden,
        [false, false, true],
        "the dropped column is still physically present"
    );
    let c = db.rel("c");
    let pk: Vec<(u32, TypeCode)> = c
        .schema
        .pk_cols()
        .iter()
        .map(|&i| (i, c.schema.columns()[i as usize].ty.tc))
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

// ── Statement cost ───────────────────────────────────────────────────────────

/// Request frames one `execute` of `sql` costs, whether or not it succeeds.
fn cost(db: &mut Db, sql: &str) -> u64 {
    let before = db.client.requests_sent();
    let _ = db.try_exec(sql);
    db.client.requests_sent() - before
}

/// DML resolves a name once and keeps the answer: the statements after the
/// first send their own requests alone, inside a transaction and out. A miss is
/// not kept, a DDL statement always resolves, and this client's own DDL forgets
/// the relations it wrote and no other. An equality, not a bound, so a regression to a second resolve
/// fails here instead of fitting under a slack ceiling.
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
        (
            "SELECT * FROM t",
            1,
            "read: the CREATE VIEW resolved `t` and wrote no row of it",
        ),
        ("SELECT * FROM t WHERE id = 2", 1, "read"),
        ("SELECT * FROM t WHERE x = 10", 1, "read"),
        ("SELECT g, COUNT(*) FROM t WHERE x = 10 GROUP BY g", 1, "read"),
        (
            "SELECT g, COUNT(*) FROM t WHERE x = 10 GROUP BY g HAVING COUNT(*) > 0",
            1,
            "read",
        ),
        ("UPDATE t SET x = 10 WHERE id = 2", 2, "read, PUSH_TXN"),
        ("INSERT INTO t VALUES (6, 1, 60)", 1, "push"),
        ("DELETE FROM t WHERE id = 6", 2, "read, PUSH_TXN"),
        (
            "SELECT * FROM t LIMIT 0",
            1,
            "resolve: no request checks a kept descriptor",
        ),
        (
            "EXPLAIN SELECT * FROM t",
            1,
            "resolve: no request checks a kept descriptor",
        ),
        (
            "SELECT nope FROM t",
            1,
            "resolve: a kept descriptor's refusal is planned again",
        ),
        ("INSERT INTO nope (id) VALUES (1)", 1, "resolve"),
        ("INSERT INTO nope (id) VALUES (1)", 1, "resolve: a miss is not kept"),
        (
            "ALTER TABLE w ADD CONSTRAINT _bad UNIQUE (id)",
            0,
            "a malformed name is refused by the statement alone",
        ),
        ("ALTER TABLE w ADD COLUMN c BIGINT", 2, "resolve, push"),
        ("ALTER TABLE w DROP COLUMN c", 3, "resolve, seek, push"),
        (
            "INSERT INTO w (id) VALUES (1)",
            2,
            "resolve, push: the ALTER wrote a column of `w`",
        ),
        ("INSERT INTO w (id) VALUES (2)", 1, "push"),
        (
            "BEGIN; INSERT INTO t VALUES (3, 1, 30); INSERT INTO t VALUES (4, 1, 40); COMMIT",
            1,
            "PUSH_TXN: the DDL on `w` wrote no row of `t`",
        ),
        (
            "BEGIN; UPDATE t SET x = x + 1 WHERE id = 3; UPDATE t SET x = x + 1 WHERE id = 4; COMMIT",
            3,
            "two reads, PUSH_TXN",
        ),
        (
            "BEGIN; UPDATE t SET x = x + 1 WHERE id = 3; UPDATE t SET x = x + 1 WHERE id = 3; COMMIT",
            2,
            "one read, PUSH_TXN: the second UPDATE reads the first one's row",
        ),
        (
            "BEGIN; INSERT INTO t VALUES (5, 1, 50); UPDATE t SET x = x + 1 WHERE id = 5; COMMIT",
            1,
            "PUSH_TXN: the UPDATE reads the INSERT's row",
        ),
        (
            "BEGIN; SELECT * FROM v; SELECT * FROM v; ROLLBACK",
            3,
            "resolve, two reads",
        ),
        ("ALTER TABLE t RENAME TO u", 3, "resolve, seek, push"),
        (
            "ALTER VIEW v AS SELECT id FROM u",
            5,
            "two resolves, one read of the catalog, alloc, push",
        ),
        (
            "CREATE OR REPLACE VIEW v AS SELECT id, x FROM u",
            5,
            "two resolves, one read of the catalog, alloc, push",
        ),
        (
            "SELECT * FROM u",
            1,
            "read: the replace resolved `u` and wrote no row of it",
        ),
        (
            "CREATE OR REPLACE VIEW v AS SELECT id, x FROM u",
            3,
            "two resolves, one read of the catalog: the definition stands",
        ),
        (
            "SELECT * FROM u",
            1,
            "read: a replace that wrote nothing drops no descriptor",
        ),
        ("CREATE INDEX ug ON u(g)", 3, "resolve, alloc, push"),
        (
            "SELECT * FROM u",
            2,
            "resolve, read: the index row names `u` as its owner",
        ),
    ] {
        assert_eq!(cost(&mut db, sql), want, "`{sql}`: {what}");
    }
    assert_eq!(
        db.scan("u", &["id", "x"]),
        at_weight_one(&[vec![1, 10], vec![2, 10], vec![3, 33], vec![4, 41], vec![5, 51]])
    );
}

#[test]
fn drop_schema_is_one_read_and_one_push_whatever_the_member_count() {
    let mut db = Db::boot(1);
    for i in 0..4 {
        db.exec(&format!(
            "CREATE TABLE m{i} (id BIGINT NOT NULL PRIMARY KEY); CREATE VIEW vw{i} AS SELECT id FROM m{i}"
        ));
    }
    let before = db.client.requests_sent();
    block_on(db.client.drop_schema(&db.sn)).unwrap();
    assert_eq!(db.client.requests_sent() - before, 2);
    block_on(db.client.create_schema(&db.sn)).unwrap();
    assert!(!db.exists("m0") && !db.exists("vw0"));
}

// ── A kept descriptor under another client's DDL ─────────────────────────────

/// `ALTER … RENAME TO` then recreating the old name: a second client's next
/// statement must see the *new* relation, though the descriptor it kept names
/// the old one, which still exists under its new name. The stale read is
/// refused, and the statement resolves and reads again.
#[test]
fn a_second_client_sees_a_rename_then_recreate() {
    let mut a = Db::boot(4);
    let mut b = a.peer();
    a.exec(T_ID_V);
    a.exec("INSERT INTO t (id, v) VALUES (1, 100)");
    assert_eq!(b.scan("t", &["id", "v"]), [[1, 100, 1]]);
    assert_eq!(cost(&mut b, "SELECT * FROM t"), 1, "`t` is kept");

    a.exec(&format!(
        "ALTER TABLE t RENAME TO u; {T_ID_V}; INSERT INTO t (id, v) VALUES (2, 200), (3, 300)"
    ));
    let before = b.client.requests_sent();
    assert_eq!(b.scan("t", &["id", "v"]), [[2, 200, 1], [3, 300, 1]]);
    assert_eq!(b.client.requests_sent() - before, 3, "refused read, resolve, read");

    b.exec("INSERT INTO t (id, v) VALUES (4, 400)");
    assert_eq!(a.scan("t", &["id", "v"]), [[2, 200, 1], [3, 300, 1], [4, 400, 1]]);
    assert_eq!(a.scan("u", &["id", "v"]), [[1, 100, 1]]);
}

/// A blocking host whose next wait, once armed, is interrupted.
struct Interruptible {
    host: BlockingHost,
    armed: Arc<AtomicBool>,
}

impl Host for Interruptible {
    fn attach(&mut self, fd: BorrowedFd<'_>) -> std::io::Result<()> {
        self.host.attach(fd)
    }

    fn poll_io(
        &mut self,
        want: Interest,
        cx: &mut Context<'_>,
        io: &mut dyn FnMut(Interest) -> bool,
    ) -> Poll<Result<(), ClientError>> {
        if self.armed.swap(false, Ordering::Relaxed) {
            let why: Box<dyn std::error::Error + Send + Sync> = "interrupted".into();
            return Poll::Ready(Err(ClientError::Interrupted(why.into())));
        }
        self.host.poll_io(want, cx, io)
    }

    fn spawn(&mut self, job: Job) {
        self.host.spawn(job)
    }
}

/// An interrupt during a RESOLVE ends the statement, a kept descriptor in the
/// plan or not: the statement asks for `t1`, which is kept, before `t2`, whose
/// RESOLVE is the one request it sends.
#[test]
fn an_interrupted_resolve_ends_the_statement() {
    let mut db = Db::boot(1);
    db.exec("CREATE TABLE t1 (id BIGINT NOT NULL PRIMARY KEY); CREATE TABLE t2 (id BIGINT NOT NULL PRIMARY KEY)");
    let armed = Arc::new(AtomicBool::new(false));
    let host = Interruptible {
        host: BlockingHost::default(),
        armed: Arc::clone(&armed),
    };
    let mut client = block_on(GnitzClient::connect_with(db.srv.sock_path(), Box::new(host))).unwrap();
    block_on(gnitz_sql::execute(&mut client, &db.sn, "SELECT * FROM t1")).unwrap();

    let before = client.requests_sent();
    armed.store(true, Ordering::Relaxed);
    let two = "WITH a AS (SELECT * FROM t1), b AS (SELECT * FROM t2) SELECT * FROM a";
    let e = block_on(gnitz_sql::execute(&mut client, &db.sn, two)).unwrap_err();
    assert!(
        matches!(e, GnitzSqlError::Client(ClientError::Interrupted(_))),
        "got: {e:?}"
    );
    assert_eq!(client.requests_sent() - before, 1, "the RESOLVE of `t2`, and no replan");
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
    block_on(b.client.push(rel.tid, &rel.schema, &batch, Update)).unwrap();

    a.exec("ALTER TABLE t ALTER COLUMN v DROP NOT NULL");

    let rel2 = b.rel("t");
    assert_eq!(rel2.tid, rel.tid);
    assert!(rel2.schema.columns()[1].is_nullable, "B sees the relaxed column");
    let mut batch = ZSetBatch::new(&rel2.schema);
    BatchAppender::new(&mut batch).add_row(2, 1).null();
    block_on(b.client.push(rel2.tid, &rel2.schema, &batch, Update)).unwrap();

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

    block_on(db.client.drop_schema(&db.sn)).unwrap();
    block_on(db.client.create_schema(&db.sn)).unwrap();
    assert!(
        !db.exists("t"),
        "the old member must not resurrect under the recreated schema"
    );

    db.exec(T_ID_V);
    let rel = db.rel("t");
    assert_ne!(rel.tid, old_tid);
    assert_eq!(visible_names(&rel.schema), ["id", "v"]);
}

/// A stale kept descriptor reaches no row: each write below is planned from the
/// descriptor of a relation another client has since renamed away, dropped and
/// recreated, or dropped an index of, and lands in the relation its name
/// resolves to now.
#[test]
fn a_stale_write_is_refused_before_it_lands() {
    let mut a = Db::boot(4);
    let mut b = a.peer();
    a.exec(&format!(
        "{T_ID_V}; CREATE TABLE sr (id SERIAL PRIMARY KEY, v BIGINT NOT NULL)"
    ));
    a.exec("CREATE INDEX ix ON t(v); INSERT INTO t VALUES (1, 100), (2, 100)");
    b.exec("INSERT INTO t VALUES (3, 300)");

    a.exec(&format!("ALTER TABLE t RENAME TO u; {T_ID_V}"));
    b.exec("INSERT INTO t VALUES (4, 400)");
    assert_eq!(b.affected("UPDATE t SET v = 401 WHERE id = 4"), 1);
    assert_eq!(b.affected("DELETE FROM t WHERE id = 1"), 0, "row 1 is `u`'s");
    assert_eq!(a.scan("t", &["id", "v"]), [[4, 401, 1]]);
    assert_eq!(a.scan("u", &["id", "v"]), [[1, 100, 1], [2, 100, 1], [3, 300, 1]]);

    // A SERIAL insert reserves ids before it pushes, and `b` holds none for
    // `sr`: the reservation is refused too, rather than advancing the sequence
    // of the table `sr` named before.
    assert!(b.scan("sr", &["v"]).is_empty());
    a.exec("ALTER TABLE sr RENAME TO old; CREATE TABLE sr (id SERIAL PRIMARY KEY, v BIGINT NOT NULL)");
    assert_eq!(
        cost(&mut b, "INSERT INTO sr (v) VALUES (2)"),
        4,
        "refused reservation, resolve, reservation, push"
    );
    a.exec("INSERT INTO old (v) VALUES (3)");
    assert_eq!(a.scan("sr", &["id", "v"]), [[1, 2, 1]]);
    assert_eq!(a.scan("old", &["id", "v"]), [[1, 3, 1]], "no id of `old` was taken");

    // The kept descriptor of `u` (resolved here) lists `ix`; the read planned
    // through it is refused once the index is gone.
    assert_eq!(b.rows("SELECT * FROM u WHERE v = 100", &["id"]), [[1, 1], [2, 1]]);
    a.exec("DROP INDEX ix");
    assert_eq!(b.rows("SELECT * FROM u WHERE v = 100", &["id"]), [[1, 1], [2, 1]]);
}

/// What a statement concludes from a kept descriptor without sending a request
/// does not stand: a planning error, a `LIMIT 0` answered from the schema and
/// an EXPLAIN are all planned again from the server's answer.
#[test]
fn a_kept_descriptor_answers_nothing_without_a_request() {
    let mut a = Db::boot(4);
    let mut b = a.peer();
    a.exec(&format!("{T_ID_V}; INSERT INTO t VALUES (1, 100)"));
    assert_eq!(b.scan("t", &["id", "v"]), [[1, 100, 1]]);

    a.exec("ALTER TABLE t ADD COLUMN c BIGINT");
    assert_eq!(
        b.rows("SELECT c FROM t", &["c"]),
        [[NULL, 1]],
        "`c` is not in the kept descriptor"
    );
    a.exec("ALTER TABLE t ADD COLUMN d BIGINT");
    b.exec("INSERT INTO t (id, v, d) VALUES (2, 200, 7)");
    assert_eq!(a.scan("t", &["id", "d"]), [[1, NULL, 1], [2, 7, 1]]);

    a.exec("ALTER TABLE t ADD COLUMN e BIGINT");
    let (schema, batch) = b.read("SELECT * FROM t LIMIT 0");
    assert_eq!(visible_names(&schema), ["id", "v", "c", "d", "e"]);
    assert!(batch.is_empty());

    let plan = |db: &mut Db| {
        let (_, batch) = db.read("EXPLAIN SELECT * FROM t WHERE v = 100");
        format!("{batch:?}")
    };
    let unindexed = plan(&mut b);
    a.exec("CREATE INDEX ix ON t(v)");
    assert_ne!(
        plan(&mut b),
        unindexed,
        "the EXPLAIN names the index another client created"
    );
}

/// A descriptor is stale only when its own relation resolves differently:
/// another client's DDL elsewhere refuses nothing, and an open transaction
/// commits across it. DDL on a relation the transaction wrote fails the COMMIT.
#[test]
fn ddl_elsewhere_refuses_nothing() {
    let mut a = Db::boot(4);
    let mut b = a.peer();
    a.exec(&format!("{T_ID_V}; CREATE TABLE w (id BIGINT NOT NULL PRIMARY KEY)"));
    b.exec("INSERT INTO t VALUES (1, 100)");

    b.exec("BEGIN; INSERT INTO t VALUES (2, 200)");
    a.exec("CREATE TABLE other (id BIGINT NOT NULL PRIMARY KEY); CREATE INDEX wx ON w(id); DROP TABLE other");
    assert_eq!(cost(&mut b, "SELECT * FROM t WHERE id = 1"), 1, "read");
    b.exec("COMMIT");
    assert_eq!(a.scan("t", &["id", "v"]), [[1, 100, 1], [2, 200, 1]]);

    b.exec("BEGIN; INSERT INTO t VALUES (3, 300)");
    a.exec("CREATE INDEX ix ON t(v)");
    b.refuses("COMMIT", Refused(WireStatus::TxnConflict), "no longer resolves");
    assert_eq!(a.scan("t", &["id", "v"]), [[1, 100, 1], [2, 200, 1]]);
}
