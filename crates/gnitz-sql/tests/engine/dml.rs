//! Writes through the planner into the engine: the targets and clauses a write
//! may name, transaction control, ON CONFLICT and UPDATE/DELETE against buffered
//! and committed rows, and reads past one reply frame.

use super::*;
use gnitz_wire::WireStatus::Error;

/// A write clause gnitz does not honour is named rather than dropped, as is a
/// write its target cannot take — a view, a stream, a reserved name, a row of
/// the wrong shape; none of them writes anything.
#[test]
fn a_refused_write_names_its_rule_and_writes_nothing() {
    let mut db = Db::boot(1);
    db.exec(
        "CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT NOT NULL, s TEXT);
         CREATE TABLE c (a BIGINT UNSIGNED, b BIGINT UNSIGNED, v BIGINT, PRIMARY KEY (a, b));
         CREATE TABLE st (id BIGINT PRIMARY KEY, v BIGINT) WITH (stream = true);
         CREATE VIEW vw AS SELECT * FROM t;
         INSERT INTO t VALUES (1, 10, 'a')",
    );
    for (sql, needle) in [
        // A VALUES row carries exactly one value per column, and only values the
        // writer evaluates — an unsupported one echoed as written.
        ("INSERT INTO t VALUES (2, 20)", "expects 3 value(s)"),
        ("INSERT INTO t VALUES (2, 20, 'b', 99)", "expects 3 value(s)"),
        (
            "INSERT INTO t VALUES (2, EXTRACT(YEAR FROM 2), 'b')",
            "EXTRACT(YEAR FROM 2)",
        ),
        ("INSERT INTO t VALUES (2, NULL, 'b')", "NOT NULL"),
        ("INSERT INTO t VALUES (2, 20, 'b') LIMIT 1", "LIMIT/OFFSET"),
        ("INSERT INTO t VALUES (2, 20, 'b') FOR UPDATE", "FOR UPDATE"),
        ("INSERT IGNORE INTO t VALUES (2, 20, 'b')", "IGNORE"),
        ("REPLACE INTO t VALUES (2, 20, 'b')", "REPLACE INTO"),
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
        // not more, not a prefix.
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
            "INSERT INTO t VALUES (1, 20, 'b') ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v WHERE v > 5",
            "DO UPDATE: WHERE",
        ),
        (
            "INSERT INTO t VALUES (1, 20, 'b'), (1, 30, 'c') ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v",
            "second time",
        ),
        ("UPDATE t SET v = 9 WHERE id = 1 RETURNING id", "RETURNING"),
        ("DELETE FROM t WHERE id = 1 RETURNING id", "RETURNING"),
        ("DELETE FROM t LIMIT 1", "LIMIT"),
        ("DELETE FROM t ORDER BY id", "ORDER BY"),
        // The join forms are refused before any name resolves, so `other` need
        // not exist.
        ("UPDATE t SET v = o.v FROM other o WHERE t.id = o.id", "join-update"),
        ("DELETE FROM t USING other o WHERE t.id = o.id", "join-delete"),
        ("UPDATE t JOIN c ON t.id = c.a SET v = 1", "exactly one simple FROM"),
        ("UPDATE t SET id = 9 WHERE id = 1", "primary key"),
        ("UPDATE t SET v = UPPER(s)", "cannot assign a string value"),
        // A written alias displaces the table name, and a qualifier naming
        // anything else is not silently ignored.
        ("UPDATE t AS x SET v = 1 WHERE t.v = 1", "not found"),
        ("DELETE FROM t WHERE nope.v = 1", "not found"),
        // A view is read-only.
        ("INSERT INTO vw VALUES (2, 20, 'b')", "is a view"),
        ("UPDATE vw SET v = 1 WHERE id = 1", "is a view"),
        ("DELETE FROM vw", "is a view"),
        ("CREATE INDEX ix ON vw (v)", "is a view"),
        // A reserved name is refused before the catalog is probed.
        ("INSERT INTO _seg999999 VALUES (1, 1)", "cannot start with '_'"),
        ("CREATE INDEX ix ON _seg999999 (v)", "cannot start with '_'"),
        // A stream has no stored row to mutate or to resolve a conflict against.
        ("UPDATE st SET v = 1 WHERE id = 1", "is a stream"),
        ("DELETE FROM st WHERE id = 1", "is a stream"),
        (
            "INSERT INTO st VALUES (1, 2) ON CONFLICT (id) DO NOTHING",
            "is a stream",
        ),
    ] {
        db.refuses(sql, Rejected, needle);
    }
    assert!(db.scan("c", &["a"]).is_empty());
    // The written alias is the one qualifier that answers.
    db.exec("UPDATE t AS x SET v = 11 WHERE x.v = 10");
    assert_eq!(db.scan("t", &["id", "v"]), [[1, 11, 1]]);
}

/// Transaction control is a client-side state machine: a COMMIT or ROLLBACK
/// with nothing open and a nested BEGIN are refused, each isolation, chaining
/// or savepoint clause names itself and opens or closes nothing, and a
/// statement that may not run inside a transaction is refused without closing
/// it. EXPLAIN issues strictly less than a SELECT, so it runs inside one.
#[test]
fn transaction_control_refuses_what_it_cannot_honour() {
    const CLAUSES: [(&str, &str); 6] = [
        ("BEGIN READ ONLY", "transaction modes"),
        ("START TRANSACTION ISOLATION LEVEL SERIALIZABLE", "transaction modes"),
        ("BEGIN DEFERRED", "BEGIN modifier"),
        ("COMMIT AND CHAIN", "AND CHAIN"),
        ("ROLLBACK AND CHAIN", "AND CHAIN"),
        ("ROLLBACK TO SAVEPOINT sp", "TO SAVEPOINT"),
    ];
    let mut db = Db::boot(1);
    db.exec("CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)");
    db.refuses("COMMIT", Refused(Error), "no transaction open");
    db.refuses("ROLLBACK", Refused(Error), "no transaction open");
    for (sql, needle) in CLAUSES {
        db.refuses(sql, Rejected, needle);
        assert!(!db.client.txn_active(), "`{sql}` opened a transaction");
    }

    db.exec("BEGIN; INSERT INTO t VALUES (1, 10)");
    db.refuses("BEGIN", Refused(Error), "already open");
    for (sql, needle) in CLAUSES.into_iter().chain([
        (
            "CREATE TABLE t2 (id BIGINT PRIMARY KEY)",
            "not allowed inside a transaction",
        ),
        ("CREATE VIEW v AS SELECT id FROM t", "not allowed inside a transaction"),
        ("CREATE INDEX ON t (v)", "not allowed inside a transaction"),
        ("DROP TABLE t", "not allowed inside a transaction"),
    ]) {
        db.refuses(sql, Rejected, needle);
        assert!(db.client.txn_active(), "`{sql}` closed the transaction");
    }
    let (schema, plan) = db.read("EXPLAIN SELECT v FROM t WHERE id = 5");
    assert_eq!(visible_names(&schema), ["plan"]);
    assert!(!plan.is_empty() && db.client.txn_active());
    db.exec("COMMIT");
    assert_eq!(db.scan("t", &["id", "v"]), [[1, 10, 1]]);
}

/// A transaction a failing call began is rolled back, even when that call first
/// committed one an earlier call opened; one an earlier call began is the
/// caller's to end.
#[test]
fn a_failing_call_rolls_back_only_the_transaction_it_began() {
    let mut db = Db::boot(1);
    db.exec("CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT); BEGIN; INSERT INTO t VALUES (1, 10)");
    db.try_exec("COMMIT; BEGIN; INSERT INTO t VALUES (2, 20); INSERT INTO nope VALUES (1, 1)")
        .expect_err("`nope` does not exist");
    assert!(
        !db.client.txn_active(),
        "the transaction this call began is rolled back"
    );
    assert_eq!(db.scan("t", &["id", "v"]), [[1, 10, 1]]);

    db.exec("BEGIN");
    db.try_exec("INSERT INTO nope VALUES (1, 1)")
        .expect_err("`nope` does not exist");
    assert!(db.client.txn_active(), "a transaction an earlier call began stays open");
    db.exec("ROLLBACK");
}

/// Another client's `ADD COLUMN` inside a transaction fails its COMMIT cleanly,
/// writing nothing.
#[test]
fn a_concurrent_add_column_fails_the_commit_cleanly() {
    let mut a = Db::boot(1);
    let mut b = a.peer();
    a.exec(
        "CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT); INSERT INTO t VALUES (1, 10);
         BEGIN; INSERT INTO t VALUES (2, 20)",
    );
    b.exec("ALTER TABLE t ADD COLUMN c BIGINT");
    a.exec("INSERT INTO t (id, v) VALUES (3, 30)");
    a.refuses("COMMIT", Refused(Error), "Schema mismatch");
    assert!(!a.client.txn_active());
    assert_eq!(a.scan("t", &["id", "v"]), [[1, 10, 1]]);
}

/// A reply of any schema splits across frames, a TEXT column included: each
/// frame carries a heap compacted to its own rows, and the DML reads built on
/// one (DELETE's key read-back, UPDATE's row read-back) inherit that.
#[test]
fn a_text_table_past_one_frame_reads_back_whole() {
    const ROWS: u64 = 5_000;
    /// `v = id % GROUPS`, so any one `v` value selects `ROWS / GROUPS` rows.
    const GROUPS: u64 = 100;
    const PER_GROUP: usize = (ROWS / GROUPS) as usize;
    let mut db = Db::boot_with_env(4, &[("GNITZ_REPLY_FRAME_BUDGET", "16384")]);
    db.exec("CREATE TABLE t (id BIGINT UNSIGNED PRIMARY KEY, v BIGINT UNSIGNED NOT NULL, s TEXT NOT NULL)");
    let rel = db.rel("t");

    // Binary push, not `INSERT … VALUES`: the parse cost would dominate.
    let text = "x".repeat(800);
    let mut batch = ZSetBatch::new(&rel.schema);
    let mut app = gnitz_core::BatchAppender::new(&mut batch, &rel.schema);
    for id in 0..ROWS {
        app.add_row(id as u128, 1).u64_val(id % GROUPS).str_val(&text);
    }
    db.client
        .push(rel.tid, &rel.schema, &batch, gnitz_wire::WireConflictMode::Update)
        .unwrap();

    // Checked by content, not by count: each frame's rows are appended into one
    // batch, and the null words and German cells of a frame are indexed by its
    // own row numbers, so a wrong offset moves values without losing any.
    let (schema, batch) = db.read("SELECT * FROM t");
    assert_eq!(batch.len(), ROWS as usize);
    let (id_ci, v_ci) = (col_idx(&schema, "id"), col_idx(&schema, "v"));
    let s = SchemaFacts::locate(&*schema, col_idx(&schema, "s"));
    let mut seen = vec![false; ROWS as usize];
    for row in 0..batch.len() {
        let id = cell(&schema, &batch, id_ci, row) as u64;
        assert!(
            id < ROWS && !std::mem::replace(&mut seen[id as usize], true),
            "row {row}: id {id}"
        );
        assert_eq!(cell(&schema, &batch, v_ci, row) as u64, id % GROUPS, "row {row}");
        assert_eq!(
            gnitz_wire::german_string_content(s.bytes(&batch, row), &batch.blob),
            text.as_bytes(),
            "row {row}"
        );
        assert_eq!(batch.weights[row], 1, "row {row}");
    }

    let count = |db: &mut Db, pred: &str| db.rows(&format!("SELECT COUNT(*) AS n FROM t {pred}"), &["n"]);
    assert_eq!(db.affected("DELETE FROM t WHERE v = 7"), PER_GROUP);
    assert_eq!(count(&mut db, ""), [[(ROWS as usize - PER_GROUP) as i64, 1]]);
    assert_eq!(db.affected("UPDATE t SET v = 4242 WHERE v = 9"), PER_GROUP);
    assert_eq!(count(&mut db, "WHERE v = 4242"), [[PER_GROUP as i64, 1]]);
    assert_eq!(db.affected("UPDATE t SET v = v + 1"), ROWS as usize - PER_GROUP);
    assert_eq!(count(&mut db, "WHERE v = 4243"), [[PER_GROUP as i64, 1]]);
    assert_eq!(db.affected("DELETE FROM t"), ROWS as usize - PER_GROUP);
    assert_eq!(count(&mut db, ""), [[0, 1]]);
}

// ── ON CONFLICT / UPDATE / DELETE ────────────────────────────────────────────

#[test]
fn on_conflict_do_update_sets_a_double_from_excluded() {
    let mut db = Db::boot(2);
    db.exec(
        "CREATE TABLE t (id BIGINT PRIMARY KEY, f DOUBLE);
         INSERT INTO t VALUES (1, 1.5);
         INSERT INTO t VALUES (1, 2.25) ON CONFLICT (id) DO UPDATE SET f = EXCLUDED.f",
    );
    assert_eq!(db.rows("SELECT id FROM t WHERE f = 2.25", &["id"]), [[1, 1]]);
    assert_eq!(db.scan("t", &["id"]), [[1, 1]]);
}

/// Inside a transaction, ON CONFLICT resolves each VALUES row against a buffered
/// row, a buffered delete and a committed row. The VALUES rows run opposite to
/// the order the existing rows come back in, so each merge must read its own
/// incoming row.
#[test]
fn on_conflict_in_a_transaction_resolves_against_buffered_and_committed_rows() {
    let mut db = Db::boot(2);
    db.exec(
        "CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT, w BIGINT);
         CREATE TABLE u (id BIGINT PRIMARY KEY, v BIGINT);
         INSERT INTO t VALUES (1, 10, 1), (2, 20, 2), (3, 30, 5);
         INSERT INTO u VALUES (1, 10), (2, 20), (3, 30);
         BEGIN;
         INSERT INTO t VALUES (4, 40, 7);
         DELETE FROM t WHERE id = 2",
    );
    let sql = "INSERT INTO t VALUES (4, 400, 0), (3, 300, 0), (2, 200, 0) \
               ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v, w = w + 1";
    assert_eq!(db.affected(sql), 3);
    db.exec("INSERT INTO u VALUES (4, 40); DELETE FROM u WHERE id = 2");
    let sql = "INSERT INTO u VALUES (5, 50), (4, 400), (3, 300), (2, 200) ON CONFLICT (id) DO NOTHING";
    assert_eq!(db.affected(sql), 2);
    db.exec("COMMIT");

    assert_eq!(
        db.scan("t", &["id", "v", "w"]),
        at_weight_one(&[vec![1, 10, 1], vec![2, 200, 0], vec![3, 300, 6], vec![4, 400, 8]])
    );
    assert_eq!(
        db.scan("u", &["id", "v"]),
        at_weight_one(&[vec![1, 10], vec![2, 200], vec![3, 30], vec![4, 40], vec![5, 50]])
    );
}

/// A non-key WHERE inside a transaction matches committed and buffered rows
/// alike, each exactly once, and sees the transaction's own earlier updates.
#[test]
fn update_and_delete_in_a_transaction_match_committed_and_buffered_rows() {
    let mut db = Db::boot(2);
    db.exec(
        "CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT, s TEXT);
         INSERT INTO t VALUES (1, 1, 'a'), (2, 2, 'b'), (3, 3, 'c');
         BEGIN;
         INSERT INTO t VALUES (4, 4, 'd'), (5, 5, 'e')",
    );
    assert_eq!(db.affected("UPDATE t SET v = v + 10 WHERE v >= 3"), 3);
    assert_eq!(db.affected("DELETE FROM t WHERE v < 2 OR v = 14"), 2);
    assert_eq!(db.affected("UPDATE t SET s = 'z' WHERE v = 15"), 1);
    db.exec("COMMIT");

    assert_eq!(
        db.scan("t", &["id", "v"]),
        at_weight_one(&[vec![2, 2], vec![3, 13], vec![5, 15]])
    );
    for (s, id) in [("b", 2), ("c", 3), ("z", 5)] {
        let sql = format!("SELECT id FROM t WHERE s = '{s}'");
        assert_eq!(db.rows(&sql, &["id"]), [[id, 1]], "{sql}");
    }
}
