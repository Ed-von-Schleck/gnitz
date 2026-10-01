//! Writes through the planner into the engine: the targets and clauses a write
//! may name, transaction control, ON CONFLICT and UPDATE/DELETE against buffered
//! and committed rows, and reads past one reply frame.

use super::*;
use gnitz_wire::WireStatus::Error;

/// A write the planner refuses reaches the caller as its rejection and writes
/// nothing, at each verb; the planner's own tests hold the rules.
#[test]
fn a_refused_write_writes_nothing() {
    let mut db = Db::boot(1);
    db.exec(
        "CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT NOT NULL, s TEXT);
         CREATE TABLE sr (id SERIAL PRIMARY KEY, v BIGINT NOT NULL);
         INSERT INTO t VALUES (1, 10, 'a')",
    );
    for (sql, needle) in [
        ("INSERT INTO t VALUES (2, 20, 'b'), (3, NULL, 'c')", "NOT NULL"),
        ("UPDATE t SET v = UPPER(s)", "cannot assign a string value"),
        ("DELETE FROM t LIMIT 1", "LIMIT"),
        ("INSERT INTO sr VALUES (1), (NULL)", "NOT NULL"),
    ] {
        db.refuses(sql, Rejected, needle);
    }
    assert_eq!(db.scan("t", &["id", "v"]), [[1, 10, 1]]);
    // The refused statement drew no SERIAL id.
    db.exec("INSERT INTO sr VALUES (7)");
    assert_eq!(db.scan("sr", &["id", "v"]), [[1, 7, 1]]);
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
    let mut app = gnitz_core::BatchAppender::new(&mut batch);
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

/// An ORDER BY key over a CTE names the CTE's column, whatever the source calls it.
#[test]
fn an_order_by_over_a_cte_names_the_cte_column() {
    let mut db = Db::boot(4);
    db.exec("CREATE TABLE t (id BIGINT PRIMARY KEY, w BIGINT NOT NULL)");
    db.insert("t", &["id", "w"], &[vec![1, 30], vec![2, 20], vec![3, 10]]);
    // x.id is t.w and x.w is t.id. The smallest x.id is 10, on the row whose x.w is 3.
    for sql in [
        "WITH x AS (SELECT w AS id, id AS w FROM t) SELECT w FROM x ORDER BY id LIMIT 1",
        "WITH x AS (SELECT w AS id, id AS w FROM t) SELECT * FROM x ORDER BY x.w DESC LIMIT 1",
    ] {
        assert_eq!(db.rows(sql, &["w"]), at_weight_one(&[vec![3]]), "`{sql}`");
    }
}

/// An ad-hoc read through CTEs returns what the flat query returns.
#[test]
fn a_read_through_ctes_matches_the_flat_query() {
    let mut db = Db::boot(4);
    db.exec("CREATE TABLE t (id BIGINT PRIMARY KEY, g BIGINT NOT NULL, v BIGINT)");
    let rows: Vec<Vec<i64>> = (1..=200).map(|i| vec![i, i % 7, (i * 37) % 101]).collect();
    db.insert("t", &["id", "g", "v"], &rows);
    for (cte, flat, cols) in [
        ("WITH x AS (SELECT g, COUNT(*) AS n, SUM(v) AS s FROM t GROUP BY g) SELECT g, n, s FROM x",
         "SELECT g, COUNT(*) AS n, SUM(v) AS s FROM t GROUP BY g", vec!["g", "n", "s"]),
        ("WITH x AS (SELECT g, SUM(v) AS s FROM t WHERE id > 30 GROUP BY g) SELECT g, s * 2 AS d FROM x WHERE s > 1150",
         "SELECT g, SUM(v) * 2 AS d FROM t WHERE id > 30 GROUP BY g HAVING SUM(v) > 1150", vec!["g", "d"]),
        ("WITH x AS (SELECT id, v + 1 AS q, g FROM t WHERE g < 5), y AS (SELECT q * 2 AS r, g FROM x WHERE id > 10) SELECT g, SUM(r) AS s FROM y GROUP BY g",
         "SELECT g, SUM((v + 1) * 2) AS s FROM t WHERE g < 5 AND id > 10 GROUP BY g", vec!["g", "s"]),
        ("WITH x AS (SELECT v AS id, id AS v FROM t) SELECT v FROM x ORDER BY id DESC, v LIMIT 5",
         "SELECT id AS v FROM t ORDER BY t.v DESC, t.id LIMIT 5", vec!["v"]),
        ("WITH x AS (SELECT 40 + 2 AS a) SELECT a + 1 AS b FROM x", "SELECT 43 AS b", vec!["b"]),
        ("WITH x AS (SELECT * FROM t) SELECT DISTINCT x.g FROM x WHERE x.v > 50", "SELECT DISTINCT g FROM t WHERE v > 50", vec!["g"]),
        ("WITH x AS (SELECT * FROM t) SELECT x.* FROM x WHERE id < 4", "SELECT * FROM t WHERE id < 4", vec!["id", "g", "v"]),
    ] {
        let (c, f) = (db.rows(cte, &cols), db.rows(flat, &cols));
        assert!(!f.is_empty(), "`{flat}` selects nothing");
        assert_eq!(c, f, "`{cte}`");
    }
}
