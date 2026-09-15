#![cfg(feature = "integration")]

//! DML contracts that need a server: the write targets a statement may name,
//! the INSERT arity and value guards, EXPLAIN inside a transaction, and the reply-frame
//! ceiling a DML read runs into.

mod common;
use common::*;
use gnitz_core::{BatchAppender, GnitzClient, ZSetBatch};

/// A VALUES row must carry exactly one value per column and only values the
/// writer evaluates (an unsupported one is echoed as the SQL written, not a
/// parser dump); a rejected INSERT writes nothing.
#[test]
fn rejected_inserts_write_nothing() {
    let (_srv, mut client, sn) = boot(1);
    exec(&mut client, &sn, "CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)");
    for (sql, variant, needle) in [
        ("INSERT INTO t VALUES (1)", "Bind", "expects 2 value(s)"),
        ("INSERT INTO t VALUES (1, 10, 99)", "Bind", "expects 2 value(s)"),
        (
            "INSERT INTO t VALUES (1, EXTRACT(YEAR FROM 2))",
            "Unsupported",
            "EXTRACT(YEAR FROM 2)",
        ),
    ] {
        assert_rejects_variant(&mut client, &sn, sql, variant, needle);
    }
    assert!(rows(&mut client, &sn, "SELECT * FROM t", &["id", "v"]).is_empty());
}

/// A view is read-only, and a reserved (`_`-prefixed) name is refused before
/// the catalog is probed — whether or not such a relation exists.
#[test]
fn writes_and_indexes_need_a_base_table() {
    let (_srv, mut client, sn) = boot(1);
    exec(&mut client, &sn, "CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)");
    exec(&mut client, &sn, "CREATE VIEW v AS SELECT * FROM t");
    for sql in [
        "INSERT INTO v VALUES (1, 2)",
        "UPDATE v SET v = 1 WHERE id = 1",
        "DELETE FROM v",
        "CREATE INDEX ix ON v (v)",
    ] {
        assert_rejects_variant(&mut client, &sn, sql, "Unsupported", "is a view");
    }
    for sql in [
        "INSERT INTO _seg999999 VALUES (1, 1)",
        "CREATE INDEX ix ON _seg999999 (v)",
    ] {
        assert_rejects_variant(&mut client, &sn, sql, "Plan", "cannot start with '_'");
    }
}

/// A write clause gnitz does not honour is named rather than dropped, as is a
/// write its target cannot take; none of them writes anything.
#[test]
fn a_refused_write_names_its_rule_and_writes_nothing() {
    let (_srv, mut client, sn) = boot(1);
    for sql in [
        "CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT NOT NULL, s TEXT)",
        "CREATE TABLE c (a BIGINT UNSIGNED, b BIGINT UNSIGNED, v BIGINT, PRIMARY KEY (a, b))",
        "CREATE TABLE st (id BIGINT PRIMARY KEY, v BIGINT) WITH (stream = true)",
        "INSERT INTO t VALUES (1, 10, 'a')",
    ] {
        exec(&mut client, &sn, sql);
    }
    for (sql, variant, needle) in [
        (
            "INSERT INTO t VALUES (2, 20, 'b') LIMIT 1",
            "Unsupported",
            "LIMIT/OFFSET",
        ),
        (
            "INSERT INTO t VALUES (2, 20, 'b') FOR UPDATE",
            "Unsupported",
            "FOR UPDATE",
        ),
        ("INSERT IGNORE INTO t VALUES (2, 20, 'b')", "Unsupported", "IGNORE"),
        ("REPLACE INTO t VALUES (2, 20, 'b')", "Unsupported", "REPLACE INTO"),
        // RETURNING projects plain source columns, a PK among them, and not
        // beside ON CONFLICT.
        (
            "INSERT INTO t VALUES (2, 20, 'b') RETURNING v + 1",
            "Unsupported",
            "RETURNING",
        ),
        (
            "INSERT INTO t VALUES (2, 20, 'b') RETURNING v",
            "Unsupported",
            "PRIMARY KEY",
        ),
        (
            "INSERT INTO t VALUES (2, 20, 'b') RETURNING * REPLACE (v + 1 AS v)",
            "Unsupported",
            "REPLACE",
        ),
        (
            "INSERT INTO t VALUES (2, 20, 'b') ON CONFLICT (id) DO NOTHING RETURNING id",
            "Unsupported",
            "RETURNING with ON CONFLICT",
        ),
        // A conflict target names exactly the primary key: not another column,
        // not more, not a prefix.
        (
            "INSERT INTO t VALUES (2, 20, 'b') ON CONFLICT (v) DO NOTHING",
            "Unsupported",
            "primary key",
        ),
        (
            "INSERT INTO t VALUES (2, 20, 'b') ON CONFLICT (id, v) DO NOTHING",
            "Unsupported",
            "primary key",
        ),
        (
            "INSERT INTO c VALUES (1, 1, 10) ON CONFLICT (a) DO NOTHING",
            "Unsupported",
            "primary key",
        ),
        (
            "INSERT INTO t VALUES (1, 20, 'b') ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v WHERE v > 5",
            "Unsupported",
            "DO UPDATE WHERE",
        ),
        (
            "INSERT INTO t VALUES (1, 20, 'b'), (1, 30, 'c') ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v",
            "Bind",
            "second time",
        ),
        (
            "UPDATE t SET v = 9 WHERE id = 1 RETURNING id",
            "Unsupported",
            "RETURNING",
        ),
        ("DELETE FROM t WHERE id = 1 RETURNING id", "Unsupported", "RETURNING"),
        ("DELETE FROM t LIMIT 1", "Unsupported", "LIMIT"),
        ("DELETE FROM t ORDER BY id", "Unsupported", "ORDER BY"),
        // The join forms are refused before any name resolves, so `other` need
        // not exist.
        (
            "UPDATE t SET v = o.v FROM other o WHERE t.id = o.id",
            "Unsupported",
            "join-update",
        ),
        (
            "DELETE FROM t USING other o WHERE t.id = o.id",
            "Unsupported",
            "join-delete",
        ),
        (
            "UPDATE t JOIN c ON t.id = c.a SET v = 1",
            "Unsupported",
            "exactly one simple FROM",
        ),
        ("UPDATE t SET id = 9 WHERE id = 1", "Unsupported", "primary key"),
        ("UPDATE t SET v = UPPER(s)", "Bind", "cannot assign a string value"),
        // A written alias displaces the table name, and a qualifier naming
        // anything else is not silently ignored.
        ("UPDATE t AS x SET v = 1 WHERE t.v = 1", "Bind", "not found"),
        ("DELETE FROM t WHERE nope.v = 1", "Bind", "not found"),
        // A stream has no stored row to mutate or to resolve a conflict against.
        ("UPDATE st SET v = 1 WHERE id = 1", "Unsupported", "is a stream"),
        ("DELETE FROM st WHERE id = 1", "Unsupported", "is a stream"),
        (
            "INSERT INTO st VALUES (1, 2) ON CONFLICT (id) DO NOTHING",
            "Unsupported",
            "is a stream",
        ),
    ] {
        assert_rejects_variant(&mut client, &sn, sql, variant, needle);
    }
    // The written alias is the one qualifier that answers.
    exec(&mut client, &sn, "UPDATE t AS x SET v = 11 WHERE x.v = 10");
    assert_eq!(
        rows(&mut client, &sn, "SELECT id, v FROM t", &["id", "v"]),
        at_weight_one(&[vec![1, 11]])
    );
}

/// EXPLAIN issues strictly less than the SELECT already allowed inside a
/// transaction, so it runs there and leaves the transaction open.
#[test]
fn explain_runs_inside_a_transaction() {
    let (_srv, mut client, sn) = boot(1);
    exec(&mut client, &sn, "CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)");
    exec(&mut client, &sn, "BEGIN");
    let (schema, batch) = read_sql(&mut client, &sn, "EXPLAIN SELECT v FROM t WHERE id = 5");
    assert_eq!(visible_names(&schema), ["plan"]);
    assert_eq!(batch.len(), 5);
    // Still open: COMMIT succeeds rather than raising "no transaction open".
    exec(&mut client, &sn, "COMMIT");
}

/// Transaction control is a client-side state machine: a COMMIT or ROLLBACK
/// with nothing open and a nested BEGIN are refused, each isolation, chaining
/// or savepoint clause names itself and opens or closes nothing, and a
/// statement that may not run inside a transaction is refused without closing
/// it.
#[test]
fn transaction_control_refuses_what_it_cannot_honour() {
    fn refuses(client: &mut GnitzClient, sn: &str, sql: &str, needle: &str) {
        let e = try_exec(client, sn, sql).expect_err(sql);
        assert!(variant_of(&e).1.contains(needle), "`{sql}`: {e:?}");
    }
    let (_srv, mut client, sn) = boot(1);
    exec(&mut client, &sn, "CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)");
    for (sql, needle) in [
        ("COMMIT", "no transaction open"),
        ("ROLLBACK", "no transaction open"),
        ("BEGIN READ ONLY", "transaction modes"),
        ("START TRANSACTION ISOLATION LEVEL SERIALIZABLE", "transaction modes"),
        ("BEGIN DEFERRED", "BEGIN modifier"),
        ("COMMIT AND CHAIN", "AND CHAIN"),
        ("ROLLBACK AND CHAIN", "AND CHAIN"),
        ("ROLLBACK TO SAVEPOINT sp", "TO SAVEPOINT"),
    ] {
        refuses(&mut client, &sn, sql, needle);
    }
    exec(&mut client, &sn, "BEGIN");
    exec(&mut client, &sn, "INSERT INTO t VALUES (1, 10)");
    for (sql, needle) in [
        ("BEGIN", "already open"),
        (
            "CREATE TABLE t2 (id BIGINT PRIMARY KEY)",
            "not allowed inside a transaction",
        ),
        ("CREATE VIEW v AS SELECT id FROM t", "not allowed inside a transaction"),
        ("CREATE INDEX ON t (v)", "not allowed inside a transaction"),
        ("DROP TABLE t", "not allowed inside a transaction"),
    ] {
        refuses(&mut client, &sn, sql, needle);
    }
    exec(&mut client, &sn, "COMMIT");
    assert_eq!(
        rows(&mut client, &sn, "SELECT * FROM t", &["id", "v"]),
        at_weight_one(&[vec![1, 10]])
    );
}

// ── Reply framing past one frame ─────────────────────────────────────────────
//
// A reply of any schema splits across frames, a TEXT column included: each frame
// carries a heap compacted to its own rows. So an unprojected read of a table
// far past the frame payload cap gets *slower*, never refused — and the DML
// reads built on one (DELETE's key read-back, UPDATE's row read-back) inherit
// that. A replicated table puts the whole table on every worker, so it crosses
// the cap at any worker count.
//
// What still bounds a whole-table UPDATE is the other direction: it writes the
// rewritten rows back as one push, and a push is one frame against the server's
// *ingress* cap. So its ceiling is the matched rows' size — a limit on the
// write, not on the read.

/// 100 000 × ≈854 B encodes to ≈85 MB — past the 64 MiB frame payload cap.
const ROWS: u64 = 100_000;
/// Wide enough that the row is dominated by its TEXT cell.
const TEXT_LEN: usize = 800;
/// A replicated write broadcasts, and 20 000-row chunks exhaust the SAL.
const CHUNK: u64 = 5_000;
/// `v = id % GROUPS`, so any one `v` value selects `ROWS / GROUPS` rows.
const GROUPS: u64 = 1_000;

/// The error a push past the server's ingress cap raises — the one ceiling a
/// whole-table UPDATE still meets.
const INGRESS_ERR: &str = "server ingress cap";

/// `SELECT COUNT(*) FROM t` — a fold sink, so it never materialises a row.
fn count_rows(client: &mut GnitzClient, sn: &str) -> i64 {
    let (schema, batch) = read_sql(client, sn, "SELECT COUNT(*) AS n FROM t");
    cell_i64(&schema, &batch, col_idx(&schema, "n"), 0)
}

#[test]
fn a_text_table_past_one_frame_reads_back_whole_where_a_whole_table_update_still_fails() {
    let (_srv, mut client, sn) = boot(4);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE t (id BIGINT UNSIGNED PRIMARY KEY, v BIGINT UNSIGNED NOT NULL, s TEXT NOT NULL) \
         WITH (replicated = true)",
    );
    let (tid, schema) = client.resolve_table_id(&sn, "t").unwrap();

    // Binary push, not `INSERT … VALUES`: the parse cost would dominate and this
    // test is about the read.
    let text = "x".repeat(TEXT_LEN);
    for start in (0..ROWS).step_by(CHUNK as usize) {
        let mut batch = ZSetBatch::new(&schema);
        let mut app = BatchAppender::new(&mut batch, &schema);
        for id in start..(start + CHUNK).min(ROWS) {
            app.add_row(id as u128, 1).u64_val(id % GROUPS).str_val(&text);
        }
        client.push(tid, &schema, &batch).unwrap();
    }

    // The unprojected read of this table spans many frames and reassembles whole
    // — checked by content, not by count: each frame's rows are appended into one
    // batch, and the null words and German cells of a frame are indexed by its
    // own row numbers, so a wrong offset moves values without losing any.
    let gnitz_core::ScanReply { schema: rschema, batch, .. } =
        client.scan(tid).expect("an 85 MB TEXT reply chunks like any other");
    assert_eq!(batch.len(), ROWS as usize);
    let (id_ci, v_ci, s_ci) = (col_idx(&rschema, "id"), col_idx(&rschema, "v"), col_idx(&rschema, "s"));
    let mut seen = vec![false; ROWS as usize];
    for (row, cell) in batch.payload[rschema.payload_idx(s_ci)]
        .bytes
        .as_chunks::<16>()
        .0
        .iter()
        .enumerate()
    {
        let id = cell_i64(&rschema, &batch, id_ci, row) as u64;
        assert!(
            id < ROWS && !std::mem::replace(&mut seen[id as usize], true),
            "row {row}: id {id}"
        );
        assert_eq!(cell_i64(&rschema, &batch, v_ci, row) as u64, id % GROUPS, "row {row}");
        assert_eq!(
            gnitz_wire::german_string_content(cell, &batch.blob),
            text.as_bytes(),
            "row {row}"
        );
        assert_eq!(batch.weights[row], 1, "row {row}");
    }

    // DELETE reads back keys: a predicate DELETE clears its matched group.
    let per_group = (ROWS / GROUPS) as usize;
    assert_eq!(affected(&mut client, &sn, "DELETE FROM t WHERE v = 7"), per_group);
    assert_eq!(count_rows(&mut client, &sn), (ROWS as usize - per_group) as i64);

    // UPDATE reads back whole rows, TEXT cell and all — selectively…
    assert_eq!(
        affected(&mut client, &sn, "UPDATE t SET v = 4242 WHERE v = 9"),
        per_group
    );

    // …while one matching the whole table still fails, now on the write-back
    // push rather than on the read that feeds it.
    assert_rejects_variant(&mut client, &sn, "UPDATE t SET v = 0", "Exec", INGRESS_ERR);

    // The no-WHERE DELETE reads every key and nothing else, and clears the table.
    assert_eq!(affected(&mut client, &sn, "DELETE FROM t"), ROWS as usize - per_group);
    assert_eq!(count_rows(&mut client, &sn), 0);
}

// ── UPDATE / DELETE / ON CONFLICT ────────────────────────────────────────────

/// A DECIMAL source rounds into an integer column and a DATE source converts
/// into a TIMESTAMP one, rather than writing the unscaled or wrong-unit register.
#[test]
fn update_set_converts_decimal_and_date_sources() {
    let (_srv, mut client, sn) = boot(2);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, i BIGINT, d DECIMAL(10,2), dt DATE, ts TIMESTAMP)",
    );
    exec(
        &mut client,
        &sn,
        "INSERT INTO t VALUES (1, 0, 2.50, DATE '2024-01-02', TIMESTAMP '2000-01-01 00:00:00')",
    );
    assert_eq!(affected(&mut client, &sn, "UPDATE t SET i = d, ts = dt"), 1);
    let got = rows(&mut client, &sn, "SELECT i, dt, ts FROM t", &["i", "dt", "ts"]);
    assert_eq!(got.len(), 1);
    let [i, dt, ts, w] = got[0][..] else { panic!("{got:?}") };
    assert_eq!((i, w), (3, 1));
    assert_eq!(ts, dt * 86_400_000_000);
}

#[test]
fn on_conflict_do_update_sets_a_double_from_excluded() {
    let (_srv, mut client, sn) = boot(2);
    exec(&mut client, &sn, "CREATE TABLE t (id BIGINT PRIMARY KEY, f DOUBLE)");
    exec(&mut client, &sn, "INSERT INTO t VALUES (1, 1.5)");
    exec(
        &mut client,
        &sn,
        "INSERT INTO t VALUES (1, 2.25) ON CONFLICT (id) DO UPDATE SET f = EXCLUDED.f",
    );
    let (schema, batch) = read_sql(&mut client, &sn, "SELECT id, f FROM t");
    assert_eq!(batch.len(), 1);
    assert_eq!(cell_f64(&schema, &batch, col_idx(&schema, "f"), 0), 2.25);
}

#[test]
fn update_rejects_a_qualified_target() {
    let (_srv, mut client, sn) = boot(1);
    exec(&mut client, &sn, "CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)");
    exec(&mut client, &sn, "INSERT INTO t VALUES (1, 1)");
    assert_rejects_variant(
        &mut client,
        &sn,
        "UPDATE t SET x.v = 1",
        "Plan",
        "column must be a simple identifier",
    );
    assert_eq!(rows(&mut client, &sn, "SELECT id, v FROM t", &["id", "v"]), [[1, 1, 1]]);
}

/// Inside a transaction, ON CONFLICT resolves each VALUES row against a buffered
/// row, a buffered delete and a committed row. The VALUES rows run opposite to
/// the order the existing rows come back in, so each merge must read its own
/// incoming row.
#[test]
fn on_conflict_in_a_transaction_resolves_against_buffered_and_committed_rows() {
    let (_srv, mut client, sn) = boot(2);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT, w BIGINT)",
    );
    exec(&mut client, &sn, "CREATE TABLE u (id BIGINT PRIMARY KEY, v BIGINT)");
    exec(
        &mut client,
        &sn,
        "INSERT INTO t VALUES (1, 10, 1), (2, 20, 2), (3, 30, 5)",
    );
    exec(&mut client, &sn, "INSERT INTO u VALUES (1, 10), (2, 20), (3, 30)");

    exec(&mut client, &sn, "BEGIN");
    exec(&mut client, &sn, "INSERT INTO t VALUES (4, 40, 7)");
    exec(&mut client, &sn, "DELETE FROM t WHERE id = 2");
    let sql = "INSERT INTO t VALUES (4, 400, 0), (3, 300, 0), (2, 200, 0) \
               ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v, w = w + 1";
    assert_eq!(affected(&mut client, &sn, sql), 3);
    exec(&mut client, &sn, "INSERT INTO u VALUES (4, 40)");
    exec(&mut client, &sn, "DELETE FROM u WHERE id = 2");
    let sql = "INSERT INTO u VALUES (5, 50), (4, 400), (3, 300), (2, 200) ON CONFLICT (id) DO NOTHING";
    assert_eq!(affected(&mut client, &sn, sql), 2);
    exec(&mut client, &sn, "COMMIT");

    assert_eq!(
        rows(&mut client, &sn, "SELECT * FROM t", &["id", "v", "w"]),
        at_weight_one(&[vec![1, 10, 1], vec![2, 200, 0], vec![3, 300, 6], vec![4, 400, 8]])
    );
    assert_eq!(
        rows(&mut client, &sn, "SELECT * FROM u", &["id", "v"]),
        at_weight_one(&[vec![1, 10], vec![2, 200], vec![3, 30], vec![4, 40], vec![5, 50]])
    );
}

/// A non-key WHERE inside a transaction matches committed and buffered rows
/// alike, each exactly once, and sees the transaction's own earlier updates.
#[test]
fn update_and_delete_in_a_transaction_match_committed_and_buffered_rows() {
    let (_srv, mut client, sn) = boot(2);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT, s TEXT)",
    );
    exec(
        &mut client,
        &sn,
        "INSERT INTO t VALUES (1, 1, 'a'), (2, 2, 'b'), (3, 3, 'c')",
    );

    exec(&mut client, &sn, "BEGIN");
    exec(&mut client, &sn, "INSERT INTO t VALUES (4, 4, 'd'), (5, 5, 'e')");
    assert_eq!(affected(&mut client, &sn, "UPDATE t SET v = v + 10 WHERE v >= 3"), 3);
    assert_eq!(affected(&mut client, &sn, "DELETE FROM t WHERE v < 2 OR v = 14"), 2);
    assert_eq!(affected(&mut client, &sn, "UPDATE t SET s = 'z' WHERE v = 15"), 1);
    exec(&mut client, &sn, "COMMIT");

    assert_eq!(
        rows(&mut client, &sn, "SELECT id, v FROM t", &["id", "v"]),
        at_weight_one(&[vec![2, 2], vec![3, 13], vec![5, 15]])
    );
    for (s, id) in [("b", 2), ("c", 3), ("z", 5)] {
        let sql = format!("SELECT id FROM t WHERE s = '{s}'");
        assert_eq!(rows(&mut client, &sn, &sql, &["id"]), [[id, 1]], "{sql}");
    }
}
