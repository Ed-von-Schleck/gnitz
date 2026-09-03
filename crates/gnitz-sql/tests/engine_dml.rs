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

// ── The reply-frame ceiling ──────────────────────────────────────────────────
//
// A scan reply can only be split across frames when every column of its schema
// is a non-German-string type whose width is a multiple of 8; a TEXT column
// disqualifies the whole schema, so the worker single-frames it and errors past
// the frame cap. An unprojected DML read therefore **fails**, not merely gets
// slow, past a bounded number of rows in one partition. A replicated table
// reaches that bound at any worker count.
//
// DELETE reads back keys, and a PK-only reply schema is chunkable, so it clears
// the bound outright. UPDATE reads back rows, so its ceiling only *moves* — from
// the table's size to the matched rows' size.

/// 100 000 × ≈854 B encodes to ≈85 MB — past the 64 MiB frame payload cap.
const ROWS: u64 = 100_000;
/// Wide enough that the row is dominated by its TEXT cell.
const TEXT_LEN: usize = 800;
/// A replicated write broadcasts, and 20 000-row chunks exhaust the SAL.
const CHUNK: u64 = 5_000;
/// `v = id % GROUPS`, so any one `v` value selects `ROWS / GROUPS` rows.
const GROUPS: u64 = 1_000;

/// The error a non-chunkable reply past the frame cap raises: what separates
/// "the read failed" from "the read was slow".
const CHUNKING_ERR: &str = "cannot be chunked";

/// `SELECT COUNT(*) FROM t` — a fold sink, so it never materialises a row.
fn count_rows(client: &mut GnitzClient, sn: &str) -> i64 {
    let (schema, batch) = read_sql(client, sn, "SELECT COUNT(*) AS n FROM t");
    cell_i64(&schema, &batch, col_idx(&schema, "n"), 0)
}

#[test]
fn a_text_table_past_one_frame_deletes_by_predicate_where_a_whole_table_update_still_fails() {
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

    // The unprojected read of this table does not get slow — it fails.
    let msg = client.scan(tid).map(|_| ()).unwrap_err().to_string();
    assert!(msg.contains(CHUNKING_ERR), "the whole-table read must fail: {msg}");

    // DELETE reads back keys, which chunk: a predicate DELETE succeeds where that
    // scan cannot even be framed.
    let per_group = (ROWS / GROUPS) as usize;
    assert_eq!(affected(&mut client, &sn, "DELETE FROM t WHERE v = 7"), per_group);
    assert_eq!(count_rows(&mut client, &sn), (ROWS as usize - per_group) as i64);

    // UPDATE reads back rows, so its ceiling moved rather than lifted: a
    // selective UPDATE succeeds…
    assert_eq!(
        affected(&mut client, &sn, "UPDATE t SET v = 4242 WHERE v = 9"),
        per_group
    );

    // …while one matching the whole table still crosses the frame cap.
    assert_rejects_variant(&mut client, &sn, "UPDATE t SET v = 0", "Exec", CHUNKING_ERR);

    // The no-WHERE DELETE reads every key and nothing else, and clears the table.
    assert_eq!(affected(&mut client, &sn, "DELETE FROM t"), ROWS as usize - per_group);
    assert_eq!(count_rows(&mut client, &sn), 0);
}
