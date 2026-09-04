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

    // The unprojected read of this table spans many frames and reassembles whole.
    let (_, batch, _) = client.scan(tid).expect("an 85 MB TEXT reply chunks like any other");
    assert_eq!(batch.map(|b| b.len()).unwrap_or(0), ROWS as usize);

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
