#![cfg(feature = "integration")]

//! The reply-frame ceiling a DML read runs into, and which half of it the
//! projected DELETE read removes.
//!
//! A scan reply can only be split across frames when every column of its schema
//! is a non-German-string type whose width is a multiple of 8; a TEXT column
//! disqualifies the whole schema, so the worker single-frames it and errors past
//! the frame cap. That makes an unprojected DML read **fail**, not merely get
//! slow, past a bounded number of rows *in one partition*.
//!
//! A `WITH (replicated = true)` table reaches that bound at any worker count —
//! every worker holds a full copy — so no key skew has to be crafted for the
//! property to hold at the default `GNITZ_WORKERS=4`.
//!
//! DELETE reads back keys, and a PK-only reply schema IS chunkable, so it clears
//! the bound outright. UPDATE reads back rows, so its ceiling only *moves* — from
//! the table's size to the matched rows' size. Both halves are pinned here.

mod common;
use common::*;
use gnitz_core::{BatchAppender, GnitzClient, ZSetBatch};
use gnitz_test_harness::ServerHandle;

/// 100 000 × ≈854 B encodes to ≈85 MB — past the 64 MiB frame payload cap.
const ROWS: u64 = 100_000;
/// Wide enough that the row is dominated by its TEXT cell.
const TEXT_LEN: usize = 800;
/// A replicated write broadcasts, and 20 000-row chunks exhaust the SAL.
const CHUNK: u64 = 5_000;
/// `v = id % GROUPS`, so any one `v` value selects `ROWS / GROUPS` rows.
const GROUPS: u64 = 1_000;

/// The error a non-chunkable reply past the frame cap raises. Pinned verbatim:
/// it is what distinguishes "the read failed" from "the read was slow".
const CHUNKING_ERR: &str = "chunking not yet implemented";

fn err_text<T: std::fmt::Debug>(r: Result<T, impl std::fmt::Debug>) -> String {
    match r {
        Ok(v) => panic!("expected the frame-cap failure, got {v:?}"),
        Err(e) => format!("{e:?}"),
    }
}

/// `SELECT COUNT(*) FROM t` — a fold sink, so it never materialises a row.
fn count_rows(client: &mut GnitzClient, sn: &str) -> i64 {
    let (schema, batch) = read_sql(client, sn, "SELECT COUNT(*) AS n FROM t");
    cell_i64(&schema, &batch, col_idx(&schema, "n"), 0)
}

#[test]
fn a_text_table_past_one_frame_deletes_by_predicate_where_a_whole_table_update_still_fails() {
    let Some(srv) = ServerHandle::start_n(4) else {
        return;
    };
    let (mut client, sn) = make_planner(&srv);
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

    // The unprojected read of this table does not get slow — it fails. This is
    // the read every DML statement used to take.
    let msg = err_text(client.scan(tid));
    assert!(msg.contains(CHUNKING_ERR), "the whole-table read must fail: {msg}");

    // DELETE reads back keys, which chunk: a predicate DELETE succeeds where that
    // scan cannot even be framed.
    let per_group = (ROWS / GROUPS) as usize;
    assert_eq!(
        affected(&mut client, &sn, "DELETE FROM t WHERE v = 7"),
        per_group,
        "the projected DELETE read serves a table the unprojected one cannot"
    );
    assert_eq!(count_rows(&mut client, &sn), (ROWS as usize - per_group) as i64);

    // UPDATE reads back rows, so its ceiling MOVED rather than lifted: a
    // selective UPDATE now succeeds…
    assert_eq!(
        affected(&mut client, &sn, "UPDATE t SET v = 4242 WHERE v = 9"),
        per_group,
        "a selective UPDATE now reads only the rows it matches"
    );

    // …while one matching the whole table still crosses the same frame in one
    // piece, and still fails. This is the ceiling the projection does not remove.
    let msg = err_text(try_exec(&mut client, &sn, "UPDATE t SET v = 0"));
    assert!(
        msg.contains(CHUNKING_ERR),
        "a whole-table UPDATE must still hit the frame cap: {msg}"
    );

    // The no-WHERE DELETE: the same statement shape, reading keys instead of
    // rows, clears the table.
    assert_eq!(
        affected(&mut client, &sn, "DELETE FROM t"),
        ROWS as usize - per_group,
        "a no-WHERE DELETE reads every key and nothing else"
    );
    assert_eq!(count_rows(&mut client, &sn), 0);
}
