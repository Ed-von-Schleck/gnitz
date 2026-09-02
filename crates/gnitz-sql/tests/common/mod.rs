#![allow(dead_code)]

//! Shared helpers for the gnitz-sql integration tests: a private server per
//! test, statements through the planner, and weighted row reads.
//!
//! Everything a test asserts about data goes through [`rows`], which carries
//! each row's Z-set weight: row presence alone reads a lost row and a
//! duplicated one as the same result.

use gnitz_core::{ColData, GnitzClient, Schema, ZSetBatch};
use gnitz_sql::{GnitzSqlError, SqlPlanner, SqlResult};
use gnitz_test_harness::ServerHandle;

/// A private `workers`-worker server, a client on it, and a fresh schema.
pub fn boot(workers: usize) -> (ServerHandle, GnitzClient, String) {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let srv = ServerHandle::start_n(workers);
    let sn = format!("s{}", SEQ.fetch_add(1, Ordering::Relaxed));
    let mut client = GnitzClient::connect(srv.sock_path()).unwrap();
    client.create_schema(&sn).unwrap();
    (srv, client, sn)
}

/// Execute `sql`, asserting it succeeds.
pub fn exec(client: &mut GnitzClient, sn: &str, sql: &str) {
    SqlPlanner::new(client, sn)
        .execute(sql)
        .unwrap_or_else(|e| panic!("`{sql}`: {e:?}"));
}

/// Execute `sql`, returning the planner's result.
pub fn try_exec(client: &mut GnitzClient, sn: &str, sql: &str) -> Result<Vec<SqlResult>, GnitzSqlError> {
    SqlPlanner::new(client, sn).execute(sql)
}

/// The error's variant name and message.
pub fn variant_of(e: &GnitzSqlError) -> (&'static str, String) {
    match e {
        GnitzSqlError::Parse(m) => ("Parse", m.to_string()),
        GnitzSqlError::Bind(m) => ("Bind", m.clone()),
        GnitzSqlError::Plan(m) => ("Plan", m.clone()),
        GnitzSqlError::Exec(m) => ("Exec", m.to_string()),
        GnitzSqlError::Unsupported(m) => ("Unsupported", m.clone()),
        other => ("other", format!("{other:?}")),
    }
}

/// Assert `sql` fails with `want_variant` whose message contains `want_msg` —
/// the guard's identity, not its wording.
pub fn assert_rejects_variant(client: &mut GnitzClient, sn: &str, sql: &str, want_variant: &str, want_msg: &str) {
    let e = try_exec(client, sn, sql)
        .err()
        .unwrap_or_else(|| panic!("`{sql}` was accepted"));
    let (variant, msg) = variant_of(&e);
    assert_eq!(variant, want_variant, "variant mismatch for `{sql}`: {e:?}");
    assert!(
        msg.contains(want_msg),
        "for `{sql}`\n  expected substring: {want_msg:?}\n  got: {msg:?}"
    );
}

/// Execute a statement expected to return `RowsAffected`.
pub fn affected(client: &mut GnitzClient, sn: &str, sql: &str) -> usize {
    match try_exec(client, sn, sql).unwrap().pop().unwrap() {
        SqlResult::RowsAffected { count } => count,
        other => panic!("expected RowsAffected from `{sql}`, got {other:?}"),
    }
}

/// Execute a row-returning statement and return its (schema, batch).
pub fn read_sql(client: &mut GnitzClient, sn: &str, sql: &str) -> (Schema, ZSetBatch) {
    match try_exec(client, sn, sql).unwrap().pop().unwrap() {
        SqlResult::Rows { schema, batch } => (schema, batch),
        other => panic!("expected Rows from `{sql}`, got {other:?}"),
    }
}

/// User-visible column names, lowercased.
pub fn visible_names(s: &Schema) -> Vec<String> {
    s.visible_columns().map(|(_, c)| c.name.to_lowercase()).collect()
}

pub fn col_idx(schema: &Schema, name: &str) -> usize {
    schema
        .columns
        .iter()
        .position(|c| c.name.eq_ignore_ascii_case(name))
        .unwrap_or_else(|| {
            let names: Vec<&str> = schema.columns.iter().map(|c| c.name.as_str()).collect();
            panic!("column '{name}' not in {names:?}")
        })
}

/// Integer column `ci` of `row`, at its own width and signedness, from
/// whichever region it lives in. The client decodes the PK region back to
/// native little-endian on receive, so both regions read alike.
pub fn cell_i64(schema: &Schema, batch: &ZSetBatch, ci: usize, row: usize) -> i64 {
    let tc = schema.columns[ci].type_code;
    let width = tc.wire_stride();
    let bytes = if schema.is_pk_col(ci) {
        batch.pks.col_window(row, schema.pk_byte_offset(ci), width)
    } else {
        match &batch.columns[ci] {
            ColData::Fixed(b) => &b[row * width..(row + 1) * width],
            other => panic!("column {ci} is not fixed-width: {other:?}"),
        }
    };
    if gnitz_wire::is_signed_int(tc as u8) {
        gnitz_wire::read_signed_exact(bytes)
    } else {
        gnitz_wire::read_unsigned_exact(bytes) as i64
    }
}

pub fn cell_f64(batch: &ZSetBatch, ci: usize, row: usize) -> f64 {
    match &batch.columns[ci] {
        ColData::Fixed(b) => f64::from_le_bytes(b[row * 8..row * 8 + 8].try_into().unwrap()),
        other => panic!("column {ci} is not fixed-width: {other:?}"),
    }
}

/// Is payload column `ci` NULL in `row`?
pub fn is_null_at(schema: &Schema, batch: &ZSetBatch, ci: usize, row: usize) -> bool {
    gnitz_core::null_word_get(batch.nulls[row], schema.payload_idx(ci))
}

/// The named integer columns of every row `sql` returns, each row with its
/// Z-set weight appended, sorted.
pub fn rows(client: &mut GnitzClient, sn: &str, sql: &str, cols: &[&str]) -> Vec<Vec<i64>> {
    let (schema, batch) = read_sql(client, sn, sql);
    let idxs: Vec<usize> = cols.iter().map(|c| col_idx(&schema, c)).collect();
    let mut out: Vec<Vec<i64>> = (0..batch.len())
        .map(|r| {
            let mut row: Vec<i64> = idxs.iter().map(|&ci| cell_i64(&schema, &batch, ci, r)).collect();
            row.push(batch.weights[r]);
            row
        })
        .collect();
    out.sort();
    out
}

/// [`rows`] of `SELECT * FROM view`.
pub fn view_rows(client: &mut GnitzClient, sn: &str, view: &str, cols: &[&str]) -> Vec<Vec<i64>> {
    rows(client, sn, &format!("SELECT * FROM {view}"), cols)
}

/// The expected side of a [`rows`] compare when every row is present exactly
/// once: `rows` with a trailing `1`, sorted.
pub fn at_weight_one(rows: &[Vec<i64>]) -> Vec<Vec<i64>> {
    let mut out: Vec<Vec<i64>> = rows.iter().map(|r| r.iter().copied().chain([1]).collect()).collect();
    out.sort();
    out
}

/// `INSERT INTO table (cols) VALUES …` for integer row tuples.
pub fn insert_rows(client: &mut GnitzClient, sn: &str, table: &str, cols: &[&str], rows: &[Vec<i64>]) {
    let vals: Vec<String> = rows
        .iter()
        .map(|r| {
            let cells: Vec<String> = r.iter().map(|v| v.to_string()).collect();
            format!("({})", cells.join(", "))
        })
        .collect();
    exec(
        client,
        sn,
        &format!("INSERT INTO {table} ({}) VALUES {}", cols.join(", "), vals.join(", ")),
    );
}
