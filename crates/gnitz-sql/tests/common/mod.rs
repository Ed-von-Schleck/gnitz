#![allow(dead_code)]

//! Shared setup/assert helpers for the gnitz-sql integration tests.
//!
//! Every integration test forks a private `gnitz-server` via
//! `ServerHandle::start()`, so each test runs against an isolated, empty
//! server: schema names only need to be unique within one server. A single
//! shared `make_planner` therefore serves every test file.

use gnitz_core::{ColData, GnitzClient, Schema, ZSetBatch};
use gnitz_sql::{GnitzSqlError, SqlPlanner, SqlResult};
use gnitz_test_harness::ServerHandle;

/// Local replacement for `Result::unwrap_err` — `SqlResult` doesn't impl `Debug`,
/// so the stdlib version doesn't compile.
pub fn must_err(r: Result<Vec<SqlResult>, GnitzSqlError>) -> GnitzSqlError {
    match r {
        Ok(_) => panic!("expected error, got Ok"),
        Err(e) => e,
    }
}

/// Returns (client, schema_name) with a unique schema already created.
/// The schema name is unique per call so parallel tests don't collide.
pub fn make_planner(srv: &ServerHandle) -> (GnitzClient, String) {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let sn = format!("s{}", SEQ.fetch_add(1, Ordering::Relaxed));
    let mut client = GnitzClient::connect(&srv.sock_path).unwrap();
    client.create_schema(&sn).unwrap();
    (client, sn)
}

/// Plan and execute a statement, asserting it succeeds. (`SqlResult` has no
/// `Debug`, so the result Vec is simply unwrapped.)
pub fn exec(client: &mut GnitzClient, sn: &str, sql: &str) {
    let mut p = SqlPlanner::new(client, sn);
    p.execute(sql).unwrap();
}

/// Read `SELECT * FROM view` and return its (schema, batch).
pub fn read_view(client: &mut GnitzClient, sn: &str, view: &str) -> (Schema, ZSetBatch) {
    let mut p = SqlPlanner::new(client, sn);
    let mut res = p.execute(&format!("SELECT * FROM {}", view)).unwrap();
    match res.pop().unwrap() {
        SqlResult::Rows { schema, batch } => (schema, batch),
        _ => panic!("expected Rows"),
    }
}

pub fn col_idx(schema: &Schema, name: &str) -> usize {
    schema.columns.iter().position(|c| c.name.eq_ignore_ascii_case(name))
        .unwrap_or_else(|| panic!("column '{}' not in view schema {:?}",
            name, schema.columns.iter().map(|c| &c.name).collect::<Vec<_>>()))
}

pub fn i64_at(batch: &ZSetBatch, col: usize, row: usize) -> i64 {
    match &batch.columns[col] {
        ColData::Fixed(b) => i64::from_le_bytes(b[row*8..row*8+8].try_into().unwrap()),
        other => panic!("expected Fixed col, got {:?}", std::mem::discriminant(other)),
    }
}

/// Read a view's named (integer) payload columns into sorted row tuples, so a
/// test can compare incremental view contents against an expected full recompute
/// without decoding the OPK PK region by hand. Works for synthetic PK-region
/// columns too (e.g. join `_join_pk` / reduce group cols re-emitted as payload).
pub fn payload_rows(client: &mut GnitzClient, sn: &str, view: &str, cols: &[&str]) -> Vec<Vec<i64>> {
    let (schema, batch) = read_view(client, sn, view);
    let idxs: Vec<usize> = cols.iter().map(|c| col_idx(&schema, c)).collect();
    let mut rows: Vec<Vec<i64>> = (0..batch.len())
        .map(|r| idxs.iter().map(|&ci| i64_at(&batch, ci, r)).collect())
        .collect();
    rows.sort();
    rows
}

pub fn f64_at(batch: &ZSetBatch, col: usize, row: usize) -> f64 {
    match &batch.columns[col] {
        ColData::Fixed(b) => f64::from_le_bytes(b[row*8..row*8+8].try_into().unwrap()),
        other => panic!("expected Fixed col, got {:?}", std::mem::discriminant(other)),
    }
}

/// Is the payload column at schema index `col` NULL in `row`? The null word
/// is indexed by payload position (schema index minus the single PK column).
pub fn is_null_at(batch: &ZSetBatch, payload_idx: usize, row: usize) -> bool {
    (batch.nulls[row] >> payload_idx) & 1 != 0
}
