#![allow(dead_code)]

//! Shared setup/assert helpers for the gnitz-sql integration tests.
//!
//! Every integration test forks a private `gnitz-server` via
//! `ServerHandle::start()`, so each test runs against an isolated, empty
//! server: schema names only need to be unique within one server. A single
//! shared `make_planner` therefore serves every test file.

use gnitz_core::{ColData, GnitzClient, PkColumn, Schema, ZSetBatch};
use gnitz_sql::{GnitzSqlError, SqlPlanner, SqlResult};
use gnitz_test_harness::ServerHandle;
use gnitz_wire::{CIRCUIT_NODES_TAB, OPCODE_FILTER};

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

/// Plan and execute a statement, asserting it succeeds.
pub fn exec(client: &mut GnitzClient, sn: &str, sql: &str) {
    let mut p = SqlPlanner::new(client, sn);
    p.execute(sql).unwrap();
}

/// Plan and execute a statement, returning the planner result so a test can
/// assert it errors (`.is_err()`) or inspect the variant.
pub fn try_exec(client: &mut GnitzClient, sn: &str, sql: &str) -> Result<Vec<SqlResult>, GnitzSqlError> {
    let mut p = SqlPlanner::new(client, sn);
    p.execute(sql)
}

/// Execute a single statement expected to return `RowsAffected`, returning the
/// reported row count.
pub fn affected(client: &mut GnitzClient, sn: &str, sql: &str) -> usize {
    let mut p = SqlPlanner::new(client, sn);
    match p.execute(sql).unwrap().pop().unwrap() {
        SqlResult::RowsAffected { count } => count,
        _ => panic!("expected RowsAffected"),
    }
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
    schema
        .columns
        .iter()
        .position(|c| c.name.eq_ignore_ascii_case(name))
        .unwrap_or_else(|| {
            panic!(
                "column '{}' not in view schema {:?}",
                name,
                schema.columns.iter().map(|c| &c.name).collect::<Vec<_>>()
            )
        })
}

pub fn i64_at(batch: &ZSetBatch, col: usize, row: usize) -> i64 {
    match &batch.columns[col] {
        ColData::Fixed(b) => i64::from_le_bytes(b[row * 8..row * 8 + 8].try_into().unwrap()),
        other => panic!("expected Fixed col, got {:?}", std::mem::discriminant(other)),
    }
}

/// Net weight of `value` in integer column `col` of `view`: sum the weights of
/// every row whose `col` equals `value`. Set-op tests read the multiplicity of a
/// single projected value this way.
pub fn view_value_weight(client: &mut GnitzClient, sn: &str, view: &str, col: &str, value: i64) -> i64 {
    let (schema, batch) = read_view(client, sn, view);
    let ci = col_idx(&schema, col);
    (0..batch.len())
        .filter(|&r| i64_at(&batch, ci, r) == value)
        .map(|r| batch.weights[r])
        .sum()
}

/// Read a PK-region integer column's value (as i64) at `row`. The client decodes
/// the OPK PK region back to native little-endian on receive, so a fixed-width
/// integer PK column reads exactly like a payload column. `ci` must be a PK
/// column index. Single-column PKs arrive as the numeric `U64s`/`U128s` variants;
/// compound PKs arrive as `Bytes` (native-LE, tightly packed in pk-column order).
pub fn pk_i64_at(schema: &Schema, batch: &ZSetBatch, ci: usize, row: usize) -> i64 {
    match &batch.pks {
        PkColumn::U64s(v) => v[row] as i64,
        PkColumn::U128s(v) => v[row] as i64,
        PkColumn::Bytes { stride, buf } => {
            let off = row * *stride as usize + schema.pk_byte_offset(ci);
            i64::from_le_bytes(buf[off..off + 8].try_into().unwrap())
        }
    }
}

/// Read integer column `ci` of `row`, whichever region it lives in: a
/// PK-region column (a base table's own PK, a shape's natural PK — e.g. a
/// GROUP BY over the source PK — or a synthetic key like `_join_pk`) is
/// decoded from `batch.pks`; a payload column from `batch.columns`.
pub fn cell_i64(schema: &Schema, batch: &ZSetBatch, ci: usize, row: usize) -> i64 {
    if schema.is_pk_col(ci) {
        pk_i64_at(schema, batch, ci, row)
    } else {
        i64_at(batch, ci, row)
    }
}

/// Read a view's named (integer) columns into sorted row tuples, so a test can
/// compare incremental view contents against an expected full recompute without
/// decoding the OPK PK region by hand.
pub fn payload_rows(client: &mut GnitzClient, sn: &str, view: &str, cols: &[&str]) -> Vec<Vec<i64>> {
    let (schema, batch) = read_view(client, sn, view);
    let idxs: Vec<usize> = cols.iter().map(|c| col_idx(&schema, c)).collect();
    let mut rows: Vec<Vec<i64>> = (0..batch.len())
        .map(|r| idxs.iter().map(|&ci| cell_i64(&schema, &batch, ci, r)).collect())
        .collect();
    rows.sort();
    rows
}

pub fn f64_at(batch: &ZSetBatch, col: usize, row: usize) -> f64 {
    match &batch.columns[col] {
        ColData::Fixed(b) => f64::from_le_bytes(b[row * 8..row * 8 + 8].try_into().unwrap()),
        other => panic!("expected Fixed col, got {:?}", std::mem::discriminant(other)),
    }
}

/// Is the payload column at schema index `col` NULL in `row`? The null word
/// is indexed by payload position (schema index minus the single PK column).
pub fn is_null_at(batch: &ZSetBatch, payload_idx: usize, row: usize) -> bool {
    (batch.nulls[row] >> payload_idx) & 1 != 0
}

/// Scan the circuit `nodes` system table once, for reuse across several
/// `opcode_node_count` calls in one test (the table is invariant after the
/// circuits are built, so one scan + round-trip serves every count).
pub fn scan_circuit_nodes(client: &mut GnitzClient) -> Option<ZSetBatch> {
    client.scan(CIRCUIT_NODES_TAB).unwrap().1
}

/// Count rows belonging to view `vid` whose column-3 u64 equals `op`, in a
/// circuit-table batch — `nodes` (from `scan_circuit_nodes`, column 3 = opcode)
/// or `node_columns` (column 3 = kind). Both tables share the compound PK
/// (view_id, sub) packed LE into 16 bytes (view_id in the low 8); a scan returns
/// the full schema order, so the discriminator is column index 3 (Fixed u64-LE,
/// non-PK).
pub fn opcode_node_count(batch: Option<&ZSetBatch>, vid: u64, op: u64) -> usize {
    let batch = match batch {
        Some(b) => b,
        None => return 0,
    };
    let opcodes = match &batch.columns[3] {
        ColData::Fixed(buf) => buf,
        other => panic!("opcode column not Fixed: {other:?}"),
    };
    (0..batch.len())
        .filter(|&i| {
            let pk = match &batch.pks {
                PkColumn::Bytes { .. } => batch.pks.get_bytes(i),
                other => panic!("circuit nodes PK not wide bytes: {other:?}"),
            };
            u64::from_le_bytes(pk[0..8].try_into().unwrap()) == vid
                && u64::from_le_bytes(opcodes[i * 8..i * 8 + 8].try_into().unwrap()) == op
        })
        .count()
}

/// Count Filter nodes for `vid` (scans the nodes table). Thin wrapper over
/// `opcode_node_count` for the NULL-join-key plan-shape tests.
pub fn filter_node_count(client: &mut GnitzClient, vid: u64) -> usize {
    opcode_node_count(scan_circuit_nodes(client).as_ref(), vid, OPCODE_FILTER)
}

/// Count `vid`'s `NODE_COL_KIND_SCAN_BOUND` param rows — one per indexed column
/// of a pushed-down backfill-scan bound, so `0` means the view's `ScanDelta`
/// carries no bound. The `node_columns` table shares the circuit-table shape
/// `opcode_node_count` matches on: (view_id, sub) PK, u64 discriminator (here
/// `kind`) at column index 3.
pub fn scan_bound_col_count(client: &mut GnitzClient, vid: u64) -> usize {
    opcode_node_count(
        client.scan(gnitz_wire::CIRCUIT_NODE_COLUMNS_TAB).unwrap().1.as_ref(),
        vid,
        gnitz_wire::NODE_COL_KIND_SCAN_BOUND,
    )
}
