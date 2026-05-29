#![cfg(feature = "integration")]

use gnitz_core::{GnitzClient, ColData, Schema, ZSetBatch};
use gnitz_sql::{GnitzSqlError, SqlPlanner, SqlResult};
use gnitz_test_harness::ServerHandle;

fn must_err(r: Result<Vec<SqlResult>, GnitzSqlError>) -> GnitzSqlError {
    match r { Ok(_) => panic!("expected error, got Ok"), Err(e) => e }
}

fn make_planner(srv: &ServerHandle) -> (GnitzClient, String) {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let sn = format!("gb{}", SEQ.fetch_add(1, Ordering::Relaxed));
    let mut client = GnitzClient::connect(&srv.sock_path).unwrap();
    client.create_schema(&sn).unwrap();
    (client, sn)
}

/// Read `SELECT * FROM view` and return its (schema, batch).
fn read_view(client: &mut GnitzClient, sn: &str, view: &str) -> (Schema, ZSetBatch) {
    let mut p = SqlPlanner::new(client, sn);
    let mut res = p.execute(&format!("SELECT * FROM {}", view)).unwrap();
    match res.pop().unwrap() {
        SqlResult::Rows { schema, batch } => (schema, batch),
        other => panic!("expected Rows, got {:?}", matches!(other, SqlResult::Rows { .. })),
    }
}

fn col_idx(schema: &Schema, name: &str) -> usize {
    schema.columns.iter().position(|c| c.name.eq_ignore_ascii_case(name))
        .unwrap_or_else(|| panic!("column '{}' not in view schema {:?}",
            name, schema.columns.iter().map(|c| &c.name).collect::<Vec<_>>()))
}

fn i64_at(batch: &ZSetBatch, col: usize, row: usize) -> i64 {
    match &batch.columns[col] {
        ColData::Fixed(b) => i64::from_le_bytes(b[row*8..row*8+8].try_into().unwrap()),
        other => panic!("expected Fixed col, got {:?}", std::mem::discriminant(other)),
    }
}

fn f64_at(batch: &ZSetBatch, col: usize, row: usize) -> f64 {
    match &batch.columns[col] {
        ColData::Fixed(b) => f64::from_le_bytes(b[row*8..row*8+8].try_into().unwrap()),
        other => panic!("expected Fixed col, got {:?}", std::mem::discriminant(other)),
    }
}

/// Is the payload column at schema index `col` NULL in `row`? The null word
/// is indexed by payload position (schema index minus the single PK column).
fn is_null_at(batch: &ZSetBatch, payload_idx: usize, row: usize) -> bool {
    (batch.nulls[row] >> payload_idx) & 1 != 0
}

// ── item 42: AVG must ignore NULLs and never emit NaN/Infinity ───────
//
// The plan's literal scenario (a *zero-count* group reaching `float_div` and
// producing NaN) is unreachable: a group whose averaged column is entirely
// NULL is suppressed by the reduce and emits no row at all. AVG's divisor is
// COUNT(non-null); `EXPR_FLOAT_DIV` additionally null-marks any zero divisor.
// So no emitted AVG row is ever NaN. This guard pins that invariant.

#[test]
fn test_avg_ignores_nulls_no_nan() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        // signed g → synthetic-PK path so g is a readable payload column.
        p.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, g BIGINT NOT NULL, x DOUBLE)").unwrap();
        p.execute("CREATE VIEW v AS SELECT g, AVG(x) AS ax FROM t GROUP BY g").unwrap();
        // g=5: only NULL (zero-count group → suppressed, no row).
        // g=6: NULL + 3.0 → mean ignores NULL → 3.0.
        // g=7: 4.0 → 4.0.
        p.execute(
            "INSERT INTO t (id, g, x) VALUES (1, 5, NULL), (2, 6, NULL), (3, 6, 3.0), (4, 7, 4.0)"
        ).unwrap();
    }
    let (schema, batch) = read_view(&mut client, &sn, "v");
    let g_col  = col_idx(&schema, "g");
    let ax_col = col_idx(&schema, "ax");
    let ax_payload = ax_col - 1; // single synthetic U128 PK at index 0

    // The all-NULL group g=5 must not appear; no emitted average is NaN/Inf.
    for r in 0..batch.len() {
        let g = i64_at(&batch, g_col, r);
        assert_ne!(g, 5, "an all-NULL group must not be emitted");
        if !is_null_at(&batch, ax_payload, r) {
            let ax = f64_at(&batch, ax_col, r);
            assert!(ax.is_finite(), "AVG must never be NaN/Infinity, got {} for g={}", ax, g);
            match g {
                6 => assert!((ax - 3.0).abs() < 1e-9, "g=6 mean ignores NULL → 3.0, got {}", ax),
                7 => assert!((ax - 4.0).abs() < 1e-9, "g=7 → 4.0, got {}", ax),
                other => panic!("unexpected group {}", other),
            }
        }
    }
    assert_eq!(batch.len(), 2, "exactly groups g=6 and g=7 are emitted");
}

// ── item 13: numeric aggregate type rejection ────────────────────────

#[test]
fn test_sum_string_rejected() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    let mut p = SqlPlanner::new(&mut client, &sn);
    p.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, g BIGINT UNSIGNED NOT NULL, s TEXT)").unwrap();
    let err = must_err(p.execute("CREATE VIEW v AS SELECT g, SUM(s) AS x FROM t GROUP BY g"));
    assert!(matches!(err, GnitzSqlError::Bind(_)), "expected Bind, got {:?}", err);
}

#[test]
fn test_sum_avg_u128_rejected() {
    // SUM/AVG accumulate into a 64-bit slot; a U128 (DECIMAL(38,0)) source
    // overflows it and the engine's decode marks it unreachable, so the planner
    // must reject SUM/AVG over U128 at bind time (mirroring the MIN/MAX reject).
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    let mut p = SqlPlanner::new(&mut client, &sn);
    p.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, g BIGINT UNSIGNED NOT NULL, big DECIMAL(38,0) NOT NULL)").unwrap();
    for func in ["SUM", "AVG"] {
        let err = must_err(p.execute(&format!(
            "CREATE VIEW v AS SELECT g, {func}(big) AS x FROM t GROUP BY g"
        )));
        assert!(matches!(err, GnitzSqlError::Bind(_)),
            "expected Bind for {func}(u128), got {:?}", err);
    }
}

#[test]
fn test_min_uuid_rejected() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    let mut p = SqlPlanner::new(&mut client, &sn);
    p.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, g BIGINT UNSIGNED NOT NULL, u UUID)").unwrap();
    let err = must_err(p.execute("CREATE VIEW v AS SELECT g, MIN(u) AS x FROM t GROUP BY g"));
    // A clean rejection (not a server panic) — the binder may reject MIN(uuid)
    // as Unsupported before the planner's numeric/orderable guard is reached.
    assert!(matches!(err, GnitzSqlError::Bind(_) | GnitzSqlError::Unsupported(_)),
        "expected clean rejection, got {:?}", err);
}

// ── item 12 + 34: float SUM and AVG ──────────────────────────────────

#[test]
fn test_float_sum_and_avg() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        p.execute("CREATE TABLE m (id BIGINT PRIMARY KEY, g BIGINT UNSIGNED NOT NULL, x DOUBLE NOT NULL)").unwrap();
        p.execute("CREATE VIEW v AS SELECT g, SUM(x) AS sx, AVG(x) AS ax FROM m GROUP BY g").unwrap();
        p.execute("INSERT INTO m (id, g, x) VALUES (1, 7, 1.5), (2, 7, 2.5)").unwrap();
    }
    let (schema, batch) = read_view(&mut client, &sn, "v");
    assert_eq!(batch.len(), 1, "one group expected");
    let sx = f64_at(&batch, col_idx(&schema, "sx"), 0);
    let ax = f64_at(&batch, col_idx(&schema, "ax"), 0);
    assert!((sx - 4.0).abs() < 1e-9, "SUM(x) should be 4.0, got {}", sx);
    assert!((ax - 2.0).abs() < 1e-9, "AVG(x) should be 2.0, got {}", ax);
    // Output column types must be F64.
    assert_eq!(schema.columns[col_idx(&schema, "sx")].type_code, gnitz_core::TypeCode::F64);
    assert_eq!(schema.columns[col_idx(&schema, "ax")].type_code, gnitz_core::TypeCode::F64);
}

// ── item 11: HAVING binds to the correct aggregate ───────────────────

#[test]
fn test_having_binds_correct_aggregate() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        // signed g forces the synthetic-PK path so g is a readable payload column.
        p.execute("CREATE TABLE h (id BIGINT PRIMARY KEY, g BIGINT NOT NULL, c1 BIGINT NOT NULL, c2 BIGINT NOT NULL)").unwrap();
        // HAVING filters on MAX(c2), which appears AFTER SUM(c1) in the SELECT list.
        p.execute("CREATE VIEW v AS SELECT g, SUM(c1) AS s1, MAX(c2) AS m2 FROM h GROUP BY g HAVING MAX(c2) > 5").unwrap();
        // g=1: SUM(c1)=10, MAX(c2)=3 → MAX(c2) not > 5 → excluded.
        // g=2: SUM(c1)=1,  MAX(c2)=9 → MAX(c2) > 5 → included.
        // (If HAVING wrongly bound to SUM(c1), g=1 would survive and g=2 would not.)
        p.execute("INSERT INTO h (id, g, c1, c2) VALUES (1, 1, 10, 3), (2, 2, 1, 9)").unwrap();
    }
    let (schema, batch) = read_view(&mut client, &sn, "v");
    assert_eq!(batch.len(), 1, "exactly one group should satisfy HAVING MAX(c2) > 5");
    let m2 = i64_at(&batch, col_idx(&schema, "m2"), 0);
    let g  = i64_at(&batch, col_idx(&schema, "g"), 0);
    assert_eq!(m2, 9, "surviving group must be the one with MAX(c2)=9");
    assert_eq!(g, 2, "surviving group must be g=2");
}
