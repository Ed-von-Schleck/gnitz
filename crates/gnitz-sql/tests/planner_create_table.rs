#![cfg(feature = "integration")]

mod helpers;

use gnitz_core::{GnitzClient, TypeCode};
use gnitz_sql::{GnitzSqlError, SqlPlanner, SqlResult};
use helpers::ServerHandle;

/// Local replacement for `Result::unwrap_err` — `SqlResult` doesn't impl `Debug`,
/// so the stdlib version doesn't compile.
fn must_err(r: Result<Vec<SqlResult>, GnitzSqlError>) -> GnitzSqlError {
    match r {
        Ok(_)  => panic!("expected error, got Ok"),
        Err(e) => e,
    }
}

fn make_planner(srv: &ServerHandle) -> (GnitzClient, String) {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let sn = format!("ct{}", SEQ.fetch_add(1, Ordering::Relaxed));
    let mut client = GnitzClient::connect(&srv.sock_path).unwrap();
    client.create_schema(&sn).unwrap();
    (client, sn)
}

/// Resolve the table just created and return its server-side schema.
fn schema_after_create(client: &mut GnitzClient, sn: &str, table: &str) -> gnitz_core::Schema {
    let (_, schema) = client.resolve_table_id(sn, table).unwrap();
    schema
}

// ── PK type and nullability coercion ─────────────────────────────────

#[test]
fn test_pk_int_widened_to_u64_and_not_nullable() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        p.execute("CREATE TABLE t1 (id INT PRIMARY KEY)").unwrap();
    }
    let s = schema_after_create(&mut client, &sn, "t1");
    let pk = &s.columns[s.pk_index_single()];
    assert_eq!(pk.type_code, TypeCode::U64);
    assert_eq!(pk.is_nullable, false);
}

#[test]
fn test_pk_smallint_widened_to_u64() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        p.execute("CREATE TABLE t2 (id SMALLINT PRIMARY KEY)").unwrap();
    }
    let s = schema_after_create(&mut client, &sn, "t2");
    assert_eq!(s.columns[s.pk_index_single()].type_code, TypeCode::U64);
}

// ── Missing or malformed PK clause ───────────────────────────────────

#[test]
fn test_no_primary_key_errors() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    let mut p = SqlPlanner::new(&mut client, &sn);
    let err = must_err(p.execute("CREATE TABLE no_pk (id INT)"));
    match err {
        GnitzSqlError::Plan(s) => assert!(s.contains("PRIMARY KEY"), "got: {}", s),
        e => panic!("expected Plan, got {:?}", e),
    }
}

#[test]
fn test_empty_column_list_errors() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    let mut p = SqlPlanner::new(&mut client, &sn);
    let err = must_err(p.execute("CREATE TABLE empty_cols ()"));
    // Either a parse error from sqlparser, or a Plan error from the missing-PK
    // guard — both are acceptable; what matters is "no panic".
    match err {
        GnitzSqlError::Plan(_) | GnitzSqlError::Parse(_) => {}
        e => panic!("expected Plan or Parse, got {:?}", e),
    }
}

#[test]
fn test_table_level_pk_multi_column_unsupported() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    let mut p = SqlPlanner::new(&mut client, &sn);
    let err = must_err(p.execute(
        "CREATE TABLE pk_multi (a INT, b INT, PRIMARY KEY (a, b))"
    ));
    match err {
        GnitzSqlError::Unsupported(s) => assert!(s.contains("PRIMARY KEY"), "got: {}", s),
        e => panic!("expected Unsupported, got {:?}", e),
    }
}

#[test]
fn test_table_level_pk_typo_bind_error() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    let mut p = SqlPlanner::new(&mut client, &sn);
    // Table-level PK with unknown column must surface as a bind error,
    // even when an inline PRIMARY KEY exists on a real column.
    let err = must_err(p.execute(
        "CREATE TABLE pk_typo (id BIGINT PRIMARY KEY, PRIMARY KEY (typo))"
    ));
    match err {
        GnitzSqlError::Bind(s) => assert!(s.contains("typo"), "got: {}", s),
        e => panic!("expected Bind, got {:?}", e),
    }
}

#[test]
fn test_two_inline_primary_keys_errors() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    let mut p = SqlPlanner::new(&mut client, &sn);
    let err = must_err(p.execute(
        "CREATE TABLE two_pk (id BIGINT PRIMARY KEY, name TEXT PRIMARY KEY)"
    ));
    match err {
        GnitzSqlError::Plan(s) => assert!(s.contains("Multiple PRIMARY KEY"), "got: {}", s),
        e => panic!("expected Plan, got {:?}", e),
    }
}

#[test]
fn test_table_level_pk_uppercase_matches_lowercase() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        // Column declared as `id` but referenced as `ID` in PRIMARY KEY (...)
        p.execute("CREATE TABLE case_pk (id BIGINT, name TEXT, PRIMARY KEY (ID))").unwrap();
    }
    let s = schema_after_create(&mut client, &sn, "case_pk");
    assert_eq!(s.columns[s.pk_index_single()].name.to_lowercase(), "id");
    assert_eq!(s.columns[s.pk_index_single()].type_code, TypeCode::U64);
}

// ── FK column widening ───────────────────────────────────────────────

#[test]
fn test_fk_int_widens_to_parent_u64_pk() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        p.execute("CREATE TABLE parent (id INT PRIMARY KEY, name TEXT)").unwrap();
        p.execute(
            "CREATE TABLE child (cid INT PRIMARY KEY, p_id INT REFERENCES parent(id))"
        ).unwrap();
    }
    let s = schema_after_create(&mut client, &sn, "child");
    let fk_col = s.columns.iter().find(|c| c.name.eq_ignore_ascii_case("p_id")).unwrap();
    assert_eq!(fk_col.type_code, TypeCode::U64,
        "FK column must be widened to match the parent's coerced PK type");
    assert_ne!(fk_col.fk_table_id, 0, "FK reference should be wired");
}

// ── GROUP BY natural-PK coverage ─────────────────────────────────────

#[test]
fn test_group_by_nullable_u64_uses_synthetic_pk() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        // BIGINT UNSIGNED → U64 so the type test would otherwise pass — the
        // synthetic-PK fallback must come from the nullability check.
        p.execute(
            "CREATE TABLE evt (id BIGINT PRIMARY KEY, grp BIGINT UNSIGNED, val BIGINT NOT NULL)"
        ).unwrap();
        p.execute(
            "CREATE VIEW v_null_grp AS SELECT grp, COUNT(*) AS n FROM evt GROUP BY grp"
        ).unwrap();
    }
    let (_, s) = client.resolve_table_or_view_id(&sn, "v_null_grp").unwrap();
    // Synthetic-PK path: first column is the U128 _group_pk hash, not the
    // nullable U64 source group column.
    assert_eq!(s.columns[0].type_code, TypeCode::U128);
    assert_eq!(s.columns[0].is_nullable, false);
    // The non-PK group column carries the source nullability through.
    let grp = s.columns.iter().find(|c| c.name.eq_ignore_ascii_case("grp")).unwrap();
    assert_eq!(grp.is_nullable, true,
        "synthetic-PK group column must propagate source nullability");
}

#[test]
fn test_group_by_nonnullable_u64_uses_natural_pk() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        // BIGINT UNSIGNED → U64 (no PK coercion involved on this column).
        p.execute(
            "CREATE TABLE evt2 (id BIGINT PRIMARY KEY, grp BIGINT UNSIGNED NOT NULL, val BIGINT NOT NULL)"
        ).unwrap();
        p.execute(
            "CREATE VIEW v_nat_grp AS SELECT grp, COUNT(*) AS n FROM evt2 GROUP BY grp"
        ).unwrap();
    }
    let (_, s) = client.resolve_table_or_view_id(&sn, "v_nat_grp").unwrap();
    // Natural-PK path: first column is the (non-nullable) source group column.
    assert_eq!(s.columns[0].type_code, TypeCode::U64);
    assert_eq!(s.columns[0].is_nullable, false);
    assert!(s.columns[0].name.eq_ignore_ascii_case("grp"));
}

#[test]
fn test_group_by_uuid_uses_natural_pk() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        p.execute(
            "CREATE TABLE evt3 (id BIGINT PRIMARY KEY, grp UUID NOT NULL, val BIGINT NOT NULL)"
        ).unwrap();
        p.execute(
            "CREATE VIEW v_uuid_grp AS SELECT grp, COUNT(*) AS n FROM evt3 GROUP BY grp"
        ).unwrap();
    }
    let (_, s) = client.resolve_table_or_view_id(&sn, "v_uuid_grp").unwrap();
    // Natural-PK path: first column is UUID, not the synthetic U128 hash.
    assert_eq!(s.columns[0].type_code, TypeCode::UUID);
    assert!(s.columns[0].name.eq_ignore_ascii_case("grp"));
}

// ── View rejection: nullable first column ────────────────────────────

#[test]
fn test_create_view_with_nullable_first_column_rejected() {
    use gnitz_core::{ColumnDef, circuit::CircuitBuilder};
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);

    // Create a real source so we can build a circuit referring to a valid table id.
    let cols = vec![
        ColumnDef { name: "id".into(), type_code: TypeCode::U64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
        ColumnDef { name: "v".into(),  type_code: TypeCode::I64, is_nullable: true,  fk_table_id: 0, fk_col_idx: 0 },
    ];
    let src_tid = client.create_table(&sn, "src", &cols, 0, true).unwrap();

    // Manually construct a SCAN→SINK circuit and try to register a view whose
    // first output column is nullable.
    let vid = client.alloc_table_id().unwrap();
    let mut cb = CircuitBuilder::new(vid, src_tid);
    let scan = cb.input_delta();
    cb.sink(scan);
    let circuit = cb.build();

    let bad_out = vec![
        ColumnDef { name: "pk".into(), type_code: TypeCode::U64, is_nullable: true,  fk_table_id: 0, fk_col_idx: 0 },
        ColumnDef { name: "v".into(),  type_code: TypeCode::I64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 },
    ];
    let err = client.create_view_with_circuit(&sn, "bad_view", "", circuit, &bad_out)
        .unwrap_err();
    let msg = format!("{:?}", err);
    assert!(msg.to_lowercase().contains("nullable"), "expected nullable-PK error, got: {}", msg);
}
