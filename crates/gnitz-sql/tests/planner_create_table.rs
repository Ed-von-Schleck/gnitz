#![cfg(feature = "integration")]

use gnitz_core::{GnitzClient, TypeCode};
use gnitz_sql::{GnitzSqlError, SqlPlanner, SqlResult};
use gnitz_test_harness::ServerHandle;

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

// ── PK type and nullability ──────────────────────────────────────────
//
// PK columns keep their declared native type (no implicit U64 coercion);
// the planner only forces NOT NULL because the PK region has no null
// bitmap. See commit 2cdf607.

#[test]
fn test_pk_int_keeps_native_type_and_not_nullable() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        p.execute("CREATE TABLE t1 (id INT PRIMARY KEY)").unwrap();
    }
    let s = schema_after_create(&mut client, &sn, "t1");
    let pk = &s.columns[s.pk_index_single()];
    assert_eq!(pk.type_code, TypeCode::I32);
    assert_eq!(pk.is_nullable, false);
}

#[test]
fn test_pk_smallint_keeps_native_type() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        p.execute("CREATE TABLE t2 (id SMALLINT PRIMARY KEY)").unwrap();
    }
    let s = schema_after_create(&mut client, &sn, "t2");
    assert_eq!(s.columns[s.pk_index_single()].type_code, TypeCode::I16);
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

// ── Compound PRIMARY KEY: admission rule ─────────────────────────────

#[test]
fn test_compound_pk_two_u64_accepted() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        p.execute(
            "CREATE TABLE cpk2 (a BIGINT UNSIGNED, b BIGINT UNSIGNED, payload BIGINT, PRIMARY KEY (a, b))"
        ).unwrap();
    }
    let s = schema_after_create(&mut client, &sn, "cpk2");
    assert_eq!(s.pk_count(), 2);
    assert_eq!(s.pk_stride(), 16);
    // PK columns are forced non-nullable.
    for &pi in s.pk_indices() {
        assert!(!s.columns[pi].is_nullable);
    }
}

#[test]
fn test_compound_pk_four_u32_accepted() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        p.execute(
            "CREATE TABLE cpk4 (a INT UNSIGNED, b INT UNSIGNED, c INT UNSIGNED, d INT UNSIGNED, \
             payload BIGINT, PRIMARY KEY (a, b, c, d))"
        ).unwrap();
    }
    let s = schema_after_create(&mut client, &sn, "cpk4");
    assert_eq!(s.pk_count(), 4);
    assert_eq!(s.pk_stride(), 16);
}

#[test]
fn test_compound_pk_resolve_preserves_order() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        // PRIMARY KEY (b, a) — non-positional order, with an unrelated payload
        // column first so the indices [2, 1] (not [0, 1]) are the only correct
        // decoding. Guards against the packed-as-usize regression in
        // resolve_table_id where a raw cast of the packed u64 produces a
        // single-element vec with `~2^63` as the index.
        p.execute(
            "CREATE TABLE cpk_order (payload BIGINT NOT NULL, a BIGINT UNSIGNED, b BIGINT UNSIGNED, \
             PRIMARY KEY (b, a))"
        ).unwrap();
    }
    // Re-resolve with a fresh client to ensure we exercise the wire decode
    // path (not whatever schema cache the planner may have populated).
    let mut fresh = GnitzClient::connect(&srv.sock_path).unwrap();
    let (_, s) = fresh.resolve_table_id(&sn, "cpk_order").unwrap();
    assert_eq!(s.pk_indices(), &[2, 1]);
}

#[test]
fn test_compound_pk_mixed_inline_and_table_level_rejected() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    let mut p = SqlPlanner::new(&mut client, &sn);
    let err = must_err(p.execute(
        "CREATE TABLE mixed_pk (a BIGINT UNSIGNED PRIMARY KEY, b BIGINT UNSIGNED, PRIMARY KEY (a, b))"
    ));
    match err {
        GnitzSqlError::Plan(s) => assert!(s.contains("Multiple PRIMARY KEY"), "got: {}", s),
        e => panic!("expected Plan, got {:?}", e),
    }
}

#[test]
fn test_compound_pk_duplicate_column_rejected() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    let mut p = SqlPlanner::new(&mut client, &sn);
    let err = must_err(p.execute(
        "CREATE TABLE dup_pk (a BIGINT UNSIGNED, b BIGINT UNSIGNED, PRIMARY KEY (a, a))"
    ));
    match err {
        GnitzSqlError::Plan(s) => assert!(s.contains("Duplicate"), "got: {}", s),
        e => panic!("expected Plan, got {:?}", e),
    }
}

#[test]
fn test_compound_pk_five_columns_rejected() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    let mut p = SqlPlanner::new(&mut client, &sn);
    let err = must_err(p.execute(
        "CREATE TABLE pk5 (a TINYINT UNSIGNED, b TINYINT UNSIGNED, c TINYINT UNSIGNED, \
         d TINYINT UNSIGNED, e TINYINT UNSIGNED, PRIMARY KEY (a, b, c, d, e))"
    ));
    match err {
        GnitzSqlError::Unsupported(s) => assert!(s.contains("at most 4"), "got: {}", s),
        e => panic!("expected Unsupported, got {:?}", e),
    }
}

#[test]
fn test_compound_pk_stride_12_accepted() {
    // (U64, U32) → stride 12. Narrow (≤ 16) but non-power-of-two: it widens to
    // a `u128` fast key via `widen_pk_le`'s zero-extend arm, so it is admitted.
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        p.execute(
            "CREATE TABLE pk_stride12 (a BIGINT UNSIGNED, b INT UNSIGNED, PRIMARY KEY (a, b))"
        ).unwrap();
    }
    let s = schema_after_create(&mut client, &sn, "pk_stride12");
    assert_eq!(s.pk_count(), 2);
    assert_eq!(s.pk_stride(), 12);
}

#[test]
fn test_compound_pk_stride_24_accepted() {
    // Three U64s → stride 24. Wide (> 16): routes through the byte-path
    // cursor/merge accessors. This is the first stride the narrow `u128` key
    // cannot hold, so it is the canonical wide-PK acceptance case.
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        p.execute(
            "CREATE TABLE pk_stride24 (a BIGINT UNSIGNED, b BIGINT UNSIGNED, c BIGINT UNSIGNED, \
             PRIMARY KEY (a, b, c))"
        ).unwrap();
    }
    let s = schema_after_create(&mut client, &sn, "pk_stride24");
    assert_eq!(s.pk_count(), 3);
    assert_eq!(s.pk_stride(), 24);
}

#[test]
fn test_compound_pk_stride_64_accepted() {
    // Four U128s → stride 64, the widest a 4-column PK can reach (the count cap
    // bounds it below MAX_PK_BYTES = 80). Wide byte-path, maximal width.
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        p.execute(
            "CREATE TABLE pk_stride64 (a UUID, b UUID, c UUID, d UUID, payload BIGINT, \
             PRIMARY KEY (a, b, c, d))"
        ).unwrap();
    }
    let s = schema_after_create(&mut client, &sn, "pk_stride64");
    assert_eq!(s.pk_count(), 4);
    assert_eq!(s.pk_stride(), 64);
}

#[test]
fn test_compound_pk_string_column_rejected() {
    // STRING PK columns are rejected before the stride check so the error
    // names the offending column.
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    let mut p = SqlPlanner::new(&mut client, &sn);
    let err = must_err(p.execute(
        "CREATE TABLE pk_str (a TEXT, b INT UNSIGNED, PRIMARY KEY (a, b))"
    ));
    match err {
        GnitzSqlError::Unsupported(s) => {
            assert!(s.contains("String"), "expected String/Blob hint, got: {}", s);
            assert!(s.contains("'a'"),
                "expected offending column name in error, got: {}", s);
        }
        e => panic!("expected Unsupported, got {:?}", e),
    }
}

#[test]
fn test_pk_float_column_rejected() {
    // Float PK columns are rejected: IEEE-754 floats break the byte-equal PK
    // contract (-0.0/+0.0, NaN), so neither F32 (REAL/FLOAT) nor F64 (DOUBLE)
    // may be a PK column.
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    let mut p = SqlPlanner::new(&mut client, &sn);
    let err = must_err(p.execute("CREATE TABLE pk_f32 (id REAL PRIMARY KEY)"));
    match err {
        GnitzSqlError::Unsupported(s) => {
            assert!(s.contains("float"), "expected float hint, got: {}", s);
            assert!(s.contains("'id'"),
                "expected offending column name in error, got: {}", s);
        }
        e => panic!("expected Unsupported, got {:?}", e),
    }
    let err = must_err(p.execute("CREATE TABLE pk_f64 (id DOUBLE PRIMARY KEY)"));
    assert!(matches!(err, GnitzSqlError::Unsupported(_)),
        "expected Unsupported for DOUBLE PK, got {:?}", err);
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
    // BIGINT maps to I64 and PKs now keep their declared native type.
    assert_eq!(s.columns[s.pk_index_single()].type_code, TypeCode::I64);
}

// ── FK column type matches parent PK ────────────────────────────────

#[test]
fn test_fk_column_type_matches_parent_native_pk() {
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
    // Parent's PK keeps its declared `INT` (I32); the child FK column is
    // rewritten to match that native type so byte widths line up.
    assert_eq!(fk_col.type_code, TypeCode::I32,
        "FK column must match the parent's native PK type");
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
    let src_tid = client.create_table(&sn, "src", &cols, &[0u32], true).unwrap();

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

// ── Compound-PK guards: view / index / FK / conflict-target ──────────

fn create_compound_pk_table(client: &mut GnitzClient, sn: &str, table: &str) {
    let mut p = SqlPlanner::new(client, sn);
    p.execute(&format!(
        "CREATE TABLE {table} (a BIGINT UNSIGNED, b BIGINT UNSIGNED, payload BIGINT, \
         PRIMARY KEY (a, b))"
    )).unwrap();
}

#[test]
fn test_view_over_compound_pk_simple_select_rejected() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    create_compound_pk_table(&mut client, &sn, "cv_src");
    let mut p = SqlPlanner::new(&mut client, &sn);
    let err = must_err(p.execute("CREATE VIEW cv_v AS SELECT * FROM cv_src"));
    match err {
        GnitzSqlError::Unsupported(s) => assert!(s.contains("compound"), "got: {}", s),
        e => panic!("expected Unsupported, got {:?}", e),
    }
}

#[test]
fn test_view_over_compound_pk_distinct_rejected() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    create_compound_pk_table(&mut client, &sn, "cv_src2");
    let mut p = SqlPlanner::new(&mut client, &sn);
    let err = must_err(p.execute("CREATE VIEW cv_d AS SELECT DISTINCT a, b FROM cv_src2"));
    match err {
        GnitzSqlError::Unsupported(s) => assert!(s.contains("compound"), "got: {}", s),
        e => panic!("expected Unsupported, got {:?}", e),
    }
}

#[test]
fn test_view_over_compound_pk_group_by_rejected() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    create_compound_pk_table(&mut client, &sn, "cv_src3");
    let mut p = SqlPlanner::new(&mut client, &sn);
    let err = must_err(p.execute(
        "CREATE VIEW cv_g AS SELECT a, COUNT(*) AS n FROM cv_src3 GROUP BY a"
    ));
    match err {
        GnitzSqlError::Unsupported(s) => assert!(s.contains("compound"), "got: {}", s),
        e => panic!("expected Unsupported, got {:?}", e),
    }
}

#[test]
fn test_view_over_compound_pk_union_rejected() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    create_compound_pk_table(&mut client, &sn, "cv_l");
    create_compound_pk_table(&mut client, &sn, "cv_r");
    let mut p = SqlPlanner::new(&mut client, &sn);
    let err = must_err(p.execute(
        "CREATE VIEW cv_u AS SELECT * FROM cv_l UNION ALL SELECT * FROM cv_r"
    ));
    match err {
        GnitzSqlError::Unsupported(s) => assert!(s.contains("compound"), "got: {}", s),
        e => panic!("expected Unsupported, got {:?}", e),
    }
}

#[test]
fn test_view_over_compound_pk_join_rejected() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    create_compound_pk_table(&mut client, &sn, "cv_jl");
    // The other side single-PK so the rejection is unambiguously about cv_jl.
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        p.execute("CREATE TABLE cv_jr (id BIGINT PRIMARY KEY, v BIGINT)").unwrap();
    }
    let mut p = SqlPlanner::new(&mut client, &sn);
    let err = must_err(p.execute(
        "CREATE VIEW cv_j AS SELECT cv_jl.a FROM cv_jl JOIN cv_jr ON cv_jl.a = cv_jr.id"
    ));
    match err {
        GnitzSqlError::Unsupported(s) => assert!(s.contains("compound"), "got: {}", s),
        e => panic!("expected Unsupported, got {:?}", e),
    }
}

#[test]
fn test_fk_references_compound_pk_rejected() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    create_compound_pk_table(&mut client, &sn, "fk_parent");
    let mut p = SqlPlanner::new(&mut client, &sn);
    let err = must_err(p.execute(
        "CREATE TABLE fk_child (cid BIGINT PRIMARY KEY, ref_a BIGINT UNSIGNED REFERENCES fk_parent(a))"
    ));
    // A compound-PK parent has no lone PK column, so a member column qualifies
    // as an FK target only via its own UNIQUE index; column 'a' has none.
    match err {
        GnitzSqlError::Unsupported(s) => assert!(s.contains("UNIQUE index"), "got: {}", s),
        e => panic!("expected Unsupported, got {:?}", e),
    }
}

#[test]
fn test_on_conflict_target_column_rejected_on_compound_pk() {
    // `ON CONFLICT (a) DO NOTHING` against PRIMARY KEY (a, b) hits the
    // validate_conflict_target guard before `pk_index_single()` is called.
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    create_compound_pk_table(&mut client, &sn, "conf_t");
    let mut p = SqlPlanner::new(&mut client, &sn);
    let err = must_err(p.execute(
        "INSERT INTO conf_t (a, b, payload) VALUES (1, 2, 3) ON CONFLICT (a) DO NOTHING"
    ));
    match err {
        GnitzSqlError::Unsupported(s) => assert!(s.contains("compound"), "got: {}", s),
        e => panic!("expected Unsupported, got {:?}", e),
    }
}

#[test]
fn test_on_conflict_no_target_accepted_on_compound_pk() {
    // The empty-target `ON CONFLICT DO NOTHING` / `DO UPDATE` arms don't
    // touch `pk_index_single()` — they route through compound-PK-aware
    // client merge paths.
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    create_compound_pk_table(&mut client, &sn, "conf_t2");
    let mut p = SqlPlanner::new(&mut client, &sn);
    p.execute(
        "INSERT INTO conf_t2 (a, b, payload) VALUES (1, 2, 3) ON CONFLICT DO NOTHING"
    ).unwrap();
}
