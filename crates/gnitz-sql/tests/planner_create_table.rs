#![cfg(feature = "integration")]

use gnitz_core::{GnitzClient, TypeCode};
use gnitz_sql::{GnitzSqlError, SqlPlanner};
use gnitz_test_harness::ServerHandle;

mod common;
use common::*;

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
    assert!(!pk.is_nullable);
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
    assert!(!s.columns[0].is_nullable);
    // The non-PK group column carries the source nullability through.
    let grp = s.columns.iter().find(|c| c.name.eq_ignore_ascii_case("grp")).unwrap();
    assert!(grp.is_nullable,
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
    assert!(!s.columns[0].is_nullable);
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
    let err = client.create_view_with_circuit(&sn, "bad_view", "", circuit, &bad_out, &[0])
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
fn test_view_over_compound_pk_simple_select_accepted() {
    // A plain projection passes the full source PK through to the leading output
    // slots; the view carries the source's compound PK. (GROUP BY and equijoin
    // over a compound-PK source are accepted too — see the tests further down;
    // a join view's output PK is the synthetic _join_pk, not the source PK.)
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    create_compound_pk_table(&mut client, &sn, "cv_src");
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        p.execute("CREATE VIEW cv_v AS SELECT * FROM cv_src").unwrap();
    }
    let (_, s) = client.resolve_table_or_view_id(&sn, "cv_v").unwrap();
    assert_eq!(s.pk_indices(), &[0, 1], "view inherits the compound source PK");
}

#[test]
fn test_view_over_compound_pk_group_by_accepted() {
    // GROUP BY over a compound-PK source is now supported. Grouping by the full
    // PK (`group_set_eq_pk`) carries the compound PK through to the view; grouping
    // by a single component takes the single-natural-PK path and registers `[0]`.
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    create_compound_pk_table(&mut client, &sn, "cv_src3");
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        // Full-PK grouping → compound natural PK `[0, 1]`.
        p.execute("CREATE VIEW cv_gfull AS SELECT a, b, COUNT(*) AS n \
                   FROM cv_src3 GROUP BY a, b").unwrap();
        // Single-component grouping → single natural PK `[0]`.
        p.execute("CREATE VIEW cv_gone AS SELECT a, COUNT(*) AS n \
                   FROM cv_src3 GROUP BY a").unwrap();
    }
    let (_, sf) = client.resolve_table_or_view_id(&sn, "cv_gfull").unwrap();
    assert_eq!(sf.pk_indices(), &[0, 1], "GROUP BY full compound PK carries it through");
    let (_, so) = client.resolve_table_or_view_id(&sn, "cv_gone").unwrap();
    assert_eq!(so.pk_indices(), &[0], "GROUP BY one PK component → single natural PK");
}

#[test]
fn test_view_over_compound_pk_too_wide_rejected() {
    // The uniform client guard in write_circuit_rows must reject a view whose
    // output column count exceeds MAX_COLUMNS (65) cleanly, never tripping the
    // engine's build_schema_from_col_defs assert. A compound-PK source plus a
    // re-selection of both PK columns as extra payload pushes SELECT * (64 cols)
    // to 66 output columns.
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    // 2 PK columns + 62 payload = 64 source columns.
    let payload = (0..62).map(|i| format!("c{i} BIGINT NOT NULL")).collect::<Vec<_>>().join(", ");
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        p.execute(&format!(
            "CREATE TABLE wide (a BIGINT UNSIGNED, b BIGINT UNSIGNED, {payload}, PRIMARY KEY (a, b))"
        )).unwrap();
    }
    let mut p = SqlPlanner::new(&mut client, &sn);
    // SELECT * (64) + the two PK columns re-emitted under new names = 66 > 65.
    let err = must_err(p.execute(
        "CREATE VIEW wv AS SELECT *, a AS da, b AS db FROM wide"));
    match err {
        GnitzSqlError::Exec(e) => {
            let s = format!("{:?}", e);
            assert!(s.contains("output columns") && s.contains("limit"),
                    "expected the column-limit guard message, got: {}", s);
        }
        e => panic!("expected Exec (client column-limit guard), got {:?}", e),
    }
    assert!(client.resolve_table_or_view_id(&sn, "wv").is_err(), "no view should be registered");
}

#[test]
fn test_view_over_compound_pk_join_source_accepted() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    create_compound_pk_table(&mut client, &sn, "cv_jl");
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        // Same-type (U64) key so the equijoin is valid. The compound source PK
        // rides through the reindex as payload; the view's output PK is the lone
        // synthetic _join_pk, independent of the source PK's arity.
        p.execute("CREATE TABLE cv_jr (id BIGINT UNSIGNED PRIMARY KEY, v BIGINT)").unwrap();
        p.execute(
            "CREATE VIEW cv_j AS SELECT cv_jl.a, cv_jr.v FROM cv_jl JOIN cv_jr ON cv_jl.a = cv_jr.id"
        ).unwrap();
    }
    let (_, s) = client.resolve_table_or_view_id(&sn, "cv_j").unwrap();
    assert_eq!(s.pk_indices(), &[0],
        "join view PK is the lone synthetic _join_pk, not the source compound PK");
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

// ── Column-level / table-level UNIQUE constraints ────────────────────

/// Index uniqueness for `table.col`, looked up via the same `index_for_column`
/// path the FK admission gate uses. `None` = no index on the column.
fn unique_on(client: &mut GnitzClient, sn: &str, table: &str, col: &str) -> Option<bool> {
    let (tid, schema) = client.resolve_table_id(sn, table).unwrap();
    let ci = schema.columns.iter().position(|c| c.name.eq_ignore_ascii_case(col)).unwrap();
    client.index_for_column(tid, ci).unwrap().map(|m| m.is_unique)
}

#[test]
fn test_column_level_unique_creates_index() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        p.execute("CREATE TABLE u (id BIGINT PRIMARY KEY, val BIGINT UNIQUE)").unwrap();
    }
    assert_eq!(unique_on(&mut client, &sn, "u", "val"), Some(true),
        "column-level UNIQUE must create a unique secondary index on val");
}

#[test]
fn test_table_level_unique_single_column() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        p.execute("CREATE TABLE ut (id BIGINT PRIMARY KEY, a BIGINT, UNIQUE(a))").unwrap();
    }
    assert_eq!(unique_on(&mut client, &sn, "ut", "a"), Some(true));
}

#[test]
fn test_named_unique_constraint_droppable_by_name() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        p.execute("CREATE TABLE un (id BIGINT PRIMARY KEY, a BIGINT, CONSTRAINT u_a UNIQUE(a))").unwrap();
    }
    assert_eq!(unique_on(&mut client, &sn, "un", "a"), Some(true));
    {
        // The constraint name is the index name, so DROP INDEX u_a removes it.
        let mut p = SqlPlanner::new(&mut client, &sn);
        p.execute("DROP INDEX u_a").unwrap();
    }
    assert_eq!(unique_on(&mut client, &sn, "un", "a"), None,
        "dropping the named constraint removes the uniqueness index");
}

#[test]
fn test_multi_column_unique_rejected() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    let mut p = SqlPlanner::new(&mut client, &sn);
    let err = must_err(p.execute(
        "CREATE TABLE um (id BIGINT PRIMARY KEY, a BIGINT, b BIGINT, UNIQUE(a, b))"));
    match err {
        GnitzSqlError::Unsupported(s) => assert!(s.contains("multi-column UNIQUE"), "got: {}", s),
        e => panic!("expected Unsupported, got {:?}", e),
    }
}

#[test]
fn test_duplicate_unique_constraint_rejected() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    let mut p = SqlPlanner::new(&mut client, &sn);
    let err = must_err(p.execute(
        "CREATE TABLE ud (id BIGINT PRIMARY KEY, a BIGINT, UNIQUE(a), UNIQUE(a))"));
    match err {
        GnitzSqlError::Plan(s) => assert!(s.contains("duplicate UNIQUE"), "got: {}", s),
        e => panic!("expected Plan, got {:?}", e),
    }
}

#[test]
fn test_unique_text_column_rejected_no_orphan() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    let mut p = SqlPlanner::new(&mut client, &sn);
    let err = must_err(p.execute(
        "CREATE TABLE ux (id BIGINT PRIMARY KEY, name TEXT UNIQUE)"));
    match err {
        GnitzSqlError::Unsupported(s) => assert!(s.contains("'name'"), "got: {}", s),
        e => panic!("expected Unsupported, got {:?}", e),
    }
    // Pre-validation runs before create_table, so no orphan table is left.
    assert!(client.resolve_table_id(&sn, "ux").is_err(),
        "type rejection must happen before the table is created");
}

#[test]
fn test_lone_pk_unique_no_secondary_index() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        p.execute("CREATE TABLE ul (id BIGINT PRIMARY KEY UNIQUE, v BIGINT)").unwrap();
    }
    // The lone PK is already unique; the redundant secondary index is dropped.
    assert_eq!(unique_on(&mut client, &sn, "ul", "id"), None);
}

#[test]
fn test_compound_pk_member_unique_creates_index() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        p.execute(
            "CREATE TABLE uc (a BIGINT UNSIGNED, b BIGINT UNSIGNED, PRIMARY KEY(a, b), UNIQUE(a))"
        ).unwrap();
    }
    // A compound-PK member is not individually unique, so UNIQUE(a) is kept.
    assert_eq!(unique_on(&mut client, &sn, "uc", "a"), Some(true));
}

#[test]
fn test_unique_plus_fk_same_column() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        p.execute("CREATE TABLE par (x BIGINT PRIMARY KEY)").unwrap();
        // refc is both an FK (non-unique auto-index, registered first) and UNIQUE.
        // The unique index must promote the FK circuit, not be deduped away.
        p.execute(
            "CREATE TABLE chl (id BIGINT PRIMARY KEY, refc BIGINT UNIQUE REFERENCES par(x))"
        ).unwrap();
    }
    assert_eq!(unique_on(&mut client, &sn, "chl", "refc"), Some(true),
        "the UNIQUE index must promote the incumbent FK circuit to unique");
}

#[test]
fn test_self_ref_fk_before_inline_pk() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        // The FK column is declared before the inline PK. Phase 3a populates
        // pk_indices before Phase 3b resolves the self-referencing FK, so this
        // no longer fails spuriously.
        p.execute("CREATE TABLE sref (refc BIGINT REFERENCES sref(id), id BIGINT PRIMARY KEY)").unwrap();
    }
    let s = schema_after_create(&mut client, &sn, "sref");
    assert!(s.columns[s.pk_index_single()].name.eq_ignore_ascii_case("id"),
        "id must be the lone PK");
}

#[test]
fn test_reserved_fk_infix_constraint_name_rejected() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    let mut p = SqlPlanner::new(&mut client, &sn);
    let err = must_err(p.execute(
        "CREATE TABLE ur (id BIGINT PRIMARY KEY, a BIGINT, CONSTRAINT my__fk_thing UNIQUE(a))"));
    match err {
        GnitzSqlError::Plan(s) => assert!(s.contains("reserved '__fk_' infix"), "got: {}", s),
        e => panic!("expected Plan, got {:?}", e),
    }
}

#[test]
fn test_invalid_identifier_constraint_name_rejected() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    let mut p = SqlPlanner::new(&mut client, &sn);
    let err = must_err(p.execute(
        "CREATE TABLE ui (id BIGINT PRIMARY KEY, a BIGINT, CONSTRAINT _my_idx UNIQUE(a))"));
    match err {
        GnitzSqlError::Plan(s) => assert!(s.contains("cannot start with '_'"), "got: {}", s),
        e => panic!("expected Plan, got {:?}", e),
    }
}

#[test]
fn test_drop_index_permitted_on_lone_pk_when_fk_referenced() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        p.execute("CREATE TABLE pidx (id BIGINT PRIMARY KEY)").unwrap();
        p.execute("CREATE TABLE cidx (cid BIGINT PRIMARY KEY, p BIGINT REFERENCES pidx(id))").unwrap();
        // A redundant unique index on the lone PK column of the FK target.
        p.execute("CREATE UNIQUE INDEX pidx_uq ON pidx(id)").unwrap();
        // The lone PK guarantees uniqueness even without the secondary index,
        // so the drop is permitted despite the FK reference.
        p.execute("DROP INDEX pidx_uq").unwrap();
    }
}

// ── CREATE INDEX naming ──────────────────────────────────────────────

#[test]
fn test_create_index_named_droppable_by_name() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        p.execute("CREATE TABLE ni (id BIGINT PRIMARY KEY, col BIGINT)").unwrap();
        p.execute("CREATE INDEX my_idx ON ni(col)").unwrap();
    }
    {
        // The auto-generated name does not exist; only the user name resolves.
        let mut p = SqlPlanner::new(&mut client, &sn);
        let err = must_err(p.execute(&format!("DROP INDEX {sn}__ni__idx_col")));
        assert!(matches!(err, GnitzSqlError::Exec(_)), "auto-name must not resolve, got {:?}", err);
        p.execute("DROP INDEX my_idx").unwrap();
    }
}

#[test]
fn test_create_index_unnamed_droppable_by_autoname() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        p.execute("CREATE TABLE ai (id BIGINT PRIMARY KEY, col BIGINT)").unwrap();
        p.execute("CREATE INDEX ON ai(col)").unwrap();
        p.execute(&format!("DROP INDEX {sn}__ai__idx_col")).unwrap();
    }
}

#[test]
fn test_create_index_reserved_infix_rejected() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        p.execute("CREATE TABLE ri (id BIGINT PRIMARY KEY, col BIGINT)").unwrap();
    }
    let mut p = SqlPlanner::new(&mut client, &sn);
    let err = must_err(p.execute("CREATE INDEX my__fk_thing ON ri(col)"));
    match err {
        GnitzSqlError::Plan(s) => assert!(s.contains("reserved '__fk_' infix"), "got: {}", s),
        e => panic!("expected Plan, got {:?}", e),
    }
}

#[test]
fn test_create_index_invalid_identifier_rejected() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        p.execute("CREATE TABLE ii (id BIGINT PRIMARY KEY, col BIGINT)").unwrap();
    }
    let mut p = SqlPlanner::new(&mut client, &sn);
    let err = must_err(p.execute("CREATE INDEX _bad ON ii(col)"));
    match err {
        GnitzSqlError::Plan(s) => assert!(s.contains("cannot start with '_'"), "got: {}", s),
        e => panic!("expected Plan, got {:?}", e),
    }
}
