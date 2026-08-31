use super::*;

// ── test_uuid_pk_create_and_drop ─────────────────────────────────────

#[test]
fn test_uuid_pk_create_and_drop() {
    let dir = temp_dir("uuid_pk_create");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();

    let tid = engine.create_table("public.uuid_tab", &[uuid_def("id")], &[0]).unwrap();
    let s = engine.registry().get_schema_desc(tid).unwrap();
    assert_eq!(s.columns[0].type_code, type_code::UUID);

    engine.drop_table("public.uuid_tab").unwrap();
    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── test_uuid_non_pk_column ──────────────────────────────────────────

#[test]
fn test_uuid_non_pk_column() {
    let dir = temp_dir("uuid_non_pk");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();

    let cols = vec![col_def("id", type_code::U64), uuid_def("uid")];
    let tid = engine.create_table("public.uuid_payload", &cols, &[0]).unwrap();
    let s = engine.registry().get_schema_desc(tid).unwrap();
    assert_eq!(s.columns[1].type_code, type_code::UUID);

    // Ingest a row with a UUID payload column
    let mut bb = BatchBuilder::new(s);
    bb.begin_row(1u128, 1);
    bb.put_int(UUID_A);
    bb.end_row();
    engine.ingest_to_family(tid, &bb.finish()).unwrap();
    engine.registry_mut().flush(tid).unwrap();

    engine.drop_table("public.uuid_payload").unwrap();
    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── test_uuid_secondary_index ────────────────────────────────────────

#[test]
fn test_uuid_secondary_index() {
    let dir = temp_dir("uuid_idx");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();

    let cols = vec![col_def("id", type_code::U64), uuid_def("uid")];
    let tid = engine.create_table("public.uuid_idxtab", &cols, &[0]).unwrap();
    let s = engine.registry().get_schema_desc(tid).unwrap();

    let mut bb = BatchBuilder::new(s);
    bb.begin_row(1u128, 1);
    bb.put_int(UUID_A);
    bb.end_row();
    engine.ingest_to_family(tid, &bb.finish()).unwrap();
    engine.registry_mut().flush(tid).unwrap();

    engine.create_index("public.uuid_idxtab", &["uid"], false).unwrap();
    assert!(engine.has_index_by_name("public__uuid_idxtab__idx_uid"));

    engine.drop_index("public__uuid_idxtab__idx_uid").unwrap();
    engine.drop_table("public.uuid_idxtab").unwrap();
    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── test_uuid_fk_u64pk_parent_rejected ───────────────────────────────

#[test]
fn test_uuid_fk_u64pk_parent_rejected() {
    let dir = temp_dir("uuid_fk_u64_mismatch");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();

    let parent_tid = engine
        .create_table("public.u64par", &[col_def("id", type_code::U64)], &[0])
        .unwrap();

    // UUID FK child → U64 PK parent should fail DDL
    let child_cols = vec![
        col_def("cid", type_code::U64),
        fk_def("uid_fk", type_code::UUID, parent_tid, 0),
    ];
    assert!(engine.create_table("public.uuid_chi7", &child_cols, &[0]).is_err());

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── test_uuid_fk_u128_col_references_uuid_pk ─────────────────────────

#[test]
fn test_uuid_fk_u128_col_references_uuid_pk() {
    let dir = temp_dir("uuid_fk_u128_vs_uuid");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();

    let parent_tid = engine
        .create_table("public.uuid_par8", &[uuid_def("id")], &[0])
        .unwrap();

    // U128 FK column → UUID PK parent should fail (different type codes)
    let child_cols = vec![
        col_def("cid", type_code::U64),
        fk_def("uid_fk", type_code::U128, parent_tid, 0),
    ];
    assert!(engine.create_table("public.uuid_chi8", &child_cols, &[0]).is_err());

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}
