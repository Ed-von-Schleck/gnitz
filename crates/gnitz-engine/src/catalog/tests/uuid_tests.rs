use super::*;

fn uuid_col_def(name: &str) -> ColumnDef {
    ColumnDef { name: name.into(), type_code: type_code::UUID, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 }
}

fn nullable_uuid_col_def(name: &str) -> ColumnDef {
    ColumnDef { name: name.into(), type_code: type_code::UUID, is_nullable: true, fk_table_id: 0, fk_col_idx: 0 }
}

// ── test_uuid_pk_create_and_drop ─────────────────────────────────────

#[test]
fn test_uuid_pk_create_and_drop() {
    let dir = temp_dir("uuid_pk_create");
    let mut engine = CatalogEngine::open(&dir).unwrap();

    let tid = engine.create_table("public.uuid_tab",
        &[uuid_col_def("id")], 0, true).unwrap();
    let s = engine.get_schema(tid).unwrap();
    assert_eq!(s.columns[0].type_code, type_code::UUID);

    engine.drop_table("public.uuid_tab").unwrap();
    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── test_uuid_non_pk_column ──────────────────────────────────────────

#[test]
fn test_uuid_non_pk_column() {
    let dir = temp_dir("uuid_non_pk");
    let mut engine = CatalogEngine::open(&dir).unwrap();

    let cols = vec![u64_col_def("id"), uuid_col_def("uid")];
    let tid = engine.create_table("public.uuid_payload", &cols, 0, true).unwrap();
    let s = engine.get_schema(tid).unwrap();
    assert_eq!(s.columns[1].type_code, type_code::UUID);

    // Ingest a row with a UUID payload column
    let mut bb = BatchBuilder::new(s);
    bb.begin_row(1, 0, 1);
    bb.put_u128(0xBBBB, 0xAAAA);
    bb.end_row();
    engine.dag.ingest_to_family(tid, bb.finish());
    let _ = engine.dag.flush(tid);

    engine.drop_table("public.uuid_payload").unwrap();
    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── test_uuid_secondary_index ────────────────────────────────────────

#[test]
fn test_uuid_secondary_index() {
    let dir = temp_dir("uuid_idx");
    let mut engine = CatalogEngine::open(&dir).unwrap();

    let cols = vec![u64_col_def("id"), uuid_col_def("uid")];
    let tid = engine.create_table("public.uuid_idxtab", &cols, 0, true).unwrap();
    let s = engine.get_schema(tid).unwrap();

    let mut bb = BatchBuilder::new(s);
    bb.begin_row(1, 0, 1);
    bb.put_u128(0xBBBB, 0xAAAA);
    bb.end_row();
    engine.dag.ingest_to_family(tid, bb.finish());
    let _ = engine.dag.flush(tid);

    engine.create_index("public.uuid_idxtab", "uid", false).unwrap();
    assert!(engine.has_index_by_name("public__uuid_idxtab__idx_uid"));

    engine.drop_index("public__uuid_idxtab__idx_uid").unwrap();
    engine.drop_table("public.uuid_idxtab").unwrap();
    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── test_uuid_fk_valid_single ────────────────────────────────────────

#[test]
fn test_uuid_fk_valid_single() {
    let dir = temp_dir("uuid_fk_valid");
    let mut engine = CatalogEngine::open(&dir).unwrap();

    let parent_tid = engine.create_table("public.uuid_par",
        &[uuid_col_def("id")], 0, true).unwrap();

    let child_cols = vec![
        u64_col_def("cid"),
        ColumnDef { name: "uid_fk".into(), type_code: type_code::UUID, is_nullable: false,
                    fk_table_id: parent_tid, fk_col_idx: 0 },
    ];
    let child_tid = engine.create_table("public.uuid_chi", &child_cols, 0, true).unwrap();

    let parent_schema = engine.get_schema(parent_tid).unwrap();
    let mut pbb = BatchBuilder::new(parent_schema);
    pbb.begin_row(0xBBBB, 0xAAAA, 1);
    pbb.end_row();
    engine.dag.ingest_to_family(parent_tid, pbb.finish());
    let _ = engine.dag.flush(parent_tid);

    let child_schema = engine.get_schema(child_tid).unwrap();
    let mut cbb = BatchBuilder::new(child_schema);
    cbb.begin_row(1, 0, 1);
    cbb.put_u128(0xBBBB, 0xAAAA);
    cbb.end_row();
    assert!(engine.validate_fk_inline(child_tid, &cbb.finish()).is_ok());

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── test_uuid_fk_invalid_single ──────────────────────────────────────

#[test]
fn test_uuid_fk_invalid_single() {
    let dir = temp_dir("uuid_fk_invalid");
    let mut engine = CatalogEngine::open(&dir).unwrap();

    let parent_tid = engine.create_table("public.uuid_par2",
        &[uuid_col_def("id")], 0, true).unwrap();

    let child_cols = vec![
        u64_col_def("cid"),
        ColumnDef { name: "uid_fk".into(), type_code: type_code::UUID, is_nullable: false,
                    fk_table_id: parent_tid, fk_col_idx: 0 },
    ];
    let child_tid = engine.create_table("public.uuid_chi2", &child_cols, 0, true).unwrap();

    let child_schema = engine.get_schema(child_tid).unwrap();
    let mut cbb = BatchBuilder::new(child_schema);
    cbb.begin_row(1, 0, 1);
    cbb.put_u128(0xDEAD, 0xBEEF);
    cbb.end_row();
    assert!(engine.validate_fk_inline(child_tid, &cbb.finish()).is_err());

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── test_uuid_fk_multiple_children_same_parent ───────────────────────

#[test]
fn test_uuid_fk_multiple_children_same_parent() {
    let dir = temp_dir("uuid_fk_multi");
    let mut engine = CatalogEngine::open(&dir).unwrap();

    let parent_tid = engine.create_table("public.uuid_par3",
        &[uuid_col_def("id")], 0, true).unwrap();

    let child_cols = vec![
        u64_col_def("cid"),
        ColumnDef { name: "uid_fk".into(), type_code: type_code::UUID, is_nullable: false,
                    fk_table_id: parent_tid, fk_col_idx: 0 },
    ];
    let child_tid = engine.create_table("public.uuid_chi3", &child_cols, 0, true).unwrap();

    let parent_schema = engine.get_schema(parent_tid).unwrap();
    let mut pbb = BatchBuilder::new(parent_schema);
    pbb.begin_row(0x1111, 0, 1);
    pbb.end_row();
    engine.dag.ingest_to_family(parent_tid, pbb.finish());
    let _ = engine.dag.flush(parent_tid);

    let child_schema = engine.get_schema(child_tid).unwrap();
    let mut cbb = BatchBuilder::new(child_schema);
    cbb.begin_row(1, 0, 1);
    cbb.put_u128(0x1111, 0);
    cbb.end_row();
    cbb.begin_row(2, 0, 1);
    cbb.put_u128(0x1111, 0);
    cbb.end_row();
    assert!(engine.validate_fk_inline(child_tid, &cbb.finish()).is_ok());

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── test_uuid_fk_multiple_children_mixed ─────────────────────────────

#[test]
fn test_uuid_fk_multiple_children_mixed() {
    let dir = temp_dir("uuid_fk_mixed");
    let mut engine = CatalogEngine::open(&dir).unwrap();

    let parent_tid = engine.create_table("public.uuid_par4",
        &[uuid_col_def("id")], 0, true).unwrap();

    let child_cols = vec![
        u64_col_def("cid"),
        ColumnDef { name: "uid_fk".into(), type_code: type_code::UUID, is_nullable: false,
                    fk_table_id: parent_tid, fk_col_idx: 0 },
    ];
    let child_tid = engine.create_table("public.uuid_chi4", &child_cols, 0, true).unwrap();

    let parent_schema = engine.get_schema(parent_tid).unwrap();
    let mut pbb = BatchBuilder::new(parent_schema);
    pbb.begin_row(0xAAAA, 0, 1);
    pbb.end_row();
    engine.dag.ingest_to_family(parent_tid, pbb.finish());
    let _ = engine.dag.flush(parent_tid);

    let child_schema = engine.get_schema(child_tid).unwrap();
    let mut cbb = BatchBuilder::new(child_schema);
    cbb.begin_row(1, 0, 1);
    cbb.put_u128(0xAAAA, 0); // valid
    cbb.end_row();
    cbb.begin_row(2, 0, 1);
    cbb.put_u128(0xDEAD, 0xBEEF); // invalid
    cbb.end_row();
    assert!(engine.validate_fk_inline(child_tid, &cbb.finish()).is_err());

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── test_uuid_fk_parent_not_yet_ingested ─────────────────────────────

#[test]
fn test_uuid_fk_parent_not_yet_ingested() {
    let dir = temp_dir("uuid_fk_no_parent");
    let mut engine = CatalogEngine::open(&dir).unwrap();

    let parent_tid = engine.create_table("public.uuid_par5",
        &[uuid_col_def("id")], 0, true).unwrap();

    let child_cols = vec![
        u64_col_def("cid"),
        ColumnDef { name: "uid_fk".into(), type_code: type_code::UUID, is_nullable: false,
                    fk_table_id: parent_tid, fk_col_idx: 0 },
    ];
    let child_tid = engine.create_table("public.uuid_chi5", &child_cols, 0, true).unwrap();

    // No parent rows ingested
    let child_schema = engine.get_schema(child_tid).unwrap();
    let mut cbb = BatchBuilder::new(child_schema);
    cbb.begin_row(1, 0, 1);
    cbb.put_u128(0xBBBB, 0xAAAA);
    cbb.end_row();
    assert!(engine.validate_fk_inline(child_tid, &cbb.finish()).is_err());

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── test_uuid_fk_nullable_column ─────────────────────────────────────

#[test]
fn test_uuid_fk_nullable_column() {
    let dir = temp_dir("uuid_fk_nullable");
    let mut engine = CatalogEngine::open(&dir).unwrap();

    let parent_tid = engine.create_table("public.uuid_par6",
        &[uuid_col_def("id")], 0, true).unwrap();

    let child_cols = vec![
        u64_col_def("cid"),
        ColumnDef { name: "uid_fk".into(), type_code: type_code::UUID, is_nullable: true,
                    fk_table_id: parent_tid, fk_col_idx: 0 },
    ];
    let child_tid = engine.create_table("public.uuid_chi6", &child_cols, 0, true).unwrap();

    // NULL FK should be accepted even with no parent rows
    let child_schema = engine.get_schema(child_tid).unwrap();
    let mut bb = BatchBuilder::new(child_schema);
    bb.begin_row(1, 0, 1);
    bb.put_null();
    bb.end_row();
    assert!(engine.validate_fk_inline(child_tid, &bb.finish()).is_ok());

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── test_uuid_fk_u64pk_parent_rejected ───────────────────────────────

#[test]
fn test_uuid_fk_u64pk_parent_rejected() {
    let dir = temp_dir("uuid_fk_u64_mismatch");
    let mut engine = CatalogEngine::open(&dir).unwrap();

    let parent_tid = engine.create_table("public.u64par",
        &[u64_col_def("id")], 0, true).unwrap();

    // UUID FK child → U64 PK parent should fail DDL
    let child_cols = vec![
        u64_col_def("cid"),
        ColumnDef { name: "uid_fk".into(), type_code: type_code::UUID, is_nullable: false,
                    fk_table_id: parent_tid, fk_col_idx: 0 },
    ];
    assert!(engine.create_table("public.uuid_chi7", &child_cols, 0, true).is_err());

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── test_uuid_fk_u128_col_references_uuid_pk ─────────────────────────

#[test]
fn test_uuid_fk_u128_col_references_uuid_pk() {
    let dir = temp_dir("uuid_fk_u128_vs_uuid");
    let mut engine = CatalogEngine::open(&dir).unwrap();

    let parent_tid = engine.create_table("public.uuid_par8",
        &[uuid_col_def("id")], 0, true).unwrap();

    // U128 FK column → UUID PK parent should fail (different type codes)
    let child_cols = vec![
        u64_col_def("cid"),
        ColumnDef { name: "uid_fk".into(), type_code: type_code::U128, is_nullable: false,
                    fk_table_id: parent_tid, fk_col_idx: 0 },
    ];
    assert!(engine.create_table("public.uuid_chi8", &child_cols, 0, true).is_err());

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}
