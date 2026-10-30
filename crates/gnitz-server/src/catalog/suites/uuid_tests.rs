use super::*;

// ── test_uuid_pk_create_and_drop ─────────────────────────────────────

#[test]
fn test_uuid_pk_create_and_drop() {
    let mut engine = fresh("uuid_pk_create");

    let tid = engine.create_table("public.uuid_tab", &[uuid_def("id")], &[0]).unwrap();
    let s = engine.registry.relation(tid).map(Relation::schema).unwrap();
    assert_eq!(s.columns()[0].type_code, TypeCode::UUID);

    engine.drop_table("public.uuid_tab").unwrap();
    finish(engine);
}

// ── test_uuid_secondary_index ────────────────────────────────────────

#[test]
fn test_uuid_secondary_index() {
    let mut engine = fresh("uuid_idx");

    let cols = vec![col_def("id", TypeCode::U64), uuid_def("uid")];
    let tid = engine.create_table("public.uuid_idxtab", &cols, &[0]).unwrap();
    let s = engine.registry.relation(tid).map(Relation::schema).unwrap();
    assert_eq!(s.columns()[1].type_code, TypeCode::UUID);

    let mut bb = BatchBuilder::new(&s);
    bb.begin_row(1u128, 1);
    bb.put_int(UUID_A);
    bb.end_row();
    engine.registry.ingest(tid, bb.finish()).unwrap();
    engine.registry.checkpoint_base().unwrap();

    engine.create_index("public.uuid_idxtab", &["uid"], false).unwrap();
    assert!(engine.has_index_by_name("public__uuid_idxtab__idx_uid"));

    engine.drop_index("public__uuid_idxtab__idx_uid").unwrap();
    engine.drop_table("public.uuid_idxtab").unwrap();
    finish(engine);
}
