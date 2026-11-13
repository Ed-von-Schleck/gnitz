use super::*;

use std::path::Path;

// A drop leaves its directory to the sweep, and the sweep takes it.
#[test]
fn dropped_relation_dir_survives_until_the_sweep() {
    let (_dir, mut engine) = empty_catalog(1);
    let tid = engine
        .create_table("public.t", &[col_def("id", TypeCode::U64)], &[0])
        .unwrap();
    let tbl_dir = relation_dir(engine.registry.base_dir(), tid);

    engine.drop_table("public.t").unwrap();
    assert!(Path::new(&tbl_dir).exists(), "a drop removes no directory itself");

    engine.registry.reclaim_orphan_relation_dirs().unwrap();
    assert!(
        !Path::new(&tbl_dir).exists(),
        "the sweep must reclaim a dropped relation's directory"
    );

    engine.close();
}

// Liveness is read at sweep time, so an id registered again after its drop
// keeps its directory.
#[test]
fn sweep_keeps_a_re_registered_id() {
    let (_dir, mut engine) = empty_catalog(1);
    let cols = vec![col_def("id", TypeCode::U64)];
    let tid = engine.create_table("public.t", &cols, &[0]).unwrap();
    let tbl_dir = relation_dir(engine.registry.base_dir(), tid);

    engine.drop_table("public.t").unwrap();
    engine.register_table(tid, PUBLIC_SCHEMA_ID, "t", &cols, &[0]).unwrap();

    engine.registry.reclaim_orphan_relation_dirs().unwrap();
    assert!(
        Path::new(&tbl_dir).exists(),
        "a re-registered id's directory must survive the sweep"
    );

    engine.close();
}
