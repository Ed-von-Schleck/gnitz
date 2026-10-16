use super::*;

use std::path::Path;

// A drop leaves its directory to the sweep, and the sweep takes it.
#[test]
fn dropped_relation_dir_survives_until_the_sweep() {
    let dir = temp_dir("sweep_dropped_dir");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    let tid = engine
        .create_table("public.t", &[col_def("id", TypeCode::U64)], &[0])
        .unwrap();
    let tbl_dir = relation_dir(&dir, tid);

    engine.drop_table("public.t").unwrap();
    assert!(Path::new(&tbl_dir).exists(), "a drop removes no directory itself");

    engine.reclaim_orphan_dirs();
    assert!(
        !Path::new(&tbl_dir).exists(),
        "the sweep must reclaim a dropped relation's directory"
    );

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// Liveness is read at sweep time, so an id registered again after its drop
// keeps its directory.
#[test]
fn sweep_keeps_a_re_registered_id() {
    let dir = temp_dir("sweep_keeps_reregistered");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    let cols = vec![col_def("id", TypeCode::U64)];
    let tid = engine.create_table("public.t", &cols, &[0]).unwrap();
    let tbl_dir = relation_dir(&dir, tid);

    engine.drop_table("public.t").unwrap();
    engine.register_table(tid, PUBLIC_SCHEMA_ID, "t", &cols, &[0]).unwrap();

    engine.reclaim_orphan_dirs();
    assert!(
        Path::new(&tbl_dir).exists(),
        "a re-registered id's directory must survive the sweep"
    );

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}
