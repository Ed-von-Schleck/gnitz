use crate::catalog::CatalogEngine;
use crate::test_support::{col_def, register_identity_view, scratch_dir};
use gnitz_wire::type_code;

/// Wiring: the drop path forgets the view's metadata and its plan together, so
/// no plan outlives its relation holding stores open.
#[test]
fn forget_evicts_the_metadata_and_the_plan() {
    let mut engine = CatalogEngine::open(&scratch_dir("dag", "forget"), 1).unwrap();
    let cols = vec![col_def("id", type_code::U64), col_def("v", type_code::I64)];
    let base = engine.create_table("public.base", &cols, &[0]).unwrap();
    let view = register_identity_view(&mut engine, base, "v", &cols);
    engine.dag.compile_view(&engine.registry, view).unwrap();
    assert!(engine.dag.metas.contains_key(&view) && engine.dag.plans.contains_key(&view));

    engine.dag.forget(view);
    assert!(engine.dag.view_meta(view).is_err(), "the metadata is gone");
    assert!(!engine.dag.plans.contains_key(&view), "and so is the plan");
    engine.close();
}
