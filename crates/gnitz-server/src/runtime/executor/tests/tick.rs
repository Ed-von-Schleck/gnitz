use super::*;
use crate::test_support::{register_identity_view, table_engine, u64_i64_cols};

/// A pending commit leaves unticked exactly the targets whose source closure
/// holds its relation, through a view in between, until its entry goes.
#[test]
fn all_ticked_reads_each_targets_source_closure() {
    let cols = u64_i64_cols();
    let (mut engine, t) = table_engine("ticks", "all_ticked", 1, &cols);
    let u = engine.create_table("public.u", &cols, &[0]).unwrap();
    let v1 = register_identity_view(&mut engine, t, "v1", &cols);
    let v2 = register_identity_view(&mut engine, v1, "v2", &cols);
    let w = register_identity_view(&mut engine, u, "w", &cols);
    let (ticks, _triggers) = Ticks::new(0);
    let dag = &engine.dag;

    assert!(ticks.all_ticked(dag, [v1, v2, w]));
    ticks.pending.borrow_mut().insert(t, 1);
    assert!(!ticks.all_ticked(dag, [v2]));
    assert!(!ticks.all_ticked(dag, [w, v1]));
    assert!(ticks.all_ticked(dag, [w]));
    // A target that is no view has no sources.
    assert!(ticks.all_ticked(dag, [t, u, u64::MAX]));
    assert!(ticks.all_ticked(dag, []));
    ticks.forget(t);
    assert!(ticks.all_ticked(dag, [v1, v2, w]));
}
