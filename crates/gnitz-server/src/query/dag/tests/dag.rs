use super::*;
use crate::test_support::{make_batch_raw, make_schema_u64_i64};

/// A second ingest into one relation appends to what its next tick drains; a
/// different relation buffers apart.
#[test]
fn buffer_unticked_appends() {
    let schema = make_schema_u64_i64();
    let mut dag = DagEngine::default();

    dag.buffer_unticked(100, make_batch_raw(&schema, &[(1, 1, 10)]));
    dag.buffer_unticked(100, make_batch_raw(&schema, &[(2, 1, 20)]));
    dag.buffer_unticked(200, make_batch_raw(&schema, &[(3, 1, 30)]));
    assert_eq!(
        dag.take_unticked(100).map(|b| b.len()),
        Some(2),
        "a second delta appends"
    );
    assert_eq!(dag.take_unticked(100).map(|b| b.len()), None, "a take drains it");
    dag.forget(200);
    assert!(
        dag.take_unticked(200).is_none(),
        "a dropped relation's buffer goes with it"
    );
}
