use super::*;
use crate::test_support::{make_batch_raw, make_schema_u64_i64, weighted_rows};

/// A second ingest into one relation appends to what its next tick drains; a
/// different relation buffers apart, and forgetting it drops its buffer and its
/// pending rebuild.
#[test]
fn unticked_deltas_append_per_relation_until_taken_or_forgotten() {
    let schema = make_schema_u64_i64();
    let mut dag = DagEngine::default();

    dag.buffer_unticked(100, make_batch_raw(&schema, &[(1, 1, 10)]));
    dag.buffer_unticked(100, make_batch_raw(&schema, &[(2, -1, 20)]));
    dag.buffer_unticked(200, make_batch_raw(&schema, &[(3, 1, 30)]));
    assert_eq!(
        weighted_rows(&dag.take_unticked(100).unwrap()),
        weighted_rows(&make_batch_raw(&schema, &[(1, 1, 10), (2, -1, 20)])),
    );
    assert!(dag.take_unticked(100).is_none(), "a take drains it");

    dag.set_rebuild([200].into_iter().collect());
    dag.forget(200);
    assert!(dag.take_unticked(200).is_none());
    assert!(!dag.awaits_rebuild(200));
}
