use super::*;
use crate::test_support::{make_batch_u128_raw, make_schema_u128_i64};

/// `clear_deltas` must empty the delta registers while leaving the trace
/// registers untouched.
#[test]
fn test_clear_deltas_clears_only_delta_registers() {
    let schema = make_schema_u128_i64();
    let row = |pk: u128, val: i64| make_batch_u128_raw(&schema, &[(pk, 1, val)]);

    let metas = [RegisterMeta::delta(schema), RegisterMeta::trace(schema, TableIdx(0))];
    let mut rf = RegisterFile {
        batches: vec![row(1, 10), row(2, 20)],
        cursors: vec![None, None],
    };
    assert_eq!(rf.batches[0].count, 1);
    rf.clear_deltas(&metas);
    assert_eq!(rf.batches[0].count, 0, "Delta register must be cleared");
    assert_eq!(rf.batches[1].count, 1, "Trace register must be preserved");
}
