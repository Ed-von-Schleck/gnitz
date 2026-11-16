use super::*;
use crate::test_support::row_key;
use crate::test_support::stream_input::{GROUPS, agg, arb_row, batch, schema};
use gnitz_wire::AggFunc::{Count, CountNonNull, Max, Min, Sum};
use proptest::prelude::*;

/// What [`AdhocFold::finish`] emits, a row at a time through
/// [`AggValues::emit_row`], as the reduce emits a group.
fn finish_by_row(fold: &AdhocFold) -> Batch {
    let carried = fold.groups.schema().payload_locators();
    let mut output = Batch::with_capacity(&fold.shape.output_schema, fold.groups.len());
    let groups = fold.groups.as_mem_batch();
    for g in 0..fold.groups.len() {
        fold.vals
            .emit_row(&mut output, Some((&groups, g, &carried[..])), groups.get_pk_bytes(g), g);
    }
    output
}

proptest! {
    /// Under every group key form, over two chunks of positive rows, the fold's
    /// column-at-a-time finish is its row-at-a-time one, weights included: a
    /// count, an integer and a float sum, scalar extremes that may hold no
    /// value, a string extreme and a 16-byte one.
    #[test]
    fn finish_emits_each_group_as_its_row(
        rows in prop::collection::vec((arb_row(), 1i64..3), 0..40),
        split in 0usize..40,
    ) {
        let (first, second) = rows.split_at(split.min(rows.len()));
        let chunks = [batch(first), batch(second)];
        let aggs = vec![
            agg(Count, 0),
            agg(Sum, 0),
            agg(Min, 0),
            agg(Max, 7),
            agg(Sum, 4),
            agg(Min, 2),
            agg(Max, 2),
            agg(CountNonNull, 2),
            agg(Max, 6),
        ];
        for group_cols in GROUPS {
            let spec = GroupAggs { group_cols: group_cols.to_vec(), aggs: aggs.clone() };
            let mut fold = AdhocFold::new(&schema(), &spec, 1 << 16).unwrap();
            for c in &chunks {
                fold.fold_ranges(c, &[(0, c.len())]).unwrap();
            }
            let want = finish_by_row(&fold);
            let out_schema = fold.shape.output_schema;
            let got = fold.finish();
            prop_assert_eq!(got.len(), want.len(), "{:?}", group_cols);
            let (got, want) = (got.as_mem_batch(), want.as_mem_batch());
            for r in 0..got.len() {
                prop_assert_eq!(got.get_weight(r), want.get_weight(r), "{:?}: row {}", group_cols, r);
                prop_assert_eq!(
                    row_key(&got, &out_schema, r),
                    row_key(&want, &out_schema, r),
                    "{:?}: row {}",
                    group_cols,
                    r
                );
            }
        }
    }
}
