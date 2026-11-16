//! Window operator tests, driven epoch by epoch through the production populate
//! and integrate paths against a brute-force walk of the integrated input.

use std::cmp::Ordering;
use std::collections::BTreeMap;

use proptest::prelude::*;

use crate::stream::ordered::tests::{Harness, arb_keys, ordered_groups};
use crate::test_support::stream_input::{
    Cells, GROUPS, Row, agg, agg_of, arb_row, arb_ticks, batch, cells, delta_to, schema,
};
use gnitz_expr::cmp_order_keys;
use gnitz_wire::{AggDescriptor, AggFunc, OrderKey};

use crate::stream::ordered::{OrderedKind, OrderedPlan};

/// Every aggregate, over every column form one reads: none, a nullable
/// integer, a string, a float, and a column the group may be keyed on.
fn aggs() -> Vec<AggDescriptor> {
    vec![
        agg(AggFunc::Sum, 0),
        AggDescriptor::COUNT_STAR,
        agg(AggFunc::CountNonNull, 0),
        agg(AggFunc::Min, 2),
        agg(AggFunc::Max, 0),
        agg(AggFunc::Max, 4),
        agg(AggFunc::Max, 3),
    ]
}

/// The counts and integer sums: the aggregates whose prefix folds as sums over
/// the index's runs.
fn summed_aggs() -> Vec<AggDescriptor> {
    vec![
        agg(AggFunc::Sum, 0),
        AggDescriptor::COUNT_STAR,
        agg(AggFunc::CountNonNull, 2),
    ]
}

/// The rank, the dense rank where `dense`, and `aggs` of every row of `model`,
/// each over the rows of its group at or before its peers under `keys`.
fn window(
    model: &BTreeMap<Row, i64>,
    group_cols: &[u32],
    keys: &[OrderKey],
    aggs: &[AggDescriptor],
    dense: bool,
) -> BTreeMap<(Cells, Cells), i64> {
    let rows: Vec<(Row, i64)> = model.iter().filter(|&(_, &w)| w > 0).map(|(r, &w)| (*r, w)).collect();
    let (b, locs, groups) = ordered_groups(&rows, group_cols, keys);
    let mb = b.as_mem_batch();
    let int = |v: i64| Some(v.to_le_bytes().to_vec());
    let mut want = BTreeMap::new();
    for group in groups {
        let (mut before, mut groups) = (0i64, 0i64);
        let mut prefix: Vec<(Cells, i64)> = Vec::new();
        let mut at = 0;
        while at < group.len() {
            let end = (at..group.len())
                .find(|&j| cmp_order_keys(&locs, &mb, group[at], group[j]) != Ordering::Equal)
                .unwrap_or(group.len());
            groups += 1;
            prefix.extend(group[at..end].iter().map(|&i| (cells(&rows[i].0), rows[i].1)));
            let values: Cells = [Some(int(before + 1)), dense.then(|| int(groups))]
                .into_iter()
                .flatten()
                .chain(aggs.iter().map(|&a| agg_of(a, &prefix)))
                .collect();
            for &i in &group[at..end] {
                want.insert((cells(&rows[i].0), values.clone()), rows[i].1);
            }
            before = prefix.iter().map(|(_, w)| w).sum();
            at = end;
        }
    }
    want
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// After every epoch the maintained output is the window over the integrated
    /// input, weights included, under every group key form of [`GROUPS`].
    #[test]
    fn the_maintained_values_are_the_values_of_the_integral(
        group_cols in prop::sample::select(&GROUPS[..]),
        keys in arb_keys(),
        ticks in arb_ticks(arb_row()),
    ) {
        let plan = OrderedPlan::window(&schema(), group_cols, &keys, &aggs(), true, true).unwrap();
        let mut h = Harness::new(plan);
        let mut model = BTreeMap::new();
        for targets in &ticks {
            h.tick(&batch(&delta_to(&mut model, targets)));
            prop_assert_eq!(h.state(group_cols), window(&model, group_cols, &keys, &aggs(), true));
        }
    }

    /// The same of a window of counts and integer sums and a rank, whose rows
    /// before an entry fold as sums over the index's runs: with many runs, each
    /// epoch's own, and rows of a later run cancelling an earlier one's.
    #[test]
    fn summed_prefix_values_are_the_values_of_the_integral(
        group_cols in prop::sample::select(&GROUPS[..]),
        keys in arb_keys(),
        ticks in prop::collection::vec(arb_row(), 8..32).prop_flat_map(|pool| {
            prop::collection::vec(prop::collection::vec((prop::sample::select(pool), 0i64..=3), 0..12), 2..10)
        }),
    ) {
        let plan = OrderedPlan::window(&schema(), group_cols, &keys, &summed_aggs(), true, false).unwrap();
        prop_assert!(matches!(&plan.kind, OrderedKind::Window(w) if w.summed_prefix));
        let mut h = Harness::new(plan);
        let mut model = BTreeMap::new();
        for targets in &ticks {
            h.tick(&batch(&delta_to(&mut model, targets)));
            prop_assert_eq!(h.state(group_cols), window(&model, group_cols, &keys, &summed_aggs(), false));
        }
    }
}

/// A row appended behind every other row of its group changes no other row, and
/// the epoch emits it alone; one put in front re-emits every row behind it.
#[test]
fn a_row_moves_the_rows_behind_it_and_no_others() {
    let key = OrderKey { col: 0, desc: false, nulls_first: false };
    let plan = OrderedPlan::window(&schema(), &[3], &[key], &[agg(AggFunc::Sum, 0)], false, false).unwrap();
    let mut h = Harness::new(plan);
    let row = |id, grp, val| {
        (
            Row {
                id,
                grp,
                val: Some(val),
                ..Row::default()
            },
            1,
        )
    };
    h.tick(&batch(&[row(1, 7, 10), row(2, 7, 20), row(3, 7, 30)]));
    assert_eq!(h.tick(&batch(&[row(4, 7, 40)])).len(), 1, "an append");
    // The new row, and a retraction and an insert for each of the four behind it.
    assert_eq!(h.tick(&batch(&[row(5, 7, 5)])).len(), 9, "a row in front");
    // A row of another group reads none of this one's.
    assert_eq!(h.tick(&batch(&[row(6, 8, 1)])).len(), 1, "another group");
}

/// A float sum is the fold of the group in index order, so a row's value is a
/// function of the group alone: retracting what an epoch added restores it bit
/// for bit.
#[test]
fn a_float_sum_is_a_function_of_the_group() {
    let key = OrderKey { col: 1, desc: false, nulls_first: false };
    let plan = OrderedPlan::window(&schema(), &[3], &[key], &[agg(AggFunc::Sum, 4)], false, false).unwrap();
    let mut h = Harness::new(plan);
    let row = |id: u64, f: f64| (Row { id, f: f.to_bits(), ..Row::default() }, 1);
    h.tick(&batch(&[row(2, 0.1), row(3, 0.2), row(4, 1e17)]));
    let before = h.state(&[3]);
    let wedge = batch(&[row(1, 0.3)]);
    h.tick(&wedge);
    assert_ne!(h.state(&[3]), before);
    h.tick(&wedge.negated());
    assert_eq!(h.state(&[3]), before);
}

#[test]
fn a_shape_the_window_cannot_run_is_refused() {
    let s = schema();
    let order = [OrderKey { col: 0, desc: false, nulls_first: false }];
    let err = |r: Result<OrderedPlan, String>| r.err().expect("refused");
    assert!(err(OrderedPlan::window(&s, &[], &order, &[], false, false)).contains("no value columns"));
    let sum = |col| [agg(AggFunc::Sum, col)];
    assert!(err(OrderedPlan::window(&s, &[], &order, &sum(9), false, false)).contains("carried column"));
    assert!(err(OrderedPlan::window(&s, &[], &order, &sum(2), false, false)).contains("not defined over"));
}
