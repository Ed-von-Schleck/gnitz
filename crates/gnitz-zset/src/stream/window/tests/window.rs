//! Window operator tests, driven epoch by epoch through the production populate
//! and integrate paths against a brute-force walk of the integrated input.

use std::cmp::Ordering;
use std::collections::BTreeMap;

use proptest::prelude::*;

use crate::stream::order_index::tests::{arb_keys, arb_row, batch, cells, schema, Cells, Harness, Row};
use crate::test_support::{arb_ticks, delta_to};
use gnitz_expr::{cmp_order_keys, order_locators};
use gnitz_wire::{AggDescriptor, AggFunc, OrderKey};

use crate::stream::order_index::OrderedPlan;

fn agg(agg_op: AggFunc, col_idx: u32) -> AggDescriptor {
    AggDescriptor { agg_op, col_idx }
}

/// Every aggregate, over every column form one reads: none, a nullable
/// integer, a string, a float, and a column the group may be keyed on.
fn aggs() -> Vec<AggDescriptor> {
    vec![
        agg(AggFunc::Sum, 2),
        AggDescriptor::COUNT_STAR,
        agg(AggFunc::CountNonNull, 2),
        agg(AggFunc::Min, 3),
        agg(AggFunc::Max, 2),
        agg(AggFunc::Max, 4),
        agg(AggFunc::Max, 1),
    ]
}

/// Both ranks and [`aggs`] of every row of `model`, each over the rows of its group at or
/// before its peers under `keys`.
fn window(model: &BTreeMap<Row, i64>, group_cols: &[u32], keys: &[OrderKey]) -> BTreeMap<(Cells, Cells), i64> {
    let rows: Vec<(Row, i64)> = model.iter().filter(|(_, &w)| w > 0).map(|(r, &w)| (*r, w)).collect();
    let b = batch(&rows);
    let mb = b.as_mem_batch();
    let locs = order_locators(keys, &schema(), false);
    let mut order: Vec<usize> = (0..rows.len()).collect();
    order.sort_by(|&x, &y| cmp_order_keys(&locs, &mb, x, y));
    let mut groups: BTreeMap<Cells, Vec<usize>> = BTreeMap::new();
    for i in order {
        let row = cells(&rows[i].0);
        let key = group_cols.iter().map(|&c| row[c as usize].clone()).collect();
        groups.entry(key).or_default().push(i);
    }
    let int = |v: i64| Some(v.to_le_bytes().to_vec());
    let mut want = BTreeMap::new();
    for group in groups.into_values() {
        let (mut before, mut dense, mut count, mut sum, mut non_null) = (0i64, 0i64, 0i64, 0i64, 0i64);
        let (mut least, mut most, mut most_f): (Option<&str>, Option<i64>, Option<f64>) = (None, None, None);
        let mut most_grp: Option<i64> = None;
        let mut at = 0;
        while at < group.len() {
            let end = (at..group.len())
                .find(|&j| cmp_order_keys(&locs, &mb, group[at], group[j]) != Ordering::Equal)
                .unwrap_or(group.len());
            dense += 1;
            for &i in &group[at..end] {
                let ((_, grp, val, s, f), w) = rows[i];
                count += w;
                most_grp = Some(most_grp.map_or(grp, |m| m.max(grp)));
                if let Some(v) = val {
                    sum = sum.wrapping_add(v.wrapping_mul(w));
                    non_null += w;
                    most = Some(most.map_or(v, |m| m.max(v)));
                }
                if let Some(s) = s {
                    least = Some(least.map_or(s, |l| l.min(s)));
                }
                let f = f64::from_bits(f);
                most_f = Some(most_f.map_or(f, |m| if f.total_cmp(&m).is_gt() { f } else { m }));
            }
            let values = vec![
                int(before + 1),
                int(dense),
                int(sum),
                int(count),
                int(non_null),
                least.map(|s| s.as_bytes().to_vec()),
                most.and_then(int),
                most_f.map(|f| f.to_bits().to_le_bytes().to_vec()),
                most_grp.and_then(int),
            ];
            for &i in &group[at..end] {
                want.insert((cells(&rows[i].0), values.clone()), rows[i].1);
            }
            before = count;
            at = end;
        }
    }
    want
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// After every epoch the maintained output is the window over the integrated
    /// input, weights included, under every group key form: V₀, a non-null
    /// column's image, and the fold of a nullable or multi-column set.
    #[test]
    fn the_maintained_values_are_the_values_of_the_integral(
        gi in 0usize..4,
        keys in arb_keys(),
        ticks in arb_ticks(arb_row()),
    ) {
        let group_cols: &[u32] = [&[][..], &[1], &[0], &[1, 0]][gi];
        let plan = OrderedPlan::window(&schema(), group_cols, &keys, &aggs(), true, true).unwrap();
        let mut h = Harness::new(plan);
        let mut model = BTreeMap::new();
        for targets in &ticks {
            h.tick(&batch(&delta_to(&mut model, targets)));
            prop_assert_eq!(h.state(group_cols), window(&model, group_cols, &keys));
        }
    }
}

/// A row appended behind every other row of its group changes no other row, and
/// the epoch emits it alone; one put in front re-emits every row behind it.
#[test]
fn a_row_moves_the_rows_behind_it_and_no_others() {
    let key = OrderKey { col: 2, desc: false, nulls_first: false };
    let plan = OrderedPlan::window(&schema(), &[1], &[key], &[agg(AggFunc::Sum, 2)], false, false).unwrap();
    let mut h = Harness::new(plan);
    let row = |id, val| ((id, 7, Some(val), None, 0), 1);
    h.tick(&batch(&[row(1, 10), row(2, 20), row(3, 30)]));
    assert_eq!(h.tick(&batch(&[row(4, 40)])).count, 1, "an append");
    // The new row, and a retraction and an insert for each of the four behind it.
    assert_eq!(h.tick(&batch(&[row(5, 5)])).count, 9, "a row in front");
    // A row of another group reads none of this one's.
    assert_eq!(
        h.tick(&batch(&[((6, 8, Some(1), None, 0), 1)])).count,
        1,
        "another group"
    );
}

/// A float sum is the fold of the group in index order, so a row's value is a
/// function of the group alone: retracting what an epoch added restores it bit
/// for bit.
#[test]
fn a_float_sum_is_a_function_of_the_group() {
    let key = OrderKey { col: 0, desc: false, nulls_first: false };
    let plan = OrderedPlan::window(&schema(), &[1], &[key], &[agg(AggFunc::Sum, 4)], false, false).unwrap();
    let mut h = Harness::new(plan);
    let row = |id: u64, f: f64| ((id, 0, None, None, f.to_bits()), 1);
    h.tick(&batch(&[row(2, 0.1), row(3, 0.2), row(4, 1e17)]));
    let before = h.state(&[1]);
    let wedge = batch(&[row(1, 0.3)]);
    h.tick(&wedge);
    assert_ne!(h.state(&[1]), before);
    h.tick(&wedge.negated());
    assert_eq!(h.state(&[1]), before);
}

#[test]
fn from_wire_rejects_what_it_cannot_run() {
    let s = schema();
    let order = [OrderKey { col: 2, desc: false, nulls_first: false }];
    let err = |r: Result<OrderedPlan, String>| r.err().expect("refused");
    assert!(err(OrderedPlan::window(&s, &[], &order, &[], false, false)).contains("no value columns"));
    assert!(err(OrderedPlan::window(&s, &[], &[], &[], true, false)).contains("no order keys"));
    let sum = |col| [agg(AggFunc::Sum, col)];
    assert!(err(OrderedPlan::window(&s, &[], &order, &sum(9), false, false)).contains("carried column"));
    assert!(err(OrderedPlan::window(&s, &[], &order, &sum(3), false, false)).contains("not defined over"));
}
