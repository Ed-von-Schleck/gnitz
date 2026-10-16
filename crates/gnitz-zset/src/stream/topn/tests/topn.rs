//! Top-N operator tests, driven epoch by epoch through the production populate
//! and integrate paths against a brute-force window over the integrated input.

use std::collections::BTreeMap;

use proptest::prelude::*;

use crate::repr::Batch;
use crate::stream::order_index::tests::{arb_keys, arb_row, batch, cells, schema, Cells, Harness, Row};
use crate::test_support::{arb_ticks, delta_to};
use gnitz_expr::{cmp_order_keys, order_locators};
use gnitz_wire::OrderKey;

use crate::stream::order_index::OrderedPlan;

/// Weight slots `offset .. offset + limit` of each group of `model`, in the
/// read path's order: `keys`, then the identity tie-break an ad-hoc `ORDER BY`
/// cuts by.
fn window(
    model: &BTreeMap<Row, i64>,
    group_cols: &[u32],
    keys: &[OrderKey],
    limit: u64,
    offset: u64,
) -> BTreeMap<(Cells, Cells), i64> {
    // One entry per weight slot.
    let slots: Vec<(Row, i64)> = model
        .iter()
        .flat_map(|(r, &w)| std::iter::repeat_n((*r, 1), w as usize))
        .collect();
    let b = batch(&slots);
    let mb = b.as_mem_batch();
    let locs = order_locators(keys, &schema(), true);
    let mut order: Vec<usize> = (0..slots.len()).collect();
    order.sort_by(|&x, &y| cmp_order_keys(&locs, &mb, x, y));
    let mut groups: BTreeMap<Cells, Vec<Cells>> = BTreeMap::new();
    for i in order {
        let row = cells(&slots[i].0);
        let key = group_cols.iter().map(|&c| row[c as usize].clone()).collect();
        groups.entry(key).or_default().push(row);
    }
    let mut want = BTreeMap::new();
    for rows in groups.into_values() {
        for row in rows.into_iter().skip(offset as usize).take(limit as usize) {
            *want.entry((row, Vec::new())).or_insert(0) += 1;
        }
    }
    want
}

fn arb_bound() -> impl Strategy<Value = u64> {
    prop_oneof![4 => 0u64..4, 1 => Just(u64::MAX)]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    /// After every epoch the maintained output is the window over the integrated
    /// input, weights included, under every group key form: V₀, the input's PK, a
    /// non-null column's image, and the fold of a nullable or multi-column set.
    #[test]
    fn the_maintained_window_is_the_window_of_the_integral(
        gi in 0usize..5,
        keys in arb_keys(),
        limit in arb_bound().prop_map(|l| l.max(1)),
        offset in arb_bound(),
        ticks in arb_ticks(arb_row()),
    ) {
        let group_cols: &[u32] = [&[][..], &[0], &[1], &[2], &[1, 3]][gi];
        let plan = OrderedPlan::top_n(&schema(), group_cols, &keys, limit, offset).unwrap();
        let mut h = Harness::new(plan);
        let mut model = BTreeMap::new();
        for targets in &ticks {
            h.tick(&batch(&delta_to(&mut model, targets)));
            prop_assert_eq!(h.state(group_cols), window(&model, group_cols, &keys, limit, offset));
        }
    }

    /// Two workers' partial windows, relayed to one combine, maintain the global
    /// window — an OFFSET included, which a partial's window has to cover.
    #[test]
    fn two_workers_partials_combine_to_the_global_window(
        keys in arb_keys(),
        limit in arb_bound().prop_map(|l| l.max(1)),
        offset in arb_bound(),
        ticks in arb_ticks(arb_row()),
    ) {
        let partial = || Harness::new(OrderedPlan::top_n_partial(&schema(), &keys, limit, offset).unwrap());
        let mut workers = [partial(), partial()];
        let partials = workers[0].plan.output_schema;
        let mut combine = Harness::new(OrderedPlan::top_n_combine(&partials, &keys, limit, offset).unwrap());
        let mut model = BTreeMap::new();
        for targets in &ticks {
            let delta = delta_to(&mut model, targets);
            // Sharded by id, as the exchange routes a keyed input.
            let relayed: Vec<Batch> = (0..2)
                .map(|w| {
                    let mine: Vec<_> = delta.iter().copied().filter(|r| r.0 .0 % 2 == w).collect();
                    workers[w as usize].tick(&batch(&mine))
                })
                .collect();
            combine.tick(&Batch::concat(&partials, relayed.iter().map(Batch::as_mem_batch)));
            prop_assert_eq!(combine.state(&[]), window(&model, &[], &keys, limit, offset));
        }
    }
}

/// Over a nullable key the rank byte places NULLs where `nulls_first` says, and
/// DESC reverses the values but not the rank — per group.
#[test]
fn a_nullable_key_places_nulls_by_nulls_first_in_either_direction() {
    let rows: [(Row, i64); 5] = [
        ((1, 0, None, None, 0), 1),
        ((2, 0, Some(-3), None, 0), 1),
        ((3, 0, Some(4), None, 0), 1),
        ((4, 1, Some(9), None, 0), 1),
        ((5, 1, None, None, 0), 1),
    ];
    for (desc, nulls_first, want) in [
        (false, false, [2, 4]),
        (false, true, [1, 5]),
        (true, false, [3, 4]),
        (true, true, [1, 5]),
    ] {
        let key = OrderKey { col: 2, desc, nulls_first };
        let mut h = Harness::new(OrderedPlan::top_n(&schema(), &[1], &[key], 1, 0).unwrap());
        h.tick(&batch(&rows));
        let ids: Vec<Vec<u8>> = h
            .state(&[1])
            .into_keys()
            .map(|(row, _)| row[0].clone().unwrap())
            .collect();
        let want: Vec<Vec<u8>> = want.iter().map(|id: &u64| id.to_le_bytes().to_vec()).collect();
        assert_eq!(ids, want, "desc={desc} nulls_first={nulls_first}");
    }
}

/// A non-null fixed-width key sits whole in the index PK, with no rank byte and
/// no image payload repeating it: values differing only in the low byte get
/// distinct PKs.
#[test]
fn a_fixed_width_key_sits_whole_in_the_index_pk() {
    let key = OrderKey { col: 1, desc: false, nulls_first: false };
    let plan = OrderedPlan::top_n(&schema(), &[], &[key], 1, 0).unwrap();
    assert_eq!(plan.index.schema.pk_stride(), 8, "no group, no rank, the whole image");
    assert_eq!(
        plan.index.schema.num_payload_cols(),
        plan.output_schema.num_payload_cols(),
        "the carried columns alone"
    );
    let idx = plan.index.batch(&batch(&[
        ((1, 0x100, None, None, 0), 1),
        ((2, 0x101, None, None, 0), 1),
    ]));
    let mb = idx.as_mem_batch();
    assert_ne!(mb.get_pk_bytes(0), mb.get_pk_bytes(1));
}

#[test]
fn from_wire_rejects_what_it_cannot_run() {
    let s = schema();
    let order = [OrderKey { col: 2, desc: false, nulls_first: false }];
    let err = |r: Result<OrderedPlan, String>| r.err().expect("refused");
    assert!(err(OrderedPlan::top_n(&s, &[], &order, 0, 0)).contains("zero limit"));
    assert!(err(OrderedPlan::top_n(&s, &[], &[], 1, 0)).contains("no order keys"));
    let oob = [OrderKey { col: 9, ..order[0] }];
    assert!(err(OrderedPlan::top_n(&s, &[], &oob, 1, 0)).contains("order column"));
}
