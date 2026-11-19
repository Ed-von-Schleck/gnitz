//! Top-N operator tests, driven epoch by epoch through the production populate
//! and integrate paths against a brute-force top-N of the integrated input.

use std::collections::BTreeMap;

use proptest::prelude::*;

use crate::stream::ordered::tests::{Harness, arb_keys, ordered_groups};
use crate::test_support::stream_input::{
    Cells, GROUPS, Row, arb_row, arb_ticks, batch, cells, delta_to, schema, shard,
};
use gnitz_wire::{OrderKey, WeightSlots};
use gnitz_zset::repr::Batch;

use crate::stream::ordered::OrderedPlan;

/// Weight slots `skip .. skip + keep` of each group of `model`, ordered
/// by `keys` and then by every column in schema order — the index's tie-break,
/// which is the identity order an ad-hoc `ORDER BY` cuts by once the input
/// leads with its key.
fn top_n_of(
    model: &BTreeMap<Row, i64>,
    group_cols: &[u32],
    keys: &[OrderKey],
    window: WeightSlots,
) -> BTreeMap<(Cells, Cells), i64> {
    // One entry per weight slot.
    let slots: Vec<(Row, i64)> = model
        .iter()
        .flat_map(|(r, &w)| std::iter::repeat_n((*r, 1), w as usize))
        .collect();
    let tie_break = (0..schema().num_columns() as u32).map(|col| OrderKey { col, desc: false, nulls_first: true });
    let order_keys: Vec<OrderKey> = keys.iter().copied().chain(tie_break).collect();
    let (_, _, groups) = ordered_groups(&slots, group_cols, &order_keys);
    let mut want = BTreeMap::new();
    for rows in groups {
        for &i in rows.iter().skip(window.skip as usize).take(window.keep as usize) {
            *want.entry((cells(&slots[i].0), Vec::new())).or_insert(0) += 1;
        }
    }
    want
}

/// A window keeping at least one slot, either bound small or absent.
fn arb_slots() -> impl Strategy<Value = WeightSlots> {
    let bound = || prop_oneof![4 => 0u64..4, 1 => Just(u64::MAX)];
    (bound(), bound()).prop_map(|(skip, keep)| WeightSlots { skip, keep: keep.max(1) })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    /// After every epoch the maintained output is the top-N of the integrated
    /// input, weights included, under every group key form of [`GROUPS`].
    #[test]
    fn the_maintained_top_n_is_the_top_n_of_the_integral(
        group_cols in prop::sample::select(&GROUPS[..]),
        keys in arb_keys(),
        slots in arb_slots(),
        ticks in arb_ticks(arb_row()),
    ) {
        let plan = OrderedPlan::top_n(&schema(), group_cols, &keys, slots).unwrap();
        let mut h = Harness::new(plan);
        let mut model = BTreeMap::new();
        for targets in &ticks {
            h.tick(&batch(&delta_to(&mut model, targets)));
            prop_assert_eq!(h.state(group_cols), top_n_of(&model, group_cols, &keys, slots));
        }
    }

    /// Two workers' partial top-Ns, relayed to one combine, maintain the global
    /// top-N — an OFFSET included, which a partial's slots have to cover.
    #[test]
    fn two_workers_partials_combine_to_the_global_top_n(
        keys in arb_keys(),
        slots in arb_slots(),
        ticks in arb_ticks(arb_row()),
    ) {
        let partial = || Harness::new(OrderedPlan::top_n_partial(&schema(), &keys, slots).unwrap());
        let mut workers = [partial(), partial()];
        let partials = *workers[0].plan.output_schema();
        let mut combine = Harness::new(OrderedPlan::top_n_combine(&partials, &keys, slots).unwrap());
        let mut model = BTreeMap::new();
        for targets in &ticks {
            let delta = delta_to(&mut model, targets);
            let relayed: Vec<Batch> = (0..2).map(|w| workers[w as usize].tick(&shard(&delta, w))).collect();
            combine.tick(&Batch::concat(&partials, relayed.iter().map(Batch::as_mem_batch)));
            prop_assert_eq!(combine.state(&[]), top_n_of(&model, &[], &keys, slots));
        }
    }
}

/// Over a nullable key the rank byte places NULLs where `nulls_first` says, and
/// DESC reverses the values but not the rank — per group, each kept row at its
/// weight.
#[test]
fn a_nullable_key_places_nulls_by_nulls_first_in_either_direction() {
    let row = |id, grp, val| (Row { id, grp, val, ..Row::default() }, 1);
    let rows = [
        row(1, 0, None),
        row(2, 0, Some(-3)),
        row(3, 0, Some(4)),
        row(4, 1, Some(9)),
        row(5, 1, None),
    ];
    for (desc, nulls_first, want) in [
        (false, false, [2, 4]),
        (false, true, [1, 5]),
        (true, false, [3, 4]),
        (true, true, [1, 5]),
    ] {
        let key = OrderKey { col: 0, desc, nulls_first };
        let mut h =
            Harness::new(OrderedPlan::top_n(&schema(), &[3], &[key], WeightSlots { skip: 0, keep: 1 }).unwrap());
        h.tick(&batch(&rows));
        let mut got: Vec<(u64, i64)> = h
            .state(&[3])
            .into_iter()
            .map(|((row, _), w)| (u64::from_le_bytes(row[1].clone().unwrap().try_into().unwrap()), w))
            .collect();
        got.sort();
        assert_eq!(got, want.map(|id| (id, 1)), "desc={desc} nulls_first={nulls_first}");
    }
}
