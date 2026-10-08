//! Top-N operator tests, driven epoch by epoch through the production populate
//! and integrate paths against a brute-force window over the integrated input.

use std::collections::BTreeMap;

use proptest::prelude::*;

use crate::repr::{Batch, BatchBuilder};
use crate::schema::{SchemaColumn, SchemaDescriptor, TypeCode};
use crate::test_support::{arb_ticks, assert_folds, cell, delta_to, TestTrace, FLOATS, STRS};
use gnitz_expr::{cmp_order_keys, order_locators};
use gnitz_wire::{OrderKey, ReduceOutSlot};

use super::op_topn::{op_topn, TopNEpoch};
use super::plan::TopNPlan;

/// `[U64 id (pk), I64 grp, I64 val NULL, STRING s NULL, F64 f]`.
fn schema() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::I64, false),
            SchemaColumn::new(TypeCode::I64, true),
            SchemaColumn::new(TypeCode::String, true),
            SchemaColumn::new(TypeCode::F64, false),
        ],
        &[0],
    )
}

/// `(id, grp, val, s, f)`, `f` as its `f64` bits.
type Row = (u64, i64, Option<i64>, Option<&'static str>, u64);
/// A row as one native cell per input column, `None` for NULL.
type Cells = Vec<Option<Vec<u8>>>;

fn cells(&(id, grp, val, s, f): &Row) -> Cells {
    vec![
        Some(id.to_le_bytes().to_vec()),
        Some(grp.to_le_bytes().to_vec()),
        val.map(|v| v.to_le_bytes().to_vec()),
        s.map(|s| s.as_bytes().to_vec()),
        Some(f.to_le_bytes().to_vec()),
    ]
}

fn batch(rows: &[(Row, i64)]) -> Batch {
    let mut b = BatchBuilder::new(&schema());
    for &((id, grp, val, s, f), w) in rows {
        b.begin_row(id as u128, w);
        b.put_int(grp as u128);
        b.put_opt_int(val.map(|v| v as u128));
        match s {
            Some(s) => b.put_string(s),
            None => b.put_null(),
        }
        b.put_u64(f);
        b.end_row();
    }
    b.finish()
}

/// The operator with its index, driven the way the VM drives it.
pub(super) struct Harness {
    pub(super) plan: TopNPlan,
    pub(super) index: TestTrace,
    /// The integral of the output, which only the test reads.
    out: TestTrace,
    /// Output column → the input column it holds; `None` for a synthetic key.
    layout: Vec<Option<usize>>,
}

impl Harness {
    pub(super) fn new(plan: TopNPlan, group_cols: &[u32]) -> Self {
        let index = TestTrace::new(plan.index.schema);
        let out = TestTrace::new(plan.output_schema);
        let layout = schema()
            .reduce_out_key(group_cols)
            .output_layout(group_cols, 0..5)
            .into_iter()
            .map(|s| match s {
                ReduceOutSlot::SyntheticKey => None,
                ReduceOutSlot::Key(c) | ReduceOutSlot::Carried(c) => Some(c as usize),
            })
            .collect();
        Harness { plan, index, out, layout }
    }

    /// One epoch: run, add the returned entries to the index, integrate the
    /// output. Returns the raw delta.
    fn tick(&mut self, delta: &Batch) -> Batch {
        let index = &self.index;
        let TopNEpoch { out, index_entries: entries } =
            op_topn(delta, &mut |first, last| index.cursor_within(first, last), &self.plan);
        assert_folds(
            &[self.plan.index.batch(delta, self.plan.key.carried())],
            &entries,
            "op_topn's index entries",
        );
        self.index.ingest(entries);
        assert_folds(std::slice::from_ref(&out), &out, "op_topn's delta");
        self.out.ingest(out.clone());
        out
    }

    /// The maintained output, each row read back as the input row it carries.
    fn state(&self) -> BTreeMap<Cells, i64> {
        let b = self.out.cursor().materialize();
        let mb = b.as_mem_batch();
        let out = self.plan.output_schema;
        (0..b.count)
            .map(|r| {
                let mut row = vec![None; 5];
                for (oc, ic) in self.layout.iter().enumerate() {
                    let Some(ic) = *ic else { continue };
                    row[ic] = cell(&mb, out.locate(oc), r);
                }
                (row, b.get_weight(r))
            })
            .collect()
    }
}

/// Weight slots `offset .. offset + limit` of each group of `model`, in the
/// read path's order: `keys`, then the identity tie-break an ad-hoc `ORDER BY`
/// cuts by.
fn window(
    model: &BTreeMap<Row, i64>,
    group_cols: &[u32],
    keys: &[OrderKey],
    limit: u64,
    offset: u64,
) -> BTreeMap<Cells, i64> {
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
            *want.entry(row).or_insert(0) += 1;
        }
    }
    want
}

fn arb_row() -> impl Strategy<Value = Row> {
    (
        0u64..4,
        0i64..2,
        prop::option::of(prop_oneof![Just(i64::MIN), -1i64..=1]),
        prop::option::of(prop::sample::select(&STRS[..])),
        prop::sample::select(&FLOATS[..]).prop_map(f64::to_bits),
    )
}

fn arb_keys() -> impl Strategy<Value = Vec<OrderKey>> {
    prop::collection::vec(
        (0u16..5, any::<bool>(), any::<bool>()).prop_map(|(col, desc, nulls_first)| OrderKey {
            col,
            desc,
            nulls_first,
        }),
        1..=3,
    )
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
        let plan = TopNPlan::from_wire(&schema(), group_cols, &keys, limit, offset).unwrap();
        let mut h = Harness::new(plan, group_cols);
        let mut model = BTreeMap::new();
        for targets in &ticks {
            h.tick(&batch(&delta_to(&mut model, targets)));
            prop_assert_eq!(h.state(), window(&model, group_cols, &keys, limit, offset));
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
        let partial = || Harness::new(TopNPlan::partial(&schema(), &keys, limit, offset).unwrap(), &[]);
        let mut workers = [partial(), partial()];
        let partials = workers[0].plan.output_schema;
        let mut combine = Harness::new(TopNPlan::combine(&partials, &keys, limit, offset).unwrap(), &[]);
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
            prop_assert_eq!(combine.state(), window(&model, &[], &keys, limit, offset));
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
        let mut h = Harness::new(TopNPlan::from_wire(&schema(), &[1], &[key], 1, 0).unwrap(), &[1]);
        h.tick(&batch(&rows));
        let ids: Vec<Vec<u8>> = h.state().into_keys().map(|row| row[0].clone().unwrap()).collect();
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
    let plan = TopNPlan::from_wire(&schema(), &[], &[key], 1, 0).unwrap();
    assert_eq!(plan.index.schema.pk_stride(), 8, "no group, no rank, the whole image");
    assert_eq!(
        plan.index.schema.num_payload_cols(),
        plan.output_schema.num_payload_cols(),
        "the carried columns alone"
    );
    let idx = plan.index.batch(
        &batch(&[((1, 0x100, None, None, 0), 1), ((2, 0x101, None, None, 0), 1)]),
        plan.key.carried(),
    );
    let mb = idx.as_mem_batch();
    assert_ne!(mb.get_pk_bytes(0), mb.get_pk_bytes(1));
}

#[test]
fn from_wire_rejects_what_it_cannot_run() {
    let s = schema();
    let order = [OrderKey { col: 2, desc: false, nulls_first: false }];
    let err = |r: Result<TopNPlan, String>| r.err().expect("refused");
    assert!(err(TopNPlan::from_wire(&s, &[], &order, 0, 0)).contains("zero limit"));
    assert!(err(TopNPlan::from_wire(&s, &[], &[], 1, 0)).contains("no order keys"));
    let oob = [OrderKey { col: 9, ..order[0] }];
    assert!(err(TopNPlan::from_wire(&s, &[], &oob, 1, 0)).contains("order column"));
}
