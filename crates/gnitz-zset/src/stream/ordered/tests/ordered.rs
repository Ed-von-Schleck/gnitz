//! The order index's own layout and refusal tests, and the fixture of the
//! operators maintained through it over the stream input model: order keys
//! over its columns, a harness driving an operator epoch by epoch through the
//! production populate and integrate paths, and the benches' row builder.

use std::collections::BTreeMap;
use std::ops::Range;

use proptest::prelude::*;

use super::{OrderedEpoch, OrderedPlan};
use crate::repr::{Batch, BatchBuilder};
use crate::schema::SchemaDescriptor;
use crate::test_support::stream_input::{batch, schema, Cells, Row};
use crate::test_support::{cell, TestTrace};
use gnitz_expr::{cmp_order_keys, order_locators, OrderLocator, ReduceOutSlot};
use gnitz_foundation::perf::Counter;
use gnitz_wire::OrderKey;

/// One to three order keys, over any column of [`schema`].
pub(super) fn arb_keys() -> impl Strategy<Value = Vec<OrderKey>> {
    prop::collection::vec(
        (0..schema().num_columns() as u16, any::<bool>(), any::<bool>())
            .prop_map(|(col, desc, nulls_first)| OrderKey { col, desc, nulls_first }),
        1..=3,
    )
}

/// `rows` by group of `group_cols`, each group in `keys` order: the batch of
/// `rows` they were ranked in, its locators of `keys`, and per group its rows'
/// positions in `rows`.
pub(super) fn ordered_groups(
    rows: &[(Row, i64)],
    group_cols: &[u32],
    keys: &[OrderKey],
) -> (Batch, Vec<OrderLocator>, Vec<Vec<usize>>) {
    let b = batch(rows);
    let locs = order_locators(keys, &schema(), false);
    let mut order: Vec<usize> = (0..rows.len()).collect();
    order.sort_by(|&x, &y| cmp_order_keys(&locs, &b.as_mem_batch(), x, y));
    let mut groups: BTreeMap<Cells, Vec<usize>> = BTreeMap::new();
    for i in order {
        groups.entry(rows[i].0.group_key(group_cols)).or_default().push(i);
    }
    (b, locs, groups.into_values().collect())
}

/// The operator with its index, driven the way the VM drives it.
pub(super) struct Harness {
    pub(super) plan: OrderedPlan,
    index: TestTrace,
    /// The integral of the output, which only the test reads.
    out: TestTrace,
    /// The epochs [`Self::measure`] ran.
    measured: usize,
}

impl Harness {
    pub(super) fn new(plan: OrderedPlan) -> Self {
        let index = TestTrace::new(*plan.index_schema());
        let out = TestTrace::new(*plan.output_schema());
        Harness { plan, index, out, measured: 0 }
    }

    /// One epoch: run, add the returned entries to the index, integrate the
    /// output. Returns the raw delta.
    pub(super) fn tick(&mut self, delta: &Batch) -> Batch {
        let index = &self.index;
        let OrderedEpoch { out, index_entries: entries } = self
            .plan
            .epoch(delta, &mut |first, last| index.cursor_within(first, last));
        self.index.ingest(entries);
        let elements = out.clone().into_consolidated().len();
        assert_eq!(out.len(), elements, "the epoch's delta holds an element once");
        self.out.ingest(out.clone());
        out
    }

    /// The maintained output of an operator over [`schema`] grouped on
    /// `group_cols`: each row as the [`schema`] row it carries, then the
    /// columns the operator adds.
    pub(super) fn state(&self, group_cols: &[u32]) -> BTreeMap<(Cells, Cells), i64> {
        let n = schema().num_columns();
        let layout = schema()
            .reduce_out_key(group_cols)
            .output_layout(group_cols, 0..n as u32);
        let b = self.out.cursor().materialize();
        let mb = b.as_mem_batch();
        let out = self.plan.output_schema();
        (0..b.len())
            .map(|r| {
                let mut row = vec![None; n];
                for (oc, slot) in layout.iter().enumerate() {
                    if let ReduceOutSlot::Key(ic) | ReduceOutSlot::Carried(ic) = *slot {
                        row[ic as usize] = cell(&mb, out.locate(oc), r);
                    }
                }
                let added = (layout.len()..out.num_columns())
                    .map(|c| cell(&mb, out.locate(c), r))
                    .collect();
                ((row, added), b.get_weight(r))
            })
            .collect()
    }

    /// One whole epoch over `delta` — its index entries, their fold and the walk
    /// — in instructions. The index is refolded to one run every eighth epoch,
    /// outside the measured region.
    pub(super) fn measure(&mut self, counter: &Counter, delta: &Batch) -> u64 {
        let Harness { plan, index, .. } = self;
        let (epoch, instructions) = counter
            .measure(|| std::hint::black_box(plan.epoch(delta, &mut |first, last| index.cursor_within(first, last))));
        index.ingest(epoch.index_entries);
        self.measured += 1;
        if self.measured.is_multiple_of(8) {
            let all = index.cursor().materialize();
            *index = TestTrace::new(*plan.index_schema());
            index.ingest((*all).clone());
        }
        instructions
    }

    /// Three bulk epochs — `first` into an empty index, `second` beside it, and
    /// the retraction of `first` — in instructions per delta row.
    pub(super) fn bulk_epochs(&mut self, counter: &Counter, first: &Batch, second: &Batch) -> [f64; 3] {
        let retraction = first.clone().negated().into_consolidated();
        [first, second, &retraction].map(|d| self.measure(counter, d) as f64 / d.len() as f64)
    }
}

/// One row of `schema` per id, at weight `w`: column 1 the group, column 2 the
/// key, every later column the id.
pub(super) fn bench_rows(
    schema: &SchemaDescriptor,
    ids: Range<u64>,
    group: impl Fn(u64) -> u64,
    key: impl Fn(u64) -> u64,
    w: i64,
) -> Batch {
    let mut b = BatchBuilder::new(schema);
    for i in ids {
        b.begin_row(i as u128, w);
        b.put_int(group(i) as u128);
        b.put_u64(key(i));
        for _ in 3..schema.num_columns() {
            b.put_u64(i);
        }
        b.end_row();
    }
    b.finish().into_consolidated()
}

/// A non-null fixed-width key sits whole in the index PK, with no rank byte and
/// no image payload repeating it: values differing only in the low byte get
/// distinct PKs.
#[test]
fn a_fixed_width_key_sits_whole_in_the_index_pk() {
    let key = OrderKey { col: 3, desc: false, nulls_first: false };
    let plan = OrderedPlan::top_n(&schema(), &[], &[key], 1, 0).unwrap();
    assert_eq!(plan.index_schema().pk_stride(), 8, "no group, no rank, the whole image");
    assert_eq!(
        plan.index_schema().num_payload_cols(),
        plan.output_schema().num_payload_cols(),
        "the carried columns alone"
    );
    let row = |id, grp| (Row { id, grp, ..Row::default() }, 1);
    let idx = plan.index.entries(&batch(&[row(1, 0x100), row(2, 0x101)]));
    let mb = idx.as_mem_batch();
    assert_ne!(mb.get_pk_bytes(0), mb.get_pk_bytes(1));
}

#[test]
fn a_shape_the_order_index_cannot_hold_is_refused() {
    let s = schema();
    let err = |r: Result<OrderedPlan, String>| r.err().expect("refused");
    assert!(err(OrderedPlan::top_n(&s, &[], &[], 1, 0)).contains("no order keys"));
    let oob = [OrderKey { col: 9, desc: false, nulls_first: false }];
    assert!(err(OrderedPlan::top_n(&s, &[], &oob, 1, 0)).contains("order column"));
}
