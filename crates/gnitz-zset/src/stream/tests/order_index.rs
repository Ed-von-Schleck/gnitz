//! The fixture of the operators maintained through an order index: a schema of
//! every column form, and a harness driving an operator epoch by epoch through
//! the production populate and integrate paths.

use std::collections::BTreeMap;

use proptest::prelude::*;

use super::{OrderedEpoch, OrderedPlan};
use crate::repr::{Batch, BatchBuilder};
use crate::schema::{SchemaColumn, SchemaDescriptor};
use crate::test_support::{cell, TestTrace, FLOATS, STRS};
use gnitz_foundation::perf::Counter;
use gnitz_wire::{OrderKey, ReduceOutSlot, TypeCode};

/// `[U64 id (pk), I64 grp, I64 val NULL, STRING s NULL, F64 f]`.
pub(in crate::stream) fn schema() -> SchemaDescriptor {
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
pub(in crate::stream) type Row = (u64, i64, Option<i64>, Option<&'static str>, u64);
/// A row as one native cell per column, `None` for NULL.
pub(in crate::stream) type Cells = Vec<Option<Vec<u8>>>;

pub(in crate::stream) fn cells(&(id, grp, val, s, f): &Row) -> Cells {
    vec![
        Some(id.to_le_bytes().to_vec()),
        Some(grp.to_le_bytes().to_vec()),
        val.map(|v| v.to_le_bytes().to_vec()),
        s.map(|s| s.as_bytes().to_vec()),
        Some(f.to_le_bytes().to_vec()),
    ]
}

pub(in crate::stream) fn batch(rows: &[(Row, i64)]) -> Batch {
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

pub(in crate::stream) fn arb_row() -> impl Strategy<Value = Row> {
    (
        0u64..4,
        0i64..2,
        prop::option::of(prop_oneof![Just(i64::MIN), -1i64..=1]),
        prop::option::of(prop::sample::select(&STRS[..])),
        prop::sample::select(&FLOATS[..]).prop_map(f64::to_bits),
    )
}

pub(in crate::stream) fn arb_keys() -> impl Strategy<Value = Vec<OrderKey>> {
    prop::collection::vec(
        (0u16..5, any::<bool>(), any::<bool>()).prop_map(|(col, desc, nulls_first)| OrderKey {
            col,
            desc,
            nulls_first,
        }),
        1..=3,
    )
}

/// The operator with its index, driven the way the VM drives it.
pub(in crate::stream) struct Harness {
    pub(in crate::stream) plan: OrderedPlan,
    index: TestTrace,
    /// The integral of the output, which only the test reads.
    out: TestTrace,
    /// The epochs [`Self::measure`] ran.
    measured: usize,
}

impl Harness {
    pub(in crate::stream) fn new(plan: OrderedPlan) -> Self {
        let index = TestTrace::new(*plan.index_schema());
        let out = TestTrace::new(*plan.output_schema());
        Harness { plan, index, out, measured: 0 }
    }

    /// One epoch: run, add the returned entries to the index, integrate the
    /// output. Returns the raw delta.
    pub(in crate::stream) fn tick(&mut self, delta: &Batch) -> Batch {
        let index = &self.index;
        let OrderedEpoch { out, index_entries: entries } = self
            .plan
            .epoch(delta, &mut |first, last| index.cursor_within(first, last));
        self.index.ingest(entries);
        let elements = out.clone().into_consolidated().count;
        assert_eq!(out.count, elements, "the epoch's delta holds an element once");
        self.out.ingest(out.clone());
        out
    }

    /// The maintained output of an operator over [`schema`] grouped on
    /// `group_cols`: each row as the [`schema`] row it carries, then the
    /// columns the operator adds.
    pub(in crate::stream) fn state(&self, group_cols: &[u32]) -> BTreeMap<(Cells, Cells), i64> {
        let layout = schema().reduce_out_key(group_cols).output_layout(group_cols, 0..5);
        let b = self.out.cursor().materialize();
        let mb = b.as_mem_batch();
        let out = self.plan.output_schema();
        (0..b.count)
            .map(|r| {
                let mut row = vec![None; 5];
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
    pub(in crate::stream) fn measure(&mut self, counter: &Counter, delta: &Batch) -> u64 {
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
}
