//! The fixture of the operators maintained through an order index: a schema of
//! every column form, and a harness driving an operator epoch by epoch through
//! the production populate and integrate paths.

use std::collections::BTreeMap;

use proptest::prelude::*;

use super::{OrderIndex, OrderedEpoch, OrderedPlan};
use crate::repr::{Batch, BatchBuilder};
use crate::schema::{SchemaColumn, SchemaDescriptor, TypeCode};
use crate::test_support::{assert_folds, cell, TestTrace, FLOATS, STRS};
use gnitz_foundation::perf::Counter;
use gnitz_wire::{OrderKey, ReduceOutSlot};

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
    /// Output column → the [`schema`] column it holds; `None` for a synthetic key.
    layout: Vec<Option<usize>>,
    /// The epochs [`Self::measure`] ran.
    measured: usize,
}

impl Harness {
    pub(in crate::stream) fn new(plan: OrderedPlan, group_cols: &[u32]) -> Self {
        let index = TestTrace::new(*plan.index_schema());
        let out = TestTrace::new(*plan.output_schema());
        let layout = schema()
            .reduce_out_key(group_cols)
            .output_layout(group_cols, 0..5)
            .into_iter()
            .map(|s| match s {
                ReduceOutSlot::SyntheticKey => None,
                ReduceOutSlot::Key(c) | ReduceOutSlot::Carried(c) => Some(c as usize),
            })
            .collect();
        Harness { plan, index, out, layout, measured: 0 }
    }

    /// One epoch: run, add the returned entries to the index, integrate the
    /// output. Returns the raw delta.
    pub(in crate::stream) fn tick(&mut self, delta: &Batch) -> Batch {
        let index = &self.index;
        let OrderedEpoch { out, index_entries: entries } = self
            .plan
            .epoch(delta, &mut |first, last| index.cursor_within(first, last));
        assert_folds(&[self.plan.index.batch(delta)], &entries, "the epoch's index entries");
        self.index.ingest(entries);
        let elements = out.clone().into_consolidated().count;
        assert_eq!(out.count, elements, "the epoch's delta holds an element once");
        self.out.ingest(out.clone());
        out
    }

    /// The maintained output: each row as the [`schema`] row it carries, then
    /// the columns the operator adds.
    pub(in crate::stream) fn state(&self) -> BTreeMap<(Cells, Cells), i64> {
        let b = self.out.cursor().materialize();
        let mb = b.as_mem_batch();
        let out = self.plan.output_schema();
        (0..b.count)
            .map(|r| {
                let mut row = vec![None; 5];
                for (oc, ic) in self.layout.iter().enumerate() {
                    let Some(ic) = *ic else { continue };
                    row[ic] = cell(&mb, out.locate(oc), r);
                }
                let added = (self.layout.len()..out.num_columns())
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

/// The entries of `delta` built a row at a time, each cell through the row
/// writer: what [`OrderIndex::batch`] builds a region at a time.
fn row_at_a_time(index: &OrderIndex, delta: &Batch) -> Batch {
    let mb = delta.as_mem_batch();
    let mut out = Batch::with_capacity(&index.schema, delta.count.max(1));
    let stride = index.key_packer.out_stride;
    let carried: Vec<crate::schema::ColumnLocator> = index.copies.iter().map(|c| c.src).collect();
    let mut image = Vec::new();
    index
        .key_packer
        .for_each_key(&mb, index.schema.pk_stride(), |row, key| {
            image.clear();
            index.lead.write_lead(&mb, row, &mut key[stride..], &mut image);
            out.begin_row(key, mb.get_weight(row));
            let mut col = 0;
            if !index.lead.image.fits_slot() {
                out.extend_col_blob(col, &image);
                col += 1;
            }
            for spec in &index.rest {
                image.clear();
                spec.append_image(&mb, row, &mut image);
                out.extend_col_blob(col, &image);
                col += 1;
            }
            out.append_cells_from(col, &carried, &mb, row);
            out.commit_row();
        });
    out
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// Row for row, a region-at-a-time entry is the row-at-a-time one: its key,
    /// its weight, its null word and every cell — over deltas that repeat rows
    /// and hold ghosts, under every group key form and every carried column.
    #[test]
    fn a_region_at_a_time_entry_is_the_row_at_a_time_one(
        gi in 0usize..6,
        keys in arb_keys(),
        also in prop::collection::vec(0u32..5, 0..4),
        rows in prop::collection::vec((arb_row(), -2i64..=2), 0..40),
    ) {
        let group_cols: &[u32] = [&[][..], &[0], &[1], &[2], &[1, 3], &[2, 1]][gi];
        let (index, _) = OrderIndex::new(&schema(), group_cols, &keys, also).unwrap();
        let delta = batch(&rows);
        let (built, want) = (index.batch(&delta), row_at_a_time(&index, &delta));
        prop_assert_eq!(built.count, want.count);
        let (a, b) = (built.as_mem_batch(), want.as_mem_batch());
        for i in 0..built.count {
            prop_assert_eq!(a.get_pk_bytes(i), b.get_pk_bytes(i), "row {}", i);
            prop_assert_eq!(a.get_weight(i), b.get_weight(i), "row {}", i);
            prop_assert_eq!(
                gnitz_wire::RowSource::get_null_word(&a, i),
                gnitz_wire::RowSource::get_null_word(&b, i),
                "row {}", i
            );
            let ord = crate::schema::payload_order::compare_full_rows(&index.schema, &a, i, &b, i);
            prop_assert_eq!(ord, std::cmp::Ordering::Equal, "row {}", i);
            for c in 0..index.schema.num_columns() {
                let loc = index.schema.locate(c);
                prop_assert_eq!(cell(&a, loc, i), cell(&b, loc, i), "row {} column {}", i, c);
            }
        }
        assert_folds(std::slice::from_ref(&want), &built.to_consolidated(), "the entries");
    }
}
