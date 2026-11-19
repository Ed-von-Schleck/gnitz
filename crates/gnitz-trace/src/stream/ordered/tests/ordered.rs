//! The order index's own layout and refusal tests, and the fixture of the
//! operators maintained through it over the stream input model: order keys
//! over its columns, a harness driving an operator epoch by epoch through the
//! production populate and integrate paths, and the benches' row builder.

use std::collections::BTreeMap;
use std::ops::Range;

use proptest::prelude::*;

use super::{OrderIndex, OrderedEpoch, OrderedPlan};
use crate::test_support::stream_input::{Cells, Row, batch, schema};
use crate::test_support::{TestTrace, cell};
use gnitz_expr::{OrderLocator, ReduceOutSlot, cmp_order_keys, order_locators};
use gnitz_foundation::perf::Counter;
use gnitz_foundation::rng::mix;
use gnitz_wire::{OrderKey, TypeCode, WeightSlots};
use gnitz_zset::algebra::copy_columns;
use gnitz_zset::repr::{Batch, BatchBuilder};
use gnitz_zset::schema::{SchemaColumn, SchemaDescriptor};

/// One to three order keys, over any column of [`schema`].
pub(super) fn arb_keys() -> impl Strategy<Value = Vec<OrderKey>> {
    prop::collection::vec(
        (0..schema().num_columns() as u32, any::<bool>(), any::<bool>())
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
    let locs = order_locators(keys, &schema(), false).unwrap();
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
        let layout = schema().reduce_out_layout(group_cols, 0..n as u32);
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
    let plan = OrderedPlan::top_n(&schema(), &[], &[key], WeightSlots { skip: 0, keep: 1 }).unwrap();
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
    assert!(err(OrderedPlan::top_n(&s, &[], &[], WeightSlots { skip: 0, keep: 1 })).contains("no order keys"));
    let oob = [OrderKey { col: 9, desc: false, nulls_first: false }];
    assert!(err(OrderedPlan::top_n(&s, &[], &oob, WeightSlots { skip: 0, keep: 1 })).contains("order column"));
}

/// [`OrderIndex::entries`] a row at a time: each row's lead and every other
/// key's image built through the row accessors and string-encoded one by one.
fn entries_by_row(index: &OrderIndex, delta: &Batch) -> Batch {
    let mb = delta.as_mem_batch();
    let n = delta.len();
    let mut out = Batch::with_capacity(&index.schema, n);
    let heap_at = match index.moves.string_slots() {
        0 => None,
        strings => out.carry_heap(&mb, strings, &[(0, n)], 0),
    };
    let (stride, group) = (index.schema.pk_stride(), index.key_packer.out_stride);
    let mut image = Vec::new();
    out.write_rows(n, |w| {
        index.key_packer.pack_rows(w.pk_mut(), stride, &mb, &[(0, n)]);
        let (keys, cells, blob) = w.pk_and_payload_mut(0..index.images.len());
        let mut cells: Vec<_> = cells.collect();
        for (row, key) in keys.chunks_exact_mut(stride).enumerate() {
            let mut cells = cells.iter_mut().map(|col| &mut col[row * 16..][..16]);
            let mut put = |image: &[u8]| {
                let cell = cells.next().expect("a cell per image column");
                cell.copy_from_slice(&gnitz_wire::encode_german_string(image, blob));
            };
            image.clear();
            index.lead.write_lead(&mb, row, &mut key[group..], &mut image);
            if !index.lead.image.fits_slot() {
                put(&image);
            }
            for spec in &index.rest {
                image.clear();
                spec.append_image(&mb, row, &mut image);
                put(&image);
            }
        }
        w.weight_mut().copy_from_slice(delta.weight());
        index.moves.write_null_rows(mb.null_bmp(), 0, w.null_bmp_mut(), 0, n);
        copy_columns(&mb, w, index.moves.copies(), n, heap_at);
    });
    out
}

/// The entries are byte for byte the row-at-a-time entries, every region and
/// the heap, for a key of every image kind, nullable or not, ascending or
/// descending, NULLs first or last, leading the index PK or behind another key,
/// grouped or not.
#[test]
fn entries_are_the_row_at_a_time_entries() {
    use gnitz_wire::TypeCode::*;
    const TYPES: [TypeCode; 10] = [U8, I16, I32, I64, U32, U64, F32, F64, U128, String];
    const STRINGS: [&str; 5] = ["", "a", "abcdefghijkl", "a\0b", "a string past the inline threshold"];
    let mut cols = vec![SchemaColumn::new(U64, false)];
    for tc in TYPES {
        for nullable in [false, true] {
            cols.push(SchemaColumn::new(tc, nullable));
        }
    }
    let s = SchemaDescriptor::new(&cols, &[0]);
    let mut b = BatchBuilder::new(&s);
    for i in 0..300u64 {
        b.begin_row_natives(&[mix(i) as u128], 1 + (i % 3) as i64);
        for (c, col) in cols.iter().enumerate().skip(1) {
            let v = mix(i * 31 + c as u64);
            // Few distinct values, so keys tie and the later keys decide.
            let v = if i % 4 == 0 { v % 3 } else { v };
            if col.nullable && v % 3 == 0 {
                b.put_null();
            } else if col.type_code == String {
                b.put_string(STRINGS[(v % STRINGS.len() as u64) as usize]);
            } else {
                let bits = (v as u128) << 64 | mix(v) as u128;
                b.put_int(bits & (u128::MAX >> (128 - 8 * col.size())));
            }
        }
        b.end_row();
    }
    let delta = b.finish();
    let n = cols.len() as u32;
    for c in 0..n {
        for (desc, nulls_first) in [(false, false), (false, true), (true, false), (true, true)] {
            let key = |col: u32| OrderKey { col, desc, nulls_first };
            for keys in [
                vec![key(c)],
                vec![key((c + 1) % n), key(c)],
                vec![key(c), key((c ^ 1) % n), key(n - 1)],
            ] {
                // No group, an I64 one, a nullable I64 one.
                for group in [&[][..], &[7], &[8]] {
                    let plan = OrderedPlan::top_n(&s, group, &keys, WeightSlots { skip: 0, keep: 1 }).unwrap();
                    let (got, want) = (plan.index.entries(&delta), entries_by_row(&plan.index, &delta));
                    let at = format!("keys {keys:?}, group {group:?}");
                    assert_eq!(got.pk(), want.pk(), "{at}: PK");
                    assert_eq!(got.weight(), want.weight(), "{at}: weights");
                    assert_eq!(got.null_bmp(), want.null_bmp(), "{at}: null words");
                    for pi in 0..plan.index.schema.num_payload_cols() {
                        assert_eq!(got.col_data(pi), want.col_data(pi), "{at}: payload {pi}");
                    }
                    assert_eq!(got.blob(), want.blob(), "{at}: heap");
                }
            }
        }
    }
}
