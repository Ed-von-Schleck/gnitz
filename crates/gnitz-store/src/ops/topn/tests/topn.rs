//! Top-N operator tests, driven epoch by epoch through the production populate
//! and integrate paths: the index and the output trace are scratch tables, so
//! every assertion is over the maintained *state*, weights and all.

use std::collections::BTreeMap;

use crate::schema::{type_code, SchemaColumn, SchemaDescriptor};
use crate::storage::{payload_is_null, payload_string, Batch, BatchBuilder, Table};
use crate::test_support::scratch_table;
use gnitz_wire::{read_i64_le, OrderKey};

use super::index::op_populate_topn;
use super::op_topn::op_topn;
use super::plan::TopNPlan;

/// `[U64 id (pk), I64 grp, I64 val NULL, STRING s]`.
fn schema() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::I64, 1),
            SchemaColumn::new(type_code::STRING, 0),
        ],
        &[0],
    )
}

/// One row: `(id, weight, grp, val, s)`.
type Row<'a> = (u64, i64, i64, Option<i64>, &'a str);

fn batch(rows: &[Row<'_>]) -> Batch {
    let mut b = BatchBuilder::new(schema());
    for &(id, w, grp, val, s) in rows {
        b.begin_row(id as u128, w);
        b.put_int(grp as u128);
        match val {
            Some(v) => b.put_int(v as u128),
            None => b.put_null(),
        }
        b.put_string(s);
        b.end_row();
    }
    b.finish()
}

fn key(col: u16, desc: bool, nulls_first: bool) -> OrderKey {
    OrderKey { col, desc, nulls_first }
}

/// The operator with its two tables, driven the way the VM drives it.
struct Harness {
    _dir: tempfile::TempDir,
    plan: TopNPlan,
    index: Table,
    trace_out: Table,
}

impl Harness {
    fn new(group_cols: &[u32], order: &[OrderKey], limit: u64, offset: u64) -> Self {
        let plan = TopNPlan::from_wire(&schema(), group_cols, order, limit, offset).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let index = scratch_table(dir.path().join("idx").to_str().unwrap(), plan.index.schema, 1);
        let trace_out = scratch_table(dir.path().join("out").to_str().unwrap(), plan.output_schema, 2);
        Harness { _dir: dir, plan, index, trace_out }
    }

    /// One epoch: populate, run, integrate the output. Returns the raw delta.
    fn tick(&mut self, delta: &Batch) -> Batch {
        op_populate_topn(delta, &mut self.index, &self.plan.index).unwrap();
        let mut history = self.index.open_cursor();
        let mut trace_out = self.trace_out.open_cursor();
        let out = op_topn(delta, &mut trace_out, &mut history, &self.plan);
        self.trace_out.ingest_borrowed_batch(&out).unwrap();
        out
    }

    /// The maintained output as `(grp, id) → (weight, val, s)`; the group is read
    /// off the carried columns so every key kind reads the same way.
    fn state(&self) -> BTreeMap<(i64, u64), (i64, Option<i64>, String)> {
        let b = self.trace_out.open_cursor().materialize();
        let carried = &self.plan.output_schema;
        // Carried payload order is input schema order minus the key region.
        let (id_pi, grp_pi, val_pi, s_pi) = match carried.num_payload_cols() {
            4 => (Some(0), 1, 2, 3),
            3 => (None, 0, 1, 2),
            n => panic!("unexpected payload width {n}"),
        };
        let mb = b.as_mem_batch();
        (0..b.count)
            .map(|r| {
                let id = match id_pi {
                    Some(pi) => read_i64_le(b.col_data(pi), r * 8) as u64,
                    None => b.get_pk(r) as u64,
                };
                let grp = read_i64_le(b.col_data(grp_pi), r * 8);
                let val = (!payload_is_null(&mb, r, val_pi)).then(|| read_i64_le(b.col_data(val_pi), r * 8));
                ((grp, id), (b.get_weight(r), val, payload_string(&mb, r, s_pi)))
            })
            .collect()
    }

    fn ids(&self) -> Vec<(i64, u64, i64)> {
        self.state()
            .into_iter()
            .map(|((g, id), (w, _, _))| (g, id, w))
            .collect()
    }

    /// An epoch's emitted delta, consolidated — the net change the operator
    /// published, which is what a downstream reader sees. The raw batch carries
    /// a retract/re-emit pair per unchanged window row; those cancel here, so
    /// this asserts the delta is *correct* without pinning that shape.
    fn net(&self, out: Batch) -> Vec<(u64, i64)> {
        let b = out.into_consolidated(&self.plan.output_schema);
        (0..b.count)
            .map(|r| match self.plan.output_schema.num_payload_cols() {
                // The id is a carried payload column under a synthetic key, and
                // the output PK itself when the group set is the input's key.
                4 => (read_i64_le(b.col_data(0), r * 8) as u64, b.get_weight(r)),
                _ => (b.get_pk(r) as u64, b.get_weight(r)),
            })
            .collect()
    }
}

#[test]
fn global_top_2_desc_promotes_from_the_index_on_retraction() {
    let mut h = Harness::new(&[], &[key(2, true, false)], 2, 0);
    h.tick(&batch(&[
        (1, 1, 0, Some(10), "a"),
        (2, 1, 0, Some(30), "b"),
        (3, 1, 0, Some(20), "c"),
    ]));
    assert_eq!(h.ids(), vec![(0, 2, 1), (0, 3, 1)]);
    // Retract the leader: id 1 is promoted from the index, not lost.
    let out = h.tick(&batch(&[(2, -1, 0, Some(30), "b")]));
    assert_eq!(h.ids(), vec![(0, 1, 1), (0, 3, 1)]);
    // The published delta is exactly the window's change: id 3 held its slot, so
    // its retract/re-emit pair cancels and only the swap survives.
    assert_eq!(h.net(out), vec![(1, 1), (2, -1)]);
    // A row below the window changes nothing in the state.
    h.tick(&batch(&[(4, 1, 0, Some(5), "d")]));
    assert_eq!(h.ids(), vec![(0, 1, 1), (0, 3, 1)]);
    // A new leader displaces the last row of the window.
    h.tick(&batch(&[(5, 1, 0, Some(40), "e")]));
    assert_eq!(h.ids(), vec![(0, 3, 1), (0, 5, 1)]);
}

#[test]
fn grouped_top_1_is_per_group_and_asc_reads_the_smallest() {
    let mut h = Harness::new(&[1], &[key(2, false, false)], 1, 0);
    h.tick(&batch(&[
        (1, 1, 7, Some(10), "a"),
        (2, 1, 7, Some(3), "b"),
        (3, 1, -5, Some(100), "c"),
        (4, 1, -5, Some(200), "d"),
    ]));
    assert_eq!(h.ids(), vec![(-5, 3, 1), (7, 2, 1)]);
    // A retraction in one group leaves the other untouched.
    h.tick(&batch(&[(2, -1, 7, Some(3), "b")]));
    assert_eq!(h.ids(), vec![(-5, 3, 1), (7, 1, 1)]);
    // Emptying a group retracts its window.
    h.tick(&batch(&[(1, -1, 7, Some(10), "a")]));
    assert_eq!(h.ids(), vec![(-5, 3, 1)]);
}

#[test]
fn weights_fill_slots_and_are_clamped_at_the_boundary() {
    // A weight-3 row straddling a limit of 2 is emitted at weight 2; once the
    // leader above it arrives it drops to weight 1.
    let mut h = Harness::new(&[], &[key(2, false, false)], 2, 0);
    h.tick(&batch(&[(1, 3, 0, Some(10), "a"), (2, 1, 0, Some(20), "b")]));
    assert_eq!(h.ids(), vec![(0, 1, 2)]);
    h.tick(&batch(&[(3, 1, 0, Some(5), "c")]));
    assert_eq!(h.ids(), vec![(0, 1, 1), (0, 3, 1)]);
    // Retracting one of the three copies leaves two, still both in the window.
    h.tick(&batch(&[(1, -1, 0, Some(10), "a")]));
    assert_eq!(h.ids(), vec![(0, 1, 1), (0, 3, 1)]);
}

#[test]
fn offset_skips_weight_slots() {
    let mut h = Harness::new(&[], &[key(2, false, false)], 2, 1);
    h.tick(&batch(&[
        (1, 1, 0, Some(10), "a"),
        (2, 2, 0, Some(20), "b"),
        (3, 1, 0, Some(30), "c"),
        (4, 1, 0, Some(40), "d"),
    ]));
    // Slots: a, b, b, c, d → skip a, keep b b.
    assert_eq!(h.ids(), vec![(0, 2, 2)]);
    h.tick(&batch(&[(2, -2, 0, Some(20), "b")]));
    // Slots: a, c, d → skip a, keep c d.
    assert_eq!(h.ids(), vec![(0, 3, 1), (0, 4, 1)]);
}

#[test]
fn group_by_the_pk_keeps_the_input_key_and_carries_the_rest() {
    // Grouping by the PK: every group is a singleton, the output PK is the
    // input's, and the carried payload is the three non-key columns.
    let mut h = Harness::new(&[0], &[key(2, false, false)], 1, 0);
    h.tick(&batch(&[(1, 1, 7, Some(10), "a"), (2, 1, 8, Some(3), "b")]));
    let st = h.state();
    assert_eq!(st[&(7, 1)], (1, Some(10), "a".to_string()));
    assert_eq!(st[&(8, 2)], (1, Some(3), "b".to_string()));
}

#[test]
fn from_wire_rejects_what_it_cannot_run() {
    let s = schema();
    assert!(TopNPlan::from_wire(&s, &[9], &[key(2, false, false)], 1, 0).is_err());
    assert!(TopNPlan::from_wire(&s, &[], &[key(9, false, false)], 1, 0).is_err());
    assert!(TopNPlan::from_wire(&s, &[], &[key(2, false, false)], 0, 0).is_err());
    let float = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::F64, 0),
        ],
        &[0],
    );
    assert!(TopNPlan::from_wire(&float, &[1], &[key(0, false, false)], 1, 0).is_err());
    // A float *order* key is fine: it has a total-order image.
    assert!(TopNPlan::from_wire(&float, &[], &[key(1, true, false)], 1, 0).is_ok());
}

/// The maintained window and the read path's ORDER BY comparator must select
/// the same rows in the same order — one is byte images in an index, the other
/// `cmp_order_keys` over the values, and nothing but this holds them together.
/// Over a limit covering every row, the emitted window *is* the full sort.
#[test]
fn the_window_order_is_the_shared_comparator_order() {
    let rows: &[Row<'_>] = &[
        (1, 1, 0, Some(5), "m"),
        (2, 1, 0, None, "z"),
        (3, 1, 0, Some(-7), "a"),
        (4, 1, 0, Some(5), "a"),
        (5, 1, 0, None, "a"),
        (6, 1, 0, Some(i64::MIN), "\u{0}b"),
        // A string that is a prefix of another: what the image's prefix-freeness
        // exists for, seen through the operator.
        (7, 1, 0, Some(5), "ab"),
        (8, 1, 0, Some(5), "abc"),
    ];
    // Every combination of direction and NULL placement on the value key, with a
    // string key and the PK behind it — the tiebreak the planner appends.
    for (desc, nulls_first) in [(false, false), (false, true), (true, false), (true, true)] {
        let keys = [
            key(2, desc, nulls_first),
            key(3, desc, nulls_first),
            key(0, false, false),
        ];
        let mut h = Harness::new(&[], &keys, rows.len() as u64, 0);
        let out = h.tick(&batch(rows));

        let b = batch(rows);
        let mb = b.as_mem_batch();
        let locs: Vec<gnitz_expr::OrderLocator> = keys
            .iter()
            .map(|k| gnitz_expr::OrderLocator {
                loc: schema().locate(k.col as usize),
                desc: k.desc,
                nulls_first: k.nulls_first,
            })
            .collect();
        let mut want: Vec<usize> = (0..b.count).collect();
        want.sort_by(|&x, &y| {
            gnitz_expr::cmp_order_keys(&locs, &mb, x, mb.get_null_word(x), &mb, y, mb.get_null_word(y))
        });
        let want: Vec<u64> = want.into_iter().map(|r| rows[r].0).collect();

        // The first epoch retracts nothing, so the delta is the window in order.
        let got: Vec<u64> = (0..out.count)
            .map(|r| read_i64_le(out.col_data(0), r * 8) as u64)
            .collect();
        assert_eq!(got, want, "desc={desc} nulls_first={nulls_first}");
    }
}
