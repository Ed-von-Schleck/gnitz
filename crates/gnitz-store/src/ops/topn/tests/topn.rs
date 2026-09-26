//! Top-N operator tests, driven epoch by epoch through the production populate
//! and integrate paths: the index and the output trace are scratch tables, so
//! every assertion is over the maintained *state*, weights and all.

use std::collections::BTreeMap;

use crate::schema::{SchemaColumn, SchemaDescriptor, TypeCode};
use crate::storage::{Batch, BatchBuilder, Table};
use crate::test_support::scratch_table;
use gnitz_expr::{payload_is_null, payload_string};
use gnitz_wire::{read_i64_le, OrderKey};

use super::op_topn::op_topn;
use super::plan::TopNPlan;

/// `[U64 id (pk), I64 grp, I64 val NULL, STRING s]`.
fn schema() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::I64, false),
            SchemaColumn::new(TypeCode::I64, true),
            SchemaColumn::new(TypeCode::String, false),
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
        Harness::of(TopNPlan::from_wire(&schema(), group_cols, order, limit, offset).unwrap())
    }

    fn of(plan: TopNPlan) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let index = scratch_table(dir.path().join("idx").to_str().unwrap(), plan.index.schema);
        let trace_out = scratch_table(dir.path().join("out").to_str().unwrap(), plan.output_schema);
        Harness { _dir: dir, plan, index, trace_out }
    }

    /// One epoch: populate, run, integrate the output. Returns the raw delta.
    fn tick(&mut self, delta: &Batch) -> Batch {
        self.index.ingest_owned_batch(self.plan.index_batch(delta)).unwrap();
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

    /// An epoch's emitted delta as `(id, weight)`, which must be folded.
    fn net(&self, b: Batch) -> Vec<(u64, i64)> {
        assert!(b.is_consolidated(), "op_topn certifies its output folded");
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
    // A row below the window changes nothing, and publishes nothing.
    let out = h.tick(&batch(&[(4, 1, 0, Some(5), "d")]));
    assert_eq!(h.net(out), vec![]);
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
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::F64, false),
        ],
        &[0],
    );
    assert!(TopNPlan::from_wire(&float, &[1], &[key(0, false, false)], 1, 0).is_err());
    // A float *order* key is fine: it has a total-order image.
    assert!(TopNPlan::from_wire(&float, &[], &[key(1, true, false)], 1, 0).is_ok());
}

/// The maintained window (byte images in an index) and the read path's
/// `cmp_order_keys` order rows alike: a window of `k` slots holds the
/// comparator's first `k` rows, for every `k`.
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
        let b = batch(rows);
        let mb = b.as_mem_batch();
        let locs: Vec<gnitz_expr::OrderLocator> = keys
            .iter()
            .map(|k| gnitz_expr::OrderLocator::of(schema().locate(k.col as usize), k))
            .collect();
        let mut want: Vec<usize> = (0..b.count).collect();
        want.sort_by(|&x, &y| gnitz_expr::cmp_order_keys(&locs, &mb, x, &mb, y));
        let want: Vec<u64> = want.into_iter().map(|r| rows[r].0).collect();

        for k in 1..=rows.len() {
            let mut h = Harness::new(&[], &keys, k as u64, 0);
            // The first epoch retracts nothing, so the delta is the window.
            let out = h.tick(&batch(rows));
            let mut got: Vec<u64> = (0..out.count)
                .map(|r| read_i64_le(out.col_data(0), r * 8) as u64)
                .collect();
            got.sort_unstable();
            let mut first_k = want[..k].to_vec();
            first_k.sort_unstable();
            assert_eq!(got, first_k, "desc={desc} nulls_first={nulls_first} k={k}");
        }
    }
}

// ── The global top-N split into per-worker partials ─────────────────────

/// A top-N combine outputs the funnel's layout.
#[test]
fn a_topn_combine_outputs_the_funnels_layout() {
    let order = [key(2, true, false), key(0, false, false)];
    let funnel = TopNPlan::from_wire(&schema(), &[], &order, 3, 1).unwrap();
    let partial = TopNPlan::partial(&schema(), &order, 3, 1).unwrap();
    let combine = TopNPlan::combine(&partial.output_schema, &order, 3, 1).unwrap();
    assert_eq!(combine.output_schema, funnel.output_schema);
}

/// Two workers' partial windows, relayed to one combine, publish exactly the
/// funnel's delta every epoch — an OFFSET included, which a partial's window
/// has to cover.
#[test]
fn two_workers_partials_combine_to_the_funnels_window() {
    let order = [key(2, false, false), key(0, false, false)];
    let (limit, offset) = (2, 1);
    let partial = || Harness::of(TopNPlan::partial(&schema(), &order, limit, offset).unwrap());
    let (mut a, mut b) = (partial(), partial());
    let partials = a.plan.output_schema;
    let mut combine = Harness::of(TopNPlan::combine(&partials, &order, limit, offset).unwrap());
    let mut funnel = Harness::new(&[], &order, limit, offset);

    let ticks: &[(&[Row<'_>], &[Row<'_>])] = &[
        (
            &[(1, 1, 0, Some(50), "a"), (2, 1, 0, Some(10), "b")],
            &[(3, 1, 0, Some(30), "c")],
        ),
        (
            &[(4, 1, 0, Some(20), "d")],
            &[(5, 1, 0, Some(5), "e"), (6, 1, 0, Some(40), "f")],
        ),
        (&[(2, -1, 0, Some(10), "b")], &[]),
        (&[], &[(5, -1, 0, Some(5), "e"), (3, -1, 0, Some(30), "c")]),
        (&[(7, 1, 0, None, "g")], &[(8, 2, 0, Some(1), "h")]),
    ];
    for (i, &(da, db)) in ticks.iter().enumerate() {
        let relayed = Batch::concat(&partials, [a.tick(&batch(da)), b.tick(&batch(db))].iter());
        let whole = Batch::concat(&schema(), [batch(da), batch(db)].iter());
        let got = combine.tick(&relayed);
        let want = funnel.tick(&whole);
        let (got, want) = (combine.net(got), funnel.net(want));
        assert_eq!(got, want, "tick {i}");
    }
}

/// A `LIMIT` past any batch runs, and so does a partial whose `limit + offset`
/// saturates.
#[test]
fn a_huge_limit_and_a_saturated_partial_run() {
    let rows = [
        (1, 1, 0, Some(3), "a"),
        (2, 1, 0, Some(1), "b"),
        (3, 1, 0, Some(2), "c"),
    ];
    let order = [key(2, false, false), key(0, false, false)];
    let mut h = Harness::new(&[], &order, u64::MAX / 2, 0);
    let out = h.tick(&batch(&rows));
    assert_eq!(h.net(out), vec![(1, 1), (2, 1), (3, 1)]);
    let mut p = Harness::of(TopNPlan::partial(&schema(), &order, u64::MAX / 2, u64::MAX).unwrap());
    let out = p.tick(&batch(&rows));
    assert_eq!(p.net(out), vec![(1, 1), (2, 1), (3, 1)]);
}
