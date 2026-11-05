//! Reduce tests: the operator, its global split and the ad-hoc fold, each held
//! to the aggregates of its input computed from their SQL definitions. The
//! operator is driven epoch by epoch through the production populate and
//! integrate paths.

use std::borrow::Cow;
use std::collections::BTreeMap;

use proptest::prelude::*;

use crate::repr::Batch;
use crate::test_support::stream_input::{
    agg, agg_of, arb_row, arb_ticks, batch, cells, delta_to, schema, shard, Cells, Row, GROUPS,
};
use crate::test_support::{assert_folds, cell, weighted_rows, TestTrace};
use gnitz_expr::{CmpOp, ReduceOutKey, RowFilter};
use gnitz_wire::{AggDescriptor, AggFunc, GroupAggs, ReadSink, SinkKind, TypeCode};

use super::{op_reduce, ReducePlan};
use crate::algebra::SinkPlan;

/// The ad-hoc fold of `aggs` by `group_cols` over [`schema`], as a read's sink.
fn fold_sink(group_cols: &[u32], aggs: Vec<AggDescriptor>, group_cap: usize) -> Result<SinkPlan, String> {
    let spec = GroupAggs { group_cols: group_cols.to_vec(), aggs };
    let sink = ReadSink { map: None, kind: SinkKind::Fold(spec) };
    SinkPlan::from_wire(&schema(), &sink, group_cap)
}

/// The operator with its two traces, driven the way the VM drives it.
pub(super) struct Harness {
    plan: ReducePlan,
    index: Option<TestTrace>,
    pub(super) out_trace: TestTrace,
    /// How often the operator has opened the value index.
    index_opens: usize,
}

impl Harness {
    pub(super) fn new(plan: ReducePlan) -> Self {
        let index = plan.index_schema().map(|schema| TestTrace::new(*schema));
        let out_trace = TestTrace::new(*plan.output_schema());
        Harness { plan, index, out_trace, index_opens: 0 }
    }

    /// Add `delta`'s entries to the value index, as the VM does ahead of
    /// `op_reduce`.
    pub(super) fn index(&mut self, delta: &Batch) {
        if let (Some(index), Some(entries)) = (&mut self.index, self.plan.index_entries(delta)) {
            index.ingest(entries);
        }
    }

    /// `op_reduce` over `delta` against the two traces, each opened at exactly
    /// the keys the operator asks for.
    pub(super) fn reduce(&mut self, delta: &Batch) -> Batch {
        let mut open_out = |first: &[u8], last: &[u8]| self.out_trace.cursor_within(first, last);
        let opens = &mut self.index_opens;
        let mut index = |first: &[u8], last: &[u8]| {
            *opens += 1;
            let index = self.index.as_ref().expect("a plan with a value index");
            index.cursor_within(first, last)
        };
        op_reduce(delta, &mut open_out, Some(&mut index), &mut self.plan)
    }

    /// `delta` as the VM hands it to the reduce: folded unless every aggregate
    /// is exact linear.
    pub(super) fn fold(&self, delta: Batch) -> Batch {
        match self.plan.is_exact_linear() {
            true => delta,
            false => delta.into_consolidated(),
        }
    }

    /// One epoch: fold the delta, index it, run, integrate the output.
    fn tick(&mut self, delta: Batch) -> Batch {
        let delta = self.fold(delta);
        self.index(&delta);
        let out = self.reduce(&delta);
        assert_folds(std::slice::from_ref(&out), &out, "op_reduce's delta");
        self.out_trace.ingest(out.clone());
        out
    }

    /// The epoch of its own a global aggregate's ground row is minted in: the
    /// row lands in the output's trace.
    fn ground(&mut self) {
        if let Some(v0) = self.plan.ground_row() {
            self.out_trace.ingest(v0);
        }
    }

    fn state(&self, group_cols: &[u32]) -> BTreeMap<Cells, i64> {
        output_rows(&self.out_trace.cursor().materialize(), group_cols)
    }
}

/// A reduce output over `group_cols` as its Z-set of `[group columns…,
/// aggregates…]` rows, a synthetic key left out.
fn output_rows(b: &Batch, group_cols: &[u32]) -> BTreeMap<Cells, i64> {
    let mb = b.as_mem_batch();
    let out = b.schema();
    let skip = usize::from(schema().reduce_out_key(group_cols) == ReduceOutKey::SyntheticFold);
    let mut z = BTreeMap::new();
    for r in 0..b.len() {
        let row: Cells = (skip..out.num_columns()).map(|c| cell(&mb, out.locate(c), r)).collect();
        *z.entry(row).or_insert(0) += b.get_weight(r);
    }
    z.retain(|_, w| *w != 0);
    z
}

/// The aggregates of `model` from scratch: one `[group columns…, aggregates…]`
/// row per non-empty group, and with `ground` the global group's row over no
/// rows at all.
fn aggregates(
    model: &BTreeMap<Row, i64>,
    group_cols: &[u32],
    aggs: &[AggDescriptor],
    ground: bool,
) -> BTreeMap<Cells, i64> {
    let mut groups: BTreeMap<Cells, Vec<(Cells, i64)>> = BTreeMap::new();
    if ground {
        groups.insert(vec![], vec![]);
    }
    for (r, &w) in model {
        groups.entry(r.group_key(group_cols)).or_default().push((cells(r), w));
    }
    groups
        .into_iter()
        .map(|(mut key, rows)| {
            key.extend(aggs.iter().map(|&a| agg_of(a, &rows)));
            (key, 1)
        })
        .collect()
}

/// A COUNT(*) among up to three aggregates over any column that admits them.
/// The plainly summed float is `x` alone, whose values are finite; both
/// floats' finite values sum exactly.
fn arb_aggs() -> impl Strategy<Value = Vec<AggDescriptor>> {
    let float = prop_oneof![
        Just(AggFunc::CountPosInf),
        Just(AggFunc::CountNegInf),
        Just(AggFunc::FiniteSum)
    ];
    let one = prop_oneof![
        (0u32..8).prop_map(|c| agg(AggFunc::CountNonNull, c)),
        (float, 4u32..6).prop_map(|(f, c)| agg(f, c)),
        prop::sample::select(&[0u32, 1, 3, 5, 7][..]).prop_map(|c| agg(AggFunc::Sum, c)),
        (0u32..8).prop_map(|c| agg(AggFunc::Min, c)),
        (0u32..8).prop_map(|c| agg(AggFunc::Max, c)),
    ];
    (prop::collection::vec(one, 0..4), any::<prop::sample::Index>()).prop_map(|(mut aggs, at)| {
        aggs.insert(at.index(aggs.len() + 1), AggDescriptor::COUNT_STAR);
        aggs
    })
}

/// [`arb_row`] over four times its group keys: a spread `k` shifts its `id`,
/// `grp` and `w`, so a group set reading one of those alone has more groups
/// than an epoch emits a row at a time.
fn arb_spread_row() -> impl Strategy<Value = Row> {
    (arb_row(), 0u8..4).prop_map(|(r, k)| Row {
        id: r.id.wrapping_add(k as u64),
        grp: r.grp + 2 * k as i64,
        w: r.w ^ ((k as u128) << 32),
        ..r
    })
}

/// After every epoch of `ticks` the maintained output is the aggregates of the
/// integrated input, weights included.
fn maintains(
    group_cols: &[u32],
    aggs: &[AggDescriptor],
    seeds: bool,
    folded: bool,
    ticks: &[Vec<(Row, i64)>],
) -> Result<(), TestCaseError> {
    let ground = seeds && group_cols.is_empty();
    let mut h = Harness::new(ReducePlan::from_wire(&schema(), group_cols, aggs, ground).unwrap());
    let mut model = BTreeMap::new();
    h.ground();
    prop_assert_eq!(h.state(group_cols), aggregates(&model, group_cols, aggs, ground));
    for targets in ticks {
        let delta = batch(&delta_to(&mut model, targets));
        h.tick(if folded { delta.into_consolidated() } else { delta });
        prop_assert_eq!(h.state(group_cols), aggregates(&model, group_cols, aggs, ground));
    }
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// After every epoch the maintained output is the aggregates of the
    /// integrated input, weights included, under every group key form — whether
    /// or not a global reduce seeds its ground row, and whether the delta arrives
    /// raw or already folded.
    #[test]
    fn the_maintained_aggregates_are_the_aggregates_of_the_integral(
        group_cols in prop::sample::select(&GROUPS[..]),
        aggs in arb_aggs(),
        seeds in any::<bool>(),
        folded in any::<bool>(),
        ticks in arb_ticks(arb_row()),
    ) {
        maintains(group_cols, &aggs, seeds, folded, &ticks)?;
    }

    /// The same over epochs that touch more groups than one emits a row at a
    /// time: those copy their stored rows out a column at a time.
    #[test]
    fn many_groups_maintain_the_aggregates_of_the_integral(
        group_cols in prop::sample::select(&GROUPS[1..]),
        aggs in arb_aggs(),
        folded in any::<bool>(),
        ticks in prop::collection::vec(arb_spread_row(), 8..24).prop_flat_map(|pool| {
            let target = (prop::sample::select(pool), 0i64..=3);
            prop::collection::vec(prop::collection::vec(target, 0..24), 1..5)
        }),
    ) {
        maintains(group_cols, &aggs, false, folded, &ticks)?;
    }

    /// Two workers' partials, relayed to one combine, maintain the global
    /// aggregates, the ground row over no input included.
    #[test]
    fn two_workers_partials_combine_to_the_global_aggregates(
        aggs in arb_aggs().prop_map(|mut aggs| {
            // A float sum has no partial.
            aggs.retain(|d| !(matches!(d.agg_op, AggFunc::Sum | AggFunc::FiniteSum) && matches!(d.col_idx, 4 | 5)));
            aggs
        }),
        ticks in arb_ticks(arb_row()),
    ) {
        let partial = || Harness::new(ReducePlan::partial(&schema(), &aggs).unwrap().unwrap());
        let mut workers = [partial(), partial()];
        let partials = *workers[0].plan.output_schema();
        let mut combine = Harness::new(ReducePlan::combine(&partials, &aggs, true).unwrap());
        let mut model = BTreeMap::new();
        combine.ground();
        prop_assert_eq!(combine.state(&[]), aggregates(&model, &[], &aggs, true));
        for targets in &ticks {
            let delta = delta_to(&mut model, targets);
            let relayed: Vec<Batch> = (0..2).map(|w| workers[w as usize].tick(shard(&delta, w))).collect();
            combine.tick(Batch::concat(&partials, relayed.iter().map(Batch::as_mem_batch)));
            prop_assert_eq!(combine.state(&[]), aggregates(&model, &[], &aggs, true));
        }
    }

    /// The ad-hoc fold, over any chunking of a scan and any filter over it, is
    /// the aggregates of the rows the filter keeps, in the layout the view's
    /// reduce emits.
    #[test]
    fn the_adhoc_fold_is_the_aggregates_of_the_rows_its_filter_keeps(
        group_cols in prop::sample::select(&GROUPS[..]),
        aggs in arb_aggs(),
        scan in prop::collection::btree_map(arb_row(), 1i64..=3, 0..12),
        // `grp` is -1 or 0, so `lo` = -1 keeps every row, 0 the interleaved
        // `grp = 0` rows, 1 none.
        lo in -1i64..=1,
        chunk in 1usize..5,
    ) {
        let mut fold = fold_sink(group_cols, aggs.clone(), usize::MAX).unwrap();
        let view = ReducePlan::from_wire(&schema(), group_cols, &aggs, false).unwrap();
        prop_assert_eq!(fold.output_schema(), view.output_schema());

        let mut keep = crate::test_support::cmp_all(&[(CmpOp::Ge, 3, lo)]).resolve_filter(&schema()).unwrap();
        let scan: Vec<(Row, i64)> = scan.into_iter().collect();
        for rows in scan.chunks(chunk) {
            fold.push(Cow::Owned(batch(rows)), &mut keep).unwrap();
        }
        let folded: BTreeMap<Row, i64> = scan.iter().filter(|(r, _)| r.grp >= lo).copied().collect();
        prop_assert_eq!(
            output_rows(&fold.finish(), group_cols),
            aggregates(&folded, group_cols, &aggs, group_cols.is_empty())
        );
    }
}

/// Epochs over runs of more groups than one emits a row at a time, their
/// extremes long strings: one whose groups are all fresh, and one with every
/// outcome of a group's new row against its stored row, beside a run past the
/// step cap that reads its extremes off the index.
#[test]
fn a_gathered_epoch_over_runs_emits_every_outcome() {
    const S: [&str; 6] = [
        "a string extreme too long to be inlined 0",
        "a string extreme too long to be inlined 1",
        "a string extreme too long to be inlined 2",
        "a string extreme too long to be inlined 3",
        "a string extreme too long to be inlined 4",
        "a string extreme too long to be inlined 5",
    ];
    let aggs = [
        agg(AggFunc::Min, 2),
        agg(AggFunc::Max, 0),
        agg(AggFunc::Sum, 7),
        AggDescriptor::COUNT_STAR,
    ];
    let row = |id: u64, grp: i64, val: i64, s: usize| Row {
        id,
        grp,
        val: Some(val),
        s: Some(S[s]),
        n: Some(1),
        ..Row::default()
    };
    let pair = |id: u64| [(row(id, 0, 0, 3), 1), (row(id, 1, 1, 5), 1)];
    let history: Vec<(Row, i64)> = (10..=18).chain([22]).flat_map(pair).collect();
    // Fresh groups on both sides of the untouched group 22.
    let fresh: Vec<(Row, i64)> = [20, 21, 23, 24, 25].into_iter().flat_map(pair).collect();
    let mut outcomes = vec![
        // A lower minimum, no retraction: stepped.
        (row(10, 2, 0, 0), 1),
        // A retraction lowering the maximum: probed.
        (row(11, 1, 1, 5), 0),
        // A swap keeping every aggregate.
        (row(12, 1, 1, 5), 0),
        (row(12, 2, 1, 5), 1),
        // An emptied group.
        (row(13, 0, 0, 3), 0),
        (row(13, 1, 1, 5), 0),
        // A higher maximum.
        (row(14, 2, 9, 4), 1),
    ];
    // A run past the step cap.
    outcomes.extend((100..240).map(|grp| (row(15, grp, grp % 7, (grp % 6) as usize), 1)));
    outcomes.extend(pair(19));

    let mut h = Harness::new(ReducePlan::from_wire(&schema(), &[1], &aggs, false).unwrap());
    let mut model = BTreeMap::new();
    for targets in [history, fresh, outcomes] {
        h.tick(batch(&delta_to(&mut model, &targets)));
        assert_eq!(h.state(&[1]), aggregates(&model, &[1], &aggs, false));
    }
}

/// Group 7's row `n`, of value `val` and string `s`.
fn member(n: i64, val: Option<i64>, s: &'static str) -> Row {
    Row {
        id: 7,
        grp: n,
        val,
        s: Some(s),
        ..Row::default()
    }
}

/// Whether an epoch setting `targets` in group 7 of a reduce grouped by `id`,
/// after one setting `history`, opens the value index. With
/// `bystander`, an insert into a fresh group joins the delta: two groups of a
/// row or two each, which the ordinal arm folds; without, group 7 is one run.
/// After each epoch the output is the aggregates of the integrated input.
fn reads_index(aggs: &[AggDescriptor], history: &[(Row, i64)], targets: &[(Row, i64)], bystander: bool) -> bool {
    let mut h = Harness::new(ReducePlan::from_wire(&schema(), &[1], aggs, false).unwrap());
    let mut model = BTreeMap::new();
    let fresh = (
        Row {
            id: 9,
            val: Some(0),
            s: Some("b"),
            ..Row::default()
        },
        1,
    );
    let targets: Vec<(Row, i64)> = targets.iter().copied().chain(bystander.then_some(fresh)).collect();
    let mut opens = 0;
    for targets in [history, &targets[..]] {
        opens = h.index_opens;
        h.tick(batch(&delta_to(&mut model, targets)));
        assert_eq!(h.state(&[1]), aggregates(&model, &[1], aggs, false));
    }
    h.index_opens > opens
}

/// MIN and MAX of the nullable `val`, and a SUM beside them.
fn scalar_extremes() -> [AggDescriptor; 4] {
    [
        AggDescriptor::COUNT_STAR,
        agg(AggFunc::Min, 0),
        agg(AggFunc::Max, 0),
        agg(AggFunc::Sum, 0),
    ]
}

/// A group whose retractions leave its scalar extremes in place reads them
/// off its delta and its stored row: a retraction strictly between its MIN
/// and its MAX, or of a NULL, opens no index, in either arm.
#[test]
fn a_retraction_that_leaves_the_extremes_in_place_reads_no_index() {
    let history = [
        (member(0, Some(1), "m"), 1),
        (member(1, Some(5), "m"), 1),
        (member(2, None, "m"), 1),
        (member(3, Some(9), "m"), 1),
    ];
    for bystander in [false, true] {
        let inner = [(member(1, Some(5), "m"), 0)];
        assert!(!reads_index(&scalar_extremes(), &history, &inner, bystander));
        let null = [(member(2, None, "m"), 0)];
        assert!(!reads_index(&scalar_extremes(), &history, &null, bystander));
        // An insert beside it moves an extreme without the index.
        let moved = [(member(1, Some(5), "m"), 0), (member(4, Some(0), "m"), 1)];
        assert!(!reads_index(&scalar_extremes(), &history, &moved, bystander));
    }
}

/// A retraction of a stored extreme reads the index unless the delta inserts
/// a value at or past it, or empties the group: the extreme held twice keeps
/// its value, and one held once recedes to the next.
#[test]
fn a_retraction_of_an_extreme_reads_the_index_unless_an_insert_reaches_it() {
    let history = [
        (member(0, Some(3), "m"), 1),
        (member(1, Some(3), "m"), 1),
        (member(2, Some(6), "m"), 1),
        (member(3, Some(9), "m"), 1),
    ];
    for bystander in [false, true] {
        let reads = |targets: &[(Row, i64)]| reads_index(&scalar_extremes(), &history, targets, bystander);
        // MIN 3 held twice, and MAX 9 held once.
        assert!(reads(&[(member(0, Some(3), "m"), 0)]));
        assert!(reads(&[(member(3, Some(9), "m"), 0)]));
        // An insert at the retracted extreme, or past it.
        for v in [3, 2] {
            assert!(!reads(&[(member(0, Some(3), "m"), 0), (member(4, Some(v), "m"), 1)]));
        }
        for v in [9, 10] {
            assert!(!reads(&[(member(3, Some(9), "m"), 0), (member(4, Some(v), "m"), 1)]));
        }
        // An insert short of it: the extreme recedes to the history's next.
        assert!(reads(&[(member(3, Some(9), "m"), 0), (member(4, Some(7), "m"), 1)]));
        // Emptied, the group has no extreme to read.
        let emptied: Vec<(Row, i64)> = history.iter().map(|&(r, _)| (r, 0)).collect();
        assert!(!reads(&emptied));
        assert!(reads(&[
            (member(0, Some(3), "m"), 0),
            (member(1, Some(3), "m"), 0),
            (member(4, Some(4), "m"), 1),
        ]));
    }
}

/// A capped run steps none of its rows for its extremes, so it reads every
/// one off the index, whatever it retracts.
#[test]
fn a_capped_run_reads_its_extremes_off_the_index() {
    let history = [(member(0, Some(3), "m"), 1), (member(1, Some(9), "m"), 1)];
    let burst = |extra: &[(Row, i64)]| -> Vec<(Row, i64)> {
        let rows = (2..=200).map(|n| (member(n, Some(n % 50 + 4), "m"), 1));
        extra.iter().copied().chain(rows).collect()
    };
    assert!(reads_index(&scalar_extremes(), &history, &burst(&[]), false));
    assert!(reads_index(
        &scalar_extremes(),
        &history,
        &burst(&[(member(1, Some(9), "m"), 0)]),
        false
    ));
}

/// A wide extreme folds no retraction, so any retraction of its group reads
/// it off the index, in either arm; a scalar extreme beside it still reads
/// its own off the delta.
#[test]
fn a_retraction_reads_a_wide_extreme_off_the_index() {
    let aggs = [
        AggDescriptor::COUNT_STAR,
        agg(AggFunc::Min, 2),
        agg(AggFunc::Max, 2),
        agg(AggFunc::Min, 0),
    ];
    let history = [
        (member(0, Some(1), "a"), 1),
        (member(1, Some(5), "m"), 1),
        (member(2, Some(9), "z"), 1),
    ];
    for bystander in [false, true] {
        assert!(reads_index(&aggs, &history, &[(member(1, Some(5), "m"), 0)], bystander));
        assert!(reads_index(&aggs, &history, &[(member(0, Some(1), "a"), 0)], bystander));
    }
}

/// Past its cap the ad-hoc fold refuses a new group and still folds into the
/// ones it holds.
#[test]
fn the_adhoc_fold_refuses_a_group_past_its_cap() {
    let mut fold = fold_sink(&[1], vec![AggDescriptor::COUNT_STAR], 2).unwrap();
    let rows = |ids: &[u64]| {
        batch(
            &ids.iter()
                .map(|&id| (Row { id, ..Row::default() }, 1))
                .collect::<Vec<_>>(),
        )
    };
    let mut all = RowFilter::for_read(&[], None, &schema()).unwrap();
    fold.push(Cow::Owned(rows(&[7, 8, 7])), &mut all).unwrap();
    let err = fold.push(Cow::Owned(rows(&[8, 9])), &mut all).unwrap_err();
    assert!(err.contains("exceeds 2 distinct groups"), "{err}");
}

/// A circuit node and a read spec are trust boundaries: neither builds over a
/// column that is not there, a float group column, a sum with no register
/// image, or more columns than a schema holds — and a circuit reduce needs the
/// COUNT(*) that tells an emptied group from a live one.
#[test]
fn a_shape_the_reduce_cannot_run_is_refused() {
    let count = AggDescriptor::COUNT_STAR;
    let refused: [(&[u32], Vec<AggDescriptor>, &str); 7] = [
        (&[9], vec![count], "group key: column 9 out of range"),
        (&[4], vec![count], "is a float"),
        (
            &[],
            vec![count, agg(AggFunc::Min, 9)],
            "aggregate column 9 out of range",
        ),
        (&[], vec![count, agg(AggFunc::Sum, 2)], "Sum is not defined over"),
        (&[], vec![count, agg(AggFunc::Sum, 6)], "Sum is not defined over"),
        (&[], vec![count; 65], "MAX_COLUMNS"),
        (&[], vec![agg(AggFunc::Min, 0)], "needs a COUNT(*)"),
    ];
    for (group_cols, aggs, why) in refused {
        let err = ReducePlan::from_wire(&schema(), group_cols, &aggs, false)
            .err()
            .expect("refused");
        assert!(err.contains(why), "{err}");
        if why == "needs a COUNT(*)" {
            continue;
        }
        let err = fold_sink(group_cols, aggs, usize::MAX).err().expect("refused");
        assert!(err.starts_with("fold: ") && err.contains(why), "{err}");
    }
}

/// The output is the key region, the group columns it does not spell, then one
/// column per aggregate: a count or a sum is never NULL, an extreme is over a
/// nullable column or the global group, which can be empty.
#[test]
fn the_output_is_the_key_the_group_columns_and_the_aggregates() {
    use TypeCode::*;
    let aggs = [
        AggDescriptor::COUNT_STAR,
        agg(AggFunc::Sum, 7),
        agg(AggFunc::Sum, 5),
        agg(AggFunc::CountNonNull, 2),
        agg(AggFunc::Min, 0),
        agg(AggFunc::Max, 1),
    ];
    let output = |group_cols: &[u32]| {
        let s = *ReducePlan::from_wire(&schema(), group_cols, &aggs, false)
            .unwrap()
            .output_schema();
        let cols: Vec<(TypeCode, bool)> = s.columns().iter().map(|c| (c.type_code, c.nullable)).collect();
        (cols, s.pk_cols().to_vec())
    };
    let aggregates = |max_nullable| {
        [
            (I64, false),
            (I64, false),
            (F64, false),
            (I64, false),
            (I64, true),
            (U64, max_nullable),
        ]
    };
    let layout = |lead: &[(TypeCode, bool)], max_nullable| [lead, &aggregates(max_nullable)].concat();
    assert_eq!(output(&[3]), (layout(&[(I64, false)], false), vec![0]));
    assert_eq!(
        output(&[1, 3]),
        (layout(&[(U64, false), (I64, false)], false), vec![0, 1])
    );
    assert_eq!(output(&[0]), (layout(&[(U128, false), (I64, true)], false), vec![0]));
    assert_eq!(output(&[]), (layout(&[(U128, false)], true), vec![0]));
}

/// Every aggregate but a float sum splits into per-worker partials: a float sum
/// depends on the order a split would change.
#[test]
fn every_aggregate_but_a_float_sum_splits() {
    for (extra, splits) in [
        (agg(AggFunc::Count, 4), true),
        (agg(AggFunc::CountNonNull, 2), true),
        (agg(AggFunc::Sum, 7), true),
        (agg(AggFunc::Sum, 5), false),
        (agg(AggFunc::Sum, 4), false),
        (agg(AggFunc::FiniteSum, 4), false),
        (agg(AggFunc::CountPosInf, 4), true),
        (agg(AggFunc::CountNegInf, 5), true),
        (agg(AggFunc::Min, 0), true),
        (agg(AggFunc::Max, 0), true),
    ] {
        let partial = ReducePlan::partial(&schema(), &[AggDescriptor::COUNT_STAR, extra]).unwrap();
        assert_eq!(partial.is_some(), splits, "{extra:?}");
    }
}

/// A scalar extreme sharing the 16-byte value slot a wide extreme forces writes
/// its 8 image bytes and then zeroes, so the same value always keys the same
/// entry whatever the arena held.
#[test]
fn a_scalar_extreme_zero_pads_the_value_slot_a_wide_one_widens() {
    let aggs = [AggDescriptor::COUNT_STAR, agg(AggFunc::Min, 0), agg(AggFunc::Max, 6)];
    let plan = ReducePlan::from_wire(&schema(), &[3], &aggs, false).unwrap();
    let row = Row {
        id: 1,
        val: Some(7),
        w: 9,
        ..Row::default()
    };
    let idx = plan.index_entries(&batch(&[(row, 1)])).unwrap();
    let mb = idx.as_mem_batch();
    let key = mb.get_pk_bytes(0);
    let n = key.len();
    assert_eq!(key[n - 17], 0, "MIN(val) is ordinal 0");
    assert_eq!(&key[n - 16..n - 8], &(7u64 ^ (1 << 63)).to_be_bytes(), "the I64 image");
    assert_eq!(&key[n - 8..], &[0u8; 8], "zero padding");
}

/// SUM over a float column is each non-NULL value, an F32 one widened to f64,
/// times its weight, added in row order: through the run arm, the ordinal arm
/// and the ad-hoc fold.
#[test]
fn a_float_sum_weighs_each_value_in_f64() {
    let aggs = [AggDescriptor::COUNT_STAR, agg(AggFunc::Sum, 4), agg(AggFunc::Sum, 5)];
    let row = |id: u64, grp: i64, f: f64, x: f32| Row {
        id,
        grp,
        f: f.to_bits(),
        x: x.to_bits(),
        ..Row::default()
    };
    let rows = [
        (row(1, 0, 0.1, 0.1), 3),
        (row(2, 0, 1e-9, 16_777_217.0), 3),
        (row(3, 0, 1.0, 0.3), 7),
        (row(4, -1, -4.0, -2.5), 1),
    ];
    // The model's sums of group 0, pinned: an F64 sum keeps the bits an f32
    // cannot, and an F32 value is widened before it is weighed.
    let want_f = 0.1f64 * 3.0 + 1e-9 * 3.0 + 1.0 * 7.0;
    let want_x = 0.1f32 as f64 * 3.0 + 16_777_217.0f32 as f64 * 3.0 + 0.3f32 as f64 * 7.0;
    let grp0: Vec<(Cells, i64)> = rows[..3].iter().map(|(r, w)| (cells(r), *w)).collect();
    assert_eq!(agg_of(agg(AggFunc::Sum, 4), &grp0), Some(want_f.to_le_bytes().to_vec()));
    assert_eq!(agg_of(agg(AggFunc::Sum, 5), &grp0), Some(want_x.to_le_bytes().to_vec()));

    let model: BTreeMap<Row, i64> = rows.iter().copied().collect();
    for group_cols in [&[][..], &[3], &[1], &[0]] {
        let want = aggregates(&model, group_cols, &aggs, false);
        let mut h = Harness::new(ReducePlan::from_wire(&schema(), group_cols, &aggs, false).unwrap());
        h.tick(batch(&rows));
        assert_eq!(h.state(group_cols), want, "group {group_cols:?}");

        let mut fold = fold_sink(group_cols, aggs.to_vec(), usize::MAX).unwrap();
        let mut all = RowFilter::for_read(&[], None, &schema()).unwrap();
        fold.push(Cow::Owned(batch(&rows)), &mut all).unwrap();
        assert_eq!(
            output_rows(&fold.finish(), group_cols),
            want,
            "ad hoc, group {group_cols:?}"
        );
    }
}

/// A wide extreme of a group whose rows follow another group's retraction in
/// the delta is stepped into its own group: the ordinal arm numbers rows, not
/// positive rows.
#[test]
fn a_wide_extreme_steps_its_own_group_past_another_groups_retraction() {
    let aggs = [AggDescriptor::COUNT_STAR, agg(AggFunc::Min, 6), agg(AggFunc::Max, 2)];
    let row = |id: u64, grp: i64, w: u128, s: &'static str| Row { id, grp, w, s: Some(s), ..Row::default() };
    let mut h = Harness::new(ReducePlan::from_wire(&schema(), &[3], &aggs, false).unwrap());
    let mut model = BTreeMap::new();
    let first = [
        (row(1, -1, 10, "m"), 1),
        (row(2, 0, 10, "m"), 1),
        (row(5, -1, 12, "k"), 1),
    ];
    // Row 1 leaves group -1; rows 3 and 4 bring group 0 a new MIN and a new MAX.
    let second = [
        (row(1, -1, 10, "m"), 0),
        (row(3, 0, 1, "a"), 1),
        (row(4, 0, 20, "z"), 1),
    ];
    for targets in [&first[..], &second[..]] {
        h.tick(batch(&delta_to(&mut model, targets)));
        assert_eq!(h.state(&[3]), aggregates(&model, &[3], &aggs, false));
    }
}

/// Two string extremes of one reduce each keep their own images: a group that
/// reads both off the index after a retraction gets its MIN and its MAX.
#[test]
fn two_string_extremes_keep_their_own_index_images() {
    let aggs = [
        AggDescriptor::COUNT_STAR,
        agg(AggFunc::Min, 2),
        agg(AggFunc::Max, 2),
        agg(AggFunc::Min, 0),
    ];
    let row = |id: u64, s: &'static str| Row {
        id,
        s: Some(s),
        val: Some(id as i64),
        ..Row::default()
    };
    let plan = ReducePlan::from_wire(&schema(), &[3], &aggs, false).unwrap();
    // A fixed-width ordinal's payload cell is the zeroed empty string, not what
    // the arena held: the cell is part of the entry's identity.
    let idx = plan.index_entries(&batch(&[(row(1, "aa"), 1)])).unwrap();
    assert_eq!(idx.len(), 3);
    assert_eq!(
        gnitz_wire::RowSource::get_col_ptr(&idx, 2, 0, 16),
        &[0u8; 16],
        "MIN(val), ordinal 2"
    );

    let mut h = Harness::new(plan);
    let mut model = BTreeMap::new();
    let first = [(row(1, "aa"), 1), (row(2, "mm"), 1), (row(3, "zz"), 1)];
    let second = [(row(1, "aa"), 0), (row(3, "zz"), 0), (row(4, "b\0b"), 1)];
    for targets in [&first[..], &second[..]] {
        h.tick(batch(&delta_to(&mut model, targets)));
        assert_eq!(h.state(&[3]), aggregates(&model, &[3], &aggs, false));
    }
}

/// Under a group key that packs by hashing, each extreme of a reduce with
/// several keys its entries as that extreme alone does: the same rows, group
/// keys, images and weights, over a NULL pattern of its own.
#[test]
fn extremes_under_a_hashed_group_key_each_key_their_entries_as_one_alone() {
    let (min, max) = (agg(AggFunc::Min, 0), agg(AggFunc::Max, 7));
    let row = |id: u64, val: Option<i64>, s: Option<&'static str>, n: Option<i16>| Row {
        id,
        grp: id as i64 % 3 - 1,
        val,
        s,
        n,
        ..Row::default()
    };
    let delta = batch(&[
        (row(1, Some(4), Some("aa"), None), 1),
        (row(2, None, None, Some(-3)), 2),
        (row(3, Some(-9), Some("zz"), Some(7)), 1),
        (row(4, Some(0), Some("aa"), None), 3),
        (row(5, None, Some("q"), Some(1)), 1),
    ])
    .into_consolidated();
    let entries = |group: &[u32], aggs: &[AggDescriptor]| {
        let plan = ReducePlan::from_wire(&schema(), group, aggs, false).unwrap();
        let idx = plan.index_entries(&delta).unwrap();
        let mb = idx.as_mem_batch();
        // Each entry with its ordinal byte cleared: the key's 9th byte from the end.
        (0..idx.len())
            .map(|i| {
                let mut key = mb.get_pk_bytes(i).to_vec();
                let ordinal = key.len() - 9;
                (std::mem::take(&mut key[ordinal]), key, idx.get_weight(i))
            })
            .collect::<Vec<_>>()
    };
    // A string and a column folded behind two packed ones.
    for group in [&[3, 2][..], &[0, 1, 3]] {
        let both = entries(group, &[AggDescriptor::COUNT_STAR, min, max]);
        let alone = [min, max].map(|a| entries(group, &[AggDescriptor::COUNT_STAR, a]));
        let want: Vec<_> = alone
            .iter()
            .enumerate()
            .flat_map(|(j, e)| e.iter().map(move |(_, key, w)| (j as u8, key.clone(), *w)))
            .collect();
        assert_eq!(both, want, "group {group:?}");
        assert_eq!(both.len(), 6, "three non-NULL values per extreme");
    }
}

/// One plan run epoch after epoch, its buffers kept across them, emits what a
/// plan built afresh for each epoch emits over the same traces, PKs and weights
/// included. Each case's epochs alternate the hashed numbering, its fallback to
/// a sort, row runs and a hashed epoch whose first group was the previous
/// hashed epoch's last.
#[test]
fn a_plan_reused_across_epochs_emits_what_a_fresh_plan_emits() {
    let row = |id: u64, val: i64, w: u128| Row {
        id,
        grp: id as i64 % 7,
        val: Some(val),
        s: Some(["a", "q", "z"][id as usize % 3]),
        w,
        n: Some(1),
        ..Row::default()
    };
    let ins = |rows: Vec<Row>| -> Vec<(Row, i64)> { rows.into_iter().map(|r| (r, 1)).collect() };
    let extremes = vec![
        AggDescriptor::COUNT_STAR,
        agg(AggFunc::Min, 3),
        agg(AggFunc::Max, 2),
        agg(AggFunc::Sum, 0),
    ];
    let linear = vec![AggDescriptor::COUNT_STAR, agg(AggFunc::Sum, 3)];

    // A packed key over two nullable payload columns, with a value index.
    let packed: Vec<Vec<(Row, i64)>> = vec![
        // 24 rows in 3 groups: hashed.
        ins((0..24).map(|id| row(id, id as i64 % 3, 0)).collect()),
        // 40 rows in 40 groups: the hash gives up, the rows are sorted.
        ins((100..140).map(|id| row(id, id as i64, 0)).collect()),
        // One row: one run.
        ins(vec![row(200, 1, 0)]),
        // Retractions and inserts in 3 groups, the last row's group numbered 1.
        (0..12)
            .map(|id| (row(id, id as i64 % 3, 0), 0))
            .chain((300..311).map(|id| (row(id, (id as i64 - 300) % 3, 0), 1)))
            .collect(),
        // The first row in the group the previous epoch numbered 1, which this
        // epoch numbers 0.
        ins((400..420).map(|id| row(id, (id as i64 - 399) % 3, 0)).collect()),
    ];
    // A 16-byte payload image: the hash keeps identities, the sort none.
    let image: Vec<Vec<(Row, i64)>> = vec![
        ins((0..24).map(|id| row(id, id as i64, id as u128 % 3)).collect()),
        ins((100..140).map(|id| row(id, -(id as i64), id as u128)).collect()),
        ins((200..220).map(|id| row(id, id as i64, 1 + id as u128 % 2)).collect()),
        (0..12).map(|id| (row(id, id as i64, id as u128 % 3), 0)).collect(),
        ins(vec![row(500, 5, 7)]),
    ];
    // The leading PK column under exact-linear aggregates: raw deltas are
    // numbered, consolidated ones taken as runs.
    let leading: Vec<Vec<(Row, i64)>> = vec![
        ins((0..20)
            .map(|i| Row { grp: i, ..row((i as u64 * 3) % 4, 1, 0) })
            .collect()),
        ins((0..40).map(|i| row(1000 - i, 2, 0)).collect()),
        ins((0..20)
            .map(|i| Row { grp: 100 + i, ..row(i as u64 / 5, 3, 0) })
            .collect()),
        ins((0..6).map(|i| row(2000 + i, 4, 0)).collect()),
        ins((0..20)
            .map(|i| Row {
                grp: 200 + i,
                ..row(3 - (i as u64 * 3) % 4, 5, 0)
            })
            .collect()),
    ];
    let consolidate_leading = [false, false, true, true, false];

    type Ticks = [Vec<(Row, i64)>];
    let cases: [(&[u32], &[AggDescriptor], &Ticks); 3] = [
        (&[0, 7], &extremes, &packed),
        (&[6], &extremes, &image),
        (&[1], &linear, &leading),
    ];
    for (group_cols, aggs, ticks) in cases {
        let plan = || ReducePlan::from_wire(&schema(), group_cols, aggs, false).unwrap();
        let (mut reused, mut fresh) = (Harness::new(plan()), Harness::new(plan()));
        let mut model = BTreeMap::new();
        for (t, targets) in ticks.iter().enumerate() {
            let delta = batch(&delta_to(&mut model, targets));
            let delta = match group_cols == [1] && consolidate_leading[t] {
                true => delta.into_consolidated(),
                false => delta,
            };
            fresh.plan = plan();
            let (a, b) = (reused.tick(delta.clone()), fresh.tick(delta));
            assert_eq!(weighted_rows(&a), weighted_rows(&b), "{group_cols:?}: epoch {t}");
            assert_eq!(
                reused.state(group_cols),
                aggregates(&model, group_cols, aggs, false),
                "{group_cols:?}: epoch {t}"
            );
        }
    }
}

/// An epoch that grows the plan's buffers past what it keeps, such as a
/// backfill, leaves the plan without them; the next small epoch builds them
/// again and keeps them.
#[test]
fn a_plan_drops_scratch_a_large_epoch_grew() {
    let aggs = [AggDescriptor::COUNT_STAR, agg(AggFunc::Min, 0)];
    let mut h = Harness::new(ReducePlan::from_wire(&schema(), &[1], &aggs, false).unwrap());
    let row = |id: u64| Row {
        id,
        val: Some(id as i64),
        ..Row::default()
    };
    let mut model = BTreeMap::new();
    let ins: Vec<(Row, i64)> = (0..5000).map(|id| (row(id), 1)).collect();
    h.tick(batch(&delta_to(&mut model, &ins)));
    assert!(h.plan.scratch.is_none());
    let del: Vec<(Row, i64)> = (0..3).map(|id| (row(id), 0)).collect();
    h.tick(batch(&delta_to(&mut model, &del)));
    assert!(h.plan.scratch.is_some());
    assert_eq!(h.state(&[1]), aggregates(&model, &[1], &aggs, false));
}

/// A float sum is of its rows' finite values, and the infinity counts carry
/// the rest, so a sum that held +inf, -inf or NaN returns to the sum of its
/// rows once they are retracted — maintained and ad hoc.
#[test]
fn a_float_sum_recovers_once_its_non_finite_rows_are_retracted() {
    let row = |id: u64, f: f64, x: f32| Row {
        id,
        f: f.to_bits(),
        x: x.to_bits(),
        ..Row::default()
    };
    let (inf, nan) = (f64::INFINITY, f64::NAN);
    let aggs = [
        AggDescriptor::COUNT_STAR,
        agg(AggFunc::FiniteSum, 4),
        agg(AggFunc::FiniteSum, 5),
        agg(AggFunc::CountPosInf, 4),
        agg(AggFunc::CountNegInf, 4),
        agg(AggFunc::CountPosInf, 5),
        agg(AggFunc::CountNegInf, 5),
    ];
    let ticks: [&[(Row, i64)]; 4] = [
        &[
            (row(1, 1.0, 1.0), 1),
            (row(2, inf, -inf as f32), 1),
            (row(3, nan, 2.5), 2),
        ],
        &[(row(2, inf, -inf as f32), -1), (row(4, -inf, nan as f32), 1)],
        &[(row(5, 0.5, inf as f32), 3), (row(4, -inf, nan as f32), -1)],
        &[(row(3, nan, 2.5), -2), (row(5, 0.5, inf as f32), -3)],
    ];
    for group_cols in [&[][..], &[3]] {
        let mut h = Harness::new(ReducePlan::from_wire(&schema(), group_cols, &aggs, false).unwrap());
        let mut model = BTreeMap::new();
        for t in ticks {
            h.tick(batch(t));
            for &(r, w) in t {
                *model.entry(r).or_insert(0) += w;
            }
            model.retain(|_, w| *w != 0);
            let want = aggregates(&model, group_cols, &aggs, false);
            assert_eq!(h.state(group_cols), want, "group {group_cols:?}");

            let mut fold = fold_sink(group_cols, aggs.to_vec(), usize::MAX).unwrap();
            let mut all = RowFilter::for_read(&[], None, &schema()).unwrap();
            fold.push(
                Cow::Owned(batch(&model.iter().map(|(&r, &w)| (r, w)).collect::<Vec<_>>())),
                &mut all,
            )
            .unwrap();
            assert_eq!(
                output_rows(&fold.finish(), group_cols),
                want,
                "ad hoc, group {group_cols:?}"
            );
        }
        let out = h.out_trace.cursor().materialize();
        let mb = out.as_mem_batch();
        let n = out.schema().num_columns();
        let cell_at = |c: usize| cell(&mb, out.schema().locate(c), 0).unwrap();
        assert_eq!(
            f64::from_le_bytes(cell_at(n - 6).try_into().unwrap()),
            1.0,
            "SUM(f), group {group_cols:?}"
        );
        assert_eq!(
            f64::from_le_bytes(cell_at(n - 5).try_into().unwrap()),
            1.0,
            "SUM(x), group {group_cols:?}"
        );
    }
}
