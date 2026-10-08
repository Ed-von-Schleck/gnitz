//! Reduce tests: the operator, its global split and the ad-hoc fold, each held
//! to the aggregates of its input computed from their SQL definitions. The
//! operator is driven epoch by epoch through the production populate and
//! integrate paths.

use std::cmp::Ordering;
use std::collections::BTreeMap;

use proptest::prelude::*;

use crate::repr::{Batch, BatchBuilder};
use crate::schema::{ColumnTable, SchemaColumn, SchemaDescriptor, SchemaFacts, TypeCode};
use crate::test_support::{assert_folds, cell, le_cell, TestTrace};
use gnitz_wire::{AggDescriptor, AggFunc, AggReadSpec, ReadSink, ReduceOutKey, SinkKind};

use super::op_reduce::op_reduce;
use super::plan::ReducePlan;
use crate::algebra::SinkPlan;

/// `[I64 val NULL, U64 id (pk), STRING s NULL, I64 grp (pk), F64 f, F32 x,
/// U128 w, I16 n NULL]`, keyed `(id, grp)`.
fn schema() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::I64, true),
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::String, true),
            SchemaColumn::new(TypeCode::I64, false),
            SchemaColumn::new(TypeCode::F64, false),
            SchemaColumn::new(TypeCode::F32, false),
            SchemaColumn::new(TypeCode::U128, false),
            SchemaColumn::new(TypeCode::I16, true),
        ],
        &[1, 3],
    )
}

/// The ad-hoc fold of `aggs` by `group_cols` over [`schema`], as a read's sink.
fn fold_sink(group_cols: &[u32], aggs: Vec<AggDescriptor>, group_cap: usize) -> Result<SinkPlan, String> {
    let spec = AggReadSpec { group_cols: group_cols.to_vec(), aggs };
    let sink = ReadSink { map: None, kind: SinkKind::Fold(spec) };
    SinkPlan::from_wire(&schema(), &sink, group_cap)
}

/// One input row, a field per column in schema order; `f` and `x` as their bits.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
struct Row {
    val: Option<i64>,
    id: u64,
    s: Option<&'static str>,
    grp: i64,
    f: u64,
    x: u32,
    w: u128,
    n: Option<i16>,
}

/// One native cell per column, `None` for NULL.
type Cells = Vec<Option<Vec<u8>>>;

fn cells(r: &Row) -> Cells {
    vec![
        r.val.map(|v| v.to_le_bytes().to_vec()),
        Some(r.id.to_le_bytes().to_vec()),
        r.s.map(|s| s.as_bytes().to_vec()),
        Some(r.grp.to_le_bytes().to_vec()),
        Some(r.f.to_le_bytes().to_vec()),
        Some(r.x.to_le_bytes().to_vec()),
        Some(r.w.to_le_bytes().to_vec()),
        r.n.map(|n| n.to_le_bytes().to_vec()),
    ]
}

fn batch(rows: &[(Row, i64)]) -> Batch {
    let mut b = BatchBuilder::new(&schema());
    for &(r, w) in rows {
        b.begin_row_natives(&[r.id as u128, r.grp as u128], w);
        b.put_opt_int(r.val.map(|v| v as u128));
        match r.s {
            Some(s) => b.put_string(s),
            None => b.put_null(),
        }
        b.put_int(r.f as u128);
        b.put_int(r.x as u128);
        b.put_int(r.w);
        b.put_opt_int(r.n.map(|n| n as u128));
        b.end_row();
    }
    b.finish()
}

/// The operator with its two traces, driven the way the VM drives it.
pub(super) struct Harness {
    plan: ReducePlan,
    index: Option<TestTrace>,
    pub(super) trace_out: TestTrace,
}

impl Harness {
    pub(super) fn new(plan: ReducePlan) -> Self {
        let index = plan.index_schema().map(|schema| TestTrace::new(*schema));
        let trace_out = TestTrace::new(*plan.output_schema());
        Harness { plan, index, trace_out }
    }

    /// Add `delta`'s entries to the value index, as the VM does ahead of
    /// `op_reduce`.
    pub(super) fn index(&mut self, delta: &Batch) {
        if let (Some(index), Some(entries)) = (&mut self.index, self.plan.index_batch(delta)) {
            index.ingest(entries);
        }
    }

    /// `op_reduce` over `delta` against the two traces, each opened at exactly
    /// the keys the operator asks for.
    pub(super) fn reduce(&self, delta: &Batch) -> Batch {
        let mut trace_out = |first: &[u8], last: &[u8]| self.trace_out.cursor_within(first, last);
        let mut index = |first: &[u8], last: &[u8]| {
            let index = self.index.as_ref().expect("a plan with a value index");
            index.cursor_within(first, last)
        };
        op_reduce(delta, &mut trace_out, Some(&mut index), &self.plan)
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
        self.trace_out.ingest(out.clone());
        out
    }

    fn state(&self, group_cols: &[u32]) -> BTreeMap<Cells, i64> {
        output_rows(&self.trace_out.cursor().materialize(), group_cols)
    }
}

/// A reduce output over `group_cols` as its Z-set of `[group columns…,
/// aggregates…]` rows, a synthetic key left out.
fn output_rows(b: &Batch, group_cols: &[u32]) -> BTreeMap<Cells, i64> {
    let mb = b.as_mem_batch();
    let out = b.schema();
    let skip = usize::from(schema().reduce_out_key(group_cols) == ReduceOutKey::SyntheticFold);
    let mut z = BTreeMap::new();
    for r in 0..b.count {
        let row: Cells = (skip..out.num_columns()).map(|c| cell(&mb, out.locate(c), r)).collect();
        *z.entry(row).or_insert(0) += b.get_weight(r);
    }
    z.retain(|_, w| *w != 0);
    z
}

/// An integer cell of column type `tc`, widened.
fn int(tc: TypeCode, c: &[u8]) -> i128 {
    let unused = 128 - 8 * c.len() as u32;
    match tc.is_signed_int() {
        true => (le_cell(c) << unused) as i128 >> unused,
        false => le_cell(c) as i128,
    }
}

/// The typed order of two cells of column type `tc`.
fn cmp_cells(tc: TypeCode, a: &[u8], b: &[u8]) -> Ordering {
    match tc {
        TypeCode::String => a.cmp(b),
        TypeCode::F64 => {
            f64::from_le_bytes(a.try_into().unwrap()).total_cmp(&f64::from_le_bytes(b.try_into().unwrap()))
        }
        TypeCode::F32 => {
            f32::from_le_bytes(a.try_into().unwrap()).total_cmp(&f32::from_le_bytes(b.try_into().unwrap()))
        }
        TypeCode::U128 => le_cell(a).cmp(&le_cell(b)),
        _ => int(tc, a).cmp(&int(tc, b)),
    }
}

/// `agg` over one group's weighted rows, by its SQL definition.
fn fold(agg: AggDescriptor, rows: &[(Cells, i64)]) -> Option<Vec<u8>> {
    let col = agg.col_idx as usize;
    let tc = schema().columns[col].type_code;
    let live = || rows.iter().filter_map(|(r, w)| r[col].as_deref().map(|c| (c, *w)));
    let i64_cell = |v: i64| Some(v.to_le_bytes().to_vec());
    match agg.agg_op {
        AggFunc::Count => i64_cell(rows.iter().map(|(_, w)| w).sum()),
        AggFunc::CountNonNull => i64_cell(live().map(|(_, w)| w).sum()),
        AggFunc::Sum if tc == TypeCode::F32 => {
            let sum = live().fold(0f64, |a, (c, w)| {
                a + f32::from_le_bytes(c.try_into().unwrap()) as f64 * w as f64
            });
            Some(sum.to_le_bytes().to_vec())
        }
        AggFunc::Sum => i64_cell(live().fold(0, |a, (c, w)| a.wrapping_add((int(tc, c) as i64).wrapping_mul(w)))),
        AggFunc::Min | AggFunc::Max => {
            let mut present: Vec<&[u8]> = live().map(|(c, _)| c).collect();
            present.sort_by(|a, b| cmp_cells(tc, a, b));
            let extreme = match agg.agg_op {
                AggFunc::Min => present.first(),
                _ => present.last(),
            };
            extreme.map(|c| c.to_vec())
        }
    }
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
        let row = cells(r);
        let key = group_cols.iter().map(|&c| row[c as usize].clone()).collect();
        groups.entry(key).or_default().push((row, w));
    }
    groups
        .into_iter()
        .map(|(mut key, rows)| {
            key.extend(aggs.iter().map(|&a| fold(a, &rows)));
            (key, 1)
        })
        .collect()
}

/// Every group key form: V₀, the whole PK, its leading column, a non-leading
/// signed PK column's image, a 16-byte payload column's image, the fold of a
/// nullable column, of a column and a string, and of a set one column wider
/// than a value index's key packs.
const GROUPS: [&[u32]; 8] = [&[], &[1, 3], &[1], &[3], &[6], &[0], &[3, 2], &[0, 1, 3]];

const VALS: [i64; 5] = [i64::MIN, -1, 0, 1, i64::MAX];
const IDS: [u64; 4] = [0, 1, 1 << 63, u64::MAX];
/// Strings that are prefixes of one another, one escaping `0x00`, and two that
/// share more leading bytes than an index key's lead slot holds.
const STRS: [&str; 6] = [
    "",
    "a",
    "ab",
    "\0b",
    "shared-lead-slot-prefix-x",
    "shared-lead-slot-prefix-y",
];
/// Floats `total_cmp` tells apart though `==` does not, and the ones it places
/// past every finite value.
const FLOATS: [f64; 6] = [-0.0, 0.0, -1.5, 2.5, f64::INFINITY, f64::NAN];
/// Dyadic and non-zero, so a sum of them is exact in any order and never `-0.0`.
const DYADICS: [f32; 4] = [-1.5, 0.25, 2.0, 4.5];
/// Pairs equal in their high and in their low eight bytes.
const WIDES: [u128; 4] = [1, 1 << 64, (1 << 64) + 1, u128::MAX];
const NARROWS: [i16; 4] = [i16::MIN, -1, 1, i16::MAX];

fn arb_row() -> impl Strategy<Value = Row> {
    (
        prop::option::of(prop::sample::select(&VALS[..])),
        prop::sample::select(&IDS[..]),
        prop::option::of(prop::sample::select(&STRS[..])),
        -1i64..1,
        prop::sample::select(&FLOATS[..]),
        prop::sample::select(&DYADICS[..]),
        prop::sample::select(&WIDES[..]),
        prop::option::of(prop::sample::select(&NARROWS[..])),
    )
        .prop_map(|(val, id, s, grp, f, x, w, n)| Row {
            val,
            id,
            s,
            grp,
            f: f.to_bits(),
            x: x.to_bits(),
            w,
            n,
        })
}

/// Each tick moves some rows of a small pool to a new multiplicity in `0..=3`,
/// so a row recurs across ticks and most ticks retract.
fn arb_ticks() -> impl Strategy<Value = Vec<Vec<(Row, i64)>>> {
    prop::collection::vec(arb_row(), 1..6).prop_flat_map(|pool| {
        prop::collection::vec(
            prop::collection::vec((prop::sample::select(pool), 0i64..=3), 0..6),
            1..6,
        )
    })
}

/// The retractions and insertions moving `model` to each `target` — so a delta
/// carries cancelling pairs and weight-0 rows, and the integrated input stays
/// a relation.
fn delta_to(model: &mut BTreeMap<Row, i64>, targets: &[(Row, i64)]) -> Vec<(Row, i64)> {
    let mut delta = Vec::new();
    for &(row, target) in targets {
        let cur = model.insert(row, target).unwrap_or(0);
        delta.extend([(row, -cur), (row, target)]);
    }
    model.retain(|_, w| *w != 0);
    delta
}

const fn agg(agg_op: AggFunc, col_idx: u32) -> AggDescriptor {
    AggDescriptor { col_idx, agg_op }
}

/// A COUNT(*) among up to three aggregates over any column that admits them.
/// The summed float is `x` alone: its values sum exactly, where `f`'s do not.
fn arb_aggs() -> impl Strategy<Value = Vec<AggDescriptor>> {
    let one = prop_oneof![
        (0u32..8).prop_map(|c| agg(AggFunc::CountNonNull, c)),
        prop::sample::select(&[0u32, 1, 3, 5, 7][..]).prop_map(|c| agg(AggFunc::Sum, c)),
        (0u32..8).prop_map(|c| agg(AggFunc::Min, c)),
        (0u32..8).prop_map(|c| agg(AggFunc::Max, c)),
    ];
    (prop::collection::vec(one, 0..4), any::<prop::sample::Index>()).prop_map(|(mut aggs, at)| {
        aggs.insert(at.index(aggs.len() + 1), AggDescriptor::COUNT_STAR);
        aggs
    })
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
        ticks in arb_ticks(),
    ) {
        let ground = seeds && group_cols.is_empty();
        let mut h = Harness::new(ReducePlan::from_wire(&schema(), group_cols, &aggs, ground).unwrap());
        let mut model = BTreeMap::new();
        h.tick(batch(&[]));
        prop_assert_eq!(h.state(group_cols), aggregates(&model, group_cols, &aggs, ground));
        for targets in &ticks {
            let delta = batch(&delta_to(&mut model, targets));
            h.tick(if folded { delta.into_consolidated() } else { delta });
            prop_assert_eq!(h.state(group_cols), aggregates(&model, group_cols, &aggs, ground));
        }
    }

    /// Two workers' partials, relayed to one combine, maintain the global
    /// aggregates, the ground row over no input included.
    #[test]
    fn two_workers_partials_combine_to_the_global_aggregates(
        aggs in arb_aggs().prop_map(|mut aggs| {
            // The float sum has no partial.
            aggs.retain(|d| !(d.agg_op == AggFunc::Sum && d.col_idx == 5));
            aggs
        }),
        ticks in arb_ticks(),
    ) {
        let partial = || Harness::new(ReducePlan::partial(&schema(), &aggs).unwrap().unwrap());
        let mut workers = [partial(), partial()];
        let partials = *workers[0].plan.output_schema();
        let mut combine = Harness::new(ReducePlan::combine(&partials, &aggs, true).unwrap());
        let mut model = BTreeMap::new();
        combine.tick(Batch::empty_with_schema(&partials));
        prop_assert_eq!(combine.state(&[]), aggregates(&model, &[], &aggs, true));
        for targets in &ticks {
            let delta = delta_to(&mut model, targets);
            // Sharded by id, as the exchange routes a keyed input.
            let relayed: Vec<Batch> = (0..2)
                .map(|w| {
                    let mine: Vec<_> = delta.iter().copied().filter(|(r, _)| r.id % 2 == w).collect();
                    workers[w as usize].tick(batch(&mine))
                })
                .collect();
            combine.tick(Batch::concat(&partials, relayed.iter().map(Batch::as_mem_batch)));
            prop_assert_eq!(combine.state(&[]), aggregates(&model, &[], &aggs, true));
        }
    }

    /// The ad-hoc fold, over any chunking of a scan and any survivor ranges of
    /// each chunk, is the aggregates of the rows in those ranges, in the layout
    /// the view's reduce emits.
    #[test]
    fn the_adhoc_fold_is_the_aggregates_of_the_ranges_it_folded(
        group_cols in prop::sample::select(&GROUPS[..]),
        aggs in arb_aggs(),
        scan in prop::collection::btree_map(arb_row(), (1i64..=3, any::<bool>()), 0..12),
        chunk in 1usize..5,
    ) {
        let mut fold = fold_sink(group_cols, aggs.clone(), usize::MAX).unwrap();
        let view = ReducePlan::from_wire(&schema(), group_cols, &aggs, false).unwrap();
        prop_assert_eq!(fold.output_schema(), view.output_schema());

        let scan: Vec<(Row, i64, bool)> = scan.into_iter().map(|(r, (w, survives))| (r, w, survives)).collect();
        for rows in scan.chunks(chunk) {
            let mut ranges: Vec<(usize, usize)> = Vec::new();
            for i in (0..rows.len()).filter(|&i| rows[i].2) {
                match ranges.last_mut() {
                    Some(last) if last.1 == i => last.1 = i + 1,
                    _ => ranges.push((i, i + 1)),
                }
            }
            let rows: Vec<(Row, i64)> = rows.iter().map(|&(r, w, _)| (r, w)).collect();
            fold.push(&batch(&rows), &mut ranges).unwrap();
        }
        let folded: BTreeMap<Row, i64> = scan.iter().filter(|r| r.2).map(|&(r, w, _)| (r, w)).collect();
        prop_assert_eq!(
            output_rows(&fold.finish(), group_cols),
            aggregates(&folded, group_cols, &aggs, group_cols.is_empty())
        );
    }
}

/// A group handed more rows in one epoch than the pre-step covers takes its
/// extremes, the burst's own among them, from the index.
#[test]
fn a_group_past_the_prestep_cap_reads_its_extremes_off_the_index() {
    let aggs = [
        AggDescriptor::COUNT_STAR,
        agg(AggFunc::Min, 0),
        agg(AggFunc::Max, 2),
        agg(AggFunc::Sum, 0),
    ];
    let row = |id: u64, val: i64, s: &'static str| Row {
        id,
        val: Some(val),
        s: Some(s),
        ..Row::default()
    };
    let mut h = Harness::new(ReducePlan::from_wire(&schema(), &[3], &aggs, false).unwrap());
    let mut model = BTreeMap::new();
    let history = [(row(0, 0, "m"), 1)];
    let burst: Vec<(Row, i64)> = (1..=200)
        .map(|id| (row(id, id as i64 - 100, ["a", "z"][id as usize % 2]), 1))
        .collect();
    for targets in [&history[..], &burst[..]] {
        h.tick(batch(&delta_to(&mut model, targets)));
        assert_eq!(h.state(&[3]), aggregates(&model, &[3], &aggs, false));
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
    fold.push(&rows(&[7, 8, 7]), &mut vec![(0, 3)]).unwrap();
    let err = fold.push(&rows(&[8, 9]), &mut vec![(0, 2)]).unwrap_err();
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
        assert!(err.starts_with("scan_spec fold: ") && err.contains(why), "{err}");
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
        let cols: Vec<(TypeCode, bool)> = s.columns[..s.num_columns()]
            .iter()
            .map(|c| (c.type_code, c.nullable))
            .collect();
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

/// Only counts and integer sums split into per-worker partials: a float sum
/// depends on the order a split would change, and an extreme has no partial a
/// retraction could be subtracted from.
#[test]
fn only_counts_and_integer_sums_split() {
    for (extra, splits) in [
        (agg(AggFunc::Count, 4), true),
        (agg(AggFunc::CountNonNull, 2), true),
        (agg(AggFunc::Sum, 7), true),
        (agg(AggFunc::Sum, 5), false),
        (agg(AggFunc::Sum, 4), false),
        (agg(AggFunc::Min, 0), true),
        (agg(AggFunc::Max, 0), true),
    ] {
        let partial = ReducePlan::partial(&schema(), &[AggDescriptor::COUNT_STAR, extra]).unwrap();
        assert_eq!(partial.is_some(), splits, "{extra:?}");
    }
}

// ---- proposed tests (appended to stream/reduce/tests/reduce.rs) ----

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
    let idx = plan.index_batch(&batch(&[(row, 1)])).unwrap();
    let mb = idx.as_mem_batch();
    let key = mb.get_pk_bytes(0);
    let n = key.len();
    assert_eq!(key[n - 17], 0, "MIN(val) is ordinal 0");
    assert_eq!(&key[n - 16..n - 8], &(7u64 ^ (1 << 63)).to_be_bytes(), "the I64 image");
    assert_eq!(&key[n - 8..], &[0u8; 8], "zero padding");
}

/// SUM over an F64 column is each non-NULL value times its weight, added in row
/// order — through the run arm, the ordinal arm and the ad-hoc fold.
#[test]
fn a_float_sum_weighs_each_value() {
    let aggs = [AggDescriptor::COUNT_STAR, agg(AggFunc::Sum, 4)];
    let row = |id: u64, grp: i64, f: f64| Row {
        id,
        grp,
        f: f.to_bits(),
        ..Row::default()
    };
    let rows = [
        (row(1, 0, 1.5), 3),
        (row(2, 0, 0.25), 2),
        (row(3, -1, 8.0), 5),
        (row(4, 0, -4.0), 1),
    ];
    for group_cols in [&[][..], &[3], &[1], &[0]] {
        let mut h = Harness::new(ReducePlan::from_wire(&schema(), group_cols, &aggs, false).unwrap());
        h.tick(batch(&rows));
        let sums: Vec<f64> = {
            let b = h.trace_out.cursor().materialize();
            let mb = b.as_mem_batch();
            let loc = b.schema().locate(b.schema().num_columns() - 1);
            (0..b.count)
                .map(|r| f64::from_le_bytes(cell(&mb, loc, r).unwrap().try_into().unwrap()))
                .collect()
        };
        let mut want: Vec<f64> = match group_cols {
            [] | [0] => vec![1.5 * 3.0 + 0.25 * 2.0 + 8.0 * 5.0 - 4.0],
            [3] => vec![40.0, 1.5 * 3.0 + 0.25 * 2.0 - 4.0],
            _ => vec![4.5, 0.5, 40.0, -4.0],
        };
        let mut got = sums.clone();
        got.sort_by(f64::total_cmp);
        want.sort_by(f64::total_cmp);
        assert_eq!(got, want, "group {group_cols:?}");

        let mut fold = fold_sink(group_cols, aggs.to_vec(), usize::MAX).unwrap();
        fold.push(&batch(&rows), &mut vec![(0, rows.len())]).unwrap();
        let b = fold.finish();
        let mb = b.as_mem_batch();
        let loc = b.schema().locate(b.schema().num_columns() - 1);
        let mut got: Vec<f64> = (0..b.count)
            .map(|r| f64::from_le_bytes(cell(&mb, loc, r).unwrap().try_into().unwrap()))
            .collect();
        got.sort_by(f64::total_cmp);
        assert_eq!(got, want, "ad hoc, group {group_cols:?}");
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
        h.tick(batch(&delta_to(&mut model, targets)).into_consolidated());
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
    let idx = plan.index_batch(&batch(&[(row(1, "aa"), 1)])).unwrap();
    assert_eq!(idx.count, 3);
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
        h.tick(batch(&delta_to(&mut model, targets)).into_consolidated());
        assert_eq!(h.state(&[3]), aggregates(&model, &[3], &aggs, false));
    }
}

/// A float SUM adds in f64: an F32 value is widened before it is weighed, and
/// an F64 sum keeps the bits an f32 cannot.
#[test]
fn a_float_sum_is_carried_in_f64() {
    let row = |id: u64, f: f64, x: f32| Row {
        id,
        f: f.to_bits(),
        x: x.to_bits(),
        ..Row::default()
    };
    let rows = [
        (row(1, 0.1, 0.1), 3),
        (row(2, 1e-9, 16_777_217.0), 3),
        (row(3, 1.0, 0.3), 7),
    ];
    let want_f = [0.1f64 * 3.0, 1e-9 * 3.0, 1.0 * 7.0].iter().fold(0f64, |a, v| a + v);
    let want_x = [0.1f32 as f64 * 3.0, 16_777_217.0f32 as f64 * 3.0, 0.3f32 as f64 * 7.0]
        .iter()
        .fold(0f64, |a, v| a + v);
    let aggs = [AggDescriptor::COUNT_STAR, agg(AggFunc::Sum, 4), agg(AggFunc::Sum, 5)];
    let read = |b: &Batch| {
        let mb = b.as_mem_batch();
        let n = b.schema().num_columns();
        let f = |c: usize| f64::from_le_bytes(cell(&mb, b.schema().locate(c), 0).unwrap().try_into().unwrap());
        assert_eq!(b.count, 1);
        (f(n - 2), f(n - 1))
    };
    for group_cols in [&[][..], &[3]] {
        let mut h = Harness::new(ReducePlan::from_wire(&schema(), group_cols, &aggs, false).unwrap());
        h.tick(batch(&rows));
        assert_eq!(
            read(&h.trace_out.cursor().materialize()),
            (want_f, want_x),
            "reduce, group {group_cols:?}"
        );
        let mut fold = fold_sink(group_cols, aggs.to_vec(), usize::MAX).unwrap();
        fold.push(&batch(&rows), &mut vec![(0, rows.len())]).unwrap();
        assert_eq!(read(&fold.finish()), (want_f, want_x), "ad hoc, group {group_cols:?}");
    }
}
