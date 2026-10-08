//! Dispatch-loop tests: epochs of hand-built programs, one per path the VM
//! itself owns — operator semantics are `gnitz-zset`'s to test.

use super::fixtures::*;
use super::*;
use crate::test_support::{
    join_reference, make_batch_u128, make_batch_u128_raw, make_schema_u128_i64, weighted_rows, zset_of,
};
use gnitz_wire::{AggDescriptor, AggFunc, TypeCode};
use gnitz_zset::repr::{Batch, BatchBuilder};
use gnitz_zset::schema::{SchemaColumn, SchemaDescriptor};

/// Each row's payload cells as signed integers of their own width, with its
/// weight, sorted — the Z-set of an output whose key the test need not spell.
/// The share of rows a worker owns by their whole PK.
fn own_rows(schema: &SchemaDescriptor) -> std::rc::Rc<algebra::ScatterPlan> {
    std::rc::Rc::new(algebra::ScatterPlan::native(gnitz_zset::algebra::Placement::full_pk(
        schema,
    )))
}

fn int_rows(b: &Batch) -> Vec<(Vec<Option<i128>>, i64)> {
    let int = |cell: Vec<u8>| {
        let fill = if cell.last().is_some_and(|&hi| hi & 0x80 != 0) {
            0xFF
        } else {
            0
        };
        let mut le = [fill; 16];
        le[..cell.len()].copy_from_slice(&cell);
        i128::from_le_bytes(le)
    };
    let mut rows: Vec<_> = weighted_rows(b)
        .into_iter()
        .map(|((_, cells), w)| (cells.into_iter().map(|c| c.map(int)).collect(), w))
        .collect();
    rows.sort();
    rows
}

#[test]
fn a_filter_feeds_the_negate_only_its_passing_rows() {
    let schema = make_schema_u128_i64();
    let mut p = TestPlan::default();
    let r0 = p.seed(schema);
    let kept = p.push(r0, schema, filter_gt(&schema, 1, 0));
    let out = p.push(kept, schema, Op::Negate);
    let mut vm = p.open(out);

    let input = make_batch_u128(&schema, &[(1, 1, 10), (2, 1, -5), (3, 1, 20)]);
    assert_rows(&vm.epoch(r0, input), &[(1, -1, 10), (3, -1, 20)]);
}

/// `A + B`, the second operand a filter of the first: both non-empty, the
/// second empty, and both empty, epoch after epoch on one program — so each
/// epoch also sees nothing of the one before.
#[test]
fn a_union_sums_its_operands_in_every_epoch() {
    let schema = make_schema_u128_i64();
    let mut p = TestPlan::default();
    let a = p.seed(schema);
    let b = p.push(a, schema, filter_gt(&schema, 1, 15));
    let out = p.push(a, schema, Op::Union { in_b: b });
    let mut vm = p.open(out);

    type Rows = &'static [(u128, i64, i64)];
    let cases: [(Rows, Rows); 3] = [
        (
            &[(1, 1, 10), (2, 1, 20), (3, 1, 30)],
            &[(1, 1, 10), (2, 2, 20), (3, 2, 30)],
        ),
        (&[(5, 1, 5)], &[(5, 1, 5)]),
        (&[], &[]),
    ];
    for (rows, want) in cases {
        assert_rows(&vm.epoch(a, make_batch_u128(&schema, rows)), want);
    }
}

/// A union hands its own register's schema to the kernel, so an operand it
/// passes through — the other being empty — leaves under the merged schema: the
/// label an exchange round gathers the peers' rows under.
#[test]
fn a_union_passes_an_operand_through_under_its_merged_schema() {
    let pk = SchemaColumn::new(TypeCode::U128, false);
    let not_null = SchemaDescriptor::new(&[pk, SchemaColumn::new(TypeCode::I64, false)], &[0]);
    let nullable = SchemaDescriptor::new(&[pk, SchemaColumn::new(TypeCode::I64, true)], &[0]);
    let merged = algebra::union_nullability_merge(&not_null, &nullable).unwrap();

    let mut p = TestPlan::default();
    let a = p.seed(not_null);
    let none = p.push(a, nullable, filter_gt(&not_null, 1, 0));
    let out = p.push(a, merged, Op::Union { in_b: none });
    let mut vm = p.open(out);

    let got = vm.epoch(a, make_batch_u128(&not_null, &[(1, 1, -3)]));
    assert_eq!(*got.schema(), merged);
    assert_rows(&got, &[(1, 1, -3)]);
}

/// A `Union` with `in_a == in_b` is `Z + Z`, which doubles every weight. The double saturates,
/// so a consolidated operand's `i64::MIN` row stays a row where a wrapping
/// double would leave a ghost under the claim.
#[test]
fn a_self_union_doubles_every_weight() {
    let schema = make_schema_u128_i64();
    let mut p = TestPlan::default();
    let r0 = p.seed(schema);
    let out = p.push(r0, schema, Op::Union { in_b: r0 });
    let mut vm = p.open(out);
    let got = vm.epoch(r0, make_batch_u128(&schema, &[(1, 1, 10), (2, 3, 20)]));
    assert_rows(&got, &[(1, 2, 10), (2, 6, 20)]);

    let got = vm.epoch(r0, make_batch_u128(&schema, &[(1, i64::MIN, 10), (2, i64::MAX, 20)]));
    assert!(!got.has_ghost());
    assert!(got.is_consolidated());
    assert_rows(&got, &[(1, i64::MIN, 10), (2, i64::MAX, 20)]);
}

#[test]
fn a_distinct_emits_only_the_positive_boundary_crossings_of_its_history() {
    let schema = make_schema_u128_i64();
    let mut p = TestPlan::default();
    let hist = p.table("hist", schema);
    let r0 = p.seed(schema);
    let out = p.push(
        r0,
        schema,
        Op::WeightClamp {
            hist,
            kind: gnitz_wire::ClampKind::Distinct,
        },
    );
    let mut vm = p.open(out);

    // 0 → 3 crosses in, 3 → 2 stays, 2 → 0 crosses out.
    for (w, want) in [(3, 1), (-1, 0), (-2, -1)] {
        let got = vm.epoch(r0, make_batch_u128(&schema, &[(1, w, 42)]));
        let want: &[_] = if want == 0 { &[] } else { &[(1, want, 42)] };
        assert_rows(&got, want);
    }
}

// ── The empty-epoch skip ─────────────────────────────────────────────────

/// A value-indexed global reduce this worker owns: its V₀ row is minted by one
/// empty epoch, which opens no value index, and the next empty epoch skips the
/// pass. A raw delta is folded before both the kernel and the index, and a
/// retracted minimum is replaced by the next one the index holds.
#[test]
fn a_global_min_mints_its_ground_row_then_tracks_its_history() {
    let schema = make_schema_u128_i64();
    let aggs = [
        AggDescriptor { col_idx: 1, agg_op: AggFunc::Min },
        AggDescriptor::COUNT_STAR,
    ];
    let plan = stream::ReducePlan::from_wire(&schema, &[], &aggs, true).unwrap();
    let mut p = TestPlan::default();
    let r0 = p.seed(schema);
    let (out, trace) = p.reduce(r0, plan);
    let mut vm = p.open(out);
    assert!(vm.pending_ground_row);

    let mut epoch = |rows: &[(u128, i64, i64)]| int_rows(&vm.epoch(r0, make_batch_u128_raw(&schema, rows)));
    assert_eq!(epoch(&[]), vec![(vec![None, Some(0)], 1)], "the ground row");
    assert!(epoch(&[]).is_empty(), "a second empty epoch has nothing left to mint");
    assert_eq!(
        epoch(&[(1, 1, 10), (3, 1, 7), (2, 1, 5), (3, -1, 7)]),
        vec![(vec![None, Some(0)], -1), (vec![Some(5), Some(2)], 1)],
    );
    assert_eq!(
        epoch(&[(2, -1, 5)]),
        vec![(vec![Some(5), Some(2)], -1), (vec![Some(10), Some(1)], 1)],
    );
    assert_eq!(int_rows(&vm.held(trace)), vec![(vec![Some(10), Some(1)], 1)]);
}

/// A non-empty first epoch mints V₀ through the reduce's ordinary path, so it
/// spends the latch too — and the all-empty epoch after it has nothing left to
/// dispatch for.
#[test]
fn a_non_empty_first_epoch_spends_the_ground_latch_too() {
    let schema = make_schema_u128_i64();
    let plan = stream::ReducePlan::from_wire(&schema, &[], &[AggDescriptor::COUNT_STAR], true).unwrap();
    let mut p = TestPlan::default();
    let r0 = p.seed(schema);
    let (out, _) = p.reduce(r0, plan);
    let mut vm = p.open(out);

    let first = vm.epoch(r0, make_batch_u128(&schema, &[(1, 1, 10)]));
    assert_eq!(int_rows(&first), vec![(vec![Some(1)], 1)]);
    assert!(!vm.pending_ground_row);
    assert!(vm.epoch(r0, Batch::empty_with_schema(&schema)).is_empty());
}

/// An epoch ends with every register free, one nothing reads included; and
/// without a ground row an empty epoch produces nothing.
#[test]
fn an_epoch_leaves_every_register_free() {
    let schema = make_schema_u128_i64();
    let mut p = TestPlan::default();
    let r0 = p.seed(schema);
    let out = p.push(r0, schema, Op::Share(own_rows(&schema)));
    // Written and never read, so no reader frees it.
    let dead = p.push(r0, schema, Op::Negate);
    let mut vm = p.open(out);

    assert_rows(&vm.epoch(r0, make_batch_u128(&schema, &[(1, 1, 10)])), &[(1, 1, 10)]);
    assert!(
        vm.batches.iter().all(|b| b.is_empty()),
        "register {} included",
        dead.at()
    );

    assert!(vm.epoch(r0, Batch::empty_with_schema(&schema)).is_empty());
}

// ── The two-term DBSP join, end to end ────────────────────────────────────

/// `ΔA ⋈ z⁻¹I(B) + ΔB ⋈ z⁻¹I(A)`, both kinds, over UNSORTED deltas carrying a
/// cancelling duplicate and two payloads on one key: the A-sourced epochs
/// integrate A and emit nothing, the B-sourced one joins against it, and the
/// epochs together denote the product weight for weight. A's first epoch leaves
/// a run holding only key 0, below every key of B: an equi probe's cursor skips
/// it, and only a cross join's whole-trace cursor pairs it.
#[test]
fn a_two_term_join_denotes_the_product_across_its_epochs() {
    let schema = make_schema_u128_i64();
    let a_epochs: [&[(u128, i64, i64)]; 2] = [&[(0, 1, 30)], &[(2, 1, 20), (1, 1, 10), (1, 2, 10), (1, 1, 11)]];
    let a_rows = make_batch_u128_raw(&schema, &a_epochs.concat());
    let b_rows = make_batch_u128_raw(&schema, &[(2, 1, 200), (1, 1, 100), (1, -1, 100), (1, 1, 101)]);

    for kind in [gnitz_wire::JoinKind::Equi, gnitz_wire::JoinKind::Cross] {
        let plan = |right: bool| stream::JoinPlan::from_wire(kind, right, &schema, &schema).unwrap();
        let out_schema = *plan(false).out_schema();
        let mut p = TestPlan::default();
        let (trace_a, trace_b) = (p.table("ta", schema), p.table("tb", schema));
        let (a, b) = (p.seed(schema), p.seed(schema));
        let ab = p.push(
            a,
            out_schema,
            Op::JoinDT {
                trace: Integral::Own(trace_b),
                plan: Box::new(plan(false)),
            },
        );
        let ba = p.push(
            b,
            out_schema,
            Op::JoinDT {
                trace: Integral::Own(trace_a),
                plan: Box::new(plan(true)),
            },
        );
        let out = p.push(ab, out_schema, Op::Union { in_b: ba });
        p.integrate(a, trace_a);
        p.integrate(b, trace_b);
        let mut vm = p.open(out);

        for rows in a_epochs {
            let got = vm.epoch(a, make_batch_u128_raw(&schema, rows));
            assert!(got.is_empty(), "{kind:?}: nothing to join against yet");
        }
        // Bilinear, so the product of the raw batches denotes the product of the
        // Z-sets they denote.
        let (want, _) = join_reference(kind, false, &schema, &schema, &a_rows, &b_rows);
        assert_eq!(zset_of(&vm.epoch(b, b_rows.clone()), &out_schema), want, "{kind:?}");
    }
}

/// A program joining the delta `d` against the integral of `t`, each seeded in
/// epochs of its own: `(vm, t, d, out_schema)`.
fn join_against_an_integral(
    kind: gnitz_wire::JoinKind,
    schema: SchemaDescriptor,
) -> (TestVm, DeltaReg, DeltaReg, SchemaDescriptor) {
    let plan = stream::JoinPlan::from_wire(kind, true, &schema, &schema).unwrap();
    let mut p = TestPlan::default();
    let trace = p.table("t", schema);
    let (t, d) = (p.seed(schema), p.seed(schema));
    let out_schema = *plan.out_schema();
    let out = p.push(
        d,
        out_schema,
        Op::JoinDT {
            trace: Integral::Own(trace),
            plan: Box::new(plan),
        },
    );
    p.integrate(t, trace);
    (p.open(out), t, d, out_schema)
}

/// A band join reads its trace between the delta's least and greatest equality
/// prefix. The trace is integrated one equality group an epoch, so each is a run
/// of its own: groups below, between, at and above the delta's two, a row at
/// weight 2, and a later epoch retracting a row of a matched group. Each rel
/// emits the matched groups' spans at their weight products and nothing else.
#[test]
fn a_band_join_emits_the_spans_of_the_groups_its_delta_names() {
    let cols = [
        SchemaColumn::new(TypeCode::U64, false),
        SchemaColumn::new(TypeCode::U64, false),
        SchemaColumn::new(TypeCode::I64, false),
    ];
    let schema = SchemaDescriptor::new(&cols, &[0, 1]);
    // `(eq, range, weight, payload)`.
    let batch = |rows: &[(u64, u64, i64, i64)]| {
        let mut b = BatchBuilder::new(&schema);
        for &(eq, range, w, val) in rows {
            b.begin_row_natives(&[eq as u128, range as u128], w);
            b.put_int(val as u128);
            b.end_row();
        }
        b.finish()
    };
    let t_epochs: [&[(u64, u64, i64, i64)]; 6] = [
        &[(1, 5, 1, 105), (1, 15, 1, 115)],
        &[(3, 5, 1, 305), (3, 10, 2, 310), (3, 15, 1, 315)],
        &[(5, 5, 1, 505), (5, 15, 1, 515)],
        &[(7, 5, 1, 705), (7, 10, 1, 710), (7, 15, 1, 715)],
        &[(9, 5, 1, 905), (9, 15, 1, 915)],
        &[(3, 15, -1, 315)],
    ];
    let trace = batch(&t_epochs.concat()).into_consolidated();
    let delta = batch(&[(3, 10, 2, 1), (7, 10, 3, 2)]);

    for &rel in gnitz_wire::RangeRel::ALL {
        let kind = gnitz_wire::JoinKind::Range { rel };
        let (mut vm, t, d, out_schema) = join_against_an_integral(kind, schema);
        for rows in t_epochs {
            assert!(vm.epoch(t, batch(rows)).is_empty());
        }
        let (want, want_rows) = join_reference(kind, true, &schema, &schema, &delta, &trace);
        assert!(want_rows > 0, "premise: {rel:?} matches something");
        let got = vm.epoch(d, delta.clone());
        assert_eq!(got.len(), want_rows, "{rel:?}: row count");
        assert_eq!(zset_of(&got, &out_schema), want, "{rel:?}");
    }
}

/// A cross join reads its whole trace, however many epochs integrated it: every
/// pair at its weight product, a row an epoch retracted in none.
#[test]
fn a_cross_join_pairs_every_row_of_a_trace_integrated_across_epochs() {
    let schema = make_schema_u128_i64();
    let t_epochs: [&[(u128, i64, i64)]; 3] = [
        &[(9, 1, 90), (4, 2, 40)],
        &[(1, 1, 10), (6, 1, 60)],
        &[(9, -1, 90), (5, 3, 50)],
    ];
    let trace = make_batch_u128_raw(&schema, &t_epochs.concat()).into_consolidated();
    let delta = make_batch_u128_raw(&schema, &[(5, 2, 500), (7, -1, 700)]);

    let (mut vm, t, d, out_schema) = join_against_an_integral(gnitz_wire::JoinKind::Cross, schema);
    for rows in t_epochs {
        assert!(vm.epoch(t, make_batch_u128_raw(&schema, rows)).is_empty());
    }
    let (want, want_rows) = join_reference(gnitz_wire::JoinKind::Cross, true, &schema, &schema, &delta, &trace);
    assert_eq!(want_rows, 8);
    let got = vm.epoch(d, delta);
    assert_eq!(got.len(), want_rows, "row count");
    assert_eq!(zset_of(&got, &out_schema), want);
}

/// A join whose B side is a table's own store: the A-sourced term reads the
/// table as the program last absorbed it, so a row the table has ingested and
/// the program has not been run over joins once, in its own epoch — whether it
/// is new, replaces a row, or removes one.
#[test]
fn a_join_over_its_source_reads_it_without_what_it_has_not_absorbed() {
    use gnitz_store::relation::{RelationKind, RelationSpec};
    const TABLE: u64 = gnitz_wire::FIRST_USER_TABLE_ID + 1;
    let schema = make_schema_u128_i64();
    let stored = stream::JoinPlan::from_wire(gnitz_wire::JoinKind::Equi, true, &schema, &schema).unwrap();
    let rekey = gnitz_wire::MapKind::Reindex {
        keep: vec![1],
        key: vec![(0, gnitz_wire::TypeCode::U128)],
        role: gnitz_wire::ReindexRole::Auxiliary,
        nulls: gnitz_wire::NullKeys::Drop,
    };
    let rekey = MapPlan::from_wire(&schema, &rekey).unwrap();
    let over = stream::JoinPlan::over_source(false, &schema, &schema, &rekey).unwrap();
    let out_schema = *over.out_schema();
    let mut p = TestPlan::default();
    let trace_a = p.table("ta", schema);
    let (a, b) = (p.seed(schema), p.seed(schema));
    let ab = p.push(
        a,
        out_schema,
        Op::JoinDT {
            trace: Integral::Relation(TABLE, gnitz_store::relation::Cut::Sealed),
            plan: Box::new(over),
        },
    );
    let ba = p.push(
        b,
        out_schema,
        Op::JoinDT {
            trace: Integral::Own(trace_a),
            plan: Box::new(stored),
        },
    );
    let out = p.push(ab, out_schema, Op::Union { in_b: ba });
    p.integrate(a, trace_a);
    let mut vm = p.open(out);
    vm.registry
        .register(RelationSpec {
            id: TABLE,
            kind: RelationKind::BaseTable,
            schema,
            placement: gnitz_zset::algebra::Placement::full_pk(&schema),
            pk_repeats: false,
        })
        .unwrap();

    let mut a_all = Batch::empty_with_schema(&schema);
    let mut got: std::collections::HashMap<_, i64> = Default::default();
    let mut absorb = |out: Batch| {
        for (row, w) in zset_of(&out, &out_schema) {
            *got.entry(row).or_default() += w;
        }
    };
    // A table write lands in the store at once and reaches the program at its tick.
    let write = |vm: &mut TestVm, rows: &[(u128, i64, i64)]| {
        vm.registry
            .ingest_pending(TABLE, make_batch_u128(&schema, rows))
            .unwrap();
    };
    type Rows = &'static [(u128, i64, i64)];
    let steps: [(Rows, Rows); 4] = [
        // (written to the table, then A's delta — run before the table's tick)
        (&[(1, 1, 100), (2, 1, 200)], &[(1, 1, 10), (2, 1, 20), (3, 1, 30)]),
        // Key 1 replaced, key 3 new, while A gains a second row on key 1.
        (&[(1, 1, 101), (3, 1, 300)], &[(1, 1, 11)]),
        // Key 2 removed, while A retracts its row there and adds one on key 3.
        (&[(2, -1, 200)], &[(2, -1, 20), (3, 1, 31)]),
        (&[], &[(1, -1, 10)]),
    ];
    for (i, (table_rows, a_rows)) in steps.into_iter().enumerate() {
        // What the term may pair with is the table before its un-ticked writes.
        let table_then = vm.registry.relation(TABLE).unwrap().cursor().materialize();
        if !table_rows.is_empty() {
            write(&mut vm, table_rows);
        }
        let a_delta = make_batch_u128_raw(&schema, a_rows);
        let ticked = vm.epoch(a, a_delta.clone());
        let (want, _) = join_reference(
            gnitz_wire::JoinKind::Equi,
            false,
            &schema,
            &schema,
            &a_delta,
            &table_then,
        );
        assert_eq!(zset_of(&ticked, &out_schema), want, "step {i}: A's epoch");
        absorb(ticked);
        a_all.append_batch(&a_delta);
        if let Some(delta) = vm.registry.seal(TABLE).unwrap() {
            absorb(vm.epoch(b, delta));
        }
    }
    let table_now = vm.registry.relation(TABLE).unwrap().cursor().materialize();
    let (want, _) = join_reference(gnitz_wire::JoinKind::Equi, false, &schema, &schema, &a_all, &table_now);
    got.retain(|_, w| *w != 0);
    assert_eq!(got, want, "the epochs together are the join of A with the table");

    // A source the program has been fed nothing of reads empty, whatever it holds.
    vm.unfed = vec![TABLE];
    assert!(vm.epoch(a, make_batch_u128_raw(&schema, &[(1, 1, 12)])).is_empty());
}

// ── Integrates run after the range, and a replay runs none ───────────────

/// A `TopN` instruction ingests its index entries itself, so the second epoch
/// sees the first one's row and displaces it instead of adding a row beside it.
#[test]
fn a_second_topn_epoch_displaces_the_first_ones_row() {
    let schema = make_schema_u128_i64();
    let order = [gnitz_wire::OrderKey { col: 1, desc: true, nulls_first: false }];
    let plan = stream::TopNPlan::from_wire(&schema, &[], &order, 1, 0).unwrap();
    let out_schema = *plan.output_schema();
    let mut p = TestPlan::default();
    let index_table = p.table("topnidx", *plan.index_schema());
    let r0 = p.seed(schema);
    let out = p.push(r0, out_schema, Op::TopN { index: index_table, plan: Box::new(plan) });
    let mut vm = p.open(out);

    // The output row carries the input row whole: its PK, then its value.
    let first = vm.epoch(r0, make_batch_u128(&schema, &[(1, 1, 10)]));
    assert_eq!(int_rows(&first), vec![(vec![Some(1), Some(10)], 1)]);
    // A strictly greater value displaces the top row entirely.
    assert_eq!(
        int_rows(&vm.epoch(r0, make_batch_u128(&schema, &[(2, 1, 99)]))),
        vec![(vec![Some(1), Some(10)], -1), (vec![Some(2), Some(99)], 1)],
    );
}

/// A hydration replay seeds a register mid-program and runs from its first
/// reader; running the integrates too would write that seed back and double the
/// trace's weights on every read, and running from the top would overwrite the
/// seed with the unseeded input's empty result.
#[test]
fn a_replay_runs_from_its_entry_and_leaves_every_trace_as_it_found_it() {
    let schema = make_schema_u128_i64();
    let mut p = TestPlan::default();
    let trace = p.table("trace", schema);
    let r0 = p.seed(schema);
    let mid = p.push(r0, schema, Op::Negate);
    let out = p.push(mid, schema, Op::Share(own_rows(&schema)));
    p.integrate(out, trace);
    let mut vm = p.open(out);

    let rows = [(1, 1, 10), (2, 1, 20)];
    vm.epoch(r0, make_batch_u128(&schema, &rows));
    let before = vm.trace(trace);

    assert_rows(&vm.replay(mid, make_batch_u128(&schema, &rows)), &rows);
    assert_eq!(vm.trace(trace), before);
}

/// A replay may not seed a register a stateful operator reads, directly or
/// through other operators.
#[test]
fn replay_entry_refuses_a_stateful_operator_the_seed_reaches() {
    let schema = make_schema_u128_i64();

    let plan = stream::ReducePlan::from_wire(&schema, &[], &[AggDescriptor::COUNT_STAR], false).unwrap();
    let mut p = TestPlan::default();
    let r0 = p.seed(schema);
    let (reduced, _) = p.reduce(r0, plan);
    let reduced_schema = p.schema_of(reduced);
    let out = p.push(reduced, reduced_schema, Op::Share(own_rows(&reduced_schema)));
    let vm = p.open(out);
    assert!(vm.replay_entry(r0).is_err(), "the reduce reads the seed");
    assert!(vm.replay_entry(reduced).is_ok());

    // A clamp reads its input's history, which the seed never entered.
    let mut p = TestPlan::default();
    let hist = p.table("hist", schema);
    let r0 = p.seed(schema);
    let out = p.push(
        r0,
        schema,
        Op::WeightClamp {
            hist,
            kind: gnitz_wire::ClampKind::Distinct,
        },
    );
    assert!(p.open(out).replay_entry(r0).is_err(), "the clamp reads the seed");
}

/// A stateful operator the seed does not reach sits on an empty register for
/// the whole replay, so it is skipped and its state is neither read nor moved —
/// wherever it stands in the program.
#[test]
fn a_replay_passes_a_stateful_operator_its_seed_does_not_reach() {
    let schema = make_schema_u128_i64();
    let mut p = TestPlan::default();
    let hist = p.table("hist", schema);
    let (a, b) = (p.seed(schema), p.seed(schema));
    let negated = p.push(a, schema, Op::Negate);
    let clamped = p.push(
        b,
        schema,
        Op::WeightClamp {
            hist,
            kind: gnitz_wire::ClampKind::Distinct,
        },
    );
    let out = p.push(negated, schema, Op::Union { in_b: clamped });
    let mut vm = p.open(out);
    assert!(vm.replay_entry(b).is_err(), "the clamp reads this seed");

    vm.epoch(b, make_batch_u128(&schema, &[(1, 1, 10)]));
    let before = vm.trace(hist);
    let replayed = vm.replay(a, make_batch_u128(&schema, &[(2, 1, 20)]));
    assert_rows(&replayed, &[(2, -1, 20)]);
    assert_eq!(vm.trace(hist), before);
}

// ── Exchange rounds ──────────────────────────────────────────────────────

/// A round hands out the rows of its input register and the program goes on
/// from what the round gathered, not from what it sent.
#[test]
fn a_round_sends_its_operand_and_resumes_from_what_it_gathered() {
    let schema = make_schema_u128_i64();
    let mut p = TestPlan::default();
    let r0 = p.seed(schema);
    let negated = p.push(r0, schema, Op::Negate);
    let gathered = p.round(negated, own_rows(&schema));
    let out = p.push(gathered, schema, Op::Negate);
    let mut vm = p.open(out);

    let mut seed = make_batch_u128(&schema, &[(1, 1, 10)]);
    let mut sent = Vec::new();
    let got = vm.epoch_with(r0, &mut seed, true, |rows, lent| {
        sent.push((weighted_rows(rows), lent));
        make_batch_u128(&schema, &[(7, 2, 70)])
    });
    let want = make_batch_u128(&schema, &[(1, -1, 10)]);
    assert_eq!(sent, [(weighted_rows(&want), false)], "the register's own batch, taken");
    assert_rows(&got, &[(7, -2, 70)]);
}

/// A round whose operand a later instruction also reads sends a copy, under the
/// operand register's own schema, and leaves the register to that reader.
#[test]
fn a_round_leaves_its_operand_to_a_later_reader() {
    let pk = SchemaColumn::new(TypeCode::U128, false);
    let not_null = SchemaDescriptor::new(&[pk, SchemaColumn::new(TypeCode::I64, false)], &[0]);
    let nullable = SchemaDescriptor::new(&[pk, SchemaColumn::new(TypeCode::I64, true)], &[0]);
    let mut p = TestPlan::default();
    let r0 = p.seed(not_null);
    // Negate hands its input's label through; the register says `nullable`.
    let negated = p.push(r0, nullable, Op::Negate);
    let gathered = p.round(negated, own_rows(&nullable));
    let out = p.push(gathered, nullable, Op::Union { in_b: negated });
    let mut vm = p.open(out);

    let mut seed = make_batch_u128(&not_null, &[(1, 1, 10)]);
    let mut sent = Vec::new();
    let got = vm.epoch_with(r0, &mut seed, true, |rows, lent| {
        sent.push((*rows.schema() == nullable, lent));
        rows.clone()
    });
    assert_eq!(sent, [(true, false)]);
    assert_rows(&got, &[(1, -2, 10)]);
}

/// A round reading the seed lends the caller's batch on, and takes it only where
/// the caller let it: a lent seed comes back whole, a taken one empty.
#[test]
fn a_round_over_the_seed_lends_or_takes_it_as_the_caller_does() {
    let schema = make_schema_u128_i64();
    let mut p = TestPlan::default();
    let r0 = p.seed(schema);
    let out = p.round(r0, own_rows(&schema));
    let mut vm = p.open(out);

    let rows = [(1, 1, 10), (2, 1, 20)];
    for take in [false, true] {
        let mut seed = make_batch_u128(&schema, &rows);
        let mut lends = Vec::new();
        let got = vm.epoch_with(r0, &mut seed, take, |rows, lent| {
            lends.push(lent);
            rows.clone()
        });
        assert_eq!(lends, [!take]);
        assert_rows(&got, &rows);
        assert_eq!(seed.len(), if take { 0 } else { rows.len() }, "take: {take}");
    }
}

/// A round runs iff the epoch's seed reaches it, whatever the seed holds: an
/// empty delta of a source that feeds it still runs it, and a delta of another
/// source never does.
#[test]
fn a_round_runs_by_what_its_epoch_seeds_not_by_what_the_seed_holds() {
    let schema = make_schema_u128_i64();
    let mut p = TestPlan::default();
    let (a, b) = (p.seed(schema), p.seed(schema));
    let relayed = p.round(a, own_rows(&schema));
    let out = p.push(relayed, schema, Op::Union { in_b: b });
    let mut vm = p.open(out);
    assert!(!vm.idles_on_empty(a) && vm.idles_on_empty(b));

    let mut rounds = 0;
    let mut echo = |rows: &Batch, _| {
        rounds += 1;
        rows.clone()
    };
    let mut none = Batch::empty_with_schema(&schema);
    assert!(vm.epoch_with(a, &mut none, true, &mut echo).is_empty());
    let mut rows = make_batch_u128(&schema, &[(1, 1, 10)]);
    assert_rows(&vm.epoch_with(b, &mut rows, true, &mut echo), &[(1, 1, 10)]);
    assert_eq!(rounds, 1, "the empty epoch of `a`, and not the epoch of `b`");
}

/// A seed its register folds is folded where the caller holds it, so the next
/// reader of the producer finds it folded; and a seed no reader may take comes
/// back with every row.
#[test]
fn a_lent_seed_comes_back_folded_and_whole() {
    let schema = make_schema_u128_i64();
    let mut p = TestPlan::default();
    let hist = p.table("hist", schema);
    let r0 = p.seed(schema);
    let kind = gnitz_wire::ClampKind::Distinct;
    let out = p.push(r0, schema, Op::WeightClamp { hist, kind });
    let mut vm = p.open(out);

    let mut seed = make_batch_u128_raw(&schema, &[(2, 1, 20), (1, 1, 10), (2, 1, 20)]);
    assert!(!seed.is_consolidated());
    let got = vm.epoch_with(r0, &mut seed, false, |_, _| unreachable!("no round"));
    assert_rows(&got, &[(1, 1, 10), (2, 1, 20)]);
    assert!(seed.is_consolidated());
    assert_rows(&seed, &[(1, 1, 10), (2, 2, 20)]);
}

/// The fold a round's gathering register owes is its senders' under a broadcast,
/// where each worker would otherwise sort every row, and the receiver's under a
/// keyed round.
#[test]
fn a_broadcast_sends_folded_what_its_reader_reads_folded() {
    let schema = make_schema_u128_i64();
    for (plan, sent_folded) in [
        (std::rc::Rc::new(algebra::ScatterPlan::broadcast()), true),
        (own_rows(&schema), false),
    ] {
        let mut p = TestPlan::default();
        let hist = p.table("hist", schema);
        let r0 = p.seed(schema);
        let gathered = p.round(r0, plan);
        let kind = gnitz_wire::ClampKind::Distinct;
        let out = p.push(gathered, schema, Op::WeightClamp { hist, kind });
        let mut vm = p.open(out);

        let mut seed = make_batch_u128_raw(&schema, &[(2, 1, 20), (1, 1, 10), (2, 1, 20)]);
        let mut folded = Vec::new();
        let got = vm.epoch_with(r0, &mut seed, true, |rows, _| {
            folded.push(rows.is_consolidated());
            rows.clone()
        });
        assert_eq!(folded, [sent_folded]);
        assert_rows(&got, &[(1, 1, 10), (2, 1, 20)]);
    }
}

/// A replay runs on one worker, so it may not seed a register that reaches a
/// round — and passes one its seed does not reach.
#[test]
fn replay_entry_refuses_a_seed_that_reaches_a_round() {
    let schema = make_schema_u128_i64();
    let mut p = TestPlan::default();
    let (a, b) = (p.seed(schema), p.seed(schema));
    let relayed = p.round(a, own_rows(&schema));
    let out = p.push(relayed, schema, Op::Union { in_b: b });
    let mut vm = p.open(out);
    assert!(vm.replay_entry(a).is_err());
    assert_rows(&vm.replay(b, make_batch_u128(&schema, &[(3, 1, 30)])), &[(3, 1, 30)]);
}
