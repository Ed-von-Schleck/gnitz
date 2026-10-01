//! Dispatch-loop tests: epochs of hand-built programs, one per path the VM
//! itself owns — operator semantics are `gnitz-zset`'s to test.

use super::fixtures::*;
use super::*;
use crate::test_support::{
    join_reference, make_batch_u128, make_batch_u128_raw, make_schema_u128_i64, opk_pk, weighted_rows, zset_of,
};
use gnitz_wire::{AggDescriptor, AggFunc, TypeCode};
use gnitz_zset::repr::{Batch, BatchBuilder};
use gnitz_zset::schema::{SchemaColumn, SchemaDescriptor};

/// Each row's payload cells as signed integers of their own width, with its
/// weight, sorted — the Z-set of an output whose key the test need not spell.
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
    assert_rows(&vm.epoch([(r0, input)]), &[(1, -1, 10), (3, -1, 20)]);
}

/// `A + B` over both operands' empty and non-empty shapes, epoch after epoch on
/// one program — so each epoch also sees nothing of the one before. An empty left
/// operand takes the VM's own `0 + B` arm.
#[test]
fn a_union_sums_its_operands_in_every_epoch() {
    let schema = make_schema_u128_i64();
    let mut p = TestPlan::default();
    let (a, b) = (p.seed(schema), p.seed(schema));
    let out = p.push(a, schema, Op::Union { in_b: b });
    let mut vm = p.open(out);

    type Rows = &'static [(u128, i64, i64)];
    let cases: [(Rows, Rows, Rows); 4] = [
        (
            &[(1, 1, 10), (2, 1, 20)],
            &[(1, 1, 10), (3, 1, 30)],
            &[(1, 2, 10), (2, 1, 20), (3, 1, 30)],
        ),
        (&[], &[(3, 2, 30), (4, -1, 40)], &[(3, 2, 30), (4, -1, 40)]),
        (&[(5, 1, 50)], &[], &[(5, 1, 50)]),
        (&[], &[], &[]),
    ];
    for (left, right, want) in cases {
        let got = vm.epoch([
            (a, make_batch_u128(&schema, left)),
            (b, make_batch_u128(&schema, right)),
        ]);
        assert_rows(&got, want);
    }
}

/// A `Union` with `in_a == in_b` is `Z + Z`: taking operand 0 and then reading
/// the emptied operand 1 would yield +1 where +2 is due.
#[test]
fn a_self_union_doubles_every_weight() {
    let schema = make_schema_u128_i64();
    let mut p = TestPlan::default();
    let r0 = p.seed(schema);
    let out = p.push(r0, schema, Op::Union { in_b: r0 });
    let mut vm = p.open(out);
    let got = vm.epoch([(r0, make_batch_u128(&schema, &[(1, 1, 10), (2, 3, 20)]))]);
    assert_rows(&got, &[(1, 2, 10), (2, 6, 20)]);
}

/// A UNION whose sides disagree on a payload column's nullability runs, and
/// leaves, under its own merged schema. Only that schema selects the null-aware
/// comparator, which sorts a NULL below -3 where the left side's null-blind one
/// reads the cell's zero bytes as 0. And an identity path hands an operand back
/// label and all, so the epilogue stamps the output register's — the label the
/// exchange wire carries to a master that cannot re-derive it.
#[test]
fn a_union_runs_and_leaves_under_its_merged_schema() {
    let pk = SchemaColumn::new(TypeCode::U128, false);
    let not_null = SchemaDescriptor::new(&[pk, SchemaColumn::new(TypeCode::I64, false)], &[0]);
    let nullable = SchemaDescriptor::new(&[pk, SchemaColumn::new(TypeCode::I64, true)], &[0]);
    let merged = algebra::union_nullability_merge(&not_null, &nullable).unwrap();

    let mut p = TestPlan::default();
    let (a, b) = (p.seed(not_null), p.seed(nullable));
    let out = p.push(a, merged, Op::Union { in_b: b });
    let mut vm = p.open(out);

    let left = || make_batch_u128(&not_null, &[(1, 1, -3)]);
    let identity = vm.epoch([(a, left())]);
    assert_eq!(*identity.schema(), merged);
    assert_rows(&identity, &[(1, 1, -3)]);

    let mut rb = BatchBuilder::new(&nullable);
    rb.begin_row(1u128, 1);
    rb.put_null();
    rb.end_row();
    let key = opk_pk(&merged, &[1]);
    assert_eq!(
        weighted_rows(&vm.epoch([(a, left()), (b, rb.finish())])),
        vec![
            ((key.clone(), vec![None]), 1),
            ((key, vec![Some((-3i64).to_le_bytes().to_vec())]), 1)
        ],
    );
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
        let got = vm.epoch([(r0, make_batch_u128(&schema, &[(1, w, 42)]))]);
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

    let mut epoch = |rows: &[(u128, i64, i64)]| int_rows(&vm.epoch([(r0, make_batch_u128_raw(&schema, rows))]));
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
    let held = vm.state.cursor(trace).materialize();
    assert_eq!(int_rows(&held), vec![(vec![Some(10), Some(1)], 1)]);
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

    let first = vm.epoch([(r0, make_batch_u128(&schema, &[(1, 1, 10)]))]);
    assert_eq!(int_rows(&first), vec![(vec![Some(1)], 1)]);
    assert!(!vm.pending_ground_row);
    assert!(vm.epoch([(r0, Batch::empty_with_schema(&schema))]).is_empty());
}

/// Without a ground row an empty epoch produces nothing and runs no dispatch —
/// but it still clears the previous epoch's registers, which is the one thing
/// the skipped prologue would otherwise have done.
#[test]
fn an_empty_epoch_skips_the_pass_and_still_clears_the_registers() {
    let schema = make_schema_u128_i64();
    let mut p = TestPlan::default();
    let r0 = p.seed(schema);
    let out = p.push(r0, schema, Op::WorkerFilter { slot: Slot::SOLO });
    // Written and never read, so nothing frees it: what still holds rows when
    // the epoch ends.
    let dead = p.push(r0, schema, Op::Negate);
    let mut vm = p.open(out);

    assert_rows(
        &vm.epoch([(r0, make_batch_u128(&schema, &[(1, 1, 10)]))]),
        &[(1, 1, 10)],
    );
    assert_eq!(vm.batches[dead.at()].len(), 1);

    assert!(vm.epoch([(r0, Batch::empty_with_schema(&schema))]).is_empty());
    assert!(
        vm.batches.iter().all(|b| b.is_empty()),
        "the skipped epoch still released the previous one's batches",
    );
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
        let out_schema = plan(false).out_schema;
        let mut p = TestPlan::default();
        let (trace_a, trace_b) = (p.table("ta", schema), p.table("tb", schema));
        let (a, b) = (p.seed(schema), p.seed(schema));
        let ab = p.push(
            a,
            out_schema,
            Op::JoinDT {
                trace: Integral::Own(trace_b),
                probe: plan(false).probe,
            },
        );
        let ba = p.push(
            b,
            out_schema,
            Op::JoinDT {
                trace: Integral::Own(trace_a),
                probe: plan(true).probe,
            },
        );
        let out = p.push(ab, out_schema, Op::Union { in_b: ba });
        p.integrate(a, trace_a);
        p.integrate(b, trace_b);
        let mut vm = p.open(out);

        for rows in a_epochs {
            let got = vm.epoch([(a, make_batch_u128_raw(&schema, rows))]);
            assert!(got.is_empty(), "{kind:?}: nothing to join against yet");
        }
        // Bilinear, so the product of the raw batches denotes the product of the
        // Z-sets they denote.
        let (want, _) = join_reference(kind, false, &schema, &schema, &a_rows, &b_rows);
        assert_eq!(zset_of(&vm.epoch([(b, b_rows.clone())]), &out_schema), want, "{kind:?}");
    }
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
    let out_schema = over.out_schema;
    let mut p = TestPlan::default();
    let trace_a = p.table("ta", schema);
    let (a, b) = (p.seed(schema), p.seed(schema));
    let ab = p.push(
        a,
        out_schema,
        Op::JoinDT {
            trace: Integral::Source(TABLE),
            probe: over.probe,
        },
    );
    let ba = p.push(
        b,
        out_schema,
        Op::JoinDT {
            trace: Integral::Own(trace_a),
            probe: stored.probe,
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
            placement: gnitz_zset::schema::Placement::full_pk(&schema),
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
        let effective = vm
            .registry
            .ingest_returning(TABLE, make_batch_u128(&schema, rows))
            .unwrap();
        match vm.unticked.get_mut(&TABLE) {
            Some(held) => held.append_batch(&effective),
            None => drop(vm.unticked.insert(TABLE, effective)),
        }
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
        if !table_rows.is_empty() {
            write(&mut vm, table_rows);
        }
        let before = vm.unticked.get(&TABLE).map(|held| held.to_consolidated().negated());
        let a_delta = make_batch_u128_raw(&schema, a_rows);
        let ticked = vm.epoch([(a, a_delta.clone())]);
        // What the term may pair with is the table less its un-ticked writes.
        let mut table_then = vm
            .registry
            .relation(TABLE)
            .unwrap()
            .cursor()
            .materialize()
            .as_ref()
            .clone();
        if let Some(undo) = &before {
            table_then.append_batch(undo);
        }
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
        if let Some(delta) = vm.unticked.remove(&TABLE) {
            absorb(vm.epoch([(b, delta)]));
        }
    }
    let table_now = vm.registry.relation(TABLE).unwrap().cursor().materialize();
    let (want, _) = join_reference(gnitz_wire::JoinKind::Equi, false, &schema, &schema, &a_all, &table_now);
    got.retain(|_, w| *w != 0);
    assert_eq!(got, want, "the epochs together are the join of A with the table");

    // A source the program has been fed nothing of reads empty, whatever it holds.
    vm.unfed = vec![TABLE];
    assert!(vm.epoch([(a, make_batch_u128_raw(&schema, &[(1, 1, 12)]))]).is_empty());
}

// ── Integrates run after the range, and a replay runs none ───────────────

/// A `TopN` returns the register its integrate reads, so an integrate running
/// against the extracted output would write nothing and the operator would never
/// see its own history: the second epoch would add a row instead of displacing
/// the first one's.
#[test]
fn a_second_topn_epoch_displaces_the_first_ones_row() {
    let schema = make_schema_u128_i64();
    let order = [gnitz_wire::OrderKey { col: 1, desc: true, nulls_first: false }];
    let plan = stream::TopNPlan::from_wire(&schema, &[], &order, 1, 0).unwrap();
    let out_schema = plan.output_schema;
    let mut p = TestPlan::default();
    let out_trace = p.table("topn", out_schema);
    let index_table = p.table("topnidx", plan.index.schema);
    let r0 = p.seed(schema);
    let out = p.push(
        r0,
        out_schema,
        Op::TopN {
            out_trace,
            plan: Box::new(BakedTopN { plan, index_table }),
        },
    );
    let mut vm = p.open(out);

    // The output row carries the input row whole: its PK, then its value.
    let first = vm.epoch([(r0, make_batch_u128(&schema, &[(1, 1, 10)]))]);
    assert_eq!(int_rows(&first), vec![(vec![Some(1), Some(10)], 1)]);
    // A strictly greater value displaces the top row entirely.
    assert_eq!(
        int_rows(&vm.epoch([(r0, make_batch_u128(&schema, &[(2, 1, 99)]))])),
        vec![(vec![Some(1), Some(10)], -1), (vec![Some(2), Some(99)], 1)],
    );
    assert_eq!(
        int_rows(&vm.state.cursor(out_trace).materialize()),
        vec![(vec![Some(2), Some(99)], 1)]
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
    let out = p.push(mid, schema, Op::WorkerFilter { slot: Slot::SOLO });
    p.integrate(out, trace);
    let mut vm = p.open(out);

    let rows = [(1, 1, 10), (2, 1, 20)];
    vm.epoch([(r0, make_batch_u128(&schema, &rows))]);
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
    let out = p.push(reduced, reduced_schema, Op::WorkerFilter { slot: Slot::SOLO });
    let vm = p.open(out);
    assert!(vm.program.replay_entry(r0).is_err(), "the reduce reads the seed");
    assert!(vm.program.replay_entry(reduced).is_ok());

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
    assert!(
        p.open(out).program.replay_entry(r0).is_err(),
        "the clamp reads the seed"
    );
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
    assert!(vm.program.replay_entry(b).is_err(), "the clamp reads this seed");

    vm.epoch([(b, make_batch_u128(&schema, &[(1, 1, 10)]))]);
    let before = vm.trace(hist);
    let replayed = vm.replay(a, make_batch_u128(&schema, &[(2, 1, 20)]));
    assert_rows(&replayed, &[(2, -1, 20)]);
    assert_eq!(vm.trace(hist), before);
}
