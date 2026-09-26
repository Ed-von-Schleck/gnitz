//! Dispatch-loop tests: one epoch of a hand-built program per opcode path.

use super::fixtures::*;
use super::*;
use crate::test_support::{make_batch_u128, make_batch_u128_raw, make_schema_u128_i64, opk_pk, zset_of};
use gnitz_store::schema::{SchemaColumn, SchemaDescriptor};
use gnitz_store::storage::{Batch, BatchBuilder, Layout, StoreError};
use gnitz_wire::AggDescriptor;
use gnitz_wire::AggFunc;
use gnitz_wire::TypeCode;

// ── Test helpers ─────────────────────────────────────────────────────────

/// One epoch seeding a single input register — production seeds through
/// `execute_epoch_multi` directly, with one entry per exchange side.
fn execute_epoch(vm: &mut TestVm, input: Batch, input_reg: u16) -> Result<Batch, StoreError> {
    vm.epoch([(DeltaReg(input_reg), input)])
}

/// A reduce with no value index, over the register convention every test reduce
/// shares: 0 = input delta, 1 = raw delta out, integrating into `out_trace`.
fn push_reduce(
    p: &mut TestPlan,
    aggs: &[AggDescriptor],
    gcols: &[u32],
    in_schema: SchemaDescriptor,
    out_trace: StateIdx,
    seeds_ground: bool,
) -> SchemaDescriptor {
    let plan = gnitz_store::ops::ReducePlan::from_wire(&in_schema, gcols, aggs, seeds_ground).unwrap();
    let out_schema = plan.shape.output_schema;
    let baked = Box::new(BakedReduce::new(plan, None));
    p.push(0, 1, Op::Reduce { out_trace, plan: baked });
    out_schema
}

/// A U128 PK plus `col_types` as payload columns — the 2- and 3-payload shapes
/// the multi-column tests need. The one-payload shape is
/// [`make_schema_u128_i64`].
fn make_schema(col_types: &[TypeCode]) -> SchemaDescriptor {
    let mut columns = [SchemaColumn::EMPTY; gnitz_wire::MAX_COLUMNS];
    columns[0] = SchemaColumn::new(TypeCode::U128, false);
    for (i, &tc) in col_types.iter().enumerate() {
        columns[i + 1] = SchemaColumn::new(tc, false);
    }
    let n = col_types.len() + 1;
    SchemaDescriptor::new(&columns[..n], &[0])
}

/// A consolidated batch with two I64 payload columns from `(pk, w, c0, c1)`.
fn make_batch_2col(schema: SchemaDescriptor, rows: &[(u128, i64, i64, i64)]) -> Batch {
    let mut bb = BatchBuilder::new(schema);
    for &(pk, w, c0, c1) in rows {
        bb.begin_row(pk, w);
        bb.put_int(c0 as u128);
        bb.put_int(c1 as u128);
        bb.end_row();
    }
    let mut b = bb.finish();
    b.certify_layout(Layout::Consolidated);
    b
}

/// Rows as `(pk, weight, col0_i64)`, in emission order — the tests that assert
/// an order deliberately read through this rather than through `zset_of`.
fn extract_rows(b: &Batch) -> Vec<(u64, i64, i64)> {
    (0..b.len())
        .map(|i| {
            let c0 = i64::from_le_bytes(b.col_data(0)[i * 8..(i + 1) * 8].try_into().unwrap());
            (b.get_pk(i) as u64, b.get_weight(i), c0)
        })
        .collect()
}

/// Schemas for a UNION whose sides disagree on the payload column's
/// nullability: `(left NOT NULL, right nullable, merged output)`. The merged
/// one is the OR the emit layer's `union_nullability_merge` produces, which
/// for this pair is the right side's.
fn union_nullability_schemas() -> (SchemaDescriptor, SchemaDescriptor, SchemaDescriptor) {
    let pk = SchemaColumn::new(TypeCode::U128, false);
    let not_null = SchemaDescriptor::new(&[pk, SchemaColumn::new(TypeCode::I64, false)], &[0]);
    let nullable = SchemaDescriptor::new(&[pk, SchemaColumn::new(TypeCode::I64, true)], &[0]);
    (not_null, nullable, nullable)
}

/// A pass-through program: `reg1 = reg0 ∪ reg2`, with reg2 never seeded unless
/// the test seeds it. Three delta registers under one schema, output reg 1.
fn union_program(schema: SchemaDescriptor) -> TestVm {
    let mut p = TestPlan::default();
    p.push(0, 1, Op::Union { in_b: DeltaReg(2) });
    p.build(vec![schema; 3], 1)
}

// ── Tests ────────────────────────────────────────────────────────────────

#[test]
fn test_filter_negate_pipeline() {
    // Filter rows where col0 > 0, then negate weights.
    // Filter keeps pk=1 (col0=10>0) and pk=3 (col0=20>0); Negate flips both.
    let schema = make_schema_u128_i64();

    // Predicate: col[1] > 0  (col[1] is the I64 payload at logical index 1)
    use gnitz_expr::Reg;
    let pred_instrs = vec![
        gnitz_expr::LogicalInstr::LoadColInt { col: 1 }, // r0 = col[1]
        gnitz_expr::LogicalInstr::LoadConst { val: 0, unsigned: false }, // r1 = 0
        gnitz_expr::LogicalInstr::Cmp {
            op: gnitz_expr::CmpOp::Gt,
            a: Reg(0),
            b: Reg(1),
        }, // r2 = r0 > r1
    ];
    let pred_prog = gnitz_expr::LogicalProgram::new(pred_instrs, gnitz_expr::Output::Result(Reg(2)), vec![]);
    let mut p = TestPlan::default();
    p.push(0, 1, Op::Filter(Box::new(pred_prog.resolve_filter(&schema).unwrap())));
    p.push(1, 2, Op::Negate);

    let input = make_batch_u128(&schema, &[(1, 1, 10), (2, 1, -5), (3, 1, 20)]);
    let mut vm = p.build(vec![schema; 3], 2);
    let result = execute_epoch(&mut vm, input, 0).unwrap();

    assert_eq!(extract_rows(&result), vec![(1, -1, 10), (3, -1, 20)]);
}

/// An epoch whose input consolidated to nothing produces no rows at all —
/// ghost elimination reaches the VM's own epilogue, not just the operators.
#[test]
fn test_empty_input() {
    let schema = make_schema_u128_i64();
    let mut vm = union_program(schema);
    let result = execute_epoch(&mut vm, make_batch_u128(&schema, &[]), 0);
    assert!(result.unwrap().is_empty());
}

#[test]
fn test_union_operator() {
    // Union both input registers: reg 0 carries the seeded batch, reg 2 a second.
    let schema = make_schema_u128_i64();
    let input = make_batch_u128(&schema, &[(1, 1, 10), (2, 1, 20)]);
    let input_b = make_batch_u128(&schema, &[(3, 1, 30)]);

    let mut vm = union_program(schema);
    let result = vm.epoch([(DeltaReg(0), input), (DeltaReg(2), input_b)]).unwrap();

    assert_eq!(result.len(), 3);
}

/// `0 + B = B`: with an empty left operand the VM hands the right one
/// straight to the output register, weights and all, instead of copying it.
/// Under single-source-per-epoch that is every right-driven epoch of every
/// set operation, whose two operands are both post-exchange relay outputs.
#[test]
fn a_union_with_an_empty_left_operand_returns_the_right_one() {
    let schema = make_schema_u128_i64();
    let input_b = make_batch_u128(&schema, &[(3, 2, 30), (4, -1, 40)]);

    let mut vm = union_program(schema);
    let result = vm
        .epoch([(DeltaReg(0), make_batch_u128(&schema, &[])), (DeltaReg(2), input_b)])
        .unwrap();

    assert_eq!(extract_rows(&result), vec![(3, 2, 30), (4, -1, 40)]);
    assert_eq!(
        usize::from(result.pk_stride()),
        schema.pk_stride(),
        "the output carries the register's own shape",
    );
}

/// A UNION whose sides disagree on a payload column's nullability must run
/// under the OUTPUT register's schema. Only the merged schema selects the
/// null-aware comparator, which sorts a NULL cell below -3; the left input's
/// null-blind one reads the cell's zero bytes as the integer 0 and sorts it
/// above. `op_union` certifies the order it produced `Consolidated`, so the next
/// `into_consolidated` trusts it rather than re-folding.
#[test]
fn test_union_runs_under_the_merged_output_schema() {
    let (schema_a, schema_b, merged) = union_nullability_schemas();

    // Both rows on the same PK, so the merge resolves them against each
    // other: left is a negative value, right a canonical zero-filled NULL.
    let mut left = make_batch_u128(&schema_a, &[(1, 1, -3)]);
    left.certify_layout(Layout::Consolidated);

    let mut rb = BatchBuilder::new(schema_b);
    rb.begin_row(1u128, 1);
    rb.put_null();
    rb.end_row();
    let mut right = rb.finish();
    right.certify_layout(Layout::Consolidated);

    let mut p = TestPlan::default();
    p.push(0, 1, Op::Union { in_b: DeltaReg(2) });
    let mut vm = p.build(vec![schema_a, merged, schema_b], 1);
    let result = vm.epoch([(DeltaReg(0), left), (DeltaReg(2), right)]).unwrap();

    assert_eq!(result.len(), 2, "Z-Set + keeps both rows");
    assert!(
        gnitz_wire::null_word_get(result.get_null_word(0), 0),
        "the merged schema's null-aware comparator sorts NULL below -3; the left \
         input's null-blind one reads the null cell as 0 and sorts it above",
    );
}

/// `op_union`'s O(1) identity path returns the left operand verbatim, label
/// included, so a batch leaves the VM carrying the left input's schema unless
/// the epilogue stamps the output register's own. The exchange wire carries
/// that label to a master that cannot re-derive it.
#[test]
fn test_union_identity_path_output_carries_out_register_schema() {
    let (schema_a, schema_b, merged) = union_nullability_schemas();

    // in_b is never seeded, so `op_union` takes the b.is_empty() identity
    // path and hands `batch_a` straight back with `schema_a` still on it.
    let left = make_batch_u128(&schema_a, &[(1, 1, -3)]);

    let mut p = TestPlan::default();
    p.push(0, 1, Op::Union { in_b: DeltaReg(2) });
    let mut vm = p.build(vec![schema_a, merged, schema_b], 1);
    let result = vm.epoch([(DeltaReg(0), left)]).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(
        *result.schema(),
        merged,
        "a batch leaving the VM carries its output register's schema, not the operand's",
    );
}

/// A `Union` with `in_a == in_b` is `Z + Z` and must double every weight. The
/// naive by-value path moves batch_a out then reads an emptied batch_b,
/// producing +1 instead of +2.
#[test]
fn test_self_union_doubles_weights() {
    let schema = make_schema_u128_i64();
    let mut p = TestPlan::default();
    p.push(0, 1, Op::Union { in_b: DeltaReg(0) });
    let input = make_batch_u128(&schema, &[(1, 1, 10), (2, 3, 20)]);
    let mut vm = p.build(vec![schema; 2], 1);
    let result = execute_epoch(&mut vm, input, 0).unwrap();
    assert_eq!(
        extract_rows(&result),
        vec![(1, 2, 10), (2, 6, 20)],
        "self-union (Z + Z) must double every weight",
    );
}

/// A `Union` that is not its operand's last reader leaves the register in
/// place, so the instruction after it still sees the batch. Without that
/// verdict such a plan could not be compiled at all — the register would have
/// had to have no later reader.
#[test]
fn a_non_consuming_union_leaves_its_operand_readable() {
    let schema = make_schema_u128_i64();
    let mut p = TestPlan::default();
    // reg1 = -reg0; reg2 = reg0 ∪ reg1; reg3 = -reg0 again.
    p.push(0, 1, Op::Negate);
    p.push(0, 2, Op::Union { in_b: DeltaReg(1) });
    p.push(0, 3, Op::Negate);

    let input = make_batch_u128(&schema, &[(1, 1, 10), (2, 3, 20)]);
    let mut vm = p.build(vec![schema; 4], 3);
    let result = execute_epoch(&mut vm, input, 0).unwrap();
    assert_eq!(extract_rows(&result), vec![(1, -1, 10), (2, -3, 20)]);
}

/// Input arrives already consolidated — weight-1 rows and a summed weight-5 row
/// alike pass through with their weights untouched.
#[test]
fn test_input_consolidation() {
    let schema = make_schema_u128_i64();

    let mut vm = union_program(schema);
    let result = execute_epoch(&mut vm, make_batch_u128(&schema, &[(1, 1, 10), (2, 1, 20)]), 0).unwrap();
    assert_eq!(extract_rows(&result), vec![(1, 1, 10), (2, 1, 20)]);

    let mut vm = union_program(schema);
    let result = execute_epoch(&mut vm, make_batch_u128(&schema, &[(1, 5, 42)]), 0).unwrap();
    assert_eq!(extract_rows(&result), vec![(1, 5, 42)]);
}

#[test]
fn test_delta_isolation_across_ticks() {
    // Two epochs on one plan: the second must not see the first's data.
    let schema = make_schema_u128_i64();
    let mut vm = union_program(schema);

    let r1 = execute_epoch(&mut vm, make_batch_u128(&schema, &[(1, 1, 10)]), 0).unwrap();
    assert_eq!(r1.len(), 1);

    let r2 = execute_epoch(&mut vm, make_batch_u128(&schema, &[(2, 1, 20), (3, 1, 30)]), 0).unwrap();
    // Exactly tick 2's two rows, not three: no bleed from tick 1.
    assert_eq!(extract_rows(&r2), vec![(2, 1, 20), (3, 1, 30)]);
}

#[test]
fn test_map_operator() {
    // MAP projection: reorder/select columns.
    let in_schema = make_schema(&[TypeCode::I64, TypeCode::I64]);
    let out_schema = make_schema_u128_i64();

    let mut p = TestPlan::default();
    let map = gnitz_store::expr::MapPlan::from_map(
        gnitz_expr::LogicalProgram::copy_cols(&[2]),
        &in_schema,
        &out_schema,
        gnitz_store::expr::PkSource::Inherit,
    )
    .unwrap();
    p.push(0, 1, Op::Map(Box::new(map)));

    let input = make_batch_2col(in_schema, &[(1, 1, 10, 100), (2, 1, 20, 200)]);
    let mut vm = p.build(vec![in_schema, out_schema], 1);
    let result = execute_epoch(&mut vm, input, 0).unwrap();

    // The projected column is the second payload col (100, 200).
    assert_eq!(extract_rows(&result), vec![(1, 1, 100), (2, 1, 200)]);
}

#[test]
fn test_distinct_multi_tick() {
    // DISTINCT clamps weights: +3 → +1, -1 → 0 (stays positive → no retraction).
    // Uses a real Table for history.
    let schema = make_schema_u128_i64();

    let dir = tempfile::tempdir().unwrap();
    let registry = vm_registry(dir.path());
    let mut p = TestPlan::default();
    // reg 0 = input delta, reg 1 = output delta, over a real history store.
    let hist = p.table("dist_test", schema);
    p.push(
        0,
        1,
        Op::WeightClamp {
            hist,
            preset: gnitz_store::ops::ClampPreset::Distinct,
        },
    );
    let mut vm = p.build_in(&registry, vec![schema; 2], 1);

    // Tick 1: insert pk=1 with weight +3 → distinct output should be +1
    let r1 = execute_epoch(&mut vm, make_batch_u128(&schema, &[(1, 3, 42)]), 0).unwrap();
    assert_eq!(extract_rows(&r1), vec![(1, 1, 42)], "clamped to +1");

    // Tick 2: delta w=-1, integral before tick = +3, after = +2 (still positive).
    // No boundary crossing → output should be empty.
    let r2 = execute_epoch(&mut vm, make_batch_u128(&schema, &[(1, -1, 42)]), 0);
    assert!(r2.unwrap().is_empty(), "no boundary crossing: output should be empty");

    // Tick 3: delta w=-2, integral before tick = +2, after = 0 (non-positive).
    // Positive→non-positive boundary crossed → retraction: output pk=1 w=-1.
    let r3 = execute_epoch(&mut vm, make_batch_u128(&schema, &[(1, -2, 42)]), 0).unwrap();
    assert_eq!(extract_rows(&r3), vec![(1, -1, 42)], "retraction");
}

/// `JoinDT` against a bound trace cursor: the delta row's weight multiplies each
/// matching trace row's, and the output carries the left payload then the right.
#[test]
fn test_join_delta_trace() {
    let left_schema = make_schema_u128_i64();
    let right_schema = make_schema_u128_i64();
    let join_schema = make_schema(&[TypeCode::I64, TypeCode::I64]);

    let dir = tempfile::tempdir().unwrap();
    let registry = vm_registry(dir.path());
    let mut p = TestPlan::default();
    let trace = p.table("join_test", right_schema);
    // reg 0 = left delta, reg 1 = output
    let probe = gnitz_store::ops::JoinPlan::from_wire(gnitz_wire::JoinKind::Equi, false, &left_schema, &right_schema)
        .unwrap()
        .probe;
    p.push(0, 1, Op::JoinDT { trace, probe });
    let mut vm = p.build_in(&registry, vec![left_schema, join_schema], 1);
    // Three trace rows on one PK, differing in payload.
    let trace_batch = make_batch_u128(&right_schema, &[(10, 1, 100), (10, 1, 200), (10, 1, 300)]);
    vm.state.ingest_owned(trace, trace_batch).unwrap();

    let input = make_batch_u128(&left_schema, &[(10, 2, 50)]);
    let result = execute_epoch(&mut vm, input, 0).unwrap();

    // 1 delta row × 3 trace rows, each at the weight product 2 × 1, and each
    // carrying the left payload before the right.
    let row = |left: i64, right: i64| {
        (
            opk_pk(&join_schema, &[10]),
            vec![Some(left.to_le_bytes().to_vec()), Some(right.to_le_bytes().to_vec())],
        )
    };
    assert_eq!(
        zset_of(&result, &join_schema),
        std::collections::HashMap::from([(row(50, 100), 2), (row(50, 200), 2), (row(50, 300), 2)]),
    );
}

/// REDUCE grouping by a *payload* column rather than the PK — the shape whose
/// output PK is synthesised from the group key.
#[test]
fn test_reduce_groups_by_a_payload_column() {
    let in_schema = make_schema(&[
        TypeCode::I64, // group col (payload col 0)
        TypeCode::I64, // agg col (payload col 1)
    ]);
    // [U128 PK, I64 group_col, I64 sum_col, I64 count companion]
    let out_schema = make_schema(&[TypeCode::I64, TypeCode::I64, TypeCode::I64]);

    let dir = tempfile::tempdir().unwrap();
    let registry = vm_registry(dir.path());
    let mut p = TestPlan::default();
    let out_trace = p.table("tr_out", out_schema);
    // SUM of payload col 1 (schema col 2), plus the trailing Count cardinality
    // companion every all-linear reduce carries.
    let agg_descs = [
        AggDescriptor { col_idx: 2, agg_op: AggFunc::Sum },
        AggDescriptor::COUNT_STAR,
    ];
    let group_cols = [1u32]; // schema col 1 = payload col 0 (group key)

    push_reduce(&mut p, &agg_descs, &group_cols, in_schema, out_trace, false);
    p.integrate(1, out_trace);
    let mut vm = p.build_in(&registry, vec![in_schema, out_schema], 1);

    // Both rows in group=1, values 10 and 20.
    let input = make_batch_2col(in_schema, &[(1, 1, 1, 10), (2, 1, 1, 20)]);
    let r1 = execute_epoch(&mut vm, input, 0).unwrap();

    assert_eq!(r1.len(), 1, "one group → one output row");
    let sum_val = i64::from_le_bytes(r1.col_data(1)[0..8].try_into().unwrap());
    assert_eq!(sum_val, 30, "SUM(10+20) must be 30");
}

/// Multi-agg reduce: COUNT + SUM on the same column in one pass.
#[test]
fn test_reduce_multi_agg() {
    // Input: U128 pk, val(I64). All rows in the same group (pk=1).
    let in_schema = make_schema_u128_i64();
    // Output: pk, count(I64), sum(I64) — GROUP BY pk → natural PK.
    let out_schema = make_schema(&[TypeCode::I64, TypeCode::I64]);

    let dir = tempfile::tempdir().unwrap();
    let registry = vm_registry(dir.path());
    let mut p = TestPlan::default();
    let out_trace = p.table("ma_tr_out", out_schema);
    let agg_descs = [
        AggDescriptor {
            col_idx: 1, // schema col index for the val column
            agg_op: AggFunc::Count,
        },
        AggDescriptor { col_idx: 1, agg_op: AggFunc::Sum },
    ];
    // GROUP BY col 0 (= pk, schema col index 0)
    let group_cols = [0u32];

    push_reduce(&mut p, &agg_descs, &group_cols, in_schema, out_trace, false);
    p.integrate(1, out_trace);
    let mut vm = p.build_in(&registry, vec![in_schema, out_schema], 1);

    // Three rows all with pk=1, vals 10, 20, 30.
    let input = make_batch_u128(&in_schema, &[(1, 1, 10), (1, 1, 20), (1, 1, 30)]);
    let result = execute_epoch(&mut vm, input, 0).unwrap();

    assert_eq!(result.len(), 1, "multi-agg should produce 1 group");
    let count_val = i64::from_le_bytes(result.col_data(0)[0..8].try_into().unwrap());
    let sum_val = i64::from_le_bytes(result.col_data(1)[0..8].try_into().unwrap());
    assert_eq!(count_val, 3, "COUNT should be 3");
    assert_eq!(sum_val, 60, "SUM should be 60");
}

// ── The empty-epoch skip ─────────────────────────────────────────────────

/// A global-ground reduce this worker owns mints its V₀ row on ONE empty epoch;
/// after that `trace_out` holds it, so every later empty epoch skips the pass.
#[test]
fn an_empty_epoch_mints_the_ground_row_once() {
    let in_schema = make_schema_u128_i64();
    let aggs = [AggDescriptor { col_idx: 1, agg_op: AggFunc::Count }];
    let dir = tempfile::tempdir().unwrap();
    let registry = vm_registry(dir.path());
    let mut p = TestPlan::default();
    let plan = gnitz_store::ops::ReducePlan::from_wire(&in_schema, &[], &aggs, true).unwrap();
    let out_schema = plan.shape.output_schema;
    let out_trace = p.table("ground_tr", out_schema);
    p.push(
        0,
        1,
        Op::Reduce {
            out_trace,
            plan: Box::new(BakedReduce::new(plan, None)),
        },
    );
    p.integrate(1, out_trace);
    let mut vm = p.build_in(&registry, vec![in_schema, out_schema], 1);
    assert!(
        vm.pending_ground_row,
        "the latch is derived from the emitted reduce plans",
    );

    let first = execute_epoch(&mut vm, Batch::empty_with_schema(&in_schema), 0).unwrap();
    assert_eq!(first.len(), 1, "the ground row");
    assert!(!vm.pending_ground_row, "cleared before the pass, not after it");
    assert!(
        execute_epoch(&mut vm, Batch::empty_with_schema(&in_schema), 0)
            .unwrap()
            .is_empty(),
        "a second empty epoch has nothing left to mint",
    );
}

/// A non-empty first epoch mints V₀ through the reduce's ordinary path, so it
/// spends the latch too — and the all-empty epoch after it has nothing left to
/// dispatch for.
#[test]
fn a_non_empty_epoch_spends_the_ground_latch_too() {
    let in_schema = make_schema_u128_i64();
    let aggs = [AggDescriptor { col_idx: 1, agg_op: AggFunc::Count }];
    let dir = tempfile::tempdir().unwrap();
    let registry = vm_registry(dir.path());
    let mut p = TestPlan::default();
    let plan = gnitz_store::ops::ReducePlan::from_wire(&in_schema, &[], &aggs, true).unwrap();
    let out_schema = plan.shape.output_schema;
    let out_trace = p.table("ground_tr", out_schema);
    p.push(
        0,
        1,
        Op::Reduce {
            out_trace,
            plan: Box::new(BakedReduce::new(plan, None)),
        },
    );
    p.integrate(1, out_trace);
    let mut vm = p.build_in(&registry, vec![in_schema, out_schema], 1);
    assert!(vm.pending_ground_row);

    let first = execute_epoch(&mut vm, make_batch_u128(&in_schema, &[(1, 1, 10)]), 0).unwrap();
    assert_eq!(first.len(), 1, "the global group's own row, at V₀'s key");
    assert!(!vm.pending_ground_row, "a dispatched epoch spends the latch either way");

    assert!(
        execute_epoch(&mut vm, Batch::empty_with_schema(&in_schema), 0)
            .unwrap()
            .is_empty(),
        "the trace already holds V₀, so there is nothing left to mint",
    );
}

/// Without a ground row an empty epoch produces nothing and runs no dispatch —
/// but it still clears the previous epoch's registers, which is the one thing
/// the skipped prologue would otherwise have done.
#[test]
fn an_empty_epoch_skips_the_pass_and_still_clears_the_registers() {
    let schema = make_schema_u128_i64();
    let mut p = TestPlan::default();
    // Reg 1 is the sink; reg 2 is written and never read, so nothing frees it
    // and it is what still holds rows when the epoch ends — the precondition the
    // empty epoch below has to clear.
    p.push(0, 1, Op::WorkerFilter { slot: Slot::SOLO });
    p.push(0, 2, Op::Negate);
    let mut vm = p.build(vec![schema; 3], 1);
    assert!(!vm.pending_ground_row);

    let out = execute_epoch(&mut vm, make_batch_u128(&schema, &[(1, 1, 10)]), 0).unwrap();
    assert_eq!(out.len(), 1, "one row through");
    assert_eq!(vm.batches[2].len(), 1, "a register nothing reads keeps its batch");

    assert!(execute_epoch(&mut vm, Batch::empty_with_schema(&schema), 0)
        .unwrap()
        .is_empty());
    assert!(
        vm.batches.iter().all(|b| b.is_empty()),
        "the skipped epoch still released the previous one's batches",
    );
}

// ── The two-term DBSP join, end to end ────────────────────────────────────

/// One side's rows as `(pk, weight, payload)`.
type JoinRows<'a> = &'a [(u128, i64, i64)];

/// `ΔA ⋈ z⁻¹I(B) + ΔB ⋈ z⁻¹I(A)` as the planner builds it: both terms in side
/// order, both integrates, one union. Registers are 0/1 = the two deltas,
/// 2/3 = the terms, 4 = the union.
fn two_term_join(
    p: &mut TestPlan,
    kind: gnitz_wire::JoinKind,
    schema: SchemaDescriptor,
) -> (SchemaDescriptor, Vec<SchemaDescriptor>) {
    let trace_a = p.table("ta", schema);
    let trace_b = p.table("tb", schema);
    let plan = |right: bool| gnitz_store::ops::JoinPlan::from_wire(kind, right, &schema, &schema).unwrap();
    let out_schema = plan(false).out_schema;

    p.push(0, 2, Op::JoinDT { trace: trace_b, probe: plan(false).probe });
    p.push(1, 3, Op::JoinDT { trace: trace_a, probe: plan(true).probe });
    p.push(2, 4, Op::Union { in_b: DeltaReg(3) });
    p.integrate(0, trace_a);
    p.integrate(1, trace_b);
    (out_schema, vec![schema, schema, out_schema, out_schema, out_schema])
}

/// The product both terms together must denote: every `(a, b)` pair the kind
/// admits, at `w_a · w_b`, keyed as the output schema keys it and carrying A's
/// payload before B's.
fn join_reference(
    kind: gnitz_wire::JoinKind,
    schema: &SchemaDescriptor,
    a: JoinRows<'_>,
    b: JoinRows<'_>,
) -> std::collections::HashMap<crate::test_support::RowKey, i64> {
    let mut want = std::collections::HashMap::new();
    for &(a_pk, a_w, a_v) in a {
        for &(b_pk, b_w, b_v) in b {
            if matches!(kind, gnitz_wire::JoinKind::Equi) && a_pk != b_pk {
                continue;
            }
            let key = match kind {
                gnitz_wire::JoinKind::Cross => [opk_pk(schema, &[a_pk]), opk_pk(schema, &[b_pk])].concat(),
                _ => opk_pk(schema, &[a_pk]),
            };
            let cells = vec![Some(a_v.to_le_bytes().to_vec()), Some(b_v.to_le_bytes().to_vec())];
            *want.entry((key, cells)).or_insert(0) += a_w * b_w;
        }
    }
    want.retain(|_, w| *w != 0);
    want
}

/// Both join kinds, over UNSORTED deltas carrying a cancelling duplicate: the
/// A-sourced epoch integrates A and emits nothing, the B-sourced one joins
/// against it, and the two epochs together denote the product weight for weight.
#[test]
fn a_two_term_join_denotes_the_product_across_both_epochs() {
    // Raw (unsorted, with a pair that cancels) and the Z-set each denotes.
    let a_raw: JoinRows = &[(2, 1, 20), (1, 1, 10), (1, 2, 10)];
    let a_net: JoinRows = &[(1, 3, 10), (2, 1, 20)];
    let b_raw: JoinRows = &[(2, 1, 200), (1, 1, 100), (1, -1, 100), (1, 1, 101)];
    let b_net: JoinRows = &[(1, 1, 101), (2, 1, 200)];

    for kind in [gnitz_wire::JoinKind::Equi, gnitz_wire::JoinKind::Cross] {
        let dir = tempfile::tempdir().unwrap();
        let registry = vm_registry(dir.path());
        let schema = make_schema_u128_i64();
        let mut p = TestPlan::default();
        let (out_schema, schemas) = two_term_join(&mut p, kind, schema);
        let mut vm = p.build_in(&registry, schemas, 4);

        let a = execute_epoch(&mut vm, make_batch_u128_raw(&schema, a_raw), 0).unwrap();
        assert!(a.is_empty(), "{kind:?}: nothing to join against yet");

        let b = execute_epoch(&mut vm, make_batch_u128_raw(&schema, b_raw), 1).unwrap();
        assert_eq!(
            zset_of(&b, &out_schema),
            join_reference(kind, &schema, a_net, b_net),
            "{kind:?}",
        );
    }
}

// ── Integrates run after the range, and a replay runs none ───────────────

/// A `Reduce` and a `TopN` both return the register their integrate reads, so an
/// integrate running against the extracted output would write nothing and the
/// operator would never see its own history: the second epoch would add a row
/// instead of retracting the first one's.
#[test]
fn a_second_epoch_retracts_the_first_ones_aggregate() {
    let in_schema = make_schema_u128_i64();
    let dir = tempfile::tempdir().unwrap();
    let registry = vm_registry(dir.path());

    let mut p = TestPlan::default();
    let plan = gnitz_store::ops::ReducePlan::from_wire(
        &in_schema,
        &[],
        &[
            AggDescriptor { col_idx: 1, agg_op: AggFunc::Sum },
            AggDescriptor::COUNT_STAR,
        ],
        false,
    )
    .unwrap();
    let out_schema = plan.shape.output_schema;
    let out_trace = p.table("sum_tr", out_schema);
    p.push(
        0,
        1,
        Op::Reduce {
            out_trace,
            plan: Box::new(BakedReduce::new(plan, None)),
        },
    );
    p.integrate(1, out_trace);
    let mut vm = p.build_in(&registry, vec![in_schema, out_schema], 1);

    let sum_of = |vm: &TestVm| -> Vec<i64> {
        let out = vm.state.cursor(out_trace).materialize();
        (0..out.len())
            .map(|i| i64::from_le_bytes(out.col_data(0)[i * 8..(i + 1) * 8].try_into().unwrap()))
            .collect()
    };

    execute_epoch(&mut vm, make_batch_u128(&in_schema, &[(1, 1, 10), (2, 1, 20)]), 0).unwrap();
    assert_eq!(sum_of(&vm), vec![30], "the integrate wrote the first aggregate");

    execute_epoch(&mut vm, make_batch_u128(&in_schema, &[(3, 1, 5)]), 0).unwrap();
    assert_eq!(
        sum_of(&vm),
        vec![35],
        "the second epoch retracted 30 and integrated 35, leaving one live row",
    );
}

/// A `TopN`'s integrate is the same aliasing shape, through a different emit arm.
#[test]
fn a_second_topn_epoch_retracts_the_first_ones_row() {
    let in_schema = make_schema_u128_i64();
    let dir = tempfile::tempdir().unwrap();
    let registry = vm_registry(dir.path());

    let mut p = TestPlan::default();
    let order = [gnitz_wire::OrderKey { col: 1, desc: true, nulls_first: false }];
    let plan = gnitz_store::ops::TopNPlan::from_wire(&in_schema, &[], &order, 1, 0).unwrap();
    let out_schema = plan.output_schema;
    let out_trace = p.table("topn_tr", out_schema);
    let index_table = p.table("topn_idx", plan.index.schema);
    p.push(
        0,
        1,
        Op::TopN {
            out_trace,
            plan: Box::new(BakedTopN { plan, index_table }),
        },
    );
    p.integrate(1, out_trace);
    let mut vm = p.build_in(&registry, vec![in_schema, out_schema], 1);

    execute_epoch(&mut vm, make_batch_u128(&in_schema, &[(1, 1, 10)]), 0).unwrap();
    assert_eq!(trace_zset(&vm, out_trace, &out_schema).len(), 1);

    // A strictly greater value displaces the top row entirely.
    execute_epoch(&mut vm, make_batch_u128(&in_schema, &[(2, 1, 99)]), 0).unwrap();
    let held = trace_zset(&vm, out_trace, &out_schema);
    assert_eq!(
        held.len(),
        1,
        "the first row was retracted, not kept beside the new one"
    );
    assert!(held.values().all(|&w| w == 1));
}

/// A hydration replay seeds a register out of a trace and re-runs the program
/// over it; running the integrates too would write that seed back and double the
/// trace's weights on every read.
#[test]
fn a_replay_leaves_every_trace_as_it_found_it() {
    let schema = make_schema_u128_i64();
    let dir = tempfile::tempdir().unwrap();
    let registry = vm_registry(dir.path());

    let mut p = TestPlan::default();
    let trace = p.table("replay_tr", schema);
    p.push(0, 1, Op::WorkerFilter { slot: Slot::SOLO });
    p.integrate(1, trace);
    let mut vm = p.build_in(&registry, vec![schema; 2], 1);

    let rows = [(1u128, 1i64, 10i64), (2, 1, 20)];
    execute_epoch(&mut vm, make_batch_u128(&schema, &rows), 0).unwrap();
    let before = trace_zset(&vm, trace, &schema);
    assert_eq!(before.len(), 2);

    let seed = (DeltaReg(0), make_batch_u128(&schema, &rows));
    let replayed = vm.replay(0, seed).unwrap();
    assert_eq!(replayed.len(), 2, "the replay still produces the rows it recomputed");
    assert_eq!(trace_zset(&vm, trace, &schema), before, "and writes none of them back");
}
