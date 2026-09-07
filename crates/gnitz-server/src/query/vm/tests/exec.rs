//! Dispatch-loop tests: one epoch of a hand-built program per opcode path.

use super::*;
use crate::test_support::{make_batch_u128, make_schema_u128_i64, opk_pk, scratch_table, zset_of};
use gnitz_store::schema::{type_code, SchemaColumn, SchemaDescriptor};
use gnitz_store::storage::{Batch, BatchBuilder, Layout, StorageError};
use gnitz_wire::AggDescriptor;
use gnitz_wire::AggFunc;

// ── Test helpers ─────────────────────────────────────────────────────────

/// One epoch seeding a single input register — production seeds through
/// `execute_epoch_multi` directly, with one entry per exchange side.
fn execute_epoch(vm: &mut VmHandle, input: Batch, input_reg: u16) -> Result<Option<Batch>, StorageError> {
    execute_epoch_multi(vm, std::iter::once((DeltaReg(input_reg), input)))
}

/// A table under `dir` backing a trace register; the caller hands it to
/// `ProgramBuilder::push_table`, which owns it from then on.
fn owned_table(dir: &std::path::Path, name: &str, schema: SchemaDescriptor) -> gnitz_store::storage::Table {
    scratch_table(dir.join(name).to_str().unwrap(), schema, 0)
}

/// A reduce with no value index, over the register convention every test reduce
/// shares: 0 = input delta, 1 = output trace, 2 = raw delta out. `out_key` is the
/// one kind a given (schema, group cols) admits, so it is derived, not passed.
fn push_reduce(
    b: &mut ProgramBuilder,
    aggs: &[AggDescriptor],
    gcols: &[u32],
    in_schema: SchemaDescriptor,
    global_ground: bool,
    i_am_owner: bool,
) -> SchemaDescriptor {
    let plan = gnitz_store::ops::ReducePlan::from_wire(&in_schema, gcols, aggs, global_ground, i_am_owner).unwrap();
    let out_schema = plan.output_schema;
    let plan_idx = b.add_reduce_plan(plan, None);
    b.push(Instr::Reduce {
        in_reg: DeltaReg(0),
        trace_out_reg: TraceReg(1),
        out_reg: DeltaReg(2),
        plan_idx,
    });
    out_schema
}

/// A U128 PK plus `col_types` as payload columns — the 2- and 3-payload shapes
/// the multi-column tests need. The one-payload shape is
/// [`make_schema_u128_i64`].
fn make_schema(col_types: &[u8]) -> SchemaDescriptor {
    let mut columns = [SchemaColumn::EMPTY; gnitz_store::schema::MAX_COLUMNS];
    columns[0] = SchemaColumn::new(type_code::U128, 0);
    for (i, &tc) in col_types.iter().enumerate() {
        columns[i + 1] = SchemaColumn::new(tc, 0);
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
    b.certify_layout(Layout::Consolidated, &schema);
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
    let pk = SchemaColumn::new(type_code::U128, 0);
    let not_null = SchemaDescriptor::new(&[pk, SchemaColumn::new(type_code::I64, 0)], &[0]);
    let nullable = SchemaDescriptor::new(&[pk, SchemaColumn::new(type_code::I64, 1)], &[0]);
    (not_null, nullable, nullable)
}

/// A pass-through program: `reg1 = reg0 ∪ reg2`, with reg2 never seeded unless
/// the test seeds it. Three delta registers under one schema, output reg 1.
fn union_program(schema: SchemaDescriptor) -> Box<VmHandle> {
    let mut builder = ProgramBuilder::new();
    builder.push(Instr::Union {
        in_a: DeltaReg(0),
        in_b: DeltaReg(2),
        out_reg: DeltaReg(1),
    });
    builder.build(vec![RegisterMeta::delta(schema); 3], DeltaReg(1))
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
        gnitz_expr::LogicalInstr::LoadConst { val: 0 },  // r1 = 0
        gnitz_expr::LogicalInstr::Cmp {
            op: gnitz_expr::CmpOp::Gt,
            a: Reg(0),
            b: Reg(1),
        }, // r2 = r0 > r1
    ];
    let pred_prog = gnitz_expr::LogicalProgram::new(pred_instrs, Vec::new(), Some(Reg(2)), vec![]);
    let mut builder = ProgramBuilder::new();
    let pred_idx = builder.push_predicate(pred_prog.resolve_filter(&schema).unwrap());
    builder.push(Instr::Filter {
        in_reg: DeltaReg(0),
        out_reg: DeltaReg(1),
        pred_idx,
    });
    builder.push(Instr::Negate {
        in_reg: DeltaReg(1),
        out_reg: DeltaReg(2),
    });

    let input = make_batch_u128(&schema, &[(1, 1, 10), (2, 1, -5), (3, 1, 20)]);
    let mut vm = builder.build(vec![RegisterMeta::delta(schema); 3], DeltaReg(2));
    let result = execute_epoch(&mut vm, input, 0).unwrap().unwrap();

    assert_eq!(extract_rows(&result), vec![(1, -1, 10), (3, -1, 20)]);
}

/// An epoch whose input consolidated to nothing produces no output batch at all
/// — ghost elimination reaches the VM's own epilogue, not just the operators.
#[test]
fn test_empty_input() {
    let schema = make_schema_u128_i64();
    let mut vm = union_program(schema);
    let result = execute_epoch(&mut vm, make_batch_u128(&schema, &[]), 0);
    assert!(result.unwrap().is_none());
}

#[test]
fn test_union_operator() {
    // Union both input registers: reg 0 carries the seeded batch, reg 2 a second.
    let schema = make_schema_u128_i64();
    let input = make_batch_u128(&schema, &[(1, 1, 10), (2, 1, 20)]);
    let input_b = make_batch_u128(&schema, &[(3, 1, 30)]);

    let mut vm = union_program(schema);
    let result = execute_epoch_multi(&mut vm, [(DeltaReg(0), input), (DeltaReg(2), input_b)])
        .unwrap()
        .unwrap();

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
    let result = execute_epoch_multi(
        &mut vm,
        [(DeltaReg(0), make_batch_u128(&schema, &[])), (DeltaReg(2), input_b)],
    )
    .unwrap()
    .expect("the right operand is the whole output");

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
    left.certify_layout(Layout::Consolidated, &schema_a);

    let mut rb = BatchBuilder::new(schema_b);
    rb.begin_row(1u128, 1);
    rb.put_null();
    rb.end_row();
    let mut right = rb.finish();
    right.certify_layout(Layout::Consolidated, &schema_b);

    let mut builder = ProgramBuilder::new();
    builder.push(Instr::Union {
        in_a: DeltaReg(0),
        in_b: DeltaReg(2),
        out_reg: DeltaReg(1),
    });
    let reg_meta = vec![
        RegisterMeta::delta(schema_a),
        RegisterMeta::delta(merged),
        RegisterMeta::delta(schema_b),
    ];
    let mut vm = builder.build(reg_meta, DeltaReg(1));
    let result = execute_epoch_multi(&mut vm, [(DeltaReg(0), left), (DeltaReg(2), right)])
        .unwrap()
        .unwrap();

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

    let mut builder = ProgramBuilder::new();
    builder.push(Instr::Union {
        in_a: DeltaReg(0),
        in_b: DeltaReg(2),
        out_reg: DeltaReg(1),
    });
    let reg_meta = vec![
        RegisterMeta::delta(schema_a),
        RegisterMeta::delta(merged),
        RegisterMeta::delta(schema_b),
    ];
    let mut vm = builder.build(reg_meta, DeltaReg(1));
    let result = execute_epoch_multi(&mut vm, [(DeltaReg(0), left)]).unwrap().unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(
        result.schema, merged,
        "a batch leaving the VM carries its output register's schema, not the operand's",
    );
}

/// A `Union` with `in_a == in_b` is `Z + Z` and must double every weight. The
/// naive by-value path moves batch_a out then reads an emptied batch_b,
/// producing +1 instead of +2.
#[test]
fn test_self_union_doubles_weights() {
    let schema = make_schema_u128_i64();
    let mut builder = ProgramBuilder::new();
    builder.push(Instr::Union {
        in_a: DeltaReg(0),
        in_b: DeltaReg(0),
        out_reg: DeltaReg(1),
    });
    let input = make_batch_u128(&schema, &[(1, 1, 10), (2, 3, 20)]);
    let mut vm = builder.build(vec![RegisterMeta::delta(schema); 2], DeltaReg(1));
    let result = execute_epoch(&mut vm, input, 0).unwrap().unwrap();
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
    let mut builder = ProgramBuilder::new();
    // reg1 = -reg0; reg2 = reg0 ∪ reg1; reg3 = -reg0 again.
    builder.push(Instr::Negate {
        in_reg: DeltaReg(0),
        out_reg: DeltaReg(1),
    });
    builder.push(Instr::Union {
        in_a: DeltaReg(0),
        in_b: DeltaReg(1),
        out_reg: DeltaReg(2),
    });
    builder.push(Instr::Negate {
        in_reg: DeltaReg(0),
        out_reg: DeltaReg(3),
    });

    let input = make_batch_u128(&schema, &[(1, 1, 10), (2, 3, 20)]);
    let mut vm = builder.build(vec![RegisterMeta::delta(schema); 4], DeltaReg(3));
    let result = execute_epoch(&mut vm, input, 0)
        .unwrap()
        .expect("the trailing reader must still see the union's operand");
    assert_eq!(extract_rows(&result), vec![(1, -1, 10), (2, -3, 20)]);
}

/// Input arrives already consolidated — weight-1 rows and a summed weight-5 row
/// alike pass through with their weights untouched.
#[test]
fn test_input_consolidation() {
    let schema = make_schema_u128_i64();

    let mut vm = union_program(schema);
    let result = execute_epoch(&mut vm, make_batch_u128(&schema, &[(1, 1, 10), (2, 1, 20)]), 0)
        .unwrap()
        .unwrap();
    assert_eq!(extract_rows(&result), vec![(1, 1, 10), (2, 1, 20)]);

    let mut vm = union_program(schema);
    let result = execute_epoch(&mut vm, make_batch_u128(&schema, &[(1, 5, 42)]), 0)
        .unwrap()
        .unwrap();
    assert_eq!(extract_rows(&result), vec![(1, 5, 42)]);
}

#[test]
fn test_delta_isolation_across_ticks() {
    // Two epochs on one plan: the second must not see the first's data.
    let schema = make_schema_u128_i64();
    let mut vm = union_program(schema);

    let r1 = execute_epoch(&mut vm, make_batch_u128(&schema, &[(1, 1, 10)]), 0)
        .unwrap()
        .unwrap();
    assert_eq!(r1.len(), 1);

    let r2 = execute_epoch(&mut vm, make_batch_u128(&schema, &[(2, 1, 20), (3, 1, 30)]), 0)
        .unwrap()
        .unwrap();
    // Exactly tick 2's two rows, not three: no bleed from tick 1.
    assert_eq!(extract_rows(&r2), vec![(2, 1, 20), (3, 1, 30)]);
}

#[test]
fn test_map_operator() {
    // MAP projection: reorder/select columns.
    let in_schema = make_schema(&[type_code::I64, type_code::I64]);
    let out_schema = make_schema_u128_i64();

    let mut builder = ProgramBuilder::new();
    let map_idx = builder.push_map(
        gnitz_store::expr::MapPlan::from_map(
            gnitz_expr::LogicalProgram::copy_cols(&[2]),
            &in_schema,
            &out_schema,
            gnitz_store::expr::PkSource::Inherit,
        )
        .unwrap(),
    );
    builder.push(Instr::Map {
        in_reg: DeltaReg(0),
        out_reg: DeltaReg(1),
        map_idx,
    });

    let input = make_batch_2col(in_schema, &[(1, 1, 10, 100), (2, 1, 20, 200)]);
    let reg_meta = vec![RegisterMeta::delta(in_schema), RegisterMeta::delta(out_schema)];
    let mut vm = builder.build(reg_meta, DeltaReg(1));
    let result = execute_epoch(&mut vm, input, 0).unwrap().unwrap();

    // The projected column is the second payload col (100, 200).
    assert_eq!(extract_rows(&result), vec![(1, 1, 100), (2, 1, 200)]);
}

#[test]
fn test_distinct_multi_tick() {
    // DISTINCT clamps weights: +3 → +1, -1 → 0 (stays positive → no retraction).
    // Uses a real Table for history.
    let schema = make_schema_u128_i64();

    let dir = tempfile::tempdir().unwrap();
    let table = owned_table(dir.path(), "dist_test", schema);
    let mut builder = ProgramBuilder::new();
    // reg 0 = input delta, reg 1 = history trace, reg 2 = output delta
    builder.push_table(table);
    builder.push(Instr::WeightClamp {
        in_reg: DeltaReg(0),
        hist_reg: TraceReg(1),
        out_reg: DeltaReg(2),
        preset: gnitz_store::ops::ClampPreset::Distinct,
    });

    let reg_meta = vec![
        RegisterMeta::delta(schema),
        RegisterMeta::trace(schema, TableIdx(0)),
        RegisterMeta::delta(schema),
    ];

    // The history register is backed by the plan's owned table, so each tick
    // opens its cursor through `bind_trace_cursors` — the production path.
    let mut vm = builder.build(reg_meta, DeltaReg(2));

    // Tick 1: insert pk=1 with weight +3 → distinct output should be +1
    let r1 = execute_epoch(&mut vm, make_batch_u128(&schema, &[(1, 3, 42)]), 0)
        .unwrap()
        .unwrap();
    assert_eq!(extract_rows(&r1), vec![(1, 1, 42)], "clamped to +1");

    // Tick 2: delta w=-1, integral before tick = +3, after = +2 (still positive).
    // No boundary crossing → output should be empty.
    let r2 = execute_epoch(&mut vm, make_batch_u128(&schema, &[(1, -1, 42)]), 0);
    assert!(r2.unwrap().is_none(), "no boundary crossing: output should be empty");

    // Tick 3: delta w=-2, integral before tick = +2, after = 0 (non-positive).
    // Positive→non-positive boundary crossed → retraction: output pk=1 w=-1.
    let r3 = execute_epoch(&mut vm, make_batch_u128(&schema, &[(1, -2, 42)]), 0)
        .unwrap()
        .unwrap();
    assert_eq!(extract_rows(&r3), vec![(1, -1, 42)], "retraction");
}

/// `JoinDT` against a bound trace cursor: the delta row's weight multiplies each
/// matching trace row's, and the output carries the left payload then the right.
#[test]
fn test_join_delta_trace() {
    let left_schema = make_schema_u128_i64();
    let right_schema = make_schema_u128_i64();
    let join_schema = make_schema(&[type_code::I64, type_code::I64]);

    let dir = tempfile::tempdir().unwrap();
    let mut table = owned_table(dir.path(), "join_test", right_schema);
    // Three trace rows on one PK, differing in payload.
    let trace_batch = make_batch_u128(&right_schema, &[(10, 1, 100), (10, 1, 200), (10, 1, 300)]);
    table.ingest_owned_batch(trace_batch).unwrap();

    let mut builder = ProgramBuilder::new();
    builder.push_table(table);
    // reg 0 = left delta, reg 1 = right trace, reg 2 = output
    builder.push(Instr::JoinDT {
        probe: gnitz_store::ops::JoinProbe::Equi,
        delta_reg: DeltaReg(0),
        trace_reg: TraceReg(1),
        out_reg: DeltaReg(2),
    });

    let reg_meta = vec![
        RegisterMeta::delta(left_schema),
        RegisterMeta::trace(right_schema, TableIdx(0)),
        RegisterMeta::delta(join_schema),
    ];
    let mut vm = builder.build(reg_meta, DeltaReg(2));

    let input = make_batch_u128(&left_schema, &[(10, 2, 50)]);
    let result = execute_epoch(&mut vm, input, 0).unwrap().unwrap();
    assert_eq!(
        vm.regfile.batches[1].len(),
        0,
        "a trace is reached through its cursor, never its register's batch — which is what \
         lets the per-epoch clear run over every register blind",
    );

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
        type_code::I64, // group col (payload col 0)
        type_code::I64, // agg col (payload col 1)
    ]);
    // [U128 PK, I64 group_col, I64 sum_col, I64 count companion]
    let out_schema = make_schema(&[type_code::I64, type_code::I64, type_code::I64]);

    let dir = tempfile::tempdir().unwrap();
    let trace_out_table = owned_table(dir.path(), "tr_out", out_schema);
    // SUM of payload col 1 (schema col 2), plus the trailing Count cardinality
    // companion every all-linear reduce carries.
    let agg_descs = [
        AggDescriptor { col_idx: 2, agg_op: AggFunc::Sum },
        AggDescriptor { col_idx: 0, agg_op: AggFunc::Count },
    ];
    let group_cols = [1u32]; // schema col 1 = payload col 0 (group key)

    let mut builder = ProgramBuilder::new();
    builder.push_table(trace_out_table);
    push_reduce(&mut builder, &agg_descs, &group_cols, in_schema, false, false);
    builder.push(Instr::Integrate {
        in_reg: DeltaReg(2),
        trace_reg: TraceReg(1),
    });

    let reg_meta = vec![
        RegisterMeta::delta(in_schema),
        RegisterMeta::trace(out_schema, TableIdx(0)),
        RegisterMeta::delta(out_schema),
    ];
    let mut vm = builder.build(reg_meta, DeltaReg(2));

    // Both rows in group=1, values 10 and 20.
    let input = make_batch_2col(in_schema, &[(1, 1, 1, 10), (2, 1, 1, 20)]);
    let r1 = execute_epoch(&mut vm, input, 0).unwrap().unwrap();

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
    let out_schema = make_schema(&[type_code::I64, type_code::I64]);

    let dir = tempfile::tempdir().unwrap();
    let trace_out_table = owned_table(dir.path(), "ma_tr_out", out_schema);
    let agg_descs = [
        AggDescriptor {
            col_idx: 1, // schema col index for the val column
            agg_op: AggFunc::Count,
        },
        AggDescriptor { col_idx: 1, agg_op: AggFunc::Sum },
    ];
    // GROUP BY col 0 (= pk, schema col index 0)
    let group_cols = [0u32];

    let mut builder = ProgramBuilder::new();
    builder.push_table(trace_out_table);
    push_reduce(&mut builder, &agg_descs, &group_cols, in_schema, false, false);
    builder.push(Instr::Integrate {
        in_reg: DeltaReg(2),
        trace_reg: TraceReg(1),
    });

    let reg_meta = vec![
        RegisterMeta::delta(in_schema),
        RegisterMeta::trace(out_schema, TableIdx(0)),
        RegisterMeta::delta(out_schema),
    ];
    let mut vm = builder.build(reg_meta, DeltaReg(2));

    // Three rows all with pk=1, vals 10, 20, 30.
    let input = make_batch_u128(&in_schema, &[(1, 1, 10), (1, 1, 20), (1, 1, 30)]);
    let result = execute_epoch(&mut vm, input, 0).unwrap().unwrap();

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
    let mut builder = ProgramBuilder::new();
    let out_schema = push_reduce(&mut builder, &aggs, &[], in_schema, true, true);
    builder.push_table(owned_table(dir.path(), "ground_tr", out_schema));
    builder.push(Instr::Integrate {
        in_reg: DeltaReg(2),
        trace_reg: TraceReg(1),
    });
    let reg_meta = vec![
        RegisterMeta::delta(in_schema),
        RegisterMeta::trace(out_schema, TableIdx(0)),
        RegisterMeta::delta(out_schema),
    ];
    let mut vm = builder.build(reg_meta, DeltaReg(2));
    assert!(
        vm.pending_ground_row,
        "the latch is derived from the finished reduce-plan pool",
    );

    let first = execute_epoch(&mut vm, Batch::empty_with_schema(&in_schema), 0)
        .unwrap()
        .expect("the ground row");
    assert_eq!(first.len(), 1);
    assert!(!vm.pending_ground_row, "cleared before the pass, not after it");
    assert!(
        execute_epoch(&mut vm, Batch::empty_with_schema(&in_schema), 0)
            .unwrap()
            .is_none(),
        "a second empty epoch has nothing left to mint",
    );
}

/// The latch is `global_ground && i_am_owner`, not `global_ground`: the workers
/// that do not own V₀ have nothing to mint, so an empty epoch must not dispatch
/// on them either.
#[test]
fn a_ground_reduce_this_worker_does_not_own_leaves_the_latch_clear() {
    let in_schema = make_schema_u128_i64();
    let aggs = [AggDescriptor { col_idx: 1, agg_op: AggFunc::Count }];
    let dir = tempfile::tempdir().unwrap();
    let mut builder = ProgramBuilder::new();
    let out_schema = push_reduce(&mut builder, &aggs, &[], in_schema, true, false);
    builder.push_table(owned_table(dir.path(), "unowned_tr", out_schema));
    let reg_meta = vec![
        RegisterMeta::delta(in_schema),
        RegisterMeta::trace(out_schema, TableIdx(0)),
        RegisterMeta::delta(out_schema),
    ];
    let mut vm = builder.build(reg_meta, DeltaReg(2));
    assert!(!vm.pending_ground_row);
    assert!(execute_epoch(&mut vm, Batch::empty_with_schema(&in_schema), 0)
        .unwrap()
        .is_none());
}

/// Without a ground row an empty epoch produces nothing and runs no dispatch —
/// but it still clears the previous epoch's registers, which is the one thing
/// the skipped prologue would otherwise have done.
#[test]
fn an_empty_epoch_skips_the_pass_and_still_clears_the_registers() {
    let schema = make_schema_u128_i64();
    let mut builder = ProgramBuilder::new();
    // Reg 1 is the sink; reg 2 is written and never read, so nothing frees it
    // and it is what still holds rows when the epoch ends — the precondition the
    // empty epoch below has to clear.
    builder.push(Instr::WorkerFilter {
        in_reg: DeltaReg(0),
        out_reg: DeltaReg(1),
        worker_id: 0,
        num_workers: 1,
    });
    builder.push(Instr::Negate {
        in_reg: DeltaReg(0),
        out_reg: DeltaReg(2),
    });
    let mut vm = builder.build(vec![RegisterMeta::delta(schema); 3], DeltaReg(1));
    assert!(!vm.pending_ground_row);

    let out = execute_epoch(&mut vm, make_batch_u128(&schema, &[(1, 1, 10)]), 0)
        .unwrap()
        .expect("one row through");
    assert_eq!(out.len(), 1);
    assert_eq!(
        vm.regfile.batches[2].len(),
        1,
        "a register nothing reads keeps its batch"
    );

    assert!(execute_epoch(&mut vm, Batch::empty_with_schema(&schema), 0)
        .unwrap()
        .is_none());
    assert!(
        vm.regfile.batches.iter().all(|b| b.is_empty()),
        "the skipped epoch still released the previous one's batches",
    );
}
