//! Reduce operator tests. Imports submodule items by name; helpers live in this
//! file rather than a shared `common.rs` so the tests file stays self-contained.

use crate::foundation::codec::{read_i64_le, read_u64_le};
use crate::schema::{
    encode_german_string, type_code, SchemaColumn, SchemaDescriptor, TypeCode, PAYLOAD_MAPPING_PK_SENTINEL,
};
use crate::storage::{Batch, Layout};
use crate::test_support::make_schema_i64pk_i64;

use super::super::util::{extract_group_key, ieee_order_bits_f32, ieee_order_bits_f32_reverse};
use super::agg::{apply_agg_from_value_index, Accumulator, AggDescriptor, AggOp};
use super::emit::{emit_global_ground, emit_reduce_row};
use super::op_reduce::cursor_matches_group;
use super::sort::{argsort_delta, build_sort_descs, compare_by_group_cols};
use crate::storage::ReadCursor;

/// Shim over [`super::op_reduce::op_reduce`] deriving `out_key` from the input
/// schema — exactly the one kind compile-time validation admits for a given
/// (schema, group cols), so the many call sites below need not repeat it.
#[allow(clippy::too_many_arguments)]
fn op_reduce(
    delta: &Batch,
    trace_in_cursor: Option<&mut ReadCursor>,
    trace_out_cursor: &mut ReadCursor,
    input_schema: &SchemaDescriptor,
    output_schema: &SchemaDescriptor,
    group_by_cols: &[u32],
    agg_descs: &[AggDescriptor],
    avi_cursor: Option<&mut ReadCursor>,
    finalize_func: Option<&crate::expr::ScalarFunc>,
    finalize_out_schema: Option<&SchemaDescriptor>,
    global_ground: bool,
    i_am_owner: bool,
) -> (Batch, Option<Batch>) {
    super::op_reduce::op_reduce(
        delta,
        trace_in_cursor,
        trace_out_cursor,
        input_schema,
        output_schema,
        group_by_cols,
        agg_descs,
        input_schema.reduce_out_key(group_by_cols),
        avi_cursor,
        finalize_func,
        finalize_out_schema,
        global_ground,
        i_am_owner,
    )
}

/// Decode a single signed I64 PK column from its OPK (big-endian, sign-flipped)
/// bytes back to the native value — the inverse of `extend_pk_opk` for an I64 PK.
#[allow(dead_code)]
fn opk_pk_i64(opk_bytes: &[u8]) -> i64 {
    let mut le = [0u8; 8];
    gnitz_wire::decode_pk_column(&opk_bytes[..8], type_code::I64, &mut le);
    i64::from_le_bytes(le)
}

fn make_schema_u64_i64() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    )
}

fn make_batch(schema: &SchemaDescriptor, rows: &[(u64, i64, i64)]) -> Batch {
    let n = rows.len();
    let mut b = Batch::with_schema(*schema, n.max(1));

    for &(pk, w, val) in rows {
        b.extend_pk(pk as u128);
        b.extend_weight(&w.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &val.to_le_bytes());
        b.count += 1;
    }
    b.set_layout_unchecked(Layout::Consolidated);
    b
}

fn make_schema_u64_f32() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::F32, 0),
        ],
        &[0],
    )
}

fn make_batch_f32(schema: &SchemaDescriptor, rows: &[(u64, i64, f32)]) -> Batch {
    let n = rows.len();
    let mut b = Batch::with_schema(*schema, n.max(1));

    for &(pk, w, val) in rows {
        b.extend_pk(pk as u128);
        b.extend_weight(&w.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &val.to_bits().to_le_bytes());
        b.count += 1;
    }
    b.set_layout_unchecked(Layout::Consolidated);
    b
}

#[test]
fn test_reduce_sum_retraction() {
    use crate::schema::{type_code, SchemaColumn};
    use crate::storage::ReadCursor;
    use std::rc::Rc;

    // Input: pk(U64), grp(I64), val(I64)
    let in_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    );

    // Output: pk(U128), grp(I64), sum(I64), count(I64). The trailing count is the
    // cardinality companion every all-linear reduce now carries; op_reduce gates
    // emission on it (and asserts its presence).
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::I64, 1),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    );

    // Empty trace_out
    let empty_out = Rc::new(Batch::empty_with_schema(&out_schema));
    let mut to_ch = ReadCursor::from_owned(&[empty_out], out_schema);

    // Tick 1: insert 3 rows in group 10: val=100, val=200, val=300
    let delta1 = {
        let mut b = Batch::with_schema(in_schema, 3);
        for (pk, val) in [(1u64, 100i64), (2, 200), (3, 300)] {
            b.extend_pk(pk as u128);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &10i64.to_le_bytes()); // grp=10
            b.extend_col(1, &val.to_le_bytes());
            b.count += 1;
        }
        b.set_layout_unchecked(Layout::Consolidated);
        b
    };

    let aggs = sum_count_aggs(2, TypeCode::I64);

    let (out1, _) = op_reduce(
        &delta1,
        None,
        &mut to_ch,
        &in_schema,
        &out_schema,
        &[1u32],
        &aggs,
        None,
        None,
        None,
        false,
        false,
    );
    // SUM of (100+200+300) = 600
    assert_eq!(out1.count, 1);
    let sum1 = read_i64_le(out1.col_data(1), 0);
    assert_eq!(sum1, 600);

    // Tick 2: retract pk=2 (val=200) → SUM should go from 600 to 400
    // Need trace_out with previous aggregate
    let prev_out = Rc::new(out1);
    let mut to_ch2 = ReadCursor::from_owned(&[prev_out], out_schema);

    let delta2 = {
        let mut b = Batch::with_schema(in_schema, 1);
        b.extend_pk(2u128);
        b.extend_weight(&(-1i64).to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &10i64.to_le_bytes());
        b.extend_col(1, &200i64.to_le_bytes());
        b.count += 1;
        b.set_layout_unchecked(Layout::Consolidated);
        b
    };

    let (out2, _) = op_reduce(
        &delta2,
        None,
        &mut to_ch2,
        &in_schema,
        &out_schema,
        &[1u32],
        &aggs,
        None,
        None,
        None,
        false,
        false,
    );
    // Output: retract old sum (600, w=-1) + insert new sum (400, w=+1) = 2 rows.
    // The group survives (cardinality 3 → 2), so the +1 is emitted.
    assert_eq!(out2.count, 2);
}

/// All-linear gate: a non-nullable SUM-only group driven to empty must emit only
/// the −1 retraction of its stored row — no +1 zombie carrying SUM=0. The reduce
/// carries the appended Count cardinality companion; once the group's net
/// cardinality reaches 0 the gate suppresses the new row, so the group vanishes
/// exactly as SQL (and the Z-set model) require.
#[test]
fn linear_sum_only_emptied_group_eliminated() {
    use crate::schema::{type_code, SchemaColumn};
    use crate::storage::ReadCursor;
    use std::rc::Rc;

    // Input: pk(U64), grp(I64), val(I64). Output (synthetic GROUP BY grp):
    // _group_pk(U128), grp(I64), sum(I64 nullable), count(I64 companion).
    let in_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    );
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::I64, 1),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    );
    let aggs = sum_count_aggs(2, TypeCode::I64);

    let reduce = |delta: &Batch, to: &mut crate::storage::ReadCursor| {
        op_reduce(
            delta,
            None,
            to,
            &in_schema,
            &out_schema,
            &[1u32],
            &aggs,
            None,
            None,
            None,
            false,
            false,
        )
        .0
    };

    // Tick 1: insert (pk1, grp=10, val=5) → group exists (sum=5, count=1).
    let empty_out = Rc::new(Batch::empty_with_schema(&out_schema));
    let mut to_ch = ReadCursor::from_owned(&[empty_out], out_schema);
    let row = |pk: u128, w: i64| {
        let mut b = Batch::with_schema(in_schema, 1);
        b.extend_pk(pk);
        b.extend_weight(&w.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &10i64.to_le_bytes()); // grp=10
        b.extend_col(1, &5i64.to_le_bytes()); // val=5
        b.count += 1;
        b.set_layout_unchecked(Layout::Consolidated);
        b
    };
    let out1 = reduce(&row(1, 1), &mut to_ch);
    assert_eq!(out1.count, 1, "group present after first insert");

    // Tick 2: retract the only row → cardinality 1 → 0. The gate suppresses the
    // +1, leaving just the −1 retraction, so the group disappears (no zombie).
    let prev = Rc::new(out1);
    let mut to_ch2 = ReadCursor::from_owned(&[prev], out_schema);
    let out2 = reduce(&row(1, -1), &mut to_ch2);
    assert_eq!(out2.count, 1, "emptied group emits only the retraction, no +1 zombie");
    assert_eq!(out2.get_weight(0), -1, "the sole output row is the −1 retraction");
    assert_eq!(
        read_i64_le(out2.col_data(1), 0),
        5,
        "retraction re-emits the stored SUM=5"
    );
}

/// All-linear gate: inserting the *first* row of a new group whose aggregated
/// column is NULL must surface the group (SQL: `SUM = NULL`), not drop it. With
/// the appended Count companion the null-blind row count is 1, so the gate emits
/// the group; the SUM/CountNonNull accumulators stay untouched, so the raw SUM
/// bit is NULL and the NullfillSum finalize renders SUM = NULL.
#[test]
fn linear_sum_only_new_all_null_group_present() {
    use crate::expr::{CmpOp, LogicalInstr, LogicalProgram};
    use crate::schema::{type_code, SchemaColumn};
    use crate::storage::ReadCursor;
    use std::rc::Rc;

    // Input: pk(U64), grp(I64), val(I64 nullable).
    let in_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::I64, 1),
        ],
        &[0],
    );
    // Raw reduce output: _group_pk | grp | sum (nullable) | cnn | count.
    // agg order mirrors agg_descs = [Sum, CountNonNull, Count] — the nullable-SUM
    // NullfillSum pair plus the trailing appended cardinality companion.
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0), // _group_pk
            SchemaColumn::new(type_code::I64, 0),  // grp
            SchemaColumn::new(type_code::I64, 1),  // sum (nullable)
            SchemaColumn::new(type_code::I64, 0),  // cnn
            SchemaColumn::new(type_code::I64, 0),  // count (companion)
        ],
        &[0],
    );
    // Finalize projects [pk, grp, sum]; cnn and the count companion are stripped.
    let fin_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::I64, 1), // sum (nullable)
        ],
        &[0],
    );
    // NullfillSum: grp → fin payload 0; sum(col 2) / (cnn(col 3) != 0) → fin
    // payload 1 (div-by-zero marks SUM NULL when the non-null count is 0).
    let i64_tc = type_code::I64;
    let instrs = vec![
        LogicalInstr::CopyCol {
            src_col: 1,
            out: 0,
            tc: i64_tc,
        },
        LogicalInstr::LoadColInt { dst: 0, col: 3 }, // r0 = cnn
        LogicalInstr::LoadConst { dst: 1, val: 0 },
        LogicalInstr::Cmp {
            op: CmpOp::Ne,
            dst: 2,
            a: 0,
            b: 1,
        }, // r2 = (cnn != 0)
        LogicalInstr::LoadColInt { dst: 3, col: 2 }, // r3 = sum
        LogicalInstr::IntDiv { dst: 4, a: 3, b: 2 }, // r4 = sum / gate
        LogicalInstr::Emit { src: 4, out: 1 },       // emit r4 → fin payload 1 (sum)
    ];
    let fin_func =
        crate::expr::ScalarFunc::from_map(LogicalProgram::new(instrs, 5, 0, vec![]), &out_schema, &fin_schema);

    let aggs = [
        AggDescriptor {
            col_idx: 2,
            agg_op: AggOp::Sum,
            col_type_code: TypeCode::I64,
            _pad: [0; 2],
        },
        AggDescriptor {
            col_idx: 2,
            agg_op: AggOp::CountNonNull,
            col_type_code: TypeCode::I64,
            _pad: [0; 2],
        },
        AggDescriptor {
            col_idx: 0,
            agg_op: AggOp::Count,
            col_type_code: TypeCode::I64,
            _pad: [0; 2],
        },
    ];

    // New group g=7 whose only row has val=NULL.
    let empty_out = Rc::new(Batch::empty_with_schema(&out_schema));
    let mut to_ch = ReadCursor::from_owned(&[empty_out], out_schema);
    let delta = {
        let mut b = Batch::with_schema(in_schema, 1);
        b.extend_pk(1u128);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&(1u64 << 1).to_le_bytes()); // val (payload idx 1) NULL
        b.extend_col(0, &7i64.to_le_bytes()); // grp=7
        b.extend_col(1, &0i64.to_le_bytes()); // val NULL (placeholder)
        b.count += 1;
        b.set_layout_unchecked(Layout::Consolidated);
        b
    };
    let (raw, fin) = op_reduce(
        &delta,
        None,
        &mut to_ch,
        &in_schema,
        &out_schema,
        &[1u32],
        &aggs,
        None,
        Some(&fin_func),
        Some(&fin_schema),
        false,
        false,
    );
    let fin = fin.expect("finalize output present");
    assert_eq!(
        raw.count, 1,
        "new all-NULL group is present (cardinality 1), not dropped"
    );
    assert_eq!(fin.count, 1);
    assert_eq!(fin.get_weight(0), 1, "one +1 row");
    assert_eq!(read_i64_le(fin.col_data(0), 0), 7, "grp=7");
    assert_eq!(
        (fin.get_null_word(0) >> 1) & 1,
        1,
        "SUM of an all-NULL group renders as NULL",
    );
}

/// All-linear gate, reuse path: a COUNT(*)-only group driven to empty is
/// eliminated. The user COUNT(*) *is* the cardinality signal (no companion is
/// appended), so the gate suppresses the +1 once the count nets to 0.
#[test]
fn count_star_only_emptied_group_eliminated() {
    use crate::schema::{type_code, SchemaColumn};
    use crate::storage::ReadCursor;
    use std::rc::Rc;

    // Input: pk(U64), grp(I64). Output (synthetic GROUP BY grp):
    // _group_pk(U128), grp(I64), count(I64).
    let in_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    );
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    );
    let aggs = [AggDescriptor {
        col_idx: 0,
        agg_op: AggOp::Count,
        col_type_code: TypeCode::I64,
        _pad: [0; 2],
    }];

    let reduce = |delta: &Batch, to: &mut crate::storage::ReadCursor| {
        op_reduce(
            delta,
            None,
            to,
            &in_schema,
            &out_schema,
            &[1u32],
            &aggs,
            None,
            None,
            None,
            false,
            false,
        )
        .0
    };

    let row = |pk: u128, w: i64| {
        let mut b = Batch::with_schema(in_schema, 1);
        b.extend_pk(pk);
        b.extend_weight(&w.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &10i64.to_le_bytes()); // grp=10
        b.count += 1;
        b.set_layout_unchecked(Layout::Consolidated);
        b
    };

    // Tick 1: insert one row → count=1.
    let empty_out = Rc::new(Batch::empty_with_schema(&out_schema));
    let mut to_ch = ReadCursor::from_owned(&[empty_out], out_schema);
    let out1 = reduce(&row(1, 1), &mut to_ch);
    assert_eq!(out1.count, 1);
    assert_eq!(read_i64_le(out1.col_data(1), 0), 1, "count=1");

    // Tick 2: retract it → count 1 → 0 → group eliminated (only the −1 retraction).
    let prev = Rc::new(out1);
    let mut to_ch2 = ReadCursor::from_owned(&[prev], out_schema);
    let out2 = reduce(&row(1, -1), &mut to_ch2);
    assert_eq!(out2.count, 1, "COUNT(*)=0 group is eliminated, not kept as a zombie");
    assert_eq!(out2.get_weight(0), -1, "the sole output row is the −1 retraction");
}

/// Nullable SUM must transition to NULL when a retraction removes the last
/// non-null contributor from a still-surviving group. Drives `op_reduce` on the
/// linear fold (Count + Sum + CountNonNull) with the finalize program the SQL
/// planner builds for a nullable SUM: the SUM is null-gated by the hidden
/// COUNT_NON_NULL companion (`sum / (cnt != 0)` → NULL when the count is zero,
/// `sum` when it is positive). Pins the mechanism without the planner — a bare
/// `[Count, Sum]` reduce has no extra column to carry the non-null count, which
/// is exactly the defect the companion fixes.
#[test]
fn test_reduce_nullable_sum_retraction_becomes_null() {
    use crate::expr::{CmpOp, LogicalInstr, LogicalProgram};
    use crate::schema::{type_code, SchemaColumn};
    use crate::storage::ReadCursor;
    use std::rc::Rc;

    // Input: pk(U64), grp(I64), val(I64, NULLABLE).
    let in_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::I64, 1), // nullable source
        ],
        &[0],
    );

    // Raw reduce output: synthetic U128 _group_pk | grp | count | sum | cnn.
    // The aggregate column order mirrors agg_descs = [Count, Sum, CountNonNull].
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0), // _group_pk
            SchemaColumn::new(type_code::I64, 0),  // grp
            SchemaColumn::new(type_code::I64, 0),  // count
            SchemaColumn::new(type_code::I64, 1),  // sum (nullable)
            SchemaColumn::new(type_code::I64, 0),  // cnn (companion)
        ],
        &[0],
    );

    // Finalized output projects [pk, grp, count, sum] — companion stripped.
    let fin_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0), // _group_pk
            SchemaColumn::new(type_code::I64, 0),  // grp
            SchemaColumn::new(type_code::I64, 0),  // count
            SchemaColumn::new(type_code::I64, 1),  // sum (nullable)
        ],
        &[0],
    );

    // Finalize program (full reduce-output column indices; resolved below):
    //   copy grp(col 1) → fin payload 0, count(col 2) → fin payload 1,
    //   emit sum-gate = sum(col 3) / (cnn(col 4) != 0) → fin payload 2.
    // div-by-zero (cnn == 0) marks the SUM NULL; div-by-1 (cnn > 0) is exact.
    let i64_tc = type_code::I64;
    let instrs = vec![
        LogicalInstr::CopyCol {
            src_col: 1,
            out: 0,
            tc: i64_tc,
        }, // grp   → fin payload 0
        LogicalInstr::CopyCol {
            src_col: 2,
            out: 1,
            tc: i64_tc,
        }, // count → fin payload 1
        LogicalInstr::LoadColInt { dst: 0, col: 4 }, // r0 = cnn (col 4)
        LogicalInstr::LoadConst { dst: 1, val: 0 },  // r1 = 0
        LogicalInstr::Cmp {
            op: CmpOp::Ne,
            dst: 2,
            a: 0,
            b: 1,
        }, // r2 = (cnn != 0) → 1/0
        LogicalInstr::LoadColInt { dst: 3, col: 3 }, // r3 = sum (col 3)
        LogicalInstr::IntDiv { dst: 4, a: 3, b: 2 }, // r4 = sum / gate (NULL when gate == 0)
        LogicalInstr::Emit { src: 4, out: 2 },       // emit r4 → fin payload 2 (sum)
    ];
    let fin_func =
        crate::expr::ScalarFunc::from_map(LogicalProgram::new(instrs, 5, 0, vec![]), &out_schema, &fin_schema);

    let aggs = [
        AggDescriptor {
            col_idx: 0,
            agg_op: AggOp::Count,
            col_type_code: TypeCode::I64,
            _pad: [0; 2],
        },
        AggDescriptor {
            col_idx: 2,
            agg_op: AggOp::Sum,
            col_type_code: TypeCode::I64,
            _pad: [0; 2],
        },
        AggDescriptor {
            col_idx: 2,
            agg_op: AggOp::CountNonNull,
            col_type_code: TypeCode::I64,
            _pad: [0; 2],
        },
    ];

    // Tick 1: insert (pk1, grp=10, val=5) and (pk2, grp=10, val=NULL).
    let empty_out = Rc::new(Batch::empty_with_schema(&out_schema));
    let mut to_ch = ReadCursor::from_owned(&[empty_out], out_schema);
    let delta1 = {
        let mut b = Batch::with_schema(in_schema, 2);
        b.extend_pk(1u128);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &10i64.to_le_bytes()); // grp
        b.extend_col(1, &5i64.to_le_bytes()); // val = 5
        b.count += 1;
        // val is payload col 1 → its null bit is bit 1.
        b.extend_pk(2u128);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&(1u64 << 1).to_le_bytes());
        b.extend_col(0, &10i64.to_le_bytes()); // grp
        b.extend_col(1, &0i64.to_le_bytes()); // val = NULL (placeholder bytes)
        b.count += 1;
        b.set_layout_unchecked(Layout::Consolidated);
        b
    };

    let (raw1, fin1) = op_reduce(
        &delta1,
        None,
        &mut to_ch,
        &in_schema,
        &out_schema,
        &[1u32],
        &aggs,
        None,
        Some(&fin_func),
        Some(&fin_schema),
        false,
        false,
    );
    let fin1 = fin1.expect("finalize output present");
    // One group: count=2, sum=5 (non-null while a contributor remains), cnn=1.
    assert_eq!(raw1.count, 1);
    assert_eq!(fin1.count, 1);
    assert_eq!(fin1.get_weight(0), 1);
    assert_eq!(read_i64_le(fin1.col_data(1), 0), 2, "count=2");
    assert_eq!(read_i64_le(fin1.col_data(2), 0), 5, "sum=5");
    assert_eq!(
        (fin1.get_null_word(0) >> 2) & 1,
        0,
        "sum non-null while a contributor remains"
    );

    // Tick 2: retract (pk1, val=5). The group survives via (pk2, NULL): count
    // drops to 1, the last non-null contributor is gone → SUM must become NULL.
    let prev_out = Rc::new(raw1);
    let mut to_ch2 = ReadCursor::from_owned(&[prev_out], out_schema);
    let delta2 = {
        let mut b = Batch::with_schema(in_schema, 1);
        b.extend_pk(1u128);
        b.extend_weight(&(-1i64).to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &10i64.to_le_bytes());
        b.extend_col(1, &5i64.to_le_bytes());
        b.count += 1;
        b.set_layout_unchecked(Layout::Consolidated);
        b
    };

    let (_raw2, fin2) = op_reduce(
        &delta2,
        None,
        &mut to_ch2,
        &in_schema,
        &out_schema,
        &[1u32],
        &aggs,
        None,
        Some(&fin_func),
        Some(&fin_schema),
        false,
        false,
    );
    let fin2 = fin2.expect("finalize output present");
    // Retract the old aggregate (w=-1) and insert the new one (w=+1).
    assert_eq!(fin2.count, 2);

    // The insert row (weight +1) carries the surviving group's new state.
    let insert_row = (0..fin2.count)
        .find(|&r| fin2.get_weight(r) == 1)
        .expect("an insert row");
    assert_eq!(
        read_i64_le(fin2.col_data(1), insert_row * 8),
        1,
        "COUNT(*) survives at 1",
    );
    assert_eq!(
        (fin2.get_null_word(insert_row) >> 2) & 1,
        1,
        "SUM becomes NULL once its last non-null contributor is retracted",
    );

    // The retraction row re-emits the prior (non-null) SUM, cancelling the old
    // output byte-for-byte — the fix introduces no ghost.
    let retract_row = (0..fin2.count)
        .find(|&r| fin2.get_weight(r) == -1)
        .expect("a retract row");
    assert_eq!(
        (fin2.get_null_word(retract_row) >> 2) & 1,
        0,
        "retracted SUM was non-null"
    );
    assert_eq!(read_i64_le(fin2.col_data(2), retract_row * 8), 5, "retracted SUM = 5",);
}

/// A group whose MIN is NULL (all contributors NULL) is kept alive by COUNT(*).
/// When a non-NULL row later joins it, the group's old (NULL-MIN) output row must
/// be retracted *as NULL* to cancel the tick-1 row byte-for-byte. MIN is a Direct
/// passthrough, so the raw null bit is user-visible — the primary user-facing bug.
#[test]
fn null_min_retraction_re_emits_null() {
    use crate::storage::ReadCursor;
    use std::rc::Rc;

    // in: pk(U64), grp(I64), val(I64 nullable); out (payload GROUP BY grp):
    // pk(U128), grp(I64), count(I64), min(I64 nullable). min is payload index 2.
    let in_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::I64, 1),
        ],
        &[0],
    );
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::I64, 1),
            SchemaColumn::new(type_code::I64, 1),
        ],
        &[0],
    );
    let aggs = [
        AggDescriptor {
            col_idx: 2,
            agg_op: AggOp::Count,
            col_type_code: TypeCode::I64,
            _pad: [0; 2],
        },
        AggDescriptor {
            col_idx: 2,
            agg_op: AggOp::Min,
            col_type_code: TypeCode::I64,
            _pad: [0; 2],
        },
    ];
    let min_null_bit = 1u64 << 2;

    // Tick 1: (pk=1, grp=10, val=NULL) → COUNT keeps the group alive, MIN=NULL.
    let empty_out = Rc::new(Batch::empty_with_schema(&out_schema));
    let mut to_ch = ReadCursor::from_owned(&[empty_out], out_schema);
    let delta1 = {
        let mut b = Batch::with_schema(in_schema, 1);
        b.extend_pk(1u128);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&(1u64 << 1).to_le_bytes()); // val (payload idx 1) NULL
        b.extend_col(0, &10i64.to_le_bytes());
        b.extend_col(1, &0i64.to_le_bytes());
        b.count += 1;
        b.set_layout_unchecked(Layout::Consolidated);
        b
    };
    let (out1, _) = op_reduce(
        &delta1,
        None,
        &mut to_ch,
        &in_schema,
        &out_schema,
        &[1u32],
        &aggs,
        None,
        None,
        None,
        false,
        false,
    );
    assert_eq!(out1.count, 1);
    assert!(
        out1.as_mem_batch().get_null_word(0) & min_null_bit != 0,
        "tick1 MIN must be NULL"
    );

    // Tick 2: (pk=2, grp=10, val=7) → MIN=7, retracts the NULL row.
    let prev = Rc::new(out1);
    let mut to_ch2 = ReadCursor::from_owned(&[prev], out_schema);
    let delta2 = {
        let mut b = Batch::with_schema(in_schema, 1);
        b.extend_pk(2u128);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &10i64.to_le_bytes());
        b.extend_col(1, &7i64.to_le_bytes());
        b.count += 1;
        b.set_layout_unchecked(Layout::Consolidated);
        b
    };
    let (out2, _) = op_reduce(
        &delta2,
        None,
        &mut to_ch2,
        &in_schema,
        &out_schema,
        &[1u32],
        &aggs,
        None,
        None,
        None,
        false,
        false,
    );
    let mb2 = out2.as_mem_batch();
    let retr = (0..out2.count)
        .find(|&i| out2.get_weight(i) < 0)
        .expect("retraction row");
    assert!(
        mb2.get_null_word(retr) & min_null_bit != 0,
        "retraction of an all-NULL MIN group must re-emit MIN as NULL to cancel \
         the tick-1 row (null_word={:#x})",
        mb2.get_null_word(retr)
    );
}

/// A linear SUM whose group stays all-NULL across a fold must keep the new
/// output row's raw SUM bit NULL: folding a previously-NULL SUM's zero bytes
/// would saturate `has_value` and decode NULL as 0. Pins the linear-fold null
/// gate that skips a NULL old aggregate. This is the raw SUM bit only — a
/// nullable SUM's user-visible null-ness rides the NullfillSum companion, which
/// `test_reduce_nullable_sum_retraction_becomes_null` covers.
#[test]
fn null_sum_fold_stays_null() {
    use crate::storage::ReadCursor;
    use std::rc::Rc;

    let in_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::I64, 1),
        ],
        &[0],
    );
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::I64, 1),
            SchemaColumn::new(type_code::I64, 1),
        ],
        &[0],
    );
    let aggs = [
        AggDescriptor {
            col_idx: 2,
            agg_op: AggOp::Count,
            col_type_code: TypeCode::I64,
            _pad: [0; 2],
        },
        AggDescriptor {
            col_idx: 2,
            agg_op: AggOp::Sum,
            col_type_code: TypeCode::I64,
            _pad: [0; 2],
        },
    ];
    let sum_null_bit = 1u64 << 2;
    let null_row = |pk: u128| {
        let mut b = Batch::with_schema(in_schema, 1);
        b.extend_pk(pk);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&(1u64 << 1).to_le_bytes()); // val NULL
        b.extend_col(0, &10i64.to_le_bytes());
        b.extend_col(1, &0i64.to_le_bytes());
        b.count += 1;
        b.set_layout_unchecked(Layout::Consolidated);
        b
    };

    let empty_out = Rc::new(Batch::empty_with_schema(&out_schema));
    let mut to_ch = ReadCursor::from_owned(&[empty_out], out_schema);
    let (out1, _) = op_reduce(
        &null_row(1),
        None,
        &mut to_ch,
        &in_schema,
        &out_schema,
        &[1u32],
        &aggs,
        None,
        None,
        None,
        false,
        false,
    );
    assert!(
        out1.as_mem_batch().get_null_word(0) & sum_null_bit != 0,
        "tick1 SUM NULL"
    );

    let prev = Rc::new(out1);
    let mut to_ch2 = ReadCursor::from_owned(&[prev], out_schema);
    let (out2, _) = op_reduce(
        &null_row(2),
        None,
        &mut to_ch2,
        &in_schema,
        &out_schema,
        &[1u32],
        &aggs,
        None,
        None,
        None,
        false,
        false,
    );
    let mb2 = out2.as_mem_batch();
    let new_row = (0..out2.count).find(|&i| out2.get_weight(i) > 0).expect("insert row");
    assert!(
        mb2.get_null_word(new_row) & sum_null_bit != 0,
        "SUM of a still-all-NULL group must stay NULL (null_word={:#x})",
        mb2.get_null_word(new_row)
    );
}

/// Reduce-of-trace over a wide PK (3×U64, stride 24, GROUP BY the full PK).
/// The retraction read seeks `trace_out` by the group's PK bytes; the u128
/// `seek` cannot carry a stride-24 key.
#[test]
fn reduce_trace_seek_wide_pk() {
    use crate::schema::{type_code, SchemaColumn};
    use crate::storage::ReadCursor;
    use crate::test_support::{opk_pk, wide_pk_3xu64_schema};
    use std::rc::Rc;

    // Wide PK: 3×U64 (stride 24) + I64 val. GROUP BY the full PK.
    let in_schema = wide_pk_3xu64_schema();
    // Output: natural wide PK (3×U64) + SUM(I64, nullable) + count companion.
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 1),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0, 1, 2],
    );
    assert!(in_schema.pk_stride() > 16, "test invariant: stride 24 is wide");

    let pk = |a: u64, b: u64, c: u64| opk_pk(&in_schema, &[a as u128, b as u128, c as u128]);

    let aggs = sum_count_aggs(3, TypeCode::I64);
    let group_by = [0u32, 1, 2];

    // Tick 1: two rows in one group (7,7,7) — val 100 and 200 → SUM = 300.
    let empty_out = Rc::new(Batch::empty_with_schema(&out_schema));
    let mut to_ch = ReadCursor::from_owned(&[empty_out], out_schema);
    let delta1 = {
        let mut b = Batch::with_schema(in_schema, 2);
        for val in [100i64, 200] {
            b.extend_pk_bytes(&pk(7, 7, 7));
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &val.to_le_bytes());
            b.count += 1;
        }
        b.set_layout_unchecked(Layout::Consolidated);
        b
    };
    let (out1, _) = op_reduce(
        &delta1,
        None,
        &mut to_ch,
        &in_schema,
        &out_schema,
        &group_by,
        &aggs,
        None,
        None,
        None,
        false,
        false,
    );
    assert_eq!(out1.count, 1, "one group");
    assert_eq!(out1.get_pk_bytes(0), &pk(7, 7, 7)[..]);
    assert_eq!(read_i64_le(out1.col_data(0), 0), 300);

    // Tick 2: retract the val=200 row. SUM 300 → 100; reads the prior aggregate
    // out of trace_out by PK bytes.
    let prev_out = Rc::new(out1);
    let mut to_ch2 = ReadCursor::from_owned(&[prev_out], out_schema);
    let delta2 = {
        let mut b = Batch::with_schema(in_schema, 1);
        b.extend_pk_bytes(&pk(7, 7, 7));
        b.extend_weight(&(-1i64).to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &200i64.to_le_bytes());
        b.count += 1;
        b.set_layout_unchecked(Layout::Consolidated);
        b
    };
    let (out2, _) = op_reduce(
        &delta2,
        None,
        &mut to_ch2,
        &in_schema,
        &out_schema,
        &group_by,
        &aggs,
        None,
        None,
        None,
        false,
        false,
    );
    // Retraction of old SUM (300, w=-1) then insert of new SUM (100, w=+1).
    assert_eq!(
        out2.count, 2,
        "wide-PK retraction must read trace_out and emit retract+insert"
    );
    assert_eq!(out2.get_weight(0), -1);
    assert_eq!(out2.get_pk_bytes(0), &pk(7, 7, 7)[..]);
    assert_eq!(read_i64_le(out2.col_data(0), 0), 300, "retracted old SUM");
    assert_eq!(out2.get_weight(1), 1);
    assert_eq!(read_i64_le(out2.col_data(0), 8), 100, "new SUM");
}

/// Incremental REDUCE over a narrow COMPOUND PK (2×U64, stride 16), GROUP BY
/// the full PK, SUM. The tick-2 retraction seeks `trace_out` by the group's
/// PK. A compound key's raw-u128 order is last-column-major, so the seek must
/// go by bytes (storage order) to land on the group and retract the old SUM.
#[test]
fn reduce_trace_seek_compound_pk() {
    use crate::schema::{type_code, SchemaColumn};
    use crate::storage::ReadCursor;
    use crate::test_support::opk_pk;
    use std::rc::Rc;

    let in_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0, 1],
    );
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 1),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0, 1],
    );
    assert!(in_schema.pk_indices().len() > 1, "test invariant: compound PK");
    assert!(in_schema.pk_stride() <= 16, "test invariant: stride 16 is narrow");

    let pk = |a: u64, b: u64| opk_pk(&in_schema, &[a as u128, b as u128]);
    let aggs = sum_count_aggs(2, TypeCode::I64);
    let group_by = [0u32, 1];

    // Tick 1: insert (1,5)->100 and (2,3)->200 (two distinct groups).
    let empty_out = Rc::new(Batch::empty_with_schema(&out_schema));
    let mut to_ch = ReadCursor::from_owned(&[empty_out], out_schema);
    let delta1 = {
        let mut b = Batch::with_schema(in_schema, 2);
        for &(a, c, val) in &[(1u64, 5u64, 100i64), (2, 3, 200)] {
            b.extend_pk_bytes(&pk(a, c));
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &val.to_le_bytes());
            b.count += 1;
        }
        b.set_layout_unchecked(Layout::Consolidated);
        b
    };
    let (out1, _) = op_reduce(
        &delta1,
        None,
        &mut to_ch,
        &in_schema,
        &out_schema,
        &group_by,
        &aggs,
        None,
        None,
        None,
        false,
        false,
    );
    assert_eq!(out1.count, 2, "two groups");
    assert_eq!(out1.get_pk_bytes(0), &pk(1, 5)[..]);
    assert_eq!(read_i64_le(out1.col_data(0), 0), 100);
    assert_eq!(out1.get_pk_bytes(1), &pk(2, 3)[..]);
    assert_eq!(read_i64_le(out1.col_data(0), 8), 200);

    // Tick 2: insert (2,3)->50. SUM for (2,3) goes 200 → 250: retract 200,
    // insert 250. Group (1,5) is untouched.
    let prev_out = Rc::new(out1);
    let mut to_ch2 = ReadCursor::from_owned(&[prev_out], out_schema);
    let delta2 = {
        let mut b = Batch::with_schema(in_schema, 1);
        b.extend_pk_bytes(&pk(2, 3));
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &50i64.to_le_bytes());
        b.count += 1;
        b.set_layout_unchecked(Layout::Consolidated);
        b
    };
    let (out2, _) = op_reduce(
        &delta2,
        None,
        &mut to_ch2,
        &in_schema,
        &out_schema,
        &group_by,
        &aggs,
        None,
        None,
        None,
        false,
        false,
    );
    assert_eq!(
        out2.count, 2,
        "compound-PK retraction must read trace_out and emit retract+insert"
    );
    assert_eq!(out2.get_pk_bytes(0), &pk(2, 3)[..]);
    assert_eq!(out2.get_weight(0), -1);
    assert_eq!(read_i64_le(out2.col_data(0), 0), 200, "retracted old SUM");
    assert_eq!(out2.get_weight(1), 1);
    assert_eq!(read_i64_le(out2.col_data(0), 8), 250, "new SUM");
}

/// Incremental REDUCE over a narrow SIGNED single-column PK (I64), GROUP BY the
/// full PK, SUM. The tick-2 retraction seek to a negative key used to
/// mislocate (signed zero-extends → negatives sort after positives in raw
/// u128), dropping the retraction.
#[test]
fn reduce_trace_seek_signed_pk() {
    use crate::schema::{type_code, SchemaColumn};
    use crate::storage::ReadCursor;
    use std::rc::Rc;

    let in_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    );
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::I64, 1),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    );
    assert!(in_schema.pk_is_signed_single_col(), "test invariant: single signed PK");

    let aggs = sum_count_aggs(1, TypeCode::I64);
    let group_by = [0u32];

    // Tick 1: insert key=-1 -> 200, key=2 -> 100.
    let empty_out = Rc::new(Batch::empty_with_schema(&out_schema));
    let mut to_ch = ReadCursor::from_owned(&[empty_out], out_schema);
    let delta1 = {
        let mut b = Batch::with_schema(in_schema, 2);
        for &(k, val) in &[(-1i64, 200i64), (2, 100)] {
            b.extend_pk_opk(&in_schema, &[(k as u64) as u128]);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &val.to_le_bytes());
            b.count += 1;
        }
        b.set_layout_unchecked(Layout::Consolidated);
        b
    };
    let (out1, _) = op_reduce(
        &delta1,
        None,
        &mut to_ch,
        &in_schema,
        &out_schema,
        &group_by,
        &aggs,
        None,
        None,
        None,
        false,
        false,
    );
    assert_eq!(out1.count, 2, "two groups (-1 sorts before 2)");
    assert_eq!(opk_pk_i64(out1.get_pk_bytes(0)), -1);
    assert_eq!(read_i64_le(out1.col_data(0), 0), 200);

    // Tick 2: insert key=-1 -> 50. SUM goes 200 → 250: retract + insert.
    let prev_out = Rc::new(out1);
    let mut to_ch2 = ReadCursor::from_owned(&[prev_out], out_schema);
    let delta2 = {
        let mut b = Batch::with_schema(in_schema, 1);
        b.extend_pk_opk(&in_schema, &[((-1i64) as u64) as u128]);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &50i64.to_le_bytes());
        b.count += 1;
        b.set_layout_unchecked(Layout::Consolidated);
        b
    };
    let (out2, _) = op_reduce(
        &delta2,
        None,
        &mut to_ch2,
        &in_schema,
        &out_schema,
        &group_by,
        &aggs,
        None,
        None,
        None,
        false,
        false,
    );
    assert_eq!(
        out2.count, 2,
        "signed-PK retraction must read trace_out and emit retract+insert"
    );
    assert_eq!(opk_pk_i64(out2.get_pk_bytes(0)), -1);
    assert_eq!(out2.get_weight(0), -1);
    assert_eq!(read_i64_le(out2.col_data(0), 0), 200, "retracted old SUM");
    assert_eq!(out2.get_weight(1), 1);
    assert_eq!(read_i64_le(out2.col_data(0), 8), 250, "new SUM");
}

#[test]
fn test_reduce_count() {
    use crate::schema::type_code;
    use crate::storage::ReadCursor;
    use std::rc::Rc;

    // Input: pk(U64), val(I64)
    let in_schema = make_schema_u64_i64();

    // Output: pk(U128), count(I64)
    let out_schema = SchemaDescriptor::new(
        &[
            crate::schema::SchemaColumn::new(type_code::U128, 0),
            crate::schema::SchemaColumn::new(type_code::I64, 1),
        ],
        &[0],
    );

    let empty_out = Rc::new(Batch::empty_with_schema(&out_schema));
    let mut to_ch = ReadCursor::from_owned(&[empty_out], out_schema);

    // 3 rows: pk=1,2,3 all GROUP BY pk (single group using pk as group)
    let delta = make_batch(&in_schema, &[(1, 1, 10), (2, 1, 20), (3, 1, 30)]);

    let agg = AggDescriptor {
        col_idx: 0,
        agg_op: AggOp::Count,
        col_type_code: TypeCode::I64,
        _pad: [0; 2],
    };

    // GROUP BY pk → each row is its own group
    let (out, _) = op_reduce(
        &delta,
        None,
        &mut to_ch,
        &in_schema,
        &out_schema,
        &[0u32],
        &[agg],
        None,
        None,
        None,
        false,
        false,
    );
    // Each pk forms its own group, COUNT=1 for each
    assert_eq!(out.count, 3);
    for i in 0..3 {
        let count = read_i64_le(out.col_data(0), i * 8);
        assert_eq!(count, 1, "each single-row group has count=1");
    }
}

#[test]
fn test_argsort_delta_f32_group() {
    let schema = make_schema_u64_f32();
    let batch = make_batch_f32(&schema, &[(1, 1, 2.0f32), (2, 1, -1.0f32), (3, 1, 0.5f32)]);
    let indices = argsort_delta(&batch, &schema, &[1]);
    // Sorted order by F32: -1.0 < 0.5 < 2.0
    assert_eq!(indices.len(), 3);
    let mb = batch.as_mem_batch();
    let vals: Vec<f32> = indices
        .iter()
        .map(|&i| {
            let ptr = mb.get_col_ptr(i as usize, 0, 4);
            f32::from_bits(u32::from_le_bytes(ptr.try_into().unwrap()))
        })
        .collect();
    assert_eq!(vals, vec![-1.0f32, 0.5f32, 2.0f32]);
}

#[test]
fn test_compare_by_group_cols_f32_negative() {
    let schema = make_schema_u64_f32();
    let batch = make_batch_f32(&schema, &[(1, 1, -5.0f32), (2, 1, 3.0f32)]);
    let mb = batch.as_mem_batch();
    let (descs_arr, descs_len) = build_sort_descs(&schema, &[1]);
    let descs = &descs_arr[..descs_len];
    let ord = compare_by_group_cols(&mb, 0, 1, descs);
    assert_eq!(ord, std::cmp::Ordering::Less);
    let ord2 = compare_by_group_cols(&mb, 1, 0, descs);
    assert_eq!(ord2, std::cmp::Ordering::Greater);
}

#[test]
fn test_promote_agg_col_f32_ordering() {
    let schema = make_schema_u64_f32();
    let vals = [-2.0f32, -1.0f32, 0.0f32, 1.0f32, 2.0f32];
    let batch = make_batch_f32(
        &schema,
        &vals
            .iter()
            .enumerate()
            .map(|(i, &v)| (i as u64 + 1, 1, v))
            .collect::<Vec<_>>(),
    );
    let mb = batch.as_mem_batch();
    let encoded: Vec<u64> = (0..vals.len())
        .map(|row| {
            let pi = schema.try_payload_idx(1).unwrap(); // col_idx=1, pk_index=0
            let ptr = mb.get_col_ptr(row, pi, 4);
            let raw32 = u32::from_le_bytes(ptr.try_into().unwrap());
            // Order-preserving F32 encode (the encode_ordered F32 arm, for_max=false).
            ieee_order_bits_f32(raw32)
        })
        .collect();
    // Encoded values must be strictly ascending (order-preserving)
    for w in encoded.windows(2) {
        assert!(w[0] < w[1], "order-preserving invariant violated: {} >= {}", w[0], w[1]);
    }
    // Round-trip invariant
    for &v in &vals {
        let bits = v.to_bits();
        assert_eq!(
            ieee_order_bits_f32_reverse(ieee_order_bits_f32(bits)),
            bits,
            "round-trip failed for {v:?}"
        );
    }
}

#[test]
fn test_extract_group_key_f32() {
    let schema = make_schema_u64_f32();
    let batch = make_batch_f32(&schema, &[(1, 1, 1.5f32), (2, 1, 2.5f32)]);
    let mb = batch.as_mem_batch();
    let key0 = extract_group_key(&mb, 0, &schema, &[1]);
    let key1 = extract_group_key(&mb, 1, &schema, &[1]);
    assert_ne!(key0, key1, "different F32 values must produce different group keys");
}

// -----------------------------------------------------------------------
// Fix 1: Schema-agnostic reads for sub-8-byte columns
// -----------------------------------------------------------------------

fn make_schema_with_type(tc: u8) -> SchemaDescriptor {
    SchemaDescriptor::new(&[SchemaColumn::new(type_code::U64, 0), SchemaColumn::new(tc, 0)], &[0])
}

fn make_batch_typed_i32(schema: &SchemaDescriptor, rows: &[(u64, i64, i32)]) -> Batch {
    let n = rows.len();
    let mut b = Batch::with_schema(*schema, n.max(1));

    for &(pk, w, val) in rows {
        b.extend_pk(pk as u128);
        b.extend_weight(&w.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &val.to_le_bytes());
        b.count += 1;
    }
    b.set_layout_unchecked(Layout::Consolidated);
    b
}

fn make_batch_typed_i16(schema: &SchemaDescriptor, rows: &[(u64, i64, i16)]) -> Batch {
    let n = rows.len();
    let mut b = Batch::with_schema(*schema, n.max(1));

    for &(pk, w, val) in rows {
        b.extend_pk(pk as u128);
        b.extend_weight(&w.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &val.to_le_bytes());
        b.count += 1;
    }
    b.set_layout_unchecked(Layout::Consolidated);
    b
}

#[test]
fn test_reduce_sum_i32() {
    use crate::storage::ReadCursor;
    use std::rc::Rc;

    let in_schema = make_schema_with_type(type_code::I32);

    // Output: pk(U128), sum(I64), count(I64) — trailing count companion.
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::I64, 1),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    );

    let empty_out = Rc::new(Batch::empty_with_schema(&out_schema));
    let mut to_ch = ReadCursor::from_owned(&[empty_out], out_schema);

    // 3 rows with I32 values, group by PK
    let delta = make_batch_typed_i32(&in_schema, &[(1, 1, 100i32), (2, 1, 200i32), (3, 1, -50i32)]);

    let aggs = sum_count_aggs(1, TypeCode::I32);

    // GROUP BY pk → each row is its own group
    let (out, _) = op_reduce(
        &delta,
        None,
        &mut to_ch,
        &in_schema,
        &out_schema,
        &[0u32],
        &aggs,
        None,
        None,
        None,
        false,
        false,
    );
    assert_eq!(out.count, 3);
    // Check values: row offsets depend on PK order (group_by_pk path)
    let sum0 = read_i64_le(out.col_data(0), 0);
    let sum1 = read_i64_le(out.col_data(0), 8);
    let sum2 = read_i64_le(out.col_data(0), 16);
    assert_eq!(sum0, 100, "SUM of I32 100");
    assert_eq!(sum1, 200, "SUM of I32 200");
    assert_eq!(sum2, -50, "SUM of I32 -50 (sign extension)");
}

#[test]
fn test_reduce_min_f32() {
    use crate::storage::ReadCursor;
    use std::rc::Rc;

    let in_schema = make_schema_with_type(type_code::F32);

    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::I64, 1),
        ],
        &[0],
    );

    let empty_out = Rc::new(Batch::empty_with_schema(&out_schema));
    let mut to_ch = ReadCursor::from_owned(&[empty_out], out_schema);

    // Use a 2-col input schema: pk(U64), val(F32), GROUP BY pk. Rows in
    // (PK, payload) order so the consolidated flag the helper stamps is honest.
    let delta = make_batch_f32(&in_schema, &[(1, 1, -1.0f32), (1, 1, 3.5f32), (1, 1, 7.0f32)]);

    let agg = AggDescriptor {
        col_idx: 1,
        agg_op: AggOp::Min,
        col_type_code: TypeCode::F32,
        _pad: [0; 2],
    };

    // GROUP BY pk → all 3 rows in same group
    let (out, _) = op_reduce(
        &delta,
        None,
        &mut to_ch,
        &in_schema,
        &out_schema,
        &[0u32],
        &[agg],
        None,
        None,
        None,
        false,
        false,
    );
    assert_eq!(out.count, 1);
    // MIN should be -1.0 stored as f64 bits
    let bits = u64::from_le_bytes(out.col_data(0)[0..8].try_into().unwrap());
    let min_val = f64::from_bits(bits);
    assert_eq!(min_val, -1.0f64, "MIN of F32 {{3.5, -1.0, 7.0}} should be -1.0");
}

#[test]
fn test_reduce_max_i16() {
    use crate::storage::ReadCursor;
    use std::rc::Rc;

    let in_schema = make_schema_with_type(type_code::I16);

    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::I64, 1),
        ],
        &[0],
    );

    let empty_out = Rc::new(Batch::empty_with_schema(&out_schema));
    let mut to_ch = ReadCursor::from_owned(&[empty_out], out_schema);

    // 3 rows with I16 values, all same PK, in (PK, payload) order so the
    // consolidated flag the helper stamps is honest.
    let delta = make_batch_typed_i16(&in_schema, &[(1, 1, -100i16), (1, 1, 50i16), (1, 1, 200i16)]);

    let agg = AggDescriptor {
        col_idx: 1,
        agg_op: AggOp::Max,
        col_type_code: TypeCode::I16,
        _pad: [0; 2],
    };

    let (out, _) = op_reduce(
        &delta,
        None,
        &mut to_ch,
        &in_schema,
        &out_schema,
        &[0u32],
        &[agg],
        None,
        None,
        None,
        false,
        false,
    );
    assert_eq!(out.count, 1);
    let max_val = read_i64_le(out.col_data(0), 0);
    assert_eq!(max_val, 200, "MAX of I16 {{-100, 200, 50}} should be 200");
}

// -----------------------------------------------------------------------
// -----------------------------------------------------------------------
// UUID non-PK GROUP BY correctness
// -----------------------------------------------------------------------

/// Schema: pk(U64) + uuid_payload(UUID). UUID is at payload index 0.
fn make_schema_u64_pk_uuid_payload() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::UUID, 0),
        ],
        &[0],
    )
}

/// Schema: pk(U64) + uuid_col(UUID) + i64_col(I64).
fn make_schema_u64_uuid_i64() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::UUID, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    )
}

fn build_batch_u64_uuid(schema: &SchemaDescriptor, rows: &[(u64, u128)]) -> Batch {
    let mut b = Batch::with_schema(*schema, rows.len().max(1));
    for &(pk, uuid) in rows {
        b.extend_pk(pk as u128);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &uuid.to_le_bytes());
        b.count += 1;
    }
    b.set_layout_unchecked(Layout::Consolidated);
    b
}

fn build_batch_u64_uuid_i64(schema: &SchemaDescriptor, rows: &[(u64, u128, i64)]) -> Batch {
    let mut b = Batch::with_schema(*schema, rows.len().max(1));
    for &(pk, uuid, val) in rows {
        b.extend_pk(pk as u128);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &uuid.to_le_bytes());
        b.extend_col(1, &val.to_le_bytes());
        b.count += 1;
    }
    b.set_layout_unchecked(Layout::Consolidated);
    b
}

#[test]
fn test_compare_by_group_cols_uuid_non_pk() {
    // UUID non-PK column used as GROUP BY column. Before the fix, compare_by_group_cols
    // falls to the else branch with cs=16, panicking on a_buf[..16] (buf is [u8; 8]).
    let schema = make_schema_u64_pk_uuid_payload();
    let uuid_lo: u128 = 0x0000_0000_0000_0000_0000_0000_0000_0001u128;
    let uuid_hi: u128 = 0xFFFF_FFFF_FFFF_FFFF_FFFF_FFFF_FFFF_FFFFu128;
    let batch = build_batch_u64_uuid(&schema, &[(1, uuid_lo), (2, uuid_hi)]);
    let mb = batch.as_mem_batch();
    let (descs_arr, descs_len) = build_sort_descs(&schema, &[1]);
    let descs = &descs_arr[..descs_len];

    // uuid_lo < uuid_hi (compare by the 128-bit value)
    let ord = compare_by_group_cols(&mb, 0, 1, descs);
    assert_eq!(
        ord,
        std::cmp::Ordering::Less,
        "uuid_lo row must compare less than uuid_hi row"
    );

    let ord2 = compare_by_group_cols(&mb, 1, 0, descs);
    assert_eq!(
        ord2,
        std::cmp::Ordering::Greater,
        "uuid_hi row must compare greater than uuid_lo row"
    );

    let ord3 = compare_by_group_cols(&mb, 0, 0, descs);
    assert_eq!(ord3, std::cmp::Ordering::Equal, "same row must compare equal to itself");
}

#[test]
fn test_argsort_delta_uuid_group() {
    // argsort_delta with UUID group column: calls compare_by_group_cols.
    // Before fix: panics. After fix: rows sorted by UUID value.
    let schema = make_schema_u64_pk_uuid_payload();
    let uuid_a: u128 = 0x1000_0000_0000_0000_0000_0000_0000_0001u128;
    let uuid_b: u128 = 0x0000_0000_0000_0000_0000_0000_0000_0002u128;
    // uuid_b < uuid_a (lower high byte)
    let batch = build_batch_u64_uuid(&schema, &[(1, uuid_a), (2, uuid_b)]);
    let indices = argsort_delta(&batch, &schema, &[1]);
    assert_eq!(indices.len(), 2);
    // Row with uuid_b (row 1) should sort before row with uuid_a (row 0)
    assert_eq!(indices[0], 1, "uuid_b (smaller) must sort first");
    assert_eq!(indices[1], 0, "uuid_a (larger) must sort second");
}

#[test]
fn test_extract_group_key_uuid_multi_col() {
    // Multi-column GROUP BY that includes a UUID column. Before fix, extract_group_key's
    // hash loop uses a [u8; 8] buffer for UUID (cs=16), panicking on buf[..16].
    let schema = make_schema_u64_uuid_i64();
    let uuid_a: u128 = 0xAAAA_BBBB_CCCC_DDDD_EEEE_FFFF_0000_0001u128;
    let uuid_b: u128 = 0x1111_2222_3333_4444_5555_6666_7777_8888u128;
    let batch = build_batch_u64_uuid_i64(&schema, &[(1, uuid_a, 42i64), (2, uuid_b, 42i64), (3, uuid_a, 43i64)]);
    let mb = batch.as_mem_batch();

    // GROUP BY (uuid_col=1, i64_col=2)
    let key0 = extract_group_key(&mb, 0, &schema, &[1, 2]); // uuid_a, 42
    let key1 = extract_group_key(&mb, 1, &schema, &[1, 2]); // uuid_b, 42
    let key2 = extract_group_key(&mb, 2, &schema, &[1, 2]); // uuid_a, 43
    let key0b = extract_group_key(&mb, 0, &schema, &[1, 2]); // same as key0

    assert_ne!(key0, key1, "different UUIDs same int must yield different group keys");
    assert_ne!(key0, key2, "same UUID different int must yield different group keys");
    assert_ne!(
        key1, key2,
        "different UUID different int must yield different group keys"
    );
    assert_eq!(key0, key0b, "same inputs must yield the same group key");
}

// -----------------------------------------------------------------------
// GROUP BY containing the PK column (mixed pk/non-pk group_by_cols).
// -----------------------------------------------------------------------

/// Schema: U64 pk (col 0) | I64 other (col 1). pk_index = 0.
fn make_schema_pk0_u64_i64() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    )
}

/// Schema: I64 other (col 0) | U64 pk (col 1). pk_index = 1.
fn make_schema_pk1_i64_u64() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::U64, 0),
        ],
        &[1],
    )
}

/// Build a 2-col batch (pk, other) with explicit pk values and `other` payload.
/// Works for either pk_index=0 or pk_index=1 since extend_col(pi, ..) addresses
/// the dense payload region — the non-PK column always lives at payload index 0.
fn build_pk_other(schema: &SchemaDescriptor, rows: &[(u64, i64)]) -> Batch {
    let mut b = Batch::with_schema(*schema, rows.len().max(1));
    for &(pk, other) in rows {
        b.extend_pk(pk as u128);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &other.to_le_bytes());
        b.count += 1;
    }
    b.set_layout_unchecked(Layout::Consolidated);
    b
}

#[test]
fn test_extract_group_key_includes_pk_pki0() {
    // GROUP BY [pk, other] with pk_index=0: hash loop must dispatch via
    // is_pk_col, not call payload_idx(0, 0) and underflow.
    let schema = make_schema_pk0_u64_i64();
    let batch = build_pk_other(&schema, &[(10, 100), (20, 100), (10, 200)]);
    let mb = batch.as_mem_batch();

    let k_pk10_v100 = extract_group_key(&mb, 0, &schema, &[0, 1]);
    let k_pk20_v100 = extract_group_key(&mb, 1, &schema, &[0, 1]);
    let k_pk10_v200 = extract_group_key(&mb, 2, &schema, &[0, 1]);
    let k_pk10_v100_again = extract_group_key(&mb, 0, &schema, &[0, 1]);

    assert_ne!(k_pk10_v100, k_pk20_v100, "different PKs, same other → distinct keys");
    assert_ne!(k_pk10_v100, k_pk10_v200, "same PK, different other → distinct keys");
    assert_eq!(k_pk10_v100, k_pk10_v100_again, "same row → same key");
}

#[test]
fn test_extract_group_key_includes_pk_pki1() {
    // GROUP BY [other, pk] with pk_index=1: previously read the wrong
    // payload column when c_idx == pki for non-zero pk_index.
    let schema = make_schema_pk1_i64_u64();
    let batch = build_pk_other(&schema, &[(10, 100), (20, 100), (10, 200)]);
    let mb = batch.as_mem_batch();

    // group_by [col 0 = other, col 1 = pk]
    let k_pk10_v100 = extract_group_key(&mb, 0, &schema, &[0, 1]);
    let k_pk20_v100 = extract_group_key(&mb, 1, &schema, &[0, 1]);
    let k_pk10_v200 = extract_group_key(&mb, 2, &schema, &[0, 1]);

    assert_ne!(k_pk10_v100, k_pk20_v100);
    assert_ne!(k_pk10_v100, k_pk10_v200);
}

#[test]
fn test_compare_by_group_cols_includes_pk() {
    // Sort/compare path must dispatch on the PK sentinel rather than
    // dereferencing a fake pi for the PK column.
    let schema = make_schema_pk0_u64_i64();
    let batch = build_pk_other(&schema, &[(10, 100), (20, 100)]);
    let mb = batch.as_mem_batch();

    let (descs_arr, descs_len) = build_sort_descs(&schema, &[0, 1]);
    let descs = &descs_arr[..descs_len];
    // First desc covers PK — must use the sentinel.
    assert_eq!(descs[0].pi, PAYLOAD_MAPPING_PK_SENTINEL);

    assert_eq!(compare_by_group_cols(&mb, 0, 1, descs), std::cmp::Ordering::Less);
    assert_eq!(compare_by_group_cols(&mb, 1, 0, descs), std::cmp::Ordering::Greater);
    assert_eq!(compare_by_group_cols(&mb, 0, 0, descs), std::cmp::Ordering::Equal);
}

#[test]
fn test_argsort_delta_pk_in_group() {
    // Multi-column group containing PK must reach compare_by_group_cols
    // and use the sentinel branch — must not panic.
    let schema = make_schema_pk0_u64_i64();
    let batch = build_pk_other(&schema, &[(20, 100), (10, 200), (10, 100)]);
    let indices = argsort_delta(&batch, &schema, &[0, 1]);
    assert_eq!(indices.len(), 3);
    // Sorted by (pk, other): (10,100), (10,200), (20,100)
    let mb = batch.as_mem_batch();
    let pks: Vec<u64> = indices.iter().map(|&i| mb.get_pk(i as usize) as u64).collect();
    assert_eq!(pks, vec![10, 10, 20]);
    let others: Vec<i64> = indices
        .iter()
        .map(|&i| i64::from_le_bytes(mb.get_col_ptr(i as usize, 0, 8).try_into().unwrap()))
        .collect();
    assert_eq!(others, vec![100, 200, 100]);
}

// -----------------------------------------------------------------------
// NULL group columns must form a distinct group (not merged with 0).
// -----------------------------------------------------------------------

/// Schema: U64 pk | nullable I64.
fn make_schema_pk_nullable_i64() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 1),
        ],
        &[0],
    )
}

/// Build a 2-col batch (pk, nullable_i64). For null rows, payload bytes
/// are zero (DirectWriter convention) and the null bit at payload pi=0 is set.
fn build_pk_null_i64(schema: &SchemaDescriptor, rows: &[(u64, Option<i64>)]) -> Batch {
    let mut b = Batch::with_schema(*schema, rows.len().max(1));
    for &(pk, val) in rows {
        b.extend_pk(pk as u128);
        b.extend_weight(&1i64.to_le_bytes());
        let null_word: u64 = if val.is_none() { 1 } else { 0 };
        b.extend_null_bmp(&null_word.to_le_bytes());
        // Nulls store as zero bytes — same byte pattern as integer 0.
        b.extend_col(0, &val.unwrap_or(0).to_le_bytes());
        b.count += 1;
    }
    b.set_layout_unchecked(Layout::Consolidated);
    b
}

#[test]
fn test_extract_group_key_null_distinct_from_zero() {
    let schema = make_schema_pk_nullable_i64();
    let batch = build_pk_null_i64(&schema, &[(1, None), (2, Some(0)), (3, Some(7)), (4, None)]);
    let mb = batch.as_mem_batch();

    let k_null = extract_group_key(&mb, 0, &schema, &[1]);
    let k_zero = extract_group_key(&mb, 1, &schema, &[1]);
    let k_seven = extract_group_key(&mb, 2, &schema, &[1]);
    let k_null2 = extract_group_key(&mb, 3, &schema, &[1]);

    assert_ne!(k_null, k_zero, "NULL must form a distinct group from 0");
    assert_ne!(k_null, k_seven);
    assert_ne!(k_zero, k_seven);
    assert_eq!(k_null, k_null2, "two NULL rows must collapse into the same group");
}

#[test]
fn test_compare_by_group_cols_nulls_first() {
    let schema = make_schema_pk_nullable_i64();
    let batch = build_pk_null_i64(&schema, &[(1, Some(7)), (2, None), (3, None)]);
    let mb = batch.as_mem_batch();
    let (descs_arr, descs_len) = build_sort_descs(&schema, &[1]);
    let descs = &descs_arr[..descs_len];

    // NULL < 7 (NULLS FIRST)
    assert_eq!(compare_by_group_cols(&mb, 1, 0, descs), std::cmp::Ordering::Less);
    assert_eq!(compare_by_group_cols(&mb, 0, 1, descs), std::cmp::Ordering::Greater);
    // NULL == NULL → equal (same group)
    assert_eq!(compare_by_group_cols(&mb, 1, 2, descs), std::cmp::Ordering::Equal);
}

#[test]
fn test_argsort_delta_nullable_no_packed_sort() {
    // Nullable single-column group must skip the packed-sort fast path
    // (which sorts raw bytes and would interleave NULLs with 0s) and
    // route through compare_by_group_cols where NULL < non-NULL.
    let schema = make_schema_pk_nullable_i64();
    let batch = build_pk_null_i64(&schema, &[(1, Some(0)), (2, None), (3, Some(5)), (4, None)]);
    let indices = argsort_delta(&batch, &schema, &[1]);
    let mb = batch.as_mem_batch();
    // NULLs must be adjacent (single group), not interleaved with 0s.
    let null_word_at = |i: u32| mb.get_null_word(i as usize) & 1 != 0;
    let null_positions: Vec<usize> = indices
        .iter()
        .enumerate()
        .filter(|&(_, &i)| null_word_at(i))
        .map(|(p, _)| p)
        .collect();
    assert_eq!(null_positions.len(), 2, "expected 2 NULL rows");
    // NULLS FIRST: both nulls at positions 0 and 1.
    assert_eq!(null_positions, vec![0, 1]);
}

// -----------------------------------------------------------------------
// Compound-PK reduce: byte-form emit + Accumulator PK-column read + order
// -----------------------------------------------------------------------

/// 2×U64 compound-PK input schema. pk_indices = [0, 1]; payload col is I64.
fn make_compound_pk_2xu64_schema() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0, 1],
    )
}

/// Build a 2×U64 compound-PK batch. Rows: (pk0, pk1, weight, val).
fn make_batch_compound_2xu64(schema: &SchemaDescriptor, rows: &[(u64, u64, i64, i64)]) -> Batch {
    let n = rows.len();
    let mut b = Batch::with_schema(*schema, n.max(1));
    for &(pk0, pk1, w, val) in rows {
        let mut pk_bytes = [0u8; 16];
        // 2×U64 compound PK: both unsigned, OPK == BE per column.
        pk_bytes[0..8].copy_from_slice(&pk0.to_be_bytes());
        pk_bytes[8..16].copy_from_slice(&pk1.to_be_bytes());
        b.extend_pk_bytes(&pk_bytes);
        b.extend_weight(&w.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &val.to_le_bytes());
        b.count += 1;
    }
    b.set_layout_unchecked(Layout::Consolidated);
    b
}

/// emit_reduce_row natural-PK byte path on a 2×U64 compound PK: PK bytes
/// must be copied verbatim from the source row, not packed from group_key.
#[test]
fn test_emit_reduce_row_compound_pk_bytes() {
    let in_schema = make_compound_pk_2xu64_schema();

    // Output schema matches what build_reduce_output_schema would produce
    // for a PkPermutation grouping on this input with a COUNT aggregate:
    // 2 PK cols (U64,U64) followed by I64 count.
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0, 1],
    );

    let pk0: u64 = 0xAAAA_BBBB_CCCC_DDDDu64;
    let pk1: u64 = 0x1111_2222_3333_4444u64;
    let input = make_batch_compound_2xu64(&in_schema, &[(pk0, pk1, 1, 99)]);
    let mb = input.as_mem_batch();

    let mut output = Batch::with_schema(out_schema, 1);
    let agg = AggDescriptor {
        col_idx: 2,
        agg_op: AggOp::Count,
        col_type_code: TypeCode::I64,
        _pad: [0; 2],
    };
    let accs: Vec<Accumulator> = vec![Accumulator::new(&agg, &in_schema)];
    // Natural-PK grouping passes the source row's PK bytes; they're copied verbatim.
    emit_reduce_row(
        &mut output,
        &mb,
        0,
        mb.get_pk_bytes(0),
        &accs,
        &in_schema,
        &out_schema,
        &[0u32, 1u32],
        true, /* use_natural_pk */
        1,
    );

    assert_eq!(output.count, 1);
    // Source PK region is OPK (big-endian); the verbatim copy preserves it.
    let mut expected = [0u8; 16];
    expected[0..8].copy_from_slice(&pk0.to_be_bytes());
    expected[8..16].copy_from_slice(&pk1.to_be_bytes());
    assert_eq!(
        output.get_pk_bytes(0),
        &expected[..],
        "compound natural-PK output must copy source row's PK bytes verbatim"
    );
}

/// Accumulator MIN on the SECOND PK column of a 2×U64 compound PK.
/// Regression: the second PK column must be decoded by walking its
/// byte offset within the PK region, not by widening the whole
/// region to u128 (which would yield column 0).
#[test]
fn test_reduce_min_pk_col_compound_pk() {
    use crate::storage::ReadCursor;
    use std::rc::Rc;

    let in_schema = make_compound_pk_2xu64_schema();

    // Output: full natural compound PK + I64 agg, matching the
    // build_reduce_output_schema layout for PkPermutation.
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 1),
        ],
        &[0, 1],
    );

    let empty_out = Rc::new(Batch::empty_with_schema(&out_schema));
    let mut to_ch = ReadCursor::from_owned(&[empty_out], out_schema);

    // Two distinct compound PKs whose pk_col_0 and pk_col_1 disagree
    // about ordering: pk_col_1 values are 7 and 3 → MIN must be 3.
    let delta = make_batch_compound_2xu64(&in_schema, &[(10, 7, 1, 100), (20, 3, 1, 200)]);

    // MIN over the SECOND PK column (col_idx=1).
    let agg = AggDescriptor {
        col_idx: 1,
        agg_op: AggOp::Min,
        col_type_code: TypeCode::U64,
        _pad: [0; 2],
    };

    let (out, _) = op_reduce(
        &delta,
        None,
        &mut to_ch,
        &in_schema,
        &out_schema,
        &[0u32, 1u32],
        &[agg],
        None,
        None,
        None,
        false,
        false,
    );
    // group_by_pk: each (pk0, pk1) is its own group, so we get one row
    // per input row; MIN within each group equals the row's pk_col_1.
    assert_eq!(out.count, 2);
    let mins: Vec<i64> = (0..out.count).map(|i| read_i64_le(out.col_data(0), i * 8)).collect();
    // Output is in pk_indices order = [0, 1] ascending, so (10,7) precedes (20,3).
    assert_eq!(
        mins,
        vec![7, 3],
        "MIN(pk_col_1) per single-row group must equal that row's pk_col_1"
    );
}

/// Single-PK U64 MIN(pk_col) — sanity check that the byte-offset
/// PK-read path produces the same result as the prior u128 path.
#[test]
fn test_reduce_min_pk_col_single_pk_u64() {
    use crate::storage::ReadCursor;
    use std::rc::Rc;

    let in_schema = make_schema_u64_i64();
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 1),
        ],
        &[0],
    );

    let empty_out = Rc::new(Batch::empty_with_schema(&out_schema));
    let mut to_ch = ReadCursor::from_owned(&[empty_out], out_schema);

    // Input is PK-sorted from consolidation; the group_by_pk fast path
    // passes that order straight through.
    let delta = make_batch(&in_schema, &[(7, 1, 0), (42, 1, 0), (99, 1, 0)]);

    let agg = AggDescriptor {
        col_idx: 0,
        agg_op: AggOp::Min,
        col_type_code: TypeCode::U64,
        _pad: [0; 2],
    };
    let (out, _) = op_reduce(
        &delta,
        None,
        &mut to_ch,
        &in_schema,
        &out_schema,
        &[0u32],
        &[agg],
        None,
        None,
        None,
        false,
        false,
    );
    // GROUP BY pk → each row is its own group; MIN(pk) per group equals the row's pk.
    assert_eq!(out.count, 3);
    let mins: Vec<i64> = (0..out.count).map(|i| read_i64_le(out.col_data(0), i * 8)).collect();
    assert_eq!(mins, vec![7, 42, 99]);
}

/// Permuted group_by_cols on a compound PK must still emit rows in
/// pk_indices order (the input is PK-sorted from consolidation; the
/// fast path skips the sort and passes row order through).
#[test]
fn test_reduce_group_by_pk_permuted_preserves_pk_order() {
    use crate::storage::ReadCursor;
    use std::rc::Rc;

    let in_schema = make_compound_pk_2xu64_schema();
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 1),
        ],
        &[0, 1],
    );

    let empty_out = Rc::new(Batch::empty_with_schema(&out_schema));
    let mut to_ch = ReadCursor::from_owned(&[empty_out], out_schema);

    // Two PKs whose [0,1] and [1,0] orderings disagree: (1,2) vs (2,1).
    // PK-sorted (pk_indices=[0,1]) order: (1,2) then (2,1).
    let delta = make_batch_compound_2xu64(&in_schema, &[(1, 2, 1, 10), (2, 1, 1, 20)]);

    let agg = AggDescriptor {
        col_idx: 2,
        agg_op: AggOp::Count,
        col_type_code: TypeCode::I64,
        _pad: [0; 2],
    };

    // group_by_cols permuted to [1, 0] — a valid set permutation of pk_indices.
    let (out, _) = op_reduce(
        &delta,
        None,
        &mut to_ch,
        &in_schema,
        &out_schema,
        &[1u32, 0u32],
        &[agg],
        None,
        None,
        None,
        false,
        false,
    );

    assert_eq!(out.count, 2);
    let row0_pk = out.get_pk_bytes(0);
    let row1_pk = out.get_pk_bytes(1);
    let p0_col0 = u64::from_be_bytes(row0_pk[0..8].try_into().unwrap());
    let p0_col1 = u64::from_be_bytes(row0_pk[8..16].try_into().unwrap());
    let p1_col0 = u64::from_be_bytes(row1_pk[0..8].try_into().unwrap());
    let p1_col1 = u64::from_be_bytes(row1_pk[8..16].try_into().unwrap());
    assert_eq!(
        (p0_col0, p0_col1),
        (1, 2),
        "first emitted row must be (1, 2) in pk_indices order"
    );
    assert_eq!(
        (p1_col0, p1_col1),
        (2, 1),
        "second emitted row must be (2, 1) in pk_indices order"
    );
}

// -----------------------------------------------------------------------
// Compound-PK subset grouping: PK-region access must be per-PK-column
// (pre-fix the slow path widened the entire region and split groups
// that share the addressed PK column but differ in other PK columns).
// -----------------------------------------------------------------------

/// compare_by_group_cols on the PK-sentinel branch must compare only
/// the addressed PK column. Two rows that share `pk_col_0` but differ
/// in `pk_col_1` must compare Equal under `GROUP BY pk_col_0`.
#[test]
fn test_compare_by_group_cols_pk_sentinel_compound_subset() {
    let schema = make_compound_pk_2xu64_schema();
    let batch = make_batch_compound_2xu64(&schema, &[(10, 7, 1, 100), (10, 9, 1, 200), (20, 7, 1, 300)]);
    let mb = batch.as_mem_batch();

    let (descs_arr, descs_len) = build_sort_descs(&schema, &[0u32]);
    let descs = &descs_arr[..descs_len];
    assert_eq!(
        descs[0].pi, PAYLOAD_MAPPING_PK_SENTINEL,
        "subset group on PK col 0 must produce a PK-sentinel SortDesc"
    );
    assert_eq!(descs[0].pk_off, 0, "pk_col_0 byte offset within PK region");

    // Same pk_col_0 (10), different pk_col_1 → Equal under GROUP BY pk_col_0.
    assert_eq!(
        compare_by_group_cols(&mb, 0, 1, descs),
        std::cmp::Ordering::Equal,
        "rows with same pk_col_0 must form one group regardless of pk_col_1",
    );
    // Different pk_col_0 → ordering follows pk_col_0.
    assert_eq!(compare_by_group_cols(&mb, 0, 2, descs), std::cmp::Ordering::Less);
    assert_eq!(compare_by_group_cols(&mb, 2, 0, descs), std::cmp::Ordering::Greater);
}

/// compare_by_group_cols on PK-sentinel with `GROUP BY pk_col_1`
/// (non-zero PK byte offset) must isolate pk_col_1.
#[test]
fn test_compare_by_group_cols_pk_sentinel_compound_pk_col_1() {
    let schema = make_compound_pk_2xu64_schema();
    let batch = make_batch_compound_2xu64(&schema, &[(1, 50, 1, 100), (2, 50, 1, 200), (3, 60, 1, 300)]);
    let mb = batch.as_mem_batch();

    let (descs_arr, descs_len) = build_sort_descs(&schema, &[1u32]);
    let descs = &descs_arr[..descs_len];
    assert_eq!(descs[0].pi, PAYLOAD_MAPPING_PK_SENTINEL);
    assert_eq!(descs[0].pk_off, 8, "pk_col_1 byte offset within PK region");

    // Same pk_col_1 (50), different pk_col_0 → Equal.
    assert_eq!(compare_by_group_cols(&mb, 0, 1, descs), std::cmp::Ordering::Equal);
    // Different pk_col_1 → ordering follows pk_col_1.
    assert_eq!(compare_by_group_cols(&mb, 0, 2, descs), std::cmp::Ordering::Less);
}

/// Single-PK U64 with `GROUP BY pk` must be bit-identical to the prior
/// whole-region widen path — pk_off = 0, cs = pk_stride = 8.
#[test]
fn test_compare_by_group_cols_pk_sentinel_single_pk_bit_identical() {
    let schema = make_schema_u64_i64();
    let batch = build_pk_other(&schema, &[(10, 100), (20, 100), (10, 200)]);
    let mb = batch.as_mem_batch();

    let (descs_arr, descs_len) = build_sort_descs(&schema, &[0u32]);
    let descs = &descs_arr[..descs_len];
    assert_eq!(descs[0].pi, PAYLOAD_MAPPING_PK_SENTINEL);
    assert_eq!(descs[0].pk_off, 0);
    assert_eq!(descs[0].cs, 8);

    assert_eq!(compare_by_group_cols(&mb, 0, 1, descs), std::cmp::Ordering::Less);
    assert_eq!(compare_by_group_cols(&mb, 1, 0, descs), std::cmp::Ordering::Greater);
    assert_eq!(compare_by_group_cols(&mb, 0, 2, descs), std::cmp::Ordering::Equal);
}

/// cursor_matches_group on a PK-sentinel SortDesc with compound PK must
/// match rows that share the addressed PK column but differ elsewhere.
#[test]
fn test_cursor_matches_group_pk_sentinel_compound_subset() {
    use crate::storage::ReadCursor;
    use std::rc::Rc;

    let schema = make_compound_pk_2xu64_schema();
    // Cursor row: (pk0=10, pk1=99). Exemplar row: (pk0=10, pk1=42).
    // GROUP BY pk_col_0 → must match (both share pk0=10).
    let cursor_batch = Rc::new(make_batch_compound_2xu64(&schema, &[(10, 99, 1, 0)]));
    let exemplar_batch = make_batch_compound_2xu64(&schema, &[(10, 42, 1, 0), (20, 42, 1, 0)]);
    let exemplar_mb = exemplar_batch.as_mem_batch();

    let mut cursor_handle = ReadCursor::from_owned(&[cursor_batch], schema);
    let cursor = &mut cursor_handle;
    cursor.rewind();
    assert!(cursor.valid, "cursor must be positioned on the single row");

    let (descs_arr, descs_len) = build_sort_descs(&schema, &[0u32]);
    let descs = &descs_arr[..descs_len];

    // Row 0 (pk0=10) shares pk_col_0 with the cursor → match.
    assert!(
        cursor_matches_group(cursor, &exemplar_mb, 0, descs),
        "exemplar (10,42) and cursor (10,99) share pk_col_0=10"
    );
    // Row 1 (pk0=20) differs from the cursor's pk_col_0=10 → no match.
    assert!(
        !cursor_matches_group(cursor, &exemplar_mb, 1, descs),
        "exemplar (20,42) differs from cursor (10,99) in pk_col_0"
    );
}

/// extract_group_key on `GROUP BY pk_col_0` (single PK column of a
/// compound PK) must return the same u128 for two rows that share
/// pk_col_0 — distinct pk_col_1 values must not collide them into
/// different groups.
#[test]
fn test_extract_group_key_single_pk_col_compound_subset() {
    let schema = make_compound_pk_2xu64_schema();
    let batch = make_batch_compound_2xu64(&schema, &[(10, 50, 1, 0), (10, 99, 1, 0), (20, 50, 1, 0)]);
    let mb = batch.as_mem_batch();

    let k0 = extract_group_key(&mb, 0, &schema, &[0u32]);
    let k1 = extract_group_key(&mb, 1, &schema, &[0u32]);
    let k2 = extract_group_key(&mb, 2, &schema, &[0u32]);

    assert_eq!(k0, 10u128, "key must equal pk_col_0 value (10), not whole PK region");
    assert_eq!(k0, k1, "rows sharing pk_col_0 must hash to the same group key");
    assert_eq!(k2, 20u128);
    assert_ne!(k0, k2);
}

/// Pair-test on single-PK U64: extract_group_key must still return
/// the full PK region (bit-identical to the prior whole-region widen).
#[test]
fn test_extract_group_key_single_pk_col_single_pk_bit_identical() {
    let schema = make_schema_u64_i64();
    let batch = make_batch(&schema, &[(42, 1, 100), (99, 1, 200)]);
    let mb = batch.as_mem_batch();

    let k0 = extract_group_key(&mb, 0, &schema, &[0u32]);
    let k1 = extract_group_key(&mb, 1, &schema, &[0u32]);
    assert_eq!(k0, 42u128, "single PK widens to the same value as before");
    assert_eq!(k1, 99u128);
}

/// End-to-end op_reduce: GROUP BY pk_col_0 (a strict subset of a
/// compound PK) with COUNT(*). Pre-fix the slow path widened the
/// whole PK region and split every (pk_col_0, pk_col_1) pair into
/// its own group; the fix collapses rows sharing pk_col_0.
#[test]
fn test_op_reduce_compound_pk_group_by_subset_count() {
    use crate::storage::ReadCursor;
    use std::rc::Rc;

    let in_schema = make_compound_pk_2xu64_schema();
    // GROUP BY a single U64 column → use_natural_pk via
    // SingleNaturalCol. Output: U64 pk + I64 count.
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 1),
        ],
        &[0],
    );

    let empty_out = Rc::new(Batch::empty_with_schema(&out_schema));
    let mut to_ch = ReadCursor::from_owned(&[empty_out], out_schema);

    // PK-sorted (pk0, pk1): (1,10), (1,20), (2,10).
    let delta = make_batch_compound_2xu64(&in_schema, &[(1, 10, 1, 0), (1, 20, 1, 0), (2, 10, 1, 0)]);

    let agg = AggDescriptor {
        col_idx: 2,
        agg_op: AggOp::Count,
        col_type_code: TypeCode::I64,
        _pad: [0; 2],
    };

    let (out, _) = op_reduce(
        &delta,
        None,
        &mut to_ch,
        &in_schema,
        &out_schema,
        &[0u32],
        &[agg],
        None,
        None,
        None,
        false,
        false,
    );

    // Two groups: pk_col_0=1 (count=2), pk_col_0=2 (count=1).
    // Pre-fix the count would be 3 (one row per (pk0, pk1) pair).
    assert_eq!(
        out.count, 2,
        "GROUP BY pk_col_0 collapses (1,10) and (1,20) into one group"
    );

    // Output rows in pk_col_0 ascending order (slow path argsorts).
    let mut entries: Vec<(u64, i64)> = (0..out.count)
        .map(|i| {
            let pk_bytes = out.get_pk_bytes(i);
            // Output group-key PK is unsigned U64, OPK == BE at rest.
            let pk = u64::from_be_bytes(pk_bytes.try_into().unwrap());
            let cnt = read_i64_le(out.col_data(0), i * 8);
            (pk, cnt)
        })
        .collect();
    entries.sort_by_key(|&(pk, _)| pk);
    assert_eq!(entries, vec![(1, 2), (2, 1)]);
}

// -----------------------------------------------------------------------
// U64 MIN/MAX: unsigned ordering for values with the high bit set.
//
// `decode_signed` returns the U64 bit pattern reinterpreted as `i64`;
// signed `<`/`>` flips for values >= 2^63. The fix dispatches on
// TypeCode::U64 in the MIN/MAX comparison sites.
// -----------------------------------------------------------------------

/// 3-col schema: U64 pk, I64 grp, U64 val. All values aggregated by grp
/// fall into a single group when `grp` is held constant.
fn make_schema_u64pk_i64grp_u64val() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::U64, 0),
        ],
        &[0],
    )
}

fn make_batch_u64pk_i64grp_u64val(
    schema: &SchemaDescriptor,
    rows: &[(u64, i64, i64, u64)], // (pk, weight, grp, u64_val)
) -> Batch {
    let n = rows.len();
    let mut b = Batch::with_schema(*schema, n.max(1));
    for &(pk, w, grp, val) in rows {
        b.extend_pk(pk as u128);
        b.extend_weight(&w.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &grp.to_le_bytes());
        b.extend_col(1, &val.to_le_bytes());
        b.count += 1;
    }
    b.set_layout_unchecked(Layout::Consolidated);
    b
}

fn make_schema_u64pk_i64grp_i64val() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    )
}

fn make_batch_u64pk_i64grp_i64val(
    schema: &SchemaDescriptor,
    rows: &[(u64, i64, i64, i64)], // (pk, weight, grp, i64_val)
) -> Batch {
    // `val as u64` writes the identical LE bytes; the twin builders differ only
    // in the tuple's val type.
    let rows: Vec<(u64, i64, i64, u64)> = rows.iter().map(|&(pk, w, grp, val)| (pk, w, grp, val as u64)).collect();
    make_batch_u64pk_i64grp_u64val(schema, &rows)
}

/// Group-by-grp output schema: synthetic U128 PK, I64 grp, U64 agg.
fn make_out_schema_grp_u64agg() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::U64, 1),
        ],
        &[0],
    )
}

fn make_out_schema_grp_i64agg() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::I64, 1),
        ],
        &[0],
    )
}

#[test]
fn test_reduce_min_u64_high_bit_set() {
    use crate::storage::ReadCursor;
    use std::rc::Rc;

    let in_schema = make_schema_u64pk_i64grp_u64val();
    let out_schema = make_out_schema_grp_u64agg();

    let empty_out = Rc::new(Batch::empty_with_schema(&out_schema));
    let mut to_ch = ReadCursor::from_owned(&[empty_out], out_schema);

    // One group (grp=7). val=1, u64::MAX, and 2^63 — the unsigned MIN is 1.
    // Pre-fix signed comparison treats u64::MAX as -1 (smallest signed),
    // so the bug reports u64::MAX as the MIN.
    let delta = make_batch_u64pk_i64grp_u64val(
        &in_schema,
        &[(1, 1, 7, u64::MAX), (2, 1, 7, 10), (3, 1, 7, 1u64 << 63), (4, 1, 7, 1)],
    );

    let agg = AggDescriptor {
        col_idx: 2,
        agg_op: AggOp::Min,
        col_type_code: TypeCode::U64,
        _pad: [0; 2],
    };

    let (out, _) = op_reduce(
        &delta,
        None,
        &mut to_ch,
        &in_schema,
        &out_schema,
        &[1u32],
        &[agg],
        None,
        None,
        None,
        false,
        false,
    );
    assert_eq!(out.count, 1);
    let min_bits = u64::from_le_bytes(out.col_data(1)[0..8].try_into().unwrap());
    assert_eq!(min_bits, 1u64, "MIN(u64) must use unsigned ordering");
}

#[test]
fn test_reduce_max_u64_high_bit_set() {
    use crate::storage::ReadCursor;
    use std::rc::Rc;

    let in_schema = make_schema_u64pk_i64grp_u64val();
    let out_schema = make_out_schema_grp_u64agg();

    let empty_out = Rc::new(Batch::empty_with_schema(&out_schema));
    let mut to_ch = ReadCursor::from_owned(&[empty_out], out_schema);

    // Same input as MIN test. Unsigned MAX is u64::MAX. Pre-fix signed
    // comparison treats 10 (positive i64) as larger than u64::MAX (=-1).
    let delta = make_batch_u64pk_i64grp_u64val(
        &in_schema,
        &[(1, 1, 7, u64::MAX), (2, 1, 7, 10), (3, 1, 7, 1u64 << 63), (4, 1, 7, 1)],
    );

    let agg = AggDescriptor {
        col_idx: 2,
        agg_op: AggOp::Max,
        col_type_code: TypeCode::U64,
        _pad: [0; 2],
    };

    let (out, _) = op_reduce(
        &delta,
        None,
        &mut to_ch,
        &in_schema,
        &out_schema,
        &[1u32],
        &[agg],
        None,
        None,
        None,
        false,
        false,
    );
    assert_eq!(out.count, 1);
    let max_bits = u64::from_le_bytes(out.col_data(1)[0..8].try_into().unwrap());
    assert_eq!(max_bits, u64::MAX, "MAX(u64) must use unsigned ordering");
}

#[test]
fn test_reduce_min_u64_incremental() {
    use crate::storage::ReadCursor;
    use std::rc::Rc;

    let in_schema = make_schema_u64pk_i64grp_u64val();
    let out_schema = make_out_schema_grp_u64agg();

    // Tick 1: one row with val=1u64<<60 → MIN = 1u64<<60.
    let empty_out = Rc::new(Batch::empty_with_schema(&out_schema));
    let mut to_ch = ReadCursor::from_owned(&[empty_out], out_schema);

    let delta1 = make_batch_u64pk_i64grp_u64val(&in_schema, &[(1, 1, 7, 1u64 << 60)]);

    let agg = AggDescriptor {
        col_idx: 2,
        agg_op: AggOp::Min,
        col_type_code: TypeCode::U64,
        _pad: [0; 2],
    };

    let empty_ti = Rc::new(Batch::empty_with_schema(&in_schema));
    let mut ti_ch = ReadCursor::from_owned(&[empty_ti], in_schema);

    let (out1, _) = op_reduce(
        &delta1,
        Some(&mut ti_ch),
        &mut to_ch,
        &in_schema,
        &out_schema,
        &[1u32],
        &[agg],
        None,
        None,
        None,
        false,
        false,
    );
    assert_eq!(out1.count, 1);
    let min1 = u64::from_le_bytes(out1.col_data(1)[0..8].try_into().unwrap());
    assert_eq!(min1, 1u64 << 60);

    // Tick 2: delta adds a row with val=1u64<<63. Replay re-steps over
    // (trace_in: tick-1 row) + (delta: tick-2 row).
    //
    // MIN(1u64<<60, 1u64<<63) is unchanged at 1u64<<60 under unsigned;
    // under buggy signed compare it would flip to 1u64<<63 = i64::MIN.
    // op_reduce emits retract+new even when the value didn't change, so
    // we get 2 rows; we assert the new emitted value is the unsigned MIN.
    let prev_out = Rc::new(out1);
    let mut to_ch2 = ReadCursor::from_owned(&[prev_out], out_schema);

    let ti2 = Rc::new(make_batch_u64pk_i64grp_u64val(&in_schema, &[(1, 1, 7, 1u64 << 60)]));
    let mut ti_ch2 = ReadCursor::from_owned(&[ti2], in_schema);

    let delta2 = make_batch_u64pk_i64grp_u64val(&in_schema, &[(2, 1, 7, 1u64 << 63)]);

    let (out2, _) = op_reduce(
        &delta2,
        Some(&mut ti_ch2),
        &mut to_ch2,
        &in_schema,
        &out_schema,
        &[1u32],
        &[agg],
        None,
        None,
        None,
        false,
        false,
    );
    assert_eq!(out2.count, 2, "retract old MIN + emit new MIN");
    let retracted = u64::from_le_bytes(out2.col_data(1)[0..8].try_into().unwrap());
    let new_min = u64::from_le_bytes(out2.col_data(1)[8..16].try_into().unwrap());
    assert_eq!(retracted, 1u64 << 60);
    assert_eq!(out2.get_weight(0), -1);
    assert_eq!(
        new_min,
        1u64 << 60,
        "MIN unchanged under unsigned ordering; bug would flip it to 1u64<<63"
    );
    assert_eq!(out2.get_weight(1), 1);
}

#[test]
fn test_reduce_max_u64_incremental() {
    use crate::storage::ReadCursor;
    use std::rc::Rc;

    let in_schema = make_schema_u64pk_i64grp_u64val();
    let out_schema = make_out_schema_grp_u64agg();

    // Tick 1: MAX over a single low value → MAX = 10.
    let empty_out = Rc::new(Batch::empty_with_schema(&out_schema));
    let mut to_ch = ReadCursor::from_owned(&[empty_out], out_schema);

    let delta1 = make_batch_u64pk_i64grp_u64val(&in_schema, &[(1, 1, 7, 10)]);

    let agg = AggDescriptor {
        col_idx: 2,
        agg_op: AggOp::Max,
        col_type_code: TypeCode::U64,
        _pad: [0; 2],
    };

    let empty_ti = Rc::new(Batch::empty_with_schema(&in_schema));
    let mut ti_ch = ReadCursor::from_owned(&[empty_ti], in_schema);

    let (out1, _) = op_reduce(
        &delta1,
        Some(&mut ti_ch),
        &mut to_ch,
        &in_schema,
        &out_schema,
        &[1u32],
        &[agg],
        None,
        None,
        None,
        false,
        false,
    );
    assert_eq!(out1.count, 1);
    let max1 = u64::from_le_bytes(out1.col_data(1)[0..8].try_into().unwrap());
    assert_eq!(max1, 10);

    // Tick 2: delta adds val=u64::MAX. Replay re-steps over both.
    // Pre-fix signed MAX would treat u64::MAX as -1, keeping MAX=10.
    let prev_out = Rc::new(out1);
    let mut to_ch2 = ReadCursor::from_owned(&[prev_out], out_schema);

    let ti2 = Rc::new(make_batch_u64pk_i64grp_u64val(&in_schema, &[(1, 1, 7, 10)]));
    let mut ti_ch2 = ReadCursor::from_owned(&[ti2], in_schema);

    let delta2 = make_batch_u64pk_i64grp_u64val(&in_schema, &[(2, 1, 7, u64::MAX)]);

    let (out2, _) = op_reduce(
        &delta2,
        Some(&mut ti_ch2),
        &mut to_ch2,
        &in_schema,
        &out_schema,
        &[1u32],
        &[agg],
        None,
        None,
        None,
        false,
        false,
    );
    // Expect: retract old MAX (10) + emit new MAX (u64::MAX).
    assert_eq!(out2.count, 2);
    let retracted = u64::from_le_bytes(out2.col_data(1)[0..8].try_into().unwrap());
    assert_eq!(retracted, 10);
    assert_eq!(out2.get_weight(0), -1);
    let new_max = u64::from_le_bytes(out2.col_data(1)[8..16].try_into().unwrap());
    assert_eq!(new_max, u64::MAX, "new MAX must be unsigned-max u64::MAX");
    assert_eq!(out2.get_weight(1), 1);
}

#[test]
fn test_avi_seed_u64_high_bit() {
    // The AVI fast path seeds an Accumulator with a U64 bit pattern via
    // `seed_from_raw_bits`, then folds in delta rows via `step_from_batch`.
    // Validates that the U64 bit pattern preserved by the AVI seed
    // compares correctly under unsigned semantics against incoming
    // delta rows.
    let in_schema = make_schema_with_type(type_code::U64);

    let desc = AggDescriptor {
        col_idx: 1,
        agg_op: AggOp::Min,
        col_type_code: TypeCode::U64,
        _pad: [0; 2],
    };
    let mut acc = Accumulator::new(&desc, &in_schema);

    // AVI seeds the accumulator with 1u64<<63 (high bit set).
    acc.seed_from_raw_bits(1u64 << 63);
    assert_eq!(acc.get_value_bits(), 1u64 << 63);

    // Build a batch with a single row val=10u64, pk=1.
    let batch = {
        let mut b = Batch::with_schema(in_schema, 1);
        b.extend_pk(1u128);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &10u64.to_le_bytes());
        b.count += 1;
        b.set_layout_unchecked(Layout::Consolidated);
        b
    };
    let mb = batch.as_mem_batch();
    acc.step_from_batch(&mb, 0, 1);

    // 10u64 < (1u64<<63) under unsigned: MIN updates to 10.
    // Under buggy signed comparison: 10i64 > i64::MIN, MIN stays at i64::MIN.
    assert_eq!(
        acc.get_value_bits(),
        10u64,
        "unsigned MIN against AVI-seeded U64 high-bit value"
    );
}

#[test]
fn test_reduce_min_u64_replay_via_trace_in() {
    use crate::storage::ReadCursor;
    use std::rc::Rc;

    // Exercise the replay path (step_from_batch over trace_in rows + delta
    // rows) with U64 values that span both halves of the unsigned range.
    // Pre-fix the signed comparator treats high-bit-set values as the
    // most-negative; unsigned MIN flips silently.
    let in_schema = make_schema_u64pk_i64grp_u64val();
    let out_schema = make_out_schema_grp_u64agg();

    let agg = AggDescriptor {
        col_idx: 2,
        agg_op: AggOp::Min,
        col_type_code: TypeCode::U64,
        _pad: [0; 2],
    };

    // Tick 1: single insertion. trace_in empty. MIN = u64::MAX.
    let empty_ti = Rc::new(Batch::empty_with_schema(&in_schema));
    let mut ti_ch = ReadCursor::from_owned(&[empty_ti], in_schema);
    let empty_to = Rc::new(Batch::empty_with_schema(&out_schema));
    let mut to_ch = ReadCursor::from_owned(&[empty_to], out_schema);

    let delta1 = make_batch_u64pk_i64grp_u64val(&in_schema, &[(1, 1, 7, u64::MAX)]);

    let (out1, _) = op_reduce(
        &delta1,
        Some(&mut ti_ch),
        &mut to_ch,
        &in_schema,
        &out_schema,
        &[1u32],
        &[agg],
        None,
        None,
        None,
        false,
        false,
    );
    assert_eq!(out1.count, 1);
    let min1 = u64::from_le_bytes(out1.col_data(1)[0..8].try_into().unwrap());
    assert_eq!(min1, u64::MAX);

    // Tick 2: trace_in = tick-1 row (pk=1, val=u64::MAX). Delta inserts
    // pk=2 with val=5. Replay re-steps over both via step_from_batch.
    //
    // Unsigned MIN(u64::MAX, 5) = 5; the smaller value replaces the seed.
    // Pre-fix signed: 5i64 > -1i64 (=u64::MAX as i64), so MIN stays at
    // u64::MAX and no MIN change is observed.
    let ti2 = Rc::new(delta1);
    let mut ti_ch2 = ReadCursor::from_owned(&[ti2], in_schema);
    let prev_out = Rc::new(out1);
    let mut to_ch2 = ReadCursor::from_owned(&[prev_out], out_schema);

    let delta2 = make_batch_u64pk_i64grp_u64val(&in_schema, &[(2, 1, 7, 5u64)]);

    let (out2, _) = op_reduce(
        &delta2,
        Some(&mut ti_ch2),
        &mut to_ch2,
        &in_schema,
        &out_schema,
        &[1u32],
        &[agg],
        None,
        None,
        None,
        false,
        false,
    );
    // Retract old MIN (u64::MAX) + emit new MIN (5).
    assert_eq!(out2.count, 2);
    let retracted = u64::from_le_bytes(out2.col_data(1)[0..8].try_into().unwrap());
    assert_eq!(retracted, u64::MAX);
    assert_eq!(out2.get_weight(0), -1);
    let new_min = u64::from_le_bytes(out2.col_data(1)[8..16].try_into().unwrap());
    assert_eq!(new_min, 5u64, "replay over trace_in + delta must use unsigned MIN");
    assert_eq!(out2.get_weight(1), 1);
}

#[test]
fn test_reduce_min_max_i64_boundary() {
    use crate::storage::ReadCursor;
    use std::rc::Rc;

    // Guard that the TypeCode::U64 branch does not leak into I64 paths:
    // MIN of {i64::MIN, -1, 0, i64::MAX} = i64::MIN,
    // MAX = i64::MAX.
    let in_schema = make_schema_u64pk_i64grp_i64val();
    let out_schema = make_out_schema_grp_i64agg();

    // MIN test.
    {
        let empty_out = Rc::new(Batch::empty_with_schema(&out_schema));
        let mut to_ch = ReadCursor::from_owned(&[empty_out], out_schema);

        let delta = make_batch_u64pk_i64grp_i64val(
            &in_schema,
            &[(1, 1, 7, i64::MIN), (2, 1, 7, -1), (3, 1, 7, 0), (4, 1, 7, i64::MAX)],
        );

        let agg = AggDescriptor {
            col_idx: 2,
            agg_op: AggOp::Min,
            col_type_code: TypeCode::I64,
            _pad: [0; 2],
        };

        let (out, _) = op_reduce(
            &delta,
            None,
            &mut to_ch,
            &in_schema,
            &out_schema,
            &[1u32],
            &[agg],
            None,
            None,
            None,
            false,
            false,
        );
        assert_eq!(out.count, 1);
        let min = read_i64_le(out.col_data(1), 0);
        assert_eq!(min, i64::MIN, "MIN(I64) signed ordering preserved");
    }

    // MAX test.
    {
        let empty_out = Rc::new(Batch::empty_with_schema(&out_schema));
        let mut to_ch = ReadCursor::from_owned(&[empty_out], out_schema);

        let delta = make_batch_u64pk_i64grp_i64val(
            &in_schema,
            &[(1, 1, 7, i64::MIN), (2, 1, 7, -1), (3, 1, 7, 0), (4, 1, 7, i64::MAX)],
        );

        let agg = AggDescriptor {
            col_idx: 2,
            agg_op: AggOp::Max,
            col_type_code: TypeCode::I64,
            _pad: [0; 2],
        };

        let (out, _) = op_reduce(
            &delta,
            None,
            &mut to_ch,
            &in_schema,
            &out_schema,
            &[1u32],
            &[agg],
            None,
            None,
            None,
            false,
            false,
        );
        assert_eq!(out.count, 1);
        let max = read_i64_le(out.col_data(1), 0);
        assert_eq!(max, i64::MAX, "MAX(I64) signed ordering preserved");
    }
}

// -----------------------------------------------------------------------
// group_by_pk fast path on unsorted input
//
// op_reduce's group_by_pk fast path walks rows in physical order and
// treats consecutive same-PK rows as one group. That assumption only
// holds when `working` is sorted; an unsorted delta (e.g. from
// `map_reindex` upstream) splits one PK into multiple groups and
// produces duplicate PK rows / double-retractions.
// -----------------------------------------------------------------------

/// Build a raw `Batch` (`sorted = false`, `consolidated = false`) with
/// one I64 payload column. `pk_encode` maps the row's PK type to the
/// u128 that `extend_pk` expects (e.g. `|pk: i64| (pk as u64) as u128`
/// for signed, `|pk: u64| pk as u128` for unsigned).
fn make_batch_raw_pk<T: Copy>(
    schema: &SchemaDescriptor,
    rows: &[(T, i64, i64)],
    pk_encode: impl Fn(T) -> u128,
) -> Batch {
    let n = rows.len();
    let mut b = Batch::with_schema(*schema, n.max(1));
    for &(pk, w, val) in rows {
        // pk_encode yields the native value; OPK-encode (sign-flip for signed).
        b.extend_pk_opk(schema, &[pk_encode(pk)]);
        b.extend_weight(&w.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &val.to_le_bytes());
        b.count += 1;
    }
    b.set_layout_unchecked(Layout::Raw);
    b
}

/// Common output schema for SUM aggregates over a single-col PK:
/// `U128 pk, I64 sum (nullable)`.
fn make_pk_sum_out_schema() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::I64, 1),
        ],
        &[0],
    )
}

/// SUM-over-single-col-PK output schema with the trailing `I64 count`
/// cardinality companion that every all-linear reduce now carries:
/// `U128 pk, I64 sum (nullable), I64 count`.
fn make_pk_sum_count_out_schema() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::I64, 1),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    )
}

/// `[Sum(sum_col, sum_tc), Count(col 0)]` — a linear SUM plus the appended
/// cardinality companion every all-linear reduce carries, the agg_descs the
/// planner produces for `SELECT …, SUM(v) … GROUP BY …`.
fn sum_count_aggs(sum_col: u32, sum_tc: TypeCode) -> [AggDescriptor; 2] {
    [
        AggDescriptor {
            col_idx: sum_col,
            agg_op: AggOp::Sum,
            col_type_code: sum_tc,
            _pad: [0; 2],
        },
        AggDescriptor {
            col_idx: 0,
            agg_op: AggOp::Count,
            col_type_code: TypeCode::I64,
            _pad: [0; 2],
        },
    ]
}

#[test]
fn test_reduce_group_by_pk_unsorted_input_linear_sum() {
    use crate::storage::ReadCursor;
    use std::rc::Rc;

    let in_schema = make_schema_u64_i64();
    let out_schema = make_pk_sum_count_out_schema();
    let empty_out = Rc::new(Batch::empty_with_schema(&out_schema));
    let mut to_ch = ReadCursor::from_owned(&[empty_out], out_schema);

    // Unsorted: pk=5 appears twice, separated by pk=3. The fast path
    // pre-fix walked physical order and split into 3 groups → emitting
    // two distinct pk=5 rows.
    let delta = make_batch_raw_pk(&in_schema, &[(5, 1, 10), (3, 1, 20), (5, 1, 30)], |pk: u64| pk as u128);

    let aggs = sum_count_aggs(1, TypeCode::I64);

    let (out, _) = op_reduce(
        &delta,
        None,
        &mut to_ch,
        &in_schema,
        &out_schema,
        &[0u32],
        &aggs,
        None,
        None,
        None,
        false,
        false,
    );

    assert_eq!(out.count, 2, "one row per distinct PK");
    let pk0 = out.get_pk_bytes(0);
    let pk1 = out.get_pk_bytes(1);
    let pk0_val = u128::from_be_bytes(pk0.try_into().unwrap());
    let pk1_val = u128::from_be_bytes(pk1.try_into().unwrap());
    assert_eq!(pk0_val, 3, "PK-sorted: 3 precedes 5");
    assert_eq!(pk1_val, 5);
    let sum0 = read_i64_le(out.col_data(0), 0);
    let sum1 = read_i64_le(out.col_data(0), 8);
    assert_eq!(sum0, 20, "SUM for pk=3");
    assert_eq!(sum1, 40, "SUM for pk=5 (10+30) — pre-fix produced two split rows");
    // The rows above are physically in canonical PK order (3 before 5), but
    // op_reduce now ships its output as an honest unconsolidated delta: it never
    // certifies the flags (a decreasing aggregate would emit a descending old/new
    // pair at the same PK), so downstream re-sorts/folds.
    assert!(
        !out.is_sorted() && !out.is_consolidated(),
        "reduce output is an unconsolidated delta"
    );
}

#[test]
fn test_reduce_group_by_pk_unsorted_input_count() {
    use crate::storage::ReadCursor;
    use std::rc::Rc;

    let in_schema = make_schema_u64_i64();
    let out_schema = make_pk_sum_out_schema(); // pk + I64 agg
    let empty_out = Rc::new(Batch::empty_with_schema(&out_schema));
    let mut to_ch = ReadCursor::from_owned(&[empty_out], out_schema);

    let delta = make_batch_raw_pk(&in_schema, &[(5, 1, 10), (3, 1, 20), (5, 1, 30)], |pk: u64| pk as u128);

    let agg = AggDescriptor {
        col_idx: 1,
        agg_op: AggOp::Count,
        col_type_code: TypeCode::I64,
        _pad: [0; 2],
    };

    let (out, _) = op_reduce(
        &delta,
        None,
        &mut to_ch,
        &in_schema,
        &out_schema,
        &[0u32],
        &[agg],
        None,
        None,
        None,
        false,
        false,
    );

    assert_eq!(out.count, 2);
    let pk0 = u128::from_be_bytes(out.get_pk_bytes(0).try_into().unwrap());
    let pk1 = u128::from_be_bytes(out.get_pk_bytes(1).try_into().unwrap());
    assert_eq!((pk0, pk1), (3, 5));
    let c0 = read_i64_le(out.col_data(0), 0);
    let c1 = read_i64_le(out.col_data(0), 8);
    assert_eq!((c0, c1), (1, 2), "pk=3 → 1, pk=5 → 2");
}

#[test]
fn test_reduce_group_by_pk_unsorted_sorted_input_equivalence() {
    use crate::storage::ReadCursor;
    use std::rc::Rc;

    let in_schema = make_schema_u64_i64();
    let out_schema = make_pk_sum_count_out_schema();
    let empty_out = Rc::new(Batch::empty_with_schema(&out_schema));
    let mut to_ch = ReadCursor::from_owned(&[empty_out], out_schema);

    // Same data as the unsorted-sum test but pre-sorted. The
    // sorted-`working` branch must produce identical output.
    let mut delta = make_batch_raw_pk(&in_schema, &[(3, 1, 20), (5, 1, 10), (5, 1, 30)], |pk: u64| pk as u128);
    delta.set_layout_unchecked(Layout::Sorted);

    let aggs = sum_count_aggs(1, TypeCode::I64);

    let (out, _) = op_reduce(
        &delta,
        None,
        &mut to_ch,
        &in_schema,
        &out_schema,
        &[0u32],
        &aggs,
        None,
        None,
        None,
        false,
        false,
    );

    assert_eq!(out.count, 2);
    let pk0 = u128::from_be_bytes(out.get_pk_bytes(0).try_into().unwrap());
    let pk1 = u128::from_be_bytes(out.get_pk_bytes(1).try_into().unwrap());
    assert_eq!((pk0, pk1), (3, 5));
    let sum0 = read_i64_le(out.col_data(0), 0);
    let sum1 = read_i64_le(out.col_data(0), 8);
    assert_eq!((sum0, sum1), (20, 40));
}

#[test]
fn test_reduce_group_by_pk_unsorted_compound_pk_permuted() {
    use crate::storage::ReadCursor;
    use std::rc::Rc;

    let in_schema = make_compound_pk_2xu64_schema();
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 1),
        ],
        &[0, 1],
    );
    let empty_out = Rc::new(Batch::empty_with_schema(&out_schema));
    let mut to_ch = ReadCursor::from_owned(&[empty_out], out_schema);

    // Unsorted compound-PK delta: physical order is (2,1) then (1,2).
    // Canonical pk_indices order should emit (1,2) first.
    let mut delta = make_batch_compound_2xu64(&in_schema, &[(2, 1, 1, 20), (1, 2, 1, 10)]);
    delta.set_layout_unchecked(Layout::Raw);

    let agg = AggDescriptor {
        col_idx: 2,
        agg_op: AggOp::Count,
        col_type_code: TypeCode::I64,
        _pad: [0; 2],
    };

    // Permuted GROUP BY: [1, 0]. PkPermutation still holds.
    let (out, _) = op_reduce(
        &delta,
        None,
        &mut to_ch,
        &in_schema,
        &out_schema,
        &[1u32, 0u32],
        &[agg],
        None,
        None,
        None,
        false,
        false,
    );

    assert_eq!(out.count, 2);
    let pk0 = out.get_pk_bytes(0);
    let pk1 = out.get_pk_bytes(1);
    let p0_col0 = u64::from_be_bytes(pk0[0..8].try_into().unwrap());
    let p0_col1 = u64::from_be_bytes(pk0[8..16].try_into().unwrap());
    let p1_col0 = u64::from_be_bytes(pk1[0..8].try_into().unwrap());
    let p1_col1 = u64::from_be_bytes(pk1[8..16].try_into().unwrap());
    // Canonical pk_indices order is [col0, col1] ascending — (1,2) first.
    // A u128.cmp on the widened PK region would put (2,1) first because
    // col1 dominates the high bytes.
    assert_eq!(
        (p0_col0, p0_col1),
        (1, 2),
        "compound-PK canonical sort: pk_indices priority, not u128 priority"
    );
    assert_eq!((p1_col0, p1_col1), (2, 1));
}

#[test]
fn test_reduce_group_by_pk_unsorted_signed_pk() {
    use crate::storage::ReadCursor;
    use std::rc::Rc;

    let in_schema = make_schema_i64pk_i64();
    // Output PK is naturally U128 here too (extends signed encoding via
    // emit_pk on a single-col PK; we only check ordering of payload). The
    // trailing I64 is the cardinality companion every all-linear reduce carries.
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::I64, 1),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    );
    let empty_out = Rc::new(Batch::empty_with_schema(&out_schema));
    let mut to_ch = ReadCursor::from_owned(&[empty_out], out_schema);

    // Unsorted signed-PK delta. A u128.cmp on the widened (zero-extended)
    // i64-as-u64 bit pattern would put negatives at the TOP (they widen
    // to large u64 values), so emit order would start with pk=2.
    let delta = make_batch_raw_pk(&in_schema, &[(-1, 1, 10), (2, 1, 20), (-3, 1, 30)], |pk: i64| {
        (pk as u64) as u128
    });

    let aggs = sum_count_aggs(1, TypeCode::I64);

    let (out, _) = op_reduce(
        &delta,
        None,
        &mut to_ch,
        &in_schema,
        &out_schema,
        &[0u32],
        &aggs,
        None,
        None,
        None,
        false,
        false,
    );

    assert_eq!(out.count, 3, "one row per distinct signed PK");
    let pks: Vec<i64> = (0..out.count)
        .map(|i| {
            // U128 output PK = group_key widened right-aligned, so the I64's
            // OPK bytes sit in the low 8 bytes; decode them back to native.
            let bytes = out.get_pk_bytes(i);
            opk_pk_i64(&bytes[8..16])
        })
        .collect();
    let sums: Vec<i64> = (0..out.count).map(|i| read_i64_le(out.col_data(0), i * 8)).collect();
    // Canonical signed order: -3, -1, 2.
    assert_eq!(
        pks,
        vec![-3, -1, 2],
        "signed PK must sort via i64 order, not u128-of-bits order"
    );
    assert_eq!(sums, vec![30, 10, 20]);
}

#[test]
fn test_reduce_group_by_pk_unsorted_with_retraction() {
    use crate::storage::ReadCursor;
    use std::rc::Rc;

    let in_schema = make_schema_u64_i64();
    let out_schema = make_pk_sum_count_out_schema();

    // Build a pre-populated trace_out carrying (pk=5, SUM=100, count=1). The
    // trailing count is the cardinality companion; the old group's one prior row
    // gives count=1, so the group survives the +1 insert delta.
    let mut prev = Batch::with_schema(out_schema, 1);
    prev.extend_pk(5u128);
    prev.extend_weight(&1i64.to_le_bytes());
    prev.extend_null_bmp(&0u64.to_le_bytes());
    prev.extend_col(0, &100i64.to_le_bytes());
    prev.extend_col(1, &1i64.to_le_bytes());
    prev.count += 1;
    prev.set_layout_unchecked(Layout::Consolidated);
    let prev_rc = Rc::new(prev);
    let mut to_ch = ReadCursor::from_owned(&[prev_rc], out_schema);

    // Unsorted delta with pk=5 split across the batch. Pre-fix: emits
    // TWO `(pk=5, w=-1, SUM=100)` retractions plus split partials.
    let delta = make_batch_raw_pk(&in_schema, &[(5, 1, 10), (3, 1, 20), (5, 1, 30)], |pk: u64| pk as u128);

    let aggs = sum_count_aggs(1, TypeCode::I64);

    let (out, _) = op_reduce(
        &delta,
        None,
        &mut to_ch,
        &in_schema,
        &out_schema,
        &[0u32],
        &aggs,
        None,
        None,
        None,
        false,
        false,
    );

    // Expected: one retract (pk=5, w=-1, SUM=100), one emit (pk=5,
    // w=+1, SUM=140), one emit (pk=3, w=+1, SUM=20). Order is canonical:
    // pk=3 first, then pk=5 (retract+emit).
    assert_eq!(
        out.count, 3,
        "exactly one retract + one new emit for pk=5 plus pk=3 emit"
    );

    let mut by_pk_w: Vec<(u128, i64, i64)> = (0..out.count)
        .map(|i| {
            let pk = u128::from_be_bytes(out.get_pk_bytes(i).try_into().unwrap());
            let w = out.get_weight(i);
            let sum = read_i64_le(out.col_data(0), i * 8);
            (pk, w, sum)
        })
        .collect();
    by_pk_w.sort_by_key(|&(pk, w, _)| (pk, w));

    assert_eq!(
        by_pk_w,
        vec![(3, 1, 20), (5, -1, 100), (5, 1, 140),],
        "single retract+emit for pk=5 (sum 10+30+100=140), single emit for pk=3"
    );
}

#[test]
fn test_reduce_min_group_by_pk_retracts_extreme() {
    use crate::storage::ReadCursor;
    use std::rc::Rc;

    // group_by_pk MIN replay path (`advance_to` + `for_each_pk_group_row`) over a
    // non-unique-PK input: one PK carrying two payloads, GROUP BY the full PK.
    // Retracting the payload that holds the current MIN must recompute MIN from
    // the survivor. This exercises the `fill_cleared_batch` → `consolidate_if_needed`
    // cancellation in the replay buffer: if the refilled batch is wrongly flagged
    // consolidated, the retracted (pk=1, val=10) keeps its positive trace copy and
    // MIN stays 10 instead of moving to 20.
    let in_schema = make_schema_u64_i64();
    let out_schema = make_pk_sum_out_schema(); // [U128 pk, I64 agg (nullable)]

    // trace_in: pk=1 with two payloads (val=10, val=20). The group IS the PK, so
    // both belong to group pk=1; MIN(10, 20) = 10.
    let ti = make_batch(&in_schema, &[(1, 1, 10), (1, 1, 20)]);
    let mut ti_ch = ReadCursor::from_owned(&[Rc::new(ti)], in_schema);

    // trace_out: old MIN(pk=1) = 10.
    let mut prev = Batch::with_schema(out_schema, 1);
    prev.extend_pk(1u128);
    prev.extend_weight(&1i64.to_le_bytes());
    prev.extend_null_bmp(&0u64.to_le_bytes());
    prev.extend_col(0, &10i64.to_le_bytes());
    prev.count += 1;
    prev.set_layout_unchecked(Layout::Consolidated);
    let mut to_ch = ReadCursor::from_owned(&[Rc::new(prev)], out_schema);

    // Delta: retract (pk=1, val=10) — the payload holding the current MIN.
    let delta = make_batch(&in_schema, &[(1, -1, 10)]);

    let agg = AggDescriptor {
        col_idx: 1,
        agg_op: AggOp::Min,
        col_type_code: TypeCode::I64,
        _pad: [0; 2],
    };

    let (out, _) = op_reduce(
        &delta,
        Some(&mut ti_ch),
        &mut to_ch,
        &in_schema,
        &out_schema,
        &[0u32], // GROUP BY PK col 0 → group_by_pk replay path
        &[agg],
        None,
        None,
        None,
        false,
        false,
    );

    // Retract old MIN=10 (w=-1) + emit new MIN=20 (w=+1). With the consolidation
    // bug the survivor and the retracted copy both step, MIN stays 10, and the
    // emitted +1 row carries 10 instead of 20.
    assert_eq!(out.count, 2, "one retract + one re-emit for pk=1");
    let mut by_w: Vec<(i64, i64)> = (0..out.count)
        .map(|i| (out.get_weight(i), read_i64_le(out.col_data(0), i * 8)))
        .collect();
    by_w.sort_by_key(|&(w, _)| w);
    assert_eq!(
        by_w,
        vec![(-1, 10), (1, 20)],
        "retract old MIN=10, insert new MIN=20 after the val=10 payload is retracted",
    );
}

// -----------------------------------------------------------------------
// Byte-form AVI: the index lookup walks the full group-key prefix, so two
// distinct groups never share a bucket. This is the only path that drives
// `seek_first_positive_with_prefix` end to end; the other reduce tests take
// the trace-scan fallback (avi = None).
// -----------------------------------------------------------------------

#[test]
fn avi_two_groups_distinct_byte_form_keys() {
    use super::super::util::encode_ordered;
    use crate::storage::ReadCursor;
    use std::rc::Rc;

    // Input: pk(U64), a(U32), b(U32), val(I64); GROUP BY (a, b), MIN(val).
    let in_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U32, 0),
            SchemaColumn::new(type_code::U32, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    );
    // Output: synthetic U128 PK, group cols (a, b), MIN.
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::U32, 0),
            SchemaColumn::new(type_code::U32, 0),
            SchemaColumn::new(type_code::I64, 1),
        ],
        &[0],
    );

    // delta: one row per group. The delta values are deliberately NOT each
    // group's minimum, so a correct result can only come from the index.
    let delta = {
        let mut b = Batch::with_schema(in_schema, 2);
        for (pk, a, bb, val) in [(1u64, 1u32, 1u32, 50i64), (2, 2, 2, 60)] {
            b.extend_pk(pk as u128);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(in_schema.try_payload_idx(1).unwrap(), &a.to_le_bytes());
            b.extend_col(in_schema.try_payload_idx(2).unwrap(), &bb.to_le_bytes());
            b.extend_col(in_schema.try_payload_idx(3).unwrap(), &val.to_le_bytes());
            b.count += 1;
        }
        b.set_layout_unchecked(Layout::Consolidated);
        b
    };

    // AVI: key = a(4) ++ b(4) ++ av_encoded(8). Group (1,1) min=10, (2,2) min=20.
    let avi_schema = crate::ops::index::make_avi_schema(&in_schema, &[1u32, 2u32]);
    assert_eq!(avi_schema.pk_stride(), 17, "4 + 4 + 1 + 8");
    let avi_batch = {
        let mut b = Batch::with_schema(avi_schema, 2);
        for (a, bb, min) in [(1u32, 1u32, 10i64), (2, 2, 20)] {
            let mut key = [0u8; 17];
            key[0..4].copy_from_slice(&a.to_le_bytes());
            key[4..8].copy_from_slice(&bb.to_le_bytes());
            let av = encode_ordered(&min.to_le_bytes(), type_code::I64, false);
            key[8] = 0; // ordinal 0 (single MIN aggregate)
            key[9..17].copy_from_slice(&av.to_be_bytes());
            b.extend_pk_bytes(&key);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.count += 1;
        }
        b
    };

    let empty_out = Rc::new(Batch::empty_with_schema(&out_schema));
    let mut to_ch = ReadCursor::from_owned(&[empty_out], out_schema);
    let mut avi_ch = ReadCursor::from_owned(&[Rc::new(avi_batch)], avi_schema);

    let agg = AggDescriptor {
        col_idx: 3,
        agg_op: AggOp::Min,
        col_type_code: TypeCode::I64,
        _pad: [0; 2],
    };

    let (out, _) = op_reduce(
        &delta,
        None,
        &mut to_ch,
        &in_schema,
        &out_schema,
        &[1u32, 2u32],
        &[agg],
        Some(&mut avi_ch),
        None,
        None,
        false,
        false,
    );

    assert_eq!(out.count, 2, "two groups → two rows");
    // Output payload: a at pi 0, b at pi 1, min at pi 2.
    for i in 0..out.count {
        let a = crate::foundation::codec::read_u32_le(out.col_data(0), i * 4);
        let bb = crate::foundation::codec::read_u32_le(out.col_data(1), i * 4);
        let min = read_i64_le(out.col_data(2), i * 8);
        let expected = match (a, bb) {
            (1, 1) => 10,
            (2, 2) => 20,
            _ => panic!("unexpected group ({a}, {bb})"),
        };
        assert_eq!(
            min, expected,
            "group ({a},{bb}) must resolve its own indexed MIN, not the other group's"
        );
    }
}

// -----------------------------------------------------------------------
// AVI lookup on a retraction: when the current MIN is retracted, the new value
// is taken from the index (its post-state), and the old value is retracted
// from trace_out. The lookup must return the smallest surviving entry. This is
// the AVI path the SQL planner builds for a single MIN/MAX; the trace-scan
// retraction test covers the avi = None fallback. (The +1/-1 consolidation of
// the retracted extremum is the AVI table cursor's job, exercised in the
// storage consolidation tests; here the AVI holds the post-state directly.)
// -----------------------------------------------------------------------

#[test]
fn avi_retraction_returns_next_extremum() {
    use super::super::util::encode_ordered;
    use crate::storage::ReadCursor;
    use std::rc::Rc;

    // Input: pk(U64), a(U32), val(I64); GROUP BY a, MIN(val).
    let in_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U32, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    );
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::U32, 0),
            SchemaColumn::new(type_code::I64, 1),
        ],
        &[0],
    );

    // delta: retract the row holding the current MIN (val=5) of group a=1.
    let delta = {
        let mut b = Batch::with_schema(in_schema, 1);
        b.extend_pk(1u128);
        b.extend_weight(&(-1i64).to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(in_schema.try_payload_idx(1).unwrap(), &1u32.to_le_bytes());
        b.extend_col(in_schema.try_payload_idx(2).unwrap(), &5i64.to_le_bytes());
        b.count += 1;
        b.set_layout_unchecked(Layout::Consolidated);
        b
    };
    let group_key = extract_group_key(&delta.as_mem_batch(), 0, &in_schema, &[1u32]);

    // AVI (post-state for group a=1): the retracted 5 is gone; the surviving
    // values are {10, 20}, so the prefix walk must return the smaller, 10.
    let avi_schema = crate::ops::index::make_avi_schema(&in_schema, &[1u32]);
    let avi_batch = {
        let mut b = Batch::with_schema(avi_schema, 2);
        for min in [10i64, 20] {
            let mut key = [0u8; 13];
            key[0..4].copy_from_slice(&1u32.to_le_bytes());
            let av = encode_ordered(&min.to_le_bytes(), type_code::I64, false);
            key[4] = 0; // ordinal 0 (single MIN aggregate)
            key[5..13].copy_from_slice(&av.to_be_bytes());
            b.extend_pk_bytes(&key);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.count += 1;
        }
        b.set_layout_unchecked(Layout::Consolidated);
        b
    };

    // trace_out: previous output for a=1 was MIN=5.
    let to_batch = {
        let mut b = Batch::with_schema(out_schema, 1);
        b.extend_pk(group_key);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &1u32.to_le_bytes()); // a
        b.extend_col(1, &5i64.to_le_bytes()); // min
        b.count += 1;
        b.set_layout_unchecked(Layout::Consolidated);
        Rc::new(b)
    };

    let mut to_ch = ReadCursor::from_owned(&[to_batch], out_schema);
    let mut avi_ch = ReadCursor::from_owned(&[Rc::new(avi_batch)], avi_schema);

    let agg = AggDescriptor {
        col_idx: 2,
        agg_op: AggOp::Min,
        col_type_code: TypeCode::I64,
        _pad: [0; 2],
    };

    let (out, _) = op_reduce(
        &delta,
        None,
        &mut to_ch,
        &in_schema,
        &out_schema,
        &[1u32],
        &[agg],
        Some(&mut avi_ch),
        None,
        None,
        false,
        false,
    );

    // Expect a retraction of the old MIN (5, weight -1) and the recomputed MIN
    // (10, weight +1) read from the index — never the retracted 5.
    let mut retracted = None;
    let mut inserted = None;
    for i in 0..out.count {
        let w = out.get_weight(i);
        let v = read_i64_le(out.col_data(1), i * 8);
        if w < 0 {
            retracted = Some(v);
        } else {
            inserted = Some(v);
        }
    }
    assert_eq!(retracted, Some(5), "must retract the stale MIN");
    assert_eq!(
        inserted,
        Some(10),
        "AVI must skip the net-zero retracted extremum and return the next MIN"
    );
}

// -----------------------------------------------------------------------
// A single narrow group column gives the AVI composite a non-power-of-two
// stride (U16 → 10, U32 → 12). The AVI cursor's `drive` widens the PK region
// through `widen_pk_le`, which must zero-extend these strides rather than
// panic.
// -----------------------------------------------------------------------

#[test]
fn avi_non_power_of_two_stride_drives_cursor() {
    use super::super::util::encode_ordered;
    use crate::storage::ReadCursor;
    use std::rc::Rc;

    for (gtc, gsize, stride) in [(type_code::U16, 2usize, 11usize), (type_code::U32, 4, 13)] {
        let in_schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(gtc, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0],
        );
        let out_schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U128, 0),
                SchemaColumn::new(gtc, 0),
                SchemaColumn::new(type_code::I64, 1),
            ],
            &[0],
        );

        let gval: u64 = 7;
        let delta = {
            let mut b = Batch::with_schema(in_schema, 1);
            b.extend_pk(1u128);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(in_schema.try_payload_idx(1).unwrap(), &gval.to_le_bytes()[..gsize]);
            b.extend_col(in_schema.try_payload_idx(2).unwrap(), &100i64.to_le_bytes());
            b.count += 1;
            b.set_layout_unchecked(Layout::Consolidated);
            b
        };

        let avi_schema = crate::ops::index::make_avi_schema(&in_schema, &[1u32]);
        assert_eq!(avi_schema.pk_stride() as usize, stride);
        let avi_batch = {
            let mut b = Batch::with_schema(avi_schema, 1);
            let mut key = vec![0u8; stride];
            key[..gsize].copy_from_slice(&gval.to_le_bytes()[..gsize]);
            let av = encode_ordered(&42i64.to_le_bytes(), type_code::I64, false);
            key[gsize] = 0; // ordinal
            key[gsize + 1..gsize + 9].copy_from_slice(&av.to_be_bytes());
            b.extend_pk_bytes(&key);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.count += 1;
            b
        };

        let empty_out = Rc::new(Batch::empty_with_schema(&out_schema));
        let mut to_ch = ReadCursor::from_owned(&[empty_out], out_schema);
        let mut avi_ch = ReadCursor::from_owned(&[Rc::new(avi_batch)], avi_schema);

        let agg = AggDescriptor {
            col_idx: 2,
            agg_op: AggOp::Min,
            col_type_code: TypeCode::I64,
            _pad: [0; 2],
        };

        let (out, _) = op_reduce(
            &delta,
            None,
            &mut to_ch,
            &in_schema,
            &out_schema,
            &[1u32],
            &[agg],
            Some(&mut avi_ch),
            None,
            None,
            false,
            false,
        );

        assert_eq!(out.count, 1, "stride {stride}");
        // Output payload: g at pi 0, min at pi 1.
        let min = read_i64_le(out.col_data(1), 0);
        assert_eq!(min, 42, "stride {stride}: indexed MIN");
    }
}

// -----------------------------------------------------------------------
// Trace-scan fallback (avi = None): retracting a group's current MIN must
// recompute the next-best from the replayed history, not re-emit the stale
// value. Guards the `fill_cleared_batch` consolidation-flag reset.
// -----------------------------------------------------------------------

#[test]
fn trace_scan_retraction_recomputes_min() {
    use crate::storage::ReadCursor;
    use std::rc::Rc;

    // Input: pk(U64), g(I64), val(I64); GROUP BY g, MIN(val).
    let in_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    );
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::I64, 1),
        ],
        &[0],
    );

    let mk_row = |b: &mut Batch, pk: u64, w: i64, g: i64, val: i64| {
        b.extend_pk(pk as u128);
        b.extend_weight(&w.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(in_schema.try_payload_idx(1).unwrap(), &g.to_le_bytes());
        b.extend_col(in_schema.try_payload_idx(2).unwrap(), &val.to_le_bytes());
        b.count += 1;
    };

    // History (trace_in): group g=1 holds val=5 (pk=1) and val=10 (pk=2).
    let ti_batch = {
        let mut b = Batch::with_schema(in_schema, 2);
        mk_row(&mut b, 1, 1, 1, 5);
        mk_row(&mut b, 2, 1, 1, 10);
        b.set_layout_unchecked(Layout::Consolidated);
        Rc::new(b)
    };

    // trace_out: previous output for g=1 is MIN=5.
    let to_batch = {
        let mut b = Batch::with_schema(out_schema, 1);
        let gk = extract_group_key(&ti_batch.as_mem_batch(), 0, &in_schema, &[1u32]);
        b.extend_pk(gk);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &1i64.to_le_bytes()); // g
        b.extend_col(1, &5i64.to_le_bytes()); // min
        b.count += 1;
        b.set_layout_unchecked(Layout::Consolidated);
        Rc::new(b)
    };

    // delta: retract the current MIN (pk=1, val=5).
    let delta = {
        let mut b = Batch::with_schema(in_schema, 1);
        mk_row(&mut b, 1, -1, 1, 5);
        b.set_layout_unchecked(Layout::Consolidated);
        b
    };

    let mut ti_ch = ReadCursor::from_owned(&[ti_batch], in_schema);
    let mut to_ch = ReadCursor::from_owned(&[to_batch], out_schema);

    let agg = AggDescriptor {
        col_idx: 2,
        agg_op: AggOp::Min,
        col_type_code: TypeCode::I64,
        _pad: [0; 2],
    };

    let (out, _) = op_reduce(
        &delta,
        Some(&mut ti_ch),
        &mut to_ch,
        &in_schema,
        &out_schema,
        &[1u32],
        &[agg],
        None,
        None,
        None,
        false,
        false,
    );

    // Expect a retraction of the old MIN (5, weight -1) and the recomputed
    // MIN (10, weight +1). Find the inserted row and assert its value.
    let mut new_min = None;
    for i in 0..out.count {
        let w = out.get_weight(i);
        let v = read_i64_le(out.col_data(1), i * 8);
        if w > 0 {
            new_min = Some(v);
        }
    }
    assert_eq!(
        new_min,
        Some(10),
        "retracting val=5 must recompute MIN=10 from replayed history, not re-emit the stale 5"
    );
}

// -----------------------------------------------------------------------
// Tie: a group holds two rows at the current MIN. Retracting one copy must
// leave the MIN unchanged — the surviving copy still pins it. Trace-scan path
// (avi = None); the replay consolidation must keep the duplicate value.
// -----------------------------------------------------------------------

#[test]
fn min_tie_retract_one_copy_keeps_min() {
    use crate::storage::ReadCursor;
    use std::rc::Rc;

    // Input: pk(U64), g(I64), val(I64); GROUP BY g, MIN(val).
    let in_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    );
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::I64, 1),
        ],
        &[0],
    );

    let mk_row = |b: &mut Batch, pk: u64, w: i64, g: i64, val: i64| {
        b.extend_pk(pk as u128);
        b.extend_weight(&w.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(in_schema.try_payload_idx(1).unwrap(), &g.to_le_bytes());
        b.extend_col(in_schema.try_payload_idx(2).unwrap(), &val.to_le_bytes());
        b.count += 1;
    };

    // History: group g=1 holds val=5 twice (pk=1, pk=2) and val=10 (pk=3).
    let ti_batch = {
        let mut b = Batch::with_schema(in_schema, 3);
        mk_row(&mut b, 1, 1, 1, 5);
        mk_row(&mut b, 2, 1, 1, 5);
        mk_row(&mut b, 3, 1, 1, 10);
        b.set_layout_unchecked(Layout::Consolidated);
        Rc::new(b)
    };
    let group_key = extract_group_key(&ti_batch.as_mem_batch(), 0, &in_schema, &[1u32]);

    let to_batch = {
        let mut b = Batch::with_schema(out_schema, 1);
        b.extend_pk(group_key);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &1i64.to_le_bytes());
        b.extend_col(1, &5i64.to_le_bytes());
        b.count += 1;
        b.set_layout_unchecked(Layout::Consolidated);
        Rc::new(b)
    };

    // delta: retract one of the two val=5 rows (pk=1).
    let delta = {
        let mut b = Batch::with_schema(in_schema, 1);
        mk_row(&mut b, 1, -1, 1, 5);
        b.set_layout_unchecked(Layout::Consolidated);
        b
    };

    let mut ti_ch = ReadCursor::from_owned(&[ti_batch], in_schema);
    let mut to_ch = ReadCursor::from_owned(&[to_batch], out_schema);

    let agg = AggDescriptor {
        col_idx: 2,
        agg_op: AggOp::Min,
        col_type_code: TypeCode::I64,
        _pad: [0; 2],
    };

    let (out, _) = op_reduce(
        &delta,
        Some(&mut ti_ch),
        &mut to_ch,
        &in_schema,
        &out_schema,
        &[1u32],
        &[agg],
        None,
        None,
        None,
        false,
        false,
    );

    let mut new_min = None;
    for i in 0..out.count {
        if out.get_weight(i) > 0 {
            new_min = Some(read_i64_le(out.col_data(1), i * 8));
        }
    }
    assert_eq!(
        new_min,
        Some(5),
        "the surviving duplicate at val=5 must keep MIN=5 after retracting one copy"
    );
}

// -----------------------------------------------------------------------
// MIN ignores NULL aggregate values: a group mixing NULL and non-NULL vals
// must aggregate only the non-NULL rows.
// -----------------------------------------------------------------------

#[test]
fn min_ignores_null_values() {
    use crate::storage::ReadCursor;
    use std::rc::Rc;

    // val is nullable.
    let in_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::I64, 1),
        ],
        &[0],
    );
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::I64, 1),
        ],
        &[0],
    );

    let null_bit = 1u64 << in_schema.try_payload_idx(2).unwrap();
    let mk_row = |b: &mut Batch, pk: u64, g: i64, val: i64, is_null: bool| {
        b.extend_pk(pk as u128);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&(if is_null { null_bit } else { 0 }).to_le_bytes());
        b.extend_col(in_schema.try_payload_idx(1).unwrap(), &g.to_le_bytes());
        b.extend_col(in_schema.try_payload_idx(2).unwrap(), &val.to_le_bytes());
        b.count += 1;
    };

    // group g=1: NULL, 7, 3 → MIN ignores NULL → 3.
    let delta = {
        let mut b = Batch::with_schema(in_schema, 3);
        mk_row(&mut b, 1, 1, 0, true);
        mk_row(&mut b, 2, 1, 7, false);
        mk_row(&mut b, 3, 1, 3, false);
        b.set_layout_unchecked(Layout::Consolidated);
        b
    };

    let empty_out = Rc::new(Batch::empty_with_schema(&out_schema));
    let mut to_ch = ReadCursor::from_owned(&[empty_out], out_schema);

    let agg = AggDescriptor {
        col_idx: 2,
        agg_op: AggOp::Min,
        col_type_code: TypeCode::I64,
        _pad: [0; 2],
    };

    let (out, _) = op_reduce(
        &delta,
        None,
        &mut to_ch,
        &in_schema,
        &out_schema,
        &[1u32],
        &[agg],
        None,
        None,
        None,
        false,
        false,
    );

    assert_eq!(out.count, 1);
    let min = read_i64_le(out.col_data(1), 0);
    assert_eq!(min, 3, "MIN must ignore the NULL row and pick 3, not 0");
}

// -----------------------------------------------------------------------
// Multi-column GROUP BY MIN retraction through the AVI: retracting the current
// MIN of a two-column group must return the next-best from the index, keyed by
// the full (a, b) prefix.
// -----------------------------------------------------------------------

#[test]
fn avi_multi_col_retraction_returns_next_extremum() {
    use super::super::util::encode_ordered;
    use crate::storage::ReadCursor;
    use std::rc::Rc;

    // Input: pk(U64), a(U32), b(U32), val(I64); GROUP BY (a, b), MIN(val).
    let in_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U32, 0),
            SchemaColumn::new(type_code::U32, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    );
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::U32, 0),
            SchemaColumn::new(type_code::U32, 0),
            SchemaColumn::new(type_code::I64, 1),
        ],
        &[0],
    );

    // delta: retract group (3, 4)'s current MIN (val=5).
    let delta = {
        let mut b = Batch::with_schema(in_schema, 1);
        b.extend_pk(1u128);
        b.extend_weight(&(-1i64).to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(in_schema.try_payload_idx(1).unwrap(), &3u32.to_le_bytes());
        b.extend_col(in_schema.try_payload_idx(2).unwrap(), &4u32.to_le_bytes());
        b.extend_col(in_schema.try_payload_idx(3).unwrap(), &5i64.to_le_bytes());
        b.count += 1;
        b.set_layout_unchecked(Layout::Consolidated);
        b
    };
    let group_key = extract_group_key(&delta.as_mem_batch(), 0, &in_schema, &[1u32, 2u32]);

    // AVI post-state for (3, 4): surviving min is 9. A decoy entry for a
    // different group (3, 5) sharing the a-byte prefix must NOT be matched.
    let avi_schema = crate::ops::index::make_avi_schema(&in_schema, &[1u32, 2u32]);
    let avi_batch = {
        let mut b = Batch::with_schema(avi_schema, 2);
        let put = |b: &mut Batch, a: u32, bb: u32, min: i64| {
            let mut key = [0u8; 17];
            key[0..4].copy_from_slice(&a.to_le_bytes());
            key[4..8].copy_from_slice(&bb.to_le_bytes());
            let av = encode_ordered(&min.to_le_bytes(), type_code::I64, false);
            key[8] = 0; // ordinal 0 (single MIN aggregate)
            key[9..17].copy_from_slice(&av.to_be_bytes());
            b.extend_pk_bytes(&key);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.count += 1;
        };
        put(&mut b, 3, 4, 9);
        put(&mut b, 3, 5, 1); // decoy: same a, different b, smaller value
        b.set_layout_unchecked(Layout::Consolidated);
        b
    };

    let to_batch = {
        let mut b = Batch::with_schema(out_schema, 1);
        b.extend_pk(group_key);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &3u32.to_le_bytes());
        b.extend_col(1, &4u32.to_le_bytes());
        b.extend_col(2, &5i64.to_le_bytes());
        b.count += 1;
        b.set_layout_unchecked(Layout::Consolidated);
        Rc::new(b)
    };

    let mut to_ch = ReadCursor::from_owned(&[to_batch], out_schema);
    let mut avi_ch = ReadCursor::from_owned(&[Rc::new(avi_batch)], avi_schema);

    let agg = AggDescriptor {
        col_idx: 3,
        agg_op: AggOp::Min,
        col_type_code: TypeCode::I64,
        _pad: [0; 2],
    };

    let (out, _) = op_reduce(
        &delta,
        None,
        &mut to_ch,
        &in_schema,
        &out_schema,
        &[1u32, 2u32],
        &[agg],
        Some(&mut avi_ch),
        None,
        None,
        false,
        false,
    );

    let mut inserted = None;
    for i in 0..out.count {
        if out.get_weight(i) > 0 {
            inserted = Some(read_i64_le(out.col_data(2), i * 8));
        }
    }
    assert_eq!(
        inserted,
        Some(9),
        "must return group (3,4)'s own next MIN (9), not the decoy group (3,5)'s value"
    );
}

// =======================================================================
// Wide byte-form AVI (composite key > 16 bytes). Multi-column and U128/UUID
// group keys produce a composite AVI key past the narrow 16-byte budget, up
// to MAX_PK_BYTES. These tests drive `seek_first_positive_with_prefix`
// through the wide cursor path (pk_stride > 16, ordered by compare_pk_bytes),
// the surface the narrow tests above never reach.
// =======================================================================

// Randomized equivalence: over many wide `(a, b)` groups the AVI lookup must
// return each group's own MIN — the same per-group extremum a trace scan
// would compute. The expected MIN is computed directly in-test (the
// reference); the AVI carries that post-state and the indexed result must
// match it group-for-group. Composite key = a(8) ++ b(8) ++ av(8) = 24 bytes.
#[test]
fn avi_wide_two_u64_groups_match_reference() {
    use super::super::util::encode_ordered;
    use crate::storage::ReadCursor;
    use std::collections::BTreeMap;
    use std::rc::Rc;

    let in_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0), // pk
            SchemaColumn::new(type_code::U64, 0), // a (group)
            SchemaColumn::new(type_code::U64, 0), // b (group)
            SchemaColumn::new(type_code::I64, 0), // val
        ],
        &[0],
    );
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 1),
        ],
        &[0],
    );
    assert_eq!(
        crate::ops::index::make_avi_schema(&in_schema, &[1u32, 2u32]).pk_stride(),
        25,
        "wide composite: 8 + 8 + 1 + 8",
    );

    // Deterministic LCG. Group coordinates span the full u64 range (high bit
    // set) to exercise the wide compare_pk_bytes ordering, not just low bytes.
    let mut state: u64 = 0x9E3779B97F4A7C15;
    let mut next = || {
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        state
    };

    // 40 distinct groups, several rows each; reference MIN computed in-test.
    let mut reference: BTreeMap<(u64, u64), i64> = BTreeMap::new();
    let mut group_coords: Vec<(u64, u64)> = Vec::new();
    for _ in 0..40 {
        let a = next() | (1u64 << 63);
        let b = next();
        group_coords.push((a, b));
        let rows = 1 + (next() % 5) as usize;
        let mut group_min = i64::MAX;
        for _ in 0..rows {
            let v = next() as i64;
            group_min = group_min.min(v);
        }
        reference.insert((a, b), group_min);
    }

    // AVI post-state: one entry per group holding its MIN, sorted by
    // compare_pk_bytes order (ascending a, then b — both unsigned).
    let avi_schema = crate::ops::index::make_avi_schema(&in_schema, &[1u32, 2u32]);
    let avi_batch = {
        let mut keys: Vec<[u8; 25]> = reference
            .iter()
            .map(|(&(a, b), &m)| {
                let mut key = [0u8; 25];
                key[0..8].copy_from_slice(&a.to_le_bytes());
                key[8..16].copy_from_slice(&b.to_le_bytes());
                let av = encode_ordered(&m.to_le_bytes(), type_code::I64, false);
                key[16] = 0; // ordinal 0 (single MIN aggregate)
                key[17..25].copy_from_slice(&av.to_be_bytes());
                key
            })
            .collect();
        // Production's ingest sorts the AVI by memcmp of the composite key
        // (the group prefix is point-probed, never numeric-range-scanned), so
        // the hand-built fixture must be in byte order — not numeric (a, b)
        // order, which diverges from memcmp under little-endian storage.
        keys.sort();
        let mut bt = Batch::with_schema(avi_schema, keys.len());
        for key in &keys {
            bt.extend_pk_bytes(key);
            bt.extend_weight(&1i64.to_le_bytes());
            bt.extend_null_bmp(&0u64.to_le_bytes());
            bt.count += 1;
        }
        bt.set_layout_unchecked(Layout::Consolidated);
        bt
    };

    // Delta: one representative insert per group. Its val is deliberately the
    // group's MIN + 1000 so a correct result can only come from the index.
    let delta = {
        let mut bt = Batch::with_schema(in_schema, group_coords.len());
        for (i, &(a, b)) in group_coords.iter().enumerate() {
            bt.extend_pk(i as u128 + 1);
            bt.extend_weight(&1i64.to_le_bytes());
            bt.extend_null_bmp(&0u64.to_le_bytes());
            bt.extend_col(in_schema.try_payload_idx(1).unwrap(), &a.to_le_bytes());
            bt.extend_col(in_schema.try_payload_idx(2).unwrap(), &b.to_le_bytes());
            let decoy = reference[&(a, b)].wrapping_add(1000);
            bt.extend_col(in_schema.try_payload_idx(3).unwrap(), &decoy.to_le_bytes());
            bt.count += 1;
        }
        bt
    };

    let empty_out = Rc::new(Batch::empty_with_schema(&out_schema));
    let mut to_ch = ReadCursor::from_owned(&[empty_out], out_schema);
    let mut avi_ch = ReadCursor::from_owned(&[Rc::new(avi_batch)], avi_schema);

    let agg = AggDescriptor {
        col_idx: 3,
        agg_op: AggOp::Min,
        col_type_code: TypeCode::I64,
        _pad: [0; 2],
    };

    let (out, _) = op_reduce(
        &delta,
        None,
        &mut to_ch,
        &in_schema,
        &out_schema,
        &[1u32, 2u32],
        &[agg],
        Some(&mut avi_ch),
        None,
        None,
        false,
        false,
    );

    assert_eq!(out.count, reference.len(), "one row per group");
    let mut seen: BTreeMap<(u64, u64), i64> = BTreeMap::new();
    for i in 0..out.count {
        let a = read_u64_le(out.col_data(0), i * 8);
        let b = read_u64_le(out.col_data(1), i * 8);
        let m = read_i64_le(out.col_data(2), i * 8);
        seen.insert((a, b), m);
    }
    assert_eq!(
        seen, reference,
        "wide AVI per-group MIN must match the in-test trace-scan reference"
    );
}

// Single U128 group column: composite = g(16) ++ av(8) = 24 bytes. Confirms a
// 16-byte group column drives the wide seek and that two groups never share a
// bucket.
#[test]
fn avi_wide_single_u128_group_distinct() {
    use super::super::util::encode_ordered;
    use crate::storage::ReadCursor;
    use std::rc::Rc;

    let in_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),  // pk
            SchemaColumn::new(type_code::U128, 0), // g (group)
            SchemaColumn::new(type_code::I64, 0),  // val
        ],
        &[0],
    );
    // A single U128 group column uses a natural PK: the group value IS the
    // output PK; the only payload column is the aggregate.
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0), // PK = group value g
            SchemaColumn::new(type_code::I64, 1),  // MIN
        ],
        &[0],
    );
    let avi_schema = crate::ops::index::make_avi_schema(&in_schema, &[1u32]);
    assert_eq!(avi_schema.pk_stride(), 25, "16 + 1 + 8");

    // Two groups, large U128 values (high 64 bits set) → exercises the full
    // 16-byte compare. Group g1 MIN=10, g2 MIN=20.
    let g1: u128 = (1u128 << 120) | 7;
    let g2: u128 = (1u128 << 120) | 9; // shares top bytes with g1, differs low
    let groups = [(g1, 10i64), (g2, 20i64)];

    let delta = {
        let mut b = Batch::with_schema(in_schema, 2);
        for (i, &(g, _)) in groups.iter().enumerate() {
            b.extend_pk(i as u128 + 1);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(in_schema.try_payload_idx(1).unwrap(), &g.to_le_bytes());
            b.extend_col(in_schema.try_payload_idx(2).unwrap(), &999i64.to_le_bytes());
            b.count += 1;
        }
        b
    };

    let avi_batch = {
        let mut b = Batch::with_schema(avi_schema, 2);
        // sorted ascending by g (both share high bytes; g1 < g2 by low byte).
        for &(g, m) in &groups {
            let mut key = [0u8; 25];
            key[0..16].copy_from_slice(&g.to_le_bytes());
            let av = encode_ordered(&m.to_le_bytes(), type_code::I64, false);
            key[16] = 0; // ordinal 0 (single MIN aggregate)
            key[17..25].copy_from_slice(&av.to_be_bytes());
            b.extend_pk_bytes(&key);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.count += 1;
        }
        b.set_layout_unchecked(Layout::Consolidated);
        b
    };

    let empty_out = Rc::new(Batch::empty_with_schema(&out_schema));
    let mut to_ch = ReadCursor::from_owned(&[empty_out], out_schema);
    let mut avi_ch = ReadCursor::from_owned(&[Rc::new(avi_batch)], avi_schema);

    let agg = AggDescriptor {
        col_idx: 2,
        agg_op: AggOp::Min,
        col_type_code: TypeCode::I64,
        _pad: [0; 2],
    };

    let (out, _) = op_reduce(
        &delta,
        None,
        &mut to_ch,
        &in_schema,
        &out_schema,
        &[1u32],
        &[agg],
        Some(&mut avi_ch),
        None,
        None,
        false,
        false,
    );

    assert_eq!(out.count, 2);
    for i in 0..out.count {
        let g = out.get_pk(i);
        let m = read_i64_le(out.col_data(0), i * 8);
        let expected = if g == g1 {
            10
        } else if g == g2 {
            20
        } else {
            panic!("group {g}")
        };
        assert_eq!(m, expected, "U128 group {g} must resolve its own MIN");
    }
}

// Mixed signed/unsigned wide key: GROUP BY (a I64, b U64), composite 24. The
// negative `a` group must sort before the positive one under the per-column
// signed comparison in compare_pk_bytes; a seek for each must land on its own
// group. Guards that make_avi_schema preserves each column's type_code.
#[test]
fn avi_wide_mixed_signed_unsigned_key() {
    use super::super::util::encode_ordered;
    use crate::storage::ReadCursor;
    use std::rc::Rc;

    let in_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0), // pk
            SchemaColumn::new(type_code::I64, 0), // a (signed group)
            SchemaColumn::new(type_code::U64, 0), // b (group)
            SchemaColumn::new(type_code::I64, 0), // val
        ],
        &[0],
    );
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 1),
        ],
        &[0],
    );
    let avi_schema = crate::ops::index::make_avi_schema(&in_schema, &[1u32, 2u32]);
    assert_eq!(avi_schema.pk_stride(), 25);

    // Groups: (-5, 10) MIN=100, (-5, 11) MIN=50, (3, 10) MIN=200.
    // Signed order on column a: -5 < 3, so the (-5,*) groups precede (3,*).
    let groups: [(i64, u64, i64); 3] = [(-5, 10, 100), (-5, 11, 50), (3, 10, 200)];

    let delta = {
        let mut b = Batch::with_schema(in_schema, 3);
        for (i, &(a, bb, _)) in groups.iter().enumerate() {
            b.extend_pk(i as u128 + 1);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(in_schema.try_payload_idx(1).unwrap(), &a.to_le_bytes());
            b.extend_col(in_schema.try_payload_idx(2).unwrap(), &bb.to_le_bytes());
            b.extend_col(in_schema.try_payload_idx(3).unwrap(), &777i64.to_le_bytes());
            b.count += 1;
        }
        b
    };

    // AVI rows in memcmp (compare_pk_bytes) order of the composite key. The
    // group prefix is point-probed, never numeric-range-scanned, so a signed
    // column stored little-endian sorts by bytes, not by signed value — which
    // is what production's ingest produces.
    let avi_batch = {
        let mut b = Batch::with_schema(avi_schema, 3);
        let mut keys: Vec<[u8; 25]> = groups
            .iter()
            .map(|&(a, bb, m)| {
                let mut key = [0u8; 25];
                key[0..8].copy_from_slice(&a.to_le_bytes());
                key[8..16].copy_from_slice(&bb.to_le_bytes());
                let av = encode_ordered(&m.to_le_bytes(), type_code::I64, false);
                key[16] = 0; // ordinal 0 (single MIN aggregate)
                key[17..25].copy_from_slice(&av.to_be_bytes());
                key
            })
            .collect();
        keys.sort();
        for key in &keys {
            b.extend_pk_bytes(key);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.count += 1;
        }
        b.set_layout_unchecked(Layout::Consolidated);
        b
    };

    let empty_out = Rc::new(Batch::empty_with_schema(&out_schema));
    let mut to_ch = ReadCursor::from_owned(&[empty_out], out_schema);
    let mut avi_ch = ReadCursor::from_owned(&[Rc::new(avi_batch)], avi_schema);

    let agg = AggDescriptor {
        col_idx: 3,
        agg_op: AggOp::Min,
        col_type_code: TypeCode::I64,
        _pad: [0; 2],
    };

    let (out, _) = op_reduce(
        &delta,
        None,
        &mut to_ch,
        &in_schema,
        &out_schema,
        &[1u32, 2u32],
        &[agg],
        Some(&mut avi_ch),
        None,
        None,
        false,
        false,
    );

    assert_eq!(out.count, 3);
    for i in 0..out.count {
        let a = read_i64_le(out.col_data(0), i * 8);
        let bb = read_u64_le(out.col_data(1), i * 8);
        let m = read_i64_le(out.col_data(2), i * 8);
        let expected = match (a, bb) {
            (-5, 10) => 100,
            (-5, 11) => 50,
            (3, 10) => 200,
            _ => panic!("unexpected group ({a}, {bb})"),
        };
        assert_eq!(m, expected, "signed-key group ({a},{bb}) must resolve its own MIN");
    }
}

// Wide prefix collision: two groups whose composite keys share their first 16
// bytes (a, b) but differ in bytes 17–24 (c). The seek must keep them distinct
// and never return the colliding group's value — even though the decoy holds a
// smaller value that would win a 16-byte-prefix-only match. GROUP BY
// (a, b, c) → composite a(8)++b(8)++c(8)++av(8) = 32 bytes.
#[test]
fn avi_wide_prefix_collision_distinct_groups() {
    use super::super::util::encode_ordered;
    use crate::storage::ReadCursor;
    use std::rc::Rc;

    let in_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0), // pk
            SchemaColumn::new(type_code::U64, 0), // a
            SchemaColumn::new(type_code::U64, 0), // b
            SchemaColumn::new(type_code::U64, 0), // c
            SchemaColumn::new(type_code::I64, 0), // val
        ],
        &[0],
    );
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 1),
        ],
        &[0],
    );
    let avi_schema = crate::ops::index::make_avi_schema(&in_schema, &[1u32, 2u32, 3u32]);
    assert_eq!(avi_schema.pk_stride(), 33, "8 + 8 + 8 + 1 + 8");

    // Both groups share (a, b) = (1, 2); they differ only in c.
    // Group (1,2,3) MIN=100; decoy (1,2,4) MIN=5 (smaller — must NOT leak).
    let groups: [(u64, u64, u64, i64); 2] = [(1, 2, 3, 100), (1, 2, 4, 5)];

    // delta inserts only group (1,2,3); its val is a decoy 999.
    let delta = {
        let mut b = Batch::with_schema(in_schema, 1);
        b.extend_pk(1u128);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(in_schema.try_payload_idx(1).unwrap(), &1u64.to_le_bytes());
        b.extend_col(in_schema.try_payload_idx(2).unwrap(), &2u64.to_le_bytes());
        b.extend_col(in_schema.try_payload_idx(3).unwrap(), &3u64.to_le_bytes());
        b.extend_col(in_schema.try_payload_idx(4).unwrap(), &999i64.to_le_bytes());
        b.count += 1;
        b
    };

    let avi_batch = {
        let mut b = Batch::with_schema(avi_schema, 2);
        // sorted by (a,b,c): (1,2,3) before (1,2,4).
        for &(a, bb, c, m) in &groups {
            let mut key = [0u8; 33];
            key[0..8].copy_from_slice(&a.to_le_bytes());
            key[8..16].copy_from_slice(&bb.to_le_bytes());
            key[16..24].copy_from_slice(&c.to_le_bytes());
            let av = encode_ordered(&m.to_le_bytes(), type_code::I64, false);
            key[24] = 0; // ordinal 0 (single MIN aggregate)
            key[25..33].copy_from_slice(&av.to_be_bytes());
            b.extend_pk_bytes(&key);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.count += 1;
        }
        b.set_layout_unchecked(Layout::Consolidated);
        b
    };

    let empty_out = Rc::new(Batch::empty_with_schema(&out_schema));
    let mut to_ch = ReadCursor::from_owned(&[empty_out], out_schema);
    let mut avi_ch = ReadCursor::from_owned(&[Rc::new(avi_batch)], avi_schema);

    let agg = AggDescriptor {
        col_idx: 4,
        agg_op: AggOp::Min,
        col_type_code: TypeCode::I64,
        _pad: [0; 2],
    };

    let (out, _) = op_reduce(
        &delta,
        None,
        &mut to_ch,
        &in_schema,
        &out_schema,
        &[1u32, 2u32, 3u32],
        &[agg],
        Some(&mut avi_ch),
        None,
        None,
        false,
        false,
    );

    assert_eq!(out.count, 1, "delta touched only group (1,2,3)");
    let c = read_u64_le(out.col_data(2), 0);
    let m = read_i64_le(out.col_data(3), 0);
    assert_eq!(c, 3, "must resolve group (1,2,3)");
    assert_eq!(
        m, 100,
        "the 16-byte-prefix-sharing decoy (1,2,4)'s smaller value must not leak"
    );
}

// Wide retraction: retracting a wide-key group's current MIN must return the
// next-best from the index — the wide analog of
// `avi_multi_col_retraction_returns_next_extremum`. A prefix-colliding decoy
// group (sharing the first 16 bytes) must not be matched.
#[test]
fn avi_wide_retraction_returns_next_extremum() {
    use super::super::util::encode_ordered;
    use crate::storage::ReadCursor;
    use std::rc::Rc;

    // GROUP BY (a U64, b U64); composite a(8)++b(8)++av(8) = 24 bytes.
    let in_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    );
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 1),
        ],
        &[0],
    );
    let avi_schema = crate::ops::index::make_avi_schema(&in_schema, &[1u32, 2u32]);
    assert_eq!(avi_schema.pk_stride(), 25);

    // Retract group (1<<40, 2)'s current MIN (val=5).
    let ga: u64 = 1 << 40;
    let gb: u64 = 2;
    let delta = {
        let mut b = Batch::with_schema(in_schema, 1);
        b.extend_pk(1u128);
        b.extend_weight(&(-1i64).to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(in_schema.try_payload_idx(1).unwrap(), &ga.to_le_bytes());
        b.extend_col(in_schema.try_payload_idx(2).unwrap(), &gb.to_le_bytes());
        b.extend_col(in_schema.try_payload_idx(3).unwrap(), &5i64.to_le_bytes());
        b.count += 1;
        b.set_layout_unchecked(Layout::Consolidated);
        b
    };
    let group_key = extract_group_key(&delta.as_mem_batch(), 0, &in_schema, &[1u32, 2u32]);

    // AVI post-state: group (ga, gb) surviving values {9, 15}; a decoy group
    // (ga, gb+1) sharing the first 8 bytes holds a smaller 1 that must not win.
    let avi_batch = {
        let mut b = Batch::with_schema(avi_schema, 3);
        let put = |b: &mut Batch, a: u64, bb: u64, m: i64| {
            let mut key = [0u8; 25];
            key[0..8].copy_from_slice(&a.to_le_bytes());
            key[8..16].copy_from_slice(&bb.to_le_bytes());
            let av = encode_ordered(&m.to_le_bytes(), type_code::I64, false);
            key[16] = 0; // ordinal 0 (single MIN aggregate)
            key[17..25].copy_from_slice(&av.to_be_bytes());
            b.extend_pk_bytes(&key);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.count += 1;
        };
        // sorted by (a, b, av): (ga,gb,9),(ga,gb,15),(ga,gb+1,1).
        put(&mut b, ga, gb, 9);
        put(&mut b, ga, gb, 15);
        put(&mut b, ga, gb + 1, 1);
        b.set_layout_unchecked(Layout::Consolidated);
        b
    };

    let to_batch = {
        let mut b = Batch::with_schema(out_schema, 1);
        b.extend_pk(group_key);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &ga.to_le_bytes());
        b.extend_col(1, &gb.to_le_bytes());
        b.extend_col(2, &5i64.to_le_bytes());
        b.count += 1;
        b.set_layout_unchecked(Layout::Consolidated);
        Rc::new(b)
    };

    let mut to_ch = ReadCursor::from_owned(&[to_batch], out_schema);
    let mut avi_ch = ReadCursor::from_owned(&[Rc::new(avi_batch)], avi_schema);

    let agg = AggDescriptor {
        col_idx: 3,
        agg_op: AggOp::Min,
        col_type_code: TypeCode::I64,
        _pad: [0; 2],
    };

    let (out, _) = op_reduce(
        &delta,
        None,
        &mut to_ch,
        &in_schema,
        &out_schema,
        &[1u32, 2u32],
        &[agg],
        Some(&mut avi_ch),
        None,
        None,
        false,
        false,
    );

    let mut retracted = None;
    let mut inserted = None;
    for i in 0..out.count {
        let w = out.get_weight(i);
        let v = read_i64_le(out.col_data(2), i * 8);
        if w < 0 {
            retracted = Some(v);
        } else {
            inserted = Some(v);
        }
    }
    assert_eq!(retracted, Some(5), "must retract the stale wide-key MIN");
    assert_eq!(
        inserted,
        Some(9),
        "wide AVI must return group (ga,gb)'s next MIN (9), not the decoy's 1"
    );
}

// =======================================================================
// Reduce-path correctness regressions: a value-independent aggregate (COUNT)
// must not read a wide PK source column; the AVI order-encoded value must be
// byte-ordered so incremental MIN/MAX reads the true extremal; an F32 AVI seed
// must promote to F64 bits; and the group-by-PK path must handle a compound PK
// of any width.
// =======================================================================

// Bug 1: COUNT(*) is compiled with a placeholder arg column index 0. When
// column 0 is a 16-byte UUID PK, the old step_from_batch decoded the PK column
// into an 8-byte scratch buffer before the per-op match — `pk_le_buf[..16]` was
// out of range and the worker crashed. COUNT is value-independent and must
// return before touching the column.
#[test]
fn count_accumulator_over_uuid_pk_does_not_panic() {
    let schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::UUID, 0), // 16-byte PK
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    );
    let mut b = Batch::with_schema(schema, 2);
    for pk in [1u128, 2] {
        b.extend_pk(pk);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(schema.try_payload_idx(1).unwrap(), &0i64.to_le_bytes());
        b.count += 1;
    }
    let desc = AggDescriptor {
        col_idx: 0,
        agg_op: AggOp::Count,
        col_type_code: TypeCode::UUID,
        _pad: [0; 2],
    };
    let mut acc = Accumulator::new(&desc, &schema);
    let mb = b.as_mem_batch();
    acc.step_from_batch(&mb, 0, 1);
    acc.step_from_batch(&mb, 1, 1);
    assert_eq!(
        acc.get_value_bits() as i64,
        2,
        "COUNT over a UUID PK column must count rows"
    );
}

// Populate a fresh ephemeral AVI table from `deltas` through the production
// `op_integrate_with_indexes`, then read the extreme of delta[0]-row-0's group
// back through the production `apply_agg_from_value_index` seek. `col_idx` is
// the aggregate source column (PK or payload). Shared by the AVI full-path
// tests below.
fn avi_read_extreme(
    in_schema: &SchemaDescriptor,
    group_by: &[u32],
    col_idx: u32,
    deltas: &[&Batch],
    for_max: bool,
) -> i64 {
    use super::super::util::GroupKeyExtractor;
    use crate::ops::index::{make_avi_schema, op_integrate_with_indexes, AviDesc};
    use crate::storage::Table;

    // The aggregate's type is the source column's type.
    let tc_enum = TypeCode::from_validated_u8(in_schema.columns[col_idx as usize].type_code);
    let avi_schema = make_avi_schema(in_schema, group_by);
    let tmp = tempfile::tempdir().unwrap();
    let mut avi_t = Table::new(
        tmp.path().to_str().unwrap(),
        avi_schema,
        0,
        1 << 20,
        crate::storage::RecoverySource::Rederive,
    )
    .unwrap();
    let agg = AggDescriptor {
        col_idx,
        agg_op: if for_max { AggOp::Max } else { AggOp::Min },
        col_type_code: tc_enum,
        _pad: [0; 2],
    };
    let aggs = [agg];
    let avi = AviDesc {
        table: &mut avi_t as *mut Table,
        group_by_cols: group_by,
        aggs: &aggs,
    };
    // Each op_integrate_with_indexes call is a separate AVI ingest; the cursor's
    // two-tier consolidation sums weights across ingests, so a retracted extreme
    // (net-zero) is skipped by seek_first_positive_with_prefix.
    for d in deltas {
        op_integrate_with_indexes(d, None, in_schema, Some(&avi)).unwrap();
    }

    let mut ch = avi_t.open_cursor();
    let extractor = GroupKeyExtractor::new(in_schema, group_by);
    let mut gk = [0u8; crate::schema::MAX_PK_BYTES];
    extractor.gather(&deltas[0].as_mem_batch(), 0, &mut gk);
    gk[extractor.stride] = 0; // ordinal 0 (single aggregate)
    let mut acc = Accumulator::new(&agg, in_schema);
    assert!(
        apply_agg_from_value_index(&mut ch, &gk[..extractor.stride + 1], for_max, &mut acc),
        "AVI seek must find the probed group",
    );
    acc.get_value_bits() as i64
}

// Bug 3: the order-encoded aggregate value must be serialized big-endian so the
// index's lexicographic byte ordering matches numeric order. With three values
// in one group whose extremes differ above the low byte ({101, 111, -5}), a
// little-endian serialization sorts 101 first and reports it as the MIN; -5 is
// never seen.
#[test]
fn avi_full_path_min_max_across_high_byte() {
    let in_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0), // pk
            SchemaColumn::new(type_code::U64, 0), // g (group)
            SchemaColumn::new(type_code::I64, 0), // val (agg)
        ],
        &[0],
    );
    // One group g=5 with values {101, 111, -5}.
    let delta = {
        let mut b = Batch::with_schema(in_schema, 3);
        for (pk, v) in [(1u128, 101i64), (2, 111), (3, -5)] {
            b.extend_pk(pk);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(in_schema.try_payload_idx(1).unwrap(), &5u64.to_le_bytes());
            b.extend_col(in_schema.try_payload_idx(2).unwrap(), &v.to_le_bytes());
            b.count += 1;
        }
        b
    };

    assert_eq!(
        avi_read_extreme(&in_schema, &[1], 2, &[&delta], false),
        -5,
        "MIN across the high-byte boundary must be -5, not 101"
    );
    assert_eq!(
        avi_read_extreme(&in_schema, &[1], 2, &[&delta], true),
        111,
        "MAX must be 111"
    );
}

/// Delta over a two-column PK `[U64 a (pk), <int> b (pk)]`, no payload: one row
/// per `(b, weight)` with a=5. `opk_pk` truncates each u128 to the column
/// width, so a negative `b as u128` still packs the correct two's-complement
/// LE bytes.
fn pk_ab_delta(schema: &SchemaDescriptor, rows: &[(i128, i64)]) -> Batch {
    let mut b = Batch::with_schema(*schema, rows.len().max(1));
    for &(bv, w) in rows {
        b.extend_pk_opk(schema, &[5u128, bv as u128]);
        b.extend_weight(&w.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.count += 1;
    }
    b.set_layout_unchecked(Layout::Consolidated);
    b
}

// A signed PK-source aggregate end-to-end: PK = (a:U64, b:I64), GROUP BY a,
// MIN/MAX(b). The population OPK-decodes b before order-encoding; without the
// decode the byte-swapped image reports the wrong extreme (101 instead of -5 for
// MIN) and the retract-at-extreme walk advances to the wrong next value.
#[test]
fn avi_full_path_pk_source_signed_min_max() {
    let in_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0, 1],
    );
    // Group a=5, b ∈ {-5, 1, 256, 100}: extremes straddle the sign (-5) and the
    // high byte (256).
    let delta = pk_ab_delta(&in_schema, &[(-5, 1), (1, 1), (256, 1), (100, 1)]);
    let read = |deltas: &[&Batch], for_max: bool| avi_read_extreme(&in_schema, &[0], 1, deltas, for_max);

    // Insert epoch.
    assert_eq!(
        read(&[&delta], false),
        -5,
        "MIN across sign/high-byte boundary must be -5"
    );
    assert_eq!(read(&[&delta], true), 256, "MAX across the high byte must be 256");

    // Retract-at-extreme: retracting the current MIN advances it to the next
    // value; retracting the current MAX falls it back.
    let retract_min = pk_ab_delta(&in_schema, &[(-5, -1)]);
    assert_eq!(
        read(&[&delta, &retract_min], false),
        1,
        "after retracting -5, MIN advances to 1"
    );
    let retract_max = pk_ab_delta(&in_schema, &[(256, -1)]);
    assert_eq!(
        read(&[&delta, &retract_max], true),
        100,
        "after retracting 256, MAX falls to 100"
    );
}

// The unsigned high-byte-boundary case at the reduce full-path level: PK =
// (a:U64, b:U64), b ∈ {1, 256, 100}. 1 (0x0001) and 256 (0x0100) byte-swap into
// each other, so a population that skips the OPK decode reports 256 as the MIN.
#[test]
fn avi_full_path_pk_source_unsigned_high_byte() {
    let in_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U64, 0),
        ],
        &[0, 1],
    );
    let delta = pk_ab_delta(&in_schema, &[(1, 1), (256, 1), (100, 1)]);
    let read = |deltas: &[&Batch], for_max: bool| avi_read_extreme(&in_schema, &[0], 1, deltas, for_max);

    assert_eq!(read(&[&delta], false), 1, "MIN must be 1, not the byte-swapped 256");
    assert_eq!(read(&[&delta], true), 256, "MAX must be 256");

    // Retract the MAX (256); MAX falls back to 100.
    let retract_max = pk_ab_delta(&in_schema, &[(256, -1)]);
    assert_eq!(
        read(&[&delta, &retract_max], true),
        100,
        "after retracting 256, MAX falls to 100"
    );
    // Retract the MIN (1); MIN advances to 100.
    let retract_min = pk_ab_delta(&in_schema, &[(1, -1)]);
    assert_eq!(
        read(&[&delta, &retract_min], false),
        100,
        "after retracting 1, MIN advances to 100"
    );
}

// Bug 4: a float aggregate's accumulator and output column are F64, so the AVI
// seed for an F32 source must promote to F64 bits — not zero-extend the raw
// 32-bit IEEE bits, which an F64 reader interprets as a tiny denormal.
#[test]
fn avi_f32_seed_promotes_to_f64_bits() {
    use super::super::util::{decode_ordered, encode_ordered};
    for v in [1.5f32, -2.25, 0.0, 1.0e30] {
        let enc = encode_ordered(&v.to_le_bytes(), type_code::F32, false);
        let bits = decode_ordered(enc, TypeCode::F32, false);
        assert_eq!(
            bits,
            f64::to_bits(v as f64),
            "F32 AVI seed must be the F64 bits of (f32 as f64) for v={v}",
        );
        assert_eq!(f64::from_bits(bits), v as f64);
    }
}

// Bug 5: a compound PK with pk_stride > 16 (two U128 columns = stride 32) whose
// GROUP BY is exactly the PK must take the unified group-by-PK path — group
// membership tested on the full PK byte window, argsorted by canonical PK
// order, emitting one weight-folded row per distinct PK. Before the
// unification the {8,16}-stride gate forced this onto the slow per-column path.
#[test]
fn reduce_wide_compound_pk_group_by_pk_counts_per_pk() {
    use crate::storage::ReadCursor;
    use std::rc::Rc;

    let in_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0), // pk col 0
            SchemaColumn::new(type_code::U128, 0), // pk col 1
        ],
        &[0, 1],
    );
    assert!(in_schema.pk_stride() > 16, "compound U128+U128 PK must exceed 16 bytes");

    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::I64, 1), // COUNT
        ],
        &[0, 1],
    );

    // Two distinct compound PKs, folded and (PK,payload)-sorted: (1,1) at weight
    // 2 → cnt 2, (1,2) at weight 1 → cnt 1. Flagged consolidated (genuinely
    // folded, ghost-free, sorted) but with `sorted` left unset, so op_reduce
    // re-derives canonical order through the argsort_pk_canonical branch — the
    // wide compound-PK path under test.
    let delta = {
        let mut b = Batch::with_schema(in_schema, 2);
        for (a, c, w) in [(1u128, 1u128, 2i64), (1, 2, 1)] {
            b.extend_pk_opk(&in_schema, &[a, c]);
            b.extend_weight(&w.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.count += 1;
        }
        b.set_layout_unchecked(Layout::Consolidated);
        b
    };

    let to_batch = Rc::new(Batch::empty_with_schema(&out_schema));
    let mut to_ch = ReadCursor::from_owned(&[to_batch], out_schema);

    let agg = AggDescriptor {
        col_idx: 0,
        agg_op: AggOp::Count,
        col_type_code: TypeCode::U128,
        _pad: [0; 2],
    };

    let (out, _) = op_reduce(
        &delta,
        None,
        &mut to_ch,
        &in_schema,
        &out_schema,
        &[0u32, 1u32],
        &[agg],
        None,
        None,
        None,
        false,
        false,
    );

    // op_reduce ships an honest unconsolidated delta — it no longer certifies the
    // flags even on the group-by-PK fast path.
    assert!(
        !out.is_sorted() && !out.is_consolidated(),
        "reduce output is an unconsolidated delta"
    );
    // The argsort_pk_canonical branch still physically orders the wide compound-PK
    // output: (1,1) (cnt 2) precedes (1,2) (cnt 1). Read counts in physical row
    // order to pin that canonical ordering (the branch under test).
    let counts: Vec<i64> = (0..out.count)
        .filter(|&i| out.get_weight(i) > 0)
        .map(|i| read_i64_le(out.col_data(0), i * 8))
        .collect();
    assert_eq!(
        counts,
        vec![2, 1],
        "two distinct compound PKs in canonical order: (1,1)→2 then (1,2)→1"
    );
}

// -----------------------------------------------------------------------
// Non-linear REDUCE fallback: single trace scan (O(trace + delta)).
//
// The fallback path is reached when all hold: non-linear aggregate,
// no combined value index (a non-AVI-eligible MIN/MAX, e.g. over a
// German string), group_by is not the PK. The tests here pin
// three properties: (1) hash correctness (extract_group_key_cursor must
// produce byte-identical results to extract_group_key for the same row),
// (2) aggregate correctness across INSERT/DELETE ticks, and (3) the
// trace is scanned at most once per tick (REWIND_CALLS ≤ 1).
// -----------------------------------------------------------------------

/// Verifies that `extract_group_key_cursor` and `extract_group_key` return
/// identical 128-bit keys for every row in a sorted batch, for the given
/// group columns. A divergence would silently merge or split groups (wrong
/// MIN/MAX results).
fn assert_group_key_cursor_matches_batch(b: &Batch, schema: &SchemaDescriptor, group_by_cols: &[u32]) {
    use super::super::util::{extract_group_key, extract_group_key_cursor};
    use crate::storage::ReadCursor;
    use std::rc::Rc;

    let rc = Rc::new(unsafe {
        // Batch has no Clone; transmute a duplicate via raw read for test use only.
        // Safety: we only read the batch through the Rc reference; the original `b`
        // is not mutated or freed while the Rc lives (both are on the stack here).
        //
        // The proper way is to build the batch into an Rc from the start, but the
        // helper receives a `&Batch` from the caller. We use ptr::read to produce an
        // owned value without running a destructor on the source.
        std::ptr::read(b as *const Batch)
    });
    let mb = rc.as_mem_batch();
    let mut ch = ReadCursor::from_owned(&[Rc::clone(&rc)], *schema);
    let cursor = &mut ch;

    for row in 0..b.count {
        assert!(cursor.valid, "cursor must be valid at row {row}");
        let batch_key = extract_group_key(&mb, row, schema, group_by_cols);
        let cursor_key = extract_group_key_cursor(cursor, schema, group_by_cols);
        assert_eq!(
            batch_key, cursor_key,
            "group key mismatch at row {row}: batch={batch_key:#034x} cursor={cursor_key:#034x}",
        );
        cursor.advance();
    }
    // Prevent rc from being dropped before mb (the MemBatch holds a raw pointer into it).
    std::mem::forget(rc);
}

#[test]
fn extract_group_key_cursor_matches_batch() {
    // Single U64 PK, group by PK column (single-PK fast path).
    {
        let schema = SchemaDescriptor::new(&[SchemaColumn::new(type_code::U64, 0)], &[0]);
        let mut b = Batch::with_schema(schema, 3);
        for pk in [1u64, 5, 100] {
            b.extend_pk(pk as u128);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.count += 1;
        }
        b.set_layout_unchecked(Layout::Consolidated);
        assert_group_key_cursor_matches_batch(&b, &schema, &[0u32]);
    }

    // Single non-nullable I64 payload, group by payload col (single-col fast path).
    {
        let schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0],
        );
        let pi = schema.try_payload_idx(1).unwrap();
        let mut b = Batch::with_schema(schema, 4);
        for (pk, grp) in [(1u64, -100i64), (2, 0), (3, 42), (4, i64::MAX)] {
            b.extend_pk(pk as u128);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(pi, &grp.to_le_bytes());
            b.count += 1;
        }
        b.set_layout_unchecked(Layout::Consolidated);
        assert_group_key_cursor_matches_batch(&b, &schema, &[1u32]);
    }

    // Single nullable I64, group by nullable col — covers NULL in the hash loop.
    {
        let schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::I64, 1), // nullable
            ],
            &[0],
        );
        let pi = schema.try_payload_idx(1).unwrap();
        let mut b = Batch::with_schema(schema, 4);
        // non-null rows
        for (pk, grp, null_bit) in [(1u64, 10i64, 0u64), (2, 20, 0), (3, 30, 0)] {
            b.extend_pk(pk as u128);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&(null_bit << pi as u64).to_le_bytes());
            b.extend_col(pi, &grp.to_le_bytes());
            b.count += 1;
        }
        // NULL row: null bit set; payload bytes irrelevant
        b.extend_pk(4u128);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&(1u64 << pi).to_le_bytes()); // pi-th bit = null
        b.extend_col(pi, &0i64.to_le_bytes());
        b.count += 1;
        b.set_layout_unchecked(Layout::Consolidated);
        assert_group_key_cursor_matches_batch(&b, &schema, &[1u32]);
    }

    // Single STRING payload, group by STRING col — covers string hash path and empty string.
    {
        let schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::STRING, 0), // non-nullable STRING
            ],
            &[0],
        );
        let pi = schema.try_payload_idx(1).unwrap();
        let mut b = Batch::with_schema(schema, 3);
        for (pk, s) in [(1u64, ""), (2, "hello"), (3, "zebra")] {
            b.extend_pk(pk as u128);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            let gs = encode_german_string(s.as_bytes(), &mut b.blob);
            b.extend_col(pi, &gs);
            b.count += 1;
        }
        b.set_layout_unchecked(Layout::Consolidated);
        assert_group_key_cursor_matches_batch(&b, &schema, &[1u32]);
    }

    // U128 PK, group by PK column.
    {
        let schema = SchemaDescriptor::new(&[SchemaColumn::new(type_code::U128, 0)], &[0]);
        let mut b = Batch::with_schema(schema, 2);
        for pk in [0u128, u128::MAX] {
            b.extend_pk(pk);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.count += 1;
        }
        b.set_layout_unchecked(Layout::Consolidated);
        assert_group_key_cursor_matches_batch(&b, &schema, &[0u32]);
    }

    // Multi-column key: (I64, I64), group by [col1, col2].
    {
        let schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::I64, 0),
                SchemaColumn::new(type_code::I64, 1), // nullable second column
            ],
            &[0],
        );
        let pi0 = schema.try_payload_idx(1).unwrap();
        let pi1 = schema.try_payload_idx(2).unwrap();
        let mut b = Batch::with_schema(schema, 4);
        // (non-null, non-null), (non-null, NULL), (non-null, non-null), (non-null, non-null)
        let rows: &[(u64, i64, Option<i64>)] = &[
            (1, 10, Some(100)),
            (2, 10, None),
            (3, 20, Some(100)),
            (4, 20, Some(200)),
        ];
        for &(pk, c1, c2) in rows {
            b.extend_pk(pk as u128);
            b.extend_weight(&1i64.to_le_bytes());
            let null_bit = if c2.is_none() { 1u64 << pi1 } else { 0 };
            b.extend_null_bmp(&null_bit.to_le_bytes());
            b.extend_col(pi0, &c1.to_le_bytes());
            b.extend_col(pi1, &c2.unwrap_or(0).to_le_bytes());
            b.count += 1;
        }
        b.set_layout_unchecked(Layout::Consolidated);
        assert_group_key_cursor_matches_batch(&b, &schema, &[1u32, 2u32]);
    }
}

/// Read (grp_i64, min_i64) pairs from a fallback-style output (U128 pk | I64 grp | I64 min).
fn read_grp_min_pairs(out: &Batch) -> Vec<(i64, i64)> {
    let grp_data = out.col_data(0);
    let min_data = out.col_data(1);
    let mut pairs: Vec<(i64, i64)> = (0..out.count)
        .filter(|&i| out.get_weight(i) > 0)
        .map(|i| {
            let g = read_i64_le(grp_data, i * 8);
            let m = read_i64_le(min_data, i * 8);
            (g, m)
        })
        .collect();
    pairs.sort_unstable();
    pairs
}

/// Run the non-linear REDUCE fallback correctness test.
///
/// Schema: U64 pk | I64 grp (nullable) | I64 val. MIN on val, grouped by grp.
/// No GI, no AVI → triggers the single-scan fallback path.
///
/// Two ticks:
///   Tick 1: insert `tick1_rows` → verify MIN per group.
///   Tick 2: apply `delta_rows` with trace_in from tick 1 → verify updated MIN.
fn run_fallback_min_i64_grp(
    tick1_rows: &[GrpValRow],
    delta_rows: &[GrpValRow],
    expected_tick1: &mut Vec<(i64, i64)>, // sorted (grp, min) pairs after tick 1
    expected_tick2: &mut Vec<(i64, i64)>, // sorted (grp, min) pairs after tick 2
) {
    use crate::storage::ReadCursor;
    use std::rc::Rc;

    let in_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 1), // nullable grp
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    );
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::I64, 1), // nullable grp (carried through)
            SchemaColumn::new(type_code::I64, 1), // nullable min
        ],
        &[0],
    );

    let agg = AggDescriptor {
        col_idx: 2,
        agg_op: AggOp::Min,
        col_type_code: TypeCode::I64,
        _pad: [0; 2],
    };

    // --- Tick 1: empty trace_in, empty trace_out ---
    let delta1 = build_grp_val_delta(&in_schema, tick1_rows);
    let empty_out = Rc::new(Batch::empty_with_schema(&out_schema));
    let mut to_ch1 = ReadCursor::from_owned(&[empty_out], out_schema);

    let (out1, _) = op_reduce(
        &delta1,
        None,
        &mut to_ch1,
        &in_schema,
        &out_schema,
        &[1u32],
        &[agg],
        None,
        None,
        None,
        false,
        false,
    );

    let got1 = read_grp_min_pairs(&out1);
    expected_tick1.sort_unstable();
    assert_eq!(got1, *expected_tick1, "tick 1 MIN mismatch");

    // --- Tick 2: trace_in = tick1 input, trace_out = tick1 output ---
    let ti_batch = Rc::new(build_grp_val_delta(&in_schema, tick1_rows));
    let to_batch = Rc::new(out1);
    let delta2 = build_grp_val_delta(&in_schema, delta_rows);

    let mut ti_ch2 = ReadCursor::from_owned(&[ti_batch], in_schema);
    let mut to_ch2 = ReadCursor::from_owned(&[to_batch], out_schema);

    let (out2, _) = op_reduce(
        &delta2,
        Some(&mut ti_ch2),
        &mut to_ch2,
        &in_schema,
        &out_schema,
        &[1u32],
        &[agg],
        None,
        None,
        None,
        false,
        false,
    );

    let grp_data = out2.col_data(0);
    let min_data = out2.col_data(1);

    // After tick 2, accumulate tick1 + delta. The net MIN per group is what
    // tick1 produced (already verified) plus the new delta's contribution.
    // We build expected from tick1 final values + delta applied.
    let mut final_rows: std::collections::BTreeMap<i64, Vec<i64>> = std::collections::BTreeMap::new();
    for &(_, grp, val, w) in tick1_rows.iter().chain(delta_rows.iter()) {
        let Some(grp) = grp else {
            continue; // NULL group: skip for simplicity (tested separately)
        };
        if w > 0 {
            final_rows.entry(grp).or_default().push(val);
        } else {
            let v = final_rows.entry(grp).or_default();
            v.retain(|&x| x != val); // remove one occurrence
        }
    }
    let mut expected2: Vec<(i64, i64)> = final_rows
        .iter()
        .filter(|(_, vals)| !vals.is_empty())
        .map(|(&g, vals)| (g, *vals.iter().min().unwrap()))
        .collect();
    expected2.sort_unstable();
    expected_tick2.sort_unstable();
    assert_eq!(expected2, *expected_tick2, "tick 2 expected mismatch in test setup");

    // The live MIN values in out2: start from tick1 result and apply net changes.
    let mut live: std::collections::BTreeMap<i64, i64> = got1.iter().cloned().collect();
    for i in 0..out2.count {
        let w = out2.get_weight(i);
        let g = read_i64_le(grp_data, i * 8);
        let m = read_i64_le(min_data, i * 8);
        if w < 0 {
            live.remove(&g);
        } else if w > 0 {
            live.insert(g, m);
        }
    }
    let mut live_pairs: Vec<(i64, i64)> = live.into_iter().collect();
    live_pairs.sort_unstable();
    assert_eq!(live_pairs, *expected_tick2, "tick 2 MIN mismatch (fallback path)");
}

/// MIN grouped by nullable I64 — linear probe branch (< HASH_THRESHOLD groups).
#[test]
fn fallback_min_nullable_i64_group_linear_probe() {
    // 3 distinct non-null groups.
    let tick1 = vec![
        (1u64, Some(10i64), 100i64, 1i64),
        (2, Some(10), 50, 1),
        (3, Some(20), 30, 1),
        (4, Some(30), 200, 1),
    ];
    // Delete the row with val=100 from group 10 → new MIN(10) = 50.
    let delta2 = vec![(1u64, Some(10i64), 100i64, -1i64)];
    let mut exp1 = vec![(10i64, 50i64), (20, 30), (30, 200)];
    let mut exp2 = vec![(10i64, 50i64), (20, 30), (30, 200)];
    run_fallback_min_i64_grp(&tick1, &delta2, &mut exp1, &mut exp2);
}

/// MIN grouped by nullable I64 — hash branch (>= HASH_THRESHOLD groups).
#[test]
fn fallback_min_nullable_i64_group_hash_path() {
    // 20 distinct groups (exceeds HASH_THRESHOLD=16).
    let tick1: Vec<GrpValRow> = (0..20)
        .flat_map(|g| {
            vec![
                ((g * 2) as u64, Some(g as i64), (g * 10 + 5) as i64, 1),
                ((g * 2 + 1) as u64, Some(g as i64), (g * 10 + 1) as i64, 1),
            ]
        })
        .collect();
    // MIN per group g = g*10+1. Delete the row with val=51 from group 5 → MIN(5) becomes 55.
    // pk=11 = g*2+1 for g=5 carries val=g*10+1=51.
    let delta2 = vec![(11u64, Some(5i64), 51i64, -1i64)]; // remove pk=11 (val=51)
    let mut exp1: Vec<(i64, i64)> = (0..20).map(|g| (g as i64, (g * 10 + 1) as i64)).collect();
    let mut exp2: Vec<(i64, i64)> = (0..20)
        .map(|g| (g as i64, if g == 5 { 55 } else { (g * 10 + 1) as i64 }))
        .collect();
    run_fallback_min_i64_grp(&tick1, &delta2, &mut exp1, &mut exp2);
}

/// MIN grouped by multi-column key (I64, I64) — no GI → fallback path.
#[test]
fn fallback_min_multi_col_group() {
    use crate::storage::ReadCursor;
    use std::rc::Rc;

    // Schema: U64 pk | I64 c1 | I64 c2 | I64 val. Group by [c1, c2].
    let in_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    );
    // Output: U128 pk | I64 c1 | I64 c2 | I64 min (nullable).
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::I64, 1),
        ],
        &[0],
    );

    let agg = AggDescriptor {
        col_idx: 3,
        agg_op: AggOp::Min,
        col_type_code: TypeCode::I64,
        _pad: [0; 2],
    };

    let pi_c1 = in_schema.try_payload_idx(1).unwrap();
    let pi_c2 = in_schema.try_payload_idx(2).unwrap();
    let pi_val = in_schema.try_payload_idx(3).unwrap();

    let make_batch = |rows: &[(u64, i64, i64, i64, i64)]| -> Batch {
        let mut b = Batch::with_schema(in_schema, rows.len().max(1));
        for &(pk, w, c1, c2, val) in rows {
            b.extend_pk(pk as u128);
            b.extend_weight(&w.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(pi_c1, &c1.to_le_bytes());
            b.extend_col(pi_c2, &c2.to_le_bytes());
            b.extend_col(pi_val, &val.to_le_bytes());
            b.count += 1;
        }
        b.set_layout_unchecked(Layout::Consolidated);
        b
    };

    // Tick 1: groups (1,1)→min=10, (1,2)→min=30, (2,1)→min=50.
    let tick1_rows = [
        (1u64, 1, 1i64, 1i64, 10i64),
        (2, 1, 1, 1, 20),
        (3, 1, 1, 2, 30),
        (4, 1, 2, 1, 50),
    ];
    let delta1 = make_batch(&tick1_rows);
    let empty_out = Rc::new(Batch::empty_with_schema(&out_schema));
    let mut to_ch = ReadCursor::from_owned(&[empty_out], out_schema);

    let (out1, _) = op_reduce(
        &delta1,
        None,
        &mut to_ch,
        &in_schema,
        &out_schema,
        &[1u32, 2u32],
        &[agg],
        None,
        None,
        None,
        false,
        false,
    );
    // Verify 3 distinct groups came out.
    assert_eq!(out1.count, 3, "tick 1 must emit 3 groups");

    // Tick 2: add val=5 to group (1,1). trace_out is empty (no prior output to
    // retract); the fallback must still pull trace rows for group (1,1) from
    // trace_in so the replay is {10, 20, 5} and MIN = 5.
    let delta2_rows = [(5u64, 1, 1i64, 1i64, 5i64)];
    let ti_batch = Rc::new(make_batch(&tick1_rows));
    let empty_out2 = Rc::new(Batch::empty_with_schema(&out_schema));
    let delta2 = make_batch(&delta2_rows);

    let mut ti_ch = ReadCursor::from_owned(&[ti_batch], in_schema);
    let mut to_ch2 = ReadCursor::from_owned(&[empty_out2], out_schema);

    let (out2, _) = op_reduce(
        &delta2,
        Some(&mut ti_ch),
        &mut to_ch2,
        &in_schema,
        &out_schema,
        &[1u32, 2u32],
        &[agg],
        None,
        None,
        None,
        false,
        false,
    );

    // No retraction (trace_out was empty), just one insert for group (1,1).
    assert_eq!(out2.count, 1, "tick 2 must emit one row for group (1,1)");
    let out2_min = out2.col_data(2);
    let new_min = read_i64_le(out2_min, 0);
    assert_eq!(
        new_min, 5,
        "MIN for group (1,1) must be 5 — trace rows (10,20) + delta (5) → min=5"
    );
}

/// Performance guard: the single-scan pre-pass must rewind the trace cursor at
/// most once per op_reduce call, regardless of how many delta groups there are.
/// Uses a `#[cfg(test)]` thread-local counter in `ReadCursor::rewind`.
#[test]
fn fallback_trace_rewind_at_most_once() {
    use crate::storage::{ReadCursor, REWIND_CALLS};
    use std::rc::Rc;

    // Schema: U64 pk | I64 grp | I64 val. 32 distinct groups (> HASH_THRESHOLD=16).
    let in_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    );
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::I64, 1),
        ],
        &[0],
    );
    let agg = AggDescriptor {
        col_idx: 2,
        agg_op: AggOp::Min,
        col_type_code: TypeCode::I64,
        _pad: [0; 2],
    };

    let pi_grp = in_schema.try_payload_idx(1).unwrap();
    let pi_val = in_schema.try_payload_idx(2).unwrap();
    let num_groups = 32usize;

    // Trace: one history row per group.
    let mut ti_b = Batch::with_schema(in_schema, num_groups);
    for g in 0..num_groups {
        ti_b.extend_pk((g as u128) * 2); // even PKs for trace
        ti_b.extend_weight(&1i64.to_le_bytes());
        ti_b.extend_null_bmp(&0u64.to_le_bytes());
        ti_b.extend_col(pi_grp, &(g as i64).to_le_bytes());
        ti_b.extend_col(pi_val, &(g as i64 * 10).to_le_bytes());
        ti_b.count += 1;
    }
    ti_b.set_layout_unchecked(Layout::Consolidated);

    // Delta: one new row per group (odd PKs).
    let mut delta_b = Batch::with_schema(in_schema, num_groups);
    for g in 0..num_groups {
        delta_b.extend_pk((g as u128) * 2 + 1);
        delta_b.extend_weight(&1i64.to_le_bytes());
        delta_b.extend_null_bmp(&0u64.to_le_bytes());
        delta_b.extend_col(pi_grp, &(g as i64).to_le_bytes());
        delta_b.extend_col(pi_val, &(g as i64 * 10 + 5).to_le_bytes());
        delta_b.count += 1;
    }
    delta_b.set_layout_unchecked(Layout::Consolidated);

    let ti_rc = Rc::new(ti_b);
    let empty_out = Rc::new(Batch::empty_with_schema(&out_schema));
    let mut ti_ch = ReadCursor::from_owned(&[ti_rc], in_schema);
    let mut to_ch = ReadCursor::from_owned(&[empty_out], out_schema);

    // Reset the per-thread rewind counter before the call.
    REWIND_CALLS.with(|c| c.set(0));

    let (out, _) = op_reduce(
        &delta_b,
        Some(&mut ti_ch),
        &mut to_ch,
        &in_schema,
        &out_schema,
        &[1u32],
        &[agg],
        None,
        None,
        None,
        false,
        false,
    );

    let rewinds = REWIND_CALLS.with(|c| c.get());
    assert!(
        rewinds <= 1,
        "trace cursor was rewound {rewinds} times; the single-scan pre-pass must rewind ≤ 1 time",
    );

    // Correctness: MIN per group = min(g*10, g*10+5) = g*10 (trace value).
    assert_eq!(out.count, num_groups, "must emit one row per group");
}

// -----------------------------------------------------------------------
// Fix B: true 128-bit group identity (collision resistance + full width).
//
// The fold path (multi-column GROUP BY / single STRING key) now streams the
// canonical per-column material into an Xxh3 digest128, so it carries a full
// 128 bits of entropy. The previous mix64 fold compressed to a 64-bit `h` and
// lifted to u128 bijectively, so it had only 2^64 cardinality — a ~2^32
// birthday bound past which a collision merges two distinct group keys into one
// group (silently wrong aggregation). The single-column int/PK fast paths are
// exact and stay byte-identical (pinned by the *_bit_identical /
// *_canonical_widen tests, unchanged above).
// -----------------------------------------------------------------------

/// 3-col schema: U64 pk | I64 c1 | STRING c2 (both non-PK group columns).
fn make_schema_u64_i64_str() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::STRING, 0),
        ],
        &[0],
    )
}

#[test]
fn test_extract_group_key_128bit_collision_resistance() {
    use std::collections::HashSet;
    let schema = make_schema_u64_i64_str();

    let pi_c1 = schema.try_payload_idx(1).unwrap();
    let pi_c2 = schema.try_payload_idx(2).unwrap();

    // Sweep many distinct (c1, c2) multi-column keys. Each (i, j) is a distinct
    // group; the 128-bit fold must map them to distinct u128 with no collision.
    let mut b = Batch::with_schema(schema, 1);
    let mut expected = 0usize;
    for i in 0..64i64 {
        for j in 0..64u32 {
            b.extend_pk((expected as u128) + 1);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(pi_c1, &i.to_le_bytes());
            let gs = encode_german_string(format!("k{j}").as_bytes(), &mut b.blob);
            b.extend_col(pi_c2, &gs);
            b.count += 1;
            expected += 1;
        }
    }
    let mb = b.as_mem_batch();

    let mut keys: HashSet<u128> = HashSet::new();
    let mut his: HashSet<u64> = HashSet::new();
    let mut los: HashSet<u64> = HashSet::new();
    for row in 0..b.count {
        let k = extract_group_key(&mb, row, &schema, &[1u32, 2u32]);
        keys.insert(k);
        his.insert((k >> 64) as u64);
        los.insert(k as u64);
    }
    assert_eq!(
        keys.len(),
        expected,
        "all distinct multi-column keys must hash to distinct 128-bit values (no collision)",
    );
    // The full 128-bit width is used: both halves carry real hash output. A
    // 64-bit-then-zero-extended key would pin the high half constant; the old
    // bijective lift would make the high half a deterministic image of the low
    // half. For 4096 keys well under the 2^32 birthday bound, an independent
    // 128-bit digest leaves each half collision-free.
    assert_eq!(
        his.len(),
        expected,
        "high 64 bits must be full-entropy (no truncation collision)"
    );
    assert_eq!(
        los.len(),
        expected,
        "low 64 bits must be full-entropy (no truncation collision)"
    );

    // Determinism: the same row hashes identically across calls (reused hasher).
    assert_eq!(
        extract_group_key(&mb, 0, &schema, &[1u32, 2u32]),
        extract_group_key(&mb, 0, &schema, &[1u32, 2u32]),
        "same row must hash identically",
    );

    // Cursor and batch paths must still agree on the fold (German-string arm).
    assert_group_key_cursor_matches_batch(&b, &schema, &[1u32, 2u32]);
}

// -----------------------------------------------------------------------
// Fix C: BLOB as a grouping key.
//
// BLOB shares the 16-byte German-string layout with STRING; the two
// group-membership compares (cursor_matches_group, compare_by_group_cols) now
// dispatch BLOB through compare_german_strings instead of `unreachable!`-ing.
// Mirrors test_distinct_blob_payload_no_panic. Long (>12-byte) blobs that share
// a 4-byte prefix force the full-content heap tail comparison.
// -----------------------------------------------------------------------

/// Schema: U64 pk | BLOB grp | I64 val. BLOB is the (non-PK) grouping key.
fn make_schema_u64_blob_grp_i64() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::BLOB, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    )
}

/// Build a (pk, weight, blob, val) batch. Blobs > 12 bytes go to the heap (long
/// form); shorter blobs stay inline — both exercise german_string_content. Rows
/// are passed in PK order so the batch is validly sorted+consolidated for use as
/// a trace cursor.
fn make_batch_blob_grp_i64(schema: &SchemaDescriptor, rows: &[(u64, i64, &[u8], i64)]) -> Batch {
    let pi_grp = schema.try_payload_idx(1).unwrap();
    let pi_val = schema.try_payload_idx(2).unwrap();
    let mut b = Batch::with_schema(*schema, rows.len().max(1));
    for &(pk, w, blob, val) in rows {
        let gs = encode_german_string(blob, &mut b.blob);
        b.extend_pk(pk as u128);
        b.extend_weight(&w.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(pi_grp, &gs);
        b.extend_col(pi_val, &val.to_le_bytes());
        b.count += 1;
    }
    b.set_layout_unchecked(Layout::Consolidated);
    b
}

#[test]
fn test_compare_by_group_cols_blob_no_panic() {
    // Two long blobs sharing the 4-byte prefix "PREF" and the same length, so
    // compare_german_strings must walk the heap tail (not just the inline
    // prefix). Pre-fix this `unreachable!`s; post-fix it orders by content.
    let schema = make_schema_u64_blob_grp_i64();
    let blob_a: &[u8] = b"PREF_aaaaaaaaaa"; // 15 bytes
    let blob_b: &[u8] = b"PREF_bbbbbbbbbb"; // 15 bytes, same prefix+length
    assert_eq!(blob_a.len(), blob_b.len());
    assert_eq!(&blob_a[..4], &blob_b[..4]);
    let batch = make_batch_blob_grp_i64(&schema, &[(1, 1, blob_a, 10), (2, 1, blob_b, 20)]);
    let mb = batch.as_mem_batch();
    let (descs_arr, descs_len) = build_sort_descs(&schema, &[1]);
    let descs = &descs_arr[..descs_len];

    assert_eq!(
        compare_by_group_cols(&mb, 0, 1, descs),
        std::cmp::Ordering::Less,
        "blob_a < blob_b by content tail"
    );
    assert_eq!(compare_by_group_cols(&mb, 1, 0, descs), std::cmp::Ordering::Greater);
    assert_eq!(
        compare_by_group_cols(&mb, 0, 0, descs),
        std::cmp::Ordering::Equal,
        "same blob compares equal"
    );

    // The hash path (Fix B's German-string arm) and the cursor compare must
    // agree with the batch compare on this BLOB key.
    assert_group_key_cursor_matches_batch(&batch, &schema, &[1u32]);
    let k0 = extract_group_key(&mb, 0, &schema, &[1u32]);
    let k1 = extract_group_key(&mb, 1, &schema, &[1u32]);
    assert_ne!(k0, k1, "distinct blobs must hash to distinct group keys");
}

#[test]
fn test_reduce_max_blob_group_retraction() {
    use crate::storage::ReadCursor;
    use std::rc::Rc;

    // GROUP BY a long BLOB column, MAX(I64 val). MAX is non-linear and there is
    // no AVI/GI, so the trace replay routes through cursor_matches_group — the
    // compare that `unreachable!`'d on BLOB before Fix C. Two distinct long
    // blobs sharing a 4-byte prefix must group separately, never panic.
    let in_schema = make_schema_u64_blob_grp_i64();
    // Output: synthetic U128 _group_pk | BLOB grp | I64 max (nullable).
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::BLOB, 0),
            SchemaColumn::new(type_code::I64, 1),
        ],
        &[0],
    );
    let agg = AggDescriptor {
        col_idx: 2,
        agg_op: AggOp::Max,
        col_type_code: TypeCode::I64,
        _pad: [0; 2],
    };

    let blob_a: &[u8] = b"PREFIX_AAAAAAAAAA"; // 17 bytes (long)
    let blob_b: &[u8] = b"PREFIX_BBBBBBBBBB"; // 17 bytes, same prefix+length
    assert_eq!(&blob_a[..6], &blob_b[..6], "shared prefix forces heap tail compare");

    // Tick 1: blob_a → {10, 30}, blob_b → {20}. Empty trace.
    let tick1: &[(u64, i64, &[u8], i64)] = &[(1, 1, blob_a, 10), (2, 1, blob_a, 30), (3, 1, blob_b, 20)];
    let delta1 = make_batch_blob_grp_i64(&in_schema, tick1);
    let empty_out = Rc::new(Batch::empty_with_schema(&out_schema));
    let mut to_ch1 = ReadCursor::from_owned(&[empty_out], out_schema);
    let (out1, _) = op_reduce(
        &delta1,
        None,
        &mut to_ch1,
        &in_schema,
        &out_schema,
        &[1u32],
        &[agg],
        None,
        None,
        None,
        false,
        false,
    );
    assert_eq!(
        out1.count, 2,
        "two distinct blobs → two groups (must not merge on shared prefix)"
    );
    let maxes1: std::collections::BTreeSet<i64> =
        (0..out1.count).map(|i| read_i64_le(out1.col_data(1), i * 8)).collect();
    assert_eq!(
        maxes1,
        [20i64, 30].into_iter().collect(),
        "MAX(blob_a)=30, MAX(blob_b)=20"
    );

    // Tick 2: retract the val=30 row from blob_a → MAX(blob_a) 30 → 10. The
    // replay scans the trace (tick1 rows) via cursor_matches_group on the BLOB.
    let ti_batch = Rc::new(make_batch_blob_grp_i64(&in_schema, tick1));
    let to_batch = Rc::new(out1);
    // Retraction: weight -1 on the val=30 blob_a row.
    let delta2 = make_batch_blob_grp_i64(&in_schema, &[(2, -1, blob_a, 30)]);
    let mut ti_ch2 = ReadCursor::from_owned(&[ti_batch], in_schema);
    let mut to_ch2 = ReadCursor::from_owned(&[to_batch], out_schema);
    let (out2, _) = op_reduce(
        &delta2,
        Some(&mut ti_ch2),
        &mut to_ch2,
        &in_schema,
        &out_schema,
        &[1u32],
        &[agg],
        None,
        None,
        None,
        false,
        false,
    );
    // blob_a's MAX updates 30 → 10: retract old (30, w=-1) + insert new (10, w=+1).
    // blob_b is untouched.
    let retract = (0..out2.count)
        .find(|&i| out2.get_weight(i) < 0)
        .map(|i| read_i64_le(out2.col_data(1), i * 8));
    let insert = (0..out2.count)
        .find(|&i| out2.get_weight(i) > 0)
        .map(|i| read_i64_le(out2.col_data(1), i * 8));
    assert_eq!(retract, Some(30), "retract old MAX(blob_a)=30");
    assert_eq!(insert, Some(10), "insert new MAX(blob_a)=10 after the 30 row is gone");
}

// ---------------------------------------------------------------------------
// Global (ungrouped) aggregate ground-row tests
//
// A no-`GROUP BY` aggregate is one logical group with an empty group-column set;
// the output PK is the synthetic constant V₀. `op_reduce` must emit exactly one
// row at V₀ over an empty/fully-retracted source (COUNT=0, SUM/MIN/MAX=NULL).
// ---------------------------------------------------------------------------

/// Source for the global-aggregate tests: `[pk:U64, val:I64(nullable)]`.
fn g_src() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 1),
        ],
        &[0],
    )
}

/// Build a delta over `g_src()` from `(pk, weight, val)` rows.
fn g_delta(rows: &[(u64, i64, i64)]) -> Batch {
    make_batch(&g_src(), rows)
}

const G_COUNT: AggDescriptor = AggDescriptor {
    col_idx: 0,
    agg_op: AggOp::Count,
    col_type_code: TypeCode::U64,
    _pad: [0; 2],
};
const G_SUM: AggDescriptor = AggDescriptor {
    col_idx: 1,
    agg_op: AggOp::Sum,
    col_type_code: TypeCode::I64,
    _pad: [0; 2],
};
const G_MIN: AggDescriptor = AggDescriptor {
    col_idx: 1,
    agg_op: AggOp::Min,
    col_type_code: TypeCode::I64,
    _pad: [0; 2],
};

/// Output `[_group_pk:U128, count:I64, min:I64(nullable)]` — the mixed
/// non-linear global-aggregate shape (`SELECT COUNT(*), MIN(x)`).
fn g_out_count_min() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::I64, 1),
        ],
        &[0],
    )
}

/// `op_reduce` over `g_src()` with empty group cols (the global-aggregate path).
#[allow(clippy::too_many_arguments)]
fn g_reduce(
    delta: &Batch,
    trace_in: Option<&mut crate::storage::ReadCursor>,
    trace_out: &mut crate::storage::ReadCursor,
    out_schema: &SchemaDescriptor,
    aggs: &[AggDescriptor],
    i_am_owner: bool,
) -> (Batch, Option<Batch>) {
    let in_schema = g_src();
    op_reduce(
        delta,
        trace_in,
        trace_out,
        &in_schema,
        out_schema,
        &[], // empty group cols ⇒ one global group at V₀
        aggs,
        None,
        None,
        None,
        true, // global_ground
        i_am_owner,
    )
}

/// Seed over an empty source emits exactly one ground row at V₀: COUNT=0, SUM=NULL.
#[test]
fn global_seed_over_empty_emits_one_ground_row() {
    use crate::storage::ReadCursor;
    use std::rc::Rc;

    let out_schema = make_pk_sum_count_out_schema();
    let empty_out = Rc::new(Batch::empty_with_schema(&out_schema));
    let mut to_ch = ReadCursor::from_owned(&[empty_out], out_schema);

    let (raw, _) = g_reduce(&g_delta(&[]), None, &mut to_ch, &out_schema, &[G_SUM, G_COUNT], true);

    assert_eq!(raw.count, 1, "empty-source global aggregate must emit one ground row");
    assert_eq!(raw.get_weight(0), 1, "ground row weight +1");
    assert_eq!(raw.get_pk(0), crate::ops::global_group_key(), "ground PK must be V₀");
    assert_eq!(raw.get_null_word(0) & 1, 1, "SUM must be NULL over empty source");
    assert_eq!(
        read_i64_le(raw.col_data(1), 0),
        0,
        "COUNT(*) must be 0 over empty source"
    );
    assert_eq!((raw.get_null_word(0) >> 1) & 1, 0, "COUNT must be present (not NULL)");
}

/// The seed is idempotent: a second empty pad whose trace_out already holds the
/// V₀ ground re-seeds nothing (no weight-2 ground is constructible).
#[test]
fn global_seed_idempotent_across_two_empty_pads() {
    use crate::storage::ReadCursor;
    use std::rc::Rc;

    let out_schema = make_pk_sum_count_out_schema();
    let empty_out = Rc::new(Batch::empty_with_schema(&out_schema));
    let mut to_ch = ReadCursor::from_owned(&[empty_out], out_schema);
    let (raw1, _) = g_reduce(&g_delta(&[]), None, &mut to_ch, &out_schema, &[G_SUM, G_COUNT], true);
    assert_eq!(raw1.count, 1, "first pad seeds the ground");

    // Second pad: trace_out now holds the V₀ ground from the first pad.
    let prev = Rc::new(raw1);
    let mut to_ch2 = ReadCursor::from_owned(&[prev], out_schema);
    let (raw2, _) = g_reduce(&g_delta(&[]), None, &mut to_ch2, &out_schema, &[G_SUM, G_COUNT], true);
    assert_eq!(raw2.count, 0, "second pad must NOT re-seed (V₀ already in trace_out)");
}

/// A non-owner worker's empty pad seeds nothing — it emits a literally empty
/// batch (asserted on the batch itself, not a merged result).
#[test]
fn global_non_owner_empty_pad_emits_zero_rows() {
    use crate::storage::ReadCursor;
    use std::rc::Rc;

    let out_schema = make_pk_sum_count_out_schema();
    let empty_out = Rc::new(Batch::empty_with_schema(&out_schema));
    let mut to_ch = ReadCursor::from_owned(&[empty_out], out_schema);
    let (raw, _) = g_reduce(
        &g_delta(&[]),
        None,
        &mut to_ch,
        &out_schema,
        &[G_SUM, G_COUNT],
        false, // not the V₀ owner
    );
    assert_eq!(raw.count, 0, "non-owner empty pad must emit a zero-row batch");
}

/// Create over a non-empty source emits one computed row and NO ground.
#[test]
fn global_create_over_nonempty_emits_computed_no_ground() {
    use crate::storage::ReadCursor;
    use std::rc::Rc;

    let out_schema = make_pk_sum_count_out_schema();
    let empty_out = Rc::new(Batch::empty_with_schema(&out_schema));
    let mut to_ch = ReadCursor::from_owned(&[empty_out], out_schema);
    let (raw, _) = g_reduce(
        &g_delta(&[(1, 1, 5), (2, 1, 10)]),
        None,
        &mut to_ch,
        &out_schema,
        &[G_SUM, G_COUNT],
        true,
    );
    assert_eq!(raw.count, 1, "one computed row, no ground");
    assert_eq!(read_i64_le(raw.col_data(0), 0), 15, "SUM=15");
    assert_eq!(read_i64_le(raw.col_data(1), 0), 2, "COUNT=2");
    assert_eq!(raw.get_null_word(0) & 1, 0, "SUM present (not NULL)");
}

/// Fully retracting an all-linear global aggregate sheds the computed row and the
/// ground branch supplies one NULL/zero row in its place (net = one ground row).
#[test]
fn global_emptied_by_delete_emits_ground() {
    use crate::storage::ReadCursor;
    use std::rc::Rc;

    let out_schema = make_pk_sum_count_out_schema();
    let empty_out = Rc::new(Batch::empty_with_schema(&out_schema));
    let mut to_ch = ReadCursor::from_owned(&[empty_out], out_schema);
    let (raw1, _) = g_reduce(
        &g_delta(&[(1, 1, 5)]),
        None,
        &mut to_ch,
        &out_schema,
        &[G_SUM, G_COUNT],
        true,
    );
    assert_eq!(read_i64_le(raw1.col_data(0), 0), 5, "SUM=5");

    // Retract the only row → cardinality 0.
    let prev = Rc::new(raw1);
    let mut to_ch2 = ReadCursor::from_owned(&[prev], out_schema);
    let (raw2, _) = g_reduce(
        &g_delta(&[(1, -1, 5)]),
        None,
        &mut to_ch2,
        &out_schema,
        &[G_SUM, G_COUNT],
        true,
    );

    // Retract old computed (-1) + emit ground (+1) = 2 rows; net view = ground.
    assert_eq!(raw2.count, 2, "retraction of old + ground insert");
    let mb = raw2.as_mem_batch();
    let pos = (0..2).find(|&i| mb.get_weight(i) == 1).expect("a +1 row");
    assert_eq!(raw2.get_null_word(pos) & 1, 1, "ground SUM=NULL");
    assert_eq!(read_i64_le(raw2.col_data(1), pos * 8), 0, "ground COUNT=0");
}

/// A seeded ground transitions to a computed row when the source is populated:
/// the ground at V₀ is retracted and the computed row replaces it.
#[test]
fn global_ground_to_computed_on_first_insert() {
    use crate::storage::ReadCursor;
    use std::rc::Rc;

    let out_schema = make_pk_sum_count_out_schema();
    // Tick 1: seed the ground over an empty source.
    let empty_out = Rc::new(Batch::empty_with_schema(&out_schema));
    let mut to_ch = ReadCursor::from_owned(&[empty_out], out_schema);
    let (ground, _) = g_reduce(&g_delta(&[]), None, &mut to_ch, &out_schema, &[G_SUM, G_COUNT], true);
    assert_eq!(ground.count, 1);

    // Tick 2: insert a row; trace_out holds the ground.
    let prev = Rc::new(ground);
    let mut to_ch2 = ReadCursor::from_owned(&[prev], out_schema);
    let (raw2, _) = g_reduce(
        &g_delta(&[(1, 1, 7)]),
        None,
        &mut to_ch2,
        &out_schema,
        &[G_SUM, G_COUNT],
        true,
    );

    // Retract ground (-1) + emit computed (+1) = 2 rows; net view = computed.
    assert_eq!(raw2.count, 2, "retract ground + emit computed");
    let mb = raw2.as_mem_batch();
    let pos = (0..2).find(|&i| mb.get_weight(i) == 1).expect("a +1 row");
    assert_eq!(read_i64_le(raw2.col_data(0), pos * 8), 7, "computed SUM=7");
    assert_eq!(read_i64_le(raw2.col_data(1), pos * 8), 1, "computed COUNT=1");
    assert_eq!(raw2.get_null_word(pos) & 1, 0, "computed SUM present");
}

/// A value-change tick on a surviving global aggregate emits the old/new computed
/// rows and NO ground delta (the group never crosses the cardinality boundary).
#[test]
fn global_value_change_emits_no_ground() {
    use crate::storage::ReadCursor;
    use std::rc::Rc;

    let out_schema = make_pk_sum_count_out_schema();
    let empty_out = Rc::new(Batch::empty_with_schema(&out_schema));
    let mut to_ch = ReadCursor::from_owned(&[empty_out], out_schema);
    let (raw1, _) = g_reduce(
        &g_delta(&[(1, 1, 5)]),
        None,
        &mut to_ch,
        &out_schema,
        &[G_SUM, G_COUNT],
        true,
    );
    assert_eq!(read_i64_le(raw1.col_data(0), 0), 5);

    // Change pk1's value 5 → 8 (retract old, insert new) — cardinality stays 1.
    let prev = Rc::new(raw1);
    let mut to_ch2 = ReadCursor::from_owned(&[prev], out_schema);
    let (raw2, _) = g_reduce(
        &g_delta(&[(1, -1, 5), (1, 1, 8)]),
        None,
        &mut to_ch2,
        &out_schema,
        &[G_SUM, G_COUNT],
        true,
    );
    assert_eq!(raw2.count, 2, "retract old computed + emit new computed");
    let mb = raw2.as_mem_batch();
    let pos = (0..2).find(|&i| mb.get_weight(i) == 1).expect("a +1 row");
    // The +1 row is the new computed value, never a NULL ground.
    assert_eq!(read_i64_le(raw2.col_data(0), pos * 8), 8, "new SUM=8");
    assert_eq!(read_i64_le(raw2.col_data(1), pos * 8), 1, "COUNT stays 1");
    assert_eq!(raw2.get_null_word(pos) & 1, 0, "no NULL ground delta on a value change");
}

/// Mixed non-linear `[COUNT(*), MIN(x)]` fully emptied → one ground row
/// `COUNT=0, MIN=NULL`, never a `COUNT=−N` zombie (the replay resets all accs).
#[test]
fn global_mixed_count_min_emptied_emits_ground() {
    use crate::storage::ReadCursor;
    use std::rc::Rc;

    let out_schema = g_out_count_min();
    let in_schema = g_src();
    let aggs = [G_COUNT, G_MIN];

    // Tick 1: insert one row (trace_in empty). MIN is non-linear → replay path.
    let empty_in = Rc::new(Batch::empty_with_schema(&in_schema));
    let mut ti1 = ReadCursor::from_owned(&[empty_in], in_schema);
    let empty_out = Rc::new(Batch::empty_with_schema(&out_schema));
    let mut to_ch = ReadCursor::from_owned(&[empty_out], out_schema);
    let (raw1, _) = g_reduce(
        &g_delta(&[(1, 1, 5)]),
        Some(&mut ti1),
        &mut to_ch,
        &out_schema,
        &aggs,
        true,
    );
    assert_eq!(read_i64_le(raw1.col_data(0), 0), 1, "COUNT=1");
    assert_eq!(read_i64_le(raw1.col_data(1), 0), 5, "MIN=5");

    // Tick 2: retract the only row. trace_in holds the integrated history.
    let hist = {
        let mut b = Batch::with_schema(in_schema, 1);
        b.extend_pk(1u128);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &5i64.to_le_bytes());
        b.count += 1;
        b.set_layout_unchecked(Layout::Consolidated);
        Rc::new(b)
    };
    let mut ti2 = ReadCursor::from_owned(&[hist], in_schema);
    let prev = Rc::new(raw1);
    let mut to_ch2 = ReadCursor::from_owned(&[prev], out_schema);
    let (raw2, _) = g_reduce(
        &g_delta(&[(1, -1, 5)]),
        Some(&mut ti2),
        &mut to_ch2,
        &out_schema,
        &aggs,
        true,
    );

    assert_eq!(raw2.count, 2, "retract old + ground insert");
    let mb = raw2.as_mem_batch();
    let pos = (0..2).find(|&i| mb.get_weight(i) == 1).expect("a +1 row");
    assert_eq!(read_i64_le(raw2.col_data(0), pos * 8), 0, "ground COUNT=0, never -N");
    assert_eq!((raw2.get_null_word(pos) >> 1) & 1, 1, "ground MIN=NULL");
}

/// A lone global `MIN` over ≥1 row, then retract the current min → the view
/// advances to the next-best value (the replay path over an empty group set).
#[test]
fn global_lone_min_retract_to_next_best() {
    use crate::storage::ReadCursor;
    use std::rc::Rc;

    // Output `[_group_pk:U128, min:I64(nullable)]`, agg `[MIN]` (no companion).
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::I64, 1),
        ],
        &[0],
    );
    let in_schema = g_src();

    // Tick 1: insert val=5 and val=3 → MIN=3.
    let empty_in = Rc::new(Batch::empty_with_schema(&in_schema));
    let mut ti1 = ReadCursor::from_owned(&[empty_in], in_schema);
    let empty_out = Rc::new(Batch::empty_with_schema(&out_schema));
    let mut to_ch = ReadCursor::from_owned(&[empty_out], out_schema);
    let (raw1, _) = g_reduce(
        &g_delta(&[(1, 1, 5), (2, 1, 3)]),
        Some(&mut ti1),
        &mut to_ch,
        &out_schema,
        &[G_MIN],
        true,
    );
    assert_eq!(read_i64_le(raw1.col_data(0), 0), 3, "MIN=3");

    // Tick 2: retract the current min (val=3) → MIN advances to 5.
    let hist = {
        let mut b = Batch::with_schema(in_schema, 2);
        for (pk, v) in [(1u128, 5i64), (2, 3)] {
            b.extend_pk(pk);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &v.to_le_bytes());
            b.count += 1;
        }
        b.set_layout_unchecked(Layout::Consolidated);
        Rc::new(b)
    };
    let mut ti2 = ReadCursor::from_owned(&[hist], in_schema);
    let prev = Rc::new(raw1);
    let mut to_ch2 = ReadCursor::from_owned(&[prev], out_schema);
    let (raw2, _) = g_reduce(
        &g_delta(&[(2, -1, 3)]),
        Some(&mut ti2),
        &mut to_ch2,
        &out_schema,
        &[G_MIN],
        true,
    );
    let mb = raw2.as_mem_batch();
    let pos = (0..raw2.count).find(|&i| mb.get_weight(i) == 1).expect("a +1 row");
    assert_eq!(
        read_i64_le(raw2.col_data(0), pos * 8),
        5,
        "MIN advances 3 → 5 (next-best)"
    );
}

/// The AVI empty-prefix path: a lone global MIN over ≥1 row resolves via the
/// 0-byte-prefix AVI seek (`avi_group_key_eligible(&[])` is true; the seek matches
/// every entry, returning the GLOBAL extremum in agg-value order), and a
/// retraction advances to the next-best AVI post-state. This is the O(log N) path
/// the planner builds for a lone global MIN/MAX; the trace-path test above covers
/// the avi = None fallback.
#[test]
fn global_lone_min_avi_empty_prefix() {
    use super::super::util::encode_ordered;
    use crate::storage::ReadCursor;
    use std::rc::Rc;

    let in_schema = g_src();
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::I64, 1),
        ],
        &[0],
    );

    // AVI for empty group cols: key = av_encoded(8) only (0-byte group prefix).
    let avi_schema = crate::ops::index::make_avi_schema(&in_schema, &[]);
    assert_eq!(avi_schema.pk_stride(), 9, "0 group + 1 ordinal + 8 av");
    let avi_with = |min: i64| {
        let mut b = Batch::with_schema(avi_schema, 1);
        let av = encode_ordered(&min.to_le_bytes(), type_code::I64, false);
        let mut key = [0u8; 9];
        key[0] = 0; // ordinal 0 (single MIN, empty group prefix)
        key[1..9].copy_from_slice(&av.to_be_bytes());
        b.extend_pk_bytes(&key);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.count += 1;
        b
    };

    let g_min_avi = |delta: &Batch, to: &mut crate::storage::ReadCursor, avi: i64| {
        let mut avi_ch = ReadCursor::from_owned(&[Rc::new(avi_with(avi))], avi_schema);
        op_reduce(
            delta,
            None,
            to,
            &in_schema,
            &out_schema,
            &[], // empty group cols
            &[G_MIN],
            Some(&mut avi_ch),
            None,
            None,
            true,
            true,
        )
    };

    // Tick 1: delta val=50 is NOT the min; the AVI holds the true global min 10,
    // so a correct result can only come from the 0-byte-prefix index seek.
    let empty_out = Rc::new(Batch::empty_with_schema(&out_schema));
    let mut to_ch = ReadCursor::from_owned(&[empty_out], out_schema);
    let (raw1, _) = g_min_avi(&g_delta(&[(1, 1, 50)]), &mut to_ch, 10);
    let mb1 = raw1.as_mem_batch();
    let p1 = (0..raw1.count).find(|&i| mb1.get_weight(i) == 1).expect("a +1 row");
    assert_eq!(
        read_i64_le(raw1.col_data(0), p1 * 8),
        10,
        "AVI empty-prefix seek returns the GLOBAL min, not the delta's 50"
    );

    // Tick 2: a retraction re-evaluates the group; the AVI post-state is 20.
    let prev = Rc::new(raw1);
    let mut to_ch2 = ReadCursor::from_owned(&[prev], out_schema);
    let (raw2, _) = g_min_avi(&g_delta(&[(2, -1, 10)]), &mut to_ch2, 20);
    let mb2 = raw2.as_mem_batch();
    let p2 = (0..raw2.count).find(|&i| mb2.get_weight(i) == 1).expect("a +1 row");
    assert_eq!(
        read_i64_le(raw2.col_data(0), p2 * 8),
        20,
        "AVI advances to the next-best post-state on retraction"
    );
}

// ---------------------------------------------------------------------------
// SumZero aggregate + two-phase global-aggregate combine
//
// SumZero = Sum's fold with Count's `0` identity: it sums its source column like
// SUM but renders an untouched accumulator as a concrete `0` (null bit clear),
// not NULL. The two-phase global-aggregate combine uses it to sum per-worker
// partial COUNT/COUNT_NON_NULL columns (a COUNT's empty value is `0`, not NULL).
// ---------------------------------------------------------------------------

/// SumZero folds values exactly like Sum, is linear (keeps the combine on the
/// linear fast path), and an untouched accumulator renders `0`, not NULL.
#[test]
fn sumzero_folds_like_sum_and_empty_renders_zero() {
    assert!(AggOp::SumZero.is_linear(), "SumZero must be linear");

    let schema = g_src();
    let desc = AggDescriptor {
        col_idx: 1,
        agg_op: AggOp::SumZero,
        col_type_code: TypeCode::I64,
        _pad: [0; 2],
    };
    let mut acc = Accumulator::new(&desc, &schema);

    // Fresh: untouched, and renders 0 (Count's identity) rather than NULL (Sum's).
    assert!(acc.is_untouched(), "fresh SumZero is untouched");
    assert!(acc.empty_renders_zero(), "an untouched SumZero renders 0, not NULL");

    // Folds values with weight, exactly like Sum: 5·(+1) + 10·(+1) + 7·(−1) = 8.
    let batch = g_delta(&[(1, 1, 5), (2, 1, 10), (3, -1, 7)]);
    let mb = batch.as_mem_batch();
    for row in 0..batch.count {
        acc.step_from_batch(&mb, row, mb.get_weight(row));
    }
    assert!(!acc.is_untouched(), "SumZero with input is touched");
    assert_eq!(
        acc.get_value_bits() as i64,
        8,
        "SumZero sums its source values (5 + 10 − 7)"
    );
}

/// `COUNT(col)` over an all-NULL group renders a concrete `0` with the null bit
/// **clear**, never NULL. Every row hits `step_from_batch`'s null gate, so the
/// CountNonNull accumulator stays untouched; the emitted column's null bit is the
/// only observable distinguishing `0` from NULL (the value bytes are zero either
/// way). Regression guard for the COUNT-family-renders-NULL emitter bug.
#[test]
fn count_non_null_all_null_group_renders_zero_null_clear() {
    let in_schema = g_src(); // [U64 pk, I64 payload(nullable)]
    let desc = AggDescriptor {
        col_idx: 1,
        agg_op: AggOp::CountNonNull,
        col_type_code: TypeCode::I64,
        _pad: [0; 2],
    };
    let mut acc = Accumulator::new(&desc, &in_schema);

    // Two rows whose payload column (payload slot 0) is NULL.
    let mut batch = Batch::with_schema(g_src(), 2);
    for pk in [1u128, 2u128] {
        batch.extend_pk(pk);
        batch.extend_weight(&1i64.to_le_bytes());
        batch.extend_null_bmp(&1u64.to_le_bytes()); // payload slot 0 = NULL
        batch.extend_col(0, &0i64.to_le_bytes());
        batch.count += 1;
    }
    batch.set_layout_unchecked(Layout::Consolidated);
    let mb = batch.as_mem_batch();
    for row in 0..batch.count {
        acc.step_from_batch(&mb, row, mb.get_weight(row));
    }
    assert!(acc.is_untouched(), "all-NULL group leaves CountNonNull untouched");

    // Emit the group row. Natural-PK grouping on the U64 PK col: output is
    // [U64 pk, I64 count], no group-exemplar column.
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    );
    let mut output = Batch::with_schema(out_schema, 1);
    let accs = vec![acc];
    emit_reduce_row(
        &mut output,
        &mb,
        0,
        mb.get_pk_bytes(0),
        &accs,
        &in_schema,
        &out_schema,
        &[0u32],
        true, /* use_natural_pk */
        1,
    );

    assert_eq!(output.count, 1);
    let out_mb = output.as_mem_batch();
    assert_eq!(
        out_mb.get_null_word(0) & 1,
        0,
        "COUNT(col) of all-NULL group must have the null bit CLEAR (renders 0, not NULL)",
    );
    assert_eq!(
        read_i64_le(out_mb.get_col_ptr(0, 0, 8), 0),
        0,
        "COUNT(col) of all-NULL group decodes to 0",
    );
}

/// `emit_global_ground` still renders the COUNT family as a concrete `0` with the
/// null bit clear after the explicit COUNT seed loop was deleted — the untouched
/// accumulators now render via `empty_renders_zero`. Guards that deletion for both
/// COUNT(*) and COUNT(col) ground columns.
#[test]
fn emit_global_ground_renders_count_family_zero_null_clear() {
    let in_schema = g_src();
    // Global-aggregate output: [_group_pk:U128, count_star:I64, count_col:I64].
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    );
    let descs = [
        AggDescriptor {
            col_idx: 0,
            agg_op: AggOp::Count,
            col_type_code: TypeCode::U64,
            _pad: [0; 2],
        },
        AggDescriptor {
            col_idx: 1,
            agg_op: AggOp::CountNonNull,
            col_type_code: TypeCode::I64,
            _pad: [0; 2],
        },
    ];
    let mut raw_output = Batch::with_schema(out_schema, 1);
    let v0 = [0u8; 16]; // U128 ground PK (V₀)
    emit_global_ground(
        &mut raw_output,
        &v0,
        &descs,
        &in_schema,
        &out_schema,
        &[], // no group-by columns
    );

    assert_eq!(raw_output.count, 1, "ground row emitted");
    let out_mb = raw_output.as_mem_batch();
    // Both count columns (payload slots 0 and 1) render 0 with the null bit clear.
    assert_eq!(
        out_mb.get_null_word(0) & 0b11,
        0,
        "COUNT(*) and COUNT(col) ground columns must be null-clear",
    );
    assert_eq!(
        read_i64_le(out_mb.get_col_ptr(0, 0, 8), 0),
        0,
        "COUNT(*) ground value is 0"
    );
    assert_eq!(
        read_i64_le(out_mb.get_col_ptr(0, 1, 8), 0),
        0,
        "COUNT(col) ground value is 0"
    );
}

/// Combine-input partial schema: `[_group_pk:U128, cnt:I64(nullable)]` — one
/// per-worker partial count column, all rows at PK V₀ (the local reduce output).
fn combine_partial_schema() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::I64, 1),
        ],
        &[0],
    )
}

/// Combine output `[_group_pk:U128, combined:I64, count_of_partials:I64]`: the
/// summed partial column plus the trailing COUNT-of-partials existence gate.
fn combine_out_schema() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    )
}

/// Build a combine-input delta of partials at PK V₀ from `(weight, Option<value>)`
/// (a `None` value is a NULL partial count column).
fn combine_partials(rows: &[(i64, Option<i64>)]) -> Batch {
    let schema = combine_partial_schema();
    let v0 = crate::ops::global_group_key();
    let mut b = Batch::with_schema(schema, rows.len().max(1));
    for &(w, val) in rows {
        b.extend_pk(v0);
        b.extend_weight(&w.to_le_bytes());
        match val {
            Some(v) => {
                b.extend_null_bmp(&0u64.to_le_bytes());
                b.extend_col(0, &v.to_le_bytes());
            }
            None => {
                b.extend_null_bmp(&1u64.to_le_bytes()); // cnt (payload idx 0) NULL
                b.extend_col(0, &0i64.to_le_bytes());
            }
        }
        b.count += 1;
    }
    b
}

// SumZero merges the partial counts; a trailing Count counts partial rows (the
// existence gate `op_reduce` finds via the lone AggOp::Count).
const C_SUMZERO: AggDescriptor = AggDescriptor {
    col_idx: 1,
    agg_op: AggOp::SumZero,
    col_type_code: TypeCode::I64,
    _pad: [0; 2],
};
const C_COUNT_PARTIALS: AggDescriptor = AggDescriptor {
    col_idx: 0,
    agg_op: AggOp::Count,
    col_type_code: TypeCode::U128,
    _pad: [0; 2],
};

/// The phase-3 combine `op_reduce` over hand-built partials (empty group cols →
/// one global group at V₀, `global_ground = true`, `i_am_owner = true`).
fn combine_reduce(delta: &Batch, trace_out: &mut crate::storage::ReadCursor, out_schema: &SchemaDescriptor) -> Batch {
    let in_schema = combine_partial_schema();
    op_reduce(
        delta,
        None,
        trace_out,
        &in_schema,
        out_schema,
        &[], // empty group cols ⇒ one global group at V₀
        &[C_SUMZERO, C_COUNT_PARTIALS],
        None,
        None,
        None,
        true, // global_ground
        true, // i_am_owner (V₀'s owner)
    )
    .0
}

/// The combine sums the partial *count values* (3 + 5 = 8), not the number of
/// partials (2). Consolidation of identical partials nets by weight.
#[test]
fn combine_sums_partial_counts_not_partial_rows() {
    use crate::storage::ReadCursor;
    use std::rc::Rc;

    let out_schema = combine_out_schema();
    let empty_out = Rc::new(Batch::empty_with_schema(&out_schema));
    let mut to_ch = ReadCursor::from_owned(&[empty_out], out_schema);

    // Two workers' partials: counts 3 and 5.
    let out = combine_reduce(
        &combine_partials(&[(1, Some(3)), (1, Some(5))]),
        &mut to_ch,
        &out_schema,
    );
    assert_eq!(out.count, 1, "one combined row");
    assert_eq!(out.get_pk(0), crate::ops::global_group_key(), "combined row at V₀");
    assert_eq!(
        read_i64_le(out.col_data(0), 0),
        8,
        "SumZero sums partial counts (3 + 5), not the partial count (#partials = 2)"
    );
    assert_eq!(read_i64_le(out.col_data(1), 0), 2, "COUNT-of-partials gate = 2");
}

/// All-NULL partials (every worker's COUNT(col) is NULL — a fresh all-NULL
/// column) combine to a concrete `0` with the null bit clear, never NULL.
#[test]
fn combine_all_null_partials_render_zero_not_null() {
    use crate::storage::ReadCursor;
    use std::rc::Rc;

    let out_schema = combine_out_schema();
    let empty_out = Rc::new(Batch::empty_with_schema(&out_schema));
    let mut to_ch = ReadCursor::from_owned(&[empty_out], out_schema);

    // Two non-empty workers, each with a NULL partial count → SumZero untouched.
    let out = combine_reduce(&combine_partials(&[(1, None), (1, None)]), &mut to_ch, &out_schema);
    assert_eq!(out.count, 1, "non-empty global (2 partials) emits one combined row");
    assert_eq!(read_i64_le(out.col_data(0), 0), 0, "all-NULL COUNT(col) combines to 0");
    assert_eq!(
        out.get_null_word(0) & 1,
        0,
        "combined COUNT(col) is 0 with null bit CLEAR, not NULL"
    );
    assert_eq!(
        read_i64_le(out.col_data(1), 0),
        2,
        "still 2 partials (global non-empty)"
    );
}

/// Full retraction nets the COUNT-of-partials to 0; the gate sheds the computed
/// row and the combine emits the ground (combined = 0 via SumZero, null clear).
#[test]
fn combine_full_retraction_sheds_to_ground() {
    use crate::storage::ReadCursor;
    use std::rc::Rc;

    let out_schema = combine_out_schema();
    let empty_out = Rc::new(Batch::empty_with_schema(&out_schema));
    let mut to_ch = ReadCursor::from_owned(&[empty_out], out_schema);

    let out1 = combine_reduce(&combine_partials(&[(1, Some(5))]), &mut to_ch, &out_schema);
    assert_eq!(read_i64_le(out1.col_data(0), 0), 5, "combined = 5");

    // Retract the only partial → COUNT-of-partials nets to 0.
    let prev = Rc::new(out1);
    let mut to_ch2 = ReadCursor::from_owned(&[prev], out_schema);
    let out2 = combine_reduce(&combine_partials(&[(-1, Some(5))]), &mut to_ch2, &out_schema);
    assert_eq!(out2.count, 2, "retract old computed (−1) + ground insert (+1)");
    let mb = out2.as_mem_batch();
    let pos = (0..2).find(|&i| mb.get_weight(i) == 1).expect("a +1 row");
    assert_eq!(
        read_i64_le(out2.col_data(0), pos * 8),
        0,
        "ground combined count = 0 (SumZero's `0` identity)"
    );
    assert_eq!(
        out2.get_null_word(pos) & 1,
        0,
        "combined count present (SumZero grounds to 0, not NULL)"
    );
}

// ===========================================================================
// Combined AggValueIndex: one table per reduce, keyed `group ‖ ordinal ‖ av`,
// serving every MIN/MAX aggregate (grouped or global). These drive op_reduce
// through a real combined index populated by the production
// `op_integrate_with_indexes` flow, so the per-ordinal write and read sides
// agree with no hand-built keys.
// ===========================================================================

/// Create an ephemeral combined-AVI table and populate it by integrating each
/// delta in `deltas` (the accumulated integral the index must reflect at read
/// time) through the real `op_integrate_with_indexes`. The caller opens a cursor
/// on the returned table.
fn build_combined_avi(
    dir: &std::path::Path,
    in_schema: &SchemaDescriptor,
    group_cols: &[u32],
    agg_descs: &[AggDescriptor],
    deltas: &[&Batch],
) -> crate::storage::Table {
    use crate::ops::index::{make_avi_schema, op_integrate_with_indexes, AviDesc};
    let avi_schema = make_avi_schema(in_schema, group_cols);
    let mut t = crate::storage::Table::new(
        dir.to_str().unwrap(),
        avi_schema,
        0,
        1 << 20,
        crate::storage::RecoverySource::Rederive,
    )
    .unwrap();
    // The value-indexed (MIN/MAX) subset, in descriptor order — exactly what the
    // compiler feeds the integrate instruction (see `emit_reduce`).
    let vi_aggs: Vec<AggDescriptor> = agg_descs
        .iter()
        .filter(|d| d.agg_op.uses_value_index())
        .copied()
        .collect();
    let avi = AviDesc {
        table: &mut t as *mut crate::storage::Table,
        group_by_cols: group_cols,
        aggs: &vi_aggs,
    };
    for d in deltas {
        op_integrate_with_indexes(d, None, in_schema, Some(&avi)).unwrap();
    }
    t
}

/// Schema `[pk:U64, g:I32, a:I64, b:I64]` (group by the I32 payload `g`).
fn cg_src() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I32, 0),
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    )
}

/// Raw delta over `cg_src()` from `(pk, w, g, a, b)` rows.
fn cg_delta(rows: &[(u64, i64, i32, i64, i64)]) -> Batch {
    let s = cg_src();
    let mut b = Batch::with_schema(s, rows.len().max(1));
    for &(pk, w, g, a, bb) in rows {
        b.extend_pk(pk as u128);
        b.extend_weight(&w.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(s.try_payload_idx(1).unwrap(), &g.to_le_bytes());
        b.extend_col(s.try_payload_idx(2).unwrap(), &a.to_le_bytes());
        b.extend_col(s.try_payload_idx(3).unwrap(), &bb.to_le_bytes());
        b.count += 1;
    }
    b
}

/// Foreign-group regression: a source PK appearing in two groups must not leak
/// one group's rows into the other's extremes. The GI over-read this fix removes
/// would corrupt MIN/MAX here; the combined index isolates each group by its key
/// prefix. The delta values are deliberately interleaved so a correct result can
/// only come from per-group isolation.
#[test]
fn reduce_multi_avi_foreign_group() {
    use crate::storage::ReadCursor;
    use std::rc::Rc;

    let in_schema = cg_src();
    // Output: [_group_pk:U128, g:I32, min_a:I64(nullable), max_b:I64(nullable), count:I64].
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::I32, 0),
            SchemaColumn::new(type_code::I64, 1),
            SchemaColumn::new(type_code::I64, 1),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    );
    let aggs = [
        AggDescriptor {
            col_idx: 2,
            agg_op: AggOp::Min,
            col_type_code: TypeCode::I64,
            _pad: [0; 2],
        },
        AggDescriptor {
            col_idx: 3,
            agg_op: AggOp::Max,
            col_type_code: TypeCode::I64,
            _pad: [0; 2],
        },
        AggDescriptor {
            col_idx: 0,
            agg_op: AggOp::Count,
            col_type_code: TypeCode::U64,
            _pad: [0; 2],
        },
    ];

    // pk=1 and pk=2 each span groups g=10 and g=20 (the foreign-group trigger).
    let delta = cg_delta(&[
        (1, 1, 10, 5, 100),
        (1, 1, 20, 7, 200),
        (2, 1, 10, 3, 50),
        (2, 1, 20, 9, 300),
    ]);

    let tmp = tempfile::tempdir().unwrap();
    let avi_t = build_combined_avi(tmp.path(), &in_schema, &[1u32], &aggs, &[&delta]);
    let mut avi_ch = avi_t.open_cursor();

    let empty_out = Rc::new(Batch::empty_with_schema(&out_schema));
    let mut to_ch = ReadCursor::from_owned(&[empty_out], out_schema);

    let (out, _) = op_reduce(
        &delta,
        None,
        &mut to_ch,
        &in_schema,
        &out_schema,
        &[1u32],
        &aggs,
        Some(&mut avi_ch),
        None,
        None,
        false,
        false,
    );

    assert_eq!(out.count, 2, "two groups → two rows");
    for i in 0..out.count {
        let g = crate::foundation::codec::read_u32_le(out.col_data(0), i * 4) as i32;
        let min_a = read_i64_le(out.col_data(1), i * 8);
        let max_b = read_i64_le(out.col_data(2), i * 8);
        let count = read_i64_le(out.col_data(3), i * 8);
        match g {
            10 => {
                assert_eq!(min_a, 3, "group 10 MIN(a) excludes group 20");
                assert_eq!(max_b, 100, "group 10 MAX(b) excludes group 20");
                assert_eq!(count, 2);
            }
            20 => {
                assert_eq!(min_a, 7, "group 20 MIN(a) excludes group 10");
                assert_eq!(max_b, 300, "group 20 MAX(b) excludes group 10");
                assert_eq!(count, 2);
            }
            other => panic!("unexpected group {other}"),
        }
    }
}

/// Schema `[pk:U64, g:I32, a:I64]`; output `[_group_pk:U128, g:I32, min:I64?, count:I64]`.
fn cg3_src() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I32, 0),
            SchemaColumn::new(type_code::I64, 1),
        ],
        &[0],
    )
}
fn cg3_out() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::I32, 0),
            SchemaColumn::new(type_code::I64, 1),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    )
}
/// `(pk, w, g, a, a_null)` delta over `cg3_src()`.
fn cg3_delta(rows: &[(u64, i64, i32, i64, bool)]) -> Batch {
    let s = cg3_src();
    let mut b = Batch::with_schema(s, rows.len().max(1));
    for &(pk, w, g, a, a_null) in rows {
        b.extend_pk(pk as u128);
        b.extend_weight(&w.to_le_bytes());
        let null_word = if a_null {
            1u64 << s.try_payload_idx(2).unwrap()
        } else {
            0
        };
        b.extend_null_bmp(&null_word.to_le_bytes());
        b.extend_col(s.try_payload_idx(1).unwrap(), &g.to_le_bytes());
        b.extend_col(s.try_payload_idx(2).unwrap(), &a.to_le_bytes());
        b.count += 1;
    }
    b
}
const CG3_MIN: AggDescriptor = AggDescriptor {
    col_idx: 2,
    agg_op: AggOp::Min,
    col_type_code: TypeCode::I64,
    _pad: [0; 2],
};
const CG3_COUNT: AggDescriptor = AggDescriptor {
    col_idx: 0,
    agg_op: AggOp::Count,
    col_type_code: TypeCode::U64,
    _pad: [0; 2],
};

/// Helper: run one combined-index reduce tick over `cg3_src()` (MIN + COUNT).
/// `avi_deltas` is the accumulated integral; `delta` is this tick's input.
fn cg3_tick(
    dir: &std::path::Path,
    avi_deltas: &[&Batch],
    delta: &Batch,
    aggs: &[AggDescriptor],
    trace_out: &mut crate::storage::ReadCursor,
) -> Batch {
    let in_schema = cg3_src();
    let out_schema = cg3_out();
    let avi_t = build_combined_avi(dir, &in_schema, &[1u32], aggs, avi_deltas);
    let mut avi_ch = avi_t.open_cursor();
    op_reduce(
        delta,
        None,
        trace_out,
        &in_schema,
        &out_schema,
        &[1u32],
        aggs,
        Some(&mut avi_ch),
        None,
        None,
        false,
        false,
    )
    .0
}

/// Linear companion folded alongside the indexed MIN: COUNT = old + Σdelta, MIN
/// from the index, across an insert then a partial retraction.
#[test]
fn reduce_multi_avi_linear_companion() {
    use crate::storage::ReadCursor;
    use std::rc::Rc;
    let aggs = [CG3_MIN, CG3_COUNT];
    let tmp = tempfile::tempdir().unwrap();
    let out_schema = cg3_out();

    // Tick 1: insert g=7 {a=5, a=8, a=3}.
    let d1 = cg3_delta(&[(1, 1, 7, 5, false), (2, 1, 7, 8, false), (3, 1, 7, 3, false)]);
    let empty = Rc::new(Batch::empty_with_schema(&out_schema));
    let mut to1 = ReadCursor::from_owned(&[empty], out_schema);
    let out1 = cg3_tick(tmp.path(), &[&d1], &d1, &aggs, &mut to1);
    assert_eq!(out1.count, 1);
    assert_eq!(read_i64_le(out1.col_data(1), 0), 3, "MIN=3 from the index");
    assert_eq!(read_i64_le(out1.col_data(2), 0), 3, "COUNT=3");

    // Tick 2: retract a=3 (the current min). MIN→5 (index post-state), COUNT 3→2.
    let d2 = cg3_delta(&[(3, -1, 7, 3, false)]);
    let prev = Rc::new(out1);
    let mut to2 = ReadCursor::from_owned(&[prev], out_schema);
    let out2 = cg3_tick(tmp.path(), &[&d1, &d2], &d2, &aggs, &mut to2);
    // retract old (MIN=3,count=3 @ -1) + insert new (MIN=5,count=2 @ +1).
    let mb = out2.as_mem_batch();
    let ins = (0..out2.count).find(|&i| mb.get_weight(i) == 1).expect("a +1 row");
    assert_eq!(
        read_i64_le(out2.col_data(1), ins * 8),
        5,
        "MIN recomputed to 5 from the index"
    );
    assert_eq!(
        read_i64_le(out2.col_data(2), ins * 8),
        2,
        "COUNT = old(3) + Σdelta(-1) = 2"
    );
}

/// Phantom-bug regression: a group fully retracted in a tick must emit exactly
/// one −1 retraction and NO +1 (the folded companion COUNT nets to 0, which the
/// cardinality gate reads to suppress the row).
#[test]
fn reduce_multi_avi_emptied_with_companion() {
    use crate::storage::ReadCursor;
    use std::rc::Rc;
    let aggs = [CG3_MIN, CG3_COUNT];
    let tmp = tempfile::tempdir().unwrap();
    let out_schema = cg3_out();

    let d1 = cg3_delta(&[(1, 1, 7, 5, false), (2, 1, 7, 8, false)]);
    let empty = Rc::new(Batch::empty_with_schema(&out_schema));
    let mut to1 = ReadCursor::from_owned(&[empty], out_schema);
    let out1 = cg3_tick(tmp.path(), &[&d1], &d1, &aggs, &mut to1);
    assert_eq!(out1.count, 1);

    // Tick 2: retract both rows → group emptied.
    let d2 = cg3_delta(&[(1, -1, 7, 5, false), (2, -1, 7, 8, false)]);
    let prev = Rc::new(out1);
    let mut to2 = ReadCursor::from_owned(&[prev], out_schema);
    let out2 = cg3_tick(tmp.path(), &[&d1, &d2], &d2, &aggs, &mut to2);
    assert_eq!(
        out2.count, 1,
        "emptied group: only the −1 retraction, no phantom insert"
    );
    assert_eq!(out2.get_weight(0), -1, "the sole row is the retraction");
}

/// Cardinality semantics: a group whose rows are all-NULL in the aggregate
/// column still exists (count > 0) and must emit `(g, NULL, count)`.
#[test]
fn reduce_multi_avi_all_null_emit() {
    use crate::storage::ReadCursor;
    use std::rc::Rc;
    let aggs = [CG3_MIN, CG3_COUNT];
    let tmp = tempfile::tempdir().unwrap();
    let out_schema = cg3_out();

    // Group 7 with two rows, both a = NULL.
    let d1 = cg3_delta(&[(1, 1, 7, 0, true), (2, 1, 7, 0, true)]);
    let empty = Rc::new(Batch::empty_with_schema(&out_schema));
    let mut to1 = ReadCursor::from_owned(&[empty], out_schema);
    let out1 = cg3_tick(tmp.path(), &[&d1], &d1, &aggs, &mut to1);
    assert_eq!(out1.count, 1, "all-NULL group with rows must still emit");
    assert_eq!(out1.get_weight(0), 1);
    assert_eq!(read_i64_le(out1.col_data(2), 0), 2, "COUNT=2");
    // MIN null bit: payload col 1 (min) → null-word bit 1.
    assert_ne!(
        out1.get_null_word(0) & (1 << 1),
        0,
        "MIN renders NULL for all-NULL group"
    );
}

/// Nullable MIN over the combined index: a mixed group `{a=5, a=NULL}` whose
/// non-null row is retracted must SURVIVE as `(g, NULL, 1)` — not be dropped —
/// then disappear when the last (NULL) row is retracted. Regression for the
/// all-NULL-group drop the cardinality gate fixes, while the nullable MIN stays
/// on the combined index.
#[test]
fn reduce_multi_avi_retract_to_all_null() {
    use crate::storage::ReadCursor;
    use std::rc::Rc;
    let aggs = [CG3_MIN, CG3_COUNT];
    let tmp = tempfile::tempdir().unwrap();
    let out_schema = cg3_out();

    // Tick 1: g=7 = {a=5, a=NULL}. MIN=5, COUNT=2.
    let d1 = cg3_delta(&[(1, 1, 7, 5, false), (2, 1, 7, 0, true)]);
    let empty = Rc::new(Batch::empty_with_schema(&out_schema));
    let mut to1 = ReadCursor::from_owned(&[empty], out_schema);
    let out1 = cg3_tick(tmp.path(), &[&d1], &d1, &aggs, &mut to1);
    assert_eq!(out1.count, 1);
    assert_eq!(read_i64_le(out1.col_data(1), 0), 5, "MIN=5");

    // Tick 2: retract a=5. Group survives via the NULL row: (g, NULL, 1).
    let d2 = cg3_delta(&[(1, -1, 7, 5, false)]);
    let prev = Rc::new(out1);
    let mut to2 = ReadCursor::from_owned(&[prev], out_schema);
    let out2 = cg3_tick(tmp.path(), &[&d1, &d2], &d2, &aggs, &mut to2);
    let mb2 = out2.as_mem_batch();
    let ins = (0..out2.count)
        .find(|&i| mb2.get_weight(i) == 1)
        .expect("a +1 row — group survives");
    assert_eq!(read_i64_le(out2.col_data(2), ins * 8), 1, "COUNT=1 (the NULL row)");
    assert_ne!(out2.get_null_word(ins) & (1 << 1), 0, "MIN now NULL but group present");

    // Rebuild tick-2 output row to seed tick-3 trace_out.
    let row2 = Rc::new({
        let mut b = Batch::with_schema(out_schema, 1);
        // copy the +1 row out of out2
        b.extend_pk_bytes(out2.get_pk_bytes(ins));
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&out2.get_null_word(ins).to_le_bytes());
        b.extend_col(0, &out2.col_data(0)[ins * 4..ins * 4 + 4]);
        b.extend_col(1, &out2.col_data(1)[ins * 8..ins * 8 + 8]);
        b.extend_col(2, &out2.col_data(2)[ins * 8..ins * 8 + 8]);
        b.count += 1;
        b.set_layout_unchecked(Layout::Consolidated);
        b
    });

    // Tick 3: retract the remaining NULL row → group gone (only the −1).
    let d3 = cg3_delta(&[(2, -1, 7, 0, true)]);
    let mut to3 = ReadCursor::from_owned(&[row2], out_schema);
    let out3 = cg3_tick(tmp.path(), &[&d1, &d2, &d3], &d3, &aggs, &mut to3);
    assert_eq!(out3.count, 1, "group now absent: only the −1 retraction");
    assert_eq!(out3.get_weight(0), -1);
}

/// `MIN(a), MAX(a)` over the SAME column: two ordinals (opposite `for_max`) in
/// one combined index. The ordinal byte keeps them from colliding.
#[test]
fn reduce_multi_avi_same_col_min_max() {
    use crate::storage::ReadCursor;
    use std::rc::Rc;
    let in_schema = cg3_src();
    // Output: [_group_pk:U128, g:I32, min:I64?, max:I64?, count:I64].
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::I32, 0),
            SchemaColumn::new(type_code::I64, 1),
            SchemaColumn::new(type_code::I64, 1),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    );
    let aggs = [
        AggDescriptor {
            col_idx: 2,
            agg_op: AggOp::Min,
            col_type_code: TypeCode::I64,
            _pad: [0; 2],
        },
        AggDescriptor {
            col_idx: 2,
            agg_op: AggOp::Max,
            col_type_code: TypeCode::I64,
            _pad: [0; 2],
        },
        CG3_COUNT,
    ];
    let delta = cg3_delta(&[(1, 1, 7, 5, false), (2, 1, 7, 8, false), (3, 1, 7, 1, false)]);

    let tmp = tempfile::tempdir().unwrap();
    let avi_t = build_combined_avi(tmp.path(), &in_schema, &[1u32], &aggs, &[&delta]);
    let mut avi_ch = avi_t.open_cursor();
    let empty = Rc::new(Batch::empty_with_schema(&out_schema));
    let mut to = ReadCursor::from_owned(&[empty], out_schema);
    let (out, _) = op_reduce(
        &delta,
        None,
        &mut to,
        &in_schema,
        &out_schema,
        &[1u32],
        &aggs,
        Some(&mut avi_ch),
        None,
        None,
        false,
        false,
    );
    assert_eq!(out.count, 1);
    assert_eq!(read_i64_le(out.col_data(1), 0), 1, "MIN(a)=1 (ordinal 0)");
    assert_eq!(
        read_i64_le(out.col_data(2), 0),
        8,
        "MAX(a)=8 (ordinal 1), no ordinal collision"
    );
}

/// Compound group key `(g1, g2)` (both fixed-int, non-nullable) resolves via the
/// combined index. Two group columns + the ordinal + the value stay within the
/// PK budget.
#[test]
fn reduce_multi_avi_compound_group_key() {
    use crate::storage::ReadCursor;
    use std::rc::Rc;
    let in_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I32, 0),
            SchemaColumn::new(type_code::I32, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    );
    // Synthetic group (g1, g2): [_group_pk:U128, g1:I32, g2:I32, min:I64?, count:I64].
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::I32, 0),
            SchemaColumn::new(type_code::I32, 0),
            SchemaColumn::new(type_code::I64, 1),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    );
    let aggs = [
        AggDescriptor {
            col_idx: 3,
            agg_op: AggOp::Min,
            col_type_code: TypeCode::I64,
            _pad: [0; 2],
        },
        AggDescriptor {
            col_idx: 0,
            agg_op: AggOp::Count,
            col_type_code: TypeCode::U64,
            _pad: [0; 2],
        },
    ];
    let delta = {
        let mut b = Batch::with_schema(in_schema, 4);
        for (pk, g1, g2, a) in [(1u64, 1i32, 1i32, 9i64), (2, 1, 1, 4), (3, 1, 2, 7), (4, 1, 2, 2)] {
            b.extend_pk(pk as u128);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(in_schema.try_payload_idx(1).unwrap(), &g1.to_le_bytes());
            b.extend_col(in_schema.try_payload_idx(2).unwrap(), &g2.to_le_bytes());
            b.extend_col(in_schema.try_payload_idx(3).unwrap(), &a.to_le_bytes());
            b.count += 1;
        }
        b
    };

    let tmp = tempfile::tempdir().unwrap();
    let avi_t = build_combined_avi(tmp.path(), &in_schema, &[1u32, 2u32], &aggs, &[&delta]);
    let mut avi_ch = avi_t.open_cursor();
    let empty = Rc::new(Batch::empty_with_schema(&out_schema));
    let mut to = ReadCursor::from_owned(&[empty], out_schema);
    let (out, _) = op_reduce(
        &delta,
        None,
        &mut to,
        &in_schema,
        &out_schema,
        &[1u32, 2u32],
        &aggs,
        Some(&mut avi_ch),
        None,
        None,
        false,
        false,
    );
    assert_eq!(out.count, 2, "two compound groups");
    for i in 0..out.count {
        let g1 = crate::foundation::codec::read_u32_le(out.col_data(0), i * 4) as i32;
        let g2 = crate::foundation::codec::read_u32_le(out.col_data(1), i * 4) as i32;
        let min = read_i64_le(out.col_data(2), i * 8);
        match (g1, g2) {
            (1, 1) => assert_eq!(min, 4, "group (1,1) MIN"),
            (1, 2) => assert_eq!(min, 2, "group (1,2) MIN"),
            other => panic!("unexpected group {other:?}"),
        }
    }
}

/// Global `MIN(a), SUM(b)` (no GROUP BY) served by the combined index, fully
/// retracted: the emptied source emits the ground row (MIN=NULL, SUM=NULL,
/// COUNT=0) — NOT a phantom SUM=0. Guards the `|| global_ground` disjunct of
/// `cardinality_idx`.
#[test]
fn reduce_multi_avi_global_emptied() {
    use crate::storage::ReadCursor;
    use std::rc::Rc;
    // [pk:U64, a:I64, b:I64]; global aggregate.
    let in_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    );
    // Output: [_group_pk:U128, min:I64?, sum:I64?, count:I64].
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::I64, 1),
            SchemaColumn::new(type_code::I64, 1),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    );
    let aggs = [
        AggDescriptor {
            col_idx: 1,
            agg_op: AggOp::Min,
            col_type_code: TypeCode::I64,
            _pad: [0; 2],
        },
        AggDescriptor {
            col_idx: 2,
            agg_op: AggOp::Sum,
            col_type_code: TypeCode::I64,
            _pad: [0; 2],
        },
        AggDescriptor {
            col_idx: 0,
            agg_op: AggOp::Count,
            col_type_code: TypeCode::U64,
            _pad: [0; 2],
        },
    ];
    let mk = |rows: &[(u64, i64, i64, i64)]| {
        let mut b = Batch::with_schema(in_schema, rows.len().max(1));
        for &(pk, w, a, bb) in rows {
            b.extend_pk(pk as u128);
            b.extend_weight(&w.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(in_schema.try_payload_idx(1).unwrap(), &a.to_le_bytes());
            b.extend_col(in_schema.try_payload_idx(2).unwrap(), &bb.to_le_bytes());
            b.count += 1;
        }
        b
    };
    let tmp = tempfile::tempdir().unwrap();

    let run = |avi_deltas: &[&Batch], delta: &Batch, to: &mut crate::storage::ReadCursor| -> Batch {
        let avi_t = build_combined_avi(tmp.path(), &in_schema, &[], &aggs, avi_deltas);
        let mut avi_ch = avi_t.open_cursor();
        op_reduce(
            delta,
            None,
            to,
            &in_schema,
            &out_schema,
            &[],
            &aggs,
            Some(&mut avi_ch),
            None,
            None,
            true,
            true,
        )
        .0
    };

    // Tick 1: insert {a=5,b=10},{a=3,b=20}. MIN=3, SUM=30, COUNT=2.
    let d1 = mk(&[(1, 1, 5, 10), (2, 1, 3, 20)]);
    let empty = Rc::new(Batch::empty_with_schema(&out_schema));
    let mut to1 = ReadCursor::from_owned(&[empty], out_schema);
    let out1 = run(&[&d1], &d1, &mut to1);
    assert_eq!(out1.count, 1);
    assert_eq!(read_i64_le(out1.col_data(0), 0), 3, "MIN=3");
    assert_eq!(read_i64_le(out1.col_data(1), 0), 30, "SUM=30");

    // Tick 2: retract everything → ground row (MIN=NULL, SUM=NULL, COUNT=0).
    let d2 = mk(&[(1, -1, 5, 10), (2, -1, 3, 20)]);
    let prev = Rc::new(out1);
    let mut to2 = ReadCursor::from_owned(&[prev], out_schema);
    let out2 = run(&[&d1, &d2], &d2, &mut to2);
    let mb = out2.as_mem_batch();
    let ins = (0..out2.count)
        .find(|&i| mb.get_weight(i) == 1)
        .expect("a +1 ground row");
    assert_ne!(out2.get_null_word(ins) & (1 << 0), 0, "ground MIN = NULL");
    assert_ne!(
        out2.get_null_word(ins) & (1 << 1),
        0,
        "ground SUM = NULL (not a phantom 0)"
    );
    assert_eq!(read_i64_le(out2.col_data(2), ins * 8), 0, "ground COUNT = 0");
}

// ===========================================================================
// Monotone trace_out probe for canonical single-column group keys.
//
// A canonical single-column GROUP BY — a single PK sub-column (Pk arm) or a
// single non-nullable routable-int payload column (Payload arm) — visits
// groups in ascending output-PK order, so `op_reduce` upgrades its `trace_out`
// retraction probe from the absolute `seek_bytes` to the galloping
// `advance_to`. These tests drive that path across multi-epoch retraction
// (incl. sign-flip boundaries), confirm a nullable group key stays on the
// absolute seek (hash arm), force `SourceMode::Multi` so the multi-source
// gallop and the debug ascending tripwire run at scale, and pin the shared
// classifier directly.
// ===========================================================================

/// One input row over a `[U64 pk, <grp>, I64 val]` schema: `(pk, grp, val,
/// weight)`. `grp == None` marks a NULL group (nullable schemas only). Shared
/// by the monotone-probe tests below and the non-linear fallback tests above.
type GrpValRow = (u64, Option<i64>, i64, i64);

/// Output schema for a synthetic-U128-PK `SUM/COUNT` reduce:
/// `[U128 pk, <grp>, I64 sum(nullable), I64 count]`. The trailing count is the
/// cardinality companion every all-linear reduce carries.
fn sum_count_out_synthetic(grp_col: SchemaColumn) -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0),
            grp_col,
            SchemaColumn::new(type_code::I64, 1),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    )
}

/// Build an input delta over `[U64 pk, <grp>, I64 val]` from `(pk, grp, val, w)`
/// rows; `grp == None` marks the group column NULL (nullable schemas only). The
/// grp column is written from an i64 image, which is byte-identical to the u64
/// image for the non-negative values the U64-group test uses.
fn build_grp_val_delta(schema: &SchemaDescriptor, rows: &[GrpValRow]) -> Batch {
    let g_pi = schema.try_payload_idx(1).unwrap();
    let v_pi = schema.try_payload_idx(2).unwrap();
    let mut b = Batch::with_schema(*schema, rows.len().max(1));
    for &(pk, grp, val, w) in rows {
        b.extend_pk(pk as u128);
        b.extend_weight(&w.to_le_bytes());
        let null_word = if grp.is_none() { 1u64 << g_pi } else { 0 };
        b.extend_null_bmp(&null_word.to_le_bytes());
        b.extend_col(g_pi, &grp.unwrap_or(0).to_le_bytes());
        b.extend_col(v_pi, &val.to_le_bytes());
        b.count += 1;
    }
    b.set_layout_unchecked(Layout::Consolidated);
    b
}

/// DBSP linear reference for `SUM(val)/COUNT` over the raw input rows: both
/// aggregates are linear, so `sum[g] += val·w` and `count[g] += w` over every
/// input row across every epoch; a group is live iff `count ≠ 0`. A NULL group
/// (`None`) is its own group.
fn sum_count_reference<'a>(
    rows: impl IntoIterator<Item = &'a GrpValRow>,
) -> std::collections::BTreeMap<Option<i64>, (i128, i64)> {
    let mut m: std::collections::BTreeMap<Option<i64>, (i128, i64)> = std::collections::BTreeMap::new();
    for &(_pk, g, v, w) in rows {
        let e = m.entry(g).or_insert((0, 0));
        e.0 += v as i128 * w as i128;
        e.1 += w;
    }
    m.retain(|_, &mut (_, c)| c != 0);
    m
}

/// Drive `op_reduce` over `epochs` against a real ephemeral `trace_out` Table,
/// integrating each epoch's output delta back into the trace (flushed once
/// after epoch 0 when `flush_after_first`). Returns the final consolidated
/// trace_out batch and the maximum source count any epoch's probe cursor was
/// built from, so a caller can assert `SourceMode::Multi` (≥ 3 sources) was
/// exercised.
fn run_reduce_trace_epochs(
    dir: &std::path::Path,
    in_schema: &SchemaDescriptor,
    out_schema: &SchemaDescriptor,
    group_by: &[u32],
    aggs: &[AggDescriptor],
    epochs: &[Batch],
    flush_after_first: bool,
) -> (std::rc::Rc<Batch>, usize) {
    // A fresh tempdir per call isolates shard files, so a constant table_id is
    // collision-free.
    let mut trace = crate::storage::Table::new(
        dir.to_str().unwrap(),
        *out_schema,
        0,
        1 << 20,
        crate::storage::RecoverySource::Rederive,
    )
    .unwrap();
    let mut max_sources = 0usize;
    for (i, d) in epochs.iter().enumerate() {
        // Sources the cursor for THIS epoch's probe sees: memtable runs + folded
        // in-memory runs + shard files (each becomes one CursorSource).
        let sources = trace.snapshot_runs().len() + trace.in_memory_runs().count() + trace.all_shard_arcs().len();
        max_sources = max_sources.max(sources);
        let out = {
            let mut ch = trace.open_cursor();
            let (out, _) = op_reduce(
                d, None, &mut ch, in_schema, out_schema, group_by, aggs, None, None, None, false, false,
            );
            out
        };
        trace.ingest_owned_batch(out).unwrap();
        if flush_after_first && i == 0 {
            trace.flush().unwrap();
        }
    }
    (trace.full_scan(), max_sources)
}

/// Read a synthetic-U128-PK reduce output `[U128 pk, <grp>, I64 sum, I64 count]`
/// into a `grp → (sum, count)` map (a NULL group key reads as `None`), asserting
/// one net-weight-1 row per group.
fn readback_synthetic_i64(batch: &Batch) -> std::collections::BTreeMap<Option<i64>, (i128, i64)> {
    let mut m: std::collections::BTreeMap<Option<i64>, (i128, i64)> = std::collections::BTreeMap::new();
    for r in 0..batch.count {
        assert_eq!(batch.get_weight(r), 1, "each live group row nets weight 1");
        let grp = (batch.get_null_word(r) & 1 == 0).then(|| read_i64_le(batch.col_data(0), r * 8));
        let sum = read_i64_le(batch.col_data(1), r * 8) as i128;
        let count = read_i64_le(batch.col_data(2), r * 8);
        assert!(
            m.insert(grp, (sum, count)).is_none(),
            "one output row per group (grp={grp:?})"
        );
    }
    m
}

// Payload I64 group key, multi-epoch retraction across the sign flip. GROUP BY
// a non-nullable I64 payload column ⇒ canonical Payload-arm key + synthetic
// U128 output PK ⇒ the monotone `advance_to` probe. Epoch 1 inserts every
// group; epoch 2 updates and deletes rows in several groups (including the
// negative ones), so the retraction probe galloping over sign-flipped output
// PKs must land on the exact old aggregate row.
#[test]
fn reduce_monotone_probe_payload_i64_group() {
    let in_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0), // grp (non-nullable payload) — Payload arm
            SchemaColumn::new(type_code::I64, 0), // val
        ],
        &[0],
    );
    let out_schema = sum_count_out_synthetic(SchemaColumn::new(type_code::I64, 0));
    let aggs = sum_count_aggs(2, TypeCode::I64);

    let epoch_rows: [&[GrpValRow]; 2] = [
        &[
            (1, Some(-5), 10, 1),
            (2, Some(-5), 20, 1),
            (3, Some(-1), 5, 1),
            (8, Some(-1), -3, 1),
            (4, Some(0), 7, 1),
            (5, Some(0), 8, 1),
            (6, Some(3), 100, 1),
            (7, Some(3), 200, 1),
        ],
        &[
            (2, Some(-5), 20, -1), // delete a group-(-5) row
            (6, Some(3), 100, -1),
            (6, Some(3), 150, 1), // update a group-3 row 100 → 150
            (4, Some(0), 7, -1),  // delete a group-0 row
            (9, Some(-1), 50, 1), // insert a new group-(-1) row
        ],
    ];
    let batches: Vec<Batch> = epoch_rows.iter().map(|r| build_grp_val_delta(&in_schema, r)).collect();
    let reference = sum_count_reference(epoch_rows.iter().flat_map(|r| r.iter()));

    let tmp = tempfile::tempdir().unwrap();
    let (final_batch, _) =
        run_reduce_trace_epochs(tmp.path(), &in_schema, &out_schema, &[1u32], &aggs, &batches, false);

    assert_eq!(
        final_batch.count,
        reference.len(),
        "one live row per group, no un-cancelled ghosts"
    );
    assert_eq!(
        readback_synthetic_i64(&final_batch),
        reference,
        "incremental SUM/COUNT across sign-flip retraction must match the from-scratch reference",
    );
}

// Payload U64 group key — the unsigned arm. A non-nullable U64 payload column is
// natural-PK-eligible, so the output PK IS the group value (stride 8) and the
// key is Payload-arm canonical ⇒ monotone `advance_to`. Groups straddle the high
// byte (1 vs 256) so a byte-order slip in the gallop would misorder them.
#[test]
fn reduce_monotone_probe_payload_u64_group() {
    let in_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U64, 0), // grp (non-nullable payload) — natural PK
            SchemaColumn::new(type_code::I64, 0), // val
        ],
        &[0],
    );
    // Natural U64 output PK: [U64 pk(=grp), I64 sum(nullable), I64 count].
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 1),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    );
    let aggs = sum_count_aggs(2, TypeCode::I64);

    let epoch_rows: [&[GrpValRow]; 2] = [
        &[
            (1, Some(0), 5, 1),
            (2, Some(1), 10, 1),
            (3, Some(1), 20, 1),
            (4, Some(256), 7, 1),
            (5, Some(1000), 100, 1),
            (6, Some(1000), 200, 1),
        ],
        &[
            (3, Some(1), 20, -1), // delete a group-1 row
            (4, Some(256), 7, -1),
            (4, Some(256), 70, 1), // update group 256: 7 → 70
            (7, Some(0), 15, 1),   // insert into group 0
        ],
    ];
    let batches: Vec<Batch> = epoch_rows.iter().map(|r| build_grp_val_delta(&in_schema, r)).collect();
    let reference = sum_count_reference(epoch_rows.iter().flat_map(|r| r.iter()));

    let tmp = tempfile::tempdir().unwrap();
    let (final_batch, _) =
        run_reduce_trace_epochs(tmp.path(), &in_schema, &out_schema, &[1u32], &aggs, &batches, false);

    assert_eq!(final_batch.count, reference.len(), "one live row per group");
    // Natural-PK readback: group value is the (big-endian, unsigned) output PK;
    // payload is [sum, count].
    let mut got: std::collections::BTreeMap<Option<i64>, (i128, i64)> = std::collections::BTreeMap::new();
    for r in 0..final_batch.count {
        assert_eq!(final_batch.get_weight(r), 1);
        let grp = u64::from_be_bytes(final_batch.get_pk_bytes(r)[..8].try_into().unwrap()) as i64;
        let sum = read_i64_le(final_batch.col_data(0), r * 8) as i128;
        let count = read_i64_le(final_batch.col_data(1), r * 8);
        assert!(
            got.insert(Some(grp), (sum, count)).is_none(),
            "one output row per group"
        );
    }
    assert_eq!(
        got, reference,
        "unsigned-arm incremental SUM/COUNT must match the reference"
    );
}

// A nullable group column takes the hash arm, so the classifier returns None,
// the probe stays on the absolute `seek_bytes`, and the ascending tripwire
// never fires. Same retraction workload plus a NULL group (its own group);
// results must still match the from-scratch reference.
#[test]
fn reduce_nullable_group_stays_absolute_seek() {
    let in_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 1), // grp NULLABLE ⇒ hash arm
            SchemaColumn::new(type_code::I64, 0), // val
        ],
        &[0],
    );
    let out_schema = sum_count_out_synthetic(SchemaColumn::new(type_code::I64, 1)); // grp nullable
    let aggs = sum_count_aggs(2, TypeCode::I64);

    let epoch_rows: [&[GrpValRow]; 2] = [
        &[
            (1, None, 10, 1),
            (2, None, 20, 1), // NULL group
            (3, Some(-1), 5, 1),
            (4, Some(7), 100, 1),
            (5, Some(7), 1, 1),
        ],
        &[
            (2, None, 20, -1),     // retract a NULL-group row
            (4, Some(7), 100, -1), // delete a group-7 row
            (6, Some(-1), 50, 1),  // insert into group -1
        ],
    ];
    let batches: Vec<Batch> = epoch_rows.iter().map(|r| build_grp_val_delta(&in_schema, r)).collect();
    let reference = sum_count_reference(epoch_rows.iter().flat_map(|r| r.iter()));
    assert!(reference.contains_key(&None), "the NULL group must stay live");

    let tmp = tempfile::tempdir().unwrap();
    let (final_batch, _) =
        run_reduce_trace_epochs(tmp.path(), &in_schema, &out_schema, &[1u32], &aggs, &batches, false);

    assert_eq!(final_batch.count, reference.len(), "one live row per group");
    assert_eq!(
        readback_synthetic_i64(&final_batch),
        reference,
        "hash-arm incremental SUM/COUNT (incl. the NULL group) must match the reference",
    );
}

// Pin the shared classifier: the two canonical single-column arms (a PK
// column, a non-nullable routable-int payload), and the hash fold (None) for
// nullable / STRING / float / multi-column / empty group sets.
// `extract_group_key_row` dispatches its fast path on the arm and `op_reduce`
// keys its monotone probe on `is_some()`, so a drift here is a correctness
// bug, not a perf one.
#[test]
fn single_col_canonical_group_key_predicate() {
    use super::super::util::{single_col_canonical_group_key, CanonicalKeyArm};
    let u64c = SchemaColumn::new(type_code::U64, 0);
    let i64c = SchemaColumn::new(type_code::I64, 0);
    let i64_null = SchemaColumn::new(type_code::I64, 1);
    let strc = SchemaColumn::new(type_code::STRING, 0);
    let f64c = SchemaColumn::new(type_code::F64, 0);
    let u128c = SchemaColumn::new(type_code::U128, 0);

    let check = |cols: &[SchemaColumn], pk: &[u32], gb: &[u32], expected: Option<CanonicalKeyArm>, label: &str| {
        let schema = SchemaDescriptor::new(cols, pk);
        assert_eq!(single_col_canonical_group_key(&schema, gb), expected, "{label}");
    };
    let (pk_arm, pl_arm) = (Some(CanonicalKeyArm::Pk), Some(CanonicalKeyArm::Payload));
    check(&[u64c, i64c], &[0], &[1], pl_arm, "non-nullable I64 payload");
    check(&[u64c, u64c], &[0], &[1], pl_arm, "non-nullable U64 payload");
    check(&[u64c, u128c], &[0], &[1], pl_arm, "U128 payload (routable_int)");
    check(&[u64c, i64c], &[0], &[0], pk_arm, "single PK column");
    check(&[u64c, i64_null], &[0], &[1], None, "nullable → hash arm");
    check(&[u64c, strc], &[0], &[1], None, "STRING → hash arm");
    check(&[u64c, f64c], &[0], &[1], None, "float → hash arm");
    check(&[u64c, i64c, i64c], &[0], &[1, 2], None, "multi-column → hash arm");
    check(&[u64c, i64c], &[0], &[], None, "empty (global) group set");
}

// Many groups per epoch over ≥ 3 trace_out sources. 200 groups spanning the sign
// flip (-100..99), six epochs each touching every group (insert / update /
// delete). One flush after epoch 0 plus accumulating memtable runs pushes the
// trace_out cursor to `SourceMode::Multi` (≥ 3 sources) from epoch 3 on, so the
// multi-source gallop (`seek_forward_multi`) and the debug ascending tripwire
// both execute across hundreds of monotone probes. Construction makes every
// group's final aggregate identical (SUM=10, COUNT=3), so any mis-landed
// retraction shows up as a wrong sum or an un-cancelled duplicate.
#[test]
fn reduce_monotone_probe_many_groups_multi_source() {
    let in_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0), // grp — Payload arm
            SchemaColumn::new(type_code::I64, 0), // val
        ],
        &[0],
    );
    let out_schema = sum_count_out_synthetic(SchemaColumn::new(type_code::I64, 0));
    let aggs = sum_count_aggs(2, TypeCode::I64);

    let groups: Vec<i64> = (-100..100).collect();
    let mut next_pk = 0u64;
    let mut epochs_data: Vec<Vec<GrpValRow>> = Vec::new();
    for ep in 0..6 {
        let mut rows = Vec::new();
        for &g in &groups {
            let vws: Vec<(i64, i64)> = match ep {
                0 => vec![(g, 1), (g + 1000, 1)],
                1 => vec![(5, 1)],
                2 => vec![(g, -1)], // retract epoch-0 insert #1
                3 => vec![(7, 1)],
                4 => vec![(g + 1000, -1)], // retract epoch-0 insert #2
                5 => vec![(-2, 1)],
                _ => unreachable!(),
            };
            for (val, w) in vws {
                rows.push((next_pk, Some(g), val, w));
                next_pk += 1;
            }
        }
        epochs_data.push(rows);
    }
    let batches: Vec<Batch> = epochs_data.iter().map(|r| build_grp_val_delta(&in_schema, r)).collect();
    let reference = sum_count_reference(epochs_data.iter().flatten());
    assert_eq!(reference.len(), 200, "all 200 groups stay live");

    let tmp = tempfile::tempdir().unwrap();
    let (final_batch, max_sources) =
        run_reduce_trace_epochs(tmp.path(), &in_schema, &out_schema, &[1u32], &aggs, &batches, true);

    assert!(
        max_sources >= 3,
        "trace_out probe must reach SourceMode::Multi (≥ 3 sources); saw {max_sources}",
    );
    assert_eq!(
        final_batch.count,
        reference.len(),
        "one live row per group, no duplicates"
    );
    assert_eq!(
        readback_synthetic_i64(&final_batch),
        reference,
        "many-group multi-source incremental SUM/COUNT must match the from-scratch reference",
    );
}

// ===========================================================================
// MIN/MAX AVI probe-skip: an all-insert integer group folds `combine(old, pos)`
// from the reduce's own accumulator and the stored trace_out extreme instead of
// probing the value index; a group with any retraction (or a float source, or a
// group past the pre-step cap) still probes. These tests drive the *real* AVI
// path — the value index is populated per epoch by `op_integrate_with_indexes`
// (post-delta, as the compiler wires it: AVI Integrate precedes Reduce) and the
// reduce reads it — and check the skip path against both the unchanged trace-scan
// (`avi = None`) path and a from-scratch oracle.
// ===========================================================================

/// Content-keyed Z-set of a consolidated reduce-output batch:
/// `(pk_bytes, payload_bytes, null_word) → summed weight`, ghosts dropped. Two
/// batches are equal as Z-sets iff their maps are equal — order-independent, and
/// exact because the emit path is shared by both reduce paths, so a correct skip
/// produces byte-identical rows to a probe.
fn zset_canonical(
    b: &Batch,
    out_schema: &SchemaDescriptor,
) -> std::collections::BTreeMap<(Vec<u8>, Vec<u8>, u64), i64> {
    let mut m = std::collections::BTreeMap::new();
    for r in 0..b.count {
        let pk = b.get_pk_bytes(r).to_vec();
        let nw = b.get_null_word(r);
        let mut payload = Vec::new();
        for (pi, _ci, col) in out_schema.payload_columns() {
            let cs = col.size() as usize;
            payload.extend_from_slice(&b.col_data(pi)[r * cs..r * cs + cs]);
        }
        *m.entry((pk, payload, nw)).or_insert(0i64) += b.get_weight(r);
    }
    m.retain(|_, &mut w| w != 0);
    m
}

/// Drive a non-linear (MIN/MAX + COUNT companion) reduce over `epochs` against
/// live ephemeral tables, returning the consolidated `trace_out` after each
/// epoch.
///
/// `use_avi = true` runs the real combined-AVI path: each input delta is
/// integrated into a live AVI table *before* the reduce (the index must reflect
/// post-delta `I(input)`), and the reduce reads that index plus the pre-delta
/// `trace_out`. `use_avi = false` runs the unchanged trace-scan reference:
/// `avi = None` with a live `trace_in` integrated *after* the reduce (it must
/// reflect the pre-delta integral during the reduce). Both maintain `trace_out`
/// identically, so a correct skip makes the two output streams equal epoch by
/// epoch.
fn run_minmax_epochs(
    in_schema: &SchemaDescriptor,
    out_schema: &SchemaDescriptor,
    group_by: &[u32],
    aggs: &[AggDescriptor],
    epochs: &[Batch],
    use_avi: bool,
    global_ground: bool,
) -> Vec<std::rc::Rc<Batch>> {
    use crate::ops::index::{make_avi_schema, op_integrate_with_indexes, AviDesc};
    use crate::storage::{RecoverySource, Table};

    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().to_str().unwrap();

    let mut trace_out = Table::new(dir, *out_schema, 0, 1 << 20, RecoverySource::Rederive).unwrap();
    let mut trace_in = (!use_avi).then(|| Table::new(dir, *in_schema, 1, 1 << 20, RecoverySource::Rederive).unwrap());
    let avi_schema = make_avi_schema(in_schema, group_by);
    let mut avi_t = use_avi.then(|| Table::new(dir, avi_schema, 2, 1 << 20, RecoverySource::Rederive).unwrap());

    // Value-indexed (MIN/MAX) subset in descriptor order — exactly what the
    // compiler feeds the integrate instruction and the reduce read side.
    let vi_aggs: Vec<AggDescriptor> = aggs.iter().filter(|d| d.agg_op.uses_value_index()).copied().collect();

    let mut states = Vec::with_capacity(epochs.len());
    for d in epochs {
        // AVI Integrate precedes Reduce: post-delta `I(input)` before the read.
        if let Some(t) = avi_t.as_mut() {
            let avi = AviDesc {
                table: t as *mut Table,
                group_by_cols: group_by,
                aggs: &vi_aggs,
            };
            op_integrate_with_indexes(d, None, in_schema, Some(&avi)).unwrap();
        }
        let out = {
            let mut to_ch = trace_out.open_cursor();
            match (avi_t.as_ref(), trace_in.as_ref()) {
                (Some(at), None) => {
                    let mut avi_ch = at.open_cursor();
                    op_reduce(
                        d,
                        None,
                        &mut to_ch,
                        in_schema,
                        out_schema,
                        group_by,
                        aggs,
                        Some(&mut avi_ch),
                        None,
                        None,
                        global_ground,
                        global_ground,
                    )
                    .0
                }
                (None, Some(ti)) => {
                    let mut ti_ch = ti.open_cursor();
                    op_reduce(
                        d,
                        Some(&mut ti_ch),
                        &mut to_ch,
                        in_schema,
                        out_schema,
                        group_by,
                        aggs,
                        None,
                        None,
                        None,
                        global_ground,
                        global_ground,
                    )
                    .0
                }
                _ => unreachable!("exactly one of avi / trace_in is live"),
            }
        };
        // Reference path: integrate the input into `trace_in` AFTER the reduce so
        // the cursor read above saw the pre-delta integral.
        if let Some(ti) = trace_in.as_mut() {
            op_integrate_with_indexes(d, Some(ti), in_schema, None).unwrap();
        }
        trace_out.ingest_owned_batch(out).unwrap();
        states.push(trace_out.full_scan());
    }
    states
}

/// `[U64 pk, I64 grp, I64 val]` → group by the I64 payload `grp`.
fn mm_in_schema() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    )
}

/// `[U128 pk, I64 grp, I64 min(nullable), I64 max(nullable), I64 count]`.
fn mm_out_schema() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::I64, 1),
            SchemaColumn::new(type_code::I64, 1),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    )
}

/// `MIN(val), MAX(val), COUNT(pk)` — MIN ordinal 0, MAX ordinal 1 in the AVI.
fn mm_aggs() -> [AggDescriptor; 3] {
    [
        AggDescriptor {
            col_idx: 2,
            agg_op: AggOp::Min,
            col_type_code: TypeCode::I64,
            _pad: [0; 2],
        },
        AggDescriptor {
            col_idx: 2,
            agg_op: AggOp::Max,
            col_type_code: TypeCode::I64,
            _pad: [0; 2],
        },
        AggDescriptor {
            col_idx: 0,
            agg_op: AggOp::Count,
            col_type_code: TypeCode::I64,
            _pad: [0; 2],
        },
    ]
}

/// Raw (unconsolidated) input delta over `mm_in_schema()` from `(pk, grp, val, w)`
/// rows; `op_reduce` consolidates internally. `grp`/`val` are non-NULL.
fn build_mm_delta(rows: &[(u64, i64, i64, i64)]) -> Batch {
    let s = mm_in_schema();
    let g_pi = s.try_payload_idx(1).unwrap();
    let v_pi = s.try_payload_idx(2).unwrap();
    let mut b = Batch::with_schema(s, rows.len().max(1));
    for &(pk, grp, val, w) in rows {
        b.extend_pk(pk as u128);
        b.extend_weight(&w.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(g_pi, &grp.to_le_bytes());
        b.extend_col(v_pi, &val.to_le_bytes());
        b.count += 1;
    }
    b
}

/// `grp → (min, max, count)` for a MIN/MAX/COUNT reduce, NULL extremes as `None`.
type MmState = std::collections::BTreeMap<i64, (Option<i64>, Option<i64>, i64)>;

/// Read a consolidated `mm_out_schema()` batch into `grp → (min, max, count)`,
/// a NULL min/max reading as `None`. Asserts one net-weight-1 row per group.
fn readback_mm(b: &Batch) -> MmState {
    let mut m = std::collections::BTreeMap::new();
    for r in 0..b.count {
        assert_eq!(b.get_weight(r), 1, "each live group is one net-weight-1 row");
        let nw = b.get_null_word(r);
        let grp = read_i64_le(b.col_data(0), r * 8);
        let min = ((nw >> 1) & 1 == 0).then(|| read_i64_le(b.col_data(1), r * 8));
        let max = ((nw >> 2) & 1 == 0).then(|| read_i64_le(b.col_data(2), r * 8));
        let count = read_i64_le(b.col_data(3), r * 8);
        assert!(m.insert(grp, (min, max, count)).is_none(), "one output row per group");
    }
    m
}

// Primary oracle: a multi-epoch stream of inserts, updates (retract+insert), and
// deletes over many groups. The real AVI (probe-skip) path must equal both the
// unchanged trace-scan reference AND a from-scratch group-by MIN/MAX/COUNT oracle
// after every epoch — weight-exact, not just row presence. An all-insert epoch
// into an existing group takes the skip path; a new group or any
// retraction/update forces a probe.
#[test]
fn avi_skip_randomized_equivalence_mixed_churn() {
    use crate::test_rng::Rng;
    use std::collections::BTreeMap;

    let in_schema = mm_in_schema();
    let out_schema = mm_out_schema();
    let aggs = mm_aggs();

    let mut rng = Rng::new(0x00C0_FFEE_1234_5678);
    let mut live: BTreeMap<u64, (i64, i64)> = BTreeMap::new(); // pk -> (grp, val)
    let mut next_pk = 1u64;
    const N_GROUPS: u64 = 10;

    let mut epochs: Vec<Batch> = Vec::new();
    let mut oracles: Vec<MmState> = Vec::new();

    for _ep in 0..30 {
        let mut ops: Vec<(u64, i64, i64, i64)> = Vec::new();
        let n_ops = 1 + rng.gen_range(10) as usize;
        for _ in 0..n_ops {
            let choice = if live.is_empty() { 0 } else { rng.gen_range(3) };
            match choice {
                // insert a fresh pk
                0 => {
                    let grp = rng.gen_range(N_GROUPS) as i64 - 3; // spans negative groups
                    let val = rng.gen_range(120) as i64 - 60;
                    let pk = next_pk;
                    next_pk += 1;
                    live.insert(pk, (grp, val));
                    ops.push((pk, grp, val, 1));
                }
                // update an existing pk's value (retract + insert)
                1 => {
                    let keys: Vec<u64> = live.keys().copied().collect();
                    let pk = keys[rng.gen_range(keys.len() as u64) as usize];
                    let (grp, old_val) = live[&pk];
                    let new_val = rng.gen_range(120) as i64 - 60;
                    ops.push((pk, grp, old_val, -1));
                    ops.push((pk, grp, new_val, 1));
                    live.insert(pk, (grp, new_val));
                }
                // delete an existing pk
                _ => {
                    let keys: Vec<u64> = live.keys().copied().collect();
                    let pk = keys[rng.gen_range(keys.len() as u64) as usize];
                    let (grp, val) = live[&pk];
                    ops.push((pk, grp, val, -1));
                    live.remove(&pk);
                }
            }
        }
        // Consolidate to a net Z-set delta: within-epoch retract+insert of the same
        // (pk, grp, val) cancels (so it never falsely suppresses `saw_negative`).
        let mut netted: BTreeMap<(u64, i64, i64), i64> = BTreeMap::new();
        for &(pk, grp, val, w) in &ops {
            *netted.entry((pk, grp, val)).or_insert(0) += w;
        }
        let rows: Vec<(u64, i64, i64, i64)> = netted
            .into_iter()
            .filter(|&(_, w)| w != 0)
            .map(|((pk, grp, val), w)| (pk, grp, val, w))
            .collect();
        epochs.push(build_mm_delta(&rows));

        // From-scratch oracle from the live base-table state.
        let mut oracle: MmState = BTreeMap::new();
        for &(grp, val) in live.values() {
            let e = oracle.entry(grp).or_insert((None, None, 0));
            e.0 = Some(e.0.map_or(val, |m: i64| m.min(val)));
            e.1 = Some(e.1.map_or(val, |m: i64| m.max(val)));
            e.2 += 1;
        }
        oracles.push(oracle);
    }

    let avi_states = run_minmax_epochs(&in_schema, &out_schema, &[1u32], &aggs, &epochs, true, false);
    let ref_states = run_minmax_epochs(&in_schema, &out_schema, &[1u32], &aggs, &epochs, false, false);

    for ep in 0..epochs.len() {
        assert_eq!(
            zset_canonical(&avi_states[ep], &out_schema),
            zset_canonical(&ref_states[ep], &out_schema),
            "epoch {ep}: AVI probe-skip path must equal the trace-scan reference",
        );
        assert_eq!(
            readback_mm(&avi_states[ep]),
            oracles[ep],
            "epoch {ep}: AVI probe-skip path must equal the from-scratch MIN/MAX oracle",
        );
    }
}

// Explicit skip/probe cases over a single group, one assertion per epoch.
#[test]
fn avi_skip_probe_unit_cases() {
    let in_schema = mm_in_schema();
    let out_schema = mm_out_schema();
    let aggs = mm_aggs();
    let run = |epochs: &[Batch]| run_minmax_epochs(&in_schema, &out_schema, &[1u32], &aggs, epochs, true, false);

    // (a)/(b) insert-only: MAX rises above old, MIN unchanged; a later insert
    // below old leaves both extremes folded from `combine(old, pos)`.
    {
        let epochs = [
            build_mm_delta(&[(1, 0, 5, 1)]), // min=max=5
            build_mm_delta(&[(2, 0, 8, 1)]), // all-insert: max 5→8 (skip), min stays 5
            build_mm_delta(&[(3, 0, 2, 1)]), // all-insert: min 5→2 (skip), max stays 8
        ];
        let s = run(&epochs);
        assert_eq!(readback_mm(&s[0])[&0], (Some(5), Some(5), 1));
        assert_eq!(
            readback_mm(&s[1])[&0],
            (Some(5), Some(8), 2),
            "insert above raises MAX via skip"
        );
        assert_eq!(
            readback_mm(&s[2])[&0],
            (Some(2), Some(8), 3),
            "insert below lowers MIN via skip"
        );
    }

    // (c) retraction strictly inside the range: probe, extremes unchanged.
    {
        let epochs = [
            build_mm_delta(&[(1, 0, 1, 1), (2, 0, 5, 1), (3, 0, 10, 1)]), // min=1, max=10
            build_mm_delta(&[(2, 0, 5, -1)]),                             // retract the interior 5
        ];
        let s = run(&epochs);
        assert_eq!(
            readback_mm(&s[1])[&0],
            (Some(1), Some(10), 2),
            "interior retraction probes; extremes hold"
        );
    }

    // (d) retraction of an extreme with multiplicity 2 at that value: probe, the
    // second copy keeps MAX at 10.
    {
        let epochs = [
            build_mm_delta(&[(1, 0, 10, 1), (2, 0, 10, 1), (3, 0, 1, 1)]), // two rows at 10
            build_mm_delta(&[(1, 0, 10, -1)]),                             // retract one 10
        ];
        let s = run(&epochs);
        assert_eq!(
            readback_mm(&s[1])[&0],
            (Some(1), Some(10), 2),
            "duplicated extreme survives one retraction"
        );
    }

    // (e) retraction of the unique extreme: probe, MAX recedes to the next value.
    {
        let epochs = [
            build_mm_delta(&[(1, 0, 10, 1), (2, 0, 5, 1), (3, 0, 1, 1)]),
            build_mm_delta(&[(1, 0, 10, -1)]), // retract the unique max
        ];
        let s = run(&epochs);
        assert_eq!(
            readback_mm(&s[1])[&0],
            (Some(1), Some(5), 2),
            "unique extreme recedes to next via probe"
        );
    }

    // (g) group emptied to zero cardinality: the cardinality gate sheds it.
    {
        let epochs = [
            build_mm_delta(&[(1, 0, 5, 1)]),
            build_mm_delta(&[(1, 0, 5, -1)]), // retract the sole row
        ];
        let s = run(&epochs);
        assert!(
            !readback_mm(&s[1]).contains_key(&0),
            "emptied group is shed, not a zombie"
        );
    }
}

// (f) A new all-insert group whose aggregate values are all NULL: the skip path
// leaves the MIN/MAX accumulators untouched and renders NULL, while COUNT counts
// the rows.
#[test]
fn avi_skip_new_all_null_group() {
    let in_schema = mm_in_schema();
    let out_schema = mm_out_schema();
    let aggs = mm_aggs();

    // Nullable-value delta: both rows have a NULL `val`.
    let delta = {
        let s = mm_in_schema();
        let g_pi = s.try_payload_idx(1).unwrap();
        let v_pi = s.try_payload_idx(2).unwrap();
        let mut b = Batch::with_schema(s, 2);
        for pk in [1u64, 2] {
            b.extend_pk(pk as u128);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&(1u64 << v_pi).to_le_bytes()); // val NULL
            b.extend_col(g_pi, &0i64.to_le_bytes());
            b.extend_col(v_pi, &0i64.to_le_bytes());
            b.count += 1;
        }
        b
    };
    let s = run_minmax_epochs(&in_schema, &out_schema, &[1u32], &aggs, &[delta], true, false);
    assert_eq!(
        readback_mm(&s[0])[&0],
        (None, None, 2),
        "all-NULL insert-only group: MIN/MAX render NULL, COUNT counts rows",
    );
}

// Cap: an *existing* group receives more than `SKIP_TRACK_CAP` all-insert rows in
// one epoch, tripping the pre-step cap so it force-probes (its partial pre-stepped
// accumulator is discarded by the probe) rather than folding. The cap trigger —
// not `!has_old` — is what's exercised; the result must match the from-scratch
// extreme, confirming the cap is correctness-neutral.
#[test]
fn avi_skip_cap_force_probes() {
    let in_schema = mm_in_schema();
    let out_schema = mm_out_schema();
    let aggs = mm_aggs();

    // Epoch 0 creates the group (extreme 500); epoch 1 inserts 200 > 128 rows into
    // the now-existing group — an all-insert, has_old group past the cap.
    let seed = build_mm_delta(&[(1, 0, 500, 1)]);
    let bulk: Vec<(u64, i64, i64, i64)> = (0..200).map(|i| (i as u64 + 10, 0, i as i64 - 50, 1)).collect();
    let epochs = [seed, build_mm_delta(&bulk)];
    let s = run_minmax_epochs(&in_schema, &out_schema, &[1u32], &aggs, &epochs, true, false);
    assert_eq!(readback_mm(&s[0])[&0], (Some(500), Some(500), 1));
    assert_eq!(
        readback_mm(&s[1])[&0],
        (Some(-50), Some(500), 201),
        "a capped all-insert existing group force-probes and still matches the from-scratch extreme",
    );
}

// (h) An integer MIN and a float MAX on distinct columns of one group in one
// insert-only epoch: the integer MIN skips (fold `combine(old, pos)`), the float
// MAX probes (floats always probe) — both correct in the same pass.
#[test]
fn avi_skip_mixed_int_min_float_max() {
    let in_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0), // grp
            SchemaColumn::new(type_code::I64, 0), // ival (integer MIN, skips)
            SchemaColumn::new(type_code::F64, 0), // fval (float MAX, probes)
        ],
        &[0],
    );
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::I64, 1), // min(ival)
            SchemaColumn::new(type_code::F64, 1), // max(fval) widens to F64
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    );
    let aggs = [
        AggDescriptor {
            col_idx: 2,
            agg_op: AggOp::Min,
            col_type_code: TypeCode::I64,
            _pad: [0; 2],
        },
        AggDescriptor {
            col_idx: 3,
            agg_op: AggOp::Max,
            col_type_code: TypeCode::F64,
            _pad: [0; 2],
        },
        AggDescriptor {
            col_idx: 0,
            agg_op: AggOp::Count,
            col_type_code: TypeCode::I64,
            _pad: [0; 2],
        },
    ];
    let build = |rows: &[(u64, i64, i64, f64, i64)]| -> Batch {
        let g_pi = in_schema.try_payload_idx(1).unwrap();
        let i_pi = in_schema.try_payload_idx(2).unwrap();
        let f_pi = in_schema.try_payload_idx(3).unwrap();
        let mut b = Batch::with_schema(in_schema, rows.len().max(1));
        for &(pk, grp, ival, fval, w) in rows {
            b.extend_pk(pk as u128);
            b.extend_weight(&w.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(g_pi, &grp.to_le_bytes());
            b.extend_col(i_pi, &ival.to_le_bytes());
            b.extend_col(f_pi, &fval.to_bits().to_le_bytes());
            b.count += 1;
        }
        b
    };
    let epochs = [
        build(&[(1, 0, 5, 1.0, 1)]),
        build(&[(2, 0, 3, 2.0, 1)]), // all-insert: int MIN 5→3 (skip), float MAX 1.0→2.0 (probe)
    ];
    let s = run_minmax_epochs(&in_schema, &out_schema, &[1u32], &aggs, &epochs, true, false);
    let read = |b: &Batch| -> (Option<i64>, Option<f64>, i64) {
        assert_eq!(b.count, 1);
        let nw = b.get_null_word(0);
        let min = ((nw >> 1) & 1 == 0).then(|| read_i64_le(b.col_data(1), 0));
        let max = ((nw >> 2) & 1 == 0).then(|| f64::from_bits(read_u64_le(b.col_data(2), 0)));
        let count = read_i64_le(b.col_data(3), 0);
        (min, max, count)
    };
    assert_eq!(
        read(&s[1]),
        (Some(3), Some(2.0), 2),
        "integer MIN skips and float MAX probes in one insert-only pass",
    );
}

/// Drive a float MIN/MAX reduce through both paths and assert equality every
/// epoch. Floats always probe, so this pins that the today-behavior float path is
/// preserved — including ±0.0 and NaN under the total order.
fn check_float_minmax_matches_reference(val_tc: TypeCode, epochs_rows: &[Vec<(u64, i64, f64, i64)>]) {
    let in_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(val_tc as u8, 0),
        ],
        &[0],
    );
    // Float MIN/MAX widen to F64 in the output, regardless of an F32 source.
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::F64, 1),
            SchemaColumn::new(type_code::F64, 1),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    );
    let aggs = [
        AggDescriptor {
            col_idx: 2,
            agg_op: AggOp::Min,
            col_type_code: val_tc,
            _pad: [0; 2],
        },
        AggDescriptor {
            col_idx: 2,
            agg_op: AggOp::Max,
            col_type_code: val_tc,
            _pad: [0; 2],
        },
        AggDescriptor {
            col_idx: 0,
            agg_op: AggOp::Count,
            col_type_code: TypeCode::I64,
            _pad: [0; 2],
        },
    ];
    let build = |rows: &[(u64, i64, f64, i64)]| -> Batch {
        let g_pi = in_schema.try_payload_idx(1).unwrap();
        let v_pi = in_schema.try_payload_idx(2).unwrap();
        let mut b = Batch::with_schema(in_schema, rows.len().max(1));
        for &(pk, grp, val, w) in rows {
            b.extend_pk(pk as u128);
            b.extend_weight(&w.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(g_pi, &grp.to_le_bytes());
            if val_tc == TypeCode::F32 {
                b.extend_col(v_pi, &(val as f32).to_bits().to_le_bytes());
            } else {
                b.extend_col(v_pi, &val.to_bits().to_le_bytes());
            }
            b.count += 1;
        }
        b
    };
    let epochs: Vec<Batch> = epochs_rows.iter().map(|r| build(r)).collect();
    let avi = run_minmax_epochs(&in_schema, &out_schema, &[1u32], &aggs, &epochs, true, false);
    let refr = run_minmax_epochs(&in_schema, &out_schema, &[1u32], &aggs, &epochs, false, false);
    for ep in 0..epochs.len() {
        assert_eq!(
            zset_canonical(&avi[ep], &out_schema),
            zset_canonical(&refr[ep], &out_schema),
            "float MIN/MAX epoch {ep}: AVI path must equal the trace-scan reference",
        );
    }
}

#[test]
fn avi_float_f64_minmax_matches_reference() {
    check_float_minmax_matches_reference(
        TypeCode::F64,
        &[
            vec![(1, 0, -0.0, 1), (2, 0, 0.0, 1), (3, 0, 5.0, 1), (4, 0, -5.0, 1)], // ±0.0 both present
            vec![(5, 0, f64::NAN, 1)],                                              // NaN is the max under total_cmp
            vec![(3, 0, 5.0, -1)],                                                  // retract a finite (always probes)
            vec![(5, 0, f64::NAN, -1)],                                             // retract the NaN
        ],
    );
}

#[test]
fn avi_float_f32_minmax_matches_reference() {
    check_float_minmax_matches_reference(
        TypeCode::F32,
        &[
            vec![(1, 0, -0.0, 1), (2, 0, 0.0, 1), (3, 0, 2.5, 1), (4, 0, -2.5, 1)],
            vec![(5, 0, f64::NAN, 1)],
            vec![(3, 0, 2.5, -1)],
        ],
    );
}

/// PK-source MAX: the aggregated column is a PK column, read through
/// `native_le_bytes` both when pre-stepping the accumulator (skip path) and when
/// the AVI is populated. `b_signed` picks a signed (sign-flipped OPK) or unsigned
/// second PK column.
fn check_pk_source_max(b_signed: bool) {
    use std::collections::BTreeMap;
    let b_tc = if b_signed { type_code::I64 } else { type_code::U64 };
    let in_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0), // a (pk, group)
            SchemaColumn::new(b_tc, 0),           // b (pk, aggregated by MAX)
            SchemaColumn::new(type_code::I64, 0), // pad (payload)
        ],
        &[0, 1],
    );
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0), // a (natural PK)
            SchemaColumn::new(b_tc, 1),           // max(b) (nullable)
            SchemaColumn::new(type_code::I64, 0), // count
        ],
        &[0],
    );
    let aggs = [
        AggDescriptor {
            col_idx: 1,
            agg_op: AggOp::Max,
            col_type_code: TypeCode::from_validated_u8(b_tc),
            _pad: [0; 2],
        },
        AggDescriptor {
            col_idx: 0,
            agg_op: AggOp::Count,
            col_type_code: TypeCode::U64,
            _pad: [0; 2],
        },
    ];
    let build = |rows: &[(u64, i64, i64)]| -> Batch {
        let pad_pi = in_schema.try_payload_idx(2).unwrap();
        let mut bt = Batch::with_schema(in_schema, rows.len().max(1));
        for &(a, b, w) in rows {
            bt.extend_pk_opk(&in_schema, &[a as u128, b as u128]);
            bt.extend_weight(&w.to_le_bytes());
            bt.extend_null_bmp(&0u64.to_le_bytes());
            bt.extend_col(pad_pi, &0i64.to_le_bytes());
            bt.count += 1;
        }
        bt
    };
    let low = if b_signed { -5 } else { 5 };
    let epochs = [
        build(&[(1, 10, 1), (1, low, 1), (2, 3, 1)]), // a=1: max=10 (count 2); a=2: max=3
        build(&[(1, 20, 1)]),                         // all-insert: a=1 max 10→20 (skip)
        build(&[(1, 20, -1)]),                        // retract extreme: a=1 max 20→10 (probe)
    ];
    let s = run_minmax_epochs(&in_schema, &out_schema, &[0u32], &aggs, &epochs, true, false);
    let read = |b: &Batch| -> BTreeMap<u64, (i64, i64)> {
        let mut m = BTreeMap::new();
        for r in 0..b.count {
            let a = u64::from_be_bytes(b.get_pk_bytes(r)[..8].try_into().unwrap());
            let max_b = read_i64_le(b.col_data(0), r * 8);
            let count = read_i64_le(b.col_data(1), r * 8);
            m.insert(a, (max_b, count));
        }
        m
    };
    assert_eq!(read(&s[0]), BTreeMap::from([(1, (10, 2)), (2, (3, 1))]));
    assert_eq!(
        read(&s[1])[&1],
        (20, 3),
        "insert-only PK-source MAX rises via the skip path"
    );
    assert_eq!(
        read(&s[2])[&1],
        (10, 2),
        "retract-at-extreme PK-source MAX recedes via the probe"
    );
}

#[test]
fn avi_skip_pk_source_max_signed() {
    check_pk_source_max(true);
}

#[test]
fn avi_skip_pk_source_max_unsigned() {
    check_pk_source_max(false);
}

// Global (ungrouped) MIN/MAX over the V₀ group (empty prefix): once the V₀ group
// exists, an insert-only epoch skips; a retract-at-extreme epoch probes and
// recedes.
#[test]
fn avi_skip_global_aggregate() {
    let in_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    );
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::I64, 1),
            SchemaColumn::new(type_code::I64, 1),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    );
    let aggs = [
        AggDescriptor {
            col_idx: 1,
            agg_op: AggOp::Min,
            col_type_code: TypeCode::I64,
            _pad: [0; 2],
        },
        AggDescriptor {
            col_idx: 1,
            agg_op: AggOp::Max,
            col_type_code: TypeCode::I64,
            _pad: [0; 2],
        },
        AggDescriptor {
            col_idx: 0,
            agg_op: AggOp::Count,
            col_type_code: TypeCode::I64,
            _pad: [0; 2],
        },
    ];
    let build = |rows: &[(u64, i64, i64)]| -> Batch {
        let v_pi = in_schema.try_payload_idx(1).unwrap();
        let mut b = Batch::with_schema(in_schema, rows.len().max(1));
        for &(pk, val, w) in rows {
            b.extend_pk(pk as u128);
            b.extend_weight(&w.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(v_pi, &val.to_le_bytes());
            b.count += 1;
        }
        b
    };
    let epochs = [
        build(&[(1, 5, 1)]),            // seed the V₀ group (new → probe): min=max=5
        build(&[(2, 8, 1), (3, 1, 1)]), // insert-only into existing V₀ (skip): min=1, max=8, count=3
        build(&[(2, 8, -1)]),           // retract the max (probe): max 8→5
    ];
    let s = run_minmax_epochs(&in_schema, &out_schema, &[], &aggs, &epochs, true, true);
    let read = |b: &Batch| -> (Option<i64>, Option<i64>, i64) {
        assert_eq!(b.count, 1, "global aggregate is exactly one row");
        let nw = b.get_null_word(0);
        let min = (nw & 1 == 0).then(|| read_i64_le(b.col_data(0), 0));
        let max = ((nw >> 1) & 1 == 0).then(|| read_i64_le(b.col_data(1), 0));
        let count = read_i64_le(b.col_data(2), 0);
        (min, max, count)
    };
    assert_eq!(read(&s[0]), (Some(5), Some(5), 1), "seed global MIN/MAX");
    assert_eq!(
        read(&s[1]),
        (Some(1), Some(8), 3),
        "insert-only global MIN/MAX into existing V₀ via skip"
    );
    assert_eq!(
        read(&s[2]),
        (Some(1), Some(5), 2),
        "retract-the-max global recedes via probe"
    );
}
