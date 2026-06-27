//! Reduce operator tests. Imports submodule items by name; helpers live in this
//! file rather than a shared `common.rs` so the tests file stays self-contained.

use crate::foundation::codec::{read_i64_le, read_u64_le};
use crate::schema::{
    encode_german_string, type_code, SchemaColumn, SchemaDescriptor, TypeCode, PAYLOAD_MAPPING_PK_SENTINEL,
};
use crate::storage::{Batch, ConsolidatedBatch};
use crate::test_support::make_schema_i64pk_i64;

use super::super::util::{extract_group_key, ieee_order_bits_f32, ieee_order_bits_f32_reverse};
use super::agg::{apply_agg_from_value_index, Accumulator, AggDescriptor, AggOp};
use super::emit::{emit_finalized_row, emit_reduce_row};
use super::op_gather::op_gather_reduce;
use super::op_reduce::{cursor_matches_group, op_reduce};
use super::sort::{argsort_delta, build_sort_descs, compare_by_group_cols, sort_owned};

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

fn make_batch(schema: &SchemaDescriptor, rows: &[(u64, i64, i64)]) -> ConsolidatedBatch {
    let n = rows.len();
    let mut b = Batch::with_schema(*schema, n.max(1));

    for &(pk, w, val) in rows {
        b.extend_pk(pk as u128);
        b.extend_weight(&w.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &val.to_le_bytes());
        b.count += 1;
    }
    b.sorted = true;
    b.consolidated = true;
    ConsolidatedBatch::new_unchecked(b)
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

fn make_batch_f32(schema: &SchemaDescriptor, rows: &[(u64, i64, f32)]) -> ConsolidatedBatch {
    let n = rows.len();
    let mut b = Batch::with_schema(*schema, n.max(1));

    for &(pk, w, val) in rows {
        b.extend_pk(pk as u128);
        b.extend_weight(&w.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &val.to_bits().to_le_bytes());
        b.count += 1;
    }
    b.sorted = true;
    b.consolidated = true;
    ConsolidatedBatch::new_unchecked(b)
}

/// 3-column source schema: U64 pk (pk_index=0), I64 grp, STRING val (nullable).
fn make_schema_3col_grp_str() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::STRING, 1),
        ],
        &[0],
    )
}

/// 3-column reduce output schema: U128 pk (pk_index=0), I64 grp, I64 agg (nullable).
fn make_reduce_str_out_schema() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::I64, 1),
        ],
        &[0],
    )
}

/// Build a 3-column Batch (U64 pk, I64 grp, STRING val) from tuples.
fn make_batch_3col_grp_str(schema: &SchemaDescriptor, rows: &[(u64, i64, i64, &str)]) -> ConsolidatedBatch {
    let n = rows.len();
    let mut b = Batch::with_schema(*schema, n.max(1));

    for &(pk, w, grp, val) in rows {
        b.extend_pk(pk as u128);
        b.extend_weight(&w.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &grp.to_le_bytes());
        let gs = encode_german_string(val.as_bytes(), &mut b.blob);
        b.extend_col(1, &gs);
        b.count += 1;
    }
    b.sorted = true;
    b.consolidated = true;
    ConsolidatedBatch::new_unchecked(b)
}

/// Build a byte-form GI Batch for a single-U64-PK source: key = gc(8) ++
/// source_pk(8). Each row is `(source_pk_u64, gc_u64)`.
fn make_gi_batch(src: &SchemaDescriptor, rows: &[(u64, u64)]) -> ConsolidatedBatch {
    let gi_schema = crate::ops::index::make_gi_schema(src);
    let n = rows.len();
    let mut b = Batch::with_schema(gi_schema, n.max(1));

    let mut key = [0u8; 16];
    for &(source_pk, gc_u64) in rows {
        // GI key = gc(LE, matching index.rs population) ++ source_pk OPK bytes.
        // The source_pk half is OPK at rest so the trace re-seek (which keys on
        // the OPK trace PK region) matches.
        key[..8].copy_from_slice(&gc_u64.to_le_bytes());
        key[8..].copy_from_slice(&source_pk.to_be_bytes());
        b.extend_pk_bytes(&key);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.count += 1;
    }
    b.sorted = true;
    b.consolidated = true;
    ConsolidatedBatch::new_unchecked(b)
}

#[test]
fn test_reduce_sum_retraction() {
    use crate::schema::{type_code, SchemaColumn};
    use crate::storage::CursorHandle;
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
    let empty_out = Rc::new(Batch::empty(3, 16));
    let mut to_ch = CursorHandle::from_owned(&[empty_out], out_schema);

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
        b.sorted = true;
        b.consolidated = true;
        ConsolidatedBatch::new_unchecked(b)
    };

    let aggs = sum_count_aggs(2, TypeCode::I64);

    let (out1, _) = op_reduce(
        &delta1,
        None,
        to_ch.cursor_mut(),
        &in_schema,
        &out_schema,
        &[1u32],
        &aggs,
        None,
        false,
        TypeCode::U64,
        None,
        0,
        None,
        None,
    );
    // SUM of (100+200+300) = 600
    assert_eq!(out1.count, 1);
    let sum1 = read_i64_le(out1.col_data(1), 0);
    assert_eq!(sum1, 600);

    // Tick 2: retract pk=2 (val=200) → SUM should go from 600 to 400
    // Need trace_out with previous aggregate
    let prev_out = Rc::new(out1);
    let mut to_ch2 = CursorHandle::from_owned(&[prev_out], out_schema);

    let delta2 = {
        let mut b = Batch::with_schema(in_schema, 1);
        b.extend_pk(2u128);
        b.extend_weight(&(-1i64).to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &10i64.to_le_bytes());
        b.extend_col(1, &200i64.to_le_bytes());
        b.count += 1;
        b.sorted = true;
        b.consolidated = true;
        ConsolidatedBatch::new_unchecked(b)
    };

    let (out2, _) = op_reduce(
        &delta2,
        None,
        to_ch2.cursor_mut(),
        &in_schema,
        &out_schema,
        &[1u32],
        &aggs,
        None,
        false,
        TypeCode::U64,
        None,
        0,
        None,
        None,
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
    use crate::storage::CursorHandle;
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

    let reduce = |delta: &ConsolidatedBatch, to: &mut crate::storage::ReadCursor| {
        op_reduce(
            delta,
            None,
            to,
            &in_schema,
            &out_schema,
            &[1u32],
            &aggs,
            None,
            false,
            TypeCode::U64,
            None,
            0,
            None,
            None,
        )
        .0
    };

    // Tick 1: insert (pk1, grp=10, val=5) → group exists (sum=5, count=1).
    let empty_out = Rc::new(Batch::empty_with_schema(&out_schema));
    let mut to_ch = CursorHandle::from_owned(&[empty_out], out_schema);
    let row = |pk: u128, w: i64| {
        let mut b = Batch::with_schema(in_schema, 1);
        b.extend_pk(pk);
        b.extend_weight(&w.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &10i64.to_le_bytes()); // grp=10
        b.extend_col(1, &5i64.to_le_bytes()); // val=5
        b.count += 1;
        b.sorted = true;
        b.consolidated = true;
        ConsolidatedBatch::new_unchecked(b)
    };
    let out1 = reduce(&row(1, 1), to_ch.cursor_mut());
    assert_eq!(out1.count, 1, "group present after first insert");

    // Tick 2: retract the only row → cardinality 1 → 0. The gate suppresses the
    // +1, leaving just the −1 retraction, so the group disappears (no zombie).
    let prev = Rc::new(out1);
    let mut to_ch2 = CursorHandle::from_owned(&[prev], out_schema);
    let out2 = reduce(&row(1, -1), to_ch2.cursor_mut());
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
    use crate::storage::CursorHandle;
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
    let fin_prog = LogicalProgram::new(instrs, 5, 0, vec![]).resolve(&out_schema, false);

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
    let mut to_ch = CursorHandle::from_owned(&[empty_out], out_schema);
    let delta = {
        let mut b = Batch::with_schema(in_schema, 1);
        b.extend_pk(1u128);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&(1u64 << 1).to_le_bytes()); // val (payload idx 1) NULL
        b.extend_col(0, &7i64.to_le_bytes()); // grp=7
        b.extend_col(1, &0i64.to_le_bytes()); // val NULL (placeholder)
        b.count += 1;
        b.sorted = true;
        b.consolidated = true;
        ConsolidatedBatch::new_unchecked(b)
    };
    let (raw, fin) = op_reduce(
        &delta,
        None,
        to_ch.cursor_mut(),
        &in_schema,
        &out_schema,
        &[1u32],
        &aggs,
        None,
        false,
        TypeCode::U64,
        None,
        0,
        Some(&fin_prog),
        Some(&fin_schema),
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
    use crate::storage::CursorHandle;
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

    let reduce = |delta: &ConsolidatedBatch, to: &mut crate::storage::ReadCursor| {
        op_reduce(
            delta,
            None,
            to,
            &in_schema,
            &out_schema,
            &[1u32],
            &aggs,
            None,
            false,
            TypeCode::U64,
            None,
            0,
            None,
            None,
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
        b.sorted = true;
        b.consolidated = true;
        ConsolidatedBatch::new_unchecked(b)
    };

    // Tick 1: insert one row → count=1.
    let empty_out = Rc::new(Batch::empty_with_schema(&out_schema));
    let mut to_ch = CursorHandle::from_owned(&[empty_out], out_schema);
    let out1 = reduce(&row(1, 1), to_ch.cursor_mut());
    assert_eq!(out1.count, 1);
    assert_eq!(read_i64_le(out1.col_data(1), 0), 1, "count=1");

    // Tick 2: retract it → count 1 → 0 → group eliminated (only the −1 retraction).
    let prev = Rc::new(out1);
    let mut to_ch2 = CursorHandle::from_owned(&[prev], out_schema);
    let out2 = reduce(&row(1, -1), to_ch2.cursor_mut());
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
    use crate::storage::CursorHandle;
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
    let fin_prog = LogicalProgram::new(instrs, 5, 0, vec![]).resolve(&out_schema, false);

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
    let empty_out = Rc::new(Batch::empty(out_schema.num_payload_cols(), 16));
    let mut to_ch = CursorHandle::from_owned(&[empty_out], out_schema);
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
        b.sorted = true;
        b.consolidated = true;
        ConsolidatedBatch::new_unchecked(b)
    };

    let (raw1, fin1) = op_reduce(
        &delta1,
        None,
        to_ch.cursor_mut(),
        &in_schema,
        &out_schema,
        &[1u32],
        &aggs,
        None,
        false,
        TypeCode::U64,
        None,
        0,
        Some(&fin_prog),
        Some(&fin_schema),
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
    let mut to_ch2 = CursorHandle::from_owned(&[prev_out], out_schema);
    let delta2 = {
        let mut b = Batch::with_schema(in_schema, 1);
        b.extend_pk(1u128);
        b.extend_weight(&(-1i64).to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &10i64.to_le_bytes());
        b.extend_col(1, &5i64.to_le_bytes());
        b.count += 1;
        b.sorted = true;
        b.consolidated = true;
        ConsolidatedBatch::new_unchecked(b)
    };

    let (_raw2, fin2) = op_reduce(
        &delta2,
        None,
        to_ch2.cursor_mut(),
        &in_schema,
        &out_schema,
        &[1u32],
        &aggs,
        None,
        false,
        TypeCode::U64,
        None,
        0,
        Some(&fin_prog),
        Some(&fin_schema),
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
    use crate::storage::CursorHandle;
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
    let mut to_ch = CursorHandle::from_owned(&[empty_out], out_schema);
    let delta1 = {
        let mut b = Batch::with_schema(in_schema, 1);
        b.extend_pk(1u128);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&(1u64 << 1).to_le_bytes()); // val (payload idx 1) NULL
        b.extend_col(0, &10i64.to_le_bytes());
        b.extend_col(1, &0i64.to_le_bytes());
        b.count += 1;
        b.sorted = true;
        b.consolidated = true;
        ConsolidatedBatch::new_unchecked(b)
    };
    let (out1, _) = op_reduce(
        &delta1,
        None,
        to_ch.cursor_mut(),
        &in_schema,
        &out_schema,
        &[1u32],
        &aggs,
        None,
        false,
        TypeCode::U64,
        None,
        0,
        None,
        None,
    );
    assert_eq!(out1.count, 1);
    assert!(
        out1.as_mem_batch().get_null_word(0) & min_null_bit != 0,
        "tick1 MIN must be NULL"
    );

    // Tick 2: (pk=2, grp=10, val=7) → MIN=7, retracts the NULL row.
    let prev = Rc::new(out1);
    let mut to_ch2 = CursorHandle::from_owned(&[prev], out_schema);
    let delta2 = {
        let mut b = Batch::with_schema(in_schema, 1);
        b.extend_pk(2u128);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &10i64.to_le_bytes());
        b.extend_col(1, &7i64.to_le_bytes());
        b.count += 1;
        b.sorted = true;
        b.consolidated = true;
        ConsolidatedBatch::new_unchecked(b)
    };
    let (out2, _) = op_reduce(
        &delta2,
        None,
        to_ch2.cursor_mut(),
        &in_schema,
        &out_schema,
        &[1u32],
        &aggs,
        None,
        false,
        TypeCode::U64,
        None,
        0,
        None,
        None,
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
    use crate::storage::CursorHandle;
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
        b.sorted = true;
        b.consolidated = true;
        ConsolidatedBatch::new_unchecked(b)
    };

    let empty_out = Rc::new(Batch::empty_with_schema(&out_schema));
    let mut to_ch = CursorHandle::from_owned(&[empty_out], out_schema);
    let (out1, _) = op_reduce(
        &null_row(1),
        None,
        to_ch.cursor_mut(),
        &in_schema,
        &out_schema,
        &[1u32],
        &aggs,
        None,
        false,
        TypeCode::U64,
        None,
        0,
        None,
        None,
    );
    assert!(
        out1.as_mem_batch().get_null_word(0) & sum_null_bit != 0,
        "tick1 SUM NULL"
    );

    let prev = Rc::new(out1);
    let mut to_ch2 = CursorHandle::from_owned(&[prev], out_schema);
    let (out2, _) = op_reduce(
        &null_row(2),
        None,
        to_ch2.cursor_mut(),
        &in_schema,
        &out_schema,
        &[1u32],
        &aggs,
        None,
        false,
        TypeCode::U64,
        None,
        0,
        None,
        None,
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
    use crate::storage::CursorHandle;
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
    let empty_out = Rc::new(Batch::empty(out_schema.num_payload_cols(), 16));
    let mut to_ch = CursorHandle::from_owned(&[empty_out], out_schema);
    let delta1 = {
        let mut b = Batch::with_schema(in_schema, 2);
        for val in [100i64, 200] {
            b.extend_pk_bytes(&pk(7, 7, 7));
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &val.to_le_bytes());
            b.count += 1;
        }
        b.sorted = true;
        b.consolidated = true;
        ConsolidatedBatch::new_unchecked(b)
    };
    let (out1, _) = op_reduce(
        &delta1,
        None,
        to_ch.cursor_mut(),
        &in_schema,
        &out_schema,
        &group_by,
        &aggs,
        None,
        false,
        TypeCode::U64,
        None,
        0,
        None,
        None,
    );
    assert_eq!(out1.count, 1, "one group");
    assert_eq!(out1.get_pk_bytes(0), &pk(7, 7, 7)[..]);
    assert_eq!(read_i64_le(out1.col_data(0), 0), 300);

    // Tick 2: retract the val=200 row. SUM 300 → 100; reads the prior aggregate
    // out of trace_out by PK bytes.
    let prev_out = Rc::new(out1);
    let mut to_ch2 = CursorHandle::from_owned(&[prev_out], out_schema);
    let delta2 = {
        let mut b = Batch::with_schema(in_schema, 1);
        b.extend_pk_bytes(&pk(7, 7, 7));
        b.extend_weight(&(-1i64).to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &200i64.to_le_bytes());
        b.count += 1;
        b.sorted = true;
        b.consolidated = true;
        ConsolidatedBatch::new_unchecked(b)
    };
    let (out2, _) = op_reduce(
        &delta2,
        None,
        to_ch2.cursor_mut(),
        &in_schema,
        &out_schema,
        &group_by,
        &aggs,
        None,
        false,
        TypeCode::U64,
        None,
        0,
        None,
        None,
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
    use crate::storage::CursorHandle;
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
    let empty_out = Rc::new(Batch::empty(out_schema.num_payload_cols(), 16));
    let mut to_ch = CursorHandle::from_owned(&[empty_out], out_schema);
    let delta1 = {
        let mut b = Batch::with_schema(in_schema, 2);
        for &(a, c, val) in &[(1u64, 5u64, 100i64), (2, 3, 200)] {
            b.extend_pk_bytes(&pk(a, c));
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &val.to_le_bytes());
            b.count += 1;
        }
        b.sorted = true;
        b.consolidated = true;
        ConsolidatedBatch::new_unchecked(b)
    };
    let (out1, _) = op_reduce(
        &delta1,
        None,
        to_ch.cursor_mut(),
        &in_schema,
        &out_schema,
        &group_by,
        &aggs,
        None,
        false,
        TypeCode::U64,
        None,
        0,
        None,
        None,
    );
    assert_eq!(out1.count, 2, "two groups");
    assert_eq!(out1.get_pk_bytes(0), &pk(1, 5)[..]);
    assert_eq!(read_i64_le(out1.col_data(0), 0), 100);
    assert_eq!(out1.get_pk_bytes(1), &pk(2, 3)[..]);
    assert_eq!(read_i64_le(out1.col_data(0), 8), 200);

    // Tick 2: insert (2,3)->50. SUM for (2,3) goes 200 → 250: retract 200,
    // insert 250. Group (1,5) is untouched.
    let prev_out = Rc::new(out1);
    let mut to_ch2 = CursorHandle::from_owned(&[prev_out], out_schema);
    let delta2 = {
        let mut b = Batch::with_schema(in_schema, 1);
        b.extend_pk_bytes(&pk(2, 3));
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &50i64.to_le_bytes());
        b.count += 1;
        b.sorted = true;
        b.consolidated = true;
        ConsolidatedBatch::new_unchecked(b)
    };
    let (out2, _) = op_reduce(
        &delta2,
        None,
        to_ch2.cursor_mut(),
        &in_schema,
        &out_schema,
        &group_by,
        &aggs,
        None,
        false,
        TypeCode::U64,
        None,
        0,
        None,
        None,
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
    use crate::storage::CursorHandle;
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
    let empty_out = Rc::new(Batch::empty(out_schema.num_payload_cols(), 16));
    let mut to_ch = CursorHandle::from_owned(&[empty_out], out_schema);
    let delta1 = {
        let mut b = Batch::with_schema(in_schema, 2);
        for &(k, val) in &[(-1i64, 200i64), (2, 100)] {
            b.extend_pk_opk(&in_schema, &[(k as u64) as u128]);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &val.to_le_bytes());
            b.count += 1;
        }
        b.sorted = true;
        b.consolidated = true;
        ConsolidatedBatch::new_unchecked(b)
    };
    let (out1, _) = op_reduce(
        &delta1,
        None,
        to_ch.cursor_mut(),
        &in_schema,
        &out_schema,
        &group_by,
        &aggs,
        None,
        false,
        TypeCode::U64,
        None,
        0,
        None,
        None,
    );
    assert_eq!(out1.count, 2, "two groups (-1 sorts before 2)");
    assert_eq!(opk_pk_i64(out1.get_pk_bytes(0)), -1);
    assert_eq!(read_i64_le(out1.col_data(0), 0), 200);

    // Tick 2: insert key=-1 -> 50. SUM goes 200 → 250: retract + insert.
    let prev_out = Rc::new(out1);
    let mut to_ch2 = CursorHandle::from_owned(&[prev_out], out_schema);
    let delta2 = {
        let mut b = Batch::with_schema(in_schema, 1);
        b.extend_pk_opk(&in_schema, &[((-1i64) as u64) as u128]);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &50i64.to_le_bytes());
        b.count += 1;
        b.sorted = true;
        b.consolidated = true;
        ConsolidatedBatch::new_unchecked(b)
    };
    let (out2, _) = op_reduce(
        &delta2,
        None,
        to_ch2.cursor_mut(),
        &in_schema,
        &out_schema,
        &group_by,
        &aggs,
        None,
        false,
        TypeCode::U64,
        None,
        0,
        None,
        None,
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
    use crate::storage::CursorHandle;
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

    let empty_out = Rc::new(Batch::empty(1, 16));
    let mut to_ch = CursorHandle::from_owned(&[empty_out], out_schema);

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
        to_ch.cursor_mut(),
        &in_schema,
        &out_schema,
        &[0u32],
        &[agg],
        None,
        false,
        TypeCode::U64,
        None,
        0,
        None,
        None,
    );
    // Each pk forms its own group, COUNT=1 for each
    assert_eq!(out.count, 3);
    for i in 0..3 {
        let count = read_i64_le(out.col_data(0), i * 8);
        assert_eq!(count, 1, "each single-row group has count=1");
    }
}

/// GI path bug: same PK, two different string payloads — the `if` must be `while`.
#[test]
fn test_reduce_gi_same_pk_multiple_payloads() {
    use crate::storage::CursorHandle;
    use std::rc::Rc;

    let input_schema = make_schema_3col_grp_str();
    let output_schema = make_reduce_str_out_schema();
    let gi_schema = crate::ops::index::make_gi_schema(&input_schema);

    // trace_in: apple and zebra both at PK=1 (apple sorts first by payload)
    let ti_batch =
        Rc::new(make_batch_3col_grp_str(&input_schema, &[(1, 1, 1, "apple"), (1, 1, 1, "zebra")]).into_inner());

    // GI: only PK=1 → group gc_u64=1
    let gi_batch = Rc::new(make_gi_batch(&input_schema, &[(1, 1)]).into_inner());

    // trace_out: empty (no previous aggregate, no retraction emitted)
    let to_batch = Rc::new(Batch::empty(output_schema.num_payload_cols(), 16));

    // delta: retract apple at PK=1
    let delta = make_batch_3col_grp_str(&input_schema, &[(1, -1, 1, "apple")]);

    let mut ti_handle = CursorHandle::from_owned(&[ti_batch], input_schema);
    let mut gi_handle = CursorHandle::from_owned(&[gi_batch], gi_schema);
    let mut to_handle = CursorHandle::from_owned(&[to_batch], output_schema);

    // MAX on STRING agg col (col_idx=2, type=STRING); no AVI
    let agg_desc = AggDescriptor {
        col_idx: 2,
        agg_op: AggOp::Max,
        col_type_code: TypeCode::String,
        _pad: [0; 2],
    };

    let (out, _fin) = op_reduce(
        &delta,
        Some(ti_handle.cursor_mut()),
        to_handle.cursor_mut(),
        &input_schema,
        &output_schema,
        &[1u32], // group_by_cols: col 1 (grp)
        &[agg_desc],
        None,                         // avi_cursor
        false,                        // avi_for_max
        TypeCode::String,             // avi_agg_col_type_code (unused; no AVI)
        Some(gi_handle.cursor_mut()), // gi_cursor
        1u32,                         // gi_col_idx: grp column
        None,                         // finalize_prog
        None,                         // finalize_out_schema
    );

    // The accumulator stores the first 8 bytes of the German string as i64.
    // "zebra" first 8 bytes: [len=5, 'z'=122, 'e'=101, 'b'=98, 'r'=114]
    // "apple" first 8 bytes: [len=5, 'a'=97,  'p'=112, 'p'=112, 'l'=108]
    let zebra_ck = i64::from_le_bytes([5, 0, 0, 0, 122, 101, 98, 114]);
    let apple_ck = i64::from_le_bytes([5, 0, 0, 0, 97, 112, 112, 108]);
    assert!(zebra_ck > apple_ck, "test invariant: zebra_ck > apple_ck");

    // With fix: replay = {apple+1, zebra+1, apple−1} → {zebra+1}; one output row.
    // With bug: replay = {apple+1, apple−1} → {}; no output row.
    assert_eq!(
        out.count, 1,
        "GI loop must be `while` to include zebra after apple is retracted; \
         `if` leaves replay empty → no output"
    );

    // Output payload layout: col_data[0]=grp(I64), col_data[1]=agg(I64)
    let agg = read_i64_le(out.col_data(1), 0);
    assert_eq!(
        agg, zebra_ck,
        "MAX of {{zebra+1}} must be zebra_ck; got {agg} (apple_ck={apple_ck})"
    );
}

// -----------------------------------------------------------------------
// op_gather_reduce
// -----------------------------------------------------------------------

#[test]
fn test_gather_reduce_retraction() {
    use crate::schema::type_code;
    use crate::storage::CursorHandle;
    use std::rc::Rc;

    // Schema: pk(U128), count(I64) — same as partial/output schema
    let schema = SchemaDescriptor::new(
        &[
            crate::schema::SchemaColumn::new(type_code::U128, 0),
            crate::schema::SchemaColumn::new(type_code::I64, 1),
        ],
        &[0],
    );

    // Tick 1: two partial COUNT=2 from different workers → global COUNT=4
    let empty_out = Rc::new(Batch::empty(1, 16));
    let mut to_ch = CursorHandle::from_owned(&[empty_out], schema);

    let mut partial1 = Batch::with_schema(schema, 2);

    // Two entries for same group key (pk=1), count=2 each
    for count in [2i64, 2] {
        partial1.extend_pk(1u128);
        partial1.extend_weight(&1i64.to_le_bytes());
        partial1.extend_null_bmp(&0u64.to_le_bytes());
        partial1.extend_col(0, &count.to_le_bytes());
        partial1.count += 1;
    }

    let agg = AggDescriptor {
        col_idx: 1,
        agg_op: AggOp::Sum,
        col_type_code: TypeCode::I64,
        _pad: [0; 2],
    };

    let out1 = op_gather_reduce(&partial1, to_ch.cursor_mut(), &schema, &[agg]);
    assert_eq!(out1.count, 1);
    let global_count = read_i64_le(out1.col_data(0), 0);
    assert_eq!(global_count, 4);

    // Tick 2: retract 1 from each worker → partial counts are -1 each → global delta = -2
    let prev_out = Rc::new(out1);
    let mut to_ch2 = CursorHandle::from_owned(&[prev_out], schema);

    let mut partial2 = Batch::with_schema(schema, 2);

    for count in [-1i64, -1] {
        partial2.extend_pk(1u128);
        partial2.extend_weight(&1i64.to_le_bytes());
        partial2.extend_null_bmp(&0u64.to_le_bytes());
        partial2.extend_col(0, &count.to_le_bytes());
        partial2.count += 1;
    }

    let out2 = op_gather_reduce(&partial2, to_ch2.cursor_mut(), &schema, &[agg]);
    // Should have 2 rows: retract old (4, w=-1) + insert new (2, w=+1)
    assert_eq!(out2.count, 2);
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

fn make_batch_typed_i32(schema: &SchemaDescriptor, rows: &[(u64, i64, i32)]) -> ConsolidatedBatch {
    let n = rows.len();
    let mut b = Batch::with_schema(*schema, n.max(1));

    for &(pk, w, val) in rows {
        b.extend_pk(pk as u128);
        b.extend_weight(&w.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &val.to_le_bytes());
        b.count += 1;
    }
    b.sorted = true;
    b.consolidated = true;
    ConsolidatedBatch::new_unchecked(b)
}

fn make_batch_typed_i16(schema: &SchemaDescriptor, rows: &[(u64, i64, i16)]) -> ConsolidatedBatch {
    let n = rows.len();
    let mut b = Batch::with_schema(*schema, n.max(1));

    for &(pk, w, val) in rows {
        b.extend_pk(pk as u128);
        b.extend_weight(&w.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &val.to_le_bytes());
        b.count += 1;
    }
    b.sorted = true;
    b.consolidated = true;
    ConsolidatedBatch::new_unchecked(b)
}

#[test]
fn test_reduce_sum_i32() {
    use crate::storage::CursorHandle;
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

    let empty_out = Rc::new(Batch::empty(2, 16));
    let mut to_ch = CursorHandle::from_owned(&[empty_out], out_schema);

    // 3 rows with I32 values, group by PK
    let delta = make_batch_typed_i32(&in_schema, &[(1, 1, 100i32), (2, 1, 200i32), (3, 1, -50i32)]);

    let aggs = sum_count_aggs(1, TypeCode::I32);

    // GROUP BY pk → each row is its own group
    let (out, _) = op_reduce(
        &delta,
        None,
        to_ch.cursor_mut(),
        &in_schema,
        &out_schema,
        &[0u32],
        &aggs,
        None,
        false,
        TypeCode::U64,
        None,
        0,
        None,
        None,
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
    use crate::storage::CursorHandle;
    use std::rc::Rc;

    let in_schema = make_schema_with_type(type_code::F32);

    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::I64, 1),
        ],
        &[0],
    );

    let empty_out = Rc::new(Batch::empty(1, 16));
    let mut to_ch = CursorHandle::from_owned(&[empty_out], out_schema);

    // Use a 2-col input schema: pk(U64), val(F32), GROUP BY pk
    let delta = make_batch_f32(&in_schema, &[(1, 1, 3.5f32), (1, 1, -1.0f32), (1, 1, 7.0f32)]);

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
        to_ch.cursor_mut(),
        &in_schema,
        &out_schema,
        &[0u32],
        &[agg],
        None,
        false,
        TypeCode::U64,
        None,
        0,
        None,
        None,
    );
    assert_eq!(out.count, 1);
    // MIN should be -1.0 stored as f64 bits
    let bits = u64::from_le_bytes(out.col_data(0)[0..8].try_into().unwrap());
    let min_val = f64::from_bits(bits);
    assert_eq!(min_val, -1.0f64, "MIN of F32 {{3.5, -1.0, 7.0}} should be -1.0");
}

#[test]
fn test_reduce_max_i16() {
    use crate::storage::CursorHandle;
    use std::rc::Rc;

    let in_schema = make_schema_with_type(type_code::I16);

    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::I64, 1),
        ],
        &[0],
    );

    let empty_out = Rc::new(Batch::empty(1, 16));
    let mut to_ch = CursorHandle::from_owned(&[empty_out], out_schema);

    // 3 rows with I16 values, all same PK
    let delta = make_batch_typed_i16(&in_schema, &[(1, 1, -100i16), (1, 1, 200i16), (1, 1, 50i16)]);

    let agg = AggDescriptor {
        col_idx: 1,
        agg_op: AggOp::Max,
        col_type_code: TypeCode::I16,
        _pad: [0; 2],
    };

    let (out, _) = op_reduce(
        &delta,
        None,
        to_ch.cursor_mut(),
        &in_schema,
        &out_schema,
        &[0u32],
        &[agg],
        None,
        false,
        TypeCode::U64,
        None,
        0,
        None,
        None,
    );
    assert_eq!(out.count, 1);
    let max_val = read_i64_le(out.col_data(0), 0);
    assert_eq!(max_val, 200, "MAX of I16 {{-100, 200, 50}} should be 200");
}

// -----------------------------------------------------------------------
// Fix 6: gather_reduce MIN retraction
// -----------------------------------------------------------------------

#[test]
fn test_gather_reduce_min_retraction() {
    use crate::storage::CursorHandle;
    use std::rc::Rc;

    // Schema: pk(U128), min_val(I64)
    let schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::I64, 1),
        ],
        &[0],
    );

    // Tick 1: partial MIN=5 from one worker → global MIN=5
    let empty_out = Rc::new(Batch::empty(1, 16));
    let mut to_ch = CursorHandle::from_owned(&[empty_out], schema);

    let mut partial1 = Batch::with_schema(schema, 1);

    partial1.extend_pk(1u128);
    partial1.extend_weight(&1i64.to_le_bytes());
    partial1.extend_null_bmp(&0u64.to_le_bytes());
    partial1.extend_col(0, &5i64.to_le_bytes());
    partial1.count += 1;

    let agg = AggDescriptor {
        col_idx: 1,
        agg_op: AggOp::Min,
        col_type_code: TypeCode::I64,
        _pad: [0; 2],
    };

    let out1 = op_gather_reduce(&partial1, to_ch.cursor_mut(), &schema, &[agg]);
    assert_eq!(out1.count, 1);
    let min1 = read_i64_le(out1.col_data(0), 0);
    assert_eq!(min1, 5);

    // Tick 2: partial MIN=3 from one worker. The old global (5) should be folded in
    // via merge_accumulated with weight=1 → combine(5). New MIN should be min(3, 5) = 3.
    let prev_out = Rc::new(out1);
    let mut to_ch2 = CursorHandle::from_owned(&[prev_out], schema);

    let mut partial2 = Batch::with_schema(schema, 1);

    partial2.extend_pk(1u128);
    partial2.extend_weight(&1i64.to_le_bytes());
    partial2.extend_null_bmp(&0u64.to_le_bytes());
    partial2.extend_col(0, &3i64.to_le_bytes());
    partial2.count += 1;

    let out2 = op_gather_reduce(&partial2, to_ch2.cursor_mut(), &schema, &[agg]);
    // Should have: retract old (5, w=-1) + insert new (3, w=+1)
    assert_eq!(out2.count, 2, "should retract old MIN and emit new MIN");
    let retracted = read_i64_le(out2.col_data(0), 0);
    assert_eq!(retracted, 5, "retraction should be old MIN value 5");
    assert_eq!(out2.get_weight(0), -1);
    let new_min = read_i64_le(out2.col_data(0), 8);
    assert_eq!(new_min, 3, "new MIN should be 3 (min of old 5 and partial 3)");
    assert_eq!(out2.get_weight(1), 1);
}

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
    b.sorted = true;
    b.consolidated = true;
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
    b.sorted = true;
    b.consolidated = true;
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
// GI group-key over-read bug: narrow-type group column
// -----------------------------------------------------------------------

/// GI path reads the group key with a hardcoded col_size=8. When the group
/// column is narrower (e.g. I32, stride=4) and group_start_idx > 0, the
/// stride-8 indexing walks into the adjacent I64 column region, producing a
/// garbage gc_u64_val. The GI seek then misses and history rows are not
/// fetched, so MIN returns only the delta value instead of the true minimum.
#[test]
fn test_reduce_gi_i32_group_key_overread() {
    use crate::storage::CursorHandle;
    use std::rc::Rc;

    // Input: U64 pk | I32 grp (4 bytes) | I64 val (8 bytes)
    let in_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I32, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    );

    // Output: U128 hash-pk | I32 grp | I64 min (nullable)
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::I32, 0),
            SchemaColumn::new(type_code::I64, 1),
        ],
        &[0],
    );

    let gi_schema = crate::ops::index::make_gi_schema(&in_schema);

    // trace_in: history row for grp=5, val=200, source pk=30
    let ti_batch = Rc::new({
        let mut b = Batch::with_schema(in_schema, 1);
        b.extend_pk(30u128);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &5i32.to_le_bytes());
        b.extend_col(1, &200i64.to_le_bytes());
        b.count += 1;
        b.sorted = true;
        b.consolidated = true;
        ConsolidatedBatch::new_unchecked(b).into_inner()
    });

    // GI: gc_u64=5 → source pk=30
    let gi_batch = Rc::new(make_gi_batch(&in_schema, &[(30, 5)]).into_inner());

    // Empty trace_out (no prior aggregate)
    let to_batch = Rc::new(Batch::empty(2, 16));

    // Delta: 2 groups so that grp=5 is at group_start_idx=1 after argsort.
    // With the bug: get_col_ptr(1, pi=0, col_size=8) uses stride 8 on a
    // column with stride 4, landing at offset 48+8=56 in the batch buffer
    // which is the start of the I64 val region. It reads val[row0]=100 as
    // gc_u64_val instead of 5, the GI seek misses, and MIN(grp=5)=300.
    let delta = {
        let mut b = Batch::with_schema(in_schema, 2);
        b.extend_pk(10u128);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &3i32.to_le_bytes());
        b.extend_col(1, &100i64.to_le_bytes());
        b.count += 1;
        b.extend_pk(20u128);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &5i32.to_le_bytes());
        b.extend_col(1, &300i64.to_le_bytes());
        b.count += 1;
        b.sorted = true;
        b.consolidated = true;
        ConsolidatedBatch::new_unchecked(b)
    };

    let mut ti_handle = CursorHandle::from_owned(&[ti_batch], in_schema);
    let mut gi_handle = CursorHandle::from_owned(&[gi_batch], gi_schema);
    let mut to_handle = CursorHandle::from_owned(&[to_batch], out_schema);

    let agg = AggDescriptor {
        col_idx: 2,
        agg_op: AggOp::Min,
        col_type_code: TypeCode::I64,
        _pad: [0; 2],
    };

    let (out, _) = op_reduce(
        &delta,
        Some(ti_handle.cursor_mut()),
        to_handle.cursor_mut(),
        &in_schema,
        &out_schema,
        &[1u32],
        &[agg],
        None,
        false,
        TypeCode::U64,
        Some(gi_handle.cursor_mut()),
        1u32,
        None,
        None,
    );

    assert_eq!(out.count, 2, "expected 2 output groups (grp=3 and grp=5)");

    // Output payload: col 0 = I32 grp (4 bytes/row), col 1 = I64 min (8 bytes/row)
    let grp_data = out.col_data(0);
    let min_data = out.col_data(1);
    let mut min_for_5 = None;
    for i in 0..2 {
        let g = i32::from_le_bytes(grp_data[i * 4..(i + 1) * 4].try_into().unwrap());
        if g == 5 {
            let m = i64::from_le_bytes(min_data[i * 8..(i + 1) * 8].try_into().unwrap());
            min_for_5 = Some(m);
        }
    }
    let m = min_for_5.expect("no output row for grp=5");
    assert_eq!(
        m, 200,
        "MIN(grp=5) must include history row val=200; \
         got {m} — GI group-key over-read produced a garbage gc_u64_val"
    );
}

// -----------------------------------------------------------------------
// Byte-form GI key: signed source PK, wide source PK, narrow equivalence.
// -----------------------------------------------------------------------

/// Read the MIN (output col 1, I64) for a given group value (output col 0).
/// Output PK is the synthetic U128 hash, so groups are matched by the carried
/// group column, not by row order.
fn gi_out_min_for_grp(out: &Batch, grp: i64) -> Option<i64> {
    let grp_data = out.col_data(0);
    let min_data = out.col_data(1);
    for i in 0..out.count {
        let g = i64::from_le_bytes(grp_data[i * 8..(i + 1) * 8].try_into().unwrap());
        if g == grp {
            return Some(i64::from_le_bytes(min_data[i * 8..(i + 1) * 8].try_into().unwrap()));
        }
    }
    None
}

/// Synthetic-PK reduce output for a single I64 group column + single I64 MIN:
/// U128 pk | I64 grp | I64 min (nullable).
fn gi_synthetic_out_schema() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::I64, 1),
        ],
        &[0],
    )
}

/// GI read-back over a SIGNED source PK with negative key values. The GI schema
/// remaps the signed source-PK column to unsigned so a zero-padded prefix seek
/// lands on the first entry of the `gc` group instead of sorting the negative
/// keys before it and skipping them; the per-hit trace re-seek uses
/// `seek_bytes`/`current_pk_eq`, which compare PK *bytes* for a signed column
/// rather than the scalar `current_key` (narrow-PK-only). Before the byte-form
/// change the negative source rows were dropped and the MIN reflected only the
/// delta.
#[test]
fn test_reduce_gi_signed_source_pk_negative() {
    use crate::storage::CursorHandle;
    use std::rc::Rc;

    // Source: I64 pk (col 0) | I64 grp (col 1) | I64 val (col 2).
    let in_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    );
    let out_schema = gi_synthetic_out_schema();
    let gi_schema = crate::ops::index::make_gi_schema(&in_schema);

    let build = |rows: &[(i64, i64, i64)]| -> Batch {
        let mut b = Batch::with_schema(in_schema, rows.len().max(1));
        for &(pk, grp, val) in rows {
            // Low 8 bytes of the u128 carry the signed PK's two's-complement LE.
            b.extend_pk((pk as u64) as u128);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &grp.to_le_bytes());
            b.extend_col(1, &val.to_le_bytes());
            b.count += 1;
        }
        b.sorted = true;
        b.consolidated = true;
        b
    };

    // History in grp=7 at NEGATIVE source PKs, ascending signed order (-5, -3).
    // The smaller val (50) lives at pk=-5: if its prefix seek is skipped, MIN
    // misses it.
    let ti_batch = Rc::new(build(&[(-5, 7, 50), (-3, 7, 300)]));

    // GI: gc=7 ++ source pk. Listed in remapped-unsigned order: -5 → 0xFF..FB
    // sorts before -3 → 0xFF..FD.
    let gi_batch = Rc::new(make_gi_batch(&in_schema, &[((-5i64) as u64, 7), ((-3i64) as u64, 7)]).into_inner());

    let to_batch = Rc::new(Batch::empty(out_schema.num_payload_cols(), 16));

    // Delta: a new positive-PK row in grp=7 with the largest val.
    let delta = build(&[(99, 7, 400)]);

    let mut ti_handle = CursorHandle::from_owned(&[ti_batch], in_schema);
    let mut gi_handle = CursorHandle::from_owned(&[gi_batch], gi_schema);
    let mut to_handle = CursorHandle::from_owned(&[to_batch], out_schema);

    let agg = AggDescriptor {
        col_idx: 2,
        agg_op: AggOp::Min,
        col_type_code: TypeCode::I64,
        _pad: [0; 2],
    };

    let (out, _) = op_reduce(
        &delta,
        Some(ti_handle.cursor_mut()),
        to_handle.cursor_mut(),
        &in_schema,
        &out_schema,
        &[1u32],
        &[agg],
        None,
        false,
        TypeCode::U64,
        Some(gi_handle.cursor_mut()),
        1u32,
        None,
        None,
    );

    let m = gi_out_min_for_grp(&out, 7).expect("no output row for grp=7");
    assert_eq!(
        m, 50,
        "MIN(grp=7) must include the negative-PK history rows (50, 300); \
         got {m} — the prefix seek skipped the negative source PKs"
    );
}

/// GI re-seek over a WIDE source PK `(U64, U64, U64)` (stride 24). The byte-form
/// GI key round-trips the full 24-byte source PK so the trace re-seek finds the
/// wide source rows; the old `(lo, hi)` u128 packing could not represent a PK
/// past 16 bytes. GROUP BY is a separate small column, so the reduce takes the
/// slow group-identity path and only the GI source re-seek exercises the wide
/// width.
#[test]
fn test_reduce_gi_wide_source_pk_stride24() {
    use crate::storage::CursorHandle;
    use std::rc::Rc;

    // Source: U64,U64,U64 PK (cols 0..2) | I64 grp (col 3) | I64 val (col 4).
    let in_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0, 1, 2],
    );
    assert!(in_schema.pk_stride() > 16, "source PK must be wide (stride 24)");
    let out_schema = gi_synthetic_out_schema();
    let gi_schema = crate::ops::index::make_gi_schema(&in_schema);
    assert_eq!(gi_schema.pk_stride(), 32, "GI key = gc(8) + 24-byte source PK");

    let build = |rows: &[([u64; 3], i64, i64)]| -> Batch {
        let mut b = Batch::with_schema(in_schema, rows.len().max(1));
        for &(pk, grp, val) in rows {
            let mut kb = [0u8; 24];
            // Compound PK of unsigned U64 columns: OPK == BE per column.
            kb[0..8].copy_from_slice(&pk[0].to_be_bytes());
            kb[8..16].copy_from_slice(&pk[1].to_be_bytes());
            kb[16..24].copy_from_slice(&pk[2].to_be_bytes());
            b.extend_pk_bytes(&kb);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &grp.to_le_bytes()); // payload idx 0 = grp (col 3)
            b.extend_col(1, &val.to_le_bytes()); // payload idx 1 = val (col 4)
            b.count += 1;
        }
        b.sorted = true;
        b.consolidated = true;
        b
    };

    // History in grp=9 at two wide PKs, ascending PK-byte order.
    let ti_batch = Rc::new(build(&[([1, 2, 3], 9, 500), ([4, 5, 6], 9, 250)]));

    // GI: gc=9 ++ 24-byte source PK, in PK-byte order.
    let gi_batch = Rc::new({
        let mut gib = Batch::with_schema(gi_schema, 2);
        for &(pk, gc) in &[([1u64, 2, 3], 9u64), ([4, 5, 6], 9)] {
            let mut key = [0u8; 32];
            // GI key = gc(LE) ++ source PK (OPK bytes). The source table stores
            // its compound U64 PK as OPK == BE per column, so the GI's source-PK
            // segment must match those bytes for the trace re-seek to land.
            key[0..8].copy_from_slice(&gc.to_le_bytes());
            key[8..16].copy_from_slice(&pk[0].to_be_bytes());
            key[16..24].copy_from_slice(&pk[1].to_be_bytes());
            key[24..32].copy_from_slice(&pk[2].to_be_bytes());
            gib.extend_pk_bytes(&key);
            gib.extend_weight(&1i64.to_le_bytes());
            gib.extend_null_bmp(&0u64.to_le_bytes());
            gib.count += 1;
        }
        gib.sorted = true;
        gib.consolidated = true;
        gib
    });

    let to_batch = Rc::new(Batch::empty(out_schema.num_payload_cols(), 16));

    // Delta: a new wide-PK row in grp=9 with the largest val.
    let delta = build(&[([7, 8, 9], 9, 900)]);

    let mut ti_handle = CursorHandle::from_owned(&[ti_batch], in_schema);
    let mut gi_handle = CursorHandle::from_owned(&[gi_batch], gi_schema);
    let mut to_handle = CursorHandle::from_owned(&[to_batch], out_schema);

    let agg = AggDescriptor {
        col_idx: 4,
        agg_op: AggOp::Min,
        col_type_code: TypeCode::I64,
        _pad: [0; 2],
    };

    let (out, _) = op_reduce(
        &delta,
        Some(ti_handle.cursor_mut()),
        to_handle.cursor_mut(),
        &in_schema,
        &out_schema,
        &[3u32],
        &[agg],
        None,
        false,
        TypeCode::U64,
        Some(gi_handle.cursor_mut()),
        3u32,
        None,
        None,
    );

    let m = gi_out_min_for_grp(&out, 9).expect("no output row for grp=9");
    assert_eq!(
        m, 250,
        "MIN(grp=9) must include the wide-PK history rows (500, 250); \
         got {m} — the wide source PK was not re-found through the GI key"
    );
}

/// The byte-form GI MIN result for a narrow (single U64) source PK must equal
/// the full-trace-scan result (no GI cursor). The byte-form key changes the
/// index bytes — the dead `source_pk_hi` payload is gone — but the re-found
/// source rows, and thus the aggregate, must be bit-identical.
#[test]
fn test_reduce_gi_narrow_source_matches_no_gi() {
    use crate::storage::CursorHandle;
    use std::rc::Rc;

    let in_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    );
    let out_schema = gi_synthetic_out_schema();
    let gi_schema = crate::ops::index::make_gi_schema(&in_schema);

    let build = |rows: &[(u64, i64, i64)]| -> Batch {
        let mut b = Batch::with_schema(in_schema, rows.len().max(1));
        for &(pk, grp, val) in rows {
            b.extend_pk(pk as u128);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &grp.to_le_bytes());
            b.extend_col(1, &val.to_le_bytes());
            b.count += 1;
        }
        b.sorted = true;
        b.consolidated = true;
        b
    };

    let history = [(1u64, 5i64, 100i64), (2, 5, 30), (3, 9, 70)];
    let delta_rows = [(4u64, 5i64, 20i64)];

    let run = |with_gi: bool| -> i64 {
        let ti_batch = Rc::new(build(&history));
        let to_batch = Rc::new(Batch::empty(out_schema.num_payload_cols(), 16));
        let delta = build(&delta_rows);
        let mut ti_handle = CursorHandle::from_owned(&[ti_batch], in_schema);
        let mut to_handle = CursorHandle::from_owned(&[to_batch], out_schema);
        let agg = AggDescriptor {
            col_idx: 2,
            agg_op: AggOp::Min,
            col_type_code: TypeCode::I64,
            _pad: [0; 2],
        };
        if with_gi {
            let gi_batch = Rc::new(make_gi_batch(&in_schema, &[(1, 5), (2, 5), (3, 9)]).into_inner());
            let mut gi_handle = CursorHandle::from_owned(&[gi_batch], gi_schema);
            let (out, _) = op_reduce(
                &delta,
                Some(ti_handle.cursor_mut()),
                to_handle.cursor_mut(),
                &in_schema,
                &out_schema,
                &[1u32],
                &[agg],
                None,
                false,
                TypeCode::U64,
                Some(gi_handle.cursor_mut()),
                1u32,
                None,
                None,
            );
            gi_out_min_for_grp(&out, 5).expect("GI: no output row for grp=5")
        } else {
            // No GI cursor → read-back falls back to the full predicate-filtered
            // trace scan.
            let (out, _) = op_reduce(
                &delta,
                Some(ti_handle.cursor_mut()),
                to_handle.cursor_mut(),
                &in_schema,
                &out_schema,
                &[1u32],
                &[agg],
                None,
                false,
                TypeCode::U64,
                None,
                1u32,
                None,
                None,
            );
            gi_out_min_for_grp(&out, 5).expect("no-GI: no output row for grp=5")
        }
    };

    let with_gi = run(true);
    let without_gi = run(false);
    assert_eq!(with_gi, 20, "MIN(grp=5) over {{100, 30, 20}} must be 20");
    assert_eq!(
        with_gi, without_gi,
        "byte-form GI MIN ({with_gi}) must equal the full-scan MIN ({without_gi})"
    );
}

/// A source PK that fills the whole column budget leaves no room for the `gc`
/// prefix, so the GI descriptor build panics rather than silently truncating.
#[test]
#[should_panic(expected = "leaves no room for the gc prefix")]
fn test_make_gi_schema_rejects_full_pk_column_budget() {
    let cols = [SchemaColumn::new(type_code::U8, 0); crate::schema::MAX_PK_COLUMNS];
    let pk: Vec<u32> = (0..crate::schema::MAX_PK_COLUMNS as u32).collect();
    let src = SchemaDescriptor::new(&cols, &pk);
    let _ = crate::ops::index::make_gi_schema(&src);
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
    b.sorted = true;
    b.consolidated = true;
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
    b.sorted = true;
    b.consolidated = true;
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
// emit_finalized_row: U128 PK projected through CopyCol must not panic
// when the destination column size is 16 bytes.
// -----------------------------------------------------------------------

#[test]
fn test_emit_finalized_row_u128_pk_copy_col() {
    use crate::expr::{LogicalInstr, LogicalProgram};

    // Raw output schema: U128 pk | I64 cnt
    let raw_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::I64, 1),
        ],
        &[0],
    );

    // Finalized schema: U128 pk_out | U128 pk_copy | I64 cnt
    let fin_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::I64, 1),
        ],
        &[0],
    );

    // Two COPY_COL instructions: copy col 0 (PK) and col 1 (cnt). `out` is the
    // dense finalized payload index (PK copy → payload 0 = fin col 1, cnt →
    // payload 1 = fin col 2); classify_output_cols carries it through verbatim.
    let instrs = vec![
        LogicalInstr::CopyCol {
            src_col: 0,
            out: 0,
            tc: 0,
        }, // copy raw col 0 (PK) → fin col 1
        LogicalInstr::CopyCol {
            src_col: 1,
            out: 1,
            tc: 0,
        }, // copy raw col 1 (cnt) → fin col 2
    ];
    let prog = LogicalProgram::new(instrs, 0, 0, vec![]).resolve(&raw_schema, false);

    // Build raw_output with one row: pk = a wide U128, cnt = 42
    let pk: u128 = 0x0123_4567_89AB_CDEF_FEDC_BA98_7654_3210u128;
    let mut raw_output = Batch::with_schema(raw_schema, 1);
    raw_output.extend_pk(pk);
    raw_output.extend_weight(&1i64.to_le_bytes());
    raw_output.extend_null_bmp(&0u64.to_le_bytes());
    raw_output.extend_col(0, &42i64.to_le_bytes());
    raw_output.count += 1;

    let mut fin_output = Batch::with_schema(fin_schema, 1);
    // Must not panic on the 16-byte PK slice. Pre-fix: `pk as u64` produced
    // 8 bytes and `[..cs]` with cs=16 panicked.
    let mut ctx = crate::expr::FinalizeContext::new(&prog, &raw_schema);
    emit_finalized_row(
        &mut fin_output,
        &raw_output,
        0,
        1,
        &prog,
        &raw_schema,
        &fin_schema,
        &mut ctx,
    );

    assert_eq!(fin_output.count, 1);
    // The PK copy lands in finalized payload column 0 (fin col 1, since fin col 0 is PK).
    let copied = u128::from_le_bytes(fin_output.col_data(0)[..16].try_into().unwrap());
    assert_eq!(copied, pk, "U128 PK must round-trip through emit_finalized_row");
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
    b.sorted = true;
    b.consolidated = true;
    b
}

/// emit_reduce_row natural-PK byte path on a 2×U64 compound PK: PK bytes
/// must be copied verbatim from the source row, not packed from group_key.
#[test]
fn test_emit_reduce_row_compound_pk_bytes() {
    let in_schema = make_compound_pk_2xu64_schema();

    // Output schema matches what build_reduce_output_schema would produce
    // for group_set_eq_pk on this input with a COUNT aggregate:
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
    use crate::storage::CursorHandle;
    use std::rc::Rc;

    let in_schema = make_compound_pk_2xu64_schema();

    // Output: full natural compound PK + I64 agg, matching the
    // build_reduce_output_schema layout for group_set_eq_pk.
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 1),
        ],
        &[0, 1],
    );

    let empty_out = Rc::new(Batch::empty(out_schema.num_payload_cols(), 16));
    let mut to_ch = CursorHandle::from_owned(&[empty_out], out_schema);

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
        to_ch.cursor_mut(),
        &in_schema,
        &out_schema,
        &[0u32, 1u32],
        &[agg],
        None,
        false,
        TypeCode::U64,
        None,
        0,
        None,
        None,
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
    use crate::storage::CursorHandle;
    use std::rc::Rc;

    let in_schema = make_schema_u64_i64();
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 1),
        ],
        &[0],
    );

    let empty_out = Rc::new(Batch::empty(1, 16));
    let mut to_ch = CursorHandle::from_owned(&[empty_out], out_schema);

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
        to_ch.cursor_mut(),
        &in_schema,
        &out_schema,
        &[0u32],
        &[agg],
        None,
        false,
        TypeCode::U64,
        None,
        0,
        None,
        None,
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
    use crate::storage::CursorHandle;
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

    let empty_out = Rc::new(Batch::empty(out_schema.num_payload_cols(), 16));
    let mut to_ch = CursorHandle::from_owned(&[empty_out], out_schema);

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
        to_ch.cursor_mut(),
        &in_schema,
        &out_schema,
        &[1u32, 0u32],
        &[agg],
        None,
        false,
        TypeCode::U64,
        None,
        0,
        None,
        None,
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
    use crate::storage::CursorHandle;
    use std::rc::Rc;

    let schema = make_compound_pk_2xu64_schema();
    // Cursor row: (pk0=10, pk1=99). Exemplar row: (pk0=10, pk1=42).
    // GROUP BY pk_col_0 → must match (both share pk0=10).
    let cursor_batch = Rc::new(make_batch_compound_2xu64(&schema, &[(10, 99, 1, 0)]));
    let exemplar_batch = make_batch_compound_2xu64(&schema, &[(10, 42, 1, 0), (20, 42, 1, 0)]);
    let exemplar_mb = exemplar_batch.as_mem_batch();

    let mut cursor_handle = CursorHandle::from_owned(&[cursor_batch], schema);
    let cursor = cursor_handle.cursor_mut();
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
    use crate::storage::CursorHandle;
    use std::rc::Rc;

    let in_schema = make_compound_pk_2xu64_schema();
    // GROUP BY a single U64 column → use_natural_pk via
    // is_single_col_natural_pk. Output: U64 pk + I64 count.
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 1),
        ],
        &[0],
    );

    let empty_out = Rc::new(Batch::empty(1, 16));
    let mut to_ch = CursorHandle::from_owned(&[empty_out], out_schema);

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
        to_ch.cursor_mut(),
        &in_schema,
        &out_schema,
        &[0u32],
        &[agg],
        None,
        false,
        TypeCode::U64,
        None,
        0,
        None,
        None,
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
) -> ConsolidatedBatch {
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
    b.sorted = true;
    b.consolidated = true;
    ConsolidatedBatch::new_unchecked(b)
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
) -> ConsolidatedBatch {
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
    b.sorted = true;
    b.consolidated = true;
    ConsolidatedBatch::new_unchecked(b)
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
    use crate::storage::CursorHandle;
    use std::rc::Rc;

    let in_schema = make_schema_u64pk_i64grp_u64val();
    let out_schema = make_out_schema_grp_u64agg();

    let empty_out = Rc::new(Batch::empty(out_schema.num_payload_cols(), 16));
    let mut to_ch = CursorHandle::from_owned(&[empty_out], out_schema);

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
        to_ch.cursor_mut(),
        &in_schema,
        &out_schema,
        &[1u32],
        &[agg],
        None,
        false,
        TypeCode::U64,
        None,
        0,
        None,
        None,
    );
    assert_eq!(out.count, 1);
    let min_bits = u64::from_le_bytes(out.col_data(1)[0..8].try_into().unwrap());
    assert_eq!(min_bits, 1u64, "MIN(u64) must use unsigned ordering");
}

#[test]
fn test_reduce_max_u64_high_bit_set() {
    use crate::storage::CursorHandle;
    use std::rc::Rc;

    let in_schema = make_schema_u64pk_i64grp_u64val();
    let out_schema = make_out_schema_grp_u64agg();

    let empty_out = Rc::new(Batch::empty(out_schema.num_payload_cols(), 16));
    let mut to_ch = CursorHandle::from_owned(&[empty_out], out_schema);

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
        to_ch.cursor_mut(),
        &in_schema,
        &out_schema,
        &[1u32],
        &[agg],
        None,
        false,
        TypeCode::U64,
        None,
        0,
        None,
        None,
    );
    assert_eq!(out.count, 1);
    let max_bits = u64::from_le_bytes(out.col_data(1)[0..8].try_into().unwrap());
    assert_eq!(max_bits, u64::MAX, "MAX(u64) must use unsigned ordering");
}

#[test]
fn test_reduce_min_u64_incremental() {
    use crate::storage::CursorHandle;
    use std::rc::Rc;

    let in_schema = make_schema_u64pk_i64grp_u64val();
    let out_schema = make_out_schema_grp_u64agg();

    // Tick 1: one row with val=1u64<<60 → MIN = 1u64<<60.
    let empty_out = Rc::new(Batch::empty(out_schema.num_payload_cols(), 16));
    let mut to_ch = CursorHandle::from_owned(&[empty_out], out_schema);

    let delta1 = make_batch_u64pk_i64grp_u64val(&in_schema, &[(1, 1, 7, 1u64 << 60)]);

    let agg = AggDescriptor {
        col_idx: 2,
        agg_op: AggOp::Min,
        col_type_code: TypeCode::U64,
        _pad: [0; 2],
    };

    let empty_ti = Rc::new(Batch::empty(in_schema.num_payload_cols(), 16));
    let mut ti_ch = CursorHandle::from_owned(&[empty_ti], in_schema);

    let (out1, _) = op_reduce(
        &delta1,
        Some(ti_ch.cursor_mut()),
        to_ch.cursor_mut(),
        &in_schema,
        &out_schema,
        &[1u32],
        &[agg],
        None,
        false,
        TypeCode::U64,
        None,
        0,
        None,
        None,
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
    let mut to_ch2 = CursorHandle::from_owned(&[prev_out], out_schema);

    let ti2 = Rc::new(make_batch_u64pk_i64grp_u64val(&in_schema, &[(1, 1, 7, 1u64 << 60)]).into_inner());
    let mut ti_ch2 = CursorHandle::from_owned(&[ti2], in_schema);

    let delta2 = make_batch_u64pk_i64grp_u64val(&in_schema, &[(2, 1, 7, 1u64 << 63)]);

    let (out2, _) = op_reduce(
        &delta2,
        Some(ti_ch2.cursor_mut()),
        to_ch2.cursor_mut(),
        &in_schema,
        &out_schema,
        &[1u32],
        &[agg],
        None,
        false,
        TypeCode::U64,
        None,
        0,
        None,
        None,
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
    use crate::storage::CursorHandle;
    use std::rc::Rc;

    let in_schema = make_schema_u64pk_i64grp_u64val();
    let out_schema = make_out_schema_grp_u64agg();

    // Tick 1: MAX over a single low value → MAX = 10.
    let empty_out = Rc::new(Batch::empty(out_schema.num_payload_cols(), 16));
    let mut to_ch = CursorHandle::from_owned(&[empty_out], out_schema);

    let delta1 = make_batch_u64pk_i64grp_u64val(&in_schema, &[(1, 1, 7, 10)]);

    let agg = AggDescriptor {
        col_idx: 2,
        agg_op: AggOp::Max,
        col_type_code: TypeCode::U64,
        _pad: [0; 2],
    };

    let empty_ti = Rc::new(Batch::empty(in_schema.num_payload_cols(), 16));
    let mut ti_ch = CursorHandle::from_owned(&[empty_ti], in_schema);

    let (out1, _) = op_reduce(
        &delta1,
        Some(ti_ch.cursor_mut()),
        to_ch.cursor_mut(),
        &in_schema,
        &out_schema,
        &[1u32],
        &[agg],
        None,
        false,
        TypeCode::U64,
        None,
        0,
        None,
        None,
    );
    assert_eq!(out1.count, 1);
    let max1 = u64::from_le_bytes(out1.col_data(1)[0..8].try_into().unwrap());
    assert_eq!(max1, 10);

    // Tick 2: delta adds val=u64::MAX. Replay re-steps over both.
    // Pre-fix signed MAX would treat u64::MAX as -1, keeping MAX=10.
    let prev_out = Rc::new(out1);
    let mut to_ch2 = CursorHandle::from_owned(&[prev_out], out_schema);

    let ti2 = Rc::new(make_batch_u64pk_i64grp_u64val(&in_schema, &[(1, 1, 7, 10)]).into_inner());
    let mut ti_ch2 = CursorHandle::from_owned(&[ti2], in_schema);

    let delta2 = make_batch_u64pk_i64grp_u64val(&in_schema, &[(2, 1, 7, u64::MAX)]);

    let (out2, _) = op_reduce(
        &delta2,
        Some(ti_ch2.cursor_mut()),
        to_ch2.cursor_mut(),
        &in_schema,
        &out_schema,
        &[1u32],
        &[agg],
        None,
        false,
        TypeCode::U64,
        None,
        0,
        None,
        None,
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
        b.sorted = true;
        b.consolidated = true;
        ConsolidatedBatch::new_unchecked(b)
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
    use crate::storage::CursorHandle;
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
    let empty_ti = Rc::new(Batch::empty(in_schema.num_payload_cols(), 16));
    let mut ti_ch = CursorHandle::from_owned(&[empty_ti], in_schema);
    let empty_to = Rc::new(Batch::empty(out_schema.num_payload_cols(), 16));
    let mut to_ch = CursorHandle::from_owned(&[empty_to], out_schema);

    let delta1 = make_batch_u64pk_i64grp_u64val(&in_schema, &[(1, 1, 7, u64::MAX)]);

    let (out1, _) = op_reduce(
        &delta1,
        Some(ti_ch.cursor_mut()),
        to_ch.cursor_mut(),
        &in_schema,
        &out_schema,
        &[1u32],
        &[agg],
        None,
        false,
        TypeCode::U64,
        None,
        0,
        None,
        None,
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
    let ti2 = Rc::new(delta1.into_inner());
    let mut ti_ch2 = CursorHandle::from_owned(&[ti2], in_schema);
    let prev_out = Rc::new(out1);
    let mut to_ch2 = CursorHandle::from_owned(&[prev_out], out_schema);

    let delta2 = make_batch_u64pk_i64grp_u64val(&in_schema, &[(2, 1, 7, 5u64)]);

    let (out2, _) = op_reduce(
        &delta2,
        Some(ti_ch2.cursor_mut()),
        to_ch2.cursor_mut(),
        &in_schema,
        &out_schema,
        &[1u32],
        &[agg],
        None,
        false,
        TypeCode::U64,
        None,
        0,
        None,
        None,
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
    use crate::storage::CursorHandle;
    use std::rc::Rc;

    // Guard that the TypeCode::U64 branch does not leak into I64 paths:
    // MIN of {i64::MIN, -1, 0, i64::MAX} = i64::MIN,
    // MAX = i64::MAX.
    let in_schema = make_schema_u64pk_i64grp_i64val();
    let out_schema = make_out_schema_grp_i64agg();

    // MIN test.
    {
        let empty_out = Rc::new(Batch::empty(out_schema.num_payload_cols(), 16));
        let mut to_ch = CursorHandle::from_owned(&[empty_out], out_schema);

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
            to_ch.cursor_mut(),
            &in_schema,
            &out_schema,
            &[1u32],
            &[agg],
            None,
            false,
            TypeCode::U64,
            None,
            0,
            None,
            None,
        );
        assert_eq!(out.count, 1);
        let min = read_i64_le(out.col_data(1), 0);
        assert_eq!(min, i64::MIN, "MIN(I64) signed ordering preserved");
    }

    // MAX test.
    {
        let empty_out = Rc::new(Batch::empty(out_schema.num_payload_cols(), 16));
        let mut to_ch = CursorHandle::from_owned(&[empty_out], out_schema);

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
            to_ch.cursor_mut(),
            &in_schema,
            &out_schema,
            &[1u32],
            &[agg],
            None,
            false,
            TypeCode::U64,
            None,
            0,
            None,
            None,
        );
        assert_eq!(out.count, 1);
        let max = read_i64_le(out.col_data(1), 0);
        assert_eq!(max, i64::MAX, "MAX(I64) signed ordering preserved");
    }
}

#[test]
fn test_gather_reduce_min_u64() {
    use crate::storage::CursorHandle;
    use std::rc::Rc;

    // Schema for op_gather_reduce: U128 pk + U64 min_val (no group cols).
    let schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::U64, 1),
        ],
        &[0],
    );

    // Tick 1: two partial MINs from different workers, both for the same
    // group pk=1. One has a small value (3), one is high-bit-set
    // (1u64<<63). Unsigned MIN across both = 3.
    // Pre-fix signed comparator treats 1u64<<63 as i64::MIN < 3, so it
    // would report 1u64<<63 as the merged MIN.
    let empty_out = Rc::new(Batch::empty(1, 16));
    let mut to_ch = CursorHandle::from_owned(&[empty_out], schema);

    let mut partial1 = Batch::with_schema(schema, 2);

    partial1.extend_pk(1u128);
    partial1.extend_weight(&1i64.to_le_bytes());
    partial1.extend_null_bmp(&0u64.to_le_bytes());
    partial1.extend_col(0, &3u64.to_le_bytes());
    partial1.count += 1;

    partial1.extend_pk(1u128);
    partial1.extend_weight(&1i64.to_le_bytes());
    partial1.extend_null_bmp(&0u64.to_le_bytes());
    partial1.extend_col(0, &(1u64 << 63).to_le_bytes());
    partial1.count += 1;

    let agg = AggDescriptor {
        col_idx: 1,
        agg_op: AggOp::Min,
        col_type_code: TypeCode::U64,
        _pad: [0; 2],
    };

    let out1 = op_gather_reduce(&partial1, to_ch.cursor_mut(), &schema, &[agg]);
    assert_eq!(out1.count, 1);
    let min1 = u64::from_le_bytes(out1.col_data(0)[0..8].try_into().unwrap());
    assert_eq!(min1, 3u64, "gather-reduce MIN across partials must be unsigned");

    // Tick 2: old global = 3 (from tick 1). New partial has u64::MAX.
    // op_gather_reduce folds the old global in via merge_accumulated →
    // combine, then merges the new partial via combine. Under unsigned
    // ordering, MIN stays at 3 (3 < u64::MAX). Under signed comparison,
    // u64::MAX → -1 wins, MIN flips to u64::MAX.
    //
    // op_gather_reduce always emits retract+new when has_old (no
    // skip-if-equal), so 2 rows are emitted; we check the new value.
    let prev_out = Rc::new(out1);
    let mut to_ch2 = CursorHandle::from_owned(&[prev_out], schema);

    let mut partial2 = Batch::with_schema(schema, 1);

    partial2.extend_pk(1u128);
    partial2.extend_weight(&1i64.to_le_bytes());
    partial2.extend_null_bmp(&0u64.to_le_bytes());
    partial2.extend_col(0, &u64::MAX.to_le_bytes());
    partial2.count += 1;

    let out2 = op_gather_reduce(&partial2, to_ch2.cursor_mut(), &schema, &[agg]);
    assert_eq!(out2.count, 2, "retract old + emit new (unchanged) MIN");
    let retracted = u64::from_le_bytes(out2.col_data(0)[0..8].try_into().unwrap());
    assert_eq!(retracted, 3u64);
    assert_eq!(out2.get_weight(0), -1);
    let new_min = u64::from_le_bytes(out2.col_data(0)[8..16].try_into().unwrap());
    assert_eq!(
        new_min, 3u64,
        "fold-old + combine-new under unsigned ordering keeps MIN at 3"
    );
    assert_eq!(out2.get_weight(1), 1);
}

// -----------------------------------------------------------------------
// group_by_pk fast path on unsorted input
//
// op_reduce's group_by_pk fast path walks rows in physical order and
// treats consecutive same-PK rows as one group. That assumption only
// holds when `working.sorted` is true; an unsorted delta (e.g. from
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
    b.sorted = false;
    b.consolidated = false;
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
    use crate::storage::CursorHandle;
    use std::rc::Rc;

    let in_schema = make_schema_u64_i64();
    let out_schema = make_pk_sum_count_out_schema();
    let empty_out = Rc::new(Batch::empty(out_schema.num_payload_cols(), 16));
    let mut to_ch = CursorHandle::from_owned(&[empty_out], out_schema);

    // Unsorted: pk=5 appears twice, separated by pk=3. The fast path
    // pre-fix walked physical order and split into 3 groups → emitting
    // two distinct pk=5 rows.
    let delta = make_batch_raw_pk(&in_schema, &[(5, 1, 10), (3, 1, 20), (5, 1, 30)], |pk: u64| pk as u128);

    let aggs = sum_count_aggs(1, TypeCode::I64);

    let (out, _) = op_reduce(
        &delta,
        None,
        to_ch.cursor_mut(),
        &in_schema,
        &out_schema,
        &[0u32],
        &aggs,
        None,
        false,
        TypeCode::U64,
        None,
        0,
        None,
        None,
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
    assert!(out.sorted, "output is PK-sorted by the fast path");
    assert!(out.consolidated, "output is consolidated by the fast path");
}

#[test]
fn test_reduce_group_by_pk_unsorted_input_count() {
    use crate::storage::CursorHandle;
    use std::rc::Rc;

    let in_schema = make_schema_u64_i64();
    let out_schema = make_pk_sum_out_schema(); // pk + I64 agg
    let empty_out = Rc::new(Batch::empty(out_schema.num_payload_cols(), 16));
    let mut to_ch = CursorHandle::from_owned(&[empty_out], out_schema);

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
        to_ch.cursor_mut(),
        &in_schema,
        &out_schema,
        &[0u32],
        &[agg],
        None,
        false,
        TypeCode::U64,
        None,
        0,
        None,
        None,
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
    use crate::storage::CursorHandle;
    use std::rc::Rc;

    let in_schema = make_schema_u64_i64();
    let out_schema = make_pk_sum_count_out_schema();
    let empty_out = Rc::new(Batch::empty(out_schema.num_payload_cols(), 16));
    let mut to_ch = CursorHandle::from_owned(&[empty_out], out_schema);

    // Same data as the unsorted-sum test but pre-sorted. The
    // `working.sorted` branch must produce identical output.
    let mut delta = make_batch_raw_pk(&in_schema, &[(3, 1, 20), (5, 1, 10), (5, 1, 30)], |pk: u64| pk as u128);
    delta.sorted = true;

    let aggs = sum_count_aggs(1, TypeCode::I64);

    let (out, _) = op_reduce(
        &delta,
        None,
        to_ch.cursor_mut(),
        &in_schema,
        &out_schema,
        &[0u32],
        &aggs,
        None,
        false,
        TypeCode::U64,
        None,
        0,
        None,
        None,
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
    use crate::storage::CursorHandle;
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
    let empty_out = Rc::new(Batch::empty(out_schema.num_payload_cols(), 16));
    let mut to_ch = CursorHandle::from_owned(&[empty_out], out_schema);

    // Unsorted compound-PK delta: physical order is (2,1) then (1,2).
    // Canonical pk_indices order should emit (1,2) first.
    let mut delta = make_batch_compound_2xu64(&in_schema, &[(2, 1, 1, 20), (1, 2, 1, 10)]);
    delta.sorted = false;
    delta.consolidated = false;

    let agg = AggDescriptor {
        col_idx: 2,
        agg_op: AggOp::Count,
        col_type_code: TypeCode::I64,
        _pad: [0; 2],
    };

    // Permuted GROUP BY: [1, 0]. group_set_eq_pk still holds.
    let (out, _) = op_reduce(
        &delta,
        None,
        to_ch.cursor_mut(),
        &in_schema,
        &out_schema,
        &[1u32, 0u32],
        &[agg],
        None,
        false,
        TypeCode::U64,
        None,
        0,
        None,
        None,
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
    use crate::storage::CursorHandle;
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
    let empty_out = Rc::new(Batch::empty(out_schema.num_payload_cols(), 16));
    let mut to_ch = CursorHandle::from_owned(&[empty_out], out_schema);

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
        to_ch.cursor_mut(),
        &in_schema,
        &out_schema,
        &[0u32],
        &aggs,
        None,
        false,
        TypeCode::I64,
        None,
        0,
        None,
        None,
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
    use crate::storage::CursorHandle;
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
    prev.sorted = true;
    prev.consolidated = true;
    let prev_rc = Rc::new(prev);
    let mut to_ch = CursorHandle::from_owned(&[prev_rc], out_schema);

    // Unsorted delta with pk=5 split across the batch. Pre-fix: emits
    // TWO `(pk=5, w=-1, SUM=100)` retractions plus split partials.
    let delta = make_batch_raw_pk(&in_schema, &[(5, 1, 10), (3, 1, 20), (5, 1, 30)], |pk: u64| pk as u128);

    let aggs = sum_count_aggs(1, TypeCode::I64);

    let (out, _) = op_reduce(
        &delta,
        None,
        to_ch.cursor_mut(),
        &in_schema,
        &out_schema,
        &[0u32],
        &aggs,
        None,
        false,
        TypeCode::U64,
        None,
        0,
        None,
        None,
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
    use crate::storage::CursorHandle;
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
    let ti = make_batch(&in_schema, &[(1, 1, 10), (1, 1, 20)]).into_inner();
    let mut ti_ch = CursorHandle::from_owned(&[Rc::new(ti)], in_schema);

    // trace_out: old MIN(pk=1) = 10.
    let mut prev = Batch::with_schema(out_schema, 1);
    prev.extend_pk(1u128);
    prev.extend_weight(&1i64.to_le_bytes());
    prev.extend_null_bmp(&0u64.to_le_bytes());
    prev.extend_col(0, &10i64.to_le_bytes());
    prev.count += 1;
    prev.sorted = true;
    prev.consolidated = true;
    let mut to_ch = CursorHandle::from_owned(&[Rc::new(prev)], out_schema);

    // Delta: retract (pk=1, val=10) — the payload holding the current MIN.
    let delta = make_batch(&in_schema, &[(1, -1, 10)]).into_inner();

    let agg = AggDescriptor {
        col_idx: 1,
        agg_op: AggOp::Min,
        col_type_code: TypeCode::I64,
        _pad: [0; 2],
    };

    let (out, _) = op_reduce(
        &delta,
        Some(ti_ch.cursor_mut()),
        to_ch.cursor_mut(),
        &in_schema,
        &out_schema,
        &[0u32], // GROUP BY PK col 0 → group_by_pk replay path
        &[agg],
        None,
        false,
        TypeCode::U64,
        None,
        0,
        None,
        None,
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
// sort_owned / op_gather_reduce canonical PK order
//
// sort_owned (used by op_gather_reduce) sorts indices by `pks[a].cmp(...)`
// on a u128 widen, which violates canonical order for signed/float
// single-col PKs and for compound PKs (where pk_indices priority is
// reversed by u128 LE byte layout).
// -----------------------------------------------------------------------

#[test]
fn test_sort_owned_signed_pk_canonical_order() {
    let schema = make_schema_i64pk_i64();
    let batch = make_batch_raw_pk(&schema, &[(-1, 1, 10), (2, 1, 20), (-3, 1, 30)], |pk: i64| {
        (pk as u64) as u128
    });
    let sorted = sort_owned(&batch, &schema);

    assert!(sorted.sorted, "sort_owned must set the sorted flag");
    assert_eq!(sorted.count, 3);
    let pks: Vec<i64> = (0..sorted.count).map(|i| opk_pk_i64(sorted.get_pk_bytes(i))).collect();
    assert_eq!(
        pks,
        vec![-3, -1, 2],
        "signed PK rows must come out in signed-ascending order"
    );
}

#[test]
fn test_sort_owned_compound_pk_canonical_order() {
    let schema = make_compound_pk_2xu64_schema();
    let mut batch = make_batch_compound_2xu64(&schema, &[(2, 1, 1, 20), (1, 2, 1, 10)]);
    batch.sorted = false;
    let sorted = sort_owned(&batch, &schema);

    assert!(sorted.sorted);
    assert_eq!(sorted.count, 2);
    let p0 = sorted.get_pk_bytes(0);
    let p1 = sorted.get_pk_bytes(1);
    // Compound PK columns are OPK (big-endian) at rest.
    let p0_c0 = u64::from_be_bytes(p0[0..8].try_into().unwrap());
    let p0_c1 = u64::from_be_bytes(p0[8..16].try_into().unwrap());
    let p1_c0 = u64::from_be_bytes(p1[0..8].try_into().unwrap());
    let p1_c1 = u64::from_be_bytes(p1[8..16].try_into().unwrap());
    assert_eq!(
        (p0_c0, p0_c1),
        (1, 2),
        "compound-PK canonical sort follows pk_indices order, not u128 LE byte order"
    );
    assert_eq!((p1_c0, p1_c1), (2, 1));
}

#[test]
fn test_gather_reduce_signed_pk_output_sorted_flag() {
    use crate::storage::CursorHandle;
    use std::rc::Rc;

    // op_gather_reduce's partial schema = output schema. Use a signed PK
    // so the sort_owned path inside must route through the order-preserving key.
    let schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::I64, 1),
        ],
        &[0],
    );
    let empty_out = Rc::new(Batch::empty(schema.num_payload_cols(), 16));
    let mut to_ch = CursorHandle::from_owned(&[empty_out], schema);

    // Unsorted partial-reduce input with negative PKs interleaved.
    let mut partial = Batch::with_schema(schema, 3);
    for &(pk, sum) in &[(-1i64, 10i64), (2i64, 20i64), (-3i64, 30i64)] {
        partial.extend_pk_opk(&schema, &[(pk as u64) as u128]);
        partial.extend_weight(&1i64.to_le_bytes());
        partial.extend_null_bmp(&0u64.to_le_bytes());
        partial.extend_col(0, &sum.to_le_bytes());
        partial.count += 1;
    }
    partial.sorted = false;
    partial.consolidated = false;

    let agg = AggDescriptor {
        col_idx: 1,
        agg_op: AggOp::Sum,
        col_type_code: TypeCode::I64,
        _pad: [0; 2],
    };

    let out = op_gather_reduce(&partial, to_ch.cursor_mut(), &schema, &[agg]);

    assert_eq!(out.count, 3);
    let pks: Vec<i64> = (0..out.count).map(|i| opk_pk_i64(out.get_pk_bytes(i))).collect();
    assert_eq!(
        pks,
        vec![-3, -1, 2],
        "gather-reduce output must be in canonical signed-PK order for output.sorted=true to be truthful"
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
    use crate::storage::CursorHandle;
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
        b.sorted = true;
        b.consolidated = true;
        ConsolidatedBatch::new_unchecked(b)
    };

    // AVI: key = a(4) ++ b(4) ++ av_encoded(8). Group (1,1) min=10, (2,2) min=20.
    let avi_schema = crate::ops::index::make_avi_schema(&in_schema, &[1u32, 2u32]);
    assert_eq!(avi_schema.pk_stride(), 16, "4 + 4 + 8");
    let avi_batch = {
        let mut b = Batch::with_schema(avi_schema, 2);
        for (a, bb, min) in [(1u32, 1u32, 10i64), (2, 2, 20)] {
            let mut key = [0u8; 16];
            key[0..4].copy_from_slice(&a.to_le_bytes());
            key[4..8].copy_from_slice(&bb.to_le_bytes());
            let av = encode_ordered(&min.to_le_bytes(), type_code::I64, false);
            key[8..16].copy_from_slice(&av.to_be_bytes());
            b.extend_pk_bytes(&key);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.count += 1;
        }
        b
    };

    let empty_out = Rc::new(Batch::empty(out_schema.num_payload_cols(), 16));
    let mut to_ch = CursorHandle::from_owned(&[empty_out], out_schema);
    let mut avi_ch = CursorHandle::from_owned(&[Rc::new(avi_batch)], avi_schema);

    let agg = AggDescriptor {
        col_idx: 3,
        agg_op: AggOp::Min,
        col_type_code: TypeCode::I64,
        _pad: [0; 2],
    };

    let (out, _) = op_reduce(
        &delta,
        None,
        to_ch.cursor_mut(),
        &in_schema,
        &out_schema,
        &[1u32, 2u32],
        &[agg],
        Some(avi_ch.cursor_mut()),
        false,
        TypeCode::I64,
        None,
        0,
        None,
        None,
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
    use crate::storage::CursorHandle;
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
        b.sorted = true;
        b.consolidated = true;
        ConsolidatedBatch::new_unchecked(b)
    };
    let group_key = extract_group_key(&delta.as_mem_batch(), 0, &in_schema, &[1u32]);

    // AVI (post-state for group a=1): the retracted 5 is gone; the surviving
    // values are {10, 20}, so the prefix walk must return the smaller, 10.
    let avi_schema = crate::ops::index::make_avi_schema(&in_schema, &[1u32]);
    let avi_batch = {
        let mut b = Batch::with_schema(avi_schema, 2);
        for min in [10i64, 20] {
            let mut key = [0u8; 12];
            key[0..4].copy_from_slice(&1u32.to_le_bytes());
            let av = encode_ordered(&min.to_le_bytes(), type_code::I64, false);
            key[4..12].copy_from_slice(&av.to_be_bytes());
            b.extend_pk_bytes(&key);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.count += 1;
        }
        b.sorted = true;
        b.consolidated = true;
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
        b.sorted = true;
        b.consolidated = true;
        Rc::new(b)
    };

    let mut to_ch = CursorHandle::from_owned(&[to_batch], out_schema);
    let mut avi_ch = CursorHandle::from_owned(&[Rc::new(avi_batch)], avi_schema);

    let agg = AggDescriptor {
        col_idx: 2,
        agg_op: AggOp::Min,
        col_type_code: TypeCode::I64,
        _pad: [0; 2],
    };

    let (out, _) = op_reduce(
        &delta,
        None,
        to_ch.cursor_mut(),
        &in_schema,
        &out_schema,
        &[1u32],
        &[agg],
        Some(avi_ch.cursor_mut()),
        false,
        TypeCode::I64,
        None,
        0,
        None,
        None,
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
    use crate::storage::CursorHandle;
    use std::rc::Rc;

    for (gtc, gsize, stride) in [(type_code::U16, 2usize, 10usize), (type_code::U32, 4, 12)] {
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
            b.sorted = true;
            b.consolidated = true;
            ConsolidatedBatch::new_unchecked(b)
        };

        let avi_schema = crate::ops::index::make_avi_schema(&in_schema, &[1u32]);
        assert_eq!(avi_schema.pk_stride() as usize, stride);
        let avi_batch = {
            let mut b = Batch::with_schema(avi_schema, 1);
            let mut key = vec![0u8; stride];
            key[..gsize].copy_from_slice(&gval.to_le_bytes()[..gsize]);
            let av = encode_ordered(&42i64.to_le_bytes(), type_code::I64, false);
            key[gsize..gsize + 8].copy_from_slice(&av.to_be_bytes());
            b.extend_pk_bytes(&key);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.count += 1;
            b
        };

        let empty_out = Rc::new(Batch::empty(out_schema.num_payload_cols(), 16));
        let mut to_ch = CursorHandle::from_owned(&[empty_out], out_schema);
        let mut avi_ch = CursorHandle::from_owned(&[Rc::new(avi_batch)], avi_schema);

        let agg = AggDescriptor {
            col_idx: 2,
            agg_op: AggOp::Min,
            col_type_code: TypeCode::I64,
            _pad: [0; 2],
        };

        let (out, _) = op_reduce(
            &delta,
            None,
            to_ch.cursor_mut(),
            &in_schema,
            &out_schema,
            &[1u32],
            &[agg],
            Some(avi_ch.cursor_mut()),
            false,
            TypeCode::I64,
            None,
            0,
            None,
            None,
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
    use crate::storage::CursorHandle;
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
        b.sorted = true;
        b.consolidated = true;
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
        b.sorted = true;
        b.consolidated = true;
        Rc::new(b)
    };

    // delta: retract the current MIN (pk=1, val=5).
    let delta = {
        let mut b = Batch::with_schema(in_schema, 1);
        mk_row(&mut b, 1, -1, 1, 5);
        b.sorted = true;
        b.consolidated = true;
        ConsolidatedBatch::new_unchecked(b)
    };

    let mut ti_ch = CursorHandle::from_owned(&[ti_batch], in_schema);
    let mut to_ch = CursorHandle::from_owned(&[to_batch], out_schema);

    let agg = AggDescriptor {
        col_idx: 2,
        agg_op: AggOp::Min,
        col_type_code: TypeCode::I64,
        _pad: [0; 2],
    };

    let (out, _) = op_reduce(
        &delta,
        Some(ti_ch.cursor_mut()),
        to_ch.cursor_mut(),
        &in_schema,
        &out_schema,
        &[1u32],
        &[agg],
        None,
        false,
        TypeCode::I64,
        None,
        0,
        None,
        None,
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
    use crate::storage::CursorHandle;
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
        b.sorted = true;
        b.consolidated = true;
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
        b.sorted = true;
        b.consolidated = true;
        Rc::new(b)
    };

    // delta: retract one of the two val=5 rows (pk=1).
    let delta = {
        let mut b = Batch::with_schema(in_schema, 1);
        mk_row(&mut b, 1, -1, 1, 5);
        b.sorted = true;
        b.consolidated = true;
        ConsolidatedBatch::new_unchecked(b)
    };

    let mut ti_ch = CursorHandle::from_owned(&[ti_batch], in_schema);
    let mut to_ch = CursorHandle::from_owned(&[to_batch], out_schema);

    let agg = AggDescriptor {
        col_idx: 2,
        agg_op: AggOp::Min,
        col_type_code: TypeCode::I64,
        _pad: [0; 2],
    };

    let (out, _) = op_reduce(
        &delta,
        Some(ti_ch.cursor_mut()),
        to_ch.cursor_mut(),
        &in_schema,
        &out_schema,
        &[1u32],
        &[agg],
        None,
        false,
        TypeCode::I64,
        None,
        0,
        None,
        None,
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
    use crate::storage::CursorHandle;
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
        b.sorted = true;
        b.consolidated = true;
        ConsolidatedBatch::new_unchecked(b)
    };

    let empty_out = Rc::new(Batch::empty(out_schema.num_payload_cols(), 16));
    let mut to_ch = CursorHandle::from_owned(&[empty_out], out_schema);

    let agg = AggDescriptor {
        col_idx: 2,
        agg_op: AggOp::Min,
        col_type_code: TypeCode::I64,
        _pad: [0; 2],
    };

    let (out, _) = op_reduce(
        &delta,
        None,
        to_ch.cursor_mut(),
        &in_schema,
        &out_schema,
        &[1u32],
        &[agg],
        None,
        false,
        TypeCode::I64,
        None,
        0,
        None,
        None,
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
    use crate::storage::CursorHandle;
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
        b.sorted = true;
        b.consolidated = true;
        ConsolidatedBatch::new_unchecked(b)
    };
    let group_key = extract_group_key(&delta.as_mem_batch(), 0, &in_schema, &[1u32, 2u32]);

    // AVI post-state for (3, 4): surviving min is 9. A decoy entry for a
    // different group (3, 5) sharing the a-byte prefix must NOT be matched.
    let avi_schema = crate::ops::index::make_avi_schema(&in_schema, &[1u32, 2u32]);
    let avi_batch = {
        let mut b = Batch::with_schema(avi_schema, 2);
        let put = |b: &mut Batch, a: u32, bb: u32, min: i64| {
            let mut key = [0u8; 16];
            key[0..4].copy_from_slice(&a.to_le_bytes());
            key[4..8].copy_from_slice(&bb.to_le_bytes());
            let av = encode_ordered(&min.to_le_bytes(), type_code::I64, false);
            key[8..16].copy_from_slice(&av.to_be_bytes());
            b.extend_pk_bytes(&key);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.count += 1;
        };
        put(&mut b, 3, 4, 9);
        put(&mut b, 3, 5, 1); // decoy: same a, different b, smaller value
        b.sorted = true;
        b.consolidated = true;
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
        b.sorted = true;
        b.consolidated = true;
        Rc::new(b)
    };

    let mut to_ch = CursorHandle::from_owned(&[to_batch], out_schema);
    let mut avi_ch = CursorHandle::from_owned(&[Rc::new(avi_batch)], avi_schema);

    let agg = AggDescriptor {
        col_idx: 3,
        agg_op: AggOp::Min,
        col_type_code: TypeCode::I64,
        _pad: [0; 2],
    };

    let (out, _) = op_reduce(
        &delta,
        None,
        to_ch.cursor_mut(),
        &in_schema,
        &out_schema,
        &[1u32, 2u32],
        &[agg],
        Some(avi_ch.cursor_mut()),
        false,
        TypeCode::I64,
        None,
        0,
        None,
        None,
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
    use crate::storage::CursorHandle;
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
        24,
        "wide composite: 8 + 8 + 8",
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
        let mut keys: Vec<[u8; 24]> = reference
            .iter()
            .map(|(&(a, b), &m)| {
                let mut key = [0u8; 24];
                key[0..8].copy_from_slice(&a.to_le_bytes());
                key[8..16].copy_from_slice(&b.to_le_bytes());
                let av = encode_ordered(&m.to_le_bytes(), type_code::I64, false);
                key[16..24].copy_from_slice(&av.to_be_bytes());
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
        bt.sorted = true;
        bt.consolidated = true;
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

    let empty_out = Rc::new(Batch::empty(out_schema.num_payload_cols(), 16));
    let mut to_ch = CursorHandle::from_owned(&[empty_out], out_schema);
    let mut avi_ch = CursorHandle::from_owned(&[Rc::new(avi_batch)], avi_schema);

    let agg = AggDescriptor {
        col_idx: 3,
        agg_op: AggOp::Min,
        col_type_code: TypeCode::I64,
        _pad: [0; 2],
    };

    let (out, _) = op_reduce(
        &delta,
        None,
        to_ch.cursor_mut(),
        &in_schema,
        &out_schema,
        &[1u32, 2u32],
        &[agg],
        Some(avi_ch.cursor_mut()),
        false,
        TypeCode::I64,
        None,
        0,
        None,
        None,
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
    use crate::storage::CursorHandle;
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
    assert_eq!(avi_schema.pk_stride(), 24, "16 + 8");

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
            let mut key = [0u8; 24];
            key[0..16].copy_from_slice(&g.to_le_bytes());
            let av = encode_ordered(&m.to_le_bytes(), type_code::I64, false);
            key[16..24].copy_from_slice(&av.to_be_bytes());
            b.extend_pk_bytes(&key);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.count += 1;
        }
        b.sorted = true;
        b.consolidated = true;
        b
    };

    let empty_out = Rc::new(Batch::empty(out_schema.num_payload_cols(), 16));
    let mut to_ch = CursorHandle::from_owned(&[empty_out], out_schema);
    let mut avi_ch = CursorHandle::from_owned(&[Rc::new(avi_batch)], avi_schema);

    let agg = AggDescriptor {
        col_idx: 2,
        agg_op: AggOp::Min,
        col_type_code: TypeCode::I64,
        _pad: [0; 2],
    };

    let (out, _) = op_reduce(
        &delta,
        None,
        to_ch.cursor_mut(),
        &in_schema,
        &out_schema,
        &[1u32],
        &[agg],
        Some(avi_ch.cursor_mut()),
        false,
        TypeCode::I64,
        None,
        0,
        None,
        None,
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
    use crate::storage::CursorHandle;
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
    assert_eq!(avi_schema.pk_stride(), 24);

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
        let mut keys: Vec<[u8; 24]> = groups
            .iter()
            .map(|&(a, bb, m)| {
                let mut key = [0u8; 24];
                key[0..8].copy_from_slice(&a.to_le_bytes());
                key[8..16].copy_from_slice(&bb.to_le_bytes());
                let av = encode_ordered(&m.to_le_bytes(), type_code::I64, false);
                key[16..24].copy_from_slice(&av.to_be_bytes());
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
        b.sorted = true;
        b.consolidated = true;
        b
    };

    let empty_out = Rc::new(Batch::empty(out_schema.num_payload_cols(), 16));
    let mut to_ch = CursorHandle::from_owned(&[empty_out], out_schema);
    let mut avi_ch = CursorHandle::from_owned(&[Rc::new(avi_batch)], avi_schema);

    let agg = AggDescriptor {
        col_idx: 3,
        agg_op: AggOp::Min,
        col_type_code: TypeCode::I64,
        _pad: [0; 2],
    };

    let (out, _) = op_reduce(
        &delta,
        None,
        to_ch.cursor_mut(),
        &in_schema,
        &out_schema,
        &[1u32, 2u32],
        &[agg],
        Some(avi_ch.cursor_mut()),
        false,
        TypeCode::I64,
        None,
        0,
        None,
        None,
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
    use crate::storage::CursorHandle;
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
    assert_eq!(avi_schema.pk_stride(), 32, "8 + 8 + 8 + 8");

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
            let mut key = [0u8; 32];
            key[0..8].copy_from_slice(&a.to_le_bytes());
            key[8..16].copy_from_slice(&bb.to_le_bytes());
            key[16..24].copy_from_slice(&c.to_le_bytes());
            let av = encode_ordered(&m.to_le_bytes(), type_code::I64, false);
            key[24..32].copy_from_slice(&av.to_be_bytes());
            b.extend_pk_bytes(&key);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.count += 1;
        }
        b.sorted = true;
        b.consolidated = true;
        b
    };

    let empty_out = Rc::new(Batch::empty(out_schema.num_payload_cols(), 16));
    let mut to_ch = CursorHandle::from_owned(&[empty_out], out_schema);
    let mut avi_ch = CursorHandle::from_owned(&[Rc::new(avi_batch)], avi_schema);

    let agg = AggDescriptor {
        col_idx: 4,
        agg_op: AggOp::Min,
        col_type_code: TypeCode::I64,
        _pad: [0; 2],
    };

    let (out, _) = op_reduce(
        &delta,
        None,
        to_ch.cursor_mut(),
        &in_schema,
        &out_schema,
        &[1u32, 2u32, 3u32],
        &[agg],
        Some(avi_ch.cursor_mut()),
        false,
        TypeCode::I64,
        None,
        0,
        None,
        None,
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
    use crate::storage::CursorHandle;
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
    assert_eq!(avi_schema.pk_stride(), 24);

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
        b.sorted = true;
        b.consolidated = true;
        ConsolidatedBatch::new_unchecked(b)
    };
    let group_key = extract_group_key(&delta.as_mem_batch(), 0, &in_schema, &[1u32, 2u32]);

    // AVI post-state: group (ga, gb) surviving values {9, 15}; a decoy group
    // (ga, gb+1) sharing the first 8 bytes holds a smaller 1 that must not win.
    let avi_batch = {
        let mut b = Batch::with_schema(avi_schema, 3);
        let put = |b: &mut Batch, a: u64, bb: u64, m: i64| {
            let mut key = [0u8; 24];
            key[0..8].copy_from_slice(&a.to_le_bytes());
            key[8..16].copy_from_slice(&bb.to_le_bytes());
            let av = encode_ordered(&m.to_le_bytes(), type_code::I64, false);
            key[16..24].copy_from_slice(&av.to_be_bytes());
            b.extend_pk_bytes(&key);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.count += 1;
        };
        // sorted by (a, b, av): (ga,gb,9),(ga,gb,15),(ga,gb+1,1).
        put(&mut b, ga, gb, 9);
        put(&mut b, ga, gb, 15);
        put(&mut b, ga, gb + 1, 1);
        b.sorted = true;
        b.consolidated = true;
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
        b.sorted = true;
        b.consolidated = true;
        Rc::new(b)
    };

    let mut to_ch = CursorHandle::from_owned(&[to_batch], out_schema);
    let mut avi_ch = CursorHandle::from_owned(&[Rc::new(avi_batch)], avi_schema);

    let agg = AggDescriptor {
        col_idx: 3,
        agg_op: AggOp::Min,
        col_type_code: TypeCode::I64,
        _pad: [0; 2],
    };

    let (out, _) = op_reduce(
        &delta,
        None,
        to_ch.cursor_mut(),
        &in_schema,
        &out_schema,
        &[1u32, 2u32],
        &[agg],
        Some(avi_ch.cursor_mut()),
        false,
        TypeCode::I64,
        None,
        0,
        None,
        None,
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

// Bug 3: the order-encoded aggregate value must be serialized big-endian so the
// index's lexicographic byte ordering matches numeric order. With three values
// in one group whose extremes differ above the low byte ({101, 111, -5}), a
// little-endian serialization sorts 101 first and reports it as the MIN; -5 is
// never seen. Drives the full production path: op_integrate_with_indexes
// populates the AVI, then apply_agg_from_value_index reads it back.
#[test]
fn avi_full_path_min_max_across_high_byte() {
    use super::super::util::GroupKeyExtractor;
    use crate::ops::index::{make_avi_schema, op_integrate_with_indexes, AviDesc};
    use crate::storage::Table;

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

    let tmp = tempfile::tempdir().unwrap();
    let group_by = vec![1u32];

    let seed = |for_max: bool, table_id: u32| -> i64 {
        let avi_schema = make_avi_schema(&in_schema, &group_by);
        let mut avi_t = Table::new(
            tmp.path().to_str().unwrap(),
            "avi",
            avi_schema,
            table_id,
            1 << 20,
            crate::storage::Persistence::Ephemeral,
        )
        .unwrap();
        let avi = AviDesc {
            table: &mut avi_t as *mut Table,
            for_max,
            agg_col_type_code: type_code::I64,
            group_by_cols: group_by.clone(),
            agg_col_idx: 2,
        };
        op_integrate_with_indexes(&delta, None, &in_schema, None, Some(&avi)).unwrap();

        let mut ch = avi_t.open_cursor();
        let extractor = GroupKeyExtractor::new(&in_schema, &group_by);
        let mut gk = [0u8; crate::schema::MAX_PK_BYTES];
        extractor.gather(&delta.as_mem_batch(), 0, &mut gk);
        let agg_desc = AggDescriptor {
            col_idx: 2,
            agg_op: if for_max { AggOp::Max } else { AggOp::Min },
            col_type_code: TypeCode::I64,
            _pad: [0; 2],
        };
        let mut acc = Accumulator::new(&agg_desc, &in_schema);
        let ok = apply_agg_from_value_index(
            ch.cursor_mut(),
            &gk[..extractor.stride],
            for_max,
            TypeCode::I64,
            &mut acc,
        );
        assert!(ok, "AVI seek must find group g=5");
        acc.get_value_bits() as i64
    };

    assert_eq!(
        seed(false, 90),
        -5,
        "MIN across the high-byte boundary must be -5, not 101"
    );
    assert_eq!(seed(true, 91), 111, "MAX must be 111");
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
    use crate::storage::CursorHandle;
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

    // Distinct PKs: (1,1) twice → cnt 2, (1,2) once → cnt 1. Unsorted input so
    // the argsort_pk_canonical branch runs.
    let delta = {
        let mut b = Batch::with_schema(in_schema, 3);
        for (a, c) in [(1u128, 2u128), (1, 1), (1, 1)] {
            b.extend_pk_opk(&in_schema, &[a, c]);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.count += 1;
        }
        b
    };

    let to_batch = Rc::new(Batch::empty(out_schema.num_payload_cols(), 16));
    let mut to_ch = CursorHandle::from_owned(&[to_batch], out_schema);

    let agg = AggDescriptor {
        col_idx: 0,
        agg_op: AggOp::Count,
        col_type_code: TypeCode::U128,
        _pad: [0; 2],
    };

    let (out, _) = op_reduce(
        &delta,
        None,
        to_ch.cursor_mut(),
        &in_schema,
        &out_schema,
        &[0u32, 1u32],
        &[agg],
        None,
        false,
        TypeCode::U128,
        None,
        0,
        None,
        None,
    );

    // One row per distinct PK; output is sorted + consolidated.
    assert!(
        out.sorted && out.consolidated,
        "group-by-PK output must be sorted+consolidated"
    );
    let mut counts: Vec<i64> = (0..out.count)
        .filter(|&i| out.get_weight(i) > 0)
        .map(|i| read_i64_le(out.col_data(0), i * 8))
        .collect();
    counts.sort_unstable();
    assert_eq!(counts, vec![1, 2], "two distinct compound PKs with counts 1 and 2");
}

// -----------------------------------------------------------------------
// Non-linear REDUCE fallback: single trace scan (O(trace + delta)).
//
// The fallback path is reached when all hold: non-linear aggregate,
// no AVI, no GroupIndex, group_by is not the PK. The tests here pin
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
    use crate::storage::CursorHandle;
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
    let mut ch = CursorHandle::from_owned(&[Rc::clone(&rc)], *schema);
    let cursor = ch.cursor_mut();

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
        b.sorted = true;
        b.consolidated = true;
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
        b.sorted = true;
        b.consolidated = true;
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
        b.sorted = true;
        b.consolidated = true;
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
        b.sorted = true;
        b.consolidated = true;
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
        b.sorted = true;
        b.consolidated = true;
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
        b.sorted = true;
        b.consolidated = true;
        assert_group_key_cursor_matches_batch(&b, &schema, &[1u32, 2u32]);
    }
}

// Helper: build a sorted+consolidated Batch for the 3-col fallback correctness schemas.
// Schema: U64 pk | group_col (type/nullable per schema) | I64 val.
// Rows: (pk, weight, group_bytes [16 if string else 8], null_bit for group_col, val).
fn make_fallback_batch_i64_grp(
    schema: &SchemaDescriptor,
    rows: &[(u64, i64, i64, bool, i64)], // (pk, w, grp, grp_is_null, val)
) -> Batch {
    let pi_grp = schema.try_payload_idx(1).unwrap();
    let pi_val = schema.try_payload_idx(2).unwrap();
    let mut b = Batch::with_schema(*schema, rows.len().max(1));
    for &(pk, w, grp, grp_null, val) in rows {
        b.extend_pk(pk as u128);
        b.extend_weight(&w.to_le_bytes());
        let null_word = if grp_null { 1u64 << pi_grp } else { 0u64 };
        b.extend_null_bmp(&null_word.to_le_bytes());
        b.extend_col(pi_grp, &grp.to_le_bytes());
        b.extend_col(pi_val, &val.to_le_bytes());
        b.count += 1;
    }
    b.sorted = true;
    b.consolidated = true;
    b
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
    tick1_rows: &[(u64, i64, i64, bool, i64)], // (pk, w, grp, grp_null, val)
    delta_rows: &[(u64, i64, i64, bool, i64)],
    expected_tick1: &mut Vec<(i64, i64)>, // sorted (grp, min) pairs after tick 1
    expected_tick2: &mut Vec<(i64, i64)>, // sorted (grp, min) pairs after tick 2
) {
    use crate::storage::CursorHandle;
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
    let delta1 = make_fallback_batch_i64_grp(&in_schema, tick1_rows);
    let empty_out = Rc::new(Batch::empty(out_schema.num_payload_cols(), 16));
    let mut to_ch1 = CursorHandle::from_owned(&[empty_out], out_schema);

    let (out1, _) = op_reduce(
        &delta1,
        None,
        to_ch1.cursor_mut(),
        &in_schema,
        &out_schema,
        &[1u32],
        &[agg],
        None,
        false,
        TypeCode::U64,
        None,
        0,
        None,
        None,
    );

    let got1 = read_grp_min_pairs(&out1);
    expected_tick1.sort_unstable();
    assert_eq!(got1, *expected_tick1, "tick 1 MIN mismatch");

    // --- Tick 2: trace_in = tick1 input, trace_out = tick1 output ---
    let ti_batch = Rc::new(make_fallback_batch_i64_grp(&in_schema, tick1_rows));
    let to_batch = Rc::new(out1);
    let delta2 = make_fallback_batch_i64_grp(&in_schema, delta_rows);

    let mut ti_ch2 = CursorHandle::from_owned(&[ti_batch], in_schema);
    let mut to_ch2 = CursorHandle::from_owned(&[to_batch], out_schema);

    let (out2, _) = op_reduce(
        &delta2,
        Some(ti_ch2.cursor_mut()),
        to_ch2.cursor_mut(),
        &in_schema,
        &out_schema,
        &[1u32],
        &[agg],
        None,
        false,
        TypeCode::U64,
        None,
        0,
        None,
        None,
    );

    let grp_data = out2.col_data(0);
    let min_data = out2.col_data(1);

    // After tick 2, accumulate tick1 + delta. The net MIN per group is what
    // tick1 produced (already verified) plus the new delta's contribution.
    // We build expected from tick1 final values + delta applied.
    let mut final_rows: std::collections::BTreeMap<i64, Vec<i64>> = std::collections::BTreeMap::new();
    for &(_, w, grp, grp_null, val) in tick1_rows.iter().chain(delta_rows.iter()) {
        if grp_null {
            continue;
        } // NULL group: skip for simplicity (tested separately)
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
        (1u64, 1i64, 10i64, false, 100i64),
        (2, 1, 10, false, 50),
        (3, 1, 20, false, 30),
        (4, 1, 30, false, 200),
    ];
    // Delete the row with val=100 from group 10 → new MIN(10) = 50.
    let delta2 = vec![(1u64, -1i64, 10i64, false, 100i64)];
    let mut exp1 = vec![(10i64, 50i64), (20, 30), (30, 200)];
    let mut exp2 = vec![(10i64, 50i64), (20, 30), (30, 200)];
    run_fallback_min_i64_grp(&tick1, &delta2, &mut exp1, &mut exp2);
}

/// MIN grouped by nullable I64 — hash branch (>= HASH_THRESHOLD groups).
#[test]
fn fallback_min_nullable_i64_group_hash_path() {
    // 20 distinct groups (exceeds HASH_THRESHOLD=16).
    let tick1: Vec<(u64, i64, i64, bool, i64)> = (0..20)
        .flat_map(|g| {
            vec![
                ((g * 2) as u64, 1, g as i64, false, (g * 10 + 5) as i64),
                ((g * 2 + 1) as u64, 1, g as i64, false, (g * 10 + 1) as i64),
            ]
        })
        .collect();
    // MIN per group g = g*10+1. Delete the row with val=51 from group 5 → MIN(5) becomes 55.
    // pk=11 = g*2+1 for g=5 carries val=g*10+1=51.
    let delta2 = vec![(11u64, -1i64, 5i64, false, 51i64)]; // remove pk=11 (val=51)
    let mut exp1: Vec<(i64, i64)> = (0..20).map(|g| (g as i64, (g * 10 + 1) as i64)).collect();
    let mut exp2: Vec<(i64, i64)> = (0..20)
        .map(|g| (g as i64, if g == 5 { 55 } else { (g * 10 + 1) as i64 }))
        .collect();
    run_fallback_min_i64_grp(&tick1, &delta2, &mut exp1, &mut exp2);
}

/// MIN grouped by multi-column key (I64, I64) — no GI → fallback path.
#[test]
fn fallback_min_multi_col_group() {
    use crate::storage::CursorHandle;
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
        b.sorted = true;
        b.consolidated = true;
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
    let empty_out = Rc::new(Batch::empty(out_schema.num_payload_cols(), 16));
    let mut to_ch = CursorHandle::from_owned(&[empty_out], out_schema);

    let (out1, _) = op_reduce(
        &delta1,
        None,
        to_ch.cursor_mut(),
        &in_schema,
        &out_schema,
        &[1u32, 2u32],
        &[agg],
        None,
        false,
        TypeCode::U64,
        None,
        0,
        None,
        None,
    );
    // Verify 3 distinct groups came out.
    assert_eq!(out1.count, 3, "tick 1 must emit 3 groups");

    // Tick 2: add val=5 to group (1,1). trace_out is empty (no prior output to
    // retract); the fallback must still pull trace rows for group (1,1) from
    // trace_in so the replay is {10, 20, 5} and MIN = 5.
    let delta2_rows = [(5u64, 1, 1i64, 1i64, 5i64)];
    let ti_batch = Rc::new(make_batch(&tick1_rows));
    let empty_out2 = Rc::new(Batch::empty(out_schema.num_payload_cols(), 16));
    let delta2 = make_batch(&delta2_rows);

    let mut ti_ch = CursorHandle::from_owned(&[ti_batch], in_schema);
    let mut to_ch2 = CursorHandle::from_owned(&[empty_out2], out_schema);

    let (out2, _) = op_reduce(
        &delta2,
        Some(ti_ch.cursor_mut()),
        to_ch2.cursor_mut(),
        &in_schema,
        &out_schema,
        &[1u32, 2u32],
        &[agg],
        None,
        false,
        TypeCode::U64,
        None,
        0,
        None,
        None,
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
    use crate::storage::{CursorHandle, REWIND_CALLS};
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
    ti_b.sorted = true;
    ti_b.consolidated = true;

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
    delta_b.sorted = true;
    delta_b.consolidated = true;

    let ti_rc = Rc::new(ti_b);
    let empty_out = Rc::new(Batch::empty(out_schema.num_payload_cols(), 16));
    let mut ti_ch = CursorHandle::from_owned(&[ti_rc], in_schema);
    let mut to_ch = CursorHandle::from_owned(&[empty_out], out_schema);

    // Reset the per-thread rewind counter before the call.
    REWIND_CALLS.with(|c| c.set(0));

    let (out, _) = op_reduce(
        &delta_b,
        Some(ti_ch.cursor_mut()),
        to_ch.cursor_mut(),
        &in_schema,
        &out_schema,
        &[1u32],
        &[agg],
        None,
        false,
        TypeCode::U64,
        None,
        0,
        None,
        None,
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
    b.sorted = true;
    b.consolidated = true;
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
    use crate::storage::CursorHandle;
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
    let empty_out = Rc::new(Batch::empty(out_schema.num_payload_cols(), 16));
    let mut to_ch1 = CursorHandle::from_owned(&[empty_out], out_schema);
    let (out1, _) = op_reduce(
        &delta1,
        None,
        to_ch1.cursor_mut(),
        &in_schema,
        &out_schema,
        &[1u32],
        &[agg],
        None,
        false,
        TypeCode::U64,
        None,
        0,
        None,
        None,
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
    let mut ti_ch2 = CursorHandle::from_owned(&[ti_batch], in_schema);
    let mut to_ch2 = CursorHandle::from_owned(&[to_batch], out_schema);
    let (out2, _) = op_reduce(
        &delta2,
        Some(ti_ch2.cursor_mut()),
        to_ch2.cursor_mut(),
        &in_schema,
        &out_schema,
        &[1u32],
        &[agg],
        None,
        false,
        TypeCode::U64,
        None,
        0,
        None,
        None,
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

/// Gather-reduce must ignore a NULL partial when combining a group: a NULL MIN
/// partial injected as `combine(0)` would corrupt the global MIN (MIN(5,0)=0).
/// Latent today (op_gather_reduce is unwired in SQL planning) but a real value
/// bug for the future GatherReduce milestone.
#[test]
fn gather_combine_skips_null_partial() {
    use crate::storage::CursorHandle;
    use std::rc::Rc;

    // partial/output: pk(U128), min(I64 nullable). min is payload index 0.
    let schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::I64, 1),
        ],
        &[0],
    );
    let agg_min = AggDescriptor {
        col_idx: 1,
        agg_op: AggOp::Min,
        col_type_code: TypeCode::I64,
        _pad: [0; 2],
    };

    // Group pk=1: an all-NULL partial and a MIN=5 partial → global MIN = 5.
    let mut partial = Batch::with_schema(schema, 2);
    partial.extend_pk(1u128);
    partial.extend_weight(&1i64.to_le_bytes());
    partial.extend_null_bmp(&1u64.to_le_bytes()); // min (payload idx 0) NULL
    partial.extend_col(0, &0i64.to_le_bytes());
    partial.count += 1;
    partial.extend_pk(1u128);
    partial.extend_weight(&1i64.to_le_bytes());
    partial.extend_null_bmp(&0u64.to_le_bytes());
    partial.extend_col(0, &5i64.to_le_bytes());
    partial.count += 1;

    let empty_out = Rc::new(Batch::empty_with_schema(&schema));
    let mut to_ch = CursorHandle::from_owned(&[empty_out], schema);
    let out = op_gather_reduce(&partial, to_ch.cursor_mut(), &schema, &[agg_min]);
    assert_eq!(out.count, 1);
    assert_eq!(
        read_i64_le(out.col_data(0), 0),
        5,
        "gather MIN must ignore the all-NULL partial, not combine it as 0"
    );
}
