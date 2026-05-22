//! Reduce operator tests. Imports submodule items by name; helpers live in this
//! file rather than a shared `common.rs` so the tests file stays self-contained.

use crate::schema::{
    PAYLOAD_MAPPING_PK_SENTINEL, SchemaColumn, SchemaDescriptor, SHORT_STRING_THRESHOLD,
    TypeCode, type_code,
};
use crate::storage::{Batch, ConsolidatedBatch};

use super::super::util::{
    extract_gc_u64, extract_group_key, ieee_order_bits_f32, ieee_order_bits_f32_reverse,
};
use super::agg::{Accumulator, AggDescriptor, AggOp};
use super::emit::{emit_finalized_row, emit_reduce_row};
use super::op_gather::op_gather_reduce;
use super::op_reduce::{cursor_matches_group, op_reduce};
use super::sort::{argsort_delta, build_sort_descs, compare_by_group_cols, sort_owned};


fn make_schema_u64_i64() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    )
}

fn make_batch(
    schema: &SchemaDescriptor,
    rows: &[(u64, i64, i64)],
) -> ConsolidatedBatch {
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
/// All strings must be <= 12 bytes (inline, no blob needed).
fn make_batch_3col_grp_str(
    schema: &SchemaDescriptor,
    rows: &[(u64, i64, i64, &str)],
) -> ConsolidatedBatch {
    let n = rows.len();
    let mut b = Batch::with_schema(*schema, n.max(1));

    for &(pk, w, grp, val) in rows {
        b.extend_pk(pk as u128);
        b.extend_weight(&w.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &grp.to_le_bytes());
        let bytes = val.as_bytes();
        assert!(bytes.len() <= SHORT_STRING_THRESHOLD, "use inline strings only");
        let mut gs = [0u8; 16];
        gs[0..4].copy_from_slice(&(bytes.len() as u32).to_le_bytes());
        gs[4..4 + bytes.len()].copy_from_slice(bytes);
        b.extend_col(1, &gs);
        b.count += 1;
    }
    b.sorted = true;
    b.consolidated = true;
    ConsolidatedBatch::new_unchecked(b)
}

/// Build a GI Batch (U128 pk: ck_lo=source_pk_lo, ck_hi=gc_u64; I64 payload: spk_hi).
fn make_gi_batch(rows: &[(u64, u64, i64)]) -> ConsolidatedBatch {
    let gi_schema = crate::ops::index::make_gi_schema();
    let n = rows.len();
    let mut b = Batch::with_schema(gi_schema, n.max(1));

    for &(ck_lo, gc_u64, spk_hi) in rows {
        b.extend_pk(((gc_u64 as u128) << 64) | (ck_lo as u128));
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &spk_hi.to_le_bytes());
        b.count += 1;
    }
    b.sorted = true;
    b.consolidated = true;
    ConsolidatedBatch::new_unchecked(b)
}

#[test]
fn test_reduce_sum_retraction() {
    use std::rc::Rc;
    use crate::storage::CursorHandle;
    use crate::schema::{SchemaColumn, type_code};

    // Input: pk(U64), grp(I64), val(I64)
    let in_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    );

    // Output: pk(U128), grp(I64), sum(I64)
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::I64, 1),
        ],
        &[0],
    );

    // Empty trace_out
    let empty_out = Rc::new(Batch::empty(2, 16));
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

    let agg = AggDescriptor {
        col_idx: 2, agg_op: AggOp::Sum, col_type_code: TypeCode::I64, _pad: [0; 2],
    };

    let (out1, _) = op_reduce(
        &delta1, None, to_ch.cursor_mut(),
        &in_schema, &out_schema, &[1u32], &[agg],
        None, false, TypeCode::U64, &[], None, None, 0, None, None,
    );
    // SUM of (100+200+300) = 600
    assert_eq!(out1.count, 1);
    let sum1 = crate::util::read_i64_le(out1.col_data(1), 0);
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
        &delta2, None, to_ch2.cursor_mut(),
        &in_schema, &out_schema, &[1u32], &[agg],
        None, false, TypeCode::U64, &[], None, None, 0, None, None,
    );
    // Output: retract old sum (600, w=-1) + insert new sum (400, w=+1) = 2 rows
    assert_eq!(out2.count, 2);
}

#[test]
fn test_reduce_count() {
    use std::rc::Rc;
    use crate::storage::CursorHandle;
    use crate::schema::type_code;

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
        col_idx: 0, agg_op: AggOp::Count, col_type_code: TypeCode::I64, _pad: [0; 2],
    };

    // GROUP BY pk → each row is its own group
    let (out, _) = op_reduce(
        &delta, None, to_ch.cursor_mut(),
        &in_schema, &out_schema, &[0u32], &[agg],
        None, false, TypeCode::U64, &[], None, None, 0, None, None,
    );
    // Each pk forms its own group, COUNT=1 for each
    assert_eq!(out.count, 3);
    for i in 0..3 {
        let count = crate::util::read_i64_le(out.col_data(0), i * 8);
        assert_eq!(count, 1, "each single-row group has count=1");
    }
}

/// GI path bug: same PK, two different string payloads — the `if` must be `while`.
#[test]
fn test_reduce_gi_same_pk_multiple_payloads() {
    use std::rc::Rc;
    use crate::storage::CursorHandle;

    let input_schema = make_schema_3col_grp_str();
    let output_schema = make_reduce_str_out_schema();
    let gi_schema = crate::ops::index::make_gi_schema();

    // trace_in: apple and zebra both at PK=1 (apple sorts first by payload)
    let ti_batch = Rc::new(make_batch_3col_grp_str(
        &input_schema,
        &[(1, 1, 1, "apple"), (1, 1, 1, "zebra")],
    ).into_inner());

    // GI: only PK=1 → group gc_u64=1
    let gi_batch = Rc::new(make_gi_batch(&[(1, 1, 0)]).into_inner());

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
        &[1u32],            // group_by_cols: col 1 (grp)
        &[agg_desc],
        None,               // avi_cursor
        false,              // avi_for_max
        TypeCode::String,   // avi_agg_col_type_code (unused; no AVI)
        &[1u32],            // avi_group_by_cols (unused)
        None,               // avi_input_schema
        Some(gi_handle.cursor_mut()), // gi_cursor
        1u32,               // gi_col_idx: grp column
        None,               // finalize_prog
        None,               // finalize_out_schema
    );

    // The accumulator stores the first 8 bytes of the German string as i64.
    // "zebra" first 8 bytes: [len=5, 'z'=122, 'e'=101, 'b'=98, 'r'=114]
    // "apple" first 8 bytes: [len=5, 'a'=97,  'p'=112, 'p'=112, 'l'=108]
    let zebra_ck = i64::from_le_bytes([5, 0, 0, 0, 122, 101, 98, 114]);
    let apple_ck = i64::from_le_bytes([5, 0, 0, 0, 97, 112, 112, 108]);
    assert!(zebra_ck > apple_ck, "test invariant: zebra_ck > apple_ck");

    // With fix: replay = {apple+1, zebra+1, apple−1} → {zebra+1}; one output row.
    // With bug: replay = {apple+1, apple−1} → {}; no output row.
    assert_eq!(out.count, 1,
        "GI loop must be `while` to include zebra after apple is retracted; \
         `if` leaves replay empty → no output");

    // Output payload layout: col_data[0]=grp(I64), col_data[1]=agg(I64)
    let agg = crate::util::read_i64_le(out.col_data(1), 0);
    assert_eq!(agg, zebra_ck,
        "MAX of {{zebra+1}} must be zebra_ck; got {agg} (apple_ck={apple_ck})");
}

// -----------------------------------------------------------------------
// op_gather_reduce
// -----------------------------------------------------------------------

#[test]
fn test_gather_reduce_retraction() {
    use std::rc::Rc;
    use crate::storage::CursorHandle;
    use crate::schema::type_code;

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
        col_idx: 1, agg_op: AggOp::Sum, col_type_code: TypeCode::I64, _pad: [0; 2],
    };

    let out1 = op_gather_reduce(&partial1, to_ch.cursor_mut(), &schema, &[agg]);
    assert_eq!(out1.count, 1);
    let global_count = crate::util::read_i64_le(out1.col_data(0), 0);
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
    let batch = make_batch_f32(&schema, &[
        (1, 1, 2.0f32),
        (2, 1, -1.0f32),
        (3, 1, 0.5f32),
    ]);
    let indices = argsort_delta(&batch, &schema, &[1]);
    // Sorted order by F32: -1.0 < 0.5 < 2.0
    assert_eq!(indices.len(), 3);
    let mb = batch.as_mem_batch();
    let vals: Vec<f32> = indices.iter().map(|&i| {
        let ptr = mb.get_col_ptr(i as usize, 0, 4);
        f32::from_bits(u32::from_le_bytes(ptr.try_into().unwrap()))
    }).collect();
    assert_eq!(vals, vec![-1.0f32, 0.5f32, 2.0f32]);
}

#[test]
fn test_compare_by_group_cols_f32_negative() {
    let schema = make_schema_u64_f32();
    let batch = make_batch_f32(&schema, &[
        (1, 1, -5.0f32),
        (2, 1, 3.0f32),
    ]);
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
        &vals.iter().enumerate().map(|(i, &v)| (i as u64 + 1, 1, v)).collect::<Vec<_>>(),
    );
    let mb = batch.as_mem_batch();
    let encoded: Vec<u64> = (0..vals.len()).map(|row| {
        let pi = schema.payload_idx(1); // col_idx=1, pk_index=0
        let ptr = mb.get_col_ptr(row, pi, 4);
        let raw32 = u32::from_le_bytes(ptr.try_into().unwrap());
        // Order-preserving F32 encode (the encode_ordered F32 arm, for_max=false).
        ieee_order_bits_f32(raw32)
    }).collect();
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
            "round-trip failed for {:?}",
            v
        );
    }
}

#[test]
fn test_extract_group_key_f32() {
    let schema = make_schema_u64_f32();
    let batch = make_batch_f32(&schema, &[
        (1, 1, 1.5f32),
        (2, 1, 2.5f32),
    ]);
    let mb = batch.as_mem_batch();
    let key0 = extract_group_key(&mb, 0, &schema, &[1]);
    let key1 = extract_group_key(&mb, 1, &schema, &[1]);
    assert_ne!(key0, key1, "different F32 values must produce different group keys");
}

// -----------------------------------------------------------------------
// Fix 1: Schema-agnostic reads for sub-8-byte columns
// -----------------------------------------------------------------------

fn make_schema_with_type(tc: u8) -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(tc, 0),
        ],
        &[0],
    )
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
    use std::rc::Rc;
    use crate::storage::CursorHandle;

    let in_schema = make_schema_with_type(type_code::I32);

    // Output: pk(U128), sum(I64)
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::I64, 1),
        ],
        &[0],
    );

    let empty_out = Rc::new(Batch::empty(1, 16));
    let mut to_ch = CursorHandle::from_owned(&[empty_out], out_schema);

    // 3 rows with I32 values, group by PK
    let delta = make_batch_typed_i32(&in_schema, &[
        (1, 1, 100i32), (2, 1, 200i32), (3, 1, -50i32),
    ]);

    let agg = AggDescriptor {
        col_idx: 1, agg_op: AggOp::Sum, col_type_code: TypeCode::I32, _pad: [0; 2],
    };

    // GROUP BY pk → each row is its own group
    let (out, _) = op_reduce(
        &delta, None, to_ch.cursor_mut(),
        &in_schema, &out_schema, &[0u32], &[agg],
        None, false, TypeCode::U64, &[], None, None, 0, None, None,
    );
    assert_eq!(out.count, 3);
    // Check values: row offsets depend on PK order (group_by_pk path)
    let sum0 = crate::util::read_i64_le(out.col_data(0), 0);
    let sum1 = crate::util::read_i64_le(out.col_data(0), 8);
    let sum2 = crate::util::read_i64_le(out.col_data(0), 16);
    assert_eq!(sum0, 100, "SUM of I32 100");
    assert_eq!(sum1, 200, "SUM of I32 200");
    assert_eq!(sum2, -50, "SUM of I32 -50 (sign extension)");
}

#[test]
fn test_reduce_min_f32() {
    use std::rc::Rc;
    use crate::storage::CursorHandle;

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
    let delta = make_batch_f32(&in_schema, &[
        (1, 1, 3.5f32), (1, 1, -1.0f32), (1, 1, 7.0f32),
    ]);

    let agg = AggDescriptor {
        col_idx: 1, agg_op: AggOp::Min, col_type_code: TypeCode::F32, _pad: [0; 2],
    };

    // GROUP BY pk → all 3 rows in same group
    let (out, _) = op_reduce(
        &delta, None, to_ch.cursor_mut(),
        &in_schema, &out_schema, &[0u32], &[agg],
        None, false, TypeCode::U64, &[], None, None, 0, None, None,
    );
    assert_eq!(out.count, 1);
    // MIN should be -1.0 stored as f64 bits
    let bits = u64::from_le_bytes(out.col_data(0)[0..8].try_into().unwrap());
    let min_val = f64::from_bits(bits);
    assert_eq!(min_val, -1.0f64, "MIN of F32 {{3.5, -1.0, 7.0}} should be -1.0");
}

#[test]
fn test_reduce_max_i16() {
    use std::rc::Rc;
    use crate::storage::CursorHandle;

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
    let delta = make_batch_typed_i16(&in_schema, &[
        (1, 1, -100i16), (1, 1, 200i16), (1, 1, 50i16),
    ]);

    let agg = AggDescriptor {
        col_idx: 1, agg_op: AggOp::Max, col_type_code: TypeCode::I16, _pad: [0; 2],
    };

    let (out, _) = op_reduce(
        &delta, None, to_ch.cursor_mut(),
        &in_schema, &out_schema, &[0u32], &[agg],
        None, false, TypeCode::U64, &[], None, None, 0, None, None,
    );
    assert_eq!(out.count, 1);
    let max_val = crate::util::read_i64_le(out.col_data(0), 0);
    assert_eq!(max_val, 200, "MAX of I16 {{-100, 200, 50}} should be 200");
}

// -----------------------------------------------------------------------
// Fix 6: gather_reduce MIN retraction
// -----------------------------------------------------------------------

#[test]
fn test_gather_reduce_min_retraction() {
    use std::rc::Rc;
    use crate::storage::CursorHandle;

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
        col_idx: 1, agg_op: AggOp::Min, col_type_code: TypeCode::I64, _pad: [0; 2],
    };

    let out1 = op_gather_reduce(&partial1, to_ch.cursor_mut(), &schema, &[agg]);
    assert_eq!(out1.count, 1);
    let min1 = crate::util::read_i64_le(out1.col_data(0), 0);
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
    let retracted = crate::util::read_i64_le(out2.col_data(0), 0);
    assert_eq!(retracted, 5, "retraction should be old MIN value 5");
    assert_eq!(out2.get_weight(0), -1);
    let new_min = crate::util::read_i64_le(out2.col_data(0), 8);
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
    assert_eq!(ord, std::cmp::Ordering::Less, "uuid_lo row must compare less than uuid_hi row");

    let ord2 = compare_by_group_cols(&mb, 1, 0, descs);
    assert_eq!(ord2, std::cmp::Ordering::Greater, "uuid_hi row must compare greater than uuid_lo row");

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
    let batch = build_batch_u64_uuid_i64(&schema, &[
        (1, uuid_a, 42i64),
        (2, uuid_b, 42i64),
        (3, uuid_a, 43i64),
    ]);
    let mb = batch.as_mem_batch();

    // GROUP BY (uuid_col=1, i64_col=2)
    let key0 = extract_group_key(&mb, 0, &schema, &[1, 2]); // uuid_a, 42
    let key1 = extract_group_key(&mb, 1, &schema, &[1, 2]); // uuid_b, 42
    let key2 = extract_group_key(&mb, 2, &schema, &[1, 2]); // uuid_a, 43
    let key0b = extract_group_key(&mb, 0, &schema, &[1, 2]); // same as key0

    assert_ne!(key0, key1, "different UUIDs same int must yield different group keys");
    assert_ne!(key0, key2, "same UUID different int must yield different group keys");
    assert_ne!(key1, key2, "different UUID different int must yield different group keys");
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
    use std::rc::Rc;
    use crate::storage::CursorHandle;

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

    let gi_schema = crate::ops::index::make_gi_schema();

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
    let gi_batch = Rc::new(make_gi_batch(&[(30, 5, 0)]).into_inner());

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
        None, false, TypeCode::U64, &[], None,
        Some(gi_handle.cursor_mut()),
        1u32,
        None, None,
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
    assert_eq!(m, 200,
        "MIN(grp=5) must include history row val=200; \
         got {m} — GI group-key over-read produced a garbage gc_u64_val");
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
    let batch = build_pk_other(&schema, &[
        (20, 100),
        (10, 200),
        (10, 100),
    ]);
    let indices = argsort_delta(&batch, &schema, &[0, 1]);
    assert_eq!(indices.len(), 3);
    // Sorted by (pk, other): (10,100), (10,200), (20,100)
    let mb = batch.as_mem_batch();
    let pks: Vec<u64> = indices.iter().map(|&i| mb.get_pk(i as usize) as u64).collect();
    assert_eq!(pks, vec![10, 10, 20]);
    let others: Vec<i64> = indices.iter().map(|&i| {
        i64::from_le_bytes(mb.get_col_ptr(i as usize, 0, 8).try_into().unwrap())
    }).collect();
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
    let batch = build_pk_null_i64(&schema, &[
        (1, None),
        (2, Some(0)),
        (3, Some(7)),
        (4, None),
    ]);
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
    let batch = build_pk_null_i64(&schema, &[
        (1, Some(7)),
        (2, None),
        (3, None),
    ]);
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
    let batch = build_pk_null_i64(&schema, &[
        (1, Some(0)),
        (2, None),
        (3, Some(5)),
        (4, None),
    ]);
    let indices = argsort_delta(&batch, &schema, &[1]);
    let mb = batch.as_mem_batch();
    // NULLs must be adjacent (single group), not interleaved with 0s.
    let null_word_at = |i: u32| mb.get_null_word(i as usize) & 1 != 0;
    let null_positions: Vec<usize> = indices.iter().enumerate()
        .filter(|&(_, &i)| null_word_at(i))
        .map(|(p, _)| p)
        .collect();
    assert_eq!(null_positions.len(), 2, "expected 2 NULL rows");
    // NULLS FIRST: both nulls at positions 0 and 1.
    assert_eq!(null_positions, vec![0, 1]);
}

// -----------------------------------------------------------------------
// UUID single-column GROUP BY: extract_gc_u64 must mix high+low halves.
// -----------------------------------------------------------------------

#[test]
fn test_extract_gc_u64_uuid_distinguishes_high_bits() {
    // Two UUIDs differing only in the high 64 bits previously truncated
    // to the same low-64 value, colliding in AVI buckets.
    let schema = make_schema_u64_pk_uuid_payload();
    let uuid_a: u128 = 0x0000_0000_0000_0001_DEAD_BEEF_CAFE_BABEu128;
    let uuid_b: u128 = 0xFFFF_FFFF_FFFF_FFFF_DEAD_BEEF_CAFE_BABEu128;
    // Sanity: low 64 bits identical
    assert_eq!(uuid_a as u64, uuid_b as u64);

    let batch = build_batch_u64_uuid(&schema, &[(1, uuid_a), (2, uuid_b)]);
    let mb = batch.as_mem_batch();

    let gc_a = extract_gc_u64(&mb, 0, &schema, &[1]);
    let gc_b = extract_gc_u64(&mb, 1, &schema, &[1]);

    assert_ne!(gc_a, gc_b,
        "UUIDs differing only in high 64 bits must produce distinct AVI bucket keys; \
         pre-fix truncation collided them");
}

// -----------------------------------------------------------------------
// emit_finalized_row: U128 PK projected through CopyCol must not panic
// when the destination column size is 16 bytes.
// -----------------------------------------------------------------------

#[test]
fn test_emit_finalized_row_u128_pk_copy_col() {
    use crate::expr::{ExprProgram, EXPR_COPY_COL};

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

    // Two COPY_COL instructions: copy col 0 (PK) and col 1 (cnt).
    // Layout per instruction: [op, dst, a1=src_col, a2]. classify_output_cols
    // reads src_col from a1 (instr[base + 2]).
    let code: Vec<i64> = vec![
        EXPR_COPY_COL, 0, 0, 0, // copy raw col 0 (PK) → fin col 1
        EXPR_COPY_COL, 0, 1, 0, // copy raw col 1 (cnt) → fin col 2
    ];
    let mut prog = ExprProgram::new(code, 0, 0, vec![]);
    prog.resolve_column_indices(&raw_schema);

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
        &mut fin_output, &raw_output, 0,
        pk, 1,
        &prog, &raw_schema, &fin_schema, &mut ctx,
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
fn make_batch_compound_2xu64(
    schema: &SchemaDescriptor,
    rows: &[(u64, u64, i64, i64)],
) -> Batch {
    let n = rows.len();
    let mut b = Batch::with_schema(*schema, n.max(1));
    for &(pk0, pk1, w, val) in rows {
        let mut pk_bytes = [0u8; 16];
        pk_bytes[0..8].copy_from_slice(&pk0.to_le_bytes());
        pk_bytes[8..16].copy_from_slice(&pk1.to_le_bytes());
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
        col_idx: 2, agg_op: AggOp::Count, col_type_code: TypeCode::I64, _pad: [0; 2],
    };
    let accs: Vec<Accumulator> = vec![Accumulator::new(&agg, &in_schema)];
    // Synthetic group_key is irrelevant on the byte path; pass arbitrary value.
    emit_reduce_row(
        &mut output, &mb, 0,
        0u128, 1,
        &[0u64], false,
        &accs, &in_schema, &out_schema,
        &[0u32, 1u32], true /* use_natural_pk */, 1,
    );

    assert_eq!(output.count, 1);
    let mut expected = [0u8; 16];
    expected[0..8].copy_from_slice(&pk0.to_le_bytes());
    expected[8..16].copy_from_slice(&pk1.to_le_bytes());
    assert_eq!(output.get_pk_bytes(0), &expected[..],
        "compound natural-PK output must copy source row's PK bytes verbatim");
}

/// Accumulator MIN on the SECOND PK column of a 2×U64 compound PK.
/// Regression: the second PK column must be decoded by walking its
/// byte offset within the PK region, not by widening the whole
/// region to u128 (which would yield column 0).
#[test]
fn test_reduce_min_pk_col_compound_pk() {
    use std::rc::Rc;
    use crate::storage::CursorHandle;

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
    let delta = make_batch_compound_2xu64(&in_schema, &[
        (10, 7, 1, 100),
        (20, 3, 1, 200),
    ]);

    // MIN over the SECOND PK column (col_idx=1).
    let agg = AggDescriptor {
        col_idx: 1, agg_op: AggOp::Min, col_type_code: TypeCode::U64, _pad: [0; 2],
    };

    let (out, _) = op_reduce(
        &delta, None, to_ch.cursor_mut(),
        &in_schema, &out_schema,
        &[0u32, 1u32], &[agg],
        None, false, TypeCode::U64, &[], None,
        None, 0, None, None,
    );
    // group_by_pk: each (pk0, pk1) is its own group, so we get one row
    // per input row; MIN within each group equals the row's pk_col_1.
    assert_eq!(out.count, 2);
    let mins: Vec<i64> = (0..out.count)
        .map(|i| crate::util::read_i64_le(out.col_data(0), i * 8))
        .collect();
    // Output is in pk_indices order = [0, 1] ascending, so (10,7) precedes (20,3).
    assert_eq!(mins, vec![7, 3],
        "MIN(pk_col_1) per single-row group must equal that row's pk_col_1");
}

/// Single-PK U64 MIN(pk_col) — sanity check that the byte-offset
/// PK-read path produces the same result as the prior u128 path.
#[test]
fn test_reduce_min_pk_col_single_pk_u64() {
    use std::rc::Rc;
    use crate::storage::CursorHandle;

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
        col_idx: 0, agg_op: AggOp::Min, col_type_code: TypeCode::U64, _pad: [0; 2],
    };
    let (out, _) = op_reduce(
        &delta, None, to_ch.cursor_mut(),
        &in_schema, &out_schema,
        &[0u32], &[agg],
        None, false, TypeCode::U64, &[], None,
        None, 0, None, None,
    );
    // GROUP BY pk → each row is its own group; MIN(pk) per group equals the row's pk.
    assert_eq!(out.count, 3);
    let mins: Vec<i64> = (0..out.count)
        .map(|i| crate::util::read_i64_le(out.col_data(0), i * 8))
        .collect();
    assert_eq!(mins, vec![7, 42, 99]);
}

/// Permuted group_by_cols on a compound PK must still emit rows in
/// pk_indices order (the input is PK-sorted from consolidation; the
/// fast path skips the sort and passes row order through).
#[test]
fn test_reduce_group_by_pk_permuted_preserves_pk_order() {
    use std::rc::Rc;
    use crate::storage::CursorHandle;

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
    let delta = make_batch_compound_2xu64(&in_schema, &[
        (1, 2, 1, 10),
        (2, 1, 1, 20),
    ]);

    let agg = AggDescriptor {
        col_idx: 2, agg_op: AggOp::Count, col_type_code: TypeCode::I64, _pad: [0; 2],
    };

    // group_by_cols permuted to [1, 0] — a valid set permutation of pk_indices.
    let (out, _) = op_reduce(
        &delta, None, to_ch.cursor_mut(),
        &in_schema, &out_schema,
        &[1u32, 0u32], &[agg],
        None, false, TypeCode::U64, &[], None,
        None, 0, None, None,
    );

    assert_eq!(out.count, 2);
    let row0_pk = out.get_pk_bytes(0);
    let row1_pk = out.get_pk_bytes(1);
    let p0_col0 = u64::from_le_bytes(row0_pk[0..8].try_into().unwrap());
    let p0_col1 = u64::from_le_bytes(row0_pk[8..16].try_into().unwrap());
    let p1_col0 = u64::from_le_bytes(row1_pk[0..8].try_into().unwrap());
    let p1_col1 = u64::from_le_bytes(row1_pk[8..16].try_into().unwrap());
    assert_eq!((p0_col0, p0_col1), (1, 2),
        "first emitted row must be (1, 2) in pk_indices order");
    assert_eq!((p1_col0, p1_col1), (2, 1),
        "second emitted row must be (2, 1) in pk_indices order");
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
    let batch = make_batch_compound_2xu64(&schema, &[
        (10, 7, 1, 100),
        (10, 9, 1, 200),
        (20, 7, 1, 300),
    ]);
    let mb = batch.as_mem_batch();

    let (descs_arr, descs_len) = build_sort_descs(&schema, &[0u32]);
    let descs = &descs_arr[..descs_len];
    assert_eq!(descs[0].pi, PAYLOAD_MAPPING_PK_SENTINEL,
        "subset group on PK col 0 must produce a PK-sentinel SortDesc");
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
    let batch = make_batch_compound_2xu64(&schema, &[
        (1, 50, 1, 100),
        (2, 50, 1, 200),
        (3, 60, 1, 300),
    ]);
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
    use std::rc::Rc;
    use crate::storage::CursorHandle;

    let schema = make_compound_pk_2xu64_schema();
    // Cursor row: (pk0=10, pk1=99). Exemplar row: (pk0=10, pk1=42).
    // GROUP BY pk_col_0 → must match (both share pk0=10).
    let cursor_batch = Rc::new(make_batch_compound_2xu64(&schema, &[
        (10, 99, 1, 0),
    ]));
    let exemplar_batch = make_batch_compound_2xu64(&schema, &[
        (10, 42, 1, 0),
        (20, 42, 1, 0),
    ]);
    let exemplar_mb = exemplar_batch.as_mem_batch();

    let mut cursor_handle = CursorHandle::from_owned(&[cursor_batch], schema);
    let cursor = cursor_handle.cursor_mut();
    cursor.seek(0u128);
    assert!(cursor.valid, "cursor must be positioned on the single row");

    let (descs_arr, descs_len) = build_sort_descs(&schema, &[0u32]);
    let descs = &descs_arr[..descs_len];

    // Row 0 (pk0=10) shares pk_col_0 with the cursor → match.
    assert!(cursor_matches_group(cursor, &exemplar_mb, 0, descs),
        "exemplar (10,42) and cursor (10,99) share pk_col_0=10");
    // Row 1 (pk0=20) differs from the cursor's pk_col_0=10 → no match.
    assert!(!cursor_matches_group(cursor, &exemplar_mb, 1, descs),
        "exemplar (20,42) differs from cursor (10,99) in pk_col_0");
}

/// extract_group_key on `GROUP BY pk_col_0` (single PK column of a
/// compound PK) must return the same u128 for two rows that share
/// pk_col_0 — distinct pk_col_1 values must not collide them into
/// different groups.
#[test]
fn test_extract_group_key_single_pk_col_compound_subset() {
    let schema = make_compound_pk_2xu64_schema();
    let batch = make_batch_compound_2xu64(&schema, &[
        (10, 50, 1, 0),
        (10, 99, 1, 0),
        (20, 50, 1, 0),
    ]);
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
    use std::rc::Rc;
    use crate::storage::CursorHandle;

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
    let delta = make_batch_compound_2xu64(&in_schema, &[
        (1, 10, 1, 0),
        (1, 20, 1, 0),
        (2, 10, 1, 0),
    ]);

    let agg = AggDescriptor {
        col_idx: 2, agg_op: AggOp::Count, col_type_code: TypeCode::I64, _pad: [0; 2],
    };

    let (out, _) = op_reduce(
        &delta, None, to_ch.cursor_mut(),
        &in_schema, &out_schema,
        &[0u32], &[agg],
        None, false, TypeCode::U64, &[], None,
        None, 0, None, None,
    );

    // Two groups: pk_col_0=1 (count=2), pk_col_0=2 (count=1).
    // Pre-fix the count would be 3 (one row per (pk0, pk1) pair).
    assert_eq!(out.count, 2, "GROUP BY pk_col_0 collapses (1,10) and (1,20) into one group");

    // Output rows in pk_col_0 ascending order (slow path argsorts).
    let mut entries: Vec<(u64, i64)> = (0..out.count)
        .map(|i| {
            let pk_bytes = out.get_pk_bytes(i);
            let pk = u64::from_le_bytes(pk_bytes.try_into().unwrap());
            let cnt = crate::util::read_i64_le(out.col_data(0), i * 8);
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
    use std::rc::Rc;
    use crate::storage::CursorHandle;

    let in_schema = make_schema_u64pk_i64grp_u64val();
    let out_schema = make_out_schema_grp_u64agg();

    let empty_out = Rc::new(Batch::empty(out_schema.num_payload_cols(), 16));
    let mut to_ch = CursorHandle::from_owned(&[empty_out], out_schema);

    // One group (grp=7). val=1, u64::MAX, and 2^63 — the unsigned MIN is 1.
    // Pre-fix signed comparison treats u64::MAX as -1 (smallest signed),
    // so the bug reports u64::MAX as the MIN.
    let delta = make_batch_u64pk_i64grp_u64val(&in_schema, &[
        (1, 1, 7, u64::MAX),
        (2, 1, 7, 10),
        (3, 1, 7, 1u64 << 63),
        (4, 1, 7, 1),
    ]);

    let agg = AggDescriptor {
        col_idx: 2, agg_op: AggOp::Min, col_type_code: TypeCode::U64, _pad: [0; 2],
    };

    let (out, _) = op_reduce(
        &delta, None, to_ch.cursor_mut(),
        &in_schema, &out_schema, &[1u32], &[agg],
        None, false, TypeCode::U64, &[], None, None, 0, None, None,
    );
    assert_eq!(out.count, 1);
    let min_bits = u64::from_le_bytes(out.col_data(1)[0..8].try_into().unwrap());
    assert_eq!(min_bits, 1u64, "MIN(u64) must use unsigned ordering");
}

#[test]
fn test_reduce_max_u64_high_bit_set() {
    use std::rc::Rc;
    use crate::storage::CursorHandle;

    let in_schema = make_schema_u64pk_i64grp_u64val();
    let out_schema = make_out_schema_grp_u64agg();

    let empty_out = Rc::new(Batch::empty(out_schema.num_payload_cols(), 16));
    let mut to_ch = CursorHandle::from_owned(&[empty_out], out_schema);

    // Same input as MIN test. Unsigned MAX is u64::MAX. Pre-fix signed
    // comparison treats 10 (positive i64) as larger than u64::MAX (=-1).
    let delta = make_batch_u64pk_i64grp_u64val(&in_schema, &[
        (1, 1, 7, u64::MAX),
        (2, 1, 7, 10),
        (3, 1, 7, 1u64 << 63),
        (4, 1, 7, 1),
    ]);

    let agg = AggDescriptor {
        col_idx: 2, agg_op: AggOp::Max, col_type_code: TypeCode::U64, _pad: [0; 2],
    };

    let (out, _) = op_reduce(
        &delta, None, to_ch.cursor_mut(),
        &in_schema, &out_schema, &[1u32], &[agg],
        None, false, TypeCode::U64, &[], None, None, 0, None, None,
    );
    assert_eq!(out.count, 1);
    let max_bits = u64::from_le_bytes(out.col_data(1)[0..8].try_into().unwrap());
    assert_eq!(max_bits, u64::MAX, "MAX(u64) must use unsigned ordering");
}

#[test]
fn test_reduce_min_u64_incremental() {
    use std::rc::Rc;
    use crate::storage::CursorHandle;

    let in_schema = make_schema_u64pk_i64grp_u64val();
    let out_schema = make_out_schema_grp_u64agg();

    // Tick 1: one row with val=1u64<<60 → MIN = 1u64<<60.
    let empty_out = Rc::new(Batch::empty(out_schema.num_payload_cols(), 16));
    let mut to_ch = CursorHandle::from_owned(&[empty_out], out_schema);

    let delta1 = make_batch_u64pk_i64grp_u64val(&in_schema, &[
        (1, 1, 7, 1u64 << 60),
    ]);

    let agg = AggDescriptor {
        col_idx: 2, agg_op: AggOp::Min, col_type_code: TypeCode::U64, _pad: [0; 2],
    };

    let empty_ti = Rc::new(Batch::empty(in_schema.num_payload_cols(), 16));
    let mut ti_ch = CursorHandle::from_owned(&[empty_ti], in_schema);

    let (out1, _) = op_reduce(
        &delta1, Some(ti_ch.cursor_mut()), to_ch.cursor_mut(),
        &in_schema, &out_schema, &[1u32], &[agg],
        None, false, TypeCode::U64, &[], None, None, 0, None, None,
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

    let ti2 = Rc::new(make_batch_u64pk_i64grp_u64val(&in_schema, &[
        (1, 1, 7, 1u64 << 60),
    ]).into_inner());
    let mut ti_ch2 = CursorHandle::from_owned(&[ti2], in_schema);

    let delta2 = make_batch_u64pk_i64grp_u64val(&in_schema, &[
        (2, 1, 7, 1u64 << 63),
    ]);

    let (out2, _) = op_reduce(
        &delta2, Some(ti_ch2.cursor_mut()), to_ch2.cursor_mut(),
        &in_schema, &out_schema, &[1u32], &[agg],
        None, false, TypeCode::U64, &[], None, None, 0, None, None,
    );
    assert_eq!(out2.count, 2, "retract old MIN + emit new MIN");
    let retracted = u64::from_le_bytes(out2.col_data(1)[0..8].try_into().unwrap());
    let new_min = u64::from_le_bytes(out2.col_data(1)[8..16].try_into().unwrap());
    assert_eq!(retracted, 1u64 << 60);
    assert_eq!(out2.get_weight(0), -1);
    assert_eq!(new_min, 1u64 << 60,
        "MIN unchanged under unsigned ordering; bug would flip it to 1u64<<63");
    assert_eq!(out2.get_weight(1), 1);
}

#[test]
fn test_reduce_max_u64_incremental() {
    use std::rc::Rc;
    use crate::storage::CursorHandle;

    let in_schema = make_schema_u64pk_i64grp_u64val();
    let out_schema = make_out_schema_grp_u64agg();

    // Tick 1: MAX over a single low value → MAX = 10.
    let empty_out = Rc::new(Batch::empty(out_schema.num_payload_cols(), 16));
    let mut to_ch = CursorHandle::from_owned(&[empty_out], out_schema);

    let delta1 = make_batch_u64pk_i64grp_u64val(&in_schema, &[
        (1, 1, 7, 10),
    ]);

    let agg = AggDescriptor {
        col_idx: 2, agg_op: AggOp::Max, col_type_code: TypeCode::U64, _pad: [0; 2],
    };

    let empty_ti = Rc::new(Batch::empty(in_schema.num_payload_cols(), 16));
    let mut ti_ch = CursorHandle::from_owned(&[empty_ti], in_schema);

    let (out1, _) = op_reduce(
        &delta1, Some(ti_ch.cursor_mut()), to_ch.cursor_mut(),
        &in_schema, &out_schema, &[1u32], &[agg],
        None, false, TypeCode::U64, &[], None, None, 0, None, None,
    );
    assert_eq!(out1.count, 1);
    let max1 = u64::from_le_bytes(out1.col_data(1)[0..8].try_into().unwrap());
    assert_eq!(max1, 10);

    // Tick 2: delta adds val=u64::MAX. Replay re-steps over both.
    // Pre-fix signed MAX would treat u64::MAX as -1, keeping MAX=10.
    let prev_out = Rc::new(out1);
    let mut to_ch2 = CursorHandle::from_owned(&[prev_out], out_schema);

    let ti2 = Rc::new(make_batch_u64pk_i64grp_u64val(&in_schema, &[
        (1, 1, 7, 10),
    ]).into_inner());
    let mut ti_ch2 = CursorHandle::from_owned(&[ti2], in_schema);

    let delta2 = make_batch_u64pk_i64grp_u64val(&in_schema, &[
        (2, 1, 7, u64::MAX),
    ]);

    let (out2, _) = op_reduce(
        &delta2, Some(ti_ch2.cursor_mut()), to_ch2.cursor_mut(),
        &in_schema, &out_schema, &[1u32], &[agg],
        None, false, TypeCode::U64, &[], None, None, 0, None, None,
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
        col_idx: 1, agg_op: AggOp::Min, col_type_code: TypeCode::U64, _pad: [0; 2],
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
    assert_eq!(acc.get_value_bits(), 10u64,
        "unsigned MIN against AVI-seeded U64 high-bit value");
}

#[test]
fn test_reduce_min_u64_replay_via_trace_in() {
    use std::rc::Rc;
    use crate::storage::CursorHandle;

    // Exercise the replay path (step_from_batch over trace_in rows + delta
    // rows) with U64 values that span both halves of the unsigned range.
    // Pre-fix the signed comparator treats high-bit-set values as the
    // most-negative; unsigned MIN flips silently.
    let in_schema = make_schema_u64pk_i64grp_u64val();
    let out_schema = make_out_schema_grp_u64agg();

    let agg = AggDescriptor {
        col_idx: 2, agg_op: AggOp::Min, col_type_code: TypeCode::U64, _pad: [0; 2],
    };

    // Tick 1: single insertion. trace_in empty. MIN = u64::MAX.
    let empty_ti = Rc::new(Batch::empty(in_schema.num_payload_cols(), 16));
    let mut ti_ch = CursorHandle::from_owned(&[empty_ti], in_schema);
    let empty_to = Rc::new(Batch::empty(out_schema.num_payload_cols(), 16));
    let mut to_ch = CursorHandle::from_owned(&[empty_to], out_schema);

    let delta1 = make_batch_u64pk_i64grp_u64val(&in_schema, &[
        (1, 1, 7, u64::MAX),
    ]);

    let (out1, _) = op_reduce(
        &delta1, Some(ti_ch.cursor_mut()), to_ch.cursor_mut(),
        &in_schema, &out_schema, &[1u32], &[agg],
        None, false, TypeCode::U64, &[], None, None, 0, None, None,
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

    let delta2 = make_batch_u64pk_i64grp_u64val(&in_schema, &[
        (2, 1, 7, 5u64),
    ]);

    let (out2, _) = op_reduce(
        &delta2, Some(ti_ch2.cursor_mut()), to_ch2.cursor_mut(),
        &in_schema, &out_schema, &[1u32], &[agg],
        None, false, TypeCode::U64, &[], None, None, 0, None, None,
    );
    // Retract old MIN (u64::MAX) + emit new MIN (5).
    assert_eq!(out2.count, 2);
    let retracted = u64::from_le_bytes(out2.col_data(1)[0..8].try_into().unwrap());
    assert_eq!(retracted, u64::MAX);
    assert_eq!(out2.get_weight(0), -1);
    let new_min = u64::from_le_bytes(out2.col_data(1)[8..16].try_into().unwrap());
    assert_eq!(new_min, 5u64,
        "replay over trace_in + delta must use unsigned MIN");
    assert_eq!(out2.get_weight(1), 1);
}

#[test]
fn test_reduce_min_max_i64_boundary() {
    use std::rc::Rc;
    use crate::storage::CursorHandle;

    // Guard that the TypeCode::U64 branch does not leak into I64 paths:
    // MIN of {i64::MIN, -1, 0, i64::MAX} = i64::MIN,
    // MAX = i64::MAX.
    let in_schema = make_schema_u64pk_i64grp_i64val();
    let out_schema = make_out_schema_grp_i64agg();

    // MIN test.
    {
        let empty_out = Rc::new(Batch::empty(out_schema.num_payload_cols(), 16));
        let mut to_ch = CursorHandle::from_owned(&[empty_out], out_schema);

        let delta = make_batch_u64pk_i64grp_i64val(&in_schema, &[
            (1, 1, 7, i64::MIN),
            (2, 1, 7, -1),
            (3, 1, 7, 0),
            (4, 1, 7, i64::MAX),
        ]);

        let agg = AggDescriptor {
            col_idx: 2, agg_op: AggOp::Min, col_type_code: TypeCode::I64, _pad: [0; 2],
        };

        let (out, _) = op_reduce(
            &delta, None, to_ch.cursor_mut(),
            &in_schema, &out_schema, &[1u32], &[agg],
            None, false, TypeCode::U64, &[], None, None, 0, None, None,
        );
        assert_eq!(out.count, 1);
        let min = crate::util::read_i64_le(out.col_data(1), 0);
        assert_eq!(min, i64::MIN, "MIN(I64) signed ordering preserved");
    }

    // MAX test.
    {
        let empty_out = Rc::new(Batch::empty(out_schema.num_payload_cols(), 16));
        let mut to_ch = CursorHandle::from_owned(&[empty_out], out_schema);

        let delta = make_batch_u64pk_i64grp_i64val(&in_schema, &[
            (1, 1, 7, i64::MIN),
            (2, 1, 7, -1),
            (3, 1, 7, 0),
            (4, 1, 7, i64::MAX),
        ]);

        let agg = AggDescriptor {
            col_idx: 2, agg_op: AggOp::Max, col_type_code: TypeCode::I64, _pad: [0; 2],
        };

        let (out, _) = op_reduce(
            &delta, None, to_ch.cursor_mut(),
            &in_schema, &out_schema, &[1u32], &[agg],
            None, false, TypeCode::U64, &[], None, None, 0, None, None,
        );
        assert_eq!(out.count, 1);
        let max = crate::util::read_i64_le(out.col_data(1), 0);
        assert_eq!(max, i64::MAX, "MAX(I64) signed ordering preserved");
    }
}

#[test]
fn test_gather_reduce_min_u64() {
    use std::rc::Rc;
    use crate::storage::CursorHandle;

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
        col_idx: 1, agg_op: AggOp::Min, col_type_code: TypeCode::U64, _pad: [0; 2],
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
    assert_eq!(new_min, 3u64,
        "fold-old + combine-new under unsigned ordering keeps MIN at 3");
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
        b.extend_pk(pk_encode(pk));
        b.extend_weight(&w.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &val.to_le_bytes());
        b.count += 1;
    }
    b.sorted = false;
    b.consolidated = false;
    b
}

/// I64 pk + I64 payload schema (signed-PK exercise of make_slow_pk_cmp).
fn make_schema_i64pk_i64val() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    )
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

#[test]
fn test_reduce_group_by_pk_unsorted_input_linear_sum() {
    use std::rc::Rc;
    use crate::storage::CursorHandle;

    let in_schema = make_schema_u64_i64();
    let out_schema = make_pk_sum_out_schema();
    let empty_out = Rc::new(Batch::empty(out_schema.num_payload_cols(), 16));
    let mut to_ch = CursorHandle::from_owned(&[empty_out], out_schema);

    // Unsorted: pk=5 appears twice, separated by pk=3. The fast path
    // pre-fix walked physical order and split into 3 groups → emitting
    // two distinct pk=5 rows.
    let delta = make_batch_raw_pk(&in_schema, &[
        (5, 1, 10),
        (3, 1, 20),
        (5, 1, 30),
    ], |pk: u64| pk as u128);

    let agg = AggDescriptor {
        col_idx: 1, agg_op: AggOp::Sum, col_type_code: TypeCode::I64, _pad: [0; 2],
    };

    let (out, _) = op_reduce(
        &delta, None, to_ch.cursor_mut(),
        &in_schema, &out_schema,
        &[0u32], &[agg],
        None, false, TypeCode::U64, &[], None,
        None, 0, None, None,
    );

    assert_eq!(out.count, 2, "one row per distinct PK");
    let pk0 = out.get_pk_bytes(0);
    let pk1 = out.get_pk_bytes(1);
    let pk0_val = u128::from_le_bytes(pk0.try_into().unwrap());
    let pk1_val = u128::from_le_bytes(pk1.try_into().unwrap());
    assert_eq!(pk0_val, 3, "PK-sorted: 3 precedes 5");
    assert_eq!(pk1_val, 5);
    let sum0 = crate::util::read_i64_le(out.col_data(0), 0);
    let sum1 = crate::util::read_i64_le(out.col_data(0), 8);
    assert_eq!(sum0, 20, "SUM for pk=3");
    assert_eq!(sum1, 40, "SUM for pk=5 (10+30) — pre-fix produced two split rows");
    assert!(out.sorted, "output is PK-sorted by the fast path");
    assert!(out.consolidated, "output is consolidated by the fast path");
}

#[test]
fn test_reduce_group_by_pk_unsorted_input_count() {
    use std::rc::Rc;
    use crate::storage::CursorHandle;

    let in_schema = make_schema_u64_i64();
    let out_schema = make_pk_sum_out_schema(); // pk + I64 agg
    let empty_out = Rc::new(Batch::empty(out_schema.num_payload_cols(), 16));
    let mut to_ch = CursorHandle::from_owned(&[empty_out], out_schema);

    let delta = make_batch_raw_pk(&in_schema, &[
        (5, 1, 10),
        (3, 1, 20),
        (5, 1, 30),
    ], |pk: u64| pk as u128);

    let agg = AggDescriptor {
        col_idx: 1, agg_op: AggOp::Count, col_type_code: TypeCode::I64, _pad: [0; 2],
    };

    let (out, _) = op_reduce(
        &delta, None, to_ch.cursor_mut(),
        &in_schema, &out_schema,
        &[0u32], &[agg],
        None, false, TypeCode::U64, &[], None,
        None, 0, None, None,
    );

    assert_eq!(out.count, 2);
    let pk0 = u128::from_le_bytes(out.get_pk_bytes(0).try_into().unwrap());
    let pk1 = u128::from_le_bytes(out.get_pk_bytes(1).try_into().unwrap());
    assert_eq!((pk0, pk1), (3, 5));
    let c0 = crate::util::read_i64_le(out.col_data(0), 0);
    let c1 = crate::util::read_i64_le(out.col_data(0), 8);
    assert_eq!((c0, c1), (1, 2), "pk=3 → 1, pk=5 → 2");
}

#[test]
fn test_reduce_group_by_pk_unsorted_sorted_input_equivalence() {
    use std::rc::Rc;
    use crate::storage::CursorHandle;

    let in_schema = make_schema_u64_i64();
    let out_schema = make_pk_sum_out_schema();
    let empty_out = Rc::new(Batch::empty(out_schema.num_payload_cols(), 16));
    let mut to_ch = CursorHandle::from_owned(&[empty_out], out_schema);

    // Same data as the unsorted-sum test but pre-sorted. The
    // `working.sorted` branch must produce identical output.
    let mut delta = make_batch_raw_pk(&in_schema, &[
        (3, 1, 20),
        (5, 1, 10),
        (5, 1, 30),
    ], |pk: u64| pk as u128);
    delta.sorted = true;

    let agg = AggDescriptor {
        col_idx: 1, agg_op: AggOp::Sum, col_type_code: TypeCode::I64, _pad: [0; 2],
    };

    let (out, _) = op_reduce(
        &delta, None, to_ch.cursor_mut(),
        &in_schema, &out_schema,
        &[0u32], &[agg],
        None, false, TypeCode::U64, &[], None,
        None, 0, None, None,
    );

    assert_eq!(out.count, 2);
    let pk0 = u128::from_le_bytes(out.get_pk_bytes(0).try_into().unwrap());
    let pk1 = u128::from_le_bytes(out.get_pk_bytes(1).try_into().unwrap());
    assert_eq!((pk0, pk1), (3, 5));
    let sum0 = crate::util::read_i64_le(out.col_data(0), 0);
    let sum1 = crate::util::read_i64_le(out.col_data(0), 8);
    assert_eq!((sum0, sum1), (20, 40));
}

#[test]
fn test_reduce_group_by_pk_unsorted_compound_pk_permuted() {
    use std::rc::Rc;
    use crate::storage::CursorHandle;

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
    let mut delta = make_batch_compound_2xu64(&in_schema, &[
        (2, 1, 1, 20),
        (1, 2, 1, 10),
    ]);
    delta.sorted = false;
    delta.consolidated = false;

    let agg = AggDescriptor {
        col_idx: 2, agg_op: AggOp::Count, col_type_code: TypeCode::I64, _pad: [0; 2],
    };

    // Permuted GROUP BY: [1, 0]. group_set_eq_pk still holds.
    let (out, _) = op_reduce(
        &delta, None, to_ch.cursor_mut(),
        &in_schema, &out_schema,
        &[1u32, 0u32], &[agg],
        None, false, TypeCode::U64, &[], None,
        None, 0, None, None,
    );

    assert_eq!(out.count, 2);
    let pk0 = out.get_pk_bytes(0);
    let pk1 = out.get_pk_bytes(1);
    let p0_col0 = u64::from_le_bytes(pk0[0..8].try_into().unwrap());
    let p0_col1 = u64::from_le_bytes(pk0[8..16].try_into().unwrap());
    let p1_col0 = u64::from_le_bytes(pk1[0..8].try_into().unwrap());
    let p1_col1 = u64::from_le_bytes(pk1[8..16].try_into().unwrap());
    // Canonical pk_indices order is [col0, col1] ascending — (1,2) first.
    // A u128.cmp on the widened PK region would put (2,1) first because
    // col1 dominates the high bytes.
    assert_eq!((p0_col0, p0_col1), (1, 2),
        "compound-PK canonical sort: pk_indices priority, not u128 priority");
    assert_eq!((p1_col0, p1_col1), (2, 1));
}

#[test]
fn test_reduce_group_by_pk_unsorted_signed_pk() {
    use std::rc::Rc;
    use crate::storage::CursorHandle;

    let in_schema = make_schema_i64pk_i64val();
    // Output PK is naturally U128 here too (extends signed encoding via
    // emit_pk on a single-col PK; we only check ordering of payload).
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::I64, 1),
        ],
        &[0],
    );
    let empty_out = Rc::new(Batch::empty(out_schema.num_payload_cols(), 16));
    let mut to_ch = CursorHandle::from_owned(&[empty_out], out_schema);

    // Unsorted signed-PK delta. A u128.cmp on the widened (zero-extended)
    // i64-as-u64 bit pattern would put negatives at the TOP (they widen
    // to large u64 values), so emit order would start with pk=2.
    let delta = make_batch_raw_pk(&in_schema, &[
        (-1, 1, 10),
        (2, 1, 20),
        (-3, 1, 30),
    ], |pk: i64| (pk as u64) as u128);

    let agg = AggDescriptor {
        col_idx: 1, agg_op: AggOp::Sum, col_type_code: TypeCode::I64, _pad: [0; 2],
    };

    let (out, _) = op_reduce(
        &delta, None, to_ch.cursor_mut(),
        &in_schema, &out_schema,
        &[0u32], &[agg],
        None, false, TypeCode::I64, &[], None,
        None, 0, None, None,
    );

    assert_eq!(out.count, 3, "one row per distinct signed PK");
    let pks: Vec<i64> = (0..out.count)
        .map(|i| {
            let bytes = out.get_pk_bytes(i);
            let low = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
            low as i64
        })
        .collect();
    let sums: Vec<i64> = (0..out.count)
        .map(|i| crate::util::read_i64_le(out.col_data(0), i * 8))
        .collect();
    // Canonical signed order: -3, -1, 2.
    assert_eq!(pks, vec![-3, -1, 2],
        "signed PK must sort via i64 order, not u128-of-bits order");
    assert_eq!(sums, vec![30, 10, 20]);
}

#[test]
fn test_reduce_group_by_pk_unsorted_with_retraction() {
    use std::rc::Rc;
    use crate::storage::CursorHandle;

    let in_schema = make_schema_u64_i64();
    let out_schema = make_pk_sum_out_schema();

    // Build a pre-populated trace_out carrying (pk=5, SUM=100).
    let mut prev = Batch::with_schema(out_schema, 1);
    prev.extend_pk(5u128);
    prev.extend_weight(&1i64.to_le_bytes());
    prev.extend_null_bmp(&0u64.to_le_bytes());
    prev.extend_col(0, &100i64.to_le_bytes());
    prev.count += 1;
    prev.sorted = true;
    prev.consolidated = true;
    let prev_rc = Rc::new(prev);
    let mut to_ch = CursorHandle::from_owned(&[prev_rc], out_schema);

    // Unsorted delta with pk=5 split across the batch. Pre-fix: emits
    // TWO `(pk=5, w=-1, SUM=100)` retractions plus split partials.
    let delta = make_batch_raw_pk(&in_schema, &[
        (5, 1, 10),
        (3, 1, 20),
        (5, 1, 30),
    ], |pk: u64| pk as u128);

    let agg = AggDescriptor {
        col_idx: 1, agg_op: AggOp::Sum, col_type_code: TypeCode::I64, _pad: [0; 2],
    };

    let (out, _) = op_reduce(
        &delta, None, to_ch.cursor_mut(),
        &in_schema, &out_schema,
        &[0u32], &[agg],
        None, false, TypeCode::U64, &[], None,
        None, 0, None, None,
    );

    // Expected: one retract (pk=5, w=-1, SUM=100), one emit (pk=5,
    // w=+1, SUM=140), one emit (pk=3, w=+1, SUM=20). Order is canonical:
    // pk=3 first, then pk=5 (retract+emit).
    assert_eq!(out.count, 3, "exactly one retract + one new emit for pk=5 plus pk=3 emit");

    let mut by_pk_w: Vec<(u128, i64, i64)> = (0..out.count)
        .map(|i| {
            let pk = u128::from_le_bytes(out.get_pk_bytes(i).try_into().unwrap());
            let w = out.get_weight(i);
            let sum = crate::util::read_i64_le(out.col_data(0), i * 8);
            (pk, w, sum)
        })
        .collect();
    by_pk_w.sort_by_key(|&(pk, w, _)| (pk, w));

    assert_eq!(by_pk_w, vec![
        (3, 1, 20),
        (5, -1, 100),
        (5, 1, 140),
    ], "single retract+emit for pk=5 (sum 10+30+100=140), single emit for pk=3");
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
    let schema = make_schema_i64pk_i64val();
    let batch = make_batch_raw_pk(&schema, &[
        (-1, 1, 10),
        (2, 1, 20),
        (-3, 1, 30),
    ], |pk: i64| (pk as u64) as u128);
    let sorted = sort_owned(&batch, &schema);

    assert!(sorted.sorted, "sort_owned must set the sorted flag");
    assert_eq!(sorted.count, 3);
    let pks: Vec<i64> = (0..sorted.count)
        .map(|i| {
            let b = sorted.get_pk_bytes(i);
            u64::from_le_bytes(b[0..8].try_into().unwrap()) as i64
        })
        .collect();
    assert_eq!(pks, vec![-3, -1, 2],
        "signed PK rows must come out in signed-ascending order");
}

#[test]
fn test_sort_owned_compound_pk_canonical_order() {
    let schema = make_compound_pk_2xu64_schema();
    let mut batch = make_batch_compound_2xu64(&schema, &[
        (2, 1, 1, 20),
        (1, 2, 1, 10),
    ]);
    batch.sorted = false;
    let sorted = sort_owned(&batch, &schema);

    assert!(sorted.sorted);
    assert_eq!(sorted.count, 2);
    let p0 = sorted.get_pk_bytes(0);
    let p1 = sorted.get_pk_bytes(1);
    let p0_c0 = u64::from_le_bytes(p0[0..8].try_into().unwrap());
    let p0_c1 = u64::from_le_bytes(p0[8..16].try_into().unwrap());
    let p1_c0 = u64::from_le_bytes(p1[0..8].try_into().unwrap());
    let p1_c1 = u64::from_le_bytes(p1[8..16].try_into().unwrap());
    assert_eq!((p0_c0, p0_c1), (1, 2),
        "compound-PK canonical sort follows pk_indices order, not u128 LE byte order");
    assert_eq!((p1_c0, p1_c1), (2, 1));
}

#[test]
fn test_gather_reduce_signed_pk_output_sorted_flag() {
    use std::rc::Rc;
    use crate::storage::CursorHandle;

    // op_gather_reduce's partial schema = output schema. Use a signed PK
    // so the sort_owned path inside must route through make_slow_pk_cmp.
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
        partial.extend_pk((pk as u64) as u128);
        partial.extend_weight(&1i64.to_le_bytes());
        partial.extend_null_bmp(&0u64.to_le_bytes());
        partial.extend_col(0, &sum.to_le_bytes());
        partial.count += 1;
    }
    partial.sorted = false;
    partial.consolidated = false;

    let agg = AggDescriptor {
        col_idx: 1, agg_op: AggOp::Sum, col_type_code: TypeCode::I64, _pad: [0; 2],
    };

    let out = op_gather_reduce(&partial, to_ch.cursor_mut(), &schema, &[agg]);

    assert_eq!(out.count, 3);
    let pks: Vec<i64> = (0..out.count)
        .map(|i| {
            let b = out.get_pk_bytes(i);
            u64::from_le_bytes(b[0..8].try_into().unwrap()) as i64
        })
        .collect();
    assert_eq!(pks, vec![-3, -1, 2],
        "gather-reduce output must be in canonical signed-PK order for output.sorted=true to be truthful");
}
