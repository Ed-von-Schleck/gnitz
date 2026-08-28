use super::*;
use crate::schema::{type_code, SchemaColumn, SchemaDescriptor};
use crate::storage::{Batch, Layout};
use crate::test_support::{
    make_batch, make_batch_bytes, make_batch_i64pk as make_signed_batch, make_schema_i64pk_i64 as make_schema_signed,
    make_schema_pk_u64_payload_blob, make_schema_u64_i64, make_wide_batch, opk_pk, opk_pk_i64, u64_pk_schema,
    wide_pk_3xu64_schema,
};

/// DBSP distinct (set-membership clamp `[-1, 1]`): the named `op_weight_clamp`
/// preset for the unit tests. `w.clamp(-1, 1) == signum(w)` for every integer.
/// Production never calls this — `Instr::WeightClamp` dispatches to
/// [`op_weight_clamp`] directly.
fn op_distinct(delta: Batch, cursor: &mut ReadCursor, schema: &SchemaDescriptor) -> (Batch, Batch) {
    op_weight_clamp(delta, cursor, schema, -1, 1)
}

#[test]
fn test_distinct_update_same_pk() {
    use std::rc::Rc;

    let schema = make_schema_u64_i64();

    // Trace: (PK=1, val=100, w=+1) — a row inserted in a previous tick.
    let trace_batch = Rc::new(make_batch(&schema, &[(1, 1, 100)]));
    let mut cursor_handle = ReadCursor::over_batches(&[trace_batch], schema);

    // Delta: UPDATE PK=1 sets val=100 → 200.
    // _enforce_unique_pk emits (PK=1, val=100, w=-1) and (PK=1, val=200, w=+1).
    // Both rows have the same PK but different payloads; sorted by payload ascending.
    let delta = make_batch(&schema, &[(1, -1, 100), (1, 1, 200)]);

    let (out, _consolidated) = op_distinct(delta, &mut cursor_handle, &schema);

    assert_eq!(
        out.count, 2,
        "expected 2 output rows after same-PK update, got {}",
        out.count
    );

    assert_eq!((out.get_pk(0) as u64), 1);
    let val0 = i64::from_le_bytes(out.col_data(0)[0..8].try_into().unwrap());
    assert_eq!(val0, 100);
    assert_eq!(out.get_weight(0), -1);

    assert_eq!((out.get_pk(1) as u64), 1);
    let val1 = i64::from_le_bytes(out.col_data(0)[8..16].try_into().unwrap());
    assert_eq!(val1, 200);
    assert_eq!(out.get_weight(1), 1);
}

#[test]
fn test_op_distinct_boundary() {
    use crate::storage::ReadCursor;
    use std::rc::Rc;

    let schema = make_schema_u64_i64();

    // Empty trace → all positive deltas emit +1
    let mut ch = crate::storage::empty_cursor(schema);

    // Delta: pk=1 w=+3, pk=2 w=+1
    let delta = make_batch(&schema, &[(1, 3, 10), (2, 1, 20)]);
    let (out, _) = op_distinct(delta, &mut ch, &schema);
    // 0→positive: both emit +1
    assert_eq!(out.count, 2);
    assert_eq!(out.get_weight(0), 1);
    assert_eq!(out.get_weight(1), 1);

    // Now trace has pk=1 w=3 and pk=2 w=1
    // Delta: pk=1 w=-2 (3→1, still positive, no output), pk=2 w=-1 (1→0, emit -1)
    let trace_batch = Rc::new(make_batch(&schema, &[(1, 3, 10), (2, 1, 20)]));
    let mut ch2 = ReadCursor::over_batches(&[trace_batch], schema);
    let delta2 = make_batch(&schema, &[(1, -2, 10), (2, -1, 20)]);
    let (out2, _) = op_distinct(delta2, &mut ch2, &schema);
    // pk=1: 3→1, positive→positive, no change
    // pk=2: 1→0, positive→non-positive, emit -1
    assert_eq!(out2.count, 1);
    assert_eq!((out2.get_pk(0) as u64), 2);
    assert_eq!(out2.get_weight(0), -1);
}

/// `positive_part` boundary behavior: the same clamp body as `distinct`, but
/// with bounds `{0, i64::MAX}` (bag multiplicity — clamp the negative part
/// only). Integral 5; ticks +3, −10, +4 emit the clamped deltas
/// `max(0,8)−max(0,5)=+3`, `max(0,−2)−max(0,8)=−8`, `max(0,2)−max(0,−2)=+2`.
/// Tick 3's pre-image integral is **negative** (−2) — the case no set-op
/// `distinct` exercises — and must still emit the correct +2.
#[test]
fn test_op_positive_part_boundary() {
    use crate::storage::ReadCursor;
    use std::rc::Rc;

    let schema = make_schema_u64_i64();

    // Tick 1: integral 5, delta +3 → w_new 8 → emit max(0,8)−max(0,5)=+3.
    let trace1 = Rc::new(make_batch(&schema, &[(1, 5, 10)]));
    let mut ch1 = ReadCursor::over_batches(&[trace1], schema);
    let (out1, _) = op_weight_clamp(make_batch(&schema, &[(1, 3, 10)]), &mut ch1, &schema, 0, i64::MAX);
    assert_eq!(out1.count, 1);
    assert_eq!(out1.get_weight(0), 3, "max(0,8) - max(0,5) = +3");

    // Tick 2: integral 8, delta −10 → w_new −2 → emit max(0,−2)−max(0,8)=−8.
    let trace2 = Rc::new(make_batch(&schema, &[(1, 8, 10)]));
    let mut ch2 = ReadCursor::over_batches(&[trace2], schema);
    let (out2, _) = op_weight_clamp(make_batch(&schema, &[(1, -10, 10)]), &mut ch2, &schema, 0, i64::MAX);
    assert_eq!(out2.count, 1);
    assert_eq!(out2.get_weight(0), -8, "max(0,-2) - max(0,8) = -8");

    // Tick 3: integral −2 (net-negative), delta +4 → w_new 2 → emit
    // max(0,2)−max(0,−2)=+2. A negative pre-image clamps to 0, so the row
    // re-enters the bag at exactly its positive part.
    let trace3 = Rc::new(make_batch(&schema, &[(1, -2, 10)]));
    let mut ch3 = ReadCursor::over_batches(&[trace3], schema);
    let (out3, _) = op_weight_clamp(make_batch(&schema, &[(1, 4, 10)]), &mut ch3, &schema, 0, i64::MAX);
    assert_eq!(out3.count, 1);
    assert_eq!(out3.get_weight(0), 2, "max(0,2) - max(0,-2) = +2");
}

/// Several payloads at one PK, exercising the (PK, payload) sub-merge inside
/// the `cogroup_left` group: a retraction-to-zero (emit -1), a no-op bump
/// (positive → positive), and a brand-new payload (emit +1) — all in one PK
/// group, walked against a multi-payload trace group in lockstep.
#[test]
fn test_distinct_multi_payload_group_submerge() {
    use crate::storage::ReadCursor;
    use std::rc::Rc;

    let schema = make_schema_u64_i64();
    // Trace PK=1 carries payloads 10, 20, 30 (each weight 1).
    let trace = Rc::new(make_batch(&schema, &[(1, 1, 10), (1, 1, 20), (1, 1, 30)]));
    let mut ch = ReadCursor::over_batches(&[trace], schema);

    // Delta at PK=1: retract 10 (1→0 ⇒ -1), bump 20 (1→2 ⇒ no change),
    // add new 40 (0→1 ⇒ +1). Payload 30 is untouched.
    let delta = make_batch(&schema, &[(1, -1, 10), (1, 1, 20), (1, 1, 40)]);
    let (out, _) = op_distinct(delta, &mut ch, &schema);

    assert_eq!(out.count, 2, "only the 10-retract and 40-insert transition");
    let payload = |r: usize| gnitz_wire::read_i64_le(out.col_data(0), r * 8);
    assert_eq!((payload(0), out.get_weight(0)), (10, -1));
    assert_eq!((payload(1), out.get_weight(1)), (40, 1));
}

fn make_batch_narrow<const N: usize>(schema: &SchemaDescriptor, rows: &[(u64, i64, i64)]) -> Batch {
    let n = rows.len();
    let mut b = Batch::with_capacity(*schema, n.max(1));
    let col_size = N;
    for &(pk, w, val) in rows {
        b.extend_pk(pk as u128);
        b.extend_weight(&w.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &val.to_le_bytes()[..col_size]);
        b.count += 1;
    }
    b.certify_layout(Layout::Consolidated, schema);
    b
}

// Regression tests for the payload comparator with sub-64-bit
// integer payload columns. Previously panicked on try_into().unwrap() because
// the slice had fewer than 8 bytes.

#[test]
fn test_distinct_i32_payload_no_panic() {
    use crate::storage::ReadCursor;
    use std::rc::Rc;

    let schema = u64_pk_schema(type_code::I32);
    let trace = Rc::new(make_batch_narrow::<4>(&schema, &[(1, 1, 42)]));
    let mut ch = ReadCursor::over_batches(&[trace], schema);

    // Delta: same (PK=1, val=42) → stays +1 → no output
    let delta = make_batch_narrow::<4>(&schema, &[(1, 1, 42)]);
    let (out, _) = op_distinct(delta, &mut ch, &schema);
    assert_eq!(out.count, 0, "I32: matching (PK,payload) should produce no output");

    // New (PK=1, val=99) → new element → +1 output
    let mut ch2 = ReadCursor::over_batches(&[Rc::new(make_batch_narrow::<4>(&schema, &[(1, 1, 42)]))], schema);
    let delta2 = make_batch_narrow::<4>(&schema, &[(1, 1, 99)]);
    let (out2, _) = op_distinct(delta2, &mut ch2, &schema);
    assert_eq!(out2.count, 1, "I32: new (PK,payload) should produce +1");
}

#[test]
fn test_distinct_i16_payload_no_panic() {
    use crate::storage::ReadCursor;
    use std::rc::Rc;

    let schema = u64_pk_schema(type_code::I16);
    let trace = Rc::new(make_batch_narrow::<2>(&schema, &[(5, 1, -100)]));
    let mut ch = ReadCursor::over_batches(&[trace], schema);

    let delta = make_batch_narrow::<2>(&schema, &[(5, 1, -100)]);
    let (out, _) = op_distinct(delta, &mut ch, &schema);
    assert_eq!(out.count, 0, "I16: matching (PK,payload) should produce no output");
}

#[test]
fn test_distinct_i8_payload_no_panic() {
    use crate::storage::ReadCursor;
    use std::rc::Rc;

    let schema = u64_pk_schema(type_code::I8);
    let trace = Rc::new(make_batch_narrow::<1>(&schema, &[(7, 1, -1)]));
    let mut ch = ReadCursor::over_batches(&[trace], schema);

    let delta = make_batch_narrow::<1>(&schema, &[(7, 1, -1)]);
    let (out, _) = op_distinct(delta, &mut ch, &schema);
    assert_eq!(out.count, 0, "I8: matching (PK,payload) should produce no output");
}

/// Regression: a non-null BLOB payload column previously panicked. The
/// (non-null, non-null) payload-compare arm routed BLOB to the fixed-width
/// comparator (`cmp_typed_le`), which `unreachable!`s on the 16-byte string
/// width. BLOB shares the German-string layout and must dispatch through
/// `compare_german_strings` like STRING.
#[test]
fn test_distinct_blob_payload_no_panic() {
    use crate::storage::ReadCursor;
    use std::rc::Rc;

    let schema = make_schema_pk_u64_payload_blob();

    // Equal (PK=1, "hi") on both sides → compare returns Equal → no output.
    let trace = Rc::new(make_batch_bytes(&schema, &[(1, 1, b"hi")]));
    let mut ch = ReadCursor::over_batches(&[trace], schema);
    let delta = make_batch_bytes(&schema, &[(1, 1, b"hi")]);
    let (out, _) = op_distinct(delta, &mut ch, &schema);
    assert_eq!(out.count, 0, "BLOB: matching (PK,payload) should produce no output");

    // A different blob at the same PK is a distinct element → +1.
    let trace2 = Rc::new(make_batch_bytes(&schema, &[(1, 1, b"hi")]));
    let mut ch2 = ReadCursor::over_batches(&[trace2], schema);
    let delta2 = make_batch_bytes(&schema, &[(1, 1, b"bye")]);
    let (out2, _) = op_distinct(delta2, &mut ch2, &schema);
    assert_eq!(out2.count, 1, "BLOB: a new payload at an existing PK emits +1");
}

fn make_schema_compound() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0, 1],
    )
}

fn make_compound_batch(schema: &SchemaDescriptor, rows: &[(u64, u64, i64, i64)]) -> Batch {
    let mut b = Batch::with_capacity(*schema, rows.len().max(1));
    for &(c0, c1, w, val) in rows {
        b.extend_pk_bytes(&opk_pk(schema, &[c0 as u128, c1 as u128]));
        b.extend_weight(&w.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &val.to_le_bytes());
        b.count += 1;
    }
    b.certify_layout(Layout::Consolidated, schema);
    b
}

/// With a compound PK the raw-u128 order is last-column-major, so the trace
/// seek must go by bytes to find an existing element. A delta that re-adds
/// an element already in the trace must net to no output (not emit `+1`).
#[test]
fn test_distinct_compound_pk_finds_existing() {
    use crate::storage::ReadCursor;
    use std::rc::Rc;

    let schema = make_schema_compound();
    // Trace in storage order: (1,5) then (2,3).
    let trace = Rc::new(make_compound_batch(&schema, &[(1, 5, 1, 100), (2, 3, 1, 200)]));
    let mut ch = ReadCursor::over_batches(&[trace], schema);

    // Re-add (2,3) with the same payload → already present → no output.
    let delta = make_compound_batch(&schema, &[(2, 3, 1, 200)]);
    let (out, _) = op_distinct(delta, &mut ch, &schema);
    assert_eq!(
        out.count, 0,
        "compound: re-adding an existing element must net to no output"
    );

    // Adding a genuinely new (2,3) payload IS a new element → +1.
    let trace2 = Rc::new(make_compound_batch(&schema, &[(1, 5, 1, 100), (2, 3, 1, 200)]));
    let mut ch2 = ReadCursor::over_batches(&[trace2], schema);
    let delta2 = make_compound_batch(&schema, &[(2, 3, 1, 999)]);
    let (out2, _) = op_distinct(delta2, &mut ch2, &schema);
    assert_eq!(out2.count, 1, "compound: a new payload at an existing PK emits +1");
    assert_eq!(out2.get_pk_bytes(0), opk_pk(&schema, &[2, 3]).as_slice());
    assert_eq!(out2.get_weight(0), 1);
}

/// Signed single-column PK: negatives sort first in storage but last in raw
/// u128. The trace seek to a negative key must find the existing element.
#[test]
fn test_distinct_signed_pk_finds_existing() {
    use crate::storage::ReadCursor;
    use std::rc::Rc;

    let schema = make_schema_signed();
    // Storage (signed) order: -3, -1, 2.
    let trace = Rc::new(make_signed_batch(&schema, &[(-3, 1, 30), (-1, 1, 10), (2, 1, 20)]));
    let mut ch = ReadCursor::over_batches(&[trace], schema);

    // Re-add (-1) with the same payload → already present → no output.
    let delta = make_signed_batch(&schema, &[(-1, 1, 10)]);
    let (out, _) = op_distinct(delta, &mut ch, &schema);
    assert_eq!(
        out.count, 0,
        "signed: re-adding an existing element must net to no output"
    );

    // Retract (-1) fully → element leaves the set → -1.
    let trace2 = Rc::new(make_signed_batch(&schema, &[(-3, 1, 30), (-1, 1, 10), (2, 1, 20)]));
    let mut ch2 = ReadCursor::over_batches(&[trace2], schema);
    let delta2 = make_signed_batch(&schema, &[(-1, -1, 10)]);
    let (out2, _) = op_distinct(delta2, &mut ch2, &schema);
    assert_eq!(out2.count, 1, "signed: fully retracting an element emits -1");
    assert_eq!(opk_pk_i64(out2.get_pk_bytes(0)), -1);
    assert_eq!(out2.get_weight(0), -1);
}

#[test]
fn test_op_distinct_consolidated_flag() {
    let schema = make_schema_u64_i64();
    let mut ch = crate::storage::empty_cursor(schema);

    let delta = make_batch(&schema, &[(1, 1, 10)]);
    let (out, consolidated) = op_distinct(delta, &mut ch, &schema);
    assert!(out.is_consolidated(), "distinct output must be consolidated");
    assert!(out.is_sorted(), "distinct output must be sorted");
    assert!(
        consolidated.is_consolidated(),
        "consolidated output must be consolidated"
    );
}

// -----------------------------------------------------------------------
// Wide-PK distinct tests
// -----------------------------------------------------------------------

#[test]
fn test_distinct_wide_pk_empty_trace_three_new_rows() {
    // Trace empty; delta has three wide-PK rows with distinct PKs.
    // All three must emit +1.
    let schema = wide_pk_3xu64_schema();
    let mut ch = crate::storage::empty_cursor(schema);

    let delta = make_wide_batch(&schema, &[(1, 0, 0, 1, 10), (2, 0, 0, 1, 20), (3, 0, 0, 1, 30)]);
    let (out, _) = op_distinct(delta, &mut ch, &schema);
    assert_eq!(out.count, 3, "three new wide-PK rows must each emit +1");
    for i in 0..3 {
        assert_eq!(out.get_weight(i), 1);
    }
}

#[test]
fn test_distinct_wide_pk_already_in_trace() {
    use crate::storage::ReadCursor;
    use std::rc::Rc;
    // Trace has (1,0,0, payload=99, w=1). Delta re-adds same (PK, payload).
    // Already in set → output must be empty.
    let schema = wide_pk_3xu64_schema();
    let trace = Rc::new(make_wide_batch(&schema, &[(1, 0, 0, 1, 99)]));
    let mut ch = ReadCursor::over_batches(&[trace], schema);

    let delta = make_wide_batch(&schema, &[(1, 0, 0, 1, 99)]);
    let (out, _) = op_distinct(delta, &mut ch, &schema);
    assert_eq!(
        out.count, 0,
        "re-adding an existing (PK,payload) must produce no output"
    );
}

#[test]
fn test_distinct_wide_pk_prefix_collision() {
    use crate::storage::ReadCursor;
    use std::rc::Rc;
    // Two wide-PK rows with the same 16-byte OPK prefix (c0=1,c1=1) but
    // differing in c2. One row in trace (w=1), one new row in delta (w=+1).
    // The row in the trace must not emit; the new row must emit +1.
    // This tests the cursor.current_pk_bytes() != key break condition.
    let schema = wide_pk_3xu64_schema();
    // (1,1,0) is already in the trace
    let trace = Rc::new(make_wide_batch(&schema, &[(1, 1, 0, 1, 50)]));
    let mut ch = ReadCursor::over_batches(&[trace], schema);

    // Delta has the NEW key (1,1, 1<<56) which shares 16 OPK bytes with (1,1,0)
    let c2_new = 1u64 << 56;
    let delta = make_wide_batch(&schema, &[(1, 1, c2_new, 1, 60)]);
    let (out, _) = op_distinct(delta, &mut ch, &schema);
    // The new row is not in the trace → emit +1. The old row is not in delta.
    assert_eq!(out.count, 1, "prefix-collision new row must emit +1");
    assert_eq!(out.get_weight(0), 1);
}

#[test]
fn test_distinct_u128_max_sentinel_bug() {
    use crate::storage::ReadCursor;
    use std::rc::Rc;
    // Single-column U128 PK schema. Trace has (u128::MAX, payload, w=1).
    // Delta re-adds the same (u128::MAX, payload, w=+1). The old sentinel bug
    // (prev_key = u128::MAX) would skip the seek and compute w_old = 0,
    // spuriously emitting +1. The fixed path uses Option<&[u8]>.
    let schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0u32],
    );
    let max_pk = u128::MAX;
    let mut trace_b = Batch::with_capacity(schema, 1);
    trace_b.extend_pk(max_pk);
    trace_b.extend_weight(&1i64.to_le_bytes());
    trace_b.extend_null_bmp(&0u64.to_le_bytes());
    trace_b.extend_col(0, &42i64.to_le_bytes());
    trace_b.count += 1;
    trace_b.certify_layout(Layout::Consolidated, &schema);

    let trace = Rc::new(trace_b);
    let mut ch = ReadCursor::over_batches(&[trace], schema);

    let mut delta = Batch::with_capacity(schema, 1);
    delta.extend_pk(max_pk);
    delta.extend_weight(&1i64.to_le_bytes());
    delta.extend_null_bmp(&0u64.to_le_bytes());
    delta.extend_col(0, &42i64.to_le_bytes());
    delta.count += 1;
    delta.certify_layout(Layout::Consolidated, &schema);

    let (out, _) = op_distinct(delta, &mut ch, &schema);
    assert_eq!(
        out.count, 0,
        "u128::MAX PK re-add must produce no output (sentinel bug regression)"
    );
}
