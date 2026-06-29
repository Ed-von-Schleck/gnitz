//! DBSP distinct operator.

use crate::schema::SchemaDescriptor;
use crate::storage::{scatter_copy, write_to_batch, Batch, Layout, ReadCursor};

use super::cogroup::cogroup_left;
use super::util::compare_cursor_payload_to_batch_row;

// ---------------------------------------------------------------------------
// Distinct
// ---------------------------------------------------------------------------

/// DBSP distinct (set-membership clamp `[-1, 1]`): the named `op_weight_clamp`
/// preset for the unit tests. `w.clamp(-1, 1) == signum(w)` for every integer.
/// Production never calls this — `Instr::WeightClamp` dispatches to
/// [`op_weight_clamp`] directly — so it is `#[cfg(test)]`.
#[cfg(test)]
pub fn op_distinct(delta: Batch, cursor: &mut ReadCursor, schema: &SchemaDescriptor) -> (Batch, Batch) {
    op_weight_clamp(delta, cursor, schema, -1, 1)
}

/// Shared body for the two weight-clamp operators. Per consolidated (PK, payload)
/// emits `clamp(w_old + Δw, lo, hi) − clamp(w_old, lo, hi)`; the `(lo, hi)` preset
/// selects the operator:
///
/// * `(-1, 1)` → `distinct` (set membership — `clamp(w, -1, 1) == signum(w)`),
/// * `(0, i64::MAX)` → `positive_part` (bag multiplicity, negative part only).
///
/// Both are the DBSP incremental form of a per-element weight clamp lifted to its
/// delta. Returns `(output_batch, consolidated_delta)`; the consolidated delta is
/// returned so the caller can feed it to `ingest_batch`.
pub fn op_weight_clamp(
    delta: Batch,
    cursor: &mut ReadCursor,
    schema: &SchemaDescriptor,
    lo: i64,
    hi: i64,
) -> (Batch, Batch) {
    // 1. Consolidate delta
    let consolidated = delta.into_consolidated(schema);
    let n = consolidated.count;
    if n == 0 {
        return (Batch::empty_with_schema(schema), consolidated);
    }

    // 2. Co-group the consolidated delta against the integral trace on PK, then
    //    run a (PK, payload) sub-merge inside each group: for each delta element
    //    fold the byte-equal trace row's weight and compute the clamped change
    //    clamp(w_old + w_delta) − clamp(w_old). `cogroup_left` visits every
    //    delta group (every element transitions or not); the inner payload merge
    //    walks the delta sub-range and the trace PK group in lockstep, both
    //    being (PK, payload)-sorted, so the per-element trace probe is the
    //    monotone forward walk the old per-row `seek_bytes` open-coded.
    let mut emit_indices: Vec<u32> = Vec::with_capacity(n);
    let mut emit_weights: Vec<i64> = Vec::with_capacity(n);

    let consolidated_mb = consolidated.as_mem_batch();

    cogroup_left(&consolidated, cursor, |key, range, m| {
        for i in range {
            let w_old: i64 = loop {
                if !m.valid || !m.current_pk_eq(key) {
                    break 0;
                }
                match compare_cursor_payload_to_batch_row(m, &consolidated_mb, i, schema) {
                    std::cmp::Ordering::Less => {
                        m.advance();
                    }
                    std::cmp::Ordering::Equal => {
                        let w = m.current_weight;
                        m.advance();
                        break w;
                    }
                    std::cmp::Ordering::Greater => break 0,
                }
            };

            let w_new = w_old.wrapping_add(consolidated.get_weight(i));
            let out_w = w_new.clamp(lo, hi) - w_old.clamp(lo, hi);
            if out_w != 0 {
                emit_indices.push(i as u32);
                emit_weights.push(out_w);
            }
        }
    });

    // 3. Scatter-copy emitting rows
    if emit_indices.is_empty() {
        return (Batch::empty_with_schema(schema), consolidated);
    }

    let mb = consolidated.as_mem_batch();
    let blob_cap = mb.blob.len().max(1);
    let mut output = write_to_batch(schema, emit_indices.len(), blob_cap, |writer| {
        scatter_copy(&mb, &emit_indices, &emit_weights, writer);
    });
    // Emitting rows are scattered in consolidated-delta order (ascending indices),
    // one per transitioning element ⇒ (PK, payload)-sorted and ghost-free.
    output.certify_layout(Layout::Consolidated, schema);

    (output, consolidated)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{type_code, SchemaColumn, SchemaDescriptor};
    use crate::storage::{Batch, Layout};
    use crate::test_support::{make_wide_batch, wide_pk_3xu64_schema};

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
        b.certify_layout(Layout::Consolidated, schema);
        b
    }

    #[test]
    fn test_op_distinct_boundary() {
        use crate::storage::CursorHandle;
        use std::rc::Rc;

        let schema = make_schema_u64_i64();

        // Empty trace → all positive deltas emit +1
        let empty = Rc::new(Batch::empty(1, 16));
        let mut ch = CursorHandle::from_owned(std::slice::from_ref(&empty), schema);

        // Delta: pk=1 w=+3, pk=2 w=+1
        let delta = make_batch(&schema, &[(1, 3, 10), (2, 1, 20)]);
        let (out, _) = op_distinct(delta, ch.cursor_mut(), &schema);
        // 0→positive: both emit +1
        assert_eq!(out.count, 2);
        assert_eq!(out.get_weight(0), 1);
        assert_eq!(out.get_weight(1), 1);

        // Now trace has pk=1 w=3 and pk=2 w=1
        // Delta: pk=1 w=-2 (3→1, still positive, no output), pk=2 w=-1 (1→0, emit -1)
        let trace_batch = Rc::new(make_batch(&schema, &[(1, 3, 10), (2, 1, 20)]));
        let mut ch2 = CursorHandle::from_owned(&[trace_batch], schema);
        let delta2 = make_batch(&schema, &[(1, -2, 10), (2, -1, 20)]);
        let (out2, _) = op_distinct(delta2, ch2.cursor_mut(), &schema);
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
        use crate::storage::CursorHandle;
        use std::rc::Rc;

        let schema = make_schema_u64_i64();

        // Tick 1: integral 5, delta +3 → w_new 8 → emit max(0,8)−max(0,5)=+3.
        let trace1 = Rc::new(make_batch(&schema, &[(1, 5, 10)]));
        let mut ch1 = CursorHandle::from_owned(&[trace1], schema);
        let (out1, _) = op_weight_clamp(
            make_batch(&schema, &[(1, 3, 10)]),
            ch1.cursor_mut(),
            &schema,
            0,
            i64::MAX,
        );
        assert_eq!(out1.count, 1);
        assert_eq!(out1.get_weight(0), 3, "max(0,8) - max(0,5) = +3");

        // Tick 2: integral 8, delta −10 → w_new −2 → emit max(0,−2)−max(0,8)=−8.
        let trace2 = Rc::new(make_batch(&schema, &[(1, 8, 10)]));
        let mut ch2 = CursorHandle::from_owned(&[trace2], schema);
        let (out2, _) = op_weight_clamp(
            make_batch(&schema, &[(1, -10, 10)]),
            ch2.cursor_mut(),
            &schema,
            0,
            i64::MAX,
        );
        assert_eq!(out2.count, 1);
        assert_eq!(out2.get_weight(0), -8, "max(0,-2) - max(0,8) = -8");

        // Tick 3: integral −2 (net-negative), delta +4 → w_new 2 → emit
        // max(0,2)−max(0,−2)=+2. A negative pre-image clamps to 0, so the row
        // re-enters the bag at exactly its positive part.
        let trace3 = Rc::new(make_batch(&schema, &[(1, -2, 10)]));
        let mut ch3 = CursorHandle::from_owned(&[trace3], schema);
        let (out3, _) = op_weight_clamp(
            make_batch(&schema, &[(1, 4, 10)]),
            ch3.cursor_mut(),
            &schema,
            0,
            i64::MAX,
        );
        assert_eq!(out3.count, 1);
        assert_eq!(out3.get_weight(0), 2, "max(0,2) - max(0,-2) = +2");
    }

    /// Several payloads at one PK, exercising the (PK, payload) sub-merge inside
    /// the `cogroup_left` group: a retraction-to-zero (emit -1), a no-op bump
    /// (positive → positive), and a brand-new payload (emit +1) — all in one PK
    /// group, walked against a multi-payload trace group in lockstep.
    #[test]
    fn test_distinct_multi_payload_group_submerge() {
        use crate::storage::CursorHandle;
        use std::rc::Rc;

        let schema = make_schema_u64_i64();
        // Trace PK=1 carries payloads 10, 20, 30 (each weight 1).
        let trace = Rc::new(make_batch(&schema, &[(1, 1, 10), (1, 1, 20), (1, 1, 30)]));
        let mut ch = CursorHandle::from_owned(&[trace], schema);

        // Delta at PK=1: retract 10 (1→0 ⇒ -1), bump 20 (1→2 ⇒ no change),
        // add new 40 (0→1 ⇒ +1). Payload 30 is untouched.
        let delta = make_batch(&schema, &[(1, -1, 10), (1, 1, 20), (1, 1, 40)]);
        let (out, _) = op_distinct(delta, ch.cursor_mut(), &schema);

        assert_eq!(out.count, 2, "only the 10-retract and 40-insert transition");
        let payload = |r: usize| crate::foundation::codec::read_i64_le(out.col_data(0), r * 8);
        assert_eq!((payload(0), out.get_weight(0)), (10, -1));
        assert_eq!((payload(1), out.get_weight(1)), (40, 1));
    }

    fn make_schema_u64_i32() -> SchemaDescriptor {
        SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::I32, 0),
            ],
            &[0],
        )
    }

    fn make_schema_u64_i16() -> SchemaDescriptor {
        SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::I16, 0),
            ],
            &[0],
        )
    }

    fn make_schema_u64_i8() -> SchemaDescriptor {
        SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::I8, 0),
            ],
            &[0],
        )
    }

    fn make_batch_narrow<const N: usize>(schema: &SchemaDescriptor, rows: &[(u64, i64, i64)]) -> Batch {
        let n = rows.len();
        let mut b = Batch::with_schema(*schema, n.max(1));
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

    // Regression tests for compare_cursor_payload_to_batch_row with sub-64-bit
    // integer payload columns. Previously panicked on try_into().unwrap() because
    // the slice had fewer than 8 bytes.

    #[test]
    fn test_distinct_i32_payload_no_panic() {
        use crate::storage::CursorHandle;
        use std::rc::Rc;

        let schema = make_schema_u64_i32();
        let trace = Rc::new(make_batch_narrow::<4>(&schema, &[(1, 1, 42)]));
        let mut ch = CursorHandle::from_owned(&[trace], schema);

        // Delta: same (PK=1, val=42) → stays +1 → no output
        let delta = make_batch_narrow::<4>(&schema, &[(1, 1, 42)]);
        let (out, _) = op_distinct(delta, ch.cursor_mut(), &schema);
        assert_eq!(out.count, 0, "I32: matching (PK,payload) should produce no output");

        // New (PK=1, val=99) → new element → +1 output
        let mut ch2 = CursorHandle::from_owned(&[Rc::new(make_batch_narrow::<4>(&schema, &[(1, 1, 42)]))], schema);
        let delta2 = make_batch_narrow::<4>(&schema, &[(1, 1, 99)]);
        let (out2, _) = op_distinct(delta2, ch2.cursor_mut(), &schema);
        assert_eq!(out2.count, 1, "I32: new (PK,payload) should produce +1");
    }

    #[test]
    fn test_distinct_i16_payload_no_panic() {
        use crate::storage::CursorHandle;
        use std::rc::Rc;

        let schema = make_schema_u64_i16();
        let trace = Rc::new(make_batch_narrow::<2>(&schema, &[(5, 1, -100)]));
        let mut ch = CursorHandle::from_owned(&[trace], schema);

        let delta = make_batch_narrow::<2>(&schema, &[(5, 1, -100)]);
        let (out, _) = op_distinct(delta, ch.cursor_mut(), &schema);
        assert_eq!(out.count, 0, "I16: matching (PK,payload) should produce no output");
    }

    #[test]
    fn test_distinct_i8_payload_no_panic() {
        use crate::storage::CursorHandle;
        use std::rc::Rc;

        let schema = make_schema_u64_i8();
        let trace = Rc::new(make_batch_narrow::<1>(&schema, &[(7, 1, -1)]));
        let mut ch = CursorHandle::from_owned(&[trace], schema);

        let delta = make_batch_narrow::<1>(&schema, &[(7, 1, -1)]);
        let (out, _) = op_distinct(delta, ch.cursor_mut(), &schema);
        assert_eq!(out.count, 0, "I8: matching (PK,payload) should produce no output");
    }

    fn make_schema_u64_blob() -> SchemaDescriptor {
        SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::BLOB, 0),
            ],
            &[0],
        )
    }

    /// Build a batch with one short (inline, ≤12-byte) BLOB payload value per row.
    fn make_batch_blob(schema: &SchemaDescriptor, rows: &[(u64, i64, &[u8])]) -> Batch {
        let mut b = Batch::with_schema(*schema, rows.len().max(1));
        for &(pk, w, val) in rows {
            assert!(val.len() <= 12, "test helper only supports inline (short) blobs");
            let mut cell = [0u8; 16];
            cell[0..4].copy_from_slice(&(val.len() as u32).to_le_bytes());
            cell[4..4 + val.len()].copy_from_slice(val);
            b.extend_pk(pk as u128);
            b.extend_weight(&w.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &cell);
            b.count += 1;
        }
        b.certify_layout(Layout::Consolidated, schema);
        b
    }

    /// Regression: a non-null BLOB payload column previously panicked. The
    /// (non-null, non-null) payload-compare arm routed BLOB to the fixed-width
    /// comparator (`cmp_typed_le`), which `unreachable!`s on the 16-byte string
    /// width. BLOB shares the German-string layout and must dispatch through
    /// `compare_german_strings` like STRING.
    #[test]
    fn test_distinct_blob_payload_no_panic() {
        use crate::storage::CursorHandle;
        use std::rc::Rc;

        let schema = make_schema_u64_blob();

        // Equal (PK=1, "hi") on both sides → compare returns Equal → no output.
        let trace = Rc::new(make_batch_blob(&schema, &[(1, 1, b"hi")]));
        let mut ch = CursorHandle::from_owned(&[trace], schema);
        let delta = make_batch_blob(&schema, &[(1, 1, b"hi")]);
        let (out, _) = op_distinct(delta, ch.cursor_mut(), &schema);
        assert_eq!(out.count, 0, "BLOB: matching (PK,payload) should produce no output");

        // A different blob at the same PK is a distinct element → +1.
        let trace2 = Rc::new(make_batch_blob(&schema, &[(1, 1, b"hi")]));
        let mut ch2 = CursorHandle::from_owned(&[trace2], schema);
        let delta2 = make_batch_blob(&schema, &[(1, 1, b"bye")]);
        let (out2, _) = op_distinct(delta2, ch2.cursor_mut(), &schema);
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

    /// Pack a compound `(U64, U64)` PK into its 16-byte region: col0 then col1,
    /// each little-endian — the layout storage stores and orders by.
    fn compound_pk_bytes(c0: u64, c1: u64) -> [u8; 16] {
        let mut b = [0u8; 16];
        b[0..8].copy_from_slice(&c0.to_le_bytes());
        b[8..16].copy_from_slice(&c1.to_le_bytes());
        b
    }

    fn make_compound_batch(schema: &SchemaDescriptor, rows: &[(u64, u64, i64, i64)]) -> Batch {
        let mut b = Batch::with_schema(*schema, rows.len().max(1));
        for &(c0, c1, w, val) in rows {
            b.extend_pk_bytes(&compound_pk_bytes(c0, c1));
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
        use crate::storage::CursorHandle;
        use std::rc::Rc;

        let schema = make_schema_compound();
        // Trace in storage order: (1,5) then (2,3).
        let trace = Rc::new(make_compound_batch(&schema, &[(1, 5, 1, 100), (2, 3, 1, 200)]));
        let mut ch = CursorHandle::from_owned(&[trace], schema);

        // Re-add (2,3) with the same payload → already present → no output.
        let delta = make_compound_batch(&schema, &[(2, 3, 1, 200)]);
        let (out, _) = op_distinct(delta, ch.cursor_mut(), &schema);
        assert_eq!(
            out.count, 0,
            "compound: re-adding an existing element must net to no output"
        );

        // Adding a genuinely new (2,3) payload IS a new element → +1.
        let trace2 = Rc::new(make_compound_batch(&schema, &[(1, 5, 1, 100), (2, 3, 1, 200)]));
        let mut ch2 = CursorHandle::from_owned(&[trace2], schema);
        let delta2 = make_compound_batch(&schema, &[(2, 3, 1, 999)]);
        let (out2, _) = op_distinct(delta2, ch2.cursor_mut(), &schema);
        assert_eq!(out2.count, 1, "compound: a new payload at an existing PK emits +1");
        assert_eq!(out2.get_pk_bytes(0), &compound_pk_bytes(2, 3));
        assert_eq!(out2.get_weight(0), 1);
    }

    fn make_schema_signed() -> SchemaDescriptor {
        SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::I64, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0],
        )
    }

    fn make_signed_batch(schema: &SchemaDescriptor, rows: &[(i64, i64, i64)]) -> Batch {
        let mut b = Batch::with_schema(*schema, rows.len().max(1));
        for &(pk, w, val) in rows {
            b.extend_pk_opk(schema, &[(pk as u64) as u128]);
            b.extend_weight(&w.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &val.to_le_bytes());
            b.count += 1;
        }
        b.certify_layout(Layout::Consolidated, schema);
        b
    }

    /// Signed single-column PK: negatives sort first in storage but last in raw
    /// u128. The trace seek to a negative key must find the existing element.
    #[test]
    fn test_distinct_signed_pk_finds_existing() {
        use crate::storage::CursorHandle;
        use std::rc::Rc;

        let schema = make_schema_signed();
        // Storage (signed) order: -3, -1, 2.
        let trace = Rc::new(make_signed_batch(&schema, &[(-3, 1, 30), (-1, 1, 10), (2, 1, 20)]));
        let mut ch = CursorHandle::from_owned(&[trace], schema);

        // Re-add (-1) with the same payload → already present → no output.
        let delta = make_signed_batch(&schema, &[(-1, 1, 10)]);
        let (out, _) = op_distinct(delta, ch.cursor_mut(), &schema);
        assert_eq!(
            out.count, 0,
            "signed: re-adding an existing element must net to no output"
        );

        // Retract (-1) fully → element leaves the set → -1.
        let trace2 = Rc::new(make_signed_batch(&schema, &[(-3, 1, 30), (-1, 1, 10), (2, 1, 20)]));
        let mut ch2 = CursorHandle::from_owned(&[trace2], schema);
        let delta2 = make_signed_batch(&schema, &[(-1, -1, 10)]);
        let (out2, _) = op_distinct(delta2, ch2.cursor_mut(), &schema);
        assert_eq!(out2.count, 1, "signed: fully retracting an element emits -1");
        let mut le = [0u8; 8];
        gnitz_wire::decode_pk_column(&out2.get_pk_bytes(0)[..8], type_code::I64, &mut le);
        assert_eq!(i64::from_le_bytes(le), -1);
        assert_eq!(out2.get_weight(0), -1);
    }

    #[test]
    fn test_op_distinct_consolidated_flag() {
        use crate::storage::CursorHandle;
        use std::rc::Rc;

        let schema = make_schema_u64_i64();
        let empty = Rc::new(Batch::empty(1, 16));
        let mut ch = CursorHandle::from_owned(&[empty], schema);

        let delta = make_batch(&schema, &[(1, 1, 10)]);
        let (out, consolidated) = op_distinct(delta, ch.cursor_mut(), &schema);
        assert!(out.is_consolidated(), "distinct output must be consolidated");
        assert!(out.is_sorted(), "distinct output must be sorted");
        assert!(
            consolidated.is_consolidated(),
            "consolidated output must be consolidated"
        );
    }

    // -----------------------------------------------------------------------
    // Wide-PK distinct tests (§8)
    // -----------------------------------------------------------------------

    #[test]
    fn test_distinct_wide_pk_empty_trace_three_new_rows() {
        use crate::storage::CursorHandle;
        use std::rc::Rc;
        // Trace empty; delta has three wide-PK rows with distinct PKs.
        // All three must emit +1.
        let schema = wide_pk_3xu64_schema();
        let empty = Rc::new(Batch::empty(1, schema.pk_stride()));
        let mut ch = CursorHandle::from_owned(&[empty], schema);

        let delta = make_wide_batch(&schema, &[(1, 0, 0, 1, 10), (2, 0, 0, 1, 20), (3, 0, 0, 1, 30)]);
        let (out, _) = op_distinct(delta, ch.cursor_mut(), &schema);
        assert_eq!(out.count, 3, "three new wide-PK rows must each emit +1");
        for i in 0..3 {
            assert_eq!(out.get_weight(i), 1);
        }
    }

    #[test]
    fn test_distinct_wide_pk_already_in_trace() {
        use crate::storage::CursorHandle;
        use std::rc::Rc;
        // Trace has (1,0,0, payload=99, w=1). Delta re-adds same (PK, payload).
        // Already in set → output must be empty.
        let schema = wide_pk_3xu64_schema();
        let trace = Rc::new(make_wide_batch(&schema, &[(1, 0, 0, 1, 99)]));
        let mut ch = CursorHandle::from_owned(&[trace], schema);

        let delta = make_wide_batch(&schema, &[(1, 0, 0, 1, 99)]);
        let (out, _) = op_distinct(delta, ch.cursor_mut(), &schema);
        assert_eq!(
            out.count, 0,
            "re-adding an existing (PK,payload) must produce no output"
        );
    }

    #[test]
    fn test_distinct_wide_pk_prefix_collision() {
        use crate::storage::CursorHandle;
        use std::rc::Rc;
        // Two wide-PK rows with the same 16-byte OPK prefix (c0=1,c1=1) but
        // differing in c2. One row in trace (w=1), one new row in delta (w=+1).
        // The row in the trace must not emit; the new row must emit +1.
        // This tests the cursor.current_pk_bytes() != key break condition.
        let schema = wide_pk_3xu64_schema();
        // (1,1,0) is already in the trace
        let trace = Rc::new(make_wide_batch(&schema, &[(1, 1, 0, 1, 50)]));
        let mut ch = CursorHandle::from_owned(&[trace], schema);

        // Delta has the NEW key (1,1, 1<<56) which shares 16 OPK bytes with (1,1,0)
        let c2_new = 1u64 << 56;
        let delta = make_wide_batch(&schema, &[(1, 1, c2_new, 1, 60)]);
        let (out, _) = op_distinct(delta, ch.cursor_mut(), &schema);
        // The new row is not in the trace → emit +1. The old row is not in delta.
        assert_eq!(out.count, 1, "prefix-collision new row must emit +1");
        assert_eq!(out.get_weight(0), 1);
    }

    #[test]
    fn test_distinct_u128_max_sentinel_bug() {
        use crate::storage::CursorHandle;
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
        let mut trace_b = Batch::with_schema(schema, 1);
        trace_b.extend_pk(max_pk);
        trace_b.extend_weight(&1i64.to_le_bytes());
        trace_b.extend_null_bmp(&0u64.to_le_bytes());
        trace_b.extend_col(0, &42i64.to_le_bytes());
        trace_b.count += 1;
        trace_b.certify_layout(Layout::Consolidated, &schema);

        let trace = Rc::new(trace_b);
        let mut ch = CursorHandle::from_owned(&[trace], schema);

        let mut delta = Batch::with_schema(schema, 1);
        delta.extend_pk(max_pk);
        delta.extend_weight(&1i64.to_le_bytes());
        delta.extend_null_bmp(&0u64.to_le_bytes());
        delta.extend_col(0, &42i64.to_le_bytes());
        delta.count += 1;
        delta.certify_layout(Layout::Consolidated, &schema);

        let (out, _) = op_distinct(delta, ch.cursor_mut(), &schema);
        assert_eq!(
            out.count, 0,
            "u128::MAX PK re-add must produce no output (sentinel bug regression)"
        );
    }
}
