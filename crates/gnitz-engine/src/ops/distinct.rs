//! DBSP distinct operator.

use crate::schema::SchemaDescriptor;
use crate::storage::{write_to_batch, Batch, ConsolidatedBatch, ReadCursor, scatter_copy};

use super::util::{signum, compare_cursor_payload_to_batch_row};

// ---------------------------------------------------------------------------
// Distinct
// ---------------------------------------------------------------------------

/// DBSP distinct: converts multiset deltas into set-membership deltas.
///
/// Returns `(output_batch, consolidated_delta)`.
/// The consolidated delta is returned so the caller can feed it to `ingest_batch`.
pub fn op_distinct(
    delta: Batch,
    cursor: &mut ReadCursor,
    schema: &SchemaDescriptor,
) -> (ConsolidatedBatch, ConsolidatedBatch) {
    assert!(!schema.pk_is_wide(),
        "op_distinct: wide PK trace is unsupported (narrow u128 seek API)");
    // 1. Consolidate delta
    let consolidated = delta.into_consolidated(schema);
    let n = consolidated.count;
    if n == 0 {
        return (ConsolidatedBatch::new_unchecked(Batch::empty_with_schema(schema)), consolidated);
    }

    // 2. Walk consolidated, collect emitting indices and weights
    let mut emit_indices: Vec<u32> = Vec::with_capacity(n);
    let mut emit_weights: Vec<i64> = Vec::with_capacity(n);

    let consolidated_mb = consolidated.as_mem_batch();
    let mut prev_key = u128::MAX;

    for i in 0..n {
        let key = consolidated.get_pk(i);
        let w_delta = consolidated.get_weight(i);

        if key != prev_key {
            cursor.seek_bytes(consolidated.get_pk_bytes(i));
        }
        prev_key = key;

        let w_old: i64 = loop {
            if !cursor.valid || cursor.current_key != key {
                break 0;
            }
            match compare_cursor_payload_to_batch_row(cursor, &consolidated_mb, i, schema) {
                std::cmp::Ordering::Less => {
                    cursor.advance();
                }
                std::cmp::Ordering::Equal => {
                    let w = cursor.current_weight;
                    cursor.advance();
                    break w;
                }
                std::cmp::Ordering::Greater => break 0,
            }
        };

        let s_old = signum(w_old);
        let w_new = w_old.wrapping_add(w_delta);
        let s_new = signum(w_new);
        let out_w = s_new - s_old;
        if out_w != 0 {
            emit_indices.push(i as u32);
            emit_weights.push(out_w);
        }
    }

    // 3. Scatter-copy emitting rows
    if emit_indices.is_empty() {
        return (ConsolidatedBatch::new_unchecked(Batch::empty_with_schema(schema)), consolidated);
    }

    let mb = consolidated.as_mem_batch();
    let blob_cap = mb.blob.len().max(1);
    let mut output =
        write_to_batch(schema, emit_indices.len(), blob_cap, |writer| {
            scatter_copy(&mb, &emit_indices, &emit_weights, writer);
        });
    output.sorted = true;
    output.consolidated = true;

    (ConsolidatedBatch::new_unchecked(output), consolidated)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{SchemaColumn, SchemaDescriptor, type_code};
    use crate::storage::Batch;

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
    ) -> Batch {
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
        b
    }

    #[test]
    fn test_op_distinct_boundary() {
        use std::rc::Rc;
        use crate::storage::CursorHandle;

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

    fn make_batch_narrow<const N: usize>(
        schema: &SchemaDescriptor,
        rows: &[(u64, i64, i64)],
    ) -> Batch {
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
        b.sorted = true;
        b.consolidated = true;
        b
    }

    // Regression tests for compare_cursor_payload_to_batch_row with sub-64-bit
    // integer payload columns. Previously panicked on try_into().unwrap() because
    // the slice had fewer than 8 bytes.

    #[test]
    fn test_distinct_i32_payload_no_panic() {
        use std::rc::Rc;
        use crate::storage::CursorHandle;

        let schema = make_schema_u64_i32();
        let trace = Rc::new(make_batch_narrow::<4>(&schema, &[(1, 1, 42)]));
        let mut ch = CursorHandle::from_owned(&[trace], schema);

        // Delta: same (PK=1, val=42) → stays +1 → no output
        let delta = make_batch_narrow::<4>(&schema, &[(1, 1, 42)]);
        let (out, _) = op_distinct(delta, ch.cursor_mut(), &schema);
        assert_eq!(out.count, 0, "I32: matching (PK,payload) should produce no output");

        // New (PK=1, val=99) → new element → +1 output
        let mut ch2 = CursorHandle::from_owned(
            &[Rc::new(make_batch_narrow::<4>(&schema, &[(1, 1, 42)]))],
            schema,
        );
        let delta2 = make_batch_narrow::<4>(&schema, &[(1, 1, 99)]);
        let (out2, _) = op_distinct(delta2, ch2.cursor_mut(), &schema);
        assert_eq!(out2.count, 1, "I32: new (PK,payload) should produce +1");
    }

    #[test]
    fn test_distinct_i16_payload_no_panic() {
        use std::rc::Rc;
        use crate::storage::CursorHandle;

        let schema = make_schema_u64_i16();
        let trace = Rc::new(make_batch_narrow::<2>(&schema, &[(5, 1, -100)]));
        let mut ch = CursorHandle::from_owned(&[trace], schema);

        let delta = make_batch_narrow::<2>(&schema, &[(5, 1, -100)]);
        let (out, _) = op_distinct(delta, ch.cursor_mut(), &schema);
        assert_eq!(out.count, 0, "I16: matching (PK,payload) should produce no output");
    }

    #[test]
    fn test_distinct_i8_payload_no_panic() {
        use std::rc::Rc;
        use crate::storage::CursorHandle;

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
        b.sorted = true;
        b.consolidated = true;
        b
    }

    /// Regression: a non-null BLOB payload column previously panicked. The
    /// (non-null, non-null) payload-compare arm routed BLOB to `cmp_typed_le`,
    /// whose Blob arm is `unreachable!`. BLOB shares the German-string layout
    /// and must dispatch through `compare_german_strings` like STRING.
    #[test]
    fn test_distinct_blob_payload_no_panic() {
        use std::rc::Rc;
        use crate::storage::CursorHandle;

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

    fn make_compound_batch(
        schema: &SchemaDescriptor,
        rows: &[(u64, u64, i64, i64)],
    ) -> Batch {
        let mut b = Batch::with_schema(*schema, rows.len().max(1));
        for &(c0, c1, w, val) in rows {
            b.extend_pk_bytes(&compound_pk_bytes(c0, c1));
            b.extend_weight(&w.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &val.to_le_bytes());
            b.count += 1;
        }
        b.sorted = true;
        b.consolidated = true;
        b
    }

    /// With a compound PK the raw-u128 order is last-column-major, so the trace
    /// seek must go by bytes to find an existing element. A delta that re-adds
    /// an element already in the trace must net to no output (not emit `+1`).
    #[test]
    fn test_distinct_compound_pk_finds_existing() {
        use std::rc::Rc;
        use crate::storage::CursorHandle;

        let schema = make_schema_compound();
        // Trace in storage order: (1,5) then (2,3).
        let trace = Rc::new(make_compound_batch(&schema, &[(1, 5, 1, 100), (2, 3, 1, 200)]));
        let mut ch = CursorHandle::from_owned(&[trace], schema);

        // Re-add (2,3) with the same payload → already present → no output.
        let delta = make_compound_batch(&schema, &[(2, 3, 1, 200)]);
        let (out, _) = op_distinct(delta, ch.cursor_mut(), &schema);
        assert_eq!(out.count, 0, "compound: re-adding an existing element must net to no output");

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

    fn make_signed_batch(
        schema: &SchemaDescriptor,
        rows: &[(i64, i64, i64)],
    ) -> Batch {
        let mut b = Batch::with_schema(*schema, rows.len().max(1));
        for &(pk, w, val) in rows {
            b.extend_pk((pk as u64) as u128);
            b.extend_weight(&w.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &val.to_le_bytes());
            b.count += 1;
        }
        b.sorted = true;
        b.consolidated = true;
        b
    }

    /// Signed single-column PK: negatives sort first in storage but last in raw
    /// u128. The trace seek to a negative key must find the existing element.
    #[test]
    fn test_distinct_signed_pk_finds_existing() {
        use std::rc::Rc;
        use crate::storage::CursorHandle;

        let schema = make_schema_signed();
        // Storage (signed) order: -3, -1, 2.
        let trace = Rc::new(make_signed_batch(&schema, &[(-3, 1, 30), (-1, 1, 10), (2, 1, 20)]));
        let mut ch = CursorHandle::from_owned(&[trace], schema);

        // Re-add (-1) with the same payload → already present → no output.
        let delta = make_signed_batch(&schema, &[(-1, 1, 10)]);
        let (out, _) = op_distinct(delta, ch.cursor_mut(), &schema);
        assert_eq!(out.count, 0, "signed: re-adding an existing element must net to no output");

        // Retract (-1) fully → element leaves the set → -1.
        let trace2 = Rc::new(make_signed_batch(&schema, &[(-3, 1, 30), (-1, 1, 10), (2, 1, 20)]));
        let mut ch2 = CursorHandle::from_owned(&[trace2], schema);
        let delta2 = make_signed_batch(&schema, &[(-1, -1, 10)]);
        let (out2, _) = op_distinct(delta2, ch2.cursor_mut(), &schema);
        assert_eq!(out2.count, 1, "signed: fully retracting an element emits -1");
        assert_eq!(out2.get_pk(0) as i64, -1);
        assert_eq!(out2.get_weight(0), -1);
    }

    #[test]
    fn test_op_distinct_consolidated_flag() {
        use std::rc::Rc;
        use crate::storage::CursorHandle;

        let schema = make_schema_u64_i64();
        let empty = Rc::new(Batch::empty(1, 16));
        let mut ch = CursorHandle::from_owned(&[empty], schema);

        let delta = make_batch(&schema, &[(1, 1, 10)]);
        let (out, consolidated) = op_distinct(delta, ch.cursor_mut(), &schema);
        assert!(out.consolidated, "distinct output must be consolidated");
        assert!(out.sorted, "distinct output must be sorted");
        assert!(consolidated.consolidated, "consolidated output must be consolidated");
    }
}
