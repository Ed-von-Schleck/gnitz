//! DBSP distinct operator.

use crate::schema::SchemaDescriptor;
use crate::storage::{write_to_owned_batch, OwnedBatch, ReadCursor, scatter_copy};

use super::util::{consolidate_owned, signum, compare_cursor_payload_to_batch_row};

// ---------------------------------------------------------------------------
// Distinct
// ---------------------------------------------------------------------------

/// DBSP distinct: converts multiset deltas into set-membership deltas.
///
/// Returns `(output_batch, consolidated_delta)`.
/// The consolidated delta is returned so the caller can feed it to `ingest_batch`.
pub fn op_distinct(
    delta: &OwnedBatch,
    cursor: &mut ReadCursor,
    schema: &SchemaDescriptor,
) -> (OwnedBatch, OwnedBatch) {
    let npc = schema.num_columns as usize - 1;

    // 1. Consolidate delta
    let consolidated = consolidate_owned(delta, schema);
    let n = consolidated.count;
    if n == 0 {
        return (OwnedBatch::empty(npc), consolidated);
    }

    // 2. Walk consolidated, collect emitting indices and weights
    let mut emit_indices: Vec<u32> = Vec::with_capacity(n);
    let mut emit_weights: Vec<i64> = Vec::with_capacity(n);

    let consolidated_mb = consolidated.as_mem_batch();
    let mut prev_key = u128::MAX;

    for i in 0..n {
        let key = consolidated.get_pk(i);
        let key_lo = key as u64;
        let key_hi = (key >> 64) as u64;
        let w_delta = consolidated.get_weight(i);

        if key != prev_key {
            cursor.seek(key);
        }
        prev_key = key;

        let w_old: i64 = loop {
            if !cursor.valid
                || cursor.current_key_lo != key_lo
                || cursor.current_key_hi != key_hi
            {
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
        return (OwnedBatch::empty(npc), consolidated);
    }

    let mb = consolidated.as_mem_batch();
    let blob_cap = mb.blob.len().max(1);
    let mut output =
        write_to_owned_batch(schema, emit_indices.len(), blob_cap, |writer| {
            scatter_copy(&mb, &emit_indices, &emit_weights, writer);
        });
    output.sorted = true;
    output.consolidated = true;
    output.schema = Some(*schema);

    (output, consolidated)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{SchemaColumn, SchemaDescriptor, type_code};
    use crate::storage::OwnedBatch;

    fn make_schema_u64_i64() -> SchemaDescriptor {
        let mut columns = [SchemaColumn {
            type_code: 0, size: 0, nullable: 0, _pad: 0,
        }; 64];
        columns[0] = SchemaColumn {
            type_code: type_code::U64, size: 8, nullable: 0, _pad: 0,
        };
        columns[1] = SchemaColumn {
            type_code: type_code::I64, size: 8, nullable: 0, _pad: 0,
        };
        SchemaDescriptor { num_columns: 2, pk_index: 0, columns }
    }

    fn make_batch(
        schema: &SchemaDescriptor,
        rows: &[(u64, i64, i64)],
    ) -> OwnedBatch {
        let n = rows.len();
        let mut b = OwnedBatch::with_schema(*schema, n.max(1));
        for &(pk, w, val) in rows {
            b.extend_pk_lo(&pk.to_le_bytes());
            b.extend_pk_hi(&0u64.to_le_bytes());
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
        use std::sync::Arc;
        use crate::storage::CursorHandle;

        let schema = make_schema_u64_i64();

        // Empty trace → all positive deltas emit +1
        let empty = Arc::new(OwnedBatch::empty(1));
        let mut ch = CursorHandle::from_owned(&[empty.clone()], schema);

        // Delta: pk=1 w=+3, pk=2 w=+1
        let delta = make_batch(&schema, &[(1, 3, 10), (2, 1, 20)]);
        let (out, _) = op_distinct(&delta, ch.cursor_mut(), &schema);
        // 0→positive: both emit +1
        assert_eq!(out.count, 2);
        assert_eq!(out.get_weight(0), 1);
        assert_eq!(out.get_weight(1), 1);

        // Now trace has pk=1 w=3 and pk=2 w=1
        // Delta: pk=1 w=-2 (3→1, still positive, no output), pk=2 w=-1 (1→0, emit -1)
        let trace_batch = Arc::new(make_batch(&schema, &[(1, 3, 10), (2, 1, 20)]));
        let mut ch2 = CursorHandle::from_owned(&[trace_batch], schema);
        let delta2 = make_batch(&schema, &[(1, -2, 10), (2, -1, 20)]);
        let (out2, _) = op_distinct(&delta2, ch2.cursor_mut(), &schema);
        // pk=1: 3→1, positive→positive, no change
        // pk=2: 1→0, positive→non-positive, emit -1
        assert_eq!(out2.count, 1);
        assert_eq!((out2.get_pk(0) as u64), 2);
        assert_eq!(out2.get_weight(0), -1);
    }

    #[test]
    fn test_op_distinct_consolidated_flag() {
        use std::sync::Arc;
        use crate::storage::CursorHandle;

        let schema = make_schema_u64_i64();
        let empty = Arc::new(OwnedBatch::empty(1));
        let mut ch = CursorHandle::from_owned(&[empty], schema);

        let delta = make_batch(&schema, &[(1, 1, 10)]);
        let (out, consolidated) = op_distinct(&delta, ch.cursor_mut(), &schema);
        assert!(out.consolidated, "distinct output must be consolidated");
        assert!(out.sorted, "distinct output must be sorted");
        assert!(consolidated.consolidated, "consolidated output must be consolidated");
    }
}
