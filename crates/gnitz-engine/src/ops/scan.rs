//! Scan trace operator.

use crate::schema::SchemaDescriptor;
use crate::storage::{Batch, ConsolidatedBatch, ReadCursor, DRAIN_BUFFER, write_to_batch};


// ---------------------------------------------------------------------------
// Scan trace (source operator)
// ---------------------------------------------------------------------------

/// Scan rows from a ReadCursor into an Batch.
///
/// Skips zero-weight rows (defense-in-depth: individual shard entries may carry
/// non-zero weights that cancel at merge level).
/// The cursor is left at the next unscanned position.
///
/// `chunk_limit <= 0` means scan everything until cursor exhaustion.
pub fn op_scan_trace(
    cursor: &mut ReadCursor,
    schema: &SchemaDescriptor,
    chunk_limit: i32,
) -> ConsolidatedBatch {
    let limit = if chunk_limit > 0 { chunk_limit as usize } else { 0 };
    if let Some(batch) = cursor.drain_single_source(limit) {
        return ConsolidatedBatch::new_unchecked(batch);
    }

    DRAIN_BUFFER.with(|buf| {
        let mut rows = buf.borrow_mut();
        cursor.drain_sorted_into(limit, &mut rows);
        if rows.is_empty() {
            return ConsolidatedBatch::new_unchecked(Batch::empty_with_schema(schema));
        }
        // For full drains the entry sum is a tight upper bound; for chunked
        // drains it can be orders of magnitude too large. Pass 0 there and
        // let `extend_from_slice` grow the blob on demand.
        let blob_cap = if limit > 0 { 0 } else { cursor.total_blob_len() };
        let mut b = write_to_batch(schema, rows.len(), blob_cap, |writer| {
            cursor.scatter_drained_into(&rows, writer);
        });
        b.sorted = true;
        b.consolidated = true;
        ConsolidatedBatch::new_unchecked(b)
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{SchemaColumn, SchemaDescriptor, type_code, MAX_COLUMNS};
    use crate::storage::Batch;

    fn make_schema_u64_i64() -> SchemaDescriptor {
        let mut columns = [SchemaColumn {
            type_code: 0, size: 0, nullable: 0, _pad: 0,
        }; MAX_COLUMNS];
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
    fn test_op_scan_trace_chunked() {
        use std::sync::Arc;
        use crate::storage::CursorHandle;

        let schema = make_schema_u64_i64();
        // 5 rows in trace
        let trace = Arc::new(make_batch(&schema, &[
            (1, 1, 10), (2, 1, 20), (3, 1, 30), (4, 1, 40), (5, 1, 50),
        ]));
        let mut ch = CursorHandle::from_owned(&[trace], schema);

        // First scan: chunk_limit=3 → get 3 rows
        let out1 = op_scan_trace(ch.cursor_mut(), &schema, 3);
        assert_eq!(out1.count, 3);
        assert_eq!(out1.get_pk(0), 1);
        assert_eq!(out1.get_pk(2), 3);
        assert!(out1.sorted);
        assert!(out1.consolidated);

        // Second scan: remaining 2 rows
        let out2 = op_scan_trace(ch.cursor_mut(), &schema, 3);
        assert_eq!(out2.count, 2);
        assert_eq!(out2.get_pk(0), 4);
        assert_eq!(out2.get_pk(1), 5);
    }

    #[test]
    fn test_op_scan_trace_empty() {
        use std::sync::Arc;
        use crate::storage::CursorHandle;

        let schema = make_schema_u64_i64();
        let empty = Arc::new(Batch::empty(1, 16));
        let mut ch = CursorHandle::from_owned(&[empty], schema);

        let out = op_scan_trace(ch.cursor_mut(), &schema, 10);
        assert_eq!(out.count, 0);
    }
}
