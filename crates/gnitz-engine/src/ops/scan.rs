//! Scan trace operator.

use crate::schema::SchemaDescriptor;
use crate::storage::{Batch, ConsolidatedBatch, ReadCursor};

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
pub fn op_scan_trace(cursor: &mut ReadCursor, schema: &SchemaDescriptor, chunk_limit: i32) -> ConsolidatedBatch {
    let limit = if chunk_limit > 0 { chunk_limit as usize } else { 0 };
    cursor
        .drain_to_batch(limit)
        .map(ConsolidatedBatch::new_unchecked)
        .unwrap_or_else(|| ConsolidatedBatch::new_unchecked(Batch::empty_with_schema(schema)))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{type_code, SchemaColumn, SchemaDescriptor};
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
        b.sorted = true;
        b.consolidated = true;
        b
    }

    #[test]
    fn test_op_scan_trace_chunked() {
        use crate::storage::CursorHandle;
        use std::rc::Rc;

        let schema = make_schema_u64_i64();
        // 5 rows in trace
        let trace = Rc::new(make_batch(
            &schema,
            &[(1, 1, 10), (2, 1, 20), (3, 1, 30), (4, 1, 40), (5, 1, 50)],
        ));
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
        use crate::storage::CursorHandle;
        use std::rc::Rc;

        let schema = make_schema_u64_i64();
        let empty = Rc::new(Batch::empty(1, 16));
        let mut ch = CursorHandle::from_owned(&[empty], schema);

        let out = op_scan_trace(ch.cursor_mut(), &schema, 10);
        assert_eq!(out.count, 0);
    }
}
