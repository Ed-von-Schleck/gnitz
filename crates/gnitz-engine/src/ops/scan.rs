//! Scan trace operator.

use crate::schema::SchemaDescriptor;
use crate::storage::{Batch, ReadCursor};

// ---------------------------------------------------------------------------
// Scan trace (source operator)
// ---------------------------------------------------------------------------

/// Scan every row from a ReadCursor into a Batch.
///
/// Skips zero-weight rows (defense-in-depth: individual shard entries may carry
/// non-zero weights that cancel at merge level).
pub fn op_scan_trace(cursor: &mut ReadCursor, schema: &SchemaDescriptor) -> Batch {
    // `drain_to_batch` certifies its output `Consolidated`; an empty scan yields an
    // empty batch (structurally consolidated).
    cursor
        .drain_to_batch(0)
        .unwrap_or_else(|| Batch::empty_with_schema(schema))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{type_code, SchemaColumn, SchemaDescriptor};
    use crate::storage::{Batch, Layout};

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
    fn test_op_scan_trace_all() {
        use crate::storage::ReadCursor;
        use std::rc::Rc;

        let schema = make_schema_u64_i64();
        let trace = Rc::new(make_batch(
            &schema,
            &[(1, 1, 10), (2, 1, 20), (3, 1, 30), (4, 1, 40), (5, 1, 50)],
        ));
        let mut ch = ReadCursor::from_owned(&[trace], schema);

        let out = op_scan_trace(&mut ch, &schema);
        assert_eq!(out.count, 5);
        assert_eq!(out.get_pk(0), 1);
        assert_eq!(out.get_pk(4), 5);
        assert!(out.is_sorted());
        assert!(out.is_consolidated());
    }

    #[test]
    fn test_op_scan_trace_empty() {
        use crate::storage::ReadCursor;
        use std::rc::Rc;

        let schema = make_schema_u64_i64();
        let empty = Rc::new(Batch::empty_with_schema(&schema));
        let mut ch = ReadCursor::from_owned(&[empty], schema);

        let out = op_scan_trace(&mut ch, &schema);
        assert_eq!(out.count, 0);
    }
}
