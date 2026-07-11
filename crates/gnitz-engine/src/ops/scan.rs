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
    use crate::test_support::{make_batch, make_schema_u64_i64};

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
