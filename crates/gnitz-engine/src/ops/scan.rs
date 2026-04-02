//! Scan trace operator.

use crate::compact::SchemaDescriptor;
use crate::memtable::OwnedBatch;
use crate::read_cursor::ReadCursor;

use super::util::append_cursor_row_to_batch;

// ---------------------------------------------------------------------------
// Scan trace (source operator)
// ---------------------------------------------------------------------------

/// Scan rows from a ReadCursor into an OwnedBatch.
///
/// Faithfully ports source.py:14-45.  Skips zero-weight rows (defense-in-depth:
/// individual shard entries may carry non-zero weights that cancel at merge level).
/// The cursor is left at the next unscanned position.
///
/// `chunk_limit <= 0` means scan everything until cursor exhaustion.
pub fn op_scan_trace(
    cursor: &mut ReadCursor,
    schema: &SchemaDescriptor,
    chunk_limit: i32,
) -> OwnedBatch {
    let cap = if chunk_limit > 0 { chunk_limit as usize } else { 64 };
    let mut output = OwnedBatch::with_schema(*schema, cap);
    output.sorted = true;       // cursor produces sorted order
    output.consolidated = true; // cursor merges → consolidated

    let mut scanned: i32 = 0;
    while cursor.valid {
        if chunk_limit > 0 && scanned >= chunk_limit {
            break;
        }
        // Skip zero-weight rows (source.py:37)
        if cursor.current_weight != 0 {
            append_cursor_row_to_batch(&mut output, cursor, schema);
            scanned += 1;
        }
        cursor.advance();
    }

    output
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compact::{SchemaColumn, SchemaDescriptor, type_code};
    use crate::memtable::OwnedBatch;

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
        b.count = 0;
        for &(pk, w, val) in rows {
            b.pk_lo.extend_from_slice(&pk.to_le_bytes());
            b.pk_hi.extend_from_slice(&0u64.to_le_bytes());
            b.weight.extend_from_slice(&w.to_le_bytes());
            b.null_bmp.extend_from_slice(&0u64.to_le_bytes());
            b.col_data[0].extend_from_slice(&val.to_le_bytes());
            b.count += 1;
        }
        b.sorted = true;
        b.consolidated = true;
        b
    }

    #[test]
    fn test_op_scan_trace_chunked() {
        use std::sync::Arc;
        use crate::read_cursor::create_cursor_from_snapshots;

        let schema = make_schema_u64_i64();
        // 5 rows in trace
        let trace = Arc::new(make_batch(&schema, &[
            (1, 1, 10), (2, 1, 20), (3, 1, 30), (4, 1, 40), (5, 1, 50),
        ]));
        let mut ch = unsafe { create_cursor_from_snapshots(&[trace], &[], schema) };

        // First scan: chunk_limit=3 → get 3 rows
        let out1 = op_scan_trace(&mut ch.cursor, &schema, 3);
        assert_eq!(out1.count, 3);
        assert_eq!(out1.get_pk_lo(0), 1);
        assert_eq!(out1.get_pk_lo(2), 3);
        assert!(out1.sorted);
        assert!(out1.consolidated);

        // Second scan: remaining 2 rows
        let out2 = op_scan_trace(&mut ch.cursor, &schema, 3);
        assert_eq!(out2.count, 2);
        assert_eq!(out2.get_pk_lo(0), 4);
        assert_eq!(out2.get_pk_lo(1), 5);
    }

    #[test]
    fn test_op_scan_trace_empty() {
        use std::sync::Arc;
        use crate::read_cursor::create_cursor_from_snapshots;

        let schema = make_schema_u64_i64();
        let empty = Arc::new(OwnedBatch::empty(1));
        let mut ch = unsafe { create_cursor_from_snapshots(&[empty], &[], schema) };

        let out = op_scan_trace(&mut ch.cursor, &schema, 10);
        assert_eq!(out.count, 0);
    }
}
