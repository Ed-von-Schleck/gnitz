//! Shared inner-join row writer. Kept `#[inline]` so the per-row column-copy
//! loops fold into their callers across the join split — the "no per-row
//! cross-file call boundary" guarantee.

use crate::schema::SchemaDescriptor;
use crate::storage::{Batch, MemBatch, ReadCursor};

use super::super::util::merge_null_words;

/// Write one composite join output row: [left_PK, left_payload..., right_payload...].
///
/// Left columns come from the delta MemBatch. Right columns come from the
/// cursor's current row — both halves through the shared, monomorphic
/// `Batch::append_payload_cols` body (German-string blob relocation included).
#[inline]
pub(super) fn write_join_row(
    output: &mut Batch,
    left_batch: &MemBatch,
    left_row: usize,
    right_cursor: &ReadCursor,
    weight: i64,
    left_schema: &SchemaDescriptor,
    right_schema: &SchemaDescriptor,
) {
    let left_null = left_batch.get_null_word(left_row);
    let right_null = right_cursor.current_null_word;

    let left_npc = left_schema.num_payload_cols();
    let null_word = merge_null_words(left_null, right_null, left_npc);

    output.extend_pk_bytes(left_batch.get_pk_bytes(left_row));
    output.extend_weight(&weight.to_le_bytes());
    output.extend_null_bmp(&null_word.to_le_bytes());

    output.append_payload_cols(0, left_schema, left_batch, left_row, left_null, None);
    let (right_src, right_row) = right_cursor.current_row_source();
    output.append_payload_cols(left_npc, right_schema, right_src, right_row, right_null, None);

    output.count += 1;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{type_code, SchemaColumn, SchemaDescriptor};
    use crate::storage::{Batch, Layout};

    /// `write_join_row` with left_npc == 64, right_npc == 0: the `right_null << 64`
    /// shift would panic in debug without the width guard.
    #[test]
    fn test_write_join_row_shift_guard_64() {
        use crate::storage::ReadCursor;
        use std::rc::Rc;
        let left_schema = make_wide_left_schema_64();
        let right_schema = pk_only_schema();
        let left = build_wide_left_row(&left_schema, 1);

        let mut trace = Batch::with_schema(right_schema, 1);
        trace.extend_pk(1u128);
        trace.extend_weight(&1i64.to_le_bytes());
        trace.extend_null_bmp(&0u64.to_le_bytes());
        trace.count += 1;
        trace.certify_layout(Layout::Consolidated, &right_schema);
        let trace = Rc::new(trace);
        let mut ch = ReadCursor::from_owned(&[trace], right_schema);
        let cursor = &mut ch;
        cursor.seek_bytes(&1u64.to_be_bytes());

        let mut output = Batch::with_schema(left_schema, 1);
        write_join_row(
            &mut output,
            &left.as_mem_batch(),
            0,
            cursor,
            1,
            &left_schema,
            &right_schema,
        );
        assert_eq!(output.count, 1);
    }

    fn make_wide_left_schema_64() -> SchemaDescriptor {
        let mut cols = vec![SchemaColumn::new(type_code::U64, 0)];
        for _ in 0..64 {
            cols.push(SchemaColumn::new(type_code::I64, 0));
        }
        SchemaDescriptor::new(&cols, &[0])
    }

    fn pk_only_schema() -> SchemaDescriptor {
        SchemaDescriptor::new(&[SchemaColumn::new(type_code::U64, 0)], &[0])
    }

    fn build_wide_left_row(left_schema: &SchemaDescriptor, pk: u64) -> Batch {
        let mut b = Batch::with_schema(*left_schema, 1);
        b.extend_pk(pk as u128);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        for pi in 0..left_schema.num_payload_cols() {
            b.extend_col(pi, &0i64.to_le_bytes());
        }
        b.count += 1;
        b
    }
}
