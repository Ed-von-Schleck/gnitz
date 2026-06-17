//! Shared join row writers (inner, outer, delta-delta). Kept `#[inline]` so the
//! per-row column-copy loops fold into their callers across the join split — the
//! §8-cluster-7 "no per-row cross-file call boundary" guarantee.

use crate::schema::SchemaDescriptor;
use gnitz_wire::is_german_string;
use crate::storage::{Batch, MemBatch, ReadCursor};

use super::super::util::{all_payload_null_mask, merge_null_words, write_string_from_batch};

// ---------------------------------------------------------------------------
// Join row writer helpers
// ---------------------------------------------------------------------------

/// Copy left-side payload columns from a MemBatch into the output.
/// Shared by all join row writers (inner, outer, DD).
#[inline]
fn write_left_payload(
    output: &mut Batch,
    left_batch: &MemBatch,
    left_row: usize,
    left_null: u64,
    left_schema: &SchemaDescriptor,
) {
    for (pi, _ci, col) in left_schema.payload_columns() {
        let cs = col.size() as usize;
        let is_null = (left_null >> pi) & 1 != 0;
        if is_null {
            output.fill_col_zero(pi, cs);
        } else if is_german_string(col.type_code) {
            write_string_from_batch(output, pi, left_batch, left_row, pi);
        } else {
            output.extend_col(pi, left_batch.get_col_ptr(left_row, pi, cs));
        }
    }
}

/// Write one composite join output row: [left_PK, left_payload..., right_payload...].
///
/// Left columns come from the delta MemBatch. Right columns come from the
/// cursor's current row, accessed via `col_ptr()` / `blob_ptr()`.
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

    write_left_payload(output, left_batch, left_row, left_null, left_schema);

    // Right payload columns (from cursor public API)
    let right_blob = right_cursor.blob_slice();  // &[u8], bounds carried
    for (rpi, ci, col) in right_schema.payload_columns() {
        let cs = col.size() as usize;
        let is_null = (right_null >> rpi) & 1 != 0;
        let out_pi = left_npc + rpi;
        if is_null {
            output.fill_col_zero(out_pi, cs);
        } else if is_german_string(col.type_code) {
            let ptr = right_cursor.col_ptr(ci, 16);
            if ptr.is_null() {
                output.fill_col_zero(out_pi, 16);
            } else {
                // SAFETY: ptr is a valid col_ptr (non-null checked above) to a
                // 16-byte German-string struct; relocate_german_string_vec
                // bounds-checks the blob offset against right_blob.
                let src = unsafe { std::slice::from_raw_parts(ptr, 16) };
                let dest = crate::schema::relocate_german_string_vec(
                    src, right_blob, &mut output.blob, None,
                );
                output.extend_col(out_pi, &dest);
            }
        } else {
            let ptr = right_cursor.col_ptr(ci, cs);
            if ptr.is_null() {
                output.fill_col_zero(out_pi, cs);
            } else {
                let src = unsafe { std::slice::from_raw_parts(ptr, cs) };
                output.extend_col(out_pi, src);
            }
        }
    }

    output.count += 1;
}

/// Write one null-filled join output row: left columns from batch, right columns all NULL.
#[inline]
pub(super) fn write_join_row_null_right(
    output: &mut Batch,
    left_batch: &MemBatch,
    left_row: usize,
    weight: i64,
    left_schema: &SchemaDescriptor,
    right_schema: &SchemaDescriptor,
) {
    let left_null = left_batch.get_null_word(left_row);

    let left_npc = left_schema.num_payload_cols();
    let right_npc = right_schema.num_payload_cols();
    let right_null_bits = all_payload_null_mask(right_npc);
    let null_word = merge_null_words(left_null, right_null_bits, left_npc);

    output.extend_pk_bytes(left_batch.get_pk_bytes(left_row));
    output.extend_weight(&weight.to_le_bytes());
    output.extend_null_bmp(&null_word.to_le_bytes());

    write_left_payload(output, left_batch, left_row, left_null, left_schema);

    // Right payload columns: all zeros (null)
    for (rpi, _ci, col) in right_schema.payload_columns() {
        output.fill_col_zero(left_npc + rpi, col.size() as usize);
    }

    output.count += 1;
}

/// Write one composite join output row where both sides come from MemBatch.
#[allow(clippy::too_many_arguments)]
#[inline]
pub(super) fn write_join_row_from_batches(
    output: &mut Batch,
    left_batch: &MemBatch,
    left_row: usize,
    right_batch: &MemBatch,
    right_row: usize,
    weight: i64,
    left_schema: &SchemaDescriptor,
    right_schema: &SchemaDescriptor,
) {
    let left_null = left_batch.get_null_word(left_row);
    let right_null = right_batch.get_null_word(right_row);

    let left_npc = left_schema.num_payload_cols();
    let null_word = merge_null_words(left_null, right_null, left_npc);

    output.extend_pk_bytes(left_batch.get_pk_bytes(left_row));
    output.extend_weight(&weight.to_le_bytes());
    output.extend_null_bmp(&null_word.to_le_bytes());

    write_left_payload(output, left_batch, left_row, left_null, left_schema);

    // Right payload columns (from right MemBatch)
    for (rpi, _ci, col) in right_schema.payload_columns() {
        let cs = col.size() as usize;
        let is_null = (right_null >> rpi) & 1 != 0;
        let out_pi = left_npc + rpi;
        if is_null {
            output.fill_col_zero(out_pi, cs);
        } else if is_german_string(col.type_code) {
            write_string_from_batch(output, out_pi, right_batch, right_row, rpi);
        } else {
            output.extend_col(out_pi, right_batch.get_col_ptr(right_row, rpi, cs));
        }
    }

    output.count += 1;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::test_common::*;
    use crate::schema::{SchemaColumn, SchemaDescriptor, type_code};
    use crate::storage::Batch;

    #[test]
    fn test_write_join_row_compound_pk_bytes() {
        // Narrow compound-PK left: 2× U64, pk_stride = 16, no payload.
        let left_schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::U64, 0),
            ],
            &[0, 1],
        );
        let right_schema = make_schema_u64_i64();

        // Known 16-byte PK region: pk0 = 0x1122..., pk1 = 0xAABB...
        let pk0: u64 = 0x1122_3344_5566_7788;
        let pk1: u64 = 0xAABB_CCDD_EEFF_0011;
        let mut pk_bytes = [0u8; 16];
        pk_bytes[0..8].copy_from_slice(&pk0.to_le_bytes());
        pk_bytes[8..16].copy_from_slice(&pk1.to_le_bytes());

        let mut left = Batch::with_schema(left_schema, 1);
        left.extend_pk_bytes(&pk_bytes);
        left.extend_weight(&1i64.to_le_bytes());
        left.extend_null_bmp(&0u64.to_le_bytes());
        left.count += 1;

        let right = make_batch(&right_schema, &[(7, 1, 42)]);

        // Output schema: 2 PK cols + right payload (1 I64) = 3 cols.
        let out_schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0, 1],
        );
        let mut output = Batch::with_schema(out_schema, 1);

        write_join_row_from_batches(
            &mut output,
            &left.as_mem_batch(),
            0,
            &right.as_mem_batch(),
            0,
            1,
            &left_schema,
            &right_schema,
        );

        assert_eq!(output.count, 1);
        assert_eq!(output.get_pk_bytes(0), &pk_bytes);
    }

    // -----------------------------------------------------------------------
    // op_anti_join_delta_trace / op_semi_join_delta_trace
    // -----------------------------------------------------------------------

    #[test]
    fn test_write_join_row_from_batches_shift_guard_64() {
        // left_npc == 64, right_npc == 0: `right_null << 64` panics in debug.
        let left_schema = make_wide_left_schema_64();
        assert_eq!(left_schema.num_payload_cols(), 64);
        let right_schema = pk_only_schema();
        let left = build_wide_left_row(&left_schema, 1);
        let mut right = Batch::with_schema(right_schema, 1);
        right.extend_pk(1u128);
        right.extend_weight(&1i64.to_le_bytes());
        right.extend_null_bmp(&0u64.to_le_bytes());
        right.count += 1;

        let mut output = Batch::with_schema(left_schema, 1); // PK + 64 payload = 65 cols
        write_join_row_from_batches(
            &mut output, &left.as_mem_batch(), 0, &right.as_mem_batch(), 0,
            1, &left_schema, &right_schema,
        );
        assert_eq!(output.count, 1);
    }

    #[test]
    fn test_write_join_row_null_right_shift_guard_64() {
        let left_schema = make_wide_left_schema_64();
        let right_schema = pk_only_schema();
        let left = build_wide_left_row(&left_schema, 1);
        let mut output = Batch::with_schema(left_schema, 1);
        write_join_row_null_right(
            &mut output, &left.as_mem_batch(), 0, 1, &left_schema, &right_schema,
        );
        assert_eq!(output.count, 1);
    }

    #[test]
    fn test_write_join_row_shift_guard_64() {
        use std::rc::Rc;
        use crate::storage::CursorHandle;
        let left_schema = make_wide_left_schema_64();
        let right_schema = pk_only_schema();
        let left = build_wide_left_row(&left_schema, 1);

        let mut trace = Batch::with_schema(right_schema, 1);
        trace.extend_pk(1u128);
        trace.extend_weight(&1i64.to_le_bytes());
        trace.extend_null_bmp(&0u64.to_le_bytes());
        trace.count += 1;
        trace.sorted = true;
        trace.consolidated = true;
        let trace = Rc::new(trace);
        let mut ch = CursorHandle::from_owned(&[trace], right_schema);
        let cursor = ch.cursor_mut();
        cursor.seek_bytes(&1u64.to_be_bytes());

        let mut output = Batch::with_schema(left_schema, 1);
        write_join_row(
            &mut output, &left.as_mem_batch(), 0, cursor, 1, &left_schema, &right_schema,
        );
        assert_eq!(output.count, 1);
    }

    // -----------------------------------------------------------------------
    // Anti-join DT uncompacted multi-entry trace (item 18)
    // -----------------------------------------------------------------------

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
