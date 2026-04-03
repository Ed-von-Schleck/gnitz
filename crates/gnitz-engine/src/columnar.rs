//! Shared columnar data access trait and generic row comparison.
//!
//! Replaces the duplicated `compare_rows` implementations in compact.rs,
//! merge.rs, and read_cursor.rs with a single generic version.

use std::cmp::Ordering;

use crate::compact::{
    compare_german_strings, read_signed, SchemaDescriptor,
    type_code::{F32 as TYPE_F32, F64 as TYPE_F64, STRING as TYPE_STRING, U128 as TYPE_U128},
};
use crate::util::{read_u32_le, read_u64_le};

// ---------------------------------------------------------------------------
// ColumnarSource trait
// ---------------------------------------------------------------------------

pub trait ColumnarSource {
    fn get_null_word(&self, row: usize) -> u64;
    fn get_col_ptr(&self, row: usize, payload_col: usize, col_size: usize) -> &[u8];
    fn blob_slice(&self) -> &[u8];
}

// ---------------------------------------------------------------------------
// Generic compare_rows
// ---------------------------------------------------------------------------

/// Compare two rows from any ColumnarSource implementations by payload columns.
///
/// This is the canonical implementation with the hoisted null_word optimisation:
/// null words are read once per row outside the column loop.
pub fn compare_rows<A: ColumnarSource, B: ColumnarSource>(
    schema: &SchemaDescriptor,
    src_a: &A,
    row_a: usize,
    src_b: &B,
    row_b: usize,
) -> Ordering {
    let pk_index = schema.pk_index as usize;
    let null_word_a = src_a.get_null_word(row_a);
    let null_word_b = src_b.get_null_word(row_b);
    let mut payload_col: usize = 0;

    for ci in 0..schema.num_columns as usize {
        if ci == pk_index {
            continue;
        }

        let null_a = (null_word_a >> payload_col) & 1 != 0;
        let null_b = (null_word_b >> payload_col) & 1 != 0;
        if null_a && null_b {
            payload_col += 1;
            continue;
        }
        if null_a {
            return Ordering::Less;
        }
        if null_b {
            return Ordering::Greater;
        }

        let col = &schema.columns[ci];
        let col_size = col.size as usize;

        let ord = match col.type_code {
            TYPE_STRING => {
                let ptr_a = src_a.get_col_ptr(row_a, payload_col, 16);
                let ptr_b = src_b.get_col_ptr(row_b, payload_col, 16);
                compare_german_strings(ptr_a, src_a.blob_slice(), ptr_b, src_b.blob_slice())
            }
            TYPE_U128 => {
                let ba = src_a.get_col_ptr(row_a, payload_col, 16);
                let bb = src_b.get_col_ptr(row_b, payload_col, 16);
                let va = ((read_u64_le(ba, 8) as u128) << 64) | (read_u64_le(ba, 0) as u128);
                let vb = ((read_u64_le(bb, 8) as u128) << 64) | (read_u64_le(bb, 0) as u128);
                va.cmp(&vb)
            }
            TYPE_F64 => {
                let ba = src_a.get_col_ptr(row_a, payload_col, 8);
                let bb = src_b.get_col_ptr(row_b, payload_col, 8);
                let va = f64::from_bits(read_u64_le(ba, 0));
                let vb = f64::from_bits(read_u64_le(bb, 0));
                va.total_cmp(&vb)
            }
            TYPE_F32 => {
                let ba = src_a.get_col_ptr(row_a, payload_col, 4);
                let bb = src_b.get_col_ptr(row_b, payload_col, 4);
                let va = f32::from_bits(read_u32_le(ba, 0));
                let vb = f32::from_bits(read_u32_le(bb, 0));
                va.total_cmp(&vb)
            }
            _ => {
                let raw_a = src_a.get_col_ptr(row_a, payload_col, col_size);
                let raw_b = src_b.get_col_ptr(row_b, payload_col, col_size);
                let va = read_signed(raw_a, col_size);
                let vb = read_signed(raw_b, col_size);
                va.cmp(&vb)
            }
        };

        payload_col += 1;

        if ord != Ordering::Equal {
            return ord;
        }
    }

    Ordering::Equal
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compact::{SchemaColumn, SchemaDescriptor, type_code};

    /// A minimal ColumnarSource for unit tests.
    struct TestBatch {
        null_bmp: Vec<u8>,
        col_data: Vec<Vec<u8>>,
        blob: Vec<u8>,
    }

    impl ColumnarSource for TestBatch {
        fn get_null_word(&self, row: usize) -> u64 {
            read_u64_le(&self.null_bmp, row * 8)
        }
        fn get_col_ptr(&self, row: usize, payload_col: usize, col_size: usize) -> &[u8] {
            let off = row * col_size;
            &self.col_data[payload_col][off..off + col_size]
        }
        fn blob_slice(&self) -> &[u8] {
            &self.blob
        }
    }

    /// Build a 3-column schema: [PK:U64, nullable I64, F64].
    fn make_schema_nullable_float() -> SchemaDescriptor {
        let mut columns = [SchemaColumn::new(0, 0); 64];
        columns[0] = SchemaColumn::new(type_code::U64, 0);
        columns[1] = SchemaColumn::new(type_code::I64, 1);
        columns[2] = SchemaColumn::new(type_code::F64, 0);
        SchemaDescriptor { num_columns: 3, pk_index: 0, columns }
    }

    /// Build a TestBatch with [nullable I64, F64] payload columns.
    /// Each row is (null_word, col0_i64, col1_f64).
    fn batch_from_rows(rows: &[(u64, i64, f64)]) -> TestBatch {
        let n = rows.len();
        let mut null_bmp = Vec::with_capacity(n * 8);
        let mut col0 = Vec::with_capacity(n * 8);
        let mut col1 = Vec::with_capacity(n * 8);

        for &(nw, c0, c1_f) in rows {
            null_bmp.extend_from_slice(&nw.to_le_bytes());
            col0.extend_from_slice(&c0.to_le_bytes());
            col1.extend_from_slice(&c1_f.to_bits().to_le_bytes());
        }

        TestBatch {
            null_bmp,
            col_data: vec![col0, col1],
            blob: vec![],
        }
    }

    /// Ports RPython test_comparator: null < non-null ordering.
    #[test]
    fn test_compare_rows_null_lt_non_null() {
        let schema = make_schema_nullable_float();
        // Row A: col1 = NULL (null_word bit 0 set), col2 = -5.0
        // Row B: col1 = 10, col2 = 5.0
        let batch = batch_from_rows(&[
            (1, 0, -5.0),   // row 0: null_word=1 → payload col 0 is null
            (0, 10, 5.0),   // row 1: null_word=0 → nothing null
        ]);
        // null < non-null
        assert_eq!(compare_rows(&schema, &batch, 0, &batch, 1), Ordering::Less);
        // non-null > null
        assert_eq!(compare_rows(&schema, &batch, 1, &batch, 0), Ordering::Greater);
    }

    /// Ports RPython test_comparator: null == null (both null → skip to next col).
    #[test]
    fn test_compare_rows_null_eq_null() {
        let schema = make_schema_nullable_float();
        // Both rows have col1 = NULL, col2 differs
        let batch = batch_from_rows(&[
            (1, 0, -5.0),   // null col1, f64 = -5.0
            (1, 0, 5.0),    // null col1, f64 = 5.0
        ]);
        // null == null → fall through to col2: -5.0 < 5.0
        assert_eq!(compare_rows(&schema, &batch, 0, &batch, 1), Ordering::Less);
    }

    /// Ports RPython test_comparator: float comparison (5.0 > -5.0).
    #[test]
    fn test_compare_rows_float() {
        let schema = make_schema_nullable_float();
        // Row B: col1 = 10, col2 = 5.0
        // Row C: col1 = 10, col2 = -5.0
        let batch = batch_from_rows(&[
            (0, 10, 5.0),
            (0, 10, -5.0),
        ]);
        // col1 equal (10 == 10), col2: 5.0 > -5.0
        assert_eq!(compare_rows(&schema, &batch, 0, &batch, 1), Ordering::Greater);
    }

    /// Ports RPython test_comparator: equality.
    #[test]
    fn test_compare_rows_equality() {
        let schema = make_schema_nullable_float();
        let batch = batch_from_rows(&[
            (0, 10, -5.0),
        ]);
        assert_eq!(compare_rows(&schema, &batch, 0, &batch, 0), Ordering::Equal);
    }

    /// Helper to build a single-payload-column TestBatch.
    fn single_col_batch(null_words: &[u64], col_data: Vec<u8>) -> TestBatch {
        let mut null_bmp = Vec::new();
        for &nw in null_words {
            null_bmp.extend_from_slice(&nw.to_le_bytes());
        }
        TestBatch { null_bmp, col_data: vec![col_data], blob: vec![] }
    }

    /// Test signed integer comparison: negative < positive via sign extension.
    #[test]
    fn test_compare_rows_signed_int() {
        let mut columns = [SchemaColumn::new(0, 0); 64];
        columns[0] = SchemaColumn::new(type_code::U64, 0);
        columns[1] = SchemaColumn::new(type_code::I64, 0);
        let schema = SchemaDescriptor { num_columns: 2, pk_index: 0, columns };

        let mut col0 = Vec::new();
        col0.extend_from_slice(&(-42i64).to_le_bytes());
        col0.extend_from_slice(&42i64.to_le_bytes());
        let batch = single_col_batch(&[0, 0], col0);
        // -42 < 42
        assert_eq!(compare_rows(&schema, &batch, 0, &batch, 1), Ordering::Less);
    }

    /// Test U128 column comparison.
    #[test]
    fn test_compare_rows_u128() {
        let mut columns = [SchemaColumn::new(0, 0); 64];
        columns[0] = SchemaColumn::new(type_code::U64, 0);
        columns[1] = SchemaColumn::new(type_code::U128, 0);
        let schema = SchemaDescriptor { num_columns: 2, pk_index: 0, columns };

        let mut col0 = Vec::new();
        // Row 0: u128 = 1 (lo=1, hi=0)
        col0.extend_from_slice(&1u64.to_le_bytes());
        col0.extend_from_slice(&0u64.to_le_bytes());
        // Row 1: u128 = (1 << 64) (lo=0, hi=1)
        col0.extend_from_slice(&0u64.to_le_bytes());
        col0.extend_from_slice(&1u64.to_le_bytes());
        let batch = single_col_batch(&[0, 0], col0);
        // 1 < (1 << 64)
        assert_eq!(compare_rows(&schema, &batch, 0, &batch, 1), Ordering::Less);
    }

    /// Test that NaN values produce a stable total order (not all-Equal, which
    /// would violate transitivity and cause sort algorithms to misbehave).
    #[test]
    fn test_compare_rows_nan() {
        let schema = make_schema_nullable_float();
        // Row 0: col0=0, col1=NaN
        // Row 1: col0=0, col1=1.0
        let batch = batch_from_rows(&[
            (0, 0, f64::NAN),
            (0, 0, 1.0),
        ]);
        // total_cmp: positive NaN is ordered above all finite values
        assert_eq!(compare_rows(&schema, &batch, 0, &batch, 1), Ordering::Greater);
        assert_eq!(compare_rows(&schema, &batch, 1, &batch, 0), Ordering::Less);
        // NaN vs NaN → Equal (IEEE 754 total order)
        assert_eq!(compare_rows(&schema, &batch, 0, &batch, 0), Ordering::Equal);
    }

    /// Test STRING column comparison via compare_rows (short strings).
    #[test]
    fn test_compare_rows_string() {
        let mut columns = [SchemaColumn::new(0, 0); 64];
        columns[0] = SchemaColumn::new(type_code::U64, 0);
        columns[1] = SchemaColumn::new(type_code::STRING, 0);
        let schema = SchemaDescriptor { num_columns: 2, pk_index: 0, columns };

        let mut col0 = Vec::new();
        // "abc" (len=3)
        let mut s1 = [0u8; 16];
        s1[0..4].copy_from_slice(&3u32.to_le_bytes());
        s1[4] = b'a'; s1[5] = b'b'; s1[6] = b'c';
        col0.extend_from_slice(&s1);
        // "abd" (len=3)
        let mut s2 = [0u8; 16];
        s2[0..4].copy_from_slice(&3u32.to_le_bytes());
        s2[4] = b'a'; s2[5] = b'b'; s2[6] = b'd';
        col0.extend_from_slice(&s2);

        let batch = single_col_batch(&[0, 0], col0);
        // "abc" < "abd"
        assert_eq!(compare_rows(&schema, &batch, 0, &batch, 1), Ordering::Less);
    }
}
