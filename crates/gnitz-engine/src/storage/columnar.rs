//! Shared columnar data access trait and generic row comparison.
//!
//! Replaces the duplicated `compare_rows` implementations in compact.rs,
//! merge.rs, and read_cursor.rs with a single generic version.

use std::cmp::Ordering;

use crate::schema::{
    compare_german_strings, read_signed, SchemaDescriptor,
    type_code::{BLOB as TYPE_BLOB, F32 as TYPE_F32, F64 as TYPE_F64, STRING as TYPE_STRING, U128 as TYPE_U128},
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
#[inline]
pub fn compare_rows<A: ColumnarSource, B: ColumnarSource>(
    schema: &SchemaDescriptor,
    src_a: &A,
    row_a: usize,
    src_b: &B,
    row_b: usize,
) -> Ordering {
    let null_word_a = src_a.get_null_word(row_a);
    let null_word_b = src_b.get_null_word(row_b);

    for (payload_col, _ci, col) in schema.payload_columns() {
        let null_a = (null_word_a >> payload_col) & 1 != 0;
        let null_b = (null_word_b >> payload_col) & 1 != 0;
        if null_a && null_b {
            continue;
        }
        if null_a {
            return Ordering::Less;
        }
        if null_b {
            return Ordering::Greater;
        }

        let col_size = col.size as usize;

        let ord = match col.type_code {
            TYPE_STRING | TYPE_BLOB => {
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

        if ord != Ordering::Equal {
            return ord;
        }
    }

    Ordering::Equal
}

// ---------------------------------------------------------------------------
// Fast path: signed-integer, non-nullable schemas
// ---------------------------------------------------------------------------

/// Returns true iff every payload column is non-nullable and a signed
/// fixed-width integer (I8/I16/I32/I64). Unsigned types are excluded:
/// `compare_rows_int_nonnull` reads via `read_signed` and would misorder
/// values ≥ the sign bit.
#[inline]
pub(crate) fn schema_is_int_nonnull(schema: &SchemaDescriptor) -> bool {
    use crate::schema::type_code::{I8, I16, I32, I64};
    schema.payload_columns().all(|(_, _, col)| {
        col.nullable == 0 && matches!(col.type_code, I8 | I16 | I32 | I64)
    })
}

/// Fast path for `compare_rows`: skips null-bitmap reads and the per-column
/// type-code dispatch. Caller MUST guarantee `schema_is_int_nonnull(schema)`
/// — otherwise the lack of null/float/string/U128 arms produces silent
/// miscompares.
#[inline]
pub(crate) fn compare_rows_int_nonnull<A: ColumnarSource, B: ColumnarSource>(
    schema: &SchemaDescriptor,
    src_a: &A, row_a: usize,
    src_b: &B, row_b: usize,
) -> Ordering {
    debug_assert!(
        schema_is_int_nonnull(schema),
        "compare_rows_int_nonnull called on schema with non-signed-integer or nullable columns",
    );
    for (payload_col, _ci, col) in schema.payload_columns() {
        let col_size = col.size as usize;
        let raw_a = src_a.get_col_ptr(row_a, payload_col, col_size);
        let raw_b = src_b.get_col_ptr(row_b, payload_col, col_size);
        let ord = read_signed(raw_a, col_size).cmp(&read_signed(raw_b, col_size));
        if ord != Ordering::Equal { return ord; }
    }
    Ordering::Equal
}

// ---------------------------------------------------------------------------
// Sort helpers
// ---------------------------------------------------------------------------

/// A `(pk, row-index)` pair used by sorting routines in merge and reduce.
/// Keeps the PK co-located with its index so the comparator reads from the
/// element being positioned rather than a separate array.
#[derive(Copy, Clone)]
pub(crate) struct SortEntry {
    pub(crate) pk: u128,
    pub(crate) idx: u32,
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{SchemaColumn, SchemaDescriptor, type_code};

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
        let mut columns = [SchemaColumn::new(0, 0); crate::schema::MAX_COLUMNS];
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

    /// Ports test_comparator: null < non-null ordering.
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

    /// Ports test_comparator: null == null (both null → skip to next col).
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

    /// Ports test_comparator: float comparison (5.0 > -5.0).
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

    /// Ports test_comparator: equality.
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
        let mut columns = [SchemaColumn::new(0, 0); crate::schema::MAX_COLUMNS];
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
        let mut columns = [SchemaColumn::new(0, 0); crate::schema::MAX_COLUMNS];
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

    // ---------------------------------------------------------------------------
    // German string comparator — direct tests (short, long, mixed lengths)
    // ---------------------------------------------------------------------------

    use crate::schema::SHORT_STRING_THRESHOLD;

    /// Build a German string struct (len > SHORT_STRING_THRESHOLD) with its heap blob.
    fn make_long_string(data: &[u8]) -> ([u8; 16], Vec<u8>) {
        assert!(data.len() > SHORT_STRING_THRESHOLD);
        let mut s = [0u8; 16];
        s[0..4].copy_from_slice(&(data.len() as u32).to_le_bytes());
        s[4..8].copy_from_slice(&data[0..4]); // prefix
        s[8..16].copy_from_slice(&0u64.to_le_bytes()); // heap_offset = 0
        (s, data.to_vec())
    }

    #[test]
    fn test_german_string_short() {
        let mut a = [0u8; 16];
        let mut b = [0u8; 16];
        // "abc" < "abd"
        a[0..4].copy_from_slice(&3u32.to_le_bytes());
        a[4] = b'a'; a[5] = b'b'; a[6] = b'c';
        b[0..4].copy_from_slice(&3u32.to_le_bytes());
        b[4] = b'a'; b[5] = b'b'; b[6] = b'd';
        assert_eq!(compare_german_strings(&a, &[], &b, &[]), Ordering::Less);

        // Equal
        b[6] = b'c';
        assert_eq!(compare_german_strings(&a, &[], &b, &[]), Ordering::Equal);

        // Shorter < longer with same prefix: "abc" < "abcz"
        b[0..4].copy_from_slice(&4u32.to_le_bytes());
        b[7] = b'z';
        assert_eq!(compare_german_strings(&a, &[], &b, &[]), Ordering::Less);
    }

    #[test]
    fn test_german_string_long() {
        // Long strings: equal except last byte
        let data_a: Vec<u8> = b"hello_world_long_A".to_vec(); // len=18
        let data_b_lt: Vec<u8> = b"hello_world_long_B".to_vec();
        let (sa, blob_a) = make_long_string(&data_a);
        let (sb_lt, blob_b_lt) = make_long_string(&data_b_lt);
        assert_eq!(compare_german_strings(&sa, &blob_a, &sb_lt, &blob_b_lt), Ordering::Less);

        // Equal long strings
        let (sb_eq, blob_b_eq) = make_long_string(&data_a);
        assert_eq!(compare_german_strings(&sa, &blob_a, &sb_eq, &blob_b_eq), Ordering::Equal);

        // Prefix differs early → less
        let data_b_prefix: Vec<u8> = b"aello_world_long_A".to_vec();
        let (sb_prefix, blob_b_prefix) = make_long_string(&data_b_prefix);
        assert_eq!(compare_german_strings(&sb_prefix, &blob_b_prefix, &sa, &blob_a), Ordering::Less);
    }

    #[test]
    fn test_german_string_mixed_short_long() {
        // Short (len=10) vs long (len=20) with same prefix; shorter < longer
        let short_data = b"0123456789"; // len=10, ≤ SHORT_STRING_THRESHOLD → short
        let mut s_short = [0u8; 16];
        s_short[0..4].copy_from_slice(&10u32.to_le_bytes());
        s_short[4..8].copy_from_slice(&short_data[0..4]);
        s_short[8..14].copy_from_slice(&short_data[4..]);
        let long_data: Vec<u8> = b"01234567890123456789".to_vec(); // len=20
        let (s_long, blob_long) = make_long_string(&long_data);
        assert_eq!(compare_german_strings(&s_short, &[], &s_long, &blob_long), Ordering::Less);
    }

    /// Test STRING column comparison via compare_rows (short strings).
    #[test]
    fn test_compare_rows_string() {
        let mut columns = [SchemaColumn::new(0, 0); crate::schema::MAX_COLUMNS];
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

    // -----------------------------------------------------------------------
    // Fast path: schema_is_int_nonnull / compare_rows_int_nonnull
    // -----------------------------------------------------------------------

    fn make_schema(cols: &[(u8, u8)]) -> SchemaDescriptor {
        // First column is PK (U64); subsequent are payload columns.
        let mut columns = [SchemaColumn::new(0, 0); crate::schema::MAX_COLUMNS];
        columns[0] = SchemaColumn::new(type_code::U64, 0);
        for (i, &(tc, nullable)) in cols.iter().enumerate() {
            columns[i + 1] = SchemaColumn::new(tc, nullable);
        }
        SchemaDescriptor {
            num_columns: 1 + cols.len() as u32,
            pk_index: 0,
            columns,
        }
    }

    /// Verify the fast path agrees with the generic `compare_rows` for I8/I16/I32/I64.
    #[test]
    fn test_compare_rows_int_nonnull_matches_generic() {
        // I64
        {
            let schema = make_schema(&[(type_code::I64, 0)]);
            let mut col = Vec::new();
            col.extend_from_slice(&(-42i64).to_le_bytes());
            col.extend_from_slice(&0i64.to_le_bytes());
            col.extend_from_slice(&42i64.to_le_bytes());
            let batch = single_col_batch(&[0, 0, 0], col);
            for i in 0..3 {
                for j in 0..3 {
                    assert_eq!(
                        compare_rows_int_nonnull(&schema, &batch, i, &batch, j),
                        compare_rows(&schema, &batch, i, &batch, j),
                        "I64 mismatch at ({i}, {j})",
                    );
                }
            }
        }
        // I32
        {
            let schema = make_schema(&[(type_code::I32, 0)]);
            let vals: [i32; 4] = [i32::MIN, -1, 0, i32::MAX];
            let mut col = Vec::new();
            for v in vals { col.extend_from_slice(&v.to_le_bytes()); }
            let batch = single_col_batch(&[0; 4], col);
            for i in 0..4 {
                for j in 0..4 {
                    assert_eq!(
                        compare_rows_int_nonnull(&schema, &batch, i, &batch, j),
                        compare_rows(&schema, &batch, i, &batch, j),
                    );
                }
            }
        }
        // I16
        {
            let schema = make_schema(&[(type_code::I16, 0)]);
            let vals: [i16; 4] = [i16::MIN, -1, 0, i16::MAX];
            let mut col = Vec::new();
            for v in vals { col.extend_from_slice(&v.to_le_bytes()); }
            let batch = single_col_batch(&[0; 4], col);
            for i in 0..4 {
                for j in 0..4 {
                    assert_eq!(
                        compare_rows_int_nonnull(&schema, &batch, i, &batch, j),
                        compare_rows(&schema, &batch, i, &batch, j),
                    );
                }
            }
        }
        // I8
        {
            let schema = make_schema(&[(type_code::I8, 0)]);
            let vals: [i8; 4] = [i8::MIN, -1, 0, i8::MAX];
            let mut col = Vec::new();
            for v in vals { col.push(v as u8); }
            let batch = single_col_batch(&[0; 4], col);
            for i in 0..4 {
                for j in 0..4 {
                    assert_eq!(
                        compare_rows_int_nonnull(&schema, &batch, i, &batch, j),
                        compare_rows(&schema, &batch, i, &batch, j),
                    );
                }
            }
        }
        // Multi-column (I32, I64): primary diff in col 0, tie-break in col 1.
        {
            let schema = make_schema(&[(type_code::I32, 0), (type_code::I64, 0)]);
            // Two payload cols means `single_col_batch` is wrong shape.
            // Build inline: 3 rows × (i32, i64).
            let rows: &[(i32, i64)] = &[(1, 100), (1, 200), (2, 50)];
            let mut col0 = Vec::new();
            let mut col1 = Vec::new();
            for &(a, b) in rows {
                col0.extend_from_slice(&a.to_le_bytes());
                col1.extend_from_slice(&b.to_le_bytes());
            }
            let mut null_bmp = Vec::new();
            for _ in rows { null_bmp.extend_from_slice(&0u64.to_le_bytes()); }
            let batch = TestBatch { null_bmp, col_data: vec![col0, col1], blob: vec![] };
            for i in 0..rows.len() {
                for j in 0..rows.len() {
                    assert_eq!(
                        compare_rows_int_nonnull(&schema, &batch, i, &batch, j),
                        compare_rows(&schema, &batch, i, &batch, j),
                        "multi-col mismatch at ({i}, {j})",
                    );
                }
            }
        }
    }

    /// Verify `schema_is_int_nonnull` rejects unsigned ints, U128, floats,
    /// strings, blobs, and any nullable column.
    #[test]
    fn test_schema_is_int_nonnull_excludes_unsigned() {
        // All-signed-int, non-nullable → true
        assert!(schema_is_int_nonnull(&make_schema(&[
            (type_code::I8, 0), (type_code::I16, 0),
            (type_code::I32, 0), (type_code::I64, 0),
        ])));

        // Nullable signed int → false
        assert!(!schema_is_int_nonnull(&make_schema(&[(type_code::I32, 1)])));

        // Each disqualifying type → false
        for tc in [
            type_code::U8, type_code::U16, type_code::U32, type_code::U64,
            type_code::U128, type_code::F32, type_code::F64,
            type_code::STRING, type_code::BLOB,
        ] {
            assert!(
                !schema_is_int_nonnull(&make_schema(&[(tc, 0)])),
                "expected schema with type_code={tc} to be rejected",
            );
        }

        // Mixed: signed int + unsigned → false (any disqualifier kills it)
        assert!(!schema_is_int_nonnull(&make_schema(&[
            (type_code::I64, 0), (type_code::U32, 0),
        ])));
    }
}
