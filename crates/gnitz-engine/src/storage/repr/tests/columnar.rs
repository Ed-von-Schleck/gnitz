use super::*;
use crate::schema::{type_code, SchemaColumn, SchemaDescriptor};
use crate::test_support::pk_only_schema;

/// Two rows both NULL in an `I64` payload column, carrying DIFFERENT bytes under
/// the null bit. [`compare_rows`] reads `null == null` and makes them one element;
/// [`compare_rows_fixedint_nonnull`] never reads the null word and splits them by
/// the garbage — which is why a nullable schema must resolve to `Generic`.
#[test]
fn null_equality_separates_the_two_payload_comparators() {
    use crate::storage::Batch;

    let pk = SchemaColumn::new(type_code::U128, 0);
    let nullable = SchemaDescriptor::new(&[pk, SchemaColumn::new(type_code::I64, 1)], &[0]);
    let non_null = SchemaDescriptor::new(&[pk, SchemaColumn::new(type_code::I64, 0)], &[0]);

    // Same PK, both NULL, different bytes under the null bit.
    let mut pair = Batch::with_capacity(nullable, 2);
    for garbage in [0x5555_5555_5555_5555u64 as i64, 0xAAAA_AAAA_AAAA_AAAAu64 as i64] {
        pair.extend_pk(0x1234_5678_9abc_def0);
        pair.extend_weight(&1i64.to_le_bytes());
        pair.extend_null_bmp(&1u64.to_le_bytes()); // payload col 0 → NULL
        pair.extend_col(0, &garbage.to_le_bytes());
        pair.count += 1;
    }

    assert_eq!(
        compare_rows(&nullable, &pair, 0, &pair, 1),
        Ordering::Equal,
        "null-aware: two NULL cells are one element whatever lies under the bit",
    );
    assert_ne!(
        compare_rows_fixedint_nonnull(&non_null, &pair, 0, &pair, 1),
        Ordering::Equal,
        "null-blind: the fixed-int comparator orders by the garbage bytes",
    );
}

/// A minimal ColumnarSource for unit tests.
struct TestBatch {
    null_bmp: Vec<u8>,
    col_data: Vec<Vec<u8>>,
    blob: Vec<u8>,
}

impl RowSource for TestBatch {
    // TestBatch models payload-only comparison; PK/weight are never read through it.
    fn get_pk_bytes(&self, _row: usize) -> &[u8] {
        &[]
    }
    fn get_null_word(&self, row: usize) -> u64 {
        gnitz_wire::read_u64_le(&self.null_bmp, row * 8)
    }
    fn get_col_ptr(&self, row: usize, payload_col: usize, col_size: usize) -> &[u8] {
        let off = row * col_size;
        &self.col_data[payload_col][off..off + col_size]
    }
    fn blob(&self) -> &[u8] {
        &self.blob
    }
    fn row_count(&self) -> usize {
        self.null_bmp.len() / 8
    }
}

impl ColumnarSource for TestBatch {
    fn get_weight(&self, _row: usize) -> i64 {
        1
    }
}

/// Build a 3-column schema: [PK:U64, nullable I64, F64].
fn make_schema_nullable_float() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 1),
            SchemaColumn::new(type_code::F64, 0),
        ],
        &[0],
    )
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

/// Null sorts below non-null.
#[test]
fn test_compare_rows_null_lt_non_null() {
    let schema = make_schema_nullable_float();
    // Row A: col1 = NULL (null_word bit 0 set), col2 = -5.0
    // Row B: col1 = 10, col2 = 5.0
    let batch = batch_from_rows(&[
        (1, 0, -5.0), // row 0: null_word=1 → payload col 0 is null
        (0, 10, 5.0), // row 1: null_word=0 → nothing null
    ]);
    // null < non-null
    assert_eq!(compare_rows(&schema, &batch, 0, &batch, 1), Ordering::Less);
    // non-null > null
    assert_eq!(compare_rows(&schema, &batch, 1, &batch, 0), Ordering::Greater);
}

/// Two nulls compare equal, so the next column decides.
#[test]
fn test_compare_rows_null_eq_null() {
    let schema = make_schema_nullable_float();
    // Both rows have col1 = NULL, col2 differs
    let batch = batch_from_rows(&[
        (1, 0, -5.0), // null col1, f64 = -5.0
        (1, 0, 5.0),  // null col1, f64 = 5.0
    ]);
    // null == null → fall through to col2: -5.0 < 5.0
    assert_eq!(compare_rows(&schema, &batch, 0, &batch, 1), Ordering::Less);
}

/// Float ordering: 5.0 > -5.0.
#[test]
fn test_compare_rows_float() {
    let schema = make_schema_nullable_float();
    // Row B: col1 = 10, col2 = 5.0
    // Row C: col1 = 10, col2 = -5.0
    let batch = batch_from_rows(&[(0, 10, 5.0), (0, 10, -5.0)]);
    // col1 equal (10 == 10), col2: 5.0 > -5.0
    assert_eq!(compare_rows(&schema, &batch, 0, &batch, 1), Ordering::Greater);
}

/// A row compares equal to itself.
#[test]
fn test_compare_rows_equality() {
    let schema = make_schema_nullable_float();
    let batch = batch_from_rows(&[(0, 10, -5.0)]);
    assert_eq!(compare_rows(&schema, &batch, 0, &batch, 0), Ordering::Equal);
}

/// Helper to build a single-payload-column TestBatch.
fn single_col_batch(null_words: &[u64], col_data: Vec<u8>) -> TestBatch {
    let mut null_bmp = Vec::new();
    for &nw in null_words {
        null_bmp.extend_from_slice(&nw.to_le_bytes());
    }
    TestBatch {
        null_bmp,
        col_data: vec![col_data],
        blob: vec![],
    }
}

/// Test signed integer comparison: negative < positive via sign extension.
#[test]
fn test_compare_rows_signed_int() {
    let schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    );

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
    let schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U128, 0),
        ],
        &[0],
    );

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

/// A 16-byte I128 payload column (a cross-sign `_join_pk` surfaced into a
/// payload slot) must order as a SIGNED two's-complement value: -1 < 0 < 1.
/// The pre-fix wildcard arm hit `read_signed(.., 16)` => unreachable!, and an
/// unsigned-u128 reading would sort -1 (= u128::MAX bits) above 0 and 1.
#[test]
fn test_compare_rows_i128_signed() {
    let schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I128, 0),
        ],
        &[0],
    );
    let mut col0 = Vec::new();
    for v in [-1i128, 0, 1] {
        col0.extend_from_slice(&v.to_le_bytes());
    }
    let batch = single_col_batch(&[0, 0, 0], col0);
    // -1 < 0 < 1 (signed), not unsigned (where -1's bits = u128::MAX > 0, 1).
    assert_eq!(compare_rows(&schema, &batch, 0, &batch, 1), Ordering::Less);
    assert_eq!(compare_rows(&schema, &batch, 1, &batch, 2), Ordering::Less);
    assert_eq!(compare_rows(&schema, &batch, 0, &batch, 2), Ordering::Less);
    assert_eq!(compare_rows(&schema, &batch, 2, &batch, 0), Ordering::Greater);
}

// Distinct UUID payloads must not collapse to Equal: a 16-byte read that
// returned 0 would make every UUID compare Equal and silently drop rows in
// consolidation/compaction.
#[test]
fn test_compare_rows_uuid_distinct() {
    let schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::UUID, 0),
        ],
        &[0],
    );

    let mut col0 = Vec::new();
    // Row 0: lo=1, hi=0
    col0.extend_from_slice(&1u64.to_le_bytes());
    col0.extend_from_slice(&0u64.to_le_bytes());
    // Row 1: lo=0, hi=1
    col0.extend_from_slice(&0u64.to_le_bytes());
    col0.extend_from_slice(&1u64.to_le_bytes());
    let batch = single_col_batch(&[0, 0], col0);
    assert_ne!(
        compare_rows(&schema, &batch, 0, &batch, 1),
        Ordering::Equal,
        "distinct UUID payloads must not compare Equal",
    );
    assert_eq!(compare_rows(&schema, &batch, 0, &batch, 1), Ordering::Less);
}

// Unsigned payloads with the high bit set must not be read as negative,
// which would reverse sort order: 0 < MAX must hold for U64/U32/U16/U8.
#[test]
fn test_compare_rows_unsigned_high_bit() {
    for (tc, size) in [
        (type_code::U64, 8usize),
        (type_code::U32, 4),
        (type_code::U16, 2),
        (type_code::U8, 1),
    ] {
        let schema = SchemaDescriptor::new(&[SchemaColumn::new(type_code::U64, 0), SchemaColumn::new(tc, 0)], &[0]);
        let max = u64::MAX >> (64 - size * 8);
        let mut col0 = Vec::new();
        col0.extend_from_slice(&0u64.to_le_bytes()[..size]);
        col0.extend_from_slice(&max.to_le_bytes()[..size]);
        let batch = single_col_batch(&[0, 0], col0);
        assert_eq!(
            compare_rows(&schema, &batch, 0, &batch, 1),
            Ordering::Less,
            "0 must be less than the max unsigned value for type {tc}",
        );
    }
}

/// Test that NaN values produce a stable total order (not all-Equal, which
/// would violate transitivity and cause sort algorithms to misbehave).
#[test]
fn test_compare_rows_nan() {
    let schema = make_schema_nullable_float();
    // Row 0: col0=0, col1=NaN
    // Row 1: col0=0, col1=1.0
    let batch = batch_from_rows(&[(0, 0, f64::NAN), (0, 0, 1.0)]);
    // total_cmp: positive NaN is ordered above all finite values
    assert_eq!(compare_rows(&schema, &batch, 0, &batch, 1), Ordering::Greater);
    assert_eq!(compare_rows(&schema, &batch, 1, &batch, 0), Ordering::Less);
    // NaN vs NaN → Equal (IEEE 754 total order)
    assert_eq!(compare_rows(&schema, &batch, 0, &batch, 0), Ordering::Equal);
}

/// Test STRING column comparison via compare_rows (short strings).
#[test]
fn test_compare_rows_string() {
    let schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::STRING, 0),
        ],
        &[0],
    );

    let mut col0 = Vec::new();
    let mut blob = Vec::new();
    col0.extend_from_slice(&gnitz_wire::encode_german_string(b"abc", &mut blob));
    col0.extend_from_slice(&gnitz_wire::encode_german_string(b"abd", &mut blob));

    let batch = single_col_batch(&[0, 0], col0);
    // "abc" < "abd"
    assert_eq!(compare_rows(&schema, &batch, 0, &batch, 1), Ordering::Less);
}

// -----------------------------------------------------------------------
// Fast path: schema_is_fixedint_nonnull / compare_rows_fixedint_nonnull
// -----------------------------------------------------------------------

fn make_schema(cols: &[(u8, u8)]) -> SchemaDescriptor {
    // First column is PK (U64); subsequent are payload columns.
    let mut columns = [SchemaColumn::EMPTY; crate::schema::MAX_COLUMNS];
    columns[0] = SchemaColumn::new(type_code::U64, 0);
    for (i, &(tc, nullable)) in cols.iter().enumerate() {
        columns[i + 1] = SchemaColumn::new(tc, nullable);
    }
    let n = 1 + cols.len();
    SchemaDescriptor::new(&columns[..n], &[0])
}

/// Encode `vals` (each value's low `cs` bytes, two's-complement for signed,
/// zero-extended for unsigned) into a single-payload-column batch and assert
/// the fast path matches the generic comparator for every ordered pair.
fn check_single_col_fixedint(tc: u8, vals: &[i128]) {
    let schema = make_schema(&[(tc, 0)]);
    let cs = gnitz_wire::wire_stride(tc);
    let mut col = Vec::new();
    for &v in vals {
        col.extend_from_slice(&v.to_le_bytes()[..cs]);
    }
    let batch = single_col_batch(&vec![0u64; vals.len()], col);
    for i in 0..vals.len() {
        for j in 0..vals.len() {
            assert_eq!(
                compare_rows_fixedint_nonnull(&schema, &batch, i, &batch, j),
                compare_rows(&schema, &batch, i, &batch, j),
                "tc={tc} mismatch at ({i}, {j})",
            );
        }
    }
}

/// Verify the fast path agrees with the generic `compare_rows` for signed,
/// unsigned, and mixed-sign non-nullable fixed-int schemas across widths.
#[test]
fn test_compare_rows_fixedint_nonnull_matches_generic() {
    // Signed: MIN / -1 / 0 / 1 / MAX exercise the sign-flip across the MSB.
    check_single_col_fixedint(type_code::I8, &[i8::MIN as i128, -1, 0, 1, i8::MAX as i128]);
    check_single_col_fixedint(type_code::I16, &[i16::MIN as i128, -1, 0, 1, i16::MAX as i128]);
    check_single_col_fixedint(type_code::I32, &[i32::MIN as i128, -1, 0, 1, i32::MAX as i128]);
    check_single_col_fixedint(type_code::I64, &[i64::MIN as i128, -1, 0, 1, i64::MAX as i128]);
    // Unsigned: include high-bit-set values that signed reads would invert.
    check_single_col_fixedint(type_code::U8, &[0, 1, 0x7F, 0x80, 0xFF]);
    check_single_col_fixedint(type_code::U16, &[0, 1, 0x7FFF, 0x8000, 0xFFFF]);
    check_single_col_fixedint(type_code::U32, &[0, 1, 0x7FFF_FFFF, 0x8000_0000, 0xFFFF_FFFF]);
    check_single_col_fixedint(
        type_code::U64,
        &[0, 1, 0x7FFF_FFFF_FFFF_FFFF, 0x8000_0000_0000_0000, u64::MAX as i128],
    );

    // Multi-column schemas. (I32, I64) is all-signed (primary diff col 0,
    // tie-break col 1); (I32, U32) and (U64, I64) mix signedness, which the
    // per-column sign flip must handle without falling back.
    for (c0, c1) in [
        (type_code::I32, type_code::I64),
        (type_code::I32, type_code::U32),
        (type_code::U64, type_code::I64),
    ] {
        let schema = make_schema(&[(c0, 0), (c1, 0)]);
        let (col0, col1) = mixed_rows(c0, c1);
        let n = col0.len() / gnitz_wire::wire_stride(c0);
        let mut null_bmp = Vec::new();
        for _ in 0..n {
            null_bmp.extend_from_slice(&0u64.to_le_bytes());
        }
        let batch = TestBatch {
            null_bmp,
            col_data: vec![col0, col1],
            blob: vec![],
        };
        for i in 0..n {
            for j in 0..n {
                assert_eq!(
                    compare_rows_fixedint_nonnull(&schema, &batch, i, &batch, j),
                    compare_rows(&schema, &batch, i, &batch, j),
                    "mixed ({c0},{c1}) mismatch at ({i}, {j})",
                );
            }
        }
    }
}

/// Build two payload columns of types `(a, b)` with rows spanning negative,
/// zero, small-positive, and high-bit-set values so signedness matters.
fn mixed_rows(a: u8, b: u8) -> (Vec<u8>, Vec<u8>) {
    let cs_a = gnitz_wire::wire_stride(a);
    let cs_b = gnitz_wire::wire_stride(b);
    // (col0, col1) value pairs as i128 images; -1 stresses signed ordering,
    // the large positive stresses unsigned high-bit ordering.
    let pairs: &[(i128, i128)] = &[(-1, 5), (-1, 7), (0, 0), (1, -1), (1, 9)];
    let mut col0 = Vec::new();
    let mut col1 = Vec::new();
    for &(v0, v1) in pairs {
        col0.extend_from_slice(&v0.to_le_bytes()[..cs_a]);
        col1.extend_from_slice(&v1.to_le_bytes()[..cs_b]);
    }
    (col0, col1)
}

/// `schema_is_fixedint_nonnull`: all-signed, all-unsigned, and mixed-sign
/// non-null schemas all pass; nullable and U128/UUID/F32/F64/STRING/BLOB fail.
#[test]
fn test_schema_is_fixedint_nonnull_bounds() {
    // All-signed, non-nullable → true
    assert!(schema_is_fixedint_nonnull(&make_schema(&[
        (type_code::I8, 0),
        (type_code::I16, 0),
        (type_code::I32, 0),
        (type_code::I64, 0),
    ])));
    // All-unsigned, non-nullable → true
    assert!(schema_is_fixedint_nonnull(&make_schema(&[
        (type_code::U8, 0),
        (type_code::U16, 0),
        (type_code::U32, 0),
        (type_code::U64, 0),
    ])));
    // Mixed signed/unsigned, non-nullable → true
    assert!(schema_is_fixedint_nonnull(&make_schema(&[
        (type_code::I64, 0),
        (type_code::U32, 0),
    ])));
    // Empty payload (all-PK) → vacuously true
    assert!(schema_is_fixedint_nonnull(&pk_only_schema(&[type_code::U64])));

    // Nullable fixed int → false
    assert!(!schema_is_fixedint_nonnull(&make_schema(&[(type_code::I32, 1)])));
    assert!(!schema_is_fixedint_nonnull(&make_schema(&[(type_code::U32, 1)])));

    // Each non-fixed-int type → false (U128/UUID exceed 8 bytes; floats/strings/blobs)
    for tc in [
        type_code::U128,
        type_code::UUID,
        type_code::F32,
        type_code::F64,
        type_code::STRING,
        type_code::BLOB,
    ] {
        assert!(
            !schema_is_fixedint_nonnull(&make_schema(&[(tc, 0)])),
            "expected schema with type_code={tc} to be rejected",
        );
    }

    // A single disqualifier among valid columns kills it.
    assert!(!schema_is_fixedint_nonnull(&make_schema(&[
        (type_code::I64, 0),
        (type_code::U128, 0),
    ])));
}

// -----------------------------------------------------------------------
// compare_rows_fixedint_nonnull ≡ compare_rows property test
// -----------------------------------------------------------------------

mod fixedint_proptest {
    use super::*;
    use proptest::prelude::*;

    fn arb_fixedint_type() -> impl Strategy<Value = u8> {
        prop_oneof![
            Just(type_code::I8),
            Just(type_code::U8),
            Just(type_code::I16),
            Just(type_code::U16),
            Just(type_code::I32),
            Just(type_code::U32),
            Just(type_code::I64),
            Just(type_code::U64),
        ]
    }

    /// `(payload type codes, n rows, per-column row-major bytes)`. 1..=4
    /// columns over all eight fixed-int types spans every sign combination
    /// including mixed; random bytes frequently set the high bit, so a
    /// signedness bug surfaces as a fast-vs-generic disagreement.
    fn arb_case() -> impl Strategy<Value = (Vec<u8>, usize, Vec<Vec<u8>>)> {
        (prop::collection::vec(arb_fixedint_type(), 1..=4), 2usize..=6usize).prop_flat_map(|(types, n)| {
            let cols: Vec<_> = types
                .iter()
                .map(|&t| {
                    let cs = gnitz_wire::wire_stride(t);
                    prop::collection::vec(any::<u8>(), n * cs)
                })
                .collect();
            (Just(types), Just(n), cols)
        })
    }

    proptest! {
        /// The load-bearing guarantee: the fixed-int fast path produces the
        /// same order as the generic comparator for every random row pair,
        /// across all widths {1,2,4,8} and every signed/unsigned mix.
        #[test]
        fn fixedint_matches_generic((types, n, col_data) in arb_case()) {
            let payload: Vec<(u8, u8)> = types.iter().map(|&t| (t, 0)).collect();
            let schema = make_schema(&payload);
            prop_assert!(schema_is_fixedint_nonnull(&schema));

            let batch = TestBatch { null_bmp: vec![0u8; n * 8], col_data, blob: vec![] };
            for i in 0..n {
                for j in 0..n {
                    prop_assert_eq!(
                        compare_rows_fixedint_nonnull(&schema, &batch, i, &batch, j),
                        compare_rows(&schema, &batch, i, &batch, j),
                        "mismatch at ({}, {})", i, j,
                    );
                }
            }
        }
    }
}

// -----------------------------------------------------------------------
// gallop_lower_bound_bytes / binary_lower_bound
// -----------------------------------------------------------------------

/// Galloping seek equals the from-scratch lower bound for EVERY hint and key.
/// Sweeps `hint` across `0..=count` (gallop branch, O(1) boundary-at-hint,
/// `hint == count` run-off, and the overshoot fallback all fall out of the
/// full sweep) and `key` across below-min / present / absent-between /
/// duplicate / above-max values. 2-byte BE keys so memcmp order = numeric.
#[test]
fn gallop_lower_bound_matches_binary_over_all_hints() {
    let vals: [u16; 8] = [10, 10, 20, 30, 30, 30, 40, 50]; // duplicates + gaps
    let arr: Vec<[u8; 2]> = vals.iter().map(|v| v.to_be_bytes()).collect();
    let count = arr.len();
    let get = |i: usize| &arr[i][..];

    let probes: [u16; 12] = [0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 60];
    for &p in &probes {
        let key = p.to_be_bytes();
        // Naive linear reference: first index whose bytes are >= key.
        let expected = (0..count).find(|&i| get(i) >= &key[..]).unwrap_or(count);
        assert_eq!(
            binary_lower_bound(0, count, &key, &get),
            expected,
            "binary_lower_bound key={p}"
        );
        for hint in 0..=count {
            assert_eq!(
                gallop_lower_bound_bytes(count, &key, hint, get),
                expected,
                "gallop key={p} hint={hint}"
            );
        }
    }
}

/// `count == 0` returns 0 for every hint and never indexes the (empty) array.
#[test]
fn gallop_lower_bound_count_zero() {
    let arr: Vec<[u8; 2]> = vec![];
    let get = |i: usize| &arr[i][..];
    let key = 7u16.to_be_bytes();
    assert_eq!(gallop_lower_bound_bytes(0, &key, 0, get), 0);
    assert_eq!(binary_lower_bound(0, 0, &key, &get), 0);
}

/// `lower_bound_opk` / `gallop_opk` (the register dispatch) return the identical
/// index as the byte oracle (`binary_lower_bound` / `gallop_lower_bound_bytes`)
/// for every full-`stride` probe, across all four arms — `u64` (≤8), `u128`
/// (9..=16), `[u128; 2]` (17..=32), and the byte fallback (>32). Order
/// equivalence between the register and byte compare per width is the
/// load-bearing property; the `from_opk` packers themselves are additionally
/// pinned by `pack_pk_be_specialization_matches_naive` and the reduce argsort
/// tests (incl. the `[u128; 2]` leading-16 tie). Values span two bytes, so a
/// wrong-endian load would miscompare.
#[test]
fn lower_bound_opk_matches_byte_search() {
    // Right-align a value into the OPK tail (high bytes zero, the natural layout
    // for a small key) at any stride; `* 7` leaves gaps for between-key probes.
    let enc = |val: u64, stride: usize| -> Vec<u8> {
        let mut k = vec![0u8; stride];
        let t = stride.min(8);
        k[stride - t..].copy_from_slice(&val.to_be_bytes()[8 - t..]);
        k
    };
    for &stride in &[4usize, 8, 12, 16, 24, 32, 40] {
        let n = 200usize;
        let region: Vec<u8> = (0..n).flat_map(|i| enc((i as u64) * 7 + 3, stride)).collect();
        let get = |i: usize| &region[i * stride..i * stride + stride];

        // Every stored key, a below-all and above-all key, and one landing in
        // each inter-key gap (the last lands above all). All exactly `stride`.
        let mut probes: Vec<Vec<u8>> = (0..n).map(|i| get(i).to_vec()).collect();
        probes.push(enc(0, stride)); // below all (the min stored key encodes 3)
        probes.push(vec![0xffu8; stride]); // above all
        probes.extend((0..n).map(|i| enc((i as u64) * 7 + 3 + 4, stride))); // in-gap

        for p in &probes {
            assert_eq!(p.len(), stride);
            let oracle = binary_lower_bound(0, n, p, &get);
            assert_eq!(lower_bound_opk(n, p, stride, get), oracle, "stride={stride} p={p:02x?}");
            for &hint in &[0usize, n / 3, n, oracle] {
                assert_eq!(gallop_lower_bound_bytes(n, p, hint, get), oracle);
                assert_eq!(
                    gallop_opk(n, p, hint, stride, get),
                    oracle,
                    "gallop stride={stride} p={p:02x?}"
                );
            }
        }
    }
}
