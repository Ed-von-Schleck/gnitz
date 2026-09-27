use super::*;
use crate::schema::{SchemaColumn, SchemaDescriptor, TypeCode};
use crate::test_support::pk_only_schema;

/// A NULL and a non-null `0` in an `I64` payload column: the same bytes, since a
/// NULL cell is zeroed. [`compare_rows`] reads the null bit and orders them apart;
/// [`FixedIntNonnull`] never reads the null word and calls them one element —
/// which is why a nullable schema must resolve to `Generic`.
#[test]
fn null_equality_separates_the_two_payload_comparators() {
    use crate::storage::Batch;

    let pk = SchemaColumn::new(TypeCode::U128, false);
    let nullable = SchemaDescriptor::new(&[pk, SchemaColumn::new(TypeCode::I64, true)], &[0]);
    let non_null = SchemaDescriptor::new(&[pk, SchemaColumn::new(TypeCode::I64, false)], &[0]);

    let mut pair = Batch::with_capacity(&nullable, 2);
    for null_word in [1u64, 0] {
        pair.extend_pk(0x1234_5678_9abc_def0);
        pair.extend_weight(&1i64.to_le_bytes());
        pair.extend_null_bmp(&null_word.to_le_bytes());
        pair.extend_col(0, &0i64.to_le_bytes());
        pair.count += 1;
    }

    assert_eq!(
        compare_rows(&nullable, &pair, 0, &pair, 1),
        Ordering::Less,
        "null-aware: NULL sorts before a non-null 0",
    );
    assert_eq!(
        FixedIntNonnull.compare(&non_null, &pair, 0, &pair, 1),
        Ordering::Equal,
        "null-blind: the fixed-int comparator sees two zero cells",
    );
}

/// A minimal `RowSource` for unit tests.
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

/// Build a 3-column schema: [PK:U64, nullable I64, F64].
fn make_schema_nullable_float() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::I64, true),
            SchemaColumn::new(TypeCode::F64, false),
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

/// Build a single-payload-column TestBatch.
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

/// The payload comparator's order contract, one case per type rule. Each is
/// asserted in both directions, so antisymmetry needs no separate test.
#[test]
fn compare_rows_orders_every_payload_type() {
    // Low `n` little-endian bytes: two's complement for signed, zero-extended
    // for unsigned — the payload cell layout.
    let le = |v: i128, n: usize| v.to_le_bytes()[..n].to_vec();
    let f = |v: f64| v.to_bits().to_le_bytes().to_vec();

    // (what, payload type, row 0 cell, row 1 cell, row 0 vs row 1)
    type Case<'a> = (&'a str, TypeCode, Vec<u8>, Vec<u8>, Ordering);
    let cases: &[Case] = &[
        (
            "signed reads two's complement",
            TypeCode::I64,
            le(-42, 8),
            le(42, 8),
            Ordering::Less,
        ),
        (
            "u128 orders across the limb",
            TypeCode::U128,
            le(1, 16),
            le(1 << 64, 16),
            Ordering::Less,
        ),
        // A cross-sign `_join_pk` surfaced into a payload slot: read as unsigned,
        // -1 (all bits set) would sort above 0.
        (
            "i128 is signed, not u128",
            TypeCode::I128,
            le(-1, 16),
            le(0, 16),
            Ordering::Less,
        ),
        // A 16-byte read returning 0 would make every UUID compare Equal and
        // silently drop rows in consolidation.
        (
            "distinct UUIDs do not collapse",
            TypeCode::UUID,
            le(1, 16),
            le(1 << 64, 16),
            Ordering::Less,
        ),
        ("f64 by sign", TypeCode::F64, f(-5.0), f(5.0), Ordering::Less),
        (
            "total_cmp puts NaN above every finite",
            TypeCode::F64,
            f(f64::NAN),
            f(1.0),
            Ordering::Greater,
        ),
        (
            "NaN ties with itself",
            TypeCode::F64,
            f(f64::NAN),
            f(f64::NAN),
            Ordering::Equal,
        ),
        // An unsigned high bit read as a sign would reverse the order.
        (
            "u64 high bit is not a sign",
            TypeCode::U64,
            le(0, 8),
            le(u64::MAX as i128, 8),
            Ordering::Less,
        ),
        (
            "u32 high bit is not a sign",
            TypeCode::U32,
            le(0, 4),
            le(u32::MAX as i128, 4),
            Ordering::Less,
        ),
        (
            "u16 high bit is not a sign",
            TypeCode::U16,
            le(0, 2),
            le(u16::MAX as i128, 2),
            Ordering::Less,
        ),
        (
            "u8 high bit is not a sign",
            TypeCode::U8,
            le(0, 1),
            le(u8::MAX as i128, 1),
            Ordering::Less,
        ),
    ];

    for (what, tc, a, b, want) in cases {
        let schema = make_schema(&[(*tc, false)]);
        let batch = single_col_batch(&[0, 0], [a.clone(), b.clone()].concat());
        assert_eq!(compare_rows(&schema, &batch, 0, &batch, 1), *want, "{what}");
        assert_eq!(
            compare_rows(&schema, &batch, 1, &batch, 0),
            want.reverse(),
            "{what}, reversed"
        );
    }
}

/// NULL sorts below non-null, and two NULLs tie so the next column decides.
#[test]
fn null_orders_below_non_null_and_ties_to_the_next_column() {
    let schema = make_schema_nullable_float();
    // (null word, I64, F64) — null-word bit 0 marks payload col 0 NULL.
    let b = batch_from_rows(&[(1, 0, -5.0), (0, 10, 5.0), (1, 0, 5.0)]);
    assert_eq!(compare_rows(&schema, &b, 0, &b, 1), Ordering::Less);
    assert_eq!(compare_rows(&schema, &b, 1, &b, 0), Ordering::Greater);
    // Rows 0 and 2 are both NULL in col 0, so the F64 column decides.
    assert_eq!(compare_rows(&schema, &b, 0, &b, 2), Ordering::Less);
}

/// The two rows compared may come from different sources — production compares a
/// shard against a `MemBatch` — and each side must resolve its long STRING cell
/// against its *own* blob arena. Two sources with distinct arenas is the only
/// shape that catches a swapped pair.
#[test]
fn compare_rows_resolves_each_source_against_its_own_blob() {
    let schema = make_schema(&[(TypeCode::String, false)]);
    let one_long_string = |s: &[u8]| {
        let mut blob = Vec::new();
        let cell = gnitz_wire::encode_german_string(s, &mut blob);
        TestBatch {
            null_bmp: 0u64.to_le_bytes().to_vec(),
            col_data: vec![cell.to_vec()],
            blob,
        }
    };
    let a = one_long_string(b"a string well past the inline threshold");
    let b = one_long_string(b"b string well past the inline threshold");
    assert_eq!(compare_rows(&schema, &a, 0, &b, 0), Ordering::Less);
    assert_eq!(compare_rows(&schema, &b, 0, &a, 0), Ordering::Greater);
}

// -----------------------------------------------------------------------
// Fast path: PayloadCmpKind / FixedIntNonnull
// -----------------------------------------------------------------------

fn make_schema(cols: &[(TypeCode, bool)]) -> SchemaDescriptor {
    // First column is PK (U64); subsequent are payload columns.
    let mut columns = [SchemaColumn::EMPTY; crate::schema::MAX_COLUMNS];
    columns[0] = SchemaColumn::new(TypeCode::U64, false);
    for (i, &(tc, nullable)) in cols.iter().enumerate() {
        columns[i + 1] = SchemaColumn::new(tc, nullable);
    }
    let n = 1 + cols.len();
    SchemaDescriptor::new(&columns[..n], &[0])
}

/// `PayloadCmpKind`: all-signed, all-unsigned, and mixed-sign non-null schemas
/// all resolve to `FixedIntNonnull`; nullable and U128/UUID/F32/F64/STRING/BLOB
/// fall to `Generic`.
#[test]
fn payload_cmp_kind_bounds() {
    let fast = |s: &SchemaDescriptor| s.payload_cmp == PayloadCmpKind::FixedIntNonnull;

    // All-signed, non-nullable → fast
    assert!(fast(&make_schema(&[
        (TypeCode::I8, false),
        (TypeCode::I16, false),
        (TypeCode::I32, false),
        (TypeCode::I64, false),
    ])));
    // All-unsigned, non-nullable → fast
    assert!(fast(&make_schema(&[
        (TypeCode::U8, false),
        (TypeCode::U16, false),
        (TypeCode::U32, false),
        (TypeCode::U64, false),
    ])));
    // Mixed signed/unsigned, non-nullable → fast
    assert!(fast(&make_schema(&[(TypeCode::I64, false), (TypeCode::U32, false)])));
    // Empty payload (all-PK) → vacuously fast
    assert!(fast(&pk_only_schema(&[TypeCode::U64])));

    // Nullable fixed int → generic
    assert!(!fast(&make_schema(&[(TypeCode::I32, true)])));
    assert!(!fast(&make_schema(&[(TypeCode::U32, true)])));

    // Each non-fixed-int type → generic (U128/UUID exceed 8 bytes; floats/strings/blobs)
    for tc in [
        TypeCode::U128,
        TypeCode::UUID,
        TypeCode::F32,
        TypeCode::F64,
        TypeCode::String,
        TypeCode::Blob,
    ] {
        assert!(
            !fast(&make_schema(&[(tc, false)])),
            "expected schema with type_code={tc} to be rejected",
        );
    }

    // A single disqualifier among valid columns kills it.
    assert!(!fast(&make_schema(&[(TypeCode::I64, false), (TypeCode::U128, false)])));
}

// -----------------------------------------------------------------------
// FixedIntNonnull ≡ Generic property test
// -----------------------------------------------------------------------

mod fixedint_proptest {
    use super::*;
    use proptest::prelude::*;

    fn arb_fixedint_type() -> impl Strategy<Value = TypeCode> {
        prop_oneof![
            Just(TypeCode::I8),
            Just(TypeCode::U8),
            Just(TypeCode::I16),
            Just(TypeCode::U16),
            Just(TypeCode::I32),
            Just(TypeCode::U32),
            Just(TypeCode::I64),
            Just(TypeCode::U64),
        ]
    }

    /// `(payload type codes, n rows, per-column row-major bytes)`. 1..=4
    /// columns over all eight fixed-int types spans every sign combination
    /// including mixed; random bytes frequently set the high bit, so a
    /// signedness bug surfaces as a fast-vs-generic disagreement.
    fn arb_case() -> impl Strategy<Value = (Vec<TypeCode>, usize, Vec<Vec<u8>>)> {
        (prop::collection::vec(arb_fixedint_type(), 1..=4), 2usize..=6usize).prop_flat_map(|(types, n)| {
            let cols: Vec<_> = types
                .iter()
                .map(|&t| {
                    let cs = t.wire_stride();
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
            let payload: Vec<(TypeCode, bool)> = types.iter().map(|&t| (t, false)).collect();
            let schema = make_schema(&payload);
            prop_assert_eq!(schema.payload_cmp, PayloadCmpKind::FixedIntNonnull);

            let batch = TestBatch { null_bmp: vec![0u8; n * 8], col_data, blob: vec![] };
            for i in 0..n {
                for j in 0..n {
                    prop_assert_eq!(
                        FixedIntNonnull.compare(&schema, &batch, i, &batch, j),
                        compare_rows(&schema, &batch, i, &batch, j),
                        "mismatch at ({}, {})", i, j,
                    );
                }
            }
        }
    }
}
