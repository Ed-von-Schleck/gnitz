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
    type Case<'a> = (&'a str, u8, Vec<u8>, Vec<u8>, Ordering);
    let cases: &[Case] = &[
        (
            "signed reads two's complement",
            type_code::I64,
            le(-42, 8),
            le(42, 8),
            Ordering::Less,
        ),
        (
            "u128 orders across the limb",
            type_code::U128,
            le(1, 16),
            le(1 << 64, 16),
            Ordering::Less,
        ),
        // A cross-sign `_join_pk` surfaced into a payload slot: read as unsigned,
        // -1 (all bits set) would sort above 0.
        (
            "i128 is signed, not u128",
            type_code::I128,
            le(-1, 16),
            le(0, 16),
            Ordering::Less,
        ),
        // A 16-byte read returning 0 would make every UUID compare Equal and
        // silently drop rows in consolidation.
        (
            "distinct UUIDs do not collapse",
            type_code::UUID,
            le(1, 16),
            le(1 << 64, 16),
            Ordering::Less,
        ),
        ("f64 by sign", type_code::F64, f(-5.0), f(5.0), Ordering::Less),
        (
            "total_cmp puts NaN above every finite",
            type_code::F64,
            f(f64::NAN),
            f(1.0),
            Ordering::Greater,
        ),
        (
            "NaN ties with itself",
            type_code::F64,
            f(f64::NAN),
            f(f64::NAN),
            Ordering::Equal,
        ),
        // An unsigned high bit read as a sign would reverse the order.
        (
            "u64 high bit is not a sign",
            type_code::U64,
            le(0, 8),
            le(u64::MAX as i128, 8),
            Ordering::Less,
        ),
        (
            "u32 high bit is not a sign",
            type_code::U32,
            le(0, 4),
            le(u32::MAX as i128, 4),
            Ordering::Less,
        ),
        (
            "u16 high bit is not a sign",
            type_code::U16,
            le(0, 2),
            le(u16::MAX as i128, 2),
            Ordering::Less,
        ),
        (
            "u8 high bit is not a sign",
            type_code::U8,
            le(0, 1),
            le(u8::MAX as i128, 1),
            Ordering::Less,
        ),
    ];

    for (what, tc, a, b, want) in cases {
        let schema = make_schema(&[(*tc, 0)]);
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
    let schema = make_schema(&[(type_code::STRING, 0)]);
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

/// The register dispatch (`lower_bound_opk` / `gallop_opk`) must return the same
/// index as the byte oracle at every width arm — `u64` (≤8), `u128` (9..=16),
/// `[u128; 2]` (17..=32) and the byte fallback (>32). Values span two bytes, so
/// a wrong-endian load miscompares rather than passing by accident.
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
