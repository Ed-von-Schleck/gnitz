use super::*;
use crate::schema::{IndexKeySpec, Placement, SchemaColumn, SchemaDescriptor, TypeCode};
use crate::storage::BatchBuilder;
use crate::test_support::{opk_pk, pk_only_schema};
use gnitz_wire::{read_signed_exact, read_unsigned_exact, Cut, KeyRange, PkColList};

/// Independent typed reference comparator over native-LE PK bytes. This is
/// the per-column column-walk that `compare_pk_bytes` used *before* the OPK
/// flip; kept here as the test oracle for "OPK byte order == typed order".
fn typed_cmp_pk_le(schema: &SchemaDescriptor, a: &[u8], b: &[u8]) -> Ordering {
    let mut off = 0usize;
    for (_ord, col) in schema.pk_columns() {
        let cs = col.size() as usize;
        let ord = match col.type_code {
            TypeCode::U128 | TypeCode::UUID => {
                let va = u128::from_le_bytes(a[off..off + 16].try_into().unwrap());
                let vb = u128::from_le_bytes(b[off..off + 16].try_into().unwrap());
                va.cmp(&vb)
            }
            TypeCode::I128 => {
                let va = i128::from_le_bytes(a[off..off + 16].try_into().unwrap());
                let vb = i128::from_le_bytes(b[off..off + 16].try_into().unwrap());
                va.cmp(&vb)
            }
            TypeCode::U64 | TypeCode::U32 | TypeCode::U16 | TypeCode::U8 => {
                read_unsigned_exact(&a[off..off + cs]).cmp(&read_unsigned_exact(&b[off..off + cs]))
            }
            _ => read_signed_exact(&a[off..off + cs]).cmp(&read_signed_exact(&b[off..off + cs])),
        };
        if ord != Ordering::Equal {
            return ord;
        }
        off += cs;
    }
    Ordering::Equal
}

/// Compare two native-LE PK tuples the way storage now does: encode each to
/// OPK, then `compare_pk_bytes` (a raw memcmp). Mirrors the read path.
fn cmp_pk_le(schema: &SchemaDescriptor, a: &[u8], b: &[u8]) -> Ordering {
    compare_pk_bytes(schema.opk_key(a).pk_bytes(), schema.opk_key(b).pk_bytes())
}

/// The load-bearing OPK property: a raw memcmp of the order-preserving keys
/// equals the typed lexicographic comparison of the PK columns.
fn assert_opk_equivalence(schema: &SchemaDescriptor, a: &[u8], b: &[u8]) {
    assert_eq!(
        cmp_pk_le(schema, a, b),
        typed_cmp_pk_le(schema, a, b),
        "OPK order disagrees with typed comparison for a={a:?} b={b:?}",
    );
}

/// Sweep one single-column PK schema pairwise over `vals`: the OPK memcmp
/// order at rest must equal `T`'s native typed order — an oracle fully
/// independent of the in-file `typed_cmp_pk_le` — and must also agree with
/// `typed_cmp_pk_le` over the same bytes. The `i == j` diagonal is the
/// equal-buffer case.
fn assert_single_col_order<T: Ord + Copy + std::fmt::Debug>(tc: TypeCode, vals: &[T], le: impl Fn(T) -> Vec<u8>) {
    let s = pk_only_schema(&[tc]);
    for &a in vals {
        for &b in vals {
            let (ab, bb) = (le(a), le(b));
            assert_eq!(cmp_pk_le(&s, &ab, &bb), a.cmp(&b), "type_code {tc}: {a:?} vs {b:?}");
            assert_opk_equivalence(&s, &ab, &bb);
        }
    }
}

/// Boundary sweeps for every PK-eligible scalar type. The value sets pin
/// sign-extension (`-1 < 1`), zero-extension (`0xFFFE > 1`), LE-vs-lex byte
/// order (`1 < 256`), and the 2^63 / 2^64 width boundaries that separate a
/// U64 image from an I64 one.
#[test]
fn opk_order_matches_native_order_per_type() {
    assert_single_col_order(TypeCode::U8, &[0u8, 1, 0x7F, 0x80, 0xFF], |v| vec![v]);
    assert_single_col_order(TypeCode::I8, &[i8::MIN, -1, 0, 1, i8::MAX], |v| vec![v as u8]);
    let le16 = |v: u16| v.to_le_bytes().to_vec();
    assert_single_col_order(TypeCode::U16, &[0u16, 1, 0x0100, 0x8000, 0xFFFE, u16::MAX], le16);
    assert_single_col_order(TypeCode::I16, &[i16::MIN, -1, 0, 1, i16::MAX], |v: i16| {
        v.to_le_bytes().to_vec()
    });
    let le32 = |v: u32| v.to_le_bytes().to_vec();
    assert_single_col_order(TypeCode::U32, &[0u32, 1, 256, 0x8000_0000, 0xFFFF_FFFE, u32::MAX], le32);
    assert_single_col_order(TypeCode::I32, &[i32::MIN, -1, 0, 1, i32::MAX], |v: i32| {
        v.to_le_bytes().to_vec()
    });
    assert_single_col_order(TypeCode::U64, &[0u64, 1, 2, 256, 1 << 63, u64::MAX], |v: u64| {
        v.to_le_bytes().to_vec()
    });
    assert_single_col_order(TypeCode::I64, &[i64::MIN, -1, 0, 1, i64::MAX], |v: i64| {
        v.to_le_bytes().to_vec()
    });
    let le128 = |v: u128| v.to_le_bytes().to_vec();
    let wide = [0u128, 1, u64::MAX as u128, u64::MAX as u128 + 1, 1 << 127, u128::MAX];
    assert_single_col_order(TypeCode::U128, &wide, le128);
    assert_single_col_order(TypeCode::UUID, &wide, le128);
    assert_single_col_order(
        TypeCode::I128,
        &[i128::MIN, -1, 0, 1, 1i128 << 63, 1i128 << 64, i128::MAX],
        |v: i128| v.to_le_bytes().to_vec(),
    );
}

#[test]
fn compare_pk_bytes_compound_u64_u64() {
    let s = pk_only_schema(&[TypeCode::U64, TypeCode::U64]);
    let mk = |a: u64, b: u64| {
        let mut v = Vec::with_capacity(16);
        v.extend_from_slice(&a.to_le_bytes());
        v.extend_from_slice(&b.to_le_bytes());
        v
    };
    let r0 = mk(1, 5);
    let r1 = mk(1, 9);
    let r2 = mk(2, 1);
    // Same first column, second column tiebreaks ascending.
    assert_eq!(cmp_pk_le(&s, &r0, &r1), Ordering::Less);
    // First column dominates (would be Greater under a u128 LE compare,
    // which would treat the second column as the high-order bits).
    assert_eq!(cmp_pk_le(&s, &r1, &r2), Ordering::Less);
    assert_opk_equivalence(&s, &r0, &r1);
    assert_opk_equivalence(&s, &r1, &r2);
    assert_opk_equivalence(&s, &r0, &r2);
    // Equal compound buffers compare Equal at every column.
    assert_eq!(cmp_pk_le(&s, &r0, &r0), Ordering::Equal);
    assert_opk_equivalence(&s, &r0, &r0);
}

#[test]
fn compare_pk_bytes_compound_mixed() {
    let s = pk_only_schema(&[TypeCode::U64, TypeCode::I32]);
    let mk = |a: u64, b: i32| {
        let mut v = Vec::with_capacity(12);
        v.extend_from_slice(&a.to_le_bytes());
        v.extend_from_slice(&b.to_le_bytes());
        v
    };
    let neg = mk(1, -5);
    let zero = mk(1, 0);
    // Per-column dispatch picks read_signed_exact for col 1 even though col 0
    // is unsigned: -5 < 0.
    assert_eq!(cmp_pk_le(&s, &neg, &zero), Ordering::Less);
    assert_opk_equivalence(&s, &neg, &zero);
}

#[test]
fn compare_pk_bytes_pk_indices_order_not_schema_order() {
    // Schema [U64, U64] with pk_indices = [1, 0]: column 1 is the first
    // PK column. The byte layout follows pk_cols() order, so the
    // first 8 bytes correspond to column 1.
    let s = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::U64, false),
        ],
        &[1, 0],
    );
    // (col1=1, col0=5) vs (col1=2, col0=0): col1 dominates.
    let mut a = Vec::with_capacity(16);
    a.extend_from_slice(&1u64.to_le_bytes()); // col1
    a.extend_from_slice(&5u64.to_le_bytes()); // col0
    let mut b = Vec::with_capacity(16);
    b.extend_from_slice(&2u64.to_le_bytes()); // col1
    b.extend_from_slice(&0u64.to_le_bytes()); // col0
    assert_eq!(cmp_pk_le(&s, &a, &b), Ordering::Less);
    // Encoder iterates pk-list order [1,0], same as the comparator.
    assert_opk_equivalence(&s, &a, &b);
}

/// Every specialized arm of `pack_pk_be` — the `{2,4,8,≥16}` register loads
/// and the `9..=15` overlapping pair — must be byte-value-identical to the
/// generic pad-and-copy at every width. A changed value would silently
/// corrupt every `pack_pk_be` consumer (the cached sort keys, the route/guard
/// keys, the bloom probes). Sweeps `0..=16` so no arm boundary is untested.
#[test]
fn pack_pk_be_specialization_matches_naive() {
    fn naive(pk: &[u8]) -> u128 {
        let take = pk.len().min(16);
        let mut buf = [0u8; 16];
        buf[..take].copy_from_slice(&pk[..take]);
        u128::from_be_bytes(buf)
    }
    for width in (0usize..=16).chain([24, 80]) {
        for seed in 0u32..256 {
            let bytes: Vec<u8> = (0..width)
                .map(|i| seed.wrapping_mul(31).wrapping_add(i as u32) as u8)
                .collect();
            assert_eq!(pack_pk_be(&bytes), naive(&bytes), "width {width} seed {seed}");
            // Same width taxonomy as `gnitz_wire::widen_pk_be`, opposite
            // alignment. Pinned here rather than expressed in code: doing the
            // latter costs a runtime 128-bit shift at every narrow width.
            if (1..16).contains(&width) {
                assert_eq!(
                    pack_pk_be(&bytes),
                    gnitz_wire::widen_pk_be(&bytes[..width]) << (8 * (16 - width)),
                    "width {width} seed {seed}: widen_pk_be identity",
                );
            }
        }
    }
}

/// `leading_u64` reads the first eight OPK bytes at *any* stride — including
/// past eight, which is where it parts company with `PkSortKey::<u64>::from_opk`
/// (dispatched only at strides ≤ 8, and out of bounds above them). A delta
/// store's `_tick ‖ view PK` key is always in that upper band.
#[test]
fn leading_u64_reads_eight_bytes_at_every_stride() {
    let bytes: Vec<u8> = (1..=24u8).collect();
    for width in [0usize, 1, 4, 8, 9, 16, 24] {
        let mut want = [0u8; 8];
        let n = width.min(8);
        want[..n].copy_from_slice(&bytes[..n]);
        assert_eq!(
            leading_u64(&bytes[..width]),
            u64::from_be_bytes(want),
            "width {width}: the leading eight bytes, right-zero-padded",
        );
        if width <= 8 {
            assert_eq!(
                <u64 as PkSortKey<'_>>::from_opk(&bytes[..width]),
                leading_u64(&bytes[..width]),
                "width {width}: the sort key is the value accessor at strides ≤ 8",
            );
        }
    }
}

#[test]
fn pk_bytes_eq_matches_byte_equality_direct() {
    // Wide (> 16): fully-equal → true; equal leading-16 prefix, differing
    // suffix → false (the tiebreak arm, otherwise only covered two layers up).
    let mut a = [0u8; 24];
    let mut b = [0u8; 24];
    for i in 0..24 {
        a[i] = i as u8;
        b[i] = i as u8;
    }
    assert!(pk_bytes_eq(&a, &b));
    b[16] ^= 1;
    assert!(!pk_bytes_eq(&a, &b));
    assert_eq!(pk_bytes_eq(&a, &b), a[..] == b[..]);
    // Narrow (≤ 16): the register arm, equal and unequal.
    let x = [1u8; 8];
    let mut y = [1u8; 8];
    assert!(pk_bytes_eq(&x, &y));
    y[7] = 2;
    assert!(!pk_bytes_eq(&x, &y));
}

// -----------------------------------------------------------------------
// OPK ↔ compare_pk_bytes property test
// -----------------------------------------------------------------------

mod opk_proptest {
    use super::*;
    use crate::test_support::arb_pk_type;
    use proptest::prelude::*;

    /// `(column type codes, pk_indices permutation, a_bytes, b_bytes)`.
    /// The permutation exercises non-identity `pk_indices` (e.g. `[1, 0]`),
    /// and 1..=4 columns spans both narrow (≤16) and wide (>16) strides.
    fn arb_pk_case() -> impl Strategy<Value = (Vec<TypeCode>, Vec<u32>, Vec<u8>, Vec<u8>)> {
        prop::collection::vec(arb_pk_type(), 1..=4).prop_flat_map(|types| {
            let stride: usize = types.iter().map(|&t| t.wire_stride()).sum();
            let n = types.len();
            (
                Just(types),
                Just((0..n as u32).collect::<Vec<u32>>()).prop_shuffle(),
                prop::collection::vec(any::<u8>(), stride),
                prop::collection::vec(any::<u8>(), stride),
            )
        })
    }

    proptest! {
        /// The order-preserving key agrees with `compare_pk_bytes` for every
        /// PK-eligible type, every 1..=4-column compound arrangement, and any
        /// `pk_indices` permutation — over random PK byte tuples.
        #[test]
        fn opk_matches_compare_pk_bytes((types, perm, a, b) in arb_pk_case()) {
            let cols: Vec<SchemaColumn> =
                types.iter().map(|&tc| SchemaColumn::new(tc, false)).collect();
            let s = SchemaDescriptor::new(&cols, &perm);
            assert_opk_equivalence(&s, &a, &b);
        }
    }

    proptest! {
        /// `compare_pk_ordering` agrees with the authoritative byte comparator
        /// at every PK width — narrow (register `pack_pk_be`) and wide
        /// (>16-byte prefix + `compare_pk_bytes` tiebreak) alike — so
        /// `== Ordering::Equal` is exactly byte equality, the property the
        /// N-way merge fold and the single-batch drain rely on for grouping.
        #[test]
        fn compare_pk_ordering_matches_byte_compare((types, perm, a, b) in arb_pk_case()) {
            let cols: Vec<SchemaColumn> =
                types.iter().map(|&tc| SchemaColumn::new(tc, false)).collect();
            let s = SchemaDescriptor::new(&cols, &perm);
            let (oa, ob) = (s.opk_key(&a), s.opk_key(&b));
            let (oa, ob) = (oa.pk_bytes(), ob.pk_bytes());
            prop_assert_eq!(compare_pk_ordering(oa, ob), compare_pk_bytes(oa, ob));
            prop_assert_eq!(
                compare_pk_ordering(oa, ob) == std::cmp::Ordering::Equal,
                oa == ob
            );
        }
    }

    proptest! {
        /// `pk_bytes_eq` is exactly byte-equality of the OPK regions at every
        /// PK width — the register-arm narrow case and the prefix-tiebreak wide
        /// case alike — the property every "same PK" merge/group fold relies on.
        #[test]
        fn pk_bytes_eq_matches_byte_equality((types, perm, a, b) in arb_pk_case()) {
            let cols: Vec<SchemaColumn> =
                types.iter().map(|&tc| SchemaColumn::new(tc, false)).collect();
            let s = SchemaDescriptor::new(&cols, &perm);
            let (oa, ob) = (s.opk_key(&a), s.opk_key(&b));
            let (oa, ob) = (oa.pk_bytes(), ob.pk_bytes());
            prop_assert_eq!(pk_bytes_eq(oa, ob), oa == ob);
            prop_assert!(pk_bytes_eq(oa, oa));
        }
    }
}

// ── seek_opk_bytes: the width-universal seek-key encoder ─────────────────

#[test]
fn seek_opk_bytes_narrow_matches_opk_key() {
    // A 16-byte word against a narrower stride reads only the stride.
    let cases = [
        pk_only_schema(&[TypeCode::U8]),  // stride 1
        pk_only_schema(&[TypeCode::U32]), // stride 4
        pk_only_schema(&[TypeCode::U64]), // stride 8
        pk_only_schema(&[TypeCode::I64]), // stride 8, signed → OPK flips the sign bit
        // Compound (U32, U32) with a *permuted* PK list [1, 0]: stride 8,
        // exercises the multi-column pk-list walk in the encoder.
        SchemaDescriptor::new(
            &[
                SchemaColumn::new(TypeCode::U32, false),
                SchemaColumn::new(TypeCode::U32, false),
            ],
            &[1, 0],
        ),
    ];
    for s in cases {
        for v in [0u128, 1, 0x0123_4567_89AB_CDEF, 0x8000_0000_0000_0000, u64::MAX as u128] {
            let want = s.opk_key(&v.to_le_bytes());
            let got = seek_opk_bytes(&s, &v.to_le_bytes()).expect("narrow seek encodes");
            assert_eq!(got, want, "narrow seek must match opk_key for {s:?} v={v:#x}");
        }
    }
}

#[test]
fn seek_opk_bytes_wide_keys_roundtrip() {
    // All-unsigned, so the OPK is each column's big-endian image.
    let s = pk_only_schema(&[TypeCode::U64; 3]);
    let cols: [u64; 3] = [0x1122_3344_5566_7788, 0x99AA_BBCC_DDEE_FF00, 0x0102_0304_0506_0708];
    let key: Vec<u8> = cols.iter().flat_map(|v| v.to_le_bytes()).collect();
    let opk = seek_opk_bytes(&s, &key).expect("24-byte key encodes");
    let want: Vec<u8> = cols.iter().flat_map(|v| v.to_be_bytes()).collect();
    assert_eq!(opk.pk_bytes(), want.as_slice());

    let s = pk_only_schema(&[TypeCode::U128; 4]);
    let cols: [u128; 4] = [
        0x0102_0304_0506_0708_090A_0B0C_0D0E_0F10,
        0x1112_1314_1516_1718_191A_1B1C_1D1E_1F20,
        0x2122_2324_2526_2728_292A_2B2C_2D2E_2F30,
        0x3132_3334_3536_3738_393A_3B3C_3D3E_3F40,
    ];
    let key: Vec<u8> = cols.iter().flat_map(|v| v.to_le_bytes()).collect();
    let opk = seek_opk_bytes(&s, &key).expect("64-byte key encodes");
    let want: Vec<u8> = cols.iter().flat_map(|v| v.to_be_bytes()).collect();
    assert_eq!(opk.pk_bytes(), want.as_slice());
}

#[test]
fn seek_opk_bytes_short_key_errs() {
    for (tc, stride) in [
        (TypeCode::U8, 1),
        (TypeCode::U32, 4),
        (TypeCode::U64, 8),
        (TypeCode::U128, 16),
    ] {
        let s = pk_only_schema(&[tc]);
        assert!(seek_opk_bytes(&s, &vec![0u8; stride - 1]).is_err(), "stride {stride}");
    }
}

#[test]
fn pkbuf_reused_buffer_matches_a_fresh_key_of_the_same_width() {
    use std::collections::HashSet;
    // A scratch buffer that held a wider key, rewritten narrower: the bytes past
    // the new width must not survive into eq/hash, or a reused key would stop
    // matching the fresh one it is supposed to equal.
    let mut a = PkBuf::from_bytes(&[0xABu8; 12]);
    a.write(8, |dst| dst.copy_from_slice(&7u64.to_le_bytes()));
    let b = PkBuf::from_bytes(&7u64.to_le_bytes());
    assert_eq!(a, b);
    let mut set: HashSet<PkBuf> = HashSet::new();
    set.insert(b);
    assert!(set.contains(&a));
    // And the zero tail is what makes `padded` sound over the reused buffer.
    assert_eq!(a.padded(12), &[7, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
}

#[test]
fn pkbuf_borrow_heterogeneous_lookup() {
    use std::collections::HashSet;
    let mut set: HashSet<PkBuf> = HashSet::new();
    set.insert(PkBuf::from_bytes(&123u64.to_le_bytes()));
    // Raw &[u8] lookup via Borrow<[u8]> — no PkBuf construction.
    assert!(set.contains(&123u64.to_le_bytes()[..]));
    assert!(!set.contains(&124u64.to_le_bytes()[..]));
}

// --- increment_key_in_place -------------------------------------------

#[test]
fn succ_increments_low_byte() {
    let mut k = [0x00, 0x00, 0x05];
    assert!(increment_key_in_place(&mut k));
    assert_eq!(k, [0x00, 0x00, 0x06]);
}

#[test]
fn succ_ripples_carry() {
    let mut k = [0x00, 0x00, 0xFF];
    assert!(increment_key_in_place(&mut k));
    assert_eq!(k, [0x00, 0x01, 0x00]);
}

#[test]
fn succ_carries_out_on_all_ff() {
    let mut k = [0xFF, 0xFF];
    assert!(!increment_key_in_place(&mut k));
    assert_eq!(k, [0x00, 0x00]);
}

#[test]
fn succ_empty_carries_out() {
    let mut k: [u8; 0] = [];
    assert!(!increment_key_in_place(&mut k));
}

// --- range_shares_prefix ----------------------------------------------

fn buf(bytes: &[u8]) -> PkBuf {
    PkBuf::from_bytes(bytes)
}

/// The last key is `end - 1`, so a range ending exactly at the next group's
/// first key still shares the prefix — one past that does not.
#[test]
fn shares_prefix_stops_at_the_group_boundary() {
    let start = buf(&[0x07, 0x00]);
    assert!(
        range_shares_prefix(&start, Some(&buf(&[0x07, 0x01])), 1),
        "one key wide"
    );
    assert!(
        range_shares_prefix(&start, Some(&buf(&[0x08, 0x00])), 1),
        "the whole group"
    );
    assert!(
        !range_shares_prefix(&start, Some(&buf(&[0x08, 0x01])), 1),
        "one key past"
    );
    // A borrow chain out of the trailing byte still lands in the group.
    assert!(range_shares_prefix(&buf(&[0x07, 0x05]), Some(&buf(&[0x08, 0x00])), 1));
}

/// `end == None` runs to the table end (all-`0xFF`), which only the topmost
/// group shares a prefix with — that is what confines a maximal-value point.
#[test]
fn shares_prefix_handles_an_unbounded_end() {
    assert!(range_shares_prefix(&buf(&[0xFF, 0xFF]), None, 1));
    assert!(!range_shares_prefix(&buf(&[0x07, 0x00]), None, 1));
    // A zero-width distribution prefix is shared by everything.
    assert!(range_shares_prefix(&buf(&[0x07, 0x00]), None, 0));
}

#[test]
fn pkbuf_wide_differs_past_byte_16() {
    // Two 24-byte keys identical in the first 16 bytes but differing in the
    // last 8 must be distinct — the failure mode of any u128-truncating key.
    let mut x = Vec::new();
    x.extend_from_slice(&1u64.to_le_bytes());
    x.extend_from_slice(&2u64.to_le_bytes());
    x.extend_from_slice(&3u64.to_le_bytes());
    let mut y = x.clone();
    y[16..24].copy_from_slice(&999u64.to_le_bytes());
    let px = PkBuf::from_bytes(&x);
    let py = PkBuf::from_bytes(&y);
    assert!(px != py);
    let mut set = std::collections::HashSet::new();
    set.insert(px);
    assert!(!set.contains(&py));
}

/// `PkBuf` byte order is a valid merge order: byte-lexicographic comparison of
/// the OPK spans equals the order the worker sorts and the master's heap pops,
/// at any width. A composite span sorts by its leading column, then trailing.
#[test]
fn pkbuf_byte_order_is_lexicographic() {
    let span = |a: u64, b: u64| {
        let mut buf = [0u8; 16];
        buf[..8].copy_from_slice(&a.to_be_bytes());
        buf[8..].copy_from_slice(&b.to_be_bytes());
        PkBuf::from_bytes(&buf)
    };
    let mut v = vec![span(2, 0), span(1, 9), span(1, 1), span(2, 0)];
    v.sort_unstable();
    assert_eq!(
        v,
        vec![span(1, 1), span(1, 9), span(2, 0), span(2, 0)],
        "sort is by leading column then trailing — byte-lexicographic"
    );
    // A narrower span sorts before a wider one sharing its prefix (memcmp over
    // bytes[..len]).
    assert!(PkBuf::from_bytes(&[1u8, 2]) < PkBuf::from_bytes(&[1u8, 2, 0]));
}

/// A row NULL in ANY indexed column is skipped (`key_bytes` → false) —
/// SQL NULL-distinctness, mirroring `Batch::project_index`.
#[test]
fn index_key_spec_skips_any_null_column() {
    let owner = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::U64, true), // a: nullable payload
            SchemaColumn::new(TypeCode::U64, true), // b: nullable payload
        ],
        &[0],
    );
    let cols = [1u32, 2];
    // (id, a, b): both present, a NULL, b NULL.
    let rows: [(u128, Option<u128>, Option<u128>); 3] = [
        (1, Some(5), Some(6)), // both present → indexed
        (2, None, Some(6)),    // a NULL → skipped
        (3, Some(5), None),    // b NULL → skipped
    ];
    let mut bb = BatchBuilder::new(owner);
    for &(id, a, b) in &rows {
        bb.begin_row(id, 1);
        for cell in [a, b] {
            bb.put_opt_int(cell);
        }
        bb.end_row();
    }
    let batch = bb.finish();
    let spec = IndexKeySpec::new(&cols, &owner).unwrap();
    let mb = batch.as_mem_batch();
    let mut keybuf = PkBuf::zeroed(0);
    assert!(spec.key_bytes(&mb, 0, &mut keybuf), "both columns present ⇒ indexed");
    assert!(
        !spec.key_bytes(&mb, 1, &mut keybuf),
        "NULL in the first indexed column ⇒ skipped"
    );
    assert!(
        !spec.key_bytes(&mb, 2, &mut keybuf),
        "NULL in the second indexed column ⇒ skipped"
    );
}

/// `key_bytes` reuses the caller's `PkBuf` scratch across specs of DIFFERENT
/// widths. When a NARROWER span (single U64 → 8 bytes) reuses a buffer that a
/// WIDER span (composite U64,U64 → 16 bytes) just filled, the high bytes the
/// wide write left in `bytes[8..16]` MUST be re-zeroed: `PkBuf`'s "tail past
/// `len` is zero" invariant is what makes `padded(width)` sound and what lets
/// the single-PK fast path widen `bytes[..len]` to a `u128`. A stale tail would
/// silently corrupt any wider-stride read of this narrowed key.
#[test]
fn key_bytes_reused_buffer_zeros_tail_when_narrowing() {
    // Owner: PK U64; two non-null U64 payload columns.
    let owner = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::U64, false),
        ],
        &[0],
    );
    // One row whose col2 carries distinct all-nonzero high bytes, so the wide
    // span writes nonzero bytes into the trailing 8 bytes the narrow span must
    // then reclaim. col1 is arbitrary (it never lands in the narrow span).
    const COL1: u64 = 0xAABB_CCDD_EEFF_0011;
    const COL2: u64 = 0x1122_3344_5566_7788;
    let mut bb = BatchBuilder::new(owner);
    bb.begin_row(1, 1);
    bb.put_int(COL1 as u128);
    bb.put_int(COL2 as u128);
    bb.end_row();
    let batch = bb.finish();
    let mb = batch.as_mem_batch();

    // WIDE composite span over (col1, col2): two promoted U64 columns ⇒ 16 bytes.
    let wide_cols = [1u32, 2];
    let wide = IndexKeySpec::new(&wide_cols, &owner).unwrap();
    assert_eq!(wide.key_size(), 16, "two U64 index columns ⇒ 16-byte span");

    // NARROW span over (col2) alone: one promoted U64 column ⇒ 8 bytes.
    let narrow_cols = [2u32];
    let narrow = IndexKeySpec::new(&narrow_cols, &owner).unwrap();
    assert_eq!(narrow.key_size(), 8, "single U64 index column ⇒ 8-byte span");

    // ONE reused scratch buffer: wide first, then narrow.
    let mut keybuf = PkBuf::zeroed(0);

    assert!(wide.key_bytes(&mb, 0, &mut keybuf), "wide row is indexed");
    assert_eq!(keybuf.pk_bytes().len(), 16);
    // The wide write dirtied the trailing 8 bytes with col2's big-endian image —
    // the precondition that gives the narrowing tail-zero step teeth.
    assert_eq!(
        &keybuf.pk_bytes()[8..16],
        &COL2.to_be_bytes(),
        "wide span packs col2 into bytes[8..16]"
    );

    assert!(narrow.key_bytes(&mb, 0, &mut keybuf), "narrow row is indexed");
    // The narrow span is col2's 8-byte OPK, and ONLY 8 bytes are meaningful.
    assert_eq!(keybuf.pk_bytes().len(), 8, "narrow span is one U64 ⇒ len == 8");
    assert_eq!(
        keybuf.pk_bytes(),
        &COL2.to_be_bytes(),
        "narrow span content is col2's big-endian OPK"
    );
    // The invariant under test: the tail the wider write left MUST be re-zeroed.
    // The wide write above dirtied exactly `[0, 16)`, so that span is all a
    // narrowing could leak.
    // `padded(16)` is exactly 16 bytes, so this is also the statement that a
    // narrow key widened to a 16-byte stride has a zero suffix.
    assert!(
        keybuf.padded(16)[8..].iter().all(|&b| b == 0),
        "narrowing must re-zero every byte past len — stale wide-write tail leaked"
    );
}

/// `probe_key` keeps the whole span's entropy: two spans sharing a 16-byte
/// prefix must not collapse together.
#[test]
fn probe_key_distinguishes_spans_sharing_a_prefix() {
    let narrow = [7u64.to_be_bytes(), 8u64.to_be_bytes()].concat();
    assert_eq!(narrow.len(), NARROW_PK_MAX_BYTES);
    assert_ne!(
        probe_key(&narrow),
        probe_key(&[8u64.to_be_bytes(), 7u64.to_be_bytes()].concat())
    );

    let wide = |tail: u64| [&7u128.to_be_bytes()[..], &tail.to_be_bytes()[..]].concat();
    assert_eq!(wide(3)[..16], wide(4)[..16], "the two spans share a 16-byte prefix");
    assert_ne!(probe_key(&wide(3)), probe_key(&wide(4)));
}

// ---------------------------------------------------------------------------
// A key range's cut pair as a base-PK key range
// ---------------------------------------------------------------------------

/// A range over `schema`'s whole PK list.
fn pk_range(schema: &SchemaDescriptor, eq: &[u128], start: Cut, end: Cut) -> KeyRange {
    KeyRange::new(PkColList::from_slice(schema.pk_cols()), eq, start, end)
}

fn opk_u64(v: u64) -> Vec<u8> {
    v.to_be_bytes().to_vec() // U64 OPK is plain big-endian
}

/// `pk >= 5` → `[OPK(5), +∞)`. The single-column mainline: no equality pins,
/// `prefix_len == pk_stride`, an unbounded upper edge.
#[test]
fn pk_range_ge_unbounded_above() {
    let s = pk_only_schema(&[TypeCode::U64]);
    let r = pk_range(&s, &[], Cut::before(5), Cut::after(u64::MAX as u128));
    let (start, end) = s.pk_range_keys(&r).unwrap();
    assert_eq!(start.pk_bytes(), opk_u64(5));
    assert!(end.is_none(), "unbounded above → end None");
}

/// `pk > 5` → `[OPK(6), +∞)` — the degenerate no-pad `succ` on the whole key.
#[test]
fn pk_range_gt_increments_whole_key() {
    let s = pk_only_schema(&[TypeCode::U64]);
    let r = pk_range(&s, &[], Cut::after(5), Cut::after(u64::MAX as u128));
    let (start, end) = s.pk_range_keys(&r).unwrap();
    assert_eq!(start.pk_bytes(), opk_u64(6));
    assert!(end.is_none());
}

/// `pk < 10` → `[OPK(0), OPK(10))`.
#[test]
fn pk_range_lt() {
    let s = pk_only_schema(&[TypeCode::U64]);
    let r = pk_range(&s, &[], Cut::before(0), Cut::before(10));
    let (start, end) = s.pk_range_keys(&r).unwrap();
    assert_eq!(start.pk_bytes(), opk_u64(0));
    assert_eq!(end.unwrap().pk_bytes(), opk_u64(10));
}

/// Full-PK point lookup `pk = 5` → `[OPK(5), OPK(6))` (degenerate cuts).
#[test]
fn pk_range_point_lookup() {
    let s = pk_only_schema(&[TypeCode::U64]);
    let r = pk_range(&s, &[], Cut::before(5), Cut::after(5));
    let (start, end) = s.pk_range_keys(&r).unwrap();
    assert_eq!(start.pk_bytes(), opk_u64(5));
    assert_eq!(end.unwrap().pk_bytes(), opk_u64(6));
}

/// An inverted interval (`pk > 10 AND pk < 3`) drains to zero rows.
#[test]
fn pk_range_inverted_is_empty() {
    let s = pk_only_schema(&[TypeCode::U64]);
    let r = pk_range(&s, &[], Cut::after(10), Cut::before(3));
    assert_eq!(s.pk_range_keys(&r), None);
}

/// A signed PK: `pk > -1` seeks to `OPK(0)` (sign-flip order); `after(image(i64::MAX))`
/// overflows `succ` → unbounded above.
#[test]
fn pk_range_signed_i64() {
    let s = pk_only_schema(&[TypeCode::I64]);
    let img = |v: i64| gnitz_wire::key_image(TypeCode::I64, v as u64 as u128);
    let r = pk_range(&s, &[], Cut::after(img(-1)), Cut::after(img(i64::MAX)));
    let (start, end) = s.pk_range_keys(&r).unwrap();
    assert_eq!(start, s.opk_key(&0i64.to_le_bytes()));
    assert!(end.is_none());
}

/// Compound PK `(a, b)` with `a = 5 AND b > 3`: the range column is `b`, so
/// `start` seeks past `(5, 3)` and stays within the `a == 5` group.
#[test]
fn pk_range_compound_prefix_eq() {
    let s = pk_only_schema(&[TypeCode::U64, TypeCode::U64]);
    let r = pk_range(&s, &[5], Cut::after(3), Cut::after(u64::MAX as u128));
    let (start, end) = s.pk_range_keys(&r).unwrap();
    // start = OPK(5,4) — the prefix `(5,3)` incremented on b.
    assert_eq!(start.pk_bytes(), opk_pk(&s, &[5, 4]));
    // end = the successor of `(5, MAX)` — carries into `a`, i.e. OPK(6, 0).
    assert_eq!(end.unwrap().pk_bytes(), opk_pk(&s, &[6, 0]));
}

/// A range over a strict PK prefix bounds the leading column and leaves the rest
/// of the key free: `a > 3` over `(a, b)` starts at `(4, 0)`.
#[test]
fn pk_prefix_range_leaves_the_trailing_column_free() {
    let s = pk_only_schema(&[TypeCode::U64, TypeCode::U64]);
    let r = KeyRange::new(PkColList::from_slice(&[0]), &[], Cut::after(3), Cut::before(9));
    let (start, end) = s.pk_range_keys(&r).unwrap();
    assert_eq!(start.pk_bytes(), opk_pk(&s, &[4, 0]));
    assert_eq!(end.unwrap().pk_bytes(), opk_pk(&s, &[9, 0]));
}

// ---------------------------------------------------------------------------
// `SchemaDescriptor::confined_worker` — the master's confinement test
// ---------------------------------------------------------------------------

/// Worker count the confinement tests route against; any count works.
const NW: usize = 4;

/// A full point is confined to the worker of its own PK bytes, at every PK
/// shape — single, wide, and compound (where the point pins the leading
/// columns through `eq_vals` and points at the last).
#[test]
fn confined_worker_confines_a_full_point() {
    let u64s = pk_only_schema(&[TypeCode::U64]);
    assert_eq!(
        u64s.confined_worker(&pk_range(&u64s, &[], Cut::before(42), Cut::after(42)), NW),
        Some(u64s.worker_for_pk(&opk_pk(&u64s, &[42]), NW))
    );

    let u128s = pk_only_schema(&[TypeCode::U128]);
    let wide = (1u128 << 100) | 7;
    assert_eq!(
        u128s.confined_worker(&pk_range(&u128s, &[], Cut::before(wide), Cut::after(wide)), NW),
        Some(u128s.worker_for_pk(&opk_pk(&u128s, &[wide]), NW))
    );

    let comp = pk_only_schema(&[TypeCode::U32, TypeCode::U64]);
    assert_eq!(
        comp.confined_worker(&pk_range(&comp, &[9], Cut::before(4), Cut::after(4)), NW),
        Some(comp.worker_for_pk(&opk_pk(&comp, &[9, 4]), NW))
    );
}

/// With a `Keyed { prefix_len: 1 }` placement every row sharing the leading
/// column lands on one worker, so pinning it and ranging the trailing column
/// is confined — to the same worker full points on `(a, b)` reach. At the
/// full-PK default the same bound spans workers.
#[test]
fn confined_worker_follows_the_distribution_prefix() {
    let cols = [
        SchemaColumn::new(TypeCode::U64, false),
        SchemaColumn::new(TypeCode::U64, false),
    ];
    let prefix = SchemaDescriptor::new_with_placement(&cols, &[0, 1], Placement::Keyed { prefix_len: 1 });
    // `a = 7 AND b > 3` — a whole trailing-column range inside one `a` group.
    let ranged = pk_range(&prefix, &[7], Cut::after(3), Cut::after(u64::MAX as u128));
    let want = prefix.worker_for_pk(&opk_pk(&prefix, &[7, 0]), NW);
    assert_eq!(prefix.confined_worker(&ranged, NW), Some(want));
    for b in [4u128, u64::MAX as u128] {
        assert_eq!(
            prefix.confined_worker(&pk_range(&prefix, &[7], Cut::before(b), Cut::after(b)), NW),
            Some(want),
            "a full point on (7, {b}) shares the group's worker"
        );
    }
    // `a = 7` alone, as a range over the PK prefix `[a]` the distribution covers.
    let group = KeyRange::point(PkColList::from_slice(&[0]), &[], 7);
    assert_eq!(prefix.confined_worker(&group, NW), Some(want));

    let full = SchemaDescriptor::new_with_placement(&cols, &[0, 1], Placement::Keyed { prefix_len: 2 });
    assert_eq!(
        full.confined_worker(&ranged, NW),
        None,
        "hashing the whole PK spreads one `a` group across workers"
    );
}

/// A maximal-value point carries out of `succ`, so its range has no `end` —
/// the all-`0xFF` last key must still confine it rather than broadcast. The
/// signed maximum's OPK is all-`0xFF` too (sign-flip).
#[test]
fn confined_worker_confines_a_maximal_point() {
    for tc in [TypeCode::U64, TypeCode::I64] {
        let s = pk_only_schema(&[tc]);
        let max = if tc == TypeCode::U64 {
            u64::MAX as u128
        } else {
            i64::MAX as u128
        };
        let r = pk_range(
            &s,
            &[],
            Cut::before(gnitz_wire::key_image(tc, max)),
            Cut::after(gnitz_wire::key_image(tc, max)),
        );
        assert!(s.pk_range_keys(&r).unwrap().1.is_none(), "after(max) carries out");
        assert_eq!(
            s.confined_worker(&r, NW),
            Some(s.worker_for_pk(&opk_pk(&s, &[max]), NW))
        );
    }
}

/// A range wider than one worker's key span is not confinable: owners are a
/// hash of the key, not monotone in key order, so only a whole-range prefix
/// match proves confinement. Neither is a relation whose rows no key places, nor a
/// range over anything but a PK prefix.
#[test]
fn confined_worker_declines_a_multi_key_range_and_an_unkeyed_relation() {
    let s = pk_only_schema(&[TypeCode::U64]);
    assert_eq!(
        s.confined_worker(&pk_range(&s, &[], Cut::before(0), Cut::after(1000)), NW),
        None
    );
    // Unbounded above from a non-maximal start: the last key is 0xFF…FF.
    assert_eq!(
        s.confined_worker(&pk_range(&s, &[], Cut::before(5), Cut::after(u64::MAX as u128)), NW),
        None
    );

    // The same point that confines above names no owner once the rows are not
    // key-placed.
    let point = pk_range(&s, &[], Cut::before(42), Cut::after(42));
    assert!(s.confined_worker(&point, NW).is_some());
    let cols = [SchemaColumn::new(TypeCode::U64, false)];
    for p in [Placement::Replicated, Placement::Local] {
        assert_eq!(
            SchemaDescriptor::new_with_placement(&cols, &[0], p).confined_worker(&point, NW),
            None
        );
    }

    // A point over a column that does not lead the PK walks an index or a full scan.
    let two = pk_only_schema(&[TypeCode::U64, TypeCode::U64]);
    let off_pk = KeyRange::point(PkColList::from_slice(&[1]), &[], 42);
    assert_eq!(two.confined_worker(&off_pk, NW), None);
}

/// A provably empty PK range is answered by worker 0 under every placement: an empty
/// answer is correct from any worker.
#[test]
fn confined_worker_answers_an_empty_range_from_worker_zero() {
    let cols = [SchemaColumn::new(TypeCode::U64, false)];
    for p in [
        Placement::Keyed { prefix_len: 1 },
        Placement::Replicated,
        Placement::Local,
    ] {
        let s = SchemaDescriptor::new_with_placement(&cols, &[0], p);
        let inverted = pk_range(&s, &[], Cut::after(1000), Cut::before(0));
        assert_eq!(s.confined_worker(&inverted, NW), Some(0), "{p:?}");
        let past_top = pk_range(&s, &[], Cut::after(u64::MAX as u128), Cut::after(u64::MAX as u128));
        assert_eq!(s.confined_worker(&past_top, NW), Some(0), "{p:?}");
    }
}

/// The indirect sort of a flat record buffer. Sweeps OPK strides at a
/// chunk-sized `n` and a large one.
#[test]
#[ignore = "microbenchmark; run explicitly with --release --ignored --nocapture"]
fn sort_indices_bench() {
    use crate::test_rng::Rng;
    use crate::test_support::bench_time_each;

    const ITERS: usize = 3;
    for &n in &[65_536usize, 4 << 20] {
        for &stride in &[4usize, 8, 12, 16, 24, 40] {
            for seq in [true, false] {
                let mut rng = Rng::new(0x5EED_0000 + n as u64 + stride as u64);
                let mut flat = vec![0u8; n * stride];
                for (i, rec) in flat.chunks_mut(stride).enumerate() {
                    for chunk in rec.chunks_mut(8) {
                        let bytes = rng.next_u64().to_be_bytes();
                        chunk.copy_from_slice(&bytes[..chunk.len()]);
                    }
                    // Distinct ids in the leading bytes: neighbours share a prefix.
                    if seq {
                        let id = (i as u64).wrapping_mul(0x9E37_79B9) % n as u64;
                        let be = id.to_be_bytes();
                        let w = stride.min(8);
                        rec[..w].copy_from_slice(&be[8 - w..]);
                    }
                }
                let elapsed = bench_time_each(ITERS, Vec::new, |mut idx| {
                    sort_indices(&flat, stride, &mut idx);
                    std::hint::black_box(&idx);
                });
                let ns = elapsed.as_secs_f64() * 1e9 / (ITERS as f64 * n as f64);
                let shape = if seq { "id" } else { "rnd" };
                println!("  n={n:<8} stride={stride:<3} {shape:<3} {ns:7.2} ns/record");
            }
        }
    }
}
