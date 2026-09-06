use super::*;
use crate::schema::{type_code, IndexKeySpec, Placement, SchemaColumn, SchemaDescriptor};
use crate::storage::BatchBuilder;
use crate::test_support::pk_only_schema;
use gnitz_wire::{read_signed_exact, read_unsigned_exact};

/// Independent typed reference comparator over native-LE PK bytes. This is
/// the per-column column-walk that `compare_pk_bytes` used *before* the OPK
/// flip; kept here as the test oracle for "OPK byte order == typed order".
fn typed_cmp_pk_le(schema: &SchemaDescriptor, a: &[u8], b: &[u8]) -> Ordering {
    let mut off = 0usize;
    for (_ord, col) in schema.pk_columns() {
        let cs = col.size() as usize;
        let ord = match col.type_code {
            type_code::U128 | type_code::UUID => {
                let va = u128::from_le_bytes(a[off..off + 16].try_into().unwrap());
                let vb = u128::from_le_bytes(b[off..off + 16].try_into().unwrap());
                va.cmp(&vb)
            }
            type_code::I128 => {
                let va = i128::from_le_bytes(a[off..off + 16].try_into().unwrap());
                let vb = i128::from_le_bytes(b[off..off + 16].try_into().unwrap());
                va.cmp(&vb)
            }
            type_code::U64 | type_code::U32 | type_code::U16 | type_code::U8 => {
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
    compare_pk_bytes(opk_key(schema, a).pk_bytes(), opk_key(schema, b).pk_bytes())
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
fn assert_single_col_order<T: Ord + Copy + std::fmt::Debug>(tc: u8, vals: &[T], le: impl Fn(T) -> Vec<u8>) {
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
    assert_single_col_order(type_code::U8, &[0u8, 1, 0x7F, 0x80, 0xFF], |v| vec![v]);
    assert_single_col_order(type_code::I8, &[i8::MIN, -1, 0, 1, i8::MAX], |v| vec![v as u8]);
    let le16 = |v: u16| v.to_le_bytes().to_vec();
    assert_single_col_order(type_code::U16, &[0u16, 1, 0x0100, 0x8000, 0xFFFE, u16::MAX], le16);
    assert_single_col_order(type_code::I16, &[i16::MIN, -1, 0, 1, i16::MAX], |v: i16| {
        v.to_le_bytes().to_vec()
    });
    let le32 = |v: u32| v.to_le_bytes().to_vec();
    assert_single_col_order(
        type_code::U32,
        &[0u32, 1, 256, 0x8000_0000, 0xFFFF_FFFE, u32::MAX],
        le32,
    );
    assert_single_col_order(type_code::I32, &[i32::MIN, -1, 0, 1, i32::MAX], |v: i32| {
        v.to_le_bytes().to_vec()
    });
    assert_single_col_order(type_code::U64, &[0u64, 1, 2, 256, 1 << 63, u64::MAX], |v: u64| {
        v.to_le_bytes().to_vec()
    });
    assert_single_col_order(type_code::I64, &[i64::MIN, -1, 0, 1, i64::MAX], |v: i64| {
        v.to_le_bytes().to_vec()
    });
    let le128 = |v: u128| v.to_le_bytes().to_vec();
    let wide = [0u128, 1, u64::MAX as u128, u64::MAX as u128 + 1, 1 << 127, u128::MAX];
    assert_single_col_order(type_code::U128, &wide, le128);
    assert_single_col_order(type_code::UUID, &wide, le128);
    assert_single_col_order(
        type_code::I128,
        &[i128::MIN, -1, 0, 1, 1i128 << 63, 1i128 << 64, i128::MAX],
        |v: i128| v.to_le_bytes().to_vec(),
    );
}

#[test]
fn compare_pk_bytes_compound_u64_u64() {
    let s = pk_only_schema(&[type_code::U64, type_code::U64]);
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
    let s = pk_only_schema(&[type_code::U64, type_code::I32]);
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
    // PK column. The byte layout follows pk_indices() order, so the
    // first 8 bytes correspond to column 1.
    let s = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U64, 0),
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
                    gnitz_wire::widen_pk_be(&bytes, width) << (8 * (16 - width)),
                    "width {width} seed {seed}: widen_pk_be identity",
                );
            }
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
    fn arb_pk_case() -> impl Strategy<Value = (Vec<u8>, Vec<u32>, Vec<u8>, Vec<u8>)> {
        prop::collection::vec(arb_pk_type(), 1..=4).prop_flat_map(|types| {
            let stride: usize = types.iter().map(|&t| gnitz_wire::wire_stride(t)).sum();
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
                types.iter().map(|&tc| SchemaColumn::new(tc, 0)).collect();
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
                types.iter().map(|&tc| SchemaColumn::new(tc, 0)).collect();
            let s = SchemaDescriptor::new(&cols, &perm);
            let (oa, ob) = (opk_key(&s, &a), opk_key(&s, &b));
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
                types.iter().map(|&tc| SchemaColumn::new(tc, 0)).collect();
            let s = SchemaDescriptor::new(&cols, &perm);
            let (oa, ob) = (opk_key(&s, &a), opk_key(&s, &b));
            let (oa, ob) = (oa.pk_bytes(), ob.pk_bytes());
            prop_assert_eq!(pk_bytes_eq(oa, ob), oa == ob);
            prop_assert!(pk_bytes_eq(oa, oa));
        }
    }
}

// ── seek_opk_bytes: the width-universal seek-key encoder ─────────────────

#[test]
fn seek_opk_bytes_narrow_matches_opk_key() {
    // For every narrow stride (≤ 16) the wire pair degenerates to `(low, &[])`,
    // so `seek_opk_bytes` must be byte-identical to a direct `opk_key` of the
    // native value — both buffer and stride.
    let cases = [
        pk_only_schema(&[type_code::U8]),  // stride 1
        pk_only_schema(&[type_code::U32]), // stride 4
        pk_only_schema(&[type_code::U64]), // stride 8
        pk_only_schema(&[type_code::I64]), // stride 8, signed → OPK flips the sign bit
        // Compound (U32, U32) with a *permuted* PK list [1, 0]: stride 8,
        // exercises the multi-column pk-list walk in the encoder.
        SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U32, 0),
                SchemaColumn::new(type_code::U32, 0),
            ],
            &[1, 0],
        ),
    ];
    // Values spanning zero, small, mixed, and a sign-bit-set word (negative
    // for the I64 case) so the sign-flip and byte order are exercised. Both
    // encoders truncate to `stride`, so an over-wide value is a valid probe.
    for s in cases {
        for v in [0u128, 1, 0x0123_4567_89AB_CDEF, 0x8000_0000_0000_0000, u64::MAX as u128] {
            let want = opk_key(&s, &v.to_le_bytes());
            let got = seek_opk_bytes(&s, v, &[]).expect("narrow seek encodes");
            assert_eq!(got, want, "narrow seek must match opk_key for {s:?} v={v:#x}");
        }
    }
}

#[test]
fn seek_opk_bytes_wide_reproduces_hand_built_opk() {
    // (U64, U64, U64) = stride 24, wide. The wire pair carries the first 16
    // native bytes in `low` and the trailing U64 in `extra`, exactly as
    // `split_ctrl_key` cuts them. All-unsigned ⇒ OPK is each column's
    // big-endian image, so the expected key is built by hand.
    let s = pk_only_schema(&[type_code::U64; 3]);
    assert_eq!(s.pk_stride(), 24);
    let (a, b, c): (u64, u64, u64) = (0x1122_3344_5566_7788, 0x99AA_BBCC_DDEE_FF00, 0x0102_0304_0506_0708);
    // Native LE image = [a_LE, b_LE, c_LE]; the pair's `low` is the first 16
    // bytes (a in the low half, b in the high half), `extra` is c's 8 bytes.
    let low = (a as u128) | ((b as u128) << 64);
    let opk = seek_opk_bytes(&s, low, &c.to_le_bytes()).expect("wide seek encodes");
    assert_eq!(opk.len, 24);
    let want: Vec<u8> = a
        .to_be_bytes()
        .into_iter()
        .chain(b.to_be_bytes())
        .chain(c.to_be_bytes())
        .collect();
    assert_eq!(opk.pk_bytes(), want.as_slice());
}

#[test]
fn seek_opk_bytes_missing_extra_errs_not_panics() {
    // A wide stride needs `stride - 16` extra bytes; too few must return Err,
    // never panic — the runtime guard the two dispatch sites rely on.
    let s = pk_only_schema(&[type_code::U64; 3]);
    assert!(seek_opk_bytes(&s, 0, &[]).is_err(), "stride 24 with no extra must Err");
    assert!(seek_opk_bytes(&s, 0, &[0u8; 7]).is_err(), "7 < 8 extra bytes must Err");
    assert!(
        seek_opk_bytes(&s, 0, &[0u8; 8]).is_ok(),
        "exactly 8 extra bytes is enough"
    );
}

#[test]
fn seek_opk_bytes_four_u128_ceiling() {
    // The widest SQL-reachable PK is 4 columns (PK_LIST_MAX_COLS); 4×U128 =
    // stride 64 exercises the `le[16..16 + needed]` copy at its ceiling
    // (`needed == 48`). All-unsigned ⇒ OPK is each column's BE image.
    // Column 0 rides in `low`; columns 1..4 (48 bytes) in `extra`.
    let s = pk_only_schema(&[type_code::U128; 4]);
    assert_eq!(s.pk_stride(), 64);
    let vals: [u128; 4] = [
        0x0102_0304_0506_0708_090A_0B0C_0D0E_0F10,
        0x1112_1314_1516_1718_191A_1B1C_1D1E_1F20,
        0x2122_2324_2526_2728_292A_2B2C_2D2E_2F30,
        0x3132_3334_3536_3738_393A_3B3C_3D3E_3F40,
    ];
    let extra: Vec<u8> = vals[1..].iter().flat_map(|v| v.to_le_bytes()).collect();
    let opk = seek_opk_bytes(&s, vals[0], &extra).expect("4×U128 encodes");
    assert_eq!(opk.len, 64);
    let want: Vec<u8> = vals.iter().flat_map(|v| v.to_be_bytes()).collect();
    assert_eq!(opk.pk_bytes(), want.as_slice());
}

#[test]
fn pkbuf_eq_hash_compare_only_len_window() {
    use std::collections::HashSet;
    // Same meaningful bytes, different tail → equal and same hash.
    let mut a = PkBuf::from_bytes(&7u64.to_le_bytes());
    let b = PkBuf::from_bytes(&7u64.to_le_bytes());
    a.bytes[8] = 0xAB; // tail garbage past len; eq/hash must ignore it
    assert!(a == b);
    let mut set: HashSet<PkBuf> = HashSet::new();
    set.insert(b);
    assert!(set.contains(&a), "tail bytes must not affect membership");
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
/// SQL NULL-distinctness, mirroring `batch_project_index`.
#[test]
fn index_key_spec_skips_any_null_column() {
    let owner = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U64, 1), // a: nullable payload
            SchemaColumn::new(type_code::U64, 1), // b: nullable payload
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
            match cell {
                Some(v) => bb.put_int(v),
                None => bb.put_null(),
            }
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
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U64, 0),
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

// =======================================================================
// ReindexPacker — the synthetic-key composer
// =======================================================================

/// Write `packer`'s key for every row of `src` into `out`'s PK region — what
/// `expr::MapPlan`'s `PkSource::Pack` arm does in production, spelled here so
/// the tests below drive the packer through a stored PK region rather than a
/// scratch buffer.
fn promote_into<R: RowSource>(packer: &ReindexPacker, src: &R, out: &mut Batch) {
    assert_eq!(out.pk_stride() as usize, packer.out_stride);
    let (n, stride) = (out.count, packer.out_stride);
    let pk = out.pk_data_mut();
    for row in 0..n {
        packer.pack_into(&mut pk[row * stride..(row + 1) * stride], src, row);
    }
}

use crate::storage::Batch;
use crate::test_support::{make_schema_pk_u64_payload_blob, make_schema_pk_u64_payload_string, opk_pk};

/// Worker count the co-partition pins route against. Any count works — the
/// property is that producer and consumer agree — but the wide-arm formula
/// pin needs a fixed one to recompute against.
const NW: usize = 4;

// -----------------------------------------------------------------------
// german_string_promote_key — the content-hash arm's own contract
// -----------------------------------------------------------------------

#[test]
fn test_german_string_promote_key_short_and_long() {
    // Two rows: one short ("foo", inline) and one long string (> 12 bytes,
    // stored in blob). Both German-string layouts execute, and distinct
    // strings hash to distinct PKs.
    let schema = make_schema_pk_u64_payload_string();
    let mut b = Batch::with_capacity(schema, 2);

    // Row 0: short string "foo" (3 bytes, inline).
    b.extend_pk(1u128);
    b.extend_weight(&1i64.to_le_bytes());
    b.extend_null_bmp(&0u64.to_le_bytes());
    let gs0 = gnitz_wire::encode_german_string(b"foo", &mut b.blob);
    b.extend_col(0, &gs0);
    b.count += 1;

    // Row 1: long string (15 bytes > SHORT_STRING_THRESHOLD=12), heap-allocated.
    let long_str: &[u8] = b"hello-world-xyz";
    b.extend_pk(2u128);
    b.extend_weight(&1i64.to_le_bytes());
    b.extend_null_bmp(&0u64.to_le_bytes());
    let gs1 = gnitz_wire::encode_german_string(long_str, &mut b.blob);
    b.extend_col(0, &gs1);
    b.count += 1;

    let mb = b.as_mem_batch();
    let pk_short = german_string_promote_key(mb.get_col_ptr(0, 0, 16), mb.blob);
    let pk_long = german_string_promote_key(mb.get_col_ptr(1, 0, 16), mb.blob);

    // The digest is deterministic but not pinned. What matters: non-empty
    // content hashes non-zero, distinct content hashes distinctly, and the
    // high half is populated (a real 128-bit hash, not a widened 64-bit one).
    assert_ne!(pk_short, 0);
    assert_ne!(pk_long, 0);
    assert_ne!(pk_short, pk_long);
    assert_ne!(
        pk_short >> 64,
        0,
        "short string PK must populate high half via xxh3_128"
    );
    assert_ne!(pk_long >> 64, 0, "long string PK must populate high half via xxh3_128");
}

#[test]
fn test_german_string_promote_key_empty_is_zero() {
    // The hash early-returns 0 for length==0 — assert this is the contract,
    // not an accidental side-effect of xxh on empty input.
    let schema = make_schema_pk_u64_payload_string();
    let mut b = Batch::with_capacity(schema, 1);
    b.extend_pk(1u128);
    b.extend_weight(&1i64.to_le_bytes());
    b.extend_null_bmp(&0u64.to_le_bytes());
    // 16-byte German string struct, length=0.
    let gs = [0u8; 16];
    b.extend_col(0, &gs);
    b.count += 1;

    let mb = b.as_mem_batch();
    assert_eq!(german_string_promote_key(mb.get_col_ptr(0, 0, 16), mb.blob), 0);
}

// -----------------------------------------------------------------------
// FoldCols::key_row — the one-shot and the streaming form are one digest
// -----------------------------------------------------------------------

/// The one-shot fold assembles into a stack scratch exactly what the streaming
/// fold pushes through `RowHasher`. Nothing else would catch them diverging: the
/// digest feeds group keys, output PKs and AVI buckets, all self-consistent
/// under either spelling.
#[test]
fn hash_fold_one_shot_matches_the_streaming_form() {
    /// The same columns folded the other way — the arm `FoldCols::new` did not pick.
    fn flipped(f: &FoldCols) -> FoldCols {
        FoldCols {
            locs: f.locs().to_vec(),
            inline: !f.inline,
        }
    }

    let schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),  // 0: PK
            SchemaColumn::new(type_code::I32, 0),  // 1: NOT NULL payload  (slot 0)
            SchemaColumn::new(type_code::U64, 1),  // 2: nullable payload  (slot 1)
            SchemaColumn::new(type_code::U128, 0), // 3: wide payload      (slot 2)
            SchemaColumn::new(type_code::U16, 1),  // 4: nullable payload  (slot 3)
        ],
        &[0],
    );
    let mut b = Batch::with_capacity(schema, 2);
    // Row 0: nothing NULL. Row 1: the two nullable columns NULL, so both the
    // marker-only arm and the marker+route-key arm run in one row.
    for (pk, null_word) in [(7u128, 0u64), (8u128, (1u64 << 1) | (1u64 << 3))] {
        b.extend_pk(pk);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&null_word.to_le_bytes());
        b.extend_col(0, &(-9i32).to_le_bytes());
        b.extend_col(1, &42u64.to_le_bytes());
        b.extend_col(2, &(u128::MAX - 3).to_le_bytes());
        b.extend_col(3, &5u16.to_le_bytes());
        b.count += 1;
    }
    let mb = b.as_mem_batch();

    let all: Vec<ColumnLocator> = (0..5).map(|c| schema.locate(c)).collect();
    // Every arity from the global aggregate's empty fold up to the full set.
    for k in 0..=all.len() {
        let f = FoldCols::new(all[..k].to_vec());
        assert!(f.inline, "arity {k} is fixed-width and fits the scratch");
        let g = flipped(&f);
        for row in 0..2 {
            let null_word = mb.get_null_word(row);
            assert_eq!(
                f.key_row(&mb, row, null_word),
                g.key_row(&mb, row, null_word),
                "arity {k}, row {row}"
            );
        }
    }

    // The two shapes that must keep streaming: variable-length content, and a
    // group set wider than the stack scratch.
    let gs = make_schema_pk_u64_payload_string();
    assert!(
        !FoldCols::new(vec![gs.locate(1)]).inline,
        "a German-string column streams"
    );
    assert!(
        !FoldCols::new(vec![all[1]; FOLD_INLINE_COLS + 1]).inline,
        "a group set past the scratch streams"
    );
}

// -----------------------------------------------------------------------
// ReindexPacker::output_schema — the layout the per-row packer writes through
// -----------------------------------------------------------------------

#[test]
fn packer_output_schema_pk_width_policy() {
    // (key column type, expected output PK type, expected pk_stride)
    let cases = [
        (type_code::U64, type_code::U64, 8u8),
        (type_code::I32, type_code::I32, 4),
        (type_code::U16, type_code::U16, 2),
        (type_code::STRING, type_code::U128, 16),
        (type_code::BLOB, type_code::U128, 16),
        (type_code::U128, type_code::U128, 16),
        (type_code::UUID, type_code::U128, 16),
        (type_code::F64, type_code::U128, 16),
    ];
    for (key_tc, want_tc, want_stride) in cases {
        // in_schema: [U64 PK, <key col>]; reindex on the payload col so the
        // PK-ineligible key types (STRING/BLOB/float) are exercisable as keys.
        let in_schema = SchemaDescriptor::new(
            &[SchemaColumn::new(type_code::U64, 0), SchemaColumn::new(key_tc, 0)],
            &[0],
        );
        let node_schema = ReindexPacker::new(&in_schema, &[(1, 0)])
            .unwrap()
            .output_schema(&in_schema, &[0, 1])
            .unwrap();
        assert_eq!(node_schema.columns[0].type_code, want_tc, "key {key_tc} → PK type");
        assert_eq!(node_schema.pk_stride(), want_stride, "key {key_tc} → pk_stride");
    }
}

#[test]
fn packer_output_schema_compound() {
    // in_schema: [U64 pk, I32, U128]; reindex on (col1 I32, col2 U128).
    let in_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I32, 0),
            SchemaColumn::new(type_code::U128, 0),
        ],
        &[0],
    );
    let out = ReindexPacker::new(&in_schema, &[(1, 0), (2, 0)])
        .unwrap()
        .output_schema(&in_schema, &[0, 1, 2])
        .unwrap();
    assert_eq!(out.pk_indices(), &[0, 1], "2-slot compound PK");
    assert_eq!(out.columns[0].type_code, type_code::I32, "slot0 keeps I32 native width");
    assert_eq!(out.columns[1].type_code, type_code::U128, "slot1 U128");
    assert_eq!(out.pk_stride(), 4 + 16, "compound stride = Σ slot widths");
    // Input columns follow the synthetic PK slots.
    assert_eq!(out.num_columns(), 2 + 3);
    assert_eq!(out.columns[2].type_code, type_code::U64);
    assert_eq!(out.columns[3].type_code, type_code::I32);
    assert_eq!(out.columns[4].type_code, type_code::U128);
}

#[test]
fn packer_output_schema_cross_width_promotes() {
    // in_schema: [U64 pk, I32, I64]; reindex on (col1 I32, col2 I64) with
    // slot 0 promoted to I64 (carried) and slot 1 self-deriving.
    let in_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I32, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    );
    let out = ReindexPacker::new(&in_schema, &[(1, type_code::I64), (2, 0)])
        .unwrap()
        .output_schema(&in_schema, &[0, 1, 2])
        .unwrap();
    assert_eq!(out.columns[0].type_code, type_code::I64, "slot0 carried T = I64");
    assert_eq!(out.columns[1].type_code, type_code::I64, "slot1 self-derives I64");
    assert_eq!(out.pk_stride(), 8 + 8, "both slots 8 bytes after promotion");
}

#[test]
fn packer_output_schema_payload_prune() {
    // in_schema: [U64 pk, I32, U128, I16]; reindex on col1; keep payload {0, 3}.
    let in_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I32, 0),
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::I16, 0),
        ],
        &[0],
    );
    let out = ReindexPacker::new(&in_schema, &[(1, 0)])
        .unwrap()
        .output_schema(&in_schema, &[0, 3])
        .unwrap();
    assert_eq!(out.pk_indices(), &[0], "single synthetic PK slot");
    assert_eq!(out.columns[0].type_code, type_code::I32, "PK slot = reindex col1 (I32)");
    // Only the two kept payload columns follow — not all four input columns.
    assert_eq!(out.num_columns(), 1 + 2, "1 PK + 2 kept payload");
    assert_eq!(out.columns[1].type_code, type_code::U64, "kept payload col 0");
    assert_eq!(out.columns[2].type_code, type_code::I16, "kept payload col 3");
}

// -----------------------------------------------------------------------
// ReindexPacker — multi-column / compound reindex packing
// -----------------------------------------------------------------------

#[test]
fn test_reindex_packer_multi_column_bytes() {
    // Compound key spanning every slot shape: a non-leading PK column (offset
    // 8), a sign-flipped I32 payload, a 16-byte U128 payload, and an F64
    // whose 8 source bytes zero-pad into a 16-byte slot.
    let schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I32, 0),
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::F64, 0),
        ],
        &[0, 1],
    );
    let pk0: u64 = 0x0102_0304_0506_0708;
    let pk1: u64 = 0xA0B0_C0D0_E0F0_0102;
    let iv: i32 = -3;
    let uv: u128 = 0xdead_beef_cafe_1234_5678_9abc_def0_0001;
    let fv: f64 = 2.5;

    let mut b = Batch::with_capacity(schema, 1);
    b.extend_pk_opk(&schema, &[pk0 as u128, pk1 as u128]);
    b.extend_weight(&1i64.to_le_bytes());
    b.extend_null_bmp(&0u64.to_le_bytes());
    b.extend_col(0, &iv.to_le_bytes()); // I32 payload (pi 0)
    b.extend_col(1, &uv.to_le_bytes()); // U128 payload (pi 1)
    b.extend_col(2, &fv.to_le_bytes()); // F64 payload (pi 2)
    b.count += 1;
    let mb = b.as_mem_batch();

    let packer = ReindexPacker::new(&schema, &[(1, 0), (2, 0), (3, 0), (4, 0)]).unwrap();
    // out_stride = 8 (Pk U64) + 4 (I32) + 16 (U128) + 16 (F64→U128) = 44.
    assert_eq!(packer.out_stride, 8 + 4 + 16 + 16);

    let mut buf = [0u8; crate::schema::MAX_PK_BYTES];
    packer.pack_into(&mut buf[..packer.out_stride], &mb, 0);

    // Expected: each column's OPK bytes concatenated at its offset.
    let mut want = Vec::new();
    want.extend_from_slice(&pk1.to_be_bytes()); // col1 Pk: BE(pk1) verbatim
    let mut i32_opk = [0u8; 4];
    gnitz_wire::encode_pk_column(&iv.to_le_bytes(), type_code::I32, &mut i32_opk);
    want.extend_from_slice(&i32_opk); // col2: sign-aware OPK
    assert_eq!(i32_opk[0], 0x7F, "I32 -3 OPK leading byte is sign-flipped (0x7F)");
    want.extend_from_slice(&uv.to_be_bytes()); // col3 Wide: BE(u128)
    let mut f64_slot = [0u8; 16];
    f64_slot[8..].copy_from_slice(&fv.to_bits().to_be_bytes()); // high 8 zero-pad, low 8 = BE(bits)
    want.extend_from_slice(&f64_slot); // col4: float, zero-padded
    assert_eq!(&buf[..packer.out_stride], &want[..], "packed compound key bytes");
    // Float slot high pad is zeroed.
    assert_eq!(&buf[28..36], &[0u8; 8], "F64 slot high pad zeroed");
}

#[test]
fn test_reindex_packer_arity1_byte_identity() {
    // The content-hash arm: a STRING and a BLOB key both pack to the
    // big-endian image of `german_string_promote_key` over the column's
    // content. The integer and PK-placement arms are the proptest's; this is
    // the arm it cannot generate (`arb_pk_type` yields PK-eligible integers).
    for schema in [make_schema_pk_u64_payload_string(), make_schema_pk_u64_payload_blob()] {
        // Three rows with distinct content, one of them empty (the zero
        // sentinel), exercising the per-row read.
        let contents: [&[u8]; 3] = [b"abc", b"", b"hello-world-xyz"];
        let mut b = Batch::with_capacity(schema, 3);
        for (r, content) in contents.iter().enumerate() {
            b.extend_pk((r + 1) as u128 * 11);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            let gs = gnitz_wire::encode_german_string(content, &mut b.blob);
            b.extend_col(0, &gs);
            b.count += 1;
        }
        let mb = b.as_mem_batch();

        let out_schema = SchemaDescriptor::new(&[SchemaColumn::new(type_code::U128, 0)], &[0]);
        let packer = ReindexPacker::new(&schema, &[(1, 0)]).unwrap();
        assert_eq!(packer.out_stride, 16, "a content-hash key is a 16-byte U128 slot");
        let mut out = Batch::zeroed(out_schema, 3);
        promote_into(&packer, &mb, &mut out);

        for row in 0..3 {
            let want = german_string_promote_key(mb.get_col_ptr(row, 0, 16), mb.blob);
            assert_eq!(
                out.get_pk_bytes(row),
                &want.to_be_bytes()[..],
                "{} row {row}: packed key is BE(content hash)",
                schema.columns[1].type_code,
            );
        }
        // The empty-content row is the zero sentinel, and the two non-empty
        // rows do not collide with it or with each other.
        assert_eq!(out.get_pk_bytes(1), &[0u8; 16], "empty content hashes to zero");
        assert_ne!(out.get_pk_bytes(0), out.get_pk_bytes(2));
        assert_ne!(out.get_pk_bytes(0), out.get_pk_bytes(1));
    }
}

#[test]
fn test_reindex_packer_float_arity1_zero_pad() {
    // A float key self-derives to a 16-byte slot from an 8-byte source: the
    // slot is 8 zero bytes ++ BE(bits).
    let schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::F64, 0),
        ],
        &[0],
    );
    let mut b = Batch::with_capacity(schema, 1);
    b.extend_pk(1u128);
    b.extend_weight(&1i64.to_le_bytes());
    b.extend_null_bmp(&0u64.to_le_bytes());
    let fv: f64 = -7.25;
    b.extend_col(0, &fv.to_le_bytes());
    b.count += 1;
    let mb = b.as_mem_batch();

    let out_schema = SchemaDescriptor::new(&[SchemaColumn::new(type_code::U128, 0)], &[0]);
    let packer = ReindexPacker::new(&schema, &[(1, 0)]).unwrap();
    assert_eq!(packer.out_stride, 16);
    let mut packer_out = Batch::zeroed(out_schema, 1);
    promote_into(&packer, &mb, &mut packer_out);

    let mut want = [0u8; 16];
    want[8..].copy_from_slice(&fv.to_bits().to_be_bytes());
    assert_eq!(
        packer_out.get_pk_bytes(0),
        &want[..],
        "float slot = zero-pad ++ BE(bits)"
    );
}

#[test]
fn test_reindex_packer_copartition_contract() {
    // The bytes the exchange scatter computes (pack_into into a scratch
    // buffer) must be byte-identical to the `_join_pk` stored by promote_into,
    // so the delta scatter and the reindexed trace land on the same partition.
    let schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I32, 0),
            SchemaColumn::new(type_code::U64, 0),
        ],
        &[0],
    );
    // Reindex on (col2 U64 payload, col1 I32 payload) — a 2-column non-PK key.
    let cols = [2u32, 1u32];
    let rows: &[(u64, i32, u64)] = &[
        (1, -5, 100),
        (2, 7, 100), // same col2 as row 0, different col1
        (3, -5, 200),
        (4, i32::MIN, 0),
    ];
    let mut b = Batch::with_capacity(schema, rows.len());
    for &(pk, c1, c2) in rows {
        b.extend_pk(pk as u128);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &c1.to_le_bytes()); // I32 payload (pi 0)
        b.extend_col(1, &c2.to_le_bytes()); // U64 payload (pi 1)
        b.count += 1;
    }
    let mb = b.as_mem_batch();

    let key: Vec<(u32, u8)> = cols.iter().map(|&c| (c, 0)).collect();
    let packer = ReindexPacker::new(&schema, &key).unwrap();
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0), // col2 → U64
            SchemaColumn::new(type_code::I32, 0), // col1 → I32
        ],
        &[0, 1],
    );
    let mut out = Batch::zeroed(out_schema, rows.len());
    promote_into(&packer, &mb, &mut out);

    for row in 0..rows.len() {
        let mut buf = [0u8; crate::schema::MAX_PK_BYTES];
        packer.pack_into(&mut buf[..packer.out_stride], &mb, row);
        // Trace side (stored _join_pk) == scatter side (scratch buffer).
        assert_eq!(out.get_pk_bytes(row), &buf[..packer.out_stride], "row {row} key bytes");
        assert_eq!(
            gnitz_wire::worker_for_pk_bytes(out.get_pk_bytes(row), NW),
            gnitz_wire::worker_for_pk_bytes(&buf[..packer.out_stride], NW),
            "row {row} co-partition",
        );
    }
    // Rows 0 and 1 share col2 but differ in col1 → distinct keys.
    assert_ne!(out.get_pk_bytes(0), out.get_pk_bytes(1));
}

#[test]
fn test_reindex_packer_copartition_contract_wide() {
    // WIDE-branch (24-byte key) co-partition pin: three independent builders
    // must agree on the bytes — the scatter (`pack_into`), the trace store
    // (`promote_into`), and the ingest OPK encoder (`opk_pk`, which never
    // touches `ReindexPacker`) — and the formula pin below catches a fork in
    // the wide routing arm, which byte-equality alone cannot.
    let schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0), // PK (not part of the key)
            SchemaColumn::new(type_code::U64, 0), // c1 payload → key slot 0
            SchemaColumn::new(type_code::U64, 0), // c2 payload → key slot 1
            SchemaColumn::new(type_code::U64, 0), // c3 payload → key slot 2
        ],
        &[0],
    );
    // Non-trivial, high-entropy column values (so a forked hash seed/shift in
    // the wide arm lands on a different bucket with overwhelming probability).
    let key: [u64; 3] = [0x0102_0304_0506_0708, 0xA0B0_C0D0_E0F0_0102, 0xdead_beef_cafe_1234];
    let mut b = Batch::with_capacity(schema, 1);
    b.extend_pk(42u128);
    b.extend_weight(&1i64.to_le_bytes());
    b.extend_null_bmp(&0u64.to_le_bytes());
    b.extend_col(0, &key[0].to_le_bytes()); // payload pi 0 (c1)
    b.extend_col(1, &key[1].to_le_bytes()); // payload pi 1 (c2)
    b.extend_col(2, &key[2].to_le_bytes()); // payload pi 2 (c3)
    b.count += 1;
    let mb = b.as_mem_batch();

    // Reindex on the three U64 payload columns → a 24-byte (3×U64) OPK key.
    let packer = ReindexPacker::new(&schema, &[(1, 0), (2, 0), (3, 0)]).unwrap();
    assert_eq!(packer.out_stride, 24, "3×U64 reindex key must be 24 bytes (wide)");

    // The reindex output schema = natural 3×U64 PK (what the reindex map stamps and
    // what the trace store holds); identical layout to `wide_pk_3xu64_schema`
    // minus the trailing payload.
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U64, 0),
        ],
        &[0, 1, 2],
    );
    assert!(out_schema.pk_stride() > 16, "test invariant: 24-byte key is wide");

    // PATH 1 — trace store: promote_into stamps the `_join_pk`; read it back.
    let mut out = Batch::zeroed(out_schema, 1);
    promote_into(&packer, &mb, &mut out);
    let consumer = out.get_pk_bytes(0);

    // PATH 2 — exchange scatter: pack_into into a scratch buffer.
    let mut buf = [0u8; crate::schema::MAX_PK_BYTES];
    packer.pack_into(&mut buf[..packer.out_stride], &mb, 0);
    let producer = &buf[..packer.out_stride];

    // PATH 3 — storage/ingest OPK encoder (no ReindexPacker involved at all).
    let oracle = opk_pk(&out_schema, &[key[0] as u128, key[1] as u128, key[2] as u128]);

    // (1) BYTE-EQUALITY teeth: all three independent builders agree, and the
    // key is genuinely wide (> 16 bytes).
    assert_eq!(consumer.len(), 24, "consumer key is the 24-byte wide region");
    assert!(consumer.len() > 16, "wide branch requires key len > 16");
    assert_eq!(producer, consumer, "scatter (pack_into) == trace store (_join_pk)");
    assert_eq!(consumer, oracle.as_slice(), "trace store == ingest OPK encoder");
    assert_eq!(producer, oracle.as_slice(), "scatter == ingest OPK encoder");

    // (2) CO-PARTITION teeth: producer and consumer route to the same worker
    // through the WIDE arm of worker_for_pk_bytes.
    let p_consumer = gnitz_wire::worker_for_pk_bytes(consumer, NW);
    let p_producer = gnitz_wire::worker_for_pk_bytes(producer, NW);
    let p_oracle = gnitz_wire::worker_for_pk_bytes(oracle.as_slice(), NW);
    assert_eq!(p_producer, p_consumer, "producer/consumer co-partition (wide)");
    assert_eq!(p_consumer, p_oracle, "trace store / ingest co-partition (wide)");

    // (3) WIDE-ARM FORMULA pin: the owner is the multiply-shift re-bucketing
    // of XXH3-64 over the OPK bytes, recomputed here. A forked seed or shift
    // would still keep producer == consumer — both call the same function —
    // so only an independent reference catches it.
    let expected = ((crate::foundation::xxh::checksum(consumer) as u128 * NW as u128) >> 64) as usize;
    assert_eq!(p_consumer, expected, "wide owner == ((xxh3_64(opk) * W) >> 64)");
    assert!(expected < NW, "the owner is a launched worker");
}

#[test]
fn test_reindex_packer_null_key_determinism() {
    // A NULL value in a nullable (unsigned) reindex key column is canonically
    // zeroed at the source; the packer reads those zeros (ignoring the null
    // bitmap) and OPK-encodes them. Two distinct rows both NULL in the key
    // column must pack that slot identically (all-zero for an unsigned key).
    let schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U32, 1), // nullable U32 key
        ],
        &[0],
    );
    let mut b = Batch::with_capacity(schema, 2);
    // Row 0 and row 1: distinct PK, both NULL in col1 (slot zeroed, null bit set).
    for pk in [10u128, 20u128] {
        b.extend_pk(pk);
        b.extend_weight(&1i64.to_le_bytes());
        // null bit for payload col index 0 (col1) set.
        b.extend_null_bmp(&1u64.to_le_bytes());
        b.extend_col(0, &0u32.to_le_bytes());
        b.count += 1;
    }
    let mb = b.as_mem_batch();

    let packer = ReindexPacker::new(&schema, &[(1, 0)]).unwrap();
    assert_eq!(packer.out_stride, 4); // U32 key → 4-byte slot

    let mut buf0 = [0u8; crate::schema::MAX_PK_BYTES];
    let mut buf1 = [0u8; crate::schema::MAX_PK_BYTES];
    packer.pack_into(&mut buf0[..packer.out_stride], &mb, 0);
    packer.pack_into(&mut buf1[..packer.out_stride], &mb, 1);

    assert_eq!(&buf0[..4], &[0u8; 4], "NULL unsigned key slot is all-zero");
    assert_eq!(&buf0[..4], &buf1[..4], "two NULL-key rows pack identically");
}

// -----------------------------------------------------------------------
// ReindexPacker::new_group_key — the presence bitmap
// -----------------------------------------------------------------------

#[test]
fn test_group_key_bitmap_bit_positions() {
    // Two packed group columns, only the second nullable: the bitmap must set
    // **bit 1**, not bit 0. Getting it wrong silently merges a NULL group with
    // a `0` group, and the end-to-end coverage groups on one nullable column,
    // where every wrong position still lands on bit 0.
    let schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0), // A: NOT NULL   → packed slot 0
            SchemaColumn::new(type_code::U32, 1), // B: nullable   → packed slot 1
        ],
        &[0],
    );
    // Row 0: B is NULL (payload slot 1 → null-word bit 1). Row 1: B == 0.
    let mut b = Batch::with_capacity(schema, 2);
    for (pk, null_word) in [(10u128, 1u64 << 1), (20u128, 0u64)] {
        b.extend_pk(pk);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&null_word.to_le_bytes());
        b.extend_col(0, &7i64.to_le_bytes()); // A, same in both rows
        b.extend_col(1, &0u32.to_le_bytes()); // B: the canonical zero under a NULL
        b.count += 1;
    }
    let mb = b.as_mem_batch();

    let packer = ReindexPacker::new_group_key(&schema, &[1, 2], &[]);
    assert_eq!(packer.out_stride, 1 + 8 + 4, "bitmap ++ I64 slot ++ U32 slot");

    let mut null_row = [0u8; crate::schema::MAX_PK_BYTES];
    let mut zero_row = [0u8; crate::schema::MAX_PK_BYTES];
    packer.pack_into(&mut null_row[..packer.out_stride], &mb, 0);
    packer.pack_into(&mut zero_row[..packer.out_stride], &mb, 1);

    assert_eq!(null_row[0], 0b10, "NULL in packed column 1 sets bit 1, not bit 0");
    assert_eq!(zero_row[0], 0, "no NULL, no bits");
    // The NULL slot is zeroed and B == 0 encodes to zeros, so the bitmap byte
    // is the *only* thing separating a NULL group from a `0` group.
    assert_eq!(
        &null_row[1..packer.out_stride],
        &zero_row[1..packer.out_stride],
        "the two rows differ in nothing but the bitmap"
    );
    assert_ne!(
        &null_row[..packer.out_stride],
        &zero_row[..packer.out_stride],
        "a NULL group must not collide with a 0 group"
    );
}

// -----------------------------------------------------------------------
// ReindexPacker::pack_into — property test over every PK-eligible type
// -----------------------------------------------------------------------

mod pack_proptest {
    use super::*;
    use crate::test_support::{arb_pk_type, pk_only_schema};
    use proptest::prelude::*;

    /// A legal carried promotion target: the widest slot of the source's own
    /// signedness. For the already-widest codes that is the self-derived type,
    /// so the two passes below are always legal, not always distinct.
    fn carried_target(tc: u8) -> u8 {
        match tc {
            type_code::U8 | type_code::U16 | type_code::U32 | type_code::U64 => type_code::U64,
            type_code::I8 | type_code::I16 | type_code::I32 | type_code::I64 => type_code::I64,
            type_code::I128 => type_code::I128,
            _ => type_code::U128, // U128, UUID
        }
    }

    /// `(column type codes, one native-LE value per column)`. 1..=MAX_PK_COLUMNS
    /// columns over every PK-eligible code, at every width combination — the
    /// surface the running-sum slot offsets live on.
    fn arb_key_case() -> impl Strategy<Value = (Vec<u8>, Vec<Vec<u8>>)> {
        prop::collection::vec(arb_pk_type(), 1..=crate::schema::MAX_PK_COLUMNS).prop_flat_map(|types| {
            let vals: Vec<_> = types
                .iter()
                .map(|&t| prop::collection::vec(any::<u8>(), gnitz_wire::wire_stride(t)))
                .collect();
            (Just(types), vals)
        })
    }

    proptest! {
        /// At every arity and every PK-eligible type, self-derived and carried:
        /// each slot equals its own wire encoder's output at the offset the
        /// running sum put it, **and** the two placements agree with each
        /// other. The second does not follow from the first — PK and payload
        /// placement go through different primitives — and it is the
        /// co-partition contract this file exists for.
        #[test]
        fn reindex_pack_matches_wire_encoders((types, vals) in arb_key_case()) {
            // Payload placement: [U64 PK, c0 .. cn-1], key = the payload columns.
            let mut cols = vec![SchemaColumn::new(type_code::U64, 0)];
            cols.extend(types.iter().map(|&tc| SchemaColumn::new(tc, 0)));
            let pay_schema = SchemaDescriptor::new(&cols, &[0]);
            let mut pb = Batch::with_capacity(pay_schema, 1);
            pb.extend_pk(1u128);
            pb.extend_weight(&1i64.to_le_bytes());
            pb.extend_null_bmp(&0u64.to_le_bytes());
            for (i, v) in vals.iter().enumerate() {
                pb.extend_col(i, v);
            }
            pb.count += 1;
            let pay_mb = pb.as_mem_batch();

            // PK placement: the same columns, all of them PK columns, OPK at rest.
            let pk_schema = pk_only_schema(&types);
            let mut opk = Vec::new();
            for (i, v) in vals.iter().enumerate() {
                let mut slot = vec![0u8; v.len()];
                gnitz_wire::encode_pk_column(v, types[i], &mut slot);
                opk.extend_from_slice(&slot);
            }
            let mut kb = Batch::with_capacity(pk_schema, 1);
            kb.extend_pk_bytes(&opk);
            kb.extend_weight(&1i64.to_le_bytes());
            kb.extend_null_bmp(&0u64.to_le_bytes());
            kb.count += 1;
            let pk_mb = kb.as_mem_batch();

            for carried in [false, true] {
                let target = |tc| if carried { carried_target(tc) } else { 0 };
                let pay_key: Vec<(u32, u8)> = types.iter().enumerate().map(|(i, &tc)| (i as u32 + 1, target(tc))).collect();
                let pk_key: Vec<(u32, u8)> = types.iter().enumerate().map(|(i, &tc)| (i as u32, target(tc))).collect();
                let pay_packer = ReindexPacker::new(&pay_schema, &pay_key).unwrap();
                let pk_packer = ReindexPacker::new(&pk_schema, &pk_key).unwrap();
                let stride = pay_packer.out_stride;
                prop_assert_eq!(stride, pk_packer.out_stride);

                let mut pay_buf = [0u8; crate::schema::MAX_PK_BYTES];
                let mut pk_buf = [0u8; crate::schema::MAX_PK_BYTES];
                pay_packer.pack_into(&mut pay_buf[..stride], &pay_mb, 0);
                pk_packer.pack_into(&mut pk_buf[..stride], &pk_mb, 0);

                // (1) Absolute.
                let (mut off, mut src_off) = (0usize, 0usize);
                for (i, &tc) in types.iter().enumerate() {
                    let out_tc = gnitz_wire::resolve_reindex_type(tc, target(tc));
                    let w = gnitz_wire::wire_stride(out_tc);
                    let src_w = gnitz_wire::wire_stride(tc);

                    let mut want_pay = vec![0u8; w];
                    gnitz_wire::encode_pk_column_promoted(&vals[i], tc, out_tc, &mut want_pay);
                    prop_assert_eq!(&pay_buf[off..off + w], &want_pay[..], "payload slot {}", i);

                    let mut want_pk = vec![0u8; w];
                    gnitz_wire::promote_opk_column(&opk[src_off..src_off + src_w], tc, out_tc, &mut want_pk);
                    prop_assert_eq!(&pk_buf[off..off + w], &want_pk[..], "pk slot {}", i);

                    off += w;
                    src_off += src_w;
                }
                prop_assert_eq!(off, stride, "slot widths sum to out_stride");

                // (2) Cross-placement.
                prop_assert_eq!(&pay_buf[..stride], &pk_buf[..stride]);
            }
        }
    }
}

/// Release-only microbench for `pack_into` — once per equijoin delta row and
/// per scatter row. Both packer shapes: a 3-column join key (offset sum, no
/// null word) and a nullable 2-column group key (bitmap + null-word read).
/// `cd crates && cargo test -p gnitz-store --release reindex_pack_bench -- --ignored --nocapture --test-threads=1`
#[test]
#[ignore]
fn reindex_pack_bench() {
    use std::hint::black_box;
    use std::time::Instant;

    const N: usize = 1_000_000;
    const ITERS: usize = 20;

    // --- 3-column join key: [U64 PK, U64, U64, U64], reindex on (1, 2, 3).
    let join_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U64, 0),
        ],
        &[0],
    );
    let mut jb = Batch::with_capacity(join_schema, N);
    for i in 0..N as u64 {
        jb.extend_pk(i as u128);
        jb.extend_weight(&1i64.to_le_bytes());
        jb.extend_null_bmp(&0u64.to_le_bytes());
        jb.extend_col(0, &i.wrapping_mul(2_654_435_761).to_le_bytes());
        jb.extend_col(1, &i.wrapping_mul(0x9E37_79B9_7F4A_7C15).to_le_bytes());
        jb.extend_col(2, &(!i).to_le_bytes());
        jb.count += 1;
    }
    let jmb = jb.as_mem_batch();
    let join_packer = ReindexPacker::new(&join_schema, &[(1, 0), (2, 0), (3, 0)]).unwrap();
    assert_eq!(join_packer.out_stride, 24);

    // --- Nullable 2-column group key: [U64 PK, I64, U32 NULL], group on (1, 2).
    let grp_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::U32, 1),
        ],
        &[0],
    );
    let mut gb = Batch::with_capacity(grp_schema, N);
    for i in 0..N as u64 {
        gb.extend_pk(i as u128);
        gb.extend_weight(&1i64.to_le_bytes());
        // Every 8th row is NULL in the nullable group column.
        gb.extend_null_bmp(&(u64::from(i % 8 == 0) << 1).to_le_bytes());
        gb.extend_col(0, &(i as i64).wrapping_mul(-7).to_le_bytes());
        gb.extend_col(1, &(i as u32).to_le_bytes());
        gb.count += 1;
    }
    let gmb = gb.as_mem_batch();
    let grp_packer = ReindexPacker::new_group_key(&grp_schema, &[1, 2], &[]);

    for (name, packer, mb) in [("join3", &join_packer, &jmb), ("group2-nullable", &grp_packer, &gmb)] {
        let stride = packer.out_stride;
        let mut buf = [0u8; crate::schema::MAX_PK_BYTES];
        // Warm up.
        packer.pack_into(&mut buf[..stride], mb, 0);

        let t = Instant::now();
        let mut acc = 0u64;
        for _ in 0..ITERS {
            for row in 0..N {
                packer.pack_into(&mut buf[..stride], mb, row);
                acc = acc.wrapping_add(black_box(buf[0]) as u64);
            }
        }
        let secs = t.elapsed().as_secs_f64();
        println!(
            "reindex_pack_bench[{name}]: {:.1} Mrows/s ({N} rows x {ITERS} iters in {secs:.3}s, stride {stride}, checksum {acc})",
            (N * ITERS) as f64 / secs / 1e6,
        );
    }
}

/// `probe_key` keeps the whole span's entropy at both widths: a span past
/// `NARROW_PK_MAX_BYTES` takes the xxh3 branch, where a `u128` image could not
/// hold it, so two spans sharing a 16-byte prefix must not collapse together.
#[test]
fn probe_key_distinguishes_spans_past_the_narrow_width() {
    let narrow = [7u64.to_be_bytes(), 8u64.to_be_bytes()].concat();
    assert_eq!(narrow.len(), NARROW_PK_MAX_BYTES);
    assert_ne!(
        probe_key(&narrow),
        probe_key(&[8u64.to_be_bytes(), 7u64.to_be_bytes()].concat())
    );

    let wide = |tail: u64| [&7u128.to_be_bytes()[..], &tail.to_be_bytes()[..]].concat();
    assert_eq!(wide(3)[..16], wide(4)[..16], "the two spans share a 16-byte prefix");
    assert_ne!(probe_key(&wide(3)), probe_key(&wide(4)));

    // A narrow span right-aligns, so leading zeros are not information.
    assert_eq!(probe_key(&[0, 0, 0, 5]), probe_key(&[5]));
}

// ---------------------------------------------------------------------------
// A range descriptor's cut pair as a base-PK key range
// ---------------------------------------------------------------------------

/// The base-PK key range, through the degenerate identity-promotion span. The
/// expected bytes below predate `for_pk`, so they pin its promotion too.
fn pk_range_keys(schema: &SchemaDescriptor, d: &RangeDescriptor) -> Result<Option<(PkBuf, Option<PkBuf>)>, String> {
    IndexKeySpec::for_pk(schema).range_keys(schema.pk_stride() as usize, d)
}

fn opk_u64(v: u64) -> Vec<u8> {
    v.to_be_bytes().to_vec() // U64 OPK is plain big-endian
}

/// `pk >= 5` → `[OPK(5), +∞)`. The single-column mainline: no equality pins,
/// `prefix_len == pk_stride`, an unbounded upper edge.
#[test]
fn pk_range_ge_unbounded_above() {
    let s = pk_only_schema(&[type_code::U64]);
    let d = RangeDescriptor::new(&[], Cut::Before(5), Cut::After(u64::MAX as u128));
    let (start, end) = pk_range_keys(&s, &d).unwrap().unwrap();
    assert_eq!(start.pk_bytes(), opk_u64(5));
    assert!(end.is_none(), "unbounded above → end None");
}

/// `pk > 5` → `[OPK(6), +∞)` — the degenerate no-pad `succ` on the whole key.
#[test]
fn pk_range_gt_increments_whole_key() {
    let s = pk_only_schema(&[type_code::U64]);
    let d = RangeDescriptor::new(&[], Cut::After(5), Cut::After(u64::MAX as u128));
    let (start, end) = pk_range_keys(&s, &d).unwrap().unwrap();
    assert_eq!(start.pk_bytes(), opk_u64(6));
    assert!(end.is_none());
}

/// `pk < 10` → `[OPK(0), OPK(10))`.
#[test]
fn pk_range_lt() {
    let s = pk_only_schema(&[type_code::U64]);
    let d = RangeDescriptor::new(&[], Cut::Before(0), Cut::Before(10));
    let (start, end) = pk_range_keys(&s, &d).unwrap().unwrap();
    assert_eq!(start.pk_bytes(), opk_u64(0));
    assert_eq!(end.unwrap().pk_bytes(), opk_u64(10));
}

/// Full-PK point lookup `pk = 5` → `[OPK(5), OPK(6))` (degenerate cuts).
#[test]
fn pk_range_point_lookup() {
    let s = pk_only_schema(&[type_code::U64]);
    let d = RangeDescriptor::new(&[], Cut::Before(5), Cut::After(5));
    let (start, end) = pk_range_keys(&s, &d).unwrap().unwrap();
    assert_eq!(start.pk_bytes(), opk_u64(5));
    assert_eq!(end.unwrap().pk_bytes(), opk_u64(6));
}

/// An inverted interval (`pk > 10 AND pk < 3`) drains to zero rows.
#[test]
fn pk_range_inverted_is_empty() {
    let s = pk_only_schema(&[type_code::U64]);
    let d = RangeDescriptor::new(&[], Cut::After(10), Cut::Before(3));
    assert_eq!(pk_range_keys(&s, &d).unwrap(), None);
}

/// A signed PK: `pk > -1` seeks to `OPK(0)` (sign-flip order); `After(i64::MAX)`
/// overflows `succ` → unbounded above.
#[test]
fn pk_range_signed_i64() {
    let s = pk_only_schema(&[type_code::I64]);
    let neg1 = (-1i64 as u64) as u128;
    let d = RangeDescriptor::new(&[], Cut::After(neg1), Cut::After((i64::MAX as u64) as u128));
    let (start, end) = pk_range_keys(&s, &d).unwrap().unwrap();
    assert_eq!(start, opk_key(&s, &0i64.to_le_bytes()));
    assert!(end.is_none());
}

/// Compound PK `(a, b)` with `a = 5 AND b > 3`: the range column is `b`, so
/// `start` seeks past `(5, 3)` and stays within the `a == 5` group.
#[test]
fn pk_range_compound_prefix_eq() {
    let s = pk_only_schema(&[type_code::U64, type_code::U64]);
    let d = RangeDescriptor::new(&[5], Cut::After(3), Cut::After(u64::MAX as u128));
    let (start, end) = pk_range_keys(&s, &d).unwrap().unwrap();
    // start = OPK(5,4) — the prefix `(5,3)` incremented on b.
    let s54 = opk_key(&s, &{
        let mut v = Vec::new();
        v.extend_from_slice(&5u64.to_le_bytes());
        v.extend_from_slice(&4u64.to_le_bytes());
        v
    });
    assert_eq!(start, s54);
    // end = the successor of `(5, MAX)` — carries into `a`, i.e. OPK(6, 0).
    let s60 = opk_key(&s, &{
        let mut v = Vec::new();
        v.extend_from_slice(&6u64.to_le_bytes());
        v.extend_from_slice(&0u64.to_le_bytes());
        v
    });
    assert_eq!(end.unwrap(), s60);
}

/// `n_eq` at the PK arity leaves no range column — a trust-boundary reject.
#[test]
fn pk_range_no_range_column_errs() {
    let s = pk_only_schema(&[type_code::U64]);
    let d = RangeDescriptor::new(&[5], Cut::Before(0), Cut::After(0));
    assert!(pk_range_keys(&s, &d).is_err());
}

// ---------------------------------------------------------------------------
// `SchemaDescriptor::confined_worker` — the master's confinement test
// ---------------------------------------------------------------------------

/// A full point is confined to the worker of its own PK bytes, at every PK
/// shape — single, wide, and compound (where the point pins the leading
/// columns through `eq_vals` and points at the last).
#[test]
fn confined_worker_confines_a_full_point() {
    let u64s = pk_only_schema(&[type_code::U64]);
    assert_eq!(
        u64s.confined_worker(&RangeDescriptor::new(&[], Cut::Before(42), Cut::After(42)), NW),
        Some(u64s.worker_for_pk(&opk_pk(&u64s, &[42]), NW))
    );

    let u128s = pk_only_schema(&[type_code::U128]);
    let wide = (1u128 << 100) | 7;
    assert_eq!(
        u128s.confined_worker(&RangeDescriptor::new(&[], Cut::Before(wide), Cut::After(wide)), NW),
        Some(u128s.worker_for_pk(&opk_pk(&u128s, &[wide]), NW))
    );

    let comp = pk_only_schema(&[type_code::U32, type_code::U64]);
    assert_eq!(
        comp.confined_worker(&RangeDescriptor::new(&[9], Cut::Before(4), Cut::After(4)), NW),
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
        SchemaColumn::new(type_code::U64, 0),
        SchemaColumn::new(type_code::U64, 0),
    ];
    let prefix = SchemaDescriptor::new_with_placement(&cols, &[0, 1], Placement::Keyed { prefix_len: 1 });
    // `a = 7 AND b > 3` — a whole trailing-column range inside one `a` group.
    let ranged = RangeDescriptor::new(&[7], Cut::After(3), Cut::After(u64::MAX as u128));
    let want = prefix.worker_for_pk(&opk_pk(&prefix, &[7, 0]), NW);
    assert_eq!(prefix.confined_worker(&ranged, NW), Some(want));
    for b in [4u128, u64::MAX as u128] {
        assert_eq!(
            prefix.confined_worker(&RangeDescriptor::new(&[7], Cut::Before(b), Cut::After(b)), NW),
            Some(want),
            "a full point on (7, {b}) shares the group's worker"
        );
    }

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
    for tc in [type_code::U64, type_code::I64] {
        let s = pk_only_schema(&[tc]);
        let max = if tc == type_code::U64 {
            u64::MAX as u128
        } else {
            i64::MAX as u128
        };
        let d = RangeDescriptor::new(&[], Cut::Before(max), Cut::After(max));
        assert!(
            pk_range_keys(&s, &d).unwrap().unwrap().1.is_none(),
            "After(max) carries out"
        );
        assert_eq!(
            s.confined_worker(&d, NW),
            Some(s.worker_for_pk(&opk_pk(&s, &[max]), NW))
        );
    }
}

/// A range wider than one worker's key span is not confinable: owners are a
/// hash of the key, not monotone in key order, so only a whole-range prefix
/// match proves confinement. A provably-empty range is not confinable
/// either — the worker answers it (a fold sink still owes its ground row) — and
/// neither is a relation whose rows no key places.
#[test]
fn confined_worker_declines_a_multi_key_range_and_an_unkeyed_relation() {
    let s = pk_only_schema(&[type_code::U64]);
    assert_eq!(
        s.confined_worker(&RangeDescriptor::new(&[], Cut::Before(0), Cut::After(1000)), NW),
        None
    );
    assert_eq!(
        s.confined_worker(&RangeDescriptor::new(&[], Cut::After(1000), Cut::Before(0)), NW),
        None,
        "an inverted range is provably empty"
    );
    // Unbounded above from a non-maximal start: the last key is 0xFF…FF.
    assert_eq!(
        s.confined_worker(
            &RangeDescriptor::new(&[], Cut::Before(5), Cut::After(u64::MAX as u128)),
            NW
        ),
        None
    );

    // The same point that confines above names no owner once the rows are not
    // key-placed.
    let point = RangeDescriptor::new(&[], Cut::Before(42), Cut::After(42));
    assert!(s.confined_worker(&point, NW).is_some());
    let cols = [SchemaColumn::new(type_code::U64, 0)];
    for p in [Placement::Replicated, Placement::Local] {
        assert_eq!(
            SchemaDescriptor::new_with_placement(&cols, &[0], p).confined_worker(&point, NW),
            None
        );
    }
}
