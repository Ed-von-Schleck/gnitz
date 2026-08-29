use super::*;
use crate::type_code;
use crate::{cmp_typed_le, FixedInt, TypeCode};

/// Bit patterns every width-parameterized sweep below truncates to its type's
/// width: both sign boundaries, the all-ones edges, and the 2^63 / 2^64 / 2^127
/// steps that separate a narrow image from a wide one.
const PATTERNS: &[u128] = &[
    0,
    1,
    2,
    0x7F,
    0x80,
    0xFF,
    0x100,
    i64::MAX as u128,
    1 << 63,
    u64::MAX as u128,
    1 << 64,
    i128::MAX as u128,
    1 << 127,
    u128::MAX,
];

/// The OPK contract: unsigned byte comparison over an encoded key IS the typed
/// order of the value it encodes, at every PK-eligible width. `cmp_typed_le` is
/// the crate's own definition of that typed order over native LE bytes, and it
/// is tested independently. Every ordered pair is checked, so a wrong sign flip
/// at any width fails here rather than as silently mis-summed weights
/// downstream.
#[test]
fn opk_byte_order_is_typed_order() {
    for tc in TypeCode::ALL.iter().filter(|t| t.is_pk_eligible()) {
        let (raw, sz) = (*tc as u8, tc.wire_stride());
        let imgs: Vec<[u8; 16]> = PATTERNS.iter().map(|p| p.to_le_bytes()).collect();
        let keys: Vec<Vec<u8>> = imgs
            .iter()
            .map(|le| {
                let mut o = vec![0u8; sz];
                encode_pk_column(&le[..sz], raw, &mut o);
                o
            })
            .collect();
        for (i, a) in imgs.iter().enumerate() {
            for (j, b) in imgs.iter().enumerate() {
                assert_eq!(
                    keys[i].cmp(&keys[j]),
                    cmp_typed_le(&a[..sz], &b[..sz], raw),
                    "tc={raw} sz={sz}: pattern {i} vs {j}",
                );
            }
        }
    }
}

/// `decode_pk_column` is `encode_pk_column`'s inverse at every PK-eligible
/// width — the bijection the byte-equal ⟺ key-equal contract rests on. Same
/// type table as the order sweep above, so a new PK-eligible type is covered by
/// both without an edit.
#[test]
fn decode_pk_column_roundtrips_every_pk_type() {
    for tc in TypeCode::ALL.iter().filter(|t| t.is_pk_eligible()) {
        let (raw, sz) = (*tc as u8, tc.wire_stride());
        for p in PATTERNS {
            let le = p.to_le_bytes();
            let mut opk = vec![0u8; sz];
            encode_pk_column(&le[..sz], raw, &mut opk);
            let mut back = vec![0u8; sz];
            decode_pk_column(&opk, raw, &mut back);
            assert_eq!(back, &le[..sz], "decode(encode(v)) != v for tc={raw} p={p:#x}");
        }
    }
}

/// The property the two key spaces exist for: a value stored as a PK column
/// and the same value stored as a payload column must produce the same key
/// in each space, or the two sides of a distributed join land on different
/// workers (routing) or miss each other's index entries (native).
///
/// Float, string and blob columns have no PK counterpart, so the two payload
/// readers must agree with each other instead: neither applies the OPK flip.
#[test]
fn pk_and_payload_keys_agree_on_one_logical_value() {
    for tc in TypeCode::ALL {
        let (raw, sz) = (tc as u8, tc.wire_stride());
        for p in PATTERNS {
            let le = p.to_le_bytes();
            let native = &le[..sz];
            if tc.is_pk_eligible() {
                let mut opk = [0u8; 16];
                encode_pk_column(native, raw, &mut opk[..sz]);
                assert_eq!(
                    pk_route_key(&opk[..sz], 0, sz),
                    payload_route_key(native, 0, sz, raw),
                    "route keys diverge for tc={raw} p={p:#x}",
                );
                assert_eq!(
                    pk_native_key(&opk[..sz], 0, sz, raw),
                    payload_native_key(native, 0, sz, raw),
                    "native keys diverge for tc={raw} p={p:#x}",
                );
            } else {
                assert_eq!(
                    payload_route_key(native, 0, sz, raw),
                    payload_native_key(native, 0, sz, raw),
                    "an unflippable column must read the same in both spaces: tc={raw} p={p:#x}",
                );
            }
        }
    }
    // The native space zero-extends: a signed source keeps its
    // two's-complement bits in the low source-width bytes and is NOT
    // sign-extended to 128 bits (`has_pk` re-encodes from the source width).
    assert_eq!(
        payload_native_key(&(-1i32).to_le_bytes(), 0, 4, type_code::I32),
        0xFFFF_FFFF
    );
    assert_eq!(payload_native_key(&(-1i16).to_le_bytes(), 0, 2, type_code::I16), 0xFFFF);
    assert_eq!(payload_native_key(&[0xFFu8], 0, 1, type_code::I8), 0xFF);
}

/// `decode_opk_i64` fuses the OPK decode with `FixedInt`'s widening, so it must
/// return the value that was encoded, over each type's whole range. A wrong XOR
/// arm is otherwise a silent wrong answer on every PK predicate.
#[test]
fn decode_opk_i64_recovers_the_encoded_value() {
    for &(fi, tc) in &[
        (FixedInt::U8, type_code::U8),
        (FixedInt::I8, type_code::I8),
        (FixedInt::U16, type_code::U16),
        (FixedInt::I16, type_code::I16),
        (FixedInt::U32, type_code::U32),
        (FixedInt::I32, type_code::I32),
        (FixedInt::U64, type_code::U64),
        (FixedInt::I64, type_code::I64),
    ] {
        let sz = fi.width();
        let (lo, hi) = fi.range();
        for v in [lo, -1, 0, 1, hi] {
            if v < lo || v > hi || v > i64::MAX as i128 {
                continue;
            }
            let mut opk = [0u8; 8];
            encode_pk_column(&fi.pack(v).to_le_bytes()[..sz], tc, &mut opk[..sz]);
            assert_eq!(decode_opk_i64(&opk[..sz], fi), v as i64, "{fi:?} v={v}");
        }
    }
    // The one edge the range walk cannot state: `U64`'s maximum does not fit an
    // `i64`, and the register holds the bit pattern, so it reads back as `-1`.
    let mut opk = [0u8; 8];
    encode_pk_column(&u64::MAX.to_le_bytes(), type_code::U64, &mut opk);
    assert_eq!(decode_opk_i64(&opk, FixedInt::U64), -1i64);
}

/// `promote_opk_column`'s identity arm must equal the general
/// decode-then-re-encode path it short-circuits, at every PK-eligible width.
#[test]
fn promote_opk_column_identity_matches_decode_encode() {
    for &(t, sz) in &[
        (type_code::I8, 1usize),
        (type_code::U8, 1),
        (type_code::I16, 2),
        (type_code::U16, 2),
        (type_code::I32, 4),
        (type_code::U32, 4),
        (type_code::I64, 8),
        (type_code::U64, 8),
    ] {
        for &v in &[0i128, 1, -1, i64::MIN as i128, i64::MAX as i128, u64::MAX as i128] {
            let le = (v as u128).to_le_bytes();
            let native = &le[..sz];
            let (mut opk, mut got) = ([0u8; 16], [0u8; 16]);
            encode_pk_column(native, t, &mut opk[..sz]);
            promote_opk_column(&opk[..sz], t, t, &mut got[..sz]);
            let decoded = decode_pk_column_owned(&opk[..sz], t);
            let mut want = [0u8; 16];
            encode_pk_column_promoted(&decoded[..sz], t, t, &mut want[..sz]);
            assert_eq!(got[..sz], want[..sz], "tc={t} v={v}");
            assert_eq!(got[..sz], opk[..sz], "identity must be the verbatim OPK bytes");
        }
    }
}

/// The width-specialized arms must agree with the general right-align form at
/// every stride a PK region can have — the 9..=15 overlapping-load band and
/// the 3/5/6/7 widths only the buffer arm serves — including the all-zero and
/// all-ones edges.
#[test]
fn widen_pk_be_matches_the_general_form() {
    let mixed: [u8; 16] = core::array::from_fn(|i| (i as u8).wrapping_mul(37).wrapping_add(1));
    for bytes in [mixed, [0u8; 16], [0xFFu8; 16]] {
        for stride in 1..=16usize {
            let mut buf = [0u8; 16];
            buf[16 - stride..].copy_from_slice(&bytes[..stride]);
            assert_eq!(
                widen_pk_be(&bytes, stride),
                u128::from_be_bytes(buf),
                "stride {stride} diverges from the general form"
            );
        }
    }
}

/// Peak worker load divided by the mean, over `keys` at `nw` workers.
fn load_spread(keys: &[u128], nw: usize) -> f64 {
    let mut counts = vec![0usize; nw];
    for &k in keys {
        counts[worker_for_key(k, nw)] += 1;
    }
    let mean = keys.len() as f64 / nw as f64;
    counts.iter().map(|&c| c as f64 / mean).fold(0.0, f64::max)
}

/// The multiply-shift re-bucketing must spread every *structured* key set
/// evenly at every launchable worker count — the shapes a real PK actually
/// takes. These sets are deterministic and low-discrepancy under a
/// golden-ratio multiplier, so 1.01× is achievable rather than merely
/// plausible; the random arm below carries its own (looser) bound.
#[test]
fn router_spreads_structured_keys_evenly() {
    const N: u128 = 100_000;
    let sequential: Vec<u128> = (0..N).collect();
    // i64 spanning zero, in the canonical (sign-flipped) routing space.
    let signed: Vec<u128> = (0..N)
        .map(|i| (i as i64 - N as i64 / 2) as u64 as u128 ^ (1u128 << 63))
        .collect();
    let strided: Vec<u128> = (0..N).map(|i| i * 4096).collect();
    // Compound OPK: (hi, lo) packed as one u128, one half held constant.
    // `const_lo` is also the wide-key arm — it is the u128 shape that varies
    // the high word. A key varying *both* halves in lockstep is not covered
    // here: the mix XORs one multiply per half, so correlated halves
    // partially cancel and measure ~1.02–1.08 at high worker counts.
    // Genuinely independent 128-bit keys (UUIDs) are the random arm below.
    let const_hi: Vec<u128> = (0..N).map(|i| (7u128 << 64) | i).collect();
    let const_lo: Vec<u128> = (0..N).map(|i| (i << 64) | 7).collect();

    for (name, keys) in [
        ("sequential u64", &sequential),
        ("signed i64", &signed),
        ("strided", &strided),
        ("compound const-hi", &const_hi),
        ("compound const-lo", &const_lo),
    ] {
        for nw in 1..=MAX_WORKERS {
            let spread = load_spread(keys, nw);
            assert!(spread <= 1.01, "{name}: worker load {spread:.4}x the mean at nw={nw}",);
        }
    }
}

/// A PK region past `NARROW_PK_MAX_BYTES` has no `u128` image, so it routes on
/// the hash of its whole OPK byte string. A 24-byte 3×`U64` compound PK reaches
/// this arm; what must hold is that the bytes past the sixteenth still move the
/// route, which a region silently truncated to a `u128` would not.
#[test]
fn worker_for_pk_bytes_reads_a_whole_wide_region() {
    let mut head = [0u8; 16];
    encode_pk_column(&7u64.to_le_bytes(), type_code::U64, &mut head[..8]);
    encode_pk_column(&9u64.to_le_bytes(), type_code::U64, &mut head[8..]);

    let mut seen = std::collections::HashSet::new();
    for tail in 0..64u64 {
        let mut opk = [0u8; 24];
        opk[..16].copy_from_slice(&head);
        encode_pk_column(&tail.to_le_bytes(), type_code::U64, &mut opk[16..]);
        assert!(opk.len() > NARROW_PK_MAX_BYTES);
        for nw in [1usize, 2, 3, 7, MAX_WORKERS] {
            assert!(worker_for_pk_bytes(&opk, nw) < nw, "route out of range at nw={nw}");
        }
        seen.insert(worker_for_pk_bytes(&opk, 8));
    }
    assert!(seen.len() > 1, "the region past 16 bytes must reach the route");
}

/// Independent 128-bit keys — the UUID shape, and the wide arm the
/// structured test leaves to this one.
///
/// Random keys carry their own sampling noise: at `nw = 64` and 100k keys,
/// max/mean sits in 1.044–1.090 for a *correct* router, so the structured
/// arm's 1.01× would be a flaky test here. 500k keys bring the same
/// measurement into 1.026–1.037, which 1.05× bounds with room.
#[test]
fn router_spreads_random_keys_evenly() {
    const N: usize = 500_000;
    // SplitMix64-style stream over both halves — deterministic, no rand dep.
    let mut s: u64 = 0x243f_6a88_85a3_08d3;
    let mut next = || {
        s = s.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = s;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    };
    let keys: Vec<u128> = (0..N).map(|_| ((next() as u128) << 64) | next() as u128).collect();
    for nw in [1, 2, 3, 4, 8, 16, 32, MAX_WORKERS] {
        let spread = load_spread(&keys, nw);
        assert!(spread <= 1.05, "random keys: {spread:.4}x the mean at nw={nw}");
    }
}

/// `worker_for_pk_bytes` on a narrow OPK region is `worker_for_key` of the
/// widened value, by construction — the invariant that makes a distributed
/// join's two sides agree, since `ColumnLocator::route_key` funnels through
/// `widen_pk_be` too.
#[test]
fn worker_for_pk_bytes_matches_widened_key() {
    for &(tc, sz) in &[
        (type_code::U8, 1usize),
        (type_code::I8, 1),
        (type_code::U16, 2),
        (type_code::I16, 2),
        (type_code::U32, 4),
        (type_code::I32, 4),
        (type_code::U64, 8),
        (type_code::I64, 8),
        (type_code::U128, 16),
    ] {
        for v in [0i128, 1, -1, 7, -7, 127, -128, 1000, -1000, i32::MAX as i128] {
            let le = (v as u128).to_le_bytes();
            let mut opk = [0u8; 16];
            encode_pk_column(&le[..sz], tc, &mut opk[..sz]);
            for nw in [1usize, 2, 3, 4, 7, 16, MAX_WORKERS] {
                assert_eq!(
                    worker_for_pk_bytes(&opk[..sz], nw),
                    worker_for_key(widen_pk_be(&opk[..sz], sz), nw),
                    "tc={tc} v={v} nw={nw}",
                );
            }
        }
    }
}

/// `encode_pk_column_promoted` with `src_tc == target_tc` is exactly
/// `encode_pk_column` — the no-widening fast path.
#[test]
fn promoted_identity_matches_encode_pk_column() {
    for &(tc, sz) in &[
        (type_code::I8, 1usize),
        (type_code::I16, 2),
        (type_code::I32, 4),
        (type_code::I64, 8),
        (type_code::U8, 1),
        (type_code::U16, 2),
        (type_code::U32, 4),
        (type_code::U64, 8),
        (type_code::U128, 16),
        (type_code::I128, 16),
    ] {
        for v in [0i128, 1, -1, 127, -128, i64::MIN as i128, i64::MAX as i128] {
            let le = v.to_le_bytes();
            let mut expect = vec![0u8; sz];
            encode_pk_column(&le[..sz], tc, &mut expect);
            let mut got = vec![0u8; sz];
            encode_pk_column_promoted(&le[..sz], tc, tc, &mut got);
            assert_eq!(got, expect, "no-widening fast path differs for tc={tc} v={v}");
        }
    }
}

// ── Co-partition property: both join sides pack equal values identically.

/// OPK-encode `v` (held in i128, low `wire_stride(tc)` LE bytes are its image)
/// as source type `tc` into a `target`-width slot, through the exact promoted
/// encoder both join sides use.
fn promote(v: i128, tc: u8, target: u8) -> [u8; 16] {
    let le = v.to_le_bytes();
    let mut out = [0u8; 16];
    encode_pk_column_promoted(
        &le[..crate::wire_stride(tc)],
        tc,
        target,
        &mut out[..crate::wire_stride(target)],
    );
    out
}

fn assert_copartition(v: i128, l: u8, r: u8, t: u8) {
    let tw = crate::wire_stride(t);
    let (bl, br) = (promote(v, l, t), promote(v, r, t));
    assert_eq!(&bl[..tw], &br[..tw], "byte-identity failed: v={v} L={l} R={r} T={t}");
    assert_eq!(
        widen_pk_be(&bl[..tw], tw),
        widen_pk_be(&br[..tw], tw),
        "widen_pk_be disagreement: v={v} T={t}"
    );
}

/// The representable `(min, max)` of a ≤8-byte integer type code, read from the
/// crate's own [`FixedInt::range`] rather than re-derived from the width.
fn range_of(tc: u8) -> (i128, i128) {
    FixedInt::from_type_code(TypeCode::from_validated_u8(tc))
        .expect("a fixed-width ≤8-byte integer type code")
        .range()
}
fn narrower(l: u8, r: u8) -> u8 {
    if crate::wire_stride(l) <= crate::wire_stride(r) {
        l
    } else {
        r
    }
}

#[test]
fn signed_ladder_copartitions() {
    use type_code::{I16, I32, I64, I8};
    for (l, r, t) in [
        (I8, I16, I16),
        (I8, I32, I32),
        (I8, I64, I64),
        (I16, I32, I32),
        (I16, I64, I64),
        (I32, I64, I64),
    ] {
        let (lo, hi) = range_of(narrower(l, r));
        for v in [0, 1, -1, lo, hi, lo + 1, hi - 1] {
            assert_copartition(v, l, r, t);
        }
    }
}

#[test]
fn unsigned_ladder_copartitions() {
    use type_code::{U128, U16, U32, U64, U8, UUID};
    for (l, r, t) in [
        (U8, U16, U16),
        (U8, U32, U32),
        (U8, U64, U64),
        (U16, U32, U32),
        (U16, U64, U64),
        (U32, U64, U64),
        (U32, U128, U128),
        (U64, U128, U128),
        (U32, UUID, U128),
    ] {
        let hi = range_of(narrower(l, r)).1;
        for v in [0, 1, 127, hi, hi - 1] {
            assert_copartition(v, l, r, t);
        }
    }
}

#[test]
fn cross_sign_copartitions() {
    use type_code::{I128, I16, I32, I64, I8, U16, U32, U64, U8};
    // (unsigned ≤8B, signed, promoted T) — the full in-scope acceptance table.
    // The U64 rows exercise the new signed-128 target at 16-byte width.
    let cases = [
        (U8, I8, I16),
        (U8, I16, I16),
        (U8, I32, I32),
        (U8, I64, I64),
        (U16, I8, I32),
        (U16, I16, I32),
        (U16, I32, I32),
        (U16, I64, I64),
        (U32, I8, I64),
        (U32, I16, I64),
        (U32, I32, I64),
        (U32, I64, I64),
        (U64, I8, I128),
        (U64, I16, I128),
        (U64, I32, I128),
        (U64, I64, I128),
    ];
    for (u, s, t) in cases {
        // Equal logical values representable on BOTH sides (the overlap
        // [0, min(u_max, s_max)]) pack byte-identically into T, so equal keys
        // co-partition to the same worker and match in the join.
        let (u_lo, u_hi) = range_of(u);
        let (s_lo, s_hi) = range_of(s);
        let hi = u_hi.min(s_hi);
        for v in [0, 1, 127, hi - 1, hi] {
            assert_copartition(v, u, s, t);
        }
        // Injectivity: across a spread drawn from both sides — including the
        // native-byte aliasing trap (e.g. U8 255 and I8 -1 share all-0xFF
        // native bytes; U8 200 and I8 -56 share byte 0xC8) — two promoted
        // T-keys are byte-equal IFF the logical values are equal. No distinct
        // values ever collide; no equal values ever diverge.
        let tw = crate::wire_stride(t);
        let probes: &[(i128, u8)] = &[
            (u_lo, u),
            (1, u),
            (127, u),
            (128, u),
            (200, u),
            (u_hi - 1, u),
            (u_hi, u),
            (s_lo, s),
            (-56, s),
            (-1, s),
            (0, s),
            (1, s),
            (127, s),
            (s_hi, s),
        ];
        let mut seen: Vec<(i128, [u8; 16])> = Vec::new();
        for &(val, tc) in probes {
            let key = promote(val, tc, t);
            for &(pv, pk) in &seen {
                assert_eq!(
                    pk[..tw] == key[..tw],
                    pv == val,
                    "cross-sign T-key equal IFF value equal failed: \
                     {val} vs {pv} (u={u} s={s} t={t})"
                );
            }
            seen.push((val, key));
        }
    }
}
