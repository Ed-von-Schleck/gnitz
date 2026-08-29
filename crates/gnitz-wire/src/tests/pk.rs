use super::*;
use crate::type_code;

/// The property the two key spaces exist for: a value stored as a PK column
/// and the same value stored as a payload column must produce the same key
/// in each space, or the two sides of a distributed join land on different
/// workers (routing) or miss each other's index entries (native).
#[test]
fn pk_and_payload_keys_agree_on_one_logical_value() {
    use crate::type_code as tc;
    for &(t, sz) in &[(tc::I8, 1usize), (tc::I16, 2), (tc::I32, 4), (tc::I64, 8), (tc::U32, 4)] {
        for &v in &[i64::MIN, -257, -1, 0, 1, 255, i64::MAX] {
            let le = v.to_le_bytes();
            let native = &le[..sz];
            let mut opk = [0u8; 16];
            encode_pk_column(native, t, &mut opk[..sz]);
            assert_eq!(
                pk_route_key(&opk[..sz], 0, sz),
                payload_route_key(native, 0, sz, t),
                "route keys diverge for tc={t} v={v}",
            );
            assert_eq!(
                pk_native_key(&opk[..sz], 0, sz, t),
                payload_native_key(native, 0, sz, t),
                "native keys diverge for tc={t} v={v}",
            );
        }
    }
    // The native space zero-extends: a signed source keeps its
    // two's-complement bits in the low source-width bytes and is NOT
    // sign-extended to 128 bits (`has_pk` re-encodes from the source width).
    assert_eq!(payload_native_key(&(-1i32).to_le_bytes(), 0, 4, tc::I32), 0xFFFF_FFFF);
    assert_eq!(payload_native_key(&(-1i16).to_le_bytes(), 0, 2, tc::I16), 0xFFFF);
    assert_eq!(payload_native_key(&[0xFFu8], 0, 1, tc::I8), 0xFF);
}

/// The three key readers compute the OPK↔native flip arithmetically instead
/// of round-tripping through `encode_pk_column` / `decode_pk_column`. Pin
/// each against the primitive it replaced — `pk_route_key` (unchanged) is an
/// independent oracle for the route space, but both *native* readers were
/// rewritten, so their mutual cross-check alone would no longer catch a
/// wrong flip. Swept over every PK-eligible type and the non-PK types the
/// payload readers must pass through unflipped.
#[test]
fn key_readers_match_the_encode_decode_forms_they_replace() {
    use crate::type_code as tc;
    let cases: &[(u8, usize)] = &[
        (tc::U8, 1),
        (tc::I8, 1),
        (tc::U16, 2),
        (tc::I16, 2),
        (tc::U32, 4),
        (tc::I32, 4),
        (tc::U64, 8),
        (tc::I64, 8),
        (tc::U128, 16),
        (tc::UUID, 16),
        (tc::I128, 16),
        (tc::F32, 4),
        (tc::F64, 8),
        (tc::STRING, 16),
        (tc::BLOB, 16),
    ];
    for &(t, sz) in cases {
        for &v in &[0i128, 1, -1, 127, -128, 255, i64::MIN as i128, i64::MAX as i128] {
            let le = (v as u128).to_le_bytes();
            let native = &le[..sz];

            // payload_native_key: the old form decoded U128/UUID/I128 whole
            // and zero-extended everything else.
            let want_native = if crate::is_wide_int(t) {
                u128::from_le_bytes(native.try_into().unwrap())
            } else {
                crate::read_unsigned_exact(&native[..sz.min(8)]) as u128
            };
            assert_eq!(payload_native_key(native, 0, sz, t), want_native, "tc={t} v={v}");

            // payload_route_key: the old form OPK-encoded integers into a
            // right-aligned scratch buffer; floats and strings went unflipped.
            let want_route = if crate::is_float(t) || crate::is_german_string(t) {
                crate::read_unsigned_exact(&native[..sz.min(8)]) as u128
            } else {
                let mut opk = [0u8; 16];
                encode_pk_column(native, t, &mut opk[16 - sz..]);
                u128::from_be_bytes(opk)
            };
            assert_eq!(payload_route_key(native, 0, sz, t), want_route, "tc={t} v={v}");

            // pk_native_key: the old form ran `decode_pk_column` into a
            // zeroed 16-byte buffer. Only PK-eligible types reach it.
            if crate::is_pk_eligible(t) {
                let mut opk = [0u8; 16];
                encode_pk_column(native, t, &mut opk[..sz]);
                let mut want = [0u8; 16];
                decode_pk_column(&opk[..sz], t, &mut want[..sz]);
                assert_eq!(
                    pk_native_key(&opk[..sz], 0, sz, t),
                    u128::from_le_bytes(want),
                    "tc={t} v={v}"
                );
            }
        }
    }
}

/// `promote_opk_column`'s identity arm must equal the general
/// decode-then-re-encode path it short-circuits, at every PK-eligible width.
#[test]
fn promote_opk_column_identity_matches_decode_encode() {
    for &(t, sz) in &[
        (crate::type_code::I8, 1usize),
        (crate::type_code::U8, 1),
        (crate::type_code::I16, 2),
        (crate::type_code::U16, 2),
        (crate::type_code::I32, 4),
        (crate::type_code::U32, 4),
        (crate::type_code::I64, 8),
        (crate::type_code::U64, 8),
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
/// the 3/5/6/7 widths only the buffer arm serves.
#[test]
fn widen_pk_be_matches_the_general_form() {
    let bytes: [u8; 16] = core::array::from_fn(|i| (i as u8).wrapping_mul(37).wrapping_add(1));
    for stride in 1..=16usize {
        let mut buf = [0u8; 16];
        buf[16 - stride..].copy_from_slice(&bytes[..stride]);
        assert_eq!(
            widen_pk_be(&bytes, stride),
            u128::from_be_bytes(buf),
            "stride {stride} diverges from the general form"
        );
    }
    // All-zero and all-ones edges at every specialized width, the
    // overlapping-load band included.
    for stride in [1usize, 2, 4, 8, 9, 12, 13, 15, 16] {
        assert_eq!(widen_pk_be(&[0u8; 16], stride), 0);
        assert_eq!(widen_pk_be(&[0xFFu8; 16], stride), u128::MAX >> (128 - stride * 8));
    }
}

fn roundtrip(tc: u8, le: &[u8]) {
    let mut opk = vec![0u8; le.len()];
    encode_pk_column(le, tc, &mut opk);
    let mut back = vec![0u8; le.len()];
    decode_pk_column(&opk, tc, &mut back);
    assert_eq!(back, le, "decode(encode(v)) != v for tc={tc} le={le:?}");
}

#[test]
fn decode_pk_column_roundtrips_signed() {
    for &(tc, sz) in &[
        (type_code::I8, 1usize),
        (type_code::I16, 2),
        (type_code::I32, 4),
        (type_code::I64, 8),
    ] {
        for v in [i64::MIN >> (64 - sz * 8), -1, 0, 1, i64::MAX >> (64 - sz * 8)] {
            roundtrip(tc, &v.to_le_bytes()[..sz]);
        }
    }
}

#[test]
fn decode_pk_column_roundtrips_unsigned() {
    for &(tc, sz) in &[
        (type_code::U8, 1usize),
        (type_code::U16, 2),
        (type_code::U32, 4),
        (type_code::U64, 8),
    ] {
        for v in [0u64, 1, 42, u64::MAX >> (64 - sz * 8)] {
            roundtrip(tc, &v.to_le_bytes()[..sz]);
        }
    }
    // U128 / UUID
    for v in [0u128, 1, 1u128 << 64, u128::MAX] {
        roundtrip(type_code::U128, &v.to_le_bytes());
        roundtrip(type_code::UUID, &v.to_le_bytes());
    }
}

/// `decode_opk_i64` is a third spelling of the OPK→native transform, so it
/// is pinned against the two branches it replaces: the signed
/// `decode_pk_column` + `read_signed` pair and the unsigned
/// `widen_pk_be(..) as i64`. A wrong XOR arm is otherwise a silent wrong
/// answer on every PK predicate.
#[test]
fn decode_opk_i64_matches_the_two_branches_it_replaces() {
    use crate::FixedInt as F;
    for &(fi, tc) in &[
        (F::U8, type_code::U8),
        (F::I8, type_code::I8),
        (F::U16, type_code::U16),
        (F::I16, type_code::I16),
        (F::U32, type_code::U32),
        (F::I32, type_code::I32),
        (F::U64, type_code::U64),
        (F::I64, type_code::I64),
    ] {
        let sz = fi.width();
        let (lo, hi) = fi.range();
        for v in [lo, -1, 0, 1, hi] {
            if v < lo || v > hi {
                continue;
            }
            let le = (v as u128).to_le_bytes();
            let mut opk = [0u8; 8];
            encode_pk_column(&le[..sz], tc, &mut opk[..sz]);

            let want = if crate::is_signed_int(tc) {
                let mut back = [0u8; 8];
                decode_pk_column(&opk[..sz], tc, &mut back[..sz]);
                crate::read_signed_exact(&back[..sz])
            } else {
                widen_pk_be(&opk[..sz], sz) as i64
            };
            assert_eq!(
                decode_opk_i64(&opk[..sz], fi),
                want,
                "decode_opk_i64 diverges for {fi:?} v={v}"
            );
        }
    }
    // The unsigned 64-bit edge: the i64 register holds the bit pattern, so
    // `u64::MAX` reads back as `-1`, exactly as `widen_pk_be(..) as i64` does.
    let mut opk = [0u8; 8];
    encode_pk_column(&u64::MAX.to_le_bytes(), type_code::U64, &mut opk);
    assert_eq!(decode_opk_i64(&opk, crate::FixedInt::U64), -1i64);
}

/// The largest worker count the SAL group format supports
/// (`runtime::protocol::sal::MAX_WORKERS`). Restated here because
/// `gnitz-wire` sits below the engine, and the router must be uniform over
/// exactly the counts the engine can launch.
const MAX_WORKERS: usize = 64;

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

#[test]
fn opk_order_equiv_signed_i64() {
    // -3 < -1 < 2 must hold byte-lexicographically after encoding.
    let mk = |v: i64| {
        let mut o = [0u8; 8];
        encode_pk_column(&v.to_le_bytes(), type_code::I64, &mut o);
        o
    };
    assert!(mk(-3) < mk(-1));
    assert!(mk(-1) < mk(2));
}

#[test]
fn opk_order_equiv_unsigned_u64() {
    let mk = |v: u64| {
        let mut o = [0u8; 8];
        encode_pk_column(&v.to_le_bytes(), type_code::U64, &mut o);
        o
    };
    assert!(mk(1) < mk(256));
    assert!(mk(256) < mk(u64::MAX));
}

#[test]
fn decode_pk_column_roundtrips_i128() {
    // The signed-128 join-key type: every value (including bit-127 negatives)
    // must survive encode→decode, and the 2^63/2^64 boundaries that
    // distinguish a U64 image from an I64 image round-trip too.
    for v in [
        i128::MIN,
        -1i128,
        0,
        1,
        i128::MAX,
        1i128 << 63,
        (1i128 << 63) - 1,
        1i128 << 64,
        (1i128 << 64) - 1,
    ] {
        roundtrip(type_code::I128, &v.to_le_bytes());
    }
}

#[test]
fn opk_order_equiv_signed_i128() {
    // -3 < -1 < 2 < 2^64 must hold byte-lexicographically after I128 encoding
    // (the signed sign-flip puts negatives below non-negatives at 16-byte width).
    let mk = |v: i128| {
        let mut o = [0u8; 16];
        encode_pk_column(&v.to_le_bytes(), type_code::I128, &mut o);
        o
    };
    assert!(mk(-3) < mk(-1));
    assert!(mk(-1) < mk(2));
    assert!(mk(2) < mk(1i128 << 64));
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

fn s_min(tc: u8) -> i128 {
    -(1i128 << (crate::wire_stride(tc) * 8 - 1))
}
fn s_max(tc: u8) -> i128 {
    (1i128 << (crate::wire_stride(tc) * 8 - 1)) - 1
}
fn u_max(tc: u8) -> i128 {
    (1i128 << (crate::wire_stride(tc) * 8)) - 1
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
        let n = narrower(l, r);
        for v in [0, 1, -1, s_min(n), s_max(n), s_min(n) + 1, s_max(n) - 1] {
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
        let n = narrower(l, r);
        for v in [0, 1, 127, u_max(n), u_max(n) - 1] {
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
        // [0, min(u_max(u), s_max(s))]) pack byte-identically into T, so equal
        // keys co-partition to the same worker and match in the join.
        let hi = u_max(u).min(s_max(s));
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
            (0, u),
            (1, u),
            (127, u),
            (128, u),
            (200, u),
            (u_max(u) - 1, u),
            (u_max(u), u),
            (s_min(s), s),
            (-56, s),
            (-1, s),
            (0, s),
            (1, s),
            (127, s),
            (s_max(s), s),
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
