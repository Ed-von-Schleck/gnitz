use super::*;
use crate::{cmp_col_window, FixedInt, TypeCode};

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
/// order of the value it encodes, at every PK-eligible width. `cmp_col_window` is
/// the crate's own definition of that typed order over native LE bytes, and it
/// is tested independently. Every ordered pair is checked, so a wrong sign flip
/// at any width fails here rather than as silently mis-summed weights
/// downstream.
#[test]
fn opk_byte_order_is_typed_order() {
    for &tc in TypeCode::ALL.iter().filter(|t| t.is_pk_eligible()) {
        let sz = tc.wire_stride();
        let imgs: Vec<[u8; 16]> = PATTERNS.iter().map(|p| p.to_le_bytes()).collect();
        let keys: Vec<Vec<u8>> = imgs
            .iter()
            .map(|le| {
                let mut o = vec![0u8; sz];
                encode_pk_column(&le[..sz], tc, &mut o);
                o
            })
            .collect();
        for (i, a) in imgs.iter().enumerate() {
            for (j, b) in imgs.iter().enumerate() {
                assert_eq!(
                    keys[i].cmp(&keys[j]),
                    cmp_col_window(&a[..sz], &[], &b[..sz], &[], tc),
                    "tc={tc} sz={sz}: pattern {i} vs {j}",
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
    for &tc in TypeCode::ALL.iter().filter(|t| t.is_pk_eligible()) {
        let sz = tc.wire_stride();
        for p in PATTERNS {
            let le = p.to_le_bytes();
            let mut opk = vec![0u8; sz];
            encode_pk_column(&le[..sz], tc, &mut opk);
            let mut back = vec![0u8; sz];
            decode_pk_column(&opk, tc, &mut back);
            assert_eq!(back, &le[..sz], "decode(encode(v)) != v for tc={tc} p={p:#x}");
        }
    }
}

/// `decode_opk_i64` fuses the OPK decode with `FixedInt`'s widening, so it must
/// return the value that was encoded, over each type's whole range. A wrong XOR
/// arm is otherwise a silent wrong answer on every PK predicate.
#[test]
fn decode_opk_i64_recovers_the_encoded_value() {
    for &(fi, tc) in &[
        (FixedInt::U8, TypeCode::U8),
        (FixedInt::I8, TypeCode::I8),
        (FixedInt::U16, TypeCode::U16),
        (FixedInt::I16, TypeCode::I16),
        (FixedInt::U32, TypeCode::U32),
        (FixedInt::I32, TypeCode::I32),
        (FixedInt::U64, TypeCode::U64),
        (FixedInt::I64, TypeCode::I64),
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
    encode_pk_column(&u64::MAX.to_le_bytes(), TypeCode::U64, &mut opk);
    assert_eq!(decode_opk_i64(&opk, FixedInt::U64), -1i64);
}

/// `store_opk_image` writes what decode → widen → encode writes, for every
/// promotion the engine performs.
#[test]
fn store_opk_image_matches_decode_widen_encode() {
    let pk_types: Vec<TypeCode> = TypeCode::ALL.iter().copied().filter(|t| t.is_pk_eligible()).collect();
    let mut pairs: Vec<(TypeCode, TypeCode)> = Vec::new();
    for &src in &pk_types {
        for &target in &pk_types {
            if src == target || src.int_domain_fits(target) {
                pairs.push((src, target));
            }
        }
    }
    pairs.push((TypeCode::UUID, TypeCode::U128));
    for (src, target) in pairs {
        let (sw, tw) = (src.wire_stride(), target.wire_stride());
        for p in PATTERNS {
            let le = p.to_le_bytes();
            let native = &le[..sw];
            let mut opk_src = [0u8; 16];
            encode_pk_column(native, src, &mut opk_src[..sw]);

            let mut got = [0u8; 16];
            store_opk_image(widen_pk_be(&opk_src[..sw]), src, sw, target, &mut got[..tw]);

            let mut wide = [0u8; 16];
            widen_native_le(native, src, &mut wide[..tw]);
            let mut want = [0u8; 16];
            encode_pk_column(&wide[..tw], target, &mut want[..tw]);
            assert_eq!(got[..tw], want[..tw], "src={src} target={target} p={p:#x}");
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
                widen_pk_be(&bytes[..stride]),
                u128::from_be_bytes(buf),
                "stride {stride} diverges from the general form"
            );
        }
    }
}

// ── Co-partition property: both join sides pack equal values identically.

/// `v`, read at source type `tc`, OPK-encoded into a `target`-width slot.
fn promote(v: i128, tc: TypeCode, target: TypeCode) -> [u8; 16] {
    let key = encode_pk_images([(tc, target, key_image(tc, v as u128))]);
    let mut out = [0u8; 16];
    out[..key.width()].copy_from_slice(key.pk_bytes());
    out
}

fn assert_copartition(v: i128, l: TypeCode, r: TypeCode, t: TypeCode) {
    let tw = t.wire_stride();
    let (bl, br) = (promote(v, l, t), promote(v, r, t));
    assert_eq!(&bl[..tw], &br[..tw], "byte-identity failed: v={v} L={l} R={r} T={t}");
    assert_eq!(
        widen_pk_be(&bl[..tw]),
        widen_pk_be(&br[..tw]),
        "widen_pk_be disagreement: v={v} T={t}"
    );
}

/// The representable `(min, max)` of a ≤8-byte integer type code, read from the
/// crate's own [`FixedInt::range`] rather than re-derived from the width.
fn range_of(tc: TypeCode) -> (i128, i128) {
    FixedInt::from_type_code(tc)
        .expect("a fixed-width ≤8-byte integer type code")
        .range()
}
fn narrower(l: TypeCode, r: TypeCode) -> TypeCode {
    if l.wire_stride() <= r.wire_stride() {
        l
    } else {
        r
    }
}

#[test]
fn signed_ladder_copartitions() {
    use TypeCode::{I16, I32, I64, I8};
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
    use TypeCode::{U128, U16, U32, U64, U8, UUID};
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
    use TypeCode::{I128, I16, I32, I64, I8, U16, U32, U64, U8};
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
        let tw = t.wire_stride();
        let probes: &[(i128, TypeCode)] = &[
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
