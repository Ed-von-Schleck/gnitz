//! Order-preserving primary-key (OPK) byte encoding.
//!
//! A PK region at rest holds **order-preserving big-endian** bytes: for every
//! pair of encoded keys `memcmp(a, b)` equals the typed lexicographic
//! comparison of the PK columns. Both the client write path (`gnitz-core`) and
//! the server read path (`gnitz-engine`) encode/decode through these functions,
//! so the primitive lives here in `gnitz-wire`, the crate both depend on.
//!
//! All PK columns are fixed-width integer scalars (floats/strings/blobs are
//! rejected at DDL), so the transform is a fixed-width bijection: unsigned types
//! map to big-endian, signed types map to big-endian with the sign bit flipped.

#[cfg(test)]
use crate::type_code;

/// Order-preserving big-endian encoding of one PK column.
///
/// `src` and `dst` are both exactly `col.size()` bytes (1/2/4/8/16). Native
/// little-endian input is byte-reversed to big-endian; signed types additionally
/// flip the sign bit of the leading byte so the signed range maps monotonically
/// onto the unsigned range. The result's unsigned lexicographic order equals the
/// numeric order of the source value.
#[inline]
pub fn encode_pk_column(src: &[u8], tc: u8, dst: &mut [u8]) {
    debug_assert_eq!(dst.len(), src.len());
    let signed = crate::is_signed_int(tc);
    match dst.len() {
        16 => dst.copy_from_slice(&u128::from_le_bytes(src.try_into().unwrap()).to_be_bytes()),
        8 => dst.copy_from_slice(&u64::from_le_bytes(src.try_into().unwrap()).to_be_bytes()),
        4 => dst.copy_from_slice(&u32::from_le_bytes(src.try_into().unwrap()).to_be_bytes()),
        2 => dst.copy_from_slice(&u16::from_le_bytes(src.try_into().unwrap()).to_be_bytes()),
        1 => dst[0] = src[0],
        other => unreachable!("PK column size must be 1/2/4/8/16, got {other}"),
    }
    if signed {
        dst[0] ^= 0x80;
    }
}

/// Symmetric inverse of [`encode_pk_column`]: decode an OPK column back to
/// native little-endian bytes. `src` and `dst` are both `col.size()` bytes.
/// Signed types un-flip the sign bit, then the big-endian image is byte-reversed
/// back to little-endian.
#[inline]
pub fn decode_pk_column(src: &[u8], tc: u8, dst: &mut [u8]) {
    debug_assert_eq!(dst.len(), src.len());
    let signed = crate::is_signed_int(tc);
    dst.copy_from_slice(src);
    if signed {
        dst[0] ^= 0x80;
    }
    match dst.len() {
        16 => { let v = u128::from_be_bytes(dst.try_into().unwrap()); dst.copy_from_slice(&v.to_le_bytes()); }
        8  => { let v = u64::from_be_bytes(dst.try_into().unwrap());  dst.copy_from_slice(&v.to_le_bytes()); }
        4  => { let v = u32::from_be_bytes(dst.try_into().unwrap());  dst.copy_from_slice(&v.to_le_bytes()); }
        2  => { let v = u16::from_be_bytes(dst.try_into().unwrap());  dst.copy_from_slice(&v.to_le_bytes()); }
        1  => {}
        other => unreachable!("PK column size must be 1/2/4/8/16, got {other}"),
    }
}

/// [`decode_pk_column`] into an owned 16-byte buffer: the decoded native
/// little-endian value occupies the leading `src.len()` bytes (the column size,
/// which must be ≤ 16). For callers that want an owned scratch buffer rather
/// than threading one through; slice the result with `&buf[..src.len()]`.
#[inline]
pub fn decode_pk_column_owned(src: &[u8], tc: u8) -> [u8; 16] {
    // `src.len()` is a schema-derived column stride and never exceeds 16; the
    // `buf[..src.len()]` slice below already panics past 16, so document the
    // contract loudly (the promoted ColPromoter Pk arm newly leans on this).
    debug_assert!(src.len() <= 16, "decode_pk_column_owned: column stride {} > 16", src.len());
    let mut buf = [0u8; 16];
    decode_pk_column(src, tc, &mut buf[..src.len()]);
    buf
}

/// OPK-encode a native-LE value of type `src_tc` into a `target_tc` slot.
/// `dst.len() == wire_stride(target_tc) >= src.len()`. Sign-extends (signed src)
/// or zero-extends (unsigned src) the native value to the target width, then
/// [`encode_pk_column`]s at `target_tc`. When `src_tc == target_tc` this is
/// exactly `encode_pk_column` (no widening) — the back-compat / identity path.
///
/// Both the trace-side reindex Map and the delta-scatter routing key go through
/// this single primitive, so equal numeric values from either side of a
/// cross-width join pack into byte-identical keys and co-partition.
#[inline]
pub fn encode_pk_column_promoted(src: &[u8], src_tc: u8, target_tc: u8, dst: &mut [u8]) {
    let src_width = src.len();
    let target_width = crate::wire_stride(target_tc);
    debug_assert_eq!(dst.len(), target_width);
    debug_assert!(target_width >= src_width);
    // The fixed 16-byte `scratch` below caps the in-scope target width; every
    // promoted `T` is a PK-eligible ≤16-byte scalar (the decode trust boundary
    // validates this), so this documents the contract a wider future type would
    // have to grow. A violation is a planner/compiler bug, never input — so it
    // must fail loudly rather than silently emit a wrong (mis-joining) key.
    debug_assert!(target_width <= 16);

    if src_tc == target_tc {
        encode_pk_column(src, src_tc, dst);
        return;
    }

    // Native LE: the sign bit is the high bit of the most-significant (last)
    // byte; the extension bytes are appended at the high LE indices.
    let is_neg = crate::is_signed_int(src_tc) && src_width > 0 && (src[src_width - 1] & 0x80) != 0;
    let pad = if is_neg { 0xFFu8 } else { 0x00u8 };

    let mut scratch = [0u8; 16];
    scratch[..src_width].copy_from_slice(src);
    scratch[src_width..target_width].fill(pad);
    encode_pk_column(&scratch[..target_width], target_tc, dst);
}

/// BE value widener for an OPK region slice. Right-aligns (left-zero-pads) the
/// `stride` bytes into a `u128` and reads big-endian, recovering the native
/// value for UNSIGNED PKs (OPK == BE for unsigned). Signed PKs return the OPK
/// value (sign-flipped) — use [`decode_pk_column`] to recover the true integer.
///
/// Schema-free OPK byte primitive (sibling of [`encode_pk_column`]). A stride
/// `> 16` is a wide region and a caller bug. Opposite alignment from a left-
/// aligned sort-key packer; never conflate the two.
#[inline(always)]
pub fn widen_pk_be(pk_bytes: &[u8], stride: usize) -> u128 {
    debug_assert!(stride <= 16, "widen_pk_be: wide PK region (stride {stride})");
    let mut buf = [0u8; 16];
    buf[16 - stride..].copy_from_slice(&pk_bytes[..stride]);
    u128::from_be_bytes(buf)
}

#[cfg(test)]
mod tests {
    use super::*;

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
            (type_code::I8, 1usize), (type_code::I16, 2),
            (type_code::I32, 4), (type_code::I64, 8),
        ] {
            for v in [i64::MIN >> (64 - sz * 8), -1, 0, 1, i64::MAX >> (64 - sz * 8)] {
                roundtrip(tc, &v.to_le_bytes()[..sz]);
            }
        }
    }

    #[test]
    fn decode_pk_column_roundtrips_unsigned() {
        for &(tc, sz) in &[
            (type_code::U8, 1usize), (type_code::U16, 2),
            (type_code::U32, 4), (type_code::U64, 8),
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

    #[test]
    fn opk_order_equiv_signed_i64() {
        // -3 < -1 < 2 must hold byte-lexicographically after encoding.
        let mk = |v: i64| { let mut o = [0u8; 8]; encode_pk_column(&v.to_le_bytes(), type_code::I64, &mut o); o };
        assert!(mk(-3) < mk(-1));
        assert!(mk(-1) < mk(2));
    }

    #[test]
    fn opk_order_equiv_unsigned_u64() {
        let mk = |v: u64| { let mut o = [0u8; 8]; encode_pk_column(&v.to_le_bytes(), type_code::U64, &mut o); o };
        assert!(mk(1) < mk(256));
        assert!(mk(256) < mk(u64::MAX));
    }

    #[test]
    fn decode_pk_column_roundtrips_i128() {
        // The signed-128 join-key type: every value (including bit-127 negatives)
        // must survive encode→decode, and the 2^63/2^64 boundaries that
        // distinguish a U64 image from an I64 image round-trip too.
        for v in [
            i128::MIN, -1i128, 0, 1, i128::MAX,
            1i128 << 63, (1i128 << 63) - 1, 1i128 << 64, (1i128 << 64) - 1,
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
    /// `encode_pk_column` — the identity / back-compat path.
    #[test]
    fn promoted_identity_matches_encode_pk_column() {
        for &(tc, sz) in &[
            (type_code::I8, 1usize), (type_code::I16, 2), (type_code::I32, 4),
            (type_code::I64, 8), (type_code::U8, 1), (type_code::U16, 2),
            (type_code::U32, 4), (type_code::U64, 8), (type_code::U128, 16),
            (type_code::I128, 16),
        ] {
            for v in [0i128, 1, -1, 127, -128, i64::MIN as i128, i64::MAX as i128] {
                let le = v.to_le_bytes();
                let mut expect = vec![0u8; sz];
                encode_pk_column(&le[..sz], tc, &mut expect);
                let mut got = vec![0u8; sz];
                encode_pk_column_promoted(&le[..sz], tc, tc, &mut got);
                assert_eq!(got, expect, "identity path differs for tc={tc} v={v}");
            }
        }
    }

    // ── §7 co-partition property: both join sides pack equal values identically.

    /// OPK-encode `v` (held in i128, low `wire_stride(tc)` LE bytes are its image)
    /// as source type `tc` into a `target`-width slot, through the exact promoted
    /// encoder both join sides use.
    fn promote(v: i128, tc: u8, target: u8) -> [u8; 16] {
        let le = v.to_le_bytes();
        let mut out = [0u8; 16];
        encode_pk_column_promoted(&le[..crate::wire_stride(tc)], tc, target,
            &mut out[..crate::wire_stride(target)]);
        out
    }

    fn assert_copartition(v: i128, l: u8, r: u8, t: u8) {
        let tw = crate::wire_stride(t);
        let (bl, br) = (promote(v, l, t), promote(v, r, t));
        assert_eq!(&bl[..tw], &br[..tw], "byte-identity failed: v={v} L={l} R={r} T={t}");
        assert_eq!(widen_pk_be(&bl[..tw], tw), widen_pk_be(&br[..tw], tw),
            "widen_pk_be disagreement: v={v} T={t}");
    }

    fn s_min(tc: u8) -> i128 { -(1i128 << (crate::wire_stride(tc) * 8 - 1)) }
    fn s_max(tc: u8) -> i128 { (1i128 << (crate::wire_stride(tc) * 8 - 1)) - 1 }
    fn u_max(tc: u8) -> i128 { (1i128 << (crate::wire_stride(tc) * 8)) - 1 }
    fn narrower(l: u8, r: u8) -> u8 { if crate::wire_stride(l) <= crate::wire_stride(r) { l } else { r } }

    #[test]
    fn signed_ladder_copartitions() {
        use type_code::{I8, I16, I32, I64};
        for (l, r, t) in [(I8,I16,I16),(I8,I32,I32),(I8,I64,I64),
                          (I16,I32,I32),(I16,I64,I64),(I32,I64,I64)] {
            let n = narrower(l, r);
            for v in [0, 1, -1, s_min(n), s_max(n), s_min(n) + 1, s_max(n) - 1] {
                assert_copartition(v, l, r, t);
            }
        }
    }

    #[test]
    fn unsigned_ladder_copartitions() {
        use type_code::{U8, U16, U32, U64, U128, UUID};
        for (l, r, t) in [(U8,U16,U16),(U8,U32,U32),(U8,U64,U64),
                          (U16,U32,U32),(U16,U64,U64),(U32,U64,U64),
                          (U32,U128,U128),(U64,U128,U128),(U32,UUID,U128)] {
            let n = narrower(l, r);
            for v in [0, 1, 127, u_max(n), u_max(n) - 1] {
                assert_copartition(v, l, r, t);
            }
        }
    }

    #[test]
    fn cross_sign_copartitions() {
        use type_code::{I8, I16, I32, I64, I128, U8, U16, U32, U64};
        // (unsigned ≤8B, signed, promoted T) — the full in-scope acceptance table.
        // The U64 rows exercise the new signed-128 target at 16-byte width.
        let cases = [
            (U8, I8, I16), (U8, I16, I16), (U8, I32, I32), (U8, I64, I64),
            (U16, I8, I32), (U16, I16, I32), (U16, I32, I32), (U16, I64, I64),
            (U32, I8, I64), (U32, I16, I64), (U32, I32, I64), (U32, I64, I64),
            (U64, I8, I128), (U64, I16, I128), (U64, I32, I128), (U64, I64, I128),
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
                (0, u), (1, u), (127, u), (128, u), (200, u),
                (u_max(u) - 1, u), (u_max(u), u),
                (s_min(s), s), (-56, s), (-1, s), (0, s), (1, s), (127, s), (s_max(s), s),
            ];
            let mut seen: Vec<(i128, [u8; 16])> = Vec::new();
            for &(val, tc) in probes {
                let key = promote(val, tc, t);
                for &(pv, pk) in &seen {
                    assert_eq!(pk[..tw] == key[..tw], pv == val,
                        "cross-sign T-key equal IFF value equal failed: \
                         {val} vs {pv} (u={u} s={s} t={t})");
                }
                seen.push((val, key));
            }
        }
    }
}
