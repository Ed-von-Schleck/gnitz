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
    let mut buf = [0u8; 16];
    decode_pk_column(src, tc, &mut buf[..src.len()]);
    buf
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
}
