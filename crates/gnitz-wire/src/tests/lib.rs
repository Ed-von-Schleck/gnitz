use super::*;

/// `checksum` must agree byte-for-byte with the C/Python `XXH3_64bits` the
/// other end of the wire runs — the interop contract this crate defines.
#[test]
fn checksum_matches_c_xxh3_64bits() {
    let body_hex = "9800000008000000a000000008000000a800000008000000b000000008000000b800000008000000c000000008000000c800000008000000d000000008000000d800000008000000e000000008000000e800000008000000f00000001000000000010000000000000000000000000000000000000000000001000000000000008000000000000000000000000000000001000000000000000300000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000";
    let body: Vec<u8> = (0..body_hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&body_hex[i..i + 2], 16).unwrap())
        .collect();
    assert_eq!(body.len(), 208);
    let computed = checksum(&body);
    assert_eq!(
        computed, 0x741C9E0BA1D8A9FD_u64,
        "xxhash-rust and C XXH3_64bits disagree: got 0x{computed:016X}"
    );
}

#[test]
fn as_le_bytes_mut_writes_through_to_the_typed_slice() {
    let mut words = [0u64; 2];
    as_le_bytes_mut(&mut words)[8..16].copy_from_slice(&0x0102_0304_0506_0708u64.to_le_bytes());
    assert_eq!(words, [0, 0x0102_0304_0506_0708]);
    assert_eq!(&as_le_bytes(&words)[8..16], &0x0102_0304_0506_0708u64.to_le_bytes());
}

/// `BitIter` yields lowest-first and stops at the empty mask — including for
/// bit 63, where `low_bits_mask`'s widest word ends.
#[test]
fn bit_iter_yields_set_bits_lowest_first() {
    assert_eq!(BitIter(0).next(), None);
    assert_eq!(BitIter(0b1011).collect::<Vec<_>>(), vec![0, 1, 3]);
    assert_eq!(BitIter(1u64 << 63).collect::<Vec<_>>(), vec![63]);
    assert_eq!(BitIter(low_bits_mask(64)).count(), 64);
}

#[test]
fn read_unsigned_zero_extends() {
    // size 1: high-bit-set vs small — must match u8.cmp.
    assert_eq!(read_unsigned_exact(&[0xFF]), 0xFF);
    assert_eq!(read_unsigned_exact(&[0x01]), 0x01);
    assert!(read_unsigned_exact(&[0xFF]) > read_unsigned_exact(&[0x01]));

    // size 2: 0xFFFE > 0x0001 as unsigned (sign-extension would invert).
    assert_eq!(read_unsigned_exact(&0xFFFEu16.to_le_bytes()), 0xFFFE);
    assert_eq!(read_unsigned_exact(&0x0001u16.to_le_bytes()), 0x0001);
    assert!(read_unsigned_exact(&0xFFFEu16.to_le_bytes()) > read_unsigned_exact(&0x0001u16.to_le_bytes()),);

    // size 4.
    let big: u32 = 0xFFFF_FFFE;
    let small: u32 = 0x0000_0001;
    assert_eq!(read_unsigned_exact(&big.to_le_bytes()), big as u64);
    assert!(read_unsigned_exact(&big.to_le_bytes()) > read_unsigned_exact(&small.to_le_bytes()),);

    // size 8: full u64 round-trip.
    let v: u64 = 0xDEAD_BEEF_CAFE_BABE;
    assert_eq!(read_unsigned_exact(&v.to_le_bytes()), v);
}
