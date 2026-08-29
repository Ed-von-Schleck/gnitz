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

/// The null-bitmap convention, pinned where it is now defined: setting and
/// clearing bit `pi` must leave every other payload slot untouched.
#[test]
fn null_word_get_set_roundtrip() {
    let mut w = 0u64;
    assert!(!null_word_get(w, 3));
    null_word_set(&mut w, 3, true);
    assert!(null_word_get(w, 3));
    assert_eq!(w, 0b1000);
    // Clearing leaves the other bits untouched.
    null_word_set(&mut w, 5, true);
    null_word_set(&mut w, 3, false);
    assert!(!null_word_get(w, 3));
    assert!(null_word_get(w, 5));
    assert_eq!(w, 0b100000);
}

/// `npc == 64` is the row-major cap, where the naive `(1 << npc) - 1` would
/// shift by the word width.
#[test]
fn all_payload_null_mask_covers_the_full_word() {
    assert_eq!(all_payload_null_mask(0), 0);
    assert_eq!(all_payload_null_mask(1), 0b1);
    assert_eq!(all_payload_null_mask(63), u64::MAX >> 1);
    assert_eq!(all_payload_null_mask(64), u64::MAX);
}

/// `left_npc == 64` is the row-major cap, where the naive
/// `left | (right << left_npc)` would shift by the word width. It is
/// reachable only with an empty right side, so dropping the shift is exact.
#[test]
fn merge_null_words_at_the_full_left_width() {
    assert_eq!(merge_null_words(0b1011, 0, 64), 0b1011);
    assert_eq!(merge_null_words(u64::MAX, 0, 64), u64::MAX);
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
