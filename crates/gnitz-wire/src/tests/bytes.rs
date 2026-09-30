use super::*;

#[test]
fn as_le_bytes_mut_writes_through_to_the_typed_slice() {
    let mut words = [0u64; 2];
    as_le_bytes_mut(&mut words)[8..16].copy_from_slice(&0x0102_0304_0506_0708u64.to_le_bytes());
    assert_eq!(words, [0, 0x0102_0304_0506_0708]);
    assert_eq!(&as_le_bytes(&words)[8..16], &0x0102_0304_0506_0708u64.to_le_bytes());
}

#[test]
fn extend_from_le_bytes_round_trips_as_le_bytes() {
    let src = [1i64, -2, i64::MAX, i64::MIN];
    let mut dst = vec![7i64];
    extend_from_le_bytes(&mut dst, as_le_bytes(&src));
    assert_eq!(dst, [7, 1, -2, i64::MAX, i64::MIN]);
}

#[test]
#[should_panic(expected = "not a whole number")]
fn extend_from_le_bytes_rejects_a_partial_scalar() {
    let mut dst: Vec<u32> = Vec::new();
    extend_from_le_bytes(&mut dst, &[0u8; 6]);
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

/// The signed reader sign-extends and the unsigned one zero-extends, at every width.
#[test]
fn exact_readers_widen_by_their_signedness() {
    for (cell, s, u) in [
        (&[0xFF][..], -1i64, 0xFFu64),
        (&[0x7F][..], 127, 127),
        (&0xFFFEu16.to_le_bytes()[..], -2, 0xFFFE),
        (&0xFFFF_FFFEu32.to_le_bytes()[..], -2, 0xFFFF_FFFE),
        (&u64::MAX.to_le_bytes()[..], -1, u64::MAX),
    ] {
        assert_eq!((read_signed_exact(cell), read_unsigned_exact(cell)), (s, u), "{cell:?}");
    }
}
