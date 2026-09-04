use super::{char_count, char_offset, char_offset_back};

/// The fixtures every character rule has to stay total on: ASCII, valid
/// multi-byte UTF-8, bytes that are not UTF-8 at all, a string with no character
/// start whatsoever, and the empty string.
const FIXTURES: [&[u8]; 5] = [
    b"abcde",
    "héllo wörld".as_bytes(),
    &[0xFF, b'a', 0x80, 0xE2, b'b'],
    &[0x80, 0x80],
    b"",
];

/// [`char_offset_back`] is [`char_offset`] counted from the other end, at every
/// count including the two the walk-from-the-front spelling got right by
/// accident: `n == 0` is the end of the string, and a count past the character
/// total is its first character start — `s.len()` for a string that has none.
#[test]
fn char_offset_back_is_char_offset_from_the_other_end() {
    for s in FIXTURES {
        let chars = char_count(s);
        for n in 0..chars + 3 {
            assert_eq!(
                char_offset_back(s, n),
                char_offset(s, 0, chars.saturating_sub(n)),
                "s={s:?} n={n}",
            );
        }
    }
}
