//! Character boundaries in STRING values, which are UTF-8: a character is a
//! codepoint, and begins at every byte that is not a continuation byte.

fn is_continuation(b: u8) -> bool {
    (b & 0xC0) == 0x80
}

/// The fewest characters to go at which the offset walks skip whole words.
const WORD_SKIP_MIN: usize = 9;

/// How many of the eight bytes of `w` begin a character: those whose top bit
/// is clear or whose next bit is set.
fn starts_in(w: [u8; 8]) -> usize {
    let w = u64::from_le_bytes(w);
    (((!w >> 7) | (w >> 6)) & 0x0101_0101_0101_0101).count_ones() as usize
}

/// Reverse the characters of `s` in place.
pub(crate) fn reverse_chars(s: &mut [u8]) {
    // ASCII has no continuation bytes, so its characters are its bytes.
    if !s.is_ascii() {
        for c in s.chunk_by_mut(|_, &b| is_continuation(b)) {
            c.reverse();
        }
    }
    s.reverse();
}

/// The number of characters in `s`.
pub(crate) fn char_count(s: &[u8]) -> usize {
    // `u32` lanes: twice as many per vector as `usize`.
    s.iter().map(|&b| !is_continuation(b) as u32).sum::<u32>() as usize
}

/// Byte offset where character `n` (from 0) begins, or `s.len()` when `s` has
/// no more than `n` characters.
pub(crate) fn char_offset(s: &[u8], mut n: usize) -> usize {
    if n == 0 {
        return 0;
    }
    // Character 0 begins at byte 0.
    let mut i = 1;
    while let Some(w) = s.get(i..i + 8).filter(|_| n >= WORD_SKIP_MIN) {
        let c = starts_in(w.try_into().unwrap());
        if c >= n {
            break;
        }
        n -= c;
        i += 8;
    }
    while i < s.len() {
        if !is_continuation(s[i]) {
            n -= 1;
            if n == 0 {
                return i;
            }
        }
        i += 1;
    }
    s.len()
}

/// Byte offset where the last `n` characters begin: `s.len()` for `n == 0`, and
/// 0 when `s` has no more than `n`.
pub(crate) fn char_offset_back(s: &[u8], mut n: usize) -> usize {
    if n == 0 {
        return s.len();
    }
    let mut i = s.len();
    while i >= 8 && n >= WORD_SKIP_MIN {
        let c = starts_in(s[i - 8..i].try_into().unwrap());
        if c >= n {
            break;
        }
        n -= c;
        i -= 8;
    }
    while i > 0 {
        i -= 1;
        if !is_continuation(s[i]) {
            n -= 1;
            if n == 0 {
                return i;
            }
        }
    }
    0
}

#[cfg(test)]
#[path = "tests/chars.rs"]
mod tests;
