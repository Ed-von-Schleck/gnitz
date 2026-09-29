use super::{char_count, char_offset, char_offset_back, reverse_chars};

/// Every helper against `str`'s own view of the same text.
fn check(s: &str) {
    let b = s.as_bytes();
    let starts: Vec<usize> = s.char_indices().map(|(k, _)| k).collect();
    assert_eq!(char_count(b), starts.len(), "count {s:?}");
    for n in 0..starts.len() + 3 {
        let front = starts.get(n).copied().unwrap_or(b.len());
        assert_eq!(char_offset(b, n), front, "offset {s:?} n={n}");
        let back = if n == 0 {
            b.len()
        } else {
            starts.len().checked_sub(n).map_or(0, |i| starts[i])
        };
        assert_eq!(char_offset_back(b, n), back, "offset_back {s:?} n={n}");
    }
    let mut r = b.to_vec();
    reverse_chars(&mut r);
    assert_eq!(r, s.chars().rev().collect::<String>().as_bytes(), "reverse {s:?}");
}

/// Every string of up to five characters over one character of each UTF-8
/// width, plus two long strings for the vectorised count, the offset walks'
/// word skips and the ASCII reverse.
#[test]
fn every_helper_agrees_with_str() {
    const ALPHABET: [char; 4] = ['a', 'é', '€', '😀'];
    for len in 0..=5u32 {
        for code in 0..ALPHABET.len().pow(len) {
            let s: String = (0..len)
                .map(|i| ALPHABET[code / ALPHABET.len().pow(i) % ALPHABET.len()])
                .collect();
            check(&s);
        }
    }
    check(&"x".repeat(100));
    check(&"héllo wörld".repeat(8));
}
