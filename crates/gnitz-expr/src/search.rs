//! Byte-level, case-sensitive substring search.

use memchr::memmem::Finder;

/// `a == b`, without the `memcmp` call for literals of at most [`SHORT_LIT`]
/// bytes, which cannot amortize it.
#[inline]
pub(crate) fn lit_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    if a.len() <= SHORT_LIT {
        return a.iter().zip(b).all(|(x, y)| x == y);
    }
    a == b
}

const SHORT_LIT: usize = 4;

/// Start positions at or below which a byte loop beats a vector scan's fixed
/// setup.
const SHORT_HAYSTACK: usize = 16;

/// Whether `h` offers a `len`-byte needle at most [`SHORT_HAYSTACK`] start
/// positions.
#[inline]
fn few_starts(h: &[u8], len: usize) -> bool {
    h.len() < len + SHORT_HAYSTACK
}

/// The byte offset of the first `needle` in `h`, `Some(0)` for an empty one.
/// For a needle that varies per row; a literal fixed at compile time takes
/// [`find_lit`].
pub(crate) fn find(h: &[u8], needle: &[u8]) -> Option<usize> {
    let Some(&first) = needle.first() else {
        return Some(0);
    };
    // How many start positions fit; `None` when the needle is longer than `h`.
    let starts = h.len().checked_sub(needle.len() - 1)?;
    let hay = &h[..starts];
    if few_starts(h, needle.len()) {
        for (i, &b) in hay.iter().enumerate() {
            if b == first && lit_eq(&h[i..i + needle.len()], needle) {
                return Some(i);
            }
        }
        return None;
    }
    let mut off = 0;
    while let Some(k) = memchr::memchr(first, &hay[off..]) {
        let i = off + k;
        if lit_eq(&h[i..i + needle.len()], needle) {
            return Some(i);
        }
        off = i + 1;
    }
    None
}

/// [`find`] through `lit`'s prebuilt searcher.
#[inline]
pub(crate) fn find_lit(h: &[u8], lit: &Finder<'static>) -> Option<usize> {
    let l = lit.needle();
    if few_starts(h, l.len()) {
        find(h, l)
    } else {
        lit.find(h)
    }
}

/// The `(start, end)` byte span of each field of `s` split on the non-empty
/// `d`, left to right — one field, `s` whole, when `d` never occurs.
pub(crate) fn fields<'a>(s: &'a [u8], d: &'a [u8]) -> impl Iterator<Item = (usize, usize)> + 'a {
    debug_assert!(!d.is_empty(), "an empty delimiter splits nothing");
    let mut next = Some(0usize);
    std::iter::from_fn(move || {
        let start = next?;
        let end = match find(&s[start..], d) {
            Some(k) => start + k,
            None => s.len(),
        };
        next = (end < s.len()).then(|| end + d.len());
        Some((start, end))
    })
}
