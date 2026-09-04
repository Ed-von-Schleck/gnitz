//! SQL LIKE / ILIKE: the pattern tokenizer and the matcher it compiles into.
//!
//! Pattern bytes in, a matcher out, so the shape contract is testable without
//! building a program. `_` steps by the character boundary [`crate::chars`]
//! defines, the same one SUBSTRING uses.
//!
//! `%` is byte-granular and `_` character-granular. The asymmetry is forced:
//! `_` must be character-granular or `'é' LIKE '_'` would fail on valid text,
//! while a character-granular `%` would answer differently from the
//! specializations below on a value nothing enforces to be valid UTF-8. On valid
//! UTF-8 the two granularities are indistinguishable.

use crate::chars::char_offset;

/// A LIKE pattern compiled once per program at resolve. Total on any bytes, so a
/// forged pattern yields a matcher rather than an error.
///
/// The specialized shapes answer without entering the walk. `Generic` is the
/// single-anchor glob walk, `O(n·m)` in value bytes × pattern *byte length* (a
/// `Lit` token costs its own length per attempt, so token count understates it).
///
/// The specializations answer no question `Generic` could not. `Suffix` and
/// `Contains` skip its per-offset retry; `Exact` and `Prefix` skip only its fixed
/// cost, which is still worth +46 (`'ab%'`) to +67 (`'abc'`) retired instructions
/// per row on the `WHERE s LIKE …` scan-filter path, measured with
/// `perf stat -e instructions:u` against a build with `specialize` forced to
/// return `Generic`. There is no in-tree bench for it — refreshing the number
/// means rebuilding that comparison by hand.
pub(crate) struct LikeMatcher {
    kind: LikeKind,
    /// ASCII-only case folding. The `Lit` bytes below are already lowercased, so
    /// this selects the comparator, and for `Contains` the two candidate scan
    /// bytes.
    ci: bool,
}

enum LikeKind {
    /// No wildcards: `'abc'`, and `''` (no tokens at all).
    Exact(Vec<u8>),
    /// `'lit%'`, and `'%'` → `Prefix(b"")`.
    Prefix(Vec<u8>),
    /// `'%lit'`.
    Suffix(Vec<u8>),
    /// `'%lit%'`.
    Contains(Vec<u8>),
    Generic(Vec<LikeTok>),
}

/// A literal run, `_`, or `%`.
enum LikeTok {
    Lit(Vec<u8>),
    AnyOne,
    AnyMany,
}

/// True iff `pattern` ends with a live escape byte — the shape the SQL binder
/// rejects ("LIKE pattern must not end with escape character"). `None` disables
/// escaping, so the answer is then always false.
///
/// A `pattern.ends_with(escape)` test would be wrong: under the default escape
/// the SQL literal `'a\\'` is three bytes, its last byte is the escape byte, and
/// it is legal — that byte is itself escaped and the pattern means the two
/// characters `a\`. Only the left-to-right walk tells a live trailing escape
/// from an escaped one.
pub fn like_pattern_ends_with_live_escape(pattern: &[u8], escape: Option<u8>) -> bool {
    tokenize(pattern, escape).1
}

/// Tokenize left to right, returning the tokens and whether the pattern ran out
/// immediately after an escape byte with nothing left to make literal.
///
/// Checking the escape before the wildcards is what makes `ESCAPE '%'` and
/// `ESCAPE '_'` fall out: the chosen byte stops being a wildcard and becomes an
/// escape introducer, leaving the other wildcard live. The escape makes the
/// *next* byte literal whatever it is.
fn tokenize(pattern: &[u8], escape: Option<u8>) -> (Vec<LikeTok>, bool) {
    let mut toks: Vec<LikeTok> = Vec::new();
    let mut ends_with_live_escape = false;
    let mut i = 0;
    while i < pattern.len() {
        let b = pattern[i];
        if Some(b) == escape {
            if i + 1 == pattern.len() {
                // No next byte to make literal. The binder rejects this shape,
                // so this arm exists only to keep a forged wire blob total: the
                // escape byte becomes a literal of its own.
                ends_with_live_escape = true;
                push_lit(&mut toks, b);
                i += 1;
            } else {
                push_lit(&mut toks, pattern[i + 1]);
                i += 2;
            }
        } else if b == b'%' {
            // Adjacent `%` collapse into one token, so `%%a` still reaches
            // `Suffix("a")`.
            if !matches!(toks.last(), Some(LikeTok::AnyMany)) {
                toks.push(LikeTok::AnyMany);
            }
            i += 1;
        } else if b == b'_' {
            toks.push(LikeTok::AnyOne);
            i += 1;
        } else {
            push_lit(&mut toks, b);
            i += 1;
        }
    }
    (toks, ends_with_live_escape)
}

/// Append a literal byte, merging into the run in progress so a redundant
/// spelling like `a\%b` still reaches the specialization table as `Exact("a%b")`.
fn push_lit(toks: &mut Vec<LikeTok>, b: u8) {
    match toks.last_mut() {
        Some(LikeTok::Lit(run)) => run.push(b),
        _ => toks.push(LikeTok::Lit(vec![b])),
    }
}

/// ILIKE's ASCII-lowercasing of the literal runs, applied *after* tokenizing —
/// folding first would corrupt an alphabetic escape character, which the
/// tokenizer then no longer recognizes.
fn fold_lits(toks: &mut [LikeTok]) {
    for t in toks {
        if let LikeTok::Lit(run) = t {
            run.make_ascii_lowercase();
        }
    }
}

/// Pick the matcher shape from the exact token sequence, never from the
/// pattern's string shape. `'a_c%'` is four tokens → `Generic`, not
/// `Prefix("a_c")`; `'%_abc%'` is four tokens → `Generic`, not `Contains("abc")`
/// (which would wrongly accept `"abc"` itself).
fn specialize(mut toks: Vec<LikeTok>) -> LikeKind {
    use std::mem::take;
    use LikeTok::{AnyMany, Lit};
    match toks.as_mut_slice() {
        // No tokens at all: matches only the empty string.
        [] => LikeKind::Exact(Vec::new()),
        [Lit(l)] => LikeKind::Exact(take(l)),
        [Lit(l), AnyMany] => LikeKind::Prefix(take(l)),
        // Matches everything, including the empty string.
        [AnyMany] => LikeKind::Prefix(Vec::new()),
        [AnyMany, Lit(l)] => LikeKind::Suffix(take(l)),
        [AnyMany, Lit(l), AnyMany] => LikeKind::Contains(take(l)),
        _ => LikeKind::Generic(toks),
    }
}

impl LikeMatcher {
    /// Compile `pattern` under `escape` (`None` = escaping disabled).
    pub(crate) fn compile(pattern: &[u8], escape: Option<u8>, ci: bool) -> Self {
        let mut toks = tokenize(pattern, escape).0;
        if ci {
            fold_lits(&mut toks);
        }
        LikeMatcher { kind: specialize(toks), ci }
    }

    /// Does `h` match? Total on any bytes, valid UTF-8 or not.
    ///
    /// `#[inline]` so the dispatch below lands inside the caller's row loop,
    /// where `self.kind` is loop-invariant and LLVM unswitches it. Out of line it
    /// is a call plus a five-way jump per row: `expr_kernel_bench` retires 21 %
    /// more on `str_like`, 11 % on a 12-byte `Contains`, 10 % on a 128-byte one.
    #[inline]
    pub(crate) fn matches(&self, h: &[u8]) -> bool {
        let ci = self.ci;
        match &self.kind {
            LikeKind::Exact(l) => lit_eq(h, l, ci),
            LikeKind::Prefix(l) => h.len() >= l.len() && lit_eq(&h[..l.len()], l, ci),
            LikeKind::Suffix(l) => h.len() >= l.len() && lit_eq(&h[h.len() - l.len()..], l, ci),
            LikeKind::Contains(l) => find(h, l, ci).is_some(),
            LikeKind::Generic(toks) => generic_match(toks, h, ci),
        }
    }
}

/// Byte compare — unequal lengths are unequal — ASCII-case-insensitively under
/// `ci`. The `Lit` bytes are already lowercased at compile, so only the haystack
/// side folds here.
///
/// `<[u8]>::eq` lowers to a `memcmp` **call** at every length, which the few-byte
/// literal compared once per candidate position cannot amortize. Those compare
/// inline; the call is left to the lengths that pay for it.
#[inline]
fn lit_eq(a: &[u8], b: &[u8], ci: bool) -> bool {
    if ci {
        return a.eq_ignore_ascii_case(b);
    }
    if a.len() != b.len() {
        return false;
    }
    if a.len() <= SHORT_LIT {
        return a.iter().zip(b).all(|(x, y)| x == y);
    }
    a == b
}

/// Literal length at or below which [`lit_eq`] compares byte by byte. Four,
/// measured: it takes 4 % off `expr_kernel_bench`'s three `str_generic_*` shapes,
/// where eight costs `str_like` 9 % — `memcmp` does eight bytes in one word.
const SHORT_LIT: usize = 4;

/// The byte offset of the first occurrence of `needle` in `h`, `Some(0)` for an
/// empty needle: a candidate scan on the needle's first byte, then a window
/// compare, ASCII-case-insensitively under `ci`. LIKE's `%x%` shape reads it as
/// a verdict; STRPOS, REPLACE and SPLIT_PART read the offset.
///
/// Candidate-start count at or below which [`find`] scans byte by byte. `memchr`'s
/// runtime dispatch and vector prologue are a fixed ~33 retired instructions, and
/// short is not a corner: an inline German string, and the haystack `fields`,
/// `REPLACE` and `SPLIT_PART` walk down toward zero.
const SHORT_HAYSTACK: usize = 16;

/// The byte offset of the first occurrence of `needle` in `h`, `Some(0)` for an
/// empty needle: a candidate scan on the needle's first byte, then a window
/// compare, ASCII-case-insensitively under `ci`. LIKE's `%x%` shape reads it as
/// a verdict; STRPOS, REPLACE and SPLIT_PART read the offset. `memchr` past
/// [`SHORT_HAYSTACK`] is worth 6.7× on a 128-byte haystack and an order of
/// magnitude on `Generic`'s repeated resumes.
pub(crate) fn find(h: &[u8], needle: &[u8], ci: bool) -> Option<usize> {
    let Some(&first) = needle.first() else {
        return Some(0);
    };
    // How many start positions fit; `None` when the needle is longer than `h`.
    let starts = h.len().checked_sub(needle.len() - 1)?;
    let hay = &h[..starts];
    // `Lit` bytes are lowercased at compile, so `first` is already the folded
    // form and only its uppercase twin needs scanning for. Under `!ci` the two
    // candidates coincide, which makes the two-byte scan behave as a one-byte
    // one — that is what keeps this a single loop with the case test hoisted out
    // of it.
    let alt = if ci { first.to_ascii_uppercase() } else { first };
    if hay.len() <= SHORT_HAYSTACK {
        for (i, &b) in hay.iter().enumerate() {
            if (b == first || b == alt) && lit_eq(&h[i..i + needle.len()], needle, ci) {
                return Some(i);
            }
        }
        return None;
    }
    let mut off = 0;
    while let Some(k) = memchr::memchr2(first, alt, &hay[off..]) {
        let i = off + k;
        if lit_eq(&h[i..i + needle.len()], needle, ci) {
            return Some(i);
        }
        off = i + 1;
    }
    None
}

/// The `(start, end)` byte span of each field of `s` split on the non-empty
/// `d`, left to right — one field, `s` whole, when `d` never occurs.
pub(crate) fn fields<'a>(s: &'a [u8], d: &'a [u8]) -> impl Iterator<Item = (usize, usize)> + 'a {
    debug_assert!(!d.is_empty(), "an empty delimiter splits nothing");
    let mut next = Some(0usize);
    std::iter::from_fn(move || {
        let start = next?;
        let end = match find(&s[start..], d, false) {
            Some(k) => start + k,
            None => s.len(),
        };
        next = (end < s.len()).then(|| end + d.len());
        Some((start, end))
    })
}

/// The single-anchor iterative glob: `ti` walks tokens, `hi` walks haystack
/// bytes, and `anchor` is the one resume point — the token and haystack position
/// to return to after the most recent `%`. Every failure retries *this*
/// position, never the position it failed at, which is what keeps each candidate
/// start reachable.
///
/// Terminates on any bytes: the anchor only ever slides right, bounding
/// backtracks at `h.len()`, and every other iteration advances `ti`.
fn generic_match(toks: &[LikeTok], h: &[u8], ci: bool) -> bool {
    let n = h.len();
    let (mut ti, mut hi) = (0usize, 0usize);
    let mut anchor: Option<(usize, usize)> = None;
    loop {
        if ti < toks.len() {
            match &toks[ti] {
                // A trailing `%` absorbs the rest: answer now rather than
                // sliding the anchor to the end one byte at a time.
                LikeTok::AnyMany if ti + 1 == toks.len() => return true,
                LikeTok::AnyMany => {
                    ti += 1;
                    anchor = Some((ti, hi));
                    continue;
                }
                LikeTok::AnyOne => {
                    if hi < n {
                        hi = char_offset(h, hi, 1);
                        ti += 1;
                        continue;
                    }
                }
                // The guard is what keeps the slice in range: a literal longer
                // than the remaining haystack would otherwise panic.
                LikeTok::Lit(l) => {
                    if hi + l.len() <= n && lit_eq(&h[hi..hi + l.len()], l, ci) {
                        hi += l.len();
                        ti += 1;
                        continue;
                    }
                }
            }
        } else if hi == n {
            return true;
        }
        // Every failure lands here: a `Lit` mismatch, an `AnyOne` with no
        // haystack left, or tokens exhausted with haystack remaining.
        match &mut anchor {
            // One byte, not one character: a boundary-aligned step would skip
            // resume positions the specializations accept on non-UTF-8 bytes.
            Some((ret_ti, ret_hi)) if *ret_hi < n => {
                *ret_hi += 1;
                // The anchor's token is what resume tries first, so if it is a
                // literal every position before its next occurrence fails on that
                // same literal, and no occurrence means no resume can succeed.
                // A `Lit` is never empty, so `find` cannot answer `Some(0)` and
                // the anchor still only moves right.
                if let Some(LikeTok::Lit(l)) = toks.get(*ret_ti) {
                    match find(&h[*ret_hi..], l, ci) {
                        Some(k) => *ret_hi += k,
                        None => return false,
                    }
                }
                (ti, hi) = (*ret_ti, *ret_hi);
            }
            _ => return false,
        }
    }
}

#[cfg(test)]
#[path = "tests/like.rs"]
mod tests;
