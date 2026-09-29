//! SQL LIKE / ILIKE: the pattern encoding, and the matcher it compiles into.
//!
//! `%` matches any characters and `_` one, as [`crate::chars`] defines them.

use memchr::memmem::Finder;

use crate::chars::char_offset;
use crate::search::{find_lit, lit_eq};

pub(crate) const ANY_MANY: u8 = 0xFF;
pub(crate) const ANY_ONE: u8 = 0xFE;

/// A LIKE pattern as a program carries it: bytes where `ANY_MANY` is `%`,
/// `ANY_ONE` is `_`, and every other byte is a literal. Neither byte occurs in
/// UTF-8 text, so the encoding needs no escape.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LikePattern(Vec<u8>);

impl LikePattern {
    /// `sql` under `escape` (`None` = escaping disabled); `None` when `sql`
    /// ends in a live escape.
    pub fn encode(sql: &str, escape: Option<char>) -> Option<Self> {
        let mut out = Vec::with_capacity(sql.len());
        let mut chars = sql.chars();
        while let Some(c) = chars.next() {
            let lit = match c {
                _ if Some(c) == escape => chars.next()?,
                '%' => {
                    out.push(ANY_MANY);
                    continue;
                }
                '_' => {
                    out.push(ANY_ONE);
                    continue;
                }
                _ => c,
            };
            out.extend_from_slice(lit.encode_utf8(&mut [0; 4]).as_bytes());
        }
        Some(LikePattern(out))
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// Whether `bytes` is a pattern [`Self::encode`] could produce: UTF-8 text
    /// between the wildcard bytes.
    pub(crate) fn is_encoding(bytes: &[u8]) -> bool {
        bytes.split(is_wildcard).all(|text| std::str::from_utf8(text).is_ok())
    }
}

fn is_wildcard(b: &u8) -> bool {
    matches!(*b, ANY_MANY | ANY_ONE)
}

pub(crate) struct LikeMatcher {
    kind: LikeKind,
    /// ILIKE. The literals are already lowercased.
    ci: bool,
}

// One per LIKE instruction, so the variants' size is immaterial.
#[allow(clippy::large_enum_variant)]
enum LikeKind {
    /// No wildcards: `'abc'`, and `''`.
    Exact(Vec<u8>),
    /// `'lit%'`, and `'%'` → `Prefix(b"")`.
    Prefix(Vec<u8>),
    /// `'%lit'`.
    Suffix(Vec<u8>),
    /// `'%lit%'`.
    Contains(Finder<'static>),
    Generic(Vec<LikeTok>),
}

// A handful per pattern, so the variants' size is immaterial.
#[allow(clippy::large_enum_variant)]
enum LikeTok {
    Lit(Finder<'static>),
    /// `_`.
    AnyOne,
    /// `%` and the literal after it.
    SkipTo(Finder<'static>),
    /// A trailing `%`.
    AnyRest,
}

/// A run of wildcards becomes its `_`s, then its `%` joined to the literal
/// after it. Under `ci` each literal is ASCII-lowercased.
fn tokenize(pattern: &[u8], ci: bool) -> Vec<LikeTok> {
    let finder = |lit: &[u8]| match ci {
        true => Finder::new(&lit.to_ascii_lowercase()).into_owned(),
        false => Finder::new(lit).into_owned(),
    };
    let mut toks = Vec::new();
    let mut rest = pattern;
    while !rest.is_empty() {
        let (wild, tail) = rest.split_at(rest.iter().take_while(|b| is_wildcard(b)).count());
        let (lit, tail) = tail.split_at(tail.iter().position(is_wildcard).unwrap_or(tail.len()));
        toks.extend(wild.iter().filter(|&&b| b == ANY_ONE).map(|_| LikeTok::AnyOne));
        match (wild.contains(&ANY_MANY), lit.is_empty()) {
            (true, true) => toks.push(LikeTok::AnyRest),
            (true, false) => toks.push(LikeTok::SkipTo(finder(lit))),
            (false, true) => {}
            (false, false) => toks.push(LikeTok::Lit(finder(lit))),
        }
        rest = tail;
    }
    toks
}

fn specialize(toks: Vec<LikeTok>) -> LikeKind {
    use LikeTok::{AnyRest, Lit, SkipTo};
    match toks.as_slice() {
        [] => LikeKind::Exact(Vec::new()),
        [Lit(l)] => LikeKind::Exact(l.needle().to_vec()),
        [Lit(l), AnyRest] => LikeKind::Prefix(l.needle().to_vec()),
        [AnyRest] => LikeKind::Prefix(Vec::new()),
        [SkipTo(l)] => LikeKind::Suffix(l.needle().to_vec()),
        [SkipTo(l), AnyRest] => LikeKind::Contains(l.clone()),
        _ => LikeKind::Generic(toks),
    }
}

impl LikeMatcher {
    /// Total: any bytes are a pattern.
    pub(crate) fn compile(pattern: &[u8], ci: bool) -> Self {
        LikeMatcher {
            kind: specialize(tokenize(pattern, ci)),
            ci,
        }
    }

    /// `folded` is scratch for ILIKE's lowercased haystack, reused across rows.
    ///
    /// `#[inline]`: `self.kind` is invariant in the caller's row loop, which
    /// lets LLVM unswitch this dispatch out of it.
    #[inline]
    pub(crate) fn matches(&self, h: &[u8], folded: &mut Vec<u8>) -> bool {
        let ci = self.ci;
        match &self.kind {
            LikeKind::Exact(l) => anchored_eq(h, l, ci),
            LikeKind::Prefix(l) => h.len() >= l.len() && anchored_eq(&h[..l.len()], l, ci),
            LikeKind::Suffix(l) => h.len() >= l.len() && anchored_eq(&h[h.len() - l.len()..], l, ci),
            LikeKind::Contains(f) => find_lit(fold(ci, h, folded), f).is_some(),
            LikeKind::Generic(toks) => generic_match(toks, fold(ci, h, folded)),
        }
    }
}

/// In place rather than folded: an anchored shape reads only the literal's
/// length of haystack.
#[inline]
fn anchored_eq(w: &[u8], l: &[u8], ci: bool) -> bool {
    if ci {
        w.eq_ignore_ascii_case(l)
    } else {
        lit_eq(w, l)
    }
}

/// `h`, or under ILIKE its ASCII-lowercased copy in `folded`.
#[inline]
fn fold<'a>(ci: bool, h: &'a [u8], folded: &'a mut Vec<u8>) -> &'a [u8] {
    if !ci {
        return h;
    }
    folded.resize(h.len(), 0);
    for (d, s) in folded.iter_mut().zip(h) {
        *d = s.to_ascii_lowercase();
    }
    folded
}

/// A glob walk with one backtrack point, `anchor`: the latest `SkipTo`'s
/// literal, the token after it, and the earliest offset the literal has not been
/// tried at. `O(n·m)` in haystack bytes × pattern bytes.
fn generic_match(toks: &[LikeTok], h: &[u8]) -> bool {
    let n = h.len();
    let (mut ti, mut hi) = (0usize, 0usize);
    let mut anchor: Option<(&Finder<'static>, usize, usize)> = None;
    loop {
        match toks.get(ti) {
            Some(LikeTok::AnyRest) => return true,
            Some(LikeTok::SkipTo(l)) => anchor = Some((l, ti + 1, hi)),
            Some(LikeTok::AnyOne) if hi < n => {
                hi += char_offset(&h[hi..], 1);
                ti += 1;
                continue;
            }
            Some(LikeTok::Lit(l)) => {
                let l = l.needle();
                if h.get(hi..hi + l.len()).is_some_and(|w| lit_eq(w, l)) {
                    hi += l.len();
                    ti += 1;
                    continue;
                }
            }
            Some(LikeTok::AnyOne) => {}
            None if hi == n => return true,
            None => {}
        }
        let Some((l, next, from)) = &mut anchor else {
            return false;
        };
        let Some(k) = find_lit(&h[*from..], l) else {
            return false;
        };
        let at = *from + k;
        *from = at + 1;
        (ti, hi) = (*next, at + l.needle().len());
    }
}

#[cfg(test)]
#[path = "tests/like.rs"]
mod tests;
