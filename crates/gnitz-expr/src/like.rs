//! SQL LIKE / ILIKE: the pattern encoding, and the matcher it compiles into.
//!
//! `%` matches any bytes and `_` one character, by the boundary [`crate::chars`]
//! defines for SUBSTRING. On valid UTF-8 the two granularities agree; on bytes
//! that are not, a byte-granular `%` is what keeps every specialization
//! answering as the `Generic` walk does.

use memchr::memmem::Finder;

use crate::chars::char_offset;
use crate::search::{find_lit, lit_eq};

pub(crate) const ANY_MANY: u8 = 0xFF;
pub(crate) const ANY_ONE: u8 = 0xFE;

/// A LIKE pattern as a program carries it: bytes where [`ANY_MANY`] is `%`,
/// [`ANY_ONE`] is `_`, and every other byte is a literal. Neither byte occurs in
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
    AnyOne,
    AnyMany,
}

/// Under `ci` each literal run is ASCII-lowercased.
fn tokenize(pattern: &[u8], ci: bool) -> Vec<LikeTok> {
    let mut toks = Vec::new();
    let mut rest = pattern;
    while let Some((&b, tail)) = rest.split_first() {
        match b {
            ANY_MANY => {
                if !matches!(toks.last(), Some(LikeTok::AnyMany)) {
                    toks.push(LikeTok::AnyMany);
                }
                rest = tail;
            }
            ANY_ONE => {
                toks.push(LikeTok::AnyOne);
                rest = tail;
            }
            _ => {
                let n = rest
                    .iter()
                    .position(|&b| matches!(b, ANY_MANY | ANY_ONE))
                    .unwrap_or(rest.len());
                let run = &rest[..n];
                let finder = if ci {
                    Finder::new(&run.to_ascii_lowercase()).into_owned()
                } else {
                    Finder::new(run).into_owned()
                };
                toks.push(LikeTok::Lit(finder));
                rest = &rest[n..];
            }
        }
    }
    toks
}

fn specialize(toks: Vec<LikeTok>) -> LikeKind {
    use LikeTok::{AnyMany, Lit};
    match toks.as_slice() {
        [] => LikeKind::Exact(Vec::new()),
        [Lit(l)] => LikeKind::Exact(l.needle().to_vec()),
        [Lit(l), AnyMany] => LikeKind::Prefix(l.needle().to_vec()),
        [AnyMany] => LikeKind::Prefix(Vec::new()),
        [AnyMany, Lit(l)] => LikeKind::Suffix(l.needle().to_vec()),
        [AnyMany, Lit(l), AnyMany] => LikeKind::Contains(l.clone()),
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

/// A glob walk with one backtrack point, `anchor`: the token after the latest
/// `%`, and the earliest offset it has not been tried at. A literal is tried only
/// where it occurs. `O(n·m)` in haystack bytes × pattern bytes.
fn generic_match(toks: &[LikeTok], h: &[u8]) -> bool {
    let n = h.len();
    let (mut ti, mut hi) = (0usize, 0usize);
    let mut anchor: Option<(usize, usize)> = None;
    loop {
        match toks.get(ti) {
            // A trailing `%` absorbs the rest.
            Some(LikeTok::AnyMany) if ti + 1 == toks.len() => return true,
            Some(LikeTok::AnyMany) => anchor = Some((ti + 1, hi)),
            Some(LikeTok::AnyOne) if hi < n => {
                hi = char_offset(h, hi, 1);
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
        let Some((ati, ahi)) = &mut anchor else { return false };
        (ti, hi) = match &toks[*ati] {
            LikeTok::Lit(l) => {
                let Some(k) = find_lit(&h[*ahi..], l) else { return false };
                let at = *ahi + k;
                *ahi = at + 1;
                (*ati + 1, at + l.needle().len())
            }
            _ if *ahi <= n => {
                let at = *ahi;
                *ahi += 1;
                (*ati, at)
            }
            _ => return false,
        };
    }
}

#[cfg(test)]
#[path = "tests/like.rs"]
mod tests;
