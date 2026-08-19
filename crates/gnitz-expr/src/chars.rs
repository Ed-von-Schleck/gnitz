//! Where a character begins, for the two kernels that need it: SUBSTRING's
//! window and LIKE's `_`.
//!
//! A leaf below both — it reads no register file, no scratch and no program, so
//! `like.rs` need not import out of the kernel file it is otherwise independent
//! of.

/// The byte index of every character start, which is the engine's one definition
/// of where a character begins: a byte whose top bits are not `10`. A
/// continuation byte belongs to the character it follows, so a *leading* one
/// belongs to no character at all. On valid UTF-8 these are exactly the codepoint
/// boundaries; on arbitrary bytes it stays total and panic-free, which is what a
/// byte-transparent engine needs.
fn char_starts(s: &[u8]) -> impl Iterator<Item = usize> + '_ {
    s.iter()
        .enumerate()
        .filter(|(_, &b)| (b & 0xC0) != 0x80)
        .map(|(k, _)| k)
}

/// Characters as the engine counts them — on valid UTF-8, the codepoint count.
pub(crate) fn char_count(s: &[u8]) -> usize {
    char_starts(s).count()
}

/// Byte offset of the `n`-th character start at or after `from`, or `s.len()`
/// when the string has fewer — the clamp SUBSTRING's window relies on. `[0x80]`
/// has no character starts, so every offset into it is `s.len()`.
///
/// Resuming from a known character start is what keeps a bounded window's cost
/// proportional to the window rather than to the string.
pub(crate) fn char_offset(s: &[u8], from: usize, n: usize) -> usize {
    char_starts(&s[from..]).nth(n).map_or(s.len(), |k| k + from)
}
