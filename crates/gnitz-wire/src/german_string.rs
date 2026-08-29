//! German string codec: 16-byte inline/blob string representation.

use crate::{read_u32_le, read_u64_le};

/// Threshold for inline German String storage (bytes).
pub const SHORT_STRING_THRESHOLD: usize = 12;

/// Encode a byte slice as a 16-byte German String struct destined for a heap
/// whose current end is `heap_off`, returning the cell and the bytes it spills
/// there — empty while the value fits inline. Returning the spill rather than
/// appending it lets a caller writing into a pre-sized slice place it directly.
///
/// Layout:
///   [0..4]  length (u32 LE)
///   [4..8]  prefix — first min(4, len) bytes, zero-padded
///   [8..16] if len ≤ 12: suffix bytes [4..len], zero-padded
///           if len > 12: blob arena offset (u64 LE)
pub(crate) fn encode_german_string_cell(s: &[u8], heap_off: usize) -> ([u8; 16], &[u8]) {
    let len = s.len();
    // The 4-byte length field caps a string at u32::MAX bytes. Silently
    // truncating `len as u32` would corrupt this persisted format, so reject
    // an over-long string outright (a release-build assert, not debug-only).
    assert!(
        len <= u32::MAX as usize,
        "encode_german_string: length {len} exceeds u32::MAX"
    );
    let mut st = [0u8; 16];
    st[0..4].copy_from_slice(&(len as u32).to_le_bytes());
    if len == 0 {
        return (st, &[]);
    }
    let pfx = len.min(4);
    st[4..4 + pfx].copy_from_slice(&s[..pfx]);
    if len <= SHORT_STRING_THRESHOLD {
        if len > 4 {
            st[8..8 + (len - 4)].copy_from_slice(&s[4..len]);
        }
        (st, &[])
    } else {
        st[8..16].copy_from_slice(&(heap_off as u64).to_le_bytes());
        (st, s)
    }
}

/// [`encode_german_string_cell`] against a growable arena: the spill, if any,
/// is appended to `blob`.
pub fn encode_german_string(s: &[u8], blob: &mut Vec<u8>) -> [u8; 16] {
    let (st, spill) = encode_german_string_cell(s, blob.len());
    blob.extend_from_slice(spill);
    st
}

/// The one blob-extent rule: `[heap_offset, heap_offset + length)` as a `usize`
/// range if it lies inside a heap of `blob_len` bytes, else `None`.
///
/// Resolved entirely in `u64` space — narrowing a 64-bit wire offset with
/// `as usize` first could truncate it back into range on a 32-bit target and
/// then point at the wrong bytes — and returned as a *range* rather than a
/// `bool` so the caller's `&blob[r]` is provably in bounds: no second bounds
/// check, no panic landing pad, no stack frame.
#[inline]
pub fn blob_extent(blob_len: usize, heap_offset: u64, length: usize) -> Option<std::ops::Range<usize>> {
    let end = heap_offset.checked_add(length as u64)?;
    if end > blob_len as u64 {
        return None;
    }
    Some(heap_offset as usize..end as usize)
}

/// `Some` iff `cell` is short — its inline content, borrowed from the cell.
/// The one place the length field's position, the short/long test and the
/// inline content base are spelled; every reader of the layout branches here.
///
/// The inline arm is one contiguous slice because the layout is contiguous:
/// `[4..8]` holds content bytes `[0..4)` and `[8..]` continues at content byte
/// 4, so `cell[4..4 + len]` is the whole value.
///
/// `#[inline(always)]`, not `#[inline]`: cross-crate inlining needs the
/// attribute, and a plain hint is not honoured at opt-level 0 — where
/// `german_string_content` is already a real call on gnitz-engine's per-row
/// comparator, so an unannotated callee would add a second call layer.
#[inline(always)]
pub fn german_string_inline(cell: &[u8]) -> Option<&[u8]> {
    let length = read_u32_le(cell, 0) as usize;
    (length <= SHORT_STRING_THRESHOLD).then(|| &cell[4..4 + length])
}

/// A **long** cell's clamped heap range, or `None` for a corrupt header. Only
/// meaningful once [`german_string_inline`] has answered `None`.
#[inline(always)]
pub fn german_string_heap(cell: &[u8], blob_len: usize) -> Option<std::ops::Range<usize>> {
    blob_extent(blob_len, read_u64_le(cell, 8), read_u32_le(cell, 0) as usize)
}

/// Decode a 16-byte German String struct into raw bytes, or `None` if a
/// long string's blob offset/length overruns `blob`. The owned, fallible
/// counterpart of [`german_string_content`] — for the trust boundaries that
/// must reject a corrupt cell rather than degrade it to empty.
pub fn try_decode_german_string(st: &[u8], blob: &[u8]) -> Option<Vec<u8>> {
    match german_string_inline(st) {
        Some(inline) => Some(inline.to_vec()),
        None => Some(blob[german_string_heap(st, blob.len())?].to_vec()),
    }
}

/// True iff `cell` is in **canonical form** against `blob` — i.e. a cell
/// `encode_german_string` could have produced. This is the one predicate every
/// German-string trust boundary asks, and it is stronger than a bare extent
/// check because the comparator's correctness depends on both halves:
///
/// - **Short** (`len ≤ SHORT_STRING_THRESHOLD`): bytes `[4 + len, 16)` must be
///   zero. `compare_german_strings` reads `[4..8]` as one `u32`, so for
///   `len < 4` a non-zero pad byte orders two cells with identical content
///   unequal while [`german_string_content`] hashes them equal — the same Z-set
///   element would then route to one worker and refuse to consolidate.
/// - **Long**: the heap extent must fit, *and* the inline prefix `[4..8)` must
///   match the first four heap bytes, for the same reason.
pub fn german_string_cell_ok(cell: &[u8], blob: &[u8]) -> bool {
    if german_string_inline(cell).is_some() {
        // Asked as "is this already what the canonicalizer would produce" —
        // which is what the short arm means. Two masked word compares instead of
        // a byte-at-a-time pad scan LLVM cannot vectorize (the start index is
        // runtime and the loop exits early), on the per-row client-push ingest
        // trust boundary.
        let c: &[u8; 16] = cell[..16].try_into().unwrap();
        return *c == canonical_short_cell(c);
    }
    // `length > 4`, so the resolved range always holds the four prefix bytes.
    match german_string_heap(cell, blob.len()) {
        Some(r) => cell[4..8] == blob[r.start..r.start + 4],
        None => false,
    }
}

/// Rebuild a **short** German-string cell in canonical form: its `length`
/// content bytes verbatim, every pad byte zero. The constructive inverse of
/// [`german_string_cell_ok`]'s short arm — whatever pad garbage `src` carried,
/// the result is a cell that predicate accepts and that orders by content.
///
/// `src`'s length field must be ≤ `SHORT_STRING_THRESHOLD`; a long cell owns
/// `[8..16)` as a heap offset and belongs to the relocator instead.
///
/// Branchless by construction: a runtime-length `copy_from_slice` would lower to
/// a `memcpy` PLT call for a ≤ 8-byte move, ~5× the cost on the per-row merge /
/// scatter / map-projection loop.
#[inline]
pub fn canonical_short_cell(src: &[u8; 16]) -> [u8; 16] {
    let length = read_u32_le(src, 0) as usize;
    debug_assert!(length <= SHORT_STRING_THRESHOLD, "canonical_short_cell on a long cell");
    // `[4..8)` holds content bytes `[0, min(length, 4))`, `[8..16)` continues at
    // content byte 4 — so in each little-endian word the *low* `keep` bytes are
    // content and the rest is pad.
    let keep_low = |word: u64, keep: usize| -> u64 {
        // The `u128` shift is what makes `keep == 8` legal (`1u64 << 64` is UB).
        word & (((1u128 << (keep * 8)) - 1) as u64)
    };
    let mut dest = [0u8; 16];
    dest[0..4].copy_from_slice(&src[0..4]);
    let pfx = u32::from_le_bytes(src[4..8].try_into().unwrap()) as u64;
    dest[4..8].copy_from_slice(&(keep_low(pfx, length.min(4)) as u32).to_le_bytes());
    let tail = u64::from_le_bytes(src[8..16].try_into().unwrap());
    dest[8..16].copy_from_slice(&keep_low(tail, length.saturating_sub(4)).to_le_bytes());
    dest
}

/// Full logical content bytes of a German string struct `s`, degrading a
/// corrupt long header to `&[]`.
///
/// The one **degrading** content accessor: ordering, hashing and the evaluator's
/// cell decode all read through it, so an overrun means empty everywhere instead
/// of each path inventing its own rule. The layout itself has other readers —
/// [`german_string_cell_ok`]'s extent check, [`canonical_short_cell`], and the
/// compaction relocator — which is why the *layout* lives in
/// [`german_string_inline`] / [`german_string_heap`] rather than here.
#[inline]
pub fn german_string_content<'a>(s: &'a [u8], blob: &'a [u8]) -> &'a [u8] {
    match german_string_inline(s) {
        Some(inline) => inline,
        None => match german_string_heap(s, blob.len()) {
            Some(r) => &blob[r],
            None => &[],
        },
    }
}

/// Byte-lexicographic order over two German string cells — the order
/// `compare_rows`, every merge heap and every compaction sorts by.
#[inline(always)]
pub fn compare_german_strings(a: &[u8], blob_a: &[u8], b: &[u8], blob_b: &[u8]) -> std::cmp::Ordering {
    // Fixed 4-byte prefix comparison — one register compare that resolves the
    // overwhelming majority of pairs without touching the blob heap. Valid
    // because a canonical cell zero-pads the prefix bytes beyond its length
    // (`encode_german_string` starts from a zeroed struct, and
    // `german_string_cell_ok` rejects anything else at the trust boundary): the
    // first differing padded byte is either a real content difference or a
    // longer string's non-zero byte against the shorter's zero pad, and both
    // order exactly as the full compare below would.
    let pfx_a = u32::from_be_bytes(a[4..8].try_into().unwrap());
    let pfx_b = u32::from_be_bytes(b[4..8].try_into().unwrap());
    if pfx_a != pfx_b {
        return pfx_a.cmp(&pfx_b);
    }
    // Vectorised memcmp with the length tiebreak `[u8]::cmp` already applies.
    german_string_content(a, blob_a).cmp(german_string_content(b, blob_b))
}

#[cfg(test)]
#[path = "tests/german_string.rs"]
mod tests;
