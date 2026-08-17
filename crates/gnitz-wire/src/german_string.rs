//! German string codec: 16-byte inline/blob string representation.

use crate::{read_u32_le, read_u64_le};

/// Threshold for inline German String storage (bytes).
pub const SHORT_STRING_THRESHOLD: usize = 12;

/// Encode a byte slice as a 16-byte German String struct, appending overflow
/// data to `blob`.
///
/// Layout:
///   [0..4]  length (u32 LE)
///   [4..8]  prefix — first min(4, len) bytes, zero-padded
///   [8..16] if len ≤ 12: suffix bytes [4..len], zero-padded
///           if len > 12: blob arena offset (u64 LE)
pub fn encode_german_string(s: &[u8], blob: &mut Vec<u8>) -> [u8; 16] {
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
        return st;
    }
    let pfx = len.min(4);
    st[4..4 + pfx].copy_from_slice(&s[..pfx]);
    if len <= SHORT_STRING_THRESHOLD {
        if len > 4 {
            st[8..8 + (len - 4)].copy_from_slice(&s[4..len]);
        }
    } else {
        let off = blob.len();
        blob.extend_from_slice(s);
        st[8..16].copy_from_slice(&(off as u64).to_le_bytes());
    }
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

/// Decode a 16-byte German String struct into raw bytes, or `None` if a
/// long string's blob offset/length overruns `blob`. The owned, fallible
/// counterpart of [`german_string_content`] — for the trust boundaries that
/// must reject a corrupt cell rather than degrade it to empty.
///
/// The short arm is one contiguous slice because the layout is contiguous:
/// `[4..8]` holds content bytes `[0..4)` and `[8..]` continues at content byte
/// 4, so `st[4..4 + len]` is the whole value.
pub fn try_decode_german_string(st: &[u8], blob: &[u8]) -> Option<Vec<u8>> {
    let length = read_u32_le(st, 0) as usize;
    if length <= SHORT_STRING_THRESHOLD {
        return Some(st[4..4 + length].to_vec());
    }
    Some(blob[blob_extent(blob.len(), read_u64_le(st, 8), length)?].to_vec())
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
    let length = read_u32_le(cell, 0) as usize;
    if length <= SHORT_STRING_THRESHOLD {
        // Asked as "is this already what the canonicalizer would produce" —
        // which is what the short arm means. Two masked word compares instead of
        // a byte-at-a-time pad scan LLVM cannot vectorize (the start index is
        // runtime and the loop exits early), on the per-row client-push ingest
        // trust boundary.
        let c: &[u8; 16] = cell[..16].try_into().unwrap();
        return *c == canonical_short_cell(c);
    }
    // `length > 4`, so the resolved range always holds the four prefix bytes.
    match blob_extent(blob.len(), read_u64_le(cell, 8), length) {
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

/// Full logical content bytes of a German string struct `s` (16-byte layout:
/// `[0..4]` = length, then inline-or-heap content). Short strings
/// (len ≤ SHORT_STRING_THRESHOLD) store content inline at `[4..4 + length]`;
/// long strings live in `blob` at the heap offset.
///
/// This is the **single** content accessor: ordering, hashing and relocation
/// all read a cell through it, so a corrupt long header degrades to `&[]` the
/// same way everywhere instead of each path inventing its own overrun rule.
#[inline]
pub fn german_string_content<'a>(s: &'a [u8], blob: &'a [u8]) -> &'a [u8] {
    let length = read_u32_le(s, 0) as usize;
    if length <= SHORT_STRING_THRESHOLD {
        return &s[4..4 + length];
    }
    match blob_extent(blob.len(), read_u64_le(s, 8), length) {
        Some(r) => &blob[r],
        None => &[],
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
mod german_string_tests {
    use super::*;
    use std::cmp::Ordering;

    fn roundtrip(s: &[u8]) {
        let mut blob = Vec::new();
        let st = encode_german_string(s, &mut blob);
        assert_eq!(
            try_decode_german_string(&st, &blob).as_deref(),
            Some(s),
            "try_decode roundtrip failed for len {}",
            s.len(),
        );
    }

    #[test]
    fn roundtrip_across_length_boundaries() {
        roundtrip(b""); // empty
        roundtrip(b"a"); // 1 (prefix only)
        roundtrip(b"abcd"); // 4 (prefix exactly full)
        roundtrip(b"abcde"); // 5 (prefix + 1 suffix byte)
        roundtrip(b"abcdefghijkl"); // 12 == SHORT_STRING_THRESHOLD (fully inline)
        roundtrip(b"abcdefghijklm"); // 13 (first length that spills to blob)
        roundtrip(&vec![0xABu8; 1000]); // long blob string
    }

    #[test]
    fn roundtrip_long_string_after_prefix_in_blob() {
        // A non-empty blob prefix exercises the offset (not just offset 0).
        let mut blob = vec![0u8; 7];
        let payload = vec![0x5Au8; 40];
        let st = encode_german_string(&payload, &mut blob);
        assert_eq!(try_decode_german_string(&st, &blob).as_deref(), Some(&payload[..]));
    }

    #[test]
    fn try_decode_rejects_offset_plus_len_past_blob() {
        // len > threshold so the decoder reads from the blob; offset 0 but the
        // blob is shorter than len.
        let mut st = [0u8; 16];
        st[0..4].copy_from_slice(&100u32.to_le_bytes());
        st[8..16].copy_from_slice(&0u64.to_le_bytes());
        let blob = vec![0u8; 50];
        assert_eq!(try_decode_german_string(&st, &blob), None);
    }

    #[test]
    fn try_decode_rejects_offset_at_blob_end() {
        let mut st = [0u8; 16];
        st[0..4].copy_from_slice(&13u32.to_le_bytes());
        st[8..16].copy_from_slice(&50u64.to_le_bytes()); // offset == blob.len()
        let blob = vec![0u8; 50];
        assert_eq!(try_decode_german_string(&st, &blob), None);
    }

    #[test]
    fn try_decode_rejects_offset_overflow() {
        // offset + len overflows u64; checked_add must catch it rather than
        // wrapping to a small in-bounds value.
        let mut st = [0u8; 16];
        st[0..4].copy_from_slice(&20u32.to_le_bytes());
        st[8..16].copy_from_slice(&(u64::MAX - 5).to_le_bytes());
        let blob = vec![0u8; 50];
        assert_eq!(try_decode_german_string(&st, &blob), None);
    }

    #[test]
    fn try_decode_long_string_exact_fit() {
        let payload = vec![0x33u8; 30];
        let mut blob = Vec::new();
        let st = encode_german_string(&payload, &mut blob);
        assert_eq!(blob.len(), 30, "exact-fit precondition");
        assert_eq!(try_decode_german_string(&st, &blob).as_deref(), Some(&payload[..]));
    }

    #[test]
    fn blob_extent_truth_table() {
        // In-bounds window resolves to the exact range.
        assert_eq!(blob_extent(8, 2, 3), Some(2..5));
        // Exact fit is in bounds; a zero-length extent at the end is too.
        assert_eq!(blob_extent(8, 0, 8), Some(0..8));
        assert_eq!(blob_extent(8, 8, 0), Some(8..8));
        // offset + length past the end, and offset itself past the end.
        assert_eq!(blob_extent(8, 6, 10), None);
        assert_eq!(blob_extent(8, 99, 1), None);
        // A corrupt length or offset near the top of the range must not wrap
        // back in — this is why the sum is checked, and checked in u64 space.
        assert_eq!(blob_extent(8, 4, usize::MAX), None);
        assert_eq!(blob_extent(8, u64::MAX - 5, 20), None);
    }

    #[test]
    fn compare_corrupt_long_cell_degrades_to_empty_without_panic() {
        // Two long-string cells with out-of-range heap offsets. Both contents
        // degrade to empty, so the comparison resolves rather than slicing OOB.
        let mut a = [0u8; 16];
        a[0..4].copy_from_slice(&20u32.to_le_bytes());
        a[4..8].copy_from_slice(b"abcd");
        a[8..16].copy_from_slice(&500u64.to_le_bytes());
        let mut b = a;
        b[8..16].copy_from_slice(&900u64.to_le_bytes());
        let blob = vec![0u8; 4];
        assert_eq!(german_string_content(&a, &blob), &[] as &[u8]);
        assert_eq!(compare_german_strings(&a, &blob, &b, &blob), Ordering::Equal);
        // And the boundary predicate rejects both, so they never get this far
        // on a real ingest path.
        assert!(!german_string_cell_ok(&a, &blob));
        assert!(!german_string_cell_ok(&b, &blob));
    }

    /// `german_string_cell_ok` is "could `encode_german_string` have produced
    /// this?". The two rejection cases are the ones the comparator's 4-byte
    /// prefix fast path can observe but `german_string_content` cannot — a cell
    /// that passes content-equality yet orders unequal would split one Z-set
    /// element's weight across two rows that consolidation never merges.
    #[test]
    fn cell_ok_accepts_canonical_and_rejects_compare_visible_corruption() {
        let mut blob = vec![0x7Fu8; 3];
        for d in [
            &b""[..],
            b"a",
            b"abcd",
            b"abcde",
            b"abcdefghijkl",
            b"abcdefghijklmnopqrstuvwxyz",
        ] {
            let cell = encode_german_string(d, &mut blob);
            assert!(german_string_cell_ok(&cell, &blob), "canonical cell rejected: {d:?}");
        }

        // Short cell with a dirty prefix pad: identical content, different order.
        let clean = encode_german_string(b"ab", &mut blob);
        let mut dirty = clean;
        dirty[6..8].copy_from_slice(&[0xFF, 0xFF]);
        assert_eq!(
            german_string_content(&clean, &blob),
            german_string_content(&dirty, &blob)
        );
        assert_ne!(
            compare_german_strings(&clean, &blob, &dirty, &blob),
            Ordering::Equal,
            "the pad is compare-visible — that is exactly what cell_ok must reject"
        );
        assert!(german_string_cell_ok(&clean, &blob));
        assert!(!german_string_cell_ok(&dirty, &blob));

        // Short cell with a dirty suffix pad past the content — same story.
        let mut dirty_suffix = encode_german_string(b"abcde", &mut blob);
        dirty_suffix[15] = 0x01;
        assert!(!german_string_cell_ok(&dirty_suffix, &blob));

        // Long cell whose inline prefix disagrees with its heap payload.
        let mut skewed = encode_german_string(b"abcdefghijklmnopqrstuvwxyz", &mut blob);
        assert!(german_string_cell_ok(&skewed, &blob));
        skewed[4] = b'z';
        assert!(!german_string_cell_ok(&skewed, &blob));
    }

    #[test]
    fn german_string_content_short_long_and_empty() {
        let mut blob = vec![0xEEu8; 5]; // non-zero heap prefix, so offsets are non-zero
        let empty = encode_german_string(b"", &mut blob);
        assert_eq!(german_string_content(&empty, &blob), &[] as &[u8]);

        let short = encode_german_string(b"abcdefghijkl", &mut blob); // 12 == threshold
        assert_eq!(german_string_content(&short, &blob), b"abcdefghijkl");

        let long_data = b"abcdefghijklmnopqrstuvwxyz";
        let long = encode_german_string(long_data, &mut blob);
        assert_eq!(read_u64_le(&long, 8), 5, "long payload must sit at a non-zero offset");
        assert_eq!(german_string_content(&long, &blob), long_data);
    }

    /// `compare_german_strings` is plain `[u8]` lexicographic order. The inline
    /// and heap cells reach that verdict through different bytes — the prefix
    /// u32 fast path, then a content compare that resolves out of the cell or
    /// out of the blob — so the classes must agree pairwise. One shared blob
    /// gives every long payload its own non-zero offset, which is what pins the
    /// content origin; a same-class comparison shifts both operands equally and
    /// cannot see it.
    #[test]
    fn compare_matches_byte_order_across_length_classes() {
        let data: &[&[u8]] = &[
            b"",
            b"a",
            b"ab",
            b"ab\0", // NUL past min_len aliases "ab"'s prefix pad
            b"abc",
            b"abcd",
            b"abcde",
            b"abce",                       // 4, prefix differs in the last byte
            b"abcdefghijkl",               // 12 — last inline length
            b"abcdefghijklm",              // 13 — first heap length
            b"abcdefghijklmnopqrst",       // 20, shares the 12-byte prefix above
            b"abcdefghijklmnopqrsu",       // 20, differs from the above at the last byte
            b"abcdefghijklmnopqrst",       // repeat: two heap offsets, one value ⇒ Equal
            b"abcd\0fghijklmnopqrst",      // 20, embedded NUL right past the prefix
            b"zyxwvutsrqponmlkjihgfedcba", // 26, prefix differs at byte 0
        ];
        let mut blob = vec![0x7Fu8; 3];
        let cells: Vec<[u8; 16]> = data.iter().map(|d| encode_german_string(d, &mut blob)).collect();

        for (i, di) in data.iter().enumerate() {
            for (j, dj) in data.iter().enumerate() {
                assert_eq!(
                    compare_german_strings(&cells[i], &blob, &cells[j], &blob),
                    di.cmp(dj),
                    "compare mismatch for {di:?} vs {dj:?}",
                );
            }
        }
    }
}
