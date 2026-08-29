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
