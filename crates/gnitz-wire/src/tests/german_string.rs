use super::*;
use std::cmp::Ordering;

/// Encode → decode at one length, out of an arena that already holds `prefix`
/// bytes: a heap payload must be found at its own non-zero offset, not at 0.
/// The two readers — the fallible `try_decode_german_string` and the infallible
/// `german_string_content` — must agree on every class.
fn roundtrip(s: &[u8], prefix: usize) {
    let mut blob = vec![0xEEu8; prefix];
    let st = encode_german_string(s, &mut blob);
    assert_eq!(
        try_decode_german_string(&st, &blob).as_deref(),
        Some(s),
        "try_decode roundtrip failed for len {} at prefix {prefix}",
        s.len(),
    );
    assert_eq!(
        german_string_content(&st, &blob),
        s,
        "the two readers disagree at len {} prefix {prefix}",
        s.len(),
    );
    if s.len() > SHORT_STRING_THRESHOLD {
        assert_eq!(
            read_u64_le(&st, 8) as usize,
            prefix,
            "a heap payload must sit past the arena's existing bytes"
        );
        assert_eq!(blob.len(), prefix + s.len(), "the payload is appended whole");
    } else {
        assert_eq!(blob.len(), prefix, "an inline payload must not touch the arena");
    }
}

#[test]
fn roundtrip_across_length_boundaries() {
    // Both a fresh arena and one with bytes already in it, so an offset the
    // encoder got wrong cannot coincide with 0.
    for prefix in [0usize, 5] {
        roundtrip(b"", prefix); // empty
        roundtrip(b"a", prefix); // 1 (prefix only)
        roundtrip(b"abcd", prefix); // 4 (prefix exactly full)
        roundtrip(b"abcde", prefix); // 5 (prefix + 1 suffix byte)
        roundtrip(b"abcdefghijkl", prefix); // 12 == SHORT_STRING_THRESHOLD (fully inline)
        roundtrip(b"abcdefghijklm", prefix); // 13 (first length that spills to blob)
        roundtrip(b"abcdefghijklmnopqrstuvwxyz", prefix); // 26
        roundtrip(&vec![0xABu8; 1000], prefix); // long blob string
    }
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
    crate::write_u32_le(&mut a, 0, 20);
    a[4..8].copy_from_slice(b"abcd");
    crate::write_u64_le(&mut a, 8, 500);
    let mut b = a;
    crate::write_u64_le(&mut b, 8, 900);
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

/// The merge heap and the cursor-vs-exemplar compare always hold **two** blob
/// arenas — one per run — so the comparator must resolve each side's payload out
/// of its own heap. The two arenas here start at different lengths, so equal
/// payloads land at different offsets and a comparator reading both sides from
/// one arena reports the wrong order.
#[test]
fn compare_resolves_each_side_from_its_own_blob() {
    let data: &[&[u8]] = &[
        b"",
        b"abcd",
        b"abcdefghijkl",               // 12 — last inline length
        b"abcdefghijklm",              // 13 — first heap length
        b"abcdefghijklmnopqrst",       // 20, shares the 12-byte prefix
        b"abcdefghijklmnopqrsu",       // 20, differs at the last byte
        b"zyxwvutsrqponmlkjihgfedcba", // 26, differs at byte 0
    ];
    let (mut blob_a, mut blob_b) = (vec![0x11u8; 3], vec![0x22u8; 41]);
    let cells_a: Vec<[u8; 16]> = data.iter().map(|d| encode_german_string(d, &mut blob_a)).collect();
    let cells_b: Vec<[u8; 16]> = data.iter().map(|d| encode_german_string(d, &mut blob_b)).collect();

    for (i, di) in data.iter().enumerate() {
        for (j, dj) in data.iter().enumerate() {
            assert_eq!(
                compare_german_strings(&cells_a[i], &blob_a, &cells_b[j], &blob_b),
                di.cmp(dj),
                "cross-arena compare mismatch for {di:?} vs {dj:?}",
            );
        }
    }
}
