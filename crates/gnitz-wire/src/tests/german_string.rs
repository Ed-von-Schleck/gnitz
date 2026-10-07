use super::*;
use std::cmp::Ordering;

/// Encode → decode at one length, out of an arena that already holds `prefix`
/// bytes: a heap payload must be found at its own non-zero offset, not at 0.
/// The encoded cell is canonical; the short-cell accessors classify it; and
/// shifting it by the bytes an appender put in front of its arena keeps its
/// content.
fn roundtrip(s: &[u8], prefix: usize) {
    let mut blob = vec![0xEEu8; prefix];
    let st = encode_german_string(s, &mut blob);
    assert_eq!(
        german_string_content(&st, &blob),
        s,
        "roundtrip failed for len {} at prefix {prefix}",
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
    assert!(german_string_cell_ok(&st, &blob), "canonical cell rejected: {s:?}");
    let short = s.len() <= SHORT_STRING_THRESHOLD;
    assert_eq!(canonical_short_cell(&st), short.then_some(st));
    assert_eq!(german_string_short_ascii(&st), short && s.is_ascii());
    let mut shifted = st;
    shift_german_string_heaps(&mut shifted, 7);
    let grown = [&[0u8; 7][..], &blob].concat();
    assert_eq!(german_string_content(&shifted, &grown), s, "shifted by 7");
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
        roundtrip(b"\xc3\xa9", prefix); // short, not ASCII
        roundtrip(b"abcdefghijkl", prefix); // 12 == SHORT_STRING_THRESHOLD (fully inline)
        roundtrip(b"abcdefghijklm", prefix); // 13 (first length that spills to blob)
        roundtrip(b"abcdefghijklmnopqrstuvwxyz", prefix); // 26
        roundtrip(&vec![0xABu8; 1000], prefix); // long blob string
    }
}

/// A long cell overrunning its heap relocates to the canonical empty string,
/// its prefix zeroed with its length: one left holding the corrupt cell's bytes
/// would order apart from an empty string that reads the same.
#[test]
fn relocate_overrunning_long_cell_becomes_clean_empty() {
    let mut src_cell = [0u8; 16];
    write_u32_le(&mut src_cell, 0, 20);
    src_cell[4..8].copy_from_slice(&0xDEAD_BEEF_u32.to_le_bytes());
    write_u64_le(&mut src_cell, 8, 999);

    let mut dst_blob: Vec<u8> = Vec::new();
    let result = relocate_german_string(&src_cell, &[0u8; 4], &mut dst_blob);
    assert_eq!(result, [0u8; 16]);
    assert!(dst_blob.is_empty());
    assert!(german_string_cell_ok(&result, &dst_blob));
}

/// A relocated short cell is rebuilt, so a skewed pad — which orders the cell
/// apart from an equal-content one — does not survive.
#[test]
fn relocate_canonicalizes_pad_bytes() {
    let mut dst_blob: Vec<u8> = Vec::new();
    for content in [&b""[..], b"a", b"abc", b"abcd", b"abcdefghijkl"] {
        let clean = encode_german_string(content, &mut Vec::new());
        let mut dirty = clean;
        dirty[4 + content.len()..].fill(0xFF);
        assert_eq!(relocate_german_string(&dirty, &[], &mut dst_blob), clean, "{content:?}");
    }
    assert!(dst_blob.is_empty(), "inline cells must not touch the blob");
}

/// A long cell's content lands wherever `place` puts it, its length and prefix
/// carried over; the appending form puts it at the destination's end.
#[test]
fn relocate_moves_only_a_long_cells_offset() {
    let content = b"abcdefghijklmnopqrst";
    let mut src_blob = vec![0x11u8; 9];
    let src = encode_german_string(content, &mut src_blob);
    let mut dst_blob = vec![0x22u8; 5];
    let out = relocate_german_string(&src, &src_blob, &mut dst_blob);
    assert_eq!(out[..8], src[..8], "length and prefix carry over");
    assert_eq!(read_u64_le(&out, 8), 5);
    assert_eq!(german_string_content(&out, &dst_blob), content);
    assert!(german_string_cell_ok(&out, &dst_blob));

    let placed = relocate_german_string_with(&src, &src_blob, |c| {
        assert_eq!(c, content);
        77
    });
    assert_eq!((&placed[..8], read_u64_le(&placed, 8)), (&src[..8], 77));
}

/// The region check is the cell check over every cell.
#[test]
fn region_ok_refuses_one_bad_cell() {
    let mut blob = Vec::new();
    let mut region = [
        encode_german_string(b"abc", &mut blob),
        encode_german_string(b"abcdefghijklmnopqrst", &mut blob),
    ];
    assert!(german_string_region_ok(region.as_flattened(), &blob));
    region[0][15] = 1;
    assert!(!german_string_region_ok(region.as_flattened(), &blob));
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
    let mut a = encode_german_string(&[b'a'; 20], &mut Vec::new());
    write_u64_le(&mut a, 8, 500);
    let mut b = a;
    write_u64_le(&mut b, 8, 900);
    let blob = vec![0u8; 4];
    assert_eq!(german_string_content(&a, &blob), &[] as &[u8]);
    assert_eq!(compare_german_strings(&a, &blob, &b, &blob), Ordering::Equal);
    // And the boundary predicate rejects both, so they never get this far
    // on a real ingest path.
    assert!(!german_string_cell_ok(&a, &blob));
    assert!(!german_string_cell_ok(&b, &blob));
}

/// `german_string_cell_ok` is "could `encode_german_string` have produced
/// this?" — the round-trip above covers the acceptances. The two rejection cases are the ones the comparator's 4-byte
/// prefix fast path can observe but `german_string_content` cannot — a cell
/// that passes content-equality yet orders unequal would split one Z-set
/// element's weight across two rows that consolidation never merges.
#[test]
fn cell_ok_accepts_canonical_and_rejects_compare_visible_corruption() {
    let mut blob = vec![0x7Fu8; 3];
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
    let clean_suffix = encode_german_string(b"abcde", &mut blob);
    let mut dirty_suffix = clean_suffix;
    dirty_suffix[15] = 0x01;
    assert!(!german_string_cell_ok(&dirty_suffix, &blob));
    // The tail word is compared under the content's mask, so this pad is not
    // compare-visible: the cells order as their contents do.
    assert_eq!(
        compare_german_strings(&clean_suffix, &blob, &dirty_suffix, &blob),
        Ordering::Equal
    );

    // Long cell whose inline prefix disagrees with its heap payload.
    let mut skewed = encode_german_string(b"abcdefghijklmnopqrstuvwxyz", &mut blob);
    assert!(german_string_cell_ok(&skewed, &blob));
    skewed[4] = b'z';
    assert!(!german_string_cell_ok(&skewed, &blob));
}

/// `compare_german_strings` is plain `[u8]` lexicographic order. The inline
/// and heap cells reach that verdict through different bytes — the prefix u32
/// fast path, then a content compare that resolves out of the cell or out of
/// its blob — so the classes must agree pairwise. The merge heap and the
/// cursor-vs-exemplar compare always hold **two** blob arenas, one per run, so
/// each side's payload must resolve out of its own heap: the arenas here start
/// at different lengths, so equal payloads land at different offsets, and a
/// comparator reading both sides from one arena, or at offset 0, reports the
/// wrong order.
#[test]
fn compare_matches_byte_order_across_classes_and_arenas() {
    let data: &[&[u8]] = &[
        b"",
        b"a",
        b"ab",
        b"ab\0", // NUL past min_len aliases "ab"'s prefix pad
        b"abc",
        b"abcd",
        b"abcd\0", // 5, a zero tail byte against the other side's pad
        b"abcde",
        b"abce",                       // 4, prefix differs in the last byte
        b"abcdefghijk",                // 11, the 12 below differing only in length
        b"abcdefghijkl",               // 12 — last inline length
        b"abcdefghijklm",              // 13 — first heap length
        b"abcdefghijklmnopqrst",       // 20, shares the 12-byte prefix above
        b"abcdefghijklmnopqrsu",       // 20, differs from the above at the last byte
        b"abcdefghijklmnopqrst",       // repeat: two heap offsets, one value ⇒ Equal
        b"abcd\0fghijklmnopqrst",      // 20, embedded NUL right past the prefix
        b"zyxwvutsrqponmlkjihgfedcba", // 26, prefix differs at byte 0
    ];
    let arena = |prefix: usize| {
        let mut blob = vec![0x7Fu8; prefix];
        let cells: Vec<[u8; 16]> = data.iter().map(|d| encode_german_string(d, &mut blob)).collect();
        (cells, blob)
    };
    let (a, b) = (arena(3), arena(41));
    for (x, y) in [(&a, &a), (&a, &b), (&b, &a)] {
        for (i, di) in data.iter().enumerate() {
            for (j, dj) in data.iter().enumerate() {
                assert_eq!(
                    compare_german_strings(&x.0[i], &x.1, &y.0[j], &y.1),
                    di.cmp(dj),
                    "compare mismatch for {di:?} vs {dj:?}",
                );
                // The lead decides only what the contents decide the same way.
                let lead = german_string_lead(&x.0[i], &x.1).cmp(&german_string_lead(&y.0[j], &y.1));
                assert!(
                    lead == Ordering::Equal || lead == di.cmp(dj),
                    "lead of {di:?} vs {dj:?}"
                );
            }
        }
    }
}

#[test]
fn the_empty_string_is_the_zero_cell() {
    let mut blob = Vec::new();
    assert_eq!(encode_german_string(&[], &mut blob), [0u8; 16]);
    assert!(blob.is_empty());
}
