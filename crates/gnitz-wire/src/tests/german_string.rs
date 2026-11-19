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
    if s.is_empty() {
        assert_eq!(st, [0u8; 16], "the empty string is the zero cell");
    }
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

/// A shift adds `delta` to a long cell's offset and leaves every byte of a
/// short cell as it was, its pad bytes included, at every length around the
/// threshold.
#[test]
fn shift_touches_only_long_offsets() {
    let mut cells = Vec::new();
    for len in 0..=SHORT_STRING_THRESHOLD as u32 + 3 {
        let mut cell = [0xA5u8; 16];
        write_u32_le(&mut cell, 0, len);
        cells.extend_from_slice(&cell);
    }
    let before = cells.clone();
    shift_german_string_heaps(&mut cells, 3);
    for (a, b) in before.as_chunks::<16>().0.iter().zip(cells.as_chunks::<16>().0) {
        let mut want = *a;
        if read_u32_le(a, 0) as usize > SHORT_STRING_THRESHOLD {
            write_u64_le(&mut want, 8, read_u64_le(a, 8) + 3);
        }
        assert_eq!(*b, want, "length {}", read_u32_le(a, 0));
    }
}

/// A relocated cell is rebuilt in canonical form: a long cell overrunning its
/// heap becomes the empty string, its prefix zeroed with its length, and a
/// skewed short-cell pad does not survive — either left as it was would order
/// the cell apart from an equal-content one.
#[test]
fn relocation_rebuilds_each_cell_canonically() {
    let mut overrun = encode_german_string(&[0xAB; 20], &mut Vec::new());
    write_u64_le(&mut overrun, 8, 999);
    let mut cases = vec![(overrun, [0u8; 16])];
    for content in [&b""[..], b"a", b"abc", b"abcd", b"abcdefghijkl"] {
        let clean = encode_german_string(content, &mut Vec::new());
        let mut dirty = clean;
        dirty[4 + content.len()..].fill(0xFF);
        cases.push((dirty, clean));
    }
    let mut dst_blob = Vec::new();
    for (src, want) in cases {
        assert_eq!(
            relocate_german_string(&src, &[0u8; 4], &mut dst_blob),
            want,
            "{src:02x?}"
        );
    }
    assert!(dst_blob.is_empty());
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
    assert_eq!(
        compare_german_strings(&a, Heap::Plain(&blob), &b, Heap::Plain(&blob)),
        Ordering::Equal
    );
    // And the boundary predicate rejects both, so they never get this far
    // on a real ingest path.
    assert!(!german_string_cell_ok(&a, &blob));
    assert!(!german_string_cell_ok(&b, &blob));
}

/// `german_string_cell_ok` is "could `encode_german_string` have produced
/// this?". The rejections below are the ones the comparator's 4-byte prefix
/// fast path can observe but `german_string_content` cannot — a cell that
/// passes content-equality yet orders unequal would split one Z-set element's
/// weight across two rows that consolidation never merges.
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
        compare_german_strings(&clean, Heap::Plain(&blob), &dirty, Heap::Plain(&blob)),
        Ordering::Equal,
    );
    assert!(!german_string_cell_ok(&dirty, &blob));

    // Short cell with a dirty suffix pad past the content — same story.
    let clean_suffix = encode_german_string(b"abcde", &mut blob);
    let mut dirty_suffix = clean_suffix;
    dirty_suffix[15] = 0x01;
    assert!(!german_string_cell_ok(&dirty_suffix, &blob));
    // The tail word is compared under the content's mask, so this pad is not
    // compare-visible: the cells order as their contents do.
    assert_eq!(
        compare_german_strings(&clean_suffix, Heap::Plain(&blob), &dirty_suffix, Heap::Plain(&blob)),
        Ordering::Equal
    );

    // Long cell whose inline prefix disagrees with its heap payload.
    let mut skewed = encode_german_string(b"abcdefghijklmnopqrstuvwxyz", &mut blob);
    assert!(german_string_cell_ok(&skewed, &blob));
    skewed[4] = b'z';
    assert!(!german_string_cell_ok(&skewed, &blob));
}

/// `compare_german_strings` is `[u8]` lexicographic order across inline and
/// heap cells, each side resolved out of its own blob arena; the arenas start
/// at different lengths, so equal payloads sit at different offsets.
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
                    compare_german_strings(&x.0[i], Heap::Plain(&x.1), &y.0[j], Heap::Plain(&y.1)),
                    di.cmp(dj),
                    "compare mismatch for {di:?} vs {dj:?}",
                );
                // The lead decides only what the contents decide the same way.
                let lead =
                    german_string_lead(&x.0[i], Heap::Plain(&x.1)).cmp(&german_string_lead(&y.0[j], Heap::Plain(&y.1)));
                assert!(
                    lead == Ordering::Equal || lead == di.cmp(dj),
                    "lead of {di:?} vs {dj:?}"
                );
            }
        }
    }
}

/// A cell's long length counts a long cell's content
/// and nothing for a short one, and its lead is the content's first eight bytes
/// zero-padded, with a short cell's pad left dirty.
#[test]
fn long_len_and_lead_agree_with_the_encoder() {
    for len in 0..=40usize {
        let content: Vec<u8> = (0..len).map(|i| b'a' + i as u8).collect();
        let mut blob = vec![0x5Au8; 3];
        let cell = encode_german_string(&content, &mut blob);
        let mut lead = [0u8; 8];
        lead[..len.min(8)].copy_from_slice(&content[..len.min(8)]);
        if len <= SHORT_STRING_THRESHOLD {
            assert_eq!(german_string_long_len(&cell), 0, "short {len}");
            let mut dirty = cell;
            dirty[4 + len..].fill(0xFF);
            assert_eq!(
                german_string_lead(&dirty, Heap::Plain(&[])),
                u64::from_be_bytes(lead),
                "dirty {len}"
            );
        } else {
            assert_eq!(german_string_long_len(&cell), len, "long {len}");
        }
        assert_eq!(
            german_string_lead(&cell, Heap::Plain(&blob)),
            u64::from_be_bytes(lead),
            "{len}"
        );
    }
}
