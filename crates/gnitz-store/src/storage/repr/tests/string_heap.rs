use super::*;

/// A relocated cell is rebuilt in canonical form: a malformed long cell becomes
/// the empty string, and a skewed pad, which would split one element's weight
/// across two rows, does not survive.
#[test]
fn relocation_rebuilds_each_cell_canonically() {
    let mut malformed = gnitz_wire::encode_german_string(&[0xAB; 20], &mut Vec::new());
    gnitz_wire::write_u64_le(&mut malformed, 8, 999);
    let mut cases = vec![(malformed, gnitz_wire::encode_german_string(b"", &mut Vec::new()))];
    for content in [&b""[..], b"a", b"abc", b"abcd", b"abcdefghijkl"] {
        let clean = gnitz_wire::encode_german_string(content, &mut Vec::new());
        let mut dirty = clean;
        dirty[4 + content.len()..].fill(0xFF);
        cases.push((dirty, clean));
    }
    let mut dst_blob = Vec::new();
    for (src, want) in cases {
        assert_eq!(
            relocate_german_string_vec(&src, &[0; 4], &mut dst_blob, None),
            want,
            "{src:02x?}"
        );
    }
    assert!(dst_blob.is_empty());
}

/// A cache relocates each distinct span once.
#[test]
fn a_cached_relocation_copies_a_span_once() {
    let mut src_blob = Vec::new();
    let cell = gnitz_wire::encode_german_string(b"hello world test dat", &mut src_blob);
    let mut dst_blob = Vec::new();
    let mut cache = BlobCache::new(0);
    let first = relocate_german_string_vec(&cell, &src_blob, &mut dst_blob, Some(&mut cache));
    let second = relocate_german_string_vec(&cell, &src_blob, &mut dst_blob, Some(&mut cache));
    assert_eq!((dst_blob, first), (src_blob, second));
}

#[test]
fn prorated_blob_cap_is_the_rounded_up_share_within_the_heap() {
    assert_eq!(prorated_blob_cap(1000, 100, 10), 100);
    assert_eq!(
        prorated_blob_cap(10, 100, 5),
        5,
        "a sub-byte share rounds up to a byte per row"
    );
    assert_eq!(prorated_blob_cap(100, 10, 20), 100, "never more than the whole heap");
    assert_eq!(prorated_blob_cap(usize::MAX, 2, usize::MAX), usize::MAX, "no overflow");
    assert_eq!(prorated_blob_cap(0, 10, 5), 0);
    assert_eq!(prorated_blob_cap(10, 0, 5), 0);
}
