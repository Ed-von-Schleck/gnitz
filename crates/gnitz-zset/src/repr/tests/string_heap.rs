use super::*;

/// A cache relocates each distinct span once.
#[test]
fn a_cached_relocation_copies_a_span_once() {
    let mut src_blob = Vec::new();
    let cell = gnitz_wire::encode_german_string(b"hello world test dat", &mut src_blob);
    let mut dst_blob = Vec::new();
    let mut cache = BlobCache::new(0);
    let first = relocated_cell(&cell, Heap::Plain(&src_blob), &mut dst_blob, Some(&mut cache));
    let second = relocated_cell(&cell, Heap::Plain(&src_blob), &mut dst_blob, Some(&mut cache));
    assert_eq!((dst_blob, first), (src_blob, second));
}

/// A cache takes a pooled map sized for its own cells, so a one-cell cache is
/// not handed the map a 50 000-span one returned.
#[test]
fn a_cache_takes_a_map_of_its_own_size() {
    const SPANS: usize = 50_000;
    let mut src_blob = Vec::new();
    let cells: Vec<[u8; 16]> = (0..SPANS)
        .map(|i| gnitz_wire::encode_german_string(format!("{i:020}").as_bytes(), &mut src_blob))
        .collect();
    let mut dst_blob = Vec::new();
    let mut large = BlobCache::new(SPANS);
    for cell in &cells {
        relocated_cell(cell, Heap::Plain(&src_blob), &mut dst_blob, Some(&mut large));
    }
    drop(large);
    let mut again = BlobCache::new(SPANS);
    assert!(again.map().capacity() >= SPANS, "precondition: the large map is pooled");
    drop(again);

    let mut small = BlobCache::new(1);
    relocated_cell(&cells[0], Heap::Plain(&src_blob), &mut dst_blob, Some(&mut small));
    assert!(small.map().capacity() <= 8, "{}", small.map().capacity());
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
