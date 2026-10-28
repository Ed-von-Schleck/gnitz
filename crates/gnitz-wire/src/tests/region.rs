use super::*;

#[test]
fn a_region_list_reads_back_what_was_pushed() {
    let (a, b) = ([1u8, 2, 3], [4u8; 8]);
    let mut regions = Regions::new();
    assert!(regions.is_empty());
    regions.push(&a);
    regions.push(&b);
    assert_eq!(&*regions, &[&a[..], &b[..]]);
}

#[test]
fn null_word_get_set_roundtrip() {
    let mut w = 0u64;
    assert!(!null_word_get(w, 3));
    null_word_set(&mut w, 3, true);
    assert!(null_word_get(w, 3));
    assert_eq!(w, 0b1000);
    // Clearing leaves the other bits untouched.
    null_word_set(&mut w, 5, true);
    null_word_set(&mut w, 3, false);
    assert!(!null_word_get(w, 3));
    assert!(null_word_get(w, 5));
    assert_eq!(w, 0b100000);

    assert_eq!(null_word_at(0b101, 2), 0b10100);
    assert_eq!(null_word_at(!0, 64), 0);
}

#[test]
fn first_not_null_violation_names_the_first_row_and_its_lowest_offending_slot() {
    let words: [u64; 4] = [0b0001, 0b0000, 0b1100, 0b0100];
    let bmp = crate::as_le_bytes(&words);
    assert_eq!(first_not_null_violation(0, bmp), None);
    assert_eq!(first_not_null_violation(0b0010, bmp), None);
    assert_eq!(first_not_null_violation(0b0110, bmp), Some((2, 2)));
    assert_eq!(first_not_null_violation(0b1001, bmp), Some((0, 0)));
    assert_eq!(first_not_null_violation(!0, &[]), None);
}

#[test]
fn first_valued_null_names_the_first_null_bit_over_a_non_zero_cell() {
    // Two 8-byte payload slots over three rows; only slot 1 is nullable.
    let cols: [[u64; 3]; 2] = [[7, 0, 9], [0, 5, 0]];
    let col = |slot: usize| (crate::as_le_bytes(&cols[slot]), 8);
    let bmp = |words: [u64; 3]| crate::as_le_bytes(&words).to_vec();
    assert_eq!(first_valued_null(0b10, &bmp([0b10, 0, 0b10]), col), None);
    assert_eq!(first_valued_null(0b10, &bmp([0b10, 0b10, 0b10]), col), Some((1, 1)));
    // A bit outside `nullable` is not this check's to judge.
    assert_eq!(first_valued_null(0b10, &bmp([0b01, 0, 0]), col), None);
}
