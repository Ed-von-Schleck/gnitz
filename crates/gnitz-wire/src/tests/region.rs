use super::*;

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
