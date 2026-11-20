use super::*;
use crate::test_support::{make_batch_bytes, make_schema_pk_u64_payload_string, prose};
use gnitz_foundation::rng::mix;

fn prose_batch(range: std::ops::Range<usize>) -> Batch {
    make_batch_bytes(
        &make_schema_pk_u64_payload_string(),
        &range.map(|i| (i as u64, 1, prose(i))).collect::<Vec<_>>(),
    )
}

#[test]
fn too_few_long_bytes_train_no_table() {
    assert!(Codebook::for_batch(None, &prose_batch(0..20)).is_none());
}

/// A store keeps its codebook while a batch codes about as its training values
/// did, and trains another on one it codes far worse.
#[test]
fn a_codebook_is_kept_until_a_batch_drifts_from_it() {
    let book = Codebook::for_batch(None, &prose_batch(0..400)).unwrap();
    let kept = Codebook::for_batch(Some(&book), &prose_batch(5000..5400)).unwrap();
    assert!(Rc::ptr_eq(&book, &kept), "more of the same text");
    let hex: Vec<(u64, i64, String)> = (0..400u64).map(|i| (i, 1, format!("{:032x}", mix(i)))).collect();
    let drifted = Codebook::for_batch(
        Some(&book),
        &make_batch_bytes(&make_schema_pk_u64_payload_string(), &hex),
    )
    .unwrap();
    assert!(!Rc::ptr_eq(&book, &drifted));
    assert_ne!(drifted.table().digest(), book.table().digest());
}
