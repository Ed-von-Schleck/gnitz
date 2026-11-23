use super::*;
use crate::test_support::{BASE64, make_batch_bytes, make_schema_pk_u64_payload_string, prose, random_chars};
use gnitz_foundation::rng::{Rng, mix};

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

/// Each row a random token, 43 characters of base64: no symbol table codes
/// them to fewer bytes.
fn token_batch(range: std::ops::Range<u64>) -> Batch {
    let mut rng = Rng::new(range.start);
    make_batch_bytes(
        &make_schema_pk_u64_payload_string(),
        &range
            .map(|i| (i, 1, random_chars(&mut rng, BASE64, 43)))
            .collect::<Vec<_>>(),
    )
}

/// A table that does not pay is kept while more values code no better under
/// it, so a store of them is not trained again on each run.
#[test]
fn a_table_that_does_not_pay_is_kept_while_values_code_no_better() {
    let book = Codebook::for_batch(None, &token_batch(0..400)).unwrap();
    assert!(!book.pays());
    let kept = Codebook::for_batch(Some(&book), &token_batch(5000..5400)).unwrap();
    assert!(Rc::ptr_eq(&book, &kept), "more random tokens");
    let prose = Codebook::for_batch(Some(&book), &prose_batch(0..400)).unwrap();
    assert!(!Rc::ptr_eq(&book, &prose));
    assert!(prose.pays());
}
