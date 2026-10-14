use super::*;
use crate::test_support::{make_string_batch, map_coded_shard, prose};

/// `rows` as weight-1 rows of their values, keyed by position.
fn batch_of(values: &[String]) -> Batch {
    let rows: Vec<(u64, i64, &[u8])> = values.iter().zip(1..).map(|(v, pk)| (pk, 1, v.as_bytes())).collect();
    make_string_batch(&rows)
}

fn prose_batch(range: std::ops::Range<usize>) -> Batch {
    batch_of(&range.map(prose).collect::<Vec<_>>())
}

/// A table read back from a shard codes every value as the encoder trained
/// over it did: what lets a merge copy a value's codes.
#[test]
fn a_table_read_back_codes_as_the_one_trained() {
    let dir = tempfile::tempdir().unwrap();
    let batch = prose_batch(0..400);
    let book = Codebook::for_batch(None, &batch).expect("enough long bytes to train");
    let shard = map_coded_shard(&dir.path().join("s.db"), &batch, &book);
    let read = Codebook::of_shard(&shard).unwrap();
    assert_eq!(read.table().digest(), book.table().digest());
    assert_eq!(read.trained_sixteenths(), book.trained_sixteenths());
    for i in 0..2000 {
        let (mut a, mut b) = (Vec::new(), Vec::new());
        book.encode_into(prose(i).as_bytes(), &mut a);
        read.encode_into(prose(i).as_bytes(), &mut b);
        assert_eq!(a, b, "value {i}");
    }
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
    let hex: Vec<String> = (0..400u64)
        .map(|i| format!("{:032x}", i.wrapping_mul(0x9E37_79B9_7F4A_7C15)))
        .collect();
    let drifted = Codebook::for_batch(Some(&book), &batch_of(&hex)).unwrap();
    assert!(!Rc::ptr_eq(&book, &drifted));
    assert_ne!(drifted.table().digest(), book.table().digest());
}
