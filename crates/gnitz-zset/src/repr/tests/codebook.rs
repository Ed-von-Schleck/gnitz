use super::*;
use crate::test_support::{map_coded_shard, mix, prose, string_batch};

fn prose_batch(range: std::ops::Range<usize>) -> Batch {
    string_batch(&range.map(|i| (i as u64, prose(i))).collect::<Vec<_>>())
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
    let hex: Vec<(u64, String)> = (0..400u64).map(|i| (i, format!("{:032x}", mix(i)))).collect();
    let drifted = Codebook::for_batch(Some(&book), &string_batch(&hex)).unwrap();
    assert!(!Rc::ptr_eq(&book, &drifted));
    assert_ne!(drifted.table().digest(), book.table().digest());
}

/// A table read off disk in an order no trained encoder keeps is refused, never
/// handed to the rebuild that asserts that order.
#[test]
fn a_table_out_of_trained_order_is_refused() {
    use crate::repr::encoding::{SYMBOLS_AT, SYMBOLS_COUNT_AT};
    use crate::repr::layout::spans_of;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s.db");
    let batch = prose_batch(0..400);
    let book = Codebook::for_batch(None, &batch).unwrap();
    map_coded_shard(&path, &batch, &book);
    let mut image = std::fs::read(&path).unwrap();
    let heap = spans_of(&image)[crate::repr::batch::REG_PAYLOAD_START + batch.schema().num_payload_cols()].off;
    let n = gnitz_wire::read_u64_le(&image, heap + SYMBOLS_COUNT_AT) as usize;
    let lens = &mut image[heap + SYMBOLS_AT + n * MAX_SYMBOL_LEN..][..n];
    assert!(lens[0] > 1 && lens[n - 1] == 1, "premise: multi-byte symbols lead");
    lens.swap(0, n - 1);
    std::fs::write(&path, &image).unwrap();
    let forged = MappedShard::open(path.to_str().unwrap(), batch.schema()).unwrap();
    assert!(forged.coded_heap().is_some(), "the lengths still parse");
    assert!(Codebook::of_shard(&forged).is_none());
}
