use super::*;
use crate::schema::TypeCode;
use crate::test_support::pk_only_schema;
use proptest::prelude::*;
use std::os::unix::fs::MetadataExt;

/// A key-only schema whose PK is `stride` bytes wide.
fn key_schema(mut stride: usize) -> crate::schema::SchemaDescriptor {
    let mut pk = Vec::new();
    for (tc, w) in [
        (TypeCode::U128, 16),
        (TypeCode::U64, 8),
        (TypeCode::U32, 4),
        (TypeCode::U8, 1),
    ] {
        while stride >= w {
            pk.push(tc);
            stride -= w;
        }
    }
    pk_only_schema(&pk)
}

proptest! {
    #[test]
    fn spill_sort_equals_reference(
        stride in prop_oneof![(1usize..=10).prop_map(|w| w * 8), prop::sample::select(vec![1usize, 4, 12, 20])],
        at in (0usize..80, 0usize..80),
        recs in proptest::collection::vec((0u8..4, 0u8..4), 0..300),
        budget in prop_oneof![Just(usize::MAX), 0usize..=720],
        per_push in 1usize..=7,
        chunk_rows in 1usize..=50,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let mut s = SpillSort::new(dir.path().to_str().unwrap(), stride, budget);
        let mut reference: Vec<Vec<u8>> = Vec::new();
        // Pushed several records at a time, so one push can cross a run boundary.
        for group in recs.chunks(per_push) {
            let mut records = vec![0u8; group.len() * stride];
            for (r, &(a, b)) in records.chunks_exact_mut(stride).zip(group) {
                // Two bytes vary, anywhere in the record and across the sign bit:
                // duplicates, and records differing in any word at any byte of it.
                r[at.0 % stride] = a * 0x55;
                r[at.1 % stride] = b * 0x55;
                reference.push(r.to_vec());
            }
            s.push(&records).unwrap();
        }
        // Spilled exactly when a run's worth of slots was pushed, into a file with no name.
        let spills = !recs.is_empty() && recs.len() * stride.next_multiple_of(8) >= budget;
        assert_eq!(s.spill.as_ref().map(|f| f.metadata().unwrap().nlink()), spills.then_some(0));
        reference.sort_unstable();
        let mut p = s.finish().unwrap();
        let (mut chunk, mut out) = (Batch::empty_with_schema(&key_schema(stride)), Vec::new());
        loop {
            p.fill(&mut chunk, chunk_rows);
            assert_eq!(chunk.count, chunk_rows.min(reference.len() - out.len()));
            if chunk.count == 0 {
                break;
            }
            out.extend((0..chunk.count).map(|r| chunk.get_pk_bytes(r).to_vec()));
            assert!((0..chunk.count).all(|r| chunk.get_weight(r) == 1));
        }
        assert_eq!(out, reference);
    }
}

/// The dir is opened by the first spill: a sort that stays in RAM never touches
/// it, and an unusable one fails the push that spills.
#[test]
fn the_spill_dir_is_opened_by_the_first_spill() {
    let mut s = SpillSort::new("/nonexistent-gnitz-spill-dir", 8, 16);
    s.push(&[0; 8]).unwrap();
    assert!(s.push(&[0; 8]).is_err());
}
