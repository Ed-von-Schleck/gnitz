use super::*;
use proptest::prelude::*;
use std::os::unix::fs::MetadataExt;

proptest! {
    #[test]
    fn spill_sort_equals_reference(
        stride in prop_oneof![(1usize..=10).prop_map(|w| w * 8), prop::sample::select(vec![1usize, 4, 12, 20])],
        at in (0usize..80, 0usize..80),
        recs in proptest::collection::vec((0u8..4, 0u8..4), 0..300),
        budget in prop_oneof![Just(usize::MAX), 0usize..=720],
        per_push in 1usize..=7,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let mut s = SpillSort::new(dir.path().to_str().unwrap(), stride, budget);
        let slot = s.slot();
        assert_eq!(slot, stride.next_multiple_of(8));
        let mut reference: Vec<Vec<u8>> = Vec::new();
        // Pushed several records at a time, so one push can cross a run boundary.
        for group in recs.chunks(per_push) {
            let mut slots = vec![0u8; group.len() * slot];
            for (r, &(a, b)) in slots.chunks_exact_mut(slot).zip(group) {
                // Two bytes vary, anywhere in the record and across the sign bit:
                // duplicates, and records differing in any word at any byte of it.
                r[at.0 % stride] = a * 0x55;
                r[at.1 % stride] = b * 0x55;
                reference.push(r[..stride].to_vec());
            }
            s.push(&slots).unwrap();
        }
        // Spilled exactly when a run's worth of bytes was pushed, into a file with no name.
        let spills = !recs.is_empty() && recs.len() * slot >= budget;
        assert_eq!(s.spill.as_ref().map(|f| f.metadata().unwrap().nlink()), spills.then_some(0));
        reference.sort_unstable();
        let mut p = s.finish().unwrap();
        let mut out = Vec::new();
        loop {
            assert_eq!(p.remaining(), reference.len() - out.len());
            let Some(k) = p.next() else { break };
            out.push(k.to_vec());
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
