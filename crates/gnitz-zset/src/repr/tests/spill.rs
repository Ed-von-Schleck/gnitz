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

// ---------------------------------------------------------------------------
// Micro-benchmark
// ---------------------------------------------------------------------------

/// The whole pipeline — push 128 MiB of random records, `finish`, drain — at
/// stride classes up to the widest the pre-flight reaches, with the data in one
/// RAM run, in 4 spilled runs and in 32.
#[test]
#[ignore = "microbenchmark; run explicitly with --release --ignored --nocapture"]
fn spill_sort_bench() {
    use crate::test_support::Rng;

    const DATA: usize = 128 << 20;
    for &stride in &[8usize, 16, 24, 40, 64] {
        let n = DATA / stride;
        let mut rng = Rng::new(0x5EED_0000 + stride as u64);
        let mut flat = vec![0u8; n * stride];
        for word in flat.chunks_exact_mut(8) {
            word.copy_from_slice(&rng.next_u64().to_be_bytes());
        }
        for (runs, budget) in [(1, usize::MAX), (4, DATA / 4), (32, DATA / 32)] {
            let dir = tempfile::tempdir().unwrap();
            let start = std::time::Instant::now();
            let mut s = SpillSort::new(dir.path().to_str().unwrap(), stride, budget);
            for r in flat.chunks_exact(stride) {
                s.push(r).unwrap();
            }
            let mut p = s.finish().unwrap();
            let mut sink = 0u64;
            while let Some(k) = p.next() {
                sink = sink.wrapping_add(u64::from(k[0] ^ k[stride - 1]));
            }
            std::hint::black_box(sink);
            let ns = start.elapsed().as_secs_f64() * 1e9 / n as f64;
            println!("  stride={stride:<3} runs={runs:<3} {ns:7.2} ns/record");
        }
    }
}
