use super::*;
use proptest::prelude::*;

fn run_case(stride: usize, recs: &[(u8, u8)], budget: Option<usize>) {
    let dir = tempfile::tempdir().unwrap();
    let mut s = SpillSort::new(dir.path().to_str().unwrap(), stride, budget.unwrap_or(usize::MAX));
    let mut reference: Vec<Vec<u8>> = Vec::new();
    for &(a, b) in recs {
        // First and last byte vary: duplicates, and records differing only at the end.
        let mut r = vec![0u8; stride];
        r[0] = a;
        r[stride - 1] = b;
        s.push(&r).unwrap();
        reference.push(r);
    }
    let spills = budget.is_some_and(|b| recs.len() >= b.div_ceil(stride).max(1));
    assert_eq!(s.spill.is_some(), spills);
    reference.sort_unstable();
    let mut p = s.finish().unwrap();
    let mut left = p.remaining();
    assert_eq!(left, recs.len());
    let mut out = Vec::new();
    while let Some(k) = p.next() {
        out.push(k.to_vec());
        left -= 1;
        assert_eq!(p.remaining(), left);
    }
    assert_eq!(out, reference);
}

proptest! {
    #[test]
    fn spill_sort_equals_reference(
        w in 1usize..=10,
        recs in proptest::collection::vec((0u8..4, 0u8..4), 0..300),
        budget in proptest::option::of(0usize..=720),
    ) {
        run_case(w * 8, &recs, budget);
    }
}

#[test]
fn spill_file_is_anonymous() {
    use std::os::unix::fs::MetadataExt;
    let dir = tempfile::tempdir().unwrap();
    let mut s = SpillSort::new(dir.path().to_str().unwrap(), 8, 8);
    s.push(&[1u8; 8]).unwrap();
    assert_eq!(s.spill.as_ref().unwrap().metadata().unwrap().nlink(), 0);
    assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
}

// ---------------------------------------------------------------------------
// Micro-benchmark
// ---------------------------------------------------------------------------

/// The whole pipeline — push 128 MiB of random records, `finish`, drain — at
/// every stride class the pre-flight reaches, with the data in one RAM run, in
/// 4 spilled runs and in 32.
#[test]
#[ignore = "microbenchmark; run explicitly with --release --ignored --nocapture"]
fn spill_sort_bench() {
    use crate::test_rng::Rng;

    const DATA: usize = 128 << 20;
    for &stride in &[8usize, 16, 24, 40, 64, 80] {
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
