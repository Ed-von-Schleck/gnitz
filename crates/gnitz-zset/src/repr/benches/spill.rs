use super::*;

/// The whole pipeline — push 128 MiB of random records, `finish`, drain — at
/// stride classes up to the widest the pre-flight reaches, with the data in one
/// RAM run, in 4 spilled runs and in 32.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
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
