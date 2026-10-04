use super::*;
use crate::test_support::Rng;
use gnitz_foundation::perf::Counter;
use std::hint::black_box;

/// Instructions and cycles per record of [`sort_indices`] over scattered
/// records, at a stride in each of its width arms, at a chunk-sized `n` and at
/// one far past the cache.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn sort_indices_bench() {
    let counters = [Counter::instructions(), Counter::cycles()];
    for n in [65_536usize, 4 << 20] {
        for stride in [8usize, 16, 24] {
            let mut rng = Rng::new(0x5EED_0000 + stride as u64);
            let mut flat = vec![0u8; n * stride];
            for word in flat.chunks_exact_mut(8) {
                word.copy_from_slice(&rng.next_u64().to_be_bytes());
            }
            let mut idx = Vec::new();
            sort_indices(&flat, stride, &mut idx);
            let [instr, cycles] = counters.each_ref().map(|counter| {
                let ((), count) = counter.measure(|| sort_indices(black_box(&flat), stride, &mut idx));
                count as f64 / n as f64
            });
            black_box(&idx);
            println!(
                "sort_indices_bench n={n:<8} stride={stride:<3} {instr:7.1} instr/record, {cycles:7.1} cycles/record"
            );
        }
    }
}

/// Instructions per [`compare_pk_ordering`] of two random keys, at a stride in
/// each arm of [`pack_pk_be`], and past the packed image both where the image
/// settles the order and where the keys share it.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn pk_compare_bench() {
    const PAIRS: usize = 100_000;
    let counter = Counter::instructions();
    // The stride, and the leading bytes every key shares.
    let cases = [1, 2, 3, 4, 5, 6, 7, 8, 12, 16, 24]
        .map(|s| (s, 0))
        .into_iter()
        .chain([(24, 16)]);
    for (stride, shared) in cases {
        let mut rng = Rng::new(0xC0FFEE + stride as u64);
        let mut flat = vec![0u8; 2 * PAIRS * stride];
        for key in flat.chunks_exact_mut(stride) {
            key[shared..].fill_with(|| rng.next_u64() as u8);
        }
        let (acc, instructions) = counter.measure(|| {
            let mut acc = 0usize;
            for pair in flat.chunks_exact(2 * stride) {
                let (a, b) = pair.split_at(stride);
                acc += compare_pk_ordering(black_box(a), black_box(b)) as i8 as usize;
            }
            acc
        });
        black_box(acc);
        println!(
            "pk_compare_bench stride={stride:<2} shared prefix={shared:<2} {:5.1} instr/compare",
            instructions as f64 / PAIRS as f64
        );
    }
}
