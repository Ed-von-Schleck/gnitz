use super::*;
use crate::test_support::Rng;
use gnitz_foundation::perf::Counter;
use std::hint::black_box;

/// Instructions per comparison of `stride`-byte key pairs laid out in `flat`.
fn per_compare(counter: &Counter, flat: &[u8], stride: usize, cmp: impl Fn(&[u8], &[u8]) -> Ordering) -> f64 {
    let (acc, instructions) = counter.measure(|| {
        let mut acc = 0usize;
        for pair in flat.chunks_exact(2 * stride) {
            let (a, b) = pair.split_at(stride);
            acc += cmp(black_box(a), black_box(b)) as i8 as usize;
        }
        acc
    });
    black_box(acc);
    instructions as f64 / (flat.len() / (2 * stride)) as f64
}

/// Instructions per [`compare_pk_ordering`] of two random keys, beside a slice
/// `memcmp` of the same pairs, at a stride in each arm of [`pack_pk_be`] and of
/// the wide split, where the first byte decides and where only the last does.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn pk_compare_bench() {
    const PAIRS: usize = 100_000;
    let counter = Counter::instructions();
    for stride in [1usize, 2, 3, 4, 5, 6, 7, 8, 12, 16, 24, 40] {
        for shared in std::iter::once(0).chain((stride > 1).then_some(stride - 1)) {
            let mut rng = Rng::new(0xC0FFEE + stride as u64);
            let mut flat = vec![0u8; 2 * PAIRS * stride];
            for key in flat.chunks_exact_mut(stride) {
                key[shared..].fill_with(|| rng.next_u64() as u8);
            }
            let ours = per_compare(&counter, &flat, stride, compare_pk_ordering);
            let memcmp = per_compare(&counter, &flat, stride, |a, b| a.cmp(b));
            println!(
                "pk_compare_bench stride={stride:<2} shared prefix={shared:<2} {ours:5.1} instr/compare, memcmp {memcmp:5.1}"
            );
        }
    }
}
