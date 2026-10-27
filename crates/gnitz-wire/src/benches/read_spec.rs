use super::*;
use gnitz_foundation::perf::Counter;
use std::hint::black_box;

/// Instructions and cycles per key of [`PkKeys::from_keys`] over scattered keys,
/// at a stride in each of its sort arms, at a chunk-sized `n` and at one far
/// past the cache.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn pk_keys_from_keys_bench() {
    let counters = [Counter::instructions(), Counter::cycles()];
    for n in [65_536usize, 4 << 20] {
        for stride in [8usize, 16, 24, 40] {
            let mut x = 0x5EED_0000 + stride as u64;
            let mut flat = vec![0u8; n * stride];
            for word in flat.chunks_exact_mut(8) {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                word.copy_from_slice(&x.to_be_bytes());
            }
            black_box(PkKeys::from_keys(stride, flat.chunks_exact(stride)));
            let [instr, cycles] = counters.each_ref().map(|counter| {
                let (keys, count) =
                    counter.measure(|| PkKeys::from_keys(stride, black_box(&flat).chunks_exact(stride)));
                black_box(keys);
                count as f64 / n as f64
            });
            println!(
                "pk_keys_from_keys_bench n={n:<8} stride={stride:<3} {instr:7.1} instr/key, {cycles:7.1} cycles/key"
            );
        }
    }
}
