use super::*;
use gnitz_foundation::perf::Counter;
use std::hint::black_box;

/// Instructions and cycles per key of [`PkKeys::from_keys`] at a stride in each of
/// its sort arms, over random words, over a dense id range shuffled, and over that
/// range ascending; at a small, a chunk-sized and a far-past-cache `n`.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn pk_keys_from_keys_bench() {
    let counters = [Counter::instructions(), Counter::cycles()];
    for shape in ["random", "dense-shuffled", "dense-ascending"] {
        for n in [1024usize, 65_536, 4 << 20] {
            for stride in [8usize, 16, 24, 40] {
                let mut x = 0x5EED_0000 + stride as u64;
                let mut next = || {
                    x ^= x << 13;
                    x ^= x >> 7;
                    x ^= x << 17;
                    x
                };
                let mut flat = vec![0u8; n * stride];
                match shape {
                    "random" => {
                        for word in flat.as_chunks_mut::<8>().0 {
                            *word = next().to_be_bytes();
                        }
                    }
                    _ => {
                        // OPK of i64 ids 0..n in the last 8 bytes, the rest a constant.
                        let mut ids: Vec<u64> = (0..n as u64).collect();
                        if shape == "dense-shuffled" {
                            for i in (1..n).rev() {
                                ids.swap(i, (next() % (i as u64 + 1)) as usize);
                            }
                        }
                        for (key, id) in flat.chunks_exact_mut(stride).zip(ids) {
                            key.fill(0x80);
                            key[stride - 8..].copy_from_slice(&(id ^ (1 << 63)).to_be_bytes());
                        }
                    }
                }
                black_box(PkKeys::from_keys(stride, flat.chunks_exact(stride)));
                let [instr, cycles] = counters.each_ref().map(|counter| {
                    let (keys, count) =
                        counter.measure(|| PkKeys::from_keys(stride, black_box(&flat).chunks_exact(stride)));
                    black_box(keys);
                    count as f64 / n as f64
                });
                println!(
                    "pk_keys_from_keys_bench {shape:<15} n={n:<8} stride={stride:<3} {instr:7.1} instr/key, {cycles:7.1} cycles/key"
                );
            }
        }
    }
}
