use super::*;
use gnitz_foundation::perf::{Counter, Counters, Event};
use gnitz_foundation::rng::Rng;
use std::hint::black_box;

/// Instructions and cycles per key of [`pk_keys`] at a stride in each of
/// its sort arms, over random words, over a dense id range shuffled, and over that
/// range ascending; at a small, a chunk-sized and a far-past-cache `n`.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn pk_keys_bench() {
    let counters = Counters::open([Event::INSTRUCTIONS, Event::CYCLES]);
    for shape in ["random", "dense-shuffled", "dense-ascending"] {
        for n in [1024usize, 65_536, 4 << 20] {
            for stride in [8usize, 16, 24, 40] {
                let mut rng = Rng::new(0x5EED_0000 + stride as u64);
                let mut flat = vec![0u8; n * stride];
                match shape {
                    "random" => {
                        for word in flat.as_chunks_mut::<8>().0 {
                            *word = rng.next_u64().to_be_bytes();
                        }
                    }
                    _ => {
                        // OPK of i64 ids 0..n in the last 8 bytes, the rest a constant.
                        let mut ids: Vec<u64> = (0..n as u64).collect();
                        if shape == "dense-shuffled" {
                            rng.shuffle(&mut ids);
                        }
                        for (key, id) in flat.chunks_exact_mut(stride).zip(ids) {
                            key.fill(0x80);
                            key[stride - 8..].copy_from_slice(&(id ^ (1 << 63)).to_be_bytes());
                        }
                    }
                }
                black_box(pk_keys(stride, flat.chunks_exact(stride)));
                let (keys, counts) = counters.measure(|| pk_keys(stride, black_box(&flat).chunks_exact(stride)));
                black_box(keys);
                let [instr, cycles] = counts.map(|count| count as f64 / n as f64);
                println!(
                    "pk_keys_bench {shape:<15} n={n:<8} stride={stride:<3} {instr:7.1} instr/key, {cycles:7.1} cycles/key"
                );
            }
        }
    }
}

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
