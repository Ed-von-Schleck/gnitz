use super::*;
use gnitz_foundation::perf::{Counters, Event};
use gnitz_foundation::rng::Rng;
use std::hint::black_box;

/// Instructions, cycles and branch misses per cell of
/// [`shift_german_string_heaps`] over a column of mixed, all-short and all-long
/// cells.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn shift_german_string_heaps_bench() {
    const N: usize = 1 << 20;
    let counters = Counters::open([Event::INSTRUCTIONS, Event::CYCLES, Event::BRANCH_MISSES]);
    let mut rng = Rng::new(0x5EED_6E57);
    // (name, shortest length, lengths drawn): mixed straddles the threshold.
    let shapes = [("mixed", 0, 25), ("all-short", 0, 13), ("all-long", 13, 12)];
    for (name, lo, span) in shapes {
        let mut cells = vec![0u8; N * 16];
        for (i, cell) in cells.as_chunks_mut::<16>().0.iter_mut().enumerate() {
            let len = lo + rng.gen_range(span) as u32;
            write_u32_le(cell, 0, len);
            if len as usize > SHORT_STRING_THRESHOLD {
                write_u64_le(cell, 8, i as u64 * 24);
            }
        }
        shift_german_string_heaps(black_box(&mut cells), black_box(1));
        let ((), counts) = counters.measure(|| shift_german_string_heaps(black_box(&mut cells), black_box(1)));
        let [instr, cycles, misses] = counts.map(|count| count as f64 / N as f64);
        black_box(&cells);
        println!(
            "shift_german_string_heaps_bench {name:<9} {instr:6.2} instr/cell, {cycles:6.2} cycles/cell, {misses:6.3} branch-misses/cell"
        );
    }
}
