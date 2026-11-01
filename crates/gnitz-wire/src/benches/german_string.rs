use super::*;
use gnitz_foundation::perf::Counter;
use std::hint::black_box;

/// Instructions, cycles and branch misses per cell of
/// [`shift_german_string_heaps`] over a column of mixed, all-short and all-long
/// cells.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn shift_german_string_heaps_bench() {
    const N: usize = 1 << 20;
    let counters = [Counter::instructions(), Counter::cycles(), Counter::branch_misses()];
    let mut x = 0x5EED_6E57u64;
    let mut next = move || {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        x
    };
    // (name, shortest length, lengths drawn): mixed straddles the threshold.
    let shapes = [("mixed", 0, 25), ("all-short", 0, 13), ("all-long", 13, 12)];
    for (name, lo, span) in shapes {
        let mut cells = vec![0u8; N * 16];
        for (i, cell) in cells.as_chunks_mut::<16>().0.iter_mut().enumerate() {
            let len = lo + (next() % span) as u32;
            write_u32_le(cell, 0, len);
            if len as usize > SHORT_STRING_THRESHOLD {
                write_u64_le(cell, 8, i as u64 * 24);
            }
        }
        shift_german_string_heaps(black_box(&mut cells), black_box(1));
        let [instr, cycles, misses] = counters.each_ref().map(|counter| {
            let ((), count) = counter.measure(|| shift_german_string_heaps(black_box(&mut cells), black_box(1)));
            count as f64 / N as f64
        });
        black_box(&cells);
        println!(
            "shift_german_string_heaps_bench {name:<9} {instr:6.2} instr/cell, {cycles:6.2} cycles/cell, {misses:6.3} branch-misses/cell"
        );
    }
}
