use super::*;
use gnitz_foundation::perf::{Counters, Event};
use gnitz_foundation::rng::Rng;
use std::hint::black_box;

/// Packing a string column of `n` rows, by the shape of its values: long
/// values drawn from a million (the churn scenario's note: the larger the
/// column, the more of its values repeat), long values drawn from five, and
/// inline values drawn from five thousand (its owner). Prints the image and heap bytes beside the cost, so a
/// cheaper pass is seen to choose as large an image.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn pack_string_column_bench() {
    let counters = Counters::open([Event::INSTRUCTIONS, Event::CYCLES]);
    type Value = fn(&mut Rng) -> String;
    let shapes: [(&str, Value); 3] = [
        ("long, of 1M values", |rng| {
            format!(
                "adjusted in round {:06} by the nightly reconciliation job",
                rng.gen_range(1_000_000)
            )
        }),
        ("long, of 5 values", |rng| {
            format!(
                "adjusted in round {:06} by the nightly reconciliation job",
                rng.gen_range(5)
            )
        }),
        ("inline, of 5000 values", |rng| {
            format!("owner-{:05}", rng.gen_range(5_000))
        }),
    ];
    for (label, value) in shapes {
        for n in [1 << 14, 1 << 16, 1 << 18, 1 << 20] {
            let mut rng = Rng::new(n as u64);
            let mut src = Vec::new();
            let cells: Vec<[u8; 16]> = (0..n)
                .map(|_| gnitz_wire::encode_german_string(value(&mut rng).as_bytes(), &mut src))
                .collect();
            let ((encoding, image, heap), [i, c]) = counters.measure(|| {
                let mut heap = HeapWriter::plain();
                let (encoding, image) = pack_string_column(black_box(&cells), Heap::Plain(&src), &mut heap);
                (encoding, image, heap.finish().1)
            });
            println!(
                "{label}, {n} rows: {:.1} instr/row, {:.1} cycles/row, {encoding:?}, {} image + {} heap bytes",
                i as f64 / n as f64,
                c as f64 / n as f64,
                image.len(),
                heap.len(),
            );
        }
    }
}
