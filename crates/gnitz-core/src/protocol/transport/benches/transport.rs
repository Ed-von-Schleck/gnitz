use super::*;
use crate::test_support::{framed, transport_pair};

/// `frames` as the bytes a peer writes.
pub(super) fn bench_wire(frames: &[usize]) -> Vec<u8> {
    frames.iter().flat_map(|&len| framed(&vec![0x5Au8; len])).collect()
}

/// One reading pass, counting the frames it hands out.
pub(super) fn bench_pass(t: &mut ClientTransport) -> usize {
    let mut frames = 0;
    let mut more = true;
    while more {
        more = t
            .read(|f| {
                std::hint::black_box(&f);
                frames += 1;
                Ok(())
            })
            .unwrap();
    }
    frames
}

/// The bursts the read benches run, each as its frames' lengths.
pub(super) fn bench_bursts() -> [(&'static str, Vec<usize>); 4] {
    [
        ("1 x 40 B", vec![40]),
        ("3000 x 40 B", vec![40; 3000]),
        ("16 x 8 KiB", vec![8 << 10; 16]),
        ("1 x 100 KiB + 40 B", vec![100 << 10, 40]),
    ]
}

/// Instructions per burst for the one reading pass that takes it, each burst
/// written whole before the pass.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn read_burst_bench() {
    const ROUNDS: u64 = 200;
    let counter = gnitz_foundation::perf::Counter::instructions().expect("instructions counter");
    let (mut t, peer) = transport_pair();
    for (name, frames) in bench_bursts() {
        let wire = bench_wire(&frames);
        let mut total = 0;
        for _ in 0..ROUNDS {
            peer.send_bytes(&wire);
            let (got, instr) = counter.measure(|| bench_pass(&mut t));
            assert_eq!(got, frames.len(), "{name}: one pass takes the burst");
            total += instr;
        }
        println!("read {name}: {} instr/burst", total / ROUNDS);
    }
}
