use super::tests::Loopback;
use super::*;
use crate::protocol::transport::bench::{bench_bursts, bench_pass, bench_wire};
use crate::protocol::transport::poll_fd;
use std::sync::mpsc;

/// Instructions per burst over the reading passes that take it, each burst
/// written whole before the first pass.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn tls_read_burst_bench() {
    const ROUNDS: u64 = 200;
    let counter = gnitz_foundation::perf::Counter::instructions().expect("instructions counter");
    let (burst_tx, burst_rx) = mpsc::channel::<Vec<u8>>();
    let (written_tx, written_rx) = mpsc::channel::<()>();
    let lb = Loopback::start(move |mut end| {
        for wire in burst_rx {
            end.write_all(&wire).unwrap();
            end.flush().unwrap();
            written_tx.send(()).unwrap();
        }
    });
    let mut t = lb.connect();
    for (name, frames) in bench_bursts() {
        let wire = bench_wire(&frames);
        let mut total = 0;
        for _ in 0..ROUNDS {
            burst_tx.send(wire.clone()).unwrap();
            written_rx.recv().unwrap();
            let mut left = frames.len();
            while left > 0 {
                poll_fd(t.as_raw_fd(), libc::POLLIN, None).unwrap();
                let (got, instr) = counter.measure(|| bench_pass(&mut t));
                left = left.checked_sub(got).expect("no frame past the burst");
                total += instr;
            }
        }
        println!("tls read {name}: {} instr/burst", total / ROUNDS);
    }
    drop(burst_tx);
    lb.join();
}
