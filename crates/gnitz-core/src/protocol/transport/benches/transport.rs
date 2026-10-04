use super::*;
use crate::test_support::{framed, transport_pair};
use gnitz_foundation::posix_io::set_sockopt_int;

/// Instructions per burst over the reading passes that take it, by transport
/// and burst shape, each burst written whole before the first pass.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn read_burst_bench() {
    const ROUNDS: u64 = 200;
    let counter = gnitz_foundation::perf::Counter::instructions().expect("instructions counter");
    let (unix, peer) = transport_pair();
    // The largest burst is written with nobody reading.
    set_sockopt_int(peer.0.as_raw_fd(), libc::SOL_SOCKET, libc::SO_SNDBUF, 1 << 20).unwrap();
    let (tls, end) = tls::tests::pair(None);
    let mut ends: [(&str, ClientTransport, Box<dyn Write>); 2] =
        [("unix", unix, Box::new(peer.0)), ("tls", tls, Box::new(end))];
    // Frame lengths: one small frame, many in one read, and a payload wide
    // enough to be read in place.
    for (name, frames) in [
        ("1 x 40 B", &[40][..]),
        ("3000 x 40 B", &[40; 3000]),
        ("1 x 200 KiB + 40 B", &[200 << 10, 40]),
    ] {
        let wire: Vec<u8> = frames.iter().flat_map(|&len| framed(&vec![0x5A; len])).collect();
        for (kind, t, peer) in &mut ends {
            let mut total = 0;
            for _ in 0..ROUNDS {
                peer.write_all(&wire).unwrap();
                peer.flush().unwrap();
                let mut left = frames.len();
                while left > 0 {
                    poll_fd(t.as_raw_fd(), libc::POLLIN, None).unwrap();
                    let ((), instr) = counter.measure(|| {
                        while t
                            .read(|f| {
                                std::hint::black_box(&f);
                                left -= 1;
                                Ok(())
                            })
                            .unwrap()
                        {}
                    });
                    total += instr;
                }
            }
            println!("{kind} read {name}: {} instr/burst", total / ROUNDS);
        }
    }
}
