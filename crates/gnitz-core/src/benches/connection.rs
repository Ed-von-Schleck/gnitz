use super::*;
use crate::test_support::{session_pair, wide_rows, wide_schema};
use std::hint::black_box;

/// Instructions to encode a push request of four nullable columns into a buffer
/// the rounds reuse, at one row and at 100k, with no NULL and with one NULL per
/// row.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn push_encode_bench() {
    let counter = gnitz_foundation::perf::Counter::instructions();
    for (nrows, iters) in [(1usize, 2000u64), (100_000, 20)] {
        for null_every in [0usize, 4] {
            let schema = wide_schema(5, true);
            let batch = wide_rows(&schema, nrows, null_every);
            let target = Target { tid: 7, token: 1 };
            let mut e = 0;
            let mut out = Vec::new();
            for _ in 0..iters {
                let push = || Request::Push {
                    target,
                    batch: &batch,
                    mode: WireConflictMode::Update,
                };
                out.clear();
                e += counter
                    .measure(|| {
                        let (verb, families) = push().write_families().expect("a write");
                        let write = SpanningWrite::cut(verb, &families, gnitz_wire::MAX_FRAME_PAYLOAD).unwrap();
                        write.encode(0, black_box(&mut out))
                    })
                    .1;
            }
            println!(
                "push_encode_bench {nrows} rows, null every {null_every}: {:.2} instr/row",
                e as f64 / (iters * nrows as u64) as f64,
            );
        }
    }
}

/// Instructions to submit a request and write its frame to the socket, by
/// request.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn submit_write_bench() {
    let schema = wide_schema(5, true);
    let batch = wide_rows(&schema, 1, 0);
    let reply_schema = Arc::new(wide_schema(5, true));
    let spec = gnitz_wire::ReadSpec::all_rows(gnitz_wire::ReadBound::None);
    submit_write("alloc ids", |s| drop(s.submit(Request::AllocIds(1))));
    submit_write("1-row push", |s| {
        drop(s.submit(Request::Push {
            target: Target { tid: 7, token: 1 },
            batch: &batch,
            mode: WireConflictMode::Update,
        }))
    });
    submit_write("scan", |s| drop(s.submit_scan(Target::from(7), &spec, &reply_schema)));
}

/// Instructions per `submit` and the write that ships it, over one session
/// that answers none of them.
fn submit_write(name: &str, submit: impl Fn(&mut Session)) {
    const ROUNDS: u64 = 2000;
    // Every round's request stays in flight, so all of them must fit under the cap.
    const _: () = assert!(ROUNDS < MAX_IN_FLIGHT as u64);
    let counter = gnitz_foundation::perf::Counter::instructions();
    let (mut s, peer) = session_pair();
    let mut total = 0;
    for _ in 0..ROUNDS {
        total += counter
            .measure(|| {
                submit(&mut s);
                s.step(Interest::WRITE);
            })
            .1;
        black_box(peer.recv());
    }
    println!("submit_write_bench {name}: {} instr", total / ROUNDS);
}
