use super::*;
use crate::test_support::{wide_rows, wide_schema};
use WireConflictMode::Update;

/// Instructions to buffer a one-row batch, by column count: as a buffer's
/// first family, and as one more push to a relation the buffer keeps growing
/// under.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn txn_buffer_push_bench() {
    const ROUNDS: u64 = 2000;
    let counter = gnitz_foundation::perf::Counter::instructions();
    for ncols in [4usize, 64] {
        let schema = wide_schema(ncols, false);
        let push = |buf: &mut TxnBuffer| {
            let batch = wide_rows(&schema, 1, 0);
            counter
                .measure(|| buf.push(1, &schema, batch, Update, BLIND).unwrap())
                .1
        };
        let mut buf = TxnBuffer::default();
        let mut first = push(&mut buf);
        for _ in 1..ROUNDS {
            first += push(&mut TxnBuffer::default());
        }
        let later: u64 = (0..ROUNDS).map(|_| push(&mut buf)).sum();
        println!(
            "txn push, {ncols} columns: new family {} / later push {} instr",
            first / ROUNDS,
            later / ROUNDS
        );
    }
}
