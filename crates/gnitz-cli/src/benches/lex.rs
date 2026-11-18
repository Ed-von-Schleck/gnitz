use super::*;
use std::hint::black_box;

/// Instructions per line to split a script whose one string literal runs
/// over many lines, as a pasted document does, each holding a quoted quote.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn open_literal_split_bench() {
    const LINES: u64 = 4000;
    let line = "it''s ".repeat(13);
    let counter = gnitz_foundation::perf::Counter::instructions();
    let (pieces, n) = counter.measure(|| {
        let mut s = Splitter::default();
        let mut pieces = s.push_line(black_box("INSERT INTO t VALUES (1, '"));
        for _ in 0..LINES {
            pieces.extend(s.push_line(black_box(&line)));
        }
        pieces.extend(s.push_line("');"));
        pieces
    });
    assert_eq!(pieces.len(), 1);
    println!("{} instr/line", n / LINES);
}
