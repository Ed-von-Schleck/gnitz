//! Release benchmark: instructions per row of pushes into a table a view scans,
//! which hold above the table's cut until the seal its next tick opens with.
//!
//! ```text
//! cd crates && cargo test -p gnitz --release ingest_unticked_bench \
//!     -- --ignored --nocapture --test-threads=1
//! ```

use super::*;
use gnitz_foundation::perf;

/// Rows every arm pushes, however it splits them.
const TOTAL_ROWS: u64 = 500_000;
/// Rows between two seals.
const TICK_ROWS: u64 = 10_000;

#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn ingest_unticked_bench() {
    let counter = perf::Counter::instructions();
    println!("{:>6} {:>10} {:>12}", "rows", "pushes", "instr/row");

    let cols = [col_def("id", TypeCode::U64), col_def("val", TypeCode::I64)];
    for rows_per_push in [1_000u64, 1] {
        let (mut engine, tid, _) = table_fixture(&format!("ingest_unticked_bench_{rows_per_push}"), &cols);
        register_identity_view(&mut engine, tid, "v", &cols);

        // Untimed: build every push up front, fresh keys throughout.
        let pushes = TOTAL_ROWS / rows_per_push;
        let batches: Vec<Batch> = (0..pushes)
            .map(|p| {
                let ids = p * rows_per_push..(p + 1) * rows_per_push;
                rows(&engine, tid, 1, ids, |id| [id])
            })
            .collect();
        let tick_every = (TICK_ROWS / rows_per_push) as usize;

        let (sealed, instructions) = counter.measure(|| {
            let mut sealed = 0;
            for (p, b) in batches.into_iter().enumerate() {
                engine.ingest_unticked(tid, b).unwrap();
                if (p + 1) % tick_every == 0 {
                    sealed += engine.registry.seal(tid).unwrap().map_or(0, |delta| delta.len());
                }
            }
            sealed
        });
        assert_eq!(sealed as u64, TOTAL_ROWS, "every push reached a seal's delta");

        println!(
            "{rows_per_push:>6} {pushes:>10} {:>12.1}",
            instructions as f64 / TOTAL_ROWS as f64
        );
        discard(engine);
    }
}
