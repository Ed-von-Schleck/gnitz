//! Release benchmark: instructions per row of one tick of a view whose output
//! `n` readers consume, each a `distinct` that reads it at net weights.
//!
//! ```text
//! cd crates && cargo test -p gnitz-server --release echo_fold_bench \
//!     -- --ignored --nocapture --test-threads=1
//! ```

use super::*;
use crate::query::Drive;
use gnitz_foundation::perf;
use gnitz_wire::{Circuit, ReadBound};

/// Timed ticks per reader count.
const TICKS: u64 = 20;
/// Fresh ids per tick.
const ROWS: u64 = 10_000;

#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn echo_fold_bench() {
    let counter = perf::Counter::instructions().expect("instructions counter");
    let cols = vec![col_def("id", TypeCode::U64), col_def("k", TypeCode::U64)];
    for readers in [0usize, 1, 2, 4] {
        let (mut engine, t, dir) = table_fixture(&format!("echo_fold_{readers}"), &cols);
        let a = register_identity_view(&mut engine, t, "a", &cols);
        backfill(&mut engine, a, &[t]);
        for r in 0..readers {
            let mut circuit = Circuit::default();
            let scan = circuit.input_delta(a, ReadBound::None);
            let d = circuit.distinct(scan);
            circuit.sink(d);
            let v = try_register_view(&mut engine, circuit, &format!("d{r}"), &cols, 0, 0).unwrap();
            backfill(&mut engine, v, &[a]);
        }
        let schema = engine.registry.relation(t).map(Relation::schema).unwrap();

        let mut instructions = 0u64;
        for round in 0..TICKS {
            let mut bb = BatchBuilder::new(schema);
            for i in 0..ROWS {
                // Descending ids, so every tick reaches the view unsorted.
                let id = round * ROWS + (ROWS - 1 - i);
                bb.begin_row(id as u128, 1);
                bb.put_u64(id.wrapping_mul(0x9E37_79B9_7F4A_7C15));
                bb.end_row();
            }
            let effective = engine.registry.ingest_returning(t, bb.finish()).unwrap();
            let what = Drive::Tick { source: t, round: round + 1 };
            let (res, n) = counter.measure(|| crate::query::drive(&mut LocalDrive(&mut engine), what, effective));
            res.unwrap();
            instructions += n;
        }
        println!(
            "readers {readers}: {:>8.1} instr/row",
            instructions as f64 / (TICKS * ROWS) as f64
        );
        engine.close();
        let _ = fs::remove_dir_all(&dir);
    }
}
