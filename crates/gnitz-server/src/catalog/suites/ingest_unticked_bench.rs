//! `ingest_unticked` into a base table no view reads. Report `ns/row` and
//! instructions per row.

use std::hint::black_box;
use std::time::Instant;

use super::*;
use gnitz_foundation::perf;

/// Rows every arm pushes, however it splits them.
const TOTAL_ROWS: u64 = 500_000;
/// Rows between two seals, standing in for auto-ticks.
const TICK_ROWS: u64 = 10_000;

#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn ingest_unticked_bench() {
    let counter = perf::Counter::instructions().expect("instructions counter");
    println!("{:>6} {:>10} {:>10} {:>12}", "rows", "pushes", "ns/row", "instr/row");

    for rows_per_push in [1_000u64, 1] {
        let dir = temp_dir(&format!("ingest_unticked_bench_{rows_per_push}"));
        let mut engine = CatalogEngine::open(&dir, 1).unwrap();
        let cols = vec![col_def("id", TypeCode::U64), col_def("val", TypeCode::I64)];
        let tid = engine.create_table("public.t", &cols, &[0]).unwrap();
        let schema = engine.registry.relation(tid).map(Relation::schema).unwrap();

        // Untimed: build every push up front, fresh keys throughout.
        let pushes = TOTAL_ROWS / rows_per_push;
        let batches: Vec<Batch> = (0..pushes)
            .map(|p| {
                let mut bb = BatchBuilder::new(&schema);
                for i in 0..rows_per_push {
                    let id = p * rows_per_push + i;
                    bb.begin_row(id as u128, 1);
                    bb.put_u64(id);
                    bb.end_row();
                }
                bb.finish()
            })
            .collect();
        let tick_every = (TICK_ROWS / rows_per_push) as usize;

        let t = Instant::now();
        let ((), instructions) = counter.measure(|| {
            for (p, b) in batches.into_iter().enumerate() {
                engine.ingest_unticked(tid, b).unwrap();
                if (p + 1) % tick_every == 0 {
                    black_box(engine.registry.seal(tid).unwrap());
                }
            }
        });
        let ns = t.elapsed().as_nanos() as f64;
        black_box(&engine);

        let rows = TOTAL_ROWS as f64;
        println!(
            "{:>6} {:>10} {:>10.1} {:>12.1}",
            rows_per_push,
            pushes,
            ns / rows,
            instructions as f64 / rows
        );
        engine.close();
        let _ = fs::remove_dir_all(&dir);
    }
}
