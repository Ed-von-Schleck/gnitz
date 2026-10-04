use super::super::*;
use super::tests::fresh;
use super::tests::seed_guard;
use super::tests::stride_schema;
use super::tests::trailing_gk;
use crate::test_support::make_batch_opk;
use gnitz_zset::repr::Batch;
use gnitz_zset::schema::key::probe_key;

/// `ShardIndex::find_pk_bytes` over a tree holding all three levels, at a narrow
/// and a wide PK stride: instructions per probe for present and absent keys.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn shard_probe_bench() {
    use gnitz_foundation::perf::Counter;
    use std::hint::black_box;
    const TERMINAL_GUARDS: u64 = 64;
    const L1_GUARDS: u64 = 16;
    const SPAN: u64 = 1 << 20; // keys one terminal guard covers
    const ROWS: u64 = 2000;
    const PROBES: u64 = 200_000;
    const STEP: u64 = (SPAN / ROWS) & !1;
    let counter = Counter::instructions().expect("instructions counter");
    for pk_cols in [1usize, 3] {
        let tmp = tempfile::tempdir().unwrap();
        let mut idx = fresh(tmp.path(), stride_schema(pk_cols));
        let total = TERMINAL_GUARDS * SPAN;
        let run = |base: u64, step: u64| -> Batch {
            let rows: Vec<_> = (0..ROWS)
                .map(|i| (trailing_gk(pk_cols, base + i * step).pk_bytes().to_vec(), 1, i as i64))
                .collect();
            make_batch_opk(&stride_schema(pk_cols), &rows)
        };
        // Terminal keys are even, so an odd key inside the range is absent there.
        for g in 0..TERMINAL_GUARDS {
            let base = g * SPAN;
            seed_guard(&mut idx, TERMINAL, trailing_gk(pk_cols, base), &run(base, STEP), 1);
        }
        let l1_span = total / L1_GUARDS;
        for g in 0..L1_GUARDS {
            for f in 0..4u64 {
                let base = g * l1_span;
                seed_guard(
                    &mut idx,
                    L1,
                    trailing_gk(pk_cols, base),
                    &run(base + 2 * f, l1_span / ROWS),
                    2,
                );
            }
        }
        for f in 0..4u64 {
            idx.append_l0_run(&run(2 * f, total / ROWS)).unwrap();
        }
        {
            let mut rng = crate::test_support::Rng::new(0x5EED_1234);
            let ranges: Vec<(PkBuf, PkBuf)> = (0..PROBES)
                .map(|_| {
                    let key = rng.gen_range(TERMINAL_GUARDS) * SPAN + rng.gen_range(ROWS) * STEP;
                    (trailing_gk(pk_cols, key), trailing_gk(pk_cols, key + 4 * STEP))
                })
                .collect();
            let mut found = 0usize;
            let ((), instructions) = counter.measure(|| {
                for &(lo, hi) in &ranges {
                    found += idx.shard_arcs_in_range(lo, hi, true).count();
                }
            });
            black_box(found);
            println!(
                "shard_range stride {}: {:.1} instr/open ({:.2} shards each)",
                pk_cols * 8,
                instructions as f64 / PROBES as f64,
                found as f64 / PROBES as f64
            );
        }
        for (label, odd) in [("present", 0u64), ("absent", 1)] {
            let mut rng = crate::test_support::Rng::new(0x5EED_1234);
            let keys: Vec<PkBuf> = (0..PROBES)
                .map(|_| {
                    let key = rng.gen_range(TERMINAL_GUARDS) * SPAN + rng.gen_range(ROWS) * STEP;
                    trailing_gk(pk_cols, key | odd)
                })
                .collect();
            let mut hits = 0usize;
            let ((), instructions) = counter.measure(|| {
                for k in &keys {
                    let key = k.pk_bytes();
                    idx.find_pk_bytes(key, probe_key(key), |_, _| hits += 1);
                }
            });
            black_box(hits);
            println!(
                "shard_probe stride {} {label}: {:.1} instr/probe ({hits} hits)",
                pk_cols * 8,
                instructions as f64 / PROBES as f64
            );
        }
    }
}
