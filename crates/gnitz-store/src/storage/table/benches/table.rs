use super::{Cut, RecoverySource, DEFAULT_RAM_TIER_BYTES};
use crate::test_support::{make_batch, make_batch_raw, make_schema_u64_i64, new_table};
use gnitz_foundation::perf::Counter;
use gnitz_wire::PkKeys;
use gnitz_zset::repr::Batch;

/// Instructions per row of `Table::ingest_owned_batch` at the cadence a worker
/// runs it: a tick is one ingest and the memtable drains on its own budget. One
/// case per way the drain ends — the memtable's own fold, a batch past the
/// memtable budget, a RAM-tier fold whose cancellation spares the spill, and the
/// spill.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn table_ingest_bench() {
    const UPDATES: u64 = 1_000_000;
    const HOT: u64 = 8192;
    const SMALL_TIER: usize = 1 << 20;
    type Rows = [Option<(u64, i64, i64)>; 2];
    // Update `u` inserts key `u`.
    let fresh = |u: u64| -> Rows { [None, Some((u, 1, u as i64))] };
    // Update `u` rewrites key `u % HOT`, retracting the row update `u - HOT` left there.
    let churn = |u: u64| -> Rows {
        let retraction = u.checked_sub(HOT).map(|old| (u % HOT, -1, old as i64));
        [retraction, Some((u % HOT, 1, u as i64))]
    };
    // Label, updates per tick, the rows of an update, the RAM-tier ceiling, whether it spills.
    type Case = (&'static str, u64, fn(u64) -> Rows, usize, bool);
    let cases: [Case; 5] = [
        ("fresh keys, 1-row ticks", 1, fresh, DEFAULT_RAM_TIER_BYTES, false),
        ("fresh keys, 100-row ticks", 100, fresh, DEFAULT_RAM_TIER_BYTES, false),
        ("fresh keys, 8192-row ticks", 8192, fresh, DEFAULT_RAM_TIER_BYTES, false),
        (
            "8192 hot keys, 16-update ticks, 1 MiB tier",
            16,
            churn,
            SMALL_TIER,
            false,
        ),
        ("fresh keys, 100-row ticks, 1 MiB tier", 100, fresh, SMALL_TIER, true),
    ];

    let schema = make_schema_u64_i64();
    let counter = Counter::instructions();
    let dir = tempfile::tempdir().unwrap();
    for (case, (label, per_tick, update, tier, spills)) in cases.into_iter().enumerate() {
        let ticks: Vec<Batch> = (0..UPDATES / per_tick)
            .map(|t| {
                let rows: Vec<_> = (t * per_tick..(t + 1) * per_tick).flat_map(update).flatten().collect();
                make_batch_raw(&schema, &rows)
            })
            .collect();
        let rows: usize = ticks.iter().map(Batch::len).sum();
        let rederive = RecoverySource::Rederive { resume_at: None };
        let mut table = new_table(dir.path().join(case.to_string()), schema, rederive, tier);
        let ((), instructions) = counter.measure(|| {
            for tick in ticks {
                table.ingest_owned_batch(tick).unwrap();
            }
        });
        assert!(table.ram_tier.row_count() > 0, "{label}: the memtable never drained");
        assert!(
            rows * 32 > tier || tier == DEFAULT_RAM_TIER_BYTES,
            "{label}: fits the tier"
        );
        assert_eq!(!table.all_shard_arcs().is_empty(), spills, "{label}: spilled");
        println!(
            "table_ingest_bench {label:<44} {:8.1} instr/row",
            instructions as f64 / rows as f64
        );
    }
}

/// Instructions per `Table::gather` of 1, 64 and 4096 keys spread over the key
/// span of a table whose rows sit in shards and in the memtable, the open
/// counted.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn pk_set_gather_bench() {
    const ROWS: u64 = 1 << 18;
    const ITERS: usize = 200;
    let schema = make_schema_u64_i64();
    let dir = tempfile::tempdir().unwrap();
    // A tier of half a round, so every round spills.
    let mut t = new_table(
        dir.path(),
        schema,
        RecoverySource::Rederive { resume_at: None },
        1 << 19,
    );
    for r in 0..8u64 {
        let rows: Vec<(u64, i64, i64)> = (0..ROWS / 8).map(|k| (k * 8 + r, 1, (k * 8 + r) as i64)).collect();
        t.ingest_owned_batch(make_batch(&schema, &rows)).unwrap();
    }
    let late: Vec<(u64, i64, i64)> = (0..64u64).map(|k| (k * (ROWS / 64) + 1, 1, 7)).collect();
    t.ingest_owned_batch(make_batch(&schema, &late)).unwrap();
    assert_eq!(
        (
            t.shard_index.total_rows(),
            t.ram_tier.row_count(),
            t.memtable.row_count()
        ),
        (ROWS as usize, 0, late.len()),
        "every round in a shard, the late rows in the memtable"
    );

    let counter = Counter::instructions();
    for n in [1u64, 64, 4096] {
        let step = ROWS / (n + 1);
        let key_bytes: Vec<[u8; 8]> = (1..=n).map(|i| (i * step).to_be_bytes()).collect();
        let passes = vec![PkKeys::from_keys(8, key_bytes.iter().map(|k| &k[..])); ITERS];
        let (rows, instructions) = counter.measure(|| {
            let gather = |keys| t.gather(keys, Cut::Now).drain_chunk(usize::MAX).map_or(0, |b| b.len());
            passes.into_iter().map(gather).sum::<usize>()
        });
        assert!(rows >= ITERS * n as usize, "every probed key holds a row");
        println!(
            "pk_set_gather_bench {n} keys: {} instr/gather",
            instructions / ITERS as u64
        );
    }
}
