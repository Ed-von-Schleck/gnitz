//! The base-table write path — `enforce_unique_pk` and the store ingest it feeds
//! — one arm per push shape. Report `ns/row`, instructions per row, and
//! instructions per probe of a never-held key against the store the arm left.

use super::enforce_unique_pk;
use crate::storage::{RecoverySource, StoreBudgets, Table, DEFAULT_RAM_TIER_BYTES};
use crate::test_support::Rng;
use crate::test_support::{make_schema_pk_u64_payload_string, make_schema_u64_i64};
use gnitz_zset::repr::{Batch, BatchBuilder};
use gnitz_zset::schema::SchemaDescriptor;

/// Rows every arm pushes, however it splits them.
const TOTAL_ROWS: usize = 500_000;
/// Few enough that nearly every push after the first few is an update, many
/// enough that the memtable folds and a probe crosses both RAM tiers.
const HOT_KEYS: u64 = 50_000;
/// Never-held keys probed against the store each arm leaves.
const MISS_PROBES: usize = 200_000;

/// `rows_per_push`-row pushes of `row(seq) = (pk, weight, payload)` for `seq` in
/// `0..TOTAL_ROWS`, timed over a store of RAM tier `ram_tier` that already holds
/// keys `0..held`.
struct Arm {
    label: &'static str,
    strings: bool,
    ram_tier: usize,
    held: u64,
    rows_per_push: usize,
    row: Box<dyn FnMut(u64) -> (u64, i64, u64)>,
}

fn arm(label: &'static str, held: u64, rows_per_push: usize, row: impl FnMut(u64) -> (u64, i64, u64) + 'static) -> Arm {
    Arm {
        label,
        strings: false,
        ram_tier: DEFAULT_RAM_TIER_BYTES,
        held,
        rows_per_push,
        row: Box::new(row),
    }
}

fn random_key(keys: u64) -> impl FnMut(u64) -> (u64, i64, u64) {
    let mut rng = Rng::new(0x5EED_1234);
    move |seq| (rng.gen_range(keys), 1, seq + 1)
}

/// A fresh random key above every held one.
fn fresh_key(rng: &mut Rng) -> u64 {
    rng.next_u64() >> 1 | 1 << 40
}

/// A fresh random key inserted, then deleted by the next push.
fn churn_rand() -> impl FnMut(u64) -> (u64, i64, u64) {
    let mut rng = Rng::new(0xC0FFEE);
    let mut last = 0;
    move |s| {
        if s % 2 == 0 {
            last = fresh_key(&mut rng);
            (last, 1, 7)
        } else {
            (last, -1, 7)
        }
    }
}

/// A fresh random key inserted, then the oldest of `held` and the inserted keys
/// deleted.
fn fifo_rand(held: u64) -> impl FnMut(u64) -> (u64, i64, u64) {
    let mut rng = Rng::new(0xF1F0);
    let mut queue: std::collections::VecDeque<u64> = (0..held).collect();
    move |s| {
        if s % 2 == 0 {
            let k = fresh_key(&mut rng);
            queue.push_back(k);
            (k, 1, 7)
        } else {
            (queue.pop_front().unwrap(), -1, 0)
        }
    }
}

/// Pushes of `per` rows each over `seqs`; a string payload is `v` spelled out
/// long enough to live on the heap.
fn pushes(
    schema: &SchemaDescriptor,
    strings: bool,
    seqs: std::ops::Range<u64>,
    per: usize,
    mut row: impl FnMut(u64) -> (u64, i64, u64),
) -> Vec<Batch> {
    let seqs: Vec<u64> = seqs.collect();
    seqs.chunks(per)
        .map(|chunk| {
            let mut bb = BatchBuilder::new(schema);
            for &seq in chunk {
                let (pk, w, v) = row(seq);
                bb.begin_row(pk as u128, w);
                if strings {
                    bb.put_string(&format!("a payload that lives on the heap #{v:>10}"));
                } else {
                    bb.put_int(v as u128);
                }
                bb.end_row();
            }
            bb.finish()
        })
        .collect()
}

#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn unique_pk_bench() {
    use std::hint::black_box;
    use std::time::Instant;

    let dir = tempfile::tempdir().unwrap();
    let new_table = |label: &str, schema: SchemaDescriptor, ram_tier: usize| {
        Table::new(
            dir.path().join(label).to_str().unwrap(),
            schema,
            RecoverySource::SalReplay,
            StoreBudgets::new(ram_tier),
        )
        .unwrap()
    };
    let apply = |t: &mut Table, batches: Vec<Batch>| {
        for b in batches {
            let eff = enforce_unique_pk(t, b);
            t.ingest_borrowed_batch(&eff).unwrap();
        }
    };

    // Untimed warmup: thread-local batch pool + arena.
    let ints = make_schema_u64_i64();
    apply(
        &mut new_table("warm", ints, DEFAULT_RAM_TIER_BYTES),
        pushes(&ints, false, 0..8000, 1000, |s| (s, 1, s)),
    );

    let mut shuffled: Vec<u64> = (0..TOTAL_ROWS as u64).collect();
    let mut rng = Rng::new(0x5EED_1234);
    rng.shuffle(&mut shuffled);
    // Keys are random where they repeat: a monotone stream would warm each
    // probe's page for the next, which production arrival order does not.
    let arms = [
        arm("insert", 0, 1000, |s| (s, 1, s)),
        arm("insert1", 0, 1, |s| (s, 1, s)),
        arm("update", 0, 1000, random_key(HOT_KEYS)),
        arm("update1", HOT_KEYS, 1, random_key(HOT_KEYS)),
        Arm {
            strings: true,
            ..arm("update_str", HOT_KEYS, 1000, random_key(HOT_KEYS))
        },
        arm("delete", TOTAL_ROWS as u64, 1000, move |s| {
            (shuffled[s as usize], -1, 0)
        }),
        arm("dupkeys", 0, 1000, random_key(100)),
        // Ascending blocks of the held keys, each revisit at a larger payload.
        arm("monotone", HOT_KEYS, 1000, |s| (s % HOT_KEYS, 1, s / HOT_KEYS + 1)),
        arm("update1_hot", 5_000, 1, random_key(5_000)),
        // Each fresh key inserted, then deleted.
        Arm {
            ram_tier: 2 << 20,
            ..arm("churnlive", 20_000, 1, |s| {
                (20_000 + s / 2, if s % 2 == 0 { 1 } else { -1 }, 7)
            })
        },
        arm("update1_big", 800_000, 1, random_key(800_000)),
        Arm {
            ram_tier: 2 << 20,
            ..arm("churn_rand", 60_000, 1, churn_rand())
        },
        arm("fifo_big", 800_000, 1, fifo_rand(800_000)),
    ];

    let counter = gnitz_foundation::perf::Counter::instructions().expect("instructions counter");
    println!(
        "{:>12} {:>10} {:>12} {:>12} {:>12} {:>12}",
        "arm", "rows", "eff_rows", "ns/row", "instr/row", "instr/miss"
    );
    for mut arm in arms {
        let schema = match arm.strings {
            true => make_schema_pk_u64_payload_string(),
            false => ints,
        };
        let mut table = new_table(arm.label, schema, arm.ram_tier);
        apply(
            &mut table,
            pushes(&schema, arm.strings, 0..arm.held, 1000, |s| (s, 1, 0)),
        );
        let batches = pushes(
            &schema,
            arm.strings,
            0..TOTAL_ROWS as u64,
            arm.rows_per_push,
            &mut arm.row,
        );

        let mut eff_rows = 0usize;
        let t = Instant::now();
        let ((), instructions) = counter.measure(|| {
            for b in batches {
                let eff = enforce_unique_pk(&table, b);
                eff_rows += eff.len();
                table.ingest_borrowed_batch(&eff).unwrap();
            }
        });
        let ns = t.elapsed().as_nanos() as f64;
        black_box(&table);

        let mut rng = Rng::new(0x0DD_B175);
        let misses: Vec<[u8; 8]> = (0..MISS_PROBES).map(|_| fresh_key(&mut rng).to_be_bytes()).collect();
        let (hits, miss_instructions) =
            counter.measure(|| misses.iter().filter(|k| table.has_pk_bytes(&k[..])).count());
        black_box(hits);

        let rows = TOTAL_ROWS as f64;
        println!(
            "{:>12} {:>10} {:>12} {:>12.1} {:>12.1} {:>12.1}",
            arm.label,
            TOTAL_ROWS,
            eff_rows,
            ns / rows,
            instructions as f64 / rows,
            miss_instructions as f64 / MISS_PROBES as f64
        );
    }
}

/// The scanned-table write path: each push lands above the cut and is sealed
/// by its tick, then a reader gathers as many random held keys at the cut.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn sealed_tick_bench() {
    use crate::storage::Cut;
    use gnitz_wire::PkKeys;
    use std::hint::black_box;

    const ROWS: usize = 200_000;
    let dir = tempfile::tempdir().unwrap();
    let ints = make_schema_u64_i64();
    let counter = gnitz_foundation::perf::Counter::instructions().expect("instructions counter");
    println!(
        "{:>10} {:>8} {:>16} {:>16}",
        "shape", "per_tick", "write instr/row", "read instr/row"
    );
    for shape in ["insert", "update"] {
        for per in [1usize, 10, 100, 1000] {
            let mut table = Table::new(
                dir.path().join(format!("{shape}{per}")).to_str().unwrap(),
                ints,
                RecoverySource::SalReplay,
                StoreBudgets::new(DEFAULT_RAM_TIER_BYTES),
            )
            .unwrap();
            let held = if shape == "update" { HOT_KEYS } else { 0 };
            for b in pushes(&ints, false, 0..held, 1000, |s| (s, 1, 0)) {
                let eff = enforce_unique_pk(&table, b);
                table.ingest_pending(eff);
                table.seal().unwrap();
            }
            let row: Box<dyn FnMut(u64) -> (u64, i64, u64)> = match shape {
                "insert" => Box::new(|s| (s, 1, s)),
                _ => Box::new(random_key(HOT_KEYS)),
            };
            let batches = pushes(&ints, false, 0..ROWS as u64, per, row);
            let mut rng = Rng::new(0xABCDEF);
            let reads: Vec<PkKeys> = (0..batches.len())
                .map(|t| {
                    let top = if shape == "update" {
                        HOT_KEYS
                    } else {
                        ((t + 1) * per) as u64
                    };
                    let keys: Vec<[u8; 8]> = (0..per).map(|_| rng.gen_range(top).to_be_bytes()).collect();
                    PkKeys::from_keys(8, keys.iter().map(|k| &k[..]))
                })
                .collect();
            let (mut write, mut read, mut got) = (0u64, 0u64, 0usize);
            for (b, keys) in batches.into_iter().zip(reads) {
                let ((), w) = counter.measure(|| {
                    let eff = enforce_unique_pk(&table, b);
                    table.ingest_pending(eff);
                    black_box(table.seal().unwrap());
                });
                write += w;
                let (n, r) = counter.measure(|| {
                    let mut g = table.gather(keys, Cut::Sealed);
                    let mut n = 0;
                    while let Some(chunk) = g.drain_chunk(4096) {
                        n += chunk.len();
                    }
                    n
                });
                read += r;
                got += n;
            }
            assert!(got > 0, "the reads found no row");
            println!(
                "{:>10} {:>8} {:>16.1} {:>16.1}",
                shape,
                per,
                write as f64 / ROWS as f64,
                read as f64 / got as f64
            );
        }
    }
}
