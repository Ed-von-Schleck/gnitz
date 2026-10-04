//! The base-table write path, in instructions per pushed row: `enforce_unique_pk`
//! against the store, then the store's ingest of the effective batch — as a table
//! no view scans takes it, and as a scanned one does, sealed after every push and
//! after every [`TICK_ROWS`].
//!
//! `cd crates && cargo test -p gnitz-store --release unique_pk_bench -- --ignored --nocapture --test-threads=1`

use super::enforce_unique_pk;
use crate::storage::{RecoverySource, DEFAULT_RAM_TIER_BYTES};
use crate::test_support::{make_schema_pk_u64_payload_string, make_schema_u64_i64, new_table};
use gnitz_zset::repr::{Batch, BatchBuilder};
use gnitz_zset::schema::SchemaDescriptor;

/// Rows each cell pushes, into a store holding keys `0..ROWS`.
const ROWS: u64 = 200_000;
/// A RAM tier the held rows overflow, so they sit in shards.
const SPILL_TIER: usize = 1 << 20;
/// Above every held key.
const FRESH: u64 = 1 << 40;
/// Pushed rows between two seals when no read forces one.
const TICK_ROWS: u64 = 10_000;

/// `(pk, weight, payload)` of the `seq`-th pushed row.
type Row = fn(u64) -> (u64, i64, u64);

/// 40 scattered bits.
fn scatter(seq: u64) -> u64 {
    seq.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 24
}

/// `per`-row pushes of `row(seq)` over `0..ROWS`; a string payload is spelled
/// out long enough to live on the heap.
fn pushes(schema: &SchemaDescriptor, per: u64, row: Row) -> Vec<Batch> {
    (0..ROWS)
        .step_by(per as usize)
        .map(|from| {
            let mut bb = BatchBuilder::new(schema);
            for (pk, w, v) in (from..from + per).map(row) {
                bb.begin_row(pk as u128, w);
                match schema.string_payload_slots() {
                    0 => bb.put_int(v as u128),
                    _ => bb.put_string(&format!("a payload that lives on the heap #{v:>10}")),
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

    let ints = make_schema_u64_i64();
    let strings = make_schema_pk_u64_payload_string();
    const RAM: usize = DEFAULT_RAM_TIER_BYTES;
    // Label, schema, RAM tier, the row of a push, effective rows per row pushed alone.
    let arms: [(&str, SchemaDescriptor, usize, Row, u64); 11] = [
        ("fresh, ascending", ints, RAM, |s| (FRESH + s, 1, s), 1),
        ("fresh, scattered", ints, RAM, |s| (FRESH + scatter(s), 1, s), 1),
        (
            "fresh, scattered, shards",
            ints,
            SPILL_TIER,
            |s| (FRESH + scatter(s), 1, s),
            1,
        ),
        ("update", ints, RAM, |s| (scatter(s) % ROWS, 1, s + 1), 2),
        ("update, shards", ints, SPILL_TIER, |s| (scatter(s) % ROWS, 1, s + 1), 2),
        ("update, strings", strings, RAM, |s| (scatter(s) % ROWS, 1, s + 1), 2),
        // The held keys in order: a memtable run covers one stretch of them.
        ("update, ascending", ints, RAM, |s| (s, 1, 1), 2),
        // 64 keys: the memtable never drains, and a 1000-row push repeats each.
        ("update, 64 keys", ints, RAM, |s| (scatter(s) % 64, 1, s + 1), 2),
        // Each held key once.
        ("delete", ints, RAM, |s| (s * 7919 % ROWS, -1, 0), 1),
        ("delete, shards", ints, SPILL_TIER, |s| (s * 7919 % ROWS, -1, 0), 1),
        // A fresh key, then its delete: one push apart, or cancelled inside one.
        (
            "insert + delete",
            ints,
            RAM,
            |s| (FRESH + scatter(s / 2), 1 - 2 * (s % 2) as i64, 7),
            1,
        ),
    ];

    let dir = tempfile::tempdir().unwrap();
    let counter = gnitz_foundation::perf::Counter::instructions();
    println!(
        "{:<26} {:>5} | {:>9} {:>9} | {:>9} {:>9} | {:>9} {:>9}",
        "instr/row", "push", "enforce", "ingest", "enforce", "seal/push", "enforce", "seal/tick"
    );
    for (case, (label, schema, tier, row, eff_per_row)) in arms.into_iter().enumerate() {
        for per in [1, 1000] {
            // Rows between two seals; `None` is the table no view scans.
            let cells = [None, Some(per), Some(TICK_ROWS)].map(|tick| {
                let path = dir.path().join(format!("{case}-{per}-{tick:?}"));
                let mut table = new_table(path, schema, RecoverySource::SalReplay, tier);
                for b in pushes(&schema, 1000, |s| (s, 1, 0)) {
                    table.ingest_owned_batch(b).unwrap();
                }
                let spilled = tier == SPILL_TIER;
                assert_eq!(!table.all_shard_arcs().is_empty(), spilled, "{label}: held in shards");

                let (mut enforce, mut store, mut eff_rows) = (0, 0, 0);
                for (push, b) in (1..).zip(pushes(&schema, per, row)) {
                    let (eff, e) = counter.measure(|| enforce_unique_pk(&table, b));
                    eff_rows += eff.len() as u64;
                    let ((), s) = counter.measure(|| match tick {
                        None => table.ingest_owned_batch(eff).unwrap(),
                        Some(rows) => {
                            table.ingest_pending(eff);
                            if push * per % rows == 0 {
                                black_box(table.seal().unwrap());
                            }
                        }
                    });
                    enforce += e;
                    store += s;
                }
                assert_eq!(!table.all_shard_arcs().is_empty(), spilled, "{label}: spilled");
                assert!(
                    per > 1 || eff_rows == ROWS * eff_per_row,
                    "{label}: {eff_rows} effective rows"
                );
                [enforce, store].map(|instructions: u64| instructions as f64 / ROWS as f64)
            });
            let [[e0, s0], [e1, s1], [e2, s2]] = cells;
            println!("{label:<26} {per:>5} | {e0:>9.1} {s0:>9.1} | {e1:>9.1} {s1:>9.1} | {e2:>9.1} {s2:>9.1}");
        }
    }
}
