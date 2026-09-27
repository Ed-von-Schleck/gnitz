//! The base-table write path — `enforce_unique_pk` and the store ingest it feeds
//! — one arm per push shape. Report `ns/row` and instructions per row.

use super::enforce_unique_pk;
use crate::schema::SchemaDescriptor;
use crate::storage::{Batch, BatchBuilder, RecoverySource, StoreBudgets, Table};
use crate::test_rng::Rng;
use crate::test_support::{make_schema_pk_u64_payload_string, make_schema_u64_i64};

/// Rows every arm pushes, however it splits them.
const TOTAL_ROWS: usize = 500_000;
/// Few enough that nearly every push after the first few is an update, many
/// enough that the store spills and a probe crosses tiers.
const HOT_KEYS: u64 = 50_000;

/// `rows_per_push`-row pushes of `row(seq) = (pk, weight, payload)` for `seq` in
/// `0..TOTAL_ROWS`, timed over a store that already holds keys `0..held`.
struct Arm {
    label: &'static str,
    strings: bool,
    held: u64,
    rows_per_push: usize,
    row: Box<dyn FnMut(u64) -> (u64, i64, u64)>,
}

fn arm(label: &'static str, held: u64, rows_per_push: usize, row: impl FnMut(u64) -> (u64, i64, u64) + 'static) -> Arm {
    Arm {
        label,
        strings: false,
        held,
        rows_per_push,
        row: Box::new(row),
    }
}

fn random_key(keys: u64) -> impl FnMut(u64) -> (u64, i64, u64) {
    let mut rng = Rng::new(0x5EED_1234);
    move |seq| (rng.gen_range(keys), 1, seq + 1)
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
            let mut bb = BatchBuilder::new(*schema);
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
    let new_table = |label: &str, schema: SchemaDescriptor| {
        Table::new(
            dir.path().join(label).to_str().unwrap(),
            schema,
            RecoverySource::SalReplay,
            StoreBudgets::default(),
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
        &mut new_table("warm", ints),
        pushes(&ints, false, 0..8000, 1000, |s| (s, 1, s)),
    );

    let mut shuffled: Vec<u64> = (0..TOTAL_ROWS as u64).collect();
    let mut rng = Rng::new(0x5EED_1234);
    for i in (1..shuffled.len()).rev() {
        shuffled.swap(i, rng.gen_range(i as u64 + 1) as usize);
    }
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
    ];

    let counter = gnitz_foundation::perf::Counter::instructions().expect("instructions counter");
    println!(
        "{:>10} {:>10} {:>12} {:>12} {:>12}",
        "arm", "rows", "eff_rows", "ns/row", "instr/row"
    );
    for mut arm in arms {
        let schema = match arm.strings {
            true => make_schema_pk_u64_payload_string(),
            false => ints,
        };
        let mut table = new_table(arm.label, schema);
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
                eff_rows += eff.count;
                table.ingest_borrowed_batch(&eff).unwrap();
            }
        });
        let ns = t.elapsed().as_nanos() as f64;
        black_box(&table);

        let rows = TOTAL_ROWS as f64;
        println!(
            "{:>10} {:>10} {:>12} {:>12.1} {:>12.1}",
            arm.label,
            TOTAL_ROWS,
            eff_rows,
            ns / rows,
            instructions as f64 / rows
        );
    }
}
