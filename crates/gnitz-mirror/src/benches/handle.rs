//! A copy with and without an index on one payload column: what a round costs
//! to apply, and what a range read of a few rows costs to answer, in
//! instructions.
//!
//! `cd crates && cargo test -p gnitz-mirror --release copy_index_bench -- --ignored --nocapture --test-threads=1`

use std::sync::Arc;

use gnitz_core::{RelDescriptor, Schema};
use gnitz_foundation::perf::Counter;
use gnitz_wire::{Cut, KeyRange, ReadBound, ReadSpec, RelClass, RelIndex, TypeCode};
use gnitz_zset::schema::encode_schema_block;
use gnitz_zset_testkit::{encode_to_wire_vec, make_batch, make_schema_u64_i64};

use super::*;

/// Rows the copy starts with, at `v = id`.
const ROWS: u64 = 200_000;
/// Updates a round carries, less the rows it names twice: each retracts a row
/// and inserts it at another `v`.
const ROUND: u64 = 1000;
const ROUNDS: u64 = 100;
const READS: u64 = 1000;
/// Rows a read's range of `v` selects.
const SELECTED: u64 = 16;

/// 40 scattered bits.
fn scatter(seq: u64) -> u64 {
    seq.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 24
}

#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn copy_index_bench() {
    const TID: u64 = gnitz_wire::FIRST_USER_TABLE_ID;
    let layout = make_schema_u64_i64();
    let schema = Schema::from_block(&encode_schema_block(&layout)).unwrap();
    let block = |rows: &[(u64, i64, i64)]| encode_to_wire_vec(&make_batch(&layout, rows));
    let cursor = |tick| DeltaCursor::from_pair(7, tick).unwrap();
    let counter = Counter::instructions();
    for indexed in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let mut store = Mirror::open(dir.path().to_str().unwrap(), MirrorConfig::default()).unwrap();
        let index = RelIndex {
            cols: PkColList::from_slice(&[1]),
            is_unique: false,
        };
        let desc = RelDescriptor {
            tid: TID,
            class: RelClass::FedView,
            pk_repeats: false,
            serial: false,
            schema: Arc::new(schema.clone()),
            indexes: indexed.then_some(index).into_iter().collect(),
            token: 1,
        };
        store.register("s", "v", &desc).unwrap();
        let mut refill = store.refill(TID).unwrap();
        let held: Vec<_> = (0..ROWS).map(|id| (id, 1, id as i64)).collect();
        for chunk in held.chunks(10_000) {
            refill.block(&block(chunk)).unwrap();
        }
        refill.seal(cursor(1)).unwrap();

        // Each round moves `ROUND` rows of the upper half to a `v` past every
        // held one.
        let mut v: Vec<i64> = (0..ROWS).map(|id| id as i64).collect();
        let rounds: Vec<Vec<u8>> = (0..ROUNDS)
            .map(|r| {
                let upper = |i| ROWS / 2 + scatter(r * ROUND + i) % (ROWS / 2);
                let mut ids: Vec<u64> = (0..ROUND).map(upper).collect();
                ids.sort_unstable();
                ids.dedup();
                let rows: Vec<_> = ids
                    .into_iter()
                    .flat_map(|id| {
                        let (old, new) = (v[id as usize], (ROWS * (r + 2) + id) as i64);
                        v[id as usize] = new;
                        [(id, -1, old), (id, 1, new)]
                    })
                    .collect();
                block(&rows)
            })
            .collect();
        let applied = ROUNDS * ROUND;
        let ((), apply) = counter.measure(|| {
            for (tick, round) in (2..).zip(&rounds) {
                store.advance(TID, &[round], cursor(tick)).unwrap();
            }
        });

        // Ranges of `v` in the lower half, which no round moved.
        let image = |v: u64| gnitz_wire::key_image(TypeCode::I64, v as u128);
        let specs: Vec<ReadSpec> = (0..READS)
            .map(|i| i * (ROWS / 2 - SELECTED) / READS)
            .map(|lo| {
                let cols = PkColList::from_slice(&[1]);
                let range = KeyRange::new(cols, &[], Cut::before(image(lo)), Cut::before(image(lo + SELECTED)));
                ReadSpec::all_rows(ReadBound::Range(range))
            })
            .collect();
        let (rows, read) = counter.measure(|| {
            specs
                .into_iter()
                .map(|spec| store.scan_spec(TID, spec, &schema).unwrap().weights.len() as u64)
                .sum::<u64>()
        });
        assert_eq!(rows, READS * SELECTED, "every read answers its span");
        println!(
            "copy_index_bench {:<10} {:>7.0} instr/applied update {:>10.0} instr/read of {SELECTED} rows",
            if indexed { "indexed" } else { "unindexed" },
            apply as f64 / applied as f64,
            read as f64 / READS as f64,
        );
    }
}
