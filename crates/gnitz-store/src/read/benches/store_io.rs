//! A probe over ascending keys, in instructions per probed key.
//!
//! `cd crates && cargo test -p gnitz-store --release probe_bench -- --ignored --nocapture --test-threads=1`

use std::hint::black_box;
use std::num::NonZeroU64;

use crate::relation::{Cut as At, RelationKind};
use crate::test_support::{
    make_batch_raw, make_schema_u64_i64, pk_only_schema, relation_fixture, RelationFixture, TID,
};
use gnitz_wire::{PkColList, Probe, TypeCode};
use gnitz_zset::repr::{Batch, BatchBuilder};
use gnitz_zset::schema::SchemaDescriptor;

/// Sorted runs a probe's cursor merges (one ingest round each).
const RUNS: usize = 8;
/// Rows the table holds.
const ROWS: u64 = 400_000;

/// A `(id U64 PK | val I64)` table of [`ROWS`] rows at `val = val_of(id)`,
/// indexed on `val`, its rows [`RUNS`] PK-interleaved runs of the RAM tier.
fn fixture(val_of: impl Fn(u64) -> i64) -> RelationFixture {
    let schema = make_schema_u64_i64();
    let rounds = (0..RUNS as u64).map(|round| {
        let rows: Vec<_> = (round..ROWS).step_by(RUNS).map(|id| (id, 1, val_of(id))).collect();
        make_batch_raw(&schema, &rows)
    });
    let r = relation_fixture(RelationKind::BaseTable, schema, &[1], rounds);
    let table = r.relation(TID).unwrap().table();
    assert_eq!((table.all_shard_arcs().len(), table.runs(At::Now).count()), (0, RUNS));
    r
}

/// The probe keys `vals`, ascending, over `schema`'s one key column.
fn keys(schema: &SchemaDescriptor, vals: impl Iterator<Item = u64>) -> Batch {
    let mut bb = BatchBuilder::new(schema);
    for v in vals {
        bb.begin_row(v as u128, 1);
        bb.end_row();
    }
    bb.finish()
}

#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn probe_bench() {
    let counter = gnitz_foundation::perf::Counter::instructions();
    let cell = |label: &str, r: &RelationFixture, probe: Probe, keys: Batch, want: u64| {
        // Untimed: the batch pool and the cursor's sources are warm.
        black_box(r.probe(TID, probe, &keys).unwrap());
        let (reply, instructions) = counter.measure(|| r.probe(TID, probe, &keys).unwrap());
        assert_eq!(reply.len() as u64, want, "{label}: reply rows");
        println!(
            "probe_bench {label:<28} {:>8} keys {:>8} rows {:>9.1} instr/key",
            keys.len(),
            reply.len(),
            instructions as f64 / keys.len() as f64
        );
    };
    let index = Probe::Index(PkColList::from_slice(&[1]));
    let index_all = |cap| Probe::IndexAll(PkColList::from_slice(&[1]), NonZeroU64::new(cap).unwrap());
    let span = pk_only_schema(&[TypeCode::I64]);
    let pk = pk_only_schema(&[TypeCode::U64]);

    // One holder per value, every sixteenth value held.
    let unique = fixture(|id| id as i64 * 16);
    let held = || (0..ROWS).map(|id| id * 16);
    cell("index, hits", &unique, index, keys(&span, held()), ROWS);
    // The open is the whole cost.
    cell("index, one hit", &unique, index, keys(&span, held().skip(1).take(1)), 1);
    // Every miss lands below the next key.
    let between = keys(&span, held().map(|v| v + 8));
    cell("index, lone misses", &unique, index, between, 0);
    // The fifteen values between two held ones: a miss lands past the keys
    // that follow it.
    let unheld = keys(&span, (0..ROWS).filter(|v| v % 16 != 0));
    cell("index, runs of misses", &unique, index, unheld, 0);
    cell("pk", &unique, Probe::Pk, keys(&pk, 0..ROWS), ROWS);
    // Above every run.
    cell("pk, misses", &unique, Probe::Pk, keys(&pk, ROWS..2 * ROWS), 0);
    cell("pk column", &unique, Probe::PkColumn(1), keys(&pk, 0..ROWS), ROWS);

    // Four holders per value, every value probed: the group ends the walk, then
    // the first entry does, then the reply's cap does.
    let groups = fixture(|id| (id / 4) as i64);
    let vals = || keys(&span, 0..ROWS / 4);
    cell("index, 4 holders, all", &groups, index_all(2 * ROWS), vals(), ROWS);
    cell("index, 4 holders, first", &groups, index, vals(), ROWS / 4);
    cell("index, 4 holders, half", &groups, index_all(ROWS / 2), vals(), ROWS / 2);
}
