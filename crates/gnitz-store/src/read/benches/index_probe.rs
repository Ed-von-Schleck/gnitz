//! An index probe over ascending keys, in instructions per probed key.
//!
//! `cd crates && cargo test -p gnitz-store --release index_probe_bench -- --ignored --nocapture --test-threads=1`

use std::hint::black_box;
use std::num::NonZeroU64;

use crate::relation::RelationKind;
use crate::test_support::{make_batch_raw, make_schema_u64_i64, relation_fixture, RelationFixture, TID};
use gnitz_wire::{PkColList, Probe};
use gnitz_zset::repr::Batch;

/// Sorted runs the index cursor merges (one ingest round each).
const INGEST_ROUNDS: u64 = 8;
/// Rows the table holds.
const ROWS: u64 = 400_000;

/// A `(id U64 PK | val I64)` table of [`ROWS`] rows at `val = val_of(id)`,
/// indexed on `val`.
fn fixture(val_of: impl Fn(u64) -> i64) -> RelationFixture {
    let schema = make_schema_u64_i64();
    let mut r = relation_fixture(RelationKind::BaseTable, schema, &[1], Batch::empty_with_schema(&schema));
    for round in 0..INGEST_ROUNDS {
        let rows: Vec<_> = (round..ROWS)
            .step_by(INGEST_ROUNDS as usize)
            .map(|id| (id, 1, val_of(id)))
            .collect();
        r.ingest(TID, make_batch_raw(&schema, &rows)).unwrap();
    }
    r
}

/// The probe keys of `vals`, ascending: each value's span.
fn probe_keys(r: &RelationFixture, vals: impl Iterator<Item = i64>) -> Batch {
    let schema = make_schema_u64_i64();
    let ix = r.relation(TID).unwrap().index_on(&[1]).unwrap();
    let (spec, ix_schema) = (ix.key_spec(), ix.schema());
    let rows: Vec<_> = vals.map(|v| (0, 1, v)).collect();
    let entries = gnitz_zset::algebra::index_entries(&make_batch_raw(&schema, &rows), &spec, &ix_schema);
    let mut keys: Vec<Vec<u8>> = (0..entries.len())
        .map(|i| entries.get_pk_bytes(i)[..spec.key_size()].to_vec())
        .collect();
    keys.sort_unstable();
    keys.dedup();
    let mut batch = Batch::with_capacity(&spec.span_schema(), keys.len());
    for key in &keys {
        batch.push_key_row(key, 1);
    }
    batch
}

#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn index_probe_bench() {
    let counter = gnitz_foundation::perf::Counter::instructions().expect("instructions counter");
    let cols = PkColList::from_slice(&[1]);
    let cell = |label: &str, r: &RelationFixture, keys: &Batch, cap: u64, want: usize| {
        let probe = Probe::Index(cols, NonZeroU64::new(cap).unwrap());
        // Untimed: the batch pool and the cursor's sources are warm.
        black_box(r.probe(TID, probe, keys).unwrap());
        let (reply, instructions) = counter.measure(|| r.probe(TID, probe, keys).unwrap());
        assert_eq!(reply.len(), want, "{label}: reply entries");
        println!(
            "index_probe_bench {label:<28} {:>8} keys {:>8} entries {:>9.1} instr/key",
            keys.len(),
            reply.len(),
            instructions as f64 / keys.len() as f64
        );
    };

    // One holder per value, every value probed.
    let dense = fixture(|id| id as i64);
    let keys = probe_keys(&dense, (0..ROWS).map(|v| v as i64));
    cell("dense hits, cap 1", &dense, &keys, 1, ROWS as usize);

    // The even values held, the odd ones probed: every miss lands below the
    // next key.
    let even = fixture(|id| id as i64 * 2);
    let keys = probe_keys(&even, (0..ROWS).map(|v| v as i64 * 2 + 1));
    cell("lone misses, cap 1", &even, &keys, 1, 0);

    // Every sixteenth value held, the fifteen between probed: a miss lands past
    // the keys that follow it.
    let sparse = fixture(|id| id as i64 * 16);
    let keys = probe_keys(&sparse, (0..ROWS).filter(|v| v % 16 != 0).map(|v| v as i64));
    cell("runs of misses, cap 1", &sparse, &keys, 1, 0);

    // Four holders per value, every value probed.
    let groups = fixture(|id| (id / 4) as i64);
    let keys = probe_keys(&groups, (0..ROWS / 4).map(|v| v as i64));
    cell("4-holder groups, cap 8", &groups, &keys, 8, ROWS as usize);
}
