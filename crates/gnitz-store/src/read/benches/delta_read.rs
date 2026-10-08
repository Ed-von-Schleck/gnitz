//! A delta read, in instructions per row of the band it covers.
//!
//! `cd crates && cargo test -p gnitz-store --release delta_read_bench -- --ignored --nocapture --test-threads=1`
//!
//! A fixture ingests one batch per round, which the delta store folds into runs
//! of ascending rounds, so a band of more than one round is a chain of them.

use std::hint::black_box;

use crate::test_support::{cmp_const, fed_view, map_of, relation_fixture, RelationFixture, TID};
use gnitz_expr::{CmpOp, LogicalProgram, SchemaFacts};
use gnitz_wire::{PkKeys, ReadBound, ReadSink, ReadSpec, SinkKind, TypeCode};
use gnitz_zset::repr::BatchBuilder;
use gnitz_zset::schema::{SchemaColumn, SchemaDescriptor};

/// `id U64 PK` and `payload` I64 columns.
fn schema(payload: usize) -> SchemaDescriptor {
    let mut cols = vec![SchemaColumn::new(TypeCode::U64, false)];
    cols.extend((0..payload).map(|_| SchemaColumn::new(TypeCode::I64, false)));
    SchemaDescriptor::new(&cols, &[0])
}

/// A fed view `id | c0 = id % 512 | c1 = round | c2 = id` whose feed holds
/// `rounds` rounds, each the ids `0..per_round`.
fn fixture(rounds: u64, per_round: u64) -> RelationFixture {
    let s = schema(3);
    let kind = fed_view(1 << 32);
    let mut r = relation_fixture(kind, s, &[], []);
    for round in 2..2 + rounds {
        let mut bb = BatchBuilder::new(&s);
        for id in 0..per_round {
            bb.begin_row(id as u128, 1);
            bb.put_u64(id % 512);
            bb.put_u64(round);
            bb.put_u64(id);
            bb.end_row();
        }
        r.ingest_at(TID, bb.finish(), Some(round), false).unwrap();
    }
    r
}

#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn delta_read_bench() {
    let counter = gnitz_foundation::perf::Counter::instructions();
    let rows = |map| ReadSink { map, kind: SinkKind::Rows { cut: None } };
    let slim = || map_of(LogicalProgram::copy_cols(&[3]), &schema(1));
    let lt = |lit| cmp_const(CmpOp::Lt, 1, lit).to_blob_bytes();
    let keys = |n: u64| {
        let ks: Vec<[u8; 8]> = (0..n).map(|i| i.to_be_bytes()).collect();
        ReadBound::PkSet(PkKeys::from_keys(8, ks.iter().map(|k| k.as_slice())))
    };
    let spec = |bound, predicate, sink| ReadSpec { bound, predicate, sink };
    for (rounds, per_round) in [(1u64, 262144u64), (64, 4096), (4096, 64)] {
        let r = fixture(rounds, per_round);
        let band = rounds * per_round;
        let cut = 1 + rounds;
        println!("--- {rounds} rounds of {per_round} rows");
        let cells: Vec<(&str, ReadSpec, usize)> = vec![
            ("the view's rows", ReadSpec::all_rows(ReadBound::None), 3),
            (
                "c0 < 256, one row in two",
                spec(ReadBound::None, lt(256), rows(None)),
                3,
            ),
            ("c0 < 1, one row in 512", spec(ReadBound::None, lt(1), rows(None)), 3),
            ("one column", spec(ReadBound::None, vec![], rows(slim())), 1),
            ("one key", spec(keys(1), vec![], rows(None)), 3),
            ("64 keys", spec(keys(64), vec![], rows(None)), 3),
        ];
        for (label, spec, payload) in cells {
            let layout = schema(payload).layout_digest();
            let run = || r.delta_read(TID, 1, cut, spec.clone(), layout).unwrap();
            black_box(run());
            let (reply, instructions) = counter.measure(run);
            println!(
                "delta_read_bench {label:<28} {:>7} rows out {:>8.1} instr/band row",
                reply.len(),
                instructions as f64 / band as f64
            );
        }
    }
}

/// A fed view over `keys` U64 key columns, then `c0 = id % 512`, then `s` where
/// `string`, every other one out of line; its feed holds `rounds` rounds of the
/// ids `0..per_round`.
fn keys_fixture(keys: usize, string: bool, rounds: u64, per_round: u64) -> (RelationFixture, SchemaDescriptor) {
    let c = SchemaColumn::new;
    let mut cols = vec![c(TypeCode::U64, false); keys];
    cols.push(c(TypeCode::I64, false));
    if string {
        cols.push(c(TypeCode::String, false));
    }
    let pk: Vec<u32> = (0..keys as u32).collect();
    let s = SchemaDescriptor::new(&cols, &pk);
    let kind = fed_view(1 << 32);
    let mut r = relation_fixture(kind, s, &[], []);
    for round in 2..2 + rounds {
        let mut bb = BatchBuilder::new(&s);
        for id in 0..per_round {
            let mut key = vec![7u128; keys];
            key[0] = id as u128;
            bb.begin_row_natives(&key, 1);
            bb.put_u64(id % 512);
            if string {
                match id % 2 {
                    0 => bb.put_string(&format!("a-fairly-long-out-of-line-value-{id}")),
                    _ => bb.put_string("short"),
                }
            }
            bb.end_row();
        }
        r.ingest_at(TID, bb.finish(), Some(round), false).unwrap();
    }
    (r, s)
}

#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn delta_read_keys_bench() {
    let counter = gnitz_foundation::perf::Counter::instructions();
    for (keys, string) in [(1, false), (3, false), (4, false), (1, true), (3, true)] {
        let lt = |lit| cmp_const(CmpOp::Lt, keys as u32, lit).to_blob_bytes();
        for (rounds, per_round) in [(1u64, 262144u64), (64, 4096)] {
            let (r, s) = keys_fixture(keys, string, rounds, per_round);
            let band = rounds * per_round;
            for (label, predicate) in [("whole", vec![]), ("half", lt(256)), ("1/512", lt(1))] {
                let spec = ReadSpec {
                    predicate,
                    ..ReadSpec::all_rows(ReadBound::None)
                };
                let run = || {
                    r.delta_read(TID, 1, 1 + rounds, spec.clone(), s.layout_digest())
                        .unwrap()
                };
                black_box(run());
                let (reply, instructions) = counter.measure(run);
                println!(
                    "delta_read_keys_bench {keys} keys string={string:<5} {rounds:>2} rounds {label:<6} {:>7} rows out {:>8.1} instr/band row",
                    reply.len(),
                    instructions as f64 / band as f64
                );
            }
        }
    }
}
