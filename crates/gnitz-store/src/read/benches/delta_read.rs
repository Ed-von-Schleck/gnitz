//! A delta read, in instructions per row of the band it covers.
//!
//! `cd crates && cargo test -p gnitz-store --release delta_read_bench -- --ignored --nocapture --test-threads=1`
//!
//! A fixture ingests one batch per round, which the delta store holds as one run
//! each, so a band of more than one round is a merge.

use std::hint::black_box;

use crate::relation::RelationKind;
use crate::test_support::{cmp_const, map_of, relation_fixture, RelationFixture, TID};
use gnitz_expr::{CmpOp, LogicalProgram, SchemaFacts};
use gnitz_wire::{PkKeys, ReadBound, ReadSink, ReadSpec, SinkKind, TypeCode, ViewProps};
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
    let kind = RelationKind::View(ViewProps::Fed {
        delta_bytes: std::num::NonZeroU64::new(1 << 32).unwrap(),
    });
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
