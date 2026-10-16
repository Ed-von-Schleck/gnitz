//! A delta read, in instructions per row of the band it covers.
//!
//! `cd crates && cargo test -p gnitz-store --release delta_read_bench -- --ignored --nocapture --test-threads=1`

use std::hint::black_box;

use crate::test_support::{cmp_all, fed_view, make_schema_u64_i64, map_of, relation_fixture, rows_spec};
use crate::test_support::{RelationFixture, TID};
use gnitz_expr::{CmpOp, LogicalProgram};
use gnitz_wire::{PkKeys, ReadBound, ReadSink, ReadSpec, TypeCode};
use gnitz_zset::repr::BatchBuilder;
use gnitz_zset::schema::{SchemaColumn, SchemaDescriptor};

/// A fed view over `keys` U64 key columns, then `c0 = id % 512`, `c1 = round`,
/// and `s` where `string`, every other one out of line; its feed holds `rounds`
/// rounds of the ids `0..per_round`, each key column after the first 7.
fn fixture(keys: usize, string: bool, rounds: u64, per_round: u64) -> (RelationFixture, SchemaDescriptor) {
    let c = SchemaColumn::new;
    let mut cols = vec![c(TypeCode::U64, false); keys];
    cols.extend([c(TypeCode::I64, false), c(TypeCode::I64, false)]);
    if string {
        cols.push(c(TypeCode::String, false));
    }
    let pk: Vec<u32> = (0..keys as u32).collect();
    let s = SchemaDescriptor::new(&cols, &pk);
    let mut r = relation_fixture(fed_view(1 << 32), s, &[], []);
    for round in 2..2 + rounds {
        let mut bb = BatchBuilder::new(&s);
        for id in 0..per_round {
            let mut key = vec![7u128; keys];
            key[0] = id as u128;
            bb.begin_row_natives(&key, 1);
            bb.put_u64(id % 512);
            bb.put_u64(round);
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
fn delta_read_bench() {
    let counter = gnitz_foundation::perf::Counter::instructions();
    let cell = |label: &str, r: &RelationFixture, spec: ReadSpec, layout: u64, want: u64, band: u64| {
        // Untimed: the batch pool is warm.
        black_box(r.delta_read(TID, 1, spec.clone(), layout).unwrap());
        let (reply, instructions) = counter.measure(|| r.delta_read(TID, 1, spec, layout).unwrap());
        assert_eq!(reply.len() as u64, want, "{label}: reply rows");
        println!(
            "delta_read_bench {label:<44} {want:>7} rows out {:>8.1} instr/band row",
            instructions as f64 / band as f64
        );
    };
    // `id | c0`: the map drops the round, so a reply's rounds fold to one row an id.
    let slim = || map_of(LogicalProgram::copy_cols(&[1]), &make_schema_u64_i64());
    let slim_layout = make_schema_u64_i64().layout_digest();
    let lt = |keys: usize, lit| cmp_all(&[(CmpOp::Lt, keys as u32, lit)]).to_blob_bytes();
    for (keys, string) in [(1, false), (3, false), (4, false), (1, true), (3, true)] {
        for (rounds, per_round) in [(1u64, 262144u64), (64, 4096), (4096, 64)] {
            let (r, s) = fixture(keys, string, rounds, per_round);
            let own = s.layout_digest();
            let band = rounds * per_round;
            // The band's rows whose `c0` is below `n`, and those whose `id` is.
            let c0_below = |n: u64| rounds * (0..per_round).filter(|id| id % 512 < n).count() as u64;
            let below = |n: u64| rounds * per_round.min(n);
            let shape = format!("{keys} keys{}, {rounds} rounds", if string { " + string" } else { "" });
            let filtered = |predicate| ReadSpec {
                predicate,
                ..ReadSpec::all_rows(ReadBound::None)
            };
            // One round whole is the captured batch itself, which costs nothing.
            if rounds > 1 {
                cell(&format!("{shape}, every row"), &r, filtered(vec![]), own, band, band);
            }
            for n in [256, 1] {
                let label = format!("{shape}, c0 < {n}");
                cell(&label, &r, filtered(lt(keys, n as i64)), own, c0_below(n), band);
            }
            if keys > 1 || string {
                continue;
            }
            cell(
                &format!("{shape}, c0 alone"),
                &r,
                rows_spec(slim(), None),
                slim_layout,
                per_round,
                band,
            );
            let keyed = |n: u128, predicate| ReadSpec {
                bound: ReadBound::PkSet(PkKeys::from_images(8, 0..n)),
                predicate,
                sink: ReadSink::all_rows(),
            };
            cell(&format!("{shape}, one key"), &r, keyed(1, vec![]), own, below(1), band);
            cell(
                &format!("{shape}, 64 keys"),
                &r,
                keyed(64, vec![]),
                own,
                below(64),
                band,
            );
            let both = keyed(64, lt(keys, 256));
            cell(&format!("{shape}, 64 keys, c0 < 256"), &r, both, own, below(64), band);
        }
    }
    // A read's fixed cost: one round of one row under a predicate and a map.
    let (r, _) = fixture(1, false, 1, 1);
    let spec = ReadSpec {
        predicate: lt(1, 256),
        ..rows_spec(slim(), None)
    };
    const CALLS: usize = 1000;
    let specs = vec![spec; CALLS];
    black_box(r.delta_read(TID, 1, specs[0].clone(), slim_layout).unwrap());
    let read = |spec| r.delta_read(TID, 1, spec, slim_layout).unwrap().len();
    let (out, instructions) = counter.measure(|| specs.into_iter().map(read).sum::<usize>());
    assert_eq!(out, CALLS);
    println!(
        "delta_read_bench one row, predicate + map {:>8.1} instr/call",
        instructions as f64 / CALLS as f64
    );
}
