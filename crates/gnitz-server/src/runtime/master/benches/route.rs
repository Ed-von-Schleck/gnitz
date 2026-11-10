use super::tests::{pk_set, registry};
use super::*;
use crate::runtime::sal::fixtures::{scan, test_log};
use crate::test_support::{make_schema_pk_u64_payload_string, make_schema_u64_i64, pk_only_schema};
use gnitz_foundation::perf::Counter;
use gnitz_store::relation::PkRule;
use gnitz_wire::{ReadBound, ReadSpec, TypeCode};
use gnitz_zset::algebra::Placement;
use gnitz_zset::repr::BatchBuilder;
use gnitz_zset::schema::encode_schema_block;

#[derive(Clone, Copy, Debug)]
enum Cell {
    Int,
    Inline,
    Heap,
}

/// What the master pays to route one pushed batch, lay it out in a scope and
/// commit it, in instructions per row and SAL bytes. A replicated batch is one
/// payload at every worker count, so the worker count varies only where each
/// worker is sent its own rows; a stream's rows are laid out outside the zone.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn push_group_layout_bench() {
    // (keyed, payload cell, rows, workers, zoned)
    let cases = [
        (false, Cell::Int, 50_000, 4, true),
        (false, Cell::Heap, 50_000, 4, true),
        (false, Cell::Int, 1, 4, true),
        (true, Cell::Int, 50_000, 4, true),
        (true, Cell::Int, 50_000, 4, false),
        (true, Cell::Inline, 50_000, 4, true),
        (true, Cell::Heap, 50_000, 4, true),
        (true, Cell::Int, 100, 4, true),
        (true, Cell::Int, 100, 16, true),
        (true, Cell::Int, 1, 4, true),
    ];

    let counter = Counter::instructions();
    for (keyed, cell, rows, nw, zoned) in cases {
        let schema = match cell {
            Cell::Int => make_schema_u64_i64(),
            Cell::Inline | Cell::Heap => make_schema_pk_u64_payload_string(),
        };
        let mut bb = BatchBuilder::new(&schema);
        for pk in 0..rows as u128 {
            bb.begin_row(pk, 1);
            match cell {
                Cell::Int => bb.put_int(pk),
                Cell::Inline => bb.put_string(&format!("s{pk:011}")),
                Cell::Heap => bb.put_string(&format!("a string past the inline prefix {pk:011}")),
            }
            bb.end_row();
        }
        let batch = bb.finish();
        let placement = if keyed {
            Placement::full_pk(&schema)
        } else {
            Placement::Replicated
        };
        let record = encode_schema_block(&schema);
        let log = test_log(16 << 20, nw, 1);
        // One group on an empty SAL; the workers sent rows of their own.
        let push = || {
            let mut excl = log.excl();
            excl.checkpoint_reset();
            counter.measure(|| {
                let mut scope = excl.begin();
                let own = with_routed(batch.as_mem_batch(), placement, nw, |rows| {
                    scope
                        .push(16, PkRule::Enforce, &record, rows, zoned)
                        .expect("group fits");
                    match rows {
                        Routed::Same(_) => None,
                        Routed::Each(each) => Some(each.iter().flatten().count()),
                    }
                });
                scope.commit();
                own
            })
        };
        push(); // the scatter's row lists and the SAL's pages are taken once
        let (own, instructions) = push();
        assert_eq!(own, keyed.then_some(nw.min(rows)), "{cell:?} keyed={keyed} nw={nw}");
        let layout = if keyed { "each" } else { "shared" };
        let zone = if zoned { "zoned" } else { "unzoned" };
        println!(
            "push_group_layout_bench nw={nw:<2} {layout:<6} {zone:<7} {:<6} {rows:>5} rows {:>8.1} instr/row {:>8} SAL bytes",
            format!("{cell:?}"),
            instructions as f64 / rows as f64,
            log.cursor(),
        );
    }
}

/// What the master pays to split one own-PK probe of ascending `u64` keys among
/// the workers that own them, in instructions per key.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn route_read_pkset_bench() {
    const KEYS: u64 = 100_000;
    let schema = pk_only_schema(&[TypeCode::U64]);
    let registry = registry(schema, Placement::full_pk(&schema));
    let blob = ReadSpec::all_rows(pk_set(8, &(0..KEYS).collect::<Vec<_>>())).encode();
    let read = scan(16, &blob);
    let counter = Counter::instructions();
    for nw in [4usize, 16] {
        std::hint::black_box(route_read(&read, &registry, nw));
        let (route, instructions) = counter.measure(|| route_read(std::hint::black_box(&read), &registry, nw));
        assert_eq!(route.set.len(), nw, "the keys spread over every worker");
        std::hint::black_box(route);
        println!(
            "route_read_pkset_bench nw={nw:<2} {KEYS} keys {:>6.1} instr/key",
            instructions as f64 / KEYS as f64
        );
    }
}

/// What the master pays to route one small read, in instructions per read: a
/// scan of the whole relation, and scans bounded by a PK set of one key, which
/// the placement's point query routes, and of eight, which is split among its
/// owners.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn route_read_small_bench() {
    const READS: u64 = 10_000;
    let schema = pk_only_schema(&[TypeCode::U64]);
    let registry = registry(schema, Placement::full_pk(&schema));
    let keys = |n: u64| ReadSpec::all_rows(pk_set(8, &(0..n).collect::<Vec<_>>())).encode();
    let counter = Counter::instructions();
    let whole = ReadSpec::all_rows(ReadBound::None).encode();
    for (name, blob) in [("whole", whole), ("1 key", keys(1)), ("8 keys", keys(8))] {
        let read = scan(16, &blob);
        let run = || {
            counter.measure(|| {
                for _ in 0..READS {
                    std::hint::black_box(route_read(std::hint::black_box(&read), &registry, 4));
                }
            })
        };
        run();
        let ((), instructions) = run();
        println!(
            "route_read_small_bench nw=4 {name:<6} {:>7.1} instr/read",
            instructions as f64 / READS as f64
        );
    }
}
