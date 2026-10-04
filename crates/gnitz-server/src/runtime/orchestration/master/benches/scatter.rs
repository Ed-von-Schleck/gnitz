use super::tests::holders_of;
use super::*;
use crate::runtime::sal::fixtures::TestLog;
use crate::runtime::sal::DirectGroup;
use crate::test_support::{make_schema_pk_u64_payload_string, make_schema_u64_i64};
use gnitz_foundation::perf::Counter;
use gnitz_zset::repr::BatchBuilder;
use gnitz_zset::schema::{encode_schema_block, Placement};

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
        let log = TestLog::new(16 << 20, nw, 1);
        // One group on an empty SAL; the workers sent rows of their own.
        let push = || {
            let mut excl = log.excl();
            excl.checkpoint_reset();
            counter.measure(|| {
                let scope = excl.begin("bench");
                let own = with_routed(&batch, placement, nw, |data| {
                    scope
                        .write(&DirectGroup::push(16, &record, data, holders_of(&data)), zoned)
                        .expect("group fits");
                    match data {
                        GroupData::Same(_) => None,
                        GroupData::Each(each) => Some(each.iter().flatten().count()),
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
