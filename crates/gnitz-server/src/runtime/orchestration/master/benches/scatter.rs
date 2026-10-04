use super::tests::holders_of;
use super::*;
use crate::runtime::sal::fixtures::TestLog;
use crate::runtime::sal::{DirectGroup, GroupTargets};
use crate::test_support::{make_schema_pk_u64_payload_string, make_schema_u64_i64};
use gnitz_zset::schema::encode_schema_block;
use gnitz_zset::schema::Placement;

/// What the master pays to route one group, lay it out in a scope and commit
/// it, per layout the scatter picks and per worker count: instructions retired
/// and SAL bytes.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn push_group_layout_bench() {
    use crate::runtime::sal::Apply;
    use gnitz_foundation::perf::Counter;
    use gnitz_zset::repr::BatchBuilder;
    use std::hint::black_box;

    const ITERS: u64 = 20;

    #[derive(Clone, Copy)]
    enum Cell {
        Int,
        Inline,
        Heap,
    }
    let build = |cell: Cell, rows: u128| {
        let schema = match cell {
            Cell::Int => make_schema_u64_i64(),
            Cell::Inline | Cell::Heap => make_schema_pk_u64_payload_string(),
        };
        let mut bb = BatchBuilder::new(&schema);
        for pk in 0..rows {
            bb.begin_row(pk, 1);
            match cell {
                Cell::Int => bb.put_int(pk),
                Cell::Inline => bb.put_string(&format!("s{pk:011}")),
                Cell::Heap => bb.put_string(&format!("a string past the inline prefix {pk:011}")),
            }
            bb.end_row();
        }
        bb.finish()
    };
    let cases = [
        ("replicated fixed", false, Cell::Int, 50_000),
        ("replicated inline string", false, Cell::Inline, 50_000),
        ("replicated heap string", false, Cell::Heap, 50_000),
        ("keyed fixed", true, Cell::Int, 50_000),
        ("keyed inline string", true, Cell::Inline, 50_000),
        ("keyed heap string", true, Cell::Heap, 50_000),
        ("keyed heap string", true, Cell::Heap, 100),
        ("keyed heap string", true, Cell::Heap, 1),
        ("keyed fixed", true, Cell::Int, 1),
    ];

    let counter = Counter::instructions();
    for nw in [1usize, 4, 16] {
        let log = TestLog::new(256 << 20, nw, 1);
        // One scope's write and commit, the `SalExcl` taken and dropped around it.
        let measure = |write: &mut dyn FnMut(&crate::runtime::sal::SalScope)| {
            let (mut total, mut bytes) = (0, 0);
            for _ in 0..ITERS {
                let mut excl = log.excl();
                excl.checkpoint_reset();
                let ((), n) = counter.measure(|| {
                    let scope = excl.begin("bench");
                    write(&scope);
                    black_box(scope.commit());
                });
                total += n;
                bytes = log.cursor();
            }
            (total / ITERS, bytes)
        };

        for (name, keyed, cell, rows) in cases {
            let batch = build(cell, rows);
            let placement = if keyed {
                Placement::full_pk(batch.schema())
            } else {
                Placement::Replicated
            };
            let record = encode_schema_block(batch.schema());
            let mut layout = "";
            let (instructions, bytes) = measure(&mut |scope| {
                with_routed(&batch, placement, nw, |data| {
                    layout = match data {
                        GroupData::Same(_) => "Same",
                        GroupData::Each(_) => "Each",
                    };
                    scope.write(&DirectGroup::push(16, &record, data, holders_of(&data)), true)
                })
                .expect("group fits");
            });
            eprintln!(
                "push_group_layout_bench NW={nw:<2} {name}, {rows} rows ({layout}): \
                 {instructions} instructions/push, {bytes} SAL bytes"
            );
        }

        let tids = [16u64];
        let (instructions, bytes) = measure(&mut |scope| {
            let tick = DirectGroup {
                targets: GroupTargets {
                    request_id: 1,
                    ..GroupTargets::UNADDRESSED
                },
                ..DirectGroup::new(Apply::Tick {
                    first_round: 2,
                    tids: gnitz_wire::as_le_bytes(&tids).into(),
                })
            };
            scope.write(&tick, false).expect("group fits");
        });
        eprintln!(
            "push_group_layout_bench NW={nw:<2} one-tid Tick: {instructions} instructions/group, {bytes} SAL bytes"
        );
    }
}
