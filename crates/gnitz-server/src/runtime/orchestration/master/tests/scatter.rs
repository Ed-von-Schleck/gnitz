use super::*;
use crate::runtime::sal::fixtures::{group_at, TestLog};
use crate::runtime::sal::DirectGroup;
use crate::runtime::wire::{decode_sal_slot, WireSchema};
use crate::test_support::{
    make_batch, make_batch_bytes_raw, make_batch_raw, make_schema_pk_u64_payload_string, make_schema_u64_i64,
    weighted_rows, zset_of,
};
use gnitz_wire::TypeCode;
use gnitz_zset::schema::{Placement, SchemaColumn, SchemaDescriptor};

/// Write `batch` to `log` as the master's push of it to relation 16, placed by
/// `placement`, handing `inspect` the layout the scatter picked.
fn push(log: &TestLog, batch: &Batch, placement: Placement, inspect: impl FnOnce(&GroupData)) {
    let relation = WireSchema::encoded(16, batch.schema());
    with_routed(batch, placement, log.writer.num_workers(), |_, data| {
        inspect(&data);
        log.excl().write(&DirectGroup::push(&relation, data, 0))
    })
    .expect("group fits");
}

/// A keyed push's slots sum to the pushed rows, each row in its owner's slot;
/// each of a replicated push's slots holds its live rows; and only a slot with
/// rows carries a schema block.
#[test]
fn a_push_group_decodes_to_each_workers_rows() {
    let nw = 4;
    let narrow = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U32, false),
            SchemaColumn::new(TypeCode::I32, false),
        ],
        &[0],
    );
    let string = make_schema_pk_u64_payload_string();
    let long: Vec<String> = (0..8)
        .map(|pk| format!("a string past the inline prefix {pk}"))
        .collect();
    let fixed = make_schema_u64_i64();
    let keyed = |b: Batch| (Placement::full_pk(b.schema()), b);
    let replicated = |b: Batch| (Placement::Replicated, b);
    type LaidOut = fn(&GroupData, &Batch) -> bool;
    let cases: [(&str, (Placement, Batch), LaidOut); 6] = [
        (
            "keyed single row: one-copy scatter, three rowless slots",
            keyed(make_batch(&fixed, &[(1, 1, 10)])),
            |g, _| matches!(g, GroupData::Scattered { .. }),
        ),
        (
            "keyed fixed-width: one-copy scatter",
            keyed(make_batch_raw(&narrow, &[(0, 1, 0), (1, 1, 1), (2, 1, 2)])),
            |g, _| matches!(g, GroupData::Scattered { .. }),
        ),
        (
            "keyed inline strings: one-copy scatter",
            keyed(make_batch_bytes_raw(
                &string,
                &[(1, 1, b"a"), (2, 3, b"twelve bytes"), (3, 1, b"")],
            )),
            |g, _| matches!(g, GroupData::Scattered { .. }),
        ),
        (
            "keyed heap strings: a sub-batch per worker",
            keyed(make_batch_bytes_raw(
                &string,
                &long
                    .iter()
                    .enumerate()
                    .map(|(pk, s)| (pk as u64, 1, s.as_bytes()))
                    .collect::<Vec<_>>(),
            )),
            |g, _| matches!(g, GroupData::Batches(b) if b.len() == 4),
        ),
        (
            "replicated live rows: the batch itself",
            replicated(make_batch(&fixed, &[(1, 1, 10), (2, 2, 20), (3, -1, 30)])),
            |g, batch| matches!(g, GroupData::Same(WireData::Whole(b)) if std::ptr::eq(*b, batch)),
        ),
        (
            "replicated with a weight-0 row: one whole batch",
            replicated(make_batch_bytes_raw(
                &string,
                &[(1, 1, long[0].as_bytes()), (2, 0, b"dropped"), (3, 2, b"c")],
            )),
            |g, _| matches!(g, GroupData::Same(WireData::Whole(_))),
        ),
    ];

    for (case, (placement, batch), laid_out) in &cases {
        let schema = *batch.schema();
        let log = TestLog::new(1 << 20, nw, 1);
        push(&log, batch, *placement, |g| assert!(laid_out(g, batch), "{case}"));
        let slots: Vec<(usize, Batch)> = group_at(log.log(), 0)
            .slots_written()
            .filter_map(|(w, bytes)| {
                let decoded = decode_sal_slot(bytes, |_, _| None).expect("every written slot decodes");
                let rows = decoded.data_batch.filter(|b| !b.is_empty());
                assert_eq!(
                    decoded.schema.is_some(),
                    rows.is_some(),
                    "{case}: slot {w}'s schema block"
                );
                rows.map(|rows| (w as usize, rows))
            })
            .collect();
        if placement.is_replicated() {
            let live = weighted_rows(batch)
                .into_iter()
                .filter(|&(_, w)| w != 0)
                .collect::<Vec<_>>();
            assert_eq!(slots.len(), nw, "{case}: every worker is sent the rows");
            assert!(
                slots.iter().all(|(_, s)| weighted_rows(s) == live),
                "{case}: every worker holds the live rows"
            );
        } else {
            for (w, s) in &slots {
                for row in 0..s.len() {
                    assert_eq!(
                        placement.owner(s.get_pk_bytes(row), nw),
                        Some(*w),
                        "{case}: a row in slot {w}"
                    );
                }
            }
            let sent = Batch::concat(&schema, slots.iter().map(|(_, s)| s.as_mem_batch()));
            assert_eq!(
                zset_of(&sent, &schema),
                zset_of(batch, &schema),
                "{case}: the slots sum to the pushed rows"
            );
        }
    }
}

/// The master's encode of one pushed batch into a SAL group, per layout the
/// scatter picks.
///
/// `cd crates && cargo test -p gnitz-server --release push_group_layout_bench -- --ignored --nocapture --test-threads=1`
#[test]
#[ignore]
fn push_group_layout_bench() {
    use gnitz_zset::repr::BatchBuilder;
    use std::hint::black_box;
    use std::time::Instant;

    const NW: usize = 4;
    const ROWS: u128 = 250_000;
    const ITERS: u32 = 50;

    let fixed = make_schema_u64_i64();
    let string = make_schema_pk_u64_payload_string();
    let build = |schema: SchemaDescriptor| {
        let mut bb = BatchBuilder::new(schema);
        for pk in 0..ROWS {
            bb.begin_row(pk, 1);
            if schema.has_german_string() {
                bb.put_string(&format!("s{:011}", pk % 100_000_000_000));
            } else {
                bb.put_int(pk);
            }
            bb.end_row();
        }
        bb.finish()
    };
    let cases = [
        ("replicated (U64, I64)", fixed, Placement::Replicated),
        ("replicated (U64, String <= 12 B)", string, Placement::Replicated),
        ("keyed (U64, String <= 12 B)", string, Placement::full_pk(&string)),
    ];

    let log = TestLog::new(64 << 20, NW, 1);
    for (name, schema, placement) in cases {
        let batch = build(schema);
        let start = Instant::now();
        for _ in 0..ITERS {
            log.seek(0);
            push(&log, &batch, placement, |_| ());
            black_box(log.cursor());
        }
        let per = start.elapsed() / ITERS;
        eprintln!("push_group_layout_bench {name}: {per:?}/push of {ROWS} rows at NW={NW}");
    }
}
