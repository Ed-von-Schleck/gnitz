use super::*;
use crate::runtime::sal::fixtures::{group_at, TestLog};
use crate::runtime::sal::DirectGroup;
use crate::runtime::wire::decode_sal_slot;
use crate::test_support::{
    make_batch, make_batch_bytes_raw, make_batch_raw, make_schema_pk_u64_payload_string, make_schema_u64_i64,
    weighted_rows, zset_of,
};
use gnitz_store::schema::{Placement, SchemaColumn, SchemaDescriptor};
use gnitz_wire::TypeCode;
use std::collections::HashMap;

/// Write `batch` to `log` as the master's push of it to relation 16, handing
/// `inspect` the layout the scatter picked.
fn push(log: &TestLog, batch: &Batch, inspect: impl FnOnce(&GroupData)) {
    let relation = WireSchema::encoded(16, *batch.schema());
    with_routed(batch, &relation, log.writer.num_workers(), |_, data| {
        inspect(&data);
        log.excl().write(&DirectGroup::push(&relation, data, 0))
    })
    .expect("group fits");
}

/// A keyed push's slots sum to the pushed rows, each of a replicated push's
/// slots holds its live rows, and only a slot with rows carries a schema block.
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
    let replicated = make_schema_u64_i64().with_placement(Placement::Replicated);
    type LaidOut = fn(&GroupData, &Batch) -> bool;
    let cases: [(&str, Batch, LaidOut); 6] = [
        (
            "keyed single row: one-copy scatter, three rowless slots",
            make_batch(&make_schema_u64_i64(), &[(1, 1, 10)]),
            |g, _| matches!(g, GroupData::Scattered { .. }),
        ),
        (
            "keyed fixed-width: one-copy scatter",
            make_batch_raw(&narrow, &[(0, 1, 0), (1, 1, 1), (2, 1, 2)]),
            |g, _| matches!(g, GroupData::Scattered { .. }),
        ),
        (
            "keyed inline strings: one-copy scatter",
            make_batch_bytes_raw(&string, &[(1, 1, b"a"), (2, 3, b"twelve bytes"), (3, 1, b"")]),
            |g, _| matches!(g, GroupData::Scattered { .. }),
        ),
        (
            "keyed heap strings: a sub-batch per worker",
            make_batch_bytes_raw(
                &string,
                &long
                    .iter()
                    .enumerate()
                    .map(|(pk, s)| (pk as u64, 1, s.as_bytes()))
                    .collect::<Vec<_>>(),
            ),
            |g, _| matches!(g, GroupData::Batches(b) if b.len() == 4),
        ),
        (
            "replicated live rows: the batch itself",
            make_batch(&replicated, &[(1, 1, 10), (2, 2, 20), (3, -1, 30)]),
            |g, batch| matches!(g, GroupData::Same(WireData::Whole(b)) if std::ptr::eq(*b, batch)),
        ),
        (
            "replicated with a weight-0 row: one whole batch",
            make_batch_bytes_raw(
                &string.with_placement(Placement::Replicated),
                &[(1, 1, long[0].as_bytes()), (2, 0, b"dropped"), (3, 2, b"c")],
            ),
            |g, _| matches!(g, GroupData::Same(WireData::Whole(_))),
        ),
    ];

    for (case, batch, laid_out) in &cases {
        let schema = *batch.schema();
        let log = TestLog::new(1 << 20, nw, 1);
        push(&log, batch, |g| assert!(laid_out(g, batch), "{case}"));
        let slots: Vec<Batch> = group_at(log.log(), 0)
            .slots_written()
            .filter_map(|(w, bytes)| {
                let decoded = decode_sal_slot(bytes, |_, _| None).expect("every written slot decodes");
                let rows = decoded.data_batch.filter(|b| !b.is_empty());
                assert_eq!(
                    decoded.schema.is_some(),
                    rows.is_some(),
                    "{case}: slot {w}'s schema block"
                );
                rows
            })
            .collect();
        if schema.placement() == Placement::Replicated {
            let live = weighted_rows(batch)
                .into_iter()
                .filter(|&(_, w)| w != 0)
                .collect::<Vec<_>>();
            assert_eq!(slots.len(), nw, "{case}: every worker is sent the rows");
            assert!(
                slots.iter().all(|s| weighted_rows(s) == live),
                "{case}: every worker holds the live rows"
            );
        } else {
            let mut z = HashMap::new();
            for (k, w) in slots.iter().flat_map(|s| zset_of(s, &schema)) {
                *z.entry(k).or_insert(0) += w;
            }
            assert_eq!(z, zset_of(batch, &schema), "{case}: the slots sum to the pushed rows");
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
    use gnitz_store::storage::BatchBuilder;
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
        ("replicated (U64, I64)", fixed.with_placement(Placement::Replicated)),
        (
            "replicated (U64, String <= 12 B)",
            string.with_placement(Placement::Replicated),
        ),
        ("keyed (U64, String <= 12 B)", string),
    ];

    let log = TestLog::new(64 << 20, NW, 1);
    for (name, schema) in cases {
        let batch = build(schema);
        let start = Instant::now();
        for _ in 0..ITERS {
            log.seek(0);
            push(&log, &batch, |_| ());
            black_box(log.cursor());
        }
        let per = start.elapsed() / ITERS;
        eprintln!("push_group_layout_bench {name}: {per:?}/push of {ROWS} rows at NW={NW}");
    }
}
