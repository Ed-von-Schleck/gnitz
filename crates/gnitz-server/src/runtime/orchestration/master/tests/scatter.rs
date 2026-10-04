use super::*;
use crate::runtime::sal::fixtures::{group_at, TestLog};
use crate::runtime::sal::{DirectGroup, GroupTargets, WorkerSet};
use crate::test_support::{
    make_batch, make_batch_bytes_raw, make_batch_raw, make_schema_pk_u64_payload_string, make_schema_u64_i64,
    weighted_rows, zset_of,
};
use gnitz_wire::control::peek_control_block;
use gnitz_wire::TypeCode;
use gnitz_zset::schema::encode_schema_block;
use gnitz_zset::schema::{Placement, SchemaColumn, SchemaDescriptor};

/// The targets of a push of `data` nothing answers: the workers that hold its rows.
pub(super) fn holders_of(data: &GroupData) -> GroupTargets {
    GroupTargets {
        set: data.holders(),
        ..GroupTargets::UNADDRESSED
    }
}

/// Write `batch` to `log` as the master's push of it to relation 16, placed by
/// `placement`, handing `inspect` the layout the scatter picked.
fn push(log: &TestLog, batch: &Batch, placement: Placement, inspect: impl FnOnce(&GroupData)) {
    let record = encode_schema_block(batch.schema());
    with_routed(batch, placement, log.writer.num_workers(), |data| {
        inspect(&data);
        log.excl()
            .write(&DirectGroup::push(16, &record, data, holders_of(&data)))
    })
    .expect("group fits");
}

/// A keyed push addresses exactly the workers that own its rows, each sent its
/// own; a replicated push sends every worker its live rows as one payload; and
/// only a payload with rows carries a schema block.
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
            "keyed single row: framed from the batch, its owner alone addressed",
            keyed(make_batch(&fixed, &[(1, 1, 10)])),
            |g, _| matches!(g, GroupData::Each(_)),
        ),
        (
            "keyed fixed-width: framed from the batch",
            keyed(make_batch_raw(&narrow, &[(0, 1, 0), (1, 1, 1), (2, 1, 2)])),
            |g, _| matches!(g, GroupData::Each(_)),
        ),
        (
            "keyed inline strings: framed from the batch",
            keyed(make_batch_bytes_raw(
                &string,
                &[(1, 1, b"a"), (2, 3, b"twelve bytes"), (3, 1, b"")],
            )),
            |g, _| matches!(g, GroupData::Each(_)),
        ),
        (
            "keyed heap strings: framed from the batch, one entry per worker",
            keyed(make_batch_bytes_raw(
                &string,
                &long
                    .iter()
                    .enumerate()
                    .map(|(pk, s)| (pk as u64, 1, s.as_bytes()))
                    .collect::<Vec<_>>(),
            )),
            |g, _| matches!(g, GroupData::Each(e) if e.len() == 4),
        ),
        (
            "replicated live rows: the batch itself",
            replicated(make_batch(&fixed, &[(1, 1, 10), (2, 2, 20), (3, -1, 30)])),
            |g, batch| matches!(g, GroupData::Same(Some(d)) if d.rows() == batch.len()),
        ),
        (
            "replicated with a weight-0 row: one whole batch",
            replicated(make_batch_bytes_raw(
                &string,
                &[(1, 1, long[0].as_bytes()), (2, 0, b"dropped"), (3, 2, b"c")],
            )),
            |g, _| matches!(g, GroupData::Same(Some(_))),
        ),
    ];

    for (case, (placement, batch), laid_out) in &cases {
        let schema = *batch.schema();
        let log = TestLog::new(1 << 20, nw, 1);
        push(&log, batch, *placement, |g| assert!(laid_out(g, batch), "{case}"));
        let msg = group_at(log.log(), 0);
        let sent: Vec<(usize, Batch)> = (0..nw)
            .filter_map(|w| {
                let bytes = msg.slot(w as u32)?;
                let control = peek_control_block(bytes).expect("a control block");
                let rows = msg.rows(bytes, |_, _| None).expect("every payload decodes");
                assert_eq!(
                    control.schema.is_some(),
                    rows.is_some(),
                    "{case}: worker {w}'s schema block"
                );
                Some((
                    w,
                    rows.unwrap_or_else(|| panic!("{case}: worker {w} is addressed without rows")),
                ))
            })
            .collect();
        if placement.is_replicated() {
            let live = weighted_rows(batch)
                .into_iter()
                .filter(|&(_, w)| w != 0)
                .collect::<Vec<_>>();
            assert_eq!(sent.len(), nw, "{case}: every worker is sent the rows");
            assert_eq!(msg.payloads().count(), 1, "{case}: as one payload");
            assert!(
                sent.iter().all(|(_, s)| weighted_rows(s) == live),
                "{case}: every worker holds the live rows"
            );
        } else {
            let owners = (0..batch.len())
                .map(|row| placement.owner(batch.get_pk_bytes(row), nw).expect("a keyed placement"))
                .fold(WorkerSet::EMPTY, WorkerSet::with);
            assert_eq!(msg.targets, owners, "{case}: the owners alone are addressed");
            assert_eq!(msg.payloads().count(), owners.len(), "{case}: one payload each");
            for (w, s) in &sent {
                for row in 0..s.len() {
                    assert_eq!(
                        placement.owner(s.get_pk_bytes(row), nw),
                        Some(*w),
                        "{case}: a row sent to worker {w}"
                    );
                }
            }
            let all = Batch::concat(&schema, sent.iter().map(|(_, s)| s.as_mem_batch()));
            assert_eq!(
                zset_of(&all, &schema),
                zset_of(batch, &schema),
                "{case}: the payloads sum to the pushed rows"
            );
        }
    }
}

/// A keyed push of rows that name one heap span — a join view's rows, pushed
/// back by a client — sends each worker that span once: no payload's heap
/// outgrows the pushed one.
#[test]
fn a_keyed_push_of_rows_sharing_a_span_keeps_each_heap_within_the_pushed_one() {
    let (nw, rows) = (4, 20u64);
    let schema = make_schema_pk_u64_payload_string();
    let heap = vec![b'v'; 200];
    let mut cell = gnitz_wire::encode_german_string(&heap, &mut Vec::new());
    gnitz_wire::write_u64_le(&mut cell, 8, 0);
    let pks: Vec<u8> = (0..rows).flat_map(u64::to_be_bytes).collect();
    let weights: Vec<u8> = (0..rows).flat_map(|_| 1i64.to_le_bytes()).collect();
    let nulls = vec![0u8; 8 * rows as usize];
    let cells = cell.repeat(rows as usize);
    let mut block = Vec::new();
    gnitz_wire::wal::append_block(&[&pks, &weights, &nulls, &cells, &heap], 0, &mut block);
    let batch = Batch::decode_foreign_wal_block(&block, &schema).expect("a canonical block");
    assert_eq!(batch.blob().len(), heap.len());

    let placement = Placement::full_pk(&schema);
    let log = TestLog::new(1 << 20, nw, 1);
    push(&log, &batch, placement, |g| {
        assert!(matches!(g, GroupData::Each(e) if e.len() == nw))
    });
    let msg = group_at(log.log(), 0);
    let sent: Vec<Batch> = (0..nw)
        .filter_map(|w| {
            let bytes = msg.slot(w as u32)?;
            msg.rows(bytes, |_, _| None).expect("every payload decodes")
        })
        .collect();
    assert!(
        sent.iter().any(|s| s.len() > 1),
        "precondition: a worker owns several rows"
    );
    for s in &sent {
        assert_eq!(
            s.blob().len(),
            heap.len(),
            "a worker's {} rows carry the span once",
            s.len()
        );
    }
    let all = Batch::concat(&schema, sent.iter().map(Batch::as_mem_batch));
    assert_eq!(zset_of(&all, &schema), zset_of(&batch, &schema));
}
