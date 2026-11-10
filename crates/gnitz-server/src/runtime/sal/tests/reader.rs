use super::{Inbound, SalMessageKind, SalReader};
use crate::runtime::frame::WireMsg;
use crate::runtime::park::WorkerSet;
use crate::runtime::sal::fixtures::{assert_fail_stops, control_push as to, group_at, scan, test_log, unanswered};
use crate::runtime::sal::format::{payload_encode, payload_rows, payload_size};
use crate::runtime::sal::{Apply, SalExcl};
use crate::runtime::test_support::{assert_child_exited_ok, fork_child};
use crate::runtime::w2m::fixtures::{id, test_rings};
use crate::runtime::w2m::RequestId;
use crate::test_support::{make_batch, make_schema_u64_i64};
use gnitz_zset::schema::encode_schema_block;

/// A reader steps past a `Flush` into the next epoch on its own: what follows
/// in the old epoch is never read, and the next epoch's first group is. A
/// `FlushEph` ends no epoch: the reader stays where the writer does, and reads
/// what is written behind it.
#[test]
fn a_reader_leaves_its_epoch_on_stepping_past_a_flush_alone() {
    let log = test_log(1 << 20, 2, 1);
    let mut reader = log.reader(0);
    let read = |reader: &mut SalReader| {
        reader
            .next()
            .map(|(m, ..)| (m.head.request.kind, m.head.request.target_id))
    };
    let mut excl = log.excl();
    excl.write(&to(1)).expect("group fits");
    excl.apply(Apply::Flush, id(1)).expect("group fits");
    excl.write(&to(2)).expect("group fits");

    assert_eq!(read(&mut reader), Some((SalMessageKind::Push, 1)));
    assert_eq!(read(&mut reader), Some((SalMessageKind::Flush, 0)));
    assert_eq!(read(&mut reader), None, "nothing behind a Flush is read");
    assert!(reader.is_empty(), "the old epoch's groups are leftovers");

    excl.checkpoint_reset();
    excl.write(&to(3)).expect("group fits");
    excl.apply(Apply::FlushEph { generation: 0 }, id(1))
        .expect("group fits");
    excl.write(&to(4)).expect("group fits");

    assert_eq!(read(&mut reader), Some((SalMessageKind::Push, 3)), "the next epoch");
    assert_eq!(read(&mut reader), Some((SalMessageKind::FlushEph, 0)));
    assert_eq!(
        read(&mut reader),
        Some((SalMessageKind::Push, 4)),
        "the group behind a FlushEph, in its epoch"
    );
    assert_eq!(read(&mut reader), None);
    assert_eq!(log.epoch(), 2, "one epoch for the whole checkpoint");
}

/// A worker process parks on the SAL through a whole checkpoint and reports
/// each group it wakes to back over its ring: an ephemeral round, which it
/// stays behind in its epoch, then the next checkpoint's base round, past which
/// it parks at the start of the next epoch until that checkpoint's ephemeral
/// round is written there behind the rewind.
#[test]
fn sal_cross_process_checkpoint() {
    let (mut writers, receiver) = test_rings(1, 4096);
    let mut writer = writers.pop().unwrap();
    let log = test_log(1 << 20, 1, 1);

    let child = || {
        let mut reader = log.reader(0);
        for _ in 0..3 {
            let (kind, generation) = loop {
                log.parks().park(0).park(|| reader.is_empty());
                if let Some((msg, ..)) = reader.next() {
                    break (msg.head.request.kind, msg.head.request.arg0);
                }
            };
            let msg = WireMsg {
                arg0: kind.as_wire() as u64,
                arg1: generation,
                ..Default::default()
            };
            writer.send_msg(id(1), &msg);
        }
    };

    let pid = unsafe { fork_child(child) };

    let report = || {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while std::time::Instant::now() < deadline {
            if let Some(slot) = receiver.try_read_slot(0) {
                let hdr = &slot.control().hdr;
                return (SalMessageKind::from_wire(hdr.arg0 as u8), hdr.arg1);
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        panic!("the worker never reported");
    };

    // Each write is the wake.
    let mut excl = log.excl();
    excl.apply(Apply::FlushEph { generation: 1 }, id(1))
        .expect("group fits");
    assert_eq!(report(), (Some(SalMessageKind::FlushEph), 1));
    assert_eq!(log.epoch(), 1);

    // The rewind follows the worker's answer to the `Flush`, as it follows
    // every worker's ACK in a base round.
    excl.apply(Apply::Flush, id(1)).expect("group fits");
    assert_eq!(report(), (Some(SalMessageKind::Flush), 0));
    assert_eq!(excl.checkpoint_reset(), 2);
    excl.apply(Apply::FlushEph { generation: 2 }, id(1))
        .expect("group fits");
    assert_eq!(
        log.cursor(),
        group_at(log.log(), 0).end,
        "at cursor 0 of the next epoch"
    );
    assert_eq!(report(), (Some(SalMessageKind::FlushEph), 2));
    drop(excl);

    unsafe { assert_child_exited_ok(pid) };
}

/// A group written to 2 of 4 workers is read by those two alone, both answering
/// on the one request id.
#[test]
fn a_group_is_read_by_the_workers_it_addresses_on_one_id() {
    let log = test_log(1 << 20, 4, 1);
    log.excl()
        .read(scan(0, &[]), WorkerSet::one(1).with(3), None, id(40))
        .expect("group fits");
    log.excl().write(&unanswered(&scan(0, &[]))).expect("group fits");

    for w in 0..4u32 {
        let mut reader = log.reader(w);
        let mut read = std::iter::from_fn(|| {
            reader
                .next()
                .map(|(m, ..)| (RequestId::to_wire(m.head.reply), m.head.joins_cut()))
        });
        if w == 1 || w == 3 {
            assert_eq!(
                read.next(),
                Some((40, false)),
                "worker {w} reads the group written to it"
            );
        }
        assert_eq!(read.next(), Some((0, false)), "worker {w} reads the unaddressed group");
        assert_eq!(read.next(), None, "worker {w}");
    }
}

/// Every member of a cut reads one count, two cuts read two, and a worker a
/// cut's first member does not address reads the same count as one it does.
#[test]
fn every_worker_numbers_a_cut_alike() {
    let log = test_log(1 << 20, 2, 1);
    let member = |excl: &mut SalExcl, set, n| excl.read(scan(0, &[]), set, None, id(n)).expect("group fits");
    // One cut whose first member addresses worker 0 alone, then a cut of one.
    let mut excl = log.excl();
    member(&mut excl, WorkerSet::one(0), 1);
    member(&mut excl, WorkerSet::ALL, 2);
    member(&mut excl, WorkerSet::ALL, 3);
    drop(excl);
    member(&mut log.excl(), WorkerSet::ALL, 4);

    let cuts = |w| {
        let mut reader = log.reader(w);
        std::iter::from_fn(|| {
            reader
                .next()
                .map(|(m, _, cut)| (RequestId::to_wire(m.head.reply), cut.0))
        })
        .collect::<Vec<_>>()
    };
    assert_eq!(cuts(0), [(1, 1), (2, 1), (3, 1), (4, 2)]);
    assert_eq!(cuts(1), [(2, 1), (3, 1), (4, 2)]);
}

#[test]
fn a_reader_is_empty_only_while_no_group_is_readable_at_its_cursor() {
    let log = test_log(1 << 20, 2, 1);
    let mut reader = log.reader(0);
    assert!(reader.is_empty(), "an unwritten log");

    log.forge(0, false, SalMessageKind::ScanSpec, &[&[], &[0u8; 32]]);
    assert!(!reader.is_empty(), "another worker's unicast");
    assert!(reader.next().is_none(), "holds nothing for worker 0");
    assert!(reader.is_empty(), "and the cursor is past it");

    log.forge(0, false, SalMessageKind::ScanSpec, &[&[0u8; 32], &[]]);
    assert!(!reader.is_empty());
    assert!(reader.next().is_some());
    assert!(reader.is_empty());

    assert!(
        SalReader::new(*log.log(), 0, 2).is_empty(),
        "an older epoch's group at the cursor"
    );
}

// ---------------------------------------------------------------------------
// The live drain's fail-stops: damage and a group no master writes abort it.
// ---------------------------------------------------------------------------

/// A digest mismatch under a passing epoch gate can only be corruption, and the
/// live drain fail-stops rather than reading it as end-of-log.
#[test]
fn the_live_path_aborts_on_a_damaged_header() {
    assert_fail_stops(
        module_path!(),
        "the_live_path_aborts_on_a_damaged_header_internal",
        "is corrupt — the log is damaged",
    );
}

/// Runs only in the re-exec'd abort child.
#[test]
fn the_live_path_aborts_on_a_damaged_header_internal() {
    if !crate::test_support::in_child_test() {
        return;
    }
    let mut log = test_log(1 << 20, 1, 1);
    log.forge(7, false, SalMessageKind::ScanSpec, &[&[0u8; 32]]);
    // A sanity read before the damage, so the abort below is the damage.
    let mut reader = log.reader(0);
    assert!(reader.next().is_some());
    log.damage_header(0);

    let mut reader = log.reader(0);
    let _ = reader.next();
    unreachable!("a damaged header on the live path must fail-stop, not park");
}

/// A read nobody would answer is refused at decode: the worker fail-stops
/// rather than replying on no id.
#[test]
fn a_read_group_without_a_request_id_aborts() {
    assert_fail_stops(
        module_path!(),
        "a_read_group_without_a_request_id_aborts_internal",
        "a read group carries no request id",
    );
}

/// Runs only in the re-exec'd abort child.
#[test]
fn a_read_group_without_a_request_id_aborts_internal() {
    if !crate::test_support::in_child_test() {
        return;
    }
    let log = test_log(1 << 20, 1, 1);
    log.excl()
        .read(scan(0, &[]), WorkerSet::ALL, None, id(1))
        .expect("group fits");
    log.excl().write(&unanswered(&scan(0, &[]))).expect("group fits");
    let mut reader = log.reader(0);
    assert!(
        matches!(reader.next_request(|_, _| None), Some(Inbound::Read { .. })),
        "an answered read decodes"
    );
    let _ = reader.next_request(|_, _| None);
    unreachable!("a read nobody answers must fail-stop");
}

/// A payload the master would never write is refused at decode: the worker
/// fail-stops rather than dropping a group the other workers apply.
#[test]
fn an_undecodable_payload_aborts() {
    assert_fail_stops(
        module_path!(),
        "an_undecodable_payload_aborts_internal",
        "holds no payload head",
    );
}

/// Runs only in the re-exec'd abort child.
#[test]
fn an_undecodable_payload_aborts_internal() {
    if !crate::test_support::in_child_test() {
        return;
    }
    let log = test_log(1 << 20, 1, 1);
    log.forge(7, false, SalMessageKind::Tick, &[&[0u8; 3]]);
    let mut reader = log.reader(0);
    let _ = reader.next_request(|_, _| None);
    unreachable!("a payload shorter than its head must fail-stop");
}

/// Rows on a group of a kind that carries none are refused at decode, though
/// the payload itself decodes.
#[test]
fn rows_on_a_group_that_carries_none_abort() {
    assert_fail_stops(
        module_path!(),
        "rows_on_a_group_that_carries_none_abort_internal",
        "a Tick group carries no rows",
    );
}

/// Runs only in the re-exec'd abort child.
#[test]
fn rows_on_a_group_that_carries_none_abort_internal() {
    if !crate::test_support::in_child_test() {
        return;
    }
    let batch = make_batch(&make_schema_u64_i64(), &[(1, 1, 10)]);
    let record = encode_schema_block(batch.schema());
    let sent = Some((&record[..], batch.wire_whole().unwrap()));
    let mut rows = vec![0; payload_size(&[], sent)];
    payload_encode(&mut rows, &[], sent);
    assert!(payload_rows(&rows, |_| None).expect("the payload decodes").1.is_some());

    let log = test_log(1 << 20, 1, 1);
    log.forge(7, false, SalMessageKind::Push, &[&rows]);
    log.forge(7, false, SalMessageKind::Tick, &[&rows]);
    let mut reader = log.reader(0);
    assert!(
        matches!(
            reader.next_request(|_, _| None),
            Some(Inbound::Apply { rows: Some(_), .. })
        ),
        "the same payload decodes on a push"
    );
    let _ = reader.next_request(|_, _| None);
    unreachable!("rows on a tick must fail-stop");
}
