//! How one committer batch is drained off the request channel, how its writes
//! form commit units, how a commit unit answers its clients, how a merged run is
//! taken back as its writes, how a pause is released, and how a write the SAL
//! refuses goes again behind a checkpoint or fails — over a dispatcher no
//! worker is launched for. What a worker does with the groups is covered end
//! to end.

use super::super::tick::tick_loop;
use super::*;
use crate::catalog::CatalogEngine;
use crate::runtime::master::fixtures::test_dispatcher;
use crate::runtime::sal::Routed;
use crate::runtime::test_support::{empty_catalog, try_poll_once, within};
use crate::test_support::{col_def, make_batch_bytes, make_batch_raw, make_schema_u64_i64};
use gnitz_wire::{TableDistribution, TableProps, TypeCode, WireConflictMode, WireStatus};
use gnitz_zset::repr::MemBatch;
use std::time::Duration;

fn batch_of(rows: usize) -> Batch {
    make_batch_raw(&make_schema_u64_i64(), &vec![(0, 1, 0); rows])
}

fn push_of(rows: usize) -> CommitRequest {
    write_of(&[rows])
}

/// A write of one family per `(tid, rows)` of `families`.
fn write(families: impl IntoIterator<Item = (u64, usize)>) -> PendingWrite {
    let families: Vec<TxnFamily> = (families.into_iter())
        .map(|(tid, rows)| TxnFamily::new(tid, WireConflictMode::Update, FamilyRows::Built(batch_of(rows))))
        .collect();
    let rows = families.iter().map(|f| f.rows.view().len()).sum();
    PendingWrite { families, waiter: waiter(rows).0 }
}

/// A client of a write of `rows` rows holding no table lock, and its reply.
fn waiter(rows: usize) -> (Waiter, oneshot::Receiver<Result<u64, WireFault>>) {
    let (done, rx) = oneshot::channel();
    (Waiter { done, rows, held: HeldTables::default() }, rx)
}

/// A write of one family per entry of `family_rows`, to tables `1..`.
fn write_of(family_rows: &[usize]) -> CommitRequest {
    CommitRequest::Write(write((1..).zip(family_rows.iter().copied())))
}

fn checkpoint() -> CommitRequest {
    let (done, _rx) = oneshot::channel();
    CommitRequest::End(BatchEnd::Checkpoint(done))
}

fn backup() -> CommitRequest {
    let (done, _rx) = oneshot::channel();
    CommitRequest::End(BatchEnd::Backup { dir: "dir".to_string(), done })
}

fn pause() -> CommitRequest {
    let (parked, _rx) = oneshot::channel();
    let (_tx, resume) = oneshot::channel();
    CommitRequest::End(BatchEnd::Pause { parked, resume })
}

fn end_label(end: &Option<BatchEnd>) -> Option<&'static str> {
    end.as_ref().map(|end| match end {
        BatchEnd::Checkpoint(_) => "checkpoint",
        BatchEnd::Backup { .. } => "backup",
        BatchEnd::Pause { .. } => "pause",
    })
}

/// A batch drains what is queued until the row cap — tested before each receive,
/// so the request that crosses it is admitted whole, and a write counts
/// every family's rows — or until an `End` request, wherever it lands.
#[test]
fn a_batch_ends_at_the_row_cap_or_an_end_request() {
    // One family alone stays under the cap; two cross it.
    let half = MAX_PENDING_ROWS / 2 + 1;
    let cases = [
        // (queued, (writes, end, left behind))
        (vec![push_of(1), CommitRequest::Reclaim, push_of(1)], (2, None, 0)),
        (
            vec![push_of(MAX_PENDING_ROWS - 1), push_of(2), push_of(1)],
            (2, None, 1),
        ),
        (vec![write_of(&[half, half]), push_of(1)], (1, None, 1)),
        (vec![checkpoint(), push_of(1)], (0, Some("checkpoint"), 1)),
        (
            vec![push_of(1), push_of(1), backup(), push_of(1)],
            (2, Some("backup"), 1),
        ),
        (vec![push_of(1), pause(), push_of(1)], (1, Some("pause"), 1)),
    ];
    for (i, (queued, want)) in cases.into_iter().enumerate() {
        let (tx, mut rx) = chan::unbounded();
        for r in queued {
            tx.send(r);
        }
        let first = rx.try_recv().expect("queued");
        let mut writes = Vec::new();
        let end = drain_ready_batch(&mut rx, first, &mut writes);
        let left = std::iter::from_fn(|| rx.try_recv()).count();
        let got = (writes.len(), end_label(&end), left);
        assert_eq!(got, want, "case {i}");
    }
}

/// A drain that tops up a batch, as one behind an fdatasync in flight does,
/// counts the rows the batch already holds towards the cap.
#[test]
fn a_top_up_drain_counts_the_rows_already_batched() {
    let (tx, mut rx) = chan::unbounded();
    for r in [push_of(1), push_of(1)] {
        tx.send(r);
    }
    let CommitRequest::Write(held) = push_of(MAX_PENDING_ROWS) else {
        unreachable!("a push is a write")
    };
    let mut writes = vec![held];
    let first = rx.try_recv().expect("queued");
    assert!(drain_ready_batch(&mut rx, first, &mut writes).is_none());
    assert_eq!(writes.len(), 2, "the cap was reached before the first top-up");
    assert_eq!(std::iter::from_fn(|| rx.try_recv()).count(), 1);
}

/// A pause holds the committer until its token drops, or until its caller is
/// dropped before the committer parked.
#[test]
fn a_pause_resumes_the_committer_when_released() {
    let (committer, CommitRequests(mut rx)) = Committer::new();
    let mut take_pause = || match rx.try_recv() {
        Some(CommitRequest::End(BatchEnd::Pause { parked, resume })) => (parked, resume),
        _ => panic!("a pause request is queued"),
    };

    assert!(try_poll_once(committer.pause()).is_none());
    let (_parked, resume) = take_pause();
    assert!(
        try_poll_once(resume).is_some(),
        "a caller dropped before the park resumes"
    );

    let mut pausing = std::pin::pin!(committer.pause());
    assert!(try_poll_once(pausing.as_mut()).is_none());
    let (parked, mut resume) = take_pause();
    parked.send(());
    let paused = try_poll_once(pausing.as_mut()).expect("parked");
    assert!(try_poll_once(&mut resume).is_none(), "held while the token lives");
    drop(paused);
    assert!(try_poll_once(&mut resume).is_some());
}

/// A merged run taken back is its clients' writes again: each client's rows,
/// in its own write, answered on its own channel.
#[test]
fn a_merged_run_splits_back_into_its_writes() {
    let schema = make_schema_u64_i64();
    let parts: Vec<Batch> = [0u64..2, 2..2, 2..5]
        .into_iter()
        .map(|pks| make_batch_raw(&schema, &pks.map(|pk| (pk, pk as i64 + 1, 0)).collect::<Vec<_>>()))
        .collect();
    let (waiters, mut rxs): (Vec<_>, Vec<_>) = parts.iter().map(|part| waiter(part.len())).unzip();
    let unit = CommitUnit {
        outcome: Outcome::Laid {
            families: vec![TxnFamily::new(
                7,
                WireConflictMode::Update,
                FamilyRows::Built(Batch::concat(&schema, parts.iter().map(Batch::as_mem_batch))),
            )],
            in_zone: false,
        },
        waiters: waiters.into(),
    };
    let writes = unit.into_writes();
    assert_eq!(writes.len(), parts.len());
    let rows = |b: MemBatch<'_>| {
        (0..b.len())
            .map(|r| (b.get_pk_bytes(r).to_vec(), b.get_weight(r)))
            .collect::<Vec<_>>()
    };
    for (i, (w, part)) in writes.into_iter().zip(&parts).enumerate() {
        let [f] = &w.families[..] else {
            panic!("write {i} is one family")
        };
        assert_eq!(
            (f.tid, rows(f.rows.view())),
            (7, rows(part.as_mem_batch())),
            "write {i}"
        );
        w.waiter.done.send(Ok(i as u64));
    }
    for (i, rx) in rxs.iter_mut().enumerate() {
        assert_eq!(try_poll_once(rx), Some(Ok(i as u64)), "client {i}");
    }
}

/// A unit answers every client it holds, a merged run's included: the zone
/// LSN when it rode inside the zone, `0` when it did not, the error when it
/// failed.
#[test]
fn a_unit_answers_every_client() {
    let unit_of = |outcome: Outcome| {
        let (waiters, rxs): (Vec<_>, Vec<_>) = (0..3).map(|_| waiter(1)).unzip();
        let unit = CommitUnit { outcome, waiters: waiters.into() };
        (unit, rxs)
    };
    let laid = |in_zone| Outcome::Laid { families: Vec::new(), in_zone };
    let cases = [
        (laid(true), Ok(9)),
        (laid(false), Ok(0)),
        (Outcome::Failed("refused".to_string().into()), Err(())),
    ];
    for (i, (outcome, want)) in cases.into_iter().enumerate() {
        let (unit, mut rxs) = unit_of(outcome);
        unit.resolve(9);
        for (c, rx) in rxs.iter_mut().enumerate() {
            let got = try_poll_once(rx).expect("answered").map_err(|_| ());
            assert_eq!(got, want, "case {i}, client {c}");
        }
    }
}

/// Each write joins the unit of the last write before it to its table when
/// both are one-family writes, so no write moves past another to one of its
/// tables.
#[test]
fn a_push_merges_behind_the_last_write_to_its_table() {
    // Arrival order, each write's rows its label: pushes to 7 on both sides of
    // a transaction over 8 and 7, and pushes to 9 and 3 nothing else writes.
    let mut s = CommitScratch {
        writes: vec![
            write([(7, 1)]),
            write([(9, 2)]),
            write([(8, 3), (7, 3)]),
            write([(7, 4)]),
            write([(3, 5)]),
            write([(9, 6)]),
            write([(7, 7)]),
        ],
        ..CommitScratch::default()
    };
    build_units(&mut s, true);
    let units: Vec<(Vec<u64>, Vec<usize>, usize)> = (s.units.iter())
        .map(|u| {
            let tids = u.laid().iter().map(|f| f.tid).collect();
            let dones = u.waiters.iter().map(|w| w.rows).collect();
            (tids, dones, u.laid()[0].rows.view().len())
        })
        .collect();
    let want = vec![
        (vec![7], vec![1], 1),
        (vec![9], vec![2, 6], 8),
        (vec![8, 7], vec![6], 3),
        (vec![7], vec![4, 7], 11),
        (vec![3], vec![5], 5),
    ];
    assert_eq!(units, want);
    assert!(s.last_unit.is_empty() && s.tails.is_empty());
}

/// The committer and the tick loop its checkpoints drain through, serving
/// `engine` with no worker launched: every round's ACKs are in at its write.
/// The feeds' pump is not running; its requests are returned to stay open.
fn workerless_committer(engine: CatalogEngine) -> (Rc<Shared>, impl Sized) {
    let (dispatcher, _) = test_dispatcher(0, engine);
    let reactor = dispatcher.reactor().clone();
    let (shared, requests, tick_requests, asks) = Shared::new(dispatcher, Duration::ZERO, None);
    reactor.spawn(tick_loop(Rc::clone(&shared), tick_requests));
    reactor.spawn(run(requests, Rc::clone(&shared)));
    (shared, asks)
}

/// A write refused for want of room goes again behind a checkpoint only when
/// one may make room for it: it is within the limit of a push, and it was
/// refused past the checkpoint's floor. A write of exactly the room a
/// checkpoint leaves commits behind one checkpoint. A write larger than that
/// room is answered `SalFull` once it is refused directly behind a checkpoint,
/// which calls for no further one; a write over the limit is answered `SalFull`
/// where it is first refused. A write of several families is measured whole,
/// its families behind the one refused included: over the limit, it fails at
/// once; within it, it goes again. The committer goes on to commit what
/// follows.
#[test]
fn a_refused_write_goes_again_only_if_a_checkpoint_may_make_room() {
    within(|| {
        let (_tmp, mut engine) = empty_catalog();
        // Replicated, so the one payload holds the rows whatever the width.
        let replicated = TableProps {
            distribution: TableDistribution::Replicated,
            ..Default::default()
        };
        let cols = [col_def("id", TypeCode::U64), col_def("s", TypeCode::String)];
        let [tid, tid_b, tid_c] = ["public.t", "public.b", "public.c"]
            .map(|name| engine.create_table_with(name, &cols, &[0], replicated).unwrap());
        let (shared, _asks) = workerless_committer(engine);
        let disp = &shared.dispatcher;
        let sal = disp.sal();

        // One row whose push occupies `footprint` log bytes, when given; a
        // small one otherwise.
        let rows = |footprint: Option<usize>| {
            let cat = shared.cat();
            let (rel, record) = cat.wire_target(tid).unwrap();
            let sized = |len: usize| {
                let batch = make_batch_bytes(&rel.schema(), &[(1, 1, &vec![b'x'; len])]);
                let bytes = sal.push_footprint(record, Routed::Same(batch.wire_whole()));
                (batch, bytes)
            };
            let (small, bytes) = sized(64);
            let Some(footprint) = footprint else { return small };
            let (batch, got) = sized(64 + footprint - bytes);
            assert_eq!(got, footprint);
            batch
        };
        let commit_all = |families: &[(u64, Option<usize>)]| {
            let families = (families.iter())
                .map(|&(tid, footprint)| {
                    TxnFamily::new(tid, WireConflictMode::Update, FamilyRows::Built(rows(footprint)))
                })
                .collect();
            disp.reactor()
                .block_on(shared.committer.write(families, HeldTables::default()))
        };
        let commit = |footprint: Option<usize>| commit_all(&[(tid, footprint)]);

        // No ephemeral round yet: the floor is offset 0.
        let limit = sal.push_room();
        disp.reactor().block_on(disp.boot_checkpoint()).unwrap();
        let room = sal.push_room();
        assert!(room < limit, "the boot checkpoint's group is in the log");
        commit(None).expect("a small write commits");
        assert_eq!(sal.epoch(), 1);

        // Refused behind the small write, and laid out directly behind the
        // checkpoint that makes room for it.
        commit(Some(room)).expect("the write fits behind a checkpoint");
        assert_eq!(sal.epoch(), 2, "one checkpoint made room");
        assert_eq!(sal.push_room(), room, "which left what the boot's did");

        // The log is past its threshold, so this batch starts with a
        // checkpoint: the write is refused directly behind it.
        let e = commit(Some(room + 8)).expect_err("no checkpoint leaves room for it");
        assert_eq!(e.status, WireStatus::SalFull, "{e}");
        assert_eq!(sal.epoch(), 3, "no checkpoint is run for a write refused at the floor");
        commit(None).expect("the committer serves the next write");
        assert_eq!(sal.epoch(), 3, "and the refusal called for none");

        // Refused behind the small write, so it goes again: once, to be refused
        // directly behind the checkpoint.
        let e = commit(Some(room + 8)).expect_err("no checkpoint leaves room for it");
        assert_eq!(e.status, WireStatus::SalFull, "{e}");
        assert_eq!(sal.epoch(), 4, "one checkpoint was run for it");
        commit(None).expect("the committer serves the next write");
        assert_eq!(sal.epoch(), 4);

        // Over the limit: refused behind the small write, and no checkpoint
        // helps.
        let e = commit(Some(limit + 8)).expect_err("no log holds it");
        assert_eq!(e.status, WireStatus::SalFull, "{e}");
        commit(None).expect("the committer serves the next write");
        assert_eq!(sal.epoch(), 4, "no checkpoint is run for a write over the limit");

        // Three families, each within the room, refused at the second behind
        // the small write: the third counted, the write is over the limit.
        let half = Some(room / 2);
        let e = commit_all(&[(tid, half), (tid_b, half), (tid_c, half)]).expect_err("no log holds it");
        assert_eq!(e.status, WireStatus::SalFull, "{e}");
        commit(None).expect("the committer serves the next write");
        assert_eq!(sal.epoch(), 4, "no checkpoint is run for it, nor called for by it");

        // Two such families are within the room: refused at the second, the
        // write goes again, and commits directly behind the checkpoint.
        commit_all(&[(tid, half), (tid_b, half)]).expect("the write fits behind a checkpoint");
        assert_eq!(sal.epoch(), 5, "one checkpoint made room");
    });
}
