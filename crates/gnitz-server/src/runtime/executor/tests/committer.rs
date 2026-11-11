//! How one committer batch is drained off the request channel, how its writes
//! form commit units, how a commit unit answers its clients, what a settle and
//! a checkpoint leave behind, and how a write or a merged run the SAL refuses
//! goes again behind a checkpoint or fails — over a dispatcher no worker is
//! launched for. What a worker does with the groups is covered end to end.

use super::super::tick::fresh_read_lock;
use super::*;
use crate::catalog::CatalogEngine;
use crate::runtime::master::fixtures::test_dispatcher;
use crate::runtime::sal::Routed;
use crate::runtime::test_support::{empty_catalog, try_poll_once, within};
use crate::test_support::{
    col_def, make_batch_bytes, make_batch_raw, make_schema_u64_i64, register_identity_view, u64_i64_cols,
};
use gnitz_wire::{TableDistribution, TableProps, TypeCode, WireConflictMode, WireStatus};
use std::time::Duration;

fn batch_of(rows: usize) -> Batch {
    make_batch_raw(&make_schema_u64_i64(), &vec![(0, 1, 0); rows])
}

fn push_of(rows: usize) -> CommitRequest {
    write_of(&[rows])
}

/// A write of one family per `(tid, rows)` of `families`.
fn write(families: impl IntoIterator<Item = (u64, usize)>) -> PendingWrite {
    write_and_reply(families).0
}

/// [`write`] holding no table lock, and its client's reply.
fn write_and_reply(
    families: impl IntoIterator<Item = (u64, usize)>,
) -> (PendingWrite, oneshot::Receiver<Result<u64, WireFault>>) {
    let families: Vec<TxnFamily> = (families.into_iter())
        .map(|(tid, rows)| TxnFamily::new(tid, WireConflictMode::Update, FamilyRows::Built(batch_of(rows))))
        .collect();
    let rows = families.iter().map(|f| f.rows.view().len()).sum();
    let (done, rx) = oneshot::channel();
    let held = HeldTables::default();
    (PendingWrite { families, rows, held, done }, rx)
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

fn settle() -> CommitRequest {
    let (done, _rx) = oneshot::channel();
    CommitRequest::End(BatchEnd::Settle(done))
}

fn end_label(end: &Option<BatchEnd>) -> Option<&'static str> {
    end.as_ref().map(|end| match end {
        BatchEnd::Checkpoint(_) => "checkpoint",
        BatchEnd::Backup { .. } => "backup",
        BatchEnd::Settle(_) => "settle",
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
        (vec![push_of(1), settle(), push_of(1)], (1, Some("settle"), 1)),
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

/// A unit answers every client it holds, a merged run's included: the zone
/// LSN when it rode inside the zone, `0` when it did not, the error when it
/// failed.
#[test]
fn a_unit_answers_every_client() {
    let cases = [
        (Ok(true), Ok(9)),
        (Ok(false), Ok(0)),
        (Err("refused".to_string().into()), Err(())),
    ];
    for (i, (outcome, want)) in cases.into_iter().enumerate() {
        let (writes, mut rxs): (Vec<_>, Vec<_>) = (0..3).map(|_| write_and_reply([(7, 1)])).unzip();
        let mut s = CommitScratch { writes, ..CommitScratch::default() };
        build_units(&mut s, true);
        let mut unit = s.units.pop().expect("a unit");
        assert!(s.units.is_empty() && unit.is_merged(), "the writes are one run");
        unit.outcome = outcome;
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
            let writes = u.writes.iter().map(|w| w.rows).collect();
            (tids, writes, u.laid()[0].rows.view().len())
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
    assert!(s.last_unit.is_empty());
}

/// The committer serving `engine` with no worker launched: every round's ACKs
/// are in at its write. Neither the tick loop nor the feeds' pump is running;
/// the pump's requests are returned to stay open.
fn workerless_committer(engine: CatalogEngine) -> (Rc<Shared>, impl Sized) {
    let (dispatcher, _) = test_dispatcher(0, engine);
    let reactor = dispatcher.reactor().clone();
    let (shared, requests, _, asks) = Shared::new(dispatcher, Duration::ZERO, None);
    reactor.spawn(run(requests, Rc::clone(&shared)));
    (shared, asks)
}

/// Replicated tables of a U64 key and a string, so the one payload holds a
/// table's rows whatever the width.
fn string_tables<const N: usize>(engine: &mut CatalogEngine, names: [&str; N]) -> [u64; N] {
    let replicated = TableProps {
        distribution: TableDistribution::Replicated,
        ..Default::default()
    };
    let cols = [col_def("id", TypeCode::U64), col_def("s", TypeCode::String)];
    names.map(|name| engine.create_table_with(name, &cols, &[0], replicated).unwrap())
}

/// One row of string table `tid` whose push occupies `footprint` log bytes,
/// when given; a small one otherwise.
fn sized_row(shared: &Shared, tid: u64, footprint: Option<usize>) -> Batch {
    let sal = shared.dispatcher.sal();
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
}

/// A checkpoint ticks what is pending under its own hold of the tick gate,
/// with no tick loop running, and that emit answers a read waiting on a drain.
#[test]
fn a_checkpoint_ticks_what_is_pending_and_answers_a_waiting_read() {
    within(|| {
        let (_tmp, mut engine) = empty_catalog();
        let cols = u64_i64_cols();
        let t = engine.create_table("public.t", &cols, &[0]).unwrap();
        let v = register_identity_view(&mut engine, t, "v", &cols);
        let (shared, _asks) = workerless_committer(engine);
        let reactor = shared.dispatcher.reactor();
        reactor.block_on(async {
            let laid_out = shared.dispatcher.sal().lock().await;
            shared.note_commit_rows(&laid_out, [(t, 1)].into_iter());
        });

        let mut read = std::pin::pin!(fresh_read_lock(&shared, [v], reactor.now()));
        assert!(try_poll_once(read.as_mut()).is_none(), "the read waits on a drain");
        reactor.block_on(shared.committer.final_checkpoint());
        assert!(shared.ticks.all_ticked(&shared.cat().dag, [v]));
        assert!(try_poll_once(read.as_mut()).is_some_and(|locked| locked.is_ok()));
    });
}

/// A settle with no write queued ahead runs the checkpoint the log is due, and
/// none on a log below its threshold.
#[test]
fn a_settle_runs_the_checkpoint_that_is_due() {
    within(|| {
        let (_tmp, mut engine) = empty_catalog();
        let [tid] = string_tables(&mut engine, ["public.t"]);
        let (shared, _asks) = workerless_committer(engine);
        let disp = &shared.dispatcher;
        let sal = disp.sal();
        disp.reactor().block_on(disp.boot_checkpoint()).unwrap();

        let rows = FamilyRows::Built(sized_row(&shared, tid, Some(sal.push_room())));
        let families = vec![TxnFamily::new(tid, WireConflictMode::Update, rows)];
        disp.reactor()
            .block_on(shared.committer.write(families, HeldTables::default()))
            .expect("the write fits behind the boot checkpoint");
        assert!(sal.needs_checkpoint(), "the write crossed the threshold");
        assert_eq!(sal.epoch(), 1);

        for _ in 0..2 {
            disp.reactor().block_on(shared.committer.settle());
            assert!(!sal.needs_checkpoint());
            assert_eq!(sal.epoch(), 2, "one checkpoint, run by the first settle");
        }
    });
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
        let [tid, tid_b, tid_c] = string_tables(&mut engine, ["public.t", "public.b", "public.c"]);
        let (shared, _asks) = workerless_committer(engine);
        let disp = &shared.dispatcher;
        let sal = disp.sal();

        let commit_all = |families: &[(u64, Option<usize>)]| {
            let families = (families.iter())
                .map(|&(family, footprint)| {
                    // Sized as a row of `tid`: the tables share one schema.
                    let rows = FamilyRows::Built(sized_row(&shared, tid, footprint));
                    TxnFamily::new(family, WireConflictMode::Update, rows)
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

/// A merged run the SAL refuses goes again as the writes it merged, each
/// committed on its own behind a checkpoint.
#[test]
fn a_refused_merged_run_goes_again_as_its_writes() {
    within(|| {
        let (_tmp, mut engine) = empty_catalog();
        let [tid] = string_tables(&mut engine, ["public.t"]);
        let (shared, _asks) = workerless_committer(engine);
        let disp = &shared.dispatcher;
        let sal = disp.sal();
        disp.reactor().block_on(disp.boot_checkpoint()).unwrap();
        let room = sal.push_room();

        // Each fits behind a checkpoint, the two together in no log. The
        // strings differ, so the merged batch holds both.
        let large = room / 4 * 3;
        let mut pushes = [large, large + 8].map(|footprint| {
            let rows = FamilyRows::Built(sized_row(&shared, tid, Some(footprint)));
            let families = vec![TxnFamily::new(tid, WireConflictMode::Update, rows)];
            Box::pin(shared.committer.write(families, HeldTables::default()))
        });
        // Both are queued before the committer takes either, so they are one
        // batch.
        for push in &mut pushes {
            assert!(try_poll_once(push.as_mut()).is_none());
        }
        for push in &mut pushes {
            disp.reactor()
                .block_on(push.as_mut())
                .expect("the write commits on its own");
        }
        assert_eq!(
            sal.epoch(),
            3,
            "one checkpoint behind the refused run, one to make room for its second write"
        );
    });
}
