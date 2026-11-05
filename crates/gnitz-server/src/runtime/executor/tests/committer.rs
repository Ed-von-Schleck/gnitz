//! How one committer batch is drained off the request channel, how its writes
//! form commit units, how a commit unit answers its clients, how a merged run is
//! taken back as its writes, and how a pause is released. The SAL layout and the
//! checkpoint sequence need a live dispatcher and worker ACKs, so they are
//! covered end to end.

use super::*;
use crate::runtime::test_support::try_poll_once;
use crate::test_support::{make_batch_raw, make_schema_u64_i64};
use gnitz_wire::WireConflictMode;
use gnitz_zset::repr::MemBatch;

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
