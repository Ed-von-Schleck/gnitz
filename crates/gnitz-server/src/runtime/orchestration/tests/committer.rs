//! The committer's two pure parts: how one batch is drained off the request
//! channel, and how a unit's groups resolve into its clients' verdicts.
//!
//! The SAL layout, the checkpoint sequence and the barrier partition need a live
//! dispatcher and worker ACKs, so they stay covered end to end.

use super::*;
use crate::runtime::reactor::Reactor;
use crate::runtime::test_support::make_reactor;
use crate::test_support::make_schema_u64_i64;
use gnitz_wire::{WireConflictMode, WireStatus};
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, Waker};

/// One non-blocking poll: `Ready(v)` once resolved, `Pending` while the verdict is
/// still owed.
fn poll_once<T>(rx: &mut oneshot::Receiver<T>) -> Poll<T> {
    Pin::new(rx).poll(&mut Context::from_waker(Waker::noop()))
}

fn fault(text: &str) -> WireFault {
    WireFault {
        status: WireStatus::Error,
        text: text.to_string(),
    }
}

fn batch_of(rows: usize) -> Batch {
    Batch::zeroed(&make_schema_u64_i64(), rows)
}

/// A push of `rows` rows. Its client's receiver is dropped: nothing here reads a
/// push's verdict, and a cancelled receiver leaves the sender usable.
fn push_of(rows: usize) -> CommitRequest {
    let (done, _rx) = oneshot::channel();
    CommitRequest::Push(PendingPush {
        tid: 7,
        batch: batch_of(rows),
        recoverable: true,
        done,
    })
}

/// A transaction of one family per entry of `family_rows`.
fn txn_of(family_rows: &[usize]) -> CommitRequest {
    let (done, _rx) = oneshot::channel();
    CommitRequest::Txn(PendingTxn {
        families: family_rows
            .iter()
            .enumerate()
            .map(|(i, &rows)| TxnFamily {
                tid: i as i64 + 1,
                mode: WireConflictMode::Update,
                batch: batch_of(rows),
            })
            .collect(),
        done,
    })
}

fn barrier_of(kind: BarrierKind) -> CommitRequest {
    let (done, _rx) = oneshot::channel();
    CommitRequest::Barrier { kind, done }
}

/// The cap is tested *before* the receive, so it bounds the batch at
/// `MAX_PENDING_ROWS` plus one whole request rather than at `MAX_PENDING_ROWS` —
/// the overshoot the constant's doc claims, here driven to nearly double.
#[test]
fn the_row_cap_admits_one_whole_request_past_itself() {
    let rows = MAX_PENDING_ROWS - 1;
    let (tx, mut rx) = chan::unbounded::<CommitRequest>();
    for _ in 0..3 {
        tx.send(push_of(rows));
    }

    let first = rx.try_recv().expect("three requests are queued");
    let batch = drain_ready_batch(&mut rx, first);

    assert_eq!(batch.pushes.len(), 2);
    let drained: usize = batch.pushes.iter().map(|p| p.batch.len()).sum();
    assert_eq!(drained, 2 * rows, "the second request is admitted whole");
    assert!(drained > MAX_PENDING_ROWS);
    assert!(rx.try_recv().is_some(), "the third push rides the next batch");
}

/// A transaction is one indivisible entry, but every family's rows count against
/// the cap — the sum the classification used to spell twice.
#[test]
fn a_transactions_families_all_count_towards_the_row_cap() {
    let half = MAX_PENDING_ROWS / 2 + 1;
    let (tx, mut rx) = chan::unbounded::<CommitRequest>();
    tx.send(txn_of(&[half, half]));
    tx.send(push_of(1));

    let first = rx.try_recv().expect("queued");
    let batch = drain_ready_batch(&mut rx, first);

    assert_eq!(batch.txns.len(), 1);
    assert!(
        batch.pushes.is_empty(),
        "two families of {half} rows already cross the cap; counting one would not"
    );
    assert!(rx.try_recv().is_some(), "the push rides the next batch");
}

#[test]
fn a_barrier_ends_the_batch_and_leaves_what_is_behind_it() {
    let (tx, mut rx) = chan::unbounded::<CommitRequest>();
    tx.send(push_of(1));
    tx.send(barrier_of(BarrierKind::Ddl));
    tx.send(push_of(1));

    let first = rx.try_recv().expect("queued");
    let batch = drain_ready_batch(&mut rx, first);

    assert_eq!(batch.pushes.len(), 1, "the push ahead of the barrier rides this batch");
    assert!(
        matches!(batch.barriers[..], [(BarrierKind::Ddl, _)]),
        "the barrier itself rides it, kind intact"
    );
    assert!(rx.try_recv().is_some(), "the push behind it does not");
}

/// The rule holds for a barrier that arrives *first* — the case the duplicated
/// classification used to except, draining every push queued behind it.
#[test]
fn a_leading_barrier_ends_the_batch_alone() {
    let (tx, mut rx) = chan::unbounded::<CommitRequest>();
    tx.send(barrier_of(BarrierKind::Reclaim { forced: true }));
    tx.send(push_of(1));

    let first = rx.try_recv().expect("queued");
    let batch = drain_ready_batch(&mut rx, first);

    assert_eq!(batch.barriers.len(), 1);
    assert!(batch.pushes.is_empty(), "a barrier ends the batch wherever it lands");
    assert!(rx.try_recv().is_some(), "the push behind it rides the next batch");
}

fn group_of(reactor: &Reactor, tid: i64, write_err: Option<WireFault>) -> GroupInfo {
    GroupInfo {
        tid,
        recoverable: true,
        req_ids: reactor.lease_acks(1, crate::runtime::sal::WorkerSet::ALL),
        merged: Batch::empty_with_schema(&make_schema_u64_i64()),
        write_err,
    }
}

#[test]
fn every_coalesced_client_of_a_push_unit_resolves_exactly_once() {
    let reactor = make_reactor();
    let (d0, mut rx0) = oneshot::channel();
    let (d1, mut rx1) = oneshot::channel();
    CommitUnit {
        groups: vec![group_of(&reactor, 7, None)],
        outcome: Outcome::Pushes(vec![d0, d1]),
    }
    .resolve(42);

    for rx in [&mut rx0, &mut rx1] {
        assert!(matches!(poll_once(rx), Poll::Ready(Ok(42))));
        // `send` consumes the sender and the poll took its value, so a second poll
        // parks — one verdict per client, never two.
        assert!(matches!(poll_once(rx), Poll::Pending));
    }
}

#[test]
fn a_push_units_group_error_reaches_every_client() {
    let reactor = make_reactor();
    let (d0, mut rx0) = oneshot::channel();
    let (d1, mut rx1) = oneshot::channel();
    CommitUnit {
        groups: vec![group_of(&reactor, 7, Some(fault("SAL full")))],
        outcome: Outcome::Pushes(vec![d0, d1]),
    }
    .resolve(42);

    for rx in [&mut rx0, &mut rx1] {
        match poll_once(rx) {
            Poll::Ready(Err(e)) => assert_eq!(e.text, "SAL full"),
            _ => panic!("every coalesced client is owed the group's error"),
        }
    }
}

#[test]
fn a_transaction_resolves_ok_only_when_every_family_committed() {
    let reactor = make_reactor();

    let (done, mut rx) = oneshot::channel();
    CommitUnit {
        groups: vec![group_of(&reactor, 1, None), group_of(&reactor, 2, None)],
        outcome: Outcome::Txn(done),
    }
    .resolve(9);
    assert!(matches!(poll_once(&mut rx), Poll::Ready(Ok(9))));

    // One failed family fails the bundle, and the first error wins — the group
    // order, not the last one seen.
    let (done, mut rx) = oneshot::channel();
    CommitUnit {
        groups: vec![
            group_of(&reactor, 1, None),
            group_of(&reactor, 2, Some(fault("second family"))),
            group_of(&reactor, 3, Some(fault("third family"))),
        ],
        outcome: Outcome::Txn(done),
    }
    .resolve(9);
    match poll_once(&mut rx) {
        Poll::Ready(Err(e)) => assert_eq!(e.text, "second family"),
        _ => panic!("a transaction with a failed family resolves Err"),
    }
}
