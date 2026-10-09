//! How one committer batch is drained off the request channel. The SAL layout and
//! the checkpoint sequence need a live dispatcher and worker ACKs, so they are
//! covered end to end.

use super::*;
use crate::test_support::{make_batch_raw, make_schema_u64_i64};
use gnitz_wire::WireConflictMode;

fn batch_of(rows: usize) -> Batch {
    make_batch_raw(&make_schema_u64_i64(), &vec![(0, 1, 0); rows])
}

fn push_of(rows: usize) -> CommitRequest {
    write_of(&[rows])
}

/// A write of one family per entry of `family_rows`.
fn write_of(family_rows: &[usize]) -> CommitRequest {
    let (done, _rx) = oneshot::channel();
    CommitRequest::Write(PendingWrite {
        families: family_rows
            .iter()
            .enumerate()
            .map(|(i, &rows)| TxnFamily {
                tid: i as u64 + 1,
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

/// A batch drains what is queued until the row cap — tested before each receive,
/// so the request that crosses it is admitted whole, and a write counts
/// every family's rows — or until a barrier, wherever it lands.
#[test]
fn a_batch_ends_at_the_row_cap_or_a_barrier() {
    use BarrierKind::{Ddl, Shutdown};
    // One family alone stays under the cap; two cross it.
    let half = MAX_PENDING_ROWS / 2 + 1;
    let cases = [
        // (queued, (writes, barrier, left behind))
        (vec![push_of(1), CommitRequest::Reclaim, push_of(1)], (2, None, 0)),
        (
            vec![push_of(MAX_PENDING_ROWS - 1), push_of(2), push_of(1)],
            (2, None, 1),
        ),
        (vec![write_of(&[half, half]), push_of(1)], (1, None, 1)),
        (vec![push_of(1), barrier_of(Ddl), push_of(1)], (1, Some(Ddl), 1)),
        (vec![barrier_of(Shutdown), push_of(1)], (0, Some(Shutdown), 1)),
    ];
    for (i, (queued, want)) in cases.into_iter().enumerate() {
        let (tx, mut rx) = chan::unbounded();
        for r in queued {
            tx.send(r);
        }
        let first = rx.try_recv().expect("queued");
        let b = drain_ready_batch(&mut rx, first);
        let left = std::iter::from_fn(|| rx.try_recv()).count();
        let got = (b.writes.len(), b.barrier.map(|(k, _)| k), left);
        assert_eq!(got, want, "case {i}");
    }
}
