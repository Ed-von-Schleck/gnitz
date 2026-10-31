//! How one committer batch is drained off the request channel, how a merged
//! run is taken back as its writes, and how a pause is released. The SAL layout and the checkpoint sequence need a live
//! dispatcher and worker ACKs, so they are covered end to end.

use super::*;
use crate::runtime::test_support::try_poll_once;
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
        let b = drain_ready_batch(&mut rx, first);
        let left = std::iter::from_fn(|| rx.try_recv()).count();
        let got = (b.writes.len(), end_label(&b.end), left);
        assert_eq!(got, want, "case {i}");
    }
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
    let (dones, mut rxs): (Vec<_>, Vec<_>) = parts
        .iter()
        .map(|part| {
            let (done, rx) = oneshot::channel();
            ((done, part.len()), rx)
        })
        .unzip();
    let unit = CommitUnit {
        outcome: Outcome::Laid {
            families: vec![TxnFamily {
                tid: 7,
                mode: WireConflictMode::Update,
                batch: Batch::concat(&schema, parts.iter().map(Batch::as_mem_batch)),
            }],
            in_zone: false,
        },
        dones,
    };
    let writes = unit.into_writes();
    assert_eq!(writes.len(), parts.len());
    let rows = |b: &Batch| {
        (0..b.len())
            .map(|r| (b.get_pk_bytes(r).to_vec(), b.get_weight(r)))
            .collect::<Vec<_>>()
    };
    for (i, (w, part)) in writes.into_iter().zip(&parts).enumerate() {
        let [f] = &w.families[..] else {
            panic!("write {i} is one family")
        };
        assert_eq!((f.tid, rows(&f.batch)), (7, rows(part)), "write {i}");
        w.done.send(Ok(i as u64));
    }
    for (i, rx) in rxs.iter_mut().enumerate() {
        assert_eq!(try_poll_once(rx), Some(Ok(i as u64)), "client {i}");
    }
}
