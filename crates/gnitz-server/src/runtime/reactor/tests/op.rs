//! One-shot ops: the slot an op's CQE and its awaiter meet in, freed by the
//! later of the two, and `fsync`, which submits at its first poll and yields
//! its CQE.

use std::os::fd::OwnedFd;
use std::time::Duration;

use super::super::test_support::*;
use super::*;

/// `fsync` submits its SQE at its first poll, so its CQE arrives with no tick
/// driven, and resolves to the CQE `res` verbatim: 0, or a socket's `-EINVAL`.
#[test]
fn fsync_is_submitted_eagerly_and_yields_the_cqe_res() {
    let r = make_reactor();
    let file = OwnedFd::from(tempfile::tempfile().expect("tempfile"));
    let (sock, _) = std::os::unix::net::UnixStream::pair().expect("socketpair");
    for (fd, want) in [(file, 0), (OwnedFd::from(sock), -libc::EINVAL)] {
        let mut fut = std::pin::pin!(r.fsync(leaked(fd)));
        let deadline = Instant::now() + Duration::from_secs(10);
        let got = loop {
            r.run_ready();
            if let Some(rc) = try_poll_once(fut.as_mut()) {
                break rc;
            }
            assert!(
                Instant::now() < deadline,
                "no CQE without a tick: the SQE was not submitted"
            );
        };
        assert_eq!(got, want);
    }
}

/// Two ops through one slot carry different ids, so a cancel of the first
/// still queued once the second holds the slot names no op in flight.
#[test]
fn a_reused_slot_gives_its_next_op_another_id() {
    let r = make_reactor();
    let first = nop(&r);
    let (id, slot) = (first.user_data, first.slot());
    r.block_on(first);
    let second = nop(&r);
    assert_eq!(second.slot(), slot, "the freed slot is reused");
    assert_ne!(second.user_data, id);
    r.block_on(second);
}

/// Submit what is queued, then dispatch every CQE that arrived.
fn complete_queued(r: &Reactor) {
    r.ring.borrow_mut().submit();
    r.run_ready();
}

/// An op dropped before its CQE keeps its slot until the CQE, which frees it;
/// the freed slot serves the next op.
#[test]
fn an_op_dropped_before_its_cqe_is_freed_by_the_cqe() {
    let r = make_reactor();
    drop(nop(&r));
    assert_eq!(r.ops.len(), 1, "the slot outlives its dropped awaiter");
    complete_queued(&r);
    assert_eq!(r.ops.len(), 0, "the CQE freed the abandoned slot");
    assert_eq!(r.block_on(nop(&r)).0, 0);
    assert_eq!(r.ops.len(), 0);
}

/// An op whose CQE came after its one pending poll, and which is then dropped
/// unpolled, frees its slot in the drop; the freed slot serves the next op.
#[test]
fn an_op_dropped_after_its_cqe_frees_its_slot() {
    let r = make_reactor();
    let mut op = nop(&r);
    assert!(try_poll_once(&mut op).is_none());
    complete_queued(&r);
    assert_eq!(r.ops.len(), 1, "the CQE leaves the result in the slot");
    drop(op);
    assert_eq!(r.ops.len(), 0, "the drop freed the completed slot");
    assert_eq!(r.block_on(nop(&r)).0, 0);
    assert_eq!(r.ops.len(), 0);
}
