//! One-shot ops: the slot an op's CQE and its awaiter meet in, freed by the
//! later of the two; `fsync`, which submits at its first poll; and `read`.

use std::io::Write;
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
    let first = nop(r);
    let (id, slot) = (first.user_data, first.slot());
    r.block_on(first);
    let second = nop(r);
    assert_eq!(second.slot(), slot, "the freed slot is reused");
    assert_ne!(second.user_data, id);
    r.block_on(second);
}

/// An op dropped before its CQE leaves what its SQE names in its slot until
/// the CQE, which frees both; an op dropped after its CQE frees its slot in
/// the drop.
#[test]
fn a_dropped_op_keeps_its_carry_until_its_cqe() {
    let r = make_reactor();
    let carry = Rc::new(());
    // SAFETY: the SQE names no memory.
    drop(unsafe { r.push_op(opcode::Nop::new().build(), Rc::clone(&carry)) });
    let done = nop(r);
    assert_eq!(
        (Rc::strong_count(&carry), r.ops.len()),
        (2, 2),
        "the slot keeps the abandoned carry"
    );
    r.submit();
    r.run_ready();
    assert_eq!(
        (Rc::strong_count(&carry), r.ops.len()),
        (1, 1),
        "its CQE frees it; the other's result waits"
    );
    drop(done);
    assert_eq!(r.ops.len(), 0, "the drop frees the completed slot");
}

/// `read` fills its buffer from the fd and hands it back with the CQE `res`.
#[test]
fn read_fills_its_buffer_and_yields_the_cqe_res() {
    let r = make_reactor();
    let (rd, wr) = std::io::pipe().expect("pipe");
    (&wr).write_all(b"bytes").expect("write");
    let (res, buf) = r.block_on(r.read(leaked(OwnedFd::from(rd)), vec![0; 16].into()));
    assert_eq!((res, &buf[..5]), (5, &b"bytes"[..]));
}
