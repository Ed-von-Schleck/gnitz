//! Reactor state: `fsync`, and the W2M park — when a pass arms it, and the
//! publishes that wake it.

use std::os::fd::OwnedFd;
use std::time::Duration;

use super::test_support::*;
use super::*;
use crate::runtime::park::WorkerSet;
use crate::runtime::test_support::{assert_child_exited_ok, fork_child};

/// `fsync` submits its SQE before returning, so its CQE arrives with no tick
/// driven — which is what lets it run while the caller works on — and resolves to
/// the CQE `res` verbatim: 0, or a socket's `-EINVAL`.
#[test]
fn fsync_is_submitted_eagerly_and_yields_the_cqe_res() {
    let r = make_reactor();
    let file = OwnedFd::from(tempfile::tempfile().expect("tempfile"));
    let (sock, _) = std::os::unix::net::UnixStream::pair().expect("socketpair");
    for (fd, want) in [(file, 0), (OwnedFd::from(sock), -libc::EINVAL)] {
        let mut fut = std::pin::pin!(r.fsync(leaked(fd)));
        let deadline = Instant::now() + Duration::from_secs(10);
        let got = loop {
            r.drain_cqes_into_wakers();
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

/// The drain the arm runs first can route a frame that wakes a task; that tick
/// must then run on instead of arming and sleeping, where nothing would wake it.
#[test]
fn a_tick_whose_arm_drain_wakes_a_task_does_not_arm() {
    within(|| {
        let (r, mut writers) = reactor_with_rings(1);
        let mut writer = writers.pop().expect("one ring");
        let lease = r.lease_acks(WorkerSet::ALL);
        let id = lease.targets().request_id;
        let done = Rc::new(Cell::new(false));
        let d = Rc::clone(&done);
        r.spawn(async move {
            // Published during the poll, after this tick's own drain.
            writer.send_ack(id);
            lease.acks().await;
            d.set(true);
        });
        r.tick(true);
        r.tick(false);
        assert!(done.get());
    });
}

/// A forked child publishes to a reactor parked through `FUTEX_WAITV` on two
/// rings: a lost wake runs into the timeout, a torn frame loses an id. The child
/// waits until the reactor parks with nothing published, so only its answer on
/// ring 1 can wake it; then it floods ring 0 as fast as it can, so the
/// drain-refresh-arm race is under pressure: ids no lease routes, which are
/// released unrouted, and the ready ACK last, so the lease resolves only once
/// every one is drained.
#[test]
fn w2m_publishes_from_another_process_wake_the_parked_reactor() {
    const N_MESSAGES: u32 = 500;
    const TIMEOUT: Duration = Duration::from_secs(30);

    let (r, mut writers) = reactor_with_rings(2);
    let child = || {
        let deadline = Instant::now() + TIMEOUT;
        while !writers[1].master_parked() {
            assert!(Instant::now() < deadline, "the reactor never parked on ring 1");
            std::thread::yield_now();
        }
        writers[1].send_ack(BOOT_READY_REQUEST_ID);
        for req_id in (BOOT_READY_REQUEST_ID + 1..=N_MESSAGES).chain([BOOT_READY_REQUEST_ID]) {
            writers[0].send_ack(req_id);
        }
    };
    let pid = unsafe { fork_child(child) };

    let lease = r.lease_ready();
    if let Either::B(()) = r.block_on(select2(lease.acks(), r.sleep(TIMEOUT))) {
        unsafe { libc::kill(pid, libc::SIGKILL) };
        panic!("reactor stalled after {TIMEOUT:?}: a lost wake");
    }
    unsafe { assert_child_exited_ok(pid) };
}
