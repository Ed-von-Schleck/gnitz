use super::*;
use crate::ProtocolError;
use crate::connection::Request;
use crate::test_support::{client_pair, interrupt_self_until, kv_schema, reply_ctrl, session_pair};
use crate::transport::poll_fd;
use gnitz_wire::{ReadBound, ReadSpec, TypeCode};
use std::collections::VecDeque;
use std::os::fd::{AsFd, BorrowedFd};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

/// An aborted park leaves its slot pending — the frame is already on the
/// wire, so the next call drains that reply before its own.
#[test]
fn an_aborted_park_leaves_its_slot_pending_and_the_next_call_drains_it_first() {
    let (s, peer) = session_pair();
    let mut fired = false;
    let hook = BlockingHost::with_hook(move || {
        if std::mem::replace(&mut fired, true) {
            Ok(())
        } else {
            Err("interrupted".into())
        }
    });
    let mut c = GnitzClient::over(s, Box::new(hook)).unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    let sig = interrupt_self_until(Arc::clone(&stop));

    // The peer never answers, so the scan parks; the signal makes the park
    // return `EINTR`, which is the one place the hook runs.
    let (sa, spec) = (Arc::new(kv_schema(TypeCode::I64)), ReadSpec::all_rows(ReadBound::None));
    let r = block_on(c.scan_spec(1, &spec, &sa));
    assert!(matches!(r, Err(ClientError::Interrupted(ref e)) if e.to_string() == "interrupted"));
    stop.store(true, Ordering::Relaxed);
    sig.join().unwrap();
    assert_eq!(c.session.interest(), Interest::READ, "the abandoned slot stays pending");

    // The peer answers both requests; the second call must get the second.
    peer.recv();
    peer.send(&reply_ctrl(1, 100));
    let h = std::thread::spawn(move || {
        peer.recv();
        peer.send(&reply_ctrl(1, 200));
        peer
    });
    assert_eq!(block_on(c.scan_spec(1, &spec, &sa)).unwrap().lsn, Some(200));
    let _peer = h.join().unwrap();
    assert_eq!(c.session.interest(), Interest::NONE);
}

/// Bytes on the cancel socket end the wait, which reads them all, so the next
/// call runs.
#[test]
fn a_cancellable_wait_gives_up_on_a_readable_cancel_socket_and_drains_it() {
    let (s, peer) = session_pair();
    let (cancel, mut canceller) = std::os::unix::net::UnixStream::pair().unwrap();
    let mut c = GnitzClient::over(s, Box::new(BlockingHost::cancelled_by(cancel).unwrap())).unwrap();
    let (sa, spec) = (Arc::new(kv_schema(TypeCode::I64)), ReadSpec::all_rows(ReadBound::None));
    std::io::Write::write_all(&mut canceller, b"xx").unwrap();
    let r = block_on(c.scan_spec(1, &spec, &sa));
    assert!(matches!(r, Err(ClientError::Interrupted(_))));

    peer.recv();
    peer.send(&reply_ctrl(1, 100));
    let h = std::thread::spawn(move || {
        peer.recv();
        peer.send(&reply_ctrl(1, 200));
        peer
    });
    assert_eq!(block_on(c.scan_spec(1, &spec, &sa)).unwrap().lsn, Some(200));
    let _peer = h.join().unwrap();
}

// ── Waiting ─────────────────────────────────────────────────────────────────

/// A peer that answers every request with a control frame naming target 0 and
/// carrying the request's ordinal, so a reply's value names the request it
/// answered.
fn numbering_peer(peer: crate::test_support::Peer) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        for k in 0.. {
            let mut len = [0u8; gnitz_wire::FRAME_LEN_PREFIX_BYTES];
            if std::io::Read::read_exact(&mut &peer.0, &mut len).is_err() {
                return;
            }
            let mut payload = vec![0u8; u32::from_le_bytes(len) as usize];
            std::io::Read::read_exact(&mut &peer.0, &mut payload).unwrap();
            peer.send(&reply_ctrl(0, k));
        }
    })
}

/// A host that pends the way a reactor does: every other ask finds the socket
/// ready, and a job runs on a thread of its own — unless it is dropped.
#[derive(Default)]
struct PendingHost {
    asked: bool,
    drops_jobs: bool,
}

impl Host for PendingHost {
    fn attach(&mut self, _: OwnedFd) -> std::io::Result<()> {
        Ok(())
    }

    fn poll_io(
        &mut self,
        _: Interest,
        _: &mut Context<'_>,
        io: &mut dyn FnMut(Interest) -> bool,
    ) -> Poll<Result<(), ClientError>> {
        self.asked = !self.asked;
        if self.asked {
            return Poll::Pending;
        }
        io(Interest { read: true, write: true });
        Poll::Ready(Ok(()))
    }

    fn spawn(&mut self, job: Job) {
        if !self.drops_jobs {
            std::thread::spawn(job);
        }
    }
}

/// A host whose every wait fails.
struct FailingHost;

impl Host for FailingHost {
    fn attach(&mut self, _: OwnedFd) -> std::io::Result<()> {
        Ok(())
    }

    fn poll_io(
        &mut self,
        _: Interest,
        _: &mut Context<'_>,
        _: &mut dyn FnMut(Interest) -> bool,
    ) -> Poll<Result<(), ClientError>> {
        Poll::Ready(Err(std::io::Error::other("no reactor").into()))
    }

    fn spawn(&mut self, job: Job) {
        job()
    }
}

/// A wait that fails is the connection's end: the reply owed arrives as the
/// connection lost to the failure, and so does every later request's.
#[test]
fn a_failed_wait_ends_the_session() {
    let (s, _peer) = session_pair();
    let mut c = GnitzClient::over(s, Box::new(FailingHost)).unwrap();
    for _ in 0..2 {
        let lost = block_on(c.alloc_ids(1));
        assert!(
            matches!(&lost, Err(ClientError::ConnectionLost(ProtocolError::IoError(e))) if e.to_string() == "no reactor"),
            "{lost:?}"
        );
    }
    assert!(c.session.is_closed());
    assert_eq!(c.requests_sent(), 1, "the second was refused unsent");
}

/// Poll `fut` to completion, parking on `fd` whenever it pends, and count the
/// polls it took.
fn drive<F: Future>(fd: BorrowedFd<'_>, fut: F) -> (F::Output, usize) {
    let mut fut = std::pin::pin!(fut);
    let mut cx = Context::from_waker(Waker::noop());
    let patience = std::time::Duration::from_millis(20);
    for polls in 1.. {
        if let Poll::Ready(out) = fut.as_mut().poll(&mut cx) {
            return (out, polls);
        }
        let _ = poll_fd(fd, Interest::READ, Some(std::time::Instant::now() + patience));
    }
    unreachable!()
}

/// Requests detached one after another are all on their way before any reply
/// is waited for, and waiting for the last answers every one before it.
#[test]
fn detached_requests_are_answered_by_one_wait() {
    let (mut c, peer) = client_pair();
    let peer = numbering_peer(peer);
    let ack = |c: &mut GnitzClient| c.ack(Request::AllocIds(1)).detach();
    let (mut first, mut second, last) = (ack(&mut c), ack(&mut c), ack(&mut c));
    assert_eq!(c.requests_sent(), 3);
    assert!(first.try_take().is_none(), "nothing has stepped the session");

    assert_eq!(block_on(c.wait(last)).unwrap(), 2);
    assert_eq!(first.try_take().unwrap().unwrap(), 0);
    assert_eq!(second.try_take().unwrap().unwrap(), 1);

    // A verb awaited whole is the same round trip, undetached.
    assert_eq!(block_on(c.alloc_ids(1)).unwrap(), 3);
    drop(c);
    peer.join().unwrap();
}

/// A reply still owed when its session goes resolves `Closed`, on a replaced
/// connection's too: nothing waits on a session that is gone.
#[test]
fn a_session_that_goes_resolves_what_it_owed() {
    let (mut c, _peer) = client_pair();
    let mut owed = c.ack(Request::AllocIds(1)).detach();
    drop(c);
    assert!(matches!(owed.try_take(), Some(Err(ClientError::Closed))));

    let (mut c, _peer) = client_pair();
    let (replacement, _peer2) = session_pair();
    let stale = c.ack(Request::AllocIds(1)).detach();
    c.session = replacement;
    assert!(matches!(block_on(c.wait(stale)), Err(ClientError::Closed)));
}

/// `serve` under a host that really pends: a verb of three round trips, then
/// two detached requests left outstanding when the queue empties. Each runs in
/// its turn, and the loop's own stepping delivers the detached replies.
#[test]
fn serve_runs_calls_in_order_and_steps_for_detached_replies() {
    let (s, peer) = session_pair();
    // Its own descriptor of the socket, which outlives the session moved into the client.
    let fd = s.as_fd().try_clone_to_owned().unwrap();
    let peer = numbering_peer(peer);
    let c = GnitzClient::over(s, Box::new(PendingHost::default())).unwrap();

    let whole = Arc::new(Mutex::new(None));
    let detached: Arc<Mutex<Vec<Sent<u64>>>> = Arc::default();
    let mut calls: VecDeque<Op> = VecDeque::new();
    let out = Arc::clone(&whole);
    calls.push_back(Box::new(move |c| {
        Box::pin(async move {
            let mut ids = Vec::new();
            for _ in 0..3 {
                ids.push(c.alloc_ids(1).await.unwrap());
            }
            *out.lock().unwrap() = Some(ids);
        })
    }));
    for _ in 0..2 {
        let detached = Arc::clone(&detached);
        calls.push_back(Box::new(move |c| {
            Box::pin(async move {
                let sent = c.ack(Request::AllocIds(1)).detach();
                detached.lock().unwrap().push(sent);
            })
        }));
    }
    let (owed, mut answered) = (Arc::clone(&detached), Vec::new());
    let next = |_: &mut Context<'_>| match calls.pop_front() {
        Some(call) => Poll::Ready(Some(call)),
        // Nothing left to run, and two replies outstanding: only the loop's
        // own stepping can deliver them.
        None => {
            let mut owed = owed.lock().unwrap();
            while let Some(reply) = owed.first_mut().and_then(Sent::try_take) {
                answered.push(reply.unwrap());
                owed.remove(0);
            }
            match owed.is_empty() {
                true => Poll::Ready(None),
                false => Poll::Pending,
            }
        }
    };
    let ((), polls) = drive(fd.as_fd(), serve(c, next));
    assert_eq!(whole.lock().unwrap().take().unwrap(), [0, 1, 2]);
    assert_eq!(answered, [3, 4]);
    assert!(polls > 1, "the host pended");
    // The peer ends at EOF, which waits for the last descriptor of the socket.
    drop(fd);
    peer.join().unwrap();
}

/// A job runs where the host puts it, and its value, its panic or its loss
/// comes back to the call that offloaded it.
#[test]
fn an_offloaded_job_is_the_hosts_to_run() {
    let here = std::thread::current().id();
    let ran_on = || std::thread::current().id();
    let (s, _peer) = session_pair();
    // Its own descriptor of the socket, which outlives the session moved into the client.
    let fd = s.as_fd().try_clone_to_owned().unwrap();
    let mut c = GnitzClient::over(s, Box::new(PendingHost::default())).unwrap();
    assert_ne!(drive(fd.as_fd(), offload(&mut *c.host, ran_on)).0.unwrap(), here);
    let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        drive(fd.as_fd(), offload(&mut *c.host, || panic!("the job's own failure")))
    }));
    assert!(panicked.is_err(), "a job's panic resumes in its caller");

    // A blocking host runs it in place.
    let (mut c, _peer) = client_pair();
    assert_eq!(block_on(offload(&mut *c.host, ran_on)).unwrap(), here);

    // A host that drops the job ends the call rather than leaving it waiting.
    let (s, _peer) = session_pair();
    let host = PendingHost { drops_jobs: true, ..Default::default() };
    let mut c = GnitzClient::over(s, Box::new(host)).unwrap();
    assert!(matches!(
        drive(fd.as_fd(), offload(&mut *c.host, || 7)).0,
        Err(ClientError::Closed)
    ));
}

/// `block_on` is for a host that never pends; over any other it would spin.
#[test]
#[should_panic(expected = "belongs to an event loop")]
fn block_on_refuses_a_host_that_pends() {
    let (s, _peer) = session_pair();
    let mut c = GnitzClient::over(s, Box::new(PendingHost::default())).unwrap();
    let _ = block_on(c.alloc_ids(1));
}

/// A host that fails ends the session as that failure: the replies it owed
/// resolve as what went wrong, and the loop ends once its calls have.
#[test]
fn serve_ends_the_session_as_the_hosts_failure() {
    let (s, _peer) = session_pair();
    let c = GnitzClient::over(s, Box::new(FailingHost)).unwrap();
    let owed: Arc<Mutex<Option<Sent<u64>>>> = Arc::default();
    let out = Arc::clone(&owed);
    let mut call: Option<Op> = Some(Box::new(move |c| {
        Box::pin(async move { *out.lock().unwrap() = Some(c.ack(Request::AllocIds(1)).detach()) })
    }));
    block_on(serve(c, |_| Poll::Ready(call.take())));
    let failed = owed.lock().unwrap().take().unwrap().try_take();
    assert!(
        matches!(&failed, Some(Err(ClientError::ConnectionLost(ProtocolError::IoError(e)))) if e.to_string() == "no reactor"),
        "{failed:?}"
    );
}
