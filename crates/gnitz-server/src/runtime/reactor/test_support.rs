//! Helpers shared by the reactor's test suites.
//!
//! Private to `reactor` and visible to every module under it, so a suite
//! attached to `conn`, `io` or `futures` reaches the same fixtures without any
//! of them becoming crate API.

use std::cell::Cell as StdCell;

use super::*;

pub(super) fn make_reactor() -> Reactor {
    Reactor::new(16).expect("reactor")
}

/// Future that returns Pending exactly once, then Ready.
pub(super) struct YieldOnce {
    yielded: bool,
}

impl YieldOnce {
    pub(super) fn new() -> Self {
        YieldOnce { yielded: false }
    }
}

impl Future for YieldOnce {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.yielded {
            Poll::Ready(())
        } else {
            self.yielded = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}

/// Wakes itself a few times, then completes. Mirrors a tight async
/// loop that would burn the reactor if double-polled per wake.
pub(super) struct DoublyWaking {
    pub(super) polls: Rc<StdCell<u32>>,
    pub(super) polled: u32,
}

impl Future for DoublyWaking {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let n = self.polled;
        self.polls.set(self.polls.get() + 1);
        if n >= 3 {
            return Poll::Ready(());
        }
        self.polled = n + 1;
        // Wake twice before returning Pending: must not get polled
        // twice in the same tick, only once on the next tick.
        cx.waker().wake_by_ref();
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}

/// Race a timer and a reply future. Sets `flag` to true if the reply
/// resolved first, leaves it false if the timer won. Polls both each
/// tick; the first to return Ready wins.
pub(super) async fn select_reply_or_timer<'a, T, R>(timer: T, reply: R, flag: &'a Rc<StdCell<bool>>)
where
    T: Future<Output = ()> + 'a,
    R: Future<Output = DecodedWire> + 'a,
{
    let mut timer = Box::pin(timer);
    let mut reply = Box::pin(reply);
    std::future::poll_fn(move |cx| {
        if timer.as_mut().poll(cx).is_ready() {
            return Poll::Ready(());
        }
        if reply.as_mut().poll(cx).is_ready() {
            flag.set(true);
            return Poll::Ready(());
        }
        Poll::Pending
    })
    .await
}

/// Build a minimal `DecodedWire` for tests — only `request_id` matters.
pub(super) fn synthetic_decoded_wire(req_id: u64) -> DecodedWire {
    use gnitz_wire::control::DecodedControl;
    DecodedWire {
        control: DecodedControl {
            request_id: req_id,
            ..Default::default()
        },
        schema: None,
        data_batch: None,
    }
}

/// A socketpair set up for an egress test: the reactor, the sender fd, and
/// the receiver end. The sender is registered in `conns` so `send_inflight`
/// accounting has something to touch, as in real flow. `sndbuf` shrinks both
/// socket buffers, so a payload larger than it is guaranteed to split across
/// several OP_SEND CQEs. The caller closes both fds.
pub(super) unsafe fn egress_pair(sndbuf: Option<i32>) -> (Rc<Reactor>, i32, i32) {
    let mut fds = [0i32; 2];
    assert_eq!(
        libc::socketpair(libc::AF_UNIX, libc::SOCK_STREAM, 0, fds.as_mut_ptr()),
        0,
        "socketpair"
    );
    let (sender, receiver) = (fds[0], fds[1]);
    if let Some(bytes) = sndbuf {
        for (fd, opt) in [(sender, libc::SO_SNDBUF), (receiver, libc::SO_RCVBUF)] {
            libc::setsockopt(
                fd,
                libc::SOL_SOCKET,
                opt,
                &bytes as *const _ as *const libc::c_void,
                std::mem::size_of::<i32>() as u32,
            );
        }
    }
    let r: Rc<Reactor> = Rc::new(make_reactor());
    r.inner
        .conns
        .borrow_mut()
        .insert(sender, Box::new(io::Conn::new(Rc::clone(&r.inner.inbound))));
    (r, sender, receiver)
}

/// Read `expect` bytes off `fd` and close it, so the send under test never
/// stalls on a full socket buffer. Returns what it actually saw.
pub(super) fn spawn_drain(fd: i32, expect: usize) -> std::thread::JoinHandle<usize> {
    std::thread::spawn(move || unsafe {
        let mut seen = 0usize;
        let mut scratch = vec![0u8; 64 * 1024];
        while seen < expect {
            let n = libc::read(fd, scratch.as_mut_ptr() as *mut libc::c_void, scratch.len());
            if n <= 0 {
                break;
            }
            seen += n as usize;
        }
        libc::close(fd);
        seen
    })
}

/// Build a length-prefixed wire frame: 4-byte LE payload length + payload.
pub(super) fn framed(payload: &[u8]) -> Vec<u8> {
    let mut v = Vec::with_capacity(4 + payload.len());
    v.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    v.extend_from_slice(payload);
    v
}

/// AF_UNIX SOCK_STREAM pair. Returns `(server_read_fd, client_write_fd)`:
/// the reactor `register_conn`s the first and recvs from it; the test
/// `write_all`s framed bytes into the second.
pub(super) unsafe fn stream_pair() -> (i32, i32) {
    let mut fds = [0i32; 2];
    let rc = libc::socketpair(libc::AF_UNIX, libc::SOCK_STREAM, 0, fds.as_mut_ptr());
    assert_eq!(rc, 0, "socketpair");
    (fds[1], fds[0])
}

/// `(read_end, write_end)` of a fresh pipe. Tests that need a real,
/// owned fd number — `reap_closing_conns` calls `libc::close` on what it
/// reaps, so a magic number would race a parallel test that owns it.
pub(super) unsafe fn pipe_pair() -> (i32, i32) {
    let mut fds = [0i32; 2];
    assert_eq!(libc::pipe(fds.as_mut_ptr()), 0, "pipe");
    (fds[0], fds[1])
}

/// Poll a fresh `recv(fd)` future exactly once, returning its result:
/// `Some(_)` if it resolved (a frame, or `None` when closed), `None` if
/// still pending.
pub(super) fn poll_recv_once(r: &Reactor, fd: i32) -> Option<Option<io::RecvBuf>> {
    let mut fut = Box::pin(r.recv(fd));
    let waker = make_waker(usize::MAX);
    let mut cx = Context::from_waker(&waker);
    match fut.as_mut().poll(&mut cx) {
        Poll::Ready(v) => Some(v),
        Poll::Pending => None,
    }
}

/// Drive the reactor up to `max` non-blocking ticks, returning `true` as
/// soon as `cond` holds after a tick (and `false` if it never does).
pub(super) fn poll_until(r: &Reactor, max: usize, mut cond: impl FnMut() -> bool) -> bool {
    (0..max).any(|_| {
        r.tick(false);
        cond()
    })
}

/// A real fd to stand in for a listener, so the `CQE_F_MORE == 0` re-arm
/// these tests trigger targets something the kernel will accept.
pub(super) fn fake_listener() -> i32 {
    unsafe { pipe_pair().0 }
}

/// Build a minimal W2M ring and write one scan slot with `req_id`.
/// The returned `W2mReceiver` owns the `InFlightState` that the slot's Drop
/// references — it must be kept alive until after the slot is dropped.
/// The returned `SharedRegion` unmaps the ring on drop.
pub(super) unsafe fn make_scan_slot(
    req_id: u32,
) -> (
    crate::runtime::w2m::W2mSlot,
    crate::runtime::w2m::W2mReceiver,
    gnitz_engine_testkit::SharedRegion,
) {
    let (receiver, region) = make_scan_ring(req_id, 1);
    let slot = receiver.try_read_slot(0).expect("scan slot");
    (slot, receiver, region)
}

/// Build a W2M ring carrying `n` scan frames, all tagged with
/// `internal_req_id` but with distinct wire request_ids (100, 101, …) so the
/// caller can verify arrival order. The returned `W2mReceiver` owns the
/// `InFlightState` the slots reference; caller reads frames via
/// `receiver.try_read_slot(0)`; the returned `SharedRegion` unmaps the ring
/// on drop, after both receiver and all slots drop.
pub(super) unsafe fn make_scan_ring(
    internal_req_id: u32,
    n: usize,
) -> (crate::runtime::w2m::W2mReceiver, gnitz_engine_testkit::SharedRegion) {
    use crate::runtime::w2m::{W2mReceiver, W2mWriter};
    use crate::runtime::wire as ipc;

    let region = crate::runtime::w2m::test_ring(64 * 1024);
    let ptr = region.ptr();

    let writer = W2mWriter::new(ptr);
    let receiver = W2mReceiver::new(vec![ptr]);
    for i in 0..n {
        let wire_req = 100u64 + i as u64;
        let msg = ipc::WireMsg {
            request_id: wire_req,
            ..Default::default()
        };
        writer.send_msg(internal_req_id as u64, &msg);
    }
    (receiver, region)
}
