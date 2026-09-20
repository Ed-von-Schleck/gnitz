//! The inbound half of a client connection: the inbound-memory budget, the
//! deframer, and the [`ClientConn`] owning a socket and its [`RecvQueue`].

use std::cell::{Cell, RefCell};
use std::mem::MaybeUninit;
use std::os::fd::{AsRawFd, OwnedFd};
use std::rc::Rc;
use std::task::{Context, Poll};

use gnitz_wire::FRAME_LEN_PREFIX_BYTES;

use super::wake_queue::WakeQueue;

/// A new connection's frame ceiling, until [`ClientConn::set_max_payload_len`] raises it.
const HELLO_PRE_HANDSHAKE_LEN: usize = gnitz_wire::HELLO_PAYLOAD_LEN as usize;

/// Lower/upper bounds on the global inbound-memory cap (see
/// [`resolve_inbound_cap`]). The floor guarantees even a tiny memory budget
/// admits at least one max-size frame; the ceiling caps the default on a
/// large box where a quarter of RAM would be an excessive inbound reserve.
const INBOUND_CAP_FLOOR: usize = 64 << 20; // 64 MiB
const INBOUND_CAP_CEIL: usize = 4usize << 30; // 4 GiB

/// The global inbound-memory ceiling, resolved once at startup: a quarter of the
/// process memory budget, or `GNITZ_INBOUND_MEM_BYTES`, never outside the bounds
/// above. A fraction of the *actual* budget scales with the deployment where a
/// flat constant would OOM a mid-size box yet never trip in a small container.
pub(super) fn resolve_inbound_cap() -> usize {
    let host = gnitz_foundation::host::available_memory_bytes();
    let default = (host / 4).clamp(INBOUND_CAP_FLOOR, INBOUND_CAP_CEIL);
    gnitz_foundation::env::env_num("GNITZ_INBOUND_MEM_BYTES", default).max(INBOUND_CAP_FLOOR)
}

/// Accounted memory weight of one inbound payload buffer of `len` bytes.
#[inline]
fn frame_weight(len: usize) -> usize {
    // Floor: a 1-byte payload really costs tens of bytes — the allocator's
    // minimum chunk plus the `RecvBuf` beside it — so without a floor a
    // tiny-frame flood slips far under the byte cap. Keeps accounted bytes ≥ RSS.
    len.max(64)
}

/// The global inbound-memory budget: `held` is the summed `frame_weight` of
/// every live inbound `RecvBuf` — in flight, queued, or held by a consumer —
/// and `cap` is the ceiling a new allocation may not push it past. Every
/// `RecvBuf` holds an `Rc` to it, so a charge outlives its owner's teardown
/// order and consume / reap / task-cancel all refund with no shadow counter.
pub(crate) struct InboundBudget {
    held: Cell<usize>,
    cap: usize,
}

impl InboundBudget {
    pub(crate) fn new(cap: usize) -> Self {
        InboundBudget { held: Cell::new(0), cap }
    }

    /// Bytes currently held.
    pub(super) fn held(&self) -> usize {
        self.held.get()
    }

    /// Charge and allocate one frame payload buffer. `None` = the charge would
    /// breach the cap, or the allocation failed; neither charges anything.
    fn alloc(self: &Rc<Self>, plen: usize) -> Option<RecvBuf> {
        if self.held.get() + frame_weight(plen) > self.cap {
            return None;
        }
        let mut v: Vec<MaybeUninit<u8>> = Vec::new();
        v.try_reserve_exact(plen).ok()?;
        // SAFETY: `plen` elements are reserved, and `MaybeUninit` needs no init.
        unsafe { v.set_len(plen) };
        Some(RecvBuf::new(v.into_boxed_slice(), Rc::clone(self)))
    }
}

/// One complete inbound frame payload, owned. Its `frame_weight` is charged to
/// the global [`InboundBudget`] at construction and refunded on drop, so the
/// charge lasts exactly as long as the buffer.
pub struct RecvBuf {
    buf: Box<[MaybeUninit<u8>]>,
    /// `Rc` rather than a raw pointer so the budget provably outlives every
    /// buffer regardless of reactor-teardown field-drop order.
    budget: Rc<InboundBudget>,
}

impl RecvBuf {
    fn new(buf: Box<[MaybeUninit<u8>]>, budget: Rc<InboundBudget>) -> Self {
        budget.held.set(budget.held.get() + frame_weight(buf.len()));
        RecvBuf { buf, budget }
    }

    /// Whether the unfilled `pos..` tail would take a whole `carry_len` read on its
    /// own, and so is read into directly instead of through the carry a carry at a time.
    fn takes_a_whole_read(&self, pos: usize, carry_len: usize) -> bool {
        self.buf.len() - pos >= carry_len
    }

    pub fn as_slice(&self) -> &[u8] {
        // SAFETY: the deframer queues a frame only once every byte of `buf` has
        // been written.
        unsafe { self.buf.assume_init_ref() }
    }
}

impl Drop for RecvBuf {
    fn drop(&mut self) {
        let held = &self.budget.held;
        held.set(held.get().saturating_sub(frame_weight(self.buf.len())));
    }
}

/// Size of the per-connection read staging buffer.
const CARRY_BYTES: usize = 32 * 1024;

/// Why a connection's recv side ended. Logged once, where the reactor retires
/// the connection.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum RecvEnd {
    /// EOF, a close sentinel, or a `close_notify`: the peer is done.
    PeerClosed,
    /// The recv itself failed (`-ECONNRESET`, `-EBADF`, …).
    Socket,
    /// A declared frame payload above the connection's ceiling.
    Oversize,
    /// The declared frame's `want` bytes would push the inbound budget past its cap.
    CapBreach { want: usize },
    /// The bytes did not parse as this transport's framing.
    Protocol,
}

/// The inbound half of one client connection: the deframer, the frames it has
/// completed, the single task awaiting them, and the recv-closed verdict. Both
/// transports embed one, so the whole ingress policy is written once.
pub(crate) struct RecvQueue {
    /// The deframer's staging buffer. One read fills it; every length prefix found
    /// in it allocates its own payload buffer and the bytes behind it move straight
    /// across, so a pipelined run costs one read rather than one per frame.
    /// Uninitialised: only bytes a completed recv wrote are ever read back.
    carry: Box<[MaybeUninit<u8>]>,
    /// Bytes at the front of `carry` the parse has not consumed. At most a split
    /// length prefix: [`RecvQueue::deliver`] compacts before it returns.
    filled: usize,
    /// The payload a length prefix has been parsed for, and how much of it has
    /// arrived. `None` between frames, and while it is `Some` the carry is empty.
    pending: Option<(RecvBuf, usize)>,
    /// Per-connection ceiling on incoming frame payload size. Initialised to
    /// `HELLO_PRE_HANDSHAKE_LEN` (HELLO payload size) so a peer sending garbage
    /// as its first frame cannot drive an allocation larger than the HELLO
    /// message. Elevated to the negotiated transport limit after HELLO
    /// validation.
    max_payload_len: usize,
    /// Complete messages awaiting pickup by `recv().await`, and the one task
    /// awaiting them. A queue, not a slot: with one slot a pipelined client
    /// deadlocks once the kernel socket buffer fills.
    frames: WakeQueue<RecvBuf>,
    /// No further frame will be queued: set on disconnect, forced close, or a
    /// protocol/cap failure. A drained queue then resolves to `None`.
    closed: bool,
    budget: Rc<InboundBudget>,
}

impl RecvQueue {
    pub(crate) fn new(budget: Rc<InboundBudget>) -> Self {
        RecvQueue {
            carry: Box::new_uninit_slice(CARRY_BYTES),
            filled: 0,
            pending: None,
            max_payload_len: HELLO_PRE_HANDSHAKE_LEN,
            frames: WakeQueue::default(),
            closed: false,
            budget,
        }
    }

    /// The window the next bytes must land in.
    pub(crate) fn remaining(&mut self) -> (*mut u8, u32) {
        let carry_len = self.carry.len();
        if let Some((buf, pos)) = &mut self.pending {
            if buf.takes_a_whole_read(*pos, carry_len) {
                let tail = &mut buf.buf[*pos..];
                return (tail.as_mut_ptr().cast::<u8>(), tail.len() as u32);
            }
        }
        // `filled` is at most a split prefix, so this is never empty.
        let tail = &mut self.carry[self.filled..];
        (tail.as_mut_ptr().cast::<u8>(), tail.len() as u32)
    }

    pub(crate) fn set_max_payload_len(&mut self, limit: usize) {
        self.max_payload_len = limit;
    }

    pub(crate) fn recv_closed(&self) -> bool {
        self.closed
    }

    /// Advance by `n` bytes just written into the window, queueing every frame
    /// they complete — one read can carry a whole pipelined run. The window the
    /// *next* bytes must land in is [`Self::remaining`], unchanged by an `Err`.
    pub(crate) fn deliver(&mut self, n: usize) -> Result<(), RecvEnd> {
        let RecvQueue {
            carry,
            filled,
            pending,
            max_payload_len,
            frames,
            budget,
            ..
        } = self;
        // Where the bytes landed — the same test `remaining` chose the window by.
        match pending {
            Some((buf, pos)) if buf.takes_a_whole_read(*pos, carry.len()) => *pos += n,
            _ => *filled += n,
        }
        // How much of the carry the parse has consumed.
        let mut cur = 0;
        // SAFETY: `filled` counts bytes a completed recv wrote into the carry.
        let init: &[u8] = unsafe { carry[..*filled].assume_init_ref() };
        loop {
            if let Some((mut buf, mut pos)) = pending.take() {
                // Whatever the read carried past this frame's length prefix; a
                // direct read landed the rest in `buf` already.
                let take = (buf.buf.len() - pos).min(*filled - cur);
                buf.buf[pos..pos + take].write_copy_of_slice(&init[cur..cur + take]);
                pos += take;
                cur += take;
                if pos < buf.buf.len() {
                    *pending = Some((buf, pos));
                    break;
                }
                // The charged `RecvBuf` moves from the deframer into the
                // delivery queue; its accounting rides along untouched.
                frames.push(buf);
                continue;
            }
            if *filled - cur < FRAME_LEN_PREFIX_BYTES {
                break;
            }
            let hdr: [u8; FRAME_LEN_PREFIX_BYTES] = init[cur..cur + FRAME_LEN_PREFIX_BYTES].try_into().unwrap();
            let plen = u32::from_le_bytes(hdr) as usize;
            // Zero is the close sentinel, not a frame.
            if plen == 0 {
                return Err(RecvEnd::PeerClosed);
            }
            if plen > *max_payload_len {
                return Err(RecvEnd::Oversize);
            }
            let Some(buf) = budget.alloc(plen) else {
                return Err(RecvEnd::CapBreach { want: frame_weight(plen) });
            };
            cur += FRAME_LEN_PREFIX_BYTES;
            *pending = Some((buf, 0));
        }
        // Only a split length prefix can be left — every other exit consumed the
        // carry whole — so this moves at most three bytes to the front, and the
        // next window is the rest of the carry.
        carry.copy_within(cur..*filled, 0);
        *filled -= cur;
        Ok(())
    }

    /// Take a completed frame without parking, beside [`Self::poll_recv`].
    pub(crate) fn try_recv(&mut self) -> Option<RecvBuf> {
        self.frames.pop()
    }

    /// Hand the next completed frame to the awaiting task. Ownership of the
    /// charged `RecvBuf` passes to the caller; its `Drop` refunds the budget
    /// once the caller is done, so the buffer stays accounted for its full
    /// residency.
    pub(crate) fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Option<RecvBuf>> {
        if let Some(buf) = self.frames.pop() {
            return Poll::Ready(Some(buf));
        }
        if self.closed {
            return Poll::Ready(None);
        }
        self.frames.park(cx);
        Poll::Pending
    }

    /// Finish the recv side and wake the parked task, so its `recv().await`
    /// resolves to `None` once the queue drains. Idempotent.
    pub(crate) fn close(&mut self) {
        self.closed = true;
        self.frames.wake();
    }

    #[cfg(test)]
    pub(super) fn queued(&self) -> usize {
        self.frames.len()
    }
}

/// One client connection: its socket and the frames deframed off it. The socket
/// closes with the last holder, and every SQE naming it is queued through one.
pub(crate) struct ClientConn {
    fd: OwnedFd,
    /// Closed when the recv side ends, after which a completing recv never
    /// re-arms.
    pub(super) q: RefCell<RecvQueue>,
}

impl ClientConn {
    pub(super) fn new(fd: OwnedFd, budget: Rc<InboundBudget>) -> Self {
        ClientConn {
            fd,
            q: RefCell::new(RecvQueue::new(budget)),
        }
    }

    pub(super) fn fd(&self) -> i32 {
        self.fd.as_raw_fd()
    }

    /// `shutdown(SHUT_RDWR)`: errors out a pending recv or send on the socket, which
    /// `close` would not. Never closes the fd; a peer already gone is not an error.
    pub(crate) fn shutdown(&self) {
        let _ = gnitz_foundation::posix_io::retry_eintr(|| unsafe { libc::shutdown(self.fd(), libc::SHUT_RDWR) });
    }

    /// End the recv side now and shut the socket down, so an armed recv completes
    /// even for a client that never sends again. Idempotent.
    pub(crate) fn close(&self) {
        self.q.borrow_mut().close();
        self.shutdown();
    }

    /// Next frame, or `None` once the recv side is closed and drained.
    pub(crate) async fn recv(&self) -> Option<RecvBuf> {
        std::future::poll_fn(|cx| self.q.borrow_mut().poll_recv(cx)).await
    }

    /// The next already-deframed frame without parking. `None` means nothing is
    /// queued right now, not that the peer is gone.
    pub(crate) fn try_recv(&self) -> Option<RecvBuf> {
        self.q.borrow_mut().try_recv()
    }

    /// Raise the ceiling on incoming frame payloads to the HELLO-negotiated limit.
    pub(crate) fn set_max_payload_len(&self, limit: usize) {
        self.q.borrow_mut().set_max_payload_len(limit);
    }
}

/// What stands between a socket and its `RecvQueue`.
pub(crate) trait RecvFilter {
    /// Where the next socket bytes land; valid until the recv completes.
    fn window(&mut self, q: &mut RecvQueue) -> (*mut u8, u32);
    /// `n` bytes landed in the window: queue the frames they complete.
    fn ingest(&mut self, n: usize, q: &mut RecvQueue) -> Result<(), RecvEnd>;
}

/// A socket whose bytes are the frames themselves.
pub(crate) struct Plain;

impl RecvFilter for Plain {
    fn window(&mut self, q: &mut RecvQueue) -> (*mut u8, u32) {
        q.remaining()
    }

    fn ingest(&mut self, n: usize, q: &mut RecvQueue) -> Result<(), RecvEnd> {
        q.deliver(n)
    }
}

#[cfg(test)]
#[path = "tests/io.rs"]
mod tests;
