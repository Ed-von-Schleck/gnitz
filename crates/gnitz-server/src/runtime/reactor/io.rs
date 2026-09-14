//! The inbound half of a client connection: the inbound-memory budget, the
//! deframer, and the [`ClientConn`] owning a socket and its [`RecvQueue`].

use std::cell::{Cell, RefCell};
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
    // Floor: a 1-byte payload really costs ~48 B (malloc's 32 B min chunk +
    // 16 B RecvBuf in the VecDeque); without the floor a tiny-frame flood
    // bypasses the byte cap ~48x. Keeps accounted bytes ≥ real RSS.
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

    /// Bytes currently held (test observability).
    #[cfg(test)]
    pub(super) fn held(&self) -> usize {
        self.held.get()
    }

    /// Charge and allocate one inbound frame payload buffer. Charges
    /// `frame_weight(plen)` at header-parse time, *before* any payload byte
    /// arrives, so a declared-but-dribbled frame can never accumulate
    /// uncounted bytes; `None` = cap breach (refused before malloc, so the
    /// budget never overshoots) or malloc failure.
    fn alloc(self: &Rc<Self>, plen: usize) -> Option<RecvBuf> {
        if self.held.get() + frame_weight(plen) > self.cap {
            return None; // refuse before malloc — no overshoot
        }
        // SAFETY: plen > 0 (zero-length frames are the close sentinel,
        // rejected before this call); null is checked below.
        let pbuf = unsafe { libc::malloc(plen) as *mut u8 };
        if pbuf.is_null() {
            return None;
        }
        // RecvBuf::new charges frame_weight(plen); its Drop refunds.
        Some(RecvBuf::new(pbuf, plen, Rc::clone(self)))
    }
}

/// One complete inbound frame payload, owned. Malloc'd when its header is
/// parsed and freed on drop on every exit path, including task cancellation at
/// an `.await` point. `ptr` is never null and `len` never 0: a failed malloc or
/// a zero-length frame closes the connection before a message is queued.
///
/// Its `frame_weight` is charged to the global [`InboundBudget`] at
/// construction and refunded on drop, so the OOM guard's accounting is tied to
/// buffer lifetime — every release path (consume, connection reap, task-cancel)
/// reconciles with no manual decrement to forget.
pub struct RecvBuf {
    pub(super) ptr: *mut u8,
    pub(super) len: usize,
    /// `Rc` rather than a raw pointer so the budget provably outlives every
    /// buffer regardless of reactor-teardown field-drop order.
    budget: Rc<InboundBudget>,
}

impl RecvBuf {
    /// Take ownership of a freshly-malloc'd `len`-byte payload buffer and charge
    /// its `frame_weight` to `budget`. The matching refund is in `Drop`.
    fn new(ptr: *mut u8, len: usize, budget: Rc<InboundBudget>) -> Self {
        budget.held.set(budget.held.get() + frame_weight(len));
        RecvBuf { ptr, len, budget }
    }

    pub fn as_slice(&self) -> &[u8] {
        // SAFETY: ptr/len describe a completed reactor recv (non-null by the
        // struct invariant); the buffer is exclusively owned until drop.
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }
}

impl Drop for RecvBuf {
    fn drop(&mut self) {
        let held = &self.budget.held;
        held.set(held.get().saturating_sub(frame_weight(self.len)));
        unsafe { libc::free(self.ptr as *mut libc::c_void) }
    }
}

/// Size of the per-connection read staging buffer.
const CARRY_BYTES: usize = 32 * 1024;

/// The deframer. One read fills the carry; every length prefix found in it
/// allocates its own payload buffer and the bytes behind it move straight
/// across, so a pipelined run costs one read rather than one per frame.
struct RecvState {
    carry: Box<[u8]>,
    /// Bytes at the front of `carry` the parse has not consumed. At most a split
    /// length prefix: [`RecvQueue::deliver`] compacts before it returns.
    filled: usize,
    /// The payload a length prefix has been parsed for, and how much of it has
    /// arrived. `None` between frames, and while it is `Some` the carry is empty.
    pending: Option<(RecvBuf, usize)>,
}

impl RecvState {
    fn new() -> Self {
        RecvState {
            carry: vec![0u8; CARRY_BYTES].into_boxed_slice(),
            filled: 0,
            pending: None,
        }
    }

    /// The window the next bytes must land in. A payload whose unfilled tail
    /// would take a whole read on its own is read into directly, so a large frame
    /// is not copied through the carry a carry at a time.
    fn remaining(&mut self) -> (*mut u8, u32) {
        if let Some((buf, pos)) = &self.pending {
            if buf.len - *pos >= self.carry.len() {
                // SAFETY: `pos <= buf.len`, and the buffer is exclusively ours.
                return (unsafe { buf.ptr.add(*pos) }, (buf.len - *pos) as u32);
            }
        }
        // `filled` is at most a split prefix, so this is never empty.
        (
            unsafe { self.carry.as_mut_ptr().add(self.filled) },
            (self.carry.len() - self.filled) as u32,
        )
    }
}

/// The inbound half of one client connection: the deframer, the frames it has
/// completed, the single task awaiting them, and the recv-closed verdict. Both
/// transports embed one, so the whole ingress policy is written once.
pub(crate) struct RecvQueue {
    state: RecvState,
    /// Per-connection ceiling on incoming frame payload size. Initialised to
    /// `HELLO_PRE_HANDSHAKE_LEN` (HELLO payload size) so a peer sending garbage
    /// as its first frame cannot drive an allocation larger than the HELLO
    /// message. Elevated to the negotiated transport limit after HELLO
    /// validation.
    max_payload_len: usize,
    /// Complete messages awaiting pickup by `recv().await`, and the one task
    /// awaiting them. A queue, not a slot: with one slot a pipelined client
    /// deadlocks once the kernel socket buffer fills. Closed on disconnect,
    /// forced close, or a protocol/cap failure.
    frames: WakeQueue<RecvBuf>,
    budget: Rc<InboundBudget>,
}

impl RecvQueue {
    pub(crate) fn new(budget: Rc<InboundBudget>) -> Self {
        RecvQueue {
            state: RecvState::new(),
            max_payload_len: HELLO_PRE_HANDSHAKE_LEN,
            frames: WakeQueue::default(),
            budget,
        }
    }

    /// The window the next bytes must be written into.
    pub(crate) fn remaining(&mut self) -> (*mut u8, u32) {
        self.state.remaining()
    }

    pub(crate) fn set_max_payload_len(&mut self, limit: usize) {
        self.max_payload_len = limit;
    }

    pub(crate) fn recv_closed(&self) -> bool {
        self.frames.is_closed()
    }

    /// Advance by `n` bytes just written into the window, queueing every frame
    /// they complete — one read can carry a whole pipelined run. Returns the
    /// window the *next* bytes must land in. `Err` ⇒ the recv side must close:
    /// oversize frame, cap breach, or the zero-length sentinel; `fd` names the
    /// connection in the cap-breach log.
    pub(crate) fn deliver(&mut self, n: usize, fd: i32) -> Result<(*mut u8, u32), ()> {
        let RecvState { carry, filled, pending } = &mut self.state;
        // Where the bytes landed — the same test `remaining` chose the window by.
        match pending {
            Some((buf, pos)) if buf.len - *pos >= carry.len() => *pos += n,
            _ => *filled += n,
        }
        // How much of the carry the parse has consumed.
        let mut cur = 0;
        loop {
            if let Some((buf, mut pos)) = pending.take() {
                // Whatever the read carried past this frame's length prefix; a
                // direct read landed the rest in `buf` already.
                let take = (buf.len - pos).min(*filled - cur);
                // SAFETY: `take` is bounded by both the carry's unconsumed span
                // and the payload's unfilled tail; the two allocations are
                // distinct.
                unsafe {
                    std::ptr::copy_nonoverlapping(carry.as_ptr().add(cur), buf.ptr.add(pos), take);
                }
                pos += take;
                cur += take;
                if pos < buf.len {
                    *pending = Some((buf, pos));
                    break;
                }
                // The charged `RecvBuf` moves from the deframer into the
                // delivery queue; its accounting rides along untouched.
                self.frames.push(buf);
                continue;
            }
            if *filled - cur < FRAME_LEN_PREFIX_BYTES {
                break;
            }
            let hdr: [u8; FRAME_LEN_PREFIX_BYTES] = carry[cur..cur + FRAME_LEN_PREFIX_BYTES].try_into().unwrap();
            let plen = u32::from_le_bytes(hdr) as usize;
            // Zero is the close sentinel, not a frame.
            if plen == 0 || plen > self.max_payload_len {
                return Err(());
            }
            let Some(buf) = self.budget.alloc(plen) else {
                gnitz_warn!(
                    "reactor: inbound cap would be exceeded, closing fd={} (held={} B + {} B, cap={} B)",
                    fd,
                    self.budget.held.get(),
                    frame_weight(plen),
                    self.budget.cap,
                );
                return Err(());
            };
            cur += FRAME_LEN_PREFIX_BYTES;
            *pending = Some((buf, 0));
        }
        // Only a split length prefix can be left — every other exit consumed the
        // carry whole — so this moves at most three bytes to the front, and the
        // next window is the rest of the carry.
        carry.copy_within(cur..*filled, 0);
        *filled -= cur;
        Ok(self.state.remaining())
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
        self.frames.poll(cx)
    }

    /// Finish the recv side and wake the parked task, so its `recv().await`
    /// resolves to `None` once the queue drains. Idempotent.
    pub(crate) fn close(&mut self) {
        self.frames.close();
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

    pub(crate) fn fd(&self) -> i32 {
        self.fd.as_raw_fd()
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

    /// Must run before the caller's next `.await`: the recv re-arms as soon as the
    /// current frame is deframed, and a frame arriving before the raise is refused
    /// at the old ceiling.
    pub(crate) fn set_max_payload_len(&self, limit: usize) {
        self.q.borrow_mut().set_max_payload_len(limit);
    }

    pub(crate) fn recv_closed(&self) -> bool {
        self.q.borrow().recv_closed()
    }
}

/// What stands between a socket and its `RecvQueue` when the socket's bytes are
/// not the frames themselves.
pub(crate) trait RecvFilter {
    /// Where the next socket bytes land; valid until the recv completes.
    fn window(&mut self) -> (*mut u8, u32);
    /// `n` bytes landed in the window: queue the frames they complete. `Err`
    /// ends the recv side.
    fn ingest(&mut self, n: usize, q: &mut RecvQueue, fd: i32) -> Result<(), ()>;
    /// The recv side ended.
    fn recv_closed(&mut self, fd: i32);
}

#[cfg(test)]
#[path = "tests/io.rs"]
mod tests;
