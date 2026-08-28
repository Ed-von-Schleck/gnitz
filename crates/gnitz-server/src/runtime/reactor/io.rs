//! The inbound half of a client connection: the global inbound-memory
//! budget, the frame deframer, and the [`RecvQueue`] that turns "n bytes
//! landed" into completed frames under one policy. Both client transports
//! embed a `RecvQueue`; they differ only in who writes into its window — an
//! io_uring recv CQE, or `rustls::Reader::read`.

use std::cell::Cell;
use std::collections::VecDeque;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

/// Pre-handshake limit applied to every newly registered connection.
/// Equals the HELLO payload size in bytes; any first frame larger than
/// this is rejected before allocation. The handshake elevates the
/// limit to the negotiated value via `Reactor::set_max_payload_len`.
pub(crate) const HELLO_PRE_HANDSHAKE_LEN: usize = gnitz_wire::HELLO_PAYLOAD_LEN as usize;

/// Lower/upper bounds on the global inbound-memory cap (see
/// `resolve_inbound_cap`). The floor guarantees even a tiny memory budget
/// admits at least one max-size frame; the ceiling caps the default on a
/// large box where a quarter of RAM would be an excessive inbound reserve.
pub(super) const INBOUND_CAP_FLOOR: usize = 64 << 20; // 64 MiB
pub(super) const INBOUND_CAP_CEIL: usize = 4usize << 30; // 4 GiB

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
    cap: Cell<usize>,
}

impl InboundBudget {
    pub(crate) fn new(cap: usize) -> Self {
        InboundBudget {
            held: Cell::new(0),
            cap: Cell::new(cap),
        }
    }

    /// Bytes currently held (test observability).
    #[cfg(test)]
    pub(crate) fn held(&self) -> usize {
        self.held.get()
    }

    /// Test-only: lower the ceiling so cap-trip paths can be exercised
    /// without allocating gigabytes.
    #[cfg(test)]
    pub(crate) fn set_cap(&self, cap: usize) {
        self.cap.set(cap);
    }

    /// Charge and allocate one inbound frame payload buffer. Charges
    /// `frame_weight(plen)` at header-parse time, *before* any payload byte
    /// arrives, so a declared-but-dribbled frame can never accumulate
    /// uncounted bytes; `None` = cap breach (refused before malloc, so the
    /// budget never overshoots) or malloc failure.
    fn alloc(self: &Rc<Self>, plen: usize) -> Option<RecvBuf> {
        if self.held.get() + frame_weight(plen) > self.cap.get() {
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
    pub(crate) fn new(ptr: *mut u8, len: usize, budget: Rc<InboundBudget>) -> Self {
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

enum RecvPhase {
    Header { pos: usize },
    Payload { buf: RecvBuf, pos: usize },
}

pub(crate) enum RecvAdvance {
    NeedMore,
    HeaderDone,
    MessageDone,
    Disconnect,
}

pub(crate) struct RecvState {
    hdr_buf: [u8; 4],
    phase: RecvPhase,
}

impl RecvState {
    pub(crate) fn new() -> Self {
        RecvState {
            hdr_buf: [0; 4],
            phase: RecvPhase::Header { pos: 0 },
        }
    }

    pub(crate) fn advance(&mut self, bytes_received: usize) -> RecvAdvance {
        match &mut self.phase {
            RecvPhase::Header { pos } => {
                *pos += bytes_received;
                if *pos < 4 {
                    return RecvAdvance::NeedMore;
                }
                if u32::from_le_bytes(self.hdr_buf) == 0 {
                    return RecvAdvance::Disconnect;
                }
                RecvAdvance::HeaderDone
            }
            RecvPhase::Payload { buf, pos } => {
                *pos += bytes_received;
                if *pos < buf.len {
                    return RecvAdvance::NeedMore;
                }
                RecvAdvance::MessageDone
            }
        }
    }

    pub(crate) fn remaining(&mut self) -> (*mut u8, u32) {
        match &mut self.phase {
            RecvPhase::Header { pos } => {
                let ptr = unsafe { self.hdr_buf.as_mut_ptr().add(*pos) };
                (ptr, (4 - *pos) as u32)
            }
            RecvPhase::Payload { buf, pos } => {
                let ptr = unsafe { buf.ptr.add(*pos) };
                (ptr, (buf.len - *pos) as u32)
            }
        }
    }

    pub(crate) fn payload_len(&self) -> usize {
        u32::from_le_bytes(self.hdr_buf) as usize
    }

    pub(crate) fn start_payload(&mut self, buf: RecvBuf) {
        self.phase = RecvPhase::Payload { buf, pos: 0 };
    }

    pub(crate) fn take_message(&mut self) -> RecvBuf {
        match std::mem::replace(&mut self.phase, RecvPhase::Header { pos: 0 }) {
            RecvPhase::Payload { buf, .. } => buf,
            _ => unreachable!("take_message called outside Payload phase"),
        }
    }

    /// Seed the 4-byte length header as if it had just been received, so a
    /// payload-phase test can start there.
    #[cfg(test)]
    pub(crate) fn seed_header(&mut self, payload_len: u32) {
        self.hdr_buf = payload_len.to_le_bytes();
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
    /// Complete messages awaiting pickup by `recv().await`. A queue, not a
    /// single slot: with one slot a pipelined client deadlocks once the kernel
    /// socket buffer fills, since the handler drains one message at a time
    /// while the kernel blocks the client's send.
    pending: VecDeque<RecvBuf>,
    /// The one task awaiting a message on this connection. Per-connection FIFO
    /// means there is never more than one.
    waiter: Option<Waker>,
    /// Set when the peer disconnected, the connection was forcibly closed, or
    /// the recv side hit a protocol/cap failure; a subsequent `recv()` past the
    /// drained queue resolves to `None`.
    recv_closed: bool,
    budget: Rc<InboundBudget>,
}

impl RecvQueue {
    pub(crate) fn new(budget: Rc<InboundBudget>) -> Self {
        RecvQueue {
            state: RecvState::new(),
            max_payload_len: HELLO_PRE_HANDSHAKE_LEN,
            pending: VecDeque::new(),
            waiter: None,
            recv_closed: false,
            budget,
        }
    }

    /// The window the next bytes must be written into: the unfilled tail of the
    /// 4-byte length header, or of the in-flight payload buffer.
    pub(crate) fn remaining(&mut self) -> (*mut u8, u32) {
        self.state.remaining()
    }

    pub(crate) fn set_max_payload_len(&mut self, limit: usize) {
        self.max_payload_len = limit;
    }

    pub(crate) fn recv_closed(&self) -> bool {
        self.recv_closed
    }

    /// Advance by `n` bytes just written into the window, applying the per-frame
    /// ceiling and the inbound charge and queueing a frame once one completes.
    /// Returns the window the *next* bytes must land in. `Err` ⇒ the recv side
    /// must close: oversize frame, cap breach, or the zero-length sentinel;
    /// `fd` names the connection in the cap-breach log.
    pub(crate) fn deliver(&mut self, n: usize, fd: i32) -> Result<(*mut u8, u32), ()> {
        match self.state.advance(n) {
            RecvAdvance::NeedMore => {}
            RecvAdvance::HeaderDone => {
                let plen = self.state.payload_len();
                if plen > self.max_payload_len {
                    return Err(());
                }
                let Some(buf) = self.budget.alloc(plen) else {
                    gnitz_warn!(
                        "reactor: inbound cap would be exceeded, closing fd={} (held={} B + {} B, cap={} B)",
                        fd,
                        self.budget.held.get(),
                        frame_weight(plen),
                        self.budget.cap.get(),
                    );
                    return Err(());
                };
                self.state.start_payload(buf);
            }
            RecvAdvance::MessageDone => {
                // The charged `RecvBuf` moves from the deframer into the
                // delivery queue; its accounting rides along untouched.
                self.pending.push_back(self.state.take_message());
                if let Some(w) = self.waiter.take() {
                    w.wake();
                }
            }
            RecvAdvance::Disconnect => return Err(()),
        }
        Ok(self.state.remaining())
    }

    /// Hand the next completed frame to the awaiting task. Ownership of the
    /// charged `RecvBuf` passes to the caller; its `Drop` refunds the budget
    /// once the caller is done, so the buffer stays accounted for its full
    /// residency.
    pub(crate) fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Option<RecvBuf>> {
        if let Some(buf) = self.pending.pop_front() {
            return Poll::Ready(Some(buf));
        }
        if self.recv_closed {
            return Poll::Ready(None);
        }
        self.waiter = Some(cx.waker().clone());
        Poll::Pending
    }

    /// Waker hygiene: the single slot must not hold a stale waker after the
    /// awaiting task is cancelled.
    pub(crate) fn clear_waiter(&mut self) {
        self.waiter = None;
    }

    /// Finish the recv side and wake the parked task, so its `recv().await`
    /// resolves to `None` once the queue drains. Idempotent.
    pub(crate) fn close(&mut self) {
        self.recv_closed = true;
        if let Some(w) = self.waiter.take() {
            w.wake();
        }
    }

    #[cfg(test)]
    pub(crate) fn pending(&self) -> &VecDeque<RecvBuf> {
        &self.pending
    }
}

/// Everything the reactor knows about one client fd. Removing a `Conn` frees
/// every buffer that fd charged to the inbound budget and is also the verdict
/// `recv()` reads as "peer gone", so no per-fd state outlives the connection
/// into a later incarnation of the same fd number.
pub(super) struct Conn {
    pub(super) q: RecvQueue,
    pub(super) recv_armed: bool,
    pub(super) closing: bool,
    pub(super) send_inflight: usize,
    /// A `PeerToken` still hands out this fd number; see its docs.
    pub(super) peer_held: bool,
}

impl Conn {
    pub(crate) fn new(budget: Rc<InboundBudget>) -> Self {
        Conn {
            q: RecvQueue::new(budget),
            recv_armed: false,
            closing: false,
            send_inflight: 0,
            peer_held: false,
        }
    }

    pub(super) fn has_outstanding(&self) -> bool {
        self.recv_armed || self.send_inflight > 0 || self.peer_held
    }
}
