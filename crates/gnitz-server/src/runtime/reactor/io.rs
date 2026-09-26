//! The inbound half of a client connection: the inbound-memory budget, the
//! frame queue, and the [`ClientConn`] owning a socket and its [`RecvQueue`].

use std::cell::{Cell, RefCell};
use std::mem::MaybeUninit;
use std::os::fd::{AsRawFd, OwnedFd};
use std::rc::Rc;
use std::task::Poll;

use gnitz_wire::{Deframer, FrameLenError};

use super::wake_queue::WakeQueue;

/// Lower/upper bounds on the global inbound-memory cap (see
/// [`resolve_inbound_cap`]). The floor guarantees even a tiny memory budget
/// admits at least one max-size frame; the ceiling caps the default on a
/// large box where a quarter of RAM would be an excessive inbound reserve.
const INBOUND_CAP_FLOOR: usize = gnitz_wire::MAX_FRAME_PAYLOAD;
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
    // Floor: 64 bytes per frame, so a flood of tiny frames is bounded by frame
    // count, not only by bytes.
    len.max(64)
}

/// A capped running total. Each [`Charge`] counts toward it until dropped.
pub(crate) struct Budget {
    held: Cell<usize>,
    cap: usize,
}

impl Budget {
    pub(crate) fn new(cap: usize) -> Rc<Budget> {
        Rc::new(Budget { held: Cell::new(0), cap })
    }

    pub(crate) fn held(&self) -> usize {
        self.held.get()
    }

    pub(crate) fn cap(&self) -> usize {
        self.cap
    }

    /// Count `weight` toward the total, or `None` if that would pass the cap.
    pub(crate) fn charge(self: &Rc<Self>, weight: usize) -> Option<Charge> {
        let held = self.held.get().checked_add(weight).filter(|&h| h <= self.cap)?;
        self.held.set(held);
        Some(Charge { budget: Rc::clone(self), weight })
    }
}

/// `weight` counted toward a [`Budget`] for as long as this lives.
pub(crate) struct Charge {
    budget: Rc<Budget>,
    weight: usize,
}

impl Drop for Charge {
    fn drop(&mut self) {
        self.budget.held.set(self.budget.held.get() - self.weight);
    }
}

/// A frame payload the deframer is filling, charged from its header on.
struct Payload {
    buf: Box<[MaybeUninit<u8>]>,
    charge: Charge,
}

impl Payload {
    /// Charge and allocate one frame payload buffer, or refuse the frame: the charge
    /// would pass the cap, or the allocation failed.
    fn alloc(budget: &Rc<Budget>, len: usize) -> Result<Payload, RecvEnd> {
        let weight = frame_weight(len);
        let breach = RecvEnd::CapBreach { want: weight };
        let charge = budget.charge(weight).ok_or(breach)?;
        let mut v: Vec<MaybeUninit<u8>> = Vec::new();
        v.try_reserve_exact(len).map_err(|_| breach)?;
        // SAFETY: `len` elements are reserved, and `MaybeUninit` needs no init.
        unsafe { v.set_len(len) };
        Ok(Payload { buf: v.into_boxed_slice(), charge })
    }
}

impl AsMut<[MaybeUninit<u8>]> for Payload {
    fn as_mut(&mut self) -> &mut [MaybeUninit<u8>] {
        &mut self.buf
    }
}

/// One complete inbound frame payload, charged to the inbound [`Budget`] for as
/// long as it lives.
pub struct RecvBuf {
    buf: Box<[u8]>,
    charge: Charge,
}

impl RecvBuf {
    pub fn as_slice(&self) -> &[u8] {
        &self.buf
    }

    /// Decode the frame, then free its bytes. The returned charge keeps counting
    /// them against the inbound budget, now on behalf of what `decode` built.
    pub(crate) fn decode<T>(self, decode: impl FnOnce(&[u8]) -> T) -> (T, Charge) {
        (decode(&self.buf), self.charge)
    }
}

/// Why a connection's recv side ended. Logged once, where the reactor retires
/// the connection.
#[derive(Debug, Clone, Copy)]
pub(crate) enum RecvEnd {
    /// EOF or a `close_notify`: the peer is done.
    PeerClosed,
    /// This end closed the connection.
    Closed,
    /// The recv itself failed (`-ECONNRESET`, `-EBADF`, …).
    Socket,
    /// A declared frame payload above `MAX_FRAME_PAYLOAD`.
    Oversize,
    /// The declared frame's `want` bytes would push the inbound budget past its cap.
    CapBreach { want: usize },
    /// The bytes did not parse as this transport's framing.
    Protocol,
}

impl RecvEnd {
    /// The recv side ended in order rather than failing.
    pub(crate) fn is_clean(self) -> bool {
        matches!(self, RecvEnd::PeerClosed | RecvEnd::Closed)
    }
}

impl std::fmt::Display for RecvEnd {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RecvEnd::PeerClosed => f.write_str("the peer closed"),
            RecvEnd::Closed => f.write_str("closed locally"),
            RecvEnd::Socket => f.write_str("the recv failed"),
            RecvEnd::Oversize => f.write_str("a frame over the payload ceiling"),
            RecvEnd::CapBreach { want } => write!(f, "a {want}-byte frame would pass the inbound cap"),
            RecvEnd::Protocol => f.write_str("a framing violation"),
        }
    }
}

impl From<FrameLenError> for RecvEnd {
    fn from(e: FrameLenError) -> Self {
        match e {
            FrameLenError::Zero => RecvEnd::Protocol,
            FrameLenError::Oversize { .. } => RecvEnd::Oversize,
        }
    }
}

/// The inbound half of one client connection: the deframer, the frames it has
/// completed, the single task awaiting them, and whether the recv side has ended.
pub(crate) struct RecvQueue {
    deframer: Deframer<Payload>,
    /// Complete messages awaiting pickup by `recv().await`, and the one task
    /// awaiting them. A queue, not a slot: with one slot a pipelined client
    /// deadlocks once the kernel socket buffer fills.
    frames: WakeQueue<RecvBuf>,
    /// No further frame will be queued. A drained queue then resolves to `None`.
    closed: bool,
    budget: Rc<Budget>,
}

impl RecvQueue {
    pub(crate) fn new(budget: Rc<Budget>) -> Self {
        RecvQueue {
            deframer: Deframer::default(),
            frames: WakeQueue::default(),
            closed: false,
            budget,
        }
    }

    pub(crate) fn recv_closed(&self) -> bool {
        self.closed
    }

    /// Deframe `src`, queueing every frame it completes.
    pub(crate) fn feed(&mut self, mut src: &[u8]) -> Result<(), RecvEnd> {
        let budget = &self.budget;
        while let Some(p) = self.deframer.feed(&mut src, |len| Payload::alloc(budget, len))? {
            // SAFETY: the deframer hands a payload out only once every byte is written.
            let buf = unsafe { p.buf.assume_init() };
            self.frames.push(RecvBuf { buf, charge: p.charge });
        }
        Ok(())
    }

    /// Take a completed frame without parking.
    pub(crate) fn try_recv(&mut self) -> Option<RecvBuf> {
        self.frames.pop()
    }

    /// No more frames will be queued; those already queued are still delivered.
    pub(super) fn close(&mut self) {
        self.closed = true;
        self.frames.wake();
    }

    /// [`Self::close`], and discard every queued frame. The payload in progress
    /// stays: an armed recv may still be writing into it.
    fn abort(&mut self) {
        self.frames.clear();
        self.close();
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
    pub(super) fn new(fd: OwnedFd, budget: Rc<Budget>) -> Self {
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

    /// End the recv side now and discard what it queued. Idempotent.
    pub(crate) fn abort(&self) {
        self.q.borrow_mut().abort();
    }

    /// Next frame, or `None` once the recv side is closed and drained.
    pub(crate) async fn recv(&self) -> Option<RecvBuf> {
        std::future::poll_fn(|cx| {
            let mut q = self.q.borrow_mut();
            if q.closed {
                Poll::Ready(q.frames.pop())
            } else {
                q.frames.poll(cx).map(Some)
            }
        })
        .await
    }

    /// The next already-deframed frame without parking. `None` means nothing is
    /// queued right now, not that the peer is gone.
    pub(crate) fn try_recv(&self) -> Option<RecvBuf> {
        self.q.borrow_mut().try_recv()
    }
}

/// What stands between a socket and its `RecvQueue`.
pub(crate) trait RecvFilter {
    /// Where the next socket bytes land; valid until the recv completes.
    fn window(&mut self, q: &mut RecvQueue) -> (*mut u8, u32);
    /// `n` bytes landed in the window: queue the frames they complete.
    fn ingest(&mut self, n: usize, q: &mut RecvQueue) -> Result<(), RecvEnd>;
}

/// Size of [`Plain`]'s read staging buffer.
const CARRY_BYTES: usize = 32 * 1024;

/// A socket whose bytes are the frames themselves. One carry read serves a
/// whole pipelined run; a payload tail at least a carry long is read into directly.
pub(crate) struct Plain {
    carry: Box<[MaybeUninit<u8>]>,
    /// The last window was the payload tail, not the carry.
    direct: bool,
}

impl Plain {
    pub(crate) fn new() -> Plain {
        Plain {
            carry: Box::new_uninit_slice(CARRY_BYTES),
            direct: false,
        }
    }
}

impl RecvFilter for Plain {
    fn window(&mut self, q: &mut RecvQueue) -> (*mut u8, u32) {
        let tail = q.deframer.payload_tail().filter(|t| t.len() >= CARRY_BYTES);
        self.direct = tail.is_some();
        let w = tail.unwrap_or(&mut self.carry[..]);
        (w.as_mut_ptr().cast(), w.len() as u32)
    }

    fn ingest(&mut self, n: usize, q: &mut RecvQueue) -> Result<(), RecvEnd> {
        if self.direct {
            // SAFETY: the completed recv wrote `n` bytes at the head of the tail.
            unsafe { q.deframer.filled(n) };
            return q.feed(&[]);
        }
        // SAFETY: the completed recv wrote `n` bytes into the carry.
        q.feed(unsafe { self.carry[..n].assume_init_ref() })
    }
}

#[cfg(test)]
#[path = "tests/io.rs"]
mod tests;
