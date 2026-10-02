//! The inbound half of a client connection: the inbound-memory budget, the
//! frame queue, and the [`ClientConn`] owning a socket and its [`RecvQueue`].

use std::cell::{Cell, OnceCell, RefCell};
use std::mem::MaybeUninit;
use std::os::fd::{AsRawFd, OwnedFd};
use std::rc::Rc;

use gnitz_wire::{Deframer, FrameLenError};

use super::sync::chan;
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
    /// The recv itself failed (`-ECONNRESET`, `-EBADF`, …).
    Socket,
    /// A declared frame payload above `MAX_FRAME_PAYLOAD`.
    Oversize,
    /// The declared frame's `want` bytes would push the inbound budget past its cap.
    CapBreach { want: usize },
    /// The declared frame's `len`-byte buffer could not be allocated.
    Alloc { len: usize },
    /// The bytes did not parse as this transport's framing.
    Protocol,
}

impl std::fmt::Display for RecvEnd {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RecvEnd::PeerClosed => f.write_str("the peer closed"),
            RecvEnd::Socket => f.write_str("the recv failed"),
            RecvEnd::Oversize => f.write_str("a frame over the payload ceiling"),
            RecvEnd::CapBreach { want } => write!(f, "a {want}-byte frame would pass the inbound cap"),
            RecvEnd::Alloc { len } => write!(f, "a {len}-byte frame could not be allocated"),
            RecvEnd::Protocol => f.write_str("a framing violation"),
        }
    }
}

impl From<FrameLenError> for RecvEnd {
    fn from(e: FrameLenError) -> Self {
        match e {
            FrameLenError::Zero => RecvEnd::Protocol,
            FrameLenError::Oversize { .. } => RecvEnd::Oversize,
            FrameLenError::Alloc { len } => RecvEnd::Alloc { len },
        }
    }
}

/// The inbound half of one client connection: the deframer, the frames it has
/// completed, and the single task awaiting them.
pub(crate) struct RecvQueue {
    /// Each payload in progress is charged to `budget` from its header on.
    deframer: Deframer<Charge>,
    /// Complete messages awaiting pickup by `recv().await`, and the one task
    /// awaiting them. A queue, not a slot: with one slot a pipelined client
    /// deadlocks once the kernel socket buffer fills.
    frames: WakeQueue<RecvBuf>,
    budget: Rc<Budget>,
}

impl RecvQueue {
    pub(crate) fn new(budget: Rc<Budget>) -> Self {
        RecvQueue {
            deframer: Deframer::default(),
            frames: WakeQueue::default(),
            budget,
        }
    }

    /// Deframe `src`, queueing every frame it completes.
    pub(crate) fn feed(&mut self, mut src: &[u8]) -> Result<(), RecvEnd> {
        let budget = &self.budget;
        while let Some((buf, charge)) = self.deframer.feed(&mut src, |len| {
            let want = frame_weight(len);
            budget.charge(want).ok_or(RecvEnd::CapBreach { want })
        })? {
            self.frames.push(RecvBuf { buf, charge });
        }
        Ok(())
    }

    /// Take a completed frame without parking.
    pub(crate) fn try_recv(&mut self) -> Option<RecvBuf> {
        self.frames.pop()
    }
}

/// One client connection: its socket and the frames deframed off it. The socket
/// closes with the last holder, and every SQE naming it is queued through one.
pub(crate) struct ClientConn {
    fd: OwnedFd,
    /// This connection's place under the connection cap, held exactly as long
    /// as the fd.
    _slot: Charge,
    pub(super) q: RefCell<RecvQueue>,
    gone: Cell<bool>,
    /// Wakes the task that sends on this socket outside requests, which then
    /// owns the shutdown.
    egress_owner: OnceCell<chan::Sender<()>>,
}

impl ClientConn {
    pub(super) fn new(fd: OwnedFd, slot: Charge, budget: Rc<Budget>) -> Self {
        ClientConn {
            fd,
            _slot: slot,
            q: RefCell::new(RecvQueue::new(budget)),
            gone: Cell::new(false),
            egress_owner: OnceCell::new(),
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

    pub(crate) fn is_gone(&self) -> bool {
        self.gone.get()
    }

    /// The client closed its sending side. Frames already queued are still
    /// delivered, and replies still go out.
    pub(super) fn end_recv(&self) {
        self.q.borrow_mut().frames.close();
    }

    /// Finish the connection and discard the frames it queued. The egress owner,
    /// if any, shuts the socket down once it has shipped what it holds.
    pub(crate) fn retire(&self) {
        if self.gone.replace(true) {
            return;
        }
        {
            let mut q = self.q.borrow_mut();
            q.frames.clear();
            q.frames.close();
        }
        match self.egress_owner.get() {
            Some(owner) => owner.send(()),
            None => self.shutdown(),
        }
    }

    /// [`Self::retire`], and shut the socket down now whoever owns egress.
    pub(crate) fn fail(&self) {
        self.retire();
        self.shutdown();
    }

    /// Hand egress to the task behind `owner`.
    pub(crate) fn set_egress_owner(&self, owner: chan::Sender<()>) {
        debug_assert!(!self.is_gone(), "egress handed over after the end");
        assert!(self.egress_owner.set(owner).is_ok(), "egress has one owner");
    }

    pub(crate) fn wake_egress(&self) {
        if let Some(owner) = self.egress_owner.get() {
            owner.send(());
        }
    }

    /// Next frame, or `None` once the recv side has ended and is drained.
    pub(crate) async fn recv(&self) -> Option<RecvBuf> {
        std::future::poll_fn(|cx| self.q.borrow_mut().frames.poll(cx)).await
    }

    /// The next already-deframed frame without parking. `None` means nothing is
    /// queued right now, not that the peer is gone.
    pub(crate) fn try_recv(&self) -> Option<RecvBuf> {
        self.q.borrow_mut().try_recv()
    }
}

/// What stands between a socket and its `RecvQueue`.
pub(crate) trait RecvFilter {
    /// Where the next socket bytes land. The reactor keeps the address until the
    /// recv completes, and calls nothing on the filter in between.
    fn window<'a>(&'a mut self, q: &'a mut RecvQueue) -> &'a mut [MaybeUninit<u8>];
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
    fn window<'a>(&'a mut self, q: &'a mut RecvQueue) -> &'a mut [MaybeUninit<u8>] {
        let tail = q.deframer.payload_tail().filter(|t| t.len() >= CARRY_BYTES);
        self.direct = tail.is_some();
        tail.unwrap_or(&mut self.carry[..])
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
