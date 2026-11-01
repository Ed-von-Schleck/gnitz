//! Client connections: their limits, the inbound budget and deframed frame
//! queue, the armed recv, the send loop and its eviction, and how a connection
//! ends. Nothing here flushes: an SQE ships with the loop's own submit, before the
//! task awaiting it can run.

use std::collections::hash_map::Entry;
use std::mem::MaybeUninit;
use std::os::fd::{FromRawFd, OwnedFd};

use gnitz_wire::{Deframer, FrameLenError, HANDSHAKE_FRAME_MAX, MAX_FRAME_PAYLOAD};
use gnitz_zset::repr::PooledBuf;
use rustc_hash::FxHashMap;

use super::*;
use crate::runtime::sync::{chan, select2, Either, WakeQueue};
use crate::runtime::w2m::W2mSlot;

/// The ceilings and deadlines a reactor's connections are held to, fixed for its
/// life. Startup resolves them from the environment; a test that has to wait one
/// out picks its own instead, which is why they are arguments rather than
/// globals.
#[derive(Clone, Copy)]
pub(crate) struct Limits {
    /// Ceiling on in-flight inbound frame payload bytes, across every
    /// connection.
    pub inbound_cap: usize,
    /// One connection's share of `inbound_cap`: once the frames it has queued
    /// for its task, and the one it is receiving, weigh more than this, its
    /// recv is not re-armed until the task takes them back under it or takes
    /// every queued one.
    pub conn_inbound_share: usize,
    /// One kernel send making no progress for this long evicts the client, whose
    /// stalled sends would otherwise pin W2M ring space and block a worker
    /// (`GNITZ_CLIENT_SEND_TIMEOUT_MS`).
    pub client_send_timeout: Duration,
    /// A failed accept is retried after this long, so closing connections get a
    /// window to free fds.
    pub accept_rearm_backoff: Duration,
    /// Ceiling on open client connections, across every listener and
    /// transport (`GNITZ_MAX_CONNS`). A connection accepted past it is closed
    /// at once.
    pub max_conns: usize,
}

impl Limits {
    /// The server's, resolved from the environment.
    pub(crate) fn from_env() -> Limits {
        let inbound_cap = resolve_inbound_cap();
        let max_conns = gnitz_foundation::env::env_num("GNITZ_MAX_CONNS", 256);
        Limits {
            inbound_cap,
            // Every connection at its share together stays under the cap, so
            // what one has queued never refuses another's frame.
            conn_inbound_share: inbound_cap / max_conns.max(1),
            client_send_timeout: Duration::from_millis(gnitz_foundation::env::env_num(
                "GNITZ_CLIENT_SEND_TIMEOUT_MS",
                30_000,
            )),
            accept_rearm_backoff: Duration::from_millis(50),
            max_conns,
        }
    }
}

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
fn resolve_inbound_cap() -> usize {
    let host = gnitz_foundation::host::available_memory_bytes();
    let default = (host / 4).clamp(INBOUND_CAP_FLOOR, INBOUND_CAP_CEIL);
    gnitz_foundation::env::env_num("GNITZ_INBOUND_MEM_BYTES", default).max(INBOUND_CAP_FLOOR)
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RecvEnd {
    /// EOF or a `close_notify`: the peer is done.
    PeerClosed,
    /// The recv itself failed (`-ECONNRESET`, `-EBADF`, …).
    Socket,
    /// A declared frame payload above the connection's frame ceiling.
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
            RecvEnd::Oversize => f.write_str("a frame over the frame ceiling"),
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

/// The inbound half of one client connection its tasks reach: the frames its
/// recv filter has completed, the single task awaiting them, and what admits the
/// next.
pub(crate) struct FrameSink {
    /// Complete messages awaiting pickup by `recv().await`, and the one task
    /// awaiting them. A queue, not a slot: with one slot a pipelined client
    /// deadlocks once the kernel socket buffer fills.
    frames: WakeQueue<RecvBuf>,
    budget: Rc<Budget>,
    /// What this connection holds of `budget` outside its task: the frames
    /// queued in `frames` and the payload in progress, charged from its header.
    held: usize,
    /// The largest payload admitted: [`HANDSHAKE_FRAME_MAX`] until the
    /// handshake admits the client, then [`MAX_FRAME_PAYLOAD`].
    max_frame: usize,
}

impl FrameSink {
    pub(super) fn new(budget: Rc<Budget>) -> Self {
        FrameSink {
            frames: WakeQueue::default(),
            budget,
            held: 0,
            max_frame: HANDSHAKE_FRAME_MAX,
        }
    }

    /// Deframe `src` through `deframer`, a recv filter's own, queueing every
    /// frame it completes. Each payload in progress is charged to the budget
    /// from its header on.
    pub(crate) fn feed(&mut self, deframer: &mut Deframer<Charge>, mut src: &[u8]) -> Result<(), RecvEnd> {
        let (budget, max_frame, held) = (&self.budget, self.max_frame, &mut self.held);
        while let Some((buf, charge)) = deframer.feed(&mut src, |len| {
            if len > max_frame {
                return Err(RecvEnd::Oversize);
            }
            // A 64-byte floor per frame, so a flood of tiny frames is bounded
            // by frame count as well as bytes.
            let want = len.max(64);
            let charge = budget.charge(want).ok_or(RecvEnd::CapBreach { want })?;
            *held += want;
            Ok(charge)
        })? {
            self.frames.push(RecvBuf {
                buf: buf.into_owned().into_boxed_slice(),
                charge,
            });
        }
        Ok(())
    }

    /// Admit frames up to [`MAX_FRAME_PAYLOAD`]: the handshake admitted the
    /// client, which until then sends no frame past [`HANDSHAKE_FRAME_MAX`].
    pub(super) fn admit_full_frames(&mut self) {
        self.max_frame = MAX_FRAME_PAYLOAD;
    }

    /// Take a completed frame without parking.
    pub(crate) fn try_recv(&mut self) -> Option<RecvBuf> {
        self.frames.pop().inspect(|f| self.held -= f.charge.weight)
    }

    /// [`WakeQueue::poll`], counting the frame taken out of [`Self::held`].
    fn poll(&mut self, cx: &Context<'_>) -> Poll<Option<RecvBuf>> {
        self.frames.poll_ready(cx).map(|()| self.try_recv())
    }

    /// Whether the connection holds more than `share` of the budget outside its
    /// task with a frame queued for it. A payload in progress past the share
    /// with nothing queued is still received: no take of the task's could bring
    /// it under.
    fn over(&self, share: usize) -> bool {
        self.held > share && self.frames.is_ready()
    }
}

/// An accepted socket and its slot under the connection cap, which the
/// connection built over it holds for its life.
pub(crate) struct Accepted {
    fd: OwnedFd,
    slot: Charge,
}

/// One client connection: its socket and the frames deframed off it. The socket
/// closes with the last holder, and every SQE naming it is queued through one.
pub(crate) struct ClientConn {
    fd: OwnedFd,
    /// This connection's place under the connection cap, held exactly as long
    /// as the fd.
    _slot: Charge,
    q: RefCell<FrameSink>,
    gone: Cell<bool>,
    /// Wakes the task that sends on this socket outside requests, which then
    /// owns the shutdown.
    egress_owner: Option<chan::Sender<()>>,
}

impl ClientConn {
    fn fd(&self) -> i32 {
        self.fd.as_raw_fd()
    }

    /// See [`FrameSink::admit_full_frames`].
    pub(crate) fn admit_full_frames(&self) {
        self.q.borrow_mut().admit_full_frames();
    }

    /// The address of the TCP peer; `None` for an AF_UNIX peer.
    pub(crate) fn peer_addr(&self) -> Option<std::net::SocketAddr> {
        // SAFETY: borrowed for one getpeername, never closed: the fd is the connection's.
        let sock = std::mem::ManuallyDrop::new(unsafe { std::net::TcpStream::from_raw_fd(self.fd()) });
        sock.peer_addr().ok()
    }

    /// `shutdown(SHUT_RDWR)`: errors out a pending recv or send on the socket, which
    /// `close` would not. Never closes the fd; a peer already gone is not an error.
    pub(crate) fn shutdown(&self) {
        // SAFETY: the connection's own fd, open while `self` lives; shutdown
        // reads or writes no memory.
        unsafe { libc::shutdown(self.fd(), libc::SHUT_RDWR) };
    }

    pub(crate) fn is_gone(&self) -> bool {
        self.gone.get()
    }

    /// The client closed its sending side. Frames already queued are still
    /// delivered, and replies still go out.
    fn end_recv(&self) {
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
            q.held = 0;
        }
        match &self.egress_owner {
            Some(owner) => owner.send(()),
            None => self.shutdown(),
        }
    }

    /// [`Self::retire`], and shut the socket down now whoever owns egress.
    pub(crate) fn fail(&self) {
        self.retire();
        self.shutdown();
    }

    pub(crate) fn wake_egress(&self) {
        if let Some(owner) = &self.egress_owner {
            owner.send(());
        }
    }

    /// Next frame, or `None` once the recv side has ended and is drained.
    pub(crate) async fn recv(&self) -> Option<RecvBuf> {
        std::future::poll_fn(|cx| self.q.borrow_mut().poll(cx)).await
    }

    /// Resolves once [`Self::recv`] would: a frame is queued, or the recv side
    /// has ended. Takes no frame.
    pub(crate) async fn readable(&self) {
        std::future::poll_fn(|cx| self.q.borrow_mut().frames.poll_ready(cx)).await
    }

    /// Whether [`Self::readable`] would resolve at once.
    pub(crate) fn is_readable(&self) -> bool {
        self.q.borrow().frames.is_ready()
    }

    /// The next already-deframed frame without parking. `None` means nothing is
    /// queued right now, not that the peer is gone.
    pub(crate) fn try_recv(&self) -> Option<RecvBuf> {
        self.q.borrow_mut().try_recv()
    }
}

/// What stands between a socket and its [`FrameSink`]. A recv writes only into
/// memory its filter owns.
pub(crate) trait RecvFilter {
    /// Where the next socket bytes land. The reactor keeps the address until the
    /// recv completes, and calls nothing on the filter in between.
    fn window(&mut self) -> &mut [MaybeUninit<u8>];
    /// `n` bytes landed in the window: queue the frames they complete.
    fn ingest(&mut self, n: usize, sink: &mut FrameSink) -> Result<(), RecvEnd>;
}

/// Size of [`Plain`]'s carry.
const CARRY_BYTES: usize = 32 * 1024;

/// A socket whose bytes are the frames themselves, read through the deframer's
/// window over the carry.
pub(crate) struct Plain {
    carry: Box<[MaybeUninit<u8>]>,
    deframer: Deframer<Charge>,
}

impl Plain {
    pub(crate) fn new() -> Plain {
        Plain {
            carry: Box::new_uninit_slice(CARRY_BYTES),
            deframer: Deframer::default(),
        }
    }
}

impl RecvFilter for Plain {
    fn window(&mut self) -> &mut [MaybeUninit<u8>] {
        self.deframer.window(&mut self.carry)
    }

    fn ingest(&mut self, n: usize, sink: &mut FrameSink) -> Result<(), RecvEnd> {
        let Plain { carry, deframer } = self;
        // SAFETY: the completed recv wrote `n` bytes at the head of the window.
        let src = unsafe { deframer.landed(carry, n) };
        sink.feed(deframer, src)
    }
}

/// The reactor's connection state: every armed recv, the budgets connections
/// are charged to, and their limits.
pub(super) struct Conns {
    /// Every connection the reactor receives on, by fd: those with a recv armed,
    /// and those in `paused`.
    armed: RefCell<FxHashMap<i32, Armed>>,
    /// The connections over their `Limits::conn_inbound_share`, whose recv is
    /// re-armed once their task takes them back under it.
    paused: RefCell<Vec<i32>>,
    /// The OOM guard shared by every connection, charged per inbound frame.
    inbound: Rc<Budget>,
    /// Open client connections under `Limits::max_conns`; each [`ClientConn`]
    /// holds one charge of it.
    slots: Rc<Budget>,
    limits: Limits,
}

impl Conns {
    pub(super) fn new(limits: Limits) -> Self {
        Conns {
            armed: RefCell::new(FxHashMap::default()),
            paused: RefCell::new(Vec::new()),
            inbound: Budget::new(limits.inbound_cap),
            slots: Budget::new(limits.max_conns),
            limits,
        }
    }
}

/// A connection the reactor receives on — its whole per-connection state. The
/// entry exists while the recv SQE is outstanding, or while the connection is
/// paused with none.
struct Armed {
    conn: Rc<ClientConn>,
    filter: Box<dyn RecvFilter>,
}

/// A send in flight: the connection whose fd its SQE names, and the bytes the
/// kernel reads. Each kernel send's op holds it until that send's CQE.
pub(super) struct Outbound {
    _conn: Rc<ClientConn>,
    body: SendBody,
}

/// An owned client-bound payload whose bytes stay in place when it moves.
pub(crate) enum SendBody {
    Pooled(PooledBuf),
    Slot(W2mSlot),
    /// Bytes several connections are sent.
    Shared(Rc<Vec<u8>>),
}

impl SendBody {
    pub(crate) fn bytes(&self) -> &[u8] {
        match self {
            SendBody::Pooled(b) => &b.0,
            SendBody::Slot(s) => s.frame_bytes(),
            SendBody::Shared(b) => b,
        }
    }

    /// Ready once the W2M ring of a slot body was given back
    /// ([`W2mSlot::poll_give_back`]); never for any other body.
    fn poll_give_back(&self, cx: &mut Context<'_>) -> Poll<()> {
        match self {
            SendBody::Slot(s) => s.poll_give_back(cx),
            _ => Poll::Pending,
        }
    }

    /// Copy a slot body out of its ring; a no-op on any other body.
    fn detach(&mut self) {
        if let SendBody::Slot(s) = self {
            s.detach();
        }
    }

    /// `fut`'s output, awaited while holding this body: should the body's W2M
    /// ring be given back meanwhile, its bytes are copied out of the ring, so
    /// no slot waits on what `fut` waits on. `fut` must not read the body.
    pub(crate) async fn holding<F: Future>(&mut self, fut: F) -> F::Output {
        let mut fut = std::pin::pin!(fut);
        std::future::poll_fn(|cx| {
            if let Poll::Ready(out) = fut.as_mut().poll(cx) {
                return Poll::Ready(out);
            }
            if self.poll_give_back(cx).is_ready() {
                self.detach();
            }
            Poll::Pending
        })
        .await
    }
}

/// This client's connection is finished.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct PeerGone;

impl Armed {
    /// Arm (or re-arm) the recv into the filter's window. The fd rides the udata
    /// id, so the completion routes back to this entry. Not flushed (see the
    /// module header).
    fn arm(&mut self, ring: &mut IoUringRing) {
        let fd = self.conn.fd();
        let w = self.filter.window();
        let sqe = opcode::Recv::new(types::Fd(fd), w.as_mut_ptr().cast(), w.len() as u32).build();
        // SAFETY: the window is memory the filter owns. Only this recv's CQE
        // removes or refills the entry, which holds the filter and the connection
        // (so `fd`); `ring` drops first.
        unsafe { ring.push(sqe, udata(KIND_RECV, fd as u32 as u64)) };
    }
}

impl Reactor {
    /// The next connection accepted on `listener`. A failed accept is retried after
    /// `Limits::accept_rearm_backoff`, so an error that persists — fd exhaustion
    /// above all, which closing connections relieve — is not retried in a hot loop.
    ///
    /// Not cancel-safe: a future dropped mid-accept leaves an op whose later
    /// result is a raw fd nothing owns.
    pub(crate) async fn accept(&self, listener: BorrowedFd<'static>) -> Accepted {
        loop {
            let sqe = opcode::Accept::new(
                types::Fd(listener.as_raw_fd()),
                std::ptr::null_mut(),
                std::ptr::null_mut(),
            )
            .build();
            // SAFETY: the SQE names no memory, and `listener` is open for the
            // process's life.
            let res = unsafe { self.push_op(sqe) }.await;
            if res < 0 {
                self.sleep(self.conns.limits.accept_rearm_backoff).await;
                continue;
            }
            // SAFETY: a fresh fd from the kernel, owned by nothing else.
            if let Some(accepted) = self.admit(unsafe { OwnedFd::from_raw_fd(res) }) {
                return accepted;
            }
        }
    }

    /// `fd`, holding one slot under the connection cap; `None`, closing `fd`, at
    /// the cap.
    pub(super) fn admit(&self, fd: OwnedFd) -> Option<Accepted> {
        let Some(slot) = self.conns.slots.charge(1) else {
            gnitz_warn!(
                "connection cap {} reached; closing fd={}",
                self.conns.slots.cap(),
                fd.as_raw_fd()
            );
            return None;
        };
        Some(Accepted { fd, slot })
    }

    /// A connection over `accepted`, charging frames to the inbound budget.
    /// `egress_owner` wakes the task that sends on the socket outside requests,
    /// if one does. Nothing is armed on it yet.
    pub(crate) fn client_conn(&self, accepted: Accepted, egress_owner: Option<chan::Sender<()>>) -> Rc<ClientConn> {
        Rc::new(ClientConn {
            fd: accepted.fd,
            _slot: accepted.slot,
            q: RefCell::new(FrameSink::new(Rc::clone(&self.conns.inbound))),
            gone: Cell::new(false),
            egress_owner,
        })
    }

    /// Arm `conn`'s first recv into `filter`'s window, and hold the connection
    /// until the recv side ends.
    pub(crate) fn register_conn(&self, conn: &Rc<ClientConn>, filter: Box<dyn RecvFilter>) {
        let fd = conn.fd();
        let mut conns = self.conns.armed.borrow_mut();
        let Entry::Vacant(slot) = conns.entry(fd) else {
            // An entry holds its connection, so its fd is open and cannot be
            // reissued.
            unreachable!("fd={fd} registered while an entry still holds it")
        };
        slot.insert(Armed { conn: Rc::clone(conn), filter })
            .arm(&mut self.ring.borrow_mut());
    }

    pub(super) fn handle_recv_cqe(&self, fd: i32, res: i32) {
        let mut conns = self.conns.armed.borrow_mut();
        let Entry::Occupied(mut entry) = conns.entry(fd) else {
            unreachable!("recv CQE for fd={fd} with no armed entry")
        };
        let armed = entry.get_mut();
        if armed.conn.is_gone() {
            // Retired while this recv was in flight: its bytes are discarded.
            entry.remove();
            return;
        }
        let end = match res {
            ..0 => RecvEnd::Socket,
            0 => RecvEnd::PeerClosed,
            n => {
                let mut q = armed.conn.q.borrow_mut();
                let ingested = armed.filter.ingest(n as usize, &mut q);
                let over = q.over(self.conns.limits.conn_inbound_share);
                drop(q);
                match ingested {
                    // Not re-armed: the socket's bytes stay in the kernel, whose
                    // flow control holds the client off.
                    Ok(()) if over => return self.conns.paused.borrow_mut().push(fd),
                    Ok(()) => return armed.arm(&mut self.ring.borrow_mut()),
                    Err(end) => end,
                }
            }
        };
        let armed = entry.remove();
        drop(conns);
        if let RecvEnd::PeerClosed = end {
            gnitz_debug!("reactor: fd={fd} recv side ended: {end}");
            return armed.conn.end_recv();
        }
        gnitz_warn!(
            "reactor: fd={fd} recv side ended: {end} (res={res}, inbound held={} of {} B)",
            self.conns.inbound.held(),
            self.conns.inbound.cap(),
        );
        match end {
            // The transport may owe the client a reply to the violation.
            RecvEnd::Protocol => armed.conn.retire(),
            // Owed nothing, and possibly not reading.
            _ => armed.conn.fail(),
        }
    } // `armed` drops: the reactor's hold on the connection ends

    /// Re-arm the recv of every paused connection its task has taken back under
    /// its share, and let go of every paused one that is gone, whose bytes no
    /// recv will read. Run ahead of every submit, so a pass that took frames
    /// re-arms before the loop sleeps.
    pub(super) fn resume_recvs(&self) {
        if self.conns.paused.borrow().is_empty() {
            return;
        }
        let mut paused = self.conns.paused.borrow_mut();
        let share = self.conns.limits.conn_inbound_share;
        let mut armed = self.conns.armed.borrow_mut();
        let mut gone = Vec::new();
        paused.retain(|&fd| {
            let Entry::Occupied(mut entry) = armed.entry(fd) else {
                unreachable!("paused fd={fd} with no entry")
            };
            if entry.get().conn.is_gone() {
                gone.push(entry.remove());
                return false;
            }
            if entry.get().conn.q.borrow().over(share) {
                return true;
            }
            entry.get_mut().arm(&mut self.ring.borrow_mut());
            false
        });
        drop((paused, armed));
        drop(gone); // the reactor's hold on those connections ends
    }

    /// Send `body`'s whole byte range on `conn`. Loops on short sends; each kernel
    /// send has its own `Limits::client_send_timeout` deadline, past which the
    /// client is evicted. A failed or evicted send finishes the connection
    /// ([`ClientConn::fail`]). An empty body sends nothing.
    ///
    /// A W2M slot is sent from its ring until the ring is given back while a
    /// send waits on the client: that send is cancelled, the slot copied out,
    /// and the rest sent from the copy.
    pub(crate) async fn send_owned(&self, conn: &Rc<ClientConn>, body: SendBody) -> Result<(), PeerGone> {
        if body.bytes().is_empty() {
            return Ok(());
        }
        let mut out = Outbound { _conn: Rc::clone(conn), body };
        let mut sent = 0usize;
        while sent < out.body.bytes().len() {
            let rest = &out.body.bytes()[sent..];
            // A send longer than an SQE carries goes out over several.
            let len = u32::try_from(rest.len()).unwrap_or(u32::MAX);
            let sqe = opcode::Send::new(types::Fd(conn.fd()), rest.as_ptr(), len).build();
            // SAFETY: the op holds `out` until the CQE, and its bytes stay in
            // place as it moves.
            let op = unsafe { self.push_named_op(sqe, Some(out)) };
            let user_data = op.user_data();
            let mut op = std::pin::pin!(op);
            let mut given_back = false;
            let waited = select2(
                std::future::poll_fn(|cx| {
                    let polled = op.as_mut().poll(cx);
                    if polled.is_pending() && !given_back && op.with_carry(|o| o.body.poll_give_back(cx)).is_ready() {
                        self.cancel_op(user_data);
                        given_back = true;
                    }
                    polled
                }),
                self.sleep(self.conns.limits.client_send_timeout),
            )
            .await;
            let rc = match waited {
                Either::A((rc, carry)) => {
                    out = carry.expect("a send carries its body");
                    if given_back {
                        // The CQE has come, so no SQE reads the body.
                        out.body.detach();
                        // Cut short for the copy: no progress, and no failure.
                        if rc == -libc::ECANCELED || rc == -libc::EINTR {
                            continue;
                        }
                    }
                    rc
                }
                // Evicted; a completion that raced the timer still counts as failed.
                // The abandoned send completes only once the socket errors; its
                // `ops` entry holds the connection and body until then.
                Either::B(()) => {
                    gnitz_warn!(
                        "client fd={} made no send progress for {:?}; evicting",
                        conn.fd(),
                        self.conns.limits.client_send_timeout
                    );
                    conn.fail();
                    return Err(PeerGone);
                }
            };
            // A zero is the kernel accepting nothing, which is not progress either.
            if rc <= 0 {
                conn.fail();
                return Err(PeerGone);
            }
            sent += rc as usize;
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "tests/conn.rs"]
mod tests;

#[cfg(test)]
#[path = "fuzz/conn.rs"]
mod fuzz;
