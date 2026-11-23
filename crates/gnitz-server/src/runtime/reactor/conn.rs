//! Client connections: their limits, the inbound budget and deframed frame
//! queue, the armed recv, the send loop and its eviction, the cork a
//! connection's task coalesces its replies in, and how a connection ends.
//! Nothing here submits: every SQE ships with the loop's own submit, before the
//! task awaiting it can run.

use gnitz_foundation::{gnitz_debug, gnitz_warn};
use std::mem::MaybeUninit;
use std::os::fd::{FromRawFd, OwnedFd};

use gnitz_wire::{Deframer, FrameBuf, FrameLenError, HANDSHAKE_FRAME_MAX, MAX_FRAME_PAYLOAD};
use gnitz_zset::repr::PooledBuf;

use super::*;
use crate::runtime::sync::{Either, Level, WakeQueue, select2};
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
    /// One kernel send making no progress for this long evicts the client
    /// (`GNITZ_CLIENT_SEND_TIMEOUT_MS`). A peer that stops reading holds its
    /// connection, the copies of every frame given back while its replies
    /// waited, and each worker's unsent train for those replies until then; it
    /// is the only reaper for a TCP peer that keeps acknowledging at a zero
    /// window and for an AF_UNIX peer, which no keepalive reaches.
    pub client_send_timeout: Duration,
    /// Ceiling on open client connections, across every listener and
    /// transport (`GNITZ_MAX_CONNS`). A connection accepted past it is closed
    /// at once.
    pub max_conns: usize,
    /// Ceiling on the frame bytes the writes spanning several frames hold
    /// together, across every connection (`GNITZ_WRITE_SPAN_BYTES`): half of
    /// `inbound_cap` unless set lower, so open writes leave the other half to
    /// the frames in progress beside them. A write whose next frame would pass
    /// it is refused, its frames dropped as they arrive.
    pub write_span_cap: usize,
}

impl Limits {
    /// The server's, resolved from the environment.
    pub(crate) fn from_env() -> Limits {
        // A quarter of the process memory budget within the bounds below, or a
        // `GNITZ_INBOUND_MEM_BYTES` no lower than the floor. A fraction of the
        // *actual* budget scales with the deployment where a flat constant would
        // OOM a mid-size box yet never trip in a small container. The floor
        // admits at least one max-size frame on even a tiny budget; the ceiling
        // caps the default on a large box, where a quarter of RAM would be an
        // excessive inbound reserve.
        const INBOUND_CAP_FLOOR: usize = MAX_FRAME_PAYLOAD;
        const INBOUND_CAP_CEIL: usize = 4 << 30;
        let host = gnitz_foundation::host::available_memory_bytes();
        let default = (host / 4).clamp(INBOUND_CAP_FLOOR, INBOUND_CAP_CEIL);
        let inbound_cap = gnitz_foundation::env::env_num_in("GNITZ_INBOUND_MEM_BYTES", default, INBOUND_CAP_FLOOR..);
        let max_conns = gnitz_foundation::env::env_num("GNITZ_MAX_CONNS", 256);
        Limits {
            inbound_cap,
            // Every connection at its share together stays under the cap, so
            // what one has queued never refuses another's frame.
            conn_inbound_share: inbound_cap / max_conns,
            client_send_timeout: Duration::from_millis(gnitz_foundation::env::env_num(
                "GNITZ_CLIENT_SEND_TIMEOUT_MS",
                30_000,
            )),
            max_conns,
            write_span_cap: gnitz_foundation::env::env_num_in(
                "GNITZ_WRITE_SPAN_BYTES",
                inbound_cap / 2,
                1..=inbound_cap / 2,
            ),
        }
    }
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
        self.budget.held.update(|v| v - self.weight);
    }
}

/// One complete inbound frame payload, charged to the inbound [`Budget`] for as
/// long as it lives. The charge is the payload's length; its buffer, from
/// [`gnitz_wire::frame_buf`]'s pool, may be up to twice that, and the pool's
/// own bound caps the difference.
pub(crate) struct RecvBuf {
    buf: FrameBuf,
    charge: Charge,
}

impl RecvBuf {
    pub(crate) fn as_slice(&self) -> &[u8] {
        &self.buf
    }

    /// `bytes` as a frame charged to a budget of its own, which nothing else
    /// draws on.
    #[cfg(test)]
    pub(crate) fn detached(bytes: &[u8]) -> RecvBuf {
        let charge = Budget::new(bytes.len())
            .charge(bytes.len())
            .expect("the budget is the frame's size");
        RecvBuf { buf: bytes.to_vec().into(), charge }
    }
}

/// Why a connection's recv side ended. Logged once, where the reactor retires
/// the connection.
#[derive(Debug, Clone, PartialEq, Eq)]
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
    /// The connection's [`Session`] refused the bytes, for this reason.
    Session(String),
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
            RecvEnd::Session(why) => write!(f, "the session refused the bytes: {why}"),
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
/// recvs have completed, the single task awaiting them, and what admits the
/// next.
struct FrameSink {
    /// Complete messages awaiting pickup by `recv().await`, and the one task
    /// awaiting them. A queue, not a slot: with one slot a pipelined client
    /// deadlocks once the kernel socket buffer fills.
    frames: WakeQueue<RecvBuf>,
    budget: Rc<Budget>,
    /// What this connection holds of `budget` outside its task: the frames
    /// queued in `frames` and the payload in progress, charged from its header.
    /// Exact only while the recv side runs: a refund once it has ended — the
    /// queue [`ClientConn::retire`] clears, or a payload in progress dropped
    /// with its recv entry — leaves `held` as it was, and nothing reads
    /// [`Self::over`] after that.
    held: usize,
    /// The largest payload admitted: [`HANDSHAKE_FRAME_MAX`] until the
    /// handshake admits the client, then [`MAX_FRAME_PAYLOAD`].
    max_frame: usize,
}

impl FrameSink {
    fn new(budget: Rc<Budget>) -> Self {
        FrameSink {
            frames: WakeQueue::default(),
            budget,
            held: 0,
            max_frame: HANDSHAKE_FRAME_MAX,
        }
    }

    /// Deframe `src` through `deframer`, the connection's own, queueing every
    /// frame it completes. Each payload in progress is charged to the budget
    /// from its header on.
    fn feed(&mut self, deframer: &mut Deframer<Charge>, mut src: &[u8]) -> Result<(), RecvEnd> {
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
            self.frames.push(RecvBuf { buf: buf.into_owned(), charge });
        }
        Ok(())
    }

    /// Admit frames up to [`MAX_FRAME_PAYLOAD`]: the handshake admitted the
    /// client, which until then sends no frame past [`HANDSHAKE_FRAME_MAX`].
    fn admit_full_frames(&mut self) {
        self.max_frame = MAX_FRAME_PAYLOAD;
    }

    /// Take a completed frame without parking.
    fn try_recv(&mut self) -> Option<RecvBuf> {
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
    /// Held, never read: dropped after `fd`, it frees the place under the cap.
    _slot: Charge,
}

/// How far a [`ClientConn`] has ended.
#[derive(Clone, Copy, PartialEq, Eq)]
enum End {
    Open,
    /// No frame is delivered any more; the socket stays open for what
    /// [`Peer::close`] sends.
    Retired,
    /// The socket is shut down: nothing is sent on it any more.
    Shut,
}

/// One client connection: its socket and the frames deframed off it. The socket
/// closes with the last holder, and every SQE naming it is queued through one.
struct ClientConn {
    /// The socket and its place under the connection cap.
    sock: Accepted,
    q: RefCell<FrameSink>,
    end: Cell<End>,
    /// `None` on a plain socket.
    session: Option<Rc<dyn Session>>,
}

impl ClientConn {
    fn fd(&self) -> i32 {
        self.sock.fd.as_raw_fd()
    }

    fn is_gone(&self) -> bool {
        self.end.get() != End::Open
    }

    /// Finish the connection and discard the frames it queued. The socket stays
    /// open for what [`Peer::close`] sends before shutting it down.
    fn retire(&self) {
        if self.is_gone() {
            return;
        }
        self.end.set(End::Retired);
        let mut q = self.q.borrow_mut();
        q.frames.clear();
        q.frames.close();
    }

    /// [`Self::retire`], and shut the socket down now: the client is owed
    /// nothing more. `shutdown(SHUT_RDWR)` errors out a pending recv or send on
    /// the socket, which `close` would not; a peer already gone is not an error.
    fn fail(&self) {
        self.retire();
        self.end.set(End::Shut);
        // SAFETY: the connection's own fd, open while `self` lives; shutdown
        // reads or writes no memory.
        unsafe { libc::shutdown(self.fd(), libc::SHUT_RDWR) };
    }

    /// Next frame, or `None` once the recv side has ended and is drained. What
    /// a session owes the socket meanwhile is [`Peer::recv`]'s to send.
    async fn recv(&self) -> Option<RecvBuf> {
        std::future::poll_fn(|cx| self.q.borrow_mut().poll(cx)).await
    }

    /// Resolves once [`Self::recv`] would: a frame is queued, or the recv side
    /// has ended. Takes no frame.
    async fn readable(&self) {
        std::future::poll_fn(|cx| self.q.borrow_mut().frames.poll_ready(cx)).await
    }

    /// Whether [`Self::readable`] would resolve at once.
    fn is_readable(&self) -> bool {
        self.q.borrow().frames.is_ready()
    }

    /// The next already-deframed frame without parking. `None` means nothing is
    /// queued right now, not that the peer is gone.
    fn try_recv(&self) -> Option<RecvBuf> {
        self.q.borrow_mut().try_recv()
    }
}

/// The codec between a connection's socket and its frames: it turns the bytes
/// a recv lands into the plaintext the frames are deframed from, and the
/// replies into the records the socket is sent. It does no I/O; the
/// connection's one task sends every record, so they leave in the order they
/// were sealed. A plain socket has none: its bytes are the frames.
pub(crate) trait Session {
    /// `cipher`, the bytes one recv landed: hand their plaintext to `plain`, in
    /// order, ending at its first refusal.
    fn ingest(&self, cipher: &[u8], plain: &mut dyn FnMut(&[u8]) -> Result<(), RecvEnd>) -> Result<(), RecvEnd>;
    /// Whether records no reply asked for await a [`Self::seal`]: a handshake
    /// flight, or the alert that refuses what was ingested.
    fn owes(&self) -> bool;
    /// The records of all of `plain`, behind every record owed.
    fn seal(&self, plain: &[u8]) -> PooledBuf;
    /// The goodbye's records, behind every record owed.
    fn close(&self) -> PooledBuf;
}

/// The most plaintext one [`Session::seal`] is handed, and so about how much
/// one send under a session carries.
const SEAL_TURN_BYTES: usize = 256 * 1024;

/// Size of a plain connection's carry, which a recv lands in unless the
/// deframer's payload in progress takes it.
const CARRY_BYTES: usize = 32 * 1024;

/// Size of the carry of a connection under a session, which every recv lands in.
const CIPHER_WINDOW_BYTES: usize = 64 * 1024;

/// The reactor's connection state: every armed recv, the budgets connections
/// are charged to, and their limits.
pub(super) struct Conns {
    /// Every connection the reactor receives on, by the key its recv's
    /// user_data carries: those with a recv armed, and those in `paused`.
    recvs: RefCell<Slab<Armed>>,
    /// The keys of the connections over their `Limits::conn_inbound_share`,
    /// whose recv is re-armed once their task takes them back under it.
    paused: RefCell<Vec<usize>>,
    /// The OOM guard shared by every connection, charged per inbound frame.
    inbound: Rc<Budget>,
    /// Open client connections under `Limits::max_conns`; each [`ClientConn`]
    /// holds one charge of it.
    slots: Rc<Budget>,
    /// The frames every connection's open write holds, under
    /// `Limits::write_span_cap`.
    spans: Rc<Budget>,
    limits: Limits,
}

impl Conns {
    pub(super) fn new(limits: Limits) -> Self {
        Conns {
            recvs: RefCell::new(Slab::new()),
            paused: RefCell::new(Vec::new()),
            inbound: Budget::new(limits.inbound_cap),
            slots: Budget::new(limits.max_conns),
            spans: Budget::new(limits.write_span_cap),
            limits,
        }
    }
}

/// A connection the reactor receives on — its whole per-connection state. The
/// entry exists while the recv SQE is outstanding, or while the connection is
/// paused with none.
struct Armed {
    conn: Rc<ClientConn>,
    /// Memory only this entry names, which the recv in flight writes into.
    carry: Box<[MaybeUninit<u8>]>,
    deframer: Deframer<Charge>,
}

/// A send in flight: the connection whose fd its SQE names, and the bytes the
/// kernel reads. Each kernel send's op holds it until that send's CQE.
struct Outbound {
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
    /// Arm (or re-arm) the recv: into the deframer's window over the carry on
    /// a plain socket, so a large payload lands in place, else into the carry.
    /// The entry's `key` rides the udata id, so the completion routes back to
    /// it. Not flushed (see the module header).
    fn arm(&mut self, key: usize, reactor: &Reactor) {
        let fd = self.conn.fd();
        let w = match self.conn.session {
            None => self.deframer.window(&mut self.carry),
            Some(_) => &mut self.carry[..],
        };
        let sqe = opcode::Recv::new(types::Fd(fd), w.as_mut_ptr().cast(), w.len() as u32).build();
        // SAFETY: the window is the entry's carry or its deframer's payload in
        // progress, which nothing touches until [`Self::ingest`]. Only this
        // recv's CQE removes or refills entry `key`, which holds them and the
        // connection (so `fd`); nothing cancels a recv by its user_data, so the
        // key needs no generation. The reactor's drop cancels the recv ahead of
        // dropping the entry.
        unsafe { reactor.push(sqe, udata(KIND_RECV, key as u64)) };
    }

    /// The recv [`Self::arm`] queued landed `n` bytes: queue the frames they
    /// complete on the connection.
    ///
    /// # Safety
    /// `n` counts the bytes that recv wrote, at the head of its window.
    unsafe fn ingest(&mut self, n: usize) -> Result<(), RecvEnd> {
        let Armed { conn, carry, deframer } = self;
        let sink = &mut *conn.q.borrow_mut();
        match &conn.session {
            None => {
                // SAFETY: the caller's contract.
                let src = unsafe { deframer.landed(carry, n) };
                sink.feed(deframer, src)
            }
            Some(session) => {
                // SAFETY: the caller's contract; the window was the carry.
                let cipher = unsafe { carry[..n].assume_init_ref() };
                let ingested = session.ingest(cipher, &mut |plain| sink.feed(deframer, plain));
                if session.owes() {
                    // For [`Peer::recv`], parked on the frames.
                    sink.frames.wake();
                }
                ingested
            }
        }
    }
}

/// A failed accept is retried after this long, so closing connections get a
/// window to free fds.
const ACCEPT_REARM_BACKOFF: Duration = Duration::from_millis(50);

impl Reactor {
    /// The next connection accepted on `listener`. A failed accept is retried after
    /// [`ACCEPT_REARM_BACKOFF`], so an error that persists — fd exhaustion
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
            let (res, ()) = unsafe { self.push_op(sqe, ()) }.await;
            if res < 0 {
                self.sleep(ACCEPT_REARM_BACKOFF).await;
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
        Some(Accepted { fd, _slot: slot })
    }

    /// A connection over `accepted`, charging frames to the inbound budget, its
    /// first recv armed, under `session` if it has one; the reactor holds it
    /// until the recv side ends.
    pub(crate) fn connect(&self, accepted: Accepted, session: Option<Rc<dyn Session>>) -> Peer {
        let carry = Box::new_uninit_slice(match session {
            None => CARRY_BYTES,
            Some(_) => CIPHER_WINDOW_BYTES,
        });
        let conn = Rc::new(ClientConn {
            sock: accepted,
            q: RefCell::new(FrameSink::new(Rc::clone(&self.conns.inbound))),
            end: Cell::new(End::Open),
            session,
        });
        let mut recvs = self.conns.recvs.borrow_mut();
        let entry = recvs.vacant_entry();
        let key = entry.key();
        entry
            .insert(Armed {
                conn: Rc::clone(&conn),
                carry,
                deframer: Deframer::default(),
            })
            .arm(key, self);
        Peer {
            reactor: self.clone(),
            conn,
            egress: None,
            fence: None,
        }
    }

    pub(super) fn handle_recv_cqe(&self, key: usize, res: i32) {
        let mut recvs = self.conns.recvs.borrow_mut();
        let armed = recvs.get_mut(key).expect("a recv's entry lives until its CQE");
        let end = match res {
            // Retired while this recv was in flight: its bytes are discarded.
            _ if armed.conn.is_gone() => None,
            ..0 => Some(RecvEnd::Socket),
            0 => Some(RecvEnd::PeerClosed),
            // SAFETY: this CQE completes the recv `arm` queued, and `n` counts
            // the bytes it wrote.
            n => match unsafe { armed.ingest(n as usize) } {
                Err(end) => Some(end),
                // Not re-armed: the socket's bytes stay in the kernel, whose
                // flow control holds the client off.
                Ok(()) if armed.conn.q.borrow().over(self.conns.limits.conn_inbound_share) => {
                    return self.conns.paused.borrow_mut().push(key);
                }
                Ok(()) => return armed.arm(key, self),
            },
        };
        let armed = recvs.remove(key);
        let Some(end) = end else { return };
        let fd = armed.conn.fd();
        if let RecvEnd::PeerClosed = end {
            gnitz_debug!("reactor: fd={fd} recv side ended: {end}");
            // The client closed its sending side. Frames already queued are
            // still delivered, and replies still go out.
            return armed.conn.q.borrow_mut().frames.close();
        }
        gnitz_warn!(
            "reactor: fd={fd} recv side ended: {end} (res={res}, inbound held={} of {} B)",
            self.conns.inbound.held(),
            self.conns.inbound.cap(),
        );
        // A session owing the client its alert leaves the socket to the
        // connection's task, which sends it ahead of the shutdown. Otherwise the
        // client is owed nothing, and possibly not reading.
        if armed.conn.session.as_ref().is_some_and(|s| s.owes()) {
            armed.conn.retire();
        } else {
            armed.conn.fail();
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
        let mut recvs = self.conns.recvs.borrow_mut();
        paused.retain(|&key| {
            let armed = &mut recvs[key];
            if armed.conn.is_gone() {
                recvs.remove(key); // the reactor's hold on the connection ends
                return false;
            }
            if armed.conn.q.borrow().over(share) {
                return true;
            }
            armed.arm(key, self);
            false
        });
    }

    /// Send `body`'s whole byte range on `conn`. Loops on short sends; each kernel
    /// send has its own `Limits::client_send_timeout` deadline, past which the
    /// client is evicted. A failed or evicted send finishes the connection
    /// ([`ClientConn::fail`]). An empty body sends nothing, and nothing is sent
    /// on a socket already shut down.
    ///
    /// A W2M slot is sent from its ring until the ring is given back while a
    /// send waits on the client: that send is cancelled, the slot copied out,
    /// and the rest sent from the copy.
    async fn send_owned(&self, conn: &Rc<ClientConn>, body: SendBody) -> Result<(), PeerGone> {
        if conn.end.get() == End::Shut {
            return Err(PeerGone);
        }
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
            let mut op = unsafe { self.push_op(sqe, out) };
            let mut given_back = false;
            let waited = select2(
                std::future::poll_fn(|cx| {
                    let polled = Pin::new(&mut op).poll(cx);
                    if polled.is_pending() && !given_back && op.carry().body.poll_give_back(cx).is_ready() {
                        op.cancel();
                        given_back = true;
                    }
                    polled
                }),
                self.sleep(self.conns.limits.client_send_timeout),
            )
            .await;
            let rc = match waited {
                Either::A((rc, carry)) => {
                    out = carry;
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
                // op's slot holds the connection and body until then.
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

/// Ceiling on a concatenation of client-bound frames (coalesced scan heads, corked
/// replies): the copy paid to save per-frame sends. `fanout_coalesced_egress_bench`
/// measures the trade.
const COALESCE_MAX_BYTES: usize = 32 * 1024;

/// The handle to one client connection, under either transport. Owned by the
/// connection's task; handlers borrow it to send replies. A `PeerGone` from any
/// send or flush means the connection is finished, and every later send or
/// flush refuses too.
pub(crate) struct Peer {
    reactor: Reactor,
    conn: Rc<ClientConn>,
    /// Replies written but not yet sent, concatenated so a run of pipelined
    /// requests leaves as one send. `None` when nothing is pending.
    egress: Option<PooledBuf>,
    /// A level, and the value it must reach before this peer sends a byte;
    /// see [`Self::fence`]. Left up once reached, which holds nothing back.
    fence: Option<(Rc<Level>, u64)>,
}

impl Peer {
    /// The budget the frames of every connection's open write are charged
    /// to, capped at [`Limits::write_span_cap`].
    pub(crate) fn write_spans(&self) -> &Rc<Budget> {
        &self.reactor.conns.spans
    }

    /// See [`FrameSink::admit_full_frames`].
    pub(crate) fn admit_full_frames(&self) {
        self.conn.q.borrow_mut().admit_full_frames();
    }

    /// The client's TCP address; `None` on the AF_UNIX socket.
    pub(crate) fn peer_addr(&self) -> Option<std::net::SocketAddr> {
        // SAFETY: borrowed for one getpeername, never closed: the fd is the connection's.
        let sock = std::mem::ManuallyDrop::new(unsafe { std::net::TcpStream::from_raw_fd(self.conn.fd()) });
        sock.peer_addr().ok()
    }

    /// The next request, shipping what is corked before parking on the client.
    /// `None` once the client is gone or has sent its last request. Not
    /// cancel-safe under a session: see [`Self::recv`].
    pub(crate) async fn next_request(&mut self) -> Option<RecvBuf> {
        self.flush_if_full().await.ok()?;
        // Not `is_readable`: a client that closed its sending side is readable,
        // and is still owed what is corked.
        if let Some(buf) = self.conn.try_recv() {
            return Some(buf);
        }
        self.flush_egress().await.ok()?;
        self.recv().await
    }

    /// Whether [`Self::next_request`] would answer without parking: a request,
    /// or the end of them, is queued.
    pub(crate) fn request_queued(&self) -> bool {
        self.conn.is_readable()
    }

    /// What `waited` resolves to, or `None` once [`Self::next_request`] would
    /// not park. What is corked ships first, so `waited` is raced with no send.
    pub(crate) async fn until_request<T>(&mut self, waited: impl Future<Output = T>) -> Result<Option<T>, PeerGone> {
        if !self.conn.is_readable() {
            self.flush_egress().await?;
        }
        Ok(match select2(waited, self.conn.readable()).await {
            Either::A(done) => Some(done),
            Either::B(()) => None,
        })
    }

    /// The cork: replies appended here leave with whatever is corked beside
    /// them.
    pub(crate) fn corked(&mut self) -> &mut Vec<u8> {
        &mut (self.egress)
            .get_or_insert_with(|| PooledBuf::with_capacity(COALESCE_MAX_BYTES))
            .0
    }

    /// Append `payload` framed: its length prefix, then its bytes.
    pub(crate) fn cork_frame(&mut self, payload: &[u8]) {
        let out = self.corked();
        out.extend_from_slice(&gnitz_wire::frame_len_prefix(payload.len()));
        out.extend_from_slice(payload);
    }

    /// Append `frame`'s bytes.
    pub(crate) fn cork(&mut self, frame: &[u8]) {
        self.corked().extend_from_slice(frame);
    }

    /// Bytes currently corked.
    fn corked_len(&self) -> usize {
        self.egress.as_ref().map_or(0, |b| b.0.len())
    }

    /// Ship what is corked once it reaches [`COALESCE_MAX_BYTES`], bounding the
    /// accumulator across a long pipelined run.
    pub(crate) async fn flush_if_full(&mut self) -> Result<(), PeerGone> {
        self.live()?;
        if self.corked_len() < COALESCE_MAX_BYTES {
            return Ok(());
        }
        self.flush_egress().await
    }

    /// Write everything corked, as one send. No-op when nothing is pending.
    pub(crate) async fn flush_egress(&mut self) -> Result<(), PeerGone> {
        self.live()?;
        let Some(buf) = self.egress.take() else {
            return Ok(());
        };
        self.send_raw(SendBody::Pooled(buf)).await
    }

    /// Send `body` behind whatever is corked: copied into the cork, or alone
    /// and uncopied when it is too large to join it. A corked body reports a
    /// gone peer at its flush, not here.
    pub(crate) async fn send(&mut self, mut body: SendBody) -> Result<(), PeerGone> {
        self.live()?;
        let len = body.bytes().len();
        let corked = self.corked_len();
        if corked == 0 || corked + len > COALESCE_MAX_BYTES {
            body.holding(self.flush_egress()).await?;
            if len > COALESCE_MAX_BYTES / 2 {
                return self.send_raw(body).await;
            }
        }
        self.cork(body.bytes());
        Ok(())
    }

    /// Send nothing until `level` reaches `at`: what is corked now and
    /// everything written after it waits, whichever send carries it. A fence
    /// already up is raised to `at` if `at` is higher; it must be on the same
    /// level.
    pub(crate) fn fence(&mut self, level: &Rc<Level>, at: u64) {
        if level.get() >= at {
            return;
        }
        match &mut self.fence {
            Some((up, v)) => {
                debug_assert!(Rc::ptr_eq(up, level), "one level fences a peer");
                *v = (*v).max(at);
            }
            None => self.fence = Some((Rc::clone(level), at)),
        }
    }

    fn live(&self) -> Result<(), PeerGone> {
        if self.conn.is_gone() { Err(PeerGone) } else { Ok(()) }
    }

    /// The next frame, or `None` once the recv side has ended and is drained;
    /// sending meanwhile what the session owes the socket unasked, its
    /// handshake flight or an alert.
    ///
    /// Not cancel-safe under a session: a future dropped mid-send leaves a
    /// flight cut short, after which the connection can only
    /// [`abort`](Self::abort).
    pub(super) async fn recv(&self) -> Option<RecvBuf> {
        let conn = &self.conn;
        let Some(session) = &conn.session else {
            return conn.recv().await;
        };
        loop {
            let frame = std::future::poll_fn(|cx| {
                if session.owes() {
                    return Poll::Ready(None);
                }
                conn.q.borrow_mut().poll(cx).map(Some)
            })
            .await;
            match frame {
                Some(frame) => return frame,
                None => {
                    let flight = SendBody::Pooled(session.seal(&[]));
                    self.reactor.send_owned(conn, flight).await.ok()?
                }
            }
        }
    }

    /// Send `body`: as it is on a plain socket, sealed by the session
    /// [`SEAL_TURN_BYTES`] a turn otherwise. Every turn but the last is sent
    /// under [`SendBody::holding`], so no send waits on the client holding a
    /// W2M slot whose ring was given back; the last is sent with `body`
    /// dropped. Waits out the [`fence`](Self::fence) first.
    async fn send_raw(&self, mut body: SendBody) -> Result<(), PeerGone> {
        #[cfg(test)]
        SENDS.update(|n| n + 1);
        if let Some((level, at)) = &self.fence
            && level.get() < *at
        {
            body.holding(std::future::poll_fn(|cx| level.poll_reached(*at, cx)))
                .await;
        }
        let conn = &self.conn;
        let Some(session) = &conn.session else {
            return self.reactor.send_owned(conn, body).await;
        };
        let len = body.bytes().len();
        let mut off = 0;
        loop {
            if conn.is_gone() {
                return Err(PeerGone);
            }
            let turn = (len - off).min(SEAL_TURN_BYTES);
            let records = session.seal(&body.bytes()[off..off + turn]);
            off += turn;
            let sending = self.reactor.send_owned(conn, SendBody::Pooled(records));
            if off == len {
                drop(body);
                return sending.await;
            }
            body.holding(sending).await?;
        }
    }

    /// Finish the connection, the client told so: its socket is shut down
    /// behind the session's goodbye and whatever else the session owes.
    pub(crate) async fn close(&self) {
        let conn = &self.conn;
        conn.retire();
        if let Some(session) = &conn.session {
            let _ = self.reactor.send_owned(conn, SendBody::Pooled(session.close())).await;
        }
        conn.fail();
    }

    /// Finish the connection now, the client owed nothing more.
    pub(crate) fn abort(&self) {
        self.conn.fail();
    }
}

#[cfg(test)]
thread_local! {
    /// Calls of `send_raw` on this thread.
    static SENDS: Cell<u64> = const { Cell::new(0) };
}

#[cfg(test)]
#[path = "tests/conn.rs"]
mod tests;

#[cfg(test)]
#[path = "benches/conn.rs"]
mod bench;

#[cfg(all(test, fuzzing))]
#[path = "fuzz/conn.rs"]
mod fuzz;
