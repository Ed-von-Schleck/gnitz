# 01 — Transport-Neutral Client Peer + Reactor UDP Datagram Ops

## Problem

The server will gain a network transport whose client connections are not file
descriptors: a userspace QUIC endpoint receives frames from stream buffers and
writes replies into stream buffers, with the kernel seeing only UDP datagrams
on a single socket. Two things block that today:

1. **The client-connection identity is a raw `i32` fd woven through the whole
   dispatch layer.** `connection_loop`, `run_hello_handshake`, `handle_message`,
   every request handler, `send_error`/`send_control_only`/`send_alloc`
   (`crates/gnitz-engine/src/runtime/orchestration/executor.rs`), and the scan
   train forwarders `fan_out_scan_async` / `fan_out_scan_single_worker_async` /
   `drain_scan_train` (`crates/gnitz-engine/src/runtime/orchestration/master/dispatch.rs`)
   all take `fd: i32` and terminate in `reactor.send_buffer*` / `reactor.send_slot*`
   / `reactor.close_fd`.

2. **The reactor has no UDP ops.** The `Ring` trait
   (`crates/gnitz-engine/src/runtime/reactor/ring.rs`) supports
   `Recv`/`Send`/`AcceptMulti`/`Fsync`/`Timeout`/`FutexWaitV`/`AsyncCancel` —
   nothing that carries a datagram with a peer address.

This plan removes both blockers with zero behavior change: a transport-neutral
`Peer` handle replaces the raw fd in the dispatch layer (the Unix
implementation delegates to the existing reactor methods verbatim), and the
reactor gains one-shot `sendmsg`/`recvmsg` datagram ops with their own
futures and tests. No listener, config, or protocol change is included; the
UNIX socket path behaves byte-identically.

## Design

### Part A — `Peer`: the transport-neutral client connection handle

New module `crates/gnitz-engine/src/runtime/orchestration/peer.rs`. The
handle is an enum, not a trait: connection transports are a closed set, enum
dispatch keeps the async methods plain (no boxed futures, no dyn), and the
orchestration layer sits above both the reactor and any future transport
engine, so the layering stays intact (the reactor keeps its fd-based API and
learns nothing about peers).

```rust
use std::rc::Rc;

use crate::runtime::reactor::Reactor;
use crate::runtime::w2m::W2mSlot;
use crate::storage::batch_pool::PooledSendBuf;

/// Transport-neutral handle to one client connection. Owned by the
/// connection task; handlers borrow it to send replies.
pub struct Peer {
    inner: PeerInner,
}

enum PeerInner {
    /// AF_UNIX stream connection serviced by the reactor's fd machinery.
    Unix { fd: i32, reactor: Rc<Reactor> },
}

impl Peer {
    pub fn unix(fd: i32, reactor: Rc<Reactor>) -> Peer {
        Peer {
            inner: PeerInner::Unix { fd, reactor },
        }
    }

    /// Next complete inbound frame payload, or `None` on disconnect.
    pub async fn recv(&self) -> Option<RecvBuf> {
        match &self.inner {
            PeerInner::Unix { fd, reactor } => {
                reactor.recv(*fd).await.map(|(ptr, len)| RecvBuf { ptr, len })
            }
        }
    }

    pub async fn send_buffer(&self, buf: PooledSendBuf) -> i32 {
        match &self.inner {
            PeerInner::Unix { fd, reactor } => reactor.send_buffer(*fd, buf).await,
        }
    }

    pub async fn send_slot(&self, slot: W2mSlot) -> i32 {
        match &self.inner {
            PeerInner::Unix { fd, reactor } => reactor.send_slot(*fd, slot).await,
        }
    }

    pub async fn send_hello_ack(&self) -> i32 {
        match &self.inner {
            PeerInner::Unix { fd, reactor } => reactor.send_hello_ack(*fd).await,
        }
    }

    /// Terminal reply send: close the connection on transport failure.
    pub async fn send_buffer_or_close(&self, buf: PooledSendBuf) {
        if self.send_buffer(buf).await < 0 {
            self.close();
        }
    }

    pub async fn send_slot_or_close(&self, slot: W2mSlot) {
        if self.send_slot(slot).await < 0 {
            self.close();
        }
    }

    /// Elevate the per-connection inbound frame ceiling after HELLO.
    /// Must run synchronously before any `.await` in the handshake task
    /// (see `Reactor::set_max_payload_len`).
    pub fn set_max_payload_len(&self, limit: usize) {
        match &self.inner {
            PeerInner::Unix { fd, reactor } => reactor.set_max_payload_len(*fd, limit),
        }
    }

    pub fn close(&self) {
        match &self.inner {
            PeerInner::Unix { fd, reactor } => reactor.close_fd(*fd),
        }
    }
}
```

`RecvBuf` subsumes the executor's `MallocGuard` (RAII free of the
reactor-malloc'd payload, executor.rs:291-300), which is deleted:

```rust
/// Owned inbound frame payload. Today always a reactor-malloc'd buffer;
/// freed on drop on every exit path, including task cancellation at an
/// `.await` point.
pub struct RecvBuf {
    ptr: *mut u8,
    len: usize,
}

impl RecvBuf {
    pub fn as_slice(&self) -> &[u8] {
        // `from_raw_parts` requires a non-null pointer even at len 0, so a
        // null ptr (a zero-length frame from a mock/alternate transport) maps
        // to the empty slice explicitly rather than constructing UB. Mirrors
        // the null check in `Drop`.
        if self.ptr.is_null() {
            return &[];
        }
        // SAFETY: ptr/len come from a completed reactor recv; the buffer
        // is exclusively owned by this RecvBuf until drop.
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }
}

impl Drop for RecvBuf {
    fn drop(&mut self) {
        if !self.ptr.is_null() {
            unsafe { libc::free(self.ptr as *mut libc::c_void) }
        }
    }
}
```

**Callers change mechanically.** `accept_loop` builds the handle and the
connection task owns it:

```rust
async fn accept_loop(shared: Rc<Shared>) {
    loop {
        let fd = shared.reactor.accept().await;
        if fd < 0 {
            continue;
        }
        shared.reactor.register_conn(fd);
        let peer = Peer::unix(fd, Rc::clone(&shared.reactor));
        let s = Rc::clone(&shared);
        shared.reactor.spawn(connection_loop(peer, s));
    }
}

async fn connection_loop(peer: Peer, shared: Rc<Shared>) {
    let bound_client_id = match peer.recv().await {
        Some(buf) => match run_hello_handshake(&peer, &shared, buf.as_slice()).await {
            HelloOutcome::Pass(b) => b,
            HelloOutcome::Reject => {
                peer.close();
                return;
            }
        },
        None => {
            peer.close();
            return;
        }
    };
    loop {
        let buf = match peer.recv().await {
            Some(v) => v,
            None => break,
        };
        handle_message(&peer, buf.as_slice(), &shared, bound_client_id).await;
    }
    peer.close();
}
```

`run_hello_handshake` takes `&Peer` and `&[u8]` (the raw ptr/len pair and the
inline `MallocGuard` disappear). Every `fd: i32` parameter in the dispatch
layer becomes `peer: &Peer`, and every
`shared.reactor.send_buffer_or_close(fd, buf)` / `send_slot_or_close(fd, slot)`
call becomes `peer.send_buffer_or_close(buf)` / `peer.send_slot_or_close(slot)`.
The `_or_close` composites move off the reactor entirely (`conn.rs:140-152`
deleted); the reactor keeps only the primitives `send_buffer` / `send_slot` /
`send_hello_ack` / `close_fd` / `recv` / `set_max_payload_len`.

In `master/dispatch.rs`, `fan_out_scan_async` and
`fan_out_scan_single_worker_async` replace their `fd: i32` parameter with
`peer: &Peer` (they keep their `reactor` parameter — `await_scan_slot`,
`scan_lease`, and req-id allocation are reactor business), and
`drain_scan_train` forwards frames via `peer.send_slot(slot).await`.

### Part B — Reactor UDP datagram ops

#### posix_io: socket creation

`crates/gnitz-engine/src/foundation/posix_io.rs` gains, next to
`server_create`:

`udp_bind` uses `socket(AF_INET|AF_INET6, SOCK_DGRAM, 0)` per the address
family, sets the buffer sizes with `setsockopt`, binds, and does not set
`O_NONBLOCK` (irrelevant under io_uring). No `connect()` — the server socket
is unconnected and every send carries an explicit destination. Every error
path closes the socket before returning: a raw `i32` fd has no `Drop`, so an
early return without `close` leaks it (same discipline as `server_create`).

```rust
/// Create + bind a UDP socket on `addr`. Requests `rcvbuf_bytes` /
/// `sndbuf_bytes` via SO_RCVBUF/SO_SNDBUF (best-effort; the kernel may
/// clamp). Returns the fd, or a negative value on error (same convention
/// as `server_create`).
pub fn udp_bind(addr: std::net::SocketAddr, rcvbuf_bytes: usize, sndbuf_bytes: usize) -> i32 {
    let family = match addr {
        std::net::SocketAddr::V4(_) => libc::AF_INET,
        std::net::SocketAddr::V6(_) => libc::AF_INET6,
    };
    unsafe {
        let fd = libc::socket(family, libc::SOCK_DGRAM, 0);
        if fd < 0 {
            return -1;
        }
        // SO_RCVBUF / SO_SNDBUF take a *const c_int (4 bytes). Passing a
        // pointer to `usize` would hand the kernel 4 of its 8 bytes — correct
        // only by luck on little-endian. Copy into a c_int and point at that.
        let rcvbuf = rcvbuf_bytes as libc::c_int;
        if libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_RCVBUF,
            &rcvbuf as *const libc::c_int as *const libc::c_void,
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        ) < 0
        {
            libc::close(fd);
            return -2;
        }
        let sndbuf = sndbuf_bytes as libc::c_int;
        if libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_SNDBUF,
            &sndbuf as *const libc::c_int as *const libc::c_void,
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        ) < 0
        {
            libc::close(fd);
            return -3;
        }
        let (ss, len) = sockaddr_from_addr(&addr);
        if libc::bind(fd, &ss as *const libc::sockaddr_storage as *const libc::sockaddr, len) < 0 {
            libc::close(fd);
            return -4;
        }
        fd
    }
}

/// getsockname() as a SocketAddr — resolves the real port after a
/// port-0 bind. Returns None on error or non-INET family.
pub fn udp_local_addr(fd: i32) -> Option<std::net::SocketAddr> {
    let mut ss: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
    let mut len = std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;
    let rc = unsafe { libc::getsockname(fd, &mut ss as *mut _ as *mut libc::sockaddr, &mut len) };
    if rc < 0 {
        return None;
    }
    addr_from_sockaddr(&ss, len)
}

/// SocketAddr → (sockaddr_storage, socklen_t). Zero-pads the storage and
/// preserves the IPv6 flowinfo/scope_id so link-local destinations route
/// (without a scope id the kernel rejects an `fe80::` send with EINVAL).
pub fn sockaddr_from_addr(addr: &std::net::SocketAddr) -> (libc::sockaddr_storage, libc::socklen_t) {
    let mut ss: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
    match addr {
        std::net::SocketAddr::V4(a) => {
            let sin = unsafe { &mut *(&mut ss as *mut _ as *mut libc::sockaddr_in) };
            sin.sin_family = libc::AF_INET as libc::sa_family_t;
            sin.sin_port = a.port().to_be();
            // octets() are already network order; from_ne_bytes keeps the
            // in-memory byte order intact on both endiannesses.
            sin.sin_addr.s_addr = u32::from_ne_bytes(a.ip().octets());
            (ss, std::mem::size_of::<libc::sockaddr_in>() as libc::socklen_t)
        }
        std::net::SocketAddr::V6(a) => {
            let sin6 = unsafe { &mut *(&mut ss as *mut _ as *mut libc::sockaddr_in6) };
            sin6.sin6_family = libc::AF_INET6 as libc::sa_family_t;
            sin6.sin6_port = a.port().to_be();
            sin6.sin6_flowinfo = a.flowinfo();
            sin6.sin6_addr.s6_addr = a.ip().octets();
            sin6.sin6_scope_id = a.scope_id();
            (ss, std::mem::size_of::<libc::sockaddr_in6>() as libc::socklen_t)
        }
    }
}

/// (sockaddr_storage, len) → SocketAddr. None for non-AF_INET/AF_INET6.
/// Preserves IPv6 flowinfo/scope_id so a reply to a received `src` reaches
/// a link-local peer.
pub fn addr_from_sockaddr(ss: &libc::sockaddr_storage, _len: libc::socklen_t) -> Option<std::net::SocketAddr> {
    match ss.ss_family as libc::c_int {
        libc::AF_INET => {
            let sin = unsafe { &*(ss as *const _ as *const libc::sockaddr_in) };
            let ip = std::net::Ipv4Addr::from(sin.sin_addr.s_addr.to_ne_bytes());
            Some(std::net::SocketAddr::V4(std::net::SocketAddrV4::new(ip, u16::from_be(sin.sin_port))))
        }
        libc::AF_INET6 => {
            let sin6 = unsafe { &*(ss as *const _ as *const libc::sockaddr_in6) };
            let ip = std::net::Ipv6Addr::from(sin6.sin6_addr.s6_addr);
            Some(std::net::SocketAddr::V6(std::net::SocketAddrV6::new(
                ip,
                u16::from_be(sin6.sin6_port),
                sin6.sin6_flowinfo,
                sin6.sin6_scope_id,
            )))
        }
        _ => None,
    }
}
```

#### Ring trait

Two new methods, implemented in `IoUringRing` exactly like `prep_recv`
(io-uring 0.7 provides `opcode::SendMsg::new(types::Fd, *const libc::msghdr)`
and `opcode::RecvMsg::new(types::Fd, *mut libc::msghdr)`; both opcodes are
kernel 5.3+, far below the existing 6.7 floor set by `FutexWaitV`):

```rust
/// Submit a one-shot sendmsg(2). The pointed-to msghdr (and everything it
/// references: iovec, payload, sockaddr) must stay alive until the CQE.
fn prep_sendmsg(&mut self, fd: i32, msg: *const libc::msghdr, user_data: u64);

/// Submit a one-shot recvmsg(2). Same lifetime contract, plus the msghdr's
/// msg_name/msg_iov buffers are written by the kernel.
fn prep_recvmsg(&mut self, fd: i32, msg: *mut libc::msghdr, user_data: u64);
```

#### Op storage and CQE routing

New module `crates/gnitz-engine/src/runtime/reactor/udp.rs`. Each in-flight op
owns all kernel-visible memory in one boxed struct held by the reactor until
the CQE drains — the same lifetime discipline as `timer_specs`
(`uring.rs:11-17`) and `send_buffers_in_flight`:

```rust
pub(super) struct UdpSendOp {
    pub(super) msghdr: libc::msghdr,
    pub(super) iov: libc::iovec,
    pub(super) dest: libc::sockaddr_storage,
    pub(super) buf: Vec<u8>, // payload; kernel reads it via iov
}

pub(super) struct UdpRecvOp {
    pub(super) msghdr: libc::msghdr,
    pub(super) iov: libc::iovec,
    pub(super) src: libc::sockaddr_storage, // kernel writes the source here
    pub(super) buf: Vec<u8>,                // capacity = max_datagram; len set on CQE
}
```

The structs are self-referential (msghdr points at the sibling iov/sockaddr
fields), so the kernel-visible pointers are wired only *after* the box is at
its final address: insert the box (zeroed msghdr) into the map first, then
wire the pointers through `get_mut` and prep the SQE — the same
address-stability order `register_conn` uses for `Box<io::Conn>`'s
`hdr_buf_ptr` (`conn.rs:76-82`). The box is never moved again until the CQE
has drained. Send-side wiring:

```rust
let op = ops.get_mut(&id).unwrap(); // Box<UdpSendOp> already in the map
op.iov = libc::iovec {
    iov_base: op.buf.as_ptr() as *mut libc::c_void,
    iov_len: op.buf.len(),
};
op.msghdr.msg_name = &mut op.dest as *mut _ as *mut libc::c_void;
op.msghdr.msg_namelen = dest_len;
op.msghdr.msg_iov = &mut op.iov;
op.msghdr.msg_iovlen = 1;
```

Recv-side wiring differs in three load-bearing details: the buffer is
`Vec::with_capacity(max_datagram)` and the iovec spans the *spare capacity*
(`iov_base = buf.as_mut_ptr()`, `iov_len = max_datagram` — an
`iov_len = buf.len()` copy-paste would be a zero-length read and turn every
datagram into `MSG_TRUNC`); `msg_namelen` must be pre-set to
`size_of::<libc::sockaddr_storage>() as libc::socklen_t` or the kernel writes
no source address; and the CQE handler reads back the *kernel-updated*
`msg_namelen` and `msg_flags` from the op's msghdr (io_uring's recvmsg path
copies both into the submitted msghdr) before `buf.set_len(res as usize)`.

New `ReactorShared` fields, appended **after** the existing `ring` field
(the op-storage maps must drop after `ring`; see the drop-order invariant
below):

```rust
/// In-flight UDP send op storage, keyed by udata id. Removed (and freed)
/// unconditionally when the CQE drains — the kernel is done with the
/// pointers at that moment, awaiter alive or not.
udp_send_ops: RefCell<FxHashMap<u64, Box<UdpSendOp>>>,
/// In-flight UDP recv op storage; same lifetime rule.
udp_recv_ops: RefCell<FxHashMap<u64, Box<UdpRecvOp>>>,
udp_recv_wakers: RefCell<FxHashMap<u64, Waker>>,
parked_udp_recv: RefCell<FxHashMap<u64, Result<UdpDatagram, i32>>>,
/// Recv ids whose future was dropped while its RECVMSG was still armed.
/// `UdpRecvFuture::drop` submits an `AsyncCancel` for the SQE (so it cannot
/// linger and consume a later datagram) and records the id here; the
/// `KIND_UDP_RECV` handler consults it and frees the op box + drops the
/// result — whether the CQE is the `-ECANCELED` from the cancel or a
/// datagram that raced it. Empty on the steady-state path.
cancelled_udp_recvs: RefCell<FxHashSet<u64>>,
```

**Drop-order invariant (already load-bearing, now documented).** Rust drops
struct fields in declaration order, and `ring: RefCell<IoUringRing>` is the
first field of `ReactorShared`. Dropping `ring` runs `IoUring`'s destructor,
which `close()`s the io_uring fd; the kernel's teardown of that fd cancels and
waits out every in-flight SQE, releasing the kernel's references to userspace
buffers. Only *after* that do `udp_send_ops` / `udp_recv_ops` (and the existing
`send_buffers_in_flight` / `conns`) drop and free those buffers. Reversing the
order would free buffers the kernel still owns — a use-after-free during
teardown. This plan keeps `ring` first and adds a doc comment on it recording
the invariant:

```rust
/// SAFETY INVARIANT: `ring` MUST be the first declared field. Rust drops
/// fields in declaration order; dropping `ring` closes the io_uring fd,
/// whose kernel-side teardown cancels all in-flight SQEs and drops the
/// kernel's references to userspace buffers. Only then is it safe to free
/// the buffers those SQEs pointed at — `udp_send_ops`, `udp_recv_ops`,
/// `send_buffers_in_flight`, `conns`. Moving `ring` below any of them is a
/// use-after-free at shutdown.
ring: RefCell<IoUringRing>,
```

UDP sends reuse the existing send result plumbing — ids come from
`alloc_send_id()` (shared counter ⇒ no collisions; it becomes `pub(super)` so
`reactor::udp` can call it — today it has no visibility modifier in
`reactor::conn`), results park in `parked_send_results`, wakers in
`send_wakers`, drop-tombstones in `cancelled_sends`. Recv ids come from the
same counter; the recv family uses its own maps below, so kind-based CQE
routing keeps the two families fully disjoint. Only the storage map is new
on the send side. Two new CQE kinds:

```rust
pub const KIND_UDP_SEND: u64 = 9;
pub const KIND_UDP_RECV: u64 = 10;
/// CQE tag for the `AsyncCancel` a dropped `UdpRecvFuture` submits against
/// its RECVMSG. A dedicated tag (not `KIND_FUTEX_CANCEL`, whose handler sets
/// `futex_waitv_cancelled`) so a UDP recv cancellation never disturbs the
/// FUTEX_WAITV shutdown handshake. The dispatch arm is a pure no-op — the
/// cancellation's *effect* is observed on the RECVMSG's own CQE.
pub const KIND_UDP_CANCEL: u64 = 11;
```

`dispatch_cqe` arms:

- `KIND_UDP_SEND`: `udp_send_ops.remove(&id)` unconditionally (frees payload +
  msghdr storage), then the exact `KIND_SEND` tail: tombstone-gated park of
  `cqe.res` into `parked_send_results` + wake.
- `KIND_UDP_RECV`: `udp_recv_ops.remove(&id)` unconditionally (frees the op
  box). If the tombstone (`cancelled_udp_recvs`) is set, consume it and drop
  everything — this covers both the `-ECANCELED` from a dropped future's cancel
  and a datagram that raced that cancel (dropping the datagram here is
  equivalent to it arriving one instant after the consumer stopped waiting).
  Otherwise park into `parked_udp_recv`:
  - `res < 0` → `Err(res)`.
  - `res >= 0` with `msghdr.msg_flags & libc::MSG_TRUNC != 0` →
    `Err(-libc::EMSGSIZE)` (the datagram exceeded the caller's
    `max_datagram`; silent truncation must not reach the consumer).
  - otherwise `buf.set_len(res as usize)`, decode
    `addr_from_sockaddr(&src, msg_namelen)` (undecodable family →
    `Err(-libc::EAFNOSUPPORT)`), park `Ok(UdpDatagram { buf, src })`, wake.
- `KIND_UDP_CANCEL`: no-op. This is the `AsyncCancel`'s own CQE; the
  cancellation's effect arrives separately as the RECVMSG's `-ECANCELED` under
  `KIND_UDP_RECV` above (same split as the FUTEX_WAITV cancel pair).

#### Public API and futures

```rust
/// One received datagram: exactly-`len` payload plus its source address.
pub struct UdpDatagram {
    pub buf: Vec<u8>,
    pub src: std::net::SocketAddr,
}

impl Reactor {
    /// Submit one datagram to `dest` on the (unconnected) UDP socket `fd`.
    /// The SQE is prepped and flushed immediately (send-path convention,
    /// like `send_buf_inner`); the future resolves to the CQE res
    /// (bytes sent, or negative errno). Datagram sends are all-or-nothing —
    /// no partial-send loop.
    pub fn send_udp(&self, fd: i32, buf: Vec<u8>, dest: std::net::SocketAddr) -> UdpSendFuture;

    /// Await one datagram on `fd`, receiving into a fresh buffer of
    /// capacity `max_datagram`. One-shot: the caller loops to keep a
    /// standing receive armed. Resolves Err(-errno) on socket error or
    /// truncation.
    pub fn recv_udp(&self, fd: i32, max_datagram: usize) -> UdpRecvFuture;
}
```

Both constructors allocate the id, build + insert the boxed op, prep the SQE,
and flush with `submit_and_wait_timeout(0, 0)` (logging on flush error, same
as `send_buf_inner`). Both futures carry a `completed` flag set on
`Poll::Ready`. Neither `Drop` frees op storage — the kernel may still hold the
pointers, and the CQE handler is the single point that frees it. `UdpSendFuture`
resolves `i32`; `UdpRecvFuture` resolves `Result<UdpDatagram, i32>`.

`UdpSendFuture::drop` is exactly `FsyncFuture`-shaped: return early if
completed, else reclaim a result parked after the last poll, else withdraw the
send waker and tombstone the id (`cancelled_sends`). A send SQE left in flight
simply completes and its result is discarded — harmless, so no cancellation.

`UdpRecvFuture::drop` adds one step, because a dropped-but-armed one-shot
RECVMSG is *not* harmless: it outlives its future and consumes the next
datagram to arrive, then the tombstone discards it — silent packet theft (and
the shared QUIC socket can't be closed to cancel one read). So a still-pending
drop first `AsyncCancel`s the SQE, so it cannot linger and eat a later
datagram:

```rust
impl Drop for UdpRecvFuture {
    fn drop(&mut self) {
        if self.completed {
            return;
        }
        self.inner.udp_recv_wakers.borrow_mut().remove(&self.id);
        // CQE already parked a datagram: reclaim (drop) it. Rare — equivalent
        // to the datagram arriving one instant after we stopped waiting.
        if self.inner.parked_udp_recv.borrow_mut().remove(&self.id).is_some() {
            return;
        }
        // CQE still pending: cancel the armed RECVMSG so it cannot linger and
        // silently consume a *later* datagram (a one-shot recv SQE outlives its
        // future). The cancel's own CQE lands on the ignored KIND_UDP_CANCEL
        // sink; the RECVMSG's CQE (-ECANCELED, or a datagram that raced the
        // cancel) lands on KIND_UDP_RECV, where the tombstone frees the op box
        // and drops the result.
        {
            let mut ring = self.inner.ring.borrow_mut();
            ring.prep_async_cancel(udata(KIND_UDP_RECV, self.id), udata(KIND_UDP_CANCEL, 0));
            let _ = ring.submit_and_wait_timeout(0, 0);
        }
        self.inner.cancelled_udp_recvs.borrow_mut().insert(self.id);
    }
}
```

This makes `recv_udp` safe to compose with a timeout — `select2(recv_udp(fd,
n), reactor.timer(deadline))` drops the losing recv on every timer win (that is
how a QUIC loss-detection loop is written), and without the cancel each such
drop would arm a datagram-eating landmine on the shared socket. The one
residual — a datagram delivered into the op buffer in the exact window before
the cancel takes effect — is dropped and is indistinguishable from ordinary
network loss (recovered by QUIC retransmission).

Concurrency contract: any number of `send_udp` ops may be in flight on one fd
(each owns its storage; io_uring may complete them in any order — CQE-order
independence already governs this reactor). Multiple concurrent `recv_udp` on
one fd are sound (each datagram lands in exactly one op) but the intended
consumer runs one at a time in a loop.

Per-datagram `Vec` allocation matches the existing per-message
`libc::malloc` on the stream recv path; no pooling until a benchmark
motivates it.

#### Shutdown semantics

Dropping the reactor with UDP ops in flight has the same contract as the
existing conn recv/send SQEs: the process is exiting (L6 in
async-invariants.md — no graceful shutdown), so no *global* shutdown handshake
is added (unlike the FUTEX_WAITV singleton, whose storage must be reclaimed
mid-run). The per-future `AsyncCancel` in `UdpRecvFuture::drop` is a
correctness measure for the live-composition case (timeout/`select2`), not a
teardown step; at reactor teardown any not-yet-drained cancel or CQE is reaped
by the kernel when the io_uring fd closes (the drop-order invariant above
guarantees that close precedes freeing the op boxes). Tests must still drain
their outstanding UDP ops before dropping the reactor (send the expected
datagram or complete the roundtrip), the same discipline the conn tests follow.

## File Changes

### 1. `crates/gnitz-engine/src/runtime/orchestration/peer.rs` (new, ~160 lines)

`Peer`, `PeerInner`, `RecvBuf` as specified. Registered in
`orchestration/mod.rs`.

### 2. `crates/gnitz-engine/src/runtime/orchestration/executor.rs`

- `accept_loop` constructs `Peer::unix` (executor.rs:269-279).
- `connection_loop`, `run_hello_handshake`, `handle_message`, the handler fns
  currently taking `fd: i32` (executor.rs:960, 1017, 1121, 1178, 1221, 1343,
  1414, 1788), and `send_control_only` / `send_error` / `send_alloc`
  (executor.rs:1810, 1815, 1904): `fd: i32` → `peer: &Peer`; reply calls go
  through `peer.*`.
- `MallocGuard` (executor.rs:291-300) deleted in favor of `RecvBuf`.

### 3. `crates/gnitz-engine/src/runtime/orchestration/master/dispatch.rs`

`fan_out_scan_async` (dispatch.rs:1192), `fan_out_scan_single_worker_async`
(dispatch.rs:1260), `drain_scan_train` (dispatch.rs:1787): `fd: i32` →
`peer: &Peer`; `reactor.send_slot(fd, slot)` → `peer.send_slot(slot)`.

### 4. `crates/gnitz-engine/src/runtime/reactor/conn.rs`

Delete `send_buffer_or_close` / `send_slot_or_close` (conn.rs:140-152); the
composition lives on `Peer`. `alloc_send_id` (conn.rs:9) becomes
`pub(super)` so `reactor::udp` can allocate ids from the shared counter.

### 5. `crates/gnitz-engine/src/runtime/reactor/ring.rs` + `uring.rs`

`prep_sendmsg` / `prep_recvmsg` on the trait and `IoUringRing` (two 6-line
impls following `prep_recv`).

### 6. `crates/gnitz-engine/src/runtime/reactor/udp.rs` (new, ~230 lines)

Op structs, box-then-wire constructors, `send_udp` / `recv_udp`,
`UdpSendFuture` (tombstone-only `Drop`) / `UdpRecvFuture` (`AsyncCancel` +
tombstone `Drop`), `UdpDatagram`.

### 7. `crates/gnitz-engine/src/runtime/reactor/mod.rs`

`KIND_UDP_SEND` / `KIND_UDP_RECV` / `KIND_UDP_CANCEL`, five new `ReactorShared`
fields (+ init in `Reactor::new`), the drop-order doc comment on the existing
`ring` field, three `dispatch_cqe` arms (`KIND_UDP_SEND`, `KIND_UDP_RECV`,
no-op `KIND_UDP_CANCEL`), `mod udp;` + re-exports.

### 8. `crates/gnitz-engine/src/foundation/posix_io.rs`

`udp_bind` (closes the fd on every error path; `c_int` setsockopt args),
`udp_local_addr`, `sockaddr_from_addr` / `addr_from_sockaddr` (round-trip
IPv6 flowinfo/scope_id) (~120 lines incl. doc comments).

## Edge cases

- **Self-referential op boxes.** The msghdr pointers are wired only after
  the box sits in its map and the box is only dropped by the CQE handler;
  HashMap resizes move the `Box` pointer slot, never the pointee. Same
  reasoning as the boxed `io::Conn` (`ReactorShared::conns` doc comment and
  `register_conn`'s insert-then-`hdr_buf_ptr` order).
- **MSG_TRUNC.** Surfaced as `Err(-EMSGSIZE)`, never a silently short
  datagram.
- **Zero-length datagrams.** Valid UDP; `res == 0` with no error parks
  `Ok(UdpDatagram { buf: empty, src })`. (Distinct from the stream path,
  where `res == 0` means EOF.)
- **EINTR/EAGAIN.** io_uring retries internally for sockets; a genuinely
  failed op parks its negative errno and the consumer decides (the future does
  not retry).
- **Send flush failure.** `submit_and_wait_timeout(0,0)` error → SQE stays
  queued, submitted on the next tick (existing convention, `send_buf_inner`).
- **fd reuse.** UDP ops carry no per-fd reactor state (no `Conn`, no
  pending queues), so the `register_conn` sentinel-clearing rules don't apply.
- **Dropped pending recv.** A one-shot RECVMSG outlives its future; a naive
  tombstone-only drop would leave it armed to consume (and discard) a later
  datagram. `UdpRecvFuture::drop` `AsyncCancel`s the SQE instead — see the
  future's `Drop` above. Load-bearing for timeout/`select2` composition.
- **Reactor field drop order.** `ring` is the first `ReactorShared` field so it
  drops (closing the io_uring fd, which cancels+drains in-flight SQEs) before
  the op-storage maps free their buffers. See the drop-order invariant above.

## Tests

New `#[cfg(test)]` module in `reactor/udp.rs`, using the reactor's
`block_on` like the existing reactor tests:

1. **v4 roundtrip:** two `udp_bind("127.0.0.1:0")` sockets; `send_udp` A→B
   (via `udp_local_addr(B)`), `recv_udp` on B returns the payload and A's
   bound address.
2. **v6 roundtrip:** same over `[::1]:0` (validates `sockaddr_storage`
   round-tripping for AF_INET6).
3. **Large datagram:** 60 000-byte payload round-trips intact.
4. **Truncation:** send 2048 bytes, `recv_udp(fd, 512)` resolves
   `Err(-EMSGSIZE)`.
5. **Pipelined sends:** 32 `send_udp` futures joined concurrently; all resolve
   to the payload length; receiver drains 32 datagrams.
6. **Cancelled recv (no packet theft):** construct a `UdpRecvFuture` (the
   constructor arms the RECVMSG) and drop it before any datagram arrives, then
   pump the reactor until `cancelled_udp_recvs` is empty — the drop's
   `AsyncCancel` drives the RECVMSG to a `-ECANCELED` CQE that consumes the
   tombstone and frees the op box. Now send **one** datagram and run a fresh
   `recv_udp` to completion: it resolves with that datagram, proving the
   cancelled op did not steal it. Assert `udp_recv_ops` and
   `cancelled_udp_recvs` are both empty at the end (nothing left in flight at
   reactor drop, per the drain-before-drop rule above).
7. **Error path:** `send_udp` on a closed fd resolves a negative errno.
8. **posix_io unit tests:** `sockaddr_from_addr` / `addr_from_sockaddr`
   round-trip for v4/v6; `udp_local_addr` resolves a port-0 bind.

Part A has no new tests: it is a pure refactor and the entire existing suite
(`make test`, `make e2e` with `WORKERS=4`) must pass unchanged — scans
exercise `drain_scan_train`/`send_slot`, pushes exercise
`send_buffer_or_close`, HELLO rejection exercises `peer.close()`.

## Implementation order

- [ ] 1. `posix_io` UDP helpers + their unit tests.
- [ ] 2. `Ring::prep_sendmsg`/`prep_recvmsg`, `reactor/udp.rs`, `dispatch_cqe`
       arms, reactor UDP tests (1–7). `make test` green.
- [ ] 3. `peer.rs` + executor/dispatch refactor, delete `MallocGuard` and the
       reactor `_or_close` methods. `make test` and `make e2e` green.
