//! Reactor UDP datagram ops: one-shot `sendmsg`/`recvmsg`, for transports
//! whose client connections are not fds (a userspace QUIC endpoint sees only
//! UDP datagrams on a single socket).
//!
//! Each in-flight op owns all kernel-visible memory (msghdr, iovec, sockaddr,
//! payload) in one heap struct that is self-referential (the msghdr points at
//! the sibling iov/sockaddr fields) and therefore must not move until its CQE
//! drains. Sends ride the stream-send machinery end to end: the op lives in
//! an `Rc` inside `SendAlive::UdpOp` (moving or cloning an `Rc` handle never
//! moves the pointee), the CQE lands on the ordinary `KIND_SEND` arm, and the
//! future is the ordinary `SendFuture`. Recvs need a dedicated CQE handler
//! (it decodes buf/src/msg_flags into the result), so the recv op is a `Box`
//! held in a `ReactorShared` map until the CQE — pointers wired only *after*
//! the box sits at its final heap address, and the box never moved again
//! until the handler removes it (HashMap resizes move the `Box` pointer slot,
//! never the pointee — same reasoning as the boxed `io::Conn`).

use super::futures::{SendAlive, SendFuture};
use super::*;

use crate::foundation::posix_io::{addr_from_sockaddr, sockaddr_from_addr};
use crate::storage::batch_pool::recycle_buf;

/// In-flight one-shot SENDMSG. The kernel reads `buf` through `iov` and
/// `dest` through `msghdr.msg_name` until the CQE.
#[allow(dead_code)] // kernel-visible storage: read via the wired msghdr pointers, never by Rust code
pub(super) struct UdpSendOp {
    msghdr: libc::msghdr,
    iov: libc::iovec,
    dest: libc::sockaddr_storage,
    buf: Vec<u8>,
}

impl Drop for UdpSendOp {
    /// Runs only once the kernel is done with the wired pointers: after the
    /// CQE (future resolved, or `send_buffers_in_flight` reclaimed a dropped
    /// future's op), or after ring teardown (see the `ReactorShared.ring`
    /// drop-order invariant). Recycling the payload keeps steady-state
    /// datagram sends allocation-free, matching the stream path's
    /// `PooledSendBuf` convention.
    fn drop(&mut self) {
        recycle_buf(std::mem::take(&mut self.buf));
    }
}

/// In-flight one-shot RECVMSG. The kernel writes the payload into `buf`'s
/// spare capacity through `iov` and the source address into `src`.
pub(super) struct UdpRecvOp {
    msghdr: libc::msghdr,
    #[allow(dead_code)] // kernel-visible storage: read via msghdr.msg_iov, never by Rust code
    iov: libc::iovec,
    src: libc::sockaddr_storage,
    /// capacity = max_datagram; len set from the CQE `res` on completion.
    buf: Vec<u8>,
}

/// One received datagram: exactly-`len` payload plus its source address.
#[allow(dead_code)] // read only by tests until the QUIC endpoint consumes datagrams
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
    ///
    /// The CQE lands on the plain `KIND_SEND` arm: UDP send ids come from
    /// the same counter as stream sends and share the waker / park /
    /// tombstone maps, and `SendAlive::UdpOp` gives the op exactly the
    /// stream buffer's keep-alive discipline — held by the future while it
    /// awaits, parked in `send_buffers_in_flight` if the future drops before
    /// the CQE.
    #[allow(dead_code)] // test-only until the QUIC endpoint drives the datagram API
    pub fn send_udp(&self, fd: i32, buf: Vec<u8>, dest: std::net::SocketAddr) -> SendFuture {
        let id = self.alloc_send_id();
        let (dest_ss, dest_len) = sockaddr_from_addr(&dest);
        let mut op = Rc::new(UdpSendOp {
            msghdr: unsafe { std::mem::zeroed() },
            iov: libc::iovec {
                iov_base: std::ptr::null_mut(),
                iov_len: 0,
            },
            dest: dest_ss,
            buf,
        });
        {
            // Wire the self-referential pointers behind the `Rc`: the pointee
            // is heap-pinned from construction, and the handle moves/clones
            // below never touch it.
            let op = Rc::get_mut(&mut op).expect("freshly constructed Rc is unique");
            op.iov = libc::iovec {
                iov_base: op.buf.as_ptr() as *mut libc::c_void,
                iov_len: op.buf.len(),
            };
            op.msghdr.msg_name = &mut op.dest as *mut _ as *mut libc::c_void;
            op.msghdr.msg_namelen = dest_len;
            op.msghdr.msg_iov = &mut op.iov;
            op.msghdr.msg_iovlen = 1;
            let msg_ptr: *const libc::msghdr = &op.msghdr;
            let mut ring = self.inner.ring.borrow_mut();
            ring.prep_sendmsg(fd, msg_ptr, udata(KIND_SEND, id));
            if let Err(e) = ring.submit_and_wait_timeout(0, 0) {
                crate::gnitz_error!(
                    "reactor: UDP send SQE flush failed for id={} (errno={}); \
                     SQE queued — will submit on next tick",
                    id,
                    e,
                );
            }
        }
        SendFuture {
            send_id: id,
            _alive: Some(SendAlive::UdpOp(op)),
            inner: Rc::clone(&self.inner),
        }
    }

    /// Await one datagram on `fd`, receiving into a fresh buffer of
    /// capacity `max_datagram`. One-shot: the caller loops to keep a
    /// standing receive armed. Resolves Err(-errno) on socket error or
    /// truncation.
    #[allow(dead_code)] // test-only until the QUIC endpoint drives the datagram API
    pub fn recv_udp(&self, fd: i32, max_datagram: usize) -> UdpRecvFuture {
        let id = self.alloc_send_id();
        {
            let mut ops = self.inner.udp_recv_ops.borrow_mut();
            // `entry` inserts and hands back `&mut` in one lookup; the
            // pointers are wired only now that the box sits at its final
            // heap address (insert-then-wire, like `register_conn`).
            let op = ops.entry(id).or_insert_with(|| {
                Box::new(UdpRecvOp {
                    msghdr: unsafe { std::mem::zeroed() },
                    iov: libc::iovec {
                        iov_base: std::ptr::null_mut(),
                        iov_len: 0,
                    },
                    src: unsafe { std::mem::zeroed() },
                    buf: Vec::with_capacity(max_datagram),
                })
            });
            // The iovec spans the buffer's *spare capacity* (`iov_len =
            // max_datagram`, not `buf.len()` — that copy-paste would be a
            // zero-length read turning every datagram into MSG_TRUNC).
            op.iov = libc::iovec {
                iov_base: op.buf.as_mut_ptr() as *mut libc::c_void,
                iov_len: max_datagram,
            };
            op.msghdr.msg_name = &mut op.src as *mut _ as *mut libc::c_void;
            // msg_namelen must be pre-set to the storage size or the kernel
            // writes no source address.
            op.msghdr.msg_namelen = std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;
            op.msghdr.msg_iov = &mut op.iov;
            op.msghdr.msg_iovlen = 1;
            let msg_ptr: *mut libc::msghdr = &mut op.msghdr;
            let mut ring = self.inner.ring.borrow_mut();
            ring.prep_recvmsg(fd, msg_ptr, udata(KIND_UDP_RECV, id));
            if let Err(e) = ring.submit_and_wait_timeout(0, 0) {
                crate::gnitz_error!(
                    "reactor: UDP recv SQE flush failed for id={} (errno={}); \
                     SQE queued — will submit on next tick",
                    id,
                    e,
                );
            }
        }
        UdpRecvFuture {
            id,
            completed: false,
            inner: Rc::clone(&self.inner),
        }
    }

    /// `KIND_UDP_RECV` CQE: free the op box unconditionally (the kernel is
    /// done with its pointers), then — unless the future was dropped — park
    /// the decoded result and wake the awaiter.
    pub(super) fn handle_udp_recv_cqe(&self, id: u64, cqe: &Cqe) {
        let op = self.inner.udp_recv_ops.borrow_mut().remove(&id);
        let cancelled = {
            let mut c = self.inner.cancelled_udp_recvs.borrow_mut();
            !c.is_empty() && c.remove(&id)
        };
        if cancelled {
            // Dropped future: covers both the -ECANCELED from its AsyncCancel
            // and a datagram that raced the cancel (dropping the datagram here
            // is equivalent to it arriving one instant after the consumer
            // stopped waiting).
            return;
        }
        let Some(mut op) = op else {
            return; // no op storage for this id — nothing to deliver
        };
        let res = cqe.res;
        let result: Result<UdpDatagram, i32> = if res < 0 {
            Err(res)
        } else if op.msghdr.msg_flags & libc::MSG_TRUNC != 0 {
            // The datagram exceeded the caller's max_datagram; silent
            // truncation must not reach the consumer. (io_uring's recvmsg path
            // copies the kernel-updated msg_flags/msg_namelen back into the
            // submitted msghdr.)
            Err(-libc::EMSGSIZE)
        } else {
            // SAFETY: the kernel wrote exactly `res` bytes through the iovec
            // spanning buf's capacity (res <= max_datagram — larger datagrams
            // take the MSG_TRUNC arm above).
            unsafe { op.buf.set_len(res as usize) };
            match addr_from_sockaddr(&op.src) {
                Some(src) => Ok(UdpDatagram { buf: op.buf, src }),
                None => Err(-libc::EAFNOSUPPORT),
            }
        };
        self.inner.parked_udp_recv.borrow_mut().insert(id, result);
        if let Some(w) = self.inner.udp_recv_wakers.borrow_mut().remove(&id) {
            w.wake();
        }
    }
}

/// Resolves to one datagram, or Err(negative errno) on socket error /
/// truncation / undecodable source family.
pub struct UdpRecvFuture {
    id: u64,
    completed: bool,
    inner: Rc<ReactorShared>,
}

impl Future for UdpRecvFuture {
    type Output = Result<UdpDatagram, i32>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let parked = self.inner.parked_udp_recv.borrow_mut().remove(&self.id);
        if let Some(r) = parked {
            self.completed = true;
            return Poll::Ready(r);
        }
        self.inner
            .udp_recv_wakers
            .borrow_mut()
            .insert(self.id, cx.waker().clone());
        Poll::Pending
    }
}

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
        // future, and the shared QUIC socket can't be closed to cancel one
        // read). This makes `recv_udp` safe to compose with a timeout —
        // `select2(recv_udp(..), timer(..))` drops the losing recv on every
        // timer win. The cancel's own CQE lands on the ignored KIND_UDP_CANCEL
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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    const BUF_BYTES: usize = 1 << 20;

    /// Create + bind a UDP socket on `addr`. Requests `rcvbuf_bytes` /
    /// `sndbuf_bytes` via SO_RCVBUF/SO_SNDBUF (best-effort; the kernel may
    /// clamp). Returns the fd, or a negative value on error.
    ///
    /// The socket is left unconnected — every send carries an explicit
    /// destination — and blocking (O_NONBLOCK is irrelevant under io_uring).
    /// Test-only until the network transport binds its QUIC socket.
    fn udp_bind(addr: std::net::SocketAddr, rcvbuf_bytes: usize, sndbuf_bytes: usize) -> i32 {
        let family = match addr {
            std::net::SocketAddr::V4(_) => libc::AF_INET,
            std::net::SocketAddr::V6(_) => libc::AF_INET6,
        };
        unsafe {
            let fd = libc::socket(family, libc::SOCK_DGRAM, 0);
            if fd < 0 {
                return -1;
            }
            if !set_sock_buf(fd, libc::SO_RCVBUF, rcvbuf_bytes) {
                libc::close(fd);
                return -2;
            }
            if !set_sock_buf(fd, libc::SO_SNDBUF, sndbuf_bytes) {
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

    /// Set a socket buffer-size option (SO_RCVBUF / SO_SNDBUF). These options
    /// take a *const c_int (4 bytes); passing a pointer to `usize` would hand
    /// the kernel 4 of its 8 bytes — correct only by luck on little-endian.
    /// Copy into a c_int and point at that.
    unsafe fn set_sock_buf(fd: i32, opt: libc::c_int, bytes: usize) -> bool {
        let v = bytes as libc::c_int;
        libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            opt,
            &v as *const libc::c_int as *const libc::c_void,
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        ) >= 0
    }

    /// getsockname() as a SocketAddr — resolves the real port after a
    /// port-0 bind. Returns None on error or non-INET family.
    fn udp_local_addr(fd: i32) -> Option<std::net::SocketAddr> {
        let mut ss: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
        let mut len = std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;
        let rc = unsafe { libc::getsockname(fd, &mut ss as *mut _ as *mut libc::sockaddr, &mut len) };
        if rc < 0 {
            return None;
        }
        addr_from_sockaddr(&ss)
    }

    #[test]
    fn test_udp_bind_resolves_port() {
        let fd = udp_bind("127.0.0.1:0".parse().unwrap(), BUF_BYTES, BUF_BYTES);
        assert!(fd >= 0, "udp_bind failed: {fd}");
        let local = udp_local_addr(fd).expect("udp_local_addr");
        assert!(local.is_ipv4());
        assert_eq!(local.ip().to_string(), "127.0.0.1");
        assert_ne!(local.port(), 0, "port-0 bind must resolve to a real port");
        close(fd);
    }

    #[test]
    fn test_udp_bind_v6() {
        let fd = udp_bind("[::1]:0".parse().unwrap(), BUF_BYTES, BUF_BYTES);
        assert!(fd >= 0, "udp_bind v6 failed: {fd}");
        let local = udp_local_addr(fd).expect("udp_local_addr");
        assert!(local.is_ipv6());
        assert_ne!(local.port(), 0);
        close(fd);
    }

    fn make_reactor() -> Reactor {
        Reactor::new(64).expect("reactor")
    }

    /// Bind a UDP socket on `addr` (port 0) and return (fd, resolved addr).
    fn bind(addr: &str) -> (i32, std::net::SocketAddr) {
        let fd = udp_bind(addr.parse().unwrap(), BUF_BYTES, BUF_BYTES);
        assert!(fd >= 0, "udp_bind({addr}) failed: {fd}");
        let local = udp_local_addr(fd).expect("udp_local_addr");
        (fd, local)
    }

    fn close(fd: i32) {
        unsafe {
            libc::close(fd);
        }
    }

    fn roundtrip(addr: &str, payload: Vec<u8>) {
        let r = make_reactor();
        let (a, a_addr) = bind(addr);
        let (b, b_addr) = bind(addr);
        let send_fut = r.send_udp(a, payload.clone(), b_addr);
        let recv_fut = r.recv_udp(b, payload.len().max(64));
        let (rc, got) = r.block_on(async move {
            let rc = send_fut.await;
            let got = recv_fut.await;
            (rc, got)
        });
        assert_eq!(rc as usize, payload.len(), "send resolved {rc}");
        let dgram = got.expect("recv_udp errored");
        assert_eq!(dgram.buf, payload);
        assert_eq!(dgram.src, a_addr, "source address must be A's bound address");
        close(a);
        close(b);
    }

    #[test]
    fn udp_v4_roundtrip() {
        roundtrip("127.0.0.1:0", b"hello over v4".to_vec());
    }

    #[test]
    fn udp_v6_roundtrip() {
        // Validates sockaddr_storage round-tripping for AF_INET6.
        roundtrip("[::1]:0", b"hello over v6".to_vec());
    }

    #[test]
    fn udp_large_datagram_roundtrips_intact() {
        let payload: Vec<u8> = (0..60_000u32).map(|i| (i % 251) as u8).collect();
        roundtrip("127.0.0.1:0", payload);
    }

    #[test]
    fn udp_truncation_is_an_error_not_a_short_read() {
        let r = make_reactor();
        let (a, _) = bind("127.0.0.1:0");
        let (b, b_addr) = bind("127.0.0.1:0");
        let send_fut = r.send_udp(a, vec![7u8; 2048], b_addr);
        let recv_fut = r.recv_udp(b, 512);
        let (rc, got) = r.block_on(async move {
            let rc = send_fut.await;
            let got = recv_fut.await;
            (rc, got)
        });
        assert_eq!(rc, 2048);
        assert_eq!(got.err(), Some(-libc::EMSGSIZE));
        close(a);
        close(b);
    }

    #[test]
    fn udp_pipelined_sends_all_complete() {
        const N: usize = 32;
        let r = make_reactor();
        let (a, _) = bind("127.0.0.1:0");
        let (b, b_addr) = bind("127.0.0.1:0");
        // All N SQEs are armed by the constructors before any await — they
        // are concurrently in flight regardless of await order.
        let sends: Vec<SendFuture> = (0..N).map(|i| r.send_udp(a, vec![i as u8; 100 + i], b_addr)).collect();
        let rcs = r.block_on(async move {
            let mut rcs = Vec::with_capacity(N);
            for s in sends {
                rcs.push(s.await);
            }
            rcs
        });
        for (i, rc) in rcs.iter().enumerate() {
            assert_eq!(*rc as usize, 100 + i, "send {i} resolved {rc}");
        }
        // Drain all 32 datagrams (order not asserted — UDP does not
        // guarantee it, though loopback preserves it in practice).
        for _ in 0..N {
            let fut = r.recv_udp(b, 2048);
            let dgram = r.block_on(fut).expect("recv errored");
            assert_eq!(dgram.buf.len(), 100 + dgram.buf[0] as usize);
        }
        close(a);
        close(b);
    }

    /// A dropped pending `UdpRecvFuture` must cancel its armed RECVMSG rather
    /// than leave it to consume (and discard) the next datagram.
    #[test]
    fn udp_cancelled_recv_does_not_steal_a_datagram() {
        let r = make_reactor();
        let (a, _) = bind("127.0.0.1:0");
        let (b, b_addr) = bind("127.0.0.1:0");

        // Arm a RECVMSG and drop the future before any datagram arrives. The
        // Drop submits an AsyncCancel and tombstones the id.
        let doomed = r.recv_udp(b, 2048);
        drop(doomed);

        // Pump the reactor until the -ECANCELED CQE consumes the tombstone
        // and frees the op box.
        let deadline = Instant::now() + Duration::from_secs(2);
        while !r.inner.cancelled_udp_recvs.borrow().is_empty() && Instant::now() < deadline {
            let _ = r.inner.ring.borrow_mut().submit_and_wait_timeout(1, 10);
            r.drain_cqes_into_wakers();
        }
        assert!(
            r.inner.cancelled_udp_recvs.borrow().is_empty(),
            "cancel CQE did not drain the tombstone within 2s"
        );
        assert!(r.inner.udp_recv_ops.borrow().is_empty(), "op box leaked");

        // Now send ONE datagram; a fresh recv must receive it — proving the
        // cancelled op did not steal it.
        let send_fut = r.send_udp(a, b"the one datagram".to_vec(), b_addr);
        let recv_fut = r.recv_udp(b, 2048);
        let (rc, got) = r.block_on(async move {
            let rc = send_fut.await;
            let got = recv_fut.await;
            (rc, got)
        });
        assert!(rc > 0);
        assert_eq!(got.expect("recv errored").buf, b"the one datagram");

        // Drain-before-drop rule: nothing left in flight at reactor drop.
        assert!(r.inner.udp_recv_ops.borrow().is_empty());
        assert!(r.inner.cancelled_udp_recvs.borrow().is_empty());
        close(a);
        close(b);
    }

    #[test]
    fn udp_send_on_invalid_fd_resolves_negative_errno() {
        let r = make_reactor();
        let (b, b_addr) = bind("127.0.0.1:0");
        // fd -1 is never valid; a *closed* fd number could be reused by a
        // concurrent test's socket (process-wide fd namespace) and let the
        // send spuriously succeed.
        let fut = r.send_udp(-1, b"doomed".to_vec(), b_addr);
        let rc = r.block_on(fut);
        assert_eq!(rc, -libc::EBADF, "send on invalid fd resolved {rc}");
        close(b);
    }
}
