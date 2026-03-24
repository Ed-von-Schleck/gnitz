use std::collections::HashMap;

use crate::conn::Conn;
use crate::recv::RecvAdvance;
use crate::ring::{make_udata, cqe_fd, cqe_tag, Cqe, Ring, CQE_F_MORE, TAG_ACCEPT, TAG_RECV, TAG_SEND};

/// Maximum payload size accepted from a client (64 MB).
const MAX_PAYLOAD_LEN: usize = 64 * 1024 * 1024;

/// A complete message ready for delivery to the caller.
#[derive(Clone, Copy)]
pub struct Message {
    pub fd: i32,
    pub ptr: *mut u8,
    pub len: usize,
}

/// Output slot array — typed view over caller-provided C arrays.
pub struct OutputSlots<'a> {
    pub fds: &'a mut [i32],
    pub ptrs: &'a mut [*mut u8],
    pub lens: &'a mut [u32],
    pub max: usize,
}

/// The core transport state machine, generic over the Ring implementation.
///
/// Owns all per-connection state. Provides a simple poll → dispatch → send API
/// that replaces the 9-dict state machine and two-pass CQE routing in executor.py.
pub struct Transport<R: Ring> {
    ring: R,
    // Box<Conn> ensures the Conn (and its inline hdr_buf) lives on the
    // heap. When the HashMap resizes, only the Box pointer moves — the
    // Conn itself stays put, so io_uring SQEs holding pointers into
    // hdr_buf remain valid.
    conns: HashMap<i32, Box<Conn>>,
    dirty_fds: Vec<i32>,
    pending_delivery: Vec<i32>,
    cqe_buf: Vec<Cqe>,
    server_fd: i32,
    accept_udata: u64,
}

impl<R: Ring> Transport<R> {
    pub fn new(ring: R, buf_size: usize) -> Self {
        Transport {
            ring,
            conns: HashMap::new(),
            dirty_fds: Vec::with_capacity(64),
            pending_delivery: Vec::new(),
            cqe_buf: vec![Cqe::default(); buf_size],
            server_fd: -1,
            accept_udata: make_udata(TAG_ACCEPT, 0),
        }
    }

    /// Start accepting connections on `server_fd`.
    pub fn accept_conn(&mut self, server_fd: i32) {
        self.server_fd = server_fd;
        self.ring.prep_accept(server_fd, self.accept_udata);
    }

    /// Register a new client connection: create Conn, arm header recv.
    fn accept_new(&mut self, fd: i32) {
        self.conns.insert(fd, Box::new(Conn::new()));
        let conn = self.conns.get_mut(&fd).unwrap();
        let hdr_ptr = conn.recv_state.hdr_buf_ptr();
        self.ring
            .prep_recv(fd, hdr_ptr, 4, make_udata(TAG_RECV, fd));
        conn.recv_armed = true;
    }

    /// Queue a response for sending. Returns 0 on success, -1 if the
    /// connection is closing or unknown.
    ///
    /// The 4-byte LE length header is prepended internally.
    pub fn send(&mut self, fd: i32, data: *const u8, len: usize) -> i32 {
        let Some(conn) = self.conns.get_mut(&fd) else {
            return -1;
        };
        if conn.closing {
            return -1;
        }
        let hdr = (len as u32).to_le_bytes();
        conn.send_queue
            .push(&hdr, unsafe { std::slice::from_raw_parts(data, len) });
        self.dirty_fds.push(fd);
        0
    }

    /// Request connection close. Actual close is deferred until all
    /// outstanding SQEs have produced CQEs.
    pub fn close_fd(&mut self, fd: i32) {
        if let Some(conn) = self.conns.get_mut(&fd) {
            conn.closing = true;
        }
    }

    /// Free a recv buffer previously returned by `poll`.
    pub fn free_recv(ptr: *mut u8) {
        if !ptr.is_null() {
            unsafe {
                libc::free(ptr as *mut libc::c_void);
            }
        }
    }

    /// Reap connections that are closing and have no outstanding SQEs.
    fn reap_closing(&mut self) {
        self.conns.retain(|&fd, conn| {
            if conn.closing && !conn.has_outstanding() {
                // Conn::drop() frees any in-progress payload buffer
                let _ = unsafe { libc::close(fd) };
                false
            } else {
                true
            }
        });
    }

    /// The main poll function. Blocks up to `timeout_ms` for I/O, processes
    /// completions, and returns complete messages via the output slots.
    ///
    /// Returns the number of messages written to the output slots.
    pub fn poll(&mut self, timeout_ms: i32, out: &mut OutputSlots<'_>) -> i32 {
        let mut msg_count: usize = 0;

        // ── Phase -1: deliver held messages from previous output overflow ──
        if !self.pending_delivery.is_empty() {
            let mut delivered = 0;
            for i in 0..self.pending_delivery.len() {
                if msg_count >= out.max {
                    break;
                }
                let fd = self.pending_delivery[i];
                let Some(conn) = self.conns.get_mut(&fd) else {
                    delivered += 1;
                    continue;
                };
                if conn.closing {
                    delivered += 1;
                    continue;
                }
                let (ptr, len) = conn.recv_state.take_message();
                out.fds[msg_count] = fd;
                out.ptrs[msg_count] = ptr;
                out.lens[msg_count] = len as u32;
                msg_count += 1;
                // Re-arm for next header
                let hdr = conn.recv_state.hdr_buf_ptr();
                self.ring.prep_recv(fd, hdr, 4, make_udata(TAG_RECV, fd));
                conn.recv_armed = true;
                delivered += 1;
            }
            self.pending_delivery.drain(..delivered);

            if msg_count >= out.max {
                let _ = self.ring.submit_and_wait_timeout(0, 0);
                self.reap_closing();
                return msg_count as i32;
            }
        }

        // ── Phase 0: arm sends for dirty connections ──
        for i in 0..self.dirty_fds.len() {
            let fd = self.dirty_fds[i];
            let Some(conn) = self.conns.get_mut(&fd) else {
                continue;
            };
            if conn.closing || conn.send_queue.inflight || !conn.send_queue.has_pending() {
                continue;
            }
            let (ptr, len) = conn.send_queue.prepare().unwrap();
            self.ring
                .prep_send(fd, ptr, len as u32, make_udata(TAG_SEND, fd));
        }
        self.dirty_fds.clear();

        // ── Initial submit + wait ──
        let _ = self.ring.submit_and_wait_timeout(1, timeout_ms);

        // ── Inner drain loop ──
        loop {
            let n = self.ring.drain_cqes(&mut self.cqe_buf);
            if n == 0 {
                break;
            }

            let mut new_recv_sqes = false;

            for ci in 0..n {
                let cqe = self.cqe_buf[ci];
                let fd = cqe_fd(cqe.user_data);

                match cqe_tag(cqe.user_data) {
                    TAG_RECV => {
                        let Some(conn) = self.conns.get_mut(&fd) else {
                            continue;
                        };
                        conn.recv_armed = false;

                        if cqe.res <= 0 || conn.closing {
                            conn.closing = true;
                            continue;
                        }

                        match conn.recv_state.advance(cqe.res as usize) {
                            RecvAdvance::NeedMore => {
                                let (buf, len) = conn.recv_state.remaining();
                                self.ring.prep_recv(
                                    fd,
                                    buf,
                                    len,
                                    make_udata(TAG_RECV, fd),
                                );
                                conn.recv_armed = true;
                                new_recv_sqes = true;
                            }
                            RecvAdvance::HeaderDone => {
                                let plen = conn.recv_state.payload_len();
                                if plen > MAX_PAYLOAD_LEN {
                                    conn.closing = true;
                                    continue;
                                }
                                let pbuf =
                                    unsafe { libc::malloc(plen) as *mut u8 };
                                if pbuf.is_null() {
                                    conn.closing = true;
                                    continue;
                                }
                                conn.recv_state.start_payload(pbuf, plen);
                                self.ring.prep_recv(
                                    fd,
                                    pbuf,
                                    plen as u32,
                                    make_udata(TAG_RECV, fd),
                                );
                                conn.recv_armed = true;
                                new_recv_sqes = true;
                            }
                            RecvAdvance::MessageDone => {
                                if msg_count >= out.max {
                                    // Output full — leave message in RecvState,
                                    // deliver in Phase -1 of next poll.
                                    self.pending_delivery.push(fd);
                                    continue;
                                }
                                let (ptr, len) = conn.recv_state.take_message();
                                out.fds[msg_count] = fd;
                                out.ptrs[msg_count] = ptr;
                                out.lens[msg_count] = len as u32;
                                msg_count += 1;
                                // Rearm for next header
                                let hdr = conn.recv_state.hdr_buf_ptr();
                                self.ring.prep_recv(
                                    fd,
                                    hdr,
                                    4,
                                    make_udata(TAG_RECV, fd),
                                );
                                conn.recv_armed = true;
                                new_recv_sqes = true;
                            }
                            RecvAdvance::Disconnect => {
                                conn.closing = true;
                            }
                        }
                    }
                    TAG_SEND => {
                        let Some(conn) = self.conns.get_mut(&fd) else {
                            continue;
                        };
                        if cqe.res > 0 && !conn.closing {
                            conn.send_queue.complete(cqe.res as usize);
                            // Immediately try to send more if queued
                            if let Some((ptr, len)) = conn.send_queue.prepare()
                            {
                                self.ring.prep_send(
                                    fd,
                                    ptr,
                                    len as u32,
                                    make_udata(TAG_SEND, fd),
                                );
                            }
                        } else {
                            conn.send_queue.inflight = false;
                            conn.closing = true;
                        }
                    }
                    TAG_ACCEPT => {
                        if cqe.res >= 0 {
                            self.accept_new(cqe.res as i32);
                        }
                        if cqe.flags & CQE_F_MORE == 0 {
                            self.ring
                                .prep_accept(self.server_fd, self.accept_udata);
                        }
                    }
                    _ => {}
                }
            }

            if !new_recv_sqes || msg_count >= out.max {
                break;
            }

            // Submit new SQEs, non-blocking
            let _ = self.ring.submit_and_wait_timeout(0, 0);
        }

        // ── Final submit: flush any SQEs from last drain iteration ──
        let _ = self.ring.submit_and_wait_timeout(0, 0);

        // ── Reap dead connections ──
        self.reap_closing();

        msg_count as i32
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mock::MockRing;
    use crate::ring::*;

    fn make_output(n: usize) -> (Vec<i32>, Vec<*mut u8>, Vec<u32>) {
        (
            vec![0i32; n],
            vec![std::ptr::null_mut(); n],
            vec![0u32; n],
        )
    }

    fn slots<'a>(
        fds: &'a mut [i32],
        ptrs: &'a mut [*mut u8],
        lens: &'a mut [u32],
    ) -> OutputSlots<'a> {
        let max = fds.len();
        OutputSlots { fds, ptrs, lens, max }
    }

    // ── Inner drain loop tests ──

    #[test]
    fn test_single_message_delivery() {
        let mut ring = MockRing::new();

        // Simulate: a connection with fd=10 has an outstanding header recv.
        // CQE arrives with 4-byte header complete.
        let payload = b"hello world!";
        let payload_len = payload.len() as u32;
        let hdr = payload_len.to_le_bytes();

        // Batch 1: header recv completes (4 bytes)
        ring.push_cqes(vec![Cqe {
            user_data: make_udata(TAG_RECV, 10),
            res: 4,
            flags: 0,
        }]);

        // Batch 2: payload recv completes
        ring.push_cqes(vec![Cqe {
            user_data: make_udata(TAG_RECV, 10),
            res: payload_len as i32,
            flags: 0,
        }]);

        let mut transport = Transport::new(ring, 256);

        // Manually insert a connection with header in-progress
        let mut conn = Conn::new();
        conn.recv_state.hdr_buf = hdr;
        conn.recv_armed = true;
        transport.conns.insert(10, Box::new(conn));

        let (mut fds, mut ptrs, mut lens) = make_output(256);
        let n = transport.poll(500, &mut slots(&mut fds, &mut ptrs, &mut lens));

        assert_eq!(n, 1);
        assert_eq!(fds[0], 10);
        assert_eq!(lens[0], payload_len);
        assert!(!ptrs[0].is_null());

        // Free the recv buffer
        Transport::<MockRing>::free_recv(ptrs[0]);
    }

    #[test]
    fn test_multi_connection_batching() {
        let mut ring = MockRing::new();

        // 3 connections all complete headers simultaneously
        ring.push_cqes(vec![
            Cqe { user_data: make_udata(TAG_RECV, 10), res: 4, flags: 0 },
            Cqe { user_data: make_udata(TAG_RECV, 11), res: 4, flags: 0 },
            Cqe { user_data: make_udata(TAG_RECV, 12), res: 4, flags: 0 },
        ]);

        // All 3 payload recvs complete
        ring.push_cqes(vec![
            Cqe { user_data: make_udata(TAG_RECV, 10), res: 8, flags: 0 },
            Cqe { user_data: make_udata(TAG_RECV, 11), res: 16, flags: 0 },
            Cqe { user_data: make_udata(TAG_RECV, 12), res: 4, flags: 0 },
        ]);

        let mut transport = Transport::new(ring, 256);

        // Insert 3 connections with header buffers set
        for (fd, plen) in [(10, 8u32), (11, 16u32), (12, 4u32)] {
            let mut conn = Conn::new();
            conn.recv_state.hdr_buf = plen.to_le_bytes();
            conn.recv_armed = true;
            transport.conns.insert(fd, Box::new(conn));
        }

        let (mut fds, mut ptrs, mut lens) = make_output(256);
        let n = transport.poll(500, &mut slots(&mut fds, &mut ptrs, &mut lens));

        assert_eq!(n, 3);

        // Free all recv buffers
        for i in 0..3 {
            Transport::<MockRing>::free_recv(ptrs[i]);
        }
    }

    // ── Deferred close tests ──

    #[test]
    fn test_deferred_close_recv_fails_send_inflight() {
        let mut ring = MockRing::new();

        // RECV CQE with res=0 (disconnect) while send is inflight
        ring.push_cqes(vec![Cqe {
            user_data: make_udata(TAG_RECV, 10),
            res: 0,
            flags: 0,
        }]);

        let mut transport = Transport::new(ring, 256);
        let mut conn = Conn::new();
        conn.recv_armed = true;
        conn.send_queue.inflight = true; // send in-flight
        transport.conns.insert(10, Box::new(conn));

        let (mut fds, mut ptrs, mut lens) = make_output(256);
        let n = transport.poll(0, &mut slots(&mut fds, &mut ptrs, &mut lens));
        assert_eq!(n, 0);

        // Conn should still exist (send still inflight)
        assert!(transport.conns.contains_key(&10));
        assert!(transport.conns[&10].closing);
    }

    #[test]
    fn test_deferred_close_both_fail_same_batch() {
        let mut ring = MockRing::new();

        // Both RECV and SEND fail in the same CQE batch
        ring.push_cqes(vec![
            Cqe { user_data: make_udata(TAG_RECV, 10), res: 0, flags: 0 },
            Cqe { user_data: make_udata(TAG_SEND, 10), res: -32, flags: 0 },
        ]);

        let mut transport = Transport::new(ring, 256);
        let mut conn = Conn::new();
        conn.recv_armed = true;
        conn.send_queue.inflight = true;
        transport.conns.insert(10, Box::new(conn));

        let (mut fds, mut ptrs, mut lens) = make_output(256);
        transport.poll(0, &mut slots(&mut fds, &mut ptrs, &mut lens));

        // Conn should be reaped (both recv_armed and inflight are now false)
        assert!(!transport.conns.contains_key(&10));
    }

    #[test]
    fn test_transport_close_fd_while_sqs_outstanding() {
        let mut ring = MockRing::new();
        // No CQEs — just testing close_fd behavior
        let mut transport = Transport::new(ring, 256);

        let mut conn = Conn::new();
        conn.recv_armed = true;
        transport.conns.insert(10, Box::new(conn));

        transport.close_fd(10);
        assert!(transport.conns[&10].closing);
        // Still alive — recv_armed is true
        assert!(transport.conns.contains_key(&10));
    }

    // ── SQ pressure test ──

    #[test]
    fn test_sq_pressure_auto_flush() {
        let mut ring = MockRing::new().with_sq_cap(4);

        // 6 connections all complete headers → 6 payload recv SQEs needed
        let mut cqes = Vec::new();
        for fd in 100..106 {
            cqes.push(Cqe {
                user_data: make_udata(TAG_RECV, fd),
                res: 4,
                flags: 0,
            });
        }
        ring.push_cqes(cqes);

        let mut transport = Transport::new(ring, 256);
        for fd in 100..106 {
            let mut conn = Conn::new();
            conn.recv_state.hdr_buf = 8u32.to_le_bytes();
            conn.recv_armed = true;
            transport.conns.insert(fd, Box::new(conn));
        }

        let (mut fds, mut ptrs, mut lens) = make_output(256);
        transport.poll(0, &mut slots(&mut fds, &mut ptrs, &mut lens));

        // Auto-flush should have fired at least once (6 SQEs > sq_cap of 4)
        assert!(transport.ring.auto_flush_count > 0);
    }

    // ── Send coalescing test ──

    #[test]
    fn test_send_coalescing() {
        #[allow(unused_mut)]
        let ring = MockRing::new();
        let mut transport = Transport::new(ring, 256);

        // Create a connection
        let conn = Conn::new();
        transport.conns.insert(10, Box::new(conn));

        // Queue 3 responses
        let data1 = b"aaa";
        let data2 = b"bbb";
        let data3 = b"ccc";
        assert_eq!(transport.send(10, data1.as_ptr(), data1.len()), 0);
        assert_eq!(transport.send(10, data2.as_ptr(), data2.len()), 0);
        assert_eq!(transport.send(10, data3.as_ptr(), data3.len()), 0);

        assert_eq!(transport.dirty_fds.len(), 3); // 3 pushes to dirty_fds
    }

    #[test]
    fn test_send_to_closing_returns_minus_one() {
        #[allow(unused_mut)]
        let ring = MockRing::new();
        let mut transport = Transport::new(ring, 256);

        let mut conn = Conn::new();
        conn.closing = true;
        transport.conns.insert(10, Box::new(conn));

        let data = b"test";
        assert_eq!(transport.send(10, data.as_ptr(), data.len()), -1);
    }

    #[test]
    fn test_send_to_unknown_fd_returns_minus_one() {
        #[allow(unused_mut)]
        let ring = MockRing::new();
        let mut transport = Transport::new(ring, 256);

        let data = b"test";
        assert_eq!(transport.send(99, data.as_ptr(), data.len()), -1);
    }

    // ── Dirty fds with stale fd ──

    #[test]
    fn test_dirty_fds_stale_fd() {
        let ring = MockRing::new();
        let mut transport = Transport::new(ring, 256);

        // fd=10 was in dirty_fds but connection was removed
        transport.dirty_fds.push(10);

        let (mut fds, mut ptrs, mut lens) = make_output(256);
        // Should not crash
        transport.poll(0, &mut slots(&mut fds, &mut ptrs, &mut lens));
    }

    // ── Payload length validation ──

    #[test]
    fn test_oversized_payload_closes_connection() {
        let mut ring = MockRing::new();

        // Header with payload_len = 0xFFFFFFFF (> MAX_PAYLOAD_LEN)
        ring.push_cqes(vec![Cqe {
            user_data: make_udata(TAG_RECV, 10),
            res: 4,
            flags: 0,
        }]);

        let mut transport = Transport::new(ring, 256);
        let mut conn = Conn::new();
        conn.recv_state.hdr_buf = 0xFFFFFFFFu32.to_le_bytes();
        conn.recv_armed = true;
        transport.conns.insert(10, Box::new(conn));

        let (mut fds, mut ptrs, mut lens) = make_output(256);
        let n = transport.poll(0, &mut slots(&mut fds, &mut ptrs, &mut lens));
        assert_eq!(n, 0);

        // Connection should be reaped (closing=true, no outstanding SQEs)
        assert!(!transport.conns.contains_key(&10));
    }

    // ── Disconnect sentinel ──

    #[test]
    fn test_disconnect_sentinel() {
        let mut ring = MockRing::new();

        ring.push_cqes(vec![Cqe {
            user_data: make_udata(TAG_RECV, 10),
            res: 4,
            flags: 0,
        }]);

        let mut transport = Transport::new(ring, 256);
        let mut conn = Conn::new();
        conn.recv_state.hdr_buf = [0, 0, 0, 0]; // payload_len = 0
        conn.recv_armed = true;
        transport.conns.insert(10, Box::new(conn));

        let (mut fds, mut ptrs, mut lens) = make_output(256);
        let n = transport.poll(0, &mut slots(&mut fds, &mut ptrs, &mut lens));
        assert_eq!(n, 0);

        // Connection reaped
        assert!(!transport.conns.contains_key(&10));
    }

    // ── Accept test ──

    #[test]
    fn test_accept_new_connection() {
        let mut ring = MockRing::new();

        // Accept CQE with new fd=20, CQE_F_MORE set (multishot continues)
        ring.push_cqes(vec![Cqe {
            user_data: make_udata(TAG_ACCEPT, 0),
            res: 20,
            flags: CQE_F_MORE,
        }]);

        let mut transport = Transport::new(ring, 256);
        transport.server_fd = 5;

        let (mut fds, mut ptrs, mut lens) = make_output(256);
        transport.poll(0, &mut slots(&mut fds, &mut ptrs, &mut lens));

        // New connection should exist
        assert!(transport.conns.contains_key(&20));
        assert!(transport.conns[&20].recv_armed);
    }

    #[test]
    fn test_accept_rearm_on_multishot_cancel() {
        let mut ring = MockRing::new();

        // Accept CQE without CQE_F_MORE → multishot cancelled, needs rearm
        ring.push_cqes(vec![Cqe {
            user_data: make_udata(TAG_ACCEPT, 0),
            res: 20,
            flags: 0, // no CQE_F_MORE
        }]);

        let mut transport = Transport::new(ring, 256);
        transport.server_fd = 5;

        let (mut fds, mut ptrs, mut lens) = make_output(256);
        transport.poll(0, &mut slots(&mut fds, &mut ptrs, &mut lens));

        // Should have prepped an accept rearm
        let accept_preps: Vec<_> = transport
            .ring
            .prepped
            .iter()
            .filter(|p| matches!(p, crate::mock::PrepRecord::Accept { .. }))
            .collect();
        assert!(!accept_preps.is_empty());
    }

    // ── Pending delivery overflow ──

    #[test]
    fn test_pending_delivery_overflow() {
        let mut ring = MockRing::new();

        // 3 connections, output slots = 2 → one overflows to pending_delivery
        // Batch 1: all 3 headers complete
        ring.push_cqes(vec![
            Cqe { user_data: make_udata(TAG_RECV, 10), res: 4, flags: 0 },
            Cqe { user_data: make_udata(TAG_RECV, 11), res: 4, flags: 0 },
            Cqe { user_data: make_udata(TAG_RECV, 12), res: 4, flags: 0 },
        ]);

        // Batch 2: all 3 payloads complete
        ring.push_cqes(vec![
            Cqe { user_data: make_udata(TAG_RECV, 10), res: 4, flags: 0 },
            Cqe { user_data: make_udata(TAG_RECV, 11), res: 4, flags: 0 },
            Cqe { user_data: make_udata(TAG_RECV, 12), res: 4, flags: 0 },
        ]);

        let mut transport = Transport::new(ring, 256);
        for fd in [10, 11, 12] {
            let mut conn = Conn::new();
            conn.recv_state.hdr_buf = 4u32.to_le_bytes();
            conn.recv_armed = true;
            transport.conns.insert(fd, Box::new(conn));
        }

        // Only 2 output slots
        let (mut fds, mut ptrs, mut lens) = make_output(2);
        let n = transport.poll(0, &mut slots(&mut fds, &mut ptrs, &mut lens));

        assert_eq!(n, 2);
        // One message should be in pending_delivery
        assert_eq!(transport.pending_delivery.len(), 1);

        // Free delivered buffers
        for i in 0..2 {
            Transport::<MockRing>::free_recv(ptrs[i]);
        }

        // Next poll should deliver the pending message
        // No new CQEs needed — Phase -1 delivers from pending_delivery
        let (mut fds2, mut ptrs2, mut lens2) = make_output(256);
        let n2 = transport.poll(0, &mut slots(&mut fds2, &mut ptrs2, &mut lens2));

        assert_eq!(n2, 1);
        assert!(transport.pending_delivery.is_empty());

        Transport::<MockRing>::free_recv(ptrs2[0]);
    }

    // ── Send CQE processing ──

    #[test]
    fn test_send_cqe_success() {
        let mut ring = MockRing::new();

        // SEND CQE succeeds
        ring.push_cqes(vec![Cqe {
            user_data: make_udata(TAG_SEND, 10),
            res: 100,
            flags: 0,
        }]);

        let mut transport = Transport::new(ring, 256);
        let mut conn = Conn::new();
        // Simulate: front has 100 bytes, inflight
        conn.send_queue.push(&[96, 0, 0, 0], &[0u8; 96]);
        conn.send_queue.prepare(); // sets inflight
        transport.conns.insert(10, Box::new(conn));

        let (mut fds, mut ptrs, mut lens) = make_output(256);
        transport.poll(0, &mut slots(&mut fds, &mut ptrs, &mut lens));

        let conn = &transport.conns[&10];
        assert!(!conn.send_queue.inflight);
        assert!(!conn.send_queue.has_pending());
    }

    #[test]
    fn test_send_cqe_error_closes_connection() {
        let mut ring = MockRing::new();

        // SEND CQE with error
        ring.push_cqes(vec![Cqe {
            user_data: make_udata(TAG_SEND, 10),
            res: -32, // EPIPE
            flags: 0,
        }]);

        let mut transport = Transport::new(ring, 256);
        let mut conn = Conn::new();
        conn.send_queue.inflight = true;
        transport.conns.insert(10, Box::new(conn));

        let (mut fds, mut ptrs, mut lens) = make_output(256);
        transport.poll(0, &mut slots(&mut fds, &mut ptrs, &mut lens));

        // Connection should be reaped
        assert!(!transport.conns.contains_key(&10));
    }
}
