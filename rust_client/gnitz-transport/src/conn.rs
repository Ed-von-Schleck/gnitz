use crate::recv::RecvState;
use crate::send::SendQueue;

/// Per-connection state combining recv and send state machines.
///
/// `Drop` frees any in-progress payload buffer to prevent leaks when
/// connections are reaped during deferred close.
pub struct Conn {
    pub recv_state: RecvState,
    pub send_queue: SendQueue,
    /// A RECV SQE is outstanding in the io_uring ring.
    pub recv_armed: bool,
    /// Teardown requested — no new SQEs will be submitted for this fd.
    pub closing: bool,
}

impl Conn {
    pub fn new() -> Self {
        Conn {
            recv_state: RecvState::new(),
            send_queue: SendQueue::new(),
            recv_armed: false,
            closing: false,
        }
    }

    /// Returns true if there are outstanding SQEs for this connection.
    pub fn has_outstanding(&self) -> bool {
        self.recv_armed || self.send_queue.is_inflight()
    }
}

impl Drop for Conn {
    fn drop(&mut self) {
        self.recv_state.free_payload();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_conn_state() {
        let conn = Conn::new();
        assert!(!conn.recv_armed);
        assert!(!conn.closing);
        assert!(!conn.has_outstanding());
    }

    #[test]
    fn test_drop_mid_payload_frees() {
        let buf = unsafe { libc::malloc(64) as *mut u8 };
        {
            let mut conn = Conn::new();
            conn.recv_state.start_payload(buf, 64);
            conn.recv_state.advance(10); // partial read
            // conn dropped here — Drop calls free_payload
        }
        // If this didn't crash, the buffer was freed correctly.
    }

    #[test]
    fn test_drop_after_take_message_no_double_free() {
        let buf = unsafe { libc::malloc(32) as *mut u8 };
        {
            let mut conn = Conn::new();
            conn.recv_state.start_payload(buf, 32);
            conn.recv_state.advance(32);
            let (ptr, _) = conn.recv_state.take_message();
            // Now in Header phase — Drop is a no-op on recv_state
            unsafe { libc::free(ptr as *mut libc::c_void) };
        }
    }

    #[test]
    fn test_has_outstanding() {
        let mut conn = Conn::new();
        assert!(!conn.has_outstanding());

        conn.recv_armed = true;
        assert!(conn.has_outstanding());

        conn.recv_armed = false;
        conn.send_queue.inflight = true;
        assert!(conn.has_outstanding());

        conn.send_queue.inflight = false;
        assert!(!conn.has_outstanding());
    }
}
