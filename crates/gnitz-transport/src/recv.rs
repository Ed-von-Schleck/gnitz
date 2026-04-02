/// Per-connection receive state machine.
///
/// Mirrors the `_CONN_WAITING_HEADER` / `_CONN_WAITING_PAYLOAD` states
/// in `executor.py:474-528`. Each io_uring RECV SQE requests exactly
/// the bytes needed — no streaming buffer, no compaction, no feed loop.

/// Current phase of the receive state machine.
pub enum RecvPhase {
    Header { pos: usize },
    Payload { buf: *mut u8, len: usize, pos: usize },
}

/// Result of advancing the state machine after a recv completion.
pub enum RecvAdvance {
    /// Partial read — rearm recv for remaining bytes.
    NeedMore,
    /// 4-byte header decoded — allocate payload buffer, start payload recv.
    HeaderDone,
    /// Payload complete — message ready for delivery.
    MessageDone,
    /// `payload_len == 0` sentinel — client disconnecting.
    Disconnect,
}

pub struct RecvState {
    pub hdr_buf: [u8; 4],
    pub phase: RecvPhase,
}

impl RecvState {
    pub fn new() -> Self {
        RecvState {
            hdr_buf: [0; 4],
            phase: RecvPhase::Header { pos: 0 },
        }
    }

    /// Advance state after receiving `bytes_received` bytes.
    pub fn advance(&mut self, bytes_received: usize) -> RecvAdvance {
        match &mut self.phase {
            RecvPhase::Header { pos } => {
                *pos += bytes_received;
                if *pos < 4 {
                    return RecvAdvance::NeedMore;
                }
                let payload_len = u32::from_le_bytes(self.hdr_buf) as usize;
                if payload_len == 0 {
                    return RecvAdvance::Disconnect;
                }
                RecvAdvance::HeaderDone
            }
            RecvPhase::Payload { pos, len, .. } => {
                *pos += bytes_received;
                if *pos < *len {
                    return RecvAdvance::NeedMore;
                }
                RecvAdvance::MessageDone
            }
        }
    }

    /// Pointer + remaining length for the next RECV SQE.
    pub fn remaining(&mut self) -> (*mut u8, u32) {
        match &mut self.phase {
            RecvPhase::Header { pos } => {
                let ptr = unsafe { self.hdr_buf.as_mut_ptr().add(*pos) };
                (ptr, (4 - *pos) as u32)
            }
            RecvPhase::Payload { buf, len, pos } => {
                let ptr = unsafe { (*buf).add(*pos) };
                (ptr, (*len - *pos) as u32)
            }
        }
    }

    /// Decoded payload length from the 4-byte header.
    pub fn payload_len(&self) -> usize {
        u32::from_le_bytes(self.hdr_buf) as usize
    }

    /// Transition from HeaderDone to Payload phase.
    pub fn start_payload(&mut self, buf: *mut u8, len: usize) {
        self.phase = RecvPhase::Payload { buf, len, pos: 0 };
    }

    /// Take ownership of the completed payload buffer.
    /// Transitions to Header phase — caller now owns the buffer.
    pub fn take_message(&mut self) -> (*mut u8, usize) {
        match &self.phase {
            RecvPhase::Payload { buf, len, .. } => {
                let result = (*buf, *len);
                self.phase = RecvPhase::Header { pos: 0 };
                result
            }
            _ => unreachable!("take_message called outside Payload phase"),
        }
    }

    /// Pointer to the header buffer (for arming recv SQEs).
    pub fn hdr_buf_ptr(&mut self) -> *mut u8 {
        self.hdr_buf.as_mut_ptr()
    }

    /// Free the payload buffer if in Payload phase.
    /// Handles both partial payloads and complete-but-untaken payloads.
    pub fn free_payload(&mut self) {
        if let RecvPhase::Payload { buf, .. } = self.phase {
            if !buf.is_null() {
                unsafe {
                    libc::free(buf as *mut libc::c_void);
                }
            }
            self.phase = RecvPhase::Header { pos: 0 };
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_header_partial_then_complete() {
        let mut rs = RecvState::new();

        // Simulate receiving 1 byte at a time
        rs.hdr_buf[0] = 0x0A;
        assert!(matches!(rs.advance(1), RecvAdvance::NeedMore));
        rs.hdr_buf[1] = 0x00;
        assert!(matches!(rs.advance(1), RecvAdvance::NeedMore));
        rs.hdr_buf[2] = 0x00;
        assert!(matches!(rs.advance(1), RecvAdvance::NeedMore));
        rs.hdr_buf[3] = 0x00;
        assert!(matches!(rs.advance(1), RecvAdvance::HeaderDone));

        assert_eq!(rs.payload_len(), 10);
    }

    #[test]
    fn test_header_complete_at_once() {
        let mut rs = RecvState::new();
        rs.hdr_buf = [0x04, 0x00, 0x00, 0x00];
        assert!(matches!(rs.advance(4), RecvAdvance::HeaderDone));
        assert_eq!(rs.payload_len(), 4);
    }

    #[test]
    fn test_disconnect_sentinel() {
        let mut rs = RecvState::new();
        rs.hdr_buf = [0x00, 0x00, 0x00, 0x00];
        assert!(matches!(rs.advance(4), RecvAdvance::Disconnect));
    }

    #[test]
    fn test_payload_partial_then_complete() {
        let mut rs = RecvState::new();
        let buf = unsafe { libc::malloc(10) as *mut u8 };
        rs.start_payload(buf, 10);

        assert!(matches!(rs.advance(3), RecvAdvance::NeedMore));
        assert!(matches!(rs.advance(5), RecvAdvance::NeedMore));
        assert!(matches!(rs.advance(2), RecvAdvance::MessageDone));

        let (ptr, len) = rs.take_message();
        assert_eq!(ptr, buf);
        assert_eq!(len, 10);
        unsafe { libc::free(ptr as *mut libc::c_void) };
    }

    #[test]
    fn test_remaining_header() {
        let mut rs = RecvState::new();
        let (_, len) = rs.remaining();
        assert_eq!(len, 4);

        // After 2 bytes received
        rs.hdr_buf[0] = 0x01;
        rs.hdr_buf[1] = 0x02;
        rs.advance(2);
        let (_, len) = rs.remaining();
        assert_eq!(len, 2);
    }

    #[test]
    fn test_remaining_payload() {
        let mut rs = RecvState::new();
        let buf = unsafe { libc::malloc(100) as *mut u8 };
        rs.start_payload(buf, 100);

        let (ptr, len) = rs.remaining();
        assert_eq!(ptr, buf);
        assert_eq!(len, 100);

        rs.advance(40);
        let (ptr, len) = rs.remaining();
        assert_eq!(ptr, unsafe { buf.add(40) });
        assert_eq!(len, 60);

        rs.advance(60);
        let (ptr, len) = rs.take_message();
        unsafe { libc::free(ptr as *mut libc::c_void) };
        let _ = len;
    }

    #[test]
    fn test_free_payload_in_payload_phase() {
        let mut rs = RecvState::new();
        let buf = unsafe { libc::malloc(64) as *mut u8 };
        rs.start_payload(buf, 64);
        rs.advance(10); // partial
        rs.free_payload();
        // Should be back in Header phase
        assert!(matches!(rs.phase, RecvPhase::Header { pos: 0 }));
    }

    #[test]
    fn test_free_payload_in_header_phase_is_noop() {
        let mut rs = RecvState::new();
        rs.free_payload(); // should not crash
        assert!(matches!(rs.phase, RecvPhase::Header { pos: 0 }));
    }

    #[test]
    fn test_take_message_then_free_no_double_free() {
        let mut rs = RecvState::new();
        let buf = unsafe { libc::malloc(32) as *mut u8 };
        rs.start_payload(buf, 32);
        rs.advance(32);

        let (ptr, _) = rs.take_message();
        // Now in Header phase — free_payload is a no-op
        rs.free_payload();
        unsafe { libc::free(ptr as *mut libc::c_void) };
    }
}
