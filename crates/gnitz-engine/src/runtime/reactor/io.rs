//! Client-ring bookkeeping: per-connection recv decoder + coalesced
//! send queue. The reactor's `accept` / `recv` / `send` methods are the
//! only callers. Re-homed from `gnitz-transport::{conn,recv,send}` so
//! the reactor's single io_uring owns the client ring in Stage 4.

pub(super) const MAX_PAYLOAD_LEN: usize = gnitz_wire::MAX_FRAME_PAYLOAD_SERVER;

pub(super) enum RecvPhase {
    Header { pos: usize },
    Payload { buf: *mut u8, len: usize, pos: usize },
}

pub(super) enum RecvAdvance {
    NeedMore,
    HeaderDone,
    MessageDone,
    Disconnect,
}

pub(super) struct RecvState {
    pub(super) hdr_buf: [u8; 4],
    pub(super) phase: RecvPhase,
}

impl RecvState {
    pub(super) fn new() -> Self {
        RecvState { hdr_buf: [0; 4], phase: RecvPhase::Header { pos: 0 } }
    }

    pub(super) fn advance(&mut self, bytes_received: usize) -> RecvAdvance {
        match &mut self.phase {
            RecvPhase::Header { pos } => {
                *pos += bytes_received;
                if *pos < 4 { return RecvAdvance::NeedMore; }
                let payload_len = u32::from_le_bytes(self.hdr_buf) as usize;
                if payload_len == 0 { return RecvAdvance::Disconnect; }
                RecvAdvance::HeaderDone
            }
            RecvPhase::Payload { pos, len, .. } => {
                *pos += bytes_received;
                if *pos < *len { return RecvAdvance::NeedMore; }
                RecvAdvance::MessageDone
            }
        }
    }

    pub(super) fn remaining(&mut self) -> (*mut u8, u32) {
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

    pub(super) fn payload_len(&self) -> usize {
        u32::from_le_bytes(self.hdr_buf) as usize
    }

    pub(super) fn start_payload(&mut self, buf: *mut u8, len: usize) {
        self.phase = RecvPhase::Payload { buf, len, pos: 0 };
    }

    pub(super) fn take_message(&mut self) -> (*mut u8, usize) {
        match &self.phase {
            RecvPhase::Payload { buf, len, .. } => {
                let result = (*buf, *len);
                self.phase = RecvPhase::Header { pos: 0 };
                result
            }
            _ => unreachable!("take_message called outside Payload phase"),
        }
    }

    pub(super) fn hdr_buf_ptr(&mut self) -> *mut u8 {
        self.hdr_buf.as_mut_ptr()
    }

    pub(super) fn free_payload(&mut self) {
        if let RecvPhase::Payload { buf, .. } = self.phase {
            if !buf.is_null() {
                unsafe { libc::free(buf as *mut libc::c_void); }
            }
            self.phase = RecvPhase::Header { pos: 0 };
        }
    }
}

pub(super) struct Conn {
    pub(super) recv_state: RecvState,
    pub(super) recv_armed: bool,
    pub(super) closing: bool,
    pub(super) send_inflight: usize,
}

impl Conn {
    pub(super) fn new() -> Self {
        Conn {
            recv_state: RecvState::new(),
            recv_armed: false,
            closing: false,
            send_inflight: 0,
        }
    }

    pub(super) fn has_outstanding(&self) -> bool {
        self.recv_armed || self.send_inflight > 0
    }
}

impl Drop for Conn {
    fn drop(&mut self) { self.recv_state.free_payload(); }
}
