//! Client-ring bookkeeping: per-connection recv decoder + coalesced
//! send queue. The reactor's `accept` / `recv` / `send` methods are the
//! only callers. Re-homed from `gnitz-transport::{conn,recv,send}` so
//! the reactor's single io_uring owns the client ring in Stage 4.

use std::cell::Cell;
use std::rc::Rc;

/// Pre-handshake limit applied to every newly registered connection.
/// Equals the HELLO payload size in bytes; any first frame larger than
/// this is rejected before allocation. The handshake elevates the
/// limit to the negotiated value via `Reactor::set_max_payload_len`.
pub(super) const HELLO_PRE_HANDSHAKE_LEN: usize = gnitz_wire::HELLO_PAYLOAD_LEN as usize;

/// Lower/upper bounds on the global inbound-memory cap (see
/// `resolve_inbound_cap`). The floor guarantees even a tiny memory budget
/// admits at least one max-size frame; the ceiling caps the default on a
/// large box where a quarter of RAM would be an excessive inbound reserve.
pub(crate) const INBOUND_CAP_FLOOR: usize = 64 << 20; // 64 MiB
pub(crate) const INBOUND_CAP_CEIL: usize = 4usize << 30; // 4 GiB

/// Accounted memory weight of one inbound payload buffer of `len` bytes.
#[inline]
pub(crate) fn frame_weight(len: usize) -> usize {
    // Floor: a 1-byte payload really costs ~48 B (malloc's 32 B min chunk +
    // 16 B RecvBuf in the VecDeque); without the floor a tiny-frame flood
    // bypasses the byte cap ~48x. Keeps accounted bytes ≥ real RSS.
    len.max(64)
}

/// One complete inbound frame payload, owned. Malloc'd by `handle_recv_cqe`
/// and freed on drop on every exit path, including task cancellation at an
/// `.await` point. `ptr` is never null and `len` never 0: a failed malloc or
/// a zero-length frame closes the connection before a message is queued.
///
/// Its `frame_weight` is charged to the reactor's global inbound-byte counter
/// at construction and refunded on drop, so the OOM guard's accounting is tied
/// to buffer lifetime — every release path (consume, connection reap,
/// task-cancel) reconciles with no manual decrement to forget.
pub struct RecvBuf {
    pub(super) ptr: *mut u8,
    pub(super) len: usize,
    /// Shared handle to the reactor's `total_inbound_bytes`. `Rc` rather than a
    /// raw pointer so the counter provably outlives every buffer regardless of
    /// reactor-teardown field-drop order.
    counter: Rc<Cell<usize>>,
}

impl RecvBuf {
    /// Take ownership of a freshly-malloc'd `len`-byte payload buffer and charge
    /// its `frame_weight` to `counter`. The matching refund is in `Drop`.
    pub(super) fn new(ptr: *mut u8, len: usize, counter: Rc<Cell<usize>>) -> Self {
        counter.set(counter.get() + frame_weight(len));
        RecvBuf { ptr, len, counter }
    }

    pub fn as_slice(&self) -> &[u8] {
        // SAFETY: ptr/len describe a completed reactor recv (non-null by the
        // struct invariant); the buffer is exclusively owned until drop.
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }
}

impl Drop for RecvBuf {
    fn drop(&mut self) {
        self.counter
            .set(self.counter.get().saturating_sub(frame_weight(self.len)));
        unsafe { libc::free(self.ptr as *mut libc::c_void) }
    }
}

pub(super) enum RecvPhase {
    Header { pos: usize },
    Payload { buf: RecvBuf, pos: usize },
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
        RecvState {
            hdr_buf: [0; 4],
            phase: RecvPhase::Header { pos: 0 },
        }
    }

    pub(super) fn advance(&mut self, bytes_received: usize) -> RecvAdvance {
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
            RecvPhase::Payload { buf, pos } => {
                *pos += bytes_received;
                if *pos < buf.len {
                    return RecvAdvance::NeedMore;
                }
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
            RecvPhase::Payload { buf, pos } => {
                let ptr = unsafe { buf.ptr.add(*pos) };
                (ptr, (buf.len - *pos) as u32)
            }
        }
    }

    pub(super) fn payload_len(&self) -> usize {
        u32::from_le_bytes(self.hdr_buf) as usize
    }

    pub(super) fn start_payload(&mut self, buf: RecvBuf) {
        self.phase = RecvPhase::Payload { buf, pos: 0 };
    }

    pub(super) fn take_message(&mut self) -> RecvBuf {
        match std::mem::replace(&mut self.phase, RecvPhase::Header { pos: 0 }) {
            RecvPhase::Payload { buf, .. } => buf,
            _ => unreachable!("take_message called outside Payload phase"),
        }
    }

    pub(super) fn hdr_buf_ptr(&mut self) -> *mut u8 {
        self.hdr_buf.as_mut_ptr()
    }

    pub(super) fn free_payload(&mut self) {
        // Resetting to Header drops any in-flight `RecvBuf`, whose `Drop` frees
        // the allocation and refunds its charge to the global counter.
        if matches!(self.phase, RecvPhase::Payload { .. }) {
            self.phase = RecvPhase::Header { pos: 0 };
        }
    }
}

pub(super) struct Conn {
    pub(super) recv_state: RecvState,
    pub(super) recv_armed: bool,
    pub(super) closing: bool,
    pub(super) send_inflight: usize,
    /// Per-connection ceiling on incoming frame payload size. Initialised
    /// to `HELLO_PRE_HANDSHAKE_LEN` (HELLO payload size) so a peer sending
    /// garbage as its first frame cannot drive an allocation larger than
    /// the HELLO message. Elevated to the negotiated transport limit by
    /// `Reactor::set_max_payload_len` after HELLO validation.
    pub(super) max_payload_len: usize,
}

impl Conn {
    pub(super) fn new() -> Self {
        Conn {
            recv_state: RecvState::new(),
            recv_armed: false,
            closing: false,
            send_inflight: 0,
            max_payload_len: HELLO_PRE_HANDSHAKE_LEN,
        }
    }

    pub(super) fn has_outstanding(&self) -> bool {
        self.recv_armed || self.send_inflight > 0
    }
}

impl Drop for Conn {
    fn drop(&mut self) {
        self.recv_state.free_payload();
    }
}
