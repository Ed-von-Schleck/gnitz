/// Completion queue entry — mirrors io_uring CQE layout.
#[derive(Clone, Copy, Debug, Default)]
#[repr(C)]
pub struct Cqe {
    pub user_data: u64,
    pub res: i32,
    pub flags: u32,
}

// Tag encoding: high 8 bits of user_data
pub const TAG_ACCEPT: u64 = 0x01 << 56;
pub const TAG_RECV: u64 = 0x02 << 56;
pub const TAG_SEND: u64 = 0x03 << 56;

const TAG_MASK: u64 = 0xFF << 56;
const FD_MASK: u64 = 0x00FF_FFFF_FFFF_FFFF;

/// io_uring CQE flag: more completions coming (multishot).
pub const CQE_F_MORE: u32 = 1 << 1;

#[inline]
pub const fn make_udata(tag: u64, fd: i32) -> u64 {
    tag | (fd as u32 as u64)
}

#[inline]
pub fn cqe_tag(udata: u64) -> u64 {
    udata & TAG_MASK
}

#[inline]
pub fn cqe_fd(udata: u64) -> i32 {
    (udata & FD_MASK) as i32
}

/// Abstraction over the io_uring submission/completion interface.
///
/// All `prep_*` methods are **infallible** — if the SQ is full, the
/// implementation auto-flushes pending SQEs to the kernel.
pub trait Ring {
    fn prep_recv(&mut self, fd: i32, buf: *mut u8, len: u32, user_data: u64);
    fn prep_send(&mut self, fd: i32, buf: *const u8, len: u32, user_data: u64);
    fn prep_accept(&mut self, fd: i32, user_data: u64);

    /// Submit a `PollAdd` op for `fd` with `mask` (POLLIN/POLLOUT bits).
    /// One-shot: a single CQE is delivered when the fd becomes ready,
    /// then the op is consumed. Re-arm by submitting another `prep_poll_add`.
    fn prep_poll_add(&mut self, fd: i32, mask: u32, user_data: u64);

    /// Submit a relative `Timeout` op that fires after `timeout_ns` nanoseconds.
    /// One-shot: a single CQE is delivered (with `res = -ETIME` on natural
    /// expiry) when the timer expires.
    fn prep_timeout(&mut self, timeout_ns: u64, user_data: u64);

    /// Submit pending SQEs and optionally wait for completions.
    ///
    /// - `min_complete > 0, timeout_ms > 0`: block until ≥min_complete CQEs
    ///   or timeout expires.
    /// - `min_complete = 0, timeout_ms = 0`: submit only, return immediately.
    ///   When no SQEs are pending, this is a no-op (0 syscalls).
    fn submit_and_wait_timeout(
        &mut self,
        min_complete: u32,
        timeout_ms: i32,
    ) -> Result<i32, i32>;

    /// Drain completed CQEs into `out`. Returns number of CQEs written.
    /// This reads from the memory-mapped completion ring — no syscall.
    fn drain_cqes(&mut self, out: &mut [Cqe]) -> usize;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_udata_roundtrip() {
        let fd = 42;
        let udata = make_udata(TAG_RECV, fd);
        assert_eq!(cqe_tag(udata), TAG_RECV);
        assert_eq!(cqe_fd(udata), fd);
    }

    #[test]
    fn test_udata_large_fd() {
        let fd = 0x00FF_FFFF; // max fd that fits in 24 bits
        let udata = make_udata(TAG_SEND, fd);
        assert_eq!(cqe_tag(udata), TAG_SEND);
        assert_eq!(cqe_fd(udata), fd);
    }

    #[test]
    fn test_tags_distinct() {
        assert_ne!(TAG_ACCEPT, TAG_RECV);
        assert_ne!(TAG_RECV, TAG_SEND);
        assert_ne!(TAG_ACCEPT, TAG_SEND);
    }
}
