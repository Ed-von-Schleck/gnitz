/// Completion queue entry — mirrors io_uring CQE layout.
#[derive(Clone, Copy, Debug, Default)]
#[repr(C)]
pub struct Cqe {
    pub user_data: u64,
    pub res: i32,
    pub flags: u32,
}

/// io_uring CQE flag: more completions coming (multishot).
pub const CQE_F_MORE: u32 = 1 << 1;

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

    /// Submit an `Fsync` op with the `DATASYNC` flag on `fd`.
    /// One-shot: a single CQE is delivered when the kernel's fdatasync
    /// completes; the CQE's `res` is the fdatasync return code.
    fn prep_fsync(&mut self, fd: i32, user_data: u64);

    /// Submit a relative `Timeout` op that fires after `timeout_ns` nanoseconds.
    /// One-shot: a single CQE is delivered (with `res = -ETIME` on natural
    /// expiry) when the timer expires.
    fn prep_timeout(&mut self, timeout_ns: u64, user_data: u64);

    /// Submit pending SQEs and optionally wait for completions.
    ///
    /// - `min_complete > 0, timeout_ms > 0`: block until ≥min_complete CQEs
    ///   or timeout expires.
    /// - `min_complete > 0, timeout_ms = -1`: block indefinitely until
    ///   ≥min_complete CQEs arrive (no timeout bound).
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
