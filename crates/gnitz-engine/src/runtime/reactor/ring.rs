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

    /// Submit an `Fsync` op with the `DATASYNC` flag on `fd`.
    /// One-shot: a single CQE is delivered when the kernel's fdatasync
    /// completes; the CQE's `res` is the fdatasync return code.
    fn prep_fsync(&mut self, fd: i32, user_data: u64);

    /// Submit a relative `Timeout` op that fires after `timeout_ns` nanoseconds.
    /// One-shot: a single CQE is delivered (with `res = -ETIME` on natural
    /// expiry) when the timer expires.
    fn prep_timeout(&mut self, timeout_ns: u64, user_data: u64);

    /// Submit a `FUTEX_WAITV` op over `nr` pointer-stable `FutexWaitV`
    /// entries. The implementation must own the storage for the array
    /// so that the kernel's asynchronous dereference stays valid until
    /// the matching CQE is drained.
    ///
    /// One-shot: a single CQE is delivered on wake / cancellation.
    /// Requires Linux 6.7+; probed at `Reactor::new`.
    ///
    /// # Safety
    /// The caller promises each `FutexWaitV` entry's `uaddr` points to
    /// a live atomic u32 shared with the producer.
    unsafe fn prep_futex_waitv(
        &mut self,
        futexv: *const io_uring::types::FutexWaitV,
        nr: u32,
        user_data: u64,
    );

    /// Submit an `AsyncCancel` op targeting an in-flight SQE identified
    /// by its `target_user_data`. The CQE for the cancelled SQE
    /// arrives first (with `res = -ECANCELED`) and the AsyncCancel's
    /// own CQE follows.
    fn prep_async_cancel(&mut self, target_user_data: u64, user_data: u64);

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
