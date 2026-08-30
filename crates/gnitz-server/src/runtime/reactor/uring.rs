//! The reactor's thin io_uring wrapper: SQE preparation, submit and CQE drain.
//!
//! Every `prep_*` is pure. A SQE whose memory the kernel reads after `submit`
//! returns is `unsafe` to queue, and that memory is kept alive by a park slot's
//! `carry`.

use io_uring::{opcode, squeue, types, IoUring};

/// Completion queue entry — the reactor's owned copy of an io_uring CQE.
#[derive(Clone, Copy, Debug, Default)]
pub(super) struct Cqe {
    pub(super) user_data: u64,
    pub(super) res: i32,
    pub(super) flags: u32,
}

/// io_uring CQE flag: more completions coming (multishot).
pub(super) const CQE_F_MORE: u32 = 1 << 1;

/// The reactor's io_uring submission/completion interface.
///
/// All `prep_*` methods are **infallible** — if the SQ is full, the
/// implementation auto-flushes pending SQEs to the kernel.
pub(super) struct IoUringRing {
    ring: IoUring,
}

impl IoUringRing {
    pub(super) fn new(entries: u32) -> std::io::Result<Self> {
        Ok(IoUringRing {
            ring: IoUring::new(entries)?,
        })
    }

    /// Queue one SQE, flushing the pending batch first if the ring is full.
    /// Infallible: the only way `push` can fail is a full SQ, which the flush
    /// clears. A full SQ therefore never reaches a caller as backpressure — it
    /// costs one extra `submit()` and is invisible above this line.
    #[inline]
    fn push(&mut self, entry: squeue::Entry) {
        if self.ring.submission().is_full() {
            let _ = self.ring.submit();
        }
        // SAFETY: every caller keeps the memory an SQE points at alive until
        // its CQE is drained — see the park slots' `carry`.
        unsafe {
            self.ring.submission().push(&entry).expect("SQ full after flush");
        }
    }

    pub(super) fn prep_recv(&mut self, fd: i32, buf: *mut u8, len: u32, user_data: u64) {
        self.push(opcode::Recv::new(types::Fd(fd), buf, len).build().user_data(user_data));
    }

    pub(super) fn prep_send(&mut self, fd: i32, buf: *const u8, len: u32, user_data: u64) {
        self.push(opcode::Send::new(types::Fd(fd), buf, len).build().user_data(user_data));
    }

    pub(super) fn prep_accept(&mut self, fd: i32, user_data: u64) {
        self.push(opcode::AcceptMulti::new(types::Fd(fd)).build().user_data(user_data));
    }

    /// Submit an `Fsync` op with the `DATASYNC` flag on `fd`. One-shot: a
    /// single CQE carrying the fdatasync return code.
    pub(super) fn prep_fsync(&mut self, fd: i32, user_data: u64) {
        self.push(
            opcode::Fsync::new(types::Fd(fd))
                .flags(types::FsyncFlags::DATASYNC)
                .build()
                .user_data(user_data),
        );
    }

    /// Submit a relative `Timeout` op against `ts`. One-shot: a single CQE
    /// (`res = -ETIME` on natural expiry).
    ///
    /// # Safety
    /// `ts` must live at this address until the CQE is drained.
    pub(super) unsafe fn prep_timeout(&mut self, ts: &types::Timespec, user_data: u64) {
        self.push(opcode::Timeout::new(ts).build().user_data(user_data));
    }

    /// Submit a `FUTEX_WAITV` op over `nr` pointer-stable `FutexWaitV` entries.
    /// One-shot: a single CQE on wake or cancellation. Requires Linux 6.7+,
    /// probed at `Reactor::new`.
    ///
    /// # Safety
    /// The caller promises the array outlives the CQE and that each entry's
    /// `uaddr` points to a live atomic u32 shared with the producer.
    pub(super) unsafe fn prep_futex_waitv(&mut self, futexv: *const types::FutexWaitV, nr: u32, user_data: u64) {
        self.push(opcode::FutexWaitV::new(futexv, nr).build().user_data(user_data));
    }

    /// Submit an `AsyncCancel` op targeting an in-flight SQE by its
    /// `target_user_data`. The cancelled SQE's CQE arrives first (with
    /// `res = -ECANCELED`), the AsyncCancel's own CQE after it.
    pub(super) fn prep_async_cancel(&mut self, target_user_data: u64, user_data: u64) {
        self.push(opcode::AsyncCancel::new(target_user_data).build().user_data(user_data));
    }

    /// Eagerly flush pending SQEs to the kernel (no wait). On failure the
    /// SQEs stay queued and go out with the next tick's submit — log and
    /// continue; `what` names the operation for the log line.
    pub(super) fn flush_sqes(&mut self, what: &str) {
        if let Err(e) = self.submit_and_wait_timeout(0, 0) {
            gnitz_error!(
                "reactor: {} SQE flush failed (errno={}); SQE queued — will submit on next tick",
                what,
                e,
            );
        }
    }

    /// Submit pending SQEs and optionally wait for completions.
    ///
    /// - `min_complete > 0, timeout_ms > 0`: block until ≥min_complete CQEs
    ///   or timeout expires.
    /// - `min_complete > 0, timeout_ms = -1`: block indefinitely until
    ///   ≥min_complete CQEs arrive (no timeout bound).
    /// - `min_complete = 0, timeout_ms = 0`: submit only, return immediately.
    ///   When no SQEs are pending, this is a no-op (0 syscalls).
    pub(super) fn submit_and_wait_timeout(&mut self, min_complete: u32, timeout_ms: i32) -> Result<i32, i32> {
        let pending = self.ring.submission().len();

        if min_complete == 0 && pending == 0 {
            return Ok(0); // no-op fast path
        }

        if min_complete == 0 || timeout_ms == 0 {
            // Submit only, no wait
            match self.ring.submit() {
                Ok(n) => Ok(n as i32),
                Err(e) => Err(e.raw_os_error().unwrap_or(-1)),
            }
        } else if timeout_ms < 0 {
            // Block indefinitely until min_complete CQEs arrive
            match self.ring.submitter().submit_and_wait(min_complete as usize) {
                Ok(n) => Ok(n as i32),
                Err(ref e) if e.raw_os_error() == Some(libc::EINTR) => Ok(0),
                Err(e) => Err(e.raw_os_error().unwrap_or(-1)),
            }
        } else {
            // Submit + wait with timeout via EXT_ARG
            let ts = types::Timespec::new()
                .sec((timeout_ms / 1000) as u64)
                .nsec(((timeout_ms % 1000) as u32) * 1_000_000);
            let args = types::SubmitArgs::new().timespec(&ts);
            match self.ring.submitter().submit_with_args(min_complete as usize, &args) {
                Ok(n) => Ok(n as i32),
                Err(ref e) if e.raw_os_error() == Some(libc::ETIME) => Ok(0),
                Err(ref e) if e.raw_os_error() == Some(libc::EINTR) => Ok(0),
                Err(e) => Err(e.raw_os_error().unwrap_or(-1)),
            }
        }
    }

    /// Drain completed CQEs into `out`. Returns number of CQEs written.
    /// This reads from the memory-mapped completion ring — no syscall.
    pub(super) fn drain_cqes(&mut self, out: &mut [Cqe]) -> usize {
        let mut count = 0;
        let cq = self.ring.completion();
        for cqe in cq {
            if count >= out.len() {
                break;
            }
            out[count] = Cqe {
                user_data: cqe.user_data(),
                res: cqe.result(),
                flags: cqe.flags(),
            };
            count += 1;
        }
        count
    }
}
