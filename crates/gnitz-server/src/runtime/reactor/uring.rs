//! The reactor's thin io_uring wrapper: SQE preparation, submit and CQE drain.
//!
//! Every `prep_*` is pure. Memory the kernel reads after submit must be kept
//! alive by the caller until the SQE's CQE.

use std::time::Duration;

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
        Ok(IoUringRing { ring: IoUring::new(entries)? })
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
        // its CQE is drained — see the module doc.
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

    /// One-shot `FUTEX_WAITV` over `nr` entries.
    /// # Safety
    /// Each entry's `uaddr` must point at a live atomic shared with the producer.
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
        if let Err(e) = self.submit() {
            gnitz_error!(
                "reactor: {} SQE flush failed (errno={}); SQE queued — will submit on next tick",
                what,
                e,
            );
        }
    }

    /// Submit pending SQEs without waiting. No syscall when the SQ is empty.
    pub(super) fn submit(&mut self) -> Result<i32, i32> {
        if self.ring.submission().is_empty() {
            return Ok(0);
        }
        self.ring
            .submit()
            .map(|n| n as i32)
            .map_err(|e| e.raw_os_error().unwrap_or(-1))
    }

    /// Submit pending SQEs and wait for one CQE, for at most `timeout` when one
    /// is given. An expired timeout or a signal returns `Ok(0)`.
    pub(super) fn wait(&mut self, timeout: Option<Duration>) -> Result<i32, i32> {
        let rc = match timeout {
            None => self.ring.submitter().submit_and_wait(1),
            Some(d) => {
                let ts = types::Timespec::from(d);
                let args = types::SubmitArgs::new().timespec(&ts);
                self.ring.submitter().submit_with_args(1, &args)
            }
        };
        match rc {
            Ok(n) => Ok(n as i32),
            Err(e) if matches!(e.raw_os_error(), Some(libc::ETIME | libc::EINTR)) => Ok(0),
            Err(e) => Err(e.raw_os_error().unwrap_or(-1)),
        }
    }

    /// Drain up to `out.len()` completed CQEs into `out`, returning how many
    /// were written. Reads the memory-mapped completion ring — no syscall.
    ///
    /// `take` bounds the iterator rather than breaking inside the loop:
    /// `CompletionQueue::next` pops the entry and advances the ring head, so a
    /// break *after* it has yielded discards that completion permanently.
    pub(super) fn drain_cqes(&mut self, out: &mut [Cqe]) -> usize {
        let mut count = 0;
        for cqe in self.ring.completion().take(out.len()) {
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
