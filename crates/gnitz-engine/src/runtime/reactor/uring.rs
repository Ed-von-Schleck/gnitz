use std::collections::HashMap;

use io_uring::{opcode, types, IoUring};

/// Completion queue entry — the reactor's owned copy of an io_uring CQE.
#[derive(Clone, Copy, Debug, Default)]
pub struct Cqe {
    pub user_data: u64,
    pub res: i32,
    pub flags: u32,
}

/// io_uring CQE flag: more completions coming (multishot).
pub const CQE_F_MORE: u32 = 1 << 1;

/// The reactor's io_uring submission/completion interface.
///
/// All `prep_*` methods are **infallible** — if the SQ is full, the
/// implementation auto-flushes pending SQEs to the kernel.
pub struct IoUringRing {
    ring: IoUring,
    /// Owns Timespec values for outstanding `prep_timeout` SQEs. The
    /// kernel reads these pointers when the timer fires (which can be
    /// long after `submit()` returns), so the storage must outlive the
    /// CQE. Keyed by the SQE's `user_data`; released (into `spec_pool`)
    /// by `release_timer_spec` when the KIND_TIMEOUT CQE is dispatched.
    timer_specs: HashMap<u64, Box<types::Timespec>>,
    /// Recycled Timespec boxes: `prep_timeout` pops instead of
    /// allocating — timers are armed per client egress frame, so the
    /// alloc/free per frame is worth avoiding. Boxed (not inline) because
    /// the kernel dereferences the stable heap address until the CQE.
    #[allow(clippy::vec_box)]
    spec_pool: Vec<Box<types::Timespec>>,
}

impl IoUringRing {
    pub fn new(entries: u32) -> std::io::Result<Self> {
        let ring = IoUring::new(entries)?;
        Ok(IoUringRing {
            ring,
            timer_specs: HashMap::new(),
            spec_pool: Vec::new(),
        })
    }

    /// Ensure at least 1 SQ slot is available. If the SQ is full,
    /// submit all pending SQEs to the kernel to free every slot.
    #[inline]
    fn ensure_sq_room(&mut self) {
        if self.ring.submission().is_full() {
            let _ = self.ring.submit();
        }
    }

    pub fn prep_recv(&mut self, fd: i32, buf: *mut u8, len: u32, user_data: u64) {
        self.ensure_sq_room();
        let entry = opcode::Recv::new(types::Fd(fd), buf, len).build().user_data(user_data);
        unsafe {
            self.ring.submission().push(&entry).unwrap();
        }
    }

    pub fn prep_send(&mut self, fd: i32, buf: *const u8, len: u32, user_data: u64) {
        self.ensure_sq_room();
        let entry = opcode::Send::new(types::Fd(fd), buf, len).build().user_data(user_data);
        unsafe {
            self.ring.submission().push(&entry).unwrap();
        }
    }

    pub fn prep_accept(&mut self, fd: i32, user_data: u64) {
        self.ensure_sq_room();
        let entry = opcode::AcceptMulti::new(types::Fd(fd)).build().user_data(user_data);
        unsafe {
            self.ring.submission().push(&entry).unwrap();
        }
    }

    /// Submit an `Fsync` op with the `DATASYNC` flag on `fd`.
    /// One-shot: a single CQE is delivered when the kernel's fdatasync
    /// completes; the CQE's `res` is the fdatasync return code.
    pub fn prep_fsync(&mut self, fd: i32, user_data: u64) {
        self.ensure_sq_room();
        let entry = opcode::Fsync::new(types::Fd(fd))
            .flags(types::FsyncFlags::DATASYNC)
            .build()
            .user_data(user_data);
        unsafe {
            self.ring.submission().push(&entry).unwrap();
        }
    }

    /// Submit a relative `Timeout` op that fires after `timeout_ns` nanoseconds.
    /// One-shot: a single CQE is delivered (with `res = -ETIME` on natural
    /// expiry) when the timer expires.
    pub fn prep_timeout(&mut self, timeout_ns: u64, user_data: u64) {
        self.ensure_sq_room();
        let mut ts = self.spec_pool.pop().unwrap_or_else(|| Box::new(types::Timespec::new()));
        *ts = types::Timespec::new()
            .sec(timeout_ns / 1_000_000_000)
            .nsec((timeout_ns % 1_000_000_000) as u32);
        let ts_ptr: *const types::Timespec = &*ts;
        self.timer_specs.insert(user_data, ts);
        let entry = opcode::Timeout::new(ts_ptr).build().user_data(user_data);
        unsafe {
            self.ring.submission().push(&entry).unwrap();
        }
    }

    /// Release the Timespec owned for `user_data` back into the pool.
    /// Called from the `KIND_TIMEOUT` CQE dispatch arm — the kernel is
    /// done with the pointer once the Timeout's CQE has been drained.
    pub fn release_timer_spec(&mut self, user_data: u64) {
        if let Some(ts) = self.timer_specs.remove(&user_data) {
            self.spec_pool.push(ts);
        }
    }

    /// Submit a `FUTEX_WAITV` op over `nr` pointer-stable `FutexWaitV`
    /// entries. The caller owns the storage for the array so that the
    /// kernel's asynchronous dereference stays valid until the matching
    /// CQE is drained.
    ///
    /// One-shot: a single CQE is delivered on wake / cancellation.
    /// Requires Linux 6.7+; probed at `Reactor::new`.
    ///
    /// # Safety
    /// The caller promises each `FutexWaitV` entry's `uaddr` points to
    /// a live atomic u32 shared with the producer.
    pub unsafe fn prep_futex_waitv(&mut self, futexv: *const types::FutexWaitV, nr: u32, user_data: u64) {
        self.ensure_sq_room();
        let entry = opcode::FutexWaitV::new(futexv, nr).build().user_data(user_data);
        self.ring.submission().push(&entry).unwrap();
    }

    /// Submit an `AsyncCancel` op targeting an in-flight SQE identified
    /// by its `target_user_data`. The CQE for the cancelled SQE
    /// arrives first (with `res = -ECANCELED`) and the AsyncCancel's
    /// own CQE follows.
    pub fn prep_async_cancel(&mut self, target_user_data: u64, user_data: u64) {
        self.ensure_sq_room();
        let entry = opcode::AsyncCancel::new(target_user_data).build().user_data(user_data);
        unsafe {
            self.ring.submission().push(&entry).unwrap();
        }
    }

    /// Eagerly flush pending SQEs to the kernel (no wait). On failure the
    /// SQEs stay queued and go out with the next tick's submit — log and
    /// continue; `what` names the operation for the log line.
    pub fn flush_sqes(&mut self, what: &str) {
        if let Err(e) = self.submit_and_wait_timeout(0, 0) {
            crate::gnitz_error!(
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
    pub fn submit_and_wait_timeout(&mut self, min_complete: u32, timeout_ms: i32) -> Result<i32, i32> {
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
    pub fn drain_cqes(&mut self, out: &mut [Cqe]) -> usize {
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
