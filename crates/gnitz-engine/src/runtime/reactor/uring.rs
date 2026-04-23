use std::collections::HashMap;

use io_uring::{opcode, types, IoUring};

use super::ring::{Cqe, Ring};

/// Production Ring implementation wrapping the `io-uring` crate.
pub struct IoUringRing {
    ring: IoUring,
    sq_entries: u32,
    /// Owns Timespec values for outstanding `prep_timeout` SQEs. The
    /// kernel reads these pointers when the timer fires (which can be
    /// long after `submit()` returns), so the storage must outlive the
    /// CQE. Keyed by the SQE's `user_data` so we can drop the Box when
    /// the matching CQE is drained.
    timer_specs: HashMap<u64, Box<types::Timespec>>,
}

impl IoUringRing {
    pub fn new(entries: u32) -> std::io::Result<Self> {
        let ring = IoUring::new(entries)?;
        let sq_entries = ring.params().sq_entries();
        Ok(IoUringRing {
            ring,
            sq_entries,
            timer_specs: HashMap::new(),
        })
    }

    /// Ensure at least 1 SQ slot is available. If the SQ is full,
    /// submit all pending SQEs to the kernel to free every slot.
    #[inline]
    fn ensure_sq_room(&mut self) {
        if self.ring.submission().len() >= self.sq_entries as usize {
            let _ = self.ring.submit();
        }
    }
}

impl Ring for IoUringRing {
    fn prep_recv(&mut self, fd: i32, buf: *mut u8, len: u32, user_data: u64) {
        self.ensure_sq_room();
        let entry = opcode::Recv::new(types::Fd(fd), buf, len)
            .build()
            .user_data(user_data);
        unsafe {
            self.ring.submission().push(&entry).unwrap();
        }
    }

    fn prep_send(&mut self, fd: i32, buf: *const u8, len: u32, user_data: u64) {
        self.ensure_sq_room();
        let entry = opcode::Send::new(types::Fd(fd), buf, len)
            .build()
            .user_data(user_data);
        unsafe {
            self.ring.submission().push(&entry).unwrap();
        }
    }

    fn prep_accept(&mut self, fd: i32, user_data: u64) {
        self.ensure_sq_room();
        let entry = opcode::AcceptMulti::new(types::Fd(fd))
            .build()
            .user_data(user_data);
        unsafe {
            self.ring.submission().push(&entry).unwrap();
        }
    }

    fn prep_fsync(&mut self, fd: i32, user_data: u64) {
        self.ensure_sq_room();
        let entry = opcode::Fsync::new(types::Fd(fd))
            .flags(types::FsyncFlags::DATASYNC)
            .build()
            .user_data(user_data);
        unsafe {
            self.ring.submission().push(&entry).unwrap();
        }
    }

    fn prep_timeout(&mut self, timeout_ns: u64, user_data: u64) {
        self.ensure_sq_room();
        let ts = Box::new(
            types::Timespec::new()
                .sec(timeout_ns / 1_000_000_000)
                .nsec((timeout_ns % 1_000_000_000) as u32),
        );
        let ts_ptr: *const types::Timespec = &*ts;
        self.timer_specs.insert(user_data, ts);
        let entry = opcode::Timeout::new(ts_ptr)
            .build()
            .user_data(user_data);
        unsafe {
            self.ring.submission().push(&entry).unwrap();
        }
    }

    unsafe fn prep_futex_waitv(
        &mut self,
        futexv: *const types::FutexWaitV,
        nr: u32,
        user_data: u64,
    ) {
        self.ensure_sq_room();
        let entry = opcode::FutexWaitV::new(futexv, nr)
            .build()
            .user_data(user_data);
        self.ring.submission().push(&entry).unwrap();
    }

    fn prep_async_cancel(&mut self, target_user_data: u64, user_data: u64) {
        self.ensure_sq_room();
        let entry = opcode::AsyncCancel::new(target_user_data)
            .build()
            .user_data(user_data);
        unsafe {
            self.ring.submission().push(&entry).unwrap();
        }
    }

    fn submit_and_wait_timeout(
        &mut self,
        min_complete: u32,
        timeout_ms: i32,
    ) -> Result<i32, i32> {
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
            match self
                .ring
                .submitter()
                .submit_with_args(min_complete as usize, &args)
            {
                Ok(n) => Ok(n as i32),
                Err(ref e) if e.raw_os_error() == Some(libc::ETIME) => Ok(0),
                Err(ref e) if e.raw_os_error() == Some(libc::EINTR) => Ok(0),
                Err(e) => Err(e.raw_os_error().unwrap_or(-1)),
            }
        }
    }

    fn drain_cqes(&mut self, out: &mut [Cqe]) -> usize {
        let mut count = 0;
        {
            // CQ iterator borrows `self.ring`; `self.timer_specs` is a
            // separate field and cannot be mutated while the iterator is live.
            // Two-pass: collect CQEs into `out`, then remove timer entries
            // from `timer_specs` by scanning `out[..count]`. Removes on
            // non-timer keys are no-ops (HashMap returns None), so there is
            // no need to pre-check membership.
            let cq = self.ring.completion();
            for cqe in cq {
                if count >= out.len() { break; }
                out[count] = Cqe {
                    user_data: cqe.user_data(),
                    res: cqe.result(),
                    flags: cqe.flags(),
                };
                count += 1;
            }
        }
        for cqe in &out[..count] {
            self.timer_specs.remove(&cqe.user_data);
        }
        count
    }
}
