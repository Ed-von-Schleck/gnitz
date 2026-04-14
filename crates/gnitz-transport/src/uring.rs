use io_uring::{opcode, types, IoUring};

use crate::ring::{Cqe, Ring};

/// Production Ring implementation wrapping the `io-uring` crate.
pub struct IoUringRing {
    ring: IoUring,
    sq_entries: u32,
    /// Owns Timespec values for outstanding `prep_timeout` SQEs. The
    /// kernel reads these pointers when the timer fires (which can be
    /// long after `submit()` returns), so the storage must outlive the
    /// CQE. Bounded only by the ring's lifetime — see the
    /// `debug_assert!` in `prep_timeout`.
    timer_specs: Vec<Box<types::Timespec>>,
}

impl IoUringRing {
    pub fn new(entries: u32) -> std::io::Result<Self> {
        let ring = IoUring::new(entries)?;
        let sq_entries = ring.params().sq_entries();
        Ok(IoUringRing {
            ring,
            sq_entries,
            timer_specs: Vec::new(),
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

    fn prep_poll_add(&mut self, fd: i32, mask: u32, user_data: u64) {
        self.ensure_sq_room();
        let entry = opcode::PollAdd::new(types::Fd(fd), mask)
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
        self.timer_specs.push(ts);
        // Catches accidental hot-path use of prep_timeout in debug builds.
        // Production growth is bounded only by the ring's lifetime.
        debug_assert!(self.timer_specs.len() < 1024,
            "prep_timeout leak: {} live Timespecs", self.timer_specs.len());
        let entry = opcode::Timeout::new(ts_ptr)
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

        if min_complete == 0 || timeout_ms <= 0 {
            // Submit only, no wait
            match self.ring.submit() {
                Ok(n) => Ok(n as i32),
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
