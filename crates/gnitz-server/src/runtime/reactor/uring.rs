//! The reactor's io_uring wrapper: queue, submit, wait, drain.

use std::mem::MaybeUninit;
use std::time::Duration;

use io_uring::{cqueue, squeue, types, IoUring};

pub(super) struct IoUringRing {
    ring: IoUring,
}

impl IoUringRing {
    pub(super) fn new(entries: u32) -> std::io::Result<Self> {
        Ok(IoUringRing { ring: IoUring::new(entries)? })
    }

    /// Queue `sqe` under `user_data`, first flushing the pending batch if the SQ
    /// is full.
    ///
    /// # Safety
    /// Every buffer and fd `sqe` names stays valid until its CQE, or until the ring
    /// drops.
    pub(super) unsafe fn push(&mut self, sqe: squeue::Entry, user_data: u64) {
        if self.ring.submission().is_full() {
            if let Err(e) = self.ring.submit() {
                gnitz_fatal_abort!("reactor: io_uring submit with a full SQ failed: {e}");
            }
        }
        // SAFETY: forwarded to the caller.
        unsafe { self.ring.submission().push(&sqe.user_data(user_data)) }.expect("the flush emptied the SQ");
    }

    /// Submit pending SQEs without waiting. No syscall when the SQ is empty and no
    /// completion overflowed; on failure the SQEs stay queued.
    pub(super) fn submit(&mut self) -> std::io::Result<()> {
        let sq = self.ring.submission();
        if sq.is_empty() && !sq.cq_overflow() {
            return Ok(());
        }
        drop(sq);
        self.ring.submit().map(drop)
    }

    /// Submit pending SQEs and wait for one CQE, for at most `timeout`. An expired
    /// timeout or a signal is not an error.
    pub(super) fn wait(&mut self, timeout: Option<Duration>) -> std::io::Result<()> {
        let rc = match timeout {
            None => self.ring.submitter().submit_and_wait(1),
            Some(d) => {
                let ts = types::Timespec::from(d);
                let args = types::SubmitArgs::new().timespec(&ts);
                self.ring.submitter().submit_with_args(1, &args)
            }
        };
        match rc {
            Err(e) if !matches!(e.raw_os_error(), Some(libc::ETIME | libc::EINTR)) => Err(e),
            _ => Ok(()),
        }
    }

    /// Pop up to `buf.len()` completed CQEs. No syscall.
    pub(super) fn fill<'a>(&mut self, buf: &'a mut [MaybeUninit<cqueue::Entry>]) -> &'a [cqueue::Entry] {
        self.ring.completion().fill(buf)
    }
}

impl Drop for IoUringRing {
    /// Cancel every submitted op. One already running in the kernel's worker pool
    /// (an fsync) runs on, and names no memory.
    fn drop(&mut self) {
        // NotFound when nothing is in flight.
        let _ = self
            .ring
            .submitter()
            .register_sync_cancel(None, types::CancelBuilder::any());
    }
}
