//! The reactor's io_uring wrapper: queue, submit, wait, drain.

use std::mem::MaybeUninit;
use std::time::Duration;

use io_uring::{cqueue, squeue, types, IoUring};

pub(super) struct IoUringRing {
    ring: IoUring,
}

/// The one answer to an `io_uring_enter` result.
fn entered(rc: std::io::Result<usize>) {
    let Err(e) = rc else { return };
    match e.raw_os_error() {
        // A wait that timed out, or that a signal ended.
        Some(libc::ETIME | libc::EINTR) => {}
        // No request memory, or completions overflowed: the SQEs stay queued for
        // the next submit.
        Some(libc::EAGAIN | libc::EBUSY) => gnitz_warn!("reactor: io_uring_enter: {e}; retrying"),
        // A broken ring or a lost completion, which no later pass repairs.
        _ => gnitz_fatal_abort!("reactor: io_uring_enter failed: {e}"),
    }
}

impl IoUringRing {
    pub(super) fn new(entries: u32) -> std::io::Result<Self> {
        Ok(IoUringRing { ring: IoUring::new(entries)? })
    }

    /// Queue `sqe` under `user_data`, first submitting the pending batch if the SQ
    /// is full.
    ///
    /// # Safety
    /// Every buffer and fd `sqe` names stays valid until its CQE, or until the ring
    /// drops.
    pub(super) unsafe fn push(&mut self, sqe: squeue::Entry, user_data: u64) {
        if self.ring.submission().is_full() {
            self.submit();
        }
        // SAFETY: forwarded to the caller.
        if unsafe { self.ring.submission().push(&sqe.user_data(user_data)) }.is_err() {
            gnitz_fatal_abort!("reactor: the SQ is still full after a submit");
        }
    }

    /// Submit pending SQEs without waiting. No syscall when the SQ is empty and no
    /// completion overflowed.
    pub(super) fn submit(&mut self) {
        let sq = self.ring.submission();
        if sq.is_empty() && !sq.cq_overflow() {
            return;
        }
        drop(sq);
        entered(self.ring.submit());
    }

    /// Submit pending SQEs and wait for one CQE, for at most `timeout`.
    pub(super) fn wait(&mut self, timeout: Option<Duration>) {
        entered(match timeout {
            None => self.ring.submitter().submit_and_wait(1),
            Some(d) => {
                let ts = types::Timespec::from(d);
                let args = types::SubmitArgs::new().timespec(&ts);
                self.ring.submitter().submit_with_args(1, &args)
            }
        });
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
