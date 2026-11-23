//! The reactor's ring: the host check, and every `io_uring_enter` the reactor
//! makes — queueing and submitting SQEs, the loop's wait, and the drop's cancel.

use std::time::Instant;

use gnitz_foundation::gnitz_warn;

use io_uring::{IoUring, Probe, opcode, squeue, types};

use super::ReactorShared;

/// The ring's SQ entries. This sets submit batching, not depth: [`ReactorShared::push`]
/// flushes a full SQ.
pub(super) const SQ_ENTRIES: u32 = 256;

/// The one answer to an `io_uring_enter` result.
fn entered(rc: std::io::Result<usize>) {
    let Err(e) = rc else { return };
    match e.raw_os_error() {
        // A wait that timed out, or that a signal ended.
        Some(libc::ETIME | libc::EINTR) => {}
        // No request memory, or completions overflowed: the SQEs stay queued for
        // the loop's next submit; a push that needed this submit to make room
        // aborts instead.
        Some(libc::EAGAIN | libc::EBUSY) => gnitz_warn!("reactor: io_uring_enter: {e}; the SQEs stay queued"),
        // A broken ring or a lost completion, which no later pass repairs.
        _ => gnitz_fatal_abort!("reactor: io_uring_enter failed: {e}"),
    }
}

/// Every opcode the reactor issues, by its kernel name.
const OPCODES: [(u8, &str); 8] = [
    (opcode::Accept::CODE, "ACCEPT"),
    (opcode::Recv::CODE, "RECV"),
    (opcode::Send::CODE, "SEND"),
    (opcode::Read::CODE, "READ"),
    (opcode::Fsync::CODE, "FSYNC"),
    (opcode::FutexWait::CODE, "FUTEX_WAIT"),
    (opcode::WaitId::CODE, "WAITID"),
    (opcode::AsyncCancel::CODE, "ASYNC_CANCEL"),
];

/// Whether this host gives the reactor what it runs on: a ring, and every
/// opcode in [`OPCODES`]. The error names the cause and its remedy — a refusal
/// here is a host's configuration, most often a container's seccomp profile.
pub(crate) fn check_host() -> Result<(), String> {
    let refused = |syscall: &str, e: std::io::Error| match gnitz_foundation::host::io_uring_refusal(&e) {
        Some(why) => format!("{syscall} failed: {e}; {why}"),
        None => format!("{syscall} failed: {e}"),
    };
    let ring = IoUring::new(2).map_err(|e| refused("io_uring_setup", e))?;
    let mut probe = Probe::new();
    ring.submitter()
        .register_probe(&mut probe)
        .map_err(|e| refused("io_uring_register", e))?;
    let missing: Vec<&str> = OPCODES
        .iter()
        .filter(|&&(code, _)| !probe.is_supported(code))
        .map(|&(_, name)| name)
        .collect();
    if missing.is_empty() {
        return Ok(());
    }
    let release = std::fs::read_to_string("/proc/sys/kernel/osrelease").unwrap_or_default();
    Err(format!(
        "kernel {} lacks io_uring IORING_OP_{}: Linux 6.7 or later is required",
        release.trim(),
        missing.join(", IORING_OP_")
    ))
}

impl ReactorShared {
    /// Queue `sqe` under `user_data`, first submitting the pending batch if the SQ
    /// is full.
    ///
    /// # Safety
    /// Every buffer and fd `sqe` names stays valid until its CQE, or until
    /// [`Self::cancel_all`], after which nothing is pushed or submitted.
    pub(super) unsafe fn push(&self, sqe: squeue::Entry, user_data: u64) {
        let mut ring = self.ring.borrow_mut();
        if ring.submission().is_full() {
            entered(ring.submit());
        }
        // SAFETY: forwarded to the caller.
        if unsafe { ring.submission().push(&sqe.user_data(user_data)) }.is_err() {
            gnitz_fatal_abort!("reactor: the SQ is full and the kernel refused its submit");
        }
    }

    /// Submit pending SQEs without waiting. No syscall when the SQ is empty and no
    /// completion overflowed.
    pub(super) fn submit(&self) {
        let mut ring = self.ring.borrow_mut();
        let sq = ring.submission();
        if sq.is_empty() && !sq.cq_overflow() {
            return;
        }
        drop(sq);
        entered(ring.submit());
    }

    /// Submit pending SQEs and wait for one CQE, or until `until` passes.
    pub(super) fn wait(&self, until: Option<Instant>) {
        let ring = self.ring.borrow();
        entered(match until {
            None => ring.submitter().submit_and_wait(1),
            Some(at) => {
                let ts = types::Timespec::from(at.saturating_duration_since(Instant::now()));
                ring.submitter()
                    .submit_with_args(1, &types::SubmitArgs::new().timespec(&ts))
            }
        });
    }

    /// Cancel every submitted op, after which the kernel touches nothing their
    /// SQEs name. The ring's last use: an SQE still queued is never submitted.
    /// An op already running in the kernel's worker pool (an fsync) runs on,
    /// and names no memory.
    pub(super) fn cancel_all(&self) {
        // NotFound when nothing is in flight.
        let _ = self
            .ring
            .borrow()
            .submitter()
            .register_sync_cancel(None, types::CancelBuilder::any());
    }
}

#[cfg(test)]
#[path = "tests/uring.rs"]
mod tests;
