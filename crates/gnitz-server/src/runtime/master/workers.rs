//! The worker processes the master forked: the watch for a death and the kill
//! that ends them. A worker takes no stop request, so the master ends one only
//! by killing it; a worker that stops by itself — a failed boot, a fail-stop —
//! is what [`Workers::death`] reports.

use std::fmt;
use std::future::{Future, poll_fn};
use std::num::NonZero;
use std::task::Poll;

use crate::runtime::reactor::Reactor;
use gnitz_foundation::posix_io::retry_eintr;

/// The master's children, by worker rank. Each stays an unreaped child until
/// the value drops, which kills and reaps every one, so its pid names that
/// worker for as long as it is held.
pub(crate) struct Workers(Vec<NonZero<libc::pid_t>>);

/// How a worker ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum WorkerEnd {
    /// It exited with this status.
    Exited(i32),
    /// This signal killed it.
    Killed(i32),
    /// This signal killed it and dumped its core.
    Dumped(i32),
}

impl WorkerEnd {
    /// How the waited-for child `info` names ended.
    fn of(info: &libc::siginfo_t) -> Self {
        // SAFETY: `info` was filled by a `waitid` on an exited child, whose
        // siginfo carries `si_status`.
        let status = unsafe { info.si_status() };
        match info.si_code {
            libc::CLD_KILLED => WorkerEnd::Killed(status),
            libc::CLD_DUMPED => WorkerEnd::Dumped(status),
            _ => WorkerEnd::Exited(status),
        }
    }
}

/// `sig` by number, and by name when it is one a worker dies of.
struct Signal(i32);

impl fmt::Display for Signal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self.0 {
            libc::SIGKILL => "SIGKILL",
            libc::SIGABRT => "SIGABRT",
            libc::SIGSEGV => "SIGSEGV",
            libc::SIGBUS => "SIGBUS",
            libc::SIGILL => "SIGILL",
            libc::SIGFPE => "SIGFPE",
            sig => return write!(f, "signal {sig}"),
        };
        write!(f, "signal {} ({name})", self.0)
    }
}

impl fmt::Display for WorkerEnd {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            WorkerEnd::Exited(code) => write!(f, "exited with status {code}"),
            WorkerEnd::Killed(sig) => write!(f, "was killed by {}", Signal(sig)),
            WorkerEnd::Dumped(sig) => write!(f, "dumped core on {}", Signal(sig)),
        }
    }
}

impl Workers {
    pub(crate) fn new(pids: Vec<NonZero<libc::pid_t>>) -> Self {
        Workers(pids)
    }

    /// Take `pid` as the next rank's worker.
    pub(crate) fn push(&mut self, pid: NonZero<libc::pid_t>) {
        self.0.push(pid);
    }

    /// Resolves with the rank of the first worker to exit and how it ended,
    /// leaving it unreaped.
    pub(crate) async fn death(&self, reactor: &Reactor) -> (usize, WorkerEnd) {
        let mut exits: Vec<_> = self
            .0
            .iter()
            .map(|pid| Box::pin(reactor.child_exit(pid.get())))
            .collect();
        let (w, res) = poll_fn(|cx| {
            (exits.iter_mut().enumerate())
                .find_map(|(w, exit)| match exit.as_mut().poll(cx) {
                    Poll::Ready(res) => Some((w, res)),
                    Poll::Pending => None,
                })
                .map_or(Poll::Pending, Poll::Ready)
        })
        .await;
        match res {
            Ok(info) => (w, WorkerEnd::of(&info)),
            // No wait on a child of this process fails but by a bug.
            Err(e) => gnitz_fatal_abort!("waitid on worker {w}: {}", std::io::Error::from_raw_os_error(-e)),
        }
    }
}

impl Drop for Workers {
    /// `SIGKILL` every worker and reap each, blocking until all have exited. A
    /// worker holds nothing a kill loses: what a restart recovers is in the
    /// synced SAL and the shards of the last checkpoint.
    fn drop(&mut self) {
        for pid in &self.0 {
            unsafe { libc::kill(pid.get(), libc::SIGKILL) };
        }
        for pid in &self.0 {
            let mut status: i32 = 0;
            let _ = retry_eintr(|| unsafe { libc::waitpid(pid.get(), &mut status, 0) });
        }
    }
}

#[cfg(test)]
#[path = "tests/workers.rs"]
mod tests;
