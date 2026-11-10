//! The worker processes the master forked: the watch for a death and the kill
//! that ends them. A worker takes no stop request, so the master ends one only
//! by killing it; a worker that stops by itself — a failed boot, a fail-stop —
//! is what [`Workers::death`] reports.

use std::future::{poll_fn, Future};
use std::num::NonZero;
use std::task::Poll;

use crate::runtime::reactor::Reactor;
use gnitz_foundation::posix_io::retry_eintr;

/// The master's children, by worker rank. Each stays an unreaped child until
/// the value drops, which kills and reaps every one, so its pid names that
/// worker for as long as it is held.
pub(crate) struct Workers(Vec<NonZero<libc::pid_t>>);

impl Workers {
    pub(crate) fn new(pids: Vec<NonZero<libc::pid_t>>) -> Self {
        Workers(pids)
    }

    /// Resolves with the rank of the first worker to exit, which it leaves
    /// unreaped.
    pub(crate) async fn death(&self, reactor: &Reactor) -> usize {
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
        if res != 0 {
            // No wait on a child of this process fails but by a bug.
            gnitz_fatal_abort!("waitid on worker {w}: {}", std::io::Error::from_raw_os_error(-res));
        }
        w
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
