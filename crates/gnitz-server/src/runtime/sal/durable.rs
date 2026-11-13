//! How far a log is durable: the waiters on an LSN, and the driver of the
//! fdatasyncs that answer them.

use std::cell::Cell;
use std::fs::File;
use std::os::fd::AsFd;

use crate::runtime::reactor::Reactor;
use crate::runtime::sync::{Level, Parked};

/// How far a log is durable, and who waits on it. LSNs are its owner's: it
/// orders them and knows nothing else of them.
#[derive(Default)]
pub(super) struct Durable {
    /// Every group published at or below this LSN is durable.
    through: Level,
    /// The highest LSN asked to be made durable.
    wanted: Cell<u64>,
    /// The LSN the fdatasync in flight makes durable; `0` while none is.
    syncing: Cell<u64>,
    /// [`Self::run`], parked until a waiter wants more than is durable.
    driver: Parked,
}

impl Durable {
    /// Start making everything at or below `lsn` durable, without waiting for
    /// it. Needs [`Self::run`] running.
    pub(super) fn want(&self, lsn: u64) {
        if lsn > self.wanted.get() {
            self.wanted.set(lsn);
            self.driver.wake();
        }
    }

    /// Resolves once everything at or below `lsn` is durable. Needs
    /// [`Self::run`] running, or a [`Self::mark`] from elsewhere.
    pub(super) async fn through(&self, lsn: u64) {
        std::future::poll_fn(|cx| {
            let reached = self.through.poll_reached(lsn, cx);
            if reached.is_pending() {
                self.want(lsn);
            }
            reached
        })
        .await
    }

    /// The LSN the fdatasync in flight makes durable; `0` while none is.
    pub(super) fn syncing(&self) -> u64 {
        self.syncing.get()
    }

    /// Everything at or below `lsn` is durable: wake whoever waits on no more.
    pub(super) fn mark(&self, lsn: u64) {
        self.through.raise(lsn);
    }

    /// Run the fdatasyncs of `file` that [`Self::through`] waits for, one at a
    /// time and fatal on failure. Each makes durable what `published` answers
    /// when it is submitted, so the waiters that arrive while one is in flight
    /// share the next. Never returns.
    pub(super) async fn run(&self, reactor: &Reactor, file: &'static File, published: impl Fn() -> u64) {
        loop {
            self.driver.until(|| self.wanted.get() > self.through.get()).await;
            let target = published();
            self.syncing.set(target);
            let rc = reactor.fsync(file.as_fd()).await;
            if rc < 0 {
                gnitz_fatal_abort!("SAL fdatasync failed rc={rc}");
            }
            self.syncing.set(0);
            self.mark(target);
        }
    }
}

#[cfg(test)]
#[path = "tests/durable.rs"]
mod tests;
