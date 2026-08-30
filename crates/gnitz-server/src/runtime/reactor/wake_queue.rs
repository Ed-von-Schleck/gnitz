//! Stream delivery: where a reactor future waits for the *next* of many values.
//!
//! [`super::park::ParkMap`] is the one-shot half of the same job. This is the
//! other half — accepted connections, a worker's continuation frames, inbound
//! client frames, exchange frames and `mpsc` — each of which would otherwise
//! spell out its own queue, waker slot and end-of-stream flag.
//!
//! No interior mutability: every user already holds one behind a `RefCell`.

use std::collections::VecDeque;
use std::task::{Context, Poll, Waker};

pub(super) struct WakeQueue<T> {
    queue: VecDeque<T>,
    /// The one awaiter parked on this queue. One, not a list: every user is a
    /// single consumer — one accept loop, one scan awaiter, one connection
    /// handler, one relay task, one `mpsc::Receiver`.
    waker: Option<Waker>,
    /// No further value will be pushed, so a drained queue resolves to `None`
    /// rather than parking. Never set by the users whose stream has no end
    /// (accept, scan routing); their futures unwrap the `Option`.
    closed: bool,
}

impl<T> Default for WakeQueue<T> {
    fn default() -> Self {
        WakeQueue {
            queue: VecDeque::new(),
            waker: None,
            closed: false,
        }
    }
}

impl<T> WakeQueue<T> {
    /// Queue `v` and wake the awaiter, if one is parked.
    pub(super) fn push(&mut self, v: T) {
        self.queue.push_back(v);
        if let Some(w) = self.waker.take() {
            w.wake();
        }
    }

    /// Hand the next value to the awaiter, park it, or report the stream ended.
    pub(super) fn poll(&mut self, cx: &Context<'_>) -> Poll<Option<T>> {
        if let Some(v) = self.queue.pop_front() {
            return Poll::Ready(Some(v));
        }
        if self.closed {
            return Poll::Ready(None);
        }
        self.waker = Some(cx.waker().clone());
        Poll::Pending
    }

    /// Take the next value without parking. For a caller that must not await.
    pub(super) fn pop(&mut self) -> Option<T> {
        self.queue.pop_front()
    }

    /// End the stream and wake the parked awaiter, so its poll past the
    /// drained queue resolves to `None`. Idempotent.
    pub(super) fn close(&mut self) {
        self.closed = true;
        if let Some(w) = self.waker.take() {
            w.wake();
        }
    }

    pub(super) fn is_closed(&self) -> bool {
        self.closed
    }

    /// Waker hygiene: a cancelled awaiter must leave no waker behind, or the
    /// next push wakes a task that is no longer listening.
    pub(super) fn clear_waiter(&mut self) {
        self.waker = None;
    }

    #[cfg(test)]
    pub(super) fn has_waiter(&self) -> bool {
        self.waker.is_some()
    }

    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.queue.len()
    }

    #[cfg(test)]
    pub(super) fn iter(&self) -> impl Iterator<Item = &T> {
        self.queue.iter()
    }
}
