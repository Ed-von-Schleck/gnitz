//! Stream delivery: where a reactor future waits for the *next* of many values,
//! and learns when there will be none.

use std::collections::VecDeque;
use std::task::{Context, Poll, Waker};

/// Slots a drained queue keeps; a burst past this returns its excess on draining.
const RETAINED_SLOTS: usize = 1024;

pub(super) struct WakeQueue<T> {
    queue: VecDeque<T>,
    /// The one awaiter. A dropped awaiter's waker stays behind; its wake is a
    /// spurious poll.
    waker: Option<Waker>,
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
        debug_assert!(!self.closed, "a value pushed after the end");
        self.queue.push_back(v);
        self.wake();
    }

    /// End the stream: once the queued values are handed out, `poll` yields `None`.
    pub(super) fn close(&mut self) {
        self.closed = true;
        self.wake();
    }

    /// Hand the next value to the awaiter, `None` once the stream has ended and
    /// is drained, or park it.
    pub(super) fn poll(&mut self, cx: &Context<'_>) -> Poll<Option<T>> {
        self.poll_ready(cx).map(|()| self.pop())
    }

    /// Park the awaiter until [`Self::poll`] would not: a value is queued or
    /// the stream has ended. Takes nothing.
    pub(super) fn poll_ready(&mut self, cx: &Context<'_>) -> Poll<()> {
        if self.closed || !self.queue.is_empty() {
            return Poll::Ready(());
        }
        self.waker = Some(cx.waker().clone());
        Poll::Pending
    }

    fn wake(&mut self) {
        if let Some(w) = self.waker.take() {
            w.wake();
        }
    }

    /// Take the next value without parking. For a caller that must not await.
    pub(super) fn pop(&mut self) -> Option<T> {
        let v = self.queue.pop_front();
        if self.queue.is_empty() && self.queue.capacity() > RETAINED_SLOTS {
            self.queue.shrink_to(RETAINED_SLOTS);
        }
        v
    }

    /// Drop every queued value and the capacity they held.
    pub(super) fn clear(&mut self) {
        self.queue = VecDeque::new();
    }
}
