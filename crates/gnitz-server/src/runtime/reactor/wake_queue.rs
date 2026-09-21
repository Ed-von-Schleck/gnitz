//! Stream delivery: where a reactor future waits for the *next* of many values.

use std::collections::VecDeque;
use std::task::{Context, Poll, Waker};

use super::park_waker;

/// Slots a drained queue keeps; a burst past this returns its excess on draining.
const RETAINED_SLOTS: usize = 1024;

pub(super) struct WakeQueue<T> {
    queue: VecDeque<T>,
    /// The one awaiter. One left by a dropped awaiter at most re-polls a live task:
    /// task keys are never reused.
    waker: Option<Waker>,
}

impl<T> Default for WakeQueue<T> {
    fn default() -> Self {
        WakeQueue { queue: VecDeque::new(), waker: None }
    }
}

impl<T> WakeQueue<T> {
    /// Queue `v` and wake the awaiter, if one is parked.
    pub(super) fn push(&mut self, v: T) {
        self.queue.push_back(v);
        self.wake();
    }

    /// Hand the next value to the awaiter, or park it.
    pub(super) fn poll(&mut self, cx: &Context<'_>) -> Poll<T> {
        match self.take() {
            Some(v) => Poll::Ready(v),
            None => {
                self.park(cx);
                Poll::Pending
            }
        }
    }

    /// Register `cx`'s waker for the next push or wake, without taking a value.
    pub(super) fn park(&mut self, cx: &Context<'_>) {
        park_waker(&mut self.waker, cx.waker());
    }

    /// Wake the parked awaiter without queueing a value, so it re-reads whatever
    /// state it waits on beside the queue.
    pub(super) fn wake(&mut self) {
        if let Some(w) = self.waker.take() {
            w.wake();
        }
    }

    /// Take the next value without parking. For a caller that must not await.
    pub(super) fn pop(&mut self) -> Option<T> {
        self.take()
    }

    /// Drop every queued value and the capacity they held.
    pub(super) fn clear(&mut self) {
        self.queue = VecDeque::new();
    }

    fn take(&mut self) -> Option<T> {
        let v = self.queue.pop_front();
        if self.queue.is_empty() && self.queue.capacity() > RETAINED_SLOTS {
            self.queue.shrink_to(RETAINED_SLOTS);
        }
        v
    }

    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.queue.len()
    }
}
