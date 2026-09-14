//! Stream delivery: where a reactor future waits for the *next* of many values.

use std::collections::VecDeque;
use std::task::{Context, Poll, Waker};

pub(super) struct WakeQueue<T> {
    queue: VecDeque<T>,
    /// The one awaiter. One left by a dropped awaiter at most re-polls a live task:
    /// task keys are never reused.
    waker: Option<Waker>,
    /// No further value will be pushed, so a drained queue resolves to `None`
    /// rather than parking. Never set on a stream with no end, whose futures
    /// unwrap the `Option`.
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
        self.park(cx);
        Poll::Pending
    }

    /// Register `cx`'s waker for the next push or close, without taking a value.
    pub(super) fn park(&mut self, cx: &Context<'_>) {
        match &mut self.waker {
            Some(w) => w.clone_from(cx.waker()),
            None => self.waker = Some(cx.waker().clone()),
        }
    }

    /// Take the next value without parking. For a caller that must not await.
    pub(super) fn pop(&mut self) -> Option<T> {
        self.queue.pop_front()
    }

    pub(super) fn is_empty(&self) -> bool {
        self.queue.is_empty()
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

    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.queue.len()
    }
}
