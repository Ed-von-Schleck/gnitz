//! Async primitives for one thread's tasks: `WakeQueue`, `oneshot`, `chan`,
//! `AsyncRwLock` and `select2`.
//!
//! All `!Send` and `Rc<RefCell<_>>`-based on purpose: the master's tasks never
//! leave the reactor's thread, so a channel send costs no atomic and a waker
//! registration no lock.
//!
//! Each primitive wakes while holding its own `RefCell` borrow, which is sound
//! only for a waker that queues and never polls, as the reactor's does.

use std::cell::RefCell;
use std::collections::VecDeque;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

// ---------------------------------------------------------------------------
// WakeQueue
// ---------------------------------------------------------------------------

/// Slots a drained queue keeps; a burst past this returns its excess on draining.
const RETAINED_SLOTS: usize = 1024;

/// Where one awaiter waits for the *next* of many values, and learns when there
/// will be none.
pub(crate) struct WakeQueue<T> {
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
    pub(crate) fn push(&mut self, v: T) {
        debug_assert!(!self.closed, "a value pushed after the end");
        self.queue.push_back(v);
        self.wake();
    }

    /// End the stream: once the queued values are handed out, `poll` yields `None`.
    pub(crate) fn close(&mut self) {
        self.closed = true;
        self.wake();
    }

    /// Hand the next value to the awaiter, `None` once the stream has ended and
    /// is drained, or park it.
    pub(crate) fn poll(&mut self, cx: &Context<'_>) -> Poll<Option<T>> {
        self.poll_ready(cx).map(|()| self.pop())
    }

    /// Park the awaiter until [`Self::poll`] would not: a value is queued or
    /// the stream has ended. Takes nothing.
    pub(crate) fn poll_ready(&mut self, cx: &Context<'_>) -> Poll<()> {
        if self.is_ready() {
            return Poll::Ready(());
        }
        self.waker = Some(cx.waker().clone());
        Poll::Pending
    }

    /// Whether [`Self::poll`] would not park.
    pub(crate) fn is_ready(&self) -> bool {
        self.closed || !self.queue.is_empty()
    }

    /// Wake the awaiter, if one is parked, with nothing queued for it.
    pub(crate) fn wake(&mut self) {
        if let Some(w) = self.waker.take() {
            w.wake();
        }
    }

    /// Take the next value without parking. For a caller that must not await.
    pub(crate) fn pop(&mut self) -> Option<T> {
        let v = self.queue.pop_front();
        if self.queue.is_empty() && self.queue.capacity() > RETAINED_SLOTS {
            self.queue.shrink_to(RETAINED_SLOTS);
        }
        v
    }

    /// Drop every queued value and the capacity they held.
    pub(crate) fn clear(&mut self) {
        self.queue = VecDeque::new();
    }
}

// ---------------------------------------------------------------------------
// oneshot
// ---------------------------------------------------------------------------
//
// Single-threaded. A receiver resolves only on a send: a sender dropped unsent
// leaves it pending forever.

pub(crate) mod oneshot {
    use super::*;

    struct State<T> {
        value: Option<T>,
        waker: Option<Waker>,
    }

    pub(crate) struct Sender<T> {
        inner: Rc<RefCell<State<T>>>,
    }
    pub(crate) struct Receiver<T> {
        inner: Rc<RefCell<State<T>>>,
    }

    pub(crate) fn channel<T>() -> (Sender<T>, Receiver<T>) {
        let s = Rc::new(RefCell::new(State { value: None, waker: None }));
        (Sender { inner: Rc::clone(&s) }, Receiver { inner: s })
    }

    impl<T> Sender<T> {
        /// Send the result. A cancelled receiver is not an error: the value is
        /// parked in state nothing will read, and dropped with the `Rc`.
        pub(crate) fn send(self, v: T) {
            let mut s = self.inner.borrow_mut();
            s.value = Some(v);
            if let Some(w) = s.waker.take() {
                w.wake();
            }
        }
    }

    impl<T> Future for Receiver<T> {
        type Output = T;
        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<T> {
            let mut s = self.inner.borrow_mut();
            if let Some(v) = s.value.take() {
                return Poll::Ready(v);
            }
            s.waker = Some(cx.waker().clone());
            Poll::Pending
        }
    }
}

// ---------------------------------------------------------------------------
// chan (unbounded)
// ---------------------------------------------------------------------------
//
// Unbounded, with every sender reaching the one `Sender` through a shared
// owner. The stream has no end: a receiving task ends by being dropped.

pub(crate) mod chan {
    use super::*;

    pub(crate) struct Sender<T> {
        inner: Rc<RefCell<WakeQueue<T>>>,
    }
    pub(crate) struct Receiver<T> {
        inner: Rc<RefCell<WakeQueue<T>>>,
    }

    pub(crate) fn unbounded<T>() -> (Sender<T>, Receiver<T>) {
        let q = Rc::new(RefCell::new(WakeQueue::default()));
        (Sender { inner: Rc::clone(&q) }, Receiver { inner: q })
    }

    impl<T> Sender<T> {
        pub(crate) fn send(&self, v: T) {
            self.inner.borrow_mut().push(v);
        }
    }

    impl<T> Receiver<T> {
        pub(crate) async fn recv(&mut self) -> T {
            std::future::poll_fn(|cx| {
                self.inner
                    .borrow_mut()
                    .poll(cx)
                    .map(|v| v.expect("a chan never closes"))
            })
            .await
        }

        pub(crate) fn try_recv(&mut self) -> Option<T> {
            self.inner.borrow_mut().pop()
        }
    }
}

// ---------------------------------------------------------------------------
// AsyncRwLock (writer-preference)
// ---------------------------------------------------------------------------
//
// Payload-free: both guards are drop tokens admitting entry to a critical
// section, and what they protect lives outside. A lock's zero-sized tag names
// what it protects, so a guard of one tag cannot stand in for another's.
// Writer-preference blocks new readers as soon as a writer parks, so a writer
// cannot starve behind a stream of readers.

#[derive(Default)]
struct RwLockInner {
    readers: usize,
    has_writer: bool,
    /// Woken as a set: `read_ok` admits all of them or none, so there is nothing
    /// here to choose between. One entry per pending poll; a cancelled reader's
    /// waker stays behind, harmlessly.
    read_waiters: VecDeque<Waker>,
    /// Parked `WriteFuture`s by id, admitted one at a time in queue order. A
    /// woken entry keeps its place, its waker taken, until its future acquires
    /// or drops.
    write_waiters: VecDeque<(u64, Option<Waker>)>,
    /// The id the next parked writer takes. Ids come from this counter alone, so
    /// they are unique among this lock's queued writers.
    next_waiter: u64,
}

impl RwLockInner {
    /// A reader may enter: no writer holds the lock and none is waiting, so a
    /// stream of readers cannot starve a writer.
    fn read_ok(&self) -> bool {
        !self.has_writer && self.write_waiters.is_empty()
    }

    /// A reader or a writer is parked on the lock. A cancelled reader's stale
    /// waker counts too.
    fn contended(&self) -> bool {
        !self.read_waiters.is_empty() || !self.write_waiters.is_empty()
    }

    /// Nobody holds the lock in either mode.
    fn free(&self) -> bool {
        !self.has_writer && self.readers == 0
    }

    /// The writer queued as `me` (`None`: not queued) may enter: the lock is free
    /// and no writer is queued ahead of it, so a fresh writer cannot take the lock
    /// from a woken one that has yet to poll.
    fn write_ok(&self, me: Option<u64>) -> bool {
        self.free() && self.write_waiters.front().is_none_or(|(first, _)| me == Some(*first))
    }
}

/// A handle on one lock tagged `K`; clones share it. Every future and guard
/// holds a clone, so none borrows the handle it came from.
pub(crate) struct AsyncRwLock<K = ()>(Rc<RefCell<RwLockInner>>, PhantomData<fn() -> K>);

impl<K> Clone for AsyncRwLock<K> {
    fn clone(&self) -> Self {
        AsyncRwLock(Rc::clone(&self.0), PhantomData)
    }
}

impl<K> Default for AsyncRwLock<K> {
    fn default() -> Self {
        AsyncRwLock(Rc::default(), PhantomData)
    }
}

impl<K> AsyncRwLock<K> {
    pub(crate) fn read(&self) -> ReadFuture<K> {
        ReadFuture { lock: self.clone() }
    }

    pub(crate) fn write(&self) -> WriteFuture<K> {
        WriteFuture { lock: self.clone(), waiter: None }
    }

    /// Admit whoever the state now allows: the first parked writer, or every
    /// parked reader when no writer waits.
    fn wake_next(&self) {
        let mut s = self.0.borrow_mut();
        if s.free() {
            if let Some((_, slot)) = s.write_waiters.front_mut() {
                if let Some(w) = slot.take() {
                    w.wake();
                }
                return;
            }
        }
        if s.read_ok() {
            for w in s.read_waiters.drain(..) {
                w.wake();
            }
        }
    }

    fn release_read(&self) {
        self.0.borrow_mut().readers -= 1;
        self.wake_next();
    }

    fn release_write(&self) {
        self.0.borrow_mut().has_writer = false;
        self.wake_next();
    }
}

pub(crate) struct ReadFuture<K = ()> {
    lock: AsyncRwLock<K>,
}

impl<K> Future for ReadFuture<K> {
    type Output = ReadGuard<K>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<ReadGuard<K>> {
        let mut s = self.lock.0.borrow_mut();
        if s.read_ok() {
            s.readers += 1;
            return Poll::Ready(ReadGuard { lock: self.lock.clone() });
        }
        s.read_waiters.push_back(cx.waker().clone());
        Poll::Pending
    }
}

pub(crate) struct ReadGuard<K = ()> {
    lock: AsyncRwLock<K>,
}

impl<K> ReadGuard<K> {
    /// Whether a reader or a writer is parked behind this guard's lock.
    pub(crate) fn contended(&self) -> bool {
        self.lock.0.borrow().contended()
    }
}

impl<K> Drop for ReadGuard<K> {
    fn drop(&mut self) {
        self.lock.release_read();
    }
}

pub(crate) struct WriteFuture<K = ()> {
    lock: AsyncRwLock<K>,
    /// The id of this future's entry in `write_waiters`: `Some` iff it is queued.
    waiter: Option<u64>,
}

impl<K> Future for WriteFuture<K> {
    type Output = WriteGuard<K>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<WriteGuard<K>> {
        let Self { lock, waiter } = self.get_mut();
        let mut s = lock.0.borrow_mut();
        if s.write_ok(*waiter) {
            s.has_writer = true;
            // `write_ok` admits a queued writer only at the front.
            if let Some(id) = waiter.take() {
                let first = s.write_waiters.pop_front();
                debug_assert!(first.is_some_and(|(f, _)| f == id));
            }
            return Poll::Ready(WriteGuard { lock: lock.clone() });
        }
        match *waiter {
            Some(id) => {
                let (_, slot) = s
                    .write_waiters
                    .iter_mut()
                    .find(|(i, _)| *i == id)
                    .expect("a queued writer has an entry");
                *slot = Some(cx.waker().clone());
            }
            None => {
                let id = s.next_waiter;
                s.next_waiter += 1;
                s.write_waiters.push_back((id, Some(cx.waker().clone())));
                *waiter = Some(id);
            }
        }
        Poll::Pending
    }
}

impl<K> Drop for WriteFuture<K> {
    fn drop(&mut self) {
        let Some(id) = self.waiter.take() else { return };
        self.lock.0.borrow_mut().write_waiters.retain(|(i, _)| *i != id);
        // This future may have been the last writer blocking the parked readers.
        self.lock.wake_next();
    }
}

pub(crate) struct WriteGuard<K = ()> {
    lock: AsyncRwLock<K>,
}

impl<K> WriteGuard<K> {
    /// Whether a reader or a writer is parked behind this guard's lock.
    pub(crate) fn contended(&self) -> bool {
        self.lock.0.borrow().contended()
    }
}

impl<K> Drop for WriteGuard<K> {
    fn drop(&mut self) {
        self.lock.release_write();
    }
}

// ---------------------------------------------------------------------------
// select2
// ---------------------------------------------------------------------------

/// Result of `select2`: which side resolved first.
pub(crate) enum Either<A, B> {
    A(A),
    B(B),
}

/// Poll `a`, then `b`; the first found ready wins, so `a` wins a tie, which
/// `Peer::until_request` relies on to answer a sync ahead of a ready request.
/// The loser is dropped, and its `Drop` releases what it registered.
pub(crate) async fn select2<A, B>(a: A, b: B) -> Either<A::Output, B::Output>
where
    A: Future,
    B: Future,
{
    let mut a = std::pin::pin!(a);
    let mut b = std::pin::pin!(b);
    std::future::poll_fn(move |cx| {
        if let Poll::Ready(v) = a.as_mut().poll(cx) {
            return Poll::Ready(Either::A(v));
        }
        if let Poll::Ready(v) = b.as_mut().poll(cx) {
            return Poll::Ready(Either::B(v));
        }
        Poll::Pending
    })
    .await
}

#[cfg(test)]
#[path = "tests/sync.rs"]
mod tests;
