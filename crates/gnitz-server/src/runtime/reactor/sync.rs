//! Async primitives for the reactor's single thread: `oneshot`, `chan`,
//! `AsyncRwLock` and `select2`.
//!
//! All `!Send` and `Rc<RefCell<_>>`-based on purpose: the reactor never leaves
//! its thread, so a channel send costs no atomic and a waker registration no
//! lock.

use std::cell::RefCell;
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

// ---------------------------------------------------------------------------
// oneshot
// ---------------------------------------------------------------------------
//
// Single-threaded. A receiver resolves only on a send: a sender dropped unsent
// leaves it pending forever.

pub mod oneshot {
    use super::*;

    struct State<T> {
        value: Option<T>,
        waker: Option<Waker>,
    }

    pub struct Sender<T> {
        inner: Rc<RefCell<State<T>>>,
    }
    pub struct Receiver<T> {
        inner: Rc<RefCell<State<T>>>,
    }

    pub fn channel<T>() -> (Sender<T>, Receiver<T>) {
        let s = Rc::new(RefCell::new(State { value: None, waker: None }));
        (Sender { inner: Rc::clone(&s) }, Receiver { inner: s })
    }

    impl<T> Sender<T> {
        /// Send the result. A cancelled receiver is not an error: the value is
        /// parked in state nothing will read, and dropped with the `Rc`.
        pub fn send(self, v: T) {
            let waker = {
                let mut s = self.inner.borrow_mut();
                s.value = Some(v);
                s.waker.take()
            };
            if let Some(w) = waker {
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
            crate::runtime::reactor::park_waker(&mut s.waker, cx.waker());
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

pub mod chan {
    use super::*;
    use crate::runtime::reactor::wake_queue::WakeQueue;

    pub struct Sender<T> {
        inner: Rc<RefCell<WakeQueue<T>>>,
    }
    pub struct Receiver<T> {
        inner: Rc<RefCell<WakeQueue<T>>>,
    }

    pub fn unbounded<T>() -> (Sender<T>, Receiver<T>) {
        let q = Rc::new(RefCell::new(WakeQueue::default()));
        (Sender { inner: Rc::clone(&q) }, Receiver { inner: q })
    }

    impl<T> Sender<T> {
        pub fn send(&self, v: T) {
            self.inner.borrow_mut().push(v);
        }
    }

    impl<T> Receiver<T> {
        pub async fn recv(&mut self) -> T {
            std::future::poll_fn(|cx| self.inner.borrow_mut().poll(cx)).await
        }

        pub fn try_recv(&mut self) -> Option<T> {
            self.inner.borrow_mut().pop()
        }
    }
}

// ---------------------------------------------------------------------------
// AsyncRwLock (writer-preference)
// ---------------------------------------------------------------------------
//
// Payload-free: both guards are drop tokens admitting entry to a critical
// section, and what they protect lives outside. Writer-preference blocks new
// readers as soon as a writer parks, so a writer cannot starve behind a stream
// of readers.

/// A parked writer's waker, shared by its `WriteFuture` and its queue entry.
type WakerSlot = RefCell<Option<Waker>>;

#[derive(Default)]
struct RwLockInner {
    readers: usize,
    has_writer: bool,
    /// Woken as a set: `read_ok` admits all of them or none, so there is nothing
    /// here to choose between. A cancelled reader's waker stays behind, harmlessly.
    read_waiters: VecDeque<Waker>,
    /// Parked `WriteFuture`s, woken one at a time in queue order. A woken entry
    /// keeps its place, its slot emptied, until its future acquires or drops.
    write_waiters: VecDeque<Rc<WakerSlot>>,
}

impl RwLockInner {
    fn unqueue(&mut self, waiter: &Rc<WakerSlot>) {
        self.write_waiters.retain(|w| !Rc::ptr_eq(w, waiter));
    }

    /// A reader may enter: no writer holds the lock and none is waiting, so a
    /// stream of readers cannot starve a writer.
    fn read_ok(&self) -> bool {
        !self.has_writer && self.write_waiters.is_empty()
    }

    /// A writer may enter: nobody holds the lock in either mode.
    fn write_ok(&self) -> bool {
        !self.has_writer && self.readers == 0
    }
}

/// A handle on one lock; clones share it. Every future and guard holds a clone,
/// so none borrows the handle it came from.
#[derive(Clone, Default)]
pub struct AsyncRwLock(Rc<RefCell<RwLockInner>>);

impl AsyncRwLock {
    pub fn read(&self) -> ReadFuture {
        ReadFuture { lock: self.clone() }
    }

    pub fn write(&self) -> WriteFuture {
        WriteFuture { lock: self.clone(), waiter: None }
    }

    /// A writer holds the lock.
    pub fn is_write_held(&self) -> bool {
        self.0.borrow().has_writer
    }

    /// Admit whoever the state now allows: the first parked writer, or every
    /// parked reader when no writer waits.
    fn wake_next(&self) {
        let mut s = self.0.borrow_mut();
        let next = if s.write_ok() {
            s.write_waiters.front().cloned()
        } else {
            None
        };
        if let Some(slot) = next {
            drop(s);
            if let Some(waker) = slot.borrow_mut().take() {
                waker.wake();
            }
        } else if s.read_ok() {
            let queued = std::mem::take(&mut s.read_waiters);
            drop(s);
            for w in queued {
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

    /// True iff nothing holds or waits for the lock. `read_waiters` is not part
    /// of it: it retains stale wakers by design; see its own doc.
    #[cfg(test)]
    pub(super) fn is_quiescent(&self) -> bool {
        let s = self.0.borrow();
        s.readers == 0 && !s.has_writer && s.write_waiters.is_empty()
    }
}

pub struct ReadFuture {
    lock: AsyncRwLock,
}

impl Future for ReadFuture {
    type Output = ReadGuard;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<ReadGuard> {
        let mut s = self.lock.0.borrow_mut();
        if s.read_ok() {
            s.readers += 1;
            return Poll::Ready(ReadGuard { lock: self.lock.clone() });
        }
        s.read_waiters.push_back(cx.waker().clone());
        Poll::Pending
    }
}

pub struct ReadGuard {
    lock: AsyncRwLock,
}

impl Drop for ReadGuard {
    fn drop(&mut self) {
        self.lock.release_read();
    }
}

pub struct WriteFuture {
    lock: AsyncRwLock,
    /// This future's entry in `write_waiters`: `Some` iff it is queued.
    waiter: Option<Rc<WakerSlot>>,
}

impl Future for WriteFuture {
    type Output = WriteGuard;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<WriteGuard> {
        let Self { lock, waiter } = self.get_mut();
        let mut s = lock.0.borrow_mut();
        if s.write_ok() {
            s.has_writer = true;
            if let Some(w) = waiter.take() {
                s.unqueue(&w);
            }
            return Poll::Ready(WriteGuard { lock: lock.clone() });
        }
        match waiter {
            Some(w) => crate::runtime::reactor::park_waker(&mut w.borrow_mut(), cx.waker()),
            None => {
                let w = Rc::new(RefCell::new(Some(cx.waker().clone())));
                s.write_waiters.push_back(Rc::clone(&w));
                *waiter = Some(w);
            }
        }
        Poll::Pending
    }
}

impl Drop for WriteFuture {
    fn drop(&mut self) {
        let Some(waiter) = self.waiter.take() else { return };
        self.lock.0.borrow_mut().unqueue(&waiter);
        // This future may have been the last writer blocking the parked readers.
        self.lock.wake_next();
    }
}

pub struct WriteGuard {
    lock: AsyncRwLock,
}

impl Drop for WriteGuard {
    fn drop(&mut self) {
        self.lock.release_write();
    }
}

// ---------------------------------------------------------------------------
// select2
// ---------------------------------------------------------------------------

/// Result of `select2`: which side resolved first.
pub enum Either<A, B> {
    A(A),
    B(B),
}

/// Race two futures; return whichever completes first. The loser is dropped,
/// and its `Drop` releases what it registered.
pub async fn select2<A, B>(a: A, b: B) -> Either<A::Output, B::Output>
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
