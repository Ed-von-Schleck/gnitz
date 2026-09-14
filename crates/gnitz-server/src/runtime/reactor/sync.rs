//! Async primitives for the reactor's single thread: `oneshot`, `chan`,
//! `AsyncMutex`, `AsyncRwLock` and `select2`.
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
// Single-threaded, cancellable: the committer's per-commit result back to the
// handler that pushed, and the DDL window's release, which is sender-drop alone.

pub mod oneshot {
    use super::*;

    struct State<T> {
        value: Option<T>,
        waker: Option<Waker>,
        sender_alive: bool,
    }

    pub struct Sender<T> {
        inner: Rc<RefCell<State<T>>>,
    }
    pub struct Receiver<T> {
        inner: Rc<RefCell<State<T>>>,
    }

    pub fn channel<T>() -> (Sender<T>, Receiver<T>) {
        let s = Rc::new(RefCell::new(State {
            value: None,
            waker: None,
            sender_alive: true,
        }));
        (Sender { inner: Rc::clone(&s) }, Receiver { inner: s })
    }

    impl<T> Sender<T> {
        /// Send the result. A cancelled receiver is not an error: the value is
        /// parked in state nothing will read, and dropped with the `Rc`.
        pub fn send(self, v: T) {
            let mut s = self.inner.borrow_mut();
            s.value = Some(v);
            if let Some(w) = s.waker.take() {
                w.wake();
            }
        }
    }

    impl<T> Drop for Sender<T> {
        fn drop(&mut self) {
            let mut s = self.inner.borrow_mut();
            s.sender_alive = false;
            if let Some(w) = s.waker.take() {
                w.wake();
            }
        }
    }

    impl<T> Future for Receiver<T> {
        /// `None` once the sender is gone without having sent — which the DDL
        /// tick gate uses as its release signal rather than an explicit value.
        type Output = Option<T>;
        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<T>> {
            let mut s = self.inner.borrow_mut();
            if let Some(v) = s.value.take() {
                return Poll::Ready(Some(v));
            }
            if !s.sender_alive {
                return Poll::Ready(None);
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
// The committer's and the tick loop's request channel. `chan`, not `spsc`:
// many tasks send, reaching the one `Sender` through a shared `Rc<Shared>`.
// It is that one — `Sender` is not cloneable — so its drop closes the queue and
// the receiver's next `recv` resolves to `None`. Neither loop exits that way in
// practice: `Shared` owns the sender and each loop's task holds an `Rc<Shared>`
// for its whole life, so both exit by reactor shutdown dropping the task.

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

    impl<T> Drop for Sender<T> {
        fn drop(&mut self) {
            self.inner.borrow_mut().close();
        }
    }

    impl<T> Receiver<T> {
        pub async fn recv(&mut self) -> Option<T> {
            std::future::poll_fn(|cx| self.inner.borrow_mut().poll(cx)).await
        }

        /// Non-blocking receive: returns `Some(T)` if the queue has an item,
        /// `None` otherwise. Never awaits — which is what lets the committer
        /// batch whatever is already pipelined behind a request without waiting
        /// on a timer for more.
        pub fn try_recv(&mut self) -> Option<T> {
            self.inner.borrow_mut().pop()
        }
    }
}

// ---------------------------------------------------------------------------
// AsyncMutex
// ---------------------------------------------------------------------------

/// Mutual exclusion with no payload: the guard is a drop token, and what it
/// protects lives outside. Exposing no shared mode is the point — the SAL
/// writer and the TLS send path would both compile, and both break, given one.
#[derive(Default)]
pub struct AsyncMutex(Rc<AsyncRwLock>);

impl AsyncMutex {
    pub fn lock(&self) -> WriteFuture {
        self.0.write()
    }
}

// ---------------------------------------------------------------------------
// AsyncRwLock (writer-preference)
// ---------------------------------------------------------------------------
//
// Payload-free, like `AsyncMutex`: both guards are drop tokens admitting
// entry to a critical section, and what they protect lives outside.
// Writer-preference blocks new readers as soon as a writer parks, so a
// writer cannot starve behind a stream of readers.

#[derive(Default)]
struct RwLockInner {
    readers: usize,
    has_writer: bool,
    /// Live parked `WriteFuture`s, counted off each future's own `parked` flag
    /// and not off `write_waiters.len()`, which drops to 0 a wake before they do.
    writers_waiting: usize,
    /// Wakers of parked futures. A cancelled future leaves its entry behind, so
    /// these hold stale wakers at rest — never assert one empty, and never hand
    /// the lock to a single entry out of one. [`AsyncRwLock::wake_next`] takes
    /// the whole queue, which is what makes a stale entry harmless.
    read_waiters: VecDeque<Waker>,
    write_waiters: VecDeque<Waker>,
}

impl RwLockInner {
    /// A reader may enter: no writer holds the lock and none is waiting, so a
    /// stream of readers cannot starve a writer.
    fn read_ok(&self) -> bool {
        !self.has_writer && self.writers_waiting == 0
    }

    /// A writer may enter: nobody holds the lock in either mode.
    fn write_ok(&self) -> bool {
        !self.has_writer && self.readers == 0
    }
}

#[derive(Default)]
pub struct AsyncRwLock {
    inner: RefCell<RwLockInner>,
}

impl AsyncRwLock {
    pub fn read(self: &Rc<Self>) -> ReadFuture {
        ReadFuture { lock: Rc::clone(self) }
    }

    pub fn write(self: &Rc<Self>) -> WriteFuture {
        WriteFuture { lock: Rc::clone(self), parked: false }
    }

    /// Wake every future the current state now admits — writers first, readers
    /// only when no writer waits. The whole queue, since it may hold stale
    /// wakers; the first woken task to poll acquires and the rest re-park.
    ///
    /// Called by every transition that can admit someone: the two guard
    /// releases and a cancelled `WriteFuture`. Miss one and its waiters park
    /// until the next release, which for a reader behind the catalog lock means
    /// until the next DDL.
    fn wake_next(&self) {
        // Scoped so the borrow cannot span the wakes: a wake re-enters the run
        // queue and can drive a poll that borrows this state again.
        let wakers = {
            let mut s = self.inner.borrow_mut();
            if s.write_ok() && s.writers_waiting > 0 {
                std::mem::take(&mut s.write_waiters)
            } else if s.read_ok() {
                std::mem::take(&mut s.read_waiters)
            } else {
                VecDeque::new()
            }
        };
        for w in wakers {
            w.wake();
        }
    }

    fn release_read(&self) {
        self.inner.borrow_mut().readers -= 1;
        self.wake_next();
    }

    fn release_write(&self) {
        self.inner.borrow_mut().has_writer = false;
        self.wake_next();
    }

    /// True iff nothing holds or waits for the lock. The waiter queues are not
    /// part of it — they retain stale wakers by design; see their own doc.
    #[cfg(test)]
    pub(super) fn is_quiescent(&self) -> bool {
        let s = self.inner.borrow();
        s.readers == 0 && !s.has_writer && s.writers_waiting == 0
    }
}

pub struct ReadFuture {
    lock: Rc<AsyncRwLock>,
}

impl Future for ReadFuture {
    type Output = ReadGuard;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<ReadGuard> {
        let mut s = self.lock.inner.borrow_mut();
        if s.read_ok() {
            s.readers += 1;
            return Poll::Ready(ReadGuard { lock: Rc::clone(&self.lock) });
        }
        s.read_waiters.push_back(cx.waker().clone());
        Poll::Pending
    }
}

pub struct ReadGuard {
    lock: Rc<AsyncRwLock>,
}

impl Drop for ReadGuard {
    fn drop(&mut self) {
        self.lock.release_read();
    }
}

pub struct WriteFuture {
    lock: Rc<AsyncRwLock>,
    /// Whether this future is counted in `writers_waiting`. Re-polling a parked
    /// future must not count it twice, and a `select2` re-polls a parked future
    /// on every wake.
    parked: bool,
}

impl Future for WriteFuture {
    type Output = WriteGuard;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<WriteGuard> {
        // Split borrow rather than an `Rc` clone: `parked` is written while
        // `lock.inner` is borrowed, and the two fields are disjoint.
        let Self { lock, parked } = self.get_mut();
        let mut s = lock.inner.borrow_mut();
        if s.write_ok() {
            s.has_writer = true;
            if *parked {
                s.writers_waiting -= 1;
                *parked = false;
            }
            return Poll::Ready(WriteGuard { lock: Rc::clone(lock) });
        }
        if !*parked {
            s.writers_waiting += 1;
            *parked = true;
        }
        s.write_waiters.push_back(cx.waker().clone());
        Poll::Pending
    }
}

impl Drop for WriteFuture {
    fn drop(&mut self) {
        if !self.parked {
            return;
        }
        self.lock.inner.borrow_mut().writers_waiting -= 1;
        self.lock.wake_next();
    }
}

pub struct WriteGuard {
    lock: Rc<AsyncRwLock>,
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
    // Stack-pinned (`pin!`), not `Box::pin`: select2 runs per client egress
    // frame, so two heap allocations per call are worth avoiding.
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
