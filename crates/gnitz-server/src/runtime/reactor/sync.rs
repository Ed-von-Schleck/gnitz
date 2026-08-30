//! Async primitives for the reactor's single thread: `oneshot`, `mpsc`,
//! `AsyncMutex`, `AsyncRwLock`, `join_all_unpin` / `join_into` and `select2`.
//!
//! All `!Send` and `Rc<RefCell<_>>`-based on purpose: the reactor never leaves
//! its thread, so a channel send costs no atomic and a waker registration no
//! lock.

use std::cell::{Cell, RefCell, RefMut};
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

/// Push `waker` into `q` unless a waker already in the queue would wake
/// the same task. A `LockFuture` / `ReadFuture` / `WriteFuture` that is
/// polled N times before the lock is released would otherwise enqueue N
/// wakers for the same task; release cycles then wake that task N times
/// (wasted polls) and the reactor's run queue gets N duplicate entries.
/// `will_wake` is a cheap pointer comparison on the waker vtable + data.
fn push_unique_waker(q: &mut VecDeque<Waker>, waker: &Waker) {
    if !q.iter().any(|existing| existing.will_wake(waker)) {
        q.push_back(waker.clone());
    }
}

// ---------------------------------------------------------------------------
// oneshot
// ---------------------------------------------------------------------------
//
// Single-threaded, cancellable. Used by the committer to send per-commit
// results back to the handler that produced the push.

pub mod oneshot {
    use super::*;

    #[derive(Debug, PartialEq, Eq)]
    pub struct Cancelled;

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
        type Output = Result<T, Cancelled>;
        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            let mut s = self.inner.borrow_mut();
            if let Some(v) = s.value.take() {
                return Poll::Ready(Ok(v));
            }
            if !s.sender_alive {
                return Poll::Ready(Err(Cancelled));
            }
            s.waker = Some(cx.waker().clone());
            Poll::Pending
        }
    }
}

// ---------------------------------------------------------------------------
// mpsc (unbounded)
// ---------------------------------------------------------------------------
//
// Used as the committer's request channel. Senders are cloneable; Drop
// of the last sender returns `None` from the receiver.

pub mod mpsc {
    use super::*;
    use crate::runtime::reactor::wake_queue::WakeQueue;

    struct State<T> {
        queue: WakeQueue<T>,
        senders: usize,
    }

    pub struct Sender<T> {
        inner: Rc<RefCell<State<T>>>,
    }
    pub struct Receiver<T> {
        inner: Rc<RefCell<State<T>>>,
    }

    pub fn unbounded<T>() -> (Sender<T>, Receiver<T>) {
        let s = Rc::new(RefCell::new(State {
            queue: WakeQueue::default(),
            senders: 1,
        }));
        (Sender { inner: Rc::clone(&s) }, Receiver { inner: s })
    }

    impl<T> Clone for Sender<T> {
        fn clone(&self) -> Self {
            self.inner.borrow_mut().senders += 1;
            Sender {
                inner: Rc::clone(&self.inner),
            }
        }
    }

    impl<T> Sender<T> {
        pub fn send(&self, v: T) {
            self.inner.borrow_mut().queue.push(v);
        }
    }

    impl<T> Drop for Sender<T> {
        fn drop(&mut self) {
            let mut s = self.inner.borrow_mut();
            s.senders -= 1;
            if s.senders == 0 {
                s.queue.close();
            }
        }
    }

    impl<T> Receiver<T> {
        pub fn recv(&mut self) -> RecvOne<'_, T> {
            RecvOne { inner: &self.inner }
        }

        /// Non-blocking receive: returns `Some(T)` if the queue has an
        /// item, `None` otherwise. Never awaits. Used by the committer
        /// to drain pipelined requests without paying the 1ms debounce
        /// timer when nothing more is available.
        pub fn try_recv(&mut self) -> Option<T> {
            self.inner.borrow_mut().queue.pop()
        }
    }

    pub struct RecvOne<'a, T> {
        inner: &'a Rc<RefCell<State<T>>>,
    }

    impl<T> Future for RecvOne<'_, T> {
        type Output = Option<T>;
        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<T>> {
            self.inner.borrow_mut().queue.poll(cx)
        }
    }
}

// ---------------------------------------------------------------------------
// AsyncMutex
// ---------------------------------------------------------------------------

/// Mutual exclusion with no payload: the guard is a drop token, and what it
/// protects lives outside. Used where one task at a time must run a critical
/// section that awaits inside it (SAL writer, client egress).
pub struct AsyncMutex {
    locked: Cell<bool>,
    waiters: RefCell<VecDeque<Waker>>,
}

impl AsyncMutex {
    pub fn new() -> Self {
        AsyncMutex {
            locked: Cell::new(false),
            waiters: RefCell::new(VecDeque::new()),
        }
    }

    pub fn lock(self: &Rc<Self>) -> LockFuture {
        LockFuture { mutex: Rc::clone(self) }
    }

    fn release(&self) {
        self.locked.set(false);
        // Wake all waiters, not just one — same reason as `pass_baton`: a
        // cancelled LockFuture can leave a stale waker in the queue, and
        // popping exactly one risks handing the lock to it forever.
        let waiters = std::mem::take(&mut *self.waiters.borrow_mut());
        for w in waiters {
            w.wake();
        }
    }
}

impl Default for AsyncMutex {
    fn default() -> Self {
        Self::new()
    }
}

pub struct LockFuture {
    mutex: Rc<AsyncMutex>,
}

impl Future for LockFuture {
    type Output = LockGuard;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<LockGuard> {
        if !self.mutex.locked.get() {
            self.mutex.locked.set(true);
            return Poll::Ready(LockGuard {
                mutex: Rc::clone(&self.mutex),
            });
        }
        push_unique_waker(&mut self.mutex.waiters.borrow_mut(), cx.waker());
        Poll::Pending
    }
}

pub struct LockGuard {
    mutex: Rc<AsyncMutex>,
}

impl Drop for LockGuard {
    fn drop(&mut self) {
        self.mutex.release();
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

struct RwLockInner {
    readers: usize,
    has_writer: bool,
    writers_waiting: usize,
    read_waiters: VecDeque<Waker>,
    write_waiters: VecDeque<Waker>,
}

pub struct AsyncRwLock {
    inner: RefCell<RwLockInner>,
}

impl AsyncRwLock {
    pub fn new() -> Self {
        AsyncRwLock {
            inner: RefCell::new(RwLockInner {
                readers: 0,
                has_writer: false,
                writers_waiting: 0,
                read_waiters: VecDeque::new(),
                write_waiters: VecDeque::new(),
            }),
        }
    }

    pub fn read(self: &Rc<Self>) -> ReadFuture {
        ReadFuture { lock: Rc::clone(self) }
    }

    pub fn write(self: &Rc<Self>) -> WriteFuture {
        WriteFuture {
            lock: Rc::clone(self),
            parked: false,
        }
    }

    fn release_read(&self) {
        let mut s = self.inner.borrow_mut();
        s.readers -= 1;
        if s.readers == 0 && s.writers_waiting > 0 {
            wake_all(std::mem::take(&mut s.write_waiters), s);
        }
    }

    fn release_write(&self) {
        let mut s = self.inner.borrow_mut();
        s.has_writer = false;
        pass_baton(s);
    }
}

/// Drop the lock-state borrow, then wake. Waking re-enters the reactor's run
/// queue and may drive a poll that borrows this state again, so the borrow must
/// be gone first.
fn wake_all(wakers: VecDeque<Waker>, state: RefMut<'_, RwLockInner>) {
    drop(state);
    for w in wakers {
        w.wake();
    }
}

/// Hand the lock on to whoever is next: queued writers first (writer
/// preference), readers only when none remain. Wakes *all* candidates rather
/// than one — a cancelled future can leave a stale waker in the queue, and
/// handing the baton to that one alone would block every live waiter forever.
/// On a single-threaded executor the thundering herd is free: the first task to
/// poll acquires and the rest re-park.
fn pass_baton(mut state: RefMut<'_, RwLockInner>) {
    let writers = std::mem::take(&mut state.write_waiters);
    if !writers.is_empty() {
        return wake_all(writers, state);
    }
    let readers = std::mem::take(&mut state.read_waiters);
    wake_all(readers, state);
}

impl Default for AsyncRwLock {
    fn default() -> Self {
        Self::new()
    }
}

pub struct ReadFuture {
    lock: Rc<AsyncRwLock>,
}

impl Future for ReadFuture {
    type Output = ReadGuard;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<ReadGuard> {
        let mut s = self.lock.inner.borrow_mut();
        if !s.has_writer && s.writers_waiting == 0 {
            s.readers += 1;
            return Poll::Ready(ReadGuard {
                lock: Rc::clone(&self.lock),
            });
        }
        push_unique_waker(&mut s.read_waiters, cx.waker());
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
    parked: bool,
}

impl Future for WriteFuture {
    type Output = WriteGuard;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<WriteGuard> {
        // `lock` is a local clone, so borrowing through it does not alias
        // `self` — the parked bookkeeping needs no dance around the borrow.
        let lock = Rc::clone(&self.lock);
        let mut s = lock.inner.borrow_mut();
        if !s.has_writer && s.readers == 0 {
            s.has_writer = true;
            if self.parked {
                s.writers_waiting -= 1;
                self.parked = false;
            }
            return Poll::Ready(WriteGuard { lock: Rc::clone(&lock) });
        }
        if !self.parked {
            s.writers_waiting += 1;
            self.parked = true;
        }
        push_unique_waker(&mut s.write_waiters, cx.waker());
        Poll::Pending
    }
}

impl Drop for WriteFuture {
    fn drop(&mut self) {
        if !self.parked {
            return;
        }
        let mut s = self.lock.inner.borrow_mut();
        s.writers_waiting -= 1;
        if s.has_writer {
            // Another writer holds the lock; it passes the baton on release.
            return;
        }
        if s.readers == 0 {
            pass_baton(s);
        } else if s.writers_waiting == 0 {
            // Readers hold the lock and this was the last live write waiter, so
            // readers blocked by `writers_waiting > 0` can now enter.
            wake_all(std::mem::take(&mut s.read_waiters), s);
        }
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
// join_all
// ---------------------------------------------------------------------------

/// Future driving `futs` to completion, writing values in input order
/// into `out`. Both buffers are caller-supplied; no internal allocation
/// happens once their capacity is large enough.
///
/// The caller-supplied scratch shape lets the committer/executor reuse
/// the same `Vec<F>` and `Vec<Option<T>>` across every commit/tick,
/// eliminating the three transient allocations that the old `join_all`
/// performed on every call (boxed futures, option slots, result vec).
pub struct JoinInto<'a, F, T> {
    futs: &'a mut Vec<F>,
    out: &'a mut Vec<Option<T>>,
}

impl<'a, F: Future<Output = T> + Unpin, T> Future for JoinInto<'a, F, T> {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let this = &mut *self;
        let n = this.futs.len();
        let mut remaining = 0;
        for i in 0..n {
            if this.out[i].is_some() {
                continue;
            }
            match Pin::new(&mut this.futs[i]).poll(cx) {
                Poll::Ready(v) => {
                    this.out[i] = Some(v);
                }
                Poll::Pending => {
                    remaining += 1;
                }
            }
        }
        if remaining == 0 {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

/// Drive every future in `futs` to completion, writing each result into
/// the same index in `out`. `out` is cleared and resized to `futs.len()`
/// on entry; allocation only happens when its capacity is too small.
pub fn join_into<'a, F, T>(futs: &'a mut Vec<F>, out: &'a mut Vec<Option<T>>) -> JoinInto<'a, F, T>
where
    F: Future<Output = T> + Unpin,
{
    let n = futs.len();
    out.clear();
    out.resize_with(n, || None);
    JoinInto { futs, out }
}

/// Drive every future in `futs` to completion, return values in input order.
/// Requires `F: Unpin`. Every production caller passes `ReplyFuture` /
/// `ScanSlotFuture` (both `Unpin`); non-`Unpin` callers must `Box::pin`
/// at the call site.
pub async fn join_all_unpin<F, T, I>(futs: I) -> Vec<T>
where
    I: IntoIterator<Item = F>,
    F: Future<Output = T> + Unpin,
{
    let mut futs: Vec<F> = futs.into_iter().collect();
    let mut out: Vec<Option<T>> = Vec::new();
    join_into(&mut futs, &mut out).await;
    out.into_iter().map(|o| o.unwrap()).collect()
}

// ---------------------------------------------------------------------------
// select2
// ---------------------------------------------------------------------------

/// Result of `select2`: which side resolved first.
pub enum Either<A, B> {
    A(A),
    B(B),
}

/// Race two futures; return whichever completes first.  The loser is
/// dropped — its `Drop` impl is responsible for releasing any registered
/// state (e.g. `TimerFuture::Drop` flips the cancellation bit on its
/// heap entry so the timer loop skips it).
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
