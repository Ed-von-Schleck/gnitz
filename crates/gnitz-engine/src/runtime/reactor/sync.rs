use std::cell::{Cell, RefCell};
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
        receiver_alive: bool,
    }

    pub struct Sender<T> { inner: Rc<RefCell<State<T>>> }
    pub struct Receiver<T> { inner: Rc<RefCell<State<T>>> }

    pub fn channel<T>() -> (Sender<T>, Receiver<T>) {
        let s = Rc::new(RefCell::new(State {
            value: None, waker: None, sender_alive: true, receiver_alive: true,
        }));
        (Sender { inner: Rc::clone(&s) }, Receiver { inner: s })
    }

    impl<T> Sender<T> {
        /// Attempt to send. Returns `Err(v)` if the receiver has been
        /// dropped (cancelled).
        pub fn send(self, v: T) -> Result<(), T> {
            let mut s = self.inner.borrow_mut();
            if !s.receiver_alive {
                return Err(v);
            }
            s.value = Some(v);
            if let Some(w) = s.waker.take() { w.wake(); }
            Ok(())
        }
    }

    impl<T> Drop for Sender<T> {
        fn drop(&mut self) {
            let mut s = self.inner.borrow_mut();
            s.sender_alive = false;
            if let Some(w) = s.waker.take() { w.wake(); }
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

    impl<T> Drop for Receiver<T> {
        fn drop(&mut self) {
            self.inner.borrow_mut().receiver_alive = false;
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

    struct State<T> {
        queue: VecDeque<T>,
        waker: Option<Waker>,
        senders: usize,
    }

    pub struct Sender<T> { inner: Rc<RefCell<State<T>>> }
    pub struct Receiver<T> { inner: Rc<RefCell<State<T>>> }

    pub fn unbounded<T>() -> (Sender<T>, Receiver<T>) {
        let s = Rc::new(RefCell::new(State {
            queue: VecDeque::new(), waker: None, senders: 1,
        }));
        (Sender { inner: Rc::clone(&s) }, Receiver { inner: s })
    }

    impl<T> Clone for Sender<T> {
        fn clone(&self) -> Self {
            self.inner.borrow_mut().senders += 1;
            Sender { inner: Rc::clone(&self.inner) }
        }
    }

    impl<T> Sender<T> {
        pub fn send(&self, v: T) {
            let mut s = self.inner.borrow_mut();
            s.queue.push_back(v);
            if let Some(w) = s.waker.take() { w.wake(); }
        }
    }

    impl<T> Drop for Sender<T> {
        fn drop(&mut self) {
            let mut s = self.inner.borrow_mut();
            s.senders -= 1;
            if s.senders == 0 {
                if let Some(w) = s.waker.take() { w.wake(); }
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
            self.inner.borrow_mut().queue.pop_front()
        }
    }

    pub struct RecvOne<'a, T> {
        inner: &'a Rc<RefCell<State<T>>>,
    }

    impl<'a, T> Future for RecvOne<'a, T> {
        type Output = Option<T>;
        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<T>> {
            let mut s = self.inner.borrow_mut();
            if let Some(v) = s.queue.pop_front() {
                return Poll::Ready(Some(v));
            }
            if s.senders == 0 {
                return Poll::Ready(None);
            }
            s.waker = Some(cx.waker().clone());
            Poll::Pending
        }
    }
}

// ---------------------------------------------------------------------------
// AsyncMutex
// ---------------------------------------------------------------------------

pub struct AsyncMutex<T> {
    locked: Cell<bool>,
    value: RefCell<T>,
    waiters: RefCell<VecDeque<Waker>>,
}

impl<T> AsyncMutex<T> {
    pub fn new(v: T) -> Self {
        AsyncMutex {
            locked: Cell::new(false),
            value: RefCell::new(v),
            waiters: RefCell::new(VecDeque::new()),
        }
    }

    pub fn lock<'a>(self: &'a Rc<Self>) -> LockFuture<'a, T> {
        LockFuture { mutex: Rc::clone(self), _p: std::marker::PhantomData }
    }

    fn release(&self) {
        self.locked.set(false);
        // Wake all waiters, not just one. A cancelled LockFuture leaves a
        // stale waker in the queue; popping exactly one risks consuming
        // that stale entry and permanently blocking every live waiter.
        // On a single-threaded executor the thundering-herd cost is free:
        // only the first task to poll acquires the lock; the rest re-park.
        let waiters = std::mem::take(&mut *self.waiters.borrow_mut());
        for w in waiters { w.wake(); }
    }
}

pub struct LockFuture<'a, T> {
    mutex: Rc<AsyncMutex<T>>,
    _p: std::marker::PhantomData<&'a ()>,
}

impl<'a, T: 'a> Future for LockFuture<'a, T> {
    type Output = LockGuard<T>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<LockGuard<T>> {
        if !self.mutex.locked.get() {
            self.mutex.locked.set(true);
            return Poll::Ready(LockGuard { mutex: Rc::clone(&self.mutex) });
        }
        push_unique_waker(&mut self.mutex.waiters.borrow_mut(), cx.waker());
        Poll::Pending
    }
}

pub struct LockGuard<T> {
    mutex: Rc<AsyncMutex<T>>,
}

impl<T> std::ops::Deref for LockGuard<T> {
    type Target = RefCell<T>;
    fn deref(&self) -> &Self::Target { &self.mutex.value }
}

impl<T> Drop for LockGuard<T> {
    fn drop(&mut self) { self.mutex.release(); }
}

// ---------------------------------------------------------------------------
// AsyncRwLock (writer-preference)
// ---------------------------------------------------------------------------
//
// `()`-valued: used as a catalog-wide barrier. Readers hold a read guard
// during INSERT; the DDL path holds a write guard during catalog
// mutation. Writer-preference blocks new readers as soon as a writer
// parks, guaranteeing DDL doesn't starve.

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
                readers: 0, has_writer: false, writers_waiting: 0,
                read_waiters: VecDeque::new(),
                write_waiters: VecDeque::new(),
            }),
        }
    }

    pub fn read<'a>(self: &'a Rc<Self>) -> ReadFuture<'a> {
        ReadFuture { lock: Rc::clone(self), _p: std::marker::PhantomData }
    }

    pub fn write<'a>(self: &'a Rc<Self>) -> WriteFuture<'a> {
        WriteFuture { lock: Rc::clone(self), parked: false, _p: std::marker::PhantomData }
    }

    fn release_read(&self) {
        let mut s = self.inner.borrow_mut();
        s.readers -= 1;
        if s.readers == 0 && s.writers_waiting > 0 {
            // Wake all queued write waiters. A single pop risks handing the
            // baton to a stale waker from a cancelled WriteFuture, leaving
            // every live write waiter permanently blocked.
            let writers = std::mem::take(&mut s.write_waiters);
            drop(s);
            for w in writers { w.wake(); }
        }
    }

    fn release_write(&self) {
        let mut s = self.inner.borrow_mut();
        s.has_writer = false;
        // Writer-preference: wake all queued writers before any readers.
        // A single pop risks handing the baton to a stale waker from a
        // cancelled WriteFuture, leaving every live write waiter blocked.
        let writers = std::mem::take(&mut s.write_waiters);
        if !writers.is_empty() {
            drop(s);
            for w in writers { w.wake(); }
            return;
        }
        // No queued writer — wake all parked readers.
        let readers = std::mem::take(&mut s.read_waiters);
        drop(s);
        for w in readers { w.wake(); }
    }
}

impl Default for AsyncRwLock {
    fn default() -> Self { Self::new() }
}

pub struct ReadFuture<'a> {
    lock: Rc<AsyncRwLock>,
    _p: std::marker::PhantomData<&'a ()>,
}

impl<'a> Future for ReadFuture<'a> {
    type Output = ReadGuard;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<ReadGuard> {
        let mut s = self.lock.inner.borrow_mut();
        if !s.has_writer && s.writers_waiting == 0 {
            s.readers += 1;
            return Poll::Ready(ReadGuard { lock: Rc::clone(&self.lock) });
        }
        push_unique_waker(&mut s.read_waiters, cx.waker());
        Poll::Pending
    }
}

pub struct ReadGuard {
    lock: Rc<AsyncRwLock>,
}

impl Drop for ReadGuard {
    fn drop(&mut self) { self.lock.release_read(); }
}

pub struct WriteFuture<'a> {
    lock: Rc<AsyncRwLock>,
    parked: bool,
    _p: std::marker::PhantomData<&'a ()>,
}

impl<'a> Future for WriteFuture<'a> {
    type Output = WriteGuard;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<WriteGuard> {
        let was_parked = self.parked;
        let lock = Rc::clone(&self.lock);
        let mut s = lock.inner.borrow_mut();
        if !s.has_writer && s.readers == 0 {
            s.has_writer = true;
            if was_parked {
                s.writers_waiting -= 1;
                drop(s);
                self.parked = false;
            }
            return Poll::Ready(WriteGuard { lock: Rc::clone(&self.lock) });
        }
        if !was_parked {
            s.writers_waiting += 1;
            drop(s);
            self.parked = true;
            push_unique_waker(
                &mut lock.inner.borrow_mut().write_waiters, cx.waker());
        } else {
            push_unique_waker(&mut s.write_waiters, cx.waker());
        }
        Poll::Pending
    }
}

impl<'a> Drop for WriteFuture<'a> {
    fn drop(&mut self) {
        if self.parked {
            let mut s = self.lock.inner.borrow_mut();
            s.writers_waiting -= 1;
            if s.has_writer {
                // Another writer holds the lock; it passes the baton on release.
                return;
            }
            if s.readers == 0 {
                // Lock is completely free. Pass the baton: wake any remaining
                // write waiters (some may be stale from prior cancellations;
                // stale wakes are harmless, live ones will acquire), and fall
                // through to read waiters only when no write waiters remain.
                let writers = std::mem::take(&mut s.write_waiters);
                if !writers.is_empty() {
                    drop(s);
                    for w in writers { w.wake(); }
                    return;
                }
                let readers = std::mem::take(&mut s.read_waiters);
                drop(s);
                for w in readers { w.wake(); }
            } else if s.writers_waiting == 0 {
                // Readers hold the lock; this was the last live write waiter.
                // Readers blocked by `writers_waiting > 0` can now enter.
                let readers = std::mem::take(&mut s.read_waiters);
                drop(s);
                for w in readers { w.wake(); }
            }
        }
    }
}

pub struct WriteGuard {
    lock: Rc<AsyncRwLock>,
}

impl Drop for WriteGuard {
    fn drop(&mut self) { self.lock.release_write(); }
}

// ---------------------------------------------------------------------------
// join2 / join_all
// ---------------------------------------------------------------------------

#[cfg(test)]
pub async fn join2<A, B>(a: A, b: B) -> (A::Output, B::Output)
where
    A: Future,
    B: Future,
{
    let mut a = Box::pin(a);
    let mut b = Box::pin(b);
    let mut a_out: Option<A::Output> = None;
    let mut b_out: Option<B::Output> = None;
    std::future::poll_fn(move |cx| {
        if a_out.is_none() {
            if let Poll::Ready(v) = a.as_mut().poll(cx) { a_out = Some(v); }
        }
        if b_out.is_none() {
            if let Poll::Ready(v) = b.as_mut().poll(cx) { b_out = Some(v); }
        }
        if a_out.is_some() && b_out.is_some() {
            Poll::Ready((a_out.take().unwrap(), b_out.take().unwrap()))
        } else {
            Poll::Pending
        }
    }).await
}

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
            if this.out[i].is_some() { continue; }
            match Pin::new(&mut this.futs[i]).poll(cx) {
                Poll::Ready(v) => { this.out[i] = Some(v); }
                Poll::Pending => { remaining += 1; }
            }
        }
        if remaining == 0 { Poll::Ready(()) } else { Poll::Pending }
    }
}

/// Drive every future in `futs` to completion, writing each result into
/// the same index in `out`. `out` is cleared and resized to `futs.len()`
/// on entry; allocation only happens when its capacity is too small.
pub fn join_into<'a, F, T>(
    futs: &'a mut Vec<F>,
    out: &'a mut Vec<Option<T>>,
) -> JoinInto<'a, F, T>
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
    let mut a = Box::pin(a);
    let mut b = Box::pin(b);
    std::future::poll_fn(move |cx| {
        if let Poll::Ready(v) = a.as_mut().poll(cx) {
            return Poll::Ready(Either::A(v));
        }
        if let Poll::Ready(v) = b.as_mut().poll(cx) {
            return Poll::Ready(Either::B(v));
        }
        Poll::Pending
    }).await
}
