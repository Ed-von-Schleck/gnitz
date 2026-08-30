//! The single-threaded async primitives: `oneshot`, `mpsc`, `AsyncMutex`,
//! `AsyncRwLock`, `join_all_unpin` and the cancellation shapes each must
//! survive.

use std::cell::Cell as StdCell;

use super::super::test_support::*;
use super::super::*;

// ------------------------------------------------------------------
// Primitives: oneshot / mpsc / AsyncMutex / AsyncRwLock
// ------------------------------------------------------------------

#[test]
fn oneshot_deliver_value() {
    let r = make_reactor();
    let got: Rc<StdCell<i32>> = Rc::new(StdCell::new(0));
    let got2 = Rc::clone(&got);
    let (tx, rx) = oneshot::channel::<i32>();
    r.spawn(async move {
        let v = rx.await.unwrap();
        got2.set(v);
    });
    // Drive one tick so the receiver registers its waker, then send.
    r.tick(false);
    tx.send(42);
    r.block_until_idle();
    assert_eq!(got.get(), 42);
}

#[test]
fn oneshot_sender_drop_cancels() {
    let r = make_reactor();
    let err: Rc<StdCell<bool>> = Rc::new(StdCell::new(false));
    let err2 = Rc::clone(&err);
    let (tx, rx) = oneshot::channel::<i32>();
    r.spawn(async move {
        let res = rx.await;
        err2.set(res.is_err());
    });
    r.tick(false);
    drop(tx);
    r.block_until_idle();
    assert!(err.get(), "dropping sender must produce Cancelled");
}

/// Sending to a cancelled receiver is not an error and never was to any caller:
/// the value is parked where nothing will read it and dropped with the channel.
#[test]
fn oneshot_send_to_dropped_receiver_is_a_noop() {
    let dropped: Rc<StdCell<bool>> = Rc::new(StdCell::new(false));
    struct Tattle(Rc<StdCell<bool>>);
    impl Drop for Tattle {
        fn drop(&mut self) {
            self.0.set(true);
        }
    }
    let (tx, rx) = oneshot::channel::<Tattle>();
    drop(rx);
    tx.send(Tattle(Rc::clone(&dropped)));
    assert!(dropped.get(), "the undeliverable value must be dropped, not leaked");
}

#[test]
fn mpsc_send_then_recv() {
    let r = make_reactor();
    let got: Rc<RefCell<Vec<i32>>> = Rc::new(RefCell::new(Vec::new()));
    let got2 = Rc::clone(&got);
    let (tx, mut rx) = mpsc::unbounded::<i32>();
    tx.send(1);
    tx.send(2);
    drop(tx);
    r.block_on(async move {
        while let Some(v) = rx.recv().await {
            got2.borrow_mut().push(v);
        }
    });
    assert_eq!(*got.borrow(), vec![1, 2]);
}

#[test]
fn mpsc_multi_senders() {
    let r = make_reactor();
    let got: Rc<RefCell<Vec<i32>>> = Rc::new(RefCell::new(Vec::new()));
    let got2 = Rc::clone(&got);
    let (tx, mut rx) = mpsc::unbounded::<i32>();
    let tx2 = tx.clone();
    tx.send(10);
    tx2.send(20);
    drop(tx);
    drop(tx2);
    r.block_on(async move {
        while let Some(v) = rx.recv().await {
            got2.borrow_mut().push(v);
        }
    });
    let mut g = got.borrow().clone();
    g.sort();
    assert_eq!(g, vec![10, 20]);
}

#[test]
fn async_mutex_serializes_access() {
    let r = make_reactor();
    let order: Rc<RefCell<Vec<u32>>> = Rc::new(RefCell::new(Vec::new()));
    let mutex: Rc<AsyncMutex> = Rc::new(AsyncMutex::new());
    for i in 0u32..3 {
        let m = Rc::clone(&mutex);
        let ord = Rc::clone(&order);
        r.spawn(async move {
            let g = m.lock().await;
            let len = ord.borrow().len();
            ord.borrow_mut().push(i);
            // Fail loudly if another task entered the section while this
            // one held the lock.
            assert_eq!(ord.borrow().len(), len + 1);
            drop(g);
        });
    }
    r.block_until_idle();
    assert_eq!(*order.borrow(), vec![0, 1, 2], "tasks must serialize, in lock order");
}

/// Structural regression: a task that acquires the SAL writer mutex,
/// writes, drops the guard, then awaits must release the mutex
/// before that await — so a concurrent relay/tick task can acquire
/// it while the first task's await is outstanding (committer pattern:
/// emit under lock, `.await` outside).
#[test]
fn sal_writer_excl_not_held_across_commit_await() {
    let r = make_reactor();
    let mutex: Rc<AsyncMutex> = Rc::new(AsyncMutex::new());
    // One-shot channel used as a stand-in for "fsync CQE / worker
    // ACK": the fake committer awaits `ack_rx`; the other task
    // sends on `ack_tx` AFTER acquiring the mutex. If the committer
    // was still holding the mutex, it would deadlock because
    // neither would make progress.
    let (ack_tx, ack_rx) = oneshot::channel::<()>();
    let commit_done: Rc<StdCell<bool>> = Rc::new(StdCell::new(false));
    let relay_done: Rc<StdCell<bool>> = Rc::new(StdCell::new(false));

    let m1 = Rc::clone(&mutex);
    let cd = Rc::clone(&commit_done);
    r.spawn(async move {
        // Emit under lock: identical pattern to the new committer.
        {
            let _guard = m1.lock().await;
            // ...SAL writes would go here...
        }
        // Lock dropped. Now wait for "fsync + ACK".
        let _ = ack_rx.await;
        cd.set(true);
    });

    let m2 = Rc::clone(&mutex);
    let rd = Rc::clone(&relay_done);
    r.spawn(async move {
        // This future MUST make progress while the committer is
        // awaiting ack_rx — proving the mutex was released.
        let _guard = m2.lock().await;
        rd.set(true);
        // Unblock the committer by sending its ACK.
        ack_tx.send(());
    });

    r.block_until_idle();
    assert!(relay_done.get(), "concurrent task must have acquired the mutex");
    assert!(commit_done.get(), "committer must complete after its ACK is delivered");
}

/// Structural regression for the SERIAL range allocation: it acquires the
/// catalog write lock AND the SAL-writer lock, emits synchronously, drops
/// BOTH, then awaits the fdatasync CQE with no locks held. A concurrent
/// catalog READER (SEEK / SEEK_BY_INDEX* / tick emission) must make progress
/// during that await — proving the write lock is not held across the fsync.
/// If it were, the writer-preferring rwlock would block the reader and both
/// would deadlock (the reader never sends the fake fsync completion).
#[test]
fn catalog_write_lock_not_held_across_serial_fsync() {
    let r = make_reactor();
    let rwlock: Rc<AsyncRwLock> = Rc::new(AsyncRwLock::new());
    let sal: Rc<AsyncMutex> = Rc::new(AsyncMutex::new());
    // Stand-in for the fsync CQE: the SERIAL task awaits it AFTER dropping
    // both locks; the reader sends it after acquiring the read lock.
    let (fsync_tx, fsync_rx) = oneshot::channel::<()>();
    let serial_done: Rc<StdCell<bool>> = Rc::new(StdCell::new(false));
    let reader_done: Rc<StdCell<bool>> = Rc::new(StdCell::new(false));

    let rw1 = Rc::clone(&rwlock);
    let sal1 = Rc::clone(&sal);
    let sd = Rc::clone(&serial_done);
    r.spawn(async move {
        // Reserve + mutate + emit under both locks, release both, THEN fsync.
        {
            let _w = rw1.write().await;
            let _s = sal1.lock().await;
            // ...synchronous SAL emission would go here...
        }
        // Both locks dropped. Park on the fsync with no locks held.
        let _ = fsync_rx.await;
        sd.set(true);
    });

    let rw2 = Rc::clone(&rwlock);
    let rd = Rc::clone(&reader_done);
    r.spawn(async move {
        // A catalog reader MUST acquire the read lock while the SERIAL task is
        // parked on its fsync — impossible if the write lock were held across
        // that await.
        let _rg = rw2.read().await;
        rd.set(true);
        // Unblock the SERIAL task's fsync.
        fsync_tx.send(());
    });

    r.block_until_idle();
    assert!(
        reader_done.get(),
        "catalog reader must acquire the read lock during the SERIAL fsync"
    );
    assert!(
        serial_done.get(),
        "SERIAL task must complete after its fsync CQE arrives"
    );
}

/// Structural regression: the relay loop acquires `sal_writer_excl` for a
/// synchronous SAL write, then releases it at scope exit before awaiting
/// the next item from its channel.  If the guard leaked across that await,
/// a concurrent committer could never acquire the mutex and would deadlock.
#[test]
fn sal_writer_excl_not_held_across_relay_recv() {
    let r = make_reactor();
    let mutex: Rc<AsyncMutex> = Rc::new(AsyncMutex::new());
    // `next_rx` stands in for the relay's `rx.recv()` — the await that
    // follows the SAL write scope.  `commit_tx` stands in for a concurrent
    // committer that must be able to acquire the SAL lock while the relay
    // task is parked on `next_rx.await`.
    let (next_tx, next_rx) = oneshot::channel::<()>();
    let (commit_tx, commit_rx) = oneshot::channel::<()>();
    let relay_done: Rc<StdCell<bool>> = Rc::new(StdCell::new(false));
    let commit_done: Rc<StdCell<bool>> = Rc::new(StdCell::new(false));

    let m1 = Rc::clone(&mutex);
    let rd = Rc::clone(&relay_done);
    r.spawn(async move {
        // Phase 2 of relay_loop: acquire lock, sync write, release.
        {
            let _sal = m1.lock().await;
            // ...emit_relay would go here...
        }
        // Lock dropped. Now await the next relay item (rx.recv()).
        let _ = next_rx.await;
        rd.set(true);
    });

    let m2 = Rc::clone(&mutex);
    let cd = Rc::clone(&commit_done);
    r.spawn(async move {
        // Committer: must be able to acquire the SAL lock while the relay
        // task is parked waiting for its next item.  Once it can, it
        // unblocks the relay by sending on `next_tx`.
        let _sal = m2.lock().await;
        cd.set(true);
        commit_tx.send(());
        next_tx.send(());
    });

    // commit_rx is unused — its role is to confirm the committer ran.
    drop(commit_rx);

    r.block_until_idle();
    assert!(
        commit_done.get(),
        "committer must have acquired the mutex while relay was parked"
    );
    assert!(relay_done.get(), "relay must complete after being unblocked");
}

#[test]
fn async_rwlock_multiple_readers() {
    let r = make_reactor();
    let active: Rc<StdCell<u32>> = Rc::new(StdCell::new(0));
    let max: Rc<StdCell<u32>> = Rc::new(StdCell::new(0));
    let lock: Rc<AsyncRwLock> = Rc::new(AsyncRwLock::new());
    for _ in 0..4 {
        let l = Rc::clone(&lock);
        let a = Rc::clone(&active);
        let m = Rc::clone(&max);
        r.spawn(async move {
            let _g = l.read().await;
            a.set(a.get() + 1);
            if a.get() > m.get() {
                m.set(a.get());
            }
            // Yield once to let other tasks acquire too.
            YieldOnce::new().await;
            a.set(a.get() - 1);
        });
    }
    r.block_until_idle();
    assert!(max.get() >= 2, "readers must overlap, got max={}", max.get());
}

#[test]
fn async_rwlock_writer_waits_for_readers() {
    let r = make_reactor();
    let order: Rc<RefCell<Vec<&'static str>>> = Rc::new(RefCell::new(Vec::new()));
    let lock: Rc<AsyncRwLock> = Rc::new(AsyncRwLock::new());
    let l1 = Rc::clone(&lock);
    let o1 = Rc::clone(&order);
    r.spawn(async move {
        let _g = l1.read().await;
        o1.borrow_mut().push("R_start");
        YieldOnce::new().await;
        o1.borrow_mut().push("R_end");
    });
    let l2 = Rc::clone(&lock);
    let o2 = Rc::clone(&order);
    r.spawn(async move {
        let _g = l2.write().await;
        o2.borrow_mut().push("W_start");
    });
    r.block_until_idle();
    let o = order.borrow().clone();
    // R_start before W_start, R_end also before W_start (writer waits).
    let r_end_pos = o.iter().position(|&s| s == "R_end").unwrap();
    let w_start_pos = o.iter().position(|&s| s == "W_start").unwrap();
    assert!(r_end_pos < w_start_pos, "writer must run after reader finishes: {o:?}");
}

/// A `LockFuture` dropped while parked (e.g. via `select2`) leaves a
/// stale waker in `AsyncMutex::waiters`.  `release()` must not pop
/// exactly one waker — doing so risks consuming the stale entry and
/// leaving all live waiters permanently blocked.
#[test]
fn async_mutex_cancelled_waiter_does_not_block_remaining() {
    let r = make_reactor();
    let mutex: Rc<AsyncMutex> = Rc::new(AsyncMutex::new());
    let done: Rc<StdCell<bool>> = Rc::new(StdCell::new(false));

    // Task A: holds the mutex, yields once (letting B and C park), then releases.
    let m_a = Rc::clone(&mutex);
    r.spawn(async move {
        let _g = m_a.lock().await;
        YieldOnce::new().await;
    });

    // Task B: races lock acquisition against an immediately-ready future.
    // `select2` polls the LockFuture first (it parks its waker inside
    // `waiters`), then `ready()` resolves. The LockFuture is dropped,
    // but its stale waker remains in the queue.
    let m_b = Rc::clone(&mutex);
    r.spawn(async move {
        let _ = select2(m_b.lock(), std::future::ready(())).await;
    });

    // Task C: must acquire the mutex once A releases — must not be
    // blocked by B's stale waker absorbing the single-pop release signal.
    let m_c = Rc::clone(&mutex);
    let d = Rc::clone(&done);
    r.spawn(async move {
        let _g = m_c.lock().await;
        d.set(true);
    });

    for _ in 0..20 {
        r.tick(false);
    }
    assert!(
        done.get(),
        "task C must acquire the mutex after task B's cancelled waiter"
    );
}

/// A `WriteFuture` dropped while parked leaves a stale waker in
/// `AsyncRwLock::write_waiters`.  `release_write()` popping exactly
/// one waker risks consuming the stale entry and leaving all remaining
/// live write waiters permanently blocked.
#[test]
fn async_rwlock_cancelled_write_waiter_does_not_block_remaining() {
    let r = make_reactor();
    let lock: Rc<AsyncRwLock> = Rc::new(AsyncRwLock::new());
    let done: Rc<StdCell<bool>> = Rc::new(StdCell::new(false));

    // Task A: holds the write lock, yields once, then releases.
    let l_a = Rc::clone(&lock);
    r.spawn(async move {
        let _g = l_a.write().await;
        YieldOnce::new().await;
    });

    // Task B: races write acquisition against an immediately-ready future.
    // Its WriteFuture parks (parked=true, writers_waiting bumped) then
    // is dropped by select2 with its stale waker still in write_waiters.
    let l_b = Rc::clone(&lock);
    r.spawn(async move {
        let _ = select2(l_b.write(), std::future::ready(())).await;
    });

    // Task C: must acquire the write lock after A releases — must not be
    // blocked by B's stale waker absorbing the single-pop release signal.
    let l_c = Rc::clone(&lock);
    let d = Rc::clone(&done);
    r.spawn(async move {
        let _g = l_c.write().await;
        d.set(true);
    });

    for _ in 0..20 {
        r.tick(false);
    }
    assert!(
        done.get(),
        "task C must acquire the write lock after task B's cancelled waiter"
    );
}

// ─────────────────────────────────────────────────────────────────
// AsyncRwLock writer-preference: new readers blocked by a parked
// writer.
// ─────────────────────────────────────────────────────────────────

/// When a write waiter is queued (writers_waiting > 0), ReadFuture
/// must block. The writer acquires the lock before the new reader.
#[test]
fn async_rwlock_new_readers_blocked_by_waiting_writer() {
    let r = make_reactor();
    let lock: Rc<AsyncRwLock> = Rc::new(AsyncRwLock::new());
    let order: Rc<RefCell<Vec<&'static str>>> = Rc::new(RefCell::new(Vec::new()));

    // Task A: holds read lock, yields once.
    let l_a = Rc::clone(&lock);
    let o_a = Rc::clone(&order);
    r.spawn(async move {
        let _g = l_a.read().await;
        o_a.borrow_mut().push("R1");
        YieldOnce::new().await;
    });

    // Task B: writer — parks while A holds the read lock.
    let l_b = Rc::clone(&lock);
    let o_b = Rc::clone(&order);
    r.spawn(async move {
        let _g = l_b.write().await;
        o_b.borrow_mut().push("W");
    });

    // Task C: new reader — must be blocked by the waiting writer
    // (writer-preference) and only enter after B releases.
    let l_c = Rc::clone(&lock);
    let o_c = Rc::clone(&order);
    r.spawn(async move {
        let _g = l_c.read().await;
        o_c.borrow_mut().push("R2");
    });

    r.block_until_idle();
    let o = order.borrow().clone();
    let w_pos = o.iter().position(|&s| s == "W").expect("W not seen");
    let r2_pos = o.iter().position(|&s| s == "R2").expect("R2 not seen");
    assert!(
        w_pos < r2_pos,
        "writer-preference violated: W must precede R2, got {o:?}"
    );
}

/// WriteFuture::Drop path 3: readers hold the lock, the dropped
/// WriteFuture was the LAST live write waiter. Pending readers
/// blocked by `writers_waiting > 0` must be unblocked.
#[test]
fn async_rwlock_last_write_waiter_cancelled_unblocks_pending_readers() {
    let r = make_reactor();
    let lock: Rc<AsyncRwLock> = Rc::new(AsyncRwLock::new());
    let done: Rc<StdCell<bool>> = Rc::new(StdCell::new(false));

    // cancel channel: dropping the sender unblocks select2 in Task B.
    let (cancel_tx, cancel_rx) = oneshot::channel::<()>();

    // Task A: holds read lock for many ticks so B stays parked.
    let l_a = Rc::clone(&lock);
    r.spawn(async move {
        let _g = l_a.read().await;
        for _ in 0..10 {
            YieldOnce::new().await;
        }
    });

    // Task B: races write acquisition vs cancel_rx. WriteFuture parks
    // (writers_waiting=1); cancel_rx stays Pending until we drop cancel_tx.
    let l_b = Rc::clone(&lock);
    r.spawn(async move {
        let _ = select2(l_b.write(), cancel_rx).await;
    });

    // Task C: new reader; parks in read_waiters while B is alive.
    let l_c = Rc::clone(&lock);
    let d = Rc::clone(&done);
    r.spawn(async move {
        let _g = l_c.read().await;
        d.set(true);
    });

    // Let A, B, C all park (A acquires read, B parks write, C parks read).
    for _ in 0..5 {
        r.tick(false);
    }
    assert!(!done.get(), "C must be blocked while write waiter B is alive");

    // Cancel B: WriteFuture::Drop path 3 must wake C.
    drop(cancel_tx);
    for _ in 0..5 {
        r.tick(false);
    }
    assert!(done.get(), "C must unblock when the last write waiter (B) is cancelled");
}

// ─────────────────────────────────────────────────────────────────
// join_all_unpin edge cases.
// ─────────────────────────────────────────────────────────────────

#[test]
fn join_all_unpin_empty_returns_empty_vec() {
    let r = make_reactor();
    let result = r.block_on(async { join_all_unpin(std::iter::empty::<std::future::Ready<i32>>()).await });
    assert!(
        result.is_empty(),
        "join_all_unpin on empty iterator must return empty vec"
    );
}

#[test]
fn join_all_unpin_single_future_completes() {
    let r = make_reactor();
    let result = r.block_on(async { join_all_unpin(std::iter::once(std::future::ready(99u32))).await });
    assert_eq!(result, vec![99u32]);
}

// ─────────────────────────────────────────────────────────────────
// mpsc::try_recv: non-blocking drain used by the committer.
// ─────────────────────────────────────────────────────────────────

#[test]
fn mpsc_try_recv_drains_queue_without_blocking() {
    let (tx, mut rx) = mpsc::unbounded::<i32>();
    tx.send(10);
    tx.send(20);
    tx.send(30);
    assert_eq!(rx.try_recv(), Some(10));
    assert_eq!(rx.try_recv(), Some(20));
    assert_eq!(rx.try_recv(), Some(30));
    assert_eq!(rx.try_recv(), None, "queue must be empty after full drain");
}
