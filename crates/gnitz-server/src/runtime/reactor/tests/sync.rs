//! The single-threaded async primitives: `oneshot`, `chan`, `AsyncMutex`,
//! `AsyncRwLock`, `join_all_unpin` and the cancellation shapes each must
//! survive.

use std::cell::Cell as StdCell;

use super::super::test_support::*;
use super::super::*;

// ------------------------------------------------------------------
// Primitives: oneshot / chan / AsyncMutex / AsyncRwLock
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
    let cancelled: Rc<StdCell<bool>> = Rc::new(StdCell::new(false));
    let cancelled2 = Rc::clone(&cancelled);
    let (tx, rx) = oneshot::channel::<i32>();
    r.spawn(async move {
        let res = rx.await;
        cancelled2.set(res.is_none());
    });
    r.tick(false);
    drop(tx);
    r.block_until_idle();
    assert!(cancelled.get(), "dropping the sender must resolve the receiver to None");
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

/// The last sender's drop is what ends the stream: `recv()` resolves to `None`,
/// which is how the committer's and the tick loop's `run` loops shut down.
#[test]
fn chan_sender_drop_ends_the_stream() {
    let r = make_reactor();
    let got: Rc<RefCell<Vec<i32>>> = Rc::new(RefCell::new(Vec::new()));
    let got2 = Rc::clone(&got);
    let (tx, mut rx) = chan::unbounded::<i32>();
    tx.send(10);
    tx.send(20);
    drop(tx);
    r.block_on(async move {
        while let Some(v) = rx.recv().await {
            got2.borrow_mut().push(v);
        }
    });
    assert_eq!(
        *got.borrow(),
        vec![10, 20],
        "a single-consumer queue delivers in send order"
    );
}

#[test]
fn async_mutex_serializes_access() {
    let r = make_reactor();
    let order: Rc<RefCell<Vec<u32>>> = Rc::new(RefCell::new(Vec::new()));
    let mutex: Rc<AsyncMutex> = Rc::new(AsyncMutex::default());
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

#[test]
fn async_rwlock_multiple_readers() {
    let r = make_reactor();
    let active: Rc<StdCell<u32>> = Rc::new(StdCell::new(0));
    let max: Rc<StdCell<u32>> = Rc::new(StdCell::new(0));
    let lock: Rc<AsyncRwLock> = Rc::new(AsyncRwLock::default());
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
    let lock: Rc<AsyncRwLock> = Rc::new(AsyncRwLock::default());
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

/// A `WriteFuture` dropped while parked (e.g. via `select2`) leaves a stale
/// waker behind. The release path must wake every candidate — handing the lock
/// to that one entry alone would block every live waiter forever.
#[test]
fn async_mutex_cancelled_waiter_does_not_block_remaining() {
    let r = make_reactor();
    let mutex: Rc<AsyncMutex> = Rc::new(AsyncMutex::default());
    let done: Rc<StdCell<bool>> = Rc::new(StdCell::new(false));

    // Task A: holds the mutex, yields once (letting B and C park), then releases.
    let m_a = Rc::clone(&mutex);
    r.spawn(async move {
        let _g = m_a.lock().await;
        YieldOnce::new().await;
    });

    // Task B: races lock acquisition against an immediately-ready future.
    // `select2` polls the lock future first (it parks its waker), then
    // `ready()` resolves. The lock future is dropped, but its stale waker
    // remains in the queue.
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

// ─────────────────────────────────────────────────────────────────
// AsyncRwLock writer-preference: new readers blocked by a parked
// writer.
// ─────────────────────────────────────────────────────────────────

/// When a write waiter is queued (writers_waiting > 0), ReadFuture
/// must block. The writer acquires the lock before the new reader.
#[test]
fn async_rwlock_new_readers_blocked_by_waiting_writer() {
    let r = make_reactor();
    let lock: Rc<AsyncRwLock> = Rc::new(AsyncRwLock::default());
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

/// Readers hold the lock and the cancelled WriteFuture was the last live write
/// waiter: the readers `writers_waiting > 0` was blocking must now enter.
#[test]
fn async_rwlock_last_write_waiter_cancelled_unblocks_pending_readers() {
    let r = make_reactor();
    let lock: Rc<AsyncRwLock> = Rc::new(AsyncRwLock::default());
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

    // Cancel B: dropping the last live write waiter must wake C.
    drop(cancel_tx);
    for _ in 0..5 {
        r.tick(false);
    }
    assert!(done.get(), "C must unblock when the last write waiter (B) is cancelled");
}

/// The reader/writer/cancellation state space, enumerated rather than sampled:
/// every task mix over the four shapes, for every length up to `MAX_TASKS`.
/// Whatever the mix, every task must run to completion and the lock must end
/// holding nothing.
///
/// The three counters are the whole assertion — the waiter queues legitimately
/// retain stale wakers at rest, so an empty-queue check would be wrong.
#[test]
fn async_rwlock_every_task_mix_finishes_and_leaves_the_lock_idle() {
    const SHAPES: u32 = 4;
    const MAX_TASKS: u32 = 5;

    // One reactor for every mix: each drains to an empty task slab before the
    // next starts, and a fresh io_uring per mix exhausts RLIMIT_MEMLOCK long
    // before the enumeration ends.
    let r = make_reactor();

    for len in 2..=MAX_TASKS {
        for mix in 0..SHAPES.pow(len) {
            let lock: Rc<AsyncRwLock> = Rc::new(AsyncRwLock::default());
            let done: Rc<StdCell<usize>> = Rc::new(StdCell::new(0));

            for i in 0..len {
                let l = Rc::clone(&lock);
                let d = Rc::clone(&done);
                match (mix / SHAPES.pow(i)) % SHAPES {
                    // Hold the lock across an await, so the rest must park.
                    0 => r.spawn(async move {
                        let _g = l.read().await;
                        YieldOnce::new().await;
                        d.set(d.get() + 1);
                    }),
                    1 => r.spawn(async move {
                        let _g = l.write().await;
                        YieldOnce::new().await;
                        d.set(d.get() + 1);
                    }),
                    // Cancel the acquire: the loser's Drop is the whole point.
                    2 => r.spawn(async move {
                        let _ = select2(l.write(), std::future::ready(())).await;
                        d.set(d.get() + 1);
                    }),
                    _ => r.spawn(async move {
                        let _ = select2(l.read(), std::future::ready(())).await;
                        d.set(d.get() + 1);
                    }),
                };
            }

            // `block_until_idle` panics on a lost wake; these catch a task that
            // finished without acquiring, and a guard that leaked its state.
            r.block_until_idle();
            assert_eq!(done.get(), len as usize, "mix {mix} of {len}: a task never ran");
            assert!(lock.is_quiescent(), "mix {mix} of {len}: the lock did not end idle");
        }
    }
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

// ─────────────────────────────────────────────────────────────────
// chan::try_recv: non-blocking drain used by the committer.
// ─────────────────────────────────────────────────────────────────

#[test]
fn chan_try_recv_drains_queue_without_blocking() {
    let (tx, mut rx) = chan::unbounded::<i32>();
    tx.send(10);
    tx.send(20);
    tx.send(30);
    assert_eq!(rx.try_recv(), Some(10));
    assert_eq!(rx.try_recv(), Some(20));
    assert_eq!(rx.try_recv(), Some(30));
    assert_eq!(rx.try_recv(), None, "queue must be empty after full drain");
}
