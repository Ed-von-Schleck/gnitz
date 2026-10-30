//! The single-threaded async primitives: `oneshot`, `AsyncRwLock`, `select2` and
//! the cancellation shapes each must survive.

use std::cell::{Cell, RefCell};
use std::rc::Rc;

use super::*;
use crate::runtime::reactor::test_support::{make_reactor, poll_until, yield_once};
use crate::runtime::test_support::try_poll_once;

/// Sending to a cancelled receiver is not an error and never was to any caller:
/// the value is parked where nothing will read it and dropped with the channel.
#[test]
fn oneshot_send_to_dropped_receiver_is_a_noop() {
    let dropped = Rc::new(Cell::new(false));
    struct Tattle(Rc<Cell<bool>>);
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

/// Every reader admitted while no writer holds or waits enters at once.
#[test]
fn async_rwlock_multiple_readers() {
    let r = make_reactor();
    let active = Rc::new(Cell::new(0u32));
    let max = Rc::new(Cell::new(0u32));
    let lock: AsyncRwLock = AsyncRwLock::default();
    for _ in 0..4 {
        let l = lock.clone();
        let a = Rc::clone(&active);
        let m = Rc::clone(&max);
        r.spawn(async move {
            let _g = l.read().await;
            a.set(a.get() + 1);
            m.set(m.get().max(a.get()));
            // Yield once to let other tasks acquire too.
            yield_once().await;
            a.set(a.get() - 1);
        });
    }
    r.block_until_idle();
    assert_eq!(max.get(), 4, "all four readers must hold the lock together");
}

/// A waiting writer blocks a new reader, and acquires the lock before it.
#[test]
fn async_rwlock_new_readers_blocked_by_waiting_writer() {
    let r = make_reactor();
    let lock: AsyncRwLock = AsyncRwLock::default();
    let order: Rc<RefCell<Vec<&'static str>>> = Rc::default();

    // Task A: holds read lock, yields once.
    let l_a = lock.clone();
    let o_a = Rc::clone(&order);
    r.spawn(async move {
        let _g = l_a.read().await;
        o_a.borrow_mut().push("R1");
        yield_once().await;
    });

    // Task B: writer — parks while A holds the read lock.
    let l_b = lock.clone();
    let o_b = Rc::clone(&order);
    r.spawn(async move {
        let _g = l_b.write().await;
        o_b.borrow_mut().push("W");
    });

    // Task C: new reader — must be blocked by the waiting writer
    // (writer-preference) and only enter after B releases.
    let l_c = lock.clone();
    let o_c = Rc::clone(&order);
    r.spawn(async move {
        let _g = l_c.read().await;
        o_c.borrow_mut().push("R2");
    });

    r.block_until_idle();
    assert_eq!(*order.borrow(), ["R1", "W", "R2"], "writer-preference violated");
}

/// Readers hold the lock and the cancelled `WriteFuture` was the last waiting
/// writer: the readers it was blocking must enter at once, not when the holder
/// releases.
#[test]
fn async_rwlock_last_write_waiter_cancelled_unblocks_pending_readers() {
    let r = make_reactor();
    let lock: AsyncRwLock = AsyncRwLock::default();
    let done = Rc::new(Cell::new(false));
    let (release_tx, release_rx) = oneshot::channel::<()>();
    let (cancel_tx, cancel_rx) = oneshot::channel::<()>();

    // Task A: holds the read lock until released.
    let l_a = lock.clone();
    r.spawn(async move {
        let _g = l_a.read().await;
        release_rx.await;
    });

    // Task B: races write acquisition against `cancel_rx`, so its WriteFuture
    // parks until the cancel.
    let l_b = lock.clone();
    r.spawn(async move {
        let _ = select2(l_b.write(), cancel_rx).await;
    });

    // Task C: new reader; parks behind B.
    let l_c = lock.clone();
    let d = Rc::clone(&done);
    r.spawn(async move {
        let _g = l_c.read().await;
        d.set(true);
    });

    r.tick(false); // A acquires, B and C park
    assert!(!done.get(), "C must be blocked while write waiter B is alive");

    cancel_tx.send(());
    assert!(
        poll_until(&r, || done.get()),
        "C must unblock when the last write waiter (B) is cancelled"
    );
    release_tx.send(());
    r.block_until_idle();
}

/// The reader/writer/cancellation state space, enumerated rather than sampled:
/// every task mix over the four shapes, for every length up to `MAX_TASKS`.
/// Whatever the mix, no writer shares the lock, every task runs to completion,
/// and the lock ends admitting both a reader and a writer.
#[test]
fn async_rwlock_every_task_mix_excludes_finishes_and_leaves_the_lock_idle() {
    const SHAPES: u32 = 4;
    const MAX_TASKS: u32 = 5;

    // One reactor for every mix: each drains to an empty task slab before the
    // next starts, and a fresh io_uring per mix exhausts RLIMIT_MEMLOCK long
    // before the enumeration ends.
    let r = make_reactor();

    for len in 2..=MAX_TASKS {
        for mix in 0..SHAPES.pow(len) {
            let lock: AsyncRwLock = AsyncRwLock::default();
            let done = Rc::new(Cell::new(0usize));
            // (readers, writers) inside the lock.
            let held = Rc::new(Cell::new((0u32, 0u32)));

            for i in 0..len {
                let l = lock.clone();
                let (d, h) = (Rc::clone(&done), Rc::clone(&held));
                match (mix / SHAPES.pow(i)) % SHAPES {
                    // Hold the lock across an await, so the rest must park.
                    shape @ (0 | 1) => r.spawn(async move {
                        let (_g, me) = if shape == 1 {
                            (Either::B(l.write().await), (0, 1))
                        } else {
                            (Either::A(l.read().await), (1, 0))
                        };
                        let (rd, wr) = h.get();
                        let inside = (rd + me.0, wr + me.1);
                        assert!(
                            inside.1 == 0 || inside == (0, 1),
                            "mix {mix} of {len}: {inside:?} (readers, writers) inside the lock"
                        );
                        h.set(inside);
                        yield_once().await;
                        let (rd, wr) = h.get();
                        h.set((rd - me.0, wr - me.1));
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
            // finished without acquiring, and a guard or waiter that leaked.
            r.block_until_idle();
            assert_eq!(done.get(), len as usize, "mix {mix} of {len}: a task never ran");
            assert!(
                try_poll_once(lock.read()).is_some(),
                "mix {mix} of {len}: a reader is refused"
            );
            assert!(
                try_poll_once(lock.write()).is_some(),
                "mix {mix} of {len}: a writer is refused"
            );
        }
    }
}

// ─────────────────────────────────────────────────────────────────
// The write handoff: one release admits one writer.
// ─────────────────────────────────────────────────────────────────

/// A release admits one parked writer, not all of them: `N` writers cost
/// `2N - 1` polls of the acquire.
#[test]
fn async_rwlock_write_release_admits_one_writer() {
    const N: usize = 16;
    let r = make_reactor();
    let lock: AsyncRwLock = AsyncRwLock::default();
    let polls = Rc::new(Cell::new(0usize));

    for _ in 0..N {
        let l = lock.clone();
        let c = Rc::clone(&polls);
        r.spawn(async move {
            let mut acquire = std::pin::pin!(l.write());
            let _g = std::future::poll_fn(|cx| {
                c.set(c.get() + 1);
                acquire.as_mut().poll(cx)
            })
            .await;
            // Without this nothing contends and every acquire takes one poll.
            yield_once().await;
        });
    }

    r.block_until_idle();
    assert_eq!(polls.get(), 2 * N - 1, "each release must wake one writer");
}

/// A release hands the lock to the first queued writer: a fresh writer polled
/// between the release and that writer's poll queues behind it.
#[test]
fn async_rwlock_a_fresh_writer_queues_behind_a_woken_one() {
    let lock: AsyncRwLock = AsyncRwLock::default();
    let held = try_poll_once(lock.write()).expect("a free lock admits a writer");
    let mut queued = Box::pin(lock.write());
    assert!(try_poll_once(&mut queued).is_none(), "a held lock parks the writer");

    drop(held);
    assert!(
        try_poll_once(lock.write()).is_none(),
        "a fresh writer must not take the lock from the woken one"
    );
    assert!(try_poll_once(&mut queued).is_some(), "the woken writer acquires");
}

// ─────────────────────────────────────────────────────────────────
// select2
// ─────────────────────────────────────────────────────────────────

/// Both sides ready at once: the first wins, which `Peer::until_request` relies
/// on to answer a sync ahead of a ready request.
#[test]
fn select2_prefers_its_first_argument_on_a_tie() {
    let won = try_poll_once(select2(std::future::ready(1), std::future::ready(2)));
    assert!(matches!(won, Some(Either::A(1))));
}
