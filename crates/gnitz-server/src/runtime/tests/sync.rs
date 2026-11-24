//! The single-threaded async primitives: `WakeQueue`, `Level`, `oneshot`,
//! `AsyncRwLock`, `select2` and the cancellation shapes each must survive.

use std::cell::Cell;
use std::rc::Rc;

use super::*;
use crate::runtime::reactor::test_support::make_reactor;
use crate::runtime::test_support::{Wakes, try_poll_once, yield_once};

/// A parked awaiter is woken by a value and by the end, a value queued before
/// the end is handed out after it, and a drained burst gives back the slots it
/// grew past the retained ones.
#[test]
fn a_wake_queue_hands_out_every_value_before_its_end() {
    let (wakes, waker) = Wakes::waker();
    let cx = Context::from_waker(&waker);
    let mut q = WakeQueue::default();
    assert_eq!(q.poll_ready(&cx), Poll::Pending);
    let burst: Vec<usize> = (0..4 * RETAINED_SLOTS).collect();
    burst.iter().for_each(|&v| q.push(v));
    assert_eq!(wakes.count(), 1, "the parked awaiter, once");
    assert_eq!(std::iter::from_fn(|| q.pop()).collect::<Vec<_>>(), burst);
    assert!(q.queue.capacity() < 2 * RETAINED_SLOTS, "{}", q.queue.capacity());

    assert_eq!(q.poll_ready(&cx), Poll::Pending);
    q.push(7);
    q.close();
    assert_eq!(wakes.count(), 2);
    assert_eq!(q.poll_ready(&cx), Poll::Ready(()));
    assert_eq!((q.pop(), q.pop()), (Some(7), None));
    assert_eq!(q.poll_ready(&cx), Poll::Ready(()), "an ended queue stays ready");
}

/// Raising a level wakes each waiter at or below it once, keeps none of them
/// and leaves the rest parked; a raise at or below the level moves nothing.
#[test]
fn a_raised_level_wakes_the_waiters_at_or_below_it_and_no_other() {
    let level = Level::default();
    let [(w5, waker5), (w9, waker9)] = [(); 2].map(|()| Wakes::waker());
    let wakers = [waker5, waker9];
    let poll = |i: usize| level.poll_reached([5, 9][i], &Context::from_waker(&wakers[i]));
    let woken = || [w5.count(), w9.count()];

    assert_eq!((poll(0), poll(1)), (Poll::Pending, Poll::Pending));
    level.raise(7);
    assert_eq!(woken(), [1, 0]);
    assert_eq!((poll(0), poll(1)), (Poll::Ready(()), Poll::Pending));

    level.raise(6);
    assert_eq!(level.get(), 7, "a level does not move back");
    assert_eq!(woken(), [1, 0]);

    level.raise(9);
    assert_eq!(woken(), [1, 2], "once per time it parked");
    assert_eq!(poll(1), Poll::Ready(()));
    assert!(level.waiters.borrow().is_empty());
}

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

/// Readers share the lock, and a writer parked behind them blocks a new reader:
/// the last reader out admits the writer ahead of it, and cancelling the writer
/// instead admits the blocked reader at once.
#[test]
fn async_rwlock_a_parked_writer_blocks_new_readers_until_it_acquires_or_drops() {
    for cancel in [false, true] {
        let lock: AsyncRwLock = AsyncRwLock::default();
        let readers: Vec<_> = (0..4)
            .map(|_| try_poll_once(lock.read()).expect("readers share the lock"))
            .collect();
        let ((w_wakes, w_waker), (r_wakes, r_waker)) = (Wakes::waker(), Wakes::waker());
        let mut writer = Box::pin(lock.write());
        let mut reader = Box::pin(lock.read());
        assert!(writer.as_mut().poll(&mut Context::from_waker(&w_waker)).is_pending());
        assert!(
            reader.as_mut().poll(&mut Context::from_waker(&r_waker)).is_pending(),
            "a parked writer blocks a new reader"
        );
        if cancel {
            drop(writer);
            assert_eq!(r_wakes.count(), 1, "the last parked writer's cancel admits the reader");
            assert!(try_poll_once(&mut reader).is_some());
            continue;
        }
        drop(readers);
        assert_eq!((w_wakes.count(), r_wakes.count()), (1, 0), "the writer goes first");
        let held = try_poll_once(&mut writer).expect("the woken writer acquires");
        assert!(try_poll_once(&mut reader).is_none(), "the writer holds the lock");
        drop(held);
        assert_eq!(r_wakes.count(), 1, "the writer's release admits the reader");
    }
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
    // next starts, and a reactor is never freed, so one per mix would leak a
    // ring for each of the enumeration's mixes.
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
                        d.update(|v| v + 1);
                    }),
                    // Cancel the acquire: the loser's Drop is the whole point.
                    2 => r.spawn(async move {
                        let _ = select2(l.write(), std::future::ready(())).await;
                        d.update(|v| v + 1);
                    }),
                    _ => r.spawn(async move {
                        let _ = select2(l.read(), std::future::ready(())).await;
                        d.update(|v| v + 1);
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
                c.update(|v| v + 1);
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

/// A guard is contended while a writer is parked behind it, and no longer once
/// that writer drops.
#[test]
fn async_rwlock_a_guard_is_contended_while_a_writer_waits() {
    let lock: AsyncRwLock = AsyncRwLock::default();
    let read = try_poll_once(lock.read()).expect("a free lock admits a reader");
    assert!(!read.contended(), "nothing waits on a lone reader");
    let mut queued = Box::pin(lock.write());
    assert!(try_poll_once(&mut queued).is_none(), "a held lock parks the writer");
    assert!(read.contended(), "a parked writer waits on the reader");
    drop(queued);
    assert!(!read.contended(), "a dropped writer leaves the queue");
}

/// Dropping a writer queued between two others removes only its own entry: the
/// first and the last still acquire in queue order.
#[test]
fn async_rwlock_a_dropped_middle_writer_keeps_the_others_in_order() {
    let lock: AsyncRwLock = AsyncRwLock::default();
    let held = try_poll_once(lock.write()).expect("a free lock admits a writer");
    let mut first = Box::pin(lock.write());
    let mut middle = Box::pin(lock.write());
    let mut last = Box::pin(lock.write());
    assert!(try_poll_once(&mut first).is_none(), "a held lock parks the writer");
    assert!(try_poll_once(&mut middle).is_none(), "a held lock parks the writer");
    assert!(try_poll_once(&mut last).is_none(), "a held lock parks the writer");

    drop(middle);
    drop(held);
    assert!(
        try_poll_once(&mut last).is_none(),
        "the last writer must wait for the first"
    );
    let g = try_poll_once(&mut first).expect("the first queued writer acquires");
    assert!(
        try_poll_once(&mut last).is_none(),
        "the lock is held by the first writer"
    );
    drop(g);
    let _g = try_poll_once(&mut last).expect("the last writer acquires next");
    assert!(try_poll_once(lock.read()).is_none(), "the last writer holds the lock");
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
