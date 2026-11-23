use super::Durable;
use crate::runtime::reactor::test_support::make_reactor;
use crate::runtime::test_support::try_poll_once;
use gnitz_foundation::posix_io::memfd;
use std::cell::{Cell, RefCell};
use std::fs::File;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Wake, Waker};

/// A wake that counts itself.
#[derive(Default)]
struct Wakes(AtomicUsize);

impl Wake for Wakes {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

/// Marking an LSN durable wakes each waiter at or below it once and leaves the
/// rest parked; an LSN at or below the durable one moves nothing.
#[test]
fn marking_durable_wakes_the_waiters_at_or_below_it_and_no_other() {
    let durable = Durable::default();
    let wakes = [5u64, 7, 9].map(|_| Arc::new(Wakes::default()));
    let wakers = wakes.each_ref().map(|w| Waker::from(Arc::clone(w)));
    let mut waits = [5u64, 7, 9].map(|lsn| Box::pin(durable.through(lsn)));
    let mut poll = |i: usize| waits[i].as_mut().poll(&mut Context::from_waker(&wakers[i]));
    let woken = || wakes.each_ref().map(|w| w.0.load(Ordering::Relaxed));

    for i in 0..3 {
        assert_eq!(poll(i), Poll::Pending, "nothing is durable");
    }
    assert_eq!(durable.wanted.get(), 9, "the highest LSN waited for");

    durable.mark(7);
    assert_eq!(woken(), [1, 1, 0]);
    assert_eq!(
        (poll(0), poll(1), poll(2)),
        (Poll::Ready(()), Poll::Ready(()), Poll::Pending)
    );

    durable.mark(6);
    assert_eq!(durable.through.get(), 7, "durability does not move back");
    assert_eq!(woken(), [1, 1, 0]);

    durable.mark(9);
    assert_eq!(woken(), [1, 1, 2], "once per time it parked");
    assert_eq!(poll(2), Poll::Ready(()));
}

/// An fdatasync makes durable what was published when it was submitted and no
/// more: an LSN published and wanted while one is in flight is answered by the
/// next, which the driver submits once the first completes.
#[test]
fn an_lsn_published_under_an_fdatasync_in_flight_waits_for_the_next() {
    let durable: &'static Durable = Box::leak(Box::default());
    let file: &'static File = Box::leak(Box::new(memfd(c"test_durable").expect("memfd")));
    let published: &'static Cell<u64> = Box::leak(Box::new(Cell::new(5)));
    let submitted: &'static RefCell<Vec<u64>> = Box::leak(Box::default());
    // The driver asks once per fdatasync, as it submits it: what is published
    // and wanted from there on, as LSN 9 is, is under that sync.
    let published_at_submit = move || {
        let covered = published.get();
        submitted.borrow_mut().push(covered);
        if covered == 5 {
            published.set(9);
            durable.want(9);
        }
        covered
    };
    let reactor = make_reactor();
    reactor.spawn(async move { durable.run(reactor, file, published_at_submit).await });

    reactor.block_on(async {
        let mut first = Box::pin(durable.through(5));
        assert!(try_poll_once(&mut first).is_none(), "published, not yet durable");
        first.await;
        durable.through(9).await;
    });
    assert_eq!(
        *submitted.borrow(),
        [5, 9],
        "the first sync covers what was published when it was submitted, the second the rest"
    );
    assert_eq!(
        durable.syncing(),
        0,
        "no fdatasync is in flight once its waiters are answered"
    );
}
