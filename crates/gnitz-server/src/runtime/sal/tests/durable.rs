use super::Durable;
use crate::runtime::reactor::test_support::make_reactor;
use crate::runtime::test_support::try_poll_once;
use gnitz_foundation::posix_io::memfd;
use std::cell::{Cell, RefCell};
use std::fs::File;

/// A pending wait asks for its LSN, the highest LSN asked for wins, and a wait
/// already answered asks for nothing. Waking the waiters is `Level`'s.
#[test]
fn a_pending_wait_wants_its_lsn_and_an_answered_one_wants_nothing() {
    let durable = Durable::default();
    for lsn in [5, 9, 7] {
        assert!(try_poll_once(durable.through(lsn)).is_none(), "nothing is durable");
    }
    assert_eq!(durable.wanted.get(), 9, "the highest LSN waited for");
    durable.mark(12);
    assert!(try_poll_once(durable.through(11)).is_some());
    assert_eq!(durable.wanted.get(), 9, "an answered wait asks for nothing");
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
