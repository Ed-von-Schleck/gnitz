//! Timers: a deadline is held exactly while its future is pending.

use super::super::test_support::*;
use super::*;

/// A sleep of nothing resolves on its first poll and registers no deadline; a
/// pending one holds its deadline until it drops, so a dropped timer neither
/// wakes its waker nor bounds a later sleep.
#[test]
fn a_timer_holds_a_deadline_only_while_pending() {
    let r = make_reactor();
    assert!(try_poll_once(r.sleep(Duration::ZERO)).is_some());
    assert!(r.timers.earliest().is_none());

    let mut tf = Box::pin(r.sleep(Duration::from_secs(3600)));
    assert!(try_poll_once(tf.as_mut()).is_none());
    assert!(r.timers.earliest().is_some(), "the poll registers the deadline");
    drop(tf);
    assert!(r.timers.earliest().is_none(), "the drop removes it");
}

/// Timers sharing a deadline each hold an entry of their own, and each is
/// woken.
#[test]
fn timers_sharing_a_deadline_each_fire() {
    within(|| {
        let r = make_reactor();
        let far = r.now() + Duration::from_secs(3600);
        let mut pending: Vec<_> = (0..3).map(|_| Box::pin(r.sleep_until(far))).collect();
        assert!(pending.iter_mut().all(|t| try_poll_once(t.as_mut()).is_none()));
        assert_eq!(r.timers.deadlines.borrow().len(), 3);
        drop(pending);

        // Sleeps created in one pass share its clock, and so a deadline.
        let fired = Rc::new(Cell::new(0));
        for _ in 0..3 {
            let (sleep, fired) = (r.sleep(Duration::from_millis(5)), Rc::clone(&fired));
            r.spawn(async move {
                sleep.await;
                fired.set(fired.get() + 1);
            });
        }
        r.block_on(r.sleep(Duration::from_millis(10)));
        assert_eq!(fired.get(), 3);
    });
}
