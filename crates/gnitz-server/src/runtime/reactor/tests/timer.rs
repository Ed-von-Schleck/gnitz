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
    assert!(r.deadlines.borrow().is_empty());

    let mut tf = Box::pin(r.sleep(Duration::from_secs(3600)));
    assert!(try_poll_once(tf.as_mut()).is_none());
    assert_eq!(r.deadlines.borrow().len(), 1, "the poll registers the deadline");
    drop(tf);
    assert!(r.deadlines.borrow().is_empty(), "the drop removes it");
}
