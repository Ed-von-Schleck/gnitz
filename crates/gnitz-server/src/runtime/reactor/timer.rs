//! The reactor's timers: a [`Reactor::sleep_until`] polled before its deadline
//! holds one entry in the deadline map until that deadline passes or it drops;
//! each pass removes and wakes every entry whose deadline has passed.

use std::collections::BTreeMap;

use super::*;

/// Every pending deadline, and the clock they are measured against.
pub(super) struct Timers {
    /// Keyed by `(deadline, timer id)` and valued by the waker to fire.
    deadlines: RefCell<BTreeMap<(Instant, u64), Waker>>,
    /// The next timer id, which orders deadlines sharing an instant; the 64-bit
    /// counter does not wrap within a process's life.
    next_id: Cell<u64>,
    /// Read once per pass by [`Timers::fire`]; see [`Reactor::now`].
    now: Cell<Instant>,
}

impl Timers {
    pub(super) fn new() -> Self {
        Timers {
            deadlines: RefCell::new(BTreeMap::new()),
            next_id: Cell::new(0),
            now: Cell::new(Instant::now()),
        }
    }

    /// The first pending deadline: the loop's blocking wait never outlasts it.
    pub(super) fn earliest(&self) -> Option<Instant> {
        self.deadlines.borrow().first_key_value().map(|(&(at, _), _)| at)
    }

    /// Read the clock for this pass, and wake every deadline it has passed.
    pub(super) fn fire(&self) {
        let now = Instant::now();
        self.now.set(now);
        let mut d = self.deadlines.borrow_mut();
        while let Some(e) = d.first_entry() {
            if e.key().0 > now {
                break;
            }
            e.remove().wake();
        }
    }
}

/// A deadline. A pending poll puts its entry in the deadline map; the pass whose
/// clock passes it removes the entry and wakes it, and a drop removes it before
/// then.
struct TimerFuture {
    deadline: Instant,
    id: u64,
    reactor: Reactor,
}

impl Reactor {
    /// Future that completes once `at` has passed.
    pub(crate) fn sleep_until(&self, at: Instant) -> impl Future<Output = ()> + use<> {
        let id = self.timers.next_id.get();
        self.timers.next_id.set(id + 1);
        TimerFuture { deadline: at, id, reactor: self.clone() }
    }

    /// Future that completes once `d` has passed since this pass began.
    pub(crate) fn sleep(&self, d: Duration) -> impl Future<Output = ()> + use<> {
        self.sleep_until(self.now() + d)
    }

    /// The clock this pass read: every timer is polled against it, so a deadline
    /// taken from it that has passed is ready at its first poll. Outside a pass
    /// it is the last pass's (or `Reactor::new`'s).
    pub(crate) fn now(&self) -> Instant {
        self.timers.now.get()
    }
}

impl Future for TimerFuture {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let timers = &self.reactor.timers;
        if timers.now.get() >= self.deadline {
            return Poll::Ready(());
        }
        timers
            .deadlines
            .borrow_mut()
            .insert((self.deadline, self.id), cx.waker().clone());
        Poll::Pending
    }
}

impl Drop for TimerFuture {
    fn drop(&mut self) {
        self.reactor
            .timers
            .deadlines
            .borrow_mut()
            .remove(&(self.deadline, self.id));
    }
}

#[cfg(test)]
#[path = "tests/timer.rs"]
mod tests;
