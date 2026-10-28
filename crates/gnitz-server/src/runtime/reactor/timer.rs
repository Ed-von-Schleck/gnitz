//! The reactor's timers: a [`Reactor::sleep`] holds one entry in the deadline
//! map while it is pending, and each pass wakes every entry whose deadline has
//! passed.

use super::*;

/// A deadline. Its entry in the deadline map lives until the future drops.
struct TimerFuture {
    deadline: Instant,
    id: u64,
    reactor: Reactor,
}

impl Reactor {
    /// Future that completes once `d` has passed.
    pub fn sleep(&self, d: Duration) -> impl Future<Output = ()> {
        TimerFuture {
            deadline: Instant::now() + d,
            id: self.alloc_timer_id(),
            reactor: self.clone(),
        }
    }

    /// The next timer id; the 64-bit counter does not wrap within a process's
    /// life.
    fn alloc_timer_id(&self) -> u64 {
        let id = self.next_timer_id.get();
        self.next_timer_id.set(id + 1);
        id
    }

    /// Wake every deadline that has passed.
    pub(super) fn fire_deadlines(&self) {
        let now = Instant::now();
        let mut d = self.deadlines.borrow_mut();
        while let Some(e) = d.first_entry() {
            if e.key().0 > now {
                break;
            }
            e.remove().wake();
        }
    }
}

impl Future for TimerFuture {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if Instant::now() >= self.deadline {
            return Poll::Ready(());
        }
        self.reactor
            .deadlines
            .borrow_mut()
            .insert((self.deadline, self.id), cx.waker().clone());
        Poll::Pending
    }
}

impl Drop for TimerFuture {
    fn drop(&mut self) {
        self.reactor.deadlines.borrow_mut().remove(&(self.deadline, self.id));
    }
}

#[cfg(test)]
#[path = "tests/timer.rs"]
mod tests;
