//! Park slots: where a reactor future waits for a CQE-delivered result.
//!
//! Timer, reply, fsync, send and raw-recv are one machine — submit an op, park
//! its result, wake the awaiter — so they share this map instead of spelling it
//! out per family. The entry is created when the op is submitted and removed
//! when its result is collected or discarded, so **entry presence is op
//! liveness**: a completion that finds no entry, or an abandoned one, has
//! nothing to deliver and retires whatever the op carried. That is what a
//! handler which parked results unconditionally would need a side tombstone set
//! per family to know.

use std::cell::RefCell;
use std::collections::hash_map::Entry;
use std::task::{Context, Poll, Waker};

use rustc_hash::FxHashMap;

/// One outstanding op.
struct Op<T, A> {
    /// The awaiter's waker while it is parked; `None` before its first poll and
    /// once the op is abandoned.
    waker: Option<Waker>,
    /// What the op owns until its completion lands: memory the kernel still
    /// reads or writes, plus whatever the completion handler needs (a send's
    /// target fd). Dropped with the entry.
    carry: Option<A>,
    /// The completion result, parked until the awaiter polls again.
    result: Option<T>,
    /// The awaiter is gone; the completion handler retires the entry rather
    /// than parking a result nobody will collect.
    abandoned: bool,
}

/// One family of outstanding ops, keyed by the op id the SQE's `user_data`
/// carries.
pub(super) struct ParkMap<T, A = ()> {
    ops: RefCell<FxHashMap<u64, Op<T, A>>>,
}

impl<T, A> Default for ParkMap<T, A> {
    fn default() -> Self {
        ParkMap {
            ops: RefCell::new(FxHashMap::default()),
        }
    }
}

impl<T, A> ParkMap<T, A> {
    /// Record `id` as outstanding, holding `carry` until its completion lands.
    pub(super) fn open(&self, id: u64, carry: Option<A>) {
        self.ops.borrow_mut().insert(
            id,
            Op {
                waker: None,
                carry,
                result: None,
                abandoned: false,
            },
        );
    }

    /// Poll `id`'s awaiter, yielding the result together with what the op
    /// carried (the buffer a raw recv wrote into).
    pub(super) fn poll(&self, id: u64, cx: &Context<'_>) -> Poll<(T, Option<A>)> {
        let mut ops = self.ops.borrow_mut();
        let Entry::Occupied(mut e) = ops.entry(id) else {
            panic!("reactor: park slot {id} polled but never opened");
        };
        if e.get().result.is_some() {
            let op = e.remove();
            return Poll::Ready((op.result.unwrap(), op.carry));
        }
        e.get_mut().waker = Some(cx.waker().clone());
        Poll::Pending
    }

    /// Deliver a completion. Retires the op outright when its awaiter is
    /// already gone; otherwise parks the result and wakes. Returns whether the
    /// value reached an awaiter, so a caller with nowhere else to route it can
    /// report the drop.
    pub(super) fn complete(&self, id: u64, value: T) -> bool {
        let waker = {
            let mut ops = self.ops.borrow_mut();
            let Entry::Occupied(mut e) = ops.entry(id) else {
                return false;
            };
            if e.get().abandoned {
                e.remove();
                return false;
            }
            let op = e.get_mut();
            op.result = Some(value);
            op.waker.take()
        };
        if let Some(w) = waker {
            w.wake();
        }
        true
    }

    /// Retire `id` outright. For a family whose completion is not guaranteed
    /// (a W2M reply, which a dead worker never sends), leaving the slot
    /// abandoned would leak it — nothing would ever come to retire it. A later
    /// completion then finds no slot and is discarded, which is the same
    /// outcome [`Self::abandon`] arranges.
    pub(super) fn close(&self, id: u64) {
        self.ops.borrow_mut().remove(&id);
    }

    /// The awaiter is gone, but the kernel will still complete this op — keep
    /// the slot (and what it carries) until it does. Returns whether the op is
    /// in fact still in flight, so the caller can decide to cancel it; a
    /// completion that already landed retires the entry here instead.
    pub(super) fn abandon(&self, id: u64) -> bool {
        let mut ops = self.ops.borrow_mut();
        let Entry::Occupied(mut e) = ops.entry(id) else {
            return false;
        };
        if e.get().result.is_some() {
            e.remove();
            return false;
        }
        let op = e.get_mut();
        op.waker = None;
        op.abandoned = true;
        true
    }

    /// Read what an outstanding op carries.
    pub(super) fn with_carry(&self, id: u64, f: impl FnOnce(&A)) {
        if let Some(carry) = self.ops.borrow().get(&id).and_then(|op| op.carry.as_ref()) {
            f(carry);
        }
    }

    #[cfg(test)]
    pub(super) fn is_open(&self, id: u64) -> bool {
        self.ops.borrow().contains_key(&id)
    }

    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.ops.borrow().len()
    }

    /// Take `id`'s parked result without resolving a future, leaving the slot
    /// open. Lets a test drive a completion handler on its own.
    #[cfg(test)]
    pub(super) fn take_result(&self, id: u64) -> Option<T> {
        self.ops.borrow_mut().get_mut(&id)?.result.take()
    }

    #[cfg(test)]
    pub(super) fn has_waker(&self, id: u64) -> bool {
        self.ops.borrow().get(&id).is_some_and(|op| op.waker.is_some())
    }

    #[cfg(test)]
    pub(super) fn is_abandoned(&self, id: u64) -> bool {
        self.ops.borrow().get(&id).is_some_and(|op| op.abandoned)
    }
}
