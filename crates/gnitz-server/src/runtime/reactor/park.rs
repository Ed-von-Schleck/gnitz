//! Park slots: where a reactor future waits for its op's one CQE. An entry lives
//! while the op is outstanding; the CQE of an abandoned one only retires it.

use std::cell::RefCell;
use std::collections::hash_map::Entry;
use std::task::{Context, Poll, Waker};

use rustc_hash::FxHashMap;

/// One outstanding op.
struct Op<T, A> {
    /// The awaiter's waker while it is parked; `None` before its first poll and
    /// once the op is abandoned.
    waker: Option<Waker>,
    /// What the op owns until its completion lands: memory the kernel may still
    /// read. Dropped with the entry, or handed back with the result.
    carry: A,
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
        ParkMap { ops: RefCell::new(FxHashMap::default()) }
    }
}

impl<T, A> ParkMap<T, A> {
    /// Record `id` as outstanding, holding `carry` until its completion lands.
    pub(super) fn open(&self, id: u64, carry: A) {
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
    /// carried, and retiring the entry.
    pub(super) fn poll(&self, id: u64, cx: &Context<'_>) -> Poll<(T, A)> {
        let mut ops = self.ops.borrow_mut();
        let Entry::Occupied(mut e) = ops.entry(id) else {
            panic!("reactor: park slot {id} polled but never opened");
        };
        if e.get().result.is_none() {
            e.get_mut().waker = Some(cx.waker().clone());
            return Poll::Pending;
        }
        let Op { result, carry, .. } = e.remove();
        Poll::Ready((result.expect("checked above"), carry))
    }

    /// Deliver a completion. Retires the op outright when its awaiter is
    /// already gone; otherwise parks the result and wakes.
    pub(super) fn complete(&self, id: u64, value: T) {
        let waker = {
            let mut ops = self.ops.borrow_mut();
            let Entry::Occupied(mut e) = ops.entry(id) else {
                return;
            };
            if e.get().abandoned {
                e.remove();
                return;
            }
            let op = e.get_mut();
            op.result = Some(value);
            op.waker.take()
        };
        if let Some(w) = waker {
            w.wake();
        }
    }

    /// The awaiter is gone, but the kernel will still complete this op — keep
    /// the slot (and what it carries) until it does. A completion that already
    /// landed retires the entry here instead.
    pub(super) fn abandon(&self, id: u64) {
        let mut ops = self.ops.borrow_mut();
        let Entry::Occupied(mut e) = ops.entry(id) else {
            return;
        };
        if e.get().result.is_some() {
            e.remove();
            return;
        }
        let op = e.get_mut();
        op.waker = None;
        op.abandoned = true;
    }

    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.ops.borrow().len()
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
