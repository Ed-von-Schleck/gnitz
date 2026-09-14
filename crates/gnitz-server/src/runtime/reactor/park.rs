//! Park slots: where a reactor future waits for its op's one CQE. An entry lives
//! while the op is outstanding; the CQE of an abandoned one only retires it.

use std::cell::RefCell;
use std::collections::hash_map::Entry;
use std::task::{Context, Poll, Waker};

use rustc_hash::FxHashMap;

use super::{park_waker, SendBody};

/// Where one outstanding op stands.
enum OpState {
    /// No completion yet; the awaiter's waker once it has polled.
    Waiting(Option<Waker>),
    /// The completion result, parked until the awaiter polls again.
    Done(i32),
    /// The awaiter is gone; the completion retires the entry rather than parking
    /// a result nobody will collect.
    Abandoned,
}

/// One outstanding op.
struct Op {
    state: OpState,
    /// What the op owns until its completion lands: memory the kernel may still
    /// read. Dropped with the entry, or handed back with the result.
    carry: Option<SendBody>,
}

/// Every outstanding op, keyed by the op id the SQE's `user_data` carries.
#[derive(Default)]
pub(super) struct ParkMap {
    ops: RefCell<FxHashMap<u64, Op>>,
}

impl ParkMap {
    /// Record `id` as outstanding, holding `carry` until its completion lands.
    pub(super) fn open(&self, id: u64, carry: Option<SendBody>) {
        self.ops
            .borrow_mut()
            .insert(id, Op { state: OpState::Waiting(None), carry });
    }

    /// Poll `id`'s awaiter, yielding the result together with what the op
    /// carried, and retiring the entry.
    pub(super) fn poll(&self, id: u64, cx: &Context<'_>) -> Poll<(i32, Option<SendBody>)> {
        let mut ops = self.ops.borrow_mut();
        let Entry::Occupied(mut e) = ops.entry(id) else {
            panic!("reactor: park slot {id} polled but never opened");
        };
        match &mut e.get_mut().state {
            OpState::Waiting(slot) => park_waker(slot, cx.waker()),
            OpState::Done(rc) => {
                let rc = *rc;
                return Poll::Ready((rc, e.remove().carry));
            }
            OpState::Abandoned => unreachable!("reactor: park slot {id} polled after abandonment"),
        }
        Poll::Pending
    }

    /// Deliver a completion. Retires the op outright when its awaiter is
    /// already gone; otherwise parks the result and wakes.
    pub(super) fn complete(&self, id: u64, res: i32) {
        let waker = {
            let mut ops = self.ops.borrow_mut();
            let Entry::Occupied(mut e) = ops.entry(id) else {
                return;
            };
            match std::mem::replace(&mut e.get_mut().state, OpState::Done(res)) {
                OpState::Waiting(w) => w,
                OpState::Abandoned => {
                    e.remove();
                    return;
                }
                OpState::Done(_) => unreachable!("reactor: op {id} completed twice"),
            }
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
        match e.get().state {
            OpState::Done(_) => {
                e.remove();
            }
            _ => e.get_mut().state = OpState::Abandoned,
        }
    }

    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.ops.borrow().len()
    }

    #[cfg(test)]
    pub(super) fn has_waker(&self, id: u64) -> bool {
        self.ops
            .borrow()
            .get(&id)
            .is_some_and(|op| matches!(op.state, OpState::Waiting(Some(_))))
    }

    #[cfg(test)]
    pub(super) fn is_abandoned(&self, id: u64) -> bool {
        self.ops
            .borrow()
            .get(&id)
            .is_some_and(|op| matches!(op.state, OpState::Abandoned))
    }
}
