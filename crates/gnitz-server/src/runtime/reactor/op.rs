//! One-shot ops: an SQE that posts one CQE to one awaiter, the two meeting in a
//! slot freed by the later of them. `fsync` and `child_exit` are two; `conn`
//! pushes the rest.

use std::any::Any;

use super::*;

/// The low 32 bits of a one-shot op's id, its slot. The 24 bits above them
/// that `ID_MASK` keeps count the ops pushed, wrapping, so a cancel still
/// queued when its op completes and the slot is reused (a submit that failed or
/// took only part of the SQ) names its own op, not the slot's next one.
const OP_SLOT_MASK: u64 = 0xFFFF_FFFF;

/// Every one-shot op in flight.
pub(super) struct Ops {
    /// Each op's slot, freed by the later of its CQE and its awaiter's end
    /// (taking the result, or being dropped), and then reused.
    slots: RefCell<Slab<OpSlot>>,
    /// Ops pushed. Each op's id holds this count, cut to the bits above its
    /// slot that `ID_MASK` keeps.
    pushed: Cell<u64>,
}

/// Where a one-shot op's CQE `res` meets its awaiter.
enum OpSlot {
    /// No CQE yet; the waker of the awaiter's last poll.
    Waiting(Option<Waker>),
    /// The CQE's `res`, not yet taken by the awaiter.
    Done(i32),
    /// The awaiter was dropped before the CQE and left here what the SQE
    /// names. The CQE frees the slot.
    Abandoned(Box<dyn Any>),
}

impl Ops {
    pub(super) fn new() -> Self {
        Ops {
            slots: RefCell::new(Slab::new()),
            pushed: Cell::new(0),
        }
    }

    /// Slots held: ops in flight, and results no awaiter has taken.
    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.slots.borrow().len()
    }

    /// The CQE of the op `user_data` names came, with `res`.
    pub(super) fn complete(&self, user_data: u64, res: i32) {
        let slot = (user_data & OP_SLOT_MASK) as usize;
        let mut slots = self.slots.borrow_mut();
        let entry = slots.get_mut(slot).expect("an op's slot lives until its CQE");
        match std::mem::replace(entry, OpSlot::Done(res)) {
            OpSlot::Waiting(waker) => {
                if let Some(w) = waker {
                    w.wake();
                }
            }
            OpSlot::Abandoned(carry) => {
                slots.remove(slot);
                drop(slots);
                // Outside the borrow: the carry may hold the last handle on a
                // connection.
                drop(carry);
            }
            OpSlot::Done(_) => unreachable!("a second CQE for op {user_data:#x}"),
        }
    }
}

/// The awaiter of a one-shot op: yields its CQE `res` and its carry. It holds
/// `carry`, what the SQE names, until the CQE; dropped before it, it leaves
/// `carry` to the slot.
pub(super) struct Op<C: 'static> {
    reactor: Reactor,
    user_data: u64,
    /// `None` once the result is taken and the slot freed.
    carry: Option<C>,
}

impl<C: 'static> Op<C> {
    fn slot(&self) -> usize {
        (self.user_data & OP_SLOT_MASK) as usize
    }

    /// What the SQE names.
    pub(super) fn carry(&self) -> &C {
        self.carry.as_ref().expect("an op holds its carry until its end")
    }

    /// Cancel this op, which has not completed: its CQE comes early,
    /// `-ECANCELED` unless it was completing anyway. Queued like any SQE: its
    /// `user_data` carries the op's count, so a cancel still queued when the op
    /// completes and its slot is reused names no other op.
    pub(super) fn cancel(&self) {
        let sqe = opcode::AsyncCancel::new(self.user_data).build();
        // SAFETY: the SQE names no memory.
        unsafe { self.reactor.ring.borrow_mut().push(sqe, udata(KIND_CANCEL, 0)) };
    }
}

impl<C: 'static + Unpin> Future for Op<C> {
    type Output = (i32, C);

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<(i32, C)> {
        assert!(self.carry.is_some(), "an op polled after its result");
        let slot = self.slot();
        let mut slots = self.reactor.ops.slots.borrow_mut();
        let res = match &mut slots[slot] {
            OpSlot::Done(res) => *res,
            OpSlot::Waiting(waker) => {
                match waker {
                    Some(w) if w.will_wake(cx.waker()) => {}
                    _ => *waker = Some(cx.waker().clone()),
                }
                return Poll::Pending;
            }
            OpSlot::Abandoned(_) => unreachable!("a live op's slot marked abandoned"),
        };
        slots.remove(slot);
        drop(slots);
        Poll::Ready((res, self.carry.take().expect("checked above")))
    }
}

impl<C: 'static> Drop for Op<C> {
    fn drop(&mut self) {
        let Some(carry) = self.carry.take() else { return };
        let slot = self.slot();
        let mut slots = self.reactor.ops.slots.borrow_mut();
        if let OpSlot::Done(_) = slots[slot] {
            // `carry` drops at the end, after the borrow.
            slots.remove(slot);
        } else {
            slots[slot] = OpSlot::Abandoned(Box::new(carry));
        }
    }
}

impl Reactor {
    /// Queue `sqe` as a one-shot op holding `carry` until its CQE, and return
    /// its awaiter, which yields the CQE `res` and `carry`. Not submitted.
    ///
    /// # Safety
    /// As [`IoUringRing::push`]; what `carry` holds counts as valid until the CQE.
    /// `sqe` posts exactly one CQE: [`Ops::complete`] finds an op's slot by its
    /// low bits alone, so a second CQE would complete the slot's next op early
    /// and free that op's carry while its SQE still reads it.
    pub(super) unsafe fn push_op<C: 'static>(&self, sqe: io_uring::squeue::Entry, carry: C) -> Op<C> {
        let slot = self.ops.slots.borrow_mut().insert(OpSlot::Waiting(None)) as u64;
        debug_assert!(slot <= OP_SLOT_MASK, "{slot} ops in flight");
        let pushed = self.ops.pushed.get() + 1;
        self.ops.pushed.set(pushed);
        let user_data = udata(KIND_OP, pushed << OP_SLOT_MASK.count_ones() | slot);
        // SAFETY: forwarded to the caller.
        unsafe { self.ring.borrow_mut().push(sqe, user_data) };
        Op {
            reactor: self.clone(),
            user_data,
            carry: Some(carry),
        }
    }

    /// An fdatasync of `fd`, submitted at its first poll rather than with the
    /// pass's own submit, so it runs while the rest of the pass does. Yields
    /// the CQE `res`: 0, or a negative errno.
    pub(crate) async fn fsync(&self, fd: BorrowedFd<'static>) -> i32 {
        let sqe = opcode::Fsync::new(types::Fd(fd.as_raw_fd()))
            .flags(types::FsyncFlags::DATASYNC)
            .build();
        // SAFETY: `fd` is open for the process's life; the SQE names no memory.
        let op = unsafe { self.push_op(sqe, ()) };
        self.ring.borrow_mut().submit();
        op.await.0
    }

    /// Resolves once child `pid` has exited, leaving it a zombie (`WNOWAIT`) for
    /// a later wait to reap. Yields the CQE `res`: 0, or a negative errno.
    pub(crate) async fn child_exit(&self, pid: libc::pid_t) -> i32 {
        let sqe = opcode::WaitId::new(libc::P_PID, pid as libc::id_t, libc::WEXITED | libc::WNOWAIT).build();
        // SAFETY: the SQE names no memory: with no `infop`, no siginfo is written.
        let op = unsafe { self.push_op(sqe, ()) };
        self.ring.borrow_mut().submit();
        op.await.0
    }
}

#[cfg(test)]
#[path = "tests/op.rs"]
mod tests;
