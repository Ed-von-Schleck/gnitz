//! Live W2M rings for the tests that need one.

use super::*;

/// One ring per capacity, mapped shared and never unmapped, so a thread or
/// forked child may use its end for the rest of the process: every ring's
/// writer, and the reader over all of them.
pub(crate) fn test_rings(capacities: &[usize]) -> (Vec<W2mWriter>, W2mReceiver) {
    rings(capacities).expect("map the test rings")
}

/// The capacity of a ring holding `n_msgs` messages of `msg_sz` bytes plus
/// `slack` spare bytes.
pub(super) fn ring_capacity(msg_sz: usize, n_msgs: usize, slack: u64) -> usize {
    (W2M_HEADER_SIZE as u64 + n_msgs as u64 * slot_stride(msg_sz) + slack) as usize
}

/// One ring of [`ring_capacity`]: its writer and its reader.
pub(super) fn make_ring(msg_sz: usize, n_msgs: usize, slack: u64) -> (W2mWriter, W2mReceiver) {
    let (mut writers, receiver) = test_rings(&[ring_capacity(msg_sz, n_msgs, slack)]);
    (writers.pop().unwrap(), receiver)
}

impl W2mWriter {
    /// The master has armed its park, and no publish has taken it.
    pub(crate) fn master_parked(&self) -> bool {
        self.master.armed()
    }
}

impl W2mReceiver {
    pub(super) fn header(&self, worker: usize) -> &'static W2mRingHeader {
        // SAFETY: a `WorkerRing` is `!Sync`, and this borrow ends with the call.
        unsafe { (*self.rings[worker].0.get()).read.header() }
    }

    /// The worker-visible free boundary: everything below it is reusable.
    pub(crate) fn release_cursor(&self, worker: usize) -> u64 {
        self.header(worker).release.value()
    }

    /// The worker's published boundary: everything below it is readable.
    pub(crate) fn write_cursor(&self, worker: usize) -> u64 {
        self.header(worker).write.load(Ordering::Acquire)
    }

    /// The publishes that took the master's arm, each one `FUTEX_WAKE`.
    pub(crate) fn wakes(&self) -> u64 {
        self.master.value()
    }
}
