//! Live W2M rings for the tests that need one.

use super::*;

/// One ring per capacity, mapped shared and never unmapped, so a thread or
/// forked child may use its end for the rest of the process: every ring's
/// writer, the reader over all of them, and every ring's wake.
pub(crate) fn test_rings(capacities: impl IntoIterator<Item = usize>) -> (Vec<W2mWriter>, W2mReceiver, Vec<SalWake>) {
    rings(capacities).expect("map the test rings")
}

/// The capacity of a ring holding `n_msgs` messages of `msg_sz` bytes plus
/// `slack` spare bytes.
pub(crate) fn ring_capacity(msg_sz: usize, n_msgs: usize, slack: u64) -> usize {
    (W2M_HEADER_SIZE as u64 + n_msgs as u64 * slot_stride(msg_sz) + slack) as usize
}

/// The wake sequence every `SalWake` on `wake`'s ring has bumped.
pub(crate) fn sal_wake_seq(wake: &SalWake) -> u64 {
    wake.park.cursor.load(Ordering::Acquire)
}

/// The master has armed its `FUTEX_WAITV` park on `worker`'s ring.
pub(crate) fn master_parked(receiver: &W2mReceiver, worker: usize) -> bool {
    receiver.rings[worker].hdr.master_park.armed()
}
