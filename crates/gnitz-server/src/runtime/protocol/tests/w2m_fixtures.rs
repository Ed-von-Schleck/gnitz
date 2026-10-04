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
