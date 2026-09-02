//! Live W2M rings for the tests that need one.

use super::*;
use crate::runtime::test_support::SharedRegion;

/// A `capacity`-byte ring, mapped and initialized.
///
/// # Safety
/// The caller must keep the returned region alive for as long as anything reads
/// or writes the ring.
pub(crate) unsafe fn test_ring(capacity: usize) -> SharedRegion {
    let region = SharedRegion::new(capacity);
    init_region(region.ptr(), capacity as u64);
    region
}

/// A ring holding `n_msgs` messages of `msg_sz` bytes plus `slack` spare bytes.
/// `slack` is what the wrap and backpressure tests differ in: it decides whether
/// one more message fits before the physical end.
///
/// # Safety
/// As [`test_ring`].
pub(crate) unsafe fn make_ring(msg_sz: usize, n_msgs: usize, slack: u64) -> SharedRegion {
    let capacity = W2M_HEADER_SIZE as u64 + n_msgs as u64 * slot_stride(msg_sz) + slack;
    test_ring(capacity as usize)
}
