//! Live W2M rings for the tests that need one.

use super::*;
/// A `capacity`-byte ring, mapped shared and initialized; never unmapped, so
/// a thread or forked child may use it for the rest of the process.
pub(crate) fn test_ring(capacity: usize) -> *mut u8 {
    let ring = gnitz_foundation::posix_io::map_anon_shared(capacity).expect("map a test ring");
    // SAFETY: a fresh mapping of `capacity` bytes, owned by nothing else.
    unsafe { init_region(ring, capacity as u64) };
    ring
}

/// A ring holding `n_msgs` messages of `msg_sz` bytes plus `slack` spare bytes.
pub(crate) fn make_ring(msg_sz: usize, n_msgs: usize, slack: u64) -> *mut u8 {
    test_ring((W2M_HEADER_SIZE as u64 + n_msgs as u64 * slot_stride(msg_sz) + slack) as usize)
}

/// The wake sequence every `SalWake` on `ring` has bumped.
///
/// # Safety
/// As [`test_ring`]: `ring` is a live, initialized region.
pub(crate) unsafe fn sal_wake_seq(ring: *mut u8) -> u64 {
    W2mRingHeader::from_raw(ring).sal_park.cursor.load(Ordering::Acquire)
}

/// The master has armed its `FUTEX_WAITV` park on `ring`.
///
/// # Safety
/// As [`test_ring`]: `ring` is a live, initialized region.
pub(crate) unsafe fn master_parked(ring: *mut u8) -> bool {
    W2mRingHeader::from_raw(ring).master_park.armed()
}
