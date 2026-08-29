//! Shared W2M ring fixtures.
//!
//! The ring module carries no `#[cfg(test)]` items of its own, so these live
//! here, reachable from every `runtime` descendant's test module as
//! `crate::runtime::tests::fixtures`.

use crate::runtime::w2m_ring::{self, RingCursor, W2M_HEADER_SIZE};
use gnitz_engine_testkit::SharedRegion;
use gnitz_wire::align8;

/// A `capacity`-byte ring, mapped and initialized.
///
/// # Safety
/// The caller must keep the returned region alive for as long as anything reads
/// or writes the ring.
pub(crate) unsafe fn test_ring(capacity: usize) -> SharedRegion {
    let region = SharedRegion::new(capacity);
    w2m_ring::init_region(region.ptr(), capacity as u64);
    region
}

/// A ring holding `n_msgs` messages of `msg_sz` bytes plus `slack` spare bytes.
/// `slack` is what the wrap and backpressure tests differ in: it decides whether
/// one more message fits before the physical end.
///
/// # Safety
/// As [`test_ring`].
pub(crate) unsafe fn make_ring(msg_sz: usize, n_msgs: usize, slack: u64) -> SharedRegion {
    let capacity = W2M_HEADER_SIZE as u64 + n_msgs as u64 * (8 + align8(msg_sz) as u64) + slack;
    test_ring(capacity as usize)
}

/// Publish one message directly, without `W2mWriter`'s park loop. Returns the
/// new write cursor and whether the publish SKIP-wrapped — a wrap is exactly a
/// cursor advance longer than the message. `None` when the ring is full.
///
/// # Safety
/// `base` must be a live ring from [`test_ring`], and the caller must be its
/// sole producer.
pub(crate) unsafe fn publish(
    base: *mut u8,
    sz: usize,
    internal_req_id: u32,
    encode: impl FnOnce(&mut [u8]),
) -> Option<(u64, bool)> {
    let mut wc = RingCursor::producer(base);
    let before = wc.virt();
    let mut reservation = w2m_ring::try_reserve(&wc, sz, internal_req_id)?;
    encode(reservation.slot());
    w2m_ring::commit(&mut wc, reservation);
    Some((wc.virt(), wc.virt() - before != (8 + align8(sz)) as u64))
}
