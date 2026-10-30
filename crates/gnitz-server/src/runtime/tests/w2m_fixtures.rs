//! Live W2M rings for the tests that need one.

use super::*;
use gnitz_zset::repr::Batch;

/// `nw` rings of `capacity` bytes each, mapped shared and never unmapped, so a
/// thread or forked child may use its end for the rest of the process: every
/// ring's writer, and the reader over all of them.
pub(crate) fn test_rings(nw: usize, capacity: usize) -> (Vec<W2mWriter>, W2mReceiver) {
    rings(nw, capacity).expect("map the test rings")
}

/// The capacity of a ring holding `n_msgs` messages of `msg_sz` bytes plus
/// `slack` spare bytes.
pub(super) fn ring_capacity(msg_sz: usize, n_msgs: usize, slack: u64) -> usize {
    (W2M_HEADER_SIZE as u64 + n_msgs as u64 * slot_stride(msg_sz) + slack) as usize
}

/// The request id `n`, which is not 0.
pub(crate) fn id(n: u32) -> RequestId {
    RequestId::from_wire(n).expect("a request id is not 0")
}

/// One ring of [`ring_capacity`]: its writer and its reader.
pub(super) fn make_ring(msg_sz: usize, n_msgs: usize, slack: u64) -> (W2mWriter, W2mReceiver) {
    let (mut writers, receiver) = test_rings(1, ring_capacity(msg_sz, n_msgs, slack));
    (writers.pop().unwrap(), receiver)
}

impl W2mWriter {
    /// The master has armed its park, and no publish has taken it.
    pub(crate) fn master_parked(&self) -> bool {
        self.master.armed()
    }

    /// Publish one frame of a train answering `id`, `last` ending it, carrying
    /// `rows` if any; the bytes a client forward of it carries.
    pub(crate) fn send_train_frame(&mut self, id: RequestId, last: bool, rows: Option<&Batch>) -> Vec<u8> {
        let msg = WireMsg::train_frame(0, last, rows.and_then(Batch::wire_whole));
        self.send_msg(id, &msg);
        gnitz_wire::framed(&msg.encode_to_vec())
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

/// A fresh ring holding one frame whose error text is `pad` bytes, read back as a
/// slot.
pub(crate) fn ring_slot(pad: usize) -> (W2mReceiver, W2mSlot) {
    let (mut writers, receiver) = test_rings(1, 256 * 1024);
    let text = vec![0x42u8; pad];
    let msg = crate::runtime::frame::WireMsg { blob: &text, ..Default::default() };
    writers[0].send_msg(id(1), &msg);
    let slot = receiver.try_read_slot(0).expect("a frame");
    (receiver, slot)
}
