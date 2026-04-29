//! W2M (worker→master) SPSC ring: write side (W2mWriter) and read side (W2mReceiver).

use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicU32, Ordering};

use crate::runtime::sys as ipc_sys;
use crate::runtime::w2m_ring::{self, W2mRingHeader, FLAG_MASTER_PARKED, FLAG_WRITER_PARKED, TryReserve};
use crate::runtime::wire::{decode_wire_ipc, DecodedWire};

/// Worker's write side of a single W2M ring.
pub struct W2mWriter {
    region_ptr: *mut u8,
}

unsafe impl Send for W2mWriter {}

impl W2mWriter {
    pub fn new(region_ptr: *mut u8, region_size: u64) -> Self {
        let hdr = unsafe { W2mRingHeader::from_raw(region_ptr as *const u8) };
        assert_eq!(
            hdr.capacity(), region_size,
            "W2mWriter region_size must match ring header capacity",
        );
        W2mWriter { region_ptr }
    }

    /// Encode wire data into the W2M ring. Blocks on `writer_seq` if full.
    /// `internal_req_id` is stored in the slot prefix so the master can route
    /// scan responses without decoding the wire frame.
    pub fn send_encoded(&self, sz: usize, internal_req_id: u32, encode_fn: impl FnOnce(&mut [u8])) {
        assert!(
            (sz as u64) <= w2m_ring::MAX_W2M_MSG,
            "W2mWriter::send_encoded: sz={} exceeds MAX_W2M_MSG={}",
            sz, w2m_ring::MAX_W2M_MSG,
        );
        let hdr = unsafe { W2mRingHeader::from_raw(self.region_ptr as *const u8) };

        let reservation = loop {
            let r = unsafe { w2m_ring::try_reserve(hdr, self.region_ptr, sz, internal_req_id) };
            match r {
                TryReserve::Ok(r) => break r,
                TryReserve::Full => {
                    let expected = hdr.writer_seq().load(Ordering::Acquire);
                    hdr.waiter_flags()
                        .fetch_or(FLAG_WRITER_PARKED, Ordering::AcqRel);
                    let room_now = unsafe { w2m_ring::has_room(hdr, sz) };
                    if !room_now {
                        let rc = ipc_sys::futex_wait_u32(
                            hdr.writer_seq() as *const AtomicU32,
                            expected,
                            -1,
                        );
                        // libc::syscall returns -1 on error (not -errno); read errno
                        // directly. EINTR (signal) and EAGAIN (value already changed)
                        // are both harmless — retry. Anything else is fatal.
                        if rc < 0 {
                            let errno = ipc_sys::errno();
                            if errno != libc::EINTR && errno != libc::EAGAIN {
                                crate::gnitz_fatal_abort!(
                                    "W2mWriter::send_encoded: futex_wait_u32 failed: \
                                     rc={} errno={}",
                                    rc, errno,
                                );
                            }
                        }
                    }
                    hdr.waiter_flags()
                        .fetch_and(!FLAG_WRITER_PARKED, Ordering::AcqRel);
                }
            }
        };

        unsafe {
            if reservation.slot_len > 0 {
                let slice = std::slice::from_raw_parts_mut(
                    reservation.slot_ptr, reservation.slot_len,
                );
                encode_fn(slice);
            }
            w2m_ring::commit(hdr, reservation);
        }

        hdr.reader_seq().fetch_add(1, Ordering::Release);
        if hdr.waiter_flags().load(Ordering::Acquire) & FLAG_MASTER_PARKED != 0 {
            let rc = ipc_sys::futex_wake_u32(hdr.reader_seq() as *const AtomicU32, 1);
            if rc < 0 {
                crate::gnitz_fatal_abort!(
                    "W2mWriter::send_encoded: futex_wake_u32 failed: rc={} errno={}",
                    rc, ipc_sys::errno(),
                );
            }
        }
    }
}

// ---------------------------------------------------------------------------
// InFlightState — tracks outstanding slots and drives consume_cursor forward
// ---------------------------------------------------------------------------

struct InFlightState {
    hdr:       &'static W2mRingHeader,
    /// Index of the slot at the front of the queue (oldest in-flight).
    front_idx: u64,
    /// new_vrc for each in-flight slot, stored at index push_idx % 64.
    /// Capacity: 64 entries ↔ max 64 simultaneous in-flight slots.
    queue:     [u64; 64],
    /// Number of in-flight slots; next push_idx = front_idx + len.
    len:       u8,
    /// Bit i is set when the slot at position (front_idx + i) has been released.
    completed: u64,
}

impl InFlightState {
    fn new(hdr: &'static W2mRingHeader) -> Self {
        InFlightState {
            hdr,
            front_idx: 0,
            queue: [0u64; 64],
            len: 0,
            completed: 0,
        }
    }

    /// Register a new in-flight slot with the given post-read new_vrc.
    /// Returns the slot's push_idx (used for release).
    fn take(&mut self, new_vrc: u64) -> u64 {
        let idx = self.front_idx + self.len as u64;
        self.queue[(idx % 64) as usize] = new_vrc;
        self.len += 1;
        idx
    }

    /// Mark the slot identified by push_idx as released.
    /// Advances consume_cursor through the completed prefix and wakes the
    /// writer if it advanced.
    fn release(&mut self, push_idx: u64) {
        let bit = push_idx - self.front_idx;
        assert!(
            bit < 64,
            "w2m in-flight bitmap overflow: {} slots simultaneously in-flight (max 64)",
            bit + 1,
        );
        self.completed |= 1u64 << bit;

        let n = self.completed.trailing_ones() as u64;
        if n > 0 {
            let last_vrc = self.queue[((self.front_idx + n - 1) % 64) as usize];
            self.completed >>= n;
            self.front_idx += n;
            self.len -= n as u8;
            self.hdr.advance_consume_cursor(last_vrc);
            self.hdr.writer_seq().fetch_add(1, Ordering::Release);
            if self.hdr.waiter_flags().load(Ordering::Acquire) & FLAG_WRITER_PARKED != 0 {
                let rc = ipc_sys::futex_wake_u32(
                    self.hdr.writer_seq() as *const AtomicU32, 1,
                );
                if rc < 0 {
                    crate::gnitz_fatal_abort!(
                        "W2mSlot::drop: futex_wake_u32 failed: rc={} errno={}",
                        rc, ipc_sys::errno(),
                    );
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// W2mSlot — RAII guard holding a ring slot until the caller is done with it
// ---------------------------------------------------------------------------

/// A zero-copy view into a W2M ring slot.
///
/// Dropping advances `consume_cursor` (possibly past multiple slots when
/// out-of-order slots complete a contiguous prefix) and wakes a parked writer.
pub struct W2mSlot {
    bytes:    &'static [u8],
    /// Borrowed directly from the ring prefix to forward to `send_buffer` without re-encoding.
    frame:    &'static [u8],
    push_idx: u64,
    /// `internal_req_id` from the slot prefix, set by the worker via
    /// `try_reserve`. Used by the master to route scan responses without
    /// decoding the wire frame.
    pub(crate) internal_req_id: u32,
    /// Raw pointer into `W2mReceiver::in_flight[worker]`.
    /// Valid for the slot's lifetime: W2mReceiver outlives all slots
    /// (slots borrow from its mmaps), and the master thread is the
    /// sole accessor of both.
    state:    *mut InFlightState,
}

impl W2mSlot {
    pub fn bytes(&self) -> &[u8] { self.bytes }
    /// The framed bytes ready for `send_buffer`: `[sz_as_u32_le | payload]`.
    pub(crate) fn frame_bytes(&self) -> &[u8] { self.frame }
}

impl Drop for W2mSlot {
    fn drop(&mut self) {
        unsafe { (*self.state).release(self.push_idx) };
    }
}

// ---------------------------------------------------------------------------
// W2mReceiver
// ---------------------------------------------------------------------------

/// Master's read side of W2M.
pub struct W2mReceiver {
    region_ptrs: Vec<*mut u8>,
    in_flight:   Vec<UnsafeCell<InFlightState>>,
}

unsafe impl Send for W2mReceiver {}

impl W2mReceiver {
    pub fn new(region_ptrs: Vec<*mut u8>) -> Self {
        let in_flight = region_ptrs.iter().map(|&p| {
            let hdr = unsafe { W2mRingHeader::from_raw(p as *const u8) };
            UnsafeCell::new(InFlightState::new(hdr))
        }).collect();
        W2mReceiver { region_ptrs, in_flight }
    }

    /// # Safety
    /// `worker` must be < `num_workers`.
    #[inline]
    pub unsafe fn header(&self, worker: usize) -> &'static W2mRingHeader {
        W2mRingHeader::from_raw(self.region_ptrs[worker] as *const u8)
    }

    /// Take a slot from the ring without advancing `consume_cursor`.
    ///
    /// `read_cursor` is advanced immediately so the next call can take the
    /// following slot. `consume_cursor` advances only when the returned
    /// `W2mSlot` is dropped, signalling the writer that space is free.
    pub fn try_read_slot(&self, worker: usize) -> Option<W2mSlot> {
        let hdr = unsafe { self.header(worker) };
        let cursor = hdr.read_cursor().load(Ordering::Acquire);
        let (ptr, sz, new_vrc, req_id) = unsafe {
            w2m_ring::try_consume(hdr, self.region_ptrs[worker] as *const u8, cursor)?
        };

        hdr.advance_read_cursor(new_vrc);

        let bytes = unsafe { std::slice::from_raw_parts(ptr, sz as usize) };
        // The 4 bytes at ptr-4 hold `sz as u32 LE` (the client length prefix).
        // Together with the payload they form the exact framed buffer that
        // `send_buffer` expects, avoiding a re-encode on the scan egress path.
        let frame = unsafe { std::slice::from_raw_parts(ptr.sub(4), sz as usize + 4) };
        let state = self.in_flight[worker].get();
        let push_idx = unsafe { (*state).take(new_vrc) };

        Some(W2mSlot { bytes, frame, push_idx, internal_req_id: req_id, state })
    }

    pub fn try_read(&self, worker: usize) -> Option<DecodedWire> {
        let slot = self.try_read_slot(worker)?;
        match decode_wire_ipc(slot.bytes()) {
            Ok(decoded) => Some(decoded),
            Err(e) => crate::gnitz_fatal_abort!(
                "W2mReceiver::try_read: worker={} decode failed: {:?} — ring corrupt",
                worker, e,
            ),
        }
    }

    pub fn wait_for(&self, worker: usize, timeout_ms: i32) -> i32 {
        let hdr = unsafe { self.header(worker) };
        hdr.waiter_flags().fetch_or(FLAG_MASTER_PARKED, Ordering::AcqRel);
        let expected = hdr.reader_seq().load(Ordering::Acquire);
        let wc = hdr.write_cursor().load(Ordering::Acquire);
        let rc_cur = hdr.read_cursor().load(Ordering::Acquire);
        let rc = if wc != rc_cur {
            0
        } else {
            ipc_sys::futex_wait_u32(
                hdr.reader_seq() as *const AtomicU32,
                expected,
                timeout_ms,
            )
        };
        hdr.waiter_flags().fetch_and(!FLAG_MASTER_PARKED, Ordering::AcqRel);
        rc
    }

    pub fn num_workers(&self) -> usize { self.region_ptrs.len() }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering;
    use crate::runtime::w2m_ring::{self, W2mRingHeader, W2M_HEADER_SIZE};
    use gnitz_wire::align8;

    unsafe fn alloc_region(size: usize) -> *mut u8 {
        let ptr = libc::mmap(
            std::ptr::null_mut(),
            size,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_ANONYMOUS | libc::MAP_SHARED,
            -1,
            0,
        );
        assert_ne!(ptr, libc::MAP_FAILED);
        std::ptr::write_bytes(ptr as *mut u8, 0, size);
        ptr as *mut u8
    }

    unsafe fn free_region(ptr: *mut u8, size: usize) {
        libc::munmap(ptr as *mut libc::c_void, size);
    }

    /// Allocate a ring that holds at most `n_msgs` messages of `msg_sz` bytes.
    unsafe fn make_ring(msg_sz: usize, n_msgs: usize) -> (*mut u8, usize, u64) {
        let msg_total = 8 + align8(msg_sz) as u64;
        let capacity = W2M_HEADER_SIZE as u64 + n_msgs as u64 * msg_total + 8;
        let size = capacity as usize;
        let ptr = alloc_region(size);
        w2m_ring::init_region_for_tests(ptr, capacity);
        (ptr, size, capacity)
    }

    /// Slots released in push order: consume_cursor advances one step at a time.
    #[test]
    fn test_w2m_slot_in_order_release() {
        unsafe {
            let (ptr, size, capacity) = make_ring(64, 4);
            let writer = W2mWriter::new(ptr, capacity);
            let receiver = W2mReceiver::new(vec![ptr]);
            let hdr = receiver.header(0);

            writer.send_encoded(64, 0, |s| { s[0] = 1; });
            writer.send_encoded(64, 0, |s| { s[0] = 2; });

            let slot_a = receiver.try_read_slot(0).expect("slot A");
            let new_vrc_a = hdr.read_cursor().load(Ordering::Acquire);

            let slot_b = receiver.try_read_slot(0).expect("slot B");
            let new_vrc_b = hdr.read_cursor().load(Ordering::Acquire);

            assert_eq!(
                hdr.consume_cursor().load(Ordering::Acquire), W2M_HEADER_SIZE as u64,
                "consume_cursor must not advance while slots are in-flight",
            );

            drop(slot_a);
            assert_eq!(
                hdr.consume_cursor().load(Ordering::Acquire), new_vrc_a,
                "consume_cursor must advance to new_vrc_a after slot A drop",
            );

            drop(slot_b);
            assert_eq!(
                hdr.consume_cursor().load(Ordering::Acquire), new_vrc_b,
                "consume_cursor must advance to new_vrc_b after slot B drop",
            );

            free_region(ptr, size);
        }
    }

    /// Slots released out of push order: consume_cursor only advances when the
    /// contiguous prefix from the head is complete.
    #[test]
    fn test_w2m_slot_out_of_order_release() {
        unsafe {
            let (ptr, size, capacity) = make_ring(64, 4);
            let writer = W2mWriter::new(ptr, capacity);
            let receiver = W2mReceiver::new(vec![ptr]);
            let hdr = receiver.header(0);

            writer.send_encoded(64, 0, |s| { s[0] = 1; });
            writer.send_encoded(64, 0, |s| { s[0] = 2; });

            let slot_a = receiver.try_read_slot(0).expect("slot A");
            let slot_b = receiver.try_read_slot(0).expect("slot B");
            let new_vrc_b = hdr.read_cursor().load(Ordering::Acquire);

            let initial_cc = W2M_HEADER_SIZE as u64;

            // Drop B first. B is not the head, so consume_cursor must not advance.
            drop(slot_b);
            assert_eq!(
                hdr.consume_cursor().load(Ordering::Acquire), initial_cc,
                "consume_cursor must not advance when non-head slot is released",
            );

            // Drop A. Both A and B complete the prefix — consume_cursor jumps to new_vrc_b.
            drop(slot_a);
            assert_eq!(
                hdr.consume_cursor().load(Ordering::Acquire), new_vrc_b,
                "consume_cursor must advance through both A and B on head release",
            );

            free_region(ptr, size);
        }
    }

    /// Dropping a slot advances consume_cursor and unparks a blocked writer.
    #[test]
    fn test_w2m_slot_writer_wakeup() {
        unsafe {
            let msg_sz = 64usize;
            let msg_total = 8 + align8(msg_sz) as u64;
            // Ring holds exactly 1 message.
            let capacity = W2M_HEADER_SIZE as u64 + msg_total + 8;
            let size = capacity as usize;
            let ptr = alloc_region(size);
            w2m_ring::init_region_for_tests(ptr, capacity);

            // Fill the ring (non-blocking direct call).
            let hdr_raw = W2mRingHeader::from_raw(ptr as *const u8);
            match w2m_ring::try_publish(hdr_raw, ptr, msg_sz, |s| { s[0] = 1; }) {
                w2m_ring::TryPublish::Ok(_) => {}
                w2m_ring::TryPublish::Full => panic!("ring should have room for first message"),
            }

            let writer = W2mWriter::new(ptr, capacity);
            let receiver = W2mReceiver::new(vec![ptr]);

            let (done_tx, done_rx) = std::sync::mpsc::channel::<()>();

            // Spawn a thread that tries to publish a second message.
            // It will block until consume_cursor advances.
            let handle = std::thread::spawn(move || {
                writer.send_encoded(msg_sz, 0, |s| { s[0] = 2; });
                let _ = done_tx.send(());
            });

            // Give the thread time to park on writer_seq.
            std::thread::sleep(std::time::Duration::from_millis(20));

            // Take and drop the head slot — consume_cursor advances, writer unparks.
            let slot = receiver.try_read_slot(0).expect("slot");
            drop(slot);

            done_rx
                .recv_timeout(std::time::Duration::from_secs(5))
                .expect("writer thread did not complete within 5 seconds");
            handle.join().expect("writer thread panicked");

            free_region(ptr, size);
        }
    }
}
