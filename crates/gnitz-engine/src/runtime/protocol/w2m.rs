//! W2M (worker→master) SPSC ring: write side (W2mWriter) and read side (W2mReceiver).

use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicU32, Ordering};

use crate::foundation::posix_io;
use crate::runtime::w2m_ring::{self, TryReserve, W2mRingHeader, FLAG_MASTER_PARKED, FLAG_WRITER_PARKED};
use crate::runtime::wire::{decode_wire_ipc, DecodedWire};

// `wait_any` builds a `futex_waitv` word list of at most `num_workers` entries,
// and `num_workers <= MAX_WORKERS`. Pin `MAX_WORKERS <= MAX_FUTEX_WAITV` here —
// the one site that names both constants — so a future `MAX_WORKERS` bump that
// outgrows the syscall cap is a build error, not a silent truncation.
const _: () = assert!(crate::runtime::sal::MAX_WORKERS <= crate::foundation::posix_io::MAX_FUTEX_WAITV);

/// Worker's write side of a single W2M ring.
pub struct W2mWriter {
    region_ptr: *mut u8,
}

unsafe impl Send for W2mWriter {}

impl W2mWriter {
    pub fn new(region_ptr: *mut u8, region_size: u64) -> Self {
        let hdr = unsafe { W2mRingHeader::from_raw(region_ptr as *const u8) };
        assert_eq!(
            hdr.capacity(),
            region_size,
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
            sz,
            w2m_ring::MAX_W2M_MSG,
        );
        let hdr = unsafe { W2mRingHeader::from_raw(self.region_ptr as *const u8) };

        let reservation = loop {
            let r = unsafe { w2m_ring::try_reserve(hdr, self.region_ptr, sz, internal_req_id) };
            match r {
                TryReserve::Ok(r) => break r,
                TryReserve::Full => {
                    let expected = hdr.writer_seq().load(Ordering::Acquire);
                    hdr.waiter_flags().fetch_or(FLAG_WRITER_PARKED, Ordering::AcqRel);
                    let room_now = unsafe { w2m_ring::has_room(hdr, sz) };
                    if !room_now {
                        let rc = posix_io::futex_wait_u32(hdr.writer_seq() as *const AtomicU32, expected, -1);
                        // libc::syscall returns -1 on error (not -errno); read errno
                        // directly. EINTR (signal) and EAGAIN (value already changed)
                        // are both harmless — retry. Anything else is fatal.
                        if rc < 0 {
                            let errno = posix_io::errno();
                            if errno != libc::EINTR && errno != libc::EAGAIN {
                                crate::gnitz_fatal_abort!(
                                    "W2mWriter::send_encoded: futex_wait_u32 failed: \
                                     rc={} errno={}",
                                    rc,
                                    errno,
                                );
                            }
                        }
                    }
                    hdr.waiter_flags().fetch_and(!FLAG_WRITER_PARKED, Ordering::AcqRel);
                }
            }
        };

        unsafe {
            if reservation.slot_len > 0 {
                let slice = std::slice::from_raw_parts_mut(reservation.slot_ptr, reservation.slot_len);
                encode_fn(slice);
            }
            w2m_ring::commit(hdr, reservation);
        }

        hdr.reader_seq().fetch_add(1, Ordering::Release);
        if hdr.waiter_flags().load(Ordering::Acquire) & FLAG_MASTER_PARKED != 0 {
            let rc = posix_io::futex_wake_u32(hdr.reader_seq() as *const AtomicU32, 1);
            if rc < 0 {
                crate::gnitz_fatal_abort!(
                    "W2mWriter::send_encoded: futex_wake_u32 failed: rc={} errno={}",
                    rc,
                    posix_io::errno(),
                );
            }
        }
    }
}

// ---------------------------------------------------------------------------
// InFlightState — tracks outstanding slots and drives consume_cursor forward
// ---------------------------------------------------------------------------

/// Maximum simultaneously in-flight (parked, un-released) W2M slots per
/// worker ring — the capacity of `InFlightState`'s queue and its `completed`
/// bitmap (a u64, so this cannot exceed 64). Every continuation-frame sender
/// must keep one ring's worth of frames below this bound; the reactor's
/// scan-queue debug_assert catches violations.
pub(crate) const W2M_MAX_IN_FLIGHT: usize = 64;

struct InFlightState {
    hdr: &'static W2mRingHeader,
    /// Index of the slot at the front of the queue (oldest in-flight).
    front_idx: u64,
    /// new_vrc for each in-flight slot, stored at push_idx % W2M_MAX_IN_FLIGHT.
    queue: [u64; W2M_MAX_IN_FLIGHT],
    /// Number of in-flight slots; next push_idx = front_idx + len.
    len: u8,
    /// Bit i is set when the slot at position (front_idx + i) has been released.
    completed: u64,
}

impl InFlightState {
    fn new(hdr: &'static W2mRingHeader) -> Self {
        InFlightState {
            hdr,
            front_idx: 0,
            queue: [0u64; W2M_MAX_IN_FLIGHT],
            len: 0,
            completed: 0,
        }
    }

    /// Register a new in-flight slot with the given post-read new_vrc.
    /// Returns the slot's push_idx (used for release).
    fn take(&mut self, new_vrc: u64) -> u64 {
        let idx = self.front_idx + self.len as u64;
        self.queue[(idx % W2M_MAX_IN_FLIGHT as u64) as usize] = new_vrc;
        self.len += 1;
        idx
    }

    /// Mark the slot identified by push_idx as released.
    /// Advances consume_cursor through the completed prefix and wakes the
    /// writer if it advanced.
    fn release(&mut self, push_idx: u64) {
        let bit = push_idx - self.front_idx;
        assert!(
            bit < W2M_MAX_IN_FLIGHT as u64,
            "w2m in-flight bitmap overflow: {} slots simultaneously in-flight (max {})",
            bit + 1,
            W2M_MAX_IN_FLIGHT,
        );
        self.completed |= 1u64 << bit;

        let n = self.completed.trailing_ones() as u64;
        if n > 0 {
            let last_vrc = self.queue[((self.front_idx + n - 1) % W2M_MAX_IN_FLIGHT as u64) as usize];
            // At full capacity (all 64 slots completed) n == 64, and
            // `>>= 64` is a shift-by-width: a debug-build panic, and on
            // release x86_64 a mask to `>> 0` that leaves `completed` full of
            // stale ones, corrupting the next release. Zero it explicitly.
            if n == 64 {
                self.completed = 0;
            } else {
                self.completed >>= n;
            }
            self.front_idx += n;
            self.len -= n as u8;
            self.hdr.advance_consume_cursor(last_vrc);
            self.hdr.writer_seq().fetch_add(1, Ordering::Release);
            if self.hdr.waiter_flags().load(Ordering::Acquire) & FLAG_WRITER_PARKED != 0 {
                let rc = posix_io::futex_wake_u32(self.hdr.writer_seq() as *const AtomicU32, 1);
                if rc < 0 {
                    crate::gnitz_fatal_abort!(
                        "W2mSlot::drop: futex_wake_u32 failed: rc={} errno={}",
                        rc,
                        posix_io::errno(),
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
    bytes: &'static [u8],
    /// Borrowed directly from the ring prefix to forward to `send_buffer` without re-encoding.
    frame: &'static [u8],
    push_idx: u64,
    /// `internal_req_id` from the slot prefix, set by the worker via
    /// `try_reserve`. Used by the master to route scan responses without
    /// decoding the wire frame.
    pub(crate) internal_req_id: u32,
    /// Raw pointer into `W2mReceiver::in_flight[worker]`.
    /// Valid for the slot's lifetime: W2mReceiver outlives all slots
    /// (slots borrow from its mmaps), and the master thread is the
    /// sole accessor of both.
    state: *mut InFlightState,
}

impl W2mSlot {
    pub fn bytes(&self) -> &[u8] {
        self.bytes
    }
    /// The framed bytes ready for `send_buffer`: `[sz_as_u32_le | payload]`.
    pub(crate) fn frame_bytes(&self) -> &[u8] {
        self.frame
    }
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
    in_flight: Vec<UnsafeCell<InFlightState>>,
}

unsafe impl Send for W2mReceiver {}

impl W2mReceiver {
    pub fn new(region_ptrs: Vec<*mut u8>) -> Self {
        let in_flight = region_ptrs
            .iter()
            .map(|&p| {
                let hdr = unsafe { W2mRingHeader::from_raw(p as *const u8) };
                UnsafeCell::new(InFlightState::new(hdr))
            })
            .collect();
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
        let (ptr, sz, new_vrc, req_id) =
            unsafe { w2m_ring::try_consume(hdr, self.region_ptrs[worker] as *const u8, cursor)? };

        hdr.advance_read_cursor(new_vrc);

        let bytes = unsafe { std::slice::from_raw_parts(ptr, sz as usize) };
        // The 4 bytes at ptr-4 hold `sz as u32 LE` (the client length prefix).
        // Together with the payload they form the exact framed buffer that
        // `send_buffer` expects, avoiding a re-encode on the scan egress path.
        let frame = unsafe { std::slice::from_raw_parts(ptr.sub(4), sz as usize + 4) };
        let state = self.in_flight[worker].get();
        let push_idx = unsafe { (*state).take(new_vrc) };

        Some(W2mSlot {
            bytes,
            frame,
            push_idx,
            internal_req_id: req_id,
            state,
        })
    }

    pub fn try_read(&self, worker: usize) -> Option<DecodedWire> {
        let slot = self.try_read_slot(worker)?;
        match decode_wire_ipc(slot.bytes()) {
            Ok(decoded) => Some(decoded),
            Err(e) => crate::gnitz_fatal_abort!(
                "W2mReceiver::try_read: worker={} decode failed: {:?} — ring corrupt",
                worker,
                e,
            ),
        }
    }

    pub fn wait_for(&self, worker: usize, timeout_ms: i32) -> i32 {
        let hdr = unsafe { self.header(worker) };
        let (expected, has_unread) = hdr.arm_master_park();
        let rc = if has_unread {
            0
        } else {
            posix_io::futex_wait_u32(hdr.reader_seq() as *const AtomicU32, expected, timeout_ms)
        };
        hdr.waiter_flags().fetch_and(!FLAG_MASTER_PARKED, Ordering::AcqRel);
        rc
    }

    /// Wait until ANY of `workers` publishes (its `reader_seq` advances) or
    /// `timeout_ms` elapses — the synchronous analogue of the reactor's FUTEX_WAITV.
    /// A single-word `wait_for` only catches a wake on the one worker it parks on;
    /// when a DIFFERENT worker is next to publish, its wake hits a different word and
    /// the call sleeps to the timeout. Waiting on all pending workers' words at once
    /// fixes that. Returns immediately if any worker already has unread data.
    ///
    /// Each ring is armed via `W2mRingHeader::arm_master_park`, which owns the
    /// load-bearing flag-publish → `reader_seq`-snapshot → unread-data-check order.
    /// A worker that bumps `reader_seq` after our snapshot trips `futex_waitv`'s
    /// value-mismatch fast return; one that published before is caught by the
    /// unread-data check (we re-read instead of parking).
    ///
    /// Does not clear `FLAG_MASTER_PARKED` on return (unlike `wait_for`).
    /// Correctness rests on `futex_waitv`'s value-compare against the snapshot, not
    /// on the flag — the flag only gates whether a worker bothers to issue a (cheap)
    /// wake syscall. Leaving it set costs at most a few no-op wakes against unparked
    /// words; once the reactor resumes it re-establishes the flag via
    /// `refresh_futex_waitv_vals`, so there is nothing to restore.
    pub fn wait_any(&self, workers: &[usize], timeout_ms: i32) -> i32 {
        let mut ptrs = [std::ptr::null::<AtomicU32>(); crate::runtime::sal::MAX_WORKERS];
        let mut expected = [0u32; crate::runtime::sal::MAX_WORKERS];
        let mut n = 0;
        for &w in workers {
            let hdr = unsafe { self.header(w) };
            let (exp, has_unread) = hdr.arm_master_park();
            // Already-published worker: don't park, let the caller re-read.
            if has_unread {
                return 0;
            }
            ptrs[n] = hdr.reader_seq() as *const AtomicU32;
            expected[n] = exp;
            n += 1;
        }
        if n == 0 {
            return 0;
        }
        posix_io::futex_waitv_u32(&ptrs[..n], &expected[..n], timeout_ms)
    }

    pub fn num_workers(&self) -> usize {
        self.region_ptrs.len()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::foundation::codec::align8;
    use crate::runtime::w2m_ring::{self, W2mRingHeader, W2M_HEADER_SIZE};
    use crate::test_support::SharedRegion;
    use std::sync::atomic::Ordering;

    /// Allocate a ring that holds at most `n_msgs` messages of `msg_sz` bytes.
    unsafe fn make_ring(msg_sz: usize, n_msgs: usize) -> (SharedRegion, u64) {
        let msg_total = 8 + align8(msg_sz) as u64;
        let capacity = W2M_HEADER_SIZE as u64 + n_msgs as u64 * msg_total + 8;
        let region = SharedRegion::new(capacity as usize);
        w2m_ring::init_region_for_tests(region.ptr(), capacity);
        (region, capacity)
    }

    /// Slots released in push order: consume_cursor advances one step at a time.
    #[test]
    fn test_w2m_slot_in_order_release() {
        unsafe {
            let (region, capacity) = make_ring(64, 4);
            let ptr = region.ptr();
            let writer = W2mWriter::new(ptr, capacity);
            let receiver = W2mReceiver::new(vec![ptr]);
            let hdr = receiver.header(0);

            writer.send_encoded(64, 0, |s| {
                s[0] = 1;
            });
            writer.send_encoded(64, 0, |s| {
                s[0] = 2;
            });

            let slot_a = receiver.try_read_slot(0).expect("slot A");
            let new_vrc_a = hdr.read_cursor().load(Ordering::Acquire);

            let slot_b = receiver.try_read_slot(0).expect("slot B");
            let new_vrc_b = hdr.read_cursor().load(Ordering::Acquire);

            assert_eq!(
                hdr.consume_cursor().load(Ordering::Acquire),
                W2M_HEADER_SIZE as u64,
                "consume_cursor must not advance while slots are in-flight",
            );

            drop(slot_a);
            assert_eq!(
                hdr.consume_cursor().load(Ordering::Acquire),
                new_vrc_a,
                "consume_cursor must advance to new_vrc_a after slot A drop",
            );

            drop(slot_b);
            assert_eq!(
                hdr.consume_cursor().load(Ordering::Acquire),
                new_vrc_b,
                "consume_cursor must advance to new_vrc_b after slot B drop",
            );
        }
    }

    /// Slots released out of push order: consume_cursor only advances when the
    /// contiguous prefix from the head is complete.
    #[test]
    fn test_w2m_slot_out_of_order_release() {
        unsafe {
            let (region, capacity) = make_ring(64, 4);
            let ptr = region.ptr();
            let writer = W2mWriter::new(ptr, capacity);
            let receiver = W2mReceiver::new(vec![ptr]);
            let hdr = receiver.header(0);

            writer.send_encoded(64, 0, |s| {
                s[0] = 1;
            });
            writer.send_encoded(64, 0, |s| {
                s[0] = 2;
            });

            let slot_a = receiver.try_read_slot(0).expect("slot A");
            let slot_b = receiver.try_read_slot(0).expect("slot B");
            let new_vrc_b = hdr.read_cursor().load(Ordering::Acquire);

            let initial_cc = W2M_HEADER_SIZE as u64;

            // Drop B first. B is not the head, so consume_cursor must not advance.
            drop(slot_b);
            assert_eq!(
                hdr.consume_cursor().load(Ordering::Acquire),
                initial_cc,
                "consume_cursor must not advance when non-head slot is released",
            );

            // Drop A. Both A and B complete the prefix — consume_cursor jumps to new_vrc_b.
            drop(slot_a);
            assert_eq!(
                hdr.consume_cursor().load(Ordering::Acquire),
                new_vrc_b,
                "consume_cursor must advance through both A and B on head release",
            );
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
            let region = SharedRegion::new(capacity as usize);
            let ptr = region.ptr();
            w2m_ring::init_region_for_tests(ptr, capacity);

            // Fill the ring (non-blocking direct call).
            let hdr_raw = W2mRingHeader::from_raw(ptr as *const u8);
            match w2m_ring::try_publish(hdr_raw, ptr, msg_sz, |s| {
                s[0] = 1;
            }) {
                w2m_ring::TryPublish::Ok(_) => {}
                w2m_ring::TryPublish::Full => panic!("ring should have room for first message"),
            }

            let writer = W2mWriter::new(ptr, capacity);
            let receiver = W2mReceiver::new(vec![ptr]);

            let (done_tx, done_rx) = std::sync::mpsc::channel::<()>();

            // Spawn a thread that tries to publish a second message.
            // It will block until consume_cursor advances.
            let handle = std::thread::spawn(move || {
                writer.send_encoded(msg_sz, 0, |s| {
                    s[0] = 2;
                });
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
        }
    }

    /// Fix F: releasing a full 64-slot in-flight prefix in one go drives
    /// `trailing_ones()` to 64, so the bare `self.completed >>= 64` would be a
    /// shift-by-width (debug panic / release UB). Release every slot EXCEPT the
    /// head first (nothing retires), then the head — completing all 64 bits at
    /// once — and assert it advances cleanly to the last slot's new_vrc.
    #[test]
    fn release_retires_full_64_prefix_without_ub() {
        unsafe {
            let (region, capacity) = make_ring(8, 64);
            let ptr = region.ptr();
            let writer = W2mWriter::new(ptr, capacity);
            let receiver = W2mReceiver::new(vec![ptr]);
            let hdr = receiver.header(0);

            for i in 0..64u8 {
                writer.send_encoded(8, 0, |s| {
                    s[0] = i;
                });
            }
            let mut slots: Vec<_> = (0..64).map(|_| receiver.try_read_slot(0).expect("slot")).collect();
            // new_vrc of the 64th slot — where consume_cursor must land once the
            // whole prefix retires.
            let last_vrc = hdr.read_cursor().load(Ordering::Acquire);

            // Release every slot except the head: completed fills bits 1..63 but
            // no prefix retires while the front (bit 0) stays unset.
            let head = slots.remove(0);
            slots.clear(); // drops push_idx 1..63
            assert_eq!(
                hdr.consume_cursor().load(Ordering::Acquire),
                W2M_HEADER_SIZE as u64,
                "no prefix may retire until the head releases",
            );

            // Release the head: all 64 bits set ⇒ trailing_ones() == 64 ⇒
            // `completed >>= 64` without the Fix F guard is UB.
            drop(head);
            assert_eq!(
                hdr.consume_cursor().load(Ordering::Acquire),
                last_vrc,
                "head release must retire the full 64-slot prefix to the last new_vrc",
            );
        }
    }

    /// BUG: a single-word `wait_for` misses a wake on another ring. `wait_for(0)`
    /// arms `FLAG_MASTER_PARKED` on ring 0 only, so a publish to ring 3 (flag
    /// clear) issues no wake and ring 0's word never changes — the wait sleeps the
    /// full ceiling even though ring 3 carried the round.
    #[test]
    fn test_wait_for_misses_publish_on_other_ring() {
        unsafe {
            let rings: Vec<(SharedRegion, u64)> = (0..4).map(|_| make_ring(64, 4)).collect();
            let receiver = W2mReceiver::new(rings.iter().map(|r| r.0.ptr()).collect());
            let (pub_ptr, pub_cap) = (rings[3].0.ptr() as usize, rings[3].1);
            let handle = std::thread::spawn(move || {
                std::thread::sleep(std::time::Duration::from_millis(50));
                W2mWriter::new(pub_ptr as *mut u8, pub_cap).send_encoded(64, 0, |s| s[0] = 7);
            });
            let start = std::time::Instant::now();
            let _ = receiver.wait_for(0, 300); // parks on ring 0; ring 3's wake can't reach it
            let elapsed = start.elapsed().as_millis();
            handle.join().unwrap();
            assert!(
                elapsed >= 250,
                "wait_for(0) must sleep the full ceiling, slept {elapsed}ms"
            );
            assert!(receiver.try_read_slot(3).is_some(), "ring 3 really did publish");
        }
    }

    /// FIX: the same setup, but `wait_any([0,1,2,3])` arms `FLAG_MASTER_PARKED` on
    /// ring 3 too, so the publish wakes the multi-word wait well before the ceiling.
    #[test]
    fn test_wait_any_woken_by_publish_on_other_ring() {
        unsafe {
            let rings: Vec<(SharedRegion, u64)> = (0..4).map(|_| make_ring(64, 4)).collect();
            let receiver = W2mReceiver::new(rings.iter().map(|r| r.0.ptr()).collect());
            let (pub_ptr, pub_cap) = (rings[3].0.ptr() as usize, rings[3].1);
            let handle = std::thread::spawn(move || {
                std::thread::sleep(std::time::Duration::from_millis(50));
                W2mWriter::new(pub_ptr as *mut u8, pub_cap).send_encoded(64, 0, |s| s[0] = 7);
            });
            let start = std::time::Instant::now();
            let _ = receiver.wait_any(&[0, 1, 2, 3], 5000); // any ring's wake reaches it
            let elapsed = start.elapsed().as_millis();
            handle.join().unwrap();
            assert!(
                elapsed < 2000,
                "wait_any must be woken by ring 3's publish, slept {elapsed}ms"
            );
            assert!(receiver.try_read_slot(3).is_some(), "ring 3 really did publish");
        }
    }

    /// NEGATIVE: with no publisher, `wait_any` sleeps to the deadline and returns
    /// -1 (the timeout/error convention callers degrade to one extra poll on).
    #[test]
    fn test_wait_any_times_out_with_no_publisher() {
        unsafe {
            let rings: Vec<(SharedRegion, u64)> = (0..1).map(|_| make_ring(64, 4)).collect();
            let receiver = W2mReceiver::new(rings.iter().map(|r| r.0.ptr()).collect());
            let start = std::time::Instant::now();
            let rc = receiver.wait_any(&[0], 200);
            let elapsed = start.elapsed().as_millis();
            assert_eq!(rc, -1, "no publisher → timeout returns -1");
            assert!(
                (150..1000).contains(&elapsed),
                "wait_any should sleep ~200ms, slept {elapsed}ms"
            );
        }
    }
}
