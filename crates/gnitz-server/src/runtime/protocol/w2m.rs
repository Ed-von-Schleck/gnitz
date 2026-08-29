//! W2M (worker→master) SPSC ring handles: the worker's [`W2mWriter`], the
//! master's [`W2mReceiver`], and the park/wake protocol between them.
//!
//! ## The park rule
//!
//! Each side parks on the cursor whose advance is the condition it waits for:
//! the worker on `release_cursor` (space freed), the master on `write_cursor`
//! (a message published). Both halves below are locked read-modify-writes, and
//! both are load-bearing:
//!
//! - the **parker** publishes its flag with `fetch_or` and re-tests the cursor
//!   after, so a peer that already acted is visible to the re-test;
//! - the **peer** publishes its cursor with a `swap` before reading the flag,
//!   so a flag it reads clear means its cursor store is already globally
//!   visible.
//!
//! Drop either RMW and the store-buffer race is real: the peer reads a
//! stale-clear flag and skips the wake while the parker reads a stale cursor
//! and parks. A fence on one side does not help — it drains that side's store
//! buffer, not the other's — and neither does the futex value-compare, which is
//! a plain kernel read that a still-buffered store makes match spuriously.

use std::cell::{Cell, UnsafeCell};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use io_uring::types::FutexWaitV;

use crate::runtime::posix;
use crate::runtime::w2m_ring::{
    self, ConsumedSlot, Reservation, RingCursor, W2mRingHeader, FLAG_MASTER_ANY, FLAG_MASTER_SYNC, FLAG_MASTER_WAITV,
    FLAG_WRITER_PARKED,
};
use crate::runtime::wire::{decode_wire_ipc, DecodedWire, WireMsg};

// `arm_park` builds a `futex_waitv` word list of at most `num_workers` entries.
// The kernel's `FUTEX_WAITV_MAX` is 128; past it the syscall returns EINVAL,
// which the caller absorbs as a timeout, silently degenerating the relay to
// polling. Make a `MAX_WORKERS` bump that outgrows it a build error.
const _: () = assert!(gnitz_wire::MAX_WORKERS <= 128);

/// The 32-bit futex word aliasing a cursor's low half. One cursor advance is
/// bounded by the ring's data capacity, which the ring module asserts is below
/// `2^32`, so an advance always changes these bits and a parked side's
/// value-compare cannot miss it.
#[inline]
fn futex_word(cursor: &AtomicU64) -> *const AtomicU32 {
    cursor as *const AtomicU64 as *const AtomicU32
}

/// A failed futex syscall is a hang, not an error anyone would see: the peer
/// stays blocked on a queue that has work in it. Outlined so `site` is not
/// spilled onto the stack on the paths that never abort.
#[cold]
#[inline(never)]
fn futex_failed(site: &str, rc: i32) -> ! {
    gnitz_fatal_abort!("{}: futex failed: rc={} errno={}", site, rc, posix::errno());
}

/// Wake a peer parked on `word`, if one is. The caller must already have
/// published `word` with an RMW — see the module docs.
#[inline]
fn wake_if_parked(word: &AtomicU64, flags: &AtomicU32, parked: u32, site: &str) {
    if flags.load(Ordering::Acquire) & parked == 0 {
        return;
    }
    let rc = posix::futex_wake_u32(futex_word(word), 1);
    if rc < 0 {
        futex_failed(site, rc);
    }
}

/// The full-ring path: publish `FLAG_WRITER_PARKED`, re-test below that barrier,
/// then sleep on `release_cursor` until the master retires a slot.
///
/// # Safety
/// The caller must be the sole producer on `wc`'s ring.
#[cold]
#[inline(never)]
unsafe fn park_for_room(wc: &RingCursor, sz: usize, req: u32) -> Reservation {
    let hdr = wc.header();
    loop {
        hdr.waiter_flags.fetch_or(FLAG_WRITER_PARKED, Ordering::AcqRel);
        let vrel = hdr.release_cursor.load(Ordering::Acquire);
        let reserved = w2m_ring::try_reserve(wc, sz, req);
        if reserved.is_none() {
            let rc = posix::futex_wait_u32(futex_word(&hdr.release_cursor), vrel as u32, -1);
            // `libc::syscall` returns -1 on error (not -errno), so read errno.
            // EINTR (a signal) and EAGAIN (the cursor already moved) both just
            // mean retry; anything else is a lost wake.
            if rc < 0 {
                let errno = posix::errno();
                if errno != libc::EINTR && errno != libc::EAGAIN {
                    futex_failed("W2mWriter::park_for_room", rc);
                }
            }
        }
        hdr.waiter_flags.fetch_and(!FLAG_WRITER_PARKED, Ordering::AcqRel);
        if let Some(r) = reserved {
            return r;
        }
    }
}

/// Worker's write side of a single W2M ring.
///
/// The write cursor is memoized here rather than re-derived from the header per
/// message. A worker process is the sole producer on its ring and drives it from
/// one thread, so the `Cell` keeps every send `&self`.
pub struct W2mWriter {
    cursor: Cell<RingCursor>,
}

unsafe impl Send for W2mWriter {}

impl W2mWriter {
    /// The ring's capacity and cursor are read from the header the region was
    /// initialized with, so neither is a parameter.
    pub fn new(region_ptr: *mut u8) -> Self {
        W2mWriter {
            // SAFETY: every W2M region is initialized before the fork that
            // hands it to a worker.
            cursor: Cell::new(unsafe { RingCursor::producer(region_ptr) }),
        }
    }

    /// Send a bare control frame: a status and optional error text, no schema
    /// and no rows. Every ACK and error reply on the ring has this shape.
    pub fn send_status(&self, target_id: u64, request_id: u64, status: u32, error_msg: &[u8]) {
        let msg = WireMsg {
            target_id,
            request_id,
            status,
            error_msg,
            ..Default::default()
        };
        self.send_msg(request_id, &msg);
    }

    /// Encode `msg` into one ring slot tagged `ring_req`. The master reactor
    /// routes a reply by that ring prefix, not by the payload's `request_id`
    /// (chunked-train frames leave that field 0). `encode_ipc` writes no
    /// checksum: the ring is a trusted shared mapping, unlike the SAL.
    pub fn send_msg(&self, ring_req: u64, msg: &WireMsg<'_>) {
        self.send_msg_sized(ring_req, msg, msg.size());
    }

    /// [`Self::send_msg`] for a caller that already sized the message (to check
    /// it against a frame cap); `sz` must be `msg.size()`.
    pub fn send_msg_sized(&self, ring_req: u64, msg: &WireMsg<'_>, sz: usize) {
        self.send_encoded(sz, ring_req as u32, |buf| {
            msg.encode_ipc(buf, 0);
        });
    }

    /// Encode one message into the ring and publish it, blocking on
    /// `release_cursor` while the ring is full. `internal_req_id` rides the slot
    /// prefix so the master can route the reply without decoding the frame.
    pub fn send_encoded(&self, sz: usize, internal_req_id: u32, encode_fn: impl FnOnce(&mut [u8])) {
        // SAFETY: a worker process is the sole producer on its own ring.
        unsafe {
            let mut wc = self.cursor.get();
            let hdr = wc.header();
            let mut reservation = match w2m_ring::try_reserve(&wc, sz, internal_req_id) {
                Some(r) => r,
                None => park_for_room(&wc, sz, internal_req_id),
            };
            encode_fn(reservation.slot());
            w2m_ring::commit(&mut wc, reservation);
            self.cursor.set(wc);

            wake_if_parked(
                &hdr.write_cursor,
                &hdr.waiter_flags,
                FLAG_MASTER_ANY,
                "W2mWriter::send_encoded",
            );
        }
    }
}

// ---------------------------------------------------------------------------
// InFlightState — the master's read cursor and its outstanding slots
// ---------------------------------------------------------------------------

/// Initial capacity hint for a per-worker ring's in-flight queue, and the soft
/// high-water threshold at which the master warns once. The queue is a growable
/// `VecDeque`, so this does not bound correctness: the real backpressure is the
/// ring's byte capacity, which blocks the worker's `send_encoded` once the ring
/// fills. Crossing this many simultaneously-parked slots on one ring means a
/// client is draining unusually slowly, so `take_next` logs one warning to make
/// the condition observable.
const W2M_MAX_IN_FLIGHT: usize = 64;

/// Capacity above which a fully-drained queue is shrunk, handing the heap it
/// grew for a burst back to the allocator. A stalled client can park a ring's
/// worth of tiny frames (millions of entries) before eviction retires them;
/// without this the peak capacity would be retained for the process lifetime.
/// 1024 entries (16 KiB) is far above the steady-state depth (a few), so the
/// shrink fires only after a genuine burst, never on the hot path.
const INFLIGHT_SHRINK_THRESHOLD: usize = 1024;

/// The master's per-ring state: its read cursor, which never leaves this
/// process, and the slots it has handed out but not yet released.
struct InFlightState {
    /// Master-local. `release_cursor <= read <= write_cursor`; the worker gates
    /// on `release_cursor` alone, so nothing outside this process reads it.
    read: RingCursor,
    /// Absolute index of the slot at the front of the queue (oldest in-flight).
    /// A slot's `push_idx` minus `front_idx` is its position in `queue`.
    front_idx: u64,
    /// One entry per in-flight slot, front = oldest: the slot's post-read vrc
    /// paired with a "released" flag set once the slot is dropped. Retirement
    /// pops the front-consecutive released prefix and advances `release_cursor`
    /// to the last popped vrc.
    queue: VecDeque<(u64, bool)>,
    /// One-shot latch for the high-water warning (see `W2M_MAX_IN_FLIGHT`).
    warned: bool,
}

impl InFlightState {
    fn new(read: RingCursor) -> Self {
        InFlightState {
            read,
            front_idx: 0,
            queue: VecDeque::with_capacity(W2M_MAX_IN_FLIGHT),
            warned: false,
        }
    }

    /// Consume the next slot and register it as in-flight in one step, so the
    /// read cursor and the retirement queue cannot disagree about what has been
    /// handed out. Returns the slot and its `push_idx` — its handle for release.
    ///
    /// # Safety
    /// The master must be the sole consumer on this ring.
    unsafe fn take_next(&mut self) -> Option<(ConsumedSlot, u64)> {
        let consumed = w2m_ring::try_consume(&mut self.read)?;
        let push_idx = self.front_idx + self.queue.len() as u64;
        self.queue.push_back((self.read.virt(), false));
        if !self.warned && self.queue.len() > W2M_MAX_IN_FLIGHT {
            self.warned = true;
            gnitz_warn!(
                "w2m: {} reply slots simultaneously in-flight on one ring (soft \
                 threshold {}); a slow client is parking frames — backpressure is \
                 the ring's {} MiB byte capacity",
                self.queue.len(),
                W2M_MAX_IN_FLIGHT,
                w2m_ring::W2M_REGION_SIZE >> 20,
            );
        }
        Some((consumed, push_idx))
    }

    /// Mark the slot identified by `push_idx` as released. Advances
    /// `release_cursor` through the front-consecutive released prefix and wakes
    /// the writer if it advanced. A stale or double `push_idx` (below
    /// `front_idx`, or past the queue tail) indexes out of bounds and panics.
    fn release(&mut self, push_idx: u64) {
        let pos = (push_idx - self.front_idx) as usize;
        self.queue[pos].1 = true;

        let mut last_vrc = None;
        while self.queue.front().is_some_and(|&(_, released)| released) {
            let (vrc, _) = self.queue.pop_front().expect("front just checked present");
            self.front_idx += 1;
            last_vrc = Some(vrc);
        }

        if let Some(vrc) = last_vrc {
            let hdr = self.read.header();
            hdr.release_cursor.swap(vrc, Ordering::AcqRel);
            wake_if_parked(
                &hdr.release_cursor,
                &hdr.waiter_flags,
                FLAG_WRITER_PARKED,
                "W2mSlot::drop",
            );
            // A burst that has fully drained can leave a large heap buffer
            // behind (VecDeque never shrinks on its own); reclaim it.
            if self.queue.is_empty() && self.queue.capacity() > INFLIGHT_SHRINK_THRESHOLD {
                self.queue.shrink_to_fit();
            }
        }
    }
}

// ---------------------------------------------------------------------------
// W2mSlot — RAII guard holding a ring slot until the caller is done with it
// ---------------------------------------------------------------------------

/// A zero-copy view into a W2M ring slot.
///
/// Dropping advances `release_cursor` (possibly past multiple slots when
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
    /// The ring's boxed `InFlightState`, whose address is stable for the
    /// receiver's life. The `W2mReceiver` must outlive every slot: its field is
    /// declared last in `ReactorShared` so it drops after every slot holder.
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

/// One worker's W2M ring on the master side: the shared header plus the master's
/// own bookkeeping for that ring. Bundled (rather than two parallel `Vec`s) so
/// both share one index and one bounds check, and can never drift apart.
struct WorkerRing {
    hdr: &'static W2mRingHeader,
    /// Boxed so the address a `W2mSlot` holds is stable no matter what happens
    /// to the `Vec` around it.
    in_flight: Box<UnsafeCell<InFlightState>>,
}

impl WorkerRing {
    /// The master-local read cursor. Only the master thread touches a ring's
    /// `InFlightState`, and this borrow does not outlive the call.
    #[inline]
    fn read_cursor(&self) -> u64 {
        unsafe { (*self.in_flight.get()).read.virt() }
    }
}

/// Master's read side of W2M.
pub struct W2mReceiver {
    rings: Vec<WorkerRing>,
}

unsafe impl Send for W2mReceiver {}

impl W2mReceiver {
    pub fn new(region_ptrs: Vec<*mut u8>) -> Self {
        let rings = region_ptrs
            .into_iter()
            .map(|p| {
                // SAFETY: every W2M region is initialized before a receiver is
                // built over it.
                let (hdr, read) = unsafe { (W2mRingHeader::from_raw(p), RingCursor::consumer(p)) };
                WorkerRing {
                    hdr,
                    in_flight: Box::new(UnsafeCell::new(InFlightState::new(read))),
                }
            })
            .collect();
        W2mReceiver { rings }
    }

    /// # Safety
    /// `worker` must be < `num_workers`.
    #[cfg(test)]
    pub(crate) unsafe fn header(&self, worker: usize) -> &'static W2mRingHeader {
        self.rings[worker].hdr
    }

    /// Take a slot from the ring without freeing its space. `release_cursor`
    /// advances only when the returned `W2mSlot` is dropped, which is what tells
    /// the writer the bytes are reusable.
    pub fn try_read_slot(&self, worker: usize) -> Option<W2mSlot> {
        let state = self.rings[worker].in_flight.get();
        // SAFETY: the master thread is the sole consumer of every ring.
        let (consumed, push_idx) = unsafe { (*state).take_next() }?;
        Some(W2mSlot {
            bytes: consumed.payload,
            frame: consumed.frame,
            push_idx,
            internal_req_id: consumed.internal_req_id,
            state,
        })
    }

    pub fn try_read(&self, worker: usize) -> Option<DecodedWire> {
        let slot = self.try_read_slot(worker)?;
        match decode_wire_ipc(slot.bytes()) {
            Ok(decoded) => Some(decoded),
            Err(e) => gnitz_fatal_abort!(
                "W2mReceiver::try_read: worker={} decode failed: {:?} — ring corrupt",
                worker,
                e,
            ),
        }
    }

    /// Arm `bit` — the caller's own park flag — on every ring in `mask` and fill
    /// `out` with the futex words to wait on, returning the filled prefix.
    /// `None` means a ring already has unread data: drain and retry, never park.
    fn arm_park<'a>(&self, mask: u64, bit: u32, out: &'a mut [FutexWaitV]) -> Option<&'a [FutexWaitV]> {
        let mut n = 0;
        let mut rest = mask;
        while rest != 0 {
            let w = rest.trailing_zeros() as usize;
            rest &= rest - 1;
            let ring = &self.rings[w];
            let hdr = ring.hdr;
            hdr.waiter_flags.fetch_or(bit, Ordering::AcqRel);
            let vwc = hdr.write_cursor.load(Ordering::Acquire);
            if vwc != ring.read_cursor() {
                return None;
            }
            out[n] = FutexWaitV::new()
                .val(vwc as u32 as u64)
                .uaddr(futex_word(&hdr.write_cursor) as u64)
                .flags(posix::FUTEX2_SIZE_U32);
            n += 1;
        }
        Some(&out[..n])
    }

    fn clear_park(&self, mask: u64, bit: u32) {
        let mut rest = mask;
        while rest != 0 {
            let w = rest.trailing_zeros() as usize;
            rest &= rest - 1;
            self.rings[w].hdr.waiter_flags.fetch_and(!bit, Ordering::AcqRel);
        }
    }

    fn all_rings(&self) -> u64 {
        let n = self.rings.len();
        if n >= 64 {
            u64::MAX
        } else {
            (1u64 << n) - 1
        }
    }

    /// Arm the reactor's persistent `FUTEX_WAITV` park on every ring, filling
    /// `out` with the words its SQE watches.
    pub fn arm_waitv<'a>(&self, out: &'a mut [FutexWaitV]) -> Option<&'a [FutexWaitV]> {
        self.arm_park(self.all_rings(), FLAG_MASTER_WAITV, out)
    }

    /// Drop the reactor's park. The reactor clears before it drains and arms
    /// after, so the flag is set only while it is genuinely blocked — which is
    /// the only window where eliding a worker's wake saves anything.
    pub fn clear_waitv(&self) {
        self.clear_park(self.all_rings(), FLAG_MASTER_WAITV);
    }

    /// Wait until any worker in the `workers` bit mask publishes, or
    /// `timeout_ms` elapses — the synchronous analogue of the reactor's
    /// `FUTEX_WAITV`. Returns immediately if any ring already has unread data.
    ///
    /// `workers` must be the caller's whole pending set: a wake reaches only the
    /// rings this armed, so a narrower mask sleeps to the timeout whenever a
    /// worker outside it carries the round.
    pub fn wait_any(&self, workers: u64, timeout_ms: i32) -> i32 {
        let mut waiters = [FutexWaitV::new(); gnitz_wire::MAX_WORKERS];
        let rc = match self.arm_park(workers, FLAG_MASTER_SYNC, &mut waiters) {
            Some(armed) if !armed.is_empty() => posix::futex_waitv_u32(armed, timeout_ms),
            _ => 0,
        };
        self.clear_park(workers, FLAG_MASTER_SYNC);
        rc
    }

    pub fn num_workers(&self) -> usize {
        self.rings.len()
    }

    /// The master-local read cursor for `worker`.
    #[cfg(test)]
    pub(crate) fn read_cursor(&self, worker: usize) -> u64 {
        self.rings[worker].read_cursor()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::tests::fixtures::{make_ring, publish};
    use crate::runtime::w2m_ring::W2M_HEADER_SIZE;
    use gnitz_engine_testkit::SharedRegion;

    fn release_cursor(recv: &W2mReceiver, w: usize) -> u64 {
        unsafe { recv.header(w) }.release_cursor.load(Ordering::Acquire)
    }

    /// Slots released in push order: release_cursor advances one step at a time.
    #[test]
    fn test_w2m_slot_in_order_release() {
        unsafe {
            let region = make_ring(64, 4, 8);
            let ptr = region.ptr();
            let writer = W2mWriter::new(ptr);
            let receiver = W2mReceiver::new(vec![ptr]);

            writer.send_encoded(64, 0, |s| s[0] = 1);
            writer.send_encoded(64, 0, |s| s[0] = 2);

            let slot_a = receiver.try_read_slot(0).expect("slot A");
            let new_vrc_a = receiver.read_cursor(0);

            let slot_b = receiver.try_read_slot(0).expect("slot B");
            let new_vrc_b = receiver.read_cursor(0);

            assert_eq!(
                release_cursor(&receiver, 0),
                W2M_HEADER_SIZE as u64,
                "release_cursor must not advance while slots are in-flight",
            );

            drop(slot_a);
            assert_eq!(
                release_cursor(&receiver, 0),
                new_vrc_a,
                "release_cursor must advance to new_vrc_a after slot A drop",
            );

            drop(slot_b);
            assert_eq!(
                release_cursor(&receiver, 0),
                new_vrc_b,
                "release_cursor must advance to new_vrc_b after slot B drop",
            );
        }
    }

    /// Slots released out of push order: release_cursor only advances when the
    /// contiguous prefix from the head is complete.
    #[test]
    fn test_w2m_slot_out_of_order_release() {
        unsafe {
            let region = make_ring(64, 4, 8);
            let ptr = region.ptr();
            let writer = W2mWriter::new(ptr);
            let receiver = W2mReceiver::new(vec![ptr]);

            writer.send_encoded(64, 0, |s| s[0] = 1);
            writer.send_encoded(64, 0, |s| s[0] = 2);

            let slot_a = receiver.try_read_slot(0).expect("slot A");
            let slot_b = receiver.try_read_slot(0).expect("slot B");
            let new_vrc_b = receiver.read_cursor(0);

            drop(slot_b);
            assert_eq!(
                release_cursor(&receiver, 0),
                W2M_HEADER_SIZE as u64,
                "release_cursor must not advance when a non-head slot is released",
            );

            drop(slot_a);
            assert_eq!(
                release_cursor(&receiver, 0),
                new_vrc_b,
                "release_cursor must advance through both A and B on head release",
            );
        }
    }

    /// Dropping a slot advances release_cursor and unparks a blocked writer.
    #[test]
    fn test_w2m_slot_writer_wakeup() {
        unsafe {
            let msg_sz = 64usize;
            // Ring holds exactly 1 message.
            let region = make_ring(msg_sz, 1, 8);
            let ptr = region.ptr();
            publish(ptr, msg_sz, 0, |s| s[0] = 1).expect("ring should have room for the first message");

            let receiver = W2mReceiver::new(vec![ptr]);
            let (done_tx, done_rx) = std::sync::mpsc::channel::<()>();

            // This second publish cannot fit; it blocks until a slot retires.
            let region_addr = ptr as usize;
            let handle = std::thread::spawn(move || {
                W2mWriter::new(region_addr as *mut u8).send_encoded(msg_sz, 0, |s| s[0] = 2);
                let _ = done_tx.send(());
            });

            // Give the thread time to park on release_cursor.
            std::thread::sleep(std::time::Duration::from_millis(20));

            let slot = receiver.try_read_slot(0).expect("slot");
            drop(slot);

            done_rx
                .recv_timeout(std::time::Duration::from_secs(5))
                .expect("writer thread did not complete within 5 seconds");
            handle.join().expect("writer thread panicked");
        }
    }

    /// Releasing every slot EXCEPT the head first (nothing retires while the
    /// front stays in-flight), then the head, must retire the whole
    /// front-consecutive prefix in a single drain.
    #[test]
    fn release_retires_full_64_prefix_in_one_drain() {
        unsafe {
            let region = make_ring(8, 64, 8);
            let ptr = region.ptr();
            let writer = W2mWriter::new(ptr);
            let receiver = W2mReceiver::new(vec![ptr]);

            for i in 0..64u8 {
                writer.send_encoded(8, 0, |s| s[0] = i);
            }
            let mut slots: Vec<_> = (0..64).map(|_| receiver.try_read_slot(0).expect("slot")).collect();
            let last_vrc = receiver.read_cursor(0);

            let head = slots.remove(0);
            slots.clear(); // drops push_idx 1..63
            assert_eq!(
                release_cursor(&receiver, 0),
                W2M_HEADER_SIZE as u64,
                "no prefix may retire until the head releases",
            );

            drop(head);
            assert_eq!(
                release_cursor(&receiver, 0),
                last_vrc,
                "head release must retire the full 64-slot prefix to the last new_vrc",
            );
        }
    }

    unsafe fn in_flight_cap(recv: &W2mReceiver, w: usize) -> usize {
        (*recv.rings[w].in_flight.get()).queue.capacity()
    }
    unsafe fn in_flight_len(recv: &W2mReceiver, w: usize) -> usize {
        (*recv.rings[w].in_flight.get()).queue.len()
    }

    /// The in-flight queue must sail well past 64 slots: take 200 (all
    /// in-flight at once), release them in a deterministic scramble, and assert
    /// `release_cursor` always sits at the new_vrc of the longest
    /// front-consecutive released prefix.
    #[test]
    fn release_past_64_in_flight_out_of_order() {
        unsafe {
            const N: usize = 200;
            let region = make_ring(8, N, 8);
            let ptr = region.ptr();
            let writer = W2mWriter::new(ptr);
            let receiver = W2mReceiver::new(vec![ptr]);

            for i in 0..N {
                writer.send_encoded(8, 0, |s| s[0] = i as u8);
            }
            let mut slots: Vec<Option<W2mSlot>> = Vec::with_capacity(N);
            let mut vrcs = Vec::with_capacity(N);
            for _ in 0..N {
                slots.push(Some(receiver.try_read_slot(0).expect("slot")));
                vrcs.push(receiver.read_cursor(0));
            }
            assert_eq!(in_flight_len(&receiver, 0), N, "all N slots tracked in-flight");
            assert_eq!(
                release_cursor(&receiver, 0),
                W2M_HEADER_SIZE as u64,
                "release_cursor must not advance while every slot is in-flight",
            );

            // Deterministic scramble (73 is coprime to 200, so this is a
            // permutation): release order is out of push order, exercising the
            // partial-prefix retirement at depth > 64.
            let order: Vec<usize> = {
                let mut o: Vec<usize> = (0..N).collect();
                o.sort_by_key(|&i| (i * 73 + 11) % N);
                o
            };
            let mut released = [false; N];
            for &idx in &order {
                slots[idx] = None; // drop → release(push_idx = idx)
                released[idx] = true;
                let mut p = 0;
                while p < N && released[p] {
                    p += 1;
                }
                let expected = if p == 0 { W2M_HEADER_SIZE as u64 } else { vrcs[p - 1] };
                assert_eq!(
                    release_cursor(&receiver, 0),
                    expected,
                    "release_cursor must track the front-consecutive released prefix (p={p})",
                );
            }
            assert_eq!(in_flight_len(&receiver, 0), 0, "queue fully drained");
        }
    }

    /// A burst grows the in-flight queue well past its initial capacity; once it
    /// fully drains, `release` hands the grown buffer back to the allocator so a
    /// single stalled-client burst does not permanently retain the peak.
    #[test]
    fn release_shrinks_queue_after_burst_drains() {
        unsafe {
            const N: usize = 4096; // comfortably above INFLIGHT_SHRINK_THRESHOLD
            let region = make_ring(8, N, 8);
            let ptr = region.ptr();
            let writer = W2mWriter::new(ptr);
            let receiver = W2mReceiver::new(vec![ptr]);

            for i in 0..N {
                writer.send_encoded(8, 0, |s| s[0] = i as u8);
            }
            let mut slots: Vec<W2mSlot> = (0..N).map(|_| receiver.try_read_slot(0).expect("slot")).collect();
            assert!(
                in_flight_cap(&receiver, 0) >= N,
                "queue must grow to hold the whole burst",
            );

            for s in slots.drain(..) {
                drop(s);
            }
            assert_eq!(in_flight_len(&receiver, 0), 0, "queue fully drained");
            assert!(
                in_flight_cap(&receiver, 0) <= INFLIGHT_SHRINK_THRESHOLD,
                "drained queue must be shrunk back to/below the threshold, got cap {}",
                in_flight_cap(&receiver, 0),
            );
        }
    }

    /// BUG: a mask naming one ring misses a wake on another. Parking on ring 0
    /// alone arms the flag there only, so a publish to ring 3 (flag clear) issues
    /// no wake and ring 0's word never changes — the wait sleeps the full ceiling
    /// even though ring 3 carried the round. This is why every caller passes its
    /// whole pending set.
    #[test]
    fn test_wait_any_on_one_ring_misses_publish_on_another() {
        unsafe {
            let rings: Vec<SharedRegion> = (0..4).map(|_| make_ring(64, 4, 8)).collect();
            let receiver = W2mReceiver::new(rings.iter().map(|r| r.ptr()).collect());
            let pub_ptr = rings[3].ptr() as usize;
            let handle = std::thread::spawn(move || {
                std::thread::sleep(std::time::Duration::from_millis(50));
                W2mWriter::new(pub_ptr as *mut u8).send_encoded(64, 0, |s| s[0] = 7);
            });
            let start = std::time::Instant::now();
            let _ = receiver.wait_any(1, 300); // parks on ring 0; ring 3's wake can't reach it
            let elapsed = start.elapsed().as_millis();
            handle.join().unwrap();
            assert!(
                elapsed >= 250,
                "a ring-0-only mask must sleep the full ceiling, slept {elapsed}ms"
            );
            assert!(receiver.try_read_slot(3).is_some(), "ring 3 really did publish");
        }
    }

    /// FIX: the same setup, but `wait_any(0b1111)` arms ring 3 too, so the
    /// publish wakes the multi-word wait well before the ceiling.
    #[test]
    fn test_wait_any_woken_by_publish_on_other_ring() {
        unsafe {
            let rings: Vec<SharedRegion> = (0..4).map(|_| make_ring(64, 4, 8)).collect();
            let receiver = W2mReceiver::new(rings.iter().map(|r| r.ptr()).collect());
            let pub_ptr = rings[3].ptr() as usize;
            let handle = std::thread::spawn(move || {
                std::thread::sleep(std::time::Duration::from_millis(50));
                W2mWriter::new(pub_ptr as *mut u8).send_encoded(64, 0, |s| s[0] = 7);
            });
            let start = std::time::Instant::now();
            let _ = receiver.wait_any(0b1111, 5000); // any ring's wake reaches it
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
            let rings: Vec<SharedRegion> = (0..1).map(|_| make_ring(64, 4, 8)).collect();
            let receiver = W2mReceiver::new(rings.iter().map(|r| r.ptr()).collect());
            let start = std::time::Instant::now();
            let rc = receiver.wait_any(1, 200);
            let elapsed = start.elapsed().as_millis();
            assert_eq!(rc, -1, "no publisher → timeout returns -1");
            assert!(
                (150..1000).contains(&elapsed),
                "wait_any should sleep ~200ms, slept {elapsed}ms"
            );
        }
    }

    /// Publish/drain throughput over one ring, and how often a publish finds a
    /// master parked — the predicate the wake gate reads, and so the quantity
    /// every change to the park protocol moves.
    ///
    /// A forked child publishes `N` control frames as fast as it can while the
    /// parent drains them, parking on the ring whenever it runs dry. Only the
    /// ratios carry meaning: absolute rates swing with machine load. The park
    /// rate is the gate's predicate sampled once per publish, not a syscall
    /// count — for that, run `strace -f -c -e trace=futex` over the same test.
    ///
    /// `cd crates && cargo test -p gnitz-server --release w2m_publish_drain_bench -- --ignored --nocapture --test-threads=1`
    #[test]
    #[ignore]
    fn w2m_publish_drain_bench() {
        use gnitz_wire::control::CTRL_BLOCK_SIZE_NO_BLOB;
        use std::hint::black_box;
        use std::time::Instant;

        const N: u64 = 200_000;
        const RING_FRAMES: usize = 64;

        let region = unsafe { make_ring(CTRL_BLOCK_SIZE_NO_BLOB, RING_FRAMES, 8) };
        let ptr = region.ptr();
        // Two u64s the child fills before `_exit`: publishes that saw a master
        // park armed, and the child's own elapsed nanos.
        let counters = SharedRegion::new(4096);
        let cptr = counters.ptr() as *mut u64;
        unsafe { std::ptr::write_bytes(counters.ptr(), 0, 4096) };

        let pid = unsafe { libc::fork() };
        assert!(pid >= 0, "fork failed");
        if pid == 0 {
            let writer = W2mWriter::new(ptr);
            let hdr = unsafe { W2mRingHeader::from_raw(ptr) };
            let t = Instant::now();
            let mut saw_parked = 0u64;
            for req in 1..=N {
                writer.send_status(0, req, gnitz_wire::STATUS_OK, &[]);
                if hdr.waiter_flags.load(Ordering::Relaxed) & FLAG_MASTER_ANY != 0 {
                    saw_parked += 1;
                }
            }
            unsafe {
                cptr.write(saw_parked);
                cptr.add(1).write(t.elapsed().as_nanos() as u64);
                libc::_exit(0);
            }
        }

        let receiver = W2mReceiver::new(vec![ptr]);
        let t = Instant::now();
        let (mut drained, mut parks) = (0u64, 0u64);
        while drained < N {
            match receiver.try_read_slot(0) {
                Some(slot) => {
                    black_box(slot.bytes());
                    drained += 1;
                }
                None => {
                    parks += 1;
                    receiver.wait_any(1, 100);
                }
            }
        }
        let drain_ns = t.elapsed().as_nanos() as u64;

        let mut status = 0;
        unsafe { libc::waitpid(pid, &mut status, 0) };
        let (saw_parked, publish_ns) = unsafe { (cptr.read(), cptr.add(1).read()) };

        println!(
            "w2m publish/drain N={N} ring={RING_FRAMES} frames: \
             drain {:.2} Mmsg/s, publish {:.2} Mmsg/s, \
             master parked at {:.1}% of publishes, {parks} drain parks ({:.1} per 1k msgs)",
            N as f64 * 1000.0 / drain_ns as f64,
            N as f64 * 1000.0 / publish_ns as f64,
            saw_parked as f64 * 100.0 / N as f64,
            parks as f64 * 1000.0 / N as f64,
        );
    }

    /// `wait_any` must leave no park flag behind: a stale bit would make every
    /// later publish spend a `FUTEX_WAKE` on a word nobody is parked on.
    #[test]
    fn wait_any_clears_its_park_flag() {
        unsafe {
            let region = make_ring(64, 4, 8);
            let receiver = W2mReceiver::new(vec![region.ptr()]);
            let _ = receiver.wait_any(1, 50);
            assert_eq!(
                receiver.header(0).waiter_flags.load(Ordering::Acquire) & FLAG_MASTER_SYNC,
                0,
                "wait_any must clear its own park flag on return",
            );
        }
    }
}
