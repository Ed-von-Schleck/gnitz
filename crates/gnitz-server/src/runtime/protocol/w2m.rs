//! W2M: the worker→master SPSC transport — one `MAP_SHARED` tail-chasing ring
//! per worker, the worker's [`W2mWriter`], the master's [`W2mReceiver`] and the
//! anonymous shared mapping they live in. [`create`] owns that mapping: it maps
//! the rings and builds their ends, and no end has a constructor of its own, so
//! a ring has one writer and one reader.
//!
//! ## Layout
//!
//! A 128-byte [`W2mRingHeader`] followed by `DCAP = capacity - W2M_HEADER_SIZE`
//! bytes of data. `write_cursor` (worker-written) and `release_cursor`
//! (master-written) are monotonic **virtual** offsets counting bytes from 0,
//! never physical positions; the master's read cursor sits between them —
//! `release <= read <= write` — and never leaves this process.
//! `phys(v) = HEADER + v % DCAP`, memoized in a [`RingCursor`] so the modulo
//! stays off both hot paths.
//!
//! When a message would not fit before `capacity`, the writer stamps
//! `SKIP_MARKER` at the current physical position and publishes at the data
//! head instead; the reader jumps the same way, so markers are transparent.
//!
//! ## The park rule
//!
//! Each side parks on the other's cursor word, through its [`Park`]: the worker
//! on `release_cursor` (space freed), the master on `write_cursor` (a message
//! published).

use std::cell::UnsafeCell;
use std::collections::VecDeque;

use io_uring::types::FutexWaitV;

use super::park::{futex_waitv_entry, map_anon_shared, Park};
use crate::runtime::wire::WireMsg;
use gnitz_wire::control::{peek_control_block, DecodedControl};

// ---------------------------------------------------------------------------
// Geometry
// ---------------------------------------------------------------------------

/// Fixed header size at the start of every W2M mmap region.
const W2M_HEADER_SIZE: usize = 128;

/// Max-size frames the ring holds at once: how far a worker streams ahead while
/// the master drains another ring.
const W2M_WINDOW_FRAMES: u64 = 16;

/// Capacity (header + data) of each per-worker W2M mmap region. The ring sweeps
/// all of it every lap, so all of it becomes resident whatever the live occupancy.
const W2M_REGION_SIZE: usize =
    (W2M_HEADER_SIZE as u64 + W2M_WINDOW_FRAMES * slot_stride(gnitz_wire::MAX_FRAME_PAYLOAD)) as usize;
const _: () = assert!(
    W2M_WINDOW_FRAMES >= 2,
    "a `MAX_FRAME_PAYLOAD` message must fit past a SKIP pad of nearly a whole slot, or `try_publish` \
     admits a message `publish_at` can never place and the writer parks forever",
);

// `arm_waitv` builds a `futex_waitv` word list of `num_workers` entries. The
// kernel's `FUTEX_WAITV_MAX` is 128; past it the wait fails with EINVAL. Make a
// `MAX_WORKERS` bump that outgrows it a build error.
const _: () = assert!(super::sal::MAX_WORKERS <= 128);

const _: () = assert!(
    2 * gnitz_wire::FRAME_LEN_PREFIX_BYTES as u64 == RING_PREFIX_BYTES,
    "the packed slot prefix is two client-prefix-sized halves"
);
const _: () = assert!(
    cfg!(target_endian = "little"),
    "the client's frame prefix is the u64's high half only on LE"
);

/// Size-prefix sentinel for "skip to the header, the real message is there".
const SKIP_MARKER: u64 = u64::MAX;

/// Width of the prefix word stamped before every slot, and of a SKIP marker.
const RING_PREFIX_BYTES: u64 = 8;

/// Bytes one `sz`-byte message occupies in the ring: its prefix word and its
/// payload padded to the prefix's alignment.
#[inline]
const fn slot_stride(sz: usize) -> u64 {
    RING_PREFIX_BYTES + sz.next_multiple_of(8) as u64
}

/// The 8-byte prefix stamped in front of every slot: `sz` in the high half,
/// `internal_req_id` in the low. Written native-endian, so on LE the high half
/// lands at `slot_ptr - 4` as `sz as u32 LE` — which is exactly the client's
/// frame length prefix, and is why [`W2mSlot::frame_bytes`] needs no re-encode.
#[inline]
fn pack_prefix(sz: usize, internal_req_id: u32) -> u64 {
    (internal_req_id as u64) | ((sz as u64) << 32)
}

/// The `(sz, internal_req_id)` [`pack_prefix`] stored.
#[inline]
fn unpack_prefix(prefix: u64) -> (u32, u32) {
    ((prefix >> 32) as u32, prefix as u32)
}

// ---------------------------------------------------------------------------
// Header layout (128 bytes, one cache line per writer)
// ---------------------------------------------------------------------------

/// Cross-process ring state: the two cursor words, each alone on its line.
///
/// ```text
/// line A — worker writes, master reads and parks
///    0   write     write_cursor: the message boundary
/// line B — master writes, worker reads and parks
///   64   release   release_cursor: the reusable-bytes boundary
/// ```
#[repr(C, align(64))]
struct W2mRingHeader {
    /// `write_cursor`: the worker publishes it, the master reads it and parks on it.
    write: Park,
    _pad_write: [u8; 56],
    /// `release_cursor`: the master publishes it, the worker reads it and parks on it.
    release: Park,
    _pad_release: [u8; 56],
}

const _: () = assert!(std::mem::size_of::<W2mRingHeader>() == W2M_HEADER_SIZE);
const _: () = assert!(std::mem::align_of::<W2mRingHeader>() == 64);
const _: () = assert!(std::mem::offset_of!(W2mRingHeader, write) == 0);
const _: () = assert!(std::mem::offset_of!(W2mRingHeader, release) == 64);

impl W2mRingHeader {
    /// Reinterpret the start of a W2M mmap region as its header. The mapping is
    /// created before `fork` and unmapped only at process exit, which is what
    /// the `'static` rests on.
    ///
    /// # Safety
    /// `ptr` must be the base of a ring [`rings`] mapped.
    #[inline]
    unsafe fn from_raw(ptr: *const u8) -> &'static Self {
        &*(ptr as *const W2mRingHeader)
    }
}

// ---------------------------------------------------------------------------
// RingCursor — the virtual cursor and its memoized physical mirror
// ---------------------------------------------------------------------------

/// One side's cursor over one ring: the virtual monotonic offset, its physical
/// mirror, and the mapping both index. Carrying the base is what lets every ring
/// operation take a single argument, so no call can pair a cursor with the wrong
/// region; [`Self::advance`] is the only way to move it, and keeps the two
/// offsets in step without a division.
#[derive(Clone, Copy)]
struct RingCursor {
    base: *mut u8,
    virt: u64,
    phys: u64,
    cap: u64,
}

impl RingCursor {
    /// Store the 8-byte prefix word — [`pack_prefix`] or [`SKIP_MARKER`] — at `at`.
    ///
    /// # Safety
    /// `at + 8` must lie within the ring, and the caller must be its producer.
    #[inline]
    unsafe fn store_prefix(&self, at: u64, val: u64) {
        (self.base.add(at as usize) as *mut u64).write_unaligned(val);
    }

    /// Load the 8-byte prefix word at physical offset `at`.
    ///
    /// # Safety
    /// `at + 8` must lie within the ring, below the published write cursor.
    #[inline]
    unsafe fn load_prefix(&self, at: u64) -> u64 {
        (self.base.add(at as usize) as *const u64).read_unaligned()
    }

    #[inline]
    fn header(&self) -> &'static W2mRingHeader {
        // SAFETY: `rings` builds every cursor, over a ring it mapped.
        unsafe { W2mRingHeader::from_raw(self.base) }
    }

    /// Bytes from the physical position to the end of the region.
    #[inline]
    fn room_to_end(&self) -> u64 {
        self.cap - self.phys
    }

    #[inline]
    fn dcap(&self) -> u64 {
        self.cap - W2M_HEADER_SIZE as u64
    }

    /// Move both forms forward by `delta` bytes. Every publish is gated on
    /// `delta <= DCAP`, so one conditional subtract restores `phys < cap`.
    #[inline]
    fn advance(&mut self, delta: u64) {
        debug_assert!(delta <= self.dcap(), "advance {delta} exceeds DCAP {}", self.dcap());
        self.virt += delta;
        self.phys += delta;
        if self.phys >= self.cap {
            self.phys -= self.dcap();
        }
    }
}

// ---------------------------------------------------------------------------
// create / publish
// ---------------------------------------------------------------------------

/// `nw` workers' rings, each a fresh shared mapping never unmapped: every worker's
/// write end in rank order, and the master's read end over all of them.
pub(crate) fn create(nw: usize) -> std::io::Result<(Vec<W2mWriter>, W2mReceiver)> {
    rings(&vec![W2M_REGION_SIZE; nw])
}

/// [`create`] over rings of `capacities` bytes: its body, and the test rings'.
/// A zeroed mapping is an empty ring with both cursors at 0, so nothing is
/// stored here.
fn rings(capacities: &[usize]) -> std::io::Result<(Vec<W2mWriter>, W2mReceiver)> {
    let mut writers = Vec::with_capacity(capacities.len());
    let mut readers = Vec::with_capacity(capacities.len());
    for &capacity in capacities {
        assert!(
            capacity as u64 >= W2M_HEADER_SIZE as u64 + slot_stride(1),
            "W2M capacity={capacity} leaves no room for a message past the {W2M_HEADER_SIZE}-byte header",
        );
        assert!(
            (capacity as u64).is_multiple_of(RING_PREFIX_BYTES),
            "W2M capacity={capacity} must be {RING_PREFIX_BYTES}-byte aligned, or the SKIP path's \
             prefix-word writes at the physical end cross the mapping",
        );
        assert!(
            capacity < u32::MAX as usize,
            "W2M capacity={capacity} must stay under 2^32: a cursor advances by at most the ring's \
             capacity, and only under 2^32 does an advance always move the low half a futex compares",
        );
        let base = map_anon_shared(capacity)?;
        // Before the first touch. Shared anonymous memory is shmem-backed, so the
        // hint is the kernel's to honour under `shmem_enabled=advise`.
        // SAFETY: the `capacity` bytes just mapped.
        unsafe { libc::madvise(base.cast(), capacity, libc::MADV_HUGEPAGE) };
        let start = RingCursor {
            base,
            virt: 0,
            phys: W2M_HEADER_SIZE as u64,
            cap: capacity as u64,
        };
        writers.push(W2mWriter { wc: start });
        readers.push(WorkerRing {
            hdr: start.header(),
            in_flight: UnsafeCell::new(InFlightState::new(start)),
        });
    }
    // Leaked with the mappings: a slot releases through its ring for as long as
    // it lives.
    let rings = Box::leak(readers.into_boxed_slice());
    Ok((writers, W2mReceiver { rings }))
}

/// Where a `total`-byte message goes: `Some((write_at, pad))`, a physical byte
/// offset and the bytes skipped to reach it. `pad == 0` is a contiguous publish;
/// `pad != 0` means stamp a SKIP marker at the current position and publish at
/// the header instead. `None` when the publish would lap the reader.
#[inline]
fn publish_at(wc: &RingCursor, vrel: u64, total: u64) -> Option<(u64, u64)> {
    debug_assert!(vrel <= wc.virt, "cursor inversion: vrel={vrel} vwc={}", wc.virt);
    let used = wc.virt - vrel;
    debug_assert!(used <= wc.dcap(), "used {used} exceeds DCAP {}", wc.dcap());
    let room_to_end = wc.room_to_end();
    if total <= room_to_end {
        (used + total <= wc.dcap()).then_some((wc.phys, 0))
    } else {
        // `cap` is 8-aligned and every advance is a multiple of 8, so `phys` is
        // 8-aligned and strictly below `cap` — the marker always fits without
        // the contiguous test above reserving room for it.
        debug_assert!(
            room_to_end >= RING_PREFIX_BYTES,
            "no room for a SKIP marker at phys={}",
            wc.phys
        );
        (used + room_to_end + total <= wc.dcap()).then_some((W2M_HEADER_SIZE as u64, room_to_end))
    }
}

// ---------------------------------------------------------------------------
// W2mWriter — the worker's write side
// ---------------------------------------------------------------------------

/// Worker's write side of a single W2M ring: its one producer, so every send
/// takes `&mut self`. The write cursor is memoized here rather than re-derived
/// from the header per message.
pub struct W2mWriter {
    wc: RingCursor,
}

// SAFETY: the raw `base` inside the cursor is a `MAP_SHARED` region valid in
// every process for its whole life, and moving the writer moves the ring's one
// producer.
unsafe impl Send for W2mWriter {}

impl W2mWriter {
    /// Where a `total`-byte slot goes now; `None` while the ring is full.
    fn placement(&self, total: u64) -> Option<(u64, u64)> {
        publish_at(&self.wc, self.wc.header().release.value(), total)
    }

    /// Publish one `sz`-byte message tagged `req`, written by `encode`. False,
    /// writing nothing, when the ring has no room for it now.
    fn try_publish(&mut self, req: u32, sz: usize, encode: impl FnOnce(&mut [u8])) -> bool {
        let total = slot_stride(sz);
        assert!(sz > 0, "w2m: an empty message is a slot `take_next` aborts on");
        assert!(
            total <= self.wc.dcap(),
            "w2m: sz={sz} exceeds this ring's {}-byte capacity — it would never fit",
            self.wc.dcap(),
        );
        let Some((write_at, pad)) = self.placement(total) else {
            return false;
        };
        // SAFETY: this writer is its ring's one producer; `placement` sized the
        // slot against the free space, and no reader touches a byte at or past
        // `write_cursor` before the publish below.
        unsafe {
            if pad != 0 {
                self.wc.store_prefix(self.wc.phys, SKIP_MARKER);
            }
            self.wc.store_prefix(write_at, pack_prefix(sz, req));
            let slot = self.wc.base.add((write_at + RING_PREFIX_BYTES) as usize);
            encode(std::slice::from_raw_parts_mut(slot, sz));
        }
        self.wc.advance(pad + total);
        // Carries the Release that makes the slot's bytes visible to the reader.
        self.wc.header().write.publish(self.wc.virt, "W2mWriter::try_publish");
        true
    }

    /// Encode `msg` into one ring slot, unless the ring has no room for the
    /// frame now. `ring_req` is the slot's prefix, which is what the master
    /// routes the reply by.
    pub fn try_send_msg(&mut self, ring_req: u32, msg: &WireMsg<'_>) -> bool {
        self.try_publish(ring_req, msg.size(), |slot| msg.encode(slot))
    }

    /// [`Self::try_send_msg`], parking on `release_cursor` while the ring is full.
    pub fn send_msg(&mut self, ring_req: u32, msg: &WireMsg<'_>) {
        let sz = msg.size();
        while !self.try_publish(ring_req, sz, |slot| msg.encode(slot)) {
            self.wait_for_room(sz);
        }
    }

    /// The full-ring path: sleep until a release, unless the ring has room
    /// already. Only this producer consumes room, so room found here is still
    /// there for the publish that follows; a release that gave too little is
    /// followed by another call, whose arm the next release sees.
    #[cold]
    #[inline(never)]
    fn wait_for_room(&self, sz: usize) {
        let release = &self.wc.header().release;
        release.park("W2mWriter::wait_for_room", || self.placement(slot_stride(sz)).is_none());
    }

    /// ACK `request_id`: the bare `Ok` control frame.
    pub fn send_ack(&mut self, request_id: u32) {
        self.send_msg(request_id, &WireMsg::default());
    }

    /// The master has armed its `FUTEX_WAITV` park on this ring.
    #[cfg(test)]
    pub(crate) fn master_parked(&self) -> bool {
        self.wc.header().write.armed()
    }
}

// ---------------------------------------------------------------------------
// InFlightState — the master's read cursor and its outstanding slots
// ---------------------------------------------------------------------------

/// Initial capacity of a ring's in-flight queue. Not a bound: the queue grows,
/// and the real backpressure is the ring's byte capacity, which parks the worker
/// in [`W2mWriter::wait_for_room`] once the ring fills.
const INFLIGHT_INITIAL_CAP: usize = 64;

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
}

impl InFlightState {
    fn new(read: RingCursor) -> Self {
        InFlightState {
            read,
            front_idx: 0,
            queue: VecDeque::with_capacity(INFLIGHT_INITIAL_CAP),
        }
    }

    /// Mark the slot identified by `push_idx` as released. Advances
    /// `release_cursor` through the front-consecutive released prefix and wakes
    /// the writer if it advanced.
    fn release(&mut self, push_idx: u64) {
        let pos = (push_idx - self.front_idx) as usize;
        self.queue[pos].1 = true;

        let mut last_vrc = None;
        while let Some(&(vrc, true)) = self.queue.front() {
            self.queue.pop_front();
            self.front_idx += 1;
            last_vrc = Some(vrc);
        }

        if let Some(vrc) = last_vrc {
            self.read.header().release.publish(vrc, "W2mSlot release");
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
    /// Length prefix and payload, borrowed in place: `frame_bytes` hands it to a
    /// client send unchanged, and [`Self::bytes`] is its tail.
    frame: &'static [u8],
    push_idx: u64,
    /// `internal_req_id` from the slot prefix, set by the worker's publish. Used
    /// by the master to route scan responses without decoding the wire frame.
    pub(crate) internal_req_id: u32,
    /// The worker whose ring this slot was read from.
    pub(crate) worker: u32,
    /// The ring it was read from, whose in-flight queue its drop releases through.
    ring: &'static WorkerRing,
}

impl W2mSlot {
    /// The wire message alone, without the length prefix in front of it.
    pub fn bytes(&self) -> &[u8] {
        &self.frame[gnitz_wire::FRAME_LEN_PREFIX_BYTES..]
    }
    /// The framed bytes ready for a client send: `[sz_as_u32_le | payload]`.
    pub(crate) fn frame_bytes(&self) -> &[u8] {
        self.frame
    }

    /// The slot's control header, aborting on failure: the ring is a trusted
    /// mapping, so a malformed slot is corruption.
    pub(crate) fn control(&self) -> DecodedControl {
        match peek_control_block(self.bytes()) {
            Ok(ctrl) => ctrl,
            Err(e) => gnitz_fatal_abort!(
                "w2m: worker={} slot control decode failed: {} — ring corrupt",
                self.worker,
                e
            ),
        }
    }
}

impl Drop for W2mSlot {
    fn drop(&mut self) {
        // SAFETY: only the master thread touches a ring's `InFlightState`, and
        // this borrow does not outlive the call.
        unsafe { (*self.ring.in_flight.get()).release(self.push_idx) };
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
    in_flight: UnsafeCell<InFlightState>,
}

impl WorkerRing {
    /// The master-local read cursor. Only the master thread touches a ring's
    /// `InFlightState`, and this borrow does not outlive the call.
    #[inline]
    fn read_cursor(&self) -> u64 {
        unsafe { (*self.in_flight.get()).read.virt }
    }

    /// Read the next message and register it as in-flight in one step, so the
    /// read cursor and the retirement queue cannot disagree about what has been
    /// handed out. `None` **iff** the ring is empty; every other departure from
    /// the layout aborts. SKIP markers are transparent.
    ///
    /// No byte at or above `vwc` is ever touched, so a torn slot is unreachable
    /// and nothing here guards against one — the writer's [`Park::publish`] is
    /// what puts a slot fully below `vwc` before the load below can see it.
    ///
    /// # Safety
    /// The master must be the sole consumer on this ring.
    #[inline]
    unsafe fn take_next(&'static self, worker: u32) -> Option<W2mSlot> {
        let st = &mut *self.in_flight.get();
        let base: *const u8 = st.read.base;
        let vwc = self.hdr.write.value();
        debug_assert!(
            st.read.virt <= vwc,
            "read cursor {} ahead of write cursor {vwc}",
            st.read.virt
        );
        if st.read.virt == vwc {
            return None;
        }
        let mut prefix = st.read.load_prefix(st.read.phys);
        if prefix == SKIP_MARKER {
            let pad = st.read.room_to_end();
            st.read.advance(pad);
            if st.read.virt == vwc {
                gnitz_fatal_abort!(
                    "w2m::take_next: SKIP marker at phys={} with no message past it — ring corrupt",
                    st.read.phys,
                );
            }
            prefix = st.read.load_prefix(st.read.phys);
        }
        let (sz, internal_req_id) = unpack_prefix(prefix);
        // `slot_stride(0)` is 8, so a zero-size slot would pass the span test.
        if sz == 0 || st.read.virt + slot_stride(sz as usize) > vwc {
            gnitz_fatal_abort!(
                "w2m::take_next: size={} at phys={} overruns the write cursor {} — ring corrupt",
                sz,
                st.read.phys,
                vwc,
            );
        }
        let payload_at = (st.read.phys + RING_PREFIX_BYTES) as usize;
        let frame = std::slice::from_raw_parts(
            base.add(payload_at - gnitz_wire::FRAME_LEN_PREFIX_BYTES),
            sz as usize + gnitz_wire::FRAME_LEN_PREFIX_BYTES,
        );
        st.read.advance(slot_stride(sz as usize));

        let push_idx = st.front_idx + st.queue.len() as u64;
        st.queue.push_back((st.read.virt, false));
        Some(W2mSlot {
            frame,
            push_idx,
            internal_req_id,
            worker,
            ring: self,
        })
    }
}

/// Master's read side of W2M.
pub struct W2mReceiver {
    /// Leaked by [`rings`], so a `W2mSlot` may outlive the receiver it was read
    /// through.
    rings: &'static [WorkerRing],
}

impl W2mReceiver {
    /// Take a slot from the ring without freeing its space. `release_cursor`
    /// advances only when the returned `W2mSlot` is dropped, which is what tells
    /// the writer the bytes are reusable.
    pub fn try_read_slot(&self, worker: usize) -> Option<W2mSlot> {
        // SAFETY: the master thread is the sole consumer of every ring.
        unsafe { self.rings[worker].take_next(worker as u32) }
    }

    /// Arm the reactor's `FUTEX_WAITV` park on every ring, filling `out` with the
    /// words its SQE watches. `None` means a ring already has unread data: every
    /// ring is disarmed again, and the caller drains and retries, never parks.
    pub fn arm_waitv<'a>(&self, out: &'a mut [FutexWaitV]) -> Option<&'a [FutexWaitV]> {
        for (w, ring) in self.rings.iter().enumerate() {
            let park = &ring.hdr.write;
            let vwc = park.arm();
            if vwc != ring.read_cursor() {
                self.clear_waitv();
                return None;
            }
            out[w] = futex_waitv_entry(park.futex_word(), vwc as u32);
        }
        Some(&out[..self.rings.len()])
    }

    /// Drop the reactor's park on the rings no publish has taken it from.
    pub fn clear_waitv(&self) {
        for ring in self.rings.iter() {
            ring.hdr.write.disarm();
        }
    }

    pub fn num_workers(&self) -> usize {
        self.rings.len()
    }

    #[cfg(test)]
    fn header(&self, worker: usize) -> &'static W2mRingHeader {
        self.rings[worker].hdr
    }

    /// The worker-visible free boundary: everything below it is reusable.
    #[cfg(test)]
    pub(crate) fn release_cursor(&self, worker: usize) -> u64 {
        self.rings[worker].hdr.release.value()
    }

    /// The worker's published boundary: everything below it is readable.
    #[cfg(test)]
    pub(crate) fn write_cursor(&self, worker: usize) -> u64 {
        self.rings[worker].hdr.write.value()
    }

    /// The master-local read cursor for `worker`.
    #[cfg(test)]
    fn read_cursor(&self, worker: usize) -> u64 {
        self.rings[worker].read_cursor()
    }
}

/// The ring fixture, shared by `w2m`'s own suites and by the `runtime` tests
/// that need a live ring. A child of `w2m`, so `rings` and the cursor
/// primitives stay private to it.
#[cfg(test)]
#[path = "tests/w2m_fixtures.rs"]
pub(crate) mod fixtures;

#[cfg(test)]
#[path = "tests/w2m.rs"]
mod tests;

#[cfg(test)]
#[path = "benches/w2m.rs"]
mod bench;
