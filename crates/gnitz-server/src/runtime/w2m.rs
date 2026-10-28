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
//! `release <= read <= write` — and never leaves this process. A
//! [`RingCursor`] keeps the physical position of its virtual offset, and a
//! [`Placement`] is where the next message goes.
//!
//! ## The park rule
//!
//! The worker parks on `release_cursor` (space freed), through that word's
//! [`Park`]. The master parks on one word of its own over all the rings, whose
//! arm a publish on any ring takes.

use std::cell::UnsafeCell;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use super::park::Park;
use crate::runtime::frame::WireMsg;
use gnitz_foundation::posix_io;
use gnitz_wire::{peek_control_block, DecodedControl};

// ---------------------------------------------------------------------------
// Geometry
// ---------------------------------------------------------------------------

/// Fixed header size at the start of every W2M mmap region.
const W2M_HEADER_SIZE: usize = 128;

/// Max-size frames the ring holds at once: how far a worker streams ahead while
/// the master drains another ring.
const W2M_WINDOW_FRAMES: u64 = 16;

/// Capacity (header + data) of each per-worker W2M mmap region. Resident as
/// far as a ring has ever got between two [`Placement::Rewind`]s: no page is
/// given back.
const W2M_REGION_SIZE: usize =
    (W2M_HEADER_SIZE as u64 + W2M_WINDOW_FRAMES * slot_stride(gnitz_wire::MAX_FRAME_PAYLOAD)) as usize;

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
/// `request_id` in the low, so that on LE the 4 bytes in front of the
/// payload are the client's frame length prefix.
#[inline]
fn pack_prefix(sz: usize, request_id: u32) -> u64 {
    (request_id as u64) | ((sz as u64) << 32)
}

/// The `(sz, request_id)` [`pack_prefix`] stored.
#[inline]
fn unpack_prefix(prefix: u64) -> (usize, u32) {
    ((prefix >> 32) as usize, prefix as u32)
}

const _: () = assert!(
    2 * gnitz_wire::FRAME_LEN_PREFIX_BYTES as u64 == RING_PREFIX_BYTES,
    "the packed slot prefix is two client-prefix-sized halves"
);
const _: () = assert!(
    cfg!(target_endian = "little"),
    "the client's frame prefix is the u64's high half only on LE"
);

// ---------------------------------------------------------------------------
// Reserved request ids
// ---------------------------------------------------------------------------

/// The request id of a group no worker answers.
pub(crate) const NO_REPLY_REQUEST_ID: u32 = 0;

/// The request id every worker's boot ACK carries; the master leases it before
/// its loop first runs.
pub(crate) const BOOT_READY_REQUEST_ID: u32 = 1;

// ---------------------------------------------------------------------------
// Header layout (128 bytes, one cache line per writer)
// ---------------------------------------------------------------------------

/// Cross-process ring state: what the worker publishes on one cache line, what
/// the master publishes on the other, a [`Park`] being a cache line of its own.
#[repr(C)]
struct W2mRingHeader {
    /// `write_cursor`: the worker publishes it, the master reads it.
    write: AtomicU64,
    /// The virtual offset the data head stands for, stored ahead of the publish
    /// of the message there.
    origin: AtomicU64,
    /// `release_cursor`: the master publishes it, the worker reads it and parks on it.
    release: Park,
}

const _: () = assert!(std::mem::size_of::<W2mRingHeader>() == W2M_HEADER_SIZE);
const _: () = assert!(std::mem::offset_of!(W2mRingHeader, release) == 64);

// ---------------------------------------------------------------------------
// RingCursor — the virtual cursor and its memoized physical mirror
// ---------------------------------------------------------------------------

/// One side's cursor over one ring: a virtual offset, the physical position it
/// stands at, and the mapping.
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
        // SAFETY: `rings` builds every cursor over the base of a ring it mapped,
        // created before `fork` and never unmapped.
        unsafe { &*(self.base as *const W2mRingHeader) }
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

    /// Move both forms forward by `delta <= DCAP` bytes.
    #[inline]
    fn advance(&mut self, delta: u64) {
        debug_assert!(delta <= self.dcap(), "advance {delta} exceeds DCAP {}", self.dcap());
        self.virt += delta;
        self.phys += delta;
        if self.phys >= self.cap {
            self.phys -= self.dcap();
        }
    }

    /// Back to the data head, the virtual offset unmoved.
    #[inline]
    fn rewind(&mut self) {
        self.phys = W2M_HEADER_SIZE as u64;
    }

    /// The physical position is where the ring's origin puts the virtual offset.
    fn in_step(&self) -> bool {
        let origin = self.header().origin.load(Ordering::Acquire);
        self.phys == W2M_HEADER_SIZE as u64 + (self.virt - origin) % self.dcap()
    }
}

// ---------------------------------------------------------------------------
// create / publish
// ---------------------------------------------------------------------------

/// `nw` workers' rings and the master's park, each a fresh shared mapping never
/// unmapped: every worker's write end in rank order, and the master's read end
/// over all of them.
pub(crate) fn create(nw: usize) -> std::io::Result<(Vec<W2mWriter>, W2mReceiver)> {
    rings(nw, W2M_REGION_SIZE)
}

/// [`create`] over `nw` rings of `capacity` bytes: its body, and the test rings'.
/// A zeroed mapping is an empty ring with both cursors and its origin at 0, and
/// an unarmed park, so nothing is stored here.
fn rings(nw: usize, capacity: usize) -> std::io::Result<(Vec<W2mWriter>, W2mReceiver)> {
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
    let master: &'static Park = &Park::map(1)?[0];
    let mut writers = Vec::with_capacity(nw);
    let mut readers = Vec::with_capacity(nw);
    for _ in 0..nw {
        let base = posix_io::map_shared(None, capacity)?;
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
        writers.push(W2mWriter { wc: start, master });
        readers.push(WorkerRing(UnsafeCell::new(InFlightState::new(start))));
    }
    // Leaked with the mappings: a slot releases through its ring for as long as
    // it lives.
    let rings = Box::leak(readers.into_boxed_slice());
    Ok((writers, W2mReceiver { rings, master }))
}

/// How a message gets from the write position to its slot.
#[derive(Clone, Copy)]
enum Placement {
    /// It starts at the write position.
    Here,
    /// The ring is empty: it starts at the data head, which becomes this offset.
    Rewind,
    /// The tail is too short for it: it starts at the data head, past the tail.
    Skip,
}

// ---------------------------------------------------------------------------
// W2mWriter — the worker's write side
// ---------------------------------------------------------------------------

/// Worker's write side of a single W2M ring: its one producer, so every send
/// takes `&mut self`.
pub struct W2mWriter {
    wc: RingCursor,
    /// The master's park, which a publish takes.
    master: &'static Park,
}

// SAFETY: the raw `base` inside the cursor and the master's park are
// `MAP_SHARED` regions valid in every process for their whole life, and moving
// the writer moves the ring's one producer.
unsafe impl Send for W2mWriter {}

impl W2mWriter {
    /// Where a `total`-byte slot goes now, `total <= DCAP`; `None` while the
    /// publish would lap the reader.
    #[inline]
    fn placement(&self, total: u64) -> Option<Placement> {
        let wc = &self.wc;
        let vrel = wc.header().release.value();
        debug_assert!(vrel <= wc.virt, "cursor inversion: vrel={vrel} vwc={}", wc.virt);
        let used = wc.virt - vrel;
        debug_assert!(used <= wc.dcap(), "used {used} exceeds DCAP {}", wc.dcap());
        if used == 0 {
            return Some(Placement::Rewind);
        }
        let room_to_end = wc.room_to_end();
        if total <= room_to_end {
            (used + total <= wc.dcap()).then_some(Placement::Here)
        } else {
            debug_assert!(
                room_to_end >= RING_PREFIX_BYTES,
                "no room for a SKIP marker at phys={}",
                wc.phys
            );
            (used + room_to_end + total <= wc.dcap()).then_some(Placement::Skip)
        }
    }

    /// Publish one `sz`-byte message tagged `request_id`, written by `encode`.
    /// False, writing nothing, when the ring has no room for it now.
    fn try_publish(&mut self, request_id: u32, sz: usize, encode: impl FnOnce(&mut [u8])) -> bool {
        let total = slot_stride(sz);
        assert!(sz > 0, "w2m: an empty message is a slot `take_next` aborts on");
        assert!(
            total <= self.wc.dcap(),
            "w2m: sz={sz} exceeds this ring's {}-byte capacity — it would never fit",
            self.wc.dcap(),
        );
        let Some(placement) = self.placement(total) else {
            return false;
        };
        // SAFETY: this writer is its ring's one producer, and `placement` found
        // the bytes unborrowed: past `write_cursor`, or all of an empty ring.
        unsafe {
            match placement {
                Placement::Here => {}
                Placement::Rewind => {
                    self.wc.rewind();
                    self.wc.header().origin.store(self.wc.virt, Ordering::Release);
                }
                Placement::Skip => {
                    self.wc.store_prefix(self.wc.phys, SKIP_MARKER);
                    self.wc.advance(self.wc.room_to_end());
                }
            }
            debug_assert!(self.wc.in_step());
            self.wc.store_prefix(self.wc.phys, pack_prefix(sz, request_id));
            let slot = self.wc.base.add((self.wc.phys + RING_PREFIX_BYTES) as usize);
            encode(std::slice::from_raw_parts_mut(slot, sz));
        }
        self.wc.advance(total);
        // SeqCst for the master's park; it also carries the Release that makes
        // the slot's bytes visible to the reader.
        self.wc.header().write.store(self.wc.virt, Ordering::SeqCst);
        self.master.take_arm();
        true
    }

    /// Encode `msg` into one ring slot, unless the ring has no room for the
    /// frame now. `request_id` goes in the slot's prefix, which is what the
    /// master routes the reply by.
    pub fn try_send_msg(&mut self, request_id: u32, msg: &WireMsg<'_>) -> bool {
        self.try_publish(request_id, msg.size(), |slot| msg.encode(slot))
    }

    /// [`Self::try_send_msg`], parking on `release_cursor` while the ring is full.
    pub fn send_msg(&mut self, request_id: u32, msg: &WireMsg<'_>) {
        while !self.try_send_msg(request_id, msg) {
            self.wait_for_room(msg.size());
        }
    }

    /// The full-ring path: sleep until a release, unless the ring has room
    /// already.
    #[cold]
    #[inline(never)]
    fn wait_for_room(&self, sz: usize) {
        let release = &self.wc.header().release;
        release.park(|| self.placement(slot_stride(sz)).is_none());
    }

    /// ACK `request_id`: the bare `Ok` control frame.
    pub fn send_ack(&mut self, request_id: u32) {
        self.send_msg(request_id, &WireMsg::default());
    }
}

// ---------------------------------------------------------------------------
// InFlightState — the master's read cursor and its outstanding slots
// ---------------------------------------------------------------------------

/// The master's per-ring state: its read cursor, which never leaves this
/// process, and the slots it has handed out but not yet released.
struct InFlightState {
    /// Master-local. `release_cursor <= read <= write_cursor`; the worker gates
    /// on `release_cursor` alone, so nothing outside this process reads it.
    read: RingCursor,
    /// Absolute index of the slot at the front of the queue (oldest in-flight).
    /// A slot's `push_idx` minus `front_idx` is its position in `queue`.
    front_idx: u64,
    /// One entry per in-flight slot, front = oldest: the offset the slot ends
    /// at, and whether it has been released.
    queue: VecDeque<(u64, bool)>,
}

impl InFlightState {
    fn new(read: RingCursor) -> Self {
        InFlightState {
            read,
            front_idx: 0,
            queue: VecDeque::new(),
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
            self.read.header().release.publish(vrc);
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
    /// The request id the worker's publish tagged the slot with: what the master
    /// routes it by without decoding the frame.
    pub(crate) request_id: u32,
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
        // SAFETY: a `WorkerRing` is `!Sync`, and this borrow ends with the call.
        unsafe { (*self.ring.0.get()).release(self.push_idx) };
    }
}

// ---------------------------------------------------------------------------
// W2mReceiver
// ---------------------------------------------------------------------------

/// One worker's W2M ring on the master side: the master's read cursor, through
/// which it reaches the shared header, and its in-flight slots, behind the cell
/// its slots release through.
struct WorkerRing(UnsafeCell<InFlightState>);

impl WorkerRing {
    /// The worker has published past the master's read cursor.
    #[inline]
    fn has_unread(&self) -> bool {
        // SAFETY: a `WorkerRing` is `!Sync`, and this borrow ends with the call.
        let read = unsafe { &(*self.0.get()).read };
        read.header().write.load(Ordering::SeqCst) != read.virt
    }

    /// Read the next message and register it as in-flight. `None` **iff** the
    /// ring is empty; every departure from the layout aborts.
    #[inline]
    fn take_next(&'static self, worker: u32) -> Option<W2mSlot> {
        // SAFETY: a `WorkerRing` is `!Sync`, and this borrow ends with the call.
        let st = unsafe { &mut *self.0.get() };
        let hdr = st.read.header();
        let vwc = hdr.write.load(Ordering::Acquire);
        debug_assert!(
            st.read.virt <= vwc,
            "read cursor {} ahead of write cursor {vwc}",
            st.read.virt
        );
        if st.read.virt == vwc {
            return None;
        }
        // Loaded behind the `vwc` that shows the message at this offset.
        if hdr.origin.load(Ordering::Acquire) == st.read.virt {
            debug_assert!(st.queue.is_empty(), "a slot in flight across a rewind");
            st.read.rewind();
        }
        // SAFETY: `read < vwc`, and a publish puts a whole slot below `vwc`,
        // the marker in front of it included.
        let mut prefix = unsafe { st.read.load_prefix(st.read.phys) };
        if prefix == SKIP_MARKER {
            let pad = st.read.room_to_end();
            st.read.advance(pad);
            if st.read.virt == vwc {
                gnitz_fatal_abort!(
                    "w2m::take_next: SKIP marker at phys={} with no message past it — ring corrupt",
                    st.read.phys,
                );
            }
            // SAFETY: as above.
            prefix = unsafe { st.read.load_prefix(st.read.phys) };
        }
        debug_assert!(st.read.in_step());
        let (sz, request_id) = unpack_prefix(prefix);
        // `slot_stride(0)` is 8, so a zero-size slot would pass the span test.
        if sz == 0 || st.read.virt + slot_stride(sz) > vwc {
            gnitz_fatal_abort!(
                "w2m::take_next: size={} at phys={} overruns the write cursor {} — ring corrupt",
                sz,
                st.read.phys,
                vwc,
            );
        }
        let payload_at = (st.read.phys + RING_PREFIX_BYTES) as usize;
        // SAFETY: the slot ends below `vwc`, and its bytes are the writer's
        // again only once this slot is released.
        let frame = unsafe {
            std::slice::from_raw_parts(
                st.read.base.add(payload_at - gnitz_wire::FRAME_LEN_PREFIX_BYTES),
                sz + gnitz_wire::FRAME_LEN_PREFIX_BYTES,
            )
        };
        st.read.advance(slot_stride(sz));

        let push_idx = st.front_idx + st.queue.len() as u64;
        st.queue.push_back((st.read.virt, false));
        Some(W2mSlot {
            frame,
            push_idx,
            request_id,
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
    /// The master's park: its value counts the publishes that took an arm.
    master: &'static Park,
}

impl W2mReceiver {
    /// Take a slot from the ring without freeing its space. `release_cursor`
    /// advances only when the returned `W2mSlot` is dropped, which is what tells
    /// the writer the bytes are reusable.
    pub fn try_read_slot(&self, worker: usize) -> Option<W2mSlot> {
        self.rings[worker].take_next(worker as u32)
    }

    /// Arm the master's park unless a ring has unread data: the futex word and
    /// the value a wait on it compares. `None` leaves it disarmed, and the caller
    /// drains and retries, never parks. The arm and the cursor loads behind it
    /// are SeqCst, the master's half of the pair [`Park`] describes. The arm
    /// stays set until a publish takes it, which moves the value; nothing else
    /// does.
    pub fn arm(&self) -> Option<(*const AtomicU32, u32)> {
        self.master.arm_for(|| !self.rings.iter().any(WorkerRing::has_unread))
    }

    pub fn num_workers(&self) -> usize {
        self.rings.len()
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
