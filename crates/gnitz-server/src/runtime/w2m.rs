//! W2M: the worker→master SPSC transport — per worker one `MAP_SHARED`
//! tail-chasing reply ring, the worker's [`W2mWriter`], the master's
//! [`W2mReceiver`] and the anonymous shared mappings they live in.
//! [`create`] owns those mappings: it maps the rings and builds their ends, and
//! no end has a constructor of its own, so a ring has one writer and one reader.
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
//! ## The applied position
//!
//! A worker acknowledges what it has applied of the log with one word of its
//! ring's header, apart from the replies: the position it has applied the log
//! through, which only rises. It takes no ring room, so it never waits on a
//! reader's pace, and the master reads it on every drain.
//!
//! ## Giving a ring back
//!
//! The master holds a slot until it is done with it, and a ring frees only a
//! released front prefix, so a slot held at a client's pace holds up every
//! slot behind it. A worker refused room by bytes another lease owns, while a
//! reply of its own waits, names a shortage in its header and wakes the
//! master. The master then gives the ring back: every slot of that ring a
//! lease holds queued is copied out of the ring, and every holder awaiting
//! with a slot is woken to copy its own ([`W2mSlot::poll_give_back`]). Each
//! lease whose queued frames were copied is **held** on that worker: its
//! worker sends it nothing more until its reader has taken the copies and asks
//! for the next frame. So what the master copies is bounded by what the ring
//! held, and while no worker is refused room by another lease, nothing is copied.
//!
//! ## The park rule
//!
//! A worker waits only on its own park, the one a SAL group or an exchange
//! part wakes: short of room, or with every reply it owes held, it arms
//! `release_cursor`'s [`Park`], and the master's release, or its end of a
//! hold, that takes that arm wakes the worker's park. So a waiting worker
//! still sees the SAL. The master parks on one word of its own over all the
//! rings, whose arm a publish, a moved applied position or a shortage on any
//! ring takes.

use std::cell::{Cell, UnsafeCell};
use std::collections::VecDeque;
use std::num::NonZeroU32;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::task::{Context, Poll, Waker};

use super::park::{Park, WorkerParks};
use crate::runtime::frame::WireMsg;
use gnitz_foundation::posix_io;
use gnitz_wire::{peek_control_block, DecodedControl};
use gnitz_zset::repr::PooledBuf;

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

/// The bit of a slot prefix's low half above every request id: the reply was
/// sent ahead of a reply the worker still owes.
const PASSED: u32 = 1 << 31;

/// The 8-byte prefix stamped in front of every slot: `sz` in the high half,
/// `request_id` and [`PASSED`] in the low, so that on LE the 4 bytes in front
/// of the payload are the client's frame length prefix.
#[inline]
fn pack_prefix(sz: usize, request_id: RequestId, passed: bool) -> u64 {
    (request_id.get() | if passed { PASSED } else { 0 }) as u64 | ((sz as u64) << 32)
}

/// The `(sz, request_id, passed)` [`pack_prefix`] stored; `None` for an id of
/// 0, which no publish writes.
#[inline]
fn unpack_prefix(prefix: u64) -> (usize, Option<RequestId>, bool) {
    let low = prefix as u32;
    (
        (prefix >> 32) as usize,
        RequestId::from_wire(low & !PASSED),
        low & PASSED != 0,
    )
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
// Request ids
// ---------------------------------------------------------------------------

/// The id a reply is routed by, below [`PASSED`]. A group with none is answered
/// by nobody, which the wire spells as 0.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub(crate) struct RequestId(NonZeroU32);

impl RequestId {
    pub(crate) const fn get(self) -> u32 {
        self.0.get()
    }

    /// The id the wire value `raw` spells, `None` for 0.
    pub(crate) const fn from_wire(raw: u32) -> Option<RequestId> {
        match NonZeroU32::new(raw) {
            Some(n) => Some(RequestId(n)),
            None => None,
        }
    }

    /// The wire value of `id`: 0 for `None`.
    pub(crate) const fn to_wire(id: Option<RequestId>) -> u32 {
        match id {
            Some(id) => id.get(),
            None => 0,
        }
    }

    /// The smallest id.
    pub(crate) const MIN: RequestId = RequestId(NonZeroU32::MIN);

    /// The largest id: the one below [`PASSED`].
    pub(crate) const MAX: RequestId = RequestId(NonZeroU32::new(PASSED - 1).unwrap());

    /// The id after this one, wrapping past [`Self::MAX`] to 1.
    pub(crate) const fn next(self) -> RequestId {
        RequestId(NonZeroU32::new(self.get() % Self::MAX.get() + 1).unwrap())
    }
}

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
    /// One past the `release_cursor` at which the worker last named a shortage
    /// of room, zero before the first; the master answers each value once.
    want_room: AtomicU64,
    /// How far the worker has applied the log, 0 before its boot recovery.
    applied: AtomicU64,
    /// `release_cursor`: the master publishes it, the worker reads it; its arm asks
    /// for a wake of the worker's park.
    release: Park,
}

const _: () = assert!(std::mem::size_of::<W2mRingHeader>() == W2M_HEADER_SIZE);
const _: () = assert!(std::mem::offset_of!(W2mRingHeader, release) == 64);

// ---------------------------------------------------------------------------
// HoldTable
// ---------------------------------------------------------------------------

/// Leases one worker may have held at once. A lease past it is copied out
/// unheld.
const HOLD_SLOTS: usize = 1024;

/// The leases a worker sends nothing more for (see the module header): the
/// first `held` of `ids`, which only the master writes. A zeroed mapping holds
/// none.
#[repr(C)]
struct HoldTable {
    held: AtomicU32,
    ids: [AtomicU32; HOLD_SLOTS],
}

impl HoldTable {
    /// A fresh shared mapping holding no lease, never unmapped.
    fn map() -> std::io::Result<&'static HoldTable> {
        let base = posix_io::map_shared(None, size_of::<HoldTable>())?;
        // SAFETY: a fresh page-aligned shared mapping of one table, never
        // unmapped; zeroed holds nothing.
        Ok(unsafe { &*base.cast::<HoldTable>() })
    }

    /// Whether `id` is held. A read racing the master's write may miss a hold
    /// or see one just ended; a parker's re-test behind its arm sees neither.
    #[inline]
    fn holds(&self, id: RequestId) -> bool {
        let n = self.held.load(Ordering::SeqCst) as usize;
        self.ids[..n.min(HOLD_SLOTS)]
            .iter()
            .any(|x| x.load(Ordering::Relaxed) == id.get())
    }
}

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

    /// Where the ring's origin puts the virtual offset `virt`.
    fn phys_of(&self, virt: u64) -> u64 {
        let origin = self.header().origin.load(Ordering::Acquire);
        W2M_HEADER_SIZE as u64 + (virt - origin) % self.dcap()
    }

    /// The physical position is where the ring's origin puts the virtual offset.
    fn in_step(&self) -> bool {
        self.phys == self.phys_of(self.virt)
    }
}

// ---------------------------------------------------------------------------
// create / publish
// ---------------------------------------------------------------------------

/// A ring per worker of `parks` and the master's park, each a fresh shared
/// mapping never unmapped: every worker's write end in rank order, and the
/// master's read end over all of them. A release wakes its worker's park.
pub(crate) fn create(parks: WorkerParks) -> std::io::Result<(Vec<W2mWriter>, W2mReceiver)> {
    // `GNITZ_W2M_RING_BYTES` shrinks every ring, so tests fill one on small
    // data; `GNITZ_REPLY_FRAME_BUDGET` lowers the frame budget, so they reach
    // multi-frame trains on small tables.
    rings(
        parks,
        gnitz_foundation::env::env_num("GNITZ_W2M_RING_BYTES", W2M_REGION_SIZE),
        gnitz_foundation::env::env_num("GNITZ_REPLY_FRAME_BUDGET", gnitz_wire::MAX_FRAME_PAYLOAD),
    )
}

/// [`create`] over rings of `capacity` bytes: its body, and the test rings'.
/// A zeroed mapping is an empty ring with both cursors and its origin at 0, and
/// an unarmed park, so nothing is stored here.
fn rings(parks: WorkerParks, capacity: usize, frame_budget: usize) -> std::io::Result<(Vec<W2mWriter>, W2mReceiver)> {
    let nw = parks.len();
    assert!(
        capacity as u64 >= W2M_HEADER_SIZE as u64 + slot_stride(1),
        "W2M capacity={capacity} leaves no room for a message past the {W2M_HEADER_SIZE}-byte header",
    );
    assert!(
        (capacity as u64).is_multiple_of(RING_PREFIX_BYTES),
        "W2M capacity={capacity} must be {RING_PREFIX_BYTES}-byte aligned, or the SKIP path's \
         prefix-word writes at the physical end cross the mapping",
    );
    let master: &'static Park = &Park::map(1)?[0];
    let mut writers = Vec::with_capacity(nw);
    let mut readers = Vec::with_capacity(nw);
    for w in 0..nw {
        let park = parks.park(w);
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
        let holds = HoldTable::map()?;
        writers.push(W2mWriter {
            wc: start,
            master,
            park,
            holds,
            frame_budget,
        });
        readers.push(WorkerRing {
            header: start.header(),
            writer_park: park,
            holds,
            applied: Cell::new(0),
            answered: Cell::new(0),
            slots: UnsafeCell::new(InFlightState {
                read: start,
                front_idx: 0,
                queue: VecDeque::new(),
            }),
        });
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
    /// This worker's park, which [`Self::park_for_master`] sleeps on.
    park: &'static Park,
    /// The leases this worker sends nothing more for.
    holds: &'static HoldTable,
    /// The wire budget of a frame of a reply train, as asked for: see
    /// [`Self::frame_budget`].
    frame_budget: usize,
}

// SAFETY: the raw `base` inside the cursor and the master's park are
// `MAP_SHARED` regions valid in every process for their whole life, and moving
// the writer moves the ring's one producer.
unsafe impl Send for W2mWriter {}

impl W2mWriter {
    /// Where a `total`-byte slot goes now, `total <= DCAP`; while the publish
    /// would lap the reader, the `release_cursor` that refusal was weighed against.
    #[inline]
    fn placement(&self, total: u64) -> Result<Placement, u64> {
        let wc = &self.wc;
        let vrel = wc.header().release.value();
        debug_assert!(vrel <= wc.virt, "cursor inversion: vrel={vrel} vwc={}", wc.virt);
        let used = wc.virt - vrel;
        debug_assert!(used <= wc.dcap(), "used {used} exceeds DCAP {}", wc.dcap());
        if used == 0 {
            return Ok(Placement::Rewind);
        }
        let room_to_end = wc.room_to_end();
        let fits = if total <= room_to_end {
            (used + total <= wc.dcap()).then_some(Placement::Here)
        } else {
            debug_assert!(
                room_to_end >= RING_PREFIX_BYTES,
                "no room for a SKIP marker at phys={}",
                wc.phys
            );
            (used + room_to_end + total <= wc.dcap()).then_some(Placement::Skip)
        };
        fits.ok_or(vrel)
    }

    /// Publish one `sz`-byte message tagged `request_id` and `passed`, written by
    /// `encode`. `Err`, writing nothing, when the ring has no room for it now: the
    /// `release_cursor` that refusal was weighed against. A writer waits for that
    /// value to move, so a release landing after the load reads as room, and it
    /// never waits on a ring its reader has emptied.
    fn try_publish(
        &mut self,
        request_id: RequestId,
        passed: bool,
        sz: usize,
        encode: impl FnOnce(&mut [u8]),
    ) -> Result<(), u64> {
        let total = slot_stride(sz);
        assert!(sz > 0, "w2m: an empty message is a slot `take_next` aborts on");
        assert!(
            total <= self.wc.dcap(),
            "w2m: sz={sz} exceeds this ring's {}-byte capacity — it would never fit",
            self.wc.dcap(),
        );
        // The slot prefix keeps `sz` in its high 32 bits.
        assert!(sz <= u32::MAX as usize, "w2m: sz={sz} does not fit a slot prefix");
        let placement = self.placement(total)?;
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
            self.wc.store_prefix(self.wc.phys, pack_prefix(sz, request_id, passed));
            let slot = self.wc.base.add((self.wc.phys + RING_PREFIX_BYTES) as usize);
            encode(std::slice::from_raw_parts_mut(slot, sz));
        }
        self.wc.advance(total);
        // SeqCst for the master's park; it also carries the Release that makes
        // the slot's bytes visible to the reader.
        self.wc.header().write.store(self.wc.virt, Ordering::SeqCst);
        self.master.take_arm();
        Ok(())
    }

    /// Encode `msg` into one ring slot, unless the ring has no room for the
    /// frame now. `request_id` goes in the slot's prefix, which is what the
    /// master routes the reply by, and so does `passed`: a one-frame reply sent
    /// ahead of a reply this worker still owes, which the master may then hold
    /// apart from the ring. `Err` is [`Self::try_publish`]'s.
    pub fn try_send_msg(&mut self, request_id: RequestId, passed: bool, msg: &WireMsg<'_>) -> Result<(), u64> {
        self.try_publish(request_id, passed, msg.size(), |slot| msg.encode(slot))
    }

    /// Publish how far this worker has applied the log. Only rises; never waits.
    pub fn applied(&mut self, at: u64) {
        let header = self.wc.header();
        debug_assert!(at >= header.applied.load(Ordering::Relaxed));
        // SeqCst for the master's park.
        header.applied.store(at, Ordering::SeqCst);
        self.master.take_arm();
    }

    /// The largest message this ring holds at all.
    pub fn max_message_bytes(&self) -> usize {
        (self.wc.dcap() - RING_PREFIX_BYTES) as usize
    }

    /// The wire budget of a frame of a reply train: never past what a client
    /// reads in one frame, nor past what this ring holds.
    pub fn frame_budget(&self) -> usize {
        self.frame_budget
            .min(gnitz_wire::MAX_FRAME_PAYLOAD)
            .min(self.max_message_bytes())
    }

    /// Everything below it the master has released.
    pub fn release_cursor(&self) -> u64 {
        self.wc.header().release.value()
    }

    /// The lease whose reader frees this ring's front: the one the oldest
    /// publish the master has not released answers on. `None` on a ring
    /// released whole.
    #[cold]
    pub fn oldest_unreleased(&self) -> Option<RequestId> {
        let wc = &self.wc;
        let release = self.release_cursor();
        if release == wc.virt {
            return None;
        }
        let at = wc.phys_of(release);
        // SAFETY: `release` is below the write cursor and no publish laps it,
        // so the prefix of a slot this writer published starts at `at`, or
        // the SKIP marker in front of one.
        let mut prefix = unsafe { wc.load_prefix(at) };
        if prefix == SKIP_MARKER {
            // SAFETY: the slot a marker stands for starts at the data head.
            prefix = unsafe { wc.load_prefix(W2M_HEADER_SIZE as u64) };
        }
        unpack_prefix(prefix).1
    }

    /// The master holds the lease `id` answers on: send it nothing more.
    #[inline]
    pub fn held(&self, id: RequestId) -> bool {
        self.holds.holds(id)
    }

    /// Ask the master to give the ring back (see the module header): once per
    /// `release_cursor`, so a refusal the master has answered names nothing
    /// more until a release moves it. The load wake of [`Park`]: the master
    /// arms, then re-tests `want_room`.
    #[cold]
    pub fn name_shortage(&mut self) {
        let want = self.release_cursor() + 1;
        let header = self.wc.header();
        if header.want_room.load(Ordering::Relaxed) == want {
            return;
        }
        header.want_room.store(want, Ordering::SeqCst);
        self.master.take_arm();
    }

    /// Wait on this worker's park, the one its SAL groups and exchange parts
    /// wake, while `still_waiting`: for the master's next release or end of a
    /// hold. The release arm goes ahead of the park's own arm and re-test: a
    /// release or end of a hold behind it wakes the park, and one ahead of it
    /// is what the re-test sees.
    pub fn park_for_master(&self, still_waiting: impl FnOnce() -> bool) {
        self.wc.header().release.arm();
        self.park.park(still_waiting);
    }
}

// ---------------------------------------------------------------------------
// InFlightState — the master's read cursor and its outstanding slots
// ---------------------------------------------------------------------------

/// Where one in-flight slot stands.
enum SlotState {
    /// Held: by a lease's queue, or by whoever took it off one.
    Held,
    /// Held by a task awaiting something else, which this wakes to copy it out.
    Waiting(Waker),
    /// The ring was given back: its holder copies it out at its next turn.
    GivenBack,
    Released,
}

/// The master's per-ring state a slot releases through: its read cursor, which
/// never leaves this process, and the slots it has handed out but not yet released.
struct InFlightState {
    /// Master-local. `release_cursor <= read <= write_cursor`; the worker gates
    /// on `release_cursor` alone, so nothing outside this process reads it.
    read: RingCursor,
    /// Absolute index of the slot at the front of the queue (oldest in-flight).
    /// A slot's `push_idx` minus `front_idx` is its position in `queue`.
    front_idx: u64,
    /// One entry per in-flight slot, front = oldest: the offset the slot ends
    /// at, and where it stands.
    queue: VecDeque<(u64, SlotState)>,
}

impl InFlightState {
    /// Where the in-flight slot `push_idx` stands.
    fn state(&mut self, push_idx: u64) -> &mut SlotState {
        &mut self.queue[(push_idx - self.front_idx) as usize].1
    }
}

// ---------------------------------------------------------------------------
// W2mSlot — RAII guard holding a ring slot until the caller is done with it
// ---------------------------------------------------------------------------

/// One message read off a W2M ring: a zero-copy view of its slot, or its bytes
/// copied out by [`Self::detach`].
///
/// Dropping a view advances `release_cursor` (possibly past multiple slots when
/// out-of-order slots complete a contiguous prefix) and wakes a parked writer;
/// dropping a copy returns its buffer to the pool.
pub struct W2mSlot {
    /// Length prefix and payload: `frame_bytes` hands them to a client send
    /// unchanged, and [`Self::bytes`] is their tail. In the ring while `ring` is
    /// set, else the copy this slot owns.
    frame: *const [u8],
    /// In the ring, the slot's in-flight index; else the copy's capacity. Either
    /// is below [`PASSED_BIT`], which says the worker sent it ahead of a reply it
    /// still owed.
    push_idx: u64,
    /// The ring the bytes are in, whose in-flight queue a drop releases through.
    ring: Option<&'static WorkerRing>,
    /// The id the worker's publish tagged the slot with: what the master routes
    /// it by without decoding the frame.
    pub(crate) request_id: RequestId,
    /// The worker whose ring this slot was read from.
    pub(crate) worker: u32,
}

/// The bit of [`W2mSlot::push_idx`] that holds [`W2mSlot::passed`].
const PASSED_BIT: u64 = 1 << 63;

impl W2mSlot {
    /// The wire message alone, without the length prefix in front of it.
    pub fn bytes(&self) -> &[u8] {
        &self.frame_bytes()[gnitz_wire::FRAME_LEN_PREFIX_BYTES..]
    }
    /// The worker sent it ahead of a reply it still owed.
    pub(crate) fn passed(&self) -> bool {
        self.push_idx & PASSED_BIT != 0
    }

    /// The slot's in-flight index, or its copy's capacity.
    fn index(&self) -> u64 {
        self.push_idx & !PASSED_BIT
    }

    /// The framed bytes ready for a client send: `[sz_as_u32_le | payload]`.
    pub(crate) fn frame_bytes(&self) -> &[u8] {
        // SAFETY: ring bytes stay the reader's until this slot's drop releases
        // them, and a copy is this slot's until its drop frees it.
        unsafe { &*self.frame }
    }

    /// Copy the message into pooled memory and release its ring space; a no-op
    /// on a copy.
    #[cold]
    #[inline(never)]
    pub(crate) fn detach(&mut self) {
        let Some(ring) = self.ring.take() else { return };
        let mut copy = std::mem::ManuallyDrop::new(PooledBuf::with_capacity(self.frame.len()));
        copy.extend_from_slice(self.frame_bytes());
        ring.release(self.index());
        self.frame = std::ptr::slice_from_raw_parts(copy.as_ptr(), copy.len());
        self.push_idx = copy.capacity() as u64 | self.push_idx & PASSED_BIT;
    }

    /// The bytes still pin their ring space.
    pub(crate) fn in_ring(&self) -> bool {
        self.ring.is_some()
    }

    /// Ready once the ring was given back while this slot was held: its holder,
    /// a task awaiting something else with it, then copies it out
    /// ([`Self::detach`]). Until then each poll leaves the caller's
    /// waker for the give-back to wake. Never ready on a copy.
    pub(crate) fn poll_give_back(&self, cx: &mut Context<'_>) -> Poll<()> {
        let Some(ring) = self.ring else { return Poll::Pending };
        // SAFETY: a `WorkerRing` is `!Sync`, and this borrow ends with the call.
        let state = unsafe { (*ring.slots.get()).state(self.index()) };
        match state {
            SlotState::GivenBack => Poll::Ready(()),
            _ => {
                *state = SlotState::Waiting(cx.waker().clone());
                Poll::Pending
            }
        }
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
        match self.ring {
            Some(ring) => ring.release(self.index()),
            None => drop_copy(self.frame, self.index()),
        }
    }
}

/// Return [`W2mSlot::detach`]'s copy at `frame`, of capacity `cap`, to the pool.
#[cold]
#[inline(never)]
fn drop_copy(frame: *const [u8], cap: u64) {
    // SAFETY: `detach` took the copy's parts from the one buffer it leaked.
    let buf = unsafe { Vec::from_raw_parts(frame as *mut u8, frame.len(), cap as usize) };
    drop(PooledBuf(buf));
}

// ---------------------------------------------------------------------------
// W2mReceiver
// ---------------------------------------------------------------------------

/// One worker's W2M ring on the master side. `!Sync`: one thread reads a ring.
struct WorkerRing {
    /// The ring's shared header: the one `slots.read` is a cursor over.
    header: &'static W2mRingHeader,
    /// The writer's park, which a release taking the writer's arm wakes.
    writer_park: &'static Park,
    /// The leases the worker sends nothing more for, which only the master writes.
    holds: &'static HoldTable,
    /// The header's `applied` the master last read.
    applied: Cell<u64>,
    /// The header's `want_room` the master last answered.
    answered: Cell<u64>,
    /// The read cursor and the in-flight slots, behind the cell its slots
    /// release through. No borrow of it outlives the call that takes it.
    slots: UnsafeCell<InFlightState>,
}

impl WorkerRing {
    /// The worker has published past the master's read cursor, moved its
    /// applied position, or named a shortage the master has not answered.
    #[inline]
    fn has_unread(&self) -> bool {
        // SAFETY: a `WorkerRing` is `!Sync`, and this borrow ends with the statement.
        let read = unsafe { (*self.slots.get()).read.virt };
        self.header.write.load(Ordering::SeqCst) != read
            || self.applied_moved()
            || self.header.want_room.load(Ordering::SeqCst) != self.answered.get()
    }

    /// The worker has moved its applied position since [`Self::applied`] last
    /// read it. SeqCst for the master's park.
    #[inline]
    fn applied_moved(&self) -> bool {
        self.header.applied.load(Ordering::SeqCst) != self.applied.get()
    }

    /// The worker has named a shortage since the last call: answered now, by
    /// waking every task awaiting with a slot of this ring.
    #[inline]
    fn answer_shortage(&self) -> bool {
        let want = self.header.want_room.load(Ordering::SeqCst);
        if want == self.answered.get() {
            return false;
        }
        self.answered.set(want);
        self.wake_holders();
        true
    }

    /// Wake every task awaiting with a slot of this ring, to copy it out.
    #[cold]
    fn wake_holders(&self) {
        // SAFETY: a `WorkerRing` is `!Sync`, and this borrow ends with the call;
        // a wake only queues its task.
        let st = unsafe { &mut *self.slots.get() };
        for (_, state) in st.queue.iter_mut() {
            if let SlotState::Waiting(w) = state {
                w.wake_by_ref();
                *state = SlotState::GivenBack;
            }
        }
    }

    /// Hold `id`: its worker sends it nothing more. False, holding nothing,
    /// when [`HOLD_SLOTS`] leases are held.
    #[cold]
    fn hold(&self, id: RequestId) -> bool {
        let holds = self.holds;
        let n = holds.held.load(Ordering::Relaxed) as usize;
        if n == HOLD_SLOTS {
            return false;
        }
        holds.ids[n].store(id.get(), Ordering::Relaxed);
        holds.held.store(n as u32 + 1, Ordering::SeqCst);
        true
    }

    /// End `id`'s hold, waking the worker if it waits on the master. The
    /// release is published again at its value: the RMW wake of [`Park`].
    #[cold]
    fn unhold(&self, id: RequestId) {
        let holds = self.holds;
        let n = holds.held.load(Ordering::Relaxed) as usize;
        let Some(at) = holds.ids[..n]
            .iter()
            .position(|x| x.load(Ordering::Relaxed) == id.get())
        else {
            debug_assert!(false, "an end of a hold that was never taken");
            return;
        };
        holds.ids[at].store(holds.ids[n - 1].load(Ordering::Relaxed), Ordering::Relaxed);
        holds.held.store(n as u32 - 1, Ordering::SeqCst);
        let release = &self.header.release;
        release.publish_waking(release.value(), self.writer_park);
    }

    /// How far the worker has applied the log, read off its header. It only
    /// rises: a position below the one last read aborts.
    fn applied(&self) -> u64 {
        let at = self.header.applied.load(Ordering::Acquire);
        let last = self.applied.get();
        if at < last {
            applied_moved_back(at, last);
        }
        self.applied.set(at);
        at
    }

    /// Mark the slot identified by `push_idx` as released. Advances
    /// `release_cursor` through the front-consecutive released prefix and, if it
    /// advanced and the writer asked, wakes the writer's park. `push_idx` is the
    /// caller's own in-flight slot, released once: `W2mSlot`'s drop and `detach`.
    fn release(&self, push_idx: u64) {
        // SAFETY: a `WorkerRing` is `!Sync`, and this borrow ends with the call.
        let st = unsafe { &mut *self.slots.get() };
        *st.state(push_idx) = SlotState::Released;

        let mut last_vrc = None;
        while let Some(&(vrc, SlotState::Released)) = st.queue.front() {
            st.queue.pop_front();
            st.front_idx += 1;
            last_vrc = Some(vrc);
        }

        if let Some(vrc) = last_vrc {
            self.header.release.publish_waking(vrc, self.writer_park);
        }
    }

    /// Read the next message and register it as in-flight. `None` **iff** the
    /// ring is empty; every departure from the layout aborts.
    #[inline]
    fn take_next(&'static self, worker: u32) -> Option<W2mSlot> {
        // SAFETY: a `WorkerRing` is `!Sync`, and this borrow ends with the call.
        let st = unsafe { &mut *self.slots.get() };
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
        let (sz, request_id, passed) = unpack_prefix(prefix);
        let Some(request_id) = request_id else {
            gnitz_fatal_abort!("w2m::take_next: request id 0 at phys={} — ring corrupt", st.read.phys)
        };
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
        st.queue.push_back((st.read.virt, SlotState::Held));
        Some(W2mSlot {
            frame,
            push_idx: push_idx | if passed { PASSED_BIT } else { 0 },
            ring: Some(self),
            request_id,
            worker,
        })
    }
}

/// Abort on a worker's applied position `at`, below the `last` one read.
/// Outlined so `at` is not spilled onto the stack on the path that never aborts.
#[cold]
#[inline(never)]
fn applied_moved_back(at: u64, last: u64) -> ! {
    gnitz_fatal_abort!("w2m: applied position {at:#x} moved back from {last:#x} — ring header corrupt");
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

    /// [`Self::lowest_applied`], once any worker has moved its applied
    /// position since it was last read. Every apply addresses every launched
    /// worker, so the log is applied as far as the slowest of them has applied it.
    #[inline]
    pub(crate) fn lowest_applied_if_moved(&self) -> Option<u64> {
        self.rings
            .iter()
            .any(WorkerRing::applied_moved)
            .then(|| self.lowest_applied())
    }

    /// The lowest position any worker has applied the log through, 0 while one
    /// is short of its boot recovery.
    #[cold]
    pub(crate) fn lowest_applied(&self) -> u64 {
        let lowest = self.rings.iter().map(WorkerRing::applied).min();
        lowest.expect("a receiver over no worker has no applied position")
    }

    /// Answer a shortage of room `worker` has named since the last call, by
    /// waking every task awaiting with a slot of its ring to copy it out;
    /// whether there was one.
    pub(crate) fn answer_shortage(&self, worker: usize) -> bool {
        self.rings[worker].answer_shortage()
    }

    /// Hold the lease `id` on `worker`: it sends that lease nothing more until
    /// [`Self::unhold`]. False when `worker` holds as many leases as it can.
    pub(crate) fn hold(&self, worker: usize, id: RequestId) -> bool {
        self.rings[worker].hold(id)
    }

    /// End the hold [`Self::hold`] took.
    pub(crate) fn unhold(&self, worker: usize, id: RequestId) {
        self.rings[worker].unhold(id)
    }

    /// Arm the master's park unless a ring has unread data, a moved applied
    /// position or a shortage: the futex word and the value a wait on it
    /// compares. `None` leaves it disarmed, and the caller drains and retries,
    /// never parks. The arm and the cursor loads behind it are SeqCst, the
    /// master's half of the pair [`Park`] describes. The arm stays set until a
    /// publish takes it, which moves the value; nothing else does.
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
