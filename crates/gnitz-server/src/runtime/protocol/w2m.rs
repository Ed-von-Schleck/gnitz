//! W2M: the worker→master SPSC transport — one `MAP_SHARED` tail-chasing ring
//! per worker, the worker's [`W2mWriter`], the master's [`W2mReceiver`], the
//! futex park/wake protocol between them, and the anonymous shared mapping
//! ([`create_region`]) they all live in.
//!
//! ## Layout
//!
//! A 128-byte [`W2mRingHeader`] followed by `DCAP = capacity - W2M_HEADER_SIZE`
//! bytes of data. `write_cursor` (worker-written) and `release_cursor`
//! (master-written) are monotonic **virtual** offsets, never physical positions;
//! the master's read cursor sits between them — `release <= read <= write` — and
//! never leaves this process. `phys(v) = HEADER + (v - HEADER) % DCAP`, memoized
//! in a [`RingCursor`] so the modulo stays off both hot paths.
//!
//! When a message would not fit before `capacity`, the writer stamps
//! `SKIP_MARKER` at the current physical position and publishes at the data
//! head instead; the reader jumps the same way, so markers are transparent.
//!
//! ## The park rule
//!
//! Each side parks on the cursor whose advance is the condition it waits for:
//! the worker on `release_cursor` (space freed), the master on `write_cursor`
//! (a message published). [`ParkWord`] pairs each cursor with its own park flags
//! and is the only way to reach either, so the read-modify-writes the protocol
//! needs cannot be written any other way.
//!
//! Who *clears* a flag differs by direction, and that asymmetry is load-bearing.
//! [`wake_master`] takes the gate, so the publishes that follow until the master
//! re-arms skip the syscall: a master parks only when the ring reads empty, so
//! any publish after its snapshot fails the futex value-compare and returns
//! without a wake. [`wake_writer`] must not — the writer waits for *enough
//! room*, which a later retirement may be the first to give, so a bit cleared on
//! its behalf is a wake nobody issues and a permanent hang.

use std::cell::{Cell, UnsafeCell};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use io_uring::types::FutexWaitV;

use crate::runtime::wire::{decode_wire_ipc, DecodedWire, WireMsg};
use gnitz_store::foundation::posix_io;
use gnitz_wire::align8;

// ---------------------------------------------------------------------------
// Geometry
// ---------------------------------------------------------------------------

/// Fixed header size at the start of every W2M mmap region.
const W2M_HEADER_SIZE: usize = 128;

/// Upper bound on a single W2M wire message (256 MiB). Large enough for any
/// real query response, small enough that `u64::MAX` is an unambiguous
/// SKIP-marker sentinel in the size prefix.
pub(crate) const MAX_W2M_MSG: u64 = 1 << 28;

/// Capacity (header + data) of each per-worker W2M mmap region.
///
/// The region is mapped whole, so a ring that sweeps all of `DCAP`
/// every lap holds ~1 GiB of resident shmem per worker for a transport whose
/// live occupancy is a few KiB. The lever is `MAX_W2M_MSG`, which is this large
/// because `worker/exchange.rs` publishes an exchange partition uncapped.
pub(crate) const W2M_REGION_SIZE: usize = 1 << 30;
// One maximum-size message must always fit past its own SKIP pad: the wrap
// branch fires only when `total > room_to_end`, so `pad < total <= MAX + 8`.
const _: () = assert!(
    W2M_REGION_SIZE as u64 >= 2 * MAX_W2M_MSG + W2M_HEADER_SIZE as u64 + 16,
    "the region must hold a maximum-size message plus its worst-case SKIP pad",
);
// A parked side value-compares a cursor's low 32 bits (see `futex_word`). One
// advance is bounded by `DCAP`, so it always changes those bits and can never
// alias a stale snapshot by `k * 2^32`.
const _: () = assert!(W2M_REGION_SIZE < u32::MAX as usize);

// `arm_park` builds a `futex_waitv` word list of at most `num_workers` entries.
// The kernel's `FUTEX_WAITV_MAX` is 128; past it the syscall returns EINVAL,
// which the caller absorbs as a timeout, silently degenerating the relay to
// polling. Make a `MAX_WORKERS` bump that outgrows it a build error.
const _: () = assert!(gnitz_wire::MAX_WORKERS <= 128);

/// Bytes of the 8-byte slot prefix that carry the client's frame length prefix.
const SLOT_LEN_PREFIX_BYTES: usize = gnitz_wire::FRAME_LEN_PREFIX_BYTES;
const _: () = assert!(
    SLOT_LEN_PREFIX_BYTES == 4,
    "the packed u64 slot prefix is two u32 halves"
);
const _: () = assert!(
    MAX_W2M_MSG <= u32::MAX as u64,
    "sz must survive the u32 half it is packed into"
);
const _: () = assert!(
    cfg!(target_endian = "little"),
    "the client's frame prefix is the u64's high half only on LE"
);

/// Size-prefix sentinel for "skip to the header, the real message is there".
const SKIP_MARKER: u64 = u64::MAX;

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

/// Set by the worker while parked on `release_cursor`; cleared by the worker
/// once its wait returns. The master reads it before spending a `FUTEX_WAKE`.
const FLAG_WRITER_PARKED: u32 = 1 << 0;
/// Set by the reactor while its `FUTEX_WAITV` SQE is armed on `write_cursor`.
const FLAG_MASTER_WAITV: u32 = 1 << 1;
/// Set by `W2mReceiver::wait_any` while it is synchronously parked. A bit of its
/// own because both master parks can be armed at once from the same thread, so
/// one bit would let whichever unparked first disarm the other's still-live gate.
const FLAG_MASTER_SYNC: u32 = 1 << 2;
/// Either master park — the worker's publish gate, taken by [`wake_master`].
const FLAG_MASTER_ANY: u32 = FLAG_MASTER_WAITV | FLAG_MASTER_SYNC;

// ---------------------------------------------------------------------------
// Header layout (128 bytes, one cache line per writer)
// ---------------------------------------------------------------------------

/// One side's park: the cursor it sleeps on and the flags saying it is asleep.
///
/// Pairing them is what makes the protocol's ordering unwritable-wrong: nothing
/// touches the flags except the three methods below, each of which does its
/// locked read-modify-write *first*. [`Self::publish`] cannot hand back flags it
/// did not read behind a `swap`, and [`Self::arm`] cannot hand back a cursor it
/// did not read behind a `fetch_or`.
///
/// Both RMWs are load-bearing. Drop either and the store-buffer race is real:
/// the peer reads a stale-clear flag and skips the wake while the parker reads a
/// stale cursor and parks. A fence on one side does not help — it drains that
/// side's store buffer, not the other's — and neither does the futex
/// value-compare, which is a plain kernel read that a still-buffered store makes
/// match spuriously.
#[repr(C)]
struct ParkWord {
    cursor: AtomicU64,
    flags: AtomicU32,
    _pad: u32,
}

impl ParkWord {
    /// Publish `virt` to the parked peer and report the flags behind that swap.
    /// A flag read here as clear means this store is already globally visible,
    /// so the peer cannot have parked on the old value.
    #[inline]
    fn publish(&self, virt: u64) -> u32 {
        self.cursor.swap(virt, Ordering::AcqRel);
        self.flags.load(Ordering::Acquire)
    }

    /// Arm `bit` and snapshot the cursor behind that RMW, so a peer that already
    /// published is visible to the caller's re-test.
    #[inline]
    fn arm(&self, bit: u32) -> u64 {
        self.flags.fetch_or(bit, Ordering::AcqRel);
        self.cursor.load(Ordering::Acquire)
    }

    /// Drop `bits`. The result is deliberately unused: binding it would force a
    /// `lock cmpxchg` retry loop where a discarded one lowers to a `lock and`.
    #[inline]
    fn disarm(&self, bits: u32) {
        self.flags.fetch_and(!bits, Ordering::AcqRel);
    }
}

/// Cross-process ring state, one cache line per writing side. Each [`ParkWord`]
/// therefore sits on the line its own **clearer** already dirties, so taking or
/// releasing a gate rides a coherence transaction that side was making anyway.
///
/// ```text
/// line A — worker writes, master reads
///    0   master_park.cursor   write_cursor: the message boundary
///    8   master_park.flags    FLAG_MASTER_*, cleared by a publish
/// line B — master writes, worker reads
///   64   writer_park.cursor   release_cursor: the reusable-bytes boundary
///   72   writer_park.flags    FLAG_WRITER_PARKED, cleared by the worker
///   80   capacity             immutable after init_region
/// ```
#[repr(C, align(64))]
struct W2mRingHeader {
    master_park: ParkWord,
    _pad_producer: [u8; 48],

    writer_park: ParkWord,
    capacity: AtomicU64,
    _pad_consumer: [u8; 40],
}

const _: () = assert!(std::mem::size_of::<W2mRingHeader>() == W2M_HEADER_SIZE);
const _: () = assert!(std::mem::align_of::<W2mRingHeader>() == 64);
// The layout above, asserted rather than described: each park whole inside one
// line, and nothing else sharing the producer's.
const _: () = assert!(std::mem::offset_of!(W2mRingHeader, master_park.cursor) == 0);
const _: () = assert!(std::mem::offset_of!(W2mRingHeader, master_park.flags) == 8);
const _: () = assert!(std::mem::offset_of!(W2mRingHeader, writer_park.cursor) == 64);
const _: () = assert!(std::mem::offset_of!(W2mRingHeader, writer_park.flags) == 72);
const _: () = assert!(std::mem::offset_of!(W2mRingHeader, capacity) == 80);

impl W2mRingHeader {
    /// Reinterpret the start of a W2M mmap region as its header. The mapping is
    /// created before `fork` and unmapped only at process exit, which is what
    /// the `'static` rests on.
    ///
    /// # Safety
    /// `ptr` must be a live W2M mmap pointer already initialized by
    /// [`init_region`].
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
    /// The producer's cursor over the ring at `base`.
    ///
    /// # Safety
    /// `base` must be a live region initialized by [`init_region`].
    unsafe fn producer(base: *mut u8) -> Self {
        Self::seed(
            base,
            W2mRingHeader::from_raw(base).master_park.cursor.load(Ordering::Acquire),
        )
    }

    /// The master's read cursor over the ring at `base`, seeded from
    /// `release_cursor`: a receiver is built with nothing in flight, and
    /// `release == read` exactly then.
    ///
    /// # Safety
    /// `base` must be a live region initialized by [`init_region`].
    unsafe fn consumer(base: *mut u8) -> Self {
        Self::seed(
            base,
            W2mRingHeader::from_raw(base).writer_park.cursor.load(Ordering::Acquire),
        )
    }

    unsafe fn seed(base: *mut u8, virt: u64) -> Self {
        let header = W2M_HEADER_SIZE as u64;
        let cap = W2mRingHeader::from_raw(base).capacity.load(Ordering::Relaxed);
        debug_assert!(cap > header, "capacity {cap} leaves no data region");
        debug_assert!(virt >= header, "virtual cursor {virt} below HEADER");
        RingCursor {
            base,
            virt,
            phys: header + (virt - header) % (cap - header),
            cap,
        }
    }

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
        // SAFETY: a cursor only ever holds a base its constructors validated.
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
// Futex park/wake
// ---------------------------------------------------------------------------

/// The 32-bit futex word aliasing a cursor's low half — see the `2^32` assert
/// beside [`W2M_REGION_SIZE`] for why those bits always move.
#[inline]
fn futex_word(cursor: &AtomicU64) -> *const AtomicU32 {
    cursor as *const AtomicU64 as *const AtomicU32
}

/// `futex2(2)` flags byte for a 32-bit atomic. Matches the kernel constant
/// `FUTEX2_SIZE_U32` (=2). No `FUTEX2_PRIVATE` bit — a W2M region is
/// `MAP_SHARED` across `fork()`.
pub(crate) const FUTEX2_SIZE_U32: u32 = 2;

/// The errno of the most recent failed syscall. The futex wrappers go through
/// `libc::syscall`, which returns `-1` rather than `-errno`.
#[inline]
fn errno() -> i32 {
    unsafe { *libc::__errno_location() }
}

/// How a futex park ended. The wrappers below classify errno here, where it is
/// still fresh, so no caller has to know which errnos the protocol tolerates.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Parked {
    /// Woken, the value had already moved, or a signal cut the wait short — all
    /// the same instruction to the caller: re-read the ring, which is
    /// authoritative.
    Retry,
    /// The timeout elapsed with no wake.
    TimedOut,
    /// A failure the protocol has no answer to. Carries the raw return code.
    Failed(i32),
}

impl Parked {
    #[inline]
    fn from_rc(rc: i32) -> Parked {
        if rc >= 0 {
            return Parked::Retry;
        }
        match errno() {
            libc::EAGAIN | libc::EINTR => Parked::Retry,
            libc::ETIMEDOUT => Parked::TimedOut,
            _ => Parked::Failed(rc),
        }
    }
}

/// Block on the futex at `ptr` until its value differs from `expected` or a wake
/// arrives; `timeout_ms < 0` blocks forever. Not the `FUTEX_PRIVATE_FLAG`
/// variant — a W2M region is `MAP_SHARED` across `fork()`.
fn futex_wait_u32(ptr: *const AtomicU32, expected: u32, timeout_ms: i32) -> Parked {
    let ts = libc::timespec {
        tv_sec: (timeout_ms as i64) / 1000,
        tv_nsec: ((timeout_ms as i64) % 1000) * 1_000_000,
    };
    let ts_ptr: *const libc::timespec = if timeout_ms < 0 { std::ptr::null() } else { &ts };
    Parked::from_rc(unsafe {
        libc::syscall(
            libc::SYS_futex,
            ptr as *const libc::c_void,
            libc::FUTEX_WAIT,
            expected as libc::c_int,
            ts_ptr,
            std::ptr::null::<u32>(),
            0u32,
        ) as i32
    })
}

/// Wake at most `n_waiters` waiters parked on `ptr` (v1 `FUTEX_WAKE`, no
/// `FUTEX_PRIVATE_FLAG` — W2M is shared). Aborts rather than reporting: a lost
/// wake leaves the peer blocked on a queue that has work in it, so there is no
/// caller-side answer to distinguish. `site` names the caller in the abort.
#[inline]
fn futex_wake_u32(ptr: *const AtomicU32, n_waiters: u32, site: &str) {
    let rc = unsafe {
        libc::syscall(
            libc::SYS_futex,
            ptr as *const libc::c_void,
            libc::FUTEX_WAKE,
            n_waiters as libc::c_int,
            std::ptr::null::<libc::timespec>(),
            std::ptr::null::<u32>(),
            0u32,
        ) as i32
    };
    if rc < 0 {
        futex_failed(site, rc);
    }
}

/// Wait on MULTIPLE futex words at once (`SYS_futex_waitv`), returning when ANY
/// differs from its expected value or is woken — the synchronous analogue of the
/// reactor's `IORING_OP_FUTEX_WAITV`, built from the same [`FutexWaitV`] so the
/// kernel struct has one declaration rather than two that can drift.
fn futex_waitv_u32(waiters: &[FutexWaitV], timeout_ms: i32) -> Parked {
    if waiters.is_empty() {
        return Parked::Retry;
    }
    let mut ts = libc::timespec { tv_sec: 0, tv_nsec: 0 };
    let ts_ptr: *const libc::timespec = if timeout_ms < 0 {
        std::ptr::null()
    } else {
        // `futex_waitv` takes an ABSOLUTE `CLOCK_MONOTONIC` deadline, unlike the
        // relative `futex_wait_u32` above; do not harmonize the two.
        unsafe {
            libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut ts);
        }
        ts.tv_sec += (timeout_ms as i64) / 1000;
        ts.tv_nsec += ((timeout_ms as i64) % 1000) * 1_000_000;
        if ts.tv_nsec >= 1_000_000_000 {
            ts.tv_sec += 1;
            ts.tv_nsec -= 1_000_000_000;
        }
        &ts
    };
    Parked::from_rc(unsafe {
        libc::syscall(
            libc::SYS_futex_waitv,
            waiters.as_ptr(),
            waiters.len() as libc::c_uint,
            0u32, // flags
            ts_ptr,
            libc::CLOCK_MONOTONIC,
        ) as i32
    })
}

/// Outlined so `site` is not spilled onto the stack on the paths that never
/// abort.
#[cold]
#[inline(never)]
fn futex_failed(site: &str, rc: i32) -> ! {
    gnitz_fatal_abort!("{}: futex failed: rc={} errno={}", site, rc, errno());
}

/// Master→worker, given the flags a retirement's [`ParkWord::publish`] returned.
///
/// Leaves `FLAG_WRITER_PARKED` set — see the module docs: only the writer may
/// clear it.
#[inline]
fn wake_writer(park: &ParkWord, flags: u32) {
    if flags & FLAG_WRITER_PARKED == 0 {
        return;
    }
    futex_wake_u32(futex_word(&park.cursor), 1, "wake_writer");
}

/// Worker→master, given the flags a publish's [`ParkWord::publish`] returned.
/// Takes the gate, so the publishes that follow until the master re-arms skip
/// the syscall.
#[inline]
fn wake_master(park: &ParkWord, flags: u32) {
    if flags & FLAG_MASTER_ANY == 0 {
        return;
    }
    park.disarm(FLAG_MASTER_ANY);
    // `count_ones()` folds to 2 — the number of master parks, and an upper bound
    // on the waiters, since either may have unparked since the flags were read.
    futex_wake_u32(futex_word(&park.cursor), FLAG_MASTER_ANY.count_ones(), "wake_master");
}

/// The mask naming workers `0..n`. Written here beside [`BitIter`] because the
/// `n == 64` arm is a shift-overflow guard, not a policy: `1u64 << 64` is UB.
#[inline]
pub(crate) fn worker_mask(n: usize) -> u64 {
    if n >= 64 {
        u64::MAX
    } else {
        (1u64 << n) - 1
    }
}

/// Yields the set bit positions of a worker mask, lowest first.
pub(crate) struct BitIter(pub(crate) u64);

impl Iterator for BitIter {
    type Item = usize;
    fn next(&mut self) -> Option<usize> {
        if self.0 == 0 {
            return None;
        }
        let w = self.0.trailing_zeros() as usize;
        self.0 &= self.0 - 1;
        Some(w)
    }
}

// ---------------------------------------------------------------------------
// init / reserve / commit
// ---------------------------------------------------------------------------

/// One worker's ring region, initialized and never unmapped: `MAP_SHARED` so
/// every forked child sees the same pages, `MAP_NORESERVE` so Linux does not
/// commit-charge all `W2M_REGION_SIZE` of it per worker at boot — which fails
/// outright under `vm.overcommit_memory=2`.
pub(crate) fn create_region() -> std::io::Result<*mut u8> {
    let ptr = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            W2M_REGION_SIZE,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_SHARED | libc::MAP_ANONYMOUS | libc::MAP_NORESERVE,
            -1,
            0,
        )
    };
    if ptr == libc::MAP_FAILED {
        return Err(std::io::Error::last_os_error());
    }
    let base = ptr as *mut u8;
    // An anonymous shared mapping is shmem-backed, so this follows
    // `shmem_enabled` — see `madvise_hugepage`.
    posix_io::madvise_hugepage(base, W2M_REGION_SIZE);
    // SAFETY: a fresh mapping of exactly `W2M_REGION_SIZE` bytes, unshared.
    unsafe { init_region(base, W2M_REGION_SIZE as u64) };
    Ok(base)
}

/// Zero the header, seat both cursors at the start of the data area and record
/// `capacity`. The asserts below are structural; the deployment's own sizing
/// floor lives at [`W2M_REGION_SIZE`].
///
/// # Safety
/// `ptr` must be a writable mapping of at least `capacity` bytes, with no other
/// thread or process reading or writing the region.
unsafe fn init_region(ptr: *mut u8, capacity: u64) {
    assert!(
        capacity >= W2M_HEADER_SIZE as u64 + 16,
        "W2M capacity={capacity} leaves no room for a message past the {W2M_HEADER_SIZE}-byte header",
    );
    assert!(
        capacity.is_multiple_of(8),
        "W2M capacity={capacity} must be 8-byte aligned, or the SKIP path's 8-byte \
         writes at the physical end cross the mapping",
    );
    // Zero first so re-init of a previously-live region clears every byte,
    // padding included.
    std::ptr::write_bytes(ptr, 0, W2M_HEADER_SIZE);
    let hdr = W2mRingHeader::from_raw(ptr);
    hdr.capacity.store(capacity, Ordering::Relaxed);
    hdr.master_park.cursor.store(W2M_HEADER_SIZE as u64, Ordering::Release);
    hdr.writer_park.cursor.store(W2M_HEADER_SIZE as u64, Ordering::Release);
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
        debug_assert!(room_to_end >= 8, "no room for a SKIP marker at phys={}", wc.phys);
        (used + room_to_end + total <= wc.dcap()).then_some((W2M_HEADER_SIZE as u64, room_to_end))
    }
}

/// A claimed slot: encode into [`Self::slot`], then hand it to [`commit`], which
/// is what publishes it. Dropping one instead loses the message and corrupts
/// nothing — its bytes sit at or past `write_cursor`, which never advanced.
#[must_use = "a Reservation must be committed or its message is dropped"]
struct Reservation {
    slot_ptr: *mut u8,
    slot_len: usize,
    /// Bytes to advance the write cursor by: `total`, or `pad + total` when
    /// the message SKIP-wrapped.
    delta: u64,
}

impl Reservation {
    /// The bytes to encode the message into.
    #[inline]
    fn slot(&mut self) -> &mut [u8] {
        // SAFETY: `try_reserve` sized this against the ring's free space, and
        // nothing else touches those bytes until `commit` publishes them.
        unsafe { std::slice::from_raw_parts_mut(self.slot_ptr, self.slot_len) }
    }
}

/// Claim a slot for a `sz`-byte message tagged `internal_req_id`, stamping its
/// [`pack_prefix`] header (and a SKIP marker first if it wraps). `None` means
/// the ring is full: the caller parks and calls again — nothing is written on
/// that path.
///
/// # Safety
/// The caller must be the sole producer on `wc`'s ring.
unsafe fn try_reserve(wc: &RingCursor, sz: usize, internal_req_id: u32) -> Option<Reservation> {
    assert!(
        sz > 0 && (sz as u64) <= MAX_W2M_MSG,
        "w2m::try_reserve: sz={sz} outside (0, {MAX_W2M_MSG}]",
    );
    let total = (8 + align8(sz)) as u64;
    let vrel = wc.header().writer_park.cursor.load(Ordering::Acquire);
    let (write_at, pad) = publish_at(wc, vrel, total)?;

    if pad != 0 {
        wc.store_prefix(wc.phys, SKIP_MARKER);
    }
    wc.store_prefix(write_at, pack_prefix(sz, internal_req_id));
    Some(Reservation {
        slot_ptr: wc.base.add(write_at as usize + 8),
        slot_len: sz,
        delta: pad + total,
    })
}

/// Publish a reservation and wake a parked master. [`ParkWord::publish`] carries
/// the Release that makes the slot's bytes visible to the consumer.
///
/// # Safety
/// `r` must come from a [`try_reserve`] against this cursor, with the slot fully
/// written since.
unsafe fn commit(wc: &mut RingCursor, r: Reservation) {
    wc.advance(r.delta);
    let park = &wc.header().master_park;
    wake_master(park, park.publish(wc.virt));
}

// ---------------------------------------------------------------------------
// W2mWriter — the worker's write side
// ---------------------------------------------------------------------------

/// The full-ring path: publish `FLAG_WRITER_PARKED`, re-test below that barrier,
/// then sleep on `release_cursor` until the master retires a slot.
///
/// # Safety
/// The caller must be the sole producer on `wc`'s ring.
#[cold]
#[inline(never)]
unsafe fn park_for_room(wc: &RingCursor, sz: usize, req: u32) -> Reservation {
    let park = &wc.header().writer_park;
    loop {
        let vrel = park.arm(FLAG_WRITER_PARKED);
        let reserved = try_reserve(wc, sz, req);
        if reserved.is_none() {
            if let Parked::Failed(rc) = futex_wait_u32(futex_word(&park.cursor), vrel as u32, -1) {
                futex_failed("W2mWriter::park_for_room", rc);
            }
        }
        park.disarm(FLAG_WRITER_PARKED);
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

// SAFETY: the raw `base` inside the cursor is a `MAP_SHARED` region valid in
// every process for its whole life. `reactor::mod.rs` moves a writer into a
// thread closure; sole-producer is what the type still requires, and moving does
// not duplicate it.
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
        self.send_encoded(msg.size(), ring_req as u32, |buf| {
            msg.encode_ipc(buf, 0);
        });
    }

    /// Encode one message into the ring and publish it, blocking on
    /// `release_cursor` while the ring is full. `internal_req_id` rides the slot
    /// prefix so the master can route the reply without decoding the frame.
    fn send_encoded(&self, sz: usize, internal_req_id: u32, encode_fn: impl FnOnce(&mut [u8])) {
        // SAFETY: a worker process is the sole producer on its own ring.
        unsafe {
            let mut wc = self.cursor.get();
            let mut reservation = match try_reserve(&wc, sz, internal_req_id) {
                Some(r) => r,
                None => park_for_room(&wc, sz, internal_req_id),
            };
            encode_fn(reservation.slot());
            commit(&mut wc, reservation);
            self.cursor.set(wc);
        }
    }
}

// ---------------------------------------------------------------------------
// InFlightState — the master's read cursor and its outstanding slots
// ---------------------------------------------------------------------------

/// Initial capacity of a ring's in-flight queue. Not a bound: the queue grows,
/// and the real backpressure is the ring's byte capacity, which parks the worker
/// in [`park_for_room`] once the ring fills.
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
            let park = &self.read.header().writer_park;
            wake_writer(park, park.publish(vrc));
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
    /// The ring's `InFlightState`, which lives inside a `Box<[WorkerRing]>` that
    /// has no `push`, so its address is stable for the receiver's life. The
    /// `W2mReceiver` must outlive every slot; `ReactorShared::w2m` names who
    /// holds it alive.
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
    /// and nothing here guards against one — [`ParkWord::publish`] is what puts
    /// a slot fully below `vwc` before the load below can see it.
    ///
    /// # Safety
    /// The master must be the sole consumer on this ring.
    #[inline]
    unsafe fn take_next(&self) -> Option<W2mSlot> {
        let state = self.in_flight.get();
        let st = &mut *state;
        let base: *const u8 = st.read.base;
        let vwc = self.hdr.master_park.cursor.load(Ordering::Acquire);
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
        if sz == 0 || sz as u64 > MAX_W2M_MSG {
            gnitz_fatal_abort!(
                "w2m::take_next: size={} at phys={} outside (0, {}] — ring corrupt",
                sz,
                st.read.phys,
                MAX_W2M_MSG,
            );
        }
        let payload_at = st.read.phys as usize + 8;
        let bytes = std::slice::from_raw_parts(base.add(payload_at), sz as usize);
        let frame = std::slice::from_raw_parts(
            base.add(payload_at - SLOT_LEN_PREFIX_BYTES),
            sz as usize + SLOT_LEN_PREFIX_BYTES,
        );
        st.read.advance(8 + align8(sz as usize) as u64);

        let push_idx = st.front_idx + st.queue.len() as u64;
        st.queue.push_back((st.read.virt, false));
        Some(W2mSlot {
            bytes,
            frame,
            push_idx,
            internal_req_id,
            state,
        })
    }
}

/// Master's read side of W2M.
pub struct W2mReceiver {
    /// A boxed slice has no `push`/`reserve`, so element addresses — which every
    /// live `W2mSlot` holds — are stable by type rather than by convention.
    rings: Box<[WorkerRing]>,
}

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
                    in_flight: UnsafeCell::new(InFlightState::new(read)),
                }
            })
            .collect();
        W2mReceiver { rings }
    }

    /// Take a slot from the ring without freeing its space. `release_cursor`
    /// advances only when the returned `W2mSlot` is dropped, which is what tells
    /// the writer the bytes are reusable.
    pub fn try_read_slot(&self, worker: usize) -> Option<W2mSlot> {
        // SAFETY: the master thread is the sole consumer of every ring.
        unsafe { self.rings[worker].take_next() }
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
        for w in BitIter(mask) {
            let ring = &self.rings[w];
            let park = &ring.hdr.master_park;
            let vwc = park.arm(bit);
            if vwc != ring.read_cursor() {
                return None;
            }
            out[n] = FutexWaitV::new()
                .val(vwc as u32 as u64)
                .uaddr(futex_word(&park.cursor) as u64)
                .flags(FUTEX2_SIZE_U32);
            n += 1;
        }
        Some(&out[..n])
    }

    fn clear_park(&self, mask: u64, bit: u32) {
        for w in BitIter(mask) {
            self.rings[w].hdr.master_park.disarm(bit);
        }
    }

    fn all_rings(&self) -> u64 {
        worker_mask(self.rings.len())
    }

    /// Arm the reactor's persistent `FUTEX_WAITV` park on every ring, filling
    /// `out` with the words its SQE watches.
    pub fn arm_waitv<'a>(&self, out: &'a mut [FutexWaitV]) -> Option<&'a [FutexWaitV]> {
        self.arm_park(self.all_rings(), FLAG_MASTER_WAITV, out)
    }

    /// Drop the reactor's park. The flag is also cleared by any worker's publish,
    /// so this only has to cover the rings no publish reached.
    pub fn clear_waitv(&self) {
        self.clear_park(self.all_rings(), FLAG_MASTER_WAITV);
    }

    /// Wait until any worker in `workers` publishes, or `timeout_ms` elapses —
    /// the synchronous analogue of the reactor's `FUTEX_WAITV`. Returns at once
    /// if any ring already has unread data. `workers` must be the caller's whole
    /// pending set: only the rings this armed can wake it.
    pub fn wait_any(&self, workers: u64, timeout_ms: i32) -> Parked {
        let mut waiters = [FutexWaitV::new(); gnitz_wire::MAX_WORKERS];
        let rc = match self.arm_park(workers, FLAG_MASTER_SYNC, &mut waiters) {
            Some(armed) => futex_waitv_u32(armed, timeout_ms),
            None => Parked::Retry,
        };
        self.clear_park(workers, FLAG_MASTER_SYNC);
        rc
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
        self.rings[worker].hdr.writer_park.cursor.load(Ordering::Acquire)
    }

    /// The worker's published boundary: everything below it is readable.
    #[cfg(test)]
    pub(crate) fn write_cursor(&self, worker: usize) -> u64 {
        self.rings[worker].hdr.master_park.cursor.load(Ordering::Acquire)
    }

    /// The master-local read cursor for `worker`.
    #[cfg(test)]
    fn read_cursor(&self, worker: usize) -> u64 {
        self.rings[worker].read_cursor()
    }
}

/// The ring fixture, shared by `w2m`'s own suites and by the `runtime` tests
/// that need a live ring. A child of `w2m`, so `init_region` and the cursor
/// primitives stay private to it.
#[cfg(test)]
#[path = "tests/w2m_fixtures.rs"]
pub(crate) mod fixtures;

#[cfg(test)]
#[path = "tests/w2m.rs"]
mod tests;
