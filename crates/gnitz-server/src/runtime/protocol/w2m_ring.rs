//! SPSC tail-chasing ring for worker→master replies.
//!
//! One `MAP_SHARED` mmap per worker: a 128-byte header followed by
//! `DCAP = capacity - W2M_HEADER_SIZE` bytes of data. The producer half is
//! [`try_reserve`] + [`commit`], the consumer half is [`try_consume`]. The
//! park/wake protocol that pairs with them lives in `w2m.rs`, on the side that
//! owns each half's state — which is why the header's atomics are `pub(crate)`
//! rather than hidden behind accessors.
//!
//! ## Cursors
//!
//! `write_cursor` (worker-written) and `release_cursor` (master-written) are
//! monotonic **virtual** offsets, never physical positions. The master's read
//! cursor sits between them — `release <= read <= write` — and is master-local
//! state, not shared. Physical position is
//! `phys(v) = HEADER + (v - HEADER) % DCAP`; each side keeps that mirror
//! memoized in a [`RingCursor`], which is what keeps the modulo — a 64-bit
//! division — off both hot paths.
//!
//! ## SKIP wrap
//!
//! When a message would not fit before `capacity`, the writer stamps
//! `SKIP_MARKER` at the current physical position, publishes the message at
//! `W2M_HEADER_SIZE` instead, and advances its cursor by `pad + total`. The
//! reader jumps the same way, so markers are transparent to it.
//!
//! ## Ordering
//!
//! Each cursor is published with a Release-carrying RMW and read `Acquire`.
//! The RMW is also the store-buffer barrier the park protocol needs; `w2m.rs`
//! states that rule with both of its halves.

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use crate::runtime::posix::{read_u64_raw, write_u64_raw};
use gnitz_wire::align8;

/// Fixed header size at the start of every W2M mmap region.
pub const W2M_HEADER_SIZE: usize = 128;

/// Upper bound on a single W2M wire message (256 MiB). Large enough for any
/// real query response, small enough that `u64::MAX` is an unambiguous
/// SKIP-marker sentinel in the size prefix.
pub const MAX_W2M_MSG: u64 = 1 << 28;

/// Capacity (header + data) of each per-worker W2M mmap region.
///
/// The region is a `memfd` mapped whole, so a ring that sweeps all of `DCAP`
/// every lap holds ~1 GiB of resident shmem per worker for a transport whose
/// live occupancy is a few KiB. The lever is `MAX_W2M_MSG`, which is this large
/// because `worker/exchange.rs` publishes an exchange partition uncapped.
pub const W2M_REGION_SIZE: usize = 1 << 30;
// One maximum-size message must always fit past its own SKIP pad: the wrap
// branch fires only when `total > room_to_end`, so `pad < total <= MAX + 8`.
const _: () = assert!(
    W2M_REGION_SIZE as u64 >= 2 * MAX_W2M_MSG + W2M_HEADER_SIZE as u64 + 16,
    "the region must hold a maximum-size message plus its worst-case SKIP pad",
);
/// A parked side watches a cursor's low 32 bits (`w2m.rs`). One advance is
/// bounded by `DCAP`, so it can never alias a stale snapshot by `k * 2^32`.
const _: () = assert!(W2M_REGION_SIZE < u32::MAX as usize);

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

/// The 8-byte prefix stamped in front of every slot: `sz` in the high half,
/// `internal_req_id` in the low. Written native-endian, so on LE the high half
/// lands at `slot_ptr - 4` as `sz as u32 LE` — which is exactly the client's
/// frame length prefix, and is why [`ConsumedSlot::frame`] needs no re-encode.
#[inline]
fn pack_prefix(sz: usize, internal_req_id: u32) -> u64 {
    (internal_req_id as u64) | ((sz as u64) << 32)
}

/// The `(sz, internal_req_id)` [`pack_prefix`] stored.
#[inline]
fn unpack_prefix(prefix: u64) -> (u32, u32) {
    ((prefix >> 32) as u32, prefix as u32)
}

/// Set by the worker while parked on `release_cursor`; the master reads it
/// before spending a `FUTEX_WAKE` on a retirement.
pub const FLAG_WRITER_PARKED: u32 = 1 << 0;
/// Set by the reactor while its `FUTEX_WAITV` SQE is armed on `write_cursor`.
pub const FLAG_MASTER_WAITV: u32 = 1 << 1;
/// Set by `W2mReceiver::wait_any` while it is synchronously parked on
/// `write_cursor`. A bit of its own because both master parks can be armed at
/// once from the same thread, and whichever unparked first would otherwise clear
/// the other's gate — leaving the still-armed one waiting for a wake no worker
/// will spend.
pub const FLAG_MASTER_SYNC: u32 = 1 << 2;
/// Either master park — the worker's publish gate.
pub const FLAG_MASTER_ANY: u32 = FLAG_MASTER_WAITV | FLAG_MASTER_SYNC;

/// Size-prefix sentinel for "skip to the header, the real message is there".
const SKIP_MARKER: u64 = u64::MAX;

// ---------------------------------------------------------------------------
// Header layout (128 bytes, one cache line per writer)
// ---------------------------------------------------------------------------

/// Cross-process ring state, grouped so each cache line has one writer.
///
/// ```text
/// line A — worker writes, master reads
///    0   write_cursor    AtomicU64   also the master's futex wait word
/// line B — master writes, worker reads
///   64   release_cursor  AtomicU64   also the worker's futex wait word
///   72   waiter_flags    AtomicU32
///   80   capacity        AtomicU64   immutable after init_region
/// ```
///
/// The trade this makes: the worker's per-publish `waiter_flags` read sits on
/// the line the master dirties on every retirement, so it can miss where it
/// would otherwise hit.
#[repr(C, align(64))]
pub struct W2mRingHeader {
    pub(crate) write_cursor: AtomicU64,
    _pad_producer: [u8; 56],

    pub(crate) release_cursor: AtomicU64,
    pub(crate) waiter_flags: AtomicU32,
    _pad_a: u32,
    pub(crate) capacity: AtomicU64,
    _pad_consumer: [u8; 40],
}

const _: () = assert!(std::mem::size_of::<W2mRingHeader>() == W2M_HEADER_SIZE);
const _: () = assert!(std::mem::align_of::<W2mRingHeader>() == 64);

impl W2mRingHeader {
    /// Reinterpret the start of a W2M mmap region as its header. The mapping is
    /// created before `fork` and unmapped only at process exit, which is what
    /// the `'static` rests on.
    ///
    /// # Safety
    /// `ptr` must be a live W2M mmap pointer already initialized by
    /// [`init_region`].
    #[inline]
    pub unsafe fn from_raw(ptr: *const u8) -> &'static Self {
        &*(ptr as *const W2mRingHeader)
    }
}

// ---------------------------------------------------------------------------
// RingCursor — the virtual cursor and its memoized physical mirror
// ---------------------------------------------------------------------------

/// One side's cursor over one ring: the virtual monotonic offset the header
/// publishes, its physical position in the data region, and the mapping both
/// index. Carrying the base is what makes every ring operation take a single
/// argument, so no call can pair a cursor with the wrong region.
///
/// [`Self::advance`] is the only way to move it, and it keeps the two offsets in
/// step with an add and a conditional subtract instead of a division.
#[derive(Clone, Copy)]
pub(crate) struct RingCursor {
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
    pub(crate) unsafe fn producer(base: *mut u8) -> Self {
        Self::seed(base, W2mRingHeader::from_raw(base).write_cursor.load(Ordering::Acquire))
    }

    /// The master's read cursor over the ring at `base`, seeded from
    /// `release_cursor`: a receiver is built with nothing in flight, and
    /// `release == read` exactly then.
    ///
    /// # Safety
    /// `base` must be a live region initialized by [`init_region`].
    pub(crate) unsafe fn consumer(base: *mut u8) -> Self {
        Self::seed(
            base,
            W2mRingHeader::from_raw(base).release_cursor.load(Ordering::Acquire),
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

    #[inline]
    pub(crate) fn header(&self) -> &'static W2mRingHeader {
        // SAFETY: a cursor only ever holds a base its constructors validated.
        unsafe { W2mRingHeader::from_raw(self.base) }
    }

    #[inline]
    pub(crate) fn virt(&self) -> u64 {
        self.virt
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
// init / publish / consume
// ---------------------------------------------------------------------------

/// Zero the header, seat both cursors at the start of the data area and record
/// `capacity`. The asserts below are structural; the deployment's own sizing
/// floor lives at [`W2M_REGION_SIZE`].
///
/// # Safety
/// `ptr` must be a writable mapping of at least `capacity` bytes, with no other
/// thread or process reading or writing the region.
pub unsafe fn init_region(ptr: *mut u8, capacity: u64) {
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
    hdr.write_cursor.store(W2M_HEADER_SIZE as u64, Ordering::Release);
    hdr.release_cursor.store(W2M_HEADER_SIZE as u64, Ordering::Release);
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

/// A claimed slot: encode into [`Self::slot`], then hand it to [`commit`],
/// which is what publishes it.
///
/// Dropping one instead loses the message and corrupts nothing: its prefix sits
/// at or past `write_cursor`, which never advanced, so the reader stops short
/// of it and the next reservation overwrites it.
#[must_use = "a Reservation must be committed or its message is dropped"]
pub(crate) struct Reservation {
    slot_ptr: *mut u8,
    slot_len: usize,
    /// Bytes to advance the write cursor by: `total`, or `pad + total` when
    /// the message SKIP-wrapped.
    delta: u64,
}

impl Reservation {
    /// The bytes to encode the message into.
    #[inline]
    pub(crate) fn slot(&mut self) -> &mut [u8] {
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
pub(crate) unsafe fn try_reserve(wc: &RingCursor, sz: usize, internal_req_id: u32) -> Option<Reservation> {
    assert!(
        sz > 0 && (sz as u64) <= MAX_W2M_MSG,
        "w2m_ring::try_reserve: sz={sz} outside (0, {MAX_W2M_MSG}]",
    );
    let total = (8 + align8(sz)) as u64;
    let vrel = wc.header().release_cursor.load(Ordering::Acquire);
    let (write_at, pad) = publish_at(wc, vrel, total)?;

    if pad != 0 {
        write_u64_raw(wc.base, wc.phys as usize, SKIP_MARKER);
    }
    write_u64_raw(wc.base, write_at as usize, pack_prefix(sz, internal_req_id));
    Some(Reservation {
        slot_ptr: wc.base.add(write_at as usize + 8),
        slot_len: sz,
        delta: pad + total,
    })
}

/// Publish a reservation: advance `wc`, then swap the new value into
/// `write_cursor`. That swap carries Release — it is what makes the slot's
/// bytes visible to the consumer — and, being an RMW, it is also the barrier
/// the master's park reads the flags behind.
///
/// # Safety
/// `r` must come from a [`try_reserve`] against this cursor, with the slot fully
/// written since.
pub(crate) unsafe fn commit(wc: &mut RingCursor, r: Reservation) {
    wc.advance(r.delta);
    wc.header().write_cursor.swap(wc.virt, Ordering::AcqRel);
}

/// One message read out of the ring, as slices into the mapping. `'static` is
/// the mapping's lifetime, on the same grounds as [`W2mRingHeader::from_raw`].
pub(crate) struct ConsumedSlot {
    /// The encoded wire message.
    pub(crate) payload: &'static [u8],
    /// `[sz as u32 LE | payload]` — the client frame, ready for `send_buffer`
    /// with no re-encode. See [`pack_prefix`].
    pub(crate) frame: &'static [u8],
    pub(crate) internal_req_id: u32,
}

/// Read the next message, advancing `rc` past it. Returns `None` **iff** the
/// ring is empty (`rc == write_cursor`), which is what makes a drain loop
/// terminate; every other departure from the layout aborts.
///
/// SKIP markers are transparent: `rc` jumps to the head of the data region and
/// the message there is returned instead.
///
/// Nothing here is defensive against a torn or half-written slot, because none
/// is reachable. The reader touches a prefix only once `rc != vwc`, where
/// `vwc` came from an `Acquire` load of the same location `commit` Release-swaps
/// after writing that prefix; it never touches a byte at or above `vwc`, SKIP
/// pad included; and a worker killed mid-publish dies before `commit`, so
/// `write_cursor` never covers its slot.
///
/// # Safety
/// The caller must be the sole consumer on `rc`'s ring.
pub(crate) unsafe fn try_consume(rc: &mut RingCursor) -> Option<ConsumedSlot> {
    let base: *const u8 = rc.base;
    let vwc = rc.header().write_cursor.load(Ordering::Acquire);
    debug_assert!(rc.virt <= vwc, "read cursor {} ahead of write cursor {vwc}", rc.virt);
    if rc.virt == vwc {
        return None;
    }
    let mut prefix = read_u64_raw(base, rc.phys as usize);
    if prefix == SKIP_MARKER {
        let pad = rc.room_to_end();
        rc.advance(pad);
        if rc.virt == vwc {
            gnitz_fatal_abort!(
                "w2m_ring::try_consume: SKIP marker at phys={} with no message past it — ring corrupt",
                rc.phys,
            );
        }
        prefix = read_u64_raw(base, rc.phys as usize);
    }
    let (sz, internal_req_id) = unpack_prefix(prefix);
    if sz == 0 || sz as u64 > MAX_W2M_MSG {
        gnitz_fatal_abort!(
            "w2m_ring::try_consume: size={} at phys={} outside (0, {}] — ring corrupt",
            sz,
            rc.phys,
            MAX_W2M_MSG,
        );
    }
    let payload_at = rc.phys as usize + 8;
    let consumed = ConsumedSlot {
        payload: std::slice::from_raw_parts(base.add(payload_at), sz as usize),
        frame: std::slice::from_raw_parts(
            base.add(payload_at - SLOT_LEN_PREFIX_BYTES),
            sz as usize + SLOT_LEN_PREFIX_BYTES,
        ),
        internal_req_id,
    };
    rc.advance(8 + align8(sz as usize) as u64);
    Some(consumed)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::tests::fixtures::{make_ring, publish};
    use gnitz_engine_testkit::SharedRegion;

    /// Consume one message and release its space, as the master does when it
    /// drops the slot. The store to `release_cursor` is what frees the producer,
    /// so the backpressure tests need it spelled out here.
    unsafe fn consume_one(rc: &mut RingCursor) -> Option<&'static [u8]> {
        let consumed = try_consume(rc)?;
        rc.header().release_cursor.store(rc.virt(), Ordering::Release);
        Some(consumed.payload)
    }

    /// Round-trip: publish one message, consume it, verify contents and cursor
    /// advance.
    #[test]
    fn test_w2m_ring_round_trip() {
        unsafe {
            let region = make_ring(128, 4, 8);
            let ptr = region.ptr();
            let hdr = W2mRingHeader::from_raw(ptr);

            let payload = [0xAAu8; 128];
            let (new_wc, wrapped) = publish(ptr, payload.len(), 0, |slot| slot.copy_from_slice(&payload))
                .expect("unexpected Full on empty ring");
            assert!(!wrapped);
            assert_eq!(new_wc, W2M_HEADER_SIZE as u64 + 8 + 128);
            assert_eq!(hdr.write_cursor.load(Ordering::Acquire), new_wc);

            let mut rc = RingCursor::consumer(ptr);
            let data = consume_one(&mut rc).expect("message must be visible");
            assert_eq!(data, &payload);
            assert_eq!(rc.virt(), new_wc);
            assert!(consume_one(&mut rc).is_none(), "ring must now read empty");
        }
    }

    /// Three back-to-back publishes decode in FIFO order.
    #[test]
    fn test_w2m_ring_multiple_messages() {
        unsafe {
            let region = make_ring(64, 4, 8);
            let ptr = region.ptr();

            for tag in 0u8..3 {
                publish(ptr, 64, 0, |slot| slot.fill(tag + 1)).expect("unexpected Full");
            }

            let mut rc = RingCursor::consumer(ptr);
            for tag in 0u8..3 {
                let data = consume_one(&mut rc).expect("message must be visible");
                assert_eq!(data.len(), 64);
                assert!(data.iter().all(|&b| b == tag + 1));
            }
            assert!(consume_one(&mut rc).is_none());
        }
    }

    /// A SKIP-wrap with the reader still behind: room for 3 messages + 16 bytes
    /// of slack, the reader lagging by exactly one.
    #[test]
    fn test_w2m_skip_marker_strict_wrap() {
        unsafe {
            let big_sz = 1 << 16; // 64 KiB
            let region = make_ring(big_sz, 3, 16);
            let ptr = region.ptr();

            for _ in 0..3 {
                publish(ptr, big_sz, 0, |_| {}).expect("initial big publish");
            }
            let mut rc = RingCursor::consumer(ptr);
            for _ in 0..2 {
                consume_one(&mut rc).expect("consume");
            }

            // The 4th no longer fits before the physical end, and the reader is
            // two messages ahead of the head, so the wrap has somewhere to land.
            let (_, wrapped) = publish(ptr, big_sz, 0, |slot| slot[0] = 0xDE).expect("SKIP wrap must succeed");
            assert!(wrapped, "the 4th publish must SKIP-wrap");

            assert_eq!(
                consume_one(&mut rc).expect("pre-SKIP big").len(),
                big_sz,
                "the third message still reads contiguously",
            );
            let wrapped_msg = consume_one(&mut rc).expect("wrapped big via SKIP");
            assert_eq!(wrapped_msg.len(), big_sz);
            assert_eq!(wrapped_msg[0], 0xDE, "the SKIP jump must land on the wrapped payload");
        }
    }

    /// A message that ends exactly at `capacity` publishes contiguously: the
    /// next write position is the header, with no marker needed.
    #[test]
    fn test_w2m_exact_fit_publishes_contiguously() {
        unsafe {
            let msg_sz = 1 << 12;
            const N: usize = 4;
            // Zero slack: DCAP is exactly N messages, so the N-th ends on `cap`.
            let region = make_ring(msg_sz, N, 0);
            let ptr = region.ptr();
            let total = 8 + align8(msg_sz) as u64;

            let mut rc = RingCursor::consumer(ptr);
            for i in 0..N {
                let (new_wc, wrapped) = publish(ptr, msg_sz, 0, |_| {}).unwrap_or_else(|| panic!("publish #{i}"));
                assert!(!wrapped, "publish #{i} must fit contiguously, not wrap");
                assert_eq!(new_wc, W2M_HEADER_SIZE as u64 + (i as u64 + 1) * total);
                consume_one(&mut rc).expect("consume");
            }
        }
    }

    /// With the ring exactly full and the reader at the head, the next publish
    /// is refused — backpressure, not a wrap over unread data.
    #[test]
    fn test_w2m_full_blocks_writer() {
        unsafe {
            let msg_sz = 1 << 16; // 64 KiB
            let region = make_ring(msg_sz, 2, 8);
            let ptr = region.ptr();

            for _ in 0..2 {
                publish(ptr, msg_sz, 0, |_| {}).expect("fill publish");
            }
            assert!(
                publish(ptr, msg_sz, 0, |_| {}).is_none(),
                "an undrained ring must refuse the publish"
            );
        }
    }

    /// Publishing past `MAX_W2M_MSG` is a caller bug, and the ring says so
    /// rather than reporting a full ring the caller would park on forever.
    #[test]
    #[should_panic(expected = "outside (0,")]
    fn test_w2m_oversized_publish_panics() {
        unsafe {
            let region = make_ring(64, 4, 8);
            let _ = publish(region.ptr(), (MAX_W2M_MSG + 1) as usize, 0, |_| {});
        }
    }

    /// The payload handed back points into the mmap region — zero-copy into
    /// `decode_wire`.
    #[test]
    fn test_w2m_decode_wire_zero_copy() {
        unsafe {
            let region = make_ring(256, 4, 8);
            let ptr = region.ptr();
            publish(ptr, 256, 0, |slot| slot.fill(0xCD)).expect("unexpected Full");

            let mut rc = RingCursor::consumer(ptr);
            let data = consume_one(&mut rc).expect("message must be visible");
            assert_eq!(
                data.as_ptr(),
                ptr.add(W2M_HEADER_SIZE + 8) as *const u8,
                "payload must be mmap-resident (zero copy)",
            );
        }
    }

    /// Regression: a writer that wraps must never land on a slot the reader has
    /// not drained. Publish 4, consume 3, then publish 5..=9 — the sequence that
    /// let a physical-cursor predicate overwrite message #4.
    #[test]
    fn test_writer_does_not_cross_reader_after_wrap() {
        unsafe {
            let msg_sz: usize = 64;
            let region = make_ring(msg_sz, 5, 16);
            let ptr = region.ptr();
            let mut rc = RingCursor::consumer(ptr);

            let publish_tag = |tag: u8| publish(ptr, msg_sz, 0, |slot| slot.fill(tag)).is_some();

            for tag in 1u8..=4 {
                assert!(publish_tag(tag), "publish #{tag}");
            }

            let mut received = Vec::new();
            for _ in 0..3 {
                received.push(consume_one(&mut rc).expect("consume")[0]);
            }
            assert_eq!(received, vec![1, 2, 3]);

            // #5 fits contiguously, #6 forces the wrap, #7..=9 chase the reader.
            for tag in 5u8..=9 {
                if !publish_tag(tag) {
                    received.push(consume_one(&mut rc).expect("drain to make room")[0]);
                    assert!(publish_tag(tag), "publish #{tag} after drain");
                }
            }

            while let Some(data) = consume_one(&mut rc) {
                received.push(data[0]);
            }
            assert_eq!(
                received,
                vec![1, 2, 3, 4, 5, 6, 7, 8, 9],
                "the writer must not overwrite unread data after a wrap",
            );
        }
    }

    /// An unaligned capacity would let the SKIP path's 8-byte writes cross the
    /// end of the mapping.
    #[test]
    #[should_panic(expected = "8-byte aligned")]
    fn init_region_rejects_unaligned_capacity() {
        unsafe {
            let cap = W2M_HEADER_SIZE + 17;
            let region = SharedRegion::new(cap);
            init_region(region.ptr(), cap as u64);
        }
    }

    /// A capacity with no room for a message past the header is refused.
    #[test]
    #[should_panic(expected = "leaves no room")]
    fn init_region_rejects_undersized_capacity() {
        unsafe {
            let cap = W2M_HEADER_SIZE + 8;
            let region = SharedRegion::new(cap);
            init_region(region.ptr(), cap as u64);
        }
    }
}
