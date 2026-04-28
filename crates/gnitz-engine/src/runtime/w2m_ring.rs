//! SPSC tail-chasing ring for worker→master replies.
//!
//! Each W2M region is a `MAP_SHARED` mmap: a 128-byte header followed by
//! a data region sized to `capacity - W2M_HEADER_SIZE` bytes. The header
//! splits across two cache lines so the producer's publish stores never
//! contend with the consumer's advance stores.
//!
//! ## Cursors are virtual monotonic offsets
//!
//! `write_cursor` and `read_cursor` are `u64` virtual offsets that only
//! ever increase; they are NOT physical positions inside the data
//! region. Physical position is derived:
//!
//! ```text
//!     DCAP = capacity - W2M_HEADER_SIZE
//!     phys(virt) = W2M_HEADER_SIZE + (virt - W2M_HEADER_SIZE) % DCAP
//! ```
//!
//! Core invariants:
//!
//! - **Empty:** `virt_wc == virt_rc`.
//! - **Unread bytes:** `virt_wc - virt_rc` (always `<= DCAP`).
//! - **Has room:** `virt_wc - virt_rc + total <= DCAP` (contiguous), or
//!   `virt_wc - virt_rc + pad + total <= DCAP` (SKIP-wrap, with
//!   `pad = cap - phys(virt_wc)`).
//!
//! `u64` overflow is a non-issue: even at 1 M msg/s × 280 B/msg
//! sustained, a `u64` virtual cursor takes ~65 years to wrap.
//!
//! ## SKIP wrap
//!
//! Wraparound uses a **SKIP marker** (`u64::MAX` as a size prefix). When
//! the next message would not fit contiguously before `capacity`, the
//! writer stamps a SKIP at the current `phys(virt_wc)`, publishes the
//! real message at `W2M_HEADER_SIZE`, and advances `virt_wc` by
//! `pad + total` (where `pad = cap - phys(virt_wc)`). The reader, on
//! encountering `SKIP_MARKER`, advances `virt_rc` by `pad` and reads the
//! message at `W2M_HEADER_SIZE`. The marker value is safe because
//! legitimate messages are bounded by `MAX_W2M_MSG` (256 MiB), far
//! below `u64::MAX`.
//!
//! Backpressure uses two flags in `waiter_flags`:
//! - `FLAG_WRITER_PARKED`: writer is parked on `writer_seq` waiting for
//!   the reader to advance `read_cursor`.
//! - `FLAG_MASTER_PARKED`: reader is parked on `reader_seq` waiting for
//!   the writer to publish.
//!
//! Wake-side stores to the peer's sequence counter plus a raw
//! `FUTEX_WAKE` syscall are gated on the flag to avoid unnecessary
//! syscalls in the hot path.
//!
//! Ordering:
//! - Cursors: Release on publish, Acquire on consume. The Release store
//!   to `write_cursor` synchronizes all prior payload/size-prefix
//!   writes; the Acquire load by the consumer is what makes those writes
//!   visible.
//! - Sequence counters: `writer_seq` and `reader_seq` use `Release` on
//!   fetch_add; parked sides use `Acquire` loads when sampling the
//!   expected value. The raw v1 `FUTEX_WAIT` syscall itself adds no
//!   memory-ordering beyond the user-space atomic load.
//! - `waiter_flags` uses `AcqRel` on fetch_or/fetch_and so the flag bit
//!   publishes before the subsequent cursor recheck (closes TOCTOU).

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use gnitz_wire::align8;

use crate::util::{read_u64_raw, write_u64_raw};

/// Fixed header size at the start of every W2M mmap region.
pub const W2M_HEADER_SIZE: usize = 128;

/// Capacity (header + data) of each per-worker W2M mmap region.
pub const W2M_REGION_SIZE: usize = 1 << 30;

/// Upper bound on a single W2M wire message (256 MiB). Large enough for
/// any real query response, small enough that `u64::MAX` can serve as
/// an unambiguous SKIP-marker sentinel in the size prefix.
pub const MAX_W2M_MSG: u64 = 1 << 28;

/// Writer set this bit in `waiter_flags` while parked on `writer_seq`.
/// Reader reads it before skipping a `FUTEX_WAKE` on publish.
pub const FLAG_WRITER_PARKED: u32 = 1 << 0;

/// Master sets this bit in `waiter_flags` while parked on `reader_seq`
/// (typically inside the io_uring `FUTEX_WAITV` SQE). Writer reads it
/// before skipping a `FUTEX_WAKE` on publish.
pub const FLAG_MASTER_PARKED: u32 = 1 << 1;

/// Size-prefix sentinel for "skip to header, real message is there".
/// `u64::MAX` is safe because legitimate sizes are capped at `MAX_W2M_MSG`.
pub const SKIP_MARKER: u64 = u64::MAX;

// ---------------------------------------------------------------------------
// Header layout (128 bytes, 2 cache lines)
// ---------------------------------------------------------------------------

/// Two-cache-line header shared between producer and consumer.
///
/// Layout (each cache line = 64 bytes):
/// ```text
/// offset  size  field                  atomic
/// ---------------------------------------------
///  0      8     write_cursor           AtomicU64  (W publishes; virtual)
///  8      4     writer_seq             AtomicU32  (W parks on this)
/// 12      4     waiter_flags           AtomicU32  (both sides)
/// 16      8     writer_wrap_count      AtomicU64  (telemetry)
/// 24     40     _pad_producer
/// ---------------------------------------------  cache-line boundary
/// 64      8     read_cursor            AtomicU64  (M publishes; virtual)
/// 72      4     reader_seq             AtomicU32  (M parks via FUTEX_WAITV)
/// 76      4     _pad_a
/// 80      8     capacity               u64        (immutable)
/// 88      8     consume_cursor         AtomicU64  (M owns; free-space sentinel)
/// 96     32     _pad_consumer
/// ---------------------------------------------
/// 128+          data region
/// ```
///
/// `write_cursor` and `read_cursor` are **virtual monotonic offsets**
/// (see module docs). Physical position inside the data region is
/// `phys(virt) = HEADER + (virt - HEADER) % (capacity - HEADER)`.
///
/// `waiter_flags` lives on the producer cache line by deliberate choice:
/// both sides mutate it, but the writer reads it on *every* publish
/// (the hot path), whereas the master reads it only on
/// `try_consume` (which already walks the producer line to load
/// `write_cursor`). Co-locating with `write_cursor` keeps the hot
/// producer path single-cache-line.
#[repr(C, align(64))]
pub struct W2mRingHeader {
    write_cursor: AtomicU64,
    writer_seq: AtomicU32,
    waiter_flags: AtomicU32,
    writer_wrap_count: AtomicU64,
    _pad_producer: [u8; 40],

    read_cursor: AtomicU64,
    reader_seq: AtomicU32,
    _pad_a: u32,
    capacity: u64,
    consume_cursor: AtomicU64,
    _pad_consumer: [u8; 32],
}

const _: () = assert!(std::mem::size_of::<W2mRingHeader>() == W2M_HEADER_SIZE);
const _: () = assert!(std::mem::align_of::<W2mRingHeader>() == 64);

impl W2mRingHeader {
    /// Reinterpret `ptr` (start of a W2M mmap region) as a `&'static`
    /// reference to the header. The mmap is process-lifetime, so the
    /// `'static` lifetime is sound as long as the region isn't unmapped
    /// before the last reference is dropped (which only happens at
    /// process exit).
    ///
    /// # Safety
    /// `ptr` must be a valid, properly-aligned W2M mmap pointer already
    /// initialized by `init_region`.
    #[inline]
    pub unsafe fn from_raw(ptr: *const u8) -> &'static Self {
        &*(ptr as *const W2mRingHeader)
    }

    #[inline]
    pub fn write_cursor(&self) -> &AtomicU64 { &self.write_cursor }
    #[inline]
    pub fn read_cursor(&self) -> &AtomicU64 { &self.read_cursor }
    #[inline]
    pub fn consume_cursor(&self) -> &AtomicU64 { &self.consume_cursor }
    #[cfg(test)]
    pub fn advance_read_cursors(&self, new_rc: u64) {
        self.read_cursor().store(new_rc, Ordering::Release);
        self.consume_cursor().store(new_rc, Ordering::Release);
    }
    #[inline]
    pub fn advance_read_cursor(&self, new_rc: u64) {
        self.read_cursor().store(new_rc, Ordering::Release);
    }
    #[inline]
    pub fn advance_consume_cursor(&self, new_cc: u64) {
        self.consume_cursor().store(new_cc, Ordering::Release);
    }
    #[inline]
    pub fn writer_seq(&self) -> &AtomicU32 { &self.writer_seq }
    #[inline]
    pub fn reader_seq(&self) -> &AtomicU32 { &self.reader_seq }
    #[inline]
    pub fn waiter_flags(&self) -> &AtomicU32 { &self.waiter_flags }
    #[inline]
    pub fn capacity(&self) -> u64 { self.capacity }
    #[cfg(test)]
    #[inline]
    pub fn writer_wrap_count(&self) -> u64 {
        self.writer_wrap_count.load(Ordering::Relaxed)
    }
}

// ---------------------------------------------------------------------------
// init / publish / consume
// ---------------------------------------------------------------------------

/// Zero the header and set both virtual cursors to `W2M_HEADER_SIZE`,
/// capacity to the given value. Panics if `capacity` can't fit the
/// worst-case wrap waste (one full SKIP pad of nearly `MAX_W2M_MSG`)
/// plus another in-flight maximum-size message, with 16 bytes of
/// alignment slack.
///
/// # Safety
/// `ptr` must be a valid, writable mmap pointer of at least `capacity`
/// bytes, and no other thread may be reading/writing the region.
pub unsafe fn init_region(ptr: *mut u8, capacity: u64) {
    assert!(
        capacity >= 2 * MAX_W2M_MSG + W2M_HEADER_SIZE as u64 + 16,
        "W2M capacity={} too small; must be >= 2*MAX_W2M_MSG ({}) + HEADER ({}) + 16",
        capacity, 2 * MAX_W2M_MSG, W2M_HEADER_SIZE,
    );
    // The SKIP-wrap path writes an 8-byte sentinel and an 8-byte size prefix
    // at phys(vwc). If capacity is not 8-aligned, phys(vwc) can sit fewer than
    // 8 bytes from the end of the buffer, causing those writes to cross the
    // mmap boundary.
    assert!(capacity.is_multiple_of(8), "W2M capacity={} must be 8-byte aligned", capacity);
    // Zero the header first so re-init on a previously-live region
    // clears every byte, including padding.
    std::ptr::write_bytes(ptr, 0, W2M_HEADER_SIZE);
    let hdr = &*(ptr as *const W2mRingHeader);
    hdr.write_cursor.store(W2M_HEADER_SIZE as u64, Ordering::Release);
    hdr.read_cursor.store(W2M_HEADER_SIZE as u64, Ordering::Release);
    hdr.consume_cursor.store(W2M_HEADER_SIZE as u64, Ordering::Release);
    // capacity is a plain u64 — not atomic — so write it through the
    // raw pointer to avoid forming a &mut reference to the shared
    // struct (which would violate the `&'static Self` aliasing model
    // used elsewhere).
    write_u64_raw(ptr, 80, capacity);
}

/// Map a virtual cursor to its physical byte offset inside the mmap
/// region. `cap` is the total region capacity (header + data). The
/// returned offset is always in `[W2M_HEADER_SIZE, cap)`.
#[inline]
fn phys(virt: u64, cap: u64) -> u64 {
    let header = W2M_HEADER_SIZE as u64;
    let dcap = cap - header;
    debug_assert!(virt >= header, "virtual cursor below HEADER: {}", virt);
    debug_assert!(dcap > 0, "data capacity must be positive");
    header + (virt - header) % dcap
}

/// Predicate: does the ring have room to publish `total` bytes given the
/// current virtual cursors? `total` is the encoded message size (8-byte
/// size prefix + payload, padded to 8-byte alignment). Returns true for
/// either a contiguous publish or a SKIP-wrap publish.
#[inline]
fn room_for(vwc: u64, vrc: u64, total: u64, cap: u64) -> bool {
    let header = W2M_HEADER_SIZE as u64;
    let dcap = cap - header;
    debug_assert!(vrc <= vwc, "cursor inversion: vrc={} vwc={}", vrc, vwc);
    debug_assert!(vwc - vrc <= dcap,
        "used {} exceeds DCAP {} (vwc={} vrc={})", vwc - vrc, dcap, vwc, vrc);
    let used = vwc - vrc;
    let phys_wc = phys(vwc, cap);
    let room_to_end = cap - phys_wc;
    if total + 8 <= room_to_end {
        used + total <= dcap
    } else {
        let pad = room_to_end;
        used + pad + total <= dcap
    }
}

/// Outcome of `try_publish`.
#[cfg(test)]
pub enum TryPublish {
    /// Published successfully; the new (virtual) write cursor is returned.
    Ok(u64),
    /// No contiguous room before `capacity` and no room to wrap without
    /// lapping the reader. The caller must park.
    Full,
}

/// Outcome of `reserve`: a callable slot the producer fills, plus the
/// cursor value to commit via `commit`. `reserve` does NOT update
/// `write_cursor` — the producer must call `commit` after filling the
/// slot so the consumer's Acquire pairing sees the populated payload.
#[must_use = "Reservation must be passed to commit(); dropping it silently corrupts the ring"]
pub struct Reservation {
    /// Raw mutable pointer to where the caller encodes the message
    /// payload (past the 8-byte size prefix that `reserve` has
    /// already stamped).
    pub(crate) slot_ptr: *mut u8,
    /// Slice length in bytes (exactly `sz` from the original call).
    pub(crate) slot_len: usize,
    /// New (virtual) `write_cursor` value to store on commit.
    pub(crate) new_wc: u64,
    /// True if a SKIP wrap occurred; commit increments
    /// `writer_wrap_count`.
    pub(crate) wrapped: bool,
    committed: bool,
}

impl Drop for Reservation {
    fn drop(&mut self) {
        if !self.committed {
            crate::gnitz_fatal_abort!(
                "w2m_ring: Reservation dropped without commit — \
                 size prefix published but write_cursor not advanced; ring is now corrupt"
            );
        }
    }
}

/// Outcome of `reserve`.
pub(crate) enum TryReserve {
    Ok(Reservation),
    Full,
}

/// Publish one message into the ring.
///
/// Two cases:
///
/// 1. **Contiguous publish.** Message fits at `phys(virt_wc)` without
///    straddling the physical end of the data region. Advance virt_wc
///    by `total`.
/// 2. **SKIP-wrap.** Message would straddle the physical end. Stamp
///    `SKIP_MARKER` at `phys(virt_wc)`, publish the real message at
///    `W2M_HEADER_SIZE` (physical), and advance virt_wc by
///    `pad + total` (where `pad = cap - phys(virt_wc)`).
///
/// Both are gated by `room_for`, which uses the unambiguous virtual
/// invariant `virt_wc - virt_rc <= DCAP` to detect whether the next
/// publish would lap the reader.
///
/// SKIP markers are transparent to the consumer.
///
/// # Safety
/// `hdr` must be the header inside the same mmap region as `data_base`
/// (which is the same pointer reinterpreted as `*mut u8`). The caller
/// must be the sole producer on this ring.
#[cfg(test)]
pub unsafe fn try_publish(
    hdr: &W2mRingHeader,
    data_base: *mut u8,
    sz: usize,
    encode: impl FnOnce(&mut [u8]),
) -> TryPublish {
    match try_reserve(hdr, data_base, sz) {
        TryReserve::Ok(r) => {
            if r.slot_len > 0 {
                let slice = std::slice::from_raw_parts_mut(r.slot_ptr, r.slot_len);
                encode(slice);
            }
            let new_wc = r.new_wc;
            commit(hdr, r);
            TryPublish::Ok(new_wc)
        }
        TryReserve::Full => TryPublish::Full,
    }
}

/// Reserve a slot for a message of size `sz` in the ring without
/// committing. On success the caller must encode into the returned
/// slot and then call `commit`. On `Full` the caller must park.
///
/// Split from `try_publish` so a blocking sender can retry the
/// reservation without needing to clone its encode closure (which is
/// `FnOnce`).
///
/// Stores the size prefix (or `SKIP_MARKER` for a wrap) inside the
/// reservation. These stores aren't yet visible to the consumer —
/// `commit` is what Release-stores the new `write_cursor`.
///
/// # Safety
/// Same preconditions as `try_publish`. The caller must not interleave
/// reserves; each `reserve` must pair with exactly one `commit`.
pub(crate) unsafe fn try_reserve(
    hdr: &W2mRingHeader,
    data_base: *mut u8,
    sz: usize,
) -> TryReserve {
    if (sz as u64) > MAX_W2M_MSG {
        crate::gnitz_error!("w2m_ring::try_reserve: sz={} exceeds MAX_W2M_MSG", sz);
        return TryReserve::Full;
    }

    let total = (8 + align8(sz)) as u64;
    let cap = hdr.capacity;
    let vwc = hdr.write_cursor.load(Ordering::Acquire);
    let vcc = hdr.consume_cursor().load(Ordering::Acquire);

    if !room_for(vwc, vcc, total, cap) {
        return TryReserve::Full;
    }

    let phys_wc = phys(vwc, cap);
    let room_to_end = cap - phys_wc;

    if total + 8 <= room_to_end {
        // Contiguous publish at the current physical position.
        let base = phys_wc as usize;
        write_u64_raw(data_base, base, sz as u64);
        TryReserve::Ok(Reservation {
            slot_ptr: data_base.add(base + 8),
            slot_len: sz,
            new_wc: vwc + total,
            wrapped: false,
            committed: false,
        })
    } else {
        // SKIP-wrap: pad with a SKIP marker, publish at HEADER.
        let pad = room_to_end;
        write_u64_raw(data_base, phys_wc as usize, SKIP_MARKER);
        let base = W2M_HEADER_SIZE;
        write_u64_raw(data_base, base, sz as u64);
        TryReserve::Ok(Reservation {
            slot_ptr: data_base.add(base + 8),
            slot_len: sz,
            new_wc: vwc + pad + total,
            wrapped: true,
            committed: false,
        })
    }
}

/// Test-only relaxed `init_region` that skips the `2 * MAX_W2M_MSG +
/// W2M_HEADER_SIZE` lower bound on `capacity`. Tests use small
/// regions (~1 MiB) so SKIP-wraps are forced within a few thousand
/// messages; production must always go through `init_region`.
///
/// # Safety
/// Same preconditions as `init_region`.
#[cfg(test)]
pub(crate) unsafe fn init_region_for_tests(ptr: *mut u8, capacity: u64) {
    assert!(capacity >= W2M_HEADER_SIZE as u64 + 16);
    assert!(capacity % 8 == 0, "W2M capacity={} must be 8-byte aligned", capacity);
    std::ptr::write_bytes(ptr, 0, W2M_HEADER_SIZE);
    let hdr = &*(ptr as *const W2mRingHeader);
    hdr.write_cursor.store(W2M_HEADER_SIZE as u64, Ordering::Release);
    hdr.read_cursor.store(W2M_HEADER_SIZE as u64, Ordering::Release);
    hdr.consume_cursor.store(W2M_HEADER_SIZE as u64, Ordering::Release);
    write_u64_raw(ptr, 80, capacity);
}

/// Read-only predicate: would a `try_reserve` for `sz` bytes succeed
/// right now? Used by writer backpressure to re-check (TOCTOU) after
/// publishing `FLAG_WRITER_PARKED` but before going into
/// `FUTEX_WAIT`. Side-effect free.
///
/// # Safety
/// `hdr` must be a valid, initialized W2M ring header.
pub(crate) unsafe fn has_room(hdr: &W2mRingHeader, sz: usize) -> bool {
    if (sz as u64) > MAX_W2M_MSG {
        return false;
    }
    let total = (8 + align8(sz)) as u64;
    let cap = hdr.capacity;
    let vwc = hdr.write_cursor.load(Ordering::Acquire);
    let vcc = hdr.consume_cursor().load(Ordering::Acquire);
    room_for(vwc, vcc, total, cap)
}

/// Commit a previously-returned `Reservation`: Release-store the new
/// `write_cursor` and bump `writer_wrap_count` if the reservation
/// wrapped. Consumes the reservation to enforce single-commit.
///
/// # Safety
/// `r` must be the result of a `try_reserve` call against the same
/// `hdr`, with the caller having fully written into the returned slot
/// since. Skipping the encode step leaves the ring in an inconsistent
/// state.
pub(crate) unsafe fn commit(hdr: &W2mRingHeader, mut r: Reservation) {
    debug_assert!(
        r.new_wc > hdr.write_cursor.load(Ordering::Relaxed),
        "commit must monotonically advance write_cursor: new_wc={} cur={}",
        r.new_wc, hdr.write_cursor.load(Ordering::Relaxed),
    );
    hdr.write_cursor.store(r.new_wc, Ordering::Release);
    if r.wrapped {
        hdr.writer_wrap_count.fetch_add(1, Ordering::Relaxed);
    }
    r.committed = true;
}

/// Try to read the next message from the ring.
///
/// `read_cursor` is the caller's snapshot of the *virtual* read cursor.
/// On success returns `(data_ptr, size, new_cursor)` — the caller must
/// store `new_cursor` back into `hdr.read_cursor()` (plus bump
/// `writer_seq` / wake a parked writer).
///
/// `SKIP_MARKER`s are transparent: when encountered, the virtual cursor
/// is advanced by `pad = cap - phys(rc)` bytes and the message past it
/// is decoded at `phys(rc + pad) == W2M_HEADER_SIZE`. The returned
/// `new_cursor` reflects the post-jump virtual position.
///
/// "Empty" is `virt_rc == virt_wc` *exactly*. Virtual cursors never
/// invert, so the comparison is unambiguous.
///
/// # Safety
/// Same preconditions as `try_publish`. The caller must be the sole
/// consumer on this ring (today: the reactor).
pub unsafe fn try_consume(
    hdr: &W2mRingHeader,
    data_base: *const u8,
    read_cursor: u64,
) -> Option<(*const u8, u32, u64)> {
    let mut vrc = read_cursor;
    let vwc = hdr.write_cursor.load(Ordering::Acquire);
    debug_assert!(vrc <= vwc, "read_cursor {} ahead of write_cursor {}", vrc, vwc);
    if vrc == vwc {
        return None;
    }
    let cap = hdr.capacity;
    let mut phys_rc = phys(vrc, cap);
    let raw_size = read_u64_raw(data_base, phys_rc as usize);
    let size = if raw_size == SKIP_MARKER {
        let pad = cap - phys_rc;
        vrc += pad;
        phys_rc = phys(vrc, cap);
        if vrc == vwc {
            // Writer stamped the SKIP but commit (Release-store of
            // virt_wc) hasn't yet covered the post-jump publish — not
            // reachable under the normal publish algorithm (commit is
            // post-everything), but treat defensively as empty.
            return None;
        }
        read_u64_raw(data_base, phys_rc as usize)
    } else {
        raw_size
    };
    if size == 0 {
        // Stale zero or torn read from a pre-wrap region the reader
        // hasn't re-visited since. Treat as empty; next wake will
        // retry.
        return None;
    }
    if size > MAX_W2M_MSG {
        crate::gnitz_fatal_abort!(
            "w2m_ring::try_consume: size={} at phys={} exceeds MAX_W2M_MSG={} — ring corrupt",
            size, phys_rc, MAX_W2M_MSG,
        );
    }
    let total = 8 + align8(size as usize);
    let new_vrc = vrc + total as u64;
    let ptr = data_base.add(phys_rc as usize + 8);
    Some((ptr, size as u32, new_vrc))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering;

    /// Allocate a zero-initialized anonymous mmap region of `size` bytes.
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

    fn region_size_for_tests() -> usize {
        // Just enough to satisfy the capacity invariant.
        let min_cap = (2 * MAX_W2M_MSG) as usize + W2M_HEADER_SIZE;
        // Round up to a megabyte.
        (min_cap + (1 << 20) - 1) & !((1 << 20) - 1)
    }

    /// Round-trip helper: consume one message via `hdr.read_cursor` and
    /// return `(ptr, sz)`. Bumps `hdr.read_cursor` on success.
    unsafe fn consume_one(
        hdr: &W2mRingHeader, ptr: *const u8,
    ) -> Option<(*const u8, u32)> {
        let rc = hdr.read_cursor().load(Ordering::Acquire);
        let (p, sz, new_rc) = try_consume(hdr, ptr, rc)?;
        hdr.advance_read_cursors(new_rc);
        Some((p, sz))
    }

    /// Round-trip: publish one message, consume it, verify contents and
    /// cursor advance.
    #[test]
    fn test_w2m_ring_round_trip() {
        unsafe {
            let size = region_size_for_tests();
            let ptr = alloc_region(size);
            init_region(ptr, size as u64);
            let hdr = W2mRingHeader::from_raw(ptr);

            let payload = [0xAAu8; 128];
            let published = match try_publish(hdr, ptr, payload.len(), |slot| {
                slot.copy_from_slice(&payload);
            }) {
                TryPublish::Ok(n) => n,
                TryPublish::Full => panic!("unexpected Full on empty ring"),
            };
            assert_eq!(
                published, W2M_HEADER_SIZE as u64 + 8 + 128,
                "new_wc must advance by 8 + aligned payload",
            );
            assert_eq!(
                hdr.write_cursor().load(Ordering::Acquire),
                W2M_HEADER_SIZE as u64 + 8 + 128,
            );

            let (data_ptr, sz, new_rc) = try_consume(hdr, ptr, W2M_HEADER_SIZE as u64)
                .expect("message must be visible");
            assert_eq!(sz, 128);
            assert_eq!(new_rc, published);
            let data = std::slice::from_raw_parts(data_ptr, sz as usize);
            assert_eq!(data, &payload);

            // Subsequent consume with the new cursor sees empty.
            assert!(try_consume(hdr, ptr, new_rc).is_none());

            free_region(ptr, size);
        }
    }

    /// Three back-to-back publishes decode in FIFO order.
    #[test]
    fn test_w2m_ring_multiple_messages() {
        unsafe {
            let size = region_size_for_tests();
            let ptr = alloc_region(size);
            init_region(ptr, size as u64);
            let hdr = W2mRingHeader::from_raw(ptr);

            for tag in 0u8..3 {
                let payload = vec![tag + 1; 64];
                match try_publish(hdr, ptr, payload.len(), |slot| {
                    slot.copy_from_slice(&payload);
                }) {
                    TryPublish::Ok(_) => {}
                    TryPublish::Full => panic!("unexpected Full"),
                }
            }

            let mut rc = W2M_HEADER_SIZE as u64;
            for tag in 0u8..3 {
                let (data_ptr, sz, new_rc) = try_consume(hdr, ptr, rc)
                    .expect("message must be visible");
                assert_eq!(sz, 64);
                let data = std::slice::from_raw_parts(data_ptr, sz as usize);
                assert!(data.iter().all(|&b| b == tag + 1));
                rc = new_rc;
            }
            assert!(try_consume(hdr, ptr, rc).is_none());

            free_region(ptr, size);
        }
    }

    /// When a message doesn't fit before `capacity`, the writer emits a
    /// SKIP marker and wraps. The consumer jumps transparently.
    ///
    /// Scenario: cap holds 3 messages, we publish-consume interleaved
    /// to keep the reader ~1 message behind. The third publish's total
    /// crosses `cap`, forcing a wrap while rc is strictly ahead of
    /// `wrap_end`.
    #[test]
    fn test_w2m_skip_marker_wraparound() {
        unsafe {
            let msg_sz = 1 << 16; // 64 KiB
            let msg_total = 8 + align8(msg_sz) as u64;
            // Room for 2 full messages + a bit — so the 3rd publish
            // can't fit contiguously but wrap_end < rc is satisfied
            // after a consume.
            let capacity = W2M_HEADER_SIZE as u64 + 2 * msg_total + 64;
            let size = capacity as usize;
            let ptr = alloc_region(size);
            init_region_raw(ptr, capacity);
            let hdr = W2mRingHeader::from_raw(ptr);

            // Publish 1, consume 1, publish 2, consume 2, publish 3.
            let mut rc = W2M_HEADER_SIZE as u64;
            for tag in 0u8..2 {
                match try_publish(hdr, ptr, msg_sz, |slot| { slot[0] = tag; }) {
                    TryPublish::Ok(_) => {}
                    TryPublish::Full => panic!("tag {} publish", tag),
                }
                let (_data_ptr, sz, new_rc) = try_consume(hdr, ptr, rc)
                    .expect("tag consume");
                assert_eq!(sz, msg_sz as u32);
                hdr.advance_read_cursors(new_rc);
                rc = new_rc;
            }
            // After two publish+consume, wc == rc. Third publish
            // triggers the wc==rc reset path (no SKIP) OR the wrap path
            // depending on cap. With cap = HEADER + 2*total + 64, the
            // third msg (total = msg_total) has wc+total = HEADER +
            // 2*total + total = HEADER + 3*total > cap, so reset fires.
            match try_publish(hdr, ptr, msg_sz, |slot| { slot[0] = 0xCC; }) {
                TryPublish::Ok(_) => {}
                TryPublish::Full => panic!("reset-based publish must succeed"),
            }
            // Consumer: after reset, rc must be re-loaded from hdr —
            // the writer's reset invalidated our local snapshot.
            rc = hdr.read_cursor().load(Ordering::Acquire);
            let (data_ptr, sz, _new_rc) = try_consume(hdr, ptr, rc)
                .expect("message after reset");
            assert_eq!(sz, msg_sz as u32);
            let d = std::slice::from_raw_parts(data_ptr, 1);
            assert_eq!(d[0], 0xCC);

            free_region(ptr, size);
        }
    }

    /// A pure SKIP-wrap (not empty-reset): cap leaves room for 3 big
    /// messages + SKIP headroom, and the reader lags by exactly 1.
    /// Forces SKIP wrap to fire with `wrap_end < rc` strictly.
    #[test]
    fn test_w2m_skip_marker_strict_wrap() {
        unsafe {
            let big_sz = 1 << 16; // 64 KiB
            let big_total = 8 + align8(big_sz) as u64;
            // Room for 3 bigs + 16 bytes slack (enough for SKIP
            // marker and a bit more).
            let capacity = W2M_HEADER_SIZE as u64 + 3 * big_total + 16;
            let size = capacity as usize;
            let ptr = alloc_region(size);
            init_region_raw(ptr, capacity);
            let hdr = W2mRingHeader::from_raw(ptr);

            // Publish 3 bigs, consume 2. wc = HEADER + 3*big_total,
            // rc = HEADER + 2*big_total. Reader is 1 big behind.
            for _ in 0..3 {
                match try_publish(hdr, ptr, big_sz, |_| {}) {
                    TryPublish::Ok(_) => {}
                    TryPublish::Full => panic!("initial big publish"),
                }
            }
            let mut rc = W2M_HEADER_SIZE as u64;
            for _ in 0..2 {
                let (_, _, new_rc) = try_consume(hdr, ptr, rc).expect("consume");
                hdr.advance_read_cursors(new_rc);
                rc = new_rc;
            }

            // Now publish a 4th big. wc + total > cap (would be
            // HEADER + 4*big_total > HEADER + 3*big_total + 16).
            // wrap_end = HEADER + big_total < rc = HEADER + 2*big_total.
            // SKIP wrap must fire.
            match try_publish(hdr, ptr, big_sz, |slot| { slot[0] = 0xDE; }) {
                TryPublish::Ok(_) => {}
                TryPublish::Full => panic!("SKIP wrap must succeed"),
            }
            assert_eq!(
                hdr.writer_wrap_count(), 1,
                "one SKIP wrap should have happened",
            );

            // Consumer reads the 3rd pre-wrap big, then jumps via
            // SKIP to the wrapped big.
            let (_, sz_a, new_rc) = try_consume(hdr, ptr, rc)
                .expect("pre-SKIP big");
            assert_eq!(sz_a, big_sz as u32);
            hdr.advance_read_cursors(new_rc);

            let (_, sz_b, _) = try_consume(hdr, ptr, new_rc)
                .expect("wrapped big via SKIP");
            assert_eq!(sz_b, big_sz as u32);

            free_region(ptr, size);
        }
    }

    /// With cap exactly large enough for 2 messages and the reader at
    /// HEADER, the third publish (no room to wrap without lapping)
    /// returns Full — demonstrating backpressure.
    #[test]
    fn test_w2m_full_blocks_writer() {
        unsafe {
            let msg_sz = 1 << 16; // 64 KiB
            let msg_total = 8 + align8(msg_sz) as u64;
            let capacity = W2M_HEADER_SIZE as u64 + 2 * msg_total + 8;
            let size = capacity as usize;
            let ptr = alloc_region(size);
            init_region_raw(ptr, capacity);
            let hdr = W2mRingHeader::from_raw(ptr);

            for _ in 0..2 {
                match try_publish(hdr, ptr, msg_sz, |_| {}) {
                    TryPublish::Ok(_) => {}
                    TryPublish::Full => panic!("fill publish"),
                }
            }
            // Reader is still at HEADER. Third publish: contiguous
            // fails (wc + total > cap), wrap fails (wrap_end >= rc),
            // reset fails (wc != rc, ring is full, not empty). Full.
            match try_publish(hdr, ptr, msg_sz, |_| {}) {
                TryPublish::Full => {}
                TryPublish::Ok(_) => panic!("undrained ring must return Full"),
            }

            free_region(ptr, size);
        }
    }

    /// Publishing with `sz > MAX_W2M_MSG` is rejected. Models a buggy caller.
    #[test]
    fn test_w2m_oversized_publish_rejected() {
        unsafe {
            let size = region_size_for_tests();
            let ptr = alloc_region(size);
            init_region(ptr, size as u64);
            let hdr = W2mRingHeader::from_raw(ptr);

            match try_publish(hdr, ptr, (MAX_W2M_MSG + 1) as usize, |_| {}) {
                TryPublish::Full => {}
                TryPublish::Ok(_) => panic!("oversized publish must be rejected"),
            }

            free_region(ptr, size);
        }
    }

    /// The `&[u8]` handed back by `try_consume` must point into the mmap
    /// region — zero-copy into `decode_wire`. Verified by address
    /// arithmetic.
    #[test]
    fn test_w2m_decode_wire_zero_copy() {
        unsafe {
            let size = region_size_for_tests();
            let ptr = alloc_region(size);
            init_region(ptr, size as u64);
            let hdr = W2mRingHeader::from_raw(ptr);

            let payload = [0xCDu8; 256];
            match try_publish(hdr, ptr, payload.len(), |slot| {
                slot.copy_from_slice(&payload);
            }) {
                TryPublish::Ok(_) => {}
                TryPublish::Full => panic!("unexpected Full"),
            }

            let (data_ptr, _sz, _) = try_consume(hdr, ptr, W2M_HEADER_SIZE as u64)
                .expect("message must be visible");
            let expected = ptr.add(W2M_HEADER_SIZE + 8) as *const u8;
            assert_eq!(data_ptr, expected,
                "data_ptr must be mmap-resident (zero copy)");

            free_region(ptr, size);
        }
    }

    /// Many publish+consume cycles with the reader lagging by one
    /// message. Forces the writer to repeatedly approach `capacity`
    /// and wrap; at least one SKIP wrap must be counted.
    #[test]
    fn test_w2m_repeated_wraps() {
        unsafe {
            let msg_sz = 1 << 16; // 64 KiB
            let msg_total = 8 + align8(msg_sz) as u64;
            // Room for 3 messages + SKIP slack.
            let capacity = W2M_HEADER_SIZE as u64 + 3 * msg_total + 16;
            let size = capacity as usize;
            let ptr = alloc_region(size);
            init_region_raw(ptr, capacity);
            let hdr = W2mRingHeader::from_raw(ptr);

            // Prime with 2 publishes so rc trails wc by 1 thereafter.
            for _ in 0..2 {
                match try_publish(hdr, ptr, msg_sz, |_| {}) {
                    TryPublish::Ok(_) => {}
                    TryPublish::Full => panic!("prime publish"),
                }
            }
            let mut rc = W2M_HEADER_SIZE as u64;

            // 20 cycles is more than enough to exceed 3 * msg_total
            // of cumulative publish bytes and force at least one wrap.
            for _ in 0..20 {
                // Consume, then publish, keeping rc 1-behind.
                let (_, _sz, new_rc) = try_consume(hdr, ptr, rc)
                    .expect("cycle consume");
                hdr.advance_read_cursors(new_rc);
                rc = new_rc;
                match try_publish(hdr, ptr, msg_sz, |_| {}) {
                    TryPublish::Ok(_) => {}
                    TryPublish::Full => panic!("cycle publish"),
                }
            }
            // Drain.
            loop {
                match try_consume(hdr, ptr, rc) {
                    Some((_, _, new_rc)) => {
                        hdr.advance_read_cursors(new_rc);
                        rc = new_rc;
                    }
                    None => break,
                }
            }
            assert!(
                hdr.writer_wrap_count() >= 1,
                "at least one SKIP wrap must have occurred (got {})",
                hdr.writer_wrap_count(),
            );

            free_region(ptr, size);
        }
    }

    /// Local alias to the pub(crate) test helper.
    unsafe fn init_region_raw(ptr: *mut u8, capacity: u64) {
        super::init_region_for_tests(ptr, capacity);
    }

    /// Regression: deterministic sequence that lets the writer cross
    /// the reader after a SKIP wrap. With the buggy physical-cursor
    /// `wc >= rc || wc + total <= rc` predicate, publish #9 lands on
    /// the slot the reader hasn't drained yet (msg #4), silently
    /// overwriting it. With the virtual-cursor fix, publish #9 must
    /// either succeed without trampling unread data or return Full.
    ///
    /// Cap = HEADER + 5*total + 16 chosen so the trace from the plan
    /// reproduces exactly: publish 4, consume 3, publish 5..=9 with
    /// the writer hitting rc precisely on publish #9.
    #[test]
    fn test_writer_does_not_cross_reader_after_wrap() {
        unsafe {
            let msg_sz: usize = 64;
            let msg_total = (8 + align8(msg_sz)) as u64;
            let capacity = W2M_HEADER_SIZE as u64 + 5 * msg_total + 16;
            let size = 4096; // one page is plenty for 600-ish bytes
            let ptr = alloc_region(size);
            init_region_raw(ptr, capacity);
            let hdr = W2mRingHeader::from_raw(ptr);

            let publish = |tag: u8| {
                match try_publish(hdr, ptr, msg_sz, |slot| {
                    slot[0] = tag;
                    for b in &mut slot[1..] { *b = tag; }
                }) {
                    TryPublish::Ok(_) => true,
                    TryPublish::Full => false,
                }
            };

            // 4 contiguous publishes.
            for tag in 1u8..=4 { assert!(publish(tag), "publish #{}", tag); }

            // Consume 3, leaving msg #4 unread at rc = HEADER + 3*total.
            let mut rc = hdr.read_cursor().load(Ordering::Acquire);
            let mut received = Vec::new();
            for _ in 0..3 {
                let (data_ptr, sz, new_rc) = try_consume(hdr, ptr, rc)
                    .expect("consume");
                assert_eq!(sz as usize, msg_sz);
                received.push(*data_ptr);
                hdr.advance_read_cursors(new_rc);
                rc = new_rc;
            }
            assert_eq!(received, vec![1, 2, 3]);

            // 5th fits contiguously. 6th forces SKIP wrap. 7th, 8th
            // chase the reader. 9th exposes the bug — with the old
            // ring it overwrites msg #4's slot.
            for tag in 5u8..=9 {
                if !publish(tag) {
                    // Acceptable post-fix outcome: the ring correctly
                    // refuses the publish. Drain to make room.
                    let (data_ptr, sz, new_rc) = try_consume(hdr, ptr, rc)
                        .expect("drain to make room");
                    assert_eq!(sz as usize, msg_sz);
                    received.push(*data_ptr);
                    hdr.advance_read_cursors(new_rc);
                    rc = new_rc;
                    assert!(publish(tag),
                        "publish #{} after drain", tag);
                }
            }

            // Drain remaining and assert every tag arrived in order.
            loop {
                match try_consume(hdr, ptr, rc) {
                    Some((data_ptr, sz, new_rc)) => {
                        assert_eq!(sz as usize, msg_sz);
                        received.push(*data_ptr);
                        hdr.advance_read_cursors(new_rc);
                        rc = new_rc;
                    }
                    None => break,
                }
            }
            assert_eq!(
                received,
                vec![1, 2, 3, 4, 5, 6, 7, 8, 9],
                "writer must not overwrite unread data after wrap",
            );

            free_region(ptr, size);
        }
    }

    /// `init_region` must reject non-8-aligned capacity: the SKIP-wrap
    /// logic writes an 8-byte sentinel and a size prefix at `phys(vwc)`,
    /// which can cross the end of the buffer when `cap` is not aligned.
    #[test]
    #[should_panic]
    fn init_region_rejects_unaligned_capacity() {
        unsafe {
            // 7 bytes past the minimum valid capacity → not 8-aligned.
            let cap = (2 * MAX_W2M_MSG) as usize + W2M_HEADER_SIZE + 16 + 7;
            let ptr = alloc_region(cap);
            init_region(ptr, cap as u64); // must panic
            free_region(ptr, cap);
        }
    }

    /// `init_region_for_tests` must also reject non-8-aligned capacity.
    #[test]
    #[should_panic]
    fn init_region_for_tests_rejects_unaligned_capacity() {
        unsafe {
            // Smallest unaligned capacity that satisfies the size lower bound.
            let cap = W2M_HEADER_SIZE + 16 + 1; // +1 makes it unaligned
            let ptr = alloc_region(cap);
            init_region_for_tests(ptr, cap as u64); // must panic
            free_region(ptr, cap);
        }
    }

    /// `consume_one` helper — exercises it just to silence dead-code.
    #[test]
    fn test_w2m_consume_one_helper() {
        unsafe {
            let size = region_size_for_tests();
            let ptr = alloc_region(size);
            init_region(ptr, size as u64);
            let hdr = W2mRingHeader::from_raw(ptr);
            match try_publish(hdr, ptr, 32, |slot| { slot.fill(0x77); }) {
                TryPublish::Ok(_) => {}
                TryPublish::Full => panic!(),
            }
            let (p, sz) = consume_one(hdr, ptr).expect("msg");
            assert_eq!(sz, 32);
            let data = std::slice::from_raw_parts(p, 32);
            assert!(data.iter().all(|&b| b == 0x77));
            free_region(ptr, size);
        }
    }

    /// Verifies that advancing `consume_cursor` unblocks `try_reserve` backpressure.
    #[test]
    fn test_consume_cursor_tracks_read_cursor() {
        unsafe {
            let msg_sz = 1 << 16; // 64 KiB
            let msg_total = (8 + align8(msg_sz)) as u64;
            let capacity = W2M_HEADER_SIZE as u64 + 2 * msg_total + 8;
            let size = capacity as usize;
            let ptr = alloc_region(size);
            init_region_raw(ptr, capacity);
            let hdr = W2mRingHeader::from_raw(ptr);

            for _ in 0..2 {
                match try_publish(hdr, ptr, msg_sz, |_| {}) {
                    TryPublish::Ok(_) => {}
                    TryPublish::Full => panic!("fill publish"),
                }
            }
            match try_publish(hdr, ptr, msg_sz, |_| {}) {
                TryPublish::Full => {}
                TryPublish::Ok(_) => panic!("ring must be Full"),
            }

            assert_eq!(hdr.consume_cursor().load(Ordering::Acquire), W2M_HEADER_SIZE as u64);
            assert_eq!(hdr.read_cursor().load(Ordering::Acquire), W2M_HEADER_SIZE as u64);

            let cursor = hdr.read_cursor().load(Ordering::Acquire);
            let (_, _, new_rc) = try_consume(hdr, ptr, cursor).expect("must have message");
            hdr.advance_read_cursors(new_rc);

            assert_eq!(hdr.consume_cursor().load(Ordering::Acquire), W2M_HEADER_SIZE as u64 + msg_total);
            assert_eq!(hdr.read_cursor().load(Ordering::Acquire), W2M_HEADER_SIZE as u64 + msg_total);

            match try_publish(hdr, ptr, msg_sz, |_| {}) {
                TryPublish::Ok(_) => {}
                TryPublish::Full => panic!("must have room after consume_cursor advance"),
            }

            free_region(ptr, size);
        }
    }
}
