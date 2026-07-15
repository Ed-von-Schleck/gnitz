//! Little-endian byte codec: fixed-width integer pack/unpack (aligned and
//! unaligned) and 8-byte alignment. The genuine cross-layer leaf — every
//! wire/shard/mmap path reads and writes through it.
//!
//! `align8` and the aligned `read_/write_u{32,64}_le` primitives are owned by
//! `gnitz_wire` (the crate that defines the wire format, and whose WAL framer
//! needs them) and re-exported here; the engine's 15+ `codec::read_u32_le`
//! callers are unchanged.

pub use gnitz_wire::{align8, read_u32_le, read_u64_le, write_u64_le};

#[inline]
pub fn read_i64_le(buf: &[u8], off: usize) -> i64 {
    i64::from_le_bytes(buf[off..off + 8].try_into().unwrap())
}

/// Reinterpret a `&[T]` of a fixed-width primitive (`i64`/`u64`/`u128`/…) as its
/// raw little-endian bytes. On the LE target the native layout *is* the shard /
/// wire byte layout, so this is the zero-copy way to hand a typed column to the
/// byte-oriented region APIs (`Batch::region_slice`, `write_shard_streaming`)
/// without a per-callsite pointer cast and `len * width` arithmetic. Only the
/// shard-writer tests need it — production region bytes are already `&[u8]`.
#[cfg(test)]
pub(crate) fn as_le_bytes<T: Copy>(vals: &[T]) -> &[u8] {
    // SAFETY: `T` is a fixed-width primitive with no padding, so its bytes are
    // fully initialised; the view spans exactly `size_of_val(vals)` bytes and so
    // never reads past `vals`' allocation.
    unsafe { std::slice::from_raw_parts(vals.as_ptr() as *const u8, std::mem::size_of_val(vals)) }
}

// The four `*_raw` accessors below do unaligned `u32`/`u64` reads and writes at
// `base + offset` bytes for the SAL and W2M mmap paths, where the offset is
// computed from a `*mut u8` base pointer that need not meet the alignment a
// `*mut u{32,64}` dereference requires — hence `read_unaligned`/`write_unaligned`.
// Each `# Safety` clause is the same contract: `base + offset + N` must lie
// inside a live allocation, writable for the writes and readable for the reads.

/// # Safety
/// `base + offset + 8` must lie inside a live, writable allocation.
#[inline]
pub(crate) unsafe fn write_u64_raw(base: *mut u8, offset: usize, val: u64) {
    (base.add(offset) as *mut u64).write_unaligned(val);
}

/// # Safety
/// `base + offset + 8` must lie inside a live, readable allocation.
#[inline]
pub(crate) unsafe fn read_u64_raw(base: *const u8, offset: usize) -> u64 {
    (base.add(offset) as *const u64).read_unaligned()
}

/// # Safety
/// `base + offset + 4` must lie inside a live, writable allocation.
#[inline]
pub(crate) unsafe fn write_u32_raw(base: *mut u8, offset: usize, val: u32) {
    (base.add(offset) as *mut u32).write_unaligned(val);
}

/// # Safety
/// `base + offset + 4` must lie inside a live, readable allocation.
#[inline]
pub(crate) unsafe fn read_u32_raw(base: *const u8, offset: usize) -> u32 {
    (base.add(offset) as *const u32).read_unaligned()
}
