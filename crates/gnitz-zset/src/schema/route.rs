//! Worker routing: which worker owns a key, for a cluster of a given size.

use gnitz_wire::{widen_pk_be, NARROW_PK_MAX_BYTES};

/// Map a 64-bit hash onto `0..num_workers` by multiply-shift.
#[inline(always)]
fn bucket(h: u64, num_workers: usize) -> usize {
    debug_assert!(num_workers >= 1, "worker routing: num_workers must be >= 1");
    ((h as u128 * num_workers as u128) >> 64) as usize
}

/// Which worker this process is, of how many: the one input that decides which
/// `w{k}of{n}` child every store of this process opens.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Slot {
    pub rank: u32,
    pub of: u32,
}

impl Slot {
    /// A one-worker process: the mirror, and every unit test.
    pub const SOLO: Slot = Slot { rank: 0, of: 1 };

    /// Panics unless `rank < of`.
    pub fn new(rank: u32, of: u32) -> Slot {
        assert!(rank < of, "slot {rank} of {of}");
        Slot { rank, of }
    }
}

/// Which of `num_workers` workers owns `key`.
#[inline(always)]
pub(crate) fn worker_for_key(pk: u128, num_workers: usize) -> usize {
    let lo = pk as u64;
    let hi = (pk >> 64) as u64;
    bucket(
        lo.wrapping_mul(0x9e3779b97f4a7c15_u64) ^ hi.wrapping_mul(0x6c62272e07bb0142_u64),
        num_workers,
    )
}

/// Which of `num_workers` workers owns an OPK PK region: a narrow one by its
/// `u128` image, a wide one by its hash.
#[inline(always)]
pub fn worker_for_pk_bytes(bytes: &[u8], num_workers: usize) -> usize {
    if bytes.len() <= NARROW_PK_MAX_BYTES {
        worker_for_key(widen_pk_be(bytes), num_workers)
    } else {
        worker_for_wide_pk(bytes, num_workers)
    }
}

#[inline(never)]
fn worker_for_wide_pk(bytes: &[u8], num_workers: usize) -> usize {
    bucket(gnitz_wire::checksum(bytes), num_workers)
}

/// The worker that owns a global (group-less) aggregate's ground row: where the
/// router sends a row keyed by no columns.
pub fn ground_owner(num_workers: usize) -> usize {
    worker_for_key(gnitz_wire::global_group_key(), num_workers)
}

#[cfg(test)]
#[path = "tests/route.rs"]
mod tests;
