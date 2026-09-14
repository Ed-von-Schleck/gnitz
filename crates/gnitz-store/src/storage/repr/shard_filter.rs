//! The per-shard PK membership filter: a [`BinaryFuse8`] over
//! [`crate::schema::key::probe_key`] fingerprints, and the on-disk region it is
//! written to and read back from.
//!
//! The region is `[descriptor: DMA_LEN][fingerprints]` — the byte pair
//! [`DmaSerializable`] exists to produce. `Descriptor::DMA_LEN` is a
//! compile-time constant, so the split point needs no framing; the shard
//! header's `OFF_SHARD_FILTER_OFFSET` / `_SIZE` pair bounds the region.
//!
//! Reading is zero-copy: [`ShardFilter`] keeps the descriptor bytes and the
//! fingerprints' file offset, and probes through [`BinaryFuse8Ref`] against the
//! shard's own mmap — the same `(offset, stride)` addressing every other region
//! of the file uses. Only the write side owns a [`BinaryFuse8`].
//!
//! The crate parses a descriptor without validating it, and `contains` indexes
//! the fingerprints from descriptor-derived values, so a corrupt descriptor
//! panics rather than answering wrongly. [`ShardFilter::parse`] therefore puts
//! [`validate`] between the region and the first probe.

use gnitz_wire::{read_u32_le, read_u64_le};
use xorf::{BinaryFuse8, BinaryFuse8Ref, Descriptor, DmaSerializable, Filter, FilterRef};

/// Build from probe keys. Sorted and deduplicated first: `probe_key` is a
/// 64-bit digest, so distinct PKs collide, and the caller's
/// adjacent-byte-equal pre-shrink cannot see it. The construction requires
/// distinct keys and asserts it under `debug_assertions`.
///
/// `None` on a construction failure, which writes the shard filterless: a
/// permanent read-cost regression for that one file, and a repeated one means
/// the key derivation has broken, hence the warning here rather than a silent
/// `None` at the call site. Do not add a caller-side retry — the crate already
/// reseeds up to 1000 times internally, each iteration O(n), and a second
/// unbounded loop would sit on the compaction path.
pub(crate) fn build(mut keys: Vec<u64>) -> Option<BinaryFuse8> {
    keys.sort_unstable();
    keys.dedup();
    match BinaryFuse8::try_from(keys.as_slice()) {
        Ok(f) => Some(f),
        Err(e) => {
            gnitz_warn!("shard PK filter construction failed over {} keys: {e}", keys.len());
            None
        }
    }
}

/// Serialize a filter to its region bytes: `[descriptor][fingerprints]`.
pub(crate) fn serialize(filter: &BinaryFuse8) -> Vec<u8> {
    let mut buf = vec![0u8; Descriptor::DMA_LEN + filter.fingerprints.len()];
    filter.dma_copy_descriptor_to(&mut buf[..Descriptor::DMA_LEN]);
    buf[Descriptor::DMA_LEN..].copy_from_slice(filter.dma_fingerprints());
    buf
}

/// The properties every descriptor the construction emits satisfies, and the
/// only ones a probe needs to stay in bounds.
///
/// `hash_of_hash` derives `h0 = (hash · scl) >> 64 < scl`, then `h1 = h0 + sl`
/// and `h2 = h0 + 2·sl`, each XORed with the low log2(sl) bits, which cannot
/// leave the sl-aligned block they land in; with `scl` a multiple of `sl` the
/// largest reachable index is `scl + 2·sl - 1`, which the length rule pins to
/// `fingerprints_len - 1`. Three rules carry their own weight beyond that
/// argument, so each is noted where it sits.
fn validate(d: &Descriptor, fingerprints_len: usize) -> Option<()> {
    // u64 throughout: `scl + 2 * sl` in u32 wraps, and a forged descriptor
    // would then pass the length rule.
    let sl = d.segment_length as u64;
    let scl = d.segment_count_length as u64;
    (sl.is_power_of_two()
        && d.segment_length_mask as u64 == sl - 1
        // At scl = 0 every h0 is 0, so h2 reaches 3·sl - 1 against a length of
        // 2·sl. The alignment rule below admits 0.
        && scl != 0
        // `{sl:4, mask:3, scl:5}` over 13 fingerprint bytes satisfies every
        // other rule and still indexes one past the end.
        && scl.is_multiple_of(sl)
        && fingerprints_len as u64 == scl + 2 * sl
        // `hash_of_hash` sums h1 and h2 in u32, so a length at or above 2^32
        // overflows: a debug panic, a wrong in-bounds index in release.
        && fingerprints_len < u32::MAX as usize)
        .then_some(())
}

/// A shard's PK filter as it sits in the file: the descriptor bytes, and where
/// the fingerprints start. Probing borrows them from the shard's mmap, so an
/// open costs no copy of an array that grows with the shard's key count.
pub(crate) struct ShardFilter {
    descriptor: [u8; Descriptor::DMA_LEN],
    fingerprints_off: usize,
    fingerprints_len: usize,
}

impl ShardFilter {
    /// Parse the filter region at `region_off` in the file. `None` when the
    /// region is structurally invalid; the caller fails the open on it.
    pub(crate) fn parse(region: &[u8], region_off: usize) -> Option<ShardFilter> {
        let d: [u8; Descriptor::DMA_LEN] = region.get(..Descriptor::DMA_LEN)?.try_into().ok()?;
        // Field order is the LE layout the write side's `dma_copy_descriptor_to`
        // produces and `BinaryFuse8Ref::from_dma` reads back;
        // `a_filter_round_trips_through_its_region` pins it.
        validate(
            &Descriptor {
                seed: read_u64_le(&d, 0),
                segment_length: read_u32_le(&d, 8),
                segment_length_mask: read_u32_le(&d, 12),
                segment_count_length: read_u32_le(&d, 16),
            },
            region.len() - Descriptor::DMA_LEN,
        )?;
        Some(ShardFilter {
            descriptor: d,
            fingerprints_off: region_off + Descriptor::DMA_LEN,
            fingerprints_len: region.len() - Descriptor::DMA_LEN,
        })
    }

    /// Whether a [`crate::schema::key::probe_key`] may be present. `file` is the
    /// shard image [`parse`](Self::parse) read its region from.
    pub(crate) fn may_contain(&self, file: &[u8], probe_key: u64) -> bool {
        let fingerprints = &file[self.fingerprints_off..self.fingerprints_off + self.fingerprints_len];
        BinaryFuse8Ref::from_dma(&self.descriptor, fingerprints).contains(&probe_key)
    }
}

#[cfg(test)]
#[path = "tests/shard_filter.rs"]
mod tests;
