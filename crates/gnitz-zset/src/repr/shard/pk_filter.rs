//! The per-shard PK membership filter: a [`BinaryFuse8`] over
//! [`crate::schema::probe_key`] fingerprints, and the on-disk region it is
//! written to and read back from.
//!
//! The region is `[descriptor: DMA_LEN][fingerprints]` — the byte pair
//! [`DmaSerializable`] exists to produce. `Descriptor::DMA_LEN` is a
//! compile-time constant, so the split point needs no framing; the shard's
//! trailing directory entry bounds the region.
//!
//! Reading is zero-copy: a probe reads the region in place through
//! [`BinaryFuse8Ref`]. Only the write side owns a [`BinaryFuse8`].
//!
//! The crate parses a descriptor without validating it, and `contains` indexes
//! the fingerprints from descriptor-derived values, so a corrupt descriptor
//! panics rather than answering wrongly. [`parse`] therefore puts
//! [`validate`] between the region and any [`BinaryFuse8Ref`].

use gnitz_wire::{read_u32_le, read_u64_le};
use xorf::{BinaryFuse8, BinaryFuse8Ref, Descriptor, DmaSerializable, FilterRef};

/// Build the filter over a PK-sorted region of `stride`-byte keys, as its region
/// bytes `[descriptor][fingerprints]`. Rows sharing a PK are adjacent, so
/// skipping a key byte-equal to its predecessor bounds the sort at the distinct
/// PKs. The keys are deduplicated again after hashing: `probe_key` is a 64-bit
/// digest, so distinct PKs collide, and the construction requires distinct keys
/// (asserted under `debug_assertions`).
///
/// `None` on a construction failure, which writes the shard filterless — a
/// permanent read-cost regression for that one file — and logs a warning: a
/// repeated one means the key derivation has broken.
pub(crate) fn build(pk_region: &[u8], stride: usize) -> Option<Vec<u8>> {
    let mut keys: Vec<u64> = Vec::with_capacity(pk_region.len() / stride);
    let mut prev: Option<&[u8]> = None;
    for chunk in pk_region.chunks_exact(stride) {
        if prev == Some(chunk) {
            continue;
        }
        prev = Some(chunk);
        keys.push(crate::schema::probe_key(chunk));
    }
    keys.sort_unstable();
    keys.dedup();
    let filter = match BinaryFuse8::try_from(keys.as_slice()) {
        Ok(f) => f,
        Err(e) => {
            gnitz_warn!("shard PK filter construction failed over {} keys: {e}", keys.len());
            return None;
        }
    };
    let mut buf = vec![0u8; Descriptor::DMA_LEN + filter.fingerprints.len()];
    filter.dma_copy_descriptor_to(&mut buf[..Descriptor::DMA_LEN]);
    buf[Descriptor::DMA_LEN..].copy_from_slice(filter.dma_fingerprints());
    Some(buf)
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
fn validate(d: &Descriptor, fingerprints_len: usize) -> bool {
    // u64 throughout: `scl + 2 * sl` in u32 wraps, and a forged descriptor
    // would then pass the length rule.
    let sl = d.segment_length as u64;
    let scl = d.segment_count_length as u64;
    sl.is_power_of_two()
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
        && fingerprints_len < u32::MAX as usize
}

/// `region` as a filter a probe can read, or `None` where its descriptor
/// would index out of bounds.
pub(crate) fn parse(region: &[u8]) -> Option<BinaryFuse8Ref<'_>> {
    let (d, fingerprints) = region.split_at_checked(Descriptor::DMA_LEN)?;
    // Field order is the LE layout the write side's `dma_copy_descriptor_to`
    // produces and `BinaryFuse8Ref::from_dma` reads back;
    // `no_key_set_produces_a_false_negative_through_its_region` pins it.
    validate(
        &Descriptor {
            seed: read_u64_le(d, 0),
            segment_length: read_u32_le(d, 8),
            segment_length_mask: read_u32_le(d, 12),
            segment_count_length: read_u32_le(d, 16),
        },
        fingerprints.len(),
    )
    .then(|| BinaryFuse8Ref::from_dma(d, fingerprints))
}

#[cfg(test)]
#[path = "tests/pk_filter.rs"]
mod tests;
