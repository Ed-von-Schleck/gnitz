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
/// 64-bit digest of a 128-bit value, so distinct PKs collide, and the caller's
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
mod tests {
    use super::*;

    /// A `u128` PK through the real key derivation, as its OPK bytes.
    fn key_u128(k: u128) -> u64 {
        crate::schema::key::probe_key(&k.to_be_bytes())
    }

    fn build_u128(pks: &[u128]) -> Option<BinaryFuse8> {
        build(pks.iter().copied().map(key_u128).collect())
    }

    /// A built filter as the reader sees it: its region bytes, and the
    /// [`ShardFilter`] parsed back out of them.
    fn through_the_region(pks: &[u128]) -> (Vec<u8>, ShardFilter) {
        let region = serialize(&build_u128(pks).unwrap());
        let filter = ShardFilter::parse(&region, 0).expect("a freshly built filter parses back");
        (region, filter)
    }

    #[test]
    fn build_and_query_no_false_negatives() {
        let pks: Vec<u128> = (0u128..1000).collect();
        let (region, filter) = through_the_region(&pks);
        for &pk in &pks {
            assert!(filter.may_contain(&region, key_u128(pk)), "false negative for key {pk}");
        }
    }

    #[test]
    fn false_positive_rate() {
        let pks: Vec<u128> = (0u128..2000).collect();
        let (region, filter) = through_the_region(&pks);
        let mut fp = 0u32;
        for i in 10_000u128..20_000 {
            if filter.may_contain(&region, key_u128(i)) {
                fp += 1;
            }
        }
        // BinaryFuse8 theoretical FPR ~0.39%. Allow up to 2%.
        assert!(fp < 200, "FPR too high: {fp}/10000");
    }

    #[test]
    fn small_set() {
        let (region, filter) = through_the_region(&[100u128, 200]);
        assert!(filter.may_contain(&region, key_u128(100)));
        assert!(filter.may_contain(&region, key_u128(200)));
    }

    #[test]
    fn build_crossing_u64_boundary() {
        let pks: [u128; 5] = [0, 1, u64::MAX as u128, (u64::MAX as u128) + 1, u128::MAX];
        let (region, filter) = through_the_region(&pks);
        for &pk in &pks {
            assert!(
                filter.may_contain(&region, key_u128(pk)),
                "false negative for pk {pk:#034x}"
            );
        }
    }

    /// The descriptor is written by the dependency's `dma_copy_descriptor_to`,
    /// validated by [`validate`]'s own parse, and read back by
    /// `BinaryFuse8Ref::from_dma`. A field-order change moves those apart, and a
    /// probe over the region is what notices.
    #[test]
    fn a_filter_round_trips_through_its_region() {
        let pks: Vec<u128> = (0u128..500).collect();
        let built = build_u128(&pks).unwrap();
        let region = serialize(&built);
        let filter = ShardFilter::parse(&region, 0).unwrap();
        assert_eq!(filter.fingerprints_len, built.fingerprints.len());
        for &pk in &pks {
            assert!(
                filter.may_contain(&region, key_u128(pk)),
                "roundtrip false negative for key {pk:#034x}",
            );
        }
    }

    /// The fingerprints are addressed by file offset, so a region that does not
    /// start at 0 must probe identically.
    #[test]
    fn a_region_at_a_nonzero_offset_probes_the_same_bytes() {
        let pks: Vec<u128> = (0u128..500).collect();
        let region = serialize(&build_u128(&pks).unwrap());
        let mut file = vec![0xABu8; 137];
        let off = file.len();
        file.extend_from_slice(&region);
        let filter = ShardFilter::parse(&region, off).unwrap();
        for &pk in &pks {
            assert!(
                filter.may_contain(&file, key_u128(pk)),
                "false negative at region offset {off} for key {pk:#034x}",
            );
        }
    }

    #[test]
    fn parse_rejects_a_region_shorter_than_the_descriptor() {
        assert!(ShardFilter::parse(&[], 0).is_none());
        assert!(ShardFilter::parse(&[0u8; Descriptor::DMA_LEN - 1], 0).is_none());
    }

    /// Every rule, one forged descriptor each. `{sl:4, mask:3, scl:5}` is the
    /// case that satisfies all the others and still indexes out of bounds.
    #[test]
    fn validate_rejects_each_way_a_descriptor_can_be_wrong() {
        let ok = Descriptor {
            seed: 0,
            segment_length: 4,
            segment_length_mask: 3,
            segment_count_length: 8,
        };
        assert!(validate(&ok, 16).is_some(), "the well-formed descriptor is admitted");

        let cases: [(&str, Descriptor, usize); 7] = [
            (
                "segment_length not a power of two",
                Descriptor {
                    segment_length: 3,
                    segment_length_mask: 2,
                    ..ok.clone()
                },
                14,
            ),
            (
                "segment_length zero",
                Descriptor {
                    segment_length: 0,
                    segment_length_mask: u32::MAX,
                    ..ok.clone()
                },
                8,
            ),
            (
                "mask disagrees with segment_length",
                Descriptor {
                    segment_length_mask: 7,
                    ..ok.clone()
                },
                16,
            ),
            (
                "segment_count_length zero",
                Descriptor {
                    segment_count_length: 0,
                    ..ok.clone()
                },
                8,
            ),
            (
                "segment_count_length not a multiple of segment_length",
                Descriptor {
                    segment_count_length: 5,
                    ..ok.clone()
                },
                13,
            ),
            ("fingerprint length disagrees", ok.clone(), 17),
            // Reachable only as a number: `scl + 2·sl` at or above 2^32
            // overflows `hash_of_hash`'s u32 arithmetic.
            (
                "fingerprint length at or above 2^32",
                Descriptor {
                    segment_length: 1 << 31,
                    segment_length_mask: (1u32 << 31) - 1,
                    segment_count_length: 1 << 31,
                    ..ok.clone()
                },
                (1usize << 31) + (1usize << 32),
            ),
        ];
        for (name, d, len) in cases {
            assert!(validate(&d, len).is_none(), "{name} must be rejected");
        }
    }
}
