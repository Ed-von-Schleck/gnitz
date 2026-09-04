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

/// No key set may produce a false negative through its serialized region. The
/// descriptor is written by the dependency's `dma_copy_descriptor_to`, validated
/// by [`validate`]'s own parse and read back by `BinaryFuse8Ref::from_dma`, so a
/// field-order change moves those apart and a probe over the region notices.
#[test]
fn no_key_set_produces_a_false_negative_through_its_region() {
    for (what, pks) in [
        ("dense", (0u128..1000).collect::<Vec<_>>()),
        ("two keys", vec![100, 200]),
        (
            "across the u64 boundary",
            vec![0, 1, u64::MAX as u128, (u64::MAX as u128) + 1, u128::MAX],
        ),
    ] {
        let built = build_u128(&pks).unwrap();
        let region = serialize(&built);
        let filter = ShardFilter::parse(&region, 0).expect("a freshly built filter parses back");
        assert_eq!(filter.fingerprints_len, built.fingerprints.len(), "{what}");
        for &pk in &pks {
            assert!(filter.may_contain(&region, key_u128(pk)), "{what}: {pk:#034x}");
        }
    }
}

#[test]
fn false_positive_rate() {
    let pks: Vec<u128> = (0u128..2000).collect();
    let (region, filter) = through_the_region(&pks);
    let fp = (10_000u128..20_000)
        .filter(|&i| filter.may_contain(&region, key_u128(i)))
        .count();
    // BinaryFuse8 theoretical FPR ~0.39%. Allow up to 2%.
    assert!(fp < 200, "FPR too high: {fp}/10000");
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
            Descriptor { segment_length_mask: 7, ..ok.clone() },
            16,
        ),
        (
            "segment_count_length zero",
            Descriptor { segment_count_length: 0, ..ok.clone() },
            8,
        ),
        (
            "segment_count_length not a multiple of segment_length",
            Descriptor { segment_count_length: 5, ..ok.clone() },
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
