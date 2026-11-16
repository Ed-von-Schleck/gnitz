use super::*;
use gnitz_foundation::rng::Rng;
use xorf::Filter;

/// A `u128` PK through the real key derivation, as its OPK bytes.
fn key_u128(k: u128) -> u64 {
    crate::schema::probe_key(&k.to_be_bytes())
}

/// Through its region, no key set produces a false negative, and
/// absent keys pass under 1% of the time (nominal ~0.4%).
#[test]
fn no_key_set_produces_a_false_negative_through_its_region() {
    let mut rng = Rng::new(0xF11E_5EED);
    let absent: Vec<u128> = (0..100_000).map(|_| rng.gen_u128()).collect();
    for (what, pks) in [
        ("dense", (0u128..1000).collect::<Vec<_>>()),
        ("two keys", vec![100, 200]),
        (
            "across the u64 boundary",
            vec![0, 1, u64::MAX as u128, (u64::MAX as u128) + 1, u128::MAX],
        ),
        (
            "200k random, a wider segment geometry",
            (0..200_000).map(|_| rng.gen_u128()).collect(),
        ),
    ] {
        let region = build(&pks.iter().flat_map(|k| k.to_be_bytes()).collect::<Vec<u8>>(), 16).unwrap();
        let filter = parse(&region).unwrap_or_else(|| panic!("{what}: a freshly built filter parses back"));
        for &pk in &pks {
            assert!(filter.contains(&key_u128(pk)), "{what}: {pk:#034x}");
        }
        let fp = absent.iter().filter(|&&k| filter.contains(&key_u128(k))).count();
        assert!(fp * 100 < absent.len(), "{what}: false-positive rate above 1%: {fp}");
    }
}

#[test]
fn a_region_shorter_than_the_descriptor_is_invalid() {
    assert!(parse(&[]).is_none());
    assert!(parse(&[0u8; Descriptor::DMA_LEN - 1]).is_none());
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
    assert!(validate(&ok, 16), "the well-formed descriptor is admitted");

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
        assert!(!validate(&d, len), "{name} must be rejected");
    }
}

/// A key no wider than [`PROBE_KEY_INJECTIVE_BYTES`] skips the sort and deduplication,
/// and its region is byte for byte the one its sorted, deduplicated
/// fingerprints build.
#[test]
fn a_narrow_key_builds_the_filter_its_sorted_fingerprints_build() {
    let sorted_region = |pk_region: &[u8], stride: usize| {
        let mut keys: Vec<u64> = pk_region.chunks_exact(stride).map(crate::schema::probe_key).collect();
        keys.sort_unstable();
        keys.dedup();
        let filter = BinaryFuse8::try_from(keys.as_slice()).unwrap();
        let mut buf = vec![0u8; Descriptor::DMA_LEN + filter.fingerprints.len()];
        filter.dma_copy_descriptor_to(&mut buf[..Descriptor::DMA_LEN]);
        buf[Descriptor::DMA_LEN..].copy_from_slice(filter.dma_fingerprints());
        buf
    };
    let mut rng = Rng::new(0x5_0471);
    for stride in 1..=PROBE_KEY_INJECTIVE_BYTES {
        let span = 1u64.checked_shl(8 * stride as u32).unwrap_or(0).wrapping_sub(1);
        for n in [1usize, 2, 3, 100, 70_000] {
            // Ascending, each key repeated by up to three rows.
            let mut pks: Vec<u64> = (0..n).map(|_| rng.next_u64() & span).collect();
            pks.extend(pks.clone().iter().step_by(3));
            pks.sort_unstable();
            let region: Vec<u8> = pks
                .iter()
                .flat_map(|k| k.to_be_bytes()[8 - stride..].to_vec())
                .collect();
            assert_eq!(
                build(&region, stride).unwrap(),
                sorted_region(&region, stride),
                "stride {stride}, {n} keys"
            );
        }
    }
}
