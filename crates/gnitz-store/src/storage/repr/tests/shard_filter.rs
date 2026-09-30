use super::*;
use crate::test_rng::Rng;

/// A `u128` PK through the real key derivation, as its OPK bytes.
fn key_u128(k: u128) -> u64 {
    crate::schema::key::probe_key(&k.to_be_bytes())
}

/// Through its serialized region, no key set produces a false negative, and
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
        let region = serialize(&build(pks.iter().copied().map(key_u128).collect()).unwrap());
        assert!(is_valid(&region), "{what}: a freshly built filter parses back");
        for &pk in &pks {
            assert!(may_contain(&region, key_u128(pk)), "{what}: {pk:#034x}");
        }
        let fp = absent.iter().filter(|&&k| may_contain(&region, key_u128(k))).count();
        assert!(fp * 100 < absent.len(), "{what}: false-positive rate above 1%: {fp}");
    }
}

#[test]
fn a_region_shorter_than_the_descriptor_is_invalid() {
    assert!(!is_valid(&[]));
    assert!(!is_valid(&[0u8; Descriptor::DMA_LEN - 1]));
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
