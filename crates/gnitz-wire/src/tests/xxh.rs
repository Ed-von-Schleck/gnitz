use super::*;

/// Shard, manifest and SAL header digests and the global group key are
/// persisted, so they are only stable while XXH3's output is. Reference values
/// from python-xxhash.
#[test]
fn xxh3_matches_the_reference_vectors() {
    let ramp: Vec<u8> = (0..=255u8).collect();
    assert_eq!(checksum(b""), 0x2D06_8005_38D3_94C2);
    assert_eq!(checksum(&ramp), 0x9408_A443_3B95_2D71);
    assert_eq!(global_group_key(), 0x99AA_06D3_0147_98D8_6001_C324_468D_497F);
    assert_eq!(checksum_128(&ramp), 0xF1F8_A93F_5084_9AC3_9408_A443_3B95_2D71);
}
