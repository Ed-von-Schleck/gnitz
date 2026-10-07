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

#[test]
fn digest_with_hole_equals_checksum_over_the_spliced_bytes() {
    // The streaming spans must reassemble into exactly seed ++ buf-without-
    // the-hole. Swept across the 240-byte input size where xxh3 switches
    // algorithms, and with the hole at both ends of the buffer.
    let buf: Vec<u8> = (0..600u32).map(|i| (i * 31 + 7) as u8).collect();
    for &len in &[8usize, 64, 239, 240, 241, 600] {
        // Every hole must fit: `digest_with_hole` requires `hole + 8 <= len`.
        for &hole in [0usize, 24, len - 8].iter().filter(|&&h| h + 8 <= len) {
            for seed in [b"".as_slice(), b"shard_7_1.db".as_slice()] {
                let mut spliced = seed.to_vec();
                spliced.extend_from_slice(&buf[..hole]);
                spliced.extend_from_slice(&buf[hole + 8..len]);
                assert_eq!(
                    digest_with_hole(seed, &buf[..len], hole),
                    checksum(&spliced),
                    "len={len} hole={hole} seed={}",
                    seed.len()
                );
            }
        }
    }
}

/// A layout digest moves with the column count, how many columns the PK holds,
/// a type code and the order of two regions.
#[test]
fn layout_digest_separates_every_region_axis() {
    use crate::TypeCode::{String as Str, I64, U64};
    let base = layout_digest(1, [U64, I64]);
    assert_ne!(base, layout_digest(1, [U64, I64, I64]), "column count");
    assert_ne!(base, layout_digest(1, [U64]), "column count");
    assert_ne!(base, layout_digest(2, [U64, I64]), "PK arity");
    assert_ne!(base, layout_digest(1, [U64, Str]), "type code");
    assert_ne!(base, layout_digest(1, [I64, U64]), "region order");
}

#[test]
fn layout_digest_equals_the_streaming_digest_past_its_buffer_too() {
    for regions in [1usize, 5, 65, 70] {
        let types = (0..regions).map(|i| [crate::TypeCode::I64, crate::TypeCode::String, crate::TypeCode::U8][i % 3]);
        let mut h = RowHasher::default();
        h.update(&[2]);
        for tc in types.clone() {
            h.update(&[tc.as_wire()]);
        }
        assert_eq!(layout_digest(2, types), h.digest(), "{regions} regions");
    }
}
