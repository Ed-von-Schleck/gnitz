use super::*;

/// The digest ignores its own eight bytes and the path's directory, and moves
/// with every other byte of the prefix and with the basename — across the
/// 240-byte input size where XXH3 switches algorithms.
#[test]
fn desc_digest_covers_the_prefix_but_its_own_slot() {
    let buf: Vec<u8> = (0..600u32).map(|i| (i * 31 + 7) as u8).collect();
    let hole = OFF_DESC_CHECKSUM..OFF_DESC_CHECKSUM + 8;
    for len in [OFF_DESC_CHECKSUM + 8, 64, 239, 240, 241, 600] {
        let prefix = &buf[..len];
        let base = desc_digest("shard_7_1.db", prefix);
        assert_eq!(desc_digest("data/t/shard_7_1.db", prefix), base, "len={len}: directory");
        assert_ne!(desc_digest("shard_7_2.db", prefix), base, "len={len}: basename");
        for at in 0..len {
            let mut flipped = prefix.to_vec();
            flipped[at] ^= 1;
            let moved = desc_digest("shard_7_1.db", &flipped) != base;
            assert_eq!(moved, !hole.contains(&at), "len={len}: byte {at}");
        }
    }
}
