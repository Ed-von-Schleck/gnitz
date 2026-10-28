use super::*;

/// The digest is a checksum over the basename followed by the prefix with its
/// own eight bytes cut out, swept across the 240-byte input size where XXH3
/// switches algorithms.
#[test]
fn desc_digest_is_the_checksum_over_the_spliced_bytes() {
    let buf: Vec<u8> = (0..600u32).map(|i| (i * 31 + 7) as u8).collect();
    for len in [OFF_DESC_CHECKSUM + 8, 64, 239, 240, 241, 600] {
        for (path, basename) in [
            ("shard_7_1.db", "shard_7_1.db"),
            ("data/t/shard_7_1.db", "shard_7_1.db"),
        ] {
            let mut spliced = basename.as_bytes().to_vec();
            spliced.extend_from_slice(&buf[..OFF_DESC_CHECKSUM]);
            spliced.extend_from_slice(&buf[OFF_DESC_CHECKSUM + 8..len]);
            assert_eq!(
                desc_digest(path, &buf[..len]),
                gnitz_wire::checksum(&spliced),
                "len={len} path={path}"
            );
        }
    }
}
