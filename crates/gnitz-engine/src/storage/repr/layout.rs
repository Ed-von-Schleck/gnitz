//! Shared shard file format constants, and the digest over the bytes that
//! decide how the rest of a shard is read.

use crate::foundation::xxh;

pub(crate) const SHARD_MAGIC: u64 = 0x31305F5A54494E47;
/// Shard file format version. A shard records only its payload-column count
/// (`OFF_FILE_NPC`) — every other aspect of a region is derived from the live
/// `SchemaDescriptor` — so a column-shape change to a system family needs a bump
/// here too, not just a header/region layout change. The reader's exact region
/// sizes come from that schema, so they reject most shards written under a
/// different one; a Constant-encoded region is one element wide under either
/// schema, and only this word rejects that.
pub(crate) const SHARD_VERSION: u64 = 12;

/// Pin the system-family column shapes to the version word above, the way
/// `gnitz_wire::wal` pins them to `WAL_FORMAT_VERSION`. Nothing else notices a
/// shape change. If this fails, bump `SHARD_VERSION` and paste the reported
/// digest here.
const _: () = assert!(
    gnitz_wire::SYS_SCHEMA_DIGEST == 5353188239287564337 && SHARD_VERSION == 12,
    "system-family column shapes changed: bump SHARD_VERSION"
);
pub(crate) const HEADER_SIZE: usize = 64;
pub(crate) const DIR_ENTRY_SIZE: usize = 32;
pub(crate) const ALIGNMENT: usize = 64;

pub(crate) const OFF_MAGIC: usize = 0;
pub(crate) const OFF_VERSION: usize = 8;
pub(crate) const OFF_ROW_COUNT: usize = 16;
pub(crate) const OFF_DESC_CHECKSUM: usize = 24;
/// The writer's `schema.num_payload_cols()` (u64 LE) — the file's own arity,
/// which fixes its region count and blob-region index. A reader whose schema is
/// wider (post-`ALTER TABLE … ADD COLUMN`) walks the directory by this count and
/// pads the columns past it to NULL.
pub(crate) const OFF_FILE_NPC: usize = 32;
pub(crate) const OFF_XOR8_OFFSET: usize = 40;
pub(crate) const OFF_XOR8_SIZE: usize = 48;
pub(crate) const OFF_XOR8_CHECKSUM: usize = 56;

/// Byte offset of directory entry `i`. The directory follows the header
/// immediately, so an entry's position is implied by its index — the file
/// stores no directory offset to disagree with this.
pub(crate) const fn dir_entry_off(i: usize) -> usize {
    HEADER_SIZE + i * DIR_ENTRY_SIZE
}

/// Length of the descriptive prefix — header plus one directory entry per
/// region — which is exactly the span [`desc_digest`] covers.
pub(crate) const fn desc_len(num_regions: usize) -> usize {
    dir_entry_off(num_regions)
}

/// XXH3-64 over a shard's descriptive prefix (header + directory), its own eight
/// bytes excluded, seeded with the shard's basename.
///
/// The seed binds a prefix to the name it was written under, so a prefix that
/// arrives from elsewhere — a rename, or a misdirected write carrying a
/// same-shaped neighbour's first sector — fails to validate. It separates names,
/// not directories: `spill_shard_name` is `shard_{table_id}_{lsn}.db` and every
/// child of a partitioned table shares the `table_id`, so the same name in two
/// sibling partition directories seeds identically.
pub(crate) fn desc_digest(basename: &[u8], data: &[u8], num_regions: usize) -> u64 {
    xxh::digest_with_hole(basename, &data[..desc_len(num_regions)], OFF_DESC_CHECKSUM)
}

/// A shard's manifest identity: the last component of its path. The L0 spill
/// writer names a shard by a dirfd-relative basename, compaction by an
/// `AT_FDCWD` full path, and the reader always holds a full path — this is the
/// one reduction all of them use, so the name the manifest records is the name
/// the digest is seeded with.
pub(crate) fn shard_basename(path: &[u8]) -> &[u8] {
    match path.iter().rposition(|&c| c == b'/') {
        Some(i) => &path[i + 1..],
        None => path,
    }
}

pub(crate) const ENCODING_RAW: u8 = 0x00;
pub(crate) const ENCODING_CONSTANT: u8 = 0x01;
pub(crate) const ENCODING_TWO_VALUE: u8 = 0x02;
/// Frame-of-reference + byte-width truncation for an integer payload region:
/// an 8-byte frame reference (the region min's bit pattern) followed by each
/// row's `value − ref` truncated to the fewest whole bytes (`bw`) that hold the
/// region's offset range. Legal only on payload column directory entries, only
/// on compaction outputs.
pub(crate) const ENCODING_FOR: u8 = 0x03;
