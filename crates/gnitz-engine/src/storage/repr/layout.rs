//! Shared shard file format constants.

pub(crate) const SHARD_MAGIC: u64 = 0x31305F5A54494E47;
/// Shard file format version. A shard records no schema — the reader derives
/// its regions from the live `SchemaDescriptor` — so a column-shape change to a
/// system family needs a bump here too, not just a header/region layout change.
/// A shard written under a wider schema passes the per-region size and checksum
/// checks; nothing but this word rejects it.
pub(crate) const SHARD_VERSION: u64 = 10;

/// Pin the system-family column shapes to the version word above, the way
/// `gnitz_wire::wal` pins them to `WAL_FORMAT_VERSION`. Nothing else notices a
/// shape change. If this fails, bump `SHARD_VERSION` and paste the reported
/// digest here.
const _: () = assert!(
    gnitz_wire::SYS_SCHEMA_DIGEST == 5353188239287564337 && SHARD_VERSION == 10,
    "system-family column shapes changed: bump SHARD_VERSION"
);
pub(crate) const HEADER_SIZE: usize = 64;
pub(crate) const DIR_ENTRY_SIZE: usize = 32;
pub(crate) const ALIGNMENT: usize = 64;

pub(crate) const OFF_MAGIC: usize = 0;
pub(crate) const OFF_VERSION: usize = 8;
pub(crate) const OFF_ROW_COUNT: usize = 16;
pub(crate) const OFF_DIR_OFFSET: usize = 24;
// Bytes [32,40) reserved (zero).
pub(crate) const OFF_XOR8_OFFSET: usize = 40;
pub(crate) const OFF_XOR8_SIZE: usize = 48;
// Bytes [56,64) reserved (zero).

pub(crate) const ENCODING_RAW: u8 = 0x00;
pub(crate) const ENCODING_CONSTANT: u8 = 0x01;
pub(crate) const ENCODING_TWO_VALUE: u8 = 0x02;
/// Frame-of-reference + byte-width truncation for an integer payload region:
/// an 8-byte frame reference (the region min's bit pattern) followed by each
/// row's `value − ref` truncated to the fewest whole bytes (`bw`) that hold the
/// region's offset range. Legal only on payload column directory entries, only
/// on compaction outputs.
pub(crate) const ENCODING_FOR: u8 = 0x03;
