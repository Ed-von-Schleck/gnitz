//! Shared shard file format constants.

pub(crate) const SHARD_MAGIC: u64 = 0x31305F5A54494E47;
pub(crate) const SHARD_VERSION: u64 = 8;
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

pub(crate) const ENCODING_RAW: u8 = 0x00;
pub(crate) const ENCODING_CONSTANT: u8 = 0x01;
pub(crate) const ENCODING_TWO_VALUE: u8 = 0x02;
/// Frame-of-reference + byte-width truncation for an integer payload region:
/// an 8-byte frame reference (the region min's bit pattern) followed by each
/// row's `value − ref` truncated to the fewest whole bytes (`bw`) that hold the
/// region's offset range. Legal only on payload column directory entries, only
/// on compaction outputs.
pub(crate) const ENCODING_FOR: u8 = 0x03;

/// Byte offset of the one-byte flags field in the shard header.
/// Bytes [57,64) are reserved (zero).
pub(crate) const OFF_FLAGS: usize = 56;

/// Shard header flag: at most one positive-weight row per PK key in this shard.
/// Only set for base-table shards that pass `PkUniqueChecker`; never set for
/// intermediate views, secondary index tables, or shards containing retractions.
pub(crate) const SHARD_FLAG_PK_UNIQUE: u8 = 0x01;
