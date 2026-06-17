//! Shared shard file format constants.

pub(crate) const SHARD_MAGIC: u64 = 0x31305F5A54494E47;
pub(crate) const SHARD_VERSION: u64 = 7;
pub(crate) const HEADER_SIZE: usize = 64;
pub(crate) const DIR_ENTRY_SIZE: usize = 32;
pub(crate) const ALIGNMENT: usize = 64;

pub(crate) const OFF_MAGIC: usize = 0;
pub(crate) const OFF_VERSION: usize = 8;
pub(crate) const OFF_ROW_COUNT: usize = 16;
pub(crate) const OFF_DIR_OFFSET: usize = 24;
pub(crate) const OFF_TABLE_ID: usize = 32;
pub(crate) const OFF_XOR8_OFFSET: usize = 40;
pub(crate) const OFF_XOR8_SIZE: usize = 48;

pub(crate) const ENCODING_RAW: u8 = 0x00;
pub(crate) const ENCODING_CONSTANT: u8 = 0x01;
pub(crate) const ENCODING_TWO_VALUE: u8 = 0x02;

/// Byte offset of the one-byte flags field in the shard header.
/// Bytes 56–63 were previously unused (zero); byte 56 is now the flags byte.
pub(crate) const OFF_FLAGS: usize = 56;

/// Shard header flag: at most one positive-weight row per PK key in this shard.
/// Only set for base-table shards that pass `PkUniqueChecker`; never set for
/// intermediate views, secondary index tables, or shards containing retractions.
pub(crate) const SHARD_FLAG_PK_UNIQUE: u8 = 0x01;
