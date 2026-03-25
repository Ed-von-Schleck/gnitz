//! Shared shard file format constants.
//!
//! Must match `gnitz/storage/layout.py`.

pub const SHARD_MAGIC: u64 = 0x31305F5A54494E47;
pub const SHARD_VERSION: u64 = 3;
pub const HEADER_SIZE: usize = 64;
pub const DIR_ENTRY_SIZE: usize = 24;
pub const ALIGNMENT: usize = 64;

pub const OFF_MAGIC: usize = 0;
pub const OFF_VERSION: usize = 8;
pub const OFF_ROW_COUNT: usize = 16;
pub const OFF_DIR_OFFSET: usize = 24;
pub const OFF_TABLE_ID: usize = 32;
pub const OFF_XOR8_OFFSET: usize = 40;
pub const OFF_XOR8_SIZE: usize = 48;
