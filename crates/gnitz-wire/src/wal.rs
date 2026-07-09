//! WAL block header constants — the one definition client and engine agree on.
//!
//! Header layout (32 bytes):
//!   [0,4)   TID          u32
//!   [4,8)   COUNT        u32
//!   [8,12)  SIZE         u32 (total block size, header + directory + regions)
//!   [12,16) VERSION      u32
//!   [16,24) CHECKSUM     u64 (XXH3 over [WAL_HEADER_SIZE, SIZE))
//!   [24,28) NUM_REGIONS  u32
//!   [28,32) RESERVED     u32

pub const WAL_HEADER_SIZE: usize = 32;
pub const WAL_FORMAT_VERSION: u32 = 6;

pub const WAL_OFF_TID: usize = 0;
pub const WAL_OFF_COUNT: usize = 4;
pub const WAL_OFF_SIZE: usize = 8;
pub const WAL_OFF_VERSION: usize = 12;
pub const WAL_OFF_CHECKSUM: usize = 16;
pub const WAL_OFF_NUM_REGIONS: usize = 24;

pub const IPC_CONTROL_TID: u32 = 0xFFFF_FFFF;
