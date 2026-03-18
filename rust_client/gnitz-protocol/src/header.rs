pub use crate::wal_block::{IPC_CONTROL_TID, WAL_BLOCK_HEADER_SIZE};

pub const FLAG_ALLOCATE_TABLE_ID:  u64 = 1;
pub const FLAG_ALLOCATE_SCHEMA_ID: u64 = 2;
pub const FLAG_SHUTDOWN:           u64 = 4;
pub const FLAG_DDL_SYNC:           u64 = 8;
pub const FLAG_EXCHANGE:           u64 = 16;
pub const FLAG_PUSH:               u64 = 32;
pub const FLAG_HAS_PK:             u64 = 64;
pub const FLAG_SEEK:               u64 = 128;
pub const FLAG_SEEK_BY_INDEX:      u64 = 256;
pub const FLAG_ALLOCATE_INDEX_ID:  u64 = 512;
pub const FLAG_HAS_SCHEMA:         u64 = 1 << 48;
pub const FLAG_HAS_DATA:           u64 = 1 << 49;

pub const STATUS_OK:    u32 = 0;
pub const STATUS_ERROR: u32 = 1;

pub const META_FLAG_NULLABLE: u64 = 1;
pub const META_FLAG_IS_PK:    u64 = 2;

/// Logical header decoded from the IPC control WAL block.
///
/// Fields align with CONTROL_SCHEMA column indices; no wire-format struct.
pub struct Header {
    pub status:     u32,
    pub target_id:  u64,
    pub client_id:  u64,
    pub flags:      u64,
    pub seek_pk_lo: u64,
    pub seek_pk_hi: u64,
    pub p4:         u64,   // seek_col_idx for FLAG_SEEK_BY_INDEX
}

impl Header {
    /// Set p4/seek_pk_lo/seek_pk_hi for FLAG_SEEK_BY_INDEX requests.
    pub fn set_seek_by_index(&mut self, col_idx: u64, key_lo: u64, key_hi: u64) {
        self.p4         = col_idx;
        self.seek_pk_lo = key_lo;
        self.seek_pk_hi = key_hi;
    }
    pub fn seek_col_idx(&self)    -> u64 { self.p4 }
    pub fn seek_idx_key_lo(&self) -> u64 { self.seek_pk_lo }
    pub fn seek_idx_key_hi(&self) -> u64 { self.seek_pk_hi }
}

impl Default for Header {
    fn default() -> Self {
        Header {
            status: 0, target_id: 0, client_id: 0, flags: 0,
            seek_pk_lo: 0, seek_pk_hi: 0, p4: 0,
        }
    }
}
